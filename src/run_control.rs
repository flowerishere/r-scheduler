//! Control a pending execution without changing its schedule or delivery history.
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    domain::{Run, Schedule},
    store::{Store, StoreError},
};

type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunTiming {
    Once { at: DateTime<Utc> },
    Delay { seconds: u32 },
}

impl RunTiming {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Once { at } => validate_time(*at),
            Self::Delay { seconds } if *seconds > 31_536_000 => Err(StoreError::InvalidInput(
                "delay seconds must be 0..31536000".into(),
            )),
            Self::Delay { .. } => Ok(()),
        }
    }

    fn resolve(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
        match self {
            Self::Once { at } => Ok(*at),
            Self::Delay { seconds } => now
                .checked_add_signed(Duration::seconds(i64::from(*seconds)))
                .ok_or_else(|| StoreError::InvalidInput("Run availability is out of range".into())),
        }
    }
}

fn validate_time(at: DateTime<Utc>) -> Result<()> {
    crate::trigger::validate_once(at).map_err(|error| StoreError::InvalidInput(error.to_string()))
}

async fn lock_run(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &str,
    id: Uuid,
) -> Result<(Schedule, Run)> {
    // Match claim, recovery, replay and cleanup: always lock parent before run.
    // Locate and lock the parent together, avoiding an extra unlocked lookup.
    let schedule = sqlx::query_as(
        "SELECT s.* FROM schedules s JOIN runs r ON r.schedule_id = s.id AND r.tenant_id = s.tenant_id
         WHERE r.tenant_id = $1 AND r.id = $2 FOR UPDATE OF s",
    )
    .bind(tenant)
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(StoreError::NotFound)?;
    let run = sqlx::query_as("SELECT * FROM runs WHERE tenant_id = $1 AND id = $2 FOR UPDATE")
        .bind(tenant)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(StoreError::NotFound)?;
    Ok((schedule, run))
}

impl Store {
    /// Cancel only work which has not been claimed. Repeating a cancellation is safe.
    pub async fn cancel_run(&self, tenant: &str, id: Uuid) -> Result<Run> {
        let mut tx = self.pool.begin().await?;
        let (_, current) = lock_run(&mut tx, tenant, id).await?;
        if current.status == "cancelled" {
            tx.commit().await?;
            return Ok(current);
        }
        if current.status != "pending" {
            return Err(StoreError::Conflict(
                "Only pending runs can be cancelled; an in-flight delivery cannot be stopped"
                    .into(),
            ));
        }
        let updated = sqlx::query_as(
            "UPDATE runs SET status = 'cancelled', finished_at = clock_timestamp(),
             last_error = 'Run cancelled by operator' WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(updated)
    }

    /// Move admission time while retaining original expiry, identity and attempts.
    pub async fn reschedule_run(
        &self,
        tenant: &str,
        id: Uuid,
        expected_available_at: DateTime<Utc>,
        timing: RunTiming,
    ) -> Result<Run> {
        validate_time(expected_available_at)?;
        timing.validate()?;
        let mut tx = self.pool.begin().await?;
        let (schedule, current) = lock_run(&mut tx, tenant, id).await?;
        if current.status != "pending"
            || schedule.status == "cancelled"
            || current.revision != schedule.revision
        {
            return Err(StoreError::Conflict(
                "Only pending runs of the current, uncancelled schedule can be rescheduled".into(),
            ));
        }
        if current.available_at != expected_available_at {
            return Err(StoreError::Conflict(
                "Run availability changed; fetch the run and retry".into(),
            ));
        }
        // Sample time after acquiring both locks. Waiting for another mutation
        // must not permit rescheduling an expired run using an old clock value.
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        let available_at = timing.resolve(now)?;
        if available_at < now {
            return Err(StoreError::InvalidInput(
                "Run availability must not be in the past; use delay 0 to enqueue now".into(),
            ));
        }
        if current
            .expires_at
            .is_some_and(|expiry| now >= expiry || available_at >= expiry)
        {
            return Err(StoreError::Conflict(
                "Run availability must be before the original admission deadline".into(),
            ));
        }
        let updated = sqlx::query_as("UPDATE runs SET available_at = $2 WHERE id = $1 RETURNING *")
            .bind(id)
            .bind(available_at)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(updated)
    }
}
