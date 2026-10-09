use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions, types::Json};
use uuid::Uuid;

use crate::domain::{Attempt, DeliveryResult, Run, Schedule, ScheduleSpec};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("Resource not found")]
    NotFound,
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    InvalidInput(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone)]
pub struct Store {
    pub pool: PgPool,
}

impl Store {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(32)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = '15s'")
                        .execute(&mut *connection)
                        .await?;
                    sqlx::query("SET lock_timeout = '5s'")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn now(&self) -> Result<DateTime<Utc>> {
        Ok(sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
            .await?)
    }

    pub async fn get_schedule(&self, tenant: &str, id: Uuid) -> Result<Schedule> {
        sqlx::query_as("SELECT * FROM schedules WHERE tenant_id = $1 AND id = $2")
            .bind(tenant)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::NotFound)
    }

    pub async fn find_idempotent(
        &self,
        tenant: &str,
        key: &str,
        hash: &str,
    ) -> Result<Option<Schedule>> {
        let found: Option<Schedule> =
            sqlx::query_as("SELECT * FROM schedules WHERE tenant_id = $1 AND idempotency_key = $2")
                .bind(tenant)
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        if found.as_ref().is_some_and(|s| s.request_hash != hash) {
            return Err(StoreError::Conflict(
                "Idempotency-Key was already used with another request".into(),
            ));
        }
        Ok(found)
    }

    pub async fn create(
        &self,
        tenant: &str,
        spec: ScheduleSpec,
        next: DateTime<Utc>,
        key: Option<&str>,
        hash: &str,
    ) -> Result<Schedule> {
        let result = sqlx::query_as::<_, Schedule>(
            "INSERT INTO schedules (id, tenant_id, spec, status, next_fire_at, idempotency_key, request_hash)
             VALUES ($1, $2, $3, 'active', $4, $5, $6)
             ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING *")
            .bind(Uuid::new_v4()).bind(tenant).bind(Json(spec)).bind(next).bind(key).bind(hash)
            .fetch_optional(&self.pool).await?;
        match result {
            Some(schedule) => Ok(schedule),
            None => self
                .find_idempotent(tenant, key.unwrap_or_default(), hash)
                .await?
                .ok_or(StoreError::NotFound),
        }
    }

    pub async fn list_schedules(
        &self,
        tenant: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Schedule>> {
        Ok(sqlx::query_as("SELECT * FROM schedules WHERE tenant_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2 OFFSET $3")
            .bind(tenant)
            .bind(limit.clamp(1, 100))
            .bind(offset.clamp(0, 100_000))
            .fetch_all(&self.pool).await?)
    }

    async fn lock_schedule(
        tx: &mut Transaction<'_, Postgres>,
        tenant: &str,
        id: Uuid,
    ) -> Result<Schedule> {
        sqlx::query_as("SELECT * FROM schedules WHERE tenant_id = $1 AND id = $2 FOR UPDATE")
            .bind(tenant)
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(StoreError::NotFound)
    }

    pub async fn replace(
        &self,
        tenant: &str,
        id: Uuid,
        expected_revision: i64,
        spec: ScheduleSpec,
        next: DateTime<Utc>,
    ) -> Result<Schedule> {
        let mut tx = self.pool.begin().await?;
        let old = Self::lock_schedule(&mut tx, tenant, id).await?;
        if old.revision != expected_revision {
            return Err(StoreError::Conflict(
                "Revision changed; fetch the schedule and retry".into(),
            ));
        }
        if old.status == "cancelled" {
            return Err(StoreError::Conflict(
                "Cancelled schedules cannot be edited".into(),
            ));
        }
        sqlx::query("UPDATE runs SET status = 'cancelled', finished_at = clock_timestamp(), last_error = 'Schedule revised' WHERE schedule_id = $1 AND status = 'pending'")
            .bind(id).execute(&mut *tx).await?;
        let status = if old.status == "paused" {
            "paused"
        } else {
            "active"
        };
        let updated = sqlx::query_as("UPDATE schedules SET spec = $2, revision = revision + 1, status = $3, next_fire_at = $4, last_error = NULL, updated_at = clock_timestamp() WHERE id = $1 RETURNING *")
            .bind(id).bind(Json(spec)).bind(status).bind(next).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(updated)
    }
    pub async fn due_schedules(&self, limit: i64) -> Result<Vec<Schedule>> {
        Ok(sqlx::query_as("SELECT * FROM schedules WHERE status = 'active' AND next_fire_at <= clock_timestamp() ORDER BY next_fire_at, id LIMIT $1")
            .bind(limit).fetch_all(&self.pool).await?)
    }
    pub async fn materialize(
        &self,
        snapshot: &Schedule,
        dates: &[DateTime<Utc>],
        next: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let current = Self::lock_schedule(&mut tx, &snapshot.tenant_id, snapshot.id).await?;
        if current.status != "active"
            || current.revision != snapshot.revision
            || current.next_fire_at != snapshot.next_fire_at
        {
            return Ok(false);
        }
        for date in dates {
            let expires_at = current
                .spec
                .retry
                .max_age_seconds
                .map(|age| {
                    date.checked_add_signed(Duration::seconds(i64::from(age)))
                        .ok_or_else(|| {
                            StoreError::Conflict(
                                "Run deadline exceeds the supported date range".into(),
                            )
                        })
                })
                .transpose()?;
            sqlx::query("INSERT INTO runs (id,schedule_id,tenant_id,revision,scheduled_at,available_at,status,spec,expires_at,finished_at,last_error)
                VALUES ($1,$2,$3,$4,$5,$5,CASE WHEN $7::timestamptz <= statement_timestamp() THEN 'dead' ELSE 'pending' END,$6,$7,
                CASE WHEN $7 <= statement_timestamp() THEN statement_timestamp() END,
                CASE WHEN $7 <= statement_timestamp() THEN 'Run admission deadline expired' END)
                ON CONFLICT (schedule_id,revision,scheduled_at) DO NOTHING")
                .bind(Uuid::new_v4()).bind(current.id).bind(&current.tenant_id).bind(current.revision)
                .bind(date).bind(&current.spec).bind(expires_at).execute(&mut *tx).await?;
        }
        let status = if next.is_some() {
            "active"
        } else {
            "completed"
        };
        sqlx::query("UPDATE schedules SET next_fire_at = $2, status = $3, last_error = NULL, updated_at = clock_timestamp() WHERE id = $1")
            .bind(current.id).bind(next).bind(status).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(true)
    }
    pub async fn rule_error(&self, snapshot: &Schedule, error: &str) -> Result<()> {
        sqlx::query("UPDATE schedules SET status = 'error', last_error = $4, updated_at = clock_timestamp()
            WHERE id = $1 AND revision = $2 AND next_fire_at IS NOT DISTINCT FROM $3 AND status = 'active'")
            .bind(snapshot.id).bind(snapshot.revision).bind(snapshot.next_fire_at).bind(error)
            .execute(&self.pool).await?;
        Ok(())
    }
    pub async fn finish(&self, run: &Run, outcome: DeliveryResult) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let changed = Self::record_outcome(&mut tx, run, outcome, false).await?;
        tx.commit().await?;
        Ok(changed)
    }
    async fn record_outcome(
        tx: &mut Transaction<'_, Postgres>,
        snapshot: &Run,
        outcome: DeliveryResult,
        expired: bool,
    ) -> Result<bool> {
        let schedule = Self::lock_schedule(tx, &snapshot.tenant_id, snapshot.schedule_id).await?;
        let current: Option<Run> = sqlx::query_as("SELECT * FROM runs WHERE id = $1 FOR UPDATE")
            .bind(snapshot.id)
            .fetch_optional(&mut **tx)
            .await?;
        let Some(current) = current else {
            return Ok(false);
        };
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut **tx)
            .await?;
        if current.status != "running"
            || current.lease_token != snapshot.lease_token
            || current
                .lease_until
                .is_none_or(|end| (end <= now) != expired)
        {
            return Ok(false);
        }
        let obsolete = schedule.status == "cancelled" || schedule.revision != current.revision;
        let delay = current
            .spec
            .retry
            .delay(current.cycle_attempts, current.id)
            .max(outcome.retry_after.as_ref().map_or(0, |hint| {
                hint.delay(now, current.spec.retry.max_delay_seconds)
            }));
        let retry_at = now + Duration::seconds(delay);
        let deadline_reached = current.expires_at.is_some_and(|end| retry_at >= end);
        let status = if outcome.success {
            "succeeded"
        } else if obsolete {
            "cancelled"
        } else if deadline_reached
            || current.cycle_attempts >= current.spec.retry.max_attempts as i32
        {
            "dead"
        } else {
            "pending"
        };
        let attempt_status = if expired {
            "lease_expired"
        } else if outcome.success {
            "succeeded"
        } else {
            "failed"
        };
        sqlx::query("UPDATE attempts SET status = $2, finished_at = $3, http_status = $4, error = $5, response_excerpt = $6 WHERE lease_token = $1 AND status = 'running'")
            .bind(current.lease_token).bind(attempt_status).bind(now).bind(outcome.http_status)
            .bind(&outcome.error).bind(&outcome.response_excerpt).execute(&mut **tx).await?;
        let available = if status == "pending" {
            retry_at
        } else {
            current.available_at
        };
        let finished = if status == "pending" { None } else { Some(now) };
        let last_error = if status == "dead" && deadline_reached {
            Some("No retry fits before the run admission deadline".to_owned())
        } else {
            outcome.error
        };
        sqlx::query("UPDATE runs SET status = $2, available_at = $3, lease_token = NULL, lease_until = NULL, last_error = $4, finished_at = $5 WHERE id = $1")
            .bind(current.id).bind(status).bind(available).bind(last_error).bind(finished)
            .execute(&mut **tx).await?;
        Ok(true)
    }
    pub async fn recover_expired(&self) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        // Lock parents while selecting, before LIMIT. A busy schedule with an
        // entire batch of expired runs must not starve unrelated recovery.
        let expired: Vec<Run> = sqlx::query_as(
            "SELECT r.* FROM runs r JOIN schedules s ON s.id = r.schedule_id
            WHERE r.status = 'running' AND r.lease_until <= clock_timestamp()
            ORDER BY r.lease_until, r.id LIMIT 100 FOR UPDATE OF s SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await?;
        let mut count = 0;
        for run in expired {
            if Self::record_outcome(
                &mut tx,
                &run,
                DeliveryResult::error("Worker lease expired; delivery outcome is unknown"),
                true,
            )
            .await?
            {
                count += 1;
            }
        }
        tx.commit().await?;
        Ok(count)
    }
    pub async fn transition(&self, tenant: &str, id: Uuid, action: &str) -> Result<Schedule> {
        let mut tx = self.pool.begin().await?;
        let old = Self::lock_schedule(&mut tx, tenant, id).await?;
        if old.status == "cancelled" && action != "cancel" {
            return Err(StoreError::Conflict(
                "Cancelled schedules cannot be resumed or paused".into(),
            ));
        }
        let status = match action {
            "pause" => "paused",
            "resume" => {
                if old.next_fire_at.is_some() {
                    "active"
                } else {
                    "completed"
                }
            }
            "cancel" => "cancelled",
            _ => return Err(StoreError::Conflict("Unknown schedule action".into())),
        };
        if action == "cancel" {
            sqlx::query("UPDATE runs SET status = 'cancelled', finished_at = clock_timestamp(), last_error = 'Schedule cancelled' WHERE schedule_id = $1 AND status = 'pending'")
                .bind(id).execute(&mut *tx).await?;
        }
        let updated = sqlx::query_as("UPDATE schedules SET status = $2, updated_at = clock_timestamp() WHERE id = $1 RETURNING *")
            .bind(id).bind(status).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(updated)
    }
    pub async fn get_run(&self, tenant: &str, id: Uuid) -> Result<Run> {
        sqlx::query_as("SELECT * FROM runs WHERE tenant_id = $1 AND id = $2")
            .bind(tenant)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::NotFound)
    }
    pub async fn list_runs(
        &self,
        tenant: &str,
        schedule: Option<Uuid>,
        status: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Run>> {
        Ok(sqlx::query_as("SELECT * FROM runs WHERE tenant_id = $1 AND ($2::uuid IS NULL OR schedule_id = $2) AND ($3::text IS NULL OR status = $3) ORDER BY created_at DESC, id DESC LIMIT $4 OFFSET $5")
            .bind(tenant).bind(schedule).bind(status).bind(limit).bind(offset).fetch_all(&self.pool).await?)
    }
    pub async fn attempts(&self, tenant: &str, id: Uuid) -> Result<Vec<Attempt>> {
        self.get_run(tenant, id).await?;
        Ok(
            sqlx::query_as("SELECT * FROM attempts WHERE run_id=$1 ORDER BY number")
                .bind(id)
                .fetch_all(&self.pool)
                .await?,
        )
    }
    pub async fn replay(&self, tenant: &str, id: Uuid) -> Result<Run> {
        let snapshot = self.get_run(tenant, id).await?;
        let mut tx = self.pool.begin().await?;
        let schedule = Self::lock_schedule(&mut tx, tenant, snapshot.schedule_id).await?;
        if schedule.status == "cancelled" || schedule.revision != snapshot.revision {
            return Err(StoreError::Conflict(
                "Cannot replay a cancelled or superseded schedule revision".into(),
            ));
        }
        let run = sqlx::query_as("UPDATE runs SET status = 'pending', cycle_attempts = 0, available_at = clock_timestamp(), finished_at = NULL WHERE id = $1 AND status = 'dead' AND (expires_at IS NULL OR expires_at > clock_timestamp()) RETURNING *")
            .bind(id).fetch_optional(&mut *tx).await?
            .ok_or_else(|| StoreError::Conflict("Only dead runs with an unexpired admission deadline can be replayed".into()))?;
        tx.commit().await?;
        Ok(run)
    }
    pub async fn stats(&self, tenant: &str) -> Result<serde_json::Value> {
        let schedule_counts: Vec<(String, i64)> = sqlx::query_as(
            "SELECT status, COUNT(*) FROM schedules WHERE tenant_id = $1 GROUP BY status",
        )
        .bind(tenant)
        .fetch_all(&self.pool)
        .await?;
        let run_counts: Vec<(String, i64)> = sqlx::query_as(
            "SELECT status, COUNT(*) FROM runs WHERE tenant_id = $1 GROUP BY status",
        )
        .bind(tenant)
        .fetch_all(&self.pool)
        .await?;
        Ok(serde_json::json!({
            "schedules": schedule_counts.into_iter().collect::<std::collections::BTreeMap<_, _>>(),
            "runs": run_counts.into_iter().collect::<std::collections::BTreeMap<_, _>>()
        }))
    }
    pub async fn expire_pending(&self) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        // Select and lock parents before applying LIMIT, as in lease recovery.
        // Locked plans must not consume the entire maintenance batch.
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT r.id FROM runs r JOIN schedules s ON s.id = r.schedule_id
             WHERE r.status = 'pending' AND r.expires_at <= statement_timestamp()
             ORDER BY r.expires_at, r.id LIMIT 100 FOR UPDATE OF s SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await?;
        let result = sqlx::query("UPDATE runs SET status = 'dead', finished_at = statement_timestamp(), last_error = 'Run admission deadline expired'
            WHERE id = ANY($1) AND status = 'pending' AND expires_at <= statement_timestamp()")
            .bind(ids).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(result.rows_affected())
    }
    pub async fn cleanup_history(
        &self,
        cutoff: DateTime<Utc>,
        limit: i64,
        apply: bool,
    ) -> Result<CleanupReport> {
        if !(1..=10_000).contains(&limit) || cutoff >= self.now().await? {
            return Err(StoreError::Conflict(
                "Cleanup requires a past cutoff and batch size 1..10000".into(),
            ));
        }
        let eligible_runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE status IN ('succeeded', 'dead', 'cancelled') AND finished_at < $1")
            .bind(cutoff).fetch_one(&self.pool).await?;
        let mut report = CleanupReport {
            cutoff,
            dry_run: !apply,
            eligible_runs,
            deleted_runs: 0,
            deleted_attempts: 0,
        };
        if !apply || eligible_runs == 0 {
            return Ok(report);
        }
        let mut tx = self.pool.begin().await?;
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT r.id FROM runs r JOIN schedules s ON s.id = r.schedule_id
            WHERE r.status IN ('succeeded', 'dead', 'cancelled') AND r.finished_at < $1
            ORDER BY r.finished_at, r.id LIMIT $2 FOR UPDATE OF s SKIP LOCKED",
        )
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        report.deleted_attempts = sqlx::query_scalar("SELECT COUNT(*) FROM attempts a JOIN runs r ON r.id = a.run_id
            WHERE r.id = ANY($1) AND r.status IN ('succeeded', 'dead', 'cancelled') AND r.finished_at < $2")
            .bind(&ids).bind(cutoff).fetch_one(&mut *tx).await?;
        report.deleted_runs = sqlx::query(
            "DELETE FROM runs WHERE id = ANY($1)
            AND status IN ('succeeded', 'dead', 'cancelled') AND finished_at < $2",
        )
        .bind(ids)
        .bind(cutoff)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(report)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct CleanupReport {
    pub cutoff: DateTime<Utc>,
    pub dry_run: bool,
    pub eligible_runs: i64,
    pub deleted_runs: u64,
    pub deleted_attempts: i64,
}
