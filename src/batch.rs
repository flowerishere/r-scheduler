//! Atomic schedule imports with tenant-scoped, durable batch idempotency.
use chrono::{DateTime, Utc};
use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{
    domain::{Schedule, ScheduleSpec},
    store::{Store, StoreError},
};

pub const MAX_BATCH_SCHEDULES: usize = 20;

pub(crate) struct PreparedSchedule {
    pub spec: ScheduleSpec,
    pub next: DateTime<Utc>,
    pub request_hash: String,
}

impl Store {
    pub(crate) async fn find_batch(
        &self,
        tenant: &str,
        key: &str,
        hash: &str,
    ) -> Result<Option<Vec<Schedule>>, StoreError> {
        let batch: Option<(String, Vec<Uuid>)> = sqlx::query_as(
            "SELECT request_hash, schedule_ids FROM schedule_batches WHERE tenant_id = $1 AND idempotency_key = $2",
        )
        .bind(tenant)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        let Some((request_hash, ids)) = batch else {
            return Ok(None);
        };
        if request_hash != hash {
            return Err(StoreError::Conflict(
                "Idempotency-Key was already used with another batch".into(),
            ));
        }
        let schedules = sqlx::query_as(
            "SELECT * FROM schedules WHERE tenant_id = $1 AND id = ANY($2) ORDER BY array_position($2, id)",
        )
        .bind(tenant)
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(Some(schedules))
    }

    pub(crate) async fn create_batch(
        &self,
        tenant: &str,
        key: &str,
        hash: &str,
        entries: &[PreparedSchedule],
    ) -> Result<(bool, Vec<Schedule>), StoreError> {
        if !(1..=MAX_BATCH_SCHEDULES).contains(&entries.len()) {
            return Err(StoreError::InvalidInput(format!(
                "schedules must contain 1..{MAX_BATCH_SCHEDULES} entries"
            )));
        }
        let ids: Vec<_> = entries.iter().map(|_| Uuid::new_v4()).collect();
        let mut tx = self.pool.begin().await?;
        // Reserve the key in the same transaction as every schedule insert.
        // A concurrent loser waits for commit, then reads the winning batch.
        let inserted = sqlx::query(
            "INSERT INTO schedule_batches (tenant_id, idempotency_key, request_hash, schedule_ids)
             VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        )
        .bind(tenant)
        .bind(key)
        .bind(hash)
        .bind(&ids)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted == 0 {
            tx.commit().await?;
            return self
                .find_batch(tenant, key, hash)
                .await?
                .map(|schedules| (false, schedules))
                .ok_or(StoreError::NotFound);
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO schedules (id, tenant_id, spec, status, next_fire_at, request_hash) ",
        );
        query.push_values(entries.iter().zip(&ids), |mut row, (entry, id)| {
            row.push_bind(id)
                .push_bind(tenant)
                .push_bind(sqlx::types::Json(&entry.spec))
                .push_bind("active")
                .push_bind(entry.next)
                .push_bind(&entry.request_hash);
        });
        query.push(" RETURNING *");
        let mut schedules: Vec<Schedule> = query.build_query_as().fetch_all(&mut *tx).await?;
        schedules.sort_unstable_by_key(|schedule| ids.iter().position(|id| *id == schedule.id));
        tx.commit().await?;
        Ok((true, schedules))
    }
}
