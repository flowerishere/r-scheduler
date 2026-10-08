use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions, types::Json};
use uuid::Uuid;

use crate::domain::{Schedule, ScheduleSpec};

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
            sqlx::query("INSERT INTO runs (id,schedule_id,tenant_id,revision,scheduled_at,available_at,status,spec) VALUES ($1,$2,$3,$4,$5,$5,'pending',$6) ON CONFLICT (schedule_id,revision,scheduled_at) DO NOTHING")
                .bind(Uuid::new_v4()).bind(current.id).bind(&current.tenant_id).bind(current.revision)
                .bind(date).bind(&current.spec).execute(&mut *tx).await?;
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
}
