//! Database-wide delivery admission. Lock order: tenant -> schedule -> run.
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use crate::{
    domain::{ConcurrencyPolicy, Run, Schedule, default_timeout},
    store::{Store, StoreError},
};

type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct TenantLimits {
    pub max_concurrent: Option<i32>,
    pub rate_per_second: Option<i32>,
    pub burst: i32,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_concurrent: None,
            rate_per_second: None,
            burst: 1,
        }
    }
}

impl TenantLimits {
    pub fn validate(&self) -> Result<()> {
        if self
            .max_concurrent
            .is_some_and(|n| !(1..=4096).contains(&n))
            || self
                .rate_per_second
                .is_some_and(|n| !(1..=10000).contains(&n))
            || !(1..=10000).contains(&self.burst)
            || (self.rate_per_second.is_none() && self.burst != 1)
        {
            return Err(StoreError::InvalidInput("max_concurrent must be 1..4096; rate_per_second and burst must be 1..10000; burst requires a rate limit".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, FromRow)]
pub struct TenantCapacity {
    pub tenant_id: String,
    pub max_concurrent: Option<i32>,
    pub rate_per_second: Option<i32>,
    pub burst: i32,
    pub running: i64,
    pub available_tokens: Option<f64>,
}

impl Store {
    /// Read-only: an authenticated tenant with no schedules has unlimited defaults.
    pub async fn tenant_capacity(&self, tenant: &str) -> Result<TenantCapacity> {
        Ok(sqlx::query_as(
            "SELECT $1::text AS tenant_id, t.max_concurrent, t.rate_per_second,
                COALESCE(t.burst, 1) AS burst,
                (SELECT COUNT(*) FROM runs WHERE tenant_id = $1 AND status = 'running') AS running,
                CASE WHEN t.rate_per_second IS NOT NULL THEN
                    LEAST(t.burst::float8, t.tokens + GREATEST(EXTRACT(EPOCH FROM (statement_timestamp() - t.token_time)), 0)::float8 * t.rate_per_second)
                END AS available_tokens
             FROM (VALUES (1)) AS singleton(n) LEFT JOIN tenant_dispatch t ON t.tenant_id = $1")
            .bind(tenant).fetch_one(&self.pool).await?)
    }

    /// Administrator operation; no HTTP endpoint allows a tenant to raise limits.
    pub async fn set_tenant_limits(
        &self,
        tenant: &str,
        limits: &TenantLimits,
    ) -> Result<TenantCapacity> {
        limits.validate()?;
        if tenant.trim().is_empty() || tenant.len() > 128 || tenant.chars().any(char::is_control) {
            return Err(StoreError::InvalidInput(
                "Tenant names must be 1..128 bytes without control characters".into(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO tenant_dispatch (tenant_id) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(tenant)
            .execute(&mut *tx)
            .await?;
        // Obtain the lock in a separate statement before taking the refill time.
        sqlx::query("SELECT tenant_id FROM tenant_dispatch WHERE tenant_id = $1 FOR UPDATE")
            .bind(tenant)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE tenant_dispatch SET max_concurrent = $2, rate_per_second = $3, burst = $4,
                tokens = CASE WHEN $3::int IS NULL THEN 1
                    WHEN rate_per_second IS NULL THEN $4::float8
                    ELSE LEAST($4::float8, tokens + GREATEST(EXTRACT(EPOCH FROM (statement_timestamp() - token_time)), 0)::float8 * rate_per_second) END,
                token_time = GREATEST(token_time, statement_timestamp())
             WHERE tenant_id = $1")
            .bind(tenant).bind(limits.max_concurrent).bind(limits.rate_per_second).bind(limits.burst)
            .execute(&mut *tx).await?;
        tx.commit().await?;
        self.tenant_capacity(tenant).await
    }

    /// Fair turns among tenants that have eligible work. Tokens and a delivery
    /// lease are committed together; a rollback consumes neither.
    pub async fn claim(&self) -> Result<Option<Run>> {
        let mut tx = self.pool.begin().await?;
        let mut visited = Vec::<String>::new();
        // A locked schedule must not trap arbitration on the same tenant. Move
        // unsuccessful turns back as well, and bound each transaction's work.
        for _ in 0..8 {
            let mut tenant: Option<String> =
                sqlx::query_scalar(include_str!("sql/claim_tenant.sql"))
                    .bind(&visited)
                    .bind(false)
                    .fetch_optional(&mut *tx)
                    .await?;
            if tenant.is_none() {
                // The first fair turns may all be empty, throttled or locked.
                // Seek distinct pending tenants without scanning old tenants
                // or every run in a large tenant backlog, then order fairly.
                tenant = sqlx::query_scalar(include_str!("sql/claim_tenant.sql"))
                    .bind(&visited)
                    .bind(true)
                    .fetch_optional(&mut *tx)
                    .await?;
            }
            let Some(tenant) = tenant else {
                break;
            };
            let claimed = Self::claim_for_tenant(&mut tx, &tenant).await?;
            if let Some(run) = claimed {
                tx.commit().await?;
                return Ok(Some(run));
            }
            sqlx::query("UPDATE tenant_dispatch SET last_served = nextval('tenant_dispatch_order') WHERE tenant_id = $1")
                .bind(&tenant).execute(&mut *tx).await?;
            visited.push(tenant);
        }
        tx.commit().await?;
        Ok(None)
    }

    async fn claim_for_tenant(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant: &str,
    ) -> Result<Option<Run>> {
        // READ COMMITTED gives a fresh snapshot after the tenant lock: concurrent
        // claims cannot both pass an outdated running count or token balance.
        let admitted: bool = sqlx::query_scalar(
            "SELECT (t.max_concurrent IS NULL OR
                (SELECT COUNT(*) FROM runs WHERE tenant_id = $1 AND status = 'running') < t.max_concurrent)
             AND (t.rate_per_second IS NULL OR
                LEAST(t.burst::float8, t.tokens + GREATEST(EXTRACT(EPOCH FROM (statement_timestamp() - t.token_time)), 0)::float8 * t.rate_per_second) >= 1)
             FROM tenant_dispatch t WHERE tenant_id = $1")
            .bind(tenant).fetch_one(&mut **tx).await?;
        if !admitted {
            return Ok(None);
        }
        let schedule: Option<Schedule> = sqlx::query_as(include_str!("sql/claim_schedule.sql"))
            .bind(tenant)
            .fetch_optional(&mut **tx)
            .await?;
        let Some(schedule) = schedule else {
            return Ok(None);
        };
        if schedule.spec.concurrency == ConcurrencyPolicy::Forbid {
            let running: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM runs WHERE schedule_id = $1 AND status = 'running')",
            )
            .bind(schedule.id)
            .fetch_one(&mut **tx)
            .await?;
            if running {
                return Ok(None);
            }
        }
        // The locked parent and fresh quota checks above still serialize
        // admission. Lease, attempt and token writes now share one round trip;
        // only the final run snapshot (including payload) crosses the wire.
        Ok(sqlx::query_as(include_str!("sql/lease_run.sql"))
            .bind(schedule.id)
            .bind(schedule.revision)
            .bind(Uuid::new_v4())
            .bind(i64::from(default_timeout()))
            .bind(Uuid::new_v4())
            .bind(tenant)
            .fetch_optional(&mut **tx)
            .await?)
    }
}
