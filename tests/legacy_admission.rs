//! Serde defaults and database admission must agree for older stored snapshots.
use chrono::Duration;
use scheduler_service::{
    dispatch::TenantLimits,
    domain::{ConcurrencyPolicy, DeliveryResult, ScheduleSpec},
    store::Store,
};
use serde_json::json;
use sqlx::PgPool;

async fn default_concurrency_remains_allowed(pool: PgPool, idle_tenants: bool) {
    let store = Store { pool };
    let at = store.now().await.unwrap() - Duration::minutes(1);
    let spec: ScheduleSpec = serde_json::from_value(json!({
        "name": "legacy default concurrency",
        "trigger": {"type": "once", "at": at},
        "target": {"url": "https://example.invalid/hook"}
    }))
    .unwrap();
    let schedule = store.create("live", spec, at, None, "test").await.unwrap();
    let dates: Vec<_> = (0..3).map(|n| at + Duration::microseconds(n)).collect();
    store.materialize(&schedule, &dates, None).await.unwrap();
    sqlx::raw_sql(
        "UPDATE schedules SET spec = spec - 'concurrency';
         UPDATE runs SET spec = spec - 'concurrency';",
    )
    .execute(&store.pool)
    .await
    .unwrap();
    if idle_tenants {
        // Force discovery beyond the bounded fast path into the fallback.
        sqlx::query(
            "INSERT INTO tenant_dispatch (tenant_id)
             SELECT 'idle-' || n::text FROM generate_series(1, 96) n",
        )
        .execute(&store.pool)
        .await
        .unwrap();
    }
    store
        .set_tenant_limits(
            "live",
            &TenantLimits {
                max_concurrent: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_schedule("live", schedule.id)
            .await
            .unwrap()
            .spec
            .concurrency,
        ConcurrencyPolicy::Allow
    );
    let first = store.claim().await.unwrap().unwrap();
    let second = store
        .claim()
        .await
        .unwrap()
        .expect("missing concurrency must use the same Allow default as serde");
    assert_ne!(first.id, second.id);
    assert_eq!(first.schedule_id, second.schedule_id);
    assert!(
        store.claim().await.unwrap().is_none(),
        "tenant quota still applies"
    );
    assert!(
        store
            .finish(
                &first,
                DeliveryResult {
                    success: true,
                    http_status: Some(200),
                    error: None,
                    response_excerpt: None,
                    retry_after: None,
                },
            )
            .await
            .unwrap()
    );
    let third = store.claim().await.unwrap().unwrap();
    assert_ne!(third.id, first.id);
    assert_ne!(third.id, second.id);
    assert_eq!(third.schedule_id, schedule.id);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn legacy_default_concurrency_works_in_fast_discovery(pool: PgPool) {
    default_concurrency_remains_allowed(pool, false).await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn legacy_default_concurrency_works_beyond_idle_tenants(pool: PgPool) {
    default_concurrency_remains_allowed(pool, true).await;
}
