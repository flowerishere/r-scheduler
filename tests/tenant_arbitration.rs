//! Candidate discovery must preserve admission and fairness across idle tenants.
use chrono::Duration;
use scheduler_service::{
    dispatch::TenantLimits,
    domain::{DeliveryResult, Schedule, ScheduleSpec},
    store::Store,
};
use serde_json::json;
use sqlx::PgPool;

async fn pending(store: &Store, tenant: &str, count: usize) -> Schedule {
    let at = store.now().await.unwrap() - Duration::minutes(1);
    let spec: ScheduleSpec = serde_json::from_value(json!({
        "name": "tenant arbitration regression",
        "trigger": {"type": "once", "at": at},
        "target": {"url": "https://example.invalid/hook"}
    }))
    .unwrap();
    let schedule = store.create(tenant, spec, at, None, "test").await.unwrap();
    let dates: Vec<_> = (0..count)
        .map(|n| at + Duration::microseconds(n as i64))
        .collect();
    assert!(store.materialize(&schedule, &dates, None).await.unwrap());
    schedule
}

async fn idle_tenants(store: &Store) {
    // Empty historical registrations retain the oldest fairness positions.
    sqlx::query(
        "INSERT INTO tenant_dispatch (tenant_id)
         SELECT 'empty-' || lpad(n::text, 3, '0') FROM generate_series(0, 95) AS n",
    )
    .execute(&store.pool)
    .await
    .unwrap();
}

fn success() -> DeliveryResult {
    DeliveryResult {
        success: true,
        http_status: Some(200),
        error: None,
        response_excerpt: None,
        retry_after: None,
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn idle_tenants_do_not_block_rotation_and_can_rejoin_after_new_work(pool: PgPool) {
    let store = Store { pool };
    idle_tenants(&store).await;
    pending(&store, "live-a", 3).await;
    pending(&store, "live-b", 3).await;
    let mut order = Vec::new();
    for _ in 0..6 {
        let run = store.claim().await.unwrap().unwrap();
        order.push(run.tenant_id.clone());
        assert!(store.finish(&run, success()).await.unwrap());
    }
    assert_eq!(
        order,
        ["live-a", "live-b", "live-a", "live-b", "live-a", "live-b"]
    );
    assert!(store.claim().await.unwrap().is_none());

    // Discover fresh work for both a previously empty and a now idle tenant.
    pending(&store, "empty-007", 1).await;
    pending(&store, "live-a", 1).await;
    for tenant in ["empty-007", "live-a"] {
        let run = store.claim().await.unwrap().unwrap();
        assert_eq!(run.tenant_id, tenant);
        assert!(store.finish(&run, success()).await.unwrap());
    }
    assert!(store.claim().await.unwrap().is_none());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn locked_front_tenants_do_not_hide_ready_tenants_beyond_the_window(pool: PgPool) {
    let store = Store { pool };
    for n in 0..40 {
        pending(&store, &format!("ready-{n:03}"), 1).await;
    }
    let mut held = store.pool.begin().await.unwrap();
    let locked: Vec<String> = sqlx::query_scalar(
        "SELECT tenant_id FROM tenant_dispatch
         ORDER BY last_served, tenant_id LIMIT 32 FOR UPDATE",
    )
    .fetch_all(&mut *held)
    .await
    .unwrap();
    assert_eq!(locked.len(), 32);

    // The held transaction stays open: SKIP LOCKED must reach later tenants.
    for tenant in ["ready-032", "ready-033"] {
        let run = tokio::time::timeout(std::time::Duration::from_secs(5), store.claim())
            .await
            .expect("tenant row locks must not block unrelated admission")
            .unwrap()
            .unwrap();
        assert_eq!(run.tenant_id, tenant);
    }
    let untouched: bool = sqlx::query_scalar(
        "SELECT bool_and(last_served = 0) FROM tenant_dispatch WHERE tenant_id = ANY($1)",
    )
    .bind(&locked)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert!(untouched);
    held.rollback().await.unwrap();
    assert_eq!(store.claim().await.unwrap().unwrap().tenant_id, "ready-000");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn discovery_beyond_idle_tenants_preserves_eligibility_and_limits(pool: PgPool) {
    let store = Store { pool };
    let capacity = pending(&store, "invalid-capacity", 2).await;
    let running = store.claim().await.unwrap().unwrap();
    assert_eq!(running.schedule_id, capacity.id);
    store
        .set_tenant_limits(
            "invalid-capacity",
            &TenantLimits {
                max_concurrent: Some(1),
                ..TenantLimits::default()
            },
        )
        .await
        .unwrap();
    idle_tenants(&store).await;
    let future = pending(&store, "invalid-future", 1).await;
    let expired = pending(&store, "invalid-expired", 1).await;
    let paused = pending(&store, "invalid-paused", 1).await;
    let stale = pending(&store, "invalid-stale", 1).await;
    pending(&store, "invalid-rate", 1).await;
    store
        .set_tenant_limits(
            "invalid-rate",
            &TenantLimits {
                rate_per_second: Some(1),
                burst: 1,
                ..TenantLimits::default()
            },
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE tenant_dispatch SET tokens = 0, token_time = clock_timestamp() + INTERVAL '1 hour'
         WHERE tenant_id = 'invalid-rate'",
    )
    .execute(&store.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE runs SET available_at = clock_timestamp() + INTERVAL '1 hour' WHERE schedule_id = $1",
    )
    .bind(future.id)
    .execute(&store.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE runs SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE schedule_id = $1",
    )
    .bind(expired.id)
    .execute(&store.pool)
    .await
    .unwrap();
    store
        .transition("invalid-paused", paused.id, "pause")
        .await
        .unwrap();
    // Exercise a retained obsolete snapshot without cancelling it first.
    sqlx::query("UPDATE runs SET revision = revision + 1 WHERE schedule_id = $1")
        .bind(stale.id)
        .execute(&store.pool)
        .await
        .unwrap();
    let valid = pending(&store, "valid", 1).await;
    let claimed = store.claim().await.unwrap().unwrap();
    assert_eq!(claimed.schedule_id, valid.id);
    assert!(store.finish(&claimed, success()).await.unwrap());
    assert!(store.claim().await.unwrap().is_none());

    let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM attempts")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(
        attempts, 2,
        "only admitted runs may create delivery attempts"
    );
    assert_eq!(
        store
            .tenant_capacity("invalid-rate")
            .await
            .unwrap()
            .available_tokens,
        Some(0.0)
    );
    assert_eq!(
        store
            .tenant_capacity("invalid-capacity")
            .await
            .unwrap()
            .running,
        1
    );
    store
        .transition("invalid-paused", paused.id, "resume")
        .await
        .unwrap();
    assert_eq!(store.claim().await.unwrap().unwrap().schedule_id, paused.id);
}
