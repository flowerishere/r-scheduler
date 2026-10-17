//! Admission regressions; scripts/test.sh supplies isolated PostgreSQL databases.
use chrono::{DateTime, Duration, Utc};
use scheduler_service::{
    dispatch::TenantLimits,
    domain::{Schedule, ScheduleSpec},
    store::Store,
};
use serde_json::json;
use sqlx::PgPool;

async fn pending_schedule(store: &Store) -> Schedule {
    let at = store.now().await.unwrap() - Duration::seconds(1);
    let spec: ScheduleSpec = serde_json::from_value(json!({
        "name": "admission optimization regression",
        "trigger": { "type": "once", "at": at },
        "target": { "url": "https://example.invalid/hook" }
    }))
    .unwrap();
    let schedule = store
        .create("tenant-a", spec, at, None, "test")
        .await
        .unwrap();
    assert!(store.materialize(&schedule, &[at], None).await.unwrap());
    schedule
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn legacy_run_without_timeout_uses_default_delivery_lease(pool: PgPool) {
    let store = Store { pool };
    pending_schedule(&store).await;
    // Stored snapshots from older writers may omit fields that serde defaults.
    sqlx::query("UPDATE runs SET spec = spec #- '{target,timeout_seconds}'")
        .execute(&store.pool)
        .await
        .unwrap();
    let missing_timeout: bool =
        sqlx::query_scalar("SELECT NOT (spec -> 'target' ? 'timeout_seconds') FROM runs")
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert!(missing_timeout);

    let before = store.now().await.unwrap();
    let run = store.claim().await.unwrap().unwrap();
    let after = store.now().await.unwrap();
    assert_eq!(run.spec.target.timeout_seconds, 30);
    let lease_until = run.lease_until.unwrap();
    assert!(lease_until >= before + Duration::seconds(45));
    assert!(lease_until <= after + Duration::seconds(45));
    assert_eq!(run.status, "running");
    assert_eq!(run.attempt_count, 1);
    assert_eq!(store.attempts("tenant-a", run.id).await.unwrap().len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn token_write_failure_rolls_back_the_entire_claim(pool: PgPool) {
    let store = Store { pool };
    let schedule = pending_schedule(&store).await;
    store
        .set_tenant_limits(
            "tenant-a",
            &TenantLimits {
                rate_per_second: Some(1),
                burst: 1,
                ..TenantLimits::default()
            },
        )
        .await
        .unwrap();
    // Freeze refill so token balance and token_time have exact expected values.
    sqlx::query("UPDATE tenant_dispatch SET token_time = clock_timestamp() + INTERVAL '1 hour'")
        .execute(&store.pool)
        .await
        .unwrap();
    let before: (f64, i64, DateTime<Utc>) = sqlx::query_as(
        "SELECT tokens, last_served, token_time FROM tenant_dispatch WHERE tenant_id = 'tenant-a'",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap();
    sqlx::raw_sql(
        "CREATE FUNCTION reject_token_charge() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'intentional token write failure'; END $$;
         CREATE TRIGGER fail_token_charge BEFORE UPDATE ON tenant_dispatch
            FOR EACH ROW WHEN (NEW.tokens < OLD.tokens)
            EXECUTE FUNCTION reject_token_charge();",
    )
    .execute(&store.pool)
    .await
    .unwrap();

    let error = store.claim().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("intentional token write failure")
    );
    let after: (f64, i64, DateTime<Utc>) = sqlx::query_as(
        "SELECT tokens, last_served, token_time FROM tenant_dispatch WHERE tenant_id = 'tenant-a'",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(after, before);
    let runs = store
        .list_runs("tenant-a", Some(schedule.id), None, 10, 0)
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);
    let pending = &runs[0];
    assert_eq!(pending.status, "pending");
    assert_eq!(pending.attempt_count, 0);
    assert_eq!(pending.cycle_attempts, 0);
    assert!(pending.lease_token.is_none());
    assert!(pending.lease_until.is_none());
    assert!(
        store
            .attempts("tenant-a", pending.id)
            .await
            .unwrap()
            .is_empty()
    );

    sqlx::raw_sql(
        "DROP TRIGGER fail_token_charge ON tenant_dispatch;
         DROP FUNCTION reject_token_charge();",
    )
    .execute(&store.pool)
    .await
    .unwrap();
    let run = store.claim().await.unwrap().unwrap();
    assert_eq!(run.id, pending.id);
    assert_eq!(run.status, "running");
    assert_eq!(run.attempt_count, 1);
    assert_eq!(run.cycle_attempts, 1);
    let attempts = store.attempts("tenant-a", run.id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].number, 1);
    assert_eq!(attempts[0].status, "running");
    assert_eq!(Some(attempts[0].lease_token), run.lease_token);
    let final_state: (f64, i64, DateTime<Utc>) = sqlx::query_as(
        "SELECT tokens, last_served, token_time FROM tenant_dispatch WHERE tenant_id = 'tenant-a'",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(final_state.0, 0.0);
    assert!(final_state.1 > before.1);
    assert_eq!(final_state.2, before.2);
    assert!(store.claim().await.unwrap().is_none());
}
