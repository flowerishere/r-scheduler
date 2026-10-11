use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use scheduler_service::{
    api::{self, AppState},
    evaluator::Evaluator,
    store::StoreError,
};
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
use tower::ServiceExt;
// Database-wide admission tests with isolated PostgreSQL.
use chrono::{DateTime, Utc};
use scheduler_service::{
    dispatch::TenantLimits,
    domain::{DeliveryResult, Schedule, ScheduleSpec},
    store::Store,
};
use serde_json::json;
use sqlx::PgPool;
use std::collections::BTreeSet;
async fn batch(store: &Store, tenant: &str, count: usize, first: DateTime<Utc>) -> Schedule {
    let spec: ScheduleSpec = serde_json::from_value(json!({
        "name": "dispatch test", "trigger": {"type":"once", "at":first},
        "target": {"url":"http://127.0.0.1:9/hook", "timeout_seconds":1}
    }))
    .unwrap();
    let schedule = store
        .create(tenant, spec, first, None, "test")
        .await
        .unwrap();
    let dates: Vec<_> = (0..count)
        .map(|n| first + chrono::Duration::microseconds(n as i64))
        .collect();
    assert!(store.materialize(&schedule, &dates, None).await.unwrap());
    schedule
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
async fn ready_tenants_rotate_despite_an_older_tenant_backlog(pool: PgPool) {
    let store = Store { pool };
    let now = store.now().await.unwrap();
    batch(&store, "a", 100, now - chrono::Duration::hours(1)).await;
    batch(&store, "b", 3, now - chrono::Duration::seconds(1)).await;
    let mut order = Vec::new();
    let mut seen = BTreeSet::new();
    for _ in 0..6 {
        let run = store.claim().await.unwrap().unwrap();
        assert!(seen.insert(run.id));
        order.push(run.tenant_id.clone());
        store.finish(&run, success()).await.unwrap();
    }
    assert_eq!(order, ["a", "b", "a", "b", "a", "b"]);
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn distributed_concurrency_limits_survive_new_store_instances_and_lowering(pool: PgPool) {
    let store = Store { pool };
    batch(
        &store,
        "a",
        30,
        store.now().await.unwrap() - chrono::Duration::minutes(1),
    )
    .await;
    let policy = TenantLimits {
        max_concurrent: Some(3),
        ..TenantLimits::default()
    };
    store.set_tenant_limits("a", &policy).await.unwrap();
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let worker = Store {
            pool: store.pool.clone(),
        };
        workers.spawn(async move {
            for _ in 0..10 {
                if let Some(run) = worker.claim().await.unwrap() {
                    return Some(run);
                }
                tokio::task::yield_now().await;
            }
            None
        });
    }
    let mut claimed = Vec::new();
    while let Some(result) = workers.join_next().await {
        if let Some(run) = result.unwrap() {
            claimed.push(run);
        }
    }
    assert_eq!(claimed.len(), 3);
    assert_eq!(store.tenant_capacity("a").await.unwrap().running, 3);
    store
        .set_tenant_limits(
            "a",
            &TenantLimits {
                max_concurrent: Some(1),
                ..policy
            },
        )
        .await
        .unwrap();
    assert!(store.claim().await.unwrap().is_none());
    for run in &claimed[..2] {
        store.finish(run, success()).await.unwrap();
    }
    assert!(store.claim().await.unwrap().is_none());
    store.finish(&claimed[2], success()).await.unwrap();
    assert!(
        Store {
            pool: store.pool.clone()
        }
        .claim()
        .await
        .unwrap()
        .is_some()
    );
    assert!(store.claim().await.unwrap().is_none());
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn shared_rate_bucket_is_durable_refills_and_does_not_reset_on_policy_reapply(pool: PgPool) {
    let store = Store { pool };
    batch(
        &store,
        "a",
        30,
        store.now().await.unwrap() - chrono::Duration::minutes(1),
    )
    .await;
    let limits = TenantLimits {
        max_concurrent: None,
        rate_per_second: Some(2),
        burst: 3,
    };
    store.set_tenant_limits("a", &limits).await.unwrap();
    // A future refill time deterministically freezes accrual without sleeping.
    // It also exercises conservative behavior when the database clock goes back.
    sqlx::query("UPDATE tenant_dispatch SET token_time = clock_timestamp() + INTERVAL '1 hour' WHERE tenant_id = 'a'")
        .execute(&store.pool).await.unwrap();
    for _ in 0..3 {
        let run = Store {
            pool: store.pool.clone(),
        }
        .claim()
        .await
        .unwrap()
        .unwrap();
        store.finish(&run, success()).await.unwrap();
    }
    assert!(store.claim().await.unwrap().is_none());
    assert_eq!(
        store.tenant_capacity("a").await.unwrap().available_tokens,
        Some(0.0)
    );
    store.set_tenant_limits("a", &limits).await.unwrap();
    assert!(
        store.claim().await.unwrap().is_none(),
        "reapplying the policy must not mint a new burst"
    );
    sqlx::query("UPDATE tenant_dispatch SET token_time = clock_timestamp() - INTERVAL '2 seconds' WHERE tenant_id = 'a'")
        .execute(&store.pool).await.unwrap();
    assert_eq!(
        store.tenant_capacity("a").await.unwrap().available_tokens,
        Some(3.0)
    );
    assert!(store.claim().await.unwrap().is_some());
    // Changing rates preserves old-rate accrued tokens, rather than refilling.
    sqlx::query("UPDATE tenant_dispatch SET tokens = 0.25, token_time = clock_timestamp() + INTERVAL '1 hour' WHERE tenant_id = 'a'")
        .execute(&store.pool).await.unwrap();
    let changed = TenantLimits {
        rate_per_second: Some(100),
        burst: 10,
        ..limits
    };
    let capacity = store.set_tenant_limits("a", &changed).await.unwrap();
    assert_eq!(capacity.available_tokens, Some(0.25));
    assert!(store.claim().await.unwrap().is_none());
    store
        .set_tenant_limits("a", &TenantLimits::default())
        .await
        .unwrap();
    assert!(store.claim().await.unwrap().is_some());
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn parallel_claims_do_not_overspend_rate_tokens(pool: PgPool) {
    let store = Store { pool };
    batch(
        &store,
        "a",
        20,
        store.now().await.unwrap() - chrono::Duration::minutes(1),
    )
    .await;
    store
        .set_tenant_limits(
            "a",
            &TenantLimits {
                rate_per_second: Some(1),
                burst: 2,
                ..TenantLimits::default()
            },
        )
        .await
        .unwrap();
    sqlx::query("UPDATE tenant_dispatch SET token_time = clock_timestamp() + INTERVAL '1 hour'")
        .execute(&store.pool)
        .await
        .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let worker = store.clone();
        tasks.spawn(async move {
            for _ in 0..8 {
                if let Some(run) = worker.claim().await.unwrap() {
                    return Some(run);
                }
                tokio::task::yield_now().await;
            }
            None
        });
    }
    let mut claimed = Vec::new();
    while let Some(result) = tasks.join_next().await {
        if let Some(run) = result.unwrap() {
            claimed.push(run);
        }
    }
    assert_eq!(claimed.len(), 2);
    assert_eq!(
        store.tenant_capacity("a").await.unwrap().available_tokens,
        Some(0.0)
    );
    let attempts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attempts")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(attempts, 2);
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn failed_claim_transaction_does_not_consume_tokens_or_create_a_lease(pool: PgPool) {
    let store = Store { pool };
    batch(
        &store,
        "a",
        1,
        store.now().await.unwrap() - chrono::Duration::minutes(1),
    )
    .await;
    store
        .set_tenant_limits(
            "a",
            &TenantLimits {
                rate_per_second: Some(1),
                burst: 1,
                ..TenantLimits::default()
            },
        )
        .await
        .unwrap();
    sqlx::raw_sql("CREATE FUNCTION reject_attempt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'intentional test failure'; END $$; CREATE TRIGGER fail_attempt BEFORE INSERT ON attempts FOR EACH ROW EXECUTE FUNCTION reject_attempt();").execute(&store.pool).await.unwrap();
    assert!(store.claim().await.is_err());
    let state = store.tenant_capacity("a").await.unwrap();
    assert_eq!(state.running, 0);
    assert_eq!(state.available_tokens, Some(1.0));
    sqlx::query("DROP TRIGGER fail_attempt ON attempts")
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(store.claim().await.unwrap().is_some());
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn lease_recovery_releases_capacity_without_refunding_consumed_tokens(pool: PgPool) {
    let store = Store { pool };
    batch(
        &store,
        "a",
        3,
        store.now().await.unwrap() - chrono::Duration::minutes(1),
    )
    .await;
    store
        .set_tenant_limits(
            "a",
            &TenantLimits {
                max_concurrent: Some(1),
                rate_per_second: Some(1),
                burst: 2,
            },
        )
        .await
        .unwrap();
    sqlx::query("UPDATE tenant_dispatch SET token_time = clock_timestamp() + INTERVAL '1 hour'")
        .execute(&store.pool)
        .await
        .unwrap();
    let first = store.claim().await.unwrap().unwrap();
    assert!(store.claim().await.unwrap().is_none());
    sqlx::query(
        "UPDATE runs SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
    )
    .bind(first.id)
    .execute(&store.pool)
    .await
    .unwrap();
    assert_eq!(store.recover_expired().await.unwrap(), 1);
    assert_eq!(store.tenant_capacity("a").await.unwrap().running, 0);
    assert_eq!(
        store.tenant_capacity("a").await.unwrap().available_tokens,
        Some(1.0)
    );
    assert!(!store.finish(&first, success()).await.unwrap());
    let second = store.claim().await.unwrap().unwrap();
    store.finish(&second, success()).await.unwrap();
    assert!(store.claim().await.unwrap().is_none());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn capacity_api_is_tenant_scoped_read_only_and_has_unlimited_defaults(pool: PgPool) {
    let store = Store { pool };
    let app = api::router(AppState::new(
        store.clone(),
        Evaluator::new("unused".into(), Duration::from_secs(1)),
        &BTreeMap::from([
            ("a".into(), "tenant-a-test-secret".into()),
            ("b".into(), "tenant-b-test-secret".into()),
        ]),
    ));
    store
        .set_tenant_limits(
            "a",
            &TenantLimits {
                max_concurrent: Some(2),
                rate_per_second: Some(3),
                burst: 4,
            },
        )
        .await
        .unwrap();
    for (key, tenant, cap) in [
        ("tenant-a-test-secret", "a", json!(2)),
        ("tenant-b-test-secret", "b", Value::Null),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/capacity?tenant_id=a")
                    .header("authorization", format!("Bearer {key}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-store");
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(body["tenant_id"], tenant);
        assert_eq!(body["max_concurrent"], cap);
    }
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/capacity")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/capacity")
                .header("authorization", "Bearer tenant-a-test-secret")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap()
        .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    let policies: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tenant_dispatch")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(
        policies, 1,
        "reading an unused tenant must not create persistent state"
    );
    assert!(matches!(
        store
            .set_tenant_limits(
                "a",
                &TenantLimits {
                    max_concurrent: Some(0),
                    ..TenantLimits::default()
                }
            )
            .await,
        Err(StoreError::InvalidInput(_))
    ));
    assert_eq!(
        store.tenant_capacity("a").await.unwrap().max_concurrent,
        Some(2)
    );
    let metrics = store.metrics("a").await.unwrap();
    assert!(metrics.contains("scheduler_delivery_concurrency_limit 2\n"));
    assert!(metrics.contains("scheduler_delivery_rate_limit 3\n"));
    assert!(metrics.contains("scheduler_delivery_burst_limit 4\n"));
    let other = store.metrics("b").await.unwrap();
    assert!(other.contains("scheduler_delivery_concurrency_limit 0\n"));
    assert!(other.contains("scheduler_delivery_rate_limit 0\n"));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn tenant_cli_provisions_limits_before_first_schedule_and_validates_input(pool: PgPool) {
    use sqlx::ConnectOptions;
    let store = Store { pool };
    let url = store.pool.connect_options().to_url_lossy().to_string();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_scheduler-service"))
        .args([
            "tenant",
            "set",
            "cli-tenant",
            "--max-concurrent",
            "2",
            "--rate-per-second",
            "1",
            "--burst",
            "2",
        ])
        .env_clear()
        .env("DATABASE_URL", &url)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let capacity: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(capacity["max_concurrent"], 2);
    assert_eq!(capacity["running"], 0);
    batch(
        &store,
        "cli-tenant",
        5,
        store.now().await.unwrap() - chrono::Duration::minutes(1),
    )
    .await;
    let policy = store.tenant_capacity("cli-tenant").await.unwrap();
    assert_eq!(policy.max_concurrent, Some(2));
    assert_eq!(policy.burst, 2);
    let show = tokio::process::Command::new(env!("CARGO_BIN_EXE_scheduler-service"))
        .args(["tenant", "show", "cli-tenant"])
        .env_clear()
        .env("DATABASE_URL", &url)
        .output()
        .await
        .unwrap();
    assert!(show.status.success());
    let shown: Value = serde_json::from_slice(&show.stdout).unwrap();
    assert_eq!(shown["rate_per_second"], 1);
    let invalid = tokio::process::Command::new(env!("CARGO_BIN_EXE_scheduler-service"))
        .args(["tenant", "set", "cli-tenant", "--max-concurrent", "0"])
        .env_clear()
        .env("DATABASE_URL", &url)
        .output()
        .await
        .unwrap();
    assert!(!invalid.status.success());
    assert_eq!(
        store
            .tenant_capacity("cli-tenant")
            .await
            .unwrap()
            .max_concurrent,
        Some(2)
    );
}
