use std::time::Duration;

use scheduler_service::{domain::Trigger, evaluator::Evaluator};

fn evaluator() -> Evaluator {
    Evaluator::new(
        env!("CARGO_BIN_EXE_scheduler-service").into(),
        Duration::from_secs(5),
    )
}

#[tokio::test]
async fn evaluator_uses_real_subprocess() {
    let result = evaluator()
        .next(
            &Trigger::Rrule {
                value: "DTSTART:20260921T090000Z\nRRULE:FREQ=DAILY;COUNT=2".into(),
            },
            "2026-09-20T00:00:00Z".parse().unwrap(),
            10,
        )
        .await
        .unwrap();
    assert_eq!(result.dates.len(), 2);
    assert!(result.exhausted);
}

#[tokio::test]
async fn expensive_rules_are_terminated_by_process_timeout() {
    let evaluator = Evaluator::new(
        env!("CARGO_BIN_EXE_scheduler-service").into(),
        Duration::from_millis(5),
    );
    let started = std::time::Instant::now();
    let error = evaluator
        .next(
            &Trigger::Rrule {
                value: "DTSTART:19000101T000000Z\nRRULE:FREQ=SECONDLY".into(),
            },
            "2026-09-21T00:00:00Z".parse().unwrap(),
            1,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[sqlx::test(migrations = false)]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn migrate_command_and_store_apply_initial_schema_idempotently(pool: sqlx::PgPool) {
    use scheduler_service::store::Store;
    use sqlx::ConnectOptions;

    let url = pool.connect_options().to_url_lossy().to_string();
    for _ in 0..2 {
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_scheduler-service"))
            .arg("migrate")
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
    }
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(versions, [1]);
    let store = Store::connect(&url).await.unwrap();
    let statement_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let lock_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(statement_timeout, "15s");
    assert_eq!(lock_timeout, "5s");
    let now = store.now().await.unwrap();
    assert!((chrono::Utc::now() - now).num_seconds().abs() < 5);
    store.pool.close().await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn initial_schema_enforces_tenants_occurrences_and_leases(pool: sqlx::PgPool) {
    use scheduler_service::domain::{Attempt, Run, Schedule, ScheduleSpec};
    use serde_json::json;
    use sqlx::types::Json;
    use uuid::Uuid;

    fn code(error: sqlx::Error, expected: &str) {
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some(expected)
        );
    }
    let at: chrono::DateTime<chrono::Utc> = "2030-01-01T00:00:00Z".parse().unwrap();
    let spec: ScheduleSpec = serde_json::from_value(json!({"name":"schema fixture", "trigger":{"type":"once","at":at}, "target":{"url":"https://example.invalid/hook"}})).unwrap();
    let id = Uuid::new_v4();
    let insert = "INSERT INTO schedules (id,tenant_id,spec,status,next_fire_at,idempotency_key,request_hash) VALUES ($1,$2,$3,'active',$4,'shared-key','fixture') RETURNING *";
    let schedule: Schedule = sqlx::query_as(insert)
        .bind(id)
        .bind("a")
        .bind(Json(&spec))
        .bind(at)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(schedule.revision, 1);
    code(
        sqlx::query(insert)
            .bind(Uuid::new_v4())
            .bind("a")
            .bind(Json(&spec))
            .bind(at)
            .execute(&pool)
            .await
            .unwrap_err(),
        "23505",
    );
    sqlx::query(insert)
        .bind(Uuid::new_v4())
        .bind("b")
        .bind(Json(&spec))
        .bind(at)
        .execute(&pool)
        .await
        .unwrap();
    let run_id = Uuid::new_v4();
    let insert_run = "INSERT INTO runs (id,schedule_id,tenant_id,revision,scheduled_at,available_at,status,spec) VALUES ($1,$2,$3,1,$4,$4,'pending',$5) RETURNING *";
    code(
        sqlx::query(insert_run)
            .bind(Uuid::new_v4())
            .bind(id)
            .bind("b")
            .bind(at)
            .bind(Json(&spec))
            .execute(&pool)
            .await
            .unwrap_err(),
        "23503",
    );
    let run: Run = sqlx::query_as(insert_run)
        .bind(run_id)
        .bind(id)
        .bind("a")
        .bind(at)
        .bind(Json(&spec))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(run.attempt_count, 0);
    assert!(run.lease_token.is_none());
    code(
        sqlx::query(insert_run)
            .bind(Uuid::new_v4())
            .bind(id)
            .bind("a")
            .bind(at)
            .bind(Json(&spec))
            .execute(&pool)
            .await
            .unwrap_err(),
        "23505",
    );
    code(
        sqlx::query("UPDATE runs SET status='running' WHERE id=$1")
            .bind(run_id)
            .execute(&pool)
            .await
            .unwrap_err(),
        "23514",
    );
    code(
        sqlx::query("UPDATE runs SET status='unknown' WHERE id=$1")
            .bind(run_id)
            .execute(&pool)
            .await
            .unwrap_err(),
        "23514",
    );
    let token = Uuid::new_v4();
    sqlx::query("UPDATE runs SET status='running',lease_token=$2,lease_until=clock_timestamp()+INTERVAL '30 seconds' WHERE id=$1").bind(run_id).bind(token).execute(&pool).await.unwrap();
    let attempt_sql = "INSERT INTO attempts (id,run_id,number,status,lease_token) VALUES ($1,$2,$3,'running',$4) RETURNING *";
    let attempt: Attempt = sqlx::query_as(attempt_sql)
        .bind(Uuid::new_v4())
        .bind(run_id)
        .bind(1)
        .bind(token)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(attempt.number, 1);
    code(
        sqlx::query(attempt_sql)
            .bind(Uuid::new_v4())
            .bind(run_id)
            .bind(1)
            .bind(Uuid::new_v4())
            .execute(&pool)
            .await
            .unwrap_err(),
        "23505",
    );
    code(
        sqlx::query(attempt_sql)
            .bind(Uuid::new_v4())
            .bind(run_id)
            .bind(2)
            .bind(token)
            .execute(&pool)
            .await
            .unwrap_err(),
        "23505",
    );
}

fn schedule_spec(at: chrono::DateTime<chrono::Utc>) -> scheduler_service::domain::ScheduleSpec {
    serde_json::from_value(serde_json::json!({
        "name": "test job",
        "trigger": {"type": "once", "at": at},
        "target": {"url": "https://example.invalid/hook"},
        "payload": {"order_id": "order-42"}
    }))
    .unwrap()
}

async fn seed_run(
    pool: &sqlx::PgPool,
    schedule: &scheduler_service::domain::Schedule,
    seconds: i64,
    running: bool,
) -> scheduler_service::domain::Run {
    let at = schedule.next_fire_at.unwrap() + chrono::Duration::seconds(seconds);
    sqlx::query_as("INSERT INTO runs (id,schedule_id,tenant_id,revision,scheduled_at,available_at,status,spec,lease_token,lease_until) VALUES ($1,$2,$3,$4,$5,$5,$6,$7,$8,$9) RETURNING *")
        .bind(uuid::Uuid::new_v4()).bind(schedule.id).bind(&schedule.tenant_id)
        .bind(schedule.revision).bind(at).bind(if running { "running" } else { "pending" })
        .bind(&schedule.spec).bind(running.then(uuid::Uuid::new_v4))
        .bind(running.then_some(at + chrono::Duration::seconds(30)))
        .fetch_one(pool).await.unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn concurrent_creates_share_idempotency_only_within_the_tenant(pool: sqlx::PgPool) {
    use scheduler_service::store::{Store, StoreError};
    let store = Store { pool };
    let at = store.now().await.unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(12));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let store = store.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            store
                .create("a", schedule_spec(at), at, Some("shared-key"), "original")
                .await
                .unwrap()
        });
    }
    let mut ids = std::collections::BTreeSet::new();
    while let Some(result) = tasks.join_next().await {
        ids.insert(result.unwrap().id);
    }
    assert_eq!(ids.len(), 1);
    let id = *ids.first().unwrap();
    assert_eq!(
        store
            .find_idempotent("a", "shared-key", "original")
            .await
            .unwrap()
            .unwrap()
            .id,
        id
    );
    assert!(
        store
            .find_idempotent("a", "missing", "original")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .find_idempotent("b", "shared-key", "original")
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store.find_idempotent("a", "shared-key", "changed").await,
        Err(StoreError::Conflict(_))
    ));
    let mut changed = schedule_spec(at);
    changed.name = "different request".into();
    assert!(matches!(
        store
            .create("a", changed, at, Some("shared-key"), "changed")
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(store.list_schedules("a", 100, 0).await.unwrap().len(), 1);
    let other = store
        .create("b", schedule_spec(at), at, Some("shared-key"), "changed")
        .await
        .unwrap();
    assert_ne!(other.id, id);
    assert!(matches!(
        store.get_schedule("b", id).await,
        Err(StoreError::NotFound)
    ));
    let first = store
        .create("a", schedule_spec(at), at, None, "original")
        .await
        .unwrap();
    let second = store
        .create("a", schedule_spec(at), at, None, "original")
        .await
        .unwrap();
    assert_ne!(first.id, second.id);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn schedule_pages_are_bounded_stable_and_tenant_scoped(pool: sqlx::PgPool) {
    use scheduler_service::store::Store;
    let store = Store { pool };
    let at = store.now().await.unwrap();
    let mut expected = Vec::new();
    for _ in 0..101 {
        expected.push(
            store
                .create("a", schedule_spec(at), at, None, "test")
                .await
                .unwrap()
                .id,
        );
    }
    store
        .create("b", schedule_spec(at), at, None, "test")
        .await
        .unwrap();
    sqlx::query("UPDATE schedules SET created_at = $1")
        .bind(at)
        .execute(&store.pool)
        .await
        .unwrap();
    expected.sort_unstable_by(|a, b| b.cmp(a));
    let first = store.list_schedules("a", 50, 0).await.unwrap();
    let second = store.list_schedules("a", 50, 50).await.unwrap();
    let last = store.list_schedules("a", 50, 100).await.unwrap();
    let actual: Vec<_> = first
        .iter()
        .chain(&second)
        .chain(&last)
        .map(|s| s.id)
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(
        store.list_schedules("a", i64::MAX, 0).await.unwrap().len(),
        100
    );
    let minimum = store.list_schedules("a", 0, -10).await.unwrap();
    assert_eq!(minimum.len(), 1);
    assert_eq!(minimum[0].id, expected[0]);
    assert!(
        store
            .list_schedules("a", 10, i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.list_schedules("b", 100, 0).await.unwrap().len(), 1);
    assert!(
        store
            .list_schedules("unknown", 100, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn replacement_is_tenant_scoped_and_invalidates_only_pending_work(pool: sqlx::PgPool) {
    use scheduler_service::{
        domain::Run,
        store::{Store, StoreError},
    };
    let store = Store { pool };
    let at = store.now().await.unwrap();
    let schedule = store
        .create("a", schedule_spec(at), at, Some("creation-key"), "original")
        .await
        .unwrap();
    let pending = seed_run(&store.pool, &schedule, 0, false).await;
    let running = seed_run(&store.pool, &schedule, 1, true).await;
    let other = store
        .create("a", schedule_spec(at), at, None, "other")
        .await
        .unwrap();
    let other_pending = seed_run(&store.pool, &other, 0, false).await;
    let next = at + chrono::Duration::hours(1);
    let mut spec = schedule_spec(next);
    spec.name = "replacement".into();
    spec.payload = serde_json::json!({"replacement": true});
    spec.target.url = "https://example.invalid/new-hook".into();
    spec.retry.max_attempts = 2;
    assert!(matches!(
        store.replace("b", schedule.id, 1, spec.clone(), next).await,
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        store.replace("a", schedule.id, 0, spec.clone(), next).await,
        Err(StoreError::Conflict(_))
    ));
    let status: String = sqlx::query_scalar("SELECT status FROM runs WHERE id=$1")
        .bind(pending.id)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(status, "pending");
    let updated = store
        .replace("a", schedule.id, 1, spec.clone(), next)
        .await
        .unwrap();
    assert_eq!(updated.id, schedule.id);
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.next_fire_at, Some(next));
    assert_eq!(
        serde_json::to_value(&updated.spec.0).unwrap(),
        serde_json::to_value(&spec).unwrap()
    );
    assert_eq!(updated.request_hash, "original");
    assert_eq!(updated.idempotency_key.as_deref(), Some("creation-key"));
    assert!(matches!(
        store
            .replace("a", schedule.id, 1, schedule_spec(at), at)
            .await,
        Err(StoreError::Conflict(_))
    ));
    let get_run = |id| {
        sqlx::query_as::<_, Run>("SELECT * FROM runs WHERE id=$1")
            .bind(id)
            .fetch_one(&store.pool)
    };
    let cancelled = get_run(pending.id).await.unwrap();
    assert_eq!(cancelled.status, "cancelled");
    assert!(cancelled.finished_at.is_some());
    assert_eq!(cancelled.last_error.as_deref(), Some("Schedule revised"));
    assert_eq!(cancelled.revision, 1);
    assert_eq!(cancelled.spec.name, schedule.spec.name);
    let in_flight = get_run(running.id).await.unwrap();
    assert_eq!(in_flight.status, "running");
    assert_eq!(in_flight.lease_token, running.lease_token);
    assert_eq!(get_run(other_pending.id).await.unwrap().status, "pending");
    let retried_create = store
        .create("a", schedule_spec(at), at, Some("creation-key"), "original")
        .await
        .unwrap();
    assert_eq!(retried_create.id, schedule.id);
    assert_eq!(retried_create.revision, 2);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn competing_replacements_cannot_overwrite_a_new_revision(pool: sqlx::PgPool) {
    use scheduler_service::store::{Store, StoreError};
    let store = Store { pool };
    let at = store.now().await.unwrap();
    let schedule = store
        .create("a", schedule_spec(at), at, None, "test")
        .await
        .unwrap();
    let mut first_spec = schedule_spec(at);
    first_spec.name = "first writer".into();
    let mut second_spec = schedule_spec(at);
    second_spec.name = "second writer".into();
    let (first, second) = tokio::join!(
        store.replace("a", schedule.id, 1, first_spec, at),
        store.replace("a", schedule.id, 1, second_spec, at)
    );
    let winner = match (first, second) {
        (Ok(winner), Err(StoreError::Conflict(_))) | (Err(StoreError::Conflict(_)), Ok(winner)) => {
            winner
        }
        other => panic!("expected exactly one winner: {other:?}"),
    };
    let saved = store.get_schedule("a", schedule.id).await.unwrap();
    assert_eq!(saved.revision, 2);
    assert_eq!(saved.spec.name, winner.spec.name);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn replacement_preserves_pause_rejects_cancellation_and_rolls_back_on_failure(
    pool: sqlx::PgPool,
) {
    use scheduler_service::store::{Store, StoreError};
    let store = Store { pool };
    let at = store.now().await.unwrap();
    for (old_status, expected) in [
        ("active", "active"),
        ("paused", "paused"),
        ("completed", "active"),
        ("error", "active"),
    ] {
        let schedule = store
            .create("a", schedule_spec(at), at, None, "test")
            .await
            .unwrap();
        sqlx::query("UPDATE schedules SET status=$2,last_error='old error' WHERE id=$1")
            .bind(schedule.id)
            .bind(old_status)
            .execute(&store.pool)
            .await
            .unwrap();
        let updated = store
            .replace("a", schedule.id, 1, schedule_spec(at), at)
            .await
            .unwrap();
        assert_eq!(updated.status, expected);
        assert!(updated.last_error.is_none());
    }
    let schedule = store
        .create("a", schedule_spec(at), at, None, "test")
        .await
        .unwrap();
    let pending = seed_run(&store.pool, &schedule, 0, false).await;
    sqlx::query("UPDATE schedules SET status='cancelled' WHERE id=$1")
        .bind(schedule.id)
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .replace("a", schedule.id, 1, schedule_spec(at), at)
            .await,
        Err(StoreError::Conflict(_))
    ));
    sqlx::query("UPDATE schedules SET status='active',revision=$2 WHERE id=$1")
        .bind(schedule.id)
        .bind(i64::MAX)
        .execute(&store.pool)
        .await
        .unwrap();
    // The revision increment fails after pending runs were updated: neither
    // update may escape the transaction.
    assert!(matches!(
        store
            .replace("a", schedule.id, i64::MAX, schedule_spec(at), at)
            .await,
        Err(StoreError::Database(_))
    ));
    let saved = store.get_schedule("a", schedule.id).await.unwrap();
    assert_eq!(saved.revision, i64::MAX);
    let run: scheduler_service::domain::Run = sqlx::query_as("SELECT * FROM runs WHERE id=$1")
        .bind(pending.id)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(run.status, "pending");
    assert!(run.finished_at.is_none());
    assert!(run.last_error.is_none());
}

mod api_tests {
    use super::{evaluator, schedule_spec as spec};
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
    };
    use chrono::Utc;
    use http_body_util::BodyExt;
    use scheduler_service::{
        api::{self, AppState},
        domain::Trigger,
        store::Store,
    };
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use std::collections::BTreeMap;
    use tower::ServiceExt;
    fn app(store: Store) -> Router {
        api::router(AppState::new(
            store,
            evaluator(),
            &BTreeMap::from([
                ("tenant-a".into(), "tenant-a-test-secret".into()),
                ("tenant-b".into(), "tenant-b-test-secret".into()),
            ]),
        ))
    }
    async fn request(
        app: &Router,
        method: &str,
        uri: &str,
        key: Option<&str>,
        body: Value,
        idempotency: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(key) = key {
            builder = builder.header("authorization", format!("Bearer {key}"));
        }
        if let Some(key) = idempotency {
            builder = builder.header("idempotency-key", key);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&body)
            .unwrap_or_else(|_| json!({"raw": String::from_utf8_lossy(&body)}));
        (status, value)
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn api_auth_tenant_isolation_and_idempotent_delay(pool: PgPool) {
        let store = Store { pool };
        let app = app(store.clone());
        assert_eq!(
            request(&app, "GET", "/v1/schedules", None, Value::Null, None)
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        let mut spec = spec(Utc::now());
        spec.trigger = Trigger::Delay { seconds: 30 };
        let body = serde_json::to_value(&spec).unwrap();
        let (status, created) = request(
            &app,
            "POST",
            "/v1/schedules",
            Some("tenant-a-test-secret"),
            body.clone(),
            Some("order-42"),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        assert_eq!(created["spec"]["trigger"]["type"], "once");
        let (_, repeated) = request(
            &app,
            "POST",
            "/v1/schedules",
            Some("tenant-a-test-secret"),
            body.clone(),
            Some("order-42"),
        )
        .await;
        assert_eq!(created["id"], repeated["id"]);
        assert_eq!(created["next_fire_at"], repeated["next_fire_at"]);
        let mut changed = body;
        changed["name"] = json!("different request");
        assert_eq!(
            request(
                &app,
                "POST",
                "/v1/schedules",
                Some("tenant-a-test-secret"),
                changed,
                Some("order-42")
            )
            .await
            .0,
            StatusCode::CONFLICT
        );
        let uri = format!("/v1/schedules/{}", created["id"].as_str().unwrap());
        assert_eq!(
            request(
                &app,
                "GET",
                &uri,
                Some("tenant-b-test-secret"),
                Value::Null,
                None
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        let (_, list) = request(
            &app,
            "GET",
            "/v1/schedules",
            Some("tenant-b-test-secret"),
            Value::Null,
            None,
        )
        .await;
        assert_eq!(list, json!([]));
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn preview_endpoint_and_input_validation(pool: PgPool) {
        let app = app(Store { pool });
        let body = json!({"trigger": {"type": "cron", "expression": "0 9 * * MON-FRI", "timezone": "Asia/Shanghai"}, "after": "2026-09-21T01:00:00Z", "count": 2});
        let (status, preview) = request(
            &app,
            "POST",
            "/v1/preview",
            Some("tenant-a-test-secret"),
            body,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{preview}");
        assert_eq!(preview["dates"][0], "2026-09-22T01:00:00Z");
        let body = json!({"trigger": {"type": "rrule", "value": "DTSTART:20260921T090000\nRRULE:FREQ=DAILY"}});
        assert_eq!(
            request(
                &app,
                "POST",
                "/v1/preview",
                Some("tenant-a-test-secret"),
                body,
                None
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        let mut invalid = spec(Utc::now());
        invalid.retry.max_attempts = 0;
        assert_eq!(
            request(
                &app,
                "POST",
                "/v1/schedules",
                Some("tenant-a-test-secret"),
                serde_json::to_value(invalid).unwrap(),
                None
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        for payload in [
            json!({"nested": ["bad\u{0000}value"]}),
            json!({"bad\u{0000}key": true}),
        ] {
            let mut invalid = spec(Utc::now());
            invalid.payload = payload;
            assert_eq!(
                request(
                    &app,
                    "POST",
                    "/v1/schedules",
                    Some("tenant-a-test-secret"),
                    serde_json::to_value(invalid).unwrap(),
                    None
                )
                .await
                .0,
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            request(
                &app,
                "GET",
                "/v1/schedules?limit=101",
                Some("tenant-a-test-secret"),
                Value::Null,
                None
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn replace_and_validation_do_not_cross_tenant_or_revision_boundaries(pool: PgPool) {
        let store = Store { pool };
        let app = app(store.clone());
        let key = Some("tenant-a-test-secret");
        let (status, created) = request(
            &app,
            "POST",
            "/v1/schedules",
            key,
            serde_json::to_value(spec(Utc::now())).unwrap(),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let uri = format!("/v1/schedules/{}", created["id"].as_str().unwrap());
        let mut replacement = spec(Utc::now());
        replacement.name = "updated".into();
        let body = json!({"expected_revision":1,"spec":replacement});
        assert_eq!(
            request(
                &app,
                "PUT",
                &uri,
                Some("tenant-b-test-secret"),
                body.clone(),
                None
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        let (status, updated) = request(&app, "PUT", &uri, key, body.clone(), None).await;
        assert_eq!(status, StatusCode::OK, "{updated}");
        assert_eq!(updated["revision"], 2);
        assert_eq!(updated["spec"]["name"], "updated");
        assert_eq!(
            request(&app, "PUT", &uri, key, body, None).await.0,
            StatusCode::CONFLICT
        );
        for target in [
            json!({"url":"file:///etc/passwd"}),
            json!({"url":"https://user:password@example.invalid/hook"}),
            json!({"url":"https://example.invalid/hook","headers":{"Idempotency-Key":"override"}}),
        ] {
            let mut invalid = serde_json::to_value(spec(Utc::now())).unwrap();
            invalid["target"] = target;
            assert_eq!(
                request(&app, "POST", "/v1/schedules", key, invalid, None)
                    .await
                    .0,
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            store
                .list_schedules("tenant-a", 100, 0)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            request(&app, "GET", "/health", None, Value::Null, None)
                .await
                .0,
            StatusCode::OK
        );
        assert_eq!(
            request(&app, "GET", "/ready", None, Value::Null, None)
                .await
                .0,
            StatusCode::OK
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn serve_command_exposes_the_authenticated_api_on_tcp(pool: PgPool) {
        use sqlx::ConnectOptions;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_scheduler-service"))
            .arg("serve")
            .env_clear()
            .env(
                "DATABASE_URL",
                pool.connect_options().to_url_lossy().as_str(),
            )
            .env("SCHEDULER_BIND", address.to_string())
            .env(
                "SCHEDULER_API_KEYS",
                r#"{"tenant-a":"tenant-a-test-secret"}"#,
            )
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(1))
            .build()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "serve exited before becoming ready"
                );
                if let Ok(response) = client.get(format!("http://{address}/ready")).send().await
                    && response.status().is_success()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let url = format!("http://{address}/v1/schedules");
        assert_eq!(
            client.get(&url).send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let response = client
            .post(&url)
            .bearer_auth("tenant-a-test-secret")
            .json(&spec(Utc::now()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM schedules WHERE tenant_id='tenant-a'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1);
        child.kill().await.unwrap();
    }
}

mod engine_tests {
    use super::{evaluator, schedule_spec as spec};
    use chrono::{DateTime, Utc};
    use scheduler_service::{
        domain::{MisfirePolicy, Run, Schedule, Trigger},
        engine::{materialize_one, scheduler_tick},
        store::Store,
    };
    use sqlx::PgPool;
    use uuid::Uuid;
    async fn runs(store: &Store, schedule: Uuid) -> Result<Vec<Run>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM runs WHERE schedule_id=$1 ORDER BY scheduled_at,id")
            .bind(schedule)
            .fetch_all(&store.pool)
            .await
    }
    async fn create_due(store: &Store) -> Schedule {
        let at = store.now().await.unwrap() - chrono::Duration::seconds(5);
        store
            .create("tenant-a", spec(at), at, None, "test")
            .await
            .unwrap()
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn atomic_materialization_rejects_stale_cursor(pool: PgPool) {
        let store = Store { pool };
        let schedule = create_due(&store).await;
        let dates = [schedule.next_fire_at.unwrap()];
        let (first, second) = tokio::join!(
            store.materialize(&schedule, &dates, None),
            store.materialize(&schedule, &dates, None)
        );
        assert_ne!(first.unwrap(), second.unwrap());
        let runs = runs(&store, schedule.id).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(
            store
                .get_schedule("tenant-a", schedule.id)
                .await
                .unwrap()
                .status,
            "completed"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn recurrence_misfire_policies(pool: PgPool) {
        let store = Store { pool };
        let now: DateTime<Utc> = "2026-09-21T12:05:30Z".parse().unwrap();
        let first: DateTime<Utc> = "2026-09-21T12:00:00Z".parse().unwrap();
        for (policy, expected) in [
            (MisfirePolicy::Skip, 0),
            (MisfirePolicy::FireOnce, 1),
            (MisfirePolicy::CatchUp, 6),
        ] {
            let mut spec = spec(first);
            spec.trigger = Trigger::Cron {
                expression: "* * * * *".into(),
                timezone: "UTC".into(),
            };
            spec.misfire = policy;
            let schedule = store
                .create("tenant-a", spec, first, None, "test")
                .await
                .unwrap();
            materialize_one(&store, &evaluator(), &schedule, now)
                .await
                .unwrap();
            let runs = runs(&store, schedule.id).await.unwrap();
            assert_eq!(runs.len(), expected, "{policy:?}");
            let next = store
                .get_schedule("tenant-a", schedule.id)
                .await
                .unwrap()
                .next_fire_at
                .unwrap();
            assert_eq!(
                next,
                "2026-09-21T12:06:00Z".parse::<DateTime<Utc>>().unwrap()
            );
        }
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn catch_up_is_bounded_and_keeps_remaining_cursor(pool: PgPool) {
        let store = Store { pool };
        let first: DateTime<Utc> = "2026-09-21T12:00:00Z".parse().unwrap();
        let now = first + chrono::Duration::seconds(150);
        let mut spec = spec(first);
        spec.trigger = Trigger::Rrule {
            value: "DTSTART:20260921T120000Z\nRRULE:FREQ=SECONDLY;COUNT=151".into(),
        };
        spec.misfire = MisfirePolicy::CatchUp;
        let schedule = store
            .create("tenant-a", spec, first, None, "test")
            .await
            .unwrap();
        materialize_one(&store, &evaluator(), &schedule, now)
            .await
            .unwrap();
        let current = store.get_schedule("tenant-a", schedule.id).await.unwrap();
        assert_eq!(
            current.next_fire_at,
            Some(first + chrono::Duration::seconds(100))
        );
        materialize_one(&store, &evaluator(), &current, now)
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE schedule_id = $1")
            .bind(schedule.id)
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(count, 151);
        assert_eq!(
            store
                .get_schedule("tenant-a", schedule.id)
                .await
                .unwrap()
                .status,
            "completed"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn scheduler_tick_materializes_due_work_and_marks_invalid_rules(pool: PgPool) {
        let store = Store { pool };
        let due = create_due(&store).await;
        let later = store.now().await.unwrap() + chrono::Duration::hours(1);
        let future = store
            .create("tenant-a", spec(later), later, None, "future")
            .await
            .unwrap();
        let mut invalid = spec(due.next_fire_at.unwrap());
        invalid.trigger = Trigger::Cron {
            expression: "invalid".into(),
            timezone: "UTC".into(),
        };
        let bad = store
            .create("tenant-a", invalid, due.next_fire_at.unwrap(), None, "bad")
            .await
            .unwrap();
        scheduler_tick(&store, &evaluator()).await.unwrap();
        assert_eq!(runs(&store, due.id).await.unwrap().len(), 1);
        assert!(runs(&store, future.id).await.unwrap().is_empty());
        let failed = store.get_schedule("tenant-a", bad.id).await.unwrap();
        assert_eq!(failed.status, "error");
        assert_eq!(failed.next_fire_at, bad.next_fire_at);
        assert!(failed.last_error.is_some());
        assert!(
            !store
                .materialize(&due, &[due.next_fire_at.unwrap()], None)
                .await
                .unwrap()
        );
        let revised = store
            .replace("tenant-a", future.id, 1, spec(later), later)
            .await
            .unwrap();
        assert!(!store.materialize(&future, &[later], None).await.unwrap());
        assert_eq!(
            store
                .get_schedule("tenant-a", future.id)
                .await
                .unwrap()
                .revision,
            revised.revision
        );
    }
}

mod claim_tests {
    use super::schedule_spec as spec;
    use scheduler_service::{
        domain::{ConcurrencyPolicy, Run, Schedule},
        store::Store,
    };
    use sqlx::PgPool;
    async fn create_due(store: &Store) -> Schedule {
        let at = store.now().await.unwrap() - chrono::Duration::seconds(5);
        store
            .create("tenant-a", spec(at), at, None, "test")
            .await
            .unwrap()
    }
    async fn create_run(store: &Store) -> Schedule {
        let schedule = create_due(store).await;
        assert!(
            store
                .materialize(&schedule, &[schedule.next_fire_at.unwrap()], None)
                .await
                .unwrap()
        );
        schedule
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn concurrent_workers_claim_once(pool: PgPool) {
        let store = Store { pool };
        create_run(&store).await;
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..12 {
            let store = store.clone();
            tasks.spawn(async move { store.claim().await.unwrap() });
        }
        let mut claims = Vec::new();
        while let Some(result) = tasks.join_next().await {
            if let Some(run) = result.unwrap() {
                claims.push(run);
            }
        }
        assert_eq!(claims.len(), 1);
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM attempts WHERE run_id=$1 AND status='running'",
        )
        .bind(claims[0].id)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(claims[0].attempt_count, 1);
        assert_eq!(claims[0].cycle_attempts, 1);
        assert!(claims[0].lease_token.is_some());
        assert!(claims[0].lease_until.unwrap() > store.now().await.unwrap());
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn forbid_concurrency_is_enforced_across_workers(pool: PgPool) {
        let store = Store { pool };
        let at = store.now().await.unwrap() - chrono::Duration::seconds(10);
        let mut spec = spec(at);
        spec.concurrency = ConcurrencyPolicy::Forbid;
        let schedule = store
            .create("tenant-a", spec, at, None, "test")
            .await
            .unwrap();
        store
            .materialize(&schedule, &[at, at + chrono::Duration::seconds(1)], None)
            .await
            .unwrap();
        let (first, second) = tokio::join!(store.claim(), store.claim());
        let claims: Vec<_> = [first.unwrap(), second.unwrap()]
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(claims.len(), 1);
        assert!(store.claim().await.unwrap().is_none());
        let other = create_run(&store).await;
        assert_eq!(store.claim().await.unwrap().unwrap().schedule_id, other.id);
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn claim_skips_paused_cancelled_stale_and_unavailable_work(pool: PgPool) {
        let store = Store { pool };
        for status in ["paused", "cancelled", "error"] {
            let schedule = create_run(&store).await;
            sqlx::query("UPDATE schedules SET status=$2 WHERE id=$1")
                .bind(schedule.id)
                .bind(status)
                .execute(&store.pool)
                .await
                .unwrap();
        }
        let stale = create_run(&store).await;
        sqlx::query("UPDATE schedules SET revision=revision+1 WHERE id=$1")
            .bind(stale.id)
            .execute(&store.pool)
            .await
            .unwrap();
        let later = create_run(&store).await;
        sqlx::query(
            "UPDATE runs SET available_at=clock_timestamp()+INTERVAL '1 hour' WHERE schedule_id=$1",
        )
        .bind(later.id)
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(store.claim().await.unwrap().is_none());
        let eligible = create_run(&store).await;
        let run = store.claim().await.unwrap().unwrap();
        assert_eq!(run.schedule_id, eligible.id);
        assert!(store.claim().await.unwrap().is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn failed_attempt_insert_rolls_back_the_claim_and_lease(pool: PgPool) {
        let store = Store { pool };
        let schedule = create_run(&store).await;
        sqlx::query("CREATE FUNCTION reject_test_attempt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test attempt failure'; END $$").execute(&store.pool).await.unwrap();
        sqlx::query("CREATE TRIGGER reject_test_attempt BEFORE INSERT ON attempts FOR EACH ROW EXECUTE FUNCTION reject_test_attempt()").execute(&store.pool).await.unwrap();
        assert!(store.claim().await.is_err());
        let run: Run = sqlx::query_as("SELECT * FROM runs WHERE schedule_id=$1")
            .bind(schedule.id)
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(run.status, "pending");
        assert_eq!(run.attempt_count, 0);
        assert_eq!(run.cycle_attempts, 0);
        assert!(run.lease_token.is_none());
        assert!(run.lease_until.is_none());
        sqlx::query("DROP TRIGGER reject_test_attempt ON attempts")
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(store.claim().await.unwrap().is_some());
    }
}

mod webhook_tests {
    use axum::{
        Json, Router,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use scheduler_service::{domain::Run, store::Store, webhook};
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use std::sync::{Arc, Mutex};

    async fn run(store: &Store, url: String) -> Run {
        let at = store.now().await.unwrap() - chrono::Duration::seconds(1);
        let mut spec = super::schedule_spec(at);
        spec.target.url = url;
        spec.target.timeout_seconds = 1;
        spec.target
            .headers
            .insert("x-callback-secret".into(), "test-only".into());
        let schedule = store
            .create("tenant-a", spec, at, None, "test")
            .await
            .unwrap();
        store.materialize(&schedule, &[at], None).await.unwrap();
        store.claim().await.unwrap().unwrap()
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires PostgreSQL; run scripts/test.sh"]
    async fn real_callbacks_preserve_envelopes_and_bound_network_behavior(pool: PgPool) {
        let captured = Arc::new(Mutex::new(Vec::<(HeaderMap, Value)>::new()));
        let received = captured.clone();
        let callback = Router::new()
            .route(
                "/hook",
                post(move |headers: HeaderMap, Json(body): Json<Value>| {
                    let received = received.clone();
                    async move {
                        received.lock().unwrap().push((headers, body));
                        "accepted"
                    }
                }),
            )
            .route(
                "/redirect",
                post(|| async { (StatusCode::FOUND, [("location", "/hook")], "redirect") }),
            )
            .route("/large", post(|| async { "x".repeat(10_000) }))
            .route(
                "/slow",
                post(|| async {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    "late"
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, callback).await.unwrap();
        });
        let store = Store { pool };
        let mut delivery = run(&store, format!("http://{address}/hook")).await;
        let denied = webhook::deliver(&delivery, false).await;
        assert!(!denied.success);
        assert!(denied.error.unwrap().contains("Private/reserved"));
        assert!(captured.lock().unwrap().is_empty());
        let accepted = webhook::deliver(&delivery, true).await;
        assert!(accepted.success);
        assert_eq!(accepted.http_status, Some(200));
        assert_eq!(accepted.response_excerpt.as_deref(), Some("accepted"));
        delivery.attempt_count = 2;
        assert!(webhook::deliver(&delivery, true).await.success);
        {
            let messages = captured.lock().unwrap();
            assert_eq!(messages.len(), 2);
            for (i, (headers, body)) in messages.iter().enumerate() {
                assert_eq!(headers["idempotency-key"], delivery.id.to_string());
                assert_eq!(headers["x-scheduler-run-id"], delivery.id.to_string());
                assert_eq!(headers["x-callback-secret"], "test-only");
                assert_eq!(headers["x-scheduler-attempt"], (i + 1).to_string());
                assert_eq!(body["run_id"], json!(delivery.id));
                assert_eq!(body["schedule_id"], json!(delivery.schedule_id));
                assert_eq!(body["revision"], delivery.revision);
                assert_eq!(body["scheduled_at"], json!(delivery.scheduled_at));
                assert_eq!(body["payload"], delivery.spec.payload);
            }
        }
        delivery.spec.0.target.url = format!("http://{address}/redirect");
        let redirect = webhook::deliver(&delivery, true).await;
        assert_eq!(redirect.http_status, Some(302));
        assert!(!redirect.success);
        assert_eq!(captured.lock().unwrap().len(), 2);
        delivery.spec.0.target.url = format!("http://{address}/large");
        let large = webhook::deliver(&delivery, true).await;
        assert!(large.success);
        assert_eq!(large.response_excerpt.unwrap().len(), 4096);
        delivery.spec.0.target.url = format!("http://{address}/slow");
        let started = std::time::Instant::now();
        assert!(!webhook::deliver(&delivery, true).await.success);
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        server.abort();
        let _ = server.await;
    }
}
