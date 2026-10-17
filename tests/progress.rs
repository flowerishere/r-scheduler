//! Retry fairness and concurrent API response semantics.
use std::{collections::BTreeMap, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use chrono::{DateTime, Utc};
use http_body_util::BodyExt;
use scheduler_service::{
    api::{self, AppState},
    domain::{ScheduleSpec, Trigger},
    engine::scheduler_tick,
    evaluator::Evaluator,
    store::Store,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

fn evaluator(executable: &str) -> Evaluator {
    Evaluator::new(executable.into(), Duration::from_secs(2))
}

fn app(store: Store) -> Router {
    api::router(AppState::new(
        store,
        evaluator(env!("CARGO_BIN_EXE_scheduler-service")),
        &BTreeMap::from([
            ("a".into(), "progress-a-test-key".into()),
            ("b".into(), "progress-b-test-key".into()),
        ]),
    ))
}

fn spec(at: DateTime<Utc>) -> ScheduleSpec {
    serde_json::from_value(
        json!({"name":"progress", "trigger":{"type":"once", "at":at},
        "target":{"url":"https://example.invalid/hook"}}),
    )
    .unwrap()
}

async fn request(
    app: &Router,
    method: &str,
    path: &str,
    body: Value,
    tenant: &str,
    key: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(
            "authorization",
            format!("Bearer progress-{tenant}-test-key"),
        )
        .header("content-type", "application/json");
    if let Some(key) = key {
        builder = builder.header("idempotency-key", key);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn transient_rule_failures_do_not_starve_due_once_schedules(pool: PgPool) {
    let store = Store { pool };
    let now = store.now().await.unwrap();
    let at = now - chrono::Duration::hours(1);
    let mut failures = Vec::new();
    for _ in 0..32 {
        let mut spec = spec(at);
        spec.trigger = Trigger::Cron {
            expression: "* * * * * *".into(),
            timezone: "UTC".into(),
        };
        failures.push(store.create("a", spec, at, None, "test").await.unwrap());
    }
    let healthy_at = now - chrono::Duration::seconds(1);
    let healthy = store
        .create("a", spec(healthy_at), healthy_at, None, "test")
        .await
        .unwrap();
    let unavailable = evaluator("/missing-progress-evaluator");
    scheduler_tick(&store, &unavailable).await.unwrap();
    // Even once retry delays expire, failed cursors must not jump back ahead
    // of normal work which has not received a turn.
    let ids: Vec<_> = failures.iter().map(|schedule| schedule.id).collect();
    sqlx::query("UPDATE schedules SET evaluation_retry_at = $2 WHERE id = ANY($1)")
        .bind(ids)
        .bind(healthy_at + chrono::Duration::microseconds(1))
        .execute(&store.pool)
        .await
        .unwrap();
    scheduler_tick(&store, &unavailable).await.unwrap();
    assert_eq!(
        store.get_schedule("a", healthy.id).await.unwrap().status,
        "completed",
        "a full batch of retryable failures kept normal once jobs from ever running"
    );
    assert_eq!(
        store
            .list_runs("a", Some(healthy.id), None, 10, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    for schedule in failures {
        let current = store.get_schedule("a", schedule.id).await.unwrap();
        assert_eq!(current.status, "active");
        assert_eq!(current.next_fire_at, schedule.next_fire_at);
        assert!(current.last_error.is_none());
    }
}

async fn recurring(store: &Store) -> scheduler_service::domain::Schedule {
    let at = store.now().await.unwrap() - chrono::Duration::hours(1);
    let mut spec = spec(at);
    spec.trigger = Trigger::Cron {
        expression: "* * * * * *".into(),
        timezone: "UTC".into(),
    };
    store.create("a", spec, at, None, "test").await.unwrap()
}

async fn retry_at(store: &Store, id: uuid::Uuid) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT evaluation_retry_at FROM schedules WHERE id = $1")
        .bind(id)
        .fetch_one(&store.pool)
        .await
        .unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn deferred_evaluation_retries_automatically_and_preserves_the_occurrence_cursor(
    pool: PgPool,
) {
    let store = Store { pool };
    let original = recurring(&store).await;
    let before = store.now().await.unwrap();
    scheduler_tick(&store, &evaluator("/missing-progress-evaluator"))
        .await
        .unwrap();
    let deferred = store.get_schedule("a", original.id).await.unwrap();
    assert_eq!(deferred.updated_at, original.updated_at);
    assert_eq!(deferred.next_fire_at, original.next_fire_at);
    let first_retry = retry_at(&store, original.id).await.unwrap();
    assert!(first_retry >= before + chrono::Duration::seconds(5));
    assert!(store.due_schedules(32).await.unwrap().is_empty());
    // A late failure report cannot extend a newer retry window.
    store.defer_evaluation(&original).await.unwrap();
    assert_eq!(retry_at(&store, original.id).await, Some(first_retry));
    sqlx::query("UPDATE schedules SET evaluation_retry_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1")
        .bind(original.id).execute(&store.pool).await.unwrap();
    scheduler_tick(&store, &evaluator(env!("CARGO_BIN_EXE_scheduler-service")))
        .await
        .unwrap();
    assert!(retry_at(&store, original.id).await.is_none());
    let runs = store
        .list_runs("a", Some(original.id), None, 10, 0)
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].scheduled_at, original.next_fire_at.unwrap());
    assert!(
        store
            .get_schedule("a", original.id)
            .await
            .unwrap()
            .next_fire_at
            > original.next_fire_at
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn a_retry_report_does_not_invalidate_a_successful_parallel_expansion(pool: PgPool) {
    let store = Store { pool };
    let original = recurring(&store).await;
    store.defer_evaluation(&original).await.unwrap();
    let next = store.now().await.unwrap() + chrono::Duration::seconds(60);
    assert!(
        store
            .materialize(&original, &[original.next_fire_at.unwrap()], Some(next))
            .await
            .unwrap()
    );
    assert!(retry_at(&store, original.id).await.is_none());
    store.defer_evaluation(&original).await.unwrap();
    assert!(retry_at(&store, original.id).await.is_none());
    assert_eq!(
        store
            .get_schedule("a", original.id)
            .await
            .unwrap()
            .next_fire_at,
        Some(next)
    );
    assert_eq!(
        store
            .list_runs("a", Some(original.id), None, 10, 0)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn lifecycle_changes_clear_retry_windows_and_fence_late_failure_reports(pool: PgPool) {
    let store = Store { pool };
    let original = recurring(&store).await;
    store.defer_evaluation(&original).await.unwrap();
    let old = store.get_schedule("a", original.id).await.unwrap();
    store.transition("a", original.id, "pause").await.unwrap();
    let resumed = store.transition("a", original.id, "resume").await.unwrap();
    store.defer_evaluation(&old).await.unwrap();
    assert!(retry_at(&store, original.id).await.is_none());
    store.defer_evaluation(&resumed).await.unwrap();
    let old = store.get_schedule("a", original.id).await.unwrap();
    let revised = store
        .replace(
            "a",
            original.id,
            old.revision,
            old.spec.0.clone(),
            old.next_fire_at.unwrap(),
        )
        .await
        .unwrap();
    store.defer_evaluation(&old).await.unwrap();
    assert!(retry_at(&store, original.id).await.is_none());
    store.defer_evaluation(&revised).await.unwrap();
    let old = store.get_schedule("a", original.id).await.unwrap();
    assert!(
        store
            .materialize(
                &old,
                &[old.next_fire_at.unwrap()],
                Some(store.now().await.unwrap() + chrono::Duration::seconds(60))
            )
            .await
            .unwrap()
    );
    store.defer_evaluation(&old).await.unwrap();
    assert!(retry_at(&store, original.id).await.is_none());
    let current = store.get_schedule("a", original.id).await.unwrap();
    store.defer_evaluation(&current).await.unwrap();
    let current = store.get_schedule("a", original.id).await.unwrap();
    store.rule_error(&current, "invalid rule").await.unwrap();
    store.defer_evaluation(&current).await.unwrap();
    assert!(retry_at(&store, original.id).await.is_none());
    assert_eq!(
        store.get_schedule("a", original.id).await.unwrap().status,
        "error"
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn expansion_retry_delay_does_not_pause_already_materialized_work(pool: PgPool) {
    let store = Store { pool };
    let original = recurring(&store).await;
    let at = original.next_fire_at.unwrap();
    store
        .materialize(&original, &[at], Some(at + chrono::Duration::seconds(1)))
        .await
        .unwrap();
    let current = store.get_schedule("a", original.id).await.unwrap();
    store.defer_evaluation(&current).await.unwrap();
    assert!(store.due_schedules(32).await.unwrap().is_empty());
    let run = store.claim().await.unwrap().unwrap();
    assert_eq!(run.schedule_id, original.id);
    assert_eq!(run.scheduled_at, at);
    assert_eq!(run.revision, original.revision);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn failed_rule_error_writes_do_not_monopolize_later_ticks(pool: PgPool) {
    let store = Store { pool };
    let now = store.now().await.unwrap();
    let old = now - chrono::Duration::hours(1);
    for _ in 0..32 {
        let mut spec = spec(old);
        spec.trigger = Trigger::Cron {
            expression: "invalid rule".into(),
            timezone: "UTC".into(),
        };
        store.create("a", spec, old, None, "test").await.unwrap();
    }
    let at = now - chrono::Duration::seconds(1);
    let healthy = store.create("a", spec(at), at, None, "test").await.unwrap();
    sqlx::raw_sql("CREATE FUNCTION refuse_rule_error() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.status = 'error' THEN RAISE EXCEPTION 'cannot record rule error'; END IF; RETURN NEW; END $$;
        CREATE TRIGGER refuse_rule_error BEFORE UPDATE ON schedules FOR EACH ROW EXECUTE FUNCTION refuse_rule_error();")
        .execute(&store.pool).await.unwrap();
    let evaluator = evaluator(env!("CARGO_BIN_EXE_scheduler-service"));
    assert!(scheduler_tick(&store, &evaluator).await.is_err());
    scheduler_tick(&store, &evaluator).await.unwrap();
    assert_eq!(
        store.get_schedule("a", healthy.id).await.unwrap().status,
        "completed"
    );
    assert_eq!(
        store
            .list_runs("a", Some(healthy.id), None, 10, 0)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn concurrent_create_retries_report_existing_resource_instead_of_creation(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let body =
        serde_json::to_value(spec(store.now().await.unwrap() + chrono::Duration::days(1))).unwrap();
    sqlx::raw_sql("CREATE FUNCTION gate_creation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_advisory_xact_lock(7348251); RETURN NEW; END $$;
        CREATE TRIGGER gate_creation BEFORE INSERT ON schedules FOR EACH ROW EXECUTE FUNCTION gate_creation();")
        .execute(&store.pool).await.unwrap();
    let mut gate = store.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(7348251)")
        .execute(&mut *gate)
        .await
        .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..2 {
        let app = app.clone();
        let body = body.clone();
        tasks.spawn(async move {
            request(
                &app,
                "POST",
                "/v1/schedules",
                body,
                "a",
                Some("concurrent-creation"),
            )
            .await
        });
    }
    // Both requests must pass the initial lookup before either insert commits.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event = 'advisory'")
                .fetch_one(&store.pool).await.unwrap();
            if waiting == 2 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    gate.commit().await.unwrap();
    let first = tasks.join_next().await.unwrap().unwrap();
    let second = tasks.join_next().await.unwrap().unwrap();
    assert_eq!(first.1["id"], second.1["id"]);
    let mut statuses = [first.0, second.0];
    statuses.sort_unstable();
    assert_eq!(statuses, [StatusCode::OK, StatusCode::CREATED]);
    assert_eq!(store.list_schedules("a", 100, 0).await.unwrap().len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn full_schedule_uuid_search_accepts_uppercase_without_crossing_tenants(pool: PgPool) {
    let store = Store { pool };
    let at = store.now().await.unwrap() + chrono::Duration::days(1);
    let schedule = store.create("a", spec(at), at, None, "test").await.unwrap();
    let id: uuid::Uuid = "abcdefab-cdef-4abc-8def-abcdefabcdef".parse().unwrap();
    sqlx::query("UPDATE schedules SET id = $1 WHERE id = $2")
        .bind(id)
        .bind(schedule.id)
        .execute(&store.pool)
        .await
        .unwrap();
    let app = app(store.clone());
    let path = format!(
        "/v1/schedules?q={}&status=active",
        id.to_string().to_uppercase()
    );
    let (status, found) = request(&app, "GET", &path, Value::Null, "a", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(found.as_array().unwrap().len(), 1);
    assert_eq!(found[0]["id"], id.to_string());
    let (status, other) = request(&app, "GET", &path, Value::Null, "b", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other, json!([]));
}
