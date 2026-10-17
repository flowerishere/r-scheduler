//! Lifecycle fencing, independent scheduler progress and timestamp bounds.
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
    domain::{DeliveryResult, Schedule, ScheduleSpec, Trigger},
    engine::scheduler_tick,
    evaluator::Evaluator,
    store::Store,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

fn evaluator() -> Evaluator {
    Evaluator::new(
        env!("CARGO_BIN_EXE_scheduler-service").into(),
        Duration::from_secs(5),
    )
}

fn spec(at: DateTime<Utc>) -> ScheduleSpec {
    serde_json::from_value(
        json!({"name":"lifecycle audit", "trigger":{"type":"once", "at":at},
        "target":{"url":"https://example.invalid/hook"}}),
    )
    .unwrap()
}

async fn recurring(store: &Store) -> Schedule {
    let at = store.now().await.unwrap() - chrono::Duration::seconds(5);
    let mut spec = spec(at);
    spec.trigger = Trigger::Cron {
        expression: "* * * * * *".into(),
        timezone: "UTC".into(),
    };
    store.create("a", spec, at, None, "test").await.unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn pause_resume_rejects_old_expansion_without_invalidating_existing_runs(pool: PgPool) {
    let store = Store { pool };
    let schedule = recurring(&store).await;
    let first = schedule.next_fire_at.unwrap();
    let second = first + chrono::Duration::seconds(1);
    assert!(
        store
            .materialize(&schedule, &[first], Some(second))
            .await
            .unwrap()
    );
    let stale = store.get_schedule("a", schedule.id).await.unwrap();
    store.transition("a", schedule.id, "pause").await.unwrap();
    assert!(store.claim().await.unwrap().is_none());
    let resumed = store.transition("a", schedule.id, "resume").await.unwrap();
    assert_eq!(resumed.revision, stale.revision);
    assert_eq!(resumed.next_fire_at, stale.next_fire_at);
    assert!(
        !store.materialize(&stale, &[second], None).await.unwrap(),
        "pre-pause expansion was accepted after resume"
    );
    assert_eq!(
        store
            .list_runs("a", Some(schedule.id), None, 100, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    let run = store.claim().await.unwrap().unwrap();
    assert_eq!(run.revision, stale.revision);
    assert!(
        store
            .finish(
                &run,
                DeliveryResult {
                    success: true,
                    http_status: Some(200),
                    error: None,
                    response_excerpt: None,
                    retry_after: None,
                }
            )
            .await
            .unwrap()
    );
    assert!(store.materialize(&resumed, &[second], None).await.unwrap());
    assert_eq!(
        store
            .list_runs("a", Some(schedule.id), None, 100, 0)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn pause_resume_fences_old_rule_errors_but_allows_a_fresh_error(pool: PgPool) {
    let store = Store { pool };
    let stale = recurring(&store).await;
    store.transition("a", stale.id, "pause").await.unwrap();
    let resumed = store.transition("a", stale.id, "resume").await.unwrap();
    store
        .rule_error(&stale, "stale calculation failed")
        .await
        .unwrap();
    let current = store.get_schedule("a", stale.id).await.unwrap();
    assert_eq!(current.status, "active");
    assert!(current.last_error.is_none());
    assert_eq!(current.updated_at, resumed.updated_at);
    store
        .rule_error(&current, "fresh calculation failed")
        .await
        .unwrap();
    let current = store.get_schedule("a", stale.id).await.unwrap();
    assert_eq!(current.status, "error");
    assert_eq!(
        current.last_error.as_deref(),
        Some("fresh calculation failed")
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn schedule_update_timestamps_advance_even_when_the_database_clock_moves_back(pool: PgPool) {
    let store = Store { pool };
    let schedule = recurring(&store).await;
    sqlx::query(
        "UPDATE schedules SET updated_at = clock_timestamp() + INTERVAL '1 hour' WHERE id = $1",
    )
    .bind(schedule.id)
    .execute(&store.pool)
    .await
    .unwrap();
    let initial = store.get_schedule("a", schedule.id).await.unwrap();
    let paused = store.transition("a", schedule.id, "pause").await.unwrap();
    assert!(paused.updated_at > initial.updated_at);
    let resumed = store.transition("a", schedule.id, "resume").await.unwrap();
    assert!(resumed.updated_at > paused.updated_at);
    let replaced = store
        .replace(
            "a",
            schedule.id,
            resumed.revision,
            resumed.spec.0.clone(),
            resumed.next_fire_at.unwrap(),
        )
        .await
        .unwrap();
    assert!(replaced.updated_at > resumed.updated_at);
    let next = store.now().await.unwrap() + chrono::Duration::seconds(60);
    assert!(
        store
            .materialize(&replaced, &[replaced.next_fire_at.unwrap()], Some(next))
            .await
            .unwrap()
    );
    let expanded = store.get_schedule("a", schedule.id).await.unwrap();
    assert!(expanded.updated_at > replaced.updated_at);
    store.rule_error(&expanded, "rule failed").await.unwrap();
    let errored = store.get_schedule("a", schedule.id).await.unwrap();
    assert!(errored.updated_at > expanded.updated_at);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn one_rule_error_write_failure_does_not_abort_other_scheduler_batches(pool: PgPool) {
    let store = Store { pool };
    let now = store.now().await.unwrap();
    let at = now - chrono::Duration::hours(1);
    let mut invalid = spec(at);
    invalid.trigger = Trigger::Cron {
        expression: "invalid rule".into(),
        timezone: "UTC".into(),
    };
    let bad = store.create("a", invalid, at, None, "test").await.unwrap();
    let mut healthy = Vec::new();
    for n in 0..7 {
        let at = now - chrono::Duration::seconds(30) + chrono::Duration::microseconds(n);
        healthy.push(store.create("a", spec(at), at, None, "test").await.unwrap());
    }
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION fail_rule_error() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.id = '{}'::uuid AND NEW.status = 'error' THEN
            RAISE EXCEPTION 'simulated rule-error write failure';
        END IF; RETURN NEW; END $$;
        CREATE TRIGGER fail_rule_error BEFORE UPDATE ON schedules
            FOR EACH ROW EXECUTE FUNCTION fail_rule_error();",
        bad.id
    ))
    .execute(&store.pool)
    .await
    .unwrap();
    assert!(scheduler_tick(&store, &evaluator()).await.is_err());
    for schedule in healthy {
        let current = store.get_schedule("a", schedule.id).await.unwrap();
        assert_eq!(
            current.status, "completed",
            "healthy schedule was skipped after another task failed"
        );
    }
    assert_eq!(
        store
            .list_runs("a", None, None, 100, 0)
            .await
            .unwrap()
            .len(),
        7
    );
    assert_eq!(
        store.get_schedule("a", bad.id).await.unwrap().status,
        "active"
    );
    sqlx::raw_sql("DROP TRIGGER fail_rule_error ON schedules; DROP FUNCTION fail_rule_error();")
        .execute(&store.pool)
        .await
        .unwrap();
    // Explicit repair bypasses the infrastructure retry delay.
    store.transition("a", bad.id, "resume").await.unwrap();
    scheduler_tick(&store, &evaluator()).await.unwrap();
    assert_eq!(
        store.get_schedule("a", bad.id).await.unwrap().status,
        "error"
    );
}

fn app(store: Store) -> Router {
    api::router(AppState::new(
        store,
        evaluator(),
        &BTreeMap::from([("a".into(), "scheduling-audit-test-key".into())]),
    ))
}

async fn request(app: &Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", "Bearer scheduling-audit-test-key")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn impossible_run_deadlines_are_rejected_before_create_or_replace(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let mut body = serde_json::to_value(spec(store.now().await.unwrap())).unwrap();
    let (status, existing) = request(&app, "POST", "/v1/schedules", body.clone()).await;
    assert_eq!(status, StatusCode::CREATED);
    let maximum = DateTime::<Utc>::MAX_UTC - chrono::Duration::nanoseconds(999);
    body["trigger"] = json!({"type":"once", "at":maximum});
    body["retry"]["max_age_seconds"] = json!(1);
    for (method, uri, input) in [
        ("POST", "/v1/schedules".to_owned(), body.clone()),
        (
            "PUT",
            format!("/v1/schedules/{}", existing["id"].as_str().unwrap()),
            json!({"expected_revision":1, "spec":body}),
        ),
    ] {
        let (status, response) = request(&app, method, &uri, input).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    }
    let schedules = store.list_schedules("a", 100, 0).await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(serde_json::to_value(&schedules[0]).unwrap(), existing);
    body["trigger"]["at"] = json!(maximum - chrono::Duration::seconds(1));
    let (status, accepted) = request(&app, "POST", "/v1/schedules", body).await;
    assert_eq!(status, StatusCode::CREATED, "{accepted}");
    let schedule = store
        .get_schedule("a", accepted["id"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap();
    assert!(
        store
            .materialize(&schedule, &[schedule.next_fire_at.unwrap()], None)
            .await
            .unwrap()
    );
    let run = store
        .list_runs("a", Some(schedule.id), None, 1, 0)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(run.expires_at, Some(maximum));
}
