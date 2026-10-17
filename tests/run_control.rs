//! Pending-run operations, immutable history, expiry and worker races.
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
    domain::{DeliveryResult, Run, Schedule, ScheduleSpec, Trigger},
    evaluator::Evaluator,
    run_control::RunTiming,
    store::{Store, StoreError},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

fn app(store: Store) -> Router {
    api::router(AppState::new(
        store,
        Evaluator::new(
            env!("CARGO_BIN_EXE_scheduler-service").into(),
            Duration::from_secs(5),
        ),
        &BTreeMap::from([
            ("a".into(), "run-control-a-test-key".into()),
            ("b".into(), "run-control-b-test-key".into()),
        ]),
    ))
}

async fn request(
    app: &Router,
    id: uuid::Uuid,
    operation: &str,
    body: Value,
    tenant: &str,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/runs/{id}/{operation}"))
                .header(
                    "authorization",
                    format!("Bearer run-control-{tenant}-test-key"),
                )
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes))),
    )
}

fn spec(at: DateTime<Utc>, age: Option<u32>) -> ScheduleSpec {
    serde_json::from_value(json!({"name":"run control", "trigger":{"type":"once", "at":at},
        "target":{"url":"https://example.invalid/hook"}, "payload":{"order_id":9007199254740993_u64},
        "metadata":{"order_id":"9007199254740993"}, "retry":{"max_age_seconds":age}})).unwrap()
}

async fn queued(store: &Store, age: Option<u32>) -> (Schedule, Run) {
    let at = store.now().await.unwrap() - chrono::Duration::seconds(5);
    let schedule = store
        .create("a", spec(at, age), at, None, "test")
        .await
        .unwrap();
    store.materialize(&schedule, &[at], None).await.unwrap();
    let schedule = store.get_schedule("a", schedule.id).await.unwrap();
    let run = store
        .list_runs("a", Some(schedule.id), None, 1, 0)
        .await
        .unwrap()
        .remove(0);
    (schedule, run)
}

fn reschedule(run: &Run, seconds: u32) -> Value {
    json!({"expected_available_at":run.available_at, "timing":{"type":"delay", "seconds":seconds}})
}

fn value(value: &impl serde::Serialize) -> Value {
    serde_json::to_value(value).unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn cancelling_one_occurrence_preserves_its_schedule_and_siblings(pool: PgPool) {
    let store = Store { pool };
    let at = store.now().await.unwrap() - chrono::Duration::seconds(30);
    let mut definition = spec(at, None);
    definition.trigger = Trigger::Cron {
        expression: "* * * * * *".into(),
        timezone: "UTC".into(),
    };
    let schedule = store
        .create("a", definition, at, None, "test")
        .await
        .unwrap();
    store
        .materialize(
            &schedule,
            &[at, at + chrono::Duration::seconds(1)],
            Some(at + chrono::Duration::hours(1)),
        )
        .await
        .unwrap();
    let before = value(&store.get_schedule("a", schedule.id).await.unwrap());
    let runs = store
        .list_runs("a", Some(schedule.id), None, 10, 0)
        .await
        .unwrap();
    let app = app(store.clone());
    let (status, cancelled) = request(&app, runs[0].id, "cancel", Value::Null, "a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["attempt_count"], 0);
    assert!(cancelled["finished_at"].is_string());
    assert_eq!(cancelled["scheduled_at"], value(&runs[0].scheduled_at));
    assert_eq!(cancelled["spec"], value(&runs[0].spec));
    assert_eq!(
        request(&app, runs[0].id, "cancel", Value::Null, "a").await,
        (StatusCode::OK, cancelled)
    );
    assert_eq!(
        value(&store.get_schedule("a", schedule.id).await.unwrap()),
        before
    );
    assert_eq!(
        value(&store.get_run("a", runs[1].id).await.unwrap()),
        value(&runs[1])
    );
    let claimed = store.claim().await.unwrap().unwrap();
    assert_eq!(claimed.id, runs[1].id);
    assert!(store.claim().await.unwrap().is_none());
    assert!(
        store
            .list_attempts("a", runs[0].id, 100, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store.replay("a", runs[0].id).await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        store.stats("a").await.unwrap(),
        json!({"schedules":{"active":1}, "runs":{"cancelled":1,"running":1}})
    );
    assert_eq!(
        store.stats("b").await.unwrap(),
        json!({"schedules":{},"runs":{}})
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn rescheduling_retry_preserves_identity_deadline_attempts_and_parent(pool: PgPool) {
    let store = Store { pool };
    let (schedule, original) = queued(&store, Some(300)).await;
    let claimed = store.claim().await.unwrap().unwrap();
    assert!(matches!(
        store.cancel_run("a", claimed.id).await,
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        store
            .reschedule_run(
                "a",
                claimed.id,
                claimed.available_at,
                RunTiming::Delay { seconds: 0 }
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        store.get_run("a", claimed.id).await.unwrap().status,
        "running"
    );
    store
        .finish(
            &claimed,
            DeliveryResult::error("receiver temporarily unavailable"),
        )
        .await
        .unwrap();
    let retry = store.get_run("a", original.id).await.unwrap();
    let attempts = value(&store.list_attempts("a", retry.id, 100, None).await.unwrap());
    let app = app(store.clone());
    let before = store.now().await.unwrap();
    let (status, deferred) =
        request(&app, retry.id, "reschedule", reschedule(&retry, 60), "a").await;
    assert_eq!(status, StatusCode::OK, "{deferred}");
    let after = store.now().await.unwrap();
    let deferred = store.get_run("a", retry.id).await.unwrap();
    assert!(deferred.available_at >= before + chrono::Duration::seconds(60));
    assert!(deferred.available_at <= after + chrono::Duration::seconds(60));
    let mut expected = value(&retry);
    expected["available_at"] = value(&deferred.available_at);
    assert_eq!(value(&deferred), expected);
    assert!(store.claim().await.unwrap().is_none());
    assert_eq!(
        value(&store.get_schedule("a", schedule.id).await.unwrap()),
        value(&schedule)
    );
    assert_eq!(
        value(&store.list_attempts("a", retry.id, 100, None).await.unwrap()),
        attempts
    );
    let (status, _) = request(
        &app,
        deferred.id,
        "reschedule",
        reschedule(&deferred, 0),
        "a",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let claimed = store.claim().await.unwrap().unwrap();
    assert_eq!(claimed.id, retry.id);
    assert_eq!(claimed.attempt_count, 2);
    assert_eq!(claimed.cycle_attempts, 2);
    assert_eq!(claimed.scheduled_at, original.scheduled_at);
    assert_eq!(claimed.expires_at, original.expires_at);
    store
        .finish(
            &claimed,
            DeliveryResult {
                success: true,
                http_status: Some(200),
                error: None,
                response_excerpt: None,
                retry_after: None,
            },
        )
        .await
        .unwrap();
    for operation in ["cancel", "reschedule"] {
        assert_eq!(
            request(&app, retry.id, operation, reschedule(&claimed, 0), "a")
                .await
                .0,
            StatusCode::CONFLICT
        );
    }
    assert_eq!(
        store
            .list_attempts("a", retry.id, 100, None)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn cancelled_retries_retain_prior_attempts_and_do_not_resume(pool: PgPool) {
    let store = Store { pool };
    let (_, run) = queued(&store, None).await;
    let claimed = store.claim().await.unwrap().unwrap();
    store
        .finish(&claimed, DeliveryResult::error("failed callback"))
        .await
        .unwrap();
    let attempts = value(&store.list_attempts("a", run.id, 100, None).await.unwrap());
    let cancelled = store.cancel_run("a", run.id).await.unwrap();
    assert_eq!(cancelled.status, "cancelled");
    assert_eq!(cancelled.attempt_count, 1);
    assert_eq!(cancelled.cycle_attempts, 1);
    assert_eq!(
        value(&store.list_attempts("a", run.id, 100, None).await.unwrap()),
        attempts
    );
    assert!(store.claim().await.unwrap().is_none());
    assert!(matches!(
        store
            .reschedule_run(
                "a",
                run.id,
                cancelled.available_at,
                RunTiming::Delay { seconds: 0 }
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn controls_are_tenant_scoped_and_reject_invalid_timing_without_mutation(pool: PgPool) {
    let store = Store { pool };
    let (_, run) = queued(&store, None).await;
    let app = app(store.clone());
    for id in [run.id, uuid::Uuid::new_v4()] {
        for operation in ["cancel", "reschedule"] {
            assert_eq!(
                request(&app, id, operation, reschedule(&run, 60), "b")
                    .await
                    .0,
                StatusCode::NOT_FOUND
            );
        }
    }
    let future = store.now().await.unwrap() + chrono::Duration::hours(1);
    let bodies = [
        json!({"expected_available_at":run.available_at, "timing":{"type":"delay", "seconds":31536001}}),
        json!({"expected_available_at":"2030-01-01T00:00:00.000000001Z", "timing":{"type":"delay", "seconds":1}}),
        json!({"expected_available_at":run.available_at, "timing":{"type":"once", "at":"2030-01-01T00:00:00.000000001Z"}}),
        json!({"expected_available_at":run.available_at, "timing":{"type":"once", "at":run.available_at}}),
    ];
    for body in bodies {
        let (status, error) = request(&app, run.id, "reschedule", body, "a").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    }
    for body in [
        json!({"timing":{"type":"delay", "seconds":1}}),
        json!({"expected_available_at":run.available_at, "timing":{"type":"cron", "expression":"* * * * *"}}),
        json!({"expected_available_at":run.available_at, "timing":{"type":"delay", "seconds":1, "unknown":true}}),
        json!({"expected_available_at":run.available_at, "timing":{"type":"once", "at":future}, "unknown":true}),
    ] {
        assert_eq!(
            request(&app, run.id, "reschedule", body, "a").await.0,
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    assert_eq!(
        value(&store.get_run("a", run.id).await.unwrap()),
        value(&run)
    );
    assert_eq!(
        store
            .list_runs("b", None, None, 100, 0)
            .await
            .unwrap()
            .len(),
        0
    );
    let (status, accepted) = request(
        &app,
        run.id,
        "reschedule",
        json!({"expected_available_at":run.available_at, "timing":{"type":"once", "at":future}}),
        "a",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{accepted}");
    assert_eq!(accepted["available_at"], value(&future));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn original_deadline_cannot_be_extended_or_revived_by_rescheduling(pool: PgPool) {
    let store = Store { pool };
    let (_, run) = queued(&store, Some(120)).await;
    let app = app(store.clone());
    for at in [
        run.expires_at.unwrap(),
        run.expires_at.unwrap() + chrono::Duration::microseconds(1),
    ] {
        assert_eq!(request(&app, run.id, "reschedule", json!({"expected_available_at":run.available_at, "timing":{"type":"once", "at":at}}), "a").await.0, StatusCode::CONFLICT);
    }
    let at = run.expires_at.unwrap() - chrono::Duration::microseconds(1);
    let updated = store
        .reschedule_run("a", run.id, run.available_at, RunTiming::Once { at })
        .await
        .unwrap();
    assert_eq!(updated.available_at, at);
    assert_eq!(updated.expires_at, run.expires_at);
    assert!(matches!(
        store
            .reschedule_run(
                "a",
                run.id,
                updated.available_at,
                RunTiming::Delay { seconds: 31536000 }
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    sqlx::query(
        "UPDATE runs SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
    )
    .bind(run.id)
    .execute(&store.pool)
    .await
    .unwrap();
    assert!(matches!(
        store
            .reschedule_run(
                "a",
                run.id,
                updated.available_at,
                RunTiming::Delay { seconds: 0 }
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(store.expire_pending().await.unwrap(), 1);
    let dead = store.get_run("a", run.id).await.unwrap();
    for operation in ["cancel", "reschedule"] {
        assert_eq!(
            request(&app, run.id, operation, reschedule(&dead, 0), "a")
                .await
                .0,
            StatusCode::CONFLICT
        );
    }
    assert_eq!(
        value(&store.get_run("a", run.id).await.unwrap()),
        value(&dead)
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn adjusted_work_still_obeys_parent_pause_and_revision_changes(pool: PgPool) {
    let store = Store { pool };
    let (schedule, run) = queued(&store, None).await;
    store.transition("a", schedule.id, "pause").await.unwrap();
    let adjusted = store
        .reschedule_run(
            "a",
            run.id,
            run.available_at,
            RunTiming::Delay { seconds: 0 },
        )
        .await
        .unwrap();
    assert!(store.claim().await.unwrap().is_none());
    store.transition("a", schedule.id, "resume").await.unwrap();
    let claimed = store.claim().await.unwrap().unwrap();
    assert_eq!(claimed.id, adjusted.id);
    store
        .finish(&claimed, DeliveryResult::error("retry later"))
        .await
        .unwrap();
    let pending = store.get_run("a", run.id).await.unwrap();
    store
        .replace(
            "a",
            schedule.id,
            schedule.revision,
            schedule.spec.0.clone(),
            run.scheduled_at,
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .reschedule_run(
                "a",
                run.id,
                pending.available_at,
                RunTiming::Delay { seconds: 0 }
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        store.get_run("a", run.id).await.unwrap().status,
        "cancelled"
    );
    let current = store.get_schedule("a", schedule.id).await.unwrap();
    store
        .materialize(&current, &[run.scheduled_at], None)
        .await
        .unwrap();
    let new_run = store
        .list_runs("a", Some(schedule.id), Some("pending"), 1, 0)
        .await
        .unwrap()
        .remove(0);
    store.transition("a", schedule.id, "cancel").await.unwrap();
    assert!(matches!(
        store
            .reschedule_run(
                "a",
                new_run.id,
                new_run.available_at,
                RunTiming::Delay { seconds: 0 }
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn concurrent_availability_changes_do_not_overwrite_each_other(pool: PgPool) {
    let store = Store { pool };
    let (_, run) = queued(&store, None).await;
    let now = store.now().await.unwrap();
    let first = now + chrono::Duration::minutes(1);
    let second = now + chrono::Duration::minutes(2);
    let (left, right) = tokio::join!(
        store.reschedule_run("a", run.id, run.available_at, RunTiming::Once { at: first }),
        store.reschedule_run(
            "a",
            run.id,
            run.available_at,
            RunTiming::Once { at: second }
        )
    );
    let winner = match (left, right) {
        (Ok(run), Err(StoreError::Conflict(_))) | (Err(StoreError::Conflict(_)), Ok(run)) => run,
        other => panic!("expected one successful update and one conflict: {other:?}"),
    };
    assert_eq!(
        value(&store.get_run("a", run.id).await.unwrap()),
        value(&winner)
    );
    assert_eq!(winner.status, "pending");
    assert_eq!(winner.attempt_count, 0);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn cancellation_racing_worker_claim_never_stops_an_in_flight_attempt(pool: PgPool) {
    let store = Store { pool };
    for _ in 0..8 {
        let (_, run) = queued(&store, None).await;
        let (cancelled, claimed) = tokio::join!(store.cancel_run("a", run.id), store.claim());
        match (cancelled, claimed.unwrap()) {
            (Ok(cancelled), None) => {
                assert_eq!(cancelled.status, "cancelled");
                assert_eq!(cancelled.attempt_count, 0);
                assert!(
                    store
                        .list_attempts("a", run.id, 100, None)
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
            (Err(StoreError::Conflict(_)), Some(claimed)) => {
                assert_eq!(claimed.id, run.id);
                assert_eq!(store.get_run("a", run.id).await.unwrap().status, "running");
                assert_eq!(
                    store
                        .list_attempts("a", run.id, 100, None)
                        .await
                        .unwrap()
                        .len(),
                    1
                );
                assert!(
                    store
                        .finish(
                            &claimed,
                            DeliveryResult {
                                success: true,
                                http_status: Some(200),
                                error: None,
                                response_excerpt: None,
                                retry_after: None
                            }
                        )
                        .await
                        .unwrap()
                );
            }
            other => panic!("cancellation and claim were not serialized: {other:?}"),
        }
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn deferral_racing_worker_claim_never_moves_a_running_attempt(pool: PgPool) {
    let store = Store { pool };
    for _ in 0..8 {
        let (_, run) = queued(&store, None).await;
        let (deferred, claimed) = tokio::join!(
            store.reschedule_run(
                "a",
                run.id,
                run.available_at,
                RunTiming::Delay { seconds: 60 }
            ),
            store.claim()
        );
        match (deferred, claimed.unwrap()) {
            (Ok(deferred), None) => {
                assert_eq!(deferred.status, "pending");
                assert!(deferred.available_at > store.now().await.unwrap());
                assert_eq!(deferred.attempt_count, 0);
                store.cancel_run("a", run.id).await.unwrap();
            }
            (Err(StoreError::Conflict(_)), Some(claimed)) => {
                assert_eq!(claimed.id, run.id);
                assert_eq!(claimed.available_at, run.available_at);
                assert_eq!(store.get_run("a", run.id).await.unwrap().status, "running");
                assert!(
                    store
                        .finish(
                            &claimed,
                            DeliveryResult {
                                success: true,
                                http_status: Some(200),
                                error: None,
                                response_excerpt: None,
                                retry_after: None
                            }
                        )
                        .await
                        .unwrap()
                );
            }
            other => panic!("deferral and claim were not serialized: {other:?}"),
        }
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn waiting_for_a_lock_cannot_revive_an_expired_run(pool: PgPool) {
    let store = Store { pool };
    let (schedule, run) = queued(&store, None).await;
    let mut lock = store.pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM schedules WHERE id = $1 FOR UPDATE")
        .bind(schedule.id)
        .execute(&mut *lock)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET expires_at = clock_timestamp() + INTERVAL '500 milliseconds' WHERE id = $1")
        .bind(run.id).execute(&mut *lock).await.unwrap();
    let cloned = store.clone();
    let task = tokio::spawn(async move {
        cloned
            .reschedule_run(
                "a",
                run.id,
                run.available_at,
                RunTiming::Delay { seconds: 0 },
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        !task.is_finished(),
        "operation must wait for the parent lock"
    );
    lock.commit().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(StoreError::Conflict(_))), "{result:?}");
    let current = store.get_run("a", run.id).await.unwrap();
    assert_eq!(current.available_at, run.available_at);
    assert_eq!(current.attempt_count, 0);
}
