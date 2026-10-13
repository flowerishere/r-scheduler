//! Regression coverage for calendar validation and delivery eligibility.
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
    dispatch::TenantLimits,
    domain::{ScheduleSpec, Trigger},
    evaluator::Evaluator,
    store::Store,
    trigger::{EvaluationRequest, evaluate},
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
        &BTreeMap::from([("a".into(), "audit-tenant-test-key".into())]),
    ))
}

async fn request(app: &Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    request_with_keys(app, method, uri, body, &[]).await
}

async fn request_with_keys(
    app: &Router,
    method: &str,
    uri: &str,
    body: Value,
    keys: &[&str],
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", "Bearer audit-tenant-test-key")
        .header("content-type", "application/json");
    for key in keys {
        builder = builder.header("idempotency-key", *key);
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
async fn escaped_rrule_is_consistent_across_preview_create_and_replace(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let mut value = "DTSTART:20300101T090000Z\nRRULE:FREQ=DAILY;COUNT=2".to_owned();
    value.push_str(&"\t".repeat(16 * 1024 - value.len()));
    let trigger = json!({"type":"rrule", "value":value});
    let (status, preview) = request(
        &app,
        "POST",
        "/v1/preview",
        json!({"trigger":trigger, "after":"2029-12-31T00:00:00Z", "count":5}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    assert_eq!(
        preview["dates"],
        json!(["2030-01-01T09:00:00Z", "2030-01-02T09:00:00Z"])
    );
    let (status, created) = request(&app, "POST", "/v1/schedules", spec(trigger.clone())).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let (status, updated) = request(
        &app,
        "PUT",
        &format!("/v1/schedules/{}", created["id"].as_str().unwrap()),
        json!({"expected_revision":1, "spec":spec(trigger)}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["revision"], 2);
    assert_eq!(updated["next_fire_at"], created["next_fire_at"]);
    assert_eq!(store.list_schedules("a", 100, 0).await.unwrap().len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn duplicate_idempotency_headers_never_create_or_reuse_a_schedule(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let body = spec(json!({"type":"delay", "seconds":3600}));
    let (status, created) = request_with_keys(
        &app,
        "POST",
        "/v1/schedules",
        body.clone(),
        &["existing-key"],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for keys in [
        ["existing-key", "existing-key"],
        ["existing-key", "new-key"],
        ["new-key", "existing-key"],
        ["new-key", "new-key"],
    ] {
        let (status, response) =
            request_with_keys(&app, "POST", "/v1/schedules", body.clone(), &keys).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{keys:?}: {response}");
    }
    let (status, repeated) =
        request_with_keys(&app, "POST", "/v1/schedules", body, &["existing-key"]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created, repeated);
    assert_eq!(store.list_schedules("a", 100, 0).await.unwrap().len(), 1);
}

fn spec(trigger: Value) -> Value {
    json!({"name":"audit", "trigger":trigger,
        "target":{"url":"https://example.invalid/hook"}})
}

fn invalid_rules() -> Vec<String> {
    let base = "DTSTART:20300101T090000Z\nRRULE:FREQ=DAILY;COUNT=2";
    [
        "exrule:FREQ=DAILY;COUNT=3",
        "ExRule:FREQ=DAILY;COUNT=3",
        "garbage:FREQ=DAILY;COUNT=3",
        "RRULE;IGNORED=yes:FREQ=DAILY;COUNT=3",
        "RDATE;value=date:20300103T090000Z",
        "EXDATE;VaLuE=DaTe:20300102T090000Z",
        "RDATE;VALUE=PERIOD:20300103T090000Z",
        "EXDATE;VALUE=unknown:20300102T090000Z",
        "EXDATE;tzid=Asia/Shanghai:20300102T090000Z",
        "DTSTART;tzid=Asia/Shanghai:20300101T090000Z",
    ]
    .into_iter()
    .map(|property| format!("{base}\n{property}"))
    .collect()
}

#[test]
fn ambiguous_calendar_properties_cannot_add_or_remove_occurrences() {
    for value in invalid_rules() {
        let result = evaluate(EvaluationRequest {
            trigger: Trigger::Rrule {
                value: value.clone(),
            },
            after: "2029-12-31T00:00:00Z".parse().unwrap(),
            count: 5,
        });
        assert!(result.is_err(), "silently accepted {value}: {result:?}");
    }
}

#[test]
fn calendar_property_case_preserves_explicit_timezones() {
    let result = evaluate(EvaluationRequest {
        trigger: Trigger::Rrule {
            value: "dtstart;tzid=Asia/Shanghai:20300101T090000\nrRuLe:FREQ=DAILY;COUNT=2\nexdate;tzid=Asia/Shanghai:20300102T090000\nrdate;VaLuE=DaTe-TiMe;tzid=Asia/Shanghai:20300103T090000".into(),
        },
        after: "2029-12-31T00:00:00Z".parse().unwrap(),
        count: 5,
    })
    .unwrap();
    let expected: Vec<DateTime<Utc>> = ["2030-01-01T01:00:00Z", "2030-01-03T01:00:00Z"]
        .into_iter()
        .map(|date| date.parse().unwrap())
        .collect();
    assert_eq!(result.dates, expected);
    assert!(result.exhausted);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn invalid_calendar_inputs_never_create_or_replace_schedules(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let (_, existing) = request(
        &app,
        "POST",
        "/v1/schedules",
        spec(json!({"type":"delay", "seconds":3600})),
    )
    .await;
    let uri = format!("/v1/schedules/{}", existing["id"].as_str().unwrap());
    let mut triggers = vec![
        json!({"type":"once", "at":"-5000-01-01T00:00:00Z"}),
        json!({"type":"once", "at":"-4713-11-23T23:59:59.999999Z"}),
    ];
    triggers.extend(
        invalid_rules()
            .into_iter()
            .map(|value| json!({"type":"rrule", "value":value})),
    );
    for trigger in triggers {
        for (method, path, body) in [
            (
                "POST",
                "/v1/preview",
                json!({"trigger":trigger, "after":"2029-12-31T00:00:00Z"}),
            ),
            ("POST", "/v1/schedules", spec(trigger.clone())),
            (
                "PUT",
                &uri,
                json!({"expected_revision":1,"spec":spec(trigger.clone())}),
            ),
        ] {
            let (status, body) = request(&app, method, path, body).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{method} {path} {trigger}: {body}"
            );
        }
    }
    let schedules = store.list_schedules("a", 100, 0).await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].revision, 1);
    assert_eq!(serde_json::to_value(&schedules[0]).unwrap(), existing);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn once_timestamp_postgres_lower_boundary_is_supported(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let (status, body) = request(
        &app,
        "POST",
        "/v1/schedules",
        spec(json!({"type":"once", "at":"-4713-11-24T00:00:00Z"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let schedule = store.list_schedules("a", 1, 0).await.unwrap().remove(0);
    assert_eq!(
        schedule.next_fire_at.unwrap(),
        "-4713-11-24T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    assert!(
        store
            .materialize(&schedule, &[schedule.next_fire_at.unwrap()], None)
            .await
            .unwrap()
    );
    assert_eq!(
        store.claim().await.unwrap().unwrap().scheduled_at,
        schedule.next_fire_at.unwrap()
    );
}

async fn ready_schedule(store: &Store, at: DateTime<Utc>) -> scheduler_service::domain::Schedule {
    let spec: ScheduleSpec = serde_json::from_value(spec(json!({"type":"once", "at":at}))).unwrap();
    let schedule = store.create("a", spec, at, None, "test").await.unwrap();
    assert!(store.materialize(&schedule, &[at], None).await.unwrap());
    schedule
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn metrics_exclude_error_schedules_and_superseded_runs(pool: PgPool) {
    let store = Store { pool };
    let now = store.now().await.unwrap();
    let errored = ready_schedule(&store, now - chrono::Duration::days(2)).await;
    let stale = ready_schedule(&store, now - chrono::Duration::days(1)).await;
    sqlx::query("UPDATE schedules SET status = 'error' WHERE id = $1")
        .bind(errored.id)
        .execute(&store.pool)
        .await
        .unwrap();
    // Simulate an older revision left by a previous deployment or data repair.
    sqlx::query("UPDATE schedules SET revision = revision + 1 WHERE id = $1")
        .bind(stale.id)
        .execute(&store.pool)
        .await
        .unwrap();
    let metrics = store.metrics("a").await.unwrap();
    assert!(metrics.contains("scheduler_ready_runs 0\n"), "{metrics}");
    assert!(metrics.contains("scheduler_oldest_ready_age_seconds 0\n"));
    assert!(metrics.contains("scheduler_runs{status=\"pending\"} 2\n"));
    ready_schedule(&store, now - chrono::Duration::seconds(1)).await;
    assert!(
        store
            .metrics("a")
            .await
            .unwrap()
            .contains("scheduler_ready_runs 1\n")
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn claim_fences_each_candidate_without_spending_tokens_on_ineligible_runs(pool: PgPool) {
    let store = Store { pool };
    let now = store.now().await.unwrap() - chrono::Duration::minutes(1);
    let errored = ready_schedule(&store, now - chrono::Duration::days(1)).await;
    sqlx::query("UPDATE schedules SET status = 'error' WHERE id = $1")
        .bind(errored.id)
        .execute(&store.pool)
        .await
        .unwrap();
    let current = ready_schedule(&store, now).await;
    // The selected schedule has both an older stale run and a current run.
    sqlx::query("UPDATE schedules SET revision = revision + 1 WHERE id = $1")
        .bind(current.id)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs (id, schedule_id, tenant_id, revision, scheduled_at, available_at, status, spec)
        SELECT gen_random_uuid(), schedule_id, tenant_id, revision + 1, scheduled_at + INTERVAL '1 second', available_at + INTERVAL '1 second', 'pending', spec FROM runs WHERE schedule_id = $1")
        .bind(current.id).execute(&store.pool).await.unwrap();
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
    let run = store.claim().await.unwrap().unwrap();
    assert_eq!(run.schedule_id, current.id);
    assert_eq!(run.revision, current.revision + 1);
    assert!(store.claim().await.unwrap().is_none());
    assert_eq!(
        store.tenant_capacity("a").await.unwrap().available_tokens,
        Some(1.0)
    );
    let attempts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attempts")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(attempts, 1);
}
