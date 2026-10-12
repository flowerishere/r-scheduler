//! Business metadata, legacy compatibility and recurrence boundary regressions.
use std::{collections::BTreeMap, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use scheduler_service::{
    api::{self, AppState},
    domain::ScheduleSpec,
    evaluator::Evaluator,
    store::Store,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

fn app(store: Store) -> Router {
    api::router(AppState::new(
        store,
        Evaluator::new(
            env!("CARGO_BIN_EXE_scheduler-service").into(),
            Duration::from_secs(5),
        ),
        &BTreeMap::from([
            ("a".into(), "tenant-a-test-secret".into()),
            ("b".into(), "tenant-b-test-secret".into()),
        ]),
    ))
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
            format!("Bearer tenant-{tenant}-test-secret"),
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
    (
        status,
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned())),
    )
}

fn spec(metadata: Value) -> Value {
    json!({"name":"invoice task", "trigger":{"type":"delay","seconds":86400},
        "target":{"url":"https://example.invalid/hook"}, "metadata":metadata})
}

fn query(metadata: &str, extra: &[(&str, &str)]) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query
        .append_pair("metadata", metadata)
        .extend_pairs(extra.iter().copied());
    format!("/v1/schedules?{}", query.finish())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn metadata_filters_are_literal_combined_and_tenant_scoped(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let metadata =
        json!({"kind":"invoice", "order.id":"42_%'\\", "__proto__":"<img src=x>", "empty":""});
    let (status, created) = request(
        &app,
        "POST",
        "/v1/schedules",
        spec(metadata.clone()),
        "a",
        Some("business-object"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id: Uuid = serde_json::from_value(created["id"].clone()).unwrap();
    let (_, other) = request(
        &app,
        "POST",
        "/v1/schedules",
        spec(metadata.clone()),
        "b",
        None,
    )
    .await;
    assert_ne!(created["id"], other["id"]);
    request(
        &app,
        "POST",
        "/v1/schedules",
        spec(json!({"kind":"invoice", "order.id":"43"})),
        "a",
        None,
    )
    .await;
    let mut legacy = spec(json!({}));
    legacy.as_object_mut().unwrap().remove("metadata");
    let (_, legacy) = request(&app, "POST", "/v1/schedules", legacy, "a", None).await;
    assert!(legacy["spec"].get("metadata").is_none());
    for filter in [
        json!({"order.id":"42_%'\\"}),
        json!({"kind":"invoice", "empty":""}),
        json!({"__proto__":"<img src=x>"}),
    ] {
        let (status, rows) = request(
            &app,
            "GET",
            &query(
                &filter.to_string(),
                &[("q", "invoice"), ("status", "active")],
            ),
            Value::Null,
            "a",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["id"], created["id"]);
    }
    for filter in [
        json!({"kind":"Invoice"}),
        json!({"kind":"invoice", "order.id":"no-match"}),
        json!({"order":{"id":"42_%'\\"}}),
    ] {
        let (status, rows) = request(
            &app,
            "GET",
            &query(&filter.to_string(), &[]),
            Value::Null,
            "a",
            None,
        )
        .await;
        if status == StatusCode::OK {
            assert_eq!(rows, json!([]));
        } else {
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
    }
    let (_, all) = request(&app, "GET", &query("{}", &[]), Value::Null, "a", None).await;
    assert_eq!(all.as_array().unwrap().len(), 3);
    let (_, page) = request(
        &app,
        "GET",
        &query("{}", &[("limit", "1"), ("offset", "1")]),
        Value::Null,
        "a",
        None,
    )
    .await;
    assert_eq!(page[0], all[1]);

    // The run's business context is immutable even when the schedule is edited.
    let schedule = store.get_schedule("a", id).await.unwrap();
    store
        .materialize(&schedule, &[schedule.next_fire_at.unwrap()], None)
        .await
        .unwrap();
    let run = store
        .list_runs("a", Some(id), None, 10, 0)
        .await
        .unwrap()
        .remove(0);
    let body = json!({"expected_revision":1,"spec":spec(json!({"kind":"revised"}))});
    let (status, updated) =
        request(&app, "PUT", &format!("/v1/schedules/{id}"), body, "a", None).await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["revision"], 2);
    assert_eq!(
        store.get_run("a", run.id).await.unwrap().spec.metadata,
        serde_json::from_value(metadata).unwrap()
    );
    assert_eq!(
        store.get_run("a", run.id).await.unwrap().status,
        "cancelled"
    );
    let (_, old) = request(
        &app,
        "GET",
        &query(r#"{"kind":"invoice","empty":""}"#, &[]),
        Value::Null,
        "a",
        None,
    )
    .await;
    assert_eq!(old, json!([]));
    let (_, new) = request(
        &app,
        "GET",
        &query(r#"{"kind":"revised"}"#, &[]),
        Value::Null,
        "a",
        None,
    )
    .await;
    assert_eq!(new[0]["id"], created["id"]);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn invalid_metadata_never_mutates_schedules(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let (_, created) = request(
        &app,
        "POST",
        "/v1/schedules",
        spec(json!({"valid":"value"})),
        "a",
        None,
    )
    .await;
    let too_many: BTreeMap<_, _> = (0..33).map(|n| (n.to_string(), "value")).collect();
    let too_large: BTreeMap<_, _> = (0..32).map(|n| (n.to_string(), "x".repeat(512))).collect();
    let invalid = [
        json!({"": "x"}),
        json!({" ":"x"}),
        json!({"x":null}),
        json!({"x":2}),
        json!({"x":{}}),
        json!([]),
        Value::Null,
        json!({"x":"\u{0000}"}),
        json!({"x":"\n"}),
        json!({"x\t":"x"}),
        json!({"x":"字".repeat(171)}),
        json!({"字".repeat(22):"x"}),
        json!(too_many),
        json!(too_large),
    ];
    for metadata in invalid {
        let (status, error) = request(
            &app,
            "POST",
            "/v1/schedules",
            spec(metadata.clone()),
            "a",
            None,
        )
        .await;
        assert!(
            matches!(
                status,
                StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
            ),
            "{error}"
        );
        let (status, _) = request(
            &app,
            "PUT",
            &format!("/v1/schedules/{}", created["id"].as_str().unwrap()),
            json!({"expected_revision":1,"spec":spec(metadata.clone())}),
            "a",
            None,
        )
        .await;
        assert!(matches!(
            status,
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
        ));
        let (status, _) = request(
            &app,
            "GET",
            &query(&metadata.to_string(), &[]),
            Value::Null,
            "a",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    for invalid in ["{broken".to_owned(), " ".repeat(8193)] {
        assert_eq!(
            request(&app, "GET", &query(&invalid, &[]), Value::Null, "a", None)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let schedules = store.list_schedules("a", 100, 0).await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].revision, 1);
    assert_eq!(schedules[0].spec.metadata["valid"], "value");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn metadata_preserves_legacy_hashes_and_canonical_idempotency(pool: PgPool) {
    // Exact pre-0.5 ScheduleSpec serialization, including original field order.
    let legacy = r#"{"name":"legacy","trigger":{"type":"once","at":"2030-01-01T00:00:00Z"},"target":{"url":"https://example.invalid/hook","headers":{},"timeout_seconds":30},"payload":null,"retry":{"max_attempts":5,"initial_delay_seconds":5,"max_delay_seconds":3600},"misfire":"fire_once","misfire_grace_seconds":60,"concurrency":"allow"}"#;
    let old_spec: ScheduleSpec = serde_json::from_str(legacy).unwrap();
    assert_eq!(serde_json::to_string(&old_spec).unwrap(), legacy);
    let store = Store { pool };
    let at = "2030-01-01T00:00:00Z".parse().unwrap();
    let old = store
        .create(
            "a",
            old_spec,
            at,
            Some("legacy"),
            &format!("{:x}", Sha256::digest(legacy)),
        )
        .await
        .unwrap();
    let app = app(store.clone());
    let mut body: Value = serde_json::from_str(legacy).unwrap();
    for empty in [false, true] {
        if empty {
            body["metadata"] = json!({});
        }
        let (status, replay) = request(
            &app,
            "POST",
            "/v1/schedules",
            body.clone(),
            "a",
            Some("legacy"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert_eq!(replay["id"], old.id.to_string());
    }
    body["metadata"] = serde_json::from_str(r#"{"z":"last","a":"first"}"#).unwrap();
    let (_, first) = request(
        &app,
        "POST",
        "/v1/schedules",
        body.clone(),
        "a",
        Some("ordered"),
    )
    .await;
    body["metadata"] = serde_json::from_str(r#"{"a":"first","z":"last"}"#).unwrap();
    let (status, second) = request(
        &app,
        "POST",
        "/v1/schedules",
        body.clone(),
        "a",
        Some("ordered"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["id"], second["id"]);
    body["metadata"]["a"] = json!("changed");
    assert_eq!(
        request(&app, "POST", "/v1/schedules", body, "a", Some("ordered"))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(store.list_schedules("a", 100, 0).await.unwrap().len(), 2);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn selective_metadata_lookup_uses_the_gin_index(pool: PgPool) {
    sqlx::query("INSERT INTO schedules (id, tenant_id, spec, status, request_hash)
        SELECT gen_random_uuid(), 'a', jsonb_build_object('metadata', jsonb_build_object('order_id', n::text)),
        'completed', 'fixture' FROM generate_series(1,5000) n").execute(&pool).await.unwrap();
    sqlx::query("ANALYZE schedules")
        .execute(&pool)
        .await
        .unwrap();
    let plan: Value = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT * FROM schedules WHERE tenant_id=$1 AND (spec -> 'metadata') @> $2
        ORDER BY created_at DESC, id DESC LIMIT $3 OFFSET $4",
    )
    .bind("a")
    .bind(sqlx::types::Json(json!({"order_id":"42"})))
    .bind(50_i64)
    .bind(0_i64)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(plan.to_string().contains("schedules_metadata"), "{plan}");
    assert_eq!(plan[0]["Plan"]["Actual Rows"], 1);
}
