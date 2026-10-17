//! Batch imports, immutable run search, and saved-rule occurrence expansion.
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
    domain::ScheduleSpec,
    evaluator::Evaluator,
    store::Store,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

fn app(store: Store) -> Router {
    app_with_evaluator(store, env!("CARGO_BIN_EXE_scheduler-service"))
}

fn app_with_evaluator(store: Store, executable: &str) -> Router {
    api::router(AppState::new(
        store,
        Evaluator::new(executable.into(), Duration::from_secs(5)),
        &BTreeMap::from([
            ("a".into(), "feature-tenant-a-test-key".into()),
            ("b".into(), "feature-tenant-b-test-key".into()),
        ]),
    ))
}

async fn request(
    app: &Router,
    method: &str,
    path: &str,
    body: Value,
    tenant: &str,
    keys: &[&str],
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(
            "authorization",
            format!("Bearer feature-tenant-{tenant}-test-key"),
        )
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
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes))),
    )
}

fn spec(name: &str) -> Value {
    json!({"name":name, "trigger":{"type":"delay", "seconds":86400}, "target":{"url":"https://example.invalid/hook"}})
}

fn query(path: &str, pairs: &[(&str, &str)]) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.extend_pairs(pairs.iter().copied());
    format!("{path}?{}", query.finish())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn batch_preserves_input_order_common_delay_anchor_and_tenant_isolation(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let mut schedules: Vec<_> = (0..20).map(|n| spec(&format!("batch-{n}"))).collect();
    schedules[17]["trigger"] = json!({"type":"once", "at":"2030-01-01T09:00:00Z"});
    schedules[18]["trigger"] =
        json!({"type":"cron", "expression":"0 9 * * *", "timezone":"Asia/Shanghai"});
    schedules[19]["trigger"] =
        json!({"type":"rrule", "value":"DTSTART:20300101T090000Z\nRRULE:FREQ=DAILY;COUNT=2"});
    let body = json!({"schedules":schedules});
    let (status, created) = request(
        &app,
        "POST",
        "/v1/schedules/batch",
        body.clone(),
        "a",
        &["import-1"],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created.as_array().unwrap().len(), 20);
    for n in 0..20 {
        assert_eq!(created[n]["spec"]["name"], format!("batch-{n}"));
        assert_eq!(created[n]["tenant_id"], "a");
        if n < 17 {
            assert_eq!(created[n]["next_fire_at"], created[0]["next_fire_at"]);
        }
    }
    let restarted = app_with_evaluator(store.clone(), "/missing-feature-evaluator");
    let (status, repeated) = request(
        &restarted,
        "POST",
        "/v1/schedules/batch",
        body.clone(),
        "a",
        &["import-1"],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{repeated}");
    assert_eq!(
        created, repeated,
        "retries must not reevaluate or move delayed jobs"
    );
    let (status, other) = request(
        &app,
        "POST",
        "/v1/schedules/batch",
        body.clone(),
        "b",
        &["import-1"],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_ne!(other[0]["id"], created[0]["id"]);
    assert_eq!(other[0]["tenant_id"], "b");
    let (status, _) = request(
        &app,
        "POST",
        "/v1/schedules",
        spec("single"),
        "a",
        &["import-1"],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "single and batch keys have separate namespaces"
    );
    let mut changed = body;
    changed["schedules"][0]["name"] = json!("different");
    let (status, _) = request(
        &app,
        "POST",
        "/v1/schedules/batch",
        changed,
        "a",
        &["import-1"],
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(store.list_schedules("a", 100, 0).await.unwrap().len(), 21);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn batch_retries_return_current_schedules_in_original_order_after_edits(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let body = json!({"schedules":[spec("first"), spec("second")]});
    let (_, created) = request(
        &app,
        "POST",
        "/v1/schedules/batch",
        body.clone(),
        "a",
        &["editable"],
    )
    .await;
    let id: Uuid = created[0]["id"].as_str().unwrap().parse().unwrap();
    let schedule = store.get_schedule("a", id).await.unwrap();
    let mut replacement = schedule.spec.0;
    replacement.name = "edited".into();
    store
        .replace("a", id, 1, replacement, schedule.next_fire_at.unwrap())
        .await
        .unwrap();
    store.transition("a", id, "cancel").await.unwrap();
    let (status, repeated) = request(
        &app,
        "POST",
        "/v1/schedules/batch",
        body,
        "a",
        &["editable"],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(repeated[0]["id"], created[0]["id"]);
    assert_eq!(repeated[0]["revision"], 2);
    assert_eq!(repeated[0]["status"], "cancelled");
    assert_eq!(repeated[0]["spec"]["name"], "edited");
    assert_eq!(repeated[1], created[1]);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn invalid_batches_never_write_schedules_or_reserve_the_key(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let good = json!({"schedules":[spec("good")]});
    for keys in [vec![], vec![""], vec!["x", "x"]] {
        let (status, error) = request(
            &app,
            "POST",
            "/v1/schedules/batch",
            good.clone(),
            "a",
            &keys,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    }
    let mut invalid_rule = spec("bad rule");
    invalid_rule["trigger"] = json!({"type":"rrule", "value":"DTSTART:20300101T090000Z\nRRULE:FREQ=MONTHLY;BYDAY=0MO;COUNT=2"});
    let mut invalid_target = spec("bad target");
    invalid_target["target"]["url"] = json!("file:///tmp/hook");
    for body in [
        json!({"schedules":[]}),
        json!({"schedules":vec![spec("too many"); 21]}),
        json!({"schedules":[spec("first"), invalid_target]}),
        json!({"schedules":[spec("first"), spec("second"), spec("third"), spec("fourth"), invalid_rule]}),
    ] {
        let (status, error) = request(
            &app,
            "POST",
            "/v1/schedules/batch",
            body,
            "a",
            &["retry-after-fix"],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        assert!(store.list_schedules("a", 100, 0).await.unwrap().is_empty());
        let keys: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM schedule_batches")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(keys, 0);
    }
    let (status, _) = request(
        &app,
        "POST",
        "/v1/schedules/batch",
        good,
        "a",
        &["retry-after-fix"],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn batch_database_failure_rolls_back_schedules_tenant_and_idempotency_record(pool: PgPool) {
    let store = Store { pool };
    sqlx::raw_sql("CREATE FUNCTION reject_import() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.spec->>'name' = 'reject' THEN RAISE EXCEPTION 'test batch failure'; END IF;
        RETURN NEW; END $$;
        CREATE TRIGGER reject_import BEFORE INSERT ON schedules FOR EACH ROW EXECUTE FUNCTION reject_import();")
        .execute(&store.pool).await.unwrap();
    let app = app(store.clone());
    let body = json!({"schedules":[spec("first"), spec("reject")]});
    let (status, _) = request(
        &app,
        "POST",
        "/v1/schedules/batch",
        body.clone(),
        "a",
        &["transaction"],
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    for table in ["schedules", "schedule_batches", "tenant_dispatch"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "partial write to {table}");
    }
    sqlx::raw_sql("DROP TRIGGER reject_import ON schedules; DROP FUNCTION reject_import();")
        .execute(&store.pool)
        .await
        .unwrap();
    let (status, _) = request(
        &app,
        "POST",
        "/v1/schedules/batch",
        body,
        "a",
        &["transaction"],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn concurrent_batch_retries_commit_once_and_conflicting_content_cannot_partially_import(
    pool: PgPool,
) {
    let store = Store { pool };
    let app = app(store.clone());
    let body = json!({"schedules":[spec("first"), spec("second")]});
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let app = app.clone();
        let body = body.clone();
        tasks.spawn(async move {
            request(
                &app,
                "POST",
                "/v1/schedules/batch",
                body,
                "a",
                &["concurrent"],
            )
            .await
        });
    }
    let mut created = 0;
    let mut winner = None;
    while let Some(result) = tasks.join_next().await {
        let (status, response) = result.unwrap();
        assert!(
            matches!(status, StatusCode::OK | StatusCode::CREATED),
            "{response}"
        );
        created += usize::from(status == StatusCode::CREATED);
        if let Some(ref winner) = winner {
            assert_eq!(*winner, response);
        } else {
            winner = Some(response);
        }
    }
    assert_eq!(created, 1);
    let other_body = json!({"schedules":[spec("third"), spec("fourth")]});
    let (one, two) = tokio::join!(
        request(&app, "POST", "/v1/schedules/batch", body, "a", &["race"]),
        request(
            &app,
            "POST",
            "/v1/schedules/batch",
            other_body,
            "a",
            &["race"]
        )
    );
    assert!([one.0, two.0].contains(&StatusCode::CREATED));
    assert!([one.0, two.0].contains(&StatusCode::CONFLICT));
    assert_eq!(store.list_schedules("a", 100, 0).await.unwrap().len(), 4);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn run_search_matches_immutable_metadata_half_open_times_and_tenant_scope(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let dates: Vec<DateTime<Utc>> = [
        "2030-01-01T09:00:00Z",
        "2030-01-02T09:00:00Z",
        "2030-01-03T09:00:00Z",
    ]
    .into_iter()
    .map(|date| date.parse().unwrap())
    .collect();
    let mut input = spec("snapshot");
    input["metadata"] =
        json!({"order.id":"42_%'\\", "env":"production", "empty":"", "__proto__":"literal"});
    let definition: ScheduleSpec = serde_json::from_value(input).unwrap();
    let mut original = None;
    for tenant in ["a", "b"] {
        let schedule = store
            .create(tenant, definition.clone(), dates[0], None, "test")
            .await
            .unwrap();
        store.materialize(&schedule, &dates, None).await.unwrap();
        if tenant == "a" {
            original = Some(schedule);
        }
    }
    let schedule = original.unwrap();
    let mut revised = definition;
    revised.metadata.insert("order.id".into(), "updated".into());
    store
        .replace("a", schedule.id, 1, revised, dates[2])
        .await
        .unwrap();
    let id = schedule.id.to_string();
    let metadata = r#"{"order.id":"42_%'\\","env":"production","empty":"","__proto__":"literal"}"#;
    let path = query(
        "/v1/runs",
        &[
            ("metadata", metadata),
            ("from", "2030-01-02T17:00:00+08:00"),
            ("until", "2030-01-03T09:00:00Z"),
            ("schedule_id", &id),
            ("status", "cancelled"),
        ],
    );
    let (status, rows) = request(&app, "GET", &path, Value::Null, "a", &[]).await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["scheduled_at"], "2030-01-02T09:00:00Z");
    assert_eq!(rows[0]["spec"]["metadata"]["order.id"], "42_%'\\");
    let (_, other) = request(&app, "GET", &path, Value::Null, "b", &[]).await;
    assert_eq!(other, json!([]));
    let path = query("/v1/runs", &[("metadata", r#"{"order.id":"updated"}"#)]);
    let (_, rows) = request(&app, "GET", &path, Value::Null, "a", &[]).await;
    assert_eq!(rows, json!([]));
    let path = query("/v1/runs", &[("metadata", "{}"), ("limit", "2")]);
    let (_, first) = request(&app, "GET", &path, Value::Null, "a", &[]).await;
    let (_, last) = request(
        &app,
        "GET",
        &format!("{path}&offset=2"),
        Value::Null,
        "a",
        &[],
    )
    .await;
    assert_eq!(first.as_array().unwrap().len(), 2);
    assert_eq!(last.as_array().unwrap().len(), 1);
    assert!(
        first
            .as_array()
            .unwrap()
            .iter()
            .all(|run| run["id"] != last[0]["id"])
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn run_search_rejects_invalid_ranges_metadata_and_unrepresentable_timestamps(pool: PgPool) {
    let app = app(Store { pool });
    for pairs in [
        vec![
            ("from", "2030-01-02T00:00:00Z"),
            ("until", "2030-01-01T00:00:00Z"),
        ],
        vec![
            ("from", "2030-01-01T00:00:00Z"),
            ("until", "2030-01-01T00:00:00Z"),
        ],
        vec![("from", "2030-01-01T00:00:00.0000001Z")],
        vec![("from", "-5000-01-01T00:00:00Z")],
        vec![("until", "2030-01-01T00:00:60Z")],
        vec![("metadata", "[]")],
        vec![("metadata", r#"{"x":1}"#)],
        vec![("metadata", r#"{"":"x"}"#)],
        vec![("metadata", "not json")],
    ] {
        let path = query("/v1/runs", &pairs);
        let (status, error) = request(&app, "GET", &path, Value::Null, "a", &[]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {error}");
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn saved_occurrences_preserve_exceptions_status_and_cursor_without_creating_runs(
    pool: PgPool,
) {
    let store = Store { pool };
    let app = app(store.clone());
    let mut input = spec("recurring");
    input["trigger"] = json!({"type":"rrule", "value":"DTSTART;TZID=Asia/Shanghai:20300101T090000\nRRULE:FREQ=DAILY;COUNT=3\nEXDATE:20300102T010000Z\nRDATE:20300105T010000Z"});
    let (status, created) = request(&app, "POST", "/v1/schedules", input, "a", &[]).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
    let path = query(
        &format!("/v1/schedules/{id}/occurrences"),
        &[("after", "2030-01-01T01:00:00Z"), ("count", "10")],
    );
    for (action, expected_status) in [("pause", "paused"), ("cancel", "cancelled")] {
        let before = store.transition("a", id, action).await.unwrap();
        let (status, expanded) = request(&app, "GET", &path, Value::Null, "a", &[]).await;
        assert_eq!(status, StatusCode::OK, "{expanded}");
        assert_eq!(
            expanded["dates"],
            json!(["2030-01-03T01:00:00Z", "2030-01-05T01:00:00Z"])
        );
        assert_eq!(expanded["exhausted"], true);
        assert_eq!(expanded["status"], expected_status);
        assert_eq!(expanded["revision"], 1);
        assert_eq!(expanded["schedule_id"], created["id"]);
        assert_eq!(
            serde_json::to_value(store.get_schedule("a", id).await.unwrap()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
        assert!(
            store
                .list_runs("a", None, None, 100, 0)
                .await
                .unwrap()
                .is_empty()
        );
    }
    let (status, _) = request(&app, "GET", &path, Value::Null, "b", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    for suffix in ["count=0", "count=257", "unknown=1", "after=bad"] {
        let path = format!("/v1/schedules/{id}/occurrences?{suffix}");
        let (status, error) = request(&app, "GET", &path, Value::Null, "a", &[]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn saved_delay_occurrences_use_the_original_absolute_time(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let (_, created) = request(&app, "POST", "/v1/schedules", spec("delayed"), "a", &[]).await;
    let path = format!(
        "/v1/schedules/{}/occurrences?count=5",
        created["id"].as_str().unwrap()
    );
    for _ in 0..2 {
        let (status, result) = request(&app, "GET", &path, Value::Null, "a", &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["dates"], json!([created["spec"]["trigger"]["at"]]));
        assert_eq!(result["exhausted"], true);
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn selective_run_search_uses_metadata_and_time_indexes(pool: PgPool) {
    let store = Store { pool };
    let input: ScheduleSpec = serde_json::from_value(spec("query fixture")).unwrap();
    let schedule = store
        .create(
            "a",
            input,
            "2030-01-01T00:00:00Z".parse().unwrap(),
            None,
            "fixture",
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs (id, schedule_id, tenant_id, revision, scheduled_at, available_at, status, spec)
        SELECT gen_random_uuid(), s.id, s.tenant_id, 1,
            '2030-01-01T00:00:00Z'::timestamptz + n * INTERVAL '1 second',
            '2030-01-01T00:00:00Z'::timestamptz + n * INTERVAL '1 second', 'pending',
            s.spec || jsonb_build_object('metadata', jsonb_build_object('order_id', n::text))
        FROM schedules s CROSS JOIN generate_series(1,5000) n WHERE s.id = $1")
        .bind(schedule.id).execute(&store.pool).await.unwrap();
    sqlx::query("ANALYZE runs")
        .execute(&store.pool)
        .await
        .unwrap();
    let metadata: Value = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT * FROM runs WHERE tenant_id = $1 AND (spec -> 'metadata') @> $2
        ORDER BY created_at DESC, id DESC LIMIT 50",
    )
    .bind("a")
    .bind(sqlx::types::Json(json!({"order_id":"42"})))
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert!(metadata.to_string().contains("runs_metadata"), "{metadata}");
    assert_eq!(metadata[0]["Plan"]["Actual Rows"], 1);
    let time: Value = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT * FROM runs WHERE tenant_id = $1 AND scheduled_at >= $2 AND scheduled_at < $3
        ORDER BY created_at DESC, id DESC LIMIT 50",
    )
    .bind("a")
    .bind("2030-01-01T00:00:42Z".parse::<DateTime<Utc>>().unwrap())
    .bind("2030-01-01T00:00:43Z".parse::<DateTime<Utc>>().unwrap())
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert!(time.to_string().contains("runs_tenant_scheduled"), "{time}");
    assert_eq!(time[0]["Plan"]["Actual Rows"], 1);
}
