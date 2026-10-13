//! HTTP framing and header validation regressions using real local callbacks.
use std::{collections::BTreeMap, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use scheduler_service::{
    api::{self, AppState},
    domain::{Run, ScheduleSpec},
    evaluator::Evaluator,
    store::Store,
    webhook,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

async fn callback(response: Vec<u8>, stall: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            let count = stream.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&buffer[..count]);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let length: usize = String::from_utf8_lossy(&request[..end])
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                if request.len() >= end + 4 + length {
                    break;
                }
            }
        }
        stream.write_all(&response).await.unwrap();
        if stall {
            std::future::pending::<()>().await;
        }
    });
    (format!("http://localhost:{}/hook", address.port()), server)
}

async fn run(store: &Store, url: String) -> Run {
    let at = store.now().await.unwrap() - chrono::Duration::seconds(1);
    let spec: ScheduleSpec = serde_json::from_value(json!({
        "name":"webhook audit", "trigger":{"type":"once", "at":at},
        "target":{"url":url,"timeout_seconds":1}, "retry":{"max_attempts":1}
    }))
    .unwrap();
    let schedule = store.create("a", spec, at, None, "test").await.unwrap();
    assert!(store.materialize(&schedule, &[at], None).await.unwrap());
    store.claim().await.unwrap().unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn errors_after_excerpt_limit_remain_delivery_failures(pool: PgPool) {
    let store = Store { pool };
    for (status, malformed_chunk) in [(200, false), (503, false), (200, true)] {
        let mut response =
            format!("HTTP/1.1 {status} Test\r\nRetry-After: 7\r\nConnection: close\r\n")
                .into_bytes();
        if malformed_chunk {
            response.extend_from_slice(b"Transfer-Encoding: chunked\r\n\r\n1000\r\n");
            response.extend(vec![b'x'; 4096]);
            response.extend_from_slice(b"\r\ninvalid-chunk-size\r\n");
        } else {
            response.extend_from_slice(b"Content-Length: 8192\r\n\r\n");
            response.extend(vec![b'x'; 4096]);
        }
        let (url, server) = callback(response, false).await;
        let run = run(&store, url).await;
        let result = webhook::deliver(&run, true).await;
        server.await.unwrap();
        assert!(!result.success, "truncated response was acknowledged");
        assert_eq!(result.http_status, Some(status));
        assert!(result.error.as_deref().unwrap().contains("response body"));
        assert_eq!(result.response_excerpt.as_deref().unwrap().len(), 4096);
        if status == 503 {
            assert_eq!(
                result.retry_after,
                Some(scheduler_service::domain::RetryAfter::Seconds(7))
            );
        }
        assert!(store.finish(&run, result).await.unwrap());
        assert_eq!(store.get_run("a", run.id).await.unwrap().status, "dead");
        let attempt = store.attempts("a", run.id).await.unwrap().remove(0);
        assert_eq!(attempt.status, "failed");
        assert_eq!(attempt.http_status, Some(status));
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn stalled_body_after_excerpt_limit_still_obeys_total_timeout(pool: PgPool) {
    let store = Store { pool };
    let mut response = b"HTTP/1.1 200 Test\r\nContent-Length: 8192\r\n\r\n".to_vec();
    response.extend(vec![b'x'; 4096]);
    let (url, server) = callback(response, true).await;
    let run = run(&store, url).await;
    let result = tokio::time::timeout(Duration::from_secs(3), webhook::deliver(&run, true))
        .await
        .unwrap();
    server.abort();
    assert!(!result.success);
    assert_eq!(result.http_status, Some(200));
    assert!(result.error.as_deref().unwrap().contains("response body"));
    assert_eq!(result.response_excerpt.as_deref().unwrap().len(), 4096);
    assert!(store.finish(&run, result).await.unwrap());
    assert_eq!(store.get_run("a", run.id).await.unwrap().status, "dead");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn complete_large_body_succeeds_with_a_bounded_excerpt(pool: PgPool) {
    let store = Store { pool };
    let mut response =
        b"HTTP/1.1 200 Test\r\nContent-Length: 8192\r\nConnection: close\r\n\r\n".to_vec();
    response.extend(vec![b'x'; 8192]);
    let (url, server) = callback(response, false).await;
    let run = run(&store, url).await;
    let result = webhook::deliver(&run, true).await;
    server.await.unwrap();
    assert!(result.success, "{:?}", result.error);
    assert_eq!(result.response_excerpt.as_deref().unwrap().len(), 4096);
    assert!(store.finish(&run, result).await.unwrap());
    assert_eq!(
        store.get_run("a", run.id).await.unwrap().status,
        "succeeded"
    );
}

fn app(store: Store) -> Router {
    api::router(AppState::new(
        store,
        Evaluator::new(
            env!("CARGO_BIN_EXE_scheduler-service").into(),
            Duration::from_secs(5),
        ),
        &BTreeMap::from([("a".into(), "webhook-audit-test-key".into())]),
    ))
}

async fn request(app: &Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", "Bearer webhook-audit-test-key")
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
async fn duplicate_target_headers_are_case_insensitive_and_never_mutate_schedules(pool: PgPool) {
    let store = Store { pool };
    let app = app(store.clone());
    let mut body = json!({"name":"headers", "trigger":{"type":"delay","seconds":3600},
        "target":{"url":"https://example.invalid/hook", "headers":{"AuThOrIzAtIoN":"test-only"}}});
    let (status, existing) = request(&app, "POST", "/v1/schedules", body.clone()).await;
    assert_eq!(status, StatusCode::CREATED);
    for headers in [
        json!({"Authorization":"first", "authorization":"second"}),
        json!({"X-Api-Key":"first", "x-api-key":"second"}),
        json!({"Authorization":"same", "AUTHORIZATION":"same"}),
    ] {
        body["target"]["headers"] = headers;
        let (status, response) = request(&app, "POST", "/v1/schedules", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
        let (status, response) = request(
            &app,
            "PUT",
            &format!("/v1/schedules/{}", existing["id"].as_str().unwrap()),
            json!({"expected_revision":1, "spec":body}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    }
    let schedules = store.list_schedules("a", 100, 0).await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(serde_json::to_value(&schedules[0]).unwrap(), existing);
}
