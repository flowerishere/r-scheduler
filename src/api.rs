use std::{collections::BTreeMap, error::Error, sync::Arc, time::Duration};

use axum::{
    Extension, Json, Router,
    body::{Body, to_bytes},
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, Version, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::{
    domain::{Attempt, Run, Schedule, ScheduleSpec, Trigger},
    evaluator::Evaluator,
    store::{Store, StoreError},
    trigger::{Evaluation, resolve_delay},
    webhook::validate_spec,
};

const MAX_BODY_BYTES: usize = 256 * 1024;
const BODY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_API_REQUESTS: usize = 64;
const MAX_TENANT_REQUESTS: usize = 8;

struct ApiTenant {
    id: String,
    key_hash: [u8; 32],
    requests: Semaphore,
}

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub evaluator: Evaluator,
    keys: Arc<Vec<ApiTenant>>,
    requests: Arc<Semaphore>,
    readiness: Arc<Semaphore>,
}

impl AppState {
    pub fn new(store: Store, evaluator: Evaluator, keys: &BTreeMap<String, String>) -> Self {
        let keys = keys
            .iter()
            .map(|(tenant, key)| ApiTenant {
                id: tenant.clone(),
                key_hash: Sha256::digest(key.as_bytes()).into(),
                requests: Semaphore::new(MAX_TENANT_REQUESTS),
            })
            .collect();
        Self {
            store,
            evaluator,
            keys: Arc::new(keys),
            requests: Arc::new(Semaphore::new(MAX_API_REQUESTS)),
            readiness: Arc::new(Semaphore::new(1)),
        }
    }
}

#[derive(Clone)]
struct Tenant(String);

#[derive(Debug)]
pub struct ApiError(StatusCode, String);

impl ApiError {
    fn busy(status: StatusCode, message: &str) -> Response {
        let mut response = Self(status, message.into()).into_response();
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        response
    }

    fn evaluation(error: anyhow::Error) -> Self {
        if error
            .downcast_ref::<crate::evaluator::EvaluatorUnavailable>()
            .is_some()
        {
            tracing::error!(error = %format!("{error:#}"), "Rule evaluator unavailable");
            return Self(StatusCode::SERVICE_UNAVAILABLE, error.to_string());
        }
        if error
            .downcast_ref::<crate::evaluator::EvaluatorBusy>()
            .is_some()
        {
            Self(StatusCode::SERVICE_UNAVAILABLE, error.to_string())
        } else {
            Self::bad(error)
        }
    }
    fn bad(error: impl std::fmt::Display) -> Self {
        Self(StatusCode::BAD_REQUEST, error.to_string())
    }
    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(%error, "API operation failed");
        Self(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".into(),
        )
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound => Self(StatusCode::NOT_FOUND, "Resource not found".into()),
            StoreError::Conflict(message) => Self(StatusCode::CONFLICT, message),
            StoreError::InvalidInput(message) => Self(StatusCode::BAD_REQUEST, message),
            error => Self::internal(error),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.0, Json(json!({"error": self.1}))).into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if self.0 == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"scheduler\""),
            );
        }
        response
    }
}

type Result<T> = std::result::Result<T, ApiError>;

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/me", get(identity))
        .route("/capacity", get(capacity))
        .route("/preview", post(preview))
        .route("/schedules", post(create).get(list_schedules))
        .route("/schedules/{id}", get(get_schedule).put(replace))
        .route("/schedules/{id}/pause", post(pause))
        .route("/schedules/{id}/resume", post(resume))
        .route("/schedules/{id}/cancel", post(cancel))
        .route("/runs", get(list_runs))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/attempts", get(attempts))
        .route("/runs/{id}/replay", post(replay))
        .route("/stats", get(stats))
        .route("/metrics", get(metrics))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate));
    Router::new()
        .route(
            "/",
            get(|| async {
                Json(json!({"service": "scheduler-service", "version": env!("CARGO_PKG_VERSION")}))
            }),
        )
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/ready", get(ready))
        .merge(crate::console::router())
        .nest("/v1", protected)
        .fallback(|| async { ApiError(StatusCode::NOT_FOUND, "Route not found".into()) })
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn authenticate(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    let mut credentials = request.headers().get_all(header::AUTHORIZATION).iter();
    let token = credentials
        .next()
        .filter(|_| credentials.next().is_none())
        .and_then(|h| h.to_str().ok())
        .and_then(|value| {
            let (scheme, token) = value.split_once(' ')?;
            scheme
                .eq_ignore_ascii_case("Bearer")
                .then(|| token.trim_start_matches(' '))
        })
        .filter(|token| !token.is_empty() && !token.bytes().any(|b| b.is_ascii_whitespace()));
    let Some(token) = token else {
        return ApiError(StatusCode::UNAUTHORIZED, "Bearer API key required".into())
            .into_response();
    };
    let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let tenant = state
        .keys
        .iter()
        .find(|tenant| bool::from(hash.ct_eq(&tenant.key_hash)));
    let Some(tenant) = tenant else {
        return ApiError(StatusCode::UNAUTHORIZED, "Invalid API key".into()).into_response();
    };
    // Fail immediately instead of creating an unbounded queue of bodies or
    // database/evaluator waiters. Both budgets are shared by all router clones.
    let Ok(_tenant_permit) = tenant.requests.try_acquire() else {
        return ApiError::busy(
            StatusCode::TOO_MANY_REQUESTS,
            "Tenant request limit reached",
        );
    };
    let Ok(_api_permit) = state.requests.try_acquire() else {
        return ApiError::busy(StatusCode::SERVICE_UNAVAILABLE, "API request limit reached");
    };
    request.extensions_mut().insert(Tenant(tenant.id.clone()));
    // Buffer before entering any handler so timeout cannot cancel a mutation
    // after it has committed. This is one deadline, never reset by slow chunks.
    let (parts, body) = request.into_parts();
    let body = match tokio::time::timeout(BODY_TIMEOUT, to_bytes(body, MAX_BODY_BYTES)).await {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(error))
            if error
                .source()
                .is_some_and(|e| e.is::<http_body_util::LengthLimitError>()) =>
        {
            Err(ApiError(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Request body exceeds 256 KiB".into(),
            ))
        }
        Ok(Err(_)) => Err(ApiError::bad("Could not read request body")),
        Err(_) => Err(ApiError(
            StatusCode::REQUEST_TIMEOUT,
            "Request body timed out".into(),
        )),
    };
    let body = match body {
        Ok(body) => body,
        Err(error) => {
            let mut response = error.into_response();
            if matches!(parts.version, Version::HTTP_10 | Version::HTTP_11) {
                response
                    .headers_mut()
                    .insert(header::CONNECTION, HeaderValue::from_static("close"));
            }
            return response;
        }
    };
    let mut response = next.run(Request::from_parts(parts, Body::from(body))).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn ready(State(state): State<AppState>) -> Response {
    // Public probes must not consume the API/worker database pool under load.
    let Ok(_permit) = state.readiness.try_acquire() else {
        return ApiError::busy(
            StatusCode::SERVICE_UNAVAILABLE,
            "Readiness check in progress",
        );
    };
    match state.store.now().await {
        Ok(_) => Json(json!({"status": "ready"})).into_response(),
        Err(_) => ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "Database unavailable".into(),
        )
        .into_response(),
    }
}

async fn identity(Extension(tenant): Extension<Tenant>) -> Json<Value> {
    Json(json!({"tenant_id": tenant.0}))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviewRequest {
    trigger: Trigger,
    after: Option<DateTime<Utc>>,
    #[serde(default = "preview_count")]
    count: usize,
}
fn preview_count() -> usize {
    10
}

async fn preview(
    State(state): State<AppState>,
    Json(mut request): Json<PreviewRequest>,
) -> Result<Json<Evaluation>> {
    let after = match request.after {
        Some(after) => after,
        None => state.store.now().await?,
    };
    resolve_delay(&mut request.trigger, after).map_err(ApiError::bad)?;
    let result = state
        .evaluator
        .next(&request.trigger, after, request.count)
        .await
        .map_err(ApiError::evaluation)?;
    Ok(Json(result))
}

async fn prepare(state: &AppState, spec: &mut ScheduleSpec) -> Result<DateTime<Utc>> {
    validate_spec(spec).map_err(ApiError::bad)?;
    let now = state.store.now().await?;
    resolve_delay(&mut spec.trigger, now).map_err(ApiError::bad)?;
    if let Trigger::Once { at } = &spec.trigger {
        return Ok(*at);
    }
    state
        .evaluator
        .next(&spec.trigger, now, 1)
        .await
        .map_err(ApiError::evaluation)?
        .dates
        .first()
        .copied()
        .ok_or_else(|| ApiError::bad("Rule has no future occurrences"))
}

async fn create(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    headers: HeaderMap,
    Json(mut spec): Json<ScheduleSpec>,
) -> Result<(StatusCode, Json<Schedule>)> {
    let key = headers
        .get("idempotency-key")
        .map(|h| h.to_str())
        .transpose()
        .map_err(ApiError::bad)?;
    if key.is_some_and(|key| key.is_empty() || key.len() > 200) {
        return Err(ApiError::bad("Idempotency-Key must contain 1..200 bytes"));
    }
    let hash = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&spec).map_err(ApiError::internal)?)
    );
    if let Some(key) = key
        && let Some(existing) = state.store.find_idempotent(&tenant.0, key, &hash).await?
    {
        return Ok((StatusCode::OK, Json(existing)));
    }
    let next = prepare(&state, &mut spec).await?;
    let schedule = state
        .store
        .create(&tenant.0, spec, next, key, &hash)
        .await?;
    Ok((StatusCode::CREATED, Json(schedule)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplaceRequest {
    expected_revision: i64,
    spec: ScheduleSpec,
}

async fn replace(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
    Json(mut request): Json<ReplaceRequest>,
) -> Result<Json<Schedule>> {
    state.store.get_schedule(&tenant.0, id).await?;
    let next = prepare(&state, &mut request.spec).await?;
    Ok(Json(
        state
            .store
            .replace(&tenant.0, id, request.expected_revision, request.spec, next)
            .await?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    #[serde(default = "page_size")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    schedule_id: Option<Uuid>,
    status: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleQuery {
    #[serde(default = "page_size")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    status: Option<String>,
    q: Option<String>,
}
fn page_size() -> i64 {
    50
}

impl ListQuery {
    fn validate(&self) -> Result<()> {
        if !(1..=100).contains(&self.limit) || !(0..=100_000).contains(&self.offset) {
            return Err(ApiError::bad(
                "limit must be 1..100; offset must be 0..100000",
            ));
        }
        if self.status.as_ref().is_some_and(|s| {
            !matches!(
                s.as_str(),
                "pending" | "running" | "succeeded" | "dead" | "cancelled"
            )
        }) {
            return Err(ApiError::bad("Unknown run status"));
        }
        Ok(())
    }
}

async fn list_schedules(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Query(query): Query<ScheduleQuery>,
) -> Result<Json<Vec<Schedule>>> {
    if !(1..=100).contains(&query.limit) || !(0..=100_000).contains(&query.offset) {
        return Err(ApiError::bad(
            "limit must be 1..100; offset must be 0..100000",
        ));
    }
    if query.status.as_ref().is_some_and(|s| {
        !matches!(
            s.as_str(),
            "active" | "paused" | "completed" | "cancelled" | "error"
        )
    }) {
        return Err(ApiError::bad("Unknown schedule status"));
    }
    if query
        .q
        .as_ref()
        .is_some_and(|q| q.len() > 200 || q.chars().any(char::is_control))
    {
        return Err(ApiError::bad(
            "q must contain at most 200 bytes without control characters",
        ));
    }
    Ok(Json(
        state
            .store
            .search_schedules(
                &tenant.0,
                crate::store::ScheduleFilter {
                    status: query.status.as_deref(),
                    query: query.q.as_deref().map(str::trim),
                },
                query.limit,
                query.offset,
            )
            .await?,
    ))
}

async fn get_schedule(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<Schedule>> {
    Ok(Json(state.store.get_schedule(&tenant.0, id).await?))
}

async fn pause(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<Schedule>> {
    Ok(Json(state.store.transition(&tenant.0, id, "pause").await?))
}
async fn resume(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<Schedule>> {
    Ok(Json(state.store.transition(&tenant.0, id, "resume").await?))
}
async fn cancel(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<Schedule>> {
    Ok(Json(state.store.transition(&tenant.0, id, "cancel").await?))
}

async fn list_runs(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<Run>>> {
    query.validate()?;
    Ok(Json(
        state
            .store
            .list_runs(
                &tenant.0,
                query.schedule_id,
                query.status.as_deref(),
                query.limit,
                query.offset,
            )
            .await?,
    ))
}
async fn get_run(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<Run>> {
    Ok(Json(state.store.get_run(&tenant.0, id).await?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AttemptQuery {
    #[serde(default = "page_size")]
    limit: i64,
    before_number: Option<i32>,
}

async fn attempts(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
    Query(query): Query<AttemptQuery>,
) -> Result<Json<Vec<Attempt>>> {
    if !(1..=100).contains(&query.limit) || query.before_number.is_some_and(|n| n < 1) {
        return Err(ApiError::bad(
            "limit must be 1..100 and before_number must be positive",
        ));
    }
    Ok(Json(
        state
            .store
            .list_attempts(&tenant.0, id, query.limit, query.before_number)
            .await?,
    ))
}

async fn replay(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<Run>> {
    Ok(Json(state.store.replay(&tenant.0, id).await?))
}
async fn stats(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
) -> Result<Json<Value>> {
    Ok(Json(state.store.stats(&tenant.0).await?))
}

async fn metrics(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
) -> Result<Response> {
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.store.metrics(&tenant.0).await?,
    )
        .into_response())
}

async fn capacity(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
) -> Result<Json<crate::dispatch::TenantCapacity>> {
    Ok(Json(state.store.tenant_capacity(&tenant.0).await?))
}

#[cfg(test)]
mod security_tests {
    use super::*;
    use axum::body::{Body, Bytes};
    use http_body_util::channel::Channel;
    use std::time::Duration;
    use tower::ServiceExt;

    fn app() -> Router {
        app_with_keys(BTreeMap::from([
            ("tenant-a".into(), "tenant-a-test-secret".into()),
            ("tenant-b".into(), "tenant-b-test-secret".into()),
        ]))
    }

    fn app_with_keys(keys: BTreeMap<String, String>) -> Router {
        let store = Store {
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
                .unwrap(),
        };
        router(AppState::new(
            store,
            Evaluator::new("unused".into(), Duration::from_secs(1)),
            &keys,
        ))
    }

    fn request(path: &str, key: &str, body: Body) -> Request {
        Request::builder()
            .method(if path == "/v1/preview" { "POST" } else { "GET" })
            .uri(path)
            .header(header::AUTHORIZATION, format!("Bearer {key}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn incomplete_authenticated_body_has_a_total_deadline() {
        let app = app();
        let (mut sender, body) = Channel::<Bytes, std::io::Error>::new(1);
        sender.send_data(Bytes::from_static(b"{")).await.unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(11),
            app.clone().oneshot(request(
                "/v1/preview",
                "tenant-a-test-secret",
                Body::new(body),
            )),
        )
        .await
        .expect("an incomplete body must not hold an API request indefinitely")
        .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        // Reading must stop on timeout; the sender must no longer have a consumer.
        assert!(sender.send_data(Bytes::from_static(b"}")).await.is_err());
        assert_eq!(
            app.oneshot(request("/v1/me", "tenant-a-test-secret", Body::empty()))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test(start_paused = true)]
    async fn slow_uploads_are_bounded_per_tenant_and_cancelled_slots_recover() {
        let app = app();
        let mut held = Vec::new();
        for _ in 0..8 {
            let (mut sender, body) = Channel::<Bytes, std::io::Error>::new(1);
            sender.send_data(Bytes::from_static(b" ")).await.unwrap();
            let task = tokio::spawn(app.clone().oneshot(request(
                "/v1/preview",
                "tenant-a-test-secret",
                Body::new(body),
            )));
            // The second frame only fits once the middleware starts reading.
            sender.send_data(Bytes::from_static(b" ")).await.unwrap();
            held.push((sender, task));
        }
        let response = app
            .clone()
            .oneshot(request("/v1/me", "tenant-a-test-secret", Body::empty()))
            .await
            .unwrap();
        // The same key on a new connection/router clone shares the tenant budget.
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(
            app.clone()
                .oneshot(request("/v1/me", "tenant-b-test-secret", Body::empty()))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        for (sender, task) in held {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            drop(sender);
        }
        for _ in 0..10 {
            assert_eq!(
                app.clone()
                    .oneshot(request("/v1/me", "tenant-a-test-secret", Body::empty()))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unauthenticated_readiness_checks_cannot_fill_the_pool_queue() {
        let app = app();
        let first = tokio::spawn(
            app.clone().oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            ),
        );
        tokio::task::yield_now().await;
        let second = tokio::time::timeout(
            Duration::from_millis(100),
            app.clone().oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await;
        first.abort();
        let _ = first.await;
        let response = second
            .expect("only one readiness query may await the pool")
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        assert_eq!(
            app.oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
            StatusCode::OK
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drip_uploads_do_not_restart_the_body_deadline() {
        let app = app();
        let (mut sender, body) = Channel::<Bytes, std::io::Error>::new(1);
        let producer = tokio::spawn(async move {
            loop {
                if sender.send_data(Bytes::from_static(b" ")).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
        let start = tokio::time::Instant::now();
        let response = tokio::time::timeout(
            Duration::from_secs(11),
            app.oneshot(request(
                "/v1/preview",
                "tenant-a-test-secret",
                Body::new(body),
            )),
        )
        .await
        .expect("small chunks must not extend the upload lifetime")
        .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(start.elapsed(), Duration::from_secs(10));
        producer.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn api_budget_is_shared_across_tenants_and_recovers_after_cancellation() {
        let app = app_with_keys(
            (0..9)
                .map(|id| (format!("tenant-{id}"), format!("tenant-{id}-test-secret")))
                .collect(),
        );
        let mut held = Vec::new();
        for id in 0..8 {
            for _ in 0..8 {
                let (mut sender, body) = Channel::<Bytes, std::io::Error>::new(1);
                sender.send_data(Bytes::from_static(b" ")).await.unwrap();
                let task = tokio::spawn(app.clone().oneshot(request(
                    "/v1/preview",
                    &format!("tenant-{id}-test-secret"),
                    Body::new(body),
                )));
                sender.send_data(Bytes::from_static(b" ")).await.unwrap();
                held.push((sender, task));
            }
        }
        // Failed admission must release the tenant's own permit each time.
        for _ in 0..10 {
            let response = app
                .clone()
                .oneshot(request("/v1/me", "tenant-8-test-secret", Body::empty()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[header::RETRY_AFTER], "1");
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        }
        assert_eq!(
            app.clone()
                .oneshot(request("/v1/me", "invalid-key", Body::empty()))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/health")
                        .body(Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let (sender, task) = held.pop().unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(sender);
        assert_eq!(
            app.oneshot(request("/v1/me", "tenant-8-test-secret", Body::empty()))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        for (sender, task) in held {
            task.abort();
            let _ = task.await;
            drop(sender);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn body_limits_cover_chunked_inputs_and_auth_runs_before_reading() {
        let app = app();
        let (_sender, body) = Channel::<Bytes, std::io::Error>::new(1);
        let response = tokio::time::timeout(
            Duration::from_millis(100),
            app.clone()
                .oneshot(request("/v1/preview", "invalid-key", Body::new(body))),
        )
        .await
        .expect("unauthorized bodies must not be read")
        .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let (mut sender, body) = Channel::<Bytes, std::io::Error>::new(2);
        sender
            .send_data(Bytes::from(vec![b' '; 256 * 1024]))
            .await
            .unwrap();
        sender.send_data(Bytes::from_static(b" ")).await.unwrap();
        let response = app
            .clone()
            .oneshot(request(
                "/v1/preview",
                "tenant-a-test-secret",
                Body::new(body),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(response.headers()[header::CONNECTION], "close");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        // Exactly the limit is valid; query extraction/handlers still work.
        assert_eq!(
            app.clone()
                .oneshot(request(
                    "/v1/me",
                    "tenant-a-test-secret",
                    Body::from(vec![b' '; 256 * 1024])
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let (sender, body) = Channel::<Bytes, std::io::Error>::new(1);
        sender.abort(std::io::Error::other("private diagnostic must not leak"));
        let response = app
            .oneshot(request("/v1/me", "tenant-a-test-secret", Body::new(body)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("private diagnostic"));
    }
}
