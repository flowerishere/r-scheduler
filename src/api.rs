use crate::{
    domain::{Schedule, ScheduleSpec, Trigger},
    evaluator::Evaluator,
    store::{Store, StoreError},
    trigger::{Evaluation, resolve_delay},
    webhook::validate_spec,
};
use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use subtle::ConstantTimeEq;
use tower_http::trace::TraceLayer;
use uuid::Uuid;
const MAX_BODY_BYTES: usize = 256 * 1024;
struct ApiTenant {
    id: String,
    key_hash: [u8; 32],
}
#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub evaluator: Evaluator,
    keys: Arc<Vec<ApiTenant>>,
}
impl AppState {
    pub fn new(store: Store, evaluator: Evaluator, keys: &BTreeMap<String, String>) -> Self {
        Self {
            store,
            evaluator,
            keys: Arc::new(
                keys.iter()
                    .map(|(id, key)| ApiTenant {
                        id: id.clone(),
                        key_hash: Sha256::digest(key.as_bytes()).into(),
                    })
                    .collect(),
            ),
        }
    }
}
#[derive(Clone)]
struct Tenant(String);
#[derive(Debug)]
pub struct ApiError(StatusCode, String);
impl ApiError {
    fn evaluation(error: anyhow::Error) -> Self {
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
        .route("/preview", post(preview))
        .route("/schedules", post(create).get(list_schedules))
        .route("/schedules/{id}", get(get_schedule).put(replace))
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
    request.extensions_mut().insert(Tenant(tenant.id.clone()));
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
async fn ready(State(state): State<AppState>) -> Response {
    match state.store.now().await {
        Ok(_) => Json(json!({"status": "ready"})).into_response(),
        Err(_) => ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "Database unavailable".into(),
        )
        .into_response(),
    }
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
struct ScheduleQuery {
    #[serde(default = "page_size")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}
fn page_size() -> i64 {
    50
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
    Ok(Json(
        state
            .store
            .list_schedules(&tenant.0, query.limit, query.offset)
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
