use super::*;
use crate::run_control::RunTiming;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RescheduleRequest {
    expected_available_at: DateTime<Utc>,
    timing: RunTiming,
}

pub(super) async fn cancel(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
) -> Result<Json<Run>> {
    Ok(Json(state.store.cancel_run(&tenant.0, id).await?))
}

pub(super) async fn reschedule(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    Path(id): Path<Uuid>,
    Json(request): Json<RescheduleRequest>,
) -> Result<Json<Run>> {
    Ok(Json(
        state
            .store
            .reschedule_run(&tenant.0, id, request.expected_available_at, request.timing)
            .await?,
    ))
}
