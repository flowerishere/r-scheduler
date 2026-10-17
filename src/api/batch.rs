use tokio::task::JoinSet;

use super::*;
use crate::batch::{MAX_BATCH_SCHEDULES, PreparedSchedule};

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BatchRequest {
    schedules: Vec<ScheduleSpec>,
}

pub(super) async fn create(
    State(state): State<AppState>,
    Extension(tenant): Extension<Tenant>,
    headers: HeaderMap,
    Json(request): Json<BatchRequest>,
) -> Result<(StatusCode, Json<Vec<Schedule>>)> {
    let key = idempotency_key(&headers)?
        .ok_or_else(|| ApiError::bad("Idempotency-Key is required for batch creation"))?;
    if !(1..=MAX_BATCH_SCHEDULES).contains(&request.schedules.len()) {
        return Err(ApiError::bad(format!(
            "schedules must contain 1..{MAX_BATCH_SCHEDULES} entries"
        )));
    }
    let hash = request_hash(&request)?;
    if let Some(existing) = state.store.find_batch(&tenant.0, key, &hash).await? {
        return Ok((StatusCode::OK, Json(existing)));
    }
    for (index, spec) in request.schedules.iter().enumerate() {
        validate_spec(spec).map_err(|error| entry_error(index, ApiError::bad(error)))?;
    }
    // Every delay in the batch has the same database clock anchor. Evaluate
    // at most four rules concurrently, before starting any write transaction.
    let now = state.store.now().await?;
    let mut prepared = Vec::with_capacity(request.schedules.len());
    for (batch, chunk) in request.schedules.chunks(4).enumerate() {
        let mut tasks = JoinSet::new();
        for (index, mut spec) in chunk.iter().cloned().enumerate() {
            let state = state.clone();
            tasks.spawn(async move {
                let hash = request_hash(&spec)?;
                let next = prepare_at(&state, &mut spec, now)
                    .await
                    .map_err(|error| entry_error(batch * 4 + index, error))?;
                Ok::<_, ApiError>((
                    batch * 4 + index,
                    PreparedSchedule {
                        spec,
                        next,
                        request_hash: hash,
                    },
                ))
            });
        }
        while let Some(result) = tasks.join_next().await {
            prepared.push(result.map_err(ApiError::internal)??);
        }
    }
    prepared.sort_unstable_by_key(|(index, _)| *index);
    let prepared: Vec<_> = prepared.into_iter().map(|(_, entry)| entry).collect();
    let (created, schedules) = state
        .store
        .create_batch(&tenant.0, key, &hash, &prepared)
        .await?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(schedules),
    ))
}

fn entry_error(index: usize, error: ApiError) -> ApiError {
    ApiError(error.0, format!("Schedule {}: {}", index + 1, error.1))
}
