use crate::webhook;
use crate::{
    domain::{MisfirePolicy, Schedule, Trigger},
    evaluator::Evaluator,
    store::Store,
};
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
const CATCH_UP_BATCH: usize = 100;

pub async fn materialize_one(
    store: &Store,
    evaluator: &Evaluator,
    schedule: &Schedule,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let Some(first) = schedule.next_fire_at else {
        return Ok(false);
    };
    if first > now {
        return Ok(false);
    }
    if matches!(schedule.spec.trigger, Trigger::Once { .. }) {
        return Ok(store.materialize(schedule, &[first], None).await?);
    }
    let spec = &schedule.spec;
    let (dates, next) = match spec.misfire {
        MisfirePolicy::CatchUp => {
            let after = first
                .checked_sub_signed(chrono::Duration::nanoseconds(1))
                .ok_or_else(|| anyhow::anyhow!("Cursor underflow"))?;
            let expansion = evaluator
                .next(&spec.trigger, after, CATCH_UP_BATCH + 1)
                .await?;
            let count = expansion
                .dates
                .iter()
                .take_while(|date| **date <= now)
                .count()
                .min(CATCH_UP_BATCH);
            let next = expansion.dates.get(count).copied();
            (
                expansion.dates.into_iter().take(count).collect::<Vec<_>>(),
                next,
            )
        }
        MisfirePolicy::FireOnce | MisfirePolicy::Skip => {
            let late = now.signed_duration_since(first)
                > chrono::Duration::seconds(i64::from(spec.misfire_grace_seconds));
            let dates = if spec.misfire == MisfirePolicy::Skip && late {
                vec![]
            } else {
                vec![first]
            };
            let next = evaluator
                .next(&spec.trigger, now, 1)
                .await?
                .dates
                .first()
                .copied();
            (dates, next)
        }
    };
    Ok(store.materialize(schedule, &dates, next).await?)
}

pub async fn scheduler_tick(store: &Store, evaluator: &Evaluator) -> anyhow::Result<()> {
    let schedules = store.due_schedules(32).await?;
    for batch in schedules.chunks(4) {
        let now = store.now().await?;
        let mut tasks = JoinSet::new();
        for schedule in batch {
            let store = store.clone();
            let evaluator = evaluator.clone();
            let schedule = schedule.clone();
            tasks.spawn(async move {
                match materialize_one(&store, &evaluator, &schedule, now).await {
                    Ok(true) => tracing::debug!(schedule_id = %schedule.id, "Materialized schedule"),
                    Ok(false) => (),
                    Err(error) => {
                        // Database and evaluator infrastructure failures remain retryable.
                        // Rule failures retain the cursor for repair.
                        if error.downcast_ref::<crate::store::StoreError>().is_none()
                            && error.downcast_ref::<crate::evaluator::EvaluatorBusy>().is_none()
                            && error.downcast_ref::<crate::evaluator::EvaluatorUnavailable>().is_none() {
                            store.rule_error(&schedule, &format!("{error:#}")).await?;
                        }
                        tracing::warn!(schedule_id = %schedule.id, error = %format!("{error:#}"), "Schedule expansion failed");
                    }
                }
                Ok::<(), anyhow::Error>(())
            });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
    }
    Ok(())
}

pub async fn scheduler_loop(
    store: Store,
    evaluator: Evaluator,
    poll: Duration,
    shutdown: CancellationToken,
) {
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        if let Err(error) = scheduler_tick(&store, &evaluator).await {
            tracing::error!(%error, "Scheduler tick failed");
        }
        tokio::select! { _ = shutdown.cancelled() => break, _ = tokio::time::sleep(poll) => () }
    }
}

pub async fn recovery_loop(store: Store, shutdown: CancellationToken) {
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        match store.recover_expired().await {
            Ok(count) if count > 0 => tracing::warn!(count, "Recovered expired worker leases"),
            Ok(_) => (),
            Err(error) => tracing::error!(%error, "Lease recovery failed"),
        }
        match store.expire_pending().await {
            Ok(count) if count > 0 => tracing::info!(count, "Expired pending runs"),
            Ok(_) => (),
            Err(error) => tracing::error!(%error, "Run expiration failed"),
        }
        tokio::select! { _ = shutdown.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => () }
    }
}

pub async fn worker_loop(
    store: Store,
    allow_private: bool,
    poll: Duration,
    shutdown: CancellationToken,
) {
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        match store.claim().await {
            Ok(Some(run)) => {
                let result = webhook::deliver(&run, allow_private).await;
                let success = result.success;
                match store.finish(&run, result).await {
                    Ok(true) => {
                        tracing::info!(run_id = %run.id, attempt = run.attempt_count, success, "Delivery attempt completed")
                    }
                    Ok(false) => {
                        tracing::warn!(run_id = %run.id, "Ignored completion after lease loss")
                    }
                    Err(error) => {
                        tracing::error!(run_id = %run.id, %error, "Could not persist result; lease recovery will retry")
                    }
                }
                continue;
            }
            Ok(None) => (),
            Err(error) => tracing::error!(%error, "Worker claim failed"),
        }
        tokio::select! { _ = shutdown.cancelled() => break, _ = tokio::time::sleep(poll) => () }
    }
}
