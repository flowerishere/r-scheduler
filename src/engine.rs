use crate::{
    domain::{MisfirePolicy, Schedule, Trigger},
    evaluator::Evaluator,
    store::Store,
};
use chrono::{DateTime, Utc};
use tokio::task::JoinSet;
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
                        // Database failures and evaluator overload remain retryable.
                        // Rule failures retain the cursor for repair.
                        if error.downcast_ref::<crate::store::StoreError>().is_none()
                            && error.downcast_ref::<crate::evaluator::EvaluatorBusy>().is_none() {
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
