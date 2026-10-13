use std::{io::Read, time::Duration};

use anyhow::Context;
use clap::Parser;
use scheduler_service::{
    api::{self, AppState},
    config::{Cli, Command, Config, Role, TenantCommand},
    dispatch::TenantLimits,
    engine,
    evaluator::Evaluator,
    store::Store,
    trigger,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if matches!(cli.command, Command::Evaluate) {
        let result = read_evaluation_request(std::io::stdin().lock())
            .and_then(trigger::evaluate)
            .map_err(|error| format!("{error:#}"));
        serde_json::to_writer(std::io::stdout().lock(), &result)?;
        return Ok(());
    }
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("scheduler_service=info,tower_http=info")),
        )
        .init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(cli))
}

fn read_evaluation_request(input: impl Read) -> anyhow::Result<trigger::EvaluationRequest> {
    let mut bytes = Vec::new();
    input
        .take((trigger::MAX_EVALUATION_INPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > trigger::MAX_EVALUATION_INPUT_BYTES {
        anyhow::bail!("Rule evaluation request exceeds 128 KiB");
    }
    serde_json::from_slice(&bytes).context("Invalid rule evaluation request")
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let role = match cli.command {
        Command::Serve { role } => role,
        Command::Migrate => {
            Store::connect(&config.database_url).await?;
            tracing::info!("Migrations applied");
            return Ok(());
        }
        Command::Evaluate => unreachable!(),
        Command::Tenant { action } => {
            let store = Store::connect(&config.database_url).await?;
            let capacity = match action {
                TenantCommand::Show { tenant } => store.tenant_capacity(&tenant).await?,
                TenantCommand::Set {
                    tenant,
                    max_concurrent,
                    rate_per_second,
                    burst,
                } => {
                    store
                        .set_tenant_limits(
                            &tenant,
                            &TenantLimits {
                                max_concurrent,
                                rate_per_second,
                                burst,
                            },
                        )
                        .await?
                }
            };
            println!("{}", serde_json::to_string(&capacity)?);
            store.pool.close().await;
            return Ok(());
        }
        Command::Cleanup {
            older_than_days,
            batch_size,
            apply,
        } => {
            let store = Store::connect(&config.database_url).await?;
            let cutoff = store.now().await? - chrono::Duration::days(i64::from(older_than_days));
            let report = store
                .cleanup_history(cutoff, i64::from(batch_size), apply)
                .await?;
            println!("{}", serde_json::to_string(&report)?);
            store.pool.close().await;
            return Ok(());
        }
    };
    let api_enabled = matches!(role, Role::All | Role::Api);
    if api_enabled && config.api_keys.is_empty() {
        anyhow::bail!("Set SCHEDULER_API_KEYS before starting the API");
    }
    let store = Store::connect(&config.database_url)
        .await
        .context("Connect and migrate PostgreSQL")?;
    let evaluator = Evaluator::new(
        std::env::current_exe()?,
        Duration::from_millis(config.rule_timeout_ms),
    );
    let shutdown = CancellationToken::new();
    let poll = Duration::from_millis(config.poll_ms);
    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    if api_enabled {
        let listener = tokio::net::TcpListener::bind(config.bind).await?;
        let app = api::router(AppState::new(
            store.clone(),
            evaluator.clone(),
            &config.api_keys,
        ));
        let stop = shutdown.clone();
        tracing::info!(address = %listener.local_addr()?, "HTTP API listening");
        tasks.spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await?;
            Ok(())
        });
    }
    if matches!(role, Role::All | Role::Scheduler) {
        let store = store.clone();
        let stop = shutdown.clone();
        tasks.spawn(async move {
            engine::scheduler_loop(store, evaluator, poll, stop).await;
            Ok(())
        });
    }
    if matches!(role, Role::All | Role::Worker) {
        let recovery_store = store.clone();
        let stop = shutdown.clone();
        tasks.spawn(async move {
            engine::recovery_loop(recovery_store, stop).await;
            Ok(())
        });
        for _ in 0..config.workers {
            let store = store.clone();
            let stop = shutdown.clone();
            let allow_private = config.allow_private_targets;
            tasks.spawn(async move {
                engine::worker_loop(store, allow_private, poll, stop).await;
                Ok(())
            });
        }
    }
    tracing::info!(?role, workers = config.workers, "Scheduler service started");
    let stop_result = tokio::select! {
        result = tasks.join_next() => match result {
            Some(Ok(Err(error))) => Err(error),
            Some(Err(error)) => Err(error.into()),
            _ => Err(anyhow::anyhow!("A service task exited unexpectedly")),
        },
        result = shutdown_signal() => result,
    };
    shutdown.cancel();
    tracing::info!("Draining active requests and deliveries");
    let drain_result = drain_tasks(&mut tasks).await;
    store.pool.close().await;
    stop_result?;
    drain_result
}

async fn drain_tasks(tasks: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        let result = result
            .map_err(anyhow::Error::from)
            .and_then(|result| result);
        if let Err(error) = result
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = terminate.recv() => () }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    async fn check_drain(panic: bool) {
        let finished = Arc::new(AtomicBool::new(false));
        let delivered = finished.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            delivered.store(true, Ordering::SeqCst);
            Ok(())
        });
        tasks.spawn(async move {
            assert!(!panic, "simulated service task panic");
            anyhow::bail!("simulated service task error")
        });
        assert!(drain_tasks(&mut tasks).await.is_err());
        assert!(
            finished.load(Ordering::SeqCst),
            "in-flight delivery was aborted"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drains_deliveries_after_a_task_error() {
        check_drain(false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drains_deliveries_after_a_task_panic() {
        check_drain(true).await;
    }
}
