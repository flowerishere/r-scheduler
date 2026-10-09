use std::{io::Read, time::Duration};

use anyhow::Context;
use clap::Parser;
use scheduler_service::{
    api::{self, AppState},
    config::{Cli, Command, Config, Role},
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
        let request = serde_json::from_reader(std::io::stdin().lock().take(32 * 1024));
        let result = request
            .map_err(anyhow::Error::from)
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
    let early_exit = tokio::select! {
        result = tasks.join_next() => Some(result),
        result = shutdown_signal() => { result?; None },
    };
    shutdown.cancel();
    tracing::info!("Draining active requests and deliveries");
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    store.pool.close().await;
    if let Some(result) = early_exit {
        if let Some(result) = result {
            result??;
        }
        anyhow::bail!("A service task exited unexpectedly");
    }
    Ok(())
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
