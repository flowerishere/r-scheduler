use clap::Parser;
use scheduler_service::{
    api::{self, AppState},
    config::{Cli, Command, Config},
    evaluator::Evaluator,
    store::Store,
    trigger,
};
use std::{io::Read, time::Duration};
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
    let config = Config::from_env()?;
    if matches!(cli.command, Command::Serve) && config.api_keys.is_empty() {
        anyhow::bail!("Set SCHEDULER_API_KEYS before starting the API");
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let store = Store::connect(&config.database_url).await?;
            match cli.command {
                Command::Migrate => store.pool.close().await,
                Command::Serve => {
                    let evaluator = Evaluator::new(
                        std::env::current_exe()?,
                        Duration::from_millis(config.rule_timeout_ms),
                    );
                    let listener = tokio::net::TcpListener::bind(config.bind).await?;
                    axum::serve(
                        listener,
                        api::router(AppState::new(store, evaluator, &config.api_keys)),
                    )
                    .await?;
                }
                Command::Evaluate => unreachable!(),
            }
            Ok(())
        })
}
