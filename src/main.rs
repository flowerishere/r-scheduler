use std::io::Read;

use clap::Parser;
use scheduler_service::{
    config::{Cli, Command, Config},
    store::Store,
    trigger,
};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Evaluate => {
            let request = serde_json::from_reader(std::io::stdin().lock().take(32 * 1024));
            let result = request
                .map_err(anyhow::Error::from)
                .and_then(trigger::evaluate)
                .map_err(|error| format!("{error:#}"));
            serde_json::to_writer(std::io::stdout().lock(), &result)?;
        }
        Command::Migrate => {
            let config = Config::from_env()?;
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let store = Store::connect(&config.database_url).await?;
                    store.pool.close().await;
                    Ok::<_, anyhow::Error>(())
                })?;
        }
    }
    Ok(())
}
