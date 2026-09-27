use std::io::Read;

use clap::Parser;
use scheduler_service::{
    config::{Cli, Command},
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
    }
    Ok(())
}
