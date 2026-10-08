use std::{collections::BTreeMap, net::SocketAddr};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the authenticated HTTP API.
    Serve,
    /// Apply embedded database migrations.
    Migrate,
    /// Internal isolated rule evaluator. JSON on stdin/stdout.
    #[command(hide = true)]
    Evaluate,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Role {
    All,
    Api,
    Scheduler,
    Worker,
}

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub bind: SocketAddr,
    pub api_keys: BTreeMap<String, String>,
    pub workers: usize,
    pub poll_ms: u64,
    pub allow_private_targets: bool,
    pub rule_timeout_ms: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
        let bind = env("SCHEDULER_BIND", "127.0.0.1:8080")
            .parse()
            .context("Invalid SCHEDULER_BIND")?;
        let api_keys: BTreeMap<String, String> =
            serde_json::from_str(&env("SCHEDULER_API_KEYS", "{}"))
                .context("SCHEDULER_API_KEYS must be a JSON object mapping tenant names to keys")?;
        validate_api_keys(&api_keys)?;
        let workers = env("SCHEDULER_WORKERS", "8").parse()?;
        let poll_ms = env("SCHEDULER_POLL_MS", "500").parse()?;
        let rule_timeout_ms = env("SCHEDULER_RULE_TIMEOUT_MS", "2000").parse()?;
        if !(1..=256).contains(&workers)
            || !(50..=60000).contains(&poll_ms)
            || !(100..=30000).contains(&rule_timeout_ms)
        {
            bail!(
                "Invalid workers (1..256), poll interval (50..60000ms), or rule timeout (100..30000ms)"
            );
        }
        let allow_private_targets = env("SCHEDULER_ALLOW_PRIVATE_TARGETS", "false").parse()?;
        Ok(Self {
            database_url,
            bind,
            api_keys,
            workers,
            poll_ms,
            allow_private_targets,
            rule_timeout_ms,
        })
    }
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn validate_api_keys(keys: &BTreeMap<String, String>) -> anyhow::Result<()> {
    for (tenant, key) in keys {
        if tenant.trim().is_empty() || tenant.len() > 128 || tenant.chars().any(char::is_control) {
            bail!("Tenant names must be 1..128 bytes without control characters");
        }
        if !(16..=4096).contains(&key.len()) || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
            bail!("API keys must be 16..4096 printable ASCII bytes without whitespace");
        }
    }
    let unique: std::collections::BTreeSet<_> = keys.values().collect();
    if unique.len() != keys.len() {
        bail!("Each tenant must use a distinct API key");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_keys_must_be_usable_in_authorization_headers() {
        for key in [
            "a".repeat(15),
            "a".repeat(4097),
            " space-prefixed-secret".into(),
            "secret with spaces".into(),
            "secret-key-contains\nnewline".into(),
            "secret-key-contains\0nul".into(),
            "非ASCII密钥-1234567890".into(),
        ] {
            let keys = BTreeMap::from([("tenant-a".into(), key.clone())]);
            let error = validate_api_keys(&keys).unwrap_err().to_string();
            assert!(!error.contains(&key));
        }
        let valid = BTreeMap::from([
            ("tenant-a".into(), "secret-A_1234567890".into()),
            ("tenant-b".into(), "secret-a_1234567890".into()),
        ]);
        assert!(validate_api_keys(&valid).is_ok());
        assert!(validate_api_keys(&BTreeMap::new()).is_ok());
        let duplicate = BTreeMap::from([
            ("a".into(), "same-secret-1234567890".into()),
            ("b".into(), "same-secret-1234567890".into()),
        ]);
        assert!(validate_api_keys(&duplicate).is_err());
    }
}
