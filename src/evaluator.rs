use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use tokio::{io::AsyncWriteExt, process::Command, sync::Semaphore};

use crate::{
    domain::Trigger,
    trigger::{Evaluation, EvaluationRequest, evaluate},
};

#[derive(Debug, thiserror::Error)]
#[error("Rule evaluator is busy; retry later")]
pub struct EvaluatorBusy;

#[derive(Debug, thiserror::Error)]
#[error("Rule evaluator is unavailable; retry later")]
pub struct EvaluatorUnavailable(#[source] anyhow::Error);

#[derive(Clone)]
pub struct Evaluator {
    executable: PathBuf,
    timeout: Duration,
    permits: Arc<Semaphore>,
}

impl Evaluator {
    pub fn new(executable: PathBuf, timeout: Duration) -> Self {
        Self {
            executable,
            timeout,
            permits: Arc::new(Semaphore::new(4)),
        }
    }

    pub async fn next(
        &self,
        trigger: &Trigger,
        after: DateTime<Utc>,
        count: usize,
    ) -> anyhow::Result<Evaluation> {
        let request = EvaluationRequest {
            trigger: trigger.clone(),
            after,
            count,
        };
        if matches!(trigger, Trigger::Once { .. }) {
            return evaluate(request);
        }
        let _permit = tokio::time::timeout(self.timeout, self.permits.acquire())
            .await
            .map_err(|_| EvaluatorBusy)??;
        let input = serde_json::to_vec(&request)?;
        let operation = async {
            let mut child = Command::new(&self.executable)
                .arg("evaluate")
                .kill_on_drop(true)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                // Do not pass database credentials or API keys into the rule subprocess.
                .env_clear()
                .spawn()
                .context("Start rule evaluator")?;
            let mut stdin = child.stdin.take().context("Open evaluator stdin")?;
            stdin.write_all(&input).await?;
            drop(stdin);
            let output = child.wait_with_output().await?;
            if !output.status.success() {
                bail!("Rule evaluator exited unsuccessfully");
            }
            let result: Result<Evaluation, String> = serde_json::from_slice(&output.stdout)
                .context("Invalid rule evaluator response")?;
            // Keep a rule rejection separate from process/IPC failures.
            Ok::<_, anyhow::Error>(result)
        };
        let result = tokio::time::timeout(self.timeout, operation)
            .await
            .context("Rule evaluation timed out; shorten the rule history or simplify the rule")?
            .map_err(EvaluatorUnavailable)?;
        result.map_err(anyhow::Error::msg)
    }
}
