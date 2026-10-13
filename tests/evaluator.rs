//! Input limits must cover JSON escaping without changing rule semantics.
use std::{process::Stdio, time::Duration};

use chrono::{DateTime, Utc};
use scheduler_service::{
    domain::Trigger,
    evaluator::{Evaluator, EvaluatorBusy, EvaluatorUnavailable},
    trigger::{EvaluationRequest, evaluate},
};
use tokio::{io::AsyncWriteExt, process::Command};

fn after() -> DateTime<Utc> {
    "2029-12-31T00:00:00Z".parse().unwrap()
}

fn padded_rule() -> Trigger {
    let mut value = "DTSTART:20300101T090000Z\nRRULE:FREQ=DAILY;COUNT=2".to_owned();
    value.push_str(&"\t".repeat(16 * 1024 - value.len()));
    Trigger::Rrule { value }
}

#[tokio::test]
async fn escaped_rule_at_raw_size_limit_survives_real_ipc() {
    let trigger = padded_rule();
    let request = EvaluationRequest {
        trigger: trigger.clone(),
        after: after(),
        count: 5,
    };
    assert!(serde_json::to_vec(&request).unwrap().len() > 32 * 1024);
    let expected = evaluate(request).unwrap();
    let evaluator = Evaluator::new(
        env!("CARGO_BIN_EXE_scheduler-service").into(),
        Duration::from_secs(5),
    );
    let result = evaluator.next(&trigger, after(), 5).await.unwrap();
    assert_eq!(result.dates, expected.dates);
    assert_eq!(result.exhausted, expected.exhausted);
}

#[tokio::test]
async fn invalid_input_is_rejected_before_starting_a_subprocess() {
    let evaluator = Evaluator::new(
        "/missing-scheduler-input-test-executable".into(),
        Duration::from_secs(1),
    );
    let valid = Trigger::Cron {
        expression: "* * * * * *".into(),
        timezone: "UTC".into(),
    };
    for (trigger, count) in [
        (valid.clone(), 0),
        (valid, 257),
        (
            Trigger::Rrule {
                value: "X".repeat(16 * 1024 + 1),
            },
            1,
        ),
        (
            Trigger::Cron {
                expression: "*".repeat(513),
                timezone: "UTC".into(),
            },
            1,
        ),
        (
            Trigger::Cron {
                expression: "* * * * * *".into(),
                timezone: "X".repeat(100 * 1024),
            },
            1,
        ),
    ] {
        let error = evaluator.next(&trigger, after(), count).await.unwrap_err();
        assert!(
            error.downcast_ref::<EvaluatorUnavailable>().is_none(),
            "{error}"
        );
        assert!(error.downcast_ref::<EvaluatorBusy>().is_none(), "{error}");
    }
}

#[tokio::test]
async fn cli_rejects_oversized_input_instead_of_accepting_a_truncated_prefix() {
    let request = EvaluationRequest {
        trigger: Trigger::Once {
            at: "2030-01-01T00:00:00Z".parse().unwrap(),
        },
        after: after(),
        count: 1,
    };
    let mut input = serde_json::to_vec(&request).unwrap();
    input.extend(vec![b' '; 128 * 1024]);
    input.extend_from_slice(b"invalid trailing input");
    let mut child = Command::new(env!("CARGO_BIN_EXE_scheduler-service"))
        .arg("evaluate")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    // A bounded reader may close stdin before this oversized write completes.
    let _ = stdin.write_all(&input).await;
    drop(stdin);
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(output.status.success());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(result["Err"].is_string(), "{result}");
}
