use std::time::Duration;

use scheduler_service::{domain::Trigger, evaluator::Evaluator};

fn evaluator() -> Evaluator {
    Evaluator::new(
        env!("CARGO_BIN_EXE_scheduler-service").into(),
        Duration::from_secs(5),
    )
}

#[tokio::test]
async fn evaluator_uses_real_subprocess() {
    let result = evaluator()
        .next(
            &Trigger::Rrule {
                value: "DTSTART:20260921T090000Z\nRRULE:FREQ=DAILY;COUNT=2".into(),
            },
            "2026-09-20T00:00:00Z".parse().unwrap(),
            10,
        )
        .await
        .unwrap();
    assert_eq!(result.dates.len(), 2);
    assert!(result.exhausted);
}

#[tokio::test]
async fn expensive_rules_are_terminated_by_process_timeout() {
    let evaluator = Evaluator::new(
        env!("CARGO_BIN_EXE_scheduler-service").into(),
        Duration::from_millis(5),
    );
    let started = std::time::Instant::now();
    let error = evaluator
        .next(
            &Trigger::Rrule {
                value: "DTSTART:19000101T000000Z\nRRULE:FREQ=SECONDLY".into(),
            },
            "2026-09-21T00:00:00Z".parse().unwrap(),
            1,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2));
}
