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

#[sqlx::test(migrations = false)]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn migrate_command_and_store_apply_initial_schema_idempotently(pool: sqlx::PgPool) {
    use scheduler_service::store::Store;
    use sqlx::ConnectOptions;

    let url = pool.connect_options().to_url_lossy().to_string();
    for _ in 0..2 {
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_scheduler-service"))
            .arg("migrate")
            .env_clear()
            .env("DATABASE_URL", &url)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(versions, [1]);
    let store = Store::connect(&url).await.unwrap();
    let statement_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let lock_timeout: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(statement_timeout, "15s");
    assert_eq!(lock_timeout, "5s");
    let now = store.now().await.unwrap();
    assert!((chrono::Utc::now() - now).num_seconds().abs() < 5);
    store.pool.close().await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn initial_schema_enforces_tenants_occurrences_and_leases(pool: sqlx::PgPool) {
    use scheduler_service::domain::{Attempt, Run, Schedule, ScheduleSpec};
    use serde_json::json;
    use sqlx::types::Json;
    use uuid::Uuid;

    fn code(error: sqlx::Error, expected: &str) {
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some(expected)
        );
    }
    let at: chrono::DateTime<chrono::Utc> = "2030-01-01T00:00:00Z".parse().unwrap();
    let spec: ScheduleSpec = serde_json::from_value(json!({"name":"schema fixture", "trigger":{"type":"once","at":at}, "target":{"url":"https://example.invalid/hook"}})).unwrap();
    let id = Uuid::new_v4();
    let insert = "INSERT INTO schedules (id,tenant_id,spec,status,next_fire_at,idempotency_key,request_hash) VALUES ($1,$2,$3,'active',$4,'shared-key','fixture') RETURNING *";
    let schedule: Schedule = sqlx::query_as(insert)
        .bind(id)
        .bind("a")
        .bind(Json(&spec))
        .bind(at)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(schedule.revision, 1);
    code(
        sqlx::query(insert)
            .bind(Uuid::new_v4())
            .bind("a")
            .bind(Json(&spec))
            .bind(at)
            .execute(&pool)
            .await
            .unwrap_err(),
        "23505",
    );
    sqlx::query(insert)
        .bind(Uuid::new_v4())
        .bind("b")
        .bind(Json(&spec))
        .bind(at)
        .execute(&pool)
        .await
        .unwrap();
    let run_id = Uuid::new_v4();
    let insert_run = "INSERT INTO runs (id,schedule_id,tenant_id,revision,scheduled_at,available_at,status,spec) VALUES ($1,$2,$3,1,$4,$4,'pending',$5) RETURNING *";
    code(
        sqlx::query(insert_run)
            .bind(Uuid::new_v4())
            .bind(id)
            .bind("b")
            .bind(at)
            .bind(Json(&spec))
            .execute(&pool)
            .await
            .unwrap_err(),
        "23503",
    );
    let run: Run = sqlx::query_as(insert_run)
        .bind(run_id)
        .bind(id)
        .bind("a")
        .bind(at)
        .bind(Json(&spec))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(run.attempt_count, 0);
    assert!(run.lease_token.is_none());
    code(
        sqlx::query(insert_run)
            .bind(Uuid::new_v4())
            .bind(id)
            .bind("a")
            .bind(at)
            .bind(Json(&spec))
            .execute(&pool)
            .await
            .unwrap_err(),
        "23505",
    );
    code(
        sqlx::query("UPDATE runs SET status='running' WHERE id=$1")
            .bind(run_id)
            .execute(&pool)
            .await
            .unwrap_err(),
        "23514",
    );
    code(
        sqlx::query("UPDATE runs SET status='unknown' WHERE id=$1")
            .bind(run_id)
            .execute(&pool)
            .await
            .unwrap_err(),
        "23514",
    );
    let token = Uuid::new_v4();
    sqlx::query("UPDATE runs SET status='running',lease_token=$2,lease_until=clock_timestamp()+INTERVAL '30 seconds' WHERE id=$1").bind(run_id).bind(token).execute(&pool).await.unwrap();
    let attempt_sql = "INSERT INTO attempts (id,run_id,number,status,lease_token) VALUES ($1,$2,$3,'running',$4) RETURNING *";
    let attempt: Attempt = sqlx::query_as(attempt_sql)
        .bind(Uuid::new_v4())
        .bind(run_id)
        .bind(1)
        .bind(token)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(attempt.number, 1);
    code(
        sqlx::query(attempt_sql)
            .bind(Uuid::new_v4())
            .bind(run_id)
            .bind(1)
            .bind(Uuid::new_v4())
            .execute(&pool)
            .await
            .unwrap_err(),
        "23505",
    );
    code(
        sqlx::query(attempt_sql)
            .bind(Uuid::new_v4())
            .bind(run_id)
            .bind(2)
            .bind(token)
            .execute(&pool)
            .await
            .unwrap_err(),
        "23505",
    );
}
