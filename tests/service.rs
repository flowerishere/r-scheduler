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

fn schedule_spec(at: chrono::DateTime<chrono::Utc>) -> scheduler_service::domain::ScheduleSpec {
    serde_json::from_value(serde_json::json!({
        "name": "test job",
        "trigger": {"type": "once", "at": at},
        "target": {"url": "https://example.invalid/hook"},
        "payload": {"order_id": "order-42"}
    }))
    .unwrap()
}

async fn seed_run(
    pool: &sqlx::PgPool,
    schedule: &scheduler_service::domain::Schedule,
    seconds: i64,
    running: bool,
) -> scheduler_service::domain::Run {
    let at = schedule.next_fire_at.unwrap() + chrono::Duration::seconds(seconds);
    sqlx::query_as("INSERT INTO runs (id,schedule_id,tenant_id,revision,scheduled_at,available_at,status,spec,lease_token,lease_until) VALUES ($1,$2,$3,$4,$5,$5,$6,$7,$8,$9) RETURNING *")
        .bind(uuid::Uuid::new_v4()).bind(schedule.id).bind(&schedule.tenant_id)
        .bind(schedule.revision).bind(at).bind(if running { "running" } else { "pending" })
        .bind(&schedule.spec).bind(running.then(uuid::Uuid::new_v4))
        .bind(running.then_some(at + chrono::Duration::seconds(30)))
        .fetch_one(pool).await.unwrap()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn concurrent_creates_share_idempotency_only_within_the_tenant(pool: sqlx::PgPool) {
    use scheduler_service::store::{Store, StoreError};
    let store = Store { pool };
    let at = store.now().await.unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(12));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let store = store.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            store
                .create("a", schedule_spec(at), at, Some("shared-key"), "original")
                .await
                .unwrap()
        });
    }
    let mut ids = std::collections::BTreeSet::new();
    while let Some(result) = tasks.join_next().await {
        ids.insert(result.unwrap().id);
    }
    assert_eq!(ids.len(), 1);
    let id = *ids.first().unwrap();
    assert_eq!(
        store
            .find_idempotent("a", "shared-key", "original")
            .await
            .unwrap()
            .unwrap()
            .id,
        id
    );
    assert!(
        store
            .find_idempotent("a", "missing", "original")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .find_idempotent("b", "shared-key", "original")
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store.find_idempotent("a", "shared-key", "changed").await,
        Err(StoreError::Conflict(_))
    ));
    let mut changed = schedule_spec(at);
    changed.name = "different request".into();
    assert!(matches!(
        store
            .create("a", changed, at, Some("shared-key"), "changed")
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(store.list_schedules("a", 100, 0).await.unwrap().len(), 1);
    let other = store
        .create("b", schedule_spec(at), at, Some("shared-key"), "changed")
        .await
        .unwrap();
    assert_ne!(other.id, id);
    assert!(matches!(
        store.get_schedule("b", id).await,
        Err(StoreError::NotFound)
    ));
    let first = store
        .create("a", schedule_spec(at), at, None, "original")
        .await
        .unwrap();
    let second = store
        .create("a", schedule_spec(at), at, None, "original")
        .await
        .unwrap();
    assert_ne!(first.id, second.id);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn schedule_pages_are_bounded_stable_and_tenant_scoped(pool: sqlx::PgPool) {
    use scheduler_service::store::Store;
    let store = Store { pool };
    let at = store.now().await.unwrap();
    let mut expected = Vec::new();
    for _ in 0..101 {
        expected.push(
            store
                .create("a", schedule_spec(at), at, None, "test")
                .await
                .unwrap()
                .id,
        );
    }
    store
        .create("b", schedule_spec(at), at, None, "test")
        .await
        .unwrap();
    sqlx::query("UPDATE schedules SET created_at = $1")
        .bind(at)
        .execute(&store.pool)
        .await
        .unwrap();
    expected.sort_unstable_by(|a, b| b.cmp(a));
    let first = store.list_schedules("a", 50, 0).await.unwrap();
    let second = store.list_schedules("a", 50, 50).await.unwrap();
    let last = store.list_schedules("a", 50, 100).await.unwrap();
    let actual: Vec<_> = first
        .iter()
        .chain(&second)
        .chain(&last)
        .map(|s| s.id)
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(
        store.list_schedules("a", i64::MAX, 0).await.unwrap().len(),
        100
    );
    let minimum = store.list_schedules("a", 0, -10).await.unwrap();
    assert_eq!(minimum.len(), 1);
    assert_eq!(minimum[0].id, expected[0]);
    assert!(
        store
            .list_schedules("a", 10, i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.list_schedules("b", 100, 0).await.unwrap().len(), 1);
    assert!(
        store
            .list_schedules("unknown", 100, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn replacement_is_tenant_scoped_and_invalidates_only_pending_work(pool: sqlx::PgPool) {
    use scheduler_service::{
        domain::Run,
        store::{Store, StoreError},
    };
    let store = Store { pool };
    let at = store.now().await.unwrap();
    let schedule = store
        .create("a", schedule_spec(at), at, Some("creation-key"), "original")
        .await
        .unwrap();
    let pending = seed_run(&store.pool, &schedule, 0, false).await;
    let running = seed_run(&store.pool, &schedule, 1, true).await;
    let other = store
        .create("a", schedule_spec(at), at, None, "other")
        .await
        .unwrap();
    let other_pending = seed_run(&store.pool, &other, 0, false).await;
    let next = at + chrono::Duration::hours(1);
    let mut spec = schedule_spec(next);
    spec.name = "replacement".into();
    spec.payload = serde_json::json!({"replacement": true});
    spec.target.url = "https://example.invalid/new-hook".into();
    spec.retry.max_attempts = 2;
    assert!(matches!(
        store.replace("b", schedule.id, 1, spec.clone(), next).await,
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        store.replace("a", schedule.id, 0, spec.clone(), next).await,
        Err(StoreError::Conflict(_))
    ));
    let status: String = sqlx::query_scalar("SELECT status FROM runs WHERE id=$1")
        .bind(pending.id)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(status, "pending");
    let updated = store
        .replace("a", schedule.id, 1, spec.clone(), next)
        .await
        .unwrap();
    assert_eq!(updated.id, schedule.id);
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.next_fire_at, Some(next));
    assert_eq!(
        serde_json::to_value(&updated.spec.0).unwrap(),
        serde_json::to_value(&spec).unwrap()
    );
    assert_eq!(updated.request_hash, "original");
    assert_eq!(updated.idempotency_key.as_deref(), Some("creation-key"));
    assert!(matches!(
        store
            .replace("a", schedule.id, 1, schedule_spec(at), at)
            .await,
        Err(StoreError::Conflict(_))
    ));
    let get_run = |id| {
        sqlx::query_as::<_, Run>("SELECT * FROM runs WHERE id=$1")
            .bind(id)
            .fetch_one(&store.pool)
    };
    let cancelled = get_run(pending.id).await.unwrap();
    assert_eq!(cancelled.status, "cancelled");
    assert!(cancelled.finished_at.is_some());
    assert_eq!(cancelled.last_error.as_deref(), Some("Schedule revised"));
    assert_eq!(cancelled.revision, 1);
    assert_eq!(cancelled.spec.name, schedule.spec.name);
    let in_flight = get_run(running.id).await.unwrap();
    assert_eq!(in_flight.status, "running");
    assert_eq!(in_flight.lease_token, running.lease_token);
    assert_eq!(get_run(other_pending.id).await.unwrap().status, "pending");
    let retried_create = store
        .create("a", schedule_spec(at), at, Some("creation-key"), "original")
        .await
        .unwrap();
    assert_eq!(retried_create.id, schedule.id);
    assert_eq!(retried_create.revision, 2);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn competing_replacements_cannot_overwrite_a_new_revision(pool: sqlx::PgPool) {
    use scheduler_service::store::{Store, StoreError};
    let store = Store { pool };
    let at = store.now().await.unwrap();
    let schedule = store
        .create("a", schedule_spec(at), at, None, "test")
        .await
        .unwrap();
    let mut first_spec = schedule_spec(at);
    first_spec.name = "first writer".into();
    let mut second_spec = schedule_spec(at);
    second_spec.name = "second writer".into();
    let (first, second) = tokio::join!(
        store.replace("a", schedule.id, 1, first_spec, at),
        store.replace("a", schedule.id, 1, second_spec, at)
    );
    let winner = match (first, second) {
        (Ok(winner), Err(StoreError::Conflict(_))) | (Err(StoreError::Conflict(_)), Ok(winner)) => {
            winner
        }
        other => panic!("expected exactly one winner: {other:?}"),
    };
    let saved = store.get_schedule("a", schedule.id).await.unwrap();
    assert_eq!(saved.revision, 2);
    assert_eq!(saved.spec.name, winner.spec.name);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires PostgreSQL; run scripts/test.sh"]
async fn replacement_preserves_pause_rejects_cancellation_and_rolls_back_on_failure(
    pool: sqlx::PgPool,
) {
    use scheduler_service::store::{Store, StoreError};
    let store = Store { pool };
    let at = store.now().await.unwrap();
    for (old_status, expected) in [
        ("active", "active"),
        ("paused", "paused"),
        ("completed", "active"),
        ("error", "active"),
    ] {
        let schedule = store
            .create("a", schedule_spec(at), at, None, "test")
            .await
            .unwrap();
        sqlx::query("UPDATE schedules SET status=$2,last_error='old error' WHERE id=$1")
            .bind(schedule.id)
            .bind(old_status)
            .execute(&store.pool)
            .await
            .unwrap();
        let updated = store
            .replace("a", schedule.id, 1, schedule_spec(at), at)
            .await
            .unwrap();
        assert_eq!(updated.status, expected);
        assert!(updated.last_error.is_none());
    }
    let schedule = store
        .create("a", schedule_spec(at), at, None, "test")
        .await
        .unwrap();
    let pending = seed_run(&store.pool, &schedule, 0, false).await;
    sqlx::query("UPDATE schedules SET status='cancelled' WHERE id=$1")
        .bind(schedule.id)
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .replace("a", schedule.id, 1, schedule_spec(at), at)
            .await,
        Err(StoreError::Conflict(_))
    ));
    sqlx::query("UPDATE schedules SET status='active',revision=$2 WHERE id=$1")
        .bind(schedule.id)
        .bind(i64::MAX)
        .execute(&store.pool)
        .await
        .unwrap();
    // The revision increment fails after pending runs were updated: neither
    // update may escape the transaction.
    assert!(matches!(
        store
            .replace("a", schedule.id, i64::MAX, schedule_spec(at), at)
            .await,
        Err(StoreError::Database(_))
    ));
    let saved = store.get_schedule("a", schedule.id).await.unwrap();
    assert_eq!(saved.revision, i64::MAX);
    let run: scheduler_service::domain::Run = sqlx::query_as("SELECT * FROM runs WHERE id=$1")
        .bind(pending.id)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(run.status, "pending");
    assert!(run.finished_at.is_none());
    assert!(run.last_error.is_none());
}
