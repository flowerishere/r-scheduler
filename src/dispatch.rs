use uuid::Uuid;

use crate::{
    domain::{ConcurrencyPolicy, Run, Schedule},
    store::{Store, StoreError},
};

impl Store {
    /// Claim under a schedule -> run lock order, with a fresh token per attempt.
    pub async fn claim(&self) -> Result<Option<Run>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let schedule: Option<Schedule> = sqlx::query_as(
            "SELECT s.* FROM schedules s
             WHERE s.status IN ('active','completed')
             AND EXISTS (SELECT 1 FROM runs r WHERE r.schedule_id=s.id
                 AND r.revision=s.revision AND r.status='pending' AND r.available_at<=statement_timestamp() AND (r.expires_at IS NULL OR r.expires_at>statement_timestamp()))
             AND (s.spec->>'concurrency' IS DISTINCT FROM 'forbid'
                 OR NOT EXISTS (SELECT 1 FROM runs r WHERE r.schedule_id=s.id AND r.status='running'))
             ORDER BY (SELECT MIN(r.available_at) FROM runs r WHERE r.schedule_id=s.id
                 AND r.revision=s.revision AND r.status='pending'), s.id
             LIMIT 1 FOR UPDATE OF s SKIP LOCKED")
            .fetch_optional(&mut *tx).await?;
        let Some(schedule) = schedule else {
            return Ok(None);
        };
        if schedule.spec.concurrency == ConcurrencyPolicy::Forbid {
            let running: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM runs WHERE schedule_id=$1 AND status='running')",
            )
            .bind(schedule.id)
            .fetch_one(&mut *tx)
            .await?;
            if running {
                return Ok(None);
            }
        }
        let candidate: Option<Run> = sqlx::query_as("SELECT * FROM runs WHERE schedule_id=$1 AND revision=$2 AND status='pending' AND available_at<=statement_timestamp() AND (expires_at IS NULL OR expires_at>statement_timestamp()) ORDER BY available_at,id LIMIT 1 FOR UPDATE SKIP LOCKED")
            .bind(schedule.id).bind(schedule.revision).fetch_optional(&mut *tx).await?;
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let token = Uuid::new_v4();
        let lease_seconds = i64::from(candidate.spec.target.timeout_seconds) + 15;
        let run: Option<Run> = sqlx::query_as("UPDATE runs SET status='running',attempt_count=attempt_count+1,cycle_attempts=cycle_attempts+1,lease_token=$2,lease_until=clock_timestamp()+make_interval(secs=>$3::double precision),finished_at=NULL WHERE id=$1 AND (expires_at IS NULL OR expires_at>clock_timestamp()) RETURNING *")
            .bind(candidate.id).bind(token).bind(lease_seconds as f64).fetch_optional(&mut *tx).await?;
        let Some(run) = run else {
            return Ok(None);
        };
        sqlx::query("INSERT INTO attempts (id,run_id,number,status,lease_token) VALUES ($1,$2,$3,'running',$4)")
            .bind(Uuid::new_v4()).bind(run.id).bind(run.attempt_count).bind(token).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(run))
    }
}
