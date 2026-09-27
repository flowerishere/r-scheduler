use chrono::{DateTime, Utc};
use sqlx::{PgPool, postgres::PgPoolOptions};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("Resource not found")]
    NotFound,
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    InvalidInput(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone)]
pub struct Store {
    pub pool: PgPool,
}

impl Store {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(32)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = '15s'")
                        .execute(&mut *connection)
                        .await?;
                    sqlx::query("SET lock_timeout = '5s'")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn now(&self) -> Result<DateTime<Utc>> {
        Ok(sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
            .await?)
    }
}
