use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, types::Json};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Trigger {
    Once {
        at: DateTime<Utc>,
    },
    Delay {
        seconds: u32,
    },
    Cron {
        expression: String,
        timezone: String,
    },
    Rrule {
        value: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTarget {
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u32,
}

fn default_timeout() -> u32 {
    30
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay_seconds: u32,
    pub max_delay_seconds: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay_seconds: 5,
            max_delay_seconds: 3600,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MisfirePolicy {
    Skip,
    #[default]
    FireOnce,
    CatchUp,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyPolicy {
    #[default]
    Allow,
    Forbid,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleSpec {
    pub name: String,
    pub trigger: Trigger,
    pub target: HttpTarget,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default)]
    pub misfire: MisfirePolicy,
    #[serde(default = "default_grace")]
    pub misfire_grace_seconds: u32,
    #[serde(default)]
    pub concurrency: ConcurrencyPolicy,
}

fn default_grace() -> u32 {
    60
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct Schedule {
    pub id: Uuid,
    pub tenant_id: String,
    pub spec: Json<ScheduleSpec>,
    pub status: String,
    pub revision: i64,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing)]
    pub request_hash: String,
    #[serde(skip_serializing)]
    pub idempotency_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct Run {
    pub id: Uuid,
    pub schedule_id: Uuid,
    pub tenant_id: String,
    pub revision: i64,
    pub scheduled_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub status: String,
    pub attempt_count: i32,
    pub cycle_attempts: i32,
    pub lease_until: Option<DateTime<Utc>>,
    #[serde(skip_serializing)]
    pub lease_token: Option<Uuid>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub spec: Json<ScheduleSpec>,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct Attempt {
    pub id: Uuid,
    pub run_id: Uuid,
    pub number: i32,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub http_status: Option<i32>,
    pub error: Option<String>,
    pub response_excerpt: Option<String>,
    #[serde(skip_serializing)]
    pub lease_token: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_retry_serialization_preserves_existing_idempotency_hashes() {
        let old = r#"{"max_attempts":5,"initial_delay_seconds":5,"max_delay_seconds":3600}"#;
        let retry: RetryPolicy = serde_json::from_str(old).unwrap();
        assert_eq!(serde_json::to_string(&retry).unwrap(), old);
    }

    #[test]
    fn schedule_defaults_and_unknown_fields_are_explicit() {
        let input = json!({"name":"demo", "trigger":{"type":"delay","seconds":0}, "target":{"url":"https://example.invalid/hook"}});
        let spec: ScheduleSpec = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(spec.target.timeout_seconds, 30);
        assert!(spec.target.headers.is_empty());
        assert!(spec.payload.is_null());
        assert_eq!(spec.misfire, MisfirePolicy::FireOnce);
        assert_eq!(spec.misfire_grace_seconds, 60);
        assert_eq!(spec.concurrency, ConcurrencyPolicy::Allow);
        for invalid in [
            json!({"name":"missing target", "trigger":{"type":"delay","seconds":1}}),
            json!({"type":"delay","seconds":-1}),
        ] {
            assert!(serde_json::from_value::<ScheduleSpec>(invalid).is_err());
        }
        let mut unknown = input;
        unknown["unrecognized"] = json!(true);
        assert!(serde_json::from_value::<ScheduleSpec>(unknown).is_err());
        assert!(serde_json::from_value::<Trigger>(json!({"type":"delay","seconds":-1})).is_err());
        assert!(
            serde_json::from_value::<Trigger>(json!({"type":"delay","seconds":1,"typo":true}))
                .is_err()
        );
    }
}

#[derive(Debug)]
pub struct DeliveryResult {
    pub success: bool,
    pub http_status: Option<i32>,
    pub error: Option<String>,
    pub response_excerpt: Option<String>,
}

impl DeliveryResult {
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            http_status: None,
            error: Some(message.into()),
            response_excerpt: None,
        }
    }
}
