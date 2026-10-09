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
    /// No new attempt may start after scheduled_at + max_age_seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_age_seconds: Option<u32>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay_seconds: 5,
            max_delay_seconds: 3600,
            max_age_seconds: None,
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
    pub expires_at: Option<DateTime<Utc>>,
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
        assert_eq!(retry.max_age_seconds, None);
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
    pub retry_after: Option<RetryAfter>,
}

impl DeliveryResult {
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            http_status: None,
            error: Some(message.into()),
            response_excerpt: None,
            retry_after: None,
        }
    }
}

impl RetryPolicy {
    pub fn delay(&self, attempt: i32, run_id: Uuid) -> i64 {
        let exponent = (attempt.saturating_sub(1) as u32).min(30);
        let base = u64::from(self.initial_delay_seconds).saturating_mul(1_u64 << exponent);
        let jitter = (run_id.as_u128() as u64).wrapping_add(attempt as u64 * 31) % (base / 4 + 1);
        base.saturating_add(jitter)
            .min(u64::from(self.max_delay_seconds)) as i64
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RetryAfter {
    Seconds(u64),
    At(DateTime<Utc>),
}

impl RetryAfter {
    pub fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Some(Self::Seconds(value.parse().unwrap_or(u64::MAX)));
        }
        httpdate::parse_http_date(value)
            .ok()
            .map(|date| Self::At(date.into()))
    }

    pub fn delay(&self, now: DateTime<Utc>, cap: u32) -> i64 {
        let seconds = match self {
            Self::Seconds(seconds) => *seconds,
            Self::At(date) => {
                let remaining = date.signed_duration_since(now);
                // Round up so a fractional second cannot start the retry early.
                remaining.num_seconds().max(0) as u64
                    + u64::from(
                        remaining > chrono::Duration::zero() && remaining.subsec_nanos() > 0,
                    )
            }
        };
        seconds.min(u64::from(cap)) as i64
    }
}

#[cfg(test)]
mod retry_after_tests {
    use super::*;
    #[test]
    fn retry_after_accepts_dates_and_seconds_with_bounded_waits() {
        let now: DateTime<Utc> = "2026-09-21T12:00:00.100Z".parse().unwrap();
        assert_eq!(RetryAfter::parse(" 7 "), Some(RetryAfter::Seconds(7)));
        let date = RetryAfter::parse("Mon, 21 Sep 2026 12:00:08 GMT").unwrap();
        assert_eq!(date.delay(now, 60), 8);
        assert_eq!(date.delay(now, 5), 5);
        assert_eq!(date.delay(now + chrono::Duration::seconds(9), 60), 0);
        assert_eq!(
            RetryAfter::parse("9999999999999999999999999")
                .unwrap()
                .delay(now, 60),
            60
        );
        for invalid in ["", "-1", "+1", "1.5", "tomorrow"] {
            assert_eq!(RetryAfter::parse(invalid), None);
        }
    }
}
