use crate::domain::{HttpTarget, ScheduleSpec};
use anyhow::{Context, bail};
use reqwest::header::{HeaderName, HeaderValue};
use std::str::FromStr;
use url::Url;
pub fn validate_spec(spec: &ScheduleSpec) -> anyhow::Result<()> {
    if contains_nul(&serde_json::to_value(spec)?) {
        bail!("JSON strings and field names must not contain U+0000");
    }
    if spec.name.trim().is_empty() || spec.name.len() > 200 {
        bail!("name must contain 1..200 bytes");
    }
    if serde_json::to_vec(&spec.payload)?.len() > 65_536 {
        bail!("payload exceeds 64 KiB");
    }
    if !(1..=100).contains(&spec.retry.max_attempts)
        || !(1..=86400).contains(&spec.retry.initial_delay_seconds)
        || spec.retry.max_delay_seconds < spec.retry.initial_delay_seconds
        || spec.retry.max_delay_seconds > 86400
    {
        bail!("Invalid retry policy: attempts 1..100, delays 1..86400, max delay >= initial delay");
    }
    if spec.misfire_grace_seconds > 86400 {
        bail!("misfire_grace_seconds must be <= 86400");
    }
    validate_target(&spec.target)?;
    Ok(())
}
fn contains_nul(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(value) => value.contains('\0'),
        serde_json::Value::Array(values) => values.iter().any(contains_nul),
        serde_json::Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains('\0') || contains_nul(value)),
        _ => false,
    }
}
pub fn validate_target(target: &HttpTarget) -> anyhow::Result<Url> {
    if target.url.len() > 4096 {
        bail!("Target URL exceeds 4096 bytes");
    }
    let url = Url::parse(&target.url).context("Invalid target URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        bail!("Target must be an HTTP(S) URL without embedded credentials or fragment");
    }
    if !(1..=300).contains(&target.timeout_seconds) {
        bail!("timeout_seconds must be 1..300");
    }
    if target.headers.len() > 16 {
        bail!("At most 16 target headers are allowed");
    }
    for (name, value) in &target.headers {
        let name = HeaderName::from_str(name).context("Invalid target header name")?;
        if name.as_str().starts_with("x-scheduler-")
            || matches!(
                name.as_str(),
                "host"
                    | "content-length"
                    | "transfer-encoding"
                    | "connection"
                    | "content-type"
                    | "idempotency-key"
                    | "upgrade"
                    | "te"
                    | "trailer"
                    | "proxy-authorization"
            )
        {
            bail!("Target header {name} is reserved");
        }
        if value.len() > 4096 {
            bail!("Target header value exceeds 4096 bytes");
        }
        HeaderValue::from_str(value).context("Invalid target header value")?;
    }
    Ok(url)
}
