use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, bail};
use reqwest::{
    Client,
    header::{HeaderName, HeaderValue},
};
use url::{Host, Url};

use crate::domain::{DeliveryResult, HttpTarget, RetryAfter, Run, ScheduleSpec, Trigger};

pub fn validate_spec(spec: &ScheduleSpec) -> anyhow::Result<()> {
    if let Trigger::Once { at } = spec.trigger {
        crate::trigger::validate_once(at)?;
    }
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
    if spec
        .retry
        .max_age_seconds
        .is_some_and(|age| !(1..=31_536_000).contains(&age))
    {
        bail!("retry.max_age_seconds must be 1..31536000 when provided");
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
pub fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let b = v4.octets();
            !v4.is_private()
                && !v4.is_loopback()
                && !v4.is_link_local()
                && !v4.is_broadcast()
                && !v4.is_unspecified()
                && !v4.is_multicast()
                && !v4.is_documentation()
                && b[0] != 0
                && b[0] < 224
                && !(b[0] == 100 && (64..=127).contains(&b[1]))
                && !(b[0] == 192 && b[1] == 0 && b[2] == 0)
                && !(b[0] == 198 && matches!(b[1], 18 | 19))
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return public_ip(IpAddr::V4(v4));
            }
            let s = v6.segments();
            // IANA special-purpose ranges inside the global-unicast envelope.
            // Also reject 6to4: an apparently global address can embed a private
            // IPv4 destination. This service does not support transition tunnels.
            if (s[0] & 0xe000) != 0x2000
                || s[0] == 0x2002
                || (s[0] == 0x2001 && s[1] == 0x0db8)
                || (s[0] == 0x3fff && s[1] & 0xf000 == 0)
            {
                return false;
            }
            if s[0] == 0x2001 && s[1] < 0x0200 {
                // Globally reachable exceptions to 2001::/23 (IANA, 2026-09).
                return (s[1] == 1 && s[2..7] == [0; 5] && (1..=3).contains(&s[7]))
                    || s[1] == 3
                    || (s[1] == 4 && s[2] == 0x112)
                    || matches!(s[1] & 0xfff0, 0x20 | 0x30);
            }
            true
        }
    }
}
pub async fn deliver(run: &Run, allow_private: bool) -> DeliveryResult {
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(u64::from(run.spec.target.timeout_seconds));
    let mut response = match tokio::time::timeout_at(deadline, send(run, allow_private)).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => return DeliveryResult::error(format!("{error:#}")),
        Err(_) => return DeliveryResult::error("HTTP delivery timed out"),
    };
    let status = response.status();
    let retry_after = if matches!(status.as_u16(), 429 | 503) {
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(RetryAfter::parse)
    } else {
        None
    };
    // Once headers arrive, retain their status and retry hints even if the
    // diagnostic body is interrupted. Both phases share one total deadline.
    let mut excerpt = Vec::new();
    let read_body = async {
        while excerpt.len() < 4096 {
            let Some(chunk) = response.chunk().await? else {
                break;
            };
            let count = chunk.len().min(4096 - excerpt.len());
            excerpt.extend_from_slice(&chunk[..count]);
        }
        Ok::<(), reqwest::Error>(())
    };
    let body_error = match tokio::time::timeout_at(deadline, read_body).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("Read response body: {}", error.without_url())),
        Err(_) => Some("HTTP response body timed out".to_owned()),
    };
    DeliveryResult {
        success: status.is_success() && body_error.is_none(),
        http_status: Some(i32::from(status.as_u16())),
        error: body_error
            .or_else(|| (!status.is_success()).then(|| format!("HTTP {}", status.as_u16()))),
        response_excerpt: Some(String::from_utf8_lossy(&excerpt).replace('\0', "")),
        retry_after,
    }
}
async fn send(run: &Run, allow_private: bool) -> anyhow::Result<reqwest::Response> {
    let target = &run.spec.target;
    let url = validate_target(target)?;
    let port = url.port_or_known_default().context("Missing target port")?;
    let (host, addresses): (String, Vec<SocketAddr>) = match url.host().context("Missing host")? {
        Host::Domain(domain) => (
            domain.to_owned(),
            tokio::net::lookup_host((domain, port))
                .await
                .context("Resolve target host")?
                .take(32)
                .collect(),
        ),
        Host::Ipv4(ip) => (ip.to_string(), vec![SocketAddr::new(IpAddr::V4(ip), port)]),
        Host::Ipv6(ip) => (ip.to_string(), vec![SocketAddr::new(IpAddr::V6(ip), port)]),
    };
    if addresses.is_empty() {
        bail!("Target host has no addresses");
    }
    if !allow_private && addresses.iter().any(|address| !public_ip(address.ip())) {
        bail!("Private/reserved target addresses are disabled");
    }
    // Pin exactly the validated addresses. Redirects and environment proxies are disabled.
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(u64::from(target.timeout_seconds)))
        .resolve_to_addrs(&host, &addresses)
        .build()?;
    let body = serde_json::json!({
        "run_id": run.id, "schedule_id": run.schedule_id, "revision": run.revision,
        "scheduled_at": run.scheduled_at, "attempt": run.attempt_count, "payload": run.spec.payload,
    });
    let mut request = client
        .post(url)
        .json(&body)
        .header("idempotency-key", run.id.to_string())
        .header("x-scheduler-run-id", run.id.to_string())
        .header("x-scheduler-attempt", run.attempt_count.to_string())
        .header("x-scheduler-scheduled-at", run.scheduled_at.to_rfc3339());
    for (name, value) in &target.headers {
        request = request.header(HeaderName::from_str(name)?, HeaderValue::from_str(value)?);
    }
    request
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("HTTP transport failure: {}", e.without_url()))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_special_ipv6_ranges_and_transition_tunnels() {
        for ip in [
            "2001:2::1",
            "2001::1",
            "2001:10::1",
            "2002:7f00:1::",
            "2002:a00:1::",
            "3fff::1",
            "3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff",
        ] {
            assert!(!public_ip(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "2001:4860:4860::8888",
            "2606:4700:4700::1111",
            "2001:1::1",
            "2001:3::1",
            "2001:4:112::1",
        ] {
            assert!(public_ip(ip.parse().unwrap()), "{ip}");
        }
    }

    #[test]
    fn rejects_local_metadata_and_mapped_addresses() {
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "::ffff:127.0.0.1",
            "fc00::1",
        ] {
            assert!(!public_ip(ip.parse().unwrap()), "{ip}");
        }
        assert!(public_ip("1.1.1.1".parse().unwrap()));
        assert!(public_ip("2606:4700:4700::1111".parse().unwrap()));
    }
}
