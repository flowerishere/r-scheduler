use std::collections::BTreeMap;

use anyhow::bail;

pub type Metadata = BTreeMap<String, String>;
pub const MAX_METADATA_BYTES: usize = 8192;

/// Identical bounds for stored metadata and containment queries. BTreeMap also
/// makes request hashes independent of the caller's JSON key order.
pub fn validate(metadata: &Metadata) -> anyhow::Result<()> {
    if metadata.len() > 32 {
        bail!("metadata supports at most 32 entries");
    }
    for (key, value) in metadata {
        if key.trim().is_empty() || key.len() > 64 || key.chars().any(char::is_control) {
            bail!("metadata keys must contain 1..64 bytes without control characters");
        }
        if value.len() > 512 || value.chars().any(char::is_control) {
            bail!("metadata values must contain at most 512 bytes without control characters");
        }
    }
    if serde_json::to_vec(metadata)?.len() > MAX_METADATA_BYTES {
        bail!("metadata exceeds 8 KiB");
    }
    Ok(())
}

pub fn parse_filter(value: &str) -> anyhow::Result<Metadata> {
    if value.len() > MAX_METADATA_BYTES {
        bail!("metadata filter exceeds 8 KiB");
    }
    let metadata = serde_json::from_str(value)?;
    validate(&metadata)?;
    Ok(metadata)
}
