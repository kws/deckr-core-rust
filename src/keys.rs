use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use crate::endpoint::EndpointAddress;
use crate::{Error, Result};

pub fn encode_key_token(raw: &str) -> String {
    if is_safe_token(raw) && !raw.starts_with("b64_") {
        raw.to_string()
    } else {
        format!("b64_{}", URL_SAFE_NO_PAD.encode(raw.as_bytes()))
    }
}

pub fn decode_key_token(token: &str) -> Result<String> {
    if !token.starts_with("b64_") {
        return Ok(token.to_string());
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(&token.as_bytes()[4..])
        .map_err(|error| Error::Invalid(format!("invalid key token {token:?}: {error}")))?;
    String::from_utf8(bytes)
        .map_err(|error| Error::Invalid(format!("invalid UTF-8 key token {token:?}: {error}")))
}

pub fn beacon_advertisement_key(feature_id: &str, advertisement_id: &str) -> String {
    format!(
        "advertisements.by_feature.{}.{}",
        encode_key_token(feature_id),
        encode_key_token(advertisement_id)
    )
}

pub fn parse_beacon_advertisement_key(key: &str) -> Option<(String, String)> {
    let parts = key.split('.').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "advertisements" || parts[1] != "by_feature" {
        return None;
    }
    Some((
        decode_key_token(parts[2]).ok()?,
        decode_key_token(parts[3]).ok()?,
    ))
}

pub fn beacon_feature_prefix(feature_id: &str) -> String {
    format!(
        "advertisements.by_feature.{}.",
        encode_key_token(feature_id)
    )
}

pub fn concord_contract_key(contract_id: &str, generation: u64) -> String {
    format!(
        "contracts.{}.{}.meta",
        encode_key_token(contract_id),
        generation
    )
}

pub fn parse_concord_contract_key(key: &str) -> Option<(String, u64)> {
    let parts = key.split('.').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "contracts" || parts[3] != "meta" {
        return None;
    }
    Some((decode_key_token(parts[1]).ok()?, parts[2].parse().ok()?))
}

pub fn concord_participant_token_key(
    contract_id: &str,
    generation: u64,
    participant: &EndpointAddress,
) -> String {
    format!(
        "contracts.{}.{}.participants.{}",
        encode_key_token(contract_id),
        generation,
        encode_key_token(participant.as_str())
    )
}

pub fn parse_concord_participant_token_key(key: &str) -> Option<(String, u64, EndpointAddress)> {
    let parts = key.split('.').collect::<Vec<_>>();
    if parts.len() != 5 || parts[0] != "contracts" || parts[3] != "participants" {
        return None;
    }
    Some((
        decode_key_token(parts[1]).ok()?,
        parts[2].parse().ok()?,
        EndpointAddress::parse(decode_key_token(parts[4]).ok()?).ok()?,
    ))
}

pub fn concord_contract_prefix(contract_id: &str, generation: u64) -> String {
    format!(
        "contracts.{}.{}.",
        encode_key_token(contract_id),
        generation
    )
}

pub fn concord_contracts_prefix() -> &'static str {
    "contracts."
}

pub fn service_use_scope_index_key(scope_id: &str) -> String {
    format!("scopes.{}", encode_key_token(scope_id))
}

fn is_safe_token(raw: &str) -> bool {
    let mut chars = raw.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
}
