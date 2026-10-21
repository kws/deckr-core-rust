//! Reusable Action Runtime contracts and validation.
//!
//! Controller-owned identities, routing, settings placement and rendering policy
//! remain application state. These helpers validate supplied facts; they do not
//! allocate or restore live action, binding or page identities.

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::{Error, Result};

mod models;
pub use models::*;
mod messages;
pub use messages::*;
mod matching;
pub use matching::*;
mod rendering;
pub use rendering::*;
mod pages;
pub use pages::*;
mod routing;
pub use routing::*;
mod availability;
pub use availability::*;
mod connection;
pub use connection::*;
mod declaration;
pub use declaration::*;

pub const ACTION_RUNTIME_NAMESPACE: &str = "dev.deckr.action_runtime.provider";
pub const ACTION_RUNTIME_ADVERTISEMENT_PROFILE: &str =
    "dev.deckr.action_runtime.provider.advertisement.v1";
pub const ACTION_RUNTIME_USE_PROFILE: &str = "dev.deckr.action_runtime.provider.service_use.v1";
pub const ACTION_AVAILABILITY_FAMILY: &str = "action_availability";
pub const ACTION_AVAILABILITY_STORE: &str = "deckr_action_runtime_service_view_v1";

/// Strict wire parsing and intrinsic validation. Resolving live identities and
/// negotiated hardware requires the separate current-state checks.
pub trait ActionModel: Serialize + DeserializeOwned {
    fn validate(&self) -> Result<()>;
    fn from_value(value: Value) -> Result<Self> {
        crate::json::validate_numbers(&value)?;
        // RawValue-backed fields preserve arbitrary JSON numbers and object keys.
        let model: Self = serde_json::from_str(&value.to_string())?;
        model.validate()?;
        Ok(model)
    }
    fn from_text(text: &str) -> Result<Self> {
        Self::from_value(crate::json::parse(text.as_bytes())?)
    }
    fn to_value(&self) -> Result<Value> {
        self.validate()?;
        let value = serde_json::to_value(self)?;
        crate::json::validate_numbers(&value)?;
        Ok(value)
    }
}

pub fn validate_provider_instance_id(value: &str) -> Result<()> {
    if !value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(Error::Invalid(
            "provider instance ID must be a restricted ASCII token".into(),
        ));
    }
    Ok(())
}
pub fn action_runtime_service_id(provider_instance_id: &str) -> Result<String> {
    validate_provider_instance_id(provider_instance_id)?;
    Ok(format!("action-runtime.{provider_instance_id}"))
}
pub fn action_runtime_endpoint(
    provider_instance_id: &str,
) -> Result<crate::endpoint::EndpointAddress> {
    crate::endpoint::EndpointAddress::parse(format!(
        "service:{}",
        action_runtime_service_id(provider_instance_id)?
    ))
}
fn text(value: &str) -> Result<()> {
    if value.is_empty() || value.trim() != value {
        Err(Error::Invalid(
            "Action Runtime text must be nonempty and unpadded".into(),
        ))
    } else {
        Ok(())
    }
}
fn optional_text(value: &Option<String>) -> Result<()> {
    value.as_deref().map_or(Ok(()), text)
}
fn token(value: &str) -> Result<()> {
    crate::lanes::require_contract_token(value, "Action Runtime identifier")
}
fn family(value: &str) -> Result<()> {
    crate::lanes::require_globally_qualified_name(value, "capability family")?;
    crate::lanes::validate_core_capability_family(value)
}
fn tokens(values: &[String]) -> Result<()> {
    if values
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != values.len()
    {
        return Err(Error::Invalid("capability names must be unique".into()));
    }
    for value in values {
        token(value)?;
    }
    Ok(())
}
