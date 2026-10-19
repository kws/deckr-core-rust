use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceIntent {
    Command,
    Query,
    Event,
    Notification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangePattern {
    OneWay,
    RequestReply,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceMessageStatus {
    Ok,
    Rejected,
    Unavailable,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceError {
    pub code: String,
    pub message: String,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_object",
        skip_serializing_if = "Map::is_empty"
    )]
    pub diagnostics: Map<String, Value>,
}
impl ServiceError {
    pub fn validate(&self) -> Result<()> {
        super::require_text(&self.code, "service error code")?;
        super::require_text(&self.message, "service error message")?;
        for value in self.diagnostics.values() {
            crate::json::validate_numbers(value)?;
        }
        Ok(())
    }
}

/// Structural service envelope body. The selected service declaration validates
/// message semantics and the params/event/result/error payload schemas.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceMessageBody {
    pub service_namespace: String,
    pub name: String,
    pub intent: ServiceIntent,
    pub exchange_pattern: ExchangePattern,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_object",
        skip_serializing_if = "Map::is_empty"
    )]
    pub params: Map<String, Value>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_object",
        skip_serializing_if = "Option::is_none"
    )]
    pub event: Option<Map<String, Value>>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_object",
        skip_serializing_if = "Option::is_none"
    )]
    pub result: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ServiceError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ServiceMessageStatus>,
}
impl ServiceMessageBody {
    pub fn validate(&self) -> Result<()> {
        super::require_text(&self.service_namespace, "service namespace")?;
        super::require_text(&self.name, "service message name")?;
        if let Some(error) = &self.error {
            error.validate()?;
        }
        for object in [
            Some(&self.params),
            self.event.as_ref(),
            self.result.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            for value in object.values() {
                crate::json::validate_numbers(value)?;
            }
        }
        Ok(())
    }
    pub fn from_value(value: Value) -> Result<Self> {
        let body: Self = serde_json::from_value(value)?;
        body.validate()?;
        Ok(body)
    }
    pub fn from_text(text: &str) -> Result<Self> {
        Self::from_value(crate::json::parse(text.as_bytes())?)
    }
    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }
}
