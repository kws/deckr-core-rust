//! Owner-supplied service declarations and compiled payload contracts.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::schema::{CompiledSchema, SchemaRegistry};
use crate::{Error, Result};

use super::{require_text, ExchangePattern, ServiceIntent, ServiceMessageBody, ServiceProtocol};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceMessageDirection {
    ConsumerToService,
    ServiceToConsumer,
    Bidirectional,
}
impl ServiceMessageDirection {
    fn reverse(self) -> Self {
        match self {
            Self::ConsumerToService => Self::ServiceToConsumer,
            Self::ServiceToConsumer => Self::ConsumerToService,
            Self::Bidirectional => Self::Bidirectional,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceViewWriter {
    Service,
    Consumer,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceOperationDefinition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}
impl ServiceOperationDefinition {
    pub fn validate(&self) -> Result<()> {
        if let Some(description) = &self.description {
            require_text(description, "service operation description")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServicePayloadSchema {
    pub schema_id: String,
    #[serde(deserialize_with = "crate::json::deserialize_value")]
    pub schema: Value,
}
impl ServicePayloadSchema {
    pub fn validate(&self) -> Result<()> {
        require_text(&self.schema_id, "service payload schemaId")?;
        let schema = self
            .schema
            .as_object()
            .ok_or_else(|| Error::Invalid("service payload schema must be an object".into()))?;
        if ![
            "$ref",
            "allOf",
            "anyOf",
            "const",
            "enum",
            "items",
            "oneOf",
            "properties",
            "type",
        ]
        .iter()
        .any(|key| schema.contains_key(*key))
        {
            return Err(Error::Invalid(
                "service payload schema must declare a supported schema keyword".into(),
            ));
        }
        crate::json::validate_numbers(&self.schema)?;
        crate::schema::validate_schema(&self.schema)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceMessageDefinition {
    pub intent: ServiceIntent,
    pub exchange_pattern: ExchangePattern,
    pub direction: ServiceMessageDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params_schema: Option<ServicePayloadSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_schema: Option<ServicePayloadSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_schema: Option<ServicePayloadSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_schema: Option<ServicePayloadSchema>,
}
impl ServiceMessageDefinition {
    pub fn validate(&self) -> Result<()> {
        if let Some(operation) = &self.operation {
            require_text(operation, "service message operation")?;
        } else if matches!(
            self.direction,
            ServiceMessageDirection::ConsumerToService | ServiceMessageDirection::Bidirectional
        ) || matches!(self.intent, ServiceIntent::Command | ServiceIntent::Query)
        {
            return Err(Error::Invalid(
                "this service message requires an operation".into(),
            ));
        }
        for schema in [
            &self.params_schema,
            &self.event_schema,
            &self.result_schema,
            &self.error_schema,
        ]
        .into_iter()
        .flatten()
        {
            schema.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceMessageKind {
    Initial,
    Reply,
}

#[derive(Debug, Clone)]
struct PayloadValidators {
    params: Option<CompiledSchema>,
    event: Option<CompiledSchema>,
    result: Option<CompiledSchema>,
    error: Option<CompiledSchema>,
}

/// Immutable owner declaration with compiled, offline payload validators.
/// Compile outside a protocol heartbeat worker. The declaration, rather than
/// an unverified advertisement, determines the meaning of every message.
#[derive(Debug, Clone)]
pub struct ValidatedServiceProtocol {
    protocol: Arc<ServiceProtocol>,
    messages: BTreeMap<String, PayloadValidators>,
}
impl ValidatedServiceProtocol {
    pub fn new(protocol: ServiceProtocol, schemas: &SchemaRegistry) -> Result<Self> {
        protocol.validate()?;
        let compile = |schema: &Option<ServicePayloadSchema>| {
            schema
                .as_ref()
                .map(|schema| schemas.compile(&schema.schema))
                .transpose()
        };
        let messages = protocol
            .messages
            .iter()
            .map(|(name, message)| {
                Ok((
                    name.clone(),
                    PayloadValidators {
                        params: compile(&message.params_schema)?,
                        event: compile(&message.event_schema)?,
                        result: compile(&message.result_schema)?,
                        error: compile(&message.error_schema)?,
                    },
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            protocol: Arc::new(protocol),
            messages,
        })
    }
    pub fn protocol(&self) -> &ServiceProtocol {
        &self.protocol
    }

    /// Validate declaration identity, direction, exchange and supplied payloads.
    /// Replies reverse a request's direction and require request/reply exchange.
    /// `params` defaults to an empty object on initial messages. Optional payload
    /// presence and application resource scope remain owner-protocol policy;
    /// a result schema alone does not require a result on an error reply.
    /// The returned operation still needs the selected context's access check.
    pub fn validate_message<'a>(
        &'a self,
        body: &ServiceMessageBody,
        direction: ServiceMessageDirection,
        kind: ServiceMessageKind,
    ) -> Result<Option<&'a str>> {
        let operation = self.validate_message_metadata(body, direction, kind)?;
        let schemas = &self.messages[&body.name];
        if kind == ServiceMessageKind::Initial || !body.params.is_empty() {
            if let Some(schema) = &schemas.params {
                schema.validate(&Value::Object(body.params.clone()))?;
            }
        }
        if let (Some(schema), Some(value)) = (&schemas.event, &body.event) {
            schema.validate(&Value::Object(value.clone()))?;
        }
        if let (Some(schema), Some(value)) = (&schemas.result, &body.result) {
            schema.validate(&Value::Object(value.clone()))?;
        }
        if let (Some(schema), Some(value)) = (&schemas.error, &body.error) {
            schema.validate(&serde_json::to_value(value)?)?;
        }
        Ok(operation)
    }

    pub(crate) fn validate_message_metadata<'a>(
        &'a self,
        body: &ServiceMessageBody,
        direction: ServiceMessageDirection,
        kind: ServiceMessageKind,
    ) -> Result<Option<&'a str>> {
        body.validate()?;
        if body.service_namespace != self.protocol.namespace {
            return Err(Error::Unauthorized(
                "service message namespace differs from the declaration".into(),
            ));
        }
        let definition = self
            .protocol
            .messages
            .get(&body.name)
            .ok_or_else(|| Error::Unsupported("service message name is not declared".into()))?;
        if body.intent != definition.intent || body.exchange_pattern != definition.exchange_pattern
        {
            return Err(Error::Invalid(
                "service message intent or exchange differs from the declaration".into(),
            ));
        }
        let expected = match kind {
            ServiceMessageKind::Initial => definition.direction,
            ServiceMessageKind::Reply
                if definition.exchange_pattern == ExchangePattern::RequestReply =>
            {
                definition.direction.reverse()
            }
            ServiceMessageKind::Reply => {
                return Err(Error::Invalid(
                    "one-way service message cannot have a reply".into(),
                ))
            }
        };
        if direction == ServiceMessageDirection::Bidirectional
            || (expected != ServiceMessageDirection::Bidirectional && expected != direction)
        {
            return Err(Error::Unauthorized(
                "service message direction differs from the declaration".into(),
            ));
        }
        Ok(definition.operation.as_deref())
    }

    /// Body-level reply correlation, in addition to the managed messenger's
    /// envelope pointer, participant sessions, expiry and request ID checks.
    pub fn validate_reply(
        &self,
        request: &ServiceMessageBody,
        reply: &ServiceMessageBody,
        direction: ServiceMessageDirection,
    ) -> Result<()> {
        if request.name != reply.name
            || request.service_namespace != reply.service_namespace
            || request.intent != reply.intent
            || request.exchange_pattern != ExchangePattern::RequestReply
        {
            return Err(Error::Invalid(
                "service reply differs from the requested message".into(),
            ));
        }
        self.validate_message(reply, direction, ServiceMessageKind::Reply)?;
        Ok(())
    }
}
