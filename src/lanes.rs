use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::authority::ContractPointer;
use crate::endpoint::{
    EndpointAddress, ACTION_PROVIDER_FAMILY, CONTROLLER_FAMILY, HARDWARE_MANAGER_FAMILY,
    SERVICE_FAMILY,
};
use crate::keys::encode_key_token;
use crate::{Error, Result};

pub const LANE_SUBJECT_PREFIX: &str = "deckr.msg";
pub const HARDWARE_MESSAGES_LANE: &str = "hardware_messages";
pub const HARDWARE_MESSAGES_SCHEMA_ID: &str = "dev.deckr.message.hardware_messages.v1";
pub const DECKR_PROTOCOL_VERSION: &str = "1";
pub const SERVICES_LANE: &str = "services";

const JSON_SCHEMA_CONTRACT_KEYS: &[&str] = &[
    "$ref",
    "allOf",
    "anyOf",
    "const",
    "enum",
    "items",
    "oneOf",
    "properties",
    "type",
];

mod descriptors;
pub use descriptors::*;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "targetType", deny_unknown_fields)]
pub enum MessageTarget {
    #[serde(rename = "endpoint")]
    Endpoint { endpoint: String },
    #[serde(rename = "broadcast")]
    Broadcast {
        scope: String,
        #[serde(rename = "endpointFamily")]
        endpoint_family: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        domain: Option<String>,
        #[serde(rename = "hopLimit", skip_serializing_if = "Option::is_none")]
        hop_limit: Option<u32>,
    },
}

impl<'de> Deserialize<'de> for MessageTarget {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged, deny_unknown_fields)]
        enum Target {
            Endpoint {
                #[serde(rename = "targetType", default = "endpoint_kind")]
                target_type: String,
                endpoint: String,
            },
            Broadcast {
                #[serde(rename = "targetType", default = "broadcast_kind")]
                target_type: String,
                scope: String,
                #[serde(rename = "endpointFamily")]
                endpoint_family: String,
                domain: Option<String>,
                #[serde(rename = "hopLimit")]
                hop_limit: Option<u32>,
            },
        }
        fn endpoint_kind() -> String {
            "endpoint".into()
        }
        fn broadcast_kind() -> String {
            "broadcast".into()
        }
        match Target::deserialize(deserializer)? {
            Target::Endpoint {
                target_type,
                endpoint,
            } if target_type == "endpoint" => Ok(Self::Endpoint { endpoint }),
            Target::Broadcast {
                target_type,
                scope,
                endpoint_family,
                domain,
                hop_limit,
            } if target_type == "broadcast" => Ok(Self::Broadcast {
                scope,
                endpoint_family,
                domain,
                hop_limit,
            }),
            _ => Err(serde::de::Error::custom(
                "recipient must be one endpoint or broadcast target",
            )),
        }
    }
}

fn default_wire_version() -> String {
    "1".into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EntitySubject {
    pub kind: String,
    #[serde(default)]
    pub identifiers: BTreeMap<String, String>,
}

impl EntitySubject {
    pub fn hardware_device(manager_id: &str, device_id: &str) -> Self {
        let mut identifiers = BTreeMap::new();
        identifiers.insert("managerId".to_string(), manager_id.to_string());
        identifiers.insert("deviceId".to_string(), device_id.to_string());
        Self {
            kind: "hardware_device".to_string(),
            identifiers,
        }
    }

    pub fn hardware_capability(
        manager_id: &str,
        device_id: &str,
        control_id: Option<&str>,
        capability_id: &str,
    ) -> Self {
        let mut identifiers = BTreeMap::new();
        identifiers.insert("managerId".to_string(), manager_id.to_string());
        identifiers.insert("deviceId".to_string(), device_id.to_string());
        if let Some(control_id) = control_id {
            identifiers.insert("controlId".to_string(), control_id.to_string());
        }
        identifiers.insert("capabilityId".to_string(), capability_id.to_string());
        Self {
            kind: "hardware_capability".to_string(),
            identifiers,
        }
    }

    pub fn device_id(&self) -> Option<&str> {
        self.identifiers.get("deviceId").map(String::as_str)
    }

    pub fn manager_id(&self) -> Option<&str> {
        self.identifiers.get("managerId").map(String::as_str)
    }

    pub fn validate(&self) -> Result<()> {
        require_non_empty(&self.kind, "entity subject kind")?;
        for (key, value) in &self.identifiers {
            require_non_empty(key, "entity subject id field")?;
            require_non_empty(value, "entity subject id value")?;
        }
        Ok(())
    }
}

impl MessageTarget {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Endpoint { endpoint } => {
                EndpointAddress::parse(endpoint)?;
            }
            Self::Broadcast {
                scope,
                endpoint_family,
                domain,
                ..
            } => {
                require_non_empty(scope, "broadcast scope")?;
                validate_endpoint_family(endpoint_family, "broadcast endpoint family")?;
                if let Some(domain) = domain {
                    require_non_empty(domain, "broadcast domain")?;
                }
            }
        }
        Ok(())
    }

    pub fn targets_endpoint(&self, endpoint: &EndpointAddress) -> Result<bool> {
        Ok(match self {
            Self::Endpoint { endpoint: target } => EndpointAddress::parse(target)? == *endpoint,
            Self::Broadcast {
                endpoint_family, ..
            } => endpoint_family == endpoint.family(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeckrMessage {
    pub message_id: String,
    #[serde(default = "default_wire_version")]
    pub protocol_version: String,
    #[serde(default = "default_wire_version")]
    pub schema_version: String,
    pub lane: String,
    pub message_type: String,
    pub sender: String,
    pub sender_session_id: String,
    pub recipient: MessageTarget,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipient_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract: Option<ContractPointer>,
    pub subject: EntitySubject,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub trace: Option<Value>,
    #[serde(deserialize_with = "crate::json::deserialize_value")]
    pub body: Value,
}

impl DeckrMessage {
    pub fn hardware_input(
        manager_id: &str,
        sender_session_id: &str,
        device_id: &str,
        contract: ContractPointer,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        Self::hardware(
            format!("hardware_manager:{manager_id}"),
            sender_session_id.to_string(),
            (
                MessageTarget::Broadcast {
                    scope: "controllers".to_string(),
                    endpoint_family: "controller".to_string(),
                    domain: None,
                    hop_limit: None,
                },
                None,
            ),
            manager_id,
            device_id,
            contract,
            body,
        )
    }

    pub fn hardware_input_to(
        manager_id: &str,
        sender_session_id: &str,
        device_id: &str,
        controller_endpoint: &str,
        controller_session_id: &str,
        contract: ContractPointer,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        Self::hardware(
            format!("hardware_manager:{manager_id}"),
            sender_session_id.to_string(),
            (
                MessageTarget::Endpoint {
                    endpoint: controller_endpoint.to_string(),
                },
                Some(controller_session_id.to_string()),
            ),
            manager_id,
            device_id,
            contract,
            body,
        )
    }

    pub fn hardware_command(
        controller_id: &str,
        controller_session_id: &str,
        manager_id: &str,
        manager_session_id: &str,
        device_id: &str,
        contract: ContractPointer,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        Self::hardware(
            format!("controller:{controller_id}"),
            controller_session_id.to_string(),
            (
                MessageTarget::Endpoint {
                    endpoint: format!("hardware_manager:{manager_id}"),
                },
                Some(manager_session_id.to_string()),
            ),
            manager_id,
            device_id,
            contract,
            body,
        )
    }

    fn hardware(
        sender: String,
        sender_session_id: String,
        recipient: (MessageTarget, Option<String>),
        manager_id: &str,
        device_id: &str,
        contract: ContractPointer,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        let (recipient, recipient_session_id) = recipient;
        contract.validate()?;
        let subject = match body.capability_id() {
            Some(capability_id) => EntitySubject::hardware_capability(
                manager_id,
                device_id,
                body.control_id(),
                capability_id,
            ),
            None => EntitySubject::hardware_device(manager_id, device_id),
        };
        Ok(Self {
            message_id: Uuid::new_v4().to_string(),
            protocol_version: DECKR_PROTOCOL_VERSION.to_string(),
            schema_version: "1".to_string(),
            lane: HARDWARE_MESSAGES_LANE.to_string(),
            message_type: body.message_type().to_string(),
            sender,
            sender_session_id,
            recipient,
            recipient_session_id,
            contract: Some(contract),
            subject,
            created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            expires_at: None,
            ttl_ms: None,
            in_reply_to: None,
            causation_id: None,
            trace: None,
            body: body.to_value()?,
        })
    }

    pub fn to_text(&self) -> Result<String> {
        self.validate()?;
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_text(text: &str) -> Result<Self> {
        Self::from_bytes(text.as_bytes())
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let message: Self = serde_json::from_value(crate::json::parse(bytes)?)?;
        message.validate()?;
        Ok(message)
    }

    pub fn hardware_body(&self) -> Result<HardwareMessageBody> {
        HardwareMessageBody::from_message(&self.message_type, &self.body)
    }

    pub fn service_body(&self) -> Result<crate::services::ServiceMessageBody> {
        crate::services::ServiceMessageBody::from_value(self.body.clone())
    }

    pub fn recipient_endpoint(&self) -> Option<&str> {
        match &self.recipient {
            MessageTarget::Endpoint { endpoint } => Some(endpoint.as_str()),
            MessageTarget::Broadcast { .. } => None,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.is_expired_at(Utc::now())
    }

    pub fn is_expired_at(&self, now: DateTime<Utc>) -> bool {
        let expires_at = self
            .expires_at
            .as_deref()
            .and_then(parse_datetime)
            .into_iter()
            .chain(self.ttl_ms.and_then(|ttl_ms| {
                parse_datetime(&self.created_at).and_then(|created_at| {
                    i64::try_from(ttl_ms)
                        .ok()
                        .and_then(Duration::try_milliseconds)
                        .and_then(|ttl| created_at.checked_add_signed(ttl))
                })
            }))
            .min();
        expires_at.is_some_and(|expires_at| expires_at <= now)
    }

    pub fn validate(&self) -> Result<()> {
        require_non_empty(&self.message_id, "message id")?;
        crate::json::validate_numbers(&self.body)?;
        if self.protocol_version != DECKR_PROTOCOL_VERSION {
            return Err(Error::Invalid(format!(
                "protocolVersion must be {DECKR_PROTOCOL_VERSION}"
            )));
        }
        require_non_empty(&self.schema_version, "schema version")?;
        require_non_empty(&self.lane, "lane")?;
        require_non_empty(&self.message_type, "message type")?;
        EndpointAddress::parse(&self.sender)?;
        require_non_empty(&self.sender_session_id, "sender session id")?;
        self.recipient.validate()?;
        if let Some(contract) = &self.contract {
            contract.validate()?;
        }
        if let Some(recipient_session_id) = &self.recipient_session_id {
            require_non_empty(recipient_session_id, "recipient session id")?;
            if !matches!(self.recipient, MessageTarget::Endpoint { .. }) {
                return Err(Error::Invalid(
                    "recipientSessionId is only valid for endpoint recipients".to_string(),
                ));
            }
        }
        self.subject.validate()?;
        parse_datetime(&self.created_at)
            .ok_or_else(|| Error::Invalid("createdAt must be an RFC 3339 datetime".to_string()))?;
        if let Some(expires_at) = &self.expires_at {
            parse_datetime(expires_at).ok_or_else(|| {
                Error::Invalid("expiresAt must be an RFC 3339 datetime".to_string())
            })?;
        }
        if !self.body.is_object() {
            return Err(Error::Invalid(
                "message body must be a JSON object".to_string(),
            ));
        }
        if let Some(trace) = &self.trace {
            let Some(trace) = trace.as_object() else {
                return Err(Error::Invalid("trace must be an object".into()));
            };
            if trace.iter().any(|(key, value)| {
                !matches!(key.as_str(), "traceParent" | "traceState")
                    || !(value.is_null() || value.is_string())
            }) {
                return Err(Error::Invalid("invalid trace context".into()));
            }
        }
        if self.lane == HARDWARE_MESSAGES_LANE {
            if self.contract.is_none() {
                return Err(Error::Invalid(
                    "hardware_messages messages require a Concord contract pointer".to_string(),
                ));
            }
            self.hardware_body()?;
            let allowed = |endpoint: &str| {
                EndpointAddress::parse(endpoint).is_ok_and(|endpoint| {
                    matches!(
                        endpoint.family(),
                        CONTROLLER_FAMILY | HARDWARE_MANAGER_FAMILY
                    )
                })
            };
            if !allowed(&self.sender)
                || match &self.recipient {
                    MessageTarget::Endpoint { endpoint } => !allowed(endpoint),
                    MessageTarget::Broadcast {
                        scope,
                        endpoint_family,
                        ..
                    } => scope != "controllers" || endpoint_family != CONTROLLER_FAMILY,
                }
            {
                return Err(Error::Invalid(
                    "hardware lane endpoint family or broadcast scope is not permitted".into(),
                ));
            }
        } else if self.lane == SERVICES_LANE {
            let allowed = |endpoint: &str| {
                EndpointAddress::parse(endpoint).is_ok_and(|endpoint| {
                    matches!(
                        endpoint.family(),
                        CONTROLLER_FAMILY | ACTION_PROVIDER_FAMILY | SERVICE_FAMILY
                    )
                })
            };
            if self.message_type != "serviceMessage"
                || self.contract.is_none()
                || !allowed(&self.sender)
                || !self.recipient_endpoint().is_some_and(allowed)
            {
                return Err(Error::Invalid("services lane requires serviceMessage, a contract, and permitted direct endpoints".into()));
            }
            self.service_body()?;
        }
        Ok(())
    }

    pub fn is_deliverable_to(
        &self,
        endpoint: &EndpointAddress,
        endpoint_session_id: &str,
    ) -> Result<bool> {
        message_is_deliverable_to(self, endpoint, endpoint_session_id)
    }

    pub fn is_directly_deliverable_to(
        &self,
        endpoint: &EndpointAddress,
        endpoint_session_id: &str,
    ) -> Result<bool> {
        self.validate()?;
        if self.is_expired() {
            return Ok(false);
        }
        if self
            .recipient_session_id
            .as_deref()
            .is_some_and(|session_id| session_id != endpoint_session_id)
        {
            return Ok(false);
        }
        let MessageTarget::Endpoint { endpoint: target } = &self.recipient else {
            return Ok(false);
        };
        Ok(EndpointAddress::parse(target)? == *endpoint)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum HardwareMessageBody {
    ControlInput {
        device_ref: DeviceRef,
        control_id: String,
        capability_id: String,
        event_type: String,
        value: Option<Value>,
        occurred_at: Option<String>,
        sequence: Option<u64>,
        sources: Vec<DeviceSourceReference>,
    },
    ControlCommand {
        device_ref: DeviceRef,
        control_id: Option<String>,
        capability_id: String,
        command_type: String,
        params: serde_json::Map<String, Value>,
    },
    CapabilityStateChanged {
        device_ref: DeviceRef,
        control_id: Option<String>,
        capability_id: String,
        state_type: Option<String>,
        value: Option<Value>,
        occurred_at: Option<String>,
        sequence: Option<u64>,
    },
    CapabilityStateRequest {
        device_ref: DeviceRef,
        control_id: Option<String>,
        capability_id: String,
        state_type: Option<String>,
        params: serde_json::Map<String, Value>,
    },
    CapabilityStateReply {
        device_ref: DeviceRef,
        control_id: Option<String>,
        capability_id: String,
        state_type: Option<String>,
        status: String,
        value: Option<Value>,
        error: Option<String>,
    },
    CommandAccepted {
        device_ref: DeviceRef,
        control_id: Option<String>,
        capability_id: String,
        command_type: String,
        accepted_at: Option<String>,
    },
    CommandRejected {
        device_ref: DeviceRef,
        control_id: Option<String>,
        capability_id: String,
        command_type: String,
        reason: String,
        message: Option<String>,
    },
    CommandReply {
        device_ref: DeviceRef,
        control_id: Option<String>,
        capability_id: String,
        command_type: String,
        result: Option<Value>,
    },
}

impl HardwareMessageBody {
    pub fn message_type(&self) -> &'static str {
        match self {
            Self::ControlInput { .. } => "controlInput",
            Self::ControlCommand { .. } => "controlCommand",
            Self::CapabilityStateChanged { .. } => "capabilityStateChanged",
            Self::CapabilityStateRequest { .. } => "capabilityStateRequest",
            Self::CapabilityStateReply { .. } => "capabilityStateReply",
            Self::CommandAccepted { .. } => "commandAccepted",
            Self::CommandRejected { .. } => "commandRejected",
            Self::CommandReply { .. } => "commandReply",
        }
    }

    pub fn control_id(&self) -> Option<&str> {
        match self {
            Self::ControlInput { control_id, .. } => Some(control_id),
            Self::ControlCommand { control_id, .. }
            | Self::CapabilityStateChanged { control_id, .. }
            | Self::CapabilityStateRequest { control_id, .. }
            | Self::CapabilityStateReply { control_id, .. }
            | Self::CommandAccepted { control_id, .. }
            | Self::CommandRejected { control_id, .. }
            | Self::CommandReply { control_id, .. } => control_id.as_deref(),
        }
    }

    pub fn capability_id(&self) -> Option<&str> {
        match self {
            Self::ControlInput { capability_id, .. }
            | Self::ControlCommand { capability_id, .. }
            | Self::CapabilityStateChanged { capability_id, .. }
            | Self::CapabilityStateRequest { capability_id, .. }
            | Self::CapabilityStateReply { capability_id, .. }
            | Self::CommandAccepted { capability_id, .. }
            | Self::CommandRejected { capability_id, .. }
            | Self::CommandReply { capability_id, .. } => Some(capability_id),
        }
    }

    pub fn device_ref(&self) -> &DeviceRef {
        match self {
            Self::ControlInput { device_ref, .. }
            | Self::ControlCommand { device_ref, .. }
            | Self::CapabilityStateChanged { device_ref, .. }
            | Self::CapabilityStateRequest { device_ref, .. }
            | Self::CapabilityStateReply { device_ref, .. }
            | Self::CommandAccepted { device_ref, .. }
            | Self::CommandRejected { device_ref, .. }
            | Self::CommandReply { device_ref, .. } => device_ref,
        }
    }

    pub fn is_command(&self) -> bool {
        matches!(
            self,
            Self::ControlCommand { .. } | Self::CapabilityStateRequest { .. }
        )
    }

    pub fn to_value(&self) -> Result<Value> {
        Ok(match self {
            Self::ControlInput {
                device_ref,
                control_id,
                capability_id,
                event_type,
                value,
                occurred_at,
                sequence,
                sources,
            } => {
                let mut value = json!({
                    "deviceRef": device_ref,
                    "controlId": control_id,
                    "capabilityId": capability_id,
                    "eventType": event_type,
                    "sources": sources,
                    "value": value
                });
                insert_optional_string(&mut value, "occurredAt", occurred_at);
                insert_optional_u64(&mut value, "sequence", *sequence);
                value
            }
            Self::ControlCommand {
                device_ref,
                control_id,
                capability_id,
                command_type,
                params,
            } => {
                let mut value = json!({
                    "deviceRef": device_ref,
                    "capabilityId": capability_id,
                    "commandType": command_type,
                    "params": params
                });
                if let Some(control_id) = control_id {
                    value["controlId"] = json!(control_id);
                }
                value
            }
            Self::CapabilityStateChanged {
                device_ref,
                control_id,
                capability_id,
                state_type,
                value,
                occurred_at,
                sequence,
            } => {
                let mut body = json!({
                    "deviceRef": device_ref,
                    "capabilityId": capability_id,
                    "value": value
                });
                insert_optional_string(&mut body, "controlId", control_id);
                insert_optional_string(&mut body, "stateType", state_type);
                insert_optional_string(&mut body, "occurredAt", occurred_at);
                insert_optional_u64(&mut body, "sequence", *sequence);
                body
            }
            Self::CapabilityStateRequest {
                device_ref,
                control_id,
                capability_id,
                state_type,
                params,
            } => {
                let mut body = json!({
                    "deviceRef": device_ref,
                    "capabilityId": capability_id,
                    "params": params
                });
                insert_optional_string(&mut body, "controlId", control_id);
                insert_optional_string(&mut body, "stateType", state_type);
                body
            }
            Self::CapabilityStateReply {
                device_ref,
                control_id,
                capability_id,
                state_type,
                status,
                value,
                error,
            } => {
                let mut body = json!({
                    "deviceRef": device_ref,
                    "capabilityId": capability_id,
                    "status": status,
                    "value": value
                });
                insert_optional_string(&mut body, "controlId", control_id);
                insert_optional_string(&mut body, "stateType", state_type);
                insert_optional_string(&mut body, "error", error);
                body
            }
            Self::CommandAccepted {
                device_ref,
                control_id,
                capability_id,
                command_type,
                accepted_at,
            } => {
                let mut body = json!({
                    "deviceRef": device_ref,
                    "capabilityId": capability_id,
                    "commandType": command_type
                });
                insert_optional_string(&mut body, "controlId", control_id);
                insert_optional_string(&mut body, "acceptedAt", accepted_at);
                body
            }
            Self::CommandRejected {
                device_ref,
                control_id,
                capability_id,
                command_type,
                reason,
                message,
            } => {
                let mut body = json!({
                    "deviceRef": device_ref,
                    "capabilityId": capability_id,
                    "commandType": command_type,
                    "reason": reason
                });
                insert_optional_string(&mut body, "controlId", control_id);
                insert_optional_string(&mut body, "message", message);
                body
            }
            Self::CommandReply {
                device_ref,
                control_id,
                capability_id,
                command_type,
                result,
            } => {
                let mut body = json!({
                    "deviceRef": device_ref,
                    "capabilityId": capability_id,
                    "commandType": command_type,
                    "result": result
                });
                insert_optional_string(&mut body, "controlId", control_id);
                body
            }
        })
    }

    pub fn from_message(message_type: &str, body: &Value) -> Result<Self> {
        let parsed: Self = match message_type {
            "controlInput" => serde_json::from_value::<ControlInputBody>(body.clone())?.into(),
            "controlCommand" => serde_json::from_value::<ControlCommandBody>(body.clone())?.into(),
            "capabilityStateChanged" => {
                serde_json::from_value::<CapabilityStateChangedBody>(body.clone())?.into()
            }
            "capabilityStateRequest" => {
                serde_json::from_value::<CapabilityStateRequestBody>(body.clone())?.into()
            }
            "capabilityStateReply" => {
                serde_json::from_value::<CapabilityStateReplyBody>(body.clone())?.into()
            }
            "commandAccepted" => {
                serde_json::from_value::<CommandAcceptedBody>(body.clone())?.into()
            }
            "commandRejected" => {
                serde_json::from_value::<CommandRejectedBody>(body.clone())?.into()
            }
            "commandReply" => serde_json::from_value::<CommandReplyBody>(body.clone())?.into(),
            other => {
                return Err(Error::Invalid(format!(
                    "unknown hardware message type {other}"
                )))
            }
        };
        parsed.validate()?;
        Ok(parsed)
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::ControlInput {
                device_ref,
                control_id,
                capability_id,
                event_type,
                occurred_at,
                sources,
                ..
            } => {
                device_ref.validate()?;
                require_non_empty(control_id, "control input target")?;
                require_non_empty(capability_id, "control input target")?;
                require_non_empty(event_type, "control input target")?;
                validate_optional_datetime(occurred_at.as_deref(), "control input occurredAt")?;
                for source in sources {
                    source.validate()?;
                }
                Ok(())
            }
            Self::ControlCommand {
                device_ref,
                control_id,
                capability_id,
                command_type,
                ..
            } => {
                device_ref.validate()?;
                if let Some(control_id) = control_id {
                    require_non_empty(control_id, "control command target")?;
                }
                require_non_empty(capability_id, "control command target")?;
                require_non_empty(command_type, "control command target")
            }
            Self::CapabilityStateChanged {
                device_ref,
                control_id,
                capability_id,
                state_type,
                occurred_at,
                ..
            } => validate_capability_state_fields(
                device_ref,
                control_id.as_deref(),
                capability_id,
                state_type.as_deref(),
                occurred_at.as_deref(),
            ),
            Self::CapabilityStateRequest {
                device_ref,
                control_id,
                capability_id,
                state_type,
                ..
            } => validate_capability_state_fields(
                device_ref,
                control_id.as_deref(),
                capability_id,
                state_type.as_deref(),
                None,
            ),
            Self::CapabilityStateReply {
                device_ref,
                control_id,
                capability_id,
                state_type,
                status,
                error,
                ..
            } => {
                validate_capability_state_fields(
                    device_ref,
                    control_id.as_deref(),
                    capability_id,
                    state_type.as_deref(),
                    None,
                )?;
                if !matches!(
                    status.as_str(),
                    "ok" | "unavailable" | "unsupported" | "rejected"
                ) {
                    return Err(Error::Invalid(
                        "capability state reply status must be ok, unavailable, unsupported, or rejected".to_string(),
                    ));
                }
                if let Some(error) = error {
                    require_non_empty(error, "capability state reply error")?;
                }
                Ok(())
            }
            Self::CommandAccepted {
                device_ref,
                control_id,
                capability_id,
                command_type,
                accepted_at,
            } => validate_command_status_fields(
                device_ref,
                control_id.as_deref(),
                capability_id,
                command_type,
                accepted_at.as_deref(),
            ),
            Self::CommandRejected {
                device_ref,
                control_id,
                capability_id,
                command_type,
                reason,
                message,
            } => {
                validate_command_status_fields(
                    device_ref,
                    control_id.as_deref(),
                    capability_id,
                    command_type,
                    None,
                )?;
                if !matches!(
                    reason.as_str(),
                    "malformed" | "unsupported" | "expired" | "unauthorized" | "stale" | "rejected"
                ) {
                    return Err(Error::Invalid(
                        "command rejection reason must be a standard hardware reason".to_string(),
                    ));
                }
                if let Some(message) = message {
                    require_non_empty(message, "command rejection message")?;
                }
                Ok(())
            }
            Self::CommandReply {
                device_ref,
                control_id,
                capability_id,
                command_type,
                ..
            } => validate_command_status_fields(
                device_ref,
                control_id.as_deref(),
                capability_id,
                command_type,
                None,
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ControlInputBody {
    device_ref: DeviceRef,
    control_id: String,
    capability_id: String,
    event_type: String,
    #[serde(default, deserialize_with = "crate::json::deserialize_optional_value")]
    value: Option<Value>,
    #[serde(default)]
    occurred_at: Option<String>,
    #[serde(default)]
    sequence: Option<u64>,
    #[serde(default)]
    sources: Vec<DeviceSourceReference>,
}

impl From<ControlInputBody> for HardwareMessageBody {
    fn from(body: ControlInputBody) -> Self {
        Self::ControlInput {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            event_type: body.event_type,
            value: body.value,
            occurred_at: body.occurred_at,
            sequence: body.sequence,
            sources: body.sources,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ControlCommandBody {
    device_ref: DeviceRef,
    control_id: Option<String>,
    capability_id: String,
    command_type: String,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    params: serde_json::Map<String, Value>,
}

impl From<ControlCommandBody> for HardwareMessageBody {
    fn from(body: ControlCommandBody) -> Self {
        Self::ControlCommand {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            command_type: body.command_type,
            params: body.params,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStateChangedBody {
    device_ref: DeviceRef,
    control_id: Option<String>,
    capability_id: String,
    state_type: Option<String>,
    #[serde(default, deserialize_with = "crate::json::deserialize_optional_value")]
    value: Option<Value>,
    occurred_at: Option<String>,
    sequence: Option<u64>,
}

impl From<CapabilityStateChangedBody> for HardwareMessageBody {
    fn from(body: CapabilityStateChangedBody) -> Self {
        Self::CapabilityStateChanged {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            state_type: body.state_type,
            value: body.value,
            occurred_at: body.occurred_at,
            sequence: body.sequence,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStateRequestBody {
    device_ref: DeviceRef,
    control_id: Option<String>,
    capability_id: String,
    state_type: Option<String>,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    params: serde_json::Map<String, Value>,
}

impl From<CapabilityStateRequestBody> for HardwareMessageBody {
    fn from(body: CapabilityStateRequestBody) -> Self {
        Self::CapabilityStateRequest {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            state_type: body.state_type,
            params: body.params,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStateReplyBody {
    device_ref: DeviceRef,
    control_id: Option<String>,
    capability_id: String,
    state_type: Option<String>,
    #[serde(default = "default_ok_status")]
    status: String,
    #[serde(default, deserialize_with = "crate::json::deserialize_optional_value")]
    value: Option<Value>,
    error: Option<String>,
}

impl From<CapabilityStateReplyBody> for HardwareMessageBody {
    fn from(body: CapabilityStateReplyBody) -> Self {
        Self::CapabilityStateReply {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            state_type: body.state_type,
            status: body.status,
            value: body.value,
            error: body.error,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommandAcceptedBody {
    device_ref: DeviceRef,
    control_id: Option<String>,
    capability_id: String,
    command_type: String,
    accepted_at: Option<String>,
}

impl From<CommandAcceptedBody> for HardwareMessageBody {
    fn from(body: CommandAcceptedBody) -> Self {
        Self::CommandAccepted {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            command_type: body.command_type,
            accepted_at: body.accepted_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommandRejectedBody {
    device_ref: DeviceRef,
    control_id: Option<String>,
    capability_id: String,
    command_type: String,
    reason: String,
    message: Option<String>,
}

impl From<CommandRejectedBody> for HardwareMessageBody {
    fn from(body: CommandRejectedBody) -> Self {
        Self::CommandRejected {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            command_type: body.command_type,
            reason: body.reason,
            message: body.message,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommandReplyBody {
    device_ref: DeviceRef,
    control_id: Option<String>,
    capability_id: String,
    command_type: String,
    #[serde(default, deserialize_with = "crate::json::deserialize_optional_value")]
    result: Option<Value>,
}

impl From<CommandReplyBody> for HardwareMessageBody {
    fn from(body: CommandReplyBody) -> Self {
        Self::CommandReply {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            command_type: body.command_type,
            result: body.result,
        }
    }
}

fn default_ok_status() -> String {
    "ok".to_string()
}

fn insert_optional_string(value: &mut Value, key: &str, field: &Option<String>) {
    if let Some(field) = field {
        value[key] = json!(field);
    }
}

fn insert_optional_u64(value: &mut Value, key: &str, field: Option<u64>) {
    if let Some(field) = field {
        value[key] = json!(field);
    }
}

fn validate_optional_datetime(value: Option<&str>, field_name: &str) -> Result<()> {
    if let Some(value) = value {
        require_non_empty(value, field_name)?;
        parse_datetime(value)
            .ok_or_else(|| Error::Invalid(format!("{field_name} must be an RFC 3339 datetime")))?;
    }
    Ok(())
}

fn validate_capability_state_fields(
    device_ref: &DeviceRef,
    control_id: Option<&str>,
    capability_id: &str,
    state_type: Option<&str>,
    occurred_at: Option<&str>,
) -> Result<()> {
    device_ref.validate()?;
    if let Some(control_id) = control_id {
        require_non_empty(control_id, "capability state target")?;
    }
    require_non_empty(capability_id, "capability state target")?;
    if let Some(state_type) = state_type {
        require_non_empty(state_type, "capability state type")?;
    }
    validate_optional_datetime(occurred_at, "capability state occurredAt")
}

fn validate_command_status_fields(
    device_ref: &DeviceRef,
    control_id: Option<&str>,
    capability_id: &str,
    command_type: &str,
    accepted_at: Option<&str>,
) -> Result<()> {
    device_ref.validate()?;
    if let Some(control_id) = control_id {
        require_non_empty(control_id, "command status target")?;
    }
    require_non_empty(capability_id, "command status target")?;
    require_non_empty(command_type, "command status target")?;
    validate_optional_datetime(accepted_at, "command acceptedAt")
}

pub fn load_fixture(path: &Path) -> Result<DeckrMessage> {
    DeckrMessage::from_text(
        &std::fs::read_to_string(path).map_err(|error| {
            Error::Invalid(format!("reading fixture {}: {error}", path.display()))
        })?,
    )
}

pub fn subject_for(message: &DeckrMessage) -> Result<String> {
    message.validate()?;
    match &message.recipient {
        MessageTarget::Endpoint { endpoint } => {
            direct_subject(&message.lane, &EndpointAddress::parse(endpoint)?)
        }
        MessageTarget::Broadcast {
            scope,
            endpoint_family,
            ..
        } => broadcast_subject(&message.lane, scope, endpoint_family),
    }
}

pub fn direct_subject(lane: &str, recipient: &EndpointAddress) -> Result<String> {
    require_non_empty(lane, "lane")?;
    Ok(format!(
        "{LANE_SUBJECT_PREFIX}.{}.to.{}.{}",
        encode_key_token(lane),
        encode_key_token(recipient.family()),
        encode_key_token(recipient.endpoint_id())
    ))
}

pub fn broadcast_subject(lane: &str, scope: &str, endpoint_family: &str) -> Result<String> {
    require_non_empty(lane, "lane")?;
    require_non_empty(scope, "broadcast scope")?;
    validate_endpoint_family(endpoint_family, "broadcast endpoint family")?;
    Ok(format!(
        "{LANE_SUBJECT_PREFIX}.{}.broadcast.{}.{}",
        encode_key_token(lane),
        encode_key_token(scope),
        encode_key_token(endpoint_family)
    ))
}

pub fn endpoint_direct_subscription_subject(
    lane: &str,
    endpoint: &EndpointAddress,
) -> Result<String> {
    direct_subject(lane, endpoint)
}

pub fn endpoint_broadcast_subscription_subject(
    lane: &str,
    endpoint: &EndpointAddress,
) -> Result<String> {
    require_non_empty(lane, "lane")?;
    Ok(format!(
        "{LANE_SUBJECT_PREFIX}.{}.broadcast.*.{}",
        encode_key_token(lane),
        encode_key_token(endpoint.family())
    ))
}

pub fn endpoint_subscription_subjects(
    lane: &str,
    endpoint: &EndpointAddress,
) -> Result<[String; 2]> {
    Ok([
        endpoint_direct_subscription_subject(lane, endpoint)?,
        endpoint_broadcast_subscription_subject(lane, endpoint)?,
    ])
}

pub fn headers_for(message: &DeckrMessage) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    headers.insert("Deckr-Message-Id".to_string(), message.message_id.clone());
    headers.insert(
        "Deckr-Message-Type".to_string(),
        message.message_type.clone(),
    );
    headers.insert("Deckr-Sender".to_string(), message.sender.clone());
    headers.insert(
        "Deckr-Sender-Session".to_string(),
        message.sender_session_id.clone(),
    );
    headers.insert("Deckr-Recipient".to_string(), recipient_header(message));
    if let Some(recipient_session_id) = &message.recipient_session_id {
        headers.insert(
            "Deckr-Recipient-Session".to_string(),
            recipient_session_id.clone(),
        );
    }
    if let Some(contract) = &message.contract {
        headers.insert(
            "Deckr-Contract-Id".to_string(),
            contract.contract_id.clone(),
        );
        headers.insert(
            "Deckr-Contract-Generation".to_string(),
            contract.generation.to_string(),
        );
    }
    if let Some(in_reply_to) = &message.in_reply_to {
        headers.insert("Deckr-In-Reply-To".to_string(), in_reply_to.clone());
    }
    headers
}

pub fn validate_headers(headers: &BTreeMap<String, String>, message: &DeckrMessage) -> Result<()> {
    message.validate()?;
    let contract_id = headers.get("Deckr-Contract-Id");
    let contract_generation = headers.get("Deckr-Contract-Generation");
    if contract_id.is_some() != contract_generation.is_some() {
        return Err(Error::Invalid(
            "NATS contract headers must be provided together".to_string(),
        ));
    }
    if message.contract.is_none() && contract_id.is_some() {
        return Err(Error::Invalid(
            "NATS contract headers disagree with Deckr envelope".to_string(),
        ));
    }
    let expected = headers_for(message);
    for key in ROUTING_HEADERS {
        if let Some(actual) = headers.get(*key) {
            if expected.get(*key) != Some(actual) {
                return Err(Error::Invalid(format!(
                    "NATS header {key} disagrees with Deckr envelope"
                )));
            }
        }
    }
    Ok(())
}

pub(crate) const ROUTING_HEADERS: &[&str] = &[
    "Deckr-Message-Id",
    "Deckr-Message-Type",
    "Deckr-Sender",
    "Deckr-Sender-Session",
    "Deckr-Recipient",
    "Deckr-Recipient-Session",
    "Deckr-Contract-Id",
    "Deckr-Contract-Generation",
    "Deckr-In-Reply-To",
];

pub fn validate_subject_hint(subject: &str, message: &DeckrMessage) -> Result<()> {
    message.validate()?;
    if !subject.starts_with(&format!("{LANE_SUBJECT_PREFIX}.")) {
        return Ok(());
    }
    let expected = subject_for(message)?;
    if subject != expected {
        return Err(Error::Invalid(
            "NATS subject disagrees with Deckr envelope recipient".to_string(),
        ));
    }
    Ok(())
}

pub fn message_targets_endpoint(
    message: &DeckrMessage,
    endpoint: &EndpointAddress,
) -> Result<bool> {
    message.validate()?;
    message.recipient.targets_endpoint(endpoint)
}

pub fn message_is_deliverable_to(
    message: &DeckrMessage,
    endpoint: &EndpointAddress,
    endpoint_session_id: &str,
) -> Result<bool> {
    message.validate()?;
    require_non_empty(endpoint_session_id, "endpoint session id")?;
    if message.is_expired() {
        return Ok(false);
    }
    if message
        .recipient_session_id
        .as_deref()
        .is_some_and(|session_id| session_id != endpoint_session_id)
    {
        return Ok(false);
    }
    message_targets_endpoint(message, endpoint)
}

fn recipient_header(message: &DeckrMessage) -> String {
    match &message.recipient {
        MessageTarget::Endpoint { endpoint } => endpoint.clone(),
        MessageTarget::Broadcast {
            scope,
            endpoint_family,
            ..
        } => format!("broadcast:{scope}:{endpoint_family}"),
    }
}

fn require_non_empty(value: &str, field_name: &str) -> Result<()> {
    if value.trim() != value || value.is_empty() {
        return Err(Error::Invalid(format!(
            "{field_name} must be non-empty with no leading or trailing whitespace"
        )));
    }
    Ok(())
}

fn validate_endpoint_family(value: &str, field_name: &str) -> Result<()> {
    require_non_empty(value, field_name)?;
    if matches!(
        value,
        ACTION_PROVIDER_FAMILY | CONTROLLER_FAMILY | HARDWARE_MANAGER_FAMILY | SERVICE_FAMILY
    ) {
        return Ok(());
    }
    Err(Error::Invalid(format!(
        "{field_name} must be a Deckr endpoint family"
    )))
}

fn require_not_endpoint_address(value: &str, field_name: &str) -> Result<()> {
    require_non_empty(value, field_name)?;
    if value.starts_with("action_provider:")
        || value.starts_with("controller:")
        || value.starts_with("hardware_manager:")
    {
        return Err(Error::Invalid(format!(
            "{field_name} must not be a Deckr endpoint address"
        )));
    }
    Ok(())
}

pub(crate) fn require_contract_token(value: &str, field_name: &str) -> Result<()> {
    require_non_empty(value, field_name)?;
    let mut segments = value.split('.');
    let Some(first) = segments.next() else {
        return Err(Error::Invalid(format!(
            "{field_name} must be a lowercase contract identifier"
        )));
    };
    if !segment_starts_with_lowercase(first) || !contract_segment_tail_valid(first) {
        return Err(Error::Invalid(format!(
            "{field_name} must be a lowercase contract identifier"
        )));
    }
    for segment in segments {
        if !segment_starts_with_lowercase_or_digit(segment) || !contract_segment_tail_valid(segment)
        {
            return Err(Error::Invalid(format!(
                "{field_name} must be a lowercase contract identifier"
            )));
        }
    }
    Ok(())
}

pub(crate) fn require_globally_qualified_name(value: &str, field_name: &str) -> Result<()> {
    require_contract_token(value, field_name)?;
    if !value.contains('.') {
        return Err(Error::Invalid(format!(
            "{field_name} must be globally namespaced"
        )));
    }
    Ok(())
}

fn segment_starts_with_lowercase(segment: &str) -> bool {
    segment
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_lowercase())
}

fn segment_starts_with_lowercase_or_digit(segment: &str) -> bool {
    segment
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

fn contract_segment_tail_valid(segment: &str) -> bool {
    !segment.is_empty()
        && segment.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
}

fn require_finite(value: f64, field_name: &str) -> Result<()> {
    if !value.is_finite() {
        return Err(Error::Invalid(format!("{field_name} must be finite")));
    }
    Ok(())
}

fn require_optional_positive_finite(value: Option<f64>, field_name: &str) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    require_finite(value, field_name)?;
    if value <= 0.0 {
        return Err(Error::Invalid(format!("{field_name} must be positive")));
    }
    Ok(())
}

fn require_normalized(value: f64, field_name: &str) -> Result<()> {
    require_finite(value, field_name)?;
    if !(0.0..=1.0).contains(&value) {
        return Err(Error::Invalid(format!(
            "{field_name} must be between 0 and 1"
        )));
    }
    Ok(())
}

fn require_unique(values: &[String], field_name: &str) -> Result<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(Error::Invalid(format!("{field_name} must be unique")));
        }
    }
    Ok(())
}

fn validate_contract_token_list(values: &[String], field_name: &str) -> Result<()> {
    for value in values {
        require_contract_token(value, field_name)?;
    }
    require_unique(values, "event or command types")
}

pub(crate) fn validate_core_capability_family(family: &str) -> Result<()> {
    if family.starts_with("dev.deckr.")
        && !matches!(
            family,
            "dev.deckr.device.power"
                | "dev.deckr.input.button"
                | "dev.deckr.input.encoder"
                | "dev.deckr.input.touch"
                | "dev.deckr.output.raster"
        )
    {
        return Err(Error::Invalid(format!(
            "unsupported Deckr core capability family: {family}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_capability_direction(direction: &str) -> Result<()> {
    if !matches!(direction, "input" | "output" | "state" | "command") {
        return Err(Error::Invalid(
            "capability direction must be input, output, state, or command".to_string(),
        ));
    }
    Ok(())
}

fn validate_access(direction: &str, access: &[String]) -> Result<()> {
    if access.is_empty() {
        return Err(Error::Invalid(
            "capability access must not be empty".to_string(),
        ));
    }
    require_unique(access, "capability access")?;
    for value in access {
        if !matches!(
            value.as_str(),
            "emits" | "readable" | "settable" | "requestable" | "invokable"
        ) {
            return Err(Error::Invalid(format!(
                "unsupported capability access: {value}"
            )));
        }
    }
    let allowed = match direction {
        "input" => ["emits"].as_slice(),
        "output" => ["settable", "invokable"].as_slice(),
        "state" => ["readable", "requestable", "settable", "emits"].as_slice(),
        "command" => ["invokable"].as_slice(),
        _ => {
            return Err(Error::Invalid(
                "unsupported capability direction".to_string(),
            ))
        }
    };
    if !access
        .iter()
        .any(|value| allowed.iter().any(|allowed| value == allowed))
    {
        return Err(Error::Invalid(format!(
            "{direction} capability access does not include a supported access"
        )));
    }
    Ok(())
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_descriptor_validation_rejects_legacy_geometry_unit() {
        let mut descriptor = sample_descriptor();
        descriptor.controls[0].geometry.as_mut().unwrap().unit = "relative".to_string();

        let error = descriptor.validate().unwrap_err().to_string();

        assert!(error.contains("geometry unit"));
    }

    #[test]
    fn device_descriptor_validation_rejects_legacy_constraint_subject() {
        let mut descriptor = sample_descriptor();
        descriptor.controls[0].output_capabilities[0].constraints[0].subject =
            "channelOrder".to_string();

        let error = descriptor.validate().unwrap_err().to_string();

        assert!(error.contains("lowercase contract identifier"));
    }

    fn sample_descriptor() -> DeviceDescriptor {
        DeviceDescriptor {
            device_id: "fip".to_string(),
            fingerprint: "fingerprint".to_string(),
            display_name: "Flight Instrument Panel".to_string(),
            manufacturer: Some("Logitech".to_string()),
            model: Some("Flight Instrument Panel".to_string()),
            serial_number: None,
            controls: vec![ControlDescriptor {
                control_id: "screen".to_string(),
                kind: "screen".to_string(),
                label: Some("Screen".to_string()),
                geometry: Some(ControlGeometry {
                    x: 0.0,
                    y: 0.0,
                    width: Some(4.0),
                    height: Some(3.0),
                    unit: "grid".to_string(),
                    ..Default::default()
                }),
                input_capabilities: Vec::new(),
                output_capabilities: vec![CapabilityDescriptor {
                    capability_id: "raster.bitmap".to_string(),
                    family: "dev.deckr.output.raster".to_string(),
                    capability_type: "bitmap".to_string(),
                    direction: "output".to_string(),
                    access: vec!["settable".to_string()],
                    value_schema: None,
                    command_schema: None,
                    constraints: vec![CapabilityConstraint {
                        constraint_type: "fixed".to_string(),
                        subject: "channel_order".to_string(),
                        value: Some(json!("bgr")),
                        ..Default::default()
                    }],
                    event_types: Vec::new(),
                    command_types: vec!["set_frame".to_string(), "clear".to_string()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            capabilities: Vec::new(),
            ..Default::default()
        }
    }
}
