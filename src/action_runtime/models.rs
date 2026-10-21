use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::*;
use crate::endpoint::EndpointAddress;
use crate::lanes::{ControlRef, DeviceRef};
use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityView {
    Raw,
    Native,
    Projected,
    Derived,
    Extension,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityProvenance {
    #[default]
    Native,
    Projection,
    Derivation,
    Extension,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionAvailabilityStatus {
    Available,
    Unavailable,
    Probing,
}

/// A source reference may omit its device to use the enclosing device. An absent
/// control selects a device-level capability.
/// Matched capabilities and inputs/outputs require both through `fully_qualified`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityRef {
    pub capability_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_ref: Option<DeviceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_id: Option<String>,
}
impl ActionModel for CapabilityRef {
    fn validate(&self) -> Result<()> {
        token(&self.capability_id)?;
        if let Some(device) = &self.device_ref {
            device.validate()?;
        }
        optional_text(&self.control_id)
    }
}
impl CapabilityRef {
    pub fn fully_qualified(&self) -> Result<()> {
        self.validate()?;
        if self.device_ref.is_none() || self.control_id.is_none() {
            return Err(Error::Invalid(
                "action capability requires deviceRef and controlId".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityRequirementSelector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub capability_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(default)]
    pub event_types: Vec<String>,
    #[serde(default)]
    pub command_types: Vec<String>,
}
impl ActionModel for CapabilityRequirementSelector {
    fn validate(&self) -> Result<()> {
        if let Some(value) = &self.capability_id {
            token(value)?;
        }
        if let Some(value) = &self.family {
            family(value)?;
        }
        if let Some(value) = &self.capability_type {
            token(value)?;
        }
        if let Some(value) = &self.direction {
            crate::lanes::validate_capability_direction(value)?;
        }
        tokens(&self.event_types)?;
        tokens(&self.command_types)?;
        if self.capability_id.is_none()
            && self.family.is_none()
            && self.capability_type.is_none()
            && self.direction.is_none()
            && self.event_types.is_empty()
            && self.command_types.is_empty()
        {
            return Err(Error::Invalid(
                "capability selector must constrain at least one field".into(),
            ));
        }
        Ok(())
    }
}
fn required() -> bool {
    true
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityRequirement {
    pub name: String,
    pub preferences: Vec<CapabilityRequirementSelector>,
    #[serde(default = "required")]
    pub required: bool,
    #[serde(default)]
    pub event_types: Vec<String>,
    #[serde(default)]
    pub command_types: Vec<String>,
    #[serde(default)]
    pub views: Vec<CapabilityView>,
}
impl ActionModel for CapabilityRequirement {
    fn validate(&self) -> Result<()> {
        text(&self.name)?;
        if self.preferences.is_empty() {
            return Err(Error::Invalid(
                "capability requirement needs a preference".into(),
            ));
        }
        for selector in &self.preferences {
            selector.validate()?;
        }
        if self
            .views
            .iter()
            .enumerate()
            .any(|(index, view)| self.views[..index].contains(view))
        {
            return Err(Error::Invalid("capability views must be unique".into()));
        }
        tokens(&self.event_types)?;
        tokens(&self.command_types)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MatchedCapability {
    pub capability: CapabilityRef,
    pub family: String,
    #[serde(rename = "type")]
    pub capability_type: String,
    pub direction: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirement_name: Option<String>,
    #[serde(default)]
    pub event_types: Vec<String>,
    #[serde(default)]
    pub command_types: Vec<String>,
    #[serde(default)]
    pub provenance: CapabilityProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<CapabilityRef>,
}
impl ActionModel for MatchedCapability {
    fn validate(&self) -> Result<()> {
        self.capability.fully_qualified()?;
        family(&self.family)?;
        token(&self.capability_type)?;
        crate::lanes::validate_capability_direction(&self.direction)?;
        text(self.requirement_name.as_deref().ok_or_else(|| {
            Error::Invalid("matched capability requires requirementName".into())
        })?)?;
        tokens(&self.event_types)?;
        tokens(&self.command_types)?;
        if let Some(source) = &self.source {
            source.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionDescriptor {
    pub action_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controllers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub property_inspector_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirements: Option<Vec<CapabilityRequirement>>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_object",
        skip_serializing_if = "Option::is_none"
    )]
    pub manifest_defaults: Option<Map<String, Value>>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_object",
        skip_serializing_if = "Option::is_none"
    )]
    pub settings_schema: Option<Map<String, Value>>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_object",
        skip_serializing_if = "Option::is_none"
    )]
    pub provider_settings_schema: Option<Map<String, Value>>,
}
impl ActionModel for ActionDescriptor {
    fn validate(&self) -> Result<()> {
        text(&self.action_id)?;
        optional_text(&self.provider_id)?;
        let mut names = BTreeSet::new();
        for requirement in self.requirements.iter().flatten() {
            requirement.validate()?;
            if !names.insert(&requirement.name) {
                return Err(Error::Invalid(
                    "action requirement names must be unique".into(),
                ));
            }
        }
        for object in [
            &self.manifest_defaults,
            &self.settings_schema,
            &self.provider_settings_schema,
        ]
        .into_iter()
        .flatten()
        {
            crate::json::validate_numbers(&Value::Object(object.clone()))?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionAvailabilityEntry {
    pub action_id: String,
    pub status: ActionAvailabilityStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor: Option<ActionDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
impl ActionModel for ActionAvailabilityEntry {
    fn validate(&self) -> Result<()> {
        text(&self.action_id)?;
        optional_text(&self.reason)?;
        if let Some(descriptor) = &self.descriptor {
            descriptor.validate()?;
            if descriptor.action_id != self.action_id {
                return Err(Error::Invalid(
                    "availability descriptor must name its entry's action".into(),
                ));
            }
        } else if self.status == ActionAvailabilityStatus::Available {
            return Err(Error::Invalid(
                "available action requires a descriptor".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionAvailabilitySelector {
    pub action_id: String,
}
impl ActionModel for ActionAvailabilitySelector {
    fn validate(&self) -> Result<()> {
        text(&self.action_id)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionAvailabilityPayload {
    pub provider_instance_id: String,
    pub provider_id: String,
    pub service_id: String,
    pub service_endpoint: EndpointAddress,
    pub service_session_id: String,
    #[serde(default)]
    pub entries: Vec<ActionAvailabilityEntry>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    pub annotations: Map<String, Value>,
    /// The upstream payload permits owner-defined additional fields.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
impl ActionModel for ActionAvailabilityPayload {
    fn from_value(value: Value) -> Result<Self> {
        crate::json::validate_numbers(&value)?;
        let Value::Object(mut object) = value else {
            return Err(Error::Invalid("availability must be an object".into()));
        };
        let mut known = Map::new();
        for key in [
            "providerInstanceId",
            "providerId",
            "serviceId",
            "serviceEndpoint",
            "serviceSessionId",
            "entries",
            "labels",
            "annotations",
        ] {
            if let Some(value) = object.remove(key) {
                known.insert(key.into(), value);
            }
        }
        let mut payload: Self = serde_json::from_str(&Value::Object(known).to_string())?;
        // Preserve extension objects verbatim, including keys resembling Serde's
        // private arbitrary-precision marker, instead of reinterpreting them.
        payload.extra = object.into_iter().collect();
        payload.validate()?;
        Ok(payload)
    }
    fn validate(&self) -> Result<()> {
        text(&self.provider_id)?;
        text(&self.service_session_id)?;
        if self.service_id != action_runtime_service_id(&self.provider_instance_id)?
            || self.service_endpoint != action_runtime_endpoint(&self.provider_instance_id)?
        {
            return Err(Error::Invalid(
                "availability identity differs from its provider instance".into(),
            ));
        }
        for entry in &self.entries {
            entry.validate()?;
        }
        for (key, value) in &self.labels {
            text(key)?;
            text(value)?;
        }
        crate::json::validate_numbers(&Value::Object(self.annotations.clone()))?;
        for (key, value) in &self.extra {
            if matches!(
                key.as_str(),
                "providerInstanceId"
                    | "providerId"
                    | "serviceId"
                    | "serviceEndpoint"
                    | "serviceSessionId"
                    | "entries"
                    | "labels"
                    | "annotations"
            ) {
                return Err(Error::Invalid(
                    "availability extension duplicates a declared field".into(),
                ));
            }
            crate::json::validate_numbers(value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionInstanceMetadata {
    pub provider_instance_id: String,
    pub provider_id: String,
    pub action_id: String,
    pub action_instance_id: String,
    pub config_id: String,
    pub context_id: String,
}
impl ActionModel for ActionInstanceMetadata {
    fn validate(&self) -> Result<()> {
        validate_provider_instance_id(&self.provider_instance_id)?;
        for value in [
            &self.provider_id,
            &self.action_id,
            &self.action_instance_id,
            &self.config_id,
            &self.context_id,
        ] {
            text(value)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingMetadata {
    pub provider_instance_id: String,
    pub provider_id: String,
    pub action_id: String,
    pub action_instance_id: String,
    pub config_id: String,
    pub context_id: String,
    pub binding_id: String,
    pub device_ref: DeviceRef,
    pub control_ref: ControlRef,
    #[serde(default)]
    pub matched_capabilities: Vec<MatchedCapability>,
    #[serde(default)]
    pub output_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handler: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_key: Option<String>,
}
impl BindingMetadata {
    pub fn action_instance(&self) -> ActionInstanceMetadata {
        ActionInstanceMetadata {
            provider_instance_id: self.provider_instance_id.clone(),
            provider_id: self.provider_id.clone(),
            action_id: self.action_id.clone(),
            action_instance_id: self.action_instance_id.clone(),
            config_id: self.config_id.clone(),
            context_id: self.context_id.clone(),
        }
    }
}
impl ActionModel for BindingMetadata {
    fn validate(&self) -> Result<()> {
        self.action_instance().validate()?;
        text(&self.binding_id)?;
        self.device_ref.validate()?;
        self.control_ref.validate()?;
        for capability in &self.matched_capabilities {
            capability.validate()?;
        }
        for value in [&self.page_session_id, &self.handler, &self.item_key] {
            optional_text(value)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageSessionMetadata {
    pub provider_instance_id: String,
    pub provider_id: String,
    pub action_instance_id: String,
    pub config_id: String,
    pub context_id: String,
    pub page_id: String,
    pub page_session_id: String,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_binding_id: Option<String>,
    #[serde(default)]
    pub bindings: Vec<BindingMetadata>,
}
impl ActionModel for PageSessionMetadata {
    fn validate(&self) -> Result<()> {
        validate_provider_instance_id(&self.provider_instance_id)?;
        for value in [
            &self.provider_id,
            &self.action_instance_id,
            &self.config_id,
            &self.context_id,
            &self.page_id,
            &self.page_session_id,
        ] {
            text(value)?;
        }
        optional_text(&self.owner_binding_id)?;
        for binding in &self.bindings {
            binding.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionInstanceLifecycle {
    pub metadata: ActionInstanceMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
impl ActionModel for ActionInstanceLifecycle {
    fn validate(&self) -> Result<()> {
        self.metadata.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageSessionLifecycle {
    pub page_session: PageSessionMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
impl ActionModel for PageSessionLifecycle {
    fn validate(&self) -> Result<()> {
        self.page_session.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingAttached {
    pub binding: BindingMetadata,
    #[serde(
        default,
        deserialize_with = "super::rendering::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub rendering: Option<RenderingSupport>,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    pub settings: Map<String, Value>,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    pub internal: Map<String, Value>,
}
impl ActionModel for BindingAttached {
    fn validate(&self) -> Result<()> {
        self.binding.validate()?;
        if let Some(support) = &self.rendering {
            support.validate()?;
        }
        crate::json::validate_numbers(&Value::Object(self.settings.clone()))?;
        crate::json::validate_numbers(&Value::Object(self.internal.clone()))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingDetached {
    pub binding: BindingMetadata,
    pub reason: String,
}
impl ActionModel for BindingDetached {
    fn validate(&self) -> Result<()> {
        self.binding.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleTargetKind {
    ActionInstance,
    Binding,
    PageSession,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleRejectionReason {
    ActionNotAvailable,
    ProviderNotReady,
    InvalidSettings,
    UnsupportedCapability,
    ResourceUnavailable,
    PermissionDenied,
    StaleLifecycle,
    InternalError,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionLifecycleRejected {
    pub target_kind: LifecycleTargetKind,
    pub reason: LifecycleRejectionReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_instance: Option<ActionInstanceMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<BindingMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_session: Option<PageSessionMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    pub details: Map<String, Value>,
}
impl ActionModel for ActionLifecycleRejected {
    fn validate(&self) -> Result<()> {
        optional_text(&self.message)?;
        match (
            self.target_kind,
            &self.action_instance,
            &self.binding,
            &self.page_session,
        ) {
            (LifecycleTargetKind::ActionInstance, Some(action), None, None) => action.validate()?,
            (LifecycleTargetKind::Binding, None, Some(binding), None) => binding.validate()?,
            (LifecycleTargetKind::PageSession, None, None, Some(page)) => page.validate()?,
            _ => {
                return Err(Error::Invalid(
                    "lifecycle rejection requires exactly its declared target".into(),
                ))
            }
        }
        crate::json::validate_numbers(&Value::Object(self.details.clone()))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityInputEvent {
    pub capability: CapabilityRef,
    pub event_type: String,
    pub occurred_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<CapabilityRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<CapabilityView>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub value: Option<Value>,
}
impl ActionModel for CapabilityInputEvent {
    fn validate(&self) -> Result<()> {
        self.capability.fully_qualified()?;
        token(&self.event_type)?;
        chrono::DateTime::parse_from_rfc3339(&self.occurred_at)
            .map_err(|_| Error::Invalid("capability input requires a date-time".into()))?;
        optional_text(&self.producer)?;
        if let Some(source) = &self.source {
            source.validate()?;
        }
        if let Some(value) = &self.value {
            crate::json::validate_numbers(value)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityInput {
    pub binding: BindingMetadata,
    pub event: CapabilityInputEvent,
}
impl ActionModel for CapabilityInput {
    fn validate(&self) -> Result<()> {
        self.binding.validate()?;
        self.event.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingOutput {
    pub binding: BindingMetadata,
    pub capability: CapabilityRef,
    pub command_type: String,
    pub generation: u64,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    pub params: Map<String, Value>,
}
impl ActionModel for BindingOutput {
    fn validate(&self) -> Result<()> {
        self.binding.validate()?;
        self.capability.fully_qualified()?;
        token(&self.command_type)?;
        crate::json::validate_numbers(&Value::Object(self.params.clone()))
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingOverlay {
    pub binding: BindingMetadata,
    pub template: String,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    pub params: Map<String, Value>,
}
impl ActionModel for BindingOverlay {
    fn validate(&self) -> Result<()> {
        self.binding.validate()?;
        token(&self.template)?;
        optional_text(&self.overlay_id)?;
        optional_text(&self.title)?;
        if self
            .duration_seconds
            .is_some_and(|duration| !duration.is_finite() || duration <= 0.0)
        {
            return Err(Error::Invalid(
                "overlay duration must be finite and positive".into(),
            ));
        }
        crate::json::validate_numbers(&Value::Object(self.params.clone()))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingOverlayClear {
    pub binding: BindingMetadata,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_id: Option<String>,
}
impl ActionModel for BindingOverlayClear {
    fn validate(&self) -> Result<()> {
        self.binding.validate()?;
        optional_text(&self.overlay_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageChildTargetKind {
    #[serde(rename = "self")]
    SelfAction,
    Action,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageChildBindingTarget {
    pub kind: PageChildTargetKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_key: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_object",
        skip_serializing_if = "Option::is_none"
    )]
    pub provider_labels: Option<Map<String, Value>>,
}
impl ActionModel for PageChildBindingTarget {
    fn validate(&self) -> Result<()> {
        optional_text(&self.action_id)?;
        optional_text(&self.instance_key)?;
        if let Some(id) = &self.provider_instance_id {
            validate_provider_instance_id(id)?;
        }
        if let Some(labels) = &self.provider_labels {
            for key in labels.keys() {
                text(key)?;
            }
            crate::json::validate_numbers(&Value::Object(labels.clone()))?;
        }
        match self.kind {
            PageChildTargetKind::SelfAction
                if self.action_id.is_some()
                    || self.provider_instance_id.is_some()
                    || self.instance_key.is_some()
                    || self
                        .provider_labels
                        .as_ref()
                        .is_some_and(|labels| !labels.is_empty()) =>
            {
                Err(Error::Invalid(
                    "self page target cannot select another action or provider".into(),
                ))
            }
            PageChildTargetKind::Action if self.action_id.is_none() => Err(Error::Invalid(
                "action page target requires an actionId".into(),
            )),
            _ => Ok(()),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageChildBindingDescriptor {
    pub control_id: String,
    pub target: PageChildBindingTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handler: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_key: Option<String>,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    pub settings: Map<String, Value>,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    pub internal: Map<String, Value>,
}
impl ActionModel for PageChildBindingDescriptor {
    fn validate(&self) -> Result<()> {
        text(&self.control_id)?;
        self.target.validate()?;
        optional_text(&self.handler)?;
        optional_text(&self.item_key)?;
        crate::json::validate_numbers(&Value::Object(self.settings.clone()))?;
        crate::json::validate_numbers(&Value::Object(self.internal.clone()))
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DynamicPageCommand {
    pub page_id: String,
    pub bindings: Vec<PageChildBindingDescriptor>,
}
impl ActionModel for DynamicPageCommand {
    fn validate(&self) -> Result<()> {
        text(&self.page_id)?;
        if self.bindings.is_empty() {
            return Err(Error::Invalid(
                "dynamic page requires child bindings".into(),
            ));
        }
        let mut controls = BTreeSet::new();
        for binding in &self.bindings {
            binding.validate()?;
            if !controls.insert(&binding.control_id) {
                return Err(Error::Invalid(
                    "dynamic page control IDs must be unique".into(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DynamicPageBody {
    pub target: PageCommandTarget,
    pub descriptor: DynamicPageCommand,
}
impl ActionModel for DynamicPageBody {
    fn validate(&self) -> Result<()> {
        self.target.validate()?;
        self.descriptor.validate()
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyActionBody {}
impl ActionModel for EmptyActionBody {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingsScope {
    ActionProviderInstance,
    ActionInstance,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingsProvenance {
    ConfigDefault,
    UserOverride,
    TemplateOverride,
    Runtime,
    StaleSchema,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SettingsTargetRef {
    pub scope: SettingsScope,
    pub controller_id: String,
    pub config_id: String,
    pub provider_instance_id: String,
    pub provider_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_id: Option<String>,
}
impl ActionModel for SettingsTargetRef {
    fn validate(&self) -> Result<()> {
        validate_provider_instance_id(&self.provider_instance_id)?;
        for value in [&self.controller_id, &self.config_id, &self.provider_id] {
            text(value)?;
        }
        for value in [&self.action_id, &self.action_instance_id, &self.stable_id] {
            optional_text(value)?;
        }
        match self.scope {
            SettingsScope::ActionProviderInstance
                if self.action_id.is_some()
                    || self.action_instance_id.is_some()
                    || self.stable_id.is_some() =>
            {
                Err(Error::Invalid(
                    "provider settings cannot carry action identity".into(),
                ))
            }
            SettingsScope::ActionInstance
                if self.action_id.is_none() || self.action_instance_id.is_none() =>
            {
                Err(Error::Invalid(
                    "instance settings require action and action-instance IDs".into(),
                ))
            }
            _ => Ok(()),
        }
    }
}
impl SettingsTargetRef {
    pub fn key(&self) -> Result<String> {
        self.validate()?;
        let mut parts = vec![
            "settings".into(),
            "target".into(),
            match self.scope {
                SettingsScope::ActionProviderInstance => "action_provider_instance",
                SettingsScope::ActionInstance => "action_instance",
            }
            .into(),
        ];
        for value in [
            &self.controller_id,
            &self.config_id,
            &self.provider_instance_id,
            &self.provider_id,
        ] {
            parts.push(crate::keys::encode_key_token(value));
        }
        if self.scope == SettingsScope::ActionInstance {
            for value in [&self.action_id, &self.action_instance_id] {
                parts.push(crate::keys::encode_key_token(
                    value.as_ref().expect("validated instance identity"),
                ));
            }
            parts.push(if self.stable_id.is_some() { "1" } else { "0" }.into());
            if let Some(value) = &self.stable_id {
                parts.push(crate::keys::encode_key_token(value));
            }
        }
        Ok(parts.join("."))
    }
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SettingsSchemaMetadata {
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_object",
        skip_serializing_if = "Option::is_none"
    )]
    pub schema: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
    #[serde(default)]
    pub stale: bool,
}
impl ActionModel for SettingsSchemaMetadata {
    fn validate(&self) -> Result<()> {
        if let Some(schema) = &self.schema {
            crate::json::validate_numbers(&Value::Object(schema.clone()))?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SettingsTargetDescription {
    pub target: SettingsTargetRef,
    pub provider_instance_id: String,
    pub provider_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, deserialize_with = "crate::json::deserialize_object")]
    pub placement: Map<String, Value>,
    #[serde(default)]
    pub schema_metadata: SettingsSchemaMetadata,
    #[serde(default)]
    pub provenance: Vec<SettingsProvenance>,
}
impl ActionModel for SettingsTargetDescription {
    fn validate(&self) -> Result<()> {
        self.target.validate()?;
        optional_text(&self.label)?;
        if self.provider_instance_id != self.target.provider_instance_id
            || self.provider_id != self.target.provider_id
            || self.action_id != self.target.action_id
        {
            return Err(Error::Invalid(
                "settings description must mirror its target identity".into(),
            ));
        }
        self.schema_metadata.validate()?;
        crate::json::validate_numbers(&Value::Object(self.placement.clone()))
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SettingsSnapshot {
    pub target: SettingsTargetRef,
    #[serde(deserialize_with = "crate::json::deserialize_object")]
    pub settings: Map<String, Value>,
    #[serde(default)]
    pub schema_metadata: SettingsSchemaMetadata,
    #[serde(default)]
    pub provenance: Vec<SettingsProvenance>,
}
impl ActionModel for SettingsSnapshot {
    fn validate(&self) -> Result<()> {
        self.target.validate()?;
        self.schema_metadata.validate()?;
        crate::json::validate_numbers(&Value::Object(self.settings.clone()))
    }
}

/// A target in the authorized controller session, with an independent generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum PageCommandTarget {
    #[serde(rename = "binding")]
    Binding {
        #[serde(rename = "bindingId")]
        binding_id: String,
        generation: u64,
    },
    #[serde(rename = "page_session")]
    PageSession {
        #[serde(rename = "pageSessionId")]
        page_session_id: String,
        generation: u64,
    },
}
impl ActionModel for PageCommandTarget {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Binding { binding_id, .. } => text(binding_id),
            Self::PageSession {
                page_session_id, ..
            } => text(page_session_id),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosePageBody {
    pub target: PageCommandTarget,
}
impl ActionModel for ClosePageBody {
    fn validate(&self) -> Result<()> {
        self.target.validate()?;
        if !matches!(self.target, PageCommandTarget::PageSession { .. }) {
            return Err(Error::Invalid(
                "close_page requires a page-session target".into(),
            ));
        }
        Ok(())
    }
}
