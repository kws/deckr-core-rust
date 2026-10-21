//! Rendering wire values and admission checks; content execution belongs to controllers.
use super::*;
use crate::authority::ContractPointer;
use crate::lanes::DeckrMessage;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const RENDER_TEXT_FORMAT: &str = "dev.deckr.render.text.v1";
pub const RENDER_IMAGE_FORMAT: &str = "dev.deckr.render.image.v1";

pub(super) fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}
fn present_object<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Map<String, Value>>, D::Error> {
    crate::json::deserialize_object(deserializer).map(Some)
}
fn nonempty(value: &str) -> Result<()> {
    if value.is_empty() {
        Err(Error::Invalid("rendering requires nonempty text".into()))
    } else {
        Ok(())
    }
}
fn positive(value: u64) -> Result<()> {
    if value == 0 {
        Err(Error::Invalid(
            "rendering requires a positive integer".into(),
        ))
    } else {
        Ok(())
    }
}
fn color(value: &str) -> Result<()> {
    if value.len() != 7
        || !value.starts_with('#')
        || !value.as_bytes()[1..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(Error::Invalid("invalid rendering color".into()));
    }
    Ok(())
}
fn format_id(value: &str) -> Result<()> {
    if matches!(value, RENDER_TEXT_FORMAT | RENDER_IMAGE_FORMAT) {
        return Ok(());
    }
    let valid = !value.starts_with("dev.deckr.")
        && value.rsplit_once(".v").is_some_and(|(name, version)| {
            !version.is_empty()
                && !version.starts_with('0')
                && version.bytes().all(|c| c.is_ascii_digit())
                && name.split('.').count() >= 2
                && name.split('.').enumerate().all(|(index, segment)| {
                    segment.as_bytes().first().is_some_and(|c| {
                        c.is_ascii_lowercase() || (index > 0 && c.is_ascii_digit())
                    }) && segment.bytes().all(|c| {
                        c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'_' | b'-')
                    })
                })
        });
    if valid {
        Ok(())
    } else {
        Err(Error::Invalid("invalid rendering format ID".into()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RenderLimits {
    pub max_message_bytes: u64,
    pub max_image_bytes: u64,
    pub max_image_pixels: u64,
    pub max_text_code_points: u64,
    pub max_render_time_ms: u64,
}
impl ActionModel for RenderLimits {
    fn validate(&self) -> Result<()> {
        for value in [
            self.max_message_bytes,
            self.max_image_bytes,
            self.max_image_pixels,
            self.max_text_code_points,
            self.max_render_time_ms,
        ] {
            positive(value)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderText {
    pub text: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub foreground: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub background: Option<String>,
}
impl ActionModel for RenderText {
    fn validate(&self) -> Result<()> {
        for value in [&self.foreground, &self.background].into_iter().flatten() {
            color(value)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RenderImageEncoding {
    Png,
    Jpeg,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RenderImageFit {
    Contain,
    Cover,
    Stretch,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderImage {
    pub image: String,
    pub encoding: RenderImageEncoding,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub fit: Option<RenderImageFit>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub background: Option<String>,
}
impl ActionModel for RenderImage {
    fn validate(&self) -> Result<()> {
        nonempty(&self.image)?;
        if let Some(value) = &self.background {
            color(value)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderFormat {
    pub id: String,
    #[serde(
        default,
        deserialize_with = "present_object",
        skip_serializing_if = "Option::is_none"
    )]
    pub options: Option<Map<String, Value>>,
}
impl ActionModel for RenderFormat {
    fn validate(&self) -> Result<()> {
        format_id(&self.id)?;
        if let Some(options) = &self.options {
            crate::json::validate_numbers(&Value::Object(options.clone()))?;
        }
        match self.id.as_str() {
            RENDER_TEXT_FORMAT if self.options.is_some() => {
                return Err(Error::Invalid("text format has no options".into()))
            }
            RENDER_IMAGE_FORMAT => {
                let encodings = self
                    .options
                    .as_ref()
                    .filter(|o| o.len() == 1)
                    .and_then(|o| o.get("encodings"))
                    .and_then(Value::as_array)
                    .ok_or_else(|| Error::Invalid("image format requires encodings".into()))?;
                let names: BTreeSet<_> = encodings.iter().filter_map(Value::as_str).collect();
                if names.len() != encodings.len()
                    || !names.contains("png")
                    || names.iter().any(|n| !matches!(*n, "png" | "jpeg"))
                {
                    return Err(Error::Invalid(
                        "image format requires unique PNG and optional JPEG encodings".into(),
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RenderTargetSupport {
    pub requirement_name: String,
    pub width: u64,
    pub height: u64,
    pub formats: Vec<RenderFormat>,
    pub limits: RenderLimits,
}
impl ActionModel for RenderTargetSupport {
    fn validate(&self) -> Result<()> {
        nonempty(&self.requirement_name)?;
        positive(self.width)?;
        positive(self.height)?;
        self.limits.validate()?;
        let mut ids = BTreeSet::new();
        for format in &self.formats {
            format.validate()?;
            if !ids.insert(format.id.as_str()) {
                return Err(Error::Invalid("duplicate rendering format".into()));
            }
        }
        if !ids.contains(RENDER_TEXT_FORMAT) || !ids.contains(RENDER_IMAGE_FORMAT) {
            return Err(Error::Invalid(
                "render target requires both baseline formats".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderingSupport {
    pub version: u64,
    pub targets: Vec<RenderTargetSupport>,
}
impl ActionModel for RenderingSupport {
    fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(Error::Invalid("unsupported rendering version".into()));
        }
        let mut names = BTreeSet::new();
        for target in &self.targets {
            target.validate()?;
            if !names.insert(&target.requirement_name) {
                return Err(Error::Invalid("duplicate render target".into()));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderContent {
    pub format: String,
    #[serde(deserialize_with = "crate::json::deserialize_object")]
    pub data: Map<String, Value>,
}
impl ActionModel for RenderContent {
    fn validate(&self) -> Result<()> {
        format_id(&self.format)?;
        let value = Value::Object(self.data.clone());
        crate::json::validate_numbers(&value)?;
        match self.format.as_str() {
            RENDER_TEXT_FORMAT => {
                RenderText::from_value(value)?;
            }
            RENDER_IMAGE_FORMAT => {
                RenderImage::from_value(value)?;
            }
            _ => {}
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RenderOperation {
    Set,
    Clear,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingRenderBody {
    pub target: PageCommandTarget,
    pub requirement_name: String,
    pub operation: RenderOperation,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub content: Option<RenderContent>,
}
fn binding_target(target: &PageCommandTarget) -> Result<(&str, u64)> {
    target.validate()?;
    match target {
        PageCommandTarget::Binding {
            binding_id,
            generation,
        } => Ok((binding_id, *generation)),
        _ => Err(Error::Invalid("rendering requires a binding target".into())),
    }
}
impl ActionModel for BindingRenderBody {
    fn validate(&self) -> Result<()> {
        binding_target(&self.target)?;
        nonempty(&self.requirement_name)?;
        if (self.operation == RenderOperation::Set) != self.content.is_some() {
            return Err(Error::Invalid(
                "set requires content; clear prohibits content".into(),
            ));
        }
        if let Some(content) = &self.content {
            content.validate()?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RenderResultStage {
    Controller,
    Hardware,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RenderResultStatus {
    Dispatched,
    Rejected,
    Failed,
    Superseded,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingRenderResultBody {
    pub request_message_id: String,
    pub target: PageCommandTarget,
    pub requirement_name: String,
    pub stage: RenderResultStage,
    pub status: RenderResultStatus,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub command_message_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub code: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub reason: Option<String>,
}
impl ActionModel for BindingRenderResultBody {
    fn validate(&self) -> Result<()> {
        nonempty(&self.request_message_id)?;
        binding_target(&self.target)?;
        nonempty(&self.requirement_name)?;
        use RenderResultStage::{Controller, Hardware};
        use RenderResultStatus::{Dispatched, Failed, Rejected, Superseded};
        let codes: &[&str] = match (self.stage, self.status) {
            (Controller, Dispatched) => &[],
            (Controller, Rejected) => &[
                "invalid_content",
                "unsupported_format",
                "unsupported_target",
                "target_not_current",
                "limit_exceeded",
                "busy",
            ],
            (Controller, Failed) => &[
                "invalid_content",
                "limit_exceeded",
                "render_failed",
                "deadline_exceeded",
                "dispatch_failed",
            ],
            (Controller, Superseded) => &["newer_output", "target_not_current"],
            (Hardware, Rejected) => &["hardware_rejected"],
            _ => {
                return Err(Error::Invalid(
                    "invalid render result stage and status".into(),
                ))
            }
        };
        if if codes.is_empty() {
            self.code.is_some()
        } else {
            !self.code.as_deref().is_some_and(|c| codes.contains(&c))
        } {
            return Err(Error::Invalid("invalid render result code".into()));
        }
        if (self.status == Dispatched || self.stage == Hardware)
            != self.command_message_id.is_some()
        {
            return Err(Error::Invalid(
                "invalid hardware command correlation".into(),
            ));
        }
        if let Some(id) = &self.command_message_id {
            nonempty(id)?;
        }
        if self
            .reason
            .as_ref()
            .is_some_and(|s| s.chars().count() > 512)
        {
            return Err(Error::Invalid(
                "render reason exceeds 512 code points".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error(transparent)]
    Invalid(#[from] Error),
    #[error("unauthorized")]
    Unauthorized,
    #[error("target_not_current")]
    TargetNotCurrent,
    #[error("unsupported_target")]
    UnsupportedTarget,
    #[error("unsupported_format")]
    UnsupportedFormat,
    #[error("limit_exceeded")]
    LimitExceeded,
}
impl RenderError {
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Invalid(_) => None,
            Self::Unauthorized => Some("unauthorized"),
            Self::TargetNotCurrent => Some("target_not_current"),
            Self::UnsupportedTarget => Some("unsupported_target"),
            Self::UnsupportedFormat => Some("unsupported_format"),
            Self::LimitExceeded => Some("limit_exceeded"),
        }
    }
}
impl From<RenderError> for Error {
    fn from(value: RenderError) -> Self {
        match value {
            RenderError::Invalid(error) => error,
            RenderError::Unauthorized => Error::Unauthorized(value.to_string()),
            _ => Error::Invalid(value.to_string()),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RenderBindingState {
    pub binding: BindingMetadata,
    #[serde(default)]
    pub rendering: Option<RenderingSupport>,
    pub active: bool,
    pub contract: ContractPointer,
    pub runtime_endpoint: String,
    pub runtime_session_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RenderCommandContext {
    pub contract: ContractPointer,
    pub controller_endpoint: String,
    pub controller_session_id: String,
    pub runtime_endpoint: String,
    pub runtime_session_id: String,
    pub local_authority: bool,
    pub hardware_authority: bool,
    #[serde(default)]
    pub bindings: BTreeMap<String, RenderBindingState>,
    pub message_bytes: u64,
}
pub fn render_match<'a>(
    binding: &'a BindingMetadata,
    name: &str,
) -> std::result::Result<&'a MatchedCapability, RenderError> {
    let mut matches = binding
        .matched_capabilities
        .iter()
        .filter(|m| m.requirement_name.as_deref() == Some(name));
    let matched = matches.next().ok_or(RenderError::UnsupportedTarget)?;
    if matches.next().is_some()
        || matched.family != "dev.deckr.output.raster"
        || matched.capability_type != "bitmap"
        || matched.direction != "output"
        || !["set_frame", "clear"]
            .iter()
            .all(|name| matched.command_types.iter().any(|command| command == name))
        || matched.capability.device_ref.as_ref() != Some(&binding.device_ref)
        || matched.capability.control_id.as_deref() != Some(&binding.control_ref.control_id)
    {
        return Err(RenderError::UnsupportedTarget);
    }
    Ok(matched)
}
pub fn validate_rendering_support(
    binding: &BindingMetadata,
    support: &RenderingSupport,
) -> std::result::Result<(), RenderError> {
    binding.validate()?;
    support.validate()?;
    let mut aliases: BTreeMap<&str, &RenderTargetSupport> = BTreeMap::new();
    for target in &support.targets {
        let matched = render_match(binding, &target.requirement_name)?;
        if let Some(previous) = aliases.insert(&matched.capability.capability_id, target) {
            if previous.width != target.width
                || previous.height != target.height
                || previous.formats != target.formats
                || previous.limits != target.limits
            {
                return Err(RenderError::UnsupportedTarget);
            }
        }
    }
    Ok(())
}
/// Return retained admission facts without acquiring authority. Recheck at final dispatch.
pub fn validate_render_command<'a>(
    message: &DeckrMessage,
    context: &'a RenderCommandContext,
) -> std::result::Result<(&'a RenderBindingState, &'a RenderTargetSupport), RenderError> {
    let ActionMessage::BindingRender(body) = ActionMessage::from_envelope(message)? else {
        return Err(Error::Invalid("message is not binding_render".into()).into());
    };
    if !context.local_authority
        || message.contract.as_ref() != Some(&context.contract)
        || message.sender != context.runtime_endpoint
        || message.sender_session_id != context.runtime_session_id
        || message.recipient_endpoint() != Some(context.controller_endpoint.as_str())
        || message.recipient_session_id.as_deref() != Some(context.controller_session_id.as_str())
    {
        return Err(RenderError::Unauthorized);
    }
    let (id, generation) = binding_target(&body.target)?;
    let current = context
        .bindings
        .get(id)
        .ok_or(RenderError::TargetNotCurrent)?;
    if !current.active
        || !context.hardware_authority
        || current.binding.binding_id != id
        || current.binding.output_generation != generation
        || current.contract != context.contract
        || current.runtime_endpoint != context.runtime_endpoint
        || current.runtime_session_id != context.runtime_session_id
    {
        return Err(RenderError::TargetNotCurrent);
    }
    let support = current
        .rendering
        .as_ref()
        .ok_or(RenderError::UnsupportedTarget)?;
    validate_rendering_support(&current.binding, support)?;
    let target = support
        .targets
        .iter()
        .find(|t| t.requirement_name == body.requirement_name)
        .ok_or(RenderError::UnsupportedTarget)?;
    if context.message_bytes > target.limits.max_message_bytes {
        return Err(RenderError::LimitExceeded);
    }
    if let Some(content) = &body.content {
        let format = target
            .formats
            .iter()
            .find(|f| f.id == content.format)
            .ok_or(RenderError::UnsupportedFormat)?;
        if content.format == RENDER_IMAGE_FORMAT
            && !format
                .options
                .as_ref()
                .and_then(|o| o.get("encodings"))
                .and_then(Value::as_array)
                .is_some_and(|e| e.contains(&content.data["encoding"]))
        {
            return Err(RenderError::UnsupportedFormat);
        }
        if content.format == RENDER_TEXT_FORMAT
            && content.data["text"].as_str().unwrap().chars().count() as u64
                > target.limits.max_text_code_points
        {
            return Err(RenderError::LimitExceeded);
        }
    }
    Ok((current, target))
}
