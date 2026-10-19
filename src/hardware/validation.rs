//! Descriptor-scoped hardware operation validation, independent of drivers.

use std::collections::BTreeMap;
use std::io::Cursor;

use base64::Engine;
use num_bigint::BigInt;
use num_rational::BigRational;
use serde_json::{Map, Value};

use crate::lanes::{CapabilityDescriptor, CapabilitySchema, DeviceDescriptor, HardwareMessageBody};
use crate::schema::{CompiledSchema, SchemaRegistry};
use crate::{Error, Result};

#[derive(Debug, Clone)]
enum PayloadSchema {
    Embedded(CompiledSchema),
    Core(CorePayload),
}

#[derive(Debug, Clone, Copy)]
enum CorePayload {
    Activation,
    Momentary,
    Encoder,
    Touch,
    Raster,
    Frame,
    Empty,
}

impl PayloadSchema {
    fn compile(schema: &CapabilitySchema, registry: &SchemaRegistry) -> Result<Self> {
        // Recognized pure references use the corresponding Rust implementation
        // of the upstream contract. Compound schemas still require resolution
        // through the host's registry, including every additional constraint.
        if let Some(object) = schema.schema.as_object() {
            if object.keys().all(|key| {
                matches!(
                    key.as_str(),
                    "$ref" | "$id" | "$schema" | "title" | "description" | "$comment"
                )
            }) {
                if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                    if let Some(name) = reference.strip_prefix("https://kws.github.io/deckr/contract/v1/schemas/deckr/hardware/capabilities/") {
                        let core = match name {
                            "button-activation-input-value.v1.schema.json" => Some(CorePayload::Activation),
                            "button-momentary-input-value.v1.schema.json" => Some(CorePayload::Momentary),
                            "encoder-relative-input-value.v1.schema.json" => Some(CorePayload::Encoder),
                            "touch-gesture-input-value.v1.schema.json" => Some(CorePayload::Touch),
                            "raster-bitmap-command-params.v1.schema.json" => Some(CorePayload::Raster),
                            "raster-bitmap-set-frame-params.v1.schema.json" => Some(CorePayload::Frame),
                            "raster-bitmap-clear-params.v1.schema.json" | "device-power-command-params.v1.schema.json" => Some(CorePayload::Empty),
                            _ => None,
                        };
                        if let Some(core) = core { return Ok(Self::Core(core)); }
                    }
                }
            }
        }
        registry.compile(&schema.schema).map(Self::Embedded)
    }
    fn validate(&self, value: &Value) -> Result<()> {
        match self {
            Self::Embedded(schema) => schema.validate(value),
            Self::Core(core) => core.validate(value),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct CapabilityValidators {
    value: Option<PayloadSchema>,
    command: Option<PayloadSchema>,
}

/// A stable descriptor and its compiled embedded schemas. Keep one per
/// negotiated device; descriptor replacement creates a new instance.
#[derive(Debug, Clone)]
pub struct ValidatedDevice {
    descriptor: DeviceDescriptor,
    schemas: BTreeMap<(Option<String>, String), CapabilityValidators>,
}

impl ValidatedDevice {
    pub fn new(descriptor: DeviceDescriptor, registry: &SchemaRegistry) -> Result<Self> {
        descriptor.validate()?;
        let mut schemas = BTreeMap::new();
        for (control_id, capability) in descriptor.capabilities.iter().map(|c| (None, c)).chain(
            descriptor.controls.iter().flat_map(|control| {
                control
                    .capabilities()
                    .map(move |c| (Some(control.control_id.clone()), c))
            }),
        ) {
            schemas.insert(
                (control_id, capability.capability_id.clone()),
                CapabilityValidators {
                    value: capability
                        .value_schema
                        .as_ref()
                        .map(|s| PayloadSchema::compile(s, registry))
                        .transpose()?,
                    command: capability
                        .command_schema
                        .as_ref()
                        .map(|s| PayloadSchema::compile(s, registry))
                        .transpose()?,
                },
            );
        }
        Ok(Self {
            descriptor,
            schemas,
        })
    }

    pub fn descriptor(&self) -> &DeviceDescriptor {
        &self.descriptor
    }

    /// Validate resource identity, operation support and payload before a
    /// managed boundary checks the exact agreement and dispatches the work.
    pub fn validate_message(&self, message: &HardwareMessageBody) -> Result<()> {
        message.validate()?;
        let reference = message.device_ref();
        if reference.device_id != self.descriptor.device_id
            || reference
                .fingerprint
                .as_ref()
                .is_some_and(|f| f != &self.descriptor.fingerprint)
        {
            return Err(Error::Unauthorized(
                "hardware message references a different device or fingerprint".into(),
            ));
        }
        let capability_id = message
            .capability_id()
            .expect("hardware bodies have a capability id");
        let capability = self
            .descriptor
            .capability(message.control_id(), capability_id)
            .ok_or_else(|| {
                Error::Unsupported("hardware capability or control is not in the descriptor".into())
            })?;
        let schemas = &self.schemas[&(
            message.control_id().map(str::to_string),
            capability_id.to_string(),
        )];
        let has_access = |access: &str| capability.access.iter().any(|a| a == access);
        match message {
            HardwareMessageBody::ControlInput {
                event_type,
                value,
                sources,
                ..
            } => {
                supported(
                    capability.direction == "input"
                        && has_access("emits")
                        && capability.event_types.contains(event_type),
                )?;
                for source in sources {
                    if source.connection_id.as_ref().is_some_and(|id| {
                        !self
                            .descriptor
                            .connections
                            .iter()
                            .any(|c| &c.connection_id == id)
                    }) {
                        return Err(Error::Invalid(
                            "input source references an unknown connection".into(),
                        ));
                    }
                }
                let value = value.as_ref().unwrap_or(&Value::Null);
                validate_core_input(capability, event_type, value)?;
                validate_optional(&schemas.value, value)?;
                validate_constraints(capability, value, &BTreeMap::new())
            }
            HardwareMessageBody::ControlCommand {
                command_type,
                params,
                ..
            } => {
                supported(
                    matches!(capability.direction.as_str(), "output" | "command")
                        && (has_access("settable") || has_access("invokable"))
                        && capability.command_types.contains(command_type),
                )?;
                let value = Value::Object(params.clone());
                let dimensions = match capability.family.as_str() {
                    "dev.deckr.output.raster" if command_type == "set_frame" => {
                        CorePayload::Frame.validate(&value)?;
                        let (width, height) = validate_raster(params)?;
                        BTreeMap::from([
                            ("width", Value::from(width)),
                            ("height", Value::from(height)),
                        ])
                    }
                    "dev.deckr.output.raster" | "dev.deckr.device.power" => {
                        CorePayload::Empty.validate(&value)?;
                        BTreeMap::new()
                    }
                    _ => BTreeMap::new(),
                };
                validate_optional(&schemas.command, &value)?;
                validate_constraints(capability, &value, &dimensions)
            }
            HardwareMessageBody::CapabilityStateRequest { .. } => {
                supported(has_access("readable") || has_access("requestable"))
            }
            HardwareMessageBody::CapabilityStateChanged { value, .. } => {
                supported(has_access("emits") && capability.direction == "state")?;
                let value = value.as_ref().unwrap_or(&Value::Null);
                validate_optional(&schemas.value, value)?;
                validate_constraints(capability, value, &BTreeMap::new())
            }
            HardwareMessageBody::CapabilityStateReply { status, value, .. } => {
                supported(has_access("readable") || has_access("requestable"))?;
                if status == "ok" {
                    let value = value.as_ref().unwrap_or(&Value::Null);
                    validate_optional(&schemas.value, value)?;
                    validate_constraints(capability, value, &BTreeMap::new())?;
                }
                Ok(())
            }
            HardwareMessageBody::CommandAccepted { command_type, .. }
            | HardwareMessageBody::CommandRejected { command_type, .. }
            | HardwareMessageBody::CommandReply { command_type, .. } => {
                supported(capability.command_types.contains(command_type))
            }
        }
    }
}

/// Check response identity before applying descriptor-specific payload rules.
/// Correlation to the envelope's message ID and sessions belongs to messaging.
pub fn validate_reply(request: &HardwareMessageBody, reply: &HardwareMessageBody) -> Result<()> {
    if request.device_ref().manager_id != reply.device_ref().manager_id
        || request.device_ref().device_id != reply.device_ref().device_id
        || request
            .device_ref()
            .fingerprint
            .as_ref()
            .zip(reply.device_ref().fingerprint.as_ref())
            .is_some_and(|(expected, actual)| expected != actual)
        || request.control_id() != reply.control_id()
        || request.capability_id() != reply.capability_id()
    {
        return Err(Error::Unauthorized(
            "hardware reply changes the requested resource".into(),
        ));
    }
    let matches = match (request, reply) {
        (
            HardwareMessageBody::ControlCommand {
                command_type: expected,
                ..
            },
            HardwareMessageBody::CommandAccepted {
                command_type: actual,
                ..
            }
            | HardwareMessageBody::CommandRejected {
                command_type: actual,
                ..
            }
            | HardwareMessageBody::CommandReply {
                command_type: actual,
                ..
            },
        ) => expected == actual,
        (
            HardwareMessageBody::CapabilityStateRequest {
                state_type: expected,
                ..
            },
            HardwareMessageBody::CapabilityStateReply {
                state_type: actual, ..
            },
        ) => expected == actual,
        _ => false,
    };
    if !matches {
        return Err(Error::Invalid(
            "hardware reply does not match the requested operation".into(),
        ));
    }
    Ok(())
}

fn supported(condition: bool) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Unsupported(
            "operation is not supported by the selected capability".into(),
        ))
    }
}
fn validate_optional(schema: &Option<PayloadSchema>, value: &Value) -> Result<()> {
    if let Some(schema) = schema {
        schema.validate(value)?;
    }
    Ok(())
}
fn invalid() -> Error {
    Error::Invalid("value does not match the selected core capability contract".into())
}
fn object<'a>(value: &'a Value, keys: &[&str]) -> Result<&'a Map<String, Value>> {
    let object = value.as_object().ok_or_else(invalid)?;
    if object.keys().any(|key| !keys.contains(&key.as_str())) {
        return Err(invalid());
    }
    Ok(object)
}
impl CorePayload {
    fn validate(self, value: &Value) -> Result<()> {
        match self {
            Self::Activation | Self::Momentary => {
                let value = object(value, &["eventType"])?;
                let events = if matches!(self, Self::Activation) {
                    &["press"][..]
                } else {
                    &["down", "up"][..]
                };
                if !value
                    .get("eventType")
                    .and_then(Value::as_str)
                    .is_some_and(|event| events.contains(&event))
                {
                    return Err(invalid());
                }
            }
            Self::Encoder => {
                let value = object(value, &["delta", "direction"])?;
                let delta = value
                    .get("delta")
                    .and_then(decimal)
                    .filter(|n| n.is_integer() && n != &BigRational::from_integer(0.into()))
                    .ok_or_else(invalid)?;
                if let Some(direction) = value.get("direction").filter(|v| !v.is_null()) {
                    if direction.as_str()
                        != Some(if delta > BigRational::from_integer(0.into()) {
                            "clockwise"
                        } else {
                            "counterclockwise"
                        })
                    {
                        return Err(invalid());
                    }
                }
            }
            Self::Touch => {
                let value = object(value, &["eventType", "direction"])?;
                match value.get("eventType").and_then(Value::as_str) {
                    Some("tap") if value.get("direction").is_none_or(Value::is_null) => {}
                    Some("swipe")
                        if value
                            .get("direction")
                            .and_then(Value::as_str)
                            .is_some_and(|d| matches!(d, "left" | "right")) => {}
                    _ => return Err(invalid()),
                }
            }
            Self::Raster => {
                if value.as_object().is_some_and(Map::is_empty) {
                    return Ok(());
                }
                Self::Frame.validate(value)?;
            }
            Self::Frame => {
                let value = object(value, &["image", "encoding", "width", "height"])?;
                if !value
                    .get("image")
                    .and_then(Value::as_str)
                    .is_some_and(|v| !v.is_empty() && v.trim() == v)
                    || !value
                        .get("encoding")
                        .and_then(Value::as_str)
                        .is_some_and(|v| matches!(v, "png" | "jpeg"))
                {
                    return Err(invalid());
                }
                for name in ["width", "height"] {
                    if let Some(n) = value.get(name).filter(|v| !v.is_null()) {
                        if !decimal(n).is_some_and(|n| {
                            n.is_integer() && n > BigRational::from_integer(0.into())
                        }) {
                            return Err(invalid());
                        }
                    }
                }
            }
            Self::Empty => {
                object(value, &[])?;
            }
        }
        Ok(())
    }
}
fn validate_core_input(
    capability: &CapabilityDescriptor,
    event: &str,
    value: &Value,
) -> Result<()> {
    let core = match (
        capability.family.as_str(),
        capability.capability_type.as_str(),
    ) {
        ("dev.deckr.input.button", "activation") => Some(CorePayload::Activation),
        ("dev.deckr.input.button", "momentary") => Some(CorePayload::Momentary),
        ("dev.deckr.input.encoder", "relative") => Some(CorePayload::Encoder),
        ("dev.deckr.input.touch", "gesture") => Some(CorePayload::Touch),
        _ => None,
    };
    if let Some(core) = core {
        core.validate(value)?;
        if !matches!(core, CorePayload::Encoder)
            && value.get("eventType").and_then(Value::as_str) != Some(event)
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn validate_raster(params: &Map<String, Value>) -> Result<(u32, u32)> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(params["image"].as_str().ok_or_else(invalid)?)
        .map_err(|_| Error::Invalid("raster image is not valid base64".into()))?;
    let expected = match params["encoding"].as_str() {
        Some("png") => image::ImageFormat::Png,
        Some("jpeg") => image::ImageFormat::Jpeg,
        _ => return Err(invalid()),
    };
    let actual = image::guess_format(&bytes)
        .map_err(|_| Error::Invalid("unrecognized raster encoding".into()))?;
    if actual != expected {
        return Err(Error::Invalid(
            "raster encoding disagrees with image bytes".into(),
        ));
    }
    // Decode the complete image, not just an untrusted dimensions header.
    let image = image::ImageReader::with_format(Cursor::new(bytes), actual)
        .decode()
        .map_err(|error| Error::Invalid(format!("invalid raster image: {error}")))?;
    for (name, actual) in [("width", image.width()), ("height", image.height())] {
        if params
            .get(name)
            .filter(|v| !v.is_null())
            .is_some_and(|v| !values_equal(v, &Value::from(actual)))
        {
            return Err(Error::Invalid(format!(
                "declared raster {name} disagrees with image bytes"
            )));
        }
    }
    Ok((image.width(), image.height()))
}

fn validate_constraints(
    capability: &CapabilityDescriptor,
    value: &Value,
    derived: &BTreeMap<&str, Value>,
) -> Result<()> {
    for constraint in &capability.constraints {
        let selected = if constraint.subject == "value" {
            Some(value)
        } else {
            derived.get(constraint.subject.as_str()).or_else(|| {
                constraint
                    .subject
                    .split('.')
                    .try_fold(value, |value, field| value.get(field))
            })
        };
        let Some(selected) = selected.filter(|v| !v.is_null()) else {
            continue;
        };
        if constraint
            .value
            .as_ref()
            .filter(|v| !v.is_null())
            .is_some_and(|v| !values_equal(v, selected))
            || (!constraint.values.is_empty()
                && !constraint.values.iter().any(|v| values_equal(v, selected)))
        {
            return Err(Error::Invalid(format!(
                "capability constraint {} excludes the value",
                constraint.subject
            )));
        }
        if constraint.minimum.is_some() || constraint.maximum.is_some() || constraint.step.is_some()
        {
            let mut number = decimal(selected).ok_or_else(invalid)?;
            if let Some(unit) = capability
                .units
                .iter()
                .find(|unit| unit.subject == constraint.subject)
            {
                if constraint
                    .unit
                    .as_ref()
                    .is_some_and(|declared| declared != &unit.unit)
                {
                    return Err(Error::Unsupported(
                        "constraint requires an undeclared unit conversion".into(),
                    ));
                }
                number *= decimal(&Value::from(unit.scale)).ok_or_else(invalid)?;
            }
            let minimum = constraint
                .minimum
                .map(|v| decimal(&Value::from(v)).ok_or_else(invalid))
                .transpose()?;
            let maximum = constraint
                .maximum
                .map(|v| decimal(&Value::from(v)).ok_or_else(invalid))
                .transpose()?;
            let step = constraint
                .step
                .map(|v| decimal(&Value::from(v)).ok_or_else(invalid))
                .transpose()?;
            let off_step = step.is_some_and(|step| {
                !((&number
                    - minimum
                        .clone()
                        .unwrap_or_else(|| BigRational::from_integer(0.into())))
                    / step)
                    .is_integer()
            });
            if minimum.is_some_and(|min| number < min)
                || maximum.is_some_and(|max| number > max)
                || off_step
            {
                return Err(Error::Invalid(format!(
                    "capability constraint {} excludes the numeric value",
                    constraint.subject
                )));
            }
        }
    }
    Ok(())
}

// Preserve integer precision, decimal steps and scaled bounds instead of
// rounding both operands through binary64. Bound exponent expansion explicitly.
fn decimal(value: &Value) -> Option<BigRational> {
    let Value::Number(number) = value else {
        return None;
    };
    let text = number.to_string();
    let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((&text, "0"));
    let exponent: i32 = exponent.parse().ok()?;
    let fraction = mantissa.split_once('.').map_or(0, |(_, part)| part.len());
    let scale = exponent.checked_sub(i32::try_from(fraction).ok()?)?;
    if scale.unsigned_abs() > 10_000 {
        return None;
    }
    let numerator: BigInt = mantissa.replace('.', "").parse().ok()?;
    let power = BigInt::from(10).pow(scale.unsigned_abs());
    Some(if scale >= 0 {
        BigRational::from_integer(numerator * power)
    } else {
        BigRational::new(numerator, power)
    })
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(_), Value::Number(_)) => decimal(left)
            .zip(decimal(right))
            .is_some_and(|(a, b)| a == b),
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| values_equal(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, a)| b.get(key).is_some_and(|b| values_equal(a, b)))
        }
        _ => left == right,
    }
}
