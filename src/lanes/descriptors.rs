use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceRef {
    pub manager_id: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ControlGeometry {
    #[serde(default)]
    pub x: f64,
    #[serde(default)]
    pub y: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<f64>,
    #[serde(default = "grid_unit")]
    pub unit: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layer: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation: Option<i64>,
}

fn grid_unit() -> String {
    "grid".into()
}
impl Default for ControlGeometry {
    fn default() -> Self {
        Self {
            x: 0.0,
            y: 0.0,
            width: None,
            height: None,
            unit: grid_unit(),
            layer: None,
            rotation: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityConstraint {
    #[serde(rename = "type")]
    pub constraint_type: String,
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceIdentifier {
    #[serde(rename = "type")]
    pub identifier_type: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceConnection {
    pub connection_id: String,
    #[serde(rename = "type")]
    pub connection_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
    #[serde(default = "available_status")]
    pub status: String,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub facts: serde_json::Map<String, Value>,
}
fn available_status() -> String {
    "available".into()
}
impl Default for DeviceConnection {
    fn default() -> Self {
        Self {
            connection_id: String::new(),
            connection_type: String::new(),
            label: None,
            transport: None,
            status: available_status(),
            facts: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DescriptorCapabilityRef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_id: Option<String>,
    pub capability_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlRef {
    pub device_ref: DeviceRef,
    pub control_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityProjection {
    #[serde(rename = "type", default = "projection_type")]
    pub projection_type: String,
    pub owner: String,
    pub source: DescriptorCapabilityRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}
fn projection_type() -> String {
    "projection".into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityUnit {
    pub subject: String,
    pub unit: String,
    #[serde(default = "unit_scale")]
    pub scale: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}
fn unit_scale() -> f64 {
    1.0
}

impl DeviceIdentifier {
    pub fn validate(&self) -> Result<()> {
        require_contract_token(&self.identifier_type, "device identifier type")?;
        require_non_empty(&self.value, "device identifier value")?;
        optional_text(&self.namespace, "device identifier namespace")?;
        optional_text(&self.issuer, "device identifier issuer")
    }
}
impl DeviceConnection {
    pub fn validate(&self) -> Result<()> {
        require_not_endpoint_address(&self.connection_id, "connection id")?;
        require_contract_token(&self.connection_type, "connection type")?;
        optional_text(&self.label, "connection label")?;
        optional_text(&self.transport, "connection transport")?;
        if !matches!(
            self.status.as_str(),
            "connected" | "available" | "unavailable" | "unknown"
        ) {
            return Err(Error::Invalid("invalid connection status".into()));
        }
        Ok(())
    }
}
impl DescriptorCapabilityRef {
    pub fn validate(&self) -> Result<()> {
        optional_text(&self.control_id, "capability reference control id")?;
        require_contract_token(&self.capability_id, "capability reference id")
    }
}
impl ControlRef {
    pub fn validate(&self) -> Result<()> {
        self.device_ref.validate()?;
        require_non_empty(&self.control_id, "control reference id")
    }
}
impl CapabilityProjection {
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.projection_type.as_str(), "projection" | "derivation")
            || !matches!(
                self.owner.as_str(),
                "hardware_manager" | "adapter" | "component"
            )
        {
            return Err(Error::Invalid(
                "invalid capability projection type or owner".into(),
            ));
        }
        self.source.validate()?;
        optional_text(&self.description, "projection description")
    }
}
impl CapabilityUnit {
    pub fn validate(&self) -> Result<()> {
        require_contract_token(&self.subject, "unit subject")?;
        require_contract_token(&self.unit, "capability unit")?;
        if let Some(symbol) = &self.symbol {
            require_contract_token(symbol, "unit symbol")?;
        }
        if !self.scale.is_finite() || self.scale <= 0.0 {
            return Err(Error::Invalid(
                "unit scale must be positive and finite".into(),
            ));
        }
        Ok(())
    }
}
fn optional_text(value: &Option<String>, name: &str) -> Result<()> {
    if let Some(value) = value {
        require_non_empty(value, name)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilitySchema {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
    pub schema: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityDescriptor {
    pub capability_id: String,
    pub family: String,
    #[serde(rename = "type")]
    pub capability_type: String,
    pub direction: String,
    pub access: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_schema: Option<CapabilitySchema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_schema: Option<CapabilitySchema>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<CapabilityConstraint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub units: Vec<CapabilityUnit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<CapabilityProjection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<DeviceSourceReference>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlDescriptor {
    pub control_id: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geometry: Option<ControlGeometry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_capabilities: Vec<CapabilityDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_capabilities: Vec<CapabilityDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config_capabilities: Vec<CapabilityDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub state_capabilities: Vec<CapabilityDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostic_capabilities: Vec<CapabilityDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surface_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_control_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related_control_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<DeviceSourceReference>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceDescriptor {
    pub device_id: String,
    pub fingerprint: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manufacturer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial_number: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub controls: Vec<ControlDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<CapabilityDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hardware_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub firmware_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<DeviceRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_status_indicator: Option<DescriptorCapabilityRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identifiers: Vec<DeviceIdentifier>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<DeviceConnection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<DeviceSourceReference>,
}

impl DeviceRef {
    pub fn validate(&self) -> Result<()> {
        require_not_endpoint_address(&self.manager_id, "device reference")?;
        require_not_endpoint_address(&self.device_id, "device reference")?;
        if let Some(fingerprint) = &self.fingerprint {
            require_non_empty(fingerprint, "fingerprint")?;
        }
        Ok(())
    }
}

impl ControlGeometry {
    pub fn validate(&self) -> Result<()> {
        require_finite(self.x, "geometry value")?;
        require_finite(self.y, "geometry value")?;
        require_optional_positive_finite(self.width, "geometry width")?;
        require_optional_positive_finite(self.height, "geometry height")?;
        if !matches!(
            self.unit.as_str(),
            "grid" | "pixel" | "normalized" | "millimeter"
        ) {
            return Err(Error::Invalid(
                "geometry unit must be grid, pixel, normalized, or millimeter".to_string(),
            ));
        }
        if self.unit == "normalized" {
            require_normalized(self.x, "normalized geometry x")?;
            require_normalized(self.y, "normalized geometry y")?;
            if let Some(width) = self.width {
                require_normalized(width, "normalized geometry width")?;
            }
            if let Some(height) = self.height {
                require_normalized(height, "normalized geometry height")?;
            }
        }
        Ok(())
    }
}

impl CapabilitySchema {
    pub fn validate(&self) -> Result<()> {
        if let Some(schema_id) = &self.schema_id {
            require_globally_qualified_name(schema_id, "schema_id")?;
        }
        let Some(object) = self.schema.as_object() else {
            return Err(Error::Invalid(
                "capability schema must be a JSON object".to_string(),
            ));
        };
        if object.is_empty() {
            return Err(Error::Invalid(
                "capability schema must not be empty".to_string(),
            ));
        }
        if !JSON_SCHEMA_CONTRACT_KEYS
            .iter()
            .any(|key| object.contains_key(*key))
        {
            return Err(Error::Invalid(
                "capability schema must include a JSON Schema contract keyword".to_string(),
            ));
        }
        crate::schema::validate_schema(&self.schema)
    }
}

impl CapabilityConstraint {
    pub fn validate(&self) -> Result<()> {
        require_contract_token(&self.constraint_type, "constraint type")?;
        require_contract_token(&self.subject, "constraint subject")?;
        if let Some(unit) = &self.unit {
            require_contract_token(unit, "capability constraint unit")?;
        }
        if let Some(minimum) = self.minimum {
            require_finite(minimum, "capability constraint number")?;
        }
        if let Some(maximum) = self.maximum {
            require_finite(maximum, "capability constraint number")?;
        }
        if let Some(step) = self.step {
            require_finite(step, "capability constraint number")?;
            if step <= 0.0 {
                return Err(Error::Invalid(
                    "capability constraint step must be positive".to_string(),
                ));
            }
        }
        if let (Some(minimum), Some(maximum)) = (self.minimum, self.maximum) {
            if minimum > maximum {
                return Err(Error::Invalid(
                    "capability constraint minimum must not exceed maximum".to_string(),
                ));
            }
        }
        if self.value.as_ref().is_none_or(Value::is_null)
            && self.values.is_empty()
            && self.minimum.is_none()
            && self.maximum.is_none()
            && self.step.is_none()
        {
            return Err(Error::Invalid(
                "capability constraint must carry a bound or value".to_string(),
            ));
        }
        Ok(())
    }
}

impl CapabilityDescriptor {
    pub fn validate(&self) -> Result<()> {
        require_contract_token(&self.capability_id, "capability_id")?;
        require_globally_qualified_name(&self.family, "capability family")?;
        validate_core_capability_family(&self.family)?;
        require_contract_token(&self.capability_type, "capability type")?;
        validate_capability_direction(&self.direction)?;
        validate_access(&self.direction, &self.access)?;
        if let Some(value_schema) = &self.value_schema {
            value_schema.validate()?;
        }
        if let Some(command_schema) = &self.command_schema {
            if !matches!(self.direction.as_str(), "output" | "command") {
                return Err(Error::Invalid(
                    "command schema is only valid on output or command capabilities".to_string(),
                ));
            }
            command_schema.validate()?;
        }
        for constraint in &self.constraints {
            constraint.validate()?;
        }
        for unit in &self.units {
            unit.validate()?;
        }
        if let Some(projection) = &self.projection {
            projection.validate()?;
        }
        for source in &self.sources {
            source.validate()?;
        }
        validate_contract_token_list(&self.event_types, "event or command type")?;
        validate_contract_token_list(&self.command_types, "event or command type")?;
        if !self.event_types.is_empty() && !matches!(self.direction.as_str(), "input" | "state") {
            return Err(Error::Invalid(
                "event types are only valid on input or state capabilities".to_string(),
            ));
        }
        if !self.command_types.is_empty()
            && !matches!(self.direction.as_str(), "output" | "command")
        {
            return Err(Error::Invalid(
                "command types are only valid on output or command capabilities".to_string(),
            ));
        }
        self.validate_core_family_type()
    }

    fn validate_core_family_type(&self) -> Result<()> {
        let direction = match self.family.as_str() {
            "dev.deckr.input.button" | "dev.deckr.input.encoder" | "dev.deckr.input.touch" => {
                Some("input")
            }
            "dev.deckr.output.raster" => Some("output"),
            "dev.deckr.device.power" => Some("command"),
            _ => None,
        };
        if direction.is_some_and(|direction| self.direction != direction) {
            return Err(Error::Invalid(
                "core capability direction disagrees with its family".into(),
            ));
        }
        match self.family.as_str() {
            "dev.deckr.input.button" => {
                if !matches!(self.capability_type.as_str(), "activation" | "momentary") {
                    return Err(Error::Invalid(
                        "dev.deckr.input.button capability type must be activation or momentary"
                            .to_string(),
                    ));
                }
                if self.capability_type == "activation" && self.event_types != ["press"] {
                    return Err(Error::Invalid(
                        "dev.deckr.input.button activation capabilities emit press only"
                            .to_string(),
                    ));
                }
                if self.capability_type == "momentary" && self.event_types != ["down", "up"] {
                    return Err(Error::Invalid(
                        "dev.deckr.input.button momentary capabilities emit down and up"
                            .to_string(),
                    ));
                }
            }
            "dev.deckr.input.encoder" => {
                if self.capability_type != "relative" {
                    return Err(Error::Invalid(
                        "dev.deckr.input.encoder capability type must be relative".to_string(),
                    ));
                }
                if self.event_types != ["rotate"] {
                    return Err(Error::Invalid(
                        "dev.deckr.input.encoder relative capabilities emit rotate".to_string(),
                    ));
                }
            }
            "dev.deckr.input.touch" => {
                if self.capability_type != "gesture" {
                    return Err(Error::Invalid(
                        "dev.deckr.input.touch capability type must be gesture".to_string(),
                    ));
                }
                if self.event_types != ["tap", "swipe"] {
                    return Err(Error::Invalid(
                        "dev.deckr.input.touch gesture capabilities emit tap and swipe".to_string(),
                    ));
                }
            }
            "dev.deckr.output.raster" => {
                if self.capability_type != "bitmap" {
                    return Err(Error::Invalid(
                        "dev.deckr.output.raster capability type must be bitmap".to_string(),
                    ));
                }
                if self.command_types != ["set_frame", "clear"] {
                    return Err(Error::Invalid(
                        "dev.deckr.output.raster bitmap capabilities support set_frame and clear"
                            .to_string(),
                    ));
                }
            }
            "dev.deckr.device.power" => {
                if self.capability_type != "screen" {
                    return Err(Error::Invalid(
                        "dev.deckr.device.power capability type must be screen".to_string(),
                    ));
                }
                if self.command_types != ["sleep", "wake"] {
                    return Err(Error::Invalid(
                        "dev.deckr.device.power screen capabilities support sleep and wake"
                            .to_string(),
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl ControlDescriptor {
    pub fn capabilities(&self) -> impl Iterator<Item = &CapabilityDescriptor> {
        self.input_capabilities
            .iter()
            .chain(&self.output_capabilities)
            .chain(&self.config_capabilities)
            .chain(&self.state_capabilities)
            .chain(&self.diagnostic_capabilities)
    }

    pub fn validate(&self) -> Result<()> {
        require_non_empty(&self.control_id, "control_id")?;
        require_contract_token(&self.kind, "control kind")?;
        if let Some(label) = &self.label {
            require_non_empty(label, "control text")?;
        }
        if let Some(geometry) = &self.geometry {
            geometry.validate()?;
        }
        optional_text(&self.group_id, "control group id")?;
        optional_text(&self.surface_id, "control surface id")?;
        optional_text(&self.parent_control_id, "parent control id")?;
        require_unique(&self.related_control_ids, "related control ids")?;
        for source in &self.sources {
            source.validate()?;
        }
        let mut capability_ids = Vec::new();
        for capability in &self.input_capabilities {
            if capability.direction != "input" {
                return Err(Error::Invalid(
                    "input_capabilities must have input direction".to_string(),
                ));
            }
            capability.validate()?;
            capability_ids.push(capability.capability_id.clone());
        }
        for capability in &self.output_capabilities {
            if capability.direction != "output" {
                return Err(Error::Invalid(
                    "output_capabilities must have output direction".to_string(),
                ));
            }
            capability.validate()?;
            capability_ids.push(capability.capability_id.clone());
        }
        for (capabilities, directions) in [
            (&self.config_capabilities, &["state", "command"][..]),
            (&self.state_capabilities, &["state"][..]),
            (&self.diagnostic_capabilities, &["state"][..]),
        ] {
            for capability in capabilities {
                if !directions.contains(&capability.direction.as_str()) {
                    return Err(Error::Invalid(
                        "capability direction disagrees with control collection".into(),
                    ));
                }
                capability.validate()?;
                capability_ids.push(capability.capability_id.clone());
            }
        }
        require_unique(&capability_ids, "capability ids on control")?;
        Ok(())
    }
}

impl DeviceDescriptor {
    /// Resolve descriptor-local identity across every capability collection.
    pub fn capability(
        &self,
        control_id: Option<&str>,
        capability_id: &str,
    ) -> Option<&CapabilityDescriptor> {
        match control_id {
            Some(control_id) => self
                .controls
                .iter()
                .find(|control| control.control_id == control_id)?
                .capabilities()
                .find(|capability| capability.capability_id == capability_id),
            None => self
                .capabilities
                .iter()
                .find(|capability| capability.capability_id == capability_id),
        }
    }

    pub fn validate(&self) -> Result<()> {
        require_not_endpoint_address(&self.device_id, "device_id")?;
        require_non_empty(&self.fingerprint, "device descriptor text")?;
        require_non_empty(&self.display_name, "device descriptor text")?;
        if let Some(manufacturer) = &self.manufacturer {
            require_non_empty(manufacturer, "device descriptor text")?;
        }
        if let Some(model) = &self.model {
            require_non_empty(model, "device descriptor text")?;
        }
        if let Some(serial_number) = &self.serial_number {
            require_non_empty(serial_number, "device descriptor text")?;
        }
        optional_text(&self.model_id, "device model id")?;
        optional_text(&self.hardware_version, "hardware version")?;
        optional_text(&self.firmware_version, "firmware version")?;
        if let Some(parent) = &self.parent {
            parent.validate()?;
        }
        for identifier in &self.identifiers {
            identifier.validate()?;
        }
        let mut connection_ids = Vec::new();
        for connection in &self.connections {
            connection.validate()?;
            connection_ids.push(connection.connection_id.clone());
        }
        require_unique(&connection_ids, "connection ids")?;
        let check_sources = |sources: &[DeviceSourceReference]| -> Result<()> {
            for source in sources {
                source.validate()?;
                if source
                    .connection_id
                    .as_ref()
                    .is_some_and(|id| !connection_ids.contains(id))
                {
                    return Err(Error::Invalid(
                        "source references an unknown connection".into(),
                    ));
                }
            }
            Ok(())
        };
        check_sources(&self.sources)?;
        let mut control_ids = Vec::new();
        for control in &self.controls {
            control.validate()?;
            control_ids.push(control.control_id.clone());
        }
        require_unique(&control_ids, "control ids")?;
        for control in &self.controls {
            check_sources(&control.sources)?;
            for reference in control
                .parent_control_id
                .iter()
                .chain(&control.related_control_ids)
            {
                if reference == &control.control_id || !control_ids.contains(reference) {
                    return Err(Error::Invalid(
                        "parent or related control reference is unknown or refers to itself".into(),
                    ));
                }
            }
        }
        let mut capability_ids = Vec::new();
        for capability in &self.capabilities {
            if !matches!(
                capability.direction.as_str(),
                "state" | "command" | "output"
            ) {
                return Err(Error::Invalid(
                    "device-level capability must have state, command or output direction".into(),
                ));
            }
            capability.validate()?;
            capability_ids.push(capability.capability_id.clone());
        }
        require_unique(&capability_ids, "device-level capability ids")?;
        for (control_id, capability) in self
            .capabilities
            .iter()
            .map(|capability| (None, capability))
            .chain(self.controls.iter().flat_map(|control| {
                control
                    .capabilities()
                    .map(move |capability| (Some(control.control_id.as_str()), capability))
            }))
        {
            check_sources(&capability.sources)?;
            if let Some(projection) = &capability.projection {
                let source = &projection.source;
                if self
                    .capability(source.control_id.as_deref(), &source.capability_id)
                    .is_none()
                    || (source.control_id.as_deref() == control_id
                        && source.capability_id == capability.capability_id)
                {
                    return Err(Error::Invalid(
                        "projection source is unknown or refers to itself".into(),
                    ));
                }
            }
        }
        if let Some(indicator) = &self.default_status_indicator {
            indicator.validate()?;
            if self
                .capability(indicator.control_id.as_deref(), &indicator.capability_id)
                .is_none_or(|capability| capability.direction != "output")
            {
                return Err(Error::Invalid(
                    "default status indicator must resolve to an output capability".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceSourceReference {
    pub source_id: String,
    #[serde(rename = "type")]
    pub source_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub facts: serde_json::Map<String, Value>,
}

impl DeviceSourceReference {
    pub fn validate(&self) -> Result<()> {
        require_not_endpoint_address(&self.source_id, "device source reference")?;
        require_contract_token(&self.source_type, "device source type")?;
        if let Some(label) = &self.label {
            require_non_empty(label, "device source reference")?;
        }
        if let Some(connection_id) = &self.connection_id {
            require_non_empty(connection_id, "device source reference")?;
        }
        Ok(())
    }
}
