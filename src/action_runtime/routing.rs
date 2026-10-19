//! Pure checks against controller-supplied current routing facts. No state is
//! allocated here, and returned hardware payloads carry no hardware authority.

use std::collections::BTreeMap;

use crate::hardware::validation::ValidatedDevice;
use crate::lanes::{CapabilityDescriptor, DeviceDescriptor, DeviceRef, HardwareMessageBody};
use crate::{Error, Result};
use serde_json::Value;

use super::*;

fn unauthorized(detail: &str) -> Error {
    Error::Unauthorized(detail.into())
}
fn device_matches(
    reference: &DeviceRef,
    manager_id: &str,
    device: &DeviceDescriptor,
) -> Result<()> {
    reference.validate()?;
    if reference.manager_id != manager_id
        || reference.device_id != device.device_id
        || reference
            .fingerprint
            .as_ref()
            .is_some_and(|fingerprint| fingerprint != &device.fingerprint)
    {
        return Err(unauthorized(
            "action reference is outside the negotiated device",
        ));
    }
    Ok(())
}
fn resolve<'a>(
    reference: &CapabilityRef,
    manager_id: &str,
    device: &'a DeviceDescriptor,
) -> Result<&'a CapabilityDescriptor> {
    reference.validate()?;
    if let Some(reference) = &reference.device_ref {
        device_matches(reference, manager_id, device)?;
    }
    device
        .capability(reference.control_id.as_deref(), &reference.capability_id)
        .ok_or_else(|| {
            unauthorized("action capability or source does not exist in the negotiated descriptor")
        })
}
impl CapabilityRequirementSelector {
    /// Test one controller-selected candidate. This does not choose a preference
    /// or decide how requirements should be allocated to controls.
    pub fn matches(&self, capability: &CapabilityDescriptor) -> Result<bool> {
        self.validate()?;
        Ok(self
            .capability_id
            .as_ref()
            .is_none_or(|id| id == &capability.capability_id)
            && self
                .family
                .as_ref()
                .is_none_or(|family| family == &capability.family)
            && self
                .capability_type
                .as_ref()
                .is_none_or(|kind| kind == &capability.capability_type)
            && self
                .direction
                .as_ref()
                .is_none_or(|direction| direction == &capability.direction)
            && self
                .event_types
                .iter()
                .all(|event| capability.event_types.contains(event))
            && self
                .command_types
                .iter()
                .all(|command| capability.command_types.contains(command)))
    }
}
fn view_matches(views: &[CapabilityView], provenance: CapabilityProvenance) -> bool {
    views.is_empty()
        || views.iter().any(|view| {
            matches!(
                (view, provenance),
                (
                    CapabilityView::Raw | CapabilityView::Native,
                    CapabilityProvenance::Native
                ) | (CapabilityView::Projected, CapabilityProvenance::Projection)
                    | (CapabilityView::Derived, CapabilityProvenance::Derivation)
                    | (CapabilityView::Extension, CapabilityProvenance::Extension)
            )
        })
}
fn satisfies(
    requirement: &CapabilityRequirement,
    matched: &MatchedCapability,
    capability: &CapabilityDescriptor,
) -> Result<bool> {
    if matched
        .requirement_name
        .as_ref()
        .is_some_and(|name| name != &requirement.name)
    {
        return Ok(false);
    }
    Ok(requirement
        .preferences
        .iter()
        .map(|selector| selector.matches(capability))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .any(|matches| matches)
        && requirement.event_types.iter().all(|event| {
            capability.event_types.contains(event)
                && (matched.event_types.is_empty() || matched.event_types.contains(event))
        })
        && requirement.command_types.iter().all(|command| {
            capability.command_types.contains(command)
                && (matched.command_types.is_empty() || matched.command_types.contains(command))
        })
        && view_matches(&requirement.views, matched.provenance))
}

/// Check the controller's resolved binding against the selected action and one
/// negotiated device. Device and capability selection remain controller policy.
pub fn validate_binding_resources(
    binding: &BindingMetadata,
    action: &ActionDescriptor,
    manager_id: &str,
    device: &ValidatedDevice,
) -> Result<()> {
    binding.validate()?;
    action.validate()?;
    if binding.action_id != action.action_id {
        return Err(unauthorized("binding names another action"));
    }
    let descriptor = device.descriptor();
    device_matches(&binding.device_ref, manager_id, descriptor)?;
    device_matches(&binding.control_ref.device_ref, manager_id, descriptor)?;
    if !descriptor
        .controls
        .iter()
        .any(|control| control.control_id == binding.control_ref.control_id)
    {
        return Err(unauthorized(
            "binding control is absent from the negotiated descriptor",
        ));
    }
    let requirements = action.requirements.as_deref().unwrap_or_default();
    let mut resolved = Vec::new();
    for matched in &binding.matched_capabilities {
        let capability = resolve(&matched.capability, manager_id, descriptor)?;
        if matched.family != capability.family
            || matched.capability_type != capability.capability_type
            || matched.direction != capability.direction
            || !matched
                .event_types
                .iter()
                .all(|event| capability.event_types.contains(event))
            || !matched
                .command_types
                .iter()
                .all(|command| capability.command_types.contains(command))
        {
            return Err(unauthorized(
                "matched capability changes the negotiated capability's meaning",
            ));
        }
        let provenance_ok = match (&capability.projection, matched.provenance) {
            (None, CapabilityProvenance::Native | CapabilityProvenance::Extension) => true,
            (Some(projection), CapabilityProvenance::Projection) => {
                projection.projection_type == "projection"
            }
            (Some(projection), CapabilityProvenance::Derivation) => {
                projection.projection_type == "derivation"
            }
            _ => false,
        };
        if !provenance_ok {
            return Err(unauthorized(
                "matched provenance differs from the negotiated descriptor",
            ));
        }
        if let Some(source) = &matched.source {
            resolve(source, manager_id, descriptor)?;
            if capability.projection.as_ref().is_some_and(|projection| {
                projection.source.control_id != source.control_id
                    || projection.source.capability_id != source.capability_id
            }) {
                return Err(unauthorized(
                    "matched projection source differs from its descriptor",
                ));
            }
        }
        if let Some(name) = &matched.requirement_name {
            let requirement = requirements
                .iter()
                .find(|requirement| requirement.name == *name)
                .ok_or_else(|| unauthorized("matched capability names an unknown requirement"))?;
            if !satisfies(requirement, matched, capability)? {
                return Err(unauthorized(
                    "resolved capability does not satisfy its action requirement",
                ));
            }
        } else if !requirements.is_empty()
            && !requirements
                .iter()
                .map(|requirement| satisfies(requirement, matched, capability))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .any(|matches| matches)
        {
            return Err(unauthorized(
                "resolved capability satisfies no action requirement",
            ));
        }
        resolved.push((matched, capability));
    }
    for requirement in requirements
        .iter()
        .filter(|requirement| requirement.required)
    {
        if !resolved
            .iter()
            .map(|(matched, capability)| satisfies(requirement, matched, capability))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .any(|matches| matches)
        {
            return Err(unauthorized("required action capability is unresolved"));
        }
    }
    Ok(())
}

/// Exact metadata comparison against controller-owned current state. The
/// application supplies the state from the held agreement and controller session.
pub fn validate_binding_reference(
    supplied: &BindingMetadata,
    current: &BindingMetadata,
) -> Result<()> {
    supplied.validate()?;
    current.validate()?;
    if supplied != current {
        return Err(unauthorized(
            "binding metadata is stale or differs from the current controller binding",
        ));
    }
    Ok(())
}
pub fn validate_action_reference(
    supplied: &ActionInstanceMetadata,
    current: &ActionInstanceMetadata,
) -> Result<()> {
    supplied.validate()?;
    current.validate()?;
    if supplied != current {
        return Err(unauthorized(
            "action instance differs from current controller state",
        ));
    }
    Ok(())
}
pub fn validate_page_reference(
    supplied: &PageSessionMetadata,
    current: &PageSessionMetadata,
) -> Result<()> {
    supplied.validate()?;
    current.validate()?;
    if supplied != current {
        return Err(unauthorized(
            "page session differs from current controller state",
        ));
    }
    Ok(())
}
fn generation(supplied: u64, current: &BindingMetadata) -> Result<()> {
    if supplied != current.output_generation {
        return Err(unauthorized(
            "output generation is not the controller's current generation",
        ));
    }
    Ok(())
}
fn same_capability(left: &CapabilityRef, right: &CapabilityRef) -> bool {
    left.capability_id == right.capability_id
        && left.control_id == right.control_id
        && match (&left.device_ref, &right.device_ref) {
            (Some(left), Some(right)) => {
                left.manager_id == right.manager_id
                    && left.device_id == right.device_id
                    && left
                        .fingerprint
                        .as_ref()
                        .zip(right.fingerprint.as_ref())
                        .is_none_or(|(left, right)| left == right)
            }
            // Relative sources inherit the enclosing device. Both references
            // are separately resolved against that negotiated device before
            // protected work; fully qualified bindings never take this branch.
            _ => true,
        }
}
fn bound_capability<'a>(
    reference: &CapabilityRef,
    current: &'a BindingMetadata,
    accepts: impl Fn(&MatchedCapability) -> bool,
) -> Result<&'a MatchedCapability> {
    reference.fully_qualified()?;
    current
        .matched_capabilities
        .iter()
        .find(|matched| same_capability(&matched.capability, reference) && accepts(matched))
        .ok_or_else(|| unauthorized("capability is not in the controller's current binding"))
}

/// Validate an output request and construct its hardware body. Sending that body
/// still requires a separate current hardware claim; the service agreement never
/// grants hardware ownership and its pointer must not be forwarded to hardware.
pub fn validate_binding_output(
    output: &BindingOutput,
    current: &BindingMetadata,
    action: &ActionDescriptor,
    manager_id: &str,
    device: &ValidatedDevice,
) -> Result<HardwareMessageBody> {
    output.validate()?;
    validate_binding_reference(&output.binding, current)?;
    generation(output.generation, current)?;
    validate_binding_resources(current, action, manager_id, device)?;
    bound_capability(&output.capability, current, |matched| {
        matched.command_types.is_empty() || matched.command_types.contains(&output.command_type)
    })?;
    let message = HardwareMessageBody::ControlCommand {
        device_ref: output
            .capability
            .device_ref
            .clone()
            .expect("qualified capability"),
        control_id: output.capability.control_id.clone(),
        capability_id: output.capability.capability_id.clone(),
        command_type: output.command_type.clone(),
        params: output.params.clone(),
    };
    device.validate_message(&message)?;
    Ok(message)
}
pub fn validate_binding_overlay(overlay: &BindingOverlay, current: &BindingMetadata) -> Result<()> {
    overlay.validate()?;
    validate_binding_reference(&overlay.binding, current)?;
    generation(overlay.generation, current)
}
pub fn validate_binding_overlay_clear(
    clear: &BindingOverlayClear,
    current: &BindingMetadata,
) -> Result<()> {
    clear.validate()?;
    validate_binding_reference(&clear.binding, current)?;
    generation(clear.generation, current)
}
pub fn validate_capability_input(
    input: &CapabilityInput,
    current: &BindingMetadata,
    action: &ActionDescriptor,
    manager_id: &str,
    device: &ValidatedDevice,
) -> Result<()> {
    input.validate()?;
    validate_binding_reference(&input.binding, current)?;
    validate_binding_resources(current, action, manager_id, device)?;
    let capability = resolve(&input.event.capability, manager_id, device.descriptor())?;
    let matched = bound_capability(&input.event.capability, current, |matched| {
        (matched.event_types.is_empty() || matched.event_types.contains(&input.event.event_type))
            && input.event.view.is_none_or(|view| {
                view_matches(&[view], matched.provenance)
                    && action.requirements.as_deref().is_none_or(|requirements| {
                        requirements.is_empty()
                            || requirements.iter().any(|requirement| {
                                (requirement.views.is_empty() || requirement.views.contains(&view))
                                    && satisfies(requirement, matched, capability).unwrap_or(false)
                            })
                    })
            })
            && input
                .event
                .source
                .as_ref()
                .zip(matched.source.as_ref())
                .is_none_or(|(source, expected)| same_capability(source, expected))
    })?;
    if !capability.event_types.contains(&input.event.event_type)
        || (!matched.event_types.is_empty()
            && !matched.event_types.contains(&input.event.event_type))
    {
        return Err(unauthorized(
            "input event is outside the selected capability operations",
        ));
    }
    if let Some(source) = &input.event.source {
        resolve(source, manager_id, device.descriptor())?;
        if matched
            .source
            .as_ref()
            .is_some_and(|expected| !same_capability(expected, source))
        {
            return Err(unauthorized("input source differs from the matched source"));
        }
        if capability.projection.as_ref().is_some_and(|projection| {
            projection.source.control_id != source.control_id
                || projection.source.capability_id != source.capability_id
        }) {
            return Err(unauthorized(
                "input source differs from the negotiated projection source",
            ));
        }
    }
    if let Some(view) = input.event.view {
        if !view_matches(&[view], matched.provenance) {
            return Err(unauthorized(
                "input view differs from the matched provenance",
            ));
        }
        for requirement in action
            .requirements
            .iter()
            .flatten()
            .filter(|requirement| matched.requirement_name.as_ref() == Some(&requirement.name))
        {
            if !requirement.views.is_empty() && !requirement.views.contains(&view) {
                return Err(unauthorized("input view is outside its action requirement"));
            }
        }
    }
    let reference = &input.event.capability;
    let message = if capability.direction == "state" {
        HardwareMessageBody::CapabilityStateChanged {
            device_ref: reference.device_ref.clone().expect("qualified capability"),
            control_id: reference.control_id.clone(),
            capability_id: reference.capability_id.clone(),
            state_type: Some(input.event.event_type.clone()),
            value: input.event.value.clone(),
            occurred_at: Some(input.event.occurred_at.clone()),
            sequence: input.event.sequence,
        }
    } else {
        HardwareMessageBody::ControlInput {
            device_ref: reference.device_ref.clone().expect("qualified capability"),
            control_id: reference.control_id.clone().expect("qualified capability"),
            capability_id: reference.capability_id.clone(),
            event_type: input.event.event_type.clone(),
            value: input.event.value.clone(),
            occurred_at: Some(input.event.occurred_at.clone()),
            sequence: input.event.sequence,
            sources: Vec::new(),
        }
    };
    device.validate_message(&message)
}

/// A controller-provided resolution, not a provider-supplied live action ID.
#[derive(Debug, Clone)]
pub struct ResolvedPageChild {
    pub control_id: String,
    pub action: ActionInstanceMetadata,
    pub provider_labels: BTreeMap<String, Value>,
}
/// Validate dynamic targets after the controller resolves them in its current
/// authorized session. This function neither chooses nor allocates children.
pub fn validate_page_children(
    command: &DynamicPageCommand,
    opener: &ActionInstanceMetadata,
    resolved: &[ResolvedPageChild],
) -> Result<()> {
    command.validate()?;
    opener.validate()?;
    if resolved.len() != command.bindings.len() {
        return Err(unauthorized(
            "dynamic page children are not completely resolved",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for child in resolved {
        child.action.validate()?;
        if !seen.insert(&child.control_id) {
            return Err(unauthorized(
                "resolved dynamic page controls are duplicated",
            ));
        }
        let requested = command
            .bindings
            .iter()
            .find(|requested| requested.control_id == child.control_id)
            .ok_or_else(|| unauthorized("resolved child was not requested"))?;
        match requested.target.kind {
            PageChildTargetKind::SelfAction => validate_action_reference(&child.action, opener)?,
            PageChildTargetKind::Action => {
                if requested.target.action_id.as_ref() != Some(&child.action.action_id)
                    || requested
                        .target
                        .provider_instance_id
                        .as_ref()
                        .is_some_and(|instance| instance != &child.action.provider_instance_id)
                    || requested
                        .target
                        .provider_labels
                        .as_ref()
                        .is_some_and(|labels| {
                            labels
                                .iter()
                                .any(|(key, value)| child.provider_labels.get(key) != Some(value))
                        })
                {
                    return Err(unauthorized(
                        "resolved child differs from its action/provider selector",
                    ));
                }
            }
        }
    }
    Ok(())
}
