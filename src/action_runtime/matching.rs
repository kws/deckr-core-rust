//! Pure matching within a caller-selected negotiated control and configured scope.

use super::*;
use crate::lanes::{CapabilityDescriptor, DeviceDescriptor, DeviceRef};
use std::collections::BTreeSet;

pub struct CapabilityMatchingScope<'a> {
    pub device: &'a DeviceDescriptor,
    pub device_ref: &'a DeviceRef,
    pub control_id: &'a str,
    pub configured_capability_ids: Option<&'a [String]>,
}
fn invalid(message: &str) -> Error {
    Error::Unauthorized(message.into())
}
fn names(values: &[String]) -> BTreeSet<&str> {
    values.iter().map(String::as_str).collect()
}
fn capabilities<'a>(scope: &CapabilityMatchingScope<'a>) -> Result<Vec<&'a CapabilityDescriptor>> {
    scope.device.validate()?;
    scope.device_ref.validate()?;
    if scope.device_ref.device_id != scope.device.device_id
        || scope
            .device_ref
            .fingerprint
            .as_ref()
            .is_some_and(|f| f != &scope.device.fingerprint)
    {
        return Err(invalid("device differs from the negotiated descriptor"));
    }
    let control = scope
        .device
        .controls
        .iter()
        .find(|c| c.control_id == scope.control_id)
        .ok_or_else(|| invalid("control is outside the negotiated descriptor"))?;
    let all = control.capabilities().collect::<Vec<_>>();
    if let Some(ids) = scope.configured_capability_ids {
        if ids
            .iter()
            .any(|id| !all.iter().any(|cap| &cap.capability_id == id))
        {
            return Err(invalid(
                "configured capability is outside the bound control",
            ));
        }
        Ok(all
            .into_iter()
            .filter(|cap| ids.contains(&cap.capability_id))
            .collect())
    } else {
        Ok(all)
    }
}
pub(super) fn view_matches(view: &CapabilityView, cap: &CapabilityDescriptor) -> bool {
    match view {
        CapabilityView::Raw => cap.projection.is_none(),
        CapabilityView::Native => cap.projection.is_none() && cap.family.starts_with("dev.deckr."),
        CapabilityView::Extension => !cap.family.starts_with("dev.deckr."),
        CapabilityView::Projected => cap
            .projection
            .as_ref()
            .is_some_and(|p| p.projection_type == "projection"),
        CapabilityView::Derived => cap
            .projection
            .as_ref()
            .is_some_and(|p| p.projection_type == "derivation"),
    }
}
fn candidates<'a>(
    requirement: &CapabilityRequirement,
    caps: &[&'a CapabilityDescriptor],
) -> Result<Vec<&'a CapabilityDescriptor>> {
    let eligible = caps
        .iter()
        .copied()
        .filter(|cap| {
            names(&requirement.event_types).is_subset(&names(&cap.event_types))
                && names(&requirement.command_types).is_subset(&names(&cap.command_types))
                && (requirement.views.is_empty()
                    || requirement.views.iter().any(|view| view_matches(view, cap)))
        })
        .collect::<Vec<_>>();
    for selector in &requirement.preferences {
        let mut selected = Vec::new();
        for cap in &eligible {
            if selector.matches(cap)? {
                selected.push(*cap);
            }
        }
        if !selected.is_empty() {
            return Ok(selected);
        }
    }
    Ok(Vec::new())
}
fn matched(
    requirement: &CapabilityRequirement,
    cap: &CapabilityDescriptor,
    scope: &CapabilityMatchingScope<'_>,
) -> Result<MatchedCapability> {
    let (provenance, source) = if let Some(projection) = &cap.projection {
        if scope
            .device
            .capability(
                projection.source.control_id.as_deref(),
                &projection.source.capability_id,
            )
            .is_none()
        {
            return Err(invalid(
                "projection source is outside the negotiated descriptor",
            ));
        }
        (
            if projection.projection_type == "derivation" {
                CapabilityProvenance::Derivation
            } else {
                CapabilityProvenance::Projection
            },
            Some(CapabilityRef {
                device_ref: Some(scope.device_ref.clone()),
                control_id: projection.source.control_id.clone(),
                capability_id: projection.source.capability_id.clone(),
            }),
        )
    } else {
        (
            if cap.family.starts_with("dev.deckr.") {
                CapabilityProvenance::Native
            } else {
                CapabilityProvenance::Extension
            },
            None,
        )
    };
    Ok(MatchedCapability {
        requirement_name: Some(requirement.name.clone()),
        capability: CapabilityRef {
            device_ref: Some(scope.device_ref.clone()),
            control_id: Some(scope.control_id.into()),
            capability_id: cap.capability_id.clone(),
        },
        family: cap.family.clone(),
        capability_type: cap.capability_type.clone(),
        direction: cap.direction.clone(),
        event_types: cap.event_types.clone(),
        command_types: cap.command_types.clone(),
        provenance,
        source,
    })
}
/// Select the first descriptor-order candidate within the first eligible preference.
/// An application may choose another equivalent candidate and validate it below.
pub fn match_capability_requirements(
    action: &ActionDescriptor,
    scope: &CapabilityMatchingScope<'_>,
) -> Result<Vec<MatchedCapability>> {
    action.validate()?;
    let caps = capabilities(scope)?;
    let mut result = Vec::new();
    for requirement in action.requirements.as_deref().unwrap_or_default() {
        let eligible = candidates(requirement, &caps)?;
        if let Some(cap) = eligible.first() {
            result.push(matched(requirement, cap, scope)?);
        } else if requirement.required {
            return Err(invalid("required capability is unavailable"));
        }
    }
    Ok(result)
}
/// Validate a complete ordered result, including application capability restrictions.
/// The supplied scope is an observation, never permission to send hardware messages.
pub fn validate_binding_resources_scoped(
    binding: &BindingMetadata,
    action: &ActionDescriptor,
    scope: &CapabilityMatchingScope<'_>,
) -> Result<()> {
    binding.validate()?;
    action.validate()?;
    let caps = capabilities(scope)?;
    if binding.action_id != action.action_id
        || &binding.device_ref != scope.device_ref
        || &binding.control_ref.device_ref != scope.device_ref
        || binding.control_ref.control_id != scope.control_id
    {
        return Err(invalid(
            "binding differs from the selected action or control scope",
        ));
    }
    let mut remaining = binding.matched_capabilities.iter();
    for requirement in action.requirements.as_deref().unwrap_or_default() {
        let eligible = candidates(requirement, &caps)?;
        if eligible.is_empty() {
            if requirement.required {
                return Err(invalid("required capability is unavailable"));
            }
            continue;
        }
        let supplied = remaining
            .next()
            .ok_or_else(|| invalid("satisfied requirement needs a match"))?;
        if supplied.requirement_name.as_ref() != Some(&requirement.name) {
            return Err(invalid(
                "matches must occur once per satisfied requirement in declaration order",
            ));
        }
        let cap = eligible
            .iter()
            .find(|cap| cap.capability_id == supplied.capability.capability_id)
            .ok_or_else(|| invalid("match is not eligible for the first available preference"))?;
        let mut expected = matched(requirement, cap, scope)?;
        if names(&supplied.event_types) != names(&expected.event_types)
            || names(&supplied.command_types) != names(&expected.command_types)
        {
            return Err(invalid("match must report complete supported name sets"));
        }
        expected.event_types.clone_from(&supplied.event_types);
        expected.command_types.clone_from(&supplied.command_types);
        if supplied != &expected {
            return Err(invalid(
                "match changes descriptor identity, provenance, or source",
            ));
        }
    }
    if remaining.next().is_some() {
        return Err(invalid(
            "match names an undeclared or unsatisfied requirement",
        ));
    }
    Ok(())
}
