//! Page checks against current application-owned observations. The application
//! serializes transitions and repeats these checks at commit.

use super::*;
use crate::authority::ContractPointer;
use crate::lanes::{DeckrMessage, DeviceRef};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageTargetState {
    pub action_instance_id: String,
    pub active: bool,
    pub generation: u64,
    pub contract: ContractPointer,
    pub runtime_endpoint: String,
    pub runtime_session_id: String,
    pub device_ref: DeviceRef,
    pub control_ids: Vec<String>,
    #[serde(default)]
    pub page_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageCommandContext {
    pub contract: ContractPointer,
    pub controller_endpoint: String,
    pub controller_session_id: String,
    pub runtime_endpoint: String,
    pub runtime_session_id: String,
    pub local_authority: bool,
    pub hardware_authority: bool,
    pub bindings: BTreeMap<String, PageTargetState>,
    pub page_sessions: BTreeMap<String, PageTargetState>,
}
/// Return the retained target without acquiring authority or changing page state.
pub fn validate_page_command<'a>(
    message: &DeckrMessage,
    context: &'a PageCommandContext,
) -> Result<&'a PageTargetState> {
    let action = ActionMessage::from_envelope(message)?;
    let (target, descriptor) = match &action {
        ActionMessage::OpenPage(body) | ActionMessage::ReplacePage(body) => {
            (&body.target, Some(&body.descriptor))
        }
        ActionMessage::ClosePage(body) => (&body.target, None),
        _ => return Err(Error::Invalid("message is not a page command".into())),
    };
    let denied = || {
        Error::Unauthorized(
            "page target is stale, inactive, outside scope, or belongs to another agreement".into(),
        )
    };
    if !context.local_authority
        || !context.hardware_authority
        || message.contract.as_ref() != Some(&context.contract)
        || message.sender != context.runtime_endpoint
        || message.sender_session_id != context.runtime_session_id
        || message.recipient_endpoint() != Some(context.controller_endpoint.as_str())
        || message.recipient_session_id.as_deref() != Some(context.controller_session_id.as_str())
    {
        return Err(denied());
    }
    let (current, generation) = match target {
        PageCommandTarget::Binding {
            binding_id,
            generation,
        } => (context.bindings.get(binding_id), generation),
        PageCommandTarget::PageSession {
            page_session_id,
            generation,
        } => (context.page_sessions.get(page_session_id), generation),
    };
    let current = current.ok_or_else(denied)?;
    if !current.active
        || current.generation != *generation
        || current.runtime_endpoint != context.runtime_endpoint
        || current.runtime_session_id != context.runtime_session_id
        || current.contract != context.contract
    {
        return Err(denied());
    }
    if let Some(descriptor) = descriptor {
        if descriptor
            .bindings
            .iter()
            .any(|child| !current.control_ids.contains(&child.control_id))
            || (matches!(action, ActionMessage::ReplacePage(_))
                && current.page_id.as_deref() != Some(descriptor.page_id.as_str()))
        {
            return Err(denied());
        }
    }
    Ok(current)
}
