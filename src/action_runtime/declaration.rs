//! Checks for the host-supplied, core-owned Action Runtime declaration.

use super::*;
use crate::services::{
    ExchangePattern, ServiceIntent, ServiceMessageDirection, ServiceProtocol, ServiceViewWriter,
};
use crate::{Error, Result};

/// Check the core declaration's identities, operations, directions, schema
/// references and view family before compiling it with `SchemaRegistry`.
/// The host must load the declaration and referenced schemas from trusted
/// upstream artifacts. This crate neither embeds nor fetches those artifacts.
pub fn validate_action_runtime_protocol(protocol: &ServiceProtocol) -> Result<()> {
    protocol.validate()?;
    if protocol.namespace != ACTION_RUNTIME_NAMESPACE
        || protocol.feature_id != ACTION_RUNTIME_NAMESPACE
        || protocol.advertisement_profile != ACTION_RUNTIME_ADVERTISEMENT_PROFILE
        || protocol.use_profile != ACTION_RUNTIME_USE_PROFILE
    {
        return Err(Error::Invalid(
            "Action Runtime declaration has a different core identity".into(),
        ));
    }
    use ServiceIntent::{Command, Event};
    use ServiceMessageDirection::{ConsumerToService, ServiceToConsumer};
    let messages = [
        (
            "action_instance_created",
            Event,
            ConsumerToService,
            "action-instance-lifecycle-body",
        ),
        (
            "action_instance_destroyed",
            Event,
            ConsumerToService,
            "action-instance-lifecycle-body",
        ),
        (
            "binding_attached",
            Event,
            ConsumerToService,
            "binding-attached-body",
        ),
        (
            "binding_detached",
            Event,
            ConsumerToService,
            "binding-detached-body",
        ),
        (
            "page_session_opened",
            Event,
            ConsumerToService,
            "page-session-lifecycle-body",
        ),
        (
            "page_session_closed",
            Event,
            ConsumerToService,
            "page-session-lifecycle-body",
        ),
        (
            "capability_input",
            Event,
            ConsumerToService,
            "capability-input-body",
        ),
        (
            "action_lifecycle_rejected",
            Event,
            ServiceToConsumer,
            "action-lifecycle-rejected-body",
        ),
        (
            "binding_output",
            Command,
            ServiceToConsumer,
            "binding-output-body",
        ),
        (
            "binding_overlay",
            Command,
            ServiceToConsumer,
            "binding-overlay-body",
        ),
        (
            "binding_overlay_clear",
            Command,
            ServiceToConsumer,
            "binding-overlay-clear-body",
        ),
        ("open_page", Command, ServiceToConsumer, "open-page-body"),
        (
            "replace_page",
            Command,
            ServiceToConsumer,
            "replace-page-body",
        ),
        (
            "close_page",
            Command,
            ServiceToConsumer,
            "empty-action-body",
        ),
    ];
    if protocol.messages.len() != messages.len() || protocol.operations.len() != messages.len() {
        return Err(Error::Invalid(
            "Action Runtime requires the fourteen core operations and messages".into(),
        ));
    }
    for (name, intent, direction, schema) in messages {
        let definition = protocol
            .messages
            .get(name)
            .ok_or_else(|| Error::Invalid(format!("missing Action Runtime message {name}")))?;
        let (payload, other) = if intent == Event {
            (&definition.event_schema, &definition.params_schema)
        } else {
            (&definition.params_schema, &definition.event_schema)
        };
        let schema_id = format!("https://kws.github.io/deckr/contract/v1/schemas/deckr/action-runtime/{schema}.v1.schema.json");
        if definition.intent != intent
            || definition.direction != direction
            || definition.exchange_pattern != ExchangePattern::OneWay
            || definition.operation.as_deref() != Some(name)
            || !protocol.operations.contains_key(name)
            || other.is_some()
            || definition.result_schema.is_some()
            || definition.error_schema.is_some()
            || !payload.as_ref().is_some_and(|payload| {
                payload.schema_id == schema_id
                    && payload.schema == serde_json::json!({"$ref":schema_id})
            })
        {
            return Err(Error::Invalid(format!(
                "Action Runtime message {name} changes the core declaration"
            )));
        }
    }
    if protocol.view_families.len() != 1
        || !protocol
            .view_families
            .get(ACTION_AVAILABILITY_FAMILY)
            .is_some_and(|view| {
                view.store_name == ACTION_AVAILABILITY_STORE
                    && view.writer == ServiceViewWriter::Service
            })
    {
        return Err(Error::Invalid(
            "Action Runtime requires the service-owned availability family".into(),
        ));
    }
    Ok(())
}
