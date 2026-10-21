//! Action messages on an immutable managed service connection.

use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

use super::*;
use crate::authority::ContractPointer;
use crate::endpoint::EndpointAddress;
use crate::lanes::DeckrMessage;
use crate::messaging::{MessageAuthority, MessageTransport};
use crate::services::{ServiceConnection, ServiceIncoming, ServiceViewWriter};
use crate::state::StateStore;
use crate::{Error, Result};

/// Exact session and agreement keys for looking up owner-managed references.
/// An ID resolved in another scope does not authorize this scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionScope {
    pub contract: ContractPointer,
    pub provider_instance_id: String,
    pub provider_id: String,
    pub service_endpoint: EndpointAddress,
    pub service_session_id: String,
    pub controller_endpoint: EndpointAddress,
    pub controller_session_id: String,
    pub local_role: ServiceViewWriter,
}

/// The owner's live reference check, composed with service authority and the
/// intrinsic Action Runtime checks. Required; there is no permissive default.
///
/// Resolve action, binding and page IDs in `scope`, check current generations
/// and selected resources using the routing helpers, and reject unresolved IDs.
/// Controller lifecycle events may introduce controller-assigned IDs according
/// to the receiving runtime's lifecycle policy. Runtime commands must never
/// allocate bindings or turn an unknown ID into current authority.
/// Use `validate_render_command` with current observations for render commands.
/// Render results describe past requests, including detached or stale targets;
/// they require current service authority but not a current target binding.
///
/// Page commands have no wire opener ID. The owner must resolve their opener
/// and current page from its authorized scope/subject, or reject ambiguity;
/// use `validate_page_children` for selected children. Do not invent wire fields.
///
/// Called synchronously before dispatch and immediately before publication,
/// after asynchronous schema validation. It must be bounded, must read current
/// state, and must not mutate state or block. It can run more than once per
/// message. Panics fail the operation without terminating the service agreement.
/// Hardware authority remains a separate check when the controller sends through
/// its current hardware client; this callback cannot confer a hardware claim.
pub trait ActionReferenceAuthority: Send + Sync {
    fn authorize(
        &self,
        scope: &ActionScope,
        envelope: &DeckrMessage,
        action: &ActionMessage,
    ) -> Result<()>;
}

/// Adds Action Runtime semantics to a managed service connection. It borrows
/// the same lifecycle, current agreement and sender; it starts no workers and
/// owns no controller state. Retained handles never acquire a successor scope.
pub struct ActionConnection<C: StateStore, T: StateStore, M: MessageTransport> {
    service: ServiceConnection<C, T, M>,
    scope: Arc<ActionScope>,
    references: Arc<dyn ActionReferenceAuthority>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Clone for ActionConnection<C, T, M> {
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            scope: self.scope.clone(),
            references: self.references.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> ActionConnection<C, T, M> {
    /// `provider_id` is the logical provider selected from trusted local
    /// configuration or authorized availability. The instance and both session
    /// identities are derived from the held service context.
    pub fn new(
        service: ServiceConnection<C, T, M>,
        provider_id: impl Into<String>,
        references: Arc<dyn ActionReferenceAuthority>,
    ) -> Result<Self> {
        service.authorize()?;
        let context = service.context();
        validate_action_runtime_protocol(context.declaration().protocol())?;
        let descriptor = context.descriptor();
        let instance = descriptor
            .service_id
            .strip_prefix("action-runtime.")
            .ok_or_else(|| {
                Error::Unauthorized("Action Runtime service ID has the wrong prefix".into())
            })?;
        validate_provider_instance_id(instance)?;
        if descriptor.endpoint != action_runtime_endpoint(instance)?
            || context.consumer().family() != "controller"
        {
            return Err(Error::Unauthorized(
                "Action Runtime requires its service endpoint and a controller consumer".into(),
            ));
        }
        let provider_id = provider_id.into();
        text(&provider_id)?;
        let contract = context.agreement().contract();
        let scope = Arc::new(ActionScope {
            contract: ContractPointer {
                contract_id: contract.contract_id.clone(),
                generation: contract.generation,
            },
            provider_instance_id: instance.into(),
            provider_id,
            service_endpoint: descriptor.endpoint.clone(),
            service_session_id: descriptor.session_id.clone(),
            controller_endpoint: context.consumer().clone(),
            controller_session_id: context.consumer_session_id().into(),
            local_role: context.local_role(),
        });
        Ok(Self {
            service,
            scope,
            references,
        })
    }
    pub fn scope(&self) -> &ActionScope {
        &self.scope
    }
    pub fn service(&self) -> &ServiceConnection<C, T, M> {
        &self.service
    }
    pub fn authorize(&self) -> Result<()> {
        self.service.authorize()
    }

    /// Build an owned service envelope. The owner may add its subject identifiers
    /// and expiry before `send`; reference resolution occurs at send time.
    pub fn message(&self, action: &ActionMessage) -> Result<DeckrMessage> {
        action.validate_provider(&self.scope.provider_instance_id, &self.scope.provider_id)?;
        let mut message = self.service.message(action.to_body()?)?;
        if let ActionMessage::BindingRenderResult(body) = action {
            message.causation_id = Some(body.request_message_id.clone());
        }
        ActionMessage::from_envelope(&message)?;
        // Scope/direction checks, without requiring page subject information
        // that the caller has not yet had a chance to add.
        self.service.authorize_outgoing(&message)?;
        Ok(message)
    }
    pub async fn send(&self, message: DeckrMessage) -> Result<()> {
        self.authorize_outgoing(&message)?;
        self.service.send_with_authority(message, self).await
    }
    pub async fn send_action(&self, action: &ActionMessage) -> Result<()> {
        self.send(self.message(action)?).await
    }
    /// Validate before invoking application behavior. Both managed client
    /// readers and provider backends supply `ServiceIncoming`. No subscription
    /// or additional authority registry is created here.
    pub async fn accept(
        &self,
        incoming: ServiceIncoming<C, T, M>,
    ) -> Result<ActionIncoming<C, T, M>> {
        self.service
            .while_current(async {
                incoming.authorize()?;
                self.authorize_incoming(&incoming.delivery.message)?;
                self.service
                    .context()
                    .validate_message(&incoming.delivery.message, false)
                    .await?;
                let action = ActionMessage::from_envelope(&incoming.delivery.message)?;
                let accepted = ActionIncoming {
                    incoming,
                    connection: self.clone(),
                    action,
                };
                // The owner's current reference may have changed while validating
                // the compiled schema above. Recheck after the last await.
                accepted.authorize()?;
                Ok(accepted)
            })
            .await
    }
    /// Check the logical identity of a payload read through `ActionAvailability`.
    /// This does not replace that view's record fences or establish provenance
    /// for an arbitrary payload supplied by the caller.
    pub fn validate_availability(&self, availability: &ActionAvailabilityPayload) -> Result<()> {
        self.authorize()?;
        availability.validate()?;
        if availability.provider_instance_id != self.scope.provider_instance_id
            || availability.provider_id != self.scope.provider_id
            || availability.service_endpoint != self.scope.service_endpoint
            || availability.service_session_id != self.scope.service_session_id
        {
            return Err(Error::Unauthorized(
                "availability differs from the held Action Runtime identity".into(),
            ));
        }
        Ok(())
    }
    fn check_references(&self, message: &DeckrMessage) -> Result<()> {
        let action = ActionMessage::from_envelope(message)?;
        action.validate_provider(&self.scope.provider_instance_id, &self.scope.provider_id)?;
        catch_unwind(AssertUnwindSafe(|| {
            self.references.authorize(&self.scope, message, &action)
        }))
        .map_err(|_| {
            Error::StateUnavailable("Action Runtime reference authority panicked".into())
        })?
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> MessageAuthority
    for ActionConnection<C, T, M>
{
    fn authorize_outgoing(&self, message: &DeckrMessage) -> Result<()> {
        self.service.authorize_outgoing(message)?;
        self.check_references(message)
    }
    fn authorize_incoming(&self, message: &DeckrMessage) -> Result<()> {
        self.service.authorize_incoming(message)?;
        self.check_references(message)
    }
}

/// An accepted message with immutable payload and its original exact scope.
/// Recheck `authorize` immediately before an effect after awaiting other work.
/// Acceptance is not a hardware command or a reusable hardware permission.
pub struct ActionIncoming<C: StateStore, T: StateStore, M: MessageTransport> {
    incoming: ServiceIncoming<C, T, M>,
    connection: ActionConnection<C, T, M>,
    action: ActionMessage,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Clone for ActionIncoming<C, T, M> {
    fn clone(&self) -> Self {
        Self {
            incoming: self.incoming.clone(),
            connection: self.connection.clone(),
            action: self.action.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> ActionIncoming<C, T, M> {
    pub fn message(&self) -> &DeckrMessage {
        &self.incoming.delivery.message
    }
    pub fn action(&self) -> &ActionMessage {
        &self.action
    }
    pub fn connection(&self) -> &ActionConnection<C, T, M> {
        &self.connection
    }
    pub fn authorize(&self) -> Result<()> {
        self.incoming.authorize()?;
        self.connection.authorize_incoming(self.message())
    }
}
