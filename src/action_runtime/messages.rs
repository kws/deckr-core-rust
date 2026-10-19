use super::*;
use crate::lanes::{DeckrMessage, MessageTarget, SERVICES_LANE};
use crate::services::{
    ExchangePattern, ServiceIntent, ServiceMessageBody, ServiceMessageDirection,
};
use crate::{Error, Result};
use serde_json::{Map, Value};

/// Typed operations from the upstream Action Runtime v1 catalog. This is a
/// payload representation, not a routing table or permission to allocate IDs.
#[derive(Debug, Clone, PartialEq)]
pub enum ActionMessage {
    ActionInstanceCreated(Box<ActionInstanceLifecycle>),
    ActionInstanceDestroyed(Box<ActionInstanceLifecycle>),
    BindingAttached(Box<BindingAttached>),
    BindingDetached(Box<BindingDetached>),
    PageSessionOpened(Box<PageSessionLifecycle>),
    PageSessionClosed(Box<PageSessionLifecycle>),
    CapabilityInput(Box<CapabilityInput>),
    ActionLifecycleRejected(Box<ActionLifecycleRejected>),
    BindingOutput(Box<BindingOutput>),
    BindingOverlay(Box<BindingOverlay>),
    BindingOverlayClear(Box<BindingOverlayClear>),
    OpenPage(Box<DynamicPageBody>),
    ReplacePage(Box<DynamicPageBody>),
    ClosePage,
}
impl ActionMessage {
    pub fn name(&self) -> &'static str {
        match self {
            Self::ActionInstanceCreated(_) => "action_instance_created",
            Self::ActionInstanceDestroyed(_) => "action_instance_destroyed",
            Self::BindingAttached(_) => "binding_attached",
            Self::BindingDetached(_) => "binding_detached",
            Self::PageSessionOpened(_) => "page_session_opened",
            Self::PageSessionClosed(_) => "page_session_closed",
            Self::CapabilityInput(_) => "capability_input",
            Self::ActionLifecycleRejected(_) => "action_lifecycle_rejected",
            Self::BindingOutput(_) => "binding_output",
            Self::BindingOverlay(_) => "binding_overlay",
            Self::BindingOverlayClear(_) => "binding_overlay_clear",
            Self::OpenPage(_) => "open_page",
            Self::ReplacePage(_) => "replace_page",
            Self::ClosePage => "close_page",
        }
    }
    pub fn direction(&self) -> ServiceMessageDirection {
        match self {
            Self::ActionInstanceCreated(_)
            | Self::ActionInstanceDestroyed(_)
            | Self::BindingAttached(_)
            | Self::BindingDetached(_)
            | Self::PageSessionOpened(_)
            | Self::PageSessionClosed(_)
            | Self::CapabilityInput(_) => ServiceMessageDirection::ConsumerToService,
            _ => ServiceMessageDirection::ServiceToConsumer,
        }
    }
    pub fn intent(&self) -> ServiceIntent {
        match self {
            Self::BindingOutput(_)
            | Self::BindingOverlay(_)
            | Self::BindingOverlayClear(_)
            | Self::OpenPage(_)
            | Self::ReplacePage(_)
            | Self::ClosePage => ServiceIntent::Command,
            _ => ServiceIntent::Event,
        }
    }
    fn payload(&self) -> Result<Value> {
        match self {
            Self::ActionInstanceCreated(body) | Self::ActionInstanceDestroyed(body) => {
                body.to_value()
            }
            Self::BindingAttached(body) => body.to_value(),
            Self::BindingDetached(body) => body.to_value(),
            Self::PageSessionOpened(body) | Self::PageSessionClosed(body) => body.to_value(),
            Self::CapabilityInput(body) => body.to_value(),
            Self::ActionLifecycleRejected(body) => body.to_value(),
            Self::BindingOutput(body) => body.to_value(),
            Self::BindingOverlay(body) => body.to_value(),
            Self::BindingOverlayClear(body) => body.to_value(),
            Self::OpenPage(body) | Self::ReplacePage(body) => body.to_value(),
            Self::ClosePage => Ok(Value::Object(Map::new())),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.payload().map(|_| ())
    }
    pub fn to_body(&self) -> Result<ServiceMessageBody> {
        let payload = self.payload()?;
        let mut body = serde_json::json!({"serviceNamespace":ACTION_RUNTIME_NAMESPACE,"name":self.name(),"intent":self.intent(),"exchangePattern":"one_way","params":{}});
        body[if self.intent() == ServiceIntent::Event {
            "event"
        } else {
            "params"
        }] = payload;
        ServiceMessageBody::from_value(body)
    }
    pub fn from_body(body: &ServiceMessageBody) -> Result<Self> {
        body.validate()?;
        if body.service_namespace != ACTION_RUNTIME_NAMESPACE
            || body.exchange_pattern != ExchangePattern::OneWay
            || body.status.is_some()
            || body.result.is_some()
            || body.error.is_some()
        {
            return Err(Error::Invalid(
                "Action Runtime requires its one-way namespace without reply payloads".into(),
            ));
        }
        let payload = match body.intent {
            ServiceIntent::Event if body.params.is_empty() => {
                Value::Object(body.event.clone().ok_or_else(|| {
                    Error::Invalid("Action Runtime event payload is required".into())
                })?)
            }
            ServiceIntent::Command if body.event.is_none() => Value::Object(body.params.clone()),
            _ => {
                return Err(Error::Invalid(
                    "Action Runtime payload position differs from its intent".into(),
                ))
            }
        };
        let action = match body.name.as_str() {
            "action_instance_created" => {
                Self::ActionInstanceCreated(Box::new(ActionInstanceLifecycle::from_value(payload)?))
            }
            "action_instance_destroyed" => Self::ActionInstanceDestroyed(Box::new(
                ActionInstanceLifecycle::from_value(payload)?,
            )),
            "binding_attached" => {
                Self::BindingAttached(Box::new(BindingAttached::from_value(payload)?))
            }
            "binding_detached" => {
                Self::BindingDetached(Box::new(BindingDetached::from_value(payload)?))
            }
            "page_session_opened" => {
                Self::PageSessionOpened(Box::new(PageSessionLifecycle::from_value(payload)?))
            }
            "page_session_closed" => {
                Self::PageSessionClosed(Box::new(PageSessionLifecycle::from_value(payload)?))
            }
            "capability_input" => {
                Self::CapabilityInput(Box::new(CapabilityInput::from_value(payload)?))
            }
            "action_lifecycle_rejected" => Self::ActionLifecycleRejected(Box::new(
                ActionLifecycleRejected::from_value(payload)?,
            )),
            "binding_output" => Self::BindingOutput(Box::new(BindingOutput::from_value(payload)?)),
            "binding_overlay" => {
                Self::BindingOverlay(Box::new(BindingOverlay::from_value(payload)?))
            }
            "binding_overlay_clear" => {
                Self::BindingOverlayClear(Box::new(BindingOverlayClear::from_value(payload)?))
            }
            "open_page" => Self::OpenPage(Box::new(DynamicPageBody::from_value(payload)?)),
            "replace_page" => Self::ReplacePage(Box::new(DynamicPageBody::from_value(payload)?)),
            "close_page" => {
                EmptyActionBody::from_value(payload)?;
                Self::ClosePage
            }
            _ => return Err(Error::Unsupported("unknown Action Runtime message".into())),
        };
        if action.intent() != body.intent {
            return Err(Error::Invalid(
                "Action Runtime message has the wrong intent".into(),
            ));
        }
        Ok(action)
    }
    /// Validate the wire shape and endpoint roles. This does not establish
    /// current Concord authority or resolve controller-owned references.
    pub fn from_envelope(message: &DeckrMessage) -> Result<Self> {
        message.validate()?;
        if message.lane != SERVICES_LANE
            || message.message_type != "serviceMessage"
            || message.contract.is_none()
            || message.recipient_session_id.is_none()
            || message.in_reply_to.is_some()
        {
            return Err(Error::Invalid(
                "Action Runtime requires a direct, session-targeted service agreement message"
                    .into(),
            ));
        }
        let MessageTarget::Endpoint { endpoint } = &message.recipient else {
            return Err(Error::Invalid(
                "Action Runtime cannot use broadcast delivery".into(),
            ));
        };
        let action = Self::from_body(&message.service_body()?)?;
        let (controller, runtime) =
            if action.direction() == ServiceMessageDirection::ConsumerToService {
                (&message.sender, endpoint)
            } else {
                (endpoint, &message.sender)
            };
        if !controller.starts_with("controller:") {
            return Err(Error::Unauthorized(
                "Action Runtime consumer must be a controller".into(),
            ));
        }
        let instance = runtime
            .strip_prefix("service:action-runtime.")
            .ok_or_else(|| {
                Error::Unauthorized("Action Runtime endpoint has the wrong service identity".into())
            })?;
        validate_provider_instance_id(instance)?;
        Ok(action)
    }
    /// Compare the payload's provider identity with a held runtime context.
    /// Page commands carry no such identity; their opener comes from owner state.
    pub fn validate_provider(&self, instance: &str, provider: &str) -> Result<()> {
        validate_provider_instance_id(instance)?;
        text(provider)?;
        self.validate()?;
        let identity = match self {
            Self::ActionInstanceCreated(body) | Self::ActionInstanceDestroyed(body) => Some((
                &body.metadata.provider_instance_id,
                &body.metadata.provider_id,
            )),
            Self::BindingAttached(body) => Some((
                &body.binding.provider_instance_id,
                &body.binding.provider_id,
            )),
            Self::BindingDetached(body) => Some((
                &body.binding.provider_instance_id,
                &body.binding.provider_id,
            )),
            Self::PageSessionOpened(body) | Self::PageSessionClosed(body) => Some((
                &body.page_session.provider_instance_id,
                &body.page_session.provider_id,
            )),
            Self::CapabilityInput(body) => Some((
                &body.binding.provider_instance_id,
                &body.binding.provider_id,
            )),
            Self::BindingOutput(body) => Some((
                &body.binding.provider_instance_id,
                &body.binding.provider_id,
            )),
            Self::BindingOverlay(body) => Some((
                &body.binding.provider_instance_id,
                &body.binding.provider_id,
            )),
            Self::BindingOverlayClear(body) => Some((
                &body.binding.provider_instance_id,
                &body.binding.provider_id,
            )),
            Self::ActionLifecycleRejected(body) => match body.target_kind {
                LifecycleTargetKind::ActionInstance => body
                    .action_instance
                    .as_ref()
                    .map(|target| (&target.provider_instance_id, &target.provider_id)),
                LifecycleTargetKind::Binding => body
                    .binding
                    .as_ref()
                    .map(|target| (&target.provider_instance_id, &target.provider_id)),
                LifecycleTargetKind::PageSession => body
                    .page_session
                    .as_ref()
                    .map(|target| (&target.provider_instance_id, &target.provider_id)),
            },
            _ => None,
        };
        if identity.is_some_and(|(actual_instance, actual_provider)| {
            actual_instance != instance || actual_provider != provider
        }) {
            return Err(Error::Unauthorized(
                "action payload names another provider".into(),
            ));
        }
        Ok(())
    }
}
