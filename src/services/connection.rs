//! Scoped messages and view access for one accepted service-use agreement.

use std::{future::Future, sync::Arc, time::Duration};

use crate::authority::ContractPointer;
use crate::lanes::{DeckrMessage, EntitySubject, MessageTarget, SERVICES_LANE};
use crate::lifecycle::{Cancellation, Phase};
use crate::messaging::{
    Delivery, MessageAuthority, MessageSession, MessageTransport, ReplyAcceptance,
};
use crate::schema::CompiledSchema;
use crate::state::StateStore;
use crate::{Error, Result};

use super::*;

struct Scope<C: StateStore, T: StateStore, M: MessageTransport> {
    context: ServiceContext<C, T>,
    messages: MessageSession<M>,
    stop: Cancellation,
}
/// Retained connections always name the same agreement and both sessions. They
/// never acquire a managed client's successor agreement or replay its commands.
pub struct ServiceConnection<C: StateStore, T: StateStore, M: MessageTransport>(
    Arc<Scope<C, T, M>>,
);
impl<C: StateStore, T: StateStore, M: MessageTransport> Clone for ServiceConnection<C, T, M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> ServiceConnection<C, T, M> {
    pub fn new(context: ServiceContext<C, T>, messages: MessageSession<M>) -> Result<Self> {
        Self::with_stop(context, messages, Cancellation::default())
    }
    pub(crate) fn with_stop(
        context: ServiceContext<C, T>,
        messages: MessageSession<M>,
        stop: Cancellation,
    ) -> Result<Self> {
        if messages.session().endpoint() != context.agreement().session().endpoint()
            || messages.session().id() != context.agreement().session().id()
        {
            return Err(Error::Invalid(
                "service messaging and agreement must use the same endpoint session".into(),
            ));
        }
        let connection = Self(Arc::new(Scope {
            context,
            messages,
            stop,
        }));
        connection.authorize()?;
        Ok(connection)
    }
    pub fn context(&self) -> &ServiceContext<C, T> {
        &self.0.context
    }
    pub fn authorize(&self) -> Result<()> {
        if self.0.stop.is_cancelled()
            || matches!(
                self.0.messages.state().phase,
                Phase::Closing | Phase::Closed
            )
        {
            return Err(Error::Closed("service connection closed".into()));
        }
        self.context().authorize()
    }
    fn direction(&self) -> ServiceMessageDirection {
        match self.context().local_role() {
            ServiceViewWriter::Consumer => ServiceMessageDirection::ConsumerToService,
            ServiceViewWriter::Service => ServiceMessageDirection::ServiceToConsumer,
        }
    }
    fn incoming_direction(&self) -> ServiceMessageDirection {
        match self.direction() {
            ServiceMessageDirection::ConsumerToService => {
                ServiceMessageDirection::ServiceToConsumer
            }
            _ => ServiceMessageDirection::ConsumerToService,
        }
    }
    /// Builds the owned envelope with the service ID, namespace and message name
    /// as subject identifiers. Owners may add application identifiers, trace data
    /// and expiry before calling `send` or `request`.
    pub fn message(&self, body: ServiceMessageBody) -> Result<DeckrMessage> {
        self.authorize()?;
        let context = self.context();
        let session = context.agreement().session();
        let (peer, peer_session) = match context.local_role() {
            ServiceViewWriter::Consumer => (
                &context.descriptor().endpoint,
                context.descriptor().session_id.as_str(),
            ),
            ServiceViewWriter::Service => (context.consumer(), context.consumer_session_id()),
        };
        let contract = context.agreement().contract();
        Ok(DeckrMessage {
            message_id: uuid::Uuid::new_v4().to_string(),
            protocol_version: "1".into(),
            schema_version: "1".into(),
            lane: SERVICES_LANE.into(),
            message_type: "serviceMessage".into(),
            sender: session.endpoint().to_string(),
            sender_session_id: session.id().into(),
            recipient: MessageTarget::Endpoint {
                endpoint: peer.to_string(),
            },
            recipient_session_id: Some(peer_session.into()),
            contract: Some(ContractPointer {
                contract_id: contract.contract_id.clone(),
                generation: contract.generation,
            }),
            subject: EntitySubject {
                kind: "service".into(),
                identifiers: [
                    ("serviceId".into(), context.descriptor().service_id.clone()),
                    ("namespace".into(), context.descriptor().namespace.clone()),
                    ("name".into(), body.name.clone()),
                ]
                .into(),
            },
            created_at: context.agreement().runtime_clock().utc_now().to_rfc3339(),
            expires_at: None,
            ttl_ms: None,
            in_reply_to: None,
            causation_id: None,
            trace: None,
            body: body.to_value()?,
        })
    }
    pub async fn send(&self, message: DeckrMessage) -> Result<()> {
        self.send_checked(message, None).await
    }
    /// Compose a protocol-specific final fence with the service fence. Kept
    /// internal so callers cannot replace the connection's own authority.
    pub(crate) async fn send_with_authority(
        &self,
        message: DeckrMessage,
        additional: &dyn MessageAuthority,
    ) -> Result<()> {
        self.send_checked(message, Some(additional)).await
    }
    async fn send_checked(
        &self,
        message: DeckrMessage,
        additional: Option<&dyn MessageAuthority>,
    ) -> Result<()> {
        self.while_current(async {
            if message.in_reply_to.is_some()
                || message.service_body()?.exchange_pattern != ExchangePattern::OneWay
            {
                return Err(Error::Invalid(
                    "use request or reply for service request/reply messages".into(),
                ));
            }
            self.context().validate_message(&message, true).await?;
            if let Some(additional) = additional {
                let authority = AdditionalAuthority {
                    connection: self,
                    additional,
                };
                self.0.messages.send_authorized(message, &authority).await
            } else {
                self.0.messages.send_authorized(message, self).await
            }
        })
        .await
    }
    pub async fn request(
        &self,
        mut message: DeckrMessage,
        timeout: Duration,
        accept: Option<ReplyAcceptance>,
    ) -> Result<DeckrMessage> {
        self.while_current(async {
            let request = message.service_body()?;
            if message.in_reply_to.is_some()
                || request.exchange_pattern != ExchangePattern::RequestReply
            {
                return Err(Error::Invalid(
                    "service request requires an initial request/reply message".into(),
                ));
            }
            if message.ttl_ms.is_none() && message.expires_at.is_none() {
                message.ttl_ms =
                    Some(u64::try_from(timeout.as_millis()).map_err(|_| {
                        Error::Invalid("service request timeout is too large".into())
                    })?);
            }
            self.context().validate_message(&message, true).await?;
            let protocol = self.context().declaration().clone();
            let direction = self.incoming_direction();
            let predicate: ReplyAcceptance = Arc::new(move |reply| {
                reply
                    .service_body()
                    .is_ok_and(|body| protocol.validate_reply(&request, &body, direction).is_ok())
                    && accept.as_ref().is_none_or(|accept| accept(reply))
            });
            self.0
                .messages
                .request_authorized(message, timeout, Some(predicate), self)
                .await
        })
        .await
    }
    pub async fn reply(&self, request: &Delivery, body: ServiceMessageBody) -> Result<()> {
        self.while_current(async {
            self.authorize_incoming(&request.message)?;
            self.context()
                .validate_message(&request.message, false)
                .await?;
            if request.message.in_reply_to.is_some() {
                return Err(Error::Invalid(
                    "a service reply cannot start another reply exchange".into(),
                ));
            }
            let request_body = request.message.service_body()?;
            let protocol = self.context().declaration().clone();
            let direction = self.direction();
            let response = body.clone();
            tokio::task::spawn_blocking(move || {
                protocol.validate_reply(&request_body, &response, direction)
            })
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("service reply validator failed: {error}"))
            })??;
            let mut reply = self.message(body)?;
            reply.subject = request.message.subject.clone();
            reply.in_reply_to = Some(request.message.message_id.clone());
            reply.causation_id = Some(request.message.message_id.clone());
            reply.trace = request.message.trace.clone();
            self.context().validate_message(&reply, true).await?;
            self.0.messages.reply_authorized(request, reply, self).await
        })
        .await
    }
    pub fn view<S: StateStore>(
        &self,
        store: &ServiceViewStore<S>,
        family: &str,
        schema: Option<CompiledSchema>,
    ) -> Result<ProtectedServiceView<S, C, T>> {
        self.authorize()?;
        store.bind(self.context().clone(), family, schema)
    }
    pub async fn while_current<R>(&self, work: impl Future<Output = Result<R>>) -> Result<R> {
        self.authorize()?;
        let result = tokio::select! {biased;
            _=self.0.stop.cancelled()=>Err(Error::Closed("service connection closed".into())),
            result=self.context().while_current(work)=>result,
        }?;
        self.authorize()?;
        Ok(result)
    }
}
struct AdditionalAuthority<'a, C: StateStore, T: StateStore, M: MessageTransport> {
    connection: &'a ServiceConnection<C, T, M>,
    additional: &'a dyn MessageAuthority,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> MessageAuthority
    for AdditionalAuthority<'_, C, T, M>
{
    fn authorize_outgoing(&self, message: &DeckrMessage) -> Result<()> {
        self.connection.authorize_outgoing(message)?;
        self.additional.authorize_outgoing(message)
    }
    fn authorize_incoming(&self, message: &DeckrMessage) -> Result<()> {
        self.connection.authorize_incoming(message)?;
        self.additional.authorize_incoming(message)
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> MessageAuthority
    for ServiceConnection<C, T, M>
{
    fn authorize_outgoing(&self, message: &DeckrMessage) -> Result<()> {
        self.authorize()?;
        self.context().authorize_outgoing(message)
    }
    fn authorize_incoming(&self, message: &DeckrMessage) -> Result<()> {
        self.authorize()?;
        if message.is_expired_at(self.context().agreement().runtime_clock().utc_now()) {
            return Err(Error::Unauthorized("service message expired".into()));
        }
        self.context().authorize_incoming(message)
    }
}
pub struct ServiceIncoming<C: StateStore, T: StateStore, M: MessageTransport> {
    pub delivery: Delivery,
    pub connection: ServiceConnection<C, T, M>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Clone for ServiceIncoming<C, T, M> {
    fn clone(&self) -> Self {
        Self {
            delivery: self.delivery.clone(),
            connection: self.connection.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> ServiceIncoming<C, T, M> {
    pub fn authorize(&self) -> Result<()> {
        self.connection.authorize_incoming(&self.delivery.message)
    }
    pub async fn reply(&self, body: ServiceMessageBody) -> Result<()> {
        self.connection.reply(&self.delivery, body).await
    }
}
