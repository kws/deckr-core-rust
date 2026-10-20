//! Exact agreement and local access scope shared by managed service boundaries.

use std::future::Future;
use std::sync::Arc;

use futures_util::StreamExt;

use crate::authority::AgreementMessageGuard;
use crate::concord::{AgreementContext, ContractRecord};
use crate::endpoint::EndpointAddress;
use crate::lanes::{DeckrMessage, SERVICES_LANE};
use crate::messaging::MessageAuthority;
use crate::state::StateStore;
use crate::{Error, Result};

use super::{
    service_access, ServiceAccess, ServiceDescriptor, ServiceMessageDirection, ServiceMessageKind,
    ServiceViewFamily, ServiceViewWriter, ValidatedServiceProtocol,
};

/// Apply the service-use profile to a generic Concord record. This is also
/// usable during provider admission, before either side has local authority.
pub fn validate_service_use(
    record: &ContractRecord,
    service: &EndpointAddress,
    consumer: &EndpointAddress,
    profile: &str,
) -> Result<()> {
    record.validate()?;
    EndpointAddress::parse(service.as_str())?;
    EndpointAddress::parse(consumer.as_str())?;
    if service.family() != crate::endpoint::SERVICE_FAMILY
        || service == consumer
        || record.profile.as_deref() != Some(profile)
        || record.terms.is_some()
        || record.terms_hash.is_some()
        || record.participants.len() != 2
        || !record.participants.contains(service)
        || !record.participants.contains(consumer)
    {
        return Err(Error::Unauthorized(
            "service-use requires a termless agreement between the selected service and consumer"
                .into(),
        ));
    }
    Ok(())
}

/// Validate a wire/store record, including the requirement that `terms` and
/// `termsHash` are absent (explicit null is not omission in the service profile).
pub fn validate_service_use_record(
    value: &serde_json::Value,
    service: &EndpointAddress,
    consumer: &EndpointAddress,
    profile: &str,
) -> Result<ContractRecord> {
    if value.get("terms").is_some() || value.get("termsHash").is_some() {
        return Err(Error::Unauthorized(
            "service-use records must omit terms and termsHash".into(),
        ));
    }
    let record = ContractRecord::from_value(value.clone())?;
    validate_service_use(&record, service, consumer, profile)?;
    Ok(record)
}

struct Scope<C: StateStore, T: StateStore> {
    guard: AgreementMessageGuard<C, T>,
    declaration: ValidatedServiceProtocol,
    descriptor: ServiceDescriptor,
    consumer: EndpointAddress,
    consumer_session: String,
    access: ServiceAccess,
}

/// Immutable service identity, participant sessions and local permissions under
/// one accepted agreement. This context never follows discovery replacements
/// or successor agreements. Generic message guards perform metadata/authority
/// checks; use `validate_message` for asynchronous payload-schema validation.
pub struct ServiceContext<C: StateStore, T: StateStore>(Arc<Scope<C, T>>);
impl<C: StateStore, T: StateStore> Clone for ServiceContext<C, T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<C: StateStore, T: StateStore> ServiceContext<C, T> {
    pub fn new(
        agreement: AgreementContext<C, T>,
        declaration: ValidatedServiceProtocol,
        descriptor: ServiceDescriptor,
        access: ServiceAccess,
    ) -> Result<Self> {
        descriptor.validate(declaration.protocol())?;
        if access.service_endpoint != descriptor.endpoint
            || access.service_namespace != descriptor.namespace
            || access.service_session_id != descriptor.session_id
        {
            return Err(Error::Unauthorized(
                "service access identifies a different candidate".into(),
            ));
        }
        let access = service_access(&descriptor, access.allowed_operations, access.allowed_views)?;
        let validity = agreement.authorized_validity()?;
        let record = validity
            .contract
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("service agreement record is missing".into()))?;
        let consumer = record
            .participants
            .iter()
            .find(|participant| **participant != descriptor.endpoint)
            .cloned()
            .ok_or_else(|| Error::Unauthorized("service agreement consumer is missing".into()))?;
        validate_service_use(
            record,
            &descriptor.endpoint,
            &consumer,
            &descriptor.use_profile,
        )?;
        validate_service_use_record(
            &agreement.authorized_record()?.value,
            &descriptor.endpoint,
            &consumer,
            &descriptor.use_profile,
        )?;
        if validity
            .tokens
            .get(descriptor.endpoint.as_str())
            .is_none_or(|token| token.session_id != descriptor.session_id)
        {
            return Err(Error::Unauthorized(
                "service descriptor session differs from the agreement".into(),
            ));
        }
        let consumer_session = validity
            .tokens
            .get(consumer.as_str())
            .ok_or_else(|| Error::Unauthorized("service consumer token is missing".into()))?
            .session_id
            .clone();
        let guard = AgreementMessageGuard::new(agreement, SERVICES_LANE, &descriptor.use_profile)?;
        let context = Self(Arc::new(Scope {
            guard,
            declaration,
            descriptor,
            consumer,
            consumer_session,
            access,
        }));
        context.authorize()?;
        Ok(context)
    }
    pub fn agreement(&self) -> &AgreementContext<C, T> {
        self.0.guard.agreement()
    }
    pub fn descriptor(&self) -> &ServiceDescriptor {
        &self.0.descriptor
    }
    pub fn declaration(&self) -> &ValidatedServiceProtocol {
        &self.0.declaration
    }
    pub fn consumer(&self) -> &EndpointAddress {
        &self.0.consumer
    }
    pub fn consumer_session_id(&self) -> &str {
        &self.0.consumer_session
    }
    pub fn access(&self) -> &ServiceAccess {
        &self.0.access
    }
    pub fn local_role(&self) -> ServiceViewWriter {
        if self.agreement().session().endpoint() == &self.0.descriptor.endpoint {
            ServiceViewWriter::Service
        } else {
            ServiceViewWriter::Consumer
        }
    }
    pub fn authorize(&self) -> Result<()> {
        let result = self.check_authority();
        if result.is_err() {
            self.agreement()
                .request_close_with_reason("service profile authority ended");
        }
        result
    }
    fn check_authority(&self) -> Result<()> {
        let validity = self.agreement().authorized_validity()?;
        let record = validity
            .contract
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("service agreement record is missing".into()))?;
        validate_service_use(
            record,
            &self.0.descriptor.endpoint,
            &self.0.consumer,
            &self.0.descriptor.use_profile,
        )?;
        validate_service_use_record(
            &self.agreement().authorized_record()?.value,
            &self.0.descriptor.endpoint,
            &self.0.consumer,
            &self.0.descriptor.use_profile,
        )?;
        for (endpoint, session) in [
            (&self.0.descriptor.endpoint, &self.0.descriptor.session_id),
            (&self.0.consumer, &self.0.consumer_session),
        ] {
            if validity
                .tokens
                .get(endpoint.as_str())
                .is_none_or(|token| &token.session_id != session)
            {
                return Err(Error::Unauthorized(
                    "service context participant session has changed".into(),
                ));
            }
        }
        Ok(())
    }
    /// Static view permissions are local data, never Concord terms. Writers
    /// additionally need the role declared for this family. A protected store
    /// must still check every record's complete fence before exposing payloads.
    pub fn authorize_view(
        &self,
        family: &str,
        view_key: &str,
        writing: bool,
    ) -> Result<&ServiceViewFamily> {
        self.authorize()?;
        let view =
            self.0.descriptor.views.get(family).ok_or_else(|| {
                Error::Unauthorized("service view family is not advertised".into())
            })?;
        if !view_key.starts_with(&view.key_prefix)
            || !self
                .0
                .access
                .allowed_views
                .get(family)
                .is_some_and(|prefixes| prefixes.iter().any(|prefix| view_key.starts_with(prefix)))
            || (writing && view.writer != self.local_role())
        {
            return Err(Error::Unauthorized(
                "service view access is outside the selected scope or writer role".into(),
            ));
        }
        Ok(view)
    }
    fn metadata(
        &self,
        message: &DeckrMessage,
        outgoing: bool,
    ) -> Result<(ServiceMessageDirection, ServiceMessageKind)> {
        self.authorize()?;
        if outgoing {
            self.0.guard.authorize_outgoing(message)?;
        } else {
            self.0.guard.authorize_incoming(message)?;
        }
        let direction = if message.sender == self.0.consumer.as_str()
            && message.recipient_endpoint() == Some(self.0.descriptor.endpoint.as_str())
        {
            ServiceMessageDirection::ConsumerToService
        } else if message.sender == self.0.descriptor.endpoint.as_str()
            && message.recipient_endpoint() == Some(self.0.consumer.as_str())
        {
            ServiceMessageDirection::ServiceToConsumer
        } else {
            return Err(Error::Unauthorized(
                "service message must directly connect the selected service and consumer".into(),
            ));
        };
        if message.message_type != "serviceMessage" {
            return Err(Error::Invalid(
                "service context requires serviceMessage".into(),
            ));
        }
        let body = message.service_body()?;
        let identifiers = &message.subject.identifiers;
        if message.subject.kind != "service"
            || identifiers.get("serviceId") != Some(&self.0.descriptor.service_id)
            || identifiers.get("namespace") != Some(&self.0.descriptor.namespace)
            || identifiers.get("name") != Some(&body.name)
        {
            return Err(Error::Unauthorized(
                "service subject must match the selected service, namespace and message name"
                    .into(),
            ));
        }
        if !self
            .0
            .descriptor
            .supported_messages
            .contains_key(&body.name)
        {
            return Err(Error::Unsupported(
                "service does not advertise this declared message".into(),
            ));
        }
        let kind = if message.in_reply_to.is_some() {
            ServiceMessageKind::Reply
        } else {
            ServiceMessageKind::Initial
        };
        if self
            .0
            .declaration
            .validate_message_metadata(&body, direction, kind)?
            .is_some_and(|operation| {
                !self
                    .0
                    .access
                    .allowed_operations
                    .iter()
                    .any(|allowed| allowed == operation)
            })
        {
            return Err(Error::Unauthorized(
                "service operation is outside the selected access scope".into(),
            ));
        }
        self.authorize()?;
        Ok((direction, kind))
    }
    pub async fn validate_message(&self, message: &DeckrMessage, outgoing: bool) -> Result<()> {
        self.while_current(async {
            let (direction, kind) = self.metadata(message, outgoing)?;
            let body = message.service_body()?;
            let protocol = self.0.declaration.clone();
            tokio::task::spawn_blocking(move || {
                protocol
                    .validate_message(&body, direction, kind)
                    .map(|_| ())
            })
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("service payload validator failed: {error}"))
            })??;
            self.metadata(message, outgoing)?;
            Ok(())
        })
        .await
    }
    /// Keep awaited work bound to this exact agreement. The caller must still
    /// check application scope before an irreversible domain operation.
    pub async fn while_current<R>(&self, operation: impl Future<Output = Result<R>>) -> Result<R> {
        self.authorize()?;
        let mut changes = self.agreement().watch_state()?;
        tokio::pin!(operation);
        loop {
            tokio::select! { biased;
                changed = changes.next() => {
                    if changed.is_none() { return Err(Error::Closed("service agreement ended".into())); }
                    self.authorize()?;
                },
                result = &mut operation => { let value = result?; self.authorize()?; return Ok(value); },
            }
        }
    }
}
impl<C: StateStore, T: StateStore> MessageAuthority for ServiceContext<C, T> {
    fn authorize_outgoing(&self, message: &DeckrMessage) -> Result<()> {
        self.metadata(message, true).map(|_| ())
    }
    fn authorize_incoming(&self, message: &DeckrMessage) -> Result<()> {
        self.metadata(message, false).map(|_| ())
    }
}
