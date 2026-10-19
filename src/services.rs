use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::beacon::Candidate;
use crate::concord::CreateContractSpec;
use crate::endpoint::{service_address, EndpointAddress};
use crate::keys::encode_key_token;
use crate::state::StateStore;
use crate::{Error, Result};

mod message;
pub use message::*;
mod declaration;
pub use declaration::*;
mod context;
pub use context::*;
mod views;
pub use views::*;
mod directory;
pub use directory::*;
mod connection;
pub use connection::*;
mod client;
pub use client::*;
mod provider;
pub use provider::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceBackendStatus {
    Available,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceViewFamily {
    pub store_name: String,
    pub key_prefix: String,
    pub writer: ServiceViewWriter,
}

impl ServiceViewFamily {
    pub fn new(store_name: impl Into<String>, key_prefix: impl Into<String>) -> Result<Self> {
        Self::with_writer(store_name, key_prefix, ServiceViewWriter::Service)
    }
    pub fn with_writer(
        store_name: impl Into<String>,
        key_prefix: impl Into<String>,
        writer: ServiceViewWriter,
    ) -> Result<Self> {
        let family = Self {
            store_name: store_name.into(),
            key_prefix: key_prefix.into(),
            writer,
        };
        family.validate()?;
        Ok(family)
    }

    pub fn validate(&self) -> Result<()> {
        require_text(&self.store_name, "service view storeName")?;
        require_text(&self.key_prefix, "service view keyPrefix")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceViewFamilyDefinition {
    pub store_name: String,
    pub writer: ServiceViewWriter,
}

impl ServiceViewFamilyDefinition {
    pub fn new(store_name: impl Into<String>) -> Result<Self> {
        Self::with_writer(store_name, ServiceViewWriter::Service)
    }
    pub fn with_writer(store_name: impl Into<String>, writer: ServiceViewWriter) -> Result<Self> {
        let definition = Self {
            store_name: store_name.into(),
            writer,
        };
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> Result<()> {
        require_text(&self.store_name, "service view storeName")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceProtocol {
    pub namespace: String,
    pub feature_id: String,
    pub advertisement_profile: String,
    pub use_profile: String,
    pub operations: BTreeMap<String, ServiceOperationDefinition>,
    pub messages: BTreeMap<String, ServiceMessageDefinition>,
    pub view_families: BTreeMap<String, ServiceViewFamilyDefinition>,
}

impl ServiceProtocol {
    pub fn new(
        namespace: impl Into<String>,
        feature_id: impl Into<String>,
        advertisement_profile: impl Into<String>,
        use_profile: impl Into<String>,
        operations: impl IntoIterator<Item = impl Into<String>>,
        view_families: BTreeMap<String, ServiceViewFamilyDefinition>,
    ) -> Result<Self> {
        let protocol = Self {
            namespace: namespace.into(),
            feature_id: feature_id.into(),
            advertisement_profile: advertisement_profile.into(),
            use_profile: use_profile.into(),
            operations: operations
                .into_iter()
                .map(|name| (name.into(), ServiceOperationDefinition::default()))
                .collect(),
            messages: BTreeMap::new(),
            view_families,
        };
        protocol.validate()?;
        Ok(protocol)
    }

    pub fn validate(&self) -> Result<()> {
        require_text(&self.namespace, "service namespace")?;
        require_text(&self.feature_id, "service feature id")?;
        require_text(&self.advertisement_profile, "service advertisement profile")?;
        require_text(&self.use_profile, "service use profile")?;
        for (operation, definition) in &self.operations {
            require_text(operation, "service operation")?;
            definition.validate()?;
        }
        for (name, message) in &self.messages {
            require_text(name, "service message name")?;
            message.validate()?;
            if message
                .operation
                .as_ref()
                .is_some_and(|operation| !self.operations.contains_key(operation))
            {
                return Err(Error::Invalid(
                    "service message operation is not declared".into(),
                ));
            }
        }
        for (name, family) in &self.view_families {
            require_text(name, "service view family name")?;
            family.validate()?;
        }
        Ok(())
    }

    pub fn from_value(value: Value) -> Result<Self> {
        let protocol: Self = serde_json::from_value(value)?;
        protocol.validate()?;
        Ok(protocol)
    }
    pub fn from_text(text: &str) -> Result<Self> {
        Self::from_value(crate::json::parse(text.as_bytes())?)
    }
    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }

    pub fn advertisement_payload(
        &self,
        service_id: impl Into<String>,
        session_id: impl Into<String>,
        backend_status: ServiceBackendStatus,
    ) -> Result<ServiceAdvertisementPayload> {
        let service_id = service_id.into();
        self.validate()?;
        let mut payload = ServiceAdvertisementPayload::new(
            self.advertisement_profile.clone(),
            service_id.clone(),
            EndpointAddress::parse(service_address(&service_id))?,
            self.namespace.clone(),
            session_id.into(),
            self.use_profile.clone(),
            backend_status,
            self.operations.keys().cloned().collect(),
            service_protocol_views(self, &service_id)?,
            BTreeMap::new(),
        )?;
        payload.supported_messages = self.messages.clone();
        payload.validate()?;
        Ok(payload)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceAdvertisementPayload {
    pub profile: String,
    pub service_id: String,
    pub service_endpoint: EndpointAddress,
    pub service_namespace: String,
    pub session_id: String,
    pub service_use_profile: String,
    pub backend_status: ServiceBackendStatus,
    pub supported_operations: Vec<String>,
    pub supported_messages: BTreeMap<String, ServiceMessageDefinition>,
    pub views: BTreeMap<String, ServiceViewFamily>,
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_ordered_object",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub diagnostics: BTreeMap<String, Value>,
}

impl ServiceAdvertisementPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        profile: String,
        service_id: String,
        service_endpoint: EndpointAddress,
        service_namespace: String,
        session_id: String,
        service_use_profile: String,
        backend_status: ServiceBackendStatus,
        supported_operations: Vec<String>,
        views: BTreeMap<String, ServiceViewFamily>,
        diagnostics: BTreeMap<String, Value>,
    ) -> Result<Self> {
        let payload = Self {
            profile,
            service_id,
            service_endpoint,
            service_namespace,
            session_id,
            service_use_profile,
            backend_status,
            supported_operations,
            supported_messages: BTreeMap::new(),
            views,
            diagnostics,
        };
        payload.validate()?;
        Ok(payload)
    }

    pub fn from_value(value: Value) -> Result<Self> {
        let payload: Self = serde_json::from_value(value)?;
        payload.validate()?;
        Ok(payload)
    }
    pub fn from_text(text: &str) -> Result<Self> {
        Self::from_value(crate::json::parse(text.as_bytes())?)
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }

    pub fn validate(&self) -> Result<()> {
        require_text(&self.profile, "service advertisement profile")?;
        require_text(&self.service_id, "service id")?;
        require_text(&self.service_namespace, "service namespace")?;
        require_text(&self.session_id, "service session id")?;
        require_text(&self.service_use_profile, "service use profile")?;
        if self.service_endpoint != EndpointAddress::parse(service_address(&self.service_id))? {
            return Err(Error::Invalid(
                "serviceEndpoint must equal service:<serviceId>".to_string(),
            ));
        }
        let mut unique = BTreeSet::new();
        for operation in &self.supported_operations {
            require_text(operation, "service operation")?;
            if !unique.insert(operation) {
                return Err(Error::Invalid(
                    "service advertisement repeats an operation".into(),
                ));
            }
        }
        for (name, message) in &self.supported_messages {
            require_text(name, "advertised service message name")?;
            message.validate()?;
            if message
                .operation
                .as_ref()
                .is_some_and(|operation| !unique.contains(operation))
            {
                return Err(Error::Invalid(
                    "advertised service message names an unsupported operation".into(),
                ));
            }
        }
        for (name, family) in &self.views {
            require_text(name, "service view family name")?;
            family.validate()?;
        }
        for value in self.diagnostics.values() {
            crate::json::validate_numbers(value)?;
        }
        Ok(())
    }
}

/// Local authorization policy, never serialized into Concord terms.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceAccess {
    pub service_endpoint: EndpointAddress,
    pub service_namespace: String,
    pub service_session_id: String,
    pub allowed_operations: Vec<String>,
    pub allowed_views: BTreeMap<String, Vec<String>>,
}

/// A fresh service-use proposal with exactly two participants and no terms.
pub fn service_use_contract(
    descriptor: &ServiceDescriptor,
    client_endpoint: &EndpointAddress,
    client_session_id: &str,
) -> Result<CreateContractSpec> {
    EndpointAddress::parse(client_endpoint.as_str())?;
    require_text(client_session_id, "client session id")?;
    require_text(&descriptor.use_profile, "service-use profile")?;
    require_text(&descriptor.session_id, "service session id")?;
    if descriptor.endpoint != EndpointAddress::parse(service_address(&descriptor.service_id))?
        || &descriptor.endpoint == client_endpoint
    {
        return Err(Error::Invalid(
            "service-use requires a service and a distinct client endpoint".into(),
        ));
    }
    let mut spec =
        CreateContractSpec::new(vec![descriptor.endpoint.clone(), client_endpoint.clone()]);
    spec.profile = Some(descriptor.use_profile.clone());
    spec.created_by = Some(client_endpoint.clone());
    spec.participant_sessions.insert(
        descriptor.endpoint.to_string(),
        descriptor.session_id.clone(),
    );
    spec.participant_sessions
        .insert(client_endpoint.to_string(), client_session_id.into());
    Ok(spec)
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceDescriptor {
    pub candidate: Option<Candidate>,
    pub service_id: String,
    pub namespace: String,
    pub endpoint: EndpointAddress,
    pub session_id: String,
    pub advertisement_profile: String,
    pub use_profile: String,
    pub supported_operations: BTreeSet<String>,
    pub supported_messages: BTreeMap<String, ServiceMessageDefinition>,
    pub views: BTreeMap<String, ServiceViewFamily>,
    pub backend_status: ServiceBackendStatus,
    pub diagnostics: BTreeMap<String, Value>,
}

impl ServiceDescriptor {
    /// Validate a provider's declaration payload before it is advertised.
    pub fn from_payload(
        payload: ServiceAdvertisementPayload,
        protocol: &ServiceProtocol,
    ) -> Result<Self> {
        payload.validate()?;
        let descriptor = Self {
            candidate: None,
            service_id: payload.service_id,
            namespace: payload.service_namespace,
            endpoint: payload.service_endpoint,
            session_id: payload.session_id,
            advertisement_profile: payload.profile,
            use_profile: payload.service_use_profile,
            supported_operations: payload.supported_operations.into_iter().collect(),
            supported_messages: payload.supported_messages,
            views: payload.views,
            backend_status: payload.backend_status,
            diagnostics: payload.diagnostics,
        };
        descriptor.validate(protocol)?;
        Ok(descriptor)
    }
    /// Validate a supplied descriptor against the owner declaration without
    /// consulting discovery. Accepted agreements retain this immutable identity.
    pub fn validate(&self, protocol: &ServiceProtocol) -> Result<()> {
        protocol.validate()?;
        require_text(&self.service_id, "service id")?;
        require_text(&self.session_id, "service session id")?;
        if self.endpoint != EndpointAddress::parse(service_address(&self.service_id))?
            || self.namespace != protocol.namespace
            || self.advertisement_profile != protocol.advertisement_profile
            || self.use_profile != protocol.use_profile
            || self
                .supported_operations
                .iter()
                .any(|operation| !protocol.operations.contains_key(operation))
            || self.supported_messages.iter().any(|(name, message)| {
                protocol.messages.get(name) != Some(message)
                    || message
                        .operation
                        .as_ref()
                        .is_some_and(|operation| !self.supported_operations.contains(operation))
            })
            || self.views != service_protocol_views(protocol, &self.service_id)?
        {
            return Err(Error::Invalid(
                "service descriptor differs from its declaration".into(),
            ));
        }
        if let Some(candidate) = &self.candidate {
            let parsed = parse_service_descriptor(candidate, protocol).ok_or_else(|| {
                Error::Invalid("service descriptor has an invalid source candidate".into())
            })?;
            if &parsed != self {
                return Err(Error::Invalid(
                    "service descriptor differs from its source candidate".into(),
                ));
            }
        }
        Ok(())
    }
}

pub fn parse_service_descriptor(
    candidate: &Candidate,
    protocol: &ServiceProtocol,
) -> Option<ServiceDescriptor> {
    protocol.validate().ok()?;
    let advertisement = &candidate.advertisement;
    if advertisement.feature_id != protocol.feature_id {
        return None;
    }
    let payload = advertisement.payload.clone()?;
    let payload = ServiceAdvertisementPayload::from_value(payload).ok()?;
    if payload.profile != protocol.advertisement_profile {
        return None;
    }
    if payload.service_namespace != protocol.namespace {
        return None;
    }
    if payload.service_use_profile != protocol.use_profile {
        return None;
    }
    if payload.service_endpoint != advertisement.endpoint {
        return None;
    }
    if payload.session_id != advertisement.session_id {
        return None;
    }
    let advertised_operations = payload
        .supported_operations
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if advertised_operations
        .iter()
        .any(|operation| !protocol.operations.contains_key(operation))
    {
        return None;
    }
    if payload
        .supported_messages
        .iter()
        .any(|(name, message)| protocol.messages.get(name) != Some(message))
    {
        return None;
    }
    let expected_views = service_protocol_views(protocol, &payload.service_id).ok()?;
    if payload.views != expected_views {
        return None;
    }
    Some(ServiceDescriptor {
        candidate: Some(candidate.clone()),
        service_id: payload.service_id,
        namespace: payload.service_namespace,
        endpoint: payload.service_endpoint,
        session_id: payload.session_id,
        advertisement_profile: payload.profile,
        use_profile: payload.service_use_profile,
        supported_operations: advertised_operations,
        supported_messages: payload.supported_messages,
        views: expected_views,
        backend_status: payload.backend_status,
        diagnostics: payload.diagnostics,
    })
}

pub fn service_access(
    descriptor: &ServiceDescriptor,
    operations: impl IntoIterator<Item = impl Into<String>>,
    views: BTreeMap<String, Vec<String>>,
) -> Result<ServiceAccess> {
    let mut allowed_operations = operations
        .into_iter()
        .map(Into::into)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for operation in &allowed_operations {
        require_text(operation, "service operation")?;
        if !descriptor.supported_operations.contains(operation) {
            return Err(Error::Invalid(format!(
                "service {:?} does not advertise operation {:?}",
                descriptor.service_id, operation
            )));
        }
    }
    allowed_operations.sort();

    let mut allowed_views = BTreeMap::<String, Vec<String>>::new();
    for (family, prefixes) in views {
        require_text(&family, "service view family")?;
        let Some(view_family) = descriptor.views.get(&family) else {
            return Err(Error::Invalid(format!(
                "service {:?} does not advertise view family {:?}",
                descriptor.service_id, family
            )));
        };
        let mut normalized = if prefixes.is_empty() {
            vec![view_family.key_prefix.clone()]
        } else {
            prefixes
        };
        normalized.sort();
        normalized.dedup();
        for prefix in &normalized {
            require_text(prefix, "service view prefix")?;
            if !prefix.starts_with(&view_family.key_prefix) {
                return Err(Error::Invalid(format!(
                    "view prefix {prefix:?} is outside service view family {family:?}"
                )));
            }
        }
        allowed_views.insert(family, normalized);
    }

    Ok(ServiceAccess {
        service_endpoint: descriptor.endpoint.clone(),
        service_namespace: descriptor.namespace.clone(),
        service_session_id: descriptor.session_id.clone(),
        allowed_operations,
        allowed_views,
    })
}

pub fn service_descriptor_sort_key(descriptor: &ServiceDescriptor) -> (String, u64, String) {
    let Some(candidate) = &descriptor.candidate else {
        return (String::new(), 0, String::new());
    };
    (
        candidate
            .advertisement
            .updated_at
            .clone()
            .or_else(|| candidate.advertisement.created_at.clone())
            .unwrap_or_default(),
        candidate.advertisement.refresh_seq,
        candidate.key.clone(),
    )
}

pub fn newest_service_descriptor(
    descriptors: impl IntoIterator<Item = ServiceDescriptor>,
) -> Option<ServiceDescriptor> {
    descriptors
        .into_iter()
        .max_by_key(service_descriptor_sort_key)
}

#[derive(Debug, Clone, Default)]
pub struct ServiceQuery {
    pub service_id: Option<String>,
    pub namespace: Option<String>,
    pub use_profile: Option<String>,
    pub operations: BTreeSet<String>,
    pub views: BTreeSet<String>,
    pub endpoint: Option<EndpointAddress>,
    pub session_id: Option<String>,
}

pub trait ServiceSelectionPolicy: Send + Sync + 'static {
    fn select(&self, descriptors: Vec<ServiceDescriptor>) -> Option<ServiceDescriptor>;
}

#[derive(Debug, Clone, Default)]
pub struct NewestServiceSelectionPolicy;

impl ServiceSelectionPolicy for NewestServiceSelectionPolicy {
    fn select(&self, descriptors: Vec<ServiceDescriptor>) -> Option<ServiceDescriptor> {
        newest_service_descriptor(descriptors)
    }
}

pub struct ServiceResolver<S: StateStore, P: ServiceSelectionPolicy = NewestServiceSelectionPolicy>
{
    directory: ServiceDirectory<S>,
    policy: P,
}

impl<S: StateStore> ServiceResolver<S, NewestServiceSelectionPolicy> {
    pub fn new(directory: ServiceDirectory<S>) -> Self {
        Self {
            directory,
            policy: NewestServiceSelectionPolicy,
        }
    }
}

impl<S: StateStore, P: ServiceSelectionPolicy> ServiceResolver<S, P> {
    pub fn with_policy(directory: ServiceDirectory<S>, policy: P) -> Self {
        Self { directory, policy }
    }

    pub fn match_services(&self, query: &ServiceQuery) -> Vec<ServiceDescriptor> {
        self.directory.match_services(query)
    }

    pub fn resolve(&self, query: &ServiceQuery) -> Option<ServiceDescriptor> {
        self.policy.select(self.match_services(query))
    }
}

pub fn service_view_key(service_id: &str, family: &str, tokens: &[&str]) -> String {
    let mut parts = vec![
        "views".to_string(),
        encode_key_token(service_id),
        encode_key_token(family),
    ];
    parts.extend(tokens.iter().map(|token| encode_key_token(token)));
    parts.join(".")
}

pub fn service_view_prefix(service_id: &str, family: &str) -> String {
    format!("{}.", service_view_key(service_id, family, &[]))
}

fn service_protocol_views(
    protocol: &ServiceProtocol,
    service_id: &str,
) -> Result<BTreeMap<String, ServiceViewFamily>> {
    require_text(service_id, "service id")?;
    protocol
        .view_families
        .iter()
        .map(|(family, definition)| {
            Ok((
                family.clone(),
                ServiceViewFamily::with_writer(
                    definition.store_name.clone(),
                    service_view_prefix(service_id, family),
                    definition.writer,
                )?,
            ))
        })
        .collect()
}

fn index(index: &mut BTreeMap<String, BTreeSet<String>>, value: &str, key: &str) {
    index
        .entry(value.to_string())
        .or_default()
        .insert(key.to_string());
}

fn index_session(
    index: &mut BTreeMap<(String, String), BTreeSet<String>>,
    endpoint: &str,
    session_id: &str,
    key: &str,
) {
    index
        .entry((endpoint.to_string(), session_id.to_string()))
        .or_default()
        .insert(key.to_string());
}

fn intersect_optional(
    keys: &mut BTreeSet<String>,
    index: &BTreeMap<String, BTreeSet<String>>,
    value: Option<&str>,
) {
    if let Some(value) = value {
        intersect_set(keys, index.get(value));
    }
}

fn intersect_set(keys: &mut BTreeSet<String>, filter: Option<&BTreeSet<String>>) {
    match filter {
        Some(filter) => keys.retain(|key| filter.contains(key)),
        None => keys.clear(),
    }
}

fn require_text(value: &str, field_name: &str) -> Result<()> {
    if value.trim() != value || value.is_empty() {
        return Err(Error::Invalid(format!(
            "{field_name} must be non-empty with no leading or trailing whitespace"
        )));
    }
    Ok(())
}
