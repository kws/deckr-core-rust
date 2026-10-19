use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time;

use crate::authority::ContractPointer;
use crate::beacon::{Beacon, BeaconFeatureEvent, BeaconFeatureEventType, Candidate};
use crate::canonical_json::canonical_json_hash_value;
use crate::endpoint::{service_address, EndpointAddress};
use crate::keys::{
    encode_key_token, service_use_scope_index_key as make_service_use_scope_index_key,
};
use crate::state::{StateStore, StateStorePolicy};
use crate::{Error, Result};

pub const SERVICE_USE_INDEX_SCHEMA_ID: &str = "dev.deckr.service-use-index.v1";
pub const DEFAULT_SERVICE_USE_INDEX_STORE_NAME: &str = "deckr_service_use_index_v1";

pub fn service_use_index_store_policy() -> StateStorePolicy {
    StateStorePolicy::persistent("Deckr service-use scope index")
}

pub fn service_use_scope_index_key(scope_id: &str) -> String {
    make_service_use_scope_index_key(scope_id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceBackendStatus {
    Available,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceViewFamily {
    pub store_name: String,
    pub key_prefix: String,
}

impl ServiceViewFamily {
    pub fn new(store_name: impl Into<String>, key_prefix: impl Into<String>) -> Result<Self> {
        let family = Self {
            store_name: store_name.into(),
            key_prefix: key_prefix.into(),
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
#[serde(rename_all = "camelCase")]
pub struct ServiceViewFamilyDefinition {
    pub store_name: String,
}

impl ServiceViewFamilyDefinition {
    pub fn new(store_name: impl Into<String>) -> Result<Self> {
        let definition = Self {
            store_name: store_name.into(),
        };
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> Result<()> {
        require_text(&self.store_name, "service view storeName")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceProtocol {
    pub namespace: String,
    pub feature_id: String,
    pub advertisement_profile: String,
    pub use_profile: String,
    pub operations: BTreeSet<String>,
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
            operations: operations.into_iter().map(Into::into).collect(),
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
        if self.operations.is_empty() {
            return Err(Error::Invalid(
                "service protocol operations must not be empty".to_string(),
            ));
        }
        for operation in &self.operations {
            require_text(operation, "service operation")?;
        }
        for (name, family) in &self.view_families {
            require_text(name, "service view family name")?;
            family.validate()?;
        }
        Ok(())
    }

    pub fn advertisement_payload(
        &self,
        service_id: impl Into<String>,
        session_id: impl Into<String>,
        backend_status: ServiceBackendStatus,
    ) -> Result<ServiceAdvertisementPayload> {
        let service_id = service_id.into();
        ServiceAdvertisementPayload::new(
            self.advertisement_profile.clone(),
            service_id.clone(),
            EndpointAddress::parse(service_address(&service_id))?,
            self.namespace.clone(),
            session_id.into(),
            self.use_profile.clone(),
            backend_status,
            self.operations.iter().cloned().collect(),
            service_protocol_views(self, &service_id)?,
            BTreeMap::new(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAdvertisementPayload {
    pub profile: String,
    pub service_id: String,
    pub service_endpoint: EndpointAddress,
    pub service_namespace: String,
    pub session_id: String,
    pub service_use_profile: String,
    pub backend_status: ServiceBackendStatus,
    pub supported_operations: Vec<String>,
    pub views: BTreeMap<String, ServiceViewFamily>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
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
        if self.supported_operations.is_empty() {
            return Err(Error::Invalid(
                "service advertisement requires operations".to_string(),
            ));
        }
        for operation in &self.supported_operations {
            require_text(operation, "service operation")?;
        }
        for (name, family) in &self.views {
            require_text(name, "service view family name")?;
            family.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceUseTerms {
    pub profile: String,
    pub service_use_scope_id: String,
    pub service_id: String,
    pub service_endpoint: EndpointAddress,
    pub service_namespace: String,
    pub service_session_id: String,
    pub client_endpoint: EndpointAddress,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_operations: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub allowed_views: BTreeMap<String, Vec<String>>,
}

impl ServiceUseTerms {
    pub fn from_value(value: Value) -> Result<Self> {
        let terms: Self = serde_json::from_value(value)?;
        terms.validate()?;
        Ok(terms)
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }

    pub fn validate(&self) -> Result<()> {
        require_text(&self.profile, "service-use terms profile")?;
        require_text(&self.service_use_scope_id, "service-use scope id")?;
        require_text(&self.service_id, "service id")?;
        require_text(&self.service_namespace, "service namespace")?;
        require_text(&self.service_session_id, "service session id")?;
        if self.service_endpoint != EndpointAddress::parse(service_address(&self.service_id))? {
            return Err(Error::Invalid(
                "serviceEndpoint must equal service:<serviceId>".to_string(),
            ));
        }
        for operation in &self.allowed_operations {
            require_text(operation, "service operation")?;
        }
        for (family, prefixes) in &self.allowed_views {
            require_text(family, "service view family")?;
            for prefix in prefixes {
                require_text(prefix, "service view prefix")?;
            }
        }
        if self.allowed_operations.is_empty()
            && !self
                .allowed_views
                .values()
                .any(|prefixes| !prefixes.is_empty())
        {
            return Err(Error::Invalid(
                "service-use terms require operations or views".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceUseScopeIndexState {
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceUseScopeIndexRecord {
    #[serde(default = "service_use_index_schema_id", rename = "schema")]
    pub schema_id: String,
    pub scope_id: String,
    pub contract: ContractPointer,
    pub terms_hash: String,
    pub service_endpoint: EndpointAddress,
    pub service_session_id: String,
    pub client_endpoint: EndpointAddress,
    pub client_session_id: String,
    pub state: ServiceUseScopeIndexState,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<ContractPointer>,
}

impl ServiceUseScopeIndexRecord {
    pub fn from_value(value: Value) -> Result<Self> {
        let record: Self = serde_json::from_value(value)?;
        record.validate()?;
        Ok(record)
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_id != SERVICE_USE_INDEX_SCHEMA_ID {
            return Err(Error::Invalid(format!(
                "service-use scope index schema must be {SERVICE_USE_INDEX_SCHEMA_ID}"
            )));
        }
        require_text(&self.scope_id, "service-use scope id")?;
        self.contract.validate()?;
        require_text(&self.terms_hash, "service-use terms hash")?;
        require_text(&self.service_session_id, "service session id")?;
        require_text(&self.client_session_id, "client session id")?;
        require_text(&self.created_at, "service-use scope index timestamp")?;
        require_text(&self.updated_at, "service-use scope index timestamp")?;
        if let Some(supersedes) = &self.supersedes {
            supersedes.validate()?;
        }
        Ok(())
    }
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
    pub views: BTreeMap<String, ServiceViewFamily>,
    pub backend_status: ServiceBackendStatus,
    pub diagnostics: BTreeMap<String, Value>,
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
    if !advertised_operations.is_subset(&protocol.operations) {
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
        views: expected_views,
        backend_status: payload.backend_status,
        diagnostics: payload.diagnostics,
    })
}

pub fn service_use_terms(
    descriptor: &ServiceDescriptor,
    client_endpoint: &EndpointAddress,
    operations: impl IntoIterator<Item = impl Into<String>>,
    views: BTreeMap<String, Vec<String>>,
) -> Result<ServiceUseTerms> {
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

    let identity = serde_json::json!({
        "profile": descriptor.use_profile,
        "serviceId": descriptor.service_id,
        "serviceEndpoint": descriptor.endpoint,
        "serviceNamespace": descriptor.namespace,
        "serviceSessionId": descriptor.session_id,
        "clientEndpoint": client_endpoint,
        "allowedOperations": allowed_operations.clone(),
        "allowedViews": allowed_views.clone(),
    });
    let digest = canonical_json_hash_value(&identity)?;
    let digest = digest.trim_start_matches("sha256:");
    let terms = ServiceUseTerms {
        profile: descriptor.use_profile.clone(),
        service_use_scope_id: format!("service-use-scope:{}", &digest[..32]),
        service_id: descriptor.service_id.clone(),
        service_endpoint: descriptor.endpoint.clone(),
        service_namespace: descriptor.namespace.clone(),
        service_session_id: descriptor.session_id.clone(),
        client_endpoint: client_endpoint.clone(),
        allowed_operations,
        allowed_views,
    };
    terms.validate()?;
    Ok(terms)
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

#[derive(Clone)]
pub struct ServiceDirectory<S: StateStore> {
    beacon: Beacon<S>,
    protocol: ServiceProtocol,
    inner: Arc<Mutex<ServiceDirectoryInner>>,
    ready: Arc<Notify>,
    watch_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Debug, Default)]
struct ServiceDirectoryInner {
    ready: bool,
    current: bool,
    closed: bool,
    descriptors_by_key: BTreeMap<String, ServiceDescriptor>,
    by_service_id: BTreeMap<String, BTreeSet<String>>,
    by_namespace: BTreeMap<String, BTreeSet<String>>,
    by_use_profile: BTreeMap<String, BTreeSet<String>>,
    by_operation: BTreeMap<String, BTreeSet<String>>,
    by_view_family: BTreeMap<String, BTreeSet<String>>,
    by_endpoint: BTreeMap<String, BTreeSet<String>>,
    by_session: BTreeMap<(String, String), BTreeSet<String>>,
}

impl<S: StateStore> ServiceDirectory<S> {
    pub fn new(beacon: Beacon<S>, protocol: ServiceProtocol) -> Result<Self> {
        protocol.validate()?;
        Ok(Self {
            beacon,
            protocol,
            inner: Arc::new(Mutex::new(ServiceDirectoryInner::default())),
            ready: Arc::new(Notify::new()),
            watch_task: Arc::new(Mutex::new(None)),
        })
    }

    pub fn protocol(&self) -> &ServiceProtocol {
        &self.protocol
    }

    pub fn start(&self) {
        let mut watch_task = self
            .watch_task
            .lock()
            .expect("service directory task mutex poisoned");
        if watch_task.is_some() {
            return;
        }
        let directory = self.clone();
        *watch_task = Some(tokio::spawn(async move {
            directory.event_loop().await;
        }));
    }

    pub async fn wait_ready(&self) {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .inner
                .lock()
                .expect("service directory mutex poisoned")
                .ready
            {
                return;
            }
            notified.await;
        }
    }

    pub fn is_current(&self) -> bool {
        let inner = self.inner.lock().expect("service directory mutex poisoned");
        inner.ready && inner.current
    }

    pub fn descriptors(&self) -> Vec<ServiceDescriptor> {
        let mut descriptors = self
            .inner
            .lock()
            .expect("service directory mutex poisoned")
            .descriptors_by_key
            .values()
            .cloned()
            .collect::<Vec<_>>();
        descriptors.sort_by_key(service_descriptor_sort_key);
        descriptors
    }

    pub fn match_services(&self, query: &ServiceQuery) -> Vec<ServiceDescriptor> {
        let inner = self.inner.lock().expect("service directory mutex poisoned");
        let mut keys = inner
            .descriptors_by_key
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        intersect_optional(&mut keys, &inner.by_service_id, query.service_id.as_deref());
        intersect_optional(&mut keys, &inner.by_namespace, query.namespace.as_deref());
        intersect_optional(
            &mut keys,
            &inner.by_use_profile,
            query.use_profile.as_deref(),
        );
        if let Some(endpoint) = &query.endpoint {
            intersect_optional(&mut keys, &inner.by_endpoint, Some(endpoint.as_str()));
        }
        if let Some(session_id) = &query.session_id {
            match &query.endpoint {
                Some(endpoint) => {
                    intersect_set(
                        &mut keys,
                        inner
                            .by_session
                            .get(&(endpoint.as_str().to_string(), session_id.clone())),
                    );
                }
                None => keys.retain(|key| {
                    inner
                        .by_session
                        .iter()
                        .any(|((_, indexed_session), session_keys)| {
                            indexed_session == session_id && session_keys.contains(key)
                        })
                }),
            }
        }
        for operation in &query.operations {
            intersect_set(&mut keys, inner.by_operation.get(operation));
        }
        for family in &query.views {
            intersect_set(&mut keys, inner.by_view_family.get(family));
        }
        let mut descriptors = keys
            .into_iter()
            .filter_map(|key| inner.descriptors_by_key.get(&key).cloned())
            .collect::<Vec<_>>();
        descriptors.sort_by_key(service_descriptor_sort_key);
        descriptors
    }

    pub fn close(&self) {
        {
            let mut inner = self.inner.lock().expect("service directory mutex poisoned");
            inner.closed = true;
            inner.current = false;
        }
        if let Some(task) = self
            .watch_task
            .lock()
            .expect("service directory task mutex poisoned")
            .take()
        {
            task.abort();
        }
    }

    async fn event_loop(self) {
        loop {
            if self
                .inner
                .lock()
                .expect("service directory mutex poisoned")
                .closed
            {
                return;
            }
            match self.beacon.watch(&self.protocol.feature_id) {
                Ok(mut events) => {
                    if !self.consume_ready_events(&mut events) {
                        time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    while let Some(event) = events.next().await {
                        match event {
                            Ok(event) => self.apply_event(event, true),
                            Err(_) => {
                                self.mark_stale();
                                break;
                            }
                        }
                    }
                }
                Err(_) => {
                    self.mark_stale();
                    time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    fn consume_ready_events(&self, events: &mut crate::beacon::BeaconFeatureWatchStream) -> bool {
        loop {
            match events.next().now_or_never() {
                Some(Some(Ok(event))) => self.apply_event(event, false),
                Some(Some(Err(_))) | Some(None) => {
                    self.mark_stale();
                    return false;
                }
                None => {
                    self.mark_current();
                    return true;
                }
            }
        }
    }

    fn apply_event(&self, event: BeaconFeatureEvent, mark_current: bool) {
        let descriptor = if matches!(
            event.event_type,
            BeaconFeatureEventType::Advertised | BeaconFeatureEventType::Updated
        ) {
            event
                .candidate
                .as_ref()
                .and_then(|candidate| parse_service_descriptor(candidate, &self.protocol))
        } else {
            None
        };
        let mut inner = self.inner.lock().expect("service directory mutex poisoned");
        inner.remove(&event.key);
        if let Some(descriptor) = descriptor {
            inner.add(event.key, descriptor);
        }
        if mark_current {
            inner.ready = true;
            inner.current = true;
            self.ready.notify_waiters();
        }
    }

    fn mark_current(&self) {
        let mut inner = self.inner.lock().expect("service directory mutex poisoned");
        inner.ready = true;
        inner.current = true;
        self.ready.notify_waiters();
    }

    fn mark_stale(&self) {
        let mut inner = self.inner.lock().expect("service directory mutex poisoned");
        inner.ready = true;
        inner.current = false;
        self.ready.notify_waiters();
    }
}

impl ServiceDirectoryInner {
    fn add(&mut self, key: String, descriptor: ServiceDescriptor) {
        index(&mut self.by_service_id, &descriptor.service_id, &key);
        index(&mut self.by_namespace, &descriptor.namespace, &key);
        index(&mut self.by_use_profile, &descriptor.use_profile, &key);
        index(&mut self.by_endpoint, descriptor.endpoint.as_str(), &key);
        index_session(
            &mut self.by_session,
            descriptor.endpoint.as_str(),
            &descriptor.session_id,
            &key,
        );
        for operation in &descriptor.supported_operations {
            index(&mut self.by_operation, operation, &key);
        }
        for family in descriptor.views.keys() {
            index(&mut self.by_view_family, family, &key);
        }
        self.descriptors_by_key.insert(key, descriptor);
    }

    fn remove(&mut self, key: &str) {
        let Some(descriptor) = self.descriptors_by_key.remove(key) else {
            return;
        };
        unindex(&mut self.by_service_id, &descriptor.service_id, key);
        unindex(&mut self.by_namespace, &descriptor.namespace, key);
        unindex(&mut self.by_use_profile, &descriptor.use_profile, key);
        unindex(&mut self.by_endpoint, descriptor.endpoint.as_str(), key);
        unindex_session(
            &mut self.by_session,
            descriptor.endpoint.as_str(),
            &descriptor.session_id,
            key,
        );
        for operation in &descriptor.supported_operations {
            unindex(&mut self.by_operation, operation, key);
        }
        for family in descriptor.views.keys() {
            unindex(&mut self.by_view_family, family, key);
        }
    }
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
                ServiceViewFamily::new(
                    definition.store_name.clone(),
                    service_view_prefix(service_id, family),
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

fn unindex(index: &mut BTreeMap<String, BTreeSet<String>>, value: &str, key: &str) {
    let Some(keys) = index.get_mut(value) else {
        return;
    };
    keys.remove(key);
    if keys.is_empty() {
        index.remove(value);
    }
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

fn unindex_session(
    index: &mut BTreeMap<(String, String), BTreeSet<String>>,
    endpoint: &str,
    session_id: &str,
    key: &str,
) {
    let Some(keys) = index.get_mut(&(endpoint.to_string(), session_id.to_string())) else {
        return;
    };
    keys.remove(key);
    if keys.is_empty() {
        index.remove(&(endpoint.to_string(), session_id.to_string()));
    }
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

fn service_use_index_schema_id() -> String {
    SERVICE_USE_INDEX_SCHEMA_ID.to_string()
}

fn require_text(value: &str, field_name: &str) -> Result<()> {
    if value.trim() != value || value.is_empty() {
        return Err(Error::Invalid(format!(
            "{field_name} must be non-empty with no leading or trailing whitespace"
        )));
    }
    Ok(())
}
