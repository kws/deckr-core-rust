//! Managed provider ownership, domain consent, dispatch and backend callbacks.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use serde_json::Value;
use tokio::sync::watch;

use crate::authority::ContractPointer;
use crate::beacon::{
    AdvertisementContent, AdvertisementState, BeaconAdvertiser, ManagedAdvertisement,
};
use crate::concord::{
    AgreementState, ConcordViewContract, ContractFilters, ManagedParticipant,
    ParticipantCandidates, ParticipantPolicy,
};
use crate::endpoint::EndpointAddress;
use crate::lanes::SERVICES_LANE;
use crate::lifecycle::{
    Cancellation, Clock, Failure, FailureKind, Phase, Snapshot, StateCell, StateWatch,
};
use crate::messaging::{
    Delivery, MessageAuthority, MessageSession, MessageSubscription, MessageTransport,
};
use crate::runtime::MaterializedConcord;
use crate::state::StateStore;
use crate::{Error, Result};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceConsumer {
    pub endpoint: EndpointAddress,
    pub session_id: String,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServicePermissions {
    pub operations: BTreeSet<String>,
    pub views: BTreeMap<String, Vec<String>>,
}
impl ServicePermissions {
    pub fn all(descriptor: &ServiceDescriptor) -> Self {
        Self {
            operations: descriptor.supported_operations.clone(),
            views: descriptor
                .views
                .keys()
                .map(|family| (family.clone(), Vec::new()))
                .collect(),
        }
    }
    fn access(&self, descriptor: &ServiceDescriptor) -> Result<ServiceAccess> {
        service_access(descriptor, self.operations.clone(), self.views.clone())
    }
}
pub type ServiceAdmissionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ServicePermissions>>> + Send + 'a>>;
pub trait ServiceAdmissionPolicy: Send + Sync + 'static {
    /// None withdraws consent. An assessment error preserves an existing grant
    /// but cannot grant a new agreement. Report known withdrawal synchronously
    /// through the provider instead of waiting for an asynchronous callback.
    fn assess(&self, consumer: ServiceConsumer) -> ServiceAdmissionFuture<'_>;
}
impl<F, Fut> ServiceAdmissionPolicy for F
where
    F: Fn(ServiceConsumer) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<ServicePermissions>>> + Send + 'static,
{
    fn assess(&self, consumer: ServiceConsumer) -> ServiceAdmissionFuture<'_> {
        Box::pin(self(consumer))
    }
}
pub type ServiceBackendFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
pub trait ServiceBackend<C: StateStore, T: StateStore, M: MessageTransport>:
    Send + Sync + 'static
{
    /// Notification before messages are dispatched for this connection. Domain
    /// readiness is decided by admission; notification failures do not revoke
    /// Concord authority. Callbacks and their cleanup must be cancellation-safe.
    fn connected(&self, _connection: ServiceConnection<C, T, M>) -> ServiceBackendFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    fn handle(&self, incoming: ServiceIncoming<C, T, M>) -> ServiceBackendFuture<'_>;
    /// The supplied context has already lost local authority. This callback
    /// releases domain resources; it cannot resume protected work.
    fn disconnected(&self, _connection: ServiceConnection<C, T, M>) -> ServiceBackendFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    /// Called after all contexts are revoked; it can overlap disconnect cleanup.
    fn shutdown(&self) -> ServiceBackendFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport, F, Fut> ServiceBackend<C, T, M> for F
where
    F: Fn(ServiceIncoming<C, T, M>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    fn handle(&self, incoming: ServiceIncoming<C, T, M>) -> ServiceBackendFuture<'_> {
        Box::pin(self(incoming))
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceBackendState {
    pub status: ServiceBackendStatus,
    pub consent: bool,
    pub diagnostics: BTreeMap<String, Value>,
}
#[derive(Debug, Clone)]
pub struct ServiceProviderOptions {
    /// Limit for each bootstrap attempt; unresolved ownership is retried.
    pub bootstrap_timeout: Duration,
    pub callback_timeout: Duration,
    pub max_connections: usize,
    pub max_in_flight: usize,
    pub max_pending_messages: usize,
}
impl Default for ServiceProviderOptions {
    fn default() -> Self {
        Self {
            bootstrap_timeout: Duration::from_secs(5),
            callback_timeout: Duration::from_secs(5),
            max_connections: 256,
            max_in_flight: 64,
            max_pending_messages: 256,
        }
    }
}
pub struct ServiceProviderSpec<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport> {
    pub advertisements: B,
    pub concord: MaterializedConcord<C, T>,
    pub messages: MessageSession<M>,
    pub protocol: ValidatedServiceProtocol,
    pub advertisement: ServiceAdvertisementPayload,
    pub initial_consent: bool,
    pub admission: Arc<dyn ServiceAdmissionPolicy>,
    pub backend: Arc<dyn ServiceBackend<C, T, M>>,
    pub options: ServiceProviderOptions,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceProviderConnectionState {
    pub consumer: ServiceConsumer,
    pub agreement: AgreementState,
    /// Connection notification has completed, so authorized dispatch can run.
    /// Domain consent and notification failures are reported independently.
    pub dispatch_ready: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceProviderState {
    pub bootstrapped: bool,
    pub backend: ServiceBackendState,
    pub participant_current: bool,
    pub discovery_current: bool,
    pub discovery_failure: Option<Failure>,
    pub advertisement: Option<AdvertisementState>,
    pub connections: BTreeMap<String, ServiceProviderConnectionState>,
    pub rejected_messages: u64,
    pub handled_messages: u64,
    pub backend_failures: u64,
}
#[derive(Clone)]
struct Facts {
    backend: ServiceBackendState,
    denied: BTreeSet<ServiceConsumer>,
    generation: u64,
}
#[derive(Clone)]
struct Grant {
    consumer: ServiceConsumer,
    permissions: ServicePermissions,
    revoked: Cancellation,
}
#[derive(Default)]
struct AdmissionState {
    cache: BTreeMap<ServiceConsumer, (u64, Option<ServicePermissions>)>,
    grants: BTreeMap<String, Grant>,
}
struct Use<C: StateStore, T: StateStore, M: MessageTransport> {
    connection: ServiceConnection<C, T, M>,
    consumer: ServiceConsumer,
    announced: bool,
    ready: bool,
}
struct Shared<C: StateStore, T: StateStore, M: MessageTransport> {
    concord: MaterializedConcord<C, T>,
    messages: MessageSession<M>,
    protocol: ValidatedServiceProtocol,
    descriptor: ServiceDescriptor,
    admission: Arc<dyn ServiceAdmissionPolicy>,
    backend: Arc<dyn ServiceBackend<C, T, M>>,
    options: ServiceProviderOptions,
    clock: Arc<dyn Clock>,
    stop: Cancellation,
    facts: watch::Sender<Facts>,
    admissions: Mutex<AdmissionState>,
    participant: Mutex<Option<ManagedParticipant<C, T>>>,
    advertisement: Mutex<Option<ManagedAdvertisement>>,
    uses: Mutex<BTreeMap<String, Use<C, T, M>>>,
    state: StateCell<ServiceProviderState>,
    cleanup_failure: Mutex<Option<Failure>>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Shared<C, T, M> {
    fn participant(&self) -> Option<ManagedParticipant<C, T>> {
        self.participant
            .lock()
            .expect("provider participant lock poisoned")
            .clone()
    }
    fn revoke_known(&self, all: bool) {
        let facts = self.facts.borrow().clone();
        let mut keys = BTreeSet::new();
        {
            let grants = &self
                .admissions
                .lock()
                .expect("provider admission lock poisoned")
                .grants;
            for (key, grant) in grants {
                if all || !facts.backend.consent || facts.denied.contains(&grant.consumer) {
                    grant.revoked.cancel();
                    keys.insert(key.clone());
                }
            }
        }
        if let Some(participant) = self.participant() {
            // Acquiring membership after updating facts coordinates with the
            // participant's final selection fence, including attachment races.
            for (key, state) in participant.state().value.contracts {
                if all || !facts.backend.consent || keys.contains(&key) {
                    if let Some(contract) = state.value.contract {
                        if let Some(agreement) = participant.agreement(&ContractPointer {
                            contract_id: contract.contract_id,
                            generation: contract.generation,
                        }) {
                            agreement.request_close_with_reason("service domain consent withdrawn");
                        }
                    }
                }
            }
            participant.wake_policy();
        }
    }
    fn close(&self) {
        self.stop.cancel();
        self.revoke_known(true);
        if let Some(participant) = self.participant() {
            participant.request_close();
        }
        if let Some(advertisement) = self
            .advertisement
            .lock()
            .expect("provider advertisement lock poisoned")
            .as_ref()
        {
            advertisement.request_close();
        }
        self.state.modify(|state| {
            state.phase = Phase::Closing;
            state.current = false;
            state.value.participant_current = false;
            true
        });
    }
    fn failure(&self, operation: &str, error: &Error) {
        self.state.modify(|state| {
            state.failure = Some(Failure::from_error(operation, error));
            true
        });
    }
    fn backend_failure(&self, error: &Error) {
        self.state.modify(|state| {
            state.value.backend_failures = state.value.backend_failures.saturating_add(1);
            state.failure = Some(Failure::from_error("service backend", error));
            true
        });
    }
    fn reject(&self, error: &Error) {
        self.state.modify(|state| {
            state.value.rejected_messages = state.value.rejected_messages.saturating_add(1);
            state.failure = Some(Failure::from_error("service dispatch", error));
            true
        });
    }
    fn observe(&self) {
        let participant = self.participant();
        let current = participant
            .as_ref()
            .is_some_and(|participant| participant.state().current)
            && self.concord.is_current()
            && !self.stop.is_cancelled();
        let backend = self.facts.borrow().backend.clone();
        let connections = self
            .uses
            .lock()
            .expect("provider connection lock poisoned")
            .iter()
            .map(|(key, owned)| {
                let mut agreement = owned.connection.context().agreement().state().value;
                agreement.local_authority &= owned.connection.authorize().is_ok();
                (
                    key.clone(),
                    ServiceProviderConnectionState {
                        consumer: owned.consumer.clone(),
                        agreement,
                        dispatch_ready: owned.ready,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        self.state.modify(|state| {
            if matches!(state.phase, Phase::Closing | Phase::Closed) {
                return false;
            }
            let ready = current && state.value.bootstrapped && backend.consent;
            if state.current == ready
                && state.value.participant_current == current
                && state.value.connections == connections
                && state.value.backend == backend
            {
                return false;
            }
            state.phase = if ready {
                Phase::Ready
            } else {
                Phase::Unavailable
            };
            state.current = ready;
            state.value.backend = backend;
            state.value.participant_current = current;
            state.value.connections = connections;
            true
        });
    }
}
struct Owner<C: StateStore, T: StateStore, M: MessageTransport> {
    shared: Arc<Shared<C, T, M>>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Drop for Owner<C, T, M> {
    fn drop(&mut self) {
        self.shared.close();
    }
}
pub struct ServiceProvider<C: StateStore, T: StateStore, M: MessageTransport> {
    owner: Arc<Owner<C, T, M>>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Clone for ServiceProvider<C, T, M> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> ServiceProvider<C, T, M> {
    pub fn start<B: StateStore>(spec: ServiceProviderSpec<B, C, T, M>) -> Result<Self> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("service provider requires a Tokio runtime".into()))?;
        let descriptor =
            ServiceDescriptor::from_payload(spec.advertisement.clone(), spec.protocol.protocol())?;
        if spec.messages.session().is_closed()
            || spec.messages.session().endpoint() != &descriptor.endpoint
            || spec.messages.session().id() != descriptor.session_id
        {
            return Err(Error::Invalid(
                "service provider requires the advertised endpoint session".into(),
            ));
        }
        let clock = spec.concord.runtime_clock();
        for limit in [
            spec.options.bootstrap_timeout,
            spec.options.callback_timeout,
        ] {
            if limit.is_zero() || clock.now().checked_add(limit).is_none() {
                return Err(Error::Invalid(
                    "provider deadlines must be finite and positive".into(),
                ));
            }
        }
        if spec.options.max_connections == 0
            || spec.options.max_in_flight == 0
            || spec.options.max_pending_messages == 0
        {
            return Err(Error::Invalid(
                "provider resource limits must be positive".into(),
            ));
        }
        let backend = ServiceBackendState {
            status: spec.advertisement.backend_status,
            consent: spec.initial_consent,
            diagnostics: spec.advertisement.diagnostics.clone(),
        };
        let facts = watch::channel(Facts {
            backend: backend.clone(),
            denied: BTreeSet::new(),
            generation: 1,
        })
        .0;
        let shared = Arc::new(Shared {
            concord: spec.concord,
            messages: spec.messages,
            protocol: spec.protocol,
            descriptor,
            admission: spec.admission,
            backend: spec.backend,
            options: spec.options,
            clock,
            stop: Cancellation::default(),
            facts,
            admissions: Mutex::default(),
            participant: Mutex::default(),
            advertisement: Mutex::default(),
            uses: Mutex::default(),
            state: StateCell::new(ServiceProviderState {
                bootstrapped: false,
                backend,
                participant_current: false,
                discovery_current: false,
                discovery_failure: None,
                advertisement: None,
                connections: BTreeMap::new(),
                rejected_messages: 0,
                handled_messages: 0,
                backend_failures: 0,
            }),
            cleanup_failure: Mutex::default(),
        });
        tokio::spawn(supervise(
            shared.clone(),
            spec.advertisements,
            spec.advertisement,
        ));
        Ok(Self {
            owner: Arc::new(Owner { shared }),
        })
    }
    pub fn state(&self) -> Snapshot<ServiceProviderState> {
        self.owner.shared.observe();
        self.owner.shared.state.snapshot()
    }
    pub fn watch_state(&self) -> Result<StateWatch<ServiceProviderState>> {
        self.state();
        self.owner.shared.state.watch()
    }
    pub async fn wait_for(
        &self,
        predicate: impl FnMut(&Snapshot<ServiceProviderState>) -> bool,
        timeout: Option<Duration>,
    ) -> Result<Snapshot<ServiceProviderState>> {
        self.state();
        self.owner
            .shared
            .state
            .wait_for(predicate, timeout, self.owner.shared.clock.as_ref())
            .await
    }
    pub async fn wait_ready(&self, timeout: Option<Duration>) -> Result<()> {
        self.wait_for(|state| state.current, timeout)
            .await
            .map(|_| ())
    }
    pub fn backend_state(&self) -> ServiceBackendState {
        self.owner.shared.facts.borrow().backend.clone()
    }
    pub fn set_backend(&self, state: ServiceBackendState) -> Result<()> {
        let shared = &self.owner.shared;
        if shared.stop.is_cancelled() {
            return Err(Error::Closed("service provider closed".into()));
        }
        for value in state.diagnostics.values() {
            crate::json::validate_numbers(value)?;
        }
        shared.facts.send_modify(|facts| {
            facts.backend = state;
            facts.generation = facts.generation.wrapping_add(1);
        });
        shared.revoke_known(false);
        shared.observe();
        Ok(())
    }
    pub fn set_consumer_consent(&self, consumer: ServiceConsumer, consent: bool) -> Result<()> {
        let shared = &self.owner.shared;
        if shared.stop.is_cancelled() {
            return Err(Error::Closed("service provider closed".into()));
        }
        EndpointAddress::parse(consumer.endpoint.as_str())?;
        require_text(&consumer.session_id, "service consumer session")?;
        shared.facts.send_modify(|facts| {
            if consent {
                facts.denied.remove(&consumer);
            } else {
                facts.denied.insert(consumer);
            }
            facts.generation = facts.generation.wrapping_add(1);
        });
        shared.revoke_known(false);
        shared.observe();
        Ok(())
    }
    /// Reassess asynchronous domain policy. Known revocation should use the
    /// synchronous consent setters so retained contexts stop immediately.
    pub fn reconsider(&self) -> Result<()> {
        let shared = &self.owner.shared;
        if shared.stop.is_cancelled() {
            return Err(Error::Closed("service provider closed".into()));
        }
        shared
            .facts
            .send_modify(|facts| facts.generation = facts.generation.wrapping_add(1));
        if let Some(participant) = shared.participant() {
            participant.wake_policy();
        }
        Ok(())
    }
    pub fn connections(&self) -> Vec<ServiceConnection<C, T, M>> {
        self.owner
            .shared
            .uses
            .lock()
            .expect("provider connection lock poisoned")
            .values()
            .filter(|owned| owned.ready && owned.connection.authorize().is_ok())
            .map(|owned| owned.connection.clone())
            .collect()
    }
    pub fn request_close(&self) {
        self.owner.shared.close();
    }
    pub async fn close(&self) -> Result<()> {
        self.request_close();
        self.owner.shared.state.wait_closed().await;
        if let Some(failure) = self
            .owner
            .shared
            .cleanup_failure
            .lock()
            .expect("provider cleanup lock poisoned")
            .as_ref()
        {
            return Err(Error::StateUnavailable(failure.detail.clone()));
        }
        Ok(())
    }
}

struct Admission<C: StateStore, T: StateStore, M: MessageTransport> {
    shared: Arc<Shared<C, T, M>>,
    completed_generation: AtomicU64,
}
#[derive(Clone)]
struct Candidate {
    key: String,
    pointer: ContractPointer,
    consumer: ServiceConsumer,
}
fn candidate<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    member: &ConcordViewContract,
) -> Result<Candidate> {
    let raw = shared
        .concord
        .raw_contract_cached(&member.contract)?
        .ok_or_else(|| Error::Unauthorized("service proposal disappeared".into()))?;
    let record = crate::concord::ContractRecord::from_value(raw.value.clone())?;
    let consumer = record
        .participants
        .iter()
        .find(|endpoint| **endpoint != shared.descriptor.endpoint)
        .ok_or_else(|| Error::Invalid("service proposal has no consumer".into()))?
        .clone();
    validate_service_use_record(
        &raw.value,
        &shared.descriptor.endpoint,
        &consumer,
        &shared.descriptor.use_profile,
    )?;
    if record.contract_id != member.contract.contract_id
        || record.generation != member.contract.generation
        || record
            .participant_sessions
            .get(shared.descriptor.endpoint.as_str())
            .is_some_and(|session| session != &shared.descriptor.session_id)
    {
        return Err(Error::Unauthorized(
            "service proposal targets another provider session or pointer".into(),
        ));
    }
    let session_id = record
        .participant_sessions
        .get(consumer.as_str())
        .cloned()
        .or_else(|| {
            member
                .validity
                .tokens
                .get(consumer.as_str())
                .map(|token| token.session_id.clone())
        })
        .ok_or_else(|| {
            Error::Unauthorized("service proposal has no observed consumer session".into())
        })?;
    require_text(&session_id, "consumer session")?;
    Ok(Candidate {
        key: member.contract.key.clone(),
        pointer: ContractPointer {
            contract_id: record.contract_id,
            generation: record.generation,
        },
        consumer: ServiceConsumer {
            endpoint: consumer,
            session_id,
        },
    })
}
fn close_keys<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    keys: &BTreeSet<String>,
    reason: &str,
) {
    if let Some(participant) = shared.participant() {
        for (key, member) in participant.state().value.contracts {
            if keys.contains(&key) {
                if let Some(contract) = member.value.contract {
                    if let Some(agreement) = participant.agreement(&ContractPointer {
                        contract_id: contract.contract_id,
                        generation: contract.generation,
                    }) {
                        agreement.request_close_with_reason(reason);
                    }
                }
            }
        }
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> ParticipantPolicy for Admission<C, T, M> {
    fn selection_is_current(&self) -> bool {
        !self.shared.stop.is_cancelled()
            && self.shared.facts.borrow().generation
                == self.completed_generation.load(Ordering::SeqCst)
    }
    async fn select(&self, candidates: ParticipantCandidates) -> Result<Vec<ContractPointer>> {
        let shared = &self.shared;
        let facts = shared.facts.borrow().clone();
        if !facts.backend.consent || shared.stop.is_cancelled() {
            self.completed_generation
                .store(facts.generation, Ordering::SeqCst);
            return Ok(Vec::new());
        }
        let mut eligible = candidates
            .contracts
            .iter()
            .filter_map(|member| candidate(shared, member).ok())
            .filter(|candidate| !facts.denied.contains(&candidate.consumer))
            .collect::<Vec<_>>();
        let selected = candidates
            .selected
            .into_iter()
            .map(|pointer| (pointer.contract_id, pointer.generation))
            .collect::<BTreeSet<_>>();
        eligible.sort_by_key(|candidate| {
            (
                !selected.contains(&(
                    candidate.pointer.contract_id.clone(),
                    candidate.pointer.generation,
                )),
                candidate.key.clone(),
            )
        });
        let consumers = eligible
            .iter()
            .map(|candidate| candidate.consumer.clone())
            .collect::<BTreeSet<_>>();
        let mut decisions = BTreeMap::<ServiceConsumer, Result<Option<ServicePermissions>>>::new();
        let mut pending = Vec::new();
        {
            let admissions = shared
                .admissions
                .lock()
                .expect("provider admission lock poisoned");
            for consumer in &consumers {
                if let Some((generation, permissions)) = admissions
                    .cache
                    .get(consumer)
                    .filter(|(generation, _)| *generation == facts.generation)
                {
                    let _ = generation;
                    decisions.insert(consumer.clone(), Ok(permissions.clone()));
                } else {
                    pending.push(consumer.clone());
                }
            }
        }
        let assessments = futures_util::stream::iter(pending)
            .map(|consumer| async move {
                let result = std::panic::AssertUnwindSafe(crate::lifecycle::deadline(
                    shared.clock.as_ref(),
                    shared.options.callback_timeout,
                    "service admission policy",
                    async { shared.admission.assess(consumer.clone()).await },
                ))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(Error::StateUnavailable(
                        "service admission callback panicked".into(),
                    ))
                });
                (consumer, result)
            })
            .buffer_unordered(shared.options.max_in_flight);
        tokio::pin!(assessments);
        while let Some((consumer, result)) = assessments.next().await {
            let result = result.and_then(|permissions| {
                if let Some(permissions) = &permissions {
                    permissions.access(&shared.descriptor)?;
                }
                Ok(permissions)
            });
            if let Err(error) = &result {
                shared.backend_failure(error);
            }
            decisions.insert(consumer, result);
        }
        let mut revoke = BTreeSet::new();
        let mut result = Vec::new();
        {
            let mut admissions = shared
                .admissions
                .lock()
                .expect("provider admission lock poisoned");
            if shared.facts.borrow().generation != facts.generation || shared.stop.is_cancelled() {
                self.completed_generation
                    .store(facts.generation, Ordering::SeqCst);
                return Ok(Vec::new());
            }
            admissions
                .cache
                .retain(|consumer, _| consumers.contains(consumer));
            for (consumer, decision) in &decisions {
                if let Ok(permissions) = decision {
                    admissions
                        .cache
                        .insert(consumer.clone(), (facts.generation, permissions.clone()));
                }
            }
            let eligible_keys = eligible
                .iter()
                .map(|candidate| candidate.key.clone())
                .collect::<BTreeSet<_>>();
            for (key, grant) in &admissions.grants {
                if !eligible_keys.contains(key) {
                    grant.revoked.cancel();
                    revoke.insert(key.clone());
                }
            }
            for candidate in eligible {
                let existing = admissions.grants.get(&candidate.key).cloned();
                let permissions = match decisions.get(&candidate.consumer) {
                    Some(Ok(permissions)) => permissions.clone(),
                    _ => existing
                        .as_ref()
                        .filter(|grant| !grant.revoked.is_cancelled())
                        .map(|grant| grant.permissions.clone()),
                };
                if existing
                    .as_ref()
                    .is_some_and(|grant| grant.revoked.is_cancelled())
                {
                    continue;
                }
                match permissions {
                    Some(permissions)
                        if existing.as_ref().is_none_or(|grant| {
                            grant.permissions == permissions && grant.consumer == candidate.consumer
                        }) =>
                    {
                        // Rejected candidates do not reserve capacity. Existing
                        // selections sort first and keep their reservation.
                        if result.len() >= shared.options.max_connections {
                            continue;
                        }
                        admissions
                            .grants
                            .entry(candidate.key)
                            .or_insert_with(|| Grant {
                                consumer: candidate.consumer,
                                permissions,
                                revoked: Cancellation::default(),
                            });
                        result.push(candidate.pointer);
                    }
                    _ => {
                        if let Some(grant) = existing {
                            grant.revoked.cancel();
                            revoke.insert(candidate.key);
                        }
                    }
                }
            }
            let selected_keys = result
                .iter()
                .map(|pointer| {
                    crate::keys::concord_contract_key(&pointer.contract_id, pointer.generation)
                })
                .collect::<BTreeSet<_>>();
            for (key, grant) in &admissions.grants {
                if !selected_keys.contains(key) {
                    grant.revoked.cancel();
                    revoke.insert(key.clone());
                }
            }
        }
        close_keys(
            shared,
            &revoke,
            "service admission changed or withdrew consent",
        );
        self.completed_generation
            .store(facts.generation, Ordering::SeqCst);
        Ok(result)
    }
}

/// Profile checks run outside asynchronous domain callbacks. In particular,
/// explicit null terms must end service authority even though generic Concord
/// may parse them the same way as omitted optional terms.
fn enforce_profiles<C: StateStore, T: StateStore, M: MessageTransport>(shared: &Shared<C, T, M>) {
    let Some(participant) = shared.participant() else {
        return;
    };
    let members = participant.state().value.contracts;
    let grants = shared
        .admissions
        .lock()
        .expect("provider admission lock poisoned")
        .grants
        .clone();
    let mut revoke = BTreeSet::new();
    for (key, grant) in grants {
        let Some(member) = members.get(&key) else {
            continue;
        };
        let Some(contract) = &member.value.contract else {
            continue;
        };
        let valid = (|| {
            let raw = shared
                .concord
                .raw_contract_cached(contract)?
                .ok_or_else(|| Error::Unauthorized("service agreement disappeared".into()))?;
            let record = validate_service_use_record(
                &raw.value,
                &shared.descriptor.endpoint,
                &grant.consumer.endpoint,
                &shared.descriptor.use_profile,
            )?;
            if record.contract_id != contract.contract_id
                || record.generation != contract.generation
                || record
                    .participant_sessions
                    .get(shared.descriptor.endpoint.as_str())
                    .is_some_and(|session| session != &shared.descriptor.session_id)
                || record
                    .participant_sessions
                    .get(grant.consumer.endpoint.as_str())
                    .is_some_and(|session| session != &grant.consumer.session_id)
            {
                return Err(Error::Unauthorized(
                    "service profile identity changed".into(),
                ));
            }
            Ok::<_, Error>(())
        })();
        if let Err(error) = valid {
            grant.revoked.cancel();
            revoke.insert(key);
            shared.failure("service profile", &error);
        }
    }
    close_keys(shared, &revoke, "service profile authority ended");
}

type Prepare<C, T, M> =
    Pin<Box<dyn Future<Output = (String, Grant, Result<ServiceConnection<C, T, M>>)> + Send>>;
type Callback<C, T, M> = Pin<Box<dyn Future<Output = CallbackDone<C, T, M>> + Send>>;
type Ending = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
enum CallbackKind {
    Connected,
    Message { dispatched: bool },
}
struct CallbackDone<C: StateStore, T: StateStore, M: MessageTransport> {
    key: String,
    connection: ServiceConnection<C, T, M>,
    kind: CallbackKind,
    result: Result<()>,
}
struct Workers<C: StateStore, T: StateStore, M: MessageTransport> {
    preparing: BTreeSet<String>,
    prepare: FuturesUnordered<Prepare<C, T, M>>,
    callbacks: FuturesUnordered<Callback<C, T, M>>,
    ending: FuturesUnordered<Ending>,
    pending: VecDeque<Delivery>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Default for Workers<C, T, M> {
    fn default() -> Self {
        Self {
            preparing: BTreeSet::new(),
            prepare: FuturesUnordered::new(),
            callbacks: FuturesUnordered::new(),
            ending: FuturesUnordered::new(),
            pending: VecDeque::new(),
        }
    }
}
fn ending<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Arc<Shared<C, T, M>>,
    connection: ServiceConnection<C, T, M>,
) -> Ending {
    let shared = shared.clone();
    Box::pin(async move {
        std::panic::AssertUnwindSafe(crate::lifecycle::deadline(
            shared.clock.as_ref(),
            shared.options.callback_timeout,
            "service disconnect callback",
            async { shared.backend.disconnected(connection).await },
        ))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| {
            Err(Error::StateUnavailable(
                "service disconnect callback panicked".into(),
            ))
        })
    })
}
fn reconcile<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Arc<Shared<C, T, M>>,
    participant: &ManagedParticipant<C, T>,
    workers: &mut Workers<C, T, M>,
) -> Result<()> {
    let ended = {
        let mut uses = shared
            .uses
            .lock()
            .expect("provider connection lock poisoned");
        let keys = uses
            .iter()
            .filter(|(_, owned)| owned.connection.authorize().is_err())
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| uses.remove(&key))
            .collect::<Vec<_>>()
    };
    for owned in ended {
        if owned.announced {
            if workers.ending.len() >= shared.options.max_connections {
                return Err(Error::StateUnavailable(
                    "provider disconnect cleanup capacity exceeded".into(),
                ));
            }
            workers.ending.push(ending(shared, owned.connection));
        }
    }
    let members = participant.state().value.contracts;
    let grants = {
        let mut admissions = shared
            .admissions
            .lock()
            .expect("provider admission lock poisoned");
        admissions.grants.retain(|key, grant| {
            members
                .get(key)
                .map_or(!grant.revoked.is_cancelled(), |member| {
                    member.phase != Phase::Closed
                })
        });
        admissions.grants.clone()
    };
    for (key, member) in members {
        if !member.value.local_authority
            || workers.preparing.contains(&key)
            || shared
                .uses
                .lock()
                .expect("provider connection lock poisoned")
                .contains_key(&key)
        {
            continue;
        }
        let Some(grant) = grants
            .get(&key)
            .filter(|grant| !grant.revoked.is_cancelled())
            .cloned()
        else {
            continue;
        };
        let Some(contract) = member.value.contract else {
            continue;
        };
        let Some(agreement) = participant.agreement(&ContractPointer {
            contract_id: contract.contract_id,
            generation: contract.generation,
        }) else {
            continue;
        };
        let protocol = shared.protocol.clone();
        let descriptor = shared.descriptor.clone();
        let messages = shared.messages.clone();
        let stop = shared.stop.clone();
        let clock = shared.clock.clone();
        let timeout = shared.options.callback_timeout;
        workers.preparing.insert(key.clone());
        workers.prepare.push(Box::pin(async move {
            let consent=grant.revoked.clone();let permissions=grant.permissions.clone();
            let initialize=async {
                let context=tokio::task::spawn_blocking(move || {let access=permissions.access(&descriptor)?;ServiceContext::new(agreement,protocol,descriptor,access)}).await.map_err(|error|Error::StateUnavailable(format!("provider context validation failed: {error}")))??;
                ServiceConnection::with_stop(context,messages,consent.clone())
            };
            let result=tokio::select! {biased;
                _=stop.cancelled()=>Err(Error::Closed("provider closed during context preparation".into())),
                _=consent.cancelled()=>Err(Error::Unauthorized("service consent ended during context preparation".into())),
                result=crate::lifecycle::deadline(clock.as_ref(),timeout,"provider context preparation",initialize)=>result,
            };
            (key,grant,result)
        }));
    }
    Ok(())
}
fn schedule<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Arc<Shared<C, T, M>>,
    workers: &mut Workers<C, T, M>,
) {
    let initial = {
        let mut uses = shared
            .uses
            .lock()
            .expect("provider connection lock poisoned");
        let capacity = shared
            .options
            .max_in_flight
            .saturating_sub(workers.callbacks.len());
        uses.iter_mut()
            .filter(|(_, owned)| !owned.announced && owned.connection.authorize().is_ok())
            .take(capacity)
            .map(|(key, owned)| {
                owned.announced = true;
                (key.clone(), owned.connection.clone())
            })
            .collect::<Vec<_>>()
    };
    for (key, connection) in initial {
        let shared = shared.clone();
        workers.callbacks.push(Box::pin(async move {
            let result =
                std::panic::AssertUnwindSafe(connection.while_current(crate::lifecycle::deadline(
                    shared.clock.as_ref(),
                    shared.options.callback_timeout,
                    "service connection callback",
                    async { shared.backend.connected(connection.clone()).await },
                )))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(Error::StateUnavailable(
                        "service connection callback panicked".into(),
                    ))
                });
            CallbackDone {
                key,
                connection,
                kind: CallbackKind::Connected,
                result,
            }
        }));
    }
    for _ in 0..workers.pending.len() {
        let delivery = workers.pending.pop_front().expect("pending delivery");
        let Some(pointer) = &delivery.message.contract else {
            shared.reject(&Error::Unauthorized(
                "service message has no agreement".into(),
            ));
            continue;
        };
        let key = crate::keys::concord_contract_key(&pointer.contract_id, pointer.generation);
        let grant = shared
            .admissions
            .lock()
            .expect("provider admission lock poisoned")
            .grants
            .get(&key)
            .cloned();
        if grant.is_none_or(|grant| grant.revoked.is_cancelled())
            || delivery.message.is_expired_at(shared.clock.utc_now())
        {
            shared.reject(&Error::Unauthorized(
                "queued service message expired or lost consent".into(),
            ));
            continue;
        }
        let connection = shared
            .uses
            .lock()
            .expect("provider connection lock poisoned")
            .get(&key)
            .filter(|owned| owned.ready)
            .map(|owned| owned.connection.clone());
        if workers.callbacks.len() >= shared.options.max_in_flight || connection.is_none() {
            workers.pending.push_back(delivery);
            continue;
        }
        let connection = connection.expect("ready connection");
        let shared = shared.clone();
        workers.callbacks.push(Box::pin(async move {
            let mut dispatched = false;
            let work = async {
                connection.authorize_incoming(&delivery.message)?;
                connection
                    .context()
                    .validate_message(&delivery.message, false)
                    .await?;
                connection.authorize_incoming(&delivery.message)?;
                dispatched = true;
                shared
                    .backend
                    .handle(ServiceIncoming {
                        delivery,
                        connection: connection.clone(),
                    })
                    .await
            };
            let result = std::panic::AssertUnwindSafe(connection.while_current(
                crate::lifecycle::deadline(
                    shared.clock.as_ref(),
                    shared.options.callback_timeout,
                    "service handler",
                    work,
                ),
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(Error::StateUnavailable("service handler panicked".into())));
            CallbackDone {
                key,
                connection,
                kind: CallbackKind::Message { dispatched },
                result,
            }
        }));
    }
}
async fn serve<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Arc<Shared<C, T, M>>,
    participant: &ManagedParticipant<C, T>,
    mut messages: MessageSubscription,
    mut profiles: crate::concord::ConcordStateWatch,
    mut publication: StateWatch<AdvertisementState>,
    workers: &mut Workers<C, T, M>,
) -> Result<()> {
    let mut participation = participant.watch_state()?;
    let mut facts = shared.facts.subscribe();
    let mut publication_open = true;
    loop {
        reconcile(shared, participant, workers)?;
        schedule(shared, workers);
        shared.observe();
        tokio::select! {
            change=profiles.next()=>{
                if change.is_none() || change.is_some_and(|state|state.closed) {return Err(Error::Closed("provider Concord sources closed".into()));}
                enforce_profiles(shared);
            },
            change=participation.next()=>{
                if change.is_none() || change.is_some_and(|state|matches!(state.phase,Phase::Closed|Phase::Terminal)) {return Err(Error::Closed("provider participation ended".into()));}
            },
            change=publication.next(),if publication_open=>{
                if let Some(snapshot)=change {
                    publication_open=snapshot.phase!=Phase::Closed;
                    shared.state.modify(|state| {state.value.discovery_current=snapshot.current;state.value.discovery_failure=snapshot.failure;state.value.advertisement=Some(snapshot.value);true});
                } else {publication_open=false;shared.state.modify(|state| {state.value.discovery_current=false;true});}
            },
            change=facts.changed()=>{change.map_err(|_|Error::Closed("provider facts ended".into()))?;shared.revoke_known(false);},
            prepared=workers.prepare.next(),if !workers.prepare.is_empty()=>{
                let (key,grant,result)=prepared.expect("pending preparation");workers.preparing.remove(&key);
                match result {
                    Ok(connection) if connection.authorize().is_ok()&&!shared.stop.is_cancelled()=>{
                        shared.uses.lock().expect("provider connection lock poisoned").insert(key,Use {connection,consumer:grant.consumer,announced:false,ready:false});
                    },
                    Ok(_)=>{},
                    Err(error)=>{if !grant.revoked.is_cancelled() {shared.failure("provider context",&error);grant.revoked.cancel();close_keys(shared,&BTreeSet::from([key]),"service context unavailable");}},
                }
            },
            completed=workers.callbacks.next(),if !workers.callbacks.is_empty()=>{
                let completed=completed.expect("pending backend callback");
                match completed.kind {
                    CallbackKind::Connected=>{
                        if let Some(owned)=shared.uses.lock().expect("provider connection lock poisoned").get_mut(&completed.key) {owned.ready=true;}
                        if completed.connection.authorize().is_ok() {if let Err(error)=completed.result {shared.backend_failure(&error);}}
                    },
                    CallbackKind::Message {dispatched}=>match completed.result {
                        Ok(())=>shared.state.modify(|state| {state.value.handled_messages=state.value.handled_messages.saturating_add(1);true}),
                        Err(error) if !dispatched=>shared.reject(&error),
                        Err(error) if completed.connection.authorize().is_ok()=>shared.backend_failure(&error),
                        Err(_)=>{},
                    },
                }
            },
            result=workers.ending.next(),if !workers.ending.is_empty()=>{if let Some(Err(error))=result {shared.backend_failure(&error);}},
            delivery=messages.next()=>{
                let delivery=delivery.ok_or_else(||Error::Closed("provider message subscription ended".into()))??;
                if delivery.message.in_reply_to.is_some() {shared.reject(&Error::Invalid("unsolicited service reply".into()));continue;}
                let accepted=delivery.message.contract.as_ref().is_some_and(|pointer| {
                    shared.admissions.lock().expect("provider admission lock poisoned").grants.get(&crate::keys::concord_contract_key(&pointer.contract_id,pointer.generation)).is_some_and(|grant|!grant.revoked.is_cancelled())
                });
                if !accepted {shared.reject(&Error::Unauthorized("service message has no admitted agreement".into()));continue;}
                if workers.pending.len()>=shared.options.max_pending_messages {shared.reject(&Error::StateUnavailable("provider message queue is full".into()));continue;}
                workers.pending.push_back(delivery);
            },
        }
    }
}
async fn publication_updates<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    advertisement: ManagedAdvertisement,
    mut payload: ServiceAdvertisementPayload,
) {
    let mut facts = shared.facts.subscribe();
    loop {
        let backend = facts.borrow_and_update().backend.clone();
        if payload.backend_status != backend.status || payload.diagnostics != backend.diagnostics {
            payload.backend_status = backend.status;
            payload.diagnostics = backend.diagnostics;
            let content = payload.to_value().map(|payload| AdvertisementContent {
                payload: Some(payload),
                ..Default::default()
            });
            let result = match content {
                Ok(content) => crate::lifecycle::deadline(
                    shared.clock.as_ref(),
                    Duration::from_secs(5),
                    "service advertisement update",
                    advertisement.update(content),
                )
                .await
                .map(|_| ()),
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                shared.state.modify(|state| {
                    state.value.discovery_current = false;
                    state.value.discovery_failure =
                        Some(Failure::from_error("service advertisement update", &error));
                    true
                });
            }
        }
        if facts.changed().await.is_err() {
            return;
        }
    }
}
async fn bootstrap_provider<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
) -> Result<()> {
    let mut retry_delay = Duration::from_millis(100);
    loop {
        let result = crate::lifecycle::deadline(
            shared.clock.as_ref(),
            shared.options.bootstrap_timeout,
            "service provider bootstrap",
            shared.concord.bootstrap(
                shared.messages.session(),
                Some(&shared.descriptor.use_profile),
                true,
            ),
        )
        .await;
        match result {
            Ok(()) => {
                shared.state.modify(|state| {
                    state.failure = None;
                    true
                });
                return Ok(());
            }
            Err(error) => {
                let failure = Failure::from_error("service provider bootstrap", &error);
                if !matches!(
                    failure.kind,
                    FailureKind::Conflict | FailureKind::Unavailable | FailureKind::Deadline
                ) {
                    return Err(error);
                }
                shared.state.modify(|state| {
                    state.phase = Phase::Unavailable;
                    state.current = false;
                    state.failure = Some(failure);
                    true
                });
            }
        }
        // The supervisor cancels both attempts and waits on endpoint/provider
        // closure. A new session never displaces live participation.
        shared
            .clock
            .sleep_until(shared.clock.now() + retry_delay)
            .await;
        retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
    }
}

async fn supervise<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    shared: Arc<Shared<C, T, M>>,
    advertisements: B,
    mut payload: ServiceAdvertisementPayload,
) {
    let mut workers = Workers::default();
    let work = async {
        let messages = shared.messages.subscribe(SERVICES_LANE).await?;
        let profiles = shared.concord.watch_state(ContractFilters {
            participant: Some(&shared.descriptor.endpoint),
            profile: Some(&shared.descriptor.use_profile),
            ..Default::default()
        })?;
        bootstrap_provider(&shared).await?;
        let participant = {
            let mut owned = shared
                .participant
                .lock()
                .expect("provider participant lock poisoned");
            if shared.stop.is_cancelled() {
                return Err(Error::Closed("provider closed during bootstrap".into()));
            }
            let participant = shared.concord.participate(
                shared.messages.session().clone(),
                Some(shared.descriptor.use_profile.clone()),
                Admission {
                    shared: shared.clone(),
                    completed_generation: AtomicU64::new(0),
                },
            )?;
            *owned = Some(participant.clone());
            participant
        };
        participant
            .wait_for(
                |state| state.current,
                Some(shared.options.bootstrap_timeout),
            )
            .await?;
        let backend = shared.facts.borrow().backend.clone();
        payload.backend_status = backend.status;
        payload.diagnostics = backend.diagnostics;
        let advertisement = {
            let mut owned = shared
                .advertisement
                .lock()
                .expect("provider advertisement lock poisoned");
            if shared.stop.is_cancelled() {
                return Err(Error::Closed("provider closed before publication".into()));
            }
            let advertisement = BeaconAdvertiser::new(
                advertisements,
                shared.protocol.protocol().feature_id.clone(),
                shared.descriptor.endpoint.clone(),
                shared.descriptor.session_id.clone(),
            )
            .clock(shared.clock.clone())
            .payload(payload.to_value()?)
            .manage()?;
            *owned = Some(advertisement.clone());
            advertisement
        };
        let publication = advertisement.watch_state()?;
        shared.state.modify(|state| {
            state.value.bootstrapped = true;
            true
        });
        shared.observe();
        tokio::select! {
            result=serve(&shared,&participant,messages,profiles,publication,&mut workers)=>result,
            _=publication_updates(&shared,advertisement,payload)=>Err(Error::Closed("provider publication updater ended".into())),
        }
    };
    let result = std::panic::AssertUnwindSafe(async {
        tokio::select! {biased;
            _=shared.stop.cancelled()=>Ok(()),
            _=shared.messages.session().closed()=>Ok(()),
            result=work=>result,
        }
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| {
        Err(Error::StateUnavailable(
            "service provider worker panicked".into(),
        ))
    });
    if let Err(error) = result {
        shared.failure("service provider", &error);
    }
    shared.close();
    workers.prepare.clear();
    workers.callbacks.clear();
    workers.pending.clear();
    let remaining = std::mem::take(
        &mut *shared
            .uses
            .lock()
            .expect("provider connection lock poisoned"),
    );
    for (_, owned) in remaining {
        if owned.announced {
            workers.ending.push(ending(&shared, owned.connection));
        }
    }
    let participant = shared
        .participant
        .lock()
        .expect("provider participant lock poisoned")
        .take();
    let advertisement = shared
        .advertisement
        .lock()
        .expect("provider advertisement lock poisoned")
        .take();
    let cleanup = async {
        let participant = async {
            if let Some(participant) = participant {
                participant.close().await
            } else {
                Ok(())
            }
        };
        let advertisement = async {
            if let Some(advertisement) = advertisement {
                advertisement.close().await
            } else {
                Ok(())
            }
        };
        let backend = async {
            let shutdown = async {
                std::panic::AssertUnwindSafe(async { shared.backend.shutdown().await })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(Error::StateUnavailable(
                            "service backend shutdown panicked".into(),
                        ))
                    })
            };
            let disconnect = async {
                let mut failure = None;
                while let Some(result) = workers.ending.next().await {
                    if let Err(error) = result {
                        shared.backend_failure(&error);
                        failure = Some(error);
                    }
                }
                failure.map_or(Ok(()), Err)
            };
            let (shutdown, disconnect) = futures_util::future::join(shutdown, disconnect).await;
            shutdown.and(disconnect)
        };
        let (participant, advertisement, backend) =
            futures_util::future::join3(participant, advertisement, backend).await;
        participant.and(advertisement).and(backend)
    };
    let result = std::panic::AssertUnwindSafe(crate::lifecycle::deadline(
        shared.clock.as_ref(),
        Duration::from_secs(5),
        "service provider cleanup",
        cleanup,
    ))
    .catch_unwind()
    .await
    .unwrap_or_else(|_| {
        Err(Error::StateUnavailable(
            "service provider cleanup panicked".into(),
        ))
    });
    if let Err(error) = result {
        *shared
            .cleanup_failure
            .lock()
            .expect("provider cleanup lock poisoned") =
            Some(Failure::from_error("provider cleanup", &error));
        shared.failure("provider cleanup", &error);
    }
    workers.ending.clear();
    shared.state.modify(|state| {
        state.phase = Phase::Closed;
        state.current = false;
        state.value.participant_current = false;
        state.value.discovery_current = false;
        state.value.connections.clear();
        true
    });
}
