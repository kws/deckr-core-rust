//! Managed service discovery, bounded concurrent negotiation and successors.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::sync::broadcast;

use crate::concord::{AgreementContext, AgreementIntent, AgreementState};
use crate::endpoint::EndpointAddress;
use crate::lanes::SERVICES_LANE;
use crate::lifecycle::{Cancellation, Clock, Failure, Phase, Snapshot, StateCell, StateWatch};
use crate::messaging::{MessageAuthority, MessageSession, MessageSubscription, MessageTransport};
use crate::runtime::MaterializedConcord;
use crate::state::StateStore;
use crate::{Error, Result};

use super::*;

pub const MAX_SERVICE_CANDIDATES: usize = 64;
pub type ServiceCandidateFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ServiceAccess>>> + Send + 'a>>;
/// Return local permissions to admit a candidate, or None to reject it. Policy
/// runs independently for each candidate and never owns Concord operations.
pub trait ServiceCandidatePolicy: Send + Sync + 'static {
    fn access(&self, descriptor: ServiceDescriptor) -> ServiceCandidateFuture<'_>;
}
impl<F, Fut> ServiceCandidatePolicy for F
where
    F: Fn(ServiceDescriptor) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<ServiceAccess>>> + Send + 'static,
{
    fn access(&self, descriptor: ServiceDescriptor) -> ServiceCandidateFuture<'_> {
        Box::pin(self(descriptor))
    }
}
#[derive(Debug, Clone)]
pub enum ServiceTarget {
    Matching(Box<ServiceQuery>),
    /// The original advertisement key, endpoint, session and descriptor meaning
    /// remain fixed. Refresh metadata may advance; discovery must be current.
    Exact(Box<ServiceDescriptor>),
}
impl ServiceTarget {
    pub fn matching(query: ServiceQuery) -> Self {
        Self::Matching(Box::new(query))
    }
    pub fn exact(descriptor: ServiceDescriptor) -> Self {
        Self::Exact(Box::new(descriptor))
    }
}
impl Default for ServiceTarget {
    fn default() -> Self {
        Self::matching(ServiceQuery::default())
    }
}
#[derive(Debug, Clone)]
pub struct ServiceClientOptions {
    pub negotiation_timeout: Duration,
    pub retry_delay: Duration,
}
impl Default for ServiceClientOptions {
    fn default() -> Self {
        Self {
            negotiation_timeout: Duration::from_secs(5),
            retry_delay: Duration::from_secs(1),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceCandidateIdentity {
    pub endpoint: EndpointAddress,
    pub session_id: String,
}
impl ServiceCandidateIdentity {
    fn of(descriptor: &ServiceDescriptor) -> Self {
        Self {
            endpoint: descriptor.endpoint.clone(),
            session_id: descriptor.session_id.clone(),
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ServiceClientStatus {
    #[default]
    Connecting,
    Ready,
    Reconnecting,
    Closed,
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ServiceClientState {
    pub status: ServiceClientStatus,
    pub discovery_current: bool,
    pub admitted: Vec<ServiceCandidateIdentity>,
    pub selected: Option<ServiceDescriptor>,
    pub agreement: Option<AgreementState>,
    pub local_authority: bool,
    pub rounds: u64,
    pub attempts: u64,
    pub rejected_messages: u64,
    pub received_messages: u64,
}
pub struct ServiceClientSpec<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport> {
    /// Borrowed host-owned index; clients do not create duplicate Beacon views.
    pub directory: ServiceDirectory<B>,
    pub protocol: ValidatedServiceProtocol,
    pub concord: MaterializedConcord<C, T>,
    pub messages: MessageSession<M>,
    pub target: ServiceTarget,
    pub policy: Arc<dyn ServiceCandidatePolicy>,
    pub options: ServiceClientOptions,
}
struct Shared<C: StateStore, T: StateStore, M: MessageTransport> {
    messages: MessageSession<M>,
    clock: Arc<dyn Clock>,
    stop: Cancellation,
    state: StateCell<ServiceClientState>,
    owned: Mutex<BTreeMap<String, AgreementContext<C, T>>>,
    selected: Mutex<Option<ServiceConnection<C, T, M>>>,
    established: AtomicBool,
    incoming: broadcast::Sender<ServiceIncoming<C, T, M>>,
    registration: Mutex<()>,
    cleanup_failure: Mutex<Option<Failure>>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Shared<C, T, M> {
    fn close(&self) {
        self.stop.cancel();
        for agreement in self
            .owned
            .lock()
            .expect("service ownership lock poisoned")
            .values()
        {
            agreement.request_close();
        }
        self.state.modify(|state| {
            state.phase = Phase::Closing;
            state.current = false;
            state.value.local_authority = false;
            state.value.status = ServiceClientStatus::Closed;
            true
        });
    }
    fn failure(&self, operation: &str, error: &Error) {
        self.state.modify(|state| {
            state.failure = Some(Failure::from_error(operation, error));
            true
        });
    }
    fn observe(&self) {
        let selected = self
            .selected
            .lock()
            .expect("service selection lock poisoned")
            .clone();
        if let Some(connection) = selected {
            let authority = connection.authorize().is_ok();
            let agreement = connection.context().agreement().state();
            self.state.modify(|state| {
                if matches!(state.phase, Phase::Closing | Phase::Closed) {
                    return false;
                }
                let status = if authority {
                    ServiceClientStatus::Ready
                } else {
                    ServiceClientStatus::Reconnecting
                };
                if state.value.status == status
                    && state.value.agreement.as_ref() == Some(&agreement.value)
                    && state.current == authority
                {
                    return false;
                }
                state.phase = if authority {
                    Phase::Ready
                } else {
                    Phase::Unavailable
                };
                state.current = authority;
                state.value.local_authority = authority;
                state.value.status = status;
                state.value.agreement = Some(agreement.value);
                if agreement.failure.is_some() {
                    state.failure = agreement.failure;
                }
                true
            });
        } else {
            self.state.modify(|state| {
                if !state.value.local_authority
                    || matches!(state.phase, Phase::Closing | Phase::Closed)
                {
                    return false;
                }
                state.phase = Phase::Unavailable;
                state.current = false;
                state.value.local_authority = false;
                state.value.status = ServiceClientStatus::Reconnecting;
                true
            });
        }
    }
    fn reconnect(&self) {
        *self
            .selected
            .lock()
            .expect("service selection lock poisoned") = None;
        self.state.modify(|state| {
            if matches!(state.phase, Phase::Closing | Phase::Closed) {
                return false;
            }
            state.phase = Phase::Unavailable;
            state.current = false;
            state.value.local_authority = false;
            state.value.status = if self.established.load(Ordering::SeqCst) {
                ServiceClientStatus::Reconnecting
            } else {
                ServiceClientStatus::Connecting
            };
            state.value.selected = None;
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
pub struct ServiceClient<C: StateStore, T: StateStore, M: MessageTransport> {
    owner: Arc<Owner<C, T, M>>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Clone for ServiceClient<C, T, M> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> ServiceClient<C, T, M> {
    pub fn start<B: StateStore>(spec: ServiceClientSpec<B, C, T, M>) -> Result<Self> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("service client requires a Tokio runtime".into()))?;
        if spec.messages.session().is_closed()
            || matches!(spec.messages.state().phase, Phase::Closing | Phase::Closed)
        {
            return Err(Error::Closed("service client endpoint is closed".into()));
        }
        if spec.directory.protocol() != spec.protocol.protocol() {
            return Err(Error::Invalid(
                "service directory and client declarations differ".into(),
            ));
        }
        if let ServiceTarget::Exact(descriptor) = &spec.target {
            descriptor.validate(spec.protocol.protocol())?;
            if descriptor.candidate.is_none() {
                return Err(Error::Invalid(
                    "exact service selection requires a discovery candidate".into(),
                ));
            }
        }
        let clock = spec.concord.runtime_clock();
        for limit in [spec.options.negotiation_timeout, spec.options.retry_delay] {
            if limit.is_zero() || clock.now().checked_add(limit).is_none() {
                return Err(Error::Invalid(
                    "service client time limits must be finite and positive".into(),
                ));
            }
        }
        spec.directory.try_start()?;
        let discovery = spec.directory.watch_state()?;
        let shared = Arc::new(Shared {
            messages: spec.messages.clone(),
            clock,
            stop: Cancellation::default(),
            state: StateCell::new(ServiceClientState::default()),
            owned: Mutex::default(),
            selected: Mutex::default(),
            established: AtomicBool::new(false),
            incoming: broadcast::channel(256).0,
            registration: Mutex::default(),
            cleanup_failure: Mutex::default(),
        });
        tokio::spawn(supervise(shared.clone(), spec, discovery));
        Ok(Self {
            owner: Arc::new(Owner { shared }),
        })
    }
    pub fn state(&self) -> Snapshot<ServiceClientState> {
        self.owner.shared.observe();
        self.owner.shared.state.snapshot()
    }
    pub fn watch_state(&self) -> Result<StateWatch<ServiceClientState>> {
        self.state();
        self.owner.shared.state.watch()
    }
    pub async fn wait_for(
        &self,
        predicate: impl FnMut(&Snapshot<ServiceClientState>) -> bool,
        timeout: Option<Duration>,
    ) -> Result<Snapshot<ServiceClientState>> {
        self.state();
        self.owner
            .shared
            .state
            .wait_for(predicate, timeout, self.owner.shared.clock.as_ref())
            .await
    }
    pub fn connection(&self) -> Result<ServiceConnection<C, T, M>> {
        let connection = self
            .owner
            .shared
            .selected
            .lock()
            .expect("service selection lock poisoned")
            .clone()
            .ok_or_else(|| {
                Error::Unauthorized("service client has no accepted agreement".into())
            })?;
        connection.authorize()?;
        Ok(connection)
    }
    pub async fn wait_ready(
        &self,
        timeout: Option<Duration>,
    ) -> Result<ServiceConnection<C, T, M>> {
        self.wait_for(|state| state.value.local_authority, timeout)
            .await?;
        self.connection()
    }
    pub fn watch_messages(&self) -> Result<ServiceMessages<C, T, M>> {
        let shared = &self.owner.shared;
        let _registration = shared
            .registration
            .lock()
            .expect("service message registration lock poisoned");
        if shared.stop.is_cancelled() {
            return Err(Error::Closed("service client closed".into()));
        }
        if shared.incoming.receiver_count() >= 256 {
            return Err(Error::StateUnavailable(
                "service message subscriber limit reached".into(),
            ));
        }
        Ok(ServiceMessages {
            receiver: Some(shared.incoming.subscribe()),
            stop: shared.stop.clone(),
        })
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
            .expect("service cleanup lock poisoned")
            .as_ref()
        {
            return Err(Error::StateUnavailable(failure.detail.clone()));
        }
        Ok(())
    }
}
pub struct ServiceMessages<C: StateStore, T: StateStore, M: MessageTransport> {
    receiver: Option<broadcast::Receiver<ServiceIncoming<C, T, M>>>,
    stop: Cancellation,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> ServiceMessages<C, T, M> {
    pub async fn next(&mut self) -> Option<Result<ServiceIncoming<C, T, M>>> {
        loop {
            let item = tokio::select! {biased;
                _=self.stop.cancelled()=>{self.receiver=None;return None;},
                item=self.receiver.as_mut()?.recv()=>item,
            };
            match item {
                Ok(item) if item.authorize().is_ok() => return Some(Ok(item)),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    self.receiver = None;
                    return None;
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    self.receiver = None;
                    return Some(Err(Error::StateUnavailable(format!(
                        "service message subscriber lost {count} deliveries"
                    ))));
                }
            }
        }
    }
}
async fn supervise<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    shared: Arc<Shared<C, T, M>>,
    spec: ServiceClientSpec<B, C, T, M>,
    discovery: StateWatch<ServiceDirectoryState>,
) {
    let work = async {
        let mut messages = shared.messages.subscribe(SERVICES_LANE).await?;
        tokio::select! {
            result=negotiate(&shared,&spec,discovery)=>result,
            result=receive(&shared,&mut messages)=>result,
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
            "service client worker panicked".into(),
        ))
    });
    if let Err(error) = result {
        shared.failure("service client", &error);
    }
    shared.close();
    close_owned(&shared, None).await;
    *shared
        .selected
        .lock()
        .expect("service selection lock poisoned") = None;
    shared.state.modify(|state| {
        state.phase = Phase::Closed;
        state.current = false;
        state.value.local_authority = false;
        state.value.selected = None;
        true
    });
}
async fn close_owned<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    keep: Option<&str>,
) {
    let owned: Vec<_> = shared
        .owned
        .lock()
        .expect("service ownership lock poisoned")
        .iter()
        .filter(|(key, _)| Some(key.as_str()) != keep)
        .map(|(key, agreement)| (key.clone(), agreement.clone()))
        .collect();
    // All losers relinquish local authority before any winner can be exposed.
    for (_, agreement) in &owned {
        agreement.request_close();
    }
    let mut cleanup: FuturesUnordered<_> = owned
        .into_iter()
        .map(|(key, agreement)| async move {
            let result = std::panic::AssertUnwindSafe(agreement.close())
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(Error::StateUnavailable(
                        "service agreement cleanup panicked".into(),
                    ))
                });
            (key, result)
        })
        .collect();
    while let Some((key, result)) = cleanup.next().await {
        shared
            .owned
            .lock()
            .expect("service ownership lock poisoned")
            .remove(&key);
        if let Err(error) = result {
            *shared
                .cleanup_failure
                .lock()
                .expect("service cleanup lock poisoned") =
                Some(Failure::from_error("service agreement cleanup", &error));
            shared.failure("service agreement cleanup", &error);
        }
    }
}
async fn receive<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    messages: &mut MessageSubscription,
) -> Result<()> {
    while let Some(delivery) = messages.next().await {
        let delivery = delivery?;
        let connection = shared
            .selected
            .lock()
            .expect("service selection lock poisoned")
            .clone();
        let Some(connection) = connection else {
            continue;
        };
        let contract = connection.context().agreement().contract();
        if delivery.message.in_reply_to.is_some()
            || delivery.message.contract.as_ref().is_none_or(|pointer| {
                pointer.contract_id != contract.contract_id
                    || pointer.generation != contract.generation
            })
        {
            continue;
        }
        let validate = async {
            connection.authorize_incoming(&delivery.message)?;
            connection
                .context()
                .validate_message(&delivery.message, false)
                .await?;
            connection.authorize_incoming(&delivery.message)
        };
        match validate.await {
            Ok(()) => {
                let _ = shared.incoming.send(ServiceIncoming {
                    delivery,
                    connection,
                });
                shared.state.modify(|state| {
                    state.value.received_messages = state.value.received_messages.saturating_add(1);
                    true
                });
            }
            Err(error) => {
                shared.state.modify(|state| {
                    state.value.rejected_messages = state.value.rejected_messages.saturating_add(1);
                    state.failure = Some(Failure::from_error("service incoming message", &error));
                    true
                });
            }
        }
    }
    Err(Error::Closed("service message subscription ended".into()))
}
fn same_meaning(left: &ServiceDescriptor, right: &ServiceDescriptor) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.candidate = None;
    right.candidate = None;
    left == right
}
fn candidates<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    spec: &ServiceClientSpec<B, C, T, M>,
) -> Vec<ServiceDescriptor> {
    let values = match &spec.target {
        ServiceTarget::Matching(query) => spec.directory.match_services(query),
        ServiceTarget::Exact(exact) => spec
            .directory
            .descriptors()
            .into_iter()
            .filter(|current| {
                current.candidate.as_ref().map(|candidate| &candidate.key)
                    == exact.candidate.as_ref().map(|candidate| &candidate.key)
                    && same_meaning(current, exact)
            })
            .collect(),
    };
    let mut unique = BTreeMap::new();
    for descriptor in values {
        if &descriptor.endpoint != spec.messages.session().endpoint() {
            unique.insert(ServiceCandidateIdentity::of(&descriptor), descriptor);
        }
    }
    unique.into_values().collect()
}
fn discovery_state<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    spec: &ServiceClientSpec<B, C, T, M>,
) {
    let current = spec.directory.is_current();
    shared.state.modify(|state| {
        if state.value.discovery_current == current {
            return false;
        }
        state.value.discovery_current = current;
        true
    });
}
type Evaluation =
    Pin<Box<dyn Future<Output = (ServiceDescriptor, Result<Option<ServiceAccess>>)> + Send>>;
type Attempt<C, T> = Pin<
    Box<
        dyn Future<
                Output = (
                    ServiceDescriptor,
                    ServiceAccess,
                    AgreementContext<C, T>,
                    Result<()>,
                ),
            > + Send,
    >,
>;
async fn race<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    spec: &ServiceClientSpec<B, C, T, M>,
    discovery: &mut StateWatch<ServiceDirectoryState>,
) -> Result<ServiceConnection<C, T, M>> {
    let expires = shared
        .clock
        .now()
        .checked_add(spec.options.negotiation_timeout)
        .ok_or_else(|| Error::Invalid("service negotiation deadline is out of range".into()))?;
    let mut admitted = BTreeSet::new();
    let mut evaluated = BTreeMap::<ServiceCandidateIdentity, ServiceDescriptor>::new();
    let mut evaluating = FuturesUnordered::<Evaluation>::new();
    let mut attempts = FuturesUnordered::<Attempt<C, T>>::new();
    shared.state.modify(|state| {
        state.value.rounds = state.value.rounds.saturating_add(1);
        state.value.admitted.clear();
        true
    });
    loop {
        discovery_state(shared, spec);
        let current = candidates(spec);
        evaluated.retain(|id, _| {
            admitted.contains(id)
                || current
                    .iter()
                    .any(|descriptor| &ServiceCandidateIdentity::of(descriptor) == id)
        });
        for descriptor in current {
            let id = ServiceCandidateIdentity::of(&descriptor);
            if admitted.len() >= MAX_SERVICE_CANDIDATES
                || evaluating.len() >= MAX_SERVICE_CANDIDATES
            {
                break;
            }
            if admitted.contains(&id)
                || evaluated
                    .get(&id)
                    .is_some_and(|old| same_meaning(old, &descriptor))
            {
                continue;
            }
            evaluated.insert(id, descriptor.clone());
            let policy = spec.policy.clone();
            evaluating.push(Box::pin(async move {
                let result = policy.access(descriptor.clone()).await;
                (descriptor, result)
            }));
        }
        tokio::select! {biased;
            _=shared.clock.sleep_until(expires)=>return Err(Error::Deadline("service negotiation timed out".into())),
            result=attempts.next(),if !attempts.is_empty()=>{
                let (descriptor,access,agreement,result)=result.expect("nonempty attempt queue");
                if let Err(error)=result {agreement.request_close();shared.failure("service negotiation",&error);continue;}
                let selected_agreement=agreement.clone();let protocol=spec.protocol.clone();
                let initialize=async {
                    tokio::task::spawn_blocking(move ||ServiceContext::new(selected_agreement,protocol,descriptor,access)).await
                        .map_err(|error|Error::StateUnavailable(format!("service context validation failed: {error}")))?
                };
                let initialized=tokio::select! {biased;
                    _=shared.clock.sleep_until(expires)=>return Err(Error::Deadline("service negotiation timed out".into())),
                    initialized=initialize=>initialized,
                };
                let context=match initialized {
                    Ok(context)=>context,
                    Err(error)=>{agreement.request_close();shared.failure("service context",&error);continue;},
                };
                let connection=ServiceConnection::with_stop(context,shared.messages.clone(),shared.stop.clone())?;
                close_owned(shared,Some(&agreement.contract().key)).await;
                connection.authorize()?;
                return Ok(connection);
            },
            evaluation=evaluating.next(),if !evaluating.is_empty()=>{
                let (descriptor,result)=evaluation.expect("nonempty policy queue");
                let access=match result {Ok(Some(access))=>access,Ok(None)=>continue,Err(error)=>{shared.failure("service candidate policy",&error);continue;}};
                let id=ServiceCandidateIdentity::of(&descriptor);
                if admitted.len()>=MAX_SERVICE_CANDIDATES || admitted.contains(&id) || !candidates(spec).iter().any(|current|ServiceCandidateIdentity::of(current)==id && same_meaning(current,&descriptor)) {continue;}
                if access.service_endpoint!=descriptor.endpoint || access.service_session_id!=descriptor.session_id || access.service_namespace!=descriptor.namespace {
                    shared.failure("service candidate access",&Error::Invalid("candidate policy returned access for another service".into()));continue;
                }
                let access=match service_access(&descriptor,access.allowed_operations,access.allowed_views) {Ok(access)=>access,Err(error)=>{shared.failure("service candidate access",&error);continue;}};
                let session=spec.messages.session();
                let mut intent=AgreementIntent::new(vec![session.endpoint().clone(),descriptor.endpoint.clone()]);
                intent.profile=Some(descriptor.use_profile.clone());
                intent.current_sessions=BTreeMap::from([(session.endpoint().to_string(),session.id().into()),(descriptor.endpoint.to_string(),descriptor.session_id.clone())]);
                let agreement=match spec.concord.propose(session.clone(),intent) {Ok(proposal)=>proposal.agreement(),Err(error)=>{shared.failure("service proposal",&error);continue;}};
                {
                    let mut owned=shared.owned.lock().expect("service ownership lock poisoned");
                    owned.insert(agreement.contract().key.clone(),agreement.clone());
                    if shared.stop.is_cancelled() {agreement.request_close();return Err(Error::Closed("service client closed".into()));}
                }
                admitted.insert(id);
                shared.state.modify(|state| {state.value.attempts=state.value.attempts.saturating_add(1);state.value.admitted=admitted.iter().cloned().collect();true});
                attempts.push(Box::pin(async move {let result=agreement.wait_ready(None).await.map(|_|());(descriptor,access,agreement,result)}));
            },
            change=discovery.next()=>{
                if change.is_none() || change.is_some_and(|state|state.phase==Phase::Closed) {return Err(Error::Closed("service directory ended".into()));}
            },
        }
    }
}
async fn negotiate<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    spec: &ServiceClientSpec<B, C, T, M>,
    mut discovery: StateWatch<ServiceDirectoryState>,
) -> Result<()> {
    loop {
        shared.reconnect();
        let result = race(shared, spec, &mut discovery).await;
        match result {
            Ok(connection) => {
                if let Err(error) = hold(shared, spec, connection, &mut discovery).await {
                    shared.failure("service acceptance", &error);
                }
                shared.reconnect();
            }
            Err(Error::Closed(reason)) => return Err(Error::Closed(reason)),
            Err(error) => shared.failure("service negotiation", &error),
        }
        close_owned(shared, None).await;
        let retry = shared
            .clock
            .now()
            .checked_add(spec.options.retry_delay)
            .ok_or_else(|| Error::Invalid("service retry deadline is out of range".into()))?;
        loop {
            tokio::select! {
                _=shared.clock.sleep_until(retry)=>break,
                change=discovery.next()=>{
                    discovery_state(shared,spec);
                    if change.is_none() || change.is_some_and(|state|state.phase==Phase::Closed) {return Err(Error::Closed("service directory ended".into()));}
                },
            }
        }
    }
}

async fn hold<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    spec: &ServiceClientSpec<B, C, T, M>,
    connection: ServiceConnection<C, T, M>,
    discovery: &mut StateWatch<ServiceDirectoryState>,
) -> Result<()> {
    let mut authority = connection.context().agreement().watch_state()?;
    connection.authorize()?;
    *shared
        .selected
        .lock()
        .expect("service selection lock poisoned") = Some(connection.clone());
    shared.established.store(true, Ordering::SeqCst);
    shared.state.modify(|state| {
        if matches!(state.phase, Phase::Closing | Phase::Closed) {
            return false;
        }
        state.phase = Phase::Ready;
        state.current = true;
        state.value.status = ServiceClientStatus::Ready;
        state.value.local_authority = true;
        state.value.selected = Some(connection.context().descriptor().clone());
        state.value.agreement = Some(connection.context().agreement().state().value);
        state.failure = None;
        true
    });
    let mut discovery_open = true;
    loop {
        shared.observe();
        if connection.authorize().is_err() {
            break;
        }
        tokio::select! {biased;
            change=authority.next()=>{if change.is_none() {break;}},
            change=discovery.next(),if discovery_open=>{discovery_open=change.is_some_and(|state|state.phase!=Phase::Closed);discovery_state(shared,spec);},
        }
    }
    Ok(())
}
