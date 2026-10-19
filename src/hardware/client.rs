//! Managed hardware discovery, claims, operations and event delivery.
//! Selection policy chooses intent; this boundary owns protocol lifetimes.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use tokio::sync::broadcast;

use crate::authority::{AgreementMessageGuard, ContractPointer};
use crate::beacon::{Beacon, BeaconSnapshot, BeaconStateWatch};
use crate::concord::{AgreementContext, AgreementIntent, AgreementState};
use crate::endpoint::{EndpointAddress, CONTROLLER_FAMILY};
use crate::lanes::{DeckrMessage, HardwareMessageBody, HARDWARE_MESSAGES_LANE};
use crate::lifecycle::{Cancellation, Clock, Failure, Phase, Snapshot, StateCell, StateWatch};
use crate::messaging::{
    MessageAuthority, MessageSession, MessageSubscription, MessageTransport, ReplyAcceptance,
};
use crate::profiles::hardware::{
    hardware_payload_from_advertisement, HardwareAdvertisementDevice, HardwareClaimDevice,
    HardwareClaimTerms, HARDWARE_CLAIM_PROFILE_ID, HARDWARE_FEATURE_ID,
};
use crate::runtime::MaterializedConcord;
use crate::schema::SchemaRegistry;
use crate::state::StateStore;
use crate::{Error, Result};

use super::validation::{validate_reply, ValidatedDevice};

#[derive(Clone, Debug, PartialEq)]
pub struct HardwareCandidate {
    pub advertisement_key: String,
    pub manager: EndpointAddress,
    pub manager_session_id: String,
    pub labels: BTreeMap<String, String>,
    pub devices: BTreeMap<String, HardwareAdvertisementDevice>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HardwareSelection {
    pub advertisement_key: String,
    /// Device IDs and requested instance counts within one manager candidate.
    pub devices: BTreeMap<String, u64>,
}

pub type HardwareSelectionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<HardwareSelection>>> + Send + 'a>>;
pub trait HardwareSelectionPolicy: Send + Sync + 'static {
    fn select(&self, candidates: Vec<HardwareCandidate>) -> HardwareSelectionFuture<'_>;
}
impl<F, Fut> HardwareSelectionPolicy for F
where
    F: Fn(Vec<HardwareCandidate>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<HardwareSelection>>> + Send + 'static,
{
    fn select(&self, candidates: Vec<HardwareCandidate>) -> HardwareSelectionFuture<'_> {
        Box::pin(self(candidates))
    }
}

#[derive(Debug, Clone)]
pub struct HardwareClientOptions {
    pub negotiation_timeout: Duration,
    pub retry_delay: Duration,
}
impl Default for HardwareClientOptions {
    fn default() -> Self {
        Self {
            negotiation_timeout: Duration::from_secs(5),
            retry_delay: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct HardwareClientState {
    pub discovery_current: bool,
    pub candidates: Vec<HardwareCandidate>,
    pub selected: Option<HardwareSelection>,
    pub agreement: Option<AgreementState>,
    pub local_authority: bool,
    pub attempts: u64,
    pub rejected_messages: u64,
    pub received_events: u64,
}

pub struct HardwareClientSpec<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport> {
    pub beacon: Beacon<B>,
    pub concord: MaterializedConcord<C, T>,
    /// Borrowed endpoint lifetime; closing this client does not close messaging.
    pub messages: MessageSession<M>,
    pub selection: Arc<dyn HardwareSelectionPolicy>,
    pub schemas: SchemaRegistry,
    pub options: HardwareClientOptions,
}

struct Scope<C: StateStore, T: StateStore, M: MessageTransport> {
    guard: AgreementMessageGuard<C, T>,
    manager: EndpointAddress,
    manager_session: String,
    devices: BTreeMap<String, Arc<ValidatedDevice>>,
    messages: MessageSession<M>,
    stop: Cancellation,
    ended: Cancellation,
    clock: Arc<dyn Clock>,
}

/// An immutable selected claim. Retained contexts never acquire a successor's
/// authority; obtain a new context from the client after recovery.
pub struct HardwareClientContext<C: StateStore, T: StateStore, M: MessageTransport>(
    Arc<Scope<C, T, M>>,
);
impl<C: StateStore, T: StateStore, M: MessageTransport> Clone for HardwareClientContext<C, T, M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> HardwareClientContext<C, T, M> {
    pub fn agreement(&self) -> &AgreementContext<C, T> {
        self.0.guard.agreement()
    }
    pub fn devices(&self) -> &BTreeMap<String, Arc<ValidatedDevice>> {
        &self.0.devices
    }
    pub fn manager(&self) -> &EndpointAddress {
        &self.0.manager
    }
    pub fn authorize(&self) -> Result<()> {
        if self.0.stop.is_cancelled()
            || self.0.ended.is_cancelled()
            || matches!(
                self.0.messages.state().phase,
                Phase::Closing | Phase::Closed
            )
        {
            return Err(Error::Closed("hardware client is closed".into()));
        }
        self.agreement().authorize()
    }
    fn device(&self, body: &HardwareMessageBody) -> Result<Arc<ValidatedDevice>> {
        if body.device_ref().manager_id != self.0.manager.endpoint_id() {
            return Err(Error::Unauthorized(
                "hardware operation names another manager".into(),
            ));
        }
        let device = self
            .0
            .devices
            .get(&body.device_ref().device_id)
            .cloned()
            .ok_or_else(|| Error::Unauthorized("hardware device is outside this claim".into()))?;
        if body
            .device_ref()
            .fingerprint
            .as_ref()
            .is_some_and(|fingerprint| fingerprint != &device.descriptor().fingerprint)
            || device
                .descriptor()
                .capability(
                    body.control_id(),
                    body.capability_id().expect("hardware capability"),
                )
                .is_none()
        {
            return Err(Error::Unauthorized(
                "hardware resource differs from the selected descriptor".into(),
            ));
        }
        Ok(device)
    }
    async fn validate_body(&self, body: HardwareMessageBody) -> Result<()> {
        let device = self.device(&body)?;
        tokio::task::spawn_blocking(move || device.validate_message(&body))
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("hardware payload validator failed: {error}"))
            })?
    }
    async fn outgoing(&self, body: HardwareMessageBody) -> Result<DeckrMessage> {
        self.authorize()?;
        if !body.is_command() {
            return Err(Error::Invalid(
                "hardware clients send commands and state requests".into(),
            ));
        }
        self.validate_body(body.clone()).await?;
        self.authorize()?;
        let local = self.agreement().session();
        let contract = self.agreement().contract();
        let mut message = DeckrMessage::hardware_command(
            local.endpoint().endpoint_id(),
            local.id(),
            self.0.manager.endpoint_id(),
            &self.0.manager_session,
            &body.device_ref().device_id.clone(),
            ContractPointer {
                contract_id: contract.contract_id.clone(),
                generation: contract.generation,
            },
            body,
        )?;
        message.created_at = self.0.clock.utc_now().to_rfc3339();
        Ok(message)
    }
    pub async fn send(&self, body: HardwareMessageBody) -> Result<()> {
        self.while_current(async {
            let message = self.outgoing(body).await?;
            self.0.messages.send_authorized(message, self).await
        })
        .await
    }
    pub async fn request(
        &self,
        body: HardwareMessageBody,
        timeout: Duration,
        accept: Option<ReplyAcceptance>,
    ) -> Result<DeckrMessage> {
        self.while_current(self.request_inner(body, timeout, accept))
            .await
    }
    async fn request_inner(
        &self,
        body: HardwareMessageBody,
        timeout: Duration,
        accept: Option<ReplyAcceptance>,
    ) -> Result<DeckrMessage> {
        let device = self.device(&body)?;
        let mut message = self.outgoing(body.clone()).await?;
        message.ttl_ms = Some(
            u64::try_from(timeout.as_millis())
                .map_err(|_| Error::Invalid("hardware request timeout is too large".into()))?,
        );
        let predicate: ReplyAcceptance = Arc::new(move |reply| {
            let Ok(response) = reply.hardware_body() else {
                return false;
            };
            validate_reply(&body, &response).is_ok()
                && device.validate_message(&response).is_ok()
                && accept.as_ref().is_none_or(|accept| accept(reply))
        });
        self.0
            .messages
            .request_authorized(message, timeout, Some(predicate), self)
            .await
    }

    async fn while_current<R>(&self, operation: impl Future<Output = Result<R>>) -> Result<R> {
        self.authorize()?;
        let mut authority = self.agreement().watch_state()?;
        tokio::pin!(operation);
        loop {
            tokio::select! { biased;
                _ = self.0.stop.cancelled() => return Err(Error::Closed("hardware client closed".into())),
                _ = self.0.ended.cancelled() => return Err(Error::Unauthorized("hardware claim ended".into())),
                changed = authority.next() => {
                    if changed.is_none() { return Err(Error::Closed("hardware agreement closed".into())); }
                    self.authorize()?;
                },
                result = &mut operation => { let value = result?; self.authorize()?; return Ok(value); },
            }
        }
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> MessageAuthority
    for HardwareClientContext<C, T, M>
{
    fn authorize_outgoing(&self, message: &DeckrMessage) -> Result<()> {
        self.authorize()?;
        self.0.guard.authorize_outgoing(message)?;
        if message.recipient_endpoint() != Some(self.0.manager.as_str())
            || !message.hardware_body()?.is_command()
        {
            return Err(Error::Unauthorized(
                "outgoing hardware route or operation differs from this claim".into(),
            ));
        }
        self.device(&message.hardware_body()?)?;
        Ok(())
    }
    fn authorize_incoming(&self, message: &DeckrMessage) -> Result<()> {
        self.authorize()?;
        self.0.guard.authorize_incoming(message)?;
        if message.sender != self.0.manager.as_str() || message.hardware_body()?.is_command() {
            return Err(Error::Unauthorized(
                "incoming hardware route or operation differs from this claim".into(),
            ));
        }
        self.device(&message.hardware_body()?)?;
        Ok(())
    }
}

pub struct HardwareEvent<C: StateStore, T: StateStore, M: MessageTransport> {
    pub message: DeckrMessage,
    pub context: HardwareClientContext<C, T, M>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Clone for HardwareEvent<C, T, M> {
    fn clone(&self) -> Self {
        Self {
            message: self.message.clone(),
            context: self.context.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore, M: MessageTransport> HardwareEvent<C, T, M> {
    pub fn authorize(&self) -> Result<()> {
        self.context.authorize_incoming(&self.message)
    }
}
pub struct HardwareEvents<C: StateStore, T: StateStore, M: MessageTransport> {
    receiver: Option<broadcast::Receiver<HardwareEvent<C, T, M>>>,
    stop: Cancellation,
    closed: bool,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> HardwareEvents<C, T, M> {
    pub async fn next(&mut self) -> Option<Result<HardwareEvent<C, T, M>>> {
        if self.closed {
            return None;
        }
        loop {
            let next = tokio::select! { biased;
                _ = self.stop.cancelled() => { self.closed = true; self.receiver = None; return None; },
                next = self.receiver.as_mut()?.recv() => next,
            };
            match next {
                Ok(event) if event.authorize().is_ok() => return Some(Ok(event)),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    self.closed = true;
                    self.receiver = None;
                    return None;
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    self.closed = true;
                    self.receiver = None;
                    return Some(Err(Error::StateUnavailable(format!(
                        "hardware event subscriber lost {count} deliveries"
                    ))));
                }
            }
        }
    }
}

struct Shared<C: StateStore, T: StateStore, M: MessageTransport> {
    messages: MessageSession<M>,
    clock: Arc<dyn Clock>,
    stop: Cancellation,
    state: StateCell<HardwareClientState>,
    owned: Mutex<Option<HardwareClientContext<C, T, M>>>,
    events: broadcast::Sender<HardwareEvent<C, T, M>>,
    event_registration: Mutex<()>,
    cleanup_failure: Mutex<Option<Failure>>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Shared<C, T, M> {
    fn close(&self) {
        self.stop.cancel();
        if let Some(context) = self
            .owned
            .lock()
            .expect("hardware ownership lock poisoned")
            .as_ref()
        {
            context.0.ended.cancel();
            context.agreement().request_close();
        }
        self.state.modify(|state| {
            state.phase = Phase::Closing;
            state.current = false;
            state.value.local_authority = false;
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
        let context = self
            .owned
            .lock()
            .expect("hardware ownership lock poisoned")
            .clone();
        if let Some(context) = context {
            let authority = context.authorize().is_ok();
            let agreement = context.agreement().state();
            self.state.modify(|state| {
                if matches!(state.phase, Phase::Closing | Phase::Closed) {
                    return false;
                }
                let phase = if authority {
                    Phase::Ready
                } else if agreement.phase == Phase::Starting {
                    Phase::Starting
                } else {
                    Phase::Unavailable
                };
                if state.phase == phase
                    && state.current == authority
                    && state.value.agreement.as_ref() == Some(&agreement.value)
                    && state.value.local_authority == authority
                    && agreement
                        .failure
                        .as_ref()
                        .is_none_or(|failure| state.failure.as_ref() == Some(failure))
                {
                    return false;
                }
                state.current = authority;
                state.phase = phase;
                state.value.agreement = Some(agreement.value);
                state.value.local_authority = authority;
                if agreement.failure.is_some() {
                    state.failure = agreement.failure;
                }
                true
            });
        }
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
pub struct HardwareClient<C: StateStore, T: StateStore, M: MessageTransport> {
    owner: Arc<Owner<C, T, M>>,
}
impl<C: StateStore, T: StateStore, M: MessageTransport> Clone for HardwareClient<C, T, M> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}

impl<C: StateStore, T: StateStore, M: MessageTransport> HardwareClient<C, T, M> {
    pub fn start<B: StateStore>(spec: HardwareClientSpec<B, C, T, M>) -> Result<Self> {
        let session = spec.messages.session();
        if session.endpoint().family() != CONTROLLER_FAMILY || session.is_closed() {
            return Err(Error::Invalid(
                "hardware client requires an open controller session".into(),
            ));
        }
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("hardware client requires a Tokio runtime".into()))?;
        let clock = spec.concord.runtime_clock();
        for limit in [spec.options.negotiation_timeout, spec.options.retry_delay] {
            if limit.is_zero() || clock.now().checked_add(limit).is_none() {
                return Err(Error::Invalid(
                    "hardware client time limits must be finite and positive".into(),
                ));
            }
        }
        let discovery = spec.beacon.watch_state(HARDWARE_FEATURE_ID)?;
        let shared = Arc::new(Shared {
            messages: spec.messages.clone(),
            clock,
            stop: Cancellation::default(),
            state: StateCell::new(HardwareClientState::default()),
            owned: Mutex::new(None),
            events: broadcast::channel(256).0,
            event_registration: Mutex::new(()),
            cleanup_failure: Mutex::new(None),
        });
        tokio::spawn(supervise(shared.clone(), spec, discovery));
        Ok(Self {
            owner: Arc::new(Owner { shared }),
        })
    }
    pub fn state(&self) -> Snapshot<HardwareClientState> {
        self.owner.shared.observe();
        self.owner.shared.state.snapshot()
    }
    pub fn watch_state(&self) -> Result<StateWatch<HardwareClientState>> {
        self.state();
        self.owner.shared.state.watch()
    }
    pub async fn wait_for(
        &self,
        predicate: impl FnMut(&Snapshot<HardwareClientState>) -> bool,
        timeout: Option<Duration>,
    ) -> Result<Snapshot<HardwareClientState>> {
        self.state();
        self.owner
            .shared
            .state
            .wait_for(predicate, timeout, self.owner.shared.clock.as_ref())
            .await
    }
    pub fn context(&self) -> Result<HardwareClientContext<C, T, M>> {
        let context = self
            .owner
            .shared
            .owned
            .lock()
            .expect("hardware ownership lock poisoned")
            .clone()
            .ok_or_else(|| Error::Unauthorized("hardware client has no selected claim".into()))?;
        context.authorize()?;
        Ok(context)
    }
    pub async fn wait_ready(
        &self,
        timeout: Option<Duration>,
    ) -> Result<HardwareClientContext<C, T, M>> {
        self.wait_for(|state| state.value.local_authority, timeout)
            .await?;
        self.context()
    }
    pub fn watch_events(&self) -> Result<HardwareEvents<C, T, M>> {
        let shared = &self.owner.shared;
        let _registration = shared
            .event_registration
            .lock()
            .expect("event registration lock poisoned");
        if shared.stop.is_cancelled() {
            return Err(Error::Closed("hardware client closed".into()));
        }
        if shared.events.receiver_count() >= 256 {
            return Err(Error::StateUnavailable(
                "hardware event subscriber limit reached".into(),
            ));
        }
        Ok(HardwareEvents {
            receiver: Some(shared.events.subscribe()),
            stop: shared.stop.clone(),
            closed: false,
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
            .expect("cleanup failure lock poisoned")
            .as_ref()
        {
            return Err(Error::StateUnavailable(failure.detail.clone()));
        }
        Ok(())
    }
}

async fn supervise<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    shared: Arc<Shared<C, T, M>>,
    spec: HardwareClientSpec<B, C, T, M>,
    discovery: BeaconStateWatch,
) {
    let work = async {
        let mut messages = shared.messages.subscribe(HARDWARE_MESSAGES_LANE).await?;
        tokio::select! {
            result = negotiate(shared.clone(), &spec, discovery) => result,
            result = receive(shared.clone(), &mut messages) => result,
        }
    };
    let result = std::panic::AssertUnwindSafe(async {
        tokio::select! { biased;
            _ = shared.stop.cancelled() => Ok(()),
            _ = shared.messages.session().closed() => Ok(()),
            result = work => result,
        }
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| {
        Err(Error::StateUnavailable(
            "hardware client worker panicked".into(),
        ))
    });
    if let Err(error) = &result {
        shared.failure("hardware client", error);
    }
    shared.close();
    close_owned(&shared).await;
    shared.state.modify(|state| {
        state.phase = Phase::Closed;
        state.current = false;
        state.value.local_authority = false;
        true
    });
}

async fn close_owned<C: StateStore, T: StateStore, M: MessageTransport>(shared: &Shared<C, T, M>) {
    let context = shared
        .owned
        .lock()
        .expect("hardware ownership lock poisoned")
        .clone();
    if let Some(context) = context {
        context.0.ended.cancel();
        context.agreement().request_close();
        let result = std::panic::AssertUnwindSafe(context.agreement().close())
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                Err(Error::StateUnavailable(
                    "hardware agreement cleanup panicked".into(),
                ))
            });
        if let Err(error) = result {
            let failure = Failure::from_error("hardware agreement cleanup", &error);
            *shared
                .cleanup_failure
                .lock()
                .expect("cleanup failure lock poisoned") = Some(failure);
            shared.failure("hardware agreement cleanup", &error);
        }
        shared.state.modify(|state| {
            state.value.agreement = Some(context.agreement().state().value);
            true
        });
    }
    *shared
        .owned
        .lock()
        .expect("hardware ownership lock poisoned") = None;
    shared.state.modify(|state| {
        state.value.local_authority = false;
        state.value.selected = None;
        state.current = false;
        true
    });
}

async fn receive<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: Arc<Shared<C, T, M>>,
    messages: &mut MessageSubscription,
) -> Result<()> {
    while let Some(delivery) = messages.next().await {
        let delivery = delivery?;
        let context = shared
            .owned
            .lock()
            .expect("hardware ownership lock poisoned")
            .clone();
        let Some(context) = context else {
            continue;
        };
        let validate = async {
            context.authorize_incoming(&delivery.message)?;
            context
                .validate_body(delivery.message.hardware_body()?)
                .await?;
            context.authorize_incoming(&delivery.message)
        };
        match validate.await {
            Ok(()) => {
                let _ = shared.events.send(HardwareEvent {
                    message: delivery.message,
                    context,
                });
                shared.state.modify(|state| {
                    state.value.received_events = state.value.received_events.saturating_add(1);
                    true
                });
            }
            Err(error) => shared.state.modify(|state| {
                state.value.rejected_messages = state.value.rejected_messages.saturating_add(1);
                state.failure = Some(Failure::from_error("hardware event", &error));
                true
            }),
        }
    }
    Err(Error::Closed("hardware lane subscription ended".into()))
}

async fn discovery_state<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    snapshot: BeaconSnapshot,
) -> Result<Vec<HardwareCandidate>> {
    let current = snapshot.current && !snapshot.closed;
    let candidates = tokio::task::spawn_blocking(move || {
        if !current {
            return Vec::new();
        }
        snapshot
            .candidates
            .into_iter()
            .filter_map(|candidate| {
                let payload = hardware_payload_from_advertisement(&candidate.advertisement).ok()?;
                Some(HardwareCandidate {
                    advertisement_key: candidate.key,
                    manager: payload.manager_endpoint,
                    manager_session_id: payload.session_id,
                    labels: payload.labels,
                    devices: payload.devices,
                })
            })
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|error| {
        Error::StateUnavailable(format!("hardware discovery validator failed: {error}"))
    })?;
    shared.state.modify(|state| {
        state.value.discovery_current = current;
        state.value.candidates = candidates.clone();
        true
    });
    Ok(candidates)
}

async fn negotiate<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    shared: Arc<Shared<C, T, M>>,
    spec: &HardwareClientSpec<B, C, T, M>,
    mut discovery: BeaconStateWatch,
) -> Result<()> {
    let mut candidates = Vec::new();
    'selection: loop {
        if candidates.is_empty() {
            let snapshot = discovery
                .next()
                .await
                .ok_or_else(|| Error::Closed("hardware discovery closed".into()))?;
            candidates = discovery_state(&shared, snapshot).await?;
            continue;
        }
        let selected = crate::lifecycle::deadline(
            shared.clock.as_ref(),
            Duration::from_secs(5),
            "hardware selection",
            spec.selection.select(candidates.clone()),
        );
        tokio::pin!(selected);
        let selected = loop {
            tokio::select! {
                selected = &mut selected => break selected,
                snapshot = discovery.next() => {
                    let current = discovery_state(&shared, snapshot.ok_or_else(|| Error::Closed("hardware discovery closed".into()))?).await?;
                    if current != candidates { candidates = current; continue 'selection; }
                },
            }
        };
        let selection = match selected {
            Ok(Some(selection)) => selection,
            Ok(None) => {
                candidates = discovery_state(
                    &shared,
                    discovery
                        .next()
                        .await
                        .ok_or_else(|| Error::Closed("hardware discovery closed".into()))?,
                )
                .await?;
                continue;
            }
            Err(error) => {
                shared.failure("hardware selection", &error);
                retry(
                    &shared,
                    &mut discovery,
                    &mut candidates,
                    spec.options.retry_delay,
                )
                .await?;
                continue;
            }
        };
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.advertisement_key == selection.advertisement_key)
            .cloned();
        let prepare = prepare_context(&shared, spec, candidate, &selection).await;
        let context = match prepare {
            Ok(context) => context,
            Err(error) => {
                shared.failure("hardware negotiation", &error);
                retry(
                    &shared,
                    &mut discovery,
                    &mut candidates,
                    spec.options.retry_delay,
                )
                .await?;
                continue;
            }
        };
        {
            let mut owned = shared
                .owned
                .lock()
                .expect("hardware ownership lock poisoned");
            *owned = Some(context.clone());
            if shared.stop.is_cancelled() {
                context.0.ended.cancel();
                context.agreement().request_close();
                return Ok(());
            }
        }
        let mut changes = context.agreement().watch_state()?;
        shared.state.modify(|state| {
            state.phase = Phase::Starting;
            state.value.selected = Some(selection);
            state.value.attempts = state.value.attempts.saturating_add(1);
            state.failure = None;
            true
        });
        let expires = shared
            .clock
            .now()
            .checked_add(spec.options.negotiation_timeout)
            .ok_or_else(|| {
                Error::Invalid("hardware negotiation deadline is out of range".into())
            })?;
        let mut ready = false;
        loop {
            shared.observe();
            let state = context.agreement().state();
            let authority = context.authorize().is_ok();
            if authority {
                ready = true;
            }
            if matches!(state.phase, Phase::Terminal | Phase::Closed) || (ready && !authority) {
                break;
            }
            tokio::select! {
                _ = shared.clock.sleep_until(expires), if !ready => { context.agreement().request_close_with_reason("hardware negotiation timed out"); shared.failure("hardware negotiation", &Error::Deadline("hardware agreement was not accepted".into())); break; },
                change = changes.next() => { if change.is_none() { break; } },
                snapshot = discovery.next() => {
                    if let Some(snapshot) = snapshot { candidates = discovery_state(&shared, snapshot).await?; }
                    else {
                        // Discovery lifetime ending does not revoke an active claim.
                        candidates.clear();
                        shared.state.modify(|state| { state.value.discovery_current = false; state.value.candidates.clear(); true });
                        if !ready { break; }
                        while changes.next().await.is_some() {
                            shared.observe();
                            if context.authorize().is_err() { break; }
                        }
                        break;
                    }
                },
            }
        }
        close_owned(&shared).await;
        shared.state.modify(|state| {
            state.phase = Phase::Unavailable;
            true
        });
        retry(
            &shared,
            &mut discovery,
            &mut candidates,
            spec.options.retry_delay,
        )
        .await?;
    }
}

async fn prepare_context<B: StateStore, C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    spec: &HardwareClientSpec<B, C, T, M>,
    candidate: Option<HardwareCandidate>,
    selection: &HardwareSelection,
) -> Result<HardwareClientContext<C, T, M>> {
    let candidate = candidate
        .ok_or_else(|| Error::Invalid("selection names an unknown hardware candidate".into()))?;
    if selection.devices.is_empty() {
        return Err(Error::Invalid(
            "hardware selection must contain devices".into(),
        ));
    }
    let mut claims = Vec::new();
    let mut descriptors = BTreeMap::new();
    for (id, count) in &selection.devices {
        let device = candidate
            .devices
            .get(id)
            .ok_or_else(|| Error::Invalid("selection names an unknown device".into()))?;
        if *count == 0
            || device
                .capacity
                .total_instances
                .is_some_and(|total| *count > total)
        {
            return Err(Error::Invalid(
                "hardware instance count exceeds advertised total capacity".into(),
            ));
        }
        let mut reference = device.device_ref.clone();
        reference.fingerprint = Some(device.descriptor.fingerprint.clone());
        claims.push(HardwareClaimDevice {
            device_ref: reference,
            instance_count: *count,
        });
        descriptors.insert(id.clone(), device.descriptor.clone());
    }
    let schemas = spec.schemas.clone();
    let devices = tokio::task::spawn_blocking(move || {
        descriptors
            .into_iter()
            .map(|(id, descriptor)| {
                ValidatedDevice::new(descriptor, &schemas).map(|device| (id, Arc::new(device)))
            })
            .collect::<Result<BTreeMap<_, _>>>()
    })
    .await
    .map_err(|error| {
        Error::StateUnavailable(format!("hardware descriptor compilation failed: {error}"))
    })??;
    let current = spec
        .beacon
        .candidate_by_key(&candidate.advertisement_key)?
        .ok_or_else(|| {
            Error::StateUnavailable("hardware candidate disappeared before negotiation".into())
        })?;
    let payload = hardware_payload_from_advertisement(&current.advertisement)?;
    if payload.manager_endpoint != candidate.manager
        || payload.session_id != candidate.manager_session_id
        || payload.devices != candidate.devices
    {
        return Err(Error::StateConflict(
            "hardware candidate changed before negotiation".into(),
        ));
    }
    let session = shared.messages.session().clone();
    let terms = HardwareClaimTerms {
        profile: HARDWARE_CLAIM_PROFILE_ID.into(),
        claim_id: uuid::Uuid::new_v4().to_string(),
        controller_endpoint: session.endpoint().clone(),
        manager_endpoint: candidate.manager.clone(),
        devices: claims,
    };
    let mut intent =
        AgreementIntent::new(vec![session.endpoint().clone(), candidate.manager.clone()]);
    intent.profile = Some(HARDWARE_CLAIM_PROFILE_ID.into());
    intent.terms = Some(terms.to_value()?);
    intent
        .current_sessions
        .insert(session.endpoint().to_string(), session.id().into());
    intent.current_sessions.insert(
        candidate.manager.to_string(),
        candidate.manager_session_id.clone(),
    );
    let proposal = spec.concord.propose(session, intent)?;
    let guard = AgreementMessageGuard::new(
        proposal.agreement(),
        HARDWARE_MESSAGES_LANE,
        HARDWARE_CLAIM_PROFILE_ID,
    )?;
    Ok(HardwareClientContext(Arc::new(Scope {
        guard,
        manager: candidate.manager,
        manager_session: candidate.manager_session_id,
        devices,
        messages: shared.messages.clone(),
        stop: shared.stop.clone(),
        ended: Cancellation::default(),
        clock: shared.clock.clone(),
    })))
}

async fn retry<C: StateStore, T: StateStore, M: MessageTransport>(
    shared: &Shared<C, T, M>,
    discovery: &mut BeaconStateWatch,
    candidates: &mut Vec<HardwareCandidate>,
    delay: Duration,
) -> Result<()> {
    let until = shared
        .clock
        .now()
        .checked_add(delay)
        .ok_or_else(|| Error::Invalid("hardware retry deadline is out of range".into()))?;
    loop {
        tokio::select! {
            _ = shared.clock.sleep_until(until) => return Ok(()),
            snapshot = discovery.next() => {
                *candidates = discovery_state(shared, snapshot.ok_or_else(|| Error::Closed("hardware discovery closed".into()))?).await?;
            },
        }
    }
}
