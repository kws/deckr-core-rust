//! Managed Deckr delivery and correlation over host-supplied transports.
//! Lane validation does not grant Concord or application-profile authority.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::{mpsc, Notify};

use crate::endpoint::EndpointAddress;
use crate::lanes::{DeckrMessage, MessageTarget, HARDWARE_MESSAGES_LANE, SERVICES_LANE};
use crate::lifecycle::{
    Cancellation, Clock, Failure, Phase, Snapshot, StateCell, StateWatch, SystemClock,
};
use crate::session::EndpointSession;
use crate::{Error, Result};

#[derive(Debug, Clone)]
pub struct Delivery {
    pub message: DeckrMessage,
    /// Transport-owned return address, never a substitute for envelope identity.
    pub reply_address: Option<String>,
}
pub type DeliveryStream = Pin<Box<dyn Stream<Item = Result<Delivery>> + Send + 'static>>;
pub struct ReplyInbox {
    pub address: String,
    /// Dropping this stream must release the inbox subscription.
    pub messages: DeliveryStream,
}

/// Adapters validate their routing hints and headers before yielding deliveries.
/// Dropping a subscription stream must release its transport resources.
pub trait MessageTransport: Clone + Send + Sync + 'static {
    fn subscribe(
        &self,
        lane: &str,
        endpoint: &EndpointAddress,
    ) -> impl Future<Output = Result<DeliveryStream>> + Send;
    fn open_inbox(&self) -> impl Future<Output = Result<ReplyInbox>> + Send;
    /// `inbox` selects an explicit return route; None selects the envelope lane.
    fn publish(
        &self,
        message: &DeckrMessage,
        inbox: Option<&str>,
        reply_address: Option<&str>,
    ) -> impl Future<Output = Result<()>> + Send;
}

pub trait LaneValidator: Send + Sync + 'static {
    /// Validation runs at the delivery boundary and must be bounded and free of I/O.
    fn validate(&self, message: &DeckrMessage) -> Result<()>;
}
impl<F: Fn(&DeckrMessage) -> Result<()> + Send + Sync + 'static> LaneValidator for F {
    fn validate(&self, message: &DeckrMessage) -> Result<()> {
        self(message)
    }
}

/// Only explicitly registered extension lanes may cross this boundary. Core
/// validators cannot be replaced by application registration.
#[derive(Clone, Default)]
pub struct LaneRegistry {
    extensions: BTreeMap<String, Arc<dyn LaneValidator>>,
}
impl LaneRegistry {
    pub fn with_lane(
        mut self,
        lane: impl Into<String>,
        validator: impl LaneValidator,
    ) -> Result<Self> {
        let lane = lane.into();
        if lane.is_empty()
            || lane.trim() != lane
            || matches!(lane.as_str(), HARDWARE_MESSAGES_LANE | SERVICES_LANE)
            || self.extensions.contains_key(&lane)
        {
            return Err(Error::Invalid(
                "lane must be a unique non-core lane name".into(),
            ));
        }
        self.extensions.insert(lane, Arc::new(validator));
        Ok(self)
    }
    fn contains(&self, lane: &str) -> bool {
        matches!(lane, HARDWARE_MESSAGES_LANE | SERVICES_LANE) || self.extensions.contains_key(lane)
    }
    pub fn validate(&self, message: &DeckrMessage) -> Result<()> {
        message.validate()?;
        match message.lane.as_str() {
            HARDWARE_MESSAGES_LANE | SERVICES_LANE => Ok(()),
            lane => self
                .extensions
                .get(lane)
                .ok_or_else(|| Error::Unsupported("lane is not registered".into()))?
                .validate(message),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessagingState {
    pub subscriptions: usize,
    pub pending_requests: usize,
    pub pending_sends: usize,
    pub rejected_messages: u64,
}
struct Shared {
    session: EndpointSession,
    clock: Arc<dyn Clock>,
    lanes: LaneRegistry,
    stop: Cancellation,
    state: StateCell<MessagingState>,
    active: AtomicUsize,
    changed: Notify,
}
impl Shared {
    fn check(&self) -> Result<()> {
        if self.stop.is_cancelled() || self.session.is_closed() {
            Err(Error::Closed("messaging endpoint closed".into()))
        } else {
            Ok(())
        }
    }
    fn reject(&self, operation: &str, error: &Error) {
        self.state.modify(|state| {
            state.value.rejected_messages = state.value.rejected_messages.saturating_add(1);
            state.failure = Some(Failure::from_error(operation, error));
            true
        });
    }
    fn incoming(&self, message: &DeckrMessage) -> Result<bool> {
        self.lanes.validate(message)?;
        Ok(!message.is_expired_at(self.clock.utc_now())
            && message
                .recipient
                .targets_endpoint(self.session.endpoint())?
            && message
                .recipient_session_id
                .as_deref()
                .is_none_or(|session| session == self.session.id()))
    }
    fn prepare(&self, mut message: DeckrMessage) -> Result<DeckrMessage> {
        message.sender = self.session.endpoint().to_string();
        message.sender_session_id = self.session.id().to_string();
        self.validate_outgoing(&message)?;
        Ok(message)
    }
    fn validate_outgoing(&self, message: &DeckrMessage) -> Result<()> {
        self.check()?;
        self.lanes.validate(message)?;
        if message.is_expired_at(self.clock.utc_now()) {
            return Err(Error::Deadline("message already expired".into()));
        }
        Ok(())
    }
    async fn cancellable<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::select! { biased;
            _ = self.stop.cancelled() => Err(Error::Closed("messaging endpoint closed".into())),
            _ = self.session.closed() => Err(Error::Closed("endpoint session closed".into())),
            result = future => result,
        }
    }
}
struct Owner<T: MessageTransport> {
    shared: Arc<Shared>,
    transport: T,
}
impl<T: MessageTransport> Drop for Owner<T> {
    fn drop(&mut self) {
        self.shared.stop.cancel();
    }
}

pub struct MessageSession<T: MessageTransport> {
    owner: Arc<Owner<T>>,
}
impl<T: MessageTransport> Clone for MessageSession<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Subscription,
    Request,
    Send,
}
struct OperationGuard {
    shared: Arc<Shared>,
    operation: Operation,
}
impl OperationGuard {
    fn new(shared: &Arc<Shared>, operation: Operation) -> Result<Self> {
        shared.check()?;
        shared.active.fetch_add(1, Ordering::SeqCst);
        shared.state.modify(|state| {
            *counter(&mut state.value, operation) += 1;
            true
        });
        let guard = Self {
            shared: shared.clone(),
            operation,
        };
        shared.check()?;
        Ok(guard)
    }
}
fn counter(state: &mut MessagingState, operation: Operation) -> &mut usize {
    match operation {
        Operation::Subscription => &mut state.subscriptions,
        Operation::Request => &mut state.pending_requests,
        Operation::Send => &mut state.pending_sends,
    }
}
impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.shared.state.modify(|state| {
            let counter = counter(&mut state.value, self.operation);
            *counter = counter.saturating_sub(1);
            true
        });
        self.shared.active.fetch_sub(1, Ordering::SeqCst);
        self.shared.changed.notify_waiters();
    }
}

pub type ReplyAcceptance = Arc<dyn Fn(&DeckrMessage) -> bool + Send + Sync>;

/// Profile boundaries implement these checks for their exact managed context.
/// Checks must be synchronous and bounded; application work belongs outside
/// them. The transport calls them again after awaited preparation and acceptance.
pub trait MessageAuthority: Send + Sync {
    fn authorize_outgoing(&self, message: &DeckrMessage) -> Result<()>;
    fn authorize_incoming(&self, message: &DeckrMessage) -> Result<()>;
}

impl<T: MessageTransport> MessageSession<T> {
    pub fn session(&self) -> &EndpointSession {
        &self.owner.shared.session
    }

    pub fn new(transport: T, session: EndpointSession) -> Result<Self> {
        Self::with_options(
            transport,
            session,
            LaneRegistry::default(),
            Arc::new(SystemClock),
        )
    }
    pub fn with_options(
        transport: T,
        session: EndpointSession,
        lanes: LaneRegistry,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        EndpointAddress::parse(session.endpoint().as_str())?;
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("messaging requires a Tokio runtime".into()))?;
        if session.is_closed() {
            return Err(Error::Closed("endpoint session closed".into()));
        }
        let state = StateCell::new(MessagingState::default());
        state.publish(
            Phase::Ready,
            true,
            MessagingState::default(),
            None,
            BTreeSet::new(),
        );
        let shared = Arc::new(Shared {
            session,
            clock,
            lanes,
            stop: Cancellation::default(),
            state,
            active: AtomicUsize::new(0),
            changed: Notify::new(),
        });
        tokio::spawn(supervise(shared.clone()));
        Ok(Self {
            owner: Arc::new(Owner { shared, transport }),
        })
    }
    pub fn state(&self) -> Snapshot<MessagingState> {
        if self.owner.shared.check().is_err() {
            self.request_close();
        }
        self.owner.shared.state.snapshot()
    }
    pub fn watch_state(&self) -> Result<StateWatch<MessagingState>> {
        self.state();
        self.owner.shared.state.watch()
    }
    pub async fn wait_for(
        &self,
        predicate: impl FnMut(&Snapshot<MessagingState>) -> bool,
        timeout: Option<Duration>,
    ) -> Result<Snapshot<MessagingState>> {
        self.state();
        self.owner
            .shared
            .state
            .wait_for(predicate, timeout, self.owner.shared.clock.as_ref())
            .await
    }
    pub fn request_close(&self) {
        let shared = &self.owner.shared;
        shared.stop.cancel();
        shared.state.modify(|state| {
            if state.phase == Phase::Closing {
                return false;
            }
            state.phase = Phase::Closing;
            state.current = false;
            true
        });
    }
    pub async fn close(&self) -> Result<()> {
        self.request_close();
        let closed = self.owner.shared.state.wait_closed().await;
        if closed.value.pending_requests + closed.value.pending_sends + closed.value.subscriptions
            != 0
        {
            return Err(Error::Deadline(
                "messaging workers did not stop before cleanup deadline".into(),
            ));
        }
        Ok(())
    }

    pub async fn send(&self, message: DeckrMessage) -> Result<()> {
        self.send_checked(message, None).await
    }

    pub async fn send_authorized(
        &self,
        message: DeckrMessage,
        authority: &dyn MessageAuthority,
    ) -> Result<()> {
        self.send_checked(message, Some(authority)).await
    }

    async fn send_checked(
        &self,
        message: DeckrMessage,
        authority: Option<&dyn MessageAuthority>,
    ) -> Result<()> {
        let shared = &self.owner.shared;
        let _operation = OperationGuard::new(shared, Operation::Send)?;
        let message = shared.prepare(message)?;
        if let Some(authority) = authority {
            authority.authorize_outgoing(&message)?;
        }
        shared
            .cancellable(crate::lifecycle::deadline(
                shared.clock.as_ref(),
                Duration::from_secs(5),
                "message publication",
                self.owner.transport.publish(&message, None, None),
            ))
            .await
    }

    pub async fn reply(&self, request: &Delivery, mut reply: DeckrMessage) -> Result<()> {
        self.reply_checked(request, &mut reply, None).await
    }

    pub async fn reply_authorized(
        &self,
        request: &Delivery,
        mut reply: DeckrMessage,
        authority: &dyn MessageAuthority,
    ) -> Result<()> {
        self.reply_checked(request, &mut reply, Some(authority))
            .await
    }

    async fn reply_checked(
        &self,
        request: &Delivery,
        reply: &mut DeckrMessage,
        authority: Option<&dyn MessageAuthority>,
    ) -> Result<()> {
        let shared = &self.owner.shared;
        if !shared.incoming(&request.message)? {
            return Err(Error::Unauthorized(
                "request is not deliverable to this endpoint".into(),
            ));
        }
        if let Some(authority) = authority {
            authority.authorize_incoming(&request.message)?;
        }
        if reply.message_id == request.message.message_id {
            return Err(Error::Invalid(
                "reply must have a different message ID".into(),
            ));
        }
        reply.recipient = MessageTarget::Endpoint {
            endpoint: request.message.sender.clone(),
        };
        reply.recipient_session_id = Some(request.message.sender_session_id.clone());
        reply.contract = request.message.contract.clone();
        reply.in_reply_to = Some(request.message.message_id.clone());
        let _operation = OperationGuard::new(shared, Operation::Send)?;
        let reply = shared.prepare(reply.clone())?;
        if let Some(authority) = authority {
            authority.authorize_outgoing(&reply)?;
        }
        shared
            .cancellable(crate::lifecycle::deadline(
                shared.clock.as_ref(),
                Duration::from_secs(5),
                "reply publication",
                self.owner
                    .transport
                    .publish(&reply, request.reply_address.as_deref(), None),
            ))
            .await
    }

    pub async fn request(
        &self,
        request: DeckrMessage,
        timeout: Duration,
        accept: Option<ReplyAcceptance>,
    ) -> Result<DeckrMessage> {
        self.request_checked(request, timeout, accept, None).await
    }

    pub async fn request_authorized(
        &self,
        request: DeckrMessage,
        timeout: Duration,
        accept: Option<ReplyAcceptance>,
        authority: &dyn MessageAuthority,
    ) -> Result<DeckrMessage> {
        self.request_checked(request, timeout, accept, Some(authority))
            .await
    }

    async fn request_checked(
        &self,
        request: DeckrMessage,
        timeout: Duration,
        accept: Option<ReplyAcceptance>,
        authority: Option<&dyn MessageAuthority>,
    ) -> Result<DeckrMessage> {
        let shared = &self.owner.shared;
        let _operation = OperationGuard::new(shared, Operation::Request)?;
        let request = shared.prepare(request)?;
        if let Some(authority) = authority {
            authority.authorize_outgoing(&request)?;
        }
        let operation = async {
            let mut inbox = self.owner.transport.open_inbox().await?;
            // Inbox lifetime is already owned when publication begins.
            shared.validate_outgoing(&request)?;
            if let Some(authority) = authority {
                authority.authorize_outgoing(&request)?;
            }
            self.owner
                .transport
                .publish(&request, None, Some(&inbox.address))
                .await?;
            while let Some(delivery) = inbox.messages.next().await {
                let delivery = match delivery {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        shared.reject("reply decoding", &error);
                        continue;
                    }
                };
                let reply = delivery.message;
                let valid = shared.incoming(&reply).unwrap_or_else(|error| {
                    shared.reject("reply validation", &error);
                    false
                });
                if !valid || !reply_matches(&request, &reply, &shared.session) {
                    continue;
                }
                if let Some(authority) = authority {
                    // A stale lifetime stops this request; an invalid reply only
                    // rejects that candidate and leaves the inbox waiting.
                    authority.authorize_outgoing(&request)?;
                    if let Err(error) = authority.authorize_incoming(&reply) {
                        shared.reject("reply authority", &error);
                        continue;
                    }
                }
                if let Some(accept) = &accept {
                    let predicate = accept.clone();
                    let candidate = reply.clone();
                    if !tokio::task::spawn_blocking(move || predicate(&candidate))
                        .await
                        .map_err(|error| {
                            Error::StateUnavailable(format!("reply predicate failed: {error}"))
                        })?
                    {
                        continue;
                    }
                }
                // The predicate can take time; never return an expired reply.
                if shared.incoming(&reply)? {
                    if let Some(authority) = authority {
                        authority.authorize_incoming(&reply)?;
                    }
                    return Ok(reply);
                }
            }
            Err(Error::Closed("reply inbox ended".into()))
        };
        shared
            .cancellable(crate::lifecycle::deadline(
                shared.clock.as_ref(),
                timeout,
                "request timed out",
                operation,
            ))
            .await
    }

    pub async fn subscribe(&self, lane: &str) -> Result<MessageSubscription> {
        let shared = &self.owner.shared;
        if !shared.lanes.contains(lane) {
            return Err(Error::Unsupported("lane is not registered".into()));
        }
        let operation = OperationGuard::new(shared, Operation::Subscription)?;
        let source = shared
            .cancellable(crate::lifecycle::deadline(
                shared.clock.as_ref(),
                Duration::from_secs(5),
                "lane subscription",
                self.owner
                    .transport
                    .subscribe(lane, shared.session.endpoint()),
            ))
            .await?;
        let (sender, receiver) = mpsc::channel(256);
        let stop = Cancellation::default();
        let failure = Arc::new(Mutex::new(None));
        let task_shared = shared.clone();
        let task_stop = stop.clone();
        let task_failure = failure.clone();
        let lane = lane.to_string();
        let worker = tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(receive(
                task_shared.clone(),
                task_stop.clone(),
                source,
                sender,
                lane,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                Err(Error::StateUnavailable(
                    "message subscription worker panicked".into(),
                ))
            });
            if let Err(error) = result {
                task_shared.reject("subscription", &error);
                *task_failure
                    .lock()
                    .expect("subscription failure lock poisoned") = Some(error.to_string());
            }
            task_stop.cancel();
            drop(operation);
        });
        Ok(MessageSubscription {
            shared: shared.clone(),
            stop,
            failure,
            receiver,
            worker: Some(worker),
        })
    }
}

fn reply_matches(request: &DeckrMessage, reply: &DeckrMessage, local: &EndpointSession) -> bool {
    reply.message_id != request.message_id
        && reply.in_reply_to.as_deref() == Some(&request.message_id)
        && reply.contract == request.contract
        && reply.recipient_endpoint() == Some(local.endpoint().as_str())
        && request
            .recipient_endpoint()
            .is_none_or(|peer| reply.sender == peer)
        && request
            .recipient_session_id
            .as_deref()
            .is_none_or(|session| reply.sender_session_id == session)
}

pub struct MessageSubscription {
    shared: Arc<Shared>,
    stop: Cancellation,
    failure: Arc<Mutex<Option<String>>>,
    receiver: mpsc::Receiver<Delivery>,
    worker: Option<tokio::task::JoinHandle<()>>,
}
impl MessageSubscription {
    pub async fn next(&mut self) -> Option<Result<Delivery>> {
        loop {
            if self.stop.is_cancelled() || self.shared.check().is_err() {
                self.receiver.close();
                return self
                    .failure
                    .lock()
                    .expect("subscription failure lock poisoned")
                    .take()
                    .map(|failure| Err(Error::StateUnavailable(failure)));
            }
            let delivery = tokio::select! { biased;
                _ = self.stop.cancelled() => continue,
                _ = self.shared.stop.cancelled() => continue,
                _ = self.shared.session.closed() => continue,
                delivery = self.receiver.recv() => delivery?,
            };
            if self.shared.incoming(&delivery.message).unwrap_or(false) {
                return Some(Ok(delivery));
            }
        }
    }
    pub async fn close(&mut self) -> Result<()> {
        self.stop.cancel();
        self.receiver.close();
        if let Some(worker) = self.worker.take() {
            worker.await.map_err(|error| {
                Error::StateUnavailable(format!("message subscription failed: {error}"))
            })?;
        }
        Ok(())
    }
}
impl Drop for MessageSubscription {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}
async fn receive(
    shared: Arc<Shared>,
    stop: Cancellation,
    mut source: DeliveryStream,
    sender: mpsc::Sender<Delivery>,
    lane: String,
) -> Result<()> {
    loop {
        let next = tokio::select! { biased;
            _ = stop.cancelled() => return Ok(()),
            _ = shared.stop.cancelled() => return Ok(()),
            _ = shared.session.closed() => return Ok(()),
            next = source.next() => next.ok_or_else(|| Error::Closed("transport subscription ended".into()))?,
        };
        let delivery = match next {
            Ok(delivery) => delivery,
            Err(error) => {
                shared.reject("message decoding", &error);
                continue;
            }
        };
        match shared.incoming(&delivery.message) {
            Ok(true) if delivery.message.lane == lane => {}
            Ok(_) => continue,
            Err(error) => {
                shared.reject("message validation", &error);
                continue;
            }
        }
        match sender.try_send(delivery) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(Error::StateUnavailable(
                    "message subscriber overflowed (256 pending deliveries)".into(),
                ))
            }
        }
    }
}
async fn supervise(shared: Arc<Shared>) {
    tokio::select! { _ = shared.stop.cancelled() => {}, _ = shared.session.closed() => {} }
    shared.stop.cancel();
    shared.state.modify(|state| {
        state.phase = Phase::Closing;
        state.current = false;
        true
    });
    let wait = async {
        loop {
            let changed = shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if shared.active.load(Ordering::SeqCst) == 0 {
                return Ok(());
            }
            changed.await;
        }
    };
    let failure = crate::lifecycle::deadline(
        shared.clock.as_ref(),
        Duration::from_secs(5),
        "messaging cleanup",
        wait,
    )
    .await
    .err();
    shared.state.modify(|state| {
        state.phase = Phase::Closed;
        state.current = false;
        if let Some(error) = failure {
            state.failure = Some(Failure::from_error("messaging cleanup", &error));
        }
        true
    });
}
