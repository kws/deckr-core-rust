//! Protocol-neutral time, cancellation, failures, and bounded current-state watches.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_core::Stream;
use futures_util::{task::AtomicWaker, StreamExt};

use crate::{Error, Result};

pub type Sleep<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Hosts can supply controlled time without changing the protocol or store.
pub trait Clock: Debug + Send + Sync + 'static {
    fn now(&self) -> Instant;
    fn utc_now(&self) -> DateTime<Utc>;
    fn sleep_until(&self, deadline: Instant) -> Sleep<'_>;
    /// A sample in [0, 1], used only for scheduling jitter.
    fn jitter(&self) -> f64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn utc_now(&self) -> DateTime<Utc> {
        Utc::now()
    }
    fn sleep_until(&self, deadline: Instant) -> Sleep<'_> {
        Box::pin(tokio::time::sleep_until(deadline.into()))
    }
    fn jitter(&self) -> f64 {
        (uuid::Uuid::new_v4().as_u128() >> 64) as f64 / u64::MAX as f64
    }
}

pub(crate) fn heartbeat_delay(clock: &dyn Clock, ttl: u64) -> Duration {
    let jitter = clock.jitter();
    let jitter = if jitter.is_finite() {
        jitter.clamp(0.0, 1.0)
    } else {
        0.5
    };
    Duration::from_secs_f64(ttl as f64 * (0.5 + 0.25 * jitter))
}

pub(crate) async fn deadline<T>(
    clock: &dyn Clock,
    limit: Duration,
    operation: &str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let deadline = clock
        .now()
        .checked_add(limit)
        .ok_or_else(|| Error::Invalid(format!("{operation} timeout is too large")))?;
    tokio::select! {
        biased;
        _ = clock.sleep_until(deadline) => Err(Error::Deadline(operation.into())),
        result = future => result,
    }
}

#[derive(Debug, Clone)]
pub struct Cancellation {
    sender: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Default for Cancellation {
    fn default() -> Self {
        Self {
            sender: Arc::new(tokio::sync::watch::channel(false).0),
        }
    }
}

impl Cancellation {
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }
    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Starting,
    Ready,
    Unavailable,
    Terminal,
    Closing,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    Invalid,
    Unavailable,
    Conflict,
    Unauthorized,
    Unsupported,
    Deadline,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub operation: String,
    pub kind: FailureKind,
    pub detail: String,
    pub mutation_code: Option<crate::MutationCode>,
}

impl Failure {
    pub fn from_error(operation: impl Into<String>, error: &Error) -> Self {
        let kind = match error {
            Error::Invalid(_) | Error::Json(_) => FailureKind::Invalid,
            Error::MaterializedViewStale(_) | Error::StateUnavailable(_) => {
                FailureKind::Unavailable
            }
            Error::Mutation {
                code:
                    crate::MutationCode::ContractInvalid
                    | crate::MutationCode::TokenInvalid
                    | crate::MutationCode::TtlInvalid,
                ..
            } => FailureKind::Invalid,
            Error::Mutation {
                code:
                    crate::MutationCode::StoreUnavailable
                    | crate::MutationCode::SourceStale
                    | crate::MutationCode::TtlMetadataMissing,
                ..
            } => FailureKind::Unavailable,
            Error::StateConflict(_) | Error::Mutation { .. } => FailureKind::Conflict,
            Error::Unauthorized(_) => FailureKind::Unauthorized,
            Error::Unsupported(_) => FailureKind::Unsupported,
            Error::Deadline(_) => FailureKind::Deadline,
            Error::Closed(_) => FailureKind::Closed,
        };
        Self {
            operation: operation.into(),
            kind,
            detail: error.to_string(),
            mutation_code: error.mutation_code(),
        }
    }
}

/// Complete immutable state. Change details coalesce for slow readers.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot<T> {
    pub version: u64,
    pub phase: Phase,
    pub current: bool,
    pub value: T,
    pub failure: Option<Failure>,
    pub changed: BTreeSet<String>,
    pub resnapshot_required: bool,
}

struct Pending<T> {
    latest: Option<Snapshot<T>>,
    interruption: Option<Snapshot<T>>,
}
struct Subscription<T> {
    pending: Mutex<Pending<T>>,
    waker: AtomicWaker,
}

pub struct StateWatch<T> {
    subscription: Arc<Subscription<T>>,
    initial: Option<Snapshot<T>>,
    done: bool,
}

impl<T: Unpin> Stream for StateWatch<T> {
    type Item = Snapshot<T>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if let Some(snapshot) = self.initial.take() {
            self.done = snapshot.phase == Phase::Closed;
            return Poll::Ready(Some(snapshot));
        }
        self.subscription.waker.register(context.waker());
        let snapshot = {
            let mut pending = self
                .subscription
                .pending
                .lock()
                .expect("lifecycle watch lock poisoned");
            if let Some(interruption) = pending.interruption.take() {
                if pending
                    .latest
                    .as_ref()
                    .is_some_and(|latest| latest.version <= interruption.version)
                {
                    pending.latest = None;
                }
                Some(interruption)
            } else {
                pending.latest.take()
            }
        };
        if let Some(snapshot) = snapshot {
            self.done = snapshot.phase == Phase::Closed;
            Poll::Ready(Some(snapshot))
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone)]
pub(crate) struct StateCell<T: Clone> {
    inner: Arc<Mutex<StateInner<T>>>,
}
struct StateInner<T: Clone> {
    snapshot: Snapshot<T>,
    subscribers: Vec<Weak<Subscription<T>>>,
    changed: Arc<tokio::sync::Notify>,
}

impl<T: Clone> StateInner<T> {
    fn notify(&mut self) {
        self.changed.notify_waiters();
        self.subscribers
            .retain(|subscriber| subscriber.strong_count() > 0);
        for subscriber in self.subscribers.iter().filter_map(Weak::upgrade) {
            let mut pending = subscriber
                .pending
                .lock()
                .expect("lifecycle watch lock poisoned");
            let mut snapshot = self.snapshot.clone();
            if let Some(previous) = pending.latest.as_ref() {
                snapshot.changed.extend(previous.changed.iter().cloned());
                snapshot.resnapshot_required |= previous.resnapshot_required;
            }
            if snapshot.changed.len() > 256 || snapshot.resnapshot_required {
                snapshot.changed.clear();
                snapshot.resnapshot_required = true;
            }
            if !snapshot.current {
                pending.interruption = Some(snapshot.clone());
            }
            pending.latest = Some(snapshot);
            subscriber.waker.wake();
        }
    }
}

impl<T: Clone> Drop for StateInner<T> {
    fn drop(&mut self) {
        if self.snapshot.phase != Phase::Closed {
            self.snapshot.version += 1;
            self.snapshot.phase = Phase::Closed;
            self.snapshot.current = false;
            self.notify();
        }
    }
}

impl<T: Clone + Unpin> StateCell<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StateInner {
                snapshot: Snapshot {
                    version: 0,
                    phase: Phase::Starting,
                    current: false,
                    value,
                    failure: None,
                    changed: BTreeSet::new(),
                    resnapshot_required: false,
                },
                subscribers: Vec::new(),
                changed: Arc::default(),
            })),
        }
    }
    pub(crate) fn snapshot(&self) -> Snapshot<T> {
        self.inner
            .lock()
            .expect("lifecycle state lock poisoned")
            .snapshot
            .clone()
    }
    /// Atomically change a snapshot without overwriting a concurrent terminal
    /// transition. Returning false keeps both state and version unchanged.
    pub(crate) fn modify(&self, update: impl FnOnce(&mut Snapshot<T>) -> bool) {
        let mut inner = self.inner.lock().expect("lifecycle state lock poisoned");
        if inner.snapshot.phase == Phase::Closed || !update(&mut inner.snapshot) {
            return;
        }
        inner.snapshot.version += 1;
        inner.snapshot.resnapshot_required = inner.snapshot.changed.len() > 256;
        if inner.snapshot.resnapshot_required {
            inner.snapshot.changed.clear();
        }
        inner.notify();
    }
    pub(crate) fn publish(
        &self,
        phase: Phase,
        current: bool,
        value: T,
        failure: Option<Failure>,
        mut changed: BTreeSet<String>,
    ) {
        let mut inner = self.inner.lock().expect("lifecycle state lock poisoned");
        if inner.snapshot.phase == Phase::Closed {
            return;
        }
        let version = inner.snapshot.version + 1;
        let resnapshot_required = changed.len() > 256;
        if resnapshot_required {
            changed.clear();
        }
        inner.snapshot = Snapshot {
            version,
            phase,
            current,
            value,
            failure,
            changed,
            resnapshot_required,
        };
        inner.notify();
    }
    pub(crate) fn watch(&self) -> Result<StateWatch<T>> {
        let mut inner = self.inner.lock().expect("lifecycle state lock poisoned");
        inner
            .subscribers
            .retain(|subscriber| subscriber.strong_count() > 0);
        if inner.subscribers.len() >= 256 {
            return Err(Error::StateUnavailable(
                "lifecycle watch registration limit (256) reached".into(),
            ));
        }
        let subscription = Arc::new(Subscription {
            pending: Mutex::new(Pending {
                latest: None,
                interruption: None,
            }),
            waker: AtomicWaker::new(),
        });
        inner.subscribers.push(Arc::downgrade(&subscription));
        let mut initial = inner.snapshot.clone();
        initial.changed.clear();
        initial.resnapshot_required = false;
        Ok(StateWatch {
            subscription,
            initial: Some(initial),
            done: false,
        })
    }
    /// Lifecycle cleanup must not need a slot in the public watch broadcaster.
    pub(crate) async fn wait_closed(&self) -> Snapshot<T> {
        let changed = self
            .inner
            .lock()
            .expect("lifecycle state lock poisoned")
            .changed
            .clone();
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let snapshot = self.snapshot();
            if snapshot.phase == Phase::Closed {
                return snapshot;
            }
            notified.await;
        }
    }
    pub(crate) async fn wait_for(
        &self,
        mut predicate: impl FnMut(&Snapshot<T>) -> bool,
        timeout: Option<Duration>,
        clock: &dyn Clock,
    ) -> Result<Snapshot<T>> {
        let mut watch = self.watch()?;
        // The initial snapshot was captured atomically with registration. Check
        // it before installing a timeout, including a zero-duration timeout.
        let initial = watch
            .next()
            .await
            .expect("watch supplies its initial snapshot");
        if predicate(&initial) {
            return Ok(initial);
        }
        if initial.phase == Phase::Closed {
            return Err(Error::Closed(
                "lifecycle closed before predicate matched".into(),
            ));
        }
        let wait = async {
            while let Some(snapshot) = watch.next().await {
                if predicate(&snapshot) {
                    return Ok(snapshot);
                }
                if snapshot.phase == Phase::Closed {
                    break;
                }
            }
            Err(Error::Closed(
                "lifecycle closed before predicate matched".into(),
            ))
        };
        match timeout {
            Some(limit) => deadline(clock, limit, "lifecycle predicate wait", wait).await,
            None => wait.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn state_watches_are_atomic_bounded_and_keep_interruptions() {
        let state = StateCell::new(Vec::<String>::new());
        state.publish(Phase::Ready, true, vec![], None, BTreeSet::new());
        let mut watch = state.watch().unwrap();
        state.publish(
            Phase::Ready,
            true,
            vec!["first".into()],
            None,
            BTreeSet::from(["first".into()]),
        );
        let initial = watch.next().await.unwrap();
        assert!(initial.value.is_empty());
        assert!(initial.changed.is_empty());
        assert_eq!(watch.next().await.unwrap().value, vec!["first"]);
        for index in 0..257 {
            state.publish(
                Phase::Ready,
                true,
                vec![index.to_string()],
                None,
                BTreeSet::from([index.to_string()]),
            );
        }
        let coalesced = watch.next().await.unwrap();
        assert!(coalesced.resnapshot_required);
        assert!(coalesced.changed.is_empty());
        assert_eq!(coalesced.value, vec!["256"]);
        state.publish(Phase::Unavailable, false, vec![], None, BTreeSet::new());
        state.publish(
            Phase::Ready,
            true,
            vec!["restored".into()],
            None,
            BTreeSet::new(),
        );
        assert!(!watch.next().await.unwrap().current);
        assert!(watch.next().await.unwrap().current);
        let readers: Vec<_> = (0..255).map(|_| state.watch().unwrap()).collect();
        assert!(state.watch().is_err());
        drop(readers);
        assert!(state.watch().is_ok());
        drop(state);
        assert_eq!(watch.next().await.unwrap().phase, Phase::Closed);
        assert!(watch.next().await.is_none());
    }

    #[tokio::test]
    async fn predicate_waits_distinguish_satisfaction_timeout_and_closure() {
        let state = StateCell::new(0);
        assert_eq!(
            state
                .wait_for(
                    |snapshot| snapshot.value == 0,
                    Some(Duration::ZERO),
                    &SystemClock
                )
                .await
                .unwrap()
                .value,
            0
        );
        assert!(matches!(
            state
                .wait_for(
                    |snapshot| snapshot.value == 1,
                    Some(Duration::ZERO),
                    &SystemClock
                )
                .await,
            Err(Error::Deadline(_))
        ));
        let waiting = state.clone();
        let task = tokio::spawn(async move {
            waiting
                .wait_for(|snapshot| snapshot.value == 2, None, &SystemClock)
                .await
        });
        state.publish(Phase::Closed, false, 1, None, BTreeSet::new());
        assert!(matches!(task.await.unwrap(), Err(Error::Closed(_))));
        assert_eq!(
            state
                .wait_for(
                    |snapshot| snapshot.phase == Phase::Closed,
                    None,
                    &SystemClock
                )
                .await
                .unwrap()
                .phase,
            Phase::Closed
        );
    }
}
