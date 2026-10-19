use futures_util::FutureExt;
use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::lifecycle::{
    deadline, Cancellation, Clock, Failure, Phase, Snapshot, StateCell, StateWatch,
};
use crate::state::StateStore;
use crate::{Error, Result};

use super::{
    advertisement_handle, make_beacon_advertisement_key, AdvertisementHandle, BeaconAdvertiser,
    BeaconProtocol,
};

/// Mutable intent; endpoint/session and advertisement identity stay fixed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AdvertisementContent {
    pub payload: Option<Value>,
    pub protocol: Option<BeaconProtocol>,
    pub operations: Vec<String>,
    pub labels: BTreeMap<String, String>,
    pub hints: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AdvertisementState {
    pub handle: Option<AdvertisementHandle>,
}

struct Update {
    content: AdvertisementContent,
    reply: oneshot::Sender<Result<AdvertisementHandle>>,
}

struct Owner {
    commands: mpsc::Sender<Update>,
    cancellation: Cancellation,
    state: StateCell<AdvertisementState>,
    clock: Arc<dyn Clock>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        // The worker owns cleanup and does not retain this owner.
        self.cancellation.cancel();
    }
}

/// A managed advertisement. Last-handle drop initiates bounded cleanup; close
/// waits for it. Discovery failures never affect any agreement lifetime.
///
/// ```no_run
/// # async fn example() -> deckr::Result<()> {
/// use deckr::beacon::BeaconAdvertiser;
/// use deckr::endpoint::EndpointAddress;
/// use deckr::state::MemoryStateStore;
/// use std::time::Duration;
///
/// let publication = BeaconAdvertiser::new(
///     MemoryStateStore::ttl_bound(300)?,
///     "org.example.worker",
///     EndpointAddress::parse_protocol("worker:main")?,
///     "session-1",
/// ).manage()?;
/// let handle = publication.wait_ready(Some(Duration::from_secs(5))).await?;
/// publication.close().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ManagedAdvertisement {
    owner: Arc<Owner>,
}

impl ManagedAdvertisement {
    pub(super) fn start<S: StateStore>(mut advertiser: BeaconAdvertiser<S>) -> Result<Self> {
        advertiser.record(1, 1, None).validate()?;
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("managed advertisements require a Tokio runtime".into()))?;
        // Managed publication always derives the schedule from observed TTL.
        advertiser.requested_refresh_interval = None;
        let (commands, receiver) = mpsc::channel(32);
        let state = StateCell::new(AdvertisementState::default());
        let cancellation = Cancellation::default();
        let clock = advertiser.clock.clone();
        let worker = tokio::spawn(run(
            advertiser,
            receiver,
            state.clone(),
            cancellation.clone(),
        ));
        Ok(Self {
            owner: Arc::new(Owner {
                commands,
                cancellation,
                state,
                clock,
                worker: Mutex::new(Some(worker)),
            }),
        })
    }

    pub fn state(&self) -> Snapshot<AdvertisementState> {
        self.owner.state.snapshot()
    }
    pub fn watch_state(&self) -> Result<StateWatch<AdvertisementState>> {
        self.owner.state.watch()
    }

    pub async fn wait_for(
        &self,
        predicate: impl FnMut(&Snapshot<AdvertisementState>) -> bool,
        timeout: Option<Duration>,
    ) -> Result<Snapshot<AdvertisementState>> {
        self.owner
            .state
            .wait_for(predicate, timeout, self.owner.clock.as_ref())
            .await
    }

    pub async fn wait_ready(&self, timeout: Option<Duration>) -> Result<AdvertisementHandle> {
        let state = self
            .wait_for(
                |state| state.current && state.phase == Phase::Ready,
                timeout,
            )
            .await?;
        state
            .value
            .handle
            .ok_or_else(|| Error::StateUnavailable("advertisement has no committed handle".into()))
    }

    /// Publish a real content change immediately. Invalid updates leave the
    /// current advertisement and its renewal worker intact.
    pub async fn update(&self, content: AdvertisementContent) -> Result<AdvertisementHandle> {
        if self.owner.cancellation.is_cancelled() {
            return Err(Error::Closed("advertisement closed".into()));
        }
        let (reply, result) = oneshot::channel();
        self.owner
            .commands
            .send(Update { content, reply })
            .await
            .map_err(|_| Error::Closed("advertisement closed".into()))?;
        result
            .await
            .map_err(|_| Error::Closed("advertisement closed during update".into()))?
    }

    /// Stop publication synchronously; the owned worker performs cleanup.
    pub fn request_close(&self) {
        self.owner.cancellation.cancel();
    }

    pub async fn close(&self) -> Result<()> {
        self.request_close();
        let worker = self
            .owner
            .worker
            .lock()
            .expect("advertisement worker lock poisoned")
            .take();
        if let Some(worker) = worker {
            worker.await.map_err(|error| {
                Error::StateUnavailable(format!("advertisement worker failed: {error}"))
            })?;
        }
        let closed = self.owner.state.wait_closed().await;
        if let Some(failure) = closed.failure {
            return Err(Error::StateUnavailable(failure.detail));
        }
        Ok(())
    }
}

struct CompletionGuard(StateCell<AdvertisementState>);
impl Drop for CompletionGuard {
    fn drop(&mut self) {
        let snapshot = self.0.snapshot();
        if snapshot.phase != Phase::Closed {
            self.0.publish(
                Phase::Closed,
                false,
                snapshot.value,
                Some(Failure::from_error(
                    "advertisement worker",
                    &Error::StateUnavailable("worker exited unexpectedly".into()),
                )),
                BTreeSet::new(),
            );
        }
    }
}

async fn run<S: StateStore>(
    advertiser: BeaconAdvertiser<S>,
    commands: mpsc::Receiver<Update>,
    state: StateCell<AdvertisementState>,
    cancellation: Cancellation,
) {
    let _completion = CompletionGuard(state.clone());
    let result = AssertUnwindSafe(run_lifetime(
        advertiser.clone(),
        commands,
        state.clone(),
        cancellation,
    ))
    .catch_unwind()
    .await;
    if result.is_err() {
        let failure = Failure::from_error(
            "advertisement worker",
            &Error::StateUnavailable("advertisement adapter panicked".into()),
        );
        state.publish(
            Phase::Closing,
            false,
            state.snapshot().value,
            Some(failure.clone()),
            BTreeSet::new(),
        );
        let cleanup = cleanup_owned(&advertiser).await;
        state.publish(
            Phase::Closed,
            false,
            AdvertisementState::default(),
            Some(
                cleanup
                    .err()
                    .map(|error| Failure::from_error("advertisement withdrawal", &error))
                    .unwrap_or(failure),
            ),
            BTreeSet::new(),
        );
    }
}

async fn run_lifetime<S: StateStore>(
    mut advertiser: BeaconAdvertiser<S>,
    mut commands: mpsc::Receiver<Update>,
    state: StateCell<AdvertisementState>,
    cancellation: Cancellation,
) {
    let clock = advertiser.clock.clone();
    let mut handle = None;
    let mut next = clock.now();
    loop {
        let update = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = clock.sleep_until(next) => None,
            update = commands.recv() => match update { Some(update) => Some(update), None => break },
        };
        let reply = if let Some(update) = update {
            let mut replacement = advertiser.clone();
            replacement.payload = update.content.payload;
            replacement.protocol = update.content.protocol;
            replacement.operations = update.content.operations;
            replacement.labels = update.content.labels;
            replacement.hints = update.content.hints;
            if let Err(error) = replacement.record(1, 1, None).validate() {
                let _ = update.reply.send(Err(error));
                continue;
            }
            advertiser = replacement;
            Some(update.reply)
        } else {
            None
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            result = deadline(clock.as_ref(), Duration::from_secs(5), "advertisement publication", advertiser.publish_or_refresh(handle.as_ref())) => result,
        };
        match &result {
            Ok(published) => {
                handle = Some(published.clone());
                next = published.next_refresh_at;
                state.publish(
                    Phase::Ready,
                    true,
                    AdvertisementState {
                        handle: handle.clone(),
                    },
                    None,
                    BTreeSet::from([published.key.clone()]),
                );
            }
            Err(error) => {
                next = clock.now() + Duration::from_secs(1);
                state.publish(
                    Phase::Unavailable,
                    false,
                    AdvertisementState {
                        handle: handle.clone(),
                    },
                    Some(Failure::from_error("advertisement publication", error)),
                    BTreeSet::new(),
                );
            }
        }
        if let Some(reply) = reply {
            let _ = reply.send(result);
        }
    }
    commands.close();
    state.publish(
        Phase::Closing,
        false,
        AdvertisementState {
            handle: handle.clone(),
        },
        None,
        BTreeSet::new(),
    );
    // Construct the owned identity even if a cancelled/failed create committed
    // remotely before its acknowledgement reached this worker.
    let identity = advertiser.record(1, 1, None);
    let key = make_beacon_advertisement_key(&identity.feature_id, &identity.advertisement_id);
    let cleanup = cleanup_owned(&advertiser).await;
    state.publish(
        Phase::Closed,
        false,
        AdvertisementState::default(),
        cleanup
            .err()
            .map(|error| Failure::from_error("advertisement withdrawal", &error)),
        BTreeSet::from([key]),
    );
}

async fn cleanup_owned<S: StateStore>(advertiser: &BeaconAdvertiser<S>) -> Result<()> {
    let clock = advertiser.clock.clone();
    let identity = advertiser.record(1, 1, None);
    let key = make_beacon_advertisement_key(&identity.feature_id, &identity.advertisement_id);
    let owned = advertisement_handle(key, &identity, 0, clock.now());
    let withdraw = async {
        AssertUnwindSafe(advertiser.withdraw(&owned))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                Err(Error::StateUnavailable(
                    "advertisement withdrawal adapter panicked".into(),
                ))
            })
            .map(|_| ())
    };
    deadline(
        clock.as_ref(),
        Duration::from_secs(5),
        "advertisement withdrawal",
        withdraw,
    )
    .await
}
