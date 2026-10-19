use std::collections::{BTreeMap, BTreeSet};
use std::env::{self, VarError};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_core::Stream;
use futures_util::task::AtomicWaker;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time;

use crate::{Error, Result};

pub const DEFAULT_STATE_TTL_SECONDS: u64 = 30;
pub const DEFAULT_STATE_RENEWAL_INTERVAL_SECONDS: u64 = 5;
pub const DEFAULT_CONCORD_TOKEN_REFRESH_SECONDS: u64 = 60;
pub const DEFAULT_STATE_RECONCILE_SECONDS: u64 = 300;
pub const CONCORD_TOKEN_REFRESH_SECONDS_ENV: &str = "DECKR_CONCORD_TOKEN_REFRESH_SECONDS";
pub const STATE_RECONCILE_SECONDS_ENV: &str = "DECKR_STATE_RECONCILE_SECONDS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateMaintenancePolicy {
    pub renewal_interval: Duration,
    pub concord_token_refresh_interval: Duration,
    pub reconcile_interval: Duration,
}

impl StateMaintenancePolicy {
    pub fn from_env() -> Result<Self> {
        Self::from_env_results(
            env::var(CONCORD_TOKEN_REFRESH_SECONDS_ENV),
            env::var(STATE_RECONCILE_SECONDS_ENV),
        )
    }

    fn from_env_results(
        concord_token_refresh_env: std::result::Result<String, VarError>,
        reconcile_env: std::result::Result<String, VarError>,
    ) -> Result<Self> {
        Ok(Self {
            renewal_interval: Duration::from_secs(DEFAULT_STATE_RENEWAL_INTERVAL_SECONDS),
            concord_token_refresh_interval: interval_from_env_result(
                CONCORD_TOKEN_REFRESH_SECONDS_ENV,
                DEFAULT_CONCORD_TOKEN_REFRESH_SECONDS,
                concord_token_refresh_env,
            )?,
            reconcile_interval: interval_from_env_result(
                STATE_RECONCILE_SECONDS_ENV,
                DEFAULT_STATE_RECONCILE_SECONDS,
                reconcile_env,
            )?,
        })
    }

    #[cfg(test)]
    fn from_reconcile_env_result(
        reconcile_env: std::result::Result<String, VarError>,
    ) -> Result<Self> {
        Self::from_env_results(Err(VarError::NotPresent), reconcile_env)
    }

    pub fn concord_token_check_interval(&self) -> Duration {
        (self.concord_token_refresh_interval / 4).max(Duration::from_secs(10))
    }
}

impl Default for StateMaintenancePolicy {
    fn default() -> Self {
        Self {
            renewal_interval: Duration::from_secs(DEFAULT_STATE_RENEWAL_INTERVAL_SECONDS),
            concord_token_refresh_interval: Duration::from_secs(
                DEFAULT_CONCORD_TOKEN_REFRESH_SECONDS,
            ),
            reconcile_interval: Duration::from_secs(DEFAULT_STATE_RECONCILE_SECONDS),
        }
    }
}

fn interval_from_env_result(
    env_name: &str,
    default_seconds: u64,
    env_value: std::result::Result<String, VarError>,
) -> Result<Duration> {
    match env_value {
        Ok(value) => interval_from_env_value(env_name, default_seconds, &value),
        Err(VarError::NotPresent) => Ok(Duration::from_secs(default_seconds)),
        Err(VarError::NotUnicode(_)) => {
            Err(Error::Invalid(format!("{env_name} must be valid Unicode")))
        }
    }
}

fn interval_from_env_value(env_name: &str, default_seconds: u64, value: &str) -> Result<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Duration::from_secs(default_seconds));
    }
    let seconds = trimmed.parse::<u64>().map_err(|_| {
        Error::Invalid(format!(
            "{env_name} must be a positive integer number of seconds"
        ))
    })?;
    if seconds == 0 {
        return Err(Error::Invalid(format!(
            "{env_name} must be greater than zero"
        )));
    }
    Ok(Duration::from_secs(seconds))
}

pub(crate) fn ttl_heartbeat_delay(requested: Option<Duration>, ttl_seconds: u64) -> Duration {
    let ttl = Duration::from_secs(ttl_seconds).as_secs_f64();
    let upper = ttl * 0.75;
    let mut lower = ttl * 0.5;
    if let Some(requested) = requested {
        lower = requested.as_secs_f64().max(lower).min(upper);
    }
    if upper <= lower {
        return Duration::from_secs_f64(lower);
    }
    Duration::from_secs_f64(lower + random_unit_interval() * (upper - lower))
}

fn random_unit_interval() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    (nanos % 1_000_000) as f64 / 1_000_000.0
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateStorePolicy {
    pub broker_ttl_seconds: Option<u64>,
    pub allow_write_ttl: bool,
    pub description: String,
}

impl StateStorePolicy {
    pub fn ttl(seconds: u64, description: impl Into<String>) -> Result<Self> {
        if seconds == 0 {
            return Err(Error::Invalid(
                "broker_ttl_seconds must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            broker_ttl_seconds: Some(seconds),
            allow_write_ttl: true,
            description: description.into(),
        })
    }

    pub fn persistent(description: impl Into<String>) -> Self {
        Self {
            broker_ttl_seconds: None,
            allow_write_ttl: false,
            description: description.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateEntry {
    pub key: String,
    pub value: Value,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateOperation {
    Put,
    Delete,
    Expire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateChange {
    pub operation: StateOperation,
    pub key: String,
    pub revision: u64,
    pub entry: Option<StateEntry>,
}

pub type StateWatchStream = Pin<Box<dyn Stream<Item = Result<StateChange>> + Send + 'static>>;

/// A recovered snapshot with its captured high-water barrier. Changes observed
/// during replay beyond that barrier are installed atomically with the snapshot.
/// Adapters must expose watch failure/recovery, never silently bridge a gap.
pub struct StateSource {
    pub entries: Vec<StateEntry>,
    pub revision: u64,
    pub replayed: Vec<StateChange>,
    pub changes: StateWatchStream,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrefixObservation {
    pub entries: Vec<StateEntry>,
    pub confirmed_missing: BTreeSet<String>,
}

#[allow(async_fn_in_trait)]
pub trait StateStore: Clone + Send + Sync + 'static {
    fn ttl_seconds(&self) -> impl Future<Output = Result<Option<u64>>> + Send;
    fn get(&self, key: &str) -> impl Future<Output = Result<Option<StateEntry>>> + Send;
    fn items(&self, prefix: &str) -> impl Future<Output = Result<Vec<StateEntry>>> + Send;
    fn put(
        &self,
        key: &str,
        value: Value,
        ttl: Option<u64>,
    ) -> impl Future<Output = Result<StateEntry>> + Send;
    fn create(
        &self,
        key: &str,
        value: Value,
        ttl: Option<u64>,
    ) -> impl Future<Output = Result<StateEntry>> + Send;
    fn update(
        &self,
        key: &str,
        value: Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> impl Future<Output = Result<StateEntry>> + Send;
    fn delete(&self, key: &str, revision: Option<u64>) -> impl Future<Output = Result<()>> + Send;
    fn watch(&self, prefix: &str) -> impl Future<Output = Result<StateWatchStream>> + Send;

    fn open_source(&self, _prefix: &str) -> impl Future<Output = Result<StateSource>> + Send {
        async {
            Err(Error::Unsupported(
                "store requires an atomic snapshot/watch adapter".into(),
            ))
        }
    }

    fn delete_revision(
        &self,
        _key: &str,
        _revision: Option<u64>,
    ) -> impl Future<Output = Result<u64>> + Send {
        async {
            Err(Error::Unsupported(
                "store requires committed delete revisions".into(),
            ))
        }
    }

    /// Changes permanently on any loss of source continuity. Raw exact stores
    /// have no materialized source and use epoch zero.
    fn authority_epoch(&self) -> Option<u64> {
        Some(0)
    }
}

pub async fn observe_prefix_current<S: StateStore>(
    state: &S,
    prefix: &str,
    known_keys: impl IntoIterator<Item = String>,
) -> Result<PrefixObservation> {
    let mut observed = state
        .items(prefix)
        .await?
        .into_iter()
        .map(|entry| (entry.key.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut confirmed_missing = BTreeSet::new();
    for key in known_keys {
        if !key.starts_with(prefix) || observed.contains_key(&key) {
            continue;
        }
        if let Some(entry) = state.get(&key).await? {
            observed.insert(key, entry);
        } else {
            confirmed_missing.insert(key);
        }
    }
    Ok(PrefixObservation {
        entries: observed.into_values().collect(),
        confirmed_missing,
    })
}

const MAX_MATERIALIZED_SUBSCRIBERS: usize = 256;
const MAX_CHANGED_IDENTITIES: usize = 256;
const MAX_ABSENT_REVISIONS: usize = 2_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MaterializedStateStatus {
    Starting,
    Ready,
    Stale,
    Closed,
}

/// Immutable current state. Change detail is bounded; membership is complete.
#[derive(Debug, Clone, PartialEq)]
pub struct StateSnapshot {
    pub version: u64,
    pub source_generation: u64,
    pub current: bool,
    pub closed: bool,
    pub entries: Vec<StateEntry>,
    pub changed: BTreeSet<String>,
    pub resnapshot_required: bool,
}

struct SnapshotSubscription {
    pending: Mutex<Option<StateSnapshot>>,
    interruption: Mutex<Option<StateSnapshot>>,
    waker: AtomicWaker,
    prefix: String,
}

pub struct StateSnapshotWatch {
    subscription: Arc<SnapshotSubscription>,
    initial: Option<StateSnapshot>,
    done: bool,
}
impl Stream for StateSnapshotWatch {
    type Item = StateSnapshot;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if let Some(initial) = self.initial.take() {
            self.done = initial.closed;
            return Poll::Ready(Some(initial));
        }
        self.subscription.waker.register(cx.waker());
        let interruption = self
            .subscription
            .interruption
            .lock()
            .expect("snapshot lock poisoned")
            .take();
        if let Some(snapshot) = interruption {
            {
                let mut pending = self
                    .subscription
                    .pending
                    .lock()
                    .expect("snapshot lock poisoned");
                if pending
                    .as_ref()
                    .is_some_and(|pending| pending.version <= snapshot.version)
                {
                    pending.take();
                }
            }
            self.done = snapshot.closed;
            return Poll::Ready(Some(snapshot));
        }
        let snapshot = self
            .subscription
            .pending
            .lock()
            .expect("snapshot lock poisoned")
            .take();
        if let Some(snapshot) = snapshot {
            self.done = snapshot.closed;
            return Poll::Ready(Some(snapshot));
        }
        Poll::Pending
    }
}

#[derive(Clone)]
pub struct MaterializedStateStore<S: StateStore> {
    state: S,
    prefix: String,
    inner: Arc<Mutex<MaterializedStateInner>>,
    owner: Arc<SourceOwner>,
}
struct SourceOwner {
    task: Mutex<Option<JoinHandle<()>>>,
    inner: Arc<Mutex<MaterializedStateInner>>,
}
impl Drop for SourceOwner {
    fn drop(&mut self) {
        self.inner
            .lock()
            .expect("source lock poisoned")
            .set_status(MaterializedStateStatus::Closed);
        if let Some(task) = self.task.get_mut().expect("task lock poisoned").take() {
            task.abort();
        }
    }
}
struct MaterializedStateInner {
    status: MaterializedStateStatus,
    entries: BTreeMap<String, StateEntry>,
    revision_by_key: BTreeMap<String, u64>,
    barrier: u64,
    generation: u64,
    authority_epoch: u64,
    subscribers: Vec<Weak<SnapshotSubscription>>,
    changed: Arc<Notify>,
}

impl<S: StateStore> MaterializedStateStore<S> {
    pub async fn start(state: S, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        let source = state.open_source(&prefix).await?;
        let inner = Arc::new(Mutex::new(MaterializedStateInner {
            status: MaterializedStateStatus::Starting,
            entries: BTreeMap::new(),
            revision_by_key: BTreeMap::new(),
            barrier: 0,
            generation: 0,
            authority_epoch: 0,
            subscribers: Vec::new(),
            changed: Arc::new(Notify::new()),
        }));
        inner
            .lock()
            .expect("source lock poisoned")
            .install_snapshot(&source);
        let watch_inner = inner.clone();
        let watch_state = state.clone();
        let watch_prefix = prefix.clone();
        let task = tokio::spawn(async move {
            let mut changes = source.changes;
            loop {
                while let Some(change) = changes.next().await {
                    let mut inner = watch_inner.lock().expect("source lock poisoned");
                    if let Ok(change) = change {
                        inner.apply_change(change);
                        if inner
                            .revision_by_key
                            .len()
                            .saturating_sub(inner.entries.len())
                            < MAX_ABSENT_REVISIONS
                        {
                            continue;
                        }
                    }
                    break;
                }
                watch_inner
                    .lock()
                    .expect("source lock poisoned")
                    .set_status(MaterializedStateStatus::Stale);
                // Drop the failed consumer before opening its replacement.
                drop(changes);
                loop {
                    time::sleep(Duration::from_millis(100)).await;
                    match watch_state.open_source(&watch_prefix).await {
                        Ok(source) => {
                            watch_inner
                                .lock()
                                .expect("source lock poisoned")
                                .install_snapshot(&source);
                            changes = source.changes;
                            break;
                        }
                        Err(_) => continue,
                    }
                }
            }
        });
        let owner = Arc::new(SourceOwner {
            task: Mutex::new(Some(task)),
            inner: inner.clone(),
        });
        Ok(Self {
            state,
            prefix,
            inner,
            owner,
        })
    }

    pub fn status(&self) -> MaterializedStateStatus {
        self.inner.lock().expect("source lock poisoned").status
    }
    pub fn is_current(&self) -> bool {
        self.status() == MaterializedStateStatus::Ready
    }
    pub fn generation(&self) -> u64 {
        self.inner.lock().expect("source lock poisoned").generation
    }
    pub fn snapshot(&self) -> StateSnapshot {
        self.inner
            .lock()
            .expect("source lock poisoned")
            .snapshot("", BTreeSet::new(), false)
    }
    pub fn authority_epoch(&self) -> Option<u64> {
        let inner = self.inner.lock().expect("source lock poisoned");
        (inner.status == MaterializedStateStatus::Ready).then_some(inner.authority_epoch)
    }
    pub async fn wait_current(&self) -> Result<()> {
        let notify = self
            .inner
            .lock()
            .expect("source lock poisoned")
            .changed
            .clone();
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.status() {
                MaterializedStateStatus::Ready => return Ok(()),
                MaterializedStateStatus::Closed => {
                    return Err(Error::Closed("source closed".into()))
                }
                _ => notified.await,
            }
        }
    }
    pub fn close(&self) {
        self.inner
            .lock()
            .expect("source lock poisoned")
            .set_status(MaterializedStateStatus::Closed);
        if let Some(task) = self.owner.task.lock().expect("task lock poisoned").as_ref() {
            task.abort();
        }
    }
    pub async fn close_and_wait(&self) {
        self.close();
        let task = self.owner.task.lock().expect("task lock poisoned").take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
    pub fn get_cached(&self, key: &str) -> Result<Option<StateEntry>> {
        let inner = self.inner.lock().expect("source lock poisoned");
        check_materialized_status(inner.status)?;
        Ok(inner.entries.get(key).cloned())
    }
    pub fn items_cached(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        let inner = self.inner.lock().expect("source lock poisoned");
        check_materialized_status(inner.status)?;
        Ok(inner
            .entries
            .values()
            .filter(|entry| entry.key.starts_with(prefix))
            .cloned()
            .collect())
    }
    pub fn cached_keys(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .items_cached("")?
            .into_iter()
            .map(|entry| entry.key)
            .collect())
    }

    /// Registers and captures the initial snapshot under the same state lock.
    pub fn watch_snapshots(&self, prefix: &str) -> Result<StateSnapshotWatch> {
        let mut inner = self.inner.lock().expect("source lock poisoned");
        inner
            .subscribers
            .retain(|subscriber| subscriber.strong_count() > 0);
        if inner.subscribers.len() >= MAX_MATERIALIZED_SUBSCRIBERS {
            return Err(Error::StateUnavailable(
                "state watch registration limit (256) reached".into(),
            ));
        }
        let initial = inner.snapshot(prefix, BTreeSet::new(), false);
        let subscription = Arc::new(SnapshotSubscription {
            pending: Mutex::new(None),
            interruption: Mutex::new(None),
            waker: AtomicWaker::new(),
            prefix: prefix.into(),
        });
        inner.subscribers.push(Arc::downgrade(&subscription));
        Ok(StateSnapshotWatch {
            subscription,
            initial: Some(initial),
            done: false,
        })
    }

    /// Compatibility change adapter. New consumers should use watch_snapshots.
    /// It diffs complete snapshots instead of retaining an unbounded event queue.
    pub fn subscribe_cached(&self) -> StateWatchStream {
        let watch = match self.watch_snapshots("") {
            Ok(watch) => watch,
            Err(error) => return Box::pin(futures_util::stream::once(async { Err(error) })),
        };
        snapshot_changes(watch, false)
    }

    /// Request repair without mutating the source from unordered exact reads.
    pub async fn reconcile_snapshot(
        &self,
        known_keys: impl IntoIterator<Item = String>,
    ) -> Result<PrefixObservation> {
        observe_prefix_current(&self.state, &self.prefix, known_keys).await
    }
    pub async fn get_exact(&self, key: &str) -> Result<Option<StateEntry>> {
        self.state.get(key).await
    }
    pub async fn items_exact(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        self.state.items(prefix).await
    }
    pub async fn ttl_seconds(&self) -> Result<Option<u64>> {
        self.state.ttl_seconds().await
    }
    pub async fn wait_observed(&self, key: &str, revision: u64) -> Result<()> {
        self.wait_observed_in_epoch(key, revision, self.authority_epoch())
            .await
    }
    async fn wait_observed_in_epoch(
        &self,
        key: &str,
        revision: u64,
        epoch: Option<u64>,
    ) -> Result<()> {
        let notify = self
            .inner
            .lock()
            .expect("source lock poisoned")
            .changed
            .clone();
        let wait = async {
            loop {
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let inner = self.inner.lock().expect("source lock poisoned");
                    check_materialized_status(inner.status)?;
                    if Some(inner.authority_epoch) != epoch {
                        return Err(Error::MaterializedViewStale(
                            "source continuity lost while waiting for a write".into(),
                        ));
                    }
                    if inner.barrier >= revision
                        || inner
                            .revision_by_key
                            .get(key)
                            .is_some_and(|observed| *observed >= revision)
                    {
                        return Ok(());
                    }
                }
                notified.await;
            }
        };
        time::timeout(Duration::from_secs(5), wait)
            .await
            .map_err(|_| {
                Error::StateUnavailable(
                    "watch did not observe committed revision before deadline".into(),
                )
            })?
    }
    pub async fn put(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        let epoch = self.authority_epoch();
        let entry = self.state.put(key, value, ttl).await?;
        self.wait_observed_in_epoch(key, entry.revision, epoch)
            .await?;
        Ok(entry)
    }
    pub async fn create(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        let epoch = self.authority_epoch();
        let entry = self.state.create(key, value, ttl).await?;
        self.wait_observed_in_epoch(key, entry.revision, epoch)
            .await?;
        Ok(entry)
    }
    pub async fn update(
        &self,
        key: &str,
        value: Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<StateEntry> {
        let epoch = self.authority_epoch();
        let entry = self.state.update(key, value, revision, ttl).await?;
        self.wait_observed_in_epoch(key, entry.revision, epoch)
            .await?;
        Ok(entry)
    }
    pub async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        let epoch = self.authority_epoch();
        let revision = self.state.delete_revision(key, revision).await?;
        self.wait_observed_in_epoch(key, revision, epoch).await
    }
}

impl MaterializedStateInner {
    fn snapshot(
        &self,
        prefix: &str,
        changed: BTreeSet<String>,
        resnapshot_required: bool,
    ) -> StateSnapshot {
        StateSnapshot {
            version: self.generation,
            source_generation: self.authority_epoch,
            current: self.status == MaterializedStateStatus::Ready,
            closed: self.status == MaterializedStateStatus::Closed,
            entries: self
                .entries
                .values()
                .filter(|entry| entry.key.starts_with(prefix))
                .cloned()
                .collect(),
            changed,
            resnapshot_required,
        }
    }
    fn notify(&mut self, changed: BTreeSet<String>) {
        self.generation += 1;
        self.changed.notify_waiters();
        self.subscribers
            .retain(|subscriber| subscriber.strong_count() > 0);
        for subscriber in self.subscribers.iter().filter_map(Weak::upgrade) {
            let mut pending = subscriber.pending.lock().expect("snapshot lock poisoned");
            let mut identities: BTreeSet<_> = changed
                .iter()
                .filter(|key| key.starts_with(&subscriber.prefix))
                .cloned()
                .collect();
            let mut overflow = false;
            if let Some(previous) = pending.as_ref() {
                identities.extend(previous.changed.iter().cloned());
                overflow = previous.resnapshot_required;
            }
            overflow |= identities.len() > MAX_CHANGED_IDENTITIES;
            if overflow {
                identities.clear();
            }
            let snapshot = self.snapshot(&subscriber.prefix, identities, overflow);
            if !snapshot.current {
                *subscriber
                    .interruption
                    .lock()
                    .expect("snapshot lock poisoned") = Some(snapshot.clone());
            }
            *pending = Some(snapshot);
            subscriber.waker.wake();
        }
    }
    fn set_status(&mut self, status: MaterializedStateStatus) {
        if self.status == MaterializedStateStatus::Closed || self.status == status {
            return;
        }
        if status != MaterializedStateStatus::Ready {
            self.authority_epoch += 1;
        }
        self.status = status;
        self.notify(BTreeSet::new());
    }
    fn install_snapshot(&mut self, source: &StateSource) {
        if self.status == MaterializedStateStatus::Closed {
            return;
        }
        let mut entries: BTreeMap<_, _> = source
            .entries
            .iter()
            .map(|entry| (entry.key.clone(), entry.clone()))
            .collect();
        let mut revisions: BTreeMap<_, _> = entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.revision))
            .collect();
        for change in &source.replayed {
            if change.revision <= source.revision
                || change.revision <= revisions.get(&change.key).copied().unwrap_or(0)
            {
                continue;
            }
            revisions.insert(change.key.clone(), change.revision);
            match &change.entry {
                Some(entry) if change.operation == StateOperation::Put => {
                    entries.insert(change.key.clone(), entry.clone());
                }
                _ => {
                    entries.remove(&change.key);
                }
            }
        }
        let changed = self
            .entries
            .keys()
            .chain(entries.keys())
            .filter(|key| self.entries.get(*key) != entries.get(*key))
            .cloned()
            .collect();
        self.revision_by_key = revisions;
        self.entries = entries;
        self.barrier = source.revision;
        self.status = MaterializedStateStatus::Ready;
        self.notify(changed);
    }
    fn apply_change(&mut self, change: StateChange) {
        if change.revision <= self.barrier
            || change.revision <= self.revision_by_key.get(&change.key).copied().unwrap_or(0)
        {
            return;
        }
        self.revision_by_key
            .insert(change.key.clone(), change.revision);
        match change.entry {
            Some(entry) if change.operation == StateOperation::Put => {
                self.entries.insert(change.key.clone(), entry);
            }
            _ => {
                self.entries.remove(&change.key);
            }
        }
        self.notify(BTreeSet::from([change.key]));
    }
}

pub(crate) fn snapshot_changes(
    watch: StateSnapshotWatch,
    include_initial: bool,
) -> StateWatchStream {
    Box::pin(futures_util::stream::unfold(
        (
            watch,
            None::<BTreeMap<String, StateEntry>>,
            std::collections::VecDeque::new(),
        ),
        move |(mut watch, mut previous, mut queued)| async move {
            loop {
                if let Some(change) = queued.pop_front() {
                    return Some((Ok(change), (watch, previous, queued)));
                }
                let snapshot = watch.next().await?;
                if !snapshot.current {
                    return Some((
                        Err(if snapshot.closed {
                            Error::Closed("source closed".into())
                        } else {
                            Error::MaterializedViewStale("source stale".into())
                        }),
                        (watch, previous, queued),
                    ));
                }
                let next: BTreeMap<_, _> = snapshot
                    .entries
                    .into_iter()
                    .map(|entry| (entry.key.clone(), entry))
                    .collect();
                if previous.is_some() || include_initial {
                    let old = previous.take().unwrap_or_default();
                    for (key, entry) in &next {
                        if old.get(key) != Some(entry) {
                            queued.push_back(StateChange {
                                operation: StateOperation::Put,
                                key: key.clone(),
                                revision: entry.revision,
                                entry: Some(entry.clone()),
                            });
                        }
                    }
                    for (key, entry) in old {
                        if !next.contains_key(&key) {
                            queued.push_back(StateChange {
                                operation: StateOperation::Delete,
                                key,
                                revision: entry.revision,
                                entry: None,
                            });
                        }
                    }
                }
                previous = Some(next);
            }
        },
    ))
}

fn check_materialized_status(status: MaterializedStateStatus) -> Result<()> {
    match status {
        MaterializedStateStatus::Ready => Ok(()),
        MaterializedStateStatus::Starting => Err(Error::MaterializedViewStale(
            "materialized state is still starting".to_string(),
        )),
        MaterializedStateStatus::Stale => Err(Error::MaterializedViewStale(
            "materialized state watch is stale".to_string(),
        )),
        MaterializedStateStatus::Closed => Err(Error::Closed(
            "materialized state store is closed".to_string(),
        )),
    }
}

impl<S: StateStore> StateStore for MaterializedStateStore<S> {
    fn authority_epoch(&self) -> Option<u64> {
        MaterializedStateStore::authority_epoch(self)
    }

    async fn open_source(&self, prefix: &str) -> Result<StateSource> {
        self.state.open_source(prefix).await
    }

    async fn delete_revision(&self, key: &str, revision: Option<u64>) -> Result<u64> {
        let epoch = self.authority_epoch();
        let revision = self.state.delete_revision(key, revision).await?;
        self.wait_observed_in_epoch(key, revision, epoch).await?;
        Ok(revision)
    }
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        self.state.ttl_seconds().await
    }

    async fn get(&self, key: &str) -> Result<Option<StateEntry>> {
        self.get_exact(key).await
    }

    async fn items(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        self.items_exact(prefix).await
    }

    async fn put(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        MaterializedStateStore::put(self, key, value, ttl).await
    }

    async fn create(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        MaterializedStateStore::create(self, key, value, ttl).await
    }

    async fn update(
        &self,
        key: &str,
        value: Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<StateEntry> {
        MaterializedStateStore::update(self, key, value, revision, ttl).await
    }

    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        MaterializedStateStore::delete(self, key, revision).await
    }

    async fn watch(&self, prefix: &str) -> Result<StateWatchStream> {
        self.state.watch(prefix).await
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryStateStore {
    inner: Arc<Mutex<MemoryStateInner>>,
}

#[derive(Debug, Default)]
struct MemoryStateInner {
    entries: BTreeMap<String, StateEntry>,
    revision: u64,
    watchers: Vec<MemoryStateWatcher>,
    ttl_seconds: Option<u64>,
}

#[derive(Debug)]
struct MemoryStateWatcher {
    prefix: String,
    sender: UnboundedSender<Result<StateChange>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ttl_bound(ttl_seconds: u64) -> Result<Self> {
        Self::new().with_ttl_seconds(ttl_seconds)
    }

    pub fn with_ttl_seconds(self, ttl_seconds: u64) -> Result<Self> {
        self.set_ttl_seconds(Some(ttl_seconds))?;
        Ok(self)
    }

    pub fn set_ttl_seconds(&self, ttl_seconds: Option<u64>) -> Result<()> {
        if ttl_seconds == Some(0) {
            return Err(Error::Invalid(
                "memory state TTL must be greater than zero".to_string(),
            ));
        }
        self.inner
            .lock()
            .expect("memory state mutex poisoned")
            .ttl_seconds = ttl_seconds;
        Ok(())
    }

    fn next_revision(inner: &mut MemoryStateInner) -> u64 {
        inner.revision += 1;
        inner.revision
    }

    fn validate_ttl(inner: &MemoryStateInner, ttl: Option<u64>) -> Result<()> {
        if let Some(ttl) = ttl {
            if Some(ttl) != inner.ttl_seconds {
                return Err(Error::Invalid(format!(
                    "memory state uses bucket TTL {:?}; per-key TTL {ttl} is not supported",
                    inner.ttl_seconds
                )));
            }
        }
        Ok(())
    }

    fn notify_watchers(inner: &mut MemoryStateInner, change: StateChange) {
        inner.watchers.retain(|watcher| {
            if !change.key.starts_with(&watcher.prefix) {
                return true;
            }
            watcher.sender.unbounded_send(Ok(change.clone())).is_ok()
        });
    }
}

impl StateStore for MemoryStateStore {
    async fn open_source(&self, prefix: &str) -> Result<StateSource> {
        let (sender, receiver) = unbounded();
        let mut inner = self.inner.lock().expect("memory state mutex poisoned");
        let entries = inner
            .entries
            .values()
            .filter(|entry| entry.key.starts_with(prefix))
            .cloned()
            .collect();
        let revision = inner.revision;
        inner.watchers.push(MemoryStateWatcher {
            prefix: prefix.to_string(),
            sender,
        });
        Ok(StateSource {
            entries,
            revision,
            replayed: Vec::new(),
            changes: Box::pin(receiver),
        })
    }
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        Ok(self
            .inner
            .lock()
            .expect("memory state mutex poisoned")
            .ttl_seconds)
    }

    async fn get(&self, key: &str) -> Result<Option<StateEntry>> {
        Ok(self
            .inner
            .lock()
            .expect("memory state mutex poisoned")
            .entries
            .get(key)
            .cloned())
    }

    async fn items(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        Ok(self
            .inner
            .lock()
            .expect("memory state mutex poisoned")
            .entries
            .iter()
            .filter_map(|(key, entry)| key.starts_with(prefix).then_some(entry.clone()))
            .collect())
    }

    async fn put(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        let mut inner = self.inner.lock().expect("memory state mutex poisoned");
        Self::validate_ttl(&inner, ttl)?;
        let entry = StateEntry {
            key: key.to_string(),
            value,
            revision: Self::next_revision(&mut inner),
        };
        inner.entries.insert(key.to_string(), entry.clone());
        Self::notify_watchers(
            &mut inner,
            StateChange {
                operation: StateOperation::Put,
                key: key.to_string(),
                revision: entry.revision,
                entry: Some(entry.clone()),
            },
        );
        Ok(entry)
    }

    async fn create(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        let mut inner = self.inner.lock().expect("memory state mutex poisoned");
        Self::validate_ttl(&inner, ttl)?;
        if inner.entries.contains_key(key) {
            return Err(Error::StateConflict(format!(
                "state key {key:?} already exists"
            )));
        }
        let entry = StateEntry {
            key: key.to_string(),
            value,
            revision: Self::next_revision(&mut inner),
        };
        inner.entries.insert(key.to_string(), entry.clone());
        Self::notify_watchers(
            &mut inner,
            StateChange {
                operation: StateOperation::Put,
                key: key.to_string(),
                revision: entry.revision,
                entry: Some(entry.clone()),
            },
        );
        Ok(entry)
    }

    async fn update(
        &self,
        key: &str,
        value: Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<StateEntry> {
        let mut inner = self.inner.lock().expect("memory state mutex poisoned");
        Self::validate_ttl(&inner, ttl)?;
        let Some(current) = inner.entries.get(key) else {
            return Err(Error::StateConflict(format!(
                "state key {key:?} is missing"
            )));
        };
        if current.revision != revision {
            return Err(Error::StateConflict(format!(
                "state key {key:?} revision changed"
            )));
        }
        let entry = StateEntry {
            key: key.to_string(),
            value,
            revision: Self::next_revision(&mut inner),
        };
        inner.entries.insert(key.to_string(), entry.clone());
        Self::notify_watchers(
            &mut inner,
            StateChange {
                operation: StateOperation::Put,
                key: key.to_string(),
                revision: entry.revision,
                entry: Some(entry.clone()),
            },
        );
        Ok(entry)
    }

    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        self.delete_revision(key, revision).await.map(|_| ())
    }

    async fn delete_revision(&self, key: &str, revision: Option<u64>) -> Result<u64> {
        let mut inner = self.inner.lock().expect("memory state mutex poisoned");
        if let Some(expected) = revision {
            let Some(current) = inner.entries.get(key) else {
                return Err(Error::StateConflict(format!(
                    "state key {key:?} is missing"
                )));
            };
            if current.revision != expected {
                return Err(Error::StateConflict(format!(
                    "state key {key:?} revision changed"
                )));
            }
        }
        inner.entries.remove(key);
        let revision = Self::next_revision(&mut inner);
        {
            Self::notify_watchers(
                &mut inner,
                StateChange {
                    operation: StateOperation::Delete,
                    key: key.to_string(),
                    revision,
                    entry: None,
                },
            );
        }
        Ok(revision)
    }

    async fn watch(&self, prefix: &str) -> Result<StateWatchStream> {
        let (sender, receiver) = unbounded();
        self.inner
            .lock()
            .expect("memory state mutex poisoned")
            .watchers
            .push(MemoryStateWatcher {
                prefix: prefix.to_string(),
                sender,
            });
        Ok(Box::pin(receiver))
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use futures_util::StreamExt;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn memory_state_watch_emits_prefix_filtered_changes() {
        let state = MemoryStateStore::new();
        let mut watch = state.watch("matched.").await.unwrap();

        state
            .put("other.key", json!({"ignored": true}), None)
            .await
            .unwrap();
        let created = state
            .create("matched.key", json!({"value": 1}), None)
            .await
            .unwrap();
        let change = watch.next().await.unwrap().unwrap();
        assert_eq!(change.operation, StateOperation::Put);
        assert_eq!(change.key, "matched.key");
        assert_eq!(change.entry, Some(created.clone()));

        let updated = state
            .update("matched.key", json!({"value": 2}), created.revision, None)
            .await
            .unwrap();
        let change = watch.next().await.unwrap().unwrap();
        assert_eq!(change.operation, StateOperation::Put);
        assert_eq!(change.entry, Some(updated.clone()));

        state
            .delete("matched.key", Some(updated.revision))
            .await
            .unwrap();
        let change = watch.next().await.unwrap().unwrap();
        assert_eq!(change.operation, StateOperation::Delete);
        assert_eq!(change.key, "matched.key");
        assert_eq!(change.entry, None);
    }

    #[tokio::test]
    async fn materialized_state_store_caches_watch_and_exact_writes() {
        let state = MemoryStateStore::new();
        state
            .put("matched.initial", json!({"value": 1}), None)
            .await
            .unwrap();
        let materialized = MaterializedStateStore::start(state.clone(), "matched.")
            .await
            .unwrap();

        assert_eq!(materialized.status(), MaterializedStateStatus::Ready);
        assert_eq!(
            materialized
                .get_cached("matched.initial")
                .unwrap()
                .unwrap()
                .value,
            json!({"value": 1})
        );

        let mut cached_watch = materialized.subscribe_cached();
        let created = materialized
            .create("matched.created", json!({"value": 2}), None)
            .await
            .unwrap();
        assert_eq!(
            materialized
                .get_cached("matched.created")
                .unwrap()
                .unwrap()
                .revision,
            created.revision
        );
        let change = cached_watch.next().await.unwrap().unwrap();
        assert_eq!(change.operation, StateOperation::Put);
        assert_eq!(change.key, "matched.created");

        state
            .put("matched.external", json!({"value": 3}), None)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if materialized
                    .get_cached("matched.external")
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        materialized
            .delete("matched.created", Some(created.revision))
            .await
            .unwrap();
        assert!(materialized
            .get_cached("matched.created")
            .unwrap()
            .is_none());

        materialized.close();
        assert!(matches!(
            materialized.get_cached("matched.initial"),
            Err(Error::Closed(_))
        ));
    }

    #[test]
    fn default_state_maintenance_policy_uses_shared_defaults() {
        let policy = StateMaintenancePolicy::default();

        assert_eq!(
            policy.renewal_interval,
            Duration::from_secs(DEFAULT_STATE_RENEWAL_INTERVAL_SECONDS)
        );
        assert_eq!(
            policy.concord_token_refresh_interval,
            Duration::from_secs(DEFAULT_CONCORD_TOKEN_REFRESH_SECONDS)
        );
        assert_eq!(
            policy.reconcile_interval,
            Duration::from_secs(DEFAULT_STATE_RECONCILE_SECONDS)
        );
        assert_eq!(
            policy.concord_token_check_interval(),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn concord_token_refresh_env_absent_or_blank_uses_default() {
        for env_value in [
            Err(VarError::NotPresent),
            Ok(String::new()),
            Ok("  ".to_string()),
        ] {
            let policy =
                StateMaintenancePolicy::from_env_results(env_value, Err(VarError::NotPresent))
                    .unwrap();
            assert_eq!(
                policy.concord_token_refresh_interval,
                Duration::from_secs(DEFAULT_CONCORD_TOKEN_REFRESH_SECONDS)
            );
        }
    }

    #[test]
    fn concord_token_refresh_env_accepts_positive_seconds() {
        let policy = StateMaintenancePolicy::from_env_results(
            Ok("  20  ".to_string()),
            Err(VarError::NotPresent),
        )
        .unwrap();

        assert_eq!(
            policy.concord_token_refresh_interval,
            Duration::from_secs(20)
        );
    }

    #[test]
    fn concord_token_refresh_env_rejects_zero() {
        let error = StateMaintenancePolicy::from_env_results(
            Ok("0".to_string()),
            Err(VarError::NotPresent),
        )
        .expect_err("zero token refresh interval should fail");

        assert_invalid(error, "greater than zero");
    }

    #[test]
    fn state_reconcile_env_absent_or_blank_uses_default() {
        for env_value in [
            Err(VarError::NotPresent),
            Ok(String::new()),
            Ok("  ".to_string()),
        ] {
            let policy = StateMaintenancePolicy::from_reconcile_env_result(env_value).unwrap();
            assert_eq!(
                policy.reconcile_interval,
                Duration::from_secs(DEFAULT_STATE_RECONCILE_SECONDS)
            );
        }
    }

    #[test]
    fn state_reconcile_env_accepts_positive_seconds() {
        let policy =
            StateMaintenancePolicy::from_reconcile_env_result(Ok("  45  ".to_string())).unwrap();

        assert_eq!(policy.reconcile_interval, Duration::from_secs(45));
    }

    #[test]
    fn state_reconcile_env_rejects_zero() {
        let error = StateMaintenancePolicy::from_reconcile_env_result(Ok("0".to_string()))
            .expect_err("zero reconcile interval should fail");

        assert_invalid(error, "greater than zero");
    }

    #[test]
    fn state_reconcile_env_rejects_malformed_values() {
        for value in ["abc", "-1", "1.5"] {
            let error = StateMaintenancePolicy::from_reconcile_env_result(Ok(value.to_string()))
                .expect_err("malformed reconcile interval should fail");
            assert_invalid(error, "positive integer");
        }
    }

    #[test]
    fn state_reconcile_env_rejects_non_unicode_values() {
        let error = StateMaintenancePolicy::from_reconcile_env_result(Err(VarError::NotUnicode(
            OsString::from("not unicode"),
        )))
        .expect_err("non-unicode reconcile interval should fail");

        assert_invalid(error, "valid Unicode");
    }

    fn assert_invalid(error: Error, expected: &str) {
        match error {
            Error::Invalid(message) => assert!(
                message.contains(expected),
                "expected {message:?} to contain {expected:?}"
            ),
            other => panic!("expected invalid error, got {other:?}"),
        }
    }
}
