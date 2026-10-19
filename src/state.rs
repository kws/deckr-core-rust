use std::collections::{BTreeMap, BTreeSet};
use std::env::{self, VarError};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

const MAX_MATERIALIZED_SUBSCRIBERS: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MaterializedStateStatus {
    Starting,
    Ready,
    Stale,
    Closed,
}

#[derive(Clone)]
pub struct MaterializedStateStore<S: StateStore> {
    state: S,
    prefix: String,
    inner: Arc<Mutex<MaterializedStateInner>>,
    watch_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Debug)]
struct MaterializedStateInner {
    status: MaterializedStateStatus,
    entries: BTreeMap<String, StateEntry>,
    revision_by_key: BTreeMap<String, u64>,
    generation: u64,
    subscribers: Vec<UnboundedSender<Result<StateChange>>>,
}

impl<S: StateStore> MaterializedStateStore<S> {
    pub async fn start(state: S, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        let mut watch = state.watch(&prefix).await?;
        let entries = state
            .items(&prefix)
            .await?
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect::<BTreeMap<_, _>>();
        let revision_by_key = entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.revision))
            .collect();
        let inner = Arc::new(Mutex::new(MaterializedStateInner {
            status: MaterializedStateStatus::Ready,
            entries,
            revision_by_key,
            generation: 0,
            subscribers: Vec::new(),
        }));
        let watch_inner = inner.clone();
        let watch_prefix = prefix.clone();
        let watch_state = state.clone();
        let watch_task = tokio::spawn(async move {
            loop {
                let retry = match watch.next().await {
                    Some(Ok(change)) => {
                        let mut inner = watch_inner
                            .lock()
                            .expect("materialized state mutex poisoned");
                        inner.apply_change(change);
                        false
                    }
                    Some(Err(error)) => {
                        let mut inner = watch_inner
                            .lock()
                            .expect("materialized state mutex poisoned");
                        inner.set_status(MaterializedStateStatus::Stale);
                        inner.notify_error(error);
                        true
                    }
                    None => {
                        let mut inner = watch_inner
                            .lock()
                            .expect("materialized state mutex poisoned");
                        inner.set_status(MaterializedStateStatus::Stale);
                        inner.notify_error(Error::StateUnavailable(
                            "materialized state watch ended".to_string(),
                        ));
                        true
                    }
                };
                if !retry {
                    continue;
                }

                loop {
                    if watch_inner
                        .lock()
                        .expect("materialized state mutex poisoned")
                        .status
                        == MaterializedStateStatus::Closed
                    {
                        return;
                    }
                    time::sleep(Duration::from_secs(1)).await;
                    match watch_state.watch(&watch_prefix).await {
                        Ok(next_watch) => {
                            watch = next_watch;
                            match watch_state.items(&watch_prefix).await {
                                Ok(entries) => {
                                    let mut inner = watch_inner
                                        .lock()
                                        .expect("materialized state mutex poisoned");
                                    inner.reconcile_snapshot(entries, &watch_prefix);
                                    inner.set_status(MaterializedStateStatus::Ready);
                                }
                                Err(error) => {
                                    let mut inner = watch_inner
                                        .lock()
                                        .expect("materialized state mutex poisoned");
                                    inner.set_status(MaterializedStateStatus::Stale);
                                    inner.notify_error(error);
                                    continue;
                                }
                            }
                            break;
                        }
                        Err(error) => {
                            let mut inner = watch_inner
                                .lock()
                                .expect("materialized state mutex poisoned");
                            inner.set_status(MaterializedStateStatus::Stale);
                            inner.notify_error(error);
                        }
                    }
                }
            }
        });
        Ok(Self {
            state,
            prefix,
            inner,
            watch_task: Arc::new(Mutex::new(Some(watch_task))),
        })
    }

    pub fn status(&self) -> MaterializedStateStatus {
        self.inner
            .lock()
            .expect("materialized state mutex poisoned")
            .status
    }

    pub fn is_current(&self) -> bool {
        self.status() == MaterializedStateStatus::Ready
    }

    pub async fn wait_current(&self) -> Result<()> {
        loop {
            match self.status() {
                MaterializedStateStatus::Ready => return Ok(()),
                MaterializedStateStatus::Closed => {
                    return Err(Error::Closed(
                        "materialized state store is closed".to_string(),
                    ))
                }
                MaterializedStateStatus::Starting | MaterializedStateStatus::Stale => {
                    time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    pub fn generation(&self) -> u64 {
        self.inner
            .lock()
            .expect("materialized state mutex poisoned")
            .generation
    }

    pub fn close(&self) {
        if let Some(task) = self
            .watch_task
            .lock()
            .expect("materialized watch task mutex poisoned")
            .take()
        {
            task.abort();
        }
        self.inner
            .lock()
            .expect("materialized state mutex poisoned")
            .set_status(MaterializedStateStatus::Closed);
    }

    pub fn get_cached(&self, key: &str) -> Result<Option<StateEntry>> {
        let inner = self
            .inner
            .lock()
            .expect("materialized state mutex poisoned");
        check_materialized_status(inner.status)?;
        Ok(inner.entries.get(key).cloned())
    }

    pub fn items_cached(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        let inner = self
            .inner
            .lock()
            .expect("materialized state mutex poisoned");
        check_materialized_status(inner.status)?;
        Ok(inner
            .entries
            .iter()
            .filter_map(|(key, entry)| key.starts_with(prefix).then_some(entry.clone()))
            .collect())
    }

    pub fn cached_keys(&self) -> Result<BTreeSet<String>> {
        let inner = self
            .inner
            .lock()
            .expect("materialized state mutex poisoned");
        check_materialized_status(inner.status)?;
        Ok(inner.entries.keys().cloned().collect())
    }

    pub fn subscribe_cached(&self) -> StateWatchStream {
        let (sender, receiver) = unbounded();
        let mut inner = self
            .inner
            .lock()
            .expect("materialized state mutex poisoned");
        if inner.subscribers.len() >= MAX_MATERIALIZED_SUBSCRIBERS {
            inner.subscribers.remove(0);
        }
        inner.subscribers.push(sender);
        Box::pin(receiver)
    }

    pub async fn reconcile_snapshot(
        &self,
        known_keys: impl IntoIterator<Item = String>,
    ) -> Result<PrefixObservation> {
        let observation = observe_prefix_current(&self.state, &self.prefix, known_keys).await?;
        let mut inner = self
            .inner
            .lock()
            .expect("materialized state mutex poisoned");
        inner.reconcile_snapshot(observation.entries.clone(), &self.prefix);
        for key in &observation.confirmed_missing {
            let revision = inner.revision_by_key.get(key).copied().unwrap_or(0) + 1;
            inner.apply_change(StateChange {
                operation: StateOperation::Delete,
                key: key.clone(),
                revision,
                entry: None,
            });
        }
        inner.set_status(MaterializedStateStatus::Ready);
        Ok(observation)
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

    pub async fn put(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        let entry = self.state.put(key, value, ttl).await?;
        self.apply_entry(entry.clone());
        Ok(entry)
    }

    pub async fn create(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        let entry = self.state.create(key, value, ttl).await?;
        self.apply_entry(entry.clone());
        Ok(entry)
    }

    pub async fn update(
        &self,
        key: &str,
        value: Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<StateEntry> {
        let entry = self.state.update(key, value, revision, ttl).await?;
        self.apply_entry(entry.clone());
        Ok(entry)
    }

    pub async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        self.state.delete(key, revision).await?;
        let mut inner = self
            .inner
            .lock()
            .expect("materialized state mutex poisoned");
        let revision = inner.revision_by_key.get(key).copied().unwrap_or(0) + 1;
        inner.apply_change(StateChange {
            operation: StateOperation::Delete,
            key: key.to_string(),
            revision,
            entry: None,
        });
        Ok(())
    }

    fn apply_entry(&self, entry: StateEntry) {
        self.inner
            .lock()
            .expect("materialized state mutex poisoned")
            .apply_change(StateChange {
                operation: StateOperation::Put,
                key: entry.key.clone(),
                revision: entry.revision,
                entry: Some(entry),
            });
    }
}

impl MaterializedStateInner {
    fn set_status(&mut self, status: MaterializedStateStatus) {
        if self.status == MaterializedStateStatus::Closed
            && status != MaterializedStateStatus::Closed
        {
            return;
        }
        self.status = status;
    }

    fn apply_change(&mut self, change: StateChange) {
        let current_revision = self.revision_by_key.get(&change.key).copied().unwrap_or(0);
        if change.revision <= current_revision {
            return;
        }
        self.revision_by_key
            .insert(change.key.clone(), change.revision);
        match change.operation {
            StateOperation::Put => {
                if let Some(entry) = &change.entry {
                    self.entries.insert(change.key.clone(), entry.clone());
                }
            }
            StateOperation::Delete | StateOperation::Expire => {
                self.entries.remove(&change.key);
            }
        }
        self.generation += 1;
        self.notify(change);
    }

    fn reconcile_snapshot(&mut self, entries: Vec<StateEntry>, prefix: &str) {
        let snapshot = entries
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect::<BTreeMap<_, _>>();
        let existing_keys = self
            .entries
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        for key in existing_keys {
            if snapshot.contains_key(&key) {
                continue;
            }
            let revision = self.revision_by_key.get(&key).copied().unwrap_or(0) + 1;
            self.apply_change(StateChange {
                operation: StateOperation::Delete,
                key,
                revision,
                entry: None,
            });
        }
        for entry in snapshot.into_values() {
            self.apply_change(StateChange {
                operation: StateOperation::Put,
                key: entry.key.clone(),
                revision: entry.revision,
                entry: Some(entry),
            });
        }
    }

    fn notify(&mut self, change: StateChange) {
        self.subscribers
            .retain(|subscriber| subscriber.unbounded_send(Ok(change.clone())).is_ok());
    }

    fn notify_error(&mut self, error: Error) {
        let message = error.to_string();
        self.subscribers.retain(|subscriber| {
            subscriber
                .unbounded_send(Err(Error::StateUnavailable(message.clone())))
                .is_ok()
        });
    }
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
        let removed = inner.entries.remove(key);
        let revision = Self::next_revision(&mut inner);
        if removed.is_some() {
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
        Ok(())
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
