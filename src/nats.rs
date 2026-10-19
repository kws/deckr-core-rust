use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use async_nats::jetstream::consumer::{
    push::Config as PushConfig, AckPolicy, DeliverPolicy, ReplayPolicy,
};
use async_nats::jetstream::kv::{Config as KvConfig, Operation, Store};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::{HeaderMap, Message, Subscriber};
use futures_util::future::{select, Either};
use futures_util::pin_mut;
use futures_util::StreamExt;
use serde_json::Value;
use uuid::Uuid;

use crate::beacon::{beacon_advertisement_store_policy, DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME};
use crate::concord::{
    concord_contract_store_policy, concord_maintenance_store_policy, concord_token_store_policy,
    ConcordCoordinator, DEFAULT_CONCORD_CONTRACT_STORE_NAME,
    DEFAULT_CONCORD_MAINTENANCE_STORE_NAME, DEFAULT_CONCORD_TOKEN_STORE_NAME,
};
use crate::endpoint::EndpointAddress;
use crate::lanes::{headers_for, validate_subject_hint, DeckrMessage, HARDWARE_MESSAGES_LANE};
use crate::state::{
    MaterializedStateStore, StateChange, StateEntry, StateOperation, StateSource, StateStore,
    StateStorePolicy, StateWatchStream,
};
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeckrRuntimeConfig {
    pub nats_url: String,
    pub beacon_advertisement_bucket: String,
    pub concord_contract_bucket: String,
    pub concord_token_bucket: String,
    pub concord_maintenance_bucket: String,
}

impl DeckrRuntimeConfig {
    pub fn new(nats_url: impl Into<String>) -> Self {
        Self {
            nats_url: nats_url.into(),
            beacon_advertisement_bucket: DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME.to_string(),
            concord_contract_bucket: DEFAULT_CONCORD_CONTRACT_STORE_NAME.to_string(),
            concord_token_bucket: DEFAULT_CONCORD_TOKEN_STORE_NAME.to_string(),
            concord_maintenance_bucket: DEFAULT_CONCORD_MAINTENANCE_STORE_NAME.to_string(),
        }
    }
}

impl From<&str> for DeckrRuntimeConfig {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for DeckrRuntimeConfig {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeBucket {
    BeaconAdvertisements,
    ConcordContracts,
    ConcordTokens,
}

#[derive(Debug, Clone)]
pub struct DeckrRuntime {
    inner: NatsDeckrRuntime,
}

impl DeckrRuntime {
    pub async fn connect(config: impl Into<DeckrRuntimeConfig>) -> Result<Self> {
        let config = config.into();
        Ok(Self {
            inner: NatsDeckrRuntime::connect_with_config(config).await?,
        })
    }

    pub fn nats(&self) -> &NatsDeckrRuntime {
        &self.inner
    }

    pub fn beacon(&self) -> &NatsStateStore {
        self.inner.beacon_advertisements()
    }

    pub fn concord(&self) -> ConcordCoordinator<NatsStateStore, NatsStateStore> {
        ConcordCoordinator::new(
            self.inner.concord_contracts().clone(),
            self.inner.concord_tokens().clone(),
        )
    }

    pub async fn materialized_concord(
        &self,
    ) -> Result<
        ConcordCoordinator<
            MaterializedStateStore<NatsStateStore>,
            MaterializedStateStore<NatsStateStore>,
        >,
    > {
        Ok(ConcordCoordinator::new(
            MaterializedStateStore::start(
                self.inner.concord_contracts().clone(),
                crate::keys::concord_contracts_prefix(),
            )
            .await?,
            MaterializedStateStore::start(
                self.inner.concord_tokens().clone(),
                crate::keys::concord_contracts_prefix(),
            )
            .await?,
        ))
    }

    pub fn endpoint(
        &self,
        endpoint: EndpointAddress,
        session_id: impl Into<String>,
    ) -> Result<EndpointSession> {
        let session_id = session_id.into();
        if session_id.trim() != session_id || session_id.is_empty() {
            return Err(Error::Invalid(
                "endpoint session id must be non-empty with no leading or trailing whitespace"
                    .to_string(),
            ));
        }
        Ok(EndpointSession {
            runtime: self.inner.clone(),
            endpoint,
            session_id,
        })
    }

    pub fn kv_bucket(&self, bucket: RuntimeBucket) -> &NatsStateStore {
        match bucket {
            RuntimeBucket::BeaconAdvertisements => self.inner.beacon_advertisements(),
            RuntimeBucket::ConcordContracts => self.inner.concord_contracts(),
            RuntimeBucket::ConcordTokens => self.inner.concord_tokens(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EndpointSession {
    runtime: NatsDeckrRuntime,
    endpoint: EndpointAddress,
    session_id: String,
}

impl EndpointSession {
    pub fn endpoint(&self) -> &EndpointAddress {
        &self.endpoint
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub async fn send(&self, message: &DeckrMessage) -> Result<()> {
        if message.sender != self.endpoint.to_string()
            || message.sender_session_id != self.session_id
        {
            return Err(Error::Invalid(
                "endpoint session can only send messages from its endpoint and session".to_string(),
            ));
        }
        self.runtime.publish(message).await
    }

    pub async fn reply_to(&self, request: &DeckrMessage, mut reply: DeckrMessage) -> Result<()> {
        reply.sender = self.endpoint.to_string();
        reply.sender_session_id = self.session_id.clone();
        reply.recipient = crate::lanes::MessageTarget::Endpoint {
            endpoint: request.sender.clone(),
        };
        reply.recipient_session_id = Some(request.sender_session_id.clone());
        reply.in_reply_to = Some(request.message_id.clone());
        reply.contract = request.contract.clone();
        self.send(&reply).await
    }

    pub async fn subscribe_lane(&self, lane: &str) -> Result<EndpointLaneSubscriber> {
        self.runtime
            .subscribe_endpoint_lane(lane, &self.endpoint)
            .await
    }

    pub async fn subscribe_hardware_messages(&self) -> Result<EndpointLaneSubscriber> {
        self.runtime
            .subscribe_endpoint_hardware_messages(&self.endpoint)
            .await
    }
}

#[derive(Debug, Clone)]
pub struct NatsDeckrRuntime {
    client: async_nats::Client,
    beacon_advertisements: NatsStateStore,
    concord_contracts: NatsStateStore,
    concord_tokens: NatsStateStore,
    concord_maintenance_bucket: String,
    connection_epoch: Arc<AtomicU64>,
}

impl NatsDeckrRuntime {
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_config(DeckrRuntimeConfig::new(url)).await
    }

    pub async fn connect_with_buckets(
        url: &str,
        beacon_advertisement_bucket: &str,
        concord_contract_bucket: &str,
        concord_token_bucket: &str,
    ) -> Result<Self> {
        Self::connect_with_config(DeckrRuntimeConfig {
            nats_url: url.to_string(),
            beacon_advertisement_bucket: beacon_advertisement_bucket.to_string(),
            concord_contract_bucket: concord_contract_bucket.to_string(),
            concord_token_bucket: concord_token_bucket.to_string(),
            concord_maintenance_bucket: DEFAULT_CONCORD_MAINTENANCE_STORE_NAME.to_string(),
        })
        .await
    }

    pub async fn connect_with_config(config: DeckrRuntimeConfig) -> Result<Self> {
        let connection_epoch = Arc::new(AtomicU64::new(0));
        let event_epoch = connection_epoch.clone();
        let client = async_nats::ConnectOptions::new()
            .event_callback(move |event| {
                let epoch = event_epoch.clone();
                async move {
                    if matches!(
                        event,
                        async_nats::Event::Disconnected | async_nats::Event::Connected
                    ) {
                        epoch.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
            .connect(config.nats_url.as_str())
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("connecting to NATS {}: {error}", config.nats_url))
            })?;
        let jetstream = async_nats::jetstream::new(client.clone());
        let beacon_advertisements = open_state_bucket(
            &jetstream,
            &client,
            &connection_epoch,
            &config.beacon_advertisement_bucket,
            beacon_advertisement_store_policy(),
        )
        .await?;
        let concord_contracts = open_state_bucket(
            &jetstream,
            &client,
            &connection_epoch,
            &config.concord_contract_bucket,
            concord_contract_store_policy(),
        )
        .await?;
        let concord_tokens = open_state_bucket(
            &jetstream,
            &client,
            &connection_epoch,
            &config.concord_token_bucket,
            concord_token_store_policy(),
        )
        .await?;
        Ok(Self {
            client,
            beacon_advertisements,
            concord_contracts,
            concord_tokens,
            concord_maintenance_bucket: config.concord_maintenance_bucket,
            connection_epoch,
        })
    }

    pub fn beacon_advertisements(&self) -> &NatsStateStore {
        &self.beacon_advertisements
    }

    pub fn concord_contracts(&self) -> &NatsStateStore {
        &self.concord_contracts
    }

    pub fn concord_tokens(&self) -> &NatsStateStore {
        &self.concord_tokens
    }

    /// Explicit optional maintenance capability; ordinary startup never opens it.
    pub async fn concord_maintenance(&self) -> Result<NatsStateStore> {
        open_state_bucket(
            &async_nats::jetstream::new(self.client.clone()),
            &self.client,
            &self.connection_epoch,
            &self.concord_maintenance_bucket,
            concord_maintenance_store_policy(),
        )
        .await
    }

    pub async fn publish(&self, message: &DeckrMessage) -> Result<()> {
        let subject = crate::lanes::subject_for(message)?;
        self.client
            .publish_with_headers(
                subject,
                nats_headers_for(message),
                serde_json::to_vec(message)?.into(),
            )
            .await
            .map_err(|error| Error::StateUnavailable(format!("publishing lane message: {error}")))
    }

    pub async fn subscribe_hardware_messages(&self) -> Result<Subscriber> {
        self.client
            .subscribe(format!(
                "{}.{}.>",
                crate::lanes::LANE_SUBJECT_PREFIX,
                crate::keys::encode_key_token(HARDWARE_MESSAGES_LANE)
            ))
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("subscribing to hardware_messages: {error}"))
            })
    }

    pub async fn subscribe_endpoint_lane(
        &self,
        lane: &str,
        endpoint: &crate::endpoint::EndpointAddress,
    ) -> Result<EndpointLaneSubscriber> {
        let [direct_subject, broadcast_subject] =
            crate::lanes::endpoint_subscription_subjects(lane, endpoint)?;
        let direct = self
            .client
            .subscribe(direct_subject.clone())
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!(
                    "subscribing to direct lane subject {direct_subject}: {error}"
                ))
            })?;
        let broadcast = self
            .client
            .subscribe(broadcast_subject.clone())
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!(
                    "subscribing to broadcast lane subject {broadcast_subject}: {error}"
                ))
            })?;
        Ok(EndpointLaneSubscriber { direct, broadcast })
    }

    pub async fn subscribe_endpoint_hardware_messages(
        &self,
        endpoint: &crate::endpoint::EndpointAddress,
    ) -> Result<EndpointLaneSubscriber> {
        self.subscribe_endpoint_lane(HARDWARE_MESSAGES_LANE, endpoint)
            .await
    }

    pub fn message_from_nats(&self, message: Message) -> Result<DeckrMessage> {
        let envelope = DeckrMessage::from_bytes(&message.payload)?;
        validate_subject_hint(message.subject.as_str(), &envelope)?;
        if let Some(headers) = message.headers.as_ref() {
            validate_nats_headers(headers, &envelope)?;
        }
        Ok(envelope)
    }
}

#[derive(Debug)]
pub struct EndpointLaneSubscriber {
    direct: Subscriber,
    broadcast: Subscriber,
}

impl EndpointLaneSubscriber {
    pub async fn next(&mut self) -> Option<Message> {
        let direct = self.direct.next();
        let broadcast = self.broadcast.next();
        pin_mut!(direct);
        pin_mut!(broadcast);
        match select(direct, broadcast).await {
            Either::Left((message, _)) | Either::Right((message, _)) => message,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NatsStateStore {
    kv: Store,
    policy: StateStorePolicy,
    client: async_nats::Client,
    connection_epoch: Arc<AtomicU64>,
}

impl NatsStateStore {
    async fn open_snapshot(&self, prefix: &str) -> Result<NatsSnapshot> {
        tokio::time::timeout(Duration::from_secs(5), self.open_snapshot_inner(prefix))
            .await
            .map_err(|_| Error::StateUnavailable("snapshot deadline exceeded".into()))?
    }

    async fn open_snapshot_inner(&self, prefix: &str) -> Result<NatsSnapshot> {
        let epoch = self.connection_epoch.load(Ordering::SeqCst);
        let status =
            self.kv.status().await.map_err(|error| {
                Error::StateUnavailable(format!("snapshot high water: {error}"))
            })?;
        let revision = status.info.state.last_sequence;
        let subject = self.client.new_inbox();
        let subscriber = self
            .client
            .subscribe(subject.clone())
            .await
            .map_err(|error| Error::StateUnavailable(format!("snapshot subscription: {error}")))?;
        let name = format!("deckr_{}", Uuid::new_v4().simple());
        // Plain push delivery exposes heartbeats and continuity gaps instead of
        // silently recreating ordered consumers inside the NATS client.
        let mut watch = NatsSourceWatch {
            store: self.clone(),
            subscriber,
            consumer: Some(name.clone()),
            epoch,
            sequence: 0,
            deadline: tokio::time::Instant::now() + Duration::from_secs(3),
        };
        let consumer = self
            .kv
            .stream
            .create_consumer(PushConfig {
                name: Some(name),
                deliver_subject: subject,
                filter_subject: format!("{}{prefix}>", self.kv.prefix),
                ack_policy: AckPolicy::None,
                deliver_policy: DeliverPolicy::LastPerSubject,
                replay_policy: ReplayPolicy::Instant,
                idle_heartbeat: Duration::from_secs(1),
                ..Default::default()
            })
            .await
            .map_err(|error| Error::StateUnavailable(format!("snapshot consumer: {error}")))?;
        let mut entries = BTreeMap::new();
        let mut replayed = Vec::new();
        if consumer.cached_info().num_pending > 0 {
            loop {
                let message = watch.next_message().await?;
                let info = message.info().map_err(|error| {
                    Error::StateUnavailable(format!("snapshot metadata: {error}"))
                })?;
                let pending = info.pending;
                let change = watch.change(&message)?;
                if change.revision > revision {
                    replayed.push(change);
                } else if let Some(entry) = change.entry {
                    entries.insert(change.key, entry);
                } else {
                    entries.remove(&change.key);
                }
                if pending == 0 {
                    break;
                }
            }
        }
        if self.connection_epoch.load(Ordering::SeqCst) != epoch {
            return Err(Error::MaterializedViewStale(
                "connection changed during snapshot".into(),
            ));
        }
        replayed.sort_by_key(|change| change.revision);
        Ok(NatsSnapshot {
            entries: entries.into_values().collect(),
            revision,
            replayed,
            watch,
        })
    }
    pub fn policy(&self) -> &StateStorePolicy {
        &self.policy
    }

    fn validate_ttl(&self, ttl: Option<u64>) -> Result<()> {
        if !self.policy.allow_write_ttl {
            if ttl.is_none() {
                return Ok(());
            }
            return Err(Error::Invalid(format!(
                "NATS {} does not use write TTL",
                self.policy.description
            )));
        }
        if let Some(ttl) = ttl {
            if Some(ttl) != self.policy.broker_ttl_seconds {
                return Err(Error::Invalid(format!(
                    "NATS current state uses broker TTL {:?}; per-key TTL {ttl} is not supported",
                    self.policy.broker_ttl_seconds
                )));
            }
        }
        Ok(())
    }
}

struct NatsSnapshot {
    entries: Vec<StateEntry>,
    revision: u64,
    replayed: Vec<StateChange>,
    watch: NatsSourceWatch,
}

struct NatsSourceWatch {
    store: NatsStateStore,
    subscriber: Subscriber,
    consumer: Option<String>,
    epoch: u64,
    sequence: u64,
    deadline: tokio::time::Instant,
}
impl NatsSourceWatch {
    async fn next_message(&mut self) -> Result<async_nats::jetstream::Message> {
        loop {
            if self.store.connection_epoch.load(Ordering::SeqCst) != self.epoch
                || self.store.client.connection_state() != async_nats::connection::State::Connected
            {
                return Err(Error::MaterializedViewStale(
                    "NATS connection continuity lost".into(),
                ));
            }
            if tokio::time::Instant::now() >= self.deadline {
                return Err(Error::MaterializedViewStale(
                    "NATS watch heartbeat missed".into(),
                ));
            }
            let message =
                match tokio::time::timeout(Duration::from_millis(25), self.subscriber.next()).await
                {
                    Err(_) => continue,
                    Ok(None) => {
                        return Err(Error::MaterializedViewStale("NATS watch ended".into()))
                    }
                    Ok(Some(message)) => message,
                };
            self.deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            if message.status == Some(async_nats::StatusCode::IDLE_HEARTBEAT) {
                if message
                    .headers
                    .as_ref()
                    .and_then(|headers| headers.get("Nats-Last-Consumer"))
                    .and_then(|value| value.as_str().parse::<u64>().ok())
                    .is_some_and(|sequence| sequence != self.sequence)
                {
                    return Err(Error::MaterializedViewStale(
                        "NATS watch sequence gap".into(),
                    ));
                }
                continue;
            }
            if message.status.is_some() {
                return Err(Error::MaterializedViewStale(
                    "NATS consumer status changed".into(),
                ));
            }
            let message = async_nats::jetstream::Message {
                message,
                context: async_nats::jetstream::new(self.store.client.clone()),
            };
            let info = message
                .info()
                .map_err(|error| Error::StateUnavailable(format!("watch metadata: {error}")))?;
            if info.consumer_sequence != self.sequence + 1 {
                return Err(Error::MaterializedViewStale(
                    "NATS watch sequence gap".into(),
                ));
            }
            self.sequence = info.consumer_sequence;
            return Ok(message);
        }
    }
    fn change(&self, message: &async_nats::jetstream::Message) -> Result<StateChange> {
        let revision = message
            .info()
            .map_err(|error| Error::StateUnavailable(format!("watch metadata: {error}")))?
            .stream_sequence;
        let key = message
            .subject
            .strip_prefix(&self.store.kv.prefix)
            .ok_or_else(|| Error::Invalid("watch subject outside bucket".into()))?
            .to_string();
        let operation = if kv_operation_from_message(&message.message) == Operation::Put {
            StateOperation::Put
        } else {
            StateOperation::Delete
        };
        let entry = if operation == StateOperation::Put {
            Some(StateEntry {
                key: key.clone(),
                value: serde_json::from_slice(&message.payload)?,
                revision,
            })
        } else {
            None
        };
        Ok(StateChange {
            operation,
            key,
            revision,
            entry,
        })
    }
    async fn next_change(&mut self) -> Result<StateChange> {
        let message = self.next_message().await?;
        self.change(&message)
    }
    async fn close(&mut self) -> Result<()> {
        if let Some(name) = self.consumer.as_ref() {
            self.store
                .kv
                .stream
                .delete_consumer(name)
                .await
                .map_err(|error| {
                    Error::StateUnavailable(format!("closing watch consumer: {error}"))
                })?;
            self.consumer = None;
        }
        Ok(())
    }
}
impl Drop for NatsSourceWatch {
    fn drop(&mut self) {
        if let Some(name) = self.consumer.take() {
            let stream = self.store.kv.stream.clone();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ =
                        tokio::time::timeout(Duration::from_secs(5), stream.delete_consumer(&name))
                            .await;
                });
            }
        }
    }
}

impl StateStore for NatsStateStore {
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        Ok(self.policy.broker_ttl_seconds)
    }

    async fn get(&self, key: &str) -> Result<Option<StateEntry>> {
        let Some(entry) = self.kv.entry(key.to_string()).await.map_err(|error| {
            Error::StateUnavailable(format!("reading state key {key}: {error}"))
        })?
        else {
            return Ok(None);
        };
        if entry.operation != Operation::Put {
            return Ok(None);
        }
        Ok(Some(StateEntry {
            key: key.to_string(),
            value: serde_json::from_slice(&entry.value)?,
            revision: entry.revision,
        }))
    }

    async fn items(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        let mut snapshot = self.open_snapshot(prefix).await?;
        snapshot.watch.close().await?;
        let mut entries: BTreeMap<_, _> = snapshot
            .entries
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect();
        for change in snapshot.replayed {
            if let Some(entry) = change.entry {
                entries.insert(change.key, entry);
            } else {
                entries.remove(&change.key);
            }
        }
        Ok(entries.into_values().collect())
    }

    async fn open_source(&self, prefix: &str) -> Result<StateSource> {
        let NatsSnapshot {
            entries,
            revision,
            replayed,
            watch,
        } = self.open_snapshot(prefix).await?;
        let changes = Box::pin(futures_util::stream::unfold(
            watch,
            |mut watch| async move {
                let item = watch.next_change().await;
                Some((item, watch))
            },
        ));
        Ok(StateSource {
            entries,
            revision,
            replayed,
            changes,
        })
    }

    async fn put(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        self.validate_ttl(ttl)?;
        let revision = self
            .kv
            .put(key, serde_json::to_vec(&value)?.into())
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("putting state key {key}: {error}"))
            })?;
        Ok(StateEntry {
            key: key.to_string(),
            value,
            revision,
        })
    }

    async fn create(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        self.validate_ttl(ttl)?;
        let result = self
            .kv
            .create(key, serde_json::to_vec(&value)?.into())
            .await;
        let revision = result.map_err(|error| {
            if is_revision_conflict(&error) {
                Error::StateConflict(format!("state key {key:?} already exists"))
            } else {
                Error::StateUnavailable(format!("creating state key {key}: {error}"))
            }
        })?;
        Ok(StateEntry {
            key: key.to_string(),
            value,
            revision,
        })
    }

    async fn update(
        &self,
        key: &str,
        value: Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<StateEntry> {
        self.validate_ttl(ttl)?;
        let result = self
            .kv
            .update(key, serde_json::to_vec(&value)?.into(), revision)
            .await;
        let new_revision = result.map_err(|error| {
            if is_revision_conflict(&error) {
                Error::StateConflict(format!("state key {key:?} revision changed"))
            } else {
                Error::StateUnavailable(format!("updating state key {key}: {error}"))
            }
        })?;
        Ok(StateEntry {
            key: key.to_string(),
            value,
            revision: new_revision,
        })
    }

    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        self.delete_revision(key, revision).await.map(|_| ())
    }

    async fn delete_revision(&self, key: &str, revision: Option<u64>) -> Result<u64> {
        let mut headers = HeaderMap::new();
        headers.insert("KV-Operation", "DEL");
        if let Some(revision) = revision {
            headers.insert("Nats-Expected-Last-Subject-Sequence", revision.to_string());
        }
        let context = async_nats::jetstream::new(self.client.clone());
        let ack = context
            .publish_with_headers(format!("{}{}", self.kv.prefix, key), headers, "".into())
            .await
            .map_err(|error| Error::StateUnavailable(format!("deleting state: {error}")))?
            .await
            .map_err(|error| {
                if is_revision_conflict(&error) {
                    Error::StateConflict(format!("state key {key:?} revision changed"))
                } else {
                    Error::StateUnavailable(format!("deleting state: {error}"))
                }
            })?;
        Ok(ack.sequence)
    }

    async fn watch(&self, prefix: &str) -> Result<StateWatchStream> {
        let source = self.open_source(prefix).await?;
        let initial = source.entries.into_iter().map(|entry| {
            Ok(StateChange {
                operation: StateOperation::Put,
                key: entry.key.clone(),
                revision: entry.revision,
                entry: Some(entry),
            })
        });
        Ok(Box::pin(
            futures_util::stream::iter(initial)
                .chain(futures_util::stream::iter(
                    source.replayed.into_iter().map(Ok),
                ))
                .chain(source.changes),
        ))
    }
}

fn kv_operation_from_message(message: &Message) -> Operation {
    let Some(headers) = &message.headers else {
        return Operation::Put;
    };
    let Some(operation) = headers.get("KV-Operation") else {
        return Operation::Put;
    };
    match operation.as_str() {
        "DEL" => Operation::Delete,
        "PURGE" => Operation::Purge,
        _ => Operation::Put,
    }
}

pub fn nats_headers_for(message: &DeckrMessage) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (key, value) in headers_for(message) {
        headers.insert(key.as_str(), value.as_str());
    }
    headers
}

pub fn validate_nats_headers(headers: &HeaderMap, message: &DeckrMessage) -> Result<()> {
    let mut map = BTreeMap::new();
    for key in [
        "Deckr-Message-Id",
        "Deckr-Message-Type",
        "Deckr-Sender",
        "Deckr-Sender-Session",
        "Deckr-Recipient",
        "Deckr-Recipient-Session",
        "Deckr-In-Reply-To",
    ] {
        if let Some(value) = headers.get(key) {
            map.insert(key.to_string(), value.as_str().to_string());
        }
    }
    crate::lanes::validate_headers(&map, message)
}

async fn open_state_bucket(
    jetstream: &JetStreamContext,
    client: &async_nats::Client,
    connection_epoch: &Arc<AtomicU64>,
    bucket: &str,
    policy: StateStorePolicy,
) -> Result<NatsStateStore> {
    match jetstream.get_key_value(bucket).await {
        Ok(store) => {
            validate_bucket(&store, bucket, &policy).await?;
            Ok(NatsStateStore {
                kv: store,
                policy,
                client: client.clone(),
                connection_epoch: connection_epoch.clone(),
            })
        }
        Err(get_error) => {
            let created = jetstream
                .create_key_value(kv_config_for_policy(bucket, &policy))
                .await;
            match created {
                Ok(store) => {
                    validate_bucket(&store, bucket, &policy).await?;
                    Ok(NatsStateStore {
                        kv: store,
                        policy,
                        client: client.clone(),
                        connection_epoch: connection_epoch.clone(),
                    })
                }
                Err(create_error) => {
                    let store = jetstream.get_key_value(bucket).await.map_err(|error| {
                        Error::StateUnavailable(format!(
                            "opening NATS KV bucket {bucket}; initial open failed with {get_error}; create failed with {create_error}; final open failed with {error}"
                        ))
                    })?;
                    validate_bucket(&store, bucket, &policy).await?;
                    Ok(NatsStateStore {
                        kv: store,
                        policy,
                        client: client.clone(),
                        connection_epoch: connection_epoch.clone(),
                    })
                }
            }
        }
    }
}

async fn validate_bucket(store: &Store, bucket: &str, policy: &StateStorePolicy) -> Result<()> {
    let status = store.status().await.map_err(|error| {
        Error::StateUnavailable(format!("inspecting NATS KV bucket {bucket}: {error}"))
    })?;
    if status.history() != 1 {
        return Err(Error::StateUnavailable(format!(
            "NATS KV bucket {bucket} has history {}; expected 1",
            status.history()
        )));
    }
    let expected_max_age = policy
        .broker_ttl_seconds
        .map(Duration::from_secs)
        .unwrap_or(Duration::ZERO);
    if status.max_age() != expected_max_age {
        return Err(Error::StateUnavailable(format!(
            "NATS KV bucket {bucket} has broker TTL {:?}; expected {:?}",
            status.max_age(),
            expected_max_age
        )));
    }
    let expected_delete_marker_ttl = policy.broker_ttl_seconds.map(Duration::from_secs);
    if status.info.config.subject_delete_marker_ttl != expected_delete_marker_ttl {
        return Err(Error::StateUnavailable(format!(
            "NATS KV bucket {bucket} has subject delete marker TTL {:?}; expected {:?}",
            status.info.config.subject_delete_marker_ttl, expected_delete_marker_ttl
        )));
    }
    Ok(())
}

fn kv_config_for_policy(bucket: &str, policy: &StateStorePolicy) -> KvConfig {
    KvConfig {
        bucket: bucket.to_string(),
        history: 1,
        max_age: policy
            .broker_ttl_seconds
            .map(Duration::from_secs)
            .unwrap_or(Duration::ZERO),
        limit_markers: policy.broker_ttl_seconds.map(Duration::from_secs),
        ..Default::default()
    }
}

fn is_revision_conflict(error: &impl Display) -> bool {
    let message = error.to_string().to_lowercase();
    message.contains("wrong last sequence")
        || message.contains("sequence mismatch")
        || message.contains("wrong last")
        || message.contains("expected")
        || message.contains("already exists")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_config_for_ttl_policy_sets_limit_markers() {
        let policy = StateStorePolicy::ttl(30, "test ttl").unwrap();

        let config = kv_config_for_policy("deckr_beacon_advertisement_v1", &policy);

        assert_eq!(config.bucket, "deckr_beacon_advertisement_v1");
        assert_eq!(config.history, 1);
        assert_eq!(config.max_age, Duration::from_secs(30));
        assert_eq!(config.limit_markers, Some(Duration::from_secs(30)));
    }

    #[test]
    fn kv_config_for_persistent_policy_omits_limit_markers() {
        let policy = StateStorePolicy::persistent("test persistent");

        let config = kv_config_for_policy("deckr_concord_contract_v1", &policy);

        assert_eq!(config.bucket, "deckr_concord_contract_v1");
        assert_eq!(config.history, 1);
        assert_eq!(config.max_age, Duration::ZERO);
        assert_eq!(config.limit_markers, None);
    }
}
