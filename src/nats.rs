use std::collections::BTreeMap;
use std::fmt::Display;
use std::time::Duration;

use async_nats::jetstream::consumer::{push::OrderedConfig, DeliverPolicy, ReplayPolicy};
use async_nats::jetstream::kv::{Config as KvConfig, Entry, Operation, Store, WatcherError};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::{HeaderMap, Message, Subscriber};
use futures_util::future::{select, Either};
use futures_util::pin_mut;
use futures_util::{StreamExt, TryStreamExt};
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
    MaterializedStateStore, StateChange, StateEntry, StateOperation, StateStore, StateStorePolicy,
    StateWatchStream,
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
    ConcordMaintenance,
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
            RuntimeBucket::ConcordMaintenance => self.inner.concord_maintenance(),
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
    concord_maintenance: NatsStateStore,
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
        let client = async_nats::connect(config.nats_url.as_str())
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("connecting to NATS {}: {error}", config.nats_url))
            })?;
        let jetstream = async_nats::jetstream::new(client.clone());
        let beacon_advertisements = open_state_bucket(
            &jetstream,
            &config.beacon_advertisement_bucket,
            beacon_advertisement_store_policy(),
        )
        .await?;
        let concord_contracts = open_state_bucket(
            &jetstream,
            &config.concord_contract_bucket,
            concord_contract_store_policy(),
        )
        .await?;
        let concord_tokens = open_state_bucket(
            &jetstream,
            &config.concord_token_bucket,
            concord_token_store_policy(),
        )
        .await?;
        let concord_maintenance = open_state_bucket(
            &jetstream,
            &config.concord_maintenance_bucket,
            concord_maintenance_store_policy(),
        )
        .await?;
        Ok(Self {
            client,
            beacon_advertisements,
            concord_contracts,
            concord_tokens,
            concord_maintenance,
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

    pub fn concord_maintenance(&self) -> &NatsStateStore {
        &self.concord_maintenance
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
}

impl NatsStateStore {
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
        let subject = format!("{}{}>", self.kv.prefix, prefix);
        let mut consumer = self
            .kv
            .stream
            .create_consumer(OrderedConfig {
                deliver_subject: format!("_INBOX.deckr.{}", Uuid::new_v4().simple()),
                description: Some("deckr prefix state listing".to_string()),
                filter_subject: subject,
                headers_only: true,
                replay_policy: ReplayPolicy::Instant,
                deliver_policy: DeliverPolicy::LastPerSubject,
                ..Default::default()
            })
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("listing state prefix {prefix:?}: {error}"))
            })?;
        let consumer_info = consumer.info().await.map_err(|error| {
            Error::StateUnavailable(format!(
                "reading state prefix {prefix:?} consumer info: {error}"
            ))
        })?;
        if consumer_info.num_pending == 0 {
            return Ok(Vec::new());
        }
        let mut messages = consumer.messages().await.map_err(|error| {
            Error::StateUnavailable(format!("reading state prefix {prefix:?}: {error}"))
        })?;
        let mut entries = Vec::new();
        while let Some(message) = match messages.try_next().await {
            Ok(message) => message,
            Err(error) => {
                return Err(Error::StateUnavailable(format!(
                    "reading state prefix {prefix:?}: {error}"
                )))
            }
        } {
            let info = message.info().map_err(|error| {
                Error::StateUnavailable(format!(
                    "reading state prefix {prefix:?} message metadata: {error}"
                ))
            })?;
            let operation = kv_operation_from_message(&message.message);
            if matches!(operation, Operation::Delete | Operation::Purge) {
                if info.pending == 0 {
                    break;
                }
                continue;
            }
            let Some(key) = message
                .subject
                .strip_prefix(&self.kv.prefix)
                .map(str::to_string)
            else {
                if info.pending == 0 {
                    break;
                }
                continue;
            };
            if !key.starts_with(prefix) {
                if info.pending == 0 {
                    break;
                }
                continue;
            }
            if let Some(entry) = self.get(&key).await? {
                entries.push(entry);
            }
            if info.pending == 0 {
                break;
            }
        }
        Ok(entries)
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
        self.kv
            .delete_expect_revision(key, revision)
            .await
            .map_err(|error| {
                if is_revision_conflict(&error) {
                    Error::StateConflict(format!("state key {key:?} revision changed"))
                } else {
                    Error::StateUnavailable(format!("deleting state key {key}: {error}"))
                }
            })
    }

    async fn watch(&self, prefix: &str) -> Result<StateWatchStream> {
        let watch_key = if prefix.is_empty() {
            ">".to_string()
        } else {
            format!("{prefix}>")
        };
        let watch = self.kv.watch(&watch_key).await.map_err(|error| {
            Error::StateUnavailable(format!("watching state prefix {prefix:?}: {error}"))
        })?;
        Ok(Box::pin(watch.map(map_nats_watch_entry)))
    }
}

fn map_nats_watch_entry(entry: std::result::Result<Entry, WatcherError>) -> Result<StateChange> {
    let entry =
        entry.map_err(|error| Error::StateUnavailable(format!("watching state: {error}")))?;
    let operation = match entry.operation {
        Operation::Put => StateOperation::Put,
        Operation::Delete | Operation::Purge => StateOperation::Delete,
    };
    let state_entry = match entry.operation {
        Operation::Put => Some(StateEntry {
            key: entry.key.clone(),
            value: serde_json::from_slice(&entry.value)?,
            revision: entry.revision,
        }),
        Operation::Delete | Operation::Purge => None,
    };
    Ok(StateChange {
        operation,
        key: entry.key,
        revision: entry.revision,
        entry: state_entry,
    })
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
    bucket: &str,
    policy: StateStorePolicy,
) -> Result<NatsStateStore> {
    match jetstream.get_key_value(bucket).await {
        Ok(store) => {
            validate_bucket(&store, bucket, &policy).await?;
            Ok(NatsStateStore { kv: store, policy })
        }
        Err(get_error) => {
            let created = jetstream
                .create_key_value(kv_config_for_policy(bucket, &policy))
                .await;
            match created {
                Ok(store) => {
                    validate_bucket(&store, bucket, &policy).await?;
                    Ok(NatsStateStore { kv: store, policy })
                }
                Err(create_error) => {
                    let store = jetstream.get_key_value(bucket).await.map_err(|error| {
                        Error::StateUnavailable(format!(
                            "opening NATS KV bucket {bucket}; initial open failed with {get_error}; create failed with {create_error}; final open failed with {error}"
                        ))
                    })?;
                    validate_bucket(&store, bucket, &policy).await?;
                    Ok(NatsStateStore { kv: store, policy })
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
