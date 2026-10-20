use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
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
use crate::lanes::{headers_for, DeckrMessage, HARDWARE_MESSAGES_LANE};
use crate::messaging::{Delivery, DeliveryStream, MessageTransport, ReplyInbox};
use crate::state::{
    MaterializedStateStore, StateChange, StateEntry, StateOperation, StateSource, StateStore,
    StateStorePolicy, StateWatchStream,
};
use crate::{Error, Result};

fn reconnect_delay(maximum: Duration, attempt: usize, sample: f64) -> Duration {
    let cap = maximum.as_secs_f64();
    let window = (cap.min(1.0) * 2_f64.powi(attempt.saturating_sub(1).min(64) as i32)).min(cap);
    Duration::from_secs_f64(window * (0.5 + sample.clamp(0.0, 1.0) * 0.5))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeckrRuntimeConfig {
    pub nats_url: String,
    pub beacon_advertisement_bucket: String,
    pub concord_contract_bucket: String,
    pub concord_token_bucket: String,
    pub concord_maintenance_bucket: String,
    /// Maximum retry interval; also the fixed wait between initial attempts.
    pub reconnect_max_interval: Duration,
    pub connection_timeout: Duration,
}

impl DeckrRuntimeConfig {
    pub fn new(nats_url: impl Into<String>) -> Self {
        Self {
            nats_url: nats_url.into(),
            beacon_advertisement_bucket: DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME.to_string(),
            concord_contract_bucket: DEFAULT_CONCORD_CONTRACT_STORE_NAME.to_string(),
            concord_token_bucket: DEFAULT_CONCORD_TOKEN_STORE_NAME.to_string(),
            concord_maintenance_bucket: DEFAULT_CONCORD_MAINTENANCE_STORE_NAME.to_string(),
            reconnect_max_interval: Duration::from_secs(60),
            connection_timeout: Duration::from_secs(5),
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
        self.inner.materialized_concord().await
    }

    pub async fn protocols(&self) -> Result<NatsProtocolRuntime> {
        self.inner.protocols().await
    }

    /// Explicit exact-store maintenance capability; starts no scans or watches.
    pub async fn maintenance(
        &self,
    ) -> Result<crate::concord::ConcordMaintenance<NatsStateStore, NatsStateStore, NatsStateStore>>
    {
        self.inner.maintenance().await
    }

    pub async fn service_views(
        &self,
        bucket: &str,
        policy: StateStorePolicy,
    ) -> Result<crate::services::ServiceViewStore<NatsStateStore>> {
        self.inner.service_views(bucket, policy).await
    }

    pub fn messaging(
        &self,
        session: crate::session::EndpointSession,
    ) -> Result<crate::messaging::MessageSession<NatsMessageTransport>> {
        crate::messaging::MessageSession::new(self.inner.message_transport(), session)
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

pub type NatsProtocolRuntime =
    crate::runtime::ProtocolRuntime<NatsStateStore, NatsStateStore, NatsStateStore>;

type ServiceViewSources = BTreeMap<
    String,
    (
        StateStorePolicy,
        Arc<tokio::sync::OnceCell<crate::services::ServiceViewStore<NatsStateStore>>>,
    ),
>;

#[derive(Clone)]
pub struct NatsDeckrRuntime {
    client: async_nats::Client,
    beacon_advertisements: NatsStateStore,
    concord_contracts: NatsStateStore,
    concord_tokens: NatsStateStore,
    concord_maintenance_bucket: String,
    connection_epoch: Arc<AtomicU64>,
    beacon_view: Arc<tokio::sync::OnceCell<crate::beacon::Beacon<NatsStateStore>>>,
    concord_view: Arc<
        tokio::sync::OnceCell<crate::runtime::MaterializedConcord<NatsStateStore, NatsStateStore>>,
    >,
    service_views: Arc<Mutex<ServiceViewSources>>,
    source_gate: Arc<tokio::sync::RwLock<()>>,
    source_stop: crate::lifecycle::Cancellation,
}

impl std::fmt::Debug for NatsDeckrRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NatsDeckrRuntime")
            .field("beacon_advertisements", &self.beacon_advertisements)
            .field("concord_contracts", &self.concord_contracts)
            .field("concord_tokens", &self.concord_tokens)
            .field("beacon_started", &self.beacon_view.initialized())
            .field("concord_started", &self.concord_view.initialized())
            .finish_non_exhaustive()
    }
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
            ..DeckrRuntimeConfig::new(url)
        })
        .await
    }

    pub async fn connect_with_config(config: DeckrRuntimeConfig) -> Result<Self> {
        if config.reconnect_max_interval.is_zero() || config.connection_timeout.is_zero() {
            return Err(Error::Invalid(
                "NATS retry interval and dial timeout must be positive".into(),
            ));
        }
        let connection_epoch = Arc::new(AtomicU64::new(0));
        let client = loop {
            let event_epoch = connection_epoch.clone();
            // The callback also runs before the first dial. Initial retries are
            // owned here so dropping the connection future stops all attempts.
            let connected = Arc::new(AtomicBool::new(false));
            let retry_connected = connected.clone();
            let maximum = config.reconnect_max_interval;
            let result = async_nats::ConnectOptions::new()
                .connection_timeout(config.connection_timeout)
                .max_reconnects(None)
                .reconnect_delay_callback(move |attempt| {
                    if !retry_connected.load(Ordering::SeqCst) {
                        return if attempt <= 1 {
                            Duration::ZERO
                        } else {
                            maximum
                        };
                    }
                    // UUID randomness is already available and independent per dial.
                    // UUID's top two bits in this half are fixed variant bits.
                    let random_bits = Uuid::new_v4().as_u128() as u64 & ((1_u64 << 62) - 1);
                    let sample = random_bits as f64 / ((1_u64 << 62) - 1) as f64;
                    reconnect_delay(maximum, attempt, sample)
                })
                .event_callback(move |event| {
                    let epoch = event_epoch.clone();
                    let connected = connected.clone();
                    async move {
                        observe_connection_event(event, &connected, &epoch);
                    }
                })
                .connect(config.nats_url.as_str())
                .await;
            match result {
                Ok(client) => break client,
                Err(error) if error.kind() == async_nats::ConnectErrorKind::ServerParse => {
                    return Err(Error::Invalid(format!("NATS server address: {error}")));
                }
                Err(_) => tokio::time::sleep(config.reconnect_max_interval).await,
            }
        };
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
            beacon_view: Arc::default(),
            concord_view: Arc::default(),
            service_views: Arc::default(),
            source_gate: Arc::default(),
            source_stop: Default::default(),
        })
    }

    /// Lazily start shared sources. Each protocol also has an independent entry
    /// point so one protocol's readiness does not depend on the other.
    pub async fn protocols(&self) -> Result<NatsProtocolRuntime> {
        Ok(crate::runtime::ProtocolRuntime::from_views(
            self.managed_beacon().await?,
            self.materialized_concord().await?,
        ))
    }

    pub async fn managed_beacon(&self) -> Result<crate::beacon::Beacon<NatsStateStore>> {
        self.source_work(async {
            self.beacon_view
                .get_or_try_init(|| {
                    crate::beacon::Beacon::start(self.beacon_advertisements.clone())
                })
                .await
                .cloned()
        })
        .await
    }

    pub async fn materialized_concord(
        &self,
    ) -> Result<crate::runtime::MaterializedConcord<NatsStateStore, NatsStateStore>> {
        self.source_work(async {
            self.concord_view
                .get_or_try_init(|| async {
                    Ok(ConcordCoordinator::new(
                        MaterializedStateStore::start(
                            self.concord_contracts.clone(),
                            crate::keys::concord_contracts_prefix(),
                        )
                        .await?,
                        MaterializedStateStore::start(
                            self.concord_tokens.clone(),
                            crate::keys::concord_contracts_prefix(),
                        )
                        .await?,
                    ))
                })
                .await
                .cloned()
        })
        .await
    }

    /// Open the explicitly named view bucket once per host runtime. Different
    /// families and agreements borrow its one materialized source. Reusing a
    /// name with incompatible TTL policy is an error, not a second consumer.
    pub async fn service_views(
        &self,
        bucket: &str,
        policy: StateStorePolicy,
    ) -> Result<crate::services::ServiceViewStore<NatsStateStore>> {
        if bucket.is_empty()
            || !bucket
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            || policy.broker_ttl_seconds == Some(0)
        {
            return Err(Error::Invalid(
                "invalid service view bucket name or TTL policy".into(),
            ));
        }
        self.source_work(async {
            let cell = {
                let mut sources = self
                    .service_views
                    .lock()
                    .expect("service source lock poisoned");
                let (existing, cell) = sources
                    .entry(bucket.into())
                    .or_insert_with(|| (policy.clone(), Arc::default()));
                if existing.broker_ttl_seconds != policy.broker_ttl_seconds
                    || existing.allow_write_ttl != policy.allow_write_ttl
                {
                    return Err(Error::Invalid(
                        "service view bucket already opened with another TTL policy".into(),
                    ));
                }
                cell.clone()
            };
            cell.get_or_try_init(|| async {
                let store = open_state_bucket(
                    &async_nats::jetstream::new(self.client.clone()),
                    &self.client,
                    &self.connection_epoch,
                    bucket,
                    policy,
                )
                .await?;
                crate::services::ServiceViewStore::new(
                    bucket,
                    MaterializedStateStore::start(store, "").await?,
                )
            })
            .await
            .cloned()
        })
        .await
    }

    async fn source_work<R>(
        &self,
        work: impl std::future::Future<Output = Result<R>>,
    ) -> Result<R> {
        let _gate = self.source_gate.read().await;
        tokio::select! { biased;
            _ = self.source_stop.cancelled() => Err(Error::Closed("runtime sources closed".into())),
            result = work => {
                if self.source_stop.is_cancelled() {
                    self.request_close_sources();
                    Err(Error::Closed("runtime sources closed".into()))
                } else {
                    result
                }
            },
        }
    }

    /// Revoke source access immediately, including pending lazy initializers.
    pub fn request_close_sources(&self) {
        self.source_stop.cancel();
        if let Some(beacon) = self.beacon_view.get() {
            beacon.materialized_store().close();
        }
        if let Some(concord) = self.concord_view.get() {
            concord.close_sources();
        }
        for (_, cell) in self
            .service_views
            .lock()
            .expect("service source lock poisoned")
            .values()
        {
            if let Some(store) = cell.get() {
                store.source().close();
            }
        }
    }

    /// The host calls this after closing its dependent endpoint lifetimes.
    pub async fn close_sources(&self) {
        self.request_close_sources();
        let _gate = self.source_gate.write().await;
        // Initialization can finish immediately before observing cancellation.
        self.request_close_sources();
        if let Some(beacon) = self.beacon_view.get() {
            beacon.materialized_store().close_and_wait().await;
        }
        if let Some(concord) = self.concord_view.get() {
            concord.close_sources_and_wait().await;
        }
        let sources: Vec<_> = self
            .service_views
            .lock()
            .expect("service source lock poisoned")
            .values()
            .filter_map(|(_, cell)| cell.get().cloned())
            .collect();
        for store in sources {
            store.source().close_and_wait().await;
        }
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

    /// Dedicated access to all three raw stores for optional maintenance.
    pub async fn maintenance(
        &self,
    ) -> Result<crate::concord::ConcordMaintenance<NatsStateStore, NatsStateStore, NatsStateStore>>
    {
        Ok(crate::concord::ConcordMaintenance::new(
            self.concord_contracts.clone(),
            self.concord_tokens.clone(),
            self.concord_maintenance().await?,
        ))
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

    pub fn message_transport(&self) -> NatsMessageTransport {
        NatsMessageTransport::new(self.client.clone())
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
        self.message_transport()
            .subscribe_lane(lane, endpoint)
            .await
    }

    pub async fn subscribe_endpoint_hardware_messages(
        &self,
        endpoint: &crate::endpoint::EndpointAddress,
    ) -> Result<EndpointLaneSubscriber> {
        self.subscribe_endpoint_lane(HARDWARE_MESSAGES_LANE, endpoint)
            .await
    }

    pub fn message_from_nats(&self, message: Message) -> Result<DeckrMessage> {
        Ok(decode_delivery(message, false)?.message)
    }
}

#[derive(Debug)]
pub struct EndpointLaneSubscriber {
    direct: Subscriber,
    broadcast: Option<Subscriber>,
}

impl EndpointLaneSubscriber {
    pub async fn next(&mut self) -> Option<Message> {
        let Some(broadcast) = self.broadcast.as_mut() else {
            return self.direct.next().await;
        };
        let direct = self.direct.next();
        let broadcast = broadcast.next();
        pin_mut!(direct);
        pin_mut!(broadcast);
        match select(direct, broadcast).await {
            Either::Left((message, _)) | Either::Right((message, _)) => message,
        }
    }
}

/// Core NATS messaging needs no JetStream or protocol-store privileges. The
/// host may share its client while each managed endpoint owns subscriptions.
#[derive(Clone, Debug)]
pub struct NatsMessageTransport {
    client: async_nats::Client,
}
impl NatsMessageTransport {
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }

    async fn subscribe_lane(
        &self,
        lane: &str,
        endpoint: &EndpointAddress,
    ) -> Result<EndpointLaneSubscriber> {
        let [direct_subject, broadcast_subject] =
            crate::lanes::endpoint_subscription_subjects(lane, endpoint)?;
        let direct = self
            .client
            .subscribe(direct_subject)
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("direct lane subscription: {error}"))
            })?;
        // Core services have no broadcast route. Hardware broadcasts address
        // controllers only; other families must not require those permissions.
        let accepts_broadcast = lane != crate::lanes::SERVICES_LANE
            && (lane != HARDWARE_MESSAGES_LANE
                || endpoint.family() == crate::endpoint::CONTROLLER_FAMILY);
        let broadcast = if accepts_broadcast {
            Some(
                self.client
                    .subscribe(broadcast_subject)
                    .await
                    .map_err(|error| {
                        Error::StateUnavailable(format!("broadcast lane subscription: {error}"))
                    })?,
            )
        } else {
            None
        };
        self.client.flush().await.map_err(|error| {
            Error::StateUnavailable(format!("lane subscription readiness: {error}"))
        })?;
        Ok(EndpointLaneSubscriber { direct, broadcast })
    }
}

fn decode_delivery(message: Message, inbox: bool) -> Result<Delivery> {
    let envelope = DeckrMessage::from_bytes(&message.payload)?;
    if !inbox && message.subject.as_str() != crate::lanes::subject_for(&envelope)? {
        return Err(Error::Invalid(
            "lane subject disagrees with envelope routing".into(),
        ));
    }
    if let Some(headers) = message.headers.as_ref() {
        validate_nats_headers(headers, &envelope)?;
    }
    Ok(Delivery {
        message: envelope,
        reply_address: message.reply.map(|address| address.to_string()),
    })
}

impl MessageTransport for NatsMessageTransport {
    async fn subscribe(&self, lane: &str, endpoint: &EndpointAddress) -> Result<DeliveryStream> {
        let subscriber = self.subscribe_lane(lane, endpoint).await?;
        Ok(Box::pin(futures_util::stream::unfold(
            subscriber,
            |mut subscriber| async move {
                subscriber
                    .next()
                    .await
                    .map(|message| (decode_delivery(message, false), subscriber))
            },
        )))
    }
    async fn open_inbox(&self) -> Result<ReplyInbox> {
        let address = self.client.new_inbox();
        let subscriber = self
            .client
            .subscribe(address.clone())
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("reply inbox subscription: {error}"))
            })?;
        self.client
            .flush()
            .await
            .map_err(|error| Error::StateUnavailable(format!("reply inbox readiness: {error}")))?;
        Ok(ReplyInbox {
            address,
            messages: Box::pin(subscriber.map(|message| decode_delivery(message, true))),
        })
    }
    async fn publish(
        &self,
        message: &DeckrMessage,
        inbox: Option<&str>,
        reply_address: Option<&str>,
    ) -> Result<()> {
        let lane_subject = crate::lanes::subject_for(message)?;
        let subject = inbox.unwrap_or(&lane_subject).to_string();
        let payload = serde_json::to_vec(message)?.into();
        let headers = nats_headers_for(message);
        let result = if let Some(reply) = reply_address {
            self.client
                .publish_with_reply_and_headers(subject, reply.to_string(), headers, payload)
                .await
        } else {
            self.client
                .publish_with_headers(subject, headers, payload)
                .await
        };
        result.map_err(|error| Error::StateUnavailable(format!("publishing message: {error}")))
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
                // Invalid bytes remain an observed record. Null cannot parse as
                // a protocol record, but must not turn into a missing key or a
                // transport discontinuity that revokes unrelated agreements.
                value: crate::json::parse(&message.payload).unwrap_or(Value::Null),
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
            value: crate::json::parse(&entry.value).unwrap_or(Value::Null),
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
                Error::mutation(
                    crate::MutationCode::KeyAlreadyExists,
                    format!("state key {key:?} already exists"),
                )
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
                Error::mutation(
                    crate::MutationCode::RevisionChanged,
                    format!("state key {key:?} revision changed"),
                )
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
                    Error::mutation(
                        crate::MutationCode::RevisionChanged,
                        format!("state key {key:?} revision changed"),
                    )
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
    for key in crate::lanes::ROUTING_HEADERS {
        for value in headers.get_all(*key) {
            if map
                .insert(key.to_string(), value.as_str().to_string())
                .is_some_and(|previous| previous != value.as_str())
            {
                return Err(Error::Invalid(format!(
                    "conflicting values for NATS header {key}"
                )));
            }
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

fn observe_connection_event(event: async_nats::Event, connected: &AtomicBool, epoch: &AtomicU64) {
    let interrupted = match event {
        // The first callback can arrive after connect() returned. It establishes
        // the connection; it must not invalidate snapshots already using it.
        async_nats::Event::Connected => connected.swap(true, Ordering::SeqCst),
        async_nats::Event::Disconnected => true,
        _ => false,
    };
    if interrupted {
        epoch.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delayed_initial_connection_event_preserves_watch_continuity() {
        let connected = AtomicBool::new(false);
        let epoch = AtomicU64::new(0);
        // async-nats dispatches callbacks in a separate task. A snapshot can
        // capture its epoch before the initial Connected callback runs.
        let snapshot_epoch = epoch.load(Ordering::SeqCst);
        observe_connection_event(async_nats::Event::Connected, &connected, &epoch);
        assert!(connected.load(Ordering::SeqCst));
        assert_eq!(epoch.load(Ordering::SeqCst), snapshot_epoch);

        observe_connection_event(async_nats::Event::Disconnected, &connected, &epoch);
        assert_ne!(epoch.load(Ordering::SeqCst), snapshot_epoch);
        let disconnected_epoch = epoch.load(Ordering::SeqCst);
        observe_connection_event(async_nats::Event::Connected, &connected, &epoch);
        assert_ne!(epoch.load(Ordering::SeqCst), disconnected_epoch);
        assert_ne!(epoch.load(Ordering::SeqCst), snapshot_epoch);
    }

    #[test]
    fn reconnect_windows_are_bounded_and_reset_after_success() {
        for cap in [0.25, 5.0, 60.0] {
            let maximum = Duration::from_secs_f64(cap);
            for attempt in 1..140 {
                let window = (cap.min(1.0) * 2_f64.powi((attempt - 1).min(64) as i32)).min(cap);
                assert_eq!(
                    reconnect_delay(maximum, attempt, 0.0),
                    Duration::from_secs_f64(window / 2.0)
                );
                assert_eq!(
                    reconnect_delay(maximum, attempt, 1.0),
                    Duration::from_secs_f64(window)
                );
            }
            assert_eq!(
                reconnect_delay(maximum, 1, 1.0),
                Duration::from_secs_f64(cap.min(1.0))
            );
        }
    }

    #[tokio::test]
    async fn initial_connection_waits_until_cancelled() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut config = DeckrRuntimeConfig::new(format!("nats://{address}"));
        config.reconnect_max_interval = Duration::from_millis(5);
        config.connection_timeout = Duration::from_millis(5);
        // Several refused dials must not end startup; dropping it cancels retries.
        assert!(tokio::time::timeout(
            Duration::from_millis(40),
            NatsDeckrRuntime::connect_with_config(config)
        )
        .await
        .is_err());
    }

    #[test]
    fn lane_and_inbox_decoding_reject_conflicting_or_partial_hints() {
        let envelope = DeckrMessage::from_text(&serde_json::json!({
            "messageId":"id", "createdAt":"2026-01-01T00:00:00Z", "lane":"services", "messageType":"serviceMessage",
            "sender":"service:peer", "senderSessionId":"peer-session", "recipient":{"endpoint":"controller:local"},
            "contract":{"contractId":"exact", "generation":1}, "subject":{"kind":"service"},
            "body":{"serviceNamespace":"org.example.test", "name":"read", "intent":"query", "exchangePattern":"request_reply"}
        }).to_string()).unwrap();
        let raw = Message {
            subject: crate::lanes::subject_for(&envelope).unwrap().into(),
            reply: None,
            payload: serde_json::to_vec(&envelope).unwrap().into(),
            headers: None,
            status: None,
            description: None,
            length: 0,
        };
        assert!(decode_delivery(raw.clone(), false).is_ok());
        let mut inbox = raw.clone();
        inbox.subject = "_INBOX.private".into();
        assert!(decode_delivery(inbox.clone(), false).is_err());
        assert!(decode_delivery(inbox, true).is_ok());
        for case in 0..5 {
            let mut bad = raw.clone();
            let mut headers = HeaderMap::new();
            match case {
                0 => {
                    headers.insert("Deckr-Contract-Id", "exact");
                }
                1 => {
                    headers.insert("Deckr-Contract-Id", "other");
                    headers.insert("Deckr-Contract-Generation", "1");
                }
                2 => {
                    headers.append("Deckr-Message-Id", "id");
                    headers.append("Deckr-Message-Id", "conflicting");
                }
                3 => {
                    headers.insert("Deckr-Recipient-Session", "not-in-envelope");
                }
                _ => {
                    headers.insert("Deckr-In-Reply-To", "not-in-envelope");
                }
            }
            bad.headers = Some(headers);
            assert!(decode_delivery(bad.clone(), false).is_err(), "case {case}");
            bad.subject = "_INBOX.private".into();
            assert!(decode_delivery(bad, true).is_err(), "inbox case {case}");
        }
    }

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
