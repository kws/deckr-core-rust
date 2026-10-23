use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::SecondsFormat;
use futures_core::Stream;
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::endpoint::EndpointAddress;
use crate::keys::{
    beacon_advertisement_key as make_beacon_advertisement_key, beacon_feature_prefix,
    parse_beacon_advertisement_key,
};
use crate::state::{
    MaterializedStateStore, StateChange, StateEntry, StateOperation, StateStore, StateStorePolicy,
};
use crate::{Error, Result};

mod managed;
pub use managed::{AdvertisementContent, AdvertisementState, ManagedAdvertisement};

pub const BEACON_ADVERTISEMENT_SCHEMA_ID: &str = "dev.deckr.beacon.advertisement.v1";
pub const DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME: &str = "deckr_beacon_advertisement_v1";
pub const DEFAULT_BEACON_TTL_SECONDS: u64 = 300;
pub const BEACON_ADVERTISEMENT_PREFIX: &str = "advertisements.";

#[derive(Debug, Clone, PartialEq)]
pub struct BeaconSnapshot {
    pub version: u64,
    pub current: bool,
    pub closed: bool,
    pub candidates: Vec<Candidate>,
    pub changed: std::collections::BTreeSet<String>,
    pub resnapshot_required: bool,
}
pub type BeaconStateWatch = Pin<Box<dyn Stream<Item = BeaconSnapshot> + Send>>;

pub fn beacon_advertisement_store_policy() -> StateStorePolicy {
    StateStorePolicy::ttl(DEFAULT_BEACON_TTL_SECONDS, "Beacon advertisement state")
        .expect("default Beacon TTL should be valid")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BeaconProtocol {
    pub namespace: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdvertisementRecord {
    #[serde(default = "advertisement_schema_id", rename = "schema")]
    pub schema_id: String,
    pub advertisement_id: String,
    pub feature_id: String,
    #[serde(deserialize_with = "crate::endpoint::protocol_serde::endpoint")]
    pub advertiser: EndpointAddress,
    #[serde(deserialize_with = "crate::endpoint::protocol_serde::endpoint")]
    pub endpoint: EndpointAddress,
    pub session_id: String,
    pub refresh_seq: u64,
    pub ttl_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<BeaconProtocol>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hints: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl AdvertisementRecord {
    pub fn from_value(value: Value) -> Result<Self> {
        let record: Self = serde_json::from_value(value)?;
        record.validate()?;
        Ok(record)
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(serde_json::to_value(self)?)
    }

    pub fn validate(&self) -> Result<()> {
        require_text(&self.schema_id, "Beacon schema")?;
        if self.schema_id != BEACON_ADVERTISEMENT_SCHEMA_ID {
            return Err(Error::Invalid(format!(
                "Beacon advertisement schema must be {BEACON_ADVERTISEMENT_SCHEMA_ID}"
            )));
        }
        require_text(&self.advertisement_id, "Beacon advertisement identity")?;
        require_text(&self.feature_id, "Beacon advertisement identity")?;
        require_text(&self.session_id, "Beacon advertisement identity")?;
        if self.refresh_seq == 0 {
            return Err(Error::Invalid(
                "refreshSeq must be greater than zero".to_string(),
            ));
        }
        if self.ttl_seconds == 0 {
            return Err(Error::Invalid(
                "ttlSeconds must be greater than zero".to_string(),
            ));
        }
        if let Some(protocol) = &self.protocol {
            require_text(&protocol.namespace, "Beacon protocol namespace")?;
            require_text(&protocol.version, "Beacon protocol version")?;
        }
        for timestamp in [&self.created_at, &self.updated_at].into_iter().flatten() {
            chrono::DateTime::parse_from_rfc3339(timestamp).map_err(|_| {
                Error::Invalid("Beacon timestamp must be an RFC 3339 datetime".into())
            })?;
        }
        for operation in &self.operations {
            require_text(operation, "Beacon operation")?;
        }
        for (key, value) in &self.labels {
            require_text(key, "Beacon label key")?;
            require_text(value, "Beacon label value")?;
        }
        if let Some(payload) = &self.payload {
            if !payload.is_object() {
                return Err(Error::Invalid(
                    "Beacon advertisement payload must be a JSON object".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdvertisementHandle {
    pub key: String,
    pub advertisement_id: String,
    pub feature_id: String,
    pub advertiser: EndpointAddress,
    pub endpoint: EndpointAddress,
    pub session_id: String,
    pub revision: u64,
    pub refresh_seq: u64,
    pub next_refresh_at: Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub key: String,
    pub advertisement: AdvertisementRecord,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeaconFeatureEventType {
    Advertised,
    Updated,
    Withdrawn,
    Expired,
    Invalid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BeaconFeatureEvent {
    pub event_type: BeaconFeatureEventType,
    pub feature_id: String,
    pub key: String,
    pub candidate: Option<Candidate>,
    pub previous: Option<Candidate>,
    pub reason: Option<String>,
}

pub type BeaconFeatureWatchStream =
    Pin<Box<dyn Stream<Item = Result<BeaconFeatureEvent>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct BeaconAdvertiser<S: StateStore> {
    state: S,
    advertisement_id: String,
    feature_id: String,
    advertiser: EndpointAddress,
    endpoint: EndpointAddress,
    session_id: String,
    requested_refresh_interval: Option<Duration>,
    protocol: Option<BeaconProtocol>,
    operations: Vec<String>,
    labels: BTreeMap<String, String>,
    hints: BTreeMap<String, Value>,
    payload: Option<Value>,
    clock: Arc<dyn crate::lifecycle::Clock>,
}

#[derive(Clone)]
pub struct Beacon<S: StateStore> {
    advertisements: MaterializedStateStore<S>,
}

impl<S: StateStore> Beacon<S> {
    pub async fn start(state: S) -> Result<Self> {
        Ok(Self {
            advertisements: MaterializedStateStore::start(state, BEACON_ADVERTISEMENT_PREFIX)
                .await?,
        })
    }

    pub fn materialized_store(&self) -> &MaterializedStateStore<S> {
        &self.advertisements
    }

    pub fn candidate_by_key(&self, key: &str) -> Result<Option<Candidate>> {
        Ok(self
            .advertisements
            .get_cached(key)?
            .and_then(candidate_from_entry))
    }

    pub fn candidates(&self, feature_id: &str) -> Result<Vec<Candidate>> {
        let prefix = beacon_feature_prefix(feature_id);
        sorted_candidates(
            self.advertisements
                .items_cached(&prefix)?
                .into_iter()
                .filter_map(candidate_from_entry)
                .filter(|candidate| candidate.advertisement.feature_id == feature_id),
        )
    }

    pub fn candidates_for_endpoint(
        &self,
        feature_id: &str,
        advertiser: &EndpointAddress,
        endpoint: &EndpointAddress,
    ) -> Result<Vec<Candidate>> {
        sorted_candidates(
            self.candidates(feature_id)?
                .into_iter()
                .filter(|candidate| {
                    &candidate.advertisement.advertiser == advertiser
                        && &candidate.advertisement.endpoint == endpoint
                }),
        )
    }

    pub fn watch(&self, feature_id: &str) -> Result<BeaconFeatureWatchStream> {
        require_text(feature_id, "Beacon feature id")?;
        let changes = crate::state::snapshot_changes(
            self.advertisements
                .watch_snapshots(&beacon_feature_prefix(feature_id))?,
            true,
        );
        let known = BTreeMap::new();
        let feature_id = feature_id.to_string();
        let live = stream::unfold(
            (changes, known, feature_id),
            |(mut changes, mut known, feature_id)| async move {
                loop {
                    let change = changes.next().await?;
                    let item = match change {
                        Ok(change) => {
                            beacon_event_from_state_change(&feature_id, &mut known, change).map(Ok)
                        }
                        Err(error) => Some(Err(error)),
                    };
                    if let Some(item) = item {
                        return Some((item, (changes, known, feature_id)));
                    }
                }
            },
        );
        Ok(Box::pin(live))
    }

    /// Canonical bounded current-state watch. `watch` is the legacy event adapter.
    pub fn watch_state(&self, feature_id: &str) -> Result<BeaconStateWatch> {
        require_text(feature_id, "Beacon feature id")?;
        let feature_id = feature_id.to_string();
        let watch = self
            .advertisements
            .watch_snapshots(&beacon_feature_prefix(&feature_id))?;
        Ok(Box::pin(watch.map(move |snapshot| {
            BeaconSnapshot {
                version: snapshot.version,
                current: snapshot.current,
                closed: snapshot.closed,
                candidates: snapshot
                    .entries
                    .into_iter()
                    .filter_map(candidate_from_entry)
                    .filter(|candidate| candidate.advertisement.feature_id == feature_id)
                    .collect(),
                changed: snapshot.changed,
                resnapshot_required: snapshot.resnapshot_required,
            }
        })))
    }
}

impl<S: StateStore> BeaconAdvertiser<S> {
    pub fn new(
        state: S,
        feature_id: impl Into<String>,
        endpoint: EndpointAddress,
        session_id: impl Into<String>,
    ) -> Self {
        Self {
            state,
            advertisement_id: Uuid::new_v4().to_string(),
            feature_id: feature_id.into(),
            advertiser: endpoint.clone(),
            endpoint,
            session_id: session_id.into(),
            requested_refresh_interval: None,
            protocol: None,
            operations: Vec::new(),
            labels: BTreeMap::new(),
            hints: BTreeMap::new(),
            payload: None,
            clock: Arc::new(crate::lifecycle::SystemClock),
        }
    }

    pub fn clock(mut self, clock: Arc<dyn crate::lifecycle::Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Own publication, TTL-derived renewal, recovery, and guarded withdrawal.
    /// Requires a Tokio runtime. The returned state initially reports starting.
    pub fn manage(self) -> Result<ManagedAdvertisement> {
        ManagedAdvertisement::start(self)
    }

    pub fn advertisement_id(mut self, advertisement_id: impl Into<String>) -> Self {
        self.advertisement_id = advertisement_id.into();
        self
    }

    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn protocol(mut self, protocol: BeaconProtocol) -> Self {
        self.protocol = Some(protocol);
        self
    }

    pub fn operations(mut self, operations: Vec<String>) -> Self {
        self.operations = operations;
        self
    }

    pub fn labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    pub fn hints(mut self, hints: BTreeMap<String, Value>) -> Self {
        self.hints = hints;
        self
    }

    pub fn refresh_interval(mut self, interval: Duration) -> Self {
        assert!(
            !interval.is_zero(),
            "Beacon refresh interval must be greater than zero"
        );
        self.requested_refresh_interval = Some(interval);
        self
    }

    pub async fn publish_or_refresh(
        &self,
        current_handle: Option<&AdvertisementHandle>,
    ) -> Result<AdvertisementHandle> {
        if let Some(handle) = current_handle {
            if let Ok(handle) = self.refresh(handle).await {
                return Ok(handle);
            }
        }
        self.publish().await
    }

    pub async fn publish(&self) -> Result<AdvertisementHandle> {
        let ttl_seconds = self.bucket_ttl_seconds().await?;
        let record = self.record(1, ttl_seconds, None);
        let key = make_beacon_advertisement_key(&record.feature_id, &record.advertisement_id);
        let value = record.to_value()?;
        let entry = match self
            .state
            .create(&key, value.clone(), Some(record.ttl_seconds))
            .await
        {
            Ok(entry) => entry,
            Err(error) if error.is_state_conflict() => {
                let current = self.state.get(&key).await?;
                let Some(current) = current else {
                    let next_refresh_at = self.next_refresh_deadline(record.ttl_seconds);
                    return self
                        .state
                        .create(&key, value, Some(record.ttl_seconds))
                        .await
                        .map(|entry| {
                            advertisement_handle(key, &record, entry.revision, next_refresh_at)
                        });
                };
                let current_record = AdvertisementRecord::from_value(current.value)?;
                if !advertisement_matches(&current_record, &record) {
                    return Err(Error::StateConflict(format!(
                        "Beacon advertisement {key:?} already exists for a different owner"
                    )));
                }
                let refreshed = self.record(
                    next_refresh_seq(current_record.refresh_seq)?,
                    ttl_seconds,
                    current_record.created_at,
                );
                self.state
                    .update(
                        &key,
                        refreshed.to_value()?,
                        current.revision,
                        Some(refreshed.ttl_seconds),
                    )
                    .await?
            }
            Err(error) => return Err(error),
        };
        let record = AdvertisementRecord::from_value(entry.value.clone())?;
        Ok(advertisement_handle(
            key,
            &record,
            entry.revision,
            self.next_refresh_deadline(record.ttl_seconds),
        ))
    }

    pub async fn refresh(&self, handle: &AdvertisementHandle) -> Result<AdvertisementHandle> {
        let ttl_seconds = self.bucket_ttl_seconds().await?;
        let Some(current) = self.state.get(&handle.key).await? else {
            return Err(Error::StateConflict(format!(
                "Beacon advertisement {:?} is missing",
                handle.key
            )));
        };
        let current_record = AdvertisementRecord::from_value(current.value)?;
        if !advertisement_matches_handle(&current_record, handle) {
            return Err(Error::StateConflict(
                "Beacon advertisement changed owner".to_string(),
            ));
        }
        let desired = self.record(1, ttl_seconds, current_record.created_at.clone());
        if advertisement_content_matches(&current_record, &desired)
            && self.clock.now() < handle.next_refresh_at
        {
            return Ok(advertisement_handle(
                handle.key.clone(),
                &current_record,
                current.revision,
                handle.next_refresh_at,
            ));
        }
        let refreshed = self.record(
            next_refresh_seq(current_record.refresh_seq)?,
            ttl_seconds,
            current_record.created_at.or_else(|| Some(self.timestamp())),
        );
        let entry = self
            .state
            .update(
                &handle.key,
                refreshed.to_value()?,
                current.revision,
                Some(refreshed.ttl_seconds),
            )
            .await?;
        Ok(advertisement_handle(
            handle.key.clone(),
            &refreshed,
            entry.revision,
            self.next_refresh_deadline(refreshed.ttl_seconds),
        ))
    }

    pub async fn withdraw(&self, handle: &AdvertisementHandle) -> Result<()> {
        let Some(current) = self.state.get(&handle.key).await? else {
            return Ok(());
        };
        let current_record = AdvertisementRecord::from_value(current.value)?;
        if !advertisement_matches_handle(&current_record, handle) {
            return Err(Error::StateConflict(
                "Beacon advertisement changed owner".to_string(),
            ));
        }
        self.state.delete(&handle.key, Some(current.revision)).await
    }

    pub async fn cleanup_restart(&self, proof: &crate::restart::VerifiedRestart) -> Result<usize> {
        cleanup_restart_advertisements(&self.state, proof, &self.endpoint).await
    }

    async fn bucket_ttl_seconds(&self) -> Result<u64> {
        match self.state.ttl_seconds().await? {
            Some(ttl_seconds) if ttl_seconds > 0 => Ok(ttl_seconds),
            Some(_) => Err(Error::StateUnavailable(
                "Beacon advertisement bucket TTL must be greater than zero".to_string(),
            )),
            None => Err(Error::StateUnavailable(
                "Beacon advertisement bucket must be TTL-bound".to_string(),
            )),
        }
    }

    fn next_refresh_deadline(&self, ttl_seconds: u64) -> Instant {
        let delay = crate::lifecycle::heartbeat_delay(self.clock.as_ref(), ttl_seconds);
        let delay = match self.requested_refresh_interval {
            Some(requested) => {
                delay.max(requested.min(Duration::from_secs_f64(ttl_seconds as f64 * 0.75)))
            }
            None => delay,
        };
        self.clock.now() + delay
    }

    fn timestamp(&self) -> String {
        self.clock
            .utc_now()
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    }

    fn record(
        &self,
        refresh_seq: u64,
        ttl_seconds: u64,
        created_at: Option<String>,
    ) -> AdvertisementRecord {
        let now = self.timestamp();
        AdvertisementRecord {
            schema_id: BEACON_ADVERTISEMENT_SCHEMA_ID.to_string(),
            advertisement_id: self.advertisement_id.clone(),
            feature_id: self.feature_id.clone(),
            advertiser: self.advertiser.clone(),
            endpoint: self.endpoint.clone(),
            session_id: self.session_id.clone(),
            refresh_seq,
            ttl_seconds,
            protocol: self.protocol.clone(),
            operations: self.operations.clone(),
            labels: self.labels.clone(),
            hints: self.hints.clone(),
            payload: self.payload.clone(),
            created_at: created_at.or_else(|| Some(now.clone())),
            updated_at: Some(now),
        }
    }
}

pub fn candidate_from_entry(entry: StateEntry) -> Option<Candidate> {
    let advertisement = AdvertisementRecord::from_value(entry.value).ok()?;
    let (feature_id, advertisement_id) = parse_beacon_advertisement_key(&entry.key)?;
    if feature_id != advertisement.feature_id || advertisement_id != advertisement.advertisement_id
    {
        return None;
    }
    Some(Candidate {
        key: entry.key,
        advertisement,
        revision: entry.revision,
    })
}

fn beacon_event_from_state_change(
    feature_id: &str,
    known: &mut BTreeMap<String, Candidate>,
    change: StateChange,
) -> Option<BeaconFeatureEvent> {
    match change.operation {
        StateOperation::Put => {
            let entry = change.entry?;
            let parsed_key = parse_beacon_advertisement_key(&entry.key);
            let candidate = candidate_from_entry(entry);
            match candidate {
                Some(candidate) if candidate.advertisement.feature_id == feature_id => {
                    let previous = known.insert(candidate.key.clone(), candidate.clone());
                    Some(BeaconFeatureEvent {
                        event_type: if previous.is_some() {
                            BeaconFeatureEventType::Updated
                        } else {
                            BeaconFeatureEventType::Advertised
                        },
                        feature_id: candidate.advertisement.feature_id.clone(),
                        key: candidate.key.clone(),
                        candidate: Some(candidate),
                        previous,
                        reason: None,
                    })
                }
                _ => {
                    let (key_feature, key) = parsed_key?;
                    if key_feature != feature_id {
                        return None;
                    }
                    let previous = known.remove(&change.key);
                    Some(BeaconFeatureEvent {
                        event_type: BeaconFeatureEventType::Invalid,
                        feature_id: key_feature,
                        key: change.key,
                        candidate: None,
                        previous,
                        reason: Some(format!("invalid Beacon advertisement {key:?}")),
                    })
                }
            }
        }
        StateOperation::Delete | StateOperation::Expire => {
            let previous = known.remove(&change.key)?;
            Some(BeaconFeatureEvent {
                event_type: if change.operation == StateOperation::Expire {
                    BeaconFeatureEventType::Expired
                } else {
                    BeaconFeatureEventType::Withdrawn
                },
                feature_id: previous.advertisement.feature_id.clone(),
                key: change.key,
                candidate: None,
                previous: Some(previous),
                reason: Some(
                    if change.operation == StateOperation::Expire {
                        "expire"
                    } else {
                        "delete"
                    }
                    .to_string(),
                ),
            })
        }
    }
}

pub async fn find_candidates<S: StateStore>(state: &S, feature_id: &str) -> Result<Vec<Candidate>> {
    let prefix = beacon_feature_prefix(feature_id);
    sorted_candidates(
        state
            .items(&prefix)
            .await?
            .into_iter()
            .filter_map(candidate_from_entry)
            .filter(|candidate| candidate.advertisement.feature_id == feature_id),
    )
}

fn sorted_candidates(candidates: impl IntoIterator<Item = Candidate>) -> Result<Vec<Candidate>> {
    let mut candidates = candidates.into_iter().collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(candidates)
}

pub fn beacon_advertisement_key(feature_id: &str, advertisement_id: &str) -> String {
    make_beacon_advertisement_key(feature_id, advertisement_id)
}

fn advertisement_schema_id() -> String {
    BEACON_ADVERTISEMENT_SCHEMA_ID.to_string()
}

fn advertisement_handle(
    key: String,
    record: &AdvertisementRecord,
    revision: u64,
    next_refresh_at: Instant,
) -> AdvertisementHandle {
    AdvertisementHandle {
        key,
        advertisement_id: record.advertisement_id.clone(),
        feature_id: record.feature_id.clone(),
        advertiser: record.advertiser.clone(),
        endpoint: record.endpoint.clone(),
        session_id: record.session_id.clone(),
        revision,
        refresh_seq: record.refresh_seq,
        next_refresh_at,
    }
}

fn advertisement_matches(left: &AdvertisementRecord, right: &AdvertisementRecord) -> bool {
    left.advertisement_id == right.advertisement_id
        && left.feature_id == right.feature_id
        && left.advertiser == right.advertiser
        && left.endpoint == right.endpoint
        && left.session_id == right.session_id
}

fn advertisement_matches_handle(
    record: &AdvertisementRecord,
    handle: &AdvertisementHandle,
) -> bool {
    record.advertisement_id == handle.advertisement_id
        && record.feature_id == handle.feature_id
        && record.advertiser == handle.advertiser
        && record.endpoint == handle.endpoint
        && record.session_id == handle.session_id
}

fn advertisement_content_matches(
    current: &AdvertisementRecord,
    desired: &AdvertisementRecord,
) -> bool {
    current.protocol == desired.protocol
        && current.operations == desired.operations
        && current.labels == desired.labels
        && current.hints == desired.hints
        && current.payload == desired.payload
        && current.ttl_seconds == desired.ttl_seconds
}

fn require_text(value: &str, field_name: &str) -> Result<()> {
    if value.trim() != value || value.is_empty() {
        return Err(Error::Invalid(format!(
            "{field_name} must be non-empty with no leading or trailing whitespace"
        )));
    }
    Ok(())
}

fn next_refresh_seq(previous: u64) -> Result<u64> {
    previous
        .checked_add(1)
        .ok_or_else(|| Error::StateConflict("Beacon refresh sequence exhausted".into()))
}

/// Discovery cleanup using confirmed process-exit evidence, never discovery as authority.
pub async fn cleanup_restart_advertisements<S: StateStore>(
    state: &S,
    proof: &crate::restart::VerifiedRestart,
    endpoint: &EndpointAddress,
) -> Result<usize> {
    let mut removed = 0;
    for entry in state.items("advertisements.by_feature.").await? {
        if entry.value.get("endpoint").and_then(Value::as_str) != Some(endpoint.as_str())
            || entry.value.get("advertiser").and_then(Value::as_str) != Some(endpoint.as_str())
            || !entry
                .value
                .get("sessionId")
                .and_then(Value::as_str)
                .is_some_and(|session| proof.contains(endpoint, session))
        {
            continue;
        }
        let record = AdvertisementRecord::from_value(entry.value)?;
        if parse_beacon_advertisement_key(&entry.key)
            != Some((record.feature_id.clone(), record.advertisement_id.clone()))
        {
            continue;
        }
        state.delete(&entry.key, Some(entry.revision)).await?;
        if state.get(&entry.key).await?.is_some() {
            return Err(Error::StateUnavailable(
                "restart advertisement cleanup is not confirmed".into(),
            ));
        }
        removed += 1;
    }
    Ok(removed)
}
