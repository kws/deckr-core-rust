//! Protected service views over host-owned materialized stores.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use tokio::sync::Notify;

use crate::authority::ContractPointer;
use crate::endpoint::{service_address, EndpointAddress};
use crate::lifecycle::{Cancellation, Clock, Failure, Phase, Snapshot, StateCell, StateWatch};
use crate::schema::CompiledSchema;
use crate::state::{
    MaterializedStateStore, StateEntry, StateSnapshot, StateSnapshotWatch, StateStore,
};
use crate::{Error, Result};

use super::{require_text, ServiceContext, ServiceViewWriter};

const FENCE_FIELDS: [&str; 10] = [
    "viewKey",
    "serviceId",
    "serviceNamespace",
    "serviceEndpoint",
    "serviceSessionId",
    "consumerEndpoint",
    "consumerSessionId",
    "writer",
    "contractId",
    "generation",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceViewRef {
    pub store_name: String,
    pub key: String,
}
impl ServiceViewRef {
    pub fn validate(&self) -> Result<()> {
        require_text(&self.store_name, "service view store name")?;
        require_text(&self.key, "service view key")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceViewFence {
    pub view_key: String,
    pub service_id: String,
    pub service_namespace: String,
    pub service_endpoint: EndpointAddress,
    pub service_session_id: String,
    pub consumer_endpoint: EndpointAddress,
    pub consumer_session_id: String,
    pub writer: ServiceViewWriter,
    pub contract_id: String,
    pub generation: u64,
}
impl ServiceViewFence {
    pub fn validate(&self) -> Result<()> {
        for value in [
            &self.view_key,
            &self.service_id,
            &self.service_namespace,
            &self.service_session_id,
            &self.consumer_session_id,
        ] {
            require_text(value, "service view fence")?;
        }
        EndpointAddress::parse(self.consumer_endpoint.as_str())?;
        if self.service_endpoint != EndpointAddress::parse(service_address(&self.service_id))?
            || self.consumer_endpoint == self.service_endpoint
        {
            return Err(Error::Invalid(
                "service view requires the identified service and a distinct consumer".into(),
            ));
        }
        ContractPointer {
            contract_id: self.contract_id.clone(),
            generation: self.generation,
        }
        .validate()
    }
    pub fn storage_key(&self) -> Result<String> {
        self.validate()?;
        service_view_storage_key(&self.view_key, &self.contract_id, self.generation)
    }
}

pub fn service_view_storage_key(
    view_key: &str,
    contract_id: &str,
    generation: u64,
) -> Result<String> {
    require_text(view_key, "service view key")?;
    ContractPointer {
        contract_id: contract_id.into(),
        generation,
    }
    .validate()?;
    Ok(format!(
        "{view_key}.contract.{}.{generation}",
        crate::keys::encode_key_token(contract_id)
    ))
}

/// The wire object is flat: fence and application fields share its root.
/// Revisions belong to `ProtectedViewEntry` metadata, not this record.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceViewRecord {
    pub fence: ServiceViewFence,
    pub payload: Map<String, Value>,
}
impl ServiceViewRecord {
    pub fn validate(&self) -> Result<()> {
        self.fence.validate()?;
        if FENCE_FIELDS
            .iter()
            .any(|field| self.payload.contains_key(*field))
        {
            return Err(Error::Invalid(
                "service view payload cannot replace fence fields".into(),
            ));
        }
        for value in self.payload.values() {
            crate::json::validate_numbers(value)?;
        }
        Ok(())
    }
    pub fn from_value(value: Value) -> Result<Self> {
        let Value::Object(mut payload) = value else {
            return Err(Error::Invalid(
                "service view record must be an object".into(),
            ));
        };
        let mut fields = Map::new();
        for name in FENCE_FIELDS {
            if let Some(value) = payload.remove(name) {
                fields.insert(name.into(), value);
            }
        }
        let record = Self {
            fence: serde_json::from_value(Value::Object(fields))?,
            payload,
        };
        record.validate()?;
        Ok(record)
    }
    pub fn from_text(text: &str) -> Result<Self> {
        Self::from_value(crate::json::parse(text.as_bytes())?)
    }
    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        let Value::Object(mut value) = serde_json::to_value(&self.fence)? else {
            unreachable!("fence is an object");
        };
        value.extend(self.payload.clone());
        Ok(Value::Object(value))
    }
}
impl Serialize for ServiceViewRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.to_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for ServiceViewRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::from_value(crate::json::deserialize_value(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtectedViewEntry {
    pub reference: ServiceViewRef,
    pub revision: u64,
    pub record: ServiceViewRecord,
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProtectedViewState {
    pub source_version: u64,
    pub source_generation: u64,
    /// Complete authorized membership, indexed by logical viewKey.
    pub entries: BTreeMap<String, ProtectedViewEntry>,
}

/// Named, host-owned source. Cloning and binding do not open native watches.
#[derive(Clone)]
pub struct ServiceViewStore<S: StateStore> {
    name: String,
    source: MaterializedStateStore<S>,
}
impl<S: StateStore> ServiceViewStore<S> {
    pub fn new(name: impl Into<String>, source: MaterializedStateStore<S>) -> Result<Self> {
        let name = name.into();
        require_text(&name, "service view store name")?;
        Ok(Self { name, source })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn source(&self) -> &MaterializedStateStore<S> {
        &self.source
    }
    /// The optional owner schema validates the complete flat record. Bindings
    /// own filtering and public watches, and borrow the store and agreement.
    pub fn bind<C: StateStore, T: StateStore>(
        &self,
        context: ServiceContext<C, T>,
        family: &str,
        schema: Option<CompiledSchema>,
    ) -> Result<ProtectedServiceView<S, C, T>> {
        self.bind_checked(context, family, schema, None)
    }
    pub(crate) fn bind_checked<C: StateStore, T: StateStore>(
        &self,
        context: ServiceContext<C, T>,
        family: &str,
        schema: Option<CompiledSchema>,
        validator: Option<RecordValidator>,
    ) -> Result<ProtectedServiceView<S, C, T>> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("protected views require a Tokio runtime".into()))?;
        context.authorize()?;
        let view =
            context.descriptor().views.get(family).ok_or_else(|| {
                Error::Unauthorized("service view family is not advertised".into())
            })?;
        if view.store_name != self.name || !view.key_prefix.starts_with(self.source.prefix()) {
            return Err(Error::Invalid(
                "service view store or materialized prefix differs from the declared family".into(),
            ));
        }
        let prefixes = context
            .access()
            .allowed_views
            .get(family)
            .cloned()
            .ok_or_else(|| {
                Error::Unauthorized("service view family is outside local access scope".into())
            })?;
        let identity = Identity {
            template: ServiceViewFence {
                view_key: view.key_prefix.clone(),
                service_id: context.descriptor().service_id.clone(),
                service_namespace: context.descriptor().namespace.clone(),
                service_endpoint: context.descriptor().endpoint.clone(),
                service_session_id: context.descriptor().session_id.clone(),
                consumer_endpoint: context.consumer().clone(),
                consumer_session_id: context.consumer_session_id().into(),
                writer: view.writer,
                contract_id: context.agreement().contract().contract_id.clone(),
                generation: context.agreement().contract().generation,
            },
            prefixes,
            store_name: self.name.clone(),
            schema,
            validator,
        };
        let changes = self.source.watch_snapshots(&view.key_prefix)?;
        let authority = context.agreement().watch_state()?;
        let shared = Arc::new(Shared {
            clock: context.agreement().runtime_clock(),
            context,
            family: family.into(),
            source: self.source.clone(),
            identity,
            stop: Cancellation::default(),
            state: StateCell::new(ProtectedViewState::default()),
            changed: Notify::new(),
        });
        tokio::spawn(supervise(shared.clone(), changes, authority));
        Ok(ProtectedServiceView {
            owner: Arc::new(Owner { shared }),
        })
    }
}

pub(crate) type RecordValidator = Arc<dyn Fn(&ServiceViewRecord) -> Result<()> + Send + Sync>;

#[derive(Clone)]
struct Identity {
    template: ServiceViewFence,
    prefixes: Vec<String>,
    store_name: String,
    schema: Option<CompiledSchema>,
    validator: Option<RecordValidator>,
}
impl Identity {
    fn fence(&self, key: &str) -> ServiceViewFence {
        let mut fence = self.template.clone();
        fence.view_key = key.into();
        fence
    }
    fn entry(&self, raw: StateEntry) -> Result<Option<ProtectedViewEntry>> {
        let record = ServiceViewRecord::from_value(raw.value)?;
        if !self
            .prefixes
            .iter()
            .any(|prefix| record.fence.view_key.starts_with(prefix))
            || record.fence != self.fence(&record.fence.view_key)
            || record.fence.storage_key()? != raw.key
        {
            return Ok(None);
        }
        if let Some(schema) = &self.schema {
            schema.validate(&record.to_value()?)?;
        }
        if let Some(validator) = &self.validator {
            validator(&record)?;
        }
        Ok(Some(ProtectedViewEntry {
            reference: ServiceViewRef {
                store_name: self.store_name.clone(),
                key: raw.key,
            },
            revision: raw.revision,
            record,
        }))
    }
}
struct Shared<S: StateStore, C: StateStore, T: StateStore> {
    context: ServiceContext<C, T>,
    family: String,
    source: MaterializedStateStore<S>,
    identity: Identity,
    clock: Arc<dyn Clock>,
    stop: Cancellation,
    state: StateCell<ProtectedViewState>,
    changed: Notify,
}
impl<S: StateStore, C: StateStore, T: StateStore> Shared<S, C, T> {
    fn close(&self) {
        self.stop.cancel();
        self.state.modify(|state| {
            state.phase = Phase::Closing;
            state.current = false;
            state.changed = state.value.entries.keys().cloned().collect();
            state.value.entries.clear();
            true
        });
        self.changed.notify_waiters();
    }
    fn authorize(&self) -> Result<()> {
        if self.stop.is_cancelled() {
            return Err(Error::Closed("protected service view closed".into()));
        }
        self.context.authorize()
    }
    fn checked(&self, mut snapshot: Snapshot<ProtectedViewState>) -> Snapshot<ProtectedViewState> {
        let source = self.source.snapshot();
        let stopped = self.stop.is_cancelled();
        let error = (!stopped).then(|| self.context.authorize().err()).flatten();
        if stopped
            || error.is_some()
            || !source.current
            || snapshot.value.source_generation != source.source_generation
            || snapshot.value.source_version != source.version
        {
            snapshot.current = false;
            snapshot.value.entries.clear();
            if !matches!(snapshot.phase, Phase::Closing | Phase::Closed) {
                snapshot.phase = if stopped || error.is_some() || source.closed {
                    Phase::Closed
                } else {
                    Phase::Unavailable
                };
            }
            if let Some(error) = error {
                snapshot.failure = Some(Failure::from_error("protected view authority", &error));
            }
        }
        snapshot
    }
    async fn cancellable<R>(&self, work: impl Future<Output = Result<R>>) -> Result<R> {
        self.authorize()?;
        tokio::select! { biased;
            _ = self.stop.cancelled() => Err(Error::Closed("protected service view closed".into())),
            result = self.context.while_current(crate::lifecycle::deadline(self.clock.as_ref(),Duration::from_secs(5),"protected service view operation",work)) => result,
        }
    }
    async fn observed(&self, version: u64, generation: u64) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.authorize()?;
            let source = self.source.snapshot();
            if !source.current || source.source_generation != generation {
                return Err(Error::MaterializedViewStale(
                    "protected view source changed during mutation".into(),
                ));
            }
            let state = self.state.snapshot();
            if state.current
                && state.value.source_generation == generation
                && state.value.source_version >= version
            {
                return Ok(());
            }
            changed.await;
        }
    }
}
struct Owner<S: StateStore, C: StateStore, T: StateStore> {
    shared: Arc<Shared<S, C, T>>,
}
impl<S: StateStore, C: StateStore, T: StateStore> Drop for Owner<S, C, T> {
    fn drop(&mut self) {
        self.shared.close();
    }
}

/// One family under one accepted agreement. Closing it revokes its local access
/// and joins private work; the host's source and the agreement remain borrowed.
pub struct ProtectedServiceView<S: StateStore, C: StateStore, T: StateStore> {
    owner: Arc<Owner<S, C, T>>,
}
impl<S: StateStore, C: StateStore, T: StateStore> Clone for ProtectedServiceView<S, C, T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}
impl<S: StateStore, C: StateStore, T: StateStore> ProtectedServiceView<S, C, T> {
    pub fn state(&self) -> Snapshot<ProtectedViewState> {
        self.owner
            .shared
            .checked(self.owner.shared.state.snapshot())
    }
    pub fn watch_state(&self) -> Result<ProtectedViewWatch<S, C, T>> {
        self.owner.shared.authorize()?;
        Ok(ProtectedViewWatch {
            shared: self.owner.shared.clone(),
            watch: self.owner.shared.state.watch()?,
            visible: BTreeMap::new(),
            closed: false,
        })
    }
    pub async fn wait_ready(&self, timeout: Option<Duration>) -> Result<()> {
        let shared = &self.owner.shared;
        let wait = async {
            let mut changes = self.watch_state()?;
            while let Some(snapshot) = changes.next().await {
                if snapshot.current {
                    return Ok(());
                }
                if snapshot.phase == Phase::Closed {
                    return Err(Error::Closed(
                        "protected view closed before readiness".into(),
                    ));
                }
            }
            Err(Error::Closed("protected view watch ended".into()))
        };
        match timeout {
            Some(timeout) => {
                crate::lifecycle::deadline(
                    shared.clock.as_ref(),
                    timeout,
                    "protected view readiness",
                    wait,
                )
                .await
            }
            None => wait.await,
        }
    }
    pub async fn get(&self, view_key: &str) -> Result<Option<ProtectedViewEntry>> {
        let shared = &self.owner.shared;
        shared
            .cancellable(async {
                shared
                    .context
                    .authorize_view(&shared.family, view_key, false)?;
                let key = shared.identity.fence(view_key).storage_key()?;
                loop {
                    let epoch = shared.source.authority_epoch();
                    let Some(raw) = shared.source.get_cached(&key)? else {
                        return Ok(None);
                    };
                    let revision = raw.revision;
                    let identity = shared.identity.clone();
                    let value =
                        tokio::task::spawn_blocking(move || identity.entry(raw).ok().flatten())
                            .await
                            .map_err(|error| {
                                Error::StateUnavailable(format!("view validator failed: {error}"))
                            })?;
                    if epoch != shared.source.authority_epoch() {
                        return Err(Error::MaterializedViewStale(
                            "protected view source changed during validation".into(),
                        ));
                    }
                    if shared
                        .source
                        .get_cached(&key)?
                        .is_some_and(|entry| entry.revision == revision)
                    {
                        return Ok(value);
                    }
                }
            })
            .await
    }
    pub async fn create(&self, view_key: &str, payload: Map<String, Value>) -> Result<u64> {
        self.write(view_key, payload, None).await
    }
    pub async fn update(
        &self,
        view_key: &str,
        payload: Map<String, Value>,
        revision: u64,
    ) -> Result<u64> {
        self.write(view_key, payload, Some(revision)).await
    }
    async fn write(
        &self,
        view_key: &str,
        payload: Map<String, Value>,
        revision: Option<u64>,
    ) -> Result<u64> {
        let shared = &self.owner.shared;
        shared
            .cancellable(async {
                shared
                    .context
                    .authorize_view(&shared.family, view_key, true)?;
                let record = ServiceViewRecord {
                    fence: shared.identity.fence(view_key),
                    payload,
                };
                let key = record.fence.storage_key()?;
                let schema = shared.identity.schema.clone();
                let validator = shared.identity.validator.clone();
                let value = tokio::task::spawn_blocking(move || {
                    let value = record.to_value()?;
                    if let Some(schema) = schema {
                        schema.validate(&value)?;
                    }
                    if let Some(validator) = validator {
                        validator(&record)?;
                    }
                    Ok::<_, Error>(value)
                })
                .await
                .map_err(|error| {
                    Error::StateUnavailable(format!("view validator failed: {error}"))
                })??;
                shared
                    .context
                    .authorize_view(&shared.family, view_key, true)?;
                shared.source.get_cached(&key)?;
                if let Some(revision) = revision {
                    self.check_mutation(&key, view_key, revision)?;
                }
                let epoch = shared.source.authority_epoch();
                let entry = match revision {
                    Some(revision) => shared.source.update(&key, value, revision, None).await?,
                    None => shared.source.create(&key, value, None).await?,
                };
                self.finish_mutation(epoch).await?;
                Ok(entry.revision)
            })
            .await
    }
    fn check_mutation(&self, key: &str, view_key: &str, revision: u64) -> Result<()> {
        if revision == 0 {
            return Err(Error::Invalid("view revision must be positive".into()));
        }
        let shared = &self.owner.shared;
        let raw = shared
            .source
            .get_cached(key)?
            .ok_or_else(|| Error::StateConflict("view record is missing".into()))?;
        if raw.revision != revision {
            return Err(Error::StateConflict("view revision changed".into()));
        }
        let record = ServiceViewRecord::from_value(raw.value)?;
        if record.fence != shared.identity.fence(view_key) {
            return Err(Error::Unauthorized(
                "view replacement belongs to a different fence".into(),
            ));
        }
        Ok(())
    }
    async fn finish_mutation(&self, epoch: Option<u64>) -> Result<()> {
        let shared = &self.owner.shared;
        if epoch != shared.source.authority_epoch() {
            return Err(Error::MaterializedViewStale(
                "protected view source changed during mutation".into(),
            ));
        }
        let target = shared.source.snapshot();
        shared
            .observed(target.version, target.source_generation)
            .await
    }
    pub async fn delete(&self, view_key: &str, revision: u64) -> Result<u64> {
        let shared = &self.owner.shared;
        shared
            .cancellable(async {
                shared
                    .context
                    .authorize_view(&shared.family, view_key, true)?;
                let key = shared.identity.fence(view_key).storage_key()?;
                self.check_mutation(&key, view_key, revision)?;
                let epoch = shared.source.authority_epoch();
                let revision = shared.source.delete_revision(&key, Some(revision)).await?;
                self.finish_mutation(epoch).await?;
                Ok(revision)
            })
            .await
    }
    pub fn request_close(&self) {
        self.owner.shared.close();
    }
    pub async fn close(&self) -> Result<()> {
        self.request_close();
        self.owner.shared.state.wait_closed().await;
        Ok(())
    }
}

/// Coalescing full snapshots. `changed` names only entries visible to this
/// watcher before or after a change; a missing changed key is a removal event.
pub struct ProtectedViewWatch<S: StateStore, C: StateStore, T: StateStore> {
    shared: Arc<Shared<S, C, T>>,
    watch: StateWatch<ProtectedViewState>,
    visible: BTreeMap<String, ProtectedViewEntry>,
    closed: bool,
}
impl<S: StateStore, C: StateStore, T: StateStore> ProtectedViewWatch<S, C, T> {
    pub async fn next(&mut self) -> Option<Snapshot<ProtectedViewState>> {
        if self.closed {
            return None;
        }
        let snapshot = tokio::select! { biased;
            _ = self.shared.stop.cancelled() => { let mut snapshot = self.shared.state.snapshot(); snapshot.phase = Phase::Closed; snapshot.current = false; snapshot.value.entries.clear(); snapshot },
            snapshot = self.watch.next() => snapshot?,
        };
        let mut snapshot = self.shared.checked(snapshot);
        snapshot.changed = changed_entries(&self.visible, &snapshot.value.entries);
        snapshot.resnapshot_required |= snapshot.changed.len() > 256;
        if snapshot.resnapshot_required {
            snapshot.changed.clear();
        }
        self.visible = snapshot.value.entries.clone();
        self.closed = snapshot.phase == Phase::Closed;
        Some(snapshot)
    }
}
fn changed_entries(
    before: &BTreeMap<String, ProtectedViewEntry>,
    after: &BTreeMap<String, ProtectedViewEntry>,
) -> BTreeSet<String> {
    before
        .keys()
        .chain(after.keys())
        .filter(|key| before.get(*key) != after.get(*key))
        .cloned()
        .collect()
}
async fn supervise<S: StateStore, C: StateStore, T: StateStore>(
    shared: Arc<Shared<S, C, T>>,
    changes: StateSnapshotWatch,
    mut authority: StateWatch<crate::concord::AgreementState>,
) {
    let result = std::panic::AssertUnwindSafe(async {
        let authority = async {
            while authority.next().await.is_some() {
                shared.context.authorize()?;
            }
            Err::<(), _>(Error::Closed("service agreement ended".into()))
        };
        tokio::select! { biased;
            _ = shared.stop.cancelled() => Ok(()),
            result = authority => result,
            result = materialize(&shared,changes) => result,
        }
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| {
        Err(Error::StateUnavailable(
            "protected view worker panicked".into(),
        ))
    });
    shared.stop.cancel();
    let before = shared.state.snapshot();
    shared.state.publish(
        Phase::Closed,
        false,
        ProtectedViewState {
            entries: BTreeMap::new(),
            ..before.value.clone()
        },
        result
            .err()
            .map(|error| Failure::from_error("protected view", &error)),
        before.value.entries.keys().cloned().collect(),
    );
    shared.changed.notify_waiters();
}
async fn materialize<S: StateStore, C: StateStore, T: StateStore>(
    shared: &Shared<S, C, T>,
    mut changes: StateSnapshotWatch,
) -> Result<()> {
    while let Some(snapshot) = changes.next().await {
        let StateSnapshot {
            version,
            source_generation,
            current,
            closed,
            entries,
            ..
        } = snapshot;
        if closed {
            return Err(Error::Closed("protected view source closed".into()));
        }
        let values = if current {
            let identity = shared.identity.clone();
            tokio::task::spawn_blocking(move || {
                entries
                    .into_iter()
                    .filter_map(|entry| identity.entry(entry).ok().flatten())
                    .map(|entry| (entry.record.fence.view_key.clone(), entry))
                    .collect::<BTreeMap<_, _>>()
            })
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("protected view validator failed: {error}"))
            })?
        } else {
            BTreeMap::new()
        };
        shared.authorize()?;
        let latest = shared.source.snapshot();
        // Never publish a payload from a superseded observation. The next
        // coalesced source snapshot carries its complete replacement membership.
        if current
            && (!latest.current
                || latest.source_generation != source_generation
                || latest.version != version)
        {
            continue;
        }
        let previous = shared.state.snapshot();
        let changed = changed_entries(&previous.value.entries, &values);
        shared.state.publish(
            if current {
                Phase::Ready
            } else {
                Phase::Unavailable
            },
            current,
            ProtectedViewState {
                source_version: version,
                source_generation,
                entries: values,
            },
            None,
            changed,
        );
        shared.changed.notify_waiters();
    }
    Err(Error::Closed("protected view source watch ended".into()))
}
