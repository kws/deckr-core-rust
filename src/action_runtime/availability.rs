//! Typed availability over the shared, contract-fenced service view lifetime.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::lifecycle::Snapshot;
use crate::services::{
    ProtectedServiceView, ProtectedViewEntry, ProtectedViewState, ProtectedViewWatch,
    ServiceContext, ServiceViewFence, ServiceViewRecord, ServiceViewRef, ServiceViewStore,
    ServiceViewWriter,
};
use crate::state::StateStore;
use crate::{Error, Result};
use serde_json::{Map, Value};

use super::*;

/// Flat wire record decoded into its authority fence and action payload. Reading
/// a raw record establishes no authority; bind an `ActionAvailability` for that.
#[derive(Debug, Clone, PartialEq)]
pub struct ActionAvailabilityRecord {
    pub fence: ServiceViewFence,
    pub availability: ActionAvailabilityPayload,
}
impl ActionAvailabilityRecord {
    pub fn from_record(record: &ServiceViewRecord) -> Result<Self> {
        record.validate()?;
        let mut payload = record.payload.clone();
        payload.insert(
            "serviceId".into(),
            Value::String(record.fence.service_id.clone()),
        );
        payload.insert(
            "serviceEndpoint".into(),
            Value::String(record.fence.service_endpoint.to_string()),
        );
        payload.insert(
            "serviceSessionId".into(),
            Value::String(record.fence.service_session_id.clone()),
        );
        let value = Self {
            fence: record.fence.clone(),
            availability: ActionAvailabilityPayload::from_value(Value::Object(payload))?,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn from_value(value: Value) -> Result<Self> {
        Self::from_record(&ServiceViewRecord::from_value(value)?)
    }
    pub fn from_text(value: &str) -> Result<Self> {
        Self::from_value(crate::json::parse(value.as_bytes())?)
    }
    pub fn validate(&self) -> Result<()> {
        self.fence.validate()?;
        self.availability.validate()?;
        if self.fence.service_namespace != ACTION_RUNTIME_NAMESPACE
            || self.fence.writer != ServiceViewWriter::Service
            || self.fence.consumer_endpoint.family() != "controller"
            || self.fence.service_id != self.availability.service_id
            || self.fence.service_endpoint != self.availability.service_endpoint
            || self.fence.service_session_id != self.availability.service_session_id
        {
            return Err(Error::Unauthorized(
                "Action Runtime availability has inconsistent service, consumer or writer identity"
                    .into(),
            ));
        }
        ServiceViewRecord {
            fence: self.fence.clone(),
            payload: storage_payload(&self.availability)?,
        }
        .validate()
    }
    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        ServiceViewRecord {
            fence: self.fence.clone(),
            payload: storage_payload(&self.availability)?,
        }
        .to_value()
    }
}
impl serde::Serialize for ActionAvailabilityRecord {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.to_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}
impl<'de> serde::Deserialize<'de> for ActionAvailabilityRecord {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::from_value(crate::json::deserialize_value(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}
fn storage_payload(availability: &ActionAvailabilityPayload) -> Result<Map<String, Value>> {
    let Value::Object(mut value) = availability.to_value()? else {
        unreachable!("availability is an object")
    };
    for name in ["serviceId", "serviceEndpoint", "serviceSessionId"] {
        value.remove(name);
    }
    Ok(value)
}
#[derive(Debug, Clone, PartialEq)]
pub struct ActionAvailabilityEntryView {
    pub reference: ServiceViewRef,
    pub revision: u64,
    pub record: ActionAvailabilityRecord,
}
fn entry(value: ProtectedViewEntry) -> Result<ActionAvailabilityEntryView> {
    Ok(ActionAvailabilityEntryView {
        reference: value.reference,
        revision: value.revision,
        record: ActionAvailabilityRecord::from_record(&value.record)?,
    })
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ActionAvailabilityState {
    pub source_version: u64,
    pub source_generation: u64,
    pub entries: BTreeMap<String, ActionAvailabilityEntryView>,
}
fn snapshot(value: Snapshot<ProtectedViewState>) -> Snapshot<ActionAvailabilityState> {
    Snapshot {
        version: value.version,
        phase: value.phase,
        current: value.current,
        failure: value.failure,
        changed: value.changed,
        resnapshot_required: value.resnapshot_required,
        value: ActionAvailabilityState {
            source_version: value.value.source_version,
            source_generation: value.value.source_generation,
            entries: value
                .value
                .entries
                .into_iter()
                .filter_map(|(key, value)| entry(value).ok().map(|value| (key, value)))
                .collect(),
        },
    }
}

/// Owns a protected-view binding, borrowing the host source and held agreement.
/// Semantic validation runs before payload visibility and before guarded writes.
pub struct ActionAvailability<S: StateStore, C: StateStore, T: StateStore> {
    inner: ProtectedServiceView<S, C, T>,
    service_id: String,
    service_session_id: String,
}
impl<S: StateStore, C: StateStore, T: StateStore> Clone for ActionAvailability<S, C, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            service_id: self.service_id.clone(),
            service_session_id: self.service_session_id.clone(),
        }
    }
}
impl<S: StateStore, C: StateStore, T: StateStore> ActionAvailability<S, C, T> {
    pub fn bind(context: ServiceContext<C, T>, store: &ServiceViewStore<S>) -> Result<Self> {
        context.authorize()?;
        let descriptor = context.descriptor();
        let instance = descriptor
            .service_id
            .strip_prefix("action-runtime.")
            .ok_or_else(|| {
                Error::Invalid("availability requires an Action Runtime service".into())
            })?;
        validate_provider_instance_id(instance)?;
        if descriptor.namespace != ACTION_RUNTIME_NAMESPACE
            || context.declaration().protocol().feature_id != ACTION_RUNTIME_NAMESPACE
            || descriptor.use_profile != ACTION_RUNTIME_USE_PROFILE
            || descriptor.advertisement_profile != ACTION_RUNTIME_ADVERTISEMENT_PROFILE
            || descriptor.endpoint != action_runtime_endpoint(instance)?
            || context.consumer().family() != "controller"
            || descriptor
                .views
                .get(ACTION_AVAILABILITY_FAMILY)
                .is_none_or(|view| {
                    view.store_name != ACTION_AVAILABILITY_STORE
                        || view.writer != ServiceViewWriter::Service
                })
        {
            return Err(Error::Invalid("availability requires the declared Action Runtime profiles and service-written family".into()));
        }
        let service_id = descriptor.service_id.clone();
        let service_session_id = descriptor.session_id.clone();
        let inner = store.bind_checked(
            context,
            ACTION_AVAILABILITY_FAMILY,
            None,
            Some(Arc::new(|record| {
                ActionAvailabilityRecord::from_record(record).map(|_| ())
            })),
        )?;
        Ok(Self {
            inner,
            service_id,
            service_session_id,
        })
    }
    pub fn state(&self) -> Snapshot<ActionAvailabilityState> {
        snapshot(self.inner.state())
    }
    pub fn watch_state(&self) -> Result<ActionAvailabilityWatch<S, C, T>> {
        Ok(ActionAvailabilityWatch {
            inner: self.inner.watch_state()?,
        })
    }
    pub async fn wait_ready(&self, timeout: Option<Duration>) -> Result<()> {
        self.inner.wait_ready(timeout).await
    }
    pub async fn get(&self, key: &str) -> Result<Option<ActionAvailabilityEntryView>> {
        self.inner.get(key).await?.map(entry).transpose()
    }
    fn payload(&self, value: &ActionAvailabilityPayload) -> Result<Map<String, Value>> {
        value.validate()?;
        if value.service_id != self.service_id
            || value.service_session_id != self.service_session_id
        {
            return Err(Error::Unauthorized(
                "availability does not belong to this held runtime session".into(),
            ));
        }
        storage_payload(value)
    }
    pub async fn create(&self, key: &str, value: &ActionAvailabilityPayload) -> Result<u64> {
        self.inner.create(key, self.payload(value)?).await
    }
    pub async fn update(
        &self,
        key: &str,
        value: &ActionAvailabilityPayload,
        revision: u64,
    ) -> Result<u64> {
        self.inner.update(key, self.payload(value)?, revision).await
    }
    pub async fn delete(&self, key: &str, revision: u64) -> Result<u64> {
        self.inner.delete(key, revision).await
    }
    pub fn request_close(&self) {
        self.inner.request_close();
    }
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}
pub struct ActionAvailabilityWatch<S: StateStore, C: StateStore, T: StateStore> {
    inner: ProtectedViewWatch<S, C, T>,
}
impl<S: StateStore, C: StateStore, T: StateStore> ActionAvailabilityWatch<S, C, T> {
    pub async fn next(&mut self) -> Option<Snapshot<ActionAvailabilityState>> {
        self.inner.next().await.map(snapshot)
    }
}
