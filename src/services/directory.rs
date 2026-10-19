//! One shared, indexed feature watch for a declared service protocol.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use futures_util::{FutureExt, StreamExt};
use tokio::task::JoinHandle;

use crate::beacon::{Beacon, BeaconStateWatch};
use crate::lifecycle::{Failure, Phase, Snapshot, StateCell, StateWatch, SystemClock};

use super::*;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ServiceDirectoryState {
    pub source_version: u64,
    pub descriptors: Vec<ServiceDescriptor>,
}

struct Shared<S: StateStore> {
    beacon: Beacon<S>,
    protocol: ServiceProtocol,
    index: Mutex<Index>,
    state: StateCell<ServiceDirectoryState>,
    started: AtomicBool,
    closed: AtomicBool,
}
impl<S: StateStore> Shared<S> {
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        *self.index.lock().expect("service index lock poisoned") = Index::default();
        self.state.modify(|state| {
            state.phase = Phase::Closed;
            state.current = false;
            state.changed = state
                .value
                .descriptors
                .iter()
                .filter_map(|descriptor| {
                    descriptor
                        .candidate
                        .as_ref()
                        .map(|candidate| candidate.key.clone())
                })
                .collect();
            state.value.descriptors.clear();
            true
        });
    }
}
struct Owner<S: StateStore> {
    shared: Arc<Shared<S>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl<S: StateStore> Drop for Owner<S> {
    fn drop(&mut self) {
        self.shared.close();
        if let Some(worker) = self
            .worker
            .get_mut()
            .expect("directory worker lock poisoned")
            .take()
        {
            worker.abort();
        }
    }
}
pub struct ServiceDirectory<S: StateStore> {
    owner: Arc<Owner<S>>,
}
impl<S: StateStore> Clone for ServiceDirectory<S> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}
#[derive(Debug, Default)]
struct Index {
    descriptors_by_key: BTreeMap<String, ServiceDescriptor>,
    by_service_id: BTreeMap<String, BTreeSet<String>>,
    by_namespace: BTreeMap<String, BTreeSet<String>>,
    by_use_profile: BTreeMap<String, BTreeSet<String>>,
    by_operation: BTreeMap<String, BTreeSet<String>>,
    by_view_family: BTreeMap<String, BTreeSet<String>>,
    by_endpoint: BTreeMap<String, BTreeSet<String>>,
    by_session: BTreeMap<(String, String), BTreeSet<String>>,
}
impl Index {
    fn add(&mut self, key: String, descriptor: ServiceDescriptor) {
        index(&mut self.by_service_id, &descriptor.service_id, &key);
        index(&mut self.by_namespace, &descriptor.namespace, &key);
        index(&mut self.by_use_profile, &descriptor.use_profile, &key);
        index(&mut self.by_endpoint, descriptor.endpoint.as_str(), &key);
        index_session(
            &mut self.by_session,
            descriptor.endpoint.as_str(),
            &descriptor.session_id,
            &key,
        );
        for operation in &descriptor.supported_operations {
            index(&mut self.by_operation, operation, &key);
        }
        for family in descriptor.views.keys() {
            index(&mut self.by_view_family, family, &key);
        }
        self.descriptors_by_key.insert(key, descriptor);
    }
}
impl<S: StateStore> ServiceDirectory<S> {
    pub fn new(beacon: Beacon<S>, protocol: ServiceProtocol) -> Result<Self> {
        protocol.validate()?;
        Ok(Self {
            owner: Arc::new(Owner {
                shared: Arc::new(Shared {
                    beacon,
                    protocol,
                    index: Mutex::default(),
                    state: StateCell::new(ServiceDirectoryState::default()),
                    started: AtomicBool::new(false),
                    closed: AtomicBool::new(false),
                }),
                worker: Mutex::default(),
            }),
        })
    }
    pub fn protocol(&self) -> &ServiceProtocol {
        &self.owner.shared.protocol
    }
    pub fn try_start(&self) -> Result<()> {
        let shared = &self.owner.shared;
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("service directory requires a Tokio runtime".into()))?;
        let mut worker = self
            .owner
            .worker
            .lock()
            .expect("directory worker lock poisoned");
        if shared.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed("service directory closed".into()));
        }
        if shared.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        let watch = shared.beacon.watch_state(&shared.protocol.feature_id)?;
        shared.started.store(true, Ordering::SeqCst);
        *worker = Some(tokio::spawn(supervise(shared.clone(), watch)));
        Ok(())
    }
    /// Compatibility entry point; `try_start` reports registration failures.
    pub fn start(&self) {
        if let Err(error) = self.try_start() {
            self.owner.shared.state.modify(|state| {
                state.phase = Phase::Unavailable;
                state.current = false;
                state.failure = Some(Failure::from_error("service directory start", &error));
                true
            });
        }
    }
    /// Compatibility wait for the initial observation (including unavailability).
    pub async fn wait_ready(&self) {
        let _ = self
            .owner
            .shared
            .state
            .wait_for(|state| state.phase != Phase::Starting, None, &SystemClock)
            .await;
    }
    pub fn state(&self) -> Snapshot<ServiceDirectoryState> {
        let mut state = self.owner.shared.state.snapshot();
        let source = self.owner.shared.beacon.materialized_store();
        if !source.is_current() || source.generation() != state.value.source_version {
            state.current = false;
            state.value.descriptors.clear();
            if !matches!(state.phase, Phase::Closed | Phase::Closing) {
                state.phase = Phase::Unavailable;
            }
        }
        state
    }
    pub fn watch_state(&self) -> Result<StateWatch<ServiceDirectoryState>> {
        self.owner.shared.state.watch()
    }
    pub fn is_current(&self) -> bool {
        self.state().current
    }
    pub fn descriptors(&self) -> Vec<ServiceDescriptor> {
        self.state().value.descriptors
    }
    pub fn match_services(&self, query: &ServiceQuery) -> Vec<ServiceDescriptor> {
        let inner = self
            .owner
            .shared
            .index
            .lock()
            .expect("service index lock poisoned");
        if !self.is_current() {
            return Vec::new();
        }
        let mut keys = inner
            .descriptors_by_key
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        intersect_optional(&mut keys, &inner.by_service_id, query.service_id.as_deref());
        intersect_optional(&mut keys, &inner.by_namespace, query.namespace.as_deref());
        intersect_optional(
            &mut keys,
            &inner.by_use_profile,
            query.use_profile.as_deref(),
        );
        if let Some(endpoint) = &query.endpoint {
            intersect_optional(&mut keys, &inner.by_endpoint, Some(endpoint.as_str()));
        }
        if let Some(session) = &query.session_id {
            match &query.endpoint {
                Some(endpoint) => intersect_set(
                    &mut keys,
                    inner
                        .by_session
                        .get(&(endpoint.to_string(), session.clone())),
                ),
                None => keys.retain(|key| {
                    inner
                        .by_session
                        .iter()
                        .any(|((_, id), keys)| id == session && keys.contains(key))
                }),
            }
        }
        for operation in &query.operations {
            intersect_set(&mut keys, inner.by_operation.get(operation));
        }
        for family in &query.views {
            intersect_set(&mut keys, inner.by_view_family.get(family));
        }
        let mut descriptors = keys
            .into_iter()
            .filter_map(|key| inner.descriptors_by_key.get(&key).cloned())
            .collect::<Vec<_>>();
        descriptors.sort_by_key(service_descriptor_sort_key);
        descriptors
    }
    pub fn close(&self) {
        let worker = self
            .owner
            .worker
            .lock()
            .expect("directory worker lock poisoned");
        self.owner.shared.close();
        if let Some(worker) = worker.as_ref() {
            worker.abort();
        }
    }
    pub async fn close_and_wait(&self) {
        self.close();
        let worker = self
            .owner
            .worker
            .lock()
            .expect("directory worker lock poisoned")
            .take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
    }
}
async fn supervise<S: StateStore>(shared: Arc<Shared<S>>, mut watch: BeaconStateWatch) {
    let result = std::panic::AssertUnwindSafe(async {
        while let Some(snapshot) = watch.next().await {
            if snapshot.closed {
                return Err(Error::Closed("service discovery closed".into()));
            }
            let protocol = shared.protocol.clone();
            let current = snapshot.current;
            let replacement = tokio::task::spawn_blocking(move || {
                let mut index = Index::default();
                if current {
                    for candidate in snapshot.candidates {
                        if let Some(descriptor) = parse_service_descriptor(&candidate, &protocol) {
                            index.add(candidate.key, descriptor);
                        }
                    }
                }
                index
            })
            .await
            .map_err(|error| {
                Error::StateUnavailable(format!("service descriptor parser failed: {error}"))
            })?;
            let mut inner = shared.index.lock().expect("service index lock poisoned");
            if shared.closed.load(Ordering::SeqCst) {
                return Ok(());
            }
            let source = shared.beacon.materialized_store();
            if current && (!source.is_current() || source.generation() != snapshot.version) {
                continue;
            }
            let changed = inner
                .descriptors_by_key
                .keys()
                .chain(replacement.descriptors_by_key.keys())
                .filter(|key| {
                    inner.descriptors_by_key.get(*key) != replacement.descriptors_by_key.get(*key)
                })
                .cloned()
                .collect();
            let mut descriptors = replacement
                .descriptors_by_key
                .values()
                .cloned()
                .collect::<Vec<_>>();
            descriptors.sort_by_key(service_descriptor_sort_key);
            *inner = replacement;
            shared.state.publish(
                if current {
                    Phase::Ready
                } else {
                    Phase::Unavailable
                },
                current,
                ServiceDirectoryState {
                    source_version: snapshot.version,
                    descriptors,
                },
                None,
                changed,
            );
        }
        Err(Error::Closed("service discovery ended".into()))
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| {
        Err(Error::StateUnavailable(
            "service directory worker panicked".into(),
        ))
    });
    if let Err(error) = result {
        shared.state.modify(|state| {
            state.failure = Some(Failure::from_error("service directory", &error));
            true
        });
    }
    shared.close();
}
