//! One shared, lazily evaluated index over the two materialized sources.

use super::*;
use crate::state::{StateSnapshot, StateSnapshotWatch};
use futures_core::Stream;
use futures_util::task::AtomicWaker;
use std::{
    pin::Pin,
    sync::{Mutex, Weak},
    task::{Context, Poll},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConcordViewStats {
    pub indexed_contracts: usize,
    pub indexed_tokens: usize,
    pub cached_validities: usize,
    pub subscribers: usize,
    /// Cache misses evaluated since this shared view was initialized.
    pub evaluations: u64,
    /// Candidate keys visited after choosing the smallest applicable index.
    pub candidate_visits: u64,
}
#[derive(Debug, Clone, Default)]
struct Filters {
    profile: Option<String>,
    participant: Option<String>,
    contract_id: Option<String>,
    pointer: Option<ContractPointer>,
    state: Option<ContractState>,
}
impl From<ContractFilters<'_>> for Filters {
    fn from(filter: ContractFilters<'_>) -> Self {
        Self {
            profile: filter.profile.map(str::to_owned),
            participant: filter.participant.map(ToString::to_string),
            contract_id: filter.contract_id.map(str::to_owned),
            pointer: filter.pointer.cloned(),
            state: filter.state,
        }
    }
}
#[derive(Debug, Clone)]
struct Selectors {
    id: String,
    generation: u64,
    profile: Option<String>,
    participants: BTreeSet<String>,
    states: Vec<u8>,
}
impl Selectors {
    fn new(entry: &StateEntry) -> Option<Self> {
        let (id, generation) = parse_concord_contract_key(&entry.key)?;
        Some(Self {
            id,
            generation,
            profile: entry
                .value
                .get("profile")
                .and_then(Value::as_str)
                .map(str::to_owned),
            participants: entry
                .value
                .get("participants")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            states: match entry.value.get("state") {
                None => vec![0],
                Some(Value::String(state)) if state == "open" => vec![0],
                Some(Value::String(state)) if state == "cancelled" => vec![1],
                _ => vec![0, 1],
            },
        })
    }
    fn fallback(&self, entry: &StateEntry) -> ContractHandle {
        ContractHandle {
            key: entry.key.clone(),
            contract_id: self.id.clone(),
            generation: self.generation,
            participants: Vec::new(),
            attached_participants: Vec::new(),
            revision: entry.revision,
            state: if self.states == [1] {
                ContractState::Cancelled
            } else {
                ContractState::Open
            },
            profile: self.profile.clone(),
            terms_hash: None,
        }
    }
}
#[derive(Debug, Clone, Copy)]
struct SourceState {
    version: u64,
    generation: u64,
    current: bool,
    closed: bool,
}
impl From<&StateSnapshot> for SourceState {
    fn from(snapshot: &StateSnapshot) -> Self {
        Self {
            version: snapshot.version,
            generation: snapshot.source_generation,
            current: snapshot.current,
            closed: snapshot.closed,
        }
    }
}
struct Subscription {
    filters: Filters,
    state: Mutex<Pending>,
    waker: AtomicWaker,
}
struct Pending {
    last: ConcordSnapshot,
    latest: Option<ConcordSnapshot>,
    interruption: Option<ConcordSnapshot>,
}
struct CachedValidity {
    member: ConcordViewContract,
    // One most-recent assertion set per contract keeps this cache bounded by
    // selected membership even when callers supply arbitrarily many sessions.
    assertions: Option<(BTreeMap<String, String>, ContractValidity)>,
}
struct Watch {
    subscription: Arc<Subscription>,
    initial: Option<ConcordSnapshot>,
    done: bool,
}
impl Stream for Watch {
    type Item = ConcordSnapshot;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if let Some(initial) = self.initial.take() {
            self.done = initial.closed;
            return Poll::Ready(Some(initial));
        }
        self.subscription.waker.register(cx.waker());
        let next = {
            let mut pending = self
                .subscription
                .state
                .lock()
                .expect("Concord subscription lock poisoned");
            if let Some(interruption) = pending.interruption.take() {
                if pending
                    .latest
                    .as_ref()
                    .is_some_and(|latest| latest.version <= interruption.version)
                {
                    pending.latest.take();
                }
                Some(interruption)
            } else {
                pending.latest.take()
            }
        };
        if let Some(next) = next {
            self.done = next.closed;
            Poll::Ready(Some(next))
        } else {
            Poll::Pending
        }
    }
}
#[derive(Default)]
struct Index {
    contracts: BTreeMap<String, StateEntry>,
    tokens: BTreeMap<String, StateEntry>,
    selectors: BTreeMap<String, Selectors>,
    ids: BTreeMap<String, BTreeSet<String>>,
    profiles: BTreeMap<String, BTreeSet<String>>,
    participants: BTreeMap<String, BTreeSet<String>>,
    states: BTreeMap<u8, BTreeSet<String>>,
    cache: BTreeMap<String, CachedValidity>,
    contract_source: Option<SourceState>,
    token_source: Option<SourceState>,
    subscribers: Vec<Weak<Subscription>>,
    version: u64,
    closed: bool,
    evaluations: u64,
    candidate_visits: u64,
}
impl std::fmt::Debug for SharedView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcordView").finish_non_exhaustive()
    }
}
pub(super) struct SharedView {
    index: Mutex<Index>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Drop for SharedView {
    fn drop(&mut self) {
        self.index
            .lock()
            .expect("Concord index lock poisoned")
            .close();
        if let Some(task) = self
            .task
            .lock()
            .expect("Concord view task lock poisoned")
            .take()
        {
            task.abort();
        }
    }
}
impl SharedView {
    fn start<C: StateStore, T: StateStore>(
        contracts: &MaterializedStateStore<C>,
        tokens: &MaterializedStateStore<T>,
    ) -> Result<Arc<Self>> {
        let cw = contracts.watch_snapshots(concord_contracts_prefix())?;
        let tw = tokens.watch_snapshots(concord_contracts_prefix())?;
        let view = Arc::new(Self {
            index: Mutex::new(Index::default()),
            task: Mutex::new(None),
        });
        view.refresh(contracts, tokens);
        let weak = Arc::downgrade(&view);
        let task = contracts.runtime_handle().spawn(run(weak, cw, tw));
        *view.task.lock().expect("Concord view task lock poisoned") = Some(task);
        Ok(view)
    }
    fn refresh<C: StateStore, T: StateStore>(
        &self,
        contracts: &MaterializedStateStore<C>,
        tokens: &MaterializedStateStore<T>,
    ) {
        let mut index = self.index.lock().expect("Concord index lock poisoned");
        if index.closed {
            return;
        }
        let cs = contracts.snapshot_if_changed(index.contract_source.map(|source| source.version));
        let ts = tokens.snapshot_if_changed(index.token_source.map(|source| source.version));
        index.apply(cs, ts);
    }
    pub(super) fn close(&self) {
        self.index
            .lock()
            .expect("Concord index lock poisoned")
            .close();
        if let Some(task) = self
            .task
            .lock()
            .expect("Concord view task lock poisoned")
            .as_ref()
        {
            task.abort();
        }
    }
    pub(super) async fn close_and_wait(&self) {
        self.close();
        let task = self
            .task
            .lock()
            .expect("Concord view task lock poisoned")
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
    fn watch(&self, filters: ContractFilters<'_>) -> Result<ConcordStateWatch> {
        self.index
            .lock()
            .expect("Concord index lock poisoned")
            .watch(filters)
    }
}
impl Index {
    fn watch(&mut self, filters: ContractFilters<'_>) -> Result<ConcordStateWatch> {
        self.subscribers.retain(|sub| sub.strong_count() > 0);
        if self.subscribers.len() >= 256 {
            return Err(Error::StateUnavailable(
                "Concord watch registration limit (256) reached".into(),
            ));
        }
        let filters = Filters::from(filters);
        let initial = self.snapshot(&filters, None);
        let subscription = Arc::new(Subscription {
            filters,
            state: Mutex::new(Pending {
                last: initial.clone(),
                latest: None,
                interruption: None,
            }),
            waker: AtomicWaker::new(),
        });
        self.subscribers.push(Arc::downgrade(&subscription));
        Ok(Box::pin(Watch {
            subscription,
            initial: Some(initial),
            done: false,
        }))
    }
}

#[derive(Default)]
struct RawSelection {
    contracts: BTreeMap<String, StateEntry>,
    tokens: BTreeMap<String, StateEntry>,
}

/// Legacy event API backed by one filtered shared-view registration. The queue
/// contains at most one diff of selected membership, not an accumulating log.
pub(super) struct NotificationAdapter {
    view: Weak<SharedView>,
    watch: ConcordStateWatch,
    filters: Filters,
    participant: Option<EndpointAddress>,
    initial: bool,
    previous: RawSelection,
    profiles: BTreeMap<(String, u64), Option<String>>,
    queued: std::collections::VecDeque<ConcordContractNotification>,
}
fn changes(
    old: &BTreeMap<String, StateEntry>,
    new: &BTreeMap<String, StateEntry>,
) -> Vec<StateChange> {
    old.keys()
        .chain(new.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| old.get(*key) != new.get(*key))
        .map(|key| StateChange {
            operation: if new.contains_key(key) {
                StateOperation::Put
            } else {
                StateOperation::Delete
            },
            key: key.clone(),
            // Like the materialized-source event adapter, removals retain the
            // last observed revision; mutations must still read exact state.
            revision: new
                .get(key)
                .or_else(|| old.get(key))
                .expect("changed entry")
                .revision,
            entry: new.get(key).cloned(),
        })
        .collect()
}
impl NotificationAdapter {
    pub(super) async fn next(&mut self) -> Result<ConcordContractNotification> {
        loop {
            if let Some(notification) = self.queued.pop_front() {
                return Ok(notification);
            }
            let snapshot = self
                .watch
                .next()
                .await
                .ok_or_else(|| Error::Closed("Concord view closed".into()))?;
            let initial = std::mem::take(&mut self.initial);
            if !snapshot.current {
                return Err(if snapshot.closed {
                    Error::Closed("Concord view closed".into())
                } else {
                    Error::MaterializedViewStale("Concord view stale".into())
                });
            }
            if initial {
                continue;
            }
            let view = self
                .view
                .upgrade()
                .ok_or_else(|| Error::Closed("Concord view closed".into()))?;
            let next = {
                let mut index = view.index.lock().expect("Concord index lock poisoned");
                if !index.current() {
                    return Err(Error::MaterializedViewStale("Concord view stale".into()));
                }
                index.raw_selection(&self.filters)
            };
            // Selection already applied the raw profile/participant indexes.
            // Keep malformed selected records observable even when no parsed
            // handle can be constructed for the first observation.
            for entry in next.contracts.values() {
                if let Some(pointer) = parse_concord_contract_key(&entry.key) {
                    self.profiles.entry(pointer).or_insert_with(|| {
                        entry
                            .value
                            .get("profile")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    });
                }
            }
            for change in changes(&self.previous.contracts, &next.contracts) {
                if let Some(notification) = contract_notification_from_change(
                    change,
                    self.filters.profile.as_deref(),
                    self.participant.as_ref(),
                    &mut self.profiles,
                ) {
                    self.queued.push_back(notification);
                }
            }
            for change in changes(&self.previous.tokens, &next.tokens) {
                if let Some(notification) = token_notification_from_change(
                    change,
                    self.filters.profile.as_deref(),
                    self.participant.as_ref(),
                    &self.profiles,
                ) {
                    self.queued.push_back(notification);
                }
            }
            self.previous = next;
        }
    }
}
async fn run(
    view: Weak<SharedView>,
    mut contracts: StateSnapshotWatch,
    mut tokens: StateSnapshotWatch,
) {
    loop {
        let (contract, snapshot) = tokio::select! {
            snapshot=contracts.next()=>(true,snapshot),
            snapshot=tokens.next()=>(false,snapshot),
        };
        let Some(view) = view.upgrade() else {
            break;
        };
        let mut index = view.index.lock().expect("Concord index lock poisoned");
        let Some(snapshot) = snapshot else {
            index.close();
            break;
        };
        if contract {
            index.apply(Some(snapshot), None);
        } else {
            index.apply(None, Some(snapshot));
        }
        if index.closed {
            break;
        }
    }
}
fn insert<K: Ord>(map: &mut BTreeMap<K, BTreeSet<String>>, selector: K, key: &str) {
    map.entry(selector).or_default().insert(key.into());
}
fn remove<K: Ord>(map: &mut BTreeMap<K, BTreeSet<String>>, selector: &K, key: &str) {
    if let Some(keys) = map.get_mut(selector) {
        keys.remove(key);
        if keys.is_empty() {
            map.remove(selector);
        }
    }
}
impl Index {
    fn raw_selection(&mut self, filters: &Filters) -> RawSelection {
        let mut result = RawSelection::default();
        for key in self.select(filters) {
            result
                .contracts
                .insert(key.clone(), self.contracts[&key].clone());
            let selector = &self.selectors[&key];
            for participant in &selector.participants {
                if let Ok(participant) = EndpointAddress::parse_protocol(participant) {
                    let key = make_concord_participant_token_key(
                        &selector.id,
                        selector.generation,
                        &participant,
                    );
                    if let Some(entry) = self.tokens.get(&key) {
                        result.tokens.insert(key, entry.clone());
                    }
                }
            }
        }
        result
    }
    fn current(&self) -> bool {
        !self.closed
            && self.contract_source.is_some_and(|s| s.current)
            && self.token_source.is_some_and(|s| s.current)
    }
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.version += 1;
            self.publish(None);
        }
    }
    fn apply(&mut self, contracts: Option<StateSnapshot>, tokens: Option<StateSnapshot>) {
        if self.closed {
            return;
        }
        let contracts = contracts.filter(|new| {
            self.contract_source
                .is_none_or(|old| new.version > old.version)
        });
        let tokens = tokens.filter(|new| {
            self.token_source
                .is_none_or(|old| new.version > old.version)
        });
        if contracts.is_none() && tokens.is_none() {
            return;
        }
        let interrupted = contracts
            .as_ref()
            .zip(self.contract_source)
            .is_some_and(|(new, old)| new.source_generation != old.generation)
            || tokens
                .as_ref()
                .zip(self.token_source)
                .is_some_and(|(new, old)| new.source_generation != old.generation);
        if interrupted && self.current() {
            self.version += 1;
            self.publish(Some(false));
        }
        if interrupted {
            self.cache.clear();
        }
        if let Some(snapshot) = contracts {
            let next: BTreeMap<_, _> = snapshot
                .entries
                .into_iter()
                .map(|entry| (entry.key.clone(), entry))
                .collect();
            let changed: BTreeSet<_> = self
                .contracts
                .keys()
                .chain(next.keys())
                .filter(|key| self.contracts.get(*key) != next.get(*key))
                .cloned()
                .collect();
            for key in changed {
                self.cache.remove(&key);
                if let Some(old) = self.selectors.remove(&key) {
                    remove(&mut self.ids, &old.id, &key);
                    if let Some(profile) = old.profile {
                        remove(&mut self.profiles, &profile, &key);
                    }
                    for participant in old.participants {
                        remove(&mut self.participants, &participant, &key);
                    }
                    for state in old.states {
                        remove(&mut self.states, &state, &key);
                    }
                }
                if let Some(selected) = next.get(&key).and_then(Selectors::new) {
                    insert(&mut self.ids, selected.id.clone(), &key);
                    if let Some(profile) = &selected.profile {
                        insert(&mut self.profiles, profile.clone(), &key);
                    }
                    for participant in &selected.participants {
                        insert(&mut self.participants, participant.clone(), &key);
                    }
                    for state in &selected.states {
                        insert(&mut self.states, *state, &key);
                    }
                    self.selectors.insert(key, selected);
                }
            }
            self.contracts = next;
            self.contract_source = Some(SourceState {
                version: snapshot.version,
                generation: snapshot.source_generation,
                current: snapshot.current,
                closed: snapshot.closed,
            });
        }
        if let Some(snapshot) = tokens {
            let next: BTreeMap<_, _> = snapshot
                .entries
                .into_iter()
                .map(|entry| (entry.key.clone(), entry))
                .collect();
            for key in self
                .tokens
                .keys()
                .chain(next.keys())
                .filter(|key| self.tokens.get(*key) != next.get(*key))
            {
                if let Some((id, generation, _)) = parse_concord_participant_token_key(key) {
                    self.cache
                        .remove(&make_concord_contract_key(&id, generation));
                }
            }
            self.tokens = next;
            self.token_source = Some(SourceState {
                version: snapshot.version,
                generation: snapshot.source_generation,
                current: snapshot.current,
                closed: snapshot.closed,
            });
        }
        self.closed = self.contract_source.is_some_and(|s| s.closed)
            || self.token_source.is_some_and(|s| s.closed);
        self.version += 1;
        self.publish(None);
    }
    fn select(&mut self, filters: &Filters) -> Vec<String> {
        let mut sets = Vec::new();
        let empty = BTreeSet::new();
        let pointer_keys = filters.pointer.as_ref().map(|pointer| {
            BTreeSet::from([make_concord_contract_key(
                &pointer.contract_id,
                pointer.generation,
            )])
        });
        if let Some(keys) = &pointer_keys {
            sets.push(keys);
        }
        if let Some(id) = &filters.contract_id {
            sets.push(self.ids.get(id).unwrap_or(&empty));
        }
        if let Some(profile) = &filters.profile {
            sets.push(self.profiles.get(profile).unwrap_or(&empty));
        }
        if let Some(participant) = &filters.participant {
            sets.push(self.participants.get(participant).unwrap_or(&empty));
        }
        if let Some(state) = filters.state {
            sets.push(
                self.states
                    .get(&if state == ContractState::Open { 0 } else { 1 })
                    .unwrap_or(&empty),
            );
        }
        if let Some(smallest) = sets.iter().min_by_key(|keys| keys.len()) {
            self.candidate_visits = self.candidate_visits.saturating_add(smallest.len() as u64);
            smallest
                .iter()
                .filter(|key| {
                    self.selectors.contains_key(*key) && sets.iter().all(|set| set.contains(*key))
                })
                .cloned()
                .collect()
        } else {
            self.candidate_visits = self
                .candidate_visits
                .saturating_add(self.selectors.len() as u64);
            self.selectors.keys().cloned().collect()
        }
    }
    fn observations(&self, key: &str) -> (Option<StateEntry>, BTreeMap<String, StateEntry>) {
        let entry = self.contracts.get(key).cloned();
        let mut tokens = BTreeMap::new();
        if let Some(record) = entry
            .as_ref()
            .and_then(|entry| ContractRecord::from_value(entry.value.clone()).ok())
        {
            for participant in record.participants {
                let key = make_concord_participant_token_key(
                    &record.contract_id,
                    record.generation,
                    &participant,
                );
                if let Some(entry) = self.tokens.get(&key) {
                    tokens.insert(key, entry.clone());
                }
            }
        }
        (entry, tokens)
    }
    fn member(&mut self, key: &str) -> ConcordViewContract {
        if let Some(member) = self.cache.get(key) {
            return member.member.clone();
        }
        let entry = &self.contracts[key];
        let handle = contract_handle_from_entry(entry.clone())
            .unwrap_or_else(|| self.selectors[key].fallback(entry));
        if !self.current() {
            return ConcordViewContract {
                contract: handle,
                validity: unavailable(None),
            };
        }
        let (entry, tokens) = self.observations(key);
        let member = ConcordViewContract {
            validity: evaluate_observations(&handle, entry, &tokens, None),
            contract: handle,
        };
        self.evaluations = self.evaluations.saturating_add(1);
        self.cache.insert(
            key.into(),
            CachedValidity {
                member: member.clone(),
                assertions: None,
            },
        );
        member
    }
    fn snapshot(&mut self, filters: &Filters, current: Option<bool>) -> ConcordSnapshot {
        let current = current.unwrap_or_else(|| self.current());
        let contracts = self
            .select(filters)
            .into_iter()
            .map(|key| {
                let mut member = self.member(&key);
                if !current {
                    member.validity = unavailable(None);
                }
                member
            })
            .collect();
        ConcordSnapshot {
            version: self.version,
            current,
            closed: self.closed,
            contracts,
            changed: Vec::new(),
            resnapshot_required: false,
        }
    }
    fn publish(&mut self, current: Option<bool>) {
        self.subscribers.retain(|sub| sub.strong_count() > 0);
        let subscribers: Vec<_> = self.subscribers.iter().filter_map(Weak::upgrade).collect();
        for sub in subscribers {
            let mut next = self.snapshot(&sub.filters, current);
            let mut pending = sub
                .state
                .lock()
                .expect("Concord subscription lock poisoned");
            if next.contracts == pending.last.contracts
                && next.current == pending.last.current
                && next.closed == pending.last.closed
            {
                continue;
            }
            let old: BTreeMap<_, _> = pending
                .last
                .contracts
                .iter()
                .map(|member| (&member.contract.key, member))
                .collect();
            let new: BTreeMap<_, _> = next
                .contracts
                .iter()
                .map(|member| (&member.contract.key, member))
                .collect();
            let mut changed: BTreeMap<_, _> = old
                .keys()
                .chain(new.keys())
                .filter(|key| old.get(*key) != new.get(*key))
                .filter_map(|key| new.get(key).or_else(|| old.get(key)))
                .map(|member| {
                    (
                        member.contract.key.clone(),
                        ContractPointer {
                            contract_id: member.contract.contract_id.clone(),
                            generation: member.contract.generation,
                        },
                    )
                })
                .collect();
            if let Some(previous) = &pending.latest {
                next.resnapshot_required |= previous.resnapshot_required;
                for pointer in &previous.changed {
                    changed.insert(
                        make_concord_contract_key(&pointer.contract_id, pointer.generation),
                        pointer.clone(),
                    );
                }
            }
            next.resnapshot_required |= changed.len() > 256;
            if !next.resnapshot_required {
                next.changed = changed.into_values().collect();
            }
            pending.last = next.clone();
            if !next.current {
                pending.interruption = Some(next.clone());
            }
            pending.latest = Some(next);
            sub.waker.wake();
        }
    }
}
impl<C: StateStore, T: StateStore>
    ConcordCoordinator<MaterializedStateStore<C>, MaterializedStateStore<T>>
{
    fn shared_view(&self) -> Result<Arc<SharedView>> {
        let mut slot = self
            .view
            .lock()
            .expect("Concord view initialization lock poisoned");
        if slot.is_none() {
            *slot = Some(SharedView::start(&self.contract_state, &self.token_state)?);
        }
        let view = slot.as_ref().expect("initialized Concord view").clone();
        view.refresh(&self.contract_state, &self.token_state);
        Ok(view)
    }
    pub(super) fn indexed_watch(&self, filters: ContractFilters<'_>) -> Result<ConcordStateWatch> {
        self.shared_view()?.watch(filters)
    }
    pub(super) fn indexed_notifications(
        &self,
        filters: ContractFilters<'_>,
    ) -> Result<ConcordContractNotificationStream> {
        let view = self.shared_view()?;
        let mut index = view.index.lock().expect("Concord index lock poisoned");
        let watch = index.watch(filters)?;
        let owned = Filters::from(filters);
        let previous = index.raw_selection(&owned);
        let profiles = previous
            .contracts
            .keys()
            .map(|key| {
                let selector = &index.selectors[key];
                (
                    (selector.id.clone(), selector.generation),
                    selector.profile.clone(),
                )
            })
            .collect();
        Ok(ConcordContractNotificationStream {
            inner: NotificationStream::Cached(NotificationAdapter {
                view: Arc::downgrade(&view),
                watch,
                filters: owned,
                participant: filters.participant.cloned(),
                initial: true,
                previous,
                profiles,
                queued: Default::default(),
            }),
        })
    }
    pub(super) fn indexed_contracts(
        &self,
        filters: ContractFilters<'_>,
    ) -> Result<Vec<ContractHandle>> {
        let view = self.shared_view()?;
        let mut index = view.index.lock().expect("Concord index lock poisoned");
        if !index.current() {
            return Err(if index.closed {
                Error::Closed("Concord view closed".into())
            } else {
                Error::MaterializedViewStale("Concord view stale".into())
            });
        }
        Ok(index
            .select(&Filters::from(filters))
            .into_iter()
            .filter_map(|key| {
                let member = index.member(&key);
                member
                    .validity
                    .contract
                    .filter(|record| {
                        make_concord_contract_key(&record.contract_id, record.generation) == key
                            && record.contract_id == member.contract.contract_id
                            && record.generation == member.contract.generation
                    })
                    .map(|_| member.contract)
            })
            .collect())
    }
    /// Diagnostics for the shared index, independent of agreement authority.
    pub fn view_stats(&self) -> Result<ConcordViewStats> {
        let view = self.shared_view()?;
        let mut index = view.index.lock().expect("Concord index lock poisoned");
        index.subscribers.retain(|sub| sub.strong_count() > 0);
        Ok(ConcordViewStats {
            indexed_contracts: index.selectors.len(),
            indexed_tokens: index.tokens.len(),
            cached_validities: index.cache.len(),
            subscribers: index.subscribers.len(),
            evaluations: index.evaluations,
            candidate_visits: index.candidate_visits,
        })
    }
    pub(super) fn indexed_validity(
        &self,
        contract: &ContractHandle,
        sessions: Option<&BTreeMap<String, String>>,
    ) -> ContractValidity {
        let Some(epoch) = self.authority_epochs() else {
            return unavailable(None);
        };
        let Ok(view) = self.shared_view() else {
            return unavailable(None);
        };
        let mut index = view.index.lock().expect("Concord index lock poisoned");
        if !index.current() {
            return unavailable(None);
        }
        let cacheable = contract.key
            == make_concord_contract_key(&contract.contract_id, contract.generation)
            && index.selectors.contains_key(&contract.key);
        let result = if cacheable && sessions.is_none_or(BTreeMap::is_empty) {
            index.member(&contract.key).validity
        } else {
            if cacheable {
                index.member(&contract.key);
                if let Some((assertions, validity)) = index
                    .cache
                    .get(&contract.key)
                    .and_then(|cached| cached.assertions.as_ref())
                {
                    if Some(assertions) == sessions && self.authority_epochs() == Some(epoch) {
                        return validity.clone();
                    }
                }
            }
            let (entry, tokens) = index.observations(&contract.key);
            index.evaluations = index.evaluations.saturating_add(1);
            let validity = evaluate_observations(contract, entry, &tokens, sessions);
            if cacheable {
                if let (Some(cached), Some(sessions)) =
                    (index.cache.get_mut(&contract.key), sessions)
                {
                    cached.assertions = Some((sessions.clone(), validity.clone()));
                }
            }
            validity
        };
        if self.authority_epochs() == Some(epoch) {
            result
        } else {
            unavailable(None)
        }
    }
}
