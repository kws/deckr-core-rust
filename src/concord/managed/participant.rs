use std::future::Future;

use super::*;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

/// Owned, immutable input to one policy invocation. Existing agreements remain
/// present as they move from pending to valid; policy only expresses consent.
#[derive(Debug, Clone)]
pub struct ParticipantCandidates {
    pub version: u64,
    pub contracts: Vec<ConcordViewContract>,
    /// Agreements already selected by this lifetime, including attachment in progress.
    pub selected: Vec<ContractPointer>,
}
pub trait ParticipantPolicy: Send + Sync + 'static {
    fn select(
        &self,
        candidates: ParticipantCandidates,
    ) -> impl Future<Output = Result<Vec<ContractPointer>>> + Send;
    /// Final synchronous fence for asynchronous domain decisions. This runs
    /// under the membership lock immediately before applying a selection; it
    /// must be bounded, perform no I/O and not call participant methods. Domain
    /// owners can advance an epoch before revoking existing memberships so a
    /// completed callback cannot attach new work from the previous epoch.
    fn selection_is_current(&self) -> bool {
        true
    }
}
impl<F, Fut> ParticipantPolicy for F
where
    F: Fn(ParticipantCandidates) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<ContractPointer>>> + Send,
{
    fn select(
        &self,
        candidates: ParticipantCandidates,
    ) -> impl Future<Output = Result<Vec<ContractPointer>>> + Send {
        self(candidates)
    }
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ParticipantState {
    pub contracts: BTreeMap<String, Snapshot<AgreementState>>,
}
struct Owner<C: StateStore, T: StateStore> {
    concord: MaterializedConcord<C, T>,
    session: EndpointSession,
    stop: Cancellation,
    wake: Arc<Notify>,
    state: StateCell<ParticipantState>,
    agreements: Arc<Mutex<BTreeMap<String, AgreementContext<C, T>>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl<C: StateStore, T: StateStore> Drop for Owner<C, T> {
    fn drop(&mut self) {
        self.stop.cancel();
        for agreement in self
            .agreements
            .lock()
            .expect("participant membership lock poisoned")
            .values()
        {
            agreement.request_close();
        }
    }
}
pub struct ManagedParticipant<C: StateStore, T: StateStore> {
    owner: Arc<Owner<C, T>>,
}
impl<C: StateStore, T: StateStore> Clone for ManagedParticipant<C, T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore> ManagedParticipant<C, T> {
    pub(super) fn start<P: ParticipantPolicy>(
        concord: MaterializedConcord<C, T>,
        session: EndpointSession,
        profile: Option<String>,
        policy: P,
    ) -> Result<Self> {
        require_runtime(&session)?;
        if let Some(profile) = &profile {
            require_text(profile, "participant profile")?;
        }
        let view = concord.watch_state(ContractFilters {
            participant: Some(session.endpoint()),
            profile: profile.as_deref(),
            ..Default::default()
        })?;
        let state = StateCell::new(ParticipantState::default());
        let stop = Cancellation::default();
        let wake = Arc::new(Notify::new());
        let agreements = Arc::new(Mutex::new(BTreeMap::new()));
        let worker = tokio::spawn(run(
            concord.clone(),
            session.clone(),
            view,
            profile,
            Arc::new(policy),
            state.clone(),
            stop.clone(),
            wake.clone(),
            agreements.clone(),
        ));
        Ok(Self {
            owner: Arc::new(Owner {
                concord,
                session,
                stop,
                wake,
                state,
                agreements,
                worker: Mutex::new(Some(worker)),
            }),
        })
    }
    pub fn state(&self) -> Snapshot<ParticipantState> {
        let value = membership(&self.owner.agreements);
        let current = self.owner.concord.is_current()
            && !self.owner.stop.is_cancelled()
            && !self.owner.session.is_closed();
        self.owner.state.modify(|snapshot| {
            // Reads can revoke currentness immediately, but readiness is only
            // published by the lifecycle worker after entry has completed.
            let current = snapshot.current && current;
            if snapshot.current == current && snapshot.value == value {
                return false;
            }
            snapshot.changed = snapshot
                .value
                .contracts
                .keys()
                .chain(value.contracts.keys())
                .cloned()
                .collect();
            snapshot.current = current;
            snapshot.value = value;
            true
        });
        self.owner.state.snapshot()
    }
    pub fn watch_state(&self) -> Result<StateWatch<ParticipantState>> {
        self.owner.state.watch()
    }
    pub async fn wait_for(
        &self,
        predicate: impl FnMut(&Snapshot<ParticipantState>) -> bool,
        timeout: Option<Duration>,
    ) -> Result<Snapshot<ParticipantState>> {
        self.owner
            .state
            .wait_for(predicate, timeout, self.owner.concord.clock.as_ref())
            .await
    }
    pub fn agreement(&self, pointer: &ContractPointer) -> Option<AgreementContext<C, T>> {
        self.owner
            .agreements
            .lock()
            .expect("participant membership lock poisoned")
            .get(&make_concord_contract_key(
                &pointer.contract_id,
                pointer.generation,
            ))
            .cloned()
    }
    /// Explicit domain-policy change. Notifications coalesce while selection runs.
    pub fn wake_policy(&self) {
        self.owner.wake.notify_one();
    }
    pub fn request_close(&self) {
        self.owner.stop.cancel();
        for agreement in self
            .owner
            .agreements
            .lock()
            .expect("participant membership lock poisoned")
            .values()
        {
            agreement.request_close();
        }
        self.owner.state.modify(|snapshot| {
            if snapshot.phase == Phase::Closing {
                return false;
            }
            snapshot.phase = Phase::Closing;
            snapshot.current = false;
            snapshot.value = membership(&self.owner.agreements);
            snapshot.changed = snapshot.value.contracts.keys().cloned().collect();
            true
        });
    }
    pub async fn close(&self) -> Result<()> {
        self.request_close();
        let worker = self
            .owner
            .worker
            .lock()
            .expect("participant worker lock poisoned")
            .take();
        if let Some(worker) = worker {
            worker
                .await
                .map_err(|e| Error::StateUnavailable(format!("participant worker failed: {e}")))?;
        }
        let closed = self.owner.state.wait_closed().await;
        if let Some(failure) = closed.failure {
            return Err(Error::StateUnavailable(failure.detail));
        }
        Ok(())
    }
}

type Members<C, T> = Arc<Mutex<BTreeMap<String, AgreementContext<C, T>>>>;
fn membership<C: StateStore, T: StateStore>(members: &Members<C, T>) -> ParticipantState {
    let members = members
        .lock()
        .expect("participant membership lock poisoned");
    ParticipantState {
        contracts: members
            .iter()
            .map(|(key, agreement)| (key.clone(), agreement.state()))
            .collect(),
    }
}
struct Completion<C: StateStore, T: StateStore> {
    state: StateCell<ParticipantState>,
    members: Members<C, T>,
}
impl<C: StateStore, T: StateStore> Drop for Completion<C, T> {
    fn drop(&mut self) {
        for agreement in self
            .members
            .lock()
            .expect("participant membership lock poisoned")
            .values()
        {
            agreement.request_close();
        }
        if self.state.snapshot().phase != Phase::Closed {
            self.state.publish(
                Phase::Closed,
                false,
                membership(&self.members),
                Some(Failure::from_error(
                    "participant worker",
                    &Error::StateUnavailable("worker exited unexpectedly".into()),
                )),
                BTreeSet::new(),
            );
        }
    }
}
struct Selection {
    task: JoinHandle<Result<Vec<ContractPointer>>>,
    candidates: BTreeMap<String, ContractHandle>,
}
impl Drop for Selection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[allow(clippy::too_many_arguments)]
async fn run<C: StateStore, T: StateStore, P: ParticipantPolicy>(
    concord: MaterializedConcord<C, T>,
    session: EndpointSession,
    mut view: ConcordStateWatch,
    profile: Option<String>,
    policy: Arc<P>,
    state: StateCell<ParticipantState>,
    stop: Cancellation,
    wake: Arc<Notify>,
    agreements: Members<C, T>,
) {
    let _completion = Completion {
        state: state.clone(),
        members: agreements.clone(),
    };
    let bootstrap = tokio::select! {
        biased;
        _ = stop.cancelled() => Err(Error::Closed("participant closing".into())),
        _ = session.closed() => Err(Error::Closed("endpoint session closed".into())),
        result = deadline(concord.clock.as_ref(), Duration::from_secs(5), "participant bootstrap", concord.bootstrap(&session, profile.as_deref(), false)) => result,
    };
    if let Err(error) = bootstrap {
        let failure = if stop.is_cancelled() || session.is_closed() {
            None
        } else {
            Some(Failure::from_error("participant bootstrap", &error))
        };
        if failure.is_some() {
            state.publish(
                Phase::Terminal,
                false,
                ParticipantState::default(),
                failure.clone(),
                BTreeSet::new(),
            );
            tokio::select! { _ = stop.cancelled() => {}, _ = session.closed() => {} }
        }
        state.publish(
            Phase::Closed,
            false,
            ParticipantState::default(),
            failure,
            BTreeSet::new(),
        );
        return;
    }
    let changed = Arc::new(Notify::new());
    let (updates, receiver) = watch::channel(0u64);
    let mut snapshot = None::<ConcordSnapshot>;
    let mut selection = None::<Selection>;
    let mut pending = false;
    let mut failure = None;
    let mut rejected = BTreeSet::new();
    loop {
        if stop.is_cancelled() || session.is_closed() {
            break;
        }
        if pending && selection.is_none() && snapshot.as_ref().is_some_and(|s| s.current) {
            pending = false;
            let source = snapshot.as_ref().expect("current snapshot");
            let existing = membership(&agreements);
            for (key, member) in &existing.contracts {
                if matches!(
                    member.phase,
                    Phase::Closing | Phase::Closed | Phase::Terminal
                ) {
                    rejected.insert(key.clone());
                }
            }
            let selected = existing
                .contracts
                .values()
                .filter(|member| matches!(member.phase, Phase::Starting | Phase::Ready))
                .filter_map(|member| member.value.contract.as_ref())
                .map(|contract| ContractPointer {
                    contract_id: contract.contract_id.clone(),
                    generation: contract.generation,
                })
                .collect();
            let contracts: Vec<_> = source
                .contracts
                .iter()
                .filter(|member| {
                    member.contract.state == ContractState::Open
                        && !rejected.contains(&member.contract.key)
                })
                .cloned()
                .collect();
            let candidates = contracts
                .iter()
                .map(|member| (member.contract.key.clone(), member.contract.clone()))
                .collect();
            let policy = policy.clone();
            let candidates_snapshot = ParticipantCandidates {
                version: source.version,
                contracts,
                selected,
            };
            selection = Some(Selection {
                task: tokio::spawn(async move { policy.select(candidates_snapshot).await }),
                candidates,
            });
        }
        let value = membership(&agreements);
        let current = snapshot.as_ref().is_some_and(|s| s.current) && concord.is_current();
        let phase = if !current || failure.is_some() {
            Phase::Unavailable
        } else {
            Phase::Ready
        };
        let previous = state.snapshot();
        if previous.value != value
            || previous.phase != phase
            || previous.current != current
            || previous.failure != failure
        {
            let keys = previous
                .value
                .contracts
                .keys()
                .chain(value.contracts.keys())
                .cloned()
                .collect();
            state.publish(phase, current, value, failure.clone(), keys);
        }
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = session.closed() => break,
            next = view.next() => {
                let Some(next) = next else { failure = Some(Failure::from_error("Concord view", &Error::Closed("participant source closed".into()))); break; };
                if next.closed { failure = Some(Failure::from_error("Concord view", &Error::Closed("participant source closed".into()))); break; }
                updates.send_modify(|version| *version += 1);
                snapshot = Some(next);
                pending = true;
            }
            result = async { match selection.as_mut() { Some(selection) => (&mut selection.task).await, None => std::future::pending().await } } => {
                let finished = selection.take().expect("selection task");
                let selected = result.map_err(|error| Error::StateUnavailable(format!("participant selector failed: {error}"))).and_then(|result| result);
                let selected = selected.and_then(|pointers| {
                    let mut keys = BTreeSet::new();
                    for pointer in pointers {
                        pointer.validate()?;
                        let key = make_concord_contract_key(&pointer.contract_id, pointer.generation);
                        if !finished.candidates.contains_key(&key) { return Err(Error::Invalid("participant selector returned an unknown pointer".into())); }
                        if !keys.insert(key) { return Err(Error::Invalid("participant selector returned a duplicate pointer".into())); }
                    }
                    Ok(keys)
                });
                match selected {
                    Err(error) => failure = Some(Failure::from_error("participant selection", &error)),
                    Ok(selected) => {
                        failure = None;
                        // An observation arriving during selection gets a fresh
                        // policy pass. Protocol checks continue in child workers.
                        if !pending && concord.is_current() {
                            let mut members = agreements.lock().expect("participant membership lock poisoned");
                            // Catch callback panics before they can poison the
                            // membership lock and prevent owned cleanup.
                            let current = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| policy.selection_is_current()));
                            if matches!(current, Ok(false)) {
                                pending = true;
                            } else if current.is_err() {
                                failure = Some(Failure::from_error("participant selection fence", &Error::StateUnavailable("selection fence panicked".into())));
                            } else {
                                for (key, agreement) in members.iter() {
                                    if !selected.contains(key) { agreement.request_close(); rejected.insert(key.clone()); }
                                }
                                for key in selected {
                                    if !members.contains_key(&key) && !rejected.contains(&key) {
                                        let contract = finished.candidates[&key].clone();
                                        let agreement = AgreementContext::participant(concord.clone(), session.clone(), contract, receiver.clone(), changed.clone());
                                        members.insert(key, agreement);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ = changed.notified() => {},
            _ = wake.notified() => pending = true,
        }
    }
    if let Some(selection) = selection.take() {
        selection.task.abort();
    }
    let members: Vec<_> = agreements
        .lock()
        .expect("participant membership lock poisoned")
        .values()
        .cloned()
        .collect();
    for member in &members {
        member.request_close();
    }
    state.publish(
        Phase::Closing,
        false,
        membership(&agreements),
        None,
        BTreeSet::new(),
    );
    for result in futures_util::future::join_all(members.iter().map(|member| member.close())).await
    {
        if let Err(error) = result {
            failure = Some(Failure::from_error("participant cleanup", &error));
        }
    }
    state.publish(
        Phase::Closed,
        false,
        membership(&agreements),
        failure,
        BTreeSet::new(),
    );
}
