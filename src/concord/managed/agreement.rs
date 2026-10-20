use futures_util::FutureExt;
use std::ops::Deref;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use super::*;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AgreementState {
    pub contract: Option<ContractHandle>,
    pub validity: Option<ContractValidity>,
    pub local_authority: bool,
    pub token: Option<ParticipantHandle>,
}

struct Held {
    token: ParticipantHandle,
    epoch: (u64, u64),
    expires: Instant,
}
struct Shared<C: StateStore, T: StateStore> {
    concord: MaterializedConcord<C, T>,
    session: EndpointSession,
    contract: ContractHandle,
    sessions: BTreeMap<String, String>,
    state: StateCell<AgreementState>,
    stop: Cancellation,
    lost: Cancellation,
    held: Mutex<Option<Held>>,
    completed: AtomicBool,
    must_cancel: AtomicBool,
    cancel_on_close: AtomicBool,
    close_reason: Mutex<Option<String>>,
    cleanup_deadline: Mutex<Option<Instant>>,
    changed: Arc<Notify>,
}
impl<C: StateStore, T: StateStore> Shared<C, T> {
    fn publish(
        &self,
        phase: Phase,
        current: bool,
        value: AgreementState,
        failure: Option<Failure>,
    ) {
        self.state.modify(|snapshot| {
            if (matches!(phase, Phase::Starting | Phase::Ready)
                && (self.lost.is_cancelled()
                    || self.stop.is_cancelled()
                    || self.session.is_closed()))
                || (snapshot.phase == Phase::Closing && phase != Phase::Closed)
            {
                return false;
            }
            if snapshot.phase == phase
                && snapshot.current == current
                && snapshot.value == value
                && snapshot.failure == failure
            {
                return false;
            }
            snapshot.phase = phase;
            snapshot.current = current;
            snapshot.value = value;
            snapshot.failure = failure;
            snapshot.changed = BTreeSet::from([self.contract.key.clone()]);
            true
        });
        self.changed.notify_one();
    }
    fn terminate(&self, failure: Failure) {
        self.must_cancel.store(true, Ordering::SeqCst);
        self.lost.cancel();
        let mut snapshot = self.state.snapshot();
        if !matches!(
            snapshot.phase,
            Phase::Closing | Phase::Closed | Phase::Terminal
        ) {
            snapshot.value.local_authority = false;
            self.publish(
                Phase::Terminal,
                self.concord.is_current(),
                snapshot.value,
                Some(failure),
            );
        }
    }
    fn authority(&self) -> bool {
        if self.stop.is_cancelled() || self.session.is_closed() || self.lost.is_cancelled() {
            return false;
        }
        let held = self.held.lock().expect("agreement ownership lock poisoned");
        let Some(held) = held.as_ref() else {
            return false;
        };
        let failure = if self.concord.authority_epochs() != Some(held.epoch) {
            Some(Error::MaterializedViewStale(
                "agreement source continuity lost".into(),
            ))
        } else if self.concord.clock.now() >= held.expires {
            Some(Error::Deadline("participant heartbeat expired".into()))
        } else {
            None
        };
        if let Some(error) = failure {
            self.terminate(Failure::from_error("authority", &error));
            return false;
        }
        let valid = self
            .concord
            .validate_cached(&self.contract, Some(&self.sessions));
        let contract_matches = valid.contract.as_ref().is_some_and(|record| {
            record.participants == self.contract.participants
                && record.profile == self.contract.profile
                && record.terms_hash == self.contract.terms_hash
        });
        let identity_matches = contract_matches
            && valid
                .tokens
                .get(self.session.endpoint().as_str())
                .is_some_and(|token| token_matches_handle(token, &held.token));
        if valid.status == ContractValidityStatus::NotYetFulfilled && identity_matches {
            return false;
        }
        if !valid.valid() || !identity_matches {
            self.terminate(Failure::from_error(
                "authority",
                &Error::Unauthorized(format!(
                    "agreement authority ended: {:?}",
                    valid.reason_code
                )),
            ));
            return false;
        }
        true
    }
}
struct Owner<C: StateStore, T: StateStore> {
    shared: Arc<Shared<C, T>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl<C: StateStore, T: StateStore> Drop for Owner<C, T> {
    fn drop(&mut self) {
        self.shared.stop.cancel();
    }
}

/// A capability scoped to one exact agreement and endpoint session. Protected
/// callers check `has_authority` immediately before dispatch; retaining a handle
/// cannot restore authority after loss or source recovery.
pub struct AgreementContext<C: StateStore, T: StateStore> {
    owner: Arc<Owner<C, T>>,
}
impl<C: StateStore, T: StateStore> Clone for AgreementContext<C, T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore> AgreementContext<C, T> {
    pub fn contract(&self) -> &ContractHandle {
        &self.owner.shared.contract
    }
    pub fn session(&self) -> &EndpointSession {
        &self.owner.shared.session
    }
    pub fn has_authority(&self) -> bool {
        self.owner.shared.authority()
    }
    pub fn authorize(&self) -> Result<()> {
        if self.has_authority() {
            Ok(())
        } else {
            Err(Error::Unauthorized(
                "agreement has no local authority".into(),
            ))
        }
    }

    /// Current protocol observations under this lifetime's authority. A valid
    /// stored record alone cannot revive a context that has lost local authority.
    pub fn authorized_validity(&self) -> Result<ContractValidity> {
        self.authorize()?;
        let shared = &self.owner.shared;
        let validity = shared
            .concord
            .validate_cached(&shared.contract, Some(&shared.sessions));
        self.authorize()?;
        if !validity.valid() {
            return Err(Error::Unauthorized(
                "agreement observations are no longer valid".into(),
            ));
        }
        Ok(validity)
    }

    /// Current raw record for profile rules that distinguish absent fields from
    /// explicit nulls. Access remains limited to this exact authorized contract.
    pub fn authorized_record(&self) -> Result<crate::state::StateEntry> {
        self.authorize()?;
        let shared = &self.owner.shared;
        let entry = shared
            .concord
            .contract_state
            .get_cached(&shared.contract.key)?
            .ok_or_else(|| Error::Unauthorized("agreement record is missing".into()))?;
        self.authorize()?;
        Ok(entry)
    }

    pub(crate) fn utc_now(&self) -> chrono::DateTime<chrono::Utc> {
        self.owner.shared.concord.clock.utc_now()
    }
    pub(crate) fn runtime_clock(&self) -> Arc<dyn crate::lifecycle::Clock> {
        self.owner.shared.concord.clock.clone()
    }
    pub fn state(&self) -> Snapshot<AgreementState> {
        self.has_authority();
        if self.owner.shared.session.is_closed() {
            self.request_close();
        }
        self.owner.shared.state.snapshot()
    }
    pub fn watch_state(&self) -> Result<StateWatch<AgreementState>> {
        self.owner.shared.state.watch()
    }
    pub async fn wait_for(
        &self,
        predicate: impl FnMut(&Snapshot<AgreementState>) -> bool,
        timeout: Option<Duration>,
    ) -> Result<Snapshot<AgreementState>> {
        self.owner
            .shared
            .state
            .wait_for(predicate, timeout, self.owner.shared.concord.clock.as_ref())
            .await
    }
    pub async fn wait_ready(&self, timeout: Option<Duration>) -> Result<AgreementState> {
        let state = self
            .wait_for(
                |s| s.value.local_authority || s.phase == Phase::Terminal,
                timeout,
            )
            .await?;
        self.authorize()?;
        Ok(state.value)
    }
    /// Revoke locally now; cleanup continues in the owned worker.
    pub fn request_close(&self) {
        let shared = &self.owner.shared;
        shared.stop.cancel();
        let mut value = shared.state.snapshot().value;
        value.local_authority = false;
        shared.publish(Phase::Closing, false, value, None);
    }
    /// End domain consent with a diagnostic cancellation reason.
    pub fn request_close_with_reason(&self, reason: impl Into<String>) {
        *self
            .owner
            .shared
            .close_reason
            .lock()
            .expect("close reason lock poisoned") = Some(reason.into());
        self.request_close();
    }

    /// Cleanly withdraw participation without requesting contract cancellation.
    /// A prior authority failure still requires cancellation.
    pub fn request_withdraw(&self) {
        // Observe an already expired token or lost continuity before marking
        // this a clean exit, even when the worker has not consumed that event.
        self.has_authority();
        self.owner
            .shared
            .cancel_on_close
            .store(false, Ordering::SeqCst);
        self.request_close();
    }

    pub async fn close(&self) -> Result<()> {
        self.request_close();
        let worker = self
            .owner
            .worker
            .lock()
            .expect("agreement worker lock poisoned")
            .take();
        if let Some(worker) = worker {
            worker
                .await
                .map_err(|e| Error::StateUnavailable(format!("agreement worker failed: {e}")))?;
        }
        let closed = self.owner.shared.state.wait_closed().await;
        if let Some(failure) = closed.failure {
            return Err(Error::StateUnavailable(failure.detail));
        }
        Ok(())
    }

    pub(super) fn participant(
        concord: MaterializedConcord<C, T>,
        session: EndpointSession,
        contract: ContractHandle,
        changes: watch::Receiver<u64>,
        changed: Arc<Notify>,
    ) -> Self {
        Self::spawn(
            concord,
            session,
            contract,
            BTreeMap::new(),
            None,
            Notifications::Shared(changes),
            changed,
            false,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn spawn(
        concord: MaterializedConcord<C, T>,
        session: EndpointSession,
        contract: ContractHandle,
        mut sessions: BTreeMap<String, String>,
        proposal: Option<CreateContractSpec>,
        notifications: Notifications,
        changed: Arc<Notify>,
        exclusive: bool,
    ) -> Self {
        sessions.insert(session.endpoint().to_string(), session.id().to_string());
        let state = StateCell::new(AgreementState {
            contract: Some(contract.clone()),
            ..Default::default()
        });
        let shared = Arc::new(Shared {
            concord,
            session,
            contract,
            sessions,
            state,
            stop: Cancellation::default(),
            lost: Cancellation::default(),
            held: Mutex::new(None),
            completed: AtomicBool::new(false),
            must_cancel: AtomicBool::new(false),
            cancel_on_close: AtomicBool::new(true),
            close_reason: Mutex::new(None),
            cleanup_deadline: Mutex::new(None),
            changed,
        });
        let worker = tokio::spawn(run(shared.clone(), proposal, notifications, exclusive));
        Self {
            owner: Arc::new(Owner {
                shared,
                worker: Mutex::new(Some(worker)),
            }),
        }
    }
}

/// One proposal, never an automatic successor loop.
///
/// ```no_run
/// # async fn example() -> deckr::Result<()> {
/// use deckr::concord::{AgreementIntent, ConcordCoordinator, ParticipantCandidates};
/// use deckr::authority::ContractPointer;
/// use deckr::endpoint::EndpointAddress;
/// use deckr::session::EndpointSession;
/// use deckr::state::{MaterializedStateStore, MemoryStateStore};
/// use std::time::Duration;
///
/// let contracts = MaterializedStateStore::start(MemoryStateStore::default(), "contracts.").await?;
/// let tokens = MaterializedStateStore::start(MemoryStateStore::ttl_bound(120)?, "contracts.").await?;
/// let concord = ConcordCoordinator::new(contracts, tokens);
/// let a = EndpointSession::new(EndpointAddress::parse_protocol("worker:a")?);
/// let b = EndpointSession::new(EndpointAddress::parse_protocol("worker:b")?);
/// let participant = concord.participate(b.clone(), None, |candidates: ParticipantCandidates| async move {
///     Ok(candidates.contracts.into_iter().map(|member| ContractPointer {
///         contract_id: member.contract.contract_id, generation: member.contract.generation,
///     }).collect())
/// })?;
/// let proposal = concord.propose(a.clone(), AgreementIntent::new(vec![a.endpoint().clone(), b.endpoint().clone()]))?;
/// proposal.wait_ready(Some(Duration::from_secs(5))).await?;
/// proposal.authorize()?;
/// // Perform the protected operation in this exact agreement's context.
/// proposal.close().await?;
/// participant.close().await?;
/// concord.close_sources_and_wait().await;
/// # Ok(())
/// # }
/// ```
pub struct ManagedProposal<C: StateStore, T: StateStore> {
    agreement: AgreementContext<C, T>,
}
impl<C: StateStore, T: StateStore> Clone for ManagedProposal<C, T> {
    fn clone(&self) -> Self {
        Self {
            agreement: self.agreement.clone(),
        }
    }
}
impl<C: StateStore, T: StateStore> Deref for ManagedProposal<C, T> {
    type Target = AgreementContext<C, T>;
    fn deref(&self) -> &Self::Target {
        &self.agreement
    }
}
impl<C: StateStore, T: StateStore> ManagedProposal<C, T> {
    pub(super) fn start(
        concord: MaterializedConcord<C, T>,
        session: EndpointSession,
        intent: AgreementIntent,
    ) -> Result<Self> {
        require_runtime(&session)?;
        if !intent.participants.contains(session.endpoint()) {
            return Err(Error::Invalid(
                "proposal must name its endpoint session".into(),
            ));
        }
        if intent
            .current_sessions
            .get(session.endpoint().as_str())
            .is_some_and(|id| id != session.id())
        {
            return Err(Error::Invalid(
                "proposal has a conflicting local session assertion".into(),
            ));
        }
        let spec = CreateContractSpec {
            participants: intent.participants,
            contract_id: Some(Uuid::new_v4().to_string()),
            generation: 1,
            profile: intent.profile,
            terms: intent.terms,
            created_by: intent.creator.or_else(|| Some(session.endpoint().clone())),
            participant_sessions: intent.current_sessions.clone(),
            supersedes: intent.supersedes,
        };
        let record = proposal_record(spec.clone(), concord.clock.as_ref())?;
        let key = make_concord_contract_key(&record.contract_id, 1);
        let contract = contract_handle(key, &record, 0);
        let notifications = Notifications::View(concord.watch_state(ContractFilters {
            contract_id: Some(&contract.contract_id),
            ..Default::default()
        })?);
        let agreement = AgreementContext::spawn(
            concord,
            session,
            contract,
            intent.current_sessions,
            Some(spec),
            notifications,
            Arc::default(),
            intent.exclusive,
        );
        Ok(Self { agreement })
    }
    pub fn agreement(&self) -> AgreementContext<C, T> {
        self.agreement.clone()
    }
}

enum Notifications {
    View(ConcordStateWatch),
    Shared(watch::Receiver<u64>),
}
impl Notifications {
    async fn next(&mut self) -> bool {
        match self {
            Self::View(view) => view.next().await.is_some_and(|snapshot| !snapshot.closed),
            Self::Shared(changes) => changes.changed().await.is_ok(),
        }
    }
}

struct Completion<C: StateStore, T: StateStore>(Arc<Shared<C, T>>);
impl<C: StateStore, T: StateStore> Drop for Completion<C, T> {
    fn drop(&mut self) {
        self.0.lost.cancel();
        if self.0.state.snapshot().phase != Phase::Closed {
            self.0.publish(
                Phase::Closed,
                false,
                AgreementState {
                    contract: Some(self.0.contract.clone()),
                    ..Default::default()
                },
                Some(Failure::from_error(
                    "agreement worker",
                    &Error::StateUnavailable("worker exited unexpectedly".into()),
                )),
            );
        }
    }
}

async fn run<C: StateStore, T: StateStore>(
    shared: Arc<Shared<C, T>>,
    proposal: Option<CreateContractSpec>,
    notifications: Notifications,
    exclusive: bool,
) {
    let _completion = Completion(shared.clone());
    let owned = ParticipantHandle {
        key: make_concord_participant_token_key(
            &shared.contract.contract_id,
            shared.contract.generation,
            shared.session.endpoint(),
        ),
        contract_id: shared.contract.contract_id.clone(),
        generation: shared.contract.generation,
        participant: shared.session.endpoint().clone(),
        session_id: shared.session.id().into(),
        token_id: Uuid::new_v4().to_string(),
        revision: 0,
        refresh_seq: 1,
        ttl_seconds: 1,
        terms_hash: shared.contract.terms_hash.clone(),
    };
    let mut created = false;
    let result = AssertUnwindSafe(run_lifetime(
        shared.clone(),
        proposal,
        notifications,
        exclusive,
        &owned,
        &mut created,
    ))
    .catch_unwind()
    .await;
    if result.is_err() {
        let failure = Failure::from_error(
            "agreement worker",
            &Error::StateUnavailable("agreement adapter panicked".into()),
        );
        shared.terminate(failure.clone());
        let cleanup_failure = cleanup(&shared, &owned, created)
            .await
            .err()
            .map(|error| Failure::from_error("agreement cleanup", &error));
        *shared
            .held
            .lock()
            .expect("agreement ownership lock poisoned") = None;
        let mut state = shared.state.snapshot().value;
        state.local_authority = false;
        state.token = None;
        shared.publish(
            Phase::Closed,
            false,
            state,
            cleanup_failure.or(Some(failure)),
        );
    }
}

async fn run_lifetime<C: StateStore, T: StateStore>(
    shared: Arc<Shared<C, T>>,
    proposal: Option<CreateContractSpec>,
    mut notifications: Notifications,
    exclusive: bool,
    owned: &ParticipantHandle,
    created: &mut bool,
) {
    let clock = shared.concord.clock.clone();

    let prepare = async {
        shared.concord.wait_current().await?;
        if let Some(spec) = proposal {
            deadline(
                clock.as_ref(),
                Duration::from_secs(5),
                "owner bootstrap",
                shared
                    .concord
                    .bootstrap(&shared.session, spec.profile.as_deref(), exclusive),
            )
            .await?;
            *created = true; // retained identity also covers an unacknowledged commit
            let creation = deadline(
                clock.as_ref(),
                Duration::from_secs(5),
                "proposal creation",
                shared.concord.create_contract(spec),
            )
            .await;
            if creation.as_ref().err().is_some_and(|error| {
                error.mutation_code() == Some(crate::MutationCode::KeyAlreadyExists)
            }) {
                *created = false;
            }
            creation?;
        }
        let epoch = shared
            .concord
            .authority_epochs()
            .ok_or_else(|| Error::MaterializedViewStale("attachment source is stale".into()))?;
        let started = clock.now();
        let attach = async {
            for attempt in 0..8 {
                let before = shared
                    .concord
                    .raw_contract_cached(&shared.contract)?
                    .map(|entry| entry.revision);
                match shared
                    .concord
                    .attach_once(
                        &shared.contract,
                        shared.session.endpoint(),
                        shared.session.id(),
                        owned.token_id.clone(),
                    )
                    .await
                {
                    Err(error) if is_state_revision_conflict(&error) && attempt < 7 => {
                        while before
                            == shared
                                .concord
                                .raw_contract_cached(&shared.contract)?
                                .map(|entry| entry.revision)
                        {
                            if !notifications.next().await {
                                return Err(Error::Closed(
                                    "Concord view closed during attachment".into(),
                                ));
                            }
                        }
                    }
                    result => return result,
                }
            }
            unreachable!("bounded attachment attempts")
        };
        let token = deadline(
            clock.as_ref(),
            Duration::from_secs(5),
            "participant attachment",
            attach,
        )
        .await?;
        if shared.concord.authority_epochs() != Some(epoch) {
            return Err(Error::MaterializedViewStale(
                "source continuity lost during attachment".into(),
            ));
        }
        let expires = started + Duration::from_secs(token.ttl_seconds);
        *shared
            .held
            .lock()
            .expect("agreement ownership lock poisoned") = Some(Held {
            token: token.clone(),
            epoch,
            expires,
        });
        shared.completed.store(true, Ordering::SeqCst);
        Ok(token)
    };
    let prepared = tokio::select! {
        biased;
        _ = shared.stop.cancelled() => Err(Error::Closed("agreement closing".into())),
        _ = shared.session.closed() => Err(Error::Closed("endpoint session closed".into())),
        result = prepare => result,
    };
    let mut failure = None;
    if let Ok(mut token) = prepared {
        let mut next = clock.now() + heartbeat_delay(clock.as_ref(), token.ttl_seconds);
        loop {
            let validity = shared
                .concord
                .validate_cached(&shared.contract, Some(&shared.sessions));
            let authority = shared.authority();
            if shared.lost.is_cancelled() {
                break;
            }
            let phase = if authority {
                Phase::Ready
            } else {
                Phase::Starting
            };
            let observed_contract = shared
                .concord
                .contract_state
                .get_cached(&shared.contract.key)
                .ok()
                .flatten()
                .and_then(contract_handle_from_entry)
                .unwrap_or_else(|| shared.contract.clone());
            let state = AgreementState {
                contract: Some(observed_contract),
                validity: Some(validity),
                local_authority: authority,
                token: Some(token.clone()),
            };
            shared.publish(phase, shared.concord.is_current(), state, None);
            let renew = tokio::select! {
                biased;
                _ = shared.stop.cancelled() => break,
                _ = shared.session.closed() => break,
                _ = shared.lost.cancelled() => break,
                _ = clock.sleep_until(next) => true,
                open = notifications.next() => { if !open { shared.terminate(Failure::from_error("Concord view", &Error::Closed("authority source closed".into()))); break; } false },
            };
            if !renew {
                continue;
            }
            if shared.lost.is_cancelled()
                || shared.concord.authority_epochs()
                    != shared
                        .held
                        .lock()
                        .expect("agreement ownership lock poisoned")
                        .as_ref()
                        .map(|held| held.epoch)
            {
                shared.terminate(Failure::from_error(
                    "renewal",
                    &Error::MaterializedViewStale("source continuity lost".into()),
                ));
                break;
            }
            let started = clock.now();
            let expires = shared
                .held
                .lock()
                .expect("agreement ownership lock poisoned")
                .as_ref()
                .expect("completed attachment")
                .expires;
            let remaining = expires
                .saturating_duration_since(started)
                .min(Duration::from_secs(5));
            let refreshed = tokio::select! {
                biased;
                _ = shared.stop.cancelled() => break,
                _ = shared.session.closed() => break,
                _ = shared.lost.cancelled() => break,
                result = deadline(clock.as_ref(), remaining, "participant renewal", shared.concord.refresh(&token)) => result,
            };
            match refreshed {
                Ok(refreshed)
                    if refreshed.refresh_seq > token.refresh_seq
                        && participant_handle_matches(&token, &refreshed) =>
                {
                    token = refreshed;
                    let mut held = shared
                        .held
                        .lock()
                        .expect("agreement ownership lock poisoned");
                    let held = held.as_mut().expect("completed attachment");
                    held.token = token.clone();
                    held.expires = started + Duration::from_secs(token.ttl_seconds);
                    next = started + heartbeat_delay(clock.as_ref(), token.ttl_seconds);
                }
                result => {
                    let error = result.err().unwrap_or_else(|| {
                        Error::StateConflict("renewal did not advance the held token".into())
                    });
                    shared.terminate(Failure::from_error("participant renewal", &error));
                    break;
                }
            }
        }
    } else if let Err(error) = prepared {
        if !shared.stop.is_cancelled() && !shared.session.is_closed() {
            shared.terminate(Failure::from_error("agreement entry", &error));
        }
    }
    shared.lost.cancel();
    let closing = shared.stop.is_cancelled() || shared.session.is_closed();
    let mut state = shared.state.snapshot().value;
    state.local_authority = false;
    if closing {
        shared.publish(Phase::Closing, false, state.clone(), None);
    }
    if let Err(error) = cleanup(&shared, owned, *created).await {
        failure = Some(Failure::from_error("agreement cleanup", &error));
    }
    *shared
        .held
        .lock()
        .expect("agreement ownership lock poisoned") = None;
    if !closing {
        if let Some(failure) = failure.clone() {
            shared.publish(
                Phase::Terminal,
                shared.concord.is_current(),
                state.clone(),
                Some(failure),
            );
        }
        tokio::select! { _ = shared.stop.cancelled() => {}, _ = shared.session.closed() => {} }
    }
    state.token = None;
    shared.publish(Phase::Closed, false, state, failure);
}

async fn cleanup<C: StateStore, T: StateStore>(
    shared: &Shared<C, T>,
    owned: &ParticipantHandle,
    created: bool,
) -> Result<()> {
    let clock = shared.concord.clock.clone();
    let expires = *shared
        .cleanup_deadline
        .lock()
        .expect("cleanup deadline lock poisoned")
        .get_or_insert_with(|| clock.now() + Duration::from_secs(5));
    let cleanup = async {
        // Unknown acknowledgement is safe: the retained token ID is checked
        // before any withdrawal or cancellation of a participant's proposal.
        let may_cancel = if created || shared.completed.load(Ordering::SeqCst) {
            true
        } else {
            shared
                .concord
                .token_state
                .get(&owned.key)
                .await?
                .is_some_and(|entry| checked_token(&entry, owned).is_ok())
        };
        let withdraw = shared.concord.withdraw(owned);
        let reason = shared
            .close_reason
            .lock()
            .expect("close reason lock poisoned")
            .clone()
            .unwrap_or_else(|| "managed agreement ended".into());
        let cancel = async {
            if may_cancel
                && (shared.must_cancel.load(Ordering::SeqCst)
                    || shared.cancel_on_close.load(Ordering::SeqCst))
            {
                shared
                    .concord
                    .cancel(&shared.contract, shared.session.endpoint(), Some(reason))
                    .await?;
            }
            Ok::<_, Error>(())
        };
        let (withdrawn, cancelled) = futures_util::future::join(withdraw, cancel).await;
        // A foreign token was never owned and must be left untouched.
        if may_cancel {
            withdrawn?;
        }
        cancelled
    };

    let guarded = async {
        AssertUnwindSafe(cleanup)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                Err(Error::StateUnavailable(
                    "agreement cleanup adapter panicked".into(),
                ))
            })
    };
    deadline(
        clock.as_ref(),
        expires.saturating_duration_since(clock.now()),
        "agreement cleanup",
        guarded,
    )
    .await
}
