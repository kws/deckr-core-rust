mod support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use deckr::authority::ContractPointer;
use deckr::concord::{
    AgreementIntent, ConcordCoordinator, ContractValidityStatus, ParticipantCandidates,
    ParticipantPolicy,
};
use deckr::endpoint::EndpointAddress;
use deckr::lifecycle::{FailureKind, Phase};
use deckr::runtime::MaterializedConcord;
use deckr::session::EndpointSession;
use deckr::state::{
    MaterializedStateStore, MemoryStateStore, StateEntry, StateSource, StateStore, StateWatchStream,
};
use deckr::{Error, Result};
use serde_json::Value;
use support::ManualClock;

fn session(id: &str) -> EndpointSession {
    EndpointSession::with_id(
        EndpointAddress::parse_protocol(format!("worker:{id}")).unwrap(),
        format!("session-{id}"),
    )
    .unwrap()
}
fn intent(a: &EndpointSession, b: &EndpointSession) -> AgreementIntent {
    let mut intent = AgreementIntent::new(vec![a.endpoint().clone(), b.endpoint().clone()]);
    intent.profile = Some("org.example.job.v1".into());
    intent
}
async fn select_all(candidates: ParticipantCandidates) -> Result<Vec<ContractPointer>> {
    Ok(candidates
        .contracts
        .into_iter()
        .map(|member| ContractPointer {
            contract_id: member.contract.contract_id,
            generation: member.contract.generation,
        })
        .collect())
}

#[tokio::test]
async fn final_selection_fence_rejects_stale_decisions_and_contains_callback_panics() {
    struct Policy {
        mode: Arc<AtomicUsize>,
        checked: Arc<AtomicUsize>,
        ready: Arc<tokio::sync::Semaphore>,
    }
    impl ParticipantPolicy for Policy {
        async fn select(&self, candidates: ParticipantCandidates) -> Result<Vec<ContractPointer>> {
            // Empty snapshots must not consume the test's selection gate.
            if !candidates.contracts.is_empty() {
                self.ready.acquire().await.unwrap().forget();
            }
            select_all(candidates).await
        }
        fn selection_is_current(&self) -> bool {
            self.checked.fetch_add(1, Ordering::SeqCst);
            match self.mode.load(Ordering::SeqCst) {
                0 => false,
                1 => panic!("injected fence panic"),
                _ => true,
            }
        }
    }
    let (concord, _, tokens, _) = setup().await;
    let a = session("fenced-a");
    let b = session("fenced-b");
    let proposal = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    bounded(proposal.wait_for(|state| state.value.token.is_some(), None))
        .await
        .unwrap();
    let mode = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(tokio::sync::Semaphore::new(1));
    let participant = concord
        .participate(
            b,
            Some("org.example.job.v1".into()),
            Policy {
                mode: mode.clone(),
                checked: checked.clone(),
                ready: ready.clone(),
            },
        )
        .unwrap();
    eventually(|| checked.load(Ordering::SeqCst) > 0).await;
    assert!(participant.state().value.contracts.is_empty());
    assert_eq!(tokens.raw.items("").await.unwrap().len(), 1);
    mode.store(1, Ordering::SeqCst);
    ready.add_permits(1);
    bounded(participant.wait_for(
        |state| {
            state
                .failure
                .as_ref()
                .is_some_and(|failure| failure.detail.contains("selection fence panicked"))
        },
        None,
    ))
    .await
    .unwrap();
    assert!(participant.state().value.contracts.is_empty());
    mode.store(2, Ordering::SeqCst);
    ready.add_permits(10);
    participant.wake_policy();
    bounded(proposal.wait_ready(None)).await.unwrap();
    participant.close().await.unwrap();
    proposal.close().await.unwrap();
    assert!(tokens.raw.items("").await.unwrap().is_empty());
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn clean_withdrawal_cannot_suppress_cancellation_after_an_expired_heartbeat() {
    let (concord, _, _, clock) = setup().await;
    let a = session("a");
    let b = session("b");
    let proposal = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    bounded(proposal.wait_for(|state| state.value.token.is_some(), None))
        .await
        .unwrap();
    let peer = concord
        .attach(proposal.contract(), b.endpoint(), b.id(), None)
        .await
        .unwrap();
    bounded(proposal.wait_ready(None)).await.unwrap();
    clock.advance(Duration::from_secs(121));
    proposal.request_withdraw();
    assert!(!proposal.has_authority());
    bounded(proposal.close()).await.unwrap();
    assert_eq!(
        concord
            .contract_record(proposal.contract())
            .await
            .unwrap()
            .unwrap()
            .state,
        deckr::concord::ContractState::Cancelled
    );
    concord.withdraw(&peer).await.unwrap();
    concord.close_sources_and_wait().await;
}
async fn eventually(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition reached");
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("operation completed")
}

#[derive(Clone)]
struct FaultStore {
    raw: MemoryStateStore,
    scans: Arc<AtomicUsize>,
    attachment_conflicts: Arc<AtomicUsize>,
    attachment_attempts: Arc<AtomicUsize>,
    conflict_unrelated: Arc<AtomicBool>,
    fail_updates: Arc<AtomicBool>,
    panic_updates: Arc<AtomicBool>,
    panic_key: Arc<Mutex<Option<String>>>,
    stall_deletes: Arc<AtomicBool>,
    stall_create: Arc<Mutex<Option<String>>>,
}
impl FaultStore {
    fn new(ttl: bool) -> Self {
        Self {
            raw: if ttl {
                MemoryStateStore::ttl_bound(120).unwrap()
            } else {
                MemoryStateStore::default()
            },
            scans: Arc::default(),
            attachment_conflicts: Arc::default(),
            attachment_attempts: Arc::default(),
            conflict_unrelated: Arc::default(),
            fail_updates: Arc::default(),
            panic_updates: Arc::default(),
            panic_key: Arc::default(),
            stall_deletes: Arc::default(),
            stall_create: Arc::default(),
        }
    }
}
impl StateStore for FaultStore {
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        self.raw.ttl_seconds().await
    }
    async fn get(&self, key: &str) -> Result<Option<StateEntry>> {
        self.raw.get(key).await
    }
    async fn items(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.raw.items(prefix).await
    }
    async fn put(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        self.raw.put(key, value, ttl).await
    }
    async fn create(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        let entry = self.raw.create(key, value, ttl).await?;
        let stall = self
            .stall_create
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|prefix| key.starts_with(prefix));
        if stall {
            std::future::pending::<()>().await;
        }
        Ok(entry)
    }
    async fn update(
        &self,
        key: &str,
        value: Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<StateEntry> {
        if key.ends_with(".meta")
            && value["state"] == "open"
            && value["attachedParticipants"]
                .as_array()
                .is_some_and(|attached| !attached.is_empty())
        {
            self.attachment_attempts.fetch_add(1, Ordering::SeqCst);
            if self
                .attachment_conflicts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                    count.checked_sub(1)
                })
                .is_ok()
            {
                if self.conflict_unrelated.load(Ordering::SeqCst) {
                    self.raw
                        .put(
                            "contracts.unrelated.1.meta",
                            serde_json::json!({"noise": true}),
                            None,
                        )
                        .await?;
                } else {
                    let current = self.raw.get(key).await?.unwrap();
                    self.raw
                        .update(key, current.value, current.revision, ttl)
                        .await?;
                }
                return Err(Error::mutation(
                    deckr::MutationCode::RevisionChanged,
                    "injected CAS conflict",
                ));
            }
        }
        assert!(
            !(self.panic_updates.load(Ordering::SeqCst)
                && self
                    .panic_key
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_none_or(|selected| selected == key)),
            "injected adapter panic"
        );
        if self.fail_updates.load(Ordering::SeqCst) {
            return Err(Error::StateUnavailable("injected update failure".into()));
        }
        self.raw.update(key, value, revision, ttl).await
    }
    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        self.delete_revision(key, revision).await.map(|_| ())
    }
    async fn delete_revision(&self, key: &str, revision: Option<u64>) -> Result<u64> {
        if self.stall_deletes.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        self.raw.delete_revision(key, revision).await
    }
    async fn watch(&self, prefix: &str) -> Result<StateWatchStream> {
        self.raw.watch(prefix).await
    }
    async fn open_source(&self, prefix: &str) -> Result<StateSource> {
        self.raw.open_source(prefix).await
    }
}
async fn setup() -> (
    MaterializedConcord<FaultStore, FaultStore>,
    FaultStore,
    FaultStore,
    ManualClock,
) {
    let contracts = FaultStore::new(false);
    let tokens = FaultStore::new(true);
    let clock = ManualClock::default();
    let concord = ConcordCoordinator::new(
        MaterializedStateStore::start(contracts.clone(), "contracts.")
            .await
            .unwrap(),
        MaterializedStateStore::start(tokens.clone(), "contracts.")
            .await
            .unwrap(),
    )
    .clock(Arc::new(clock.clone()));
    concord.wait_current().await.unwrap();
    (concord, contracts, tokens, clock)
}

#[tokio::test]
async fn proposals_and_participants_renew_independently_without_discovery_scans() {
    let (concord, contracts, tokens, clock) = setup().await;
    let a = session("a");
    let b = session("b");
    let participant = concord
        .participate(b.clone(), Some("org.example.job.v1".into()), select_all)
        .unwrap();
    let proposal = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    let ready = bounded(proposal.wait_ready(None)).await.unwrap();
    assert!(ready.local_authority);
    let pointer = ContractPointer {
        contract_id: proposal.contract().contract_id.clone(),
        generation: 1,
    };
    let other = participant.agreement(&pointer).unwrap();
    bounded(other.wait_ready(None)).await.unwrap();
    assert!(participant.state().current);
    let owned = ready.token.unwrap();
    clock.advance(Duration::from_secs(74));
    tokio::task::yield_now().await;
    assert_eq!(
        proposal.state().value.token.as_ref().unwrap().refresh_seq,
        1
    );
    clock.advance(Duration::from_secs(1));
    eventually(|| {
        proposal
            .state()
            .value
            .token
            .as_ref()
            .is_some_and(|token| token.refresh_seq == 2)
            && other
                .state()
                .value
                .token
                .as_ref()
                .is_some_and(|token| token.refresh_seq == 2)
    })
    .await;
    assert_eq!(contracts.scans.load(Ordering::SeqCst), 0);
    assert_eq!(tokens.scans.load(Ordering::SeqCst), 0);
    bounded(proposal.close()).await.unwrap();
    assert!(!proposal.has_authority());
    assert!(tokens.raw.get(&owned.key).await.unwrap().is_none());
    bounded(participant.close()).await.unwrap();
    assert!(concord.is_current(), "borrowed sources remain open");
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn failed_renewal_is_terminal_and_never_creates_a_successor() {
    let (concord, contracts, tokens, clock) = setup().await;
    let a = session("a");
    let b = session("b");
    let participant = concord.participate(b.clone(), None, select_all).unwrap();
    let proposal = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    bounded(proposal.wait_ready(None)).await.unwrap();
    tokens.fail_updates.store(true, Ordering::SeqCst);
    clock.advance(Duration::from_secs(75));
    eventually(|| proposal.state().phase == Phase::Terminal).await;
    assert!(!proposal.has_authority());
    tokens.fail_updates.store(false, Ordering::SeqCst);
    clock.advance(Duration::from_secs(120));
    tokio::task::yield_now().await;
    assert!(!proposal.has_authority());
    assert_eq!(contracts.raw.items("contracts.").await.unwrap().len(), 1);
    bounded(proposal.close()).await.unwrap();
    bounded(participant.close()).await.unwrap();
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn selector_failures_are_explicit_and_blocked_selectors_do_not_block_shutdown() {
    let (concord, _, _, _) = setup().await;
    let participant = concord
        .participate(session("b"), None, |_candidates| async {
            Ok(vec![ContractPointer {
                contract_id: "unknown".into(),
                generation: 1,
            }])
        })
        .unwrap();
    eventually(|| participant.state().failure.is_some()).await;
    assert_eq!(
        participant.state().failure.unwrap().kind,
        FailureKind::Invalid
    );
    assert!(bounded(participant.close()).await.is_err());
    let blocked = concord
        .participate(session("c"), None, |_candidates| async {
            std::future::pending::<Result<Vec<ContractPointer>>>().await
        })
        .unwrap();
    eventually(|| blocked.state().phase == Phase::Ready).await;
    let _state = blocked.watch_state().unwrap();
    bounded(blocked.close()).await.unwrap();
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn different_live_sessions_are_never_adopted_or_cancelled_by_bootstrap() {
    let (concord, _, tokens, _) = setup().await;
    let a = session("a");
    let b = session("b");
    let participant = concord.participate(b.clone(), None, select_all).unwrap();
    let proposal = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    bounded(proposal.wait_ready(None)).await.unwrap();
    let contender = EndpointSession::with_id(a.endpoint().clone(), "contender").unwrap();
    assert!(concord.bootstrap(&contender, None, true).await.is_err());
    assert!(proposal.has_authority());
    let unowned = concord.participate(contender, None, select_all).unwrap();
    eventually(|| {
        unowned
            .state()
            .value
            .contracts
            .values()
            .any(|s| s.phase == Phase::Terminal)
    })
    .await;
    assert!(proposal.has_authority());
    bounded(unowned.close()).await.unwrap();
    assert_eq!(tokens.raw.items("contracts.").await.unwrap().len(), 2);
    bounded(proposal.close()).await.unwrap();
    bounded(participant.close()).await.unwrap();
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn peer_session_references_do_not_block_bootstrap_but_pending_token_ownership_does() {
    let (concord, contracts, _, _) = setup().await;
    let client = session("client");
    let provider = session("provider");
    let other = session("pending");
    let contender = EndpointSession::with_id(provider.endpoint().clone(), "new-provider").unwrap();
    let mut spec = deckr::concord::CreateContractSpec::new(vec![
        client.endpoint().clone(),
        provider.endpoint().clone(),
        other.endpoint().clone(),
    ]);
    spec.created_by = Some(client.endpoint().clone());
    spec.participant_sessions
        .insert(provider.endpoint().to_string(), provider.id().to_owned());
    let pending = concord.create_contract(spec).await.unwrap();
    concord
        .attach(&pending, client.endpoint(), client.id(), None)
        .await
        .unwrap();
    eventually(|| concord.contract_record_cached(&pending).unwrap().is_some()).await;
    let before = contracts.raw.get(&pending.key).await.unwrap();
    concord.bootstrap(&contender, None, true).await.unwrap();
    assert_eq!(contracts.raw.get(&pending.key).await.unwrap(), before);
    concord
        .attach(&pending, provider.endpoint(), provider.id(), None)
        .await
        .unwrap();
    let owned = contracts.raw.get(&pending.key).await.unwrap();
    assert_eq!(
        concord.validate(&pending, None).await.status,
        ContractValidityStatus::NotYetFulfilled
    );
    assert!(matches!(
        concord.bootstrap(&contender, None, true).await,
        Err(Error::StateConflict(_))
    ));
    assert_eq!(contracts.raw.get(&pending.key).await.unwrap(), owned);
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn cancelled_close_and_unknown_attachment_acknowledgements_keep_cleanup_owned() {
    let (concord, contracts, tokens, clock) = setup().await;
    let a = session("a");
    let b = session("b");
    let proposal = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    bounded(proposal.wait_for(|s| s.value.token.is_some(), None))
        .await
        .unwrap();
    tokens.stall_deletes.store(true, Ordering::SeqCst);
    let handle = proposal.agreement();
    let closing = tokio::spawn(async move { handle.close().await });
    eventually(|| proposal.state().phase == Phase::Closing).await;
    closing.abort();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    clock.advance(Duration::from_secs(5));
    eventually(|| proposal.state().phase == Phase::Closed).await;
    assert_eq!(
        proposal.state().failure.unwrap().kind,
        FailureKind::Deadline
    );
    assert!(!proposal.has_authority());
    assert_eq!(
        concord.validate(proposal.contract(), None).await.status,
        ContractValidityStatus::Cancelled
    );
    tokens.stall_deletes.store(false, Ordering::SeqCst);
    *tokens.stall_create.lock().unwrap() = Some("contracts.".into());
    let partial = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    let token_key = deckr::concord::concord_participant_token_key(
        &partial.contract().contract_id,
        1,
        a.endpoint(),
    );
    bounded(async {
        loop {
            if tokens.raw.get(&token_key).await.unwrap().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!partial.has_authority());
    bounded(partial.close()).await.unwrap();
    assert!(tokens.raw.get(&token_key).await.unwrap().is_none());
    assert_eq!(contracts.raw.items("contracts.").await.unwrap().len(), 2);
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn blocked_attachment_and_selector_do_not_stall_other_renewals() {
    let (concord, _, tokens, clock) = setup().await;
    let a = session("a");
    let b = session("b");
    let blocked_policy = Arc::new(AtomicBool::new(false));
    let observed_policy = Arc::new(AtomicBool::new(false));
    let policy_flag = blocked_policy.clone();
    let observed = observed_policy.clone();
    let participant = concord
        .participate(b.clone(), None, move |candidates| {
            let blocked = policy_flag.clone();
            let observed = observed.clone();
            async move {
                if blocked.load(Ordering::SeqCst) {
                    observed.store(true, Ordering::SeqCst);
                    std::future::pending::<()>().await;
                }
                select_all(candidates).await
            }
        })
        .unwrap();
    let stalled = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    *tokens.stall_create.lock().unwrap() = Some(deckr::keys::concord_contract_prefix(
        &stalled.contract().contract_id,
        1,
    ));
    let healthy = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    bounded(healthy.wait_ready(None)).await.unwrap();
    assert!(!stalled.has_authority());
    blocked_policy.store(true, Ordering::SeqCst);
    participant.wake_policy();
    eventually(|| observed_policy.load(Ordering::SeqCst)).await;
    clock.advance(Duration::from_secs(75));
    eventually(|| {
        healthy
            .state()
            .value
            .token
            .as_ref()
            .is_some_and(|token| token.refresh_seq == 2)
    })
    .await;
    assert!(healthy.has_authority());
    bounded(participant.close()).await.unwrap();
    bounded(stalled.close()).await.unwrap();
    bounded(healthy.close()).await.unwrap();
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn endpoint_close_revokes_authority_before_remote_cleanup_finishes() {
    let (concord, _, tokens, clock) = setup().await;
    let a = session("a");
    let b = session("b");
    let participant = concord.participate(b.clone(), None, select_all).unwrap();
    let proposal = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    bounded(proposal.wait_ready(None)).await.unwrap();
    tokens.stall_deletes.store(true, Ordering::SeqCst);
    a.close();
    assert!(!proposal.has_authority());
    assert!(!proposal.state().value.local_authority);
    for _ in 0..30 {
        tokio::task::yield_now().await;
    }
    clock.advance(Duration::from_secs(5));
    eventually(|| proposal.state().phase == Phase::Closed).await;
    assert!(bounded(proposal.close()).await.is_err());
    tokens.stall_deletes.store(false, Ordering::SeqCst);
    // The provider may have started cleanup before the injected stall ended.
    participant.request_close();
    for _ in 0..30 {
        tokio::task::yield_now().await;
    }
    clock.advance(Duration::from_secs(5));
    let _ = bounded(participant.close()).await;
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn partial_attachment_has_eight_cas_attempts_and_cleans_its_retained_identity() {
    let (concord, contracts, tokens, _) = setup().await;
    contracts.attachment_conflicts.store(20, Ordering::SeqCst);
    let a = session("a");
    let proposal = concord
        .propose(a.clone(), AgreementIntent::new(vec![a.endpoint().clone()]))
        .unwrap();
    eventually(|| proposal.state().phase == Phase::Terminal).await;
    bounded(async {
        loop {
            if tokens.raw.items("contracts.").await.unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(contracts.attachment_attempts.load(Ordering::SeqCst), 8);
    assert!(!proposal.has_authority());
    assert_eq!(
        proposal.state().failure.unwrap().mutation_code,
        Some(deckr::MutationCode::RevisionChanged)
    );
    bounded(proposal.close()).await.unwrap();
    assert_eq!(
        concord.validate(proposal.contract(), None).await.status,
        ContractValidityStatus::Cancelled
    );
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn proposal_collision_never_cancels_the_existing_record() {
    let (concord, contracts, _, _) = setup().await;
    let a = session("a");
    let proposal = concord
        .propose(a.clone(), AgreementIntent::new(vec![a.endpoint().clone()]))
        .unwrap();
    let mut spec = deckr::concord::CreateContractSpec::new(vec![a.endpoint().clone()]);
    spec.contract_id = Some(proposal.contract().contract_id.clone());
    // Commit before the newly spawned proposal task gets a turn.
    let exact = ConcordCoordinator::new(contracts.clone(), FaultStore::new(true));
    let existing = exact.create_contract(spec).await.unwrap();
    eventually(|| proposal.state().phase == Phase::Terminal).await;
    bounded(proposal.close()).await.unwrap();
    assert_eq!(
        exact
            .contract_record(&existing)
            .await
            .unwrap()
            .unwrap()
            .state,
        deckr::concord::ContractState::Open
    );
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn adapter_panics_revoke_authority_and_run_guarded_cleanup() {
    let (concord, _, tokens, clock) = setup().await;
    let a = session("a");
    let b = session("b");
    let participant = concord.participate(b.clone(), None, select_all).unwrap();
    let proposal = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    let owned = bounded(proposal.wait_ready(None))
        .await
        .unwrap()
        .token
        .unwrap();
    *tokens.panic_key.lock().unwrap() = Some(owned.key.clone());
    tokens.panic_updates.store(true, Ordering::SeqCst);
    clock.advance(Duration::from_secs(75));
    eventually(|| proposal.state().phase == Phase::Closed).await;
    assert!(!proposal.has_authority());
    assert!(proposal.state().failure.is_some());
    assert!(tokens.raw.get(&owned.key).await.unwrap().is_none());
    assert_eq!(
        concord.validate(proposal.contract(), None).await.status,
        ContractValidityStatus::Cancelled
    );
    assert!(bounded(proposal.close()).await.is_err());
    let _ = bounded(participant.close()).await;
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn withdrawn_domain_consent_cannot_restore_the_same_agreement() {
    let (concord, _, _, _) = setup().await;
    let a = session("a");
    let b = session("b");
    let consent = Arc::new(AtomicBool::new(true));
    let policy = consent.clone();
    let participant = concord
        .participate(b.clone(), None, move |candidates| {
            let accept = policy.load(Ordering::SeqCst);
            async move {
                if accept {
                    select_all(candidates).await
                } else {
                    Ok(Vec::new())
                }
            }
        })
        .unwrap();
    let first = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    bounded(first.wait_ready(None)).await.unwrap();
    consent.store(false, Ordering::SeqCst);
    participant.wake_policy();
    eventually(|| !first.has_authority()).await;
    consent.store(true, Ordering::SeqCst);
    participant.wake_policy();
    let successor = concord.propose(a.clone(), intent(&a, &b)).unwrap();
    bounded(successor.wait_ready(None)).await.unwrap();
    assert_ne!(
        first.contract().contract_id,
        successor.contract().contract_id
    );
    assert!(!first.has_authority());
    bounded(first.close()).await.unwrap();
    bounded(successor.close()).await.unwrap();
    bounded(participant.close()).await.unwrap();
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn malformed_existing_ownership_blocks_bootstrap_without_mutation() {
    let (concord, contracts, _, _) = setup().await;
    let a = session("a");
    let existing = concord
        .create_contract(deckr::concord::CreateContractSpec::new(vec![a
            .endpoint()
            .clone()]))
        .await
        .unwrap();
    let mut value = contracts
        .raw
        .get(&existing.key)
        .await
        .unwrap()
        .unwrap()
        .value;
    value["unexpected"] = serde_json::json!(true);
    contracts
        .raw
        .put(&existing.key, value.clone(), None)
        .await
        .unwrap();
    eventually(|| concord.contract_record_cached(&existing).is_err()).await;
    assert!(concord.bootstrap(&a, None, true).await.is_err());
    let proposal = concord
        .propose(a.clone(), AgreementIntent::new(vec![a.endpoint().clone()]))
        .unwrap();
    eventually(|| proposal.state().phase == Phase::Terminal).await;
    bounded(proposal.close()).await.unwrap();
    assert_eq!(contracts.raw.items("contracts.").await.unwrap().len(), 1);
    assert_eq!(
        contracts
            .raw
            .get(&existing.key)
            .await
            .unwrap()
            .unwrap()
            .value,
        value
    );
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn attachment_conflicts_wait_for_the_selected_contract() {
    let (concord, contracts, _, clock) = setup().await;
    contracts.attachment_conflicts.store(20, Ordering::SeqCst);
    contracts.conflict_unrelated.store(true, Ordering::SeqCst);
    let a = session("filtered-retry");
    let proposal = concord
        .propose(a.clone(), AgreementIntent::new(vec![a.endpoint().clone()]))
        .unwrap();
    eventually(|| contracts.attachment_attempts.load(Ordering::SeqCst) >= 1).await;
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert_eq!(contracts.attachment_attempts.load(Ordering::SeqCst), 1);
    clock.advance(Duration::from_secs(5));
    eventually(|| proposal.state().phase == Phase::Terminal).await;
    assert!(!proposal.has_authority());
    bounded(proposal.close()).await.unwrap();
    concord.close_sources_and_wait().await;
}
