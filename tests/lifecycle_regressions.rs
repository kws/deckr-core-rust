use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use deckr::beacon::{Beacon, BeaconAdvertiser};
use deckr::concord::{
    ConcordCoordinator, ConcordParticipantManager, ContractFilters, CreateContractSpec,
};
use deckr::endpoint::EndpointAddress;
use deckr::hardware::runtime::{
    HardwareCommandFuture, HardwareCommandHandler, HardwareCommandOutcome, HardwareLaneTransport,
    HardwareManagerRuntime, HardwareManagerRuntimeSpec, HardwareMessageStream,
};
use deckr::lanes::DeckrMessage;
use deckr::state::{
    MaterializedStateStore, MemoryStateStore, StateChange, StateEntry, StateSource, StateStore,
    StateWatchStream,
};
use deckr::{Error, Result};
use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{stream, StreamExt};
use serde_json::{json, Value};
use tokio::sync::Notify;

#[derive(Clone)]
struct ControlledStore {
    raw: MemoryStateStore,
    faults: Arc<Mutex<Vec<UnboundedSender<Result<StateChange>>>>>,
    paused: Arc<AtomicBool>,
    resume: Arc<Notify>,
    fail_updates: Arc<AtomicBool>,
    pause_create_reply: Arc<AtomicBool>,
    fail_deletes: Arc<AtomicBool>,
    stall_deletes: Arc<AtomicBool>,
    active_sources: Arc<AtomicUsize>,
}
struct SourceGuard(Arc<AtomicUsize>);
impl Drop for SourceGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ControlledStore {
    fn new(ttl: Option<u64>) -> Self {
        let raw = MemoryStateStore::new();
        raw.set_ttl_seconds(ttl).unwrap();
        Self {
            raw,
            faults: Arc::default(),
            paused: Arc::default(),
            resume: Arc::default(),
            fail_updates: Arc::default(),
            pause_create_reply: Arc::default(),
            fail_deletes: Arc::default(),
            stall_deletes: Arc::default(),
            active_sources: Arc::default(),
        }
    }
    fn fail_watch(&self) {
        self.faults.lock().unwrap().retain(|sender| {
            sender
                .unbounded_send(Err(Error::StateUnavailable(
                    "injected watch failure".into(),
                )))
                .is_ok()
        });
    }
    fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.resume.notify_waiters();
    }
}
impl StateStore for ControlledStore {
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        self.raw.ttl_seconds().await
    }
    async fn get(&self, key: &str) -> Result<Option<StateEntry>> {
        self.raw.get(key).await
    }
    async fn items(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        self.raw.items(prefix).await
    }
    async fn put(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        self.raw.put(key, value, ttl).await
    }
    async fn create(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        let entry = self.raw.create(key, value, ttl).await?;
        while self.pause_create_reply.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
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
        if self.fail_deletes.load(Ordering::SeqCst) {
            return Err(Error::StateUnavailable("injected delete failure".into()));
        }
        self.raw.delete_revision(key, revision).await
    }
    async fn watch(&self, prefix: &str) -> Result<StateWatchStream> {
        self.raw.watch(prefix).await
    }
    async fn open_source(&self, prefix: &str) -> Result<StateSource> {
        let source = self.raw.open_source(prefix).await?;
        let paused = self.paused.clone();
        let resume = self.resume.clone();
        let changes = source.changes.then(move |change| {
            let paused = paused.clone();
            let resume = resume.clone();
            async move {
                loop {
                    let notified = resume.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if !paused.load(Ordering::SeqCst) {
                        break;
                    }
                    notified.await;
                }
                change
            }
        });
        let (sender, receiver) = unbounded();
        self.faults.lock().unwrap().push(sender);
        self.active_sources.fetch_add(1, Ordering::SeqCst);
        let guard = SourceGuard(self.active_sources.clone());
        let changes = Box::pin(stream::select(Box::pin(changes), receiver));
        let changes = stream::unfold((changes, guard), |(mut changes, guard)| async move {
            let change = changes.next().await?;
            Some((change, (changes, guard)))
        });
        Ok(StateSource {
            entries: source.entries,
            revision: source.revision,
            replayed: source.replayed,
            changes: Box::pin(changes),
        })
    }
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn exact_mutations_wait_for_ordered_watch_observations() {
    let store = ControlledStore::new(None);
    let source = MaterializedStateStore::start(store.clone(), "")
        .await
        .unwrap();
    store.paused.store(true, Ordering::SeqCst);
    let writer = source.clone();
    let create = tokio::spawn(async move { writer.create("key", json!(1), None).await });
    // Inspect the raw commit without allowing its watch observation through.
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.get("key").await.unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!create.is_finished());
    assert!(source.get_cached("key").unwrap().is_none());
    store.resume();
    let created = create.await.unwrap().unwrap();
    assert_eq!(
        source.get_cached("key").unwrap().unwrap().revision,
        created.revision
    );

    store.paused.store(true, Ordering::SeqCst);
    let writer = source.clone();
    let delete = tokio::spawn(async move { writer.delete("key", Some(created.revision)).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.get("key").await.unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(source.get_cached("key").unwrap().is_some());
    assert!(!delete.is_finished());
    store.resume();
    delete.await.unwrap().unwrap();
    assert!(source.get_cached("key").unwrap().is_none());
    source.close_and_wait().await;
}

#[tokio::test]
async fn recovery_during_an_exact_write_does_not_hide_lost_continuity() {
    let store = ControlledStore::new(None);
    let source = MaterializedStateStore::start(store.clone(), "")
        .await
        .unwrap();
    store.pause_create_reply.store(true, Ordering::SeqCst);
    let writer = source.clone();
    let create = tokio::spawn(async move { writer.create("key", json!(1), None).await });
    wait_until(|| source.get_cached("key").unwrap().is_some()).await;
    store.fail_watch();
    wait_until(|| !source.is_current()).await;
    source.wait_current().await.unwrap();
    store.pause_create_reply.store(false, Ordering::SeqCst);
    assert!(matches!(
        create.await.unwrap(),
        Err(Error::MaterializedViewStale(_))
    ));
    assert!(source.get_cached("key").unwrap().is_some());
    source.close_and_wait().await;
}

#[tokio::test]
async fn unaccepted_candidate_can_be_admitted_later() {
    let concord = ConcordCoordinator::new(
        MemoryStateStore::new(),
        MemoryStateStore::ttl_bound(120).unwrap(),
    );
    let client = EndpointAddress::parse("controller:client").unwrap();
    let provider = EndpointAddress::parse("service:provider").unwrap();
    let contract = concord
        .create_contract(CreateContractSpec::new(vec![
            client.clone(),
            provider.clone(),
        ]))
        .await
        .unwrap();
    concord
        .attach(&contract, &client, "client-session", None)
        .await
        .unwrap();
    let mut manager =
        ConcordParticipantManager::new(concord, provider, "provider-session".into()).unwrap();
    assert!(manager
        .reconcile(|_, _| Ok(false), None)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        manager
            .reconcile(|_, _| Ok(true), None)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(manager.has_authority(&contract.key));
}

#[tokio::test]
async fn stale_source_permanently_revokes_authority_even_after_recovery() {
    let contracts = ControlledStore::new(None);
    let tokens = ControlledStore::new(Some(120));
    let c = MaterializedStateStore::start(contracts.clone(), "contracts.")
        .await
        .unwrap();
    let t = MaterializedStateStore::start(tokens.clone(), "contracts.")
        .await
        .unwrap();
    let concord = ConcordCoordinator::new(c.clone(), t.clone());
    let client = EndpointAddress::parse("controller:client").unwrap();
    let provider = EndpointAddress::parse("service:provider").unwrap();
    let spec = CreateContractSpec::new(vec![client.clone(), provider.clone()]);
    let contract = concord.create_contract(spec.clone()).await.unwrap();
    concord
        .attach(&contract, &client, "client-session", None)
        .await
        .unwrap();
    let mut manager =
        ConcordParticipantManager::new(concord.clone(), provider, "provider-session".into())
            .unwrap();
    assert!(manager
        .reconcile_cached(|_, _| Ok(true), None)
        .await
        .unwrap()[0]
        .validity
        .valid());
    // Keep exact records valid: cleanup cannot reach either store.
    contracts.fail_updates.store(true, Ordering::SeqCst);
    tokens.fail_deletes.store(true, Ordering::SeqCst);
    contracts.fail_watch();
    wait_until(|| !c.is_current()).await;
    assert!(!manager.has_authority(&contract.key));
    assert!(manager.managed_contracts().is_empty());
    tokio::time::timeout(Duration::from_secs(2), c.wait_current())
        .await
        .unwrap()
        .unwrap();
    assert!(concord.validate(&contract, None).await.valid());
    assert!(manager
        .reconcile_managed_cached(None)
        .await
        .unwrap()
        .is_empty());
    assert!(manager
        .reconcile_cached(|_, _| Ok(true), None)
        .await
        .unwrap()
        .is_empty());
    contracts.fail_updates.store(false, Ordering::SeqCst);
    tokens.fail_deletes.store(false, Ordering::SeqCst);
    let successor = concord.create_contract(spec).await.unwrap();
    concord
        .attach(&successor, &client, "client-session", None)
        .await
        .unwrap();
    let managed = manager
        .reconcile_cached(|_, _| Ok(true), None)
        .await
        .unwrap();
    assert!(managed
        .iter()
        .any(|m| m.contract.contract_id == successor.contract_id && m.validity.valid()));
    c.close_and_wait().await;
    t.close_and_wait().await;
}

#[tokio::test]
async fn failed_required_renewal_cannot_resume_the_same_agreement() {
    let contracts = ControlledStore::new(None);
    let tokens = ControlledStore::new(Some(1));
    let concord = ConcordCoordinator::new(contracts.clone(), tokens.clone());
    let client = EndpointAddress::parse("controller:client").unwrap();
    let provider = EndpointAddress::parse("service:provider").unwrap();
    let contract = concord
        .create_contract(CreateContractSpec::new(vec![
            client.clone(),
            provider.clone(),
        ]))
        .await
        .unwrap();
    concord
        .attach(&contract, &client, "client-session", None)
        .await
        .unwrap();
    let mut manager =
        ConcordParticipantManager::new(concord, provider, "provider-session".into()).unwrap();
    assert!(manager.reconcile(|_, _| Ok(true), None).await.unwrap()[0]
        .validity
        .valid());
    let mut other_handle = manager.clone();
    tokio::time::sleep(Duration::from_millis(800)).await;
    contracts.fail_updates.store(true, Ordering::SeqCst);
    tokens.fail_updates.store(true, Ordering::SeqCst);
    tokens.fail_deletes.store(true, Ordering::SeqCst);
    assert!(manager.reconcile_managed(None).await.is_err());
    assert!(!manager.has_authority(&contract.key));
    assert!(manager.managed_contracts().is_empty());
    assert!(!other_handle.has_authority(&contract.key));
    tokens.fail_updates.store(false, Ordering::SeqCst);
    assert!(manager.reconcile_managed(None).await.unwrap().is_empty());
    assert!(other_handle
        .reconcile_managed(None)
        .await
        .unwrap()
        .is_empty());
}

#[derive(Clone)]
struct NullLane;
impl HardwareLaneTransport for NullLane {
    async fn publish_hardware_message(&self, _: DeckrMessage) -> Result<()> {
        Ok(())
    }
    async fn subscribe_hardware_messages<'a>(
        &'a self,
        _: &'a EndpointAddress,
    ) -> Result<HardwareMessageStream> {
        Ok(Box::pin(stream::pending()))
    }
}
struct NullHandler;
impl HardwareCommandHandler for NullHandler {
    fn handle_hardware_command<'a>(&'a self, _: DeckrMessage) -> HardwareCommandFuture<'a> {
        Box::pin(async { Ok(HardwareCommandOutcome::Handled) })
    }
}

#[tokio::test]
async fn endpoint_lifetimes_share_sources_without_closing_each_other() {
    let contracts = ControlledStore::new(None);
    let tokens = ControlledStore::new(Some(120));
    let beacon = ControlledStore::new(Some(300));
    let protocols =
        deckr::runtime::ProtocolRuntime::start(beacon.clone(), contracts.clone(), tokens.clone())
            .await
            .unwrap();
    let manager = |id: &str| {
        HardwareManagerRuntime::with_concord(
            HardwareManagerRuntimeSpec {
                manager_id: id.into(),
                session_id: format!("{id}-session"),
                labels: Default::default(),
                beacon_state: beacon.clone(),
                concord_contract_state: contracts.clone(),
                concord_token_state: tokens.clone(),
                lane: NullLane,
                maintenance_policy: Default::default(),
                command_handler: Arc::new(NullHandler),
                reset_handler: None,
            },
            protocols.concord().clone(),
        )
        .unwrap()
    };
    let first = manager("first");
    let second = manager("second");
    let mut tasks = tokio::task::JoinSet::new();
    first.start(&mut tasks).await.unwrap();
    second.start(&mut tasks).await.unwrap();
    for store in [&beacon, &contracts, &tokens] {
        assert_eq!(store.active_sources.load(Ordering::SeqCst), 1);
    }
    first.stop().await.unwrap();
    assert!(protocols.concord().is_current());
    assert!(protocols.beacon().materialized_store().is_current());
    assert_eq!(contracts.active_sources.load(Ordering::SeqCst), 1);
    second.stop().await.unwrap();
    protocols.close().await;
    for store in [&beacon, &contracts, &tokens] {
        assert_eq!(store.active_sources.load(Ordering::SeqCst), 0);
    }
    while tasks.join_next().await.is_some() {}
}

#[tokio::test]
async fn stop_joins_workers_and_closes_owned_sources_with_blocked_cleanup() {
    let contracts = ControlledStore::new(None);
    let tokens = ControlledStore::new(Some(120));
    let beacon = ControlledStore::new(Some(300));
    let runtime = HardwareManagerRuntime::new(HardwareManagerRuntimeSpec {
        manager_id: "manager".into(),
        session_id: "session".into(),
        labels: Default::default(),
        beacon_state: beacon.clone(),
        concord_contract_state: contracts.clone(),
        concord_token_state: tokens.clone(),
        lane: NullLane,
        maintenance_policy: Default::default(),
        command_handler: Arc::new(NullHandler),
        reset_handler: None,
    })
    .await
    .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    runtime.start(&mut tasks).await.unwrap();
    beacon.stall_deletes.store(true, Ordering::SeqCst);
    assert!(tokio::time::timeout(Duration::from_secs(6), runtime.stop())
        .await
        .unwrap()
        .is_err());
    drop(runtime);
    assert_eq!(contracts.active_sources.load(Ordering::SeqCst), 0);
    assert_eq!(tokens.active_sources.load(Ordering::SeqCst), 0);
    tokio::time::timeout(Duration::from_secs(1), async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn snapshots_are_atomic_bounded_and_do_not_evict_existing_readers() {
    let source = MaterializedStateStore::start(MemoryStateStore::new(), "")
        .await
        .unwrap();
    let mut watch = source.watch_snapshots("").unwrap();
    source.create("first", json!(1), None).await.unwrap();
    let initial = watch.next().await.unwrap();
    assert!(initial.entries.is_empty());
    assert!(initial.current);
    let next = watch.next().await.unwrap();
    assert_eq!(next.entries.len(), 1);
    assert!(next.version > initial.version);
    for i in 0..257 {
        source
            .create(&format!("key-{i}"), json!(i), None)
            .await
            .unwrap();
    }
    let overflow = watch.next().await.unwrap();
    assert!(overflow.resnapshot_required);
    assert!(overflow.changed.is_empty());
    assert_eq!(overflow.entries.len(), 258);
    let readers: Vec<_> = (0..255)
        .map(|_| source.watch_snapshots("").unwrap())
        .collect();
    assert!(source.watch_snapshots("").is_err());
    source.create("last", json!(0), None).await.unwrap();
    assert_eq!(watch.next().await.unwrap().entries.len(), 259);
    drop(readers);
    assert!(source.watch_snapshots("").is_ok());
    source.close_and_wait().await;
    let closed = watch.next().await.unwrap();
    assert!(closed.closed);
    assert!(!closed.current);
    assert!(watch.next().await.is_none());
}

#[tokio::test]
async fn recovery_atomically_removes_missed_deletions_and_exposes_epoch_change() {
    let store = ControlledStore::new(None);
    let source = MaterializedStateStore::start(store.clone(), "")
        .await
        .unwrap();
    source.create("gone", json!(1), None).await.unwrap();
    let before = source.authority_epoch();
    let mut snapshots = source.watch_snapshots("").unwrap();
    assert!(snapshots.next().await.unwrap().current);
    store.paused.store(true, Ordering::SeqCst);
    store.delete("gone", None).await.unwrap();
    store.fail_watch();
    wait_until(|| !source.is_current()).await;
    source.wait_current().await.unwrap();
    assert!(source.get_cached("gone").unwrap().is_none());
    assert_ne!(source.authority_epoch(), before);
    // A slow reader must still observe the interruption after recovery finished.
    assert!(!snapshots.next().await.unwrap().current);
    assert!(snapshots.next().await.unwrap().current);
    store.resume();
    source.close_and_wait().await;
}

#[tokio::test]
async fn protocol_watches_start_with_complete_filtered_membership() {
    let store = MemoryStateStore::ttl_bound(300).unwrap();
    let beacon = Beacon::start(store.clone()).await.unwrap();
    let advertiser = BeaconAdvertiser::new(
        store,
        "org.example.feature",
        EndpointAddress::parse_protocol("worker:main").unwrap(),
        "session",
    );
    advertiser.publish().await.unwrap();
    wait_until(|| beacon.candidates("org.example.feature").unwrap().len() == 1).await;
    let mut watch = beacon.watch_state("org.example.feature").unwrap();
    let snapshot = watch.next().await.unwrap();
    assert!(snapshot.current);
    assert_eq!(snapshot.candidates.len(), 1);
    beacon.materialized_store().close_and_wait().await;

    let c = MaterializedStateStore::start(MemoryStateStore::new(), "contracts.")
        .await
        .unwrap();
    let t = MaterializedStateStore::start(MemoryStateStore::ttl_bound(120).unwrap(), "contracts.")
        .await
        .unwrap();
    let concord = ConcordCoordinator::new(c, t);
    let mut spec = CreateContractSpec::new(vec![
        EndpointAddress::parse_protocol("worker:main").unwrap(),
        EndpointAddress::parse_protocol("scheduler:main").unwrap(),
    ]);
    spec.profile = Some("org.example.work.v1".into());
    concord.create_contract(spec).await.unwrap();
    let mut watch = concord
        .watch_state(ContractFilters {
            profile: Some("org.example.work.v1"),
            ..Default::default()
        })
        .unwrap();
    let snapshot = watch.next().await.unwrap();
    assert!(snapshot.current);
    assert_eq!(snapshot.contracts.len(), 1);
    assert!(snapshot.changed.is_empty());
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn managed_contexts_do_not_recover_old_authority_when_exact_records_stay_valid() {
    use deckr::concord::{AgreementIntent, ParticipantCandidates};
    use deckr::session::EndpointSession;
    let contracts = ControlledStore::new(None);
    let tokens = ControlledStore::new(Some(120));
    let c = MaterializedStateStore::start(contracts.clone(), "contracts.")
        .await
        .unwrap();
    let t = MaterializedStateStore::start(tokens.clone(), "contracts.")
        .await
        .unwrap();
    let concord = ConcordCoordinator::new(c.clone(), t.clone());
    let a = EndpointSession::new(EndpointAddress::parse_protocol("worker:a").unwrap());
    let b = EndpointSession::new(EndpointAddress::parse_protocol("worker:b").unwrap());
    let participant = concord
        .participate(
            b.clone(),
            None,
            |candidates: ParticipantCandidates| async move {
                Ok(candidates
                    .contracts
                    .into_iter()
                    .map(|member| deckr::authority::ContractPointer {
                        contract_id: member.contract.contract_id,
                        generation: member.contract.generation,
                    })
                    .collect())
            },
        )
        .unwrap();
    let proposal = concord
        .propose(
            a.clone(),
            AgreementIntent::new(vec![a.endpoint().clone(), b.endpoint().clone()]),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), proposal.wait_ready(None))
        .await
        .unwrap()
        .unwrap();
    let pointer = deckr::authority::ContractPointer {
        contract_id: proposal.contract().contract_id.clone(),
        generation: 1,
    };
    let accepted = participant.agreement(&pointer).unwrap();
    contracts.fail_updates.store(true, Ordering::SeqCst);
    tokens.fail_deletes.store(true, Ordering::SeqCst);
    contracts.fail_watch();
    wait_until(|| !c.is_current()).await;
    assert!(!proposal.has_authority());
    assert!(!accepted.has_authority());
    tokio::time::timeout(Duration::from_secs(2), c.wait_current())
        .await
        .unwrap()
        .unwrap();
    assert!(concord.validate(proposal.contract(), None).await.valid());
    assert!(!proposal.has_authority());
    assert!(!accepted.has_authority());
    contracts.fail_updates.store(false, Ordering::SeqCst);
    tokens.fail_deletes.store(false, Ordering::SeqCst);
    let _ = proposal.close().await;
    let _ = participant.close().await;
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn shared_concord_index_preserves_interruption_and_removes_missing_members_after_recovery() {
    let contracts = ControlledStore::new(None);
    let tokens = ControlledStore::new(Some(120));
    let c = MaterializedStateStore::start(contracts.clone(), "contracts.")
        .await
        .unwrap();
    let t = MaterializedStateStore::start(tokens, "contracts.")
        .await
        .unwrap();
    let concord = ConcordCoordinator::new(c.clone(), t.clone());
    let contract = concord
        .create_contract(CreateContractSpec::new(vec![
            EndpointAddress::parse_protocol("worker:recover").unwrap(),
        ]))
        .await
        .unwrap();
    let mut watch = concord.watch_state(Default::default()).unwrap();
    assert_eq!(watch.next().await.unwrap().contracts.len(), 1);
    contracts.fail_watch();
    tokio::time::timeout(Duration::from_secs(2), async {
        while c.is_current() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    contracts.raw.delete(&contract.key, None).await.unwrap();
    c.wait_current().await.unwrap();
    // A synchronous query may synchronize the index ahead of its worker. The
    // queued watch must still expose lost currentness before recovered state.
    assert!(concord
        .contracts_cached(Default::default())
        .unwrap()
        .is_empty());
    let lost = watch.next().await.unwrap();
    assert!(!lost.current);
    let recovered = watch.next().await.unwrap();
    assert!(recovered.current);
    assert!(recovered.contracts.is_empty());
    concord.close_sources_and_wait().await;
}
