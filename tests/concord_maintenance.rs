mod support;
use deckr::concord::*;
use deckr::endpoint::EndpointAddress;
use deckr::keys::{concord_contract_key, concord_participant_token_key};
use deckr::lifecycle::{Clock, Phase};
use deckr::state::{MemoryStateStore, StateEntry, StateStore, StateWatchStream};
use deckr::{Error, Result};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use support::ManualClock;

#[derive(Clone)]
struct Store {
    raw: MemoryStateStore,
    calls: Arc<AtomicUsize>,
    watches: Arc<AtomicUsize>,
    unavailable: Arc<AtomicBool>,
    foreign: Arc<Mutex<Option<StateEntry>>>,
    delete_mode: Arc<AtomicUsize>,
    deletes: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
    pause_get: Arc<AtomicBool>,
    get_entered: Arc<AtomicBool>,
    get_release: Arc<tokio::sync::Semaphore>,
}
impl Store {
    fn new(raw: MemoryStateStore) -> Self {
        Self {
            raw,
            calls: Arc::default(),
            watches: Arc::default(),
            unavailable: Arc::default(),
            foreign: Arc::default(),
            delete_mode: Arc::default(),
            deletes: Arc::default(),
            cancelled: Arc::default(),
            pause_get: Arc::default(),
            get_entered: Arc::default(),
            get_release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
    fn check(&self) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.unavailable.load(Ordering::SeqCst) {
            Err(Error::StateUnavailable("test store unavailable".into()))
        } else {
            Ok(())
        }
    }
}
struct Cancelled(Arc<AtomicUsize>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl StateStore for Store {
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.raw.ttl_seconds().await
    }
    async fn get(&self, key: &str) -> Result<Option<StateEntry>> {
        self.check()?;
        let result = self.raw.get(key).await;
        if self.pause_get.swap(false, Ordering::SeqCst) {
            self.get_entered.store(true, Ordering::SeqCst);
            self.get_release.acquire().await.unwrap().forget();
        }
        result
    }
    async fn items(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        self.check()?;
        let mut entries = self.raw.items(prefix).await?;
        if let Some(foreign) = self.foreign.lock().unwrap().clone() {
            entries.push(foreign);
        }
        Ok(entries)
    }
    async fn put(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        self.check()?;
        self.raw.put(key, value, ttl).await
    }
    async fn create(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        self.check()?;
        self.raw.create(key, value, ttl).await
    }
    async fn update(
        &self,
        key: &str,
        value: Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<StateEntry> {
        self.check()?;
        self.raw.update(key, value, revision, ttl).await
    }
    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        self.check()?;
        self.deletes.fetch_add(1, Ordering::SeqCst);
        let mode = self.delete_mode.load(Ordering::SeqCst);
        match mode {
            1 => Err(Error::StateConflict("test revision race".into())),
            2 => Err(Error::StateUnavailable("test delete unavailable".into())),
            3 => {
                self.raw.delete(key, revision).await?;
                Err(Error::StateUnavailable(
                    "delete committed without acknowledgement".into(),
                ))
            }
            4 => {
                self.raw.delete(key, revision).await?;
                let _cancelled = Cancelled(self.cancelled.clone());
                std::future::pending().await
            }
            5 => {
                self.delete_mode.store(0, Ordering::SeqCst);
                let mut current = self.raw.get(key).await?.unwrap();
                current.value["cancelReason"] = json!("changed concurrently");
                self.raw
                    .update(key, current.value, current.revision, None)
                    .await?;
                Err(Error::StateConflict(
                    "contract changed before deletion".into(),
                ))
            }
            _ => self.raw.delete(key, revision).await,
        }
    }
    async fn watch(&self, prefix: &str) -> Result<StateWatchStream> {
        self.watches.fetch_add(1, Ordering::SeqCst);
        self.raw.watch(prefix).await
    }
}
#[derive(Default)]
struct Audit {
    events: Mutex<Vec<MaintenanceAuditEvent>>,
    fail: AtomicBool,
    panic: AtomicBool,
    block: AtomicBool,
    entered: AtomicBool,
    cancelled: Arc<AtomicUsize>,
}
impl MaintenanceAudit for Audit {
    fn record(&self, event: MaintenanceAuditEvent) -> MaintenanceAuditFuture<'_> {
        Box::pin(async move {
            self.entered.store(true, Ordering::SeqCst);
            if self.panic.swap(false, Ordering::SeqCst) {
                panic!("audit panic");
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::StateUnavailable("audit unavailable".into()));
            }
            if self.block.load(Ordering::SeqCst) {
                let _cancelled = Cancelled(self.cancelled.clone());
                std::future::pending::<()>().await;
            }
            self.events.lock().unwrap().push(event);
            Ok(())
        })
    }
}
struct Harness {
    contracts: Store,
    tokens: Store,
    markers: Store,
    clock: Arc<ManualClock>,
    maintenance: ConcordMaintenance<Store, Store, Store>,
    audit: Arc<Audit>,
}
impl Harness {
    fn new() -> Self {
        let contracts = Store::new(MemoryStateStore::default());
        let tokens = Store::new(MemoryStateStore::ttl_bound(30).unwrap());
        let markers = Store::new(MemoryStateStore::default());
        let clock = Arc::new(ManualClock::default());
        let maintenance =
            ConcordMaintenance::new(contracts.clone(), tokens.clone(), markers.clone())
                .with_clock(clock.clone())
                .with_options(MaintenanceOptions {
                    stale_grace: Duration::from_secs(10),
                    retention: Duration::from_secs(20),
                    operation_timeout: Duration::from_secs(5),
                })
                .unwrap();
        Self {
            contracts,
            tokens,
            markers,
            clock,
            maintenance,
            audit: Arc::default(),
        }
    }
    async fn contract(&self, id: &str, attached: bool) -> StateEntry {
        self.contracts.raw.create(&concord_contract_key(id,1),json!({"contractId":id,"generation":1,"participants":["app:a","app:b"],"attachedParticipants":if attached {vec!["app:a","app:b"]} else {vec![]},"state":"open","createdAt":self.clock.utc_now().to_rfc3339()}),None).await.unwrap()
    }
    async fn cancelled(&self, id: &str) -> StateEntry {
        let mut entry = self.contract(id, true).await;
        entry.value["state"] = json!("cancelled");
        entry.value["cancelledAt"] = json!(self.clock.utc_now().to_rfc3339());
        entry.value["cancelledBy"] = json!("app:a");
        entry.value["cancelReason"] = json!("test");
        self.contracts
            .raw
            .update(&entry.key, entry.value, entry.revision, None)
            .await
            .unwrap()
    }
    async fn token(&self, id: &str, participant: &str, hash: Option<String>) -> StateEntry {
        self.tokens.raw.create(&concord_participant_token_key(id,1,&EndpointAddress::parse_protocol(participant).unwrap()),json!({"contractId":id,"generation":1,"participant":participant,"sessionId":"session","tokenId":"token","refreshSeq":1,"ttlSeconds":30,"termsHash":hash}),None).await.unwrap()
    }
    async fn scan(&self) -> MaintenanceReport {
        self.maintenance.scan(self.audit.as_ref()).await.unwrap()
    }
    fn advance(&self, seconds: u64) {
        self.clock.advance(Duration::from_secs(seconds));
    }
    async fn exists(&self, id: &str) -> bool {
        self.contracts
            .raw
            .get(&concord_contract_key(id, 1))
            .await
            .unwrap()
            .is_some()
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn maintenance_is_lazy_and_pending_live_tokens_reset_stale_observation() {
    let h = Harness::new();
    assert_eq!(
        h.contracts.calls.load(Ordering::SeqCst)
            + h.tokens.calls.load(Ordering::SeqCst)
            + h.markers.calls.load(Ordering::SeqCst),
        0
    );
    h.contract("pending", false).await;
    let first = h.scan().await;
    assert_eq!(first.stale_observations_created, 1);
    assert_eq!(first.contracts_cancelled, 0);
    h.token("pending", "app:a", None).await;
    h.advance(11);
    let live = h.scan().await;
    assert_eq!(live.stale_observations_removed, 1);
    assert_eq!(live.contracts_cancelled, 0);
    assert!(h.exists("pending").await);
    assert_eq!(
        h.contracts.watches.load(Ordering::SeqCst)
            + h.tokens.watches.load(Ordering::SeqCst)
            + h.markers.watches.load(Ordering::SeqCst),
        0
    );
    h.tokens
        .raw
        .delete(
            &concord_participant_token_key(
                "pending",
                1,
                &EndpointAddress::parse_protocol("app:a").unwrap(),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(h.scan().await.stale_observations_created, 1);
    h.advance(9);
    assert_eq!(h.scan().await.contracts_cancelled, 0);
    h.advance(1);
    assert_eq!(h.scan().await.contracts_cancelled, 1);
    let entry = h
        .contracts
        .raw
        .get(&concord_contract_key("pending", 1))
        .await
        .unwrap()
        .unwrap();
    let record = ContractRecord::from_value(entry.value).unwrap();
    assert_eq!(record.state, ContractState::Cancelled);
    assert_eq!(record.cancelled_by.unwrap().as_str(), "concord:maintenance");
    assert_eq!(
        record.cancel_reason.as_deref(),
        Some(CONCORD_REAPER_STALE_REASON)
    );
}

#[tokio::test]
async fn unavailable_tokens_are_not_staleness_evidence_and_recovery_can_preserve_contract() {
    let h = Harness::new();
    h.contract("waiting", false).await;
    h.scan().await;
    h.advance(11);
    h.tokens.unavailable.store(true, Ordering::SeqCst);
    let unavailable = h.scan().await;
    assert_eq!(unavailable.contracts_cancelled, 0);
    assert!(!unavailable.failures.is_empty());
    h.tokens.unavailable.store(false, Ordering::SeqCst);
    h.token("waiting", "app:b", None).await;
    assert_eq!(h.scan().await.stale_observations_removed, 1);
    assert!(h.exists("waiting").await);
}

#[tokio::test]
async fn retention_audits_metadata_before_deletion_and_reports_completed_cleanup() {
    let h = Harness::new();
    let mut contract = h.cancelled("retained / generation").await;
    let terms = json!({"secret":"do not copy full terms into audit"});
    let hash = deckr::canonical_json::canonical_json_hash_value(&terms).unwrap();
    contract.value["terms"] = terms;
    contract.value["termsHash"] = json!(hash);
    h.contracts
        .raw
        .update(&contract.key, contract.value, contract.revision, None)
        .await
        .unwrap();
    h.token("retained / generation", "app:a", Some(hash.clone()))
        .await;
    h.token("retained / generation", "app:b", Some(hash.clone()))
        .await;
    h.advance(19);
    assert_eq!(h.scan().await.contracts_deleted, 0);
    assert!(h.audit.events.lock().unwrap().is_empty());
    h.advance(1);
    let report = h.scan().await;
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(
        (
            report.contracts_deleted,
            report.tokens_deleted,
            report.cleanups_completed
        ),
        (1, 2, 1)
    );
    assert!(report.cleanup_pending.is_empty());
    assert!(!h.exists("retained / generation").await);
    assert!(h.markers.raw.items("").await.unwrap().is_empty());
    let events = h.audit.events.lock().unwrap();
    let MaintenanceAuditEvent::BeforeContractDeletion(audit) = &events[0] else {
        panic!("deletion audit expected")
    };
    assert_eq!(audit.listed_token_keys, 2);
    assert_eq!(audit.tokens.len(), 2);
    assert_eq!(audit.terms_hash, Some(hash));
    assert_eq!(audit.status, ContractValidityStatus::Cancelled);
    assert_eq!(audit.participants.len(), 2);
    assert_eq!(audit.cancel_reason.as_deref(), Some("test"));
    assert!(!serde_json::to_string(audit)
        .unwrap()
        .contains("do not copy"));
}

#[tokio::test]
async fn malformed_or_foreign_token_lists_block_retained_contract_deletion() {
    let h = Harness::new();
    h.cancelled("target").await;
    h.advance(20);
    let other = h.token("other", "app:a", None).await;
    *h.tokens.foreign.lock().unwrap() = Some(other.clone());
    let report = h.scan().await;
    assert_eq!(report.contracts_deleted, 0);
    assert!(!report.failures.is_empty());
    assert!(h.exists("target").await);
    assert!(h.audit.events.lock().unwrap().is_empty());
    assert!(h.tokens.raw.get(&other.key).await.unwrap().is_some());
    *h.tokens.foreign.lock().unwrap() = None;
    let malformed = h.token("target", "app:b", None).await;
    let mut value = malformed.value;
    value["contractId"] = json!("other");
    h.tokens
        .raw
        .update(&malformed.key, value, malformed.revision, None)
        .await
        .unwrap();
    assert_eq!(h.scan().await.contracts_deleted, 0);
    assert!(h.markers.raw.items("cleanup.").await.unwrap().is_empty());
}

#[tokio::test]
async fn cleanup_retries_at_most_eight_passes_and_resumes_without_contract_record() {
    let h = Harness::new();
    h.cancelled("cleanup").await;
    h.token("cleanup", "app:a", None).await;
    h.advance(20);
    h.tokens.delete_mode.store(1, Ordering::SeqCst);
    let first = h.scan().await;
    assert_eq!(first.contracts_deleted, 1);
    assert_eq!(first.tokens_deleted, 0);
    assert_eq!(first.cleanup_pending.len(), 1);
    assert_eq!(h.tokens.deletes.load(Ordering::SeqCst), 8);
    assert_eq!(
        h.audit
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, MaintenanceAuditEvent::Conflict { .. }))
            .count(),
        8
    );
    h.tokens.delete_mode.store(0, Ordering::SeqCst);
    let resumed = h.scan().await;
    assert_eq!(
        (
            resumed.contracts_scanned,
            resumed.tokens_deleted,
            resumed.cleanups_completed
        ),
        (0, 1, 1)
    );
    assert!(resumed.cleanup_pending.is_empty());
    assert!(h.markers.raw.items("cleanup.").await.unwrap().is_empty());
}

#[tokio::test]
async fn contract_cas_conflict_retains_marker_and_next_scan_rechecks_new_revision() {
    let h = Harness::new();
    h.cancelled("race").await;
    h.token("race", "app:a", None).await;
    h.advance(20);
    h.contracts.delete_mode.store(5, Ordering::SeqCst);
    let first = h.scan().await;
    assert_eq!(first.contracts_deleted, 0);
    assert!(!first.failures.is_empty());
    assert!(h.exists("race").await);
    assert_eq!(h.markers.raw.items("cleanup.").await.unwrap().len(), 1);
    let next = h.scan().await;
    assert_eq!((next.contracts_deleted, next.tokens_deleted), (1, 1));
    assert!(next.failures.is_empty(), "{:?}", next.failures);
}

#[tokio::test]
async fn committed_delete_without_acknowledgement_leaves_durable_cleanup_for_next_scan() {
    let h = Harness::new();
    h.cancelled("lost-ack").await;
    h.token("lost-ack", "app:a", None).await;
    h.advance(20);
    h.contracts.delete_mode.store(3, Ordering::SeqCst);
    let first = h.scan().await;
    assert_eq!(first.contracts_deleted, 0);
    assert!(!h.exists("lost-ack").await);
    assert_eq!(first.cleanup_pending.len(), 1);
    let next = h.scan().await;
    assert_eq!((next.tokens_deleted, next.cleanups_completed), (1, 1));
    assert!(next.cleanup_pending.is_empty());
}

#[tokio::test]
async fn invalid_open_contract_is_cancelled_without_repairing_its_application_data() {
    let h = Harness::new();
    let entry = h.contract("invalid", true).await;
    let mut value = entry.value;
    value["attachedParticipants"] = json!(["app:unlisted"]);
    h.contracts
        .raw
        .update(&entry.key, value, entry.revision, None)
        .await
        .unwrap();
    assert_eq!(h.scan().await.stale_observations_created, 1);
    h.advance(10);
    assert_eq!(h.scan().await.contracts_cancelled, 1);
    let cancelled = h.contracts.raw.get(&entry.key).await.unwrap().unwrap();
    assert_eq!(
        cancelled.value["attachedParticipants"],
        json!(["app:unlisted"])
    );
    assert_eq!(cancelled.value["state"], json!("cancelled"));
}

#[tokio::test]
async fn marker_identity_and_audit_failure_prevent_deletion_and_orphans_are_guarded() {
    let h = Harness::new();
    h.cancelled("protected").await;
    h.advance(20);
    h.audit.fail.store(true, Ordering::SeqCst);
    assert_eq!(h.scan().await.contracts_deleted, 0);
    assert!(h.markers.raw.items("cleanup.").await.unwrap().is_empty());
    h.audit.fail.store(false, Ordering::SeqCst);
    let key = concord_token_cleanup_key("protected", 1);
    h.markers.raw.create(&key,json!({"contractId":"wrong","generation":1,"contractRevision":1,"cleanupStartedAt":h.clock.utc_now().to_rfc3339()}),None).await.unwrap();
    assert_eq!(h.scan().await.contracts_deleted, 0);
    assert!(h.exists("protected").await);
    let orphan = concord_stale_observation_key("orphan", 1);
    h.markers.raw.create(&orphan,json!({"contractId":"orphan","generation":1,"firstObservedStaleAt":h.clock.utc_now().to_rfc3339(),"status":"missing_token"}),None).await.unwrap();
    assert_eq!(h.scan().await.stale_observations_removed, 1);
    assert!(h.markers.raw.get(&orphan).await.unwrap().is_none());
    assert!(h.markers.raw.get(&key).await.unwrap().is_some());
}

#[tokio::test]
async fn managed_close_cancels_pending_delete_and_retains_resume_marker_without_closing_stores() {
    let h = Harness::new();
    h.cancelled("interrupted").await;
    h.token("interrupted", "app:a", None).await;
    h.advance(20);
    h.contracts.delete_mode.store(4, Ordering::SeqCst);
    let managed = h
        .maintenance
        .manage(h.audit.clone(), Duration::from_secs(300))
        .unwrap();
    until(|| h.contracts.deletes.load(Ordering::SeqCst) > 0).await;
    let closer = managed.clone();
    let close = tokio::spawn(async move { closer.close().await });
    until(|| matches!(managed.state().phase, Phase::Closing | Phase::Closed)).await;
    close.abort();
    tokio::time::timeout(Duration::from_secs(2), managed.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(managed.state().phase, Phase::Closed);
    assert!(managed.request_scan().is_err());
    assert_eq!(h.contracts.cancelled.load(Ordering::SeqCst), 1);
    assert!(!h.exists("interrupted").await);
    assert_eq!(h.markers.raw.items("cleanup.").await.unwrap().len(), 1);
    h.contracts.delete_mode.store(0, Ordering::SeqCst);
    assert_eq!(h.scan().await.cleanups_completed, 1);
    h.contract("unrelated", false).await;
    assert!(h.exists("unrelated").await);
}

#[tokio::test]
async fn managed_last_owner_drop_cancels_audit_and_panics_are_observable_and_recoverable() {
    let h = Harness::new();
    h.cancelled("audit").await;
    h.advance(20);
    h.audit.panic.store(true, Ordering::SeqCst);
    let managed = h
        .maintenance
        .manage(h.audit.clone(), Duration::from_secs(300))
        .unwrap();
    managed
        .wait_for(|state| state.phase == Phase::Unavailable, None)
        .await
        .unwrap();
    assert!(managed.state().failure.is_some());
    assert!(h.exists("audit").await);
    h.audit.entered.store(false, Ordering::SeqCst);
    h.audit.block.store(true, Ordering::SeqCst);
    managed.request_scan().unwrap();
    until(|| h.audit.entered.load(Ordering::SeqCst)).await;
    let mut watch = managed.watch_state().unwrap();
    drop(managed);
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(state) = watch.next().await {
            if state.phase == Phase::Closed {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(h.audit.cancelled.load(Ordering::SeqCst), 1);
    assert!(h.exists("audit").await);
    h.audit.block.store(false, Ordering::SeqCst);
    let successor = h
        .maintenance
        .manage(h.audit.clone(), Duration::from_secs(300))
        .unwrap();
    successor
        .wait_for(|state| state.value.scans_completed == 1, None)
        .await
        .unwrap();
    assert!(!h.exists("audit").await);
    successor.close().await.unwrap();
}

#[tokio::test]
async fn a_live_refresh_arriving_during_marker_read_prevents_stale_cancellation() {
    let h = Harness::new();
    h.contract("refresh", false).await;
    h.scan().await;
    h.advance(10);
    h.markers.pause_get.store(true, Ordering::SeqCst);
    let capability = h.maintenance.clone();
    let audit = h.audit.clone();
    let scan = tokio::spawn(async move { capability.scan(audit.as_ref()).await });
    until(|| h.markers.get_entered.load(Ordering::SeqCst)).await;
    h.token("refresh", "app:a", None).await;
    h.markers.get_release.add_permits(1);
    let report = scan.await.unwrap().unwrap();
    assert_eq!(report.contracts_cancelled, 0);
    assert_eq!(report.stale_observations_removed, 1);
    assert!(h.exists("refresh").await);
}

#[tokio::test]
async fn token_unavailability_and_marker_delete_conflict_leave_cleanup_pending() {
    let h = Harness::new();
    h.cancelled("pending-cleanup").await;
    h.token("pending-cleanup", "app:a", None).await;
    h.advance(20);
    h.tokens.delete_mode.store(2, Ordering::SeqCst);
    let first = h.scan().await;
    assert_eq!(first.contracts_deleted, 1);
    assert_eq!(first.cleanups_completed, 0);
    assert_eq!(first.cleanup_pending.len(), 1);
    h.tokens.unavailable.store(true, Ordering::SeqCst);
    let unavailable = h.scan().await;
    assert_eq!(unavailable.cleanups_completed, 0);
    assert_eq!(unavailable.cleanup_pending.len(), 1);
    assert!(!unavailable.failures.is_empty());
    h.tokens.unavailable.store(false, Ordering::SeqCst);
    h.tokens.delete_mode.store(0, Ordering::SeqCst);
    h.markers.delete_mode.store(1, Ordering::SeqCst);
    let conflict = h.scan().await;
    assert_eq!(conflict.tokens_deleted, 1);
    assert_eq!(conflict.cleanups_completed, 0);
    assert_eq!(conflict.cleanup_pending.len(), 1);
    h.markers.delete_mode.store(0, Ordering::SeqCst);
    assert_eq!(h.scan().await.cleanups_completed, 1);
}

#[tokio::test]
async fn managed_scans_follow_host_clock_and_time_out_blocked_audit_work() {
    let h = Harness::new();
    h.cancelled("scheduled").await;
    let managed = h
        .maintenance
        .manage(h.audit.clone(), Duration::from_secs(30))
        .unwrap();
    managed
        .wait_for(|state| state.value.scans_completed == 1, None)
        .await
        .unwrap();
    assert!(h.exists("scheduled").await);
    h.audit.block.store(true, Ordering::SeqCst);
    h.advance(30);
    until(|| h.audit.entered.load(Ordering::SeqCst)).await;
    h.advance(5);
    managed
        .wait_for(|state| state.value.scans_completed == 2, None)
        .await
        .unwrap();
    assert_eq!(managed.state().phase, Phase::Unavailable);
    assert!(h.exists("scheduled").await);
    assert_eq!(h.audit.cancelled.load(Ordering::SeqCst), 1);
    h.audit.block.store(false, Ordering::SeqCst);
    managed.request_scan().unwrap();
    managed
        .wait_for(|state| state.value.scans_completed == 3, None)
        .await
        .unwrap();
    assert!(!h.exists("scheduled").await);
    managed.close().await.unwrap();
}

#[tokio::test]
async fn invalid_contract_identity_never_redirects_maintenance_to_another_generation() {
    let h = Harness::new();
    let bad = h.contract("bad-key", false).await;
    h.contract("healthy", false).await;
    let healthy_token = h.token("healthy", "app:a", None).await;
    let mut value = bad.value;
    value["contractId"] = json!("healthy");
    h.contracts
        .raw
        .update(&bad.key, value, bad.revision, None)
        .await
        .unwrap();
    assert_eq!(h.scan().await.stale_observations_created, 1);
    h.advance(10);
    assert_eq!(h.scan().await.contracts_cancelled, 1);
    h.advance(20);
    let cleanup = h.scan().await;
    assert_eq!(cleanup.contracts_deleted, 1);
    assert!(!h.exists("bad-key").await);
    assert!(h.exists("healthy").await);
    assert_eq!(
        h.tokens.raw.get(&healthy_token.key).await.unwrap(),
        Some(healthy_token)
    );
    let events = h.audit.events.lock().unwrap();
    let MaintenanceAuditEvent::BeforeContractDeletion(audit) = &events[0] else {
        panic!("deletion audit expected")
    };
    assert_eq!(audit.contract.contract_id, "bad-key");
    assert_eq!(audit.record_contract_id.as_deref(), Some("healthy"));
    assert_eq!(audit.status, ContractValidityStatus::InvalidContract);
}
