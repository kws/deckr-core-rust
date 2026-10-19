#[path = "support/service.rs"]
mod service_support;

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

use deckr::lifecycle::Phase;
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use deckr::state::{
    MaterializedStateStore, MemoryStateStore, StateChange, StateEntry, StateSource, StateStore,
    StateWatchStream,
};
use deckr::{Error, Result};
use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{stream, StreamExt};
use serde_json::{json, Map, Value};
use service_support::{bounded, Harness};
use tokio::sync::Notify;

type View = ProtectedServiceView<ControlledStore, MemoryStateStore, MemoryStateStore>;
type Watch = ProtectedViewWatch<ControlledStore, MemoryStateStore, MemoryStateStore>;

#[derive(Clone, Default)]
struct ControlledStore {
    raw: MemoryStateStore,
    paused: Arc<AtomicBool>,
    resumed: Arc<Notify>,
    fail_open: Arc<AtomicBool>,
    block_create: Arc<AtomicBool>,
    create_started: Arc<AtomicBool>,
    faults: Arc<Mutex<Vec<UnboundedSender<Result<StateChange>>>>>,
    reads: Arc<AtomicUsize>,
    scans: Arc<AtomicUsize>,
    opens: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
}
struct SourceGuard(Arc<AtomicUsize>);
impl Drop for SourceGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ControlledStore {
    fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }
    fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.resumed.notify_waiters();
    }
    fn fail_watch(&self) {
        self.fail_open.store(true, Ordering::SeqCst);
        self.faults.lock().unwrap().retain(|sender| {
            sender
                .unbounded_send(Err(Error::StateUnavailable(
                    "injected source failure".into(),
                )))
                .is_ok()
        });
    }
}
impl StateStore for ControlledStore {
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        self.raw.ttl_seconds().await
    }
    async fn get(&self, key: &str) -> Result<Option<StateEntry>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
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
        self.create_started.store(true, Ordering::SeqCst);
        if self.block_create.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        self.raw.create(key, value, ttl).await
    }
    async fn update(
        &self,
        key: &str,
        value: Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<StateEntry> {
        self.raw.update(key, value, revision, ttl).await
    }
    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        self.raw.delete(key, revision).await
    }
    async fn delete_revision(&self, key: &str, revision: Option<u64>) -> Result<u64> {
        self.raw.delete_revision(key, revision).await
    }
    async fn watch(&self, prefix: &str) -> Result<StateWatchStream> {
        self.raw.watch(prefix).await
    }
    async fn open_source(&self, prefix: &str) -> Result<StateSource> {
        if self.fail_open.load(Ordering::SeqCst) {
            return Err(Error::StateUnavailable("injected recovery failure".into()));
        }
        let source = self.raw.open_source(prefix).await?;
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        let guard = SourceGuard(self.active.clone());
        let paused = self.paused.clone();
        let resumed = self.resumed.clone();
        let changes = source.changes.then(move |item| {
            let paused = paused.clone();
            let resumed = resumed.clone();
            async move {
                loop {
                    let notified = resumed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if !paused.load(Ordering::SeqCst) {
                        break;
                    }
                    notified.await;
                }
                item
            }
        });
        let (sender, faults) = unbounded();
        self.faults.lock().unwrap().push(sender);
        let merged = Box::pin(stream::select(Box::pin(changes), faults));
        let changes = stream::unfold((merged, guard), |(mut changes, guard)| async move {
            Some((changes.next().await?, (changes, guard)))
        });
        Ok(StateSource {
            entries: source.entries,
            revision: source.revision,
            replayed: source.replayed,
            changes: Box::pin(changes),
        })
    }
}

struct Views {
    service: Harness,
    raw: ControlledStore,
    store: ServiceViewStore<ControlledStore>,
    reader: View,
    writer: View,
}
impl Views {
    async fn new() -> Self {
        let service = Harness::new().await;
        let raw = ControlledStore::default();
        let source = MaterializedStateStore::start(raw.clone(), "")
            .await
            .unwrap();
        let store = ServiceViewStore::new("test_views", source).unwrap();
        let schema = SchemaRegistry::default()
            .compile(&json!({
                "type":"object", "properties":{"data":{"type":"integer"}}, "required":["data"]
            }))
            .unwrap();
        let reader = store
            .bind(service.consumer.clone(), "state", Some(schema.clone()))
            .unwrap();
        let writer = store
            .bind(service.service.clone(), "state", Some(schema))
            .unwrap();
        bounded(reader.wait_ready(None)).await.unwrap();
        bounded(writer.wait_ready(None)).await.unwrap();
        Self {
            service,
            raw,
            store,
            reader,
            writer,
        }
    }
    fn key(&self, name: &str) -> String {
        format!(
            "{}desk.{name}",
            self.service.consumer.descriptor().views["state"].key_prefix
        )
    }
    async fn settled(&self) {
        until(|| self.reader.state().current && self.writer.state().current).await;
    }
    async fn close(self) {
        self.reader.close().await.unwrap();
        self.writer.close().await.unwrap();
        self.store.source().close_and_wait().await;
        until(|| self.raw.active.load(Ordering::SeqCst) == 0).await;
        self.service.close().await;
    }
}
fn payload(data: i64) -> Map<String, Value> {
    json!({"data":data}).as_object().unwrap().clone()
}
async fn until(mut predicate: impl FnMut() -> bool) {
    bounded(async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
async fn next_current(watch: &mut Watch) -> deckr::lifecycle::Snapshot<ProtectedViewState> {
    bounded(async {
        loop {
            let state = watch.next().await.expect("view still open");
            assert_ne!(state.phase, Phase::Closed);
            if state.current {
                return state;
            }
        }
    })
    .await
}

#[tokio::test]
async fn cached_reads_share_one_source_and_writes_wait_for_ordered_observations() {
    let h = Views::new().await;
    let key = h.key("lamp");
    h.raw.pause();
    let writer = h.writer.clone();
    let writing_key = key.clone();
    let write = tokio::spawn(async move { writer.create(&writing_key, payload(1)).await });
    bounded(async {
        while h.raw.raw.items("").await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!write.is_finished());
    assert!(h.reader.get(&key).await.unwrap().is_none());
    h.raw.resume();
    let revision = bounded(write).await.unwrap().unwrap();
    assert_eq!(h.writer.state().value.entries[&key].revision, revision);
    h.settled().await;
    let reads = h.raw.reads.load(Ordering::SeqCst);
    let scans = h.raw.scans.load(Ordering::SeqCst);
    let opens = h.raw.opens.load(Ordering::SeqCst);
    for _ in 0..1000 {
        assert_eq!(
            h.reader.get(&key).await.unwrap().unwrap().revision,
            revision
        );
    }
    assert_eq!(h.raw.reads.load(Ordering::SeqCst), reads);
    assert_eq!(h.raw.scans.load(Ordering::SeqCst), scans);
    assert_eq!(h.raw.opens.load(Ordering::SeqCst), opens);
    assert_eq!(h.raw.active.load(Ordering::SeqCst), 1);

    h.raw.pause();
    let writer = h.writer.clone();
    let writing_key = key.clone();
    let update =
        tokio::spawn(async move { writer.update(&writing_key, payload(2), revision).await });
    bounded(async {
        while h.raw.raw.items("").await.unwrap()[0].revision == revision {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!update.is_finished());
    assert_eq!(
        h.reader.get(&key).await.unwrap().unwrap().record.payload["data"],
        json!(1)
    );
    h.raw.resume();
    let updated = bounded(update).await.unwrap().unwrap();
    assert!(updated > revision);
    assert_eq!(h.writer.state().value.entries[&key].revision, updated);
    assert!(h.writer.update(&key, payload(3), revision).await.is_err());
    assert!(h.writer.delete(&key, revision).await.is_err());
    h.raw.pause();
    let writer = h.writer.clone();
    let deleting_key = key.clone();
    let delete = tokio::spawn(async move { writer.delete(&deleting_key, updated).await });
    bounded(async {
        while !h.raw.raw.items("").await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!delete.is_finished());
    assert!(h.reader.get(&key).await.unwrap().is_some());
    h.raw.resume();
    assert!(bounded(delete).await.unwrap().unwrap() > updated);
    assert!(!h.writer.state().value.entries.contains_key(&key));
    assert!(h.reader.get(&key).await.unwrap().is_none());
    h.close().await;
}

#[tokio::test]
async fn all_fences_hide_replacements_and_emit_only_removal_of_previously_visible_entries() {
    let h = Views::new().await;
    let mut watch = h.reader.watch_state().unwrap();
    next_current(&mut watch).await;
    for (index, (field, replacement)) in [
        ("viewKey", json!("views.wrong")),
        ("serviceId", json!("other")),
        ("serviceNamespace", json!("another.namespace")),
        ("serviceEndpoint", json!("service:other")),
        ("serviceSessionId", json!("other-session")),
        ("consumerEndpoint", json!("controller:other")),
        ("consumerSessionId", json!("other-consumer")),
        ("writer", json!("consumer")),
        ("contractId", json!("other-contract")),
        ("generation", json!(2)),
    ]
    .into_iter()
    .enumerate()
    {
        let key = h.key(&index.to_string());
        h.writer.create(&key, payload(1)).await.unwrap();
        h.settled().await;
        assert!(next_current(&mut watch)
            .await
            .value
            .entries
            .contains_key(&key));
        let entry = h.reader.get(&key).await.unwrap().unwrap();
        let mut foreign = entry.record.to_value().unwrap();
        foreign[field] = replacement;
        foreign["secret"] = json!("must not escape");
        let replacement = h
            .store
            .source()
            .update(&entry.reference.key, foreign.clone(), entry.revision, None)
            .await
            .unwrap();
        h.settled().await;
        let removed = next_current(&mut watch).await;
        assert!(removed.value.entries.is_empty(), "{field}");
        assert!(removed.changed.contains(&key), "{field}");
        assert!(h.reader.get(&key).await.unwrap().is_none(), "{field}");
        assert!(
            h.writer
                .update(&key, payload(2), replacement.revision)
                .await
                .is_err(),
            "{field}"
        );
        assert!(
            h.writer.delete(&key, replacement.revision).await.is_err(),
            "{field}"
        );
        assert_eq!(
            h.raw
                .raw
                .get(&entry.reference.key)
                .await
                .unwrap()
                .unwrap()
                .value,
            foreign
        );
        h.store
            .source()
            .delete(&entry.reference.key, Some(replacement.revision))
            .await
            .unwrap();
        h.settled().await;
        assert!(
            next_current(&mut watch).await.changed.is_empty(),
            "hidden deletion {field}"
        );
    }
    h.close().await;
}

#[tokio::test]
async fn queued_snapshots_cannot_expose_a_superseded_payload() {
    let h = Views::new().await;
    let key = h.key("queued");
    let mut watch = h.reader.watch_state().unwrap();
    next_current(&mut watch).await;
    let revision = h.writer.create(&key, payload(42)).await.unwrap();
    h.settled().await;
    let entry = h.reader.get(&key).await.unwrap().unwrap();
    let mut replacement = entry.record.to_value().unwrap();
    replacement["serviceSessionId"] = json!("replacement");
    h.store
        .source()
        .update(&entry.reference.key, replacement, revision, None)
        .await
        .unwrap();
    let snapshot = bounded(watch.next()).await.unwrap();
    assert!(snapshot.value.entries.is_empty());
    assert!(snapshot.changed.is_empty());
    h.close().await;
}

#[tokio::test]
async fn exact_revision_guards_preserve_replacements_not_yet_observed_locally() {
    let h = Views::new().await;
    let key = h.key("raced");
    let revision = h.writer.create(&key, payload(1)).await.unwrap();
    let entry = h.reader.get(&key).await.unwrap().unwrap();
    let mut foreign = entry.record.to_value().unwrap();
    foreign["consumerSessionId"] = json!("new-owner");
    h.raw.pause();
    let replacement = h
        .raw
        .raw
        .update(&entry.reference.key, foreign.clone(), revision, None)
        .await
        .unwrap();
    assert_eq!(
        h.store
            .source()
            .get_cached(&entry.reference.key)
            .unwrap()
            .unwrap()
            .revision,
        revision
    );
    assert!(h.writer.update(&key, payload(2), revision).await.is_err());
    assert!(h.writer.delete(&key, revision).await.is_err());
    assert_eq!(
        h.raw.raw.get(&entry.reference.key).await.unwrap().unwrap(),
        replacement
    );
    h.raw.resume();
    h.store
        .source()
        .wait_observed(&entry.reference.key, replacement.revision)
        .await
        .unwrap();
    h.settled().await;
    assert!(h.reader.get(&key).await.unwrap().is_none());
    // Observations outside this family still advance source versions and must
    // not leave the protected projection permanently behind.
    h.store
        .source()
        .create("unrelated.key", json!({}), None)
        .await
        .unwrap();
    h.settled().await;
    assert!(h.reader.state().value.entries.is_empty());
    h.close().await;
}

#[tokio::test]
async fn source_recovery_removes_missed_deletes_without_revoking_service_authority() {
    let h = Views::new().await;
    let key = h.key("gone");
    h.writer.create(&key, payload(1)).await.unwrap();
    h.settled().await;
    let entry = h.reader.get(&key).await.unwrap().unwrap();
    let mut watch = h.reader.watch_state().unwrap();
    next_current(&mut watch).await;
    h.raw.pause();
    h.raw
        .raw
        .delete(&entry.reference.key, Some(entry.revision))
        .await
        .unwrap();
    h.raw.fail_watch();
    until(|| !h.store.source().is_current()).await;
    assert!(h.reader.get(&key).await.is_err());
    assert!(h
        .writer
        .create(&h.key("while-stale"), payload(2))
        .await
        .is_err());
    let unavailable = bounded(watch.next()).await.unwrap();
    assert!(!unavailable.current);
    assert!(unavailable.value.entries.is_empty());
    assert!(unavailable.changed.contains(&key));
    h.service.consumer.authorize().unwrap();
    h.service.service.authorize().unwrap();
    h.raw.resume();
    h.raw.fail_open.store(false, Ordering::SeqCst);
    bounded(h.reader.wait_ready(None)).await.unwrap();
    assert!(h.reader.get(&key).await.unwrap().is_none());
    assert!(next_current(&mut watch).await.value.entries.is_empty());
    assert_eq!(h.raw.active.load(Ordering::SeqCst), 1);
    assert_eq!(h.raw.opens.load(Ordering::SeqCst), 2);
    h.service.consumer.authorize().unwrap();
    h.writer
        .create(&h.key("after-recovery"), payload(3))
        .await
        .unwrap();
    h.close().await;
}

#[tokio::test]
async fn schemas_writer_roles_prefixes_and_record_identity_are_enforced() {
    let h = Views::new().await;
    let key = h.key("valid");
    assert!(h.reader.create(&key, payload(1)).await.is_err());
    assert!(h.writer.create(&key, Map::new()).await.is_err());
    let mut invalid = payload(1);
    invalid.insert("generation".into(), json!(2));
    assert!(h.writer.create(&key, invalid).await.is_err());
    let outside = format!(
        "{}elsewhere",
        h.service.service.descriptor().views["state"].key_prefix
    );
    assert!(h.writer.create(&outside, payload(1)).await.is_err());
    assert!(h.reader.get(&outside).await.is_err());
    let revision = h.writer.create(&key, payload(1)).await.unwrap();
    let entry = h.reader.get(&key).await.unwrap().unwrap();
    let mut invalid = entry.record.to_value().unwrap();
    invalid["data"] = json!("invalid schema");
    h.store
        .source()
        .update(&entry.reference.key, invalid, revision, None)
        .await
        .unwrap();
    h.settled().await;
    assert!(h.reader.get(&key).await.unwrap().is_none());
    assert!(h.reader.state().value.entries.is_empty());

    let requests = h
        .store
        .bind(h.service.consumer.clone(), "requests", None)
        .unwrap();
    let provider = h
        .store
        .bind(h.service.service.clone(), "requests", None)
        .unwrap();
    bounded(requests.wait_ready(None)).await.unwrap();
    bounded(provider.wait_ready(None)).await.unwrap();
    let request_key = format!(
        "{}set",
        h.service.consumer.descriptor().views["requests"].key_prefix
    );
    requests.create(&request_key, payload(42)).await.unwrap();
    assert_eq!(
        provider
            .get(&request_key)
            .await
            .unwrap()
            .unwrap()
            .record
            .fence
            .writer,
        ServiceViewWriter::Consumer
    );
    assert!(provider.create(&request_key, payload(1)).await.is_err());
    let wrong = ServiceViewStore::new("another_bucket", h.store.source().clone()).unwrap();
    assert!(wrong
        .bind(h.service.consumer.clone(), "state", None)
        .is_err());
    let narrow = MaterializedStateStore::start(h.raw.clone(), &key)
        .await
        .unwrap();
    assert!(ServiceViewStore::new("test_views", narrow.clone())
        .unwrap()
        .bind(h.service.consumer.clone(), "state", None)
        .is_err());
    narrow.close_and_wait().await;
    requests.close().await.unwrap();
    provider.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn public_watch_limits_and_slow_readers_do_not_block_mutation_or_closure() {
    let h = Views::new().await;
    let mut watches: Vec<_> = (0..256).map(|_| h.writer.watch_state().unwrap()).collect();
    assert!(h.writer.watch_state().is_err());
    for watch in &mut watches {
        next_current(watch).await;
    }
    for index in 0..258 {
        h.writer
            .create(&h.key(&index.to_string()), payload(index))
            .await
            .unwrap();
    }
    let snapshot = next_current(&mut watches[0]).await;
    assert!(snapshot.resnapshot_required);
    assert!(snapshot.changed.is_empty());
    assert_eq!(snapshot.value.entries.len(), 258);
    let closing = h.writer.clone();
    let task = tokio::spawn(async move { closing.close().await });
    tokio::task::yield_now().await;
    task.abort();
    h.writer.request_close();
    for watch in &mut watches {
        let snapshot = bounded(watch.next()).await.unwrap();
        assert_eq!(snapshot.phase, Phase::Closed);
        assert!(snapshot.failure.is_none());
        assert!(snapshot.value.entries.is_empty());
        assert!(watch.next().await.is_none());
    }
    assert!(h.store.source().is_current());
    h.service.consumer.authorize().unwrap();
    assert!(h.reader.get(&h.key("0")).await.unwrap().is_some());
    h.close().await;
}

#[tokio::test]
async fn authority_loss_cancels_pending_mutations_and_retained_watches() {
    let h = Views::new().await;
    let key = h.key("never-committed");
    let mut watch = h.reader.watch_state().unwrap();
    next_current(&mut watch).await;
    h.raw.block_create.store(true, Ordering::SeqCst);
    let writer = h.writer.clone();
    let write_key = key.clone();
    let writing = tokio::spawn(async move { writer.create(&write_key, payload(1)).await });
    until(|| h.raw.create_started.load(Ordering::SeqCst)).await;
    h.service.proposal.request_close();
    assert!(bounded(writing).await.unwrap().is_err());
    let closed = bounded(watch.next()).await.unwrap();
    assert_eq!(closed.phase, Phase::Closed);
    assert!(closed.value.entries.is_empty());
    assert!(h.reader.get(&key).await.is_err());
    assert!(h.raw.raw.items("").await.unwrap().is_empty());
    assert!(h.store.source().is_current());
    h.close().await;
}

#[tokio::test]
async fn last_owner_drop_and_source_closure_release_bindings_without_ending_agreements() {
    let h = Views::new().await;
    let binding = h
        .store
        .bind(h.service.consumer.clone(), "state", None)
        .unwrap();
    bounded(binding.wait_ready(None)).await.unwrap();
    let mut watch = binding.watch_state().unwrap();
    next_current(&mut watch).await;
    drop(binding);
    assert_eq!(bounded(watch.next()).await.unwrap().phase, Phase::Closed);
    assert!(h.store.source().is_current());
    let mut reader_watch = h.reader.watch_state().unwrap();
    next_current(&mut reader_watch).await;
    h.store.source().close_and_wait().await;
    assert_eq!(
        bounded(reader_watch.next()).await.unwrap().phase,
        Phase::Closed
    );
    h.service.consumer.authorize().unwrap();
    h.service.service.authorize().unwrap();
    h.close().await;
}

#[test]
fn flat_records_reject_duplicate_or_missing_fences_and_keep_revisions_as_metadata() {
    let record = ServiceViewRecord::from_value(json!({
        "viewKey":"views.test.state.current", "serviceId":"test", "serviceNamespace":"org.example.test",
        "serviceEndpoint":"service:test", "serviceSessionId":"service-session",
        "consumerEndpoint":"controller:test", "consumerSessionId":"consumer-session",
        "writer":"service", "contractId":"contract / é", "generation":7,"data":42
    })).unwrap();
    let wire = record.to_value().unwrap();
    assert_eq!(wire["data"], json!(42));
    assert!(wire.get("fence").is_none());
    assert!(wire.get("revision").is_none());
    assert_eq!(
        ServiceViewRecord::from_text(&wire.to_string()).unwrap(),
        record
    );
    assert_eq!(
        record.fence.storage_key().unwrap(),
        "views.test.state.current.contract.b64_Y29udHJhY3QgLyDDqQ.7"
    );
    let duplicated = wire.to_string().replacen('{', "{\"generation\":7,", 1);
    assert!(ServiceViewRecord::from_text(&duplicated).is_err());
    for field in [
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
    ] {
        let mut missing = wire.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(ServiceViewRecord::from_value(missing).is_err(), "{field}");
    }
    assert!(service_view_storage_key("views.test", "contract", 0).is_err());
    assert!(ServiceViewRef {
        store_name: String::new(),
        key: "key".into()
    }
    .validate()
    .is_err());
}
