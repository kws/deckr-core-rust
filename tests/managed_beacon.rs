mod support;

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use deckr::beacon::{AdvertisementContent, BeaconAdvertiser};
use deckr::endpoint::EndpointAddress;
use deckr::lifecycle::{Clock, FailureKind, Phase};
use deckr::state::{MemoryStateStore, StateEntry, StateSource, StateStore, StateWatchStream};
use deckr::{Error, Result};
use futures_util::StreamExt;
use serde_json::{json, Value};

use support::ManualClock;

#[derive(Clone)]
struct FaultStore {
    raw: MemoryStateStore,
    fail_reads: Arc<AtomicBool>,
    panic_reads: Arc<AtomicBool>,
    stall_deletes: Arc<AtomicBool>,
    scans: Arc<AtomicUsize>,
    stall_create_reply: Arc<AtomicBool>,
}
impl FaultStore {
    fn new() -> Self {
        Self {
            raw: MemoryStateStore::ttl_bound(120).unwrap(),
            fail_reads: Arc::default(),
            panic_reads: Arc::default(),
            stall_deletes: Arc::default(),
            scans: Arc::default(),
            stall_create_reply: Arc::default(),
        }
    }
}
impl StateStore for FaultStore {
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        assert!(
            !self.panic_reads.load(Ordering::SeqCst),
            "injected adapter panic"
        );
        if self.fail_reads.load(Ordering::SeqCst) {
            return Err(Error::StateUnavailable("injected failure".into()));
        }
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
        if self.stall_create_reply.load(Ordering::SeqCst) {
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
        self.raw.update(key, value, revision, ttl).await
    }
    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        if self.stall_deletes.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        self.raw.delete(key, revision).await
    }
    async fn watch(&self, prefix: &str) -> Result<StateWatchStream> {
        self.raw.watch(prefix).await
    }
    async fn open_source(&self, prefix: &str) -> Result<StateSource> {
        self.raw.open_source(prefix).await
    }
}

fn advertiser(store: FaultStore, clock: &ManualClock) -> BeaconAdvertiser<FaultStore> {
    BeaconAdvertiser::new(
        store,
        "org.example.worker",
        EndpointAddress::parse_protocol("worker:one").unwrap(),
        "session",
    )
    .payload(json!({"version": 1}))
    .clock(Arc::new(clock.clone()))
}

#[tokio::test]
async fn managed_publication_owns_ttl_schedule_updates_and_recovery() {
    let clock = ManualClock::default();
    let store = FaultStore::new();
    let publication = advertiser(store.clone(), &clock)
        .refresh_interval(Duration::from_secs(3600))
        .manage()
        .unwrap();
    let mut watch = publication.watch_state().unwrap();
    assert_eq!(watch.next().await.unwrap().phase, Phase::Starting);
    let initial = publication.wait_ready(None).await.unwrap();
    assert_eq!(
        initial.next_refresh_at - clock.now(),
        Duration::from_secs(75)
    );
    assert_eq!(
        store.get(&initial.key).await.unwrap().unwrap().value["updatedAt"],
        "2026-01-01T00:00:00Z"
    );
    let changed = publication
        .update(AdvertisementContent {
            payload: Some(json!({"version": 2})),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(changed.revision > initial.revision);
    let unchanged = publication
        .update(AdvertisementContent {
            payload: Some(json!({"version": 2})),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(changed.revision, unchanged.revision);
    assert!(publication
        .update(AdvertisementContent {
            payload: Some(json!(1)),
            ..Default::default()
        })
        .await
        .is_err());
    assert_eq!(publication.state().phase, Phase::Ready);
    clock.advance(Duration::from_secs(75));
    let refreshed = publication
        .wait_for(
            |state| {
                state
                    .value
                    .handle
                    .as_ref()
                    .is_some_and(|handle| handle.refresh_seq > changed.refresh_seq)
            },
            None,
        )
        .await
        .unwrap();
    let handle = refreshed.value.handle.unwrap();
    store
        .delete(&handle.key, Some(handle.revision))
        .await
        .unwrap();
    clock.advance(Duration::from_secs(75));
    let recovered = publication
        .wait_for(
            |state| {
                state
                    .value
                    .handle
                    .as_ref()
                    .is_some_and(|current| current.revision > handle.revision)
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        recovered.value.handle.as_ref().unwrap().advertisement_id,
        initial.advertisement_id
    );
    assert_eq!(store.scans.load(Ordering::SeqCst), 0);
    publication.close().await.unwrap();
    assert!(store.get(&initial.key).await.unwrap().is_none());
    assert_eq!(publication.state().phase, Phase::Closed);
    assert!(matches!(
        publication.wait_ready(None).await,
        Err(Error::Closed(_))
    ));
    assert!(matches!(
        publication.update(AdvertisementContent::default()).await,
        Err(Error::Closed(_))
    ));
}

#[tokio::test]
async fn publication_failure_is_observable_and_recovers() {
    let store = FaultStore::new();
    let clock = ManualClock::default();
    let publication = advertiser(store.clone(), &clock).manage().unwrap();
    let handle = publication.wait_ready(None).await.unwrap();
    let mut watch = publication.watch_state().unwrap();
    assert!(watch.next().await.unwrap().current);
    store.fail_reads.store(true, Ordering::SeqCst);
    clock.advance(Duration::from_secs(75));
    publication
        .wait_for(|state| state.phase == Phase::Unavailable, None)
        .await
        .unwrap();
    store.fail_reads.store(false, Ordering::SeqCst);
    clock.advance(Duration::from_secs(1));
    let restored = publication.wait_ready(None).await.unwrap();
    assert!(restored.revision > handle.revision);
    // Coalescing must retain the interruption even after successful recovery.
    let interruption = watch.next().await.unwrap();
    assert!(!interruption.current);
    assert_eq!(interruption.failure.unwrap().kind, FailureKind::Unavailable);
    assert!(watch.next().await.unwrap().current);
    publication.close().await.unwrap();
}

#[tokio::test]
async fn close_is_bounded_and_last_handle_drop_withdraws() {
    let store = FaultStore::new();
    let clock = ManualClock::default();
    let publication = advertiser(store.clone(), &clock).manage().unwrap();
    publication.wait_ready(None).await.unwrap();
    store.stall_deletes.store(true, Ordering::SeqCst);
    let closer = publication.clone();
    let close = tokio::spawn(async move { closer.close().await });
    publication
        .wait_for(|state| state.phase == Phase::Closing, None)
        .await
        .unwrap();
    // The cleanup deadline is installed in the same worker poll as Closing.
    tokio::task::yield_now().await;
    close.abort();
    assert!(close.await.unwrap_err().is_cancelled());
    clock.advance(Duration::from_secs(5));
    publication
        .wait_for(|state| state.phase == Phase::Closed, None)
        .await
        .unwrap();
    assert!(publication.close().await.is_err());
    assert_eq!(publication.state().phase, Phase::Closed);
    assert_eq!(
        publication.state().failure.unwrap().kind,
        FailureKind::Deadline
    );

    store.stall_deletes.store(false, Ordering::SeqCst);
    let publication = advertiser(store.clone(), &clock).manage().unwrap();
    let handle = publication.wait_ready(None).await.unwrap();
    let mut watch = publication.watch_state().unwrap();
    drop(publication);
    while watch.next().await.unwrap().phase != Phase::Closed {}
    assert!(store.get(&handle.key).await.unwrap().is_none());
}

#[tokio::test]
async fn closure_does_not_delete_a_different_owner() {
    let store = FaultStore::new();
    let clock = ManualClock::default();
    let publication = advertiser(store.clone(), &clock).manage().unwrap();
    let handle = publication.wait_ready(None).await.unwrap();
    let mut replacement = store.get(&handle.key).await.unwrap().unwrap().value;
    replacement["sessionId"] = json!("other");
    store.put(&handle.key, replacement, None).await.unwrap();
    assert!(publication.close().await.is_err());
    assert!(store.get(&handle.key).await.unwrap().is_some());
}

#[tokio::test]
async fn cancelled_initial_publication_cleans_up_an_unacknowledged_commit() {
    let store = FaultStore::new();
    let clock = ManualClock::default();
    store.stall_create_reply.store(true, Ordering::SeqCst);
    let publication = advertiser(store.clone(), &clock).manage().unwrap();
    let committed = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(entry) = store
                .raw
                .items("advertisements.")
                .await
                .unwrap()
                .into_iter()
                .next()
            {
                break entry;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(publication.state().value.handle.is_none());
    let readers: Vec<_> = (0..256)
        .map(|_| publication.watch_state().unwrap())
        .collect();
    assert!(publication.watch_state().is_err());
    publication.close().await.unwrap();
    assert!(store.get(&committed.key).await.unwrap().is_none());
    drop(readers);
}

#[tokio::test]
async fn adapter_panic_still_withdraws_the_owned_advertisement() {
    let clock = ManualClock::default();
    let store = FaultStore::new();
    let publication = advertiser(store.clone(), &clock).manage().unwrap();
    let handle = publication.wait_ready(None).await.unwrap();
    store.panic_reads.store(true, Ordering::SeqCst);
    clock.advance(Duration::from_secs(75));
    tokio::time::timeout(
        Duration::from_secs(2),
        publication.wait_for(|state| state.phase == Phase::Closed, None),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(store.raw.get(&handle.key).await.unwrap().is_none());
    assert!(publication.state().failure.is_some());
    assert!(publication.close().await.is_err());
}
