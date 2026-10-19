#![cfg(feature = "nats")]

use std::time::Duration;

use deckr::nats::{DeckrRuntime, DeckrRuntimeConfig};
use deckr::state::{MaterializedStateStore, StateStore};
use futures_util::{StreamExt, TryStreamExt};
use serde_json::json;

/// Explicitly opt in with a disposable broker; never runs in ordinary tests.
#[tokio::test]
#[ignore = "requires DECKR_NATS_TEST_URL pointing to a disposable JetStream broker"]
async fn participation_does_not_require_maintenance_and_sources_recover_without_leaks() {
    let url = std::env::var("DECKR_NATS_TEST_URL")
        .expect("set DECKR_NATS_TEST_URL to a disposable broker");
    let client = async_nats::connect(&url).await.unwrap();
    let context = async_nats::jetstream::new(client);
    let prefix = format!("deckr_test_{}", uuid::Uuid::new_v4().simple());
    let mut config = DeckrRuntimeConfig::new(url);
    config.beacon_advertisement_bucket = format!("{prefix}_beacon");
    config.concord_contract_bucket = format!("{prefix}_contracts");
    config.concord_token_bucket = format!("{prefix}_tokens");
    config.concord_maintenance_bucket = format!("{prefix}_maintenance");
    // Ordinary participation must start despite an unusable optional store.
    let maintenance = context
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: config.concord_maintenance_bucket.clone(),
            history: 5,
            ..Default::default()
        })
        .await
        .unwrap();
    let runtime = DeckrRuntime::connect(config.clone()).await.unwrap();
    assert!(runtime.nats().concord_maintenance().await.is_err());
    assert_eq!(maintenance.status().await.unwrap().history(), 5);

    let raw = runtime.nats().concord_contracts().clone();
    raw.create("records.initial", json!(1), None).await.unwrap();
    let source = MaterializedStateStore::start(raw.clone(), "records.")
        .await
        .unwrap();
    assert_eq!(
        source.get_cached("records.initial").unwrap().unwrap().value,
        json!(1)
    );
    let entry = source
        .create("records.created", json!(2), None)
        .await
        .unwrap();
    assert_eq!(
        source
            .get_cached("records.created")
            .unwrap()
            .unwrap()
            .revision,
        entry.revision
    );
    source
        .delete("records.created", Some(entry.revision))
        .await
        .unwrap();
    assert!(source.get_cached("records.created").unwrap().is_none());

    let mut bucket = context
        .get_stream(format!("KV_{}", config.concord_contract_bucket))
        .await
        .unwrap();
    for _ in 0..5 {
        assert_eq!(raw.items("records.").await.unwrap().len(), 1);
    }
    assert_eq!(bucket.info().await.unwrap().state.consumer_count, 1);
    let before = source.authority_epoch();
    let mut snapshots = source.watch_snapshots("records.").unwrap();
    assert!(snapshots.next().await.unwrap().current);
    let names: Vec<String> = bucket.consumer_names().try_collect().await.unwrap();
    assert_eq!(names.len(), 1);
    bucket.delete_consumer(&names[0]).await.unwrap();
    raw.delete("records.initial", None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(6), async {
        while snapshots.next().await.unwrap().current {}
        while !snapshots.next().await.unwrap().current {}
    })
    .await
    .unwrap();
    assert_ne!(source.authority_epoch(), before);
    assert!(source.get_cached("records.initial").unwrap().is_none());
    source.close_and_wait().await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while bucket.info().await.unwrap().state.consumer_count != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for name in [
        config.beacon_advertisement_bucket,
        config.concord_contract_bucket,
        config.concord_token_bucket,
        config.concord_maintenance_bucket,
    ] {
        context.delete_key_value(&name).await.unwrap();
    }
}
