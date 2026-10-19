#![cfg(feature = "nats")]

use std::time::Duration;

use deckr::nats::{DeckrRuntime, DeckrRuntimeConfig};
use deckr::state::{MaterializedStateStore, StateStore, StateStorePolicy};
use futures_util::{StreamExt, TryStreamExt};
use serde_json::json;

#[cfg(feature = "conformance")]
#[path = "support/nats_action_path.rs"]
mod action_path;

#[tokio::test]
#[ignore = "requires DECKR_NATS_TEST_URL pointing to a disposable JetStream broker"]
async fn dedicated_maintenance_audits_and_cleans_retained_records_without_runtime_watchers() {
    use deckr::concord::{
        MaintenanceAudit, MaintenanceAuditEvent, MaintenanceAuditFuture, MaintenanceOptions,
    };
    use std::sync::{Arc, Mutex};
    #[derive(Default)]
    struct Audit(Mutex<Vec<MaintenanceAuditEvent>>);
    impl MaintenanceAudit for Audit {
        fn record(&self, event: MaintenanceAuditEvent) -> MaintenanceAuditFuture<'_> {
            Box::pin(async move {
                self.0.lock().unwrap().push(event);
                Ok(())
            })
        }
    }
    let url = std::env::var("DECKR_NATS_TEST_URL").unwrap();
    let context = async_nats::jetstream::new(async_nats::connect(&url).await.unwrap());
    let prefix = format!("deckr_maintenance_{}", uuid::Uuid::new_v4().simple());
    let mut config = DeckrRuntimeConfig::new(url);
    config.beacon_advertisement_bucket = format!("{prefix}_beacon");
    config.concord_contract_bucket = format!("{prefix}_contracts");
    config.concord_token_bucket = format!("{prefix}_tokens");
    config.concord_maintenance_bucket = format!("{prefix}_maintenance");
    let runtime = DeckrRuntime::connect(config.clone()).await.unwrap();
    let capability = runtime
        .maintenance()
        .await
        .unwrap()
        .with_options(MaintenanceOptions {
            retention: Duration::ZERO,
            ..Default::default()
        })
        .unwrap();
    let id = "retained";
    let contract_key = deckr::keys::concord_contract_key(id, 1);
    let token_key = deckr::keys::concord_participant_token_key(
        id,
        1,
        &deckr::endpoint::EndpointAddress::parse_protocol("app:worker").unwrap(),
    );
    let contracts = runtime.nats().concord_contracts();
    let tokens = runtime.nats().concord_tokens();
    contracts.create(&contract_key,json!({"contractId":id,"generation":1,"participants":["app:worker"],"attachedParticipants":["app:worker"],"state":"cancelled","cancelledAt":chrono::Utc::now().to_rfc3339(),"cancelledBy":"app:worker","cancelReason":"test complete"}),None).await.unwrap();
    tokens.create(&token_key,json!({"contractId":id,"generation":1,"participant":"app:worker","sessionId":"session","tokenId":"token","refreshSeq":1,"ttlSeconds":tokens.ttl_seconds().await.unwrap().unwrap()}),None).await.unwrap();
    let audit = Arc::new(Audit::default());
    let report = capability.scan(audit.as_ref()).await.unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(
        (
            report.contracts_deleted,
            report.tokens_deleted,
            report.cleanups_completed
        ),
        (1, 1, 1)
    );
    assert!(report.cleanup_pending.is_empty());
    assert!(contracts.get(&contract_key).await.unwrap().is_none());
    assert!(tokens.get(&token_key).await.unwrap().is_none());
    assert_eq!(audit.0.lock().unwrap().len(), 1);
    assert!(runtime
        .nats()
        .concord_maintenance()
        .await
        .unwrap()
        .items("")
        .await
        .unwrap()
        .is_empty());
    for bucket in [
        &config.concord_contract_bucket,
        &config.concord_token_bucket,
        &config.concord_maintenance_bucket,
    ] {
        let mut stream = context.get_stream(format!("KV_{bucket}")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while stream.info().await.unwrap().state.consumer_count != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    runtime.nats().close_sources().await;
    for bucket in [
        config.beacon_advertisement_bucket,
        config.concord_contract_bucket,
        config.concord_token_bucket,
        config.concord_maintenance_bucket,
    ] {
        context.delete_key_value(&bucket).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires DECKR_NATS_TEST_URL pointing to a disposable broker"]
async fn managed_messages_use_reply_inboxes_and_independent_endpoint_lifetimes() {
    use deckr::endpoint::EndpointAddress;
    use deckr::lanes::DeckrMessage;
    use deckr::messaging::MessageSession;
    use deckr::nats::NatsMessageTransport;
    use deckr::session::EndpointSession;

    let client = async_nats::connect(std::env::var("DECKR_NATS_TEST_URL").unwrap())
        .await
        .unwrap();
    let transport = NatsMessageTransport::new(client.clone());
    let id = uuid::Uuid::new_v4().simple().to_string();
    let local = EndpointSession::with_id(
        EndpointAddress::parse(format!("controller:{id}")).unwrap(),
        "local",
    )
    .unwrap();
    let peer = EndpointSession::with_id(
        EndpointAddress::parse(format!("service:{id}")).unwrap(),
        "peer",
    )
    .unwrap();
    let requester = MessageSession::new(transport.clone(), local.clone()).unwrap();
    let provider = MessageSession::new(transport, peer.clone()).unwrap();
    let mut subscription = provider.subscribe("services").await.unwrap();
    let message = DeckrMessage::from_text(&json!({
        "messageId":"request", "createdAt":chrono::Utc::now().to_rfc3339(),
        "lane":"services", "messageType":"serviceMessage", "sender":local.endpoint().as_str(), "senderSessionId":local.id(),
        "recipient":{"endpoint":peer.endpoint().as_str()}, "recipientSessionId":peer.id(),
        "contract":{"contractId":"test", "generation":1}, "subject":{"kind":"service"},
        "body":{"serviceNamespace":"org.example.test", "name":"read", "intent":"query", "exchangePattern":"request_reply"}
    }).to_string()).unwrap();
    let caller = requester.clone();
    let request =
        tokio::spawn(async move { caller.request(message, Duration::from_secs(3), None).await });
    let incoming = tokio::time::timeout(Duration::from_secs(3), subscription.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(incoming.reply_address.is_some());
    let mut reply = incoming.message.clone();
    reply.message_id = "reply".into();
    provider.reply(&incoming, reply).await.unwrap();
    assert_eq!(request.await.unwrap().unwrap().message_id, "reply");
    local.close();
    requester.close().await.unwrap();
    assert!(provider.state().current);
    peer.close();
    provider.close().await.unwrap();
    assert!(subscription.next().await.is_none());
    client.flush().await.unwrap();
}

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

    let view_bucket = format!("{prefix}_views");
    let (views, same_views) = tokio::join!(
        runtime.service_views(&view_bucket, StateStorePolicy::persistent("service views")),
        runtime.service_views(&view_bucket, StateStorePolicy::persistent("same policy")),
    );
    let views = views.unwrap();
    let same_views = same_views.unwrap();
    let view_entry = views
        .source()
        .create("views.initial", json!({"data":1}), None)
        .await
        .unwrap();
    assert_eq!(
        same_views
            .source()
            .get_cached("views.initial")
            .unwrap()
            .unwrap()
            .revision,
        view_entry.revision
    );
    let mut view_stream = context
        .get_stream(format!("KV_{view_bucket}"))
        .await
        .unwrap();
    assert_eq!(view_stream.info().await.unwrap().state.consumer_count, 1);
    assert!(runtime
        .service_views(
            &view_bucket,
            StateStorePolicy::ttl(30, "incompatible").unwrap()
        )
        .await
        .is_err());

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
    runtime.nats().close_sources().await;
    assert!(!views.source().is_current());
    assert!(!same_views.source().is_current());
    assert!(runtime
        .service_views(&view_bucket, StateStorePolicy::persistent("closed"))
        .await
        .is_err());
    assert!(runtime.nats().managed_beacon().await.is_err());
    assert!(runtime.materialized_concord().await.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while view_stream.info().await.unwrap().state.consumer_count != 0 {
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
        view_bucket,
    ] {
        context.delete_key_value(&name).await.unwrap();
    }
}
