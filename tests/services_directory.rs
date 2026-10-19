use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use deckr::beacon::{
    Beacon, BeaconAdvertiser, BeaconFeatureEventType, BEACON_ADVERTISEMENT_SCHEMA_ID,
    DEFAULT_BEACON_TTL_SECONDS,
};
use deckr::endpoint::{service_address, EndpointAddress};
use deckr::services::{
    parse_service_descriptor, service_use_terms, service_view_prefix, ServiceBackendStatus,
    ServiceDirectory, ServiceProtocol, ServiceQuery, ServiceResolver, ServiceViewFamilyDefinition,
};
use deckr::state::{MemoryStateStore, StateStore};
use futures_util::StreamExt;
use tokio::time;

const SERVICE_ID: &str = "openhab-home";
const BACKUP_SERVICE_ID: &str = "openhab-backup";
const SESSION_ID: &str = "service-session";

#[tokio::test]
async fn service_descriptor_parsing_validates_protocol_identity() {
    let state = MemoryStateStore::ttl_bound(DEFAULT_BEACON_TTL_SECONDS).unwrap();
    let beacon = Beacon::start(state.clone()).await.unwrap();
    let protocol = service_protocol(SERVICE_ID);
    publish_service(&state, &protocol, SERVICE_ID, "ad-1", SESSION_ID).await;

    wait_until(|| beacon.candidates(&protocol.feature_id).unwrap().len() == 1).await;
    let candidate = beacon.candidates(&protocol.feature_id).unwrap()[0].clone();
    let descriptor = parse_service_descriptor(&candidate, &protocol).unwrap();

    assert_eq!(descriptor.service_id, SERVICE_ID);
    assert_eq!(descriptor.endpoint.as_str(), service_address(SERVICE_ID));
    assert!(descriptor.supported_operations.contains("sendCommand"));
    assert_eq!(
        descriptor.views["items"].key_prefix,
        service_view_prefix(SERVICE_ID, "items")
    );

    let mut wrong_protocol = protocol.clone();
    wrong_protocol.feature_id = "dev.deckr.other.feature".to_string();
    assert!(parse_service_descriptor(&candidate, &wrong_protocol).is_none());
}

#[tokio::test]
async fn service_use_scope_id_includes_namespace() {
    let state = MemoryStateStore::ttl_bound(DEFAULT_BEACON_TTL_SECONDS).unwrap();
    let beacon = Beacon::start(state.clone()).await.unwrap();
    let protocol = service_protocol(SERVICE_ID);
    publish_service(&state, &protocol, SERVICE_ID, "ad-1", SESSION_ID).await;

    wait_until(|| beacon.candidates(&protocol.feature_id).unwrap().len() == 1).await;
    let candidate = beacon.candidates(&protocol.feature_id).unwrap()[0].clone();
    let descriptor = parse_service_descriptor(&candidate, &protocol).unwrap();
    let client = EndpointAddress::parse("action_provider:test-provider").unwrap();
    let terms = service_use_terms(&descriptor, &client, ["sendCommand"], BTreeMap::new()).unwrap();
    let mut other_namespace = descriptor.clone();
    other_namespace.namespace = "dev.deckr.other.service".to_string();
    let other_terms =
        service_use_terms(&other_namespace, &client, ["sendCommand"], BTreeMap::new()).unwrap();

    assert_eq!(terms.service_namespace, "dev.deckr.openhab.service");
    assert_eq!(other_terms.service_namespace, "dev.deckr.other.service");
    assert_ne!(terms.service_use_scope_id, other_terms.service_use_scope_id);
}

#[tokio::test]
async fn beacon_watch_emits_semantic_feature_events() {
    let state = MemoryStateStore::ttl_bound(DEFAULT_BEACON_TTL_SECONDS).unwrap();
    let beacon = Beacon::start(state.clone()).await.unwrap();
    let protocol = service_protocol(SERVICE_ID);
    let mut events = beacon.watch(&protocol.feature_id).unwrap();

    let advertiser = service_advertiser(&state, &protocol, SERVICE_ID, "ad-1", SESSION_ID);
    let handle = advertiser.publish().await.unwrap();
    let advertised = next_event(&mut events).await;
    assert_eq!(advertised.event_type, BeaconFeatureEventType::Advertised);
    assert_eq!(
        advertised.candidate.unwrap().advertisement.advertisement_id,
        "ad-1"
    );

    advertiser.withdraw(&handle).await.unwrap();
    let withdrawn = next_event(&mut events).await;
    assert_eq!(withdrawn.event_type, BeaconFeatureEventType::Withdrawn);
    assert!(withdrawn.previous.is_some());
}

#[tokio::test]
async fn service_directory_indexes_and_removes_live_descriptors() {
    let state = MemoryStateStore::ttl_bound(DEFAULT_BEACON_TTL_SECONDS).unwrap();
    let beacon = Beacon::start(state.clone()).await.unwrap();
    let protocol = service_protocol(SERVICE_ID);
    let directory = ServiceDirectory::new(beacon, protocol.clone()).unwrap();
    directory.start();
    directory.wait_ready().await;
    assert!(directory.is_current());
    assert!(directory.descriptors().is_empty());

    let first_advertiser = service_advertiser(&state, &protocol, SERVICE_ID, "ad-1", SESSION_ID);
    let first = first_advertiser.publish().await.unwrap();
    wait_until(|| directory.descriptors().len() == 1).await;

    let backup_advertiser = service_advertiser(
        &state,
        &protocol,
        BACKUP_SERVICE_ID,
        "ad-2",
        "backup-session",
    );
    let backup = backup_advertiser.publish().await.unwrap();
    wait_until(|| directory.descriptors().len() == 2).await;

    let query = ServiceQuery {
        service_id: Some(BACKUP_SERVICE_ID.to_string()),
        operations: BTreeSet::from(["sendCommand".to_string()]),
        views: BTreeSet::from(["items".to_string()]),
        endpoint: Some(EndpointAddress::parse(service_address(BACKUP_SERVICE_ID)).unwrap()),
        session_id: Some("backup-session".to_string()),
        ..ServiceQuery::default()
    };
    let resolver = ServiceResolver::new(directory.clone());
    let descriptor = resolver.resolve(&query).unwrap();
    assert_eq!(descriptor.service_id, BACKUP_SERVICE_ID);
    assert_eq!(
        descriptor.views["items"].key_prefix,
        service_view_prefix(BACKUP_SERVICE_ID, "items")
    );

    state
        .put(
            &first.key,
            serde_json::json!({
                "schema": BEACON_ADVERTISEMENT_SCHEMA_ID,
                "advertisementId": "ad-1"
            }),
            Some(DEFAULT_BEACON_TTL_SECONDS),
        )
        .await
        .unwrap();
    wait_until(|| directory.descriptors().len() == 1).await;

    backup_advertiser.withdraw(&backup).await.unwrap();
    wait_until(|| directory.descriptors().is_empty()).await;

    let second_advertiser =
        service_advertiser(&state, &protocol, SERVICE_ID, "ad-3", "service-session-2");
    let second = second_advertiser.publish().await.unwrap();
    wait_until(|| directory.descriptors().len() == 1).await;

    second_advertiser.withdraw(&second).await.unwrap();
    wait_until(|| directory.descriptors().is_empty()).await;
    directory.close();
}

#[tokio::test]
async fn service_directory_replays_current_watch_descriptors() {
    let state = MemoryStateStore::ttl_bound(DEFAULT_BEACON_TTL_SECONDS).unwrap();
    let beacon = Beacon::start(state.clone()).await.unwrap();
    let protocol = service_protocol(SERVICE_ID);
    publish_service(&state, &protocol, SERVICE_ID, "ad-1", SESSION_ID).await;
    publish_service(
        &state,
        &protocol,
        BACKUP_SERVICE_ID,
        "ad-2",
        "backup-session",
    )
    .await;

    let directory = ServiceDirectory::new(beacon, protocol).unwrap();
    directory.start();
    directory.wait_ready().await;

    let service_ids = directory
        .descriptors()
        .into_iter()
        .map(|descriptor| descriptor.service_id)
        .collect::<Vec<_>>();
    assert_eq!(service_ids, vec![SERVICE_ID, BACKUP_SERVICE_ID]);
    directory.close();
}

fn service_protocol(_service_id: &str) -> ServiceProtocol {
    let mut views = BTreeMap::new();
    views.insert(
        "items".to_string(),
        ServiceViewFamilyDefinition::new("deckr_openhab_service_view_v1").unwrap(),
    );
    ServiceProtocol::new(
        "dev.deckr.openhab.service",
        "dev.deckr.openhab.service",
        "dev.deckr.openhab.service.advertisement.v1",
        "dev.deckr.openhab.service_use.v1",
        ["setItemScope", "sendCommand"],
        views,
    )
    .unwrap()
}

fn service_advertiser(
    state: &MemoryStateStore,
    protocol: &ServiceProtocol,
    service_id: &str,
    advertisement_id: &str,
    session_id: &str,
) -> BeaconAdvertiser<MemoryStateStore> {
    let payload = protocol
        .advertisement_payload(service_id, session_id, ServiceBackendStatus::Available)
        .unwrap()
        .to_value()
        .unwrap();
    BeaconAdvertiser::new(
        state.clone(),
        protocol.feature_id.clone(),
        EndpointAddress::parse(service_address(service_id)).unwrap(),
        session_id,
    )
    .advertisement_id(advertisement_id)
    .payload(payload)
}

async fn publish_service(
    state: &MemoryStateStore,
    protocol: &ServiceProtocol,
    service_id: &str,
    advertisement_id: &str,
    session_id: &str,
) {
    service_advertiser(state, protocol, service_id, advertisement_id, session_id)
        .publish()
        .await
        .unwrap();
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..100 {
        if predicate() {
            return;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition did not become true");
}

async fn next_event(
    events: &mut deckr::beacon::BeaconFeatureWatchStream,
) -> deckr::beacon::BeaconFeatureEvent {
    time::timeout(Duration::from_secs(1), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
