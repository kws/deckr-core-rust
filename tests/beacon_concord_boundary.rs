use std::collections::BTreeMap;

use deckr::beacon::{find_candidates, BeaconAdvertiser, DEFAULT_BEACON_TTL_SECONDS};
use deckr::concord::{
    ConcordCoordinator, ContractState, ContractValidityStatus, CreateContractSpec,
    DEFAULT_CONCORD_TOKEN_TTL_SECONDS,
};
use deckr::endpoint::EndpointAddress;
use deckr::state::{MemoryStateStore, StateStore};

const FEATURE_ID: &str = "dev.deckr.test.feature";

#[tokio::test]
async fn beacon_publish_or_refresh_recreates_missing_advertisement() {
    let beacon_state = MemoryStateStore::ttl_bound(DEFAULT_BEACON_TTL_SECONDS).unwrap();
    let advertiser_endpoint = EndpointAddress::parse("hardware_manager:rust").unwrap();
    let advertiser = BeaconAdvertiser::new(
        beacon_state.clone(),
        FEATURE_ID,
        advertiser_endpoint,
        "advertiser-session",
    )
    .advertisement_id("ad-1")
    .payload(serde_json::json!({"generation": 1}));

    let first = advertiser.publish().await.unwrap();
    beacon_state
        .delete(&first.key, Some(first.revision))
        .await
        .unwrap();
    let recovered = advertiser.publish_or_refresh(Some(&first)).await.unwrap();

    assert_eq!(recovered.key, first.key);
    assert!(recovered.revision > first.revision);
    let candidates = find_candidates(&beacon_state, FEATURE_ID).await.unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].advertisement.advertisement_id, "ad-1");
    assert_eq!(candidates[0].advertisement.refresh_seq, 1);
}

#[tokio::test]
async fn withdrawing_beacon_advertisement_leaves_concord_contract_open() {
    let beacon_state = MemoryStateStore::ttl_bound(DEFAULT_BEACON_TTL_SECONDS).unwrap();
    let contract_state = MemoryStateStore::new();
    let token_state = MemoryStateStore::ttl_bound(DEFAULT_CONCORD_TOKEN_TTL_SECONDS).unwrap();
    let advertiser_endpoint = EndpointAddress::parse("hardware_manager:rust").unwrap();
    let controller_endpoint = EndpointAddress::parse("controller:main").unwrap();
    let concord = ConcordCoordinator::new(contract_state, token_state);

    let advertiser = BeaconAdvertiser::new(
        beacon_state.clone(),
        FEATURE_ID,
        advertiser_endpoint.clone(),
        "advertiser-session",
    )
    .advertisement_id("ad-1")
    .payload(serde_json::json!({}));
    let advertisement = advertiser.publish().await.unwrap();
    let contract = concord
        .create_contract(CreateContractSpec {
            participants: vec![advertiser_endpoint.clone(), controller_endpoint],
            contract_id: Some("contract-1".to_string()),
            generation: 1,
            profile: None,
            terms: None,
            created_by: Some(advertiser_endpoint.clone()),
            participant_sessions: BTreeMap::new(),
            supersedes: None,
        })
        .await
        .unwrap();

    advertiser.withdraw(&advertisement).await.unwrap();

    assert!(find_candidates(&beacon_state, FEATURE_ID)
        .await
        .unwrap()
        .is_empty());
    let record = concord.contract_record(&contract).await.unwrap().unwrap();
    assert_eq!(record.state, ContractState::Open);
    assert_eq!(
        concord.validate(&contract, None).await.status,
        ContractValidityStatus::NotYetFulfilled
    );
}

#[tokio::test]
async fn cancelling_concord_contract_leaves_beacon_advertisement_available() {
    let beacon_state = MemoryStateStore::ttl_bound(DEFAULT_BEACON_TTL_SECONDS).unwrap();
    let contract_state = MemoryStateStore::new();
    let token_state = MemoryStateStore::ttl_bound(DEFAULT_CONCORD_TOKEN_TTL_SECONDS).unwrap();
    let advertiser_endpoint = EndpointAddress::parse("hardware_manager:rust").unwrap();
    let controller_endpoint = EndpointAddress::parse("controller:main").unwrap();
    let concord = ConcordCoordinator::new(contract_state, token_state);

    let advertiser = BeaconAdvertiser::new(
        beacon_state.clone(),
        FEATURE_ID,
        advertiser_endpoint.clone(),
        "advertiser-session",
    )
    .advertisement_id("ad-1")
    .payload(serde_json::json!({}));
    let advertisement = advertiser.publish().await.unwrap();
    let contract = concord
        .create_contract(CreateContractSpec {
            participants: vec![advertiser_endpoint.clone(), controller_endpoint],
            contract_id: Some("contract-1".to_string()),
            generation: 1,
            profile: None,
            terms: None,
            created_by: Some(advertiser_endpoint.clone()),
            participant_sessions: BTreeMap::new(),
            supersedes: None,
        })
        .await
        .unwrap();

    concord
        .cancel(&contract, &advertiser_endpoint, Some("test".to_string()))
        .await
        .unwrap();

    assert_eq!(
        concord.validate(&contract, None).await.status,
        ContractValidityStatus::Cancelled
    );
    let candidates = find_candidates(&beacon_state, FEATURE_ID).await.unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].key, advertisement.key);
}
