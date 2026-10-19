use std::collections::BTreeMap;

use deckr::concord::{
    evaluate_contract, ConcordCoordinator, ContractHandle, ContractObservations,
    CreateContractSpec, ValidityReason,
};
use deckr::endpoint::EndpointAddress;
use deckr::state::{MaterializedStateStore, MemoryStateStore, StateStore};
use deckr::MutationCode;
use serde_json::json;

async fn observations() -> (
    ConcordCoordinator<MemoryStateStore, MemoryStateStore>,
    MemoryStateStore,
    MemoryStateStore,
    ContractHandle,
    ContractObservations,
) {
    let contracts = MemoryStateStore::default();
    let tokens = MemoryStateStore::ttl_bound(120).unwrap();
    let concord = ConcordCoordinator::new(contracts.clone(), tokens.clone());
    let a = EndpointAddress::parse_protocol("worker:a").unwrap();
    let b = EndpointAddress::parse_protocol("worker:b").unwrap();
    let mut spec = CreateContractSpec::new(vec![b.clone(), a.clone()]);
    spec.terms = Some(json!({"example": true}));
    let contract = concord.create_contract(spec).await.unwrap();
    concord.attach(&contract, &a, "a", None).await.unwrap();
    concord.attach(&contract, &b, "b", None).await.unwrap();
    let observations = ContractObservations {
        contract: contracts.get(&contract.key).await.unwrap(),
        tokens: tokens
            .items("contracts.")
            .await
            .unwrap()
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect(),
        current_sessions: BTreeMap::new(),
    };
    (concord, contracts, tokens, contract, observations)
}

#[tokio::test]
async fn fixed_observations_have_typed_reasons_and_preserve_prior_token_identities() {
    let (_, _, _, handle, observations) = observations().await;
    assert!(evaluate_contract(&handle, &observations).valid());
    let a = handle.participants[0].to_string();
    let b = handle.participants[1].to_string();
    let last_key = deckr::concord::concord_participant_token_key(
        &handle.contract_id,
        1,
        &handle.participants[1],
    );
    let changes = [
        (
            "contractId",
            json!("different"),
            ValidityReason::TokenContractMismatch,
            false,
        ),
        (
            "generation",
            json!(2),
            ValidityReason::TokenGenerationMismatch,
            false,
        ),
        (
            "participant",
            json!("worker:other"),
            ValidityReason::TokenParticipantMismatch,
            false,
        ),
        (
            "termsHash",
            json!("wrong"),
            ValidityReason::TokenTermsHashMismatch,
            true,
        ),
        (
            "refreshSeq",
            json!(0),
            ValidityReason::TokenMalformed,
            false,
        ),
    ];
    for (field, value, reason, keep_last) in changes {
        let mut changed = observations.clone();
        changed.tokens.get_mut(&last_key).unwrap().value[field] = value;
        let evaluated = evaluate_contract(&handle, &changed);
        assert_eq!(evaluated.reason_code, Some(reason));
        assert_eq!(evaluated.status, reason.status());
        assert!(evaluated.tokens.contains_key(&a));
        assert_eq!(evaluated.tokens.contains_key(&b), keep_last);
        assert_eq!(
            evaluated.participant.as_ref().map(|p| p.as_str()),
            Some(b.as_str())
        );
    }
    let mut changed = observations.clone();
    changed.tokens.get_mut(&last_key).unwrap().key = "wrong-key".into();
    assert_eq!(
        evaluate_contract(&handle, &changed).reason_code,
        Some(ValidityReason::TokenKeyMismatch)
    );
    changed = observations.clone();
    changed.tokens.remove(&last_key);
    assert_eq!(
        evaluate_contract(&handle, &changed).reason_code,
        Some(ValidityReason::TokenMissing)
    );
    changed.contract.as_mut().unwrap().value["attachedParticipants"] = json!([a]);
    assert_eq!(
        evaluate_contract(&handle, &changed).reason_code,
        Some(ValidityReason::ParticipantNotAttached)
    );
    changed = observations.clone();
    changed.current_sessions.insert(b, "new-session".into());
    assert_eq!(
        evaluate_contract(&handle, &changed).reason_code,
        Some(ValidityReason::TokenSessionMismatch)
    );
    changed = observations.clone();
    changed.contract.as_mut().unwrap().key = "wrong-key".into();
    assert_eq!(
        evaluate_contract(&handle, &changed).reason_code,
        Some(ValidityReason::ContractKeyMismatch)
    );
    changed = observations.clone();
    changed.contract.as_mut().unwrap().value = json!({"broken": true});
    assert_eq!(
        evaluate_contract(&handle, &changed).reason_code,
        Some(ValidityReason::ContractMalformed)
    );
    changed.contract = None;
    assert_eq!(
        evaluate_contract(&handle, &changed).reason_code,
        Some(ValidityReason::ContractMissing)
    );
    let mut other_handle = handle.clone();
    other_handle.generation = 2;
    assert_eq!(
        evaluate_contract(&other_handle, &observations).reason_code,
        Some(ValidityReason::ContractPointerMismatch)
    );
    assert!(
        evaluate_contract(&handle, &observations).valid(),
        "evaluation never changes its inputs"
    );
}

#[tokio::test]
async fn exact_cached_and_pure_paths_agree_on_malformed_and_identity_mismatched_records() {
    let (exact, contracts, tokens, handle, _) = observations().await;
    let c = MaterializedStateStore::start(contracts.clone(), "contracts.")
        .await
        .unwrap();
    let t = MaterializedStateStore::start(tokens.clone(), "contracts.")
        .await
        .unwrap();
    let cached = ConcordCoordinator::new(c.clone(), t.clone());
    cached.wait_current().await.unwrap();
    let token_key = deckr::concord::concord_participant_token_key(
        &handle.contract_id,
        1,
        &handle.participants[1],
    );
    for value in [json!({"broken": true}), {
        let mut token = tokens.get(&token_key).await.unwrap().unwrap().value;
        token["participant"] = json!("worker:other");
        token
    }] {
        t.put(&token_key, value, Some(120)).await.unwrap();
        let fixed = ContractObservations {
            contract: contracts.get(&handle.key).await.unwrap(),
            tokens: tokens
                .items("contracts.")
                .await
                .unwrap()
                .into_iter()
                .map(|entry| (entry.key.clone(), entry))
                .collect(),
            current_sessions: BTreeMap::new(),
        };
        let expected = evaluate_contract(&handle, &fixed);
        assert_eq!(exact.validate(&handle, None).await, expected);
        assert_eq!(cached.validate_cached(&handle, None), expected);
    }
    c.close_and_wait().await;
    assert_eq!(
        cached.validate_cached(&handle, None).reason_code,
        Some(ValidityReason::SourceUnavailable)
    );
    t.close_and_wait().await;
}

#[tokio::test]
async fn guarded_mutations_reject_wrong_contract_and_token_keys_without_writes() {
    let (concord, contracts, tokens, handle, observations) = observations().await;
    let mut foreign = handle.clone();
    foreign.contract_id = "not-the-record".into();
    let error = concord
        .cancel(&foreign, &foreign.participants[0], None)
        .await
        .unwrap_err();
    assert_eq!(
        error.mutation_code(),
        Some(MutationCode::ContractIdentityMismatch)
    );
    let error = concord
        .attach(&foreign, &foreign.participants[0], "a", None)
        .await
        .unwrap_err();
    assert_eq!(
        error.mutation_code(),
        Some(MutationCode::ContractIdentityMismatch)
    );
    let mut token = concord
        .participant_token(&handle, &handle.participants[0])
        .await
        .unwrap()
        .unwrap();
    let real = tokens.get(&token.key).await.unwrap().unwrap();
    token.key = "wrong-key".into();
    tokens.put(&token.key, real.value, Some(120)).await.unwrap();
    let error = concord.withdraw(&token).await.unwrap_err();
    assert_eq!(
        error.mutation_code(),
        Some(MutationCode::TokenIdentityMismatch)
    );
    assert!(tokens.get(&token.key).await.unwrap().is_some());
    assert_eq!(
        contracts.get(&handle.key).await.unwrap(),
        observations.contract
    );
}

#[tokio::test]
async fn filtered_views_ignore_unrelated_updates_and_keep_malformed_selected_records() {
    use deckr::concord::ContractFilters;
    use futures_util::{FutureExt, StreamExt};
    let (_, contracts, tokens, handle, _) = observations().await;
    let c = MaterializedStateStore::start(contracts.clone(), "contracts.")
        .await
        .unwrap();
    let t = MaterializedStateStore::start(tokens, "contracts.")
        .await
        .unwrap();
    let concord = ConcordCoordinator::new(c.clone(), t.clone());
    concord.wait_current().await.unwrap();
    let mut watch = concord
        .watch_state(ContractFilters {
            participant: Some(&handle.participants[0]),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(watch.next().await.unwrap().contracts.len(), 1);
    c.put(
        "contracts.dW5yZWxhdGVk.1.meta",
        json!({"participants": ["worker:other"], "malformed": true}),
        None,
    )
    .await
    .unwrap();
    assert!(watch.next().now_or_never().is_none());
    let mut value = contracts.get(&handle.key).await.unwrap().unwrap().value;
    value["schemaId"] = json!("not-a-contract-schema");
    c.put(&handle.key, value, None).await.unwrap();
    let changed = watch.next().await.unwrap();
    assert_eq!(changed.contracts.len(), 1);
    assert_eq!(
        changed.contracts[0].validity.reason_code,
        Some(ValidityReason::ContractMalformed)
    );
    assert_eq!(changed.changed.len(), 1);
    concord.close_sources_and_wait().await;
}
