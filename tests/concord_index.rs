use deckr::authority::ContractPointer;
use deckr::concord::{
    ConcordCoordinator, ContractFilters, ContractState, ContractValidityStatus, ValidityReason,
};
use deckr::endpoint::EndpointAddress;
use deckr::keys::{concord_contract_key, concord_participant_token_key};
use deckr::runtime::MaterializedConcord;
use deckr::state::{MaterializedStateStore, MemoryStateStore, StateStore};
use futures_util::{FutureExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

fn record(id: &str, profile: &str, participant: &str, state: &str) -> Value {
    json!({"contractId":id,"generation":1,"participants":[participant],"attachedParticipants":[],"profile":profile,"state":state})
}
async fn sources(
    raw: MemoryStateStore,
    tokens: MemoryStateStore,
) -> (
    MaterializedConcord<MemoryStateStore, MemoryStateStore>,
    MaterializedStateStore<MemoryStateStore>,
    MaterializedStateStore<MemoryStateStore>,
) {
    let c = MaterializedStateStore::start(raw, "contracts.")
        .await
        .unwrap();
    let t = MaterializedStateStore::start(tokens, "contracts.")
        .await
        .unwrap();
    let concord = ConcordCoordinator::new(c.clone(), t.clone());
    concord.wait_current().await.unwrap();
    (concord, c, t)
}
async fn bounded<R>(work: impl std::future::Future<Output = R>) -> R {
    tokio::time::timeout(Duration::from_secs(3), work)
        .await
        .expect("operation completes")
}

#[tokio::test]
async fn filters_use_the_smallest_index_and_unrelated_contracts_are_never_evaluated() {
    let raw = MemoryStateStore::new();
    for n in 0..500 {
        let id = format!("unrelated-{n}");
        let mut value = record(&id, "org.example.jobs", "worker:other", "open");
        value["terms"] = json!({"not":"a valid terms hash"});
        raw.put(&concord_contract_key(&id, 1), value, None)
            .await
            .unwrap();
    }
    for n in 0..5 {
        let id = format!("cancelled-{n}");
        raw.put(
            &concord_contract_key(&id, 1),
            record(&id, "org.example.jobs", "worker:other", "cancelled"),
            None,
        )
        .await
        .unwrap();
    }
    let key = concord_contract_key("selected", 1);
    raw.put(
        &key,
        record("selected", "org.example.jobs", "worker:selected", "open"),
        None,
    )
    .await
    .unwrap();
    let (concord, c, t) = sources(raw, MemoryStateStore::ttl_bound(120).unwrap()).await;
    assert_eq!(concord.view_stats().unwrap().evaluations, 0);
    let participant = EndpointAddress::parse_protocol("worker:selected").unwrap();
    let filter = ContractFilters {
        profile: Some("org.example.jobs"),
        participant: Some(&participant),
        ..Default::default()
    };
    let selected = concord.contracts_cached(filter).unwrap();
    assert_eq!(selected.len(), 1);
    let stats = concord.view_stats().unwrap();
    assert_eq!(stats.indexed_contracts, 506);
    assert_eq!(stats.evaluations, 1);
    assert_eq!(stats.candidate_visits, 1);
    let mut watch = concord.clone().watch_state(filter).unwrap();
    assert_eq!(watch.next().await.unwrap().contracts.len(), 1);
    for _ in 0..1000 {
        assert_eq!(concord.contracts_cached(filter).unwrap().len(), 1);
        assert_eq!(
            concord.validate_cached(&selected[0], None).status,
            ContractValidityStatus::NotYetFulfilled
        );
    }
    assert_eq!(concord.view_stats().unwrap().evaluations, 1);
    let before = concord.view_stats().unwrap();
    let cancelled = concord
        .contracts_cached(ContractFilters {
            profile: Some("org.example.jobs"),
            state: Some(ContractState::Cancelled),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(cancelled.len(), 5);
    let after = concord.view_stats().unwrap();
    assert_eq!(after.candidate_visits - before.candidate_visits, 5);
    assert_eq!(after.evaluations - before.evaluations, 5);
    c.put(
        &concord_contract_key("new-unrelated", 1),
        json!({"participants":["worker:other"],"profile":"org.example.jobs","broken":true}),
        None,
    )
    .await
    .unwrap();
    assert_eq!(concord.view_stats().unwrap().evaluations, 6);
    assert!(watch.next().now_or_never().is_none());
    let pointer = ContractPointer {
        contract_id: "selected".into(),
        generation: 1,
    };
    assert_eq!(
        concord
            .contracts_cached(ContractFilters {
                pointer: Some(&pointer),
                contract_id: Some("other"),
                ..Default::default()
            })
            .unwrap()
            .len(),
        0
    );
    concord.close_sources_and_wait().await;
    assert!(!c.is_current());
    assert!(!t.is_current());
}

#[tokio::test]
async fn token_changes_invalidate_only_the_related_contract_and_session_assertions_are_cached() {
    let raw = MemoryStateStore::new();
    let tokens = MemoryStateStore::ttl_bound(120).unwrap();
    let participant = EndpointAddress::parse_protocol("worker:one").unwrap();
    for id in ["a", "b"] {
        let mut value = record(id, "jobs", participant.as_str(), "open");
        value["attachedParticipants"] = json!([participant.as_str()]);
        raw.put(&concord_contract_key(id, 1), value, None)
            .await
            .unwrap();
        tokens.put(&concord_participant_token_key(id,1,&participant),json!({"contractId":id,"generation":1,"participant":participant.as_str(),"sessionId":"session","tokenId":id,"refreshSeq":1,"ttlSeconds":120}),None).await.unwrap();
    }
    let (concord, _c, t) = sources(raw, tokens).await;
    let contracts = concord.contracts_cached(Default::default()).unwrap();
    assert_eq!(concord.view_stats().unwrap().evaluations, 2);
    let sessions = BTreeMap::from([(participant.to_string(), "session".to_string())]);
    for _ in 0..1000 {
        assert!(concord
            .validate_cached(&contracts[0], Some(&sessions))
            .valid());
    }
    assert_eq!(concord.view_stats().unwrap().evaluations, 3);
    let token_key = concord_participant_token_key("b", 1, &participant);
    t.put(&token_key, json!({"invalid":"token"}), None)
        .await
        .unwrap();
    assert!(concord
        .validate_cached(&contracts[0], Some(&sessions))
        .valid());
    assert_eq!(concord.view_stats().unwrap().evaluations, 3);
    assert_eq!(
        concord.validate_cached(&contracts[1], None).reason_code,
        Some(ValidityReason::TokenMalformed)
    );
    assert_eq!(concord.view_stats().unwrap().evaluations, 4);
    let wrong = BTreeMap::from([(participant.to_string(), "successor".to_string())]);
    assert_eq!(
        concord.validate_cached(&contracts[0], Some(&wrong)).status,
        ContractValidityStatus::SessionMismatch
    );
    assert!(concord
        .validate_cached(&contracts[0], Some(&sessions))
        .valid());
    assert_eq!(concord.view_stats().unwrap().cached_validities, 2);
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn selector_changes_and_malformed_replacements_update_complete_membership() {
    let raw = MemoryStateStore::new();
    let key = concord_contract_key("change", 1);
    raw.put(&key, record("change", "old", "worker:one", "open"), None)
        .await
        .unwrap();
    let (concord, c, _t) = sources(raw, MemoryStateStore::ttl_bound(120).unwrap()).await;
    let mut old = concord
        .watch_state(ContractFilters {
            profile: Some("old"),
            ..Default::default()
        })
        .unwrap();
    let mut new = concord
        .watch_state(ContractFilters {
            profile: Some("new"),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(old.next().await.unwrap().contracts.len(), 1);
    assert!(new.next().await.unwrap().contracts.is_empty());
    let mut malformed = record("change", "old", "worker:one", "open");
    malformed["unknownField"] = json!(true);
    c.put(&key, malformed.clone(), None).await.unwrap();
    let invalid = bounded(old.next()).await.unwrap();
    assert_eq!(
        invalid.contracts[0].validity.reason_code,
        Some(ValidityReason::ContractMalformed)
    );
    malformed["profile"] = json!("new");
    c.put(&key, malformed, None).await.unwrap();
    assert!(bounded(old.next()).await.unwrap().contracts.is_empty());
    assert_eq!(
        bounded(new.next()).await.unwrap().contracts[0]
            .validity
            .reason_code,
        Some(ValidityReason::ContractMalformed)
    );
    c.delete(&key, None).await.unwrap();
    assert!(bounded(new.next()).await.unwrap().contracts.is_empty());
    assert_eq!(concord.view_stats().unwrap().indexed_contracts, 0);
    assert_eq!(concord.view_stats().unwrap().cached_validities, 0);
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn hundreds_of_logical_watches_share_one_subscription_to_each_source() {
    let (concord, c, t) = sources(
        MemoryStateStore::new(),
        MemoryStateStore::ttl_bound(120).unwrap(),
    )
    .await;
    let mut watches = Vec::new();
    for _ in 0..128 {
        let mut watch = concord.clone().watch_state(Default::default()).unwrap();
        watch.next().await.unwrap();
        watches.push(watch);
    }
    let mut legacy = Vec::new();
    for _ in 0..128 {
        legacy.push(
            concord
                .watch_contract_notifications_cached(None, None)
                .await
                .unwrap(),
        );
    }
    assert!(concord.watch_state(Default::default()).is_err());
    assert_eq!(concord.view_stats().unwrap().subscribers, 256);
    let extra_c: Vec<_> = (0..255).map(|_| c.watch_snapshots("").unwrap()).collect();
    let extra_t: Vec<_> = (0..255).map(|_| t.watch_snapshots("").unwrap()).collect();
    assert!(c.watch_snapshots("").is_err());
    assert!(t.watch_snapshots("").is_err());
    concord.close_sources_and_wait().await;
    for mut watch in watches {
        assert!(bounded(watch.next()).await.unwrap().closed);
        assert!(watch.next().await.is_none());
    }
    for mut watch in legacy {
        assert!(matches!(
            bounded(watch.next()).await,
            Err(deckr::Error::Closed(_))
        ));
    }
    drop((extra_c, extra_t));
}

#[tokio::test]
async fn compatibility_notifications_use_filtered_observations_and_preserve_malformed_records() {
    use deckr::concord::ConcordNotificationSource;
    use deckr::state::StateOperation;
    let (concord, c, t) = sources(
        MemoryStateStore::new(),
        MemoryStateStore::ttl_bound(120).unwrap(),
    )
    .await;
    let participant = EndpointAddress::parse_protocol("worker:one").unwrap();
    let mut watch = concord
        .watch_contract_notifications_cached(Some("jobs"), Some(&participant))
        .await
        .unwrap();
    let key = concord_contract_key("selected", 1);
    let mut selected = record("selected", "jobs", participant.as_str(), "open");
    selected["unknown"] = json!(true);
    let created = c.put(&key, selected.clone(), None).await.unwrap();
    let notification = bounded(watch.next()).await.unwrap();
    assert_eq!(notification.contract_id, "selected");
    assert!(notification.contract.is_none());
    assert_eq!(notification.change.entry.unwrap(), created);
    selected.as_object_mut().unwrap().remove("unknown");
    c.put(&key, selected.clone(), None).await.unwrap();
    assert!(bounded(watch.next()).await.unwrap().contract.is_some());
    let token_key = concord_participant_token_key("selected", 1, &participant);
    let token = t.put(&token_key, json!({"contractId":"selected","generation":1,"participant":participant,"sessionId":"session","tokenId":"token","refreshSeq":1,"ttlSeconds":120}), None).await.unwrap();
    let notification = bounded(watch.next()).await.unwrap();
    assert_eq!(notification.source, ConcordNotificationSource::Token);
    assert_eq!(notification.change.entry.unwrap(), token);
    selected["profile"] = json!("elsewhere");
    c.put(&key, selected, None).await.unwrap();
    let notification = bounded(watch.next()).await.unwrap();
    assert_eq!(notification.source, ConcordNotificationSource::Contract);
    assert_eq!(notification.operation, StateOperation::Delete);
    let evaluated = concord.view_stats().unwrap().evaluations;
    c.put(
        &concord_contract_key("unrelated", 1),
        record("unrelated", "jobs", "worker:other", "open"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(concord.view_stats().unwrap().evaluations, evaluated);
    assert!(watch.next().now_or_never().is_none());
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn slow_watch_overflow_keeps_full_membership_and_coalesces_change_details() {
    let (concord, c, _t) = sources(
        MemoryStateStore::new(),
        MemoryStateStore::ttl_bound(120).unwrap(),
    )
    .await;
    let mut watch = concord
        .watch_state(ContractFilters {
            profile: Some("jobs"),
            ..Default::default()
        })
        .unwrap();
    watch.next().await.unwrap();
    for n in 0..300 {
        let id = format!("job-{n}");
        c.put(
            &concord_contract_key(&id, 1),
            record(&id, "jobs", "worker:one", "open"),
            None,
        )
        .await
        .unwrap();
    }
    assert_eq!(concord.view_stats().unwrap().indexed_contracts, 300);
    let latest = bounded(watch.next()).await.unwrap();
    assert_eq!(latest.contracts.len(), 300);
    assert!(latest.changed.is_empty());
    assert!(latest.resnapshot_required);
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn synchronous_queries_can_initialize_off_runtime_threads_and_last_owner_drop_closes_watches()
{
    let raw = MemoryStateStore::new();
    raw.put(
        &concord_contract_key("sync", 1),
        record("sync", "jobs", "worker:one", "open"),
        None,
    )
    .await
    .unwrap();
    let (concord, c, t) = sources(raw, MemoryStateStore::ttl_bound(120).unwrap()).await;
    let reader = concord.clone();
    let count =
        std::thread::spawn(move || reader.contracts_cached(Default::default()).unwrap().len())
            .join()
            .unwrap();
    assert_eq!(count, 1);
    let mut watch = concord.watch_state(Default::default()).unwrap();
    watch.next().await.unwrap();
    drop(concord);
    assert!(bounded(watch.next()).await.unwrap().closed);
    assert!(c.is_current());
    assert!(t.is_current());
    c.close_and_wait().await;
    t.close_and_wait().await;
}
