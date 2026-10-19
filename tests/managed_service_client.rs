#[path = "support/service_bus.rs"]
mod service_bus;

use std::collections::BTreeSet;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use deckr::authority::ContractPointer;
use deckr::beacon::{AdvertisementHandle, BeaconAdvertiser};
use deckr::concord::{ContractRecord, ContractState, ManagedParticipant, ParticipantCandidates};
use deckr::endpoint::EndpointAddress;
use deckr::lifecycle::Phase;
use deckr::messaging::MessageSession;
use deckr::runtime::ProtocolRuntime;
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use deckr::session::EndpointSession;
use deckr::state::{MaterializedStateStore, MemoryStateStore, StateStore};
use deckr::Result;
use futures_util::StreamExt;
use serde_json::json;
use service_bus::Bus;

type Connection = ServiceConnection<MemoryStateStore, MemoryStateStore, Bus>;
type Participant = ManagedParticipant<MemoryStateStore, MemoryStateStore>;
struct Harness {
    runtime: ProtocolRuntime<MemoryStateStore, MemoryStateStore, MemoryStateStore>,
    advertisements: MemoryStateStore,
    contracts: MemoryStateStore,
    tokens: MemoryStateStore,
    directory: ServiceDirectory<MemoryStateStore>,
    protocol: ValidatedServiceProtocol,
    messages: MessageSession<Bus>,
    bus: Bus,
}
impl Harness {
    async fn new() -> Self {
        let advertisements = MemoryStateStore::ttl_bound(300).unwrap();
        let contracts = MemoryStateStore::default();
        let tokens = MemoryStateStore::ttl_bound(30).unwrap();
        let runtime =
            ProtocolRuntime::start(advertisements.clone(), contracts.clone(), tokens.clone())
                .await
                .unwrap();
        let protocol=ServiceProtocol::from_value(json!({
            "namespace":"org.example.test","featureId":"org.example.test","advertisementProfile":"org.example.test.ad.v1","useProfile":"org.example.test.use.v1",
            "operations":{"read":{},"write":{}},
            "messages":{
                "read":{"intent":"query","direction":"consumer_to_service","operation":"read","exchangePattern":"request_reply","resultSchema":{"schemaId":"result","schema":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"]}}},
                "write":{"intent":"command","direction":"consumer_to_service","operation":"write","exchangePattern":"one_way","paramsSchema":{"schemaId":"write","schema":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}}},
                "changed":{"intent":"event","direction":"service_to_consumer","exchangePattern":"one_way","eventSchema":{"schemaId":"event","schema":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}}},
                "ask":{"intent":"query","direction":"service_to_consumer","operation":"read","exchangePattern":"request_reply","resultSchema":{"schemaId":"result","schema":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"]}}}
            },
            "viewFamilies":{"state":{"storeName":"test_views","writer":"service"},"requests":{"storeName":"test_views","writer":"consumer"}}
        })).unwrap();
        let directory = ServiceDirectory::new(runtime.beacon().clone(), protocol.clone()).unwrap();
        directory.try_start().unwrap();
        bounded(directory.wait_ready()).await;
        let protocol = ValidatedServiceProtocol::new(protocol, &SchemaRegistry::default()).unwrap();
        let bus = Bus::default();
        let session = EndpointSession::with_id(
            EndpointAddress::parse("controller:client").unwrap(),
            "consumer",
        )
        .unwrap();
        let messages = MessageSession::new(bus.clone(), session).unwrap();
        Self {
            runtime,
            advertisements,
            contracts,
            tokens,
            directory,
            protocol,
            messages,
            bus,
        }
    }
    async fn advertise(
        &self,
        id: &str,
        session: &str,
        ad: &str,
    ) -> (EndpointSession, AdvertisementHandle) {
        let endpoint = EndpointSession::with_id(
            EndpointAddress::parse(format!("service:{id}")).unwrap(),
            session,
        )
        .unwrap();
        let advertiser = BeaconAdvertiser::new(
            self.advertisements.clone(),
            self.protocol.protocol().feature_id.clone(),
            endpoint.endpoint().clone(),
            session,
        )
        .advertisement_id(ad)
        .payload(
            self.protocol
                .protocol()
                .advertisement_payload(id, session, ServiceBackendStatus::Available)
                .unwrap()
                .to_value()
                .unwrap(),
        );
        let handle = advertiser.publish().await.unwrap();
        until(|| {
            self.directory
                .descriptors()
                .iter()
                .any(|descriptor| descriptor.candidate.as_ref().unwrap().key == handle.key)
        })
        .await;
        (endpoint, handle)
    }
    fn participant(&self, session: EndpointSession) -> Participant {
        self.runtime
            .concord()
            .participate(
                session,
                Some(self.protocol.protocol().use_profile.clone()),
                |candidates: ParticipantCandidates| async move {
                    Ok(candidates
                        .contracts
                        .into_iter()
                        .map(|member| ContractPointer {
                            contract_id: member.contract.contract_id,
                            generation: member.contract.generation,
                        })
                        .collect())
                },
            )
            .unwrap()
    }
    fn spec(&self) -> ServiceClientSpec<MemoryStateStore, MemoryStateStore, MemoryStateStore, Bus> {
        ServiceClientSpec {
            directory: self.directory.clone(),
            protocol: self.protocol.clone(),
            concord: self.runtime.concord().clone(),
            messages: self.messages.clone(),
            target: ServiceTarget::matching(ServiceQuery::default()),
            policy: Arc::new(|descriptor: ServiceDescriptor| async move {
                Ok(Some(all_access(&descriptor)))
            }),
            options: ServiceClientOptions {
                negotiation_timeout: Duration::from_secs(10),
                retry_delay: Duration::from_millis(10),
            },
        }
    }
    async fn provider_connection(
        &self,
        participant: &Participant,
        client: &Connection,
    ) -> Connection {
        let contract = client.context().agreement().contract();
        let pointer = ContractPointer {
            contract_id: contract.contract_id.clone(),
            generation: contract.generation,
        };
        let agreement = bounded(async {
            loop {
                if let Some(agreement) = participant.agreement(&pointer) {
                    break agreement;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        bounded(agreement.wait_ready(None)).await.unwrap();
        let messages = MessageSession::new(self.bus.clone(), agreement.session().clone()).unwrap();
        let descriptor = client.context().descriptor().clone();
        let context = ServiceContext::new(
            agreement,
            self.protocol.clone(),
            descriptor.clone(),
            all_access(&descriptor),
        )
        .unwrap();
        ServiceConnection::new(context, messages).unwrap()
    }
    async fn records(&self) -> Vec<ContractRecord> {
        self.contracts
            .items("")
            .await
            .unwrap()
            .into_iter()
            .map(|entry| ContractRecord::from_value(entry.value).unwrap())
            .collect()
    }
    async fn close(self) {
        self.messages.close().await.unwrap();
        self.directory.close_and_wait().await;
        self.runtime.close().await;
    }
}
fn all_access(descriptor: &ServiceDescriptor) -> ServiceAccess {
    service_access(
        descriptor,
        descriptor.supported_operations.clone(),
        descriptor
            .views
            .keys()
            .map(|name| (name.clone(), vec![]))
            .collect(),
    )
    .unwrap()
}
fn body(name: &str) -> ServiceMessageBody {
    ServiceMessageBody::from_value(json!({"serviceNamespace":"org.example.test","name":name,"intent":if name=="changed" {"event"}else if name=="write" {"command"}else{"query"},"exchangePattern":if name=="read"||name=="ask" {"request_reply"}else{"one_way"}})).unwrap()
}
async fn bounded<R>(work: impl std::future::Future<Output = R>) -> R {
    tokio::time::timeout(Duration::from_secs(15), work)
        .await
        .expect("operation completes")
}
async fn until(mut check: impl FnMut() -> bool) {
    bounded(async {
        while !check() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test]
async fn races_sixty_four_unique_candidates_and_closes_every_loser_before_acceptance() {
    let h = Harness::new().await;
    let mut providers = Vec::new();
    for index in 0..65 {
        providers.push(
            h.advertise(
                &format!("candidate-{index:02}"),
                "service-session",
                &format!("ad-{index}"),
            )
            .await
            .0,
        );
    }
    h.advertise("candidate-00", "service-session", "duplicate-ad")
        .await;
    let mut spec = h.spec();
    spec.options.negotiation_timeout = Duration::from_secs(60);
    let client = ServiceClient::start(spec).unwrap();
    until(|| client.state().value.attempts == 64).await;
    let state = client.state();
    assert_eq!(state.value.admitted.len(), 64);
    assert!(!state.value.local_authority);
    assert_eq!(
        state
            .value
            .admitted
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .len(),
        64
    );
    assert!(!state
        .value
        .admitted
        .iter()
        .any(|candidate| candidate.endpoint.as_str() == "service:candidate-64"));
    bounded(async {
        while h.records().await.len() != 64 || h.tokens.items("").await.unwrap().len() != 64 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let participant = h.participant(providers[17].clone());
    let winner = bounded(client.wait_ready(None)).await.unwrap();
    assert_eq!(
        winner.context().descriptor().endpoint.as_str(),
        "service:candidate-17"
    );
    let records = h.records().await;
    assert_eq!(
        records
            .iter()
            .filter(|record| record.state == ContractState::Open)
            .count(),
        1
    );
    assert_eq!(h.tokens.items("").await.unwrap().len(), 2);
    assert_eq!(client.state().value.attempts, 64);
    client.close().await.unwrap();
    participant.close().await.unwrap();
    assert!(h.tokens.items("").await.unwrap().is_empty());
    h.close().await;
}

#[tokio::test]
async fn later_candidates_join_pending_race_while_other_policy_work_is_blocked() {
    let h = Harness::new().await;
    h.advertise("pending", "pending-session", "ad-pending")
        .await;
    h.advertise("blocked", "blocked-session", "ad-blocked")
        .await;
    let entered = Arc::new(AtomicUsize::new(0));
    let marker = entered.clone();
    let mut spec = h.spec();
    spec.policy = Arc::new(move |descriptor: ServiceDescriptor| {
        let marker = marker.clone();
        async move {
            if descriptor.service_id == "blocked" {
                marker.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            Ok(Some(all_access(&descriptor)))
        }
    });
    let client = ServiceClient::start(spec).unwrap();
    until(|| client.state().value.attempts == 1 && entered.load(Ordering::SeqCst) == 1).await;
    let (late, _) = h.advertise("late", "late-session", "ad-late").await;
    let participant = h.participant(late);
    let selected = bounded(client.wait_ready(None)).await.unwrap();
    assert_eq!(selected.context().descriptor().service_id, "late");
    assert_eq!(client.state().value.rounds, 1);
    assert_eq!(client.state().value.attempts, 2);
    client.close().await.unwrap();
    participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn accepted_service_survives_discovery_replacement_and_negotiates_fresh_successors() {
    let h = Harness::new().await;
    let (first, ad) = h.advertise("first", "one", "ad-first").await;
    let participant = h.participant(first);
    let client = ServiceClient::start(h.spec()).unwrap();
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    let pointer = connection
        .context()
        .agreement()
        .contract()
        .contract_id
        .clone();
    let mut states = client.watch_state().unwrap();
    states.next().await;
    h.advertisements.delete(&ad.key, None).await.unwrap();
    until(|| h.directory.descriptors().is_empty()).await;
    connection.authorize().unwrap();
    assert!(client.state().value.local_authority);
    let (successor, _) = h.advertise("first", "two", "ad-second").await;
    let successor_participant = h.participant(successor);
    assert_eq!(
        client
            .connection()
            .unwrap()
            .context()
            .descriptor()
            .session_id,
        "one"
    );
    let mut command = body("write");
    command.params.insert("value".into(), json!(5));
    connection
        .send(connection.message(command).unwrap())
        .await
        .unwrap();
    connection.context().agreement().request_close();
    bounded(async {
        loop {
            if states.next().await.unwrap().value.status == ServiceClientStatus::Reconnecting {
                break;
            }
        }
    })
    .await;
    until(|| {
        client
            .connection()
            .is_ok_and(|current| current.context().descriptor().session_id == "two")
    })
    .await;
    let fresh = client.connection().unwrap();
    assert_ne!(fresh.context().agreement().contract().contract_id, pointer);
    assert!(connection.authorize().is_err());
    assert_eq!(
        h.bus
            .published()
            .iter()
            .filter(|message| message
                .service_body()
                .is_ok_and(|body| body.name == "write"))
            .count(),
        1
    );
    client.close().await.unwrap();
    participant.close().await.unwrap();
    successor_participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn exact_selection_pins_the_supplied_candidate_and_requires_current_discovery() {
    let h = Harness::new().await;
    let (wanted, ad) = h.advertise("wanted", "wanted-session", "wanted-ad").await;
    let (other, _) = h.advertise("other", "other-session", "other-ad").await;
    let wanted_participant = h.participant(wanted);
    let other_participant = h.participant(other);
    let descriptor = h
        .directory
        .descriptors()
        .into_iter()
        .find(|descriptor| descriptor.service_id == "wanted")
        .unwrap();
    let mut spec = h.spec();
    spec.target = ServiceTarget::Exact(Box::new(descriptor.clone()));
    let client = ServiceClient::start(spec).unwrap();
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    assert_eq!(connection.context().descriptor().service_id, "wanted");
    assert_eq!(client.state().value.attempts, 1);
    client.close().await.unwrap();
    h.advertisements.delete(&ad.key, None).await.unwrap();
    until(|| h.directory.descriptors().len() == 1).await;
    let mut spec = h.spec();
    spec.target = ServiceTarget::Exact(Box::new(descriptor));
    spec.options.negotiation_timeout = Duration::from_millis(100);
    let missing = ServiceClient::start(spec).unwrap();
    assert!(missing
        .wait_ready(Some(Duration::from_millis(200)))
        .await
        .is_err());
    assert_eq!(missing.state().value.attempts, 0);
    missing.close().await.unwrap();
    wanted_participant.close().await.unwrap();
    other_participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn connections_validate_messages_replies_events_and_protected_views() {
    let h = Harness::new().await;
    let (session, _) = h
        .advertise("provider", "service-session", "provider-ad")
        .await;
    let provider_messages = MessageSession::new(h.bus.clone(), session.clone()).unwrap();
    let mut incoming = provider_messages.subscribe("services").await.unwrap();
    let participant = h.participant(session);
    let client = ServiceClient::start(h.spec()).unwrap();
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    let provider = h.provider_connection(&participant, &connection).await;
    let request = connection.message(body("read")).unwrap();
    let requester = connection.clone();
    let result = tokio::spawn(async move {
        requester
            .request(request, Duration::from_secs(3), None)
            .await
    });
    let received = bounded(incoming.next()).await.unwrap().unwrap();
    assert!(received.reply_address.is_some());
    let mut invalid = provider.message(body("read")).unwrap();
    invalid.body["result"] = json!({"answer":"wrong"});
    provider_messages.reply(&received, invalid).await.unwrap();
    let mut response = body("read");
    response.result = Some(json!({"answer":42}).as_object().unwrap().clone());
    response.status = Some(ServiceMessageStatus::Ok);
    provider.reply(&received, response).await.unwrap();
    assert_eq!(
        bounded(result)
            .await
            .unwrap()
            .unwrap()
            .service_body()
            .unwrap()
            .result
            .unwrap()["answer"],
        json!(42)
    );

    let mut watch = client.watch_messages().unwrap();
    let mut event = body("changed");
    event.event = Some(json!({"value":7}).as_object().unwrap().clone());
    provider
        .send(provider.message(event.clone()).unwrap())
        .await
        .unwrap();
    let delivered = bounded(watch.next()).await.unwrap().unwrap();
    delivered.authorize().unwrap();
    assert_eq!(
        delivered
            .delivery
            .message
            .service_body()
            .unwrap()
            .event
            .unwrap()["value"],
        json!(7)
    );
    let mut invalid = provider.message(event).unwrap();
    invalid.body["event"]["value"] = json!("bad");
    provider_messages.send(invalid).await.unwrap();
    until(|| client.state().value.rejected_messages == 1).await;
    let mut invalid = body("write");
    invalid.params.insert("value".into(), json!("bad"));
    assert!(connection
        .send(connection.message(invalid).unwrap())
        .await
        .is_err());
    assert!(connection
        .send(connection.message(body("read")).unwrap())
        .await
        .is_err());

    let source = MaterializedStateStore::start(MemoryStateStore::default(), "")
        .await
        .unwrap();
    let store = ServiceViewStore::new("test_views", source.clone()).unwrap();
    let writer = provider.view(&store, "state", None).unwrap();
    let reader = connection.view(&store, "state", None).unwrap();
    bounded(writer.wait_ready(None)).await.unwrap();
    bounded(reader.wait_ready(None)).await.unwrap();
    let key = format!(
        "{}current",
        connection.context().descriptor().views["state"].key_prefix
    );
    writer
        .create(&key, json!({"data":42}).as_object().unwrap().clone())
        .await
        .unwrap();
    assert_eq!(
        reader.get(&key).await.unwrap().unwrap().record.payload["data"],
        json!(42)
    );
    client.close().await.unwrap();
    assert!(reader.get(&key).await.is_err());
    assert!(delivered.authorize().is_err());
    reader.close().await.unwrap();
    writer.close().await.unwrap();
    source.close_and_wait().await;
    provider_messages.close().await.unwrap();
    participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn service_to_consumer_requests_preserve_inbox_routes_and_declared_reply_direction() {
    let h = Harness::new().await;
    let (session, _) = h.advertise("provider", "session", "ad").await;
    let participant = h.participant(session);
    let client = ServiceClient::start(h.spec()).unwrap();
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    let provider = h.provider_connection(&participant, &connection).await;
    let mut incoming = client.watch_messages().unwrap();
    let query = provider.message(body("ask")).unwrap();
    let result =
        tokio::spawn(async move { provider.request(query, Duration::from_secs(3), None).await });
    let request = bounded(incoming.next()).await.unwrap().unwrap();
    let mut response = body("ask");
    response.result = Some(json!({"answer":9}).as_object().unwrap().clone());
    request.reply(response).await.unwrap();
    assert_eq!(
        bounded(result)
            .await
            .unwrap()
            .unwrap()
            .service_body()
            .unwrap()
            .result
            .unwrap()["answer"],
        json!(9)
    );
    client.close().await.unwrap();
    participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn agreement_loss_cancels_a_pending_request_without_replaying_it_after_reconnect() {
    let h = Harness::new().await;
    let (session, _) = h.advertise("provider", "session", "ad").await;
    let participant = h.participant(session);
    let client = ServiceClient::start(h.spec()).unwrap();
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    let query = connection.message(body("read")).unwrap();
    let old = connection
        .context()
        .agreement()
        .contract()
        .contract_id
        .clone();
    let requester = connection.clone();
    let request = tokio::spawn(async move {
        requester
            .request(query, Duration::from_secs(10), None)
            .await
    });
    until(|| {
        h.bus
            .published()
            .iter()
            .any(|message| message.service_body().is_ok_and(|body| body.name == "read"))
    })
    .await;
    connection.context().agreement().request_close();
    assert!(bounded(request).await.unwrap().is_err());
    until(|| {
        client
            .connection()
            .is_ok_and(|connection| connection.context().agreement().contract().contract_id != old)
    })
    .await;
    assert_eq!(
        h.bus
            .published()
            .iter()
            .filter(|message| message.service_body().is_ok_and(|body| body.name == "read"))
            .count(),
        1
    );
    assert_eq!(h.bus.routes(), 1);
    client.close().await.unwrap();
    participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn clients_share_directory_lifetime_and_late_closure_cannot_spawn_owned_work() {
    let h = Harness::new().await;
    let first = ServiceClient::start(h.spec()).unwrap();
    let second = ServiceClient::start(h.spec()).unwrap();
    until(|| h.bus.routes() == 2).await;
    first.close().await.unwrap();
    assert!(h.directory.is_current());
    assert!(h.messages.state().current);
    let (session, _) = h.advertise("provider", "session", "ad").await;
    let participant = h.participant(session);
    bounded(second.wait_ready(None)).await.unwrap();
    assert_eq!(first.state().phase, Phase::Closed);
    let mut messages = second.watch_messages().unwrap();
    let closing = second.clone();
    let close = tokio::spawn(async move { closing.close().await });
    tokio::task::yield_now().await;
    close.abort();
    second.request_close();
    bounded(second.wait_for(|state| state.phase == Phase::Closed, None))
        .await
        .unwrap();
    assert!(messages.next().await.is_none());
    assert!(h.tokens.items("").await.unwrap().is_empty());
    assert!(h.directory.is_current());
    participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn blocked_policy_and_last_owner_drop_release_private_subscriptions() {
    let h = Harness::new().await;
    h.advertise("blocked", "session", "ad").await;
    let entered = Arc::new(AtomicUsize::new(0));
    let marker = entered.clone();
    let mut spec = h.spec();
    spec.policy = Arc::new(move |_: ServiceDescriptor| {
        let marker = marker.clone();
        async move {
            marker.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<Result<Option<ServiceAccess>>>().await
        }
    });
    let client = ServiceClient::start(spec).unwrap();
    let mut states = client.watch_state().unwrap();
    until(|| entered.load(Ordering::SeqCst) == 1).await;
    drop(client);
    bounded(async {
        while states
            .next()
            .await
            .is_some_and(|state| state.phase != Phase::Closed)
        {}
    })
    .await;
    until(|| h.bus.routes() == 0).await;
    assert!(h.contracts.items("").await.unwrap().is_empty());
    assert!(h.directory.is_current());
    h.close().await;
}

#[tokio::test]
async fn directory_last_owner_drop_closes_watches_and_releases_the_shared_feature_registration() {
    let h = Harness::new().await;
    let directory =
        ServiceDirectory::new(h.runtime.beacon().clone(), h.protocol.protocol().clone()).unwrap();
    directory.try_start().unwrap();
    bounded(directory.wait_ready()).await;
    let mut changes = directory.watch_state().unwrap();
    changes.next().await;
    drop(directory);
    assert_eq!(bounded(changes.next()).await.unwrap().phase, Phase::Closed);
    // The live directory uses one source registration, regardless of clients.
    let first = ServiceClient::start(h.spec()).unwrap();
    let second = ServiceClient::start(h.spec()).unwrap();
    let mut registrations = Vec::new();
    until(|| h.bus.routes() == 2).await;
    while let Ok(watch) = h
        .runtime
        .beacon()
        .watch_state(&h.protocol.protocol().feature_id)
    {
        registrations.push(watch);
    }
    assert_eq!(registrations.len(), 255);
    first.close().await.unwrap();
    second.close().await.unwrap();
    drop(registrations);
    h.close().await;
}

#[tokio::test]
async fn query_and_local_policy_control_admission_and_permissions_before_proposals() {
    let h = Harness::new().await;
    for id in ["outside-query", "rejected", "wrong-access", "allowed"] {
        h.advertise(id, "session", id).await;
    }
    let mut spec = h.spec();
    spec.target = ServiceTarget::matching(ServiceQuery {
        service_id: Some("allowed".into()),
        operations: BTreeSet::from(["read".into()]),
        ..Default::default()
    });
    spec.policy = Arc::new(|descriptor: ServiceDescriptor| async move {
        Ok(Some(
            service_access(&descriptor, ["read"], Default::default()).unwrap(),
        ))
    });
    let session = EndpointSession::with_id(
        EndpointAddress::parse("service:allowed").unwrap(),
        "session",
    )
    .unwrap();
    let participant = h.participant(session);
    let client = ServiceClient::start(spec).unwrap();
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    assert_eq!(client.state().value.attempts, 1);
    let mut command = body("write");
    command.params.insert("value".into(), json!(1));
    assert!(connection
        .send(connection.message(command).unwrap())
        .await
        .is_err());
    assert!(h.bus.published().is_empty());
    client.close().await.unwrap();
    let mut spec = h.spec();
    spec.policy = Arc::new(|descriptor: ServiceDescriptor| async move {
        if descriptor.service_id == "wrong-access" {
            let mut access = all_access(&descriptor);
            access.service_session_id = "wrong".into();
            return Ok(Some(access));
        }
        Ok(None)
    });
    spec.options.negotiation_timeout = Duration::from_millis(100);
    let rejected = ServiceClient::start(spec).unwrap();
    assert!(rejected
        .wait_ready(Some(Duration::from_millis(200)))
        .await
        .is_err());
    assert_eq!(rejected.state().value.attempts, 0);
    assert_eq!(h.records().await.len(), 1);
    rejected.close().await.unwrap();
    participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn negotiation_timeouts_clean_owned_tokens_before_fresh_attempts() {
    let h = Harness::new().await;
    h.advertise("pending", "session", "ad").await;
    let mut spec = h.spec();
    spec.options.negotiation_timeout = Duration::from_millis(60);
    let client = ServiceClient::start(spec).unwrap();
    until(|| client.state().value.attempts >= 2).await;
    let records = h.records().await;
    assert!(records
        .iter()
        .any(|record| record.state == ContractState::Cancelled));
    assert!(
        records
            .iter()
            .filter(|record| record.state == ContractState::Open)
            .count()
            <= 1
    );
    assert_eq!(
        records
            .iter()
            .map(|record| &record.contract_id)
            .collect::<BTreeSet<_>>()
            .len(),
        records.len()
    );
    client.close().await.unwrap();
    assert!(h.tokens.items("").await.unwrap().is_empty());
    assert!(h
        .records()
        .await
        .iter()
        .all(|record| record.state == ContractState::Cancelled));
    h.close().await;
}

#[tokio::test]
async fn discovery_closure_preserves_a_healthy_connection_and_endpoint_closure_revokes_it() {
    let h = Harness::new().await;
    let (session, _) = h.advertise("provider", "session", "ad").await;
    let participant = h.participant(session);
    let client = ServiceClient::start(h.spec()).unwrap();
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    h.directory.close_and_wait().await;
    until(|| !client.state().value.discovery_current).await;
    connection.authorize().unwrap();
    assert!(client.state().value.local_authority);
    h.messages.session().close();
    bounded(client.wait_for(|state| state.phase == Phase::Closed, None))
        .await
        .unwrap();
    assert!(connection.authorize().is_err());
    assert!(h.tokens.items("").await.unwrap().is_empty());
    assert!(h.runtime.concord().is_current());
    participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn slow_message_readers_fail_explicitly_without_blocking_client_authority() {
    let h = Harness::new().await;
    let (session, _) = h.advertise("provider", "session", "ad").await;
    let participant = h.participant(session);
    let client = ServiceClient::start(h.spec()).unwrap();
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    let provider = h.provider_connection(&participant, &connection).await;
    let mut watchers = (0..256)
        .map(|_| client.watch_messages().unwrap())
        .collect::<Vec<_>>();
    assert!(client.watch_messages().is_err());
    for index in 0..258 {
        let mut event = body("changed");
        event.event = Some(json!({"value":index}).as_object().unwrap().clone());
        provider
            .send(provider.message(event).unwrap())
            .await
            .unwrap();
        until(|| client.state().value.received_messages == index + 1).await;
    }
    assert!(bounded(watchers[0].next()).await.unwrap().is_err());
    assert!(watchers[0].next().await.is_none());
    connection.authorize().unwrap();
    client.close().await.unwrap();
    for watcher in &mut watchers {
        assert!(watcher.next().await.is_none());
    }
    participant.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn policy_panic_closes_already_started_agreements_and_reports_failure() {
    let h = Harness::new().await;
    h.advertise("pending", "session", "pending-ad").await;
    h.advertise("panics", "session", "panic-ad").await;
    let release = Arc::new(tokio::sync::Notify::new());
    let signal = release.clone();
    let mut spec = h.spec();
    spec.policy = Arc::new(move |descriptor: ServiceDescriptor| {
        let signal = signal.clone();
        async move {
            if descriptor.service_id == "panics" {
                signal.notified().await;
                panic!("injected candidate policy panic");
            }
            Ok(Some(all_access(&descriptor)))
        }
    });
    let client = ServiceClient::start(spec).unwrap();
    until(|| client.state().value.attempts == 1).await;
    bounded(async {
        while h.records().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    release.notify_one();
    bounded(client.wait_for(|state| state.phase == Phase::Closed, None))
        .await
        .unwrap();
    assert!(client.state().failure.is_some());
    assert!(h
        .records()
        .await
        .iter()
        .all(|record| record.state == ContractState::Cancelled));
    assert!(h.tokens.items("").await.unwrap().is_empty());
    until(|| h.bus.routes() == 0).await;
    assert!(h.directory.is_current());
    assert!(h.runtime.concord().is_current());
    h.close().await;
}

#[tokio::test]
async fn queued_messages_recheck_expiry_before_delivery_to_application_code() {
    let h = Harness::new().await;
    let (session, _) = h.advertise("provider", "session", "ad").await;
    let participant = h.participant(session);
    let client = ServiceClient::start(h.spec()).unwrap();
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    let provider = h.provider_connection(&participant, &connection).await;
    let mut messages = client.watch_messages().unwrap();
    let mut event = body("changed");
    event.event = Some(json!({"value":1}).as_object().unwrap().clone());
    let mut expiring = provider.message(event.clone()).unwrap();
    expiring.ttl_ms = Some(200);
    provider.send(expiring).await.unwrap();
    until(|| client.state().value.received_messages == 1).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    event
        .event
        .as_mut()
        .unwrap()
        .insert("value".into(), json!(2));
    provider
        .send(provider.message(event).unwrap())
        .await
        .unwrap();
    assert_eq!(
        bounded(messages.next())
            .await
            .unwrap()
            .unwrap()
            .delivery
            .message
            .service_body()
            .unwrap()
            .event
            .unwrap()["value"],
        json!(2)
    );
    client.close().await.unwrap();
    participant.close().await.unwrap();
    h.close().await;
}
