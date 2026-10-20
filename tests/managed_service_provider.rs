#[path = "support/service_bus.rs"]
mod service_bus;
mod support;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use deckr::concord::{ContractRecord, ContractState, CreateContractSpec};
use deckr::endpoint::EndpointAddress;
use deckr::lanes::DeckrMessage;
use deckr::lifecycle::{Clock, Phase, SystemClock};
use deckr::messaging::{LaneRegistry, MessageSession};
use deckr::runtime::ProtocolRuntime;
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use deckr::session::EndpointSession;
use deckr::state::{
    MaterializedStateStore, MemoryStateStore, StateEntry, StateSource, StateStore, StateWatchStream,
};
use deckr::{Error, Result};
use futures_util::StreamExt;
use serde_json::{json, Value};
use service_bus::Bus;
use tokio::sync::Semaphore;

type Connection = ServiceConnection<MemoryStateStore, MemoryStateStore, Bus>;
type Incoming = ServiceIncoming<MemoryStateStore, MemoryStateStore, Bus>;
type Provider = ServiceProvider<MemoryStateStore, MemoryStateStore, Bus>;
type Client = ServiceClient<MemoryStateStore, MemoryStateStore, Bus>;
type View = ProtectedServiceView<MemoryStateStore, MemoryStateStore, MemoryStateStore>;

struct Backend {
    views: ServiceViewStore<MemoryStateStore>,
    connections: Mutex<Vec<Connection>>,
    writers: Mutex<Vec<View>>,
    messages: Mutex<Vec<DeckrMessage>>,
    block_connected: AtomicBool,
    block_handlers: AtomicBool,
    panic_handler: AtomicBool,
    handler_cancelled: Arc<AtomicUsize>,
    disconnects: AtomicUsize,
    shutdowns: AtomicUsize,
    await_disconnect: AtomicBool,
    disconnected: Semaphore,
    gate: Semaphore,
}
struct Cancelled(Arc<AtomicUsize>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl ServiceBackend<MemoryStateStore, MemoryStateStore, Bus> for Backend {
    fn connected(&self, connection: Connection) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            self.connections.lock().unwrap().push(connection.clone());
            if self.block_connected.load(Ordering::SeqCst) {
                self.gate.acquire().await.unwrap().forget();
            }
            let writer = connection.view(&self.views, "state", None)?;
            writer.wait_ready(None).await?;
            let key = format!(
                "{}current",
                connection.context().descriptor().views["state"].key_prefix
            );
            writer
                .create(&key, json!({"value":7}).as_object().unwrap().clone())
                .await?;
            self.writers.lock().unwrap().push(writer);
            Ok(())
        })
    }
    fn handle(&self, incoming: Incoming) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            self.messages
                .lock()
                .unwrap()
                .push(incoming.delivery.message.clone());
            if self.panic_handler.swap(false, Ordering::SeqCst) {
                panic!("test backend panic");
            }
            if self.block_handlers.load(Ordering::SeqCst) {
                let _cancelled = Cancelled(self.handler_cancelled.clone());
                self.gate.acquire().await.unwrap().forget();
            }
            if incoming.delivery.message.service_body()?.name == "read" {
                let mut response = body("read");
                response.status = Some(ServiceMessageStatus::Ok);
                response.result = Some(json!({"answer":42}).as_object().unwrap().clone());
                incoming.reply(response).await?;
            }
            Ok(())
        })
    }
    fn disconnected(&self, connection: Connection) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            assert!(connection.authorize().is_err());
            self.disconnects.fetch_add(1, Ordering::SeqCst);
            self.disconnected.add_permits(1);
            Ok(())
        })
    }
    fn shutdown(&self) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            if self.await_disconnect.load(Ordering::SeqCst) {
                self.disconnected.acquire().await.unwrap().forget();
            }
            Ok(())
        })
    }
}
struct Harness {
    clock: Arc<dyn Clock>,
    runtime: ProtocolRuntime<MemoryStateStore, MemoryStateStore, MemoryStateStore>,
    ads: MemoryStateStore,
    contracts: MemoryStateStore,
    tokens: MemoryStateStore,
    protocol: ValidatedServiceProtocol,
    directory: ServiceDirectory<MemoryStateStore>,
    bus: Bus,
    messages: MessageSession<Bus>,
    backend: Arc<Backend>,
}
impl Harness {
    async fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock)).await
    }
    async fn with_clock(clock: Arc<dyn Clock>) -> Self {
        let ads = MemoryStateStore::ttl_bound(300).unwrap();
        let contracts = MemoryStateStore::default();
        let tokens = MemoryStateStore::ttl_bound(30).unwrap();
        let runtime = ProtocolRuntime::start(ads.clone(), contracts.clone(), tokens.clone())
            .await
            .unwrap();
        let runtime = ProtocolRuntime::from_views(
            runtime.beacon().clone(),
            runtime.concord().clone().clock(clock.clone()),
        );
        let protocol = ServiceProtocol::from_value(json!({
            "namespace":"org.example.provider","featureId":"org.example.provider","advertisementProfile":"org.example.provider.ad.v1","useProfile":"org.example.provider.use.v1",
            "operations":{"read":{},"write":{}},
            "messages":{
                "read":{"intent":"query","direction":"consumer_to_service","operation":"read","exchangePattern":"request_reply","resultSchema":{"schemaId":"result","schema":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"]}}},
                "write":{"intent":"command","direction":"consumer_to_service","operation":"write","exchangePattern":"one_way","paramsSchema":{"schemaId":"write","schema":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}}},
                "changed":{"intent":"event","direction":"service_to_consumer","exchangePattern":"one_way","eventSchema":{"schemaId":"event","schema":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}}},
                "ask":{"intent":"query","direction":"service_to_consumer","operation":"read","exchangePattern":"request_reply","resultSchema":{"schemaId":"result","schema":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"]}}}
            },"viewFamilies":{"state":{"storeName":"provider_views","writer":"service"}}
        })).unwrap();
        let directory = ServiceDirectory::new(runtime.beacon().clone(), protocol.clone()).unwrap();
        directory.try_start().unwrap();
        bounded(directory.wait_ready()).await;
        let protocol = ValidatedServiceProtocol::new(protocol, &SchemaRegistry::default()).unwrap();
        let bus = Bus::default();
        let messages = MessageSession::with_options(
            bus.clone(),
            session("service:provider", "provider-session"),
            LaneRegistry::default(),
            clock.clone(),
        )
        .unwrap();
        let views = MaterializedStateStore::start(MemoryStateStore::default(), "")
            .await
            .unwrap();
        let backend = Arc::new(Backend {
            views: ServiceViewStore::new("provider_views", views).unwrap(),
            connections: Mutex::default(),
            writers: Mutex::default(),
            messages: Mutex::default(),
            block_connected: AtomicBool::new(false),
            block_handlers: AtomicBool::new(false),
            panic_handler: AtomicBool::new(false),
            handler_cancelled: Arc::default(),
            disconnects: AtomicUsize::new(0),
            shutdowns: AtomicUsize::new(0),
            await_disconnect: AtomicBool::new(false),
            disconnected: Semaphore::new(0),
            gate: Semaphore::new(0),
        });
        Self {
            clock,
            runtime,
            ads,
            contracts,
            tokens,
            protocol,
            directory,
            bus,
            messages,
            backend,
        }
    }
    fn permissions(&self) -> ServicePermissions {
        ServicePermissions {
            operations: ["read".into(), "write".into()].into(),
            views: BTreeMap::from([("state".into(), vec![])]),
        }
    }
    fn spec(
        &self,
    ) -> ServiceProviderSpec<MemoryStateStore, MemoryStateStore, MemoryStateStore, Bus> {
        let permissions = self.permissions();
        ServiceProviderSpec {
            advertisements: self.ads.clone(),
            concord: self.runtime.concord().clone(),
            messages: self.messages.clone(),
            protocol: self.protocol.clone(),
            advertisement: self
                .protocol
                .protocol()
                .advertisement_payload(
                    "provider",
                    "provider-session",
                    ServiceBackendStatus::Available,
                )
                .unwrap(),
            initial_consent: true,
            admission: Arc::new(move |_: ServiceConsumer| {
                let permissions = permissions.clone();
                async move { Ok(Some(permissions)) }
            }),
            backend: self.backend.clone(),
            options: ServiceProviderOptions::default(),
        }
    }
    fn client(&self, id: &str) -> (Client, MessageSession<Bus>) {
        let messages = MessageSession::with_options(
            self.bus.clone(),
            session(&format!("controller:{id}"), id),
            LaneRegistry::default(),
            self.clock.clone(),
        )
        .unwrap();
        let client = ServiceClient::start(ServiceClientSpec {
            directory: self.directory.clone(),
            protocol: self.protocol.clone(),
            concord: self.runtime.concord().clone(),
            messages: messages.clone(),
            target: ServiceTarget::matching(ServiceQuery::default()),
            policy: Arc::new(|descriptor: ServiceDescriptor| async move {
                Ok(Some(service_access(
                    &descriptor,
                    descriptor.supported_operations.clone(),
                    descriptor
                        .views
                        .keys()
                        .map(|name| (name.clone(), vec![]))
                        .collect(),
                )?))
            }),
            options: ServiceClientOptions {
                negotiation_timeout: Duration::from_secs(10),
                retry_delay: Duration::from_millis(10),
            },
        })
        .unwrap();
        (client, messages)
    }
    async fn pair(&self) -> (Provider, Client, Connection, MessageSession<Bus>) {
        let provider = ServiceProvider::start(self.spec()).unwrap();
        let (client, messages) = self.client("client");
        let connection = bounded(client.wait_ready(None)).await.unwrap();
        until(|| provider.connections().len() == 1).await;
        (provider, client, connection, messages)
    }
    async fn close(self) {
        self.messages.close().await.unwrap();
        let writers = self
            .backend
            .writers
            .lock()
            .unwrap()
            .drain(..)
            .collect::<Vec<_>>();
        for writer in writers {
            writer.close().await.unwrap();
        }
        self.backend.views.source().close_and_wait().await;
        self.directory.close_and_wait().await;
        self.runtime.close().await;
    }
}
fn session(endpoint: &str, id: &str) -> EndpointSession {
    EndpointSession::with_id(EndpointAddress::parse(endpoint).unwrap(), id).unwrap()
}
fn body(name: &str) -> ServiceMessageBody {
    ServiceMessageBody::from_value(json!({"serviceNamespace":"org.example.provider","name":name,"intent":if name=="changed" {"event"}else if name=="write" {"command"}else{"query"},"exchangePattern":if name=="read"||name=="ask" {"request_reply"}else{"one_way"}})).unwrap()
}
fn command(connection: &Connection, value: i64) -> DeckrMessage {
    let mut body = body("write");
    body.params.insert("value".into(), json!(value));
    connection.message(body).unwrap()
}
async fn bounded<R>(work: impl std::future::Future<Output = R>) -> R {
    tokio::time::timeout(Duration::from_secs(10), work)
        .await
        .expect("operation completes")
}
async fn until(mut predicate: impl FnMut() -> bool) {
    bounded(async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test]
async fn managed_boundaries_dispatch_both_directions_and_publish_protected_views() {
    let h = Harness::new().await;
    let (provider, client, connection, messages) = h.pair().await;
    let mut request = connection.message(body("read")).unwrap();
    request
        .subject
        .identifiers
        .insert("resourceId".into(), "lamp".into());
    let subject = request.subject.clone();
    request.trace = Some(json!({"traceParent":"test-trace"}));
    let id = request.message_id.clone();
    let pointer = request.contract.clone();
    let response = bounded(connection.request(request, Duration::from_secs(3), None))
        .await
        .unwrap();
    assert_eq!(response.in_reply_to.as_ref(), Some(&id));
    assert_eq!(response.subject, subject);
    assert_eq!(response.contract, pointer);
    assert_eq!(response.trace, Some(json!({"traceParent":"test-trace"})));
    assert_eq!(
        response.service_body().unwrap().result.unwrap()["answer"],
        json!(42)
    );
    let mut events = client.watch_messages().unwrap();
    let server = provider.connections().pop().unwrap();
    let mut event = body("changed");
    event.event = Some(json!({"value":8}).as_object().unwrap().clone());
    server.send(server.message(event).unwrap()).await.unwrap();
    assert_eq!(
        bounded(events.next())
            .await
            .unwrap()
            .unwrap()
            .delivery
            .message
            .service_body()
            .unwrap()
            .event
            .unwrap()["value"],
        json!(8)
    );
    let requester = server.clone();
    let result = tokio::spawn(async move {
        requester
            .request(
                requester.message(body("ask")).unwrap(),
                Duration::from_secs(3),
                None,
            )
            .await
    });
    let incoming = bounded(events.next()).await.unwrap().unwrap();
    let mut reply = body("ask");
    reply.status = Some(ServiceMessageStatus::Ok);
    reply.result = Some(json!({"answer":9}).as_object().unwrap().clone());
    incoming.reply(reply).await.unwrap();
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
    let reader = connection.view(&h.backend.views, "state", None).unwrap();
    bounded(reader.wait_ready(None)).await.unwrap();
    let key = format!(
        "{}current",
        connection.context().descriptor().views["state"].key_prefix
    );
    assert_eq!(
        reader.get(&key).await.unwrap().unwrap().record.payload["value"],
        json!(7)
    );
    provider.close().await.unwrap();
    assert!(server.authorize().is_err());
    assert!(reader.get(&key).await.is_err());
    assert_eq!(h.backend.disconnects.load(Ordering::SeqCst), 1);
    assert_eq!(h.backend.shutdowns.load(Ordering::SeqCst), 1);
    client.close().await.unwrap();
    messages.close().await.unwrap();
    reader.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn known_consent_withdrawal_revokes_contexts_views_and_blocked_handlers_synchronously() {
    let h = Harness::new().await;
    let (provider, client, connection, messages) = h.pair().await;
    let server = provider.connections().pop().unwrap();
    let writer = h.backend.writers.lock().unwrap()[0].clone();
    h.backend.block_handlers.store(true, Ordering::SeqCst);
    connection.send(command(&connection, 1)).await.unwrap();
    until(|| !h.backend.messages.lock().unwrap().is_empty()).await;
    let mut state = provider.backend_state();
    state.consent = false;
    provider.set_backend(state.clone()).unwrap();
    assert!(server.authorize().is_err());
    let key = format!(
        "{}current",
        server.context().descriptor().views["state"].key_prefix
    );
    assert!(writer.get(&key).await.is_err());
    until(|| h.backend.handler_cancelled.load(Ordering::SeqCst) == 1).await;
    until(|| h.backend.disconnects.load(Ordering::SeqCst) == 1).await;
    h.backend.block_handlers.store(false, Ordering::SeqCst);
    state.consent = true;
    provider.set_backend(state).unwrap();
    until(|| {
        client.connection().is_ok_and(|fresh| {
            fresh.context().agreement().contract().contract_id
                != connection.context().agreement().contract().contract_id
        })
    })
    .await;
    assert!(connection.authorize().is_err());
    assert!(server.authorize().is_err());
    assert_eq!(h.backend.messages.lock().unwrap().len(), 1);
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn selective_consent_preserves_other_consumers_and_stale_admission_cannot_attach() {
    let h = Harness::new().await;
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(AtomicUsize::new(0));
    let mut spec = h.spec();
    let permissions = h.permissions();
    let blocker = gate.clone();
    let marker = entered.clone();
    spec.admission = Arc::new(move |consumer: ServiceConsumer| {
        let blocker = blocker.clone();
        let marker = marker.clone();
        let permissions = permissions.clone();
        async move {
            if consumer.session_id == "delayed" {
                marker.fetch_add(1, Ordering::SeqCst);
                blocker.acquire().await.unwrap().forget();
            }
            Ok(Some(permissions))
        }
    });
    let provider = ServiceProvider::start(spec).unwrap();
    let (first, first_messages) = h.client("first");
    let first_connection = bounded(first.wait_ready(None)).await.unwrap();
    let (delayed, delayed_messages) = h.client("delayed");
    until(|| entered.load(Ordering::SeqCst) > 0).await;
    provider
        .set_consumer_consent(
            ServiceConsumer {
                endpoint: delayed_messages.session().endpoint().clone(),
                session_id: "delayed".into(),
            },
            false,
        )
        .unwrap();
    gate.add_permits(10);
    // A later consumer can become ready only after the blocked, obsolete
    // selection has completed and the participant has run a new policy pass.
    let (later, later_messages) = h.client("later");
    let later_connection = bounded(later.wait_ready(None)).await.unwrap();
    until(|| provider.connections().len() == 2).await;
    first_connection
        .send(command(&first_connection, 3))
        .await
        .unwrap();
    until(|| !h.backend.messages.lock().unwrap().is_empty()).await;
    assert!(first_connection.authorize().is_ok());
    assert!(delayed.connection().is_err());
    assert!(h
        .backend
        .connections
        .lock()
        .unwrap()
        .iter()
        .all(|connection| connection.context().consumer_session_id() != "delayed"));
    let first_server = provider
        .connections()
        .into_iter()
        .find(|connection| connection.context().consumer_session_id() == "first")
        .unwrap();
    provider
        .set_consumer_consent(
            ServiceConsumer {
                endpoint: first_messages.session().endpoint().clone(),
                session_id: "first".into(),
            },
            false,
        )
        .unwrap();
    assert!(first_server.authorize().is_err());
    assert!(later_connection.authorize().is_ok());
    later_connection
        .send(command(&later_connection, 4))
        .await
        .unwrap();
    until(|| h.backend.messages.lock().unwrap().len() == 2).await;
    first.close().await.unwrap();
    delayed.close().await.unwrap();
    later.close().await.unwrap();
    provider.close().await.unwrap();
    first_messages.close().await.unwrap();
    delayed_messages.close().await.unwrap();
    later_messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn backend_metadata_and_discovery_loss_do_not_withdraw_consent() {
    let h = Harness::new().await;
    let (provider, client, connection, messages) = h.pair().await;
    let mut state = provider.backend_state();
    state.status = ServiceBackendStatus::Unavailable;
    state.diagnostics.insert("reason".into(), json!("warming"));
    provider.set_backend(state).unwrap();
    until(|| {
        h.directory
            .descriptors()
            .iter()
            .any(|descriptor| descriptor.backend_status == ServiceBackendStatus::Unavailable)
    })
    .await;
    let ad = h.ads.items("").await.unwrap().pop().unwrap();
    h.ads.delete(&ad.key, Some(ad.revision)).await.unwrap();
    until(|| h.directory.descriptors().is_empty()).await;
    connection.send(command(&connection, 4)).await.unwrap();
    until(|| h.backend.messages.lock().unwrap().len() == 1).await;
    assert!(connection.authorize().is_ok());
    assert!(provider.connections()[0].authorize().is_ok());
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn invalid_payloads_namespaces_operations_and_senders_never_reach_backend() {
    let h = Harness::new().await;
    let (provider, client, connection, messages) = h.pair().await;
    let mut invalid = command(&connection, 1);
    invalid.body["params"]["value"] = json!("invalid");
    messages.send(invalid).await.unwrap();
    let mut invalid = command(&connection, 1);
    invalid.body["serviceNamespace"] = json!("org.example.wrong");
    messages.send(invalid).await.unwrap();
    let mut invalid = command(&connection, 1);
    invalid.body["name"] = json!("undeclared");
    messages.send(invalid).await.unwrap();
    let intruder =
        MessageSession::new(h.bus.clone(), session("controller:intruder", "intruder")).unwrap();
    let mut invalid = command(&connection, 1);
    invalid.sender = intruder.session().endpoint().to_string();
    invalid.sender_session_id = "intruder".into();
    intruder.send(invalid).await.unwrap();
    until(|| provider.state().value.rejected_messages == 4).await;
    assert!(h.backend.messages.lock().unwrap().is_empty());
    connection.send(command(&connection, 2)).await.unwrap();
    until(|| provider.state().value.handled_messages == 1).await;
    assert_eq!(h.backend.messages.lock().unwrap().len(), 1);
    assert!(connection.authorize().is_ok());
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    intruder.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn null_terms_revoke_authority_while_domain_reassessment_is_blocked() {
    let h = Harness::new().await;
    let block = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicUsize::new(0));
    let mut spec = h.spec();
    let marker = entered.clone();
    let blocked = block.clone();
    let permissions = h.permissions();
    spec.admission = Arc::new(move |_: ServiceConsumer| {
        let marker = marker.clone();
        let blocked = blocked.clone();
        let permissions = permissions.clone();
        async move {
            if blocked.load(Ordering::SeqCst) {
                marker.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            Ok(Some(permissions))
        }
    });
    let provider = ServiceProvider::start(spec).unwrap();
    let (client, messages) = h.client("client");
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    until(|| provider.connections().len() == 1).await;
    let server = provider.connections().pop().unwrap();
    block.store(true, Ordering::SeqCst);
    provider.reconsider().unwrap();
    until(|| entered.load(Ordering::SeqCst) > 0).await;
    let key = &connection.context().agreement().contract().key;
    let mut entry = h.contracts.get(key).await.unwrap().unwrap();
    entry.value["terms"] = json!(null);
    h.contracts
        .update(key, entry.value, entry.revision, None)
        .await
        .unwrap();
    until(|| server.authorize().is_err()).await;
    until(|| h.backend.disconnects.load(Ordering::SeqCst) == 1).await;
    assert!(h.backend.messages.lock().unwrap().is_empty());
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn provider_capacity_counts_grants_and_does_not_reserve_rejected_candidates() {
    let h = Harness::new().await;
    let mut spec = h.spec();
    spec.options.max_connections = 1;
    let permissions = h.permissions();
    spec.admission = Arc::new(move |consumer: ServiceConsumer| {
        let permissions = permissions.clone();
        async move { Ok((consumer.session_id != "denied").then_some(permissions)) }
    });
    let provider = ServiceProvider::start(spec).unwrap();
    bounded(provider.wait_ready(None)).await.unwrap();
    // Stable contract IDs put the rejected proposal before the eligible one.
    let rejected = session("controller:denied", "denied");
    let allowed = session("controller:allowed", "allowed");
    let propose = |consumer: EndpointSession, id: &'static str| {
        let h = &h;
        async move {
            let mut intent = CreateContractSpec::new(vec![
                consumer.endpoint().clone(),
                h.messages.session().endpoint().clone(),
            ]);
            intent.contract_id = Some(id.into());
            intent.profile = Some(h.protocol.protocol().use_profile.clone());
            let contract = h.runtime.concord().create_contract(intent).await.unwrap();
            let token = h
                .runtime
                .concord()
                .attach(&contract, consumer.endpoint(), consumer.id(), None)
                .await
                .unwrap();
            (contract, token)
        }
    };
    let rejected = propose(rejected, "aaa").await;
    let allowed = propose(allowed, "zzz").await;
    until(|| provider.connections().len() == 1).await;
    assert_eq!(
        provider.connections()[0].context().consumer_session_id(),
        "allowed"
    );
    h.runtime.concord().withdraw(&allowed.1).await.unwrap();
    h.runtime.concord().withdraw(&rejected.1).await.unwrap();
    provider.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn handler_panics_are_observable_without_revoking_healthy_agreements() {
    let h = Harness::new().await;
    let (provider, client, connection, messages) = h.pair().await;
    h.backend.panic_handler.store(true, Ordering::SeqCst);
    connection.send(command(&connection, 1)).await.unwrap();
    until(|| provider.state().value.backend_failures == 1).await;
    assert!(connection.authorize().is_ok());
    connection.send(command(&connection, 2)).await.unwrap();
    until(|| provider.state().value.handled_messages == 1).await;
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn close_runs_disconnect_and_shutdown_concurrently_and_keeps_borrowed_sources_open() {
    let h = Harness::new().await;
    let (provider, client, connection, messages) = h.pair().await;
    h.backend.await_disconnect.store(true, Ordering::SeqCst);
    let closer = provider.clone();
    let close = tokio::spawn(async move { closer.close().await });
    until(|| !provider.state().current).await;
    close.abort();
    bounded(provider.close()).await.unwrap();
    assert_eq!(provider.state().phase, Phase::Closed);
    assert!(connection.authorize().is_err());
    assert_eq!(h.backend.disconnects.load(Ordering::SeqCst), 1);
    assert_eq!(h.backend.shutdowns.load(Ordering::SeqCst), 1);
    assert!(h.runtime.concord().is_current());
    assert!(h.runtime.beacon().materialized_store().is_current());
    assert!(h.backend.views.source().is_current());
    assert!(!h.messages.session().is_closed());
    let mut probe = h.messages.subscribe("services").await.unwrap();
    probe.close().await.unwrap();
    client.close().await.unwrap();
    messages.close().await.unwrap();
    assert!(h.tokens.items("").await.unwrap().is_empty());
    assert!(h.ads.items("").await.unwrap().is_empty());
    h.close().await;
}

#[tokio::test]
async fn live_competing_provider_waits_until_ownership_clears_before_publication() {
    let h = Harness::new().await;
    let (provider, client, connection, messages) = h.pair().await;
    let other_messages =
        MessageSession::new(h.bus.clone(), session("service:provider", "competitor")).unwrap();
    let mut spec = h.spec();
    spec.messages = other_messages.clone();
    spec.advertisement.session_id = "competitor".into();
    let competitor = ServiceProvider::start(spec).unwrap();
    bounded(competitor.wait_for(|state| state.phase == Phase::Unavailable, None))
        .await
        .unwrap();
    // More than one retry must preserve the live owner's authority.
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(competitor.state().phase, Phase::Unavailable);
    assert!(!competitor.state().value.bootstrapped);
    assert!(competitor.state().failure.is_some());
    assert_eq!(h.ads.items("").await.unwrap().len(), 1);
    assert!(connection.authorize().is_ok());
    assert_eq!(h.backend.connections.lock().unwrap().len(), 1);
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    bounded(competitor.wait_ready(None)).await.unwrap();
    assert!(competitor.state().value.bootstrapped);
    assert!(competitor.state().failure.is_none());
    let (successor, successor_messages) = h.client("successor");
    let next = bounded(successor.wait_ready(None)).await.unwrap();
    assert_eq!(next.context().descriptor().session_id, "competitor");
    assert_ne!(
        next.context().agreement().contract().contract_id,
        connection.context().agreement().contract().contract_id
    );
    assert!(connection.authorize().is_err());
    successor.close().await.unwrap();
    successor_messages.close().await.unwrap();
    competitor.close().await.unwrap();
    other_messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn waiting_provider_close_stops_bootstrap_without_displacing_live_owner() {
    let h = Harness::new().await;
    let (provider, client, connection, messages) = h.pair().await;
    let other_messages =
        MessageSession::new(h.bus.clone(), session("service:provider", "competitor")).unwrap();
    let mut spec = h.spec();
    spec.messages = other_messages.clone();
    spec.advertisement.session_id = "competitor".into();
    let competitor = ServiceProvider::start(spec).unwrap();
    bounded(competitor.wait_for(|state| state.phase == Phase::Unavailable, None))
        .await
        .unwrap();
    bounded(competitor.close()).await.unwrap();
    assert_eq!(competitor.state().phase, Phase::Closed);
    assert!(!competitor.state().value.bootstrapped);
    assert!(connection.authorize().is_ok());
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(h.ads.items("").await.unwrap().is_empty());
    other_messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn endpoint_close_and_last_owner_drop_release_owned_work() {
    for close_endpoint in [false, true] {
        let h = Harness::new().await;
        let (provider, client, connection, messages) = h.pair().await;
        let mut states = provider.watch_state().unwrap();
        if close_endpoint {
            h.messages.session().close();
        }
        drop(provider);
        bounded(async {
            while states
                .next()
                .await
                .is_some_and(|state| state.phase != Phase::Closed)
            {}
        })
        .await;
        assert!(connection.authorize().is_err());
        assert_eq!(h.backend.shutdowns.load(Ordering::SeqCst), 1);
        client.close().await.unwrap();
        messages.close().await.unwrap();
        assert!(h.tokens.items("").await.unwrap().is_empty());
        assert!(h.ads.items("").await.unwrap().is_empty());
        let records = h.contracts.items("").await.unwrap();
        assert!(records
            .iter()
            .all(|record| ContractRecord::from_value(record.value.clone())
                .unwrap()
                .state
                == ContractState::Cancelled));
        h.close().await;
    }
}

#[tokio::test]
async fn permission_changes_require_fresh_agreements_and_assessment_errors_preserve_existing_consent(
) {
    let h = Harness::new().await;
    let mode = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut spec = h.spec();
    let policy_mode = mode.clone();
    let marker = calls.clone();
    let permissions = h.permissions();
    spec.admission = Arc::new(move |_: ServiceConsumer| {
        let mode = policy_mode.clone();
        let marker = marker.clone();
        let mut permissions = permissions.clone();
        async move {
            marker.fetch_add(1, Ordering::SeqCst);
            match mode.load(Ordering::SeqCst) {
                1 => {
                    return Err(Error::StateUnavailable(
                        "assessment temporarily unavailable".into(),
                    ))
                }
                2 => {
                    permissions.operations.remove("write");
                }
                _ => {}
            }
            Ok(Some(permissions))
        }
    });
    let provider = ServiceProvider::start(spec).unwrap();
    let (client, messages) = h.client("client");
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    until(|| provider.connections().len() == 1).await;
    let server = provider.connections().pop().unwrap();
    mode.store(1, Ordering::SeqCst);
    provider.reconsider().unwrap();
    until(|| provider.state().value.backend_failures > 0).await;
    assert!(server.authorize().is_ok());
    assert!(connection.authorize().is_ok());
    mode.store(2, Ordering::SeqCst);
    provider.reconsider().unwrap();
    until(|| server.authorize().is_err()).await;
    until(|| {
        client.connection().is_ok_and(|fresh| {
            fresh.context().agreement().contract().contract_id
                != connection.context().agreement().contract().contract_id
        })
    })
    .await;
    until(|| provider.connections().len() == 1).await;
    let fresh = client.connection().unwrap();
    // Consumer-local permissions cannot override the provider's narrower scope.
    fresh.send(command(&fresh, 4)).await.unwrap();
    until(|| provider.state().value.rejected_messages > 0).await;
    assert!(h.backend.messages.lock().unwrap().is_empty());
    assert_eq!(
        bounded(fresh.request(
            fresh.message(body("read")).unwrap(),
            Duration::from_secs(2),
            None
        ))
        .await
        .unwrap()
        .service_body()
        .unwrap()
        .result
        .unwrap()["answer"],
        json!(42)
    );
    assert!(calls.load(Ordering::SeqCst) >= 3);
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn full_queue_and_expired_queued_messages_never_reach_a_blocked_backend() {
    let clock = support::ManualClock::default();
    let h = Harness::with_clock(Arc::new(clock.clone())).await;
    let mut spec = h.spec();
    spec.options.max_in_flight = 1;
    spec.options.max_pending_messages = 1;
    let provider = ServiceProvider::start(spec).unwrap();
    let (client, messages) = h.client("client");
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    until(|| provider.connections().len() == 1).await;
    h.backend.block_handlers.store(true, Ordering::SeqCst);
    connection.send(command(&connection, 1)).await.unwrap();
    until(|| h.backend.messages.lock().unwrap().len() == 1).await;
    let mut expires = command(&connection, 2);
    expires.ttl_ms = Some(60);
    connection.send(expires).await.unwrap();
    connection.send(command(&connection, 3)).await.unwrap();
    until(|| provider.state().value.rejected_messages == 1).await;
    clock.advance(Duration::from_millis(80));
    h.backend.block_handlers.store(false, Ordering::SeqCst);
    h.backend.gate.add_permits(1);
    until(|| provider.state().value.rejected_messages == 2).await;
    assert_eq!(h.backend.messages.lock().unwrap().len(), 1);
    assert!(connection.authorize().is_ok());
    connection.send(command(&connection, 4)).await.unwrap();
    until(|| provider.state().value.handled_messages == 2).await;
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn connection_callback_failure_does_not_revoke_agreement_or_block_future_dispatch() {
    let clock = support::ManualClock::default();
    let h = Harness::with_clock(Arc::new(clock.clone())).await;
    h.backend.block_connected.store(true, Ordering::SeqCst);
    let mut spec = h.spec();
    spec.options.callback_timeout = Duration::from_millis(30);
    let provider = ServiceProvider::start(spec).unwrap();
    let (client, messages) = h.client("client");
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    until(|| !h.backend.connections.lock().unwrap().is_empty()).await;
    clock.advance(Duration::from_millis(31));
    until(|| provider.state().value.backend_failures == 1).await;
    until(|| provider.connections().len() == 1).await;
    assert!(connection.authorize().is_ok());
    connection.send(command(&connection, 5)).await.unwrap();
    until(|| provider.state().value.handled_messages == 1).await;
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn unavailable_view_source_does_not_remove_independent_message_authority() {
    let h = Harness::new().await;
    h.backend.views.source().close_and_wait().await;
    let provider = ServiceProvider::start(h.spec()).unwrap();
    let (client, messages) = h.client("client");
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    until(|| provider.state().value.backend_failures == 1).await;
    assert!(connection.authorize().is_ok());
    assert!(h.backend.writers.lock().unwrap().is_empty());
    let reply = bounded(connection.request(
        connection.message(body("read")).unwrap(),
        Duration::from_secs(2),
        None,
    ))
    .await
    .unwrap();
    assert_eq!(
        reply.service_body().unwrap().result.unwrap()["answer"],
        json!(42)
    );
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[derive(Clone)]
struct PublicationStore {
    raw: MemoryStateStore,
    block_updates: Arc<AtomicBool>,
    entered: Arc<AtomicUsize>,
    fail_delete: Arc<AtomicBool>,
    block_create_reply: Arc<AtomicBool>,
}
impl PublicationStore {
    fn new(raw: MemoryStateStore) -> Self {
        Self {
            raw,
            block_updates: Arc::default(),
            entered: Arc::default(),
            fail_delete: Arc::default(),
            block_create_reply: Arc::default(),
        }
    }
}
impl StateStore for PublicationStore {
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        self.raw.ttl_seconds().await
    }
    async fn get(&self, key: &str) -> Result<Option<StateEntry>> {
        self.raw.get(key).await
    }
    async fn items(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        self.raw.items(prefix).await
    }
    async fn put(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        self.raw.put(key, value, ttl).await
    }
    async fn create(&self, key: &str, value: Value, ttl: Option<u64>) -> Result<StateEntry> {
        let entry = self.raw.create(key, value, ttl).await?;
        if self.block_create_reply.load(Ordering::SeqCst) {
            self.entered.fetch_add(1, Ordering::SeqCst);
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
        if self.block_updates.load(Ordering::SeqCst) {
            self.entered.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        self.raw.update(key, value, revision, ttl).await
    }
    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(Error::StateUnavailable(
                "injected publication cleanup failure".into(),
            ));
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
fn publication_spec(
    h: &Harness,
    store: PublicationStore,
) -> ServiceProviderSpec<PublicationStore, MemoryStateStore, MemoryStateStore, Bus> {
    let spec = h.spec();
    ServiceProviderSpec {
        advertisements: store,
        concord: spec.concord,
        messages: spec.messages,
        protocol: spec.protocol,
        advertisement: spec.advertisement,
        initial_consent: spec.initial_consent,
        admission: spec.admission,
        backend: spec.backend,
        options: spec.options,
    }
}

#[tokio::test]
async fn ttl_renewal_continues_during_blocked_backend_and_publication_callbacks() {
    let clock = support::ManualClock::default();
    let h = Harness::with_clock(Arc::new(clock.clone())).await;
    let ads = PublicationStore::new(h.ads.clone());
    let mut spec = publication_spec(&h, ads.clone());
    spec.options.callback_timeout = Duration::from_secs(60);
    let provider = ServiceProvider::start(spec).unwrap();
    let (client, messages) = h.client("client");
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    until(|| provider.connections().len() == 1).await;
    let revisions = h
        .tokens
        .items("")
        .await
        .unwrap()
        .into_iter()
        .map(|entry| (entry.key, entry.revision))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(revisions.len(), 2);
    h.backend.block_handlers.store(true, Ordering::SeqCst);
    connection.send(command(&connection, 1)).await.unwrap();
    until(|| h.backend.messages.lock().unwrap().len() == 1).await;
    ads.block_updates.store(true, Ordering::SeqCst);
    let mut state = provider.backend_state();
    state.diagnostics.insert("revision".into(), json!(2));
    provider.set_backend(state).unwrap();
    until(|| ads.entered.load(Ordering::SeqCst) > 0).await;
    clock.advance(Duration::from_secs(20));
    bounded(async {
        loop {
            let tokens = h.tokens.items("").await.unwrap();
            if tokens.len() == 2
                && tokens
                    .iter()
                    .all(|entry| entry.revision > revisions[&entry.key])
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(connection.authorize().is_ok());
    assert!(provider.connections()[0].authorize().is_ok());
    assert_eq!(h.backend.handler_cancelled.load(Ordering::SeqCst), 0);
    client.close().await.unwrap();
    provider.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn stalled_publication_does_not_hold_consent_or_dispatch_and_unknown_create_is_cleaned_up() {
    for stall_create in [false, true] {
        let h = Harness::new().await;
        let ads = PublicationStore::new(h.ads.clone());
        ads.block_create_reply.store(stall_create, Ordering::SeqCst);
        let provider = ServiceProvider::start(publication_spec(&h, ads.clone())).unwrap();
        let (client, messages) = h.client("client");
        let connection = bounded(client.wait_ready(None)).await.unwrap();
        until(|| provider.connections().len() == 1).await;
        if !stall_create {
            ads.block_updates.store(true, Ordering::SeqCst);
            let mut state = provider.backend_state();
            state.diagnostics.insert("updated".into(), json!(true));
            provider.set_backend(state).unwrap();
        }
        until(|| ads.entered.load(Ordering::SeqCst) > 0).await;
        let response = bounded(connection.request(
            connection.message(body("read")).unwrap(),
            Duration::from_secs(2),
            None,
        ))
        .await
        .unwrap();
        assert_eq!(
            response.service_body().unwrap().result.unwrap()["answer"],
            json!(42)
        );
        let server = provider.connections().pop().unwrap();
        let mut state = provider.backend_state();
        state.consent = false;
        provider.set_backend(state).unwrap();
        assert!(server.authorize().is_err());
        bounded(provider.close()).await.unwrap();
        client.close().await.unwrap();
        messages.close().await.unwrap();
        assert!(h.ads.items("").await.unwrap().is_empty());
        assert!(h.tokens.items("").await.unwrap().is_empty());
        h.close().await;
    }
}

#[tokio::test]
async fn failed_remote_cleanup_is_reported_after_local_authority_and_workers_end() {
    let h = Harness::new().await;
    let ads = PublicationStore::new(h.ads.clone());
    let provider = ServiceProvider::start(publication_spec(&h, ads.clone())).unwrap();
    let (client, messages) = h.client("client");
    let connection = bounded(client.wait_ready(None)).await.unwrap();
    until(|| provider.connections().len() == 1).await;
    let server = provider.connections().pop().unwrap();
    ads.fail_delete.store(true, Ordering::SeqCst);
    assert!(bounded(provider.close()).await.is_err());
    assert_eq!(provider.state().phase, Phase::Closed);
    assert!(server.authorize().is_err());
    assert!(connection.authorize().is_err());
    assert_eq!(h.backend.disconnects.load(Ordering::SeqCst), 1);
    assert_eq!(h.backend.shutdowns.load(Ordering::SeqCst), 1);
    assert!(h.tokens.items("").await.unwrap().is_empty());
    client.close().await.unwrap();
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn immediate_close_prevents_startup_and_public_watch_limits_do_not_block_cleanup() {
    let h = Harness::new().await;
    let provider = ServiceProvider::start(h.spec()).unwrap();
    let mut watches = Vec::new();
    while let Ok(watch) = provider.watch_state() {
        watches.push(watch);
        assert!(watches.len() <= 256);
    }
    provider.request_close();
    bounded(provider.close()).await.unwrap();
    assert!(!provider.state().value.bootstrapped);
    assert_eq!(h.bus.routes(), 0);
    assert!(h.ads.items("").await.unwrap().is_empty());
    assert!(h.backend.connections.lock().unwrap().is_empty());
    assert_eq!(h.backend.shutdowns.load(Ordering::SeqCst), 1);
    h.close().await;
}
