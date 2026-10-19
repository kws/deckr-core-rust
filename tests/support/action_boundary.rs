//! Managed boundaries use the real upstream declaration and schema resources.

#[path = "service_bus.rs"]
mod service_bus;
use deckr::action_runtime::*;
use deckr::endpoint::EndpointAddress;
use deckr::lanes::DeckrMessage;
use deckr::messaging::MessageSession;
use deckr::runtime::ProtocolRuntime;
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use deckr::session::EndpointSession;
use deckr::state::MemoryStateStore;
use deckr::{Error, Result};
use service_bus::Bus;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

type Connection = ServiceConnection<MemoryStateStore, MemoryStateStore, Bus>;
type Action = ActionConnection<MemoryStateStore, MemoryStateStore, Bus>;
type Incoming = ActionIncoming<MemoryStateStore, MemoryStateStore, Bus>;

fn protocol() -> ServiceProtocol {
    ServiceProtocol::from_text(&super::artifact_text(
        "catalogs/action-runtime-protocol.v1.json",
    ))
    .unwrap()
}
fn declaration() -> ValidatedServiceProtocol {
    let mut registry = SchemaRegistry::default();
    let mut pending = vec![super::artifact_path("schemas")];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                let schema: serde_json::Value =
                    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
                if let Some(id) = schema.get("$id").and_then(serde_json::Value::as_str) {
                    registry = registry.with_resource(id.to_string(), schema).unwrap();
                }
            }
        }
    }
    ValidatedServiceProtocol::new(protocol(), &registry).unwrap()
}
fn message(name: &str) -> ActionMessage {
    let wire = DeckrMessage::from_text(&super::artifact_text(&format!(
        "fixtures/valid/coverage/deckr/action-runtime/messages/{name}.v1.json"
    )))
    .unwrap();
    ActionMessage::from_body(&wire.service_body().unwrap()).unwrap()
}
fn binding() -> BindingMetadata {
    let ActionMessage::BindingOverlay(body) = message("binding-overlay") else {
        unreachable!()
    };
    body.binding
}
struct References {
    scope: Mutex<Option<ActionScope>>,
    binding: Mutex<BindingMetadata>,
    calls: AtomicUsize,
    panic: AtomicBool,
    page_ready: AtomicBool,
}
impl References {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            scope: Mutex::new(None),
            binding: Mutex::new(binding()),
            calls: AtomicUsize::new(0),
            panic: AtomicBool::new(false),
            page_ready: AtomicBool::new(false),
        })
    }
    fn bind(&self, action: &Action) {
        *self.scope.lock().unwrap() = Some(action.scope().clone());
    }
}
impl ActionReferenceAuthority for References {
    fn authorize(
        &self,
        scope: &ActionScope,
        _envelope: &DeckrMessage,
        action: &ActionMessage,
    ) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.panic.swap(false, Ordering::SeqCst) {
            panic!("reference check panic");
        }
        if self.scope.lock().unwrap().as_ref() != Some(scope) {
            return Err(Error::Unauthorized("owner scope differs".into()));
        }
        let current = self.binding.lock().unwrap();
        match action {
            ActionMessage::BindingOverlay(body) => validate_binding_overlay(body, &current),
            ActionMessage::BindingOverlayClear(body) => {
                validate_binding_overlay_clear(body, &current)
            }
            ActionMessage::ActionInstanceCreated(body)
            | ActionMessage::ActionInstanceDestroyed(body) => {
                validate_action_reference(&body.metadata, &current.action_instance())
            }
            ActionMessage::OpenPage(body) | ActionMessage::ReplacePage(body)
                if self.page_ready.load(Ordering::SeqCst) =>
            {
                let opener = current.action_instance();
                let resolved = body
                    .descriptor
                    .bindings
                    .iter()
                    .map(|child| ResolvedPageChild {
                        control_id: child.control_id.clone(),
                        action: opener.clone(),
                        provider_labels: BTreeMap::new(),
                    })
                    .collect::<Vec<_>>();
                validate_page_children(&body.descriptor, &opener, &resolved)
            }
            ActionMessage::ClosePage if self.page_ready.load(Ordering::SeqCst) => Ok(()),
            _ => Err(Error::Unauthorized(
                "owner has no current resolution for this message".into(),
            )),
        }
    }
}
struct Backend {
    references: Arc<References>,
    connection: Mutex<Option<Action>>,
    received: Mutex<Vec<Incoming>>,
}
impl ServiceBackend<MemoryStateStore, MemoryStateStore, Bus> for Backend {
    fn connected(&self, connection: Connection) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let action = ActionConnection::new(connection, "example", self.references.clone())?;
            self.references.bind(&action);
            *self.connection.lock().unwrap() = Some(action);
            Ok(())
        })
    }
    fn handle(
        &self,
        incoming: ServiceIncoming<MemoryStateStore, MemoryStateStore, Bus>,
    ) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let connection = self.connection.lock().unwrap().clone().unwrap();
            let action = connection.accept(incoming).await?;
            self.received.lock().unwrap().push(action);
            Ok(())
        })
    }
}
struct Harness {
    runtime: ProtocolRuntime<MemoryStateStore, MemoryStateStore, MemoryStateStore>,
    directory: ServiceDirectory<MemoryStateStore>,
    provider: ServiceProvider<MemoryStateStore, MemoryStateStore, Bus>,
    client: ServiceClient<MemoryStateStore, MemoryStateStore, Bus>,
    provider_messages: MessageSession<Bus>,
    client_messages: MessageSession<Bus>,
    backend: Arc<Backend>,
    references: Arc<References>,
    action: Action,
    bus: Bus,
}
impl Harness {
    async fn new() -> Self {
        let ads = MemoryStateStore::ttl_bound(30).unwrap();
        let runtime = ProtocolRuntime::start(
            ads.clone(),
            MemoryStateStore::default(),
            MemoryStateStore::ttl_bound(30).unwrap(),
        )
        .await
        .unwrap();
        let protocol = declaration();
        let directory =
            ServiceDirectory::new(runtime.beacon().clone(), protocol.protocol().clone()).unwrap();
        directory.try_start().unwrap();
        bounded(directory.wait_ready()).await;
        let bus = Bus::default();
        let provider_messages = MessageSession::new(
            bus.clone(),
            EndpointSession::with_id(
                action_runtime_endpoint("example").unwrap(),
                "provider-session",
            )
            .unwrap(),
        )
        .unwrap();
        let client_messages = MessageSession::new(
            bus.clone(),
            EndpointSession::with_id(
                EndpointAddress::parse("controller:main").unwrap(),
                "controller-session",
            )
            .unwrap(),
        )
        .unwrap();
        let backend = Arc::new(Backend {
            references: References::new(),
            connection: Mutex::new(None),
            received: Mutex::new(Vec::new()),
        });
        let operations = protocol
            .protocol()
            .operations
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let granted = operations.clone();
        let provider = ServiceProvider::start(ServiceProviderSpec {
            advertisements: ads,
            concord: runtime.concord().clone(),
            messages: provider_messages.clone(),
            protocol: protocol.clone(),
            advertisement: protocol
                .protocol()
                .advertisement_payload(
                    "action-runtime.example",
                    "provider-session",
                    ServiceBackendStatus::Available,
                )
                .unwrap(),
            initial_consent: true,
            admission: Arc::new(move |_: ServiceConsumer| {
                let operations = granted.clone();
                async move {
                    Ok(Some(ServicePermissions {
                        operations: operations.into_iter().collect(),
                        views: BTreeMap::new(),
                    }))
                }
            }),
            backend: backend.clone(),
            options: Default::default(),
        })
        .unwrap();
        let client = ServiceClient::start(ServiceClientSpec {
            directory: directory.clone(),
            protocol,
            concord: runtime.concord().clone(),
            messages: client_messages.clone(),
            target: Default::default(),
            policy: Arc::new(move |descriptor: ServiceDescriptor| {
                let operations = operations.clone();
                async move {
                    Ok(Some(service_access(
                        &descriptor,
                        operations,
                        BTreeMap::new(),
                    )?))
                }
            }),
            options: Default::default(),
        })
        .unwrap();
        let service = bounded(client.wait_ready(None)).await.unwrap();
        until(|| backend.connection.lock().unwrap().is_some()).await;
        let references = References::new();
        let action = ActionConnection::new(service, "example", references.clone()).unwrap();
        references.bind(&action);
        Self {
            runtime,
            directory,
            provider,
            client,
            provider_messages,
            client_messages,
            backend,
            references,
            action,
            bus,
        }
    }
    fn provider_action(&self) -> Action {
        self.backend.connection.lock().unwrap().clone().unwrap()
    }
    async fn close(self) {
        self.client.close().await.unwrap();
        self.provider.close().await.unwrap();
        self.client_messages.close().await.unwrap();
        self.provider_messages.close().await.unwrap();
        self.directory.close_and_wait().await;
        self.runtime.close().await;
    }
}
async fn bounded<R>(work: impl std::future::Future<Output = R>) -> R {
    tokio::time::timeout(Duration::from_secs(5), work)
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
async fn managed_action_connections_dispatch_both_directions_and_reject_stale_owner_state() {
    let h = Harness::new().await;
    let created_event = ActionMessage::ActionInstanceCreated(Box::new(ActionInstanceLifecycle {
        metadata: binding().action_instance(),
        reason: None,
    }));
    h.action.send_action(&created_event).await.unwrap();
    until(|| !h.backend.received.lock().unwrap().is_empty()).await;
    let created = h.backend.received.lock().unwrap()[0].clone();
    created.authorize().unwrap();
    let mut reader = h.client.watch_messages().unwrap();
    let provider = h.provider_action();
    provider
        .send_action(&message("binding-overlay"))
        .await
        .unwrap();
    let overlay = bounded(reader.next()).await.unwrap().unwrap();
    let accepted = h.action.accept(overlay.clone()).await.unwrap();
    accepted.authorize().unwrap();
    assert_eq!(accepted.action().name(), "binding_overlay");
    assert_eq!(
        accepted.message().contract.as_ref(),
        Some(&h.action.scope().contract)
    );
    h.references.binding.lock().unwrap().output_generation += 1;
    assert!(accepted.authorize().is_err());
    assert!(h.action.accept(overlay).await.is_err());
    h.action.authorize().unwrap();
    h.references.binding.lock().unwrap().output_generation -= 1;
    h.references
        .scope
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .controller_session_id = "other-session".into();
    assert!(accepted.authorize().is_err());
    h.references.bind(&h.action);
    accepted.authorize().unwrap();
    h.client.close().await.unwrap();
    assert!(accepted.authorize().is_err());
    assert!(h.action.send_action(&created_event).await.is_err());
    assert!(provider
        .send_action(&message("binding-overlay"))
        .await
        .is_err());
    h.close().await;
}

#[tokio::test]
async fn provider_identity_direction_and_owner_panic_cannot_bypass_action_dispatch() {
    let h = Harness::new().await;
    let provider = h.provider_action();
    for field in ["providerInstanceId", "providerId"] {
        let mut invalid = provider.message(&message("binding-overlay")).unwrap();
        invalid.body["params"]["binding"][field] = serde_json::json!("other");
        assert!(provider.send(invalid).await.is_err());
    }
    let mut availability = ActionAvailabilityPayload::from_value(serde_json::json!({
        "providerInstanceId":"example", "providerId":"example", "serviceId":"action-runtime.example",
        "serviceEndpoint":"service:action-runtime.example", "serviceSessionId":"provider-session", "entries":[]
    })).unwrap();
    h.action.validate_availability(&availability).unwrap();
    availability.provider_id = "other".into();
    assert!(h.action.validate_availability(&availability).is_err());
    availability.provider_id = "example".into();
    availability.service_session_id = "successor-session".into();
    assert!(h.action.validate_availability(&availability).is_err());
    let mut invalid = provider.message(&message("binding-overlay")).unwrap();
    invalid.recipient_session_id = Some("other-session".into());
    assert!(provider.send(invalid).await.is_err());
    assert!(h.action.message(&message("binding-overlay")).is_err());
    h.backend.references.panic.store(true, Ordering::SeqCst);
    assert!(matches!(
        provider.send_action(&message("binding-overlay")).await,
        Err(Error::StateUnavailable(_))
    ));
    assert!(h.bus.published().is_empty());
    provider.authorize().unwrap();
    provider
        .send_action(&message("binding-overlay"))
        .await
        .unwrap();
    assert_eq!(h.bus.published().len(), 1);
    h.close().await;
}

#[tokio::test]
async fn page_commands_require_owner_resolution_without_invented_wire_identity() {
    let h = Harness::new().await;
    let provider = h.provider_action();
    assert!(provider.send_action(&message("open-page")).await.is_err());
    h.backend
        .references
        .page_ready
        .store(true, Ordering::SeqCst);
    let mut reader = h.client.watch_messages().unwrap();
    provider.send_action(&message("open-page")).await.unwrap();
    let incoming = bounded(reader.next()).await.unwrap().unwrap();
    assert!(h.action.accept(incoming.clone()).await.is_err());
    h.references.page_ready.store(true, Ordering::SeqCst);
    let accepted = h.action.accept(incoming).await.unwrap();
    assert!(accepted.message().subject.identifiers.is_empty());
    h.references.page_ready.store(false, Ordering::SeqCst);
    assert!(accepted.authorize().is_err());
    h.action.authorize().unwrap();
    h.close().await;
}

#[test]
fn reference_changes_during_schema_validation_are_checked_before_publish_and_accept() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let h = Harness::new().await;
            let provider = h.provider_action();
            let (release, blocked) = std::sync::mpsc::channel();
            let (started, ready) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            });
            ready.await.unwrap();
            let sending = provider.clone();
            let send =
                tokio::spawn(async move { sending.send_action(&message("binding-overlay")).await });
            until(|| h.backend.references.calls.load(Ordering::SeqCst) > 0).await;
            h.backend
                .references
                .binding
                .lock()
                .unwrap()
                .output_generation += 1;
            release.send(()).unwrap();
            blocker.await.unwrap();
            assert!(bounded(send).await.unwrap().is_err());
            assert!(h.bus.published().is_empty());
            h.backend
                .references
                .binding
                .lock()
                .unwrap()
                .output_generation -= 1;
            let mut reader = h.client.watch_messages().unwrap();
            provider
                .send_action(&message("binding-overlay"))
                .await
                .unwrap();
            let incoming = bounded(reader.next()).await.unwrap().unwrap();
            let (release, blocked) = std::sync::mpsc::channel();
            let (started, ready) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            });
            ready.await.unwrap();
            h.references.calls.store(0, Ordering::SeqCst);
            let action = h.action.clone();
            let accept = tokio::spawn(async move { action.accept(incoming).await });
            until(|| h.references.calls.load(Ordering::SeqCst) > 0).await;
            h.references.binding.lock().unwrap().output_generation += 1;
            release.send(()).unwrap();
            blocker.await.unwrap();
            assert!(bounded(accept).await.unwrap().is_err());
            h.action.authorize().unwrap();
            h.close().await;
        });
}

#[test]
fn core_declaration_cannot_relabel_operations_directions_payloads_or_view_ownership() {
    let valid = protocol();
    validate_action_runtime_protocol(&valid).unwrap();
    let mut changed = Vec::new();
    let mut p = valid.clone();
    p.messages.get_mut("binding_output").unwrap().direction =
        ServiceMessageDirection::ConsumerToService;
    changed.push(p);
    let mut p = valid.clone();
    p.messages.get_mut("binding_output").unwrap().operation = Some("open_page".into());
    changed.push(p);
    let mut p = valid.clone();
    p.messages
        .get_mut("binding_output")
        .unwrap()
        .params_schema
        .as_mut()
        .unwrap()
        .schema = serde_json::json!({"type":"object"});
    changed.push(p);
    let mut p = valid.clone();
    p.messages
        .get_mut("binding_output")
        .unwrap()
        .exchange_pattern = ExchangePattern::RequestReply;
    changed.push(p);
    let mut p = valid.clone();
    p.messages.remove("close_page");
    changed.push(p);
    let mut p = valid.clone();
    p.view_families
        .get_mut(ACTION_AVAILABILITY_FAMILY)
        .unwrap()
        .writer = ServiceViewWriter::Consumer;
    changed.push(p);
    let mut p = valid.clone();
    p.feature_id = "org.example.feature".into();
    changed.push(p);
    for p in changed {
        assert!(validate_action_runtime_protocol(&p).is_err());
    }
}
