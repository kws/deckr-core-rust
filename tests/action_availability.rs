#[path = "support/service_bus.rs"]
mod service_bus;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use deckr::action_runtime::*;
use deckr::endpoint::EndpointAddress;
use deckr::lifecycle::Phase;
use deckr::messaging::MessageSession;
use deckr::runtime::ProtocolRuntime;
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use deckr::session::EndpointSession;
use deckr::state::{MaterializedStateStore, MemoryStateStore, StateStore};
use serde_json::json;
use service_bus::Bus;

type View = ActionAvailability<MemoryStateStore, MemoryStateStore, MemoryStateStore>;
type Connection = ServiceConnection<MemoryStateStore, MemoryStateStore, Bus>;
struct Backend {
    store: ServiceViewStore<MemoryStateStore>,
    writers: Mutex<Vec<View>>,
    connections: Mutex<Vec<Connection>>,
}
impl ServiceBackend<MemoryStateStore, MemoryStateStore, Bus> for Backend {
    fn connected(&self, connection: Connection) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let view = ActionAvailability::bind(connection.context().clone(), &self.store)?;
            view.wait_ready(None).await?;
            let key = format!(
                "{}available",
                connection.context().descriptor().views[ACTION_AVAILABILITY_FAMILY].key_prefix
            );
            view.create(&key, &payload()).await?;
            self.writers.lock().unwrap().push(view);
            self.connections.lock().unwrap().push(connection);
            Ok(())
        })
    }
    fn handle(
        &self,
        _incoming: ServiceIncoming<MemoryStateStore, MemoryStateStore, Bus>,
    ) -> ServiceBackendFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}
fn payload() -> ActionAvailabilityPayload {
    ActionAvailabilityPayload::from_value(json!({"providerInstanceId":"clock","providerId":"org.example.clock","serviceId":"action-runtime.clock","serviceEndpoint":"service:action-runtime.clock","serviceSessionId":"provider-session","entries":[{"actionId":"org.example.clock.time","status":"available","descriptor":{"actionId":"org.example.clock.time"}}],"annotations":{"owner":{"$serde_json::private::Number":"literal"}}})).unwrap()
}
struct Harness {
    runtime: ProtocolRuntime<MemoryStateStore, MemoryStateStore, MemoryStateStore>,
    directory: ServiceDirectory<MemoryStateStore>,
    raw: MemoryStateStore,
    backend: Arc<Backend>,
    provider: ServiceProvider<MemoryStateStore, MemoryStateStore, Bus>,
    client: ServiceClient<MemoryStateStore, MemoryStateStore, Bus>,
    provider_messages: MessageSession<Bus>,
    client_messages: MessageSession<Bus>,
    connection: Connection,
    reader: View,
    key: String,
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
        let protocol = ServiceProtocol::new(
            ACTION_RUNTIME_NAMESPACE,
            ACTION_RUNTIME_NAMESPACE,
            ACTION_RUNTIME_ADVERTISEMENT_PROFILE,
            ACTION_RUNTIME_USE_PROFILE,
            Vec::<String>::new(),
            BTreeMap::from([(
                ACTION_AVAILABILITY_FAMILY.into(),
                ServiceViewFamilyDefinition::new(ACTION_AVAILABILITY_STORE).unwrap(),
            )]),
        )
        .unwrap();
        let directory = ServiceDirectory::new(runtime.beacon().clone(), protocol.clone()).unwrap();
        directory.try_start().unwrap();
        bounded(directory.wait_ready()).await;
        let advertisement = protocol
            .advertisement_payload(
                "action-runtime.clock",
                "provider-session",
                ServiceBackendStatus::Available,
            )
            .unwrap();
        let protocol = ValidatedServiceProtocol::new(protocol, &SchemaRegistry::default()).unwrap();
        let bus = Bus::default();
        let provider_messages = MessageSession::new(
            bus.clone(),
            EndpointSession::with_id(
                action_runtime_endpoint("clock").unwrap(),
                "provider-session",
            )
            .unwrap(),
        )
        .unwrap();
        let client_messages = MessageSession::new(
            bus,
            EndpointSession::with_id(
                EndpointAddress::parse("controller:main").unwrap(),
                "controller-session",
            )
            .unwrap(),
        )
        .unwrap();
        let raw = MemoryStateStore::default();
        let source = MaterializedStateStore::start(raw.clone(), "")
            .await
            .unwrap();
        let backend = Arc::new(Backend {
            store: ServiceViewStore::new(ACTION_AVAILABILITY_STORE, source).unwrap(),
            writers: Mutex::default(),
            connections: Mutex::default(),
        });
        let provider = ServiceProvider::start(ServiceProviderSpec {
            advertisements: ads,
            concord: runtime.concord().clone(),
            messages: provider_messages.clone(),
            protocol: protocol.clone(),
            advertisement,
            initial_consent: true,
            admission: Arc::new(|_: ServiceConsumer| async {
                Ok(Some(ServicePermissions {
                    operations: Default::default(),
                    views: BTreeMap::from([(ACTION_AVAILABILITY_FAMILY.into(), Vec::new())]),
                }))
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
            target: ServiceTarget::default(),
            policy: Arc::new(|descriptor: ServiceDescriptor| async move {
                Ok(Some(service_access(
                    &descriptor,
                    Vec::<String>::new(),
                    BTreeMap::from([(ACTION_AVAILABILITY_FAMILY.into(), Vec::new())]),
                )?))
            }),
            options: Default::default(),
        })
        .unwrap();
        let connection = bounded(client.wait_ready(None)).await.unwrap();
        until(|| !backend.writers.lock().unwrap().is_empty()).await;
        let reader =
            ActionAvailability::bind(connection.context().clone(), &backend.store).unwrap();
        bounded(reader.wait_ready(None)).await.unwrap();
        let key = format!(
            "{}available",
            connection.context().descriptor().views[ACTION_AVAILABILITY_FAMILY].key_prefix
        );
        Self {
            runtime,
            directory,
            raw,
            backend,
            provider,
            client,
            provider_messages,
            client_messages,
            connection,
            reader,
            key,
        }
    }
    fn writer(&self) -> View {
        self.backend.writers.lock().unwrap()[0].clone()
    }
    async fn close(self) {
        self.client.close().await.unwrap();
        self.provider.close().await.unwrap();
        self.reader.close().await.unwrap();
        let writers = std::mem::take(&mut *self.backend.writers.lock().unwrap());
        for writer in writers {
            writer.close().await.unwrap();
        }
        self.client_messages.close().await.unwrap();
        self.provider_messages.close().await.unwrap();
        self.backend.store.source().close_and_wait().await;
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
async fn managed_service_provider_publishes_typed_availability_with_flat_exact_fences() {
    let h = Harness::new().await;
    let mut declaration = h.connection.context().declaration().protocol().clone();
    declaration.feature_id = "org.example.wrong".into();
    let mut descriptor = h.connection.context().descriptor().clone();
    descriptor.candidate = None;
    let wrong = ServiceContext::new(
        h.connection.context().agreement().clone(),
        ValidatedServiceProtocol::new(declaration, &SchemaRegistry::default()).unwrap(),
        descriptor,
        h.connection.context().access().clone(),
    )
    .unwrap();
    assert!(ActionAvailability::bind(wrong, &h.backend.store).is_err());
    let entry = h.reader.get(&h.key).await.unwrap().unwrap();
    assert_eq!(entry.record.availability, payload());
    assert_eq!(entry.record.fence.consumer_session_id, "controller-session");
    assert_eq!(
        entry.record.fence.contract_id,
        h.connection.context().agreement().contract().contract_id
    );
    let stored = h.raw.items("").await.unwrap().pop().unwrap();
    assert_eq!(stored.value["providerInstanceId"], json!("clock"));
    assert_eq!(stored.value["writer"], json!("service"));
    assert!(stored.value.get("payload").is_none());
    assert_eq!(
        stored.value["annotations"]["owner"],
        json!({"$serde_json::private::Number":"literal"})
    );
    assert_eq!(
        ActionAvailabilityRecord::from_value(stored.value.clone())
            .unwrap()
            .to_value()
            .unwrap(),
        stored.value
    );
    assert!(h
        .reader
        .create(&format!("{}consumer", h.key), &payload())
        .await
        .is_err());
    let mut wrong = payload();
    wrong.service_session_id = "other-session".into();
    assert!(h
        .writer()
        .update(&h.key, &wrong, entry.revision)
        .await
        .is_err());
    let mut wrong = payload();
    wrong.entries[0].descriptor.as_mut().unwrap().action_id = "other".into();
    assert!(h
        .writer()
        .update(&h.key, &wrong, entry.revision)
        .await
        .is_err());
    let mut updated = payload();
    updated.entries[0].status = ActionAvailabilityStatus::Unavailable;
    updated.entries[0].reason = Some("backend warming".into());
    let revision = h
        .writer()
        .update(&h.key, &updated, entry.revision)
        .await
        .unwrap();
    assert_eq!(
        h.reader.get(&h.key).await.unwrap().unwrap().revision,
        revision
    );
    h.writer().delete(&h.key, revision).await.unwrap();
    assert!(h.reader.get(&h.key).await.unwrap().is_none());
    h.close().await;
}
#[tokio::test]
async fn malformed_availability_replacement_only_removes_previously_visible_entries() {
    let h = Harness::new().await;
    let mut watch = h.reader.watch_state().unwrap();
    assert_eq!(watch.next().await.unwrap().value.entries.len(), 1);
    let mut raw = h.raw.items("").await.unwrap().pop().unwrap();
    raw.value["providerInstanceId"] = json!("different");
    h.raw
        .update(&raw.key, raw.value.clone(), raw.revision, None)
        .await
        .unwrap();
    let removed = bounded(async {
        loop {
            let state = watch.next().await.unwrap();
            if state.current && state.value.entries.is_empty() {
                break state;
            }
        }
    })
    .await;
    assert!(removed.changed.contains(&h.key));
    assert!(h.reader.get(&h.key).await.unwrap().is_none());
    assert!(h.connection.authorize().is_ok());
    let raw = h.raw.get(&raw.key).await.unwrap().unwrap();
    let mut wrong = raw.value.clone();
    wrong["providerInstanceId"] = json!("clock");
    wrong["entries"][0]["descriptor"]["actionId"] = json!("wrong-action");
    h.raw
        .update(&raw.key, wrong, raw.revision, None)
        .await
        .unwrap();
    until(|| h.reader.state().current).await;
    assert!(h.reader.state().value.entries.is_empty());
    h.close().await;
}
#[tokio::test]
async fn availability_source_closure_does_not_end_service_authority() {
    let h = Harness::new().await;
    let mut watch = h.reader.watch_state().unwrap();
    watch.next().await.unwrap();
    h.backend.store.source().close_and_wait().await;
    bounded(async {
        while watch
            .next()
            .await
            .is_some_and(|state| state.phase != Phase::Closed)
        {}
    })
    .await;
    assert!(h.reader.get(&h.key).await.is_err());
    assert!(h.connection.authorize().is_ok());
    assert!(h.provider.connections()[0].authorize().is_ok());
    h.close().await;
}
#[tokio::test]
async fn retained_availability_cannot_outlive_its_agreement_and_closure_keeps_source_borrowed() {
    let h = Harness::new().await;
    let writer = h.writer();
    let mut state = h.provider.backend_state();
    state.consent = false;
    h.provider.set_backend(state).unwrap();
    assert!(writer.get(&h.key).await.is_err());
    assert!(writer
        .create(&format!("{}late", h.key), &payload())
        .await
        .is_err());
    until(|| !h.reader.state().current).await;
    h.reader.close().await.unwrap();
    writer.close().await.unwrap();
    assert!(h.backend.store.source().is_current());
    h.close().await;
}
