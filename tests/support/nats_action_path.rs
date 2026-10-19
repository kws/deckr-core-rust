//! The architecture's H -> S -> H example across three independent NATS hosts.
//! Application routing state is deliberately supplied by this test controller.

use deckr::action_runtime::*;
use deckr::authority::ContractPointer;
use deckr::endpoint::EndpointAddress;
use deckr::hardware::client::{
    HardwareCandidate, HardwareClient, HardwareClientSpec, HardwareSelection,
};
use deckr::hardware::runtime::{
    HardwareCommandContext, HardwareCommandFuture, HardwareCommandHandler, HardwareCommandOutcome,
    HardwareManagerRuntime,
};
use deckr::hardware::validation::ValidatedDevice;
use deckr::lanes::{DeckrMessage, DeviceDescriptor, HardwareMessageBody};
use deckr::nats::{DeckrRuntime, DeckrRuntimeConfig, NatsMessageTransport, NatsStateStore};
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use deckr::session::EndpointSession;
use deckr::state::{StateStore, StateStorePolicy};
use deckr::{Error, Result};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinSet;

type Connection = ServiceConnection<NatsStateStore, NatsStateStore, NatsMessageTransport>;
type Action = ActionConnection<NatsStateStore, NatsStateStore, NatsMessageTransport>;
type Incoming = ActionIncoming<NatsStateStore, NatsStateStore, NatsMessageTransport>;

fn declaration() -> ValidatedServiceProtocol {
    let root = std::env::var_os("DECKR_SPEC_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../deckr"))
        .join("contract/v1");
    let protocol = ServiceProtocol::from_text(
        &std::fs::read_to_string(root.join("catalogs/action-runtime-protocol.v1.json")).unwrap(),
    )
    .unwrap();
    let mut registry = SchemaRegistry::default();
    let mut pending = vec![root.join("schemas")];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                let schema: Value =
                    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
                if let Some(id) = schema.get("$id").and_then(Value::as_str) {
                    registry = registry.with_resource(id.to_string(), schema).unwrap();
                }
            }
        }
    }
    ValidatedServiceProtocol::new(protocol, &registry).unwrap()
}
fn descriptor() -> DeviceDescriptor {
    serde_json::from_value(json!({
        "deviceId":"deck","fingerprint":"first","displayName":"Test device",
        "controls":[{"controlId":"key","kind":"button",
            "inputCapabilities":[{"capabilityId":"press","family":"dev.deckr.input.button","type":"activation","direction":"input","access":["emits"],"eventTypes":["press"]}],
            "outputCapabilities":[{"capabilityId":"lamp","family":"org.example.lamp","type":"brightness","direction":"output","access":["settable"],"commandTypes":["set"],"commandSchema":{"schema":{"type":"object","properties":{"level":{"type":"integer","minimum":0,"maximum":10}},"required":["level"],"additionalProperties":false}}}]
        }]
    })).unwrap()
}
fn binding(manager: &str, instance: &str) -> BindingMetadata {
    let device = json!({"managerId":manager,"deviceId":"deck","fingerprint":"first"});
    BindingMetadata::from_value(json!({
        "providerInstanceId":instance,"providerId":"org.example.actions","actionId":"toggle",
        "actionInstanceId":"action-1","bindingId":"binding-1","configId":"layout-1","contextId":"page-1",
        "deviceRef":device,"controlRef":{"deviceRef":device,"controlId":"key"},"outputGeneration":1,
        "matchedCapabilities":[
            {"capability":{"deviceRef":device,"controlId":"key","capabilityId":"press"},"family":"dev.deckr.input.button","type":"activation","direction":"input","eventTypes":["press"]},
            {"capability":{"deviceRef":device,"controlId":"key","capabilityId":"lamp"},"family":"org.example.lamp","type":"brightness","direction":"output","commandTypes":["set"]}
        ]
    })).unwrap()
}
fn output(binding: &BindingMetadata) -> ActionMessage {
    ActionMessage::BindingOutput(Box::new(BindingOutput::from_value(json!({
        "binding":binding,"capability":{"deviceRef":binding.device_ref,"controlId":"key","capabilityId":"lamp"},
        "commandType":"set","generation":binding.output_generation,"params":{"level":5}
    })).unwrap()))
}
struct References {
    scope: Mutex<Option<ActionScope>>,
    binding: Mutex<BindingMetadata>,
    action: ActionDescriptor,
    device: ValidatedDevice,
}
impl References {
    fn new(binding: BindingMetadata) -> Arc<Self> {
        Arc::new(Self {
            scope: Mutex::new(None),
            binding: Mutex::new(binding),
            action: ActionDescriptor::from_value(json!({"actionId":"toggle"})).unwrap(),
            device: ValidatedDevice::new(descriptor(), &SchemaRegistry::default()).unwrap(),
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
        _: &DeckrMessage,
        action: &ActionMessage,
    ) -> Result<()> {
        if self.scope.lock().unwrap().as_ref() != Some(scope) {
            return Err(Error::Unauthorized(
                "controller scope is no longer current".into(),
            ));
        }
        let current = self.binding.lock().unwrap();
        let manager = &current.device_ref.manager_id;
        match action {
            ActionMessage::CapabilityInput(body) => {
                validate_capability_input(body, &current, &self.action, manager, &self.device)
            }
            ActionMessage::BindingOutput(body) => {
                validate_binding_output(body, &current, &self.action, manager, &self.device)
                    .map(|_| ())
            }
            _ => Err(Error::Unauthorized(
                "test controller has no current route for this message".into(),
            )),
        }
    }
}
#[derive(Default)]
struct Driver(Mutex<Vec<HardwareCommandContext>>);
impl HardwareCommandHandler for Driver {
    fn handle_authorized_hardware_command<'a>(
        &'a self,
        context: HardwareCommandContext,
    ) -> HardwareCommandFuture<'a> {
        Box::pin(async move {
            context.authorize()?;
            let HardwareMessageBody::ControlCommand {
                device_ref,
                control_id,
                capability_id,
                command_type,
                ..
            } = context.message().hardware_body()?
            else {
                return Err(Error::Invalid("expected control command".into()));
            };
            self.0.lock().unwrap().push(context.clone());
            context
                .reply(HardwareMessageBody::CommandReply {
                    device_ref,
                    control_id,
                    capability_id,
                    command_type,
                    result: Some(json!({"done":true})),
                })
                .await?;
            Ok(HardwareCommandOutcome::Handled)
        })
    }
}
struct Backend {
    references: Arc<References>,
    connection: Mutex<Option<Action>>,
    received: Mutex<Vec<Incoming>>,
}
impl ServiceBackend<NatsStateStore, NatsStateStore, NatsMessageTransport> for Backend {
    fn connected(&self, service: Connection) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let action =
                ActionConnection::new(service, "org.example.actions", self.references.clone())?;
            self.references.bind(&action);
            *self.connection.lock().unwrap() = Some(action);
            Ok(())
        })
    }
    fn handle(
        &self,
        incoming: ServiceIncoming<NatsStateStore, NatsStateStore, NatsMessageTransport>,
    ) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let action = self.connection.lock().unwrap().clone().unwrap();
            let accepted = action.accept(incoming).await?;
            self.received.lock().unwrap().push(accepted);
            Ok(())
        })
    }
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("managed operation completes")
}
async fn until(mut predicate: impl FnMut() -> bool) {
    bounded(async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
}

#[test]
#[ignore = "requires DECKR_NATS_TEST_URL pointing to a disposable JetStream broker and the upstream spec checkout"]
fn hardware_action_routing_keeps_independent_authority_across_three_hosts() {
    // Nested debug-build futures for three NATS hosts exceed the test harness's
    // small default stack. Size this scenario's runner explicitly; protocol
    // workers retain Tokio's ordinary stack and need no environment override.
    std::thread::Builder::new()
        .name("hardware-action-path".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap()
                .block_on(Box::pin(exercise_hardware_action_path()));
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn exercise_hardware_action_path() {
    let url = std::env::var("DECKR_NATS_TEST_URL").unwrap();
    let id = uuid::Uuid::new_v4().simple().to_string();
    let manager_id = format!("manager-{id}");
    let mut config = DeckrRuntimeConfig::new(&url);
    config.beacon_advertisement_bucket = format!("path_{id}_beacon");
    config.concord_contract_bucket = format!("path_{id}_contracts");
    config.concord_token_bucket = format!("path_{id}_tokens");
    config.concord_maintenance_bucket = format!("path_{id}_maintenance");
    let controller_host = DeckrRuntime::connect(config.clone()).await.unwrap();
    let hardware_host = DeckrRuntime::connect(config.clone()).await.unwrap();
    let action_host = DeckrRuntime::connect(config.clone()).await.unwrap();
    let controller = controller_host.protocols().await.unwrap();
    let action_protocols = action_host.protocols().await.unwrap();
    let driver = Arc::new(Driver::default());
    let manager = HardwareManagerRuntime::from_deckr_runtime(
        &hardware_host,
        &manager_id,
        "manager-session",
        BTreeMap::new(),
        driver.clone(),
        None,
        Default::default(),
    )
    .await
    .unwrap();
    manager.set_device(descriptor()).await.unwrap();
    let mut workers = JoinSet::new();
    bounded(manager.start(&mut workers)).await.unwrap();
    let messages = controller_host
        .messaging(
            EndpointSession::with_id(
                EndpointAddress::parse(format!("controller:{id}")).unwrap(),
                "controller-session",
            )
            .unwrap(),
        )
        .unwrap();
    let hardware = HardwareClient::start(HardwareClientSpec {
        beacon: controller.beacon().clone(),
        concord: controller.concord().clone(),
        messages: messages.clone(),
        selection: Arc::new(|candidates: Vec<HardwareCandidate>| async move {
            Ok(candidates.first().map(|candidate| HardwareSelection {
                advertisement_key: candidate.advertisement_key.clone(),
                devices: BTreeMap::from([("deck".into(), 1)]),
            }))
        }),
        schemas: SchemaRegistry::default(),
        options: Default::default(),
    })
    .unwrap();
    let mut inputs = hardware.watch_events().unwrap();
    let h = bounded(hardware.wait_ready(None)).await.unwrap();
    let h_pointer = ContractPointer {
        contract_id: h.agreement().contract().contract_id.clone(),
        generation: 1,
    };

    let protocol = declaration();
    let directory =
        ServiceDirectory::new(controller.beacon().clone(), protocol.protocol().clone()).unwrap();
    directory.try_start().unwrap();
    let provider_messages = action_host
        .messaging(
            EndpointSession::with_id(action_runtime_endpoint(&id).unwrap(), "provider-session")
                .unwrap(),
        )
        .unwrap();
    let current_binding = binding(&manager_id, &id);
    let refs = References::new(current_binding.clone());
    let backend = Arc::new(Backend {
        references: References::new(current_binding.clone()),
        connection: Mutex::new(None),
        received: Mutex::new(Vec::new()),
    });
    let advertisement = protocol
        .protocol()
        .advertisement_payload(
            format!("action-runtime.{id}"),
            "provider-session",
            ServiceBackendStatus::Available,
        )
        .unwrap();
    let permissions = ServicePermissions::all(
        &ServiceDescriptor::from_payload(advertisement.clone(), protocol.protocol()).unwrap(),
    );
    let provider = ServiceProvider::start(ServiceProviderSpec {
        advertisements: action_host.beacon().clone(),
        concord: action_protocols.concord().clone(),
        messages: provider_messages.clone(),
        protocol: protocol.clone(),
        advertisement,
        initial_consent: true,
        admission: Arc::new(move |_: ServiceConsumer| {
            let permissions = permissions.clone();
            async move { Ok(Some(permissions)) }
        }),
        backend: backend.clone(),
        options: Default::default(),
    })
    .unwrap();
    let service = ServiceClient::start(ServiceClientSpec {
        directory: directory.clone(),
        protocol,
        concord: controller.concord().clone(),
        messages: messages.clone(),
        target: Default::default(),
        policy: Arc::new(|descriptor: ServiceDescriptor| async move {
            let views = descriptor
                .views
                .iter()
                .map(|(name, family)| (name.clone(), vec![family.key_prefix.clone()]))
                .collect();
            Ok(Some(service_access(
                &descriptor,
                descriptor.supported_operations.iter().cloned(),
                views,
            )?))
        }),
        options: Default::default(),
    })
    .unwrap();
    let s = ActionConnection::new(
        bounded(service.wait_ready(None)).await.unwrap(),
        "org.example.actions",
        refs.clone(),
    )
    .unwrap();
    refs.bind(&s);
    until(|| backend.connection.lock().unwrap().is_some()).await;
    let provider_action = backend.connection.lock().unwrap().clone().unwrap();
    assert_ne!(s.scope().contract, h_pointer);
    let mut outputs = service.watch_messages().unwrap();

    // A real manager event reaches the controller under H; only then does the
    // controller resolve its binding and send the canonical action input under S.
    manager
        .handle_hardware_message(HardwareMessageBody::ControlInput {
            device_ref: current_binding.device_ref.clone(),
            control_id: "key".into(),
            capability_id: "press".into(),
            event_type: "press".into(),
            value: Some(json!({"eventType":"press"})),
            occurred_at: None,
            sequence: None,
            sources: vec![],
        })
        .await
        .unwrap();
    let input = bounded(inputs.next()).await.unwrap().unwrap();
    input.authorize().unwrap();
    assert_eq!(input.message.contract.as_ref(), Some(&h_pointer));
    let action_input=ActionMessage::CapabilityInput(Box::new(CapabilityInput::from_value(json!({"binding":current_binding,"event":{"capability":{"deviceRef":current_binding.device_ref,"controlId":"key","capabilityId":"press"},"eventType":"press","occurredAt":chrono::Utc::now().to_rfc3339(),"value":{"eventType":"press"}}})).unwrap()));
    s.send_action(&action_input).await.unwrap();
    until(|| !backend.received.lock().unwrap().is_empty()).await;
    let received = backend.received.lock().unwrap()[0].clone();
    received.authorize().unwrap();
    assert_eq!(
        received.message().contract.as_ref(),
        Some(&s.scope().contract)
    );

    provider_action
        .send_action(&output(&current_binding))
        .await
        .unwrap();
    let accepted = s
        .accept(bounded(outputs.next()).await.unwrap().unwrap())
        .await
        .unwrap();
    accepted.authorize().unwrap();
    let ActionMessage::BindingOutput(body) = accepted.action() else {
        unreachable!()
    };
    let command = validate_binding_output(
        body,
        &current_binding,
        &refs.action,
        &manager_id,
        &h.devices()["deck"],
    )
    .unwrap();
    let reply = bounded(h.request(command.clone(), Duration::from_secs(3), None))
        .await
        .unwrap();
    assert_eq!(reply.contract.as_ref(), Some(&h_pointer));
    assert_eq!(
        driver.0.lock().unwrap()[0].message().contract.as_ref(),
        Some(&h_pointer)
    );

    // Availability is carried by a separate shared source and exact S fences.
    let writer_store = action_host
        .service_views(
            ACTION_AVAILABILITY_STORE,
            StateStorePolicy::persistent("action availability"),
        )
        .await
        .unwrap();
    let reader_store = controller_host
        .service_views(
            ACTION_AVAILABILITY_STORE,
            StateStorePolicy::persistent("action availability"),
        )
        .await
        .unwrap();
    let writer =
        ActionAvailability::bind(provider_action.service().context().clone(), &writer_store)
            .unwrap();
    let reader = ActionAvailability::bind(s.service().context().clone(), &reader_store).unwrap();
    bounded(writer.wait_ready(None)).await.unwrap();
    bounded(reader.wait_ready(None)).await.unwrap();
    let key = format!(
        "{}available",
        s.service().context().descriptor().views[ACTION_AVAILABILITY_FAMILY].key_prefix
    );
    let payload=ActionAvailabilityPayload::from_value(json!({"providerInstanceId":id,"providerId":"org.example.actions","serviceId":format!("action-runtime.{id}"),"serviceEndpoint":action_runtime_endpoint(&id).unwrap(),"serviceSessionId":"provider-session","entries":[{"actionId":"toggle","status":"available","descriptor":{"actionId":"toggle"}}]})).unwrap();
    writer.create(&key, &payload).await.unwrap();
    until(|| reader.state().value.entries.contains_key(&key)).await;
    assert_eq!(
        reader
            .get(&key)
            .await
            .unwrap()
            .unwrap()
            .record
            .fence
            .contract_id,
        s.scope().contract.contract_id
    );
    writer.close().await.unwrap();

    // Loss of S revokes retained action messages and availability, while H
    // remains valid and can still complete an independently authorized command.
    let mut backend_state = provider.backend_state();
    backend_state.consent = false;
    provider.set_backend(backend_state.clone()).unwrap();
    until(|| s.authorize().is_err()).await;
    assert!(received.authorize().is_err());
    assert!(accepted.authorize().is_err());
    assert!(reader.get(&key).await.is_err());
    h.authorize().unwrap();
    bounded(h.request(command.clone(), Duration::from_secs(3), None))
        .await
        .unwrap();
    backend_state.consent = true;
    provider.set_backend(backend_state).unwrap();
    let successor = ActionConnection::new(
        bounded(service.wait_ready(None)).await.unwrap(),
        "org.example.actions",
        refs.clone(),
    )
    .unwrap();
    refs.bind(&successor);
    assert_ne!(successor.scope().contract, s.scope().contract);
    assert!(s.authorize().is_err());
    until(|| {
        backend
            .connection
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|a| a.scope().contract == successor.scope().contract)
    })
    .await;
    let provider_successor = backend.connection.lock().unwrap().clone().unwrap();
    assert_eq!(
        driver.0.lock().unwrap().len(),
        2,
        "negotiating S must not replay hardware commands"
    );

    // Losing H leaves S usable, but an output accepted under S cannot use the
    // old hardware context or become a hardware command by forwarding S.
    bounded(hardware.close()).await.unwrap();
    provider_successor
        .send_action(&output(&current_binding))
        .await
        .unwrap();
    let retained = successor
        .accept(bounded(outputs.next()).await.unwrap().unwrap())
        .await
        .unwrap();
    retained.authorize().unwrap();
    assert!(h
        .request(command, Duration::from_secs(3), None)
        .await
        .is_err());
    assert_eq!(driver.0.lock().unwrap().len(), 2);
    refs.binding.lock().unwrap().output_generation += 1;
    assert!(retained.authorize().is_err());
    provider_successor
        .send_action(&output(&current_binding))
        .await
        .unwrap();
    assert!(successor
        .accept(bounded(outputs.next()).await.unwrap().unwrap())
        .await
        .is_err());
    successor.authorize().unwrap();

    reader.close().await.unwrap();
    bounded(service.close()).await.unwrap();
    bounded(provider.close()).await.unwrap();
    bounded(manager.stop()).await.unwrap();
    bounded(async {
        while let Some(result) = workers.join_next().await {
            match result {
                Ok(result) => result.unwrap(),
                Err(error) => assert!(error.is_cancelled(), "{error}"),
            }
        }
    })
    .await;
    messages.close().await.unwrap();
    provider_messages.close().await.unwrap();
    directory.close_and_wait().await;
    assert!(controller_host
        .nats()
        .concord_tokens()
        .items("")
        .await
        .unwrap()
        .is_empty());
    assert!(controller_host.beacon().items("").await.unwrap().is_empty());
    controller_host.nats().close_sources().await;
    hardware_host.nats().close_sources().await;
    action_host.nats().close_sources().await;
    let js = async_nats::jetstream::new(async_nats::connect(url).await.unwrap());
    for bucket in [
        &config.beacon_advertisement_bucket,
        &config.concord_contract_bucket,
        &config.concord_token_bucket,
        ACTION_AVAILABILITY_STORE,
    ] {
        let mut stream = js.get_stream(format!("KV_{bucket}")).await.unwrap();
        bounded(async {
            while stream.info().await.unwrap().state.consumer_count != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        js.delete_key_value(bucket).await.unwrap();
    }
}
