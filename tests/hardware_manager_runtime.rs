mod support;

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use deckr::authority::ContractPointer;
use deckr::beacon::{Beacon, BeaconAdvertiser};
use deckr::concord::{
    concord_participant_token_key, ConcordCoordinator, ContractHandle, ContractState,
    ContractValidityStatus, CreateContractSpec,
};
use deckr::endpoint::EndpointAddress;
use deckr::hardware::runtime::{
    HardwareCommandContext, HardwareCommandFuture, HardwareCommandHandler, HardwareCommandOutcome,
    HardwareLaneTransport, HardwareManagerRuntime, HardwareManagerRuntimeSpec,
    HardwareMessageStream, HardwareResetFuture, HardwareResetHandler,
};
use deckr::lanes::{DeckrMessage, DeviceDescriptor, DeviceRef, HardwareMessageBody};
use deckr::lifecycle::Phase;
use deckr::messaging::{Delivery, DeliveryStream};
use deckr::profiles::hardware::{
    hardware_payload_from_advertisement, HardwareClaimDevice, HardwareClaimTerms,
    HARDWARE_CLAIM_PROFILE_ID, HARDWARE_FEATURE_ID,
};
use deckr::state::{MaterializedStateStore, MemoryStateStore, StateMaintenancePolicy, StateStore};
use deckr::{Error, Result};
use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_util::StreamExt;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

type TestRuntime = HardwareManagerRuntime<
    MemoryStateStore,
    MemoryStateStore,
    MemoryStateStore,
    MemoryHardwareLane,
>;
type TestInboxes = BTreeMap<String, UnboundedSender<Result<Delivery>>>;

#[derive(Clone)]
struct MemoryHardwareLane {
    published: Arc<Mutex<Vec<DeckrMessage>>>,
    reply_routes: Arc<Mutex<Vec<Option<String>>>>,
    inboxes: Arc<std::sync::Mutex<TestInboxes>>,
    subscribed: Arc<std::sync::atomic::AtomicBool>,
    inbound_tx: UnboundedSender<Result<Delivery>>,
    inbound_rx: Arc<Mutex<Option<UnboundedReceiver<Result<Delivery>>>>>,
}

impl Default for MemoryHardwareLane {
    fn default() -> Self {
        let (inbound_tx, inbound_rx) = unbounded();
        Self {
            published: Arc::default(),
            reply_routes: Arc::default(),
            inboxes: Arc::default(),
            subscribed: Arc::default(),
            inbound_tx,
            inbound_rx: Arc::new(Mutex::new(Some(inbound_rx))),
        }
    }
}

impl MemoryHardwareLane {
    async fn published(&self) -> Vec<DeckrMessage> {
        self.published.lock().await.clone()
    }

    fn publish_inbound(&self, message: DeckrMessage) {
        self.publish_request(message, None);
    }

    fn publish_request(&self, message: DeckrMessage, reply_address: Option<String>) {
        self.inbound_tx
            .unbounded_send(Ok(Delivery {
                message,
                reply_address,
            }))
            .expect("test inbound hardware lane should be open");
    }
}

impl HardwareLaneTransport for MemoryHardwareLane {
    async fn publish_hardware_message(&self, message: DeckrMessage) -> Result<()> {
        self.published.lock().await.push(message);
        Ok(())
    }

    async fn publish_hardware_reply(
        &self,
        message: DeckrMessage,
        reply_address: Option<String>,
    ) -> Result<()> {
        self.reply_routes.lock().await.push(reply_address.clone());
        self.publish_hardware_message(message.clone()).await?;
        if let Some(sender) = reply_address
            .as_ref()
            .and_then(|address| self.inboxes.lock().unwrap().get(address).cloned())
        {
            sender
                .unbounded_send(Ok(Delivery {
                    message,
                    reply_address: None,
                }))
                .map_err(|_| Error::Closed("test reply inbox closed".into()))?;
        }
        Ok(())
    }

    async fn subscribe_hardware_messages<'a>(
        &'a self,
        endpoint: &'a EndpointAddress,
    ) -> Result<HardwareMessageStream> {
        let stream = self.subscribe_hardware_deliveries(endpoint).await?;
        Ok(Box::pin(
            stream.map(|delivery| delivery.map(|delivery| delivery.message)),
        ))
    }

    async fn subscribe_hardware_deliveries<'a>(
        &'a self,
        _endpoint: &'a EndpointAddress,
    ) -> Result<DeliveryStream> {
        let stream =
            self.inbound_rx.lock().await.take().ok_or_else(|| {
                Error::Invalid("test hardware lane already subscribed".to_string())
            })?;
        self.subscribed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let stream: DeliveryStream = Box::pin(stream);
        Ok(stream)
    }
}

#[derive(Default)]
struct RecordingCommandHandler {
    outcomes: Mutex<VecDeque<HardwareCommandOutcome>>,
    messages: Mutex<Vec<DeckrMessage>>,
    contexts: Mutex<Vec<HardwareCommandContext>>,
}

impl RecordingCommandHandler {
    async fn push_outcome(&self, outcome: HardwareCommandOutcome) {
        self.outcomes.lock().await.push_back(outcome);
    }

    async fn messages(&self) -> Vec<DeckrMessage> {
        self.messages.lock().await.clone()
    }
}

impl HardwareCommandHandler for RecordingCommandHandler {
    fn handle_hardware_command<'a>(
        &'a self,
        context: HardwareCommandContext,
    ) -> HardwareCommandFuture<'a> {
        Box::pin(async move {
            context.authorize()?;
            let message = context.message().clone();
            self.contexts.lock().await.push(context);
            self.messages.lock().await.push(message);
            Ok(self
                .outcomes
                .lock()
                .await
                .pop_front()
                .unwrap_or(HardwareCommandOutcome::Handled))
        })
    }
}

#[derive(Default)]
struct RecordingResetHandler {
    devices: Mutex<Vec<String>>,
}

impl RecordingResetHandler {
    async fn devices(&self) -> Vec<String> {
        self.devices.lock().await.clone()
    }
}

impl HardwareResetHandler for RecordingResetHandler {
    fn reset_hardware_device<'a>(&'a self, device_id: &'a str) -> HardwareResetFuture<'a> {
        Box::pin(async move {
            self.devices.lock().await.push(device_id.to_string());
            Ok(())
        })
    }
}

struct Harness {
    runtime: TestRuntime,
    beacon_state: MemoryStateStore,
    contract_state: MemoryStateStore,
    token_state: MemoryStateStore,
    lane: MemoryHardwareLane,
    command_handler: Arc<RecordingCommandHandler>,
    reset_handler: Arc<RecordingResetHandler>,
    concord: ConcordCoordinator<MemoryStateStore, MemoryStateStore>,
}

async fn harness() -> Harness {
    harness_with_policy(StateMaintenancePolicy {
        renewal_interval: Duration::from_secs(3600),
        concord_token_refresh_interval: Duration::from_secs(3600),
        reconcile_interval: Duration::from_secs(3600),
    })
    .await
}

async fn harness_with_policy(maintenance_policy: StateMaintenancePolicy) -> Harness {
    let beacon_state = MemoryStateStore::ttl_bound(30).unwrap();
    let contract_state = MemoryStateStore::new();
    let token_state = MemoryStateStore::ttl_bound(30).unwrap();
    let lane = MemoryHardwareLane::default();
    let command_handler = Arc::new(RecordingCommandHandler::default());
    let reset_handler = Arc::new(RecordingResetHandler::default());
    let concord = ConcordCoordinator::new(contract_state.clone(), token_state.clone());
    let runtime = HardwareManagerRuntime::new(HardwareManagerRuntimeSpec {
        manager_id: "manager-main".to_string(),
        session_id: "manager-session".to_string(),
        labels: BTreeMap::from([("driver".to_string(), "test".to_string())]),
        beacon_state: beacon_state.clone(),
        concord_contract_state: contract_state.clone(),
        concord_token_state: token_state.clone(),
        lane: lane.clone(),
        maintenance_policy,
        command_handler: command_handler.clone(),
        reset_handler: Some(reset_handler.clone()),
    })
    .await
    .unwrap();
    Harness {
        runtime,
        beacon_state,
        contract_state,
        token_state,
        lane,
        command_handler,
        reset_handler,
        concord,
    }
}

fn descriptor(device_id: &str) -> DeviceDescriptor {
    descriptor_with_fingerprint(device_id, &format!("fingerprint:{device_id}"))
}

fn descriptor_with_fingerprint(device_id: &str, fingerprint: &str) -> DeviceDescriptor {
    serde_json::from_value(serde_json::json!({
        "deviceId":device_id, "fingerprint":fingerprint, "displayName":"Test Device",
        "controls":[
            {"controlId":"screen", "kind":"screen", "outputCapabilities":[
                {"capabilityId":"raster.bitmap", "family":"dev.deckr.output.raster", "type":"bitmap", "direction":"output", "access":["settable","readable"], "commandTypes":["set_frame","clear"]}
            ]},
            {"controlId":"button.1", "kind":"button", "inputCapabilities":[
                {"capabilityId":"button.press", "family":"dev.deckr.input.button", "type":"activation", "direction":"input", "access":["emits"], "eventTypes":["press"]}
            ]}
        ]
    })).unwrap()
}

fn control_input(device_id: &str) -> HardwareMessageBody {
    HardwareMessageBody::ControlInput {
        device_ref: DeviceRef {
            manager_id: "manager-main".to_string(),
            device_id: device_id.to_string(),
            fingerprint: None,
        },
        control_id: "button.1".to_string(),
        capability_id: "button.press".to_string(),
        event_type: "press".to_string(),
        value: Some(serde_json::json!({"eventType":"press"})),
        occurred_at: None,
        sequence: None,
        sources: Vec::new(),
    }
}

fn control_command(controller_id: &str, controller_session: &str) -> DeckrMessage {
    control_command_with_contract(
        controller_id,
        controller_session,
        default_contract_id(controller_id),
    )
}

fn control_command_with_contract(
    controller_id: &str,
    controller_session: &str,
    contract_id: &str,
) -> DeckrMessage {
    DeckrMessage::hardware_command(
        controller_id,
        controller_session,
        "manager-main",
        "manager-session",
        "deck",
        contract_pointer(contract_id),
        HardwareMessageBody::ControlCommand {
            device_ref: DeviceRef {
                manager_id: "manager-main".to_string(),
                device_id: "deck".to_string(),
                fingerprint: None,
            },
            control_id: Some("screen".to_string()),
            capability_id: "raster.bitmap".to_string(),
            command_type: "clear".to_string(),
            params: Default::default(),
        },
    )
    .unwrap()
}

fn capability_state_request(controller_id: &str, controller_session: &str) -> DeckrMessage {
    DeckrMessage::hardware_command(
        controller_id,
        controller_session,
        "manager-main",
        "manager-session",
        "deck",
        contract_pointer(default_contract_id(controller_id)),
        HardwareMessageBody::CapabilityStateRequest {
            device_ref: DeviceRef {
                manager_id: "manager-main".to_string(),
                device_id: "deck".to_string(),
                fingerprint: None,
            },
            control_id: Some("screen".to_string()),
            capability_id: "raster.bitmap".to_string(),
            state_type: Some("frame".to_string()),
            params: Default::default(),
        },
    )
    .unwrap()
}

fn contract_pointer(contract_id: &str) -> ContractPointer {
    ContractPointer {
        contract_id: contract_id.to_string(),
        generation: 1,
    }
}

fn default_contract_id(controller_id: &str) -> &str {
    match controller_id {
        "a" => "claim-a",
        "b" => "claim-b",
        _ => "claim-a",
    }
}

async fn create_claim(
    concord: &ConcordCoordinator<MemoryStateStore, MemoryStateStore>,
    contract_id: &str,
    controller_id: &str,
    controller_session: &str,
) -> ContractHandle {
    create_claim_with_count(concord, contract_id, controller_id, controller_session, 1).await
}

async fn create_claim_with_count(
    concord: &ConcordCoordinator<MemoryStateStore, MemoryStateStore>,
    contract_id: &str,
    controller_id: &str,
    controller_session: &str,
    instance_count: u64,
) -> ContractHandle {
    let manager = EndpointAddress::parse("hardware_manager:manager-main").unwrap();
    let controller = EndpointAddress::parse(format!("controller:{controller_id}")).unwrap();
    let terms = HardwareClaimTerms {
        profile: HARDWARE_CLAIM_PROFILE_ID.to_string(),
        claim_id: format!("claim-{contract_id}"),
        controller_endpoint: controller.clone(),
        manager_endpoint: manager.clone(),
        devices: vec![HardwareClaimDevice {
            device_ref: DeviceRef {
                manager_id: "manager-main".to_string(),
                device_id: "deck".to_string(),
                fingerprint: Some("fingerprint:deck".to_string()),
            },
            instance_count,
        }],
    };
    let contract = concord
        .create_contract(CreateContractSpec {
            participants: vec![controller.clone(), manager],
            contract_id: Some(contract_id.to_string()),
            generation: 1,
            profile: Some(HARDWARE_CLAIM_PROFILE_ID.to_string()),
            terms: Some(terms.to_value().unwrap()),
            created_by: Some(controller.clone()),
            participant_sessions: BTreeMap::new(),
            supersedes: None,
        })
        .await
        .unwrap();
    concord
        .attach(&contract, &controller, controller_session, None)
        .await
        .unwrap();
    materialize().await;
    contract
}

async fn hardware_payload(
    beacon_state: &MemoryStateStore,
) -> deckr::profiles::hardware::HardwareBeaconPayload {
    let beacon = Beacon::start(beacon_state.clone()).await.unwrap();
    let candidates = beacon.candidates(HARDWARE_FEATURE_ID).unwrap();
    assert_eq!(candidates.len(), 1);
    hardware_payload_from_advertisement(&candidates[0].advertisement).unwrap()
}

async fn materialize() {
    tokio::time::sleep(Duration::from_millis(20)).await;
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..100 {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition did not become true");
}

async fn start_runtime(h: &Harness) -> JoinSet<Result<()>> {
    let mut tasks = JoinSet::new();
    h.runtime.start(&mut tasks).await.unwrap();
    tasks
}

async fn stop_runtime(h: &Harness, mut tasks: JoinSet<Result<()>>) {
    h.runtime.stop().await.unwrap();
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

#[tokio::test]
async fn beacon_replacement_does_not_revoke_live_claim() {
    let h = harness_with_policy(StateMaintenancePolicy {
        renewal_interval: Duration::from_millis(20),
        concord_token_refresh_interval: Duration::from_secs(3600),
        reconcile_interval: Duration::from_secs(3600),
    })
    .await;
    let mut tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let contract = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;
    let entry = h
        .beacon_state
        .items("advertisements.")
        .await
        .unwrap()
        .remove(0);
    let mut value = entry.value;
    value["sessionId"] = serde_json::json!("replacement-session");
    h.beacon_state
        .put(&entry.key, value, Some(30))
        .await
        .unwrap();
    // A real inventory change requests publication immediately. Managed
    // publication owns TTL scheduling; the old renewal knob is not a timer.
    h.runtime.set_device(descriptor("spare")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.runtime.diagnostics().await.discovery_error.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 2).await;
    assert!(h.concord.validate(&contract, None).await.valid());
    let state = h.runtime.state().await;
    assert_eq!(state.phase, Phase::Unavailable);
    assert!(state.failure.is_some());
    assert!(
        state.value.participation.unwrap().value.contracts[&contract.key]
            .value
            .local_authority
    );
    // Closure cannot withdraw a replacement owned by another session.
    assert!(h.runtime.stop().await.is_err());
    assert!(h.beacon_state.get(&entry.key).await.unwrap().is_some());
    let closed = h.runtime.state().await;
    assert_eq!(closed.phase, Phase::Closed);
    assert!(closed.failure.is_some());
    while tasks.join_next().await.is_some() {}
}

#[tokio::test]
async fn hardware_state_watches_cover_inventory_authority_and_bounded_closure() {
    let h = harness().await;
    let mut watch = h.runtime.watch_state().await.unwrap();
    let initial = watch.next().await.unwrap();
    assert_eq!(initial.phase, Phase::Starting);
    assert!(initial.value.devices.is_empty());
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let inventory = watch.next().await.unwrap();
    assert!(inventory.version > initial.version);
    assert_eq!(inventory.value.devices.len(), 1);
    assert!(initial.value.devices.is_empty());
    let tasks = start_runtime(&h).await;
    let ready = h
        .runtime
        .wait_for(|state| state.phase == Phase::Ready, Some(Duration::ZERO))
        .await
        .unwrap();
    assert!(ready.current);
    let claim = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    let active = h
        .runtime
        .wait_for(
            |state| {
                state
                    .value
                    .participation
                    .as_ref()
                    .is_some_and(|participant| {
                        participant
                            .value
                            .contracts
                            .get(&claim.key)
                            .is_some_and(|member| member.value.local_authority)
                    })
            },
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    assert!(active.version > ready.version);
    assert!(matches!(
        h.runtime.wait_for(|_| false, Some(Duration::ZERO)).await,
        Err(Error::Deadline(_))
    ));

    let mut subscriptions = Vec::new();
    for _ in 1..256 {
        subscriptions.push(h.runtime.watch_state().await.unwrap());
    }
    assert!(h.runtime.watch_state().await.is_err());
    // Shutdown must not need another slot in the bounded broadcaster.
    stop_runtime(&h, tasks).await;
    let closed = h.runtime.state().await;
    assert_eq!(closed.phase, Phase::Closed);
    assert!(!closed.current);
    assert!(closed
        .value
        .participation
        .unwrap()
        .value
        .contracts
        .values()
        .all(|member| !member.value.local_authority));
    drop(subscriptions);
    assert!(matches!(
        h.runtime
            .wait_for(|state| state.phase == Phase::Ready, None)
            .await,
        Err(Error::Closed(_))
    ));
    while let Some(state) = watch.next().await {
        if state.phase == Phase::Closed {
            assert!(watch.next().await.is_none());
            break;
        }
    }
}

#[tokio::test]
async fn manager_renews_from_ttl_while_a_driver_callback_is_blocked() {
    let contracts = MemoryStateStore::default();
    let tokens = MemoryStateStore::ttl_bound(4).unwrap();
    let concord = ConcordCoordinator::new(contracts.clone(), tokens.clone());
    let clock = Arc::new(support::ManualClock::default());
    let sources = ConcordCoordinator::new(
        MaterializedStateStore::start(contracts.clone(), "contracts.")
            .await
            .unwrap(),
        MaterializedStateStore::start(tokens.clone(), "contracts.")
            .await
            .unwrap(),
    )
    .clock(clock.clone());
    let lane = MemoryHardwareLane::default();
    let driver_entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let entered = driver_entered.clone();
    let handler = Arc::new(move |_context: HardwareCommandContext| {
        let entered = entered.clone();
        async move {
            entered.store(true, std::sync::atomic::Ordering::SeqCst);
            std::future::pending::<Result<HardwareCommandOutcome>>().await
        }
    });
    let runtime = HardwareManagerRuntime::with_concord(
        HardwareManagerRuntimeSpec {
            manager_id: "manager-main".into(),
            session_id: "manager-session".into(),
            labels: BTreeMap::new(),
            beacon_state: MemoryStateStore::ttl_bound(30).unwrap(),
            concord_contract_state: contracts,
            concord_token_state: tokens,
            lane: lane.clone(),
            maintenance_policy: StateMaintenancePolicy {
                renewal_interval: Duration::from_secs(3600),
                concord_token_refresh_interval: Duration::from_secs(3600),
                reconcile_interval: Duration::from_secs(3600),
            },
            command_handler: handler,
            reset_handler: None,
        },
        sources.clone(),
    )
    .unwrap();
    runtime.set_device(descriptor("deck")).await.unwrap();
    let mut tasks = JoinSet::new();
    runtime.start(&mut tasks).await.unwrap();
    let claim = create_claim(&concord, "claim-a", "main", "controller-session").await;
    runtime
        .wait_for(
            |state| {
                state
                    .value
                    .participation
                    .as_ref()
                    .is_some_and(|participant| {
                        participant
                            .value
                            .contracts
                            .get(&claim.key)
                            .is_some_and(|member| member.value.local_authority)
                    })
            },
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    lane.publish_inbound(control_command("main", "controller-session"));
    tokio::time::timeout(Duration::from_secs(1), async {
        while !driver_entered.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    clock.advance(Duration::from_secs(3));
    let renewed = runtime
        .wait_for(
            |state| {
                state
                    .value
                    .participation
                    .as_ref()
                    .is_some_and(|participant| {
                        participant
                            .value
                            .contracts
                            .get(&claim.key)
                            .is_some_and(|member| {
                                member
                                    .value
                                    .token
                                    .as_ref()
                                    .is_some_and(|token| token.refresh_seq == 2)
                            })
                    })
            },
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    assert!(
        renewed.value.participation.unwrap().value.contracts[&claim.key]
            .value
            .local_authority
    );
    tokio::time::timeout(Duration::from_secs(1), runtime.stop())
        .await
        .unwrap()
        .unwrap();
    while tasks.join_next().await.is_some() {}
    assert!(sources.is_current());
    sources.close_sources_and_wait().await;
}

#[tokio::test]
async fn malformed_lane_message_is_isolated_from_live_claims() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let contract = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;
    h.lane
        .inbound_tx
        .unbounded_send(Err(Error::Invalid("malformed message".into())))
        .unwrap();
    let mut malformed = control_command("main", "controller-session");
    malformed.created_at = "invalid timestamp".into();
    h.lane.publish_inbound(malformed);
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 2).await;
    assert!(h.concord.validate(&contract, None).await.valid());
    assert_eq!(h.runtime.diagnostics().await.rejected_messages, 2);
    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn material_descriptor_change_with_same_fingerprint_ends_claim() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let contract = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;
    let mut changed = descriptor("deck");
    changed.controls.push(deckr::lanes::ControlDescriptor {
        control_id: "new-control".into(),
        kind: "button".into(),
        label: None,
        geometry: None,
        input_capabilities: vec![],
        output_capabilities: vec![],
        ..Default::default()
    });
    h.runtime.set_device(changed).await.unwrap();
    assert!(!h.concord.validate(&contract, None).await.valid());
    assert_eq!(h.reset_handler.devices().await, vec!["deck"]);
    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn claim_instance_count_must_fit_advertised_capacity() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let contract =
        create_claim_with_count(&h.concord, "claim-a", "main", "controller-session", 2).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    assert!(!h.concord.validate(&contract, None).await.valid());
    let manager = EndpointAddress::parse("hardware_manager:manager-main").unwrap();
    assert!(h
        .concord
        .participant_token(&contract, &manager)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        hardware_payload(&h.beacon_state).await.devices["deck"]
            .capacity
            .available_instances,
        Some(1)
    );
    stop_runtime(&h, tasks).await;
}

#[derive(Clone)]
struct FailingCancellationStore {
    raw: MemoryStateStore,
    fail_updates: Arc<std::sync::atomic::AtomicBool>,
}

impl StateStore for FailingCancellationStore {
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        self.raw.ttl_seconds().await
    }
    async fn get(&self, key: &str) -> Result<Option<deckr::state::StateEntry>> {
        self.raw.get(key).await
    }
    async fn items(&self, prefix: &str) -> Result<Vec<deckr::state::StateEntry>> {
        self.raw.items(prefix).await
    }
    async fn put(
        &self,
        key: &str,
        value: serde_json::Value,
        ttl: Option<u64>,
    ) -> Result<deckr::state::StateEntry> {
        self.raw.put(key, value, ttl).await
    }
    async fn create(
        &self,
        key: &str,
        value: serde_json::Value,
        ttl: Option<u64>,
    ) -> Result<deckr::state::StateEntry> {
        self.raw.create(key, value, ttl).await
    }
    async fn update(
        &self,
        key: &str,
        value: serde_json::Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<deckr::state::StateEntry> {
        if self.fail_updates.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::StateUnavailable(
                "injected cancellation failure".into(),
            ));
        }
        self.raw.update(key, value, revision, ttl).await
    }
    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        self.raw.delete(key, revision).await
    }
    async fn delete_revision(&self, key: &str, revision: Option<u64>) -> Result<u64> {
        self.raw.delete_revision(key, revision).await
    }
    async fn watch(&self, prefix: &str) -> Result<deckr::state::StateWatchStream> {
        self.raw.watch(prefix).await
    }
    async fn open_source(&self, prefix: &str) -> Result<deckr::state::StateSource> {
        self.raw.open_source(prefix).await
    }
}

#[tokio::test]
async fn failed_cancellation_revokes_local_authority_and_withdraws_token() {
    let contracts = FailingCancellationStore {
        raw: MemoryStateStore::new(),
        fail_updates: Arc::default(),
    };
    let tokens = MemoryStateStore::ttl_bound(30).unwrap();
    let concord = ConcordCoordinator::new(contracts.raw.clone(), tokens.clone());
    let lane = MemoryHardwareLane::default();
    let handler = Arc::new(RecordingCommandHandler::default());
    let runtime = HardwareManagerRuntime::new(HardwareManagerRuntimeSpec {
        manager_id: "manager-main".into(),
        session_id: "manager-session".into(),
        labels: BTreeMap::new(),
        beacon_state: MemoryStateStore::ttl_bound(30).unwrap(),
        concord_contract_state: contracts.clone(),
        concord_token_state: tokens,
        lane: lane.clone(),
        maintenance_policy: StateMaintenancePolicy {
            renewal_interval: Duration::from_secs(3600),
            concord_token_refresh_interval: Duration::from_secs(3600),
            reconcile_interval: Duration::from_secs(3600),
        },
        command_handler: handler.clone(),
        reset_handler: None,
    })
    .await
    .unwrap();
    let mut tasks = JoinSet::new();
    runtime.start(&mut tasks).await.unwrap();
    runtime.set_device(descriptor("deck")).await.unwrap();
    let contract = create_claim(&concord, "claim-a", "main", "controller-session").await;
    lane.publish_inbound(control_command("main", "controller-session"));
    tokio::time::timeout(Duration::from_secs(1), async {
        while handler.messages().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    contracts
        .fail_updates
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(runtime
        .set_device(descriptor_with_fingerprint("deck", "replacement"))
        .await
        .is_err());
    lane.publish_inbound(control_command("main", "controller-session"));
    tokio::time::timeout(Duration::from_secs(1), async {
        while lane.published().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(handler.messages().await.len(), 1);
    let manager = EndpointAddress::parse("hardware_manager:manager-main").unwrap();
    assert!(concord
        .participant_token(&contract, &manager)
        .await
        .unwrap()
        .is_none());
    // Managed participation retains the failed cleanup in its closed state.
    assert!(runtime.stop().await.is_err());
    while tasks.join_next().await.is_some() {}
}

async fn wait_for_handler_messages(h: &Harness, count: usize) {
    for _ in 0..100 {
        if h.command_handler.messages().await.len() >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let published = h.lane.published().await;
    panic!(
        "command handler did not receive {count} messages; published={:?}",
        published
            .iter()
            .map(|message| match message.hardware_body() {
                Ok(HardwareMessageBody::CommandRejected { reason, .. }) => {
                    format!("{}:{reason}", message.message_type)
                }
                Ok(HardwareMessageBody::CapabilityStateReply { status, .. }) => {
                    format!("{}:{status}", message.message_type)
                }
                _ => message.message_type.clone(),
            })
            .collect::<Vec<_>>()
    );
}

async fn wait_for_published(h: &Harness, count: usize) -> Vec<DeckrMessage> {
    for _ in 0..100 {
        let published = h.lane.published().await;
        if published.len() >= count {
            return published;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("hardware lane did not publish {count} messages");
}

async fn wait_for_reset_devices(h: &Harness, expected: &[&str]) {
    let expected = expected
        .iter()
        .map(|device_id| device_id.to_string())
        .collect::<Vec<_>>();
    for _ in 0..100 {
        if h.reset_handler.devices().await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("reset handler did not receive expected devices");
}

async fn wait_for_capacity(h: &Harness, device_id: &str, claimed_instances: u64) {
    for _ in 0..100 {
        let payload = hardware_payload(&h.beacon_state).await;
        if payload
            .devices
            .get(device_id)
            .is_some_and(|device| device.capacity.claimed_instances == claimed_instances)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("hardware capacity did not reach claimed_instances={claimed_instances}");
}

#[tokio::test]
async fn publishes_beacon_payload_and_skips_noop_refresh() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();

    let payload = hardware_payload(&h.beacon_state).await;
    let deck = payload.devices.get("deck").unwrap();
    assert_eq!(deck.capacity.claimed_instances, 0);
    assert_eq!(deck.capacity.available_instances, Some(1));

    create_claim(&h.concord, "claim-b", "main", "controller-session").await;
    h.lane.publish_inbound(control_command_with_contract(
        "main",
        "controller-session",
        "claim-b",
    ));
    wait_for_handler_messages(&h, 1).await;
    wait_for_capacity(&h, "deck", 1).await;
    let payload = hardware_payload(&h.beacon_state).await;
    let deck = payload.devices.get("deck").unwrap();
    assert_eq!(deck.capacity.claimed_instances, 1);
    assert_eq!(deck.capacity.available_instances, Some(0));

    let beacon = Beacon::start(h.beacon_state.clone()).await.unwrap();
    let before = beacon.candidates(HARDWARE_FEATURE_ID).unwrap()[0]
        .advertisement
        .refresh_seq;
    h.lane
        .publish_inbound(control_command("other", "other-session"));
    wait_for_published(&h, 1).await;
    let after = beacon.candidates(HARDWARE_FEATURE_ID).unwrap()[0]
        .advertisement
        .refresh_seq;
    assert_eq!(before, after);

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn startup_preserves_competing_hardware_beacon_without_exit_evidence() {
    let h = harness().await;
    let manager = EndpointAddress::parse("hardware_manager:manager-main").unwrap();
    let stale_advertiser = BeaconAdvertiser::new(
        h.beacon_state.clone(),
        HARDWARE_FEATURE_ID,
        manager,
        "old-manager-session",
    )
    .advertisement_id("stale-hardware-ad")
    .payload(serde_json::json!({"profile": "stale"}));
    let stale = stale_advertiser.publish().await.unwrap();

    let tasks = start_runtime(&h).await;

    assert!(h.beacon_state.get(&stale.key).await.unwrap().is_some());
    let beacon = Beacon::start(h.beacon_state.clone()).await.unwrap();
    let candidates = beacon.candidates(HARDWARE_FEATURE_ID).unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates
        .iter()
        .any(|candidate| candidate.advertisement.advertisement_id == "stale-hardware-ad"));

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn fresh_claim_is_reconciled_before_command_rejection() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    create_claim(&h.concord, "claim-a", "main", "controller-session").await;

    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;

    assert_eq!(h.command_handler.messages().await.len(), 1);
    assert!(h.lane.published().await.is_empty());

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn unauthorized_and_unsupported_commands_emit_wire_replies() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();

    h.lane
        .publish_inbound(control_command("other", "other-session"));
    let published = wait_for_published(&h, 1).await;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].message_type, "commandRejected");
    let rejected = published[0].hardware_body().unwrap();
    assert!(matches!(
        rejected,
        HardwareMessageBody::CommandRejected { ref reason, ref message, .. }
            if reason == "unauthorized" && message.as_deref() == Some("Hardware command unauthorized")
    ));

    create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    wait_for_capacity(&h, "deck", 1).await;
    h.command_handler
        .push_outcome(HardwareCommandOutcome::Unsupported)
        .await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;
    let published = wait_for_published(&h, 2).await;
    let rejected = published.last().unwrap().hardware_body().unwrap();
    assert!(matches!(
        rejected,
        HardwareMessageBody::CommandRejected { ref reason, ref message, .. }
            if reason == "unsupported" && message.as_deref() == Some("Hardware command unsupported")
    ));

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn capability_state_request_rejections_use_expected_wire_replies() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;

    h.lane
        .publish_inbound(capability_state_request("main", "controller-session"));
    let published = wait_for_published(&h, 1).await;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].message_type, "capabilityStateReply");
    let rejected = published[0].hardware_body().unwrap();
    assert!(matches!(
        rejected,
        HardwareMessageBody::CapabilityStateReply { ref status, ref error, .. }
            if status == "rejected" && error.as_deref() == Some("Hardware state request stale")
    ));

    h.runtime.set_device(descriptor("deck")).await.unwrap();
    create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    wait_for_capacity(&h, "deck", 1).await;
    h.command_handler
        .push_outcome(HardwareCommandOutcome::Unsupported)
        .await;
    h.lane
        .publish_inbound(capability_state_request("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;
    let published = wait_for_published(&h, 2).await;
    let rejected = published.last().unwrap().hardware_body().unwrap();
    assert!(matches!(
        rejected,
        HardwareMessageBody::CapabilityStateReply { ref status, ref error, .. }
            if status == "unsupported"
                && error.as_deref() == Some("Hardware state request unsupported")
    ));

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn routes_only_claimed_control_input_and_capability_state() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;

    h.runtime
        .handle_hardware_message(control_input("deck"))
        .await
        .unwrap();
    h.runtime
        .handle_hardware_message(HardwareMessageBody::CommandAccepted {
            device_ref: DeviceRef {
                manager_id: "manager-main".to_string(),
                device_id: "deck".to_string(),
                fingerprint: None,
            },
            control_id: Some("screen".to_string()),
            capability_id: "raster.bitmap".to_string(),
            command_type: "clear".to_string(),
            accepted_at: None,
        })
        .await
        .unwrap();

    let published = h.lane.published().await;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].message_type, "controlInput");
    assert_eq!(published[0].recipient_endpoint(), Some("controller:main"));

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn cancelled_claim_resets_device_and_releases_capacity() {
    let h = harness_with_policy(StateMaintenancePolicy {
        renewal_interval: Duration::from_secs(3600),
        concord_token_refresh_interval: Duration::from_secs(3600),
        reconcile_interval: Duration::from_millis(20),
    })
    .await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let contract = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;

    h.concord
        .cancel(
            &contract,
            &EndpointAddress::parse("controller:main").unwrap(),
            Some("done".to_string()),
        )
        .await
        .unwrap();
    materialize().await;

    wait_for_reset_devices(&h, &["deck"]).await;
    wait_for_capacity(&h, "deck", 0).await;
    let payload = hardware_payload(&h.beacon_state).await;
    let deck = payload.devices.get("deck").unwrap();
    assert_eq!(deck.capacity.claimed_instances, 0);
    assert_eq!(deck.capacity.available_instances, Some(1));

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn removed_device_cancels_live_claim() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let contract = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;

    h.runtime
        .remove_device("deck", "disconnected")
        .await
        .unwrap();
    let record = h.concord.contract_record(&contract).await.unwrap().unwrap();
    assert_eq!(record.state, ContractState::Cancelled);
    assert_eq!(
        record.cancel_reason.as_deref(),
        Some("hardware device deck disconnected")
    );
    let manager = EndpointAddress::parse("hardware_manager:manager-main").unwrap();
    assert!(h
        .concord
        .participant_token(&contract, &manager)
        .await
        .unwrap()
        .is_none());
    let payload = hardware_payload(&h.beacon_state).await;
    assert!(!payload.devices.contains_key("deck"));

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn replaced_device_cancels_live_claim_and_releases_capacity() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let contract = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;

    h.runtime
        .set_device(descriptor_with_fingerprint(
            "deck",
            "fingerprint:replacement",
        ))
        .await
        .unwrap();

    let record = h.concord.contract_record(&contract).await.unwrap().unwrap();
    assert_eq!(record.state, ContractState::Cancelled);
    assert_eq!(
        record.cancel_reason.as_deref(),
        Some("hardware device deck replaced")
    );
    let manager = EndpointAddress::parse("hardware_manager:manager-main").unwrap();
    assert!(h
        .concord
        .participant_token(&contract, &manager)
        .await
        .unwrap()
        .is_none());
    wait_for_reset_devices(&h, &["deck"]).await;
    wait_for_capacity(&h, "deck", 0).await;
    let payload = hardware_payload(&h.beacon_state).await;
    let deck = payload.devices.get("deck").unwrap();
    assert_eq!(deck.capacity.claimed_instances, 0);
    assert_eq!(deck.capacity.available_instances, Some(1));

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn competing_claims_choose_existing_or_lowest_key() {
    let h = harness().await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let existing = create_claim(&h.concord, "claim-b", "b", "session-b").await;
    let tasks = start_runtime(&h).await;
    h.lane.publish_inbound(control_command("b", "session-b"));
    wait_for_handler_messages(&h, 1).await;
    let lower = create_claim(&h.concord, "claim-a", "a", "session-a").await;
    h.lane.publish_inbound(control_command("a", "session-a"));
    wait_for_published(&h, 1).await;

    assert_eq!(
        h.concord.validate(&existing, None).await.status,
        ContractValidityStatus::Valid
    );
    assert_eq!(
        h.concord.validate(&lower, None).await.status,
        ContractValidityStatus::NotYetFulfilled
    );
    stop_runtime(&h, tasks).await;

    let h = harness().await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let lower = create_claim(&h.concord, "claim-a", "a", "session-a").await;
    let higher = create_claim(&h.concord, "claim-b", "b", "session-b").await;
    let tasks = start_runtime(&h).await;
    h.lane.publish_inbound(control_command("a", "session-a"));
    wait_for_handler_messages(&h, 1).await;
    h.lane.publish_inbound(control_command("b", "session-b"));
    wait_for_published(&h, 1).await;
    assert_eq!(
        h.concord.validate(&lower, None).await.status,
        ContractValidityStatus::Valid
    );
    assert_eq!(
        h.concord.validate(&higher, None).await.status,
        ContractValidityStatus::NotYetFulfilled
    );
    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn watch_notifications_admit_a_waiting_claim_when_capacity_is_released() {
    let h = harness().await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let claim_a = create_claim(&h.concord, "claim-a", "a", "session-a").await;
    let claim_b = create_claim(&h.concord, "claim-b", "b", "session-b").await;
    let tasks = start_runtime(&h).await;
    h.lane.publish_inbound(control_command("a", "session-a"));
    wait_for_handler_messages(&h, 1).await;

    let manager = EndpointAddress::parse("hardware_manager:manager-main").unwrap();
    assert!(h
        .concord
        .participant_token(&claim_a, &manager)
        .await
        .unwrap()
        .is_some());
    assert!(h
        .concord
        .participant_token(&claim_b, &manager)
        .await
        .unwrap()
        .is_none());

    h.concord
        .cancel(
            &claim_a,
            &EndpointAddress::parse("controller:a").unwrap(),
            Some("done".to_string()),
        )
        .await
        .unwrap();
    wait_for_reset_devices(&h, &["deck"]).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.concord.validate(&claim_b, None).await.status != ContractValidityStatus::Valid {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let manager_token = h
        .concord
        .participant_token(&claim_b, &manager)
        .await
        .unwrap()
        .unwrap();

    let controller_b = EndpointAddress::parse("controller:b").unwrap();
    let controller_b_token = h
        .concord
        .participant_token(&claim_b, &controller_b)
        .await
        .unwrap()
        .unwrap();
    h.concord.refresh(&controller_b_token).await.unwrap();
    materialize().await;

    assert_eq!(
        h.concord
            .participant_token(&claim_b, &manager)
            .await
            .unwrap()
            .unwrap()
            .token_id,
        manager_token.token_id
    );
    wait_for_capacity(&h, "deck", 1).await;
    let payload = hardware_payload(&h.beacon_state).await;
    let deck = payload.devices.get("deck").unwrap();
    assert_eq!(deck.capacity.claimed_instances, 1);
    assert_eq!(deck.capacity.available_instances, Some(0));

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn lost_manager_token_drops_route_without_resurrecting_authority() {
    let h = harness_with_policy(StateMaintenancePolicy {
        renewal_interval: Duration::from_secs(3600),
        concord_token_refresh_interval: Duration::from_secs(3600),
        reconcile_interval: Duration::from_millis(20),
    })
    .await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let contract = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;

    let manager = EndpointAddress::parse("hardware_manager:manager-main").unwrap();
    h.token_state
        .delete(
            &concord_participant_token_key(&contract.contract_id, contract.generation, &manager),
            None,
        )
        .await
        .unwrap();
    materialize().await;
    wait_for_reset_devices(&h, &["deck"]).await;
    let before = h.lane.published().await.len();
    h.runtime
        .handle_hardware_message(control_input("deck"))
        .await
        .unwrap();
    assert_eq!(h.lane.published().await.len(), before);
    assert_eq!(h.reset_handler.devices().await, vec!["deck"]);

    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn start_routes_subscribed_commands_and_stop_withdraws_beacon_and_claims() {
    let h = harness().await;
    let mut tasks = start_runtime(&h).await;

    let beacon = Beacon::start(h.beacon_state.clone()).await.unwrap();
    assert_eq!(beacon.candidates(HARDWARE_FEATURE_ID).unwrap().len(), 1);

    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let contract = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_until(|| {
        h.command_handler
            .messages
            .try_lock()
            .is_ok_and(|messages| messages.len() == 1)
    })
    .await;
    let manager = EndpointAddress::parse("hardware_manager:manager-main").unwrap();
    assert!(h
        .concord
        .participant_token(&contract, &manager)
        .await
        .unwrap()
        .is_some());

    h.runtime.stop().await.unwrap();
    let beacon = Beacon::start(h.beacon_state.clone()).await.unwrap();
    assert!(beacon.candidates(HARDWARE_FEATURE_ID).unwrap().is_empty());
    assert!(h
        .concord
        .participant_token(&contract, &manager)
        .await
        .unwrap()
        .is_none());
    let record = h.concord.contract_record(&contract).await.unwrap().unwrap();
    assert_eq!(record.state, ContractState::Open);
    assert_eq!(
        h.concord.validate(&contract, None).await.status,
        ContractValidityStatus::MissingToken
    );

    assert!(h
        .lane
        .inbound_tx
        .unbounded_send(Ok(Delivery {
            message: control_command("main", "controller-session"),
            reply_address: None
        }))
        .is_err());
    assert_eq!(h.command_handler.messages().await.len(), 1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .unwrap();
}

#[derive(Clone)]
struct SlowBeaconStore {
    raw: MemoryStateStore,
    block_listings: Arc<std::sync::atomic::AtomicBool>,
    listing: Arc<tokio::sync::Notify>,
    block_updates: Arc<std::sync::atomic::AtomicBool>,
    block_deletes: Arc<std::sync::atomic::AtomicBool>,
    updating: Arc<tokio::sync::Notify>,
    deleting: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
    lane_subscribed: Arc<std::sync::atomic::AtomicBool>,
    premature_publication: Arc<std::sync::atomic::AtomicBool>,
}
impl SlowBeaconStore {
    fn new(lane: &MemoryHardwareLane) -> Self {
        Self {
            raw: MemoryStateStore::ttl_bound(30).unwrap(),
            block_listings: Arc::default(),
            listing: Arc::default(),
            block_updates: Arc::default(),
            block_deletes: Arc::default(),
            updating: Arc::default(),
            deleting: Arc::default(),
            resume: Arc::default(),
            lane_subscribed: lane.subscribed.clone(),
            premature_publication: Arc::default(),
        }
    }
    async fn until_resumed(&self, blocked: &std::sync::atomic::AtomicBool) {
        loop {
            let notified = self.resume.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !blocked.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}
impl StateStore for SlowBeaconStore {
    async fn ttl_seconds(&self) -> Result<Option<u64>> {
        self.raw.ttl_seconds().await
    }
    async fn get(&self, key: &str) -> Result<Option<deckr::state::StateEntry>> {
        self.raw.get(key).await
    }
    async fn items(&self, prefix: &str) -> Result<Vec<deckr::state::StateEntry>> {
        if self
            .block_listings
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.listing.notify_one();
        }
        self.until_resumed(&self.block_listings).await;
        self.raw.items(prefix).await
    }
    async fn put(
        &self,
        key: &str,
        value: serde_json::Value,
        ttl: Option<u64>,
    ) -> Result<deckr::state::StateEntry> {
        self.raw.put(key, value, ttl).await
    }
    async fn create(
        &self,
        key: &str,
        value: serde_json::Value,
        ttl: Option<u64>,
    ) -> Result<deckr::state::StateEntry> {
        if !self
            .lane_subscribed
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.premature_publication
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        self.raw.create(key, value, ttl).await
    }
    async fn update(
        &self,
        key: &str,
        value: serde_json::Value,
        revision: u64,
        ttl: Option<u64>,
    ) -> Result<deckr::state::StateEntry> {
        if self.block_updates.load(std::sync::atomic::Ordering::SeqCst) {
            self.updating.notify_one();
        }
        self.until_resumed(&self.block_updates).await;
        self.raw.update(key, value, revision, ttl).await
    }
    async fn delete(&self, key: &str, revision: Option<u64>) -> Result<()> {
        if self.block_deletes.load(std::sync::atomic::Ordering::SeqCst) {
            self.deleting.notify_one();
        }
        self.until_resumed(&self.block_deletes).await;
        self.raw.delete(key, revision).await
    }
    async fn watch(&self, prefix: &str) -> Result<deckr::state::StateWatchStream> {
        self.raw.watch(prefix).await
    }
    async fn open_source(&self, prefix: &str) -> Result<deckr::state::StateSource> {
        self.raw.open_source(prefix).await
    }
}

#[tokio::test]
async fn blocked_discovery_does_not_hold_authority_locks_and_close_is_shared() {
    use std::sync::atomic::Ordering;
    let contracts = MemoryStateStore::new();
    let tokens = MemoryStateStore::ttl_bound(30).unwrap();
    let concord = ConcordCoordinator::new(contracts.clone(), tokens.clone());
    let lane = MemoryHardwareLane::default();
    let beacon = SlowBeaconStore::new(&lane);
    let handler = Arc::new(RecordingCommandHandler::default());
    let runtime = HardwareManagerRuntime::new(HardwareManagerRuntimeSpec {
        manager_id: "manager-main".into(),
        session_id: "manager-session".into(),
        labels: BTreeMap::new(),
        beacon_state: beacon.clone(),
        concord_contract_state: contracts,
        concord_token_state: tokens,
        lane: lane.clone(),
        maintenance_policy: StateMaintenancePolicy::default(),
        command_handler: handler.clone(),
        reset_handler: None,
    })
    .await
    .unwrap();
    runtime.set_device(descriptor("deck")).await.unwrap();
    assert!(
        beacon
            .raw
            .items("advertisements.")
            .await
            .unwrap()
            .is_empty(),
        "inventory configuration must not publish before startup"
    );
    let mut tasks = JoinSet::new();
    runtime.start(&mut tasks).await.unwrap();
    assert!(!beacon.premature_publication.load(Ordering::SeqCst));
    let claim = create_claim(&concord, "claim-a", "main", "controller-session").await;
    lane.publish_inbound(control_command("main", "controller-session"));
    tokio::time::timeout(Duration::from_secs(1), async {
        while handler.messages().await.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    beacon.block_updates.store(true, Ordering::SeqCst);
    let inventory = runtime.clone();
    let update = tokio::spawn(async move { inventory.set_device(descriptor("spare")).await });
    tokio::time::timeout(Duration::from_secs(1), beacon.updating.notified())
        .await
        .unwrap();
    lane.publish_inbound(control_command("main", "controller-session"));
    tokio::time::timeout(Duration::from_secs(1), async {
        while handler.messages().await.len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(concord.validate(&claim, None).await.valid());
    assert!(!update.is_finished());
    beacon.block_updates.store(false, Ordering::SeqCst);
    beacon.resume.notify_waiters();
    update.await.unwrap().unwrap();
    beacon.block_deletes.store(true, Ordering::SeqCst);
    let first = runtime.clone();
    let closing = tokio::spawn(async move { first.stop().await });
    tokio::time::timeout(Duration::from_secs(1), beacon.deleting.notified())
        .await
        .unwrap();
    closing.abort();
    let second = runtime.clone();
    let waiting = tokio::spawn(async move { second.stop().await });
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(
        !waiting.is_finished(),
        "a second close must join the ongoing cleanup"
    );
    beacon.block_deletes.store(false, Ordering::SeqCst);
    beacon.resume.notify_waiters();
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(beacon
        .raw
        .items("advertisements.")
        .await
        .unwrap()
        .is_empty());
    while tasks.join_next().await.is_some() {}
}

#[tokio::test]
async fn bootstrap_preserves_a_competing_live_manager_and_its_advertisement() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let claim = create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;
    let advertisements = h.beacon_state.items("advertisements.").await.unwrap();
    let contender = HardwareManagerRuntime::new(HardwareManagerRuntimeSpec {
        manager_id: "manager-main".into(),
        session_id: "competing-session".into(),
        labels: BTreeMap::new(),
        beacon_state: h.beacon_state.clone(),
        concord_contract_state: h.contract_state.clone(),
        concord_token_state: h.token_state.clone(),
        lane: MemoryHardwareLane::default(),
        maintenance_policy: StateMaintenancePolicy::default(),
        command_handler: Arc::new(RecordingCommandHandler::default()),
        reset_handler: None,
    })
    .await
    .unwrap();
    contender.set_device(descriptor("deck")).await.unwrap();
    let mut contender_tasks = JoinSet::new();
    assert!(contender.start(&mut contender_tasks).await.is_err());
    assert!(h.concord.validate(&claim, None).await.valid());
    assert_eq!(
        h.beacon_state.items("advertisements.").await.unwrap(),
        advertisements
    );
    contender.stop().await.unwrap();
    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn stopping_during_startup_cannot_publish_or_spawn_late_workers() {
    use std::sync::atomic::Ordering;
    let lane = MemoryHardwareLane::default();
    let beacon = SlowBeaconStore::new(&lane);
    beacon.block_listings.store(true, Ordering::SeqCst);
    let contracts = MemoryStateStore::new();
    let tokens = MemoryStateStore::ttl_bound(30).unwrap();
    let sources = ConcordCoordinator::new(
        deckr::state::MaterializedStateStore::start(contracts.clone(), "contracts.").await.unwrap(),
        deckr::state::MaterializedStateStore::start(tokens.clone(), "contracts.").await.unwrap(),
    ).with_restart(Some(deckr::restart::VerifiedRestart::from_handoff(serde_json::json!({
        "schema":"dev.deckr.runtime.restart_handoff.v1", "instanceId":"hardware",
        "predecessors":[{"predecessorId":"container:old", "exitCode":137,
          "participants":[{"endpoint":"hardware_manager:manager-main", "sessionId":"old-manager"}]}]
    }), "hardware").unwrap()));
    let runtime = HardwareManagerRuntime::with_concord(
        HardwareManagerRuntimeSpec {
            manager_id: "manager-main".into(),
            session_id: "manager-session".into(),
            labels: BTreeMap::new(),
            beacon_state: beacon.clone(),
            concord_contract_state: contracts,
            concord_token_state: tokens,
            lane,
            maintenance_policy: StateMaintenancePolicy::default(),
            command_handler: Arc::new(RecordingCommandHandler::default()),
            reset_handler: None,
        },
        sources,
    )
    .unwrap();
    runtime.set_device(descriptor("deck")).await.unwrap();
    let starting = runtime.clone();
    let started = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        let result = starting.start(&mut tasks).await;
        (result, tasks)
    });
    tokio::time::timeout(Duration::from_secs(1), beacon.listing.notified())
        .await
        .unwrap();
    runtime.stop().await.unwrap();
    beacon.block_listings.store(false, Ordering::SeqCst);
    beacon.resume.notify_waiters();
    let (result, tasks) = tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert!(tasks.is_empty());
    assert!(beacon
        .raw
        .items("advertisements.")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn cancelled_start_cleans_up_without_an_explicit_stop_call() {
    use std::sync::atomic::Ordering;
    let lane = MemoryHardwareLane::default();
    let beacon = SlowBeaconStore::new(&lane);
    beacon.block_listings.store(true, Ordering::SeqCst);
    let contracts = MemoryStateStore::new();
    let tokens = MemoryStateStore::ttl_bound(30).unwrap();
    let sources = ConcordCoordinator::new(
        deckr::state::MaterializedStateStore::start(contracts.clone(), "contracts.").await.unwrap(),
        deckr::state::MaterializedStateStore::start(tokens.clone(), "contracts.").await.unwrap(),
    ).with_restart(Some(deckr::restart::VerifiedRestart::from_handoff(serde_json::json!({
        "schema":"dev.deckr.runtime.restart_handoff.v1", "instanceId":"hardware",
        "predecessors":[{"predecessorId":"container:old", "exitCode":137,
          "participants":[{"endpoint":"hardware_manager:manager-main", "sessionId":"old-manager"}]}]
    }), "hardware").unwrap()));
    let runtime = HardwareManagerRuntime::with_concord(
        HardwareManagerRuntimeSpec {
            manager_id: "manager-main".into(),
            session_id: "manager-session".into(),
            labels: BTreeMap::new(),
            beacon_state: beacon.clone(),
            concord_contract_state: contracts,
            concord_token_state: tokens,
            lane,
            maintenance_policy: StateMaintenancePolicy::default(),
            command_handler: Arc::new(RecordingCommandHandler::default()),
            reset_handler: None,
        },
        sources,
    )
    .unwrap();
    let starting = runtime.clone();
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        starting.start(&mut tasks).await
    });
    tokio::time::timeout(Duration::from_secs(1), beacon.listing.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    runtime
        .wait_for(
            |state| state.phase == Phase::Closed,
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    assert!(beacon
        .raw
        .items("advertisements.")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn descriptor_checks_precede_driver_dispatch_and_contexts_are_fenced() {
    let h = harness().await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let tasks = start_runtime(&h).await;
    create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    for (field, value, expected) in [
        ("capabilityId", serde_json::json!("missing"), "unsupported"),
        (
            "commandType",
            serde_json::json!("unsupported_command"),
            "unsupported",
        ),
        ("params", serde_json::json!({"unexpected":true}), "rejected"),
        (
            "deviceRef",
            serde_json::json!({"managerId":"manager-main", "deviceId":"deck", "fingerprint":"wrong"}),
            "stale",
        ),
    ] {
        let count = h.lane.published().await.len();
        let mut message = control_command("main", "controller-session");
        message.body[field] = value;
        h.lane.publish_inbound(message);
        wait_for_published(&h, count + 1).await;
        assert_eq!(
            h.lane.published().await.last().unwrap().body["reason"],
            expected
        );
        assert!(h.command_handler.messages().await.is_empty());
    }
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;
    let context = h.command_handler.contexts.lock().await[0].clone();
    assert_eq!(context.device().device_id, "deck");
    context.authorize().unwrap();
    let mut invalid_input = control_input("deck");
    if let HardwareMessageBody::ControlInput { value, .. } = &mut invalid_input {
        *value = Some(serde_json::json!({"eventType":"down"}));
    }
    assert!(h
        .runtime
        .handle_hardware_message(invalid_input)
        .await
        .is_err());
    let mut replacement = descriptor("deck");
    replacement.firmware_version = Some("changed".into());
    h.runtime.set_device(replacement).await.unwrap();
    assert!(context.authorize().is_err());
    assert_eq!(context.device().firmware_version, None);
    stop_runtime(&h, tasks).await;
}

fn response_for(request: &DeckrMessage) -> HardwareMessageBody {
    match request.hardware_body().unwrap() {
        HardwareMessageBody::ControlCommand {
            device_ref,
            control_id,
            capability_id,
            command_type,
            ..
        } => HardwareMessageBody::CommandReply {
            device_ref,
            control_id,
            capability_id,
            command_type,
            result: Some(serde_json::json!({"done":true})),
        },
        HardwareMessageBody::CapabilityStateRequest {
            device_ref,
            control_id,
            capability_id,
            state_type,
            ..
        } => HardwareMessageBody::CapabilityStateReply {
            device_ref,
            control_id,
            capability_id,
            state_type,
            status: "ok".into(),
            value: Some(serde_json::json!(true)),
            error: None,
        },
        _ => panic!("request expected"),
    }
}

fn assert_correlated(request: &DeckrMessage, reply: &DeckrMessage) {
    assert_ne!(reply.message_id, request.message_id);
    assert_eq!(
        reply.in_reply_to.as_deref(),
        Some(request.message_id.as_str())
    );
    assert_eq!(reply.contract, request.contract);
    assert_eq!(reply.recipient_endpoint(), Some(request.sender.as_str()));
    assert_eq!(
        reply.recipient_session_id.as_deref(),
        Some(request.sender_session_id.as_str())
    );
    assert_eq!(reply.sender, "hardware_manager:manager-main");
    assert_eq!(reply.sender_session_id, "manager-session");
}

#[tokio::test]
async fn every_managed_rejection_preserves_inbox_route_and_request_correlation() {
    let h = harness().await;
    let tasks = start_runtime(&h).await;
    let request = control_command("main", "controller-session");
    h.lane
        .publish_request(request.clone(), Some("_INBOX.stale".into()));
    assert_correlated(&request, &wait_for_published(&h, 1).await[0]);

    h.runtime.set_device(descriptor("deck")).await.unwrap();
    create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    let mut invalid = control_command("main", "controller-session");
    invalid.body["params"] = serde_json::json!({"invalid":true});
    h.lane
        .publish_request(invalid.clone(), Some("_INBOX.invalid".into()));
    assert_correlated(&invalid, &wait_for_published(&h, 2).await[1]);

    h.command_handler
        .push_outcome(HardwareCommandOutcome::Unsupported)
        .await;
    let unsupported = capability_state_request("main", "controller-session");
    h.lane
        .publish_request(unsupported.clone(), Some("_INBOX.unsupported".into()));
    assert_correlated(&unsupported, &wait_for_published(&h, 3).await[2]);
    assert_eq!(
        *h.lane.reply_routes.lock().await,
        vec![
            Some("_INBOX.stale".into()),
            Some("_INBOX.invalid".into()),
            Some("_INBOX.unsupported".into())
        ]
    );
    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn scoped_driver_replies_validate_operation_and_lose_authority_with_device_changes() {
    let h = harness().await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let tasks = start_runtime(&h).await;
    create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    let request = control_command("main", "controller-session");
    h.lane
        .publish_request(request.clone(), Some("_INBOX.reply".into()));
    wait_for_handler_messages(&h, 1).await;
    let context = h.command_handler.contexts.lock().await[0].clone();
    let response = response_for(&request);
    for case in 0..3 {
        let mut invalid = response.clone();
        if let HardwareMessageBody::CommandReply {
            device_ref,
            capability_id,
            command_type,
            ..
        } = &mut invalid
        {
            match case {
                0 => device_ref.device_id = "different-device".into(),
                1 => *capability_id = "different-capability".into(),
                _ => *command_type = "set_frame".into(),
            }
        }
        assert!(context.reply(invalid).await.is_err());
    }
    assert!(context.reply(control_input("deck")).await.is_err());
    assert!(h.lane.published().await.is_empty());
    context.reply(response.clone()).await.unwrap();
    assert_correlated(&request, &h.lane.published().await[0]);
    assert_eq!(
        h.lane.reply_routes.lock().await[0].as_deref(),
        Some("_INBOX.reply")
    );
    let mut replacement = descriptor("deck");
    replacement.firmware_version = Some("replacement".into());
    h.runtime.set_device(replacement).await.unwrap();
    assert!(context.reply(response).await.is_err());
    assert_eq!(h.lane.published().await.len(), 1);
    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn state_replies_enforce_descriptor_schema_and_use_the_managed_sender_session() {
    let h = harness().await;
    let mut device = descriptor("deck");
    device.controls[0].output_capabilities[0].value_schema = Some(deckr::lanes::CapabilitySchema {
        schema_id: None,
        schema: serde_json::json!({"type":"boolean"}),
    });
    h.runtime.set_device(device).await.unwrap();
    let tasks = start_runtime(&h).await;
    create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    let mut request = capability_state_request("main", "controller-session");
    request.recipient_session_id = None;
    h.lane.publish_inbound(request.clone());
    wait_for_handler_messages(&h, 1).await;
    let context = h.command_handler.contexts.lock().await[0].clone();
    let mut invalid = response_for(&request);
    if let HardwareMessageBody::CapabilityStateReply { value, .. } = &mut invalid {
        *value = Some(serde_json::json!("not a boolean"));
    }
    assert!(context.reply(invalid).await.is_err());
    let mut wrong_operation = response_for(&request);
    if let HardwareMessageBody::CapabilityStateReply { state_type, .. } = &mut wrong_operation {
        *state_type = Some("different-state".into());
    }
    assert!(context.reply(wrong_operation).await.is_err());
    context.reply(response_for(&request)).await.unwrap();
    assert_correlated(&request, &h.lane.published().await[0]);
    assert_eq!(*h.lane.reply_routes.lock().await, vec![None]);
    stop_runtime(&h, tasks).await;
    assert!(context.reply(response_for(&request)).await.is_err());
    assert_eq!(h.lane.published().await.len(), 1);
}

#[tokio::test]
async fn expired_or_wrong_session_requests_are_not_dispatched_or_answered() {
    let h = harness().await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let tasks = start_runtime(&h).await;
    create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    let mut expired = control_command("main", "controller-session");
    expired.ttl_ms = Some(0);
    h.lane
        .publish_request(expired, Some("_INBOX.expired".into()));
    let mut previous = control_command("main", "controller-session");
    previous.recipient_session_id = Some("previous-manager-session".into());
    h.lane
        .publish_request(previous, Some("_INBOX.previous".into()));
    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_handler_messages(&h, 1).await;
    assert!(h.lane.published().await.is_empty());
    assert!(h.lane.reply_routes.lock().await.is_empty());
    stop_runtime(&h, tasks).await;
}

#[tokio::test]
async fn managed_request_completes_through_the_hardware_driver_reply_path() {
    use deckr::messaging::{MessageSession, MessageTransport, ReplyInbox};
    use deckr::session::EndpointSession;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[derive(Clone)]
    struct ControllerTransport(MemoryHardwareLane);
    struct Inbox {
        lane: MemoryHardwareLane,
        address: String,
        receiver: UnboundedReceiver<Result<Delivery>>,
    }
    impl Drop for Inbox {
        fn drop(&mut self) {
            self.lane.inboxes.lock().unwrap().remove(&self.address);
        }
    }
    impl futures_core::Stream for Inbox {
        type Item = Result<Delivery>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.receiver).poll_next(cx)
        }
    }
    impl MessageTransport for ControllerTransport {
        async fn subscribe(&self, _: &str, _: &EndpointAddress) -> Result<DeliveryStream> {
            Err(Error::Unsupported("request-only test transport".into()))
        }
        async fn open_inbox(&self) -> Result<ReplyInbox> {
            let address = format!("_INBOX.{}", uuid::Uuid::new_v4().simple());
            let (sender, receiver) = unbounded();
            self.0
                .inboxes
                .lock()
                .unwrap()
                .insert(address.clone(), sender);
            Ok(ReplyInbox {
                address: address.clone(),
                messages: Box::pin(Inbox {
                    lane: self.0.clone(),
                    address,
                    receiver,
                }),
            })
        }
        async fn publish(
            &self,
            message: &DeckrMessage,
            _: Option<&str>,
            reply_address: Option<&str>,
        ) -> Result<()> {
            self.0
                .publish_request(message.clone(), reply_address.map(str::to_owned));
            Ok(())
        }
    }
    let h = harness().await;
    h.runtime.set_device(descriptor("deck")).await.unwrap();
    let tasks = start_runtime(&h).await;
    create_claim(&h.concord, "claim-a", "main", "controller-session").await;
    let messages = MessageSession::new(
        ControllerTransport(h.lane.clone()),
        EndpointSession::with_id(
            EndpointAddress::parse("controller:main").unwrap(),
            "controller-session",
        )
        .unwrap(),
    )
    .unwrap();
    let sender = messages.clone();
    let request = control_command("main", "controller-session");
    let outgoing = request.clone();
    let request_task =
        tokio::spawn(async move { sender.request(outgoing, Duration::from_secs(2), None).await });
    wait_for_handler_messages(&h, 1).await;
    let context = h.command_handler.contexts.lock().await[0].clone();
    context.reply(response_for(&request)).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(2), request_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_correlated(&request, &reply);
    assert_eq!(reply.message_type, "commandReply");
    assert!(h.lane.inboxes.lock().unwrap().is_empty());
    assert_eq!(messages.state().value.pending_requests, 0);
    messages.close().await.unwrap();
    stop_runtime(&h, tasks).await;
}
