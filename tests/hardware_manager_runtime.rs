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
    HardwareCommandFuture, HardwareCommandHandler, HardwareCommandOutcome, HardwareLaneTransport,
    HardwareManagerRuntime, HardwareManagerRuntimeSpec, HardwareMessageStream, HardwareResetFuture,
    HardwareResetHandler,
};
use deckr::lanes::{DeckrMessage, DeviceDescriptor, DeviceRef, HardwareMessageBody};
use deckr::profiles::hardware::{
    hardware_payload_from_advertisement, HardwareClaimDevice, HardwareClaimTerms,
    HARDWARE_CLAIM_PROFILE_ID, HARDWARE_FEATURE_ID,
};
use deckr::state::{MemoryStateStore, StateMaintenancePolicy, StateStore};
use deckr::{Error, Result};
use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

type TestRuntime = HardwareManagerRuntime<
    MemoryStateStore,
    MemoryStateStore,
    MemoryStateStore,
    MemoryHardwareLane,
>;

#[derive(Clone)]
struct MemoryHardwareLane {
    published: Arc<Mutex<Vec<DeckrMessage>>>,
    inbound_tx: UnboundedSender<Result<DeckrMessage>>,
    inbound_rx: Arc<Mutex<Option<UnboundedReceiver<Result<DeckrMessage>>>>>,
}

impl Default for MemoryHardwareLane {
    fn default() -> Self {
        let (inbound_tx, inbound_rx) = unbounded();
        Self {
            published: Arc::default(),
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
        self.inbound_tx
            .unbounded_send(Ok(message))
            .expect("test inbound hardware lane should be open");
    }
}

impl HardwareLaneTransport for MemoryHardwareLane {
    async fn publish_hardware_message(&self, message: DeckrMessage) -> Result<()> {
        self.published.lock().await.push(message);
        Ok(())
    }

    async fn subscribe_hardware_messages<'a>(
        &'a self,
        _endpoint: &'a EndpointAddress,
    ) -> Result<HardwareMessageStream> {
        let stream =
            self.inbound_rx.lock().await.take().ok_or_else(|| {
                Error::Invalid("test hardware lane already subscribed".to_string())
            })?;
        let stream: HardwareMessageStream = Box::pin(stream);
        Ok(stream)
    }
}

#[derive(Default)]
struct RecordingCommandHandler {
    outcomes: Mutex<VecDeque<HardwareCommandOutcome>>,
    messages: Mutex<Vec<DeckrMessage>>,
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
    fn handle_hardware_command<'a>(&'a self, message: DeckrMessage) -> HardwareCommandFuture<'a> {
        Box::pin(async move {
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
        concord_contract_state: contract_state,
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
    DeviceDescriptor {
        device_id: device_id.to_string(),
        fingerprint: fingerprint.to_string(),
        display_name: "Test Device".to_string(),
        manufacturer: None,
        model: None,
        serial_number: None,
        controls: Vec::new(),
        capabilities: Vec::new(),
    }
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
        value: None,
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
            instance_count: 1,
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
async fn startup_removes_stale_hardware_beacon_for_same_manager_endpoint() {
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

    assert!(h.beacon_state.get(&stale.key).await.unwrap().is_none());
    let beacon = Beacon::start(h.beacon_state.clone()).await.unwrap();
    let candidates = beacon.candidates(HARDWARE_FEATURE_ID).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_ne!(
        candidates[0].advertisement.advertisement_id,
        "stale-hardware-ad"
    );

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
async fn token_notifications_do_not_trigger_broad_claim_discovery() {
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
    assert!(h
        .concord
        .participant_token(&claim_b, &manager)
        .await
        .unwrap()
        .is_none());

    let controller_b = EndpointAddress::parse("controller:b").unwrap();
    let controller_b_token = h
        .concord
        .participant_token(&claim_b, &controller_b)
        .await
        .unwrap()
        .unwrap();
    h.concord.refresh(&controller_b_token).await.unwrap();
    materialize().await;

    assert!(h
        .concord
        .participant_token(&claim_b, &manager)
        .await
        .unwrap()
        .is_none());
    let payload = hardware_payload(&h.beacon_state).await;
    let deck = payload.devices.get("deck").unwrap();
    assert_eq!(deck.capacity.claimed_instances, 0);
    assert_eq!(deck.capacity.available_instances, Some(1));

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

    h.lane
        .publish_inbound(control_command("main", "controller-session"));
    wait_for_published(&h, 1).await;
    assert_eq!(h.command_handler.messages().await.len(), 1);
    let published = h.lane.published().await;
    assert!(matches!(
        published.last().unwrap().hardware_body().unwrap(),
        HardwareMessageBody::CommandRejected { ref reason, .. } if reason == "unauthorized"
    ));

    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}
