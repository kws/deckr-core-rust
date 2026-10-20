use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use deckr::endpoint::EndpointAddress;
use deckr::hardware::client::{
    HardwareCandidate, HardwareClient, HardwareClientOptions, HardwareClientSpec, HardwareSelection,
};
use deckr::hardware::runtime::{
    HardwareCommandContext, HardwareCommandFuture, HardwareCommandHandler, HardwareCommandOutcome,
    HardwareManagerRuntime, HardwareManagerRuntimeSpec, HardwareTransport,
};
use deckr::lanes::{DeckrMessage, DeviceDescriptor, DeviceRef, HardwareMessageBody};
use deckr::lifecycle::{Clock, Phase, SystemClock};
use deckr::messaging::{Delivery, DeliveryStream, MessageSession, MessageTransport, ReplyInbox};
use deckr::runtime::ProtocolRuntime;
use deckr::schema::SchemaRegistry;
use deckr::session::EndpointSession;
use deckr::state::{MemoryStateStore, StateMaintenancePolicy, StateStore};
use deckr::{Error, Result};
use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_core::Stream;
use serde_json::json;
use tokio::task::JoinSet;

mod support;

#[derive(Clone, Default)]
struct Bus(Arc<Mutex<BusState>>);
#[derive(Default)]
struct BusState {
    routes: BTreeMap<String, (Route, UnboundedSender<Result<Delivery>>)>,
    published: Vec<(DeckrMessage, Option<String>)>,
}
enum Route {
    Lane(String, EndpointAddress),
    Inbox,
}
struct Subscription {
    bus: Bus,
    key: String,
    receiver: UnboundedReceiver<Result<Delivery>>,
}
impl Drop for Subscription {
    fn drop(&mut self) {
        self.bus.0.lock().unwrap().routes.remove(&self.key);
    }
}
impl Stream for Subscription {
    type Item = Result<Delivery>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(cx)
    }
}
impl Bus {
    fn register(&self, key: String, route: Route) -> DeliveryStream {
        let (sender, receiver) = unbounded();
        self.0
            .lock()
            .unwrap()
            .routes
            .insert(key.clone(), (route, sender));
        Box::pin(Subscription {
            bus: self.clone(),
            key,
            receiver,
        })
    }
    fn routes(&self) -> usize {
        self.0.lock().unwrap().routes.len()
    }
}
impl MessageTransport for Bus {
    async fn subscribe(&self, lane: &str, endpoint: &EndpointAddress) -> Result<DeliveryStream> {
        Ok(self.register(
            uuid::Uuid::new_v4().to_string(),
            Route::Lane(lane.into(), endpoint.clone()),
        ))
    }
    async fn open_inbox(&self) -> Result<ReplyInbox> {
        let address = format!("inbox.{}", uuid::Uuid::new_v4());
        Ok(ReplyInbox {
            messages: self.register(address.clone(), Route::Inbox),
            address,
        })
    }
    async fn publish(
        &self,
        message: &DeckrMessage,
        inbox: Option<&str>,
        reply_address: Option<&str>,
    ) -> Result<()> {
        let mut bus = self.0.lock().unwrap();
        bus.published
            .push((message.clone(), reply_address.map(str::to_owned)));
        for (key, (route, sender)) in &bus.routes {
            let matches = match (inbox, route) {
                (Some(address), Route::Inbox) => address == key,
                (None, Route::Lane(lane, endpoint)) => {
                    *lane == message.lane && message.recipient.targets_endpoint(endpoint)?
                }
                _ => false,
            };
            if matches {
                let _ = sender.unbounded_send(Ok(Delivery {
                    message: message.clone(),
                    reply_address: reply_address.map(str::to_owned),
                }));
            }
        }
        Ok(())
    }
}

struct Driver {
    reply: AtomicBool,
    contexts: Mutex<Vec<HardwareCommandContext>>,
}
impl Default for Driver {
    fn default() -> Self {
        Self {
            reply: AtomicBool::new(true),
            contexts: Mutex::new(Vec::new()),
        }
    }
}
impl HardwareCommandHandler for Driver {
    fn handle_authorized_hardware_command<'a>(
        &'a self,
        context: HardwareCommandContext,
    ) -> HardwareCommandFuture<'a> {
        Box::pin(async move {
            self.contexts.lock().unwrap().push(context.clone());
            if self.reply.load(Ordering::SeqCst) {
                context.reply(response(context.message())).await?;
            }
            Ok(HardwareCommandOutcome::Handled)
        })
    }
}
fn response(message: &DeckrMessage) -> HardwareMessageBody {
    match message.hardware_body().unwrap() {
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
            result: Some(json!({"done":true})),
        },
        _ => panic!("command expected"),
    }
}
fn descriptor(id: &str, fingerprint: &str) -> DeviceDescriptor {
    serde_json::from_value(json!({
        "deviceId":id,"fingerprint":fingerprint,"displayName":"Test device",
        "controls":[
            {"controlId":"screen","kind":"screen","outputCapabilities":[{"capabilityId":"image","family":"dev.deckr.output.raster","type":"bitmap","direction":"output","access":["settable"],"commandTypes":["set_frame","clear"]}]},
            {"controlId":"button","kind":"button","inputCapabilities":[{"capabilityId":"press","family":"dev.deckr.input.button","type":"activation","direction":"input","access":["emits"],"eventTypes":["press"]}]}
        ]
    })).unwrap()
}
fn command() -> HardwareMessageBody {
    HardwareMessageBody::ControlCommand {
        device_ref: DeviceRef {
            manager_id: "manager".into(),
            device_id: "deck".into(),
            fingerprint: None,
        },
        control_id: Some("screen".into()),
        capability_id: "image".into(),
        command_type: "clear".into(),
        params: Default::default(),
    }
}
fn input() -> HardwareMessageBody {
    HardwareMessageBody::ControlInput {
        device_ref: DeviceRef {
            manager_id: "manager".into(),
            device_id: "deck".into(),
            fingerprint: None,
        },
        control_id: "button".into(),
        capability_id: "press".into(),
        event_type: "press".into(),
        value: Some(json!({"eventType":"press"})),
        occurred_at: None,
        sequence: None,
        sources: vec![],
    }
}
async fn select(candidates: Vec<HardwareCandidate>) -> Result<Option<HardwareSelection>> {
    Ok(candidates
        .first()
        .filter(|candidate| candidate.devices.contains_key("deck"))
        .map(|candidate| HardwareSelection {
            advertisement_key: candidate.advertisement_key.clone(),
            devices: BTreeMap::from([("deck".into(), 1)]),
        }))
}
type Client = HardwareClient<MemoryStateStore, MemoryStateStore, Bus>;
type Manager = HardwareManagerRuntime<
    MemoryStateStore,
    MemoryStateStore,
    MemoryStateStore,
    HardwareTransport<Bus>,
>;
struct Harness {
    client: Client,
    manager: Manager,
    protocols: ProtocolRuntime<MemoryStateStore, MemoryStateStore, MemoryStateStore>,
    advertisements: MemoryStateStore,
    messages: MessageSession<Bus>,
    session: EndpointSession,
    bus: Bus,
    driver: Arc<Driver>,
    tasks: JoinSet<Result<()>>,
}
impl Harness {
    async fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock)).await
    }
    async fn with_clock(clock: Arc<dyn Clock>) -> Self {
        let advertisements = MemoryStateStore::ttl_bound(30).unwrap();
        let contracts = MemoryStateStore::default();
        let tokens = MemoryStateStore::ttl_bound(30).unwrap();
        let protocols =
            ProtocolRuntime::start(advertisements.clone(), contracts.clone(), tokens.clone())
                .await
                .unwrap();
        let protocols = ProtocolRuntime::from_views(
            protocols.beacon().clone(),
            protocols.concord().clone().clock(clock.clone()),
        );
        let bus = Bus::default();
        let driver = Arc::new(Driver::default());
        let manager = HardwareManagerRuntime::with_concord(
            HardwareManagerRuntimeSpec {
                manager_id: "manager".into(),
                session_id: "manager-session".into(),
                labels: BTreeMap::from([("room".into(), "studio".into())]),
                beacon_state: advertisements.clone(),
                concord_contract_state: contracts,
                concord_token_state: tokens,
                lane: HardwareTransport(bus.clone()),
                maintenance_policy: StateMaintenancePolicy::default(),
                command_handler: driver.clone(),
                reset_handler: None,
            },
            protocols.concord().clone(),
        )
        .unwrap();
        manager
            .set_device(descriptor("deck", "first"))
            .await
            .unwrap();
        let mut tasks = JoinSet::new();
        manager.start(&mut tasks).await.unwrap();
        let session = EndpointSession::with_id(
            EndpointAddress::parse("controller:client").unwrap(),
            "client-session",
        )
        .unwrap();
        let messages =
            MessageSession::with_options(bus.clone(), session.clone(), Default::default(), clock)
                .unwrap();
        let client = HardwareClient::start(HardwareClientSpec {
            beacon: protocols.beacon().clone(),
            concord: protocols.concord().clone(),
            messages: messages.clone(),
            selection: Arc::new(select),
            schemas: SchemaRegistry::default(),
            options: HardwareClientOptions {
                negotiation_timeout: Duration::from_secs(2),
                retry_delay: Duration::from_millis(20),
            },
        })
        .unwrap();
        Self {
            client,
            manager,
            protocols,
            advertisements,
            messages,
            session,
            bus,
            driver,
            tasks,
        }
    }
    async fn close(mut self) {
        bounded(self.client.close()).await.unwrap();
        bounded(self.manager.stop()).await.unwrap();
        self.messages.close().await.unwrap();
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        self.protocols.close().await;
        assert_eq!(self.bus.routes(), 0);
    }
}
async fn bounded<R>(future: impl std::future::Future<Output = R>) -> R {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("operation completes")
}
async fn eventually(mut condition: impl FnMut() -> bool) {
    bounded(async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test]
async fn client_discovers_negotiates_validates_operations_and_delivers_scoped_events() {
    let h = Harness::new().await;
    let mut events = h.client.watch_events().unwrap();
    let context = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    assert_eq!(context.devices()["deck"].descriptor().fingerprint, "first");
    let mut observed = h.client.state().value.candidates;
    assert_eq!(observed[0].labels["room"], "studio");
    observed[0].labels.insert("room".into(), "office".into());
    assert_eq!(
        h.client.state().value.candidates[0].labels["room"],
        "studio"
    );
    let terms = h
        .protocols
        .concord()
        .contract_record(context.agreement().contract())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        terms.participant_sessions["controller:client"],
        "client-session"
    );
    assert_eq!(
        terms.participant_sessions["hardware_manager:manager"],
        "manager-session"
    );
    let reply = bounded(context.request(command(), Duration::from_secs(2), None))
        .await
        .unwrap();
    assert_eq!(reply.message_type, "commandReply");
    let mut rejection = reply.clone();
    rejection.message_type = "commandRejected".into();
    rejection.body.as_object_mut().unwrap().remove("result");
    rejection.body["reason"] = json!("rejected");
    let mut stale = rejection.clone();
    stale.sender_session_id = "old-session".into();
    h.bus.publish(&stale, None, None).await.unwrap();
    h.bus.publish(&rejection, None, None).await.unwrap();
    let outcome = bounded(events.next()).await.unwrap().unwrap();
    assert_eq!(outcome.message.message_type, "commandRejected");
    assert_eq!(outcome.message.in_reply_to, reply.in_reply_to);
    outcome.authorize().unwrap();
    context.authorize().unwrap();
    assert_eq!(h.bus.routes(), 2);
    let mut invalid = command();
    if let HardwareMessageBody::ControlCommand { params, .. } = &mut invalid {
        params.insert("unexpected".into(), json!(true));
    }
    assert!(context.send(invalid).await.is_err());
    assert_eq!(h.driver.contexts.lock().unwrap().len(), 1);
    h.manager.handle_hardware_message(input()).await.unwrap();
    let event = bounded(events.next()).await.unwrap().unwrap();
    event.authorize().unwrap();
    assert_eq!(event.message.message_type, "controlInput");
    bounded(h.client.close()).await.unwrap();
    assert!(event.authorize().is_err());
    assert!(bounded(events.next()).await.is_none());
    assert!(h.messages.state().current);
    assert!(h.protocols.concord().is_current());
    h.close().await;
}

#[tokio::test]
async fn discovery_loss_preserves_claim_and_material_device_change_negotiates_a_successor() {
    let h = Harness::new().await;
    let first = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    let pointer = first.agreement().contract().contract_id.clone();
    for entry in h.advertisements.items("advertisements.").await.unwrap() {
        h.advertisements
            .delete(&entry.key, Some(entry.revision))
            .await
            .unwrap();
    }
    bounded(h.client.wait_for(
        |state| state.value.candidates.is_empty(),
        Some(Duration::from_secs(2)),
    ))
    .await
    .unwrap();
    first.authorize().unwrap();
    assert_eq!(
        h.client
            .context()
            .unwrap()
            .agreement()
            .contract()
            .contract_id,
        pointer
    );
    first
        .request(command(), Duration::from_secs(2), None)
        .await
        .unwrap();
    h.manager
        .set_device(descriptor("deck", "replacement"))
        .await
        .unwrap();
    assert!(first.authorize().is_err());
    bounded(h.client.wait_for(
        |state| state.value.local_authority && state.value.attempts >= 2,
        Some(Duration::from_secs(3)),
    ))
    .await
    .unwrap();
    let successor = h.client.context().unwrap();
    assert_ne!(successor.agreement().contract().contract_id, pointer);
    assert_eq!(
        successor.devices()["deck"].descriptor().fingerprint,
        "replacement"
    );
    assert!(first.send(command()).await.is_err());
    successor
        .request(command(), Duration::from_secs(2), None)
        .await
        .unwrap();
    h.close().await;
}

#[tokio::test]
async fn endpoint_closure_cancels_pending_requests_and_releases_only_owned_subscriptions() {
    let h = Harness::new().await;
    let context = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    h.driver.reply.store(false, Ordering::SeqCst);
    let pending = tokio::spawn(async move {
        context
            .request(command(), Duration::from_secs(30), None)
            .await
    });
    eventually(|| !h.driver.contexts.lock().unwrap().is_empty()).await;
    assert_eq!(h.bus.routes(), 3);
    h.session.close();
    assert!(bounded(pending).await.unwrap().is_err());
    bounded(h.client.close()).await.unwrap();
    assert_eq!(h.bus.routes(), 1);
    assert_eq!(h.client.state().phase, Phase::Closed);
    assert!(h.protocols.concord().is_current());
    h.close().await;
}

#[tokio::test]
async fn invalid_reply_candidates_do_not_end_the_request() {
    let h = Harness::new().await;
    let context = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    h.driver.reply.store(false, Ordering::SeqCst);
    let owner = context.clone();
    let pending =
        tokio::spawn(async move { owner.request(command(), Duration::from_secs(2), None).await });
    eventually(|| !h.driver.contexts.lock().unwrap().is_empty()).await;
    let (request, inbox) = h
        .bus
        .0
        .lock()
        .unwrap()
        .published
        .iter()
        .find(|(_, inbox)| inbox.is_some())
        .cloned()
        .unwrap();
    let mut reply = request.clone();
    reply.sender = "hardware_manager:manager".into();
    reply.sender_session_id = "manager-session".into();
    reply.recipient = deckr::lanes::MessageTarget::Endpoint {
        endpoint: request.sender.clone(),
    };
    reply.recipient_session_id = Some(request.sender_session_id.clone());
    reply.message_id = "bad-operation".into();
    reply.in_reply_to = Some(request.message_id.clone());
    reply.message_type = "commandReply".into();
    reply.body = response(&request).to_value().unwrap();
    reply.body["commandType"] = json!("set_frame");
    h.bus.publish(&reply, inbox.as_deref(), None).await.unwrap();
    reply.message_id = "bad-device".into();
    reply.body["deviceRef"]["deviceId"] = json!("unclaimed");
    h.bus.publish(&reply, inbox.as_deref(), None).await.unwrap();
    eventually(|| h.messages.state().value.rejected_messages > 0).await;
    assert!(!pending.is_finished());
    let driver = h.driver.contexts.lock().unwrap()[0].clone();
    driver.reply(response(&request)).await.unwrap();
    assert_eq!(
        bounded(pending).await.unwrap().unwrap().body["commandType"],
        "clear"
    );
    h.close().await;
}

#[tokio::test]
async fn losing_a_claim_cancels_its_requests_before_successor_operations() {
    let h = Harness::new().await;
    let context = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    h.driver.reply.store(false, Ordering::SeqCst);
    let old = context.clone();
    let pending =
        tokio::spawn(async move { old.request(command(), Duration::from_secs(30), None).await });
    eventually(|| !h.driver.contexts.lock().unwrap().is_empty()).await;
    h.manager
        .set_device(descriptor("deck", "next"))
        .await
        .unwrap();
    assert!(bounded(pending).await.unwrap().is_err());
    eventually(|| h.bus.routes() == 2).await;
    bounded(h.client.wait_for(
        |state| state.value.attempts >= 2 && state.value.local_authority,
        Some(Duration::from_secs(3)),
    ))
    .await
    .unwrap();
    assert!(context.authorize().is_err());
    h.driver.reply.store(true, Ordering::SeqCst);
    h.client
        .context()
        .unwrap()
        .request(command(), Duration::from_secs(2), None)
        .await
        .unwrap();
    h.close().await;
}

#[tokio::test]
async fn event_validation_and_slow_watchers_do_not_revoke_the_live_claim() {
    let h = Harness::new().await;
    let context = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    let contract = context.agreement().contract();
    let mut watcher = h.client.watch_events().unwrap();
    let valid = DeckrMessage::hardware_input_to(
        "manager",
        "manager-session",
        "deck",
        "controller:client",
        "client-session",
        deckr::authority::ContractPointer {
            contract_id: contract.contract_id.clone(),
            generation: contract.generation,
        },
        input(),
    )
    .unwrap();
    let mut bad = valid.clone();
    bad.contract.as_mut().unwrap().generation += 1;
    h.bus.publish(&bad, None, None).await.unwrap();
    bad = valid.clone();
    bad.body["value"] = json!({"eventType":"invalid"});
    h.bus.publish(&bad, None, None).await.unwrap();
    h.bus.publish(&valid, None, None).await.unwrap();
    let event = bounded(watcher.next()).await.unwrap().unwrap();
    event.authorize().unwrap();
    assert_eq!(h.client.state().value.rejected_messages, 2);
    for received in 2..=258 {
        h.bus.publish(&valid, None, None).await.unwrap();
        bounded(h.client.wait_for(
            |state| state.value.received_events >= received,
            Some(Duration::from_secs(1)),
        ))
        .await
        .unwrap();
    }
    assert!(matches!(
        bounded(watcher.next()).await,
        Some(Err(Error::StateUnavailable(_)))
    ));
    assert!(watcher.next().await.is_none());
    context.authorize().unwrap();
    let mut new_watcher = h.client.watch_events().unwrap();
    h.bus.publish(&valid, None, None).await.unwrap();
    bounded(new_watcher.next())
        .await
        .unwrap()
        .unwrap()
        .authorize()
        .unwrap();
    h.close().await;
}

#[tokio::test]
async fn timed_out_competing_negotiations_are_cleaned_without_disturbing_the_owner() {
    let h = Harness::new().await;
    let first = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    let session = EndpointSession::with_id(
        EndpointAddress::parse("controller:competitor").unwrap(),
        "competitor-session",
    )
    .unwrap();
    let messages = MessageSession::new(h.bus.clone(), session).unwrap();
    let competing = HardwareClient::start(HardwareClientSpec {
        beacon: h.protocols.beacon().clone(),
        concord: h.protocols.concord().clone(),
        messages: messages.clone(),
        selection: Arc::new(select),
        schemas: SchemaRegistry::default(),
        options: HardwareClientOptions {
            negotiation_timeout: Duration::from_millis(50),
            retry_delay: Duration::from_millis(20),
        },
    })
    .unwrap();
    bounded(competing.wait_for(
        |state| state.value.attempts >= 2,
        Some(Duration::from_secs(2)),
    ))
    .await
    .unwrap();
    assert!(!competing.state().value.local_authority);
    bounded(competing.close()).await.unwrap();
    first.authorize().unwrap();
    assert!(messages.state().current);
    let competitor = EndpointAddress::parse("controller:competitor").unwrap();
    let contracts = h
        .protocols
        .concord()
        .contracts(deckr::concord::ContractFilters {
            participant: Some(&competitor),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(contracts.len() >= 2);
    for contract in contracts {
        let record = h
            .protocols
            .concord()
            .contract_record(&contract)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, deckr::concord::ContractState::Cancelled);
        assert!(h
            .protocols
            .concord()
            .participant_token(&contract, &competitor)
            .await
            .unwrap()
            .is_none());
    }
    messages.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn dropping_a_client_cancels_a_blocked_selection_and_leaves_shared_resources_open() {
    let h = Harness::new().await;
    h.client.close().await.unwrap();
    let entered = Arc::new(AtomicBool::new(false));
    let policy_entered = entered.clone();
    let blocked = HardwareClient::start(HardwareClientSpec {
        beacon: h.protocols.beacon().clone(),
        concord: h.protocols.concord().clone(),
        messages: h.messages.clone(),
        selection: Arc::new(move |_: Vec<HardwareCandidate>| {
            let entered = policy_entered.clone();
            async move {
                entered.store(true, Ordering::SeqCst);
                std::future::pending::<Result<Option<HardwareSelection>>>().await
            }
        }),
        schemas: SchemaRegistry::default(),
        options: HardwareClientOptions::default(),
    })
    .unwrap();
    let mut state = blocked.watch_state().unwrap();
    eventually(|| entered.load(Ordering::SeqCst)).await;
    drop(blocked);
    use futures_util::StreamExt;
    bounded(async {
        while let Some(state) = state.next().await {
            if state.phase == Phase::Closed {
                return;
            }
        }
        panic!("closed state must be observed");
    })
    .await;
    eventually(|| h.bus.routes() == 1).await;
    assert!(h.messages.state().current);
    assert!(h.protocols.concord().is_current());
    h.close().await;
}

#[tokio::test]
async fn closed_discovery_does_not_end_a_current_hardware_claim() {
    let h = Harness::new().await;
    let context = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    h.protocols
        .beacon()
        .materialized_store()
        .close_and_wait()
        .await;
    bounded(h.client.wait_for(
        |state| !state.value.discovery_current,
        Some(Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    context.authorize().unwrap();
    context
        .request(command(), Duration::from_secs(1), None)
        .await
        .unwrap();
    // Once authority actually ends, a closed discovery view cannot negotiate a successor.
    context.agreement().request_close();
    bounded(
        h.client
            .wait_for(|state| state.phase == Phase::Closed, None),
    )
    .await
    .unwrap();
    assert!(h.client.context().is_err());
    assert!(h.messages.state().current);
    h.close().await;
}

#[tokio::test]
async fn invalid_selection_is_rejected_before_creating_a_claim_and_policy_can_recover() {
    let h = Harness::new().await;
    h.client.close().await.unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let selected_calls = calls.clone();
    let client = HardwareClient::start(HardwareClientSpec {
        beacon: h.protocols.beacon().clone(),
        concord: h.protocols.concord().clone(),
        messages: h.messages.clone(),
        selection: Arc::new(move |candidates: Vec<HardwareCandidate>| {
            let call = selected_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                let mut selection = select(candidates).await?.unwrap();
                match call {
                    0 => selection.advertisement_key = "unknown".into(),
                    1 => selection.devices = BTreeMap::new(),
                    2 => selection.devices = BTreeMap::from([("unknown".into(), 1)]),
                    3 => selection.devices = BTreeMap::from([("deck".into(), 0)]),
                    4 => selection.devices = BTreeMap::from([("deck".into(), 2)]),
                    _ => {}
                }
                Ok(Some(selection))
            }
        }),
        schemas: SchemaRegistry::default(),
        options: HardwareClientOptions {
            retry_delay: Duration::from_millis(10),
            ..Default::default()
        },
    })
    .unwrap();
    bounded(client.wait_ready(Some(Duration::from_secs(2))))
        .await
        .unwrap();
    assert_eq!(client.state().value.attempts, 1);
    assert!(calls.load(Ordering::SeqCst) >= 6);
    client.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn one_claim_can_scope_multiple_devices_and_losing_one_ends_all_its_operations() {
    let h = Harness::new().await;
    h.client.close().await.unwrap();
    h.manager
        .set_device(descriptor("second", "second-fingerprint"))
        .await
        .unwrap();
    let client = HardwareClient::start(HardwareClientSpec {
        beacon: h.protocols.beacon().clone(),
        concord: h.protocols.concord().clone(),
        messages: h.messages.clone(),
        selection: Arc::new(|candidates: Vec<HardwareCandidate>| async move {
            Ok(candidates
                .first()
                .filter(|candidate| candidate.devices.len() == 2)
                .map(|candidate| HardwareSelection {
                    advertisement_key: candidate.advertisement_key.clone(),
                    devices: BTreeMap::from([("deck".into(), 1), ("second".into(), 1)]),
                }))
        }),
        schemas: SchemaRegistry::default(),
        options: HardwareClientOptions {
            retry_delay: Duration::from_millis(10),
            ..Default::default()
        },
    })
    .unwrap();
    let context = bounded(client.wait_ready(Some(Duration::from_secs(2))))
        .await
        .unwrap();
    assert_eq!(context.devices().len(), 2);
    let mut second = command();
    if let HardwareMessageBody::ControlCommand { device_ref, .. } = &mut second {
        device_ref.device_id = "second".into();
    }
    context
        .request(second, Duration::from_secs(1), None)
        .await
        .unwrap();
    h.manager
        .remove_device("second", "device disconnected")
        .await
        .unwrap();
    assert!(context.authorize().is_err());
    assert!(context.send(command()).await.is_err());
    client.close().await.unwrap();
    h.close().await;
}

#[tokio::test]
async fn cancelled_close_waiter_does_not_abandon_claim_cleanup() {
    let h = Harness::new().await;
    let context = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    let mut close = Box::pin(h.client.close());
    assert!(futures_util::poll!(&mut close).is_pending());
    drop(close);
    assert!(context.authorize().is_err());
    bounded(
        h.client
            .wait_for(|state| state.phase == Phase::Closed, None),
    )
    .await
    .unwrap();
    let record = h
        .protocols
        .concord()
        .contract_record(context.agreement().contract())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, deckr::concord::ContractState::Cancelled);
    assert!(h.messages.state().current);
    h.close().await;
}

#[tokio::test]
async fn manager_and_client_use_the_injected_clock_for_requests_replies_and_events() {
    let clock = Arc::new(support::ManualClock::default());
    let h = Harness::with_clock(clock.clone()).await;
    let context = bounded(h.client.wait_ready(Some(Duration::from_secs(3))))
        .await
        .unwrap();
    let reply = bounded(context.request(command(), Duration::from_secs(1), None))
        .await
        .unwrap();
    assert_eq!(reply.created_at, clock.utc_now().to_rfc3339());
    let mut events = h.client.watch_events().unwrap();
    h.manager.handle_hardware_message(input()).await.unwrap();
    let event = bounded(events.next()).await.unwrap().unwrap();
    assert_eq!(event.message.created_at, clock.utc_now().to_rfc3339());
    h.driver.reply.store(false, Ordering::SeqCst);
    let owner = context.clone();
    let request =
        tokio::spawn(async move { owner.request(command(), Duration::from_secs(1), None).await });
    eventually(|| h.driver.contexts.lock().unwrap().len() == 2).await;
    let expired = h.driver.contexts.lock().unwrap()[1].clone();
    clock.advance(Duration::from_secs(1));
    assert!(matches!(
        bounded(request).await.unwrap(),
        Err(Error::Deadline(_))
    ));
    assert!(expired.authorize().is_err());
    assert!(expired.reply(response(expired.message())).await.is_err());
    context.authorize().unwrap();
    h.driver.reply.store(true, Ordering::SeqCst);
    let reply = bounded(context.request(command(), Duration::from_secs(1), None))
        .await
        .unwrap();
    assert_eq!(reply.created_at, clock.utc_now().to_rfc3339());
    h.close().await;
}
