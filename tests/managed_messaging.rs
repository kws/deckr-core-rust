mod support;

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use deckr::endpoint::EndpointAddress;
use deckr::lanes::{DeckrMessage, MessageTarget};
use deckr::lifecycle::{Clock, Phase};
use deckr::messaging::{
    Delivery, DeliveryStream, LaneRegistry, MessageSession, MessageTransport, ReplyInbox,
};
use deckr::session::EndpointSession;
use deckr::{Error, Result};
use futures_core::Stream;
use serde_json::json;
use support::ManualClock;
use tokio::sync::mpsc;

#[derive(Clone, Default)]
struct Transport {
    shared: Arc<TransportState>,
}
#[derive(Default)]
struct TransportState {
    senders: Mutex<Vec<mpsc::UnboundedSender<Result<Delivery>>>>,
    publications: Mutex<Vec<Publication>>,
    live: Arc<AtomicUsize>,
    stall_publish: AtomicBool,
    fail_publish: AtomicBool,
    advance_on_open: Mutex<Option<ManualClock>>,
}
type Publication = (DeckrMessage, Option<String>, Option<String>);
struct TrackedStream {
    receiver: mpsc::UnboundedReceiver<Result<Delivery>>,
    live: Arc<AtomicUsize>,
}
impl Drop for TrackedStream {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Stream for TrackedStream {
    type Item = Result<Delivery>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}
impl Transport {
    fn stream(&self) -> DeliveryStream {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.shared.senders.lock().unwrap().push(sender);
        self.shared.live.fetch_add(1, Ordering::SeqCst);
        Box::pin(TrackedStream {
            receiver,
            live: self.shared.live.clone(),
        })
    }
    fn push(&self, index: usize, message: DeckrMessage) {
        self.shared.senders.lock().unwrap()[index]
            .send(Ok(Delivery {
                message,
                reply_address: None,
            }))
            .unwrap();
    }
    fn last(&self) -> DeckrMessage {
        self.shared
            .publications
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .0
            .clone()
    }
    fn live(&self) -> usize {
        self.shared.live.load(Ordering::SeqCst)
    }
    fn publications(&self) -> usize {
        self.shared.publications.lock().unwrap().len()
    }
}
impl MessageTransport for Transport {
    async fn subscribe(&self, _: &str, _: &EndpointAddress) -> Result<DeliveryStream> {
        Ok(self.stream())
    }
    async fn open_inbox(&self) -> Result<ReplyInbox> {
        if let Some(clock) = self.shared.advance_on_open.lock().unwrap().as_ref() {
            clock.advance(Duration::from_secs(1));
        }
        Ok(ReplyInbox {
            address: "_INBOX.test".into(),
            messages: self.stream(),
        })
    }
    async fn publish(
        &self,
        message: &DeckrMessage,
        inbox: Option<&str>,
        reply_address: Option<&str>,
    ) -> Result<()> {
        if reply_address.is_some() {
            assert!(self.live() > 0, "inbox exists before publication");
        }
        self.shared.publications.lock().unwrap().push((
            message.clone(),
            inbox.map(str::to_owned),
            reply_address.map(str::to_owned),
        ));
        if self.shared.stall_publish.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.shared.fail_publish.load(Ordering::SeqCst) {
            return Err(Error::StateUnavailable("publish failed".into()));
        }
        Ok(())
    }
}
fn session() -> EndpointSession {
    EndpointSession::with_id(
        EndpointAddress::parse("controller:test").unwrap(),
        "local-session",
    )
    .unwrap()
}
fn setup() -> (
    MessageSession<Transport>,
    Transport,
    EndpointSession,
    ManualClock,
) {
    let transport = Transport::default();
    let endpoint = session();
    let clock = ManualClock::default();
    let messaging = MessageSession::with_options(
        transport.clone(),
        endpoint.clone(),
        LaneRegistry::default(),
        Arc::new(clock.clone()),
    )
    .unwrap();
    (messaging, transport, endpoint, clock)
}
fn message() -> DeckrMessage {
    DeckrMessage::from_text(&json!({
        "messageId":"request", "createdAt":"2026-01-01T00:00:00Z",
        "lane":"services", "messageType":"serviceMessage",
        "sender":"controller:test", "senderSessionId":"local-session",
        "recipient":{"targetType":"endpoint","endpoint":"service:peer"}, "recipientSessionId":"remote-session",
        "contract":{"contractId":"agreement", "generation":1},
        "subject":{"kind":"service", "identifiers":{}},
        "body":{"serviceNamespace":"org.example.service", "name":"read", "intent":"query", "exchangePattern":"request_reply"}
    }).to_string()).unwrap()
}
fn reply(request: &DeckrMessage, id: &str) -> DeckrMessage {
    let mut reply = request.clone();
    reply.message_id = id.into();
    reply.sender = request.recipient_endpoint().unwrap().into();
    reply.sender_session_id = request.recipient_session_id.clone().unwrap();
    reply.recipient = MessageTarget::Endpoint {
        endpoint: request.sender.clone(),
    };
    reply.recipient_session_id = Some(request.sender_session_id.clone());
    reply.in_reply_to = Some(request.message_id.clone());
    reply
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
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
async fn request_ignores_malformed_unrelated_stale_and_rejected_replies() {
    let (messaging, transport, _, _) = setup();
    let owner = messaging.clone();
    let task = tokio::spawn(async move {
        owner
            .request(
                message(),
                Duration::from_secs(5),
                Some(Arc::new(|reply| reply.message_id == "accepted")),
            )
            .await
    });
    eventually(|| transport.publications() == 1).await;
    let request = transport.last();
    transport.shared.senders.lock().unwrap()[0]
        .send(Err(Error::Invalid("malformed JSON".into())))
        .unwrap();
    for case in 0..9 {
        let mut candidate = reply(&request, "rejected");
        match case {
            0 => candidate.message_id = request.message_id.clone(),
            1 => candidate.in_reply_to = Some("different".into()),
            2 => candidate.contract.as_mut().unwrap().generation += 1,
            3 => candidate.sender_session_id = "old-session".into(),
            4 => candidate.recipient_session_id = Some("old-local-session".into()),
            5 => candidate.sender = "service:stranger".into(),
            6 => candidate.ttl_ms = Some(0),
            7 => candidate.body["intent"] = json!("invalid"),
            _ => {} // structurally valid but rejected by the application predicate
        }
        transport.push(0, candidate);
    }
    transport.push(0, reply(&request, "accepted"));
    assert_eq!(bounded(task).await.unwrap().unwrap().message_id, "accepted");
    assert_eq!(transport.live(), 0);
    assert!(messaging.state().value.rejected_messages >= 2);
    assert_eq!(messaging.state().value.pending_requests, 0);
    bounded(messaging.close()).await.unwrap();
}

#[tokio::test]
async fn request_releases_inbox_on_timeout_failure_cancellation_and_endpoint_close() {
    for outcome in ["timeout", "failure", "cancellation", "endpoint"] {
        let (messaging, transport, endpoint, clock) = setup();
        transport
            .shared
            .fail_publish
            .store(outcome == "failure", Ordering::SeqCst);
        let owner = messaging.clone();
        let task =
            tokio::spawn(
                async move { owner.request(message(), Duration::from_secs(5), None).await },
            );
        eventually(|| transport.publications() == 1).await;
        match outcome {
            "timeout" => {
                clock.advance(Duration::from_secs(5));
                assert!(matches!(
                    bounded(task).await.unwrap(),
                    Err(Error::Deadline(_))
                ));
            }
            "failure" => {
                assert!(matches!(
                    bounded(task).await.unwrap(),
                    Err(Error::StateUnavailable(_))
                ));
            }
            "cancellation" => {
                task.abort();
                assert!(bounded(task).await.unwrap_err().is_cancelled());
            }
            _ => {
                endpoint.close();
                assert!(matches!(
                    bounded(task).await.unwrap(),
                    Err(Error::Closed(_))
                ));
            }
        }
        eventually(|| transport.live() == 0 && messaging.state().value.pending_requests == 0).await;
        bounded(messaging.close()).await.unwrap();
    }
}

#[tokio::test]
async fn send_owns_identity_and_reply_preserves_route_pointer_and_correlation() {
    let (messaging, transport, _, _) = setup();
    let mut outgoing = message();
    outgoing.sender = "service:forged".into();
    outgoing.sender_session_id = "forged".into();
    messaging.send(outgoing).await.unwrap();
    assert_eq!(transport.last().sender, "controller:test");
    assert_eq!(transport.last().sender_session_id, "local-session");
    let request = Delivery {
        message: reply(&message(), "incoming"),
        reply_address: Some("_INBOX.peer".into()),
    };
    let mut answer = message();
    answer.message_id = "response".into();
    answer.contract = None;
    messaging.reply(&request, answer).await.unwrap();
    {
        let publications = transport.shared.publications.lock().unwrap();
        let (reply, route, return_route) = &publications[1];
        assert_eq!(route.as_deref(), Some("_INBOX.peer"));
        assert!(return_route.is_none());
        assert_eq!(reply.in_reply_to.as_deref(), Some("incoming"));
        assert_eq!(reply.contract, request.message.contract);
        assert_eq!(reply.recipient_endpoint(), Some("service:peer"));
        assert_eq!(
            reply.recipient_session_id.as_deref(),
            Some("remote-session")
        );
    }
    bounded(messaging.close()).await.unwrap();
}

#[tokio::test]
async fn subscriber_filters_invalid_items_and_endpoint_closure_discards_queued_work() {
    let (messaging, transport, endpoint, _) = setup();
    let mut subscription = messaging.subscribe("services").await.unwrap();
    let mut stale = reply(&message(), "stale");
    stale.recipient_session_id = Some("previous".into());
    transport.push(0, stale);
    transport.shared.senders.lock().unwrap()[0]
        .send(Err(Error::Invalid("broken".into())))
        .unwrap();
    transport.push(0, reply(&message(), "current"));
    assert_eq!(
        bounded(subscription.next())
            .await
            .unwrap()
            .unwrap()
            .message
            .message_id,
        "current"
    );
    transport.push(0, reply(&message(), "queued"));
    endpoint.close();
    assert!(bounded(subscription.next()).await.is_none());
    bounded(messaging.close()).await.unwrap();
    assert_eq!(transport.live(), 0);
    assert_eq!(messaging.state().phase, Phase::Closed);
}

#[tokio::test]
async fn slow_subscriber_fails_without_blocking_other_subscribers() {
    let (messaging, transport, _, _) = setup();
    let mut slow = messaging.subscribe("services").await.unwrap();
    let mut fast = messaging.subscribe("services").await.unwrap();
    for index in 0..257 {
        transport.push(0, reply(&message(), &format!("message-{index}")));
    }
    eventually(|| transport.live() == 1).await;
    assert!(matches!(
        bounded(slow.next()).await,
        Some(Err(Error::StateUnavailable(_)))
    ));
    assert!(bounded(slow.next()).await.is_none());
    transport.push(1, reply(&message(), "fast"));
    assert_eq!(
        bounded(fast.next())
            .await
            .unwrap()
            .unwrap()
            .message
            .message_id,
        "fast"
    );
    bounded(fast.close()).await.unwrap();
    assert_eq!(transport.live(), 0);
    bounded(messaging.close()).await.unwrap();
}

#[tokio::test]
async fn last_owner_drop_and_cancelled_close_still_release_workers() {
    let (messaging, transport, _, _) = setup();
    let mut subscription = messaging.subscribe("services").await.unwrap();
    let mut watcher = messaging.watch_state().unwrap();
    drop(messaging);
    eventually(|| transport.live() == 0).await;
    assert!(bounded(subscription.next()).await.is_none());
    use futures_util::StreamExt;
    let last = bounded(async {
        let mut last = None;
        while let Some(state) = watcher.next().await {
            last = Some(state);
        }
        last
    })
    .await;
    assert_eq!(last.unwrap().phase, Phase::Closed);

    let (messaging, transport, _, _) = setup();
    transport.shared.stall_publish.store(true, Ordering::SeqCst);
    let owner = messaging.clone();
    let pending = tokio::spawn(async move { owner.send(message()).await });
    eventually(|| transport.publications() == 1).await;
    let mut closing = Box::pin(messaging.close());
    assert!(futures_util::poll!(closing.as_mut()).is_pending());
    drop(closing);
    assert!(matches!(
        bounded(pending).await.unwrap(),
        Err(Error::Closed(_))
    ));
    bounded(messaging.close()).await.unwrap();
}

#[tokio::test]
async fn expiry_is_rechecked_after_application_acceptance_and_timeouts_do_not_overflow() {
    let (messaging, transport, _, clock) = setup();
    let predicate_clock = clock.clone();
    let owner = messaging.clone();
    let task = tokio::spawn(async move {
        owner
            .request(
                message(),
                Duration::from_secs(10),
                Some(Arc::new(move |_| {
                    predicate_clock.advance(Duration::from_secs(1));
                    true
                })),
            )
            .await
    });
    eventually(|| transport.publications() == 1).await;
    let mut expired_during_predicate = reply(&transport.last(), "short-lived");
    expired_during_predicate.ttl_ms = Some(1000);
    transport.push(0, expired_during_predicate);
    eventually(|| clock.utc_now().timestamp() > 1767225600).await;
    transport.push(0, reply(&transport.last(), "durable"));
    assert_eq!(bounded(task).await.unwrap().unwrap().message_id, "durable");
    assert!(matches!(
        messaging.request(message(), Duration::MAX, None).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(transport.live(), 0);
    bounded(messaging.close()).await.unwrap();
}

#[test]
fn envelope_defaults_and_strict_json_and_lane_validation() {
    let base = message();
    assert_eq!(base.protocol_version, "1");
    for bad in [
        "{\"a\":1,\"a\":2}",
        "{\"nested\":[{\"x\":1,\"x\":2}]}",
        "{\"a\":1e999}",
        "{\"a\":\"\\uD800\"}",
    ] {
        assert!(deckr::json::parse(bad.as_bytes()).is_err(), "{bad}");
    }
    assert!(
        deckr::json::parse(b"{\"a\":9999999999999999999999999999999999999999999999999999999}")
            .is_ok()
    );
    let mut value = serde_json::to_value(&base).unwrap();
    value["recipient"]
        .as_object_mut()
        .unwrap()
        .remove("targetType");
    assert!(DeckrMessage::from_text(&value.to_string()).is_ok());
    for extra in [
        json!({"endpoint":"service:peer", "domain":null}),
        json!({"endpoint":"service:peer", "targetType":null}),
    ] {
        value["recipient"] = extra;
        assert!(DeckrMessage::from_text(&value.to_string()).is_err());
    }
    for field in ["messageId", "createdAt"] {
        let mut missing = serde_json::to_value(&base).unwrap();
        missing.as_object_mut().unwrap().remove(field);
        assert!(DeckrMessage::from_text(&missing.to_string()).is_err());
    }
    let mut invalid = base.clone();
    invalid.sender = "hardware_manager:no-services".into();
    assert!(invalid.validate().is_err());
    invalid = base.clone();
    invalid.trace = Some(json!({"traceParent":42}));
    assert!(invalid.validate().is_err());
    invalid = base.clone();
    invalid.body["unknown"] = json!(true);
    assert!(invalid.validate().is_err());
    let mut long_lived = base;
    long_lived.ttl_ms = Some(u64::MAX);
    assert!(!long_lived.is_expired_at(ManualClock::default().utc_now()));
    long_lived.expires_at = Some(long_lived.created_at.clone());
    assert!(long_lived.is_expired_at(ManualClock::default().utc_now()));
}

#[test]
fn wire_json_preserves_objects_named_like_serde_internal_numbers() {
    let body = deckr::json::parse(br#"{"$serde_json::private::Number":"not a number","nested":[{"$serde_json::private::RawValue":"not JSON"}],"integer":99999999999999999999999999999999999999999999999999}"#).unwrap();
    assert!(body.is_object());
    assert_eq!(body["$serde_json::private::Number"], "not a number");
    let mut envelope = message();
    envelope.body["params"] = body.clone();
    let decoded = DeckrMessage::from_text(&envelope.to_text().unwrap()).unwrap();
    assert_eq!(decoded.body["params"], body);
    assert_eq!(
        decoded.service_body().unwrap().params,
        body.as_object().unwrap().clone()
    );
    assert!(
        deckr::json::parse(format!("{}0{}", "[".repeat(130), "]".repeat(130)).as_bytes()).is_err()
    );
}

#[tokio::test]
async fn protected_messages_use_only_the_selected_agreement_and_live_sessions() {
    use deckr::authority::{AgreementMessageGuard, ContractPointer};
    use deckr::concord::{AgreementIntent, ConcordCoordinator};
    use deckr::messaging::MessageAuthority;
    use deckr::state::{MaterializedStateStore, MemoryStateStore};

    let (messaging, transport, local, clock) = setup();
    let peer = EndpointSession::with_id(
        EndpointAddress::parse("service:peer").unwrap(),
        "remote-session",
    )
    .unwrap();
    let concord = ConcordCoordinator::new(
        MaterializedStateStore::start(MemoryStateStore::default(), "contracts.")
            .await
            .unwrap(),
        MaterializedStateStore::start(MemoryStateStore::ttl_bound(120).unwrap(), "contracts.")
            .await
            .unwrap(),
    )
    .clock(Arc::new(clock));
    let mut intent = AgreementIntent::new(vec![local.endpoint().clone(), peer.endpoint().clone()]);
    intent.profile = Some("org.example.profile".into());
    let agreement = concord.propose(local, intent).unwrap();
    bounded(agreement.wait_for(|state| state.value.token.is_some(), None))
        .await
        .unwrap();
    let peer_token = concord
        .attach(agreement.contract(), peer.endpoint(), peer.id(), None)
        .await
        .unwrap();
    bounded(agreement.wait_ready(None)).await.unwrap();
    let guard =
        AgreementMessageGuard::new(agreement.agreement(), "services", "org.example.profile")
            .unwrap();
    let mut request = message();
    request.contract = Some(ContractPointer {
        contract_id: agreement.contract().contract_id.clone(),
        generation: agreement.contract().generation,
    });
    messaging
        .send_authorized(request.clone(), &guard)
        .await
        .unwrap();
    let incoming = reply(&request, "incoming");
    guard.authorize_incoming(&incoming).unwrap();
    for case in 0..5 {
        let mut bad = incoming.clone();
        match case {
            0 => bad.contract.as_mut().unwrap().generation += 1,
            1 => bad.sender_session_id = "previous-session".into(),
            2 => bad.recipient_session_id = None,
            3 => bad.sender = "service:stranger".into(),
            _ => bad.contract.as_mut().unwrap().contract_id = "other-valid-contract".into(),
        }
        assert!(matches!(
            guard.authorize_incoming(&bad),
            Err(Error::Unauthorized(_))
        ));
    }
    agreement.request_close();
    assert!(matches!(
        guard.authorize_incoming(&incoming),
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        messaging.send_authorized(request, &guard).await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(transport.publications(), 1);
    bounded(agreement.close()).await.unwrap();
    concord.withdraw(&peer_token).await.unwrap();
    bounded(messaging.close()).await.unwrap();
    concord.close_sources_and_wait().await;
}

#[tokio::test]
async fn authority_is_rechecked_after_inbox_creation_and_reply_acceptance() {
    use deckr::messaging::MessageAuthority;
    struct Guard {
        transport: Transport,
        allow: Arc<AtomicBool>,
    }
    impl MessageAuthority for Guard {
        fn authorize_outgoing(&self, _: &DeckrMessage) -> Result<()> {
            if self.transport.live() > 0 || !self.allow.load(Ordering::SeqCst) {
                Err(Error::Unauthorized("domain consent withdrawn".into()))
            } else {
                Ok(())
            }
        }
        fn authorize_incoming(&self, _: &DeckrMessage) -> Result<()> {
            if self.allow.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(Error::Unauthorized("domain consent withdrawn".into()))
            }
        }
    }
    let (messaging, transport, _, _) = setup();
    let guard = Guard {
        transport: transport.clone(),
        allow: Arc::new(AtomicBool::new(true)),
    };
    assert!(matches!(
        messaging
            .request_authorized(message(), Duration::from_secs(5), None, &guard)
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(transport.publications(), 0);
    assert_eq!(transport.live(), 0);

    // The second guard permits publication and revokes inside the awaited predicate.
    let consent = Arc::new(AtomicBool::new(true));
    let guard = Guard {
        transport: Transport::default(),
        allow: consent.clone(),
    };
    let owner = messaging.clone();
    let task = tokio::spawn(async move {
        owner
            .request_authorized(
                message(),
                Duration::from_secs(5),
                Some(Arc::new(move |_| {
                    consent.store(false, Ordering::SeqCst);
                    true
                })),
                &guard,
            )
            .await
    });
    eventually(|| transport.publications() == 1).await;
    transport.push(1, reply(&transport.last(), "reply"));
    assert!(matches!(
        bounded(task).await.unwrap(),
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(transport.live(), 0);
    bounded(messaging.close()).await.unwrap();
}

#[tokio::test]
async fn expired_request_is_not_published_after_waiting_for_inbox_readiness() {
    let (messaging, transport, _, clock) = setup();
    *transport.shared.advance_on_open.lock().unwrap() = Some(clock);
    let mut request = message();
    request.ttl_ms = Some(1);
    assert!(matches!(
        messaging
            .request(request, Duration::from_secs(5), None)
            .await,
        Err(Error::Deadline(_))
    ));
    assert_eq!(transport.publications(), 0);
    assert_eq!(transport.live(), 0);
    bounded(messaging.close()).await.unwrap();
}

#[tokio::test]
async fn endpoint_lifetimes_can_share_a_transport_without_sharing_shutdown() {
    let (first, transport, first_endpoint, _) = setup();
    let second_endpoint = EndpointSession::with_id(
        EndpointAddress::parse("controller:second").unwrap(),
        "second-session",
    )
    .unwrap();
    let second = MessageSession::new(transport.clone(), second_endpoint.clone()).unwrap();
    let mut first_subscription = first.subscribe("services").await.unwrap();
    let mut second_subscription = second.subscribe("services").await.unwrap();
    first_endpoint.close();
    bounded(first.close()).await.unwrap();
    assert!(bounded(first_subscription.next()).await.is_none());
    assert_eq!(transport.live(), 1);
    let mut delivery = reply(&message(), "second");
    delivery.recipient = MessageTarget::Endpoint {
        endpoint: second_endpoint.endpoint().to_string(),
    };
    delivery.recipient_session_id = Some(second_endpoint.id().to_string());
    transport.push(1, delivery);
    assert_eq!(
        bounded(second_subscription.next())
            .await
            .unwrap()
            .unwrap()
            .message
            .message_id,
        "second"
    );
    bounded(second.close()).await.unwrap();
    assert_eq!(transport.live(), 0);
}

#[tokio::test]
async fn extension_lanes_are_explicit_and_reply_correlation_preserves_absent_pointers() {
    let transport = Transport::default();
    let mut request = message();
    request.lane = "org.example.extension".into();
    request.contract = None;
    let unregistered = MessageSession::new(transport.clone(), session()).unwrap();
    assert!(matches!(
        unregistered.send(request.clone()).await,
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        unregistered.subscribe(&request.lane).await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(transport.publications(), 0);
    unregistered.close().await.unwrap();
    let lanes = LaneRegistry::default()
        .with_lane(request.lane.clone(), |_: &DeckrMessage| Ok(()))
        .unwrap();
    let messaging = MessageSession::with_options(
        transport.clone(),
        session(),
        lanes,
        Arc::new(ManualClock::default()),
    )
    .unwrap();
    let owner = messaging.clone();
    let task =
        tokio::spawn(async move { owner.request(request, Duration::from_secs(5), None).await });
    eventually(|| transport.publications() == 1).await;
    let mut unexpected_pointer = reply(&transport.last(), "wrong");
    unexpected_pointer.contract = message().contract;
    transport.push(0, unexpected_pointer);
    transport.push(0, reply(&transport.last(), "without-pointer"));
    assert!(bounded(task).await.unwrap().unwrap().contract.is_none());
    assert_eq!(transport.live(), 0);
    bounded(messaging.close()).await.unwrap();
}
