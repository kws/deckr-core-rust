#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use deckr::authority::ContractPointer;
use deckr::concord::{AgreementIntent, ManagedParticipant, ManagedProposal, ParticipantCandidates};
use deckr::endpoint::EndpointAddress;
use deckr::lanes::{DeckrMessage, EntitySubject, MessageTarget};
use deckr::runtime::ProtocolRuntime;
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use deckr::session::EndpointSession;
use deckr::state::MemoryStateStore;
use serde_json::json;

pub type Context = ServiceContext<MemoryStateStore, MemoryStateStore>;
pub struct Harness {
    pub runtime: ProtocolRuntime<MemoryStateStore, MemoryStateStore, MemoryStateStore>,
    pub contracts: MemoryStateStore,
    pub proposal: ManagedProposal<MemoryStateStore, MemoryStateStore>,
    pub participant: ManagedParticipant<MemoryStateStore, MemoryStateStore>,
    pub consumer: Context,
    pub service: Context,
}
impl Harness {
    pub async fn new() -> Self {
        let contracts = MemoryStateStore::default();
        let runtime = ProtocolRuntime::start(
            MemoryStateStore::ttl_bound(30).unwrap(),
            contracts.clone(),
            MemoryStateStore::ttl_bound(30).unwrap(),
        )
        .await
        .unwrap();
        let declaration = ServiceProtocol::from_value(json!({
            "namespace":"org.example.test", "featureId":"org.example.test", "advertisementProfile":"org.example.test.ad.v1", "useProfile":"org.example.test.use.v1",
            "operations":{"read":{},"write":{}},
            "messages":{
                "read":{"intent":"query","direction":"consumer_to_service","operation":"read","exchangePattern":"request_reply","resultSchema":{"schemaId":"result","schema":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"]}}},
                "write":{"intent":"command","direction":"consumer_to_service","operation":"write","exchangePattern":"one_way"},
                "changed":{"intent":"event","direction":"service_to_consumer","exchangePattern":"one_way"}
            },
            "viewFamilies":{"state":{"storeName":"test_views","writer":"service"},"requests":{"storeName":"test_views","writer":"consumer"}}
        })).unwrap();
        let payload = declaration
            .advertisement_payload("test", "service-session", ServiceBackendStatus::Available)
            .unwrap();
        let descriptor = ServiceDescriptor {
            candidate: None,
            service_id: payload.service_id,
            namespace: payload.service_namespace,
            endpoint: payload.service_endpoint,
            session_id: payload.session_id,
            advertisement_profile: payload.profile,
            use_profile: payload.service_use_profile,
            supported_operations: payload.supported_operations.into_iter().collect(),
            supported_messages: payload.supported_messages,
            views: payload.views,
            backend_status: payload.backend_status,
            diagnostics: payload.diagnostics,
        };
        let service =
            EndpointSession::with_id(descriptor.endpoint.clone(), descriptor.session_id.clone())
                .unwrap();
        let consumer = EndpointSession::with_id(
            EndpointAddress::parse("controller:test").unwrap(),
            "consumer-session",
        )
        .unwrap();
        let participant = runtime
            .concord()
            .participate(
                service.clone(),
                Some(descriptor.use_profile.clone()),
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
            .unwrap();
        let mut intent = AgreementIntent::new(vec![
            service.endpoint().clone(),
            consumer.endpoint().clone(),
        ]);
        intent.profile = Some(descriptor.use_profile.clone());
        intent.current_sessions = BTreeMap::from([
            (service.endpoint().to_string(), service.id().into()),
            (consumer.endpoint().to_string(), consumer.id().into()),
        ]);
        let proposal = runtime.concord().propose(consumer.clone(), intent).unwrap();
        bounded(proposal.wait_ready(None)).await.unwrap();
        let pointer = ContractPointer {
            contract_id: proposal.contract().contract_id.clone(),
            generation: 1,
        };
        let service_agreement = participant.agreement(&pointer).unwrap();
        bounded(service_agreement.wait_ready(None)).await.unwrap();
        let protocol =
            ValidatedServiceProtocol::new(declaration, &SchemaRegistry::default()).unwrap();
        let views = BTreeMap::from([
            (
                "state".into(),
                vec![format!("{}desk.", descriptor.views["state"].key_prefix)],
            ),
            ("requests".into(), vec![]),
        ]);
        let consumer_access = service_access(&descriptor, ["read"], views.clone()).unwrap();
        let service_access = service_access(&descriptor, ["read", "write"], views).unwrap();
        let consumer = ServiceContext::new(
            proposal.agreement(),
            protocol.clone(),
            descriptor.clone(),
            consumer_access,
        )
        .unwrap();
        let service =
            ServiceContext::new(service_agreement, protocol, descriptor, service_access).unwrap();
        Self {
            runtime,
            contracts,
            proposal,
            participant,
            consumer,
            service,
        }
    }
    pub async fn close(self) {
        bounded(self.proposal.close()).await.unwrap();
        bounded(self.participant.close()).await.unwrap();
        self.runtime.close().await;
    }
    pub fn message(&self, name: &str, from_service: bool) -> DeckrMessage {
        let definition = &self.consumer.declaration().protocol().messages[name];
        let body = ServiceMessageBody {
            service_namespace: self.consumer.descriptor().namespace.clone(),
            name: name.into(),
            intent: definition.intent,
            exchange_pattern: definition.exchange_pattern,
            params: Default::default(),
            event: None,
            result: None,
            error: None,
            status: None,
        };
        let (sender, sender_session, recipient, recipient_session) = if from_service {
            (
                "service:test",
                "service-session",
                "controller:test",
                "consumer-session",
            )
        } else {
            (
                "controller:test",
                "consumer-session",
                "service:test",
                "service-session",
            )
        };
        DeckrMessage {
            message_id: uuid::Uuid::new_v4().to_string(),
            protocol_version: "1".into(),
            schema_version: "1".into(),
            lane: "services".into(),
            message_type: "serviceMessage".into(),
            sender: sender.into(),
            sender_session_id: sender_session.into(),
            recipient: MessageTarget::Endpoint {
                endpoint: recipient.into(),
            },
            recipient_session_id: Some(recipient_session.into()),
            contract: Some(ContractPointer {
                contract_id: self.proposal.contract().contract_id.clone(),
                generation: 1,
            }),
            subject: EntitySubject {
                kind: "service".into(),
                identifiers: Default::default(),
            },
            created_at: chrono::Utc::now().to_rfc3339(),
            expires_at: None,
            ttl_ms: None,
            in_reply_to: None,
            causation_id: None,
            trace: None,
            body: body.to_value().unwrap(),
        }
    }
}
pub async fn bounded<R>(future: impl std::future::Future<Output = R>) -> R {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("operation completes")
}
