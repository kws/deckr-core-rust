#[path = "support/service.rs"]
mod service_support;

use deckr::lanes::{DeckrMessage, MessageTarget};
use deckr::messaging::MessageAuthority;
use deckr::services::*;
use deckr::state::StateStore;
use deckr::{Error, Result};
use serde_json::{json, Value};
use service_support::{bounded, Harness};

#[tokio::test]
async fn service_context_checks_exact_peers_sessions_direction_and_operation_scope() {
    let h = Harness::new().await;
    let message = h.message("read", false);
    h.consumer.validate_message(&message, true).await.unwrap();
    h.service.validate_message(&message, false).await.unwrap();
    assert!(h
        .consumer
        .authorize_outgoing(&h.message("write", false))
        .is_err());
    assert!(h
        .consumer
        .authorize_outgoing(&h.message("changed", false))
        .is_err());
    h.consumer
        .validate_message(&h.message("changed", true), false)
        .await
        .unwrap();
    for (path, value) in [
        ("/senderSessionId", json!("wrong")),
        ("/recipientSessionId", json!("wrong")),
        ("/contract/generation", json!(2)),
        ("/body/serviceNamespace", json!("another")),
        ("/body/name", json!("unknown")),
        ("/body/intent", json!("command")),
    ] {
        let mut wire = serde_json::to_value(&message).unwrap();
        *wire.pointer_mut(path).unwrap() = value;
        let changed: DeckrMessage = serde_json::from_value(wire).unwrap();
        assert!(
            h.service.validate_message(&changed, false).await.is_err(),
            "{path}"
        );
    }
    let mut self_message = message.clone();
    self_message.recipient = MessageTarget::Endpoint {
        endpoint: message.sender.clone(),
    };
    self_message.recipient_session_id = Some(message.sender_session_id.clone());
    assert!(h.consumer.authorize_outgoing(&self_message).is_err());
    h.close().await;
}

#[tokio::test]
async fn reply_payload_validation_is_separate_from_fast_authority_checks() {
    let h = Harness::new().await;
    let request = h.message("read", false);
    let mut reply = h.message("read", true);
    reply.in_reply_to = Some(request.message_id.clone());
    reply.body["result"] = json!({"answer":42});
    h.service.validate_message(&reply, true).await.unwrap();
    h.consumer.validate_message(&reply, false).await.unwrap();
    reply.body["result"] = json!({"answer":"wrong"});
    h.consumer.authorize_incoming(&reply).unwrap();
    assert!(h.consumer.validate_message(&reply, false).await.is_err());
    reply.in_reply_to = None;
    assert!(h.consumer.authorize_incoming(&reply).is_err());
    h.close().await;
}

#[tokio::test]
async fn view_scope_checks_declared_writer_and_restricted_family_prefixes() {
    let h = Harness::new().await;
    let state = format!(
        "{}desk.lamp",
        h.consumer.descriptor().views["state"].key_prefix
    );
    let request = format!(
        "{}set",
        h.consumer.descriptor().views["requests"].key_prefix
    );
    h.consumer.authorize_view("state", &state, false).unwrap();
    assert!(h.consumer.authorize_view("state", &state, true).is_err());
    h.service.authorize_view("state", &state, true).unwrap();
    h.consumer
        .authorize_view("requests", &request, true)
        .unwrap();
    assert!(h
        .service
        .authorize_view("requests", &request, true)
        .is_err());
    assert!(h
        .consumer
        .authorize_view("state", &state.replace("desk.", "desk-other."), false)
        .is_err());
    let mut access = h.consumer.access().clone();
    access.service_session_id = "another-session".into();
    assert!(ServiceContext::new(
        h.proposal.agreement(),
        h.consumer.declaration().clone(),
        h.consumer.descriptor().clone(),
        access
    )
    .is_err());
    h.close().await;
}

#[tokio::test]
async fn service_profile_rejects_explicit_null_terms_even_when_generic_concord_is_valid() {
    let h = Harness::new().await;
    let contract = h.proposal.contract();
    let mut raw = h.contracts.get(&contract.key).await.unwrap().unwrap();
    for name in ["terms", "termsHash"] {
        raw.value[name] = Value::Null;
        assert!(validate_service_use_record(
            &raw.value,
            &h.consumer.descriptor().endpoint,
            h.consumer.consumer(),
            &h.consumer.descriptor().use_profile
        )
        .is_err());
        raw.value.as_object_mut().unwrap().remove(name);
    }
    raw.value["terms"] = Value::Null;
    h.contracts
        .update(&contract.key, raw.value, raw.revision, None)
        .await
        .unwrap();
    bounded(async {
        while h
            .proposal
            .agreement()
            .authorized_record()
            .unwrap()
            .value
            .get("terms")
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    h.proposal.authorize().unwrap();
    assert!(h.consumer.authorize().is_err());
    assert!(h.proposal.authorize().is_err());
    assert!(h.service.authorize().is_err());
    h.close().await;
}

#[tokio::test]
async fn ending_an_agreement_cancels_waiting_service_work_and_keeps_sources_open() {
    let h = Harness::new().await;
    let context = h.consumer.clone();
    let work = tokio::spawn(async move {
        context
            .while_current(std::future::pending::<Result<()>>())
            .await
    });
    tokio::task::yield_now().await;
    h.proposal.request_close();
    assert!(matches!(
        bounded(work).await.unwrap(),
        Err(Error::Unauthorized(_)) | Err(Error::Closed(_))
    ));
    assert!(h.consumer.authorize().is_err());
    assert!(h.runtime.concord().is_current());
    h.close().await;
}
