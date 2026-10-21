use deckr::action_runtime::*;
use deckr::lanes::DeckrMessage;
use serde_json::{json, Value};

fn target() -> Value {
    json!({"kind":"binding", "bindingId":"binding", "generation":3})
}
fn result() -> Value {
    json!({"target":target(), "requirementName":"display", "requestMessageId":"request", "stage":"controller", "status":"dispatched", "commandMessageId":"hardware"})
}
fn envelope() -> Value {
    json!({"messageId":"result", "lane":"services", "messageType":"serviceMessage", "sender":"controller:main", "senderSessionId":"controller-session",
        "recipient":{"targetType":"endpoint", "endpoint":"service:action-runtime.clock"}, "recipientSessionId":"runtime-session", "contract":{"contractId":"agreement", "generation":1},
        "subject":{"kind":"service", "identifiers":{"serviceId":"action-runtime.clock", "namespace":ACTION_RUNTIME_NAMESPACE, "name":"binding_render_result"}},
        "createdAt":"2026-10-21T10:00:00Z", "causationId":"request", "body":{"serviceNamespace":ACTION_RUNTIME_NAMESPACE, "name":"binding_render_result", "intent":"event", "exchangePattern":"one_way", "event":result()}})
}

#[test]
fn rendering_rejects_nulls_unknown_fields_and_invalid_status_correlations() {
    let valid = result();
    assert!(BindingRenderResultBody::from_value(valid.clone()).is_ok());
    for (key, value) in [
        ("code", json!("busy")),
        ("commandMessageId", Value::Null),
        ("stage", json!("hardware")),
        ("reason", Value::Null),
        ("unknown", json!(true)),
    ] {
        let mut invalid = valid.clone();
        invalid[key] = value;
        assert!(
            BindingRenderResultBody::from_value(invalid).is_err(),
            "{key}"
        );
    }
    let mut reason = valid.clone();
    reason["reason"] = json!("😀".repeat(512));
    assert!(BindingRenderResultBody::from_value(reason.clone()).is_ok());
    reason["reason"] = json!("😀".repeat(513));
    assert!(BindingRenderResultBody::from_value(reason).is_err());
    for invalid in [
        json!({"text":"hello", "font":"hidden"}),
        json!({"text":null}),
        json!({"text":"hello", "foreground":"red"}),
    ] {
        assert!(RenderText::from_value(invalid).is_err());
    }
    let set = json!({"target":target(), "requirementName":"display", "operation":"set", "content":{"format":RENDER_TEXT_FORMAT, "data":{"text":"hello"}}});
    assert!(BindingRenderBody::from_value(set.clone()).is_ok());
    let mut clear = set.clone();
    clear["operation"] = json!("clear");
    assert!(BindingRenderBody::from_value(clear).is_err());
    let mut invalid = set;
    invalid["target"] = json!({"kind":"page_session", "pageSessionId":"page", "generation":3});
    assert!(BindingRenderBody::from_value(invalid).is_err());
}

#[test]
fn extension_payloads_preserve_numbers_and_reserved_keys() {
    let raw = r#"{"format":"org.example.render.graph.v1","data":{"large":9007199254740993,"$serde_json::private::Number":"data","nested":[null,true]}}"#;
    let content = RenderContent::from_text(raw).unwrap();
    assert_eq!(content.data["large"].to_string(), "9007199254740993");
    assert_eq!(content.data["$serde_json::private::Number"], "data");
    assert_eq!(
        RenderContent::from_value(content.to_value().unwrap()).unwrap(),
        content
    );
    for format in [
        "dev.deckr.unknown.v1",
        "org.example.graph",
        "org.example.graph.v0",
        "org.example.graph.v01",
    ] {
        assert!(RenderContent::from_value(json!({"format":format,"data":{}})).is_err());
    }
}

#[test]
fn rendering_results_require_request_causation_and_preserve_received_size() {
    let value = envelope();
    let raw = serde_json::to_string_pretty(&value).unwrap();
    let message = DeckrMessage::from_text(&raw).unwrap();
    assert!(ActionMessage::from_envelope(&message).is_ok());
    assert_eq!(message.wire_size_bytes().unwrap(), raw.len());
    assert!(message.wire_size_bytes().unwrap() > message.to_text().unwrap().len());
    assert_eq!(
        message,
        DeckrMessage::from_text(&message.to_text().unwrap()).unwrap()
    );
    assert!(!message.to_text().unwrap().contains("receivedSize"));
    for (key, value) in [
        ("causationId", json!("another")),
        ("causationId", Value::Null),
        ("inReplyTo", json!("request")),
    ] {
        let mut invalid = envelope();
        invalid[key] = value;
        let message = DeckrMessage::from_text(&invalid.to_string()).unwrap();
        assert!(ActionMessage::from_envelope(&message).is_err());
    }
}
