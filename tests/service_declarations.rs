use deckr::beacon::{AdvertisementRecord, Candidate};
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use serde_json::{json, Value};

fn declaration() -> Value {
    json!({
        "namespace":"org.example.lights.v1", "featureId":"org.example.lights",
        "advertisementProfile":"org.example.lights.advertisement.v1", "useProfile":"org.example.lights.service_use.v1",
        "operations":{"read":{"description":"Read a selected light"},"set":{}},
        "messages":{
            "get_light":{"operation":"read","direction":"consumer_to_service","intent":"query","exchangePattern":"request_reply",
                "paramsSchema":{"schemaId":"params", "schema":{"type":"object","properties":{"light":{"type":"string"}},"required":["light"],"additionalProperties":false}},
                "resultSchema":{"schemaId":"result", "schema":{"type":"object","properties":{"on":{"type":"boolean"}},"required":["on"],"additionalProperties":false}},
                "errorSchema":{"schemaId":"error", "schema":{"type":"object","properties":{"code":{"const":"unavailable"}}}}
            },
            "changed":{"direction":"service_to_consumer","intent":"event","exchangePattern":"one_way",
                "eventSchema":{"schemaId":"event", "schema":{"type":"object","properties":{"on":{"type":"boolean"}},"required":["on"]}}},
            "set_light":{"operation":"set","direction":"bidirectional","intent":"command","exchangePattern":"one_way"}
        },
        "viewFamilies":{"lights":{"storeName":"example_lights","writer":"service"},"requests":{"storeName":"example_lights","writer":"consumer"}}
    })
}
fn request() -> ServiceMessageBody {
    ServiceMessageBody::from_value(json!({"serviceNamespace":"org.example.lights.v1","name":"get_light","intent":"query","exchangePattern":"request_reply","params":{"light":"desk"}})).unwrap()
}
fn candidate(protocol: &ServiceProtocol) -> Candidate {
    let payload = protocol
        .advertisement_payload(
            "lights-main",
            "service-session",
            ServiceBackendStatus::Available,
        )
        .unwrap();
    Candidate {
        key: "advertisements.test".into(), revision: 1,
        advertisement: AdvertisementRecord::from_value(json!({
            "featureId":protocol.feature_id, "advertiser":"service:lights-main",
            "endpoint":"service:lights-main", "sessionId":"service-session", "advertisementId":"test", "refreshSeq":1, "ttlSeconds":30,
            "payload":payload.to_value().unwrap()
        })).unwrap(),
    }
}

#[test]
fn declaration_round_trip_retains_messages_schemas_operations_and_view_writers() {
    let protocol = ServiceProtocol::from_value(declaration()).unwrap();
    assert_eq!(protocol.to_value().unwrap(), declaration());
    assert_eq!(
        protocol.operations["read"].description.as_deref(),
        Some("Read a selected light")
    );
    let payload = protocol
        .advertisement_payload(
            "lights-main",
            "service-session",
            ServiceBackendStatus::Degraded,
        )
        .unwrap();
    assert_eq!(payload.supported_messages, protocol.messages);
    assert_eq!(
        payload.views["requests"].writer,
        ServiceViewWriter::Consumer
    );
    assert_eq!(payload.views["lights"].writer, ServiceViewWriter::Service);
    assert_eq!(
        ServiceAdvertisementPayload::from_value(payload.to_value().unwrap()).unwrap(),
        payload
    );
    assert!(parse_service_descriptor(&candidate(&protocol), &protocol).is_some());
}

#[test]
fn declarations_require_operations_for_the_defined_directions_and_intents() {
    for direction in [
        "consumer_to_service",
        "service_to_consumer",
        "bidirectional",
    ] {
        for intent in ["command", "query", "event", "notification"] {
            let mut document = declaration();
            document["messages"] =
                json!({"m":{"direction":direction,"intent":intent,"exchangePattern":"one_way"}});
            let optional =
                direction == "service_to_consumer" && matches!(intent, "event" | "notification");
            assert_eq!(
                ServiceProtocol::from_value(document.clone()).is_ok(),
                optional,
                "{direction} {intent}"
            );
            document["messages"]["m"]["operation"] = json!("unknown");
            assert!(ServiceProtocol::from_value(document.clone()).is_err());
            document["messages"]["m"]["operation"] = json!("read");
            ServiceProtocol::from_value(document).unwrap();
        }
    }
    let mut notifications = declaration();
    notifications["operations"] = json!({});
    notifications["messages"] = json!({"ready":{"direction":"service_to_consumer","intent":"notification","exchangePattern":"one_way"}});
    let protocol = ServiceProtocol::from_value(notifications).unwrap();
    assert!(protocol
        .advertisement_payload("ready", "session", ServiceBackendStatus::Available)
        .unwrap()
        .supported_operations
        .is_empty());
}

#[test]
fn advertisements_cannot_replace_the_declarations_message_meaning_or_view_fences() {
    let protocol = ServiceProtocol::from_value(declaration()).unwrap();
    let original = candidate(&protocol);
    for (path, value) in [
        ("/supportedMessages/get_light/intent", json!("command")),
        (
            "/supportedMessages/get_light/exchangePattern",
            json!("one_way"),
        ),
        (
            "/supportedMessages/get_light/direction",
            json!("service_to_consumer"),
        ),
        ("/supportedMessages/get_light/operation", json!("set")),
        (
            "/supportedMessages/get_light/paramsSchema/schema",
            json!({"type":"object"}),
        ),
        ("/views/lights/writer", json!("consumer")),
        ("/views/lights/storeName", json!("wrong_store")),
        (
            "/views/lights/keyPrefix",
            json!("views.another_service.lights."),
        ),
        ("/serviceNamespace", json!("org.example.other")),
        ("/sessionId", json!("replacement")),
        ("/supportedOperations", json!(["read", "read"])),
    ] {
        let mut changed = original.clone();
        *changed
            .advertisement
            .payload
            .as_mut()
            .unwrap()
            .pointer_mut(path)
            .unwrap() = value;
        assert!(
            parse_service_descriptor(&changed, &protocol).is_none(),
            "{path}"
        );
    }
    let mut unknown = original.clone();
    unknown.advertisement.payload.as_mut().unwrap()["supportedMessages"]["unknown"] =
        json!({"direction":"service_to_consumer","intent":"event","exchangePattern":"one_way"});
    assert!(parse_service_descriptor(&unknown, &protocol).is_none());
    let mut reduced = original.clone();
    reduced.advertisement.payload.as_mut().unwrap()["supportedMessages"]
        .as_object_mut()
        .unwrap()
        .remove("set_light");
    reduced.advertisement.payload.as_mut().unwrap()["supportedOperations"] = json!(["read"]);
    assert!(parse_service_descriptor(&reduced, &protocol).is_some());
}

#[test]
fn supplied_payloads_use_the_selected_schema_and_replies_reverse_the_request_direction() {
    use ServiceMessageDirection::{ConsumerToService, ServiceToConsumer};
    use ServiceMessageKind::{Initial, Reply};
    let protocol = ValidatedServiceProtocol::new(
        ServiceProtocol::from_value(declaration()).unwrap(),
        &SchemaRegistry::default(),
    )
    .unwrap();
    let request = request();
    assert_eq!(
        protocol
            .validate_message(&request, ConsumerToService, Initial)
            .unwrap(),
        Some("read")
    );
    assert!(protocol
        .validate_message(&request, ServiceToConsumer, Initial)
        .is_err());
    let mut invalid = request.clone();
    invalid.params.insert("light".into(), json!(42));
    assert!(protocol
        .validate_message(&invalid, ConsumerToService, Initial)
        .is_err());
    invalid = request.clone();
    invalid.name = "undeclared".into();
    assert!(protocol
        .validate_message(&invalid, ConsumerToService, Initial)
        .is_err());
    let mut reply = request.clone();
    reply.params.clear();
    reply.result = Some(json!({"on":true}).as_object().unwrap().clone());
    reply.status = Some(ServiceMessageStatus::Ok);
    protocol
        .validate_reply(&request, &reply, ServiceToConsumer)
        .unwrap();
    assert!(protocol
        .validate_reply(&request, &reply, ConsumerToService)
        .is_err());
    reply
        .result
        .as_mut()
        .unwrap()
        .insert("on".into(), json!("yes"));
    assert!(protocol
        .validate_reply(&request, &reply, ServiceToConsumer)
        .is_err());
    reply.result = None;
    reply.status = Some(ServiceMessageStatus::Unavailable);
    reply.error = Some(ServiceError {
        code: "unavailable".into(),
        message: "temporarily absent".into(),
        diagnostics: Default::default(),
    });
    protocol
        .validate_reply(&request, &reply, ServiceToConsumer)
        .unwrap();
    reply.error.as_mut().unwrap().code = "wrong-error".into();
    assert!(protocol
        .validate_reply(&request, &reply, ServiceToConsumer)
        .is_err());
    let mut event = ServiceMessageBody::from_value(json!({"serviceNamespace":"org.example.lights.v1","name":"changed","intent":"event","exchangePattern":"one_way","event":{"on":false}})).unwrap();
    protocol
        .validate_message(&event, ServiceToConsumer, Initial)
        .unwrap();
    assert!(protocol
        .validate_message(&event, ServiceToConsumer, Reply)
        .is_err());
    event
        .event
        .as_mut()
        .unwrap()
        .insert("on".into(), json!("no"));
    assert!(protocol
        .validate_message(&event, ServiceToConsumer, Initial)
        .is_err());
}

#[test]
fn embedded_schemas_validate_their_dialect_and_resolve_only_explicit_resources() {
    let mut document = declaration();
    document["messages"]["get_light"]["paramsSchema"]["schema"] =
        json!({"$ref":"https://example.test/params"});
    let protocol = ServiceProtocol::from_value(document.clone()).unwrap();
    assert!(ValidatedServiceProtocol::new(protocol.clone(), &SchemaRegistry::default()).is_err());
    let registry = SchemaRegistry::default()
        .with_resource(
            "https://example.test/params",
            declaration()["messages"]["get_light"]["paramsSchema"]["schema"].clone(),
        )
        .unwrap();
    ValidatedServiceProtocol::new(protocol, &registry).unwrap();
    for schema in [
        json!({"type":"invented"}),
        json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":42}),
        json!({}),
        json!(true),
        json!({"description":"only documentation"}),
    ] {
        document["messages"]["get_light"]["paramsSchema"]["schema"] = schema;
        assert!(ServiceProtocol::from_value(document.clone()).is_err());
    }
}

#[test]
fn required_fields_unknown_fields_and_strict_application_json_are_preserved() {
    let protocol = ServiceProtocol::from_value(declaration()).unwrap();
    let payload = protocol
        .advertisement_payload("lights", "session", ServiceBackendStatus::Available)
        .unwrap()
        .to_value()
        .unwrap();
    for field in ["supportedMessages", "supportedOperations", "views"] {
        let mut bad = payload.clone();
        bad.as_object_mut().unwrap().remove(field);
        assert!(ServiceAdvertisementPayload::from_value(bad).is_err());
    }
    let mut bad = payload.clone();
    bad["unexpected"] = json!(true);
    assert!(ServiceAdvertisementPayload::from_value(bad).is_err());
    let mut bad = payload.clone();
    bad["views"]["lights"]
        .as_object_mut()
        .unwrap()
        .remove("writer");
    assert!(ServiceAdvertisementPayload::from_value(bad).is_err());
    let mut diagnostic = payload;
    diagnostic["diagnostics"] =
        json!({"application":{"$serde_json::private::Number":"ordinary value"}});
    let parsed = ServiceAdvertisementPayload::from_text(&diagnostic.to_string()).unwrap();
    assert_eq!(
        parsed.diagnostics["application"],
        diagnostic["diagnostics"]["application"]
    );
    let duplicate = declaration().to_string().replacen(
        "\"messages\":",
        "\"namespace\":\"duplicate\",\"messages\":",
        1,
    );
    assert!(ServiceProtocol::from_text(&duplicate).is_err());
}
