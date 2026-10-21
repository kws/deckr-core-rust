use std::collections::BTreeMap;

use deckr::action_runtime::*;
use deckr::hardware::validation::ValidatedDevice;
use deckr::lanes::{DeckrMessage, DeviceDescriptor, HardwareMessageBody};
use deckr::schema::SchemaRegistry;
use deckr::services::{ServiceIntent, ServiceMessageBody};
use serde_json::{json, Value};

fn instance() -> Value {
    json!({"providerInstanceId":"provider-1","providerId":"org.example.actions","actionId":"org.example.test","actionInstanceId":"instance-1","configId":"layout-1","contextId":"context-1"})
}
fn device_ref() -> Value {
    json!({"managerId":"manager","deviceId":"device","fingerprint":"fingerprint"})
}
fn capability(id: &str) -> Value {
    json!({"deviceRef":device_ref(),"controlId":"button-1","capabilityId":id})
}
fn binding_value() -> Value {
    let mut binding = instance();
    binding["bindingId"] = json!("binding-1");
    binding["deviceRef"] = device_ref();
    binding["controlRef"] = json!({"deviceRef":device_ref(),"controlId":"button-1"});
    binding["outputGeneration"] = json!(4);
    binding["matchedCapabilities"] = json!([
        {"capability":capability("button"),"family":"dev.deckr.input.button","type":"activation","direction":"input","requirementName":"activate","eventTypes":["press"]},
        {"capability":capability("lamp"),"family":"org.example.lamp","type":"brightness","direction":"output","requirementName":"light","commandTypes":["set"],"provenance":"extension"}
    ]);
    binding
}
fn binding() -> BindingMetadata {
    BindingMetadata::from_value(binding_value()).unwrap()
}
fn action() -> ActionDescriptor {
    ActionDescriptor::from_value(json!({"actionId":"org.example.test","requirements":[
        {"name":"activate","preferences":[{"family":"dev.deckr.input.button","type":"activation","direction":"input"}],"eventTypes":["press"],"views":["native"]},
        {"name":"light","preferences":[{"capabilityId":"lamp","family":"org.example.lamp","commandTypes":["set"]}],"commandTypes":["set"]}
    ]})).unwrap()
}
fn device() -> ValidatedDevice {
    let descriptor:DeviceDescriptor=serde_json::from_value(json!({
        "deviceId":"device","fingerprint":"fingerprint","displayName":"Test device","controls":[{
            "controlId":"button-1","kind":"button",
            "inputCapabilities":[{"capabilityId":"button","family":"dev.deckr.input.button","type":"activation","direction":"input","access":["emits"],"eventTypes":["press"]}],
            "outputCapabilities":[{"capabilityId":"lamp","family":"org.example.lamp","type":"brightness","direction":"output","access":["settable"],"commandTypes":["set"],"commandSchema":{"schema":{"type":"object","required":["level"],"properties":{"level":{"type":"integer","minimum":0,"maximum":10}},"additionalProperties":false}}}]
        }]
    })).unwrap();
    ValidatedDevice::new(descriptor, &SchemaRegistry::default()).unwrap()
}
fn availability() -> Value {
    json!({"providerInstanceId":"provider-1","providerId":"org.example.actions","serviceId":"action-runtime.provider-1","serviceEndpoint":"service:action-runtime.provider-1","serviceSessionId":"service-session","entries":[{"actionId":"org.example.test","status":"available","descriptor":{"actionId":"org.example.test"}}]})
}
fn output() -> BindingOutput {
    BindingOutput::from_value(json!({"binding":binding_value(),"capability":capability("lamp"),"commandType":"set","generation":4,"params":{"level":5}})).unwrap()
}

#[test]
fn availability_checks_identity_descriptors_and_restricted_provider_tokens() {
    let payload = ActionAvailabilityPayload::from_value(availability()).unwrap();
    assert_eq!(
        payload.service_endpoint,
        action_runtime_endpoint("provider-1").unwrap()
    );
    for invalid in [
        "",
        " with-space",
        "with space",
        "é",
        "-start",
        "bad/part",
        ":endpoint",
    ] {
        assert!(validate_provider_instance_id(invalid).is_err(), "{invalid}");
    }
    for valid in ["Main-1", "clock.2", "a_b-c"] {
        validate_provider_instance_id(valid).unwrap();
    }
    for field in [
        "providerInstanceId",
        "serviceId",
        "serviceEndpoint",
        "serviceSessionId",
    ] {
        let mut invalid = availability();
        invalid[field] = json!("wrong");
        if field == "serviceSessionId" {
            invalid[field] = json!("");
        }
        assert!(
            ActionAvailabilityPayload::from_value(invalid).is_err(),
            "{field}"
        );
    }
    let mut invalid = availability();
    invalid["entries"][0]["descriptor"]["actionId"] = json!("other");
    assert!(ActionAvailabilityPayload::from_value(invalid).is_err());
    let mut invalid = availability();
    invalid["entries"][0]["descriptor"] = Value::Null;
    assert!(ActionAvailabilityPayload::from_value(invalid.clone()).is_err());
    invalid["entries"][0]["status"] = json!("probing");
    ActionAvailabilityPayload::from_value(invalid).unwrap();
    let mut invalid = availability();
    invalid["labels"] = json!({" padded":"value"});
    assert!(ActionAvailabilityPayload::from_value(invalid).is_err());
    let mut duplicate = action().to_value().unwrap();
    let requirement = duplicate["requirements"][0].clone();
    duplicate["requirements"]
        .as_array_mut()
        .unwrap()
        .push(requirement);
    assert!(ActionDescriptor::from_value(duplicate).is_err());
}

#[test]
fn application_json_preserves_large_numbers_and_private_marker_keys_without_coercion() {
    let marker = json!({"$serde_json::private::Number":"application text"});
    let mut value = availability();
    value["extension"] = marker.clone();
    value["annotations"] = json!({"nested":marker.clone()});
    let payload = ActionAvailabilityPayload::from_text(&value.to_string()).unwrap();
    assert_eq!(payload.extra["extension"], marker);
    assert_eq!(payload.to_value().unwrap()["extension"], marker);
    let large=ActionDescriptor::from_text(r#"{"actionId":"test","manifestDefaults":{"large":184467440737095516170000001,"object":{"$serde_json::private::Number":"not a number"}}}"#).unwrap();
    assert_eq!(
        large.to_value().unwrap()["manifestDefaults"]["large"].to_string(),
        "184467440737095516170000001"
    );
    assert_eq!(
        large.to_value().unwrap()["manifestDefaults"]["object"],
        json!({"$serde_json::private::Number":"not a number"})
    );
    assert!(ActionDescriptor::from_text(r#"{"actionId":"a","actionId":"b"}"#).is_err());
    assert!(
        ActionDescriptor::from_text(r#"{"actionId":"a","manifestDefaults":{"bad":1e9999}}"#)
            .is_err()
    );
    assert!(ActionDescriptor::from_value(json!({"actionId":123})).is_err());
    let mut duplicate = payload;
    duplicate
        .extra
        .insert("serviceId".into(), json!("override"));
    assert!(duplicate.to_value().is_err());
}

fn message_values() -> Vec<(&'static str, Value)> {
    let mut page = instance();
    page.as_object_mut().unwrap().remove("actionId");
    page["pageId"] = json!("page");
    page["pageSessionId"] = json!("page-session");
    page["generation"] = json!(0);
    let dynamic = json!({"target":{"kind":"page_session","pageSessionId":"page-session","generation":0},"descriptor":{"pageId":"page","bindings":[{"controlId":"button-1","target":{"kind":"self"}}]}});
    vec![
        ("action_instance_created", json!({"metadata":instance()})),
        ("action_instance_destroyed", json!({"metadata":instance()})),
        (
            "binding_attached",
            json!({"binding":binding_value(),"settings":{"amount":2}}),
        ),
        (
            "binding_detached",
            json!({"binding":binding_value(),"reason":"removed"}),
        ),
        ("page_session_opened", json!({"pageSession":page.clone()})),
        ("page_session_closed", json!({"pageSession":page})),
        (
            "capability_input",
            json!({"binding":binding_value(),"event":{"capability":capability("button"),"eventType":"press","occurredAt":"2026-01-01T00:00:00Z","value":{"eventType":"press"}}}),
        ),
        (
            "action_lifecycle_rejected",
            json!({"targetKind":"action_instance","actionInstance":instance(),"reason":"action_not_available"}),
        ),
        ("binding_output", output().to_value().unwrap()),
        (
            "binding_overlay",
            json!({"binding":binding_value(),"generation":4,"template":"flash","durationSeconds":1.5}),
        ),
        (
            "binding_overlay_clear",
            json!({"binding":binding_value(),"generation":4}),
        ),
        ("open_page", dynamic.clone()),
        ("replace_page", dynamic),
        (
            "close_page",
            json!({"target":{"kind":"page_session","pageSessionId":"page-session","generation":0}}),
        ),
    ]
}
fn wire_body(name: &str, payload: Value) -> ServiceMessageBody {
    let event = !matches!(
        name,
        "binding_output"
            | "binding_overlay"
            | "binding_overlay_clear"
            | "open_page"
            | "replace_page"
            | "close_page"
    );
    let mut body = json!({"serviceNamespace":ACTION_RUNTIME_NAMESPACE,"name":name,"intent":if event{"event"}else{"command"},"exchangePattern":"one_way","params":{}});
    body[if event { "event" } else { "params" }] = payload;
    ServiceMessageBody::from_value(body).unwrap()
}
#[test]
fn core_messages_enforce_payload_positions_names_and_reply_exclusion() {
    for (name, payload) in message_values() {
        let body = wire_body(name, payload);
        let message = ActionMessage::from_body(&body).unwrap();
        assert_eq!(message.name(), name);
        assert_eq!(
            ActionMessage::from_body(&message.to_body().unwrap()).unwrap(),
            message
        );
        message
            .validate_provider("provider-1", "org.example.actions")
            .unwrap();
        let mut invalid = body.clone();
        invalid.result = Some(Default::default());
        assert!(ActionMessage::from_body(&invalid).is_err(), "{name}");
        let mut invalid = body.clone();
        invalid.name = "unknown".into();
        assert!(ActionMessage::from_body(&invalid).is_err());
        let mut invalid = body.clone();
        invalid.service_namespace = "other".into();
        assert!(ActionMessage::from_body(&invalid).is_err());
        let mut invalid = body.clone();
        if body.intent == ServiceIntent::Event {
            invalid.event = None;
        } else {
            invalid.event = Some(Default::default());
        }
        assert!(ActionMessage::from_body(&invalid).is_err());
        if !matches!(name, "open_page" | "replace_page" | "close_page") {
            assert!(message
                .validate_provider("other", "org.example.actions")
                .is_err());
            assert!(message.validate_provider("provider-1", "other").is_err());
        }
    }
    assert!(ActionMessage::from_body(&wire_body(
        "close_page",
        json!({"actionInstanceId":"invented"})
    ))
    .is_err());
    let rejected =
        json!({"targetKind":"binding","actionInstance":instance(),"reason":"internal_error"});
    assert!(ActionLifecycleRejected::from_value(rejected).is_err());
}
#[test]
fn action_envelopes_require_direct_controller_and_runtime_roles_and_target_sessions() {
    let body = wire_body("action_instance_created", json!({"metadata":instance()}));
    let value = json!({"messageId":"m1","protocolVersion":"1","schemaVersion":"1","lane":"services","messageType":"serviceMessage","sender":"controller:main","senderSessionId":"controller-session","recipient":{"targetType":"endpoint","endpoint":"service:action-runtime.provider-1"},"recipientSessionId":"service-session","contract":{"contractId":"service-use","generation":1},"createdAt":"2026-01-01T00:00:00Z","subject":{"kind":"service","identifiers":{"serviceId":"action-runtime.provider-1","namespace":ACTION_RUNTIME_NAMESPACE,"name":"action_instance_created"}},"body":body.to_value().unwrap()});
    let message = DeckrMessage::from_text(&value.to_string()).unwrap();
    ActionMessage::from_envelope(&message).unwrap();
    let mut invalid = message.clone();
    invalid.recipient_session_id = None;
    assert!(ActionMessage::from_envelope(&invalid).is_err());
    let mut invalid = message.clone();
    invalid.in_reply_to = Some("prior".into());
    assert!(ActionMessage::from_envelope(&invalid).is_err());
    let mut invalid = message.clone();
    invalid.sender = "hardware_manager:main".into();
    assert!(ActionMessage::from_envelope(&invalid).is_err());
    let mut invalid = message;
    invalid.recipient = deckr::lanes::MessageTarget::Endpoint {
        endpoint: "service:other".into(),
    };
    assert!(ActionMessage::from_envelope(&invalid).is_err());
}

#[test]
fn binding_resolution_checks_negotiated_resources_requirements_and_sources() {
    let current = binding();
    let action = action();
    let device = device();
    validate_binding_resources(&current, &action, "manager", &device).unwrap();
    for field in [
        "device",
        "control",
        "family",
        "requirement",
        "source",
        "provenance",
        "command",
    ] {
        let mut invalid = current.clone();
        match field {
            "device" => invalid.device_ref.device_id = "other".into(),
            "control" => invalid.control_ref.control_id = "other".into(),
            "family" => invalid.matched_capabilities[0].family = "org.example.wrong".into(),
            "requirement" => {
                invalid.matched_capabilities[0].requirement_name = Some("missing".into())
            }
            "source" => {
                invalid.matched_capabilities[0].source = Some(CapabilityRef {
                    device_ref: None,
                    control_id: None,
                    capability_id: "missing".into(),
                })
            }
            "provenance" => {
                invalid.matched_capabilities[0].provenance = CapabilityProvenance::Projection
            }
            _ => invalid.matched_capabilities[1]
                .command_types
                .push("unsupported".into()),
        };
        assert!(
            validate_binding_resources(&invalid, &action, "manager", &device).is_err(),
            "{field}"
        );
    }
    let mut missing = current.clone();
    missing.matched_capabilities.remove(0);
    assert!(validate_binding_resources(&missing, &action, "manager", &device).is_err());
    let mut optional = action.clone();
    optional.requirements.as_mut().unwrap()[0].required = false;
    assert!(validate_binding_resources(&missing, &optional, "manager", &device).is_err());
    optional.requirements.as_mut().unwrap()[0].preferences[0].capability_type =
        Some("momentary".into());
    validate_binding_resources(&missing, &optional, "manager", &device).unwrap();
    let mut mismatch = action;
    mismatch.requirements.as_mut().unwrap()[0].preferences[0].capability_type =
        Some("momentary".into());
    assert!(validate_binding_resources(&current, &mismatch, "manager", &device).is_err());
    assert!(CapabilityRequirementSelector::from_value(json!({})).is_err());
    assert!(
        CapabilityRequirementSelector::from_value(json!({"family":"dev.deckr.unknown"})).is_err()
    );
}
#[test]
fn outputs_and_overlays_require_current_binding_generations_and_valid_hardware_params() {
    let current = binding();
    let action = action();
    let device = device();
    let output = output();
    assert!(matches!(
        validate_binding_output(&output, &current, &action, "manager", &device).unwrap(),
        HardwareMessageBody::ControlCommand { .. }
    ));
    let mut unqualified_fingerprint = output.clone();
    unqualified_fingerprint
        .capability
        .device_ref
        .as_mut()
        .unwrap()
        .fingerprint = None;
    validate_binding_output(
        &unqualified_fingerprint,
        &current,
        &action,
        "manager",
        &device,
    )
    .unwrap();
    let mut stale = output.clone();
    stale.generation = 3;
    assert!(validate_binding_output(&stale, &current, &action, "manager", &device).is_err());
    let mut invented = output.clone();
    invented.binding.binding_id = "invented".into();
    assert!(validate_binding_output(&invented, &current, &action, "manager", &device).is_err());
    let mut crossing = output.clone();
    crossing.capability.device_ref.as_mut().unwrap().device_id = "other".into();
    assert!(validate_binding_output(&crossing, &current, &action, "manager", &device).is_err());
    let mut malformed = output;
    malformed.params.insert("level".into(), json!(100));
    assert!(validate_binding_output(&malformed, &current, &action, "manager", &device).is_err());
    let mut overlay = BindingOverlay::from_value(
        json!({"binding":binding_value(),"generation":4,"template":"flash"}),
    )
    .unwrap();
    validate_binding_overlay(&overlay, &current).unwrap();
    overlay.generation = 5;
    assert!(validate_binding_overlay(&overlay, &current).is_err());
    let clear =
        BindingOverlayClear::from_value(json!({"binding":binding_value(),"generation":3})).unwrap();
    assert!(validate_binding_overlay_clear(&clear, &current).is_err());
    let mut new_binding = current.clone();
    new_binding.output_generation = 5;
    assert!(validate_binding_reference(&current, &new_binding).is_err());
}

#[test]
fn input_and_output_preserve_configured_capability_preferences() {
    let mut descriptor = device().descriptor().clone();
    let control = &mut descriptor.controls[0];
    let mut preferred_button = control.input_capabilities[0].clone();
    preferred_button.capability_id = "preferred-button".into();
    control.input_capabilities.push(preferred_button);
    let mut preferred_lamp = control.output_capabilities[0].clone();
    preferred_lamp.capability_id = "preferred-lamp".into();
    control.output_capabilities.push(preferred_lamp);
    let device = ValidatedDevice::new(descriptor, &SchemaRegistry::default()).unwrap();
    let mut action = action();
    for (requirement, preferred_id) in action
        .requirements
        .as_mut()
        .unwrap()
        .iter_mut()
        .zip(["preferred-button", "preferred-lamp"])
    {
        let mut preferred = requirement.preferences[0].clone();
        preferred.capability_id = Some(preferred_id.into());
        requirement.preferences.insert(0, preferred);
    }
    let current = binding();
    let ids = ["button".into(), "lamp".into()];
    let scope = CapabilityMatchingScope {
        device: device.descriptor(),
        device_ref: &current.device_ref,
        control_id: &current.control_ref.control_id,
        configured_capability_ids: Some(&ids),
    };
    assert_eq!(
        match_capability_requirements(&action, &scope).unwrap(),
        current.matched_capabilities
    );
    validate_binding_resources_scoped(&current, &action, &scope).unwrap();
    let input = CapabilityInput::from_value(json!({
        "binding": binding_value(),
        "event": {
            "capability": capability("button"), "eventType": "press",
            "occurredAt": "2026-01-01T00:00:00Z", "view": "native",
            "value": {"eventType": "press"}
        }
    }))
    .unwrap();
    let output = output();
    let check_input = |input: &CapabilityInput, ids: Option<&[String]>| {
        validate_capability_input_scoped(input, &current, &action, "manager", &device, ids)
    };
    let check_output = |output: &BindingOutput, ids: Option<&[String]>| {
        validate_binding_output_scoped(output, &current, &action, "manager", &device, ids)
    };
    check_input(&input, Some(&ids)).unwrap();
    assert!(matches!(
        check_output(&output, Some(&ids)).unwrap(),
        HardwareMessageBody::ControlCommand { .. }
    ));
    assert!(validate_capability_input(&input, &current, &action, "manager", &device).is_err());
    assert!(validate_binding_output(&output, &current, &action, "manager", &device).is_err());
    for disallowed in [
        None,
        Some([].as_slice()),
        Some(["missing".into()].as_slice()),
    ] {
        assert!(check_input(&input, disallowed).is_err());
        assert!(check_output(&output, disallowed).is_err());
    }
    let mut invalid_input = input.clone();
    invalid_input.event.value = Some(json!({"eventType": "wrong"}));
    assert!(check_input(&invalid_input, Some(&ids)).is_err());
    let mut invalid_output = output.clone();
    invalid_output.generation = 3;
    assert!(check_output(&invalid_output, Some(&ids)).is_err());
    invalid_output = output.clone();
    invalid_output.params.insert("level".into(), json!(100));
    assert!(check_output(&invalid_output, Some(&ids)).is_err());
    invalid_output = output.clone();
    invalid_output.capability.capability_id = "preferred-lamp".into();
    assert!(check_output(&invalid_output, Some(&ids)).is_err());
    assert!(validate_binding_output_scoped(
        &output,
        &current,
        &action,
        "other-manager",
        &device,
        Some(&ids)
    )
    .is_err());
}

#[test]
fn input_events_match_the_bound_capability_and_validate_its_payload() {
    let current = binding();
    let action = action();
    let device = device();
    let mut input=CapabilityInput::from_value(json!({"binding":binding_value(),"event":{"capability":capability("button"),"eventType":"press","occurredAt":"2026-01-01T00:00:00Z","view":"native","value":{"eventType":"press"}}})).unwrap();
    validate_capability_input(&input, &current, &action, "manager", &device).unwrap();
    input.event.value = Some(json!({"eventType":"wrong"}));
    assert!(validate_capability_input(&input, &current, &action, "manager", &device).is_err());
    input.event.value = Some(json!({"eventType":"press"}));
    input.event.view = Some(CapabilityView::Projected);
    assert!(validate_capability_input(&input, &current, &action, "manager", &device).is_err());
    input.event.view = Some(CapabilityView::Raw);
    validate_capability_input(&input, &current, &action, "manager", &device).unwrap();
    let mut anonymous = current.clone();
    anonymous.matched_capabilities[0].requirement_name = None;
    input.binding = anonymous.clone();
    assert!(validate_capability_input(&input, &anonymous, &action, "manager", &device).is_err());
    let mut alternatives = action.clone();
    let mut raw_requirement = alternatives.requirements.as_ref().unwrap()[0].clone();
    raw_requirement.name = "raw-input".into();
    raw_requirement.views = vec![CapabilityView::Raw];
    alternatives
        .requirements
        .as_mut()
        .unwrap()
        .push(raw_requirement);
    let mut alternative_binding = current.clone();
    let mut raw_match = alternative_binding.matched_capabilities[0].clone();
    raw_match.requirement_name = Some("raw-input".into());
    alternative_binding.matched_capabilities.push(raw_match);
    input.binding = alternative_binding.clone();
    validate_capability_input(
        &input,
        &alternative_binding,
        &alternatives,
        "manager",
        &device,
    )
    .unwrap();
    input.binding = current.clone();
    input.event.view = None;
    input.event.event_type = "unsupported".into();
    assert!(validate_capability_input(&input, &current, &action, "manager", &device).is_err());
}

#[test]
fn projected_sources_must_match_negotiated_provenance_even_when_metadata_omits_source() {
    let mut descriptor = device().descriptor().clone();
    let mut projected = descriptor.controls[0].input_capabilities[0].clone();
    projected.capability_id = "projected".into();
    projected.projection = Some(deckr::lanes::CapabilityProjection {
        projection_type: "projection".into(),
        owner: "adapter".into(),
        source: deckr::lanes::DescriptorCapabilityRef {
            control_id: Some("button-1".into()),
            capability_id: "button".into(),
        },
        description: None,
    });
    descriptor.controls[0].input_capabilities.push(projected);
    let device = ValidatedDevice::new(descriptor, &SchemaRegistry::default()).unwrap();
    let mut action = action();
    action.requirements.as_mut().unwrap()[0].views = vec![CapabilityView::Projected];
    let mut current = binding();
    current.matched_capabilities[0].capability.capability_id = "projected".into();
    current.matched_capabilities[0].provenance = CapabilityProvenance::Projection;
    current.matched_capabilities[0].source = Some(CapabilityRef {
        device_ref: Some(serde_json::from_value(device_ref()).unwrap()),
        control_id: Some("button-1".into()),
        capability_id: "button".into(),
    });
    validate_binding_resources(&current, &action, "manager", &device).unwrap();
    let mut input=CapabilityInput::from_value(json!({"binding":current.to_value().unwrap(),"event":{"capability":capability("projected"),"source":capability("button"),"view":"projected","eventType":"press","occurredAt":"2026-01-01T00:00:00Z","value":{"eventType":"press"}}})).unwrap();
    validate_capability_input(&input, &current, &action, "manager", &device).unwrap();
    let mut omitted_source = current.clone();
    omitted_source.matched_capabilities[0].source = None;
    input.binding = omitted_source.clone();
    input.event.source = Some(CapabilityRef::from_value(capability("lamp")).unwrap());
    assert!(
        validate_capability_input(&input, &omitted_source, &action, "manager", &device).is_err()
    );
}
#[test]
fn dynamic_page_resolution_uses_owned_instances_and_provider_selectors() {
    let opener = ActionInstanceMetadata::from_value(instance()).unwrap();
    let value = json!({"pageId":"page","bindings":[{"controlId":"one","target":{"kind":"self"}},{"controlId":"two","target":{"kind":"action","actionId":"org.example.other","providerInstanceId":"other","providerLabels":{"room":"office"},"instanceKey":"stable"}}]});
    let command = DynamicPageCommand::from_value(value.clone()).unwrap();
    let mut selected = opener.clone();
    selected.provider_instance_id = "other".into();
    selected.action_id = "org.example.other".into();
    selected.action_instance_id = "controller-allocated-child".into();
    let mut children = vec![
        ResolvedPageChild {
            control_id: "one".into(),
            action: opener.clone(),
            provider_labels: BTreeMap::new(),
        },
        ResolvedPageChild {
            control_id: "two".into(),
            action: selected,
            provider_labels: BTreeMap::from([("room".into(), json!("office"))]),
        },
    ];
    validate_page_children(&command, &opener, &children).unwrap();
    children[0].action.action_instance_id = "invented".into();
    assert!(validate_page_children(&command, &opener, &children).is_err());
    children[0].action = opener.clone();
    children[1].provider_labels.clear();
    assert!(validate_page_children(&command, &opener, &children).is_err());
    let mut duplicate = value.clone();
    duplicate["bindings"][1]["controlId"] = json!("one");
    assert!(DynamicPageCommand::from_value(duplicate).is_err());
    let mut injected = value;
    injected["bindings"][1]["target"]["actionInstanceId"] = json!("provider-allocated");
    assert!(DynamicPageCommand::from_value(injected).is_err());
    assert!(
        PageChildBindingTarget::from_value(json!({"kind":"self","instanceKey":"injected"}))
            .is_err()
    );
    assert!(DynamicPageCommand::from_value(json!({"pageId":"empty","bindings":[]})).is_err());
}
#[test]
fn settings_targets_and_descriptions_preserve_identity_and_stable_key_layouts() {
    let target=SettingsTargetRef::from_value(json!({"scope":"action_instance","controllerId":"controller","configId":"config","providerInstanceId":"provider-1","providerId":"org.example.actions","actionId":"org.example.test","actionInstanceId":"instance","stableId":"page / é"})).unwrap();
    assert!(target
        .key()
        .unwrap()
        .ends_with(".instance.1.b64_cGFnZSAvIMOp"));
    let description = json!({"target":target.to_value().unwrap(),"providerInstanceId":"provider-1","providerId":"org.example.actions","actionId":"org.example.test"});
    SettingsTargetDescription::from_value(description.clone()).unwrap();
    let mut mismatch = description;
    mismatch["providerInstanceId"] = json!("other");
    assert!(SettingsTargetDescription::from_value(mismatch).is_err());
    let mut provider = target.to_value().unwrap();
    provider["scope"] = json!("action_provider_instance");
    assert!(SettingsTargetRef::from_value(provider).is_err());
    assert!(SettingsSnapshot::from_value(json!({"target":target.to_value().unwrap()})).is_err());
}
