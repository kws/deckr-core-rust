use deckr::hardware::validation::ValidatedDevice;
use deckr::lanes::{CapabilitySchema, ControlGeometry, DeviceDescriptor};
use deckr::lanes::{DeviceRef, HardwareMessageBody};
use deckr::schema::{validate_schema, SchemaRegistry};
use serde_json::{json, Value};

fn descriptor() -> Value {
    json!({
        "deviceId": "deck", "fingerprint": "revision-1", "displayName": "Desk panel",
        "modelId": "panel-1", "hardwareVersion": "2", "firmwareVersion": "3",
        "identifiers": [{"type":"serial", "value":"123", "issuer":"manufacturer"}],
        "connections": [{"connectionId":"usb-1", "type":"usb", "transport":"usb", "facts":{"port":2}}],
        "sources": [{"sourceId":"device-source", "type":"device", "connectionId":"usb-1"}],
        "defaultStatusIndicator": {"controlId":"screen", "capabilityId":"image"},
        "controls": [
            {"controlId":"screen", "kind":"screen", "groupId":"main", "surfaceId":"front",
             "geometry":{"x":1, "y":0, "rotation":90, "layer":2},
             "outputCapabilities":[{"capabilityId":"image", "family":"dev.deckr.output.raster", "type":"bitmap", "direction":"output", "access":["settable"], "commandTypes":["set_frame","clear"]}],
             "stateCapabilities":[{"capabilityId":"temperature", "family":"org.example.temperature", "type":"sensor", "direction":"state", "access":["readable"], "valueSchema":{"schema":{"type":"number"}}, "units":[{"subject":"value","unit":"celsius"}], "sources":[{"sourceId":"sensor", "type":"sensor", "connectionId":"usb-1"}]}],
             "configCapabilities":[{"capabilityId":"calibrate", "family":"org.example.calibration", "type":"sensor", "direction":"command", "access":["invokable"], "commandTypes":["reset"]}],
             "diagnosticCapabilities":[{"capabilityId":"health", "family":"org.example.health", "type":"status", "direction":"state", "access":["readable"]}]},
            {"controlId":"button", "kind":"button", "parentControlId":"screen", "relatedControlIds":["screen"],
             "inputCapabilities":[{"capabilityId":"press", "family":"dev.deckr.input.button", "type":"activation", "direction":"input", "access":["emits"], "eventTypes":["press"], "projection":{"owner":"adapter", "source":{"controlId":"screen", "capabilityId":"temperature"}}}]}
        ]
    })
}

fn accepts(value: Value) -> bool {
    serde_json::from_value::<DeviceDescriptor>(value).is_ok_and(|device| device.validate().is_ok())
}

#[test]
fn hardware_profiles_validate_nested_claim_references_and_reject_unknown_fields() {
    use deckr::profiles::hardware::{HardwareBeaconPayload, HardwareClaimTerms};
    let claim = json!({"claimId":"claim", "controllerEndpoint":"controller:main", "managerEndpoint":"hardware_manager:main", "devices":[{"deviceRef":{"managerId":"main", "deviceId":"deck"}, "instanceCount":1}]});
    HardwareClaimTerms::from_value(claim.clone()).unwrap();
    let mut invalid = claim.clone();
    invalid["devices"][0]["deviceRef"]["deviceId"] = json!("");
    assert!(HardwareClaimTerms::from_value(invalid).is_err());
    let mut invalid = claim;
    invalid["devices"][0]["unrecognizedPolicy"] = json!(true);
    assert!(HardwareClaimTerms::from_value(invalid).is_err());

    let advertisement = json!({"managerId":"main", "managerEndpoint":"hardware_manager:main", "sessionId":"session", "devices":{"deck":{"deviceRef":{"managerId":"main","deviceId":"deck","fingerprint":"revision-1"},"descriptor":descriptor()}}});
    HardwareBeaconPayload::from_value(advertisement.clone()).unwrap();
    let mut invalid = advertisement.clone();
    invalid["devices"]["deck"]["capacity"] = json!({"unrecognizedPolicy":true});
    assert!(HardwareBeaconPayload::from_value(invalid).is_err());
    let mut invalid = advertisement;
    invalid["devices"]["deck"]["deviceRef"]["fingerprint"] = json!("different");
    assert!(HardwareBeaconPayload::from_value(invalid).is_err());
}

#[test]
fn descriptors_preserve_all_collections_relationships_and_schema_defaults() {
    let value = descriptor();
    let device: DeviceDescriptor = serde_json::from_value(value.clone()).unwrap();
    device.validate().unwrap();
    assert_eq!(device.connections[0].status, "available");
    assert_eq!(device.controls[0].geometry.as_ref().unwrap().unit, "grid");
    assert_eq!(device.controls[0].state_capabilities[0].units[0].scale, 1.0);
    assert_eq!(
        device.controls[1].input_capabilities[0]
            .projection
            .as_ref()
            .unwrap()
            .projection_type,
        "projection"
    );
    assert_eq!(
        device
            .capability(Some("screen"), "calibrate")
            .unwrap()
            .direction,
        "command"
    );
    assert!(device.capability(None, "calibrate").is_none());
    let serialized = serde_json::to_value(&device).unwrap();
    for field in [
        "modelId",
        "hardwareVersion",
        "firmwareVersion",
        "identifiers",
        "sources",
        "defaultStatusIndicator",
    ] {
        assert_eq!(serialized[field], value[field], "{field}");
    }
    let round_trip: DeviceDescriptor = serde_json::from_value(serialized).unwrap();
    assert_eq!(device, round_trip);
    assert_eq!(
        serde_json::from_value::<ControlGeometry>(json!({})).unwrap(),
        ControlGeometry::default()
    );
}

#[test]
fn descriptor_references_and_collection_directions_are_checked() {
    for (pointer, replacement) in [
        ("/sources/0/connectionId", json!("unknown")),
        (
            "/controls/0/stateCapabilities/0/sources/0/connectionId",
            json!("unknown"),
        ),
        ("/controls/1/parentControlId", json!("button")),
        ("/controls/1/relatedControlIds", json!(["unknown"])),
        (
            "/controls/1/inputCapabilities/0/projection/source/controlId",
            json!("unknown"),
        ),
        (
            "/controls/1/inputCapabilities/0/projection/source",
            json!({"controlId":"button", "capabilityId":"press"}),
        ),
        ("/defaultStatusIndicator/capabilityId", json!("temperature")),
        (
            "/controls/0/diagnosticCapabilities/0/capabilityId",
            json!("temperature"),
        ),
        ("/controls/0/configCapabilities/0/direction", json!("state")),
        ("/controls/0/stateCapabilities/0/units/0/scale", json!(0)),
        ("/controls/0/outputCapabilities/0/direction", json!("state")),
    ] {
        let mut value = descriptor();
        // Some negative cases add an optional field rather than replacing one.
        if pointer.ends_with("/scale") {
            value["controls"][0]["stateCapabilities"][0]["units"][0]["scale"] = replacement;
        } else {
            *value.pointer_mut(pointer).unwrap() = replacement;
        }
        assert!(!accepts(value), "accepted {pointer}");
    }
    let mut duplicate = descriptor();
    let connection = duplicate["connections"][0].clone();
    duplicate["connections"]
        .as_array_mut()
        .unwrap()
        .push(connection);
    assert!(!accepts(duplicate));
    let mut unknown = descriptor();
    unknown["controls"][0]["futureField"] = json!(true);
    assert!(!accepts(unknown));
}

#[test]
fn embedded_schema_dialects_and_offline_reference_resolution_are_enforced() {
    let invalid = CapabilitySchema {
        schema_id: None,
        schema: json!({"type":"number", "minimum":"zero"}),
    };
    assert!(invalid.validate().is_err());
    assert!(validate_schema(
        &json!({"$schema":"https://example.test/unknown-dialect", "type":"string"})
    )
    .is_err());
    let registry = SchemaRegistry::default().with_resource("https://example.test/count", json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema", "type":"integer", "minimum":1
    })).unwrap();
    let schema = json!({"type":"object", "properties":{"count":{"$ref":"https://example.test/count"}}, "required":["count"], "additionalProperties":false});
    assert!(SchemaRegistry::default().compile(&schema).is_err());
    let compiled = registry.compile(&schema).unwrap();
    compiled.validate(&json!({"count":2})).unwrap();
    assert!(compiled.validate(&json!({"count":0})).is_err());
    assert!(compiled.validate(&json!({"count":2,"extra":true})).is_err());
    assert!(registry
        .clone()
        .with_resource("https://example.test/count", json!({"type":"string"}))
        .is_err());
    assert!(registry
        .compile(&json!({"$ref":"file:///definitely-not-a-schema"}))
        .is_err());
    assert!(registry
        .compile(&json!({"$ref":"https://example.test/missing"}))
        .is_err());
    let old_draft = registry.compile(&json!({"$schema":"http://json-schema.org/draft-04/schema#", "type":"number", "minimum":0, "exclusiveMinimum":true})).unwrap();
    assert!(old_draft.validate(&json!(0)).is_err());
    old_draft.validate(&json!(1)).unwrap();
}

fn command(params: Value) -> HardwareMessageBody {
    HardwareMessageBody::ControlCommand {
        device_ref: DeviceRef {
            manager_id: "manager".into(),
            device_id: "deck".into(),
            fingerprint: Some("revision-1".into()),
        },
        control_id: Some("screen".into()),
        capability_id: "image".into(),
        command_type: "set_frame".into(),
        params: params.as_object().unwrap().clone(),
    }
}

#[test]
fn raster_validation_checks_decoding_dimensions_and_selected_command() {
    use base64::Engine;
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(2, 3)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    let image = base64::engine::general_purpose::STANDARD.encode(bytes.into_inner());
    let mut value = descriptor();
    value["controls"][0]["outputCapabilities"][0]["commandSchema"] = json!({"schema":{"$ref":"https://kws.github.io/deckr/contract/v1/schemas/deckr/hardware/capabilities/raster-bitmap-command-params.v1.schema.json"}});
    value["controls"][0]["outputCapabilities"][0]["constraints"] = json!([{"type":"fixed", "subject":"width", "value":2}, {"type":"fixed", "subject":"height", "value":3}]);
    let device = ValidatedDevice::new(
        serde_json::from_value(value).unwrap(),
        &SchemaRegistry::default(),
    )
    .unwrap();
    device
        .validate_message(&command(json!({"image":image,"encoding":"png"})))
        .unwrap();
    for params in [
        json!({"image":image,"encoding":"jpeg"}),
        json!({"image":image,"encoding":"png","width":3}),
        json!({"image":"not base64","encoding":"png"}),
        json!({"image":"iVBORw0KGgo=","encoding":"png"}),
        json!({}),
    ] {
        assert!(device.validate_message(&command(params)).is_err());
    }
    let mut clear = command(json!({}));
    if let HardwareMessageBody::ControlCommand { command_type, .. } = &mut clear {
        *command_type = "clear".into();
    }
    device.validate_message(&clear).unwrap();
    let mut invalid_clear = command(json!({"image":image,"encoding":"png"}));
    if let HardwareMessageBody::ControlCommand { command_type, .. } = &mut invalid_clear {
        *command_type = "clear".into();
    }
    assert!(device.validate_message(&invalid_clear).is_err());
}

#[test]
fn extension_state_values_obey_schema_units_and_constraints() {
    let mut value = descriptor();
    let capability = &mut value["controls"][0]["stateCapabilities"][0];
    capability["access"] = json!(["readable", "emits"]);
    capability["units"][0]["scale"] = json!(0.1);
    capability["constraints"] =
        json!([{"type":"range", "subject":"value", "minimum":0, "maximum":10, "unit":"celsius"}]);
    let device = ValidatedDevice::new(
        serde_json::from_value(value).unwrap(),
        &SchemaRegistry::default(),
    )
    .unwrap();
    let mut state = HardwareMessageBody::CapabilityStateChanged {
        device_ref: DeviceRef {
            manager_id: "manager".into(),
            device_id: "deck".into(),
            fingerprint: None,
        },
        control_id: Some("screen".into()),
        capability_id: "temperature".into(),
        state_type: None,
        value: Some(json!(50)),
        occurred_at: None,
        sequence: None,
    };
    device.validate_message(&state).unwrap();
    for invalid in [json!(150), json!("warm")] {
        if let HardwareMessageBody::CapabilityStateChanged { value, .. } = &mut state {
            *value = Some(invalid);
        }
        assert!(device.validate_message(&state).is_err());
    }
}

#[test]
fn numeric_checks_preserve_large_integers_and_exact_decimal_steps() {
    let mut value = descriptor();
    let capability = &mut value["controls"][0]["stateCapabilities"][0];
    capability["access"] = json!(["readable", "emits"]);
    capability["units"] = json!([]);
    capability["constraints"] =
        json!([{"type":"range", "subject":"value", "maximum":9007199254740992u64}]);
    let device = ValidatedDevice::new(
        serde_json::from_value(value.clone()).unwrap(),
        &SchemaRegistry::default(),
    )
    .unwrap();
    let mut state = HardwareMessageBody::CapabilityStateChanged {
        device_ref: DeviceRef {
            manager_id: "manager".into(),
            device_id: "deck".into(),
            fingerprint: None,
        },
        control_id: Some("screen".into()),
        capability_id: "temperature".into(),
        state_type: None,
        value: Some(json!(9007199254740993u64)),
        occurred_at: None,
        sequence: None,
    };
    assert!(device.validate_message(&state).is_err());
    value["controls"][0]["stateCapabilities"][0]["constraints"] =
        json!([{"type":"range", "subject":"value", "step":0.1}]);
    let device = ValidatedDevice::new(
        serde_json::from_value(value).unwrap(),
        &SchemaRegistry::default(),
    )
    .unwrap();
    if let HardwareMessageBody::CapabilityStateChanged { value, .. } = &mut state {
        *value = Some(json!(0.3));
    }
    device.validate_message(&state).unwrap();
    if let HardwareMessageBody::CapabilityStateChanged { value, .. } = &mut state {
        *value = Some(json!(0.31));
    }
    assert!(device.validate_message(&state).is_err());

    let mut value = descriptor();
    let capability = &mut value["controls"][1]["inputCapabilities"][0];
    capability["family"] = json!("dev.deckr.input.encoder");
    capability["type"] = json!("relative");
    capability["eventTypes"] = json!(["rotate"]);
    let device = ValidatedDevice::new(
        serde_json::from_value(value).unwrap(),
        &SchemaRegistry::default(),
    )
    .unwrap();
    for (number, valid) in [
        ("2.0", true),
        ("1.00000000000000000001", false),
        ("100000000000000000000000000000000000000000000000001", true),
        ("1e100001", false),
    ] {
        let delta: Value = serde_json::from_str(number).unwrap();
        let input = HardwareMessageBody::ControlInput {
            device_ref: DeviceRef {
                manager_id: "manager".into(),
                device_id: "deck".into(),
                fingerprint: None,
            },
            control_id: "button".into(),
            capability_id: "press".into(),
            event_type: "rotate".into(),
            value: Some(json!({"delta":delta})),
            occurred_at: None,
            sequence: None,
            sources: vec![],
        };
        assert_eq!(device.validate_message(&input).is_ok(), valid, "{number}");
    }
}
