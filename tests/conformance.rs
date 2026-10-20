//! Conformance cases consume artifacts from the separate kws/deckr checkout.

use std::collections::BTreeMap;
use std::path::PathBuf;

use deckr::beacon::{beacon_advertisement_key, AdvertisementRecord};
use deckr::canonical_json::{canonical_json_bytes_value, canonical_json_hash_value};
use deckr::concord::{
    concord_contract_key, concord_participant_token_key, ContractRecord, ParticipantTokenRecord,
};
use deckr::endpoint::EndpointAddress;
use deckr::keys::{decode_key_token, encode_key_token};
use deckr::lanes::{headers_for, subject_for, DeckrMessage};
use deckr::profiles::hardware::{
    hardware_payload_from_advertisement, HardwareBeaconPayload, HardwareClaimTerms,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

#[path = "support/action_boundary.rs"]
mod action_boundary;

#[test]
fn action_runtime_semantic_vectors() {
    use deckr::action_runtime::*;
    use deckr::lanes::{DeviceDescriptor, DeviceRef};
    let vectors: Value = artifact("vectors/action-runtime-semantics.v1.json");
    for case in vectors["cases"].as_array().unwrap() {
        let context = &vectors["contexts"][case["contextRef"].as_str().unwrap()];
        let result = (|| -> deckr::Result<()> {
            match case["rule"].as_str().unwrap() {
                "SEM-ACT-002" => {
                    let action = ActionDescriptor::from_value(context["actionDescriptor"].clone())?;
                    let binding = BindingMetadata::from_value(case["instance"].clone())?;
                    let device: DeviceDescriptor =
                        serde_json::from_value(context["deviceDescriptor"].clone())?;
                    let device_ref: DeviceRef =
                        serde_json::from_value(context["deviceRef"].clone())?;
                    let ids: Vec<String> =
                        serde_json::from_value(context["configuredCapabilityIds"].clone())?;
                    let scope = CapabilityMatchingScope {
                        device: &device,
                        device_ref: &device_ref,
                        control_id: context["controlId"].as_str().unwrap(),
                        configured_capability_ids: Some(&ids),
                    };
                    validate_binding_resources_scoped(&binding, &action, &scope)?;
                    let mut generated = binding.clone();
                    generated.matched_capabilities =
                        match_capability_requirements(&action, &scope)?;
                    validate_binding_resources_scoped(&generated, &action, &scope)
                }
                "SEM-ACT-003" => {
                    let message = DeckrMessage::from_text(&case["instance"].to_string())?;
                    let context = serde_json::from_value(context.clone())?;
                    validate_page_command(&message, &context).map(|_| ())
                }
                "SEM-ACT-005" => ActionMessage::from_envelope(&DeckrMessage::from_text(
                    &case["instance"].to_string(),
                )?)
                .map(|_| ()),
                rule => panic!("unknown Action Runtime semantic rule {rule}"),
            }
        })();
        assert_eq!(
            result.is_ok(),
            case["semanticValid"].as_bool().unwrap(),
            "{}: {result:?}",
            case["id"]
        );
    }
}

#[test]
fn maintenance_records_match_upstream_examples() {
    use deckr::concord::{StaleObservationRecord, TokenCleanupRecord};
    for (directory, valid) in [("valid", true), ("invalid", false)] {
        let stale = StaleObservationRecord::from_text(&artifact_text(&format!(
            "fixtures/{directory}/coverage/protocols/concord/stale-observation.v1.json"
        )));
        assert_eq!(stale.is_ok(), valid);
        let cleanup = TokenCleanupRecord::from_text(&artifact_text(&format!(
            "fixtures/{directory}/coverage/protocols/concord/token-cleanup.v1.json"
        )));
        assert_eq!(cleanup.is_ok(), valid);
        if valid {
            let stale = stale.unwrap();
            assert_eq!(
                StaleObservationRecord::from_value(stale.to_value().unwrap()).unwrap(),
                stale
            );
            let cleanup = cleanup.unwrap();
            assert_eq!(
                TokenCleanupRecord::from_value(cleanup.to_value().unwrap()).unwrap(),
                cleanup
            );
        }
    }
}

#[derive(Debug, Deserialize)]
struct KeyTokenVectors {
    cases: Vec<KeyTokenCase>,
}

#[derive(Debug, Deserialize)]
struct KeyTokenCase {
    raw: String,
    token: String,
}

#[test]
fn key_token_vectors_match_contract_artifacts() {
    let vectors: KeyTokenVectors = artifact("vectors/key-tokens.v1.json");
    for case in vectors.cases {
        assert_eq!(encode_key_token(&case.raw), case.token);
        assert_eq!(decode_key_token(&case.token).unwrap(), case.raw);
    }
}

#[derive(Debug, Deserialize)]
struct BeaconConcordKeyVectors {
    cases: Vec<BeaconConcordKeyCase>,
}

#[derive(Debug, Deserialize)]
struct BeaconConcordKeyCase {
    helper: String,
    input: Value,
    key: String,
}

#[test]
fn beacon_concord_key_vectors_match_contract_artifacts() {
    let vectors: BeaconConcordKeyVectors = artifact("vectors/beacon-concord-keys.v1.json");
    for case in vectors.cases {
        let actual = match case.helper.as_str() {
            "beacon_advertisement_key" => beacon_advertisement_key(
                case.input["featureId"].as_str().unwrap(),
                case.input["advertisementId"].as_str().unwrap(),
            ),
            "concord_contract_key" => concord_contract_key(
                case.input["contractId"].as_str().unwrap(),
                case.input["generation"].as_u64().unwrap(),
            ),
            "concord_participant_token_key" => concord_participant_token_key(
                case.input["contractId"].as_str().unwrap(),
                case.input["generation"].as_u64().unwrap(),
                &EndpointAddress::parse(case.input["participant"].as_str().unwrap()).unwrap(),
            ),
            other => panic!("unknown key helper {other}"),
        };
        assert_eq!(actual, case.key);
    }
}

#[derive(Debug, Deserialize)]
struct TermsHashVectors {
    cases: Vec<TermsHashCase>,
}

#[derive(Debug, Deserialize)]
struct TermsHashCase {
    id: String,
    #[serde(rename = "inputJson")]
    input_json: String,
    #[serde(rename = "canonicalJson")]
    canonical_json: String,
    hash: String,
}

#[test]
fn concord_terms_hash_vectors_match_contract_artifacts() {
    let vectors: TermsHashVectors = artifact("vectors/concord-terms-hash.v1.json");
    for case in vectors.cases {
        let value: Value = serde_json::from_str(&case.input_json).unwrap();
        assert_eq!(
            String::from_utf8(canonical_json_bytes_value(&value).unwrap()).unwrap(),
            case.canonical_json,
            "{}",
            case.id
        );
        assert_eq!(
            canonical_json_hash_value(&value).unwrap(),
            case.hash,
            "{}",
            case.id
        );
    }
}

#[derive(Debug, Deserialize)]
struct NatsLaneVectors {
    cases: Vec<NatsLaneCase>,
}

#[derive(Debug, Deserialize)]
struct NatsLaneCase {
    fixture: String,
    subject: String,
    headers: BTreeMap<String, String>,
}

#[test]
fn nats_lane_vectors_match_contract_artifacts() {
    let vectors: NatsLaneVectors = artifact("vectors/nats-lane.v1.json");
    for case in vectors.cases {
        let fixture = artifact_text(&case.fixture);
        let message = DeckrMessage::from_text(&fixture).unwrap();
        assert_eq!(subject_for(&message).unwrap(), case.subject);
        let actual_headers = headers_for(&message);
        for (key, value) in case.headers {
            assert_eq!(actual_headers.get(&key), Some(&value), "{key}");
        }
    }
}

#[test]
fn contract_fixtures_parse_and_enforce_profile_semantics() {
    let hardware_ad: Value = artifact("fixtures/valid/beacon/hardware-advertisement.v1.json");
    let hardware_ad = AdvertisementRecord::from_value(hardware_ad).unwrap();
    hardware_payload_from_advertisement(&hardware_ad).unwrap();

    let hardware_payload: Value = artifact("fixtures/valid/profiles/hardware.v1.json");
    HardwareBeaconPayload::from_value(hardware_payload).unwrap();

    let hardware_claim: Value = artifact("fixtures/valid/profiles/hardware-claim.v1.json");
    HardwareClaimTerms::from_value(hardware_claim).unwrap();

    let contract: Value = artifact("fixtures/valid/concord/hardware-claim-contract.v1.json");
    ContractRecord::from_value(contract).unwrap();

    let token: Value = artifact("fixtures/valid/concord/hardware-claim-token.v1.json");
    ParticipantTokenRecord::from_value(token).unwrap();

    let invalid_hardware_payload: Value =
        artifact("fixtures/invalid/profiles/hardware-missing-manager.v1.json");
    assert!(HardwareBeaconPayload::from_value(invalid_hardware_payload).is_err());

    let invalid_token: Value =
        artifact("fixtures/invalid/concord/token-missing-participant.v1.json");
    assert!(ParticipantTokenRecord::from_value(invalid_token).is_err());

    let invalid_message =
        artifact_text("fixtures/invalid/hardware/control-input-missing-device-ref.v1.json");
    assert!(DeckrMessage::from_text(&invalid_message).is_err());
}

#[test]
fn strict_records_reject_non_object_extension_payloads() {
    let mut advertisement: Value = artifact("fixtures/valid/beacon/hardware-advertisement.v1.json");
    advertisement["payload"] = json!("not-an-object");
    assert!(AdvertisementRecord::from_value(advertisement).is_err());

    let mut contract: Value = artifact("fixtures/valid/concord/hardware-claim-contract.v1.json");
    contract["terms"] = json!("not-an-object");
    assert!(ContractRecord::from_value(contract).is_err());
}

#[test]
fn neutral_beacon_and_concord_fixtures_do_not_require_deckr_endpoints() {
    let advertisement: Value =
        artifact("fixtures/valid/protocols/beacon/non-deckr-advertisement.v1.json");
    let advertisement = AdvertisementRecord::from_value(advertisement).unwrap();
    assert_eq!(advertisement.endpoint.family(), "worker");
    assert!(EndpointAddress::parse(advertisement.endpoint.as_str()).is_err());
    let contract: Value = artifact("fixtures/valid/protocols/concord/non-deckr-contract.v1.json");
    let contract = ContractRecord::from_value(contract).unwrap();
    assert!(contract
        .participants
        .iter()
        .any(|endpoint| endpoint.family() == "scheduler"));
}

fn artifact_path(path: &str) -> PathBuf {
    let spec_dir = std::env::var_os("DECKR_SPEC_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../deckr"));
    spec_dir.join("contract/v1").join(path)
}

#[test]
fn messaging_models_follow_upstream_coverage_examples() {
    for name in [
        "messaging/message",
        "services/message",
        "hardware/messages",
        "hardware/messages/control-input",
        "hardware/messages/control-command",
        "hardware/messages/command-accepted",
        "hardware/messages/command-rejected",
        "hardware/messages/command-reply",
        "hardware/messages/capability-state-request",
        "hardware/messages/capability-state-reply",
        "hardware/messages/capability-state-changed",
    ] {
        for valid in [true, false] {
            let path = format!(
                "fixtures/{}/coverage/deckr/{name}.v1.json",
                if valid { "valid" } else { "invalid" }
            );
            assert_eq!(
                DeckrMessage::from_text(&artifact_text(&path)).is_ok(),
                valid,
                "{path}"
            );
        }
    }
    for name in ["endpoint-target", "broadcast-target"] {
        for valid in [true, false] {
            let path = format!(
                "fixtures/{}/coverage/deckr/messaging/{name}.v1.json",
                if valid { "valid" } else { "invalid" }
            );
            let accepted =
                serde_json::from_str::<deckr::lanes::MessageTarget>(&artifact_text(&path))
                    .is_ok_and(|target| target.validate().is_ok());
            assert_eq!(accepted, valid, "{path}");
        }
    }
}

#[test]
fn hardware_descriptor_models_follow_upstream_coverage_examples() {
    use deckr::lanes::*;
    use deckr::profiles::hardware::*;
    macro_rules! check {
        ($kind:ty, $name:literal) => {
            for valid in [true, false] {
                let path = format!(
                    "fixtures/{}/coverage/deckr/hardware/{}.v1.json",
                    if valid { "valid" } else { "invalid" },
                    $name
                );
                let text = artifact_text(&path);
                let accepted = serde_json::from_str::<$kind>(&text)
                    .is_ok_and(|value| value.validate().is_ok());
                assert_eq!(accepted, valid, "{path}");
            }
        };
    }
    check!(DeviceDescriptor, "device-descriptor");
    check!(ControlDescriptor, "control-descriptor");
    check!(CapabilityDescriptor, "capability-descriptor");
    check!(DeviceConnection, "device-connection");
    check!(DeviceIdentifier, "device-identifier");
    check!(DeviceSourceReference, "device-source-reference");
    check!(DescriptorCapabilityRef, "descriptor-capability-ref");
    check!(ControlRef, "control-ref");
    check!(CapabilityProjection, "capability-projection");
    check!(CapabilityUnit, "capability-unit");
    check!(CapabilityConstraint, "capability-constraint");
    check!(CapabilitySchema, "capability-schema");
    check!(ControlGeometry, "control-geometry");
    check!(DeviceRef, "device-ref");
    check!(ProfileCapacity, "profile-capacity");
    check!(HardwareAdvertisementDevice, "hardware-advertisement-device");
    check!(HardwareBeaconPayload, "advertisement-payload");
    check!(HardwareClaimDevice, "hardware-claim-device");
    check!(HardwareClaimTerms, "claim-terms");
}

#[test]
fn service_declaration_models_follow_upstream_coverage_examples() {
    use deckr::services::*;
    macro_rules! check {
        ($kind:ty, $name:literal) => {
            for valid in [true, false] {
                let path = format!(
                    "fixtures/{}/coverage/deckr/services/{}.v1.json",
                    if valid { "valid" } else { "invalid" },
                    $name
                );
                let accepted = serde_json::from_str::<$kind>(&artifact_text(&path))
                    .is_ok_and(|value| value.validate().is_ok());
                assert_eq!(accepted, valid, "{path}");
            }
        };
    }
    check!(ServiceProtocol, "protocol-definition");
    check!(ServiceMessageDefinition, "service-message-definition");
    check!(ServiceOperationDefinition, "service-operation-definition");
    check!(ServicePayloadSchema, "service-payload-schema");
    check!(
        ServiceViewFamilyDefinition,
        "service-view-family-definition"
    );
    check!(ServiceViewFamily, "service-view-family");
    check!(ServiceViewRef, "view-ref");
    check!(ServiceViewRecord, "view-record");
    check!(ServiceAdvertisementPayload, "service-advertisement-payload");
    check!(ServiceMessageBody, "service-message-body");
    check!(ServiceError, "service-error");
    for valid in [true, false] {
        let path = format!(
            "fixtures/{}/coverage/deckr/services/use-contract.v1.json",
            if valid { "valid" } else { "invalid" }
        );
        let value: Value = artifact(&path);
        let accepted = ContractRecord::from_value(value.clone()).is_ok_and(|record| {
            let Some(service) = record
                .participants
                .iter()
                .find(|participant| participant.family() == "service")
            else {
                return false;
            };
            let Some(consumer) = record
                .participants
                .iter()
                .find(|participant| *participant != service)
            else {
                return false;
            };
            let Some(profile) = record.profile.as_deref() else {
                return false;
            };
            validate_service_use_record(&value, service, consumer, profile).is_ok()
        });
        assert_eq!(accepted, valid, "{path}");
    }
}

#[test]
fn service_view_keys_match_upstream_vectors() {
    use deckr::services::{service_view_key, service_view_storage_key};
    let vectors: Value = artifact("vectors/service-view-keys.v1.json");
    for case in vectors["cases"].as_array().unwrap() {
        let view_key = service_view_key(
            case["serviceId"].as_str().unwrap(),
            case["family"].as_str().unwrap(),
            &[case["token"].as_str().unwrap()],
        );
        assert_eq!(view_key, case["viewKey"]);
        assert_eq!(
            service_view_storage_key(
                &view_key,
                case["contractId"].as_str().unwrap(),
                case["generation"].as_u64().unwrap()
            )
            .unwrap(),
            case["storageKey"]
        );
    }
}

#[test]
fn service_catalog_compiles_with_upstream_resources_and_validates_its_payloads() {
    use deckr::schema::SchemaRegistry;
    use deckr::services::*;
    let protocol =
        ServiceProtocol::from_text(&artifact_text("catalogs/action-runtime-protocol.v1.json"))
            .unwrap();
    deckr::action_runtime::validate_action_runtime_protocol(&protocol).unwrap();
    let mut registry = SchemaRegistry::default();
    let mut pending = vec![artifact_path("schemas")];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                let schema: Value =
                    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
                if let Some(id) = schema.get("$id").and_then(Value::as_str) {
                    registry = registry.with_resource(id.to_string(), schema).unwrap();
                }
            }
        }
    }
    let validated = ValidatedServiceProtocol::new(protocol.clone(), &registry).unwrap();
    for (name, definition) in &protocol.messages {
        let path = format!(
            "fixtures/valid/coverage/deckr/action-runtime/messages/{}.v1.json",
            name.replace('_', "-")
        );
        let message = DeckrMessage::from_text(&artifact_text(&path)).unwrap();
        validated
            .validate_message(
                &message.service_body().unwrap(),
                definition.direction,
                ServiceMessageKind::Initial,
            )
            .unwrap_or_else(|error| panic!("{path}: {error}"));
    }
}

#[test]
fn action_runtime_models_validate_upstream_positive_and_negative_examples() {
    use deckr::action_runtime::*;
    fn check<T: ActionModel>(name: &str) {
        for (directory, valid) in [("valid", true), ("invalid", false)] {
            let path = format!("fixtures/{directory}/coverage/deckr/action-runtime/{name}.v1.json");
            let parsed = T::from_text(&artifact_text(&path));
            assert_eq!(parsed.is_ok(), valid, "{path}: {:?}", parsed.err());
            if valid {
                let model = T::from_text(&artifact_text(&path)).unwrap();
                T::from_value(model.to_value().unwrap()).unwrap();
            }
        }
    }
    check::<ActionDescriptor>("action-descriptor");
    check::<ActionAvailabilityEntry>("action-availability-entry");
    check::<ActionAvailabilitySelector>("action-availability-selector");
    check::<ActionAvailabilityPayload>("availability-payload");
    check::<ActionInstanceMetadata>("action-instance-metadata");
    check::<ActionInstanceLifecycle>("action-instance-lifecycle-body");
    check::<ActionLifecycleRejected>("action-lifecycle-rejected-body");
    check::<BindingMetadata>("binding-metadata");
    check::<BindingAttached>("binding-attached-body");
    check::<BindingDetached>("binding-detached-body");
    check::<BindingOutput>("binding-output-body");
    check::<BindingOverlay>("binding-overlay-body");
    check::<BindingOverlayClear>("binding-overlay-clear-body");
    check::<CapabilityInput>("capability-input-body");
    check::<CapabilityInputEvent>("capability-input-event");
    check::<CapabilityRequirement>("capability-requirement");
    check::<CapabilityRequirementSelector>("capability-requirement-selector");
    check::<MatchedCapability>("matched-capability");
    check::<DynamicPageCommand>("dynamic-page-command");
    check::<DynamicPageBody>("open-page-body");
    check::<DynamicPageBody>("replace-page-body");
    check::<PageChildBindingDescriptor>("page-child-binding-descriptor");
    check::<PageChildBindingTarget>("page-child-binding-target");
    check::<PageSessionMetadata>("page-session-metadata");
    check::<PageSessionLifecycle>("page-session-lifecycle-body");
    check::<SettingsTargetRef>("settings-target-ref");
    check::<SettingsTargetDescription>("settings-target-description");
    check::<SettingsSchemaMetadata>("settings-schema-metadata");
    check::<SettingsSnapshot>("settings-snapshot");
    check::<EmptyActionBody>("empty-action-body");
}

#[test]
fn action_runtime_message_shapes_and_settings_keys_follow_upstream_contracts() {
    use deckr::action_runtime::*;
    let declaration: Value = artifact("catalogs/action-runtime-protocol.v1.json");
    for (name, definition) in declaration["messages"].as_object().unwrap() {
        for (directory, valid) in [("valid", true), ("invalid", false)] {
            let path = format!(
                "fixtures/{directory}/coverage/deckr/action-runtime/messages/{}.v1.json",
                name.replace('_', "-")
            );
            let parsed = DeckrMessage::from_text(&artifact_text(&path))
                .and_then(|message| ActionMessage::from_envelope(&message));
            assert_eq!(parsed.is_ok(), valid, "{path}: {:?}", parsed.as_ref().err());
            if let Ok(action) = parsed {
                assert_eq!(action.name(), name);
                assert_eq!(
                    serde_json::to_value(action.direction()).unwrap(),
                    definition["direction"]
                );
                assert_eq!(
                    ActionMessage::from_body(&action.to_body().unwrap()).unwrap(),
                    action
                );
            }
        }
    }
    let vectors: Value = artifact("vectors/settings-target-keys.v1.json");
    for case in vectors["cases"].as_array().unwrap() {
        assert_eq!(
            SettingsTargetRef::from_value(case["target"].clone())
                .unwrap()
                .key()
                .unwrap(),
            case["key"]
        );
    }
    for (directory, valid) in [("valid", true), ("invalid", false)] {
        let path =
            format!("fixtures/{directory}/coverage/deckr/action-runtime/availability-view.v1.json");
        let parsed = ActionAvailabilityRecord::from_text(&artifact_text(&path));
        assert_eq!(parsed.is_ok(), valid, "{path}: {:?}", parsed.as_ref().err());
        if let Ok(record) = parsed {
            assert_eq!(
                ActionAvailabilityRecord::from_value(record.to_value().unwrap()).unwrap(),
                record
            );
        }
    }
}

fn artifact_text(path: &str) -> String {
    let artifact = artifact_path(path);
    std::fs::read_to_string(&artifact).unwrap_or_else(|error| {
        panic!(
            "cannot read upstream Deckr artifact {}: {error}. Check out kws/deckr at ../deckr or set DECKR_SPEC_DIR to its repository root",
            artifact.display()
        )
    })
}

fn artifact<T: DeserializeOwned>(path: &str) -> T {
    serde_json::from_str(&artifact_text(path)).unwrap_or_else(|error| {
        panic!(
            "invalid upstream Deckr artifact {}: {error}",
            artifact_path(path).display()
        )
    })
}
