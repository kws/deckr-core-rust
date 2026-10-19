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

fn artifact_path(path: &str) -> PathBuf {
    let spec_dir = std::env::var_os("DECKR_SPEC_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../deckr"));
    spec_dir.join("contract/v1").join(path)
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
