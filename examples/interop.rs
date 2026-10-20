//! JSON-lines driver for an external mixed-language lifecycle test.
use std::io::{self, BufRead};
use std::time::Duration;

use deckr::authority::ContractPointer;
use deckr::canonical_json::canonical_json_hash_value;
use deckr::concord::{
    AgreementIntent, AgreementState, ManagedParticipant, ManagedProposal, ParticipantCandidates,
};
use deckr::endpoint::EndpointAddress;
use deckr::nats::{DeckrRuntime, DeckrRuntimeConfig, NatsStateStore};
use deckr::session::EndpointSession;
use serde_json::{json, Value};

fn status(state: AgreementState) -> Option<Value> {
    let contract = state.contract?;
    let validity = state.validity.as_ref();
    let record = validity.and_then(|value| value.contract.as_ref());
    Some(json!({
        "contractId": contract.contract_id, "generation": contract.generation,
        "authority": state.local_authority, "valid": validity.is_some_and(|value| value.valid()),
        "termsHash": record.and_then(|value| value.terms_hash.as_ref()),
        "calculatedHash": record.and_then(|value| value.terms.as_ref()).map(|terms| canonical_json_hash_value(terms).unwrap()),
        "refreshSeq": validity.and_then(|value| value.tokens.get("controller:rust")).map(|value| value.refresh_seq).unwrap_or(0)
    }))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let mut config = DeckrRuntimeConfig::new(&args[1]);
    let prefix = &args[2];
    config.beacon_advertisement_bucket = format!("{prefix}_beacon");
    config.concord_contract_bucket = format!("{prefix}_contracts");
    config.concord_token_bucket = format!("{prefix}_tokens");
    config.reconnect_max_interval = Duration::from_millis(100);
    config.connection_timeout = Duration::from_millis(200);
    let runtime = DeckrRuntime::connect(config).await?;
    let concord = runtime.materialized_concord().await?;
    concord.wait_current().await?;
    let session = EndpointSession::with_id(
        EndpointAddress::parse_protocol("controller:rust")?,
        "rust-session",
    )?;
    let mut proposal: Option<ManagedProposal<NatsStateStore, NatsStateStore>> = None;
    let mut participant: Option<ManagedParticipant<NatsStateStore, NatsStateStore>> = None;
    println!("{{\"ready\":true}}");
    for line in io::stdin().lock().lines() {
        let command = deckr::json::parse(line?.as_bytes())?;
        let operation = command["op"].as_str().unwrap_or_default();
        if matches!(operation, "mode" | "stop") {
            if let Some(old) = proposal.take() {
                // close reports the terminal cause, including the intentional outage.
                if let Err(error) = old.close().await {
                    eprintln!("proposal closed: {error}");
                }
            }
            if let Some(old) = participant.take() {
                if let Err(error) = old.close().await {
                    eprintln!("participant closed: {error}");
                }
            }
            if operation == "stop" {
                println!("{{\"ok\":true}}");
                break;
            }
            if command["mode"] == "participant" {
                participant = Some(concord.participate(
                    session.clone(),
                    Some("org.example.interop.v1".into()),
                    |candidates: ParticipantCandidates| async move {
                        Ok(candidates
                            .contracts
                            .into_iter()
                            .map(|item| ContractPointer {
                                contract_id: item.contract.contract_id,
                                generation: item.contract.generation,
                            })
                            .collect())
                    },
                )?);
            }
            println!("{{\"ok\":true}}");
        } else if operation == "propose" {
            let mut intent = AgreementIntent::new(
                [
                    "controller:rust",
                    "controller:python",
                    "controller:typescript",
                ]
                .into_iter()
                .map(EndpointAddress::parse_protocol)
                .collect::<deckr::Result<Vec<_>>>()?,
            );
            intent.profile = Some("org.example.interop.v1".into());
            intent.terms = Some(command["terms"].clone());
            let created = concord.propose(session.clone(), intent)?;
            println!("{}", json!({"contractId":created.contract().contract_id}));
            proposal = Some(created);
        } else if operation == "status" {
            let mut records = Vec::new();
            if let Some(owner) = &proposal {
                if let Some(value) = status(owner.state().value) {
                    records.push(value);
                }
            }
            if let Some(owner) = &participant {
                records.extend(
                    owner
                        .state()
                        .value
                        .contracts
                        .into_values()
                        .filter_map(|value| status(value.value)),
                );
            }
            println!(
                "{}",
                json!({"current":concord.is_current(),"contracts":records})
            );
        } else {
            println!("{{\"error\":\"Unknown operation\"}}");
        }
    }
    if let Some(owner) = proposal {
        if let Err(error) = owner.close().await {
            eprintln!("proposal closed: {error}");
        }
    }
    if let Some(owner) = participant {
        if let Err(error) = owner.close().await {
            eprintln!("participant closed: {error}");
        }
    }
    concord.close_sources_and_wait().await;
    runtime.nats().close_sources().await;
    Ok(())
}
