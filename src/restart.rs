//! Trusted supervisor input. Parsing does not establish that a process exited.
use std::collections::BTreeSet;
use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

use crate::endpoint::EndpointAddress;
use crate::{Error, Result};

pub const RESTART_HANDOFF_SCHEMA: &str = "dev.deckr.runtime.restart_handoff.v1";

#[derive(Debug, Clone)]
pub struct VerifiedRestart {
    pub instance_id: String,
    participants: BTreeSet<(String, String)>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Handoff {
    schema: String,
    instance_id: String,
    predecessors: Vec<Predecessor>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Predecessor {
    predecessor_id: String,
    exit_code: i32,
    participants: Vec<Participant>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Participant {
    endpoint: String,
    session_id: String,
}
fn text(value: &str) -> Result<()> {
    if value.is_empty() || value.trim() != value {
        return Err(Error::Invalid(
            "restart identity must be nonempty unpadded text".into(),
        ));
    }
    Ok(())
}
impl VerifiedRestart {
    /// The caller must authenticate the supervisor and its confirmed process exit.
    pub fn from_handoff(value: Value, expected_instance: &str) -> Result<Self> {
        let raw: Handoff = serde_json::from_value(value)?;
        text(expected_instance)?;
        text(&raw.instance_id)?;
        if raw.schema != RESTART_HANDOFF_SCHEMA || raw.instance_id != expected_instance {
            return Err(Error::Invalid(
                "restart handoff schema or instance mismatch".into(),
            ));
        }
        if raw.predecessors.is_empty() || raw.predecessors.len() > 256 {
            return Err(Error::Invalid(
                "restart predecessors must contain 1 to 256 lifetimes".into(),
            ));
        }
        let mut participants = BTreeSet::new();
        let mut lifetimes = BTreeSet::new();
        for predecessor in raw.predecessors {
            text(&predecessor.predecessor_id)?;
            if !lifetimes.insert(predecessor.predecessor_id) {
                return Err(Error::Invalid("duplicate restart predecessor".into()));
            }
            let _confirmed_exit = predecessor.exit_code;
            if predecessor.participants.is_empty() || predecessor.participants.len() > 256 {
                return Err(Error::Invalid(
                    "restart participants must contain 1 to 256 identities".into(),
                ));
            }
            for row in predecessor.participants {
                EndpointAddress::parse_protocol(&row.endpoint)?;
                text(&row.session_id)?;
                if !participants.insert((row.endpoint, row.session_id)) {
                    return Err(Error::Invalid("duplicate restart participant".into()));
                }
            }
        }
        Ok(Self {
            instance_id: raw.instance_id,
            participants,
        })
    }

    pub fn contains(&self, endpoint: &EndpointAddress, session_id: &str) -> bool {
        self.participants
            .contains(&(endpoint.to_string(), session_id.to_owned()))
    }
    pub fn participants(&self) -> impl Iterator<Item = (&str, &str)> {
        self.participants
            .iter()
            .map(|(endpoint, session)| (endpoint.as_str(), session.as_str()))
    }
}

/// Read only a supervisor-controlled channel. Missing files permit first boot.
pub fn load_restart_handoff(path: &Path, instance_id: &str) -> Result<Option<VerifiedRestart>> {
    if !path.is_absolute() {
        return Err(Error::Invalid(
            "restart handoff path must be absolute".into(),
        ));
    }
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Error::Invalid(format!(
                "restart handoff read failed: {error}"
            )))
        }
    };
    if bytes.len() > 262144 {
        return Err(Error::Invalid("restart handoff is too large".into()));
    }
    VerifiedRestart::from_handoff(crate::json::parse(&bytes)?, instance_id).map(Some)
}
pub fn restart_handoff_from_env() -> Result<Option<VerifiedRestart>> {
    let Some(path) = std::env::var_os("DECKR_RESTART_HANDOFF") else {
        return Ok(None);
    };
    let instance = std::env::var("DECKR_INSTANCE_ID").map_err(|_| {
        Error::Invalid("DECKR_INSTANCE_ID is required with DECKR_RESTART_HANDOFF".into())
    })?;
    load_restart_handoff(Path::new(&path), &instance)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn handoff() -> Value {
        json!({"schema":RESTART_HANDOFF_SCHEMA, "instanceId":"provider", "predecessors":[{
            "predecessorId":"container:old", "exitCode":137,
            "participants":[{"endpoint":"service:restart", "sessionId":"dead"}]
        }]})
    }
    #[test]
    fn rejects_wrong_instance_unknown_fields_duplicate_identities_and_invalid_exit() {
        assert!(VerifiedRestart::from_handoff(handoff(), "other").is_err());
        for exit in [json!(true), json!(2147483648i64), json!(1.5)] {
            let mut value = handoff();
            value["predecessors"][0]["exitCode"] = exit;
            assert!(VerifiedRestart::from_handoff(value, "provider").is_err());
        }
        let mut value = handoff();
        value["extra"] = json!(true);
        assert!(VerifiedRestart::from_handoff(value, "provider").is_err());
        let mut value = handoff();
        let duplicate = value["predecessors"][0]["participants"][0].clone();
        value["predecessors"][0]["participants"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        assert!(VerifiedRestart::from_handoff(value, "provider").is_err());
    }
    #[test]
    fn retains_all_exited_lifetimes_without_granting_authority_over_a_successor() {
        let mut value = handoff();
        let mut older = value["predecessors"][0].clone();
        older["predecessorId"] = json!("container:older");
        older["participants"][0]["sessionId"] = json!("older");
        value["predecessors"].as_array_mut().unwrap().push(older);
        let proof = VerifiedRestart::from_handoff(value, "provider").unwrap();
        let endpoint = EndpointAddress::parse_protocol("service:restart").unwrap();
        assert!(proof.contains(&endpoint, "dead") && proof.contains(&endpoint, "older"));
        assert!(!proof.contains(&endpoint, "successor"));
    }
}
