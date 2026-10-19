//! Fixed-observation evaluation shared by cached and exact readers.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidityReason {
    SourceUnavailable,
    ContractMissing,
    ContractMalformed,
    ContractKeyMismatch,
    ContractPointerMismatch,
    ContractCancelled,
    ParticipantNotAttached,
    TokenMissing,
    TokenMalformed,
    TokenKeyMismatch,
    TokenContractMismatch,
    TokenParticipantMismatch,
    TokenLocalAuthorityLost,
    TokenGenerationMismatch,
    TokenTermsHashMismatch,
    TokenSessionMismatch,
}

impl ValidityReason {
    pub fn status(self) -> ContractValidityStatus {
        use ContractValidityStatus as S;
        use ValidityReason as R;
        match self {
            R::SourceUnavailable => S::Unavailable,
            R::ContractMissing => S::MissingContract,
            R::ContractMalformed | R::ContractKeyMismatch | R::ContractPointerMismatch => {
                S::InvalidContract
            }
            R::ContractCancelled => S::Cancelled,
            R::ParticipantNotAttached => S::NotYetFulfilled,
            R::TokenMissing => S::MissingToken,
            R::TokenMalformed
            | R::TokenKeyMismatch
            | R::TokenContractMismatch
            | R::TokenParticipantMismatch
            | R::TokenLocalAuthorityLost => S::InvalidToken,
            R::TokenGenerationMismatch => S::GenerationMismatch,
            R::TokenTermsHashMismatch => S::TermsHashMismatch,
            R::TokenSessionMismatch => S::SessionMismatch,
        }
    }
}

/// The caller collects these observations before evaluation. Entries are kept
/// raw, including malformed records and identity mismatches.
#[derive(Debug, Clone, Default)]
pub struct ContractObservations {
    pub contract: Option<StateEntry>,
    pub tokens: BTreeMap<String, StateEntry>,
    pub current_sessions: BTreeMap<String, String>,
}

/// Pure evaluation: no I/O, writes, source access, or local-lease adoption.
pub fn evaluate_contract(
    handle: &ContractHandle,
    observations: &ContractObservations,
) -> ContractValidity {
    evaluate_observations(
        handle,
        observations.contract.clone(),
        &observations.tokens,
        Some(&observations.current_sessions),
    )
}

fn outcome(
    reason: ValidityReason,
    contract: Option<ContractRecord>,
    tokens: BTreeMap<String, ParticipantTokenRecord>,
    participant: Option<EndpointAddress>,
    detail: Option<String>,
) -> ContractValidity {
    ContractValidity {
        status: reason.status(),
        contract,
        tokens,
        reason: detail,
        reason_code: Some(reason),
        participant,
    }
}

pub(super) fn evaluate_observations(
    handle: &ContractHandle,
    entry: Option<StateEntry>,
    entries: &BTreeMap<String, StateEntry>,
    sessions: Option<&BTreeMap<String, String>>,
) -> ContractValidity {
    use ValidityReason as R;
    let empty = BTreeMap::new;
    let Some(entry) = entry else {
        return outcome(R::ContractMissing, None, empty(), None, None);
    };
    let record = match ContractRecord::from_value(entry.value) {
        Ok(record) => record,
        Err(error) => {
            return outcome(
                R::ContractMalformed,
                None,
                empty(),
                None,
                Some(error.to_string()),
            )
        }
    };
    if entry.key != make_concord_contract_key(&record.contract_id, record.generation) {
        return outcome(R::ContractKeyMismatch, Some(record), empty(), None, None);
    }
    if entry.key != handle.key
        || record.contract_id != handle.contract_id
        || record.generation != handle.generation
    {
        return outcome(
            R::ContractPointerMismatch,
            Some(record),
            empty(),
            None,
            None,
        );
    }
    if record.state == ContractState::Cancelled {
        return outcome(R::ContractCancelled, Some(record), empty(), None, None);
    }
    let mut participants = record.participants.clone();
    participants.sort();
    let mut pending = None;
    let mut tokens = BTreeMap::new();
    for participant in participants {
        let attached = record.attached_participants.contains(&participant);
        let key = make_concord_participant_token_key(
            &record.contract_id,
            record.generation,
            &participant,
        );
        let Some(entry) = entries.get(&key) else {
            if attached {
                return outcome(
                    R::TokenMissing,
                    Some(record),
                    tokens,
                    Some(participant),
                    None,
                );
            }
            pending.get_or_insert(participant);
            continue;
        };
        let token = match ParticipantTokenRecord::from_value(entry.value.clone()) {
            Ok(token) => token,
            Err(error) => {
                return outcome(
                    R::TokenMalformed,
                    Some(record),
                    tokens,
                    Some(participant),
                    Some(error.to_string()),
                )
            }
        };
        // Keep only tokens whose canonical key and identity match. A later
        // terminal observation must not discard earlier valid token identities.
        let identity = if entry.key != key {
            Some(R::TokenKeyMismatch)
        } else if token.contract_id != record.contract_id {
            Some(R::TokenContractMismatch)
        } else if token.generation != record.generation {
            Some(R::TokenGenerationMismatch)
        } else if token.participant != participant {
            Some(R::TokenParticipantMismatch)
        } else {
            None
        };
        if let Some(reason) = identity {
            return outcome(reason, Some(record), tokens, Some(participant), None);
        }
        let reason = if record.terms_hash.is_some() && record.terms_hash != token.terms_hash {
            Some(R::TokenTermsHashMismatch)
        } else if record
            .participant_sessions
            .get(participant.as_str())
            .is_some_and(|session| session != &token.session_id)
            || sessions
                .and_then(|sessions| sessions.get(participant.as_str()))
                .is_some_and(|session| session != &token.session_id)
        {
            Some(R::TokenSessionMismatch)
        } else {
            None
        };
        tokens.insert(participant.to_string(), token);
        if let Some(reason) = reason {
            return outcome(reason, Some(record), tokens, Some(participant), None);
        }
        if !attached {
            pending.get_or_insert(participant);
        }
    }
    if let Some(participant) = pending {
        return outcome(
            R::ParticipantNotAttached,
            Some(record),
            tokens,
            Some(participant),
            None,
        );
    }
    ContractValidity {
        status: ContractValidityStatus::Valid,
        contract: Some(record),
        tokens,
        reason: None,
        reason_code: None,
        participant: None,
    }
}
