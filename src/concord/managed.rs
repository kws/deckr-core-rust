//! Supervised, protocol-neutral agreement lifetimes.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;
use crate::lifecycle::{
    deadline, heartbeat_delay, Cancellation, Failure, Phase, Snapshot, StateCell, StateWatch,
};
use crate::runtime::MaterializedConcord;
use crate::session::EndpointSession;

mod agreement;
mod participant;
pub use agreement::{AgreementContext, AgreementState, ManagedProposal};
pub use participant::{
    ManagedParticipant, ParticipantCandidates, ParticipantPolicy, ParticipantState,
};

/// One new proposal. Contract IDs and generation are allocated by the runtime.
#[derive(Debug, Clone)]
pub struct AgreementIntent {
    pub participants: Vec<EndpointAddress>,
    pub profile: Option<String>,
    pub terms: Option<Value>,
    pub supersedes: Option<ContractPointer>,
    pub current_sessions: BTreeMap<String, String>,
    pub creator: Option<EndpointAddress>,
    /// Profile policy: unresolved participation by another session blocks entry.
    pub exclusive: bool,
}
impl AgreementIntent {
    pub fn new(participants: Vec<EndpointAddress>) -> Self {
        Self {
            participants,
            profile: None,
            terms: None,
            supersedes: None,
            current_sessions: BTreeMap::new(),
            creator: None,
            exclusive: false,
        }
    }
}

impl<C: StateStore, T: StateStore>
    ConcordCoordinator<MaterializedStateStore<C>, MaterializedStateStore<T>>
{
    pub fn propose(
        &self,
        session: EndpointSession,
        intent: AgreementIntent,
    ) -> Result<ManagedProposal<C, T>> {
        ManagedProposal::start(self.clone(), session, intent)
    }
    pub fn participate<P: ParticipantPolicy>(
        &self,
        session: EndpointSession,
        profile: Option<String>,
        policy: P,
    ) -> Result<ManagedParticipant<C, T>> {
        ManagedParticipant::start(self.clone(), session, profile, policy)
    }

    /// Check ownership before profile readiness or a new proposal. A different
    /// live session and an empty local registry never prove lost authority.
    pub async fn bootstrap(
        &self,
        session: &EndpointSession,
        profile: Option<&str>,
        exclusive: bool,
    ) -> Result<()> {
        if session.is_closed() {
            return Err(Error::Closed("endpoint session closed".into()));
        }
        if self
            .restart
            .as_ref()
            .is_some_and(|proof| proof.contains(session.endpoint(), session.id()))
        {
            return Err(Error::Invalid(
                "a restarted endpoint must use a fresh session".into(),
            ));
        }
        self.wait_current().await?;
        // Keep raw matching records: malformed ownership evidence is an
        // unresolved bootstrap condition, not evidence that the role is free.
        let mut candidates = Vec::new();
        for entry in self
            .contract_state
            .items_cached(concord_contracts_prefix())?
        {
            let Some((contract_id, generation)) = parse_concord_contract_key(&entry.key) else {
                continue;
            };
            if (self.restart.is_none()
                && profile.is_some_and(|profile| {
                    entry.value.get("profile").and_then(Value::as_str) != Some(profile)
                }))
                || !entry
                    .value
                    .get("participants")
                    .and_then(Value::as_array)
                    .is_some_and(|participants| {
                        participants.iter().any(|participant| {
                            participant.as_str() == Some(session.endpoint().as_str())
                        })
                    })
            {
                continue;
            }
            let contract = contract_handle_from_entry(entry.clone()).unwrap_or(ContractHandle {
                key: entry.key,
                contract_id,
                generation,
                participants: vec![session.endpoint().clone()],
                attached_participants: Vec::new(),
                revision: entry.revision,
                state: ContractState::Open,
                profile: profile.map(str::to_owned),
                terms_hash: None,
            });
            candidates.push(contract);
        }
        for contract in candidates {
            let observed = self.validate(&contract, None).await;
            let verified_restart = observed
                .tokens
                .get(session.endpoint().as_str())
                .is_some_and(|token| {
                    self.restart
                        .as_ref()
                        .is_some_and(|proof| proof.contains(session.endpoint(), &token.session_id))
                });
            if verified_restart
                && matches!(
                    observed.status,
                    ContractValidityStatus::Valid | ContractValidityStatus::NotYetFulfilled
                )
            {
                self.cancel(
                    &contract,
                    session.endpoint(),
                    Some("verified_restart".into()),
                )
                .await?;
                if !matches!(
                    self.validate(&contract, None).await.status,
                    ContractValidityStatus::Cancelled | ContractValidityStatus::MissingContract
                ) {
                    return Err(Error::StateUnavailable(
                        "restart cleanup is not confirmed".into(),
                    ));
                }
                continue;
            }
            match observed.status {
                ContractValidityStatus::Cancelled | ContractValidityStatus::MissingContract => {
                    continue
                }
                ContractValidityStatus::MissingToken
                | ContractValidityStatus::GenerationMismatch
                | ContractValidityStatus::TermsHashMismatch
                | ContractValidityStatus::SessionMismatch
                | ContractValidityStatus::InvalidToken => {
                    self.cancel(
                        &contract,
                        session.endpoint(),
                        Some("bootstrap confirmed permanent authority loss".into()),
                    )
                    .await?;
                    if self.validate(&contract, None).await.status
                        != ContractValidityStatus::Cancelled
                    {
                        return Err(Error::StateUnavailable(
                            "bootstrap cleanup is not confirmed".into(),
                        ));
                    }
                }
                ContractValidityStatus::Valid | ContractValidityStatus::NotYetFulfilled => {
                    // A peer's session reference does not establish ownership.
                    // A valid local participant token does, even while pending.
                    let competing = observed
                        .tokens
                        .get(session.endpoint().as_str())
                        .is_some_and(|token| token.session_id != session.id());
                    if exclusive
                        && competing
                        && profile.is_none_or(|value| contract.profile.as_deref() == Some(value))
                    {
                        return Err(Error::StateConflict(
                            "exclusive endpoint ownership is unresolved".into(),
                        ));
                    }
                }
                _ => {
                    return Err(Error::StateUnavailable(format!(
                        "bootstrap cannot establish ownership: {:?}",
                        observed.reason_code
                    )))
                }
            }
        }
        if !self.is_current() {
            return Err(Error::MaterializedViewStale(
                "bootstrap source lost currentness".into(),
            ));
        }
        Ok(())
    }
}

fn require_runtime(session: &EndpointSession) -> Result<()> {
    if session.is_closed() {
        return Err(Error::Closed("endpoint session closed".into()));
    }
    tokio::runtime::Handle::try_current()
        .map_err(|_| Error::Invalid("managed Concord requires a Tokio runtime".into()))?;
    Ok(())
}
