use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContractPointer {
    pub contract_id: String,
    pub generation: u64,
}

impl ContractPointer {
    pub fn validate(&self) -> Result<()> {
        if self.contract_id.trim() != self.contract_id || self.contract_id.is_empty() {
            return Err(Error::Invalid("contract id must not be empty".to_string()));
        }
        if self.generation == 0 {
            return Err(Error::Invalid(
                "generation must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Exact Concord and session guard for a selected Deckr lane/profile. Higher
/// hardware and service boundaries additionally validate resource and operation
/// rules before invoking a handler; this guard alone grants no resource access.
#[derive(Clone)]
pub struct AgreementMessageGuard<C: crate::state::StateStore, T: crate::state::StateStore> {
    agreement: crate::concord::AgreementContext<C, T>,
    lane: String,
}
impl<C: crate::state::StateStore, T: crate::state::StateStore> AgreementMessageGuard<C, T> {
    pub fn new(
        agreement: crate::concord::AgreementContext<C, T>,
        lane: impl Into<String>,
        expected_profile: &str,
    ) -> Result<Self> {
        let lane = lane.into();
        if lane.is_empty()
            || lane.trim() != lane
            || expected_profile.is_empty()
            || agreement.contract().profile.as_deref() != Some(expected_profile)
        {
            return Err(Error::Invalid(
                "message context requires its selected lane and agreement profile".into(),
            ));
        }
        Ok(Self { agreement, lane })
    }
    pub fn agreement(&self) -> &crate::concord::AgreementContext<C, T> {
        &self.agreement
    }

    fn check(&self, message: &crate::lanes::DeckrMessage, outgoing: bool) -> Result<()> {
        message.validate()?;
        let contract = self.agreement.contract();
        if message.lane != self.lane
            || message.contract.as_ref().is_none_or(|pointer| {
                pointer.contract_id != contract.contract_id
                    || pointer.generation != contract.generation
            })
            || message.is_expired_at(self.agreement.utc_now())
        {
            return Err(Error::Unauthorized(
                "message does not identify this current agreement and lane".into(),
            ));
        }
        let validity = self.agreement.authorized_validity()?;
        let local = self.agreement.session();
        let sender = validity
            .tokens
            .get(&message.sender)
            .ok_or_else(|| Error::Unauthorized("sender is not an agreement participant".into()))?;
        if sender.session_id != message.sender_session_id {
            return Err(Error::Unauthorized(
                "sender session differs from the participant token".into(),
            ));
        }
        if outgoing {
            if message.sender != local.endpoint().as_str()
                || message.sender_session_id != local.id()
            {
                return Err(Error::Unauthorized(
                    "outgoing identity differs from the managed endpoint".into(),
                ));
            }
        } else if !message.recipient.targets_endpoint(local.endpoint())? {
            return Err(Error::Unauthorized(
                "message does not target the managed endpoint".into(),
            ));
        }
        if let Some(recipient) = message.recipient_endpoint() {
            let recipient = validity.tokens.get(recipient).ok_or_else(|| {
                Error::Unauthorized("recipient is not an agreement participant".into())
            })?;
            if message.recipient_session_id.as_deref() != Some(recipient.session_id.as_str()) {
                return Err(Error::Unauthorized(
                    "protected direct messages require the participant recipient session".into(),
                ));
            }
        }
        self.agreement.authorize()
    }
}
impl<C: crate::state::StateStore, T: crate::state::StateStore> crate::messaging::MessageAuthority
    for AgreementMessageGuard<C, T>
{
    fn authorize_outgoing(&self, message: &crate::lanes::DeckrMessage) -> Result<()> {
        self.check(message, true)
    }
    fn authorize_incoming(&self, message: &crate::lanes::DeckrMessage) -> Result<()> {
        self.check(message, false)
    }
}
