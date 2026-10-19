use thiserror::Error as ThisError;

/// Stable codes for guarded store and protocol mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationCode {
    KeyAlreadyExists,
    RevisionChanged,
    ContractInvalid,
    TokenInvalid,
    ContractIdentityMismatch,
    TokenIdentityMismatch,
    StoreUnavailable,
    SourceStale,
    TtlMetadataMissing,
    TtlInvalid,
}

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("invalid Deckr data: {0}")]
    Invalid(String),
    #[error("materialized view stale: {0}")]
    MaterializedViewStale(String),
    #[error("closed: {0}")]
    Closed(String),
    #[error("deadline exceeded: {0}")]
    Deadline(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("mutation {code:?}: {detail}")]
    Mutation { code: MutationCode, detail: String },
    #[error("state conflict: {0}")]
    StateConflict(String),
    #[error("state unavailable: {0}")]
    StateUnavailable(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn mutation(code: MutationCode, detail: impl Into<String>) -> Self {
        Self::Mutation {
            code,
            detail: detail.into(),
        }
    }
    pub fn mutation_code(&self) -> Option<MutationCode> {
        match self {
            Self::Mutation { code, .. } => Some(*code),
            Self::StateUnavailable(_) => Some(MutationCode::StoreUnavailable),
            Self::MaterializedViewStale(_) => Some(MutationCode::SourceStale),
            _ => None,
        }
    }
    pub fn is_state_conflict(&self) -> bool {
        matches!(
            self,
            Self::StateConflict(_)
                | Self::Mutation {
                    code: MutationCode::KeyAlreadyExists
                        | MutationCode::RevisionChanged
                        | MutationCode::ContractInvalid
                        | MutationCode::TokenInvalid
                        | MutationCode::ContractIdentityMismatch
                        | MutationCode::TokenIdentityMismatch,
                    ..
                }
        )
    }
}
