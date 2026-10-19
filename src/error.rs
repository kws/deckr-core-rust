use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("invalid Deckr data: {0}")]
    Invalid(String),
    #[error("materialized view stale: {0}")]
    MaterializedViewStale(String),
    #[error("closed: {0}")]
    Closed(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("state conflict: {0}")]
    StateConflict(String),
    #[error("state unavailable: {0}")]
    StateUnavailable(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
