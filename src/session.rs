//! Protocol-neutral endpoint identity and local lifetime.
use std::sync::Arc;

use crate::endpoint::EndpointAddress;
use crate::lifecycle::Cancellation;
use crate::{Error, Result};

#[derive(Debug)]
struct Session {
    endpoint: EndpointAddress,
    id: String,
    cancellation: Cancellation,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// A host-owned endpoint session. Closing it stops dependent local workers;
/// those workers still own their protocol cleanup.
#[derive(Debug, Clone)]
pub struct EndpointSession(Arc<Session>);
impl EndpointSession {
    pub fn new(endpoint: EndpointAddress) -> Self {
        Self::with_id(endpoint, uuid::Uuid::new_v4().to_string()).expect("UUID is a valid session")
    }
    pub fn with_id(endpoint: EndpointAddress, id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if id.is_empty() || id.trim() != id {
            return Err(Error::Invalid(
                "session ID must be nonempty and trimmed".into(),
            ));
        }
        Ok(Self(Arc::new(Session {
            endpoint,
            id,
            cancellation: Cancellation::default(),
        })))
    }
    pub fn endpoint(&self) -> &EndpointAddress {
        &self.0.endpoint
    }
    pub fn id(&self) -> &str {
        &self.0.id
    }
    pub fn is_closed(&self) -> bool {
        self.0.cancellation.is_cancelled()
    }
    pub fn close(&self) {
        self.0.cancellation.cancel();
    }
    pub async fn closed(&self) {
        self.0.cancellation.cancelled().await
    }
}
