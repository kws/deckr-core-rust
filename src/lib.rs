//! Rust implementation of the Deckr protocols and shared runtime.
//!
//! The specification and conformance artifacts are maintained in
//! [kws/deckr](https://github.com/kws/deckr). Enable the `nats` feature for the
//! NATS runtime and JetStream KV adapter.
//!
//! ```
//! use deckr::endpoint::EndpointAddress;
//! use deckr::lanes::direct_subject;
//!
//! let endpoint = EndpointAddress::parse("controller:main")?;
//! assert_eq!(
//!     direct_subject("hardware_messages", &endpoint)?,
//!     "deckr.msg.hardware_messages.to.controller.main"
//! );
//! # Ok::<(), deckr::Error>(())
//! ```

pub mod action_runtime;
pub mod authority;
pub mod beacon;
pub mod canonical_json;
pub mod concord;
pub mod endpoint;
pub mod hardware;
pub mod json;
pub mod keys;
pub mod lanes;
pub mod lifecycle;
pub mod messaging;
#[cfg(feature = "nats")]
pub mod nats;
pub mod profiles;
pub mod restart;
pub mod runtime;
pub mod schema;
pub mod services;
pub mod session;
pub mod state;

mod error;

pub use error::{Error, MutationCode, Result};
