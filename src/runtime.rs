//! Host-owned protocol sources shared by independent endpoint lifetimes.
//!
//! Close dependent participants and advertisements before closing this runtime.
//! Cloning a view shares its source; it does not open another store watch.

use crate::beacon::Beacon;
use crate::concord::ConcordCoordinator;
use crate::state::{MaterializedStateStore, StateStore};
use crate::Result;

pub type MaterializedConcord<C, T> =
    ConcordCoordinator<MaterializedStateStore<C>, MaterializedStateStore<T>>;

#[derive(Clone)]
pub struct ProtocolRuntime<B: StateStore, C: StateStore, T: StateStore> {
    beacon: Beacon<B>,
    concord: MaterializedConcord<C, T>,
}

impl<B: StateStore, C: StateStore, T: StateStore> ProtocolRuntime<B, C, T> {
    pub fn from_views(beacon: Beacon<B>, concord: MaterializedConcord<C, T>) -> Self {
        Self { beacon, concord }
    }

    pub async fn start(advertisements: B, contracts: C, tokens: T) -> Result<Self> {
        let beacon = Beacon::start(advertisements).await?;
        let contracts =
            MaterializedStateStore::start(contracts, crate::keys::concord_contracts_prefix())
                .await?;
        let tokens =
            MaterializedStateStore::start(tokens, crate::keys::concord_contracts_prefix()).await?;
        Ok(Self {
            beacon,
            concord: ConcordCoordinator::new(contracts, tokens),
        })
    }

    pub fn beacon(&self) -> &Beacon<B> {
        &self.beacon
    }

    pub fn concord(&self) -> &MaterializedConcord<C, T> {
        &self.concord
    }

    /// Closing the shared sources ends authority in every dependent participant.
    pub async fn close(&self) {
        futures_util::future::join(
            self.beacon.materialized_store().close_and_wait(),
            self.concord.close_sources_and_wait(),
        )
        .await;
    }
}
