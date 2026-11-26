//! The lease one pooled emitter takes on its client's pooled client, before it opens.
//!
//! Layer: data plane.
//! - **Owns.** Leasing an emitter's client from this node's pooled-client service as the pooled
//!   client of the emitter's connector, and pairing the lease with the wait publication of that
//!   emitter, so its connector takes both through its host handle.
//! - **Depends on.** The shared client registry, the vocabulary that names a domain, a client and
//!   a graph node, and the connector contract's pooled client and its lease.
//! - **Must not know.** Any connector crate, how a batch is mapped, how a row is encoded, or what
//!   an emitter writes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::{PooledClient, PooledClientLease};
use nervix_models::ClientPoolBounds;

use super::*;
use crate::runtime::shared_clients::LeasedPooledClient;

impl EmitterConnectorContext {
    /// Takes this emitter's interest in the node's instance of `client` as the pooled client `K`
    /// of its connector, opening the instance on first local use with `bounds`.
    ///
    /// The pool belongs to the named client, so every local emitter of that client borrows from it
    /// and the declared maximum bounds the node's connections rather than this emitter's. The
    /// lease travels to the emitter in its host handle, so the pool stays open for exactly as long
    /// as the emitter writes through it, and the lease publishes the wait of every checkout the
    /// pool does not answer at once.
    ///
    /// A client that cannot open, or that another connector opened, fails the emitter's
    /// initialization with its typed cause beneath.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "installs the emitter's retained shared-client lease and pool-wait \
                      publication"
        )
    )]
    pub(super) async fn lease_pooled_client<K: PooledClient>(
        &self,
        client: &EmitterClientSpec,
        bounds: ClientPoolBounds,
    ) -> EmitterRuntimeResult<PooledClientLease<K>> {
        let LeasedPooledClient { pooled, interest } = self
            .runtime
            .lease_shared_client::<K>(&self.domain, client, bounds)
            .await
            .change_context(EmitterRuntimeError::InitializeConnector)?;
        let waits = self.runtime.register_pool_wait(
            self.registry_key(),
            client.name.clone(),
            self.interactions.clone(),
        );
        Ok(PooledClientLease::new(pooled, waits, interest))
    }
}
