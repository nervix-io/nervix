//! Where one peer is reached, and the certificate name expected there.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The dialable address of a peer, and the resolution of one advertised endpoint into
//!   the targets that reach it.
//! - **Depends on.** The vocabulary's advertised endpoints and the interconnect's peer resolver.
//! - **Must not know.** Which operation a caller sends to the target it selects.

use std::{net::SocketAddr, time::Duration};

use error_stack::Report;
use nervix_dns::DnsLookupError;
use nervix_models::NodeEndpoint;

use crate::socket::PeerResolver;

/// One advertised address and the certificate name expected there.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerTarget {
    pub addr: SocketAddr,
    pub server_name: String,
}

impl PeerTarget {
    pub fn new(addr: SocketAddr, server_name: impl Into<String>) -> Self {
        Self {
            addr,
            server_name: server_name.into(),
        }
    }

    /// Every target `endpoint` resolves to through `resolver` within `budget`, in resolution
    /// order.
    ///
    /// The advertised host stays the expected certificate name, so a peer reached through any one
    /// of its addresses is still authenticated against the name it advertised. A successful
    /// resolution always holds at least one target.
    pub async fn resolve(
        resolver: &PeerResolver,
        endpoint: &NodeEndpoint,
        budget: Duration,
    ) -> Result<Vec<Self>, Report<DnsLookupError>> {
        let addresses = resolver
            .resolve(endpoint.host(), endpoint.port(), budget)
            .await?;
        let mut targets = Vec::with_capacity(addresses.len());
        for addr in addresses {
            targets.push(Self::new(addr, endpoint.host()));
        }
        Ok(targets)
    }

    /// The endpoint this target authenticates: its certificate name and the port it is reached on.
    pub fn endpoint(&self) -> NodeEndpoint {
        NodeEndpoint::new(self.server_name.clone(), self.addr.port())
    }
}
