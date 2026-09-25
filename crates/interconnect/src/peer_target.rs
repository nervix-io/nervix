//! Where one peer is reached, and the certificate name expected there.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The dialable address of a peer, and the resolution of one advertised endpoint into
//!   the targets that reach it.
//! - **Depends on.** The vocabulary's advertised endpoints and host name resolution.
//! - **Must not know.** Which operation a caller sends to the target it selects.

use std::{io, net::SocketAddr};

use nervix_models::NodeEndpoint;

use crate::socket::lookup_host;

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

    /// Every target `endpoint` currently resolves to, in resolution order.
    ///
    /// The advertised host stays the expected certificate name, so a peer reached through any one
    /// of its addresses is still authenticated against the name it advertised.
    pub async fn resolve(endpoint: &NodeEndpoint) -> io::Result<Vec<Self>> {
        let resolved = lookup_host((endpoint.host(), endpoint.port())).await?;
        let targets = resolved
            .map(|addr| Self::new(addr, endpoint.host()))
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return Err(io::Error::other(format!(
                "advertised endpoint '{endpoint}' resolved to no addresses"
            )));
        }
        Ok(targets)
    }
}
