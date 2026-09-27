//! TCP and name-resolution operations used by the interconnect transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Selection of the socket and resolver implementation at the I/O boundary.
//! - **Depends on.** Tokio sockets and the node's Hickory resolver in production, and Turmoil's
//!   simulated sockets and DNS in the dedicated simulation build.
//! - **Must not know.** TLS, HTTP/2, peer identity, or transport operations.

use std::{net::SocketAddr, time::Duration};

use error_stack::Report;
use nervix_dns::DnsLookupError;
#[cfg(feature = "turmoil")]
use nervix_dns::DnsLookupFailure;
#[cfg(not(feature = "turmoil"))]
use nervix_dns::DnsResolver;
#[cfg(not(feature = "turmoil"))]
pub(crate) use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "turmoil")]
pub(crate) use turmoil::net::{TcpListener, TcpStream};

/// Resolves the host a peer advertised into the addresses the transport dials.
///
/// Production resolves through the node's Hickory resolver, whose cache and configuration belong
/// to the node that loaded it. The simulation build resolves through the simulated host's own DNS
/// table and never constructs a production resolver.
#[derive(Clone)]
pub struct PeerResolver {
    #[cfg(not(feature = "turmoil"))]
    dns: DnsResolver,
}

#[cfg(not(feature = "turmoil"))]
impl PeerResolver {
    /// Resolve peers through `dns`, the resolver this node loaded at startup.
    pub fn new(dns: DnsResolver) -> Self {
        Self { dns }
    }

    /// Every address `host` resolves to now, each with `port`, in resolution order, within
    /// `budget`. A successful result holds at least one address.
    pub(crate) async fn resolve(
        &self,
        host: &str,
        port: u16,
        budget: Duration,
    ) -> Result<Vec<SocketAddr>, Report<DnsLookupError>> {
        self.dns.resolve(host, port, budget).await
    }
}

#[cfg(feature = "turmoil")]
impl PeerResolver {
    /// Resolve peers through the simulated host's DNS table.
    pub fn simulated() -> Self {
        Self {}
    }

    /// The one address the simulated DNS table holds for `host`, with `port`. The table answers
    /// without waiting, so the budget never runs out.
    pub(crate) async fn resolve(
        &self,
        host: &str,
        port: u16,
        _budget: Duration,
    ) -> Result<Vec<SocketAddr>, Report<DnsLookupError>> {
        let resolved = match turmoil::net::lookup_host((host, port)).await {
            Ok(resolved) => resolved,
            Err(error) => {
                let failure = DnsLookupError::new(host, DnsLookupFailure::NameNotFound);
                return Err(Report::new(failure).attach_printable(error));
            }
        };
        Ok(resolved.collect())
    }
}
