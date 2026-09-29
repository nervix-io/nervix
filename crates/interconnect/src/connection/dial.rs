//! Finding the address one outbound pool connection dials.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** How a registered peer endpoint is dialled through a resolved address.
//! - **Depends on.** The parent connection state's registered targets and peer resolver, the shared
//!   DNS setup budget, and the socket seam.
//! - **Must not know.** TLS, HTTP/2, or the operations a connection will carry.

use std::net::SocketAddr;

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_dns::ConnectionBudget;
use nervix_models::NodeEndpoint;
use nervix_primitives::{net::TcpStream, time::timeout};
use tracing::debug;
use triomphe::Arc;

use super::{ConnectionSlotKey, TransportState};
use crate::TransportError;

/// How the connections of one registered peer endpoint find the address they dial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutboundDial {
    /// Resolve the advertised endpoint again for every connection attempt.
    Advertised,
    /// Dial the one address a bootstrap exchange authenticated for the endpoint.
    Authenticated(SocketAddr),
}

/// A TCP stream to one of an endpoint's addresses, and which address it reached.
pub(super) struct DialedStream {
    pub(super) stream: TcpStream,
    pub(super) addr: SocketAddr,
}

impl TransportState {
    /// Open a TCP stream to the slot's endpoint within `budget`.
    ///
    /// An advertised endpoint is resolved again for this attempt, so an expired DNS answer is
    /// replaced before the transport dials, and the lookup spends the same setup budget as the
    /// connection. The addresses are then tried in resolution order, each within an equal share of
    /// what remains of the budget, so an address that cannot connect leaves time for the next.
    pub(super) async fn dial(
        &self,
        key: &ConnectionSlotKey,
        budget: &ConnectionBudget,
    ) -> Result<DialedStream, Report<TransportError>> {
        let dial = self.current_dial(key)?;
        let addresses = match dial {
            OutboundDial::Authenticated(addr) => vec![addr],
            OutboundDial::Advertised => {
                let resolved = self
                    .resolver
                    .resolve(key.endpoint.host(), key.endpoint.port(), budget.remaining())
                    .await;
                match resolved {
                    Ok(addresses) => addresses,
                    Err(report) => {
                        let failure = report.current_context().failure();
                        return Err(report.change_context(TransportError::Resolution {
                            endpoint: key.endpoint.clone(),
                            failure,
                        }));
                    }
                }
            }
        };
        let mut last_failure = None;
        for attempt in budget.attempts(&addresses) {
            nervix_primitives::task::consume_budget().await;
            let addr = attempt.address;
            let share = attempt.budget;
            match timeout(share, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    return Ok(DialedStream { stream, addr });
                }
                Ok(Err(error)) => {
                    debug!(%error, endpoint = %key.endpoint, %addr, "interconnect address refused");
                    last_failure = Some(TransportError::Io(error));
                }
                Err(_) => {
                    debug!(endpoint = %key.endpoint, %addr, "interconnect address did not connect");
                    last_failure = Some(TransportError::ConnectionSetupTimeout {
                        peer: NodeEndpoint::from(addr),
                        timeout: share,
                    });
                }
            }
        }
        let last_failure = last_failure.assured("a resolved endpoint has at least one address");
        Err(Report::new(last_failure))
    }

    /// How the slot's endpoint is dialled now. A slot whose endpoint has been replaced or removed is
    /// being cancelled, and its attempt ends here.
    fn current_dial(
        &self,
        key: &ConnectionSlotKey,
    ) -> Result<OutboundDial, Report<TransportError>> {
        let current = self
            .targets
            .get(&key.node_id)
            .map(|current| Arc::clone(current.value()));
        let Some(current) = current else {
            return Err(Report::new(TransportError::Closed(key.endpoint.clone())));
        };
        if current.endpoint != key.endpoint {
            return Err(Report::new(TransportError::Closed(key.endpoint.clone())));
        }
        Ok(current.dial)
    }
}
