//! The connections a node's public listeners accept, which a failed accept never stops.
//!
//! Layer: edges.
//!
//! - **Owns.** Taking the next connection a public listener accepts, disabling Nagle's algorithm
//!   on it, and what an accept that failed means.
//! - **Depends on.** The listening socket, and in a testing build the seam that fails one of its
//!   accepts.
//! - **Must not know.** The protocol served on an accepted connection, or how the node stops.
//!
//! A public listener that stops requests the node's stop, so a listener that ended on a failed
//! accept would end the node. None does: a listener stops only when the node closes admission. A
//! connection its client reset or abandoned before the listener took it fails only itself, and the
//! listener accepts the next at once. Any other failure, such as the process running out of file
//! descriptors, is a fault of the node rather than of one connection. The listener reports it and
//! accepts again, so it takes connections as soon as the node has room for them: the first such
//! failure after the listener last took a connection is reported at `warn` and the ones repeating
//! it at `debug`, and the connection that ends the run is reported at `info` with how many accepts
//! failed in it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{io, net::SocketAddr};

use meticulous::OptionExt as _;
use nervix_primitives::net::{TcpListener, TcpStream};
use nervix_recovery::Reported as _;
use tracing::{debug, info, warn};

use crate::ConfiguredFaultInjection;
#[cfg(feature = "testing")]
use crate::fault_injection::ListenerAcceptFaults;

/// The public listeners of a node, as their records name them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(in crate::application) enum PublicListenerKind {
    /// The native gRPC session listener.
    Grpc,
    /// The HTTP listener of the node's `VHOST` endpoints.
    Http,
    /// The HTTPS listener of the node's `VHOST` endpoints.
    Https,
    /// The observability listener.
    Observability,
    /// The web console listener.
    Console,
    /// The web console's TLS listener.
    ConsoleTls,
}

/// How a public listener of an ordinary build accepts: as its socket does.
#[cfg(not(feature = "testing"))]
struct ListenerAcceptFaults {
    _marker: (),
}

#[cfg(not(feature = "testing"))]
impl ListenerAcceptFaults {
    async fn accept(&self, listener: &TcpListener) -> io::Result<(TcpStream, SocketAddr)> {
        listener.accept().await
    }
}

/// The accepts of a testing build's public listener, which fail where a scenario armed it.
#[cfg(feature = "testing")]
fn listener_accept_faults(injection: &ConfiguredFaultInjection) -> ListenerAcceptFaults {
    injection.listener_accept_faults()
}

/// The accepts of an ordinary build's public listener, which no scenario can fail.
#[cfg(not(feature = "testing"))]
fn listener_accept_faults(_injection: &ConfiguredFaultInjection) -> ListenerAcceptFaults {
    ListenerAcceptFaults { _marker: () }
}

/// One public listener, which accepts until the node closes admission whatever an accept meets.
pub(in crate::application) struct PublicListener {
    listener: TcpListener,
    kind: PublicListenerKind,
    /// The accepts that failed since the listener last took a connection.
    failed_accepts: u64,
    faults: ListenerAcceptFaults,
}

impl PublicListener {
    pub(in crate::application) fn new(
        listener: TcpListener,
        kind: PublicListenerKind,
        injection: &ConfiguredFaultInjection,
    ) -> Self {
        Self {
            listener,
            kind,
            failed_accepts: 0,
            faults: listener_accept_faults(injection),
        }
    }

    /// The next connection a client opened to this listener, with the address it came from, once
    /// the listener takes one. A failed accept is reported and the listener accepts again; it
    /// never ends the wait.
    pub(in crate::application) async fn accept(&mut self) -> (TcpStream, SocketAddr) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let accepted = self.faults.accept(&self.listener).await;
            match accepted {
                Ok((stream, peer)) => {
                    self.took_connection();
                    stream
                        .set_nodelay(true)
                        .reported("disabling Nagle on an accepted connection");
                    return (stream, peer);
                }
                Err(error) => self.failed(&error),
            }
        }
    }

    fn took_connection(&mut self) {
        if self.failed_accepts == 0 {
            return;
        }
        let listener: &'static str = self.kind.into();
        info!(
            listener,
            failed_accepts = self.failed_accepts,
            "a public listener accepts connections again"
        );
        self.failed_accepts = 0;
    }

    fn failed(&mut self, error: &io::Error) {
        let listener: &'static str = self.kind.into();
        if fails_only_its_connection(error) {
            debug!(listener, %error, "a connection ended before its listener accepted it");
            return;
        }
        self.failed_accepts = self.failed_accepts.checked_add(1).assured(
            "a listener retries an accept at most once per poll, so its failures in one run take \
             centuries to reach u64::MAX",
        );
        if self.failed_accepts == 1 {
            warn!(
                listener,
                %error,
                "a public listener failed to accept a connection; it keeps accepting"
            );
        } else {
            debug!(listener, %error, failed_accepts = self.failed_accepts, "a public listener failed to accept a connection again");
        }
    }
}

/// Whether an accept failed because of the connection it was taking, which its client reset or
/// abandoned before the listener took it, or because a signal interrupted it, rather than because
/// of the node.
fn fails_only_its_connection(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::Interrupted
    )
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use std::time::Duration;

    use nervix_primitives::{net::TcpStream, time::timeout};

    use super::*;
    use crate::{captured_logs::CapturedLogs, fault_injection::FaultInjection};

    const WAIT: Duration = Duration::from_secs(30);

    #[nervix_primitives::test]
    async fn a_failed_accept_is_reported_and_the_listener_takes_the_next_connection() {
        let injection = FaultInjection::default();
        let socket = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port binds");
        let address = socket
            .local_addr()
            .expect("a bound listener has an address");
        let mut listener = PublicListener::new(socket, PublicListenerKind::Console, &injection);
        injection.fail_next_listener_accept(address);
        let client = TcpStream::connect(address)
            .await
            .expect("the listener's backlog takes the connection");

        let logs = CapturedLogs::default();
        // `#[nervix_primitives::test]` runs the whole future on the calling thread, so the
        // thread-local default the guard installs stays in force across the await.
        let guard = tracing::subscriber::set_default(logs.subscriber());
        let (accepted, peer) = timeout(WAIT, listener.accept())
            .await
            .expect("the listener takes the connection after its failed accept");
        drop(guard);

        timeout(WAIT, injection.listener_accept_failure(address))
            .await
            .expect("the armed failure happened before the connection was taken");
        assert_eq!(
            peer,
            client
                .local_addr()
                .expect("a connected client has an address")
        );
        assert!(
            accepted
                .nodelay()
                .expect("an accepted socket reports its options"),
            "an accepted connection has Nagle's algorithm disabled"
        );
        assert_eq!(
            listener.failed_accepts, 0,
            "taking a connection ends the run"
        );
        let logs = logs.contents();
        assert!(
            logs.contains("a public listener failed to accept a connection; it keeps accepting")
                && logs.contains("listener=\"console\"")
                && logs.contains("injected public listener accept failure"),
            "the failed accept is reported with its listener and error:\n{logs}"
        );
        assert!(
            logs.contains("a public listener accepts connections again")
                && logs.contains("failed_accepts=1"),
            "the connection that ends the run is reported with how many accepts failed:\n{logs}"
        );
    }

    #[test]
    fn only_an_accept_its_connection_failed_is_skipped_without_a_report() {
        for kind in [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::Interrupted,
        ] {
            assert!(
                fails_only_its_connection(&io::Error::from(kind)),
                "{kind:?}"
            );
        }
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::OutOfMemory,
            io::ErrorKind::PermissionDenied,
        ] {
            assert!(
                !fails_only_its_connection(&io::Error::from(kind)),
                "{kind:?}"
            );
        }
    }
}
