//! Sockets, selected for the build's execution mode: TCP listeners and streams with their owned
//! halves, UDP sockets and, on Unix, local sockets.
//!
//! Ordinary execution takes Tokio's sockets over the operating system's network. A Turmoil build
//! takes Turmoil's instead: every listener, stream and datagram socket belongs to the simulated host
//! whose task creates it and crosses only the simulated network, and [`lookup_host`], which only
//! this mode has, answers from the simulated DNS table. Turmoil has neither a socket that is
//! configured before it connects or listens nor a local socket, so a Turmoil build has neither.
//!
//! No model checker simulates a network. Shuttle and Loom builds take Tokio's sockets, outside every
//! model. A Shuttle execution has no Tokio reactor, so creating a socket inside a check panics and
//! fails the check, and Loom model code may not name one.
//!
//! This family carries bytes and never decides what a name means. Addresses are the standard
//! library's values and the byte-stream traits are Tokio's in every mode. A node resolves names
//! through its own resolver in `nervix-dns`, and no mode but Turmoil's offers a lookup here, so no
//! caller reaches the operating system's resolver through this module.

#[cfg(not(feature = "turmoil"))]
pub use tokio::net::{TcpListener, TcpSocket, TcpStream, UdpSocket, tcp};
#[cfg(all(unix, not(feature = "turmoil")))]
pub use tokio::net::{UnixListener, UnixStream, unix};
#[cfg(feature = "turmoil")]
pub use turmoil::net::{TcpListener, TcpStream, UdpSocket, lookup_host, tcp};

/// Configure kernel keepalive on an already selected native TCP stream.
///
/// This borrows its descriptor; it creates no socket and changes no ownership. A simulated
/// socket has no kernel keepalive options, so this operation is absent in Turmoil.
///
/// ```no_run
/// use nervix_primitives::net::{TcpStream, configure_tcp_keepalive};
/// use std::time::Duration;
/// fn configure(stream: &TcpStream) -> std::io::Result<()> {
///     configure_tcp_keepalive(stream, Duration::from_secs(60), Duration::from_secs(10), 3)
/// }
/// ```
///
/// A listener has no connected peer on which to configure keepalive:
///
/// ```compile_fail
/// use nervix_primitives::net::{TcpListener, configure_tcp_keepalive};
/// use std::time::Duration;
/// fn configure(listener: &TcpListener) -> std::io::Result<()> {
///     configure_tcp_keepalive(listener, Duration::from_secs(60), Duration::from_secs(10), 3)
/// }
/// ```
#[cfg(not(feature = "turmoil"))]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        library,
        reason = "native socket configuration preserves the selected stream's I/O error contract"
    )
)]
pub fn configure_tcp_keepalive(
    stream: &TcpStream,
    time: std::time::Duration,
    interval: std::time::Duration,
    retries: u32,
) -> std::io::Result<()> {
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(time)
        .with_interval(interval)
        .with_retries(retries);
    socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive)
}
