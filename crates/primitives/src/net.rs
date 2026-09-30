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
