//! TCP and DNS operations used by the production interconnect transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Selection of the socket and resolver implementation at the I/O boundary.
//! - **Depends on.** Tokio in production and Turmoil in the dedicated simulation build.
//! - **Must not know.** TLS, HTTP/2, peer identity, or transport operations.

#[cfg(not(feature = "turmoil"))]
pub(crate) use tokio::net::{TcpListener, TcpStream, lookup_host};
#[cfg(feature = "turmoil")]
pub(crate) use turmoil::net::{TcpListener, TcpStream, lookup_host};
