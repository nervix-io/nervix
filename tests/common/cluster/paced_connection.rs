//! The two TCP sockets a paced interconnect connection forwards between.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Connecting one accepted socket to its private target with TCP_NODELAY on both legs.
//! - **Depends on.** Primitive TCP sockets and the target's literal address.
//! - **Must not know.** Byte cadence, TLS records, HTTP/2 frames or product deadlines.

use std::{io, net::SocketAddr};

use nervix_primitives::net::TcpStream;

pub(super) struct PacedConnection {
    pub(super) incoming: TcpStream,
    pub(super) outgoing: TcpStream,
}

impl PacedConnection {
    pub(super) async fn connect(incoming: TcpStream, target: SocketAddr) -> io::Result<Self> {
        incoming.set_nodelay(true)?;
        let outgoing = TcpStream::connect(target).await?;
        outgoing.set_nodelay(true)?;
        Ok(Self { incoming, outgoing })
    }
}
