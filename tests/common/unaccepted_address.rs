//! A loopback address a client can dial and never connect to, for a client that must give up on
//! one resolved address and try the next.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One listening socket at a chosen address and the one connection that holds its
//!   accept queue full, so the kernel drops every later handshake and a connection attempt to the
//!   address waits until its client abandons it.
//! - **Depends on.** Tokio's TCP sockets through the primitives boundary.
//! - **Must not know.** DNS, Nervix, or what a client would have sent once connected.
//!
//! # Why a full accept queue
//!
//! An address nothing listens on refuses a connection at once, and a silent endpoint accepts it,
//! so neither holds an attempt before its connection exists. A listener whose backlog is zero
//! admits one completed connection to its accept queue, and Linux drops every handshake that
//! arrives while that queue is full, without answering it. The listener never accepts, so the
//! connection that fills the queue stays in it for as long as this fixture lives, and every other
//! client's attempt keeps retransmitting its first segment until it gives up.

use std::{io, net::SocketAddr};

use nervix_primitives::net::{TcpListener, TcpSocket, TcpStream};

/// The backlog that admits exactly one queued connection.
const FULL_AFTER_ONE_CONNECTION: u32 = 0;

/// An address whose every connection attempt waits until its client gives up.
#[derive(Debug)]
pub(crate) struct UnacceptedAddress {
    /// Never accepted from, so its queue stays full.
    _listener: TcpListener,
    /// The connection that fills the listener's queue.
    _queued: TcpStream,
}

impl UnacceptedAddress {
    /// Listens at `address` and fills the listener's accept queue.
    pub(crate) async fn listen(address: SocketAddr) -> io::Result<Self> {
        let socket = match address {
            SocketAddr::V4(_) => TcpSocket::new_v4()?,
            SocketAddr::V6(_) => TcpSocket::new_v6()?,
        };
        socket.bind(address)?;
        let listener = socket.listen(FULL_AFTER_ONE_CONNECTION)?;
        let queued = TcpStream::connect(listener.local_addr()?).await?;
        Ok(Self {
            _listener: listener,
            _queued: queued,
        })
    }
}
