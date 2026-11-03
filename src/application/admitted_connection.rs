//! A client connection a public listener accepted, which ends when the stopping node cuts it.
//!
//! Layer: edges.
//!
//! - **Owns.** Accepting connections for a public listener and failing every read and write on
//!   them once the node cuts the connections it accepted, so the protocol served on a connection
//!   ends together with the requests it carries.
//! - **Depends on.** Tokio's listener and I/O traits, the cancellation token that cuts accepted
//!   connections, and tonic's connection information.
//! - **Must not know.** The protocol carried on a connection or what its requests do.
//!
//! Closing admission does not end a connection: the server serving the listener stops accepting
//! and asks each connection to take no new call, and a session still writes the ending that tells
//! its client why it ends. A gRPC server's graceful shutdown then waits until every connection it
//! accepted has closed, and a request that waits for its client, such as a resource upload waiting
//! for its next chunk or a session whose client reads nothing, would hold that wait open for as
//! long as the client chooses. Cutting the connection itself, once shutdown's grace period has
//! passed, releases the wait without asking the client.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Stream, stream};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{CancellationToken, WaitForCancellationFutureOwned},
};
use nervix_recovery::Reported as _;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::Connected;

/// A public listener whose accepted connections end when the node cuts them.
pub(in crate::application) struct AdmittingListener {
    listener: TcpListener,
    cut: CancellationToken,
}

impl AdmittingListener {
    pub(in crate::application) fn new(listener: TcpListener, cut: CancellationToken) -> Self {
        Self { listener, cut }
    }

    /// Every connection the listener accepts, in the form a gRPC server serves.
    pub(in crate::application) fn into_connections(
        self,
    ) -> impl Stream<Item = io::Result<AdmittedConnection<TcpStream>>> {
        stream::unfold(self, Self::accept_next)
    }

    async fn accept_next(self) -> Option<(io::Result<AdmittedConnection<TcpStream>>, Self)> {
        let accepted = self.listener.accept().await;
        let connection = match accepted {
            Ok((stream, _)) => {
                stream
                    .set_nodelay(true)
                    .reported("disabling Nagle on an accepted gRPC connection");
                Ok(AdmittedConnection::new(stream, &self.cut))
            }
            Err(error) => Err(error),
        };
        Some((connection, self))
    }
}

/// An accepted connection whose reads and writes fail once the node cuts it.
pub(in crate::application) struct AdmittedConnection<IO> {
    io: IO,
    cut: Cut,
}

/// Whether the node has cut the connections its listener accepted.
enum Cut {
    Pending(Pin<Box<WaitForCancellationFutureOwned>>),
    Done,
}

impl Cut {
    /// Reports whether the node has cut the connection. Until it has, `context` is woken when it
    /// does.
    fn poll_done(&mut self, context: &mut Context<'_>) -> bool {
        let Self::Pending(cutting) = self else {
            return true;
        };
        if cutting.as_mut().poll(context).is_pending() {
            return false;
        }
        *self = Self::Done;
        true
    }
}

impl<IO> AdmittedConnection<IO> {
    fn new(io: IO, cut: &CancellationToken) -> Self {
        Self {
            io,
            cut: Cut::Pending(Box::pin(cut.clone().cancelled_owned())),
        }
    }
}

fn connection_cut() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "the stopping node cut the connection",
    )
}

impl<IO> AsyncRead for AdmittedConnection<IO>
where
    IO: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let connection = self.get_mut();
        if connection.cut.poll_done(context) {
            return Poll::Ready(Err(connection_cut()));
        }
        Pin::new(&mut connection.io).poll_read(context, buf)
    }
}

impl<IO> AsyncWrite for AdmittedConnection<IO>
where
    IO: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let connection = self.get_mut();
        if connection.cut.poll_done(context) {
            return Poll::Ready(Err(connection_cut()));
        }
        Pin::new(&mut connection.io).poll_write(context, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let connection = self.get_mut();
        if connection.cut.poll_done(context) {
            return Poll::Ready(Err(connection_cut()));
        }
        Pin::new(&mut connection.io).poll_write_vectored(context, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let connection = self.get_mut();
        if connection.cut.poll_done(context) {
            return Poll::Ready(Err(connection_cut()));
        }
        Pin::new(&mut connection.io).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let connection = self.get_mut();
        if connection.cut.poll_done(context) {
            return Poll::Ready(Err(connection_cut()));
        }
        Pin::new(&mut connection.io).poll_shutdown(context)
    }
}

impl<IO> Connected for AdmittedConnection<IO>
where
    IO: Connected,
{
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}

#[cfg(test)]
mod tests {
    use std::{pin::pin, task::Waker};

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[nervix_primitives::test]
    async fn an_admitted_connection_carries_data_until_it_is_cut() {
        let cut = CancellationToken::new();
        let (mut client, server) = tokio::io::duplex(64);
        let mut connection = AdmittedConnection::new(server, &cut);

        client
            .write_all(b"ping")
            .await
            .expect("the client half accepts the write");
        let mut received = [0; 4];
        connection
            .read_exact(&mut received)
            .await
            .expect("an admitted connection reads until it is cut");
        connection
            .write_all(b"pong")
            .await
            .expect("an admitted connection writes until it is cut");

        assert_eq!(&received, b"ping");
    }

    #[nervix_primitives::test]
    async fn cutting_the_connection_ends_a_read_that_waits_for_the_client() {
        let cut = CancellationToken::new();
        let (_client, server) = tokio::io::duplex(64);
        let mut connection = AdmittedConnection::new(server, &cut);
        let mut received = [0; 4];
        let mut read = pin!(connection.read(&mut received));
        let mut context = Context::from_waker(Waker::noop());
        assert!(
            read.as_mut().poll(&mut context).is_pending(),
            "a read waits while the client sends nothing"
        );

        cut.cancel();

        let error = read
            .await
            .expect_err("cutting the connection must end the waiting read");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    }

    #[nervix_primitives::test]
    async fn cutting_the_connection_fails_every_later_write() {
        let cut = CancellationToken::new();
        let (_client, server) = tokio::io::duplex(64);
        let mut connection = AdmittedConnection::new(server, &cut);

        cut.cancel();

        let error = connection
            .write_all(b"late")
            .await
            .expect_err("a connection must not write after it was cut");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    }
}
