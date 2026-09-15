//! A client connection a public listener accepted, which ends when the node closes admission.
//!
//! Layer: edges.
//!
//! - **Owns.** Accepting connections for a public listener and failing every read and write on
//!   them once admission closes, so the protocol served on a connection ends together with the
//!   requests it carries.
//! - **Depends on.** Tokio's listener and I/O traits, the admission cancellation token, and
//!   tonic's connection information.
//! - **Must not know.** The protocol carried on a connection or what its requests do.
//!
//! A gRPC server's graceful shutdown waits until every connection it accepted has closed, and a
//! request that waits for its client, such as a resource upload waiting for its next chunk, would
//! hold that wait open for as long as the client chooses. Ending the connection itself releases
//! the wait without asking the client.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Stream, stream};
use nervix_recovery::Reported as _;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::transport::server::Connected;

/// A public listener whose accepted connections end when admission closes.
pub(in crate::application) struct AdmittingListener {
    listener: TcpListener,
    admission: CancellationToken,
}

impl AdmittingListener {
    pub(in crate::application) fn new(listener: TcpListener, admission: CancellationToken) -> Self {
        Self {
            listener,
            admission,
        }
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
                Ok(AdmittedConnection::new(stream, &self.admission))
            }
            Err(error) => Err(error),
        };
        Some((connection, self))
    }
}

/// An accepted connection whose reads and writes fail once admission closes.
pub(in crate::application) struct AdmittedConnection<IO> {
    io: IO,
    admission: Admission,
}

/// Whether the listener that accepted a connection still admits clients.
enum Admission {
    Open(Pin<Box<WaitForCancellationFutureOwned>>),
    Closed,
}

impl Admission {
    /// Reports whether admission has closed. While it is still open, `context` is woken when it
    /// closes.
    fn poll_closed(&mut self, context: &mut Context<'_>) -> bool {
        let Self::Open(closing) = self else {
            return true;
        };
        if closing.as_mut().poll(context).is_pending() {
            return false;
        }
        *self = Self::Closed;
        true
    }
}

impl<IO> AdmittedConnection<IO> {
    fn new(io: IO, admission: &CancellationToken) -> Self {
        Self {
            io,
            admission: Admission::Open(Box::pin(admission.clone().cancelled_owned())),
        }
    }
}

fn admission_closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "the node closed admission",
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
        if connection.admission.poll_closed(context) {
            return Poll::Ready(Err(admission_closed()));
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
        if connection.admission.poll_closed(context) {
            return Poll::Ready(Err(admission_closed()));
        }
        Pin::new(&mut connection.io).poll_write(context, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let connection = self.get_mut();
        if connection.admission.poll_closed(context) {
            return Poll::Ready(Err(admission_closed()));
        }
        Pin::new(&mut connection.io).poll_write_vectored(context, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let connection = self.get_mut();
        if connection.admission.poll_closed(context) {
            return Poll::Ready(Err(admission_closed()));
        }
        Pin::new(&mut connection.io).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let connection = self.get_mut();
        if connection.admission.poll_closed(context) {
            return Poll::Ready(Err(admission_closed()));
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

    #[tokio::test]
    async fn an_admitted_connection_carries_data_while_admission_is_open() {
        let admission = CancellationToken::new();
        let (mut client, server) = tokio::io::duplex(64);
        let mut connection = AdmittedConnection::new(server, &admission);

        client
            .write_all(b"ping")
            .await
            .expect("the client half accepts the write");
        let mut received = [0; 4];
        connection
            .read_exact(&mut received)
            .await
            .expect("an admitted connection reads while admission is open");
        connection
            .write_all(b"pong")
            .await
            .expect("an admitted connection writes while admission is open");

        assert_eq!(&received, b"ping");
    }

    #[tokio::test]
    async fn closing_admission_ends_a_read_that_waits_for_the_client() {
        let admission = CancellationToken::new();
        let (_client, server) = tokio::io::duplex(64);
        let mut connection = AdmittedConnection::new(server, &admission);
        let mut received = [0; 4];
        let mut read = pin!(connection.read(&mut received));
        let mut context = Context::from_waker(Waker::noop());
        assert!(
            read.as_mut().poll(&mut context).is_pending(),
            "a read waits while the client sends nothing"
        );

        admission.cancel();

        let error = read
            .await
            .expect_err("closing admission must end the waiting read");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    }

    #[tokio::test]
    async fn closing_admission_fails_every_later_write() {
        let admission = CancellationToken::new();
        let (_client, server) = tokio::io::duplex(64);
        let mut connection = AdmittedConnection::new(server, &admission);

        admission.cancel();

        let error = connection
            .write_all(b"late")
            .await
            .expect_err("a connection must not write after admission closed");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    }
}
