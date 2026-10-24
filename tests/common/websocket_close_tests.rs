//! Protocol regressions for a fixture replying to an observed WebSocket Close.
//!
//! Layer: test harness.
//! - **Owns.** Real frame parsing over a transport whose reply writes can fail deterministically.
//! - **Depends on.** The fixture close owner and the WebSocket protocol driver.
//! - **Must not know.** Scenario state, connector internals or node shutdown policy.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::StreamExt as _;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WebsocketError, Message,
        protocol::{Role, frame::coding::CloseCode},
    },
};

use crate::websocket_close::reply_to_close;

/// One masked client Close frame carrying the normal closing status, 1000.
const CLIENT_CLOSE: &[u8] = &[0x88, 0x82, 0x11, 0x22, 0x33, 0x44, 0x12, 0xca];

struct ClosingTransport {
    read: usize,
    write_failure: Option<io::ErrorKind>,
    written: Vec<u8>,
}

impl AsyncRead for ClosingTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = &CLIENT_CLOSE[self.read..];
        let count = remaining.len().min(buffer.remaining());
        buffer.put_slice(&remaining[..count]);
        self.read += count;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ClosingTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(kind) = self.write_failure {
            return Poll::Ready(Err(io::Error::from(kind)));
        }
        self.written.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn received_close(write_failure: Option<io::ErrorKind>) -> WebSocketStream<ClosingTransport> {
    let mut socket = WebSocketStream::from_raw_socket(
        ClosingTransport {
            read: 0,
            write_failure,
            written: Vec::new(),
        },
        Role::Server,
        None,
    )
    .await;
    assert!(
        matches!(socket.next().await, Some(Ok(Message::Close(Some(frame)))) if frame.code == CloseCode::Normal)
    );
    socket
}

#[nervix_primitives::test]
async fn a_close_reply_finishes_when_the_initiator_has_already_ended_tcp() {
    for kind in [io::ErrorKind::ConnectionReset, io::ErrorKind::BrokenPipe] {
        let mut socket = received_close(Some(kind)).await;
        reply_to_close(&mut socket)
            .await
            .expect("the fixture observed Close before its reply found TCP ended");
    }
}

#[nervix_primitives::test]
async fn a_close_reply_preserves_an_unexpected_transport_failure() {
    let mut socket = received_close(Some(io::ErrorKind::PermissionDenied)).await;
    let error = reply_to_close(&mut socket)
        .await
        .expect_err("an unexpected reply failure remains a fixture failure");
    let cause = error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<WebsocketError>())
        .expect("the I/O boundary preserves the typed WebSocket cause");
    assert!(
        matches!(cause, WebsocketError::Io(error) if error.kind() == io::ErrorKind::PermissionDenied)
    );
}

#[nervix_primitives::test]
async fn a_close_reply_sends_the_observed_normal_status() {
    let mut socket = received_close(None).await;
    reply_to_close(&mut socket)
        .await
        .expect("the initiator still reads the reply");
    assert_eq!(socket.get_ref().written, [0x88, 0x02, 0x03, 0xe8]);
}
