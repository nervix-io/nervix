//! The outbound connection's outgoing buffer: what its transport holds of the frames this client
//! writes, and how the client's own frames wait for room that automatic replies filled.
//!
//! Layer: test harness.
//! - **Owns.** Pinning the transport limits a client's keys give its connection, filling the
//!   write buffer of a transport whose peer takes nothing, and replaying what a peer that resumed
//!   reading was written.
//! - **Depends on.** The ingestor's fixture, plan and connection owners, the client vocabulary,
//!   execution primitives and the WebSocket driver.
//! - **Must not know.** Registry, graph or host synchronization internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test boundary declared above")
)]

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{FutureExt as _, SinkExt as _};
use nervix_primitives::task::unconstrained;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WebsocketError,
        protocol::{
            CloseFrame, Role, WebSocket,
            frame::{Frame, coding::CloseCode},
        },
    },
};

use super::*;

fn entries(keys: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
    keys.iter()
        .map(|(key, value)| ClientConfigEntry {
            key: (*key).to_string(),
            value: (*value).to_string(),
        })
        .collect()
}

/// The transport limits of a client configured with `keys` beside an endpoint.
fn transport(keys: &[(&str, &str)]) -> WebSocketConfig {
    let mut config = entries(&[("endpoint", "ws://peer.test/events")]);
    config.extend(entries(keys));
    let client = WebsocketsClientConfig::parse(&config).assured("the fixture keys are valid");
    WebsocketsConnectionIngestorPlan::transport_config(&client)
        .assured("the fixture's outgoing buffer fits every supported address space")
}

#[nervix_primitives::test]
async fn a_connection_holds_its_write_buffer_and_one_frame_of_the_largest_message() {
    struct Sizing {
        keys: &'static [(&'static str, &'static str)],
        write_buffer: usize,
        capacity: usize,
    }
    for sizing in [
        // The defaults: the transport's own 128 KiB write buffer, and a 64 MiB message.
        Sizing {
            keys: &[],
            write_buffer: 131_072,
            capacity: 131_072 + 67_108_864 + 14,
        },
        Sizing {
            keys: &[("write_buffer_size", "1024"), ("max_message_size", "65536")],
            write_buffer: 1024,
            capacity: 1024 + 65_536 + 14,
        },
        // A pong, or the reply to a peer's Close, echoes up to 125 bytes whatever the client's
        // own messages may hold.
        Sizing {
            keys: &[("write_buffer_size", "0"), ("max_message_size", "16")],
            write_buffer: 0,
            capacity: 125 + 14,
        },
    ] {
        let (ingestor, _peer) =
            tests::connected_pair_with(entries(sizing.keys), WebsocketsUpgradeHeaders::default())
                .await;
        let connection = ingestor
            .connection
            .as_ref()
            .assured("the pair is connected");
        let config = connection.relay.get_config();
        assert_eq!(
            config.write_buffer_size, sizing.write_buffer,
            "{:?}",
            sizing.keys
        );
        assert_eq!(
            config.max_write_buffer_size, sizing.capacity,
            "{:?}",
            sizing.keys
        );
    }
}

#[test]
fn an_outgoing_buffer_beyond_the_address_space_is_no_capacity() {
    let beyond_by_its_buffer = WebsocketOutgoingBuffer {
        write_buffer: usize::MAX,
        largest_message: CONTROL_FRAME_PAYLOAD_BYTES,
    };
    assert_eq!(beyond_by_its_buffer.capacity(), None);
    let beyond_by_its_header = WebsocketOutgoingBuffer {
        write_buffer: 0,
        largest_message: usize::MAX - 13,
    };
    assert_eq!(beyond_by_its_header.capacity(), None);
    let largest = WebsocketOutgoingBuffer {
        write_buffer: 0,
        largest_message: usize::MAX - 14,
    };
    assert_eq!(largest.capacity(), Some(usize::MAX));
}

/// A transport whose peer takes nothing and sends nothing: every read and write would block.
struct Unwritable;

impl io::Read for Unwritable {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

impl io::Write for Unwritable {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

/// Whether the transport took `frame` into its buffer: a write it could not finish keeps the frame
/// queued, and only a frame that does not fit is refused.
fn queued(written: tokio_tungstenite::tungstenite::Result<()>) -> bool {
    match written {
        Err(WebsocketError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => true,
        Err(WebsocketError::WriteBufferFull(_)) => false,
        other => panic!("a transport whose peer takes nothing queues or refuses, not {other:?}"),
    }
}

#[test]
fn the_largest_message_fits_onto_a_full_write_buffer_and_one_more_byte_does_not() {
    const WRITE_BUFFER: usize = 1024;
    const LARGEST_MESSAGE: usize = 65_536;
    let config = transport(&[("write_buffer_size", "1024"), ("max_message_size", "65536")]);
    for (message, fits) in [(LARGEST_MESSAGE, true), (LARGEST_MESSAGE + 1, false)] {
        let mut client = WebSocket::from_raw_socket(Unwritable, Role::Client, Some(config));
        // One frame whose masked 8-byte header and payload fill the write buffer exactly, which
        // the transport collects without writing.
        client
            .write(Message::Binary(vec![0; WRITE_BUFFER - 8]))
            .assured("a frame within the write buffer is collected without a write");
        // The largest message travels with the largest header, 14 bytes.
        let written = client.write(Message::Binary(vec![0; message]));
        assert_eq!(queued(written), fits, "a {message}-byte message");
    }
}

#[test]
fn a_full_size_control_frame_fits_whatever_the_largest_message() {
    let config = transport(&[("write_buffer_size", "0"), ("max_message_size", "16")]);
    let mut client = WebSocket::from_raw_socket(Unwritable, Role::Client, Some(config));
    // A pong and the reply to a peer's Close echo up to 125 bytes, which a ping of that size
    // matches frame for frame.
    let written = client.write(Message::Ping(vec![0; 125]));
    assert!(queued(written), "a 125-byte control frame fits");
}

/// A peer that sent `incoming` and then stopped reading: its transport takes no byte until the
/// test resumes the peer, and then takes everything the client writes.
struct StoppedReading {
    incoming: io::Cursor<Vec<u8>>,
    reading: bool,
    read: Vec<u8>,
}

impl StoppedReading {
    fn sending(frames: Vec<Frame>) -> Self {
        let mut incoming = Vec::new();
        for frame in frames {
            frame
                .format(&mut incoming)
                .assured("a frame formats into memory");
        }
        Self {
            incoming: io::Cursor::new(incoming),
            reading: false,
            read: Vec::new(),
        }
    }

    /// Resumes reading. The tests call it between the client's operations, while nothing waits on
    /// a write, so no waker needs to be woken.
    fn resume_reading(&mut self) {
        self.reading = true;
    }
}

impl AsyncRead for StoppedReading {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().incoming).poll_read(context, buffer)
    }
}

impl AsyncWrite for StoppedReading {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let peer = self.get_mut();
        if !peer.reading {
            // The tests poll a waiting write once and never rely on being woken.
            return Poll::Pending;
        }
        peer.read.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.reading {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Without a write buffer and with a 16-byte largest message, the outgoing buffer holds 139
/// bytes, one full-size control frame. The automatic pongs to three 63-byte pings are 69 bytes
/// each: the reads buffer the first two, 138 bytes, and keep the third pending.
async fn pongs_filling_the_buffer(mut frames_after: Vec<Frame>) -> WebSocketStream<StoppedReading> {
    let config = transport(&[("write_buffer_size", "0"), ("max_message_size", "16")]);
    let mut frames: Vec<Frame> = (0..3).map(|_| Frame::ping(vec![7; 63])).collect();
    frames.append(&mut frames_after);
    let mut client = WebSocketStream::from_raw_socket(
        StoppedReading::sending(frames),
        Role::Client,
        Some(config),
    )
    .await;
    for _ in 0..3 {
        let read = client
            .next()
            .await
            .assured("the peer sent three pings")
            .assured("each ping is a valid frame");
        assert!(matches!(read, Message::Ping(_)), "got {read:?}");
    }
    client
}

#[nervix_primitives::test]
async fn the_clients_own_frame_waits_for_room_that_automatic_pongs_filled() {
    let mut client = pongs_filling_the_buffer(Vec::new()).await;
    // A 6-byte ping does not fit beside the 138 bytes of pongs, so a write that does not wait for
    // room is refused.
    let refused = unconstrained(client.send(Message::Ping(Vec::new()))).now_or_never();
    assert!(
        matches!(refused, Some(Err(WebsocketError::WriteBufferFull(_)))),
        "got {refused:?}"
    );
    // The client's own frame waits for the peer to take the pongs instead.
    let waiting = unconstrained(client.send_own(Message::Ping(Vec::new()))).now_or_never();
    assert!(
        waiting.is_none(),
        "the client's own ping waits for room, got {waiting:?}"
    );
}

#[nervix_primitives::test]
async fn a_peer_close_is_answered_once_the_pongs_before_it_are_written() {
    let close = Frame::close(Some(CloseFrame {
        code: CloseCode::Policy,
        reason: "bye".into(),
    }));
    let mut client = pongs_filling_the_buffer(vec![close]).await;
    let read = client
        .next()
        .await
        .assured("the peer sent its Close after the pings")
        .assured("the Close is a valid frame");
    assert!(matches!(read, Message::Close(Some(_))), "got {read:?}");
    // The reply does not fit beside the buffered pongs until the peer takes them.
    client.get_mut().resume_reading();
    client
        .flush_reply()
        .await
        .assured("a peer that reads again takes everything");
    let read = std::mem::take(&mut client.get_mut().read);
    let mut replay = WebSocketStream::from_raw_socket(
        tokio::io::join(io::Cursor::new(read), tokio::io::sink()),
        Role::Server,
        None,
    )
    .await;
    let mut pongs = 0_usize;
    loop {
        nervix_primitives::task::consume_budget().await;
        match replay.next().await {
            Some(Ok(Message::Pong(payload))) => {
                assert_eq!(payload, vec![7; 63]);
                pongs += 1;
            }
            Some(Ok(Message::Close(Some(frame)))) => {
                assert_eq!(frame.code, CloseCode::Policy);
                assert_eq!(frame.reason, "bye");
                break;
            }
            other => panic!(
                "the peer reads the buffered pongs and then the reply to its Close, not {other:?}"
            ),
        }
    }
    assert_eq!(pongs, 2, "the pongs the buffer held go first");
}
