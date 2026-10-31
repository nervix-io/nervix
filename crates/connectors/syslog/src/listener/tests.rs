//! Syslog stream framing qualification.
//!
//! Layer: test harness.
//! - **Owns.** RFC 6587 frames and failures at every read boundary, the read cursor and its single
//!   compaction per read, the property that framing does not depend on how reads split a stream,
//!   the connection handoff and listener lifecycle that retain frames and established connections,
//!   the deadlines of an accepted connection, and the oversized datagram a listener drops.
//! - **Depends on.** The production stream frame decoder, connection reader, TLS acceptance and UDP
//!   listener, the execution clock's paused test mode, and a generated TLS server identity.
//! - **Must not know.** Codecs, runtime collectors or graph placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    collections::VecDeque,
    future::{Future as _, poll_fn},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use nervix_connector::DeadlineElapsed;
use nervix_models::DeadlineKey;
use nervix_primitives::time::Instant;
use nonzero_ext::nonzero;
use tokio::io::{AsyncWriteExt as _, DuplexStream, ReadBuf};

use super::*;

/// The frames a whole stream produced, and how the stream ended.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Framing {
    frames: Vec<Vec<u8>>,
    ending: Ending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Ending {
    /// Every byte belonged to a complete frame.
    Clean,
    /// The stream ended inside a frame.
    Incomplete,
    /// Framing failed.
    Failed(FrameFailure),
}

/// A framing failure with the values it reports.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FrameFailure {
    MalformedOctetCount,
    InvalidOctetCount,
    OversizedOctetCount { length: usize },
    OversizedNonTransparentFrame,
    OversizedBufferedFrame,
    NonOctetTlsFrame,
}

impl FrameFailure {
    fn of(error: &Report<SyslogFrameError>) -> Self {
        match error.current_context() {
            SyslogFrameError::MalformedOctetCount => Self::MalformedOctetCount,
            SyslogFrameError::InvalidOctetCount { .. } => Self::InvalidOctetCount,
            SyslogFrameError::OversizedOctetCount { length, .. } => {
                Self::OversizedOctetCount { length: *length }
            }
            SyslogFrameError::OversizedNonTransparentFrame { .. } => {
                Self::OversizedNonTransparentFrame
            }
            SyslogFrameError::OversizedBufferedFrame { .. } => Self::OversizedBufferedFrame,
            SyslogFrameError::NonOctetTlsFrame => Self::NonOctetTlsFrame,
        }
    }
}

/// Appends the start of `bytes` through the decoder's read buffer, as one read does, and answers
/// how many bytes the read took.
fn read_into(decoder: &mut StreamFrameDecoder, bytes: &[u8]) -> Result<usize, FrameFailure> {
    let mut buffer = match decoder.read_buffer() {
        Ok(buffer) => buffer,
        Err(error) => return Err(FrameFailure::of(&error)),
    };
    let taken = bytes.len().min(buffer.remaining_mut());
    buffer.put_slice(&bytes[..taken]);
    Ok(taken)
}

/// Frames `stream` the way a connection reads it: every complete frame before the next read, and
/// reads of the sizes in `reads` in turn, each further bounded by what the decoder admits. Without
/// read sizes every read takes as much as the decoder admits.
fn frame_in_reads(
    stream: &[u8],
    reads: &[usize],
    max_message_size: NonZeroUsize,
    allow_non_transparent: bool,
) -> Framing {
    let mut decoder = StreamFrameDecoder::new(max_message_size, allow_non_transparent);
    let mut frames = Vec::new();
    let mut position = 0;
    let mut reads = reads.iter().cycle();
    loop {
        match decoder.next_frame() {
            Ok(Some(frame)) => {
                frames.push(frame);
                continue;
            }
            Ok(None) => {}
            Err(error) => {
                return Framing {
                    frames,
                    ending: Ending::Failed(FrameFailure::of(&error)),
                };
            }
        }
        if position == stream.len() {
            let ending = if decoder.is_empty() {
                Ending::Clean
            } else {
                Ending::Incomplete
            };
            return Framing { frames, ending };
        }
        let read = match reads.next() {
            Some(read) => (*read).max(1),
            None => usize::MAX,
        };
        let end = stream.len().min(position.saturating_add(read));
        match read_into(&mut decoder, &stream[position..end]) {
            Ok(taken) => position += taken,
            Err(failure) => {
                return Framing {
                    frames,
                    ending: Ending::Failed(failure),
                };
            }
        }
    }
}

/// RFC 6587 framing of a whole stream, written from the framing rules rather than from the
/// decoder: a frame beginning with a digit is octet counted, any other is terminated by LF with a
/// CR before it removed, and TLS admits octet counting only. A frame the stream ends inside fails
/// when its bytes already break a rule, and is incomplete otherwise.
fn reference_framing(
    stream: &[u8],
    max_message_size: usize,
    allow_non_transparent: bool,
) -> Framing {
    let mut frames = Vec::new();
    let mut rest = stream;
    let ending = loop {
        let Some(first) = rest.first() else {
            break Ending::Clean;
        };
        if first.is_ascii_digit() {
            let digits = rest.iter().take_while(|byte| byte.is_ascii_digit()).count();
            if digits > MAX_OCTET_COUNT_DIGITS {
                break Ending::Failed(FrameFailure::MalformedOctetCount);
            }
            match rest.get(digits) {
                None => break Ending::Incomplete,
                Some(b' ') => {}
                Some(_) => break Ending::Failed(FrameFailure::MalformedOctetCount),
            }
            if *first == b'0' {
                break Ending::Failed(FrameFailure::MalformedOctetCount);
            }
            let length = std::str::from_utf8(&rest[..digits])
                .expect("the prefix is ASCII digits")
                .parse::<usize>()
                .expect("ten digits fit a 64-bit usize");
            if length > max_message_size {
                break Ending::Failed(FrameFailure::OversizedOctetCount { length });
            }
            let payload_start = digits + 1;
            let Some(payload) = rest.get(payload_start..payload_start + length) else {
                break Ending::Incomplete;
            };
            frames.push(payload.to_vec());
            rest = &rest[payload_start + length..];
        } else if !allow_non_transparent {
            break Ending::Failed(FrameFailure::NonOctetTlsFrame);
        } else {
            let Some(delimiter) = rest.iter().position(|byte| *byte == b'\n') else {
                let pending = rest.len() - usize::from(rest.last() == Some(&b'\r'));
                if pending > max_message_size {
                    break Ending::Failed(FrameFailure::OversizedNonTransparentFrame);
                }
                break Ending::Incomplete;
            };
            let payload = match rest[..delimiter].strip_suffix(b"\r") {
                Some(payload) => payload,
                None => &rest[..delimiter],
            };
            if payload.len() > max_message_size {
                break Ending::Failed(FrameFailure::OversizedNonTransparentFrame);
            }
            frames.push(payload.to_vec());
            rest = &rest[delimiter + 1..];
        }
    };
    Framing { frames, ending }
}

fn whole(stream: &[u8], max_message_size: NonZeroUsize, allow_non_transparent: bool) -> Framing {
    frame_in_reads(stream, &[], max_message_size, allow_non_transparent)
}

fn frames(frames: &[&[u8]]) -> Vec<Vec<u8>> {
    frames.iter().map(|frame| frame.to_vec()).collect()
}

#[test]
fn stream_decoder_interleaves_both_rfc6587_framings() {
    assert_eq!(
        whole(b"5 helloalpha\r\n4 test", nonzero!(128usize), true),
        Framing {
            frames: frames(&[b"hello", b"alpha", b"test"]),
            ending: Ending::Clean,
        }
    );
}

#[test]
fn stream_decoder_rejects_malformed_and_oversized_frames() {
    let failure = |stream: &[u8], max: NonZeroUsize| whole(stream, max, true).ending;
    assert_eq!(
        failure(b"12x payload", nonzero!(128usize)),
        Ending::Failed(FrameFailure::MalformedOctetCount)
    );
    assert_eq!(
        failure(b"5 hello", nonzero!(4usize)),
        Ending::Failed(FrameFailure::OversizedOctetCount { length: 5 })
    );
    assert_eq!(
        failure(b"hello\n", nonzero!(4usize)),
        Ending::Failed(FrameFailure::OversizedNonTransparentFrame)
    );
    assert_eq!(
        failure(b"hello", nonzero!(4usize)),
        Ending::Failed(FrameFailure::OversizedNonTransparentFrame)
    );
}

#[test]
fn stream_decoder_limits_octet_count_prefix_to_ten_digits() {
    assert_eq!(
        whole(b"12345678901", nonzero!(128usize), true).ending,
        Ending::Failed(FrameFailure::MalformedOctetCount)
    );
    assert_eq!(
        whole(b"12345678901 x", nonzero!(128usize), true).ending,
        Ending::Failed(FrameFailure::MalformedOctetCount)
    );
    assert_eq!(
        whole(b"1234567890", nonzero!(128usize), true).ending,
        Ending::Incomplete
    );
}

#[test]
fn stream_decoder_rejects_zero_and_leading_zero_octet_counts() {
    for stream in [b"0 ".as_slice(), b"05 hello".as_slice()] {
        assert_eq!(
            whole(stream, nonzero!(128usize), true).ending,
            Ending::Failed(FrameFailure::MalformedOctetCount)
        );
    }
}

#[test]
fn stream_decoder_accepts_a_maximum_size_frame_with_split_crlf() {
    assert_eq!(
        frame_in_reads(b"hello\r\n", &[6, 1], nonzero!(5usize), true),
        Framing {
            frames: frames(&[b"hello"]),
            ending: Ending::Clean,
        }
    );
}

#[test]
fn stream_decoder_rejects_non_transparent_tls_framing() {
    assert_eq!(
        whole(b"<13>line framed\n", nonzero!(128usize), false).ending,
        Ending::Failed(FrameFailure::NonOctetTlsFrame)
    );
    assert_eq!(
        whole(b"4 tls!4 tl", nonzero!(128usize), false),
        Framing {
            frames: frames(&[b"tls!"]),
            ending: Ending::Incomplete,
        }
    );
}

/// Streams covering both framings, every failure and every way a stream can end, each split at
/// every byte into two reads and read one byte at a time.
#[test]
fn every_split_of_every_stream_frames_like_one_read() {
    let streams: [&[u8]; 14] = [
        b"5 hello<13>line one\r\n11 octet framed<14>line two\n",
        b"123 ",
        b"12",
        b"<13>unterminated",
        b"<13>ends in a carriage return\r",
        b"2 ab12x",
        b"9999999999 x",
        b"12345678901",
        b"0 ",
        b"40 too long for the limit",
        b"<13>this line is longer than the limit\n",
        b"<13>this line is longer than the limit and has no end",
        b"\n\r\n3 abc",
        b"",
    ];
    let max_message_size = nonzero!(24usize);
    for stream in streams {
        let expected = reference_framing(stream, max_message_size.get(), true);
        assert_eq!(
            whole(stream, max_message_size, true),
            expected,
            "{stream:?}"
        );
        assert_eq!(
            frame_in_reads(stream, &[1], max_message_size, true),
            expected,
            "{stream:?}"
        );
        for split in 1..stream.len() {
            assert_eq!(
                frame_in_reads(stream, &[split, usize::MAX], max_message_size, true),
                expected,
                "{stream:?} split at {split}"
            );
        }
    }
}

#[test]
fn a_read_compacts_the_bytes_framed_before_it_once() {
    let mut decoder = StreamFrameDecoder::new(nonzero!(128usize), true);
    assert_eq!(read_into(&mut decoder, b"5 hello5 world4 te"), Ok(18));
    assert_eq!(
        decoder.next_frame().expect("valid frame"),
        Some(b"hello".to_vec())
    );
    assert_eq!(
        decoder.next_frame().expect("valid frame"),
        Some(b"world".to_vec())
    );
    assert_eq!(decoder.next_frame().expect("valid prefix"), None);
    // Framing only moves the cursor; the bytes before it stay until the next read discards them.
    assert_eq!((decoder.cursor, decoder.buffer.len()), (14, 18));
    assert_eq!(read_into(&mut decoder, b"st"), Ok(2));
    assert_eq!(
        (decoder.cursor, decoder.buffer.as_slice()),
        (0, b"4 test".as_slice())
    );
    assert_eq!(
        decoder.next_frame().expect("valid frame"),
        Some(b"test".to_vec())
    );
    assert!(decoder.is_empty());
}

#[test]
fn a_line_split_across_reads_is_searched_once() {
    let mut decoder = StreamFrameDecoder::new(nonzero!(128usize), true);
    assert_eq!(read_into(&mut decoder, b"<13>split"), Ok(9));
    assert_eq!(decoder.next_frame().expect("a line may continue"), None);
    assert_eq!(
        decoder.progress,
        FrameProgress::NonTransparent { searched: 9 }
    );
    assert_eq!(read_into(&mut decoder, b" line\r"), Ok(6));
    assert_eq!(decoder.next_frame().expect("a line may continue"), None);
    assert_eq!(
        decoder.progress,
        FrameProgress::NonTransparent { searched: 15 }
    );
    assert_eq!(read_into(&mut decoder, b"\n"), Ok(1));
    assert_eq!(
        decoder.next_frame().expect("valid line"),
        Some(b"<13>split line".to_vec())
    );
    assert_eq!(decoder.progress, FrameProgress::Unread);
}

#[test]
fn an_octet_counted_payload_split_across_reads_keeps_its_count() {
    let mut decoder = StreamFrameDecoder::new(nonzero!(128usize), true);
    assert_eq!(read_into(&mut decoder, b"12 octet"), Ok(8));
    assert_eq!(decoder.next_frame().expect("valid prefix"), None);
    assert_eq!(
        decoder.progress,
        FrameProgress::OctetCounted {
            payload_start: 3,
            frame_end: 15,
        }
    );
    assert_eq!(read_into(&mut decoder, b" framed"), Ok(7));
    assert_eq!(
        decoder.next_frame().expect("valid frame"),
        Some(b"octet framed".to_vec())
    );
}

#[test]
fn a_read_never_takes_more_than_the_frame_bound_admits() {
    let max_message_size = nonzero!(16usize);
    let mut decoder = StreamFrameDecoder::new(max_message_size, true);
    let bound = max_message_size.get() + MAX_OCTET_COUNT_DIGITS + 1;
    let stream = vec![b'7'; 64];
    assert_eq!(read_into(&mut decoder, &stream), Ok(bound));
    assert_eq!(
        decoder
            .next_frame()
            .map_err(|error| FrameFailure::of(&error)),
        Err(FrameFailure::MalformedOctetCount)
    );
}

#[test]
fn bolero_stream_framing_matches_the_reference_however_reads_split_it() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .with_type::<(u8, bool, Vec<(u8, u8)>, Vec<u8>)>()
        .for_each(|(max, allow_non_transparent, fragments, reads)| {
            let max_message_size = NonZeroUsize::new(usize::from(*max % 48) + 1)
                .expect("one is added to the remainder");
            // Frames of both kinds around the size limit, with stray bytes, digits, carriage
            // returns and line feeds between them, so streams reach every failure and ending.
            let mut stream = Vec::new();
            for (kind, value) in fragments {
                let length = usize::from(*value) % (max_message_size.get() + 4);
                match kind % 8 {
                    0 | 1 => {
                        stream.extend_from_slice(length.to_string().as_bytes());
                        stream.push(b' ');
                        stream.extend(std::iter::repeat_n(b'o', length));
                    }
                    2 | 3 => {
                        stream.push(b'<');
                        stream.extend(std::iter::repeat_n(b'l', length));
                        if kind & 0x10 != 0 {
                            stream.push(b'\r');
                        }
                        stream.push(b'\n');
                    }
                    4 => stream.push(*value),
                    5 => stream.push(b'0' + value % 10),
                    6 => stream.push(b'\r'),
                    _ => stream.push(b'\n'),
                }
            }
            let reads = reads
                .iter()
                .map(|read| usize::from(*read))
                .collect::<Vec<_>>();
            let expected =
                reference_framing(&stream, max_message_size.get(), *allow_non_transparent);
            assert_eq!(
                frame_in_reads(&stream, &reads, max_message_size, *allow_non_transparent),
                expected
            );
            assert_eq!(
                whole(&stream, max_message_size, *allow_non_transparent),
                expected
            );
        });
}

/// A connection that hands its reader the scripted chunks one read at a time, then ends.
struct ScriptedStream {
    chunks: VecDeque<Vec<u8>>,
}

impl ScriptedStream {
    fn new(chunks: &[&[u8]]) -> Self {
        Self {
            chunks: chunks.iter().map(|chunk| chunk.to_vec()).collect(),
        }
    }
}

impl AsyncRead for ScriptedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(mut chunk) = self.chunks.pop_front() {
            let taken = chunk.len().min(buffer.remaining());
            buffer.put_slice(&chunk[..taken]);
            if taken < chunk.len() {
                chunk.drain(..taken);
                self.chunks.push_front(chunk);
            }
        }
        Poll::Ready(Ok(()))
    }
}

async fn read_connection(
    chunks: &[&[u8]],
    allow_non_transparent: bool,
) -> (error_stack::Result<(), SyslogConnectionError>, Vec<Vec<u8>>) {
    let (tx, mut rx) = mpsc::channel(STREAM_INTAKE_QUEUE_CAPACITY);
    let (_paused, paused_rx) = watch::channel(false);
    let peer_addr = SocketAddr::from(([127, 0, 0, 1], 6514));
    let outcome = read_stream_connection(
        ScriptedStream::new(chunks),
        peer_addr,
        nonzero!(128usize),
        allow_non_transparent,
        None,
        tx,
        paused_rx,
    )
    .await;
    let mut frames = Vec::new();
    while let Ok(frame) = rx.try_recv() {
        assert_eq!(frame.peer_addr, peer_addr);
        frames.push(frame.payload);
    }
    (outcome, frames)
}

#[nervix_primitives::test]
async fn a_connection_delivers_frames_its_reads_split() {
    let (outcome, delivered) = read_connection(
        &[b"1", b"2 octet", b" framed<13>li", b"ne\r", b"\n4 tls!"],
        true,
    )
    .await;
    outcome.expect("every frame was complete when the connection ended");
    assert_eq!(delivered, frames(&[b"octet framed", b"<13>line", b"tls!"]));
}

#[nervix_primitives::test]
async fn a_connection_that_ends_inside_a_frame_reports_it_incomplete() {
    let (outcome, delivered) = read_connection(&[b"5 hello", b"9 trunc"], true).await;
    let error = outcome.expect_err("the second frame never completed");
    assert!(matches!(
        error.current_context(),
        SyslogConnectionError::IncompleteFrame
    ));
    assert_eq!(delivered, frames(&[b"hello"]));
}

#[nervix_primitives::test]
async fn a_connection_closes_on_the_first_malformed_frame() {
    let (outcome, delivered) = read_connection(&[b"4 tls!", b"<13>line\n"], false).await;
    let error = outcome.expect_err("TLS admits octet counting only");
    assert!(matches!(
        error.current_context(),
        SyslogConnectionError::Frame
    ));
    assert!(error.contains::<SyslogFrameError>());
    assert_eq!(delivered, frames(&[b"tls!"]));
}

#[nervix_primitives::test]
async fn a_decoded_frame_waiting_for_intake_survives_suspension() {
    let (tx, mut rx) = mpsc::channel(1);
    let peer_addr = SocketAddr::from(([127, 0, 0, 1], 6514));
    tx.send(ReceivedSyslogFrame {
        payload: b"first".to_vec(),
        peer_addr,
    })
    .await
    .expect("the intake queue is open");
    let (pause_tx, pause_rx) = watch::channel(false);
    let mut connection = Box::pin(read_stream_connection(
        ScriptedStream::new(&[b"6 second"]),
        peer_addr,
        nonzero!(128usize),
        true,
        None,
        tx,
        pause_rx,
    ));

    // Poll through decoding to the full queue. This is the exact cancellation point: a pause
    // change used to win the select and discard the already decoded second frame.
    poll_fn(|context| {
        assert!(matches!(connection.as_mut().poll(context), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    pause_tx.send_replace(true);
    assert_eq!(
        rx.recv()
            .await
            .expect("the first frame stays queued")
            .payload,
        b"first"
    );
    pause_tx.send_replace(false);
    nervix_primitives::time::timeout(Duration::from_secs(1), connection)
        .await
        .expect("the connection resumes")
        .expect("the second frame is complete");
    assert_eq!(
        rx.recv()
            .await
            .expect("the decoded frame must survive")
            .payload,
        b"second"
    );
}

#[nervix_primitives::test]
async fn a_failed_accept_keeps_an_established_connection_and_rebinds() {
    use tokio::io::AsyncWriteExt as _;

    let reservation = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve an available listener port");
    let bind_addr = reservation
        .local_addr()
        .expect("the reserved port has an address")
        .to_string();
    drop(reservation);
    let plan = SyslogListenerIngestorPlan::new(
        vec![
            ClientConfigEntry {
                key: "protocol".to_string(),
                value: "tcp".to_string(),
            },
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "127.0.0.1:5514".to_string(),
            },
        ],
        |_| bind_addr,
    )
    .expect("the TCP plan is valid");
    let mut source = SyslogListenerIngestor::open(&plan, 0)
        .await
        .expect("the source opens");
    source.resume().await.expect("the listener binds");
    let peer_addr = SocketAddr::from(([127, 0, 0, 1], 6514));
    let (mut client, server) = tokio::io::duplex(128);
    let Some(SyslogListener::Stream(listener)) = source.listener.as_mut() else {
        panic!("the source has a stream listener");
    };
    let tx = listener.frame_tx.clone();
    let paused = listener.paused.subscribe();
    listener.connections.spawn(read_stream_connection(
        server,
        peer_addr,
        nonzero!(128usize),
        true,
        None,
        tx,
        paused,
    ));

    let failure = source.on_listener_error(Report::new(SyslogListenerError::StreamAccept {
        source: std::io::Error::other("accept failed"),
    }));
    assert!(failure.contains::<SyslogListenerError>());
    assert!(source.needs_resume());

    let blocker = TcpListener::bind(&source.bind_addr)
        .await
        .expect("another listener claims the retired socket's port");
    let retry_failure = source
        .resume()
        .await
        .expect_err("rebinding must report a transient error while the port is busy");
    assert!(retry_failure.contains::<SyslogListenerError>());
    assert!(source.needs_resume());

    client
        .write_all(b"6 second")
        .await
        .expect("the established connection remains writable");
    client.shutdown().await.expect("the client closes cleanly");
    let Some(SyslogListener::Stream(listener)) = source.listener.as_mut() else {
        panic!("the source retains its stream state");
    };
    let frame = nervix_primitives::time::timeout(Duration::from_secs(1), listener.frame_rx.recv())
        .await
        .expect("the connection keeps reading")
        .expect("the connection keeps its intake channel");
    assert_eq!(frame.payload, b"second");
    drop(blocker);
    source.resume().await.expect("the listener rebinds");
    assert!(!source.needs_resume());
    source.close().await.expect("the source closes");
}

#[test]
fn listener_connection_and_frame_failures_name_their_cause_once() {
    let refused = || std::io::Error::other("the peer reset the connection");
    let parse_failure = "4294967296000000000000"
        .parse::<usize>()
        .expect_err("the count exceeds every usize");
    let rendered = [
        format!(
            "{:#}",
            Report::new(SyslogListenerError::Bind {
                transport: "UDP",
                addr: "127.0.0.1:5514".to_string(),
                source: refused(),
            })
        ),
        format!(
            "{:#}",
            Report::new(SyslogListenerError::UdpReceive { source: refused() })
        ),
        format!(
            "{:#}",
            Report::new(SyslogListenerError::StreamAccept { source: refused() })
        ),
        format!(
            "{:#}",
            Report::new(SyslogConnectionError::TlsHandshake { source: refused() })
        ),
        format!(
            "{:#}",
            Report::new(SyslogConnectionError::StreamRead { source: refused() })
        ),
        format!(
            "{:#}",
            Report::new(DeadlineElapsed {
                operation: "the TLS handshake",
                key: DeadlineKey::ConnectTimeout,
                bound: Duration::from_secs(1),
            })
            .change_context(SyslogConnectionError::Deadline)
        ),
        format!(
            "{:#}",
            Report::new(SyslogFrameError::InvalidOctetCount {
                source: parse_failure.clone(),
            })
        ),
    ];

    assert_eq!(
        rendered,
        [
            "Syslog UDP bind '127.0.0.1:5514' failed: the peer reset the connection".to_string(),
            "Syslog UDP listener receive failed: the peer reset the connection".to_string(),
            "Syslog stream listener accept failed: the peer reset the connection".to_string(),
            "TLS handshake failed: the peer reset the connection".to_string(),
            "stream read failed: the peer reset the connection".to_string(),
            "connection ran out of its deadline: the TLS handshake did not finish within \
             connect_timeout_ms 1s"
                .to_string(),
            format!("malformed Syslog octet count: {parse_failure}"),
        ]
    );
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_a_decoded_frame_racing_suspension_is_handed_off_once() {
    nervix_model_harness::shuttle::check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let (tx, mut rx) = mpsc::channel(1);
            let peer_addr = SocketAddr::from(([127, 0, 0, 1], 6514));
            tx.send(ReceivedSyslogFrame {
                payload: b"first".to_vec(),
                peer_addr,
            })
            .await
            .expect("the intake queue is open");
            let (pause_tx, pause_rx) = watch::channel(false);
            let mut connection = Box::pin(read_stream_connection(
                ScriptedStream::new(&[b"6 second"]),
                peer_addr,
                nonzero!(128usize),
                true,
                None,
                tx,
                pause_rx,
            ));
            poll_fn(|context| {
                assert!(matches!(connection.as_mut().poll(context), Poll::Pending));
                Poll::Ready(())
            })
            .await;

            let reader = nervix_primitives::task::spawn(connection);
            let pauser_tx = pause_tx.clone();
            let pauser = nervix_primitives::task::spawn(async move {
                pauser_tx.send_replace(true);
                nervix_primitives::task::yield_now().await;
                pauser_tx.send_replace(false);
            });
            assert_eq!(
                rx.recv()
                    .await
                    .expect("the queued frame is retained")
                    .payload,
                b"first"
            );
            assert_eq!(
                rx.recv()
                    .await
                    .expect("the pending frame is retained")
                    .payload,
                b"second"
            );
            reader
                .await
                .expect("the connection task finishes")
                .expect("the stream has complete frames");
            pauser.await.expect("the pause task finishes");
            assert!(rx.try_recv().is_err(), "the pending frame is sent once");
        });
    });
}

/// The idle deadline every connection check below gives its client's `idle_timeout_ms`.
const IDLE: Duration = Duration::from_secs(5);

fn idle_deadline() -> ConfiguredDeadline {
    ConfiguredDeadline::idle(&[ClientConfigEntry {
        key: "idle_timeout_ms".to_string(),
        value: IDLE.as_millis().to_string(),
    }])
    .assured("five seconds is a valid deadline")
    .assured("the entry sets idle_timeout_ms")
}

/// The deadline whose elapsed wait ended `outcome`, which the connection's failure keeps beneath it.
fn elapsed_deadline(outcome: &error_stack::Result<(), SyslogConnectionError>) -> DeadlineElapsed {
    let error = outcome
        .as_ref()
        .err()
        .assured("the connection ended at its deadline");
    assert!(matches!(
        error.current_context(),
        SyslogConnectionError::Deadline
    ));
    *error
        .frames()
        .find_map(|frame| frame.downcast_ref::<DeadlineElapsed>())
        .assured("the elapsed deadline stays beneath the connection's failure")
}

/// A connection the check writes to through `client`, read by the production connection reader
/// with the idle deadline, and the frames it delivered.
struct IdleConnection {
    client: DuplexStream,
    reader: nervix_primitives::task::JoinHandle<error_stack::Result<(), SyslogConnectionError>>,
    frames: mpsc::Receiver<ReceivedSyslogFrame>,
    paused: watch::Sender<bool>,
}

impl IdleConnection {
    fn open() -> Self {
        let (client, server) = tokio::io::duplex(1024);
        let (tx, frames) = mpsc::channel(STREAM_INTAKE_QUEUE_CAPACITY);
        let (paused, paused_rx) = watch::channel(false);
        let reader = nervix_primitives::task::spawn(read_stream_connection(
            server,
            SocketAddr::from(([127, 0, 0, 1], 6514)),
            nonzero!(128usize),
            true,
            Some(idle_deadline()),
            tx,
            paused_rx,
        ));
        Self {
            client,
            reader,
            frames,
            paused,
        }
    }

    async fn ended(self) -> error_stack::Result<(), SyslogConnectionError> {
        self.reader
            .await
            .assured("the connection reader does not panic")
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_connection_that_sends_nothing_is_closed_at_its_idle_deadline() {
    let started = Instant::now();
    let connection = IdleConnection::open();

    let outcome = connection.ended().await;

    assert_eq!(started.elapsed(), IDLE);
    let elapsed = elapsed_deadline(&outcome);
    assert_eq!(elapsed.key, DeadlineKey::IdleTimeout);
    assert_eq!(elapsed.bound, IDLE);
    assert_eq!(
        elapsed.to_string(),
        "waiting for the connection's next bytes did not finish within idle_timeout_ms 5s"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn each_read_restarts_the_idle_deadline() {
    let mut connection = IdleConnection::open();
    let mut last_frame = Instant::now();
    for frame in [b"5 first", b"5 again", b"5 third"] {
        nervix_primitives::time::sleep(IDLE * 3 / 4).await;
        connection
            .client
            .write_all(frame)
            .await
            .assured("the connection reads what the client writes");
        let delivered = connection
            .frames
            .recv()
            .await
            .assured("the reader delivers each frame");
        assert_eq!(delivered.payload, &frame[2..]);
        last_frame = Instant::now();
    }

    let outcome = connection.ended().await;

    assert_eq!(last_frame.elapsed(), IDLE);
    assert_eq!(elapsed_deadline(&outcome).key, DeadlineKey::IdleTimeout);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_suspended_connection_is_not_closed_for_idleness() {
    let connection = IdleConnection::open();
    connection.paused.send_replace(true);
    nervix_primitives::time::sleep(IDLE * 4).await;
    assert!(
        !connection.reader.is_finished(),
        "a connection the listener does not read is never idle"
    );
    let resumed = Instant::now();
    connection.paused.send_replace(false);

    let outcome = connection.ended().await;

    assert_eq!(resumed.elapsed(), IDLE);
    assert_eq!(elapsed_deadline(&outcome).key, DeadlineKey::IdleTimeout);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_connection_without_an_idle_deadline_waits_for_its_sender() {
    let (mut client, server) = tokio::io::duplex(1024);
    let (tx, mut frames) = mpsc::channel(STREAM_INTAKE_QUEUE_CAPACITY);
    let (_paused, paused_rx) = watch::channel(false);
    let reader = nervix_primitives::task::spawn(read_stream_connection(
        server,
        SocketAddr::from(([127, 0, 0, 1], 6514)),
        nonzero!(128usize),
        true,
        None,
        tx,
        paused_rx,
    ));
    nervix_primitives::time::sleep(Duration::from_secs(3_600)).await;
    client
        .write_all(b"4 late")
        .await
        .assured("the connection reads what the client writes");
    let delivered = frames.recv().await.assured("the reader delivers the frame");
    assert_eq!(delivered.payload, b"late");
    drop(client);
    reader
        .await
        .assured("the connection reader does not panic")
        .assured("the client closed the connection after a complete frame");
}

/// A TLS acceptor for a self-signed `localhost` identity.
fn tls_acceptor() -> TlsAcceptor {
    nervix_connector::install_rustls_crypto_provider();
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .assured("a self-signed identity for a fixed name can be generated");
    let key = rustls_pki_types::PrivateKeyDer::Pkcs8(rustls_pki_types::PrivatePkcs8KeyDer::from(
        identity.key_pair.serialize_der(),
    ));
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![identity.cert.der().clone()], key)
        .assured("a generated identity is a valid server identity");
    TlsAcceptor::from(nervix_primitives::sync::StdArc::new(config))
}

#[nervix_primitives::test(start_paused = true)]
async fn a_tls_handshake_that_never_completes_is_closed_at_connect_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("a loopback TCP port is available");
    let address = listener.local_addr().assured("the listener is bound");
    // Opens the connection and never sends its client hello.
    let client = TcpStream::connect(address)
        .await
        .assured("the listener accepts connections");
    let (accepted, _) = listener.accept().await.assured("the connection arrives");
    let handshake = ConfiguredDeadline::connect(&[ClientConfigEntry {
        key: "connect_timeout_ms".to_string(),
        value: "2000".to_string(),
    }])
    .assured("two seconds is a valid deadline");
    let started = Instant::now();

    let outcome = accept_tls(&tls_acceptor(), accepted, handshake).await;

    assert_eq!(started.elapsed(), Duration::from_secs(2));
    let error = outcome
        .err()
        .assured("a client that never says hello is not accepted");
    assert!(matches!(
        error.current_context(),
        SyslogConnectionError::Deadline
    ));
    let elapsed = error
        .frames()
        .find_map(|frame| frame.downcast_ref::<DeadlineElapsed>())
        .assured("the elapsed deadline stays beneath the connection's failure");
    assert_eq!(elapsed.key, DeadlineKey::ConnectTimeout);
    assert_eq!(elapsed.bound, Duration::from_secs(2));
    drop(client);
}

#[nervix_primitives::test]
async fn a_client_that_does_not_speak_tls_fails_its_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("a loopback TCP port is available");
    let address = listener.local_addr().assured("the listener is bound");
    let mut client = TcpStream::connect(address)
        .await
        .assured("the listener accepts connections");
    let (accepted, _) = listener.accept().await.assured("the connection arrives");
    // A plain TCP sender writes a frame where the listener expects its client hello.
    client
        .write_all(b"<13>1 - - - - - - sent without TLS\n")
        .await
        .assured("the loopback connection takes the frame");
    let handshake =
        ConfiguredDeadline::connect(&[]).assured("an omitted key takes the default deadline");

    let outcome = accept_tls(&tls_acceptor(), accepted, handshake).await;

    let error = outcome
        .err()
        .assured("bytes that are not a TLS record never complete a handshake");
    assert!(matches!(
        error.current_context(),
        SyslogConnectionError::TlsHandshake { .. }
    ));
    drop(client);
}

#[nervix_primitives::test]
async fn an_oversized_datagram_is_dropped_and_the_next_one_received() {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .assured("a loopback UDP port is available");
    let address = socket.local_addr().assured("the socket is bound");
    let mut listener = SyslogUdpListener {
        socket,
        datagram: vec![0_u8; 65_535],
    };
    let sender = UdpSocket::bind("127.0.0.1:0")
        .await
        .assured("a loopback UDP port is available");
    let bound = nonzero!(32usize);
    sender
        .send_to(&[b'x'; 33], address)
        .await
        .assured("the oversized datagram is sent");
    sender
        .send_to(b"<13>1 - - - - - -", address)
        .await
        .assured("the datagram within the bound is sent");

    let oversized = listener
        .receive(bound)
        .await
        .assured("the listener receives the first datagram");
    let within = listener
        .receive(bound)
        .await
        .assured("the listener receives the second datagram");

    assert!(matches!(oversized, SyslogReceived::OversizedDatagram));
    let SyslogReceived::Message(message) = within else {
        panic!("a datagram within max_message_size is a message");
    };
    assert_eq!(message.payload(), b"<13>1 - - - - - -");
    assert_eq!(
        message.peer_addr,
        sender.local_addr().assured("the sender is bound")
    );
}

/// A listener message hands over the one metadata value SYSLOG LISTENER declares, its peer address
/// as STRING, and names that peer as its position.
#[test]
fn a_message_hands_over_its_peer_as_declared_metadata_and_names_it_as_its_position() {
    use nervix_connector::{
        IngestTransportPosition, RetainedIngestMetadata, RetainedIngestMetadataValue as Value,
    };
    use nervix_models::{IngestorEntityKind, SyslogIngestorEntity};

    let peer = std::net::SocketAddr::from(([192, 0, 2, 7], 5514));
    let message = SyslogListenerIngestorMessage::new(b"<13>event".to_vec(), peer);
    let metadata = message.metadata();

    let retained = RetainedIngestMetadata::capture(metadata.fields);
    assert_eq!(
        retained.values(),
        [Value::String("192.0.2.7:5514".to_string())]
    );
    let declared = IngestorEntityKind::Syslog(SyslogIngestorEntity::Listener)
        .capabilities()
        .metadata;
    let declared_types = declared
        .fields()
        .iter()
        .map(|field| Some(field.ty()))
        .collect::<Vec<_>>();
    let handed_types = retained.values().iter().map(Value::ty).collect::<Vec<_>>();
    assert_eq!(handed_types, declared_types);
    assert_eq!(
        metadata.position,
        Some(IngestTransportPosition::Syslog { peer })
    );
    let mut headers = 0;
    metadata.headers.headers().visit(&mut |_, _| headers += 1);
    assert_eq!(headers, 0);
}
