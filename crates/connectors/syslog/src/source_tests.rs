//! Syslog stream framing qualification.
//!
//! Layer: test harness.
//! - **Owns.** RFC 6587 frames and failures at every read boundary, the read cursor and its single
//!   compaction per read, and the property that framing does not depend on how reads split a
//!   stream.
//! - **Depends on.** The production stream frame decoder and connection reader.
//! - **Must not know.** Codecs, runtime collectors or listener lifecycle.

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use nonzero_ext::nonzero;
use tokio::io::ReadBuf;

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
