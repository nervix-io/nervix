//! Replays of exact network packetizations through the production HTTP input path.
//!
//! Layer: test harness.
//!
//! - **Owns.** A scripted peer that hands Hyper's HTTP/1 server one chosen byte segment per read,
//!   through the production accounting reader and body collection, and what each replay observes:
//!   every read window, the intake charge at each read and the collected body.
//! - **Depends on.** The listener input owner, the production HTTP builder and the executor.
//! - **Must not know.** Routes, codecs or graph dispatch.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    collections::VecDeque,
    convert::Infallible,
    io,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::{Buf as _, Bytes};
use http_body_util::Empty;
use hyper::{Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{ExecutionConfig, Executor, MemoryClass};
use nervix_primitives::{
    sync::{Arc, blocking::Mutex},
    time::timeout,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use ubyte::ByteUnit;

use super::{AccountedIo, InputAccounting, collect};
use crate::application::{
    listener_io::http_builder,
    listener_settings::{ListenerOverrides, ListenerSettings},
};

/// The largest TCP segment an Ethernet path carries with timestamps: what a fast server reads at a
/// time from a client on an ordinary network.
const ETHERNET_SEGMENT: usize = 1448;
/// The piece size the public fragmented-input scenario streams its body in.
const STREAMED_PIECE: usize = 4096;
/// The JSON string the public fragmented-input scenario sends, below its 1 MiB body limit.
const PAYLOAD_VALUE_BYTES: usize = 768 * 1024;

/// One read Hyper asked the transport for.
#[derive(Clone, Copy)]
struct ReadWindow {
    start: usize,
    capacity: usize,
    /// The intake class's charge when the read reached the transport, after its window was
    /// admitted.
    intake_reserved: u64,
}
impl ReadWindow {
    fn end(self) -> usize {
        self.start
            .checked_add(self.capacity)
            .assured("a read window is a live allocation's spare range")
    }
    fn overlaps(self, other: Self) -> bool {
        self.start < other.end() && other.start < self.end()
    }
    fn holds(self, address: usize) -> bool {
        self.start <= address && address < self.end()
    }
}

/// What the collected body looked like when the endpoint handed it on.
struct CollectedBody {
    len: usize,
    address: usize,
    /// The intake class's charge while the collected body alone is held.
    intake_reserved: u64,
}

#[derive(Default)]
struct Observations {
    windows: Vec<ReadWindow>,
    response: Vec<u8>,
    reader: Option<Waker>,
    /// Reads that reached the transport before the endpoint began collecting the body.
    reads_before_collection: Option<usize>,
    outcome: Option<Result<CollectedBody, String>>,
}

/// Delivers one scripted segment per read, then end of input once the response is written.
struct ScriptedPeer {
    segments: VecDeque<Bytes>,
    executor: Executor,
    observations: Arc<Mutex<Observations>>,
}
impl AsyncRead for ScriptedPeer {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let window = ReadWindow {
            start: buf.filled().as_ptr().addr(),
            capacity: buf.capacity(),
            intake_reserved: this.executor.memory(MemoryClass::Intake).reserved_bytes,
        };
        let mut observations = this.observations.lock();
        let Some(segment) = this.segments.front_mut() else {
            if observations.response.is_empty() {
                observations.reader = Some(cx.waker().clone());
                return Poll::Pending;
            }
            return Poll::Ready(Ok(()));
        };
        observations.windows.push(window);
        let delivered = segment.len().min(buf.remaining());
        buf.put_slice(&segment[..delivered]);
        segment.advance(delivered);
        if segment.is_empty() {
            this.segments.pop_front();
        }
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for ScriptedPeer {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut observations = self.observations.lock();
        observations.response.extend_from_slice(data);
        if let Some(reader) = observations.reader.take() {
            reader.wake();
        }
        Poll::Ready(Ok(data.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// How the request declares its body.
#[derive(Clone, Copy)]
enum Framing {
    /// Chunked transfer coding, one chunk per streamed piece, as a streaming client sends it.
    Chunked { piece: usize },
    /// A declared `Content-Length`.
    Declared,
}

/// How the network cuts the request's bytes into reads.
#[derive(Clone, Copy)]
enum Packetization {
    /// The head, then one read per chunk and one for the terminating chunk.
    PerChunk,
    /// Consecutive reads of at most this many bytes over the whole request.
    Segments(usize),
}

struct ScriptedRequest {
    head: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    payload_len: usize,
}
impl ScriptedRequest {
    fn json(value_bytes: usize, framing: Framing) -> Self {
        let payload = serde_json::to_vec(&serde_json::json!({ "value": "x".repeat(value_bytes) }))
            .assured("the bounded JSON fixture encodes");
        let mut head = b"POST /ingest HTTP/1.1\r\nhost: replay.example.com\r\n".to_vec();
        let mut chunks = Vec::new();
        match framing {
            Framing::Chunked { piece } => {
                head.extend_from_slice(b"transfer-encoding: chunked\r\n\r\n");
                for data in payload.chunks(piece) {
                    let mut chunk = format!("{:x}\r\n", data.len()).into_bytes();
                    chunk.extend_from_slice(data);
                    chunk.extend_from_slice(b"\r\n");
                    chunks.push(chunk);
                }
                chunks.push(b"0\r\n\r\n".to_vec());
            }
            Framing::Declared => {
                head.extend_from_slice(
                    format!("content-length: {}\r\n\r\n", payload.len()).as_bytes(),
                );
                chunks.push(payload.clone());
            }
        }
        Self {
            head,
            chunks,
            payload_len: payload.len(),
        }
    }

    fn segments(&self, packetization: Packetization) -> VecDeque<Bytes> {
        match packetization {
            Packetization::PerChunk => {
                let mut segments = VecDeque::from([Bytes::from(self.head.clone())]);
                for chunk in &self.chunks {
                    segments.push_back(Bytes::from(chunk.clone()));
                }
                segments
            }
            Packetization::Segments(size) => {
                let mut stream = self.head.clone();
                for chunk in &self.chunks {
                    stream.extend_from_slice(chunk);
                }
                stream.chunks(size).map(Bytes::copy_from_slice).collect()
            }
        }
    }
}

struct Replay {
    status: Option<StatusCode>,
    outcome: Result<CollectedBody, String>,
    windows: Vec<ReadWindow>,
    reads_before_collection: usize,
    intake_after: u64,
}
impl Replay {
    /// The read allocations Hyper moved to after the endpoint began collecting. A window that
    /// overlaps the previous one is the same allocation rewound or extended; any other is a new
    /// one the input ledger charges.
    fn read_allocations_while_collecting(&self) -> usize {
        let mut previous: Option<ReadWindow> = None;
        let mut allocations = 0_usize;
        for (index, window) in self.windows.iter().enumerate() {
            let same_allocation = previous.is_some_and(|previous| previous.overlaps(*window));
            previous = Some(*window);
            if !same_allocation && index >= self.reads_before_collection {
                allocations = allocations
                    .checked_add(1)
                    .assured("the count is bounded by the scripted reads");
            }
        }
        allocations
    }

    fn peak_intake_at_reads(&self) -> u64 {
        let mut peak = 0_u64;
        for window in &self.windows {
            peak = peak.max(window.intake_reserved);
        }
        peak
    }
}

fn settings(read_buffer: ByteUnit) -> ListenerSettings {
    ListenerSettings::resolve(ListenerOverrides {
        buffer_bytes: Some(read_buffer),
        http_body_bytes: Some(ByteUnit::Mebibyte(1)),
        // One request per replayed connection, so serving ends after its response.
        http_keepalive: Some(false),
        ..Default::default()
    })
    .assured("the replay's listener settings are valid")
}

fn executor() -> Executor {
    let mut config = ExecutionConfig::default();
    config.budgets.intake = ByteUnit::Mebibyte(4);
    Executor::new(config).assured("the replay's 4 MiB intake pool is valid")
}

async fn replay(
    settings: ListenerSettings,
    request: &ScriptedRequest,
    packetization: Packetization,
) -> Replay {
    let executor = executor();
    let accounting = InputAccounting::new(settings);
    let observations = Arc::new(Mutex::new(Observations::default()));
    let peer = ScriptedPeer {
        segments: request.segments(packetization),
        executor: executor.clone(),
        observations: observations.clone(),
    };
    let service_executor = executor.clone();
    let service_accounting = accounting.clone();
    let service_observations = observations.clone();
    let service = service_fn(move |mut request: Request<Incoming>| {
        let executor = service_executor.clone();
        let accounting = service_accounting.clone();
        let observations = service_observations.clone();
        async move {
            let reads = observations.lock().windows.len();
            observations.lock().reads_before_collection = Some(reads);
            let (status, outcome) = match collect(request.body_mut(), &executor, &accounting).await
            {
                Ok(body) => {
                    let collected = CollectedBody {
                        len: body.len(),
                        address: body.as_ref().as_ptr().addr(),
                        intake_reserved: executor.memory(MemoryClass::Intake).reserved_bytes,
                    };
                    (StatusCode::ACCEPTED, Ok(collected))
                }
                Err(error) => (error.current_context().status(), Err(format!("{error:#}"))),
            };
            observations.lock().outcome = Some(outcome);
            Ok::<_, Infallible>(
                Response::builder()
                    .status(status)
                    .body(Empty::<Bytes>::new())
                    .assured("a status-only response is always valid"),
            )
        }
    });
    let connection = http_builder(settings).serve_connection(
        TokioIo::new(AccountedIo::new(peer, accounting.clone())),
        service,
    );
    timeout(Duration::from_secs(30), connection)
        .await
        .assured("a scripted request is served without waiting on real I/O")
        .assured("the scripted connection completes its one exchange");
    let intake_after = executor.memory(MemoryClass::Intake).reserved_bytes;
    let mut observations = observations.lock();
    let status = hyper_status(&observations.response);
    Replay {
        status,
        outcome: observations
            .outcome
            .take()
            .assured("the scripted request reached the endpoint's body collection"),
        windows: std::mem::take(&mut observations.windows),
        reads_before_collection: observations
            .reads_before_collection
            .assured("the endpoint began collecting the body"),
        intake_after,
    }
}

/// The status code of the scripted response, read from its status line.
fn hyper_status(response: &[u8]) -> Option<StatusCode> {
    let line = response.split(|byte| *byte == b'\n').next()?;
    let code = line.split(|byte| *byte == b' ').nth(1)?;
    StatusCode::from_bytes(code).ok()
}

#[nervix_primitives::test]
async fn a_body_in_ethernet_sized_reads_is_admitted_within_its_bounds() {
    let request = ScriptedRequest::json(
        PAYLOAD_VALUE_BYTES,
        Framing::Chunked {
            piece: STREAMED_PIECE,
        },
    );
    for read_buffer in [
        ByteUnit::Kibibyte(8),
        ListenerSettings::default().buffer_bytes(),
    ] {
        let replay = replay(
            settings(read_buffer),
            &request,
            Packetization::Segments(ETHERNET_SEGMENT),
        )
        .await;
        let reason = match &replay.outcome {
            Ok(_) => "admitted".to_string(),
            Err(reason) => reason.clone(),
        };
        assert_eq!(
            replay.status,
            Some(StatusCode::ACCEPTED),
            "a {} byte body in {ETHERNET_SEGMENT} byte reads is within a 1 MiB limit and a 4 MiB \
             pool with a {read_buffer} read buffer: {reason}",
            request.payload_len,
        );
        let collected = replay
            .outcome
            .as_ref()
            .assured("the asserted status reports a collected body");
        assert_eq!(collected.len, request.payload_len);
        // One allocation for the read after the first frame, which that frame still pins, and at
        // most one for each time a body larger than one read buffer moves on a data worker while
        // the frame it makes room for is held: a logarithmic number, not one per read.
        assert!(
            replay.read_allocations_while_collecting() <= 10,
            "the connection keeps reusing its read buffer instead of pinning one per read: {} \
             read allocations over {} reads",
            replay.read_allocations_while_collecting(),
            replay.windows.len(),
        );
        assert_eq!(
            replay.intake_after, 0,
            "the request returns its whole charge"
        );
    }
}

#[nervix_primitives::test]
async fn streamed_pieces_read_whole_are_admitted_within_their_bounds() {
    let request = ScriptedRequest::json(
        PAYLOAD_VALUE_BYTES,
        Framing::Chunked {
            piece: STREAMED_PIECE,
        },
    );
    for read_buffer in [
        ByteUnit::Kibibyte(8),
        ListenerSettings::default().buffer_bytes(),
    ] {
        let replay = replay(settings(read_buffer), &request, Packetization::PerChunk).await;
        assert_eq!(replay.status, Some(StatusCode::ACCEPTED));
        assert_eq!(replay.intake_after, 0);
    }
}

#[nervix_primitives::test]
async fn a_declared_length_body_keeps_exactly_its_own_charge() {
    let request = ScriptedRequest::json(PAYLOAD_VALUE_BYTES, Framing::Declared);
    let replay = replay(
        settings(ListenerSettings::default().buffer_bytes()),
        &request,
        Packetization::Segments(ETHERNET_SEGMENT),
    )
    .await;
    assert_eq!(replay.status, Some(StatusCode::ACCEPTED));
    let collected = replay
        .outcome
        .as_ref()
        .assured("the asserted status reports a collected body");
    assert_eq!(collected.len, request.payload_len);
    assert_eq!(
        collected.intake_reserved,
        u64::try_from(request.payload_len).assured("the fixture's length fits a byte count"),
        "a body copied out of several reads holds exactly its declared length while dispatched"
    );
    // The scratch, the declared body and two of Hyper's initial 8 KiB read buffers: the one the
    // first data frame pinned and the one the connection reuses afterwards.
    let bound = settings(ListenerSettings::default().buffer_bytes())
        .input_scratch_bytes()
        .assured("the default scratch fits a byte count")
        .checked_add(u64::try_from(request.payload_len).assured("the fixture's length fits"))
        .and_then(|bytes| bytes.checked_add(2 * 8192))
        .assured("the bound of a 1 MiB fixture fits a byte count");
    assert!(
        replay.peak_intake_at_reads() <= bound,
        "the charge while receiving follows the declared body, not the number of reads: peak {} \
         above {bound}",
        replay.peak_intake_at_reads(),
    );
    assert_eq!(replay.intake_after, 0);
}

#[nervix_primitives::test]
async fn a_body_read_whole_keeps_its_read_buffer_without_a_copy() {
    let request = ScriptedRequest::json(64, Framing::Declared);
    let replay = replay(
        settings(ListenerSettings::default().buffer_bytes()),
        &request,
        Packetization::Segments(usize::MAX),
    )
    .await;
    assert_eq!(replay.status, Some(StatusCode::ACCEPTED));
    let collected = replay
        .outcome
        .as_ref()
        .assured("the asserted status reports a collected body");
    assert_eq!(collected.len, request.payload_len);
    assert!(
        replay
            .windows
            .iter()
            .any(|window| window.holds(collected.address)),
        "a single data frame is handed on inside the read buffer it arrived in"
    );
    assert_eq!(replay.intake_after, 0);
}
