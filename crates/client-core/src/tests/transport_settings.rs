//! The HTTP/2 settings and connection limits of the client's gRPC channel, observed by real peers.
//!
//! Most peers are the maintained `h2` server on the other end of an in-memory stream, so a test
//! decides whether the peer answers pings and reads the flow-control credit the client grants it.
//! Where a test must decide exactly which frame the client receives and when, a scripted peer
//! writes and reads RFC 9113 frames itself, as a test fixture may. The connector's TCP options
//! and connection attempt delay are observed on loopback sockets. Every channel is built from the
//! endpoint the client's connector configures, and every connection is dialled by the connector
//! the client dials with, so these tests pin the values a real connection takes, not a copy of
//! them.

use std::{
    collections::BTreeMap,
    future::poll_fn,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_dns::{DnsConfiguration, DnsResolver, NameServers};
use nervix_primitives::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{AbortOnDropHandle, spawn},
    time::{Instant, timeout},
};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tonic::{
    body::Body,
    codegen::{Service, http},
    transport::Channel,
};
use url::Url;

use crate::{ConnectDns, ConnectOptions, connection::GrpcConnector};

/// Generous room for in-memory bytes in flight; no test sends more than a few frames.
const DUPLEX_BYTES: usize = 1 << 20;

/// Large enough that the peer's own send buffer never bounds the credit a test reads.
const PEER_SEND_BUFFER_BYTES: usize = 64 << 20;

/// What the peer asks to send on a call: more than any window a test grants.
const PEER_WANTED_BYTES: usize = 32 << 20;

/// A connector that hands the channel one end of an in-memory stream, once.
struct DuplexConnector(Option<tokio::io::DuplexStream>);

impl Service<http::Uri> for DuplexConnector {
    type Response = hyper_util::rt::TokioIo<tokio::io::DuplexStream>;
    type Error = std::io::Error;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Uri) -> Self::Future {
        let stream = self
            .0
            .take()
            .ok_or_else(|| std::io::Error::other("the test peer serves one connection"));
        std::future::ready(stream.map(hyper_util::rt::TokioIo::new))
    }
}

/// One call the peer accepted, and the connection it arrived on.
struct AcceptedCall {
    connection: h2::server::Connection<tokio::io::DuplexStream, Bytes>,
    respond: h2::server::SendResponse<Bytes>,
}

/// The client's channel to a fresh peer, with the peer's end of the stream.
async fn channel_to_peer(options: ConnectOptions) -> (Channel, tokio::io::DuplexStream) {
    let (client, peer) = tokio::io::duplex(DUPLEX_BYTES);
    let server = Url::parse("http://peer.test").assured("a fixed origin is a URL");
    let connector = GrpcConnector::new(options).assured("the test options validate");
    let channel = connector
        .endpoint(&server)
        .assured("the fixed origin is a valid endpoint")
        .connect_with_connector(DuplexConnector(Some(client)))
        .await
        .assured("an in-memory connection opens");
    (channel, peer)
}

/// Sends one call on `channel` and keeps it open until its answer, a failure or the test's end.
/// The caller keeps `channel` for as long as the call should live.
async fn open_call(
    channel: &Channel,
) -> AbortOnDropHandle<Result<http::Response<Body>, tonic::transport::Error>> {
    let mut channel = channel.clone();
    poll_fn(|context| channel.poll_ready(context))
        .await
        .assured("the channel's connection is open and takes a call");
    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri(http::Uri::from_static("/nervix.test.Peer/Call"))
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header(http::header::TE, "trailers")
        .body(Body::empty())
        .assured("a fixed request is valid");
    AbortOnDropHandle::new(spawn(channel.call(request)))
}

/// Accepts the first call on the peer's end, after which nothing serves the connection: it reads
/// no frame and answers no ping until the test serves it again.
async fn accept_call(peer: tokio::io::DuplexStream) -> AcceptedCall {
    let mut connection = h2::server::Builder::new()
        .max_send_buffer_size(PEER_SEND_BUFFER_BYTES)
        .handshake::<_, Bytes>(peer)
        .await
        .assured("the client opens HTTP/2 with its preface");
    let (_request, respond) = connection
        .accept()
        .await
        .assured("the client sends its call")
        .assured("the call is a valid HTTP/2 request");
    AcceptedCall {
        connection,
        respond,
    }
}

/// Serves `connection` until it closes: reads every frame and answers every ping.
fn serve(
    mut connection: h2::server::Connection<tokio::io::DuplexStream, Bytes>,
) -> AbortOnDropHandle<()> {
    AbortOnDropHandle::new(spawn(async move {
        loop {
            nervix_primitives::task::consume_budget().await;
            match connection.accept().await {
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return,
            }
        }
    }))
}

/// How long after the peer falls silent the client ends a call that is still open.
async fn silence_until_the_call_fails(options: ConnectOptions) -> Duration {
    let (channel, peer) = channel_to_peer(options).await;
    let call = open_call(&channel).await;
    let accepted = accept_call(peer).await;
    let silent_since = Instant::now();
    let failed = timeout(Duration::from_secs(24 * 60 * 60), call)
        .await
        .assured("the client ends a call whose peer answers no ping")
        .assured("the call task runs to its end");
    assert!(
        failed.is_err(),
        "a call whose peer answers no ping fails rather than completing"
    );
    let silence = silent_since.elapsed();
    drop(accepted);
    drop(channel);
    silence
}

/// A connection whose peer stops answering is found dead after one keepalive interval without a
/// frame and one keepalive timeout without a ping's answer, both on by default.
#[nervix_primitives::test(start_paused = true)]
async fn a_call_whose_peer_stops_answering_fails_after_the_default_keepalive() {
    let defaults = ConnectOptions::default();
    assert_eq!(defaults.keepalive_interval, Duration::from_secs(30));
    assert_eq!(defaults.keepalive_timeout, Duration::from_secs(20));
    let silence = silence_until_the_call_fails(defaults).await;
    assert!(
        silence >= Duration::from_secs(50) && silence < Duration::from_secs(51),
        "the default keepalive ends a silent connection after 30 s and 20 s, not after {silence:?}"
    );
}

/// The configured interval and timeout replace the defaults on the channel.
#[nervix_primitives::test(start_paused = true)]
async fn a_configured_keepalive_ends_a_silent_connection_on_its_own_schedule() {
    let options = ConnectOptions {
        keepalive_interval: Duration::from_secs(2),
        keepalive_timeout: Duration::from_millis(1_500),
        ..ConnectOptions::default()
    };
    let silence = silence_until_the_call_fails(options).await;
    assert!(
        silence >= Duration::from_millis(3_500) && silence < Duration::from_millis(4_500),
        "a 2 s interval and a 1.5 s timeout end a silent connection after 3.5 s, not after \
         {silence:?}"
    );
}

/// A peer that answers its pings keeps a quiet call open for as long as the call lasts.
#[nervix_primitives::test(start_paused = true)]
async fn a_quiet_call_whose_peer_answers_its_pings_stays_open() {
    let (channel, peer) = channel_to_peer(ConnectOptions::default()).await;
    let mut call = open_call(&channel).await;
    let accepted = accept_call(peer).await;
    let _served = serve(accepted.connection);
    let quiet = timeout(Duration::from_secs(10 * 60), &mut call).await;
    assert!(
        quiet.is_err(),
        "ten minutes of answered pings keep the call open: {quiet:?}"
    );
    drop(accepted.respond);
    drop(channel);
}

/// Without an open call the client sends no ping, so an idle connection to a peer that serves
/// nothing is still the one the next call takes.
#[nervix_primitives::test(start_paused = true)]
async fn an_idle_connection_is_not_pinged() {
    let (channel, peer) = channel_to_peer(ConnectOptions::default()).await;
    let first = open_call(&channel).await;
    let accepted = accept_call(peer).await;
    let mut respond = accepted.respond;
    respond
        .send_response(http::Response::new(()), true)
        .assured("the peer answers the call with an empty response");
    let mut connection = accepted.connection;
    // Serve until the client has the answer, so the call closes and the connection goes idle.
    let response = nervix_primitives::select! {
        response = first => response,
        accepted = connection.accept() => match accepted {
            Some(Ok(_)) => panic!("the client sent a second call before the first answer"),
            Some(Err(error)) => panic!("the connection failed before the answer: {error}"),
            None => panic!("the connection closed before the answer"),
        },
    };
    response
        .assured("the call task ends")
        .assured("the empty answer reaches the client");
    // Nothing serves the connection now. A pinged connection would be closed after 50 s; ten
    // idle minutes later it still carries the next call to the peer.
    nervix_primitives::time::sleep(Duration::from_secs(10 * 60)).await;
    let second = open_call(&channel).await;
    let next = timeout(Duration::from_secs(1), connection.accept())
        .await
        .assured("the next call reaches the peer on the idle connection")
        .assured("the connection is still open")
        .assured("the next call is a valid request");
    drop(next);
    drop(second);
    drop(channel);
}

/// How many bytes the peer may send on one new call before the client reads any of them.
async fn credit_of_a_new_call(options: ConnectOptions) -> usize {
    let (channel, peer) = channel_to_peer(options).await;
    let _call = open_call(&channel).await;
    let accepted = accept_call(peer).await;
    let mut respond = accepted.respond;
    let mut stream = respond
        .send_response(http::Response::new(()), false)
        .assured("the peer opens its answer");
    let _served = serve(accepted.connection);
    stream.reserve_capacity(PEER_WANTED_BYTES);
    // The client grants its windows before it sends the call, so the credit is complete once it
    // stops growing; a few scheduling rounds leave room for the frames already in memory.
    let mut credit = 0;
    for _ in 0..64 {
        nervix_primitives::task::yield_now().await;
        credit = credit.max(stream.capacity());
    }
    drop(channel);
    credit
}

/// The peer may send one call no more than the stream window, and every call together no more
/// than the connection window.
#[nervix_primitives::test]
async fn the_peer_sends_no_more_than_the_declared_windows() {
    let defaults = ConnectOptions::default();
    assert_eq!(defaults.stream_window_bytes.get(), 2 * 1024 * 1024);
    assert_eq!(defaults.connection_window_bytes.get(), 5 * 1024 * 1024);
    assert_eq!(credit_of_a_new_call(defaults).await, 2 * 1024 * 1024);
    let small_stream = ConnectOptions {
        stream_window_bytes: NonZeroU32::new(1_000).assured("a positive window"),
        ..ConnectOptions::default()
    };
    assert_eq!(credit_of_a_new_call(small_stream).await, 1_000);
    // h2 widens the connection window from the 65,535 bytes every connection starts with only by
    // at least half of its current size, so a declared window more than half again as large
    // shows at once; a smaller one is reached as data flows, and never exceeded.
    let small_connection = ConnectOptions {
        stream_window_bytes: NonZeroU32::new(1_000_000).assured("a positive window"),
        connection_window_bytes: NonZeroU32::new(300_000).assured("a positive window"),
        ..ConnectOptions::default()
    };
    assert_eq!(credit_of_a_new_call(small_connection).await, 300_000);
    let barely_wider = ConnectOptions {
        connection_window_bytes: NonZeroU32::new(70_000).assured("a positive window"),
        ..ConnectOptions::default()
    };
    let credit = credit_of_a_new_call(barely_wider).await;
    assert!(
        credit <= 70_000,
        "the peer never gets more credit than the declared connection window, not {credit}"
    );
}

/// A frame type a scripted peer writes or looks for (RFC 9113 section 6).
const DATA: u8 = 0x0;
const HEADERS: u8 = 0x1;
const RST_STREAM: u8 = 0x3;
const SETTINGS: u8 = 0x4;
const GOAWAY: u8 = 0x7;
const WINDOW_UPDATE: u8 = 0x8;

/// The SETTINGS flag that acknowledges the other side's settings (RFC 9113 section 6.5).
const ACK: u8 = 0x1;

/// A SETTINGS identifier (RFC 9113 section 6.5.2).
const HEADER_TABLE_SIZE: u16 = 0x1;
const ENABLE_PUSH: u16 = 0x2;
const MAX_CONCURRENT_STREAMS: u16 = 0x3;
const INITIAL_WINDOW_SIZE: u16 = 0x4;
const MAX_FRAME_SIZE: u16 = 0x5;
const MAX_HEADER_LIST_SIZE: u16 = 0x6;

/// The connection preface every HTTP/2 client sends first (RFC 9113 section 3.4).
const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// The length of every frame header (RFC 9113 section 4.1).
const FRAME_HEADER_BYTES: usize = 9;

/// How long a peer waits for something the client is expected to do; generous, since the wait
/// ends as soon as it happens.
const EXPECTED_WITHIN: Duration = Duration::from_secs(30);

/// One frame a scripted peer read: its RFC 9113 section 4.1 type, stream and payload.
struct Frame {
    kind: u8,
    stream: u32,
    payload: Vec<u8>,
}

impl Frame {
    /// The error code of a RST_STREAM or GOAWAY frame (RFC 9113 sections 6.4 and 6.8).
    fn error_code(&self) -> h2::Reason {
        let code = match self.kind {
            RST_STREAM => self.payload.get(..4),
            GOAWAY => self.payload.get(4..8),
            other => panic!("a frame of type {other} carries no error code"),
        };
        let code = code.assured("the client sends a complete RST_STREAM or GOAWAY frame");
        let code: [u8; 4] = code.try_into().assured("an error code is four bytes");
        h2::Reason::from(u32::from_be_bytes(code))
    }
}

/// A peer that writes and reads HTTP/2 frames itself, so a test decides exactly which frame the
/// client receives and when. It writes only what these tests need, its SETTINGS and their
/// acknowledgement and DATA frames, and reads every frame the client sends.
struct ScriptedPeer {
    stream: tokio::io::DuplexStream,
}

impl ScriptedPeer {
    /// Reads the client's connection preface and its first SETTINGS frame, each identifier with
    /// its value, and answers nothing yet.
    async fn accept(mut stream: tokio::io::DuplexStream) -> (Self, BTreeMap<u16, u32>) {
        let mut preface = [0_u8; 24];
        timeout(EXPECTED_WITHIN, stream.read_exact(&mut preface))
            .await
            .assured("the client opens HTTP/2 within the deadline")
            .assured("the client sends its whole preface");
        assert_eq!(preface.as_slice(), CLIENT_PREFACE);
        let mut peer = Self { stream };
        let settings = peer.read_frame().await;
        assert_eq!(
            settings.kind, SETTINGS,
            "a client's first frame is SETTINGS"
        );
        // Each setting is a 16-bit identifier and a 32-bit value (RFC 9113 section 6.5.1).
        let (entries, partial) = settings.payload.as_chunks::<6>();
        assert!(
            partial.is_empty(),
            "a SETTINGS payload holds whole settings"
        );
        let mut values = BTreeMap::new();
        for [first, second, value @ ..] in entries {
            let identifier = u16::from_be_bytes([*first, *second]);
            values.insert(identifier, u32::from_be_bytes(*value));
        }
        (peer, values)
    }

    /// Sends the peer's own SETTINGS, empty so that every setting keeps its initial value, and
    /// acknowledges the client's.
    async fn settle(&mut self) {
        self.write_frame(SETTINGS, 0, 0, &[]).await;
        self.write_frame(SETTINGS, ACK, 0, &[]).await;
    }

    /// Sends the peer's own SETTINGS with the largest stream window HTTP/2 allows, widens the
    /// connection window to the same, and acknowledges the client's SETTINGS.
    async fn grant_every_window(&mut self) {
        let largest = crate::capabilities::LARGEST_RECEIVE_WINDOW_BYTES;
        let mut settings = Vec::with_capacity(6);
        settings.extend_from_slice(&INITIAL_WINDOW_SIZE.to_be_bytes());
        settings.extend_from_slice(&largest.to_be_bytes());
        self.write_frame(SETTINGS, 0, 0, &settings).await;
        self.write_frame(SETTINGS, ACK, 0, &[]).await;
        let increment = largest
            .checked_sub(crate::capabilities::INITIAL_CONNECTION_WINDOW_BYTES)
            .assured("the largest window is above the initial one");
        self.write_frame(WINDOW_UPDATE, 0, 0, &increment.to_be_bytes())
            .await;
    }

    /// Sends a DATA frame on `stream` after the client cancelled the call it carried.
    async fn late_data(&mut self, stream: u32) {
        self.write_frame(DATA, 0, stream, b"late").await;
    }

    async fn write_frame(&mut self, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
        let length = u32::try_from(payload.len())
            .assured("a scripted frame carries a few bytes")
            .to_be_bytes();
        let mut header = [0_u8; FRAME_HEADER_BYTES];
        header[..3].copy_from_slice(&length[1..]);
        header[3] = kind;
        header[4] = flags;
        header[5..].copy_from_slice(&stream.to_be_bytes());
        self.stream
            .write_all(&header)
            .await
            .assured("the in-memory connection takes the peer's frames");
        self.stream
            .write_all(payload)
            .await
            .assured("the in-memory connection takes the peer's frames");
    }

    async fn read_frame(&mut self) -> Frame {
        let mut header = [0_u8; FRAME_HEADER_BYTES];
        timeout(EXPECTED_WITHIN, self.stream.read_exact(&mut header))
            .await
            .assured("the client sends its next frame within the deadline")
            .assured("the connection stays open until the client's next frame");
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]);
        let mut payload = vec![0; usize::try_from(length).assured("a frame length fits usize")];
        self.stream
            .read_exact(&mut payload)
            .await
            .assured("the client sends the whole frame");
        // The stream identifier's most significant bit is reserved (RFC 9113 section 4.1).
        let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        Frame {
            kind: header[3],
            stream,
            payload,
        }
    }

    /// The next frame of `kind`, past every other frame the client sends meanwhile: its SETTINGS
    /// acknowledgement, window updates and the like.
    async fn next(&mut self, kind: u8) -> Frame {
        loop {
            nervix_primitives::task::consume_budget().await;
            let frame = self.read_frame().await;
            if frame.kind == kind {
                return frame;
            }
        }
    }

    /// The streams the client resets as closed, in order, up to and including `last`.
    async fn closed_resets_through(&mut self, last: u32) -> Vec<u32> {
        let mut streams = Vec::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let reset = self.next(RST_STREAM).await;
            assert_eq!(reset.error_code(), h2::Reason::STREAM_CLOSED);
            streams.push(reset.stream);
            if reset.stream == last {
                return streams;
            }
        }
    }
}

/// A fresh channel to a scripted peer that has answered the client's SETTINGS.
async fn channel_to_scripted_peer(options: ConnectOptions) -> (Channel, ScriptedPeer) {
    let (channel, peer) = channel_to_peer(options).await;
    let (mut peer, _settings) = ScriptedPeer::accept(peer).await;
    peer.settle().await;
    (channel, peer)
}

/// Opens a call and cancels it before any answer: the peer reads its request, the caller drops the
/// call, and the peer reads the client's RST_STREAM(CANCEL). Returns the call's stream.
async fn cancelled_call(channel: &Channel, peer: &mut ScriptedPeer) -> u32 {
    let call = open_call(channel).await;
    let request = peer.next(HEADERS).await;
    drop(call);
    let reset = peer.next(RST_STREAM).await;
    assert_eq!(reset.stream, request.stream);
    assert_eq!(reset.error_code(), h2::Reason::CANCEL);
    request.stream
}

/// The client's first SETTINGS frame advertises the limits the node must honour, each the declared
/// or the fixed value: the declared stream window and header list, the protocol's initial header
/// table and frame size, and no stream the node may open, since server push is off.
#[nervix_primitives::test]
async fn the_client_advertises_its_declared_and_fixed_settings() {
    let (channel, peer) = channel_to_peer(ConnectOptions::default()).await;
    let (_peer, settings) = ScriptedPeer::accept(peer).await;
    let expected = BTreeMap::from([
        (HEADER_TABLE_SIZE, 4_096),
        (ENABLE_PUSH, 0),
        (MAX_CONCURRENT_STREAMS, 0),
        (INITIAL_WINDOW_SIZE, 2 * 1024 * 1024),
        (MAX_FRAME_SIZE, 16 * 1024),
        (MAX_HEADER_LIST_SIZE, 16 * 1024),
    ]);
    assert_eq!(settings, expected);
    drop(channel);

    let declared = ConnectOptions {
        header_list_bytes: NonZeroU32::new(8_192).assured("a positive bound"),
        ..ConnectOptions::default()
    };
    let (channel, peer) = channel_to_peer(declared).await;
    let (_peer, settings) = ScriptedPeer::accept(peer).await;
    assert_eq!(settings.get(&MAX_HEADER_LIST_SIZE), Some(&8_192));
    drop(channel);
}

/// Response headers whose header list, counted as HTTP/2 counts it, takes `bytes`: the status and
/// one filler field, each its name and value plus 32 bytes.
fn response_with_header_list(bytes: usize) -> http::Response<()> {
    // `:status: 200` counts 7 + 3 + 32 bytes and `x-fill: <value>` 6 + the value + 32.
    let filler = bytes
        .checked_sub(42 + 38)
        .assured("the tested header lists hold both fields");
    http::Response::builder()
        .header("x-fill", "a".repeat(filler))
        .body(())
        .assured("a filler header is a valid response header")
}

/// How a call ends that the peer answers with a header list of `bytes`.
async fn call_answered_with_a_header_list_of(
    options: ConnectOptions,
    bytes: usize,
) -> Result<http::Response<Body>, tonic::transport::Error> {
    let (channel, peer) = channel_to_peer(options).await;
    let call = open_call(&channel).await;
    let accepted = accept_call(peer).await;
    let mut respond = accepted.respond;
    respond
        .send_response(response_with_header_list(bytes), true)
        .assured("the peer answers the call");
    let _served = serve(accepted.connection);
    let answer = timeout(EXPECTED_WITHIN, call)
        .await
        .assured("the call ends once its answer arrives")
        .assured("the call task runs to its end");
    drop(channel);
    answer
}

/// The client accepts a header list one byte below its declared bound and refuses one that
/// reaches it, as h2 counts it.
#[nervix_primitives::test]
async fn a_header_list_reaches_the_client_only_below_its_declared_bound() {
    let declared = || ConnectOptions {
        header_list_bytes: NonZeroU32::new(8_192).assured("a positive bound"),
        ..ConnectOptions::default()
    };
    let below = call_answered_with_a_header_list_of(declared(), 8_191).await;
    assert!(
        below.is_ok(),
        "a header list below the bound reaches the client: {below:?}"
    );
    let reaching = call_answered_with_a_header_list_of(declared(), 8_192).await;
    assert!(
        reaching.is_err(),
        "a header list that reaches the bound is refused: {reaching:?}"
    );
}

/// The bytes of one chunk of an endless request body.
const BODY_CHUNK_BYTES: usize = 1024;

/// The content of every chunk of an endless request body.
static BODY_CHUNK: [u8; BODY_CHUNK_BYTES] = [0; BODY_CHUNK_BYTES];

/// A request body that never ends, counting the bytes the client has taken from it.
struct EndlessBody {
    taken: Arc<AtomicUsize>,
}

impl http_body::Body for EndlessBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        self.taken.fetch_add(BODY_CHUNK_BYTES, Ordering::Relaxed);
        Poll::Ready(Some(Ok(http_body::Frame::data(Bytes::from_static(
            &BODY_CHUNK,
        )))))
    }
}

/// The request bytes a call takes from its endless body, when the peer grants every window and
/// then reads nothing more: what fills the in-memory connection, plus the call's send buffer.
async fn bytes_a_stalled_call_takes(options: ConnectOptions) -> usize {
    let (channel, peer) = channel_to_peer(options).await;
    let (mut peer, _settings) = ScriptedPeer::accept(peer).await;
    // The peer reads nothing after this. The client writes until the in-memory connection is
    // full, then queues the call's bytes until its send buffer is full, and then takes no more.
    peer.grant_every_window().await;
    let taken = Arc::new(AtomicUsize::new(0));
    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri(http::Uri::from_static("/nervix.test.Peer/Upload"))
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header(http::header::TE, "trailers")
        .body(Body::new(EndlessBody {
            taken: Arc::clone(&taken),
        }))
        .assured("a fixed request is valid");
    let mut caller = channel.clone();
    poll_fn(|context| caller.poll_ready(context))
        .await
        .assured("the channel's connection is open and takes a call");
    let _call = AbortOnDropHandle::new(spawn(caller.call(request)));
    let mut settled = 0;
    let mut previous = 0;
    while settled < 64 {
        nervix_primitives::task::yield_now().await;
        let now = taken.load(Ordering::Relaxed);
        if now == previous {
            settled += 1;
        } else {
            previous = now;
            settled = 0;
        }
    }
    drop(peer);
    drop(channel);
    previous
}

/// A call whose peer reads nothing takes as many more body bytes under the default 1 MiB send
/// buffer than under a declared 256 KiB one as the two buffers differ: the bytes the connection
/// holds are the same in both, and at most one body chunk and one frame differ.
#[nervix_primitives::test]
async fn a_stalled_call_queues_no_more_than_its_declared_send_buffer() {
    let defaults = ConnectOptions::default();
    assert_eq!(defaults.send_buffer_bytes.get(), 1024 * 1024);
    let with_default = bytes_a_stalled_call_takes(defaults).await;
    let declared = ConnectOptions {
        send_buffer_bytes: NonZeroU32::new(256 * 1024).assured("a positive bound"),
        ..ConnectOptions::default()
    };
    let with_declared = bytes_a_stalled_call_takes(declared).await;
    let difference = with_default
        .checked_sub(with_declared)
        .assured("the larger send buffer takes more of the body");
    let expected: usize = 768 * 1024;
    // One body chunk, and the one frame the connection's writer may hold beyond the in-memory
    // stream, can fall either side.
    let tolerance = BODY_CHUNK_BYTES + 16 * 1024 + FRAME_HEADER_BYTES;
    assert!(
        difference.abs_diff(expected) <= tolerance,
        "the send buffers differ by {expected} bytes, and the call took {difference} more \
         ({with_default} under the default, {with_declared} under the declared buffer)"
    );
}

/// A connector whose connection never opens, so every call waits for it.
struct NeverConnects;

impl Service<http::Uri> for NeverConnects {
    type Response = hyper_util::rt::TokioIo<tokio::io::DuplexStream>;
    type Error = std::io::Error;
    type Future = std::future::Pending<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Uri) -> Self::Future {
        std::future::pending()
    }
}

/// How many calls a channel takes while its connection opens before the next caller waits.
async fn calls_taken_while_connecting(options: ConnectOptions) -> usize {
    let server = Url::parse("http://peer.test").assured("a fixed origin is a URL");
    let connector = GrpcConnector::new(options).assured("the test options validate");
    let channel = connector
        .endpoint(&server)
        .assured("the fixed origin is a valid endpoint")
        .connect_with_connector_lazy(NeverConnects);
    let mut waiting = Vec::new();
    loop {
        nervix_primitives::task::consume_budget().await;
        // Let the channel's sender take what it was handed before the next caller asks.
        for _ in 0..16 {
            nervix_primitives::task::yield_now().await;
        }
        let mut caller = channel.clone();
        let ready = timeout(
            Duration::ZERO,
            poll_fn(|context| caller.poll_ready(context)),
        )
        .await;
        match ready {
            Ok(ready) => {
                ready.assured("a channel whose connection is opening takes calls");
                let request = http::Request::builder()
                    .uri(http::Uri::from_static("/nervix.test.Peer/Call"))
                    .body(Body::empty())
                    .assured("a fixed request is valid");
                waiting.push(caller.call(request));
            }
            Err(_) => return waiting.len(),
        }
    }
}

/// While a channel's connection opens, it queues the declared number of calls, and its sender holds
/// one more, waiting to send it; the next caller waits for room.
#[nervix_primitives::test]
async fn calls_beyond_the_declared_queue_wait_while_the_connection_opens() {
    let defaults = ConnectOptions::default();
    assert_eq!(defaults.call_queue_capacity.get(), 1_024);
    assert_eq!(calls_taken_while_connecting(defaults).await, 1_025);
    let declared = ConnectOptions {
        call_queue_capacity: NonZeroUsize::new(3).assured("a positive capacity"),
        ..ConnectOptions::default()
    };
    assert_eq!(calls_taken_while_connecting(declared).await, 4);
}

/// The client remembers the declared number of calls it cancelled: a frame for one of them is
/// ignored, and a frame for a cancelled call beyond them is answered with RST_STREAM(STREAM_CLOSED),
/// as for a stream the client never heard of.
#[nervix_primitives::test]
async fn the_client_remembers_the_declared_number_of_cancelled_calls() {
    let defaults = ConnectOptions::default();
    assert_eq!(defaults.remembered_reset_streams.get(), 50);
    let (channel, mut peer) = channel_to_scripted_peer(defaults).await;
    let mut cancelled = Vec::new();
    for _ in 0..51 {
        cancelled.push(cancelled_call(&channel, &mut peer).await);
    }
    let last_remembered = cancelled[49];
    let first_forgotten = cancelled[50];
    // A reset for the remembered call would come first: the client resets in the order frames
    // arrive.
    peer.late_data(last_remembered).await;
    peer.late_data(first_forgotten).await;
    assert_eq!(
        peer.closed_resets_through(first_forgotten).await,
        vec![first_forgotten]
    );
    drop(channel);

    let declared = ConnectOptions {
        remembered_reset_streams: NonZeroU32::MIN,
        ..ConnectOptions::default()
    };
    let (channel, mut peer) = channel_to_scripted_peer(declared).await;
    let remembered = cancelled_call(&channel, &mut peer).await;
    let forgotten = cancelled_call(&channel, &mut peer).await;
    peer.late_data(remembered).await;
    peer.late_data(forgotten).await;
    assert_eq!(peer.closed_resets_through(forgotten).await, vec![forgotten]);
    drop(channel);
}

/// The client remembers a cancelled call for the declared duration: within it a frame for the call
/// is ignored, and once it has passed the call is forgotten and the frame answered as for a stream
/// the client never heard of.
///
/// h2 times this with the standard library's clock rather than the runtime's, so the expiry is
/// waited for in real time, with a duration far shorter than the wait.
#[nervix_primitives::test]
async fn the_client_forgets_a_cancelled_call_after_the_declared_duration() {
    let defaults = ConnectOptions::default();
    assert_eq!(defaults.reset_stream_duration, Duration::from_secs(1));
    // An hour outlasts the test. With one call remembered, a second cancelled call is forgotten at
    // once and marks where a reset for the first would have come.
    let lasting = ConnectOptions {
        remembered_reset_streams: NonZeroU32::MIN,
        reset_stream_duration: Duration::from_secs(60 * 60),
        ..ConnectOptions::default()
    };
    let (channel, mut peer) = channel_to_scripted_peer(lasting).await;
    let remembered = cancelled_call(&channel, &mut peer).await;
    let forgotten = cancelled_call(&channel, &mut peer).await;
    peer.late_data(remembered).await;
    peer.late_data(forgotten).await;
    assert_eq!(peer.closed_resets_through(forgotten).await, vec![forgotten]);
    drop(channel);

    let brief = ConnectOptions {
        reset_stream_duration: Duration::from_millis(1),
        ..ConnectOptions::default()
    };
    let (channel, mut peer) = channel_to_scripted_peer(brief).await;
    let expired = cancelled_call(&channel, &mut peer).await;
    nervix_primitives::time::sleep(Duration::from_millis(50)).await;
    peer.late_data(expired).await;
    assert_eq!(peer.closed_resets_through(expired).await, vec![expired]);
    drop(channel);
}

/// How many of the peer's protocol errors the client answers with a stream reset before it ends
/// the connection with GOAWAY(ENHANCE_YOUR_CALM). The errors are frames for calls the client
/// cancelled and no longer remembers, sent one at a time once the client has answered the last, so
/// that every answer is read in the order the client counts it.
async fn resets_before_the_client_gives_up(options: ConnectOptions, errors: usize) -> usize {
    let remembering_one = ConnectOptions {
        remembered_reset_streams: NonZeroU32::MIN,
        ..options
    };
    let (channel, mut peer) = channel_to_scripted_peer(remembering_one).await;
    let _remembered = cancelled_call(&channel, &mut peer).await;
    let mut forgotten = Vec::with_capacity(errors);
    for _ in 0..errors {
        forgotten.push(cancelled_call(&channel, &mut peer).await);
    }
    let mut resets = 0;
    for stream in forgotten {
        nervix_primitives::task::consume_budget().await;
        peer.late_data(stream).await;
        loop {
            nervix_primitives::task::consume_budget().await;
            let frame = peer.read_frame().await;
            match frame.kind {
                RST_STREAM => {
                    assert_eq!(frame.stream, stream);
                    assert_eq!(frame.error_code(), h2::Reason::STREAM_CLOSED);
                    resets += 1;
                    break;
                }
                GOAWAY => {
                    assert_eq!(frame.error_code(), h2::Reason::ENHANCE_YOUR_CALM);
                    drop(channel);
                    return resets;
                }
                _ => {}
            }
        }
    }
    panic!("the client answered all {errors} protocol errors without ending the connection")
}

/// The default and a declared error-reset limit each let that many protocol errors through as
/// stream resets and end the connection at the next.
#[nervix_primitives::test]
async fn the_client_ends_a_connection_after_the_declared_number_of_error_resets() {
    let defaults = ConnectOptions::default();
    assert_eq!(defaults.max_error_reset_streams.get(), 1_024);
    assert_eq!(
        resets_before_the_client_gives_up(defaults, 1_025).await,
        1_024
    );
    let declared = ConnectOptions {
        max_error_reset_streams: NonZeroU32::new(2).assured("a positive count"),
        ..ConnectOptions::default()
    };
    assert_eq!(resets_before_the_client_gives_up(declared, 3).await, 2);
}

/// Before the node's first SETTINGS frame says how many calls it allows, the client opens 100, the
/// number RFC 9113 recommends that every peer allows, and opens the next once they arrive.
#[nervix_primitives::test]
async fn the_client_opens_one_hundred_calls_before_the_node_settings_arrive() {
    let (channel, peer) = channel_to_peer(ConnectOptions::default()).await;
    let (mut peer, _settings) = ScriptedPeer::accept(peer).await;
    let mut calls = Vec::with_capacity(101);
    for _ in 0..101 {
        calls.push(open_call(&channel).await);
    }
    for _ in 0..100 {
        peer.next(HEADERS).await;
    }
    // Nothing more arrives: the client sends no other frame before the node's SETTINGS.
    let early = timeout(Duration::from_millis(200), peer.read_frame()).await;
    assert!(
        early.is_err(),
        "the 101st call waits for the node's SETTINGS"
    );
    peer.settle().await;
    let next = peer.next(HEADERS).await;
    assert_eq!(
        next.stream, 201,
        "the 101st call opens once the SETTINGS arrive"
    );
    drop(calls);
    drop(channel);
}

/// A resolver loaded from fixture files that asks only `authority`, for one client's connector.
async fn fixture_dns(authority: &DnsAuthority) -> (DnsResolver, tempfile::TempDir) {
    let files = tempfile::tempdir().assured("a DNS fixture directory can be created");
    let resolver_configuration = files.path().join("resolv.conf");
    let hosts_file = files.path().join("hosts");
    std::fs::write(
        &resolver_configuration,
        "search --\noptions ndots:1 timeout:1 attempts:1\n",
    )
    .assured("the fixture resolver configuration can be written");
    std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
    let dns = DnsResolver::load(
        DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![authority.address()]),
        },
        nervix_dns::DnsSettings::default(),
    )
    .await
    .assured("the fixture DNS configuration is valid");
    (dns, files)
}

/// A client connector with `options`, resolving through `authority`.
async fn connector_resolving_through(
    authority: &DnsAuthority,
    options: ConnectOptions,
) -> (GrpcConnector, tempfile::TempDir) {
    let (dns, files) = fixture_dns(authority).await;
    let mut connector = GrpcConnector::new(ConnectOptions {
        dns: ConnectDns::Resolver(dns),
        ..options
    })
    .assured("the test options validate");
    connector
        .load_dns()
        .await
        .assured("a loaded resolver is reused");
    (connector, files)
}

/// Every connection the client dials sends small writes at once rather than holding them back,
/// and asks the kernel for no keepalive.
#[nervix_primitives::test]
async fn the_connector_dials_without_delaying_writes_or_kernel_keepalive() {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .assured("the IPv4 loopback address is available");
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let authority = DnsAuthority::start_on_loopback()
        .await
        .assured("the fixture can bind a local DNS port");
    let (connector, _files) =
        connector_resolving_through(&authority, ConnectOptions::default()).await;
    let mut dialer = connector.http_connector();
    let uri: http::Uri = format!("http://127.0.0.1:{port}")
        .parse()
        .assured("a loopback origin is a URI");
    poll_fn(|context| dialer.poll_ready(context))
        .await
        .assured("the connector is always ready");
    let dialled = timeout(EXPECTED_WITHIN, dialer.call(uri))
        .await
        .assured("a loopback connection opens within the deadline")
        .assured("the loopback listener accepts the connection");
    let stream = dialled.into_inner();
    assert!(
        stream.nodelay().assured("TCP_NODELAY can be read"),
        "small writes are sent at once"
    );
    let socket = TcpSocket::from_std_stream(stream.into_std().assured("a stream can be released"));
    assert!(
        !socket.keepalive().assured("SO_KEEPALIVE can be read"),
        "the kernel sends no keepalive probes"
    );
    drop(listener);
}

/// A name whose IPv4 address accepts no connection and whose IPv6 address does.
const BOTH_FAMILIES: &str = "both-families.nervix.test";

/// An IPv6 loopback listener, and an IPv4 loopback socket bound to the same port.
struct OnePort {
    reachable: TcpListener,
    full: TcpSocket,
    port: u16,
}

/// How many fresh ports `OnePort::bind` tries before it gives up.
const PORT_ATTEMPTS: usize = 64;

impl OnePort {
    /// Binds both sockets. The operating system hands out a port per address, so a port free on
    /// the IPv6 loopback address may be taken on the IPv4 one by another socket; such a port is
    /// given up and another one tried.
    async fn bind() -> Self {
        for _ in 0..PORT_ATTEMPTS {
            nervix_primitives::task::consume_budget().await;
            let reachable = TcpListener::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0)))
                .await
                .assured("the IPv6 loopback address is available");
            let port = reachable
                .local_addr()
                .assured("a bound listener has an address")
                .port();
            let full = TcpSocket::new_v4().assured("an IPv4 socket can be created");
            match full.bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port))) {
                Ok(()) => {
                    return Self {
                        reachable,
                        full,
                        port,
                    };
                }
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {}
                Err(error) => panic!("the IPv4 loopback socket cannot bind port {port}: {error}"),
            }
        }
        panic!("no port was free on both loopback addresses in {PORT_ATTEMPTS} attempts")
    }
}

/// How long the client takes to open a channel to `BOTH_FAMILIES`. The connection attempt to the
/// IPv4 address, which the resolver answers first, never ends, so the client gets through only
/// once its connection attempt delay lets it try the IPv6 address too.
///
/// That delay is a timer the connector runs in real time, against real sockets, so this measures
/// real time and asserts only how long the client waited at least.
async fn time_to_reach_the_other_family(options: ConnectOptions) -> Duration {
    let OnePort {
        reachable,
        full,
        port,
    } = OnePort::bind().await;
    let full = full.listen(0).assured("the IPv4 socket listens");
    // Fill the IPv4 listener's accept queue. Once it is full the kernel drops every further
    // connection attempt, so an attempt to connect to it neither succeeds nor fails. A loopback
    // connection the queue has room for opens far sooner than the second each attempt is given.
    let mut queued = Vec::new();
    loop {
        nervix_primitives::task::consume_budget().await;
        let attempt = timeout(
            Duration::from_secs(1),
            TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port))),
        )
        .await;
        match attempt {
            Ok(connected) => queued.push(connected.assured("a queued connection opens")),
            Err(_) => break,
        }
        assert!(
            queued.len() < 16,
            "the accept queue of a zero backlog fills"
        );
    }
    let authority = DnsAuthority::start_on_loopback()
        .await
        .assured("the fixture can bind a local DNS port");
    authority.set(
        BOTH_FAMILIES,
        DnsAnswer::Addresses {
            addresses: vec![
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(Ipv6Addr::LOCALHOST),
            ],
            ttl: Duration::from_secs(60),
        },
    );
    let (connector, _files) = connector_resolving_through(&authority, options).await;
    let server = Url::parse(&format!("http://{BOTH_FAMILIES}:{port}"))
        .assured("the fixture name and port form a URL");
    let started = Instant::now();
    let channel = timeout(Duration::from_secs(60), connector.connect(&server))
        .await
        .assured("the client reaches the IPv6 address within the deadline")
        .assured("the IPv6 listener accepts the connection");
    let elapsed = started.elapsed();
    drop(channel);
    drop(queued);
    drop(full);
    drop(reachable);
    elapsed
}

/// The client tries a host name's second address family once its declared connection attempt
/// delay has passed: a delay longer than hyper-util's own 300 ms holds the client back for at least
/// that long, and the default 300 ms for at least its own.
#[nervix_primitives::test]
async fn the_other_address_family_is_tried_after_the_declared_attempt_delay() {
    let declared_delay = Duration::from_millis(1_500);
    let declared = ConnectOptions {
        connection_attempt_delay: declared_delay,
        ..ConnectOptions::default()
    };
    let elapsed = time_to_reach_the_other_family(declared).await;
    assert!(
        elapsed >= declared_delay,
        "the client waited {elapsed:?} before trying the IPv6 address, not its declared \
         {declared_delay:?}"
    );
    let defaults = ConnectOptions::default();
    let default_delay = defaults.connection_attempt_delay;
    assert_eq!(default_delay, Duration::from_millis(300));
    let elapsed = time_to_reach_the_other_family(defaults).await;
    assert!(
        elapsed >= default_delay,
        "the client waited {elapsed:?} before trying the IPv6 address, not the default \
         {default_delay:?}"
    );
}
