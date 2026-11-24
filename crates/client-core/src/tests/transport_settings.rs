//! The HTTP/2 settings of the client's gRPC channel, observed by a real HTTP/2 peer.
//!
//! The peer is the maintained `h2` server on the other end of an in-memory stream, so a test
//! decides whether the peer answers pings and reads the flow-control credit the client grants it.
//! Every channel is built from the endpoint the client's connector configures, so these tests pin
//! the values a real connection takes, not a copy of them.

use std::{future::poll_fn, num::NonZeroU32, task::Poll, time::Duration};

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    task::{AbortOnDropHandle, spawn},
    time::{Instant, timeout},
};
use tonic::{
    body::Body,
    codegen::{Service, http},
    transport::Channel,
};
use url::Url;

use crate::{ConnectOptions, connection::GrpcConnector};

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
