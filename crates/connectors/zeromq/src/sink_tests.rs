//! The deadlines a ZeroMQ client's keys set on its PUSH sink: connecting to the peer within
//! `connect_timeout_ms`, and each send within `timeout_ms`.
//!
//! Each test drives the production sink against a loopback peer: a port that refuses every
//! connection, a listener that takes a connection and never greets it, or a PULL socket that
//! completes the ZMTP handshake and then reads a connection only while something receives from it.

use std::time::Duration;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    DeadlineElapsed, PerRecordOutcomeParts, RecordSink, SinkPublishError, SinkRecord, SinkRecordId,
    SinkStartError, SinkStartResult,
};
use nervix_models::{ClientConfigEntry, DEFAULT_CONNECT_TIMEOUT, DeadlineKey, Timestamp};
use nervix_primitives::{
    net::{TcpListener, TcpSocket},
    task::spawn,
    time::{Instant, pause, resume, sleep, timeout},
};
use zeromq::{PullSocket, Socket as _, SocketRecv as _};

use super::{ZEROMQ, ZeroMqSink};

/// How long a test waits for an open or a publish before it reports that nothing bounded the
/// wait. On a paused clock it passes as soon as nothing else is pending.
const UNBOUNDED_WAIT: Duration = Duration::from_secs(24 * 60 * 60);

/// One message larger than every buffer between the sink and a peer that reads nothing: the
/// sink's socket send buffer, which Linux grows to at most 4 MiB unless `net.ipv4.tcp_wmem` says
/// otherwise, and the receive buffer of a peer that never reads, which keeps its initial size.
const LARGER_THAN_THE_CONNECTION: usize = 16 * 1024 * 1024;

/// How long a peer may take to read one message once it starts reading. Generous, because the
/// read ends as soon as the message arrives.
const READ_BOUND: Duration = Duration::from_secs(60);

fn entries(pairs: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
    pairs
        .iter()
        .map(|(key, value)| ClientConfigEntry {
            key: (*key).to_string(),
            value: (*value).to_string(),
        })
        .collect()
}

/// A loopback address whose port the returned socket holds without listening, so every
/// connection to it is refused for as long as the socket lives.
fn refusing_address() -> (TcpSocket, String) {
    let socket = TcpSocket::new_v4().assured("an IPv4 TCP socket can be created");
    socket
        .bind("127.0.0.1:0".parse().assured("a literal socket address"))
        .assured("a loopback TCP port is available");
    let address = socket.local_addr().assured("the socket is bound");
    (socket, format!("tcp://{address}"))
}

/// A PULL peer bound to a loopback port. It completes the ZMTP handshake of every connection,
/// and reads nothing more from a connection unless something receives from it.
async fn pull_peer() -> (PullSocket, String) {
    let mut peer = PullSocket::new();
    let endpoint = peer
        .bind("tcp://127.0.0.1:0")
        .await
        .assured("a loopback TCP port is available");
    (peer, endpoint.to_string())
}

/// Opens a sink with the client entries `config` lists, and fails the test if nothing bounds the
/// open.
async fn open(config: &[(&str, &str)]) -> SinkStartResult<ZeroMqSink> {
    let opened = timeout(UNBOUNDED_WAIT, ZeroMqSink::open(&entries(config))).await;
    let Ok(opened) = opened else {
        panic!("nothing bounded the open, which still waited after {UNBOUNDED_WAIT:?}");
    };
    opened
}

/// Publishes one record of `size` bytes, and fails the test if nothing bounds the publish.
async fn publish(sink: &mut ZeroMqSink, size: usize) -> PerRecordOutcomeParts<SinkRecordId> {
    let record = SinkRecord::new(
        SinkRecordId::new(0),
        None,
        vec![b'x'; size],
        Vec::new(),
        Timestamp::from_unix_nanos(0),
    );
    let published = timeout(UNBOUNDED_WAIT, sink.publish(vec![record])).await;
    let Ok(outcome) = published else {
        panic!("nothing bounded the publish, which still waited after {UNBOUNDED_WAIT:?}");
    };
    outcome.into_parts()
}

/// Asserts that `failure` ran out of the deadline `key` set to `bound`, with the elapsed deadline
/// beneath it, and that its message leads the report, as the emitter shows it.
fn assert_elapsed<C>(failure: &Report<C>, key: DeadlineKey, bound: Duration, message: &str) {
    let elapsed = failure
        .frames()
        .find_map(|frame| frame.downcast_ref::<DeadlineElapsed>())
        .assured("the elapsed deadline stays beneath the failure");
    assert_eq!(elapsed.key, key);
    assert_eq!(elapsed.bound, bound);
    let leading = failure
        .frames()
        .find_map(|frame| frame.downcast_ref::<String>())
        .assured("the failure carries a message");
    assert_eq!(leading, message);
}

#[nervix_primitives::test(start_paused = true)]
async fn an_open_whose_peer_refuses_every_connection_fails_at_connect_timeout_ms() {
    let (_refusing, address) = refusing_address();
    let started = Instant::now();

    let failure = open(&[("addr", &address), ("connect_timeout_ms", "1000")])
        .await
        .err()
        .assured("a peer that refuses every connection never accepts the sink");

    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert!(matches!(
        failure.current_context(),
        SinkStartError::Initialize { sink: ZEROMQ }
    ));
    assert_elapsed(
        &failure,
        DeadlineKey::ConnectTimeout,
        Duration::from_secs(1),
        "connecting to the ZeroMQ peer did not finish within connect_timeout_ms 1s",
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn an_open_whose_peer_never_answers_its_handshake_fails_at_connect_timeout_ms() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("a loopback TCP port is available");
    let address = format!(
        "tcp://{}",
        listener.local_addr().assured("the listener is bound")
    );
    // The peer takes the sink's connection and never sends its ZMTP greeting.
    let peer = spawn(async move {
        let (connection, _) = listener
            .accept()
            .await
            .assured("the sink connects to the peer");
        connection
    });
    let started = Instant::now();

    let failure = open(&[("addr", &address), ("connect_timeout_ms", "1000")])
        .await
        .err()
        .assured("a peer that never greets the sink never completes the handshake");

    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert!(matches!(
        failure.current_context(),
        SinkStartError::Initialize { sink: ZEROMQ }
    ));
    assert_elapsed(
        &failure,
        DeadlineKey::ConnectTimeout,
        Duration::from_secs(1),
        "connecting to the ZeroMQ peer did not finish within connect_timeout_ms 1s",
    );
    let _held = peer
        .await
        .assured("the peer held the connection it accepted");
}

#[nervix_primitives::test(start_paused = true)]
async fn an_open_without_connect_timeout_ms_takes_the_default_deadline() {
    let (_refusing, address) = refusing_address();
    let started = Instant::now();

    let failure = open(&[("addr", &address)])
        .await
        .err()
        .assured("a peer that refuses every connection never accepts the sink");

    assert_eq!(started.elapsed(), DEFAULT_CONNECT_TIMEOUT);
    assert_elapsed(
        &failure,
        DeadlineKey::ConnectTimeout,
        DEFAULT_CONNECT_TIMEOUT,
        "connecting to the ZeroMQ peer did not finish within connect_timeout_ms 30s",
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_deadline_that_breaks_the_rule_fails_the_open_as_invalid_configuration() {
    let (_refusing, address) = refusing_address();
    for bind in ["false", "true"] {
        for key in ["connect_timeout_ms", "timeout_ms"] {
            for value in ["0", "1s", ""] {
                let failure = open(&[("addr", &address), ("bind", bind), (key, value)])
                    .await
                    .err()
                    .assured("a deadline that breaks the rule never opens a socket");
                assert!(
                    matches!(
                        failure.current_context(),
                        SinkStartError::InvalidConfiguration { sink: ZEROMQ }
                    ),
                    "bind {bind}, {key} {value:?}: {failure:?}"
                );
            }
        }
    }
}

#[nervix_primitives::test]
async fn a_send_to_a_peer_that_stops_reading_fails_at_timeout_ms() {
    let (_peer, address) = pull_peer().await;
    let mut sink = open(&[("addr", &address), ("timeout_ms", "2000")])
        .await
        .assured("the peer completes the handshake");
    // The send fills every buffer between the sink and the peer and then waits for room, which
    // the peer never makes; the clock stops so the deadline is measured exactly.
    pause();
    let started = Instant::now();

    let outcome = publish(&mut sink, LARGER_THAN_THE_CONNECTION).await;

    // The clock paused between two of its millisecond ticks, and Tokio rounds a deadline up to
    // the next tick.
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(2) && elapsed <= Duration::from_millis(2_001),
        "the send failed after {elapsed:?}"
    );
    assert!(outcome.delivered.is_empty());
    let failure = outcome
        .infrastructure_error
        .assured("a send the peer never reads fails the publish");
    assert!(matches!(
        failure.current_context(),
        SinkPublishError::Publish { sink: ZEROMQ }
    ));
    assert_elapsed(
        &failure,
        DeadlineKey::Timeout,
        Duration::from_secs(2),
        "sending a message to the ZeroMQ peer did not finish within timeout_ms 2s",
    );
}

#[nervix_primitives::test]
async fn a_send_its_peer_reads_within_timeout_ms_delivers_the_message() {
    let (mut peer, address) = pull_peer().await;
    let mut sink = open(&[("addr", &address), ("timeout_ms", "2000")])
        .await
        .assured("the peer completes the handshake");
    let reader = spawn(async move {
        let received = timeout(READ_BOUND, peer.recv()).await;
        let Ok(received) = received else {
            panic!("the peer did not read the message within {READ_BOUND:?}");
        };
        let message = received.assured("the peer receives the message");
        let frames = message.into_vec();
        let payload = frames
            .first()
            .assured("a message carries its payload frame");
        payload.to_vec()
    });

    let outcome = publish(&mut sink, 1_024).await;

    assert_eq!(outcome.delivered, vec![SinkRecordId::new(0)]);
    assert!(outcome.infrastructure_error.is_none());
    let received = reader.await.assured("the peer read the message");
    assert_eq!(received, vec![b'x'; 1_024]);
}

#[nervix_primitives::test]
async fn a_send_without_timeout_ms_waits_for_its_peer_to_read() {
    let (mut peer, address) = pull_peer().await;
    let mut sink = open(&[("addr", &address)])
        .await
        .assured("the peer completes the handshake");
    pause();
    let reader = spawn(async move {
        // The peer reads nothing for an hour. It then reads the whole message on the real clock,
        // because the kernel signals the room each read makes on timers of its own, which a paused
        // clock would skip past.
        sleep(Duration::from_secs(3_600)).await;
        resume();
        let received = timeout(READ_BOUND, peer.recv()).await;
        let Ok(received) = received else {
            panic!("the peer did not read the message within {READ_BOUND:?}");
        };
        let message = received.assured("the peer receives the message");
        let frames = message.into_vec();
        let payload = frames
            .first()
            .assured("a message carries its payload frame");
        payload.len()
    });
    let started = Instant::now();

    let outcome = publish(&mut sink, LARGER_THAN_THE_CONNECTION).await;

    assert!(started.elapsed() >= Duration::from_secs(3_600));
    assert_eq!(outcome.delivered, vec![SinkRecordId::new(0)]);
    assert!(outcome.infrastructure_error.is_none());
    let received = reader.await.assured("the peer read the whole message");
    assert_eq!(received, LARGER_THAN_THE_CONNECTION);
}
