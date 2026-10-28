//! Regressions for the TCP proxy a scenario pauses or resets, and for the silent endpoint it stands
//! alone as.
//!
//! Layer: test harness.
//! - **Owns.** An echo dependency behind the proxy, a stand-in Pulsar broker that answers one topic
//!   lookup, the clients that reach them, and when the dependency read what they wrote.
//! - **Depends on.** The stallable TCP proxy, Tokio's TCP sockets and codecs, the Pulsar frame
//!   codec and the primitives boundary's monotonic clock.
//! - **Must not know.** Scenario state or any connector's protocol beyond a Pulsar topic lookup.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use futures_util::SinkExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    stream::StreamExt as _,
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, timeout},
};
use pulsar::message::{
    Codec as PulsarCodec, Message as PulsarFrame,
    proto::{
        BaseCommand, CommandLookupTopic, CommandLookupTopicResponse,
        base_command::Type as CommandType,
        command_lookup_topic_response::LookupType as TopicLookupType,
    },
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_util::codec::Framed;

use crate::stallable_tcp_proxy::StallableTcpProxy;

/// How long a test waits for an outcome the proxy must produce. Generous, because each wait ends
/// as soon as its outcome arrives.
const OUTCOME_BOUND: Duration = Duration::from_secs(30);

/// A dependency that echoes every byte it reads, and reports when it read them.
struct EchoDependency {
    address: SocketAddr,
    reads: mpsc::UnboundedReceiver<Instant>,
    task: JoinHandle<()>,
}

impl EchoDependency {
    async fn start() -> Self {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .assured("loopback has a free ephemeral port");
        let address = listener
            .local_addr()
            .assured("a bound listener knows its address");
        let (reported, reads) = mpsc::unbounded_channel();
        let task = nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                nervix_primitives::task::spawn(Self::echo(stream, reported.clone()));
            }
        });
        Self {
            address,
            reads,
            task,
        }
    }

    async fn echo(mut stream: TcpStream, reported: mpsc::UnboundedSender<Instant>) {
        let mut chunk = [0_u8; 1024];
        loop {
            nervix_primitives::task::consume_budget().await;
            let length = match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(length) => length,
            };
            if reported.send(Instant::now()).is_err() {
                return;
            }
            let bytes = chunk
                .get(..length)
                .assured("a read returns at most as many bytes as its buffer holds");
            if stream.write_all(bytes).await.is_err() {
                return;
            }
        }
    }

    /// When the dependency next read bytes from one of its connections.
    async fn next_read(&mut self) -> Instant {
        timeout(OUTCOME_BOUND, self.reads.recv())
            .await
            .assured("the proxy carries a client's bytes to the dependency within the bound")
            .assured("the dependency keeps reporting while its accept loop runs")
    }

    async fn proxy(&self) -> StallableTcpProxy {
        StallableTcpProxy::start(self.address.ip().to_string(), self.address.port())
            .await
            .assured("loopback has a free ephemeral port for the proxy")
    }
}

impl Drop for EchoDependency {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn connect(proxy: &StallableTcpProxy) -> TcpStream {
    timeout(OUTCOME_BOUND, TcpStream::connect(proxy.local_addr()))
        .await
        .assured("a loopback listener accepts a connection within the bound")
        .assured("the proxy listens on its address")
}

async fn write(client: &mut TcpStream, bytes: &[u8]) {
    client
        .write_all(bytes)
        .await
        .assured("a loopback connection takes a short write into its socket buffer");
}

async fn read_echo(client: &mut TcpStream, expected: &[u8]) {
    let mut echoed = vec![0_u8; expected.len()];
    timeout(OUTCOME_BOUND, client.read_exact(&mut echoed))
        .await
        .assured("the proxy carries the echo back within the bound")
        .assured("the connection stays open while the proxy forwards it");
    assert_eq!(echoed, expected);
}

/// A client's read once the proxy closed its connection: end of stream, or a reset.
async fn read_closed(client: &mut TcpStream) {
    let mut byte = [0_u8; 1];
    let read = timeout(OUTCOME_BOUND, client.read(&mut byte))
        .await
        .assured("a reset connection closes within the bound");
    match read {
        Ok(length) => assert_eq!(length, 0, "a reset connection carries nothing more"),
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::ConnectionReset),
    }
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_proxy_carries_bytes_both_ways() {
    let mut dependency = EchoDependency::start().await;
    let proxy = dependency.proxy().await;
    let mut client = connect(&proxy).await;

    write(&mut client, b"carried").await;

    dependency.next_read().await;
    read_echo(&mut client, b"carried").await;
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_proxy_holds_bytes_until_it_resumes() {
    let mut dependency = EchoDependency::start().await;
    let proxy = dependency.proxy().await;
    let mut open = connect(&proxy).await;
    write(&mut open, b"before").await;
    dependency.next_read().await;
    read_echo(&mut open, b"before").await;

    proxy.set_paused(true);
    write(&mut open, b"held").await;
    // A connection accepted while the proxy is paused is held the same way.
    let mut late = connect(&proxy).await;
    write(&mut late, b"late").await;
    proxy.accepted_at_least(2).await;
    let resumed = Instant::now();
    proxy.set_paused(false);

    // Nothing a client wrote while the proxy was paused reached the dependency before it resumed.
    let first = dependency.next_read().await;
    let second = dependency.next_read().await;
    assert!(first >= resumed, "a paused proxy forwarded bytes");
    assert!(second >= resumed, "a paused proxy forwarded bytes");
    read_echo(&mut open, b"held").await;
    read_echo(&mut late, b"late").await;
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn resetting_closes_forwarded_connections_and_the_proxy_keeps_listening() {
    let mut dependency = EchoDependency::start().await;
    let proxy = dependency.proxy().await;
    let mut forwarded = connect(&proxy).await;
    write(&mut forwarded, b"open").await;
    dependency.next_read().await;
    read_echo(&mut forwarded, b"open").await;

    proxy.reset_connections();

    read_closed(&mut forwarded).await;
    let mut after = connect(&proxy).await;
    write(&mut after, b"after").await;
    dependency.next_read().await;
    read_echo(&mut after, b"after").await;
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_endpoint_accepts_every_connection_and_holds_it_until_reset() {
    let endpoint = StallableTcpProxy::silent()
        .await
        .assured("loopback has a free ephemeral port for the endpoint");
    let mut first = connect(&endpoint).await;
    write(&mut first, b"unanswered").await;
    let mut second = connect(&endpoint).await;
    endpoint.accepted_at_least(2).await;

    endpoint.reset_connections();

    read_closed(&mut first).await;
    read_closed(&mut second).await;
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_endpoint_records_when_it_accepted_each_connection_in_order() {
    let endpoint = StallableTcpProxy::silent()
        .await
        .assured("loopback has a free ephemeral port for the endpoint");
    let before = Instant::now();
    let _first = connect(&endpoint).await;
    endpoint.accepted_at_least(1).await;
    let _second = connect(&endpoint).await;
    endpoint.accepted_at_least(2).await;

    let first = endpoint
        .accepted_at(0)
        .assured("the endpoint recorded the connection it accepted first");
    let second = endpoint
        .accepted_at(1)
        .assured("the endpoint recorded the connection it accepted second");
    assert!(
        before <= first,
        "a connection was recorded before it was opened"
    );
    assert!(
        first <= second,
        "the endpoint recorded its connections out of order"
    );
    assert_eq!(endpoint.accepted_at(2), None);
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_pause_resumes_the_proxy_once_its_duration_passes() {
    let mut dependency = EchoDependency::start().await;
    let proxy = dependency.proxy().await;
    let mut client = connect(&proxy).await;
    proxy.accepted_at_least(1).await;

    proxy.set_paused(true);
    let resumed = nervix_primitives::task::spawn(proxy.resume_after(Duration::from_millis(200)));
    write(&mut client, b"held").await;

    let resumed_at = timeout(OUTCOME_BOUND, resumed)
        .await
        .assured("a timed pause ends within the bound")
        .assured("the timed pause's task does not panic")
        .assured("the proxy keeps running until the pause ends");
    // The bytes written while the proxy was paused reached the dependency only once it resumed.
    let read_at = dependency.next_read().await;
    assert!(read_at >= resumed_at, "a paused proxy forwarded bytes");
    read_echo(&mut client, b"held").await;
}

/// A stand-in Pulsar broker that answers the first frame a client sends with a topic lookup answer
/// naming the broker itself.
async fn answer_one_lookup(broker: TcpListener, broker_url: String) {
    let (stream, _) = broker
        .accept()
        .await
        .assured("the proxy connects to the broker on the client's behalf");
    let mut frames = Framed::new(stream, PulsarCodec);
    let request = timeout(OUTCOME_BOUND, frames.next())
        .await
        .assured("the client's lookup reaches the broker within the bound")
        .assured("the proxy forwards the client's frame before it closes")
        .assured("the proxy carries the client's frame unchanged");
    let request_id = request
        .command
        .lookup_topic
        .assured("the client sends a topic lookup")
        .request_id;
    let answer = PulsarFrame {
        command: BaseCommand {
            r#type: i32::from(CommandType::LookupResponse),
            lookup_topic_response: Some(CommandLookupTopicResponse {
                broker_service_url: Some(broker_url),
                response: Some(i32::from(TopicLookupType::Connect)),
                request_id,
                authoritative: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        },
        payload: None,
    };
    frames
        .send(answer)
        .await
        .assured("the proxy reads the broker's answer while the client waits for it");
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pulsar_proxy_advertises_itself_in_topic_lookup_answers() {
    let broker = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .assured("loopback has a free ephemeral port");
    let broker_address = broker
        .local_addr()
        .assured("a bound listener knows its address");
    let answering = nervix_primitives::task::spawn(answer_one_lookup(
        broker,
        format!("pulsar://{broker_address}"),
    ));
    let proxy =
        StallableTcpProxy::start_pulsar(broker_address.ip().to_string(), broker_address.port())
            .await
            .assured("loopback has a free ephemeral port for the proxy");
    let stream = timeout(
        OUTCOME_BOUND,
        TcpStream::connect((Ipv4Addr::LOCALHOST, proxy.local_port())),
    )
    .await
    .assured("a loopback listener accepts a connection within the bound")
    .assured("the proxy listens on its port");
    let mut client = Framed::new(stream, PulsarCodec);

    let lookup = PulsarFrame {
        command: BaseCommand {
            r#type: i32::from(CommandType::Lookup),
            lookup_topic: Some(CommandLookupTopic {
                topic: "persistent://public/default/stalled".to_string(),
                request_id: 7,
                authoritative: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        },
        payload: None,
    };
    client
        .send(lookup)
        .await
        .assured("a loopback connection takes a short write into its socket buffer");
    let answer = timeout(OUTCOME_BOUND, client.next())
        .await
        .assured("the broker's answer reaches the client within the bound")
        .assured("the proxy forwards the answer before it closes")
        .assured("the proxy re-encodes the answer as a Pulsar frame");

    // The client's next connection goes to the proxy, so it stays behind the same pause gate.
    let answered = answer
        .command
        .lookup_topic_response
        .assured("the broker answered with a topic lookup response");
    assert_eq!(answered.request_id, 7);
    assert_eq!(
        answered.broker_service_url,
        Some(format!("pulsar://{}", proxy.local_addr()))
    );
    answering
        .await
        .assured("the stand-in broker answers without panicking");
}
