//! How an MQTT source reads one batch: its first publish, the publishes it gathers after it under
//! a batch timeout, and how a read ends when the host cancels it.
//!
//! Most checks read from a fake event loop the check feeds through a channel, on a paused clock,
//! so a batch timeout passes only once nothing else can happen. The last reads through the
//! client's event loop from a scripted broker, on the real clock.

use std::{future::Future, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector as _, SourceBatch, SourceBatchRequest, SourceConnector as _,
    SourceMessage as _, SourceReadCancellation,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, MqttQos, MqttSession};
use nervix_primitives::{
    sync::mpsc,
    time::{Instant, sleep, timeout},
};
use rumqttc::{Publish, QoS};

use super::{
    MqttSource, MqttSourceError, MqttSourceMessage, MqttSourcePlan, MqttSourceResult,
    MqttSourceSettings, PublishRead,
};
use crate::{scripted_broker::ScriptedBroker, test_fixtures::DnsFixture};

/// A batch timeout that only a full batch or the host's cancellation ends a gathering before.
const LONG_BATCH_TIMEOUT: Duration = Duration::from_secs(3_600);

/// How long a check through the client waits for a read, and for the publishes the scripted
/// broker wrote to reach the client.
const OBSERVATION_BUDGET: Duration = Duration::from_secs(30);

/// How long a check through the client lets the client receive before it reads again.
const RECEIVE_INTERVAL: Duration = Duration::from_millis(10);

/// The control packet type of SUBSCRIBE, which the high nibble of a packet's first byte holds.
const SUBSCRIBE: u8 = 8;

/// A connection whose publishes the check delivers through a channel. The client's event loop
/// queues every publish it reads before its poll can be dropped, so a read that stops waiting for
/// the next publish loses nothing a later read would have returned; a channel receive does the
/// same.
struct DeliveredPublishes {
    publishes: mpsc::UnboundedReceiver<Publish>,
}

impl PublishRead for DeliveredPublishes {
    fn next_publish(&mut self) -> impl Future<Output = MqttSourceResult<Publish>> + Send {
        async move {
            match self.publishes.recv().await {
                Some(publish) => Ok(publish),
                // The check dropped its end: the connection is lost.
                None => Err(Report::new(MqttSourceError::Receive)),
            }
        }
    }
}

/// A connection the check has already delivered the publishes `ids` to, and the end it delivers
/// more through.
fn delivered(ids: &[u8]) -> (mpsc::UnboundedSender<Publish>, DeliveredPublishes) {
    let (deliveries, publishes) = mpsc::unbounded_channel();
    for id in ids {
        deliveries
            .send(Publish::new("events", QoS::AtLeastOnce, vec![*id], None))
            .assured("the connection holds the receiving end");
    }
    (deliveries, DeliveredPublishes { publishes })
}

/// A request for up to `max_messages` publishes, gathered under `batch_timeout` when it has one.
fn request(max_messages: usize, batch_timeout: Option<Duration>) -> SourceBatchRequest {
    SourceBatchRequest {
        max_messages: std::num::NonZeroUsize::new(max_messages)
            .assured("a check asks for at least one message"),
        batch_timeout,
    }
}

/// What a read handed the host, by the ids of its publishes.
#[derive(Debug, PartialEq, Eq)]
enum Read {
    /// A batch the read completed: it is full, or its batch timeout passed.
    Completed(Vec<u8>),
    /// A read the host's cancellation ended, with what it had gathered.
    Interrupted(Vec<u8>),
}

impl Read {
    fn of(batch: SourceBatch<MqttSourceMessage>) -> Self {
        match batch {
            SourceBatch::Messages(messages) => Self::Completed(Self::ids(&messages)),
            SourceBatch::Interrupted { gathered } => Self::Interrupted(Self::ids(&gathered)),
            SourceBatch::ResumeRequired | SourceBatch::Closed => {
                panic!("a read from a connection neither resumes nor closes")
            }
        }
    }

    fn ids(messages: &[MqttSourceMessage]) -> Vec<u8> {
        let mut ids = Vec::with_capacity(messages.len());
        for message in messages {
            let [id] = message.payload() else {
                panic!("a check's payload is the one byte of its publish's id");
            };
            ids.push(*id);
        }
        ids
    }
}

/// Reads one batch from `connection`, which never fails while the check keeps its end.
async fn read_batch(
    connection: &mut DeliveredPublishes,
    request: SourceBatchRequest,
    cancellation: &SourceReadCancellation,
) -> Read {
    let batch = MqttSource::read_batch(connection, request, cancellation)
        .await
        .assured("a connection whose end the check keeps fails no read");
    Read::of(batch)
}

#[nervix_primitives::test(start_paused = true)]
async fn a_read_the_host_cancelled_ends_without_waiting_with_every_publish_it_took() {
    let (_deliveries, mut connection) = delivered(&[1, 2]);
    let cancellation = SourceReadCancellation::new();
    cancellation.cancel();
    let started = Instant::now();

    let read = read_batch(
        &mut connection,
        request(8, Some(LONG_BATCH_TIMEOUT)),
        &cancellation,
    )
    .await;

    assert_eq!(
        read,
        Read::Interrupted(vec![1, 2]),
        "a read the host cancelled hands over every publish it took"
    );
    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "a read the host cancelled ends without waiting for its batch timeout"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_read_cancelled_while_it_waits_for_the_next_publish_ends_with_what_it_took() {
    let (_deliveries, mut connection) = delivered(&[1]);
    let cancellation = SourceReadCancellation::new();
    let started = Instant::now();
    // The read takes the delivered publish at once and then waits for the next one, so the clock
    // moves only while it waits.
    let host = async {
        sleep(Duration::from_secs(1)).await;
        cancellation.cancel();
    };

    let (read, ()) = tokio::join!(
        read_batch(
            &mut connection,
            request(8, Some(LONG_BATCH_TIMEOUT)),
            &cancellation,
        ),
        host
    );

    assert_eq!(read, Read::Interrupted(vec![1]));
    assert_eq!(
        started.elapsed(),
        Duration::from_secs(1),
        "the read ended when the host cancelled it, not when its batch timeout passed"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_read_cancelled_before_its_first_publish_takes_nothing() {
    let (_deliveries, mut connection) = delivered(&[]);
    let cancellation = SourceReadCancellation::new();
    cancellation.cancel();

    let read = read_batch(
        &mut connection,
        request(8, Some(LONG_BATCH_TIMEOUT)),
        &cancellation,
    )
    .await;

    assert_eq!(read, Read::Interrupted(Vec::new()));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_read_whose_batch_timeout_passes_completes_with_every_publish_it_gathered() {
    let (_deliveries, mut connection) = delivered(&[1, 2]);
    let started = Instant::now();

    let read = read_batch(
        &mut connection,
        request(8, Some(Duration::from_millis(100))),
        &SourceReadCancellation::new(),
    )
    .await;

    assert_eq!(read, Read::Completed(vec![1, 2]));
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(100),
        "an uncancelled read gathers until its batch timeout passes"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_read_that_fills_its_batch_completes_at_once_and_leaves_the_rest_for_the_next() {
    let (_deliveries, mut connection) = delivered(&[1, 2, 3, 4]);
    let cancellation = SourceReadCancellation::new();
    let started = Instant::now();

    let first = read_batch(
        &mut connection,
        request(3, Some(LONG_BATCH_TIMEOUT)),
        &cancellation,
    )
    .await;
    let next = read_batch(&mut connection, request(1, None), &cancellation).await;

    assert_eq!(first, Read::Completed(vec![1, 2, 3]));
    assert_eq!(next, Read::Completed(vec![4]));
    assert_eq!(started.elapsed(), Duration::ZERO);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_read_of_one_message_or_without_a_batch_timeout_completes_with_its_first_publish() {
    let (_deliveries, mut connection) = delivered(&[1, 2]);
    let cancellation = SourceReadCancellation::new();

    let single = read_batch(
        &mut connection,
        request(1, Some(LONG_BATCH_TIMEOUT)),
        &cancellation,
    )
    .await;
    let untimed = read_batch(&mut connection, request(8, None), &cancellation).await;

    assert_eq!(single, Read::Completed(vec![1]));
    assert_eq!(untimed, Read::Completed(vec![2]));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_connection_lost_while_a_batch_gathers_fails_the_read() {
    let (deliveries, mut connection) = delivered(&[1]);
    drop(deliveries);

    let failed = MqttSource::read_batch(
        &mut connection,
        request(8, Some(LONG_BATCH_TIMEOUT)),
        &SourceReadCancellation::new(),
    )
    .await;

    let Err(error) = failed else {
        panic!("a read whose connection is lost while it gathers fails");
    };
    assert!(matches!(error.current_context(), MqttSourceError::Receive));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_batch_timeout_beyond_the_clock_fails_the_read() {
    let (_deliveries, mut connection) = delivered(&[1, 2]);

    let failed = MqttSource::read_batch(
        &mut connection,
        request(8, Some(Duration::MAX)),
        &SourceReadCancellation::new(),
    )
    .await;

    let Err(error) = failed else {
        panic!("a batch deadline the clock cannot hold fails the read");
    };
    assert!(matches!(
        error.current_context(),
        MqttSourceError::BatchDeadline
    ));
}

/// One ACK-mode source instance subscribing to `events` through `addr` and `dns`.
fn acknowledged_plan(addr: &str, dns: DnsResolver) -> MqttSourcePlan {
    MqttSourcePlan::new(MqttSourceSettings {
        instances: vec![vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: addr.to_string(),
        }]],
        client_id_conflict: None,
        default_client_id: "client".to_string(),
        share_group: "default~ingestor".to_string(),
        topic: "events".to_string(),
        session: MqttSession::Persistent,
        qos: MqttQos::AtLeastOnce,
        manual_acks: true,
        dns,
    })
}

/// A QoS 1 PUBLISH to `events` under the packet identifier `packet_id`, carrying `payload`.
fn publish_packet(packet_id: u16, payload: &[u8]) -> Vec<u8> {
    let topic = b"events";
    let [topic_high, topic_low] = u16::try_from(topic.len())
        .assured("the topic is six bytes")
        .to_be_bytes();
    let [id_high, id_low] = packet_id.to_be_bytes();
    let mut body = vec![topic_high, topic_low];
    body.extend_from_slice(topic);
    // The packet identifier, and an empty property section.
    body.extend_from_slice(&[id_high, id_low, 0]);
    body.extend_from_slice(payload);
    let remaining = u8::try_from(body.len()).assured("a check's publish is a few bytes long");
    assert!(
        remaining < 0x80,
        "a Remaining Length below 128 takes one byte"
    );
    let mut packet = vec![0x32, remaining];
    packet.extend(body);
    packet
}

#[nervix_primitives::test]
async fn cancelled_reads_through_the_client_hand_over_every_publish_the_broker_sent_once() {
    let fixture = DnsFixture::start().await;
    let broker = ScriptedBroker::start().await;
    let plan = acknowledged_plan(&broker.addr(), fixture.dns.clone());
    let mut source = MqttSource::open(&plan, 0)
        .await
        .assured("an MQTT source opens without connecting");
    let grant = async {
        let mut connection = broker.accept().await;
        let (packet_type, body) = connection.read_packet().await;
        assert_eq!(
            packet_type, SUBSCRIBE,
            "the source subscribes once connected"
        );
        // SUBACK: the SUBSCRIBE's packet identifier, an empty property section, QoS 1 granted.
        connection
            .write(&[0x90, 0x04, body[0], body[1], 0, 1])
            .await;
        connection
    };
    let (resumed, mut connection) = tokio::join!(source.resume(), grant);
    resumed.assured("the scripted broker granted the subscription");
    connection.write(&publish_packet(1, b"1")).await;
    connection.write(&publish_packet(2, b"2")).await;

    // A read the host has already cancelled takes only the publishes the client already holds, so
    // the check reads again until the client has received both.
    let cancellation = SourceReadCancellation::new();
    cancellation.cancel();
    let deadline = Instant::now()
        .checked_add(OBSERVATION_BUDGET)
        .assured("thirty seconds after now fits the monotonic clock");
    let mut handed_over = Vec::new();
    while handed_over.len() < 2 {
        nervix_primitives::task::consume_budget().await;
        let reading = source.next_batch(request(8, Some(LONG_BATCH_TIMEOUT)), &cancellation);
        let Ok(batch) = timeout(OBSERVATION_BUDGET, reading).await else {
            panic!(
                "a read the host cancelled was still gathering after {OBSERVATION_BUDGET:?}, \
                 waiting for its batch timeout"
            );
        };
        let batch = batch.assured("the scripted broker keeps the connection open");
        let SourceBatch::Interrupted { gathered } = batch else {
            panic!("a read the host cancelled ends interrupted");
        };
        if gathered.is_empty() {
            assert!(
                Instant::now() < deadline,
                "the client received the publishes within {OBSERVATION_BUDGET:?}"
            );
            sleep(RECEIVE_INTERVAL).await;
            continue;
        }
        for message in &gathered {
            handed_over.push(message.payload().to_vec());
        }
    }

    assert_eq!(
        handed_over,
        [b"1".to_vec(), b"2".to_vec()],
        "the interrupted reads handed over every publish the broker sent, each once and in order"
    );
}
