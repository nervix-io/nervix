//! How an MQTT ingestor reads one batch: its first publish, the publishes it gathers after it under
//! a batch timeout, and how a read ends when the host cancels it; and how the connection it reads
//! through acknowledges what the host accepted and keeps itself alive meanwhile.
//!
//! Most checks read from a fake connection the check feeds through a channel, on a paused clock,
//! so a batch timeout passes only once nothing else can happen. The last read and acknowledge
//! through the client's event loop and a scripted broker, on the real clock.

use std::{num::NonZeroU64, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector as _, IngestorBatch, IngestorBatchRequest, IngestorConnector as _,
    IngestorError, IngestorMessage as _, IngestorReadCancellation,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, MQTT_SOURCE_MAXIMUM_PACKET_BYTES, MqttQos, MqttSession};
use nervix_primitives::{
    sync::mpsc,
    time::{Instant, sleep, timeout},
};
use rumqttc::{Publish, QoS};

use super::{
    GatherPublishOutcome, MqttTopicIngestor, MqttTopicIngestorError, MqttTopicIngestorMessage,
    MqttTopicIngestorPlan, MqttTopicIngestorResult, MqttTopicIngestorSettings, PublishRead,
    PublishReadOutcome,
};
use crate::topic::{
    ingestor::metrics_fixture::IngestorMetricsFixture,
    scripted_broker::{ScriptedBroker, ScriptedConnection},
    test_fixtures::DnsFixture,
};

/// A batch timeout that only a full batch or the host's cancellation ends a gathering before.
const LONG_BATCH_TIMEOUT: Duration = Duration::from_secs(3_600);

/// How long a check through the client waits for a read, and for the publishes the scripted
/// broker wrote to reach the client.
const OBSERVATION_BUDGET: Duration = Duration::from_secs(30);

/// How long a check through the client lets the client receive before it reads again.
const RECEIVE_INTERVAL: Duration = Duration::from_millis(10);

/// A connection whose publishes the check delivers through a channel, as the connection's driver
/// hands them to the ingestor through its read-ahead: a read that stops waiting for the next publish
/// loses nothing a later read would have returned.
struct DeliveredPublishes {
    publishes: mpsc::UnboundedReceiver<Publish>,
}

impl DeliveredPublishes {
    async fn receive(&mut self) -> MqttTopicIngestorResult<Publish> {
        match self.publishes.recv().await {
            Some(publish) => Ok(publish),
            None => Err(Report::new(MqttTopicIngestorError::Receive)),
        }
    }
}

impl PublishRead for DeliveredPublishes {
    async fn next_publish(&mut self) -> MqttTopicIngestorResult<PublishReadOutcome> {
        Ok(PublishReadOutcome::Message(Box::new(self.receive().await?)))
    }

    async fn gather_publish(&mut self) -> MqttTopicIngestorResult<GatherPublishOutcome> {
        Ok(GatherPublishOutcome::Message(Box::new(
            self.receive().await?,
        )))
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
fn request(max_messages: usize, batch_timeout: Option<Duration>) -> IngestorBatchRequest {
    IngestorBatchRequest {
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
    fn of(batch: IngestorBatch<MqttTopicIngestorMessage>) -> Self {
        match batch {
            IngestorBatch::Messages(messages) => Self::Completed(Self::ids(&messages)),
            IngestorBatch::Interrupted { gathered } => Self::Interrupted(Self::ids(&gathered)),
            IngestorBatch::Oversized { payloads } => {
                panic!("this ordinary-publish fixture unexpectedly dropped {payloads}")
            }
            IngestorBatch::ResumeRequired | IngestorBatch::Closed => {
                panic!("a read from a connection neither resumes nor closes")
            }
        }
    }

    fn ids(messages: &[MqttTopicIngestorMessage]) -> Vec<u8> {
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
    request: IngestorBatchRequest,
    cancellation: &IngestorReadCancellation,
) -> Read {
    let batch = MqttTopicIngestor::read_batch(connection, request, cancellation)
        .await
        .assured("a connection whose end the check keeps fails no read");
    Read::of(batch)
}

#[nervix_primitives::test(start_paused = true)]
async fn a_read_the_host_cancelled_ends_without_waiting_with_every_publish_it_took() {
    let (_deliveries, mut connection) = delivered(&[1, 2]);
    let cancellation = IngestorReadCancellation::new();
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
    let cancellation = IngestorReadCancellation::new();
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
    let cancellation = IngestorReadCancellation::new();
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
        &IngestorReadCancellation::new(),
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
    let cancellation = IngestorReadCancellation::new();
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
    let cancellation = IngestorReadCancellation::new();

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

    let failed = MqttTopicIngestor::read_batch(
        &mut connection,
        request(8, Some(LONG_BATCH_TIMEOUT)),
        &IngestorReadCancellation::new(),
    )
    .await;

    let Err(error) = failed else {
        panic!("a read whose connection is lost while it gathers fails");
    };
    assert!(matches!(
        error.current_context(),
        MqttTopicIngestorError::Receive
    ));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_batch_timeout_beyond_the_clock_fails_the_read() {
    let (_deliveries, mut connection) = delivered(&[1, 2]);

    let failed = MqttTopicIngestor::read_batch(
        &mut connection,
        request(8, Some(Duration::MAX)),
        &IngestorReadCancellation::new(),
    )
    .await;

    let Err(error) = failed else {
        panic!("a batch deadline the clock cannot hold fails the read");
    };
    assert!(matches!(
        error.current_context(),
        MqttTopicIngestorError::BatchDeadline
    ));
}

/// One ACK-mode ingestor instance subscribing to `events` through `addr` and `dns`, whose publishes
/// wait on their acknowledgements `window` at a time.
fn acknowledged_plan(addr: &str, dns: DnsResolver, window: NonZeroU64) -> MqttTopicIngestorPlan {
    MqttTopicIngestorPlan::new(MqttTopicIngestorSettings {
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
        acknowledgement_window: Some(window),
        dns,
        read_ahead_dropped: IngestorMetricsFixture::new().counter(),
    })
}

/// An ingestor of `plan` that `broker` granted its subscription, and the broker's end of the ingestor's
/// connection.
async fn subscribed(
    plan: &MqttTopicIngestorPlan,
    broker: &ScriptedBroker,
) -> (MqttTopicIngestor, ScriptedConnection) {
    let mut source = MqttTopicIngestor::open(plan, 0)
        .await
        .assured("an MQTT source opens without connecting");
    let grant = async {
        let mut connection = broker.accept().await;
        connection.grant_subscription().await;
        connection
    };
    let (resumed, connection) = tokio::join!(source.resume(), grant);
    resumed.assured("the scripted broker granted the subscription");
    (source, connection)
}

#[nervix_primitives::test]
async fn cancelled_reads_through_the_client_hand_over_every_publish_the_broker_sent_once() {
    let fixture = DnsFixture::start().await;
    let broker = ScriptedBroker::start().await;
    let plan = acknowledged_plan(&broker.addr(), fixture.dns.clone(), NonZeroU64::MIN);
    let (mut source, mut connection) = subscribed(&plan, &broker).await;
    connection.write_publish(1, b"1").await;
    connection.write_publish(2, b"2").await;

    // A read the host has already cancelled takes only the publishes the client already holds, so
    // the check reads again until the client has received both.
    let cancellation = IngestorReadCancellation::new();
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
        let IngestorBatch::Interrupted { gathered } = batch else {
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

#[nervix_primitives::test]
async fn oversized_publishes_are_counted_and_acknowledged_before_the_next_publish() {
    for manual in [false, true] {
        nervix_primitives::task::consume_budget().await;
        let fixture = DnsFixture::start().await;
        let broker = ScriptedBroker::start().await;
        let mut plan = acknowledged_plan(&broker.addr(), fixture.dns.clone(), NonZeroU64::MIN);
        if !manual {
            plan.acknowledgement = super::IngestorAcknowledgement::OnReceipt;
        }
        let (mut source, mut connection) = subscribed(&plan, &broker).await;
        let maximum = usize::try_from(MQTT_SOURCE_MAXIMUM_PACKET_BYTES)
            .assured("the declared admission bound fits the address space");
        // The payload fills the admission bound, so its complete packet exceeds it.
        connection.write_publish(1, &vec![b'x'; maximum]).await;
        connection.write_publish(2, b"2").await;

        let cancellation = IngestorReadCancellation::new();
        let read = source.next_batch(request(8, Some(LONG_BATCH_TIMEOUT)), &cancellation);
        let Ok(result) = timeout(OBSERVATION_BUDGET, read).await else {
            panic!("the oversized publish was not consumed within {OBSERVATION_BUDGET:?}");
        };
        let batch = result.assured("a bounded oversized publish keeps its broker connection");
        let IngestorBatch::Oversized { payloads } = batch else {
            panic!("the source reports the consumed oversized payload before ordinary messages");
        };
        assert_eq!(payloads.get(), 1);
        let Ok(packet_id) = timeout(OBSERVATION_BUDGET, connection.read_puback()).await else {
            panic!("the oversized publish's PUBACK was not written within {OBSERVATION_BUDGET:?}");
        };
        assert_eq!(packet_id, 1);

        let messages = read_whole_batch(&mut source, 1).await;
        assert_eq!(messages[0].payload(), b"2");
        if manual {
            source
                .acknowledge(&positions(&messages))
                .await
                .assured("the connected source writes the ordinary publish's acknowledgement");
        }
        let Ok(packet_id) = timeout(OBSERVATION_BUDGET, connection.read_puback()).await else {
            panic!("the ordinary publish's PUBACK was not written within {OBSERVATION_BUDGET:?}");
        };
        assert_eq!(packet_id, 2);
    }
}

#[nervix_primitives::test]
async fn an_oversized_publish_splits_a_parallel_batch_without_losing_its_neighbours() {
    let fixture = DnsFixture::start().await;
    let broker = ScriptedBroker::start().await;
    let plan = acknowledged_plan(
        &broker.addr(),
        fixture.dns.clone(),
        nonzero_ext::nonzero!(3_u64),
    );
    let (mut source, mut connection) = subscribed(&plan, &broker).await;
    let maximum = usize::try_from(MQTT_SOURCE_MAXIMUM_PACKET_BYTES)
        .assured("the declared admission bound fits the address space");
    connection.write_publish(1, b"1").await;
    connection.write_publish(2, &vec![b'x'; maximum]).await;
    connection.write_publish(3, b"3").await;

    let cancellation = IngestorReadCancellation::new();
    let reading = source.next_batch(request(3, Some(LONG_BATCH_TIMEOUT)), &cancellation);
    let Ok(result) = timeout(OBSERVATION_BUDGET, reading).await else {
        panic!("the partial batch was not returned within {OBSERVATION_BUDGET:?}");
    };
    let batch = result.assured("an oversized publish does not fail a partial batch");
    let IngestorBatch::Messages(first) = batch else {
        panic!("the ordinary publish before the oversized one completes its partial batch");
    };
    assert_eq!(Read::ids(&first), vec![b'1']);
    source
        .acknowledge(&positions(&first))
        .await
        .assured("the first ordinary publish's acknowledgement is written");
    let Ok(packet_id) = timeout(OBSERVATION_BUDGET, connection.read_puback()).await else {
        panic!("the first ordinary acknowledgement did not reach the broker");
    };
    assert_eq!(packet_id, 1);

    let batch = source
        .next_batch(
            request(3, Some(LONG_BATCH_TIMEOUT)),
            &IngestorReadCancellation::new(),
        )
        .await
        .assured("the consumed oversized notification is retained");
    let IngestorBatch::Oversized { payloads } = batch else {
        panic!("the next read reports the oversized publish exactly once");
    };
    assert_eq!(payloads.get(), 1);
    let Ok(packet_id) = timeout(OBSERVATION_BUDGET, connection.read_puback()).await else {
        panic!("the oversized acknowledgement did not reach the broker");
    };
    assert_eq!(packet_id, 2);
    let last = read_whole_batch(&mut source, 1).await;
    assert_eq!(Read::ids(&last), vec![b'3']);
    source
        .acknowledge(&positions(&last))
        .await
        .assured("the final ordinary publish's acknowledgement is written");
    let Ok(packet_id) = timeout(OBSERVATION_BUDGET, connection.read_puback()).await else {
        panic!("the final ordinary acknowledgement did not reach the broker");
    };
    assert_eq!(packet_id, 3);
}

#[nervix_primitives::test]
async fn a_connection_the_broker_closes_fails_the_read_and_is_dropped() {
    let fixture = DnsFixture::start().await;
    let broker = ScriptedBroker::start().await;
    let plan = acknowledged_plan(&broker.addr(), fixture.dns.clone(), NonZeroU64::MIN);
    let (mut source, mut connection) = subscribed(&plan, &broker).await;
    connection.write_publish(1, b"1").await;
    drop(connection);

    let cancellation = IngestorReadCancellation::new();
    let reading = source.next_batch(request(8, Some(LONG_BATCH_TIMEOUT)), &cancellation);
    let Ok(read) = timeout(OBSERVATION_BUDGET, reading).await else {
        panic!(
            "a read whose connection the broker closed was still waiting after \
             {OBSERVATION_BUDGET:?}"
        );
    };

    let Err(error) = read else {
        panic!("a read whose connection the broker closed fails");
    };
    assert!(matches!(
        error.current_context(),
        IngestorError::Read { connector: "mqtt" }
    ));
    assert!(matches!(
        error.downcast_ref::<MqttTopicIngestorError>(),
        Some(MqttTopicIngestorError::Receive)
    ));
    assert!(
        source.needs_resume(),
        "a read that lost its connection drops it, so the source resumes before it reads again"
    );
}

/// The control packet type of PINGREQ, which the high nibble of a packet's first byte holds.
const PINGREQ: u8 = 12;

/// Reads one complete batch of `messages` publishes the scripted broker already wrote, through the
/// client.
async fn read_whole_batch(
    source: &mut MqttTopicIngestor,
    messages: usize,
) -> Vec<MqttTopicIngestorMessage> {
    let cancellation = IngestorReadCancellation::new();
    let reading = source.next_batch(request(messages, Some(LONG_BATCH_TIMEOUT)), &cancellation);
    let Ok(batch) = timeout(OBSERVATION_BUDGET, reading).await else {
        panic!("the client received {messages} publishes within {OBSERVATION_BUDGET:?}");
    };
    let batch = batch.assured("the scripted broker keeps the connection open");
    let IngestorBatch::Messages(read) = batch else {
        panic!("an uncancelled read of a full batch completes with its messages");
    };
    assert_eq!(
        read.len(),
        messages,
        "the batch holds every publish it asked for"
    );
    read
}

/// The positions of `messages`, which the host acknowledges or rejects together.
fn positions(messages: &[MqttTopicIngestorMessage]) -> Vec<super::MqttTopicIngestorPosition> {
    let mut positions = Vec::with_capacity(messages.len());
    for message in messages {
        positions.push(message.position().clone());
    }
    positions
}

/// Reads the `count` PUBACKs the client sends next, and returns the packet identifiers they
/// acknowledge, in the order the client wrote them.
async fn read_pubacks(connection: &mut ScriptedConnection, count: usize) -> Vec<u16> {
    let mut acknowledged = Vec::with_capacity(count);
    for _ in 0..count {
        nervix_primitives::task::consume_budget().await;
        let Ok(packet_id) = timeout(OBSERVATION_BUDGET, connection.read_puback()).await else {
            panic!("the client wrote {count} PUBACKs within {OBSERVATION_BUDGET:?}");
        };
        acknowledged.push(packet_id);
    }
    acknowledged
}

#[nervix_primitives::test]
async fn an_acknowledged_batch_has_written_every_puback_it_asks_for() {
    let fixture = DnsFixture::start().await;
    let broker = ScriptedBroker::start().await;
    let window = NonZeroU64::new(3).assured("three is non-zero");
    let plan = acknowledged_plan(&broker.addr(), fixture.dns.clone(), window);
    let (mut source, mut connection) = subscribed(&plan, &broker).await;
    for packet_id in 1..=3_u16 {
        connection.write_publish(packet_id, b"x").await;
    }
    let read = read_whole_batch(&mut source, 3).await;

    let positions = positions(&read);
    let acknowledging = source.acknowledge(&positions);
    let Ok(acknowledged) = timeout(OBSERVATION_BUDGET, acknowledging).await else {
        panic!("an acknowledgement through a polled connection returned within its budget");
    };
    acknowledged.assured("the connection wrote every PUBACK the batch asks for");

    assert_eq!(
        read_pubacks(&mut connection, 3).await,
        [1, 2, 3],
        "the broker received every PUBACK the batch asks for, in order"
    );
}

#[nervix_primitives::test]
async fn a_connection_sends_its_keep_alive_while_its_ingestor_reads_nothing() {
    let fixture = DnsFixture::start().await;
    let broker = ScriptedBroker::start().await;
    let plan = acknowledged_plan(&broker.addr(), fixture.dns.clone(), NonZeroU64::MIN);
    let mut source = MqttTopicIngestor::open(&plan, 0)
        .await
        .assured("an MQTT source opens without connecting");
    let grant = async {
        let mut connection = broker.accept_announcing_keep_alive(1).await;
        connection.grant_subscription().await;
        connection
    };
    let (resumed, mut connection) = tokio::join!(source.resume(), grant);
    resumed.assured("the scripted broker granted the subscription");

    // From here on the ingestor reads nothing, as while its host waits on acknowledgement trees.
    let Ok((packet_type, _)) = timeout(OBSERVATION_BUDGET, connection.read_packet()).await else {
        panic!(
            "a connection whose broker announced a one-second keep-alive sent nothing within \
             {OBSERVATION_BUDGET:?}"
        );
    };
    assert_eq!(
        packet_type, PINGREQ,
        "the connection keeps its keep-alive while its source reads nothing"
    );
    assert!(!source.needs_resume());
}

#[nervix_primitives::test]
async fn a_batch_of_a_whole_window_of_publishes_is_acknowledged_without_waiting_for_room() {
    let fixture = DnsFixture::start().await;
    let broker = ScriptedBroker::start().await;
    let window = NonZeroU64::new(1_500).assured("1,500 is non-zero");
    let plan = acknowledged_plan(&broker.addr(), fixture.dns.clone(), window);
    let (mut source, mut connection) = subscribed(&plan, &broker).await;
    for packet_id in 1..=1_500_u16 {
        connection.write_publish(packet_id, b"x").await;
    }
    let read = read_whole_batch(&mut source, 1_500).await;

    let positions = positions(&read);
    let acknowledging = source.acknowledge(&positions);
    let Ok(acknowledged) = timeout(OBSERVATION_BUDGET, acknowledging).await else {
        panic!(
            "acknowledging a whole window of 1,500 publishes still waited after \
             {OBSERVATION_BUDGET:?}"
        );
    };
    acknowledged.assured("the connection wrote every PUBACK the batch asks for");

    let mut expected = Vec::with_capacity(1_500);
    expected.extend(1..=1_500_u16);
    assert_eq!(read_pubacks(&mut connection, 1_500).await, expected);
}

#[nervix_primitives::test]
async fn an_acknowledgement_after_its_connection_ended_fails_and_drops_the_connection() {
    let fixture = DnsFixture::start().await;
    let broker = ScriptedBroker::start().await;
    let plan = acknowledged_plan(&broker.addr(), fixture.dns.clone(), NonZeroU64::MIN);
    let (mut source, mut connection) = subscribed(&plan, &broker).await;
    connection.write_publish(1, b"x").await;
    let read = read_whole_batch(&mut source, 1).await;
    drop(connection);
    let deadline = Instant::now()
        .checked_add(OBSERVATION_BUDGET)
        .assured("thirty seconds after now fits the monotonic clock");
    loop {
        nervix_primitives::task::consume_budget().await;
        let ended = match source.connection.as_ref() {
            Some(connection) => connection.has_ended(),
            None => panic!("the source keeps its connection until it reads or acknowledges"),
        };
        if ended {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the connection noticed the broker closed it within {OBSERVATION_BUDGET:?}"
        );
        sleep(RECEIVE_INTERVAL).await;
    }

    let Err(error) = source.acknowledge(&positions(&read)).await else {
        panic!("an acknowledgement whose connection ended cannot be written");
    };
    assert!(matches!(
        error.current_context(),
        IngestorError::Acknowledge { connector: "mqtt" }
    ));
    assert!(
        error.frames().any(|frame| matches!(
            frame.downcast_ref::<MqttTopicIngestorError>(),
            Some(MqttTopicIngestorError::Receive)
        )),
        "the lost connection is the cause beneath the acknowledgement: {error:?}"
    );
    assert!(
        source.needs_resume(),
        "an acknowledgement that failed drops its connection, so the source resumes first"
    );
    let Err(rejected) = source.reject(&positions(&read)).await else {
        panic!("a publish whose connection ended cannot be rejected on another one");
    };
    assert!(matches!(
        rejected.current_context(),
        IngestorError::Reject { connector: "mqtt" }
    ));
}
