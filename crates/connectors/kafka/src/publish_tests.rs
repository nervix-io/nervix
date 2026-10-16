//! The Kafka sink's publishes against librdkafka's in-process mock cluster.
//!
//! Each test drives the production sink through a real librdkafka producer, so a record's answer
//! is the delivery report the producer gives for it. The mock brokers answer at once unless a test
//! delays, refuses or removes one of them.

use std::{collections::BTreeSet, num::NonZeroUsize, path::PathBuf, time::Duration};

use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, RecordSink, SinkAcknowledgements, SinkBoundedExecution,
    SinkEventReporter, SinkGeneralErrorHandler, SinkHost, SinkRecord, SinkRecordId,
    SinkStagingDirectory, SinkTransientErrorStatus,
};
use nervix_models::{ClientConfigEntry, Timestamp, TopicName};
use nervix_primitives::time::{Instant, sleep};
use rdkafka::{
    error::{KafkaError, RDKafkaErrorCode},
    mocking::MockCluster,
    producer::{DefaultProducerContext, Producer},
    types::{RDKafkaApiKey, RDKafkaRespErr},
};

use super::{KafkaSink, KafkaSinkConfig};

const TOPIC: &str = "orders";

/// Keys librdkafka's default partitioner sends to partition 0 of a two-partition topic: the CRC-32
/// of each key is even.
const PARTITION_ZERO_KEYS: [&str; 2] = ["answered-1", "answered-2"];

/// A key librdkafka's default partitioner sends to partition 1 of a two-partition topic: its CRC-32
/// is odd.
const PARTITION_ONE_KEY: &str = "held-1";

/// A producer queue of 1 KiB, the smallest librdkafka accepts.
const ONE_KIB_QUEUE: (&str, &str) = ("queue.buffering.max.kbytes", "1");

/// Messages of 1000 bytes at most, the smallest `message.max.bytes` librdkafka accepts.
const THOUSAND_BYTE_MESSAGES: (&str, &str) = ("message.max.bytes", "1000");

/// A payload larger than a [`ONE_KIB_QUEUE`] producer queue holds, so the producer can never take
/// it into that queue, and larger than a [`THOUSAND_BYTE_MESSAGES`] message.
const OVERSIZED_PAYLOAD_BYTES: usize = 2048;

/// A host that records nothing, for a sink whose publishes report everything in their outcome.
struct SilentHost;

impl SinkTransientErrorStatus for SilentHost {
    fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}

    fn clear_transient_error(&self) {}
}

impl SinkEventReporter for SilentHost {
    fn report_error(&self, _message: String) {}
}

impl SinkStagingDirectory for SilentHost {
    fn staging_directory(&self) -> PathBuf {
        std::env::temp_dir()
    }
}

impl SinkBoundedExecution for SilentHost {
    fn executor(&self) -> nervix_execution::Executor {
        nervix_execution::Executor::default()
    }
}

impl SinkGeneralErrorHandler for SilentHost {
    fn handle_general_error(&self, _acks: &SinkAcknowledgements, _reason: String) {}
}

/// A mock cluster of `brokers` brokers holding the test topic with `partitions` partitions.
fn cluster(brokers: i32, partitions: i32) -> MockCluster<'static, DefaultProducerContext> {
    let cluster = MockCluster::new(brokers).expect("librdkafka builds an in-process mock cluster");
    cluster
        .create_topic(TOPIC, partitions, 1)
        .expect("the mock cluster creates the test topic");
    cluster
}

/// A sink publishing in `mode` to the test topic of `cluster`, through a client whose entries add
/// `entries` to the cluster's bootstrap servers.
fn sink(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    entries: &[(&str, &str)],
    mode: BrokerPublishingMode,
) -> KafkaSink {
    let mut config = vec![ClientConfigEntry {
        key: "bootstrap.servers".to_string(),
        value: cluster.bootstrap_servers(),
    }];
    for (key, value) in entries {
        config.push(ClientConfigEntry {
            key: (*key).to_string(),
            value: (*value).to_string(),
        });
    }
    let config = KafkaSinkConfig {
        config,
        topic: TopicName::parse(TOPIC).expect("the test topic is a valid topic name"),
        mode,
    };
    KafkaSink::new(config, SinkHost::new(SilentHost))
        .expect("librdkafka accepts the test producer configuration")
}

fn confirmed(max_in_flight: usize, timeout: Duration) -> BrokerPublishingMode {
    BrokerPublishingMode::Ack(AckConfirmation {
        max_in_flight: NonZeroUsize::new(max_in_flight)
            .expect("the test window holds at least one record"),
        timeout,
    })
}

fn record(index: usize, key: &str, payload: Vec<u8>) -> SinkRecord {
    SinkRecord::new(
        SinkRecordId::new(index),
        Some(key.to_string()),
        payload,
        Vec::new(),
        Timestamp::from_unix_nanos(0),
    )
}

fn small_payload(index: usize) -> Vec<u8> {
    format!("{{\"user_id\":{index}}}").into_bytes()
}

fn ids(indexes: impl IntoIterator<Item = usize>) -> BTreeSet<SinkRecordId> {
    indexes.into_iter().map(SinkRecordId::new).collect()
}

#[nervix_primitives::test]
async fn a_full_producer_queue_settles_the_answers_the_broker_already_gave() {
    let cluster = cluster(2, 2);
    // Partition 0's leader answers at once and partition 1's a second later, so the window, full
    // at three records, waits for its oldest record on partition 1 while the broker answers the
    // two records behind it on partition 0.
    cluster
        .partition_leader(TOPIC, 0, Some(1))
        .expect("broker 1 leads partition 0");
    cluster
        .partition_leader(TOPIC, 1, Some(2))
        .expect("broker 2 leads partition 1");
    cluster
        .broker_round_trip_time(2, Duration::from_secs(1))
        .expect("the mock cluster delays broker 2");
    let mut sink = sink(
        &cluster,
        &[ONE_KIB_QUEUE],
        confirmed(3, Duration::from_secs(60)),
    );

    // Once the oldest record's answer arrives, the producer refuses the oversized record with a
    // full queue while the window still holds the two answers the broker gave.
    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ONE_KEY, small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[0], small_payload(1)),
            record(2, PARTITION_ZERO_KEYS[1], small_payload(2)),
            record(3, PARTITION_ZERO_KEYS[0], vec![0; OVERSIZED_PAYLOAD_BYTES]),
        ])
        .await;

    let parts = outcome.into_parts();
    assert_eq!(
        parts.delivered.into_iter().collect::<BTreeSet<_>>(),
        ids([0, 1, 2]),
        "every record the broker confirmed before the queue was full is delivered"
    );
    assert!(parts.rejected.is_empty());
    let failure = parts
        .infrastructure_error
        .expect("the full producer queue ends the publish");
    assert_eq!(
        failure.downcast_ref::<KafkaError>(),
        Some(&KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull))
    );
}

#[nervix_primitives::test]
async fn a_confirmed_publish_delivers_each_record_its_broker_answers() {
    let cluster = cluster(1, 1);
    let mut sink = sink(&cluster, &[], confirmed(2, Duration::from_secs(60)));

    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ONE_KEY, small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[0], small_payload(1)),
            record(2, PARTITION_ZERO_KEYS[1], small_payload(2)),
        ])
        .await;

    let parts = outcome.into_parts();
    assert_eq!(
        parts.delivered.into_iter().collect::<BTreeSet<_>>(),
        ids([0, 1, 2])
    );
    assert!(parts.rejected.is_empty());
    assert!(
        parts.infrastructure_error.is_none(),
        "a broker that answers every record fails nothing: {:?}",
        parts.infrastructure_error
    );
}

#[nervix_primitives::test]
async fn a_record_its_broker_refuses_for_good_is_rejected() {
    let cluster = cluster(1, 1);
    cluster.request_errors(
        RDKafkaApiKey::Produce,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE],
    );
    let mut sink = sink(&cluster, &[], confirmed(1, Duration::from_secs(60)));

    // A window of one hands the second record over only after the first one's answer, so each
    // travels in a produce request of its own and only the first meets the refusal.
    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[1], small_payload(1)),
        ])
        .await;

    let parts = outcome.into_parts();
    assert_eq!(parts.delivered, vec![SinkRecordId::new(1)]);
    let refused = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);
    let rejected = parts
        .rejected
        .iter()
        .map(|rejected| (rejected.id, rejected.error.message.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        rejected,
        vec![(
            SinkRecordId::new(0),
            format!("kafka rejected record: {refused}")
        )]
    );
    assert!(
        parts.infrastructure_error.is_none(),
        "a refused record fails nothing else: {:?}",
        parts.infrastructure_error
    );
}

#[nervix_primitives::test]
async fn a_record_the_producer_refuses_for_its_own_size_is_rejected_and_the_publish_goes_on() {
    let cluster = cluster(1, 1);
    let mut sink = sink(
        &cluster,
        &[THOUSAND_BYTE_MESSAGES],
        confirmed(2, Duration::from_secs(60)),
    );

    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[1], vec![0; OVERSIZED_PAYLOAD_BYTES]),
            record(2, PARTITION_ZERO_KEYS[0], small_payload(2)),
        ])
        .await;

    let parts = outcome.into_parts();
    assert_eq!(
        parts.delivered.into_iter().collect::<BTreeSet<_>>(),
        ids([0, 2]),
        "the records around the refused one are delivered"
    );
    let refused = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);
    let rejected = parts
        .rejected
        .iter()
        .map(|rejected| (rejected.id, rejected.error.message.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        rejected,
        vec![(
            SinkRecordId::new(1),
            format!("kafka rejected record: {refused}")
        )]
    );
    assert!(
        parts.infrastructure_error.is_none(),
        "a record refused for its own size fails nothing else: {:?}",
        parts.infrastructure_error
    );
}

#[nervix_primitives::test]
async fn the_producer_gives_up_a_record_the_publish_stopped_waiting_for() {
    let cluster = cluster(1, 1);
    cluster
        .broker_down(1)
        .expect("the mock cluster takes broker 1 down");
    let ack_timeout = Duration::from_millis(300);
    let mut sink = sink(&cluster, &[], confirmed(1, ack_timeout));

    let outcome = sink
        .publish(vec![record(0, PARTITION_ZERO_KEYS[0], small_payload(0))])
        .await;

    let parts = outcome.into_parts();
    assert!(parts.delivered.is_empty());
    assert!(parts.rejected.is_empty());
    assert!(
        parts.infrastructure_error.is_some(),
        "an unanswered record ends the publish once ACK TIMEOUT passes"
    );

    // The host's retry sends the record again. The producer gives the first copy up at the same
    // ACK TIMEOUT, librdkafka's next timeout scan included, rather than holding it for the broker's
    // return and delivering it beside the retry's copy.
    let deadline = Instant::now() + Duration::from_secs(30);
    while sink.producer.in_flight_count() > 0 {
        assert!(
            Instant::now() < deadline,
            "the producer still holds the record the publish gave up on"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[nervix_primitives::test]
async fn an_unconfirmed_publish_delivers_each_record_the_producer_takes() {
    let cluster = cluster(1, 1);
    let mut sink = sink(
        &cluster,
        &[THOUSAND_BYTE_MESSAGES],
        BrokerPublishingMode::NoAck,
    );

    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[1], vec![0; OVERSIZED_PAYLOAD_BYTES]),
            record(2, PARTITION_ZERO_KEYS[0], small_payload(2)),
        ])
        .await;

    let parts = outcome.into_parts();
    assert_eq!(
        parts.delivered,
        vec![SinkRecordId::new(0), SinkRecordId::new(2)]
    );
    let rejected = parts
        .rejected
        .iter()
        .map(|rejected| rejected.id)
        .collect::<Vec<_>>();
    assert_eq!(rejected, vec![SinkRecordId::new(1)]);
    assert!(
        parts.infrastructure_error.is_none(),
        "a producer that takes every other record fails nothing: {:?}",
        parts.infrastructure_error
    );
}

#[nervix_primitives::test]
async fn a_full_producer_queue_ends_an_unconfirmed_publish_after_the_records_it_took() {
    let cluster = cluster(1, 1);
    let mut sink = sink(&cluster, &[ONE_KIB_QUEUE], BrokerPublishingMode::NoAck);

    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[1], small_payload(1)),
            record(2, PARTITION_ZERO_KEYS[0], vec![0; OVERSIZED_PAYLOAD_BYTES]),
        ])
        .await;

    let parts = outcome.into_parts();
    assert_eq!(
        parts.delivered,
        vec![SinkRecordId::new(0), SinkRecordId::new(1)],
        "the producer taking a record into its queue delivers it without a delivery report"
    );
    assert!(parts.rejected.is_empty());
    let failure = parts
        .infrastructure_error
        .expect("the full producer queue ends the publish");
    assert_eq!(
        failure.downcast_ref::<KafkaError>(),
        Some(&KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull))
    );
}
