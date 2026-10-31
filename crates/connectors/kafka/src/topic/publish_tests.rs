//! The Kafka emitter's publishes against librdkafka's in-process mock cluster.
//!
//! Each test drives the production emitter through a real librdkafka producer, so a record's answer
//! is the delivery report the producer gives for it. The mock brokers answer at once unless a test
//! delays, refuses or removes one of them.

use std::{
    collections::BTreeSet,
    future::Future,
    num::NonZeroUsize,
    path::PathBuf,
    pin::Pin,
    task::{Context, Wake, Waker},
    time::Duration,
};

use meticulous::ResultExt as _;
use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, EmitterBoundedExecution, EmitterEventReporter,
    EmitterFinish, EmitterFinishError, EmitterGeneralErrorHandler, EmitterHost, EmitterLifecycle,
    EmitterRecord, EmitterRecordAcknowledgements, EmitterRecordId, EmitterStagingDirectory,
    EmitterTransientErrorStatus, RecordEmitter, RecordEnvelope,
};
use nervix_models::{ClientConfigEntry, KafkaTopicName, Timestamp};
use nervix_primitives::{
    sync::{
        StdArc,
        blocking::{Mutex, mpsc},
        oneshot,
    },
    time::{Instant, sleep, timeout},
};
use rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer as _},
    error::{KafkaError, RDKafkaErrorCode},
    message::{Message as _, Timestamp as KafkaTimestamp},
    mocking::MockCluster,
    producer::DefaultProducerContext,
    types::{RDKafkaApiKey, RDKafkaRespErr},
};

use super::{
    KafkaAnswer, KafkaCreateTime, KafkaQueueWaitError, KafkaTopicEmitter, KafkaTopicEmitterConfig,
};

pub(super) const TOPIC: &str = "orders";

/// Keys librdkafka's default partitioner sends to partition 0 of a two-partition topic: the CRC-32
/// of each key is even.
pub(super) const PARTITION_ZERO_KEYS: [&str; 2] = ["answered-1", "answered-2"];

/// A key librdkafka's default partitioner sends to partition 1 of a two-partition topic: its CRC-32
/// is odd.
const PARTITION_ONE_KEY: &str = "held-1";

/// A producer queue of 1 KiB, the smallest librdkafka accepts.
const ONE_KIB_QUEUE: (&str, &str) = ("queue.buffering.max.kbytes", "1");

/// A producer that holds every record it takes for two minutes before it sends a batch, so a
/// record a `NO ACK` publish delivered is still queued when the test finishes the emitter. librdkafka
/// refuses a record whose linger is not shorter than its default five-minute message timeout.
const TWO_MINUTE_LINGER: (&str, &str) = ("linger.ms", "120000");

/// Messages of 1000 bytes at most, the smallest `message.max.bytes` librdkafka accepts.
const THOUSAND_BYTE_MESSAGES: (&str, &str) = ("message.max.bytes", "1000");

/// A producer that reconnects to a broker within a tenth of a second of its return.
pub(super) const FAST_RECONNECT: [(&str, &str); 2] = [
    ("reconnect.backoff.ms", "10"),
    ("reconnect.backoff.max.ms", "100"),
];

/// A payload larger than a [`ONE_KIB_QUEUE`] producer queue holds, so the producer can never take
/// it into that queue, and larger than a [`THOUSAND_BYTE_MESSAGES`] message.
const OVERSIZED_PAYLOAD_BYTES: usize = 2048;

/// A host that records nothing, for a emitter whose publishes report everything in their outcome. It
/// hands the emitter `executor`.
pub(super) struct SilentHost {
    executor: nervix_execution::Executor,
}

impl SilentHost {
    pub(super) fn on(executor: nervix_execution::Executor) -> Self {
        Self { executor }
    }
}

impl EmitterTransientErrorStatus for SilentHost {
    fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}

    fn clear_transient_error(&self) {}
}

impl EmitterEventReporter for SilentHost {
    fn report_error(&self, _message: String) {}
}

impl EmitterStagingDirectory for SilentHost {
    fn staging_directory(&self) -> PathBuf {
        std::env::temp_dir()
    }
}

impl EmitterBoundedExecution for SilentHost {
    fn executor(&self) -> nervix_execution::Executor {
        self.executor.clone()
    }
}

impl EmitterGeneralErrorHandler for SilentHost {
    fn handle_general_error(&self, _acks: &EmitterRecordAcknowledgements, _reason: String) {}
}

/// A waker that holds the thread that wakes it until the test lets it go.
///
/// A record's answer polled once with this waker is woken by librdkafka's polling thread while it
/// serves that record's delivery report. The wake keeps the thread there: the report's event is not
/// destroyed yet, and every event the producer queues after it waits to be served.
pub(super) struct PollingThreadHold {
    /// Tells the test that the waker holds a thread, the first time it is woken.
    held: Mutex<Option<oneshot::Sender<()>>>,
    /// Ends once the test drops its end, which lets the held thread go.
    released: Mutex<mpsc::Receiver<()>>,
}

/// A [`PollingThreadHold`]'s waker and the test's two ends of the hold.
pub(super) struct HeldWaker {
    pub(super) waker: Waker,
    /// Resolves once the waker holds a thread.
    pub(super) held: oneshot::Receiver<()>,
    /// Lets the held thread go when dropped. Nothing is ever sent through it.
    pub(super) release: mpsc::Sender<()>,
}

impl PollingThreadHold {
    pub(super) fn waker() -> HeldWaker {
        let (held_sender, held) = oneshot::channel();
        let (release, released) = mpsc::channel();
        let hold = StdArc::new(Self {
            held: Mutex::new(Some(held_sender)),
            released: Mutex::new(released),
        });
        HeldWaker {
            waker: Waker::from(hold),
            held,
            release,
        }
    }
}

impl Wake for PollingThreadHold {
    fn wake(self: StdArc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        if let Some(held) = self.held.lock().take() {
            held.send(())
                .expect("the test awaits the hold before it lets the thread go");
        }
        let released = self.released.lock().recv();
        assert!(
            released.is_err(),
            "the test lets the held thread go by dropping its end, never by sending through it"
        );
    }
}

/// A mock cluster of `brokers` brokers holding the test topic with `partitions` partitions.
pub(super) fn cluster(
    brokers: i32,
    partitions: i32,
) -> MockCluster<'static, DefaultProducerContext> {
    let cluster = MockCluster::new(brokers).expect("librdkafka builds an in-process mock cluster");
    cluster
        .create_topic(TOPIC, partitions, 1)
        .expect("the mock cluster creates the test topic");
    cluster
}

/// A emitter publishing in `mode` to the test topic of `cluster`, through a client whose entries add
/// `entries` to the cluster's bootstrap servers.
fn sink(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    entries: &[(&str, &str)],
    mode: BrokerPublishingMode,
) -> KafkaTopicEmitter {
    emitter_on(
        cluster,
        entries,
        mode,
        nervix_execution::Executor::default(),
    )
}

/// A emitter like [`emitter`]'s, whose host hands it `executor`.
pub(super) fn emitter_on(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    entries: &[(&str, &str)],
    mode: BrokerPublishingMode,
    executor: nervix_execution::Executor,
) -> KafkaTopicEmitter {
    KafkaTopicEmitter::new(
        emitter_config(cluster, entries, mode),
        EmitterHost::new(SilentHost::on(executor)),
    )
    .expect("librdkafka accepts the test producer configuration")
}

/// The configuration of a emitter publishing in `mode` to the test topic of `cluster`, through a
/// client whose entries add `entries` to the cluster's bootstrap servers.
pub(super) fn emitter_config(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    entries: &[(&str, &str)],
    mode: BrokerPublishingMode,
) -> KafkaTopicEmitterConfig {
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
    KafkaTopicEmitterConfig {
        config,
        topic: KafkaTopicName::parse(TOPIC).expect("the test topic is a valid topic name"),
        mode,
    }
}

pub(super) fn confirmed(max_in_flight: usize, timeout: Duration) -> BrokerPublishingMode {
    BrokerPublishingMode::Ack(AckConfirmation {
        max_in_flight: NonZeroUsize::new(max_in_flight)
            .expect("the test window holds at least one record"),
        timeout,
    })
}

pub(super) fn record(index: usize, key: &str, payload: Vec<u8>) -> EmitterRecord {
    EmitterRecord::new(
        EmitterRecordId::new(index),
        Some(key.to_string()),
        payload,
        Vec::new(),
        Timestamp::from_unix_nanos(0),
    )
}

pub(super) fn small_payload(index: usize) -> Vec<u8> {
    format!("{{\"user_id\":{index}}}").into_bytes()
}

fn ids(indexes: impl IntoIterator<Item = usize>) -> BTreeSet<EmitterRecordId> {
    indexes.into_iter().map(EmitterRecordId::new).collect()
}

/// Hands `record` to `emitter`'s producer as a confirmed publish does, and returns the answer that
/// publish would await for it.
pub(super) fn awaited_hand_over(sink: &KafkaTopicEmitter, record: EmitterRecord) -> KafkaAnswer {
    let (answer, report) = oneshot::channel();
    let create_time =
        KafkaCreateTime::of(&record).expect("a test record's timestamp is a Kafka record time");
    sink.hand_over(&record, create_time, Some(answer))
        .expect("the producer takes the record");
    KafkaAnswer { report }
}

/// A record like [`record`]'s, whose envelope's `TIMESTAMP` is the RFC 3339 instant `written`.
fn timed_record(index: usize, written: &str) -> EmitterRecord {
    let timestamp = written
        .parse::<Timestamp>()
        .expect("every case writes an RFC 3339 instant");
    record(index, PARTITION_ZERO_KEYS[0], small_payload(index)).with_envelope(RecordEnvelope {
        timestamp: Some(timestamp),
        ..RecordEnvelope::default()
    })
}

/// One record a consumer read back from the test topic: its payload and the time its Kafka record
/// carries.
#[derive(Debug, PartialEq, Eq)]
struct ReadBack {
    payload: Vec<u8>,
    time: KafkaTimestamp,
}

/// The first `count` records of the test topic, read back in partition order by a consumer of
/// `cluster` within thirty seconds.
fn read_back(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    count: usize,
) -> Vec<ReadBack> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("group.id", "record-times")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .assured("librdkafka accepts the reader's configuration");
    consumer
        .subscribe(&[TOPIC])
        .assured("a reader subscribes to the test topic");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut records = Vec::with_capacity(count);
    while records.len() < count {
        assert!(
            Instant::now() < deadline,
            "read back {} of {count} records within thirty seconds",
            records.len()
        );
        let Some(message) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        let message = message.assured("the mock broker serves the records the test produced");
        records.push(ReadBack {
            payload: message.payload().unwrap_or_default().to_vec(),
            time: message.timestamp(),
        });
    }
    records
}

#[nervix_primitives::test]
async fn a_confirmed_window_larger_than_the_producer_queue_delivers_every_record() {
    let cluster = cluster(1, 1);
    let mut sink = sink(
        &cluster,
        &[("queue.buffering.max.messages", "2"), ("linger.ms", "200")],
        confirmed(8, Duration::from_secs(60)),
    );
    let records = (0..8)
        .map(|index| record(index, PARTITION_ZERO_KEYS[0], small_payload(index)))
        .collect();

    let parts = sink.publish(records).await.into_parts();

    assert!(
        parts.infrastructure_error.is_none(),
        "a full producer queue waits for its outstanding answers: {:?}",
        parts.infrastructure_error
    );
    assert!(parts.rejected.is_empty());
    assert_eq!(parts.delivered.len(), 8);
    assert_eq!(
        parts.delivered.into_iter().collect::<BTreeSet<_>>(),
        ids(0..8)
    );
    assert_eq!(sink.retained_records(), 0);
}

#[nervix_primitives::test]
async fn a_record_larger_than_the_whole_producer_queue_is_rejected() {
    let cluster = cluster(1, 1);
    let mut sink = sink(
        &cluster,
        &[ONE_KIB_QUEUE],
        confirmed(8, Duration::from_secs(60)),
    );
    let parts = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[0], vec![0; OVERSIZED_PAYLOAD_BYTES]),
            record(2, PARTITION_ZERO_KEYS[0], small_payload(2)),
        ])
        .await
        .into_parts();

    assert!(
        parts.infrastructure_error.is_none(),
        "a record the queue can never hold is a record rejection: {:?}",
        parts.infrastructure_error
    );
    assert_eq!(
        parts.delivered.into_iter().collect::<BTreeSet<_>>(),
        ids([0, 2])
    );
    assert_eq!(parts.rejected.len(), 1);
    assert_eq!(parts.rejected[0].id, EmitterRecordId::new(1));
    assert!(parts.rejected[0].error.message.contains("2048"));
    assert!(parts.rejected[0].error.message.contains("1024"));
}

#[nervix_primitives::test]
async fn a_producer_queue_still_occupied_by_a_served_callback_waits_for_its_return() {
    let cluster = cluster(1, 1);
    cluster
        .broker_down(1)
        .assured("broker 1 belongs to the single-broker mock cluster");
    let mut sink = sink(
        &cluster,
        &[("queue.buffering.max.messages", "1")],
        confirmed(8, Duration::from_secs(60)),
    );
    let hold = PollingThreadHold::waker();
    let mut probe = awaited_hand_over(&sink, record(0, PARTITION_ZERO_KEYS[0], small_payload(0)));
    assert!(
        Pin::new(&mut probe)
            .poll(&mut Context::from_waker(&hold.waker))
            .is_pending()
    );
    cluster
        .broker_up(1)
        .assured("broker 1 belongs to the single-broker mock cluster");
    assert!(
        matches!(
            timeout(Duration::from_secs(30), hold.held).await,
            Ok(Ok(()))
        ),
        "the polling thread must hold its callback before the native queue place is freed"
    );

    let mut publishing =
        Box::pin(sink.publish(vec![record(1, PARTITION_ZERO_KEYS[0], small_payload(1))]));
    let polled = publishing
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    // Release before asserting so a failing assertion cannot keep the producer's thread held.
    drop(hold.release);
    assert!(
        polled.is_pending(),
        "the refused record waits for the callback to release its queue place"
    );
    let parts = publishing.await.into_parts();
    assert!(parts.infrastructure_error.is_none());
    assert!(parts.rejected.is_empty());
    assert_eq!(parts.delivered, vec![EmitterRecordId::new(1)]);
}

#[nervix_primitives::test]
async fn a_producer_queue_held_without_an_outstanding_answer_expires_its_admission_wait() {
    let cluster = cluster(1, 1);
    cluster
        .broker_down(1)
        .assured("broker 1 belongs to the single-broker mock cluster");
    let ack_timeout = Duration::from_millis(50);
    let mut sink = sink(
        &cluster,
        &[("queue.buffering.max.messages", "1")],
        confirmed(8, ack_timeout),
    );
    let hold = PollingThreadHold::waker();
    let mut probe = awaited_hand_over(&sink, record(0, PARTITION_ZERO_KEYS[0], small_payload(0)));
    assert!(
        Pin::new(&mut probe)
            .poll(&mut Context::from_waker(&hold.waker))
            .is_pending()
    );
    cluster
        .broker_up(1)
        .assured("broker 1 belongs to the single-broker mock cluster");
    assert!(
        matches!(
            timeout(Duration::from_secs(30), hold.held).await,
            Ok(Ok(()))
        ),
        "the polling thread must hold its callback before the native queue place is freed"
    );

    let parts = sink
        .publish(vec![record(1, PARTITION_ZERO_KEYS[0], small_payload(1))])
        .await
        .into_parts();
    drop(hold.release);
    let Some(failure) = parts.infrastructure_error else {
        panic!("a callback that stays held must exhaust admission's ACK TIMEOUT");
    };
    assert_eq!(
        failure.downcast_ref::<KafkaQueueWaitError>(),
        Some(&KafkaQueueWaitError::TimedOut {
            timeout: ack_timeout
        })
    );
    assert_eq!(
        failure.downcast_ref::<KafkaError>(),
        Some(&KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull))
    );
    assert!(parts.delivered.is_empty());
    assert!(parts.rejected.is_empty());
}

#[nervix_primitives::test]
async fn an_unconfirmed_publish_rejects_a_payload_larger_than_its_producer_queue() {
    let cluster = cluster(1, 1);
    let mut sink = sink(&cluster, &[ONE_KIB_QUEUE], BrokerPublishingMode::NoAck);
    let parts = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], vec![0; OVERSIZED_PAYLOAD_BYTES]),
            record(1, PARTITION_ZERO_KEYS[0], small_payload(1)),
        ])
        .await
        .into_parts();
    assert!(parts.infrastructure_error.is_none());
    assert_eq!(parts.delivered, vec![EmitterRecordId::new(1)]);
    assert_eq!(parts.rejected.len(), 1);
    assert_eq!(parts.rejected[0].id, EmitterRecordId::new(0));
    assert!(parts.rejected[0].error.message.contains("2048"));
    assert!(parts.rejected[0].error.message.contains("1024"));
}

#[nervix_primitives::test]
async fn a_producer_queue_accepts_its_exact_payload_capacity_with_key_and_headers() {
    let cluster = cluster(1, 1);
    let mut sink = sink(
        &cluster,
        &[ONE_KIB_QUEUE],
        confirmed(1, Duration::from_secs(60)),
    );
    let mut message = record(0, &"k".repeat(2048), vec![0; 1024]);
    message.headers = vec![("large-header".to_string(), "v".repeat(2048))];
    let parts = sink.publish(vec![message]).await.into_parts();
    assert!(parts.infrastructure_error.is_none());
    assert!(parts.rejected.is_empty());
    assert_eq!(parts.delivered, vec![EmitterRecordId::new(0)]);
}

#[nervix_primitives::test]
async fn a_producer_queue_size_rejection_keeps_the_answers_the_broker_already_gave() {
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

    // The final record exceeds the whole queue's byte capacity while the window still holds
    // answers for the preceding records.
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
    assert_eq!(parts.rejected.len(), 1);
    assert_eq!(parts.rejected[0].id, EmitterRecordId::new(3));
    assert!(parts.infrastructure_error.is_none());
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
    assert_eq!(parts.delivered, vec![EmitterRecordId::new(1)]);
    let refused = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);
    let rejected = parts
        .rejected
        .iter()
        .map(|rejected| (rejected.id, rejected.error.message.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        rejected,
        vec![(
            EmitterRecordId::new(0),
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
            EmitterRecordId::new(1),
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
    while sink.retained_records() > 0 {
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
        vec![EmitterRecordId::new(0), EmitterRecordId::new(2)]
    );
    let rejected = parts
        .rejected
        .iter()
        .map(|rejected| rejected.id)
        .collect::<Vec<_>>();
    assert_eq!(rejected, vec![EmitterRecordId::new(1)]);
    assert!(
        parts.infrastructure_error.is_none(),
        "a producer that takes every other record fails nothing: {:?}",
        parts.infrastructure_error
    );
}

#[nervix_primitives::test]
async fn a_full_producer_queue_ends_an_unconfirmed_publish_after_the_records_it_took() {
    let cluster = cluster(1, 1);
    cluster
        .broker_down(1)
        .assured("broker 1 belongs to the single-broker mock cluster");
    let mut sink = sink(
        &cluster,
        &[("queue.buffering.max.messages", "2")],
        BrokerPublishingMode::NoAck,
    );

    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[1], small_payload(1)),
            record(2, PARTITION_ZERO_KEYS[0], small_payload(2)),
        ])
        .await;

    let parts = outcome.into_parts();
    assert_eq!(
        parts.delivered,
        vec![EmitterRecordId::new(0), EmitterRecordId::new(1)],
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

#[nervix_primitives::test]
async fn finishing_sends_the_records_a_lingering_producer_still_queues() {
    let cluster = cluster(1, 1);
    let mut sink = sink(&cluster, &[TWO_MINUTE_LINGER], BrokerPublishingMode::NoAck);
    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[1], small_payload(1)),
        ])
        .await;
    let parts = outcome.into_parts();
    assert!(
        parts.infrastructure_error.is_none(),
        "the lingering producer takes both records: {:?}",
        parts.infrastructure_error
    );
    assert_eq!(
        parts.delivered,
        vec![EmitterRecordId::new(0), EmitterRecordId::new(1)]
    );
    assert_eq!(
        sink.retained_records(),
        2,
        "the lingering producer still queues both delivered records"
    );

    let finished = sink
        .finish(Instant::now() + Duration::from_secs(30))
        .await
        .expect("the mock broker takes every queued record before the deadline");

    assert_eq!(finished, EmitterFinish::Flushed);
    assert_eq!(
        sink.retained_records(),
        0,
        "the flush sends every queued record despite the linger"
    );
}

#[nervix_primitives::test]
async fn finishing_a_producer_that_queues_nothing_has_nothing_to_send() {
    let cluster = cluster(1, 1);
    let mut sink = sink(&cluster, &[], confirmed(2, Duration::from_secs(60)));
    let outcome = sink
        .publish(vec![record(0, PARTITION_ZERO_KEYS[0], small_payload(0))])
        .await;
    assert_eq!(
        outcome.into_parts().delivered,
        vec![EmitterRecordId::new(0)]
    );

    let finished = sink
        .finish(Instant::now() + Duration::from_secs(30))
        .await
        .expect("a producer whose records were all answered has nothing to send");

    assert_eq!(finished, EmitterFinish::NothingHeld);
}

#[nervix_primitives::test]
async fn finishing_after_its_deadline_counts_the_records_the_producer_still_queues() {
    let cluster = cluster(1, 1);
    cluster
        .broker_down(1)
        .expect("the mock cluster takes broker 1 down");
    let mut sink = sink(&cluster, &[], BrokerPublishingMode::NoAck);
    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[1], small_payload(1)),
            record(2, PARTITION_ZERO_KEYS[0], small_payload(2)),
        ])
        .await;
    assert_eq!(
        outcome.into_parts().delivered,
        vec![
            EmitterRecordId::new(0),
            EmitterRecordId::new(1),
            EmitterRecordId::new(2)
        ]
    );

    let unsent = sink
        .finish(Instant::now())
        .await
        .expect_err("a broker that is down takes nothing before a deadline that already passed");

    assert_eq!(
        *unsent.current_context(),
        EmitterFinishError::Unsent {
            connector: "kafka",
            records: 3,
        }
    );
    assert_eq!(
        unsent.downcast_ref::<KafkaError>(),
        Some(&KafkaError::Flush(RDKafkaErrorCode::OperationTimedOut)),
        "the flush's own timeout stays beneath the count"
    );
}

#[nervix_primitives::test]
async fn finishing_counts_the_queued_records_while_a_served_report_holds_the_polling_thread() {
    let cluster = cluster(2, 2);
    cluster
        .partition_leader(TOPIC, 0, Some(1))
        .expect("broker 1 leads partition 0");
    cluster
        .partition_leader(TOPIC, 1, Some(2))
        .expect("broker 2 leads partition 1");
    // Broker 2 stays down, so the producer keeps every record on partition 1 queued.
    cluster
        .broker_down(2)
        .expect("the mock cluster takes broker 2 down");
    // Broker 1 stays down until the probe's answer is awaited, so the probe's report cannot arrive
    // before the hold's waker is registered for it.
    cluster
        .broker_down(1)
        .expect("the mock cluster takes broker 1 down");
    let mut sink = sink(&cluster, &FAST_RECONNECT, BrokerPublishingMode::NoAck);
    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ONE_KEY, small_payload(0)),
            record(1, PARTITION_ONE_KEY, small_payload(1)),
            record(2, PARTITION_ONE_KEY, small_payload(2)),
        ])
        .await;
    assert_eq!(
        outcome.into_parts().delivered,
        vec![
            EmitterRecordId::new(0),
            EmitterRecordId::new(1),
            EmitterRecordId::new(2)
        ]
    );

    let hold = PollingThreadHold::waker();
    let mut probe = awaited_hand_over(&sink, record(3, PARTITION_ZERO_KEYS[0], small_payload(3)));
    let polled = Pin::new(&mut probe).poll(&mut Context::from_waker(&hold.waker));
    assert!(
        polled.is_pending(),
        "no broker is up to answer the probe record yet"
    );
    cluster
        .broker_up(1)
        .expect("the mock cluster brings broker 1 up");
    timeout(Duration::from_secs(30), hold.held)
        .await
        .expect("the polling thread serves the probe record's delivery report within 30s")
        .expect("the hold tells the test the thread it holds");

    // The polling thread is inside the probe record's delivery callback, so librdkafka still counts
    // the probe's message and queues every event behind it. Neither is a record the producer holds.
    let unsent = sink
        .finish(Instant::now())
        .await
        .expect_err("a broker that is down takes nothing before a deadline that already passed");

    assert_eq!(
        *unsent.current_context(),
        EmitterFinishError::Unsent {
            connector: "kafka",
            records: 3,
        }
    );
    assert_eq!(
        unsent.downcast_ref::<KafkaError>(),
        Some(&KafkaError::Flush(RDKafkaErrorCode::OperationTimedOut)),
        "the flush's own timeout stays beneath the count"
    );
    drop(hold.release);
}

#[nervix_primitives::test]
async fn finishing_while_only_a_served_report_holds_the_polling_thread_finds_no_record_held() {
    let cluster = cluster(1, 1);
    // The broker stays down until the probe's answer is awaited, so the probe's report cannot
    // arrive before the hold's waker is registered for it.
    cluster
        .broker_down(1)
        .expect("the mock cluster takes broker 1 down");
    let mut sink = sink(&cluster, &FAST_RECONNECT, BrokerPublishingMode::NoAck);
    let hold = PollingThreadHold::waker();
    let mut probe = awaited_hand_over(&sink, record(0, PARTITION_ZERO_KEYS[0], small_payload(0)));
    let polled = Pin::new(&mut probe).poll(&mut Context::from_waker(&hold.waker));
    assert!(
        polled.is_pending(),
        "no broker is up to answer the probe record yet"
    );
    cluster
        .broker_up(1)
        .expect("the mock cluster brings broker 1 up");
    timeout(Duration::from_secs(30), hold.held)
        .await
        .expect("the polling thread serves the probe record's delivery report within 30s")
        .expect("the hold tells the test the thread it holds");

    // Only the delivery callback the polling thread is inside, and the events queued behind it,
    // remain: the producer holds no record.
    let finished = sink
        .finish(Instant::now() + Duration::from_secs(30))
        .await
        .expect("a producer that holds no record has nothing to send");

    assert_eq!(finished, EmitterFinish::NothingHeld);
    drop(hold.release);
}

#[nervix_primitives::test]
async fn a_record_the_producer_refuses_for_its_own_size_is_never_retained() {
    let cluster = cluster(1, 1);
    // The broker stays down, so every record the producer takes stays retained.
    cluster
        .broker_down(1)
        .expect("the mock cluster takes broker 1 down");
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
        vec![EmitterRecordId::new(0), EmitterRecordId::new(2)]
    );
    let rejected = parts
        .rejected
        .iter()
        .map(|rejected| rejected.id)
        .collect::<Vec<_>>();
    assert_eq!(rejected, vec![EmitterRecordId::new(1)]);
    assert_eq!(
        sink.retained_records(),
        2,
        "the producer retains the two records it took, and not the one it refused"
    );
    let unsent = sink
        .finish(Instant::now())
        .await
        .expect_err("a broker that is down takes nothing before a deadline that already passed");
    assert_eq!(
        *unsent.current_context(),
        EmitterFinishError::Unsent {
            connector: "kafka",
            records: 2,
        }
    );
}

#[nervix_primitives::test]
async fn a_record_a_full_producer_queue_refuses_is_never_retained() {
    let cluster = cluster(1, 1);
    // The broker stays down, so every record the producer takes stays retained.
    cluster
        .broker_down(1)
        .expect("the mock cluster takes broker 1 down");
    let mut sink = sink(
        &cluster,
        &[("queue.buffering.max.messages", "2")],
        BrokerPublishingMode::NoAck,
    );

    let outcome = sink
        .publish(vec![
            record(0, PARTITION_ZERO_KEYS[0], small_payload(0)),
            record(1, PARTITION_ZERO_KEYS[1], small_payload(1)),
            record(2, PARTITION_ZERO_KEYS[0], small_payload(2)),
        ])
        .await;

    let parts = outcome.into_parts();
    assert_eq!(
        parts.delivered,
        vec![EmitterRecordId::new(0), EmitterRecordId::new(1)]
    );
    let failure = parts
        .infrastructure_error
        .expect("the full producer queue ends the publish");
    assert_eq!(
        failure.downcast_ref::<KafkaError>(),
        Some(&KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull))
    );
    assert_eq!(
        sink.retained_records(),
        2,
        "the producer retains the two records it took, and not the one its full queue refused"
    );
}

#[nervix_primitives::test]
async fn finishing_counts_a_record_a_confirmed_publish_stopped_waiting_for() {
    let cluster = cluster(1, 1);
    cluster
        .broker_down(1)
        .expect("the mock cluster takes broker 1 down");
    let mut sink = sink(&cluster, &[], confirmed(1, Duration::from_secs(60)));
    // Cancelling the wait does not release the record librdkafka already took.
    let mut publishing =
        Box::pin(sink.publish(vec![record(0, PARTITION_ZERO_KEYS[0], small_payload(0))]));
    assert!(
        publishing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(publishing);
    let unsent = sink
        .finish(Instant::now())
        .await
        .expect_err("a broker that is down takes nothing before a deadline that already passed");
    assert_eq!(
        *unsent.current_context(),
        EmitterFinishError::Unsent {
            connector: "kafka",
            records: 1,
        },
        "the record the publish stopped waiting for is still the producer's"
    );
}

#[nervix_primitives::test]
async fn a_flush_that_gives_up_after_every_report_was_served_sent_every_record() {
    let cluster = cluster(1, 1);
    // The broker stays down until the finish waits in its flush, so both reports arrive during it.
    cluster
        .broker_down(1)
        .expect("the mock cluster takes broker 1 down");
    let mut sink = sink(&cluster, &FAST_RECONNECT, BrokerPublishingMode::NoAck);
    let outcome = sink
        .publish(vec![record(0, PARTITION_ZERO_KEYS[0], small_payload(0))])
        .await;
    assert_eq!(
        outcome.into_parts().delivered,
        vec![EmitterRecordId::new(0)]
    );
    // The probe follows record 0 into the queue of the one partition, so one produce request
    // carries both and one delivery report event answers them, record 0 first.
    let hold = PollingThreadHold::waker();
    let mut probe = awaited_hand_over(&sink, record(1, PARTITION_ZERO_KEYS[1], small_payload(1)));
    let polled = Pin::new(&mut probe).poll(&mut Context::from_waker(&hold.waker));
    assert!(
        polled.is_pending(),
        "no broker is up to answer the probe record yet"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut finishing = std::pin::pin!(sink.finish(deadline));
    let started = finishing
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    assert!(
        started.is_pending(),
        "the finish waits in its flush for the two records the producer retains"
    );
    cluster
        .broker_up(1)
        .expect("the mock cluster brings broker 1 up");
    // The polling thread serves record 0's report, then the probe's, and is held inside the
    // probe's. librdkafka holds both messages until it has served their event, so the flush gives
    // up at its deadline although neither record is retained any more.
    let finished = finishing
        .await
        .expect("a producer whose every record was reported has sent them");

    assert_eq!(finished, EmitterFinish::Flushed);
    hold.held
        .await
        .expect("the polling thread was held inside the probe's report before the flush gave up");
    drop(hold.release);
}

#[nervix_primitives::test]
async fn a_record_carries_its_envelope_timestamp_as_the_time_its_kafka_record_was_created() {
    let cluster = cluster(1, 1);
    let mut sink = sink(&cluster, &[], confirmed(1, Duration::from_secs(60)));

    let parts = sink
        .publish(vec![
            timed_record(0, "2001-02-03T04:05:06.789Z"),
            timed_record(1, "2001-02-03T04:05:06.789999999Z"),
            timed_record(2, "1970-01-01T00:00:00.001Z"),
        ])
        .await
        .into_parts();

    assert!(
        parts.infrastructure_error.is_none(),
        "a broker that answers every record fails nothing: {:?}",
        parts.infrastructure_error
    );
    assert!(parts.rejected.is_empty());
    assert_eq!(
        parts.delivered.into_iter().collect::<BTreeSet<_>>(),
        ids(0..3)
    );
    assert_eq!(
        read_back(&cluster, 3),
        vec![
            ReadBack {
                payload: small_payload(0),
                time: KafkaTimestamp::CreateTime(981_173_106_789),
            },
            ReadBack {
                payload: small_payload(1),
                time: KafkaTimestamp::CreateTime(981_173_106_789),
            },
            ReadBack {
                payload: small_payload(2),
                time: KafkaTimestamp::CreateTime(1),
            },
        ]
    );
}

#[nervix_primitives::test]
async fn a_record_without_a_timestamp_carries_the_create_time_its_producer_gives_it() {
    let cluster = cluster(1, 1);
    let mut sink = sink(&cluster, &[], confirmed(1, Duration::from_secs(60)));

    // The timed record goes first, so a record that took its create time from the record before it
    // would read back with the timed record's 2001 time.
    let parts = sink
        .publish(vec![
            timed_record(0, "2001-02-03T04:05:06.789Z"),
            record(1, PARTITION_ZERO_KEYS[0], small_payload(1)),
        ])
        .await
        .into_parts();

    assert_eq!(
        parts.delivered.into_iter().collect::<BTreeSet<_>>(),
        ids(0..2)
    );
    let read = read_back(&cluster, 2);
    let [timed, untimed] = read.as_slice() else {
        panic!("the consumer reads back exactly the two records: {read:?}");
    };
    assert_eq!(
        timed,
        &ReadBack {
            payload: small_payload(0),
            time: KafkaTimestamp::CreateTime(981_173_106_789),
        }
    );
    assert_eq!(untimed.payload, small_payload(1));
    let KafkaTimestamp::CreateTime(created) = untimed.time else {
        panic!("the producer gives a record without a timestamp its own create time: {untimed:?}");
    };
    assert!(
        created > 981_173_106_789,
        "the producer's clock gives the untimed record a time after its timed neighbour's, not \
         {created}"
    );
}

#[nervix_primitives::test]
async fn a_confirmed_publish_rejects_a_record_whose_timestamp_precedes_the_first_record_time() {
    let cluster = cluster(1, 1);
    let mut sink = sink(&cluster, &[], confirmed(4, Duration::from_secs(60)));

    let parts = sink
        .publish(vec![
            timed_record(0, "2001-02-03T04:05:06.789Z"),
            timed_record(1, "1970-01-01T00:00:00.000999999Z"),
            timed_record(2, "1969-12-31T23:59:59.999Z"),
            timed_record(3, "1970-01-01T00:00:00.001Z"),
        ])
        .await
        .into_parts();

    assert!(
        parts.infrastructure_error.is_none(),
        "a record without a Kafka record time fails nothing else: {:?}",
        parts.infrastructure_error
    );
    assert_eq!(
        parts.delivered.into_iter().collect::<BTreeSet<_>>(),
        ids([0, 3])
    );
    let rejected = parts
        .rejected
        .iter()
        .map(|rejected| (rejected.id, rejected.error.message.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        rejected,
        vec![
            (
                EmitterRecordId::new(1),
                "kafka record timestamp 1970-01-01T00:00:00.000999999Z precedes \
                 1970-01-01T00:00:00.001Z, the earliest time a kafka record carries"
                    .to_string()
            ),
            (
                EmitterRecordId::new(2),
                "kafka record timestamp 1969-12-31T23:59:59.999Z precedes \
                 1970-01-01T00:00:00.001Z, the earliest time a kafka record carries"
                    .to_string()
            ),
        ]
    );
    assert_eq!(
        read_back(&cluster, 2),
        vec![
            ReadBack {
                payload: small_payload(0),
                time: KafkaTimestamp::CreateTime(981_173_106_789),
            },
            ReadBack {
                payload: small_payload(3),
                time: KafkaTimestamp::CreateTime(1),
            },
        ]
    );
    assert_eq!(sink.retained_records(), 0);
}

#[nervix_primitives::test]
async fn an_unconfirmed_publish_rejects_a_record_whose_timestamp_precedes_the_first_record_time() {
    let cluster = cluster(1, 1);
    let mut sink = sink(&cluster, &[], BrokerPublishingMode::NoAck);

    let parts = sink
        .publish(vec![
            timed_record(0, "1970-01-01T00:00:00Z"),
            timed_record(1, "2001-02-03T04:05:06.789Z"),
        ])
        .await
        .into_parts();

    assert!(
        parts.infrastructure_error.is_none(),
        "a record without a Kafka record time fails nothing else: {:?}",
        parts.infrastructure_error
    );
    assert_eq!(parts.delivered, vec![EmitterRecordId::new(1)]);
    let rejected = parts
        .rejected
        .iter()
        .map(|rejected| (rejected.id, rejected.error.message.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        rejected,
        vec![(
            EmitterRecordId::new(0),
            "kafka record timestamp 1970-01-01T00:00:00Z precedes 1970-01-01T00:00:00.001Z, the \
             earliest time a kafka record carries"
                .to_string()
        )]
    );
    assert_eq!(
        read_back(&cluster, 1),
        vec![ReadBack {
            payload: small_payload(1),
            time: KafkaTimestamp::CreateTime(981_173_106_789),
        }]
    );
}
