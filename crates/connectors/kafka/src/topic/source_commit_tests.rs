//! The Kafka source's consumer-group commits against librdkafka's in-process mock cluster.
//!
//! Each test drives the production source through a real librdkafka consumer that joins a group on
//! the mock broker, and reads the group's committed offset back through a second consumer, so a
//! commit counts only once the broker stored it. The broker answers at once unless a test makes it
//! refuse a commit or hold its answers.

use std::{num::NonZeroUsize, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, ParsedRetryPolicy, SourceAckPolicy, SourceBatch, SourceBatchRequest,
    SourceConnector, SourceMessage, SourceReadCancellation, SourceResume,
};
use nervix_models::{ClientConfigEntry, KafkaTopicName};
use nervix_primitives::time::timeout;
use rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::{KafkaError, RDKafkaErrorCode},
    mocking::MockCluster,
    producer::{DefaultProducerContext, FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    types::{RDKafkaApiKey, RDKafkaRespErr},
};

use super::{
    GroupCommitRequest, GroupCommits, KafkaGroupCommit, KafkaOffsetPosition, KafkaSource,
    KafkaSourceError, KafkaSourceOffsetMode, KafkaSourcePlan,
};

const TOPIC: &str = "orders";
const GROUP: &str = "orders-ingestor";

/// An auto-commit interval no test outlasts, so the driver's own cadence never commits during one.
const DRIVER_CADENCE_MS: &str = "3600000";

/// A round-trip time no test outlasts, so the broker holds the answer to every request it receives
/// until the test sets its round-trip time back to zero.
const HELD_ROUND_TRIP: Duration = Duration::from_secs(3600);

/// How long a test waits for its source to join the group and read the published record. The mock
/// coordinator holds a new group's first rebalance for three seconds.
pub(super) const READ_DEADLINE: Duration = Duration::from_secs(60);

/// A mock cluster of one broker holding the test topic with one partition.
pub(super) fn cluster() -> MockCluster<'static, DefaultProducerContext> {
    let cluster = MockCluster::new(1).assured("librdkafka builds an in-process mock cluster");
    cluster
        .create_topic(TOPIC, 1, 1)
        .assured("the mock cluster creates the test topic");
    cluster
}

/// The plan of a source in the test group, whose offsets `commit` commits.
pub(super) fn plan(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    commit: KafkaGroupCommit,
) -> KafkaSourcePlan {
    let entries = [
        ("bootstrap.servers", cluster.bootstrap_servers()),
        ("auto.offset.reset", "earliest".to_string()),
        ("auto.commit.interval.ms", DRIVER_CADENCE_MS.to_string()),
    ];
    let config = entries
        .into_iter()
        .map(|(key, value)| ClientConfigEntry {
            key: key.to_string(),
            value,
        })
        .collect();
    KafkaSourcePlan {
        executor: nervix_execution::Executor::default(),
        config,
        topic: KafkaTopicName::parse(TOPIC).assured("the test topic is a valid Kafka topic"),
        offset_mode: KafkaSourceOffsetMode::ConsumerGroup {
            group_id: GROUP.to_string(),
            commit,
        },
    }
}

/// Publishes one record to the test topic and returns once the broker stored it.
pub(super) async fn publish(cluster: &MockCluster<'static, DefaultProducerContext>) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .create()
        .assured("librdkafka accepts the test producer configuration");
    let record = FutureRecord::<(), str>::to(TOPIC).payload("order");
    let delivered = producer.send(record, Duration::from_secs(10)).await;
    delivered
        .map_err(|(error, _record)| error)
        .assured("the mock broker stores the test record");
}

/// A source whose offsets `commit` commits, subscribed to the test topic in the test group.
pub(super) async fn subscribed_source(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    commit: KafkaGroupCommit,
) -> KafkaSource {
    let mut source = KafkaSource::open(&plan(cluster, commit), 0)
        .await
        .assured("librdkafka accepts the test consumer configuration");
    let resumed = source
        .resume()
        .await
        .assured("a consumer subscribes to a topic its broker serves");
    assert!(matches!(resumed, SourceResume::Ready));
    source
}

/// Reads the next record from `source` once it joined its group, and returns its position.
pub(super) async fn read_positions(source: &mut KafkaSource) -> Vec<KafkaOffsetPosition> {
    let request = SourceBatchRequest {
        max_messages: NonZeroUsize::MIN,
        batch_timeout: None,
    };
    let batch = timeout(
        READ_DEADLINE,
        source.next_batch(request, &SourceReadCancellation::new()),
    )
    .await
    .assured("the source joins its group and reads the published record within its deadline")
    .assured("the source reads the record its broker serves");
    let SourceBatch::Messages(messages) = batch else {
        panic!("a subscribed source reads a batch of messages");
    };
    messages
        .iter()
        .map(|message| message.position().clone())
        .collect()
}

/// The next offset the test group committed for the test topic's partition, absent while it
/// committed none.
fn committed_offset(cluster: &MockCluster<'static, DefaultProducerContext>) -> Option<i64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("group.id", GROUP)
        .set("enable.auto.commit", "false")
        .create()
        .assured("librdkafka accepts the offset reader's configuration");
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(TOPIC, 0);
    let committed = consumer
        .committed_offsets(partitions, Duration::from_secs(10))
        .assured("the mock coordinator answers an offset fetch");
    let element = committed
        .find_partition(TOPIC, 0)
        .assured("an offset fetch answers every partition it asked for");
    match element.offset() {
        Offset::Offset(offset) => Some(offset),
        Offset::Beginning
        | Offset::End
        | Offset::Stored
        | Offset::Invalid
        | Offset::OffsetTail(_) => None,
    }
}

#[nervix_primitives::test]
async fn an_acknowledged_batch_returns_once_its_group_committed_it() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut source = subscribed_source(&cluster, KafkaGroupCommit::EachAcknowledgedBatch).await;
    let positions = read_positions(&mut source).await;

    // librdkafka retries this answer, so the broker stores the commit on its third attempt.
    cluster.request_errors(
        RDKafkaApiKey::OffsetCommit,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS; 2],
    );
    source
        .acknowledge(&positions)
        .await
        .assured("the broker stores the commit's third attempt");

    assert_eq!(committed_offset(&cluster), Some(1));
}

#[nervix_primitives::test]
async fn a_commit_its_group_refuses_fails_the_acknowledgement() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut source = subscribed_source(&cluster, KafkaGroupCommit::EachAcknowledgedBatch).await;
    let positions = read_positions(&mut source).await;

    cluster.request_errors(
        RDKafkaApiKey::OffsetCommit,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED],
    );
    let acknowledged = source.acknowledge(&positions).await;

    let Err(report) = acknowledged else {
        panic!("a commit the broker refuses fails the acknowledgement");
    };
    let failure = report
        .downcast_ref::<KafkaSourceError>()
        .assured("the acknowledgement failure names the commit that failed");
    assert!(
        matches!(failure, KafkaSourceError::CommitOffset { group } if group == GROUP),
        "unexpected Kafka source failure {failure:?}"
    );
    let cause = report
        .downcast_ref::<KafkaError>()
        .assured("the commit failure keeps the driver's typed cause");
    assert_eq!(
        cause,
        &KafkaError::ConsumerCommit(RDKafkaErrorCode::GroupAuthorizationFailed)
    );
    assert_eq!(committed_offset(&cluster), None);
}

#[nervix_primitives::test]
async fn a_commit_its_coordinator_does_not_answer_fails_at_its_deadline_and_holds_the_next() {
    let cluster = cluster();
    publish(&cluster).await;
    publish(&cluster).await;
    let mut source = subscribed_source(&cluster, KafkaGroupCommit::EachAcknowledgedBatch).await;
    let first_positions = read_positions(&mut source).await;
    let second_positions = read_positions(&mut source).await;
    let short_deadline = Duration::from_secs(1);
    let mut commits = GroupCommits::new(short_deadline);

    // The coordinator holds its answers, so a commit stays unanswered until the test releases
    // them. Its broker stays up: an outage can outlast the member's session, after which the
    // coordinator refuses the commits of a member it no longer knows.
    cluster
        .broker_round_trip_time(1, HELD_ROUND_TRIP)
        .assured("the mock cluster delays the answers of a broker it has");
    for _attempt in 0..2 {
        let request = GroupCommitRequest::acknowledged(&first_positions)
            .assured("the read position is a valid offset");
        let committed = commits
            .commit(&source.control, &source.consumer, GROUP, request)
            .await;
        let Err(report) = committed else {
            panic!("a commit no coordinator answers fails at its deadline");
        };
        assert!(
            matches!(
                report.current_context(),
                KafkaSourceError::CommitUnanswered { group, deadline }
                    if group == GROUP && *deadline == short_deadline
            ),
            "unexpected Kafka source failure {report:?}"
        );
        assert!(commits.unanswered.is_some());
    }

    // Once the coordinator answers again, the next commit waits for the unanswered one and then
    // commits its own, later offset: the unanswered commit's late answer is not the next one's.
    cluster
        .broker_round_trip_time(1, Duration::ZERO)
        .assured("the mock cluster releases the answers of a broker it has");
    commits.deadline = READ_DEADLINE;
    let request = GroupCommitRequest::acknowledged(&second_positions)
        .assured("the read position is a valid offset");
    commits
        .commit(&source.control, &source.consumer, GROUP, request)
        .await
        .assured("the group's coordinator answers once it releases its answers");
    assert!(commits.unanswered.is_none());
    assert_eq!(committed_offset(&cluster), Some(2));
}

#[nervix_primitives::test]
async fn suspending_commits_what_the_driver_delivered_since_its_last_commit() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut source = subscribed_source(&cluster, KafkaGroupCommit::DriverCadence).await;
    let positions = read_positions(&mut source).await;

    source
        .acknowledge(&positions)
        .await
        .assured("acknowledging under the driver's cadence commits nothing itself");
    assert_eq!(committed_offset(&cluster), None);

    source
        .suspend()
        .await
        .assured("the broker stores the commit of what the driver delivered");
    assert_eq!(committed_offset(&cluster), Some(1));
}

#[nervix_primitives::test]
async fn closing_commits_what_the_driver_delivered_since_its_last_commit() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut source = subscribed_source(&cluster, KafkaGroupCommit::DriverCadence).await;
    read_positions(&mut source).await;

    source
        .close()
        .await
        .assured("the broker stores the commit of what the driver delivered");
    assert_eq!(committed_offset(&cluster), Some(1));
}

#[nervix_primitives::test]
async fn closing_an_acknowledged_group_commits_nothing_it_did_not_acknowledge() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut source = subscribed_source(&cluster, KafkaGroupCommit::EachAcknowledgedBatch).await;
    read_positions(&mut source).await;

    source
        .close()
        .await
        .assured("an acknowledged group has nothing outstanding to commit");
    assert_eq!(committed_offset(&cluster), None);
}

#[test]
fn only_no_ack_leaves_the_commits_to_the_driver() {
    let retry = ParsedRetryPolicy {
        backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(1),
    };
    let sequential = SourceAckPolicy::Sequential {
        timeout: Duration::from_secs(30),
        retry,
    };
    let parallel = SourceAckPolicy::Parallel {
        max_in_flight: NonZeroUsize::MIN,
        batch_timeout: Duration::from_millis(10),
        timeout: Duration::from_secs(30),
        retry,
    };

    assert_eq!(
        KafkaGroupCommit::for_acknowledgement(SourceAckPolicy::None),
        KafkaGroupCommit::DriverCadence
    );
    assert_eq!(
        KafkaGroupCommit::for_acknowledgement(sequential),
        KafkaGroupCommit::EachAcknowledgedBatch
    );
    assert_eq!(
        KafkaGroupCommit::for_acknowledgement(parallel),
        KafkaGroupCommit::EachAcknowledgedBatch
    );
}
