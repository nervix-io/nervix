//! Where the Kafka ingestor starts a partition after a rejection or without an offset of its own.
//!
//! Outside the layer order: a harness.
//! - **Owns.** The rejected-partition floors, how the client's reset policy is read, and
//!   regressions through the production ingestor against librdkafka's mock cluster: a consumer
//!   group that loses its rewound partition before the rejected record is read again, and
//!   domain-owned offsets that resume a partition, reset it at a new start point, or start it
//!   without a recorded offset.
//! - **Depends on.** The connector contract, librdkafka's mock broker and the boundary's
//!   thread-blocking lock and watch channel.
//! - **Must not know.** Runtime graph ownership, registry Models or cluster placement.

use std::time::Duration;

use async_trait::async_trait;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector as _, IngestorBatch, IngestorBatchRequest, IngestorConnector,
    IngestorMessage as _, IngestorReadCancellation, IngestorResume,
};
use nervix_models::{ClientConfigEntry, KafkaPartitionSchedule, KafkaTopicName, Timestamp};
use nervix_primitives::{
    sync::{Arc, blocking::Mutex, watch},
    time::{Instant, timeout},
};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer as _, RebalanceProtocol},
    mocking::MockCluster,
    producer::DefaultProducerContext,
    topic_partition_list::{Offset, TopicPartitionList},
};

use super::{
    KafkaDomainOffsetHost, KafkaDomainOffsetInitialization, KafkaDomainOffsetResult,
    KafkaDomainOffsetServices, KafkaDomainOffsetStart, KafkaGroupCommit, KafkaOffsetPosition,
    KafkaOffsetReset, KafkaTopicIngestor, KafkaTopicIngestorError, KafkaTopicIngestorOffsetMode,
    KafkaTopicIngestorPlan,
    commit_tests::{READ_DEADLINE, cluster, plan as group_plan, publish, read_positions},
    rejection::RejectionFloors,
};

const TOPIC: &str = "orders";

/// How long one read waits for a record before the test publishes another.
const PUBLISH_INTERVAL: Duration = Duration::from_secs(2);

fn position(partition: i32, offset: i64) -> KafkaOffsetPosition {
    KafkaOffsetPosition {
        topic: TOPIC.to_string(),
        partition,
        offset,
    }
}

fn entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn assignment_of(partitions: &[i32]) -> TopicPartitionList {
    let mut assignment = TopicPartitionList::new();
    for partition in partitions {
        assignment.add_partition(TOPIC, *partition);
    }
    assignment
}

fn start_of(assignment: &TopicPartitionList, partition: i32) -> Offset {
    assignment
        .find_partition(TOPIC, partition)
        .assured("the test assignment names the partition it asks about")
        .offset()
}

#[test]
fn a_partition_keeps_the_floor_of_its_earliest_rejection() {
    let mut floors = RejectionFloors::default();
    floors.record(&[position(0, 7), position(1, 3)]);
    floors.record(&[position(0, 9), position(1, 2)]);
    let mut assignment = assignment_of(&[0, 1]);

    floors
        .stage(&mut assignment, &RebalanceProtocol::Eager)
        .assured("a floor is a valid start offset");

    assert_eq!(start_of(&assignment, 0), Offset::Offset(7));
    assert_eq!(start_of(&assignment, 1), Offset::Offset(2));
}

#[test]
fn only_a_commit_past_the_rejected_record_confirms_its_floor() {
    let mut floors = RejectionFloors::default();
    floors.record(&[position(0, 7), position(1, 3)]);

    // A next offset of 7 commits the records before the rejected one, not the rejected record.
    floors.confirm(&[position(0, 7), position(1, 4)]);

    let mut assignment = assignment_of(&[0, 1]);
    floors
        .stage(&mut assignment, &RebalanceProtocol::Cooperative)
        .assured("a floor is a valid start offset");
    assert_eq!(start_of(&assignment, 0), Offset::Offset(7));
    assert_eq!(start_of(&assignment, 1), Offset::Invalid);
    floors.confirm(&[position(0, 8)]);
    assert!(floors.is_empty());
}

#[test]
fn an_eager_assignment_forgets_the_floors_of_partitions_another_member_took() {
    let mut floors = RejectionFloors::default();
    floors.record(&[position(0, 7), position(1, 3)]);
    let mut assignment = assignment_of(&[0]);

    floors
        .stage(&mut assignment, &RebalanceProtocol::Eager)
        .assured("a floor is a valid start offset");

    assert_eq!(start_of(&assignment, 0), Offset::Offset(7));
    let mut later = assignment_of(&[0, 1]);
    floors
        .stage(&mut later, &RebalanceProtocol::Eager)
        .assured("a floor is a valid start offset");
    assert_eq!(start_of(&later, 0), Offset::Offset(7));
    assert_eq!(start_of(&later, 1), Offset::Invalid);
}

#[test]
fn a_cooperative_assignment_keeps_the_floors_of_partitions_it_does_not_add() {
    let mut floors = RejectionFloors::default();
    floors.record(&[position(0, 7), position(1, 3)]);
    let mut added = assignment_of(&[0]);

    floors
        .stage(&mut added, &RebalanceProtocol::Cooperative)
        .assured("a floor is a valid start offset");

    let mut later = assignment_of(&[1]);
    floors
        .stage(&mut later, &RebalanceProtocol::Cooperative)
        .assured("a floor is a valid start offset");
    assert_eq!(start_of(&later, 1), Offset::Offset(3));
}

/// An ingestor in the test group whose client starts a partition the group committed nothing for at
/// the partition's end, as librdkafka's default reset does, and whose group session lapses six
/// seconds after its broker stops answering.
async fn latest_group_source(
    cluster: &MockCluster<'static, DefaultProducerContext>,
) -> KafkaTopicIngestor {
    let mut plan = group_plan(cluster, KafkaGroupCommit::EachAcknowledgedBatch);
    plan.config.retain(|entry| entry.key != "auto.offset.reset");
    plan.config.extend([
        entry("auto.offset.reset", "latest"),
        entry("session.timeout.ms", "6000"),
        entry("heartbeat.interval.ms", "500"),
    ]);
    let mut source = KafkaTopicIngestor::open(&plan, 0)
        .await
        .assured("librdkafka accepts the test consumer configuration");
    let resumed = source
        .resume()
        .await
        .assured("a consumer subscribes to a topic its broker serves");
    assert!(matches!(resumed, IngestorResume::Ready));
    source
}

/// Publishes a record at a time until `source`, which starts its partition at the partition's end,
/// reads one, and returns that record's position.
async fn read_once_assigned(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    source: &mut KafkaTopicIngestor,
) -> Vec<KafkaOffsetPosition> {
    let deadline = Instant::now()
        .checked_add(READ_DEADLINE)
        .assured("the read deadline fits the monotonic clock");
    let request = IngestorBatchRequest {
        max_messages: std::num::NonZeroUsize::MIN,
        batch_timeout: None,
    };
    loop {
        assert!(
            Instant::now() < deadline,
            "the group assigned the source its partition and it read a record within its deadline"
        );
        publish(cluster).await;
        let cancellation = IngestorReadCancellation::new();
        let read = timeout(PUBLISH_INTERVAL, source.next_batch(request, &cancellation)).await;
        if let Ok(Ok(IngestorBatch::Messages(messages))) = read {
            return messages
                .iter()
                .map(|message| message.position().clone())
                .collect();
        }
    }
}

#[nervix_primitives::test]
async fn a_rejection_retried_after_the_source_rejoins_its_group_is_read_again() {
    let cluster = cluster();
    let mut source = latest_group_source(&cluster).await;
    let rejected = read_once_assigned(&cluster, &mut source).await;

    // The host suspends and resumes an ingestor whose rejection failed, then retries the rejection
    // before it reads again. Rejoining the group revokes the partition and assigns it anew, and
    // the group committed nothing to resume it from.
    source
        .suspend()
        .await
        .assured("an acknowledged group leaves without a commit to make");
    let resumed = source
        .resume()
        .await
        .assured("a consumer subscribes to a topic its broker serves");
    assert!(matches!(resumed, IngestorResume::Ready));
    source
        .reject(&rejected)
        .await
        .assured("a rejection that cannot seek yet leaves its floor for the next assignment");

    assert_eq!(read_positions(&mut source).await, rejected);
}

#[nervix_primitives::test]
async fn a_rejected_record_is_read_again_after_an_outage_outlasts_the_group_session() {
    let cluster = cluster();
    let mut source = latest_group_source(&cluster).await;
    let rejected = read_once_assigned(&cluster, &mut source).await;
    cluster
        .broker_down(1)
        .assured("the mock cluster takes down a broker it has");
    source
        .reject(&rejected)
        .await
        .assured("the source rewinds the partition it is still assigned");

    // The ingestor keeps polling through the outage, which serves the group's revocation once the
    // session lapses and loses the rewind with the partition.
    let revocation_deadline = Instant::now()
        .checked_add(READ_DEADLINE)
        .assured("the outage deadline fits the monotonic clock");
    let request = IngestorBatchRequest {
        max_messages: std::num::NonZeroUsize::MIN,
        batch_timeout: None,
    };
    loop {
        let assignment = source
            .consumer
            .assignment()
            .assured("librdkafka reports a consumer's assignment");
        if assignment.count() == 0 {
            break;
        }
        assert!(
            Instant::now() < revocation_deadline,
            "the group revoked the partition once the session lapsed"
        );
        let cancellation = IngestorReadCancellation::new();
        let polled = timeout(PUBLISH_INTERVAL, source.next_batch(request, &cancellation)).await;
        if let Ok(Ok(IngestorBatch::Messages(messages))) = polled {
            panic!(
                "the source read {} records from a broker that is down",
                messages.len()
            );
        }
    }
    cluster
        .broker_up(1)
        .assured("the mock cluster brings back a broker it took down");

    // The group assigns the partition again only once the ingestor rejoins it, so the read again
    // has a deadline of its own from the moment the broker is back.
    let read_deadline = Instant::now()
        .checked_add(READ_DEADLINE)
        .assured("the read deadline fits the monotonic clock");
    let read = loop {
        assert!(
            Instant::now() < read_deadline,
            "the source read the rejected record again within its deadline"
        );
        let cancellation = IngestorReadCancellation::new();
        let polled = timeout(PUBLISH_INTERVAL, source.next_batch(request, &cancellation)).await;
        if let Ok(Ok(IngestorBatch::Messages(messages))) = polled {
            break messages
                .iter()
                .map(|message| message.position().clone())
                .collect::<Vec<_>>();
        }
    };
    assert_eq!(read, rejected);
}

#[nervix_primitives::test]
async fn a_commit_past_the_rejected_record_lets_the_group_resume_from_its_committed_offset() {
    let cluster = cluster();
    let mut source = latest_group_source(&cluster).await;
    let rejected = read_once_assigned(&cluster, &mut source).await;
    source
        .reject(&rejected)
        .await
        .assured("the source rewinds the partition it is still assigned");
    assert_eq!(read_positions(&mut source).await, rejected);

    source
        .acknowledge(&rejected)
        .await
        .assured("the group commits the record read again");

    assert!(source.consumer.context().floors().is_empty());
    publish(&cluster).await;
    source
        .suspend()
        .await
        .assured("an acknowledged group leaves without a commit to make");
    let resumed = source
        .resume()
        .await
        .assured("a consumer subscribes to a topic its broker serves");
    assert!(matches!(resumed, IngestorResume::Ready));
    let next = read_positions(&mut source).await;
    let mut expected = Vec::new();
    for rejected in &rejected {
        let next = rejected
            .offset
            .checked_add(1)
            .assured("a test partition holds only a few records");
        expected.push(position(rejected.partition, next));
    }
    assert_eq!(next, expected);
}

/// Where the test domain's offsets start the instance.
#[derive(Clone, Copy)]
enum ScriptedStart {
    /// Nothing recorded for the test topic; a paced domain also names its current time.
    Unrecorded { domain_time: Option<Timestamp> },
    /// The next offset the domain recorded for the topic's partition.
    Recorded { next_offset: i64 },
    /// A new start point at a timestamp.
    At(Timestamp),
}

impl ScriptedStart {
    fn start(self) -> KafkaDomainOffsetStart {
        match self {
            Self::Unrecorded { domain_time } => KafkaDomainOffsetStart::Resume {
                positions: Vec::new(),
                missing_partition_timestamp: domain_time,
            },
            Self::Recorded { next_offset } => KafkaDomainOffsetStart::Resume {
                positions: vec![position(0, next_offset)],
                missing_partition_timestamp: None,
            },
            Self::At(timestamp) => KafkaDomainOffsetStart::At(timestamp),
        }
    }
}

/// Domain offsets that start the test topic as scripted and assign its one partition to instance
/// zero, recording the starts the ingestor stores and the offsets it resets.
struct ScriptedDomainOffsets {
    start: ScriptedStart,
    stored: Arc<Mutex<Vec<KafkaOffsetPosition>>>,
    resets: Arc<Mutex<Vec<KafkaOffsetPosition>>>,
}

#[async_trait]
impl KafkaDomainOffsetServices for ScriptedDomainOffsets {
    fn generation(&self) -> Option<u64> {
        Some(1)
    }

    async fn initialization(
        &self,
        _partitions: &[i32],
    ) -> KafkaDomainOffsetResult<KafkaDomainOffsetInitialization> {
        Ok(KafkaDomainOffsetInitialization {
            generation: 1,
            start: self.start.start(),
            schedule: Some(KafkaPartitionSchedule {
                observed_partitions: vec![0],
                rebalance_epoch: 0,
                instance_assignments: vec![vec![0]],
            }),
        })
    }

    async fn reset(&self, positions: Vec<KafkaOffsetPosition>) -> KafkaDomainOffsetResult<()> {
        self.resets.lock().extend(positions);
        Ok(())
    }

    async fn commit(&self, position: KafkaOffsetPosition) -> KafkaDomainOffsetResult<()> {
        self.stored.lock().push(position);
        Ok(())
    }
}

/// A domain-offset ingestor, the starts it stores in its domain and the offsets it resets there.
struct DomainSource {
    source: KafkaTopicIngestor,
    stored: Arc<Mutex<Vec<KafkaOffsetPosition>>>,
    resets: Arc<Mutex<Vec<KafkaOffsetPosition>>>,
    /// Held so the ingestor's partition watch stays open.
    _partition_watch: watch::Sender<u64>,
}

/// A domain-offset ingestor whose domain starts it as `start` says and whose client resets a
/// partition without an offset as `reset` says.
async fn domain_source(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    reset: &str,
    start: ScriptedStart,
) -> DomainSource {
    let stored = Arc::new(Mutex::new(Vec::new()));
    let resets = Arc::new(Mutex::new(Vec::new()));
    let (partition_watch, rebalanced) = watch::channel(0);
    let plan = KafkaTopicIngestorPlan {
        executor: nervix_execution::Executor::default(),
        config: vec![
            entry("bootstrap.servers", &cluster.bootstrap_servers()),
            entry("auto.offset.reset", reset),
        ],
        topic: KafkaTopicName::parse(TOPIC).assured("the test topic is a valid Kafka topic"),
        offset_mode: KafkaTopicIngestorOffsetMode::Domain {
            group_id: "orders-domain".to_string(),
            offsets: KafkaDomainOffsetHost::new(ScriptedDomainOffsets {
                start,
                stored: Arc::clone(&stored),
                resets: Arc::clone(&resets),
            }),
            rebalance: rebalanced,
        },
    };
    let source = KafkaTopicIngestor::open(&plan, 0)
        .await
        .assured("librdkafka accepts the test consumer configuration");
    DomainSource {
        source,
        stored,
        resets,
        _partition_watch: partition_watch,
    }
}

#[nervix_primitives::test]
async fn an_unrecorded_partition_starts_at_its_end_when_the_client_resets_to_latest() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut domain = domain_source(
        &cluster,
        "latest",
        ScriptedStart::Unrecorded { domain_time: None },
    )
    .await;

    let resumed = domain
        .source
        .resume()
        .await
        .assured("the source takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    assert_eq!(*domain.stored.lock(), vec![position(0, 1)]);
    assert!(
        domain.resets.lock().is_empty(),
        "a resume replaces no domain offsets"
    );
    publish(&cluster).await;
    assert_eq!(
        read_positions(&mut domain.source).await,
        vec![position(0, 2)]
    );
}

#[nervix_primitives::test]
async fn an_unrecorded_partition_starts_at_its_earliest_record_when_the_client_resets_to_earliest()
{
    let cluster = cluster();
    publish(&cluster).await;
    let mut domain = domain_source(
        &cluster,
        "earliest",
        ScriptedStart::Unrecorded { domain_time: None },
    )
    .await;

    let resumed = domain
        .source
        .resume()
        .await
        .assured("the source takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    assert_eq!(*domain.stored.lock(), vec![position(0, 0)]);
    assert_eq!(
        read_positions(&mut domain.source).await,
        vec![position(0, 1)]
    );
}

#[nervix_primitives::test]
async fn a_paced_partition_without_a_record_after_the_domain_time_starts_where_its_end_stood() {
    let cluster = cluster();
    publish(&cluster).await;
    let after_every_record = Timestamp::from_unix_nanos(i64::MAX / 2);
    let mut domain = domain_source(
        &cluster,
        "earliest",
        ScriptedStart::Unrecorded {
            domain_time: Some(after_every_record),
        },
    )
    .await;

    let resumed = domain
        .source
        .resume()
        .await
        .assured("the source takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    assert_eq!(*domain.stored.lock(), vec![position(0, 1)]);
    publish(&cluster).await;
    assert_eq!(
        read_positions(&mut domain.source).await,
        vec![position(0, 2)]
    );
}

#[nervix_primitives::test]
async fn a_recorded_partition_resumes_from_its_domain_offset() {
    let cluster = cluster();
    publish(&cluster).await;
    publish(&cluster).await;
    let mut domain = domain_source(
        &cluster,
        "latest",
        ScriptedStart::Recorded { next_offset: 1 },
    )
    .await;

    let resumed = domain
        .source
        .resume()
        .await
        .assured("the source takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    assert!(
        domain.stored.lock().is_empty(),
        "a partition with a domain offset records no start"
    );
    assert!(
        domain.resets.lock().is_empty(),
        "a resume replaces no domain offsets"
    );
    assert_eq!(
        read_positions(&mut domain.source).await,
        vec![position(0, 2)]
    );
}

#[nervix_primitives::test]
async fn a_domain_offset_past_what_its_partition_holds_resumes_at_the_partition_end() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut domain = domain_source(
        &cluster,
        "earliest",
        ScriptedStart::Recorded { next_offset: 7 },
    )
    .await;

    let resumed = domain
        .source
        .resume()
        .await
        .assured("the source takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    publish(&cluster).await;
    assert_eq!(
        read_positions(&mut domain.source).await,
        vec![position(0, 2)]
    );
}

#[nervix_primitives::test]
async fn a_new_start_point_replaces_the_domain_offset_of_every_partition() {
    let cluster = cluster();
    publish(&cluster).await;
    // librdkafka's mock broker finds no record by timestamp, so the start point places the
    // partition at its end as it stood before the lookup.
    let start = Timestamp::from_unix_nanos(0);
    let mut domain = domain_source(&cluster, "earliest", ScriptedStart::At(start)).await;

    let resumed = domain
        .source
        .resume()
        .await
        .assured("the source takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    assert_eq!(*domain.resets.lock(), vec![position(0, 1)]);
    assert!(
        domain.stored.lock().is_empty(),
        "a new start point records its starts as one reset"
    );
    publish(&cluster).await;
    assert_eq!(
        read_positions(&mut domain.source).await,
        vec![position(0, 2)]
    );
}

#[nervix_primitives::test]
async fn a_client_that_refuses_resets_cannot_start_an_unrecorded_partition() {
    let cluster = cluster();
    let mut domain = domain_source(
        &cluster,
        "error",
        ScriptedStart::Unrecorded { domain_time: None },
    )
    .await;

    let resumed = domain.source.resume().await;

    let Err(report) = resumed else {
        panic!("a partition without an offset has no start under a refusing reset");
    };
    let failure = report
        .downcast_ref::<KafkaTopicIngestorError>()
        .assured("the resume failure names the partition it could not start");
    assert!(
        matches!(
            failure,
            KafkaTopicIngestorError::ResetRefused { topic, partition: 0 } if topic == TOPIC
        ),
        "unexpected Kafka source failure {failure:?}"
    );
    assert!(domain.stored.lock().is_empty());
}

#[test]
fn a_partition_without_an_offset_starts_where_librdkafka_resets_it() {
    let mut config = ClientConfig::new();
    let undeclared = KafkaOffsetReset::of(&config).assured("an undeclared policy has a default");
    assert_eq!(undeclared, KafkaOffsetReset::Latest);

    // librdkafka reads a declared policy without regard to case or leading spaces.
    let declared = [
        (" Earliest", KafkaOffsetReset::Earliest),
        ("beginning", KafkaOffsetReset::Earliest),
        ("END", KafkaOffsetReset::Latest),
        ("error", KafkaOffsetReset::Refuse),
    ];
    for (value, expected) in declared {
        config.set("auto.offset.reset", value);
        let policy = KafkaOffsetReset::of(&config).assured("librdkafka accepts the test policies");
        assert_eq!(policy, expected, "auto.offset.reset = {value:?}");
    }

    config.set("auto.offset.reset", "newest");
    let refused = KafkaOffsetReset::of(&config);
    let Err(report) = refused else {
        panic!("librdkafka refuses a policy it does not name");
    };
    assert!(matches!(
        report.current_context(),
        KafkaTopicIngestorError::Initialize
    ));
}
