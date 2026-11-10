//! Where the Kafka ingestor starts a partition after a rejection or without an offset of its own.
//!
//! Outside the layer order: a harness.
//! - **Owns.** The rejected-partition floors, the position each offset mode gives its consumer, and
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
use nervix_models::{
    ClientConfigEntry, IngestStartPosition, KafkaPartitionSchedule, KafkaTopicName, Timestamp,
};
use nervix_primitives::{
    sync::{Arc, blocking::Mutex, watch},
    time::{Instant, timeout},
};
use rdkafka::{
    consumer::{Consumer as _, RebalanceProtocol},
    mocking::MockCluster,
    producer::DefaultProducerContext,
    topic_partition_list::{Offset, TopicPartitionList},
};

use super::{
    DomainStart, KafkaDomainOffsetHost, KafkaDomainOffsetInitialization, KafkaDomainOffsetResult,
    KafkaDomainOffsetServices, KafkaDomainOffsetStart, KafkaGroupCommit, KafkaOffsetPosition,
    KafkaTopicIngestor, KafkaTopicIngestorError, KafkaTopicIngestorOffsetMode,
    KafkaTopicIngestorPlan, ScheduledPartitions,
    commit_tests::{READ_DEADLINE, cluster, plan as group_plan, publish, read_positions},
    rejection::RejectionFloors,
};

const TOPIC: &str = "orders";

#[nervix_primitives::test]
async fn an_explicit_start_resolves_the_whole_topic_before_any_instance_polls() {
    let broker = MockCluster::new(1).assured("the mock cluster has one broker");
    broker
        .create_topic(TOPIC, 3, 1)
        .assured("the test topic has three partitions");
    let domain = domain_ingestor(
        &broker,
        ScriptedStart::At(Timestamp::from_unix_nanos(i64::MAX / 2)),
    )
    .await;
    let offsets = domain
        .ingestor
        .control
        .run(&domain.ingestor.consumer, |consumer, budget| {
            let scheduled = ScheduledPartitions {
                topic: TOPIC,
                partitions: &[0, 1, 2],
                assigned: std::collections::BTreeSet::from([0]),
            };
            let start = DomainStart::resolve(
                consumer,
                &scheduled,
                KafkaDomainOffsetStart::At(Timestamp::from_unix_nanos(i64::MAX / 2)),
                budget,
            )?;
            Ok(start.offsets().clone())
        })
        .await
        .assured("the broker resolves a future start to each empty partition's end");
    assert_eq!(
        offsets,
        std::collections::BTreeMap::from([(0, 0), (1, 0), (2, 0)])
    );
}

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

/// An ingestor in the test group declared `START AT LATEST`, so a partition the group committed
/// nothing for starts at its end, and whose group session lapses six seconds after its broker stops
/// answering.
async fn latest_group_ingestor(
    cluster: &MockCluster<'static, DefaultProducerContext>,
) -> KafkaTopicIngestor {
    let mut plan = group_plan(cluster, KafkaGroupCommit::EachAcknowledgedBatch);
    let KafkaTopicIngestorOffsetMode::ConsumerGroup { start_at, .. } = &mut plan.offset_mode else {
        panic!("the test plan reads through the test group");
    };
    *start_at = IngestStartPosition::Latest;
    plan.config.extend([
        entry("session.timeout.ms", "6000"),
        entry("heartbeat.interval.ms", "500"),
    ]);
    let mut ingestor = KafkaTopicIngestor::open(&plan, 0)
        .await
        .assured("librdkafka accepts the test consumer configuration");
    let resumed = ingestor
        .resume()
        .await
        .assured("a consumer subscribes to a topic its broker serves");
    assert!(matches!(resumed, IngestorResume::Ready));
    ingestor
}

/// Publishes a record at a time until `ingestor`, which starts its partition at the partition's end,
/// reads one, and returns that record's position.
async fn read_once_assigned(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    ingestor: &mut KafkaTopicIngestor,
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
            "the group assigned the ingestor its partition and it read a record within its \
             deadline"
        );
        publish(cluster).await;
        let cancellation = IngestorReadCancellation::new();
        let read = timeout(
            PUBLISH_INTERVAL,
            ingestor.next_batch(request, &cancellation),
        )
        .await;
        if let Ok(Ok(IngestorBatch::Messages(messages))) = read {
            return messages
                .iter()
                .map(|message| message.position().clone())
                .collect();
        }
    }
}

#[nervix_primitives::test]
async fn a_rejection_retried_after_the_ingestor_rejoins_its_group_is_read_again() {
    let cluster = cluster();
    let mut ingestor = latest_group_ingestor(&cluster).await;
    let rejected = read_once_assigned(&cluster, &mut ingestor).await;

    // The host suspends and resumes an ingestor whose rejection failed, then retries the rejection
    // before it reads again. Rejoining the group revokes the partition and assigns it anew, and
    // the group committed nothing to resume it from.
    ingestor
        .suspend()
        .await
        .assured("an acknowledged group leaves without a commit to make");
    let resumed = ingestor
        .resume()
        .await
        .assured("a consumer subscribes to a topic its broker serves");
    assert!(matches!(resumed, IngestorResume::Ready));
    ingestor
        .reject(&rejected)
        .await
        .assured("a rejection that cannot seek yet leaves its floor for the next assignment");

    assert_eq!(read_positions(&mut ingestor).await, rejected);
}

#[nervix_primitives::test]
async fn a_rejected_record_is_read_again_after_an_outage_outlasts_the_group_session() {
    let cluster = cluster();
    let mut ingestor = latest_group_ingestor(&cluster).await;
    let rejected = read_once_assigned(&cluster, &mut ingestor).await;
    cluster
        .broker_down(1)
        .assured("the mock cluster takes down a broker it has");
    ingestor
        .reject(&rejected)
        .await
        .assured("the ingestor rewinds the partition it is still assigned");

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
        let assignment = ingestor
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
        let polled = timeout(
            PUBLISH_INTERVAL,
            ingestor.next_batch(request, &cancellation),
        )
        .await;
        if let Ok(Ok(IngestorBatch::Messages(messages))) = polled {
            panic!(
                "the ingestor read {} records from a broker that is down",
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
            "the ingestor read the rejected record again within its deadline"
        );
        let cancellation = IngestorReadCancellation::new();
        let polled = timeout(
            PUBLISH_INTERVAL,
            ingestor.next_batch(request, &cancellation),
        )
        .await;
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
    let mut ingestor = latest_group_ingestor(&cluster).await;
    let rejected = read_once_assigned(&cluster, &mut ingestor).await;
    ingestor
        .reject(&rejected)
        .await
        .assured("the ingestor rewinds the partition it is still assigned");
    assert_eq!(read_positions(&mut ingestor).await, rejected);

    ingestor
        .acknowledge(&rejected)
        .await
        .assured("the group commits the record read again");

    assert!(ingestor.consumer.context().floors().is_empty());
    publish(&cluster).await;
    ingestor
        .suspend()
        .await
        .assured("an acknowledged group leaves without a commit to make");
    let resumed = ingestor
        .resume()
        .await
        .assured("a consumer subscribes to a topic its broker serves");
    assert!(matches!(resumed, IngestorResume::Ready));
    let next = read_positions(&mut ingestor).await;
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
    /// Another initializer accepts this start while this instance resolves its own lookup.
    ConcurrentAt {
        timestamp: Timestamp,
        canonical_next_offset: i64,
    },
    /// A second explicit generation appears after the first start was accepted.
    ChangedGenerationAt(Timestamp),
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
            Self::At(timestamp)
            | Self::ConcurrentAt { timestamp, .. }
            | Self::ChangedGenerationAt(timestamp) => KafkaDomainOffsetStart::At(timestamp),
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
        if matches!(self.start, ScriptedStart::ChangedGenerationAt(_))
            && !self.resets.lock().is_empty()
        {
            return Some(2);
        }
        Some(1)
    }

    async fn initialization(
        &self,
        _partitions: &[i32],
    ) -> KafkaDomainOffsetResult<KafkaDomainOffsetInitialization> {
        let accepted = self.resets.lock();
        let mut generation = 1;
        let start = if accepted.is_empty() {
            self.start.start()
        } else if let ScriptedStart::ChangedGenerationAt(timestamp) = self.start {
            generation = 2;
            KafkaDomainOffsetStart::At(timestamp)
        } else {
            let positions = match self.start {
                ScriptedStart::ConcurrentAt {
                    canonical_next_offset,
                    ..
                } => vec![position(0, canonical_next_offset)],
                _ => accepted.clone(),
            };
            KafkaDomainOffsetStart::Resume {
                positions,
                missing_partition_timestamp: None,
            }
        };
        Ok(KafkaDomainOffsetInitialization {
            generation,
            start,
            schedule: Some(KafkaPartitionSchedule {
                observed_partitions: vec![0],
                rebalance_epoch: 0,
                instance_assignments: vec![vec![0]],
            }),
        })
    }

    async fn reset(
        &self,
        generation: u64,
        positions: Vec<KafkaOffsetPosition>,
    ) -> KafkaDomainOffsetResult<()> {
        assert_eq!(generation, 1);
        self.resets.lock().extend(positions);
        Ok(())
    }

    async fn commit(&self, position: KafkaOffsetPosition) -> KafkaDomainOffsetResult<()> {
        self.stored.lock().push(position);
        Ok(())
    }
}

/// A domain-offset ingestor, the starts it stores in its domain and the offsets it resets there.
struct DomainIngestor {
    ingestor: KafkaTopicIngestor,
    stored: Arc<Mutex<Vec<KafkaOffsetPosition>>>,
    resets: Arc<Mutex<Vec<KafkaOffsetPosition>>>,
    /// Held so the ingestor's partition watch stays open.
    _partition_watch: watch::Sender<u64>,
}

/// A domain-offset ingestor whose domain starts it as `start` says.
async fn domain_ingestor(
    cluster: &MockCluster<'static, DefaultProducerContext>,
    start: ScriptedStart,
) -> DomainIngestor {
    let stored = Arc::new(Mutex::new(Vec::new()));
    let resets = Arc::new(Mutex::new(Vec::new()));
    let (partition_watch, rebalanced) = watch::channel(0);
    let plan = KafkaTopicIngestorPlan {
        settings: crate::KafkaTopicIngestorSettings::default(),
        executor: nervix_execution::Executor::default(),
        config: vec![entry("bootstrap.servers", &cluster.bootstrap_servers())],
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
    let ingestor = KafkaTopicIngestor::open(&plan, 0)
        .await
        .assured("librdkafka accepts the test consumer configuration");
    DomainIngestor {
        ingestor,
        stored,
        resets,
        _partition_watch: partition_watch,
    }
}

#[nervix_primitives::test]
async fn an_unrecorded_partition_starts_at_its_earliest_record() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut domain =
        domain_ingestor(&cluster, ScriptedStart::Unrecorded { domain_time: None }).await;

    let resumed = domain
        .ingestor
        .resume()
        .await
        .assured("the ingestor takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    assert_eq!(*domain.stored.lock(), vec![position(0, 0)]);
    assert_eq!(
        read_positions(&mut domain.ingestor).await,
        vec![position(0, 1)]
    );
}

#[nervix_primitives::test]
async fn a_paced_partition_without_a_record_after_the_domain_time_starts_where_its_end_stood() {
    let cluster = cluster();
    publish(&cluster).await;
    let after_every_record = Timestamp::from_unix_nanos(i64::MAX / 2);
    let mut domain = domain_ingestor(
        &cluster,
        ScriptedStart::Unrecorded {
            domain_time: Some(after_every_record),
        },
    )
    .await;

    let resumed = domain
        .ingestor
        .resume()
        .await
        .assured("the ingestor takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    assert_eq!(*domain.stored.lock(), vec![position(0, 1)]);
    publish(&cluster).await;
    assert_eq!(
        read_positions(&mut domain.ingestor).await,
        vec![position(0, 2)]
    );
}

#[nervix_primitives::test]
async fn a_recorded_partition_resumes_from_its_domain_offset() {
    let cluster = cluster();
    publish(&cluster).await;
    publish(&cluster).await;
    let mut domain = domain_ingestor(&cluster, ScriptedStart::Recorded { next_offset: 1 }).await;

    let resumed = domain
        .ingestor
        .resume()
        .await
        .assured("the ingestor takes up the partition its schedule assigns it");

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
        read_positions(&mut domain.ingestor).await,
        vec![position(0, 2)]
    );
}

#[nervix_primitives::test]
async fn a_domain_offset_past_what_its_partition_holds_resumes_at_the_partition_end() {
    let cluster = cluster();
    publish(&cluster).await;
    let mut domain = domain_ingestor(&cluster, ScriptedStart::Recorded { next_offset: 7 }).await;

    let resumed = domain
        .ingestor
        .resume()
        .await
        .assured("the ingestor takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    publish(&cluster).await;
    assert_eq!(
        read_positions(&mut domain.ingestor).await,
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
    let mut domain = domain_ingestor(&cluster, ScriptedStart::At(start)).await;

    let resumed = domain
        .ingestor
        .resume()
        .await
        .assured("the ingestor takes up the partition its schedule assigns it");

    assert!(matches!(resumed, IngestorResume::Ready));
    assert_eq!(*domain.resets.lock(), vec![position(0, 1)]);
    assert!(
        domain.stored.lock().is_empty(),
        "a new start point records its starts as one reset"
    );
    publish(&cluster).await;
    assert_eq!(
        read_positions(&mut domain.ingestor).await,
        vec![position(0, 2)]
    );
}

#[nervix_primitives::test]
async fn a_concurrent_explicit_start_uses_the_canonical_partition_position() {
    let cluster = cluster();
    publish(&cluster).await;
    // Another initializer chose offset 0 before this record arrived. The mock broker's later
    // timestamp lookup finds no timestamp, so this instance resolves the partition's end, 1.
    let mut domain = domain_ingestor(
        &cluster,
        ScriptedStart::ConcurrentAt {
            timestamp: Timestamp::from_unix_nanos(0),
            canonical_next_offset: 0,
        },
    )
    .await;
    let resumed = domain
        .ingestor
        .resume()
        .await
        .assured("the ingestor takes its partition up after the generation start is accepted");
    assert!(matches!(resumed, IngestorResume::Ready));
    assert_eq!(*domain.resets.lock(), vec![position(0, 1)]);
    publish(&cluster).await;
    assert_eq!(
        read_positions(&mut domain.ingestor).await,
        vec![position(0, 1)]
    );
    assert_eq!(
        read_positions(&mut domain.ingestor).await,
        vec![position(0, 2)]
    );
}

#[nervix_primitives::test]
async fn a_new_generation_during_start_acceptance_requires_another_resume() {
    let cluster = cluster();
    let mut domain = domain_ingestor(
        &cluster,
        ScriptedStart::ChangedGenerationAt(Timestamp::from_unix_nanos(0)),
    )
    .await;
    let failure = domain
        .ingestor
        .resume()
        .await
        .err()
        .assured("a changed start ends this resume attempt before polling");
    assert!(matches!(
        failure.downcast_ref::<KafkaTopicIngestorError>(),
        Some(KafkaTopicIngestorError::UnresolvedDomainStart {
            accepted_generation: 1,
            observed_generation: 2,
        })
    ));
    assert_eq!(*domain.resets.lock(), vec![position(0, 0)]);
}

#[test]
fn a_consumer_group_starts_where_its_statement_declares() {
    let entries = [entry("bootstrap.servers", "127.0.0.1:9092")];
    for (start_at, reset) in [
        (IngestStartPosition::Earliest, "earliest"),
        (IngestStartPosition::Latest, "latest"),
    ] {
        let mode = KafkaTopicIngestorOffsetMode::ConsumerGroup {
            group_id: "readers".to_string(),
            commit: KafkaGroupCommit::EachAcknowledgedBatch,
            start_at,
        };

        let config = mode.consumer_config(&entries);

        assert_eq!(config.get("auto.offset.reset"), Some(reset));
        assert_eq!(config.get("group.id"), Some("readers"));
        assert_eq!(config.get("bootstrap.servers"), Some("127.0.0.1:9092"));
    }
}

#[test]
fn a_domain_offset_consumer_past_what_a_partition_retains_moves_to_the_earliest_record() {
    let (_partition_watch, rebalanced) = watch::channel(0);
    let mode = KafkaTopicIngestorOffsetMode::Domain {
        group_id: "orders-domain".to_string(),
        offsets: KafkaDomainOffsetHost::new(ScriptedDomainOffsets {
            start: ScriptedStart::Unrecorded { domain_time: None },
            stored: Arc::new(Mutex::new(Vec::new())),
            resets: Arc::new(Mutex::new(Vec::new())),
        }),
        rebalance: rebalanced,
    };

    let config = mode.consumer_config(&[entry("bootstrap.servers", "127.0.0.1:9092")]);

    assert_eq!(config.get("auto.offset.reset"), Some("earliest"));
    assert_eq!(config.get("group.id"), Some("orders-domain"));
}
