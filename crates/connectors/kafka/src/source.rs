//! Kafka source transport and host-owned domain-offset services.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Kafka consumer configuration, polling, headers and metadata, partition assignment,
//!   offset acknowledgement, partition inspection, and the opaque services through which a source
//!   reads and advances host-owned domain offsets.
//! - **Depends on.** The connector contract, Kafka vocabulary values, `error-stack`, Tokio, and
//!   `rust-rdkafka`.
//! - **Must not know.** Runtime offset-state types, domain execution maps, relays, branches,
//!   schedules outside the typed Kafka partition schedule, or registry state.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    time::Duration,
};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, SourceAckPolicy, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceReadCancellation,
    SourceResult, SourceResume,
};
use nervix_models::{ClientConfigEntry, KafkaPartitionSchedule, KafkaTopicName, Timestamp};
use nervix_primitives::{
    sync::{Arc, watch},
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};
use nervix_recovery::Discarded as _;
use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::{KafkaError, KafkaResult, RDKafkaErrorCode},
    message::{Headers, Message as KafkaMessage},
    topic_partition_list::{Offset, TopicPartitionList},
};
use thiserror::Error;
use tracing::{debug, warn};

const KAFKA: &str = "kafka";
const DOMAIN_ASSIGNMENT_RETRY: Duration = Duration::from_millis(100);

/// How long a consumer-group commit waits for its coordinator's answer.
///
/// librdkafka keeps retrying a commit it could not send for as long as the group has no reachable
/// coordinator, so the source stops waiting at this deadline and the commit fails. The commit stays
/// the instance's one commit in flight, which the next commit waits for first.
const COMMIT_ANSWER_DEADLINE: Duration = Duration::from_secs(30);

/// The next unread offset for one Kafka topic partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaOffsetPosition {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Where a domain-owned Kafka source resolves its next assignment from.
pub enum KafkaDomainOffsetStart {
    Resume {
        positions: Vec<KafkaOffsetPosition>,
        missing_partition_timestamp: Option<Timestamp>,
    },
    At(Timestamp),
}

/// Host-owned state needed to initialize one domain-offset source instance.
pub struct KafkaDomainOffsetInitialization {
    pub generation: u64,
    pub start: KafkaDomainOffsetStart,
    pub schedule: Option<KafkaPartitionSchedule>,
}

/// Why host-owned Kafka offset state could not serve the source connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum KafkaDomainOffsetError {
    #[error("failed to read host-owned Kafka offset state")]
    Read,
    #[error("failed to replace host-owned Kafka offsets")]
    Reset,
    #[error("failed to commit a host-owned Kafka offset")]
    Commit,
}

pub type KafkaDomainOffsetResult<T> = Result<T, Report<KafkaDomainOffsetError>>;

/// Runtime services used only by Kafka's domain-owned offset mode.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained host service is invoked by each admitted driver operation"
    )
)]
pub trait KafkaDomainOffsetServices: Send + Sync + 'static {
    fn generation(&self) -> Option<u64>;

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a resumed Kafka driver resolves its initial assigned offsets before \
                      consuming records"
        )
    )]
    async fn initialization(
        &self,
        partitions: &[i32],
    ) -> KafkaDomainOffsetResult<KafkaDomainOffsetInitialization>;

    async fn reset(&self, positions: Vec<KafkaOffsetPosition>) -> KafkaDomainOffsetResult<()>;

    async fn commit(&self, position: KafkaOffsetPosition) -> KafkaDomainOffsetResult<()>;
}

struct KafkaDomainOffsetHostInner {
    services: Box<dyn KafkaDomainOffsetServices>,
}

/// Opaque access to host-owned replicated Kafka offset state.
#[derive(Clone)]
pub struct KafkaDomainOffsetHost {
    inner: Arc<KafkaDomainOffsetHostInner>,
}

impl KafkaDomainOffsetHost {
    pub fn new(services: impl KafkaDomainOffsetServices) -> Self {
        Self {
            inner: Arc::new(KafkaDomainOffsetHostInner {
                services: Box::new(services),
            }),
        }
    }

    pub fn generation(&self) -> Option<u64> {
        self.inner.services.generation()
    }

    pub async fn initialization(
        &self,
        partitions: &[i32],
    ) -> KafkaDomainOffsetResult<KafkaDomainOffsetInitialization> {
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "Kafka resume installs its exact domain-offset placement before polling; the source \
             retains the resulting position",
            self.inner.services.initialization(partitions)
        )
        .await
    }

    pub async fn reset(&self, positions: Vec<KafkaOffsetPosition>) -> KafkaDomainOffsetResult<()> {
        self.inner.services.reset(positions).await
    }

    pub async fn commit(&self, position: KafkaOffsetPosition) -> KafkaDomainOffsetResult<()> {
        self.inner.services.commit(position).await
    }
}

/// How one Kafka source records the offsets it has accepted.
#[derive(Clone)]
pub enum KafkaSourceOffsetMode {
    ConsumerGroup {
        group_id: String,
        commit: KafkaGroupCommit,
    },
    Domain {
        group_id: String,
        offsets: KafkaDomainOffsetHost,
        rebalance: watch::Receiver<u64>,
    },
}

impl KafkaSourceOffsetMode {
    /// Whether the driver commits the positions it delivers on its own cadence.
    fn driver_commits(&self) -> bool {
        matches!(
            self,
            Self::ConsumerGroup {
                commit: KafkaGroupCommit::DriverCadence,
                ..
            }
        )
    }
}

/// Who commits a consumer group's offsets, which the ingestor's acknowledgement mode decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaGroupCommit {
    /// `MODE NO ACK`: the driver commits the positions it delivered on its own cadence,
    /// `auto.commit.interval.ms`, so acknowledging them confirms nothing on the transport.
    /// Leaving the group commits what that cadence has not committed yet.
    DriverCadence,
    /// An `ACK` mode: the source commits each acknowledged batch's positions and acknowledging
    /// returns once the group's coordinator answered the commit.
    EachAcknowledgedBatch,
}

impl KafkaGroupCommit {
    /// Who commits under `acknowledgement`.
    pub fn for_acknowledgement(acknowledgement: SourceAckPolicy) -> Self {
        match acknowledgement {
            SourceAckPolicy::None => Self::DriverCadence,
            SourceAckPolicy::Sequential { .. } | SourceAckPolicy::Parallel { .. } => {
                Self::EachAcknowledgedBatch
            }
        }
    }
}

/// The complete connector-owned plan for opening Kafka source instances.
#[derive(Clone)]
pub struct KafkaSourcePlan {
    pub config: Vec<ClientConfigEntry>,
    pub topic: KafkaTopicName,
    pub offset_mode: KafkaSourceOffsetMode,
}

#[derive(Debug, Error)]
pub enum KafkaSourceError {
    #[error("failed to initialize Kafka consumer")]
    Initialize,
    #[error("failed to subscribe to Kafka topic '{topic}'")]
    Subscribe { topic: String },
    #[error("failed to build Kafka offset commit for topic '{topic}' partition {partition}")]
    BuildOffsetCommit { topic: String, partition: i32 },
    #[error("Kafka did not commit the offsets of consumer group '{group}'")]
    CommitOffset { group: String },
    #[error("Kafka offset commit task for consumer group '{group}' failed")]
    CommitTask { group: String },
    #[error(
        "Kafka consumer group '{group}' did not answer an offset commit within {}",
        humantime::format_duration(*deadline)
    )]
    CommitUnanswered { group: String, deadline: Duration },
    #[error("failed to build Kafka assignment for topic '{topic}' partition {partition}")]
    BuildAssignment { topic: String, partition: i32 },
    #[error("failed to assign Kafka partitions for topic '{topic}'")]
    Assign { topic: String },
    #[error("failed to clear Kafka partition assignment for topic '{topic}'")]
    Unassign { topic: String },
    #[error("failed to fetch Kafka metadata for topic '{topic}'")]
    FetchMetadata { topic: String },
    #[error("Kafka partition inspection task failed")]
    InspectPartitions,
    #[error("Kafka returned no metadata for topic '{topic}'")]
    MissingMetadata { topic: String },
    #[error("failed to build Kafka timestamp query for topic '{topic}' partition {partition}")]
    BuildTimestampQuery { topic: String, partition: i32 },
    #[error("failed to resolve Kafka offsets by timestamp for topic '{topic}'")]
    ResolveTimestampOffsets { topic: String },
    #[error("failed to fetch Kafka watermarks for topic '{topic}' partition {partition}")]
    FetchWatermarks { topic: String, partition: i32 },
    #[error("unsupported Kafka domain offset for topic '{topic}' partition {partition}")]
    UnsupportedDomainOffset { topic: String, partition: i32 },
    #[error("failed to seek Kafka topic '{topic}' partition {partition}")]
    Seek { topic: String, partition: i32 },
    #[error("failed to read the Kafka partition assignment for topic '{topic}'")]
    ReadAssignment { topic: String },
    #[error("Kafka topic '{topic}' partition {partition} returned the maximum offset")]
    OffsetOverflow { topic: String, partition: i32 },
    #[error("Kafka batch timeout exceeds the monotonic clock range")]
    BatchDeadline,
}

#[derive(Debug, Default, PartialEq)]
enum KafkaConsumerAssignment {
    #[default]
    Unknown,
    Cleared,
    Partitions(TopicPartitionList),
}

impl KafkaConsumerAssignment {
    fn is_current(&self, assignment: &TopicPartitionList) -> bool {
        match self {
            Self::Unknown => false,
            Self::Cleared => assignment.count() == 0,
            Self::Partitions(current) => current == assignment,
        }
    }

    fn apply(
        &mut self,
        consumer: &StreamConsumer,
        topic: &str,
        assignment: &TopicPartitionList,
    ) -> Result<(), Report<KafkaSourceError>> {
        if self.is_current(assignment) {
            return Ok(());
        }
        if assignment.count() == 0 {
            return self.clear(consumer, topic);
        }
        consumer.assign(assignment).map_err(|source| {
            Report::new(KafkaSourceError::Assign {
                topic: topic.to_string(),
            })
            .attach_printable(source.to_string())
        })?;
        *self = Self::Partitions(assignment.clone());
        Ok(())
    }

    fn clear(
        &mut self,
        consumer: &StreamConsumer,
        topic: &str,
    ) -> Result<(), Report<KafkaSourceError>> {
        if *self == Self::Cleared {
            return Ok(());
        }
        consumer.unassign().map_err(|source| {
            Report::new(KafkaSourceError::Unassign {
                topic: topic.to_string(),
            })
            .attach_printable(source.to_string())
        })?;
        *self = Self::Cleared;
        Ok(())
    }
}

pub struct KafkaSourceMessage {
    message: rdkafka::message::OwnedMessage,
    position: KafkaOffsetPosition,
}

impl KafkaSourceMessage {
    fn from_message(
        message: rdkafka::message::OwnedMessage,
    ) -> Result<Self, Report<KafkaSourceError>> {
        let next_offset = message.offset().checked_add(1).ok_or_else(|| {
            Report::new(KafkaSourceError::OffsetOverflow {
                topic: message.topic().to_string(),
                partition: message.partition(),
            })
        })?;
        let position = KafkaOffsetPosition {
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: next_offset,
        };
        Ok(Self { message, position })
    }
}

impl IngestMessageHeaders for KafkaSourceMessage {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        let Some(headers) = self.message.headers() else {
            return;
        };
        for header in headers.iter() {
            let value = match header.value {
                Some(value) => String::from_utf8_lossy(value),
                None => std::borrow::Cow::Borrowed(""),
            };
            visit(header.key, value.as_ref());
        }
    }
}

impl SourceMessage for KafkaSourceMessage {
    type Position = KafkaOffsetPosition;

    fn payload(&self) -> &[u8] {
        self.message.payload().unwrap_or_default()
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        self
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::Kafka {
            topic: self.message.topic(),
            partition: self.message.partition(),
            offset: self.message.offset(),
            headers: self,
        }
    }
}

pub struct KafkaSource {
    /// Shared with the blocking-pool job of a commit in flight, which keeps the consumer open
    /// until librdkafka answers it.
    consumer: Arc<StreamConsumer>,
    topic: nervix_models::KafkaTopicName,
    offset_mode: KafkaSourceOffsetMode,
    commits: GroupCommits,
    instance_index: u64,
    assignment: KafkaConsumerAssignment,
    observed_generation: Option<u64>,
}

#[async_trait]
impl SourceConnector for KafkaSource {
    type Plan = KafkaSourcePlan;

    async fn open(plan: &Self::Plan, instance_index: u64) -> SourceResult<Self> {
        let mut config = ClientConfig::new();
        for entry in &plan.config {
            config.set(&entry.key, &entry.value);
        }
        let group_id = match &plan.offset_mode {
            KafkaSourceOffsetMode::ConsumerGroup { group_id, .. }
            | KafkaSourceOffsetMode::Domain { group_id, .. } => group_id,
        };
        config.set("group.id", group_id);
        config.set("enable.partition.eof", "false");
        config.set(
            "enable.auto.commit",
            if plan.offset_mode.driver_commits() {
                "true"
            } else {
                "false"
            },
        );
        let consumer = config
            .create()
            .map_err(|source| {
                Report::new(KafkaSourceError::Initialize).attach_printable(source.to_string())
            })
            .change_context(SourceError::Open { connector: KAFKA })?;
        Ok(Self {
            consumer: Arc::new(consumer),
            topic: plan.topic.clone(),
            offset_mode: plan.offset_mode.clone(),
            commits: GroupCommits::new(COMMIT_ANSWER_DEADLINE),
            instance_index,
            assignment: KafkaConsumerAssignment::default(),
            observed_generation: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        let KafkaSourceOffsetMode::Domain {
            offsets, rebalance, ..
        } = &mut self.offset_mode
        else {
            return false;
        };
        let generation_changed = offsets.generation() != self.observed_generation;
        let rebalance_changed = rebalance.has_changed().unwrap_or(false);
        generation_changed || rebalance_changed
    }

    async fn suspend(&mut self) -> SourceResult<()> {
        match &self.offset_mode {
            KafkaSourceOffsetMode::ConsumerGroup { group_id, commit } => self
                .commits
                .leave_group(&self.consumer, group_id, *commit)
                .await
                .change_context(SourceError::Suspend { connector: KAFKA }),
            KafkaSourceOffsetMode::Domain { .. } => self
                .assignment
                .clear(&self.consumer, self.topic.as_str())
                .change_context(SourceError::Suspend { connector: KAFKA }),
        }
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if let KafkaSourceOffsetMode::ConsumerGroup { .. } = &self.offset_mode {
            self.consumer
                .subscribe(&[self.topic.as_str()])
                .map_err(|source| {
                    Report::new(KafkaSourceError::Subscribe {
                        topic: self.topic.as_str().to_string(),
                    })
                    .attach_printable(source.to_string())
                })
                .change_context(SourceError::Resume { connector: KAFKA })?;
            return Ok(SourceResume::Ready);
        }

        let offsets = match &mut self.offset_mode {
            KafkaSourceOffsetMode::Domain {
                offsets, rebalance, ..
            } => {
                rebalance.borrow_and_update();
                offsets.clone()
            }
            KafkaSourceOffsetMode::ConsumerGroup { .. } => {
                return Ok(SourceResume::Ready);
            }
        };
        let ready = self
            .initialize_domain_offsets(&offsets)
            .await
            .change_context(SourceError::Resume { connector: KAFKA })?;
        if ready {
            Ok(SourceResume::Ready)
        } else {
            Ok(SourceResume::Waiting {
                retry_after: DOMAIN_ASSIGNMENT_RETRY,
            })
        }
    }

    async fn close(&mut self) -> SourceResult<()> {
        match &self.offset_mode {
            KafkaSourceOffsetMode::ConsumerGroup { group_id, commit } => self
                .commits
                .leave_group(&self.consumer, group_id, *commit)
                .await
                .change_context(SourceError::Close { connector: KAFKA }),
            KafkaSourceOffsetMode::Domain { .. } => self
                .assignment
                .clear(&self.consumer, self.topic.as_str())
                .change_context(SourceError::Close { connector: KAFKA }),
        }
    }
}

#[async_trait]
impl BrokerSourceConnector for KafkaSource {
    type Message = KafkaSourceMessage;
    type Position = KafkaOffsetPosition;

    async fn next_batch(
        &mut self,
        request: SourceBatchRequest,
        cancellation: &SourceReadCancellation,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let first = match &mut self.offset_mode {
            KafkaSourceOffsetMode::Domain { rebalance, .. } => {
                nervix_primitives::select! {
                    changed = rebalance.changed() => {
                        return if changed.is_ok() {
                            Ok(SourceBatch::ResumeRequired)
                        } else {
                            Ok(SourceBatch::Closed)
                        };
                    }
                    message = self.consumer.recv() => message,
                    () = cancellation.cancelled() => {
                        return Ok(SourceBatch::Interrupted {
                            gathered: Vec::new(),
                        });
                    }
                }
            }
            KafkaSourceOffsetMode::ConsumerGroup { .. } => {
                let Some(message) = cancellation.unless_cancelled(self.consumer.recv()).await
                else {
                    return Ok(SourceBatch::Interrupted {
                        gathered: Vec::new(),
                    });
                };
                message
            }
        };
        let first = first
            .map_err(|source| {
                Report::new(KafkaSourceError::Initialize).attach_printable(source.to_string())
            })
            .change_context(SourceError::Read { connector: KAFKA })?
            .detach();
        let first = KafkaSourceMessage::from_message(first)
            .change_context(SourceError::Read { connector: KAFKA })?;
        let mut messages = Vec::with_capacity(request.max_messages.get());
        messages.push(first);
        if request.max_messages == NonZeroUsize::MIN {
            return Ok(SourceBatch::Messages(messages));
        }
        let Some(batch_timeout) = request.batch_timeout else {
            return Ok(SourceBatch::Messages(messages));
        };
        let deadline = Instant::now().checked_add(batch_timeout).ok_or_else(|| {
            Report::new(SourceError::Read { connector: KAFKA })
                .attach(KafkaSourceError::BatchDeadline)
        })?;
        while messages.len() < request.max_messages.get() {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                _ = sleep_until(deadline) => break,
                next = self.consumer.recv() => {
                    match next {
                        Ok(message) => {
                            let message = KafkaSourceMessage::from_message(message.detach())
                                .change_context(SourceError::Read { connector: KAFKA })?;
                            messages.push(message);
                        }
                        Err(error) => {
                            warn!(error = %error, "failed to receive another Kafka batch message");
                        }
                    }
                }
                () = cancellation.cancelled() => {
                    return Ok(SourceBatch::Interrupted { gathered: messages });
                }
            }
        }
        Ok(SourceBatch::Messages(messages))
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        let positions = latest_positions(positions);
        match &self.offset_mode {
            // The driver commits the positions it delivered on its own cadence, so acknowledging
            // them confirms nothing on the transport.
            KafkaSourceOffsetMode::ConsumerGroup {
                commit: KafkaGroupCommit::DriverCadence,
                ..
            } => Ok(()),
            KafkaSourceOffsetMode::ConsumerGroup {
                group_id,
                commit: KafkaGroupCommit::EachAcknowledgedBatch,
            } => {
                let request = GroupCommitRequest::acknowledged(&positions)
                    .change_context(SourceError::Acknowledge { connector: KAFKA })?;
                self.commits
                    .commit(&self.consumer, group_id, request)
                    .await
                    .change_context(SourceError::Acknowledge { connector: KAFKA })
            }
            KafkaSourceOffsetMode::Domain { offsets, .. } => {
                for position in positions {
                    nervix_primitives::task::consume_budget().await;
                    offsets
                        .commit(position)
                        .await
                        .change_context(SourceError::Acknowledge { connector: KAFKA })?;
                }
                Ok(())
            }
        }
    }

    async fn reject(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        let starts = earliest_message_offsets(positions)
            .change_context(SourceError::Reject { connector: KAFKA })?;
        let assigned = self
            .assigned_partitions()
            .change_context(SourceError::Reject { connector: KAFKA })?;
        for position in starts {
            nervix_primitives::task::consume_budget().await;
            // Kafka can seek only a partition this consumer is fetching. A rebalance may have moved
            // the partition to another group member while the batch was in flight; it needs no
            // seek, because whichever member is assigned it next, this one included, resumes it
            // from the committed offset, and no offset past an unacknowledged record was committed.
            if !assigned.contains(&position) {
                debug!(
                    topic = position.topic.as_str(),
                    partition = position.partition,
                    offset = position.offset,
                    "rejected Kafka partition is no longer assigned; its next holder resumes it \
                     from the committed offset"
                );
                continue;
            }
            self.seek(&position)
                .change_context(SourceError::Reject { connector: KAFKA })?;
        }
        Ok(())
    }
}

impl KafkaSource {
    async fn initialize_domain_offsets(
        &mut self,
        host: &KafkaDomainOffsetHost,
    ) -> SourceResult<bool> {
        let partitions = topic_partitions(&self.consumer, self.topic.as_str())
            .change_context(SourceError::Resume { connector: KAFKA })?;
        let initialization = host
            .initialization(&partitions)
            .await
            .change_context(SourceError::Resume { connector: KAFKA })?;
        let is_new_start = matches!(initialization.start, KafkaDomainOffsetStart::At(_));
        let offsets = match initialization.start {
            KafkaDomainOffsetStart::Resume {
                positions,
                missing_partition_timestamp,
            } => resume_offsets(
                &self.consumer,
                self.topic.as_str(),
                &partitions,
                positions,
                missing_partition_timestamp,
            )
            .change_context(SourceError::Resume { connector: KAFKA })?,
            KafkaDomainOffsetStart::At(timestamp) => offsets_for_partitions_by_timestamp(
                &self.consumer,
                self.topic.as_str(),
                partitions.iter().copied(),
                timestamp,
            )
            .change_context(SourceError::Resume { connector: KAFKA })?,
        };
        let ready = assign_offsets_for_instance(
            &self.consumer,
            self.topic.as_str(),
            &offsets,
            initialization.schedule.as_ref(),
            self.instance_index,
            &mut self.assignment,
        )
        .change_context(SourceError::Resume { connector: KAFKA })?;

        if is_new_start {
            let positions = concrete_next_offsets_from_assignment(
                &self.consumer,
                self.topic.as_str(),
                &offsets,
            )
            .change_context(SourceError::Resume { connector: KAFKA })?;
            host.reset(positions)
                .await
                .change_context(SourceError::Resume { connector: KAFKA })?;
        }
        self.observed_generation = Some(initialization.generation);
        Ok(ready)
    }

    fn assigned_partitions(&self) -> Result<AssignedPartitions, Report<KafkaSourceError>> {
        let assignment = self.consumer.assignment().map_err(|source| {
            Report::new(KafkaSourceError::ReadAssignment {
                topic: self.topic.as_str().to_string(),
            })
            .attach_printable(source.to_string())
        })?;
        Ok(AssignedPartitions::of_topic(
            self.topic.as_str(),
            &assignment,
        ))
    }

    fn seek(&self, position: &KafkaOffsetPosition) -> Result<(), Report<KafkaSourceError>> {
        self.consumer
            .seek(
                &position.topic,
                position.partition,
                Offset::Offset(position.offset),
                Duration::from_secs(5),
            )
            .map_err(|source| {
                Report::new(KafkaSourceError::Seek {
                    topic: position.topic.clone(),
                    partition: position.partition,
                })
                .attach_printable(source.to_string())
            })
    }
}

/// What one consumer-group commit commits.
enum GroupCommitRequest {
    /// The latest acknowledged offset of each partition of an acknowledged batch.
    Acknowledged(TopicPartitionList),
    /// The positions the driver delivered since its last commit.
    Delivered,
}

impl GroupCommitRequest {
    /// The commit of an acknowledged batch's `positions`, each the next offset of its partition.
    fn acknowledged(positions: &[KafkaOffsetPosition]) -> Result<Self, Report<KafkaSourceError>> {
        let mut offsets = TopicPartitionList::new();
        for position in positions {
            offsets
                .add_partition_offset(
                    &position.topic,
                    position.partition,
                    Offset::Offset(position.offset),
                )
                .map_err(|source| {
                    Report::new(source).change_context(KafkaSourceError::BuildOffsetCommit {
                        topic: position.topic.clone(),
                        partition: position.partition,
                    })
                })?;
        }
        Ok(Self::Acknowledged(offsets))
    }

    /// Commits through `consumer` and returns the coordinator's answer.
    ///
    /// librdkafka's synchronous commit parks the calling thread until the coordinator answers.
    fn commit(self, consumer: &StreamConsumer) -> KafkaResult<()> {
        match self {
            Self::Acknowledged(offsets) => consumer.commit(&offsets, CommitMode::Sync),
            Self::Delivered => {
                let committed = consumer.commit_consumer_state(CommitMode::Sync);
                match committed {
                    // The driver delivered nothing since its last commit.
                    Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => Ok(()),
                    other => other,
                }
            }
        }
    }
}

/// One source instance's consumer-group commits, which run one at a time.
///
/// A commit parks a blocking-pool thread until its coordinator answers, and the source waits for
/// that answer at most `deadline`. A commit whose answer outlasts the deadline stays in flight
/// until librdkafka answers it, and the next commit waits for it first, so an instance never parks
/// more than one thread.
struct GroupCommits {
    deadline: Duration,
    /// The commit whose answer outlasted its deadline, still owned by librdkafka.
    unanswered: Option<JoinHandle<KafkaResult<()>>>,
}

impl GroupCommits {
    fn new(deadline: Duration) -> Self {
        Self {
            deadline,
            unanswered: None,
        }
    }

    /// Commits `request` for consumer group `group_id` through `consumer`, and returns once the
    /// group's coordinator answered it, or fails when the deadline passes first.
    async fn commit(
        &mut self,
        consumer: &Arc<StreamConsumer>,
        group_id: &str,
        request: GroupCommitRequest,
    ) -> Result<(), Report<KafkaSourceError>> {
        if let Some(mut unanswered) = self.unanswered.take() {
            let earlier = timeout(self.deadline, &mut unanswered).await;
            let Ok(earlier) = earlier else {
                self.unanswered = Some(unanswered);
                return Err(self.unanswered_failure(group_id));
            };
            earlier.discarded(
                "the acknowledgement or leave that started the commit already failed when its \
                 deadline passed, so a late answer owes nothing",
            );
        }
        let consumer = consumer.clone();
        let mut pending =
            nervix_primitives::task::spawn_blocking(move || request.commit(&consumer));
        let answered = timeout(self.deadline, &mut pending).await;
        let Ok(joined) = answered else {
            self.unanswered = Some(pending);
            return Err(self.unanswered_failure(group_id));
        };
        let committed = joined.map_err(|source| {
            Report::new(source).change_context(KafkaSourceError::CommitTask {
                group: group_id.to_string(),
            })
        })?;
        committed.map_err(|source| {
            Report::new(source).change_context(KafkaSourceError::CommitOffset {
                group: group_id.to_string(),
            })
        })
    }

    /// Leaves consumer group `group_id` through `consumer`, first committing what `commit` leaves
    /// outstanding.
    ///
    /// An acknowledged group has nothing outstanding: acknowledging a batch returned only once its
    /// commit was answered. The driver's cadence leaves the positions it delivered since its last
    /// commit, which are committed and answered before the group can hand the partitions to
    /// another member. A failed commit is returned once the consumer left the group all the same.
    async fn leave_group(
        &mut self,
        consumer: &Arc<StreamConsumer>,
        group_id: &str,
        commit: KafkaGroupCommit,
    ) -> Result<(), Report<KafkaSourceError>> {
        let committed = match commit {
            KafkaGroupCommit::DriverCadence => {
                self.commit(consumer, group_id, GroupCommitRequest::Delivered)
                    .await
            }
            KafkaGroupCommit::EachAcknowledgedBatch => Ok(()),
        };
        consumer.unsubscribe();
        committed
    }

    fn unanswered_failure(&self, group_id: &str) -> Report<KafkaSourceError> {
        Report::new(KafkaSourceError::CommitUnanswered {
            group: group_id.to_string(),
            deadline: self.deadline,
        })
    }
}

/// The partitions of the source topic that the consumer's current assignment holds.
struct AssignedPartitions {
    topic: String,
    partitions: BTreeSet<i32>,
}

impl AssignedPartitions {
    fn of_topic(topic: &str, assignment: &TopicPartitionList) -> Self {
        let partitions = assignment
            .elements_for_topic(topic)
            .iter()
            .map(|element| element.partition())
            .collect();
        Self {
            topic: topic.to_string(),
            partitions,
        }
    }

    fn contains(&self, position: &KafkaOffsetPosition) -> bool {
        position.topic == self.topic && self.partitions.contains(&position.partition)
    }
}

/// The latest acknowledged offset of every partition, ordered by topic and partition, which the
/// source commits.
pub fn latest_positions(positions: &[KafkaOffsetPosition]) -> Vec<KafkaOffsetPosition> {
    let mut topics = BTreeMap::<String, BTreeMap<i32, i64>>::new();
    for position in positions {
        // A topic is found by its borrowed name, so only its first position copies the name. A
        // partition is found before it is inserted rather than through an entry: positions arrive
        // in runs from a few partitions, where the lookup measured faster (`bench-kafka-offsets`).
        if let Some(partitions) = topics.get_mut(&position.topic) {
            if let Some(offset) = partitions.get_mut(&position.partition) {
                *offset = (*offset).max(position.offset);
            } else {
                partitions.insert(position.partition, position.offset);
            }
        } else {
            topics.insert(
                position.topic.clone(),
                BTreeMap::from([(position.partition, position.offset)]),
            );
        }
    }
    let mut latest = Vec::new();
    for (topic, partitions) in topics {
        for (partition, offset) in partitions {
            latest.push(KafkaOffsetPosition {
                topic: topic.clone(),
                partition,
                offset,
            });
        }
    }
    latest
}

/// The earliest rejected message of every partition, ordered by topic and partition, from which the
/// source reads again.
pub fn earliest_message_offsets(
    positions: &[KafkaOffsetPosition],
) -> Result<Vec<KafkaOffsetPosition>, Report<KafkaSourceError>> {
    let mut topics = BTreeMap::<String, BTreeMap<i32, i64>>::new();
    for position in positions {
        let offset = position.offset.checked_sub(1).ok_or_else(|| {
            Report::new(KafkaSourceError::UnsupportedDomainOffset {
                topic: position.topic.clone(),
                partition: position.partition,
            })
        })?;
        // Folded as in `latest_positions`, for the same measured reason.
        if let Some(partitions) = topics.get_mut(&position.topic) {
            if let Some(current) = partitions.get_mut(&position.partition) {
                *current = (*current).min(offset);
            } else {
                partitions.insert(position.partition, offset);
            }
        } else {
            topics.insert(
                position.topic.clone(),
                BTreeMap::from([(position.partition, offset)]),
            );
        }
    }
    let mut earliest = Vec::new();
    for (topic, partitions) in topics {
        for (partition, offset) in partitions {
            earliest.push(KafkaOffsetPosition {
                topic: topic.clone(),
                partition,
                offset,
            });
        }
    }
    Ok(earliest)
}

fn topic_partitions(
    consumer: &StreamConsumer,
    topic: &str,
) -> Result<Vec<i32>, Report<KafkaSourceError>> {
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .map_err(|source| {
            Report::new(KafkaSourceError::FetchMetadata {
                topic: topic.to_string(),
            })
            .attach_printable(source.to_string())
        })?;
    let Some(topic_metadata) = metadata.topics().iter().find(|entry| entry.name() == topic) else {
        return Err(Report::new(KafkaSourceError::MissingMetadata {
            topic: topic.to_string(),
        }));
    };
    Ok(topic_metadata
        .partitions()
        .iter()
        .map(|partition| partition.id())
        .collect())
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the Kafka driver owns timestamp lookup and its caller-supplied partition \
                  iterator"
    )
)]
fn offsets_for_partitions_by_timestamp<I>(
    consumer: &StreamConsumer,
    topic: &str,
    partitions: I,
    timestamp: Timestamp,
) -> Result<BTreeMap<i32, Offset>, Report<KafkaSourceError>>
where
    I: IntoIterator<Item = i32>,
{
    let mut query = TopicPartitionList::new();
    let timestamp_ms = timestamp.unix_nanos().div_euclid(1_000_000);
    for partition in partitions {
        query
            .add_partition_offset(topic, partition, Offset::Offset(timestamp_ms))
            .map_err(|source| {
                Report::new(KafkaSourceError::BuildTimestampQuery {
                    topic: topic.to_string(),
                    partition,
                })
                .attach_printable(source.to_string())
            })?;
    }
    let resolved = consumer
        .offsets_for_times(query, Duration::from_secs(5))
        .map_err(|source| {
            Report::new(KafkaSourceError::ResolveTimestampOffsets {
                topic: topic.to_string(),
            })
            .attach_printable(source.to_string())
        })?;
    let mut offsets = BTreeMap::new();
    for element in resolved.elements() {
        let offset = match element.offset() {
            Offset::Invalid => Offset::End,
            other => other,
        };
        offsets.insert(element.partition(), offset);
    }
    Ok(offsets)
}

fn normalized_resume_offset(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    next_offset: i64,
) -> Result<Offset, Report<KafkaSourceError>> {
    let (low, high) = consumer
        .fetch_watermarks(topic, partition, Duration::from_secs(5))
        .map_err(|source| {
            Report::new(KafkaSourceError::FetchWatermarks {
                topic: topic.to_string(),
                partition,
            })
            .attach_printable(source.to_string())
        })?;
    Ok(Offset::Offset(next_offset.clamp(low, high)))
}

fn resume_offsets(
    consumer: &StreamConsumer,
    topic: &str,
    partitions: &[i32],
    positions: Vec<KafkaOffsetPosition>,
    missing_partition_timestamp: Option<Timestamp>,
) -> Result<BTreeMap<i32, Offset>, Report<KafkaSourceError>> {
    let mut recorded = BTreeMap::new();
    for position in positions {
        if position.topic == topic {
            recorded.insert(position.partition, position.offset);
        }
    }
    let mut offsets = BTreeMap::new();
    let mut missing = Vec::new();
    for partition in partitions {
        if let Some(next_offset) = recorded.get(partition) {
            offsets.insert(
                *partition,
                normalized_resume_offset(consumer, topic, *partition, *next_offset)?,
            );
        } else {
            missing.push(*partition);
        }
    }
    if let Some(timestamp) = missing_partition_timestamp {
        offsets.extend(offsets_for_partitions_by_timestamp(
            consumer, topic, missing, timestamp,
        )?);
    } else {
        for partition in missing {
            offsets.insert(partition, Offset::Beginning);
        }
    }
    Ok(offsets)
}

fn assign_offsets_for_instance(
    consumer: &StreamConsumer,
    topic: &str,
    offsets: &BTreeMap<i32, Offset>,
    schedule: Option<&KafkaPartitionSchedule>,
    instance_index: u64,
    consumer_assignment: &mut KafkaConsumerAssignment,
) -> Result<bool, Report<KafkaSourceError>> {
    let has_topic_partitions = schedule.is_some() && !offsets.is_empty();
    let assigned_partitions = if let Some(schedule) = schedule
        && let Ok(instance_index) = usize::try_from(instance_index)
        && let Some(assignments) = schedule.instance_assignments.get(instance_index)
    {
        assignments.iter().copied().collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    let mut assignment = TopicPartitionList::new();
    for (partition, offset) in offsets {
        if assigned_partitions.contains(partition) {
            assignment
                .add_partition_offset(topic, *partition, *offset)
                .map_err(|source| {
                    Report::new(KafkaSourceError::BuildAssignment {
                        topic: topic.to_string(),
                        partition: *partition,
                    })
                    .attach_printable(source.to_string())
                })?;
        }
    }
    consumer_assignment.apply(consumer, topic, &assignment)?;
    Ok(has_topic_partitions)
}

fn concrete_next_offsets_from_assignment(
    consumer: &StreamConsumer,
    topic: &str,
    offsets: &BTreeMap<i32, Offset>,
) -> Result<Vec<KafkaOffsetPosition>, Report<KafkaSourceError>> {
    let mut concrete = Vec::with_capacity(offsets.len());
    for (partition, offset) in offsets {
        let next_offset = match offset {
            Offset::Offset(value) => *value,
            Offset::Beginning => {
                let mapping_input = consumer
                    .fetch_watermarks(topic, *partition, Duration::from_secs(5))
                    .map(|(low, _)| low);
                match mapping_input {
                    Ok(mapped_value) => Ok(mapped_value),
                    Err(source) => Err(Report::new(KafkaSourceError::FetchWatermarks {
                        topic: topic.to_string(),
                        partition: *partition,
                    })
                    .attach_printable(source.to_string())),
                }
            }?,
            Offset::End | Offset::Invalid => {
                let mapping_input = consumer
                    .fetch_watermarks(topic, *partition, Duration::from_secs(5))
                    .map(|(_, high)| high);
                match mapping_input {
                    Ok(mapped_value) => Ok(mapped_value),
                    Err(source) => Err(Report::new(KafkaSourceError::FetchWatermarks {
                        topic: topic.to_string(),
                        partition: *partition,
                    })
                    .attach_printable(source.to_string())),
                }
            }?,
            Offset::Stored | Offset::OffsetTail(_) => {
                return Err(Report::new(KafkaSourceError::UnsupportedDomainOffset {
                    topic: topic.to_string(),
                    partition: *partition,
                }));
            }
        };
        concrete.push(KafkaOffsetPosition {
            topic: topic.to_string(),
            partition: *partition,
            offset: next_offset,
        });
    }
    Ok(concrete)
}

pub struct TopicPartitionInspector {
    consumer: Arc<StreamConsumer>,
}

impl TopicPartitionInspector {
    pub fn new(
        config: &[ClientConfigEntry],
        group_id: String,
    ) -> Result<Self, Report<KafkaSourceError>> {
        let mut client_config = ClientConfig::new();
        for entry in config {
            client_config.set(&entry.key, &entry.value);
        }
        client_config.set("group.id", group_id);
        client_config.set("enable.partition.eof", "false");
        client_config.set("enable.auto.commit", "false");
        let consumer = client_config.create().map_err(|source| {
            Report::new(KafkaSourceError::Initialize).attach_printable(source.to_string())
        })?;
        Ok(Self {
            consumer: Arc::new(consumer),
        })
    }

    pub async fn partitions(&self, topic: &str) -> Result<Vec<i32>, Report<KafkaSourceError>> {
        let consumer = self.consumer.clone();
        let topic = topic.to_string();
        nervix_primitives::task::spawn_blocking(move || topic_partitions(&consumer, &topic))
            .await
            .map_err(|source| {
                Report::new(KafkaSourceError::InspectPartitions)
                    .attach_printable(source.to_string())
            })?
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    fn assignment_at(offset: i64) -> TopicPartitionList {
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset("events", 0, Offset::Offset(offset))
            .assured("the test uses a nonnegative literal Kafka offset");
        assignment
    }

    /// Both KAFKA TOPIC sides declare ordered repeats with exact names: the record headers a
    /// sink writes keep every write in the order written, and a reader of the produced record
    /// visits them in that order, a repeated name once for each of its values.
    #[test]
    fn written_headers_read_back_in_write_order_with_every_repeat() {
        let mut written = Vec::new();
        for (name, value) in [
            ("trace", "a"),
            ("Tenant", "x"),
            ("trace", "b"),
            ("tenant", "y"),
        ] {
            written.push((name.to_string(), value.to_string()));
        }
        let message = rdkafka::message::OwnedMessage::new(
            Some(b"{}".to_vec()),
            None,
            "events".to_string(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            Some(crate::KafkaSink::owned_headers(&written)),
        );
        let message =
            KafkaSourceMessage::from_message(message).assured("offset zero has a next offset");
        let mut visited = Vec::new();
        message.visit(&mut |name, value| visited.push(format!("{name}={value}")));
        assert_eq!(visited, ["trace=a", "Tenant=x", "trace=b", "tenant=y"]);
    }

    #[test]
    fn kafka_assignment_state_suppresses_only_identical_requests() {
        let assignment = assignment_at(10);
        let changed_offset = assignment_at(11);
        let empty = TopicPartitionList::new();
        let current = KafkaConsumerAssignment::Partitions(assignment.clone());

        assert!(current.is_current(&assignment));
        assert!(!current.is_current(&changed_offset));
        assert!(!current.is_current(&empty));
        assert!(KafkaConsumerAssignment::Cleared.is_current(&empty));
        assert!(!KafkaConsumerAssignment::Unknown.is_current(&assignment));
    }

    /// A generated acknowledged or rejected position over a few topics and partitions.
    #[derive(Debug, bolero::TypeGenerator)]
    struct GeneratedPosition {
        topic: u8,
        partition: u8,
        offset: i64,
    }

    #[derive(Debug, bolero::TypeGenerator)]
    struct GeneratedPositions {
        #[generator(bolero::generator::produce_with::<Vec<GeneratedPosition>>().len(0_usize..=64))]
        positions: Vec<GeneratedPosition>,
    }

    /// The topic partition the reference folds offsets under.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct PartitionKey {
        topic: String,
        partition: i32,
    }

    impl GeneratedPositions {
        fn positions(&self) -> Vec<KafkaOffsetPosition> {
            self.positions
                .iter()
                .map(|position| KafkaOffsetPosition {
                    topic: format!("topic-{}", position.topic % 3),
                    partition: i32::from(position.partition % 4),
                    offset: position.offset,
                })
                .collect()
        }
    }

    fn key(position: &KafkaOffsetPosition) -> PartitionKey {
        PartitionKey {
            topic: position.topic.clone(),
            partition: position.partition,
        }
    }

    fn ordered(folded: BTreeMap<PartitionKey, i64>) -> Vec<KafkaOffsetPosition> {
        folded
            .into_iter()
            .map(|(key, offset)| KafkaOffsetPosition {
                topic: key.topic,
                partition: key.partition,
                offset,
            })
            .collect()
    }

    /// The greatest acknowledged offset of every topic partition, ordered by topic and partition.
    fn latest_reference(positions: &[KafkaOffsetPosition]) -> Vec<KafkaOffsetPosition> {
        let mut folded = BTreeMap::new();
        for position in positions {
            let current = folded.entry(key(position)).or_insert(position.offset);
            *current = (*current).max(position.offset);
        }
        ordered(folded)
    }

    /// The offset of the earliest rejected message of every topic partition, or absent when a
    /// position has no message before it.
    fn earliest_reference(positions: &[KafkaOffsetPosition]) -> Option<Vec<KafkaOffsetPosition>> {
        let mut folded = BTreeMap::new();
        for position in positions {
            let offset = position.offset.checked_sub(1)?;
            let current = folded.entry(key(position)).or_insert(offset);
            *current = (*current).min(offset);
        }
        Some(ordered(folded))
    }

    /// Acknowledged positions fold to each partition's latest offset and rejected positions to
    /// each partition's earliest message, in topic and partition order, whatever the order and
    /// repetition of the positions; a position with no message before it is refused.
    #[test]
    fn bolero_partition_offsets_fold_to_the_latest_and_earliest_per_partition() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(1024)
            .with_type::<GeneratedPositions>()
            .for_each(|generated| {
                let positions = generated.positions();
                assert_eq!(latest_positions(&positions), latest_reference(&positions));
                let expected = earliest_reference(&positions);
                match earliest_message_offsets(&positions) {
                    Ok(earliest) => assert_eq!(Some(earliest), expected),
                    Err(report) => {
                        assert_eq!(expected, None);
                        assert!(matches!(
                            report.current_context(),
                            KafkaSourceError::UnsupportedDomainOffset { .. }
                        ));
                    }
                }
            });
    }

    #[test]
    fn kafka_positions_keep_latest_commits_and_earliest_rejections() {
        let positions = vec![
            KafkaOffsetPosition {
                topic: "events".to_string(),
                partition: 0,
                offset: 12,
            },
            KafkaOffsetPosition {
                topic: "events".to_string(),
                partition: 0,
                offset: 15,
            },
        ];
        assert_eq!(latest_positions(&positions)[0].offset, 15);
        assert_eq!(
            earliest_message_offsets(&positions).assured("the positions are positive")[0].offset,
            11,
        );
    }

    #[test]
    fn kafka_rewinds_only_partitions_the_consumer_is_assigned() {
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition("events", 0);
        assignment.add_partition("events", 2);
        assignment.add_partition("audit", 1);
        let assigned = AssignedPartitions::of_topic("events", &assignment);

        let assigned_start = KafkaOffsetPosition {
            topic: "events".to_string(),
            partition: 2,
            offset: 7,
        };
        let moved_start = KafkaOffsetPosition {
            topic: "events".to_string(),
            partition: 1,
            offset: 7,
        };
        let other_topic_start = KafkaOffsetPosition {
            topic: "audit".to_string(),
            partition: 1,
            offset: 7,
        };
        assert!(assigned.contains(&assigned_start));
        assert!(!assigned.contains(&moved_start));
        assert!(!assigned.contains(&other_topic_start));
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "source_commit_tests.rs"]
mod commit_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "source_read_tests.rs"]
mod read_tests;
