//! Kafka source transport and host-owned domain-offset services.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Kafka consumer configuration, polling, headers and metadata, partition assignment,
//!   offset acknowledgement, partition inspection, and the opaque services through which a source
//!   reads and advances host-owned domain offsets.
//! - **Depends on.** The connector contract, Kafka vocabulary values, the bounded executor,
//!   the primitive boundary, `error-stack` and `rust-rdkafka`.
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

#[path = "source_control.rs"]
mod control;
#[path = "source_rejection.rs"]
mod rejection;

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    time::Duration,
};

use async_trait::async_trait;
use control::{CommitTask, ControlBudget, KafkaControl};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, SourceAckPolicy, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceReadCancellation,
    SourceResult, SourceResume,
};
use nervix_execution::Executor;
use nervix_models::{
    ClientConfigEntry, ConnectorKind, KafkaPartitionSchedule, KafkaTopicName, Timestamp,
};
use nervix_primitives::{
    sync::{Arc, watch},
    time::{Instant, sleep_until, timeout},
};
use nervix_recovery::{Discarded as _, Reported as _};
use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer},
    error::{KafkaError, KafkaResult, RDKafkaErrorCode},
    message::{Headers, Message as KafkaMessage},
    topic_partition_list::{Offset, TopicPartitionList},
};
use rejection::{KafkaConsumer, KafkaConsumerContext};
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
    /// The node-owned executor used by this source and its inspectors.
    pub executor: Executor,
    pub config: Vec<ClientConfigEntry>,
    pub topic: KafkaTopicName,
    pub offset_mode: KafkaSourceOffsetMode,
}

#[derive(Debug, Error)]
pub enum KafkaSourceError {
    #[error("the {connector} source cannot admit its control call", connector = ConnectorKind::Kafka)]
    ControlAdmission,
    #[error("the {connector} source control call could not run", connector = ConnectorKind::Kafka)]
    ControlExecution,
    #[error("the {connector} control call did not finish within socket.timeout.ms {bound}", connector = ConnectorKind::Kafka, bound = humantime::format_duration(*timeout))]
    ControlTimeout { timeout: Duration },
    #[error("the {connector} control call was cancelled", connector = ConnectorKind::Kafka)]
    ControlCancelled,
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
    #[error(
        "Kafka topic '{topic}' partition {partition} has no domain offset, and the client's \
         auto.offset.reset refuses to start a partition without one"
    )]
    ResetRefused { topic: String, partition: i32 },
}

/// Where the client's `auto.offset.reset` starts a partition that has no offset to resume from,
/// under each of the names librdkafka accepts for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString)]
enum KafkaOffsetReset {
    /// The partition's earliest retained record.
    #[strum(
        serialize = "smallest",
        serialize = "earliest",
        serialize = "beginning"
    )]
    Earliest,
    /// The partition's end, so only the records produced after it was taken up are read.
    #[strum(serialize = "largest", serialize = "latest", serialize = "end")]
    Latest,
    /// No start: the partition cannot be read until it has an offset.
    #[strum(serialize = "error")]
    Refuse,
}

impl KafkaOffsetReset {
    /// The policy `config` declares, or librdkafka's own default when it declares none.
    ///
    /// `auto.offset.reset` is a topic property, which librdkafka's client configuration reports
    /// only once the client sets one. A client that declares no policy gets the property's
    /// default, `largest`.
    fn of(config: &ClientConfig) -> Result<Self, Report<KafkaSourceError>> {
        if config.get("auto.offset.reset").is_none() {
            return Ok(Self::Latest);
        }
        let native = config
            .create_native_config()
            .change_context(KafkaSourceError::Initialize)?;
        let value = native
            .get("auto.offset.reset")
            .change_context(KafkaSourceError::Initialize)?;
        let policy = value.parse::<Self>().assured(
            "librdkafka accepts a declared policy under any of its seven names, in any case, and \
             reports it under the first name of its value, which this policy parses",
        );
        Ok(policy)
    }

    /// The next offset of each of `partitions` of `topic` this policy starts it at.
    fn starts(
        self,
        consumer: &KafkaConsumer,
        topic: &str,
        partitions: &BTreeSet<i32>,
        budget: &ControlBudget<'_>,
    ) -> Result<BTreeMap<i32, i64>, Report<KafkaSourceError>> {
        let mut starts = BTreeMap::new();
        for partition in partitions {
            budget.remaining()?;
            let watermarks = PartitionWatermarks::fetch(consumer, topic, *partition, budget)?;
            let start = match self {
                Self::Earliest => watermarks.low,
                Self::Latest => watermarks.high,
                Self::Refuse => {
                    return Err(Report::new(KafkaSourceError::ResetRefused {
                        topic: topic.to_string(),
                        partition: *partition,
                    }));
                }
            };
            starts.insert(*partition, start);
        }
        Ok(starts)
    }
}

/// The offset of a partition's earliest retained record and the offset the next record produced to
/// it will take.
struct PartitionWatermarks {
    low: i64,
    high: i64,
}

impl PartitionWatermarks {
    fn fetch(
        consumer: &KafkaConsumer,
        topic: &str,
        partition: i32,
        budget: &ControlBudget<'_>,
    ) -> Result<Self, Report<KafkaSourceError>> {
        let (low, high) = consumer
            .fetch_watermarks(topic, partition, budget.remaining()?)
            .map_err(|source| {
                Report::new(source).change_context(KafkaSourceError::FetchWatermarks {
                    topic: topic.to_string(),
                    partition,
                })
            })?;
        Ok(Self { low, high })
    }
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
        consumer: &KafkaConsumer,
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
            Report::new(source).change_context(KafkaSourceError::Assign {
                topic: topic.to_string(),
            })
        })?;
        *self = Self::Partitions(assignment.clone());
        Ok(())
    }

    fn clear(
        &mut self,
        consumer: &KafkaConsumer,
        topic: &str,
    ) -> Result<(), Report<KafkaSourceError>> {
        if *self == Self::Cleared {
            return Ok(());
        }
        consumer.unassign().map_err(|source| {
            Report::new(source).change_context(KafkaSourceError::Unassign {
                topic: topic.to_string(),
            })
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
    control: KafkaControl,
    /// Shared with the blocking-pool job of a commit in flight, which keeps the consumer open
    /// until librdkafka answers it.
    consumer: Arc<KafkaConsumer>,
    topic: nervix_models::KafkaTopicName,
    offset_mode: KafkaSourceOffsetMode,
    /// Where a partition with no domain offset starts when no paced domain time places it.
    offset_reset: KafkaOffsetReset,
    commits: GroupCommits,
    /// Whether a rejection's floor may still be outstanding in the consumer's context, so an
    /// acknowledged commit confirms it.
    rejections_outstanding: bool,
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
        let control = KafkaControl::new(&config, plan.executor.clone())
            .change_context(SourceError::Open { connector: KAFKA })?;
        let offset_reset =
            KafkaOffsetReset::of(&config).change_context(SourceError::Open { connector: KAFKA })?;
        let consumer = config
            .create_with_context(KafkaConsumerContext::default())
            .map_err(|source| Report::new(source).change_context(KafkaSourceError::Initialize))
            .change_context(SourceError::Open { connector: KAFKA })?;
        Ok(Self {
            control,
            consumer: Arc::new(consumer),
            topic: plan.topic.clone(),
            offset_mode: plan.offset_mode.clone(),
            offset_reset,
            commits: GroupCommits::new(COMMIT_ANSWER_DEADLINE),
            rejections_outstanding: false,
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
                .leave_group(&self.control, &self.consumer, group_id, *commit)
                .await
                .change_context(SourceError::Suspend { connector: KAFKA }),
            KafkaSourceOffsetMode::Domain { .. } => self
                .clear_assignment()
                .await
                .change_context(SourceError::Suspend { connector: KAFKA }),
        }
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if let KafkaSourceOffsetMode::ConsumerGroup { .. } = &self.offset_mode {
            let topic = self.topic.as_str().to_owned();
            self.control
                .run(&self.consumer, move |consumer, _budget| {
                    consumer
                        .subscribe(&[&topic])
                        .change_context(KafkaSourceError::Subscribe { topic })
                })
                .await
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
                .leave_group(&self.control, &self.consumer, group_id, *commit)
                .await
                .change_context(SourceError::Close { connector: KAFKA }),
            KafkaSourceOffsetMode::Domain { .. } => self
                .clear_assignment()
                .await
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
            .map_err(|source| Report::new(source).change_context(KafkaSourceError::Initialize))
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
                    .commit(&self.control, &self.consumer, group_id, request)
                    .await
                    .change_context(SourceError::Acknowledge { connector: KAFKA })?;
                // A commit past a rejected record confirms the record was delivered again, so the
                // group may resume its partition from the committed offset from now on.
                if self.rejections_outstanding {
                    self.rejections_outstanding =
                        self.consumer.context().confirm_commit(&positions);
                }
                Ok(())
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
        // A consumer group can revoke a partition before its rewound records are read again, and
        // then resumes it from a committed offset past them, or from where the client's reset
        // policy starts a partition the group committed nothing for. The floor starts the
        // partition at its earliest rejected record whenever the group assigns it to this
        // consumer, until a commit passes that record. Domain offsets need none: they record only
        // acknowledged positions, which never pass a rejected record.
        if let KafkaSourceOffsetMode::ConsumerGroup { .. } = &self.offset_mode {
            self.consumer.context().record_rejection(&starts);
            self.rejections_outstanding = true;
        }
        let topic = self.topic.as_str().to_owned();
        self.control
            .run(&self.consumer, move |consumer, budget| {
                let assignment =
                    consumer
                        .assignment()
                        .change_context(KafkaSourceError::ReadAssignment {
                            topic: topic.clone(),
                        })?;
                let assigned = AssignedPartitions::of_topic(&topic, &assignment);
                for position in starts {
                    budget.remaining()?;
                    if !assigned.contains(&position) {
                        debug!(
                            topic = position.topic.as_str(),
                            partition = position.partition,
                            offset = position.offset,
                            "rejected Kafka partition is not assigned to this consumer, so it is \
                             not sought now"
                        );
                        continue;
                    }
                    consumer
                        .seek(
                            &position.topic,
                            position.partition,
                            Offset::Offset(position.offset),
                            budget.remaining()?,
                        )
                        .change_context(KafkaSourceError::Seek {
                            topic: position.topic,
                            partition: position.partition,
                        })?;
                }
                Ok(())
            })
            .await
            .change_context(SourceError::Reject { connector: KAFKA })
    }
}

impl KafkaSource {
    async fn clear_assignment(&mut self) -> Result<(), Report<KafkaSourceError>> {
        let mut assignment = std::mem::take(&mut self.assignment);
        let topic = self.topic.as_str().to_owned();
        self.assignment = self
            .control
            .run(&self.consumer, move |consumer, _budget| {
                assignment.clear(consumer, &topic)?;
                Ok(assignment)
            })
            .await?;
        Ok(())
    }

    async fn initialize_domain_offsets(
        &mut self,
        host: &KafkaDomainOffsetHost,
    ) -> SourceResult<bool> {
        let topic = self.topic.as_str().to_owned();
        let partitions = self
            .control
            .run(&self.consumer, move |consumer, budget| {
                topic_partitions(consumer, &topic, budget)
            })
            .await
            .change_context(SourceError::Resume { connector: KAFKA })?;
        let initialization = host
            .initialization(&partitions)
            .await
            .change_context(SourceError::Resume { connector: KAFKA })?;
        let generation = initialization.generation;
        let topic = self.topic.as_str().to_owned();
        let instance_index = self.instance_index;
        let offset_reset = self.offset_reset;
        let mut assignment = std::mem::take(&mut self.assignment);
        struct InitializedOffsets {
            ready: bool,
            assignment: KafkaConsumerAssignment,
            record: DomainOffsetRecord,
        }
        let initialized = self
            .control
            .run(&self.consumer, move |consumer, budget| {
                let ready = initialization.schedule.is_some() && !partitions.is_empty();
                let scheduled = ScheduledPartitions {
                    topic: &topic,
                    partitions: &partitions,
                    assigned: scheduled_partitions(
                        initialization.schedule.as_ref(),
                        instance_index,
                    ),
                };
                let start = DomainStart::resolve(
                    consumer,
                    &scheduled,
                    initialization.start,
                    offset_reset,
                    budget,
                )?;
                assign_offsets_for_instance(
                    consumer,
                    &scheduled,
                    start.offsets(),
                    &mut assignment,
                    budget,
                )?;
                Ok(InitializedOffsets {
                    ready,
                    assignment,
                    record: start.into_record(&topic),
                })
            })
            .await
            .change_context(SourceError::Resume { connector: KAFKA })?;
        self.assignment = initialized.assignment;
        initialized
            .record
            .store(host)
            .await
            .change_context(SourceError::Resume { connector: KAFKA })?;
        self.observed_generation = Some(generation);
        Ok(initialized.ready)
    }
}

/// The partitions of a source topic, and which of them the domain's schedule assigns one instance.
struct ScheduledPartitions<'a> {
    topic: &'a str,
    partitions: &'a [i32],
    assigned: BTreeSet<i32>,
}

/// Where a partition that has no domain offset starts.
enum UnrecordedPartitionStart {
    /// At its first record at or after a paced domain's current logical time.
    At(Timestamp),
    /// Where the client's `auto.offset.reset` starts a partition without an offset.
    Reset(KafkaOffsetReset),
}

impl UnrecordedPartitionStart {
    /// The next offset each of `partitions` of `topic` starts at.
    fn starts(
        &self,
        consumer: &KafkaConsumer,
        topic: &str,
        partitions: &BTreeSet<i32>,
        budget: &ControlBudget<'_>,
    ) -> Result<BTreeMap<i32, i64>, Report<KafkaSourceError>> {
        if partitions.is_empty() {
            return Ok(BTreeMap::new());
        }
        match self {
            Self::At(timestamp) => offsets_at(consumer, topic, partitions, *timestamp, budget),
            Self::Reset(policy) => policy.starts(consumer, topic, partitions, budget),
        }
    }
}

/// Where the partitions a domain-offset instance reads start after it resumed.
enum DomainStart {
    /// The instance's partitions resume from their domain offsets; those in `unrecorded` had none
    /// and start where the domain's time or the client's reset policy places them.
    Resumed {
        offsets: BTreeMap<i32, i64>,
        unrecorded: BTreeSet<i32>,
    },
    /// A new start point places every partition of the topic, replacing the domain offsets.
    Reset { offsets: BTreeMap<i32, i64> },
}

impl DomainStart {
    fn resolve(
        consumer: &KafkaConsumer,
        scheduled: &ScheduledPartitions<'_>,
        start: KafkaDomainOffsetStart,
        offset_reset: KafkaOffsetReset,
        budget: &ControlBudget<'_>,
    ) -> Result<Self, Report<KafkaSourceError>> {
        match start {
            KafkaDomainOffsetStart::Resume {
                positions,
                missing_partition_timestamp,
            } => {
                let unrecorded_start = match missing_partition_timestamp {
                    Some(timestamp) => UnrecordedPartitionStart::At(timestamp),
                    None => UnrecordedPartitionStart::Reset(offset_reset),
                };
                Self::resume(consumer, scheduled, positions, &unrecorded_start, budget)
            }
            KafkaDomainOffsetStart::At(timestamp) => {
                let partitions = scheduled.partitions.iter().copied().collect();
                let offsets =
                    offsets_at(consumer, scheduled.topic, &partitions, timestamp, budget)?;
                Ok(Self::Reset { offsets })
            }
        }
    }

    /// Resumes each partition the schedule assigns the instance from its domain offset, clamped to
    /// what the partition still retains, and starts each one that has none where
    /// `unrecorded_start` places it.
    fn resume(
        consumer: &KafkaConsumer,
        scheduled: &ScheduledPartitions<'_>,
        positions: Vec<KafkaOffsetPosition>,
        unrecorded_start: &UnrecordedPartitionStart,
        budget: &ControlBudget<'_>,
    ) -> Result<Self, Report<KafkaSourceError>> {
        let mut recorded = BTreeMap::new();
        for position in positions {
            if position.topic == scheduled.topic {
                recorded.insert(position.partition, position.offset);
            }
        }
        let mut offsets = BTreeMap::new();
        let mut unrecorded = BTreeSet::new();
        for partition in scheduled.partitions {
            budget.remaining()?;
            if !scheduled.assigned.contains(partition) {
                continue;
            }
            let Some(next_offset) = recorded.get(partition) else {
                unrecorded.insert(*partition);
                continue;
            };
            let watermarks =
                PartitionWatermarks::fetch(consumer, scheduled.topic, *partition, budget)?;
            offsets.insert(
                *partition,
                (*next_offset).clamp(watermarks.low, watermarks.high),
            );
        }
        let starts = unrecorded_start.starts(consumer, scheduled.topic, &unrecorded, budget)?;
        offsets.extend(starts);
        Ok(Self::Resumed {
            offsets,
            unrecorded,
        })
    }

    fn offsets(&self) -> &BTreeMap<i32, i64> {
        match self {
            Self::Resumed { offsets, .. } | Self::Reset { offsets } => offsets,
        }
    }

    /// What the instance records in its domain offsets once it took its partitions up.
    fn into_record(self, topic: &str) -> DomainOffsetRecord {
        match self {
            Self::Resumed {
                offsets,
                unrecorded,
            } => {
                let mut starts = Vec::with_capacity(unrecorded.len());
                for (partition, offset) in offsets {
                    if unrecorded.contains(&partition) {
                        starts.push(KafkaOffsetPosition {
                            topic: topic.to_string(),
                            partition,
                            offset,
                        });
                    }
                }
                DomainOffsetRecord::Starts(starts)
            }
            Self::Reset { offsets } => {
                let mut positions = Vec::with_capacity(offsets.len());
                for (partition, offset) in offsets {
                    positions.push(KafkaOffsetPosition {
                        topic: topic.to_string(),
                        partition,
                        offset,
                    });
                }
                DomainOffsetRecord::Reset(positions)
            }
        }
    }
}

/// What a domain-offset instance records in its domain offsets once it took its partitions up.
enum DomainOffsetRecord {
    /// The start of each partition it took up without a domain offset, so a later resume restarts
    /// the partition there instead of placing it again, past what it has not acknowledged.
    Starts(Vec<KafkaOffsetPosition>),
    /// Every partition's start at a new start point, replacing what the domain offsets recorded.
    Reset(Vec<KafkaOffsetPosition>),
}

impl DomainOffsetRecord {
    async fn store(self, host: &KafkaDomainOffsetHost) -> KafkaDomainOffsetResult<()> {
        match self {
            Self::Starts(positions) => {
                for position in positions {
                    nervix_primitives::task::consume_budget().await;
                    host.commit(position).await?;
                }
                Ok(())
            }
            Self::Reset(positions) => host.reset(positions).await,
        }
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
    fn commit(self, consumer: &KafkaConsumer) -> KafkaResult<()> {
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
    /// The exact submitted commit, retained until this owner consumes its answer.
    unanswered: Option<CommitTask>,
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
        control: &KafkaControl,
        consumer: &Arc<KafkaConsumer>,
        group_id: &str,
        request: GroupCommitRequest,
    ) -> Result<(), Report<KafkaSourceError>> {
        if let Some(unanswered) = self.unanswered.as_mut() {
            let earlier = timeout(self.deadline, unanswered).await;
            let Ok(earlier) = earlier else {
                return Err(self.unanswered_failure(group_id));
            };
            self.unanswered = None;
            earlier.discarded(
                "the acknowledgement or leave that started this commit already timed out or was \
                 cancelled, so its late answer owes nothing",
            );
        }
        // Install before awaiting: cancellation of this future must not detach an untracked
        // commit and let the next request submit another one.
        self.unanswered = Some(control.start_commit(consumer, request)?);
        let pending = self
            .unanswered
            .as_mut()
            .verified("the exact commit was installed above");
        let answered = timeout(self.deadline, pending).await;
        let Ok(joined) = answered else {
            return Err(self.unanswered_failure(group_id));
        };
        self.unanswered = None;
        let executed = joined.change_context(KafkaSourceError::CommitTask {
            group: group_id.to_owned(),
        })?;
        let committed = executed.change_context(KafkaSourceError::ControlExecution)?;
        committed.change_context(KafkaSourceError::CommitOffset {
            group: group_id.to_owned(),
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
        control: &KafkaControl,
        consumer: &Arc<KafkaConsumer>,
        group_id: &str,
        commit: KafkaGroupCommit,
    ) -> Result<(), Report<KafkaSourceError>> {
        let committed = match commit {
            KafkaGroupCommit::DriverCadence => {
                self.commit(control, consumer, group_id, GroupCommitRequest::Delivered)
                    .await
            }
            KafkaGroupCommit::EachAcknowledgedBatch => Ok(()),
        };
        let left = control
            .run(consumer, move |consumer, _budget| {
                consumer.unsubscribe();
                Ok(())
            })
            .await;
        match committed {
            Ok(()) => left,
            Err(error) => {
                left.reported("unsubscribe after a failed consumer-group commit");
                Err(error)
            }
        }
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
    consumer: &KafkaConsumer,
    topic: &str,
    budget: &ControlBudget<'_>,
) -> Result<Vec<i32>, Report<KafkaSourceError>> {
    let metadata = consumer
        .fetch_metadata(Some(topic), budget.remaining()?)
        .map_err(|source| {
            Report::new(source).change_context(KafkaSourceError::FetchMetadata {
                topic: topic.to_string(),
            })
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

/// The next offset of each of `partitions` of `topic` at `timestamp`: its first record at or after
/// the timestamp, or, when it has none yet, its end as it stood before the lookup, so a record
/// produced after the lookup is read rather than skipped.
fn offsets_at(
    consumer: &KafkaConsumer,
    topic: &str,
    partitions: &BTreeSet<i32>,
    timestamp: Timestamp,
    budget: &ControlBudget<'_>,
) -> Result<BTreeMap<i32, i64>, Report<KafkaSourceError>> {
    let timestamp_ms = timestamp.unix_nanos().div_euclid(1_000_000);
    let mut ends = BTreeMap::new();
    let mut query = TopicPartitionList::new();
    for partition in partitions {
        budget.remaining()?;
        let watermarks = PartitionWatermarks::fetch(consumer, topic, *partition, budget)?;
        ends.insert(*partition, watermarks.high);
        query
            .add_partition_offset(topic, *partition, Offset::Offset(timestamp_ms))
            .map_err(|source| {
                Report::new(source).change_context(KafkaSourceError::BuildTimestampQuery {
                    topic: topic.to_string(),
                    partition: *partition,
                })
            })?;
    }
    let resolved = consumer
        .offsets_for_times(query, budget.remaining()?)
        .map_err(|source| {
            Report::new(source).change_context(KafkaSourceError::ResolveTimestampOffsets {
                topic: topic.to_string(),
            })
        })?;
    let mut offsets = BTreeMap::new();
    for element in resolved.elements() {
        budget.remaining()?;
        let partition = element.partition();
        element.error().map_err(|source| {
            Report::new(source).change_context(KafkaSourceError::ResolveTimestampOffsets {
                topic: topic.to_string(),
            })
        })?;
        // The lookup answers only for the partitions it was asked about.
        let Some(end) = ends.get(&partition) else {
            return Err(Report::new(KafkaSourceError::ResolveTimestampOffsets {
                topic: topic.to_string(),
            }));
        };
        let offset = match element.offset() {
            Offset::Offset(offset) => offset,
            // No record of the partition is at or after the timestamp yet.
            Offset::End | Offset::Invalid => *end,
            Offset::Beginning | Offset::Stored | Offset::OffsetTail(_) => {
                return Err(Report::new(KafkaSourceError::UnsupportedDomainOffset {
                    topic: topic.to_string(),
                    partition,
                }));
            }
        };
        offsets.insert(partition, offset);
    }
    Ok(offsets)
}

/// The partitions the domain's schedule assigns instance `instance_index`, none without a schedule.
fn scheduled_partitions(
    schedule: Option<&KafkaPartitionSchedule>,
    instance_index: u64,
) -> BTreeSet<i32> {
    let Some(schedule) = schedule else {
        return BTreeSet::new();
    };
    let Ok(instance_index) = usize::try_from(instance_index) else {
        return BTreeSet::new();
    };
    let Some(assignments) = schedule.instance_assignments.get(instance_index) else {
        return BTreeSet::new();
    };
    assignments.iter().copied().collect()
}

/// Assigns the consumer each partition `scheduled` gives the instance, at its offset in `offsets`.
fn assign_offsets_for_instance(
    consumer: &KafkaConsumer,
    scheduled: &ScheduledPartitions<'_>,
    offsets: &BTreeMap<i32, i64>,
    consumer_assignment: &mut KafkaConsumerAssignment,
    budget: &ControlBudget<'_>,
) -> Result<(), Report<KafkaSourceError>> {
    let mut assignment = TopicPartitionList::new();
    for (partition, offset) in offsets {
        budget.remaining()?;
        if scheduled.assigned.contains(partition) {
            assignment
                .add_partition_offset(scheduled.topic, *partition, Offset::Offset(*offset))
                .map_err(|source| {
                    Report::new(source).change_context(KafkaSourceError::BuildAssignment {
                        topic: scheduled.topic.to_string(),
                        partition: *partition,
                    })
                })?;
        }
    }
    budget.remaining()?;
    consumer_assignment.apply(consumer, scheduled.topic, &assignment)
}

pub struct TopicPartitionInspector {
    control: KafkaControl,
    consumer: Arc<KafkaConsumer>,
}

impl TopicPartitionInspector {
    pub fn new(
        config: &[ClientConfigEntry],
        group_id: String,
        executor: Executor,
    ) -> Result<Self, Report<KafkaSourceError>> {
        let mut client_config = ClientConfig::new();
        for entry in config {
            client_config.set(&entry.key, &entry.value);
        }
        client_config.set("group.id", group_id);
        client_config.set("enable.partition.eof", "false");
        client_config.set("enable.auto.commit", "false");
        let control = KafkaControl::new(&client_config, executor)?;
        let consumer = client_config
            .create_with_context(KafkaConsumerContext::default())
            .map_err(|source| Report::new(source).change_context(KafkaSourceError::Initialize))?;
        Ok(Self {
            control,
            consumer: Arc::new(consumer),
        })
    }

    pub async fn partitions(&self, topic: &str) -> Result<Vec<i32>, Report<KafkaSourceError>> {
        let topic = topic.to_owned();
        self.control
            .run(&self.consumer, move |consumer, budget| {
                topic_partitions(consumer, &topic, budget)
            })
            .await
            .change_context(KafkaSourceError::InspectPartitions)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "source_control_tests.rs"]
mod control_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "source_rejection_tests.rs"]
mod rejection_tests;
