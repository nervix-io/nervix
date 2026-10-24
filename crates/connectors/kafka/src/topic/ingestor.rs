//! Kafka ingestor transport and host-owned domain-offset services.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Kafka consumer configuration, polling, headers and metadata, partition assignment,
//!   offset acknowledgement, partition inspection, consumer retirement, and the opaque services
//!   through which a ingestor reads and advances host-owned domain offsets.
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

#[path = "ingestor_control.rs"]
mod control;
#[path = "ingestor_retirement.rs"]
mod retirement;

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    time::Duration,
};

use async_trait::async_trait;
use control::{CommitTask, ControlBudget, KafkaControl};
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::{
    BrokerIngestorConnector, IngestMessageHeaders, IngestMetadataRow, IngestorAckPolicy,
    IngestorBatch, IngestorBatchRequest, IngestorConnector, IngestorError, IngestorMessage,
    IngestorReadCancellation, IngestorResult, IngestorResume,
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
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::{KafkaError, KafkaResult, RDKafkaErrorCode},
    message::{Headers, Message as KafkaMessage},
    topic_partition_list::{Offset, TopicPartitionList},
};
use retirement::KafkaConsumerHandle;
use thiserror::Error;
use tracing::{debug, warn};

const KAFKA: &str = "kafka";
const DOMAIN_ASSIGNMENT_RETRY: Duration = Duration::from_millis(100);

/// How long a consumer-group commit waits for its coordinator's answer.
///
/// librdkafka keeps retrying a commit it could not send for as long as the group has no reachable
/// coordinator, so the ingestor stops waiting at this deadline and the commit fails. The commit stays
/// the instance's one commit in flight, which the next commit waits for first.
const COMMIT_ANSWER_DEADLINE: Duration = Duration::from_secs(30);

/// The next unread offset for one Kafka topic partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaOffsetPosition {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Where a domain-owned Kafka ingestor resolves its next assignment from.
pub enum KafkaDomainOffsetStart {
    Resume {
        positions: Vec<KafkaOffsetPosition>,
        missing_partition_timestamp: Option<Timestamp>,
    },
    At(Timestamp),
}

/// Host-owned state needed to initialize one domain-offset ingestor instance.
pub struct KafkaDomainOffsetInitialization {
    pub generation: u64,
    pub start: KafkaDomainOffsetStart,
    pub schedule: Option<KafkaPartitionSchedule>,
}

/// Why host-owned Kafka offset state could not serve the ingestor connector.
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

/// How one Kafka ingestor records the offsets it has accepted.
#[derive(Clone)]
pub enum KafkaTopicIngestorOffsetMode {
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

impl KafkaTopicIngestorOffsetMode {
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
    /// An `ACK` mode: the ingestor commits each acknowledged batch's positions and acknowledging
    /// returns once the group's coordinator answered the commit.
    EachAcknowledgedBatch,
}

impl KafkaGroupCommit {
    /// Who commits under `acknowledgement`.
    pub fn for_acknowledgement(acknowledgement: IngestorAckPolicy) -> Self {
        match acknowledgement {
            IngestorAckPolicy::None => Self::DriverCadence,
            IngestorAckPolicy::Sequential { .. } | IngestorAckPolicy::Parallel { .. } => {
                Self::EachAcknowledgedBatch
            }
        }
    }
}

/// The complete connector-owned plan for opening Kafka ingestor instances.
#[derive(Clone)]
pub struct KafkaTopicIngestorPlan {
    /// The node-owned executor used by this ingestor and its inspectors.
    pub executor: Executor,
    pub config: Vec<ClientConfigEntry>,
    pub topic: KafkaTopicName,
    pub offset_mode: KafkaTopicIngestorOffsetMode,
}

#[derive(Debug, Error)]
pub enum KafkaTopicIngestorError {
    #[error("the {connector} source cannot admit its control call", connector = ConnectorKind::Kafka)]
    ControlAdmission,
    #[error("the {connector} source control call could not run", connector = ConnectorKind::Kafka)]
    ControlExecution,
    #[error("the {connector} control call did not finish within socket.timeout.ms {bound}", connector = ConnectorKind::Kafka, bound = humantime::format_duration(*timeout))]
    ControlTimeout { timeout: Duration },
    #[error("the {connector} control call was cancelled", connector = ConnectorKind::Kafka)]
    ControlCancelled,
    #[error(
        "the {connector} consumer cannot reserve the job that will destroy it",
        connector = ConnectorKind::Kafka
    )]
    RetirementAdmission,
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
    /// Nervix provisions nothing in Kafka, so a consumer reports a topic the broker does not have
    /// rather than letting the broker create it.
    #[error("{connector} topic '{topic}' does not exist", connector = ConnectorKind::Kafka)]
    MissingTopic { topic: String },
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

impl KafkaTopicIngestorError {
    /// Whether the failure is a topic the broker does not have.
    pub fn is_missing_topic(&self) -> bool {
        matches!(self, Self::MissingTopic { .. })
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
        consumer: &StreamConsumer,
        topic: &str,
        assignment: &TopicPartitionList,
    ) -> Result<(), Report<KafkaTopicIngestorError>> {
        if self.is_current(assignment) {
            return Ok(());
        }
        if assignment.count() == 0 {
            return self.clear(consumer, topic);
        }
        consumer.assign(assignment).map_err(|source| {
            Report::new(source).change_context(KafkaTopicIngestorError::Assign {
                topic: topic.to_string(),
            })
        })?;
        *self = Self::Partitions(assignment.clone());
        Ok(())
    }

    fn clear(
        &mut self,
        consumer: &StreamConsumer,
        topic: &str,
    ) -> Result<(), Report<KafkaTopicIngestorError>> {
        if *self == Self::Cleared {
            return Ok(());
        }
        consumer.unassign().map_err(|source| {
            Report::new(source).change_context(KafkaTopicIngestorError::Unassign {
                topic: topic.to_string(),
            })
        })?;
        *self = Self::Cleared;
        Ok(())
    }
}

pub struct KafkaTopicIngestorMessage {
    message: rdkafka::message::OwnedMessage,
    position: KafkaOffsetPosition,
}

impl KafkaTopicIngestorMessage {
    fn from_message(
        message: rdkafka::message::OwnedMessage,
    ) -> Result<Self, Report<KafkaTopicIngestorError>> {
        let next_offset = message.offset().checked_add(1).ok_or_else(|| {
            Report::new(KafkaTopicIngestorError::OffsetOverflow {
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

impl IngestMessageHeaders for KafkaTopicIngestorMessage {
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

impl IngestorMessage for KafkaTopicIngestorMessage {
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

pub struct KafkaTopicIngestor {
    control: KafkaControl,
    /// Shared with the driver jobs the ingestor admits. A commit in flight keeps the consumer open
    /// until librdkafka answers it, and whichever holder lets it go last retires it.
    consumer: KafkaConsumerHandle,
    topic: nervix_models::KafkaTopicName,
    offset_mode: KafkaTopicIngestorOffsetMode,
    commits: GroupCommits,
    instance_index: u64,
    assignment: KafkaConsumerAssignment,
    observed_generation: Option<u64>,
}

#[async_trait]
impl IngestorConnector for KafkaTopicIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);

    type Plan = KafkaTopicIngestorPlan;

    async fn open(plan: &Self::Plan, instance_index: u64) -> IngestorResult<Self> {
        let group_id = match &plan.offset_mode {
            KafkaTopicIngestorOffsetMode::ConsumerGroup { group_id, .. }
            | KafkaTopicIngestorOffsetMode::Domain { group_id, .. } => group_id,
        };
        let config = consumer_config(&plan.config, group_id, plan.offset_mode.driver_commits());
        let control = KafkaControl::new(&config, plan.executor.clone())
            .change_context(IngestorError::Open { connector: KAFKA })?;
        let consumer = KafkaConsumerHandle::create(&config, plan.executor.clone())
            .change_context(IngestorError::Open { connector: KAFKA })?;
        Ok(Self {
            control,
            consumer,
            topic: plan.topic.clone(),
            offset_mode: plan.offset_mode.clone(),
            commits: GroupCommits::new(COMMIT_ANSWER_DEADLINE),
            instance_index,
            assignment: KafkaConsumerAssignment::default(),
            observed_generation: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        let KafkaTopicIngestorOffsetMode::Domain {
            offsets, rebalance, ..
        } = &mut self.offset_mode
        else {
            return false;
        };
        let generation_changed = offsets.generation() != self.observed_generation;
        let rebalance_changed = rebalance.has_changed().unwrap_or(false);
        generation_changed || rebalance_changed
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        match &self.offset_mode {
            KafkaTopicIngestorOffsetMode::ConsumerGroup { group_id, commit } => self
                .commits
                .leave_group(&self.control, &self.consumer, group_id, *commit)
                .await
                .change_context(IngestorError::Suspend { connector: KAFKA }),
            KafkaTopicIngestorOffsetMode::Domain { .. } => self
                .clear_assignment()
                .await
                .change_context(IngestorError::Suspend { connector: KAFKA }),
        }
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if let KafkaTopicIngestorOffsetMode::ConsumerGroup { .. } = &self.offset_mode {
            let topic = self.topic.as_str().to_owned();
            self.control
                .run(&self.consumer, move |consumer, _budget| {
                    consumer
                        .subscribe(&[&topic])
                        .change_context(KafkaTopicIngestorError::Subscribe { topic })
                })
                .await
                .change_context(IngestorError::Resume { connector: KAFKA })?;
            return Ok(IngestorResume::Ready);
        }

        let offsets = match &mut self.offset_mode {
            KafkaTopicIngestorOffsetMode::Domain {
                offsets, rebalance, ..
            } => {
                rebalance.borrow_and_update();
                offsets.clone()
            }
            KafkaTopicIngestorOffsetMode::ConsumerGroup { .. } => {
                return Ok(IngestorResume::Ready);
            }
        };
        let ready = self
            .initialize_domain_offsets(&offsets)
            .await
            .change_context(IngestorError::Resume { connector: KAFKA })?;
        if ready {
            Ok(IngestorResume::Ready)
        } else {
            Ok(IngestorResume::Waiting {
                retry_after: DOMAIN_ASSIGNMENT_RETRY,
            })
        }
    }

    async fn close(&mut self) -> IngestorResult<()> {
        match &self.offset_mode {
            KafkaTopicIngestorOffsetMode::ConsumerGroup { group_id, commit } => self
                .commits
                .leave_group(&self.control, &self.consumer, group_id, *commit)
                .await
                .change_context(IngestorError::Close { connector: KAFKA }),
            KafkaTopicIngestorOffsetMode::Domain { .. } => self
                .clear_assignment()
                .await
                .change_context(IngestorError::Close { connector: KAFKA }),
        }
    }
}

#[async_trait]
impl BrokerIngestorConnector for KafkaTopicIngestor {
    type Message = KafkaTopicIngestorMessage;
    type Position = KafkaOffsetPosition;

    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        let first = match &mut self.offset_mode {
            KafkaTopicIngestorOffsetMode::Domain { rebalance, .. } => {
                nervix_primitives::select! {
                    changed = rebalance.changed() => {
                        return if changed.is_ok() {
                            Ok(IngestorBatch::ResumeRequired)
                        } else {
                            Ok(IngestorBatch::Closed)
                        };
                    }
                    message = self.consumer.recv() => message,
                    () = cancellation.cancelled() => {
                        return Ok(IngestorBatch::Interrupted {
                            gathered: Vec::new(),
                        });
                    }
                }
            }
            KafkaTopicIngestorOffsetMode::ConsumerGroup { .. } => {
                let Some(message) = cancellation.unless_cancelled(self.consumer.recv()).await
                else {
                    return Ok(IngestorBatch::Interrupted {
                        gathered: Vec::new(),
                    });
                };
                message
            }
        };
        let first = first
            .map_err(|source| self.receive_failure(source))
            .change_context(IngestorError::Read { connector: KAFKA })?
            .detach();
        let first = KafkaTopicIngestorMessage::from_message(first)
            .change_context(IngestorError::Read { connector: KAFKA })?;
        let mut messages = Vec::with_capacity(request.max_messages.get());
        messages.push(first);
        if request.max_messages == NonZeroUsize::MIN {
            return Ok(IngestorBatch::Messages(messages));
        }
        let Some(batch_timeout) = request.batch_timeout else {
            return Ok(IngestorBatch::Messages(messages));
        };
        let deadline = Instant::now().checked_add(batch_timeout).ok_or_else(|| {
            Report::new(IngestorError::Read { connector: KAFKA })
                .attach(KafkaTopicIngestorError::BatchDeadline)
        })?;
        while messages.len() < request.max_messages.get() {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                _ = sleep_until(deadline) => break,
                next = self.consumer.recv() => {
                    match next {
                        Ok(message) => {
                            let message = KafkaTopicIngestorMessage::from_message(message.detach())
                                .change_context(IngestorError::Read { connector: KAFKA })?;
                            messages.push(message);
                        }
                        Err(error) => {
                            warn!(error = %error, "failed to receive another Kafka batch message");
                        }
                    }
                }
                () = cancellation.cancelled() => {
                    return Ok(IngestorBatch::Interrupted { gathered: messages });
                }
            }
        }
        Ok(IngestorBatch::Messages(messages))
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let positions = latest_positions(positions);
        match &self.offset_mode {
            // The driver commits the positions it delivered on its own cadence, so acknowledging
            // them confirms nothing on the transport.
            KafkaTopicIngestorOffsetMode::ConsumerGroup {
                commit: KafkaGroupCommit::DriverCadence,
                ..
            } => Ok(()),
            KafkaTopicIngestorOffsetMode::ConsumerGroup {
                group_id,
                commit: KafkaGroupCommit::EachAcknowledgedBatch,
            } => {
                let request = GroupCommitRequest::acknowledged(&positions)
                    .change_context(IngestorError::Acknowledge { connector: KAFKA })?;
                self.commits
                    .commit(&self.control, &self.consumer, group_id, request)
                    .await
                    .change_context(IngestorError::Acknowledge { connector: KAFKA })
            }
            KafkaTopicIngestorOffsetMode::Domain { offsets, .. } => {
                for position in positions {
                    nervix_primitives::task::consume_budget().await;
                    offsets
                        .commit(position)
                        .await
                        .change_context(IngestorError::Acknowledge { connector: KAFKA })?;
                }
                Ok(())
            }
        }
    }

    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let starts = earliest_message_offsets(positions)
            .change_context(IngestorError::Reject { connector: KAFKA })?;
        let topic = self.topic.as_str().to_owned();
        self.control
            .run(&self.consumer, move |consumer, budget| {
                let assignment = consumer.assignment().change_context(
                    KafkaTopicIngestorError::ReadAssignment {
                        topic: topic.clone(),
                    },
                )?;
                let assigned = AssignedPartitions::of_topic(&topic, &assignment);
                for position in starts {
                    budget.remaining()?;
                    if !assigned.contains(&position) {
                        debug!(
                            topic = position.topic.as_str(),
                            partition = position.partition,
                            offset = position.offset,
                            "rejected Kafka partition is no longer assigned; its next holder \
                             resumes it from the committed offset"
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
                        .change_context(KafkaTopicIngestorError::Seek {
                            topic: position.topic,
                            partition: position.partition,
                        })?;
                }
                Ok(())
            })
            .await
            .change_context(IngestorError::Reject { connector: KAFKA })
    }
}

impl KafkaTopicIngestor {
    /// The failure a receive reports. A topic the broker does not have is reported by name, since
    /// the consumer never lets the broker create it.
    fn receive_failure(&self, source: KafkaError) -> Report<KafkaTopicIngestorError> {
        if let KafkaError::MessageConsumption(code) = &source
            && names_missing_topic(*code)
        {
            return Report::new(KafkaTopicIngestorError::MissingTopic {
                topic: self.topic.as_str().to_string(),
            })
            .attach(source);
        }
        Report::new(source).change_context(KafkaTopicIngestorError::Initialize)
    }

    async fn clear_assignment(&mut self) -> Result<(), Report<KafkaTopicIngestorError>> {
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
    ) -> IngestorResult<bool> {
        let topic = self.topic.as_str().to_owned();
        let partitions = self
            .control
            .run(&self.consumer, move |consumer, budget| {
                topic_partitions(consumer, &topic, budget)
            })
            .await
            .change_context(IngestorError::Resume { connector: KAFKA })?;
        let initialization = host
            .initialization(&partitions)
            .await
            .change_context(IngestorError::Resume { connector: KAFKA })?;
        let generation = initialization.generation;
        let topic = self.topic.as_str().to_owned();
        let instance_index = self.instance_index;
        let mut assignment = std::mem::take(&mut self.assignment);
        struct InitializedOffsets {
            ready: bool,
            assignment: KafkaConsumerAssignment,
            reset: Option<Vec<KafkaOffsetPosition>>,
        }
        let initialized = self
            .control
            .run(&self.consumer, move |consumer, budget| {
                let is_new_start = matches!(initialization.start, KafkaDomainOffsetStart::At(_));
                let offsets = match initialization.start {
                    KafkaDomainOffsetStart::Resume {
                        positions,
                        missing_partition_timestamp,
                    } => resume_offsets(
                        consumer,
                        &topic,
                        &partitions,
                        positions,
                        missing_partition_timestamp,
                        budget,
                    )?,
                    KafkaDomainOffsetStart::At(timestamp) => offsets_for_partitions_by_timestamp(
                        consumer, &topic, partitions, timestamp, budget,
                    )?,
                };
                let ready = assign_offsets_for_instance(
                    consumer,
                    &topic,
                    &offsets,
                    initialization.schedule.as_ref(),
                    instance_index,
                    &mut assignment,
                    budget,
                )?;
                let reset = if is_new_start {
                    Some(concrete_next_offsets_from_assignment(
                        consumer, &topic, &offsets, budget,
                    )?)
                } else {
                    None
                };
                Ok(InitializedOffsets {
                    ready,
                    assignment,
                    reset,
                })
            })
            .await
            .change_context(IngestorError::Resume { connector: KAFKA })?;
        self.assignment = initialized.assignment;
        if let Some(positions) = initialized.reset {
            host.reset(positions)
                .await
                .change_context(IngestorError::Resume { connector: KAFKA })?;
        }
        self.observed_generation = Some(generation);
        Ok(initialized.ready)
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
    fn acknowledged(
        positions: &[KafkaOffsetPosition],
    ) -> Result<Self, Report<KafkaTopicIngestorError>> {
        let mut offsets = TopicPartitionList::new();
        for position in positions {
            offsets
                .add_partition_offset(
                    &position.topic,
                    position.partition,
                    Offset::Offset(position.offset),
                )
                .map_err(|source| {
                    Report::new(source).change_context(KafkaTopicIngestorError::BuildOffsetCommit {
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

/// One ingestor instance's consumer-group commits, which run one at a time.
///
/// A commit parks a blocking-pool thread until its coordinator answers, and the ingestor waits for
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
        consumer: &KafkaConsumerHandle,
        group_id: &str,
        request: GroupCommitRequest,
    ) -> Result<(), Report<KafkaTopicIngestorError>> {
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
        let executed = joined.change_context(KafkaTopicIngestorError::CommitTask {
            group: group_id.to_owned(),
        })?;
        let committed = executed.change_context(KafkaTopicIngestorError::ControlExecution)?;
        committed.change_context(KafkaTopicIngestorError::CommitOffset {
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
        consumer: &KafkaConsumerHandle,
        group_id: &str,
        commit: KafkaGroupCommit,
    ) -> Result<(), Report<KafkaTopicIngestorError>> {
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

    fn unanswered_failure(&self, group_id: &str) -> Report<KafkaTopicIngestorError> {
        Report::new(KafkaTopicIngestorError::CommitUnanswered {
            group: group_id.to_string(),
            deadline: self.deadline,
        })
    }
}

/// The partitions of the ingestor topic that the consumer's current assignment holds.
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
/// ingestor commits.
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
/// ingestor reads again.
pub fn earliest_message_offsets(
    positions: &[KafkaOffsetPosition],
) -> Result<Vec<KafkaOffsetPosition>, Report<KafkaTopicIngestorError>> {
    let mut topics = BTreeMap::<String, BTreeMap<i32, i64>>::new();
    for position in positions {
        let offset = position.offset.checked_sub(1).ok_or_else(|| {
            Report::new(KafkaTopicIngestorError::UnsupportedDomainOffset {
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

/// The configuration of every Kafka consumer Nervix runs: the client's pass-through entries, then
/// the keys Nervix decides, which override any entry of the same name. A consumer never lets the
/// broker create a topic it names, because Nervix provisions nothing in an external system.
fn consumer_config(
    entries: &[ClientConfigEntry],
    group_id: &str,
    enable_auto_commit: bool,
) -> ClientConfig {
    let mut config = ClientConfig::new();
    for entry in entries {
        config.set(&entry.key, &entry.value);
    }
    let auto_commit = if enable_auto_commit { "true" } else { "false" };
    config.set("group.id", group_id);
    config.set("enable.partition.eof", "false");
    config.set("enable.auto.commit", auto_commit);
    config.set("allow.auto.create.topics", "false");
    config
}

/// Whether a broker or driver error code says the topic does not exist. librdkafka reports a
/// subscribed topic the broker lacks as the broker's own code, and a topic its metadata cache lacks
/// as the local one.
fn names_missing_topic(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic
    )
}

fn topic_partitions(
    consumer: &StreamConsumer,
    topic: &str,
    budget: &ControlBudget<'_>,
) -> Result<Vec<i32>, Report<KafkaTopicIngestorError>> {
    let metadata = consumer
        .fetch_metadata(Some(topic), budget.remaining()?)
        .map_err(|source| {
            Report::new(source).change_context(KafkaTopicIngestorError::FetchMetadata {
                topic: topic.to_string(),
            })
        })?;
    let Some(topic_metadata) = metadata.topics().iter().find(|entry| entry.name() == topic) else {
        return Err(Report::new(KafkaTopicIngestorError::MissingMetadata {
            topic: topic.to_string(),
        }));
    };
    if let Some(error) = topic_metadata.error()
        && names_missing_topic(error.into())
    {
        return Err(Report::new(KafkaTopicIngestorError::MissingTopic {
            topic: topic.to_string(),
        })
        .attach(KafkaError::MetadataFetch(error.into())));
    }
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
    budget: &ControlBudget<'_>,
) -> Result<BTreeMap<i32, Offset>, Report<KafkaTopicIngestorError>>
where
    I: IntoIterator<Item = i32>,
{
    let mut query = TopicPartitionList::new();
    let timestamp_ms = timestamp.unix_nanos().div_euclid(1_000_000);
    for partition in partitions {
        budget.remaining()?;
        query
            .add_partition_offset(topic, partition, Offset::Offset(timestamp_ms))
            .map_err(|source| {
                Report::new(source).change_context(KafkaTopicIngestorError::BuildTimestampQuery {
                    topic: topic.to_string(),
                    partition,
                })
            })?;
    }
    let resolved = consumer
        .offsets_for_times(query, budget.remaining()?)
        .map_err(|source| {
            Report::new(source).change_context(KafkaTopicIngestorError::ResolveTimestampOffsets {
                topic: topic.to_string(),
            })
        })?;
    let mut offsets = BTreeMap::new();
    for element in resolved.elements() {
        budget.remaining()?;
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
    budget: &ControlBudget<'_>,
) -> Result<Offset, Report<KafkaTopicIngestorError>> {
    let (low, high) = consumer
        .fetch_watermarks(topic, partition, budget.remaining()?)
        .map_err(|source| {
            Report::new(source).change_context(KafkaTopicIngestorError::FetchWatermarks {
                topic: topic.to_string(),
                partition,
            })
        })?;
    Ok(Offset::Offset(next_offset.clamp(low, high)))
}

fn resume_offsets(
    consumer: &StreamConsumer,
    topic: &str,
    partitions: &[i32],
    positions: Vec<KafkaOffsetPosition>,
    missing_partition_timestamp: Option<Timestamp>,
    budget: &ControlBudget<'_>,
) -> Result<BTreeMap<i32, Offset>, Report<KafkaTopicIngestorError>> {
    let mut recorded = BTreeMap::new();
    for position in positions {
        if position.topic == topic {
            recorded.insert(position.partition, position.offset);
        }
    }
    let mut offsets = BTreeMap::new();
    let mut missing = Vec::new();
    for partition in partitions {
        budget.remaining()?;
        if let Some(next_offset) = recorded.get(partition) {
            offsets.insert(
                *partition,
                normalized_resume_offset(consumer, topic, *partition, *next_offset, budget)?,
            );
        } else {
            missing.push(*partition);
        }
    }
    if let Some(timestamp) = missing_partition_timestamp {
        offsets.extend(offsets_for_partitions_by_timestamp(
            consumer, topic, missing, timestamp, budget,
        )?);
    } else {
        for partition in missing {
            budget.remaining()?;
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
    budget: &ControlBudget<'_>,
) -> Result<bool, Report<KafkaTopicIngestorError>> {
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
        budget.remaining()?;
        if assigned_partitions.contains(partition) {
            assignment
                .add_partition_offset(topic, *partition, *offset)
                .map_err(|source| {
                    Report::new(source).change_context(KafkaTopicIngestorError::BuildAssignment {
                        topic: topic.to_string(),
                        partition: *partition,
                    })
                })?;
        }
    }
    budget.remaining()?;
    consumer_assignment.apply(consumer, topic, &assignment)?;
    Ok(has_topic_partitions)
}

fn concrete_next_offsets_from_assignment(
    consumer: &StreamConsumer,
    topic: &str,
    offsets: &BTreeMap<i32, Offset>,
    budget: &ControlBudget<'_>,
) -> Result<Vec<KafkaOffsetPosition>, Report<KafkaTopicIngestorError>> {
    let mut concrete = Vec::with_capacity(offsets.len());
    for (partition, offset) in offsets {
        budget.remaining()?;
        let next_offset = match offset {
            Offset::Offset(value) => *value,
            Offset::Beginning => {
                let mapping_input = consumer
                    .fetch_watermarks(topic, *partition, budget.remaining()?)
                    .map(|(low, _)| low);
                match mapping_input {
                    Ok(mapped_value) => Ok(mapped_value),
                    Err(source) => Err(Report::new(source).change_context(
                        KafkaTopicIngestorError::FetchWatermarks {
                            topic: topic.to_string(),
                            partition: *partition,
                        },
                    )),
                }
            }?,
            Offset::End | Offset::Invalid => {
                let mapping_input = consumer
                    .fetch_watermarks(topic, *partition, budget.remaining()?)
                    .map(|(_, high)| high);
                match mapping_input {
                    Ok(mapped_value) => Ok(mapped_value),
                    Err(source) => Err(Report::new(source).change_context(
                        KafkaTopicIngestorError::FetchWatermarks {
                            topic: topic.to_string(),
                            partition: *partition,
                        },
                    )),
                }
            }?,
            Offset::Stored | Offset::OffsetTail(_) => {
                return Err(Report::new(
                    KafkaTopicIngestorError::UnsupportedDomainOffset {
                        topic: topic.to_string(),
                        partition: *partition,
                    },
                ));
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
    control: KafkaControl,
    consumer: KafkaConsumerHandle,
}

impl TopicPartitionInspector {
    pub fn new(
        config: &[ClientConfigEntry],
        group_id: String,
        executor: Executor,
    ) -> Result<Self, Report<KafkaTopicIngestorError>> {
        let client_config = consumer_config(config, &group_id, false);
        let control = KafkaControl::new(&client_config, executor.clone())?;
        let consumer = KafkaConsumerHandle::create(&client_config, executor)?;
        Ok(Self { control, consumer })
    }

    pub async fn partitions(
        &self,
        topic: &str,
    ) -> Result<Vec<i32>, Report<KafkaTopicIngestorError>> {
        let topic = topic.to_owned();
        self.control
            .run(&self.consumer, move |consumer, budget| {
                topic_partitions(consumer, &topic, budget)
            })
            .await
            .map_err(|report| {
                if report.current_context().is_missing_topic() {
                    report
                } else {
                    report.change_context(KafkaTopicIngestorError::InspectPartitions)
                }
            })
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
    /// emitter writes keep every write in the order written, and a reader of the produced record
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
            Some(crate::KafkaTopicEmitter::owned_headers(&written)),
        );
        let message = KafkaTopicIngestorMessage::from_message(message)
            .assured("offset zero has a next offset");
        let mut visited = Vec::new();
        message.visit(&mut |name, value| visited.push(format!("{name}={value}")));
        assert_eq!(visited, ["trace=a", "Tenant=x", "trace=b", "tenant=y"]);
    }

    #[test]
    fn a_consumer_never_lets_the_broker_create_a_topic_whatever_the_client_sets() {
        let entries = [
            ClientConfigEntry {
                key: "bootstrap.servers".to_string(),
                value: "127.0.0.1:9092".to_string(),
            },
            ClientConfigEntry {
                key: "allow.auto.create.topics".to_string(),
                value: "true".to_string(),
            },
            ClientConfigEntry {
                key: "group.id".to_string(),
                value: "operator".to_string(),
            },
        ];

        let config = consumer_config(&entries, "nervix_domain_orders_events", true);

        assert_eq!(config.get("allow.auto.create.topics"), Some("false"));
        assert_eq!(config.get("group.id"), Some("nervix_domain_orders_events"));
        assert_eq!(config.get("enable.auto.commit"), Some("true"));
        assert_eq!(config.get("enable.partition.eof"), Some("false"));
        assert_eq!(config.get("bootstrap.servers"), Some("127.0.0.1:9092"));
        let manual = consumer_config(&entries, "nervix_domain_orders_events", false);
        assert_eq!(manual.get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn only_an_unknown_topic_names_a_missing_topic() {
        assert!(names_missing_topic(
            RDKafkaErrorCode::UnknownTopicOrPartition
        ));
        assert!(names_missing_topic(RDKafkaErrorCode::UnknownTopic));
        for code in [
            RDKafkaErrorCode::UnknownPartition,
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::LeaderNotAvailable,
            RDKafkaErrorCode::AllBrokersDown,
        ] {
            assert!(!names_missing_topic(code), "{code:?}");
        }
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
                            KafkaTopicIngestorError::UnsupportedDomainOffset { .. }
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
#[path = "ingestor_commit_tests.rs"]
mod commit_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "ingestor_read_tests.rs"]
mod read_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "ingestor_control_tests.rs"]
mod control_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "ingestor_retirement_tests.rs"]
mod retirement_tests;
