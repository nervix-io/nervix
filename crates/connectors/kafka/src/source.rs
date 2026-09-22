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

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    time::Duration,
};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceResult, SourceResume,
};
use nervix_models::{ClientConfigEntry, KafkaPartitionSchedule, Timestamp, TopicName};
use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{Headers, Message as KafkaMessage},
    topic_partition_list::{Offset, TopicPartitionList},
};
use thiserror::Error;
use tokio::{
    sync::watch,
    time::{Instant, sleep_until},
};
use tracing::warn;
use triomphe::Arc;

const KAFKA: &str = "kafka";
const DOMAIN_ASSIGNMENT_RETRY: Duration = Duration::from_millis(100);

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
pub trait KafkaDomainOffsetServices: Send + Sync + 'static {
    fn generation(&self) -> Option<u64>;

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
        self.inner.services.initialization(partitions).await
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
    },
    Domain {
        group_id: String,
        offsets: KafkaDomainOffsetHost,
        rebalance: watch::Receiver<u64>,
    },
}

/// The complete connector-owned plan for opening Kafka source instances.
#[derive(Clone)]
pub struct KafkaSourcePlan {
    pub config: Vec<ClientConfigEntry>,
    pub topic: TopicName,
    pub offset_mode: KafkaSourceOffsetMode,
    pub enable_auto_commit: bool,
}

#[derive(Debug, Error)]
pub enum KafkaSourceError {
    #[error("failed to initialize Kafka consumer")]
    Initialize,
    #[error("failed to subscribe to Kafka topic '{topic}'")]
    Subscribe { topic: String },
    #[error("failed to build Kafka offset commit for topic '{topic}' partition {partition}")]
    BuildOffsetCommit { topic: String, partition: i32 },
    #[error("failed to commit Kafka offsets")]
    CommitOffset,
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
    consumer: StreamConsumer,
    topic: nervix_models::TopicName,
    offset_mode: KafkaSourceOffsetMode,
    enable_auto_commit: bool,
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
            KafkaSourceOffsetMode::ConsumerGroup { group_id }
            | KafkaSourceOffsetMode::Domain { group_id, .. } => group_id,
        };
        config.set("group.id", group_id);
        config.set("enable.partition.eof", "false");
        config.set(
            "enable.auto.commit",
            if plan.enable_auto_commit {
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
            consumer,
            topic: plan.topic.clone(),
            offset_mode: plan.offset_mode.clone(),
            enable_auto_commit: plan.enable_auto_commit,
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
            KafkaSourceOffsetMode::ConsumerGroup { .. } => {
                self.consumer.unsubscribe();
                Ok(())
            }
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
            KafkaSourceOffsetMode::ConsumerGroup { .. } => {
                self.consumer.unsubscribe();
                Ok(())
            }
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
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let first = match &mut self.offset_mode {
            KafkaSourceOffsetMode::Domain { rebalance, .. } => {
                tokio::select! {
                    changed = rebalance.changed() => {
                        return if changed.is_ok() {
                            Ok(SourceBatch::ResumeRequired)
                        } else {
                            Ok(SourceBatch::Closed)
                        };
                    }
                    message = self.consumer.recv() => message,
                }
            }
            KafkaSourceOffsetMode::ConsumerGroup { .. } => self.consumer.recv().await,
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
            tokio::task::consume_budget().await;
            tokio::select! {
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
            }
        }
        Ok(SourceBatch::Messages(messages))
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        let positions = latest_positions(positions);
        match &self.offset_mode {
            KafkaSourceOffsetMode::ConsumerGroup { .. } if self.enable_auto_commit => Ok(()),
            KafkaSourceOffsetMode::ConsumerGroup { .. } => self
                .commit_consumer_offsets(&positions)
                .change_context(SourceError::Acknowledge { connector: KAFKA }),
            KafkaSourceOffsetMode::Domain { offsets, .. } => {
                for position in positions {
                    tokio::task::consume_budget().await;
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
        for position in starts {
            tokio::task::consume_budget().await;
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

    fn commit_consumer_offsets(
        &self,
        positions: &[KafkaOffsetPosition],
    ) -> Result<(), Report<KafkaSourceError>> {
        let mut offsets = TopicPartitionList::new();
        for position in positions {
            offsets
                .add_partition_offset(
                    &position.topic,
                    position.partition,
                    Offset::Offset(position.offset),
                )
                .map_err(|source| {
                    Report::new(KafkaSourceError::BuildOffsetCommit {
                        topic: position.topic.clone(),
                        partition: position.partition,
                    })
                    .attach_printable(source.to_string())
                })?;
        }
        self.consumer
            .commit(&offsets, CommitMode::Async)
            .map_err(|source| {
                Report::new(KafkaSourceError::CommitOffset).attach_printable(source.to_string())
            })
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

fn latest_positions(positions: &[KafkaOffsetPosition]) -> Vec<KafkaOffsetPosition> {
    let mut topics = BTreeMap::<String, BTreeMap<i32, i64>>::new();
    for position in positions {
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

fn earliest_message_offsets(
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
            Offset::Beginning => consumer
                .fetch_watermarks(topic, *partition, Duration::from_secs(5))
                .map(|(low, _)| low)
                .map_err(|source| {
                    Report::new(KafkaSourceError::FetchWatermarks {
                        topic: topic.to_string(),
                        partition: *partition,
                    })
                    .attach_printable(source.to_string())
                })?,
            Offset::End | Offset::Invalid => consumer
                .fetch_watermarks(topic, *partition, Duration::from_secs(5))
                .map(|(_, high)| high)
                .map_err(|source| {
                    Report::new(KafkaSourceError::FetchWatermarks {
                        topic: topic.to_string(),
                        partition: *partition,
                    })
                    .attach_printable(source.to_string())
                })?,
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
        tokio::task::spawn_blocking(move || topic_partitions(&consumer, &topic))
            .await
            .map_err(|source| {
                Report::new(KafkaSourceError::InspectPartitions)
                    .attach_printable(source.to_string())
            })?
    }
}

#[cfg(test)]
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
}
