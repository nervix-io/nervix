//! Kafka source composition and transport execution.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Kafka connector plan with host-owned intake and replicated domain
//!   offsets. The Kafka transport below is isolated for its move into `nervix-connector-kafka`.
//! - **Depends on.** The connector source contract, typed Kafka plans, broker clients, and
//!   pre-resolved runtime handles.
//! - **Must not know.** NSPL parsing, registry validation, or placement computation.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    IngestMessageHeaders, IngestMetadataRow, SourceAckPolicy, SourceBatch, SourceBatchRequest,
    SourceCapabilities, SourceConnector, SourceError, SourceMessage, SourceMetadataScope,
    SourcePlan, SourceResult, SourceResume,
};
use nervix_connector_kafka::{
    KafkaDomainOffsetError, KafkaDomainOffsetHost, KafkaDomainOffsetInitialization,
    KafkaDomainOffsetResult, KafkaDomainOffsetServices, KafkaDomainOffsetStart,
    KafkaOffsetPosition, KafkaSourceOffsetMode, KafkaSourcePlan,
};
use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{Headers, Message as KafkaMessage},
    topic_partition_list::{Offset, TopicPartitionList},
};

use super::{
    super::*,
    source::{BrokerSourceHost, BrokerSourceHostSpec, run_source_instance},
};

const KAFKA: &str = "kafka";
const DOMAIN_ASSIGNMENT_RETRY: Duration = Duration::from_millis(100);

pub(crate) struct KafkaIngestor;

#[derive(Debug, Error)]
pub(crate) enum KafkaSourceError {
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

struct KafkaSourceMessage {
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

struct KafkaSource {
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
    type Message = KafkaSourceMessage;
    type Position = KafkaOffsetPosition;

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

    async fn next_batch(
        &mut self,
        request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let first = self
            .consumer
            .recv()
            .await
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

pub(crate) struct TopicPartitionInspector {
    consumer: StreamConsumer,
}

impl TopicPartitionInspector {
    pub(crate) fn new(
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
        Ok(Self { consumer })
    }

    pub(crate) fn partitions(&self, topic: &str) -> Result<Vec<i32>, Report<KafkaSourceError>> {
        topic_partitions(&self.consumer, topic)
    }
}

struct RuntimeKafkaDomainOffsets {
    runtime: Runtime,
    domain: DomainName,
    ingestor: IngestorName,
    topic: String,
    state: KafkaOffsetStateOriginator,
}

#[async_trait]
impl KafkaDomainOffsetServices for RuntimeKafkaDomainOffsets {
    fn generation(&self) -> Option<u64> {
        self.runtime
            .inner
            .domains
            .get(&self.domain)
            .map(|state| state.start_version)
    }

    async fn initialization(
        &self,
        partitions: &[i32],
    ) -> KafkaDomainOffsetResult<KafkaDomainOffsetInitialization> {
        let Some(domain_state) = self.runtime.inner.domains.get(&self.domain) else {
            return Err(
                Report::new(KafkaDomainOffsetError::Read).attach_printable(format!(
                    "domain '{}' is not installed",
                    self.domain.as_str()
                )),
            );
        };
        let generation = domain_state.start_version;
        let last_start = domain_state.last_start.clone();
        drop(domain_state);
        let schedule = if let Some(execution) = self.runtime.inner.executions.get(&self.domain)
            && let Some(node) = execution.schedule.nodes.get(&NodeRef::new(
                ModelKind::Ingestor,
                ModelName::from(&self.ingestor),
            )) {
            node.kafka_partition_schedule.clone()
        } else {
            None
        };
        let start = match last_start {
            nervix_models::DomainStartPoint::Resume => {
                let missing_partition_timestamp = self
                    .runtime
                    .current_paced_domain_time(&self.domain)
                    .map_err(|error| {
                        Report::new(KafkaDomainOffsetError::Read)
                            .attach_printable(error.to_string())
                    })?;
                let mut positions = Vec::new();
                for partition in partitions {
                    if let Some(offset) = self.state.read().next_offset(&self.topic, *partition) {
                        positions.push(KafkaOffsetPosition {
                            topic: self.topic.clone(),
                            partition: *partition,
                            offset,
                        });
                    }
                }
                KafkaDomainOffsetStart::Resume {
                    positions,
                    missing_partition_timestamp,
                }
            }
            nervix_models::DomainStartPoint::At { timestamp, .. } => {
                KafkaDomainOffsetStart::At(timestamp)
            }
            nervix_models::DomainStartPoint::Now { .. } => {
                return Err(
                    Report::new(KafkaDomainOffsetError::Read).attach_printable(format!(
                        "domain '{}' has an unresolved START AT NOW",
                        self.domain.as_str(),
                    )),
                );
            }
        };
        Ok(KafkaDomainOffsetInitialization {
            generation,
            start,
            schedule,
        })
    }

    async fn reset(&self, positions: Vec<KafkaOffsetPosition>) -> KafkaDomainOffsetResult<()> {
        self.runtime
            .reset_domain_kafka_offsets(&self.state, positions)
            .await
            .change_context(KafkaDomainOffsetError::Reset)
    }

    async fn commit(&self, position: KafkaOffsetPosition) -> KafkaDomainOffsetResult<()> {
        self.runtime
            .commit_domain_kafka_offset(&self.state, position)
            .await
            .change_context(KafkaDomainOffsetError::Commit)
    }
}

impl KafkaIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: KafkaIngestorStartPlan,
        kafka_offset_state: Option<KafkaOffsetStateOriginator>,
    ) -> Result<(), RuntimeError> {
        let KafkaIngestorStartPlan {
            ingestor,
            client,
            topic,
            offset_mode,
            instances,
            mode,
            offset_state_placement: _,
        } = plan;
        let domain = &ingestor.domain;
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if runtime.inner.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let acknowledgement = match &mode {
            KafkaIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => SourceAckPolicy::Parallel {
                max_in_flight: addressable_count(*max),
                batch_timeout: Runtime::parse_duration_setting(
                    domain,
                    &ingestor.name,
                    "batch timeout",
                    batch_timeout,
                )?,
                timeout: Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?,
                retry: Runtime::parse_retry_policy(domain, &ingestor.name, retry_policy)?,
            },
            KafkaIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => SourceAckPolicy::Sequential {
                timeout: Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?,
                retry: Runtime::parse_retry_policy(domain, &ingestor.name, retry_policy)?,
            },
            KafkaIngestMode::NoAckParallel => SourceAckPolicy::None,
        };
        let capabilities = SourceCapabilities::new(
            true,
            SourceMetadataScope::Kafka,
            ingestor.quiesce.supports(ingestor.quiesce.mode()),
            instances,
            acknowledgement.support(),
        );
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        let branched_runtime = runtime.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let quiesce = runtime
            .ingestor_quiesce_control(domain, &ingestor.name)
            .verified(
                "the runtime registers quiesce control for an ingestor before it starts the task",
            );
        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;

        let (shutdown_tx, _) = watch::channel(false);
        let rebalance_tx = if offset_mode == KafkaOffsetMode::Domain {
            Some(watch::channel(0_u64).0)
        } else {
            None
        };
        let mut tasks = Vec::with_capacity(instances.get().arch_into());
        if let Some(rebalance_tx) = rebalance_tx.as_ref() {
            let inspector = TopicPartitionInspector::new(
                &resolved_client.entries,
                format!(
                    "nervix_domain_watch_{}_{}",
                    domain.as_str(),
                    ingestor.name.as_str(),
                ),
            )
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_topic = topic.clone();
            let task_events = runtime.events().clone();
            let mut shutdown = shutdown_tx.subscribe();
            let rebalance_tx = rebalance_tx.clone();
            tasks.push(tokio::spawn(async move {
                let mut observed = match inspector.partitions(task_topic.as_str()) {
                    Ok(mut partitions) => {
                        partitions.sort_unstable();
                        partitions
                    }
                    Err(error) => {
                        task_events.report_error(format!(
                            "failed to inspect Kafka partitions for ingestor '{}' in domain '{}': \
                             {error}",
                            task_ingestor.as_str(),
                            task_domain.as_str(),
                        ));
                        Vec::new()
                    }
                };
                loop {
                    tokio::task::consume_budget().await;
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                        _ = sleep(DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL) => {}
                    }
                    let mut current = match inspector.partitions(task_topic.as_str()) {
                        Ok(partitions) => partitions,
                        Err(error) => {
                            task_events.report_error(format!(
                                "failed to inspect Kafka partitions for ingestor '{}' in domain \
                                 '{}': {error}",
                                task_ingestor.as_str(),
                                task_domain.as_str(),
                            ));
                            continue;
                        }
                    };
                    current.sort_unstable();
                    if current != observed {
                        observed = current.clone();
                        let epoch = rebalance_tx
                            .borrow()
                            .checked_add(1)
                            .assured("an ingestor cannot observe 2^64 partition rebalances");
                        rebalance_tx.send_replace(epoch);
                        info!(
                            domain = task_domain.as_str(),
                            ingestor = task_ingestor.as_str(),
                            topic = task_topic.as_str(),
                            partitions = ?current,
                            rebalance_epoch = epoch,
                            "detected Kafka partition topology change"
                        );
                    }
                }
            }));
        }

        let enable_auto_commit = acknowledgement == SourceAckPolicy::None
            && matches!(offset_mode, KafkaOffsetMode::ConsumerGroup(_));
        let source_offset_mode = match offset_mode {
            KafkaOffsetMode::ConsumerGroup(group) => KafkaSourceOffsetMode::ConsumerGroup {
                group_id: group.as_str().to_string(),
            },
            KafkaOffsetMode::Domain => {
                let Some(state) = kafka_offset_state else {
                    return Err(RuntimeError::StartIngestor {
                        domain: domain.as_str().to_string(),
                        ingestor: ingestor.name.as_str().to_string(),
                        reason: "Kafka DOMAIN offsets are not authoritative on this node"
                            .to_string(),
                    });
                };
                let offsets = KafkaDomainOffsetHost::new(RuntimeKafkaDomainOffsets {
                    runtime: runtime.clone(),
                    domain: domain.clone(),
                    ingestor: ingestor.name.clone(),
                    topic: topic.as_str().to_string(),
                    state,
                });
                KafkaSourceOffsetMode::Domain {
                    group_id: format!(
                        "nervix_domain_{}_{}",
                        domain.as_str(),
                        ingestor.name.as_str(),
                    ),
                    offsets,
                    rebalance: rebalance_tx
                        .as_ref()
                        .verified("DOMAIN offset mode creates the rebalance publisher above")
                        .subscribe(),
                }
            }
        };
        let source_plan = SourcePlan {
            connector: KafkaSourcePlan {
                config: resolved_client.entries,
                topic: topic.clone(),
                offset_mode: source_offset_mode,
                enable_auto_commit,
            },
            capabilities,
            acknowledgement,
        };
        runtime.prepare_ingestor_readiness(
            domain,
            &ingestor.name,
            source_plan.capabilities.instances(),
        );

        for instance_index in 0..source_plan.capabilities.instances().get() {
            let source = KafkaSource::open(&source_plan.connector, instance_index)
                .await
                .map_err(|error| RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            let host = BrokerSourceHost::build(BrokerSourceHostSpec {
                runtime: runtime.clone(),
                domain: domain.clone(),
                ingestor: ingestor.name.clone(),
                timestamp_source: ingestor.timestamp_source.clone(),
                output_routes: dependencies.output_routes.clone(),
                filter_where: dependencies.filter_where.clone(),
                codec: dependencies.codec.clone(),
                metrics: dependencies.metrics.clone(),
                branched_senders: branched_runtime.senders.clone(),
                quiesce: quiesce.clone(),
                shutdown: shutdown_tx.subscribe(),
                instance_index,
                metadata_kind: IngestMetadataKind::Kafka,
            });
            let shutdown = shutdown_tx.subscribe();
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_topic = topic.clone();
            let acknowledgement = source_plan.acknowledgement;
            let client_mounts = resolved_client.mounts.clone();
            tasks.push(tokio::spawn(async move {
                let _client_mounts = client_mounts;
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    topic = task_topic.as_str(),
                    instance = instance_index,
                    "started Kafka ingestor"
                );
                run_source_instance(source, host, acknowledgement, shutdown).await;
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    instance = instance_index,
                    "stopped Kafka ingestor"
                );
            }));
        }

        runtime.inner.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks,
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
