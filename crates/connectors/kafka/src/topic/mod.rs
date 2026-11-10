//! Kafka ingestor and emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Kafka consumer and producer configuration, ingestor polling and offset handling,
//!   consumer and producer retirement off the async workers, record and header publication, the
//!   create time each record carries from its envelope's `TIMESTAMP`, native queue capacity and
//!   admission backpressure, delivery-report classification, the records the producer holds until
//!   their delivery reports are served, the delivery bound a confirmed publish gives the producer,
//!   the purge through which a failed confirmed publish gives up the records the producer still
//!   holds, and producer queue shutdown.
//! - **Depends on.** The connector contract and its confirmation window, vocabulary values, the
//!   bounded executor, `error-stack`, Tokio, and `rust-rdkafka`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

#[cfg(feature = "testing")]
#[doc(hidden)]
pub use rdkafka as testing_rdkafka;

mod delivery;
mod ingestor;
mod retirement;
mod settings;
use std::{
    ffi::c_int,
    future::Future,
    num::{NonZeroU32, NonZeroU64},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use arch_into::ArchInto as _;
use async_trait::async_trait;
use delivery::{HandedOverRecord, KafkaDeliveryReport, KafkaProducerContext};
use error_stack::{Report, ResultExt as _};
pub use ingestor::{
    KafkaDomainOffsetError, KafkaDomainOffsetHost, KafkaDomainOffsetInitialization,
    KafkaDomainOffsetResult, KafkaDomainOffsetServices, KafkaDomainOffsetStart, KafkaGroupCommit,
    KafkaOffsetPosition, KafkaTopicIngestor, KafkaTopicIngestorError, KafkaTopicIngestorMessage,
    KafkaTopicIngestorOffsetMode, KafkaTopicIngestorPlan, TopicPartitionInspector,
};
// Criterion measures the per-partition folds an acknowledgement and a rejection run.
#[cfg(feature = "benchmarks")]
pub use ingestor::{earliest_message_offsets, latest_positions};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    AckConfirmation, BrokerAnswer, BrokerPublishingMode, BrokerVerdict, ConfirmationWindow,
    EmitterFinish, EmitterFinishError, EmitterFinishResult, EmitterHost, EmitterLifecycle,
    EmitterOpenError, EmitterRecord, EmitterRecordId, EmitterStartResult, EmitterWriteError,
    EmitterWriteResult, PerRecordOutcome, RecordEmitter, RejectedEmitterMember,
};
use nervix_execution::{Executor, MemoryClass, StorageClass};
use nervix_models::{ClientConfigEntry, ConnectorKind, KafkaTopicName, Timestamp};
use nervix_primitives::{
    sync::oneshot,
    time::{Instant, sleep, timeout_at},
};
use rdkafka::{
    config::ClientConfig,
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header as KafkaHeader, OwnedHeaders},
    producer::{BaseRecord, Producer, PurgeConfig, ThreadedProducer},
};
use retirement::{NativeClient, RetirementCharge, Retiring};
pub use settings::{
    KafkaDomainAssignmentRetry, KafkaTopicIngestorSettings, KafkaTopicSettingsError,
};
use thiserror::Error;

const KAFKA: &str = "kafka";

/// The librdkafka property that bounds how long the producer keeps trying to deliver a record it
/// took, its own retries included, under the one name a Kafka client accepts for it.
const DELIVERY_TIMEOUT_PROPERTY: &str = "delivery.timeout.ms";

/// The librdkafka property that, set to `true`, withholds the delivery report of every record the
/// broker took.
const ONLY_ERROR_REPORTS_PROPERTY: &str = "delivery.report.only.error";

/// The longest `delivery.timeout.ms` librdkafka accepts, which it holds as a C `int` of
/// milliseconds.
const LONGEST_MESSAGE_TIMEOUT_MILLIS: u32 = i32::MAX.unsigned_abs();

/// Nanoseconds in one millisecond, the unit `delivery.timeout.ms` counts in.
const NANOS_PER_MILLI: u32 = 1_000_000;

/// The longest single wait inside librdkafka's flush. A finish waits in such steps until its
/// deadline, so each step's milliseconds fit the C `int` librdkafka takes.
const FLUSH_STEP: Duration = Duration::from_secs(1);

/// The native queue byte limit, expressed by librdkafka in kibibytes.
const QUEUE_CAPACITY_PROPERTY: &str = "queue.buffering.max.kbytes";

/// A delivery callback can wake its waiter before librdkafka frees the native message's queue
/// place. Recheck that short interval cooperatively; ACK TIMEOUT bounds the complete wait.
const QUEUE_RECHECK_INTERVAL: Duration = Duration::from_millis(1);

pub struct KafkaTopicEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub topic: KafkaTopicName,
    pub mode: BrokerPublishingMode,
    pub partition_from_branch: bool,
}

/// The librdkafka producer a Kafka emitter publishes through.
type KafkaProducer = ThreadedProducer<KafkaProducerContext>;

/// Destroying a producer is native work that waits. rust-rdkafka first stops the producer's
/// polling thread, which finishes its current poll of up to 100 milliseconds and any delivery
/// callback it is serving. It then purges the records the producer still queues or has in flight
/// and flushes for up to 500 milliseconds so their reports are served, and librdkafka joins its
/// threads last.
impl NativeClient for KafkaProducer {
    const DESTRUCTION: &'static str = "destroying a retired Kafka producer";
}

struct KafkaProducerPreparation {
    config: ClientConfig,
    executor: nervix_execution::Executor,
    partitioned: bool,
}

/// Startup configuration shares the producer's slot; no preparation storage remains beside a
/// live native producer. Its optional partition metadata supplies the space for the cold pointer.
enum KafkaProducerState {
    Prepared(Box<KafkaProducerPreparation>),
    Ready {
        producer: Retiring<KafkaProducer>,
        partitioning: Option<Box<KafkaPartitionMetadata>>,
    },
}

pub struct KafkaTopicEmitter {
    /// Shared with the flush a finish runs on the runtime's blocking pool, which keeps the producer
    /// until librdkafka's flush returns, and whichever lets it go last retires it.
    producer: KafkaProducerState,
    topic: Box<str>,
    mode: BrokerPublishingMode,
    /// librdkafka's validated payload-byte capacity, resolved once with the producer's config.
    #[cfg_attr(
        nervix_lint,
        nervix::state(
            installed,
            reason = "the producer's native queue capacity is resolved once at construction"
        )
    )]
    queue_capacity_bytes: NonZeroU64,
}

/// Only explicit partition routing discovers destination metadata. Ordinary publication keeps
/// its existing producer, allocation count and fixed footprint.
struct KafkaPartitionMetadata {
    executor: Executor,
    timeout: Duration,
}

#[derive(Debug, Error)]
enum KafkaPartitionMetadataError {
    #[error("{connector} partition metadata admission failed", connector = ConnectorKind::Kafka)]
    Admission,
    #[error("{connector} partition metadata execution failed", connector = ConnectorKind::Kafka)]
    Execution,
    #[error("{connector} partition metadata exceeded socket.timeout.ms", connector = ConnectorKind::Kafka)]
    Timeout,
    #[error("{connector} partition metadata was cancelled", connector = ConnectorKind::Kafka)]
    Cancelled,
    #[error("{connector} topic '{topic}' has no usable destination partitions", connector = ConnectorKind::Kafka)]
    Partitions { topic: Box<str> },
}

impl KafkaPartitionMetadata {
    fn new(config: &ClientConfig, executor: Executor) -> EmitterStartResult<Self> {
        let native = config
            .create_native_config()
            .map_err(KafkaTopicEmitter::invalid_configuration)?;
        let value = native
            .get("socket.timeout.ms")
            .map_err(KafkaTopicEmitter::invalid_configuration)?;
        let milliseconds = value
            .parse::<u32>()
            .assured("librdkafka validates and renders a bounded positive socket timeout");
        Ok(Self {
            executor,
            timeout: Duration::from_millis(u64::from(milliseconds)),
        })
    }

    async fn partition_count(
        &self,
        producer: &Retiring<KafkaProducer>,
        topic: &str,
    ) -> Result<u32, Report<EmitterWriteError>> {
        let failure = || EmitterWriteError::Publish {
            connector: ConnectorKind::Kafka.keyword(),
        };
        let deadline = Instant::now()
            .checked_add(self.timeout)
            .assured("the validated native timeout fits the monotonic clock");
        let producer = producer.clone();
        let topic = topic.to_owned().into_boxed_str();
        let topic_bytes = topic.len();
        let operation =
            move |_charge: nervix_execution::Reservation,
                  cancellation: &nervix_execution::Cancellation| {
                if cancellation.is_cancelled() {
                    return Err(Report::new(KafkaPartitionMetadataError::Cancelled));
                }
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or_else(|| Report::new(KafkaPartitionMetadataError::Timeout))?;
                let metadata = producer
                    .client()
                    .fetch_metadata(Some(&topic), remaining)
                    .change_context(KafkaPartitionMetadataError::Partitions {
                        topic: topic.clone(),
                    })?;
                if cancellation.is_cancelled() {
                    return Err(Report::new(KafkaPartitionMetadataError::Cancelled));
                }
                let topic_metadata = metadata
                    .topics()
                    .iter()
                    .find(|candidate| candidate.name() == topic.as_ref())
                    .ok_or_else(|| {
                        Report::new(KafkaPartitionMetadataError::Partitions {
                            topic: topic.clone(),
                        })
                    })?;
                if let Some(error) = topic_metadata.error() {
                    return Err(Report::new(KafkaError::MetadataFetch(error.into()))
                        .change_context(KafkaPartitionMetadataError::Partitions { topic }));
                }
                let count = u32::try_from(topic_metadata.partitions().len())
                    .assured("native partition counts fit u32");
                if count == 0 {
                    return Err(Report::new(KafkaPartitionMetadataError::Partitions {
                        topic,
                    }));
                }
                Ok(count)
            };
        let charge_bytes = std::mem::size_of_val(&operation)
            .checked_add(topic_bytes)
            .assured("the charged job is held in memory");
        let charge = self
            .executor
            .try_reserve(MemoryClass::Commands, charge_bytes.arch_into())
            .change_context(KafkaPartitionMetadataError::Admission)
            .change_context_lazy(failure)?;
        let running = self
            .executor
            .run_storage(StorageClass::Connector, charge, operation);
        timeout_at(deadline, running)
            .await
            .map_err(|_| Report::new(KafkaPartitionMetadataError::Timeout))
            .change_context_lazy(failure)?
            .change_context(KafkaPartitionMetadataError::Execution)
            .change_context_lazy(failure)?
            .change_context_lazy(failure)
    }
}

/// Why a Kafka emitter could not create its producer, beneath the emitter's start failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
enum KafkaProducerStartError {
    /// The node's commands budget has no room for the charge of the job that will destroy the
    /// producer, so the producer is not created. The refusal judged nothing, so the host starts the
    /// emitter again after its retry delay.
    #[error(
        "the {connector} producer cannot reserve the job that will destroy it",
        connector = ConnectorKind::Kafka
    )]
    RetirementAdmission,
}

/// Why a full native queue could not provide a place after every current-window answer settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
enum KafkaQueueWaitError {
    #[error("{KAFKA} producer queue did not admit the record within ACK TIMEOUT {}", humantime::format_duration(*.timeout))]
    TimedOut { timeout: Duration },
    #[error("ACK TIMEOUT {} for {KAFKA} producer queue admission is past the monotonic clock's range", humantime::format_duration(*.timeout))]
    DeadlineOutOfRange { timeout: Duration },
}

/// How long librdkafka keeps trying to deliver a record a confirmed publish handed it, in the
/// whole milliseconds `delivery.timeout.ms` counts.
///
/// A confirmed publish stops waiting for a record's answer `ACK TIMEOUT` after it handed the
/// record over, so librdkafka stops trying to deliver it after the same `ACK TIMEOUT`, rounded up
/// to the millisecond it counts in, instead of retrying for its five-minute default. librdkafka
/// checks that bound only about once a second, though, so a publish that stops waiting also gives
/// up what the producer still holds before the host's retry sends the record again; see
/// [`KafkaTopicEmitter::give_up`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KafkaMessageTimeout {
    millis: u32,
}

/// Why a confirmed publish's `ACK TIMEOUT` cannot bound how long librdkafka tries to deliver a
/// record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the conversion returns a pure range validation refusal"
    )
)]
enum KafkaMessageTimeoutError {
    /// A zero `ACK TIMEOUT` would set `delivery.timeout.ms` to `0`, which librdkafka reads as no
    /// bound at all.
    #[error("ACK TIMEOUT 0s cannot bound librdkafka's delivery of a record")]
    Zero,
    /// librdkafka cannot hold a `delivery.timeout.ms` this long.
    #[error(
        "ACK TIMEOUT {} is longer than the {LONGEST_MESSAGE_TIMEOUT_MILLIS}ms librdkafka's \
         delivery.timeout.ms can hold",
        humantime::format_duration(*.timeout)
    )]
    TooLong { timeout: Duration },
}

impl TryFrom<Duration> for KafkaMessageTimeout {
    type Error = KafkaMessageTimeoutError;

    fn try_from(timeout: Duration) -> Result<Self, Self::Error> {
        let whole_millis = timeout.as_millis();
        let rounded_millis = if timeout.subsec_nanos().is_multiple_of(NANOS_PER_MILLI) {
            whole_millis
        } else {
            whole_millis
                .checked_add(1)
                .assured("a duration's whole milliseconds stay far below u128::MAX")
        };
        let Ok(millis) = u32::try_from(rounded_millis) else {
            return Err(KafkaMessageTimeoutError::TooLong { timeout });
        };
        if millis > LONGEST_MESSAGE_TIMEOUT_MILLIS {
            return Err(KafkaMessageTimeoutError::TooLong { timeout });
        }
        if millis == 0 {
            return Err(KafkaMessageTimeoutError::Zero);
        }
        Ok(Self { millis })
    }
}

/// The earliest time a Kafka record carries, in Unix milliseconds: the Unix epoch's first
/// millisecond. librdkafka reads a zero timestamp as the time it takes the record, and a Kafka
/// record carries no time before the epoch.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "librdkafka reads a zero record timestamp as the time it takes the record, and a \
                  Kafka record carries no time before the Unix epoch"
    )
)]
const FIRST_RECORD_MILLIS: i64 = 1;

/// The time a Kafka record carries as its `CreateTime`, in the whole Unix milliseconds Kafka
/// counts, from the `TIMESTAMP` its record's envelope sets.
///
/// A timestamp counts nanoseconds, so a record carries the millisecond that contains it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KafkaCreateTime {
    /// At least [`FIRST_RECORD_MILLIS`].
    unix_millis: i64,
}

/// Why a record's `TIMESTAMP` cannot be the time its Kafka record carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the conversion returns a pure range validation refusal"
    )
)]
enum KafkaCreateTimeError {
    /// The millisecond that contains the timestamp is not after the Unix epoch.
    #[error(
        "{KAFKA} record timestamp {} precedes {}, the earliest time a {KAFKA} record carries",
        .timestamp.to_rfc3339(),
        KafkaCreateTime::FIRST.timestamp().to_rfc3339()
    )]
    BeforeFirstMillisecond { timestamp: Timestamp },
}

impl KafkaCreateTime {
    /// The earliest time a Kafka record carries.
    const FIRST: Self = Self {
        unix_millis: FIRST_RECORD_MILLIS,
    };

    /// The time the Kafka record of `record` carries: the one its envelope's `TIMESTAMP` sets, or
    /// none, where librdkafka gives the record the time it takes it.
    fn of(record: &EmitterRecord) -> Result<Option<Self>, KafkaCreateTimeError> {
        let Some(timestamp) = record.envelope.timestamp else {
            return Ok(None);
        };
        Self::try_from(timestamp).map(Some)
    }

    /// The instant at the start of this record time's millisecond.
    fn timestamp(self) -> Timestamp {
        let unix_nanos = self
            .unix_millis
            .checked_mul(i64::from(NANOS_PER_MILLI))
            .assured("a record time is the millisecond of a signed Unix-nanosecond timestamp");
        Timestamp::from_unix_nanos(unix_nanos)
    }
}

impl TryFrom<Timestamp> for KafkaCreateTime {
    type Error = KafkaCreateTimeError;

    fn try_from(timestamp: Timestamp) -> Result<Self, Self::Error> {
        let unix_millis = timestamp
            .unix_nanos()
            .div_euclid(i64::from(NANOS_PER_MILLI));
        if unix_millis < FIRST_RECORD_MILLIS {
            return Err(KafkaCreateTimeError::BeforeFirstMillisecond { timestamp });
        }
        Ok(Self { unix_millis })
    }
}

/// The broker's answer for one record handed to the producer, read from the delivery report
/// librdkafka gives for it.
struct KafkaAnswer {
    report: oneshot::Receiver<KafkaDeliveryReport>,
}

impl Future for KafkaAnswer {
    type Output = BrokerAnswer;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<BrokerAnswer> {
        let report = match Pin::new(&mut self.report).poll(context) {
            Poll::Ready(report) => report,
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(KafkaTopicEmitter::broker_answer(report))
    }
}

/// Maps a resolved source ordinal into the broker's positive, native partition count.
impl KafkaTopicEmitter {
    fn destination_partition(source: u64, count: NonZeroU32) -> i32 {
        i32::try_from(source % u64::from(count.get()))
            .assured("Kafka exposes at most i32::MAX partitions")
    }
}

impl KafkaTopicEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Kafka(nervix_models::KafkaEmitterEntity::Topic);

    fn prepare(config: KafkaTopicEmitterConfig, host: EmitterHost) -> EmitterStartResult<Self> {
        let producer_config = Self::producer_config(&config.config, config.mode)?;
        let queue_capacity_bytes = Self::producer_queue_capacity(&producer_config)?;
        Ok(Self {
            producer: KafkaProducerState::Prepared(Box::new(KafkaProducerPreparation {
                config: producer_config,
                executor: host.executor(),
                partitioned: config.partition_from_branch,
            })),
            topic: config.topic.into_boxed_str(),
            mode: config.mode,
            queue_capacity_bytes,
        })
    }

    /// Reads the effective native limit, including librdkafka's default, without a second default
    /// table. Native configuration validation has already constrained this field to a positive
    /// C integer before its decimal representation is read.
    fn producer_queue_capacity(config: &ClientConfig) -> EmitterStartResult<NonZeroU64> {
        let native = config
            .create_native_config()
            .map_err(Self::invalid_configuration)?;
        let value = native
            .get(QUEUE_CAPACITY_PROPERTY)
            .map_err(Self::invalid_configuration)?;
        let kibibytes = value.parse::<u32>().assured(
            "librdkafka renders its validated positive C-integer queue limit as decimal digits",
        );
        let bytes = u64::from(kibibytes)
            .checked_mul(1024)
            .assured("a C-integer kibibyte limit times 1024 fits u64");
        Ok(NonZeroU64::new(bytes).assured("librdkafka validates a positive queue byte limit"))
    }

    fn producer(&self) -> &Retiring<KafkaProducer> {
        match &self.producer {
            KafkaProducerState::Prepared(_) => None,
            KafkaProducerState::Ready { producer, .. } => Some(producer),
        }
        .verified("the host resumes the emitter before publishing")
    }

    fn invalid_configuration(error: KafkaError) -> Report<EmitterOpenError> {
        let description = error.to_string();
        Report::new(error)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: KAFKA })
            .attach_printable(description)
    }

    /// A payload exceeding the entire native byte capacity cannot be admitted on any retry.
    /// librdkafka's queue accounting charges the payload alone, separately from wire-size checks.
    fn queue_size_rejection(
        &self,
        record: &EmitterRecord,
    ) -> Option<RejectedEmitterMember<EmitterRecordId>> {
        let bytes: u64 = record.payload.len().arch_into();
        if bytes <= self.queue_capacity_bytes.get() {
            return None;
        }
        Some(record.rejected(format!(
            "{KAFKA} record payload is {bytes} bytes, exceeding {QUEUE_CAPACITY_PROPERTY} \
             capacity {} bytes",
            self.queue_capacity_bytes,
        )))
    }

    /// The configuration of a producer that publishes in `mode` through a client whose entries are
    /// `entries`.
    ///
    /// The entries pass through as written, except that the producer always asks for the delivery
    /// report of every record: a record stays retained until its report is served, so a record the
    /// broker took without a report would count as unsent forever. A confirmed publish then sets
    /// `delivery.timeout.ms` to its `ACK TIMEOUT`, replacing the client's own entry. A Kafka client
    /// accepts that bound under this one name, so no entry can keep the producer trying to deliver
    /// a record after the publish stopped waiting for it.
    fn producer_config(
        entries: &[ClientConfigEntry],
        mode: BrokerPublishingMode,
    ) -> EmitterStartResult<ClientConfig> {
        let mut config = ClientConfig::new();
        for entry in entries {
            config.set(&entry.key, &entry.value);
        }
        config.set(ONLY_ERROR_REPORTS_PROPERTY, "false");
        let BrokerPublishingMode::Ack(settings) = mode else {
            return Ok(config);
        };
        let message_timeout = match KafkaMessageTimeout::try_from(settings.timeout) {
            Ok(message_timeout) => message_timeout,
            Err(error) => {
                let description = error.to_string();
                return Err(Report::new(error)
                    .change_context(EmitterOpenError::InvalidConfiguration { connector: KAFKA })
                    .attach_printable(description));
            }
        };
        config.set(
            DELIVERY_TIMEOUT_PROPERTY,
            message_timeout.millis.to_string(),
        );
        Ok(config)
    }

    /// Creates the producer `config` describes, taking the charge of the job that will destroy it
    /// on `executor` first.
    fn create_producer(
        config: &ClientConfig,
        executor: Executor,
    ) -> EmitterStartResult<Retiring<KafkaProducer>> {
        let retirement = match RetirementCharge::reserve(executor) {
            Ok(retirement) => retirement,
            Err(refused) => {
                let refused = refused.change_context(KafkaProducerStartError::RetirementAdmission);
                let description = format!("{refused:#}");
                return Err(refused
                    .change_context(EmitterOpenError::Initialize { connector: KAFKA })
                    .attach_printable(description));
            }
        };
        match config.create_with_context(KafkaProducerContext::new()) {
            Ok(producer) => Ok(retirement.hold(producer)),
            Err(error) => {
                let description = error.to_string();
                Err(Report::new(error)
                    .change_context(EmitterOpenError::Initialize { connector: KAFKA })
                    .attach_printable(description))
            }
        }
    }

    /// Publishes `records` without awaiting delivery reports: the producer taking a record into
    /// its queue delivers it.
    async fn publish_unconfirmed(
        &self,
        producer: &Retiring<KafkaProducer>,
        records: Vec<EmitterRecord>,
        partitions: Option<u32>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            if let Some(rejected) = self.queue_size_rejection(&record) {
                outcome.reject(rejected);
                continue;
            }
            let create_time = match KafkaCreateTime::of(&record) {
                Ok(create_time) => create_time,
                Err(refusal) => {
                    outcome.reject(record.rejected(refusal.to_string()));
                    continue;
                }
            };
            match self.hand_over(producer, &record, create_time, None, partitions) {
                Ok(()) => outcome.deliver(record.id),
                Err(error) if Self::is_record_rejection(&error) => {
                    outcome.reject(record.rejected(format!("kafka rejected record: {error}")));
                }
                Err(error) => {
                    outcome.fail(Self::refused_hand_over(error));
                    return outcome;
                }
            }
        }
        outcome
    }

    /// Publishes `records` and awaits librdkafka's delivery report for each through the
    /// confirmation window `settings` declare.
    ///
    /// Every way the publish can fail ends it through [`KafkaTopicEmitter::give_up`]: the producer
    /// gives up every record it still holds, and the window settles each answer the broker gave
    /// before that, so the host's retry never resends a record the broker confirmed and never races
    /// a copy the producer could still deliver. A full producer queue waits for an outstanding
    /// answer before trying the same record again.
    async fn publish_confirmed(
        &self,
        producer: &Retiring<KafkaProducer>,
        records: Vec<EmitterRecord>,
        settings: AckConfirmation,
        partitions: Option<u32>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        let mut window: ConfirmationWindow<KafkaAnswer> =
            ConfirmationWindow::new(KAFKA, settings, records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            if let Some(rejected) = self.queue_size_rejection(&record) {
                window.reject(rejected);
                continue;
            }
            let create_time = match KafkaCreateTime::of(&record) {
                Ok(create_time) => create_time,
                Err(refusal) => {
                    window.reject(record.rejected(refusal.to_string()));
                    continue;
                }
            };
            let answer = match self
                .hand_over_confirmed(
                    producer,
                    &record,
                    create_time,
                    &mut window,
                    settings.timeout,
                    partitions,
                )
                .await
            {
                Ok(answer) => answer,
                Err(failure) => {
                    if let Some(error) = failure.downcast_ref::<KafkaError>()
                        && Self::is_record_rejection(error)
                    {
                        window.reject(record.rejected(format!("{KAFKA} rejected record: {error}")));
                        continue;
                    }
                    return Self::give_up(producer, window, failure).await;
                }
            };
            if let Err(failure) = window.admit(record.id, record.occurred_at, answer).await {
                return Self::give_up(producer, window, failure).await;
            }
        }
        if let Err(failure) = window.settle_awaited().await {
            return Self::give_up(producer, window, failure).await;
        }
        window.finish().await
    }

    /// Ends a confirmed publish with `failure` once the producer gave up every record it still
    /// holds.
    ///
    /// librdkafka keeps trying to deliver a record until its `delivery.timeout.ms` passes, checks
    /// that bound only about once a second, and still sends a record whose bound has passed until
    /// that check fails it. The host's retry sends every record this publish leaves unresolved
    /// again, so a copy the producer still held could reach the broker beside the retry's. The
    /// producer therefore purges every record it queues or has in flight, which ends each record's
    /// delivery with a report, and the window settles the reports that arrive within `ACK TIMEOUT`:
    /// a record the broker took before the purge is confirmed by its report instead of being sent
    /// again. A record already on its way to the broker when the purge ran stays ambiguous, as any
    /// unanswered record is, because the broker may still store it.
    ///
    /// The purge does not wait for librdkafka's broker threads, so it parks no asynchronous worker;
    /// the window waits for the reports instead.
    async fn give_up(
        producer: &Retiring<KafkaProducer>,
        window: ConfirmationWindow<KafkaAnswer>,
        failure: Report<EmitterWriteError>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        producer.purge(PurgeConfig::default().queue().inflight().non_blocking());
        window.fail_once_given_up(failure).await
    }

    /// Retries a refused enqueue after settling the oldest outstanding answer. A native callback
    /// may still occupy its place after the last answer was read; only that empty-window wait gets
    /// a separate admission deadline, without extending any handed-over record's deadline.
    async fn hand_over_confirmed(
        &self,
        producer: &Retiring<KafkaProducer>,
        record: &EmitterRecord,
        create_time: Option<KafkaCreateTime>,
        window: &mut ConfirmationWindow<KafkaAnswer>,
        timeout: Duration,
        partitions: Option<u32>,
    ) -> EmitterWriteResult<KafkaAnswer> {
        let mut queue_deadline = None;
        loop {
            nervix_primitives::task::consume_budget().await;
            let (answer, report) = oneshot::channel();
            match self.hand_over(producer, record, create_time, Some(answer), partitions) {
                Ok(()) => return Ok(KafkaAnswer { report }),
                Err(error @ KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)) => {
                    if window.settle_next().await?.is_some() {
                        continue;
                    }
                    let deadline = match queue_deadline {
                        Some(deadline) => deadline,
                        None => {
                            let Some(deadline) = Instant::now().checked_add(timeout) else {
                                return Err(Self::queue_wait_failed(
                                    error,
                                    KafkaQueueWaitError::DeadlineOutOfRange { timeout },
                                ));
                            };
                            queue_deadline = Some(deadline);
                            deadline
                        }
                    };
                    if timeout_at(deadline, sleep(QUEUE_RECHECK_INTERVAL))
                        .await
                        .is_err()
                    {
                        return Err(Self::queue_wait_failed(
                            error,
                            KafkaQueueWaitError::TimedOut { timeout },
                        ));
                    }
                }
                Err(error) => return Err(Self::refused_hand_over(error)),
            }
        }
    }

    fn queue_wait_failed(
        error: KafkaError,
        failure: KafkaQueueWaitError,
    ) -> Report<EmitterWriteError> {
        let description = failure.to_string();
        Report::new(error)
            .change_context(failure)
            .change_context(EmitterWriteError::Publish { connector: KAFKA })
            .attach_printable(description)
    }

    /// Hands `message` to the producer, which retains it until librdkafka serves its delivery
    /// report.
    ///
    /// `create_time` is the time the record carries, where its envelope sets one; without it,
    /// librdkafka gives the record the time it takes it. `answer` is where a confirmed publish
    /// awaits that report; a `NO ACK` publish awaits none. A record the producer refuses is never
    /// retained: its delivery opaque comes back with the refusal and is dropped here.
    fn hand_over(
        &self,
        producer: &Retiring<KafkaProducer>,
        message: &EmitterRecord,
        create_time: Option<KafkaCreateTime>,
        answer: Option<oneshot::Sender<KafkaDeliveryReport>>,
        partitions: Option<u32>,
    ) -> Result<(), KafkaError> {
        let handed_over = producer.context().retain(answer);
        let mut record = BaseRecord::<[u8], [u8], Box<HandedOverRecord>>::with_opaque_to(
            self.topic.as_ref(),
            handed_over,
        )
        .payload(message.payload.as_slice());
        if let Some(destination) = &message.destination {
            if let Some(key) = &destination.key {
                record = record.key(key.as_bytes());
            }
            if let Some(source) = destination.source_partition {
                let count = partitions.verified("an explicit destination has a metadata snapshot");
                let partition = Self::destination_partition(
                    source,
                    NonZeroU32::new(count).verified("metadata resolved a positive partition count"),
                );
                record = record.partition(partition);
            }
        }
        if !message.headers.is_empty() {
            record = record.headers(Self::owned_headers(&message.headers));
        }
        if let Some(create_time) = create_time {
            record = record.timestamp(create_time.unix_millis);
        }
        producer.send(record).map_err(|(source, _record)| source)
    }

    /// The record headers a record's header writes become: every write in the order written, a
    /// repeated name once for each of its values.
    fn owned_headers(headers: &[(String, String)]) -> OwnedHeaders {
        let mut owned_headers = OwnedHeaders::new_with_capacity(headers.len());
        for (key, value) in headers {
            owned_headers = owned_headers.insert(KafkaHeader {
                key,
                value: Some(value.as_str()),
            });
        }
        owned_headers
    }

    /// The broker's answer for a record whose delivery report is `report`.
    ///
    /// A delivered record is confirmed, and a delivery error about the record itself refuses it
    /// for good. Any other ending leaves the record's outcome unknown: the broker, the connection
    /// or the producer failed, or the producer dropped the record without a report.
    fn broker_answer(
        report: Result<KafkaDeliveryReport, oneshot::error::RecvError>,
    ) -> BrokerAnswer {
        match report {
            Ok(KafkaDeliveryReport::Delivered) => Ok(BrokerVerdict::Confirmed),
            Ok(KafkaDeliveryReport::Failed(error)) if Self::is_record_rejection(&error) => {
                Ok(BrokerVerdict::Rejected {
                    message: format!("kafka rejected record: {error}"),
                })
            }
            Ok(KafkaDeliveryReport::Failed(error)) => {
                let description = format!("kafka delivery failed: {error}");
                Err(Report::new(error)
                    .change_context(EmitterWriteError::Publish { connector: KAFKA })
                    .attach_printable(description))
            }
            Err(dropped) => Err(Report::new(dropped)
                .change_context(EmitterWriteError::Publish { connector: KAFKA })
                .attach_printable("kafka producer dropped the record without a delivery report")),
        }
    }

    /// The failure that ends a publish whose record the producer refused to take for a reason
    /// that is not the record's own, such as a full producer queue.
    fn refused_hand_over(error: KafkaError) -> Report<EmitterWriteError> {
        let description = format!("kafka producer refused the record: {error}");
        Report::new(error)
            .change_context(EmitterWriteError::Publish { connector: KAFKA })
            .attach_printable(description)
    }

    /// Sends every record `producer` queues, whatever `linger.ms` says, until each is delivered
    /// or `deadline` passes.
    ///
    /// librdkafka ignores `linger.ms` only while a flush call waits: each broker thread reads that
    /// state when it next serves its partitions. rust-rdkafka's `Producer::flush` calls librdkafka's
    /// flush with no wait and polls between calls, so a broker thread that does not serve within
    /// that instant keeps lingering, and a loaded node reaches the deadline with every record still
    /// queued. This waits inside librdkafka's flush instead, one step at a time, while the
    /// producer's polling thread takes the delivery reports that let the flush end.
    fn flush_until(producer: &KafkaProducer, deadline: Instant) -> Result<(), KafkaError> {
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            let step = remaining.min(FLUSH_STEP);
            let step_millis = c_int::try_from(step.as_millis())
                .assured("a flush step lasts at most FLUSH_STEP, one second of milliseconds");
            let native = producer.client().native_ptr();
            // SAFETY: `producer` keeps the librdkafka handle `native` alive for the whole call, and
            // librdkafka lets any thread flush a producer while its polling thread polls it.
            let answer = unsafe { rdkafka::bindings::rd_kafka_flush(native, step_millis) };
            let code = RDKafkaErrorCode::from(answer);
            if code == RDKafkaErrorCode::NoError {
                return Ok(());
            }
            if code != RDKafkaErrorCode::OperationTimedOut || remaining.is_zero() {
                return Err(KafkaError::Flush(code));
            }
        }
    }

    /// How many records the producer holds whose delivery report librdkafka has not served yet.
    ///
    /// The errors, statistics and other events waiting in the producer's queue are not records, so
    /// this is not librdkafka's own out-queue length, which counts them too.
    fn retained_records(&self) -> u64 {
        match &self.producer {
            KafkaProducerState::Ready { producer, .. } => producer.context().retained_records(),
            KafkaProducerState::Prepared(_) => 0,
        }
    }

    fn is_record_rejection(error: &KafkaError) -> bool {
        let KafkaError::MessageProduction(code) = error else {
            return false;
        };
        matches!(
            code,
            RDKafkaErrorCode::InvalidMessage
                | RDKafkaErrorCode::InvalidMessageSize
                | RDKafkaErrorCode::MessageSizeTooLarge
                | RDKafkaErrorCode::InvalidTimestamp
                | RDKafkaErrorCode::InvalidRecord
        )
    }
}

#[async_trait]
impl EmitterLifecycle for KafkaTopicEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    fn needs_resume(&mut self) -> bool {
        matches!(self.producer, KafkaProducerState::Prepared(_))
    }

    fn resume(
        &mut self,
    ) -> nervix_connector::EmitterLifecycleStep<'_, nervix_connector::EmitterResume> {
        if !self.needs_resume() {
            return nervix_connector::EmitterLifecycleStep::ready(Ok(
                nervix_connector::EmitterResume::Ready,
            ));
        }
        nervix_connector::EmitterLifecycleStep::run(async move {
            if let KafkaProducerState::Prepared(prepared) = &self.producer {
                let KafkaProducerPreparation {
                    config,
                    executor,
                    partitioned,
                } = prepared.as_ref();
                let partitioning = if *partitioned {
                    Some(Box::new(
                        KafkaPartitionMetadata::new(config, executor.clone()).change_context(
                            nervix_connector::EmitterLifecycleError::Resume { connector: KAFKA },
                        )?,
                    ))
                } else {
                    None
                };
                let producer = Self::create_producer(config, executor.clone()).change_context(
                    nervix_connector::EmitterLifecycleError::Resume { connector: KAFKA },
                )?;
                self.producer = KafkaProducerState::Ready {
                    producer,
                    partitioning,
                };
            }
            Ok(nervix_connector::EmitterResume::Ready)
        })
    }

    /// Flushes the producer's queue, which holds the records a `NO ACK` publish delivered and
    /// librdkafka has not sent yet, and the records of a confirmed publish its caller stopped
    /// awaiting before it ended. A flush sends them at once, whatever `linger.ms` says.
    ///
    /// The outcome counts records alone, each until librdkafka serves its delivery report, and never
    /// the events waiting in the producer's queue beside them.
    ///
    /// The flush holds a handle of its own, so a flush that outlives this finish keeps the producer
    /// until librdkafka's flush returns, and retires it if the emitter is gone by then.
    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        if self.retained_records() == 0 {
            return Ok(EmitterFinish::NothingHeld);
        }
        let producer = self.producer().clone();
        let flushing =
            nervix_primitives::task::spawn_blocking(move || Self::flush_until(&producer, deadline));
        let flushed = flushing.await.map_err(|source| {
            Report::new(source).change_context(EmitterFinishError::Flush { connector: KAFKA })
        })?;
        match flushed {
            Ok(()) => Ok(EmitterFinish::Flushed),
            Err(error @ KafkaError::Flush(RDKafkaErrorCode::OperationTimedOut)) => {
                let records = self.retained_records();
                if records == 0 {
                    // The flush waits until librdkafka holds no message, and librdkafka still holds
                    // one while the polling thread serves its report, so a flush can give up after
                    // the last record's report was served. Every record then left the producer.
                    return Ok(EmitterFinish::Flushed);
                }
                Err(
                    Report::new(error).change_context(EmitterFinishError::Unsent {
                        connector: KAFKA,
                        records,
                    }),
                )
            }
            Err(error) => {
                Err(Report::new(error)
                    .change_context(EmitterFinishError::Flush { connector: KAFKA }))
            }
        }
    }
}

#[async_trait]
impl RecordEmitter for KafkaTopicEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        let KafkaProducerState::Ready {
            producer,
            partitioning,
        } = &self.producer
        else {
            let mut outcome = PerRecordOutcome::with_capacity(records.len());
            outcome.fail(Report::new(EmitterWriteError::NotInitialized {
                connector: KAFKA,
            }));
            return outcome;
        };
        let partitions = match partitioning {
            None => None,
            Some(metadata) => match metadata.partition_count(producer, &self.topic).await {
                Ok(count) => Some(count),
                Err(failure) => {
                    let mut outcome = PerRecordOutcome::with_capacity(records.len());
                    outcome.fail(failure);
                    return outcome;
                }
            },
        };
        match self.mode {
            BrokerPublishingMode::NoAck => {
                self.publish_unconfirmed(producer, records, partitions)
                    .await
            }
            BrokerPublishingMode::Ack(settings) => {
                self.publish_confirmed(producer, records, settings, partitions)
                    .await
            }
        }
    }
}

impl nervix_connector::EmitterConnector for KafkaTopicEmitter {
    const KIND: nervix_models::EmitterEntityKind = Self::KIND;
    type Plan = KafkaTopicEmitterConfig;
    async fn open(plan: Self::Plan, host: EmitterHost) -> EmitterStartResult<Self> {
        Self::prepare(plan, host)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use error_stack::{AttachmentKind, Context, FrameKind, Report};
    use meticulous::ResultExt as _;
    use nervix_connector::{
        AckConfirmation, BrokerPublishingMode, BrokerVerdict, EmitterOpenError, EmitterWriteError,
    };
    use nervix_models::ClientConfigEntry;
    use nervix_primitives::sync::oneshot;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    use super::{
        KafkaDeliveryReport, KafkaMessageTimeout, KafkaMessageTimeoutError, KafkaTopicEmitter,
        LONGEST_MESSAGE_TIMEOUT_MILLIS,
    };

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn confirmed(timeout: Duration) -> BrokerPublishingMode {
        BrokerPublishingMode::Ack(AckConfirmation {
            max_in_flight: NonZeroUsize::MIN,
            timeout,
        })
    }

    /// How an emitter describes `report`: its first printable attachment.
    fn description<C: Context>(report: &Report<C>) -> Option<String> {
        report.frames().find_map(|frame| match frame.kind() {
            FrameKind::Attachment(AttachmentKind::Printable(attachment)) => {
                Some(attachment.to_string())
            }
            FrameKind::Context(_) | FrameKind::Attachment(_) => None,
        })
    }

    #[test]
    fn a_delivered_record_is_confirmed() {
        let answer = KafkaTopicEmitter::broker_answer(Ok(KafkaDeliveryReport::Delivered));

        let Ok(verdict) = answer else {
            panic!("a delivered record has an answer, got {answer:?}");
        };
        assert_eq!(verdict, BrokerVerdict::Confirmed);
    }

    #[test]
    fn a_delivery_error_about_the_record_refuses_it_for_good() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);

        let answer =
            KafkaTopicEmitter::broker_answer(Ok(KafkaDeliveryReport::Failed(error.clone())));

        let Ok(verdict) = answer else {
            panic!("a refused record has an answer, got {answer:?}");
        };
        assert_eq!(
            verdict,
            BrokerVerdict::Rejected {
                message: format!("kafka rejected record: {error}"),
            }
        );
    }

    #[test]
    fn any_other_delivery_error_leaves_the_record_unknown_under_its_typed_cause() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut);

        let answer =
            KafkaTopicEmitter::broker_answer(Ok(KafkaDeliveryReport::Failed(error.clone())));

        let Err(failure) = answer else {
            panic!("a timed out record has no answer, got {answer:?}");
        };
        assert_eq!(
            failure.current_context(),
            &EmitterWriteError::Publish { connector: "kafka" }
        );
        assert_eq!(failure.downcast_ref::<KafkaError>(), Some(&error));
        assert_eq!(
            description(&failure),
            Some(format!("kafka delivery failed: {error}"))
        );
    }

    #[nervix_primitives::test]
    async fn a_report_the_producer_dropped_leaves_the_record_unknown() {
        let (answer, report) = oneshot::channel::<KafkaDeliveryReport>();
        drop(answer);
        let dropped = report.await;

        let answer = KafkaTopicEmitter::broker_answer(dropped);

        let Err(failure) = answer else {
            panic!("a record without a report has no answer, got {answer:?}");
        };
        assert_eq!(
            failure.current_context(),
            &EmitterWriteError::Publish { connector: "kafka" }
        );
        assert!(
            failure
                .downcast_ref::<oneshot::error::RecvError>()
                .is_some(),
            "the closed report channel stays beneath the publish failure"
        );
        assert_eq!(
            description(&failure),
            Some("kafka producer dropped the record without a delivery report".to_string())
        );
    }

    #[test]
    fn a_refused_hand_over_keeps_the_producer_error_beneath_the_publish_failure() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull);

        let failure = KafkaTopicEmitter::refused_hand_over(error.clone());

        assert_eq!(
            failure.current_context(),
            &EmitterWriteError::Publish { connector: "kafka" }
        );
        assert_eq!(failure.downcast_ref::<KafkaError>(), Some(&error));
        assert_eq!(
            description(&failure),
            Some(format!("kafka producer refused the record: {error}"))
        );
    }

    #[test]
    fn the_message_timeout_is_ack_timeout_rounded_up_to_its_millisecond() {
        let longest = Duration::from_millis(u64::from(LONGEST_MESSAGE_TIMEOUT_MILLIS));
        for (timeout, millis) in [
            (Duration::from_secs(3), 3_000),
            (Duration::from_millis(1_500), 1_500),
            (Duration::from_micros(1_500), 2),
            (Duration::from_nanos(1), 1),
            (longest, LONGEST_MESSAGE_TIMEOUT_MILLIS),
        ] {
            assert_eq!(
                KafkaMessageTimeout::try_from(timeout),
                Ok(KafkaMessageTimeout { millis }),
                "ACK TIMEOUT {timeout:?}"
            );
        }
    }

    #[test]
    fn an_ack_timeout_librdkafka_cannot_hold_is_refused() {
        let longest = Duration::from_millis(u64::from(LONGEST_MESSAGE_TIMEOUT_MILLIS));
        for timeout in [
            longest + Duration::from_nanos(1),
            longest + Duration::from_millis(1),
            Duration::MAX,
        ] {
            assert_eq!(
                KafkaMessageTimeout::try_from(timeout),
                Err(KafkaMessageTimeoutError::TooLong { timeout }),
                "ACK TIMEOUT {timeout:?}"
            );
        }
        assert_eq!(
            KafkaMessageTimeout::try_from(Duration::ZERO),
            Err(KafkaMessageTimeoutError::Zero)
        );
    }

    #[test]
    fn a_confirmed_producer_gives_up_on_a_record_after_ack_timeout() {
        let entries = [
            entry("bootstrap.servers", "127.0.0.1:9092"),
            entry("delivery.timeout.ms", "300000"),
        ];

        let config =
            KafkaTopicEmitter::producer_config(&entries, confirmed(Duration::from_millis(2_500)))
                .expect("an ACK TIMEOUT of seconds fits librdkafka's delivery timeout");

        assert_eq!(config.get("delivery.timeout.ms"), Some("2500"));
        assert_eq!(config.get("bootstrap.servers"), Some("127.0.0.1:9092"));
        let native = config
            .create_native_config()
            .expect("librdkafka accepts the confirmed producer's configuration");
        assert_eq!(
            native
                .get("delivery.timeout.ms")
                .expect("librdkafka reads its delivery timeout"),
            "2500"
        );
    }

    #[test]
    fn an_unconfirmed_producer_keeps_the_client_delivery_bound() {
        let entries = [
            entry("bootstrap.servers", "127.0.0.1:9092"),
            entry("delivery.timeout.ms", "300000"),
        ];

        let config = KafkaTopicEmitter::producer_config(&entries, BrokerPublishingMode::NoAck)
            .expect("the client entries pass through");

        assert_eq!(config.get("delivery.timeout.ms"), Some("300000"));
    }

    #[test]
    fn a_producer_reports_every_record_in_every_delivery_mode() {
        let entries = [entry("bootstrap.servers", "127.0.0.1:9092")];

        for mode in [
            BrokerPublishingMode::NoAck,
            confirmed(Duration::from_secs(5)),
        ] {
            let config = KafkaTopicEmitter::producer_config(&entries, mode)
                .expect("the client entries pass through");

            assert_eq!(
                config.get("delivery.report.only.error"),
                Some("false"),
                "a record stays retained until its report is served, in {mode:?}"
            );
            assert_eq!(config.get("bootstrap.servers"), Some("127.0.0.1:9092"));
        }
    }

    #[test]
    fn native_queue_configuration_installs_a_positive_capacity_or_its_typed_failure() {
        let config = KafkaTopicEmitter::producer_config(
            &[entry("queue.buffering.max.kbytes", "1")],
            BrokerPublishingMode::NoAck,
        )
        .assured("NO ACK configuration does not perform a fallible conversion");
        assert_eq!(
            KafkaTopicEmitter::producer_queue_capacity(&config)
                .assured("one KiB is within librdkafka's positive C-integer queue range")
                .get(),
            1024
        );

        for value in ["0", "-1", "not-a-capacity"] {
            let config = KafkaTopicEmitter::producer_config(
                &[entry("queue.buffering.max.kbytes", value)],
                BrokerPublishingMode::NoAck,
            )
            .assured("NO ACK configuration does not perform a fallible conversion");
            let Err(failure) = KafkaTopicEmitter::producer_queue_capacity(&config) else {
                panic!("native validation must refuse queue capacity {value}");
            };
            assert_eq!(
                failure.current_context(),
                &EmitterOpenError::InvalidConfiguration { connector: "kafka" }
            );
            assert!(failure.contains::<KafkaError>());
            assert!(format!("{failure:#}").contains("queue.buffering.max.kbytes"));
        }
    }

    #[test]
    fn an_ack_timeout_librdkafka_cannot_hold_refuses_the_emitter_configuration() {
        let timeout = Duration::from_secs(30 * 24 * 60 * 60);

        let Err(error) = KafkaTopicEmitter::producer_config(&[], confirmed(timeout)) else {
            panic!("an ACK TIMEOUT of 30 days exceeds librdkafka's message timeout");
        };

        assert_eq!(
            error.current_context(),
            &EmitterOpenError::InvalidConfiguration { connector: "kafka" }
        );
        assert_eq!(
            error.downcast_ref::<KafkaMessageTimeoutError>(),
            Some(&KafkaMessageTimeoutError::TooLong { timeout })
        );
        assert_eq!(
            description(&error),
            Some(KafkaMessageTimeoutError::TooLong { timeout }.to_string())
        );
    }

    #[test]
    fn oversized_and_invalid_records_are_definitive_rejections() {
        for code in [
            RDKafkaErrorCode::InvalidMessage,
            RDKafkaErrorCode::InvalidMessageSize,
            RDKafkaErrorCode::MessageSizeTooLarge,
            RDKafkaErrorCode::InvalidTimestamp,
            RDKafkaErrorCode::InvalidRecord,
        ] {
            assert!(KafkaTopicEmitter::is_record_rejection(
                &KafkaError::MessageProduction(code)
            ));
        }
    }

    #[test]
    fn broker_availability_errors_remain_infrastructure_failures() {
        for code in [
            RDKafkaErrorCode::QueueFull,
            RDKafkaErrorCode::UnknownTopicOrPartition,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::MessageTimedOut,
        ] {
            assert!(!KafkaTopicEmitter::is_record_rejection(
                &KafkaError::MessageProduction(code)
            ));
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "create_time_tests.rs"]
mod create_time_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "publish_tests.rs"]
mod publish_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod retirement_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "emitter_retirement_tests.rs"]
mod emitter_retirement_tests;

#[cfg(test)]
mod destination_properties;
