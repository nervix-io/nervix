use error_stack::{AttachmentKind, FrameKind, Report, ResultExt as _};
use thiserror::Error;

use super::{
    *,
    physical_time::{PhysicalDeadline, PhysicalDeadlineCapability},
};

pub(in crate::runtime) mod clickhouse;
mod iceberg;
mod kafka;
mod mongodb;
mod mqtt;
mod mysql;
mod nats;
mod otel;
mod postgres;
pub(in crate::runtime) mod pulsar;
mod rabbitmq;
mod redis;
mod sentry;
mod sqs;
mod syslog;
mod zeromq;

use clickhouse::ClickHouseEmitter;
use iceberg::{
    IcebergEmitter, IcebergEmitterClientConfig, IcebergEmitterError, IcebergEmitterInit,
    IcebergEmitterResult,
};
use kafka::KafkaEmitter;
use mongodb::MongoDbEmitter;
pub(in crate::runtime) use mongodb::{MongoDbClient, open_mongodb_client};
use mqtt::MqttEmitter;
use mysql::MySqlEmitter;
pub(in crate::runtime) use mysql::{MySqlPool, MySqlSharedPool, open_mysql_pool};
use nats::NatsEmitter;
use otel::{OtelEmitter, OtelEmitterInit};
use postgres::PostgresEmitter;
pub(in crate::runtime) use postgres::{PgPool, open_postgres_pool};
use pulsar::PulsarEmitter;
use rabbitmq::RabbitMqEmitter;
use redis::RedisEmitter;
pub(in crate::runtime) use redis::{RedisCommandPool, open_redis_command_pool};
use sentry::SentryEmitter;
use sqs::{SqsEmitter, SqsPublishingMode};
use syslog::SyslogEmitter;
use zeromq::ZeroMqEmitter;

const RETRY_ACK_ALIVE_EACH: Duration = Duration::from_millis(100);

pub(in crate::runtime) struct EmitterTask;

#[derive(Debug)]
struct EmitterBufferedMessages {
    reported: Arc<AtomicUsize>,
    generic: AtomicUsize,
    iceberg: AtomicUsize,
}

impl EmitterBufferedMessages {
    fn new(reported: Arc<AtomicUsize>) -> Self {
        Self {
            reported,
            generic: AtomicUsize::new(0),
            iceberg: AtomicUsize::new(0),
        }
    }

    fn set_generic(&self, messages: usize) {
        self.generic.store(messages, Ordering::Release);
        self.report_total();
    }

    fn set_iceberg(&self, messages: usize) {
        self.iceberg.store(messages, Ordering::Release);
        self.report_total();
    }

    fn report_total(&self) {
        self.reported.store(
            self.generic
                .load(Ordering::Acquire)
                .checked_add(self.iceberg.load(Ordering::Acquire))
                .assured("both counts total messages this node already holds in memory"),
            Ordering::Release,
        );
    }
}

impl Default for EmitterBufferedMessages {
    fn default() -> Self {
        Self::new(Arc::new(AtomicUsize::new(0)))
    }
}

/// What a sink connector needs to publish one emitter's output. The staging directory and the
/// event bus are read through `runtime` rather than copied in beside it, so the context carries
/// one handle to node state instead of a second view of the same values.
#[derive(Clone)]
pub(in crate::runtime) struct EmitterSinkContext {
    runtime: Runtime,
    domain: DomainName,
    emitter: EmitterName,
    error_policies: ErrorPolicies,
    udfs: Option<UdfExecutor>,
    /// The bound clock that resolves this emitter's explicit `FLUSH EACH` and `COMMIT EACH`
    /// cadences. Publish attempts, retry backoff, acknowledgement keepalive and stop deadlines
    /// stay on the monotonic clock and never read it.
    clock: DomainClock,
}

struct EmitterPublishControl<'a> {
    fault_injection: &'a ConfiguredFaultInjection,
    shutdown_rx: &'a mut watch::Receiver<bool>,
    stop_rx: &'a mut watch::Receiver<Option<Instant>>,
    backoff: &'a mut RuntimeReconnectBackoff,
}

async fn await_until_emitter_stop_deadline<T>(
    stop_rx: &mut watch::Receiver<Option<Instant>>,
    future: impl std::future::Future<Output = T>,
) -> Result<T, ()> {
    tokio::pin!(future);
    loop {
        tokio::task::consume_budget().await;
        let stop_deadline = *stop_rx.borrow();
        if let Some(deadline) = stop_deadline {
            return tokio::time::timeout_at(deadline, &mut future)
                .await
                .map_err(|_| ());
        }
        tokio::select! {
            output = &mut future => return Ok(output),
            changed = stop_rx.changed() => {
                if changed.is_err() {
                    return Ok(future.await);
                }
            }
        }
    }
}

fn emitter_stop_deadline_elapsed() -> Report<EmitterRuntimeError> {
    Report::new(EmitterRuntimeError::StopDeadlineElapsed)
        .attach_printable("emitter stop deadline elapsed while publishing or retrying")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrokerPublishingMode {
    NoAck,
    Ack(AckConfirmation),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MqttPublishingMode {
    Qos0,
    Qos1(AckConfirmation),
    Qos2(AckConfirmation),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NatsPublishingMode {
    Core,
    JetStream(AckConfirmation),
}

/// How many publishes may await confirmation at once, and how long each one may take.
///
/// `MODE ACK SEQUENTIAL` and `MODE ACK PARALLEL MAX <n>` both name a window of at least one, so
/// the window is non-zero by construction and no publishing path has to decide what a window of
/// zero would mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AckConfirmation {
    max_in_flight: NonZeroUsize,
    timeout: Duration,
}

#[derive(Debug, Clone)]
enum CompiledSqsFifoGroup {
    FromBranch,
    Expression(CompiledProgramWithMaterializedInterest),
}

/// The publishing behavior of the one transport family a sink belongs to.
///
/// `MODE` is checked against the sink before anything else, so the family and the settings it
/// carries are decided together and travel as one value. Holding them apart, as one option per
/// family, made "MQTT settings on a Kafka sink" and "no settings at all" representable, and every
/// sink had to reject both again while starting up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmitterTransportMode {
    Broker(BrokerPublishingMode),
    Mqtt(MqttPublishingMode),
    Nats(NatsPublishingMode),
    Sqs(SqsPublishingMode),
    /// `MODE ACK` on an HTTP endpoint sink, where the response is the acknowledgment and there is
    /// no transport-level publishing mode to carry.
    Request,
}

#[derive(Debug, Clone, Copy)]
struct EmitterPublishingSettings {
    retry_policy: ParsedRetryPolicy,
    transport: EmitterTransportMode,
}

impl EmitterPublishingSettings {
    fn parse(
        domain: &DomainName,
        emitter: &EmitterName,
        sink: &EmitSink,
        mode: &EmitterPublishingMode,
    ) -> Result<Self, RuntimeError> {
        if !sink.accepts_publishing_mode(mode) {
            return Err(Self::invalid_setting(
                domain,
                emitter,
                format!(
                    "MODE {} is not supported by the {} sink",
                    mode.kind_label(),
                    sink.transport_label()
                ),
            ));
        }
        let retry_policy = mode.retry_policy();
        let retry_policy = ParsedRetryPolicy {
            backoff: Runtime::parse_runtime_node_duration_setting(
                domain,
                "emitter",
                emitter,
                "retry backoff",
                &retry_policy.backoff,
            )?,
            max_backoff: Runtime::parse_runtime_node_duration_setting(
                domain,
                "emitter",
                emitter,
                "retry max backoff",
                &retry_policy.max_backoff,
            )?,
        };
        if retry_policy.backoff.is_zero() {
            return Err(Self::invalid_setting(
                domain,
                emitter,
                "retry backoff must be greater than zero",
            ));
        }
        if retry_policy.max_backoff < retry_policy.backoff {
            return Err(Self::invalid_setting(
                domain,
                emitter,
                "retry max backoff must be greater than or equal to retry backoff",
            ));
        }

        let transport =
            match mode {
                // A NATS sink publishes through its own core client even without an
                // acknowledgment, so the shared `NO_ACK` still resolves to the NATS family
                // rather than the broker one.
                EmitterPublishingMode::NoAck { .. } if matches!(sink, EmitSink::Nats { .. }) => {
                    EmitterTransportMode::Nats(NatsPublishingMode::Core)
                }
                EmitterPublishingMode::NoAck { .. } => {
                    EmitterTransportMode::Broker(BrokerPublishingMode::NoAck)
                }
                EmitterPublishingMode::BrokerAck {
                    window,
                    ack_timeout,
                    ..
                } => EmitterTransportMode::Broker(BrokerPublishingMode::Ack(
                    Self::parse_confirmation(domain, emitter, window, ack_timeout)?,
                )),
                EmitterPublishingMode::MqttQos0 { .. } => {
                    EmitterTransportMode::Mqtt(MqttPublishingMode::Qos0)
                }
                EmitterPublishingMode::MqttQos1 {
                    window,
                    ack_timeout,
                    ..
                } => EmitterTransportMode::Mqtt(MqttPublishingMode::Qos1(
                    Self::parse_confirmation(domain, emitter, window, ack_timeout)?,
                )),
                EmitterPublishingMode::MqttQos2 {
                    window,
                    ack_timeout,
                    ..
                } => EmitterTransportMode::Mqtt(MqttPublishingMode::Qos2(
                    Self::parse_confirmation(domain, emitter, window, ack_timeout)?,
                )),
                EmitterPublishingMode::NatsJetStream {
                    window,
                    ack_timeout,
                    ..
                } => EmitterTransportMode::Nats(NatsPublishingMode::JetStream(
                    Self::parse_confirmation(domain, emitter, window, ack_timeout)?,
                )),
                EmitterPublishingMode::SqsSingle { .. } => {
                    EmitterTransportMode::Sqs(SqsPublishingMode::Single)
                }
                EmitterPublishingMode::SqsBatch { .. } => {
                    EmitterTransportMode::Sqs(SqsPublishingMode::Batch)
                }
                EmitterPublishingMode::RequestAck { .. } => EmitterTransportMode::Request,
            };
        Ok(Self {
            retry_policy,
            transport,
        })
    }

    fn invalid_setting(
        domain: &DomainName,
        emitter: &EmitterName,
        reason: impl Into<String>,
    ) -> RuntimeError {
        RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "invalid publishing mode for emitter '{}': {}",
                emitter.as_str(),
                reason.into()
            ),
        }
    }

    fn parse_confirmation(
        domain: &DomainName,
        emitter: &EmitterName,
        window: &EmitterAckWindow,
        ack_timeout: &str,
    ) -> Result<AckConfirmation, RuntimeError> {
        let timeout = Runtime::parse_runtime_node_duration_setting(
            domain,
            "emitter",
            emitter,
            "ack timeout",
            ack_timeout,
        )?;
        if timeout.is_zero() {
            return Err(Self::invalid_setting(
                domain,
                emitter,
                "ack timeout must be greater than zero",
            ));
        }
        let max_in_flight = match window {
            EmitterAckWindow::Sequential => NonZeroUsize::MIN,
            EmitterAckWindow::Parallel { max } => addressable_count(*max),
        };
        Ok(AckConfirmation {
            max_in_flight,
            timeout,
        })
    }

    fn broker_mode(self) -> EmitterRuntimeResult<BrokerPublishingMode> {
        match self.transport {
            EmitterTransportMode::Broker(mode) => Ok(mode),
            _ => Err(emitter_config_error(
                "emitter sink requires a broker publishing mode",
            )),
        }
    }

    fn mqtt_mode(self) -> EmitterRuntimeResult<MqttPublishingMode> {
        match self.transport {
            EmitterTransportMode::Mqtt(mode) => Ok(mode),
            _ => Err(emitter_config_error(
                "MQTT sink requires an MQTT publishing mode",
            )),
        }
    }

    fn nats_mode(self) -> EmitterRuntimeResult<NatsPublishingMode> {
        match self.transport {
            EmitterTransportMode::Nats(mode) => Ok(mode),
            _ => Err(emitter_config_error(
                "NATS sink requires a NATS publishing mode",
            )),
        }
    }

    fn sqs_mode(self) -> EmitterRuntimeResult<SqsPublishingMode> {
        match self.transport {
            EmitterTransportMode::Sqs(mode) => Ok(mode),
            _ => Err(emitter_config_error(
                "SQS sink requires an SQS publishing mode",
            )),
        }
    }
}

struct EmitterBatchContext<'a> {
    runtime: &'a Runtime,
    domain: &'a DomainName,
    emitter: &'a EmitterName,
    metric_relay: Option<&'a RelayName>,
    error_policies: &'a ErrorPolicies,
    source_filters: &'a HashMap<RelayName, CompiledProgramWithMaterializedInterest>,
    filter_map: Option<&'a CompiledEmitterFilterMapProgram>,
    sqs_fifo_group: Option<&'a CompiledSqsFifoGroup>,
    materialized_state: &'a [nervix_models::MaterializedStateDependency],
}

#[derive(Clone)]
struct EmitterPublishBatch {
    batch: RelayRecordBatch,
    execution_now: Timestamp,
    headers: Option<Vec<EmitterHeaders>>,
    sqs_message_groups: Vec<Result<Option<String>, String>>,
    delivered: Vec<bool>,
}

pub(super) struct EmitterBatchExecutionContext<'a> {
    pub(super) batch_index: usize,
    pub(super) batch: &'a RelayRecordBatch,
    pub(super) execution_now: Timestamp,
}

/// The bound every byte estimate in this module relies on: each term counts bytes of a batch,
/// header, or group identifier this node already holds in memory, so their total is bounded by the
/// address space those values occupy.
const BYTES_IN_MEMORY: &str =
    "every term counts bytes of a value this node already holds in memory";

impl EmitterPublishBatch {
    fn from_batch(batch: RelayRecordBatch, execution_now: Timestamp) -> Self {
        let row_count = batch.batch.batch().num_rows();
        Self {
            batch,
            execution_now,
            headers: None,
            sqs_message_groups: vec![Ok(None); row_count],
            delivered: vec![false; row_count],
        }
    }

    fn new(
        batch: RelayRecordBatch,
        headers: Option<Vec<EmitterHeaders>>,
        execution_now: Timestamp,
    ) -> Result<Self, String> {
        let row_count = batch.batch.batch().num_rows();
        if let Some(headers) = &headers
            && row_count != headers.len()
        {
            return Err(format!(
                "emitter header count {} does not match row count {}",
                headers.len(),
                row_count
            ));
        }
        Ok(Self {
            batch,
            execution_now,
            headers,
            sqs_message_groups: vec![Ok(None); row_count],
            delivered: vec![false; row_count],
        })
    }

    fn with_sqs_message_groups(
        mut self,
        groups: Vec<Result<Option<String>, String>>,
    ) -> Result<Self, String> {
        let row_count = self.batch.batch.batch().num_rows();
        if groups.len() != row_count {
            return Err(format!(
                "SQS FIFO group count {} does not match emitter row count {row_count}",
                groups.len()
            ));
        }
        self.sqs_message_groups = groups;
        Ok(self)
    }

    fn estimated_bytes(&self) -> u64 {
        self.batch
            .estimated_bytes()
            .checked_add(
                self.headers
                    .iter()
                    .flatten()
                    .flatten()
                    .map(|(name, value)| {
                        let name_len: u64 = name.len().arch_into();
                        let value_len: u64 = value.len().arch_into();
                        name_len.checked_add(value_len).assured(BYTES_IN_MEMORY)
                    })
                    .try_fold(0_u64, u64::checked_add)
                    .assured(BYTES_IN_MEMORY),
            )
            .assured(BYTES_IN_MEMORY)
            .checked_add(
                self.sqs_message_groups
                    .iter()
                    .map(|group| match group {
                        Ok(Some(group)) | Err(group) => group.len().arch_into(),
                        Ok(None) => 0,
                    })
                    .try_fold(0_u64, u64::checked_add)
                    .assured(BYTES_IN_MEMORY),
            )
            .assured(BYTES_IN_MEMORY)
    }

    fn headers_for_row(&self, row: usize) -> Option<&EmitterHeaders> {
        static EMPTY: EmitterHeaders = Vec::new();

        match &self.headers {
            Some(headers) => headers.get(row),
            None if row < self.batch.keys.len() => Some(&EMPTY),
            None => None,
        }
    }

    fn message_count(&self) -> u64 {
        self.batch.message_count()
    }

    fn domain_timestamp(&self) -> Option<Timestamp> {
        self.batch.domain_timestamp()
    }

    fn merged_acks(&self) -> AckSet {
        self.batch.merged_acks()
    }

    fn is_delivered(&self, row: usize) -> bool {
        self.delivered.get(row).copied().unwrap_or(false)
    }

    fn mark_delivered(&mut self, row: usize) -> Result<(), String> {
        let delivered_rows = self.delivered.len();
        let delivered = self.delivered.get_mut(row).ok_or_else(|| {
            format!(
                "emitter delivered row {row} is outside batch with {} rows",
                delivered_rows
            )
        })?;
        if !*delivered {
            let acks = self.batch.acks.get(row).ok_or_else(|| {
                format!(
                    "emitter delivered row {row} is outside ack set with {} rows",
                    self.batch.acks.len()
                )
            })?;
            acks.ack_success();
            *delivered = true;
        }
        Ok(())
    }

    fn mark_rejected(&mut self, row: usize) -> Result<(), String> {
        let delivered_rows = self.delivered.len();
        let delivered = self.delivered.get_mut(row).ok_or_else(|| {
            format!("emitter rejected row {row} is outside batch with {delivered_rows} rows")
        })?;
        *delivered = true;
        Ok(())
    }

    async fn mark_rejected_after_delivery(
        &mut self,
        row: usize,
        delivery: impl std::future::Future<Output = ()>,
    ) -> EmitterRuntimeResult<()> {
        delivery.await;
        self.mark_rejected(row).map_err(|reason| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(reason)
        })
    }

    fn pending_record_chunks(&self, max_batch: NonZeroU64) -> Vec<Vec<usize>> {
        self.pending_record_rows()
            .chunks(addressable_count(max_batch).get())
            .map(<[usize]>::to_vec)
            .collect()
    }

    fn pending_record_rows(&self) -> Vec<usize> {
        (0..self.batch.batch.batch().num_rows())
            .filter(|row| !self.delivered.get(*row).copied().unwrap_or(false))
            .collect()
    }
}

#[derive(Debug)]
struct EncodedBrokerRecord {
    batch_index: usize,
    row_index: usize,
    key: Option<String>,
    payload: Vec<u8>,
    headers: EmitterHeaders,
    sqs_message_group: Result<Option<String>, String>,
    acks: AckSet,
    execution_now: Timestamp,
}

impl EncodedBrokerRecord {
    const fn position(&self) -> BrokerRecordPosition {
        BrokerRecordPosition {
            batch_index: self.batch_index,
            row_index: self.row_index,
        }
    }
}

/// Where one record sits in a publish call: which of the emitter's batches it came from and which
/// row of that batch it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BrokerRecordPosition {
    pub(super) batch_index: usize,
    pub(super) row_index: usize,
}

pub(super) struct RejectedEmitterRecord {
    pub(super) position: BrokerRecordPosition,
    pub(super) reason: String,
    pub(super) structured_error: Option<StructuredMessageError>,
}

pub(super) struct PerRecordPublishOutcome {
    pub(super) delivered: Vec<BrokerRecordPosition>,
    pub(super) rejected: Vec<RejectedEmitterRecord>,
    pub(super) infrastructure_error: Option<Report<EmitterRuntimeError>>,
}

impl PerRecordPublishOutcome {
    pub(super) fn empty() -> Self {
        Self {
            delivered: Vec::new(),
            rejected: Vec::new(),
            infrastructure_error: None,
        }
    }

    pub(super) fn fail(&mut self, error: Report<EmitterRuntimeError>) {
        self.infrastructure_error = Some(error);
    }

    pub(super) fn deliver(&mut self, position: BrokerRecordPosition) {
        self.delivered.push(position);
    }

    pub(super) fn reject(&mut self, position: BrokerRecordPosition, reason: impl Into<String>) {
        self.rejected.push(RejectedEmitterRecord {
            position,
            reason: reason.into(),
            structured_error: None,
        });
    }

    pub(super) fn reject_structured(
        &mut self,
        position: BrokerRecordPosition,
        error: StructuredMessageError,
    ) {
        self.rejected.push(RejectedEmitterRecord {
            position,
            reason: String::new(),
            structured_error: Some(error),
        });
    }

    pub(super) fn filter_mapped_chunks<T>(
        &mut self,
        batch_index: usize,
        rows: &[Result<T, StructuredMessageError>],
        pending_chunks: &[Vec<usize>],
        sink: &str,
    ) -> EmitterRuntimeResult<Vec<Vec<usize>>> {
        let mut filtered = Vec::with_capacity(pending_chunks.len());
        for chunk in pending_chunks {
            let mut filtered_chunk = Vec::with_capacity(chunk.len());
            for row in chunk {
                match rows.get(*row) {
                    Some(Ok(_)) => filtered_chunk.push(*row),
                    Some(Err(error)) => {
                        self.reject_structured(
                            BrokerRecordPosition {
                                batch_index,
                                row_index: *row,
                            },
                            error.clone(),
                        );
                    }
                    None => {
                        return Err(Report::new(EmitterRuntimeError::EncodeBatch)
                            .attach_printable(format!(
                                "{sink} pending row {row} is outside mapped batch with {} rows",
                                rows.len()
                            )));
                    }
                }
            }
            if !filtered_chunk.is_empty() {
                filtered.push(filtered_chunk);
            }
        }
        Ok(filtered)
    }
}

pub(in crate::runtime) struct PublishReport {
    messages: u64,
    bytes: u64,
    domain_timestamp: Timestamp,
}

pub(in crate::runtime) struct CompiledSqlValuesProgram {
    program: Arc<VmCompiledProgram>,
    label: &'static str,
    error_sites: CompiledMessageErrorSites,
}

impl CompiledSqlValuesProgram {
    fn structured_side_error(
        &self,
        execution_now: Timestamp,
        reason: String,
        span: VmSpan,
    ) -> StructuredMessageError {
        let site = self.error_sites.get(&span);
        let operation = match site {
            Some(site) => site.operation,
            None => MessageErrorOperation::Values,
        };
        structured_message_error(
            execution_now,
            MessageErrorCode::Evaluation,
            reason,
            operation,
            site.and_then(|site| site.operation_index),
            site.map(|site| site.fields.iter().cloned())
                .into_iter()
                .flatten(),
        )
    }
}

pub(in crate::runtime) type EmitterRuntimeResult<T> = Result<T, Report<EmitterRuntimeError>>;

type EmitterPublishResult = Result<Option<PublishReport>, EmitterPublishFailure>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmitterPublishBatchOwner {
    Caller,
    Buffer,
    Sink,
}

struct EmitterPublishFailure {
    error: Report<EmitterRuntimeError>,
    batch_owner: EmitterPublishBatchOwner,
}

impl EmitterPublishFailure {
    fn caller(error: Report<EmitterRuntimeError>) -> Self {
        Self {
            error,
            batch_owner: EmitterPublishBatchOwner::Caller,
        }
    }

    fn buffer(error: Report<EmitterRuntimeError>) -> Self {
        Self {
            error,
            batch_owner: EmitterPublishBatchOwner::Buffer,
        }
    }

    fn sink(error: Report<EmitterRuntimeError>) -> Self {
        Self {
            error,
            batch_owner: EmitterPublishBatchOwner::Sink,
        }
    }

    fn drain_failed_batches(
        self,
        current: &mut Option<EmitterPublishBatch>,
        buffer: &mut EmitterBatchBuffer,
    ) -> (Report<EmitterRuntimeError>, Vec<EmitterPublishBatch>) {
        let batches = match self.batch_owner {
            EmitterPublishBatchOwner::Caller => {
                let mut batches = buffer.drain_pending();
                batches.extend(current.take());
                batches
            }
            EmitterPublishBatchOwner::Buffer => {
                current.take();
                buffer.drain_pending()
            }
            EmitterPublishBatchOwner::Sink => {
                current.take();
                Vec::new()
            }
        };
        (self.error, batches)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(in crate::runtime) enum EmitterRuntimeError {
    #[error("invalid emitter sink configuration")]
    InvalidSinkConfig,
    #[error("failed to initialize emitter sink")]
    InitializeSink,
    #[error("emitter sink client is not initialized")]
    SinkNotInitialized,
    #[error("emitter flush policy is not initialized")]
    FlushPolicyNotInitialized,
    #[error("fault injector failed emitter publish")]
    FaultInjected,
    #[error("emitter shutdown while stalled")]
    ShutdownWhileStalled,
    #[error("the emitter could not resolve its flush cadence against the domain clock")]
    FlushTiming,
    #[error("the emitter retry deadline is outside the monotonic clock range")]
    RetryTiming,
    #[error("emitter stop deadline elapsed")]
    StopDeadlineElapsed,
    #[error("failed to encode emitter batch")]
    EncodeBatch,
    #[error("failed to publish emitter batch")]
    PublishBatch,
    #[error("emitter publish is stalled")]
    PublishStalled,
}

impl EmitterRuntimeError {
    fn is_retryable_publish_failure(self) -> bool {
        match self {
            Self::SinkNotInitialized | Self::PublishBatch | Self::PublishStalled => true,
            Self::FlushPolicyNotInitialized
            | Self::InvalidSinkConfig
            | Self::InitializeSink
            | Self::FaultInjected
            | Self::ShutdownWhileStalled
            | Self::StopDeadlineElapsed
            | Self::FlushTiming
            | Self::RetryTiming
            | Self::EncodeBatch => false,
        }
    }
}

impl PublishReport {
    fn flushed(messages: u64, bytes: u64, domain_timestamp: Timestamp) -> Self {
        Self {
            messages,
            bytes,
            domain_timestamp,
        }
    }

    fn merge(self, other: Self) -> Self {
        Self {
            messages: self
                .messages
                .checked_add(other.messages)
                .assured("both counts total messages this node already published"),
            bytes: self.bytes.checked_add(other.bytes).assured(BYTES_IN_MEMORY),
            domain_timestamp: self.domain_timestamp.max(other.domain_timestamp),
        }
    }

    fn merge_optional(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        match (left, right) {
            (Some(left), Some(right)) => Some(left.merge(right)),
            (Some(report), None) | (None, Some(report)) => Some(report),
            (None, None) => None,
        }
    }
}

/// The emitter's own buffer of published batches and the cadence deadline that releases them.
///
/// The cadence is the emitter's explicit `FLUSH EACH` or `FLUSH IMMEDIATE` policy and nothing
/// else. Retry backoff and acknowledgement keepalive are owned by [`EmitterRetrySchedule`], so a
/// failed publish attempt leaves this buffer's pending batches, acknowledgements and cadence
/// deadline exactly as they were.
#[derive(Default)]
struct EmitterBatchBuffer {
    flush_policy: Option<RuntimeFlushPolicy>,
    pending: Vec<EmitterPublishBatch>,
    pending_messages: u64,
    pending_bytes: u64,
    cadence: BranchBufferTimer,
    buffered_messages: Arc<EmitterBufferedMessages>,
}

impl EmitterBatchBuffer {
    fn new(
        context: &EmitterSinkContext,
        flush_policy: &FlushPolicy,
        buffered_messages: Arc<EmitterBufferedMessages>,
    ) -> Self {
        Self {
            flush_policy: context.parse_flush_policy("emitter", flush_policy),
            pending: Vec::new(),
            pending_messages: 0,
            pending_bytes: 0,
            cadence: BranchBufferTimer::default(),
            buffered_messages,
        }
    }

    fn update_buffered_messages(&self) {
        self.buffered_messages
            .set_generic(self.pending_messages.arch_into());
    }

    fn reconfigure(
        &mut self,
        context: &EmitterSinkContext,
        flush_policy: &FlushPolicy,
    ) -> EmitterRuntimeResult<()> {
        self.flush_policy = context.parse_flush_policy("emitter", flush_policy);
        self.cadence.clear();
        if self.pending.is_empty() {
            return Ok(());
        }
        let Some(flush_policy) = self.flush_policy else {
            return Ok(());
        };
        self.arm_cadence(context, flush_policy)
    }

    fn arm_cadence(
        &mut self,
        context: &EmitterSinkContext,
        flush_policy: RuntimeFlushPolicy,
    ) -> EmitterRuntimeResult<()> {
        let snapshot = context.execution_snapshot()?;
        self.cadence
            .arm_flush(flush_policy, &context.clock, &snapshot)
            .change_context(EmitterRuntimeError::FlushTiming)
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn deadline(&self) -> Option<BranchBufferDeadline> {
        self.cadence.deadline()
    }

    fn push(
        &mut self,
        context: &EmitterSinkContext,
        batch: EmitterPublishBatch,
    ) -> EmitterRuntimeResult<bool> {
        let Some(flush_policy) = self.flush_policy else {
            return Err(Report::new(EmitterRuntimeError::FlushPolicyNotInitialized));
        };
        self.pending_messages = self
            .pending_messages
            .checked_add(batch.message_count())
            .assured("both counts total messages this emitter already holds in memory");
        self.pending_bytes = self
            .pending_bytes
            .checked_add(batch.estimated_bytes())
            .assured("both counts estimate bytes of batches this node already holds in memory");
        self.pending.push(batch);
        self.update_buffered_messages();
        self.arm_cadence(context, flush_policy)?;
        Ok(flush_policy.size_boundary_reached(self.pending_bytes))
    }

    /// Retains a batch that arrived while the emitter was draining.
    ///
    /// A drain publishes everything the emitter holds regardless of cadence, so the batch it
    /// accepts needs no deadline: the drain that took it is what releases it. This also keeps a
    /// drain independent of the domain clock, which a stopping domain has already uninstalled.
    fn retain_for_drain(&mut self, batch: EmitterPublishBatch) -> EmitterRuntimeResult<()> {
        if self.flush_policy.is_none() {
            return Err(Report::new(EmitterRuntimeError::FlushPolicyNotInitialized));
        }
        self.pending_messages = self
            .pending_messages
            .checked_add(batch.message_count())
            .assured("both counts total messages this emitter already holds in memory");
        self.pending_bytes = self
            .pending_bytes
            .checked_add(batch.estimated_bytes())
            .assured("both counts estimate bytes of batches this node already holds in memory");
        self.pending.push(batch);
        self.update_buffered_messages();
        Ok(())
    }

    fn is_due(&self, context: &EmitterSinkContext) -> EmitterRuntimeResult<bool> {
        let snapshot = context.execution_snapshot()?;
        self.cadence
            .is_due(&context.clock, &snapshot)
            .change_context(EmitterRuntimeError::FlushTiming)
    }

    fn should_flush(
        &self,
        context: &EmitterSinkContext,
        force: bool,
    ) -> EmitterRuntimeResult<bool> {
        if self.pending.is_empty() {
            return Ok(false);
        }
        if force {
            return Ok(true);
        }
        self.is_due(context)
    }

    fn pending_acks(&self) -> AckSet {
        AckSet::merged(self.pending.iter().map(EmitterPublishBatch::merged_acks))
    }

    fn drain_pending(&mut self) -> Vec<EmitterPublishBatch> {
        let pending = std::mem::take(&mut self.pending);
        self.pending_messages = 0;
        self.pending_bytes = 0;
        self.cadence.clear();
        self.update_buffered_messages();
        pending
    }

    fn clear(&mut self) {
        self.pending.clear();
        self.pending_messages = 0;
        self.pending_bytes = 0;
        self.cadence.clear();
        self.update_buffered_messages();
    }

    fn report(&self) -> Option<PublishReport> {
        if self.pending.is_empty() {
            return None;
        }
        let bytes = self.pending_bytes;
        let domain_timestamp = self
            .pending
            .iter()
            .map(|batch| batch.domain_timestamp().unwrap_or(batch.execution_now))
            .max()
            .verified("a non-empty emitter buffer has an observation time for every batch");
        Some(PublishReport::flushed(
            self.pending_messages,
            bytes,
            domain_timestamp,
        ))
    }
}

impl Drop for EmitterBatchBuffer {
    fn drop(&mut self) {
        self.pending_acks().no_ack("emitter dropped buffered batch");
        self.buffered_messages.set_generic(0);
    }
}

/// One deferral of an emitter's publish attempt: how long the emitter waits, which
/// acknowledgements it keeps alive meanwhile, and what the operator is told about the wait.
struct EmitterRetryDeferral<'a> {
    wait: Duration,
    acks: AckSet,
    waiting_for_stall_clear: bool,
    reason: Option<&'a str>,
}

/// The emitter's physical retry state: when the next publish attempt is allowed, and how often
/// the acknowledgements it is holding are kept alive until then.
///
/// Both deadlines are monotonic. Retry backoff measures real unavailability of an external system
/// and an acknowledgement keepalive measures a real upstream liveness expectation, so neither is
/// scaled by the domain's pace. While a retry is scheduled it replaces the emitter's cadence wake,
/// which stays armed and unchanged underneath it.
#[derive(Default)]
struct EmitterRetrySchedule {
    retry_at: Option<PhysicalDeadline>,
    ack_alive_at: Option<PhysicalDeadline>,
    acks: AckSet,
    waiting_for_stall_clear: bool,
}

impl EmitterRetrySchedule {
    fn is_active(&self) -> bool {
        self.retry_at.is_some()
    }

    fn schedule(
        &mut self,
        delay: Duration,
        acks: AckSet,
        waiting_for_stall_clear: bool,
    ) -> EmitterRuntimeResult<()> {
        let retry_at = PhysicalDeadlineCapability::new()
            .after(delay)
            .change_context(EmitterRuntimeError::RetryTiming)?;
        self.retry_at = Some(retry_at);
        if !acks.is_empty() {
            self.acks = acks;
        }
        self.ack_alive_at = (!self.acks.is_empty()).then(|| Self::next_ack_alive_at(retry_at));
        self.waiting_for_stall_clear = waiting_for_stall_clear;
        Ok(())
    }

    fn include_acks(&mut self, acks: AckSet) {
        if acks.is_empty() {
            return;
        }
        self.acks = AckSet::merged([std::mem::take(&mut self.acks), acks]);
        if let Some(retry_at) = self.retry_at
            && self.ack_alive_at.is_none()
        {
            self.ack_alive_at = Some(Self::next_ack_alive_at(retry_at));
        }
    }

    fn next_ack_alive_at(retry_at: PhysicalDeadline) -> PhysicalDeadline {
        PhysicalDeadlineCapability::new()
            .after(RETRY_ACK_ALIVE_EACH)
            .assured("the acknowledgement keepalive interval is a fixed hundred milliseconds")
            .min(retry_at)
    }

    /// The wake the emitter asks for: the retry and keepalive deadlines while a retry is
    /// scheduled, and otherwise the emitter's ordinary flush cadence.
    fn wake(&self, ordinary: RuntimeWake) -> RuntimeWake {
        let Some(retry_at) = self.retry_at else {
            return ordinary;
        };
        let wake = RuntimeWake::never().with_physical(retry_at);
        match self.ack_alive_at {
            Some(ack_alive_at) => wake.with_physical(ack_alive_at),
            None => wake,
        }
    }

    fn retry_is_due(&mut self) -> bool {
        let Some(retry_at) = self.retry_at else {
            return true;
        };
        let physical_time = PhysicalDeadlineCapability::new();
        if physical_time.is_reached(retry_at) {
            self.retry_at = None;
            self.ack_alive_at = None;
            return true;
        }
        if self
            .ack_alive_at
            .is_some_and(|ack_alive_at| physical_time.is_reached(ack_alive_at))
        {
            self.acks.ack_alive();
            self.ack_alive_at = Some(Self::next_ack_alive_at(retry_at));
        }
        false
    }

    fn release_if_stall_cleared(&mut self, stalled: bool) -> bool {
        if !self.waiting_for_stall_clear || stalled {
            return false;
        }
        self.retry_at = None;
        self.ack_alive_at = None;
        self.waiting_for_stall_clear = false;
        true
    }

    /// Defers the next publish attempt and records the transient error that explains the wait.
    ///
    /// Constructing the monotonic deadline is the only fallible part. A configured or
    /// server-supplied wait the monotonic clock cannot represent leaves no retry scheduled, so the
    /// emitter falls back to its unchanged flush cadence and the failure is recorded as the
    /// emitter's transient error instead of disappearing.
    fn defer(&mut self, context: &EmitterSinkContext, deferral: EmitterRetryDeferral<'_>) {
        let EmitterRetryDeferral {
            wait,
            acks,
            waiting_for_stall_clear,
            reason,
        } = deferral;
        match self.schedule(wait, acks, waiting_for_stall_clear) {
            Ok(()) => {
                if let Some(reason) = reason {
                    context.runtime.record_emitter_transient_error_with_backoff(
                        &context.domain,
                        &context.emitter,
                        reason,
                        wait,
                    );
                }
            }
            Err(error) => {
                context.runtime.record_emitter_transient_error(
                    &context.domain,
                    &context.emitter,
                    emitter_error_message(&error),
                );
            }
        }
    }

    fn clear(&mut self) {
        self.retry_at = None;
        self.ack_alive_at = None;
        self.acks = AckSet::empty();
        self.waiting_for_stall_clear = false;
    }
}


fn compile_sql_values_program(
    label: &'static str,
    namespace: &'static str,
    domain: &DomainName,
    emitter: &EmitterName,
    values: &[ClickHouseValueMapping],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledSqlValuesProgram, RuntimeError> {
    if values.is_empty() {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "{label} emitter '{}' requires at least one VALUES mapping",
                emitter.as_str()
            ),
        });
    }
    let assignments = values
        .iter()
        .enumerate()
        .map(|(index, mapping)| {
            Ok(nervix_models::Assignment {
                target: nervix_models::AssignmentTarget::bare(
                    FieldName::parse(&format!("c{index}")).map_err(|error| error.to_string())?,
                ),
                value: mapping.expression.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()
        .map_err(|reason| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "{label} VALUES for '{}' is invalid: {reason}",
                emitter.as_str()
            ),
        })?;
    let parsed = lower_route_construction(
        &nervix_models::RouteConstruction {
            assignments,
            ..nervix_models::RouteConstruction::default()
        },
        nervix_vm::SemanticNamespaces::new("input", namespace),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "{label} VALUES for '{}' is invalid: {reason}",
            emitter.as_str()
        ),
    })?;
    let empty_sink_schema =
        StdArc::new(arrow_schema::Schema::new(Vec::<arrow_schema::Field>::new()));
    let infer_bindings = vec![
        VmCompileBinding::writeonly(namespace, empty_sink_schema),
        VmCompileBinding::readonly("input", input_schema.clone()),
        VmCompileBinding::readonly("message", input_schema.clone()),
    ];
    let inferred_fields = infer_vm_set_expr_types_for_bindings_with_udfs(
        &parsed,
        infer_bindings,
        runtime_udf_signatures(udfs),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "{label} VALUES type inference failed for '{}': {}",
            emitter.as_str(),
            error.message
        ),
    })?;
    let output_schema = StdArc::new(arrow_schema::Schema::new(
        inferred_fields
            .into_iter()
            .map(|inferred| {
                arrow_schema::Field::new(inferred.field, inferred.data_type, inferred.nullable)
            })
            .collect::<Vec<_>>(),
    ));
    let compile_bindings = vec![
        VmCompileBinding::writeonly(namespace, output_schema.clone()),
        VmCompileBinding::readonly("input", input_schema.clone()),
        VmCompileBinding::readonly("message", input_schema),
    ];
    let mut error_sites = compiled_message_error_sites(
        &parsed,
        &vec![MessageErrorOperation::Values; parsed.inner.set.len()],
        None,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "{label} VALUES message-error metadata for '{}' is invalid: {reason}",
            emitter.as_str()
        ),
    })?;
    for site in error_sites.values_mut() {
        if site.operation != MessageErrorOperation::Values {
            continue;
        }
        let Some(index) = site.operation_index.map(|index| index.arch_into()) else {
            continue;
        };
        let Some(mapping) = values.get(index) else {
            continue;
        };
        let internal_target = format!("{namespace}.c{index}");
        let external_target = format!("{namespace}.{}", mapping.column);
        site.fields = SortedSet::from_unsorted(
            site.fields
                .iter()
                .map(|field| {
                    if field.as_str() == internal_target {
                        FieldPath::new(external_target.clone())
                    } else {
                        field.clone()
                    }
                })
                .collect(),
        );
    }
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema.clone(),
        VmSchemaSensitivity::default(),
        compile_bindings,
        runtime_udf_compile_options(
            udfs,
            VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                allow_sensitive_output: false,
                ..VmCompileOptions::default()
            },
        ),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "{label} VALUES compile failed for '{}': {}",
            emitter.as_str(),
            error.message
        ),
    })?;
    Ok(CompiledSqlValuesProgram {
        program: Arc::new(compiled),
        label,
        error_sites,
    })
}

fn compile_clickhouse_values_program(
    domain: &DomainName,
    emitter: &EmitterName,
    values: &[ClickHouseValueMapping],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledSqlValuesProgram, RuntimeError> {
    compile_sql_values_program(
        "ClickHouse",
        "clickhouse",
        domain,
        emitter,
        values,
        input_schema,
        udfs,
    )
}

fn compile_postgres_values_program(
    domain: &DomainName,
    emitter: &EmitterName,
    values: &[PostgresValueMapping],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledSqlValuesProgram, RuntimeError> {
    compile_sql_values_program(
        "Postgres",
        "postgres",
        domain,
        emitter,
        values,
        input_schema,
        udfs,
    )
}

fn compile_mysql_values_program(
    domain: &DomainName,
    emitter: &EmitterName,
    values: &[MySqlValueMapping],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledSqlValuesProgram, RuntimeError> {
    compile_sql_values_program(
        "MySQL",
        "mysql",
        domain,
        emitter,
        values,
        input_schema,
        udfs,
    )
}

fn compile_mongodb_values_program(
    domain: &DomainName,
    emitter: &EmitterName,
    values: &[MongoDbValueMapping],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledSqlValuesProgram, RuntimeError> {
    compile_sql_values_program(
        "MongoDB",
        "mongodb",
        domain,
        emitter,
        values,
        input_schema,
        udfs,
    )
}

fn compile_iceberg_values_program(
    domain: &DomainName,
    emitter: &EmitterName,
    values: &[IcebergValueMapping],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledSqlValuesProgram, RuntimeError> {
    compile_sql_values_program(
        "Iceberg",
        "iceberg",
        domain,
        emitter,
        values,
        input_schema,
        udfs,
    )
}

async fn sql_mapped_batch_values(
    program: &CompiledSqlValuesProgram,
    mappings: &[ClickHouseValueMapping],
    batch: &RelayRecordBatch,
    execution_now: Timestamp,
) -> EmitterRuntimeResult<Vec<Result<Vec<serde_json::Value>, StructuredMessageError>>> {
    let output = execute_sql_values_program(program, batch, execution_now).await?;
    let row_count = output.row_count();
    let mut rows = Vec::with_capacity(row_count);
    for row in 0..row_count {
        if let Some(side_error) = output.errors().row(row).first() {
            rows.push(Err(program.structured_side_error(
                execution_now,
                format!(
                    "{} VALUES side error {}: {} at {}",
                    program.label,
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ),
                side_error.span,
            )));
            continue;
        }
        let mapped = mappings
            .iter()
            .enumerate()
            .map(|(index, _mapping)| {
                let field = format!("c{index}");
                vm_output_value(&output, row, &field).map(|value| match value.as_ref() {
                    Some(value) => runtime_value_to_json(value),
                    None => serde_json::Value::Null,
                })
            })
            .collect::<Result<Vec<_>, _>>();
        let mapped = match mapped {
            Ok(mapped) => mapped,
            Err(error) => {
                rows.push(Err(structured_message_error(
                    execution_now,
                    MessageErrorCode::Validation,
                    format!(
                        "{} VALUES failed to decode output row: {error}",
                        program.label
                    ),
                    MessageErrorOperation::Values,
                    None,
                    std::iter::empty(),
                )));
                continue;
            }
        };
        rows.push(Ok(mapped));
    }
    Ok(rows)
}

async fn execute_sql_values_program(
    program: &CompiledSqlValuesProgram,
    batch: &RelayRecordBatch,
    execution_now: Timestamp,
) -> EmitterRuntimeResult<VmTypedBatch> {
    let side_inputs = HashMap::default();
    let lookup_columns = HashMap::default();
    let input = project_vm_input_batch(
        &program.program.input_schema,
        &VmInputProjectionSources {
            carrier: &batch.batch,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &batch.keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: None,
        },
        None,
    )
    .map_err(|error| Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(error))?;
    let result = execute_program_with_selection_in_context(
        &program.program,
        &input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| {
        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
            "{} VALUES execution failed: {error}",
            program.label
        ))
    })?;
    let row_count = batch.batch.batch().num_rows();
    if result.batch.row_count() != row_count {
        return Err(
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "{} VALUES produced {} rows for {} input records",
                program.label,
                result.batch.row_count(),
                row_count
            )),
        );
    }
    Ok(result.batch)
}

fn runtime_value_to_json(value: &RuntimeValue) -> serde_json::Value {
    match value {
        RuntimeValue::U8(value) => serde_json::Value::from(*value),
        RuntimeValue::I8(value) => serde_json::Value::from(*value),
        RuntimeValue::U16(value) => serde_json::Value::from(*value),
        RuntimeValue::I16(value) => serde_json::Value::from(*value),
        RuntimeValue::U32(value) => serde_json::Value::from(*value),
        RuntimeValue::I32(value) => serde_json::Value::from(*value),
        RuntimeValue::U64(value) => serde_json::Value::from(*value),
        RuntimeValue::I64(value) => serde_json::Value::from(*value),
        RuntimeValue::Bool(value) => serde_json::Value::from(*value),
        RuntimeValue::String(value) => serde_json::Value::from(value.clone()),
        RuntimeValue::Datetime(value) => serde_json::Value::from(value.to_rfc3339()),
        RuntimeValue::F32(value) => serde_json::Value::from(value.into_inner()),
        RuntimeValue::F64(value) => serde_json::Value::from(value.into_inner()),
        RuntimeValue::Array(values) | RuntimeValue::Vec(values) => {
            serde_json::Value::Array(values.iter().map(runtime_value_to_json).collect())
        }
    }
}

fn emitter_report(
    context: EmitterRuntimeError,
    error: impl std::fmt::Display,
) -> Report<EmitterRuntimeError> {
    Report::new(context).attach_printable(error.to_string())
}

fn emitter_config_error(error: impl std::fmt::Display) -> Report<EmitterRuntimeError> {
    emitter_report(EmitterRuntimeError::InvalidSinkConfig, error)
}

fn emitter_init_error(error: impl std::fmt::Display) -> Report<EmitterRuntimeError> {
    emitter_report(EmitterRuntimeError::InitializeSink, error)
}

fn emitter_publish_error(error: impl std::fmt::Display) -> Report<EmitterRuntimeError> {
    emitter_report(EmitterRuntimeError::PublishBatch, error)
}

#[derive(Debug, Clone, Copy)]
struct EmitterMinimumRetryDelay(Duration);

fn emitter_publish_error_with_minimum_retry_delay(
    error: impl std::fmt::Display,
    delay: Duration,
) -> Report<EmitterRuntimeError> {
    emitter_publish_error(error).attach(EmitterMinimumRetryDelay(delay))
}

fn emitter_minimum_retry_delay(error: &Report<EmitterRuntimeError>) -> Duration {
    match error.downcast_ref::<EmitterMinimumRetryDelay>() {
        Some(attachment) => attachment.0,
        None => Duration::ZERO,
    }
}

fn emitter_retry_delay(
    backoff: &mut RuntimeReconnectBackoff,
    error: &Report<EmitterRuntimeError>,
) -> Duration {
    backoff
        .take_next_delay()
        .max(emitter_minimum_retry_delay(error))
}

async fn await_emitter_confirmation<F>(acks: &AckSet, future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::pin!(future);
    loop {
        tokio::task::consume_budget().await;
        acks.ack_alive();
        tokio::select! {
            result = &mut future => return result,
            _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => {}
        }
    }
}

fn emitter_config_value(
    config: &[nervix_models::ClientConfigEntry],
    key: &str,
    missing_message: impl FnOnce() -> String,
) -> EmitterRuntimeResult<String> {
    client_config_value(config, key, missing_message).map_err(emitter_config_error)
}

fn emitter_optional_bool_client_config_value(
    config: &[nervix_models::ClientConfigEntry],
    key: &str,
) -> EmitterRuntimeResult<Option<bool>> {
    optional_bool_client_config_value(config, key).map_err(emitter_config_error)
}

fn emitter_read_tls_file(path: &PathBuf, label: &str) -> EmitterRuntimeResult<Vec<u8>> {
    read_tls_file(path, label).map_err(emitter_config_error)
}

fn emitter_service_url_has_scheme(
    raw: &str,
    label: &'static str,
    expected_scheme: &str,
) -> EmitterRuntimeResult<bool> {
    ServiceUrl::new(raw, label)
        .has_scheme(expected_scheme)
        .map_err(emitter_config_error)
}

impl EmitterSinkContext {
    fn report_init_error(&self, sink: &str, error: &str) {
        self.runtime.events().report_error(format!(
            "failed to initialize {sink} emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to initialize emitter sink"
        );
    }

    fn report_publish_error(&self, sink: &str, error: &str) {
        self.runtime.events().report_error(format!(
            "failed to publish {sink} message for emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to publish emitter message"
        );
    }

    fn report_flush_error(&self, sink: &str, error: &str) {
        self.runtime.events().report_error(format!(
            "failed to flush {sink} rows for emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to flush emitter rows"
        );
    }

    /// Reads the emitter's domain execution time for a cadence decision.
    fn execution_snapshot(&self) -> EmitterRuntimeResult<DomainExecutionSnapshot> {
        self.clock
            .snapshot()
            .change_context(EmitterRuntimeError::FlushTiming)
    }

    fn parse_flush_policy(&self, kind: &str, policy: &FlushPolicy) -> Option<RuntimeFlushPolicy> {
        match Runtime::parse_runtime_node_flush_policy(&self.domain, kind, &self.emitter, policy) {
            Ok(policy) => Some(policy),
            Err(error) => {
                self.runtime.events().report_error(error.to_string());
                warn!(
                    domain = self.domain.as_str(),
                    emitter = self.emitter.as_str(),
                    error = %error,
                    "failed to parse emitter flush policy"
                );
                None
            }
        }
    }
}

enum SinkEmitter {
    Kafka(KafkaEmitter),
    Pulsar(PulsarEmitter),
    RabbitMq(RabbitMqEmitter),
    Redis(RedisEmitter),
    Mqtt(MqttEmitter),
    Nats(NatsEmitter),
    ZeroMq(ZeroMqEmitter),
    Syslog(SyslogEmitter),
    Sqs(SqsEmitter),
    Sentry(SentryEmitter),
    Otel(OtelEmitter),
    ClickHouse(ClickHouseEmitter),
    Postgres(PostgresEmitter),
    MySql(MySqlEmitter),
    MongoDb(MongoDbEmitter),
    Iceberg(IcebergEmitter),
    Missing { reason: String },
}

#[derive(Clone)]
struct SinkEmitterRuntime {
    input_schema: Arc<CompiledSchema>,
    buffered_messages: Arc<EmitterBufferedMessages>,
}

struct SinkEmitterInit<'a> {
    sink: &'a EmitSink,
    flush_policy: &'a FlushPolicy,
    publishing: EmitterPublishingSettings,
    client: Option<&'a Model>,
    resolved: Option<&'a ResolvedClientConfig>,
    catalog_client: Option<&'a Model>,
    catalog_resolved: Option<&'a ResolvedClientConfig>,
    context: &'a EmitterSinkContext,
    runtime: SinkEmitterRuntime,
}

impl SinkEmitter {
    async fn new_until_cancelled(
        init: SinkEmitterInit<'_>,
        work_cancel_rx: &mut watch::Receiver<bool>,
    ) -> Self {
        tokio::select! {
            biased;
            _ = wait_for_emitter_work_cancel(work_cancel_rx) => Self::Missing {
                reason: "emitter sink initialization canceled while stopping".to_string(),
            },
            emitter = Self::new(init) => emitter,
        }
    }

    async fn new(init: SinkEmitterInit<'_>) -> Self {
        let SinkEmitterInit {
            sink,
            flush_policy,
            publishing,
            client,
            resolved,
            catalog_client,
            catalog_resolved,
            context,
            runtime,
        } = init;
        let SinkEmitterRuntime {
            input_schema,
            buffered_messages,
        } = runtime;
        match (sink, client, catalog_client) {
            (EmitSink::Kafka { .. }, Some(Model::ClientKafka(client)), _) => {
                let result = match publishing.broker_mode() {
                    Ok(mode) => KafkaEmitter::new(client, resolved, mode),
                    Err(error) => Err(error),
                };
                Self::from_result("kafka", context, result).map(Self::Kafka)
            }
            (EmitSink::Pulsar { topic, .. }, Some(Model::ClientPulsar(client)), _) => {
                let mode = match publishing.broker_mode() {
                    Ok(mode) => mode,
                    Err(error) => {
                        return Self::missing_after_emitter_init_error("pulsar", context, &error);
                    }
                };
                match PulsarEmitter::new(client, resolved, topic, mode).await {
                    Ok(emitter) => Self::Pulsar(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("pulsar", context, &error),
                }
            }
            (EmitSink::RabbitMq { queue, .. }, Some(Model::ClientRabbitMq(client)), _) => {
                let mode = match publishing.broker_mode() {
                    Ok(mode) => mode,
                    Err(error) => {
                        return Self::missing_after_emitter_init_error("rabbitmq", context, &error);
                    }
                };
                match RabbitMqEmitter::new(client, resolved, queue, mode).await {
                    Ok(emitter) => Self::RabbitMq(emitter),
                    Err(error) => {
                        Self::missing_after_emitter_init_error("rabbitmq", context, &error)
                    }
                }
            }
            (EmitSink::Redis { .. }, Some(model @ Model::ClientRedis(client)), _) => {
                match RedisEmitter::new(model, client, resolved, context).await {
                    Ok(emitter) => Self::Redis(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("redis", context, &error),
                }
            }
            (EmitSink::Mqtt { topic, .. }, Some(Model::ClientMqtt(client)), _) => {
                let result = match publishing.mqtt_mode() {
                    Ok(mode) => MqttEmitter::new(
                        client,
                        resolved,
                        topic,
                        context,
                        mode,
                        publishing.retry_policy,
                    ),
                    Err(error) => Err(error),
                };
                Self::from_result("mqtt", context, result).map(Self::Mqtt)
            }
            (EmitSink::Nats { subject, .. }, Some(Model::ClientNats(client)), _) => {
                let mode = match publishing.nats_mode() {
                    Ok(mode) => mode,
                    Err(error) => {
                        return Self::missing_after_emitter_init_error("nats", context, &error);
                    }
                };
                match NatsEmitter::new(client, resolved, subject, mode, publishing.retry_policy)
                    .await
                {
                    Ok(emitter) => Self::Nats(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("nats", context, &error),
                }
            }
            (EmitSink::ZeroMq { .. }, Some(Model::ClientZeroMq(client)), _) => {
                match ZeroMqEmitter::new(client, resolved).await {
                    Ok(emitter) => Self::ZeroMq(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("zeromq", context, &error),
                }
            }
            (EmitSink::Syslog { .. }, Some(Model::ClientSyslog(client)), _) => {
                match SyslogEmitter::new(client, resolved).await {
                    Ok(emitter) => Self::Syslog(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("syslog", context, &error),
                }
            }
            (EmitSink::Sqs { queue, .. }, Some(Model::ClientSqs(client)), _) => {
                let mode = match publishing.sqs_mode() {
                    Ok(mode) => mode,
                    Err(error) => {
                        return Self::missing_after_emitter_init_error("sqs", context, &error);
                    }
                };
                match SqsEmitter::new(client, resolved, queue, mode).await {
                    Ok(emitter) => Self::Sqs(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("sqs", context, &error),
                }
            }
            (EmitSink::Sentry { .. }, Some(Model::ClientSentry(client)), _) => {
                Self::from_result("sentry", context, SentryEmitter::new(client, resolved))
                    .map(Self::Sentry)
            }
            (
                EmitSink::Otel {
                    signal,
                    values,
                    attributes,
                    resource,
                    scope,
                    ..
                },
                Some(Model::ClientOtel(client)),
                _,
            ) => Self::Otel(OtelEmitter::new(OtelEmitterInit {
                client,
                resolved,
                context,
                signal,
                values,
                attributes,
                resource,
                scope: scope.as_ref(),
                input_schema: input_schema.arrow_schema(),
            })),
            (EmitSink::ClickHouse { values, .. }, Some(Model::ClientClickHouse(client)), _) => {
                Self::ClickHouse(ClickHouseEmitter::new(
                    client,
                    resolved,
                    context,
                    values,
                    input_schema.arrow_schema(),
                ))
            }
            (EmitSink::Postgres { values, .. }, Some(model @ Model::ClientPostgres(client)), _) => {
                Self::Postgres(
                    PostgresEmitter::new(
                        model,
                        client,
                        resolved,
                        context,
                        values,
                        input_schema.arrow_schema(),
                    )
                    .await,
                )
            }
            (EmitSink::MySql { values, .. }, Some(model @ Model::ClientMySql(client)), _) => {
                Self::MySql(
                    MySqlEmitter::new(
                        model,
                        client,
                        resolved,
                        context,
                        values,
                        input_schema.arrow_schema(),
                    )
                    .await,
                )
            }
            (EmitSink::MongoDb { values, .. }, Some(model @ Model::ClientMongoDb(client)), _) => {
                Self::MongoDb(
                    MongoDbEmitter::new(
                        model,
                        client,
                        resolved,
                        context,
                        values,
                        input_schema.arrow_schema(),
                    )
                    .await,
                )
            }
            (
                EmitSink::Iceberg {
                    backend: IcebergStorageBackend::S3,
                    table,
                    values,
                    location,
                    catalog,
                    commit_each,
                    max_commit_size,
                    ..
                },
                Some(Model::ClientS3(client)),
                Some(Model::ClientIcebergRest(catalog_client)),
            ) => Self::from_iceberg_result(
                context,
                IcebergEmitter::new(IcebergEmitterInit {
                    client: IcebergEmitterClientConfig::S3(client),
                    resolved,
                    catalog_client,
                    catalog_resolved,
                    context,
                    table,
                    values,
                    location,
                    catalog,
                    flush_policy,
                    commit_each,
                    max_commit_size,
                    input_schema,
                    buffered_messages: buffered_messages.clone(),
                })
                .await,
            ),
            (
                EmitSink::Iceberg {
                    backend: IcebergStorageBackend::Gcs,
                    table,
                    values,
                    location,
                    catalog,
                    commit_each,
                    max_commit_size,
                    ..
                },
                Some(Model::ClientGcs(client)),
                Some(Model::ClientIcebergRest(catalog_client)),
            ) => Self::from_iceberg_result(
                context,
                IcebergEmitter::new(IcebergEmitterInit {
                    client: IcebergEmitterClientConfig::Gcs(client),
                    resolved,
                    catalog_client,
                    catalog_resolved,
                    context,
                    table,
                    values,
                    location,
                    catalog,
                    flush_policy,
                    commit_each,
                    max_commit_size,
                    input_schema,
                    buffered_messages: buffered_messages.clone(),
                })
                .await,
            ),
            (
                EmitSink::Iceberg {
                    backend: IcebergStorageBackend::AzureBlob,
                    table,
                    values,
                    location,
                    catalog,
                    commit_each,
                    max_commit_size,
                    ..
                },
                Some(Model::ClientAzureBlob(client)),
                Some(Model::ClientIcebergRest(catalog_client)),
            ) => Self::from_iceberg_result(
                context,
                IcebergEmitter::new(IcebergEmitterInit {
                    client: IcebergEmitterClientConfig::AzureBlob(client),
                    resolved,
                    catalog_client,
                    catalog_resolved,
                    context,
                    table,
                    values,
                    location,
                    catalog,
                    flush_policy,
                    commit_each,
                    max_commit_size,
                    input_schema,
                    buffered_messages,
                })
                .await,
            ),
            _ => Self::Missing {
                reason: format!("{} emitter sink client is not initialized", sink.label()),
            },
        }
    }

    fn from_result<T>(
        sink: &str,
        context: &EmitterSinkContext,
        result: EmitterRuntimeResult<T>,
    ) -> SinkEmitterResult<T> {
        match result {
            Ok(value) => SinkEmitterResult::Ready(value),
            Err(error) => {
                let reason = emitter_error_message(&error);
                context.report_init_error(sink, &reason);
                SinkEmitterResult::Missing { reason }
            }
        }
    }

    fn missing_after_emitter_init_error(
        sink: &str,
        context: &EmitterSinkContext,
        error: &Report<EmitterRuntimeError>,
    ) -> Self {
        let reason = emitter_error_message(error);
        context.report_init_error(sink, &reason);
        Self::Missing { reason }
    }

    fn from_iceberg_result(
        context: &EmitterSinkContext,
        result: IcebergEmitterResult<IcebergEmitter>,
    ) -> Self {
        match result {
            Ok(emitter) => Self::Iceberg(emitter),
            Err(error) => {
                let reason = iceberg_error_message(&error);
                context.report_init_error("iceberg", &reason);
                Self::Missing { reason }
            }
        }
    }

    /// The wake that releases this emitter's buffered work on its own flush cadence.
    fn cadence_wake(&self, clock: &DomainClock, buffer: &EmitterBatchBuffer) -> RuntimeWake {
        let wake = match buffer.deadline() {
            Some(deadline) => RuntimeWake::never().with_buffer(clock, deadline),
            None => RuntimeWake::never(),
        };
        match self {
            Self::Iceberg(emitter) => emitter.cadence_wake(clock, wake),
            _ => wake,
        }
    }

    fn missing_reason(&self) -> Option<&str> {
        if let Self::Missing { reason } = self {
            Some(reason.as_str())
        } else {
            None
        }
    }

    fn requires_publish_failure_reinitialization(&self) -> bool {
        !matches!(self, Self::Kafka(_) | Self::Mqtt(_) | Self::Nats(_))
    }

    fn pending_acks(&self, buffer: &EmitterBatchBuffer) -> AckSet {
        let sink_acks = if let Self::Iceberg(emitter) = self {
            emitter.pending_acks()
        } else {
            AckSet::empty()
        };
        AckSet::merged([buffer.pending_acks(), sink_acks])
    }

    async fn finish_transport(&self, deadline: Instant) -> EmitterRuntimeResult<()> {
        match self {
            Self::Kafka(emitter) => emitter.flush_local_queue(deadline).await,
            Self::Pulsar(_)
            | Self::RabbitMq(_)
            | Self::Redis(_)
            | Self::Mqtt(_)
            | Self::Nats(_)
            | Self::ZeroMq(_)
            | Self::Syslog(_)
            | Self::Sqs(_)
            | Self::Sentry(_)
            | Self::Otel(_)
            | Self::ClickHouse(_)
            | Self::Postgres(_)
            | Self::MySql(_)
            | Self::MongoDb(_)
            | Self::Iceberg(_)
            | Self::Missing { .. } => Ok(()),
        }
    }

    fn reconnect_after(&self, error: &Report<EmitterRuntimeError>) -> bool {
        if let EmitterRuntimeError::PublishStalled = error.current_context() {
            false
        } else {
            self.requires_publish_failure_reinitialization() && !matches!(self, Self::Iceberg(_))
        }
    }

    fn reconfigure_flush_policy(
        &mut self,
        context: &EmitterSinkContext,
        flush_policy: &FlushPolicy,
    ) -> IcebergEmitterResult<()> {
        if let Self::Iceberg(emitter) = self
            && let Some(policy) = context.parse_flush_policy("iceberg emitter", flush_policy)
        {
            emitter.reconfigure_flush_policy(context, policy)?;
        }
        Ok(())
    }

    async fn flush_due(
        &mut self,
        sink: &EmitSink,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        codec: Option<Arc<CompiledCodec>>,
        buffer: &mut EmitterBatchBuffer,
        retry: bool,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        if let Self::Iceberg(_) = self
            && let EmitSink::Iceberg { .. } = sink
        {
            let accepted = self
                .transfer_retry_buffer_to_iceberg(context, control, buffer, retry)
                .await?;
            let Self::Iceberg(emitter) = self else {
                unreachable!("checked Iceberg emitter must remain Iceberg")
            };
            let mut result = {
                let _confirmation_wait = context
                    .runtime
                    .begin_emitter_confirmation_wait(&context.domain, &context.emitter);
                await_until_emitter_stop_deadline(control.stop_rx, async {
                    if retry {
                        emitter.finish(context).await
                    } else {
                        emitter.flush_due(context).await
                    }
                })
                .await
                .map_err(|()| emitter_stop_deadline_elapsed())?
            };
            loop {
                tokio::task::consume_budget().await;
                Self::finish_iceberg_rejected_records(context, emitter, control.stop_rx).await?;
                match result {
                    Ok(published) => {
                        control.backoff.reset();
                        context
                            .runtime
                            .clear_emitter_transient_error(&context.domain, &context.emitter);
                        return Ok(PublishReport::merge_optional(accepted, published));
                    }
                    Err(error) if error.current_context().is_retryable_publish_failure() => {
                        let acks = emitter.pending_acks();
                        if !Self::wait_for_iceberg_retry(
                            sink.label(),
                            context,
                            control,
                            &acks,
                            &error,
                        )
                        .await?
                        {
                            return Ok(None);
                        }
                        result = {
                            let _confirmation_wait = context
                                .runtime
                                .begin_emitter_confirmation_wait(&context.domain, &context.emitter);
                            await_until_emitter_stop_deadline(
                                control.stop_rx,
                                emitter.finish(context),
                            )
                            .await
                            .map_err(|()| emitter_stop_deadline_elapsed())?
                        };
                    }
                    Err(error) => {
                        let message = iceberg_error_message(&error);
                        context.report_flush_error(sink.label(), &message);
                        return Err(Report::new(EmitterRuntimeError::InvalidSinkConfig)
                            .attach_printable(message));
                    }
                }
            }
        }
        if !buffer.should_flush(context, retry)? {
            return Ok(None);
        }
        self.flush_buffer(sink, context, control, codec, buffer)
            .await
    }

    async fn flush_all(
        &mut self,
        sink: &EmitSink,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        codec: Option<Arc<CompiledCodec>>,
        buffer: &mut EmitterBatchBuffer,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        if let EmitSink::Iceberg { .. } = sink
            && let Self::Iceberg(_) = self
        {
            let accepted = self
                .transfer_retry_buffer_to_iceberg(context, control, buffer, true)
                .await?;
            let Self::Iceberg(emitter) = self else {
                unreachable!("checked Iceberg emitter must remain Iceberg")
            };
            let mut result = {
                let _confirmation_wait = context
                    .runtime
                    .begin_emitter_confirmation_wait(&context.domain, &context.emitter);
                await_until_emitter_stop_deadline(control.stop_rx, emitter.finish(context))
                    .await
                    .map_err(|()| emitter_stop_deadline_elapsed())?
            };
            loop {
                tokio::task::consume_budget().await;
                Self::finish_iceberg_rejected_records(context, emitter, control.stop_rx).await?;
                match result {
                    Ok(published) => {
                        control.backoff.reset();
                        context
                            .runtime
                            .clear_emitter_transient_error(&context.domain, &context.emitter);
                        return Ok(PublishReport::merge_optional(accepted, published));
                    }
                    Err(error) if error.current_context().is_retryable_publish_failure() => {
                        let acks = emitter.pending_acks();
                        if !Self::wait_for_iceberg_retry(
                            sink.label(),
                            context,
                            control,
                            &acks,
                            &error,
                        )
                        .await?
                        {
                            return Err(Report::new(EmitterRuntimeError::ShutdownWhileStalled)
                                .attach_printable(
                                    "emitter drain stopped while Iceberg work remained pending",
                                ));
                        }
                        result = {
                            let _confirmation_wait = context
                                .runtime
                                .begin_emitter_confirmation_wait(&context.domain, &context.emitter);
                            await_until_emitter_stop_deadline(
                                control.stop_rx,
                                emitter.finish(context),
                            )
                            .await
                            .map_err(|()| emitter_stop_deadline_elapsed())?
                        };
                    }
                    Err(error) => {
                        let message = iceberg_error_message(&error);
                        context.report_flush_error(sink.label(), &message);
                        return Err(Report::new(EmitterRuntimeError::InvalidSinkConfig)
                            .attach_printable(message));
                    }
                }
            }
        } else {
            loop {
                tokio::task::consume_budget().await;
                match self
                    .flush_buffer(sink, context, control, codec.clone(), buffer)
                    .await
                {
                    Ok(report) => {
                        control.backoff.reset();
                        context
                            .runtime
                            .clear_emitter_transient_error(&context.domain, &context.emitter);
                        return Ok(report);
                    }
                    Err(error)
                        if error.current_context() == &EmitterRuntimeError::StopDeadlineElapsed =>
                    {
                        return Err(error);
                    }
                    Err(error) if emitter_publish_error_is_retryable(&error) => {
                        let reason = emitter_error_message(&error);
                        let wait = emitter_retry_delay(control.backoff, &error);
                        context.runtime.record_emitter_transient_error_with_backoff(
                            &context.domain,
                            &context.emitter,
                            reason.clone(),
                            wait,
                        );
                        context.report_flush_error(sink.label(), &reason);
                        let waited = await_until_emitter_stop_deadline(
                            control.stop_rx,
                            RuntimeReconnectBackoff::wait_duration_with_ack_alive(
                                wait,
                                control.shutdown_rx,
                                &buffer.pending_acks(),
                            ),
                        )
                        .await
                        .map_err(|()| emitter_stop_deadline_elapsed())?;
                        if !waited {
                            return Err(Report::new(EmitterRuntimeError::ShutdownWhileStalled));
                        }
                    }
                    Err(error) => {
                        context.report_flush_error(sink.label(), &emitter_error_message(&error));
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn publish_batch(
        &mut self,
        sink: &EmitSink,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        codec: Option<Arc<CompiledCodec>>,
        buffer: &mut EmitterBatchBuffer,
        batch: EmitterPublishBatch,
    ) -> EmitterPublishResult {
        if let EmitSink::Iceberg { .. } = sink
            && let Self::Iceberg(_) = self
        {
            self.check_fault_injection(context, control)
                .map_err(EmitterPublishFailure::caller)?;
            let Self::Iceberg(emitter) = self else {
                unreachable!("checked Iceberg emitter must remain Iceberg")
            };
            let result = {
                let _confirmation_wait = context
                    .runtime
                    .begin_emitter_confirmation_wait(&context.domain, &context.emitter);
                await_until_emitter_stop_deadline(
                    control.stop_rx,
                    emitter.publish_batch(context, batch.batch, batch.execution_now),
                )
                .await
                .map_err(|()| EmitterPublishFailure::sink(emitter_stop_deadline_elapsed()))?
            };
            Self::finish_iceberg_rejected_records(context, emitter, control.stop_rx)
                .await
                .map_err(EmitterPublishFailure::sink)?;
            return match result {
                Ok(report) => Ok(report),
                Err(error) if error.current_context().is_retryable_publish_failure() => {
                    Err(EmitterPublishFailure::sink(
                        Report::new(EmitterRuntimeError::PublishBatch)
                            .attach_printable(iceberg_error_message(&error)),
                    ))
                }
                Err(error) => {
                    let message = iceberg_error_message(&error);
                    context.report_flush_error("iceberg", &message);
                    Err(EmitterPublishFailure::sink(
                        Report::new(EmitterRuntimeError::InvalidSinkConfig)
                            .attach_printable(message),
                    ))
                }
            };
        }

        if buffer
            .push(context, batch)
            .map_err(EmitterPublishFailure::caller)?
        {
            self.flush_buffer(sink, context, control, codec, buffer)
                .await
                .map_err(EmitterPublishFailure::buffer)
        } else {
            Ok(None)
        }
    }

    async fn transfer_retry_buffer_to_iceberg(
        &mut self,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &mut EmitterBatchBuffer,
        force: bool,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        if !buffer.should_flush(context, force)? {
            return Ok(None);
        }
        self.check_fault_injection(context, control)?;
        let Self::Iceberg(emitter) = self else {
            return Ok(None);
        };
        let pending = buffer.drain_pending();
        let mut pending = pending.into_iter();
        let mut report = None;
        while let Some(batch) = pending.next() {
            tokio::task::consume_budget().await;
            match emitter
                .publish_batch(context, batch.batch, batch.execution_now)
                .await
            {
                Ok(published) => {
                    report = PublishReport::merge_optional(report, published);
                }
                Err(error) => {
                    for batch in pending {
                        tokio::task::consume_budget().await;
                        buffer.push(context, batch)?;
                    }
                    return Err(Report::new(EmitterRuntimeError::PublishBatch)
                        .attach_printable(iceberg_error_message(&error)));
                }
            }
        }
        Ok(report)
    }

    async fn flush_buffer(
        &mut self,
        sink: &EmitSink,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        codec: Option<Arc<CompiledCodec>>,
        buffer: &mut EmitterBatchBuffer,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        if buffer.is_empty() {
            return Ok(None);
        }
        self.check_fault_injection(context, control)?;
        let report = buffer.report();
        let pending_acks = buffer.pending_acks();
        {
            let _confirmation_wait = context
                .runtime
                .begin_emitter_confirmation_wait(&context.domain, &context.emitter);
            let publish = Box::pin(self.publish_buffered_batches(
                sink,
                context,
                codec,
                buffer.pending.as_mut_slice(),
            ));
            await_until_emitter_stop_deadline(
                control.stop_rx,
                await_emitter_confirmation(&pending_acks, publish),
            )
            .await
            .map_err(|()| emitter_stop_deadline_elapsed())??;
        }
        buffer.clear();
        Ok(report)
    }

    async fn publish_buffered_batches(
        &mut self,
        sink: &EmitSink,
        context: &EmitterSinkContext,
        codec: Option<Arc<CompiledCodec>>,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()> {
        if let EmitSink::Kafka { .. }
        | EmitSink::Pulsar { .. }
        | EmitSink::RabbitMq { .. }
        | EmitSink::Redis { .. }
        | EmitSink::Mqtt { .. }
        | EmitSink::Nats { .. }
        | EmitSink::ZeroMq { .. }
        | EmitSink::Syslog { .. }
        | EmitSink::Sqs { .. }
        | EmitSink::Sentry { .. } = sink
            && codec.is_none()
        {
            return Err(
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "{} emitter requires an encoding codec",
                    sink.label()
                )),
            );
        }
        if let (Some(codec), EmitSink::Kafka { topic, .. }, Self::Kafka(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish(topic, records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }
        if let (Some(codec), EmitSink::Pulsar { .. }, Self::Pulsar(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish(records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }
        if let (Some(codec), EmitSink::RabbitMq { queue, .. }, Self::RabbitMq(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish_records(queue, records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }
        if let (Some(codec), EmitSink::Mqtt { topic, .. }, Self::Mqtt(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish_records(topic, records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }
        if let (Some(codec), EmitSink::Nats { .. }, Self::Nats(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish_records(records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }
        if let (Some(codec), EmitSink::Redis { channel, .. }, Self::Redis(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish_records(channel, records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }
        if let (Some(codec), EmitSink::ZeroMq { .. }, Self::ZeroMq(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish_records(records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }
        if let (Some(codec), EmitSink::Syslog { .. }, Self::Syslog(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish_records(records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }
        if let (Some(codec), EmitSink::Sqs { .. }, Self::Sqs(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish(records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }
        if let (Some(codec), EmitSink::Sentry { .. }, Self::Sentry(emitter)) =
            (codec.clone(), sink, &mut *self)
        {
            let records = encode_broker_records(codec, context, batches).await?;
            let outcome = emitter.publish(records).await;
            return finish_per_record_publish(context, batches, outcome).await;
        }

        match (&mut *self, sink) {
            (
                Self::Otel(emitter),
                EmitSink::Otel {
                    signal,
                    values,
                    attributes,
                    ..
                },
            ) => {
                for batch_index in 0..batches.len() {
                    tokio::task::consume_budget().await;
                    let outcome = {
                        let batch = &batches[batch_index];
                        let pending_rows = batch.pending_record_rows();
                        emitter
                            .publish_pending_rows(
                                EmitterBatchExecutionContext {
                                    batch_index,
                                    batch: &batch.batch,
                                    execution_now: batch.execution_now,
                                },
                                signal,
                                values,
                                attributes,
                                &pending_rows,
                            )
                            .await
                    };
                    finish_per_record_publish(context, batches, outcome).await?;
                }
                return Ok(());
            }
            (
                Self::ClickHouse(emitter),
                EmitSink::ClickHouse {
                    table,
                    values,
                    max_batch,
                    ..
                },
            ) => {
                for batch_index in 0..batches.len() {
                    tokio::task::consume_budget().await;
                    let outcome = {
                        let batch = &batches[batch_index];
                        let pending_chunks = batch.pending_record_chunks(*max_batch);
                        emitter
                            .publish_pending_chunks(
                                batch_index,
                                table,
                                values,
                                &batch.batch,
                                &pending_chunks,
                                batch.execution_now,
                            )
                            .await
                    };
                    finish_per_record_publish(context, batches, outcome).await?;
                }
                return Ok(());
            }
            (
                Self::Postgres(emitter),
                EmitSink::Postgres {
                    table,
                    values,
                    conflict_action,
                    max_batch,
                    ..
                },
            ) => {
                for batch_index in 0..batches.len() {
                    tokio::task::consume_budget().await;
                    let outcome = {
                        let batch = &batches[batch_index];
                        let pending_chunks = batch.pending_record_chunks(*max_batch);
                        emitter
                            .publish_pending_chunks(
                                EmitterBatchExecutionContext {
                                    batch_index,
                                    batch: &batch.batch,
                                    execution_now: batch.execution_now,
                                },
                                table,
                                values,
                                conflict_action,
                                &pending_chunks,
                            )
                            .await
                    };
                    finish_per_record_publish(context, batches, outcome).await?;
                }
                return Ok(());
            }
            (
                Self::MySql(emitter),
                EmitSink::MySql {
                    table,
                    values,
                    conflict_action,
                    max_batch,
                    ..
                },
            ) => {
                for batch_index in 0..batches.len() {
                    tokio::task::consume_budget().await;
                    let outcome = {
                        let batch = &batches[batch_index];
                        let pending_chunks = batch.pending_record_chunks(*max_batch);
                        emitter
                            .publish_pending_chunks(
                                EmitterBatchExecutionContext {
                                    batch_index,
                                    batch: &batch.batch,
                                    execution_now: batch.execution_now,
                                },
                                table,
                                values,
                                conflict_action,
                                &pending_chunks,
                            )
                            .await
                    };
                    finish_per_record_publish(context, batches, outcome).await?;
                }
                return Ok(());
            }
            (
                Self::MongoDb(emitter),
                EmitSink::MongoDb {
                    collection,
                    values,
                    conflict_action,
                    max_batch,
                    ..
                },
            ) => {
                for batch_index in 0..batches.len() {
                    tokio::task::consume_budget().await;
                    let outcome = {
                        let batch = &batches[batch_index];
                        let pending_chunks = batch.pending_record_chunks(*max_batch);
                        emitter
                            .publish_pending_chunks(
                                EmitterBatchExecutionContext {
                                    batch_index,
                                    batch: &batch.batch,
                                    execution_now: batch.execution_now,
                                },
                                collection,
                                values,
                                conflict_action,
                                &pending_chunks,
                            )
                            .await
                    };
                    finish_per_record_publish(context, batches, outcome).await?;
                }
                return Ok(());
            }
            _ => {}
        }

        Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
            .attach_printable("emitter has no initialized sink client for its configured sink"))
    }
    fn check_fault_injection(
        &self,
        context: &EmitterSinkContext,
        control: &EmitterPublishControl<'_>,
    ) -> EmitterRuntimeResult<()> {
        if control
            .fault_injection
            .emitter_should_fail(&context.emitter)
        {
            let reason = format!(
                "fault injector failed emitter '{}'",
                context.emitter.as_str()
            );
            context.runtime.events().report_error(format!(
                "{} in domain '{}'",
                reason,
                context.domain.as_str()
            ));
            warn!(
                domain = context.domain.as_str(),
                emitter = context.emitter.as_str(),
                "fault injector failed emitter publish"
            );
            return Err(Report::new(EmitterRuntimeError::FaultInjected).attach_printable(reason));
        }
        if control
            .fault_injection
            .emitter_should_stall(&context.emitter)
        {
            return Err(Report::new(EmitterRuntimeError::PublishStalled)
                .attach_printable("fault injector stalled emitter publish"));
        }
        Ok(())
    }

    async fn wait_for_iceberg_retry(
        sink: &str,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        acks: &AckSet,
        error: &Report<IcebergEmitterError>,
    ) -> EmitterRuntimeResult<bool> {
        let reason = iceberg_error_message(error);
        let wait = control.backoff.next_delay();
        if error.current_context() == &IcebergEmitterError::Commit {
            context.runtime.record_iceberg_commit_failure_with_backoff(
                &context.domain,
                &context.emitter,
                reason.clone(),
                wait,
            );
        } else {
            context.runtime.record_emitter_transient_error_with_backoff(
                &context.domain,
                &context.emitter,
                reason.clone(),
                wait,
            );
        }
        context.report_flush_error(sink, &reason);
        await_until_emitter_stop_deadline(
            control.stop_rx,
            control
                .backoff
                .wait_with_ack_alive(control.shutdown_rx, acks),
        )
        .await
        .map_err(|()| emitter_stop_deadline_elapsed())
    }

    async fn finish_iceberg_rejected_records(
        context: &EmitterSinkContext,
        emitter: &mut IcebergEmitter,
        stop_rx: &mut watch::Receiver<Option<Instant>>,
    ) -> EmitterRuntimeResult<()> {
        while let Some(rejected) = emitter.next_rejected_record_message() {
            tokio::task::consume_budget().await;
            let (message, error) = match rejected {
                Ok(rejected) => rejected,
                Err((reason, acks)) => {
                    context.runtime.handle_general_error_for_acks(
                        &context.domain,
                        ModelKind::Emitter,
                        &context.emitter,
                        &context.error_policies,
                        std::iter::once(&acks),
                        format!(
                            "Iceberg emitter '{}' failed to materialize rejected VALUES row: \
                             {reason}",
                            context.emitter.as_str()
                        ),
                    );
                    emitter.finish_rejected_record();
                    continue;
                }
            };
            await_until_emitter_stop_deadline(
                stop_rx,
                context
                    .runtime
                    .handle_structured_message_error(MessageErrorHandling {
                        domain: &context.domain,
                        node_kind: ModelKind::Emitter,
                        node: &ModelName::from(&context.emitter),
                        source_route: None,
                        policy: &context.error_policies.message,
                        message,
                        execution_now: error.occurred_at,
                        error,
                        partial_output: None,
                        materialized_state: HashMap::default(),
                        ingest_metadata: None,
                    }),
            )
            .await
            .map_err(|()| emitter_stop_deadline_elapsed())?;
            emitter.finish_rejected_record();
        }
        Ok(())
    }
}

fn iceberg_error_message(error: &Report<IcebergEmitterError>) -> String {
    format!("{error:?}")
}

fn emitter_error_message(error: &Report<EmitterRuntimeError>) -> String {
    error
        .frames()
        .find_map(|frame| match frame.kind() {
            FrameKind::Attachment(AttachmentKind::Printable(attachment)) => {
                Some(attachment.to_string())
            }
            FrameKind::Context(_) | FrameKind::Attachment(_) => None,
        })
        .unwrap_or_else(|| error.current_context().to_string())
}

fn emitter_unavailable_reason(
    sink: &SinkEmitter,
    fault_injection: &ConfiguredFaultInjection,
    emitter: &EmitterName,
) -> Option<String> {
    if let Some(reason) = sink.missing_reason() {
        return Some(reason.to_owned());
    }
    if fault_injection.emitter_should_stall(emitter) {
        Some("fault injector stalled emitter publish".to_string())
    } else {
        None
    }
}

async fn wait_for_emitter_work_cancel(work_cancel_rx: &mut watch::Receiver<bool>) {
    loop {
        tokio::task::consume_budget().await;
        if *work_cancel_rx.borrow() {
            return;
        }
        if work_cancel_rx.changed().await.is_err() {
            return;
        }
    }
}

enum SinkEmitterResult<T> {
    Ready(T),
    Missing { reason: String },
}

async fn encode_pending_broker_payloads(
    codec: Arc<CompiledCodec>,
    context: &EmitterSinkContext,
    batch: &EmitterPublishBatch,
    pending_rows: Vec<usize>,
) -> EmitterRuntimeResult<Vec<(usize, Result<Vec<u8>, CodecError>)>> {
    if codec.requires_blocking_encode() {
        let arrow_batch = batch.batch.batch.clone();
        let codec_name = codec.name.as_str().to_string();
        return tokio::task::spawn_blocking(move || {
            let encoder = codec.batch_encoder(&arrow_batch)?;
            Ok::<_, CodecError>(
                pending_rows
                    .into_iter()
                    .map(|row_index| {
                        let mut payload = Vec::new();
                        let result = encoder
                            .encode_row_into(row_index, &mut payload)
                            .map(|()| payload);
                        (row_index, result)
                    })
                    .collect(),
            )
        })
        .await
        .map_err(|error| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "emitter '{}' blocking codec task for '{}' failed: {error}",
                context.emitter.as_str(),
                codec_name
            ))
        })?
        .map_err(|error| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "emitter '{}' failed to initialize columnar encoding: {error}",
                context.emitter.as_str()
            ))
        });
    }

    let encoder = codec.batch_encoder(&batch.batch.batch).map_err(|error| {
        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
            "emitter '{}' failed to initialize columnar encoding: {error}",
            context.emitter.as_str()
        ))
    })?;
    Ok(pending_rows
        .into_iter()
        .map(|row_index| {
            let mut payload = Vec::new();
            let result = encoder
                .encode_row_into(row_index, &mut payload)
                .map(|()| payload);
            (row_index, result)
        })
        .collect())
}

async fn encode_broker_records(
    codec: Arc<CompiledCodec>,
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
) -> EmitterRuntimeResult<Vec<EncodedBrokerRecord>> {
    let mut encoded = Vec::new();
    let mut rejected = Vec::new();
    for (batch_index, batch) in batches.iter().enumerate() {
        tokio::task::consume_budget().await;
        let row_count = batch.batch.batch.batch().num_rows();
        let pending_rows = (0..row_count)
            .filter(|row_index| !batch.is_delivered(*row_index))
            .collect::<Vec<_>>();
        let batch_acks = batch.merged_acks();
        let payloads = await_emitter_confirmation(
            &batch_acks,
            encode_pending_broker_payloads(codec.clone(), context, batch, pending_rows),
        )
        .await?;

        for (row_index, payload) in payloads {
            tokio::task::consume_budget().await;
            let key = batch
                .batch
                .keys
                .get(row_index)
                .ok_or_else(|| {
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "emitter batch row {row_index} has no branch key entry"
                    ))
                })?
                .as_ref()
                .map(|key| key.as_str().to_string());
            let headers = batch.headers_for_row(row_index).cloned().ok_or_else(|| {
                Report::new(EmitterRuntimeError::EncodeBatch)
                    .attach_printable(format!("emitter batch row {row_index} has no header entry"))
            })?;
            let sqs_message_group = batch
                .sqs_message_groups
                .get(row_index)
                .cloned()
                .ok_or_else(|| {
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "emitter batch row {row_index} has no SQS FIFO group entry"
                    ))
                })?;
            let acks = batch.batch.acks.get(row_index).cloned().ok_or_else(|| {
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "emitter batch row {row_index} has no acknowledgment entry"
                ))
            })?;
            let payload = match payload {
                Ok(payload) => payload,
                Err(error) => {
                    rejected.push(RejectedEmitterRecord {
                        position: BrokerRecordPosition {
                            batch_index,
                            row_index,
                        },
                        reason: format!(
                            "emitter '{}' failed to encode record: {error}",
                            context.emitter.as_str()
                        ),
                        structured_error: None,
                    });
                    continue;
                }
            };
            encoded.push(EncodedBrokerRecord {
                batch_index,
                row_index,
                key,
                payload,
                headers,
                sqs_message_group,
                acks,
                execution_now: batch.execution_now,
            });
        }
    }
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Encode).await?;
    Ok(encoded)
}
async fn finish_per_record_publish(
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
    outcome: PerRecordPublishOutcome,
) -> EmitterRuntimeResult<()> {
    let PerRecordPublishOutcome {
        delivered,
        rejected,
        infrastructure_error,
    } = outcome;
    for BrokerRecordPosition {
        batch_index,
        row_index,
    } in delivered
    {
        let batch = batches.get_mut(batch_index).ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "broker confirmation references missing emitter batch {batch_index}"
            ))
        })?;
        batch.mark_delivered(row_index).map_err(|reason| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(reason)
        })?;
    }
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Publish).await?;
    match infrastructure_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn finish_rejected_records(
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
    rejected: Vec<RejectedEmitterRecord>,
    operation: MessageErrorOperation,
) -> EmitterRuntimeResult<()> {
    for rejected in rejected {
        tokio::task::consume_budget().await;
        let BrokerRecordPosition {
            batch_index,
            row_index,
        } = rejected.position;
        let batch = batches.get_mut(batch_index).ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "record rejection references missing emitter batch {batch_index}"
            ))
        })?;
        let execution_now = batch.execution_now;
        let error = if let Some(error) = rejected.structured_error {
            error
        } else {
            structured_message_error(
                execution_now,
                MessageErrorCode::External,
                rejected.reason,
                operation,
                None,
                std::iter::empty(),
            )
        };
        let record = batch.batch.runtime_row(row_index).map_err(|reason| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(reason)
        })?;
        let key = batch.batch.keys.get(row_index).cloned().ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "record rejection row {row_index} has no branch key"
            ))
        })?;
        let acks = batch.batch.acks.get(row_index).cloned().ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "record rejection row {row_index} has no acknowledgment set"
            ))
        })?;
        batch
            .mark_rejected_after_delivery(
                row_index,
                context
                    .runtime
                    .handle_structured_message_error(MessageErrorHandling {
                        domain: &context.domain,
                        node_kind: ModelKind::Emitter,
                        node: &ModelName::from(&context.emitter),
                        source_route: None,
                        policy: &context.error_policies.message,
                        message: RelayMessage { key, record, acks },
                        error,
                        partial_output: None,
                        materialized_state: HashMap::default(),
                        ingest_metadata: None,
                        execution_now,
                    }),
            )
            .await?;
    }
    Ok(())
}

impl<T> SinkEmitterResult<T> {
    fn map(self, f: impl FnOnce(T) -> SinkEmitter) -> SinkEmitter {
        match self {
            Self::Ready(value) => f(value),
            Self::Missing { reason } => SinkEmitter::Missing { reason },
        }
    }
}

fn emitter_publish_error_is_retryable(error: &Report<EmitterRuntimeError>) -> bool {
    error.current_context().is_retryable_publish_failure()
}

fn emitter_message_error_operation(
    error: &Report<EmitterRuntimeError>,
    codec_route: bool,
) -> MessageErrorOperation {
    match (error.current_context(), codec_route) {
        (EmitterRuntimeError::EncodeBatch, true) => MessageErrorOperation::Encode,
        (EmitterRuntimeError::EncodeBatch, false) => MessageErrorOperation::Values,
        _ => MessageErrorOperation::Publish,
    }
}

trait EmitSinkLabel {
    fn label(&self) -> &'static str;
}

impl EmitSinkLabel for EmitSink {
    fn label(&self) -> &'static str {
        match self {
            EmitSink::Kafka { .. } => "kafka",
            EmitSink::Pulsar { .. } => "pulsar",
            EmitSink::RabbitMq { .. } => "rabbitmq",
            EmitSink::Redis { .. } => "redis",
            EmitSink::Mqtt { .. } => "mqtt",
            EmitSink::Nats { .. } => "nats",
            EmitSink::ZeroMq { .. } => "zeromq",
            EmitSink::Syslog { .. } => "syslog",
            EmitSink::Sqs { .. } => "sqs",
            EmitSink::Sentry { .. } => "sentry",
            EmitSink::Otel { .. } => "otel",
            EmitSink::ClickHouse { .. } => "clickhouse",
            EmitSink::Postgres { .. } => "postgres",
            EmitSink::MySql { .. } => "mysql",
            EmitSink::MongoDb { .. } => "mongodb",
            EmitSink::Iceberg { .. } => "iceberg",
        }
    }
}

impl EmitterTask {
    pub(in crate::runtime) fn spawn(
        runtime: &Runtime,
        build: EmitterTaskBuildDeps<'_>,
        emitter: CreateEmitter,
        inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
    ) -> Result<ScheduledEmitterTask, RuntimeError> {
        let EmitterTaskBuildDeps {
            domain,
            shutdown_tx,
            codecs,
            clients,
            deps,
        } = build;
        let EmitterTaskDeps {
            input_schema,
            input_branching,
            materialized_relay_specs: materialized_stream_specs,
            lookups,
        } = deps;
        let codec = if let Some(codec_name) = &emitter.encode_using_codec {
            Some(codecs.get(codec_name).cloned().ok_or_else(|| {
                RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!("missing emitter codec '{}'", codec_name.as_str()),
                }
            })?)
        } else {
            None
        };
        let output_compiled_schema = match codec.as_ref() {
            Some(codec) => codec.schema(),
            None => input_schema.clone(),
        };
        let udfs = runtime.udf_executor(domain);
        let filter_map = compile_emitter_filter_map_program(
            domain,
            &emitter,
            input_schema.arrow_schema(),
            input_schema.vm_sensitivity(),
            output_compiled_schema.arrow_schema(),
            output_compiled_schema.vm_sensitivity(),
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized_stream_specs,
                available_lookups: &lookups,
                current_branching: &input_branching,
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: udfs.as_ref(),
            },
        )?;
        let sqs_fifo_group = match emitter.sink.as_ref() {
            EmitSink::Sqs {
                fifo_group: Some(nervix_models::SqsFifoGroup::FromBranch),
                ..
            } => Some(CompiledSqsFifoGroup::FromBranch),
            EmitSink::Sqs {
                fifo_group: Some(nervix_models::SqsFifoGroup::Expression(_)),
                ..
            } => compile_sqs_fifo_group_program(
                domain,
                &emitter,
                input_schema.arrow_schema(),
                input_schema.vm_sensitivity(),
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &lookups,
                    current_branching: &input_branching,
                    current_branch_schema: None,
                    current_branch_sensitivity: None,
                    udfs: udfs.as_ref(),
                },
            )?
            .map(CompiledSqsFifoGroup::Expression),
            _ => None,
        };
        let mut source_filters = HashMap::default();
        for source_filter in emitter.from.where_clauses() {
            let program = compile_scoped_filter_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: &ModelName::from(&emitter.name),
                },
                Some(&source_filter.where_clause),
                RuntimeVmSchema {
                    schema: input_schema.arrow_schema(),
                    sensitivity: input_schema.vm_sensitivity(),
                },
                MessageErrorOperation::SourceWhere,
                RuntimeVmCompileContext {
                    available_materialized_streams: &materialized_stream_specs,
                    available_lookups: &lookups,
                    current_branching: &input_branching,
                    current_branch_schema: None,
                    current_branch_sensitivity: None,
                    udfs: udfs.as_ref(),
                },
                RuntimeFilterScope::Source {
                    namespace: "input",
                    allow_header_reads: false,
                    allow_metadata: false,
                },
            )?
            .verified(
                "a FROM WHERE clause is present here, and a present clause always compiles to a \
                 program",
            );
            source_filters.insert(source_filter.relay.clone(), program);
        }
        let client = clients.get(emitter.sink.client()).cloned();
        let catalog_client = emitter
            .sink
            .iceberg_catalog_client()
            .and_then(|client| clients.get(client))
            .cloned();
        let task_domain = domain.clone();
        let task_emitter = emitter.name.clone();
        let task_metric_relay = if emitter.from.relays().len() == 1 {
            emitter.from.first().cloned()
        } else {
            None
        };
        let task_sink = emitter.sink.clone();
        let task_publishing = EmitterPublishingSettings::parse(
            domain,
            &emitter.name,
            &emitter.sink,
            &emitter.publishing_mode,
        )?;
        let task_flush_policy = emitter.flush_policy.clone();
        let task_error_policies = emitter.error_policies.clone();
        let task_materialized_state = emitter.materialized_state.clone();
        let fault_injection = runtime.inner.fault_injection.clone();
        let runtime = runtime.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let interaction_shutdown_rx = shutdown_tx.subscribe();
        let mut domain_work_cancel_rx = shutdown_tx.subscribe();
        let (work_cancel, mut work_cancel_rx) = watch::channel(false);
        let task_work_cancel = work_cancel.clone();
        let quiesce_counters =
            runtime.node_quiesce_counters(domain, NodeRef::new(ModelKind::Emitter, &emitter.name));
        let force_flush = runtime.force_flush_participant(domain, quiesce_counters.clone());
        let emitter_buffer_count = runtime
            .inner
            .emitter_buffers
            .entry(DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Emitter,
                emitter.name.clone(),
            ))
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        let buffered_messages =
            Arc::new(EmitterBufferedMessages::new(emitter_buffer_count.clone()));
        let resolved_client =
            resolve_emitter_client(&runtime, domain, &emitter.sink, client.as_deref())?;
        let resolved_catalog_client = resolve_emitter_catalog_client(
            &runtime,
            domain,
            &emitter.sink,
            catalog_client.as_deref(),
        )?;
        let input_collect_policy = Runtime::parse_runtime_node_input_collect_policy(
            domain,
            "emitter",
            &emitter.name,
            emitter.from.collect_policy.as_ref(),
        )?;
        let (commands, command_rx) = mpsc::channel(4);
        let (stop_signal, mut stop_rx) = watch::channel(None);
        let task_stop_signal = stop_signal.clone();

        let task = tokio::spawn(async move {
            // The emitter's explicit FLUSH EACH and COMMIT EACH cadences are domain logical
            // durations, so every emitter binds the domain clock whether or not it collects input.
            let domain_clock = match runtime.bind_domain_clock(&task_domain) {
                Ok(clock) => clock,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "emitter '{}' in domain '{}' could not bind its domain clock: {error}",
                        task_emitter.as_str(),
                        task_domain.as_str(),
                    ));
                    return;
                }
            };
            let work_cancel_forwarder = AbortOnDropHandle::new(tokio::spawn(async move {
                if *domain_work_cancel_rx.borrow()
                    || domain_work_cancel_rx.changed().await.is_err()
                    || *domain_work_cancel_rx.borrow()
                {
                    task_work_cancel.send_replace(true);
                }
            }));
            let _client_mounts = resolved_client
                .as_ref()
                .and_then(|config| config.mounts.clone());
            let interaction_inputs = inputs
                .into_iter()
                .map(|(relay, receiver)| {
                    let input = RelayInteractionInput::new(relay, receiver, input_collect_policy);
                    if input_collect_policy.is_some() {
                        input.with_domain_clock(domain_clock.clone())
                    } else {
                        input
                    }
                })
                .collect();
            let mut interaction = RelayInteraction::with_commands(
                interaction_inputs,
                interaction_shutdown_rx,
                Some(force_flush),
                Some(quiesce_counters),
                command_rx,
            )
            .verified(
                "the registry validated this emitter's inputs, and a non-empty input list builds \
                 an interaction",
            );
            let context = EmitterSinkContext {
                runtime: runtime.clone(),
                domain: task_domain.clone(),
                emitter: task_emitter.clone(),
                error_policies: task_error_policies.clone(),
                udfs,
                clock: domain_clock,
            };
            let mut publish_backoff =
                RuntimeReconnectBackoff::from_policy(task_publishing.retry_policy);
            let mut emitter_buffer =
                EmitterBatchBuffer::new(&context, &task_flush_policy, buffered_messages.clone());
            let sink_runtime = SinkEmitterRuntime {
                input_schema: input_schema.clone(),
                buffered_messages,
            };
            let mut sink = SinkEmitter::new_until_cancelled(
                SinkEmitterInit {
                    sink: &task_sink,
                    flush_policy: &task_flush_policy,
                    publishing: task_publishing,
                    client: client.as_deref(),
                    resolved: resolved_client.as_ref(),
                    catalog_client: catalog_client.as_deref(),
                    catalog_resolved: resolved_catalog_client.as_ref(),
                    context: &context,
                    runtime: sink_runtime.clone(),
                },
                &mut work_cancel_rx,
            )
            .await;
            let mut reconnect_on_wake = sink.missing_reason().is_some();
            let mut retry_schedule = EmitterRetrySchedule::default();
            if let Some(reason) = sink.missing_reason() {
                let wait = publish_backoff.take_next_delay();
                retry_schedule.defer(
                    &context,
                    EmitterRetryDeferral {
                        wait,
                        acks: AckSet::empty(),
                        waiting_for_stall_clear: false,
                        reason: Some(reason),
                    },
                );
            } else {
                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
            }
            let batch_context = EmitterBatchContext {
                runtime: &runtime,
                domain: &task_domain,
                emitter: &task_emitter,
                metric_relay: task_metric_relay.as_ref(),
                error_policies: &task_error_policies,
                source_filters: &source_filters,
                filter_map: filter_map.as_ref(),
                sqs_fifo_group: sqs_fifo_group.as_ref(),
                materialized_state: &task_materialized_state,
            };
            loop {
                tokio::task::consume_budget().await;
                let wake = retry_schedule
                    .wake(sink.cadence_wake(&context.clock, &emitter_buffer));
                let receive_input = !retry_schedule.is_active()
                    || emitter_buffer_count.load(Ordering::Acquire) == 0;
                let work = match interaction.next_with_input(wake, receive_input).await {
                    Ok(work) => work,
                    Err(error) => {
                        let reason = error.to_string();
                        context.report_flush_error(task_sink.label(), &reason);
                        runtime.handle_internal_processor_error_for_acks(
                            &task_domain,
                            ModelKind::Emitter,
                            &task_emitter,
                            &task_error_policies,
                            error.acks(),
                            reason,
                        );
                        continue;
                    }
                };
                let (input_event, mut work) = work.into_parts();
                match input_event {
                    RelayInteractionEvent::Command(EmitterTaskCommand::Reconfigure {
                        config,
                        response,
                    }) => {
                        // The new cadence replaces the old one for the batches already buffered,
                        // so a reconfiguration that cannot read the domain clock leaves them
                        // without a deadline and is recorded as the emitter's transient error.
                        if let Err(error) =
                            emitter_buffer.reconfigure(&context, &config.flush_policy)
                        {
                            let reason = emitter_error_message(&error);
                            runtime.record_emitter_transient_error(
                                &task_domain,
                                &task_emitter,
                                reason.clone(),
                            );
                            context.report_flush_error(task_sink.label(), &reason);
                        }
                        if let Err(error) =
                            sink.reconfigure_flush_policy(&context, &config.flush_policy)
                        {
                            let reason = iceberg_error_message(&error);
                            runtime.record_emitter_transient_error(
                                &task_domain,
                                &task_emitter,
                                reason.clone(),
                            );
                            context.report_flush_error(task_sink.label(), &reason);
                        }
                        response
                            .send(())
                            .means_peer_left("emitter reconfiguration requester");
                    }
                    RelayInteractionEvent::Command(EmitterTaskCommand::Stop {
                        deadline,
                        response,
                    }) => {
                        if response.is_closed() {
                            clear_emitter_stop_signal(&task_stop_signal, deadline);
                            continue;
                        }
                        if emitter_buffer_count.load(Ordering::Acquire) > 0
                            && let Some(reason) =
                                emitter_unavailable_reason(&sink, &fault_injection, &task_emitter)
                        {
                            runtime.record_emitter_transient_error(
                                &task_domain,
                                &task_emitter,
                                reason.clone(),
                            );
                            context.report_flush_error(task_sink.label(), &reason);
                            clear_emitter_stop_signal(&task_stop_signal, deadline);
                            response
                                .send(Err(format!("emitter final flush failed: {reason}")))
                                .means_peer_left("emitter stop requester");
                            continue;
                        }
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        let drained = tokio::time::timeout_at(deadline, async {
                            let report = sink
                                .flush_all(
                                    &task_sink,
                                    &context,
                                    &mut control,
                                    codec.clone(),
                                    &mut emitter_buffer,
                                )
                                .await?;
                            sink.finish_transport(deadline).await?;
                            Ok::<_, Report<EmitterRuntimeError>>(report)
                        })
                        .await;
                        let result = match drained {
                            Ok(Ok(report)) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                if let Some(report) = report.as_ref() {
                                    batch_context.observe_sent(report);
                                }
                                Ok(())
                            }
                            Ok(Err(error)) => {
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(task_sink.label(), &reason);
                                Err(format!("emitter final flush failed: {reason}"))
                            }
                            Err(_) => {
                                let reason = format!(
                                    "emitter '{}' did not drain before its configured deadline",
                                    task_emitter.as_str()
                                );
                                context.report_flush_error(task_sink.label(), &reason);
                                Err(reason)
                            }
                        };
                        let should_stop = result.is_ok();
                        if !should_stop {
                            clear_emitter_stop_signal(&task_stop_signal, deadline);
                        }
                        if response.send(result).is_ok() && should_stop {
                            break;
                        }
                        if should_stop {
                            clear_emitter_stop_signal(&task_stop_signal, deadline);
                        }
                    }
                    RelayInteractionEvent::ForceFlush(completion) => {
                        if emitter_buffer_count.load(Ordering::Acquire) > 0
                            && let Some(reason) =
                                emitter_unavailable_reason(&sink, &fault_injection, &task_emitter)
                        {
                            retry_schedule.include_acks(sink.pending_acks(&emitter_buffer));
                            if retry_schedule.is_active() {
                                runtime.record_emitter_transient_error_with_backoff(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                    publish_backoff.next_delay(),
                                );
                            } else {
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: publish_backoff.take_next_delay(),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: fault_injection
                                            .emitter_should_stall(&task_emitter),
                                        reason: Some(&reason),
                                    },
                                );
                            }
                            context.report_flush_error(task_sink.label(), &reason);
                            completion.complete();
                            continue;
                        }
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        match sink
                            .flush_all(
                                &task_sink,
                                &context,
                                &mut control,
                                codec.clone(),
                                &mut emitter_buffer,
                            )
                            .await
                        {
                            Ok(Some(report)) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                batch_context.observe_sent(&report);
                            }
                            Ok(None) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                            }
                            Err(error) if emitter_publish_error_is_retryable(&error) => {
                                let reason = emitter_error_message(&error);
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: emitter_retry_delay(&mut publish_backoff, &error),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: *error.current_context()
                                            == EmitterRuntimeError::PublishStalled,
                                        reason: Some(&reason),
                                    },
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                context.report_flush_error(task_sink.label(), &reason);
                            }
                            Err(error) => {
                                retry_schedule.clear();
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(task_sink.label(), &reason);
                                let pending = emitter_buffer.drain_pending();
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batches(pending, reason, operation)
                                    .await;
                            }
                        }
                        completion.complete();
                    }
                    RelayInteractionEvent::Stopped(reason) => {
                        debug!(
                            domain = task_domain.as_str(),
                            emitter = task_emitter.as_str(),
                            ?reason,
                            "emitter relay interaction stopped"
                        );
                        if emitter_buffer_count.load(Ordering::Acquire) > 0
                            && let Some(reason) =
                                emitter_unavailable_reason(&sink, &fault_injection, &task_emitter)
                        {
                            runtime.record_emitter_transient_error(
                                &task_domain,
                                &task_emitter,
                                reason.clone(),
                            );
                            context.report_flush_error(task_sink.label(), &reason);
                            let pending = emitter_buffer.drain_pending();
                            batch_context
                                .handle_publish_error_batches(
                                    pending,
                                    reason,
                                    MessageErrorOperation::Publish,
                                )
                                .await;
                            break;
                        }
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        match sink
                            .flush_all(
                                &task_sink,
                                &context,
                                &mut control,
                                codec.clone(),
                                &mut emitter_buffer,
                            )
                            .await
                        {
                            Ok(Some(report)) => batch_context.observe_sent(&report),
                            Ok(None) => {}
                            Err(error) => {
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(task_sink.label(), &reason);
                                let pending = emitter_buffer.drain_pending();
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batches(pending, reason, operation)
                                    .await;
                            }
                        }
                        break;
                    }
                    RelayInteractionEvent::Wake => {
                        let retry_was_active = retry_schedule.is_active();
                        let retry_is_due = retry_schedule.retry_is_due();
                        let stall_cleared = retry_schedule.release_if_stall_cleared(
                            fault_injection.emitter_should_stall(&task_emitter),
                        );
                        if !retry_is_due && !stall_cleared {
                            continue;
                        }
                        let retry_attempt = retry_was_active && (retry_is_due || stall_cleared);
                        if reconnect_on_wake || sink.missing_reason().is_some() {
                            sink = SinkEmitter::new_until_cancelled(
                                SinkEmitterInit {
                                    sink: &task_sink,
                                    flush_policy: &task_flush_policy,
                                    publishing: task_publishing,
                                    client: client.as_deref(),
                                    resolved: resolved_client.as_ref(),
                                    catalog_client: catalog_client.as_deref(),
                                    catalog_resolved: resolved_catalog_client.as_ref(),
                                    context: &context,
                                    runtime: sink_runtime.clone(),
                                },
                                &mut work_cancel_rx,
                            )
                            .await;
                            if let Some(reason) = sink.missing_reason() {
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: publish_backoff.take_next_delay(),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: false,
                                        reason: Some(reason),
                                    },
                                );
                                reconnect_on_wake = true;
                                continue;
                            }
                            reconnect_on_wake = false;
                            runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                        }
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        match sink
                            .flush_due(
                                &task_sink,
                                &context,
                                &mut control,
                                codec.clone(),
                                &mut emitter_buffer,
                                retry_attempt,
                            )
                            .await
                        {
                            Ok(Some(report)) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                batch_context.observe_sent(&report);
                            }
                            Ok(None) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                            }
                            Err(error) if emitter_publish_error_is_retryable(&error) => {
                                let reason = emitter_error_message(&error);
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: emitter_retry_delay(&mut publish_backoff, &error),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: *error.current_context()
                                            == EmitterRuntimeError::PublishStalled,
                                        reason: Some(&reason),
                                    },
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                context.report_flush_error(task_sink.label(), &reason);
                            }
                            Err(error) => {
                                retry_schedule.clear();
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(task_sink.label(), &reason);
                                let pending = emitter_buffer.drain_pending();
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batches(pending, reason, operation)
                                    .await;
                            }
                        }
                    }
                    RelayInteractionEvent::Batch {
                        relay: input_relay,
                        batch,
                    } => {
                        let delivery_observation = batch.delivery_observation(current_timestamp());
                        let physical_node_id =
                            runtime.inner.remote_dispatch.local_node_id.read().clone();
                        runtime
                            .inner
                            .metrics
                            .observe_global_node_received(NodeBatchObservation {
                                domain: &task_domain,
                                kind: ModelKind::Emitter,
                                node: &ModelName::from(&task_emitter),
                                relay: &input_relay,
                                physical_node_id: physical_node_id.as_ref(),
                                messages: batch.message_count(),
                                bytes: batch.estimated_bytes(),
                                domain_timestamp: delivery_observation.domain_timestamp,
                            });
                        runtime.mark_branch_aggregated_metrics_updated(
                            &task_domain,
                            ModelKind::Emitter,
                            &task_emitter,
                        );
                        for seconds in delivery_observation.latency_seconds {
                            runtime
                                .inner
                                .metrics
                                .observe_global_delivery_latency_at_domain_time(
                                    NodeLatencyObservation {
                                        domain: &task_domain,
                                        kind: ModelKind::Emitter,
                                        node: &ModelName::from(&task_emitter),
                                        relay: &input_relay,
                                        physical_node_id: physical_node_id.as_ref(),
                                        seconds,
                                        domain_timestamp: delivery_observation.domain_timestamp,
                                    },
                                );
                        }
                        let wait_for_required_state = !interaction.is_terminal_drain();
                        let publish_batch = match batch_context
                            .process(
                                &input_relay,
                                batch,
                                &mut work_cancel_rx,
                                wait_for_required_state,
                                work.as_mut(),
                            )
                            .await
                        {
                            Some(batch) => batch,
                            None => continue,
                        };

                        if interaction.is_draining() {
                            if let Err(error) =
                                emitter_buffer.retain_for_drain(publish_batch.clone())
                            {
                                let reason = emitter_error_message(&error);
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batch(publish_batch, reason, operation)
                                    .await;
                            } else if !interaction.is_terminal_drain() {
                                retry_schedule.include_acks(publish_batch.merged_acks());
                            }
                            continue;
                        }

                        if retry_schedule.is_active()
                            || emitter_unavailable_reason(&sink, &fault_injection, &task_emitter)
                                .is_some()
                        {
                            let unavailable =
                                emitter_unavailable_reason(&sink, &fault_injection, &task_emitter);
                            if let Err(error) = emitter_buffer.push(&context, publish_batch.clone())
                            {
                                let reason = emitter_error_message(&error);
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batch(publish_batch, reason, operation)
                                    .await;
                                continue;
                            }
                            retry_schedule.include_acks(publish_batch.merged_acks());
                            if !retry_schedule.is_active() {
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait: publish_backoff.take_next_delay(),
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: fault_injection
                                            .emitter_should_stall(&task_emitter),
                                        reason: unavailable.as_deref(),
                                    },
                                );
                                if let Some(reason) = unavailable.as_deref() {
                                    context.report_publish_error(task_sink.label(), reason);
                                }
                            }
                            reconnect_on_wake |= sink.missing_reason().is_some();
                            continue;
                        }

                        let mut pending_batch = Some(publish_batch);
                        let mut control = EmitterPublishControl {
                            fault_injection: &fault_injection,
                            shutdown_rx: &mut shutdown_rx,
                            stop_rx: &mut stop_rx,
                            backoff: &mut publish_backoff,
                        };
                        let publish_result = sink
                            .publish_batch(
                                &task_sink,
                                &context,
                                &mut control,
                                codec.clone(),
                                &mut emitter_buffer,
                                pending_batch
                                    .as_ref()
                                    .verified("this branch only runs while a batch is pending")
                                    .clone(),
                            )
                            .await;
                        match publish_result {
                            Ok(Some(report)) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                batch_context.observe_sent(&report);
                                pending_batch.take();
                            }
                            Ok(None) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                pending_batch.take();
                            }
                            Err(failure) if emitter_publish_error_is_retryable(&failure.error) => {
                                let EmitterPublishFailure { error, batch_owner } = failure;
                                let wait = emitter_retry_delay(&mut publish_backoff, &error);
                                if let EmitterPublishBatchOwner::Caller = batch_owner
                                    && let Some(batch) = pending_batch.take()
                                    && let Err(retain_error) =
                                        emitter_buffer.push(&context, batch.clone())
                                {
                                    retry_schedule.clear();
                                    let reason = emitter_error_message(&retain_error);
                                    let operation = emitter_message_error_operation(
                                        &retain_error,
                                        codec.is_some(),
                                    );
                                    batch_context
                                        .handle_publish_error_batch(batch, reason, operation)
                                        .await;
                                    continue;
                                }
                                if let EmitterPublishBatchOwner::Buffer
                                | EmitterPublishBatchOwner::Sink = batch_owner
                                {
                                    pending_batch.take();
                                }
                                let reason = emitter_error_message(&error);
                                retry_schedule.defer(
                                    &context,
                                    EmitterRetryDeferral {
                                        wait,
                                        acks: sink.pending_acks(&emitter_buffer),
                                        waiting_for_stall_clear: *error.current_context()
                                            == EmitterRuntimeError::PublishStalled,
                                        reason: Some(&reason),
                                    },
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                context.report_publish_error(task_sink.label(), &reason);
                            }
                            Err(failure) => {
                                retry_schedule.clear();
                                let (error, failed_batches) = failure
                                    .drain_failed_batches(&mut pending_batch, &mut emitter_buffer);
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_publish_error(task_sink.label(), &reason);
                                let operation =
                                    emitter_message_error_operation(&error, codec.is_some());
                                batch_context
                                    .handle_publish_error_batches(failed_batches, reason, operation)
                                    .await;
                            }
                        }
                    }
                }
            }
            drop(work_cancel_forwarder);
        });
        Ok(ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        })
    }
}

fn resolve_emitter_client(
    runtime: &Runtime,
    domain: &DomainName,
    sink: &EmitSink,
    client: Option<&Model>,
) -> Result<Option<ResolvedClientConfig>, RuntimeError> {
    let resolved = match (sink, client) {
        (EmitSink::Kafka { .. }, Some(Model::ClientKafka(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::Pulsar { .. }, Some(Model::ClientPulsar(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::RabbitMq { .. }, Some(Model::ClientRabbitMq(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::Redis { .. }, Some(Model::ClientRedis(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::Mqtt { .. }, Some(Model::ClientMqtt(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::Nats { .. }, Some(Model::ClientNats(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::ZeroMq { .. }, Some(Model::ClientZeroMq(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::Syslog { .. }, Some(Model::ClientSyslog(client))) => Some((|| {
            let resolved =
                runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config)?;
            let config = crate::runtime::syslog::SyslogClientConfig::parse(
                &resolved.entries,
                crate::runtime::syslog::SyslogDirection::Emit,
            )
            .map_err(|error| error.to_string())?;
            if config.protocol == crate::runtime::syslog::SyslogProtocol::Tls {
                config
                    .tls_client_config()
                    .map_err(|error| error.to_string())?;
            }
            Ok(resolved)
        })()),
        (EmitSink::Sqs { .. }, Some(Model::ClientSqs(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::Sentry { .. }, Some(Model::ClientSentry(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::Otel { .. }, Some(Model::ClientOtel(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::ClickHouse { .. }, Some(Model::ClientClickHouse(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::Postgres { .. }, Some(Model::ClientPostgres(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::MySql { .. }, Some(Model::ClientMySql(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (EmitSink::MongoDb { .. }, Some(Model::ClientMongoDb(client))) => {
            Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config))
        }
        (
            EmitSink::Iceberg {
                backend: IcebergStorageBackend::S3,
                ..
            },
            Some(Model::ClientS3(client)),
        ) => Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config)),
        (
            EmitSink::Iceberg {
                backend: IcebergStorageBackend::Gcs,
                ..
            },
            Some(Model::ClientGcs(client)),
        ) => Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config)),
        (
            EmitSink::Iceberg {
                backend: IcebergStorageBackend::AzureBlob,
                ..
            },
            Some(Model::ClientAzureBlob(client)),
        ) => Some(runtime.resolve_client_config(domain, client.mount.as_ref(), &client.config)),
        _ => None,
    };
    resolved
        .transpose()
        .map_err(|reason| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "failed to resolve {} emitter client: {}",
                sink.label(),
                reason
            ),
        })
}

fn resolve_emitter_catalog_client(
    runtime: &Runtime,
    domain: &DomainName,
    sink: &EmitSink,
    client: Option<&Model>,
) -> Result<Option<ResolvedClientConfig>, RuntimeError> {
    let Some(catalog_client) = sink.iceberg_catalog_client() else {
        return Ok(None);
    };
    let Some(Model::ClientIcebergRest(client)) = client else {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "Iceberg catalog client '{}' must be an ICEBERG_REST client",
                catalog_client.as_str()
            ),
        });
    };
    runtime
        .resolve_client_config(domain, client.mount.as_ref(), &client.config)
        .map(Some)
        .map_err(|reason| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "failed to resolve Iceberg REST catalog client '{}': {}",
                catalog_client.as_str(),
                reason
            ),
        })
}

impl EmitterBatchContext<'_> {
    fn observe_sent(&self, report: &PublishReport) {
        if let Some(relay) = self.metric_relay {
            self.runtime
                .inner
                .metrics
                .observe_global_node_sent(NodeBatchObservation {
                    domain: self.domain,
                    kind: ModelKind::Emitter,
                    node: &ModelName::from(self.emitter),
                    relay,
                    physical_node_id: self
                        .runtime
                        .inner
                        .remote_dispatch
                        .local_node_id
                        .read()
                        .as_ref(),
                    messages: report.messages,
                    bytes: report.bytes,
                    domain_timestamp: Some(report.domain_timestamp),
                });
        } else {
            self.runtime
                .inner
                .metrics
                .observe_global_node_without_stream_sent(NodeWithoutRelayObservation {
                    domain: self.domain,
                    kind: ModelKind::Emitter,
                    node: &ModelName::from(self.emitter),
                    physical_node_id: self
                        .runtime
                        .inner
                        .remote_dispatch
                        .local_node_id
                        .read()
                        .as_ref(),
                    messages: report.messages,
                    bytes: report.bytes,
                    domain_timestamp: Some(report.domain_timestamp),
                });
        }
        self.runtime.mark_branch_aggregated_metrics_updated(
            self.domain,
            ModelKind::Emitter,
            self.emitter,
        );
    }

    async fn handle_publish_error_batches(
        &self,
        batches: impl IntoIterator<Item = EmitterPublishBatch>,
        reason: String,
        operation: MessageErrorOperation,
    ) {
        for batch in batches {
            self.handle_publish_error_batch(batch, reason.clone(), operation)
                .await;
        }
    }

    async fn handle_publish_error_batch(
        &self,
        batch: EmitterPublishBatch,
        reason: String,
        operation: MessageErrorOperation,
    ) {
        let delivered = batch.delivered.clone();
        let messages = match batch.batch.try_into_messages() {
            Ok(messages) => messages,
            Err(error) => {
                let (message, batch) = *error;
                self.runtime.handle_general_error_for_acks(
                    self.domain,
                    ModelKind::Emitter,
                    self.emitter,
                    self.error_policies,
                    batch.acks.iter(),
                    format!("{reason}; {message}"),
                );
                return;
            }
        };
        for (row, message) in messages.into_iter().enumerate() {
            if delivered.get(row).copied().unwrap_or(false) {
                continue;
            }
            self.runtime
                .handle_structured_message_error(MessageErrorHandling {
                    domain: self.domain,
                    node_kind: ModelKind::Emitter,
                    node: &ModelName::from(self.emitter),
                    source_route: None,
                    policy: &self.error_policies.message,
                    message,
                    error: structured_message_error(
                        batch.execution_now,
                        MessageErrorCode::External,
                        reason.clone(),
                        operation,
                        None,
                        std::iter::empty(),
                    ),
                    partial_output: None,
                    materialized_state: HashMap::default(),
                    ingest_metadata: None,
                    execution_now: batch.execution_now,
                })
                .await;
        }
    }

    async fn process(
        &self,
        input_relay: &RelayName,
        batch: RelayRecordBatch,
        shutdown_rx: &mut watch::Receiver<bool>,
        wait_for_required_state: bool,
        quiesce_work: Option<&mut NodeQuiesceWorkGuard>,
    ) -> Option<EmitterPublishBatch> {
        let dependency_error_acks = batch.acks.clone();
        let batch = match self
            .runtime
            .resolve_materialized_dependencies_for_batch(
                self.domain,
                input_relay,
                self.materialized_state,
                batch,
                MaterializedBatchWaitContext {
                    shutdown_rx,
                    wait_for_required_state,
                    quiesce_work,
                },
            )
            .await
        {
            Ok(Some(resolved)) => resolved,
            Ok(None) => return None,
            Err(error) => {
                self.runtime.handle_internal_processor_error_for_acks(
                    self.domain,
                    ModelKind::Emitter,
                    self.emitter,
                    self.error_policies,
                    dependency_error_acks.iter(),
                    format!(
                        "emitter '{}' failed to resolve materialized dependencies: {error}",
                        self.emitter.as_str()
                    ),
                );
                return None;
            }
        };
        // The node-wide dependencies are resolved once for the batch, so every emitter program
        // reads that snapshot instead of re-reading the state store per program.
        let (batch, materialized_values, execution_now) = batch;
        let batch = self
            .filter_source_batch(input_relay, batch, &materialized_values, execution_now)
            .await?;
        let sqs_message_groups = match self.sqs_fifo_group {
            None => vec![Ok(None); batch.batch.batch().num_rows()],
            Some(CompiledSqsFifoGroup::FromBranch) => batch
                .keys
                .iter()
                .map(|key| match key.as_ref() {
                    Some(key) => Ok(Some(key.as_str().to_string())),
                    None => {
                        Err("SQS FIFO GROUP FROM BRANCH received an unbranched record".to_string())
                    }
                })
                .collect(),
            Some(CompiledSqsFifoGroup::Expression(program)) => {
                match evaluate_sqs_fifo_group_program(
                    self.emitter,
                    program,
                    &batch,
                    execution_now,
                    &materialized_values,
                )
                .await
                {
                    Ok(groups) => groups,
                    Err(error) => {
                        self.runtime.handle_general_error_for_acks(
                            self.domain,
                            ModelKind::Emitter,
                            self.emitter,
                            self.error_policies,
                            error.acks.iter(),
                            error.reason,
                        );
                        return None;
                    }
                }
            }
        };
        let Some(filter_map) = self.filter_map else {
            return match EmitterPublishBatch::from_batch(batch, execution_now)
                .with_sqs_message_groups(sqs_message_groups)
            {
                Ok(batch) => Some(batch),
                Err(error) => {
                    self.runtime.handle_general_error_for_acks(
                        self.domain,
                        ModelKind::Emitter,
                        self.emitter,
                        self.error_policies,
                        std::iter::empty::<&AckSet>(),
                        format!(
                            "emitter '{}' failed to build SQS FIFO group batch: {error}",
                            self.emitter.as_str()
                        ),
                    );
                    None
                }
            };
        };
        match plan_emitter_filter_map_batch(
            self.emitter,
            filter_map,
            batch,
            execution_now,
            &materialized_values,
        )
        .await
        {
            Ok(plan) => {
                let selected_sqs_message_groups = plan
                    .source_rows
                    .iter()
                    .map(|row| match sqs_message_groups.get(*row) {
                        Some(group) => group.clone(),
                        None => Err(format!(
                            "SQS FIFO group source row {row} is outside the source batch"
                        )),
                    })
                    .collect::<Vec<_>>();
                self.runtime
                    .handle_planned_message_errors(
                        self.domain,
                        ModelKind::Emitter,
                        self.emitter,
                        self.error_policies,
                        plan.message_errors,
                    )
                    .await;
                let batch = plan.batch?;
                match EmitterPublishBatch::new(batch, plan.headers, execution_now)
                    .and_then(|batch| batch.with_sqs_message_groups(selected_sqs_message_groups))
                {
                    Ok(batch) => Some(batch),
                    Err(error) => {
                        self.runtime.handle_general_error_for_acks(
                            self.domain,
                            ModelKind::Emitter,
                            self.emitter,
                            self.error_policies,
                            std::iter::empty::<&AckSet>(),
                            format!(
                                "emitter '{}' failed to build filtered header batch: {}",
                                self.emitter.as_str(),
                                error
                            ),
                        );
                        None
                    }
                }
            }
            Err(error) => {
                self.runtime.handle_general_error_for_acks(
                    self.domain,
                    ModelKind::Emitter,
                    self.emitter,
                    self.error_policies,
                    error.acks.iter(),
                    error.reason,
                );
                None
            }
        }
    }

    async fn filter_source_batch(
        &self,
        input_relay: &RelayName,
        batch: RelayRecordBatch,
        side_inputs: &HashMap<String, RuntimeValue>,
        execution_now: Timestamp,
    ) -> Option<RelayRecordBatch> {
        let Some(program) = self.source_filters.get(input_relay) else {
            return Some(batch);
        };
        let plan = match plan_filter_map_messages(
            "emitter",
            self.emitter,
            "FROM WHERE",
            program,
            batch,
            execution_now,
            side_inputs,
        )
        .await
        {
            Ok(plan) => plan,
            Err(error) => {
                self.runtime.handle_general_error_for_acks(
                    self.domain,
                    ModelKind::Emitter,
                    self.emitter,
                    self.error_policies,
                    error.acks.iter(),
                    format!("input relay '{}': {}", input_relay.as_str(), error.reason),
                );
                return None;
            }
        };
        self.runtime
            .handle_planned_message_errors(
                self.domain,
                ModelKind::Emitter,
                self.emitter,
                self.error_policies,
                plan.message_errors,
            )
            .await;
        plan.batch
    }
}

#[cfg(test)]
mod publishing_mode_tests {
    use nonzero_ext::nonzero;

    use super::*;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn retry(backoff: &str, max_backoff: &str) -> RetryPolicy {
        RetryPolicy {
            backoff: backoff.to_string(),
            max_backoff: max_backoff.to_string(),
        }
    }

    fn kafka_sink() -> EmitSink {
        EmitSink::Kafka {
            client: named("kafka_client"),
            topic: named("events"),
        }
    }

    #[test]
    fn parses_declared_emitter_retry_confirmation_window_and_timeout() {
        let domain = DomainName::try_from("test").expect("valid domain");
        let emitter = named("out");
        let settings = EmitterPublishingSettings::parse(
            &domain,
            &emitter,
            &kafka_sink(),
            &EmitterPublishingMode::BrokerAck {
                window: EmitterAckWindow::Parallel {
                    max: nonzero!(17u64),
                },
                ack_timeout: "3s".to_string(),
                retry_policy: retry("25ms", "2s"),
            },
        )
        .expect("valid publishing settings");

        assert_eq!(settings.retry_policy.backoff, Duration::from_millis(25));
        assert_eq!(settings.retry_policy.max_backoff, Duration::from_secs(2));
        assert_eq!(
            settings.transport,
            EmitterTransportMode::Broker(BrokerPublishingMode::Ack(AckConfirmation {
                max_in_flight: nonzero!(17usize),
                timeout: Duration::from_secs(3),
            }))
        );
    }

    #[test]
    fn parses_transport_specific_mqtt_and_jetstream_confirmation_modes() {
        let domain = DomainName::try_from("test").expect("valid domain");
        let emitter = named("out");
        let mqtt = EmitterPublishingSettings::parse(
            &domain,
            &emitter,
            &EmitSink::Mqtt {
                client: named("mqtt_client"),
                topic: named("events"),
            },
            &EmitterPublishingMode::MqttQos2 {
                window: EmitterAckWindow::Sequential,
                ack_timeout: "7s".to_string(),
                retry_policy: retry("10ms", "1s"),
            },
        )
        .expect("valid MQTT mode");
        assert_eq!(
            mqtt.transport,
            EmitterTransportMode::Mqtt(MqttPublishingMode::Qos2(AckConfirmation {
                max_in_flight: nonzero!(1usize),
                timeout: Duration::from_secs(7),
            }))
        );

        let nats = EmitterPublishingSettings::parse(
            &domain,
            &emitter,
            &EmitSink::Nats {
                client: named("nats_client"),
                subject: named("events"),
            },
            &EmitterPublishingMode::NatsJetStream {
                window: EmitterAckWindow::Parallel {
                    max: nonzero!(23u64),
                },
                ack_timeout: "11s".to_string(),
                retry_policy: retry("10ms", "1s"),
            },
        )
        .expect("valid JetStream mode");
        assert_eq!(
            nats.transport,
            EmitterTransportMode::Nats(NatsPublishingMode::JetStream(AckConfirmation {
                max_in_flight: nonzero!(23usize),
                timeout: Duration::from_secs(11),
            }))
        );
    }

    #[test]
    fn rejects_foreign_mode_and_inverted_retry_bounds() {
        let domain = DomainName::try_from("test").expect("valid domain");
        let emitter = named("out");
        let foreign = EmitterPublishingSettings::parse(
            &domain,
            &emitter,
            &kafka_sink(),
            &EmitterPublishingMode::MqttQos0 {
                retry_policy: retry("25ms", "2s"),
            },
        )
        .expect_err("foreign mode must fail");
        assert!(
            foreign
                .to_string()
                .contains("not supported by the KAFKA sink")
        );

        let inverted = EmitterPublishingSettings::parse(
            &domain,
            &emitter,
            &kafka_sink(),
            &EmitterPublishingMode::NoAck {
                retry_policy: retry("2s", "25ms"),
            },
        )
        .expect_err("inverted retry bounds must fail");
        assert!(inverted.to_string().contains("greater than or equal"));
    }

    #[tokio::test]
    async fn emitter_backoff_resets_to_the_declared_initial_delay() {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
        };
        let mut backoff = RuntimeReconnectBackoff::from_policy(policy);
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        assert_eq!(backoff.next_delay(), Duration::from_millis(1));
        assert!(backoff.wait(&mut shutdown_rx).await);
        assert_eq!(backoff.next_delay(), Duration::from_millis(2));
        assert!(backoff.wait(&mut shutdown_rx).await);
        assert_eq!(backoff.next_delay(), Duration::from_millis(4));
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(1));
    }

    #[test]
    fn emitter_retry_delay_honors_the_server_minimum() {
        let mut backoff = RuntimeReconnectBackoff::from_policy(ParsedRetryPolicy {
            backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
        });
        let error = emitter_publish_error_with_minimum_retry_delay(
            "receiver requested a longer retry",
            Duration::from_secs(2),
        );

        assert_eq!(
            emitter_retry_delay(&mut backoff, &error),
            Duration::from_secs(2)
        );
        assert_eq!(backoff.next_delay(), Duration::from_millis(20));
    }

    #[tokio::test]
    async fn queued_stop_bounds_an_active_infrastructure_retry() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, mut stop_rx) = watch::channel(None);
        let task_stop_signal = stop_signal.clone();
        let retry_started = Arc::new(Notify::new());
        let task_retry_started = retry_started.clone();
        let task = tokio::spawn(async move {
            let mut backoff = RuntimeReconnectBackoff::from_policy(ParsedRetryPolicy {
                backoff: Duration::from_secs(30),
                max_backoff: Duration::from_secs(30),
            });
            let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
            let (acks, _completion) = AckSet::root();
            task_retry_started.notify_one();
            let retry = backoff.wait_with_ack_alive(&mut shutdown_rx, &acks);
            assert!(
                await_until_emitter_stop_deadline(&mut stop_rx, retry)
                    .await
                    .is_err(),
                "the queued stop deadline must interrupt the active retry wait"
            );

            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("the stop command must remain queued while retrying");
            };
            task_stop_signal.send_replace(None);
            let _ = response.send(Err(
                "infrastructure retry exceeded drain deadline".to_string()
            ));
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };
        retry_started.notified().await;

        let started = Instant::now();
        let failed = scheduled
            .stop(Duration::from_millis(40))
            .await
            .expect_err("the active infrastructure retry must fail the bounded drain");
        assert_eq!(
            failed.reason(),
            "infrastructure retry exceeded drain deadline"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a 30 second retry backoff must not hide a 40 millisecond drain deadline"
        );
        assert!(
            failed
                .into_task()
                .expect("the failed drain must retain the old task")
                .stop_signal
                .borrow()
                .is_none(),
            "a recoverable stop failure must clear its stop signal"
        );
    }

    #[tokio::test]
    async fn cancelled_message_error_delivery_keeps_the_record_pending() {
        let schema = Arc::new(compile_schema(&nervix_models::CreateSchema {
            name: named("events"),
            fields: vec![nervix_models::SchemaField {
                name: named("value"),
                ty: nervix_models::ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        }));
        let (acks, _completion) = AckSet::root();
        let batch = RelayRecordBatch::single(
            schema,
            None,
            test_runtime_row([(
                "value".to_string(),
                RuntimeValue::String("poison".to_string()),
            )]),
            acks,
        )
        .expect("test emitter batch must build");
        let mut batch = EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(100));

        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                batch.mark_rejected_after_delivery(0, std::future::pending()),
            )
            .await
            .is_err(),
            "the simulated message-error delivery must remain pending"
        );
        assert!(
            !batch.delivered[0],
            "cancelling message-error delivery must leave the poison record retryable"
        );

        batch
            .mark_rejected_after_delivery(0, std::future::ready(()))
            .await
            .expect("completed message-error delivery must account for the record");
        assert!(batch.delivered[0]);
    }

    #[tokio::test]
    async fn rejected_record_ack_remains_held_for_message_error_delivery() {
        let schema = Arc::new(compile_schema(&nervix_models::CreateSchema {
            name: named("events"),
            fields: vec![nervix_models::SchemaField {
                name: named("value"),
                ty: nervix_models::ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        }));
        let (acks, mut completion) = AckSet::root();
        let message_error_acks = acks.clone();
        let batch = RelayRecordBatch::single(
            schema,
            None,
            test_runtime_row([(
                "value".to_string(),
                RuntimeValue::String("poison".to_string()),
            )]),
            acks,
        )
        .expect("test emitter batch must build");
        let mut batch = EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(100));
        batch
            .mark_rejected(0)
            .expect("test record must be marked rejected");
        let mut buffer = EmitterBatchBuffer::default();
        buffer.pending.push(batch);

        buffer.clear();

        assert!(
            tokio::time::timeout(Duration::from_millis(20), completion.wait_for_progress(),)
                .await
                .is_err(),
            "clearing an accounted poison record must not release its source ACK"
        );
        message_error_acks.ack_success();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use nervix_models::{
        ChannelName, ClientName, CollectionName, CreateSchema, DomainName, EmitterName, ModelName,
        ParseAsType, QueueName, RelayName, SchemaName, SubjectName, TableName, TopicName,
    };
    use nonzero_ext::nonzero;

    use super::*;

    fn input_schema() -> Arc<CompiledSchema> {
        static SCHEMA: OnceLock<Arc<CompiledSchema>> = OnceLock::new();
        let value = FieldName::parse("value").expect("valid field name");
        SCHEMA
            .get_or_init(|| {
                Arc::new(compile_schema(&CreateSchema {
                    name: SchemaName::from(
                        &ModelName::parse("emitter_input").expect("valid schema name"),
                    ),
                    fields: vec![nervix_models::SchemaField {
                        name: value,
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }))
            })
            .clone()
    }

    fn input_batch_with(value: i64, timestamp: i64, acks: AckSet) -> RelayRecordBatch {
        RelayRecordBatch::single(
            input_schema(),
            None,
            test_runtime_row([("value".to_string(), RuntimeValue::I64(value))])
                .with_ingested_at_watermarks(Timestamp::from_unix_nanos(timestamp)),
            acks,
        )
        .expect("valid emitter input batch")
    }

    fn input_batch() -> RelayRecordBatch {
        input_batch_with(1, 0, AckSet::empty())
    }

    fn input_value(batch: &RelayRecordBatch) -> i64 {
        let record = batch.runtime_row(0).expect("batch must contain one row");
        let Ok(Some(RuntimeValue::I64(value))) = record.value("value") else {
            panic!("test batch must contain an I64 value")
        };
        value
    }

    fn sink_context() -> EmitterSinkContext {
        let domain = DomainName::parse("emitter_tests").expect("valid domain");
        EmitterSinkContext {
            runtime: Runtime::default(),
            clock: test_domain_clock(&domain),
            domain,
            emitter: EmitterName::parse("output").expect("valid emitter name"),
            error_policies: ErrorPolicies::handled_by_log(),
            udfs: None,
        }
    }

    #[test]
    fn publish_batch_requires_one_header_row_per_record() {
        let batch = input_batch();
        let from_batch =
            EmitterPublishBatch::from_batch(batch.clone(), Timestamp::from_unix_nanos(100));
        assert!(from_batch.headers.is_none());
        assert_eq!(from_batch.message_count(), 1);
        assert_eq!(
            from_batch.domain_timestamp(),
            Some(Timestamp::from_unix_nanos(0))
        );

        let headers = vec![vec![("route".to_string(), "fast".to_string())]];
        let with_headers = EmitterPublishBatch::new(
            batch.clone(),
            Some(headers.clone()),
            Timestamp::from_unix_nanos(100),
        )
        .expect("row-aligned headers must build");
        assert_eq!(with_headers.headers.as_ref(), Some(&headers));
        let header_bytes: u64 = "routefast".len().arch_into();
        assert_eq!(
            with_headers.estimated_bytes(),
            batch.estimated_bytes() + header_bytes
        );

        let error = match EmitterPublishBatch::new(
            batch,
            Some(Vec::new()),
            Timestamp::from_unix_nanos(100),
        ) {
            Err(error) => error,
            Ok(_) => panic!("missing row headers must be rejected"),
        };
        assert!(error.contains("header count 0 does not match row count 1"));
    }

    #[tokio::test]
    async fn publish_batch_ack_helpers_preserve_and_complete_all_roots() {
        let (acks, completion) = AckSet::root();
        let batch = EmitterPublishBatch::from_batch(
            input_batch_with(1, 0, acks),
            Timestamp::from_unix_nanos(100),
        );

        assert!(!batch.merged_acks().is_empty());
        batch.merged_acks().ack_success();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
    }

    #[test]
    fn batch_buffer_rejects_input_without_an_initialized_flush_policy() {
        let mut buffer = EmitterBatchBuffer::default();
        let error = buffer
            .push(
                &sink_context(),
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect_err("an unconfigured buffer must reject input");

        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::FlushPolicyNotInitialized
        );
        assert!(buffer.is_empty());
        assert_eq!(buffer.pending_bytes, 0);
        assert!(buffer.deadline().is_none());
    }

    #[test]
    fn batch_buffer_tracks_size_messages_deadline_and_latest_timestamp() {
        let reported_messages = Arc::new(AtomicUsize::new(0));
        let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        buffer.buffered_messages = buffered_messages.clone();
        let first = EmitterPublishBatch::from_batch(
            input_batch_with(1, 10, AckSet::empty()),
            Timestamp::from_unix_nanos(100),
        );
        let second_batch = RelayRecordBatch::from_messages(
            input_schema(),
            vec![
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("value".to_string(), RuntimeValue::I64(2))])
                        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(20)),
                    acks: AckSet::empty(),
                },
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("value".to_string(), RuntimeValue::I64(3))])
                        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(20)),
                    acks: AckSet::empty(),
                },
            ],
        )
        .expect("valid multi-row emitter input batch");
        let second = EmitterPublishBatch::new(
            second_batch,
            Some(vec![
                vec![("name".to_string(), "value".to_string())],
                Vec::new(),
            ]),
            Timestamp::from_unix_nanos(100),
        )
        .expect("headers must align");
        let expected_bytes = first
            .estimated_bytes()
            .checked_add(second.estimated_bytes())
            .assured("the two test batches are far smaller than the u64 byte range");

        let context = sink_context();
        assert!(!buffer.push(&context, first).expect("first batch must buffer"));
        assert_eq!(buffer.pending_messages, 1);
        assert!(buffer.deadline().is_some(), "the first push arms the cadence");
        assert!(!buffer.push(&context, second).expect("second batch must buffer"));

        assert!(
            !buffer
                .is_due(&context)
                .expect("the fixture clock stays installed"),
            "a second push does not bring the cadence forward"
        );
        assert_eq!(reported_messages.load(Ordering::Acquire), 3);
        assert_eq!(buffer.pending_messages, 3);
        let report = buffer
            .report()
            .expect("pending batches must produce a report");
        assert_eq!(report.messages, 3);
        assert_eq!(report.bytes, expected_bytes);
        assert_eq!(report.domain_timestamp, Timestamp::from_unix_nanos(20));
        assert_eq!(buffer.pending_bytes, expected_bytes);
    }

    #[test]
    fn batch_buffer_honors_its_size_boundary_and_logical_cadence() {
        let context = sink_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: 1,
        });

        assert!(
            buffer
                .push(
                    &context,
                    EmitterPublishBatch::from_batch(
                        input_batch(),
                        Timestamp::from_unix_nanos(100),
                    )
                )
                .expect("batch must buffer")
        );
        assert!(matches!(
            buffer.deadline(),
            Some(BranchBufferDeadline::Logical(_))
        ));
        assert!(
            !buffer
                .is_due(&context)
                .expect("the fixture clock stays installed")
        );

        let mut immediate = EmitterBatchBuffer::default();
        immediate.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        immediate
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("batch must buffer");
        assert!(matches!(
            immediate.deadline(),
            Some(BranchBufferDeadline::Physical(_))
        ));
    }

    #[test]
    fn forced_retry_ignores_the_ordinary_buffer_deadline() {
        let context = sink_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("retry batch must buffer");
        assert!(
            !buffer
                .is_due(&context)
                .expect("the fixture clock stays installed")
        );

        assert!(
            buffer
                .should_flush(&context, true)
                .expect("a forced flush needs no clock read")
        );
        assert!(
            !buffer
                .should_flush(&context, false)
                .expect("the fixture clock stays installed")
        );
    }

    #[tokio::test]
    async fn retry_wake_attempts_a_buffer_before_its_ordinary_deadline() {
        let fault_injection = ConfiguredFaultInjection::default();
        let mut backoff = RuntimeReconnectBackoff::default();
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (_stop_tx, mut stop_rx) = watch::channel(None);
        let mut control = EmitterPublishControl {
            fault_injection: &fault_injection,
            shutdown_rx: &mut shutdown_rx,
            stop_rx: &mut stop_rx,
            backoff: &mut backoff,
        };
        let context = sink_context();
        let sink_config = EmitSink::Nats {
            client: ClientName::parse("client").expect("valid client name"),
            subject: SubjectName::parse("subject").expect("valid subject name"),
        };
        let mut sink = SinkEmitter::Missing {
            reason: "test sink intentionally has no client".to_string(),
        };
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("retry batch must buffer");

        assert!(
            sink.flush_due(
                &sink_config,
                &context,
                &mut control,
                None,
                &mut buffer,
                false,
            )
            .await
            .expect("ordinary wake must remain idle")
            .is_none()
        );
        let error = match sink
            .flush_due(
                &sink_config,
                &context,
                &mut control,
                None,
                &mut buffer,
                true,
            )
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("retry wake must attempt the buffered batch"),
        };

        assert_eq!(*error.current_context(), EmitterRuntimeError::EncodeBatch);
        assert_eq!(buffer.pending.len(), 1);
    }

    #[test]
    fn generic_and_iceberg_buffer_counts_are_summed_independently() {
        let reported = Arc::new(AtomicUsize::new(0));
        let buffered = EmitterBufferedMessages::new(reported.clone());

        buffered.set_generic(2);
        buffered.set_iceberg(3);
        assert_eq!(reported.load(Ordering::Acquire), 5);

        buffered.set_generic(0);
        assert_eq!(reported.load(Ordering::Acquire), 3);
        buffered.set_iceberg(0);
        assert_eq!(reported.load(Ordering::Acquire), 0);
    }

    #[test]
    fn batch_buffer_drain_clear_reconfigure_and_drop_reset_accounting() {
        let context = sink_context();
        let reported_messages = Arc::new(AtomicUsize::new(0));
        let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
        let mut buffer = EmitterBatchBuffer::new(
            &context,
            &FlushPolicy::Each {
                interval: "10s".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            buffered_messages.clone(),
        );
        assert!(buffer.flush_policy.is_some());
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("configured buffer must accept input");
        assert_eq!(buffer.pending_messages, 1);
        buffer
            .reconfigure(&context, &FlushPolicy::Immediate)
            .expect("the fixture clock stays installed");
        assert_eq!(buffer.flush_policy, Some(RuntimeFlushPolicy::Immediate));
        assert!(
            matches!(buffer.deadline(), Some(BranchBufferDeadline::Physical(_))),
            "a reconfigured Immediate cadence re-arms the retained batches physically"
        );

        let drained = buffer.drain_pending();
        assert_eq!(drained.len(), 1);
        assert!(buffer.is_empty());
        assert_eq!(buffer.pending_bytes, 0);
        assert_eq!(buffer.pending_messages, 0);
        assert!(buffer.deadline().is_none());
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);

        buffer
            .push(
                &sink_context(),
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("reconfigured buffer must accept input");
        assert_eq!(buffer.pending_messages, 1);
        buffer.clear();
        assert!(buffer.report().is_none());
        assert_eq!(buffer.pending_messages, 0);
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);

        buffered_messages.set_generic(7);
        drop(buffer);
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn batch_buffer_ack_helpers_merge_and_complete_pending_batches() {
        let (first_acks, first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        let context = sink_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(1, 0, first_acks),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("first batch must buffer");
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(2, 0, second_acks),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("second batch must buffer");

        assert!(!buffer.pending_acks().is_empty());
        buffer.pending_acks().ack_success();
        assert_eq!(first_completion.wait().await, AckOutcome::Ack);
        assert_eq!(second_completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn retained_batch_clone_owns_exactly_one_attached_ack_share() {
        let (root, mut completion) = AckSet::root();
        let attached = root.attached();
        let batch = EmitterPublishBatch::from_batch(
            input_batch_with(1, 0, attached),
            Timestamp::from_unix_nanos(100),
        );
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);

        buffer
            .push(&sink_context(), batch.clone())
            .expect("retry buffer must retain the batch");
        root.ack_success();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), completion.wait_for_progress())
                .await
                .is_err(),
            "the retained attached share must keep the root pending"
        );

        buffer.pending_acks().ack_success();
        buffer.clear();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        drop(batch);
    }

    #[test]
    fn retry_schedule_preserves_its_deadline_until_a_retry_attempt() {
        let mut retry = EmitterRetrySchedule::default();
        retry
            .schedule(Duration::from_secs(10), AckSet::empty(), false)
            .expect("the fixture backoff fits the monotonic clock range");
        let retry_at = retry.retry_at.expect("retry must have a deadline");

        assert!(!retry.retry_is_due());
        assert!(!retry.retry_is_due());
        assert_eq!(retry.retry_at, Some(retry_at));
    }

    #[test]
    fn an_active_retry_replaces_the_ordinary_cadence_wake() {
        let context = sink_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("batch must buffer");
        let cadence = buffer.deadline().expect("the push arms the logical cadence");
        let mut retry = EmitterRetrySchedule::default();

        let idle = retry.wake(RuntimeWake::never().with_buffer(&context.clock, cadence.clone()));
        assert!(
            idle.is_reached()
                .expect("the fixture clock stays installed")
                == false
        );

        retry
            .schedule(Duration::ZERO, AckSet::empty(), false)
            .expect("the fixture backoff fits the monotonic clock range");
        let deferred = retry.wake(RuntimeWake::never().with_buffer(&context.clock, cadence));
        assert!(
            deferred
                .is_reached()
                .expect("a monotonic retry deadline needs no clock read"),
            "an active retry replaces the cadence wake with its own due deadline"
        );
        assert!(
            buffer.deadline().is_some(),
            "the cadence the retry replaced stays armed underneath it"
        );
    }

    #[test]
    fn retry_schedule_releases_a_stall_as_soon_as_the_fault_clears() {
        let mut retry = EmitterRetrySchedule::default();
        retry
            .schedule(Duration::from_secs(30), AckSet::empty(), true)
            .expect("the fixture backoff fits the monotonic clock range");

        assert!(!retry.release_if_stall_cleared(true));
        assert!(retry.is_active());
        assert!(retry.release_if_stall_cleared(false));
        assert!(!retry.is_active());
    }

    #[tokio::test]
    async fn retry_schedule_heartbeats_acks_added_by_a_force_drain() {
        let (existing, mut existing_completion) = AckSet::root();
        let (force_drained, mut force_completion) = AckSet::root();
        let mut retry = EmitterRetrySchedule::default();
        retry
            .schedule(Duration::from_secs(30), existing, false)
            .expect("the fixture backoff fits the monotonic clock range");
        retry.include_acks(force_drained);
        retry.ack_alive_at = Some(
            PhysicalDeadlineCapability::new()
                .after(Duration::ZERO)
                .expect("an immediate keepalive fits the monotonic clock range"),
        );

        assert!(!retry.retry_is_due());

        assert_eq!(
            existing_completion.wait_for_progress().await,
            AckProgress::Alive
        );
        assert_eq!(
            force_completion.wait_for_progress().await,
            AckProgress::Alive
        );
    }

    #[tokio::test]
    async fn batch_buffer_drop_no_acks_every_retained_batch() {
        let (first_acks, first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        let reported_messages = Arc::new(AtomicUsize::new(0));
        let buffered_messages = Arc::new(EmitterBufferedMessages::new(reported_messages.clone()));
        let context = sink_context();
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer.buffered_messages = buffered_messages.clone();
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(1, 0, first_acks),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("first batch must buffer");
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(2, 0, second_acks),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("second batch must buffer");

        drop(buffer);

        assert_eq!(
            first_completion.wait().await,
            AckOutcome::NoAck("emitter dropped buffered batch".to_string())
        );
        assert_eq!(
            second_completion.wait().await,
            AckOutcome::NoAck("emitter dropped buffered batch".to_string())
        );
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn flush_all_returns_the_failure_and_retains_unpublished_batches() {
        let fault_injection = ConfiguredFaultInjection::default();
        let mut backoff = RuntimeReconnectBackoff::default();
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (_stop_tx, mut stop_rx) = watch::channel(None);
        let mut control = EmitterPublishControl {
            fault_injection: &fault_injection,
            shutdown_rx: &mut shutdown_rx,
            stop_rx: &mut stop_rx,
            backoff: &mut backoff,
        };
        let context = sink_context();
        let sink_config = EmitSink::Nats {
            client: ClientName::parse("client").expect("valid client name"),
            subject: SubjectName::parse("subject").expect("valid subject name"),
        };
        let mut sink = SinkEmitter::Missing {
            reason: "test sink intentionally has no client".to_string(),
        };
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer
            .push(
                &sink_context(),
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .expect("batch must buffer");

        let error = match sink
            .flush_all(&sink_config, &context, &mut control, None, &mut buffer)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("an unencodable buffered batch must fail final flush"),
        };

        assert_eq!(*error.current_context(), EmitterRuntimeError::EncodeBatch);
        assert_eq!(buffer.pending.len(), 1);
        assert_eq!(buffer.pending[0].message_count(), 1);
    }

    #[tokio::test]
    async fn scheduled_emitter_stop_returns_final_flush_failure_as_recoverable() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, _stop_rx) = watch::channel(None);
        let finished = Arc::new(AtomicBool::new(false));
        let task_finished = finished.clone();
        let task = tokio::spawn(async move {
            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("scheduled emitter must receive its stop command")
            };
            let _ = response.send(Err(
                "emitter final flush failed: broker unavailable".to_string()
            ));
            task_finished.store(true, Ordering::Release);
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };

        let error = scheduled
            .stop(Duration::from_secs(1))
            .await
            .expect_err("final flush failure must reach the stopping caller");

        assert_eq!(
            error.reason(),
            "emitter final flush failed: broker unavailable"
        );
        let mut retained = error
            .into_task()
            .expect("a failed drain must retain the scheduled task");
        let _ = (&mut retained.task).await;
        assert!(finished.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn scheduled_emitter_stop_retains_a_task_that_drops_its_response() {
        struct Dropped(Arc<AtomicBool>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, _) = watch::channel(None);
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            let _dropped = Dropped(task_dropped);
            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("scheduled emitter must receive its stop command")
            };
            drop(response);
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };

        let error = scheduled
            .stop(Duration::from_secs(1))
            .await
            .expect_err("a dropped response must fail stopping");

        assert_eq!(
            error.reason(),
            "scheduled emitter task dropped its stop response"
        );
        let mut retained = error
            .into_task()
            .expect("a dropped response must leave the task recoverable");
        assert!(!dropped.load(Ordering::Acquire));
        retained.task.abort();
        let _ = (&mut retained.task).await;
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn scheduled_emitter_stop_timeout_retains_the_task() {
        struct Dropped(Arc<AtomicBool>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, _) = watch::channel(None);
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            let _dropped = Dropped(task_dropped);
            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("scheduled emitter must receive its stop command")
            };
            let _response = response;
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };

        let error = scheduled
            .stop(Duration::from_millis(5))
            .await
            .expect_err("a missing stop response must time out");

        assert_eq!(error.reason(), "scheduled emitter task timed out draining");
        let mut retained = error
            .into_task()
            .expect("a timed-out drain must leave the task recoverable");
        assert!(!dropped.load(Ordering::Acquire));
        retained.task.abort();
        let _ = (&mut retained.task).await;
        assert!(dropped.load(Ordering::Acquire));
    }
    #[test]
    fn publish_failure_drains_exactly_the_batches_owned_by_the_buffer() {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        let context = sink_context();
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(1, 0, AckSet::empty()),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("older batch must buffer");
        buffer
            .push(
                &context,
                EmitterPublishBatch::from_batch(
                    input_batch_with(2, 0, AckSet::empty()),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("current clone must buffer");
        let mut current = Some(EmitterPublishBatch::from_batch(
            input_batch_with(2, 0, AckSet::empty()),
            Timestamp::from_unix_nanos(100),
        ));
        let failure = EmitterPublishFailure::buffer(Report::new(EmitterRuntimeError::EncodeBatch));

        let (error, failed) = failure.drain_failed_batches(&mut current, &mut buffer);

        assert_eq!(*error.current_context(), EmitterRuntimeError::EncodeBatch);
        assert!(current.is_none());
        assert!(buffer.is_empty());
        assert_eq!(
            failed.len(),
            2,
            "the current caller clone must not be duplicated"
        );
        assert_eq!(input_value(&failed[0].batch), 1);
        assert_eq!(input_value(&failed[1].batch), 2);
    }

    #[test]
    fn caller_owned_publish_failure_includes_current_after_older_buffered_batches() {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer
            .push(
                &sink_context(),
                EmitterPublishBatch::from_batch(
                    input_batch_with(1, 0, AckSet::empty()),
                    Timestamp::from_unix_nanos(100),
                ),
            )
            .expect("older batch must buffer");
        let mut current = Some(EmitterPublishBatch::from_batch(
            input_batch_with(2, 0, AckSet::empty()),
            Timestamp::from_unix_nanos(100),
        ));
        let failure = EmitterPublishFailure::caller(Report::new(
            EmitterRuntimeError::FlushPolicyNotInitialized,
        ));

        let (_error, failed) = failure.drain_failed_batches(&mut current, &mut buffer);

        assert!(current.is_none());
        assert!(buffer.is_empty());
        assert_eq!(failed.len(), 2);
        assert_eq!(input_value(&failed[0].batch), 1);
        assert_eq!(input_value(&failed[1].batch), 2);
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn buffering_does_not_wait_for_sink_fault_until_a_flush_is_required() {
        let fault_injection = ConfiguredFaultInjection::default();
        fault_injection.fail_emitter("output");
        let mut backoff = RuntimeReconnectBackoff::default();
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (_stop_tx, mut stop_rx) = watch::channel(None);
        let mut control = EmitterPublishControl {
            fault_injection: &fault_injection,
            shutdown_rx: &mut shutdown_rx,
            stop_rx: &mut stop_rx,
            backoff: &mut backoff,
        };
        let context = sink_context();
        let client = ClientName::parse("client").expect("valid client name");
        let subject = SubjectName::parse("subject").expect("valid subject name");
        let sink_config = EmitSink::Nats { client, subject };
        let mut sink = SinkEmitter::Missing {
            reason: "test sink intentionally has no client".to_string(),
        };
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });

        let published = match sink
            .publish_batch(
                &sink_config,
                &context,
                &mut control,
                None,
                &mut buffer,
                EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100)),
            )
            .await
        {
            Ok(published) => published,
            Err(failure) => panic!(
                "a batch below the flush boundary must only be buffered: {}",
                emitter_error_message(&failure.error)
            ),
        };

        assert!(published.is_none());
        assert_eq!(buffer.pending.len(), 1);
        assert_eq!(buffer.pending[0].message_count(), 1);
    }

    #[test]
    fn emitter_error_classification_is_explicit_for_every_context() {
        for retryable in [
            EmitterRuntimeError::SinkNotInitialized,
            EmitterRuntimeError::PublishBatch,
            EmitterRuntimeError::PublishStalled,
        ] {
            assert!(retryable.is_retryable_publish_failure());
            assert!(emitter_publish_error_is_retryable(&Report::new(retryable)));
        }
        for terminal in [
            EmitterRuntimeError::InvalidSinkConfig,
            EmitterRuntimeError::InitializeSink,
            EmitterRuntimeError::FlushPolicyNotInitialized,
            EmitterRuntimeError::FaultInjected,
            EmitterRuntimeError::EncodeBatch,
        ] {
            assert!(!terminal.is_retryable_publish_failure());
            assert!(!emitter_publish_error_is_retryable(&Report::new(terminal)));
        }

        assert_eq!(
            emitter_message_error_operation(&Report::new(EmitterRuntimeError::EncodeBatch), true),
            MessageErrorOperation::Encode
        );
        assert_eq!(
            emitter_message_error_operation(&Report::new(EmitterRuntimeError::EncodeBatch), false),
            MessageErrorOperation::Values
        );
        assert_eq!(
            emitter_message_error_operation(&Report::new(EmitterRuntimeError::PublishBatch), true),
            MessageErrorOperation::Publish
        );
    }

    #[test]
    fn emitter_error_message_prefers_printable_context() {
        let attached = Report::new(EmitterRuntimeError::PublishBatch)
            .attach_printable("specific broker failure");
        assert_eq!(emitter_error_message(&attached), "specific broker failure");

        let bare = Report::new(EmitterRuntimeError::EncodeBatch);
        assert_eq!(
            emitter_error_message(&bare),
            "failed to encode emitter batch"
        );
    }

    #[test]
    fn runtime_values_convert_to_json_without_losing_exact_scalar_types() {
        let datetime =
            chrono::DateTime::parse_from_rfc3339("2026-08-04T12:34:56Z").expect("valid datetime");
        let cases = [
            (RuntimeValue::U8(1), serde_json::json!(1)),
            (RuntimeValue::I8(-2), serde_json::json!(-2)),
            (RuntimeValue::U16(3), serde_json::json!(3)),
            (RuntimeValue::I16(-4), serde_json::json!(-4)),
            (RuntimeValue::U32(5), serde_json::json!(5)),
            (RuntimeValue::I32(-6), serde_json::json!(-6)),
            (RuntimeValue::U64(7), serde_json::json!(7)),
            (RuntimeValue::I64(-8), serde_json::json!(-8)),
            (RuntimeValue::Bool(true), serde_json::json!(true)),
            (
                RuntimeValue::String("value".to_string()),
                serde_json::json!("value"),
            ),
            (
                RuntimeValue::Datetime(datetime),
                serde_json::json!("2026-08-04T12:34:56+00:00"),
            ),
            (
                RuntimeValue::F32(OrderedFloat(1.25)),
                serde_json::json!(1.25),
            ),
            (
                RuntimeValue::F64(OrderedFloat(-2.5)),
                serde_json::json!(-2.5),
            ),
            (
                RuntimeValue::Array(vec![RuntimeValue::I64(1)]),
                serde_json::json!([1]),
            ),
            (
                RuntimeValue::Vec(vec![RuntimeValue::String("x".to_string())]),
                serde_json::json!(["x"]),
            ),
        ];

        for (value, expected) in cases {
            assert_eq!(runtime_value_to_json(&value), expected);
        }
    }

    #[test]
    fn every_sink_has_a_stable_diagnostic_label() {
        let id = RelayName::parse("target").expect("valid relay name");
        let catalog = IcebergCatalog::Rest {
            client: ClientName::parse("target").expect("valid name"),
        };
        let sinks = vec![
            (
                EmitSink::Kafka {
                    client: ClientName::parse("target").expect("valid name"),
                    topic: TopicName::parse("target").expect("valid name"),
                },
                "kafka",
            ),
            (
                EmitSink::Pulsar {
                    client: ClientName::parse("target").expect("valid name"),
                    topic: TopicName::parse("target").expect("valid name"),
                },
                "pulsar",
            ),
            (
                EmitSink::RabbitMq {
                    client: ClientName::parse("target").expect("valid name"),
                    queue: QueueName::parse("target").expect("valid name"),
                },
                "rabbitmq",
            ),
            (
                EmitSink::Redis {
                    client: ClientName::parse("target").expect("valid name"),
                    channel: ChannelName::parse("target").expect("valid name"),
                },
                "redis",
            ),
            (
                EmitSink::Mqtt {
                    client: ClientName::parse("target").expect("valid name"),
                    topic: TopicName::parse("target").expect("valid name"),
                },
                "mqtt",
            ),
            (
                EmitSink::Nats {
                    client: ClientName::parse("target").expect("valid name"),
                    subject: SubjectName::parse("target").expect("valid name"),
                },
                "nats",
            ),
            (
                EmitSink::ZeroMq {
                    client: ClientName::parse("target").expect("valid name"),
                },
                "zeromq",
            ),
            (
                EmitSink::Syslog {
                    client: ClientName::parse("target").expect("valid name"),
                },
                "syslog",
            ),
            (
                EmitSink::Sqs {
                    client: ClientName::parse("target").expect("valid name"),
                    queue: id.as_str().to_string(),
                    fifo_group: None,
                },
                "sqs",
            ),
            (
                EmitSink::Sentry {
                    client: ClientName::parse("target").expect("valid name"),
                },
                "sentry",
            ),
            (
                EmitSink::Otel {
                    client: ClientName::parse("target").expect("valid name"),
                    signal: OtelSignal::Logs,
                    values: Vec::new(),
                    attributes: Vec::new(),
                    resource: Vec::new(),
                    scope: None,
                },
                "otel",
            ),
            (
                EmitSink::ClickHouse {
                    client: ClientName::parse("target").expect("valid name"),
                    table: TableName::parse("target").expect("valid name"),
                    values: Vec::new(),
                    max_batch: nonzero!(1u64),
                },
                "clickhouse",
            ),
            (
                EmitSink::Postgres {
                    client: ClientName::parse("target").expect("valid name"),
                    table: TableName::parse("target").expect("valid name"),
                    values: Vec::new(),
                    conflict_action: PostgresConflictAction::None,
                    max_batch: nonzero!(1u64),
                },
                "postgres",
            ),
            (
                EmitSink::MySql {
                    client: ClientName::parse("target").expect("valid name"),
                    table: TableName::parse("target").expect("valid name"),
                    values: Vec::new(),
                    conflict_action: MySqlConflictAction::None,
                    max_batch: nonzero!(1u64),
                },
                "mysql",
            ),
            (
                EmitSink::MongoDb {
                    client: ClientName::parse("target").expect("valid name"),
                    collection: CollectionName::parse("target").expect("valid name"),
                    values: Vec::new(),
                    conflict_action: MongoDbConflictAction::None,
                    max_batch: nonzero!(1u64),
                },
                "mongodb",
            ),
            (
                EmitSink::Iceberg {
                    backend: IcebergStorageBackend::S3,
                    client: ClientName::parse("target").expect("valid name"),
                    table: TableName::parse("target").expect("valid name"),
                    values: Vec::new(),
                    location: "s3://bucket/table".to_string(),
                    catalog,
                    commit_each: "1s".to_string(),
                    max_commit_size: "1MiB".to_string(),
                },
                "iceberg",
            ),
        ];

        for (sink, expected) in sinks {
            assert_eq!(sink.label(), expected);
        }
    }

    #[test]
    fn sink_context_reports_configuration_and_publish_failures() {
        let context = sink_context();
        let mut events = context.runtime.events().subscribe();

        context.report_init_error("nats", "init failed");
        context.report_publish_error("nats", "publish failed");
        context.report_flush_error("nats", "flush failed");
        assert!(
            context
                .parse_flush_policy(
                    "emitter",
                    &FlushPolicy::Each {
                        interval: "not-a-duration".to_string(),
                        max_batch_size: "1MiB".to_string()
                    }
                )
                .is_none()
        );

        let messages = (0..4)
            .map(|_| {
                let RuntimeEvent::Error(message) =
                    events.try_recv().expect("error event must be emitted");
                message
            })
            .collect::<Vec<_>>();
        assert!(messages[0].contains("failed to initialize nats emitter"));
        assert!(messages[1].contains("failed to publish nats message"));
        assert!(messages[2].contains("failed to flush nats rows"));
        assert!(messages[3].contains("invalid flush_each 'not-a-duration'"));
    }

    #[test]
    fn sql_value_compilers_reject_empty_mappings_before_compilation() {
        let domain = DomainName::parse("emitter_tests").expect("valid domain");
        let emitter = EmitterName::parse("output").expect("valid emitter name");
        let schema = input_schema().arrow_schema();

        let errors = [
            compile_clickhouse_values_program(&domain, &emitter, &[], schema.clone(), None),
            compile_postgres_values_program(&domain, &emitter, &[], schema.clone(), None),
            compile_mysql_values_program(&domain, &emitter, &[], schema.clone(), None),
            compile_mongodb_values_program(&domain, &emitter, &[], schema.clone(), None),
            compile_iceberg_values_program(&domain, &emitter, &[], schema, None),
        ];
        for result in errors {
            let Err(error) = result else {
                panic!("empty VALUES mappings must fail before compilation")
            };
            assert!(
                error
                    .to_string()
                    .contains("requires at least one VALUES mapping")
            );
        }
    }
}
