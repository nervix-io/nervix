//! External emitter execution.
//!
//! Layer: data plane.
//! - **Owns.** Emitter task lifecycle, batching, retries, acknowledgements and connector dispatch.
//! - **Depends on.** Execution plans, Arrow batches, bound clocks and connector implementations.
//! - **Must not know.** NSPL parsing, registry validation or placement policy.

use error_stack::{AttachmentKind, FrameKind, Report, ResultExt as _};
use nervix_connector::{
    MappedSinkRows, PerRecordOutcome, RecordSink, RowSink, SinkAcknowledgementServices,
    SinkAcknowledgements, SinkCommitReport, SinkDeadline, SinkEventReporter,
    SinkGeneralErrorHandler, SinkHost, SinkPublishError, SinkRecord, SinkRecordPosition,
    SinkRetryDelay, SinkStagingDirectory, SinkStartError, SinkTransientErrorStatus,
    physical_time::{PhysicalDeadline, PhysicalDeadlineCapability, actual_utc_now},
};
use nervix_connector_clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
use nervix_connector_kafka::{KafkaSink, KafkaSinkConfig};
use nervix_connector_mongodb::{MongoDbSink, MongoDbSinkConfig};
use nervix_connector_mqtt::{MqttSink, MqttSinkConfig};
use nervix_connector_mysql::{MySqlSink, MySqlSinkConfig};
use nervix_connector_nats::{NatsSink, NatsSinkConfig};
use nervix_connector_otel::{OtelLiteral, OtelResourceAttribute, OtelSink, OtelSinkConfig};
use nervix_connector_postgres::{PostgresSink, PostgresSinkConfig};
use nervix_connector_pulsar::{PulsarSink, PulsarSinkConfig};
use nervix_connector_rabbitmq::{RabbitMqSink, RabbitMqSinkConfig};
pub(in crate::runtime) use nervix_connector_redis::{
    RedisClientError, RedisCommandPool, open_redis_command_pool,
};
use nervix_connector_redis::{
    RedisPoolHandle, RedisPoolServices, RedisPoolWait, RedisSink, RedisSinkConfig,
};
use nervix_connector_sentry::{SentrySink, SentrySinkConfig};
use nervix_connector_sqs::{SqsSink, SqsSinkConfig};
use nervix_connector_syslog::{SyslogSink, SyslogSinkConfig};
use nervix_connector_zeromq::{ZeroMqSink, ZeroMqSinkConfig};
use thiserror::Error;

use super::*;

mod iceberg;
mod mapped_values;
mod pooled_clients;

use iceberg::{IcebergCommitPolicy, IcebergSink, IcebergSinkConfig};
use mapped_values::{MappedValuesProjection, MappedValuesProjectionInit};
use pooled_clients::PooledSinkClient;

const RETRY_ACK_ALIVE_EACH: Duration = Duration::from_millis(100);

pub(in crate::runtime) struct EmitterTask;

/// What one emitter still holds: the batches its own buffer collected, and the rows a sink that
/// publishes on its commit boundary staged out of that buffer.
#[derive(Debug)]
struct EmitterBufferedMessages {
    reported: Arc<AtomicUsize>,
    buffered: AtomicUsize,
    staged: AtomicUsize,
}

impl EmitterBufferedMessages {
    fn new(reported: Arc<AtomicUsize>) -> Self {
        Self {
            reported,
            buffered: AtomicUsize::new(0),
            staged: AtomicUsize::new(0),
        }
    }

    fn set_buffered(&self, messages: usize) {
        self.buffered.store(messages, Ordering::Release);
        self.report_total();
    }

    fn set_staged(&self, messages: usize) {
        self.staged.store(messages, Ordering::Release);
        self.report_total();
    }

    fn report_total(&self) {
        self.reported.store(
            self.buffered
                .load(Ordering::Acquire)
                .checked_add(self.staged.load(Ordering::Acquire))
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

#[derive(Debug, Clone)]
enum CompiledSqsFifoGroup {
    FromBranch,
    Expression(CompiledProgramWithMaterializedInterest),
}

struct EmitterBatchContext<'a> {
    runtime: &'a Runtime,
    routing: &'a mut DomainRoutingCache,
    /// Bound once with the task, so resolving a batch's materialized dependencies never re-binds
    /// the domain's execution.
    domain_clock: &'a DomainClock,
    domain: &'a DomainName,
    emitter: &'a EmitterName,
    node: &'a ModelName,
    output_metrics: &'a EmitterOutputMetrics,
    error_policies: &'a ErrorPolicies,
    source_filters: &'a HashMap<RelayName, CompiledProgramWithMaterializedInterest>,
    filter_map: Option<&'a CompiledEmitterFilterMapProgram>,
    sqs_fifo_group: Option<&'a CompiledSqsFifoGroup>,
    materialized_state: &'a [nervix_models::MaterializedStateDependency],
}

enum EmitterOutputMetrics {
    Relay(BatchMetricsHandle),
    WithoutRelay(MessageMetricsHandle),
}

impl EmitterOutputMetrics {
    fn observe(&self, report: &PublishReport) {
        match self {
            Self::Relay(metrics) => {
                metrics.observe(report.messages, report.bytes, Some(report.domain_timestamp))
            }
            Self::WithoutRelay(metrics) => {
                metrics.observe(report.messages, report.bytes, Some(report.domain_timestamp))
            }
        }
    }
}

/// A source batch whose node-wide materialized dependencies are resolved.
///
/// The dependencies are read once for the batch, so the snapshot and the execution time travel
/// with it and every emitter program for it reads exactly the same state.
struct ResolvedEmitterInput {
    batch: RelayRecordBatch,
    materialized_values: HashMap<String, RuntimeValue>,
    execution_now: Timestamp,
}

#[derive(Clone)]
struct EmitterPublishBatch {
    batch: RelayRecordBatch,
    execution_now: Timestamp,
    headers: Option<Vec<EmitterHeaders>>,
    sqs_message_groups: Vec<Result<Option<String>, SqsMessageGroupError>>,
    delivered: Vec<bool>,
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
    ) -> EmitterRuntimeResult<Self> {
        let row_count = batch.batch.batch().num_rows();
        if let Some(headers) = &headers
            && row_count != headers.len()
        {
            return Err(Report::new(EmitterRuntimeError::HeaderCountMismatch {
                header_count: headers.len(),
                row_count,
            }));
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
        groups: Vec<Result<Option<String>, SqsMessageGroupError>>,
    ) -> EmitterRuntimeResult<Self> {
        let row_count = self.batch.batch.batch().num_rows();
        if groups.len() != row_count {
            return Err(Report::new(EmitterRuntimeError::SqsGroupCountMismatch {
                group_count: groups.len(),
                row_count,
            }));
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
                        Ok(Some(group)) => group.len().arch_into(),
                        Ok(None) | Err(_) => 0,
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

    fn mark_delivered(
        &mut self,
        row: usize,
        acknowledgements: DeliveredAcknowledgements,
    ) -> EmitterRuntimeResult<()> {
        let delivered_rows = self.delivered.len();
        let delivered = self.delivered.get_mut(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::DeliveryRowOutOfBounds {
                row,
                row_count: delivered_rows,
            })
        })?;
        if !*delivered {
            let ack_rows = self.batch.acks.len();
            let acks = self.batch.acks.get(row).ok_or_else(|| {
                Report::new(EmitterRuntimeError::AcknowledgementRowOutOfBounds {
                    row,
                    row_count: ack_rows,
                })
            })?;
            match acknowledgements {
                DeliveredAcknowledgements::Host => acks.ack_success(),
                DeliveredAcknowledgements::Sink => {}
            }
            *delivered = true;
        }
        Ok(())
    }

    fn mark_rejected(&mut self, row: usize) -> EmitterRuntimeResult<()> {
        let delivered_rows = self.delivered.len();
        let delivered = self.delivered.get_mut(row).ok_or_else(|| {
            Report::new(EmitterRuntimeError::RejectionRowOutOfBounds {
                row,
                row_count: delivered_rows,
            })
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
        self.mark_rejected(row)
    }

    fn pending_record_rows(&self) -> Vec<usize> {
        (0..self.batch.batch.batch().num_rows())
            .filter(|row| !self.delivered.get(*row).copied().unwrap_or(false))
            .collect()
    }

    /// The acknowledgements of `rows`, for a sink that takes them with the write.
    fn acks_for_rows(&self, rows: &[usize]) -> AckSet {
        AckSet::merged(
            rows.iter()
                .filter_map(|row| self.batch.acks.get(*row).cloned()),
        )
    }
}

/// Why the host is asking a sink to publish what it staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkCommitReason {
    /// Only the sink's own commit deadline releases what it staged.
    Cadence,
    /// A retry publishes everything staged, and a shutdown that cuts its backoff short leaves the
    /// rest for the attempt after it.
    Retry,
    /// A drain publishes everything staged, and a shutdown that cuts its backoff short fails it.
    Drain,
}

impl SinkCommitReason {
    fn forces_commit(self) -> bool {
        match self {
            Self::Cadence => false,
            Self::Retry | Self::Drain => true,
        }
    }

    /// This reason once an attempt has already started committing, which every further attempt
    /// finishes whatever the cadence says.
    fn forced(self) -> Self {
        match self {
            Self::Cadence => Self::Retry,
            Self::Retry => Self::Retry,
            Self::Drain => Self::Drain,
        }
    }
}

/// Who resolves the acknowledgements of the rows one write delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveredAcknowledgements {
    /// The host resolves them as the write returns.
    Host,
    /// The sink took them with the rows and resolves them when its own commit succeeds.
    Sink,
}

#[derive(Debug)]
struct EncodedBrokerRecord {
    batch_index: usize,
    row_index: usize,
    key: Option<String>,
    payload: Vec<u8>,
    headers: EmitterHeaders,
    message_group: Result<Option<String>, SqsMessageGroupError>,
    execution_now: Timestamp,
}

impl EncodedBrokerRecord {
    const fn position(&self) -> SinkRecordPosition {
        SinkRecordPosition {
            batch_index: self.batch_index,
            row_index: self.row_index,
        }
    }
}

pub(super) struct RejectedEmitterRecord {
    pub(super) position: SinkRecordPosition,
    pub(super) reason: String,
    pub(super) structured_error: Option<StructuredMessageError>,
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(in crate::runtime) enum EmitterRuntimeError {
    #[error("invalid emitter sink configuration")]
    InvalidSinkConfig,
    #[error("failed to initialize emitter sink")]
    InitializeSink,
    #[error("emitter sink client is not initialized")]
    SinkNotInitialized,
    #[error("emitter flush policy is not initialized")]
    FlushPolicyNotInitialized,
    #[error("emitter header count {header_count} does not match row count {row_count}")]
    HeaderCountMismatch {
        header_count: usize,
        row_count: usize,
    },
    #[error("SQS FIFO group count {group_count} does not match emitter row count {row_count}")]
    SqsGroupCountMismatch {
        group_count: usize,
        row_count: usize,
    },
    #[error("emitter delivered row {row} is outside batch with {row_count} rows")]
    DeliveryRowOutOfBounds { row: usize, row_count: usize },
    #[error("emitter delivered row {row} is outside ack set with {row_count} rows")]
    AcknowledgementRowOutOfBounds { row: usize, row_count: usize },
    #[error("emitter rejected row {row} is outside batch with {row_count} rows")]
    RejectionRowOutOfBounds { row: usize, row_count: usize },
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
    #[error("emitter final flush failed")]
    FinalFlush,
    #[error("OTEL RESOURCE attribute '{attribute}' must be a literal value or a literal array")]
    InvalidOtelResource { attribute: String },
    #[error("failed to encode emitter batch")]
    EncodeBatch,
    #[error("failed to publish emitter batch")]
    PublishBatch,
    #[error("emitter publish is stalled")]
    PublishStalled,
}

impl EmitterRuntimeError {
    fn is_retryable_publish_failure(&self) -> bool {
        match self {
            Self::SinkNotInitialized | Self::PublishBatch | Self::PublishStalled => true,
            Self::FlushPolicyNotInitialized
            | Self::HeaderCountMismatch { .. }
            | Self::SqsGroupCountMismatch { .. }
            | Self::DeliveryRowOutOfBounds { .. }
            | Self::AcknowledgementRowOutOfBounds { .. }
            | Self::RejectionRowOutOfBounds { .. }
            | Self::InvalidSinkConfig
            | Self::InvalidOtelResource { .. }
            | Self::InitializeSink
            | Self::FaultInjected
            | Self::ShutdownWhileStalled
            | Self::StopDeadlineElapsed
            | Self::FinalFlush
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
            .set_buffered(self.pending_messages.arch_into());
    }

    /// Publishes what a sink that commits separately still holds, which a drain reads together
    /// with this buffer's own pending batches.
    fn report_staged_messages(&self, messages: u64) {
        self.buffered_messages.set_staged(messages.arch_into());
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

    /// Starts the cadence for a buffer that just stopped being empty.
    ///
    /// An armed cadence is left alone, so a later batch joining the same buffer neither brings the
    /// deadline forward nor pays for a clock read.
    fn arm_cadence(
        &mut self,
        context: &EmitterSinkContext,
        flush_policy: RuntimeFlushPolicy,
    ) -> EmitterRuntimeResult<()> {
        if self.cadence.is_armed() {
            return Ok(());
        }
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
        self.retain(batch);
        self.arm_cadence(context, flush_policy)?;
        Ok(flush_policy.size_boundary_reached(self.pending_bytes))
    }

    /// Retains a batch whose release the caller already owns.
    ///
    /// A drain publishes everything the emitter holds, and a batch put back after a failed
    /// transfer is released by the retry that failure schedules. Neither needs a cadence deadline,
    /// and leaving the clock out keeps both paths working for a domain that has already
    /// uninstalled it.
    fn retain_without_cadence(&mut self, batch: EmitterPublishBatch) -> EmitterRuntimeResult<()> {
        if self.flush_policy.is_none() {
            return Err(Report::new(EmitterRuntimeError::FlushPolicyNotInitialized));
        }
        self.retain(batch);
        Ok(())
    }

    fn retain(&mut self, batch: EmitterPublishBatch) {
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
        self.buffered_messages.set_buffered(0);
    }
}

/// One deferral of an emitter's publish attempt: how long the emitter waits, which
/// acknowledgements it keeps alive meanwhile, and what the operator is told about the wait.
struct EmitterRetryDeferral<'a> {
    wait: Duration,
    acks: EmitterAcknowledgements,
    waiting_for_stall_clear: bool,
    reason: Option<&'a str>,
}

#[derive(Default)]
struct EmitterAcknowledgements {
    runtime: AckSet,
    sink: Option<SinkAcknowledgements>,
}

impl EmitterAcknowledgements {
    fn is_empty(&self) -> bool {
        self.runtime.is_empty()
            && self
                .sink
                .as_ref()
                .is_none_or(SinkAcknowledgements::is_empty)
    }

    fn extend(&mut self, further: Self) {
        self.runtime = AckSet::merged([std::mem::take(&mut self.runtime), further.runtime]);
        if further.sink.is_some() {
            self.sink = further.sink;
        }
    }
}

impl From<AckSet> for EmitterAcknowledgements {
    fn from(runtime: AckSet) -> Self {
        Self {
            runtime,
            sink: None,
        }
    }
}

impl AcknowledgementKeepalive for EmitterAcknowledgements {
    fn keep_alive(&self) {
        self.runtime.ack_alive();
        if let Some(acks) = &self.sink {
            acks.keep_alive();
        }
    }
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
    acks: EmitterAcknowledgements,
    waiting_for_stall_clear: bool,
}

impl EmitterRetrySchedule {
    fn is_active(&self) -> bool {
        self.retry_at.is_some()
    }

    fn schedule(
        &mut self,
        delay: Duration,
        acks: impl Into<EmitterAcknowledgements>,
        waiting_for_stall_clear: bool,
    ) -> EmitterRuntimeResult<()> {
        let acks = acks.into();
        let retry_at = PhysicalDeadlineCapability::operational()
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

    fn include_acks(&mut self, acks: impl Into<EmitterAcknowledgements>) {
        let acks = acks.into();
        if acks.is_empty() {
            return;
        }
        self.acks.extend(acks);
        if let Some(retry_at) = self.retry_at
            && self.ack_alive_at.is_none()
        {
            self.ack_alive_at = Some(Self::next_ack_alive_at(retry_at));
        }
    }

    fn next_ack_alive_at(retry_at: PhysicalDeadline) -> PhysicalDeadline {
        let keepalive = PhysicalDeadlineCapability::operational()
            .after(RETRY_ACK_ALIVE_EACH)
            .assured("the acknowledgement keepalive interval is a fixed hundred milliseconds");
        // The last keepalive of a wait lands on the retry itself rather than after it.
        keepalive.min(retry_at)
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
        let physical_time = PhysicalDeadlineCapability::operational();
        if physical_time.is_reached(retry_at) {
            self.retry_at = None;
            self.ack_alive_at = None;
            return true;
        }
        if self
            .ack_alive_at
            .is_some_and(|ack_alive_at| physical_time.is_reached(ack_alive_at))
        {
            self.acks.keep_alive();
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
        self.acks = EmitterAcknowledgements::default();
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
    let mut assignments = Vec::with_capacity(values.len());
    for (index, mapping) in values.iter().enumerate() {
        let field = FieldName::parse(&format!("c{index}")).assured(
            "a generated name containing c followed by decimal digits is a valid field name",
        );
        assignments.push(nervix_models::Assignment {
            target: nervix_models::AssignmentTarget::bare(field),
            value: mapping.expression.clone(),
        });
    }
    let parsed = lower_route_construction(
        &nervix_models::RouteConstruction {
            assignments,
            ..nervix_models::RouteConstruction::default()
        },
        nervix_vm::SemanticScopePolicy::read_write("input", namespace),
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

fn mapped_column_names(mappings: &[ClickHouseValueMapping]) -> Vec<String> {
    mappings
        .iter()
        .map(|mapping| mapping.column.clone())
        .collect()
}

/// The `RESOURCE` attributes an OTEL emitter exports with, which are fixed for its lifetime.
///
/// A resource value describes the emitting service rather than a record, so only a literal or a
/// literal array can supply one.
fn otel_resource_attributes(
    resource: &[OtelValueMapping],
) -> EmitterRuntimeResult<Vec<OtelResourceAttribute>> {
    let mut attributes = Vec::with_capacity(resource.len());
    for mapping in resource {
        let value = otel_literal(&mapping.expression).ok_or_else(|| {
            Report::new(EmitterRuntimeError::InvalidOtelResource {
                attribute: mapping.column.clone(),
            })
        })?;
        attributes.push(OtelResourceAttribute {
            key: mapping.column.clone(),
            value,
        });
    }
    Ok(attributes)
}

/// The literal a `RESOURCE` value carries, or nothing for an expression that reads a record.
fn otel_literal(expression: &nervix_models::Expression) -> Option<OtelLiteral> {
    match expression {
        nervix_models::Expression::Literal(ModelLiteral::I64(value)) => {
            Some(OtelLiteral::I64(*value))
        }
        nervix_models::Expression::Literal(ModelLiteral::F64(value)) => {
            Some(OtelLiteral::F64(value.value()))
        }
        nervix_models::Expression::Literal(ModelLiteral::Bool(value)) => {
            Some(OtelLiteral::Bool(*value))
        }
        nervix_models::Expression::Literal(ModelLiteral::String(value)) => {
            Some(OtelLiteral::String(value.clone()))
        }
        nervix_models::Expression::Literal(ModelLiteral::Null) => Some(OtelLiteral::Null),
        nervix_models::Expression::Array(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(otel_literal(item)?);
            }
            Some(OtelLiteral::Array(values))
        }
        _ => None,
    }
}

fn emitter_report(
    context: EmitterRuntimeError,
    error: impl std::fmt::Display,
) -> Report<EmitterRuntimeError> {
    Report::new(context).attach_printable(error.to_string())
}

fn emitter_init_error(error: impl std::fmt::Display) -> Report<EmitterRuntimeError> {
    emitter_report(EmitterRuntimeError::InitializeSink, error)
}

/// How long the sink asked this emitter to wait, which bounds its next attempt from below.
fn emitter_minimum_retry_delay(error: &Report<EmitterRuntimeError>) -> Duration {
    match error.downcast_ref::<SinkRetryDelay>() {
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

async fn await_emitter_confirmation<F>(acks: &impl AcknowledgementKeepalive, future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::pin!(future);
    loop {
        tokio::task::consume_budget().await;
        acks.keep_alive();
        tokio::select! {
            result = &mut future => return result,
            _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => {}
        }
    }
}

impl EmitterSinkContext {
    fn sink_host(&self) -> SinkHost {
        SinkHost::new(self.clone())
    }

    /// Leases this node's shared Redis command pool for `plan`'s client.
    ///
    /// The lease travels with the handle the sink holds, so the pool stays open for exactly as
    /// long as the sink publishes through it.
    async fn lease_redis_pool(
        &self,
        plan: &RedisSinkPlan,
    ) -> EmitterRuntimeResult<RedisPoolHandle> {
        let lease = self
            .runtime
            .lease_shared_client(&self.domain, &plan.client, plan.pooled_client())
            .await
            .map_err(|error| emitter_init_error(error.to_string()))?;
        let pool = lease
            .client()
            .redis(&plan.client.name)
            .map_err(|error| emitter_init_error(error.to_string()))?
            .clone();
        Ok(RedisPoolHandle::new(LeasedRedisPool {
            _lease: lease,
            pool,
            runtime: self.runtime.clone(),
            client: plan.client.name.clone(),
            waiter: DomainNodeRef::node_in(
                self.domain.clone(),
                ModelKind::Emitter,
                self.emitter.clone(),
            ),
        }))
    }

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

    /// The commit cadence and maximum commit size a sink that publishes on its own commit
    /// boundary was declared with, resolved here so the connector receives typed policy.
    fn parse_commit_policy(
        &self,
        kind: &str,
        commit_each: &str,
        max_commit_size: &str,
    ) -> EmitterRuntimeResult<IcebergCommitPolicy> {
        let interval = Runtime::parse_runtime_node_duration_setting(
            &self.domain,
            kind,
            &self.emitter,
            "commit_each",
            commit_each,
        )
        .map_err(|error| emitter_report(EmitterRuntimeError::InvalidSinkConfig, error))?;
        let max_size = max_commit_size
            .parse::<ubyte::ByteUnit>()
            .map_err(|error| {
                Report::new(EmitterRuntimeError::InvalidSinkConfig)
                    .attach_printable(format!("max_commit_size '{max_commit_size}': {error}"))
            })?
            .as_u64();
        Ok(IcebergCommitPolicy { interval, max_size })
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

/// One emitter's interest in this node's shared Redis pool, and the wait it records while that
/// pool hands a connection over.
struct LeasedRedisPool {
    /// Held for as long as the sink publishes: releasing it gives back this emitter's interest in
    /// the shared client, which closes the pool once its last local user leaves.
    _lease: SharedClientLease,
    pool: RedisCommandPool,
    runtime: Runtime,
    /// The client borrowed from, named in this emitter's pool wait.
    client: ClientName,
    /// This emitter, as the key its pool wait is recorded under for `DESCRIBE` to read.
    waiter: DomainNodeRef,
}

impl RedisPoolServices for LeasedRedisPool {
    fn pool(&self) -> RedisCommandPool {
        self.pool.clone()
    }

    fn begin_pool_wait(&self) -> RedisPoolWait {
        RedisPoolWait::new(self.runtime.pool_wait_guard(&self.waiter, &self.client))
    }
}

impl SinkAcknowledgementServices for AckSet {
    fn acknowledge(&self) {
        self.ack_success();
    }

    fn keep_alive(&self) {
        self.ack_alive();
    }

    fn reject(&self, reason: String) {
        self.no_ack(reason);
    }

    fn is_empty(&self) -> bool {
        AckSet::is_empty(self)
    }
}

impl SinkTransientErrorStatus for EmitterSinkContext {
    fn record_transient_error(&self, reason: String, retry_after: Duration) {
        self.runtime.record_emitter_transient_error_with_backoff(
            &self.domain,
            &self.emitter,
            reason,
            retry_after,
        );
    }

    fn clear_transient_error(&self) {
        self.runtime
            .clear_emitter_transient_error(&self.domain, &self.emitter);
    }
}

impl SinkEventReporter for EmitterSinkContext {
    fn report_error(&self, message: String) {
        self.runtime.events().report_error(format!(
            "sink error for emitter '{}' in domain '{}': {message}",
            self.emitter.as_str(),
            self.domain.as_str(),
        ));
    }
}

impl SinkStagingDirectory for EmitterSinkContext {
    fn staging_directory(&self) -> PathBuf {
        self.runtime.temp_dir().to_path_buf()
    }
}

impl SinkGeneralErrorHandler for EmitterSinkContext {
    fn handle_general_error(&self, acks: &SinkAcknowledgements, reason: String) {
        match self.error_policies.general {
            GeneralErrorPolicy::Ignore => acks.acknowledge(),
            GeneralErrorPolicy::Log => {
                self.runtime.events().report_error(format!(
                    "emitter '{}' general error in domain '{}': {}",
                    self.emitter.as_str(),
                    self.domain.as_str(),
                    reason
                ));
                warn!(
                    domain = self.domain.as_str(),
                    emitter = self.emitter.as_str(),
                    reason = %reason,
                    "runtime node handled general error"
                );
                acks.reject(reason);
            }
        }
    }
}

enum SinkEmitter {
    Record(Box<dyn RecordSink>),
    Row(RowSinkEmitter),
    Missing { reason: String },
}

impl From<Box<dyn RecordSink>> for SinkEmitter {
    fn from(sink: Box<dyn RecordSink>) -> Self {
        Self::Record(sink)
    }
}

impl From<RowSinkEmitter> for SinkEmitter {
    fn from(sink: RowSinkEmitter) -> Self {
        Self::Row(sink)
    }
}

/// A row sink and the host projection whose mapped columns it writes.
///
/// The mapping is evaluated here, once per batch, so the sink receives Arrow columns and the rows
/// it must write and never learns what produced them.
struct RowSinkEmitter {
    sink: Box<dyn RowSink>,
    projection: MappedValuesProjection,
}

impl RowSinkEmitter {
    /// Writes every buffered batch, one projection and one virtual call per batch.
    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()> {
        let acknowledgements = match self.sink.retains_acknowledgements() {
            true => DeliveredAcknowledgements::Sink,
            false => DeliveredAcknowledgements::Host,
        };
        for batch_index in 0..batches.len() {
            tokio::task::consume_budget().await;
            let mut projected = {
                let batch = &batches[batch_index];
                let pending_rows = batch.pending_record_rows();
                // A batch whose rows a previous attempt already delivered has nothing left to map.
                if pending_rows.is_empty() {
                    continue;
                }
                self.projection
                    .project(
                        batch_index,
                        &batch.batch,
                        batch.execution_now,
                        &pending_rows,
                    )
                    .await?
            };
            let rejected = projected.take_rejected();
            finish_rejected_records(context, batches, rejected, MessageErrorOperation::Values)
                .await?;
            if projected.is_empty() {
                continue;
            }
            // A sink that resolves acknowledgements on its own commit boundary takes the ones its
            // write carries, so the host stops owning them the moment the write accepts the rows.
            let retained = match acknowledgements {
                DeliveredAcknowledgements::Sink => {
                    let batch = &batches[batch_index];
                    Some(SinkAcknowledgements::new(
                        batch.acks_for_rows(projected.selected_rows()),
                    ))
                }
                DeliveredAcknowledgements::Host => None,
            };
            let outcome = self.sink.publish(projected.rows(retained)).await;
            finish_record_sink_publish(context, batches, outcome, acknowledgements).await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct SinkEmitterRuntime {
    input_schema: Arc<CompiledSchema>,
}

struct SinkEmitterInit<'a> {
    plan: &'a EmitterStartPlan,
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
            plan,
            context,
            runtime,
        } = init;
        let SinkEmitterRuntime { input_schema } = runtime;
        match &plan.sink {
            EmitterSinkPlan::Kafka(sink) => Self::from_record_sink_result(
                "kafka",
                context,
                KafkaSink::new(
                    KafkaSinkConfig {
                        config: sink.client.config.entries.clone(),
                        topic: sink.topic.clone(),
                        mode: sink.mode,
                    },
                    context.sink_host(),
                ),
            ),
            EmitterSinkPlan::Pulsar(sink) => Self::from_record_sink_result(
                "pulsar",
                context,
                PulsarSink::new(
                    PulsarSinkConfig {
                        config: sink.client.config.entries.clone(),
                        topic: sink.topic.clone(),
                        mode: sink.mode,
                    },
                    context.sink_host(),
                )
                .await,
            ),
            EmitterSinkPlan::RabbitMq(sink) => Self::from_record_sink_result(
                "rabbitmq",
                context,
                RabbitMqSink::new(
                    RabbitMqSinkConfig {
                        config: sink.client.config.entries.clone(),
                        queue: sink.queue.clone(),
                        mode: sink.mode,
                    },
                    context.sink_host(),
                )
                .await,
            ),
            EmitterSinkPlan::Redis(sink) => match context.lease_redis_pool(sink).await {
                Ok(pool) => Self::from_record_sink_result(
                    "redis",
                    context,
                    RedisSink::new(
                        RedisSinkConfig {
                            pool,
                            channel: sink.channel.clone(),
                        },
                        context.sink_host(),
                    ),
                ),
                Err(error) => Self::missing_after_emitter_init_error("redis", context, &error),
            },
            EmitterSinkPlan::Mqtt(sink) => Self::from_record_sink_result(
                "mqtt",
                context,
                MqttSink::new(
                    MqttSinkConfig {
                        config: sink.client.config.entries.clone(),
                        topic: sink.topic.clone(),
                        mode: sink.mode,
                        retry_policy: plan.retry_policy,
                        default_client_id: format!(
                            "{}-{}",
                            context.domain.as_str(),
                            context.emitter.as_str()
                        ),
                    },
                    context.sink_host(),
                ),
            ),
            EmitterSinkPlan::Nats(sink) => Self::from_record_sink_result(
                "nats",
                context,
                NatsSink::new(
                    NatsSinkConfig {
                        config: sink.client.config.entries.clone(),
                        subject: sink.subject.clone(),
                        mode: sink.mode,
                        retry_policy: plan.retry_policy,
                    },
                    context.sink_host(),
                )
                .await,
            ),
            EmitterSinkPlan::ZeroMq(sink) => Self::from_record_sink_result(
                "zeromq",
                context,
                ZeroMqSink::new(
                    ZeroMqSinkConfig {
                        config: sink.client.config.entries.clone(),
                    },
                    context.sink_host(),
                )
                .await,
            ),
            EmitterSinkPlan::Syslog(sink) => Self::from_record_sink_result(
                "syslog",
                context,
                SyslogSink::new(
                    SyslogSinkConfig {
                        config: sink.client.config.entries.clone(),
                    },
                    context.sink_host(),
                )
                .await,
            ),
            EmitterSinkPlan::Sqs(sink) => Self::from_record_sink_result(
                "sqs",
                context,
                SqsSink::new(
                    SqsSinkConfig {
                        config: sink.client.config.entries.clone(),
                        queue: sink.queue.clone(),
                        mode: sink.mode,
                    },
                    context.sink_host(),
                )
                .await,
            ),
            EmitterSinkPlan::Sentry(sink) => Self::from_record_sink_result(
                "sentry",
                context,
                SentrySink::new(
                    SentrySinkConfig {
                        config: sink.client.config.entries.clone(),
                    },
                    context.sink_host(),
                ),
            ),
            EmitterSinkPlan::Otel(sink) => {
                // The signal's own values and its attributes are mapped as one program, so the
                // attribute columns follow the signal's own in the batch the host projects.
                let mut mappings = Vec::with_capacity(
                    sink.values
                        .len()
                        .checked_add(sink.attributes.len())
                        .assured("an emitter maps fewer columns than usize can count"),
                );
                mappings.extend_from_slice(&sink.values);
                mappings.extend_from_slice(&sink.attributes);
                match Self::values_projection(
                    context,
                    MappedValuesProjectionInit {
                        label: "OTEL",
                        namespace: "otel",
                        domain: &context.domain,
                        emitter: &context.emitter,
                        values: &mappings,
                        input_schema: input_schema.arrow_schema(),
                        udfs: context.udfs.as_ref(),
                        max_batch: None,
                    },
                ) {
                    SinkEmitterResult::Missing { reason } => Self::Missing { reason },
                    SinkEmitterResult::Ready(projection) => {
                        let resource = match otel_resource_attributes(&sink.resource) {
                            Ok(resource) => resource,
                            Err(error) => {
                                let reason = error.current_context().to_string();
                                context.report_init_error("otel", &reason);
                                return Self::Missing { reason };
                            }
                        };
                        let config = OtelSinkConfig {
                            config: sink.client.config.entries.clone(),
                            signal: sink.signal.clone(),
                            values: mapped_column_names(&sink.values),
                            attributes: mapped_column_names(&sink.attributes),
                            resource,
                            scope: sink.scope.clone(),
                            mapped_schema: projection.mapped_schema().clone(),
                        };
                        Self::from_row_sink_result(
                            "otel",
                            context,
                            projection,
                            OtelSink::new(config, context.sink_host()),
                        )
                    }
                }
            }
            EmitterSinkPlan::ClickHouse(sink) => Self::values_projection(
                context,
                MappedValuesProjectionInit {
                    label: "ClickHouse",
                    namespace: "clickhouse",
                    domain: &context.domain,
                    emitter: &context.emitter,
                    values: &sink.values,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                    max_batch: Some(sink.max_batch),
                },
            )
            .map(|projection| {
                Self::from_row_sink_result(
                    "clickhouse",
                    context,
                    projection,
                    ClickHouseSink::new(
                        ClickHouseSinkConfig {
                            config: sink.client.config.entries.clone(),
                            table: sink.table.clone(),
                        },
                        context.sink_host(),
                    ),
                )
            }),
            EmitterSinkPlan::Postgres(sink) => {
                match Self::values_projection(
                    context,
                    MappedValuesProjectionInit {
                        label: "Postgres",
                        namespace: "postgres",
                        domain: &context.domain,
                        emitter: &context.emitter,
                        values: &sink.values,
                        input_schema: input_schema.arrow_schema(),
                        udfs: context.udfs.as_ref(),
                        max_batch: Some(sink.max_batch),
                    },
                ) {
                    SinkEmitterResult::Missing { reason } => Self::Missing { reason },
                    SinkEmitterResult::Ready(projection) => {
                        let connections = match PooledSinkClient::lease(
                            context,
                            &sink.client,
                            sink.pooled_client(),
                        )
                        .await
                        {
                            Ok(connections) => connections,
                            Err(error) => {
                                let reason = error.to_string();
                                context.report_init_error("postgres", &reason);
                                return Self::Missing { reason };
                            }
                        };
                        Self::from_row_sink_result(
                            "postgres",
                            context,
                            projection,
                            PostgresSink::new(
                                PostgresSinkConfig {
                                    table: sink.table.clone(),
                                    conflict_action: sink.conflict_action.clone(),
                                },
                                Box::new(connections),
                                context.sink_host(),
                            ),
                        )
                    }
                }
            }
            EmitterSinkPlan::MySql(sink) => {
                match Self::values_projection(
                    context,
                    MappedValuesProjectionInit {
                        label: "MySQL",
                        namespace: "mysql",
                        domain: &context.domain,
                        emitter: &context.emitter,
                        values: &sink.values,
                        input_schema: input_schema.arrow_schema(),
                        udfs: context.udfs.as_ref(),
                        max_batch: Some(sink.max_batch),
                    },
                ) {
                    SinkEmitterResult::Missing { reason } => Self::Missing { reason },
                    SinkEmitterResult::Ready(projection) => {
                        let connections = match PooledSinkClient::lease(
                            context,
                            &sink.client,
                            sink.pooled_client(),
                        )
                        .await
                        {
                            Ok(connections) => connections,
                            Err(error) => {
                                let reason = error.to_string();
                                context.report_init_error("mysql", &reason);
                                return Self::Missing { reason };
                            }
                        };
                        Self::from_row_sink_result(
                            "mysql",
                            context,
                            projection,
                            MySqlSink::new(
                                MySqlSinkConfig {
                                    table: sink.table.clone(),
                                    conflict_action: sink.conflict_action,
                                },
                                Box::new(connections),
                                context.sink_host(),
                            ),
                        )
                    }
                }
            }
            EmitterSinkPlan::MongoDb(sink) => {
                match Self::values_projection(
                    context,
                    MappedValuesProjectionInit {
                        label: "MongoDB",
                        namespace: "mongodb",
                        domain: &context.domain,
                        emitter: &context.emitter,
                        values: &sink.values,
                        input_schema: input_schema.arrow_schema(),
                        udfs: context.udfs.as_ref(),
                        max_batch: Some(sink.max_batch),
                    },
                ) {
                    SinkEmitterResult::Missing { reason } => Self::Missing { reason },
                    SinkEmitterResult::Ready(projection) => {
                        let client = match PooledSinkClient::lease(
                            context,
                            &sink.client,
                            sink.pooled_client(),
                        )
                        .await
                        {
                            Ok(client) => client,
                            Err(error) => {
                                let reason = error.to_string();
                                context.report_init_error("mongodb", &reason);
                                return Self::Missing { reason };
                            }
                        };
                        Self::from_row_sink_result(
                            "mongodb",
                            context,
                            projection,
                            MongoDbSink::new(
                                MongoDbSinkConfig {
                                    config: sink.client.config.entries.clone(),
                                    collection: sink.collection.clone(),
                                    conflict_action: sink.conflict_action.clone(),
                                },
                                Box::new(client),
                                context.sink_host(),
                            ),
                        )
                    }
                }
            }
            EmitterSinkPlan::Iceberg(sink) => {
                match Self::values_projection(
                    context,
                    MappedValuesProjectionInit {
                        label: "Iceberg",
                        namespace: "iceberg",
                        domain: &context.domain,
                        emitter: &context.emitter,
                        values: &sink.values,
                        input_schema: input_schema.arrow_schema(),
                        udfs: context.udfs.as_ref(),
                        // One commit reads every staged file back at once, so a staged write
                        // carries the whole batch the host released to it.
                        max_batch: None,
                    },
                ) {
                    SinkEmitterResult::Missing { reason } => Self::Missing { reason },
                    SinkEmitterResult::Ready(projection) => {
                        let commit = match context.parse_commit_policy(
                            "iceberg emitter",
                            &sink.commit_each,
                            &sink.max_commit_size,
                        ) {
                            Ok(commit) => commit,
                            Err(error) => {
                                return Self::missing_after_emitter_init_error(
                                    "iceberg", context, &error,
                                );
                            }
                        };
                        let mapped_schema = projection.mapped_schema().clone();
                        let opened = IcebergSink::new(
                            IcebergSinkConfig {
                                backend: sink.backend,
                                storage_config: sink.storage.config.entries.clone(),
                                catalog_name: sink.catalog.name.as_str().to_string(),
                                catalog_config: sink.catalog.config.entries.clone(),
                                namespace: context.domain.as_str().to_string(),
                                table: sink.table.clone(),
                                location: sink.location.clone(),
                                mapped_schema,
                                commit,
                                writer: context.emitter.as_str().to_string(),
                            },
                            context.sink_host(),
                        )
                        .await;
                        Self::from_row_sink_result("iceberg", context, projection, opened)
                    }
                }
            }
        }
    }

    fn from_record_sink_result<T>(
        sink: &str,
        context: &EmitterSinkContext,
        result: Result<T, Report<SinkStartError>>,
    ) -> Self
    where
        T: RecordSink + 'static,
    {
        match result {
            Ok(value) => {
                let sink: Box<dyn RecordSink> = Box::new(value);
                Self::from(sink)
            }
            Err(error) => {
                let error = error.change_context(EmitterRuntimeError::InitializeSink);
                let reason = emitter_error_message(&error);
                context.report_init_error(sink, &reason);
                Self::Missing { reason }
            }
        }
    }

    /// Compiles one row sink's `VALUES` mapping before the sink it feeds is opened.
    ///
    /// A mapping that cannot compile never produces a column, so the emitter reports the failure
    /// the same way it reports a client it could not open and recompiles on its next attempt.
    fn values_projection(
        context: &EmitterSinkContext,
        init: MappedValuesProjectionInit<'_>,
    ) -> SinkEmitterResult<MappedValuesProjection> {
        let sink = init.namespace;
        match MappedValuesProjection::compile(init) {
            Ok(projection) => SinkEmitterResult::Ready(projection),
            Err(error) => {
                let reason = error.to_string();
                context.report_init_error(sink, &reason);
                SinkEmitterResult::Missing { reason }
            }
        }
    }

    fn from_row_sink_result<T>(
        sink: &str,
        context: &EmitterSinkContext,
        projection: MappedValuesProjection,
        result: Result<T, Report<SinkStartError>>,
    ) -> Self
    where
        T: RowSink + 'static,
    {
        match result {
            Ok(value) => Self::from(RowSinkEmitter {
                sink: Box::new(value),
                projection,
            }),
            Err(error) => {
                let error = error.change_context(EmitterRuntimeError::InitializeSink);
                let reason = emitter_error_message(&error);
                context.report_init_error(sink, &reason);
                Self::Missing { reason }
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

    /// The wake that releases this emitter's buffered work on its own flush cadence and its
    /// sink's staged work on that sink's commit boundary.
    fn cadence_wake(&self, clock: &DomainClock, buffer: &EmitterBatchBuffer) -> RuntimeWake {
        let wake = match buffer.deadline() {
            Some(deadline) => RuntimeWake::never().with_buffer(clock, deadline),
            None => RuntimeWake::never(),
        };
        match self.commit_deadline() {
            Some(SinkDeadline::Domain(due_at)) => wake.with_buffer(
                clock,
                BranchBufferDeadline::Logical(clock.deadline_at(due_at)),
            ),
            Some(SinkDeadline::Physical(deadline)) => wake.with_physical(deadline),
            None => wake,
        }
    }

    /// When the staged work this sink holds has to be published.
    fn commit_deadline(&self) -> Option<SinkDeadline> {
        match self {
            Self::Record(sink) => sink.commit_deadline(),
            Self::Row(row) => row.sink.commit_deadline(),
            Self::Missing { .. } => None,
        }
    }

    /// Whether this sink publishes what it accepts later, on its own commit boundary.
    ///
    /// Such a sink neither acknowledges a row nor counts it as sent when the host's write returns:
    /// its commit does both.
    fn publishes_on_commit(&self) -> bool {
        match self {
            Self::Record(sink) => sink.retains_acknowledgements(),
            Self::Row(row) => row.sink.retains_acknowledgements(),
            Self::Missing { .. } => false,
        }
    }

    /// How many messages this sink staged out of the host's buffer and has not published yet.
    fn staged_messages(&self) -> u64 {
        match self {
            Self::Record(sink) => sink.staged_messages(),
            Self::Row(row) => row.sink.staged_messages(),
            Self::Missing { .. } => 0,
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
        match self {
            Self::Record(sink) => !sink.keeps_client_on_publish_failure(),
            Self::Row(row) => !row.sink.keeps_client_on_publish_failure(),
            Self::Missing { .. } => true,
        }
    }

    fn pending_acks(&self, buffer: &EmitterBatchBuffer) -> EmitterAcknowledgements {
        let sink = match self {
            Self::Record(sink) => sink.pending_acks(),
            Self::Row(row) => row.sink.pending_acks(),
            Self::Missing { .. } => None,
        };
        EmitterAcknowledgements {
            runtime: buffer.pending_acks(),
            sink,
        }
    }

    async fn finish_transport(&mut self, deadline: Instant) -> EmitterRuntimeResult<()> {
        match self {
            Self::Record(sink) => sink.finish(deadline).await.map_err(sink_publish_failure),
            Self::Row(row) => row
                .sink
                .finish(deadline)
                .await
                .map_err(sink_publish_failure),
            Self::Missing { .. } => Ok(()),
        }
    }

    fn reconnect_after(&self, error: &Report<EmitterRuntimeError>) -> bool {
        if let EmitterRuntimeError::PublishStalled = error.current_context() {
            false
        } else {
            self.requires_publish_failure_reinitialization()
        }
    }

    async fn flush_due(
        &mut self,
        sink: &EmitterSinkPlan,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        codec: Option<Arc<CompiledCodec>>,
        buffer: &mut EmitterBatchBuffer,
        retry: bool,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        let flushed = if buffer.should_flush(context, retry)? {
            self.flush_buffer(sink, context, control, codec, buffer)
                .await?
        } else {
            None
        };
        let reason = match retry {
            true => SinkCommitReason::Retry,
            false => SinkCommitReason::Cadence,
        };
        let committed = self
            .commit_staged(sink.label(), context, control, buffer, reason)
            .await?;
        Ok(PublishReport::merge_optional(flushed, committed))
    }

    async fn flush_all(
        &mut self,
        sink: &EmitterSinkPlan,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        codec: Option<Arc<CompiledCodec>>,
        buffer: &mut EmitterBatchBuffer,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        let flushed;
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
                    flushed = report;
                    break;
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
        let committed = self
            .commit_staged(
                sink.label(),
                context,
                control,
                buffer,
                SinkCommitReason::Drain,
            )
            .await?;
        Ok(PublishReport::merge_optional(flushed, committed))
    }

    async fn publish_batch(
        &mut self,
        sink: &EmitterSinkPlan,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        codec: Option<Arc<CompiledCodec>>,
        buffer: &mut EmitterBatchBuffer,
        batch: EmitterPublishBatch,
    ) -> EmitterPublishResult {
        if !buffer
            .push(context, batch)
            .map_err(EmitterPublishFailure::caller)?
        {
            return Ok(None);
        }
        let flushed = self
            .flush_buffer(sink, context, control, codec, buffer)
            .await
            .map_err(EmitterPublishFailure::buffer)?;
        // The write may have reached the sink's commit boundary, which publishes here rather than
        // waiting for the next wake. Its failure belongs to the sink: the rows it staged are no
        // longer in this buffer.
        let committed = self
            .commit_staged_once(context, control, buffer, SinkCommitReason::Cadence)
            .await
            .map_err(EmitterPublishFailure::sink)?;
        Ok(PublishReport::merge_optional(flushed, committed))
    }

    /// Whether a sink-owned deadline has been reached.
    fn sink_deadline_reached(
        context: &EmitterSinkContext,
        deadline: SinkDeadline,
    ) -> EmitterRuntimeResult<bool> {
        match deadline {
            SinkDeadline::Domain(due_at) => {
                let snapshot = context.execution_snapshot()?;
                context
                    .clock
                    .deadline_reached(&context.clock.deadline_at(due_at), &snapshot)
                    .change_context(EmitterRuntimeError::FlushTiming)
            }
            SinkDeadline::Physical(deadline) => {
                Ok(PhysicalDeadlineCapability::operational().is_reached(deadline))
            }
        }
    }

    /// Publishes what the sink staged when its commit boundary is reached, retrying a failed
    /// commit on the emitter's declared backoff while the acknowledgements it holds stay alive.
    async fn commit_staged(
        &mut self,
        label: &str,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &EmitterBatchBuffer,
        reason: SinkCommitReason,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        let mut reason = reason;
        loop {
            tokio::task::consume_budget().await;
            match self
                .commit_staged_once(context, control, buffer, reason)
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
                Err(error) if error.current_context().is_retryable_publish_failure() => {
                    let acks = self.pending_acks(buffer);
                    if !Self::wait_for_commit_retry(label, context, control, &acks, &error).await? {
                        return match reason {
                            SinkCommitReason::Drain => {
                                Err(Report::new(EmitterRuntimeError::ShutdownWhileStalled)
                                    .attach_printable(
                                        "emitter drain stopped while a staged commit remained \
                                         pending",
                                    ))
                            }
                            SinkCommitReason::Cadence | SinkCommitReason::Retry => Ok(None),
                        };
                    }
                    // An attempt the cadence started keeps retrying whatever is staged, exactly as
                    // the failure it is recovering from was already committing it.
                    reason = reason.forced();
                }
                Err(error) => {
                    context.report_flush_error(label, &emitter_error_message(&error));
                    return Err(error);
                }
            }
        }
    }

    /// One commit attempt, bounded by the emitter's stop deadline and keeping every
    /// acknowledgement the sink retained alive while the external system confirms it.
    async fn commit_staged_once(
        &mut self,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        buffer: &EmitterBatchBuffer,
        reason: SinkCommitReason,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        let Some(deadline) = self.commit_deadline() else {
            return Ok(None);
        };
        if !reason.forces_commit() && !Self::sink_deadline_reached(context, deadline)? {
            return Ok(None);
        }
        let acks = self.pending_acks(buffer);
        let committed = {
            let _confirmation_wait = context
                .runtime
                .begin_emitter_confirmation_wait(&context.domain, &context.emitter);
            let commit = Box::pin(self.commit_sink());
            await_until_emitter_stop_deadline(
                control.stop_rx,
                await_emitter_confirmation(&acks, commit),
            )
            .await
            .map_err(|()| emitter_stop_deadline_elapsed())?
        };
        buffer.report_staged_messages(self.staged_messages());
        let report = committed?.map(|report| {
            PublishReport::flushed(report.messages, report.bytes, report.domain_timestamp)
        });
        Ok(report)
    }

    async fn commit_sink(&mut self) -> EmitterRuntimeResult<Option<SinkCommitReport>> {
        match self {
            Self::Record(sink) => sink.commit().await.map_err(sink_publish_failure),
            Self::Row(row) => row.sink.commit().await.map_err(sink_publish_failure),
            Self::Missing { .. } => Ok(None),
        }
    }

    async fn flush_buffer(
        &mut self,
        sink: &EmitterSinkPlan,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        codec: Option<Arc<CompiledCodec>>,
        buffer: &mut EmitterBatchBuffer,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        if buffer.is_empty() {
            return Ok(None);
        }
        self.check_fault_injection(context, control)?;
        // A sink that stages what it accepts has not published anything yet, so its commit counts
        // these messages as sent and this write counts none.
        let report = match self.publishes_on_commit() {
            true => None,
            false => buffer.report(),
        };
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
            let published = await_until_emitter_stop_deadline(
                control.stop_rx,
                await_emitter_confirmation(&pending_acks, publish),
            )
            .await;
            buffer.report_staged_messages(self.staged_messages());
            published.map_err(|()| emitter_stop_deadline_elapsed())??;
        }
        buffer.clear();
        Ok(report)
    }

    async fn publish_buffered_batches(
        &mut self,
        sink: &EmitterSinkPlan,
        context: &EmitterSinkContext,
        codec: Option<Arc<CompiledCodec>>,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()> {
        if let EmitterSinkPlan::Kafka(_)
        | EmitterSinkPlan::Pulsar(_)
        | EmitterSinkPlan::RabbitMq(_)
        | EmitterSinkPlan::Redis(_)
        | EmitterSinkPlan::Mqtt(_)
        | EmitterSinkPlan::Nats(_)
        | EmitterSinkPlan::ZeroMq(_)
        | EmitterSinkPlan::Syslog(_)
        | EmitterSinkPlan::Sqs(_)
        | EmitterSinkPlan::Sentry(_) = sink
            && codec.is_none()
        {
            return Err(
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "{} emitter requires an encoding codec",
                    sink.label()
                )),
            );
        }
        if let Self::Row(emitter) = &mut *self {
            return emitter.publish_batches(context, batches).await;
        }
        if let (Some(codec), Self::Record(emitter)) = (codec, &mut *self) {
            let encoded = encode_broker_records(codec, context, batches).await?;
            let records = sink_records(context, batches, encoded).await?;
            let outcome = emitter.publish(records).await;
            return finish_record_sink_publish(
                context,
                batches,
                outcome,
                DeliveredAcknowledgements::Host,
            )
            .await;
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
        // An unavailable client is the one injected fault a sink recovers from on its own: the
        // publish fails the way an unreachable external system does, and the emitter retries it on
        // its declared backoff until the fault clears.
        if control
            .fault_injection
            .sink_client_is_unavailable(&context.emitter)
        {
            return Err(Report::new(EmitterRuntimeError::PublishBatch)
                .attach_printable("sink fault injector returned an unavailable client"));
        }
        Ok(())
    }

    async fn wait_for_commit_retry(
        sink: &str,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        acks: &EmitterAcknowledgements,
        error: &Report<EmitterRuntimeError>,
    ) -> EmitterRuntimeResult<bool> {
        let reason = emitter_error_message(error);
        let wait = control.backoff.next_delay();
        context.runtime.record_commit_failure_with_backoff(
            &context.domain,
            &context.emitter,
            reason.clone(),
            wait,
        );
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
}

pub(super) fn emitter_error_message(error: &Report<EmitterRuntimeError>) -> String {
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
            let message_group = batch
                .sqs_message_groups
                .get(row_index)
                .cloned()
                .ok_or_else(|| {
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "emitter batch row {row_index} has no SQS FIFO group entry"
                    ))
                })?;
            let payload = match payload {
                Ok(payload) => payload,
                Err(error) => {
                    rejected.push(RejectedEmitterRecord {
                        position: SinkRecordPosition {
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
                message_group,
                execution_now: batch.execution_now,
            });
        }
    }
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Encode).await?;
    Ok(encoded)
}

/// The records a sink publishes, once every record whose ordering group could not be evaluated
/// has been rejected here.
///
/// A sink that orders records per group receives the group already evaluated for its record, so
/// the expression that produced it, and the reason a record has none, both stay with the host.
async fn sink_records(
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
    encoded: Vec<EncodedBrokerRecord>,
) -> EmitterRuntimeResult<Vec<SinkRecord>> {
    let mut records = Vec::with_capacity(encoded.len());
    let mut rejected = Vec::new();
    for record in encoded {
        tokio::task::consume_budget().await;
        let position = record.position();
        let message_group = match record.message_group {
            Ok(message_group) => message_group,
            Err(error) => {
                rejected.push(RejectedEmitterRecord {
                    position,
                    reason: error.to_string(),
                    structured_error: None,
                });
                continue;
            }
        };
        let sink_record = SinkRecord::new(
            position,
            record.key,
            record.payload,
            record.headers,
            record.execution_now,
        );
        records.push(match message_group {
            Some(message_group) => sink_record.with_message_group(message_group),
            None => sink_record,
        });
    }
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Publish).await?;
    Ok(records)
}

async fn finish_record_sink_publish(
    context: &EmitterSinkContext,
    batches: &mut [EmitterPublishBatch],
    outcome: PerRecordOutcome,
    acknowledgements: DeliveredAcknowledgements,
) -> EmitterRuntimeResult<()> {
    let outcome = outcome.into_parts();
    for SinkRecordPosition {
        batch_index,
        row_index,
    } in outcome.delivered
    {
        let batch = batches.get_mut(batch_index).ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "sink confirmation references missing emitter batch {batch_index}"
            ))
        })?;
        batch.mark_delivered(row_index, acknowledgements)?;
    }
    let rejected = outcome
        .rejected
        .into_iter()
        .map(|rejected| RejectedEmitterRecord {
            position: rejected.position,
            reason: String::new(),
            structured_error: Some(rejected.error),
        })
        .collect();
    finish_rejected_records(context, batches, rejected, MessageErrorOperation::Publish).await?;
    match outcome.infrastructure_error {
        Some(error) => Err(sink_publish_failure(error)),
        None => Ok(()),
    }
}

/// A connector's publish failure as this emitter's own, keeping a misconfigured sink out of the
/// retry loop it would never leave.
fn sink_publish_failure(error: Report<SinkPublishError>) -> Report<EmitterRuntimeError> {
    match error.current_context() {
        SinkPublishError::Misconfigured { .. } => {
            error.change_context(EmitterRuntimeError::InvalidSinkConfig)
        }
        SinkPublishError::NotInitialized { .. }
        | SinkPublishError::Publish { .. }
        | SinkPublishError::Finish { .. }
        | SinkPublishError::Commit { .. } => {
            error.change_context(EmitterRuntimeError::PublishBatch)
        }
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
        let SinkRecordPosition {
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

impl EmitterTask {
    pub(in crate::runtime) fn spawn(
        runtime: &Runtime,
        build: EmitterTaskBuildDeps<'_>,
        emitter: CreateEmitter,
        plan: EmitterStartPlan,
        inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
    ) -> Result<ScheduledEmitterTask, RuntimeError> {
        let EmitterTaskBuildDeps {
            domain,
            shutdown_tx,
            codecs,
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
        let task_domain = domain.clone();
        let task_emitter = emitter.name.clone();
        let task_metric_relay = if emitter.from.relays().len() == 1 {
            emitter.from.first().cloned()
        } else {
            None
        };
        let dispatcher = runtime.inner.remote_dispatcher.load();
        let physical_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        let task_input_metrics = inputs
            .iter()
            .map(|(relay, _)| {
                let metrics = runtime.inner.metrics.resolve_node_input_metrics(
                    domain,
                    ModelKind::Emitter,
                    &ModelName::from(&emitter.name),
                    relay,
                    physical_node_id,
                    None,
                );
                (relay.clone(), metrics)
            })
            .collect::<HashMap<_, _>>();
        let task_output_metrics = match &task_metric_relay {
            Some(relay) => {
                EmitterOutputMetrics::Relay(runtime.inner.metrics.resolve_node_batch_metrics(
                    NodeBatchMetricsSpec {
                        domain,
                        kind: ModelKind::Emitter,
                        node: &ModelName::from(&emitter.name),
                        relay,
                        physical_node_id,
                        direction: "sent",
                        branch_key: None,
                    },
                ))
            }
            None => EmitterOutputMetrics::WithoutRelay(
                runtime.inner.metrics.resolve_global_node_message_metrics(
                    domain,
                    ModelKind::Emitter,
                    &ModelName::from(&emitter.name),
                    physical_node_id,
                    "sent",
                ),
            ),
        };
        let task_flush_policy = emitter.flush_policy.clone();
        let task_error_policies = emitter.error_policies.clone();
        let task_materialized_state = emitter.materialized_state.clone();
        let routing_relay = inputs
            .first()
            .map(|(relay, _)| relay.clone())
            .verified("the registry validated that every emitter has at least one input");
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
        if let EmitterSinkPlan::Syslog(sink) = &plan.sink
            && let Err(error) = SyslogSink::check_client_config(&sink.client.config.entries)
        {
            let error = error.change_context(EmitterRuntimeError::InvalidSinkConfig);
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "failed to resolve syslog emitter client: {}",
                    emitter_error_message(&error)
                ),
            });
        }
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
            let shared_routing = match runtime
                .wait_for_domain_routing(&task_domain, &routing_relay)
                .await
            {
                Ok(routing) => routing,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "emitter '{}' in domain '{}' could not bind its routing snapshot: {error}",
                        task_emitter.as_str(),
                        task_domain.as_str(),
                    ));
                    return;
                }
            };
            let mut routing = DomainRoutingCache::new(shared_routing);
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
            let mut interaction_inputs = Vec::with_capacity(inputs.len());
            for (relay, receiver) in inputs {
                let input = match input_collect_policy {
                    Some(policy) => RelayInteractionInput::collecting(
                        relay,
                        receiver,
                        policy,
                        domain_clock.clone(),
                    ),
                    None => RelayInteractionInput::immediate(relay, receiver),
                };
                interaction_inputs.push(input);
            }
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
                clock: domain_clock.clone(),
            };
            let mut publish_backoff = RuntimeReconnectBackoff::from_policy(plan.retry_policy);
            let mut emitter_buffer =
                EmitterBatchBuffer::new(&context, &task_flush_policy, buffered_messages.clone());
            let sink_runtime = SinkEmitterRuntime {
                input_schema: input_schema.clone(),
            };
            let mut sink = SinkEmitter::new_until_cancelled(
                SinkEmitterInit {
                    plan: &plan,
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
                        acks: EmitterAcknowledgements::default(),
                        waiting_for_stall_clear: false,
                        reason: Some(reason),
                    },
                );
            } else {
                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
            }
            let task_emitter_node = ModelName::from(&task_emitter);
            let mut batch_context = EmitterBatchContext {
                runtime: &runtime,
                routing: &mut routing,
                domain_clock: &domain_clock,
                domain: &task_domain,
                emitter: &task_emitter,
                node: &task_emitter_node,
                output_metrics: &task_output_metrics,
                error_policies: &task_error_policies,
                source_filters: &source_filters,
                filter_map: filter_map.as_ref(),
                sqs_fifo_group: sqs_fifo_group.as_ref(),
                materialized_state: &task_materialized_state,
            };
            loop {
                tokio::task::consume_budget().await;
                let wake = retry_schedule.wake(sink.cadence_wake(&context.clock, &emitter_buffer));
                let receive_input = !retry_schedule.is_active()
                    || emitter_buffer_count.load(Ordering::Acquire) == 0;
                let work = match interaction.next_with_input(wake, receive_input).await {
                    Ok(work) => work,
                    // The emitter holds work whose cadence it cannot resolve, because the domain
                    // clock is not readable in this generation. There is no wall-clock fallback
                    // for a logical cadence, so the work stays buffered and unpublished while the
                    // acknowledgements it owns are kept alive on the physical beat.
                    Err(RelayInteractionError::WakeTiming { reason, .. }) => {
                        runtime.record_emitter_transient_error(&task_domain, &task_emitter, reason);
                        let acks = sink.pending_acks(&emitter_buffer);
                        RuntimeReconnectBackoff::wait_duration_with_ack_alive(
                            RETRY_ACK_ALIVE_EACH,
                            &mut shutdown_rx,
                            &acks,
                        )
                        .await;
                        continue;
                    }
                    Err(error) => {
                        let reason = error.to_string();
                        context.report_flush_error(plan.sink.label(), &reason);
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
                            context.report_flush_error(plan.sink.label(), &reason);
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
                            context.report_flush_error(plan.sink.label(), &reason);
                            clear_emitter_stop_signal(&task_stop_signal, deadline);
                            response
                                .send(Err(Report::new(EmitterRuntimeError::FinalFlush)
                                    .attach_printable(format!(
                                        "emitter final flush failed: {reason}"
                                    ))))
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
                                    &plan.sink,
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
                                context.report_flush_error(plan.sink.label(), &reason);
                                Err(
                                    Report::new(EmitterRuntimeError::FinalFlush).attach_printable(
                                        format!("emitter final flush failed: {reason}"),
                                    ),
                                )
                            }
                            Err(_) => {
                                let reason = format!(
                                    "emitter '{}' did not drain before its configured deadline",
                                    task_emitter.as_str()
                                );
                                context.report_flush_error(plan.sink.label(), &reason);
                                Err(Report::new(EmitterRuntimeError::StopDeadlineElapsed)
                                    .attach_printable(reason))
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
                            context.report_flush_error(plan.sink.label(), &reason);
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
                                &plan.sink,
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
                                        waiting_for_stall_clear: error.current_context()
                                            == &EmitterRuntimeError::PublishStalled,
                                        reason: Some(&reason),
                                    },
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                context.report_flush_error(plan.sink.label(), &reason);
                            }
                            Err(error) => {
                                retry_schedule.clear();
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(plan.sink.label(), &reason);
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
                            context.report_flush_error(plan.sink.label(), &reason);
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
                                &plan.sink,
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
                                context.report_flush_error(plan.sink.label(), &reason);
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
                                    plan: &plan,
                                    context: &context,
                                    runtime: sink_runtime.clone(),
                                },
                                &mut work_cancel_rx,
                            )
                            .await;
                            emitter_buffer.report_staged_messages(sink.staged_messages());
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
                                &plan.sink,
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
                                        waiting_for_stall_clear: error.current_context()
                                            == &EmitterRuntimeError::PublishStalled,
                                        reason: Some(&reason),
                                    },
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                context.report_flush_error(plan.sink.label(), &reason);
                            }
                            Err(error) => {
                                retry_schedule.clear();
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                );
                                context.report_flush_error(plan.sink.label(), &reason);
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
                        let delivery_observation = batch.delivery_observation(actual_utc_now());
                        let input_metrics = task_input_metrics
                            .get(&input_relay)
                            .verified("the task resolves metrics for every declared emitter input");
                        input_metrics.observe_batch(
                            batch.message_count(),
                            batch.estimated_bytes(),
                            delivery_observation.domain_timestamp,
                        );
                        runtime.mark_branch_aggregated_metrics_updated(
                            &task_domain,
                            ModelKind::Emitter,
                            &task_emitter,
                        );
                        for seconds in delivery_observation.latency_seconds {
                            input_metrics.observe_delivery_latency(
                                seconds,
                                delivery_observation.domain_timestamp,
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
                                emitter_buffer.retain_without_cadence(publish_batch.clone())
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
                                    context.report_publish_error(plan.sink.label(), reason);
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
                                &plan.sink,
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
                                        waiting_for_stall_clear: error.current_context()
                                            == &EmitterRuntimeError::PublishStalled,
                                        reason: Some(&reason),
                                    },
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                context.report_publish_error(plan.sink.label(), &reason);
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
                                context.report_publish_error(plan.sink.label(), &reason);
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

impl EmitterBatchContext<'_> {
    fn observe_sent(&self, report: &PublishReport) {
        self.output_metrics.observe(report);
        self.runtime.mark_branch_aggregated_metrics_updated(
            self.domain,
            ModelKind::Emitter,
            self.node,
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
                let failure = *error;
                self.report_general_error(
                    failure.preserved.acks.iter(),
                    format!("{reason}; {}", failure.error),
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
                    node: self.node,
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

    /// Report a failure that no single message owns, so the node-wide general error policy
    /// decides what happens to the acknowledgments the failed work was holding.
    fn report_general_error<'a>(&self, acks: impl IntoIterator<Item = &'a AckSet>, reason: String) {
        self.runtime.handle_general_error_for_acks(
            self.domain,
            ModelKind::Emitter,
            self.emitter,
            self.error_policies,
            acks,
            reason,
        );
    }

    /// Deliver the message errors a plan recorded, one for each row its program rejected.
    async fn deliver_planned_message_errors(&self, errors: Vec<PlannedMessageError>) {
        self.runtime
            .handle_planned_message_errors(
                self.domain,
                ModelKind::Emitter,
                self.emitter,
                self.error_policies,
                errors,
            )
            .await;
    }

    /// Resolve the node-wide materialized dependencies of one source batch.
    ///
    /// `None` means the batch is no longer this call's to publish: a declaration skipped it, the
    /// wait for required state ended without it, or resolution failed and its acknowledgments
    /// have already been reported.
    async fn resolve_materialized_dependencies(
        &mut self,
        input_relay: &RelayName,
        batch: RelayRecordBatch,
        wait: MaterializedBatchWaitContext<'_>,
    ) -> Option<ResolvedEmitterInput> {
        // Resolution consumes the batch, so the acknowledgments a failure would report are taken
        // while the batch still holds them.
        let dependency_error_acks = batch.acks.clone();
        let resolution = self
            .runtime
            .resolve_materialized_dependencies_for_batch(
                MaterializedDomainHandles {
                    routing: &mut *self.routing,
                    domain_clock: self.domain_clock,
                    domain: self.domain,
                },
                input_relay,
                self.materialized_state,
                batch,
                wait,
            )
            .await;
        let resolved = match resolution {
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
        let (batch, materialized_values, execution_now) = resolved;
        Some(ResolvedEmitterInput {
            batch,
            materialized_values,
            execution_now,
        })
    }

    async fn process(
        &mut self,
        input_relay: &RelayName,
        batch: RelayRecordBatch,
        shutdown_rx: &mut watch::Receiver<bool>,
        wait_for_required_state: bool,
        quiesce_work: Option<&mut NodeQuiesceWorkGuard>,
    ) -> Option<EmitterPublishBatch> {
        let resolved = self
            .resolve_materialized_dependencies(
                input_relay,
                batch,
                MaterializedBatchWaitContext {
                    shutdown_rx,
                    wait_for_required_state,
                    quiesce_work,
                },
            )
            .await?;
        let ResolvedEmitterInput {
            batch,
            materialized_values,
            execution_now,
        } = resolved;

        let batch = self
            .filter_source_batch(input_relay, batch, &materialized_values, execution_now)
            .await?;

        // FIFO groups are evaluated over the filtered source batch and stay indexed by source
        // row, so the filter map below can hand every published row the group its own input
        // produced. A row that cannot produce one keeps its reason and is rejected at the send;
        // only a failure of the whole evaluation drops the batch here.
        let source_sqs_message_groups = match self.sqs_fifo_group {
            None => vec![Ok(None); batch.batch.batch().num_rows()],
            Some(CompiledSqsFifoGroup::FromBranch) => {
                let mut groups = Vec::with_capacity(batch.keys.len());
                for key in &batch.keys {
                    let group = match key.as_ref() {
                        Some(key) => Ok(Some(key.as_str().to_string())),
                        None => Err(SqsMessageGroupError::UnbranchedRecord),
                    };
                    groups.push(group);
                }
                groups
            }
            Some(CompiledSqsFifoGroup::Expression(program)) => {
                let evaluated = evaluate_sqs_fifo_group_program(
                    self.emitter,
                    program,
                    &batch,
                    execution_now,
                    &materialized_values,
                )
                .await;
                match evaluated {
                    Ok(groups) => groups,
                    Err(error) => {
                        let error = error.current_context();
                        self.report_general_error(error.acks.iter(), error.reason.clone());
                        return None;
                    }
                }
            }
        };

        let Some(filter_map) = self.filter_map else {
            // Without a filter map every source row publishes as it arrived, so the groups
            // already align with the batch row for row.
            let publish_batch = EmitterPublishBatch::from_batch(batch, execution_now);
            match publish_batch.with_sqs_message_groups(source_sqs_message_groups) {
                Ok(batch) => return Some(batch),
                Err(error) => {
                    self.report_general_error(
                        std::iter::empty::<&AckSet>(),
                        format!(
                            "emitter '{}' failed to build SQS FIFO group batch: {error}",
                            self.emitter.as_str()
                        ),
                    );
                    return None;
                }
            }
        };

        let planned = plan_emitter_filter_map_batch(
            self.emitter,
            filter_map,
            batch,
            execution_now,
            &materialized_values,
        )
        .await;
        let plan = match planned {
            Ok(plan) => plan,
            Err(error) => {
                self.report_general_error(error.acks.iter(), error.reason);
                return None;
            }
        };

        // The plan reports the source row of every output row in output order, so reading the
        // source groups through it keeps each published row with the group its own input
        // produced.
        let mut selected_sqs_message_groups = Vec::with_capacity(plan.source_rows.len());
        for source_row in &plan.source_rows {
            let group = match source_sqs_message_groups.get(*source_row) {
                Some(group) => group.clone(),
                None => Err(SqsMessageGroupError::SourceRowOutOfBounds { row: *source_row }),
            };
            selected_sqs_message_groups.push(group);
        }

        self.deliver_planned_message_errors(plan.message_errors)
            .await;

        // A plan that kept no row has no batch to publish, and the rows it rejected have just
        // been reported.
        let batch = plan.batch?;
        let publish_batch = match EmitterPublishBatch::new(batch, plan.headers, execution_now) {
            Ok(publish_batch) => publish_batch,
            Err(error) => {
                self.report_filtered_batch_error(error);
                return None;
            }
        };
        match publish_batch.with_sqs_message_groups(selected_sqs_message_groups) {
            Ok(batch) => Some(batch),
            Err(error) => {
                self.report_filtered_batch_error(error);
                None
            }
        }
    }

    /// Report a filtered batch whose rows, headers, and FIFO groups stopped agreeing in count.
    /// Both counts are checked while building the same batch, so either one failing is the same
    /// failure to report.
    fn report_filtered_batch_error(&self, error: Report<EmitterRuntimeError>) {
        self.report_general_error(
            std::iter::empty::<&AckSet>(),
            format!(
                "emitter '{}' failed to build filtered header batch: {}",
                self.emitter.as_str(),
                error
            ),
        );
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
            MessageErrorOperation::SourceWhere,
            program,
            batch,
            execution_now,
            side_inputs,
        )
        .await
        {
            Ok(plan) => plan,
            Err(error) => {
                self.report_general_error(
                    error.acks.iter(),
                    format!("input relay '{}': {}", input_relay.as_str(), error.reason),
                );
                return None;
            }
        };
        self.deliver_planned_message_errors(plan.message_errors)
            .await;
        plan.batch
    }
}

#[cfg(test)]
mod publishing_tests {
    use nervix_connector::ParsedRetryPolicy;

    use super::*;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
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
        let error = sink_publish_failure(
            Report::new(SinkPublishError::Publish { sink: "test" })
                .attach(SinkRetryDelay(Duration::from_secs(2))),
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
            let _ = response.send(Err(Report::new(EmitterRuntimeError::StopDeadlineElapsed)
                .attach_printable("infrastructure retry exceeded drain deadline")));
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

    use nervix_connector::ResolvedClientConfig;
    use nervix_connector_nats::{NatsPublishingMode, NatsSink};
    use nervix_models::{
        ClientName, CreateSchema, DomainName, EmitterName, ModelName, ParseAsType, SchemaName,
        SubjectName,
    };

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

    /// A NATS sink whose client is never opened: the tests using it drive a `Missing` sink.
    fn nats_sink() -> EmitterSinkPlan {
        EmitterSinkPlan::Nats(NatsSinkPlan {
            client: EmitterClientSpec {
                name: ClientName::parse("client").expect("valid client name"),
                config: ResolvedClientConfig::default(),
            },
            subject: SubjectName::parse("subject").expect("valid subject name"),
            mode: NatsPublishingMode::Core,
        })
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
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::HeaderCountMismatch {
                header_count: 0,
                row_count: 1,
            }
        );
    }

    #[test]
    fn publish_batch_rejects_misaligned_groups_and_row_updates() {
        let groups =
            match EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100))
                .with_sqs_message_groups(Vec::new())
            {
                Ok(_) => panic!("SQS groups must stay aligned with source rows"),
                Err(error) => error,
            };
        assert_eq!(
            *groups.current_context(),
            EmitterRuntimeError::SqsGroupCountMismatch {
                group_count: 0,
                row_count: 1,
            }
        );

        let mut delivered =
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
        let error = delivered
            .mark_delivered(1, DeliveredAcknowledgements::Host)
            .expect_err("delivery cannot address a missing row");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::DeliveryRowOutOfBounds {
                row: 1,
                row_count: 1,
            }
        );

        delivered.batch.acks.clear();
        let error = delivered
            .mark_delivered(0, DeliveredAcknowledgements::Host)
            .expect_err("delivery requires the row's acknowledgement set");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::AcknowledgementRowOutOfBounds {
                row: 0,
                row_count: 0,
            }
        );

        let mut rejected =
            EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100));
        let error = rejected
            .mark_rejected(1)
            .expect_err("rejection cannot address a missing row");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::RejectionRowOutOfBounds {
                row: 1,
                row_count: 1,
            }
        );
    }

    /// A connector decides why its own configuration is unusable, so the host states only that the
    /// sink could not be initialized and reports the connector's message as the reason it is
    /// missing.
    #[test]
    fn a_connector_start_failure_becomes_a_missing_sink_with_its_own_message() {
        let sink = SinkEmitter::from_record_sink_result::<NatsSink>(
            "NATS",
            &sink_context(),
            Err(
                Report::new(SinkStartError::InvalidConfiguration { sink: "NATS" })
                    .attach_printable("missing NATS client config key 'servers'"),
            ),
        );

        let SinkEmitter::Missing { reason } = sink else {
            panic!("a start failure must leave the sink missing")
        };
        assert_eq!(reason, "missing NATS client config key 'servers'");
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
        assert!(
            !buffer
                .push(&context, first)
                .expect("first batch must buffer")
        );
        assert_eq!(buffer.pending_messages, 1);
        assert!(
            buffer.deadline().is_some(),
            "the first push arms the cadence"
        );
        assert!(
            !buffer
                .push(&context, second)
                .expect("second batch must buffer")
        );

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
                    EmitterPublishBatch::from_batch(input_batch(), Timestamp::from_unix_nanos(100),)
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
        let sink_config = nats_sink();
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

    #[tokio::test]
    async fn a_sink_that_commits_separately_takes_the_rows_it_accepted() {
        let (first_acks, mut first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        let mut batch = EmitterPublishBatch::from_batch(
            RelayRecordBatch::from_messages(
                input_schema(),
                vec![
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
                        acks: first_acks,
                    },
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([("value".to_string(), RuntimeValue::I64(2))]),
                        acks: second_acks,
                    },
                ],
            )
            .expect("valid two-row emitter batch"),
            Timestamp::from_unix_nanos(100),
        );

        let retained = batch.acks_for_rows(&[0]);
        batch
            .mark_delivered(0, DeliveredAcknowledgements::Sink)
            .expect("a staged row must be marked delivered");
        batch
            .mark_delivered(1, DeliveredAcknowledgements::Host)
            .expect("a published row must be marked delivered");

        assert!(batch.pending_record_rows().is_empty());
        assert_eq!(second_completion.wait().await, AckOutcome::Ack);
        // The staged row is the sink's until its commit resolves it, so the host left it alone.
        retained.ack_alive();
        assert_eq!(
            first_completion.wait_for_progress().await,
            AckProgress::Alive
        );
        retained.ack_success();
        assert_eq!(first_completion.wait().await, AckOutcome::Ack);
    }

    #[test]
    fn a_started_commit_keeps_forcing_itself_through_every_retry() {
        assert!(!SinkCommitReason::Cadence.forces_commit());
        assert!(SinkCommitReason::Retry.forces_commit());
        assert!(SinkCommitReason::Drain.forces_commit());

        assert_eq!(SinkCommitReason::Cadence.forced(), SinkCommitReason::Retry);
        assert_eq!(SinkCommitReason::Retry.forced(), SinkCommitReason::Retry);
        assert_eq!(SinkCommitReason::Drain.forced(), SinkCommitReason::Drain);
    }

    #[test]
    fn buffered_and_staged_message_counts_are_summed_independently() {
        let reported = Arc::new(AtomicUsize::new(0));
        let buffered = EmitterBufferedMessages::new(reported.clone());

        buffered.set_buffered(2);
        buffered.set_staged(3);
        assert_eq!(reported.load(Ordering::Acquire), 5);

        buffered.set_buffered(0);
        assert_eq!(reported.load(Ordering::Acquire), 3);
        buffered.set_staged(0);
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

        buffered_messages.set_buffered(7);
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
        let cadence = buffer
            .deadline()
            .expect("the push arms the logical cadence");
        let mut retry = EmitterRetrySchedule::default();

        let idle = retry.wake(RuntimeWake::never().with_buffer(&context.clock, cadence.clone()));
        assert!(
            !idle
                .is_reached()
                .expect("the fixture clock stays installed"),
            "an idle retry leaves the ordinary cadence wake in place"
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
        let (sink_retained, mut sink_completion) = AckSet::root();
        let mut retry = EmitterRetrySchedule::default();
        retry
            .schedule(Duration::from_secs(30), existing, false)
            .expect("the fixture backoff fits the monotonic clock range");
        retry.include_acks(EmitterAcknowledgements {
            runtime: force_drained,
            sink: Some(SinkAcknowledgements::new(sink_retained)),
        });
        retry.ack_alive_at = Some(
            PhysicalDeadlineCapability::operational()
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
        assert_eq!(
            sink_completion.wait_for_progress().await,
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
        let sink_config = nats_sink();
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
            let _ = response.send(Err(Report::new(EmitterRuntimeError::FinalFlush)
                .attach_printable("emitter final flush failed: broker unavailable")));
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
        let sink_config = nats_sink();
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

    #[tokio::test]
    async fn sink_context_reports_configuration_and_publish_failures() {
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

        let mut messages = Vec::with_capacity(4);
        for _ in 0..4 {
            tokio::task::consume_budget().await;
            let RuntimeEvent::Error(message) =
                events.recv().await.expect("error event must be emitted");
            messages.push(message);
        }
        assert!(messages[0].contains("failed to initialize nats emitter"));
        assert!(messages[1].contains("failed to publish nats message"));
        assert!(messages[2].contains("failed to flush nats rows"));
        assert!(messages[3].contains("invalid flush_each 'not-a-duration'"));
    }

    #[tokio::test]
    async fn sink_host_delegates_runtime_services_and_general_error_policy() {
        let context = sink_context();
        let host = context.sink_host();
        let mut events = context.runtime.events().subscribe();

        host.record_transient_error(
            "broker temporarily unavailable".to_string(),
            Duration::from_millis(25),
        );
        assert_eq!(
            context
                .runtime
                .emitter_transient_error(&context.domain, &context.emitter),
            Some("broker temporarily unavailable".to_string())
        );
        assert!(
            context
                .runtime
                .emitter_reconnect_backoff(&context.domain, &context.emitter)
                .is_some()
        );
        host.clear_transient_error();
        assert_eq!(
            context
                .runtime
                .emitter_transient_error(&context.domain, &context.emitter),
            None
        );

        host.report_error("connector background error".to_string());
        let RuntimeEvent::Error(message) = events
            .recv()
            .await
            .expect("the host must publish the event");
        assert_eq!(
            message,
            "sink error for emitter 'output' in domain 'emitter_tests': connector background error"
        );
        assert_eq!(host.staging_directory(), context.runtime.temp_dir());

        let (logged_acks, logged_completion) = AckSet::root();
        let logged_acks = SinkAcknowledgements::new(logged_acks);
        host.handle_general_error(&logged_acks, "publish failed".to_string());
        let RuntimeEvent::Error(message) = events
            .recv()
            .await
            .expect("the logged general error must publish an event");
        assert!(message.contains("publish failed"));
        assert_eq!(
            logged_completion.wait().await,
            AckOutcome::NoAck("publish failed".to_string())
        );

        let mut ignored_context = sink_context();
        ignored_context.error_policies.general = GeneralErrorPolicy::Ignore;
        let (ignored_acks, ignored_completion) = AckSet::root();
        ignored_context.sink_host().handle_general_error(
            &SinkAcknowledgements::new(ignored_acks),
            "ignored failure".to_string(),
        );
        assert_eq!(ignored_completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn sink_acknowledgement_handle_preserves_ack_lifecycle() {
        let (acks, mut completion) = AckSet::root();
        let acks = SinkAcknowledgements::new(acks);

        assert!(!acks.is_empty());
        acks.keep_alive();
        assert_eq!(completion.wait_for_progress().await, AckProgress::Alive);
        acks.acknowledge();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        assert!(SinkAcknowledgements::new(AckSet::empty()).is_empty());
    }

    #[test]
    fn sql_value_compilers_reject_empty_mappings_before_compilation() {
        let domain = DomainName::parse("emitter_tests").expect("valid domain");
        let emitter = EmitterName::parse("output").expect("valid emitter name");
        let schema = input_schema().arrow_schema();

        let errors =
            [("Iceberg", "iceberg"), ("ClickHouse", "clickhouse")].map(|(label, namespace)| {
                MappedValuesProjection::compile(MappedValuesProjectionInit {
                    label,
                    namespace,
                    domain: &domain,
                    emitter: &emitter,
                    values: &[],
                    input_schema: schema.clone(),
                    udfs: None,
                    max_batch: None,
                })
                .err()
            });
        for result in errors {
            let Some(error) = result else {
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
