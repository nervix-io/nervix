use error_stack::{AttachmentKind, FrameKind, Report};
use thiserror::Error;

use super::*;

pub(in crate::runtime) mod clickhouse;
mod iceberg;
mod kafka;
mod mongodb;
mod mqtt;
mod mysql;
mod nats;
mod postgres;
pub(in crate::runtime) mod pulsar;
mod rabbitmq;
mod redis;
mod sentry;
mod sqs;
mod zeromq;

use clickhouse::ClickHouseEmitter;
use iceberg::{
    IcebergEmitter, IcebergEmitterClientConfig, IcebergEmitterError, IcebergEmitterInit,
    IcebergEmitterResult,
};
use kafka::KafkaEmitter;
use mongodb::MongoDbEmitter;
use mqtt::MqttEmitter;
use mysql::MySqlEmitter;
use nats::NatsEmitter;
use postgres::PostgresEmitter;
use pulsar::PulsarEmitter;
use rabbitmq::RabbitMqEmitter;
use redis::RedisEmitter;
use sentry::SentryEmitter;
use sqs::SqsEmitter;
use zeromq::ZeroMqEmitter;

const ENCODE_CHUNK_ROWS: usize = 1_024;
const BLOCKING_ENCODE_CHUNKS: usize = 2;
const PUBLISH_IN_FLIGHT: usize = 256;
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
                .saturating_add(self.iceberg.load(Ordering::Acquire)),
            Ordering::Release,
        );
    }
}

impl Default for EmitterBufferedMessages {
    fn default() -> Self {
        Self::new(Arc::new(AtomicUsize::new(0)))
    }
}

#[derive(Clone)]
pub(in crate::runtime) struct EmitterSinkContext {
    domain: Domain,
    emitter: Identifier,
    temp_dir: Arc<PathBuf>,
    events: broadcast::Sender<RuntimeEvent>,
    udfs: Option<UdfExecutor>,
}

struct EmitterPublishControl<'a> {
    runtime: &'a Runtime,
    fault_injector: &'a EmitterFaultInjector,
    backoff: &'a mut RuntimeReconnectBackoff,
}

struct EmitterBatchContext<'a> {
    runtime: &'a Runtime,
    domain: &'a Domain,
    emitter: &'a Identifier,
    metric_relay: Option<&'a Identifier>,
    error_policies: &'a ErrorPolicies,
    source_filters: &'a HashMap<Identifier, CompiledProgramWithMaterializedInterest>,
    filter_map: Option<&'a CompiledEmitterFilterMapProgram>,
    materialized_state: &'a [nervix_models::MaterializedStateDependency],
    materialized_stream_owner_nodes: &'a HashMap<Identifier, Option<String>>,
}

#[derive(Clone)]
struct EmitterPublishBatch {
    batch: RelayRecordBatch,
    headers: Option<Vec<EmitterHeaders>>,
}

impl EmitterPublishBatch {
    fn from_batch(batch: RelayRecordBatch) -> Self {
        Self {
            batch,
            headers: None,
        }
    }

    fn new(batch: RelayRecordBatch, headers: Option<Vec<EmitterHeaders>>) -> Result<Self, String> {
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
        Ok(Self { batch, headers })
    }

    fn estimated_bytes(&self) -> u64 {
        self.batch.estimated_bytes().saturating_add(
            self.headers
                .iter()
                .flatten()
                .flatten()
                .map(|(name, value)| {
                    u64::try_from(name.len())
                        .unwrap_or(u64::MAX)
                        .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
                })
                .fold(0_u64, u64::saturating_add),
        )
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

    fn ack_success(&self) {
        self.batch.ack_success();
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
            messages: self.messages.saturating_add(other.messages),
            bytes: self.bytes.saturating_add(other.bytes),
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

#[derive(Default)]
struct EmitterBatchBuffer {
    flush_policy: Option<RuntimeFlushPolicy>,
    pending: Vec<EmitterPublishBatch>,
    pending_messages: u64,
    pending_bytes: u64,
    flush_at: Option<Instant>,
    buffered_messages: Arc<EmitterBufferedMessages>,
}

impl EmitterBatchBuffer {
    fn new(
        context: &EmitterSinkContext,
        flush_each: &str,
        max_batch_size: Option<&str>,
        buffered_messages: Arc<EmitterBufferedMessages>,
    ) -> Self {
        Self {
            flush_policy: context.parse_flush_policy_with_max(
                "emitter",
                flush_each,
                max_batch_size,
            ),
            pending: Vec::new(),
            pending_messages: 0,
            pending_bytes: 0,
            flush_at: None,
            buffered_messages,
        }
    }

    fn update_buffered_messages(&self) {
        self.buffered_messages
            .set_generic(usize::try_from(self.pending_messages).unwrap_or(usize::MAX));
    }

    fn reconfigure(
        &mut self,
        context: &EmitterSinkContext,
        flush_each: &str,
        max_batch_size: Option<&str>,
    ) {
        self.flush_policy =
            context.parse_flush_policy_with_max("emitter", flush_each, max_batch_size);
        self.flush_at = self
            .flush_policy
            .filter(|_| !self.pending.is_empty())
            .map(|policy| Instant::now() + policy.interval());
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn deadline(&self) -> Option<Instant> {
        self.flush_at
    }

    fn push(&mut self, batch: EmitterPublishBatch) -> EmitterRuntimeResult<bool> {
        let Some(flush_policy) = self.flush_policy else {
            return Err(Report::new(EmitterRuntimeError::FlushPolicyNotInitialized));
        };
        self.pending_messages = self.pending_messages.saturating_add(batch.message_count());
        self.pending_bytes = self.pending_bytes.saturating_add(batch.estimated_bytes());
        self.pending.push(batch);
        self.update_buffered_messages();
        if self.flush_at.is_none() {
            self.flush_at = Some(Instant::now() + flush_policy.interval());
        }
        Ok(flush_policy.size_boundary_reached(self.pending_bytes))
    }

    fn is_due(&self) -> bool {
        self.flush_at
            .is_some_and(|deadline| deadline <= Instant::now())
    }

    fn should_flush(&self, force: bool) -> bool {
        !self.pending.is_empty() && (force || self.is_due())
    }

    fn defer_retry(&mut self, delay: Duration) {
        if !self.pending.is_empty() {
            self.flush_at = Some(Instant::now() + delay);
        }
    }

    fn retain_for_retry(
        &mut self,
        batch: EmitterPublishBatch,
        delay: Duration,
    ) -> EmitterRuntimeResult<()> {
        self.push(batch)?;
        self.defer_retry(delay);
        Ok(())
    }

    fn pending_acks(&self) -> AckSet {
        AckSet::merged(self.pending.iter().map(EmitterPublishBatch::merged_acks))
    }

    fn ack_pending_success(&self) {
        for batch in &self.pending {
            batch.ack_success();
        }
    }

    fn drain_pending(&mut self) -> Vec<EmitterPublishBatch> {
        let pending = std::mem::take(&mut self.pending);
        self.pending_messages = 0;
        self.pending_bytes = 0;
        self.flush_at = None;
        self.update_buffered_messages();
        pending
    }

    fn clear(&mut self) {
        self.pending.clear();
        self.pending_messages = 0;
        self.pending_bytes = 0;
        self.flush_at = None;
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
            .filter_map(EmitterPublishBatch::domain_timestamp)
            .max()
            .unwrap_or_else(current_timestamp);
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

#[derive(Default)]
struct EmitterRetrySchedule {
    retry_at: Option<Instant>,
    ack_alive_at: Option<Instant>,
    acks: AckSet,
    waiting_for_stall_clear: bool,
}

impl EmitterRetrySchedule {
    fn is_active(&self) -> bool {
        self.retry_at.is_some()
    }

    fn schedule(&mut self, delay: Duration, acks: AckSet, waiting_for_stall_clear: bool) {
        let now = Instant::now();
        let retry_at = now + delay;
        self.retry_at = Some(retry_at);
        if !acks.is_empty() {
            self.acks = acks;
        }
        self.ack_alive_at =
            (!self.acks.is_empty()).then(|| (now + RETRY_ACK_ALIVE_EACH).min(retry_at));
        self.waiting_for_stall_clear = waiting_for_stall_clear;
    }

    fn include_acks(&mut self, acks: AckSet) {
        if acks.is_empty() {
            return;
        }
        self.acks = AckSet::merged([std::mem::take(&mut self.acks), acks]);
        if let Some(retry_at) = self.retry_at
            && self.ack_alive_at.is_none()
        {
            self.ack_alive_at = Some((Instant::now() + RETRY_ACK_ALIVE_EACH).min(retry_at));
        }
    }

    fn deadline(&self, ordinary: Option<Instant>) -> Option<Instant> {
        let retry = match (self.retry_at, self.ack_alive_at) {
            (Some(retry_at), Some(ack_alive_at)) => Some(retry_at.min(ack_alive_at)),
            (Some(retry_at), None) => Some(retry_at),
            (None, _) => None,
        };
        if retry.is_some() { retry } else { ordinary }
    }

    fn retry_is_due(&mut self) -> bool {
        let Some(retry_at) = self.retry_at else {
            return true;
        };
        let now = Instant::now();
        if now >= retry_at {
            self.retry_at = None;
            self.ack_alive_at = None;
            return true;
        }
        if self
            .ack_alive_at
            .is_some_and(|ack_alive_at| now >= ack_alive_at)
        {
            self.acks.ack_alive();
            self.ack_alive_at = Some((now + RETRY_ACK_ALIVE_EACH).min(retry_at));
        }
        false
    }

    fn release_if_stall_cleared(&mut self, fault: Option<EmitterFaultMode>) -> bool {
        if !self.waiting_for_stall_clear || fault.is_some() {
            return false;
        }
        self.retry_at = None;
        self.ack_alive_at = None;
        self.waiting_for_stall_clear = false;
        true
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
    domain: &Domain,
    emitter: &Identifier,
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
                    Identifier::parse(&format!("c{index}")).map_err(|error| error.to_string())?,
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
        nervix_nspl::vm_program::SemanticNamespaces::new("input", namespace),
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
        udfs.map(|executor| executor.signatures().clone())
            .unwrap_or_default(),
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
            .map(|(field, data_type, nullable)| {
                arrow_schema::Field::new(field, data_type, nullable)
            })
            .collect::<Vec<_>>(),
    ));
    let compile_bindings = vec![
        VmCompileBinding::writeonly(namespace, output_schema.clone()),
        VmCompileBinding::readonly("input", input_schema.clone()),
        VmCompileBinding::readonly("message", input_schema),
    ];
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
    })
}

fn compile_clickhouse_values_program(
    domain: &Domain,
    emitter: &Identifier,
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
    domain: &Domain,
    emitter: &Identifier,
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
    domain: &Domain,
    emitter: &Identifier,
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
    domain: &Domain,
    emitter: &Identifier,
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
    domain: &Domain,
    emitter: &Identifier,
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
) -> EmitterRuntimeResult<Vec<Vec<serde_json::Value>>> {
    let side_inputs = HashMap::default();
    let lookup_columns = HashMap::default();
    let input = project_vm_input_batch(
        &program.program.input_schema,
        &VmInputProjectionSources {
            carrier: &batch.batch,
            keys: &batch.keys,
            side_inputs: &side_inputs,
            lookup_columns: &lookup_columns,
            uninitialized: None,
        },
    )
    .map_err(|error| Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(error))?;
    let result = execute_program_with_selection_in_context(
        program.program.as_ref(),
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
    if let Some(side_error) = result.batch.errors().iter().flatten().next() {
        return Err(
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "{} VALUES side error {}: {} at {}",
                program.label,
                side_error.code.as_str(),
                side_error.message,
                side_error.span
            )),
        );
    }
    let mut rows = Vec::with_capacity(row_count);
    for row in 0..row_count {
        let output = vm_output_row_to_decoded_record(&result.batch, row).map_err(|error| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(error)
        })?;
        rows.push(
            mappings
                .iter()
                .enumerate()
                .map(|(index, _mapping)| {
                    let field = format!("c{index}");
                    if let Some(value) = output.value(&field) {
                        runtime_value_to_json(value)
                    } else {
                        serde_json::Value::Null
                    }
                })
                .collect(),
        );
    }
    Ok(rows)
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
        let _ = self.events.send(RuntimeEvent::Error(format!(
            "failed to initialize {sink} emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        )));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to initialize emitter sink"
        );
    }

    fn report_publish_error(&self, sink: &str, error: &str) {
        let _ = self.events.send(RuntimeEvent::Error(format!(
            "failed to publish {sink} message for emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        )));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to publish emitter message"
        );
    }

    fn report_flush_error(&self, sink: &str, error: &str) {
        let _ = self.events.send(RuntimeEvent::Error(format!(
            "failed to flush {sink} rows for emitter '{}' in domain '{}': {error}",
            self.emitter.as_str(),
            self.domain.as_str(),
        )));
        warn!(
            domain = self.domain.as_str(),
            emitter = self.emitter.as_str(),
            error,
            "failed to flush emitter rows"
        );
    }

    fn parse_flush_policy_with_max(
        &self,
        kind: &str,
        flush_each: &str,
        max_batch_size: Option<&str>,
    ) -> Option<RuntimeFlushPolicy> {
        match Runtime::parse_runtime_node_flush_policy(
            &self.domain,
            kind,
            &self.emitter,
            flush_each,
            max_batch_size,
        ) {
            Ok(policy) => Some(policy),
            Err(error) => {
                let _ = self.events.send(RuntimeEvent::Error(error.to_string()));
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
    Sqs(SqsEmitter),
    Sentry(SentryEmitter),
    ClickHouse(ClickHouseEmitter),
    Postgres(PostgresEmitter),
    MySql(MySqlEmitter),
    MongoDb(MongoDbEmitter),
    Iceberg(IcebergEmitter),
    Missing { reason: String },
}

enum EncodedChunkPublisher<'a> {
    Kafka {
        topic: &'a Identifier,
        emitter: &'a KafkaEmitter,
    },
    Sink {
        emitter: &'a mut SinkEmitter,
        sink: &'a EmitSink,
    },
}

impl EncodedChunkPublisher<'_> {
    async fn publish_chunk(
        &mut self,
        context: &EmitterSinkContext,
        batch: &EmitterPublishBatch,
        start: usize,
        payloads: Vec<Vec<u8>>,
    ) -> EmitterRuntimeResult<()> {
        let message_count = payloads.len();
        let end = start.saturating_add(message_count);
        let keys = batch.batch.keys.get(start..end).ok_or_else(|| {
            Report::new(EmitterRuntimeError::EncodeBatch)
                .attach_printable("encoded payload range is outside emitter branch keys")
        })?;
        let empty_headers;
        let headers = match &batch.headers {
            Some(headers) => headers.get(start..end).ok_or_else(|| {
                Report::new(EmitterRuntimeError::EncodeBatch)
                    .attach_printable("encoded payload range is outside emitter headers")
            })?,
            None if end <= batch.batch.keys.len() => {
                empty_headers = vec![EmitterHeaders::new(); message_count];
                empty_headers.as_slice()
            }
            None => {
                return Err(Report::new(EmitterRuntimeError::EncodeBatch)
                    .attach_printable("encoded payload range is outside emitter rows"));
            }
        };

        match self {
            Self::Kafka { topic, emitter } => {
                emitter
                    .publish_chunk(topic, keys, payloads, headers)
                    .await?;
            }
            Self::Sink { emitter, sink } => match (&mut **emitter, *sink) {
                (SinkEmitter::Pulsar(emitter), EmitSink::Pulsar { .. }) => {
                    emitter
                        .publish_chunk(keys, payloads, headers, PUBLISH_IN_FLIGHT)
                        .await?;
                }
                (SinkEmitter::RabbitMq(emitter), EmitSink::RabbitMq { queue, .. }) => {
                    emitter
                        .publish_chunk(queue, payloads, headers, PUBLISH_IN_FLIGHT)
                        .await?;
                }
                (SinkEmitter::Redis(emitter), EmitSink::Redis { channel, .. }) => {
                    emitter.publish_chunk(channel, payloads).await?;
                }
                (SinkEmitter::Mqtt(emitter), EmitSink::Mqtt { .. }) => {
                    emitter.publish_chunk(payloads).await?;
                }
                (SinkEmitter::Nats(emitter), EmitSink::Nats { .. }) => {
                    emitter.publish_chunk(payloads, headers).await?;
                }
                (SinkEmitter::ZeroMq(emitter), EmitSink::ZeroMq { .. }) => {
                    for payload in payloads {
                        tokio::task::consume_budget().await;
                        emitter.publish(payload).await?;
                    }
                }
                (SinkEmitter::Sqs(emitter), EmitSink::Sqs { .. }) => {
                    for (offset, payload) in payloads.into_iter().enumerate() {
                        tokio::task::consume_budget().await;
                        emitter.publish(payload, &headers[offset]).await?;
                    }
                }
                (SinkEmitter::Sentry(emitter), EmitSink::Sentry { .. }) => {
                    for payload in payloads {
                        tokio::task::consume_budget().await;
                        emitter.publish(payload).await?;
                    }
                }
                _ => {
                    return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                        .attach_printable("emitter has no initialized sink client"));
                }
            },
        }
        trace!(
            domain = context.domain.as_str(),
            emitter = context.emitter.as_str(),
            rows = message_count,
            "emitter published encoded chunk"
        );
        Ok(())
    }
}

#[derive(Clone)]
struct SinkEmitterRuntime {
    input_schema: Arc<CompiledSchema>,
    buffered_messages: Arc<EmitterBufferedMessages>,
}

struct SinkEmitterInit<'a> {
    sink: &'a EmitSink,
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
                Self::from_result("kafka", context, KafkaEmitter::new(client, resolved))
                    .map(Self::Kafka)
            }
            (EmitSink::Pulsar { topic, .. }, Some(Model::ClientPulsar(client)), _) => {
                match PulsarEmitter::new(client, resolved, topic).await {
                    Ok(emitter) => Self::Pulsar(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("pulsar", context, &error),
                }
            }
            (EmitSink::RabbitMq { .. }, Some(Model::ClientRabbitMq(client)), _) => {
                match RabbitMqEmitter::new(client, resolved).await {
                    Ok(emitter) => Self::RabbitMq(emitter),
                    Err(error) => {
                        Self::missing_after_emitter_init_error("rabbitmq", context, &error)
                    }
                }
            }
            (EmitSink::Redis { .. }, Some(Model::ClientRedis(client)), _) => {
                match RedisEmitter::new(client, resolved).await {
                    Ok(emitter) => Self::Redis(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("redis", context, &error),
                }
            }
            (EmitSink::Mqtt { topic, .. }, Some(Model::ClientMqtt(client)), _) => {
                Self::from_result(
                    "mqtt",
                    context,
                    MqttEmitter::new(client, resolved, topic, context),
                )
                .map(Self::Mqtt)
            }
            (EmitSink::Nats { subject, .. }, Some(Model::ClientNats(client)), _) => {
                match NatsEmitter::new(client, resolved, subject).await {
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
            (EmitSink::Sqs { queue, .. }, Some(Model::ClientSqs(client)), _) => {
                match SqsEmitter::new(client, resolved, queue).await {
                    Ok(emitter) => Self::Sqs(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("sqs", context, &error),
                }
            }
            (EmitSink::Sentry { .. }, Some(Model::ClientSentry(client)), _) => {
                Self::from_result("sentry", context, SentryEmitter::new(client, resolved))
                    .map(Self::Sentry)
            }
            (EmitSink::ClickHouse { values, .. }, Some(Model::ClientClickHouse(client)), _) => {
                Self::ClickHouse(ClickHouseEmitter::new(
                    client,
                    resolved,
                    context,
                    values,
                    input_schema.arrow_schema(),
                ))
            }
            (EmitSink::Postgres { values, .. }, Some(Model::ClientPostgres(client)), _) => {
                Self::Postgres(
                    PostgresEmitter::new(
                        client,
                        resolved,
                        context,
                        values,
                        input_schema.arrow_schema(),
                    )
                    .await,
                )
            }
            (EmitSink::MySql { values, .. }, Some(Model::ClientMySql(client)), _) => Self::MySql(
                MySqlEmitter::new(
                    client,
                    resolved,
                    context,
                    values,
                    input_schema.arrow_schema(),
                )
                .await,
            ),
            (EmitSink::MongoDb { values, .. }, Some(Model::ClientMongoDb(client)), _) => {
                Self::MongoDb(
                    MongoDbEmitter::new(
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
                    flush_each,
                    max_batch_size,
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
                    flush_each,
                    max_batch_size: max_batch_size.as_deref(),
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
                    flush_each,
                    max_batch_size,
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
                    flush_each,
                    max_batch_size: max_batch_size.as_deref(),
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
                    flush_each,
                    max_batch_size,
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
                    flush_each,
                    max_batch_size: max_batch_size.as_deref(),
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

    fn flush_deadline(&self, buffer: &EmitterBatchBuffer) -> Option<Instant> {
        let sink_deadline = match self {
            Self::Iceberg(emitter) => emitter.flush_deadline(),
            _ => None,
        };
        match (sink_deadline, buffer.deadline()) {
            (Some(sink), Some(buffer)) => Some(sink.min(buffer)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    fn missing_reason(&self) -> Option<&str> {
        if let Self::Missing { reason } = self {
            Some(reason.as_str())
        } else {
            None
        }
    }

    fn pending_acks(&self, buffer: &EmitterBatchBuffer) -> AckSet {
        let sink_acks = if let Self::Iceberg(emitter) = self {
            emitter.pending_acks()
        } else {
            AckSet::empty()
        };
        AckSet::merged([buffer.pending_acks(), sink_acks])
    }

    fn reconnect_after(&self, error: &Report<EmitterRuntimeError>) -> bool {
        if let EmitterRuntimeError::PublishStalled = error.current_context() {
            false
        } else {
            !matches!(self, Self::Iceberg(_))
        }
    }

    fn reconfigure_flush_policy(
        &mut self,
        context: &EmitterSinkContext,
        flush_each: &str,
        max_batch_size: Option<&str>,
    ) {
        if let Self::Iceberg(emitter) = self
            && let Some(policy) =
                context.parse_flush_policy_with_max("iceberg emitter", flush_each, max_batch_size)
        {
            emitter.reconfigure_flush_policy(policy);
        }
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
            let flushed = if retry {
                emitter.finish().await
            } else {
                emitter.flush_due().await
            };
            return match flushed {
                Ok(published) => Ok(PublishReport::merge_optional(accepted, published)),
                Err(error) => {
                    let message = iceberg_error_message(&error);
                    context.report_flush_error(sink.label(), &message);
                    Err(Report::new(EmitterRuntimeError::PublishBatch).attach_printable(message))
                }
            };
        }
        if !buffer.should_flush(retry) {
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
            return emitter
                .finish()
                .await
                .map(|published| PublishReport::merge_optional(accepted, published))
                .map_err(|error| {
                    Report::new(EmitterRuntimeError::PublishBatch)
                        .attach_printable(iceberg_error_message(&error))
                });
        }
        self.flush_buffer(sink, context, control, codec, buffer)
            .await
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
            self.wait_for_fault_injector(context, control, &batch.merged_acks())
                .await
                .map_err(EmitterPublishFailure::caller)?;
            let Self::Iceberg(emitter) = self else {
                unreachable!("checked Iceberg emitter must remain Iceberg")
            };
            return match emitter.publish_batch(batch.batch).await {
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
                    Ok(None)
                }
            };
        }

        if buffer.push(batch).map_err(EmitterPublishFailure::caller)? {
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
        if !buffer.should_flush(force) {
            return Ok(None);
        }
        self.wait_for_fault_injector(context, control, &buffer.pending_acks())
            .await?;
        let Self::Iceberg(emitter) = self else {
            return Ok(None);
        };
        let pending = buffer.drain_pending();
        let mut pending = pending.into_iter();
        let mut report = None;
        while let Some(batch) = pending.next() {
            tokio::task::consume_budget().await;
            match emitter.publish_batch(batch.batch).await {
                Ok(published) => {
                    report = PublishReport::merge_optional(report, published);
                }
                Err(error) => {
                    for batch in pending {
                        tokio::task::consume_budget().await;
                        buffer.push(batch)?;
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
        self.wait_for_fault_injector(context, control, &buffer.pending_acks())
            .await?;
        let report = buffer.report();
        self.publish_buffered_batches(sink, context, codec, buffer.pending.as_slice())
            .await?;
        buffer.ack_pending_success();
        buffer.clear();
        Ok(report)
    }

    async fn publish_buffered_batches(
        &mut self,
        sink: &EmitSink,
        context: &EmitterSinkContext,
        codec: Option<Arc<CompiledCodec>>,
        batches: &[EmitterPublishBatch],
    ) -> EmitterRuntimeResult<()> {
        match (&mut *self, sink) {
            (Self::ClickHouse(emitter), EmitSink::ClickHouse { table, values, .. }) => {
                for batch in batches {
                    tokio::task::consume_budget().await;
                    emitter.publish_batch(table, values, &batch.batch).await?;
                }
                return Ok(());
            }
            (
                Self::Postgres(emitter),
                EmitSink::Postgres {
                    table,
                    values,
                    conflict_action,
                    ..
                },
            ) => {
                for batch in batches {
                    tokio::task::consume_budget().await;
                    emitter
                        .publish_batch(table, values, conflict_action, &batch.batch)
                        .await?;
                }
                return Ok(());
            }
            (
                Self::MySql(emitter),
                EmitSink::MySql {
                    table,
                    values,
                    conflict_action,
                    ..
                },
            ) => {
                for batch in batches {
                    tokio::task::consume_budget().await;
                    emitter
                        .publish_batch(table, values, conflict_action, &batch.batch)
                        .await?;
                }
                return Ok(());
            }
            (
                Self::MongoDb(emitter),
                EmitSink::MongoDb {
                    collection,
                    values,
                    conflict_action,
                    ..
                },
            ) => {
                for batch in batches {
                    tokio::task::consume_budget().await;
                    emitter
                        .publish_batch(collection, values, conflict_action, &batch.batch)
                        .await?;
                }
                return Ok(());
            }
            _ => {}
        }

        let Some(codec) = codec else {
            return Err(Report::new(EmitterRuntimeError::EncodeBatch)
                .attach_printable("encoded emitter has no compiled codec"));
        };
        let mut publisher = match (&mut *self, sink) {
            (Self::Kafka(emitter), EmitSink::Kafka { topic, .. }) => {
                EncodedChunkPublisher::Kafka { topic, emitter }
            }
            (emitter, sink) => EncodedChunkPublisher::Sink { emitter, sink },
        };
        let mut publish_result = Ok(());
        for batch in batches {
            tokio::task::consume_budget().await;
            if let Err(error) =
                Self::publish_encoded_batch(&mut publisher, context, codec.clone(), batch).await
            {
                publish_result = Err(error);
                break;
            }
        }
        publish_result
    }

    async fn publish_encoded_batch(
        publisher: &mut EncodedChunkPublisher<'_>,
        context: &EmitterSinkContext,
        codec: Arc<CompiledCodec>,
        batch: &EmitterPublishBatch,
    ) -> EmitterRuntimeResult<()> {
        let row_count = batch.batch.batch.batch().num_rows();
        if batch.batch.keys.len() != row_count
            || batch
                .headers
                .as_ref()
                .is_some_and(|headers| headers.len() != row_count)
        {
            return Err(
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                    "emitter '{}' encoded batch row, branch key, and header counts differ",
                    context.emitter.as_str()
                )),
            );
        }
        if codec.requires_blocking_encode() {
            return Self::publish_blocking_encoded_batch(
                publisher, context, codec, batch, row_count,
            )
            .await;
        }

        if let EncodedChunkPublisher::Kafka { topic, emitter } = publisher {
            let encoder = codec
                .batch_encoder(&batch.batch.batch)
                .map_err(|error| Self::encode_batch_error(context, error))?;
            let mut payload = Vec::new();
            for row_index in 0..row_count {
                tokio::task::consume_budget().await;
                encoder
                    .encode_row_into(row_index, &mut payload)
                    .map_err(|error| Self::encode_batch_error(context, error))?;
                emitter
                    .publish_message(
                        topic,
                        batch.batch.keys[row_index].as_ref(),
                        &payload,
                        batch.headers_for_row(row_index).ok_or_else(|| {
                            Report::new(EmitterRuntimeError::EncodeBatch)
                                .attach_printable("encoded row is outside emitter headers")
                        })?,
                    )
                    .await?;
            }
            trace!(
                domain = context.domain.as_str(),
                emitter = context.emitter.as_str(),
                rows = row_count,
                "emitter serialized an Arrow batch directly into Kafka"
            );
            return Ok(());
        }

        for start in (0..row_count).step_by(ENCODE_CHUNK_ROWS) {
            tokio::task::consume_budget().await;
            let end = start.saturating_add(ENCODE_CHUNK_ROWS).min(row_count);
            let payloads = codec
                .encode_batch(&batch.batch.batch, start..end)
                .map_err(|error| Self::encode_batch_error(context, error))?;
            publisher
                .publish_chunk(context, batch, start, payloads)
                .await?;
        }
        Ok(())
    }

    async fn publish_blocking_encoded_batch(
        publisher: &mut EncodedChunkPublisher<'_>,
        context: &EmitterSinkContext,
        codec: Arc<CompiledCodec>,
        batch: &EmitterPublishBatch,
        row_count: usize,
    ) -> EmitterRuntimeResult<()> {
        let (encoded_tx, mut encoded_rx) = mpsc::channel(BLOCKING_ENCODE_CHUNKS);
        let arrow_batch = batch.batch.batch.clone();
        let codec_name = codec.name.as_str().to_string();
        let encoder = tokio::task::spawn_blocking(move || {
            for start in (0..row_count).step_by(ENCODE_CHUNK_ROWS) {
                let end = start.saturating_add(ENCODE_CHUNK_ROWS).min(row_count);
                let payloads = codec.encode_batch(&arrow_batch, start..end);
                let failed = payloads.is_err();
                if encoded_tx.blocking_send((start, payloads)).is_err() || failed {
                    break;
                }
            }
        });

        while let Some((start, payloads)) = encoded_rx.recv().await {
            tokio::task::consume_budget().await;
            let payloads = match payloads {
                Ok(payloads) => payloads,
                Err(error) => {
                    drop(encoded_rx);
                    let _ = encoder.await;
                    return Err(Self::encode_batch_error(context, error));
                }
            };
            if let Err(error) = publisher
                .publish_chunk(context, batch, start, payloads)
                .await
            {
                drop(encoded_rx);
                let _ = encoder.await;
                return Err(error);
            }
        }
        encoder.await.map_err(|error| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "emitter '{}' blocking codec task for '{}' failed: {error}",
                context.emitter.as_str(),
                codec_name
            ))
        })?;
        Ok(())
    }

    fn encode_batch_error(
        context: &EmitterSinkContext,
        error: CodecError,
    ) -> Report<EmitterRuntimeError> {
        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
            "emitter '{}' failed to encode columnar batch: {error}",
            context.emitter.as_str()
        ))
    }

    async fn wait_for_fault_injector(
        &self,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        acks: &AckSet,
    ) -> EmitterRuntimeResult<()> {
        match control.fault_injector.fault_mode(&context.emitter) {
            Some(EmitterFaultMode::Fail) => {
                let reason = format!(
                    "fault injector failed emitter '{}'",
                    context.emitter.as_str()
                );
                let _ = context.events.send(RuntimeEvent::Error(format!(
                    "{} in domain '{}'",
                    reason,
                    context.domain.as_str()
                )));
                warn!(
                    domain = context.domain.as_str(),
                    emitter = context.emitter.as_str(),
                    "fault injector failed emitter publish"
                );
                Err(Report::new(EmitterRuntimeError::FaultInjected).attach_printable(reason))
            }
            Some(EmitterFaultMode::Stall) => {
                let wait = control.backoff.next_delay();
                control.runtime.record_emitter_transient_error_with_backoff(
                    &context.domain,
                    &context.emitter,
                    "fault injector stalled emitter publish",
                    wait,
                );
                let _ = context.events.send(RuntimeEvent::Error(format!(
                    "fault injector stalled emitter '{}' in domain '{}' before reconnect retry in \
                     {}",
                    context.emitter.as_str(),
                    context.domain.as_str(),
                    humantime::format_duration(wait),
                )));
                warn!(
                    domain = context.domain.as_str(),
                    emitter = context.emitter.as_str(),
                    reconnect_backoff = %humantime::format_duration(wait),
                    "fault injector stalled emitter publish"
                );
                acks.ack_alive();
                Err(Report::new(EmitterRuntimeError::PublishStalled)
                    .attach_printable("fault injector stalled emitter publish"))
            }
            None => Ok(()),
        }
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
    fault_injector: &EmitterFaultInjector,
    emitter: &Identifier,
) -> Option<String> {
    sink.missing_reason().map(str::to_owned).or_else(|| {
        if let Some(EmitterFaultMode::Stall) = fault_injector.fault_mode(emitter) {
            Some("fault injector stalled emitter publish".to_string())
        } else {
            None
        }
    })
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
            EmitSink::Sqs { .. } => "sqs",
            EmitSink::Sentry { .. } => "sentry",
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
        inputs: Vec<(Identifier, RelayRuntimeFanIn)>,
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
            materialized_relay_owner_nodes: materialized_stream_owner_nodes,
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
        let output_compiled_schema = codec
            .as_ref()
            .map(|codec| codec.schema())
            .unwrap_or_else(|| input_schema.clone());
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
        let mut source_filters = HashMap::default();
        for source_filter in emitter.from.where_clauses() {
            let program = compile_scoped_filter_program(
                RuntimeCompileTarget {
                    domain,
                    identifier: &emitter.name,
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
            .expect("an emitter FROM WHERE expression must compile to a program");
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
        let task_flush_each = emitter.flush_each.clone();
        let task_max_batch_size = emitter.max_batch_size.clone();
        let task_error_policies = emitter.error_policies.clone();
        let task_materialized_state = emitter.materialized_state.clone();
        let task_events = runtime.events.clone();
        let fault_injector = runtime.emitter_faults.clone();
        let runtime = runtime.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let mut domain_work_cancel_rx = shutdown_tx.subscribe();
        let (work_cancel, mut work_cancel_rx) = watch::channel(false);
        let task_work_cancel = work_cancel.clone();
        let quiesce_counters = runtime.node_quiesce_counters(domain, &emitter.name);
        let force_flush = runtime.force_flush_participant(domain, quiesce_counters.clone());
        let emitter_buffer_count = runtime
            .emitter_buffers
            .entry(RuntimeKey::new(domain.clone(), emitter.name.clone()))
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

        let task = tokio::spawn(async move {
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
                    RelayInteractionInput::new(relay, receiver, input_collect_policy)
                })
                .collect();
            let mut interaction = RelayInteraction::with_commands(
                interaction_inputs,
                shutdown_rx,
                Some(force_flush),
                Some(quiesce_counters),
                command_rx,
            )
            .expect("validated emitter inputs must build a relay interaction");
            let context = EmitterSinkContext {
                domain: task_domain.clone(),
                emitter: task_emitter.clone(),
                temp_dir: runtime.temp_dir.clone(),
                events: task_events.clone(),
                udfs,
            };
            let mut publish_backoff = RuntimeReconnectBackoff::default();
            let mut emitter_buffer = EmitterBatchBuffer::new(
                &context,
                &task_flush_each,
                task_max_batch_size.as_deref(),
                buffered_messages.clone(),
            );
            let sink_runtime = SinkEmitterRuntime {
                input_schema: input_schema.clone(),
                buffered_messages,
            };
            let mut sink = SinkEmitter::new_until_cancelled(
                SinkEmitterInit {
                    sink: &task_sink,
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
                retry_schedule.schedule(wait, AckSet::empty(), false);
                runtime.record_emitter_transient_error_with_backoff(
                    &task_domain,
                    &task_emitter,
                    reason,
                    wait,
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
                materialized_state: &task_materialized_state,
                materialized_stream_owner_nodes: &materialized_stream_owner_nodes,
            };
            loop {
                tokio::task::consume_budget().await;
                let wake_at = retry_schedule.deadline(sink.flush_deadline(&emitter_buffer));
                let receive_input = !retry_schedule.is_active()
                    || emitter_buffer_count.load(Ordering::Acquire) == 0;
                let work = match interaction.next_with_input(wake_at, receive_input).await {
                    Ok(work) => work,
                    Err(error) => {
                        let reason = error.to_string();
                        context.report_flush_error(task_sink.label(), &reason);
                        runtime.handle_internal_processor_error_for_acks(
                            &task_domain,
                            "emitter",
                            &task_emitter,
                            &task_error_policies,
                            error.acks(),
                            reason,
                        );
                        continue;
                    }
                };
                let (input_event, _work) = work.into_parts();
                match input_event {
                    RelayInteractionEvent::Command(EmitterTaskCommand::Reconfigure {
                        config,
                        response,
                    }) => {
                        emitter_buffer.reconfigure(
                            &context,
                            &config.flush_each,
                            config.max_batch_size.as_deref(),
                        );
                        sink.reconfigure_flush_policy(
                            &context,
                            &config.flush_each,
                            config.max_batch_size.as_deref(),
                        );
                        let _ = response.send(());
                    }
                    RelayInteractionEvent::Command(EmitterTaskCommand::Stop { response }) => {
                        if emitter_buffer_count.load(Ordering::Acquire) > 0
                            && let Some(reason) =
                                emitter_unavailable_reason(&sink, &fault_injector, &task_emitter)
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
                                    reason.clone(),
                                    MessageErrorOperation::Publish,
                                )
                                .await;
                            let _ =
                                response.send(Err(format!("emitter final flush failed: {reason}")));
                            break;
                        }
                        let mut control = EmitterPublishControl {
                            runtime: &runtime,
                            fault_injector: &fault_injector,
                            backoff: &mut publish_backoff,
                        };
                        let stop_result = match sink
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
                                Ok(())
                            }
                            Ok(None) => {
                                publish_backoff.reset();
                                retry_schedule.clear();
                                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                                Ok(())
                            }
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
                                    .handle_publish_error_batches(
                                        pending,
                                        reason.clone(),
                                        operation,
                                    )
                                    .await;
                                Err(format!("emitter final flush failed: {reason}"))
                            }
                        };
                        let _ = response.send(stop_result);
                        break;
                    }
                    RelayInteractionEvent::ForceFlush(completion) => {
                        if emitter_buffer_count.load(Ordering::Acquire) > 0
                            && let Some(reason) =
                                emitter_unavailable_reason(&sink, &fault_injector, &task_emitter)
                        {
                            retry_schedule.include_acks(sink.pending_acks(&emitter_buffer));
                            let wait = if retry_schedule.is_active() {
                                publish_backoff.next_delay()
                            } else {
                                let wait = publish_backoff.take_next_delay();
                                retry_schedule.schedule(
                                    wait,
                                    sink.pending_acks(&emitter_buffer),
                                    fault_injector.fault_mode(&task_emitter)
                                        == Some(EmitterFaultMode::Stall),
                                );
                                wait
                            };
                            runtime.record_emitter_transient_error_with_backoff(
                                &task_domain,
                                &task_emitter,
                                reason.clone(),
                                wait,
                            );
                            context.report_flush_error(task_sink.label(), &reason);
                            completion.complete();
                            continue;
                        }
                        let mut control = EmitterPublishControl {
                            runtime: &runtime,
                            fault_injector: &fault_injector,
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
                                let wait = publish_backoff.take_next_delay();
                                retry_schedule.schedule(
                                    wait,
                                    sink.pending_acks(&emitter_buffer),
                                    *error.current_context() == EmitterRuntimeError::PublishStalled,
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                runtime.record_emitter_transient_error_with_backoff(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                    wait,
                                );
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
                                emitter_unavailable_reason(&sink, &fault_injector, &task_emitter)
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
                            runtime: &runtime,
                            fault_injector: &fault_injector,
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
                        let stall_cleared = retry_schedule
                            .release_if_stall_cleared(fault_injector.fault_mode(&task_emitter));
                        if !retry_is_due && !stall_cleared {
                            continue;
                        }
                        let retry_attempt = retry_was_active && (retry_is_due || stall_cleared);
                        if reconnect_on_wake || sink.missing_reason().is_some() {
                            sink = SinkEmitter::new_until_cancelled(
                                SinkEmitterInit {
                                    sink: &task_sink,
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
                                let wait = publish_backoff.take_next_delay();
                                retry_schedule.schedule(
                                    wait,
                                    sink.pending_acks(&emitter_buffer),
                                    false,
                                );
                                runtime.record_emitter_transient_error_with_backoff(
                                    &task_domain,
                                    &task_emitter,
                                    reason,
                                    wait,
                                );
                                reconnect_on_wake = true;
                                continue;
                            }
                            reconnect_on_wake = false;
                            runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
                        }
                        let mut control = EmitterPublishControl {
                            runtime: &runtime,
                            fault_injector: &fault_injector,
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
                                let wait = publish_backoff.take_next_delay();
                                retry_schedule.schedule(
                                    wait,
                                    sink.pending_acks(&emitter_buffer),
                                    *error.current_context() == EmitterRuntimeError::PublishStalled,
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                runtime.record_emitter_transient_error_with_backoff(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                    wait,
                                );
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
                        let physical_node_id = runtime.local_node_id.read().clone();
                        runtime
                            .metrics
                            .observe_global_node_received(NodeBatchObservation {
                                domain: &task_domain,
                                kind: ModelKind::Emitter,
                                node: &task_emitter,
                                relay: &input_relay,
                                physical_node_id: physical_node_id.as_deref(),
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
                                .metrics
                                .observe_global_delivery_latency_at_domain_time(
                                    NodeLatencyObservation {
                                        domain: &task_domain,
                                        kind: ModelKind::Emitter,
                                        node: &task_emitter,
                                        relay: &input_relay,
                                        physical_node_id: physical_node_id.as_deref(),
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
                            )
                            .await
                        {
                            Some(batch) => batch,
                            None => continue,
                        };

                        if interaction.is_draining() {
                            let wait = publish_backoff.next_delay();
                            if let Err(error) =
                                emitter_buffer.retain_for_retry(publish_batch.clone(), wait)
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
                            || emitter_unavailable_reason(&sink, &fault_injector, &task_emitter)
                                .is_some()
                        {
                            let unavailable =
                                emitter_unavailable_reason(&sink, &fault_injector, &task_emitter);
                            if let Err(error) = emitter_buffer
                                .retain_for_retry(publish_batch.clone(), Duration::ZERO)
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
                                let wait = publish_backoff.take_next_delay();
                                retry_schedule.schedule(
                                    wait,
                                    sink.pending_acks(&emitter_buffer),
                                    fault_injector.fault_mode(&task_emitter)
                                        == Some(EmitterFaultMode::Stall),
                                );
                                if let Some(reason) = unavailable.as_deref() {
                                    runtime.record_emitter_transient_error_with_backoff(
                                        &task_domain,
                                        &task_emitter,
                                        reason,
                                        wait,
                                    );
                                    context.report_publish_error(task_sink.label(), reason);
                                }
                            }
                            reconnect_on_wake |= sink.missing_reason().is_some();
                            continue;
                        }

                        let mut pending_batch = Some(publish_batch);
                        let mut control = EmitterPublishControl {
                            runtime: &runtime,
                            fault_injector: &fault_injector,
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
                                    .expect("pending emitter batch must exist")
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
                                let wait = publish_backoff.take_next_delay();
                                if let EmitterPublishBatchOwner::Caller = batch_owner
                                    && let Some(batch) = pending_batch.take()
                                    && let Err(retain_error) = emitter_buffer
                                        .retain_for_retry(batch.clone(), Duration::ZERO)
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
                                retry_schedule.schedule(
                                    wait,
                                    sink.pending_acks(&emitter_buffer),
                                    *error.current_context() == EmitterRuntimeError::PublishStalled,
                                );
                                reconnect_on_wake = sink.reconnect_after(&error);
                                let reason = emitter_error_message(&error);
                                runtime.record_emitter_transient_error_with_backoff(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                    wait,
                                );
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
            work_cancel,
            task,
        })
    }
}

fn resolve_emitter_client(
    runtime: &Runtime,
    domain: &Domain,
    sink: &EmitSink,
    client: Option<&Model>,
) -> Result<Option<ResolvedClientConfig>, RuntimeError> {
    let resolved = match (sink, client) {
        (EmitSink::Kafka { .. }, Some(Model::ClientKafka(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::Pulsar { .. }, Some(Model::ClientPulsar(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::RabbitMq { .. }, Some(Model::ClientRabbitMq(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::Redis { .. }, Some(Model::ClientRedis(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::Mqtt { .. }, Some(Model::ClientMqtt(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::Nats { .. }, Some(Model::ClientNats(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::ZeroMq { .. }, Some(Model::ClientZeroMq(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::Sqs { .. }, Some(Model::ClientSqs(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::Sentry { .. }, Some(Model::ClientSentry(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::ClickHouse { .. }, Some(Model::ClientClickHouse(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::Postgres { .. }, Some(Model::ClientPostgres(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::MySql { .. }, Some(Model::ClientMySql(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (EmitSink::MongoDb { .. }, Some(Model::ClientMongoDb(client))) => {
            Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config))
        }
        (
            EmitSink::Iceberg {
                backend: IcebergStorageBackend::S3,
                ..
            },
            Some(Model::ClientS3(client)),
        ) => Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config)),
        (
            EmitSink::Iceberg {
                backend: IcebergStorageBackend::Gcs,
                ..
            },
            Some(Model::ClientGcs(client)),
        ) => Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config)),
        (
            EmitSink::Iceberg {
                backend: IcebergStorageBackend::AzureBlob,
                ..
            },
            Some(Model::ClientAzureBlob(client)),
        ) => Some(runtime.resolve_client_config(client.mount.as_ref(), &client.config)),
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
    domain: &Domain,
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
        .resolve_client_config(client.mount.as_ref(), &client.config)
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
                .metrics
                .observe_global_node_sent(NodeBatchObservation {
                    domain: self.domain,
                    kind: ModelKind::Emitter,
                    node: self.emitter,
                    relay,
                    physical_node_id: self.runtime.local_node_id.read().as_deref(),
                    messages: report.messages,
                    bytes: report.bytes,
                    domain_timestamp: Some(report.domain_timestamp),
                });
        } else {
            self.runtime
                .metrics
                .observe_global_node_without_stream_sent(NodeWithoutRelayObservation {
                    domain: self.domain,
                    kind: ModelKind::Emitter,
                    node: self.emitter,
                    physical_node_id: self.runtime.local_node_id.read().as_deref(),
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
        let messages = match batch.batch.try_into_messages() {
            Ok(messages) => messages,
            Err(error) => {
                let (message, batch) = *error;
                self.runtime.handle_general_error_for_acks(
                    self.domain,
                    "emitter",
                    self.emitter,
                    self.error_policies,
                    batch.acks.iter(),
                    format!("{reason}; {message}"),
                );
                return;
            }
        };
        for message in messages {
            self.runtime
                .handle_structured_message_error(MessageErrorHandling {
                    domain: self.domain,
                    node_kind: "emitter",
                    node: self.emitter,
                    source_route: None,
                    policy: &self.error_policies.message,
                    message,
                    error: structured_message_error(
                        MessageErrorCode::External,
                        reason.clone(),
                        operation,
                        None,
                        std::iter::empty(),
                    ),
                    partial_output: None,
                    materialized_state: HashMap::default(),
                    ingest_metadata: None,
                })
                .await;
        }
    }

    async fn process(
        &self,
        input_relay: &Identifier,
        batch: RelayRecordBatch,
        shutdown_rx: &mut watch::Receiver<bool>,
        wait_for_required_state: bool,
    ) -> Option<EmitterPublishBatch> {
        let dependency_error_acks = batch.acks.clone();
        let batch = match self
            .runtime
            .resolve_materialized_dependencies_for_batch(
                self.domain,
                input_relay,
                self.materialized_state,
                batch,
                shutdown_rx,
                wait_for_required_state,
            )
            .await
        {
            Ok(Some(batch)) => batch,
            Ok(None) => return None,
            Err(error) => {
                self.runtime.handle_internal_processor_error_for_acks(
                    self.domain,
                    "emitter",
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
        let batch = self.filter_source_batch(input_relay, batch).await?;
        let Some(filter_map) = self.filter_map else {
            return Some(EmitterPublishBatch::from_batch(batch));
        };
        let side_inputs = match self
            .runtime
            .load_materialized_side_inputs(
                self.domain,
                &batch.key,
                &filter_map.materialized_interest,
                self.materialized_stream_owner_nodes,
            )
            .await
        {
            Ok(values) => values,
            Err(error) => {
                self.runtime.handle_general_error_for_acks(
                    self.domain,
                    "emitter",
                    self.emitter,
                    self.error_policies,
                    batch.acks.iter(),
                    format!(
                        "emitter '{}' failed to load materialized side inputs: {}",
                        self.emitter.as_str(),
                        error
                    ),
                );
                return None;
            }
        };
        match plan_emitter_filter_map_batch(
            self.emitter,
            filter_map,
            batch,
            self.runtime
                .current_stream_expiration_time(self.domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp),
            &side_inputs,
        )
        .await
        {
            Ok(plan) => {
                self.runtime
                    .handle_planned_message_errors(
                        self.domain,
                        "emitter",
                        self.emitter,
                        self.error_policies,
                        plan.message_errors,
                    )
                    .await;
                let batch = plan.batch?;
                match EmitterPublishBatch::new(batch, plan.headers) {
                    Ok(batch) => Some(batch),
                    Err(error) => {
                        self.runtime.handle_general_error_for_acks(
                            self.domain,
                            "emitter",
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
                    "emitter",
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
        input_relay: &Identifier,
        batch: RelayRecordBatch,
    ) -> Option<RelayRecordBatch> {
        let Some(program) = self.source_filters.get(input_relay) else {
            return Some(batch);
        };
        let side_inputs = match self
            .runtime
            .load_materialized_side_inputs(
                self.domain,
                &batch.key,
                &program.materialized_interest,
                self.materialized_stream_owner_nodes,
            )
            .await
        {
            Ok(values) => values,
            Err(error) => {
                self.runtime.handle_general_error_for_acks(
                    self.domain,
                    "emitter",
                    self.emitter,
                    self.error_policies,
                    batch.acks.iter(),
                    format!(
                        "emitter '{}' failed to load FROM WHERE side inputs for relay '{}': {}",
                        self.emitter.as_str(),
                        input_relay.as_str(),
                        error
                    ),
                );
                return None;
            }
        };
        let plan = match plan_filter_map_messages(
            "emitter",
            self.emitter,
            "FROM WHERE",
            program,
            batch,
            self.runtime
                .current_stream_expiration_time(self.domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp),
            &side_inputs,
        )
        .await
        {
            Ok(plan) => plan,
            Err(error) => {
                self.runtime.handle_general_error_for_acks(
                    self.domain,
                    "emitter",
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
                "emitter",
                self.emitter,
                self.error_policies,
                plan.message_errors,
            )
            .await;
        plan.batch
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use nervix_models::{CreateSchema, ParseAsType};

    use super::*;

    fn input_schema() -> Arc<CompiledSchema> {
        static SCHEMA: OnceLock<Arc<CompiledSchema>> = OnceLock::new();
        let value = Identifier::parse("value").expect("valid field name");
        SCHEMA
            .get_or_init(|| {
                Arc::new(compile_schema(&CreateSchema {
                    name: Identifier::parse("emitter_input").expect("valid schema name"),
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
            RuntimeRecord::from_fields([("value".to_string(), RuntimeValue::I64(value))])
                .with_ingested_at_watermarks(Timestamp::from_unix_nanos(timestamp)),
            acks,
        )
        .expect("valid emitter input batch")
    }

    fn input_batch() -> RelayRecordBatch {
        input_batch_with(1, 0, AckSet::empty())
    }

    fn input_value(batch: &RelayRecordBatch) -> i64 {
        let record = batch.runtime_record(0).expect("batch must contain one row");
        let Some(RuntimeValue::I64(value)) = record.value("value") else {
            panic!("test batch must contain an I64 value")
        };
        *value
    }

    fn sink_context() -> EmitterSinkContext {
        let (events, _) = broadcast::channel(4);
        EmitterSinkContext {
            domain: Domain::parse("emitter_tests").expect("valid domain"),
            emitter: Identifier::parse("output").expect("valid emitter name"),
            temp_dir: Arc::new(PathBuf::new()),
            events,
            udfs: None,
        }
    }

    #[test]
    fn publish_batch_requires_one_header_row_per_record() {
        let batch = input_batch();
        let from_batch = EmitterPublishBatch::from_batch(batch.clone());
        assert!(from_batch.headers.is_none());
        assert_eq!(from_batch.message_count(), 1);
        assert_eq!(
            from_batch.domain_timestamp(),
            Some(Timestamp::from_unix_nanos(0))
        );

        let headers = vec![vec![("route".to_string(), "fast".to_string())]];
        let with_headers = EmitterPublishBatch::new(batch.clone(), Some(headers.clone()))
            .expect("row-aligned headers must build");
        assert_eq!(with_headers.headers.as_ref(), Some(&headers));
        assert_eq!(
            with_headers.estimated_bytes(),
            batch.estimated_bytes() + u64::try_from("routefast".len()).unwrap()
        );

        let error = match EmitterPublishBatch::new(batch, Some(Vec::new())) {
            Err(error) => error,
            Ok(_) => panic!("missing row headers must be rejected"),
        };
        assert!(error.contains("header count 0 does not match row count 1"));
    }

    #[tokio::test]
    async fn publish_batch_ack_helpers_preserve_and_complete_all_roots() {
        let (acks, completion) = AckSet::root();
        let batch = EmitterPublishBatch::from_batch(input_batch_with(1, 0, acks));

        assert!(!batch.merged_acks().is_empty());
        batch.ack_success();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
    }

    #[test]
    fn batch_buffer_rejects_input_without_an_initialized_flush_policy() {
        let mut buffer = EmitterBatchBuffer::default();
        let error = buffer
            .push(EmitterPublishBatch::from_batch(input_batch()))
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
        let first = EmitterPublishBatch::from_batch(input_batch_with(1, 10, AckSet::empty()));
        let second_batch = RelayRecordBatch::from_messages(
            input_schema(),
            vec![
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "value".to_string(),
                        RuntimeValue::I64(2),
                    )])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(20)),
                    acks: AckSet::empty(),
                },
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "value".to_string(),
                        RuntimeValue::I64(3),
                    )])
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
        )
        .expect("headers must align");
        let expected_bytes = first
            .estimated_bytes()
            .saturating_add(second.estimated_bytes());

        assert!(!buffer.push(first).expect("first batch must buffer"));
        assert_eq!(buffer.pending_messages, 1);
        let first_deadline = buffer.deadline().expect("first push must set a deadline");
        assert!(!buffer.push(second).expect("second batch must buffer"));

        assert_eq!(buffer.deadline(), Some(first_deadline));
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
    fn batch_buffer_honors_size_boundary_and_retry_deadline() {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: 1,
        });

        assert!(
            buffer
                .push(EmitterPublishBatch::from_batch(input_batch()))
                .expect("batch must buffer")
        );
        let original = buffer.deadline().expect("push must set a deadline");
        buffer.defer_retry(Duration::from_secs(120));
        let deferred = buffer.deadline().expect("retry must retain a deadline");
        assert!(deferred > original);

        buffer.flush_at = Some(Instant::now());
        assert!(buffer.is_due());
        buffer.flush_at = Some(Instant::now() + Duration::from_secs(60));
        assert!(!buffer.is_due());
    }

    #[test]
    fn forced_retry_ignores_the_ordinary_buffer_deadline() {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        buffer
            .push(EmitterPublishBatch::from_batch(input_batch()))
            .expect("retry batch must buffer");
        assert!(!buffer.is_due());

        assert!(buffer.should_flush(true));
        assert!(!buffer.should_flush(false));
    }

    #[tokio::test]
    async fn retry_wake_attempts_a_buffer_before_its_ordinary_deadline() {
        let runtime = Runtime::default();
        let fault_injector = EmitterFaultInjector::default();
        let mut backoff = RuntimeReconnectBackoff::default();
        let mut control = EmitterPublishControl {
            runtime: &runtime,
            fault_injector: &fault_injector,
            backoff: &mut backoff,
        };
        let context = sink_context();
        let sink_config = EmitSink::Nats {
            client: Identifier::parse("client").expect("valid client name"),
            subject: Identifier::parse("subject").expect("valid subject name"),
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
            .push(EmitterPublishBatch::from_batch(input_batch()))
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
        let mut buffer =
            EmitterBatchBuffer::new(&context, "10s", Some("1MiB"), buffered_messages.clone());
        assert!(buffer.flush_policy.is_some());
        buffer
            .push(EmitterPublishBatch::from_batch(input_batch()))
            .expect("configured buffer must accept input");
        assert_eq!(buffer.pending_messages, 1);
        buffer.reconfigure(&context, "IMMEDIATE", None);
        assert_eq!(buffer.flush_policy, Some(RuntimeFlushPolicy::Immediate));
        assert!(buffer.deadline().is_some());

        let drained = buffer.drain_pending();
        assert_eq!(drained.len(), 1);
        assert!(buffer.is_empty());
        assert_eq!(buffer.pending_bytes, 0);
        assert_eq!(buffer.pending_messages, 0);
        assert!(buffer.deadline().is_none());
        assert_eq!(reported_messages.load(Ordering::Acquire), 0);

        buffer
            .push(EmitterPublishBatch::from_batch(input_batch()))
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
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer
            .push(EmitterPublishBatch::from_batch(input_batch_with(
                1, 0, first_acks,
            )))
            .expect("first batch must buffer");
        buffer
            .push(EmitterPublishBatch::from_batch(input_batch_with(
                2,
                0,
                second_acks,
            )))
            .expect("second batch must buffer");

        assert!(!buffer.pending_acks().is_empty());
        buffer.ack_pending_success();
        assert_eq!(first_completion.wait().await, AckOutcome::Ack);
        assert_eq!(second_completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn retained_batch_clone_owns_exactly_one_attached_ack_share() {
        let (root, mut completion) = AckSet::root();
        let attached = root.attached();
        let batch = EmitterPublishBatch::from_batch(input_batch_with(1, 0, attached));
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);

        buffer
            .retain_for_retry(batch.clone(), Duration::from_secs(1))
            .expect("retry buffer must retain the batch");
        root.ack_success();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), completion.wait_for_progress())
                .await
                .is_err(),
            "the retained attached share must keep the root pending"
        );

        buffer.ack_pending_success();
        buffer.clear();
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        drop(batch);
    }

    #[test]
    fn retry_schedule_preserves_its_deadline_until_a_retry_attempt() {
        let mut retry = EmitterRetrySchedule::default();
        retry.schedule(Duration::from_secs(10), AckSet::empty(), false);
        let retry_at = retry.retry_at.expect("retry must have a deadline");

        assert_eq!(retry.deadline(Some(Instant::now())), Some(retry_at));
        assert_eq!(retry.deadline(Some(Instant::now())), Some(retry_at));
        assert_eq!(retry.retry_at, Some(retry_at));
    }

    #[test]
    fn retry_schedule_releases_a_stall_as_soon_as_the_fault_clears() {
        let mut retry = EmitterRetrySchedule::default();
        retry.schedule(Duration::from_secs(30), AckSet::empty(), true);

        assert!(!retry.release_if_stall_cleared(Some(EmitterFaultMode::Stall)));
        assert!(retry.is_active());
        assert!(retry.release_if_stall_cleared(None));
        assert!(!retry.is_active());
    }

    #[tokio::test]
    async fn retry_schedule_heartbeats_acks_added_by_a_force_drain() {
        let (existing, mut existing_completion) = AckSet::root();
        let (force_drained, mut force_completion) = AckSet::root();
        let mut retry = EmitterRetrySchedule::default();
        retry.schedule(Duration::from_secs(30), existing, false);
        retry.include_acks(force_drained);
        retry.ack_alive_at = Some(Instant::now());

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
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer.buffered_messages = buffered_messages.clone();
        buffer
            .push(EmitterPublishBatch::from_batch(input_batch_with(
                1, 0, first_acks,
            )))
            .expect("first batch must buffer");
        buffer
            .push(EmitterPublishBatch::from_batch(input_batch_with(
                2,
                0,
                second_acks,
            )))
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
        let runtime = Runtime::default();
        let fault_injector = EmitterFaultInjector::default();
        let mut backoff = RuntimeReconnectBackoff::default();
        let mut control = EmitterPublishControl {
            runtime: &runtime,
            fault_injector: &fault_injector,
            backoff: &mut backoff,
        };
        let context = sink_context();
        let sink_config = EmitSink::Nats {
            client: Identifier::parse("client").expect("valid client name"),
            subject: Identifier::parse("subject").expect("valid subject name"),
        };
        let mut sink = SinkEmitter::Missing {
            reason: "test sink intentionally has no client".to_string(),
        };
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Immediate);
        buffer
            .push(EmitterPublishBatch::from_batch(input_batch()))
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
    async fn scheduled_emitter_stop_returns_final_flush_failure_after_task_exit() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let (work_cancel, mut work_cancel_rx) = watch::channel(false);
        let finished = Arc::new(AtomicBool::new(false));
        let task_finished = finished.clone();
        let task = tokio::spawn(async move {
            work_cancel_rx
                .changed()
                .await
                .expect("stop must cancel in-progress emitter work");
            assert!(*work_cancel_rx.borrow());
            let Some(EmitterTaskCommand::Stop { response }) = command_rx.recv().await else {
                panic!("scheduled emitter must receive its stop command")
            };
            let _ = response.send(Err(
                "emitter final flush failed: broker unavailable".to_string()
            ));
            task_finished.store(true, Ordering::Release);
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            work_cancel,
            task,
        };

        let error = scheduled
            .stop()
            .await
            .expect_err("final flush failure must reach the stopping caller");

        assert_eq!(
            error,
            "emitter final flush failed: broker unavailable".to_string()
        );
        assert!(finished.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn scheduled_emitter_stop_aborts_a_task_that_drops_its_response() {
        struct Dropped(Arc<AtomicBool>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let (commands, mut command_rx) = mpsc::channel(1);
        let (work_cancel, _) = watch::channel(false);
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            let _dropped = Dropped(task_dropped);
            let Some(EmitterTaskCommand::Stop { response }) = command_rx.recv().await else {
                panic!("scheduled emitter must receive its stop command")
            };
            drop(response);
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            work_cancel,
            task,
        };

        let error = scheduled
            .stop()
            .await
            .expect_err("a dropped response must fail stopping");

        assert_eq!(
            error,
            "scheduled emitter task dropped its stop response".to_string()
        );
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn scheduled_emitter_stop_timeout_aborts_and_joins_the_task() {
        struct Dropped(Arc<AtomicBool>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let (commands, mut command_rx) = mpsc::channel(1);
        let (work_cancel, _) = watch::channel(false);
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            let _dropped = Dropped(task_dropped);
            let Some(EmitterTaskCommand::Stop { response }) = command_rx.recv().await else {
                panic!("scheduled emitter must receive its stop command")
            };
            let _response = response;
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            work_cancel,
            task,
        };

        let error = scheduled
            .stop_with_grace(Duration::from_millis(5))
            .await
            .expect_err("a missing stop response must time out");

        assert_eq!(
            error,
            "scheduled emitter task timed out stopping".to_string()
        );
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn publish_failure_drains_exactly_the_batches_owned_by_the_buffer() {
        let mut buffer = EmitterBatchBuffer::default();
        buffer.flush_policy = Some(RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        });
        buffer
            .push(EmitterPublishBatch::from_batch(input_batch_with(
                1,
                0,
                AckSet::empty(),
            )))
            .expect("older batch must buffer");
        buffer
            .push(EmitterPublishBatch::from_batch(input_batch_with(
                2,
                0,
                AckSet::empty(),
            )))
            .expect("current clone must buffer");
        let mut current = Some(EmitterPublishBatch::from_batch(input_batch_with(
            2,
            0,
            AckSet::empty(),
        )));
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
            .push(EmitterPublishBatch::from_batch(input_batch_with(
                1,
                0,
                AckSet::empty(),
            )))
            .expect("older batch must buffer");
        let mut current = Some(EmitterPublishBatch::from_batch(input_batch_with(
            2,
            0,
            AckSet::empty(),
        )));
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

    #[tokio::test]
    async fn buffering_does_not_wait_for_sink_fault_until_a_flush_is_required() {
        let runtime = Runtime::default();
        let fault_injector = EmitterFaultInjector::default();
        fault_injector.fail_emitter("output");
        let mut backoff = RuntimeReconnectBackoff::default();
        let mut control = EmitterPublishControl {
            runtime: &runtime,
            fault_injector: &fault_injector,
            backoff: &mut backoff,
        };
        let context = sink_context();
        let client = Identifier::parse("client").expect("valid client name");
        let subject = Identifier::parse("subject").expect("valid subject name");
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
                EmitterPublishBatch::from_batch(input_batch()),
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
        let id = Identifier::parse("target").expect("valid identifier");
        let catalog = IcebergCatalog::Rest { client: id.clone() };
        let sinks = vec![
            (
                EmitSink::Kafka {
                    client: id.clone(),
                    topic: id.clone(),
                },
                "kafka",
            ),
            (
                EmitSink::Pulsar {
                    client: id.clone(),
                    topic: id.clone(),
                },
                "pulsar",
            ),
            (
                EmitSink::RabbitMq {
                    client: id.clone(),
                    queue: id.clone(),
                },
                "rabbitmq",
            ),
            (
                EmitSink::Redis {
                    client: id.clone(),
                    channel: id.clone(),
                },
                "redis",
            ),
            (
                EmitSink::Mqtt {
                    client: id.clone(),
                    topic: id.clone(),
                },
                "mqtt",
            ),
            (
                EmitSink::Nats {
                    client: id.clone(),
                    subject: id.clone(),
                },
                "nats",
            ),
            (EmitSink::ZeroMq { client: id.clone() }, "zeromq"),
            (
                EmitSink::Sqs {
                    client: id.clone(),
                    queue: id.clone(),
                },
                "sqs",
            ),
            (EmitSink::Sentry { client: id.clone() }, "sentry"),
            (
                EmitSink::ClickHouse {
                    client: id.clone(),
                    table: id.clone(),
                    values: Vec::new(),
                    flush_each: "1s".to_string(),
                },
                "clickhouse",
            ),
            (
                EmitSink::Postgres {
                    client: id.clone(),
                    table: id.clone(),
                    values: Vec::new(),
                    conflict_action: PostgresConflictAction::None,
                    max_batch: 1,
                    flush_each: "1s".to_string(),
                },
                "postgres",
            ),
            (
                EmitSink::MySql {
                    client: id.clone(),
                    table: id.clone(),
                    values: Vec::new(),
                    conflict_action: MySqlConflictAction::None,
                    max_batch: 1,
                    flush_each: "1s".to_string(),
                },
                "mysql",
            ),
            (
                EmitSink::MongoDb {
                    client: id.clone(),
                    collection: id.clone(),
                    values: Vec::new(),
                    conflict_action: MongoDbConflictAction::None,
                    max_batch: 1,
                    flush_each: "1s".to_string(),
                },
                "mongodb",
            ),
            (
                EmitSink::Iceberg {
                    backend: IcebergStorageBackend::S3,
                    client: id.clone(),
                    table: id,
                    values: Vec::new(),
                    location: "s3://bucket/table".to_string(),
                    catalog,
                    flush_each: "1s".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
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
        let mut events = context.events.subscribe();

        context.report_init_error("nats", "init failed");
        context.report_publish_error("nats", "publish failed");
        context.report_flush_error("nats", "flush failed");
        assert!(
            context
                .parse_flush_policy_with_max("emitter", "not-a-duration", Some("1MiB"))
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
        let domain = Domain::parse("emitter_tests").expect("valid domain");
        let emitter = Identifier::parse("output").expect("valid emitter name");
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
