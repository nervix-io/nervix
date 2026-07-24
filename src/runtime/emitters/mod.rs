use std::future;

use error_stack::{AttachmentKind, FrameKind, Report};
use thiserror::Error;

use super::*;

pub(in crate::runtime) mod clickhouse;
mod iceberg;
mod kafka;
mod kinesis;
mod mongodb;
mod mqtt;
mod mysql;
mod nats;
mod postgres;
pub(in crate::runtime) mod pulsar;
mod rabbitmq;
mod redis;
mod sqs;
mod zeromq;

use clickhouse::ClickHouseEmitter;
use iceberg::{
    IcebergEmitter, IcebergEmitterClientConfig, IcebergEmitterError, IcebergEmitterInit,
    IcebergEmitterResult,
};
use kafka::KafkaEmitter;
use kinesis::KinesisEmitter;
use mongodb::MongoDbEmitter;
use mqtt::MqttEmitter;
use mysql::MySqlEmitter;
use nats::NatsEmitter;
use postgres::PostgresEmitter;
use pulsar::PulsarEmitter;
use rabbitmq::RabbitMqEmitter;
use redis::RedisEmitter;
use sqs::SqsEmitter;
use zeromq::ZeroMqEmitter;

pub(in crate::runtime) struct EmitterTask;

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
    shutdown_rx: &'a mut watch::Receiver<bool>,
    backoff: &'a mut RuntimeReconnectBackoff,
}

struct EmitterBatchContext<'a> {
    runtime: &'a Runtime,
    domain: &'a Domain,
    emitter: &'a Identifier,
    input_relay: &'a Identifier,
    error_policies: &'a ErrorPolicies,
    filter_map: Option<&'a CompiledEmitterFilterMapProgram>,
    materialized_state: &'a [nervix_models::MaterializedStateDependency],
    materialized_stream_owner_nodes: &'a HashMap<Identifier, Option<String>>,
    schema: Arc<CompiledSchema>,
}

#[derive(Clone)]
struct EmitterPublishBatch {
    batch: RelayRecordBatch,
    headers: Vec<EmitterHeaders>,
}

impl EmitterPublishBatch {
    fn from_batch(batch: RelayRecordBatch) -> Self {
        let header_count = batch.records.len();
        Self {
            batch,
            headers: vec![Vec::new(); header_count],
        }
    }

    fn new(batch: RelayRecordBatch, headers: Vec<EmitterHeaders>) -> Result<Self, String> {
        if batch.records.len() != headers.len() {
            return Err(format!(
                "emitter header count {} does not match row count {}",
                headers.len(),
                batch.records.len()
            ));
        }
        Ok(Self { batch, headers })
    }

    fn estimated_bytes(&self) -> u64 {
        self.batch.estimated_bytes().saturating_add(
            self.headers
                .iter()
                .flatten()
                .map(|(name, value)| {
                    u64::try_from(name.len())
                        .unwrap_or(u64::MAX)
                        .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
                })
                .fold(0_u64, u64::saturating_add),
        )
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
    #[error("failed to encode emitter batch")]
    EncodeBatch,
    #[error("failed to publish emitter batch")]
    PublishBatch,
}

impl EmitterRuntimeError {
    fn is_retryable_publish_failure(self) -> bool {
        match self {
            Self::SinkNotInitialized | Self::PublishBatch => true,
            Self::FlushPolicyNotInitialized
            | Self::InvalidSinkConfig
            | Self::InitializeSink
            | Self::FaultInjected
            | Self::ShutdownWhileStalled
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
}

#[derive(Default)]
struct EmitterBatchBuffer {
    flush_policy: Option<RuntimeFlushPolicy>,
    pending: Vec<EmitterPublishBatch>,
    pending_bytes: u64,
    flush_at: Option<Instant>,
}

impl EmitterBatchBuffer {
    fn new(context: &EmitterSinkContext, flush_each: &str, max_batch_size: Option<&str>) -> Self {
        Self {
            flush_policy: context.parse_flush_policy_with_max(
                "emitter",
                flush_each,
                max_batch_size,
            ),
            pending: Vec::new(),
            pending_bytes: 0,
            flush_at: None,
        }
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
        self.pending_bytes = self.pending_bytes.saturating_add(batch.estimated_bytes());
        self.pending.push(batch);
        let should_flush = match flush_policy {
            RuntimeFlushPolicy::Immediate => true,
            RuntimeFlushPolicy::Each {
                interval,
                max_batch_size,
            } => {
                if self.flush_at.is_none() {
                    self.flush_at = Some(Instant::now() + interval);
                }
                self.pending_bytes >= max_batch_size
            }
        };
        Ok(should_flush)
    }

    fn is_due(&self) -> bool {
        self.flush_at
            .is_some_and(|deadline| deadline <= Instant::now())
    }

    fn defer_retry(&mut self, delay: Duration) {
        if !self.pending.is_empty() {
            self.flush_at = Some(Instant::now() + delay);
        }
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
        self.pending_bytes = 0;
        self.flush_at = None;
        pending
    }

    fn clear(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
        self.flush_at = None;
    }

    fn report(&self) -> Option<PublishReport> {
        if self.pending.is_empty() {
            return None;
        }
        let messages = self
            .pending
            .iter()
            .map(EmitterPublishBatch::message_count)
            .fold(0_u64, u64::saturating_add);
        let bytes = self.pending_bytes;
        let domain_timestamp = self
            .pending
            .iter()
            .filter_map(EmitterPublishBatch::domain_timestamp)
            .max()
            .unwrap_or_else(current_timestamp);
        Some(PublishReport::flushed(messages, bytes, domain_timestamp))
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
    let records = augment_runtime_records_with_branch_keys(batch.records.clone(), &batch.keys)
        .map_err(|error| Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(error))?;
    let input = vm_typed_batch_from_runtime_records(&records, &program.program.input_schema)
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
    if result.batch.row_count() != batch.records.len() {
        return Err(
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                "{} VALUES produced {} rows for {} input records",
                program.label,
                result.batch.row_count(),
                batch.records.len()
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
    let mut rows = Vec::with_capacity(batch.records.len());
    for row in 0..batch.records.len() {
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
    Kinesis(KinesisEmitter),
    RabbitMq(RabbitMqEmitter),
    Redis(RedisEmitter),
    Mqtt(MqttEmitter),
    Nats(NatsEmitter),
    ZeroMq(ZeroMqEmitter),
    Sqs(SqsEmitter),
    ClickHouse(ClickHouseEmitter),
    Postgres(PostgresEmitter),
    MySql(MySqlEmitter),
    MongoDb(MongoDbEmitter),
    Iceberg(IcebergEmitter),
    Missing { reason: String },
}

impl SinkEmitter {
    async fn new(
        sink: &EmitSink,
        client: Option<&Model>,
        resolved: Option<&ResolvedClientConfig>,
        catalog_client: Option<&Model>,
        catalog_resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
        input_schema: Arc<CompiledSchema>,
    ) -> Self {
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
            (EmitSink::Kinesis { .. }, Some(Model::ClientKinesis(client)), _) => {
                match KinesisEmitter::new(client, resolved).await {
                    Ok(emitter) => Self::Kinesis(emitter),
                    Err(error) => {
                        Self::missing_after_emitter_init_error("kinesis", context, &error)
                    }
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
            (EmitSink::Mqtt { .. }, Some(Model::ClientMqtt(client)), _) => {
                Self::from_result("mqtt", context, MqttEmitter::new(client, resolved, context))
                    .map(Self::Mqtt)
            }
            (EmitSink::Nats { .. }, Some(Model::ClientNats(client)), _) => {
                match NatsEmitter::new(client, resolved).await {
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
            (EmitSink::Sqs { .. }, Some(Model::ClientSqs(client)), _) => {
                match SqsEmitter::new(client, resolved).await {
                    Ok(emitter) => Self::Sqs(emitter),
                    Err(error) => Self::missing_after_emitter_init_error("sqs", context, &error),
                }
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
        match self {
            Self::Iceberg(emitter) => emitter.flush_deadline(),
            _ => buffer.deadline(),
        }
    }

    fn missing_reason(&self) -> Option<&str> {
        if let Self::Missing { reason } = self {
            Some(reason.as_str())
        } else {
            None
        }
    }

    async fn flush_due(
        &mut self,
        sink: &EmitSink,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        codec: Option<Arc<CompiledCodec>>,
        buffer: &mut EmitterBatchBuffer,
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        if let Self::Iceberg(emitter) = self
            && let EmitSink::Iceberg { .. } = sink
        {
            return match emitter.flush_due().await {
                Ok(published) => Ok(published),
                Err(error) => {
                    let message = iceberg_error_message(&error);
                    context.report_flush_error(sink.label(), &message);
                    Err(Report::new(EmitterRuntimeError::PublishBatch).attach_printable(message))
                }
            };
        }
        if !buffer.is_due() {
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
    ) -> Option<PublishReport> {
        if let EmitSink::Iceberg { .. } = sink
            && let Self::Iceberg(emitter) = self
        {
            return match emitter.finish().await {
                Ok(published) => published,
                Err(error) => {
                    let message = iceberg_error_message(&error);
                    context.report_flush_error(sink.label(), &message);
                    None
                }
            };
        } else {
            return match self
                .flush_buffer(sink, context, control, codec, buffer)
                .await
            {
                Ok(published) => published,
                Err(error) => {
                    let message = emitter_error_message(&error);
                    context.report_flush_error(sink.label(), &message);
                    None
                }
            };
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
    ) -> EmitterRuntimeResult<Option<PublishReport>> {
        self.wait_for_fault_injector(context, control, &batch.merged_acks())
            .await?;
        if let (Self::Iceberg(emitter), EmitSink::Iceberg { .. }) = (&mut *self, sink) {
            return match emitter.publish_batch(batch.batch).await {
                Ok(report) => Ok(report),
                Err(error) if error.current_context().is_retryable_publish_failure() => {
                    Err(Report::new(EmitterRuntimeError::PublishBatch)
                        .attach_printable(iceberg_error_message(&error)))
                }
                Err(error) => {
                    let message = iceberg_error_message(&error);
                    context.report_flush_error("iceberg", &message);
                    Ok(None)
                }
            };
        }

        if buffer.push(batch)? {
            self.flush_buffer(sink, context, control, codec, buffer)
                .await
        } else {
            Ok(None)
        }
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
        for batch in batches {
            for ((record, key), headers) in batch
                .batch
                .records
                .iter()
                .zip(batch.batch.keys.iter())
                .zip(batch.headers.iter())
            {
                let payload = encode_emitted_payload(codec.clone(), record.clone())
                    .await
                    .map_err(|error| {
                        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                            "emitter '{}' failed to encode message: {error}",
                            context.emitter.as_str()
                        ))
                    })?;
                self.publish_encoded_payload(sink, key, record, &payload, headers)
                    .await?;
                trace!(
                    domain = context.domain.as_str(),
                    emitter = context.emitter.as_str(),
                    key = branch_key_display(key),
                    payload = String::from_utf8_lossy(&payload).to_string(),
                    "emitter published message"
                );
            }
        }
        Ok(())
    }

    async fn publish_encoded_payload(
        &mut self,
        sink: &EmitSink,
        key: &Option<BranchKey>,
        record: &RuntimeRecord,
        payload: &[u8],
        headers: &EmitterHeaders,
    ) -> EmitterRuntimeResult<()> {
        match (self, sink) {
            (Self::Kafka(emitter), EmitSink::Kafka { topic, .. }) => {
                let message = RelayMessage {
                    key: key.clone(),
                    record: record.clone(),
                    acks: AckSet::empty(),
                };
                emitter.publish(topic, &message, payload, headers).await
            }
            (Self::Pulsar(emitter), EmitSink::Pulsar { .. }) => {
                let message = RelayMessage {
                    key: key.clone(),
                    record: record.clone(),
                    acks: AckSet::empty(),
                };
                emitter.publish(&message, payload, headers).await
            }
            (Self::Kinesis(emitter), EmitSink::Kinesis { relay, .. }) => {
                let message = RelayMessage {
                    key: key.clone(),
                    record: record.clone(),
                    acks: AckSet::empty(),
                };
                emitter.publish(relay, &message, payload).await
            }
            (Self::RabbitMq(emitter), EmitSink::RabbitMq { queue, .. }) => {
                emitter.publish(queue, payload, headers).await
            }
            (Self::Redis(emitter), EmitSink::Redis { channel, .. }) => {
                emitter.publish(channel, payload).await
            }
            (Self::Mqtt(emitter), EmitSink::Mqtt { topic, .. }) => {
                emitter.publish(topic, payload).await
            }
            (Self::Nats(emitter), EmitSink::Nats { subject, .. }) => {
                emitter.publish(subject, payload, headers).await
            }
            (Self::ZeroMq(emitter), EmitSink::ZeroMq { .. }) => emitter.publish(payload).await,
            (Self::Sqs(emitter), EmitSink::Sqs { queue, .. }) => {
                emitter.publish(queue, payload, headers).await
            }
            _ => Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("emitter has no initialized sink client")),
        }
    }

    async fn wait_for_fault_injector(
        &self,
        context: &EmitterSinkContext,
        control: &mut EmitterPublishControl<'_>,
        acks: &AckSet,
    ) -> EmitterRuntimeResult<()> {
        loop {
            tokio::task::consume_budget().await;
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
                    return Err(
                        Report::new(EmitterRuntimeError::FaultInjected).attach_printable(reason)
                    );
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
                        "fault injector stalled emitter '{}' in domain '{}' before reconnect \
                         retry in {}",
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
                    if !control
                        .backoff
                        .wait_with_ack_alive(control.shutdown_rx, acks)
                        .await
                    {
                        return Err(Report::new(EmitterRuntimeError::ShutdownWhileStalled));
                    }
                }
                None => {
                    control.backoff.reset();
                    return Ok(());
                }
            }
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
            EmitSink::Kinesis { .. } => "kinesis",
            EmitSink::RabbitMq { .. } => "rabbitmq",
            EmitSink::Redis { .. } => "redis",
            EmitSink::Mqtt { .. } => "mqtt",
            EmitSink::Nats { .. } => "nats",
            EmitSink::ZeroMq { .. } => "zeromq",
            EmitSink::Sqs { .. } => "sqs",
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
        receiver: RelayRuntimeFanIn,
    ) -> Result<JoinHandle<()>, RuntimeError> {
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
            input_branching_schema,
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
                current_branch_schema: input_branching_schema.as_ref(),
                current_branch_sensitivity: None,
                udfs: udfs.as_ref(),
            },
        )?;
        let client = clients.get(emitter.sink.client()).cloned();
        let catalog_client = emitter
            .sink
            .iceberg_catalog_client()
            .and_then(|client| clients.get(client))
            .cloned();
        let task_domain = domain.clone();
        let task_emitter = emitter.name.clone();
        let task_from_relay = emitter.from_relay.clone();
        let task_sink = emitter.sink.clone();
        let task_flush_each = emitter.flush_each.clone();
        let task_max_batch_size = emitter.max_batch_size.clone();
        let task_error_policies = emitter.error_policies.clone();
        let task_materialized_state = emitter.materialized_state.clone();
        let task_events = runtime.events.clone();
        let fault_injector = runtime.emitter_faults.clone();
        let runtime = runtime.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let resolved_client =
            resolve_emitter_client(&runtime, domain, &emitter.sink, client.as_deref())?;
        let resolved_catalog_client = resolve_emitter_catalog_client(
            &runtime,
            domain,
            &emitter.sink,
            catalog_client.as_deref(),
        )?;

        Ok(tokio::spawn(async move {
            let _client_mounts = resolved_client
                .as_ref()
                .and_then(|config| config.mounts.clone());
            let mut input = receiver;
            let context = EmitterSinkContext {
                domain: task_domain.clone(),
                emitter: task_emitter.clone(),
                temp_dir: runtime.temp_dir.clone(),
                events: task_events.clone(),
                udfs,
            };
            let mut publish_backoff = RuntimeReconnectBackoff::default();
            let mut emitter_buffer =
                EmitterBatchBuffer::new(&context, &task_flush_each, task_max_batch_size.as_deref());
            let mut sink = SinkEmitter::new(
                &task_sink,
                client.as_deref(),
                resolved_client.as_ref(),
                catalog_client.as_deref(),
                resolved_catalog_client.as_ref(),
                &context,
                input_schema.clone(),
            )
            .await;
            if let Some(reason) = sink.missing_reason() {
                runtime.record_emitter_transient_error_with_backoff(
                    &task_domain,
                    &task_emitter,
                    reason,
                    publish_backoff.next_delay(),
                );
            } else {
                runtime.clear_emitter_transient_error(&task_domain, &task_emitter);
            }
            let batch_context = EmitterBatchContext {
                runtime: &runtime,
                domain: &task_domain,
                emitter: &task_emitter,
                input_relay: &task_from_relay,
                error_policies: &task_error_policies,
                filter_map: filter_map.as_ref(),
                materialized_state: &task_materialized_state,
                materialized_stream_owner_nodes: &materialized_stream_owner_nodes,
                schema: output_compiled_schema.clone(),
            };

            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            let mut control = EmitterPublishControl {
                                runtime: &runtime,
                                fault_injector: &fault_injector,
                                shutdown_rx: &mut shutdown_rx,
                                backoff: &mut publish_backoff,
                            };
                            let _ = sink
                                .flush_all(
                                    &task_sink,
                                    &context,
                                    &mut control,
                                    codec.clone(),
                                    &mut emitter_buffer,
                                )
                                .await;
                            break;
                        }
                    }
                    _ = async {
                        if let Some(deadline) = sink.flush_deadline(&emitter_buffer) {
                            sleep_until(deadline).await;
                        } else {
                            future::pending::<()>().await;
                        }
                    } => {
                        let mut control = EmitterPublishControl {
                            runtime: &runtime,
                            fault_injector: &fault_injector,
                            shutdown_rx: &mut shutdown_rx,
                            backoff: &mut publish_backoff,
                        };
                        match sink
                            .flush_due(
                                &task_sink,
                                &context,
                                &mut control,
                                codec.clone(),
                                &mut emitter_buffer,
                            )
                            .await {
                            Ok(Some(report)) => {
                                publish_backoff.reset();
                                runtime.clear_emitter_transient_error(
                                    &task_domain,
                                    &task_emitter,
                                );
                                runtime.metrics.observe_global_node_sent(NodeBatchObservation {
                                    domain: &task_domain,
                                    kind: ModelKind::Emitter,
                                    node: &task_emitter,
                                    relay: &task_from_relay,
                                    physical_node_id: runtime.local_node_id.read().as_deref(),
                                    messages: report.messages,
                                    bytes: report.bytes,
                                    domain_timestamp: Some(report.domain_timestamp),
                                });
                                runtime.mark_branch_aggregated_metrics_updated(
                                    &task_domain,
                                    ModelKind::Emitter,
                                    &task_emitter,
                                );
                            }
                            Ok(None) => {}
                            Err(error) if emitter_publish_error_is_retryable(&error) => {
                                let reason = emitter_error_message(&error);
                                let wait = publish_backoff.next_delay();
                                emitter_buffer.defer_retry(wait);
                                runtime.record_emitter_transient_error_with_backoff(
                                    &task_domain,
                                    &task_emitter,
                                    reason.clone(),
                                    wait,
                                );
                                context.report_flush_error(task_sink.label(), &reason);
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
                                    .handle_publish_error_batches(pending, reason, operation)
                                    .await;
                            }
                        }
                    }
                    message = input.recv() => {
                        let batch = match message {
                            Some(batch) => batch,
                            None => {
                                let mut control = EmitterPublishControl {
                                    runtime: &runtime,
                                    fault_injector: &fault_injector,
                                    shutdown_rx: &mut shutdown_rx,
                                    backoff: &mut publish_backoff,
                                };
                                let _ = sink
                                    .flush_all(
                                        &task_sink,
                                        &context,
                                        &mut control,
                                        codec.clone(),
                                        &mut emitter_buffer,
                                    )
                                    .await;
                                break;
                            }
                        };
                        runtime.metrics.observe_global_node_received(NodeBatchObservation {
                            domain: &task_domain,
                            kind: ModelKind::Emitter,
                            node: &task_emitter,
                            relay: &task_from_relay,
                            physical_node_id: runtime.local_node_id.read().as_deref(),
                            messages: batch.message_count(),
                            bytes: batch.estimated_bytes(),
                            domain_timestamp: batch.domain_timestamp(),
                        });
                        runtime.mark_branch_aggregated_metrics_updated(
                            &task_domain,
                            ModelKind::Emitter,
                            &task_emitter,
                        );
                        let delivery_latencies = batch.delivery_latency_seconds(current_timestamp());
                        for seconds in delivery_latencies {
                            runtime.metrics.observe_global_delivery_latency_at_domain_time(
                                NodeLatencyObservation {
                                    domain: &task_domain,
                                    kind: ModelKind::Emitter,
                                    node: &task_emitter,
                                    relay: &task_from_relay,
                                    physical_node_id: runtime.local_node_id.read().as_deref(),
                                    seconds,
                                    domain_timestamp: batch.domain_timestamp(),
                                },
                            );
                            runtime.mark_branch_aggregated_metrics_updated(
                                &task_domain,
                                ModelKind::Emitter,
                                &task_emitter,
                            );
                        }
                        let publish_batch = match batch_context
                            .process(batch, &mut shutdown_rx)
                            .await
                        {
                            Some(batch) => batch,
                            None => continue,
                        };

                        {
                            let mut pending_batch = Some(publish_batch);
                            loop {
                                tokio::task::consume_budget().await;
                                let batch = pending_batch
                                    .as_ref()
                                    .or_else(|| emitter_buffer.pending.first())
                                    .expect("pending emitter batch must exist");
                                let batch_acks = batch.merged_acks();
                                while let Some(reason) =
                                    sink.missing_reason().map(str::to_owned)
                                {
                                    tokio::task::consume_budget().await;
                                    let wait = publish_backoff.next_delay();
                                    runtime.record_emitter_transient_error_with_backoff(
                                        &task_domain,
                                        &task_emitter,
                                        &reason,
                                        wait,
                                    );
                                    if !publish_backoff
                                        .wait_with_ack_alive(&mut shutdown_rx, &batch_acks)
                                        .await
                                    {
                                        break;
                                    }
                                    sink = SinkEmitter::new(
                                        &task_sink,
                                        client.as_deref(),
                                        resolved_client.as_ref(),
                                        catalog_client.as_deref(),
                                        resolved_catalog_client.as_ref(),
                                        &context,
                                        input_schema.clone(),
                                    )
                                    .await;
                                    if sink.missing_reason().is_none() {
                                        publish_backoff.reset();
                                        runtime.clear_emitter_transient_error(
                                            &task_domain,
                                            &task_emitter,
                                        );
                                    }
                                }
                                if sink.missing_reason().is_some() {
                                    break;
                                }
                                let mut control = EmitterPublishControl {
                                    runtime: &runtime,
                                    fault_injector: &fault_injector,
                                    shutdown_rx: &mut shutdown_rx,
                                    backoff: &mut publish_backoff,
                                };
                                let publish_result = if let Some(batch) = pending_batch.as_ref() {
                                    sink.publish_batch(
                                        &task_sink,
                                        &context,
                                        &mut control,
                                        codec.clone(),
                                        &mut emitter_buffer,
                                        batch.clone(),
                                    )
                                    .await
                                } else {
                                    sink.flush_buffer(
                                        &task_sink,
                                        &context,
                                        &mut control,
                                        codec.clone(),
                                        &mut emitter_buffer,
                                    )
                                    .await
                                };
                                match publish_result {
                                    Ok(Some(report)) => {
                                        publish_backoff.reset();
                                        runtime.clear_emitter_transient_error(
                                            &task_domain,
                                            &task_emitter,
                                        );
                                        runtime.metrics.observe_global_node_sent(
                                            NodeBatchObservation {
                                                domain: &task_domain,
                                                kind: ModelKind::Emitter,
                                                node: &task_emitter,
                                                relay: &task_from_relay,
                                                physical_node_id: runtime
                                                    .local_node_id
                                                    .read()
                                                    .as_deref(),
                                                messages: report.messages,
                                                bytes: report.bytes,
                                                domain_timestamp: Some(report.domain_timestamp),
                                            },
                                        );
                                        runtime.mark_branch_aggregated_metrics_updated(
                                            &task_domain,
                                            ModelKind::Emitter,
                                            &task_emitter,
                                        );
                                        pending_batch.take();
                                        break;
                                    }
                                    Ok(None) => {
                                        pending_batch.take();
                                        break;
                                    }
                                    Err(error) if emitter_publish_error_is_retryable(&error) => {
                                        if !emitter_buffer.is_empty() {
                                            pending_batch.take();
                                        }
                                        let reason = emitter_error_message(&error);
                                        let wait = publish_backoff.next_delay();
                                        runtime.record_emitter_transient_error_with_backoff(
                                            &task_domain,
                                            &task_emitter,
                                            reason.clone(),
                                            wait,
                                        );
                                        context.report_publish_error(task_sink.label(), &reason);
                                        if !publish_backoff
                                            .wait_with_ack_alive(&mut shutdown_rx, &batch_acks)
                                            .await
                                        {
                                            if let Some(batch) = pending_batch.take() {
                                                runtime.handle_general_error_for_acks(
                                                    &task_domain,
                                                    "emitter",
                                                    &task_emitter,
                                                    &task_error_policies,
                                                    batch.batch.acks.iter(),
                                                    reason,
                                                );
                                            } else {
                                                let pending_acks = emitter_buffer
                                                    .pending
                                                    .iter()
                                                    .flat_map(|batch| {
                                                        batch.batch.acks.iter().cloned()
                                                    })
                                                    .collect::<Vec<_>>();
                                                runtime.handle_general_error_for_acks(
                                                    &task_domain,
                                                    "emitter",
                                                    &task_emitter,
                                                    &task_error_policies,
                                                    pending_acks.iter(),
                                                    reason,
                                                );
                                                emitter_buffer.clear();
                                            }
                                            break;
                                        }
                                        sink = SinkEmitter::new(
                                            &task_sink,
                                            client.as_deref(),
                                            resolved_client.as_ref(),
                                            catalog_client.as_deref(),
                                            resolved_catalog_client.as_ref(),
                                            &context,
                                            input_schema.clone(),
                                        )
                                        .await;
                                    }
                                    Err(error) => {
                                        let reason = emitter_error_message(&error);
                                        runtime.record_emitter_transient_error(
                                            &task_domain,
                                            &task_emitter,
                                            reason.clone(),
                                        );
                                        context.report_publish_error(task_sink.label(), &reason);
                                        if let Some(batch) = pending_batch.take() {
                                            let operation = emitter_message_error_operation(
                                                &error,
                                                codec.is_some(),
                                            );
                                            batch_context
                                                .handle_publish_error_batch(
                                                    batch,
                                                    reason,
                                                    operation,
                                                )
                                                .await;
                                        } else {
                                            let pending = emitter_buffer.drain_pending();
                                            let operation = emitter_message_error_operation(
                                                &error,
                                                codec.is_some(),
                                            );
                                            batch_context
                                                .handle_publish_error_batches(
                                                    pending,
                                                    reason,
                                                    operation,
                                                )
                                                .await;
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }))
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
        (EmitSink::Kinesis { .. }, Some(Model::ClientKinesis(client))) => {
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
        batch: RelayRecordBatch,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> Option<EmitterPublishBatch> {
        let dependency_error_acks = batch.acks.clone();
        let batch = match self
            .runtime
            .resolve_materialized_dependencies_for_batch(
                self.domain,
                self.input_relay,
                self.materialized_state,
                batch,
                shutdown_rx,
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
        match plan_emitter_filter_map_messages(
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
                if plan.messages.is_empty() {
                    return None;
                }
                match RelayRecordBatch::from_messages(self.schema.clone(), plan.messages) {
                    Ok(batch) => match EmitterPublishBatch::new(batch, plan.headers) {
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
                    },
                    Err(error) => {
                        self.runtime.handle_general_error_for_acks(
                            self.domain,
                            "emitter",
                            self.emitter,
                            self.error_policies,
                            std::iter::empty::<&AckSet>(),
                            format!(
                                "emitter '{}' failed to build filtered arrow batch: {}",
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
}
