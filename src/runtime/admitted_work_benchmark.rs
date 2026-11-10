//! Opaque drivers for measuring data-plane work the node admits through its bounded executor:
//! preparing a branched entrypoint input, encoding an emitter's rows through a codec
//! transformation, and flushing those rows through the production emitter host.
//!
//! Layer: test harness.
//! - **Owns.** Opaque benchmark drivers and their initialized runtime contexts.
//! - **Depends on.** Runtime execution and its typed codec inputs.
//! - **Must not know.** Live graph placement or control-plane transaction ownership.
//!
//! This module only exists with the `benchmarks` feature. Its public surface exposes benchmark
//! operations and what they produced, never Nervix runtime carriers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    CodecJaqFormat, CodecJaqTransformations, CodecWireFormat, CreateCodec, CreateSchema,
    DomainClockAuthority, DomainConfig, DomainName, DomainNodeRef, DomainPace, DomainStartPoint,
    DomainState, DomainStatus, EmitterName, ErrorPolicies, FieldName, ModelKind, ParseAsType,
    ResolvedCodecWireFormat, SchemaName, Timestamp,
};

use super::{
    BranchInstanceAckBoundary, BranchKey, CompiledCodec, DomainClockLifecycle,
    DomainRoutingSnapshot, EmitterConnectorContext, EmitterPublishBatch, Executor, RelayMessage,
    RelayRecordBatch, Runtime, emitter_encoding::encode_pending_broker_payloads,
    prepare_branched_entrypoint_input,
};
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{
        CompiledSchema, RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow, RuntimeValue,
        compile_codec, compile_schema,
    },
};

/// The runtime a driver blocks on. The executor's jobs run on its blocking pool, as on a node.
fn benchmark_runtime() -> nervix_primitives::runtime::Runtime {
    nervix_primitives::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .assured("a two-worker runtime builds on every supported host")
}

fn identifier<T>(raw: &str) -> T
where
    T: for<'a> TryFrom<&'a str>,
{
    match T::try_from(raw) {
        Ok(identifier) => identifier,
        Err(_) => panic!("benchmark identifier '{raw}' does not satisfy the identifier grammar"),
    }
}

/// A `tenant` string and an I64 `value`, the shape both drivers carry.
fn benchmark_schema() -> nervix_primitives::sync::Arc<CompiledSchema> {
    let field = |name: &str, ty: ParseAsType| nervix_models::SchemaField {
        name: identifier::<FieldName>(name),
        ty,
        optional: false,
        sensitive: false,
    };
    nervix_primitives::sync::Arc::new(compile_schema(&CreateSchema {
        name: identifier::<SchemaName>("admitted_work_benchmark"),
        fields: vec![
            field("tenant", ParseAsType::String),
            field("value", ParseAsType::I64),
        ],
    }))
}

/// `rows` rows spread evenly over `branches` tenants, one relay batch per tenant, in tenant order.
fn tenant_batches(
    schema: &nervix_primitives::sync::Arc<CompiledSchema>,
    rows: usize,
    branches: usize,
) -> Vec<RelayRecordBatch> {
    let per_branch = rows
        .checked_div(branches)
        .assured("a benchmark has at least one branch");
    let mut batches = Vec::with_capacity(branches);
    for branch in 0..branches {
        let tenant = format!("tenant_{branch}");
        let key = BranchKey::from_fields([(
            identifier::<FieldName>("tenant"),
            RuntimeValue::String(tenant.clone()),
        )])
        .assured("a tenant key has one field");
        let values = (0..per_branch)
            .map(|row| i64::try_from(row).assured("a benchmark batch holds far fewer rows"))
            .collect::<Vec<_>>();
        let record_batch = arrow_array::RecordBatch::try_new(
            schema.arrow_schema(),
            vec![
                nervix_primitives::sync::StdArc::new(arrow_array::StringArray::from(vec![
                    tenant.as_str();
                    per_branch
                ])),
                nervix_primitives::sync::StdArc::new(arrow_array::Int64Array::from(values)),
            ],
        )
        .assured("the columns are built here for the schema declared beside them");
        let runtime_batch = nervix_primitives::sync::Arc::new(
            RuntimeRecordBatch::from_record_batch(schema.arrow_schema(), record_batch)
                .assured("the batch was built for this schema"),
        );
        let watermark = Timestamp::from_unix_nanos(1);
        let mut messages = Vec::with_capacity(per_branch);
        for row in 0..per_branch {
            messages.push(RelayMessage {
                key: Some(key.clone()),
                record: RuntimeRow::new(
                    nervix_primitives::sync::Arc::clone(&runtime_batch),
                    row,
                    RuntimeRecordMetadata::from_ingested_at_watermarks(watermark, watermark),
                )
                .assured("the row index is inside the batch built above"),
                acks: AckSet::empty(),
            });
        }
        batches.push(
            RelayRecordBatch::from_messages(nervix_primitives::sync::Arc::clone(schema), messages)
                .assured("every message of one tenant shares its key and the schema"),
        );
    }
    batches
}

/// One branched entrypoint input, prepared into its branch batches through the executor as an
/// ingestor's route task prepares every input it forwards.
pub struct BranchedInputBenchmark {
    runtime: nervix_primitives::runtime::Runtime,
    executor: Executor,
    input: RelayRecordBatch,
}

impl BranchedInputBenchmark {
    /// An input of `rows` rows spread evenly over `branches` branch keys.
    pub fn new(rows: usize, branches: usize) -> Self {
        let schema = benchmark_schema();
        let input = RelayRecordBatch::concat(tenant_batches(&schema, rows, branches))
            .assured("the tenant batches share one schema");
        Self {
            runtime: benchmark_runtime(),
            executor: Executor::default(),
            input,
        }
    }

    /// Prepare one copy of the input, and answer how many branch batches it produced.
    pub fn prepare(&self) -> usize {
        let prepared = self.runtime.block_on(prepare_branched_entrypoint_input(
            &self.executor,
            self.input.clone(),
            BranchInstanceAckBoundary::Preserve,
        ));
        match prepared {
            Ok(branches) => branches.len(),
            Err(_) => panic!("the benchmark input prepares into its branches"),
        }
    }
}

/// One emitter batch whose every row is encoded through an `ON EMITTING` JAQ program, as a record
/// emitter's rows are before they are published.
pub struct TransformedEncodingBenchmark {
    runtime: nervix_primitives::runtime::Runtime,
    context: EmitterConnectorContext,
    codec: nervix_primitives::sync::Arc<CompiledCodec>,
    batch: EmitterPublishBatch,
    rows: Vec<usize>,
}

impl TransformedEncodingBenchmark {
    /// A batch of `rows` rows of one tenant, encoded with an identity transformation.
    pub fn new(rows: usize) -> Self {
        Self::with_runtime(rows, benchmark_runtime)
    }

    fn with_runtime(
        rows: usize,
        build_runtime: impl FnOnce() -> nervix_primitives::runtime::Runtime,
    ) -> Self {
        let schema = benchmark_schema();
        let transformations = CodecJaqTransformations {
            on_ingestion: None,
            on_emitting: Some(".".to_string()),
            on_emitting_batch: None,
        };
        let codec = compile_codec(
            &CreateCodec {
                name: identifier("admitted_work_codec"),
                wire_format: CodecWireFormat::JaqNative {
                    format: CodecJaqFormat::Json,
                    transformations: transformations.clone(),
                },
                schema: identifier("admitted_work_benchmark"),
                encoding_rules: Vec::new(),
            },
            nervix_primitives::sync::Arc::clone(&schema),
            ResolvedCodecWireFormat::JaqNative {
                format: CodecJaqFormat::Json,
                transformations: &transformations,
            },
        )
        .assured("the identity program encodes the benchmark schema");
        let batch = tenant_batches(&schema, rows, 1)
            .into_iter()
            .next()
            .assured("one tenant yields one batch");
        let domain = identifier::<DomainName>("admitted_work");
        let lifecycle = DomainClockLifecycle::new(domain.clone());
        lifecycle.synchronize(
            &DomainState {
                id: domain.clone(),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                },
                status: DomainStatus::Running,
                start_version: 0,
                last_start: DomainStartPoint::Resume,
                clock: None,
            },
            &DomainClockAuthority::initial(),
        );
        let clock = lifecycle
            .bind()
            .assured("the benchmark installs its unpaced domain clock above");
        let node_runtime = Runtime::new();
        let emitter = identifier::<EmitterName>("admitted_work_emitter");
        let key = nervix_models::DomainNodeUnitRef::single(DomainNodeRef::node_in(
            domain.clone(),
            ModelKind::Emitter,
            emitter.clone(),
        ));
        Self {
            runtime: build_runtime(),
            context: EmitterConnectorContext {
                routing: node_runtime
                    .stage_domain_routing(&domain, DomainRoutingSnapshot::default())
                    .shared(),
                metrics_dirty: node_runtime.branch_metrics_mark(
                    &domain,
                    ModelKind::Emitter,
                    &emitter,
                ),
                status: node_runtime.emitter_status(&key),
                readiness: node_runtime.register_emitter_readiness(&key),
                confirmation_waits: node_runtime.emitter_confirmation_counter(&key),
                interactions: node_runtime.inner.metrics.register_emitter_interactions(
                    &domain,
                    &emitter,
                    nervix_models::Unit::SINGLE,
                    None,
                    nervix_models::EmitterEntityKind::Kafka(
                        nervix_models::KafkaEmitterEntity::Topic,
                    ),
                ),
                runtime: node_runtime,
                domain,
                emitter,
                unit: nervix_models::Unit::SINGLE,
                error_policies: ErrorPolicies::handled_by_log(),
                udfs: None,
                clock,
            },
            codec,
            batch: EmitterPublishBatch::new(
                identifier("admitted_work_relay"),
                batch,
                None,
                Timestamp::from_unix_nanos(1),
            )
            .assured("a batch without headers has no header count to mismatch"),
            rows: (0..rows).collect(),
        }
    }

    /// Encode every row of the batch once, and answer how many payloads were produced.
    pub fn encode(&self) -> usize {
        let encoded = self.runtime.block_on(encode_pending_broker_payloads(
            nervix_primitives::sync::Arc::clone(&self.codec),
            &self.context,
            &self.batch,
            self.rows.clone(),
        ));
        match encoded {
            Ok(payloads) => payloads.len(),
            Err(_) => panic!("the benchmark rows encode through the identity program"),
        }
    }
}

/// One retained emitter and buffer, with all rows confirmed by the scripted record boundary.
/// Construction and handle registration stay outside the measured flush.
pub struct EmitterFlushBenchmark {
    work: TransformedEncodingBenchmark,
    connector: super::emitter_publishing::EmitterConnectorState,
    buffer: super::emitter_buffer::EmitterBatchBuffer,
    faults: crate::ConfiguredFaultInjection,
    backoff: super::RuntimeReconnectBackoff,
    _shutdown: nervix_primitives::sync::watch::Sender<bool>,
    shutdown_rx: nervix_primitives::sync::watch::Receiver<bool>,
    _stop: nervix_primitives::sync::watch::Sender<Option<nervix_primitives::time::Instant>>,
    stop_rx: nervix_primitives::sync::watch::Receiver<Option<nervix_primitives::time::Instant>>,
}

impl EmitterFlushBenchmark {
    /// A batch of `rows` rows using the existing identity-transformation encoding fixture.
    pub fn new(rows: usize) -> Self {
        let work = TransformedEncodingBenchmark::with_runtime(rows, || {
            nervix_primitives::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_time()
                .build()
                .assured("the flush host runs on a two-worker runtime with timers")
        });
        let driver = nervix_connector_fake::FakeEmitter::<nervix_connector_fake::Records>::new(
            nervix_connector_fake::PublishScript::Repeat(
                nervix_connector_fake::PublishAnswer::ConfirmAll,
            ),
        );
        let connector = super::emitter_publishing::EmitterConnectorState::opened(Box::new(
            super::emitter_encoding::EncodedRecordEmitter {
                connector: Box::new(driver),
                codec: nervix_primitives::sync::Arc::clone(&work.codec),
                batch: None,
            },
        ));
        let buffer = super::emitter_buffer::EmitterBatchBuffer::new(
            &work.context,
            &nervix_models::FlushPolicy::Each {
                interval: "1s".to_string(),
                max_batch_size: "1B".to_string(),
            },
            nervix_primitives::sync::Arc::new(
                super::emitter_buffer::EmitterBufferedMessages::default(),
            ),
        );
        let (shutdown, shutdown_rx) = nervix_primitives::sync::watch::channel(false);
        let (stop, stop_rx) = nervix_primitives::sync::watch::channel(None);
        Self {
            work,
            connector,
            buffer,
            faults: crate::ConfiguredFaultInjection::default(),
            backoff: super::RuntimeReconnectBackoff::default(),
            _shutdown: shutdown,
            shutdown_rx,
            _stop: stop,
            stop_rx,
        }
    }

    /// Buffer, encode and publish one batch through the same owner that flushes a running emitter.
    /// The returned delivered-row count is the benchmark's complete publication oracle.
    pub fn flush(&mut self) -> u64 {
        let mut control = super::emitter_publishing::EmitterPublishControl {
            fault_injection: &self.faults,
            shutdown_rx: &mut self.shutdown_rx,
            stop_rx: &mut self.stop_rx,
            backoff: &mut self.backoff,
        };
        let result = self.work.runtime.block_on(self.connector.publish_batch(
            &self.work.context,
            &mut control,
            &mut self.buffer,
            self.work.batch.clone(),
        ));
        match result {
            Ok(Some(report)) => report.messages,
            Ok(None) => panic!("every benchmark batch exceeds its one-byte flush boundary"),
            Err(failure) => panic!("the benchmark flush failed: {:#}", failure.error()),
        }
    }
}
