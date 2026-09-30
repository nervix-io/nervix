//! Opaque drivers for measuring data-plane work the node admits through its bounded executor:
//! preparing a branched entrypoint input, and encoding an emitter's rows through a codec
//! transformation.
//!
//! This module only exists with the `benchmarks` feature. Its public surface exposes benchmark
//! operations and what they produced, never Nervix runtime carriers.
//!
//! Layer: test and benchmark harness.
//! - **Owns.** Bounded-executor benchmark inputs and retained sink context dependencies.
//! - **Depends on.** The production runtime, codec and batch preparation APIs.
//! - **Must not know.** Graph scheduling or deployment decisions.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    CodecJaqFormat, CodecJaqTransformations, CodecWireFormat, CreateCodec, CreateSchema,
    DomainClockAuthority, DomainConfig, DomainName, DomainNodeRef, DomainPace, DomainStartPoint,
    DomainState, DomainStatus, EmitterName, ErrorPolicies, FieldName, ModelKind, ParseAsType,
    PlacementPolicy, ResolvedCodecWireFormat, SchemaName, Timestamp,
};

use super::{
    ArcSwap, BranchInstanceAckBoundary, BranchKey, BranchMetricsMark, CompiledCodec,
    DomainClockLifecycle, DomainRoutingSnapshot, EmitterPublishBatch, EmitterSinkContext, Executor,
    RelayMessage, RelayRecordBatch, Runtime, StdArc,
    emitter_encoding::encode_pending_broker_payloads, prepare_branched_entrypoint_input,
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
fn benchmark_schema() -> triomphe::Arc<CompiledSchema> {
    let field = |name: &str, ty: ParseAsType| nervix_models::SchemaField {
        name: identifier::<FieldName>(name),
        ty,
        optional: false,
        sensitive: false,
    };
    triomphe::Arc::new(compile_schema(&CreateSchema {
        name: identifier::<SchemaName>("admitted_work_benchmark"),
        fields: vec![
            field("tenant", ParseAsType::String),
            field("value", ParseAsType::I64),
        ],
    }))
}

/// `rows` rows spread evenly over `branches` tenants, one relay batch per tenant, in tenant order.
fn tenant_batches(
    schema: &triomphe::Arc<CompiledSchema>,
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
                std::sync::Arc::new(arrow_array::StringArray::from(vec![
                    tenant.as_str();
                    per_branch
                ])),
                std::sync::Arc::new(arrow_array::Int64Array::from(values)),
            ],
        )
        .assured("the columns are built here for the schema declared beside them");
        let runtime_batch = triomphe::Arc::new(
            RuntimeRecordBatch::from_record_batch(schema.arrow_schema(), record_batch)
                .assured("the batch was built for this schema"),
        );
        let watermark = Timestamp::from_unix_nanos(1);
        let mut messages = Vec::with_capacity(per_branch);
        for row in 0..per_branch {
            messages.push(RelayMessage {
                key: Some(key.clone()),
                record: RuntimeRow::new(
                    triomphe::Arc::clone(&runtime_batch),
                    row,
                    RuntimeRecordMetadata::from_ingested_at_watermarks(watermark, watermark),
                )
                .assured("the row index is inside the batch built above"),
                acks: AckSet::empty(),
            });
        }
        batches.push(
            RelayRecordBatch::from_messages(triomphe::Arc::clone(schema), messages)
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
/// sink's rows are before they are published.
pub struct TransformedEncodingBenchmark {
    runtime: nervix_primitives::runtime::Runtime,
    context: EmitterSinkContext,
    codec: triomphe::Arc<CompiledCodec>,
    batch: EmitterPublishBatch,
    rows: Vec<usize>,
}

impl TransformedEncodingBenchmark {
    /// A batch of `rows` rows of one tenant, encoded with an identity transformation.
    pub fn new(rows: usize) -> Self {
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
            triomphe::Arc::clone(&schema),
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
                    placement: PlacementPolicy::Neutral,
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
        let runtime = Runtime::new();
        let emitter = identifier::<EmitterName>("admitted_work_emitter");
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Emitter, emitter.clone());
        Self {
            runtime: benchmark_runtime(),
            context: EmitterSinkContext {
                routing: StdArc::new(ArcSwap::from_pointee(DomainRoutingSnapshot::default())),
                metrics_dirty: BranchMetricsMark::default(),
                status: runtime.emitter_status(&key),
                confirmation_waits: runtime.emitter_confirmation_counter(&key),
                runtime,
                domain,
                emitter,
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
            triomphe::Arc::clone(&self.codec),
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
