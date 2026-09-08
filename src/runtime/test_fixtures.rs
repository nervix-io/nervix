//! Fixtures the runtime's unit tests share.
//!
//! A fixture lives here only when tests in more than one runtime module build the same
//! value: names, schemas, branch keys, WASM envelopes, and the small runtime handles a
//! test needs before it can exercise anything. A fixture used by one module belongs in
//! that module's own test module instead.

use std::{num::NonZeroUsize, sync::Arc as StdArc};

use ahash::{HashMap, HashSet};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema as ArrowSchema;
use nervix_models::{
    Assignment, AssignmentTarget, AssignmentTargetScope, BranchName, BranchSelection,
    ClusterNodeName, CreateBranch, CreateSchema, DomainConfig, DomainName, DomainPace, DomainState,
    DomainStatus, ErrorPolicies, Expression, FieldName, FieldReference, FieldScope,
    IngestQuiesceMode, IngestorName, MessageErrorPolicy, ModelKind, ModelName, OutputBranch,
    ParseAsType, ProcessorOutput, ProcessorOutputs, RelayName, ScheduledNode, SchemaField,
    SchemaName, Timestamp,
};
use nervix_vm::window::lower_window_assignments;
use nervix_wasm::{
    WasmAckSidecar, WasmEnvelope, WasmOutputColumnRef, WasmOutputRow, WasmRoutedOutput,
};
use tokio::time::{Duration, sleep, timeout};
use triomphe::Arc;

use super::{
    wasm_output::{WasmMaterializedOutput, WasmOutputError, WasmOutputValidator},
    wasm_processor::wasm_envelope_from_relay_batch,
    *,
};
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{
        RuntimeRecordBatch, RuntimeRow, RuntimeValue, compile_schema, test_runtime_row,
    },
};

pub(super) fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).expect("valid name")
}

pub(super) fn row_value(row: &RuntimeRow, field: &str) -> Option<RuntimeValue> {
    row.value(field).expect("Arrow row value must be readable")
}

pub(super) fn batch_value(batch: &RuntimeRecordBatch, field: &str) -> Option<RuntimeValue> {
    assert_eq!(batch.batch().num_rows(), 1, "expected one Arrow row");
    batch
        .value(0, field)
        .expect("Arrow batch value must be readable")
}

pub(super) fn vm_input_from_test_rows(
    rows: &[RuntimeRow],
    schema: &StdArc<ArrowSchema>,
) -> Result<super::VmTypedBatch, String> {
    let batches = rows
        .iter()
        .map(RuntimeRow::one_row_batch)
        .collect::<Vec<_>>();
    let carrier = RuntimeRecordBatch::concat(&batches.iter().collect::<Vec<_>>())?;
    let keys = vec![None; rows.len()];
    let side_inputs = HashMap::default();
    let lookup_columns = HashMap::default();
    super::project_vm_input_batch(
        schema,
        &super::VmInputProjectionSources {
            carrier: &carrier,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: None,
        },
        None,
    )
}

/// The headers of one test message, standing in for a connector's borrowed message.
pub(super) struct TestIngestHeaders<'a>(pub(super) &'a [(&'a str, &'a str)]);

impl super::IngestMessageHeaders for TestIngestHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in self.0 {
            visit(name, value);
        }
    }
}

/// Builds one group's ingest metadata through the group builders, as the runtime does.
pub(super) fn ingest_metadata_for_test(
    kind: super::IngestMetadataKind,
    rows: &[super::IngestMetadataRow<'_>],
) -> super::IngestFilterMapMetadata {
    let mut builders = super::IngestMetadataBuilders::new(kind, rows.len());
    for row in rows {
        builders
            .append(row)
            .expect("test metadata row must match the group's source kind");
    }
    builders.finish().expect("test metadata must build")
}

pub(super) async fn execute_filter_map_for_test(
    program: &super::CompiledProgramWithMaterializedInterest,
    record: RuntimeRow,
    branch_key: Option<&BranchKey>,
    metadata: Option<&super::IngestFilterMapMetadata>,
    now: Timestamp,
) -> Result<Option<RuntimeRow>, String> {
    super::execute_filter_map_on_record(
        &named("test_filter_map"),
        program,
        record,
        branch_key,
        metadata,
        &HashMap::default(),
        now,
    )
    .await
}

pub(super) fn expression(raw: &str) -> nervix_models::Expression {
    nervix_nspl::parse_expression(raw).expect("valid semantic expression")
}

pub(super) fn construction(raw: &str) -> nervix_models::RouteConstruction {
    nervix_nspl::parse_route_construction(raw).expect("valid route construction")
}

pub(super) fn window_outputs(relay: &str, set: &str) -> ProcessorOutputs {
    ProcessorOutputs::new(vec![ProcessorOutput {
        relay: named(relay),
        construction: construction(set),
        flush_policy: None,
        message_error_policy: MessageErrorPolicy::Log,
        branch: None,
    }])
}

pub(super) fn with_inherit_all(mut outputs: ProcessorOutputs) -> ProcessorOutputs {
    for output in &mut outputs.routes {
        output.construction.inherit = Some(nervix_models::Inheritance::All);
    }
    outputs
}

pub(super) fn window_aggregate(set: &str) -> nervix_vm::window::WindowAggregateProgram {
    lower_window_assignments(&construction(set))
        .expect("window route construction should lower")
        .inner
}

pub(super) fn compile_window_aggregate_for_test(
    aggregate: &nervix_vm::window::WindowAggregateProgram,
    input_type: ParseAsType,
    output_schema: &super::CompiledSchema,
) -> super::CompiledWindowAggregateProgram {
    let input_relay = named::<RelayName>("events");
    let output_relay = named::<RelayName>("summary");
    let input_schema = compile_schema(&CreateSchema {
        name: SchemaName::from(&ModelName::from(&input_relay)),
        fields: vec![SchemaField {
            name: named("latency"),
            ty: input_type,
            optional: false,
            sensitive: false,
        }],
    });
    let mut relay_schemas = HashMap::default();
    relay_schemas.insert(input_relay.clone(), Arc::new(input_schema));
    relay_schemas.insert(output_relay.clone(), Arc::new(output_schema.clone()));

    super::CompiledWindowAggregateProgram::compile(
        aggregate,
        &[input_relay],
        &output_relay,
        &relay_schemas,
        None,
    )
    .expect("window aggregate should compile")
}

pub(super) fn window_inputs(
    aggregate: &nervix_vm::window::WindowAggregateProgram,
    value: RuntimeValue,
) -> Vec<super::WindowAggregateInput> {
    aggregate
        .demands()
        .iter()
        .map(|_| super::WindowAggregateInput {
            value: Some(value.clone()),
        })
        .collect()
}

pub(super) fn branch_key(
    fields: impl IntoIterator<Item = (FieldName, RuntimeValue)>,
) -> Option<BranchKey> {
    BranchKey::from_fields(fields)
        .expect("test branch key must be non-empty")
        .into()
}

pub(super) fn concrete_branch_key(
    fields: impl IntoIterator<Item = (FieldName, RuntimeValue)>,
) -> BranchKey {
    branch_key(fields).expect("test branch key must be concrete")
}

pub(super) fn string_branch_key(field: &str, value: &str) -> Option<BranchKey> {
    branch_key([(named(field), RuntimeValue::String(value.to_string()))])
}

pub(super) fn u32_branch_key(field: &str, value: u32) -> Option<BranchKey> {
    branch_key([(named(field), RuntimeValue::U32(value))])
}

pub(super) fn key_label(key: &Option<BranchKey>) -> &str {
    key.as_ref().expect("test branch key must exist").as_str()
}

pub(super) fn domain(raw: &str) -> DomainName {
    DomainName::parse(raw).expect("valid domain")
}

pub(super) const TWO_ITEM_TEST_CHANNEL_CAPACITY: usize = 2;

pub(super) fn nonzero_capacity(capacity: usize) -> NonZeroUsize {
    NonZeroUsize::new(capacity).expect("test relay capacity must be nonzero")
}

pub(super) fn branched_by(relay: &str, fields: &[&str]) -> OutputBranch {
    if fields.is_empty() {
        OutputBranch::Unbranched
    } else {
        OutputBranch::BranchedBy {
            branch: named(&format!("by_{relay}")),
            assignments: branch_mappings(fields),
        }
    }
}

pub(super) fn processor_branched_by(relay: &str, fields: &[&str]) -> BranchSelection {
    if fields.is_empty() {
        BranchSelection::unbranched()
    } else {
        BranchSelection::branched_by(named(&format!("by_{relay}")))
    }
}

pub(super) fn branch_mappings(fields: &[&str]) -> Vec<Assignment> {
    fields
        .iter()
        .map(|field| Assignment {
            target: AssignmentTarget {
                scope: AssignmentTargetScope::Bare,
                field: named(field),
            },
            value: Expression::Field(FieldReference::scoped(FieldScope::Message, named(field))),
        })
        .collect()
}

pub(super) fn branch_model(schema: &str, relay: &str, _fields: &[&str]) -> PlannedModel {
    let branch = named::<BranchName>(&format!("by_{relay}"));
    PlannedModel {
        kind: ModelKind::Branch,
        identifier: ModelName::from(&branch.clone()),
        model: nervix_models::Model::Branch(CreateBranch {
            name: branch,
            schema: named(schema),
            ttl: "5m".to_string(),
            eviction: None,
        }),
    }
}

pub(super) fn test_relay_boundary_services() -> Arc<super::RelayBoundaryServices> {
    Arc::new(super::RelayBoundaryServices::new(
        super::RelayBoundaryFanout::direct_with_capacity(STUPID_CHANNEL_CAPACITY_REMOVE_ME),
        0,
        0,
        Vec::new(),
        None,
    ))
}

pub(super) fn test_ingestor_quiesce_control(
    runtime: &super::Runtime,
    domain: &DomainName,
    ingestor: &IngestorName,
    mode: IngestQuiesceMode,
) -> Arc<super::IngestorQuiesceControl> {
    let metric_labels = runtime
        .inner
        .metrics
        .register_ingestor_quiesce(domain, ingestor, None);
    Arc::new(super::IngestorQuiesceControl::new(
        mode,
        runtime.inner.metrics.clone(),
        metric_labels,
    ))
}

pub(super) fn paced_domain_state(raw: &str) -> DomainState {
    DomainState {
        id: domain(raw),
        config: DomainConfig {
            pace: DomainPace::Paced,
            period: "1s".to_string(),
            skew: "250ms".to_string(),
            placement: nervix_models::PlacementPolicy::Neutral,
        },
        status: DomainStatus::Running,
        start_version: 0,
        last_start: nervix_models::DomainStartPoint::Resume,
        clock: None,
    }
}

pub(super) fn test_schema(fields: &[(&str, ParseAsType)]) -> Arc<super::CompiledSchema> {
    Arc::new(compile_schema(&CreateSchema {
        name: named("test_schema"),
        fields: fields
            .iter()
            .map(|(name, ty)| nervix_models::SchemaField {
                name: named(name),
                ty: ty.clone(),
                optional: false,
                sensitive: false,
            })
            .collect(),
    }))
}

/// One field of a test schema whose optionality varies per field.
pub(super) struct OptionalTestField {
    pub(super) name: &'static str,
    pub(super) ty: ParseAsType,
    pub(super) optional: bool,
}

pub(super) fn test_optional_schema(fields: &[OptionalTestField]) -> Arc<super::CompiledSchema> {
    Arc::new(compile_schema(&CreateSchema {
        name: named("test_schema"),
        fields: fields
            .iter()
            .map(|field| nervix_models::SchemaField {
                name: named(field.name),
                ty: field.ty.clone(),
                optional: field.optional,
                sensitive: false,
            })
            .collect(),
    }))
}

pub(super) async fn wasm_input_for_records(
    schema: &Arc<super::CompiledSchema>,
    records: Vec<RuntimeRow>,
) -> (WasmEnvelope, super::WasmAckMap) {
    let messages = records
        .into_iter()
        .map(|record| super::RelayMessage {
            key: string_branch_key("tenant", "test"),
            record,
            acks: AckSet::empty(),
        })
        .collect();
    let batch = super::RelayRecordBatch::from_messages(Arc::clone(schema), messages)
        .expect("test relay batch must build");
    let mut next_token = 1;
    wasm_envelope_from_relay_batch(&Executor::default(), &batch, &mut next_token)
        .await
        .expect("WASM input envelope must build")
}

pub(super) async fn wasm_input_for_values(
    schema: &Arc<super::CompiledSchema>,
    values: &[i32],
) -> (WasmEnvelope, super::WasmAckMap) {
    let records = values
        .iter()
        .map(|value| test_runtime_row([("value".to_string(), RuntimeValue::I32(*value))]))
        .collect();
    wasm_input_for_records(schema, records).await
}

pub(super) fn wasm_input_acks(envelope: &WasmEnvelope) -> &WasmAckSidecar {
    envelope
        .input_acks()
        .expect("test envelope must be a WASM input")
}

pub(super) fn validate_wasm_test_outputs(
    input_schema: &Arc<super::CompiledSchema>,
    output_schema: &Arc<super::CompiledSchema>,
    ack_map: &super::WasmAckMap,
    outputs: Vec<WasmEnvelope>,
) -> Result<Vec<WasmMaterializedOutput>, WasmOutputError> {
    validate_wasm_test_output_groups(
        input_schema,
        vec![("output", Arc::clone(output_schema))],
        ack_map,
        outputs,
    )
}

pub(super) fn validate_wasm_test_output_groups(
    input_schema: &Arc<super::CompiledSchema>,
    schemas: Vec<(&str, Arc<super::CompiledSchema>)>,
    ack_map: &super::WasmAckMap,
    outputs: Vec<WasmEnvelope>,
) -> Result<Vec<WasmMaterializedOutput>, WasmOutputError> {
    let output_schemas = schemas
        .into_iter()
        .map(|(relay, schema)| (named::<RelayName>(relay), schema))
        .collect::<Vec<_>>();
    let output_routes = super::RelayProcessorOutputsNode {
        routes: output_schemas
            .iter()
            .map(|(relay, _)| super::RelayProcessorOutputNode {
                relay: relay.clone(),
                construction: nervix_models::RouteConstruction::default(),
                branch: None,
                flush_policy: None,
                message_error_policy: MessageErrorPolicy::Log,
                pending: Vec::new(),
                next_flush: None,
                compiled_program: None,
                compiled_branch_program: None,
            })
            .collect(),
    };
    WasmOutputValidator {
        ack_map,
        input_schema,
        output_schemas: &output_schemas,
        output_routes: &output_routes,
    }
    .validate(outputs)
}

pub(super) fn wasm_test_output(
    columns: Vec<WasmOutputColumnRef>,
    rows: Vec<WasmOutputRow>,
) -> WasmEnvelope {
    WasmEnvelope::output(
        Vec::new(),
        vec![WasmRoutedOutput::new(
            "output",
            columns,
            WasmAckSidecar {
                rows,
                acked: Vec::new(),
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        )],
    )
}

pub(super) fn wasm_test_generated_output(
    generated_arrow_ipc_batch: Vec<u8>,
    columns: Vec<WasmOutputColumnRef>,
    rows: Vec<WasmOutputRow>,
) -> WasmEnvelope {
    WasmEnvelope::output(
        generated_arrow_ipc_batch,
        vec![WasmRoutedOutput::new(
            "output",
            columns,
            WasmAckSidecar {
                rows,
                acked: Vec::new(),
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        )],
    )
}

pub(super) fn wasm_guest_column(field: arrow_schema::Field, array: ArrayRef) -> Vec<u8> {
    let schema = StdArc::new(ArrowSchema::new(vec![field.with_name("")]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![array]).expect("guest column batch must build");
    wasm_guest_stream(schema, &[batch])
}

pub(super) fn wasm_generated_pool(
    fields: Vec<arrow_schema::Field>,
    arrays: Vec<ArrayRef>,
) -> Vec<u8> {
    let schema = StdArc::new(ArrowSchema::new(
        fields
            .into_iter()
            .map(|field| field.with_name(""))
            .collect::<Vec<_>>(),
    ));
    let batch =
        RecordBatch::try_new(schema.clone(), arrays).expect("generated pool batch must build");
    wasm_guest_stream(schema, &[batch])
}

pub(super) fn wasm_guest_stream(schema: StdArc<ArrowSchema>, batches: &[RecordBatch]) -> Vec<u8> {
    let mut ipc = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut ipc, &schema).expect("guest column writer must build");
        for batch in batches {
            writer.write(batch).expect("guest column must encode");
        }
        writer.finish().expect("guest column stream must finish");
    }
    ipc
}

pub(super) fn scheduled_model(
    kind: ModelKind,
    identifier: ModelName,
    model: nervix_models::Model,
) -> ScheduledNode {
    ScheduledNode {
        identifier,
        kind,
        config: Box::new(model),
        effective_branching: None,
        effective_branching_schema: None,
        schema_fingerprint: [0; 32],
        kafka_partition_schedule: None,
        primary_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
        assigned_nodes: vec![ClusterNodeName::parse("node-1").expect("valid name")],
    }
}

pub(super) fn junction_branch_template(
    processor: &str,
    input_relay: &str,
) -> super::BranchInstanceTemplate {
    let processor = named::<ModelName>(processor);
    let input_relay = named::<RelayName>(input_relay);
    super::BranchInstanceTemplate {
        source_kind: ModelKind::Junction,
        source: RelayName::from(&processor.clone()),
        root_relay: input_relay.clone(),
        branch: None,
        branch_ttl: None,
        branch_max_instances: None,
        error_policies: ErrorPolicies::handled_by_log(),
        relays: HashMap::default(),
        materialized_streams: HashSet::default(),
        processors: [(
            processor.clone(),
            super::RelayProcessorTemplate {
                kind: ModelKind::Junction,
                processor,
                input_relays: vec![input_relay],
                input_collect_policies: HashMap::default(),
                error_policies: ErrorPolicies::handled_by_log(),
                from_where: HashMap::default(),
                filter_where: None,
                materialized_state: Vec::new(),
                operation: super::RelayProcessorOperationTemplate::Junction {
                    output_routes: super::RelayProcessorOutputsTemplate { routes: Vec::new() },
                },
            },
        )]
        .into_iter()
        .collect(),
    }
}

pub(super) fn quiesce_test_batch() -> super::RelayRecordBatch {
    super::RelayRecordBatch::single(
        test_schema(&[("value", ParseAsType::I64)]),
        None,
        test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
        AckSet::empty(),
    )
    .expect("quiesce test batch should build")
}

pub(super) async fn wait_for_persisted_runtime_state_lsm(
    runtime: &super::Runtime,
    placement: &RuntimeStatePlacement,
    expected_lsm: u64,
) {
    let store = runtime
        .inner
        .state_store
        .as_ref()
        .expect("test runtime should have a state store")
        .clone();
    timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::consume_budget().await;
            if store
                .latest_snapshot(placement)
                .expect("snapshot lookup should succeed")
                .is_some_and(|snapshot| snapshot.lsm == expected_lsm)
            {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("dirty state should persist within the snapshot interval");
}
