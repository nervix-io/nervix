//! Fixtures the runtime's unit tests share.
//!
//! A fixture lives here only when tests in more than one runtime module build the same
//! value: names, schemas, branch keys, WASM envelopes, and the small runtime handles a
//! test needs before it can exercise anything. A fixture used by one module belongs in
//! that module's own test module instead.

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc as StdArc, OnceLock},
};

pub(in crate::runtime) const STUPID_CHANNEL_CAPACITY_REMOVE_ME: NonZeroUsize = NonZeroUsize::MIN;

use ahash::HashMap;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema as ArrowSchema;
use nervix_execution::sync::ArcSwapOption;
use nervix_models::{
    Assignment, AssignmentTarget, AssignmentTargetScope, BranchName, BranchSelection,
    ClusterNodeIncarnation, ClusterNodeName, CreateBranch, CreateSchema, DomainConfig, DomainName,
    DomainPace, DomainState, DomainStatus, ErrorPolicies, Expression, FieldName, FieldReference,
    FieldScope, IngestQuiesceMode, IngestorName, MessageErrorPolicy, ModelKind, ModelName,
    OutputBranch, ParseAsType, ProcessorOutput, ProcessorOutputs, RelayName, ResolvedBranching,
    ScheduledNode, SchemaField, SchemaFingerprint, SchemaName, Timestamp,
};
use nervix_vm::window::lower_window_assignments;
use nervix_wasm::{
    WasmAckSidecar, WasmEnvelope, WasmOutputColumnRef, WasmOutputRow, WasmRoutedOutput,
};
use tokio::{
    sync::watch,
    time::{Duration, sleep, timeout},
};
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
    let carrier = RuntimeRecordBatch::concat(&batches.iter().collect::<Vec<_>>())
        .map_err(|error| error.to_string())?;
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
    .map_err(|error| error.to_string())
}

/// The headers of one test message, standing in for a connector's borrowed message.
pub(super) struct TestIngestHeaders<'a>(pub(super) &'a [(&'a str, &'a str)]);

impl nervix_connector::IngestMessageHeaders for TestIngestHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in self.0 {
            visit(name, value);
        }
    }
}

/// Builds one group's ingest metadata through the group builders, as the runtime does.
pub(super) fn ingest_metadata_for_test(
    kind: super::IngestMetadataKind,
    rows: &[nervix_connector::IngestMetadataRow<'_>],
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
    super::filter_map::execute_filter_map_on_record(
        &named::<ModelName>("test_filter_map"),
        program,
        record,
        branch_key,
        metadata,
        &HashMap::default(),
        now,
    )
    .await
    .map_err(|error| error.to_string())
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

/// The accumulator plan of a single-route window over an `events` relay whose `latency` field
/// has `input_type`, writing `output_fields`.
pub(super) fn window_plan(
    set: &str,
    input_type: ParseAsType,
    output_fields: &[(&str, ParseAsType)],
) -> super::WindowAccumulatorPlan {
    let aggregate = window_aggregate(set);
    let compiled =
        compile_window_aggregate_for_test(&aggregate, input_type, &test_schema(output_fields));
    super::WindowAccumulatorPlan::new([&compiled.route])
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

/// Attaches `runtime` to a cluster of one node on loopback, named `node_id`, the way a starting
/// node attaches to the cluster it joined. The runtime then learns its identity and incarnation
/// from a real transport and cluster handle, and the incarnation that cluster announced is
/// returned for the requests a test builds on the node's behalf.
pub(super) async fn attach_loopback_cluster(
    runtime: &super::Runtime,
    node_id: &ClusterNodeName,
) -> ClusterNodeIncarnation {
    let interconnect = crate::application::test_fixtures::test_interconnect("test", node_id).await;
    // Nothing serves gRPC or the web console in a runtime test. Gossip only announces these
    // addresses, so they name the interconnect listener.
    let interconnect_addr = interconnect.local_addr();
    let cluster = crate::cluster::start_cluster(crate::cluster::ClusterSettings {
        cluster_id: "test".to_string(),
        node_id: node_id.clone(),
        grpc_listen_addr: interconnect_addr,
        client_advertise_url: crate::application::test_fixtures::test_service_url(
            interconnect_addr,
        ),
        console_advertise_url: crate::application::test_fixtures::test_service_url(
            interconnect_addr,
        ),
        interconnect_advertise_addr: interconnect_addr.into(),
        bootstrap_host: None,
        interconnect: interconnect.clone(),
        node_unavailability_timeout: Duration::from_secs(10),
    })
    .await
    .expect("a loopback cluster of one node should start");
    let incarnation = cluster.local_incarnation();
    runtime.attach_remote_dispatcher(Arc::new(cluster), interconnect);
    incarnation
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
            pace: DomainPace::Paced {
                period: "1s"
                    .parse()
                    .assured("one second is a positive fixture cadence"),
                skew: "250ms"
                    .parse()
                    .assured("250 milliseconds fits the fixture skew representation"),
            },
            placement: nervix_models::PlacementPolicy::Neutral,
        },
        status: DomainStatus::Running,
        start_version: 0,
        last_start: nervix_models::DomainStartPoint::Resume,
        clock: None,
    }
}

pub(super) fn unpaced_domain_state(raw: &str) -> DomainState {
    DomainState {
        id: domain(raw),
        config: DomainConfig {
            pace: DomainPace::Unpaced,
            placement: nervix_models::PlacementPolicy::Neutral,
        },
        status: DomainStatus::Running,
        start_version: 0,
        last_start: nervix_models::DomainStartPoint::Resume,
        clock: None,
    }
}

pub(super) fn install_unpaced_test_domain(runtime: &super::Runtime, domain: &DomainName) {
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        unpaced_domain_state(domain.as_str()),
    )]));
    runtime.inner.domain_routings.insert(
        domain.clone(),
        StdArc::new(ArcSwap::from_pointee(DomainRoutingSnapshot::default())),
    );
}

/// Publish the runtime-state identity of the `kind` node `identifier` in `domain`, as a committed
/// schedule carrying that node does, so its schema-bound state can be placed. A WASM processor also
/// gets the first guest-state generation of every branch.
pub(super) fn publish_state_identity(
    runtime: &super::Runtime,
    domain: &DomainName,
    kind: ModelKind,
    identifier: impl Into<ModelName>,
) {
    let mut wasm_state_generations = None;
    if kind == ModelKind::WasmProcessor {
        wasm_state_generations = Some(nervix_models::WasmStateGenerations::first());
    }
    runtime.inner.state_identities.insert(
        nervix_models::DomainNodeRef::node_in(domain.clone(), kind, identifier.into()),
        super::ScheduledStateIdentity {
            schema_fingerprint: SchemaFingerprint::from_digest([7; 32]),
            wasm_state_generations,
        },
    );
}

pub(super) fn test_domain_clock_authority() -> nervix_models::DomainClockAuthority {
    nervix_models::DomainClockAuthority::assigned(
        nervix_models::DomainClockAuthorityRevision::INITIAL,
        nervix_models::ClusterNodeIdentity::new(
            nervix_models::ClusterNodeName::parse("test-clock-authority")
                .expect("the fixture authority name is valid"),
            nervix_models::ClusterNodeIncarnation::new(1),
        ),
    )
}

pub(super) fn test_domain_clock(domain: &DomainName) -> super::DomainClock {
    let lifecycle = super::DomainClockLifecycle::new(domain.clone());
    lifecycle.synchronize(
        &unpaced_domain_state(domain.as_str()),
        &test_domain_clock_authority(),
    );
    lifecycle
        .bind()
        .expect("the fixture installs an unpaced domain clock")
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

pub(super) fn test_branching(fields: &[(&str, ParseAsType)]) -> ResolvedBranching {
    test_named_branching("test_branch", fields)
}

/// The definition an unbranched relay of `fields` declares to its session subscribers.
pub(super) fn unbranched_subscription_definition(
    fields: &[(&str, ParseAsType)],
) -> RelaySubscriptionDefinition {
    RelaySubscriptionDefinition::new(test_schema(fields), ResolvedBranching::unbranched())
}

pub(super) fn test_named_branching(
    branch: &str,
    fields: &[(&str, ParseAsType)],
) -> ResolvedBranching {
    ResolvedBranching::branched(
        named(branch),
        CreateSchema {
            name: named("test_branch_schema"),
            fields: fields
                .iter()
                .map(|(name, ty)| SchemaField {
                    name: named(name),
                    ty: ty.clone(),
                    optional: false,
                    sensitive: false,
                })
                .collect(),
        },
    )
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
                flush_timer: BranchBufferTimer::default(),
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

pub(super) fn scheduled_model(model: nervix_models::Model) -> ScheduledNode {
    let resolved_branching = match &model {
        nervix_models::Model::Relay(model) => {
            assert!(
                model.branching.is_unbranched(),
                "branched schedule fixtures must provide their resolved branch and schema"
            );
            Some(ResolvedBranching::unbranched())
        }
        nervix_models::Model::Generator(model) => {
            assert_unbranched_schedule_fixture(&model.branched_by)
        }
        nervix_models::Model::Inferencer(model) => {
            assert_unbranched_schedule_fixture(&model.branched_by)
        }
        nervix_models::Model::WasmProcessor(model) => {
            assert_unbranched_schedule_fixture(&model.branched_by)
        }
        nervix_models::Model::Deduplicator(model) => {
            assert_unbranched_schedule_fixture(&model.branched_by)
        }
        nervix_models::Model::Correlator(model) => {
            assert_unbranched_schedule_fixture(&model.branched_by)
        }
        nervix_models::Model::Junction(model) => {
            assert_unbranched_schedule_fixture(&model.branched_by)
        }
        nervix_models::Model::Reorderer(model) => {
            assert_unbranched_schedule_fixture(&model.branched_by)
        }
        nervix_models::Model::WindowProcessor(model) => {
            assert_unbranched_schedule_fixture(&model.branched_by)
        }
        _ => None,
    };
    ScheduledNode::new(model, SchemaFingerprint::from_digest([1; 32]))
        .with_resolved_branching(resolved_branching)
        .placed_on(
            Some(ClusterNodeName::parse("node-1").expect("valid name")),
            vec![ClusterNodeName::parse("node-1").expect("valid name")],
        )
}

fn assert_unbranched_schedule_fixture(
    branching: &nervix_models::BranchSelection,
) -> Option<ResolvedBranching> {
    assert!(
        branching.is_unbranched(),
        "branched schedule fixtures must provide their resolved branch and schema"
    );
    Some(ResolvedBranching::unbranched())
}

pub(super) fn install_test_domain_execution(
    runtime: &Runtime,
    domain: &DomainName,
    nodes: Vec<ScheduledNode>,
    routing: DomainRoutingSnapshot,
) {
    let (shutdown, _) = watch::channel(false);
    runtime.install_domain_execution(
        domain,
        DomainExecution {
            schedule: DomainSchedule::new(domain.clone(), nodes, Vec::new()),
            start_version: 0,
            domain_clock: test_domain_clock(domain),
            shutdown,
            graph: StdArc::new(ArcSwapOption::empty()),
            routing: runtime.stage_domain_routing(domain, routing),
            branched_ingestors: HashMap::default(),
            branched_entrypoints: HashMap::default(),
            endpoint_routes: HashMap::default(),
            node_tasks: HashMap::default(),
            emitter_tasks: HashMap::default(),
            generator_tasks: HashMap::default(),
            reingestor_tasks: HashMap::default(),
            placement_tasks: HashMap::default(),
            relay_state_tasks: HashMap::default(),
            relay_owner_tasks: HashMap::default(),
            clients: HashMap::default(),
            tasks: Vec::new(),
        },
    );
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
        wasm_state_reset: None,
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

pub(super) fn input_schema() -> Arc<CompiledSchema> {
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

pub(super) fn input_batch_with(value: i64, timestamp: i64, acks: AckSet) -> RelayRecordBatch {
    RelayRecordBatch::single(
        input_schema(),
        None,
        test_runtime_row([("value".to_string(), RuntimeValue::I64(value))])
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(timestamp)),
        acks,
    )
    .expect("valid emitter input batch")
}

pub(super) fn input_batch() -> RelayRecordBatch {
    input_batch_with(1, 0, AckSet::empty())
}

pub(super) fn input_value(batch: &RelayRecordBatch) -> i64 {
    let record = batch.runtime_row(0).expect("batch must contain one row");
    let Ok(Some(RuntimeValue::I64(value))) = record.value("value") else {
        panic!("test batch must contain an I64 value")
    };
    value
}

pub(super) fn sink_context() -> EmitterSinkContext {
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
