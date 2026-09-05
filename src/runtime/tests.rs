use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use ahash::{HashMap, HashSet};
use arc_swap::ArcSwapOption;
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema as ArrowSchema;
use fjall::Database;
use nervix_interconnect::{EntityGatePurpose, RelayPayload, RelayPayloadKind};
use nervix_models::{
    AckMode, Assignment, AssignmentTarget, AssignmentTargetScope, BranchSelection,
    ClientConfigEntry, ClusterSchedule, CodecWireFormat, CreateBranch, CreateClientHttp,
    CreateClientMqtt, CreateClientPrometheus, CreateClientWebsockets, CreateClientZeroMq,
    CreateCodec, CreateDeduplicator, CreateEmitter, CreateGenerator, CreateInferencer,
    CreateIngestor, CreateJsonWireSchema, CreateJunction, CreateLookup, CreateReingestor,
    CreateRelay, CreateSchema, CreateWasmProcessor, CreateWindowProcessor, Domain, DomainConfig,
    DomainPace, DomainSchedule, DomainState, DomainStatus, DomainTick, EmitSink,
    EmitterPublishingMode, ErrorPolicies, Expression, FieldPath, FieldReference, FieldScope,
    GeneralErrorPolicy, Identifier, InferencerTensorDeclaration, InferencerTensorDimension,
    InferencerTensorElementType, InferencerTensorMapping, InferencerTensorRepresentation,
    InferencerTensorSchema, IngestQuiesceMode, IngestQuiesceOverflow, IngestSource,
    IngestTimestampSource, JsonType, MessageErrorCode, MessageErrorOperation, MessageErrorPolicy,
    ModelKind, MqttIngestMode, MqttQos, MqttSession, OutputBranch, ParseAsType,
    ProcessorInputWhere, ProcessorInputs, ProcessorOutput, ProcessorOutputs, RelayBranching,
    RemoteAckOutcome, RemoteAckRegistration, RemoteAckResolution, ResourceId, ResourceVersion,
    ResourceVersionStatus, RetryPolicy, ScheduledNode, SchemaField, SqsFifoGroup,
    StructuredMessageError, Timestamp, WindowBound, WireSchemaField, ZeroMqIngestMode,
};
use nervix_nspl::window_processor::aggregate::lower_window_assignments;
use nervix_wasm::{
    WasmAckSidecar, WasmAckToken, WasmAckTokenSet, WasmEnvelope, WasmOutputColumnRef,
    WasmOutputRow, WasmRoutedOutput,
};
use ordered_float::OrderedFloat;
use sorted_vec::{SortedSet, SortedVec};

fn inferencer_tensor_schema(size: u32) -> InferencerTensorSchema {
    InferencerTensorSchema {
        representation: InferencerTensorRepresentation::Dense,
        element_type: InferencerTensorElementType::F32,
        dimensions: vec![InferencerTensorDimension::Fixed(size)],
    }
}
use tempfile::tempdir;
use tokio::{
    sync::{Mutex, mpsc, watch},
    time::{Duration, Instant, sleep, timeout},
};
use triomphe::Arc;

use super::{
    BranchInstanceRegistry, BranchKey, BranchedProcessorOperationSpec, RelayMessage,
    RuntimeStateKind, RuntimeStatePlacement, RuntimeStateStore, STUPID_CHANNEL_CAPACITY_REMOVE_ME,
    ScheduledEmitterTask, WindowAggregateFunction, WindowProcessorState, advance_window,
    evaluate_window_aggregate, message_timestamp, window_output_metadata,
};
use crate::{
    metrics::RuntimeMetrics,
    resource::ResourceStore,
    runtime_ack::{AckOutcome, AckRootTracker, AckSet},
    runtime_schema::{
        RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow, RuntimeValue, compile_schema,
        test_runtime_row,
    },
};

fn identifier(raw: &str) -> Identifier {
    Identifier::parse(raw).expect("valid identifier")
}

fn row_value(row: &RuntimeRow, field: &str) -> Option<RuntimeValue> {
    row.value(field).expect("Arrow row value must be readable")
}

fn batch_value(batch: &RuntimeRecordBatch, field: &str) -> Option<RuntimeValue> {
    assert_eq!(batch.batch().num_rows(), 1, "expected one Arrow row");
    batch
        .value(0, field)
        .expect("Arrow batch value must be readable")
}

fn vm_input_from_test_rows(
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
struct TestIngestHeaders<'a>(&'a [(&'a str, &'a str)]);

impl super::IngestMessageHeaders for TestIngestHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in self.0 {
            visit(name, value);
        }
    }
}

/// Builds one group's ingest metadata through the group builders, as the runtime does.
fn ingest_metadata_for_test(
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

async fn execute_filter_map_for_test(
    program: &super::CompiledProgramWithMaterializedInterest,
    record: RuntimeRow,
    branch_key: Option<&BranchKey>,
    metadata: Option<&super::IngestFilterMapMetadata>,
    now: Timestamp,
) -> Result<Option<RuntimeRow>, String> {
    super::execute_filter_map_on_record(
        &identifier("test_filter_map"),
        program,
        record,
        branch_key,
        metadata,
        &HashMap::default(),
        now,
    )
    .await
}

#[test]
fn domain_drain_status_reports_structured_emitter_publishing_state() {
    let runtime = super::Runtime::new();
    let domain = domain("default");
    let confirming = identifier("confirming");
    let retrying = identifier("retrying");
    let iceberg = identifier("iceberg");

    for (emitter, pending_messages) in [
        (&confirming, 3_usize),
        (&retrying, 2_usize),
        (&iceberg, 5_usize),
    ] {
        runtime.emitter_buffers.insert(
            super::RuntimeKey::new(domain.clone(), emitter.clone()),
            Arc::new(AtomicUsize::new(pending_messages)),
        );
    }

    let confirmation = runtime.begin_emitter_confirmation_wait(&domain, &confirming);
    runtime.record_emitter_transient_error_with_backoff(
        &domain,
        &retrying,
        "sensitive infrastructure detail that drain status must not expose",
        Duration::from_secs(2),
    );
    runtime.record_iceberg_commit_failure_with_backoff(
        &domain,
        &iceberg,
        "sensitive catalog detail that drain status must not expose",
        Duration::from_secs(3),
    );

    let status = runtime.domain_drain_status(&domain);

    assert_eq!(status.emitter_publishing.len(), 3);
    assert_eq!(
        status.emitter_publishing[0],
        super::EmitterPublishingDrainStatus {
            emitter: confirming,
            state: super::EmitterPublishingDrainState::AwaitingConfirmation,
            pending_messages: 3,
            retry_backoff: None,
            retry_wait: None,
        }
    );
    assert_eq!(status.emitter_publishing[1].emitter, iceberg);
    assert_eq!(
        status.emitter_publishing[1].state,
        super::EmitterPublishingDrainState::RetryingIcebergCommit
    );
    assert_eq!(
        status.emitter_publishing[1].retry_backoff,
        Some(Duration::from_secs(3))
    );
    assert!(
        status.emitter_publishing[1]
            .retry_wait
            .is_some_and(|wait| wait <= Duration::from_secs(3))
    );
    assert_eq!(status.emitter_publishing[2].emitter, retrying);
    assert_eq!(
        status.emitter_publishing[2].state,
        super::EmitterPublishingDrainState::RetryingInfrastructure
    );
    assert_eq!(
        status.emitter_publishing[2].retry_backoff,
        Some(Duration::from_secs(2))
    );
    let affected_emitters = [
        status.emitter_publishing[0].emitter.clone(),
        status.emitter_publishing[1].emitter.clone(),
        status.emitter_publishing[2].emitter.clone(),
    ]
    .into_iter()
    .map(|identifier| crate::registry::RegistryEntity {
        kind: ModelKind::Emitter,
        identifier,
    })
    .collect::<Vec<_>>();
    let entity_status = runtime
        .entity_drain_status(
            &domain,
            &[],
            &affected_emitters,
            EntityGatePurpose::ModelAlteration,
        )
        .emitter_publishing;
    assert_eq!(entity_status.len(), status.emitter_publishing.len());
    for (entity, domain) in entity_status.iter().zip(&status.emitter_publishing) {
        assert_eq!(entity.emitter, domain.emitter);
        assert_eq!(entity.state, domain.state);
        assert_eq!(entity.pending_messages, domain.pending_messages);
        assert_eq!(entity.retry_backoff, domain.retry_backoff);
        assert_eq!(entity.retry_wait.is_some(), domain.retry_wait.is_some());
    }

    drop(confirmation);
    assert!(
        runtime
            .domain_drain_status(&domain)
            .emitter_publishing
            .iter()
            .all(|status| status.emitter != identifier("confirming")),
        "a completed confirmation must disappear from drain status"
    );
}

fn expression(raw: &str) -> nervix_models::Expression {
    nervix_nspl::parse_expression(raw).expect("valid semantic expression")
}

fn construction(raw: &str) -> nervix_models::RouteConstruction {
    nervix_nspl::parse_route_construction(raw).expect("valid route construction")
}

#[tokio::test]
async fn scheduled_emitter_stop_keeps_a_failed_drain_task_available_for_retry() {
    let grace = Duration::from_millis(50);
    let started = Instant::now();
    let (commands, mut command_rx) = mpsc::channel(2);
    let (stop_signal, _stop_rx) = watch::channel(None);
    let task = tokio::spawn(async move {
        let Some(super::EmitterTaskCommand::Stop { deadline, response }) = command_rx.recv().await
        else {
            panic!("expected the first emitter stop command");
        };
        assert!(deadline > started);
        assert!(deadline <= started + grace + Duration::from_millis(10));
        let _ = response.send(Err("transport drain failed".to_string()));

        let Some(super::EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
            panic!("expected the retried emitter stop command");
        };
        let _ = response.send(Ok(()));
    });
    let scheduled = ScheduledEmitterTask {
        commands,
        stop_signal,
        task,
    };

    let failed = scheduled
        .stop(grace)
        .await
        .expect_err("a failed transport drain must fail the emitter stop");
    assert_eq!(failed.reason(), "transport drain failed");
    let scheduled = failed
        .into_task()
        .expect("a failed drain must leave the old emitter task available");

    scheduled
        .stop(grace)
        .await
        .expect("the retained emitter task must accept a later successful stop");
}

#[tokio::test]
async fn scheduled_emitter_stop_clears_signal_when_the_response_is_dropped() {
    let (commands, mut command_rx) = mpsc::channel(1);
    let (stop_signal, _stop_rx) = watch::channel(None);
    let task = tokio::spawn(async move {
        let Some(super::EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
            panic!("expected an emitter stop command");
        };
        drop(response);
    });
    let scheduled = ScheduledEmitterTask {
        commands,
        stop_signal,
        task,
    };

    let failed = scheduled
        .stop(Duration::from_millis(50))
        .await
        .expect_err("a dropped drain response must retain the scheduled task");
    assert_eq!(
        failed.reason(),
        "scheduled emitter task dropped its stop response"
    );
    assert!(
        failed
            .into_task()
            .expect("the failed stop must return its task")
            .stop_signal
            .borrow()
            .is_none(),
        "a dropped response must not leave the retained emitter interrupted"
    );
}

fn window_outputs(relay: &str, set: &str) -> ProcessorOutputs {
    ProcessorOutputs::new(vec![ProcessorOutput {
        relay: identifier(relay),
        construction: construction(set),
        flush_policy: None,
        message_error_policy: MessageErrorPolicy::Log,
        branch: None,
    }])
}

fn with_inherit_all(mut outputs: ProcessorOutputs) -> ProcessorOutputs {
    for output in &mut outputs.routes {
        output.construction.inherit = Some(nervix_models::Inheritance::All);
    }
    outputs
}

fn window_aggregate(set: &str) -> nervix_nspl::window_processor::aggregate::WindowAggregateProgram {
    lower_window_assignments(&construction(set))
        .expect("window route construction should lower")
        .inner
}

fn compile_window_aggregate_for_test(
    aggregate: &nervix_nspl::window_processor::aggregate::WindowAggregateProgram,
    input_type: ParseAsType,
    output_schema: &super::CompiledSchema,
) -> super::CompiledWindowAggregateProgram {
    let input_relay = identifier("events");
    let output_relay = identifier("summary");
    let input_schema = compile_schema(&CreateSchema {
        name: input_relay.clone(),
        fields: vec![SchemaField {
            name: identifier("latency"),
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

fn window_inputs(
    aggregate: &nervix_nspl::window_processor::aggregate::WindowAggregateProgram,
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

fn branch_key(fields: impl IntoIterator<Item = (Identifier, RuntimeValue)>) -> Option<BranchKey> {
    BranchKey::from_fields(fields)
        .expect("test branch key must be non-empty")
        .into()
}

fn concrete_branch_key(fields: impl IntoIterator<Item = (Identifier, RuntimeValue)>) -> BranchKey {
    branch_key(fields).expect("test branch key must be concrete")
}

fn string_branch_key(field: &str, value: &str) -> Option<BranchKey> {
    branch_key([(identifier(field), RuntimeValue::String(value.to_string()))])
}

#[test]
fn branch_key_rejects_empty_fields() {
    assert!(BranchKey::from_fields([]).is_err());
}

fn u32_branch_key(field: &str, value: u32) -> Option<BranchKey> {
    branch_key([(identifier(field), RuntimeValue::U32(value))])
}

fn key_label(key: &Option<BranchKey>) -> &str {
    key.as_ref().expect("test branch key must exist").as_str()
}

fn domain(raw: &str) -> Domain {
    Domain::parse(raw).expect("valid domain")
}

const TWO_ITEM_TEST_CHANNEL_CAPACITY: usize = 2;

fn nonzero_capacity(capacity: usize) -> NonZeroUsize {
    NonZeroUsize::new(capacity).expect("test relay capacity must be nonzero")
}

fn branched_by(relay: &str, fields: &[&str]) -> OutputBranch {
    if fields.is_empty() {
        OutputBranch::Unbranched
    } else {
        OutputBranch::BranchedBy {
            branch: identifier(&format!("by_{relay}")),
            assignments: branch_mappings(fields),
        }
    }
}

fn processor_branched_by(relay: &str, fields: &[&str]) -> BranchSelection {
    if fields.is_empty() {
        BranchSelection::unbranched()
    } else {
        BranchSelection::branched_by(identifier(&format!("by_{relay}")))
    }
}

fn branch_mappings(fields: &[&str]) -> Vec<Assignment> {
    fields
        .iter()
        .map(|field| Assignment {
            target: AssignmentTarget {
                scope: AssignmentTargetScope::Bare,
                field: identifier(field),
            },
            value: Expression::Field(FieldReference::scoped(
                FieldScope::Message,
                identifier(field),
            )),
        })
        .collect()
}

fn branch_model_tuple(
    schema: &str,
    relay: &str,
    _fields: &[&str],
) -> (ModelKind, Identifier, nervix_models::Model) {
    let branch = identifier(&format!("by_{relay}"));
    (
        ModelKind::Branch,
        branch.clone(),
        nervix_models::Model::Branch(CreateBranch {
            name: branch,
            schema: identifier(schema),
            ttl: "5m".to_string(),
            eviction: None,
        }),
    )
}

fn test_relay_boundary_services() -> Arc<super::RelayBoundaryServices> {
    Arc::new(super::RelayBoundaryServices::new(
        super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(
            STUPID_CHANNEL_CAPACITY_REMOVE_ME,
        )),
        0,
        0,
        Vec::new(),
        None,
    ))
}

fn test_ingestor_quiesce_control(
    runtime: &super::Runtime,
    domain: &Domain,
    ingestor: &Identifier,
    mode: IngestQuiesceMode,
) -> Arc<super::IngestorQuiesceControl> {
    let metric_labels = runtime
        .metrics
        .register_ingestor_quiesce(domain, ingestor, None);
    Arc::new(super::IngestorQuiesceControl::new(
        mode,
        runtime.metrics.clone(),
        metric_labels,
    ))
}

#[test]
fn ownership_handoff_stops_new_intake_and_dispatches_already_admitted_payloads() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let ingestor = identifier("source");
    let control =
        test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Drop);
    control.engage(super::IngestorQuiesceCause::OwnershipHandoff);

    assert!(control.should_skip_poll());
    assert!(control.should_suspend_intake());
    assert_eq!(control.endpoint_admission(), Err(None));
    assert!(matches!(
        control.intake(
            0,
            super::BufferedIngestPayload::new(
                b"admitted",
                super::BufferedIngestMetadata::without_headers(),
            ),
            false,
        ),
        super::IngestorQuiesceIntake::Dispatch(_)
    ));
    assert_eq!(control.counters().dropped_total, 0);

    control.release(super::IngestorQuiesceCause::OwnershipHandoff);
    assert!(!control.should_skip_poll());
}

#[test]
fn quiesce_buffer_enforces_drop_oldest_per_instance() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let ingestor = identifier("source");
    let control = test_ingestor_quiesce_control(
        &runtime,
        &domain,
        &ingestor,
        IngestQuiesceMode::Buffer {
            max_size: "5B".to_string(),
            overflow: IngestQuiesceOverflow::DropOldest,
        },
    );
    control.engage(super::IngestorQuiesceCause::EntityHold);

    assert!(matches!(
        control.intake(
            0,
            super::BufferedIngestPayload::new(
                b"one",
                super::BufferedIngestMetadata::without_headers(),
            ),
            false,
        ),
        super::IngestorQuiesceIntake::Buffered
    ));
    assert!(matches!(
        control.intake(
            0,
            super::BufferedIngestPayload::new(
                b"two",
                super::BufferedIngestMetadata::without_headers(),
            ),
            false,
        ),
        super::IngestorQuiesceIntake::Buffered
    ));
    assert_eq!(control.counters().buffered_records, 1);
    assert_eq!(control.counters().buffered_bytes, 3);
    assert_eq!(control.counters().dropped_total, 1);

    control.release(super::IngestorQuiesceCause::EntityHold);
    assert_eq!(
        control
            .pop_buffered(0)
            .expect("newest payload should remain")
            .payload(),
        b"two"
    );
}

#[test]
fn endpoint_quiesce_buffer_rejects_overflow_without_discarding_acknowledged_payloads() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let ingestor = identifier("source");
    let control = test_ingestor_quiesce_control(
        &runtime,
        &domain,
        &ingestor,
        IngestQuiesceMode::EndpointBuffer {
            max_size: "5B".to_string(),
        },
    );
    control.engage(super::IngestorQuiesceCause::EntityHold);

    assert!(matches!(
        control.intake(
            0,
            super::BufferedIngestPayload::new(
                b"kept",
                super::BufferedIngestMetadata::without_headers(),
            ),
            true,
        ),
        super::IngestorQuiesceIntake::Buffered
    ));
    assert!(matches!(
        control.intake(
            0,
            super::BufferedIngestPayload::new(
                b"no",
                super::BufferedIngestMetadata::without_headers(),
            ),
            true,
        ),
        super::IngestorQuiesceIntake::Rejected { retry_after: None }
    ));
    assert_eq!(control.counters().buffered_records, 1);
    assert_eq!(control.counters().rejected_total, 1);
    assert_eq!(control.counters().dropped_total, 0);

    control.release(super::IngestorQuiesceCause::EntityHold);
    assert_eq!(
        control
            .pop_buffered(0)
            .expect("the acknowledged payload must remain buffered")
            .payload(),
        b"kept"
    );
}

#[test]
fn source_replacement_waits_when_the_active_hold_mode_is_not_supported() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let ingestor = identifier("source");
    let control = test_ingestor_quiesce_control(
        &runtime,
        &domain,
        &ingestor,
        IngestQuiesceMode::EndpointBuffer {
            max_size: "1KiB".to_string(),
        },
    );
    control.engage(super::IngestorQuiesceCause::EntityHold);
    control.update_declared_source(&IngestSource::ZeroMq {
        client: identifier("zeromq"),
        mode: ZeroMqIngestMode::NoAckSequential,
        quiesce: IngestQuiesceMode::Suspend,
    });

    assert!(control.should_suspend_intake());
    control.release(super::IngestorQuiesceCause::EntityHold);
    assert_eq!(control.mode(), IngestQuiesceMode::Suspend);
    assert!(!control.should_suspend_intake());
}

#[test]
fn memory_pressure_turns_buffer_into_zero_capacity_without_losing_existing_payloads() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let ingestor = identifier("source");
    let control = test_ingestor_quiesce_control(
        &runtime,
        &domain,
        &ingestor,
        IngestQuiesceMode::Buffer {
            max_size: "1KiB".to_string(),
            overflow: IngestQuiesceOverflow::DropNewest,
        },
    );
    control.engage(super::IngestorQuiesceCause::EntityHold);
    assert!(matches!(
        control.intake(
            0,
            super::BufferedIngestPayload::new(
                b"retained",
                super::BufferedIngestMetadata::without_headers(),
            ),
            false,
        ),
        super::IngestorQuiesceIntake::Buffered
    ));

    control.engage(super::IngestorQuiesceCause::MemoryPressure);
    assert!(matches!(
        control.intake(
            0,
            super::BufferedIngestPayload::new(
                b"discarded",
                super::BufferedIngestMetadata::without_headers(),
            ),
            false,
        ),
        super::IngestorQuiesceIntake::Dropped
    ));
    assert_eq!(control.counters().buffered_records, 1);
    assert_eq!(control.counters().dropped_total, 1);

    control.release(super::IngestorQuiesceCause::MemoryPressure);
    control.release(super::IngestorQuiesceCause::EntityHold);
    assert_eq!(
        control
            .pop_buffered(0)
            .expect("pre-pressure payload should remain")
            .payload(),
        b"retained"
    );
}

#[tokio::test]
async fn memory_pressure_quiesces_registered_ingestors_without_stopping_them() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let ingestor = identifier("source");
    let key = super::RuntimeKey::new(domain.clone(), ingestor.clone());
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let stopped = Arc::new(AtomicBool::new(false));
    let task_stopped = stopped.clone();
    let task = tokio::spawn(async move {
        let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
        task_stopped.store(true, Ordering::SeqCst);
    });

    runtime.ingestors.insert(
        key.clone(),
        super::IngestorRuntime::Background {
            shutdown: shutdown_tx,
            branched: Vec::new(),
            tasks: vec![task],
        },
    );
    runtime.ingestor_quiescence.insert(
        key.clone(),
        test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Suspend),
    );

    assert_eq!(runtime.pause_ingestors_for_memory_pressure().await, 1);
    assert!(runtime.ingestors_paused_for_memory_pressure());
    assert!(!stopped.load(Ordering::SeqCst));
    assert!(runtime.ingestors.get(&key).is_some());
    assert_eq!(
        runtime
            .ingestor_quiescence
            .get(&key)
            .and_then(|control| control.cause()),
        Some(super::IngestorQuiesceCause::MemoryPressure)
    );
    assert!(
        runtime
            .resume_one_ingestor_after_memory_pressure()
            .await
            .expect("resume should succeed")
    );
    assert!(
        !runtime
            .resume_one_ingestor_after_memory_pressure()
            .await
            .expect("pause should clear after the last ingestor resumes")
    );
    runtime
        .stop_ingestor(&domain, &ingestor)
        .await
        .expect("test ingestor should stop");
}

#[tokio::test]
async fn memory_pressure_resume_clears_pause_when_no_ingestors_are_pending() {
    let runtime = super::Runtime::default();

    assert_eq!(runtime.pause_ingestors_for_memory_pressure().await, 0);
    assert!(runtime.ingestors_paused_for_memory_pressure());
    assert!(
        !runtime
            .resume_one_ingestor_after_memory_pressure()
            .await
            .expect("resume should succeed")
    );
    assert!(!runtime.ingestors_paused_for_memory_pressure());
}

#[tokio::test]
async fn ack_alive_resets_ingestor_ack_timeout() {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (acks, completion) = AckSet::root();
    let ack_task = acks.clone();

    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        ack_task.ack_alive();
        sleep(Duration::from_millis(150)).await;
        ack_task.ack_success();
    });

    assert_eq!(
        super::Runtime::await_ack_completion(
            &mut shutdown_rx,
            completion,
            Duration::from_millis(200),
        )
        .await,
        Some(AckOutcome::Ack)
    );
    drop(shutdown_tx);
}

#[tokio::test]
async fn remote_ack_alive_packet_resets_ingestor_ack_timeout() {
    let runtime = super::Runtime::default();
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (acks, completion) = AckSet::root();
    runtime.pending_remote_acks.insert(7, acks);
    let runtime_task = runtime.clone();

    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
            ack_id: 7,
            outcome: RemoteAckOutcome::Alive,
        });
        sleep(Duration::from_millis(150)).await;
        runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
            ack_id: 7,
            outcome: RemoteAckOutcome::Ack,
        });
    });

    assert_eq!(
        super::Runtime::await_ack_completion(
            &mut shutdown_rx,
            completion,
            Duration::from_millis(200),
        )
        .await,
        Some(AckOutcome::Ack)
    );
    assert!(
        runtime.pending_remote_acks.get(&7).is_none(),
        "terminal ack must clear the pending remote ack"
    );
    drop(shutdown_tx);
}

#[tokio::test]
async fn remote_relay_admission_alive_resets_dispatch_timeout() {
    let runtime = super::Runtime::default();
    let (admission_tx, admission_rx) = mpsc::unbounded_channel();
    runtime.pending_relay_admissions.insert(9, admission_tx);
    let runtime_task = runtime.clone();

    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
            ack_id: 9,
            outcome: RemoteAckOutcome::Alive,
        });
        sleep(Duration::from_millis(150)).await;
        runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
            ack_id: 9,
            outcome: RemoteAckOutcome::Ack,
        });
    });

    assert_eq!(
        super::RemoteDispatcher::await_relay_admission(
            "relay-owner",
            admission_rx,
            Duration::from_millis(200),
        )
        .await,
        Ok(())
    );
    assert!(
        runtime.pending_relay_admissions.get(&9).is_none(),
        "terminal admission ack must clear pending admission state"
    );
}

#[tokio::test]
async fn forwarding_to_a_remote_relay_owner_extends_the_ack_chain() {
    let (acks, completion) = AckSet::root();
    let forwarded = super::RemoteDispatcher::forwarded_ack(&acks);
    let completion = completion.wait();
    tokio::pin!(completion);

    acks.ack_success();
    assert!(
        timeout(Duration::from_millis(50), &mut completion)
            .await
            .is_err(),
        "local producer completion must wait for the remote relay path"
    );
    forwarded.ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), &mut completion)
            .await
            .expect("remote relay completion should resolve the producer ACK"),
        AckOutcome::Ack
    );
}

#[tokio::test]
async fn relay_gate_holds_nonowner_dispatch_before_the_remote_slot() {
    let services = test_relay_boundary_services();
    services.replace_owner_node(Some("node-2".to_string()));
    let gate = services.fanout.dispatch_gate();
    let lease = super::RelayDispatchGateLease::engage(
        gate,
        Instant::now() + Duration::from_secs(1),
        "relay owner is moving",
    );
    let task_services = services.clone();
    let dispatch = tokio::spawn(async move {
        task_services
            .dispatch_to_owner(
                &domain("default"),
                &identifier("orders"),
                &quiesce_test_batch(),
            )
            .await
    });

    tokio::task::yield_now().await;
    assert!(
        !dispatch.is_finished(),
        "a gated nonowner must not enter its remote dispatch slot"
    );

    drop(lease);
    timeout(Duration::from_secs(1), dispatch)
        .await
        .expect("releasing the gate should wake the nonowner dispatch")
        .expect("dispatch task should join")
        .expect_err("the isolated test has no remote dispatcher");
}

#[tokio::test]
async fn relay_gate_allows_admitted_owner_batches_to_reach_consumers() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("orders");
    let services = test_relay_boundary_services();
    let mut consumer = services.add_local_runtime_consumer(AckMode::Attached);
    let gate = services.fanout.dispatch_gate();
    let mut lease = super::RelayDispatchGateLease::engage(
        gate,
        Instant::now() + Duration::from_secs(1),
        "relay owner is moving",
    );
    assert!(lease.wait_quiescent().await);

    timeout(
        Duration::from_millis(100),
        services.fanout_owner_batch(
            &runtime.metrics,
            &domain,
            &relay,
            None,
            &quiesce_test_batch(),
        ),
    )
    .await
    .expect("the gate must not pause a batch already admitted to the owner buffer")
    .expect("the owner should fan out the admitted batch");
    timeout(Duration::from_secs(1), consumer.recv())
        .await
        .expect("the attached consumer should receive the admitted batch")
        .expect("the attached consumer should remain open");

    services.remove_local_runtime_consumer(AckMode::Attached);
}

#[tokio::test]
async fn window_aggregate_evaluator_computes_vm_expression_percentile_and_array() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("count"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("adjusted_count"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("p50"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("latencies"),
                ty: ParseAsType::Array {
                    element: Box::new(ParseAsType::F64),
                    len: 2,
                },
                optional: false,
                sensitive: false,
            },
        ],
    });
    let aggregate = window_aggregate(
        "SET count = COUNT(input.latency), adjusted_count = COUNT(input.latency) + 2, p50 = \
         PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), latencies = \
         [PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), \
         PERCENTILE_LINEAR_HISTOGRAM(input.latency, 100, 10, 0, 100, '2s')]",
    );
    let mut state = WindowProcessorState::new(&aggregate);
    for value in [10.0, 20.0, 30.0] {
        state
            .push_message(
                &aggregate,
                Timestamp::now(),
                RelayMessage {
                    key: None,
                    record: test_runtime_row([(
                        "latency".to_string(),
                        RuntimeValue::F64(OrderedFloat(value)),
                    )]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::F64(OrderedFloat(value))),
            )
            .expect("aggregate state should accept message");
    }

    let compiled = compile_window_aggregate_for_test(&aggregate, ParseAsType::F64, &output_schema);
    let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
        .await
        .expect("aggregate should evaluate");

    assert_eq!(batch_value(&record, "count"), Some(RuntimeValue::I64(3)));
    assert_eq!(
        batch_value(&record, "adjusted_count"),
        Some(RuntimeValue::I64(5))
    );
    assert_eq!(
        batch_value(&record, "p50"),
        Some(RuntimeValue::F64(OrderedFloat(25.0)))
    );
    assert_eq!(
        batch_value(&record, "latencies"),
        Some(RuntimeValue::Array(vec![
            RuntimeValue::F64(OrderedFloat(25.0)),
            RuntimeValue::F64(OrderedFloat(35.0)),
        ]))
    );
}

#[tokio::test]
async fn window_aggregate_inputs_evaluate_one_batch_in_one_vm_execution() {
    let output_schema = test_schema(&[("adjusted_total", ParseAsType::I64)]);
    let aggregate = window_aggregate("SET adjusted_total = SUM(120 / input.latency)");
    let compiled = compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
    let rows = [10, 0, 30]
        .into_iter()
        .map(|latency| test_runtime_row([("latency".to_string(), RuntimeValue::I64(latency))]))
        .collect::<Vec<_>>();
    let carrier = RuntimeRecordBatch::from_rows(rows[0].batch().schema(), rows.iter())
        .expect("window input rows should form one Arrow batch");
    super::WINDOW_AGGREGATE_INPUT_VM_EXECUTIONS.store(0, Ordering::Relaxed);

    let evaluated =
        super::evaluate_window_aggregate_inputs(&compiled, &carrier, Timestamp::from_unix_nanos(1))
            .await
            .expect("batched window aggregate inputs should evaluate");

    assert_eq!(
        super::WINDOW_AGGREGATE_INPUT_VM_EXECUTIONS.load(Ordering::Relaxed),
        1,
        "all input rows must share one aggregate-input VM execution"
    );
    assert_eq!(evaluated.len(), 3);
    let first = evaluated[0]
        .as_ref()
        .expect("the first window input row should evaluate");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].value, Some(RuntimeValue::I64(12)));
    assert!(
        evaluated[1]
            .as_ref()
            .expect_err("division by zero should fail only its input row")
            .contains("division_by_zero")
    );
    let third = evaluated[2]
        .as_ref()
        .expect("the third window input row should evaluate");
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].value, Some(RuntimeValue::I64(4)));
}

#[tokio::test]
async fn window_linear_histogram_percentiles_share_accumulator_by_config() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("p50"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("p90"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("p50_other_range"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
        ],
    });
    let aggregate = window_aggregate(
        "SET p50 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s'), p90 = \
         PERCENTILE_LINEAR_HISTOGRAM(input.latency, 90, 10, 0, 100, '2s'), p50_other_range = \
         PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 200, '2s')",
    );
    let mut state = WindowProcessorState::new(&aggregate);

    assert_eq!(
        state.accumulators.len(),
        2,
        "same input and histogram config should share one accumulator"
    );

    for value in [10, 20, 30] {
        state
            .push_message(
                &aggregate,
                Timestamp::now(),
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(value))]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(value)),
            )
            .expect("aggregate state should accept message");
    }

    let compiled = compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
    let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
        .await
        .expect("aggregate should evaluate");

    assert_eq!(
        batch_value(&record, "p50"),
        Some(RuntimeValue::F64(OrderedFloat(25.0)))
    );
    assert_eq!(
        batch_value(&record, "p90"),
        Some(RuntimeValue::F64(OrderedFloat(35.0)))
    );
    assert_eq!(
        batch_value(&record, "p50_other_range"),
        Some(RuntimeValue::F64(OrderedFloat(30.0)))
    );
}

#[test]
fn window_advance_removes_step_messages() {
    let aggregate = window_aggregate("SET count = COUNT(input.latency)");
    let mut state = WindowProcessorState::new(&aggregate);
    for sequence in 0..5 {
        state
            .push_message(
                &aggregate,
                Timestamp::now(),
                RelayMessage {
                    key: None,
                    record: test_runtime_row([(
                        "latency".to_string(),
                        RuntimeValue::I64(sequence as i64),
                    )]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(sequence as i64)),
            )
            .expect("aggregate state should accept message");
    }

    advance_window(&mut state, &aggregate, Some(2), None, Timestamp::now())
        .expect("window should advance");

    assert_eq!(state.entries.len(), 3);
    assert_eq!(state.entries.front().map(|entry| entry.sequence), Some(2));
    assert_eq!(
        state.accumulators[0]
            .evaluate(WindowAggregateFunction::Count, None)
            .expect("count should evaluate"),
        RuntimeValue::I64(3)
    );
}

#[tokio::test]
async fn linear_histogram_zero_delay_removes_step_values_immediately() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![nervix_models::SchemaField {
            name: identifier("p0"),
            ty: ParseAsType::F64,
            optional: false,
            sensitive: false,
        }],
    });
    let aggregate = window_aggregate(
        "SET p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '0ms')",
    );
    let mut state = WindowProcessorState::new(&aggregate);
    for (timestamp, value) in [
        (Timestamp::from_unix_nanos(0), 10),
        (Timestamp::from_unix_nanos(1_000_000_000), 90),
    ] {
        state
            .push_message(
                &aggregate,
                timestamp,
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(value))]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(value)),
            )
            .expect("aggregate state should accept message");
    }

    advance_window(
        &mut state,
        &aggregate,
        Some(1),
        None,
        Timestamp::from_unix_nanos(1_000_000_000),
    )
    .expect("window should advance");
    let compiled = compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
    let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
        .await
        .expect("aggregate should evaluate");

    assert_eq!(
        batch_value(&record, "p0"),
        Some(RuntimeValue::F64(OrderedFloat(95.0)))
    );
}

#[tokio::test]
async fn linear_histogram_delay_retains_removed_step_values_until_expired() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![nervix_models::SchemaField {
            name: identifier("p0"),
            ty: ParseAsType::F64,
            optional: false,
            sensitive: false,
        }],
    });
    let aggregate = window_aggregate(
        "SET p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '2s')",
    );
    let mut state = WindowProcessorState::new(&aggregate);
    for (timestamp, value) in [
        (Timestamp::from_unix_nanos(0), 10),
        (Timestamp::from_unix_nanos(1_000_000_000), 90),
    ] {
        state
            .push_message(
                &aggregate,
                timestamp,
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(value))]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(value)),
            )
            .expect("aggregate state should accept message");
    }

    advance_window(
        &mut state,
        &aggregate,
        Some(1),
        None,
        Timestamp::from_unix_nanos(1_000_000_000),
    )
    .expect("window should advance");
    let compiled = compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
    let retained = evaluate_window_aggregate(&compiled, &state, &output_schema)
        .await
        .expect("aggregate should evaluate while delay retains value");
    assert_eq!(
        batch_value(&retained, "p0"),
        Some(RuntimeValue::F64(OrderedFloat(15.0)))
    );

    state
        .push_message(
            &aggregate,
            Timestamp::from_unix_nanos(2_000_000_000),
            RelayMessage {
                key: None,
                record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(90))]),
                acks: AckSet::empty(),
            },
            window_inputs(&aggregate, RuntimeValue::I64(90)),
        )
        .expect("aggregate state should accept message before delay expires");
    let still_retained = evaluate_window_aggregate(&compiled, &state, &output_schema)
        .await
        .expect("aggregate should evaluate before delay expires");
    assert_eq!(
        batch_value(&still_retained, "p0"),
        Some(RuntimeValue::F64(OrderedFloat(15.0)))
    );

    state
        .push_message(
            &aggregate,
            Timestamp::from_unix_nanos(4_000_000_000),
            RelayMessage {
                key: None,
                record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(90))]),
                acks: AckSet::empty(),
            },
            window_inputs(&aggregate, RuntimeValue::I64(90)),
        )
        .expect("aggregate state should accept message after delay expires");
    let expired = evaluate_window_aggregate(&compiled, &state, &output_schema)
        .await
        .expect("aggregate should evaluate after delay expires");
    assert_eq!(
        batch_value(&expired, "p0"),
        Some(RuntimeValue::F64(OrderedFloat(95.0)))
    );
}

#[tokio::test]
async fn linear_histogram_delay_exposes_timeout_deadline_without_new_messages() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![nervix_models::SchemaField {
            name: identifier("p0"),
            ty: ParseAsType::F64,
            optional: false,
            sensitive: false,
        }],
    });
    let aggregate = window_aggregate(
        "SET p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '2s')",
    );
    let mut state = WindowProcessorState::new(&aggregate);
    for (timestamp, value) in [
        (Timestamp::from_unix_nanos(0), 10),
        (Timestamp::from_unix_nanos(1_000_000_000), 90),
    ] {
        state
            .push_message(
                &aggregate,
                timestamp,
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(value))]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(value)),
            )
            .expect("aggregate state should accept message");
    }

    advance_window(
        &mut state,
        &aggregate,
        Some(1),
        None,
        Timestamp::from_unix_nanos(1_000_000_000),
    )
    .expect("window should advance");
    assert_eq!(
        state.next_timeout_deadline(),
        Some(Timestamp::from_unix_nanos(3_000_000_000))
    );

    assert!(
        !state
            .purge_timeouts(Timestamp::from_unix_nanos(2_999_999_999))
            .expect("early purge check should succeed")
    );
    assert!(
        state
            .purge_timeouts(Timestamp::from_unix_nanos(3_000_000_000))
            .expect("due purge should succeed")
    );
    assert_eq!(state.next_timeout_deadline(), None);

    let compiled = compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
    let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
        .await
        .expect("aggregate should evaluate after timeout purge");
    assert_eq!(
        batch_value(&record, "p0"),
        Some(RuntimeValue::F64(OrderedFloat(95.0)))
    );
}

#[tokio::test]
async fn window_aggregate_state_updates_first_last_min_max_and_sum() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("first_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("last_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("min_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("max_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("total_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
        ],
    });
    let aggregate = window_aggregate(
        "SET first_latency = FIRST(input.latency), last_latency = LAST(input.latency), \
         min_latency = MIN(input.latency), max_latency = MAX(input.latency), total_latency = \
         SUM(input.latency)",
    );
    let mut state = WindowProcessorState::new(&aggregate);
    for value in [30, 10, 20] {
        state
            .push_message(
                &aggregate,
                Timestamp::now(),
                RelayMessage {
                    key: None,
                    record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(value))]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(value)),
            )
            .expect("aggregate state should accept message");
    }

    assert_eq!(
        state.accumulators.len(),
        3,
        "FIRST/LAST and MIN/MAX should each share one physical structure"
    );
    let compiled = compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
    let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
        .await
        .expect("aggregate should evaluate");

    assert_eq!(
        batch_value(&record, "first_latency"),
        Some(RuntimeValue::I64(30))
    );
    assert_eq!(
        batch_value(&record, "last_latency"),
        Some(RuntimeValue::I64(20))
    );
    assert_eq!(
        batch_value(&record, "min_latency"),
        Some(RuntimeValue::I64(10))
    );
    assert_eq!(
        batch_value(&record, "max_latency"),
        Some(RuntimeValue::I64(30))
    );
    assert_eq!(
        batch_value(&record, "total_latency"),
        Some(RuntimeValue::I64(60))
    );

    advance_window(&mut state, &aggregate, Some(1), None, Timestamp::now())
        .expect("window should advance");
    let record = evaluate_window_aggregate(&compiled, &state, &output_schema)
        .await
        .expect("aggregate should evaluate after removal");

    assert_eq!(
        batch_value(&record, "first_latency"),
        Some(RuntimeValue::I64(10))
    );
    assert_eq!(
        batch_value(&record, "last_latency"),
        Some(RuntimeValue::I64(20))
    );
    assert_eq!(
        batch_value(&record, "min_latency"),
        Some(RuntimeValue::I64(10))
    );
    assert_eq!(
        batch_value(&record, "max_latency"),
        Some(RuntimeValue::I64(20))
    );
    assert_eq!(
        batch_value(&record, "total_latency"),
        Some(RuntimeValue::I64(30))
    );
}

#[test]
fn window_message_timestamp_uses_low_watermark() {
    let message = RelayMessage {
        key: None,
        record: test_runtime_row([]).with_metadata(
            RuntimeRecordMetadata::from_ingested_at_watermarks(
                Timestamp::from_unix_nanos(10),
                Timestamp::from_unix_nanos(20),
            ),
        ),
        acks: AckSet::empty(),
    };

    let timestamp = message_timestamp(&message);

    assert_eq!(timestamp, Timestamp::from_unix_nanos(10));
}

#[test]
fn window_output_metadata_uses_window_low_and_emit_high_watermark() {
    let aggregate = window_aggregate("SET count = COUNT(input.latency)");
    let mut state = WindowProcessorState::new(&aggregate);
    for timestamp in [
        Timestamp::from_unix_nanos(30),
        Timestamp::from_unix_nanos(10),
        Timestamp::from_unix_nanos(20),
    ] {
        state
            .push_message(
                &aggregate,
                timestamp,
                RelayMessage {
                    key: None,
                    record: test_runtime_row([(
                        "latency".to_string(),
                        RuntimeValue::I64(timestamp.unix_nanos()),
                    )]),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(timestamp.unix_nanos())),
            )
            .expect("aggregate state should accept message");
    }

    let metadata = window_output_metadata(&state, Timestamp::from_unix_nanos(40))
        .expect("non-empty window should emit metadata");

    assert_eq!(
        metadata.ingested_at_low_watermark(),
        Timestamp::from_unix_nanos(10)
    );
    assert_eq!(
        metadata.ingested_at_high_watermark(),
        Timestamp::from_unix_nanos(40)
    );
}

#[tokio::test]
async fn window_processor_state_snapshot_roundtrips_entries_and_accumulators() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("count"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("first_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("p50"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
        ],
    });
    let aggregate = window_aggregate(
        "SET count = COUNT(input.latency), first_latency = FIRST(input.latency), p50 = \
         PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 10, 0, 100, '2s')",
    );
    let mut state = WindowProcessorState::new(&aggregate);
    for (timestamp, value) in [
        (Timestamp::from_unix_nanos(10), 10),
        (Timestamp::from_unix_nanos(20), 30),
    ] {
        state
            .push_message(
                &aggregate,
                timestamp,
                RelayMessage {
                    key: string_branch_key("tenant", "acme"),
                    record: test_runtime_row([("latency".to_string(), RuntimeValue::I64(value))])
                        .with_metadata(RuntimeRecordMetadata::from_ingested_at_watermarks(
                            timestamp, timestamp,
                        )),
                    acks: AckSet::empty(),
                },
                window_inputs(&aggregate, RuntimeValue::I64(value)),
            )
            .expect("window should accept message");
    }

    let input_schema = test_schema(&[("latency", ParseAsType::I64)]);
    let restored = WindowProcessorState::from_snapshot(
        &aggregate,
        input_schema.as_ref(),
        state.to_snapshot().expect("snapshot should encode"),
    )
    .expect("snapshot should restore");
    let compiled = compile_window_aggregate_for_test(&aggregate, ParseAsType::I64, &output_schema);
    let record = evaluate_window_aggregate(&compiled, &restored, &output_schema)
        .await
        .expect("restored aggregate should evaluate");

    assert_eq!(restored.entries.len(), 2);
    assert_eq!(
        key_label(&restored.entries.front().unwrap().message.key),
        r#"{"tenant":"acme"}"#
    );
    assert_eq!(batch_value(&record, "count"), Some(RuntimeValue::I64(2)));
    assert_eq!(
        batch_value(&record, "first_latency"),
        Some(RuntimeValue::I64(10))
    );
    assert_eq!(
        batch_value(&record, "p50"),
        Some(RuntimeValue::F64(OrderedFloat(35.0)))
    );
}

fn paced_domain_state(raw: &str) -> DomainState {
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

fn test_schema(fields: &[(&str, ParseAsType)]) -> Arc<super::CompiledSchema> {
    Arc::new(compile_schema(&CreateSchema {
        name: identifier("test_schema"),
        fields: fields
            .iter()
            .map(|(name, ty)| nervix_models::SchemaField {
                name: identifier(name),
                ty: ty.clone(),
                optional: false,
                sensitive: false,
            })
            .collect(),
    }))
}

fn test_optional_schema(fields: &[(&str, ParseAsType, bool)]) -> Arc<super::CompiledSchema> {
    Arc::new(compile_schema(&CreateSchema {
        name: identifier("test_schema"),
        fields: fields
            .iter()
            .map(|(name, ty, optional)| nervix_models::SchemaField {
                name: identifier(name),
                ty: ty.clone(),
                optional: *optional,
                sensitive: false,
            })
            .collect(),
    }))
}

fn wasm_input_for_records(
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
    super::wasm_envelope_from_relay_batch(&batch, &mut next_token)
        .expect("WASM input envelope must build")
}

fn wasm_input_for_values(
    schema: &Arc<super::CompiledSchema>,
    values: &[i32],
) -> (WasmEnvelope, super::WasmAckMap) {
    let records = values
        .iter()
        .map(|value| test_runtime_row([("value".to_string(), RuntimeValue::I32(*value))]))
        .collect();
    wasm_input_for_records(schema, records)
}

fn wasm_input_acks(envelope: &WasmEnvelope) -> &WasmAckSidecar {
    envelope
        .input_acks()
        .expect("test envelope must be a WASM input")
}

fn validate_wasm_test_outputs(
    input_schema: &Arc<super::CompiledSchema>,
    output_schema: &Arc<super::CompiledSchema>,
    ack_map: &super::WasmAckMap,
    outputs: Vec<WasmEnvelope>,
) -> Result<Vec<super::WasmMaterializedOutput>, super::WasmOutputError> {
    validate_wasm_test_output_groups(
        input_schema,
        vec![("output", Arc::clone(output_schema))],
        ack_map,
        outputs,
    )
}

fn validate_wasm_test_output_groups(
    input_schema: &Arc<super::CompiledSchema>,
    schemas: Vec<(&str, Arc<super::CompiledSchema>)>,
    ack_map: &super::WasmAckMap,
    outputs: Vec<WasmEnvelope>,
) -> Result<Vec<super::WasmMaterializedOutput>, super::WasmOutputError> {
    let output_schemas = schemas
        .into_iter()
        .map(|(relay, schema)| (identifier(relay), schema))
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
    super::WasmOutputValidator {
        ack_map,
        input_schema,
        output_schemas: &output_schemas,
        output_routes: &output_routes,
    }
    .validate(outputs)
}

fn wasm_test_output(columns: Vec<WasmOutputColumnRef>, rows: Vec<WasmOutputRow>) -> WasmEnvelope {
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

fn wasm_test_generated_output(
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

fn wasm_guest_column(field: arrow_schema::Field, array: ArrayRef) -> Vec<u8> {
    let schema = StdArc::new(ArrowSchema::new(vec![field.with_name("")]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![array]).expect("guest column batch must build");
    wasm_guest_stream(schema, &[batch])
}

fn wasm_generated_pool(fields: Vec<arrow_schema::Field>, arrays: Vec<ArrayRef>) -> Vec<u8> {
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

fn wasm_guest_stream(schema: StdArc<ArrowSchema>, batches: &[RecordBatch]) -> Vec<u8> {
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

#[test]
fn wasm_input_envelope_retains_one_shared_source_batch_and_source_tokens() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (envelope, ack_map) = wasm_input_for_values(&schema, &[10, 20, 30]);
    let WasmEnvelope::Input {
        arrow_ipc_batch,
        acks,
    } = envelope
    else {
        panic!("host must construct an input envelope");
    };

    assert!(!arrow_ipc_batch.is_empty());
    assert_eq!(acks.rows.len(), 3);
    for (row, expected_token) in acks.rows.iter().zip(1_u64..) {
        assert_eq!(row.tokens, vec![WasmAckToken(expected_token)]);
        assert_eq!(row.source_token, Some(WasmAckToken(expected_token)));
    }
    let first = ack_map.get(&1).expect("first token must exist");
    for (input_row, token) in (1_u64..=3).enumerate() {
        let context = ack_map.get(&token).expect("token context must exist");
        assert!(Arc::ptr_eq(&first.input_batch, &context.input_batch));
        assert_eq!(context.input_row, input_row);
    }
}

#[test]
fn wasm_identity_input_reference_reuses_exact_source_array() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (input, ack_map) = wasm_input_for_values(&schema, &[10, 20, 30]);
    let source = ack_map[&1].input_batch.batch().column(0).clone();
    let outputs = validate_wasm_test_outputs(
        &schema,
        &schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            wasm_input_acks(&input).rows.clone(),
        )],
    )
    .expect("identity reference must materialize");

    assert!(StdArc::ptr_eq(&source, outputs[0].batch.batch().column(0)));
}

#[test]
fn wasm_contiguous_input_reference_shares_source_buffers() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (input, ack_map) = wasm_input_for_values(&schema, &[10, 20, 30, 40]);
    let rows = wasm_input_acks(&input).rows[1..3].to_vec();
    let outputs = validate_wasm_test_outputs(
        &schema,
        &schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            rows,
        )],
    )
    .expect("contiguous reference must materialize");
    let source_data = ack_map[&1].input_batch.batch().column(0).to_data();
    let output_data = outputs[0].batch.batch().column(0).to_data();

    assert_eq!(
        output_data.buffers()[0].as_ptr(),
        source_data.buffers()[0]
            .as_ptr()
            .wrapping_add(std::mem::size_of::<i32>())
    );
    let values = outputs[0]
        .batch
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("output must be I32");
    assert_eq!(values.values().as_ref(), &[20, 30]);
}

#[test]
fn wasm_general_input_selection_filters_reorders_and_duplicates_rows() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (input, ack_map) = wasm_input_for_values(&schema, &[10, 20, 30, 40]);
    let input_rows = wasm_input_acks(&input).rows.clone();
    let rows = vec![
        input_rows[3].clone(),
        input_rows[1].clone(),
        input_rows[1].clone(),
    ];
    let outputs = validate_wasm_test_outputs(
        &schema,
        &schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            rows,
        )],
    )
    .expect("general selection must materialize");
    let values = outputs[0]
        .batch
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("output must be I32");

    assert_eq!(values.values().as_ref(), &[40, 20, 20]);
}

#[test]
fn wasm_input_references_materialize_rows_from_multiple_retained_batches() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (first_input, mut ack_map) = wasm_input_for_values(&schema, &[10]);
    let (second_input, mut second_ack_map) = wasm_input_for_values(&schema, &[20]);
    let second_context = second_ack_map.remove(&1).expect("second token must exist");
    ack_map.insert(2, second_context);
    let mut rows = wasm_input_acks(&first_input).rows.clone();
    let mut second_row = wasm_input_acks(&second_input).rows.clone().remove(0);
    second_row.tokens = vec![WasmAckToken(2)];
    second_row.source_token = Some(WasmAckToken(2));
    rows.push(second_row);

    let outputs = validate_wasm_test_outputs(
        &schema,
        &schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            rows,
        )],
    )
    .expect("live sources retained across batches must materialize");
    let values = outputs[0]
        .batch
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("output must be I32");

    assert_eq!(values.values().as_ref(), &[10, 20]);
}

#[test]
fn wasm_identity_references_support_every_internal_arrow_field_kind() {
    let schema = test_schema(&[
        ("u8", ParseAsType::U8),
        ("i8", ParseAsType::I8),
        ("u16", ParseAsType::U16),
        ("i16", ParseAsType::I16),
        ("u32", ParseAsType::U32),
        ("i32", ParseAsType::I32),
        ("u64", ParseAsType::U64),
        ("i64", ParseAsType::I64),
        ("bool", ParseAsType::Bool),
        ("string", ParseAsType::String),
        ("datetime", ParseAsType::Datetime),
        ("f32", ParseAsType::F32),
        ("f64", ParseAsType::F64),
        (
            "array",
            ParseAsType::Array {
                element: Box::new(ParseAsType::I32),
                len: 2,
            },
        ),
        (
            "vec",
            ParseAsType::Vec {
                element: Box::new(ParseAsType::String),
            },
        ),
    ]);
    let record = test_runtime_row([
        ("u8".to_string(), RuntimeValue::U8(1)),
        ("i8".to_string(), RuntimeValue::I8(-2)),
        ("u16".to_string(), RuntimeValue::U16(3)),
        ("i16".to_string(), RuntimeValue::I16(-4)),
        ("u32".to_string(), RuntimeValue::U32(5)),
        ("i32".to_string(), RuntimeValue::I32(-6)),
        ("u64".to_string(), RuntimeValue::U64(7)),
        ("i64".to_string(), RuntimeValue::I64(-8)),
        ("bool".to_string(), RuntimeValue::Bool(true)),
        (
            "string".to_string(),
            RuntimeValue::String("value".to_string()),
        ),
        (
            "datetime".to_string(),
            RuntimeValue::Datetime(
                chrono::DateTime::parse_from_rfc3339("2026-07-13T12:34:56Z")
                    .expect("timestamp must parse"),
            ),
        ),
        ("f32".to_string(), RuntimeValue::F32(OrderedFloat(1.5))),
        ("f64".to_string(), RuntimeValue::F64(OrderedFloat(2.5))),
        (
            "array".to_string(),
            RuntimeValue::Array(vec![RuntimeValue::I32(9), RuntimeValue::I32(10)]),
        ),
        (
            "vec".to_string(),
            RuntimeValue::Vec(vec![
                RuntimeValue::String("a".to_string()),
                RuntimeValue::String("b".to_string()),
            ]),
        ),
    ]);
    let (input, ack_map) = wasm_input_for_records(&schema, vec![record]);
    let source_columns = ack_map[&1].input_batch.batch().columns().to_vec();
    let outputs = validate_wasm_test_outputs(
        &schema,
        &schema,
        &ack_map,
        vec![wasm_test_output(
            (0..schema.arrow_schema().fields().len())
                .map(|column_index| WasmOutputColumnRef::Input {
                    column_index: u32::try_from(column_index).expect("field index must fit u32"),
                })
                .collect(),
            wasm_input_acks(&input).rows.clone(),
        )],
    )
    .expect("all internal field kinds must materialize");

    for (source, output) in source_columns
        .iter()
        .zip(outputs[0].batch.batch().columns())
    {
        assert!(StdArc::ptr_eq(source, output));
    }
}

#[test]
fn wasm_zero_row_output_builds_exact_empty_destination_columns() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (_, ack_map) = wasm_input_for_values(&schema, &[10]);
    let outputs = validate_wasm_test_outputs(
        &schema,
        &schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            Vec::new(),
        )],
    )
    .expect("zero-row output must build");

    assert_eq!(outputs[0].batch.batch().num_rows(), 0);
    assert_eq!(outputs[0].batch.batch().num_columns(), 1);
    assert_eq!(outputs[0].batch.batch().schema(), schema.arrow_schema());
}

#[test]
fn wasm_uninitialized_column_uses_destination_type_and_ack_row_count() {
    let input_schema = test_schema(&[("input", ParseAsType::I32)]);
    let output_schema = test_optional_schema(&[("value", ParseAsType::I64, true)]);
    let rows = vec![
        WasmOutputRow {
            tokens: Vec::new(),
            source_token: None,
        },
        WasmOutputRow {
            tokens: Vec::new(),
            source_token: None,
        },
    ];

    let outputs = validate_wasm_test_outputs(
        &input_schema,
        &output_schema,
        &super::WasmAckMap::default(),
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::uninitialized()],
            rows,
        )],
    )
    .expect("uninitialized output must pass host validation");

    assert_eq!(outputs[0].batch.batch().num_rows(), 2);
    assert_eq!(
        outputs[0].batch.batch().column(0).data_type(),
        &arrow_schema::DataType::Int64
    );
    assert_eq!(outputs[0].batch.batch().column(0).null_count(), 2);
    assert!(outputs[0].uninitialized_columns.contains(&0));
}

#[test]
fn wasm_mixed_input_and_generated_columns_match_destination_schema() {
    let input_schema = test_schema(&[("value", ParseAsType::I32)]);
    let output_schema =
        test_schema(&[("value", ParseAsType::I32), ("bucket", ParseAsType::String)]);
    let (input, ack_map) = wasm_input_for_values(&input_schema, &[2, 4]);
    let field = output_schema.arrow_schema().field(1).clone();
    let ipc = wasm_guest_column(field, StdArc::new(StringArray::from(vec!["EVEN", "EVEN"])));
    let outputs = validate_wasm_test_outputs(
        &input_schema,
        &output_schema,
        &ack_map,
        vec![wasm_test_generated_output(
            ipc,
            vec![
                WasmOutputColumnRef::input(0),
                WasmOutputColumnRef::generated(0),
            ],
            wasm_input_acks(&input).rows.clone(),
        )],
    )
    .expect("mixed output must materialize");

    assert_eq!(
        outputs[0].batch.batch().schema(),
        output_schema.arrow_schema()
    );
    assert_eq!(outputs[0].batch.batch().num_rows(), 2);
}

#[test]
fn wasm_shared_generated_column_reuses_one_array_across_routes_and_fields() {
    let input_schema = test_schema(&[("value", ParseAsType::I32)]);
    let enriched_schema = test_schema(&[
        ("value", ParseAsType::I32),
        ("bucket", ParseAsType::String),
        ("bucket_copy", ParseAsType::String),
    ]);
    let audit_schema = test_schema(&[
        ("value", ParseAsType::I32),
        ("classification", ParseAsType::String),
    ]);
    let (input, ack_map) = wasm_input_for_values(&input_schema, &[2, 4]);
    let rows = wasm_input_acks(&input).rows.clone();
    let generated_arrow_ipc_batch = wasm_guest_column(
        enriched_schema.arrow_schema().field(1).clone(),
        StdArc::new(StringArray::from(vec!["EVEN", "EVEN"])),
    );
    let output = WasmEnvelope::output(
        generated_arrow_ipc_batch,
        vec![
            WasmRoutedOutput::new(
                "enriched",
                vec![
                    WasmOutputColumnRef::input(0),
                    WasmOutputColumnRef::generated(0),
                    WasmOutputColumnRef::generated(0),
                ],
                WasmAckSidecar {
                    rows: rows.clone(),
                    ..WasmAckSidecar::default()
                },
            ),
            WasmRoutedOutput::new(
                "audit",
                vec![
                    WasmOutputColumnRef::input(0),
                    WasmOutputColumnRef::generated(0),
                ],
                WasmAckSidecar {
                    rows,
                    ..WasmAckSidecar::default()
                },
            ),
        ],
    );
    let outputs = validate_wasm_test_output_groups(
        &input_schema,
        vec![("enriched", enriched_schema), ("audit", audit_schema)],
        &ack_map,
        vec![output],
    )
    .expect("shared generated output must materialize");

    let first = outputs[0].batch.batch().column(1);
    assert!(StdArc::ptr_eq(first, outputs[0].batch.batch().column(2)));
    assert!(StdArc::ptr_eq(first, outputs[1].batch.batch().column(1)));
    let input_values = outputs[0]
        .batch
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("input reference must remain I32");
    assert_eq!(input_values.values().as_ref(), &[2, 4]);
}

#[test]
fn wasm_generated_pool_rejects_out_of_range_and_unreferenced_columns() {
    let input_schema = test_schema(&[("input", ParseAsType::I32)]);
    let output_schema = test_schema(&[("generated", ParseAsType::String)]);
    let field = output_schema.arrow_schema().field(0).clone();
    let one_column =
        wasm_guest_column(field.clone(), StdArc::new(StringArray::from(vec!["value"])));
    let rows = vec![WasmOutputRow {
        tokens: Vec::new(),
        source_token: None,
    }];
    let out_of_range = validate_wasm_test_outputs(
        &input_schema,
        &output_schema,
        &super::WasmAckMap::default(),
        vec![wasm_test_generated_output(
            one_column,
            vec![WasmOutputColumnRef::generated(1)],
            rows.clone(),
        )],
    )
    .expect_err("out-of-range generated column must fail");
    assert!(matches!(
        out_of_range,
        super::WasmOutputError::GeneratedColumnOutOfRange {
            column_index: 1,
            ..
        }
    ));

    let two_columns = wasm_generated_pool(
        vec![field.clone(), field],
        vec![
            StdArc::new(StringArray::from(vec!["used"])),
            StdArc::new(StringArray::from(vec!["unused"])),
        ],
    );
    let unreferenced = validate_wasm_test_outputs(
        &input_schema,
        &output_schema,
        &super::WasmAckMap::default(),
        vec![wasm_test_generated_output(
            two_columns,
            vec![WasmOutputColumnRef::generated(0)],
            rows,
        )],
    )
    .expect_err("unreferenced generated column must fail");
    assert!(matches!(
        unreferenced,
        super::WasmOutputError::UnreferencedGeneratedColumn { column_index: 1 }
    ));
}

#[test]
fn wasm_generated_pool_rejects_route_shape_type_and_row_mismatches() {
    let input_schema = test_schema(&[("input", ParseAsType::I32)]);
    let output_schema = test_schema(&[("generated", ParseAsType::String)]);
    let field = output_schema.arrow_schema().field(0).clone();
    let generated = wasm_guest_column(
        field.clone(),
        StdArc::new(StringArray::from(vec!["first", "second"])),
    );
    let one_row = vec![WasmOutputRow {
        tokens: Vec::new(),
        source_token: None,
    }];
    let row_count = validate_wasm_test_outputs(
        &input_schema,
        &output_schema,
        &super::WasmAckMap::default(),
        vec![wasm_test_generated_output(
            generated,
            vec![WasmOutputColumnRef::generated(0)],
            one_row.clone(),
        )],
    )
    .expect_err("generated row count must match every referencing route");
    assert!(matches!(
        row_count,
        super::WasmOutputError::GeneratedColumnRowCountMismatch {
            expected: 1,
            actual: 2,
            ..
        }
    ));

    let nullable_output_schema = test_optional_schema(&[("generated", ParseAsType::String, true)]);
    let nullability = validate_wasm_test_outputs(
        &input_schema,
        &nullable_output_schema,
        &super::WasmAckMap::default(),
        vec![wasm_test_generated_output(
            wasm_guest_column(field, StdArc::new(StringArray::from(vec!["value"]))),
            vec![WasmOutputColumnRef::generated(0)],
            one_row.clone(),
        )],
    )
    .expect_err("generated nullability must match the destination");
    assert!(matches!(
        nullability,
        super::WasmOutputError::GeneratedColumnTypeMismatch { .. }
    ));

    let column_count = validate_wasm_test_outputs(
        &input_schema,
        &output_schema,
        &super::WasmAckMap::default(),
        vec![wasm_test_output(Vec::new(), one_row)],
    )
    .expect_err("routed output column count must match the destination");
    assert!(matches!(
        column_count,
        super::WasmOutputError::RoutedOutputColumnCountMismatch {
            expected: 1,
            actual: 0,
            ..
        }
    ));

    let empty_group = validate_wasm_test_outputs(
        &input_schema,
        &output_schema,
        &super::WasmAckMap::default(),
        vec![WasmEnvelope::output(Vec::new(), Vec::new())],
    )
    .expect_err("empty output group must fail");
    assert!(matches!(
        empty_group,
        super::WasmOutputError::EmptyOutputGroup { .. }
    ));
}

#[test]
fn wasm_shared_generated_column_requires_each_route_to_use_the_pool_row_layout() {
    let input_schema = test_schema(&[("input", ParseAsType::I32)]);
    let first_schema = test_schema(&[("first", ParseAsType::String)]);
    let second_schema = test_schema(&[("second", ParseAsType::String)]);
    let generated = wasm_guest_column(
        first_schema.arrow_schema().field(0).clone(),
        StdArc::new(StringArray::from(vec!["one", "two"])),
    );
    let row = WasmOutputRow {
        tokens: Vec::new(),
        source_token: None,
    };
    let output = WasmEnvelope::output(
        generated,
        vec![
            WasmRoutedOutput::new(
                "first",
                vec![WasmOutputColumnRef::generated(0)],
                WasmAckSidecar {
                    rows: vec![row.clone(), row.clone()],
                    ..WasmAckSidecar::default()
                },
            ),
            WasmRoutedOutput::new(
                "second",
                vec![WasmOutputColumnRef::generated(0)],
                WasmAckSidecar {
                    rows: vec![row],
                    ..WasmAckSidecar::default()
                },
            ),
        ],
    );
    let error = validate_wasm_test_output_groups(
        &input_schema,
        vec![("first", first_schema), ("second", second_schema)],
        &super::WasmAckMap::default(),
        vec![output],
    )
    .expect_err("one pool cannot serve routes with different row counts");

    assert!(matches!(
        error,
        super::WasmOutputError::GeneratedColumnRowCountMismatch {
            output_relay,
            expected: 1,
            actual: 2,
            ..
        } if output_relay == "second"
    ));
}

#[tokio::test]
async fn wasm_routed_output_fanout_waits_for_every_downstream_ack() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (input, mut ack_map) = wasm_input_for_values(&schema, &[2]);
    let (root_acks, completion) = AckSet::root();
    ack_map.get_mut(&1).expect("token must exist").acks = root_acks;
    let row = wasm_input_acks(&input).rows[0].clone();
    let output = WasmEnvelope::output(
        Vec::new(),
        vec![
            WasmRoutedOutput::new(
                "first",
                vec![WasmOutputColumnRef::input(0)],
                WasmAckSidecar {
                    rows: vec![row.clone()],
                    ..WasmAckSidecar::default()
                },
            ),
            WasmRoutedOutput::new(
                "second",
                vec![WasmOutputColumnRef::input(0)],
                WasmAckSidecar {
                    rows: vec![row],
                    ..WasmAckSidecar::default()
                },
            ),
        ],
    );
    let mut outputs = validate_wasm_test_output_groups(
        &schema,
        vec![
            ("first", Arc::clone(&schema)),
            ("second", Arc::clone(&schema)),
        ],
        &ack_map,
        vec![output],
    )
    .expect("fanout output must validate");
    let mut token_use_counts = super::wasm_output_token_use_counts(&outputs);

    let first = outputs.remove(0);
    let first = super::relay_batch_from_wasm_output(
        &None,
        first.schema,
        first.batch,
        first.acks.rows,
        HashSet::default(),
        &mut ack_map,
        &mut token_use_counts,
    )
    .expect("first routed batch must build");
    let completion_task = tokio::spawn(completion.wait());
    first.batch.acks[0].ack_success();
    tokio::task::yield_now().await;
    assert!(
        !completion_task.is_finished(),
        "the first downstream ACK must not complete the fanned-out input"
    );

    let second = outputs.remove(0);
    let second = super::relay_batch_from_wasm_output(
        &None,
        second.schema,
        second.batch,
        second.acks.rows,
        HashSet::default(),
        &mut ack_map,
        &mut token_use_counts,
    )
    .expect("second routed batch must build");
    second.batch.acks[0].ack_success();
    let outcome = timeout(Duration::from_millis(50), completion_task)
        .await
        .expect("the final downstream ACK must complete the input")
        .expect("ACK completion task must not panic");
    assert_eq!(outcome, AckOutcome::Ack);
}

#[test]
fn wasm_guest_generated_rows_do_not_require_source_tokens() {
    let input_schema = test_schema(&[("input_value", ParseAsType::I32)]);
    let output_schema = test_schema(&[("value", ParseAsType::I32)]);
    let ipc = wasm_guest_column(
        output_schema.arrow_schema().field(0).clone(),
        StdArc::new(Int32Array::from(vec![42])),
    );
    let outputs = validate_wasm_test_outputs(
        &input_schema,
        &output_schema,
        &super::WasmAckMap::default(),
        vec![wasm_test_generated_output(
            ipc,
            vec![WasmOutputColumnRef::generated(0)],
            vec![WasmOutputRow {
                tokens: Vec::new(),
                source_token: None,
            }],
        )],
    )
    .expect("fully generated output rows may omit a source token");

    assert_eq!(outputs[0].batch.batch().num_rows(), 1);
}

#[test]
fn wasm_generated_arrow_contract_rejects_invalid_stream_shapes_and_schema() {
    let input_schema = test_schema(&[("value", ParseAsType::I32)]);
    let output_schema = test_schema(&[("value", ParseAsType::I32)]);
    let (input, ack_map) = wasm_input_for_values(&input_schema, &[2]);
    let rows = wasm_input_acks(&input).rows.clone();
    let destination_field = output_schema.arrow_schema().field(0).clone();
    let validate_ipc = |ipc| {
        validate_wasm_test_outputs(
            &input_schema,
            &output_schema,
            &ack_map,
            vec![wasm_test_generated_output(
                ipc,
                vec![WasmOutputColumnRef::generated(0)],
                rows.clone(),
            )],
        )
    };

    let empty = validate_ipc(Vec::new()).expect_err("generated reference without a pool must fail");
    assert!(matches!(
        empty,
        super::WasmOutputError::GeneratedColumnOutOfRange { .. }
    ));

    let empty_schema = StdArc::new(ArrowSchema::empty());
    let zero_fields = wasm_guest_stream(
        empty_schema.clone(),
        &[RecordBatch::new_empty(empty_schema)],
    );
    let zero_fields = validate_ipc(zero_fields).expect_err("zero fields must fail");
    assert!(matches!(
        zero_fields,
        super::WasmOutputError::InvalidGeneratedArrowIpc { .. }
    ));

    let named_field_schema = StdArc::new(ArrowSchema::new(vec![destination_field.clone()]));
    let named_field_batch = RecordBatch::try_new(
        named_field_schema.clone(),
        vec![StdArc::new(Int32Array::from(vec![2]))],
    )
    .expect("named field batch must build");
    let named_field = validate_ipc(wasm_guest_stream(named_field_schema, &[named_field_batch]))
        .expect_err("generated field names must be empty");
    assert!(matches!(
        named_field,
        super::WasmOutputError::InvalidGeneratedArrowIpc { .. }
    ));

    let one_field_schema = StdArc::new(ArrowSchema::new(vec![
        destination_field.clone().with_name(""),
    ]));
    let no_batches = validate_ipc(wasm_guest_stream(one_field_schema.clone(), &[]))
        .expect_err("missing guest record batch must fail");
    assert!(matches!(
        no_batches,
        super::WasmOutputError::GeneratedRecordBatchCount { actual: 0 }
    ));
    let one_batch = RecordBatch::try_new(
        one_field_schema.clone(),
        vec![StdArc::new(Int32Array::from(vec![2]))],
    )
    .expect("one-field batch must build");
    let multiple_batches = validate_ipc(wasm_guest_stream(
        one_field_schema,
        &[one_batch.clone(), one_batch],
    ))
    .expect_err("multiple guest batches must fail");
    assert!(matches!(
        multiple_batches,
        super::WasmOutputError::GeneratedRecordBatchCount { actual: 2 }
    ));

    let mismatched_field = validate_ipc(wasm_guest_column(
        arrow_schema::Field::new("ignored", arrow_schema::DataType::Utf8, false),
        StdArc::new(StringArray::from(vec!["wrong type"])),
    ))
    .expect_err("guest field mismatch must fail");
    assert!(matches!(
        mismatched_field,
        super::WasmOutputError::GeneratedColumnTypeMismatch { .. }
    ));

    let row_count = validate_ipc(wasm_guest_column(
        destination_field.clone(),
        StdArc::new(Int32Array::from(vec![2, 4])),
    ))
    .expect_err("guest row-count mismatch must fail");
    assert!(matches!(
        row_count,
        super::WasmOutputError::GeneratedColumnRowCountMismatch { .. }
    ));

    let mut trailing_ipc =
        wasm_guest_column(destination_field, StdArc::new(Int32Array::from(vec![2])));
    trailing_ipc.push(0);
    let trailing = validate_ipc(trailing_ipc).expect_err("trailing guest IPC must fail");
    assert!(matches!(
        trailing,
        super::WasmOutputError::InvalidGeneratedArrowIpc { .. }
    ));
}

#[test]
fn wasm_input_reference_validation_rejects_invalid_mapping_and_source_tokens() {
    let input_schema = test_schema(&[("value", ParseAsType::I32)]);
    let renamed_schema = test_schema(&[("renamed_value", ParseAsType::I32)]);
    let string_schema = test_schema(&[("value", ParseAsType::String)]);
    let nullable_schema = test_optional_schema(&[("value", ParseAsType::I32, true)]);
    let (input, ack_map) = wasm_input_for_values(&input_schema, &[10]);
    let rows = wasm_input_acks(&input).rows.clone();

    validate_wasm_test_outputs(
        &input_schema,
        &renamed_schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            rows.clone(),
        )],
    )
    .expect("explicit input references may rename fields");

    let out_of_range = validate_wasm_test_outputs(
        &input_schema,
        &input_schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 9 }],
            rows.clone(),
        )],
    )
    .expect_err("out-of-range input column must fail");
    assert!(matches!(
        out_of_range,
        super::WasmOutputError::InputColumnOutOfRange { .. }
    ));

    let type_mismatch = validate_wasm_test_outputs(
        &input_schema,
        &string_schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            rows.clone(),
        )],
    )
    .expect_err("input type mismatch must fail");
    assert!(matches!(
        type_mismatch,
        super::WasmOutputError::InputColumnTypeMismatch { .. }
    ));

    let nullability_mismatch = validate_wasm_test_outputs(
        &input_schema,
        &nullable_schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            rows.clone(),
        )],
    )
    .expect_err("input nullability mismatch must fail");
    assert!(matches!(
        nullability_mismatch,
        super::WasmOutputError::InputColumnTypeMismatch { .. }
    ));

    let mut missing_source = rows.clone();
    missing_source[0].source_token = None;
    let missing = validate_wasm_test_outputs(
        &input_schema,
        &input_schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            missing_source,
        )],
    )
    .expect_err("missing source token must fail");
    assert!(matches!(
        missing,
        super::WasmOutputError::MissingSourceToken { .. }
    ));

    let mut unknown_source = rows.clone();
    unknown_source[0].tokens = vec![WasmAckToken(99)];
    unknown_source[0].source_token = Some(WasmAckToken(99));
    let unknown = validate_wasm_test_outputs(
        &input_schema,
        &input_schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            unknown_source,
        )],
    )
    .expect_err("unknown source token must fail");
    assert!(matches!(
        unknown,
        super::WasmOutputError::UnknownSourceToken { .. }
    ));

    let mut not_carried = rows;
    not_carried[0].tokens.clear();
    let not_carried = validate_wasm_test_outputs(
        &input_schema,
        &input_schema,
        &ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            not_carried,
        )],
    )
    .expect_err("source token outside lineage must fail");
    assert!(matches!(
        not_carried,
        super::WasmOutputError::SourceTokenNotCarried { .. }
    ));
}

#[test]
fn wasm_callback_rejects_tokens_that_are_both_carried_and_terminal() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (input, ack_map) = wasm_input_for_values(&schema, &[10]);
    let output = WasmEnvelope::output(
        Vec::new(),
        vec![WasmRoutedOutput::new(
            "output",
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            WasmAckSidecar {
                rows: wasm_input_acks(&input).rows.clone(),
                acked: vec![WasmAckTokenSet {
                    tokens: vec![WasmAckToken(1)],
                }],
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        )],
    );

    let error = validate_wasm_test_outputs(&schema, &schema, &ack_map, vec![output])
        .expect_err("one token cannot be carried and terminally completed");
    assert!(matches!(
        error,
        super::WasmOutputError::InvalidTokenDecision { token: 1, .. }
    ));

    let duplicate_terminal = WasmEnvelope::output(
        Vec::new(),
        vec![
            WasmRoutedOutput::new(
                "output",
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                WasmAckSidecar {
                    rows: Vec::new(),
                    acked: vec![WasmAckTokenSet {
                        tokens: vec![WasmAckToken(1)],
                    }],
                    nacked: Vec::new(),
                    message_errors: Vec::new(),
                },
            ),
            WasmRoutedOutput::new(
                "output",
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                WasmAckSidecar {
                    rows: Vec::new(),
                    acked: Vec::new(),
                    nacked: vec![nervix_wasm::WasmNackSet {
                        tokens: vec![WasmAckToken(1)],
                        reason: "rejected".to_string(),
                    }],
                    message_errors: Vec::new(),
                },
            ),
        ],
    );
    let error = validate_wasm_test_outputs(&schema, &schema, &ack_map, vec![duplicate_terminal])
        .expect_err("one token cannot receive multiple terminal decisions");
    assert!(matches!(
        error,
        super::WasmOutputError::InvalidTokenDecision { token: 1, .. }
    ));
}

#[test]
fn wasm_reference_to_terminally_removed_or_other_branch_token_is_rejected() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (input, _) = wasm_input_for_values(&schema, &[10]);
    let empty_ack_map = super::WasmAckMap::default();

    let error = validate_wasm_test_outputs(
        &schema,
        &schema,
        &empty_ack_map,
        vec![wasm_test_output(
            vec![WasmOutputColumnRef::Input { column_index: 0 }],
            wasm_input_acks(&input).rows.clone(),
        )],
    )
    .expect_err("a token outside the current live branch map must fail");
    assert!(matches!(
        error,
        super::WasmOutputError::UnknownSourceToken { token: 1, .. }
    ));
}

#[tokio::test]
async fn wasm_callback_validation_is_all_or_nothing_for_terminal_decisions() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let (input, mut ack_map) = wasm_input_for_values(&schema, &[10]);
    let (acks, completion) = AckSet::root();
    ack_map.get_mut(&1).expect("token must exist").acks = acks;
    let output_group = WasmEnvelope::output(
        Vec::new(),
        vec![
            WasmRoutedOutput::new(
                "output",
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                WasmAckSidecar {
                    rows: Vec::new(),
                    acked: vec![WasmAckTokenSet {
                        tokens: vec![WasmAckToken(1)],
                    }],
                    nacked: Vec::new(),
                    message_errors: Vec::new(),
                },
            ),
            WasmRoutedOutput::new(
                "output",
                Vec::new(),
                WasmAckSidecar {
                    rows: wasm_input_acks(&input).rows.clone(),
                    ..WasmAckSidecar::default()
                },
            ),
        ],
    );

    validate_wasm_test_outputs(&schema, &schema, &ack_map, vec![output_group])
        .expect_err("later malformed output must reject the whole callback");
    assert!(
        timeout(Duration::from_millis(50), completion.wait())
            .await
            .is_err(),
        "validation must not apply an earlier terminal ACK"
    );
}

fn scheduled_model(
    kind: ModelKind,
    identifier: Identifier,
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
        primary_node: Some("node-1".to_string()),
        assigned_nodes: vec!["node-1".to_string()],
    }
}

#[tokio::test]
async fn scheduled_relay_placement_does_not_create_metric_replication_state() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("events");
    let schema = identifier("event");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0s".to_string(),
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: DomainStatus::Running,
            start_version: 1,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
    )]));
    let schedule = ClusterSchedule {
        domains: vec![DomainSchedule {
            domain: domain.clone(),
            nodes: vec![
                ScheduledNode {
                    identifier: schema.clone(),
                    kind: ModelKind::Schema,
                    config: Box::new(nervix_models::Model::Schema(CreateSchema {
                        name: schema.clone(),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::I64,
                            optional: false,
                            sensitive: false,
                        }],
                    })),
                    effective_branching: None,
                    effective_branching_schema: None,
                    schema_fingerprint: [0; 32],
                    kafka_partition_schedule: None,
                    primary_node: None,
                    assigned_nodes: Vec::new(),
                },
                scheduled_model(
                    ModelKind::Relay,
                    relay.clone(),
                    nervix_models::Model::Relay(CreateRelay {
                        name: relay.clone(),
                        schema,
                        buffer: 2,
                        branching: RelayBranching::unbranched(),
                        materialized_state: None,
                    }),
                ),
            ],
            placement_groups: Vec::new(),
        }],
    };

    runtime
        .apply_cluster_schedule("node-1", &schedule)
        .await
        .expect("relay schedule should build");

    assert!(
        !runtime
            .replicated_branch_aggregated_states
            .iter()
            .any(|state| state.key().kind == ModelKind::Relay),
        "relay metrics must remain volatile owner-only state"
    );
}

#[tokio::test]
async fn entity_gate_hold_quiesces_an_ingestor_without_stopping_it() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("events");
    let ingestor = identifier("events_source");
    let operation_id = 41;

    let fanout = super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let gate = fanout.dispatch_gate();
    runtime
        .relay_boundary_fanouts
        .insert((domain.clone(), relay.clone()), fanout);

    let key = super::RuntimeKey::new(domain.clone(), ingestor.clone());
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let stopped = StdArc::new(AtomicBool::new(false));
    let task_stopped = stopped.clone();
    let task = tokio::spawn(async move {
        let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
        task_stopped.store(true, Ordering::SeqCst);
    });
    runtime.ingestors.insert(
        key.clone(),
        super::IngestorRuntime::Background {
            shutdown: shutdown_tx,
            branched: Vec::new(),
            tasks: vec![task],
        },
    );
    runtime.ingestor_quiescence.insert(
        key.clone(),
        test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Suspend),
    );

    let affected = crate::registry::RegistryEntity {
        kind: ModelKind::Ingestor,
        identifier: ingestor.clone(),
    };
    runtime
        .engage_entity_gate_operation(
            operation_id,
            &domain,
            std::slice::from_ref(&relay),
            std::slice::from_ref(&affected),
            EntityGatePurpose::ModelAlteration,
            super::EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(5),
                reason: "quiesce regression",
            },
        )
        .await
        .expect("entity hold should engage");

    assert!(runtime.ingestors.get(&key).is_some());
    assert!(!stopped.load(Ordering::SeqCst));
    assert!(gate.is_closed());
    assert!(runtime.entity_gate_operation_is_held(operation_id, &domain));
    assert_eq!(
        runtime
            .ingestor_quiescence
            .get(&key)
            .and_then(|control| control.cause()),
        Some(super::IngestorQuiesceCause::EntityHold)
    );

    runtime
        .release_entity_gate_operation(operation_id, &domain)
        .await
        .expect("entity hold should release");
    assert!(!runtime.entity_gate_operation_is_held(operation_id, &domain));
    assert!(!gate.is_closed());
    assert!(runtime.ingestors.get(&key).is_some());
    assert!(!stopped.load(Ordering::SeqCst));
    assert_eq!(
        runtime
            .ingestor_quiescence
            .get(&key)
            .and_then(|control| control.cause()),
        None
    );

    runtime
        .stop_ingestor(&domain, &ingestor)
        .await
        .expect("test ingestor should stop");
}

#[tokio::test]
async fn entity_gate_operation_releases_when_its_lease_deadline_expires() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("events");
    let operation_id = 42;
    let fanout = super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let gate = fanout.dispatch_gate();
    runtime
        .relay_boundary_fanouts
        .insert((domain.clone(), relay.clone()), fanout);

    runtime
        .engage_entity_gate_operation(
            operation_id,
            &domain,
            std::slice::from_ref(&relay),
            &[],
            EntityGatePurpose::OwnershipHandoff,
            super::EntityGateLease {
                deadline: Instant::now() + Duration::from_millis(25),
                reason: "deadline regression",
            },
        )
        .await
        .expect("entity hold should engage");
    assert!(gate.is_closed());

    tokio::time::timeout(Duration::from_secs(1), async {
        while runtime.entity_gate_operation_is_held(operation_id, &domain) {
            tokio::task::consume_budget().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("entity hold should release at its deadline");
    assert!(!gate.is_closed());
}

#[tokio::test]
async fn paused_schedule_keeps_full_execution_without_rebuilding_unchanged_graph() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let schema = identifier("notification");
    let relay = identifier("notifications");
    let running = DomainState {
        id: domain.clone(),
        config: DomainConfig {
            pace: DomainPace::Unpaced,
            period: "1s".to_string(),
            skew: "0s".to_string(),
            placement: nervix_models::PlacementPolicy::Neutral,
        },
        status: DomainStatus::Running,
        start_version: 1,
        last_start: nervix_models::DomainStartPoint::Resume,
        clock: None,
    };
    runtime.sync_domains(&BTreeMap::from([(domain.clone(), running.clone())]));
    let schedule = ClusterSchedule {
        domains: vec![DomainSchedule {
            domain: domain.clone(),
            nodes: vec![
                scheduled_model(
                    ModelKind::Schema,
                    schema.clone(),
                    nervix_models::Model::Schema(CreateSchema {
                        name: schema.clone(),
                        fields: vec![SchemaField {
                            name: identifier("user_id"),
                            ty: ParseAsType::I64,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                ),
                scheduled_model(
                    ModelKind::Relay,
                    relay.clone(),
                    nervix_models::Model::Relay(CreateRelay {
                        name: relay.clone(),
                        schema,
                        buffer: 2,
                        branching: RelayBranching::unbranched(),
                        materialized_state: None,
                    }),
                ),
            ],
            placement_groups: Vec::new(),
        }],
    };
    runtime
        .apply_cluster_schedule("node-1", &schedule)
        .await
        .expect("running schedule should build");
    let graph_before_pause = runtime
        .executions
        .get(&domain)
        .expect("execution should exist")
        .graph
        .clone();

    let mut paused = running;
    paused.status = DomainStatus::Paused;
    runtime.sync_domains(&BTreeMap::from([(domain.clone(), paused)]));
    runtime
        .apply_cluster_schedule("node-1", &schedule)
        .await
        .expect("paused schedule should remain active");

    let execution = runtime
        .executions
        .get(&domain)
        .expect("paused execution should remain");
    assert!(!execution.passive_only);
    assert!(StdArc::ptr_eq(&graph_before_pause, &execution.graph));
    assert!(execution.relay_registries.contains_key(&relay));
}

#[tokio::test]
async fn stale_cluster_state_cannot_replace_a_newer_runtime_schedule() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let schema = identifier("notification");
    let relay = identifier("notifications");
    let domains = BTreeMap::from([(
        domain.clone(),
        DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0s".to_string(),
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: DomainStatus::Running,
            start_version: 1,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
    )]);
    let schema_node = scheduled_model(
        ModelKind::Schema,
        schema.clone(),
        nervix_models::Model::Schema(CreateSchema {
            name: schema.clone(),
            fields: vec![SchemaField {
                name: identifier("user_id"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }),
    );
    let stale_schedule = ClusterSchedule {
        domains: vec![DomainSchedule {
            domain: domain.clone(),
            nodes: vec![schema_node.clone()],
            placement_groups: Vec::new(),
        }],
    };
    let current_schedule = ClusterSchedule {
        domains: vec![DomainSchedule {
            domain: domain.clone(),
            nodes: vec![
                schema_node,
                scheduled_model(
                    ModelKind::Relay,
                    relay.clone(),
                    nervix_models::Model::Relay(CreateRelay {
                        name: relay.clone(),
                        schema,
                        buffer: 2,
                        branching: RelayBranching::unbranched(),
                        materialized_state: None,
                    }),
                ),
            ],
            placement_groups: Vec::new(),
        }],
    };

    runtime
        .apply_cluster_state("node-1", 2, &domains, &current_schedule)
        .await
        .expect("current cluster state should build");
    runtime
        .apply_cluster_state("node-1", 1, &domains, &stale_schedule)
        .await
        .expect("stale cluster state should be ignored");

    let execution = runtime
        .executions
        .get(&domain)
        .expect("current execution should remain");
    assert_eq!(execution.schedule, current_schedule.domains[0]);
    assert!(execution.relay_registries.contains_key(&relay));
}

#[tokio::test]
async fn scheduled_mqtt_client_id_conflicts_are_visible_on_describe() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0s".to_string(),
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: DomainStatus::Running,
            start_version: 0,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
    )]));

    let schema = identifier("notification");
    let wire_schema = identifier("notification_wire");
    let codec = identifier("notification_json");
    let relay = identifier("notifications");
    let client = identifier("mqtt_main");
    let ingestor = identifier("mqtt_notifications");
    let result = runtime
        .apply_cluster_schedule(
            "node-1",
            &ClusterSchedule {
                domains: vec![DomainSchedule {
                    domain: domain.clone(),
                    nodes: vec![
                        scheduled_model(
                            ModelKind::Schema,
                            schema.clone(),
                            nervix_models::Model::Schema(CreateSchema {
                                name: schema.clone(),
                                fields: vec![SchemaField {
                                    name: identifier("user_id"),
                                    ty: ParseAsType::I64,
                                    optional: false,
                                    sensitive: false,
                                }],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::WireJsonSchema,
                            wire_schema.clone(),
                            nervix_models::Model::WireJsonSchema(CreateJsonWireSchema {
                                name: wire_schema.clone(),
                                strictness: Default::default(),
                                fields: vec![WireSchemaField {
                                    name: identifier("user_id"),
                                    ty: JsonType::Integer,
                                    optional: false,
                                }],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Codec,
                            codec.clone(),
                            nervix_models::Model::Codec(CreateCodec {
                                name: codec.clone(),
                                wire_format: CodecWireFormat::Json,
                                wire_schema: Some(wire_schema.clone()),
                                schema: schema.clone(),
                                encoding_rules: Vec::new(),
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Relay,
                            relay.clone(),
                            nervix_models::Model::Relay(CreateRelay {
                                name: relay.clone(),
                                schema: schema.clone(),
                                buffer: 2,
                                branching: RelayBranching::unbranched(),
                                materialized_state: None,
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Client,
                            client.clone(),
                            nervix_models::Model::ClientMqtt(CreateClientMqtt {
                                name: client.clone(),
                                mount: None,
                                config: vec![
                                    ClientConfigEntry {
                                        key: "addr".to_string(),
                                        value: "mqtt://127.0.0.1:1883".to_string(),
                                    },
                                    ClientConfigEntry {
                                        key: "client_id".to_string(),
                                        value: "fixed-client".to_string(),
                                    },
                                ],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Ingestor,
                            ingestor.clone(),
                            nervix_models::Model::Ingestor(CreateIngestor {
                                name: ingestor.clone(),
                                output_routes: with_inherit_all(ProcessorOutputs::single(
                                    relay.clone(),
                                ))
                                .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                                .with_branch(OutputBranch::Unbranched),
                                decode_using_codec: codec.clone(),
                                timestamp_source: None,
                                source: IngestSource::Mqtt {
                                    client,
                                    topic: "notifications".to_string(),
                                    instances: 2,
                                    mode: MqttIngestMode::NoAckSequential {
                                        session: MqttSession::Clean,
                                        qos: MqttQos::AtMostOnce,
                                    },
                                    quiesce: nervix_models::IngestQuiesceMode::Drop,
                                },
                                general_error_policy: GeneralErrorPolicy::Log,
                                filter_where: None,
                            }),
                        ),
                    ],
                    placement_groups: Vec::new(),
                }],
            },
        )
        .await;

    result.expect("fixed mqtt client_id conflict should be reported by ingestor state");

    let describe = runtime
        .describe_local_ingestor(&domain, &ingestor)
        .expect("describe should succeed for scheduled ingestor");
    assert!(describe.running);
    assert!(
        describe.transient_error.as_deref().is_some_and(
            |error| error.contains("MQTT client_id 'fixed-client' is shared by 2 instances")
        ),
        "describe should expose mqtt client_id conflict, got {:?}",
        describe.transient_error
    );
}

#[tokio::test]
async fn scheduled_ingestor_start_failure_removes_partial_domain_execution() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0s".to_string(),
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: DomainStatus::Running,
            start_version: 0,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
    )]));

    let schema = identifier("notification");
    let wire_schema = identifier("notification_wire");
    let codec = identifier("notification_json");
    let relay = identifier("notifications");
    let client = identifier("mqtt_main");
    let ingestor = identifier("mqtt_notifications");
    let result = runtime
        .apply_cluster_schedule(
            "node-1",
            &ClusterSchedule {
                domains: vec![DomainSchedule {
                    domain: domain.clone(),
                    nodes: vec![
                        scheduled_model(
                            ModelKind::Schema,
                            schema.clone(),
                            nervix_models::Model::Schema(CreateSchema {
                                name: schema.clone(),
                                fields: vec![SchemaField {
                                    name: identifier("user_id"),
                                    ty: ParseAsType::I64,
                                    optional: false,
                                    sensitive: false,
                                }],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::WireJsonSchema,
                            wire_schema.clone(),
                            nervix_models::Model::WireJsonSchema(CreateJsonWireSchema {
                                name: wire_schema.clone(),
                                strictness: Default::default(),
                                fields: vec![WireSchemaField {
                                    name: identifier("user_id"),
                                    ty: JsonType::Integer,
                                    optional: false,
                                }],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Codec,
                            codec.clone(),
                            nervix_models::Model::Codec(CreateCodec {
                                name: codec.clone(),
                                wire_format: CodecWireFormat::Json,
                                wire_schema: Some(wire_schema.clone()),
                                schema: schema.clone(),
                                encoding_rules: Vec::new(),
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Relay,
                            relay.clone(),
                            nervix_models::Model::Relay(CreateRelay {
                                name: relay.clone(),
                                schema: schema.clone(),
                                buffer: 2,
                                branching: RelayBranching::unbranched(),
                                materialized_state: None,
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Client,
                            client.clone(),
                            nervix_models::Model::ClientMqtt(CreateClientMqtt {
                                name: client.clone(),
                                mount: None,
                                config: vec![ClientConfigEntry {
                                    key: "addr".to_string(),
                                    value: "mqtt://127.0.0.1:1883".to_string(),
                                }],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Ingestor,
                            ingestor.clone(),
                            nervix_models::Model::Ingestor(CreateIngestor {
                                name: ingestor.clone(),
                                output_routes: with_inherit_all(ProcessorOutputs::single(
                                    relay.clone(),
                                ))
                                .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                                .with_branch(OutputBranch::Unbranched),
                                decode_using_codec: codec.clone(),
                                timestamp_source: None,
                                source: IngestSource::Mqtt {
                                    client,
                                    topic: "notifications".to_string(),
                                    instances: 1,
                                    mode: MqttIngestMode::AckSequential {
                                        timeout: "oops".to_string(),
                                        retry_policy: RetryPolicy {
                                            backoff: "100ms".to_string(),
                                            max_backoff: "200ms".to_string(),
                                        },
                                    },
                                    quiesce: nervix_models::IngestQuiesceMode::Drop,
                                },
                                general_error_policy: GeneralErrorPolicy::Log,
                                filter_where: None,
                            }),
                        ),
                    ],
                    placement_groups: Vec::new(),
                }],
            },
        )
        .await;

    let error = result.expect_err("invalid ACK timeout must fail schedule application");
    assert!(
        error.to_string().contains("invalid ack timeout 'oops'"),
        "unexpected start error: {error}"
    );
    assert!(
        !runtime.executions.contains_key(&domain),
        "failed scheduled ingestor start must not leave a partial domain execution"
    );
    assert!(
        !runtime
            .ingestors
            .contains_key(&super::RuntimeKey::new(domain.clone(), ingestor.clone())),
        "failed scheduled ingestor start must not leave an ingestor runtime"
    );
    let describe_error = runtime
        .describe_local_ingestor(&domain, &ingestor)
        .expect_err("describe should expose the domain instantiation error");
    assert!(
        describe_error.contains("invalid ack timeout 'oops'"),
        "describe should expose start error, got {describe_error}"
    );
}

#[tokio::test]
async fn branch_preserving_processors_build_standalone_schedule_nodes() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let order_schema = identifier("order_event");
    let order_relay = |name: &str| {
        scheduled_model(
            ModelKind::Relay,
            identifier(name),
            nervix_models::Model::Relay(CreateRelay {
                name: identifier(name),
                schema: order_schema.clone(),
                buffer: 2,
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            }),
        )
    };
    let schedule = DomainSchedule {
        domain: domain.clone(),
        nodes: vec![
            scheduled_model(
                ModelKind::Schema,
                order_schema.clone(),
                nervix_models::Model::Schema(CreateSchema {
                    name: order_schema.clone(),
                    fields: vec![SchemaField {
                        name: identifier("order_id"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }),
            ),
            order_relay("orders"),
            order_relay("projected_orders"),
            order_relay("left_orders"),
            order_relay("right_orders"),
            order_relay("joined_orders"),
            scheduled_model(
                ModelKind::Deduplicator,
                identifier("dedup_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_orders"),
                    from: ProcessorInputs::single(identifier("orders")),
                    output_routes: (ProcessorOutputs::single(identifier("projected_orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
            scheduled_model(
                ModelKind::Junction,
                identifier("join_orders"),
                nervix_models::Model::Junction(CreateJunction {
                    name: identifier("join_orders"),
                    from: ProcessorInputs::new(
                        vec![identifier("left_orders"), identifier("right_orders")],
                        Vec::new(),
                    ),
                    output_routes: (ProcessorOutputs::single(identifier("joined_orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: BranchSelection::unbranched(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ],
        placement_groups: Vec::new(),
    };

    runtime
        .rebuild_domain_from_schedule("node-1", &domain, Some(schedule), true)
        .await
        .expect("standalone branch-preserving processors must build");
    runtime
        .rebuild_domain_from_schedule("node-1", &domain, None, true)
        .await
        .expect("domain teardown must stop processor runtimes");
}

#[test]
fn emitter_entity_pause_gates_every_input_relay() {
    let emitter = CreateEmitter {
        name: identifier("combined_sink"),
        from: ProcessorInputs::new(
            vec![identifier("source_b"), identifier("source_a")],
            Vec::new(),
        ),
        encode_using_codec: Some(identifier("event_codec")),
        sink: Box::new(EmitSink::ZeroMq {
            client: identifier("sink"),
        }),
        flush_each: "IMMEDIATE".to_string(),
        max_batch_size: None,
        error_policies: ErrorPolicies::handled_by_log(),
        publishing_mode: EmitterPublishingMode::NoAck {
            retry_policy: RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        mode: AckMode::Attached,
        construction: nervix_models::RouteConstruction::default(),
        materialized_state: Vec::new(),
    };
    let input_relay = |name: &str| {
        scheduled_model(
            ModelKind::Relay,
            identifier(name),
            nervix_models::Model::Relay(CreateRelay {
                name: identifier(name),
                schema: identifier("event"),
                buffer: 2,
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            }),
        )
    };
    let mut schedule = DomainSchedule {
        domain: domain("testing"),
        nodes: vec![
            input_relay("source_a"),
            input_relay("source_b"),
            scheduled_model(
                ModelKind::Emitter,
                emitter.name.clone(),
                nervix_models::Model::Emitter(emitter.clone()),
            ),
        ],
        placement_groups: Vec::new(),
    };
    let emitter_node = schedule
        .nodes
        .iter_mut()
        .find(|node| node.kind == ModelKind::Emitter)
        .expect("test schedule must contain its emitter");
    emitter_node.primary_node = Some("node-2".to_string());
    emitter_node.assigned_nodes = vec!["node-2".to_string()];
    let entity = crate::registry::RegistryEntity {
        kind: ModelKind::Emitter,
        identifier: emitter.name,
    };

    assert_eq!(
        super::Runtime::entity_pause_relays_for_schedule(&schedule, &[entity]),
        vec![identifier("source_a"), identifier("source_b")]
    );
    let remote_consumers =
        super::Runtime::remote_runtime_consumers_for_schedule(&schedule, "node-1");
    assert_eq!(remote_consumers.len(), 2);
    for relay in [identifier("source_a"), identifier("source_b")] {
        let consumers = remote_consumers
            .get(&relay)
            .expect("every emitter input needs a remote consumer");
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].relay, relay);
        assert_eq!(consumers[0].node_id, "node-2");
    }
}

#[test]
fn ownership_handoff_keeps_internal_moved_group_relays_open() {
    let junction = |name: &str, input: &str, output: &str| {
        scheduled_model(
            ModelKind::Junction,
            identifier(name),
            nervix_models::Model::Junction(CreateJunction {
                name: identifier(name),
                from: ProcessorInputs::single(identifier(input)),
                output_routes: (ProcessorOutputs::single(identifier(output)))
                    .with_flush_policy("IMMEDIATE".to_string(), None),
                branched_by: BranchSelection::unbranched(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            }),
        )
    };
    let schedule = DomainSchedule {
        domain: domain("testing"),
        nodes: vec![
            junction("corridor_source", "inbound", "corridor_stage"),
            junction("corridor_sink", "corridor_stage", "outbound"),
        ],
        placement_groups: Vec::new(),
    };
    let affected =
        ["corridor_source", "corridor_sink"].map(|name| crate::registry::RegistryEntity {
            kind: ModelKind::Junction,
            identifier: identifier(name),
        });

    assert_eq!(
        super::Runtime::entity_pause_relays_for_schedule(&schedule, &affected),
        vec![identifier("corridor_stage"), identifier("inbound")]
    );
    assert_eq!(
        super::Runtime::ownership_handoff_relays_for_schedule(&schedule, &affected),
        vec![identifier("inbound")]
    );
}

#[tokio::test]
async fn scheduled_processor_entity_swap_is_not_junction_specific() {
    let runtime = super::Runtime::default();
    *runtime.local_node_id.write() = Some("node-1".to_string());
    let domain = domain("default");
    let event_schema = identifier("event");
    let processor = identifier("deduplicate_events");
    let schedule = DomainSchedule {
        domain: domain.clone(),
        nodes: vec![
            scheduled_model(
                ModelKind::Schema,
                event_schema.clone(),
                nervix_models::Model::Schema(CreateSchema {
                    name: event_schema.clone(),
                    fields: vec![SchemaField {
                        name: identifier("event_id"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }),
            ),
            scheduled_model(
                ModelKind::Relay,
                identifier("events"),
                nervix_models::Model::Relay(CreateRelay {
                    name: identifier("events"),
                    schema: event_schema.clone(),
                    buffer: 2,
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }),
            ),
            scheduled_model(
                ModelKind::Relay,
                identifier("unique_events"),
                nervix_models::Model::Relay(CreateRelay {
                    name: identifier("unique_events"),
                    schema: event_schema,
                    buffer: 2,
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }),
            ),
            scheduled_model(
                ModelKind::Deduplicator,
                processor.clone(),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: processor.clone(),
                    from: ProcessorInputs::single(identifier("events")),
                    output_routes: with_inherit_all(ProcessorOutputs::single(identifier(
                        "unique_events",
                    )))
                    .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![expression("input.event_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ],
        placement_groups: Vec::new(),
    };

    runtime
        .rebuild_domain_from_schedule("node-1", &domain, Some(schedule.clone()), true)
        .await
        .expect("scheduled deduplicator must build");
    let entity = crate::registry::RegistryEntity {
        kind: ModelKind::Deduplicator,
        identifier: processor.clone(),
    };
    assert_eq!(
        runtime.entity_pause_relays(&domain, std::slice::from_ref(&entity)),
        vec![identifier("events")],
        "every scheduled processor swap must gate its input relays"
    );

    let mut desired = schedule;
    let nervix_models::Model::Deduplicator(config) = desired
        .nodes
        .iter_mut()
        .find(|node| node.kind == ModelKind::Deduplicator)
        .expect("schedule must contain the processor")
        .config
        .as_mut()
    else {
        panic!("scheduled processor must contain a deduplicator model");
    };
    config.mode = AckMode::Detached;

    runtime
        .swap_scheduled_nodes(&domain, desired.clone(), &[entity], &[], &[])
        .await
        .expect("non-junction scheduled processors must use the shared swap path");
    let execution = runtime
        .executions
        .get(&domain)
        .expect("domain execution must remain installed");
    assert_eq!(execution.schedule, desired);
    assert!(
        execution
            .node_tasks
            .contains_key(&crate::registry::RegistryEntity {
                kind: ModelKind::Deduplicator,
                identifier: processor,
            })
    );
}

#[tokio::test]
async fn scheduled_entity_swap_reinstalls_state_schema_fingerprints() {
    let runtime = super::Runtime::default();
    *runtime.local_node_id.write() = Some("node-1".to_string());
    let domain = domain("default");
    let event_schema = identifier("event");
    let processor = identifier("deduplicate_events");
    let schedule = DomainSchedule {
        domain: domain.clone(),
        nodes: vec![
            scheduled_model(
                ModelKind::Schema,
                event_schema.clone(),
                nervix_models::Model::Schema(CreateSchema {
                    name: event_schema.clone(),
                    fields: vec![SchemaField {
                        name: identifier("event_id"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }),
            ),
            scheduled_model(
                ModelKind::Relay,
                identifier("events"),
                nervix_models::Model::Relay(CreateRelay {
                    name: identifier("events"),
                    schema: event_schema.clone(),
                    buffer: 2,
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }),
            ),
            scheduled_model(
                ModelKind::Relay,
                identifier("unique_events"),
                nervix_models::Model::Relay(CreateRelay {
                    name: identifier("unique_events"),
                    schema: event_schema,
                    buffer: 2,
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }),
            ),
            scheduled_model(
                ModelKind::Deduplicator,
                processor.clone(),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: processor.clone(),
                    from: ProcessorInputs::single(identifier("events")),
                    output_routes: with_inherit_all(ProcessorOutputs::single(identifier(
                        "unique_events",
                    )))
                    .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![expression("input.event_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ],
        placement_groups: Vec::new(),
    };

    runtime
        .rebuild_domain_from_schedule("node-1", &domain, Some(schedule.clone()), true)
        .await
        .expect("scheduled deduplicator must build");

    let mut desired = schedule;
    let processor_node = desired
        .nodes
        .iter_mut()
        .find(|node| node.kind == ModelKind::Deduplicator)
        .expect("schedule must contain the processor");
    processor_node.schema_fingerprint = [7; 32];
    let nervix_models::Model::Deduplicator(config) = processor_node.config.as_mut() else {
        panic!("scheduled processor must contain a deduplicator model");
    };
    config.mode = AckMode::Detached;
    let entity = crate::registry::RegistryEntity {
        kind: ModelKind::Deduplicator,
        identifier: processor.clone(),
    };

    runtime
        .swap_scheduled_nodes(&domain, desired, &[entity], &[], &[])
        .await
        .expect("entity swap must apply");

    let installed = runtime
        .state_schema_fingerprints
        .get(&super::RuntimeStateSchemaKey::new(
            domain,
            ModelKind::Deduplicator,
            processor,
        ))
        .map(|entry| *entry.value());
    assert_eq!(
        installed,
        Some([7; 32]),
        "an entity swap must reinstall the schedule's state schema fingerprints so persisted \
         runtime state is not stranded under the pre-swap fingerprint"
    );
}

#[test]
fn processor_template_refresh_is_not_junction_specific() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let input = identifier("events");
    let output = identifier("unique_events");
    let processor = identifier("deduplicate_events");
    let collect_policy = super::RuntimeInputCollectPolicy {
        interval: Duration::from_secs(1),
        max_batch_size: Some(1024),
    };
    let template = super::RelayProcessorTemplate {
        kind: ModelKind::Deduplicator,
        processor: processor.clone(),
        input_relays: vec![input.clone()],
        input_collect_policies: [(input.clone(), collect_policy)].into_iter().collect(),
        error_policies: ErrorPolicies::handled_by_log(),
        from_where: HashMap::default(),
        filter_where: None,
        materialized_state: Vec::new(),
        operation: super::RelayProcessorOperationTemplate::Deduplicator {
            output_routes: super::RelayProcessorOutputsTemplate {
                routes: vec![super::RelayProcessorOutputTemplate {
                    output_relay: output,
                    construction: nervix_models::RouteConstruction::default(),
                    flush_policy: Some(super::RuntimeFlushPolicy::Immediate),
                    message_error_policy: MessageErrorPolicy::Log,
                }],
            },
            deduplicate_on: vec![expression("input.event_id")],
            max_time: Duration::from_secs(600),
        },
    };
    let mut node = template
        .instantiate(&runtime, &domain, &None)
        .expect("deduplicator template must instantiate");

    let mut desired = template.clone();
    desired.filter_where = Some(expression("input.event_id > 0"));
    desired.input_collect_policies.insert(
        input.clone(),
        super::RuntimeInputCollectPolicy {
            interval: Duration::from_secs(2),
            max_batch_size: None,
        },
    );
    let super::RelayProcessorOperationTemplate::Deduplicator { max_time, .. } =
        &mut desired.operation
    else {
        panic!("test template must remain a deduplicator");
    };
    *max_time = Duration::from_secs(30);

    node.apply_node_template(desired)
        .expect("non-junction dynamic template fields must refresh in place");
    assert_eq!(node.filter_where, Some(expression("input.event_id > 0")));
    assert_eq!(
        node.input_collectors
            .get(&input)
            .expect("collector must remain installed")
            .policy
            .interval,
        Duration::from_secs(2)
    );
    let super::RelayProcessorOperationNode::Deduplicator { max_time, .. } = &node.operation else {
        panic!("runtime node must remain a deduplicator");
    };
    assert_eq!(*max_time, Duration::from_secs(30));

    let mut incompatible = template;
    let super::RelayProcessorOperationTemplate::Deduplicator { deduplicate_on, .. } =
        &mut incompatible.operation
    else {
        panic!("test template must remain a deduplicator");
    };
    *deduplicate_on = vec![expression("input.other_id")];
    assert!(
        node.apply_node_template(incompatible)
            .expect_err("a keyspace change must not hot-refresh")
            .contains("state keyspace")
    );
}

#[tokio::test]
async fn processor_branch_tasks_are_created_and_reused_per_branch_key() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let graph: super::SharedActiveGraph = StdArc::new(ArcSwapOption::from(None));
    let schema = Arc::new(compile_schema(&CreateSchema {
        name: identifier("notification"),
        fields: vec![SchemaField {
            name: identifier("user_id"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    }));
    let template = super::BranchInstanceTemplate {
        source_kind: ModelKind::Deduplicator,
        source: identifier("dedup_users"),
        root_relay: identifier("orders"),
        branch: None,
        branch_ttl: None,
        branch_max_instances: None,
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            identifier("projected_orders"),
            super::RelayProcessorRelayTemplate {
                registry: super::RelayRegistry::new(),
                services: test_relay_boundary_services(),
            },
        )]
        .into_iter()
        .collect(),
        materialized_streams: HashSet::default(),
        processors: [(
            identifier("dedup_users"),
            super::RelayProcessorTemplate {
                kind: ModelKind::Deduplicator,
                processor: identifier("dedup_users"),
                input_relays: vec![identifier("orders")],
                input_collect_policies: HashMap::default(),
                error_policies: ErrorPolicies::handled_by_log(),
                from_where: HashMap::default(),
                filter_where: None,
                materialized_state: Vec::new(),
                operation: super::RelayProcessorOperationTemplate::Deduplicator {
                    output_routes: super::RelayProcessorOutputsTemplate {
                        routes: vec![super::RelayProcessorOutputTemplate {
                            output_relay: identifier("projected_orders"),
                            construction: nervix_models::RouteConstruction {
                                inherit: Some(nervix_models::Inheritance::All),
                                ..nervix_models::RouteConstruction::default()
                            },
                            flush_policy: Some(super::RuntimeFlushPolicy::Immediate),
                            message_error_policy: MessageErrorPolicy::Log,
                        }],
                    },
                    deduplicate_on: vec![expression("input.user_id")],
                    max_time: Duration::from_secs(600),
                },
            },
        )]
        .into_iter()
        .collect(),
    };
    let mut instances =
        super::BranchInstanceRegistry::<Option<super::BranchKey>, super::ProcessorBranchTask>::new(
        );
    let now = super::current_timestamp();
    let dequeued_work = || {
        super::NodeQuiesceWorkGuard::begin(
            runtime.node_quiesce_counters(&domain, &identifier("dedup_users")),
        )
    };
    let branch_batch = |user_id: i64, tenant: &str| {
        super::RelayRecordBatch::from_messages(
            schema.clone(),
            vec![RelayMessage {
                key: string_branch_key("tenant", tenant),
                record: test_runtime_row([("user_id".to_string(), RuntimeValue::I64(user_id))]),
                acks: AckSet::empty(),
            }],
        )
        .expect("branch batch should build")
    };

    super::dispatch_processor_node_input(
        super::ProcessorNodeDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            graph: &graph,
            template: &template,
            now,
        },
        &mut instances,
        identifier("orders"),
        branch_batch(42, "acme"),
        dequeued_work(),
    )
    .await;
    let mut states = instances.states();
    assert_eq!(states.len(), 1);
    let first = states.pop().expect("first branch task must exist");

    super::dispatch_processor_node_input(
        super::ProcessorNodeDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            graph: &graph,
            template: &template,
            now,
        },
        &mut instances,
        identifier("orders"),
        branch_batch(43, "acme"),
        dequeued_work(),
    )
    .await;
    let states = instances.states();
    assert_eq!(states.len(), 1);
    assert!(
        Arc::ptr_eq(&first, &states[0]),
        "same branch key must reuse the existing processor branch task"
    );

    super::dispatch_processor_node_input(
        super::ProcessorNodeDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            graph: &graph,
            template: &template,
            now,
        },
        &mut instances,
        identifier("orders"),
        branch_batch(7, "beta"),
        dequeued_work(),
    )
    .await;
    assert_eq!(instances.states().len(), 2);

    super::shutdown_all_processor_branch_instances(
        &runtime,
        &domain,
        &identifier("dedup_users"),
        None,
        &mut instances,
    )
    .await;
    assert!(instances.states().is_empty());
}

fn junction_branch_template(processor: &str, input_relay: &str) -> super::BranchInstanceTemplate {
    let processor = identifier(processor);
    let input_relay = identifier(input_relay);
    super::BranchInstanceTemplate {
        source_kind: ModelKind::Junction,
        source: processor.clone(),
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

fn quiesce_test_batch() -> super::RelayRecordBatch {
    super::RelayRecordBatch::single(
        test_schema(&[("value", ParseAsType::I64)]),
        None,
        test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
        AckSet::empty(),
    )
    .expect("quiesce test batch should build")
}

#[test]
fn pending_materialized_batches_remain_visible_in_entity_drain_status() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let processor = identifier("wait_for_customer");
    let input_relay = identifier("orders");
    let template = junction_branch_template(processor.as_str(), input_relay.as_str());
    let mut branch = template
        .instantiate(&runtime, &domain, None)
        .expect("junction branch should instantiate")
        .into_inner();
    branch
        .processors
        .get_mut(&processor)
        .expect("junction processor should exist")
        .pending_materialized
        .push_back(super::PendingMaterializedBatch::new(
            input_relay,
            quiesce_test_batch(),
        ));
    let counters = runtime.node_quiesce_counters(&domain, &processor);
    let mut gauges = super::BranchQuiesceGauges::new(counters.clone());

    gauges.observe(&branch, &processor);

    assert_eq!(counters.collected_inputs.load(Ordering::Acquire), 0);
    assert_eq!(counters.pending_materialized.load(Ordering::Acquire), 1);
    let status = runtime.entity_drain_status(
        &domain,
        &[],
        &[crate::registry::RegistryEntity {
            kind: ModelKind::Junction,
            identifier: processor.clone(),
        }],
        EntityGatePurpose::ModelAlteration,
    );
    assert_eq!(status.node_work_items, 1);
    assert!(!status.is_drained());

    let handoff_status = runtime.entity_drain_status(
        &domain,
        &[],
        &[crate::registry::RegistryEntity {
            kind: ModelKind::Junction,
            identifier: processor,
        }],
        EntityGatePurpose::OwnershipHandoff,
    );
    assert_eq!(handoff_status.node_work_items, 0);
    assert!(handoff_status.is_drained());

    drop(gauges);
    assert_eq!(counters.outstanding_work(), 0);
}

#[test]
fn blocking_materialized_wait_is_excluded_only_from_ownership_handoff_work() {
    let counters = Arc::new(super::NodeQuiesceCounters::default());
    let mut work = super::NodeQuiesceWorkGuard::begin(counters.clone());

    assert_eq!(
        counters.outstanding_work_for(EntityGatePurpose::ModelAlteration),
        1
    );
    assert_eq!(
        counters.outstanding_work_for(EntityGatePurpose::OwnershipHandoff),
        1
    );

    work.park_for_required_materialized_state();
    assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 0);
    assert_eq!(counters.pending_materialized.load(Ordering::Acquire), 1);
    assert_eq!(
        counters.outstanding_work_for(EntityGatePurpose::ModelAlteration),
        1
    );
    assert_eq!(
        counters.outstanding_work_for(EntityGatePurpose::OwnershipHandoff),
        0
    );

    work.resume_from_required_materialized_state();
    assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 1);
    assert_eq!(counters.pending_materialized.load(Ordering::Acquire), 0);
    drop(work);
    assert_eq!(counters.outstanding_work(), 0);
}

#[tokio::test]
async fn dropping_pending_materialized_batch_nacks_its_ack_root() {
    let tracker = Arc::new(AckRootTracker::default());
    let (acks, completion) = AckSet::tracked_root(tracker.clone());
    let pending = super::PendingMaterializedBatch::new(
        identifier("orders"),
        super::RelayRecordBatch::single(
            test_schema(&[("value", ParseAsType::I64)]),
            None,
            test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
            acks,
        )
        .expect("pending materialized test batch should build"),
    );

    assert_eq!(tracker.outstanding(), 1);
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    drop(pending);

    assert_eq!(
        completion.wait().await,
        AckOutcome::NoAck(
            "node stopped while waiting for required materialized state at relay 'orders'"
                .to_string()
        )
    );
    assert_eq!(tracker.outstanding(), 0);
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
}

#[test]
fn required_wait_ack_does_not_block_ownership_handoff_drain_status() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let ingestor = identifier("orders_source");
    let tracker = Arc::new(AckRootTracker::default());
    runtime.in_flight_by_ingestor.insert(
        super::RuntimeKey::new(domain.clone(), ingestor.clone()),
        tracker.clone(),
    );
    let (acks, _completion) = AckSet::tracked_root(tracker);
    let _required_wait = acks.required_wait_guard();
    let affected = [crate::registry::RegistryEntity {
        kind: ModelKind::Ingestor,
        identifier: ingestor,
    }];

    let alteration =
        runtime.entity_drain_status(&domain, &[], &affected, EntityGatePurpose::ModelAlteration);
    assert_eq!(alteration.outstanding_acks, 1);
    assert!(!alteration.is_drained());

    let handoff =
        runtime.entity_drain_status(&domain, &[], &affected, EntityGatePurpose::OwnershipHandoff);
    assert_eq!(handoff.outstanding_acks, 0);
    assert!(handoff.is_drained());
}

#[tokio::test]
async fn relay_owner_buffer_remains_visible_in_entity_drain_status() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("orders");
    let services = test_relay_boundary_services();
    let _owner_receiver = services.activate_owner_buffer();
    runtime
        .relay_boundary_fanouts
        .insert((domain.clone(), relay.clone()), services.fanout.clone());
    services
        .enqueue_owner_batch(
            &runtime.metrics,
            &domain,
            &relay,
            None,
            &quiesce_test_batch(),
        )
        .await
        .expect("the relay owner buffer should admit the batch");

    let status = runtime.entity_drain_status(
        &domain,
        std::slice::from_ref(&relay),
        &[],
        EntityGatePurpose::ModelAlteration,
    );

    assert_eq!(status.buffered_relay_batches, 1);
    assert!(!status.is_drained());
}

#[tokio::test]
async fn relay_owner_buffer_retains_the_upstream_ack_until_fanout() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("orders");
    let services = test_relay_boundary_services();
    let mut owner_receiver = services.activate_owner_buffer();
    let (acks, completion) = AckSet::root();
    let batch = super::RelayRecordBatch::single(
        test_schema(&[("value", ParseAsType::I64)]),
        None,
        test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
        acks.clone(),
    )
    .expect("relay owner ACK test batch should build");

    services
        .enqueue_owner_batch(&runtime.metrics, &domain, &relay, None, &batch)
        .await
        .expect("the relay owner buffer should admit the batch");
    let completion = completion.wait();
    tokio::pin!(completion);
    acks.ack_success();
    assert!(
        timeout(Duration::from_millis(50), &mut completion)
            .await
            .is_err(),
        "owner admission must not complete the upstream ACK before fanout"
    );

    owner_receiver
        .recv()
        .await
        .expect("the owner buffer should retain the batch")
        .ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), &mut completion)
            .await
            .expect("owner fanout completion should resolve the ACK"),
        AckOutcome::Ack
    );
}

#[tokio::test]
async fn processor_dispatch_hands_dequeued_work_into_branch_mailbox() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let processor = identifier("route_orders");
    let input_relay = identifier("orders");
    let template = junction_branch_template(processor.as_str(), input_relay.as_str());
    let counters = runtime.node_quiesce_counters(&domain, &processor);
    let (input_tx, mut input_rx) = mpsc::channel(1);
    let (stop_tx, _stop_rx) = mpsc::channel(1);
    let task = tokio::spawn(std::future::pending::<()>());
    let mut instances =
        super::BranchInstanceRegistry::<Option<super::BranchKey>, super::ProcessorBranchTask>::new(
        );
    instances.insert_restored(
        None,
        super::current_timestamp(),
        super::ProcessorBranchTask {
            input: input_tx,
            stop: stop_tx,
            task: parking_lot::Mutex::new(Some(task)),
        },
    );

    super::dispatch_processor_node_input(
        super::ProcessorNodeDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            graph: &StdArc::new(ArcSwapOption::from(None)),
            template: &template,
            now: super::current_timestamp(),
        },
        &mut instances,
        input_relay.clone(),
        quiesce_test_batch(),
        super::NodeQuiesceWorkGuard::begin(counters.clone()),
    )
    .await;

    assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 1);
    let queued = input_rx
        .recv()
        .await
        .expect("processor input should remain in the branch mailbox");
    assert_eq!(queued.relay, input_relay);
    assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 1);
    drop(queued);
    assert_eq!(counters.mailbox_and_in_flight.load(Ordering::Acquire), 0);

    let entry = instances
        .remove(&None)
        .expect("test branch task should remain registered");
    let task = entry
        .task
        .lock()
        .take()
        .expect("test branch task should still be running");
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn processor_handoff_drains_ready_batches_from_every_input() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let processor = identifier("route_orders");
    let orders = identifier("orders");
    let returns = identifier("returns");
    let mut template = junction_branch_template(processor.as_str(), orders.as_str());
    template
        .processors
        .get_mut(&processor)
        .expect("junction processor should exist")
        .input_relays
        .push(returns.clone());
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let orders_broadcast = super::RelayBroadcast::with_capacity(nonzero_capacity(2));
    let returns_broadcast = super::RelayBroadcast::with_capacity(nonzero_capacity(2));
    let orders_input = super::RelayRuntimeFanIn::new(orders_broadcast.new_receiver());
    let returns_input = super::RelayRuntimeFanIn::new(returns_broadcast.new_receiver());
    let acme = string_branch_key("tenant", "acme");
    let beta = string_branch_key("tenant", "beta");
    let batch = |key, value| {
        super::RelayRecordBatch::single(
            schema.clone(),
            key,
            test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]),
            AckSet::empty(),
        )
        .expect("processor input batch should build")
    };
    orders_broadcast
        .broadcast(batch(acme.clone(), 1))
        .await
        .expect("orders batch should queue");
    returns_broadcast
        .broadcast(batch(beta.clone(), 2))
        .await
        .expect("returns batch should queue");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (commands, command_rx) = mpsc::channel(1);
    let (response, handoffs) = tokio::sync::oneshot::channel();
    commands
        .send(super::ProcessorNodeCommand::Handoff { response })
        .await
        .expect("handoff command should queue before the processor starts");
    let task = tokio::spawn(super::run_processor_node_runtime(
        super::ProcessorRuntimeContext::new(
            runtime.clone(),
            domain.clone(),
            StdArc::new(ArcSwapOption::from(None)),
        ),
        template,
        vec![(orders, orders_input), (returns, returns_input)],
        shutdown_rx,
        command_rx,
        Vec::new(),
        Duration::from_secs(60),
    ));

    let handoffs = timeout(Duration::from_secs(2), handoffs)
        .await
        .expect("processor handoff should finish")
        .expect("processor should return handoff state");
    timeout(Duration::from_secs(2), task)
        .await
        .expect("processor supervisor should stop")
        .expect("processor supervisor should join");
    assert_eq!(handoffs.len(), 2);
    assert!(handoffs.iter().any(|handoff| handoff.key == acme));
    assert!(handoffs.iter().any(|handoff| handoff.key == beta));
    assert_eq!(
        runtime
            .node_quiesce_counters(&domain, &processor)
            .outstanding_work(),
        0
    );
    drop(shutdown_tx);
}

#[tokio::test]
async fn scheduled_processor_handoff_bounds_command_backpressure_and_aborts_the_task() {
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let (commands, _command_rx) = mpsc::channel(1);
    let (first_response, _first_receiver) = tokio::sync::oneshot::channel();
    commands
        .send(super::ProcessorNodeCommand::Handoff {
            response: first_response,
        })
        .await
        .expect("first command should fill the processor mailbox");
    let dropped = Arc::new(AtomicBool::new(false));
    let task_dropped = dropped.clone();
    let task = tokio::spawn(async move {
        let _dropped = Dropped(task_dropped);
        std::future::pending::<()>().await;
    });
    let scheduled = super::ScheduledNodeTask { commands, task };

    let error = scheduled
        .handoff_within(Duration::from_millis(10))
        .await
        .expect_err("a full command mailbox must bound handoff");

    assert_eq!(
        error,
        "scheduled node task timed out accepting handoff".to_string()
    );
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn scheduled_processor_handoff_aborts_a_task_that_drops_its_response() {
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let (commands, mut command_rx) = mpsc::channel(1);
    let dropped = Arc::new(AtomicBool::new(false));
    let task_dropped = dropped.clone();
    let task = tokio::spawn(async move {
        let _dropped = Dropped(task_dropped);
        let Some(super::ProcessorNodeCommand::Handoff { response }) = command_rx.recv().await
        else {
            panic!("scheduled processor must receive its handoff command")
        };
        drop(response);
        std::future::pending::<()>().await;
    });
    let scheduled = super::ScheduledNodeTask { commands, task };

    let error = scheduled
        .handoff()
        .await
        .expect_err("a dropped handoff response must fail");

    assert_eq!(
        error,
        "scheduled node task dropped its handoff response".to_string()
    );
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn runtime_state_store_persists_latest_snapshot_with_monotonic_lsm() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };

    let first_lsm = 1;
    store
        .persist_latest_snapshot(&placement, first_lsm, b"first")
        .expect("first snapshot should persist");
    let second_lsm = 2;
    store
        .persist_latest_snapshot(&placement, second_lsm, b"second")
        .expect("second snapshot should persist");

    assert_eq!(first_lsm, 1);
    assert_eq!(second_lsm, 2);
    assert_eq!(
        store
            .latest_snapshot(&placement)
            .expect("latest snapshot should load")
            .expect("latest snapshot should exist")
            .payload,
        b"second".to_vec()
    );
}

async fn wait_for_persisted_runtime_state_lsm(
    runtime: &super::Runtime,
    placement: &RuntimeStatePlacement,
    expected_lsm: u64,
) {
    let store = runtime
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

#[tokio::test]
async fn deduplicator_snapshot_task_persists_dirty_state_on_interval() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime =
        super::Runtime::with_persistence(Some(db), Duration::from_millis(10), Default::default())
            .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_deduplicator_state(placement.clone())
        .expect("deduplicator state should initialize");
    let (shutdown_tx, _) = watch::channel(false);
    let task = runtime
        .spawn_deduplicator_snapshot_task(&shutdown_tx, state.clone())
        .expect("persisted runtime should spawn a snapshot task");

    assert!(state.reserve_new_key(
        super::DeduplicatorKey::new(vec![super::ReorderKeyPart::Utf8("txn-1".to_string())]),
        Timestamp::from_unix_nanos(1),
        Duration::from_secs(600),
    ));
    let expected_lsm = state.current_lsm.load(Ordering::SeqCst);

    wait_for_persisted_runtime_state_lsm(&runtime, &placement, expected_lsm).await;
    assert_eq!(
        state.last_persisted_lsm.load(Ordering::SeqCst),
        expected_lsm
    );
    assert!(!state.dirty.load(Ordering::SeqCst));

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");
}

#[tokio::test]
async fn materialized_relay_snapshot_task_owns_persistence() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime =
        super::Runtime::with_persistence(Some(db), Duration::from_secs(3_600), Default::default())
            .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::MaterializedRelay,
        kind: ModelKind::Relay,
        identifier: identifier("latest_orders"),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    let schema = test_schema(&[("status", ParseAsType::String)]);
    let state = runtime
        .replicated_materialized_stream_state(placement.clone(), schema.arrow_schema(), None)
        .expect("materialized relay state should initialize");
    let (shutdown_tx, _) = watch::channel(false);
    let task = runtime
        .spawn_materialized_stream_snapshot_task(&shutdown_tx, state.clone())
        .expect("persisted runtime should spawn a snapshot task");
    let record = test_runtime_row([(
        "status".to_string(),
        RuntimeValue::String("ready".to_string()),
    )]);

    runtime.update_materialized_stream_last_by_timestamp(&state, &None, &record);

    assert_eq!(state.current_lsm.load(Ordering::SeqCst), 1);
    assert!(state.dirty.load(Ordering::SeqCst));
    assert_eq!(state.last_persisted_lsm.load(Ordering::SeqCst), 0);
    assert!(
        runtime
            .state_store
            .as_ref()
            .expect("test runtime should have a state store")
            .latest_snapshot(&placement)
            .expect("snapshot lookup should succeed")
            .is_none(),
        "the relay-state hot path must not persist a snapshot"
    );

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");
    assert_eq!(
        runtime
            .state_store
            .as_ref()
            .expect("test runtime should have a state store")
            .latest_snapshot(&placement)
            .expect("snapshot lookup should succeed")
            .expect("shutdown should flush the dirty snapshot")
            .lsm,
        1
    );
}

#[tokio::test]
async fn window_processor_snapshot_task_persists_dirty_state_on_interval() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime =
        super::Runtime::with_persistence(Some(db), Duration::from_millis(10), Default::default())
            .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::WindowProcessor,
        kind: ModelKind::WindowProcessor,
        identifier: identifier("latency_window"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_window_processor_state(placement.clone())
        .expect("window processor state should initialize");
    let (shutdown_tx, _) = watch::channel(false);
    let (snapshot_request_tx, mut snapshot_requests) = mpsc::channel(1);
    let task = runtime
        .spawn_window_processor_snapshot_task(&shutdown_tx, state.clone(), snapshot_request_tx)
        .expect("persisted runtime should spawn a snapshot task");
    let live_state =
        WindowProcessorState::new(&window_aggregate("SET count = COUNT(input.latency)"));
    let snapshot_state = state.clone();
    let snapshot_owner = tokio::spawn(async move {
        let response = timeout(Duration::from_secs(1), snapshot_requests.recv())
            .await
            .expect("snapshot task should request live state")
            .expect("snapshot request channel should remain open");
        let result = snapshot_state
            .replace_state(&live_state)
            .map(|_| ())
            .map_err(|error| error.to_string());
        let _ = response.send(result);
    });

    state.mark_live_dirty();
    let expected_lsm = 1;

    wait_for_persisted_runtime_state_lsm(&runtime, &placement, expected_lsm).await;
    snapshot_owner
        .await
        .expect("snapshot owner should stop cleanly");
    assert_eq!(
        state.last_persisted_lsm.load(Ordering::SeqCst),
        expected_lsm
    );
    assert!(!state.dirty.load(Ordering::SeqCst));

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");
}

#[test]
fn runtime_state_store_purges_only_stale_schema_fingerprints() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let base = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: None,
    };
    let current = RuntimeStatePlacement {
        schema_fingerprint: [2; 32],
        ..base.clone()
    };
    store
        .persist_latest_snapshot(&base, 1, b"old")
        .expect("old snapshot should persist");
    store
        .persist_latest_snapshot(&current, 2, b"current")
        .expect("current snapshot should persist");

    store
        .purge_stale_schema_fingerprints(
            &base.domain,
            &HashMap::from_iter([(
                (base.kind, base.identifier.clone()),
                current.schema_fingerprint,
            )]),
        )
        .expect("stale snapshots should purge");

    assert!(
        store
            .latest_snapshot(&base)
            .expect("old snapshot lookup should succeed")
            .is_none()
    );
    assert_eq!(
        store
            .latest_snapshot(&current)
            .expect("current snapshot lookup should succeed")
            .expect("current snapshot should remain")
            .payload,
        b"current".to_vec()
    );
}

#[test]
fn runtime_state_store_purges_only_the_requested_domain() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let stopped = RuntimeStatePlacement {
        domain: domain("stopped"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: None,
    };
    let running = RuntimeStatePlacement {
        domain: domain("running"),
        ..stopped.clone()
    };
    store
        .persist_latest_snapshot(&stopped, 1, b"stopped")
        .expect("stopped-domain snapshot should persist");
    store
        .persist_latest_snapshot(&running, 2, b"running")
        .expect("running-domain snapshot should persist");

    store
        .purge_domain(&stopped.domain)
        .expect("stopped-domain snapshots should purge");

    assert!(
        store
            .latest_snapshot(&stopped)
            .expect("stopped-domain snapshot lookup should succeed")
            .is_none()
    );
    assert_eq!(
        store
            .latest_snapshot(&running)
            .expect("running-domain snapshot lookup should succeed")
            .expect("running-domain snapshot should remain")
            .payload,
        b"running".to_vec()
    );
}

#[test]
fn runtime_state_store_purges_only_the_requested_entity() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let removed = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::MaterializedRelay,
        kind: ModelKind::Relay,
        identifier: identifier("events"),
        schema_fingerprint: [1; 32],
        branch_key: None,
    };
    let retained = RuntimeStatePlacement {
        identifier: identifier("audit"),
        ..removed.clone()
    };
    store
        .persist_latest_snapshot(&removed, 1, b"removed")
        .expect("removed snapshot should persist");
    store
        .persist_latest_snapshot(&retained, 2, b"retained")
        .expect("retained snapshot should persist");

    store
        .purge_entity(
            &removed.domain,
            removed.state,
            removed.kind,
            &removed.identifier,
        )
        .expect("entity snapshots should purge");

    assert!(
        store
            .latest_snapshot(&removed)
            .expect("removed snapshot lookup should succeed")
            .is_none()
    );
    assert_eq!(
        store
            .latest_snapshot(&retained)
            .expect("retained snapshot lookup should succeed")
            .expect("unrelated entity snapshot should remain")
            .payload,
        b"retained".to_vec()
    );
}

#[test]
fn kafka_offset_state_roundtrips_partition_schedule_through_fjall() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::KafkaOffset,
        kind: ModelKind::Ingestor,
        identifier: identifier("kafka_notifications"),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    let state =
        super::ReplicatedKafkaOffsetState::new(placement.clone(), None, Vec::new(), 0, None)
            .expect("kafka state should initialize");
    let (offset_lsm, offset_payload) = state
        .replace_offsets(HashMap::from_iter([
            (("notifications".to_string(), 0), 12),
            (("notifications".to_string(), 1), 18),
        ]))
        .expect("offsets should update");
    store
        .persist_latest_snapshot(&placement, offset_lsm, &offset_payload)
        .expect("offset snapshot should persist");
    let (schedule_lsm, schedule_payload) = state
        .update_partition_schedule("notifications", 2, vec![0, 1])
        .expect("schedule should update")
        .expect("schedule snapshot should be produced");
    store
        .persist_latest_snapshot(&placement, schedule_lsm, &schedule_payload)
        .expect("schedule snapshot should persist");

    let restored = super::ReplicatedKafkaOffsetState::new(
        placement.clone(),
        None,
        Vec::new(),
        0,
        store
            .latest_snapshot(&placement)
            .expect("snapshot should load"),
    )
    .expect("restored kafka state should initialize");
    assert_eq!(restored.next_offset("notifications", 0), Some(12));
    assert_eq!(restored.next_offset("notifications", 1), Some(18));
    assert_eq!(
        restored.describe_topic("notifications"),
        Some(super::KafkaDomainOffsetDescribe {
            topic: "notifications".to_string(),
            instances: 2,
            observed_partitions: vec![0, 1],
            rebalance_epoch: 0,
            instance_assignments: vec![vec![0], vec![1]],
        })
    );
}

#[test]
fn branch_aggregated_state_snapshot_roundtrips_metrics() {
    let metrics = RuntimeMetrics::default();
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: identifier("redis_notifications"),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    let relay = identifier("notifications");
    let state = super::ReplicatedBranchAggregatedState::new(
        placement.clone(),
        Some("node-1".to_string()),
        "node-1".to_string(),
        Vec::new(),
        0,
        &metrics,
        None,
    )
    .expect("branch-aggregated state should initialize");
    metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
        domain: &placement.domain,
        kind: placement.kind,
        node: &placement.identifier,
        relay: &relay,
        physical_node_id: Some("node-1"),
        messages: 2,
        bytes: 64,
        domain_timestamp: None,
    });
    let lsm = state.mark_metrics_updated();
    let snapshot = state
        .latest_snapshot(&metrics)
        .expect("metrics snapshot should encode");
    assert_eq!(snapshot.lsm, lsm);

    let restored_metrics = RuntimeMetrics::default();
    let _restored = super::ReplicatedBranchAggregatedState::new(
        placement.clone(),
        Some("node-1".to_string()),
        "node-1".to_string(),
        Vec::new(),
        0,
        &restored_metrics,
        Some(snapshot),
    )
    .expect("branch-aggregated state should restore");

    let rendered = restored_metrics.describe_global_target(
        &placement.domain,
        "INGESTOR",
        &placement.identifier,
    );
    assert!(
        rendered.iter().any(
            |line| line.contains("messages_total sent relay=notifications")
                && line.contains("total=2")
        ),
        "expected restored metrics total in {rendered:?}"
    );
}

#[test]
fn describe_restores_branch_aggregated_metrics_from_store_without_materialized_state() {
    let dir = tempdir().expect("temp dir should open");
    let domain = domain("default");
    let ingestor = identifier("redis_notifications");
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: ingestor.clone(),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    {
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let store = RuntimeStateStore::from_database(db).expect("state store should open");
        let metrics = RuntimeMetrics::default();
        metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
            domain: &domain,
            kind: ModelKind::Ingestor,
            node: &ingestor,
            relay: &identifier("notifications"),
            physical_node_id: Some("node-3"),
            messages: 19,
            bytes: 1900,
            domain_timestamp: None,
        });
        let snapshot = super::BranchAggregatedRuntimeStateSnapshot {
            metrics: metrics.snapshot_global_target(
                &domain,
                ModelKind::Ingestor,
                &ingestor,
                "node-3",
            ),
        };
        let payload = super::encode_branch_aggregated_snapshot(&snapshot)
            .expect("branch-aggregated snapshot should encode");
        store
            .persist_latest_snapshot(&placement, 7, &payload)
            .expect("snapshot should persist");
    }

    let db = Database::builder(dir.path())
        .open()
        .expect("db should reopen");
    let runtime =
        super::Runtime::with_persistence(Some(db), Duration::from_millis(100), Default::default())
            .expect("runtime should open persisted state");
    runtime
        .metrics
        .register_global_node(&domain, ModelKind::Ingestor, &ingestor, Some("node-3"));

    let rendered = runtime.describe_metrics_for(&domain, "INGESTOR", &ingestor);
    assert!(
        rendered.iter().any(|line| line
            .contains("messages_total sent relay=notifications physical_node=node-3")
            && line.contains("total=19")),
        "expected persisted branch-aggregated metrics before START retry in {rendered:?}"
    );
}

#[test]
fn describe_restores_branch_aggregated_metrics_when_state_lsm_is_current_but_metrics_missing() {
    let dir = tempdir().expect("temp dir should open");
    let domain = domain("default");
    let ingestor = identifier("redis_notifications");
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: ingestor.clone(),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db.clone()).expect("state store should open");
    let persisted_metrics = RuntimeMetrics::default();
    persisted_metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
        domain: &domain,
        kind: ModelKind::Ingestor,
        node: &ingestor,
        relay: &identifier("notifications"),
        physical_node_id: Some("node-3"),
        messages: 19,
        bytes: 1900,
        domain_timestamp: None,
    });
    let snapshot = super::BranchAggregatedRuntimeStateSnapshot {
        metrics: persisted_metrics.snapshot_global_target(
            &domain,
            ModelKind::Ingestor,
            &ingestor,
            "node-3",
        ),
    };
    let payload = super::encode_branch_aggregated_snapshot(&snapshot)
        .expect("branch-aggregated snapshot should encode");
    store
        .persist_latest_snapshot(&placement, 7, &payload)
        .expect("snapshot should persist");

    let runtime =
        super::Runtime::with_persistence(Some(db), Duration::from_millis(100), Default::default())
            .expect("runtime should open persisted state");
    let stale_state = Arc::new(
        super::ReplicatedBranchAggregatedState::new(
            placement.clone(),
            Some("node-3".to_string()),
            "node-3".to_string(),
            Vec::new(),
            0,
            &RuntimeMetrics::default(),
            store
                .latest_snapshot(&placement)
                .expect("snapshot should load"),
        )
        .expect("stale state should initialize"),
    );
    stale_state.mark_metrics_updated();
    runtime
        .replicated_branch_aggregated_states
        .insert(placement, stale_state);
    runtime
        .metrics
        .register_global_node(&domain, ModelKind::Ingestor, &ingestor, Some("node-3"));

    let rendered = runtime.describe_metrics_for(&domain, "INGESTOR", &ingestor);
    assert!(
        rendered.iter().any(|line| line
            .contains("messages_total sent relay=notifications physical_node=node-3")
            && line.contains("total=19")),
        "expected persisted branch-aggregated metrics despite current stale LSM in {rendered:?}"
    );
}

#[test]
fn describe_does_not_reapply_equal_lsm_snapshot_over_active_metrics() {
    let dir = tempdir().expect("temp dir should open");
    let domain = domain("default");
    let ingestor = identifier("redis_notifications");
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: ingestor.clone(),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db.clone()).expect("state store should open");
    let persisted_metrics = RuntimeMetrics::default();
    persisted_metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
        domain: &domain,
        kind: ModelKind::Ingestor,
        node: &ingestor,
        relay: &identifier("notifications"),
        physical_node_id: Some("node-3"),
        messages: 19,
        bytes: 1900,
        domain_timestamp: None,
    });
    let snapshot = super::BranchAggregatedRuntimeStateSnapshot {
        metrics: persisted_metrics.snapshot_global_target(
            &domain,
            ModelKind::Ingestor,
            &ingestor,
            "node-3",
        ),
    };
    let payload = super::encode_branch_aggregated_snapshot(&snapshot)
        .expect("branch-aggregated snapshot should encode");
    store
        .persist_latest_snapshot(&placement, 7, &payload)
        .expect("snapshot should persist");

    let runtime =
        super::Runtime::with_persistence(Some(db), Duration::from_millis(100), Default::default())
            .expect("runtime should open persisted state");
    let state = Arc::new(
        super::ReplicatedBranchAggregatedState::new(
            placement.clone(),
            Some("node-3".to_string()),
            "node-3".to_string(),
            Vec::new(),
            0,
            &RuntimeMetrics::default(),
            store
                .latest_snapshot(&placement)
                .expect("snapshot should load"),
        )
        .expect("state should initialize"),
    );
    runtime
        .replicated_branch_aggregated_states
        .insert(placement, state);
    runtime
        .metrics
        .observe_global_node_sent(crate::metrics::NodeBatchObservation {
            domain: &domain,
            kind: ModelKind::Ingestor,
            node: &ingestor,
            relay: &identifier("notifications"),
            physical_node_id: Some("node-3"),
            messages: 1,
            bytes: 100,
            domain_timestamp: None,
        });

    let rendered = runtime.describe_metrics_for(&domain, "INGESTOR", &ingestor);
    assert!(
        rendered.iter().any(|line| line
            .contains("messages_total sent relay=notifications physical_node=node-3")
            && line.contains(" total=1 ")),
        "expected active metrics to remain authoritative for equal LSM in {rendered:?}"
    );
}

#[tokio::test]
async fn state_sync_request_returns_latest_snapshot_only_when_lsm_advances() {
    let runtime = super::Runtime::default();
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_deduplicator_state(placement.clone())
        .expect("deduplicator state should initialize");
    assert!(state.reserve_new_key(
        super::DeduplicatorKey::new(vec![super::ReorderKeyPart::Utf8("txn-1".to_string())]),
        Timestamp::from_unix_nanos(1),
        Duration::from_secs(600),
    ));
    let lsm = state.current_lsm.load(Ordering::SeqCst);

    let first = runtime
        .handle_state_sync_request(&placement, 0)
        .await
        .expect("state sync request should succeed")
        .expect("snapshot should be returned");
    assert_eq!(first.lsm, lsm);

    let none = runtime
        .handle_state_sync_request(&placement, lsm)
        .await
        .expect("state sync request should succeed");
    assert!(none.is_none());
}

#[test]
fn deduplicator_key_reservation_reports_new_and_duplicate_keys() {
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = super::ReplicatedDeduplicatorState::new(placement, None)
        .expect("deduplicator state should initialize");
    let seen_at = Timestamp::from_unix_nanos(1);
    let max_time = Duration::from_secs(600);

    let key = super::DeduplicatorKey::new(vec![super::ReorderKeyPart::Utf8("txn-1".to_string())]);
    assert!(state.reserve_new_key(key.clone(), seen_at, max_time));
    assert!(!state.reserve_new_key(key, seen_at, max_time));
    assert_eq!(state.current_lsm.load(Ordering::SeqCst), 1);
}

#[test]
fn runtime_state_placement_storage_key_includes_branch_key() {
    let tenant_beta = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: string_branch_key("tenant", "beta"),
    };
    let tenant = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };

    assert_ne!(tenant_beta.as_storage_key(), tenant.as_storage_key());
    let branch_aggregated = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    assert_ne!(
        tenant_beta.as_storage_key(),
        branch_aggregated.as_storage_key()
    );
    let deduplicator_global = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: None,
    };
    assert_ne!(
        deduplicator_global.as_storage_key(),
        branch_aggregated.as_storage_key()
    );
}

#[test]
fn schema_fingerprints_reuse_unaffected_state_and_isolate_changed_state() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let identifier = identifier("dedup_orders");
    let schedule = |fingerprint| DomainSchedule {
        domain: domain.clone(),
        nodes: vec![ScheduledNode {
            identifier: identifier.clone(),
            kind: ModelKind::Deduplicator,
            config: Box::new(nervix_models::Model::Schema(CreateSchema {
                name: identifier.clone(),
                fields: Vec::new(),
            })),
            effective_branching: None,
            effective_branching_schema: None,
            schema_fingerprint: fingerprint,
            kafka_partition_schedule: None,
            primary_node: Some("node-1".to_string()),
            assigned_nodes: vec!["node-1".to_string()],
        }],
        placement_groups: Vec::new(),
    };

    runtime.install_state_schema_fingerprints(&schedule([1; 32]));
    let original_placement = runtime.state_placement(
        &domain,
        RuntimeStateKind::Deduplicator,
        ModelKind::Deduplicator,
        &identifier,
        None,
    );
    let original = runtime
        .replicated_deduplicator_state(original_placement.clone())
        .expect("state should initialize");

    runtime.install_state_schema_fingerprints(&schedule([1; 32]));
    let unchanged = runtime
        .replicated_deduplicator_state(runtime.state_placement(
            &domain,
            RuntimeStateKind::Deduplicator,
            ModelKind::Deduplicator,
            &identifier,
            None,
        ))
        .expect("unchanged state should initialize");
    assert!(Arc::ptr_eq(&original, &unchanged));

    runtime.install_state_schema_fingerprints(&schedule([2; 32]));
    let changed = runtime
        .replicated_deduplicator_state(runtime.state_placement(
            &domain,
            RuntimeStateKind::Deduplicator,
            ModelKind::Deduplicator,
            &identifier,
            None,
        ))
        .expect("changed state should initialize");
    assert!(!Arc::ptr_eq(&original, &changed));
    runtime
        .purge_stale_runtime_state(&domain)
        .expect("stale state should purge");
    assert!(
        !runtime
            .replicated_deduplicator_states
            .contains_key(&original_placement)
    );
}

#[tokio::test]
async fn relay_dispatch_detaches_subscription_delivery_from_ack_chain() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay = Identifier::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("customer_id", ParseAsType::String)]);
    let registry = super::RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        super::RelayRetention::default(),
    );
    let mut subscription_rx = services.subscription_receiver();
    let mut runtime_rx = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Attached);

    let (acks, completion) = AckSet::root();
    let batch = super::RelayRecordBatch::single(
        schema,
        string_branch_key("customer", "42"),
        test_runtime_row([(
            "customer_id".to_string(),
            RuntimeValue::String("42".to_string()),
        )]),
        acks.clone(),
    )
    .expect("batch should build");

    runtime
        .ingest_stream_boundary_message(&domain, &relay, &registry, &services, &batch)
        .await
        .expect("dispatch should succeed");

    let subscription_batch = subscription_rx
        .recv()
        .await
        .expect("subscription should receive batch");
    assert!(subscription_batch.acks.iter().all(AckSet::is_empty));

    acks.ack_success();

    let runtime_message = runtime_rx
        .recv()
        .await
        .expect("runtime consumer should receive");
    for ack in runtime_message.acks.iter() {
        ack.ack_success();
    }

    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    drop(subscription_batch);
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn relay_dispatch_detaches_detached_runtime_consumers_from_ack_chain() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay = Identifier::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let registry = super::RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        super::RelayRetention::default(),
    );
    let mut runtime_rx = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Detached);
    let (acks, completion) = AckSet::root();
    let batch = super::RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 52),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(52))]),
        acks.clone(),
    )
    .expect("batch should build");

    runtime
        .ingest_stream_boundary_message(&domain, &relay, &registry, &services, &batch)
        .await
        .expect("dispatch should succeed");

    acks.ack_success();

    let runtime_message = runtime_rx
        .recv()
        .await
        .expect("runtime consumer should receive message");
    assert!(runtime_message.acks.iter().all(AckSet::is_empty));
    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn relay_runtime_consumer_broadcast_fans_out_to_multiple_attached_receivers() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay = Identifier::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let registry = super::RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        super::RelayRetention::default(),
    );
    let mut first_consumer = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Attached);
    let mut second_consumer = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Attached);

    let (acks, completion) = AckSet::root();
    let batch = super::RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 52),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(52))]),
        acks.clone(),
    )
    .expect("batch should build");

    runtime
        .ingest_stream_boundary_message(&domain, &relay, &registry, &services, &batch)
        .await
        .expect("dispatch should succeed");
    acks.ack_success();

    let first_message = first_consumer
        .recv()
        .await
        .expect("first runtime consumer should receive message");
    let second_message = second_consumer
        .recv()
        .await
        .expect("second runtime consumer should receive message");
    for ack in first_message.acks.iter().chain(second_message.acks.iter()) {
        ack.ack_success();
    }

    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn concrete_relay_reuses_branch_collapse_for_runtime_consumers() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay = Identifier::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let registry = super::RelayRegistry::new();
    let branch_collapse = Arc::new(super::BranchCollapseNode::with_capacity(nonzero_capacity(
        STUPID_CHANNEL_CAPACITY_REMOVE_ME,
    )));
    let mut first_fan_in = super::RelayRuntimeFanIn::new(
        branch_collapse.runtime_consumer_receiver_for_mode(AckMode::Attached),
    );
    let mut second_fan_in = super::RelayRuntimeFanIn::new(
        branch_collapse.runtime_consumer_receiver_for_mode(AckMode::Attached),
    );
    let services = Arc::new(super::RelayBoundaryServices::new(
        super::RelayBoundaryFanout::BranchCollapse(branch_collapse),
        2,
        0,
        Vec::new(),
        None,
    ));
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        super::RelayRetention::default(),
    );
    let mut relay_runtime = super::ConcreteRelayRuntime::new(super::ConcreteRelayRuntimeBuild {
        runtime: runtime.clone(),
        domain: domain.clone(),
        relay: relay.clone(),
        registry,
        services,
        key: Some(concrete_branch_key([(
            identifier("user_id"),
            RuntimeValue::U32(52),
        )])),
    });
    let (acks, completion) = AckSet::root();
    let batch = super::RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 52),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(52))]),
        acks.clone(),
    )
    .expect("batch should build");

    relay_runtime
        .dispatch_boundary(&batch)
        .await
        .expect("concrete relay should dispatch");

    let received = timeout(Duration::from_secs(1), first_fan_in.recv())
        .await
        .expect("first fan-in should receive")
        .expect("first fan-in should stay open");
    assert_eq!(received.message_count(), 1);
    for ack in received.acks {
        ack.ack_success();
    }
    let received = timeout(Duration::from_secs(1), second_fan_in.recv())
        .await
        .expect("second fan-in should receive")
        .expect("second fan-in should stay open");
    assert_eq!(received.message_count(), 1);
    for ack in received.acks {
        ack.ack_success();
    }
    acks.ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn unbranched_relay_uses_direct_fanout_without_branch_collapse() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("notifications");

    let fanout = runtime
        .relay_boundary_fanout_with_capacity(
            &domain,
            &relay,
            false,
            nonzero_capacity(STUPID_CHANNEL_CAPACITY_REMOVE_ME),
        )
        .await;

    assert!(!fanout.uses_branch_collapse());
}

#[tokio::test]
async fn execution_builder_uses_direct_fanout_for_unbranched_relay() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let schema = identifier("notification");
    let relay = identifier("notifications");

    runtime
        .rebuild_domain_from_schedule(
            "node-1",
            &domain,
            Some(DomainSchedule {
                domain: domain.clone(),
                nodes: vec![
                    scheduled_model(
                        ModelKind::Schema,
                        schema.clone(),
                        nervix_models::Model::Schema(CreateSchema {
                            name: schema.clone(),
                            fields: vec![nervix_models::SchemaField {
                                name: identifier("user_id"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            }],
                        }),
                    ),
                    scheduled_model(
                        ModelKind::Relay,
                        relay.clone(),
                        nervix_models::Model::Relay(CreateRelay {
                            name: relay.clone(),
                            schema,
                            buffer: STUPID_CHANNEL_CAPACITY_REMOVE_ME,
                            branching: RelayBranching::unbranched(),
                            materialized_state: None,
                        }),
                    ),
                ],
                placement_groups: Vec::new(),
            }),
            true,
        )
        .await
        .expect("unbranched relay execution should build");

    let execution = runtime
        .executions
        .get(&domain)
        .expect("domain execution should exist");
    let services = execution
        .relay_services
        .get(&relay)
        .expect("relay services should exist");
    assert!(!services.fanout.uses_branch_collapse());
}

#[test]
fn flush_immediate_schedules_100_microsecond_system_timeout() {
    let now = Timestamp::from_unix_nanos(1_000_000);
    let mut output = super::RelayProcessorOutputNode {
        relay: identifier("notifications"),
        construction: nervix_models::RouteConstruction::default(),
        branch: None,
        flush_policy: Some(super::RuntimeFlushPolicy::Immediate),
        message_error_policy: MessageErrorPolicy::Log,
        pending: Vec::new(),
        next_flush: None,
        compiled_program: None,
        compiled_branch_program: None,
    };

    assert_eq!(output.schedule_input_flush(now, u64::MAX), Some(false));
    assert_eq!(
        output.next_flush,
        Some(super::checked_add_duration_to_timestamp(
            now,
            Duration::from_micros(100)
        ))
    );
    assert!(
        !output.flush_deadline_due(super::checked_add_duration_to_timestamp(
            now,
            Duration::from_micros(99)
        ))
    );
    assert!(
        output.flush_deadline_due(super::checked_add_duration_to_timestamp(
            now,
            Duration::from_micros(100)
        ))
    );
}

#[test]
fn relay_record_batches_can_be_concatenated_without_losing_metadata() {
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let left = super::RelayRecordBatch::single(
        schema.clone(),
        u32_branch_key("user_id", 42),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(42))])
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(100)),
        AckSet::empty(),
    )
    .expect("left batch should build");
    let right = super::RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 42),
        test_runtime_row([("user_id".to_string(), RuntimeValue::U32(43))])
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(200)),
        AckSet::empty(),
    )
    .expect("right batch should build");

    let concatenated =
        super::RelayRecordBatch::concat(vec![left, right]).expect("batches should concat");

    assert_eq!(concatenated.batch.batch().num_rows(), 2);
    let messages = concatenated
        .try_into_messages()
        .expect("concatenated batch should decode");
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages[0].record.metadata().ingested_at_low_watermark(),
        Timestamp::from_unix_nanos(100)
    );
    assert_eq!(
        messages[1].record.metadata().ingested_at_low_watermark(),
        Timestamp::from_unix_nanos(200)
    );
}

#[tokio::test]
async fn reorderer_buffer_applies_one_columnar_permutation_to_batches_and_sidecars() {
    let schema = test_schema(&[("sequence", ParseAsType::U32)]);
    let batch = |rows: Vec<(u32, i64, AckSet)>| {
        super::RelayRecordBatch::from_messages(
            schema.clone(),
            rows.into_iter()
                .map(|(sequence, ingested_at, acks)| RelayMessage {
                    key: None,
                    record: test_runtime_row([(
                        "sequence".to_string(),
                        RuntimeValue::U32(sequence),
                    )])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(ingested_at)),
                    acks,
                })
                .collect(),
        )
        .expect("test relay batch should build")
    };
    let (first_ordered_acks, first_ordered_completion) = AckSet::root();
    let mut buffer = super::ReordererOutputBuffer::default();
    buffer.push(
        batch(vec![
            (3, 300, AckSet::empty()),
            (1, 100, first_ordered_acks),
        ]),
        Arc::new(vec![
            super::ReordererRowOrder {
                key: vec![super::ReorderKeyPart::UInt64(3)],
                arrival_sequence: 0,
            },
            super::ReordererRowOrder {
                key: vec![super::ReorderKeyPart::UInt64(1)],
                arrival_sequence: 1,
            },
        ]),
        Timestamp::from_unix_nanos(10),
    );
    buffer.push(
        batch(vec![(2, 200, AckSet::empty()), (1, 101, AckSet::empty())]),
        Arc::new(vec![
            super::ReordererRowOrder {
                key: vec![super::ReorderKeyPart::UInt64(2)],
                arrival_sequence: 2,
            },
            super::ReordererRowOrder {
                key: vec![super::ReorderKeyPart::UInt64(1)],
                arrival_sequence: 3,
            },
        ]),
        Timestamp::from_unix_nanos(20),
    );

    let ordered = buffer
        .take_ordered_batch()
        .expect("buffered Arrow batches should reorder");

    let sequences = (0..ordered.batch.batch().num_rows())
        .map(|row| {
            ordered
                .batch
                .value(row, "sequence")
                .expect("sequence should decode")
                .expect("sequence should be initialized")
        })
        .collect::<Vec<_>>();
    let watermarks = ordered
        .metadata
        .iter()
        .map(RuntimeRecordMetadata::ingested_at_low_watermark)
        .collect::<Vec<_>>();

    assert_eq!(
        sequences,
        [
            RuntimeValue::U32(1),
            RuntimeValue::U32(1),
            RuntimeValue::U32(2),
            RuntimeValue::U32(3),
        ]
    );
    assert_eq!(
        watermarks,
        [
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(101),
            Timestamp::from_unix_nanos(200),
            Timestamp::from_unix_nanos(300),
        ]
    );
    assert_eq!(ordered.acks.len(), 4);
    ordered.acks[0].ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), first_ordered_completion.wait())
            .await
            .expect("the first reordered row should retain its source ACK"),
        AckOutcome::Ack
    );
    assert!(buffer.is_empty());
    assert_eq!(buffer.estimated_bytes(), 0);
}

#[test]
fn relay_fanout_shares_arrow_columns_and_exposes_row_views() {
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let batch = super::RelayRecordBatch::from_messages(
        schema,
        vec![
            RelayMessage {
                key: u32_branch_key("user_id", 42),
                record: test_runtime_row([("user_id".to_string(), RuntimeValue::U32(42))]),
                acks: AckSet::empty(),
            },
            RelayMessage {
                key: u32_branch_key("user_id", 42),
                record: test_runtime_row([("user_id".to_string(), RuntimeValue::U32(43))]),
                acks: AckSet::empty(),
            },
        ],
    )
    .expect("relay batch should build");
    let source_column = batch.batch.batch().column(0).clone();

    let fanout = batch.into_attached_fanout(3);

    assert_eq!(fanout.len(), 3);
    for output in &fanout {
        assert!(StdArc::ptr_eq(
            &source_column,
            output.batch.batch().column(0)
        ));
    }
    let row = fanout[0]
        .runtime_row(1)
        .expect("a node may address an Arrow row view");
    assert_eq!(row_value(&row, "user_id"), Some(RuntimeValue::U32(43)));
}

#[tokio::test]
async fn owner_ingress_touches_expiring_stream_state() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay_id = Identifier::parse("notifications").expect("valid identifier");
    let expiring_state = runtime.expiring_stream_state(&domain, &relay_id);
    let registry = expiring_state.registry.clone();
    let services = test_relay_boundary_services();
    let (shutdown, _) = watch::channel(false);
    let mut relay_registries = HashMap::default();
    relay_registries.insert(relay_id.clone(), registry);
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let mut relay_schemas = HashMap::default();
    relay_schemas.insert(relay_id.clone(), schema.clone());
    let mut relay_services = HashMap::default();
    relay_services.insert(relay_id.clone(), services.clone());
    runtime.executions.insert(
        domain.clone(),
        super::DomainExecution {
            schedule: DomainSchedule {
                domain: domain.clone(),
                nodes: Vec::new(),
                placement_groups: Vec::new(),
            },
            passive_only: false,
            start_version: 0,
            shutdown,
            graph: StdArc::new(ArcSwapOption::empty()),
            relay_registries,
            relay_schemas,
            relay_services,
            relay_branchings: HashMap::default(),
            relay_branching_schemas: HashMap::default(),
            materialized_stream_specs: HashMap::default(),
            materialized_stream_owner_nodes: HashMap::default(),
            branched_ingestors: HashMap::default(),
            branched_entrypoints: HashMap::default(),
            codecs: HashMap::default(),
            signaling_protocols: HashMap::default(),
            lookups: HashMap::default(),
            udfs: nervix_roto::UdfExecutor::default(),
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
    let batch_ipc = schema
        .batch_from_test_rows([[("user_id".to_string(), RuntimeValue::U32(42))]])
        .expect("batch should build")
        .to_arrow_ipc_bytes()
        .expect("batch ipc should serialize");

    let key = u32_branch_key("user_id", 42);
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay_id,
        expiring_state.registry.clone(),
        services,
        super::RelayRetention::default(),
    );
    runtime
        .handle_remote_stream(RelayPayload {
            kind: RelayPayloadKind::Ingress,
            domain: domain.clone(),
            relay: relay_id.clone(),
            key: BranchKey::to_remote_key(&key),
            batch_ipc,
            metadata: vec![
                test_runtime_row([("user_id".to_string(), RuntimeValue::U32(42))])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(42))
                    .metadata()
                    .to_remote(),
            ],
            acks: vec![None],
            admission: Some(RemoteAckRegistration {
                ack_id: 1,
                reply_node_id: "producer-node".to_string(),
            }),
        })
        .await
        .expect("remote relay payload should dispatch");
    timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::consume_budget().await;
            if runtime
                .describe_local_stream_exists(&domain, &relay_id, &key)
                .expect("stream existence should be queryable")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner should admit and observe the relay branch");
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should drain");
}

#[tokio::test]
async fn relay_owner_enforces_branch_capacity_across_batches() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("orders");
    let registry = super::RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        super::RelayRetention {
            branch_ttl: None,
            branch_capacity: Some(2),
        },
    );
    let schema = test_schema(&[]);
    let keys = [
        string_branch_key("tenant", "acme"),
        string_branch_key("tenant", "globex"),
        string_branch_key("tenant", "initech"),
    ];
    for key in &keys {
        tokio::task::consume_budget().await;
        let batch = super::RelayRecordBatch::single(
            schema.clone(),
            key.clone(),
            test_runtime_row([]),
            AckSet::empty(),
        )
        .expect("relay batch should build");
        services
            .enqueue_owner_batch(&runtime.metrics, &domain, &relay, None, &batch)
            .await
            .expect("owner should admit the batch");
    }

    timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::consume_budget().await;
            if !registry.contains_key(&keys[0])
                && registry.contains_key(&keys[1])
                && registry.contains_key(&keys[2])
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("relay owner should evict the least-recently-used branch");
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn relay_owner_expires_branch_presence_by_ttl() {
    let runtime = super::Runtime::with_persistence(
        None,
        Duration::from_secs(60),
        super::RuntimeTestHooks {
            branch_instance_expiration_scan_interval: Some(Duration::from_millis(5)),
            ..Default::default()
        },
    )
    .expect("runtime should build");
    let domain = domain("default");
    let relay = identifier("orders");
    let key = string_branch_key("tenant", "acme");
    let registry = super::RelayRegistry::new();
    let services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        registry.clone(),
        services.clone(),
        super::RelayRetention {
            branch_ttl: Some(Duration::from_millis(20)),
            branch_capacity: None,
        },
    );
    let batch = super::RelayRecordBatch::single(
        test_schema(&[]),
        key.clone(),
        test_runtime_row([]),
        AckSet::empty(),
    )
    .expect("relay batch should build");
    services
        .enqueue_owner_batch(&runtime.metrics, &domain, &relay, None, &batch)
        .await
        .expect("owner should admit the batch");

    timeout(Duration::from_secs(1), async {
        while !registry.contains_key(&key) {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("relay owner should observe branch presence");
    timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::consume_budget().await;
            if !registry.contains_key(&key) {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("relay owner should expire idle branch presence");
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn stop_domain_execution_preserves_expiring_relay_branch_registry() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("notifications");
    let branch = string_branch_key("tenant", "acme");
    let expiring_state = runtime.expiring_stream_state(&domain, &relay);
    expiring_state.touch(&branch, Timestamp::from_unix_nanos(1));
    let (shutdown, _) = watch::channel(false);

    runtime
        .stop_domain_execution(
            &domain,
            super::DomainExecution {
                schedule: DomainSchedule {
                    domain: domain.clone(),
                    nodes: Vec::new(),
                    placement_groups: Vec::new(),
                },
                passive_only: false,
                start_version: 0,
                shutdown,
                graph: StdArc::new(ArcSwapOption::empty()),
                relay_registries: HashMap::default(),
                relay_schemas: HashMap::default(),
                relay_services: HashMap::default(),
                relay_branchings: HashMap::default(),
                relay_branching_schemas: HashMap::default(),
                materialized_stream_specs: HashMap::default(),
                materialized_stream_owner_nodes: HashMap::default(),
                branched_ingestors: HashMap::default(),
                branched_entrypoints: HashMap::default(),
                codecs: HashMap::default(),
                signaling_protocols: HashMap::default(),
                lookups: HashMap::default(),
                udfs: nervix_roto::UdfExecutor::default(),
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
        )
        .await;

    assert!(expiring_state.contains_key(&branch));
}

#[tokio::test]
async fn relay_state_shutdown_drains_every_ready_batch() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("materialized_orders");
    let schema = test_schema(&[("value", ParseAsType::I64)]);
    let state = runtime
        .replicated_materialized_stream_state(
            RuntimeStatePlacement {
                domain: domain.clone(),
                state: RuntimeStateKind::MaterializedRelay,
                kind: ModelKind::Relay,
                identifier: relay.clone(),
                schema_fingerprint: [0; 32],
                branch_key: None,
            },
            schema.arrow_schema(),
            None,
        )
        .expect("materialized state should initialize");
    let broadcast = super::RelayBroadcast::with_capacity(nonzero_capacity(2));
    let receiver = super::RelayRuntimeFanIn::new(broadcast.new_receiver());
    let task = runtime.spawn_relay_state_task(
        &domain,
        super::RelayStateTaskSpec {
            relay: relay.clone(),
            state: state.clone(),
            retention: super::RelayRetention::default(),
            receiver,
        },
    );
    let acme = string_branch_key("tenant", "acme");
    let beta = string_branch_key("tenant", "beta");
    for (key, value) in [(acme.clone(), 1), (beta.clone(), 2)] {
        tokio::task::consume_budget().await;
        broadcast
            .broadcast(
                super::RelayRecordBatch::single(
                    schema.clone(),
                    key,
                    test_runtime_row([("value".to_string(), RuntimeValue::I64(value))])
                        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(value)),
                    AckSet::empty(),
                )
                .expect("materialized batch should build"),
            )
            .await
            .expect("materialized batch should queue");
    }

    task.stop(Duration::from_secs(1))
        .await
        .expect("relay state task should drain before the shutdown deadline");

    assert!(state.entries.contains_key(&acme));
    assert!(state.entries.contains_key(&beta));
    assert_eq!(
        runtime
            .node_quiesce_counters(&domain, &relay)
            .outstanding_work(),
        0
    );
}

#[test]
fn lookup_queries_surface_recorded_domain_instantiation_errors() {
    let runtime = super::Runtime::new();
    runtime.domain_instantiation_errors.insert(
        domain("default"),
        "failed to build domain execution for 'default': lookup load failed".to_string(),
    );

    let error = runtime
        .query_local_lookup(&domain("default"), &identifier("zip_codes"), "99926")
        .expect_err("lookup should surface stored instantiation errors");

    assert!(error.contains("failed to build domain execution for 'default'"));
    assert!(error.contains("lookup load failed"));
}

#[tokio::test]
async fn describe_ingestor_surfaces_instantiation_error_when_runtime_is_missing() {
    let runtime = super::Runtime::new();
    let domain = domain("default");
    let ingestor = identifier("mqtt_notifications");
    runtime.domain_instantiation_errors.insert(
        domain.clone(),
        "failed to build domain execution for 'default': ingestor start failed".to_string(),
    );
    let (shutdown, _) = watch::channel(false);
    runtime.executions.insert(
        domain.clone(),
        super::DomainExecution {
            schedule: DomainSchedule {
                domain: domain.clone(),
                nodes: Vec::new(),
                placement_groups: Vec::new(),
            },
            passive_only: false,
            start_version: 0,
            shutdown,
            graph: StdArc::new(ArcSwapOption::empty()),
            relay_registries: HashMap::default(),
            relay_schemas: HashMap::default(),
            relay_services: HashMap::default(),
            relay_branchings: HashMap::default(),
            relay_branching_schemas: HashMap::default(),
            materialized_stream_specs: HashMap::default(),
            materialized_stream_owner_nodes: HashMap::default(),
            branched_ingestors: HashMap::default(),
            branched_entrypoints: HashMap::default(),
            codecs: HashMap::default(),
            signaling_protocols: HashMap::default(),
            lookups: HashMap::default(),
            udfs: nervix_roto::UdfExecutor::default(),
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

    let describe = runtime
        .describe_local_ingestor(&domain, &ingestor)
        .expect("describe should succeed");

    assert!(!describe.running);
    assert!(
        describe
            .transient_error
            .as_deref()
            .is_some_and(|error| error.contains("ingestor start failed")),
        "describe should expose domain instantiation error, got {:?}",
        describe.transient_error
    );
}

#[test]
fn runtime_uses_configured_timestamp_field_when_present() {
    let runtime = super::Runtime::new();
    let record = test_runtime_row([(
        "occurred_at".to_string(),
        RuntimeValue::Datetime(
            chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z").expect("valid timestamp"),
        ),
    )])
    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(1));

    let timestamp = runtime
        .resolve_ingested_record_timestamp(
            &domain("paced"),
            &identifier("ing"),
            Some(&IngestTimestampSource::At(identifier("occurred_at"))),
            &record,
        )
        .expect("timestamp should resolve");

    assert_eq!(
        timestamp,
        Timestamp::from(
            chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z")
                .expect("valid timestamp")
                .to_utc()
        )
    );
}

#[test]
fn runtime_uses_ingested_watermark_for_timestamp_now() {
    let runtime = super::Runtime::new();
    let record =
        test_runtime_row([]).with_ingested_at_watermarks(Timestamp::from_unix_nanos(9_876_543));

    let timestamp = runtime
        .resolve_ingested_record_timestamp(
            &domain("paced"),
            &identifier("ing"),
            Some(&IngestTimestampSource::Now),
            &record,
        )
        .expect("timestamp should resolve");

    assert_eq!(timestamp, Timestamp::from_unix_nanos(9_876_543));
}

#[test]
fn paced_domain_requires_explicit_timestamp_source() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(domain("paced"), paced_domain_state("paced"));
    runtime.sync_domains(&domains);

    let error = runtime
        .resolve_ingested_record_timestamp(
            &domain("paced"),
            &identifier("ing"),
            None,
            &test_runtime_row([]).with_ingested_at_watermarks(Timestamp::from_unix_nanos(1)),
        )
        .expect_err("paced domain should require explicit timestamp source");

    assert!(error.contains("TIMESTAMP NOW or TIMESTAMP AT <field>"));
}

#[test]
fn paced_domains_accept_records_inside_tick_window() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(domain("paced"), paced_domain_state("paced"));
    runtime.sync_domains(&domains);
    runtime.handle_domain_tick(
        &domain("paced"),
        &DomainTick {
            tick_id: 1,
            logical_timestamp: Timestamp::from_unix_nanos(0),
            wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
            duration_ms: 1_000,
        },
    );

    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_200_000_000),
            )
            .is_ok()
    );
    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_400_000_000),
            )
            .is_err()
    );
}

#[test]
fn paced_domains_accept_records_while_clock_is_running_before_ticks_arrive() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(domain("paced"), paced_domain_state("paced"));
    runtime.sync_domains(&domains);
    runtime.handle_domain_clock_start(
        &domain("paced"),
        Timestamp::from_unix_nanos(10_000_000_000),
        Timestamp::from_unix_nanos(10_000_000_000),
        "1.0",
    );

    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_200_000_000),
            )
            .is_ok()
    );
    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(11_200_000_000),
            )
            .is_ok()
    );
}

#[test]
fn sync_domains_clears_ticks_when_paced_domain_stops() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(domain("paced"), paced_domain_state("paced"));
    runtime.sync_domains(&domains);
    runtime.handle_domain_tick(
        &domain("paced"),
        &DomainTick {
            tick_id: 1,
            logical_timestamp: Timestamp::from_unix_nanos(0),
            wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
            duration_ms: 1_000,
        },
    );

    domains.insert(
        domain("paced"),
        DomainState {
            id: domain("paced"),
            config: DomainConfig {
                pace: DomainPace::Paced,
                period: "1s".to_string(),
                skew: "250ms".to_string(),
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
    );
    runtime.sync_domains(&domains);

    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_000_000),
            )
            .is_err()
    );
}

#[test]
fn sync_domains_preserves_clock_state_but_rejects_ingestion_while_paused() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(domain("paced"), paced_domain_state("paced"));
    runtime.sync_domains(&domains);
    runtime.handle_domain_tick(
        &domain("paced"),
        &DomainTick {
            tick_id: 1,
            logical_timestamp: Timestamp::from_unix_nanos(0),
            wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
            duration_ms: 1_000,
        },
    );

    let mut paused = paced_domain_state("paced");
    paused.status = DomainStatus::Paused;
    domains.insert(domain("paced"), paused);
    runtime.sync_domains(&domains);

    assert_eq!(
        runtime
            .domains
            .get(&domain("paced"))
            .expect("domain should remain")
            .ticks
            .lock()
            .len(),
        1
    );
    let error = runtime
        .ensure_domain_allows_ingestion(
            &domain("paced"),
            &identifier("ing"),
            Timestamp::from_unix_nanos(10_000_000_000),
        )
        .expect_err("paused domain must reject ingestion");
    assert!(error.contains("paused"));
}

#[test]
fn stopped_unpaced_domain_rejects_ingestion() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(
        domain("default"),
        DomainState {
            id: domain("default"),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0ms".to_string(),
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: nervix_models::DomainStartPoint::Resume,
            clock: None,
        },
    );
    runtime.sync_domains(&domains);

    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("default"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_000_000),
            )
            .is_err()
    );
}

#[tokio::test]
async fn direct_fanout_owner_buffer_uses_configured_capacity() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("orders");
    let schema = test_schema(&[]);
    let fanout = runtime
        .relay_boundary_fanout_with_capacity(&domain, &relay, false, nonzero_capacity(1))
        .await;
    let mut receiver = fanout.activate_owner_buffer();
    let owner_buffer = fanout.owner_buffer().expect("owner buffer must be active");

    owner_buffer
        .broadcast(
            super::RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("branch", "first"),
                test_runtime_row([]),
                AckSet::empty(),
            )
            .expect("first batch should build"),
        )
        .await
        .expect("first send should succeed");

    let pending_send = tokio::spawn({
        let owner_buffer = owner_buffer.clone();
        async move {
            owner_buffer
                .broadcast(
                    super::RelayRecordBatch::single(
                        schema,
                        string_branch_key("branch", "second"),
                        test_runtime_row([]),
                        AckSet::empty(),
                    )
                    .expect("second batch should build"),
                )
                .await
        }
    });

    sleep(Duration::from_millis(50)).await;
    assert!(
        !pending_send.is_finished(),
        "second send must wait for receiver capacity"
    );

    let first = receiver
        .recv()
        .await
        .expect("receiver should get first batch");
    assert_eq!(key_label(&first.key), r#"{"branch":"first"}"#);

    pending_send
        .await
        .expect("pending send should join")
        .expect("second send should succeed");

    let second = receiver
        .recv()
        .await
        .expect("receiver should get second batch");
    assert_eq!(key_label(&second.key), r#"{"branch":"second"}"#);
}

#[tokio::test]
async fn relay_boundary_fanout_resize_preserves_existing_owner_receiver() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("orders");
    let schema = test_schema(&[]);
    let fanout = runtime
        .relay_boundary_fanout_with_capacity(&domain, &relay, false, nonzero_capacity(1))
        .await;
    let mut receiver = fanout.activate_owner_buffer();
    let resized = runtime
        .relay_boundary_fanout_with_capacity(&domain, &relay, false, nonzero_capacity(5))
        .await;

    let broadcast = match (&fanout, &resized) {
        (
            super::RelayBoundaryFanout::Direct(original),
            super::RelayBoundaryFanout::Direct(resized_fanout),
        ) => {
            assert!(Arc::ptr_eq(original, resized_fanout));
            assert_eq!(resized_fanout.owner_buffer_len(), Some((0, 5)));
            assert_eq!(resized_fanout.subscriptions.capacity(), 1);
            assert_eq!(resized_fanout.attached_runtime_consumers.capacity(), 1);
            assert_eq!(resized_fanout.detached_runtime_consumers.capacity(), 1);
            resized_fanout
                .owner_buffer()
                .expect("owner buffer must remain active")
        }
        _ => panic!("unbranched relay must use direct fanout"),
    };

    broadcast
        .broadcast(
            super::RelayRecordBatch::single(
                schema,
                string_branch_key("branch", "after_resize"),
                test_runtime_row([]),
                AckSet::empty(),
            )
            .expect("batch should build"),
        )
        .await
        .expect("send after resize should succeed");

    let batch = receiver
        .recv()
        .await
        .expect("existing receiver should get batch after resize");
    assert_eq!(key_label(&batch.key), r#"{"branch":"after_resize"}"#);
}

#[tokio::test]
async fn message_error_set_uses_vm_functions_and_captured_snapshots() {
    let source = test_runtime_row([("input_id".to_string(), RuntimeValue::U32(7))]);
    let message = RelayMessage {
        key: None,
        record: source,
        acks: AckSet::empty(),
    };
    let partial_output =
        test_runtime_row([("total".to_string(), RuntimeValue::I64(41))]).one_row_batch();
    let materialized_state = HashMap::from_iter([(
        "relay_state.profiles.plan".to_string(),
        RuntimeValue::String("pro".to_string()),
    )]);
    let reference = uuid::Uuid::now_v7();
    let occurred_at = Timestamp::now();
    let error = StructuredMessageError {
        reference,
        code: MessageErrorCode::Evaluation,
        message: "division failed".to_string(),
        operation: MessageErrorOperation::Set,
        operation_index: Some(2),
        fields: SortedSet::from_unsorted(vec![
            FieldPath::new("input.denominator"),
            FieldPath::new("output.total"),
        ]),
        occurred_at,
    };
    let input_schema = test_schema(&[("input_id", ParseAsType::U32)]);
    let partial_schema = test_schema(&[("total", ParseAsType::I64)]);
    let state_schema = test_schema(&[("plan", ParseAsType::String)]);
    let output_schema = test_optional_schema(&[
        ("input_id", ParseAsType::U32, false),
        ("message_digest", ParseAsType::String, false),
        ("attempted", ParseAsType::I64, true),
        ("plan", ParseAsType::String, false),
        ("operation", ParseAsType::String, false),
        ("operation_index", ParseAsType::U32, true),
    ]);
    let materialized_specs = HashMap::from_iter([(
        identifier("profiles"),
        super::RuntimeMaterializedRelaySpec::new(
            state_schema.arrow_schema(),
            super::VmSchemaSensitivity::default(),
            Vec::new(),
        ),
    )]);
    let assignments = construction(
        "SET input_id = input.input_id, message_digest = md5(error.message), attempted = \
         partial_output.total, plan = relay_state.profiles.plan, operation = error.operation, \
         operation_index = error.operation_index",
    )
    .assignments;
    let program = super::compile_message_error_set_program(
        &domain("default"),
        &identifier("calculate"),
        &assignments,
        output_schema,
        super::MessageErrorCompileSchemas {
            input: Some(input_schema),
            left: None,
            right: None,
            partial_output: Some(partial_schema),
            current_branching: Vec::new(),
            allow_header_reads: false,
        },
        super::RuntimeVmCompileContext {
            available_materialized_streams: &materialized_specs,
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("message-error SET should compile through the VM");
    let output = super::Runtime::execute_message_error_set_program(
        &program,
        &message,
        &error,
        Some(&partial_output),
        &materialized_state,
        None,
        occurred_at,
    )
    .await
    .expect("message-error SET should execute through the VM");

    assert_eq!(row_value(&output, "input_id"), Some(RuntimeValue::U32(7)));
    assert_eq!(row_value(&output, "attempted"), Some(RuntimeValue::I64(41)));
    assert_eq!(
        row_value(&output, "plan"),
        Some(RuntimeValue::String("pro".to_string()))
    );
    assert_eq!(
        row_value(&output, "operation"),
        Some(RuntimeValue::String("set".to_string()))
    );
    assert_eq!(
        row_value(&output, "operation_index"),
        Some(RuntimeValue::U32(2))
    );
    let Some(RuntimeValue::String(digest)) = row_value(&output, "message_digest") else {
        panic!("message digest should be a string");
    };
    assert_eq!(digest.len(), 32);
}

#[test]
fn message_error_routes_preserve_branch_identity_without_reconstruction() {
    let incoming = string_branch_key("tenant", "acme");
    let relay = identifier("processing_errors");
    let reference = uuid::Uuid::now_v7();

    assert_eq!(
        super::preserved_message_error_branch(
            &[identifier("tenant")],
            &incoming,
            &relay,
            reference,
        )
        .expect("matching branched error route should preserve its key"),
        incoming
    );
    assert!(
        super::preserved_message_error_branch(&[], &incoming, &relay, reference)
            .expect_err("unbranched error relay must reject a branch")
            .contains("cannot receive branched message error")
    );
    assert!(
        super::preserved_message_error_branch(&[identifier("tenant")], &None, &relay, reference,)
            .expect_err("branched error relay must reject unbranched execution")
            .contains("cannot receive unbranched message error")
    );
}

#[test]
fn correlator_runtime_rows_use_only_left_and_right_scopes() {
    let left = test_runtime_row([
        ("id".to_string(), RuntimeValue::U32(1)),
        (
            "relay_state.profiles.status".to_string(),
            RuntimeValue::String("active".to_string()),
        ),
    ]);
    let right = test_runtime_row([("id".to_string(), RuntimeValue::U32(2))]);
    let left = RuntimeRecordBatch::from_rows(left.batch().schema(), std::iter::once(&left))
        .expect("left batch should build");
    let right = RuntimeRecordBatch::from_rows(right.batch().schema(), std::iter::once(&right))
        .expect("right batch should build");
    let materialized_state = [super::CorrelatorMaterializedState {
        left: Arc::new(HashMap::default()),
        right: Arc::new(HashMap::default()),
    }];
    let combined = super::correlator_input_batch(&left, &right, &[], &materialized_state)
        .and_then(|batch| batch.runtime_row(0, RuntimeRecordMetadata::test()))
        .expect("correlator inputs should form one Arrow row");

    assert_eq!(row_value(&combined, "left.id"), Some(RuntimeValue::U32(1)));
    assert_eq!(row_value(&combined, "right.id"), Some(RuntimeValue::U32(2)));
    assert_eq!(
        row_value(&combined, "left.relay_state.profiles.status"),
        Some(RuntimeValue::String("active".to_string()))
    );
    assert_eq!(row_value(&combined, "relay_state.profiles.status"), None);
    assert_eq!(row_value(&combined, "id"), None);
}

#[test]
fn ingest_group_rows_share_the_group_batch_allocation() {
    let first = test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]);
    let second = test_runtime_row([("value".to_string(), RuntimeValue::I64(2))]);
    let batch =
        RuntimeRecordBatch::from_rows(first.batch().schema(), [&first, &second].into_iter())
            .expect("test rows should form one ingest group");
    let rows = super::IngestGroupRows {
        batch: Arc::new(batch),
        record_metadata: vec![RuntimeRecordMetadata::test(); 2],
        ingest_metadata: ingest_metadata_for_test(
            super::IngestMetadataKind::Headers,
            &[
                super::IngestMetadataRow::Headers {
                    headers: &super::NoIngestHeaders,
                },
                super::IngestMetadataRow::Headers {
                    headers: &super::NoIngestHeaders,
                },
            ],
        ),
        acks: vec![AckSet::empty(), AckSet::empty()],
    };

    let first = rows.row(0).expect("first row should exist");
    let second = rows.row(1).expect("second row should exist");

    assert!(
        Arc::ptr_eq(first.batch(), second.batch()),
        "row views from one ingest group must retain the same batch allocation"
    );
}

#[test]
fn ingest_group_builds_one_metadata_column_set_for_all_of_its_messages() {
    let topic = "metering_events";
    let headers = TestIngestHeaders(&[("route", "primary")]);
    let mut group = super::PendingIngestGroup::new(super::IngestMetadataKind::Kafka, 8);

    super::INGEST_METADATA_BUILDER_SETS_OPENED.with(|count| count.set(0));
    super::INGEST_METADATA_COLUMN_SETS_BUILT.with(|count| count.set(0));

    for offset in 0..3i64 {
        let record = test_runtime_row([("value".to_string(), RuntimeValue::I64(offset))]);
        group
            .append(
                vec![record.one_row_batch()],
                &[super::IngestMetadataRow::Kafka {
                    topic,
                    partition: 1,
                    offset,
                    headers: &headers,
                }],
                vec![AckSet::empty()],
                Timestamp::from_unix_nanos(1),
            )
            .expect("each decoded message must append into the open group");
    }

    assert_eq!(
        super::INGEST_METADATA_BUILDER_SETS_OPENED.with(std::cell::Cell::get),
        1,
        "a group must open exactly one set of metadata builders, not one per message"
    );
    assert_eq!(
        super::INGEST_METADATA_COLUMN_SETS_BUILT.with(std::cell::Cell::get),
        0,
        "an open group must not build metadata columns before it closes"
    );

    let rows = group.into_rows().expect("the group must close");

    assert_eq!(
        super::INGEST_METADATA_COLUMN_SETS_BUILT.with(std::cell::Cell::get),
        1,
        "closing a group must build exactly one metadata column set"
    );
    assert_eq!(rows.len(), 3);
    let offsets = rows
        .metadata_rows()
        .expect("a Kafka group exposes its metadata columns")
        .field_column("offset")
        .expect("metadata projection must succeed")
        .expect("Kafka offset column must exist");
    let offsets = offsets
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("Kafka offsets must remain INT64");
    assert_eq!(offsets.values(), &[0, 1, 2]);
}

#[tokio::test]
async fn correlator_where_matches_pending_candidates_in_one_vm_execution() {
    let left_schema = test_schema(&[("id", ParseAsType::U32), ("marker", ParseAsType::I64)]);
    let right_schema = test_schema(&[("id", ParseAsType::U32)]);
    let processor = identifier("join_profiles");
    let program = super::compile_correlator_where_program(
        &processor,
        &expression("left.id = right.id"),
        &[identifier("left_profiles")],
        left_schema.arrow_schema(),
        &[identifier("right_profiles")],
        right_schema.arrow_schema(),
        None,
    )
    .expect("correlator WHERE should compile");
    let now = super::current_timestamp();
    let pending = |id, marker| super::CorrelatorPendingMessage {
        received_at: now,
        message: RelayMessage {
            key: None,
            record: test_runtime_row([
                ("id".to_string(), RuntimeValue::U32(id)),
                ("marker".to_string(), RuntimeValue::I64(marker)),
            ]),
            acks: AckSet::empty(),
        },
        materialized_state: Arc::new(HashMap::default()),
    };
    let mut state = super::CorrelatorBranchState {
        pending_left: vec![pending(7, 1), pending(8, 2), pending(7, 3)],
        pending_right: Vec::new(),
    };
    let incoming = super::CorrelatorPendingMessage {
        received_at: now,
        message: RelayMessage {
            key: None,
            record: test_runtime_row([("id".to_string(), RuntimeValue::U32(7))]),
            acks: AckSet::empty(),
        },
        materialized_state: Arc::new(HashMap::default()),
    };
    super::CORRELATOR_WHERE_VM_EXECUTIONS.store(0, Ordering::Relaxed);

    let (matched_left, _matched_right) = super::correlate_incoming_message(
        &processor,
        &program,
        super::CorrelatorSide::Right,
        nervix_models::CorrelatorMatchPolicy::Latest,
        &mut state,
        incoming,
        now,
    )
    .await
    .expect("batched WHERE evaluation should succeed")
    .expect("matching candidates should produce a correlation");

    assert_eq!(
        super::CORRELATOR_WHERE_VM_EXECUTIONS.load(Ordering::Relaxed),
        1,
        "all candidate pairs must share one WHERE VM execution"
    );
    assert_eq!(
        row_value(&matched_left.message.record, "marker"),
        Some(RuntimeValue::I64(3))
    );
    assert_eq!(state.pending_left.len(), 1);
    assert_eq!(
        row_value(&state.pending_left[0].message.record, "marker"),
        Some(RuntimeValue::I64(2))
    );

    let incoming_left = pending(7, 4);
    let right_candidate = |id| super::CorrelatorPendingMessage {
        received_at: now,
        message: RelayMessage {
            key: None,
            record: test_runtime_row([("id".to_string(), RuntimeValue::U32(id))]),
            acks: AckSet::empty(),
        },
        materialized_state: Arc::new(HashMap::default()),
    };
    let right_candidates = vec![right_candidate(7), right_candidate(8), right_candidate(7)];
    super::CORRELATOR_WHERE_VM_EXECUTIONS.store(0, Ordering::Relaxed);

    let matching = super::evaluate_correlator_where_matches(
        &processor,
        &program,
        super::CorrelatorSide::Left,
        &incoming_left,
        &right_candidates,
        now,
    )
    .await
    .expect("left-side arrival should evaluate its right-side candidates");

    assert_eq!(matching, vec![true, false, true]);
    assert_eq!(
        super::CORRELATOR_WHERE_VM_EXECUTIONS.load(Ordering::Relaxed),
        1,
        "a left-side arrival must also batch all candidate pairs"
    );
}

#[tokio::test]
async fn correlator_output_evaluates_all_matched_pairs_once_per_route() {
    let left_schema = test_schema(&[("id", ParseAsType::U32)]);
    let right_schema = test_schema(&[("score", ParseAsType::I64)]);
    let output_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("status", ParseAsType::String),
        ("score", ParseAsType::I64),
    ]);
    let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
    let state_schema = test_schema(&[("status", ParseAsType::String)]);
    let branch = identifier("by_tenant");
    let materialized_specs = HashMap::from_iter([(
        identifier("profiles"),
        super::RuntimeMaterializedRelaySpec::new(
            state_schema.arrow_schema(),
            super::VmSchemaSensitivity::default(),
            vec![branch.clone()],
        ),
    )]);
    let program = super::CorrelatorOutputCompileContext {
        processor: &identifier("join_profiles"),
        left_schema: left_schema.arrow_schema(),
        left_sensitivity: super::VmSchemaSensitivity::default(),
        right_schema: right_schema.arrow_schema(),
        right_sensitivity: super::VmSchemaSensitivity::default(),
        output_relay: &identifier("joined_profiles"),
        output_schema: output_schema.arrow_schema(),
        output_sensitivity: super::VmSchemaSensitivity::default(),
        construction: &construction(
            "SET tenant = branch.tenant, status = relay_state.profiles.status, score = \
             right.score WHERE relay_state.profiles.status != \"blocked\"",
        ),
        runtime: super::RuntimeVmCompileContext {
            available_materialized_streams: &materialized_specs,
            available_lookups: &HashMap::default(),
            current_branching: std::slice::from_ref(&branch),
            current_branch_schema: Some(&branch_schema),
            current_branch_sensitivity: None,
            udfs: None,
        },
    }
    .compile()
    .expect("correlator output should compile");
    let left_batch = Arc::new(
        left_schema
            .batch_from_test_rows([
                [("id".to_string(), RuntimeValue::U32(7))],
                [("id".to_string(), RuntimeValue::U32(8))],
            ])
            .expect("left rows should build"),
    );
    let right_batch = Arc::new(
        right_schema
            .batch_from_test_rows([
                [("score".to_string(), RuntimeValue::I64(41))],
                [("score".to_string(), RuntimeValue::I64(42))],
            ])
            .expect("right rows should build"),
    );
    let now = super::current_timestamp();
    let key = string_branch_key("tenant", "acme");
    let correlation = |row, status: &str| {
        let state = Arc::new(HashMap::from_iter([(
            "relay_state.profiles.status".to_string(),
            RuntimeValue::String(status.to_string()),
        )]));
        (
            super::CorrelatorPendingMessage {
                received_at: now,
                message: RelayMessage {
                    key: key.clone(),
                    record: RuntimeRow::new(left_batch.clone(), row, RuntimeRecordMetadata::test())
                        .expect("left row should exist"),
                    acks: AckSet::empty(),
                },
                materialized_state: Arc::new(HashMap::default()),
            },
            super::CorrelatorPendingMessage {
                received_at: now,
                message: RelayMessage {
                    key: key.clone(),
                    record: RuntimeRow::new(
                        right_batch.clone(),
                        row,
                        RuntimeRecordMetadata::test(),
                    )
                    .expect("right row should exist"),
                    acks: AckSet::empty(),
                },
                materialized_state: state,
            },
        )
    };
    let correlations = vec![correlation(0, "active"), correlation(1, "paused")];
    let matched = super::CorrelatorMatchedBatch::from_correlations(&correlations, &[&program])
        .expect("matched pairs should form one Arrow batch");
    super::CORRELATOR_OUTPUT_VM_EXECUTIONS.store(0, Ordering::Relaxed);

    let outcomes = super::evaluate_correlator_output_batch(
        &identifier("join_profiles"),
        &program,
        &matched,
        vec![AckSet::empty(), AckSet::empty()],
        now,
    )
    .await
    .expect("batched correlator output should evaluate");
    let messages = outcomes
        .into_iter()
        .map(|outcome| match outcome {
            Ok(Some(message)) => message,
            Ok(None) => panic!("route WHERE must retain every eligible pair"),
            Err(error) => panic!("correlator output should succeed: {}", error.error.message),
        })
        .collect::<Vec<_>>();

    assert_eq!(
        super::CORRELATOR_OUTPUT_VM_EXECUTIONS.load(Ordering::Relaxed),
        1,
        "all matched pairs for one route must share one output VM execution"
    );
    assert_eq!(messages.len(), 2);
    assert_eq!(
        row_value(&messages[0].record, "status"),
        Some(RuntimeValue::String("active".to_string()))
    );
    assert_eq!(
        row_value(&messages[1].record, "status"),
        Some(RuntimeValue::String("paused".to_string()))
    );
    assert_eq!(
        row_value(&messages[0].record, "score"),
        Some(RuntimeValue::I64(41))
    );
    assert_eq!(
        row_value(&messages[1].record, "score"),
        Some(RuntimeValue::I64(42))
    );
    let output_batch = messages[0].record.batch().clone();
    let relay_batch = super::build_stream_record_batch_preserving_acks(output_schema, messages)
        .expect("batched correlator output should form one relay batch");
    assert!(
        Arc::ptr_eq(&relay_batch.batch, &output_batch),
        "relay batching must preserve the correlator's shared output allocation"
    );
}

#[test]
fn normalize_http_host_strips_port_and_normalizes_case() {
    assert_eq!(
        super::normalize_http_host(" Example.COM:8080 "),
        "example.com"
    );
    assert_eq!(
        super::normalize_http_host("api.example.com"),
        "api.example.com"
    );
}

#[test]
fn next_retry_delay_doubles_and_caps() {
    let policy = super::ParsedRetryPolicy {
        backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(1),
    };

    assert_eq!(
        super::next_retry_delay(policy.backoff, policy),
        Duration::from_millis(200)
    );
    assert_eq!(
        super::next_retry_delay(Duration::from_millis(700), policy),
        Duration::from_secs(1)
    );
}

#[test]
fn parse_mqtt_addr_handles_valid_and_invalid_inputs() {
    assert_eq!(
        super::ingestors::mqtt::MqttIngestor::parse_addr(
            "mqtt://user:pass@broker.example.com:1883/topic"
        )
        .expect("must parse"),
        super::ingestors::mqtt::MqttIngestorAddr {
            host: "broker.example.com".to_string(),
            port: 1883,
            tls: false,
        }
    );
    assert_eq!(
        super::ingestors::mqtt::MqttIngestor::parse_addr("mqtts://broker.example.com:8883")
            .expect("must parse"),
        super::ingestors::mqtt::MqttIngestorAddr {
            host: "broker.example.com".to_string(),
            port: 8883,
            tls: true,
        }
    );
    assert_eq!(
        super::ingestors::mqtt::MqttIngestor::parse_addr("mqtt://[2001:db8::1]:1883/topic")
            .expect("must parse"),
        super::ingestors::mqtt::MqttIngestorAddr {
            host: "2001:db8::1".to_string(),
            port: 1883,
            tls: false,
        }
    );
    assert_eq!(
        super::ingestors::mqtt::MqttIngestor::parse_addr(
            "mqtt://broker.example.com:1883?keep_alive=30"
        )
        .expect("must parse"),
        super::ingestors::mqtt::MqttIngestorAddr {
            host: "broker.example.com".to_string(),
            port: 1883,
            tls: false,
        }
    );
    assert!(
        super::ingestors::mqtt::MqttIngestor::parse_addr("http://broker.example.com:1883").is_err()
    );
    assert!(super::ingestors::mqtt::MqttIngestor::parse_addr("mqtt://broker.example.com").is_err());
    assert!(super::ingestors::mqtt::MqttIngestor::parse_addr("mqtt://:1883").is_err());
}

#[test]
fn url_scheme_detection_uses_url_parser() {
    assert!(
        super::ServiceUrl::new(
            "amqps://guest:guest@[2001:db8::1]:5671/%2f?heartbeat=30",
            "RabbitMQ addr"
        )
        .has_scheme("amqps")
        .expect("must parse")
    );
    assert!(
        super::ServiceUrl::new("rediss://127.0.0.1:6380/?protocol=resp3", "Redis addr")
            .has_scheme("rediss")
            .expect("must parse")
    );
    assert!(
        super::ServiceUrl::new("tls://127.0.0.1:4223?name=nervix", "NATS addr")
            .has_scheme("tls")
            .expect("must parse")
    );
    assert!(
        !super::ServiceUrl::new("amqp://guest:guest@127.0.0.1:5672/%2f", "RabbitMQ addr")
            .has_scheme("amqps")
            .expect("must parse")
    );
    assert_eq!(
        super::ServiceUrl::new("wss://example.com/socket?token=abc", "WebSockets endpoint")
            .scheme()
            .expect("must parse"),
        "wss"
    );
    assert!(
        super::ServiceUrl::new("not a url", "RabbitMQ addr")
            .has_scheme("amqps")
            .is_err()
    );
}

#[test]
fn mqtt_client_builder_uses_configured_or_default_client_id() {
    let client = CreateClientMqtt {
        name: identifier("mqtt_main"),
        mount: None,
        config: vec![nervix_models::ClientConfigEntry {
            key: "addr".to_string(),
            value: "mqtt://broker.example.com:1883".to_string(),
        }],
    };

    super::ingestors::mqtt::MqttIngestor::client_from_client(&client, "default-client")
        .expect("must build client from default id");

    let client_with_id = CreateClientMqtt {
        name: identifier("mqtt_main"),
        mount: None,
        config: vec![
            nervix_models::ClientConfigEntry {
                key: "addr".to_string(),
                value: "mqtt://broker.example.com:1883".to_string(),
            },
            nervix_models::ClientConfigEntry {
                key: "client_id".to_string(),
                value: "explicit-client".to_string(),
            },
        ],
    };

    super::ingestors::mqtt::MqttIngestor::client_from_client(&client_with_id, "default-client")
        .expect("must build client from explicit id");
}

#[test]
fn prometheus_helpers_render_payload_and_validate_inputs() {
    let sample = super::ingestors::prometheus::PrometheusVectorResult {
        metric: BTreeMap::from([("source".to_string(), "local".to_string())]),
        value: (1_735_782_245.25, "12.5".to_string()),
    };

    let timestamp =
        super::ingestors::prometheus::PrometheusIngestor::timestamp_to_rfc3339(sample.value.0)
            .expect("valid ts");
    assert!(timestamp.starts_with("2025-"));

    let payload = super::ingestors::prometheus::PrometheusIngestor::sample_payload(&sample)
        .expect("must render");
    let value: serde_json::Value = serde_json::from_slice(&payload).expect("valid json");
    assert_eq!(value["source"], "local");
    assert_eq!(value["value"], 12.5);
    assert_eq!(value["timestamp"], timestamp);

    let bad_value = super::ingestors::prometheus::PrometheusVectorResult {
        metric: BTreeMap::new(),
        value: (1.0, "NaN".to_string()),
    };
    assert!(super::ingestors::prometheus::PrometheusIngestor::sample_payload(&bad_value).is_err());
    assert!(
        super::ingestors::prometheus::PrometheusIngestor::timestamp_to_rfc3339(f64::INFINITY)
            .is_err()
    );
}

#[test]
fn prometheus_query_url_uses_url_parser_for_path_and_query() {
    let url = super::ingestors::prometheus::PrometheusIngestor::query_url(
        "http://prometheus:9090/base/?stale=true",
        vec![("query".to_string(), "vector(1)".to_string())],
    )
    .expect("must build url");
    assert_eq!(
        url.as_str(),
        "http://prometheus:9090/base/api/v1/query?query=vector%281%29"
    );
}

#[test]
fn runtime_duration_parsers_validate_and_report_context() {
    let domain = domain("default");
    let ingestor = identifier("orders_ingestor");

    assert_eq!(
        super::Runtime::parse_ack_timeout(&domain, &ingestor, "2s").expect("valid timeout"),
        Duration::from_secs(2)
    );
    assert_eq!(
        super::Runtime::parse_duration_setting(&domain, &ingestor, "batch timeout", "250ms")
            .expect("valid duration"),
        Duration::from_millis(250)
    );

    let err = super::Runtime::parse_ack_timeout(&domain, &ingestor, "oops")
        .expect_err("invalid ack timeout");
    assert!(
        matches!(err, super::RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid ack timeout 'oops'"))
    );

    let err = super::Runtime::parse_duration_setting(&domain, &ingestor, "batch timeout", "oops")
        .expect_err("invalid duration");
    assert!(
        matches!(err, super::RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid batch timeout 'oops'"))
    );

    let retry = RetryPolicy {
        backoff: "100ms".to_string(),
        max_backoff: "1s".to_string(),
    };
    let parsed =
        super::Runtime::parse_retry_policy(&domain, &ingestor, &retry).expect("valid retry policy");
    assert_eq!(parsed.backoff, Duration::from_millis(100));
    assert_eq!(parsed.max_backoff, Duration::from_secs(1));

    let bad_retry = RetryPolicy {
        backoff: "oops".to_string(),
        max_backoff: "1s".to_string(),
    };
    let err = super::Runtime::parse_retry_policy(&domain, &ingestor, &bad_retry)
        .expect_err("invalid retry backoff");
    assert!(
        matches!(err, super::RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid retry backoff 'oops'"))
    );

    let bad_max_retry = RetryPolicy {
        backoff: "100ms".to_string(),
        max_backoff: "oops".to_string(),
    };
    let err = super::Runtime::parse_retry_policy(&domain, &ingestor, &bad_max_retry)
        .expect_err("invalid retry max_backoff");
    assert!(
        matches!(err, super::RuntimeError::StartIngestor { reason, .. } if reason.contains("retry max backoff") && reason.contains("oops"))
    );
}

#[tokio::test]
async fn client_resource_mounts_expand_into_runtime_paths() {
    let store_root = tempdir().expect("resource store tempdir");
    let source_root = tempdir().expect("resource source tempdir");
    let ca_path = source_root.path().join("ca.pem");
    std::fs::write(&ca_path, "test-ca").expect("ca file should be written");

    let mount_domain = Domain::parse("tenant").expect("valid domain");
    let store = ResourceStore::open(store_root.path()).expect("resource store should open");
    store
        .install_from_directory(
            ResourceId::new(mount_domain.clone(), identifier("dev_tls"), 1),
            source_root.path(),
            "node-1",
            Timestamp::from_unix_nanos(0),
        )
        .await
        .expect("resource version should install");

    let runtime = super::Runtime::new();
    runtime.attach_resources(
        Arc::new(store),
        ResourceVersionStatus {
            next_version_by_resource: SortedVec::from_unsorted(vec![(
                mount_domain.clone(),
                identifier("dev_tls"),
                2,
            )]),
            versions: SortedVec::from_unsorted(vec![ResourceVersion {
                id: ResourceId::new(mount_domain.clone(), identifier("dev_tls"), 1),
                root_checksum: "root".to_string(),
                manifest_checksum: "manifest".to_string(),
                file_count: 1,
                total_bytes: 7,
                created_at: Timestamp::from_unix_nanos(0),
                created_by_node: "node-1".to_string(),
            }]),
            replicas: SortedVec::new(),
        },
    );

    let resolved = runtime
        .resolve_client_config(
            &mount_domain,
            Some(&identifier("dev_tls")),
            &[ClientConfigEntry {
                key: "tls_ca_file".to_string(),
                value: "{{ dev_tls }}/ca.pem".to_string(),
            }],
        )
        .expect("client config should resolve");

    assert!(resolved.mounts.is_some());
    assert_eq!(resolved.entries.len(), 1);
    let mounted_ca = PathBuf::from(&resolved.entries[0].value);
    assert!(mounted_ca.ends_with("ca.pem"));
    assert_eq!(
        std::fs::read_to_string(&mounted_ca).expect("mounted ca should be readable"),
        "test-ca"
    );

    let other_domain = Domain::parse("other").expect("valid domain");
    let error = runtime
        .resolve_client_config(
            &other_domain,
            Some(&identifier("dev_tls")),
            &[ClientConfigEntry {
                key: "tls_ca_file".to_string(),
                value: "{{ dev_tls }}/ca.pem".to_string(),
            }],
        )
        .expect_err("another domain must not see this domain's resource");
    assert!(
        error.contains("has no installed versions in domain 'other'"),
        "unexpected error: {error}"
    );
}

#[test]
fn client_resource_mounts_reject_unknown_placeholders() {
    let runtime = super::Runtime::new();
    let error = runtime
        .resolve_client_config(
            &Domain::parse("tenant").expect("valid domain"),
            None,
            &[ClientConfigEntry {
                key: "tls_ca_file".to_string(),
                value: "{{dev_tls}}/ca.pem".to_string(),
            }],
        )
        .expect_err("unknown placeholder should fail");
    assert!(error.contains("failed to render client config template"));
}

#[test]
fn client_config_instance_placeholder_renders_for_concrete_instance() {
    let runtime = super::Runtime::new();
    let resolved = runtime
        .resolve_client_config_with_instance(
            &Domain::parse("tenant").expect("valid domain"),
            None,
            &[ClientConfigEntry {
                key: "client_id".to_string(),
                value: "mqtt-client-{{instance}}".to_string(),
            }],
            7,
        )
        .expect("instance placeholder should resolve");

    assert_eq!(resolved.entries[0].value, "mqtt-client-7");
}

#[test]
fn client_config_extractors_handle_defaults_and_missing_keys() {
    let zeromq = CreateClientZeroMq {
        name: identifier("zmq"),
        mount: None,
        config: vec![
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "tcp://127.0.0.1:5555".to_string(),
            },
            ClientConfigEntry {
                key: "bind".to_string(),
                value: "TRUE".to_string(),
            },
        ],
    };
    assert_eq!(
        super::ingestors::zeromq::ZeroMqIngestor::addr_from_client(&zeromq).expect("addr"),
        "tcp://127.0.0.1:5555"
    );
    assert!(super::ingestors::zeromq::ZeroMqIngestor::bind_from_client(
        &zeromq
    ));

    let http = CreateClientHttp {
        name: identifier("http"),
        mount: None,
        config: vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value: "https://example.com/api".to_string(),
        }],
    };
    assert_eq!(
        super::ingestors::http::HttpIngestor::endpoint_from_client(&http).expect("endpoint"),
        "https://example.com/api"
    );
    assert_eq!(
        super::ingestors::http::HttpIngestor::method_from_client(&http).expect("default method"),
        reqwest::Method::GET
    );

    let http_post = CreateClientHttp {
        name: identifier("http"),
        mount: None,
        config: vec![
            ClientConfigEntry {
                key: "endpoint".to_string(),
                value: "https://example.com/api".to_string(),
            },
            ClientConfigEntry {
                key: "method".to_string(),
                value: "POST".to_string(),
            },
        ],
    };
    assert_eq!(
        super::ingestors::http::HttpIngestor::method_from_client(&http_post).expect("post method"),
        reqwest::Method::POST
    );
    assert!(
        super::ingestors::http::HttpIngestor::method_from_client(&CreateClientHttp {
            name: identifier("http"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "method".to_string(),
                value: "NOT A METHOD".to_string(),
            }],
        })
        .is_err()
    );

    let websocket = CreateClientWebsockets {
        name: identifier("ws"),
        mount: None,
        signaling_protocol: None,
        config: vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value: "wss://example.com/socket".to_string(),
        }],
    };
    assert_eq!(
        super::ingestors::websockets::WebsocketsIngestor::endpoint_from_client(&websocket)
            .expect("endpoint"),
        "wss://example.com/socket"
    );

    let prometheus = CreateClientPrometheus {
        name: identifier("prom"),
        mount: None,
        config: vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: "http://prometheus:9090".to_string(),
        }],
    };
    assert_eq!(
        super::ingestors::prometheus::PrometheusIngestor::addr_from_client(&prometheus)
            .expect("addr"),
        "http://prometheus:9090"
    );

    let zeromq_default = CreateClientZeroMq {
        name: identifier("zmq"),
        mount: None,
        config: vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: "tcp://127.0.0.1:5555".to_string(),
        }],
    };
    assert!(!super::ingestors::zeromq::ZeroMqIngestor::bind_from_client(
        &zeromq_default
    ));

    assert!(
        super::ingestors::zeromq::ZeroMqIngestor::addr_from_client(&CreateClientZeroMq {
            name: identifier("zmq"),
            mount: None,
            config: vec![],
        })
        .expect_err("missing zeromq addr")
        .contains("missing ZeroMQ client config key 'addr'")
    );
    assert!(
        super::ingestors::http::HttpIngestor::endpoint_from_client(&CreateClientHttp {
            name: identifier("http"),
            mount: None,
            config: vec![],
        })
        .expect_err("missing http endpoint")
        .contains("missing HTTP client config key 'endpoint'")
    );
    assert!(
        super::ingestors::websockets::WebsocketsIngestor::endpoint_from_client(
            &CreateClientWebsockets {
                name: identifier("ws"),
                mount: None,
                signaling_protocol: None,
                config: vec![],
            }
        )
        .expect_err("missing websocket endpoint")
        .contains("missing WebSockets client config key 'endpoint'")
    );
    assert!(
        super::ingestors::prometheus::PrometheusIngestor::addr_from_client(
            &CreateClientPrometheus {
                name: identifier("prom"),
                mount: None,
                config: vec![],
            }
        )
        .expect_err("missing prometheus addr")
        .contains("missing Prometheus client config key 'addr'")
    );
}

#[test]
fn clickhouse_client_config_validates_tls_ca_file() {
    let error = match super::emitters::clickhouse::ClickHouseEmitter::client_from_config(&[
        ClientConfigEntry {
            key: "addr".to_string(),
            value: "https://127.0.0.1:8124".to_string(),
        },
        ClientConfigEntry {
            key: "tls_ca_file".to_string(),
            value: "/tmp/nervix-missing-clickhouse-ca.pem".to_string(),
        },
    ]) {
        Ok(_) => panic!("missing ClickHouse TLS CA should fail"),
        Err(error) => error,
    };
    let error = format!("{error:?}");

    assert!(error.contains("TLS CA certificate"));
}

#[test]
fn pulsar_tls_options_load_certificate_chain_and_flags() {
    let tempdir = tempdir().expect("tempdir should be created");
    let ca_path = tempdir.path().join("ca.pem");
    std::fs::write(&ca_path, "test-ca").expect("ca file should be written");

    let options = super::emitters::pulsar::PulsarEmitter::tls_options_from_config(&[
        ClientConfigEntry {
            key: "tls_ca_file".to_string(),
            value: ca_path.display().to_string(),
        },
        ClientConfigEntry {
            key: "tls_allow_insecure_connection".to_string(),
            value: "true".to_string(),
        },
        ClientConfigEntry {
            key: "tls_hostname_verification_enabled".to_string(),
            value: "false".to_string(),
        },
    ])
    .expect("pulsar tls options should load")
    .expect("tls options should be present");

    assert_eq!(
        options
            .certificate_chain
            .expect("certificate chain should be present"),
        b"test-ca".to_vec()
    );
    assert!(options.allow_insecure_connection);
    assert!(!options.tls_hostname_verification_enabled);
}

#[test]
fn pulsar_tls_options_reject_client_auth_material() {
    let error = super::emitters::pulsar::PulsarEmitter::tls_options_from_config(&[
        ClientConfigEntry {
            key: "tls_cert_file".to_string(),
            value: "/tmp/client.crt".to_string(),
        },
        ClientConfigEntry {
            key: "tls_key_file".to_string(),
            value: "/tmp/client.key".to_string(),
        },
    ])
    .expect_err("pulsar mTLS material should be rejected");
    let error = format!("{error:?}");

    assert!(error.contains("tls_cert_file"));
    assert!(error.contains("tls_key_file"));
}

#[test]
fn pulsar_tls_options_reject_invalid_boolean_values() {
    let error =
        super::emitters::pulsar::PulsarEmitter::tls_options_from_config(&[ClientConfigEntry {
            key: "tls_allow_insecure_connection".to_string(),
            value: "maybe".to_string(),
        }])
        .expect_err("invalid pulsar tls boolean should be rejected");
    let error = format!("{error:?}");

    assert!(error.contains("tls_allow_insecure_connection"));
    assert!(error.contains("maybe"));
}

#[test]
fn http_and_prometheus_clients_validate_timeout_configuration() {
    let client = super::HttpClientConfig::new(
        &[ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "250".to_string(),
        }],
        "HTTP",
    )
    .build();
    assert!(client.is_ok());

    let err = super::HttpClientConfig::new(
        &[ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "oops".to_string(),
        }],
        "HTTP",
    )
    .build()
    .expect_err("invalid timeout");
    assert!(err.contains("invalid HTTP timeout_ms 'oops'"));

    let err = super::ingestors::prometheus::PrometheusIngestor::client_from_client(
        &CreateClientPrometheus {
            name: identifier("prom"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "oops".to_string(),
            }],
        },
    )
    .expect_err("invalid prometheus timeout");
    assert!(err.contains("Prometheus timeout_ms"));
}

#[test]
fn mqtt_client_builder_requires_addr_and_retry_delay_handles_overflow() {
    let err = super::ingestors::mqtt::MqttIngestor::client_from_client(
        &CreateClientMqtt {
            name: identifier("mqtt_main"),
            mount: None,
            config: vec![],
        },
        "default-client",
    )
    .err()
    .expect("missing mqtt addr");
    assert!(err.contains("missing MQTT client config key 'addr'"));

    let policy = super::ParsedRetryPolicy {
        backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(10),
    };
    assert_eq!(
        super::next_retry_delay(Duration::MAX, policy),
        Duration::from_secs(10)
    );
}

#[test]
fn branched_node_specs_capture_downstream_processing_tree() {
    let specs = super::branched_node_specs_from_models(
        [
            branch_model_tuple("tenant", "orders", &["tenant"]),
            branch_model_tuple("tenant", "projected_orders", &["tenant"]),
            (
                ModelKind::Ingestor,
                identifier("orders_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("orders_ingestor"),
                    output_routes: (ProcessorOutputs::single(identifier("orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                        .with_branch(branched_by("orders", &["tenant"])),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_orders"),
                    from: ProcessorInputs::single(identifier("orders"))
                        .with_collect_policy("25ms".to_string(), Some("2MiB".to_string())),
                    output_routes: (ProcessorOutputs::single(identifier("projected_orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_projected_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_projected_orders"),
                    from: ProcessorInputs::single(identifier("projected_orders")),
                    output_routes: (ProcessorOutputs::single(identifier("aggregated_orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("projected_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
            (
                ModelKind::Emitter,
                identifier("orders_emitter"),
                nervix_models::Model::Emitter(CreateEmitter {
                    name: identifier("orders_emitter"),
                    from: ProcessorInputs::single(identifier("aggregated_orders")),
                    encode_using_codec: Some(identifier("orders_codec")),
                    sink: Box::new(EmitSink::ZeroMq {
                        client: identifier("zmq_client"),
                    }),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    error_policies: ErrorPolicies::handled_by_log(),
                    publishing_mode: EmitterPublishingMode::NoAck {
                        retry_policy: RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    construction: nervix_models::RouteConstruction::default(),
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let spec = &specs.entrypoints[0];
    assert_eq!(spec.identifier, identifier("orders_ingestor"));
    assert_eq!(spec.root_relay, identifier("orders"));
    assert_eq!(spec.branch.as_ref(), Some(&identifier("by_orders")));
    assert_eq!(specs.processors.len(), 2);
    let dedup_orders = &specs.processors[0];
    assert_eq!(dedup_orders.spec.processor, identifier("dedup_orders"));
    assert_eq!(dedup_orders.spec.input_relays, vec![identifier("orders")]);
    let collect_policy = dedup_orders
        .spec
        .input_collect_policies
        .get(&identifier("orders"))
        .expect("input collection policy must be planned for its source relay");
    assert_eq!(collect_policy.collect_for, "25ms");
    assert_eq!(collect_policy.max_batch_size.as_deref(), Some("2MiB"));
    assert_eq!(dedup_orders.branch.as_ref(), Some(&identifier("by_orders")));
    assert_eq!(dedup_orders.branch_ttl.as_deref(), Some("5m"));
    assert_eq!(dedup_orders.branch_max_instances, None);
    let BranchedProcessorOperationSpec::Deduplicator { output_routes, .. } =
        &dedup_orders.spec.operation
    else {
        panic!("expected deduplicator output");
    };
    let output = output_routes
        .routes
        .first()
        .expect("deduplicator should have output route");
    assert_eq!(output.relay, identifier("projected_orders"));
    let dedup_projected = &specs.processors[1];
    assert_eq!(
        dedup_projected.spec.processor,
        identifier("dedup_projected_orders")
    );
    assert_eq!(
        dedup_projected.spec.input_relays,
        vec![identifier("projected_orders")]
    );
    assert_eq!(dedup_projected.branch_ttl.as_deref(), Some("5m"));
}

#[test]
fn branched_node_specs_capture_window_processor_as_branch_node() {
    let specs = super::branched_node_specs_from_models(
        [
            branch_model_tuple("host", "metrics", &["host"]),
            branch_model_tuple("host", "metric_summary", &["host"]),
            (
                ModelKind::Ingestor,
                identifier("metrics_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("metrics_ingestor"),
                    output_routes: (ProcessorOutputs::single(identifier("metrics")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                        .with_branch(branched_by("metrics", &["host"])),
                    decode_using_codec: identifier("metrics_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            ),
            (
                ModelKind::WindowProcessor,
                identifier("metric_window"),
                nervix_models::Model::WindowProcessor(CreateWindowProcessor {
                    name: identifier("metric_window"),
                    from: ProcessorInputs::single(identifier("metrics")),
                    output_routes: window_outputs(
                        "metric_summary",
                        "SET count = COUNT(input.latency)",
                    ),
                    branched_by: processor_branched_by("metrics", &["host"]),
                    width: WindowBound {
                        messages: Some(100),
                        duration: None,
                    },
                    step: WindowBound {
                        messages: Some(10),
                        duration: None,
                    },
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_summary"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_summary"),
                    from: ProcessorInputs::single(identifier("metric_summary")),
                    output_routes: (ProcessorOutputs::single(identifier("projected_summary")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("metric_summary", &["host"]),
                    deduplicate_on: vec![expression("input.count")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let spec = &specs.entrypoints[0];
    assert_eq!(spec.root_relay, identifier("metrics"));
    assert_eq!(specs.processors.len(), 2);
    let window = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == identifier("metric_window"))
        .expect("window processor spec must exist");
    let BranchedProcessorOperationSpec::WindowProcessor {
        output_routes,
        width,
        step,
    } = &window.spec.operation
    else {
        panic!("expected window processor branch node");
    };
    let output = output_routes
        .routes
        .first()
        .expect("window processor should have output route");
    assert_eq!(output.relay, identifier("metric_summary"));
    assert_eq!(width.messages, Some(100));
    assert_eq!(step.messages, Some(10));
    assert_eq!(output.construction.assignments.len(), 1);
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == identifier("dedup_summary")
                && node.spec.input_relays == vec![identifier("metric_summary")])
    );
}

#[test]
fn branched_node_specs_capture_inferencer_as_branch_node() {
    let specs = super::branched_node_specs_from_models(
        [
            branch_model_tuple("tenant", "features", &["tenant"]),
            branch_model_tuple("tenant", "scores", &["tenant"]),
            (
                ModelKind::Ingestor,
                identifier("features_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("features_ingestor"),
                    output_routes: (ProcessorOutputs::single(identifier("features")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                        .with_branch(branched_by("features", &["tenant"])),
                    decode_using_codec: identifier("features_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            ),
            (
                ModelKind::Inferencer,
                identifier("score_model"),
                nervix_models::Model::Inferencer(CreateInferencer {
                    name: identifier("score_model"),
                    from: ProcessorInputs::single(identifier("features")),
                    output_routes: (ProcessorOutputs::single(identifier("scores")))
                        .with_flush_policy("IMMEDIATE".to_string(), None),
                    branched_by: processor_branched_by("features", &["tenant"]),
                    resource: identifier("fraud_model"),
                    resource_version: Some(3),
                    file: "models/fraud.onnx".to_string(),
                    inputs: vec![InferencerTensorMapping {
                        tensor: "features".to_string(),
                        schema: inferencer_tensor_schema(2),
                        expression: expression("input.vector"),
                    }],
                    output_schema: vec![InferencerTensorDeclaration {
                        tensor: "score".to_string(),
                        schema: inferencer_tensor_schema(1),
                    }],
                    mode: AckMode::Attached,
                    filter_where: Some(expression("input.active")),
                    materialized_state: Vec::new(),
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_scores"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_scores"),
                    from: ProcessorInputs::single(identifier("scores")),
                    output_routes: (ProcessorOutputs::single(identifier("projected_scores")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("scores", &["tenant"]),
                    deduplicate_on: vec![expression("input.score")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let spec = &specs.entrypoints[0];
    assert_eq!(spec.root_relay, identifier("features"));
    assert_eq!(specs.processors.len(), 2);
    let inferencer = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == identifier("score_model"))
        .expect("inferencer spec must exist");
    let BranchedProcessorOperationSpec::Inferencer {
        output_routes,
        resource,
        resource_version,
        file,
        inputs,
        output_schema,
        ..
    } = &inferencer.spec.operation
    else {
        panic!("expected inferencer branch node");
    };
    let output = output_routes
        .routes
        .first()
        .expect("inferencer should have output route");
    assert_eq!(output.relay, identifier("scores"));
    assert_eq!(resource, &identifier("fraud_model"));
    assert_eq!(*resource_version, Some(3));
    assert_eq!(file, "models/fraud.onnx");
    assert_eq!(inputs.len(), 1);
    assert_eq!(output_schema.len(), 1);
    assert_eq!(output.flush_each.as_deref(), Some("IMMEDIATE"));
    assert_eq!(
        inferencer.spec.filter_where,
        Some(expression("input.active"))
    );
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == identifier("dedup_scores")
                && node.spec.input_relays == vec![identifier("scores")])
    );
}

#[test]
fn branched_node_specs_capture_reingestor_entrypoint_tree() {
    let specs = super::branched_node_specs_from_models(
        [
            branch_model_tuple("tenant", "tenant_orders", &["tenant"]),
            (
                ModelKind::Reingestor,
                identifier("tenant_partition"),
                nervix_models::Model::Reingestor(CreateReingestor {
                    name: identifier("tenant_partition"),
                    from: ProcessorInputs::single(identifier("orders")),
                    output_routes: with_inherit_all(ProcessorOutputs::single(identifier(
                        "tenant_orders",
                    )))
                    .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                    .with_branch(branched_by("tenant_orders", &["tenant"])),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_orders"),
                    from: ProcessorInputs::single(identifier("tenant_orders")),
                    output_routes: (ProcessorOutputs::single(identifier("projected_orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("tenant_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let spec = &specs.entrypoints[0];
    assert_eq!(spec.kind, ModelKind::Reingestor);
    assert_eq!(spec.identifier, identifier("tenant_partition"));
    assert_eq!(spec.root_relay, identifier("tenant_orders"));
    assert_eq!(spec.branch.as_ref(), Some(&identifier("by_tenant_orders")));
    assert_eq!(specs.processors.len(), 1);
    assert_eq!(
        specs.processors[0].spec.processor,
        identifier("dedup_orders")
    );
    assert_eq!(
        specs.processors[0].spec.input_relays,
        vec![identifier("tenant_orders")]
    );
    assert_eq!(
        specs.processors[0].branch.as_ref(),
        Some(&identifier("by_tenant_orders"))
    );
    assert_eq!(specs.processors[0].branch_ttl.as_deref(), Some("5m"));
}

#[test]
fn branched_node_specs_capture_processor_output_route_tree() {
    let specs = super::branched_node_specs_from_models(
        [
            branch_model_tuple("tenant", "orders", &["tenant"]),
            branch_model_tuple("tenant", "urgent_orders", &["tenant"]),
            branch_model_tuple("tenant", "default_orders", &["tenant"]),
            (
                ModelKind::Ingestor,
                identifier("orders_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("orders_ingestor"),
                    output_routes: (ProcessorOutputs::single(identifier("orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                        .with_branch(branched_by("orders", &["tenant"])),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("orders_splitter"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("orders_splitter"),
                    from: ProcessorInputs::single(identifier("orders")),
                    output_routes: (ProcessorOutputs::new(vec![
                        ProcessorOutput {
                            relay: identifier("urgent_orders"),
                            construction: nervix_nspl::parse_route_construction(
                                "WHERE output.urgent",
                            )
                            .expect("route construction must parse"),
                            flush_policy: None,
                            message_error_policy: MessageErrorPolicy::Log,
                            branch: None,
                        },
                        ProcessorOutput {
                            relay: identifier("default_orders"),
                            construction: nervix_models::RouteConstruction::default(),
                            flush_policy: None,
                            message_error_policy: MessageErrorPolicy::Log,
                            branch: None,
                        },
                    ]))
                    .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: Some(expression("input.active")),
                    materialized_state: Vec::new(),
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_urgent"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_urgent"),
                    from: ProcessorInputs::single(identifier("urgent_orders")),
                    output_routes: (ProcessorOutputs::single(identifier("urgent_projected")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("urgent_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_default"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_default"),
                    from: ProcessorInputs::single(identifier("default_orders")),
                    output_routes: (ProcessorOutputs::single(identifier("default_projected")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("default_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    assert_eq!(specs.processors.len(), 3);
    let splitter = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == identifier("orders_splitter"))
        .expect("splitter spec must exist");
    let BranchedProcessorOperationSpec::Deduplicator { output_routes, .. } =
        &splitter.spec.operation
    else {
        panic!("expected deduplicator output routes");
    };
    assert_eq!(splitter.spec.filter_where, Some(expression("input.active")));
    assert_eq!(output_routes.routes.len(), 2);
    assert_eq!(
        output_routes.routes[0].construction.where_clause,
        Some(expression("output.urgent"))
    );
    assert_eq!(output_routes.routes[0].relay, identifier("urgent_orders"));
    assert_eq!(output_routes.routes[1].relay, identifier("default_orders"));
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == identifier("dedup_urgent")
                && node.spec.input_relays == vec![identifier("urgent_orders")])
    );
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == identifier("dedup_default")
                && node.spec.input_relays == vec![identifier("default_orders")])
    );
}

#[test]
fn branched_node_specs_capture_junction_as_single_branch_processor() {
    let specs = super::branched_node_specs_from_models(
        [
            branch_model_tuple("tenant", "left_stream", &["tenant"]),
            branch_model_tuple("tenant", "right_stream", &["tenant"]),
            branch_model_tuple("tenant", "joined_stream", &["tenant"]),
            (
                ModelKind::Ingestor,
                identifier("left_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("left_ingestor"),
                    output_routes: (ProcessorOutputs::single(identifier("left_stream")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                        .with_branch(branched_by("left_stream", &["tenant"])),
                    decode_using_codec: identifier("notification_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ),
            (
                ModelKind::Ingestor,
                identifier("right_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("right_ingestor"),
                    output_routes: (ProcessorOutputs::single(identifier("right_stream")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                        .with_branch(branched_by("right_stream", &["tenant"])),
                    decode_using_codec: identifier("notification_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ),
            (
                ModelKind::Junction,
                identifier("join_streams"),
                nervix_models::Model::Junction(CreateJunction {
                    name: identifier("join_streams"),
                    from: ProcessorInputs::new(
                        vec![identifier("left_stream"), identifier("right_stream")],
                        Vec::new(),
                    ),
                    output_routes: (ProcessorOutputs::single(identifier("joined_stream")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("left_stream", &["tenant"]),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_joined"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_joined"),
                    from: ProcessorInputs::single(identifier("joined_stream")),
                    output_routes: (ProcessorOutputs::single(identifier("projected_joined")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("joined_stream", &["tenant"]),
                    deduplicate_on: vec![expression("input.tenant")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 2);
    assert_eq!(
        specs
            .processors
            .iter()
            .filter(|node| node.spec.processor == identifier("join_streams"))
            .count(),
        1
    );
    let junction = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == identifier("join_streams"))
        .expect("junction spec must exist");
    assert_eq!(
        junction.spec.input_relays,
        vec![identifier("left_stream"), identifier("right_stream")]
    );
    let BranchedProcessorOperationSpec::Junction { output_routes, .. } = &junction.spec.operation
    else {
        panic!("expected junction processor");
    };
    let output = output_routes
        .routes
        .first()
        .expect("junction should have output route");
    assert_eq!(output.relay, identifier("joined_stream"));
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == identifier("dedup_joined"))
    );
}

#[test]
fn branched_node_specs_capture_single_processor_output_route_tree() {
    let specs = super::branched_node_specs_from_models(
        [
            branch_model_tuple("tenant", "orders", &["tenant"]),
            branch_model_tuple("tenant", "projected_orders", &["tenant"]),
            (
                ModelKind::Ingestor,
                identifier("orders_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("orders_ingestor"),
                    output_routes: (ProcessorOutputs::single(identifier("orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                        .with_branch(branched_by("orders", &["tenant"])),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("orders_filter"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("orders_filter"),
                    from: ProcessorInputs::new(
                        vec![identifier("orders")],
                        vec![ProcessorInputWhere {
                            relay: identifier("orders"),
                            where_clause: expression("input.active"),
                        }],
                    ),
                    output_routes: (ProcessorOutputs::single(identifier("projected_orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: Some(expression("input.active")),
                    materialized_state: Vec::new(),
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_projected"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_projected"),
                    from: ProcessorInputs::single(identifier("projected_orders")),
                    output_routes: (ProcessorOutputs::single(identifier("aggregated_orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("projected_orders", &["tenant"]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    let orders_filter = specs
        .processors
        .iter()
        .find(|node| node.spec.processor == identifier("orders_filter"))
        .expect("orders filter spec must exist");
    assert_eq!(
        orders_filter.spec.from_where.get(&identifier("orders")),
        Some(&expression("input.active"))
    );
    let BranchedProcessorOperationSpec::Deduplicator { output_routes, .. } =
        &orders_filter.spec.operation
    else {
        panic!("expected processor output routes");
    };
    assert_eq!(
        orders_filter.spec.filter_where,
        Some(expression("input.active"))
    );
    assert_eq!(output_routes.routes.len(), 1);
    assert_eq!(
        output_routes.routes[0].relay,
        identifier("projected_orders")
    );
    assert!(
        specs
            .processors
            .iter()
            .any(|node| node.spec.processor == identifier("dedup_projected")
                && node.spec.input_relays == vec![identifier("projected_orders")])
    );
}

#[test]
fn branched_node_specs_include_singleton_branch_for_empty_branching() {
    let specs = super::branched_node_specs_from_models(
        [
            (
                ModelKind::Ingestor,
                identifier("orders_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("orders_ingestor"),
                    output_routes: (ProcessorOutputs::single(identifier("orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                        .with_branch(OutputBranch::Unbranched),
                    decode_using_codec: identifier("orders_codec"),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                        quiesce: nervix_models::IngestQuiesceMode::Suspend,
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_orders"),
                    from: ProcessorInputs::single(identifier("orders")),
                    output_routes: (ProcessorOutputs::single(identifier("projected_orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: processor_branched_by("orders", &[]),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    assert_eq!(
        specs.entrypoints[0].identifier,
        identifier("orders_ingestor")
    );
    assert_eq!(specs.entrypoints[0].root_relay, identifier("orders"));
    assert_eq!(specs.entrypoints[0].branch, None);
    assert_eq!(specs.entrypoints[0].branch_ttl, None);
    assert_eq!(specs.processors.len(), 1);
    assert_eq!(
        specs.processors[0].spec.processor,
        identifier("dedup_orders")
    );
    assert_eq!(specs.processors[0].branch_ttl, None);
    assert_eq!(specs.processors[0].branch, None);
    assert_eq!(specs.processors[0].branch_max_instances, None);
}

#[test]
fn branched_processor_specs_do_not_require_an_entrypoint() {
    let specs = super::branched_node_specs_from_models(
        [
            (
                ModelKind::Relay,
                identifier("orders"),
                nervix_models::Model::Relay(CreateRelay {
                    name: identifier("orders"),
                    schema: identifier("order_event"),
                    buffer: 1,
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_orders"),
                    from: ProcessorInputs::single(identifier("orders")),
                    output_routes: (ProcessorOutputs::single(identifier("projected_orders")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    branched_by: BranchSelection::unbranched(),
                    deduplicate_on: vec![expression("input.order_id")],
                    max_time: "10m".to_string(),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert!(specs.entrypoints.is_empty());
    assert_eq!(specs.processors.len(), 1);
    assert_eq!(
        specs.processors[0].spec.processor,
        identifier("dedup_orders")
    );
    assert_eq!(
        specs.processors[0].spec.input_relays,
        vec![identifier("orders")]
    );
    assert_eq!(specs.processors[0].branch_ttl, None);
}

#[test]
fn branched_wasm_processor_specs_preserve_global_error_policy() {
    let specs = super::branched_node_specs_from_models(
        [
            (
                ModelKind::Relay,
                identifier("orders"),
                nervix_models::Model::Relay(CreateRelay {
                    name: identifier("orders"),
                    schema: identifier("order_event"),
                    buffer: 1,
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                }),
            ),
            (
                ModelKind::WasmProcessor,
                identifier("filter_orders"),
                nervix_models::Model::WasmProcessor(CreateWasmProcessor {
                    name: identifier("filter_orders"),
                    from: ProcessorInputs::single(identifier("orders")),
                    output_routes: ProcessorOutputs::single(identifier("filtered_orders")),
                    branched_by: BranchSelection::unbranched(),
                    resource: identifier("filter_resource"),
                    resource_version: None,
                    file: "filter.wasm".to_string(),
                    limits: nervix_models::WasmProcessorLimits {
                        max_fuel: 1_000_000_000,
                        max_memory_bytes: 64 * 1024 * 1024,
                    },
                    global_error_policy: GeneralErrorPolicy::Ignore,
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.processors.len(), 1);
    assert_eq!(
        specs.processors[0].spec.error_policies.general,
        GeneralErrorPolicy::Ignore
    );
    assert_eq!(
        specs.processors[0].spec.error_policies.message,
        MessageErrorPolicy::Log
    );
}

#[test]
fn branched_node_specs_include_reingestor_with_declared_branching() {
    let specs = super::branched_node_specs_from_models(
        [
            branch_model_tuple("tenant", "tenant_notifications", &["tenant"]),
            (
                ModelKind::Reingestor,
                identifier("tenant_partition"),
                nervix_models::Model::Reingestor(CreateReingestor {
                    name: identifier("tenant_partition"),
                    from: ProcessorInputs::single(identifier("notifications")),
                    output_routes: (ProcessorOutputs::single(identifier("tenant_notifications")))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                        .with_branch(branched_by("tenant_notifications", &["tenant"])),
                    mode: AckMode::Attached,
                    filter_where: None,
                    materialized_state: Vec::new(),
                }),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.entrypoints.len(), 1);
    assert_eq!(
        specs.entrypoints[0].identifier,
        identifier("tenant_partition")
    );
    assert_eq!(
        specs.entrypoints[0].root_relay,
        identifier("tenant_notifications")
    );
}

#[tokio::test]
async fn branched_root_without_children_acks_success() {
    let runtime = super::Runtime::default();
    let root_domain = domain("default");
    let root_relay = identifier("tenant_orders");
    let root_registry = super::RelayRegistry::new();
    let root_services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &root_domain,
        &root_relay,
        root_registry.clone(),
        root_services.clone(),
        super::RelayRetention::default(),
    );
    let mut root = super::BranchRuntime {
        key: Some(concrete_branch_key([(
            identifier("tenant"),
            RuntimeValue::String("acme".to_string()),
        )])),
        runtime: runtime.clone(),
        domain: root_domain.clone(),
        source_kind: ModelKind::Ingestor,
        source: identifier("metric_ingestor"),
        root_relay: root_relay.clone(),
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            root_relay.clone(),
            super::ConcreteRelayRuntime::new(super::ConcreteRelayRuntimeBuild {
                runtime,
                domain: root_domain,
                relay: root_relay,
                registry: root_registry,
                services: root_services,
                key: Some(concrete_branch_key([(
                    identifier("tenant"),
                    RuntimeValue::String("acme".to_string()),
                )])),
            }),
        )]
        .into_iter()
        .collect(),
        materialized_states: HashMap::default(),
        relay_state_epoch: None,
        processors: HashMap::default(),
    };
    let graph = StdArc::new(ArcSwapOption::from(None));
    let (acks, completion) = AckSet::root();
    let schema = test_schema(&[("tenant", ParseAsType::String)]);

    root.dispatch(
        &graph,
        super::RelayRecordBatch::single(
            schema,
            string_branch_key("tenant", "acme"),
            test_runtime_row([(
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            )]),
            acks,
        )
        .expect("batch should build"),
    )
    .await;

    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn reingestor_branched_entrypoint_splits_precomputed_keys_with_arrow_filters() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let root_relay = identifier("tenant_orders");
    let fanout = super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(
        TWO_ITEM_TEST_CHANNEL_CAPACITY,
    ));
    let mut fan_in =
        super::RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
    let services = Arc::new(super::RelayBoundaryServices::new(
        fanout,
        1,
        0,
        Vec::new(),
        None,
    ));
    let registry = super::RelayRegistry::new();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &root_relay,
        registry.clone(),
        services.clone(),
        super::RelayRetention::default(),
    );
    let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::U32)]);
    let template = super::BranchInstanceTemplate {
        source_kind: ModelKind::Reingestor,
        source: identifier("tenant_partition"),
        root_relay: root_relay.clone(),
        branch: None,
        branch_ttl: None,
        branch_max_instances: None,
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            root_relay.clone(),
            super::RelayProcessorRelayTemplate {
                registry,
                services: services.clone(),
            },
        )]
        .into_iter()
        .collect(),
        materialized_streams: HashSet::default(),
        processors: HashMap::default(),
    };
    let inputs = [
        super::RelayRecordBatch::single(
            schema.clone(),
            string_branch_key("tenant", "acme"),
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("value".to_string(), RuntimeValue::U32(1)),
            ]),
            AckSet::empty(),
        )
        .expect("acme batch should build"),
        super::RelayRecordBatch::single(
            schema.clone(),
            string_branch_key("tenant", "beta"),
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("beta".to_string()),
                ),
                ("value".to_string(), RuntimeValue::U32(2)),
            ]),
            AckSet::empty(),
        )
        .expect("beta batch should build"),
        super::RelayRecordBatch::single(
            schema.clone(),
            string_branch_key("tenant", "acme"),
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("value".to_string(), RuntimeValue::U32(3)),
            ]),
            AckSet::empty(),
        )
        .expect("second acme batch should build"),
    ];
    let route_runtime = super::IngestorRouteRuntime::new(
        runtime,
        domain,
        identifier("tenant_partition"),
        StdArc::new(ArcSwapOption::from(None)),
        super::IngestorRouteTemplate {
            branch: template,
            ack_boundary: super::BranchInstanceAckBoundary::Reingestor(AckMode::Attached),
            flush_policy: super::RuntimeFlushPolicy::Immediate,
        },
        Duration::from_secs(30),
    );
    let expected_message_count = inputs.len();
    for input in inputs {
        route_runtime
            .sender()
            .send(input)
            .await
            .expect("reingestor route should accept input");
    }

    let outputs = timeout(Duration::from_secs(1), async {
        let mut outputs = Vec::new();
        let mut received_message_count = 0;
        while received_message_count < expected_message_count {
            let output = fan_in
                .recv()
                .await
                .expect("runtime consumer should remain open");
            received_message_count += output.batch.batch().num_rows();
            outputs.push(output);
        }
        outputs
    })
    .await
    .expect("all output rows should arrive");

    let mut output_rows = outputs
        .iter()
        .flat_map(|output| {
            (0..output.batch.batch().num_rows()).map(|row| {
                (
                    key_label(&output.key).to_string(),
                    output
                        .batch
                        .row_to_json_string(row)
                        .expect("output row should serialize"),
                )
            })
        })
        .collect::<Vec<_>>();
    output_rows.sort();
    assert_eq!(
        output_rows,
        vec![
            (
                r#"{"tenant":"acme"}"#.to_string(),
                r#"{"tenant":"acme","value":1}"#.to_string(),
            ),
            (
                r#"{"tenant":"acme"}"#.to_string(),
                r#"{"tenant":"acme","value":3}"#.to_string(),
            ),
            (
                r#"{"tenant":"beta"}"#.to_string(),
                r#"{"tenant":"beta","value":2}"#.to_string(),
            ),
        ]
    );
    route_runtime.shutdown().await;
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn reingestor_branched_entrypoint_reuses_existing_branches() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let root_relay = identifier("tenant_orders");
    let services = Arc::new(super::RelayBoundaryServices::new(
        super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(1)),
        0,
        0,
        Vec::new(),
        None,
    ));
    let registry = super::RelayRegistry::new();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &root_relay,
        registry.clone(),
        services.clone(),
        super::RelayRetention::default(),
    );
    let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::U32)]);
    let template = super::BranchInstanceTemplate {
        source_kind: ModelKind::Reingestor,
        source: identifier("tenant_partition"),
        root_relay: root_relay.clone(),
        branch: None,
        branch_ttl: None,
        branch_max_instances: None,
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            root_relay.clone(),
            super::RelayProcessorRelayTemplate {
                registry,
                services: services.clone(),
            },
        )]
        .into_iter()
        .collect(),
        materialized_streams: HashSet::default(),
        processors: HashMap::default(),
    };
    let graph = StdArc::new(ArcSwapOption::from(None));
    let mut instances =
        BranchInstanceRegistry::<Option<BranchKey>, Mutex<super::BranchRuntime>>::new();
    let (branch_sender, _) = mpsc::channel(1);
    let route_task = super::IngestorRouteTask {
        runtime_handle: runtime.clone(),
        domain: domain.clone(),
        ingestor: identifier("tenant_partition"),
        template: super::IngestorRouteTemplate {
            branch: template.clone(),
            ack_boundary: super::BranchInstanceAckBoundary::Reingestor(AckMode::Detached),
            flush_policy: super::RuntimeFlushPolicy::Immediate,
        },
        branch_sender,
        pending: HashMap::default(),
    };

    for round in 0..3 {
        let mut prepared = Vec::new();
        for index in 0..64 {
            let input = super::RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", &format!("tenant-{index}")),
                test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String(format!("tenant-{index}")),
                    ),
                    ("value".to_string(), RuntimeValue::U32(round * 64 + index)),
                ]),
                AckSet::empty(),
            )
            .expect("single-branch batch should build");
            prepared.extend(route_task.prepare_input(input).await);
        }
        super::BranchExecutionRuntime::dispatch_prepared_inputs(
            super::BranchExecutionDispatchContext {
                runtime_handle: &runtime,
                domain: &domain,
                ingestor: &identifier("tenant_partition"),
                graph: &graph,
                template: &template,
                now: Timestamp::from_unix_nanos(1_000_000_000 + i64::from(round)),
            },
            &mut instances,
            prepared,
        )
        .await;

        assert_eq!(instances.len(), 64);
    }
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn reingestor_propagates_attached_ack_into_branched_entrypoint() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("tenant_orders");
    let output_registry = super::RelayRegistry::new();
    let output_services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &relay,
        output_registry.clone(),
        output_services.clone(),
        super::RelayRetention::default(),
    );
    let mut output_subscription = output_services.subscription_receiver();
    let schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("user_id", ParseAsType::U32),
    ]);
    let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
    let (execution_shutdown, _) = watch::channel(false);
    runtime.executions.insert(
        domain.clone(),
        super::DomainExecution {
            schedule: DomainSchedule {
                domain: domain.clone(),
                nodes: Vec::new(),
                placement_groups: Vec::new(),
            },
            passive_only: false,
            start_version: 0,
            shutdown: execution_shutdown,
            graph: StdArc::new(ArcSwapOption::empty()),
            relay_registries: HashMap::default(),
            relay_schemas: [
                (identifier("orders"), schema.clone()),
                (identifier("tenant_orders"), schema.clone()),
            ]
            .into_iter()
            .collect(),
            relay_services: HashMap::default(),
            relay_branchings: [(relay.clone(), vec![identifier("tenant")])]
                .into_iter()
                .collect(),
            relay_branching_schemas: [(relay.clone(), Some(branch_schema))].into_iter().collect(),
            materialized_stream_specs: HashMap::default(),
            materialized_stream_owner_nodes: HashMap::default(),
            branched_ingestors: HashMap::default(),
            branched_entrypoints: HashMap::default(),
            codecs: HashMap::default(),
            signaling_protocols: HashMap::default(),
            lookups: HashMap::default(),
            udfs: nervix_roto::UdfExecutor::default(),
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
    let branched_runtime = super::IngestorRouteRuntime::new(
        runtime.clone(),
        domain.clone(),
        identifier("tenant_partition"),
        StdArc::new(ArcSwapOption::from(None)),
        super::IngestorRouteTemplate {
            branch: super::BranchInstanceTemplate {
                source_kind: ModelKind::Reingestor,
                source: identifier("tenant_partition"),
                root_relay: relay.clone(),
                branch: None,
                branch_ttl: Some(Duration::from_secs(30)),
                branch_max_instances: None,
                error_policies: ErrorPolicies::handled_by_log(),
                relays: [(
                    relay.clone(),
                    super::RelayProcessorRelayTemplate {
                        registry: output_registry.clone(),
                        services: output_services.clone(),
                    },
                )]
                .into_iter()
                .collect(),
                materialized_streams: HashSet::default(),
                processors: HashMap::default(),
            },
            ack_boundary: super::BranchInstanceAckBoundary::Reingestor(AckMode::Attached),
            flush_policy: super::RuntimeFlushPolicy::Immediate,
        },
        Duration::from_secs(30),
    );
    assert_eq!(
        branched_runtime.sender().max_capacity(),
        super::STUPID_CHANNEL_CAPACITY_REMOVE_ME
    );
    let (shutdown_tx, _) = watch::channel(false);
    let broadcast =
        super::RelayBroadcast::with_capacity(nonzero_capacity(STUPID_CHANNEL_CAPACITY_REMOVE_ME));
    let fan_in = super::RelayRuntimeFanIn::new(broadcast.new_receiver());
    let mut branched_entrypoint_senders = HashMap::default();
    branched_entrypoint_senders.insert(relay, branched_runtime.sender());
    let task = runtime
        .spawn_reingestor_task(
            &domain,
            &shutdown_tx,
            &branched_entrypoint_senders,
            CreateReingestor {
                name: identifier("tenant_partition"),
                from: ProcessorInputs::single(identifier("orders")),
                output_routes: with_inherit_all(ProcessorOutputs::single(identifier(
                    "tenant_orders",
                )))
                .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()))
                .with_branch(branched_by("tenant_orders", &["tenant"])),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            },
            identifier("orders"),
            fan_in,
        )
        .expect("reingestor task should spawn");
    let (acme_acks, acme_completion) = AckSet::root();
    let (beta_acks, beta_completion) = AckSet::root();
    let mut acme_completion = Box::pin(acme_completion.wait());
    let mut beta_completion = Box::pin(beta_completion.wait());

    broadcast
        .broadcast(
            super::RelayRecordBatch::from_messages(
                schema,
                vec![
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([
                            (
                                "tenant".to_string(),
                                RuntimeValue::String("acme".to_string()),
                            ),
                            ("user_id".to_string(), RuntimeValue::U32(42)),
                        ]),
                        acks: acme_acks.attached(),
                    },
                    RelayMessage {
                        key: None,
                        record: test_runtime_row([
                            (
                                "tenant".to_string(),
                                RuntimeValue::String("beta".to_string()),
                            ),
                            ("user_id".to_string(), RuntimeValue::U32(7)),
                        ]),
                        acks: beta_acks.attached(),
                    },
                ],
            )
            .expect("batch should build"),
        )
        .await
        .expect("message should broadcast");
    acme_acks.ack_success();
    beta_acks.ack_success();

    assert!(
        timeout(Duration::from_millis(20), output_subscription.recv())
            .await
            .is_err(),
        "reingestor output must remain buffered until its flush deadline"
    );
    assert!(
        timeout(Duration::from_millis(1), &mut acme_completion)
            .await
            .is_err(),
        "attached acme ACK must remain pending with the buffered output"
    );
    assert!(
        timeout(Duration::from_millis(1), &mut beta_completion)
            .await
            .is_err(),
        "attached beta ACK must remain pending with the buffered output"
    );

    let first_output_batch = timeout(Duration::from_secs(1), output_subscription.recv())
        .await
        .expect("first output subscription should receive")
        .expect("output subscription should stay open");
    let second_output_batch = timeout(Duration::from_secs(1), output_subscription.recv())
        .await
        .expect("second output subscription should receive")
        .expect("output subscription should stay open");
    assert_eq!(
        timeout(Duration::from_secs(1), &mut acme_completion)
            .await
            .expect("acme ack completion should resolve after output dispatch"),
        AckOutcome::Ack
    );
    assert_eq!(
        timeout(Duration::from_secs(1), &mut beta_completion)
            .await
            .expect("beta ack completion should resolve after output dispatch"),
        AckOutcome::Ack
    );
    let tenants = [first_output_batch, second_output_batch]
        .into_iter()
        .flat_map(|batch| {
            batch
                .try_into_messages()
                .expect("output batch should expand")
        })
        .filter_map(|output| match output.record.value("tenant") {
            Ok(Some(RuntimeValue::String(tenant))) => Some(tenant),
            _ => None,
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        tenants,
        ["acme".to_string(), "beta".to_string()]
            .into_iter()
            .collect::<HashSet<_>>()
    );

    let _ = shutdown_tx.send(true);
    let _ = task.await;
    branched_runtime.shutdown().await;
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn reingestor_force_and_shutdown_flush_buffered_routes() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let input_relay = identifier("orders");
    let output_relay = identifier("tenant_orders");
    let reingestor = identifier("tenant_partition");
    let schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("user_id", ParseAsType::U32),
    ]);
    let (execution_shutdown, _) = watch::channel(false);
    runtime.executions.insert(
        domain.clone(),
        super::DomainExecution {
            schedule: DomainSchedule {
                domain: domain.clone(),
                nodes: Vec::new(),
                placement_groups: Vec::new(),
            },
            passive_only: false,
            start_version: 0,
            shutdown: execution_shutdown,
            graph: StdArc::new(ArcSwapOption::empty()),
            relay_registries: HashMap::default(),
            relay_schemas: [
                (input_relay.clone(), schema.clone()),
                (output_relay.clone(), schema.clone()),
            ]
            .into_iter()
            .collect(),
            relay_services: HashMap::default(),
            relay_branchings: HashMap::default(),
            relay_branching_schemas: HashMap::default(),
            materialized_stream_specs: HashMap::default(),
            materialized_stream_owner_nodes: HashMap::default(),
            branched_ingestors: HashMap::default(),
            branched_entrypoints: HashMap::default(),
            codecs: HashMap::default(),
            signaling_protocols: HashMap::default(),
            lookups: HashMap::default(),
            udfs: nervix_roto::UdfExecutor::default(),
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
    let (shutdown_tx, _) = watch::channel(false);
    let broadcast = super::RelayBroadcast::with_capacity(nonzero_capacity(4));
    let fan_in = super::RelayRuntimeFanIn::new(broadcast.new_receiver());
    let (output_tx, mut output_rx) = mpsc::channel(4);
    let task = runtime
        .spawn_reingestor_task(
            &domain,
            &shutdown_tx,
            &[(output_relay.clone(), output_tx)].into_iter().collect(),
            CreateReingestor {
                name: reingestor.clone(),
                from: ProcessorInputs::single(input_relay.clone()),
                output_routes: with_inherit_all(ProcessorOutputs::single(output_relay.clone()))
                    .with_flush_policy("10s".to_string(), Some("1MiB".to_string())),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            },
            input_relay,
            fan_in,
        )
        .expect("reingestor task should spawn");
    let input_batch = |user_id, acks| {
        super::RelayRecordBatch::single(
            schema.clone(),
            None,
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("user_id".to_string(), RuntimeValue::U32(user_id)),
            ]),
            acks,
        )
        .expect("input batch should build")
    };

    let (first_acks, first_completion) = AckSet::root();
    let mut first_completion = Box::pin(first_completion.wait());
    broadcast
        .broadcast(input_batch(1, first_acks.attached()))
        .await
        .expect("first input should broadcast");
    first_acks.ack_success();
    assert!(
        timeout(Duration::from_millis(20), output_rx.recv())
            .await
            .is_err(),
        "long-cadence reingestor output must remain buffered"
    );
    assert!(
        timeout(Duration::from_millis(1), &mut first_completion)
            .await
            .is_err(),
        "force-flush input ACK must remain pending with buffered output"
    );

    runtime.force_flush_domain(&domain);
    let forced = timeout(Duration::from_secs(1), output_rx.recv())
        .await
        .expect("force flush should publish buffered output")
        .expect("reingestor output should remain open");
    assert_eq!(
        row_value(
            &forced
                .runtime_row(0)
                .expect("forced output should contain an Arrow row"),
            "user_id",
        ),
        Some(RuntimeValue::U32(1))
    );
    forced.ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), &mut first_completion)
            .await
            .expect("force-flushed ACK should resolve"),
        AckOutcome::Ack
    );
    timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::consume_budget().await;
            let pending = runtime
                .force_flush_by_domain
                .get(&domain)
                .map(|force_flush| force_flush.pending())
                .unwrap_or_default();
            if pending == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("force-flush generation should complete after output publication");

    let (second_acks, second_completion) = AckSet::root();
    let mut second_completion = Box::pin(second_completion.wait());
    broadcast
        .broadcast(input_batch(2, second_acks.attached()))
        .await
        .expect("second input should broadcast");
    second_acks.ack_success();
    assert!(
        timeout(Duration::from_millis(20), &mut second_completion)
            .await
            .is_err(),
        "shutdown input ACK must remain pending while its output is buffered"
    );
    shutdown_tx
        .send(true)
        .expect("reingestor shutdown receiver should remain open");
    timeout(Duration::from_secs(1), task)
        .await
        .expect("reingestor should stop after draining input")
        .expect("reingestor task should not panic");
    let stopped = timeout(Duration::from_secs(1), output_rx.recv())
        .await
        .expect("shutdown should publish buffered output")
        .expect("reingestor output should remain open");
    assert_eq!(
        row_value(
            &stopped
                .runtime_row(0)
                .expect("shutdown output should contain an Arrow row"),
            "user_id",
        ),
        Some(RuntimeValue::U32(2))
    );
    stopped.ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), &mut second_completion)
            .await
            .expect("shutdown-flushed ACK should resolve"),
        AckOutcome::Ack
    );
    assert_eq!(
        runtime
            .node_quiesce_counters(&domain, &reingestor)
            .output_buffers
            .load(Ordering::Acquire),
        0,
        "reingestor output gauge must be cleared when the task exits"
    );
}

#[tokio::test]
async fn branch_entrypoint_dispatches_an_ingestor_prepared_batch_immediately() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let root_relay = identifier("notifications");
    let fanout = super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(1));
    let mut fan_in =
        super::RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
    let services = Arc::new(super::RelayBoundaryServices::new(
        fanout,
        1,
        0,
        Vec::new(),
        None,
    ));
    let registry = super::RelayRegistry::new();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &root_relay,
        registry.clone(),
        services.clone(),
        super::RelayRetention::default(),
    );
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let branched_runtime = super::BranchExecutionRuntime::new(
        runtime,
        domain,
        identifier("notifications_ingestor"),
        StdArc::new(ArcSwapOption::from(None)),
        super::BranchInstanceTemplate {
            source_kind: ModelKind::Ingestor,
            source: identifier("notifications_ingestor"),
            root_relay: root_relay.clone(),
            branch: None,
            branch_ttl: None,
            branch_max_instances: None,
            error_policies: ErrorPolicies::handled_by_log(),
            relays: [(
                root_relay,
                super::RelayProcessorRelayTemplate {
                    registry,
                    services: services.clone(),
                },
            )]
            .into_iter()
            .collect(),
            materialized_streams: HashSet::default(),
            processors: HashMap::default(),
        },
        Duration::from_secs(30),
    );

    branched_runtime
        .sender()
        .send(
            super::RelayRecordBatch::single(
                schema,
                None,
                test_runtime_row([("user_id".to_string(), RuntimeValue::U32(42))]),
                AckSet::empty(),
            )
            .expect("ingestor output batch should build"),
        )
        .await
        .expect("branch entrypoint should accept an ingestor-prepared batch");

    let batch = timeout(Duration::from_millis(100), fan_in.recv())
        .await
        .expect("branch entrypoint must not apply a second flush delay")
        .expect("runtime consumer should remain open");
    assert_eq!(batch.message_count(), 1);

    branched_runtime.shutdown().await;
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn ingestor_and_reingestor_routes_apply_size_boundaries_independently_per_branch() {
    let cases = [
        (
            ModelKind::Ingestor,
            super::BranchInstanceAckBoundary::Preserve,
            "notifications_ingestor",
        ),
        (
            ModelKind::Reingestor,
            super::BranchInstanceAckBoundary::Reingestor(AckMode::Attached),
            "notifications_reingestor",
        ),
    ];
    for (source_kind, ack_boundary, source) in cases {
        tokio::task::consume_budget().await;
        let runtime = super::Runtime::default();
        let domain = domain("default");
        let root_relay = identifier("notifications");
        let fanout = super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(4));
        let mut fan_in = super::RelayRuntimeFanIn::new(
            fanout.runtime_consumer_receiver_for_mode(AckMode::Attached),
        );
        let services = Arc::new(super::RelayBoundaryServices::new(
            fanout,
            1,
            0,
            Vec::new(),
            None,
        ));
        let registry = super::RelayRegistry::new();
        let owner_task = runtime.spawn_relay_owner_task(
            &domain,
            &root_relay,
            registry.clone(),
            services.clone(),
            super::RelayRetention::default(),
        );
        let schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("user_id", ParseAsType::U32),
        ]);
        let batch = |tenant: &str, user_id| {
            super::RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", tenant),
                test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String(tenant.to_string()),
                    ),
                    ("user_id".to_string(), RuntimeValue::U32(user_id)),
                ]),
                AckSet::empty(),
            )
            .expect("ingestor output batch should build")
        };
        let acme_one = batch("acme", 1);
        let max_batch_size = acme_one.estimated_bytes() + 1;
        let route_runtime = super::IngestorRouteRuntime::new(
            runtime,
            domain,
            identifier(source),
            StdArc::new(ArcSwapOption::from(None)),
            super::IngestorRouteTemplate {
                branch: super::BranchInstanceTemplate {
                    source_kind,
                    source: identifier(source),
                    root_relay: root_relay.clone(),
                    branch: None,
                    branch_ttl: None,
                    branch_max_instances: None,
                    error_policies: ErrorPolicies::handled_by_log(),
                    relays: [(
                        root_relay,
                        super::RelayProcessorRelayTemplate {
                            registry,
                            services: services.clone(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    materialized_streams: HashSet::default(),
                    processors: HashMap::default(),
                },
                ack_boundary,
                flush_policy: super::RuntimeFlushPolicy::Each {
                    interval: Duration::from_secs(10),
                    max_batch_size,
                },
            },
            Duration::from_secs(30),
        );

        route_runtime
            .sender()
            .send(acme_one)
            .await
            .expect("acme batch should enter the route");
        route_runtime
            .sender()
            .send(batch("beta", 1))
            .await
            .expect("beta batch should enter the route");
        assert!(
            timeout(Duration::from_millis(50), fan_in.recv())
                .await
                .is_err(),
            "different branches must not share a size boundary"
        );

        route_runtime
            .sender()
            .send(batch("acme", 2))
            .await
            .expect("second acme batch should enter the route");
        let acme = timeout(Duration::from_secs(1), fan_in.recv())
            .await
            .expect("acme size boundary should flush")
            .expect("runtime consumer should remain open");
        assert_eq!(key_label(&acme.key), r#"{"tenant":"acme"}"#);
        assert_eq!(acme.message_count(), 2);
        assert!(
            timeout(Duration::from_millis(50), fan_in.recv())
                .await
                .is_err(),
            "beta must remain pending until its own size boundary"
        );

        route_runtime
            .sender()
            .send(batch("beta", 2))
            .await
            .expect("second beta batch should enter the route");
        let beta = timeout(Duration::from_secs(1), fan_in.recv())
            .await
            .expect("beta size boundary should flush")
            .expect("runtime consumer should remain open");
        assert_eq!(key_label(&beta.key), r#"{"tenant":"beta"}"#);
        assert_eq!(beta.message_count(), 2);

        route_runtime.shutdown().await;
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }
}

#[test]
fn relay_batch_estimated_bytes_counts_arrow_payload_buffers() {
    let schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("user_id", ParseAsType::U32),
    ]);
    let batch = super::RelayRecordBatch::single(
        schema,
        None,
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("user_id".to_string(), RuntimeValue::U32(42)),
        ]),
        AckSet::empty(),
    )
    .expect("relay batch should build");
    let payload_bytes = batch
        .batch
        .batch()
        .columns()
        .iter()
        .map(|column| {
            column
                .to_data()
                .get_slice_memory_size()
                .expect("test Arrow type should report its logical payload size") as u64
        })
        .sum::<u64>();
    let allocated_bytes = batch
        .batch
        .batch()
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size() as u64)
        .sum::<u64>();

    assert!(allocated_bytes > payload_bytes);
    assert_eq!(batch.estimated_bytes(), payload_bytes);
}

#[tokio::test]
async fn filter_map_lookup_hash_map_enriches_rows_and_filters_misses() {
    let input_schema = test_schema(&[
        ("id", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("title", ParseAsType::String),
    ]);
    let lookup_schema = test_schema(&[
        ("normalized_title", ParseAsType::String),
        ("city_name", ParseAsType::String),
        ("region_name", ParseAsType::String),
    ]);
    let lookup_batch = lookup_schema
        .batch_from_test_rows([[
            (
                "normalized_title".to_string(),
                RuntimeValue::String("mr".to_string()),
            ),
            (
                "city_name".to_string(),
                RuntimeValue::String("Chicago".to_string()),
            ),
            (
                "region_name".to_string(),
                RuntimeValue::String("IL".to_string()),
            ),
        ]])
        .expect("lookup fixture should build as Arrow");
    let lookup = Arc::new(super::LookupRuntime {
        model: CreateLookup {
            name: identifier("titles_by_normalized"),
            key_field: identifier("normalized_title"),
            resource: identifier("titles_data"),
            path: "lookup.jsonl".to_string(),
            decode_using_codec: identifier("title_lookup_codec"),
        },
        resource_version: 1,
        schema: lookup_schema,
        batch: Arc::new(lookup_batch),
        entries: Arc::new(HashMap::from_iter([("mr".to_string(), 0)])),
    });
    let lookups = HashMap::from_iter([(identifier("titles_by_normalized"), lookup)]);
    let output_schema = Arc::new(compile_schema(&CreateSchema {
        name: identifier("lookup_output"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("id"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("active"),
                ty: ParseAsType::Bool,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("title_key"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("city"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("region"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: false,
            },
        ],
    }));
    let program = super::compile_processor_output_filter_map_program(
        super::RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &identifier("project_titles"),
        },
        &[identifier("incoming_logs")],
        &identifier("projected_titles"),
        &construction(
            "INHERIT ALL EXCEPT title SET title_key = lower(input.title), city = \
             LOOKUP_HASH_MAP(\"titles_by_normalized\", lower(input.title), \"city_name\"), region \
             = LOOKUP_HASH_MAP(\"titles_by_normalized\", lower(input.title), \"region_name\") \
             WHERE NOT is_null(LOOKUP_HASH_MAP(\"titles_by_normalized\", lower(input.title), \
             \"city_name\"))",
        ),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        None,
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &lookups,
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("filter-map should compile")
    .expect("program should exist");
    assert_eq!(program.lookup_hash_maps.len(), 2);

    let (hit_acks, _hit_completion) = AckSet::root();
    let (miss_acks, _miss_completion) = AckSet::root();
    let batch = super::RelayRecordBatch::from_messages(
        input_schema,
        vec![
            RelayMessage {
                key: string_branch_key("tenant", "acme"),
                record: test_runtime_row([
                    ("id".to_string(), RuntimeValue::String("hit-1".to_string())),
                    ("active".to_string(), RuntimeValue::Bool(true)),
                    ("title".to_string(), RuntimeValue::String("MR".to_string())),
                ]),
                acks: hit_acks,
            },
            RelayMessage {
                key: string_branch_key("tenant", "acme"),
                record: test_runtime_row([
                    ("id".to_string(), RuntimeValue::String("miss-1".to_string())),
                    ("active".to_string(), RuntimeValue::Bool(true)),
                    (
                        "title".to_string(),
                        RuntimeValue::String("Unknown".to_string()),
                    ),
                ]),
                acks: miss_acks,
            },
        ],
    )
    .expect("batch should build");

    let plan = super::plan_filter_map_messages(
        "deduplicator",
        &identifier("project_titles"),
        "FILTER-MAP",
        &program,
        batch,
        super::current_timestamp(),
        &HashMap::default(),
    )
    .await
    .expect("filter-map planning should succeed");
    let messages = plan
        .batch
        .expect("filter-map should produce a batch")
        .try_into_messages()
        .expect("filter-map batch should convert to messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(
        row_value(&messages[0].record, "city"),
        Some(RuntimeValue::String("Chicago".to_string()))
    );
    assert_eq!(
        row_value(&messages[0].record, "region"),
        Some(RuntimeValue::String("IL".to_string()))
    );
}

#[tokio::test]
async fn filter_map_can_read_branch_namespace() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
        ("branch_tenant", ParseAsType::String),
    ]);
    let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
    let program = super::compile_processor_output_filter_map_program(
        super::RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &identifier("project_notifications"),
        },
        &[identifier("notifications")],
        &identifier("projected_notifications"),
        &construction(
            "INHERIT ALL SET branch_tenant = branch.tenant, amount = amount + 1 WHERE \
             branch.tenant = output.tenant",
        ),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: input_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        None,
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[identifier("tenant")],
            current_branch_schema: Some(&branch_schema),
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("filter-map should compile")
    .expect("program should exist");

    let (acks, _completion) = AckSet::root();
    let batch = super::RelayRecordBatch::from_messages(
        input_schema,
        vec![RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("amount".to_string(), RuntimeValue::I64(7)),
                (
                    "branch_tenant".to_string(),
                    RuntimeValue::String("".to_string()),
                ),
            ]),
            acks,
        }],
    )
    .expect("batch should build");

    let plan = super::plan_filter_map_messages(
        "deduplicator",
        &identifier("project_notifications"),
        "FILTER-MAP",
        &program,
        batch,
        super::current_timestamp(),
        &HashMap::default(),
    )
    .await
    .expect("filter-map planning should succeed");

    assert!(
        plan.message_errors.is_empty(),
        "projection should not produce message errors: {:?}",
        plan.message_errors
            .iter()
            .map(|error| error.error.message.as_str())
            .collect::<Vec<_>>()
    );

    let messages = plan
        .batch
        .expect("filter-map should produce a batch")
        .try_into_messages()
        .expect("filter-map batch should convert to messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(
        row_value(&messages[0].record, "branch_tenant"),
        Some(RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(
        row_value(&messages[0].record, "amount"),
        Some(RuntimeValue::I64(8))
    );
}

#[tokio::test]
async fn projection_can_read_branch_namespace() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("amount", ParseAsType::I64),
    ]);
    let output_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
        ("branch_tenant", ParseAsType::String),
    ]);
    let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
    let program = super::compile_processor_output_filter_map_program(
        super::RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &identifier("project_notifications"),
        },
        &[identifier("notifications")],
        &identifier("projected_notifications"),
        &construction(
            "INHERIT tenant, amount SET branch_tenant = branch.tenant, amount = amount + 1 WHERE \
             branch.tenant = output.tenant",
        ),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        None,
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[identifier("tenant")],
            current_branch_schema: Some(&branch_schema),
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("filter-map should compile")
    .expect("program should exist");

    let (acks, _completion) = AckSet::root();
    let batch = super::RelayRecordBatch::from_messages(
        input_schema,
        vec![RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("active".to_string(), RuntimeValue::Bool(true)),
                ("amount".to_string(), RuntimeValue::I64(7)),
            ]),
            acks,
        }],
    )
    .expect("batch should build");

    let plan = super::plan_filter_map_messages(
        "processor",
        &identifier("project_notifications"),
        "FILTER-MAP",
        &program,
        batch,
        super::current_timestamp(),
        &HashMap::default(),
    )
    .await
    .expect("filter-map planning should succeed");

    assert!(
        plan.message_errors.is_empty(),
        "projection should not produce message errors: {:?}",
        plan.message_errors
            .iter()
            .map(|error| error.error.message.as_str())
            .collect::<Vec<_>>()
    );

    let messages = plan
        .batch
        .expect("filter-map should produce a batch")
        .try_into_messages()
        .expect("filter-map batch should convert to messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(
        row_value(&messages[0].record, "branch_tenant"),
        Some(RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(
        row_value(&messages[0].record, "amount"),
        Some(RuntimeValue::I64(8))
    );
    assert_eq!(
        messages[0].key.as_ref(),
        string_branch_key("tenant", "acme").as_ref()
    );
}

#[tokio::test]
async fn inherit_all_preserves_fixed_size_array_values_through_the_vm() {
    let schema = test_schema(&[(
        "vector",
        ParseAsType::Array {
            element: Box::new(ParseAsType::F32),
            len: 2,
        },
    )]);
    let program = super::compile_processor_output_filter_map_program(
        super::RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &identifier("copy_vectors"),
        },
        &[identifier("vectors")],
        &identifier("copied_vectors"),
        &construction("INHERIT ALL"),
        super::RuntimeVmSchemaPair {
            input: schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        None,
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("array inheritance should compile")
    .expect("INHERIT ALL should produce a VM program");
    let expected = RuntimeValue::Array(vec![
        RuntimeValue::F32(1.25.into()),
        RuntimeValue::F32((-2.5).into()),
    ]);
    let batch = super::RelayRecordBatch::from_messages(
        schema,
        vec![RelayMessage {
            key: None,
            record: test_runtime_row([("vector".to_string(), expected.clone())]),
            acks: AckSet::empty(),
        }],
    )
    .expect("array input batch should build");

    let plan = super::plan_filter_map_messages(
        "junction",
        &identifier("copy_vectors"),
        "FILTER-MAP",
        &program,
        batch,
        super::current_timestamp(),
        &HashMap::default(),
    )
    .await
    .expect("array inheritance should execute");

    assert!(plan.message_errors.is_empty());
    let output = plan.batch.expect("array output batch should exist");
    let record = output
        .runtime_row(0)
        .expect("array output row should materialize at the test boundary");
    assert_eq!(row_value(&record, "vector"), Some(expected));
}

#[tokio::test]
async fn ordered_set_error_reports_operation_index_and_previous_partial_value() {
    let input_schema = test_schema(&[
        ("amount", ParseAsType::I64),
        ("denominator", ParseAsType::I64),
    ]);
    let output_schema = test_schema(&[("amount", ParseAsType::I64)]);
    let program = super::compile_processor_output_filter_map_program(
        super::RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &identifier("calculate_amount"),
        },
        &[identifier("amounts")],
        &identifier("calculated_amounts"),
        &construction("SET amount = input.amount, amount = amount / input.denominator"),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        None,
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("ordered SET should compile")
    .expect("ordered SET should produce a program");
    let batch = super::RelayRecordBatch::from_messages(
        input_schema,
        vec![
            RelayMessage {
                key: None,
                record: test_runtime_row([
                    ("amount".to_string(), RuntimeValue::I64(7)),
                    ("denominator".to_string(), RuntimeValue::I64(0)),
                ]),
                acks: AckSet::empty(),
            },
            RelayMessage {
                key: None,
                record: test_runtime_row([
                    ("amount".to_string(), RuntimeValue::I64(10)),
                    ("denominator".to_string(), RuntimeValue::I64(2)),
                ]),
                acks: AckSet::empty(),
            },
        ],
    )
    .expect("input batch should build");

    let plan = super::plan_filter_map_messages(
        "junction",
        &identifier("calculate_amount"),
        "FILTER-MAP",
        &program,
        batch,
        super::current_timestamp(),
        &HashMap::default(),
    )
    .await
    .expect("a side error is a planned message error");

    let successful = plan
        .batch
        .as_ref()
        .expect("the successful row should remain in the output batch");
    assert_eq!(successful.message_count(), 1);
    let successful_record = successful
        .runtime_row(0)
        .expect("successful output row should materialize at the test boundary");
    assert_eq!(
        row_value(&successful_record, "amount"),
        Some(RuntimeValue::I64(5))
    );
    let [error] = plan.message_errors.as_slice() else {
        panic!("expected exactly one planned message error");
    };
    assert_eq!(error.error.code, MessageErrorCode::Evaluation);
    assert_eq!(error.error.operation, MessageErrorOperation::Set);
    assert_eq!(error.error.operation_index, Some(1));
    assert_eq!(
        error
            .error
            .fields
            .iter()
            .map(FieldPath::as_str)
            .collect::<Vec<_>>(),
        vec!["input.denominator", "output.amount"]
    );
    assert_eq!(
        error.partial_output.as_ref().and_then(|output| output
            .value(0, "amount")
            .expect("partial output is readable")),
        Some(RuntimeValue::I64(7)),
        "partial output: {:?}; error: {}",
        error.partial_output,
        error.error.message
    );
}

#[test]
fn filter_map_rejects_branch_namespace_without_branch_schema() {
    let schema = test_schema(&[("tenant", ParseAsType::String)]);
    let error = super::compile_processor_output_filter_map_program(
        super::RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &identifier("project_notifications"),
        },
        &[identifier("notifications")],
        &identifier("projected_notifications"),
        &construction("INHERIT ALL WHERE branch.tenant = output.tenant"),
        super::RuntimeVmSchemaPair {
            input: schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        None,
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect_err("branch namespace must require a branch schema");
    let error = error.to_string();

    assert!(
        error.contains("branch.tenant") || error.contains("namespace 'branch'"),
        "expected branch namespace error, got {error}"
    );
}

#[test]
fn filter_map_rejects_missing_branch_key() {
    let schema = test_schema(&[("tenant", ParseAsType::String)]);
    let branch_schema = test_schema(&[("region", ParseAsType::String)]).arrow_schema();
    let error = super::compile_processor_output_filter_map_program(
        super::RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &identifier("project_notifications"),
        },
        &[identifier("notifications")],
        &identifier("projected_notifications"),
        &construction("INHERIT ALL WHERE branch.tenant = output.tenant"),
        super::RuntimeVmSchemaPair {
            input: schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        None,
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[identifier("region")],
            current_branch_schema: Some(&branch_schema),
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect_err("branch namespace must reject missing keys");
    let error = error.to_string();

    assert!(
        error.contains("branch.tenant") || error.contains("tenant"),
        "expected missing branch key error, got {error}"
    );
}

#[tokio::test]
async fn emitter_invocations_run_after_set_for_selected_rows_and_append_headers() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("raw", ParseAsType::String),
        ("active", ParseAsType::Bool),
    ]);
    let output_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("normalized", ParseAsType::String),
    ]);
    let emitter = CreateEmitter {
        name: identifier("kafka_notifications"),
        from: ProcessorInputs::single(identifier("notifications")),
        encode_using_codec: Some(identifier("notification_codec")),
        sink: Box::new(EmitSink::Kafka {
            client: identifier("kafka_main"),
            topic: identifier("notifications_out"),
        }),
        flush_each: "100ms".to_string(),
        max_batch_size: Some("1MiB".to_string()),
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        publishing_mode: EmitterPublishingMode::NoAck {
            retry_policy: RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        construction: construction(
            "INHERIT tenant SET normalized = lower(input.raw) WHERE input.active INVOKE \
             write_header(lower(\"TENANT\"),
             input.tenant), write_header(\"route\", output.normalized), write_header(\"route\", \
             \"second\")",
        ),
        materialized_state: Vec::new(),
    };
    let program = super::compile_emitter_filter_map_program(
        &domain("default"),
        &emitter,
        input_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        output_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("emitter filter-map must compile")
    .expect("program must exist");
    let mut unsupported_emitter = emitter.clone();
    *unsupported_emitter.sink = EmitSink::ZeroMq {
        client: identifier("zeromq_main"),
    };
    let error = super::compile_emitter_filter_map_program(
        &domain("default"),
        &unsupported_emitter,
        input_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        output_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect_err("ZeroMQ emitters must reject write_header");
    assert!(error.to_string().contains("ZEROMQ emitters do not support"));
    let messages = [true, false]
        .into_iter()
        .map(|active| {
            let (acks, _completion) = AckSet::root();
            RelayMessage {
                key: None,
                record: test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    (
                        "raw".to_string(),
                        RuntimeValue::String("FAST-LANE".to_string()),
                    ),
                    ("active".to_string(), RuntimeValue::Bool(active)),
                ]),
                acks,
            }
        })
        .collect::<Vec<_>>();
    let batch =
        super::RelayRecordBatch::from_messages(input_schema, messages).expect("batch must build");

    let plan = super::plan_emitter_filter_map_batch(
        &emitter.name,
        &program,
        batch,
        Timestamp::from_unix_nanos(1),
        &HashMap::default(),
    )
    .await
    .expect("emitter filter-map must execute");

    let output = plan
        .batch
        .expect("selected emitter output must remain a batch");
    assert_eq!(output.batch.batch().num_rows(), 1);
    let output_record = output
        .runtime_row(0)
        .expect("test may inspect the selected output row");
    assert_eq!(plan.source_rows, vec![0]);
    assert_eq!(
        row_value(&output_record, "normalized"),
        Some(RuntimeValue::String("fast-lane".to_string()))
    );
    assert_eq!(
        plan.headers,
        Some(vec![vec![
            ("tenant".to_string(), "acme".to_string()),
            ("route".to_string(), "fast-lane".to_string()),
            ("route".to_string(), "second".to_string()),
        ]])
    );
}

#[tokio::test]
async fn sqs_fifo_group_expression_evaluates_per_source_row_in_order() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("region", ParseAsType::String),
    ]);
    let emitter = CreateEmitter {
        name: identifier("sqs_notifications"),
        from: ProcessorInputs::single(identifier("notifications")),
        encode_using_codec: Some(identifier("notification_codec")),
        sink: Box::new(EmitSink::Sqs {
            client: identifier("sqs_main"),
            queue: "notifications.fifo".to_string(),
            fifo_group: Some(SqsFifoGroup::Expression(expression(
                "concat(input.tenant, '-', input.region)",
            ))),
        }),
        flush_each: "100ms".to_string(),
        max_batch_size: Some("1MiB".to_string()),
        mode: AckMode::Attached,
        error_policies: ErrorPolicies::handled_by_log(),
        publishing_mode: EmitterPublishingMode::SqsBatch {
            retry_policy: RetryPolicy {
                backoff: "250ms".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        construction: construction("INHERIT ALL"),
        materialized_state: Vec::new(),
    };
    let program = super::compile_sqs_fifo_group_program(
        &domain("default"),
        &emitter,
        input_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("SQS FIFO group expression must compile")
    .expect("expression mode must produce a program");
    let messages = [("acme", "us"), ("globex", "eu"), ("acme", "ap")]
        .into_iter()
        .map(|(tenant, region)| {
            let (acks, _completion) = AckSet::root();
            RelayMessage {
                key: None,
                record: test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String(tenant.to_string()),
                    ),
                    (
                        "region".to_string(),
                        RuntimeValue::String(region.to_string()),
                    ),
                ]),
                acks,
            }
        })
        .collect::<Vec<_>>();
    let batch = super::RelayRecordBatch::from_messages(input_schema, messages)
        .expect("SQS source batch must build");

    let groups = super::evaluate_sqs_fifo_group_program(
        &emitter.name,
        &program,
        &batch,
        Timestamp::from_unix_nanos(1),
        &HashMap::default(),
    )
    .await
    .expect("SQS FIFO group expression must execute");

    assert_eq!(
        groups,
        vec![
            Ok(Some("acme-us".to_string())),
            Ok(Some("globex-eu".to_string())),
            Ok(Some("acme-ap".to_string())),
        ]
    );
}

#[tokio::test]
async fn filter_map_on_runtime_row_evaluates_only_selected_arrow_row() {
    let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::U32)]);
    let where_clause = expression("input.value = (3 AS U32)");
    let program = super::compile_session_filter_map_program(
        &domain("default"),
        &identifier("selected_row_subscription"),
        Some(&where_clause),
        schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("subscription filter must compile")
    .expect("WHERE clause must produce a program");
    let rows = [
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("value".to_string(), RuntimeValue::U32(1)),
        ]),
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("value".to_string(), RuntimeValue::U32(3)),
        ]),
    ];
    let row_batches = rows
        .iter()
        .map(RuntimeRow::one_row_batch)
        .collect::<Vec<_>>();
    let batch = RuntimeRecordBatch::concat(&row_batches.iter().collect::<Vec<_>>())
        .expect("multi-row Arrow batch should build");
    let selected = RuntimeRow::new(Arc::new(batch), 1, rows[1].metadata().clone())
        .expect("second Arrow row should be addressable");

    let output = execute_filter_map_for_test(
        &program,
        selected,
        None,
        None,
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("selected row filter-map must execute")
    .expect("second Arrow row must pass the filter");

    assert_eq!(row_value(&output, "value"), Some(RuntimeValue::U32(3)));
}

#[tokio::test]
async fn filter_map_internal_types_roundtrip_matches_http_logic_fixture() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("u8", ParseAsType::U8),
        ("i8", ParseAsType::I8),
        ("u16", ParseAsType::U16),
        ("i16", ParseAsType::I16),
        ("u32", ParseAsType::U32),
        ("i32", ParseAsType::I32),
        ("u64", ParseAsType::U64),
        ("i64", ParseAsType::I64),
        ("f32", ParseAsType::F32),
        ("f64", ParseAsType::F64),
        ("occurred_at", ParseAsType::Datetime),
        ("raw", ParseAsType::String),
    ]);
    let output_schema = Arc::new(compile_schema(&CreateSchema {
        name: identifier("logic_output"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("u8_next"),
                ty: ParseAsType::U8,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("i8_abs"),
                ty: ParseAsType::I8,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("u16_keep"),
                ty: ParseAsType::U16,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("i16_prev"),
                ty: ParseAsType::I16,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("u32_same"),
                ty: ParseAsType::U32,
                optional: true,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("i32_neg"),
                ty: ParseAsType::I32,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("u64_next"),
                ty: ParseAsType::U64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("i64_keep"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("f32_next"),
                ty: ParseAsType::F32,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("f64_keep"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("bool_copy"),
                ty: ParseAsType::Bool,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("occurred_text"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("occurred_copy"),
                ty: ParseAsType::Datetime,
                optional: false,
                sensitive: false,
            },
        ],
    }));
    let program = super::compile_ingestor_filter_map_program(
        &domain("default"),
        &identifier("logic_ingestor"),
        &IngestSource::Endpoint {
            endpoint: identifier("logic_endpoint"),
            mode: nervix_models::EndpointIngestMode::NoAckSequential,
            quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                max_size: "1MiB".to_string(),
            },
        },
        &construction(
            "INHERIT tenant SET u8_next = input.u8 + (1 AS U8), i8_abs = abs(input.i8), u16_keep \
             = coalesce(input.u16, (0 AS U16)), i16_prev = input.i16 - (1 AS I16), u32_same = \
             coalesce(nullif(input.u32, (999 AS U32)), (0 AS U32)), i32_neg = -input.i32, \
             u64_next = input.u64 + (2 AS U64), i64_keep = input.i64, f32_next = input.f32 + (1.5 \
             AS F32), f64_keep = input.f64, bool_copy = input.active, occurred_text = \
             input.occurred_at AS STRING, occurred_copy = (input.occurred_at AS STRING) AS \
             DATETIME WHERE input.active AND input.occurred_at > ('2026-04-07T00:00:00Z' AS \
             DATETIME)",
        ),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("filter-map must compile")
    .expect("program must exist");

    let record = test_runtime_row([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        ("active".to_string(), RuntimeValue::Bool(true)),
        ("u8".to_string(), RuntimeValue::U8(5)),
        ("i8".to_string(), RuntimeValue::I8(-7)),
        ("u16".to_string(), RuntimeValue::U16(9)),
        ("i16".to_string(), RuntimeValue::I16(12)),
        ("u32".to_string(), RuntimeValue::U32(42)),
        ("i32".to_string(), RuntimeValue::I32(-11)),
        ("u64".to_string(), RuntimeValue::U64(100)),
        ("i64".to_string(), RuntimeValue::I64(-64)),
        ("f32".to_string(), RuntimeValue::F32(OrderedFloat(2.5))),
        ("f64".to_string(), RuntimeValue::F64(OrderedFloat(7.25))),
        (
            "occurred_at".to_string(),
            RuntimeValue::Datetime(
                chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z")
                    .expect("valid timestamp"),
            ),
        ),
        (
            "raw".to_string(),
            RuntimeValue::String("ignored".to_string()),
        ),
    ]);

    let output =
        execute_filter_map_for_test(&program, record, None, None, Timestamp::from_unix_nanos(1))
            .await
            .expect("filter-map must execute")
            .expect("record must not be filtered out");

    assert_eq!(
        row_value(&output, "tenant"),
        Some(RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(row_value(&output, "u8_next"), Some(RuntimeValue::U8(6)));
    assert_eq!(row_value(&output, "i8_abs"), Some(RuntimeValue::I8(7)));
    assert_eq!(row_value(&output, "u16_keep"), Some(RuntimeValue::U16(9)));
    assert_eq!(row_value(&output, "i16_prev"), Some(RuntimeValue::I16(11)));
    assert_eq!(row_value(&output, "u32_same"), Some(RuntimeValue::U32(42)));
    assert_eq!(row_value(&output, "i32_neg"), Some(RuntimeValue::I32(11)));
    assert_eq!(row_value(&output, "u64_next"), Some(RuntimeValue::U64(102)));
    assert_eq!(row_value(&output, "i64_keep"), Some(RuntimeValue::I64(-64)));
    assert_eq!(
        row_value(&output, "f32_next"),
        Some(RuntimeValue::F32(OrderedFloat(4.0)))
    );
    assert_eq!(
        row_value(&output, "f64_keep"),
        Some(RuntimeValue::F64(OrderedFloat(7.25)))
    );
    assert_eq!(
        row_value(&output, "bool_copy"),
        Some(RuntimeValue::Bool(true))
    );
    assert_eq!(
        row_value(&output, "occurred_text"),
        Some(RuntimeValue::String(
            "2026-04-07T12:34:56+00:00".to_string()
        ))
    );
    assert_eq!(
        row_value(&output, "occurred_copy"),
        Some(RuntimeValue::Datetime(
            chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z").expect("valid timestamp"),
        ))
    );
}

#[tokio::test]
async fn reorderer_key_program_evaluates_direct_u32_field() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("sequence", ParseAsType::U32),
        ("payload", ParseAsType::String),
    ]);
    let program = super::compile_reorderer_program(
        &identifier("order_notifications"),
        &[identifier("incoming_notifications")],
        &[expression("input.sequence")],
        input_schema.arrow_schema(),
        None,
    )
    .expect("reorderer key program should compile");
    let records = vec![
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("sequence".to_string(), RuntimeValue::U32(3)),
            (
                "payload".to_string(),
                RuntimeValue::String("third".to_string()),
            ),
        ]),
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("sequence".to_string(), RuntimeValue::U32(1)),
            (
                "payload".to_string(),
                RuntimeValue::String("first".to_string()),
            ),
        ]),
    ];
    let input = vm_input_from_test_rows(&records, &program.program.input_schema)
        .expect("VM input batch should build");
    let output = super::execute_program_with_selection_in_context(
        &program.program,
        &input,
        &super::VmExecutionContext {
            now: Timestamp::from_unix_nanos(1),
            injector: None,
        },
    )
    .await
    .expect("reorderer key program should execute");

    assert_eq!(program.key_count, 1);
    assert_eq!(
        program.key_column_offset,
        output.batch.columns().len().saturating_sub(1)
    );
    assert_eq!(
        super::reorder_key_part(output.batch.column(program.key_column_offset), 0),
        super::ReorderKeyPart::UInt64(3)
    );
    assert_eq!(
        super::reorder_key_part(output.batch.column(program.key_column_offset), 1),
        super::ReorderKeyPart::UInt64(1)
    );
}

#[tokio::test]
async fn large_vm_batches_preserve_results_through_public_vm_api() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("sequence", ParseAsType::U32),
        ("payload", ParseAsType::String),
    ]);
    let program = super::compile_reorderer_program(
        &identifier("order_notifications"),
        &[identifier("incoming_notifications")],
        &[expression("input.sequence")],
        input_schema.arrow_schema(),
        None,
    )
    .expect("reorderer key program should compile");
    let records = (0..=super::VM_SPAWN_BLOCKING_ROW_THRESHOLD)
        .map(|sequence| {
            test_runtime_row([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("sequence".to_string(), RuntimeValue::U32(sequence as u32)),
                (
                    "payload".to_string(),
                    RuntimeValue::String(format!("payload-{sequence}")),
                ),
            ])
        })
        .collect::<Vec<_>>();
    let input = vm_input_from_test_rows(&records, &program.program.input_schema)
        .expect("VM input batch should build");

    let output = super::execute_program_with_selection_in_context(
        &program.program,
        &input,
        &super::VmExecutionContext {
            now: Timestamp::from_unix_nanos(1),
            injector: None,
        },
    )
    .await
    .expect("large VM batch should execute");

    assert_eq!(
        output.batch.row_count(),
        super::VM_SPAWN_BLOCKING_ROW_THRESHOLD + 1
    );
}

#[test]
fn processor_key_expressions_reject_relay_qualified_fields() {
    assert!(nervix_nspl::parse_expression("incoming_notifications.sequence").is_err());
}

#[tokio::test]
async fn ingestor_filter_map_accepts_missing_optional_input_fields() {
    let input_schema = Arc::new(compile_schema(&CreateSchema {
        name: identifier("optional_logic"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("raw"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: false,
            },
        ],
    }));
    let output_schema = Arc::new(compile_schema(&CreateSchema {
        name: identifier("optional_logic_output"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("normalized"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: false,
            },
        ],
    }));
    let program = super::compile_ingestor_filter_map_program(
        &domain("default"),
        &identifier("logic_ingestor"),
        &IngestSource::Endpoint {
            endpoint: identifier("logic_endpoint"),
            mode: nervix_models::EndpointIngestMode::NoAckSequential,
            quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                max_size: "1MiB".to_string(),
            },
        },
        &construction("INHERIT tenant SET normalized = lower(input.raw)"),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("filter-map must compile")
    .expect("program must exist");

    let output = execute_filter_map_for_test(
        &program,
        test_runtime_row([(
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        )]),
        None,
        None,
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("filter-map must execute")
    .expect("record must not be filtered out");

    assert_eq!(
        row_value(&output, "tenant"),
        Some(RuntimeValue::String("acme".to_string()))
    );
    assert!(row_value(&output, "raw").is_none());
    assert!(row_value(&output, "normalized").is_none());
}

#[tokio::test]
async fn kafka_ingestor_filter_map_can_read_metadata_namespace() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("amount", ParseAsType::I64),
        ("raw", ParseAsType::String),
    ]);
    let program = super::compile_ingestor_filter_map_program(
        &domain("default"),
        &identifier("logic_ingestor"),
        &IngestSource::Kafka {
            client: identifier("logic_kafka"),
            topic: identifier("logic_notifications"),
            offset_mode: nervix_models::KafkaOffsetMode::Domain,
            instances: 1,
            mode: nervix_models::KafkaIngestMode::AckSequential {
                timeout: "5s".to_string(),
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "100ms".to_string(),
                    max_backoff: "200ms".to_string(),
                },
            },
            quiesce: nervix_models::IngestQuiesceMode::Suspend,
        },
        &construction(
            "INHERIT tenant SET topic = metadata.topic, partition = metadata.partition, offset = \
             metadata.offset WHERE metadata.offset >= 0",
        ),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: Arc::new(compile_schema(&CreateSchema {
                name: identifier("metadata_output"),
                fields: vec![
                    nervix_models::SchemaField {
                        name: identifier("tenant"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                    nervix_models::SchemaField {
                        name: identifier("topic"),
                        ty: ParseAsType::String,
                        optional: true,
                        sensitive: false,
                    },
                    nervix_models::SchemaField {
                        name: identifier("partition"),
                        ty: ParseAsType::I32,
                        optional: true,
                        sensitive: false,
                    },
                    nervix_models::SchemaField {
                        name: identifier("offset"),
                        ty: ParseAsType::I64,
                        optional: true,
                        sensitive: false,
                    },
                ],
            }))
            .arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("filter-map must compile")
    .expect("program must exist");

    let record = test_runtime_row([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        ("active".to_string(), RuntimeValue::Bool(true)),
        ("amount".to_string(), RuntimeValue::I64(7)),
        ("raw".to_string(), RuntimeValue::String("meta".to_string())),
    ]);
    let metadata = ingest_metadata_for_test(
        super::IngestMetadataKind::Kafka,
        &[super::IngestMetadataRow::Kafka {
            topic: "logic_notifications_t123",
            partition: 2,
            offset: 42,
            headers: &super::NoIngestHeaders,
        }],
    );

    let output = execute_filter_map_for_test(
        &program,
        record,
        None,
        Some(&metadata),
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("filter-map must execute")
    .expect("record must not be filtered out");

    assert_eq!(
        row_value(&output, "tenant"),
        Some(RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(
        row_value(&output, "topic"),
        Some(RuntimeValue::String("logic_notifications_t123".to_string()))
    );
    assert_eq!(row_value(&output, "partition"), Some(RuntimeValue::I32(2)));
    assert_eq!(row_value(&output, "offset"), Some(RuntimeValue::I64(42)));
    assert!(row_value(&output, "active").is_none());
    assert!(row_value(&output, "amount").is_none());
    assert!(row_value(&output, "raw").is_none());

    let grouped_metadata = ingest_metadata_for_test(
        super::IngestMetadataKind::Kafka,
        &[
            super::IngestMetadataRow::Kafka {
                topic: "logic_notifications_t123",
                partition: 2,
                offset: 42,
                headers: &super::NoIngestHeaders,
            },
            super::IngestMetadataRow::Kafka {
                topic: "logic_notifications_t123",
                partition: 3,
                offset: 43,
                headers: &super::NoIngestHeaders,
            },
        ],
    );
    let offsets = grouped_metadata
        .field_column("offset")
        .expect("metadata projection must succeed")
        .expect("Kafka offset column must exist");
    let offsets = offsets
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("Kafka offsets must remain INT64");
    assert_eq!(offsets.values(), &[42, 43]);
    let selected = grouped_metadata
        .select(&[false, true])
        .expect("metadata row selection must succeed");
    let selected_offset = selected
        .field_column("offset")
        .expect("selected metadata projection must succeed")
        .expect("selected Kafka offset column must exist");
    let selected_offset = selected_offset
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("selected Kafka offset must remain INT64");
    assert_eq!(selected_offset.values(), &[43]);
}

#[tokio::test]
async fn ingestor_header_functions_preserve_order_and_missing_value_semantics() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("header_name", ParseAsType::String),
        ("raw", ParseAsType::String),
    ]);
    let output_schema = Arc::new(compile_schema(&CreateSchema {
        name: identifier("header_output"),
        fields: vec![
            SchemaField {
                name: identifier("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            SchemaField {
                name: identifier("first"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: false,
            },
            SchemaField {
                name: identifier("total"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
        ],
    }));
    let source = IngestSource::Kafka {
        client: identifier("logic_kafka"),
        topic: identifier("logic_notifications"),
        offset_mode: nervix_models::KafkaOffsetMode::Domain,
        instances: 1,
        mode: nervix_models::KafkaIngestMode::AckSequential {
            timeout: "5s".to_string(),
            retry_policy: RetryPolicy {
                backoff: "100ms".to_string(),
                max_backoff: "200ms".to_string(),
            },
        },
        quiesce: nervix_models::IngestQuiesceMode::Suspend,
    };
    let program = super::compile_ingestor_filter_map_program(
        &domain("default"),
        &identifier("header_ingestor"),
        &source,
        &construction(
            "INHERIT tenant SET first = read_header(lower(input.header_name)), total = \
             count(read_headers(lower(input.header_name))) WHERE read_header(\"tenant\") = \
             input.tenant AND count(read_headers(\"missing\")) = 0",
        ),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("header filter-map must compile")
    .expect("program must exist");
    let record = test_runtime_row([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        (
            "header_name".to_string(),
            RuntimeValue::String("ROUTE".to_string()),
        ),
        ("raw".to_string(), RuntimeValue::String("body".to_string())),
    ]);
    let first_headers = TestIngestHeaders(&[
        ("tenant", "acme"),
        ("route", "primary"),
        ("route", "secondary"),
    ]);
    let metadata = ingest_metadata_for_test(
        super::IngestMetadataKind::Headers,
        &[super::IngestMetadataRow::Headers {
            headers: &first_headers,
        }],
    );

    let output = execute_filter_map_for_test(
        &program,
        record.clone(),
        None,
        Some(&metadata),
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("header filter-map must execute")
    .expect("record must be selected");

    assert_eq!(
        row_value(&output, "first"),
        Some(RuntimeValue::String("primary".to_string()))
    );
    assert_eq!(row_value(&output, "total"), Some(RuntimeValue::I64(2)));

    let second_record = test_runtime_row([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        (
            "header_name".to_string(),
            RuntimeValue::String("route".to_string()),
        ),
        (
            "raw".to_string(),
            RuntimeValue::String("second".to_string()),
        ),
    ]);
    let grouped_carrier =
        RuntimeRecordBatch::concat(&[&record.one_row_batch(), &second_record.one_row_batch()])
            .expect("grouped carrier must concatenate");
    let second_headers = TestIngestHeaders(&[("tenant", "acme"), ("route", "backup")]);
    let grouped_metadata = ingest_metadata_for_test(
        super::IngestMetadataKind::Headers,
        &[
            super::IngestMetadataRow::Headers {
                headers: &first_headers,
            },
            super::IngestMetadataRow::Headers {
                headers: &second_headers,
            },
        ],
    );
    let grouped_runtime_metadata =
        vec![record.metadata().clone(), second_record.metadata().clone()];
    let grouped_keys = vec![None, None];
    let grouped_outcomes = super::evaluate_filter_map_on_batch(
        ModelKind::Ingestor.as_str(),
        &identifier("header_ingestor"),
        &program,
        super::FilterMapOutcomeInputs {
            carrier: &grouped_carrier,
            record_metadata: &grouped_runtime_metadata,
            keys: &grouped_keys,
            filter_map_metadata: Some(&grouped_metadata),
            side_inputs: &HashMap::default(),
        },
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("grouped header filter-map must execute");
    let grouped_outputs = grouped_outcomes
        .into_iter()
        .map(|outcome| match outcome {
            super::SingleRecordFilterMapOutcome::Output(record) => record,
            super::SingleRecordFilterMapOutcome::Filtered => {
                panic!("grouped header row must be selected")
            }
            super::SingleRecordFilterMapOutcome::MessageError { error, .. } => {
                panic!("grouped header row failed: {}", error.message)
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        row_value(&grouped_outputs[0], "first"),
        Some(RuntimeValue::String("primary".to_string()))
    );
    assert_eq!(
        row_value(&grouped_outputs[0], "total"),
        Some(RuntimeValue::I64(2))
    );
    assert_eq!(
        row_value(&grouped_outputs[1], "first"),
        Some(RuntimeValue::String("backup".to_string()))
    );
    assert_eq!(
        row_value(&grouped_outputs[1], "total"),
        Some(RuntimeValue::I64(1))
    );

    let top_filter = super::compile_expression_filter_program(
        super::RuntimeCompileTarget {
            domain: &domain("default"),
            identifier: &identifier("header_ingestor"),
        },
        Some(&expression(
            "read_header(lower(input.header_name)) = \"primary\" AND \
             count(read_headers(\"missing\")) = 0",
        )),
        super::RuntimeVmSchema {
            schema: input_schema.arrow_schema(),
            sensitivity: super::VmSchemaSensitivity::default(),
        },
        true,
        MessageErrorOperation::FilterWhere,
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("top FILTER WHERE must compile")
    .expect("program must exist");
    assert!(
        execute_filter_map_for_test(
            &top_filter,
            record,
            None,
            Some(&metadata),
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("top FILTER WHERE must execute")
        .is_some()
    );
}

#[tokio::test]
async fn finalized_output_filter_reads_constructed_output_values() {
    let output_schema =
        test_schema(&[("tenant", ParseAsType::String), ("total", ParseAsType::I64)]);
    let program = super::compile_finalized_output_filter_program(
        &domain("default"),
        &identifier("aggregate_route"),
        Some(&expression("output.total >= 100 AND tenant = \"acme\"")),
        output_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_branching: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
            udfs: None,
        },
    )
    .expect("finalized output filter must compile")
    .expect("program must exist");

    let selected = test_runtime_row([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        ("total".to_string(), RuntimeValue::I64(130)),
    ]);
    assert!(
        execute_filter_map_for_test(
            &program,
            selected,
            None,
            None,
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("finalized output filter must execute")
        .is_some()
    );

    let rejected = test_runtime_row([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        ("total".to_string(), RuntimeValue::I64(99)),
    ]);
    assert!(
        execute_filter_map_for_test(
            &program,
            rejected,
            None,
            None,
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("finalized output filter must execute")
        .is_none()
    );
}

#[tokio::test]
async fn generator_set_program_projects_columnar_state_and_branch_context() {
    let source_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
        (
            "samples",
            ParseAsType::Array {
                element: Box::new(ParseAsType::F32),
                len: 2,
            },
        ),
        (
            "labels",
            ParseAsType::Vec {
                element: Box::new(ParseAsType::String),
            },
        ),
    ]);
    let output_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
        (
            "samples",
            ParseAsType::Array {
                element: Box::new(ParseAsType::F32),
                len: 2,
            },
        ),
        (
            "labels",
            ParseAsType::Vec {
                element: Box::new(ParseAsType::String),
            },
        ),
    ]);
    let branch_schema = test_schema(&[("tenant", ParseAsType::String)]);
    let output = ProcessorOutput {
        relay: identifier("generated_notifications"),
        construction: construction(
            "SET tenant = branch.tenant, amount = relay_state.notifications.amount + 1, samples = \
             relay_state.notifications.samples, labels = relay_state.notifications.labels",
        ),
        flush_policy: Some(nervix_models::OutputFlushPolicy {
            flush_each: "100ms".to_string(),
            max_batch_size: Some("1MiB".to_string()),
        }),
        message_error_policy: MessageErrorPolicy::Log,
        branch: None,
    };
    let generator = CreateGenerator {
        name: identifier("synth_notifications"),
        materialized_relay: identifier("notifications"),
        branched_by: processor_branched_by("generated_notifications", &["tenant"]),
        each: "100ms".to_string(),
        output_routes: ProcessorOutputs::new(vec![output.clone()]),
    };

    let program = super::compile_generator_set_program(
        &domain("default"),
        &generator,
        &output,
        super::GeneratorSetProgramSchemas {
            output: output_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
            source: source_schema.arrow_schema(),
            branch: Some(branch_schema.arrow_schema()),
        },
        None,
    )
    .expect("generator set program must compile");

    let samples = RuntimeValue::Array(vec![
        RuntimeValue::F32(OrderedFloat(1.0)),
        RuntimeValue::F32(OrderedFloat(2.5)),
    ]);
    let labels = RuntimeValue::Vec(vec![
        RuntimeValue::String("api".to_string()),
        RuntimeValue::String("prod".to_string()),
    ]);
    let source = source_schema
        .batch_from_test_rows([[
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("amount".to_string(), RuntimeValue::I64(7)),
            ("samples".to_string(), samples.clone()),
            ("labels".to_string(), labels.clone()),
        ]])
        .expect("generator source batch must build");
    let branch_key = string_branch_key("tenant", "acme");
    let context_projection = super::GeneratorContextProjection::new(
        &identifier("notifications"),
        source_schema.arrow_schema().as_ref(),
        Some(branch_schema.arrow_schema().as_ref()),
    );
    let context = context_projection
        .project(&source, &branch_key)
        .expect("generator context must project");
    let source_samples = source
        .schema()
        .index_of("samples")
        .expect("source samples column must exist");
    let context_samples = context
        .schema()
        .index_of("relay_state.notifications.samples")
        .expect("context samples column must exist");
    assert!(StdArc::ptr_eq(
        source.batch().column(source_samples),
        context.batch().column(context_samples),
    ));
    let input = super::GeneratorRouteInputProjection::new(&program.compiled.input_schema)
        .project(&program.compiled.input_schema, &context, &branch_key)
        .expect("generator route input must project");
    let input_samples = input
        .schema()
        .index_of("relay_state.notifications.samples")
        .expect("route input samples column must exist");
    let input_samples = input.column(input_samples).to_array_ref();
    assert!(StdArc::ptr_eq(
        context.batch().column(context_samples),
        &input_samples,
    ));

    let output = super::execute_generator_program_on_context(
        &program,
        &input,
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("generator program must execute");
    let super::GeneratorProgramOutcome::Output(output) = output else {
        panic!("generator program must emit one row");
    };

    assert_eq!(
        row_value(&output, "tenant"),
        Some(RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(row_value(&output, "amount"), Some(RuntimeValue::I64(8)));
    assert_eq!(row_value(&output, "samples"), Some(samples));
    assert_eq!(row_value(&output, "labels"), Some(labels));
}

#[tokio::test]
async fn materialized_dependencies_resolve_defaults_and_stop_in_declaration_order() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let state_schema = test_optional_schema(&[
        ("status", ParseAsType::String, false),
        ("note", ParseAsType::String, true),
    ]);
    let (shutdown, _) = watch::channel(false);
    let materialized_stream_specs = ["profiles", "rules"]
        .into_iter()
        .map(|relay| {
            (
                identifier(relay),
                super::RuntimeMaterializedRelaySpec::new(
                    state_schema.arrow_schema(),
                    super::VmSchemaSensitivity::default(),
                    Vec::new(),
                ),
            )
        })
        .collect();
    let relay_registries = [(identifier("input"), super::RelayRegistry::new())]
        .into_iter()
        .collect();
    runtime.executions.insert(
        domain.clone(),
        super::DomainExecution {
            schedule: DomainSchedule {
                domain: domain.clone(),
                nodes: Vec::new(),
                placement_groups: Vec::new(),
            },
            passive_only: false,
            start_version: 0,
            shutdown,
            graph: StdArc::new(ArcSwapOption::empty()),
            relay_registries,
            relay_schemas: HashMap::default(),
            relay_services: HashMap::default(),
            relay_branchings: HashMap::default(),
            relay_branching_schemas: HashMap::default(),
            materialized_stream_specs,
            materialized_stream_owner_nodes: HashMap::default(),
            branched_ingestors: HashMap::default(),
            branched_entrypoints: HashMap::default(),
            codecs: HashMap::default(),
            signaling_protocols: HashMap::default(),
            lookups: HashMap::default(),
            udfs: nervix_roto::UdfExecutor::default(),
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

    let default = nervix_models::MaterializedStateDependency {
        relay: identifier("profiles"),
        policy: nervix_models::MaterializedStatePolicy::Default(vec![Assignment {
            target: AssignmentTarget::bare(identifier("status")),
            value: Expression::Literal(nervix_models::Literal::String("unknown".to_string())),
        }]),
    };
    let resolved = runtime
        .resolve_materialized_dependencies(&domain, &None, &[default])
        .await
        .expect("default dependency should resolve");
    let super::MaterializedDependencyResolution::Ready(values) = resolved else {
        panic!("default dependency should be ready");
    };
    assert_eq!(
        values.get("relay_state.profiles.status"),
        Some(&RuntimeValue::String("unknown".to_string()))
    );
    assert!(!values.contains_key("relay_state.profiles.note"));

    let wait = nervix_models::MaterializedStateDependency {
        relay: identifier("profiles"),
        policy: nervix_models::MaterializedStatePolicy::RequiredWait,
    };
    let skip = nervix_models::MaterializedStateDependency {
        relay: identifier("rules"),
        policy: nervix_models::MaterializedStatePolicy::RequiredSkip,
    };
    assert!(matches!(
        runtime
            .resolve_materialized_dependencies(&domain, &None, &[wait.clone(), skip.clone()])
            .await
            .expect("missing dependencies should produce a policy outcome"),
        super::MaterializedDependencyResolution::Wait
    ));
    assert!(matches!(
        runtime
            .resolve_materialized_dependencies(&domain, &None, &[skip, wait])
            .await
            .expect("missing dependencies should produce a policy outcome"),
        super::MaterializedDependencyResolution::Skip
    ));

    let (acks, completion) = AckSet::root();
    let retained_row = state_schema
        .batch_from_test_rows([[(
            "status".to_string(),
            RuntimeValue::String("pending".to_string()),
        )]])
        .expect("required-wait branch Arrow batch must build")
        .runtime_row(0, RuntimeRecordMetadata::test())
        .expect("required-wait branch Arrow row must build");
    let retained = super::RelayRecordBatch::single(
        state_schema.clone(),
        string_branch_key("tenant", "acme"),
        retained_row,
        acks,
    )
    .expect("required-wait branch batch must build");
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let input_relay = identifier("input");
    let dependencies = [nervix_models::MaterializedStateDependency {
        relay: identifier("profiles"),
        policy: nervix_models::MaterializedStatePolicy::RequiredWait,
    }];
    let resolution = runtime.resolve_materialized_dependencies_for_batch(
        &domain,
        &input_relay,
        &dependencies,
        retained,
        super::MaterializedBatchWaitContext {
            shutdown_rx: &mut shutdown_rx,
            wait_for_required_state: true,
            quiesce_work: None,
        },
    );
    tokio::pin!(resolution);
    assert!(
        timeout(Duration::from_millis(50), &mut resolution)
            .await
            .is_err(),
        "an empty non-owner relay registry must not evict retained branch work"
    );
    shutdown_tx.send_replace(true);
    assert!(
        timeout(Duration::from_secs(1), &mut resolution)
            .await
            .expect("shutdown should release retained branch work")
            .expect("retained branch resolution should not fail")
            .is_none()
    );
    assert!(matches!(
        completion.wait().await,
        AckOutcome::NoAck(reason)
            if reason.contains("node stopped while waiting for required materialized state")
    ));

    let (acks, completion) = AckSet::root();
    let retained_row = state_schema
        .batch_from_test_rows([[(
            "status".to_string(),
            RuntimeValue::String("pending".to_string()),
        )]])
        .expect("required-wait test Arrow batch must build")
        .runtime_row(0, RuntimeRecordMetadata::test())
        .expect("required-wait test Arrow row must build");
    let retained = super::RelayRecordBatch::single(state_schema, None, retained_row, acks)
        .expect("required-wait test batch must build");
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
    assert!(
        runtime
            .resolve_materialized_dependencies_for_batch(
                &domain,
                &identifier("input"),
                &[nervix_models::MaterializedStateDependency {
                    relay: identifier("profiles"),
                    policy: nervix_models::MaterializedStatePolicy::RequiredWait,
                }],
                retained,
                super::MaterializedBatchWaitContext {
                    shutdown_rx: &mut shutdown_rx,
                    wait_for_required_state: false,
                    quiesce_work: None,
                },
            )
            .await
            .expect("terminal drain must resolve retained materialized work")
            .is_none()
    );
    assert_eq!(
        completion.wait().await,
        AckOutcome::NoAck(
            "node stopped while waiting for required materialized state at relay 'input'"
                .to_string()
        )
    );
}
