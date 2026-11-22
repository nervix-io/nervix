//! Direct evidence for the one inference session a branch owns.
//!
//! Layer: test harness.
//! Owns: worker classes and waiting admission of model loads and runs, session retention across
//! abandoned waits, the definition changes that make a branch load its model again, row refusal
//! and state atomicity, batched grouping and arrival order, chunking by working set, and the
//! projection of native diagnostics.
//! Depends on: the production session owner and the generated ONNX fixtures.
//! Must not know: graph scheduling, routes or registry implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks the branch's native session and its model runs"
    )
)]

use std::num::{NonZeroU32, NonZeroUsize};

use arrow_array::{Array as _, FixedSizeListArray, Float32Array, ListArray};
use nervix_execution::{ExecutionConfig, Executor};
use nervix_models::{
    Expression, InferencerStateInitial, InferencerTensorDimension, InferencerTensorElementType,
    InferencerTensorRepresentation, InferencerTensorSchema, Literal,
};
use ordered_float::OrderedFloat;

use super::*;
use crate::{
    registry::InferencerStatePortPlan,
    runtime::{
        FilledCpuClass, RuntimeState, RuntimeStatePlacement,
        inferencer_state::ReplicatedInferencerState, published_generation::PublishedGenerations,
        single_worker_executor,
    },
    runtime_schema::{RuntimeValue, test_runtime_row},
};

fn dense(dimensions: Vec<InferencerTensorDimension>) -> InferencerTensorSchema {
    InferencerTensorSchema {
        representation: InferencerTensorRepresentation::Dense,
        element_type: InferencerTensorElementType::F32,
        dimensions,
    }
}

fn fixed(size: u32) -> InferencerTensorDimension {
    InferencerTensorDimension::Fixed(NonZeroU32::new(size).expect("test axes are positive"))
}

fn port(name: &str, dimensions: Vec<InferencerTensorDimension>) -> InferencerTensorMapping {
    InferencerTensorMapping {
        tensor: name.to_owned(),
        schema: dense(dimensions),
        expression: Expression::Literal(Literal::Null),
    }
}

fn output(name: &str, dimensions: Vec<InferencerTensorDimension>) -> InferencerTensorDeclaration {
    InferencerTensorDeclaration {
        tensor: name.to_owned(),
        schema: dense(dimensions),
    }
}

fn model(file: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/onnx")
        .join(file)
}

fn f32s(values: &[f32]) -> RuntimeValue {
    RuntimeValue::Array(
        values
            .iter()
            .map(|value| RuntimeValue::F32(OrderedFloat(*value)))
            .collect(),
    )
}

fn sequence(rows: &[[f32; 2]]) -> RuntimeValue {
    RuntimeValue::Vec(rows.iter().map(|row| f32s(row)).collect())
}

/// One mapped batch whose rows carry the named port values, in order.
fn mapped(rows: Vec<Vec<(&str, RuntimeValue)>>) -> RuntimeRecordBatch {
    let rows = rows
        .into_iter()
        .map(|row| {
            test_runtime_row(
                row.into_iter()
                    .map(|(name, value)| (name.to_owned(), value)),
            )
        })
        .collect::<Vec<_>>();
    let schema = rows[0].batch().schema();
    RuntimeRecordBatch::from_rows(schema, rows.iter()).expect("the test rows share one schema")
}

/// The F32 values of `row` of a fixed or dynamic output column, flattened.
fn output_values(batch: &RuntimeRecordBatch, column: usize, row: usize) -> Vec<f32> {
    let mut current = batch.batch().column(column).slice(row, 1);
    loop {
        if let Some(fixed) = current.as_any().downcast_ref::<FixedSizeListArray>() {
            let start = usize::try_from(fixed.value_offset(0)).expect("nonnegative offset");
            let length = usize::try_from(fixed.value_length()).expect("nonnegative length");
            current = fixed.values().slice(start, current.len() * length);
            continue;
        }
        if let Some(list) = current.as_any().downcast_ref::<ListArray>() {
            let offsets = list.value_offsets();
            let start = usize::try_from(offsets[0]).expect("nonnegative offset");
            let end = usize::try_from(offsets[offsets.len() - 1]).expect("nonnegative offset");
            current = list.values().slice(start, end - start);
            continue;
        }
        let leaves = current
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("tensor leaves are F32");
        return leaves.values().to_vec();
    }
}

async fn prepared(
    executor: &Executor,
    inputs: Vec<InferencerTensorMapping>,
    outputs: Vec<InferencerTensorDeclaration>,
    mode: InferencerExecutionMode,
    state: Option<&InferencerStatePlan>,
) -> Arc<PreparedInference> {
    Arc::new(
        PreparedInference::prepare(
            executor,
            &inputs,
            &outputs,
            mode,
            nervix_models::InferencerWorkingMemory::Released,
            state,
            None,
            nervix_vm::VmSettings::default(),
        )
        .await
        .expect("the test definition prepares"),
    )
}

fn running_state() -> InferencerStatePlan {
    InferencerStatePlan {
        bindings: vec![InferencerStatePortPlan {
            input: "state_in".to_owned(),
            output: "state_out".to_owned(),
            schema: dense(vec![fixed(2)]),
            elements: 2,
            initial: InferencerStateInitial::Zeros,
        }],
        bytes_per_branch: 8,
    }
}

/// An unpublished, unreplicated publication for one unbranched test inferencer.
fn publication() -> Arc<ReplicatedInferencerState> {
    let placement = RuntimeStatePlacement {
        domain: nervix_models::DomainName::parse("inference").assured("a valid domain"),
        state: RuntimeState::Inferencer {
            schema: nervix_models::SchemaFingerprint::from_digest([7; 32]),
            model: nervix_models::InferencerModelFingerprint::from_digest([9; 32]),
        },
        kind: nervix_models::ModelKind::Inferencer,
        identifier: nervix_models::ModelName::parse("running").assured("a valid model name"),
        unit: nervix_models::Unit::SINGLE,
        branch_key: None,
    };
    Arc::new(ReplicatedInferencerState::from_generations(
        placement,
        PublishedGenerations::restored(0, None),
    ))
}

fn state_metrics() -> crate::metrics::InferencerStateMetrics {
    let metrics = crate::metrics::RuntimeMetrics::default();
    let domain = "inference_tests"
        .parse()
        .assured("the test domain is valid");
    let name = "scoring".parse().assured("the test inferencer is valid");
    metrics.inferencer_state_metrics(&domain, &name, nervix_models::Unit::SINGLE, None)
}

/// A branch session over the checked running sum, carrying its `STATE` from the initial zeros.
async fn checked_running(executor: &Executor) -> (Arc<PreparedInference>, InferenceSession) {
    let state = running_state();
    let prepared = prepared(
        executor,
        vec![port("features", vec![fixed(2)])],
        vec![output("score", vec![fixed(1)])],
        InferencerExecutionMode::PerMessage,
        Some(&state),
    )
    .await;
    let publication = publication();
    let carried = CarriedState::restore(
        publication,
        prepared
            .state
            .as_ref()
            .verified("the definition carries STATE"),
        1,
        1024,
        state_metrics(),
    );
    let mut session = InferenceSession::new(Some(carried));
    session
        .load(executor, &model("checked_running_sum.onnx"), &prepared)
        .await
        .expect("the checked running sum loads");
    (prepared, session)
}

fn features(rows: &[[f32; 2]]) -> RuntimeRecordBatch {
    mapped(
        rows.iter()
            .map(|row| vec![("features", f32s(row))])
            .collect(),
    )
}

fn carried_values(session: &InferenceSession) -> Vec<f32> {
    let generation = session
        .carried()
        .verified("the session carries STATE")
        .generation();
    let (_, values) = generation.tensor(0).expect("the carried F32 tensor");
    values.to_vec()
}

#[nervix_primitives::test]
async fn a_model_loads_on_the_bulk_workers_and_runs_on_the_extension_workers() {
    let executor = Executor::default();
    let matrix = vec![fixed(2), fixed(3)];
    let prepared = prepared(
        &executor,
        vec![port("matrix", matrix.clone())],
        vec![output("transformed", matrix)],
        InferencerExecutionMode::PerMessage,
        None,
    )
    .await;
    let mut session = InferenceSession::new(None);
    session
        .load(&executor, &model("matrix_identity.onnx"), &prepared)
        .await
        .expect("the identity model loads");
    assert_eq!(executor.snapshot().bulk_cpu.completed, 1);

    let matrix = RuntimeValue::Array(vec![f32s(&[1.0, 2.0, 3.0]), f32s(&[4.0, 5.0, 6.0])]);
    let run = session
        .run(
            &executor,
            &prepared,
            &mapped(vec![vec![("matrix", matrix)]]),
        )
        .await;

    assert_eq!(run.accepted, vec![0]);
    assert!(run.refused.is_empty() && run.failed.is_empty());
    let outputs = run.outputs.expect("the accepted message has outputs");
    assert_eq!(
        output_values(&outputs, 0, 0),
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    );
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.extension_cpu.admitted, 1);
    assert_eq!(snapshot.extension_cpu.completed, 1);
    assert_eq!(snapshot.data_cpu.admitted, 0);
    assert_eq!(snapshot.bulk_memory.reserved_bytes, 0);
    assert_eq!(snapshot.relay_memory.reserved_bytes, 0);
}

/// The identity model's definition, whose sessions keep `working_memory`.
async fn identity_keeping(
    executor: &Executor,
    working_memory: nervix_models::InferencerWorkingMemory,
) -> PreparedInference {
    let matrix = vec![fixed(2), fixed(3)];
    PreparedInference::prepare(
        executor,
        &[port("matrix", matrix.clone())],
        &[output("transformed", matrix)],
        InferencerExecutionMode::PerMessage,
        working_memory,
        None,
        None,
        nervix_vm::VmSettings::default(),
    )
    .await
    .expect("the identity definition prepares")
}

#[nervix_primitives::test]
async fn a_definition_that_changes_only_its_working_memory_is_another_binding() {
    let executor = Executor::default();
    let released =
        identity_keeping(&executor, nervix_models::InferencerWorkingMemory::Released).await;
    let released_again =
        identity_keeping(&executor, nervix_models::InferencerWorkingMemory::Released).await;
    let retained =
        identity_keeping(&executor, nervix_models::InferencerWorkingMemory::Retained).await;
    // The same policy is the same binding, so a branch keeps the session it loaded.
    assert!(released.same_binding(&released_again));
    // Another policy is another binding, so every branch loads its model again, into a session
    // that keeps the new working memory, rather than keeping the session built for the old one.
    assert!(!released.same_binding(&retained));
    assert!(!retained.same_binding(&released));
}

#[nervix_primitives::test]
async fn one_admitted_job_runs_every_message_of_a_batch_one_at_a_time() {
    let executor = Executor::default();
    let prepared = prepared(
        &executor,
        vec![port("features", vec![fixed(2)])],
        vec![output("score", vec![fixed(1)])],
        InferencerExecutionMode::PerMessage,
        None,
    )
    .await;
    let mut session = InferenceSession::new(None);
    session
        .load(&executor, &model("simple_score.onnx"), &prepared)
        .await
        .expect("the per-message model loads");
    let run = session
        .run(
            &executor,
            &prepared,
            &features(&[[1.0, 0.0], [0.0, 1.0], [2.0, 1.0]]),
        )
        .await;
    assert_eq!(run.accepted, vec![0, 1, 2]);
    let outputs = run.outputs.expect("three accepted messages");
    assert_eq!(output_values(&outputs, 0, 0), vec![0.875]);
    assert_eq!(output_values(&outputs, 0, 1), vec![-0.375]);
    assert_eq!(output_values(&outputs, 0, 2), vec![1.125]);
    assert_eq!(executor.snapshot().extension_cpu.completed, 1);
}

#[nervix_primitives::test]
async fn an_accepted_run_waits_for_extension_room_and_keeps_its_rows() {
    let executor = single_worker_executor();
    let prepared = prepared(
        &executor,
        vec![port("features", vec![fixed(2)])],
        vec![output("score", vec![fixed(1)])],
        InferencerExecutionMode::PerMessage,
        None,
    )
    .await;
    let mut session = InferenceSession::new(None);
    session
        .load(&executor, &model("simple_score.onnx"), &prepared)
        .await
        .expect("the per-message model loads");
    let filled = FilledCpuClass::fill(&executor, CpuClass::Extension).await;
    let batch = features(&[[1.0, 0.0]]);
    let mut run = Box::pin(session.run(&executor, &prepared, &batch));
    assert!(
        futures_util::poll!(run.as_mut()).is_pending(),
        "an accepted model run waits without becoming an inference failure"
    );
    filled.release().await;
    let run = run.await;
    assert_eq!(run.accepted, vec![0]);
    assert_eq!(executor.snapshot().extension_cpu.refused, 0);
}

#[nervix_primitives::test]
async fn the_first_batch_waits_for_bulk_room_to_load_its_session() {
    let executor = single_worker_executor();
    let prepared = prepared(
        &executor,
        vec![port("features", vec![fixed(2)])],
        vec![output("score", vec![fixed(1)])],
        InferencerExecutionMode::PerMessage,
        None,
    )
    .await;
    let mut session = InferenceSession::new(None);
    let path = model("simple_score.onnx");
    crate::runtime::test_fixtures::accepted_work_after_cpu_room(
        &executor,
        CpuClass::Bulk,
        session.load(&executor, &path, &prepared),
    )
    .await
    .assured("the retained batch waits for its session");
    assert!(!session.needs_load());
    assert_eq!(executor.snapshot().bulk_cpu.refused, 0);
}

#[nervix_primitives::test]
async fn a_refused_message_fails_alone_and_leaves_the_carried_state() {
    let executor = Executor::default();
    let (prepared, mut session) = checked_running(&executor).await;
    // A first feature of 2 asks the fixture to reshape its one-element score into two elements.
    let run = session
        .run(
            &executor,
            &prepared,
            &features(&[[1.0, 10.0], [2.0, 100.0], [1.0, 30.0]]),
        )
        .await;
    assert_eq!(run.accepted, vec![0, 2]);
    assert_eq!(run.refused.len(), 1);
    assert_eq!(run.refused[0].row, 1);
    assert!(matches!(
        run.refused[0].refusal,
        MessageRefusal::Model { .. }
    ));
    assert!(run.failed.is_empty());
    assert!(run.advanced);
    let outputs = run.outputs.expect("two accepted messages");
    assert_eq!(output_values(&outputs, 0, 0), vec![11.0]);
    assert_eq!(output_values(&outputs, 0, 1), vec![42.0]);
    assert_eq!(carried_values(&session), vec![2.0, 40.0]);
}

#[nervix_primitives::test]
async fn without_state_a_refused_message_fails_alone() {
    let executor = Executor::default();
    let prepared = prepared(
        &executor,
        vec![
            port("features", vec![fixed(2)]),
            port("state_in", vec![fixed(2)]),
        ],
        vec![
            output("score", vec![fixed(1)]),
            output("state_out", vec![fixed(2)]),
        ],
        InferencerExecutionMode::PerMessage,
        None,
    )
    .await;
    let mut session = InferenceSession::new(None);
    session
        .load(&executor, &model("checked_running_sum.onnx"), &prepared)
        .await
        .expect("the checked running sum loads with ordinary state ports");
    let rows = [[1.0, 10.0], [2.0, 100.0], [1.0, 30.0]]
        .iter()
        .map(|row| vec![("features", f32s(row)), ("state_in", f32s(&[0.0, 0.0]))])
        .collect();
    let run = session.run(&executor, &prepared, &mapped(rows)).await;
    assert_eq!(run.accepted, vec![0, 2]);
    assert_eq!(run.refused.len(), 1);
    assert_eq!(run.refused[0].row, 1);
    assert!(!run.advanced);
    let outputs = run.outputs.expect("two accepted messages");
    assert_eq!(output_values(&outputs, 0, 0), vec![11.0]);
    assert_eq!(output_values(&outputs, 0, 1), vec![31.0]);
}

#[nervix_primitives::test]
async fn a_batched_run_groups_messages_by_shape_and_keeps_their_arrival_order() {
    let executor = Executor::default();
    let sequence_axes = vec![
        InferencerTensorDimension::Batch,
        InferencerTensorDimension::Dynamic,
        fixed(2),
    ];
    let prepared = prepared(
        &executor,
        vec![
            port("features", sequence_axes.clone()),
            port("mask", sequence_axes.clone()),
        ],
        vec![output("scores", sequence_axes)],
        InferencerExecutionMode::Batched,
        None,
    )
    .await;
    let mut session = InferenceSession::new(None);
    session
        .load(&executor, &model("dynamic_batch_score.onnx"), &prepared)
        .await
        .expect("the dynamic batch model loads");
    let batch = mapped(vec![
        vec![
            ("features", sequence(&[[1.0, 10.0]])),
            ("mask", sequence(&[[100.0, 1000.0]])),
        ],
        vec![
            ("features", sequence(&[[2.0, 20.0], [4.0, 40.0]])),
            ("mask", sequence(&[[200.0, 2000.0], [400.0, 4000.0]])),
        ],
        vec![
            ("features", sequence(&[[3.0, 30.0]])),
            ("mask", sequence(&[[300.0, 3000.0]])),
        ],
    ]);
    let run = session.run(&executor, &prepared, &batch).await;
    assert_eq!(run.accepted, vec![0, 1, 2]);
    let outputs = run.outputs.expect("three accepted messages");
    // The one-step sequences share one call, whose batch-wide mean feature is (2, 20).
    assert_eq!(output_values(&outputs, 0, 0), vec![103.0, 1030.0]);
    assert_eq!(
        output_values(&outputs, 0, 1),
        vec![204.0, 2040.0, 408.0, 4080.0]
    );
    assert_eq!(output_values(&outputs, 0, 2), vec![305.0, 3050.0]);
    assert_eq!(executor.snapshot().extension_cpu.completed, 1);
}

#[nervix_primitives::test]
async fn a_batched_call_the_model_refuses_fails_its_messages_together() {
    let executor = Executor::default();
    let sequence_axes = vec![
        InferencerTensorDimension::Batch,
        InferencerTensorDimension::Dynamic,
        fixed(2),
    ];
    let prepared = prepared(
        &executor,
        vec![
            port("features", sequence_axes.clone()),
            port("mask", sequence_axes.clone()),
        ],
        vec![output("scores", sequence_axes)],
        InferencerExecutionMode::Batched,
        None,
    )
    .await;
    let mut session = InferenceSession::new(None);
    session
        .load(&executor, &model("dynamic_batch_score.onnx"), &prepared)
        .await
        .expect("the dynamic batch model loads");
    // Two steps cannot be added to three, so the model refuses the second group's call.
    let batch = mapped(vec![
        vec![
            ("features", sequence(&[[1.0, 10.0]])),
            ("mask", sequence(&[[100.0, 1000.0]])),
        ],
        vec![
            ("features", sequence(&[[2.0, 20.0], [4.0, 40.0]])),
            ("mask", sequence(&[[1.0, 1.0], [2.0, 2.0], [3.0, 3.0]])),
        ],
    ]);
    let run = session.run(&executor, &prepared, &batch).await;
    assert_eq!(run.accepted, vec![0]);
    assert_eq!(run.failed.len(), 1);
    assert_eq!(run.failed[0].rows, vec![1]);
    assert!(matches!(
        run.failed[0].error.current_context(),
        InferencerError::Refused { .. }
    ));
    let outputs = run.outputs.expect("the first group's message");
    assert_eq!(output_values(&outputs, 0, 0), vec![102.0, 1020.0]);
}

#[test]
fn messages_join_and_split_around_a_non_leading_batch_axis() {
    let slices = [
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
    ];
    let borrowed = slices.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let mut joined = vec![0.0; 12];
    join_batch(&mut joined, &borrowed, &[2, 3], 1).expect("two messages of shape [2, 3]");
    assert_eq!(
        joined,
        vec![
            1.0, 2.0, 3.0, 10.0, 20.0, 30.0, 4.0, 5.0, 6.0, 40.0, 50.0, 60.0
        ]
    );
    let mut gathered = Vec::new();
    for (position, slice) in slices.iter().enumerate() {
        let shape = batch_slice(&joined, &[2, 2, 3], 1, position, &mut gathered);
        assert_eq!(shape, vec![2, 3]);
        assert_eq!(&gathered, slice);
    }
    let mut short = vec![0.0; 12];
    assert!(join_batch(&mut short, &[&slices[0], &[1.0]], &[2, 3], 1).is_err());
}

#[nervix_primitives::test]
async fn a_message_too_large_for_the_relay_limit_fails_alone_and_the_rest_are_chunked() {
    let executor = Executor::default();
    let prepared = prepared(
        &executor,
        vec![port("values", vec![InferencerTensorDimension::Dynamic])],
        vec![output("echo", vec![InferencerTensorDimension::Dynamic])],
        InferencerExecutionMode::PerMessage,
        None,
    )
    .await;
    let batch = mapped(vec![
        vec![(
            "values",
            RuntimeValue::Vec(vec![RuntimeValue::F32(OrderedFloat(1.0)); 2]),
        )],
        vec![(
            "values",
            RuntimeValue::Vec(vec![RuntimeValue::F32(OrderedFloat(1.0)); 64]),
        )],
        vec![(
            "values",
            RuntimeValue::Vec(vec![RuntimeValue::F32(OrderedFloat(1.0)); 2]),
        )],
        vec![(
            "values",
            RuntimeValue::Vec(vec![RuntimeValue::F32(OrderedFloat(1.0)); 2]),
        )],
    ]);
    // A message's own tensor sizes it, never a row slice whose list values the whole batch shares.
    let vector = &prepared.inputs[0].schema;
    let small = 2 * TensorColumn::message_bytes(vector, &[2], 2).expect("a pair's bytes");
    let large = 2 * TensorColumn::message_bytes(vector, &[64], 64).expect("64 values' bytes");
    // Two small messages fit one job; the large one fits none, and the whole batch fits no job.
    let limit = 2 * small;
    assert!(large > limit);
    assert!(2 * batch.estimated_bytes() > limit);
    let mut run = InferenceRun::default();
    let chunks = InferenceSession::plan_chunks(&prepared, &batch, limit, &mut run);
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.rows.clone())
            .collect::<Vec<_>>(),
        vec![vec![0, 2], vec![3]]
    );
    assert_eq!(run.failed.len(), 1);
    assert_eq!(run.failed[0].rows, vec![1]);
    assert!(matches!(
        run.failed[0].error.current_context(),
        InferencerError::WorkingSet
    ));
}

#[nervix_primitives::test]
async fn a_batch_whose_working_set_fits_the_limit_runs_as_one_job() {
    let executor = Executor::default();
    let prepared = prepared(
        &executor,
        vec![port("values", vec![InferencerTensorDimension::Dynamic])],
        vec![output("echo", vec![InferencerTensorDimension::Dynamic])],
        InferencerExecutionMode::PerMessage,
        None,
    )
    .await;
    let pair = || {
        vec![(
            "values",
            RuntimeValue::Vec(vec![RuntimeValue::F32(OrderedFloat(1.0)); 2]),
        )]
    };
    let batch = mapped(vec![pair(), pair(), pair()]);
    let whole = 2 * batch.estimated_bytes();
    let mut run = InferenceRun::default();
    let chunks = InferenceSession::plan_chunks(&prepared, &batch, whole, &mut run);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].rows, vec![0, 1, 2]);
    assert_eq!(chunks[0].charge, whole);
    assert!(run.failed.is_empty());
}

/// An executor whose relay operation limit is `limit` bytes, so a batch whose messages together
/// exceed it runs as several admitted jobs.
fn executor_with_relay_limit(limit: u64) -> Executor {
    Executor::new(ExecutionConfig {
        limits: nervix_execution::OperationLimits {
            relay_decoded_bytes: ubyte::ByteUnit::Byte(limit),
            ..nervix_execution::OperationLimits::default()
        },
        ..ExecutionConfig::default()
    })
    .expect("a smaller relay operation limit keeps the default budgets valid")
}

/// One output batch that the checked running sum's declared `score` column cannot be joined with:
/// its column holds plain `F32` values instead of one-element tensors.
fn unjoinable_output() -> RecordBatch {
    let schema = StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "score",
        arrow_schema::DataType::Float32,
        false,
    )]));
    let column: arrow_array::ArrayRef = StdArc::new(Float32Array::from(vec![0.0_f32]));
    RecordBatch::try_new(schema, vec![column]).expect("the column matches its own schema")
}

#[nervix_primitives::test]
async fn messages_that_fail_because_their_jobs_outputs_cannot_be_joined_leave_the_carried_state() {
    let executor = Executor::default();
    let (prepared, mut session) = checked_running(&executor).await;
    let first = session
        .run(&executor, &prepared, &features(&[[1.0, 10.0]]))
        .await;
    assert!(first.advanced);
    assert_eq!(carried_values(&session), vec![1.0, 10.0]);

    // The relay operation limit holds one message's working set and not two, so each message of
    // the batch runs as its own admitted job. A first feature of 1 keeps the fixture's one-element
    // score.
    let batch = features(&[[1.0, 20.0], [1.0, 30.0], [1.0, 40.0]]);
    let row_bytes = prepared
        .message_bytes(&batch, 0)
        .expect("the message's tensors are readable");
    let one_message = prepared
        .message_working_bytes(row_bytes)
        .expect("one message's working set fits in u64");
    let mut planned = InferenceRun::default();
    let chunks = InferenceSession::plan_chunks(&prepared, &batch, one_message, &mut planned);
    assert_eq!(chunks.len(), 3);
    let narrow = executor_with_relay_limit(one_message);
    let mut jobs = session.run_jobs(&narrow, &prepared, &batch).await;
    assert_eq!(
        jobs.run.accepted,
        vec![0, 1, 2],
        "refused {:?}, failed {:?}",
        jobs.run.refused,
        jobs.run.failed
    );
    assert_eq!(jobs.outputs.len(), 3);

    // Joining several jobs' outputs fails only when the joined columns would overflow their
    // offsets, which takes more tensor values than a test can allocate. An output the declared
    // schema cannot join with makes the same join fail.
    jobs.outputs.push(unjoinable_output());
    let run = session.join_outputs(&prepared, jobs);
    assert!(run.outputs.is_none());
    assert!(run.accepted.is_empty());
    assert_eq!(run.failed.len(), 1);
    assert_eq!(run.failed[0].rows, vec![0, 1, 2]);
    assert!(matches!(
        run.failed[0].error.current_context(),
        InferencerError::OutputBatch
    ));
    // Admission publishes a generation only for a run that advanced the carried state.
    assert!(!run.advanced);
    assert_eq!(carried_values(&session), vec![1.0, 10.0]);

    // The failed messages come again, and each counts once.
    let again = session.run(&narrow, &prepared, &batch).await;
    assert!(again.advanced);
    assert_eq!(again.accepted, vec![0, 1, 2]);
    let outputs = again.outputs.expect("three accepted messages");
    assert_eq!(output_values(&outputs, 0, 0), vec![32.0]);
    assert_eq!(output_values(&outputs, 0, 1), vec![63.0]);
    assert_eq!(output_values(&outputs, 0, 2), vec![104.0]);
    assert_eq!(carried_values(&session), vec![4.0, 100.0]);
}

#[nervix_primitives::test]
async fn abandoning_a_full_queue_wait_keeps_the_branch_session_and_its_state() {
    let mut config = ExecutionConfig::default();
    config.workers.extension_cpu = NonZeroUsize::MIN;
    config.workers.pending_jobs = nervix_execution::PendingJobCounts::uniform(NonZeroUsize::MIN);
    let executor = Executor::new(config).expect("one extension worker with one queue place");
    let (prepared, mut session) = checked_running(&executor).await;
    let first = session
        .run(&executor, &prepared, &features(&[[1.0, 10.0]]))
        .await;
    assert_eq!(first.accepted, vec![0]);
    let (started, has_started) = nervix_primitives::sync::oneshot::channel();
    let (release, released) = nervix_primitives::sync::oneshot::channel();
    let occupied = executor.clone();
    let occupying_charge = executor
        .try_reserve(MemoryClass::Relay, 1)
        .expect("the occupying job has a charge");
    let occupying = nervix_primitives::task::spawn(async move {
        occupied
            .run_cpu(CpuClass::Extension, occupying_charge, move |_charge, _| {
                started.send(()).expect("the test waits for occupation");
                released
                    .blocking_recv()
                    .expect("the test releases its worker");
            })
            .await
    });
    has_started
        .await
        .expect("the sole extension worker is occupied");
    let queued_executor = executor.clone();
    let queued_charge = executor
        .try_reserve(MemoryClass::Relay, 1)
        .expect("the queued job has a charge");
    let queued = nervix_primitives::task::spawn(async move {
        queued_executor
            .run_cpu(CpuClass::Extension, queued_charge, |_charge, _| ())
            .await
    });
    while executor.snapshot().extension_cpu.pending != 1 {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::task::yield_now().await;
    }
    let before = session
        .carried()
        .verified("the session carries STATE")
        .generation();
    let batch = features(&[[1.0, 1000.0]]);
    let mut waiting = Box::pin(session.run(&executor, &prepared, &batch));
    let pending = futures_util::poll!(waiting.as_mut()).is_pending();
    // Abandon the full-queue wait without moving the branch's native session into a job.
    drop(waiting);
    release
        .send(())
        .expect("the occupying job retains its release receiver");
    occupying
        .await
        .expect("the occupying task joins")
        .expect("the occupying job exits");
    queued
        .await
        .expect("the queued task joins")
        .expect("the queued job exits");
    assert!(pending, "an accepted inference waits for a place");
    assert!(
        !session.needs_load(),
        "abandoning the wait retains the branch's native session"
    );
    assert!(Arc::ptr_eq(
        &before,
        &session
            .carried()
            .verified("the session carries STATE")
            .generation()
    ));
    let next = session
        .run(&executor, &prepared, &features(&[[1.0, 30.0]]))
        .await;
    let outputs = next
        .outputs
        .expect("the next message runs on the retained session");
    assert_eq!(output_values(&outputs, 0, 0), vec![42.0]);
    assert_eq!(carried_values(&session), vec![2.0, 40.0]);
}

#[nervix_primitives::test]
async fn a_native_refusal_reaches_the_log_without_tensor_values() {
    let executor = Executor::default();
    let (prepared, _) = checked_running(&executor).await;
    let logs = crate::captured_logs::CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .with_max_level(tracing::Level::ERROR)
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let definition = prepared.clone();
    let generation = prepared
        .state
        .as_ref()
        .verified("the definition carries STATE")
        .initial
        .clone();
    let path = model("checked_running_sum.onnx");
    let batch = features(&[[314159.0, 10.0]]);
    let limit = executor.limits().relay_decoded_bytes.as_u64();
    let charge = executor
        .try_reserve(MemoryClass::Relay, 4096)
        .expect("the tiny diagnostic invocation has bounded working memory");
    let outcome = executor
        .run_cpu(CpuClass::Extension, charge, move |_charge, cancellation| {
            tracing::dispatcher::with_default(&dispatch, || {
                let mut bound = BoundSession::load(&path, &definition, cancellation)
                    .expect("the production owner loads the diagnostic fixture");
                bound.run_messages(
                    &definition,
                    Some(&*generation),
                    &batch,
                    &[0],
                    limit,
                    cancellation,
                )
            })
        })
        .await
        .expect("the diagnostic invocation is admitted");
    assert_eq!(
        outcome.refused.len(),
        1,
        "the requested shape refuses this row"
    );
    let diagnostics = logs.contents();
    assert!(
        !diagnostics.is_empty(),
        "the native refusal must retain its diagnostic"
    );
    assert!(
        diagnostics.lines().all(|line| {
            line.contains("native ONNX diagnostic")
                && line.contains("native_level=Error")
                && line.contains("native_site=")
        }),
        "native diagnostics must project a static message, severity and code site: {diagnostics}"
    );
    assert!(
        !diagnostics.contains("314159"),
        "a native diagnostic must not expose the data-derived dimension"
    );
}
