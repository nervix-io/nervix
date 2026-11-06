//! Direct evidence for the branch-owned native inference owner.
//!
//! Layer: test harness.
//! Owns: sliced Arrow input ownership, public output layout and refusal atomicity.
//! Depends on: the production stateful owner and the generated ONNX fixture.
//! Must not know: graph scheduling or registry implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks native tensor ownership and current representations"
    )
)]

use std::num::{NonZeroU32, NonZeroUsize};

use arrow_array::{Array as _, FixedSizeListArray, Float32Array};
use nervix_execution::{ExecutionConfig, Executor};
use nervix_models::{
    Expression, InferencerTensorElementType, InferencerTensorRepresentation, Literal,
};

use super::*;
use crate::registry::InferencerStatePortPlan;

fn schema(dimensions: &[u32]) -> InferencerTensorSchema {
    InferencerTensorSchema {
        representation: InferencerTensorRepresentation::Dense,
        element_type: InferencerTensorElementType::F32,
        dimensions: dimensions
            .iter()
            .map(|size| {
                InferencerTensorDimension::Fixed(
                    NonZeroU32::new(*size).expect("test axes are positive"),
                )
            })
            .collect(),
    }
}
fn row(values: &[f32]) -> RuntimeRecordBatch {
    let tensor = schema(&[2]);
    let data_type = tensor.message_type().arrow_data_type();
    let arrow_schema::DataType::FixedSizeList(field, length) = &data_type else {
        unreachable!()
    };
    let array = FixedSizeListArray::try_new(
        field.clone(),
        *length,
        StdArc::new(Float32Array::from(values.to_vec())),
        None,
    )
    .expect("the fixture consists of pairs");
    let schema = StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "features", data_type, false,
    )]));
    RuntimeRecordBatch::from_record_batch(
        schema.clone(),
        RecordBatch::try_new(schema, vec![StdArc::new(array)]).expect("the tensor column matches"),
    )
    .expect("the Arrow schema is exact")
}
fn score(batch: &RuntimeRecordBatch) -> f32 {
    let column = batch
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("score is one fixed array");
    let values = column.value(0);
    values
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("score elements are F32")
        .value(0)
}

#[test]
fn a_sliced_tensor_borrows_its_original_arrow_values() {
    let batch = row(&[100.0, 1000.0, 1.0, 10.0]);
    let values = batch
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("pairs")
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("F32")
        .clone();
    let selected = batch.slice(1, 1).expect("the second input row exists");
    let input = ArrowTensorInput::from_row(&selected, 0, "features", &schema(&[2]))
        .expect("one valid tensor row");
    assert_eq!(input.shape, [2]);
    assert_eq!(input.values.values().as_ref(), &[1.0, 10.0]);
    assert_eq!(
        input.values.values().as_ptr(),
        values.values().as_ptr().wrapping_add(2)
    );
    let native = input
        .to_native()
        .expect("the model input owns its native allocation");
    drop(input);
    drop(selected);
    drop(values);
    drop(batch);
    assert_eq!(
        native
            .try_extract_tensor::<f32>()
            .expect("native input outlives every Arrow owner")
            .1,
        &[1.0, 10.0]
    );
}

#[test]
fn public_output_conversion_preserves_float_bits_and_nested_offsets() {
    let mut tensor = schema(&[2]);
    tensor.dimensions.push(InferencerTensorDimension::Dynamic);
    let values = [f32::from_bits(0x7fc0_1234), -0.0, 1.0, 2.0, 3.0, 4.0];
    assert_eq!(output_arrow_bytes(&tensor, &[2, 3], values.len()), Some(36));
    let array = output_tensor_array(
        &tensor,
        &[2, 3],
        &values,
        &tensor.message_type().arrow_data_type(),
    )
    .expect("the public tensor has bounded offsets");
    assert_eq!(array.data_type(), &tensor.message_type().arrow_data_type());
    let rows = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("fixed outer axis")
        .value(0);
    let lists = rows
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .expect("dynamic inner axis");
    assert_eq!(lists.value_offsets(), &[0, 3, 6]);
    let actual = lists
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("F32 leaves");
    assert_eq!(
        actual
            .values()
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        values
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    );
}

#[test]
fn bolero_stateful_tensor_columns_preserve_shape_and_float_bits() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            for rank in 0..=4 {
                let mut entropy = nervix_arbitrary::Entropy::new(bytes);
                let mut tensor = schema(&[]);
                let mut shape = Vec::with_capacity(rank);
                for axis in 0..rank {
                    let dynamic = entropy.flag();
                    let size = if dynamic && axis + 1 == rank {
                        usize::from(entropy.byte() % 5)
                    } else {
                        1 + usize::from(entropy.byte() % 4)
                    };
                    shape.push(size);
                    tensor.dimensions.push(if dynamic {
                        InferencerTensorDimension::Dynamic
                    } else {
                        InferencerTensorDimension::Fixed(
                            NonZeroU32::new(u32::try_from(size).expect("bounded axis"))
                                .expect("fixed axes are positive"),
                        )
                    });
                }
                let count = shape.iter().product::<usize>();
                let values = (0..count)
                    .map(|_| {
                        f32::from_bits(u32::from_le_bytes([
                            entropy.byte(),
                            entropy.byte(),
                            entropy.byte(),
                            entropy.byte(),
                        ]))
                    })
                    .collect::<Vec<_>>();
                let data_type = tensor.message_type().arrow_data_type();
                let array = output_tensor_array(&tensor, &shape, &values, &data_type)
                    .expect("bounded current dense tensors encode");
                assert_eq!(array.data_type(), &data_type);
                assert_eq!(array.null_count(), 0);
                let arrow_schema =
                    StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                        "features", data_type, false,
                    )]));
                let record_batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
                    .expect("one ordered tensor column");
                let batch =
                    RuntimeRecordBatch::from_record_batch(arrow_schema.clone(), record_batch)
                        .expect("the exact current tensor schema");
                assert_eq!(batch.schema(), arrow_schema);
                assert_eq!(batch.batch().num_rows(), 1);
                let decoded = ArrowTensorInput::from_row(&batch, 0, "features", &tensor)
                    .expect("the complete tensor decodes");
                assert_eq!(decoded.shape, shape);
                assert_eq!(
                    decoded
                        .values
                        .values()
                        .iter()
                        .map(|value| value.to_bits())
                        .collect::<Vec<_>>(),
                    values
                        .iter()
                        .map(|value| value.to_bits())
                        .collect::<Vec<_>>()
                );
            }
        });
}

#[nervix_primitives::test]
async fn native_refusal_keeps_the_previous_generation_and_other_branches_independent() {
    let mut config = ExecutionConfig::default();
    config.workers.extension_cpu = NonZeroUsize::MIN;
    config.workers.pending_jobs = NonZeroUsize::MIN;
    let executor = Executor::new(config).expect("one extension worker with one queue place");
    let state = InferencerStatePlan {
        bindings: vec![InferencerStatePortPlan {
            input: "state_in".to_owned(),
            output: "state_out".to_owned(),
            schema: schema(&[2]),
            elements: 2,
            initial: InferencerStateInitial::Zeros,
        }],
        bytes_per_branch: 8,
    };
    let prepared = Arc::new(
        PreparedStatefulInference::prepare(
            &executor,
            &[InferencerTensorMapping {
                tensor: "features".to_owned(),
                schema: schema(&[2]),
                expression: Expression::Literal(Literal::Null),
            }],
            &[InferencerTensorDeclaration {
                tensor: "score".to_owned(),
                schema: schema(&[1]),
            }],
            &state,
            None,
        )
        .await
        .expect("INITIAL is prepared on the bounded workers"),
    );
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/onnx/checked_running_sum.onnx");
    let mut first = prepared.instantiate();
    let mut second = prepared.instantiate();
    let first_result = first
        .execute(&executor, &path, &prepared, row(&[1.0, 10.0]))
        .await
        .expect("admitted")
        .expect("valid reshape");
    assert_eq!(score(&first_result), 11.0);
    let second_result = second
        .execute(&executor, &path, &prepared, row(&[1.0, 100.0]))
        .await
        .expect("admitted")
        .expect("valid reshape");
    assert_eq!(score(&second_result), 101.0);
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
    let before_admission = first.generation.clone();
    let admission = nervix_primitives::time::timeout(
        std::time::Duration::from_secs(5),
        first.execute(&executor, &path, &prepared, row(&[1.0, 1000.0])),
    )
    .await;
    // Release and join before asserting, including when an implementation waited outside the queue.
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
    let admission = admission
        .expect("full queue refusal does not await the occupied worker")
        .expect_err("a full extension queue refuses before evaluating the row");
    assert!(matches!(
        admission.current_context(),
        StatefulInferenceError::Execution
    ));
    assert!(admission.frames().any(|frame| matches!(
        frame.downcast_ref::<nervix_execution::ExecutionError>(),
        Some(nervix_execution::ExecutionError::QueueFull {
            class: "extension_cpu",
            ..
        })
    )));
    assert!(Arc::ptr_eq(&before_admission, &first.generation));
    assert_native_diagnostic_projection(&executor, &path, &prepared).await;
    let before = first.generation.clone();
    let refused = first
        .execute(&executor, &path, &prepared, row(&[2.0, 100.0]))
        .await
        .expect("the row reaches the model");
    assert!(
        refused.is_err(),
        "reshaping a scalar sum into two elements is refused"
    );
    assert!(
        Arc::ptr_eq(&before, &first.generation),
        "no part of a refused successor is installed"
    );
    let next = first
        .execute(&executor, &path, &prepared, row(&[1.0, 30.0]))
        .await
        .expect("admitted")
        .expect("valid reshape");
    assert_eq!(score(&next), 42.0);
    let (_, native_values) = first.generation.tensors[0]
        .try_extract_tensor::<f32>()
        .expect("carried native F32 state");
    assert_eq!(native_values, &[2.0, 40.0]);
    let second_next = second
        .execute(&executor, &path, &prepared, row(&[1.0, 100.0]))
        .await
        .expect("admitted")
        .expect("valid reshape");
    assert_eq!(score(&second_next), 202.0);
    let mut recreated = prepared.instantiate();
    assert_eq!(
        score(
            &recreated
                .execute(&executor, &path, &prepared, row(&[1.0, 10.0]))
                .await
                .expect("admitted")
                .expect("valid reshape")
        ),
        11.0
    );
}

async fn assert_native_diagnostic_projection(
    executor: &Executor,
    path: &Path,
    prepared: &Arc<PreparedStatefulInference>,
) {
    let logs = crate::captured_logs::CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .with_max_level(tracing::Level::ERROR)
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let mut session = BoundSession::load(executor, path, prepared)
        .await
        .expect("the production owner loads the diagnostic fixture");
    let definition = prepared.clone();
    let generation = prepared.initial.clone();
    let input = row(&[314159.0, 10.0]);
    let limit = executor.limits().relay_decoded_bytes.as_u64();
    let charge = executor
        .try_reserve(MemoryClass::Relay, 4096)
        .expect("the tiny diagnostic invocation has bounded working memory");
    let result = executor
        .run_cpu(CpuClass::Extension, charge, move |_charge, cancellation| {
            tracing::dispatcher::with_default(&dispatch, || {
                session.run(&definition, &generation, &input, limit, cancellation)
            })
        })
        .await
        .expect("the diagnostic invocation is admitted");
    assert!(result.is_err(), "the requested shape refuses this row");
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
