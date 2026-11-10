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

/// The fixture's successive cumulative tensors each stand for one acknowledged row. Native
/// values are prepared outside the model; publication/cut ordering uses selected primitives.
#[cfg(feature = "shuttle")]
#[test]
fn shuttle_a_forced_inferencer_publication_holds_every_row_acknowledged_before_the_cut() {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_primitives::sync::{mpsc, oneshot, watch};

    use crate::runtime::{
        CapturedInferencer, RuntimeState, RuntimeStatePlacement,
        inferencer_state::ReplicatedInferencerState, published_generation::PublishedGenerations,
    };

    let tensors = [vec![1.0, 10.0], vec![3.0, 30.0]].map(|values| {
        Arc::new(NativeGeneration {
            tensors: vec![
                Tensor::<f32>::from_array(([2], values))
                    .assured("the fixed F32 fixture builds")
                    .into_dyn(),
            ],
        })
    });
    nervix_model_harness::shuttle::check_interleavings(move || {
        let tensors = tensors.clone();
        shuttle::future::block_on(async move {
            let placement = RuntimeStatePlacement {
                domain: nervix_models::DomainName::parse("inference").assured("a valid domain"),
                state: RuntimeState::Inferencer {
                    schema: nervix_models::SchemaFingerprint::from_digest([7; 32]),
                    model: nervix_models::InferencerModelFingerprint::from_digest([9; 32]),
                },
                kind: nervix_models::ModelKind::Inferencer,
                identifier: nervix_models::ModelName::parse("running")
                    .assured("a valid model name"),
                unit: nervix_models::Unit::SINGLE,
                branch_key: None,
            };
            let state = Arc::new(ReplicatedInferencerState::from_generations(
                placement,
                PublishedGenerations::restored(0, None),
            ));
            let (updates, mut rows) =
                mpsc::channel::<(Arc<NativeGeneration>, oneshot::Sender<()>)>(1);
            let (requests, mut cuts) = mpsc::channel::<oneshot::Sender<()>>(1);
            let (acknowledged, mut acknowledgements) = watch::channel(0u64);
            let branch_state = state.clone();
            let branch = nervix_primitives::task::spawn(async move {
                let mut session = None;
                let (mut rows_open, mut cuts_open) = (true, true);
                while rows_open || cuts_open {
                    nervix_primitives::task::consume_budget().await;
                    nervix_primitives::select! {
                        biased;
                        cut = cuts.recv(), if cuts_open => match cut {
                            Some(answer) => {
                                if let Some(session) = &session { branch_state.publish_session(session, 17, 1024); }
                                answer.send(()).assured("the cut waits for forced publication");
                            }
                            None => cuts_open = false,
                        },
                        row = rows.recv(), if rows_open => match row {
                            Some((generation, answer)) => {
                                let current = StatefulInferenceSession { session: None, generation };
                                branch_state.publish_session(&current, 17, 1024);
                                session = Some(current);
                                answer.send(()).assured("the source waits for its admitted row");
                            }
                            None => rows_open = false,
                        },
                    }
                }
            });
            let source = nervix_primitives::task::spawn(async move {
                for (index, generation) in tensors.into_iter().enumerate() {
                    let (answer, applied) = oneshot::channel();
                    updates
                        .send((generation, answer))
                        .await
                        .assured("the branch receives both rows");
                    applied.await.assured("the branch acknowledges publication");
                    acknowledged.send_replace(u64::try_from(index + 1).assured("two rows fit"));
                }
            });
            let before = *acknowledgements
                .wait_for(|count| *count >= 1)
                .await
                .assured("the cut bounds a real acknowledged row");
            let (answer, published) = oneshot::channel();
            requests
                .send(answer)
                .await
                .assured("the branch accepts the checkpoint");
            published.await.assured("the cut awaits forced publication");
            let captured = CapturedInferencer::published(&state)
                .assured("the cut retains a concrete inference generation");
            let revision = captured.revision();
            let retained = captured
                .published_generation()
                .assured("the cut retained a native publication");
            assert_eq!(retained.incarnation, 17);
            let (_, values) = retained.generation.tensors[0]
                .try_extract_tensor::<f32>()
                .assured("the captured tensor is F32");
            let expected = if before == 1 {
                [1.0, 10.0]
            } else {
                [3.0, 30.0]
            };
            assert!(
                values[0] >= expected[0] && values[1] >= expected[1],
                "the tensors contain every row acknowledged before the cut"
            );
            let bits = values
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>();
            assert!(
                revision >= before,
                "the frozen generation includes every acknowledged row"
            );
            drop(requests);
            source
                .await
                .assured("Shuttle fails the execution if its task panics");
            branch
                .await
                .assured("Shuttle fails the execution if its task panics");
            let retained = captured
                .published_generation()
                .assured("the frozen publication remains present");
            let (_, values) = retained.generation.tensors[0]
                .try_extract_tensor::<f32>()
                .assured("the retained tensor stays F32");
            assert_eq!(
                values
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                bits,
                "later publications cannot alter the cut's tensor bits"
            );
            assert_eq!(
                captured.revision(),
                revision,
                "later publications cannot replace the cut"
            );
        });
    });
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
    config.workers.pending_jobs = nervix_execution::PendingJobCounts::uniform(NonZeroUsize::MIN);
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
            nervix_vm::VmSettings::default(),
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

#[test]
fn bolero_native_snapshot_projection_preserves_tensor_shapes_and_float_bits() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes| {
            let mut values = vec![
                0.0,
                -0.0,
                f32::from_bits(0x7fc1_2345),
                f32::from_bits(0x7f80_0001),
                f32::INFINITY,
                f32::NEG_INFINITY,
            ];
            values.extend(bytes.chunks(4).map(|chunk| {
                let mut word = [0; 4];
                word[..chunk.len()].copy_from_slice(chunk);
                f32::from_bits(u32::from_le_bytes(word))
            }));
            let expected = values
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>();
            let tensor_schema =
                schema(&[u32::try_from(values.len()).expect("the bounded shape fits")]);
            let plan = InferencerStatePlan {
                bytes_per_branch: u64::try_from(values.len() * size_of::<f32>())
                    .expect("the bounded tensor fits"),
                bindings: vec![InferencerStatePortPlan {
                    input: "state_in".into(),
                    output: "state_out".into(),
                    schema: tensor_schema,
                    elements: values.len(),
                    initial: InferencerStateInitial::Zeros,
                }],
            };
            crate::runtime::native_checkpoint_properties::in_admitted_job(move |cancellation| {
                let tensor = crate::inferencer_state_snapshot::TensorSnapshot {
                    shape: vec![u64::try_from(values.len()).expect("the bounded length fits u64")],
                    values: std::borrow::Cow::Owned(values),
                };
                let generation = NativeGeneration::restore(vec![tensor], &plan, cancellation)
                    .expect("the current native tensor restores");
                let projected = generation
                    .snapshot(cancellation)
                    .expect("the current native tensor projects");
                assert_eq!(projected.len(), 1);
                assert_eq!(
                    projected[0].shape,
                    [u64::try_from(expected.len()).expect("the bounded length fits u64")]
                );
                assert_eq!(
                    projected[0]
                        .values
                        .iter()
                        .map(|value| value.to_bits())
                        .collect::<Vec<_>>(),
                    expected
                );
            });
        });
}

/// Native fixtures are opaque immutable values. Shuttle explores only the production carrier's
/// publication/capture and the branch's freeze boundary, not ONNX or reference-count ordering.
#[cfg(feature = "shuttle")]
#[test]
fn shuttle_inferencer_publication_keeps_captured_tensors_stable_through_handoff_freeze() {
    use nervix_models::{InferencerModelFingerprint, SchemaFingerprint};
    use nervix_primitives::sync::{mpsc, oneshot, watch};

    use crate::runtime::{
        RuntimeState, RuntimeStatePlacement, inferencer_state::ReplicatedInferencerState,
        published_generation::PublishedGenerations,
    };
    let first = Arc::new(NativeGeneration {
        tensors: vec![
            Tensor::<f32>::from_array(([2], vec![1.0, 10.0]))
                .expect("the bounded native fixture builds")
                .into_dyn(),
        ],
    });
    let second = Arc::new(NativeGeneration {
        tensors: vec![
            Tensor::<f32>::from_array(([2], vec![3.0, 30.0]))
                .expect("the bounded native fixture builds")
                .into_dyn(),
        ],
    });
    nervix_model_harness::shuttle::check_interleavings(move || {
        let first = first.clone();
        let second = second.clone();
        shuttle::future::block_on(async move {
            let placement = RuntimeStatePlacement {
                domain: nervix_models::DomainName::parse("inference").expect("valid domain"),
                state: RuntimeState::Inferencer {
                    schema: SchemaFingerprint::from_digest([7; 32]),
                    model: InferencerModelFingerprint::from_digest([9; 32]),
                },
                kind: nervix_models::ModelKind::Inferencer,
                identifier: nervix_models::ModelName::parse("running").expect("valid node"),
                unit: nervix_models::Unit::SINGLE,
                branch_key: None,
            };
            let state = Arc::new(ReplicatedInferencerState::from_generations(
                placement,
                PublishedGenerations::restored(0, None),
            ));
            let (admitted, mut admissions) = mpsc::channel::<Arc<NativeGeneration>>(1);
            let (frozen, mut freezes) = mpsc::channel::<oneshot::Sender<()>>(1);
            let (acknowledged, mut acknowledgements) = watch::channel(0_u64);
            let branch_state = state.clone();
            let branch = nervix_primitives::task::spawn(async move {
                let mut revision = 0;
                loop {
                    nervix_primitives::task::consume_budget().await;
                    nervix_primitives::select! {
                        biased;
                        request = freezes.recv() => {
                            let answer = request.expect("the freeze caller remains alive");
                            answer.send(()).expect("the capture waits for freeze");
                            break;
                        }
                        generation = admissions.recv() => {
                            let Some(generation) = generation else { continue; };
                            let session = StatefulInferenceSession { session: None, generation };
                            branch_state.publish_session(&session, 17, crate::inferencer_state_snapshot::footprint([(1, 2)]).expect("bounded footprint"));
                            revision += 1;
                            acknowledged.send_replace(revision);
                        }
                    }
                }
            });
            admitted
                .send(first)
                .await
                .expect("the first batch is admitted");
            acknowledgements
                .wait_for(|revision| *revision >= 1)
                .await
                .expect("the branch acknowledges publication");
            let writer = state.generations.load();
            let writer_value = writer
                .value
                .as_ref()
                .expect("the first batch published")
                .generation
                .clone();
            let before = writer_value.tensors[0]
                .try_extract_tensor::<f32>()
                .expect("F32 fixture")
                .1
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>();
            let next = nervix_primitives::task::spawn(async move {
                let _ = admitted.send(second).await;
            });
            let (answer, answered) = oneshot::channel();
            frozen
                .send(answer)
                .await
                .expect("the branch receives its freeze");
            answered.await.expect("the branch finishes its freeze");
            let captured = state.generations.load();
            assert!(captured.revision >= writer.revision);
            assert_eq!(
                captured
                    .value
                    .as_ref()
                    .expect("the frozen state retains its last publication")
                    .incarnation,
                17
            );
            assert_eq!(
                before,
                writer_value.tensors[0]
                    .try_extract_tensor::<f32>()
                    .expect("F32 fixture")
                    .1
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>()
            );
            next.await.expect("the modeled submission finishes");
            branch.await.expect("the modeled branch finishes");
        });
    });
}
