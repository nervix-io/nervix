//! Direct evidence for the carried state of an inferencer branch.
//!
//! Layer: test harness.
//! Owns: forced and frozen publication of carried generations, and the native snapshot projection.
//! Depends on: the production carried-state owner and its publication.
//! Must not know: graph scheduling or registry implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks carried native tensors and their publication"
    )
)]

use std::num::NonZeroU32;

#[cfg(feature = "shuttle")]
use meticulous::ResultExt as _;
use nervix_models::{
    InferencerTensorElementType, InferencerTensorRepresentation, InferencerTensorSchema,
};

use super::*;
use crate::registry::InferencerStatePortPlan;

/// The metrics a carried state of the Shuttle checks tracks its bytes in.
#[cfg(feature = "shuttle")]
fn state_metrics() -> crate::metrics::InferencerStateMetrics {
    let metrics = crate::metrics::RuntimeMetrics::default();
    let domain = "inference_tests"
        .parse()
        .assured("the test domain is valid");
    let name = "scoring".parse().assured("the test inferencer is valid");
    metrics.inferencer_state_metrics(&domain, &name, nervix_models::Unit::SINGLE, None)
}

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
            let metrics = state_metrics();
            let branch = nervix_primitives::task::spawn(async move {
                let mut carried = None::<CarriedState>;
                let (mut rows_open, mut cuts_open) = (true, true);
                while rows_open || cuts_open {
                    nervix_primitives::task::consume_budget().await;
                    nervix_primitives::select! {
                        biased;
                        cut = cuts.recv(), if cuts_open => match cut {
                            Some(answer) => {
                                if let Some(carried) = &carried { carried.publish(); }
                                answer.send(()).assured("the cut waits for forced publication");
                            }
                            None => cuts_open = false,
                        },
                        row = rows.recv(), if rows_open => match row {
                            Some((generation, answer)) => {
                                let current = CarriedState {
                                    generation,
                                    publication: branch_state.clone(),
                                    incarnation: 17,
                                    snapshot_bytes: 1024,
                                    bytes: metrics.track_branch(8),
                                };
                                current.publish();
                                carried = Some(current);
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
            let metrics = state_metrics();
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
                            let carried = CarriedState {
                                generation,
                                publication: branch_state.clone(),
                                incarnation: 17,
                                snapshot_bytes: crate::inferencer_state_snapshot::footprint([(1, 2)]).expect("bounded footprint"),
                                bytes: metrics.track_branch(8),
                            };
                            carried.publish();
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
