//! Layer: test harness.
//! Owns: complete tensor-page values, current typed schemas, paging and byte-refusal oracles.
//! Depends on: the production native restore and page owners under ordinary bulk admission.
//! Must not know: graph scheduling, session rendering or a copied inspection algorithm.

use std::{borrow::Cow, num::NonZeroU32};

use arrow_array::{Array as _, Float32Array, ListArray, StringArray, UInt64Array};
use nervix_execution::Executor;
use nervix_models::{
    InferencerStateInitial, InferencerTensorDimension, InferencerTensorElementType,
    InferencerTensorRepresentation, InferencerTensorSchema, SchemaField,
};

use super::*;
use crate::{
    inferencer_state_snapshot::{BranchSnapshot, Snapshot, TensorSnapshot},
    runtime::{PersistedRuntimeStateEntry, RuntimeState, RuntimeStatePlacement, named},
    runtime_schema::RuntimeValue,
};

fn plan(entries: usize) -> InferencerStatePlan {
    let mut bindings = Vec::new();
    for index in 0..entries {
        bindings.push(InferencerStatePortPlan {
            input: format!("input_{index}"),
            output: format!("output_{index}"),
            elements: 2,
            schema: InferencerTensorSchema {
                representation: InferencerTensorRepresentation::Dense,
                element_type: InferencerTensorElementType::F32,
                dimensions: vec![InferencerTensorDimension::Fixed(
                    NonZeroU32::new(2).assured("fixture axis is positive"),
                )],
            },
            initial: InferencerStateInitial::Zeros,
        });
    }
    InferencerStatePlan {
        bindings,
        bytes_per_branch: u64::try_from(entries)
            .assured("fixture count fits u64")
            .checked_mul(8)
            .assured("fixture states are small"),
    }
}

async fn branch(
    executor: &Executor,
    plan: &InferencerStatePlan,
    tenant: &str,
    bits: [u32; 2],
) -> CapturedBranch {
    let key = BranchKey::from_fields([(named("tenant"), RuntimeValue::String(tenant.to_owned()))])
        .assured("one current branch field");
    let snapshot_bytes = plan.snapshot_bytes().assured("bounded fixture footprint");
    let memory = executor
        .try_reserve(
            MemoryClass::Bulk,
            snapshot_bytes
                .checked_mul(4)
                .assured("small fixture charge"),
        )
        .assured("fixture admission");
    let entries = plan.bindings.len();
    let (snapshot, memory) = executor
        .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
            let mut tensors = Vec::new();
            for _ in 0..entries {
                tensors.push(TensorSnapshot {
                    shape: vec![2],
                    values: Cow::Owned(bits.map(f32::from_bits).to_vec()),
                });
            }
            let payload = Snapshot {
                revision: 42,
                branch: Some(BranchSnapshot {
                    incarnation: 17,
                    tensors,
                }),
            }
            .encode(snapshot_bytes, cancellation)
            .assured("current fixture encodes");
            (PersistedRuntimeStateEntry { lsm: 42, payload }, memory)
        })
        .await
        .assured("bounded fixture encoding");
    let state = ReplicatedInferencerState::new(
        RuntimeStatePlacement {
            domain: named("inference"),
            kind: ModelKind::Inferencer,
            identifier: named("running"),
            unit: Unit::SINGLE,
            branch_key: Some(key.clone()),
            state: RuntimeState::Inferencer {
                schema: nervix_models::SchemaFingerprint::from_digest([3; 32]),
                model: nervix_models::InferencerModelFingerprint::from_digest([5; 32]),
            },
        },
        Some((snapshot, memory)),
        plan,
        executor,
    )
    .await
    .assured("the production owner restores current native tensors");
    CapturedBranch {
        key: Some(key),
        registration: [7; 16],
        generation: state.generations.load(),
        persisted: 42,
        confirmed: 2,
        replicas: 2,
    }
}

async fn read(
    executor: &Executor,
    branches: &[CapturedBranch],
    plan: &InferencerStatePlan,
    after: Option<(Option<BranchKeyFingerprint>, u64)>,
    rows: usize,
    bytes: u64,
) -> ReadResult<ProcessorStatePage> {
    let branches = branches.to_vec();
    let plan = plan.clone();
    let memory = executor
        .try_reserve(MemoryClass::Bulk, 1)
        .assured("fixture admission");
    executor
        .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
            page::build(
                branches,
                &plan,
                None,
                after,
                ProcessorStatePageBounds { rows, bytes },
                memory,
                cancellation,
            )
        })
        .await
        .assured("bounded production page completes")
}

#[nervix_primitives::test]
async fn tensor_sections_preserve_every_float_bit_shape_port_and_header_beyond_three_sections() {
    let executor = Executor::default();
    let plan = plan(6);
    let first_bits = [0x8000_0000, 0x7fc1_2345];
    let second_bits = [0x7f80_0000, 0xff80_0000];
    let branches = vec![
        branch(&executor, &plan, "alpha", first_bits).await,
        branch(&executor, &plan, "beta", second_bits).await,
    ];
    let retained = branches[0].generation.clone();
    let page = read(&executor, &branches, &plan, None, 12, 1_048_576)
        .await
        .assured("the bounded page fits");
    assert_eq!(
        page.counts,
        StateInspectionCounts {
            branches: 2,
            rows: 12,
            bytes: 96
        }
    );
    assert_eq!(page.sections.len(), 7);
    assert!(!page.more);
    assert_eq!(page.offset, 12);
    let header = &page.sections[0];
    assert_eq!(header.rows, 2);
    let stages = header.payload[7]
        .as_any()
        .downcast_ref::<StringArray>()
        .assured("declared string stage");
    assert_eq!(stages.value(0), "REPLICA_CONFIRMED");
    for (index, section) in page.sections.iter().enumerate().skip(1) {
        let expected = ProcessorStateSection::Tensor(
            u32::try_from(
                index
                    .checked_sub(1)
                    .assured("tensor sections follow the header"),
            )
            .assured("fixture index"),
        );
        assert_eq!(section.section, expected);
        let fields = expected.inferencer_fields(false);
        let schema = SchemaField::arrow_schema(&fields);
        for (field, column) in schema.fields().iter().zip(&section.payload) {
            assert_eq!(field.data_type(), column.data_type());
        }
        let values = section.payload[3]
            .as_any()
            .downcast_ref::<ListArray>()
            .assured("declared VEC values");
        let shapes = section.payload[2]
            .as_any()
            .downcast_ref::<ListArray>()
            .assured("declared VEC shape");
        assert_eq!(shapes.value_length(0), 1);
        let shape = shapes.value(0);
        assert_eq!(
            shape
                .as_any()
                .downcast_ref::<UInt64Array>()
                .assured("exact shape axes")
                .values()
                .as_ref(),
            &[2]
        );
        let input = section.payload[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .assured("declared input port");
        let output = section.payload[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .assured("declared output port");
        let binding = &plan.bindings[index.checked_sub(1).assured("tensor follows the header")];
        assert_eq!(input.value(0), binding.input);
        assert_eq!(output.value(0), binding.output);
        for (row, bits) in [first_bits, second_bits].iter().enumerate() {
            let value = values.value(row);
            let floats = value
                .as_any()
                .downcast_ref::<Float32Array>()
                .assured("exact F32 list elements");
            assert_eq!(
                floats
                    .values()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                bits.as_slice()
            );
        }
    }
    assert!(StdArc::ptr_eq(&retained, &branches[0].generation));
    assert_eq!(retained.revision, 42);
}

#[nervix_primitives::test]
async fn single_tensor_pages_keep_complete_counts_and_refuse_a_foreign_branch_position() {
    let executor = Executor::default();
    let plan = plan(2);
    let branches = vec![
        branch(&executor, &plan, "alpha", [0, 1]).await,
        branch(&executor, &plan, "beta", [2, 3]).await,
    ];
    let mut after = None;
    let mut fingerprints = BTreeSet::new();
    for expected_offset in 1..=4 {
        nervix_primitives::task::consume_budget().await;
        let page = read(&executor, &branches, &plan, after, 1, 1_048_576)
            .await
            .assured("a tensor and its header fit");
        assert_eq!(
            page.counts,
            StateInspectionCounts {
                branches: 2,
                rows: 4,
                bytes: 32
            }
        );
        assert_eq!(page.offset, expected_offset);
        assert_eq!(page.more, expected_offset < 4);
        assert_eq!(
            page.runs
                .iter()
                .filter(|run| !run.section.is_metadata())
                .map(|run| run.rows)
                .sum::<usize>(),
            1
        );
        assert_eq!(
            page.runs
                .iter()
                .filter(|run| run.section.is_metadata())
                .map(|run| run.rows)
                .sum::<usize>(),
            1
        );
        fingerprints.insert(page.fingerprint);
        after = Some((page.last_branch, page.offset));
    }
    assert_eq!(
        fingerprints.len(),
        1,
        "paging does not change the immutable cut"
    );
    let denied = read(
        &executor,
        &branches,
        &plan,
        Some((Some(BranchKeyFingerprint::new([99; 32])), 1)),
        1,
        1_048_576,
    )
    .await
    .err()
    .assured("a foreign position is refused");
    assert!(matches!(
        denied.current_context(),
        ProcessorStatePageError::Position
    ));
    let page = read(&executor, &branches, &plan, None, 1, 1)
        .await
        .assured("counting needs no tensor copy");
    assert_eq!(page.counts.rows, 4);
    assert_eq!(page.offset, 0);
    assert!(page.more);
    assert!(page.runs.is_empty());
}
