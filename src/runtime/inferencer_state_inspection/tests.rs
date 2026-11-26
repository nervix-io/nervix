//! Layer: test harness.
//! Owns: complete tensor-page values, current typed schemas, paging and byte-refusal oracles.
//! Depends on: the production native restore and page owners under ordinary bulk admission.
//! Must not know: graph scheduling, session rendering or a copied inspection algorithm.

use std::{borrow::Cow, num::NonZeroU32};

use arrow_array::{Array as _, Float32Array, ListArray, StringArray, UInt64Array};
use nervix_execution::{ADMISSION_TOKEN_BYTES, Executor};
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
) -> RetainedBranch {
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
    retained(key, &state)
}

/// The fixture `state` of branch `key` as a page retains it: confirmed by both of its replicas.
fn retained(key: BranchKey, state: &ReplicatedInferencerState) -> RetainedBranch {
    let generation = state.generations.load();
    RetainedBranch {
        branch: CapturedBranch {
            key: Some(key),
            registration: [7; 16],
            revision: generation.revision,
            persisted: 42,
            confirmed: 2,
            replicas: 2,
        },
        generation,
    }
}

async fn read(
    executor: &Executor,
    branches: &[RetainedBranch],
    plan: &InferencerStatePlan,
    after: Option<(Option<BranchKeyFingerprint>, u64)>,
    rows: usize,
    bytes: u64,
) -> ReadResult<ProcessorStatePage> {
    read_for(
        executor,
        branches,
        plan,
        after,
        rows,
        bytes,
        crate::runtime::PageReply::Local,
    )
    .await
}

/// One page, as [`read`] reads it, for the reader `reply` names. Its second pass retains the
/// fixtures' publications of the branches it returns tensors from.
async fn read_for(
    executor: &Executor,
    branches: &[RetainedBranch],
    plan: &InferencerStatePlan,
    after: Option<(Option<BranchKeyFingerprint>, u64)>,
    rows: usize,
    bytes: u64,
    reply: crate::runtime::PageReply,
) -> ReadResult<ProcessorStatePage> {
    let fixtures = branches.to_vec();
    let plan = plan.clone();
    let memory = executor
        .try_reserve(MemoryClass::Bulk, ADMISSION_TOKEN_BYTES)
        .assured("fixture admission");
    executor
        .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
            let mut captured = Vec::new();
            for fixture in &fixtures {
                captured.push(fixture.branch.clone());
            }
            // The page asks for its branches in the fixtures' canonical order, so one forward
            // pass over them serves every request.
            let mut publications = fixtures.iter();
            let retain = |branch: &CapturedBranch| -> ReadResult<Option<RetainedBranch>> {
                let key = branch.key.as_ref().map(BranchKey::fingerprint);
                for fixture in publications.by_ref() {
                    if fixture.branch.key.as_ref().map(BranchKey::fingerprint) == key {
                        return Ok(Some(fixture.clone()));
                    }
                }
                Ok(None)
            };
            page::build(
                page::CapturedBranches {
                    branches: captured,
                    catalog: nervix_models::StateInspectionKeyCatalog::Unrequested,
                },
                retain,
                &plan,
                after,
                ProcessorStatePageBounds { rows, bytes, reply },
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

/// The values of every wide fixture tensor: 256 KiB of F32.
const WIDE_ELEMENTS: usize = 64 * 1024;

/// The bulk memory a wide fixture page may take: room for two of its rows, not for all of them.
const WIDE_PAGE_ROOM: u64 = 3 * 1024 * 1024;

/// The bulk memory a wide fixture page may take when its owner presents all eight rows in one page
/// while their bodies as well do not fit.
const WIDE_WHOLE_PAGE_ROOM: u64 = 23 * 1024 * 1024 / 2;

/// The byte bound a session transfer gives every page, as a page read at the default limit has.
const TRANSFER_BOUND: u64 = 64 * 1024 * 1024;

/// A plan of one state binding whose tensor holds `elements` values.
fn wide_plan(elements: usize) -> InferencerStatePlan {
    let axis = u32::try_from(elements).assured("a fixture axis fits u32");
    InferencerStatePlan {
        bindings: vec![InferencerStatePortPlan {
            input: "state_in".to_owned(),
            output: "state_out".to_owned(),
            elements,
            schema: InferencerTensorSchema {
                representation: InferencerTensorRepresentation::Dense,
                element_type: InferencerTensorElementType::F32,
                dimensions: vec![InferencerTensorDimension::Fixed(
                    NonZeroU32::new(axis).assured("fixture axis is positive"),
                )],
            },
            initial: InferencerStateInitial::Zeros,
        }],
        bytes_per_branch: u64::try_from(elements)
            .assured("a fixture count fits u64")
            .checked_mul(4)
            .assured("fixture states are bounded"),
    }
}

/// A restored branch of `tenant` whose one wide tensor holds `fill` in every value.
async fn wide_branch(
    executor: &Executor,
    plan: &InferencerStatePlan,
    tenant: &str,
    fill: f32,
) -> RetainedBranch {
    let key = BranchKey::from_fields([(named("tenant"), RuntimeValue::String(tenant.to_owned()))])
        .assured("one current branch field");
    let snapshot_bytes = plan.snapshot_bytes().assured("bounded fixture footprint");
    let memory = executor
        .try_reserve(
            MemoryClass::Bulk,
            snapshot_bytes
                .checked_mul(4)
                .assured("a bounded fixture charge"),
        )
        .assured("fixture admission");
    let elements = plan.bindings[0].elements;
    let axis = u64::try_from(elements).assured("a fixture axis fits u64");
    let (snapshot, memory) = executor
        .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
            let payload = Snapshot {
                revision: 42,
                branch: Some(BranchSnapshot {
                    incarnation: 17,
                    tensors: vec![TensorSnapshot {
                        shape: vec![axis],
                        values: Cow::Owned(vec![fill; elements]),
                    }],
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
    retained(key, &state)
}

/// Eight wide branches in the canonical order a capture sorts them in, each with its fill.
async fn wide_branches(
    executor: &Executor,
    plan: &InferencerStatePlan,
) -> Vec<(RetainedBranch, f32)> {
    let mut branches = Vec::new();
    for index in 0_u8..8 {
        let fill = f32::from(index);
        let branch = wide_branch(executor, plan, &format!("tenant-{index}"), fill).await;
        branches.push((branch, fill));
    }
    branches.sort_by(|(left, _), (right, _)| {
        BranchKey::canonical_order(&left.branch.key, &right.branch.key)
    });
    branches
}

/// Holds every byte of the executor's free bulk memory but `room`, as an owner whose other work
/// holds the rest.
fn hold_all_but(executor: &Executor, room: u64) -> Reservation {
    let memory = executor.snapshot().bulk_memory;
    let free = memory
        .capacity_bytes
        .checked_sub(memory.reserved_bytes)
        .assured("a pool reserves no more than it holds");
    executor
        .try_reserve(
            MemoryClass::Bulk,
            free.checked_sub(room)
                .assured("the fixture leaves the pool more than its room"),
        )
        .assured("the free pool admits the hold")
}

/// The fill of every tensor row a wide page returned, in page order.
fn returned_fills(page: &ProcessorStatePage) -> Vec<f32> {
    let Some(section) = page.sections.get(1) else {
        panic!("a wide page carries its header and its one tensor section");
    };
    let values = section.payload[3]
        .as_any()
        .downcast_ref::<ListArray>()
        .assured("declared VEC values");
    let mut fills = Vec::new();
    for row in 0..values.len() {
        let value = values.value(row);
        let floats = value
            .as_any()
            .downcast_ref::<Float32Array>()
            .assured("exact F32 list elements");
        fills.push(floats.value(0));
    }
    fills
}

/// An inferencer page whose tensor rows the bulk class cannot hold whole ends before the row it
/// can no longer hold, with complete counts and a continuation, and the pages that follow return
/// every other row once, in order. The read is never refused while a row can be returned.
#[nervix_primitives::test]
async fn an_inferencer_page_beyond_the_bulk_class_ends_where_it_runs_out_and_the_rest_follow() {
    let executor = Executor::default();
    let plan = wide_plan(WIDE_ELEMENTS);
    let fixtures = wide_branches(&executor, &plan).await;
    let mut branches = Vec::new();
    let mut expected = Vec::new();
    for (branch, fill) in &fixtures {
        branches.push(branch.clone());
        expected.push(*fill);
    }
    let held = hold_all_but(&executor, WIDE_PAGE_ROOM);
    let mut after = None;
    let mut fills = Vec::new();
    let mut pages = 0_usize;
    loop {
        nervix_primitives::task::consume_budget().await;
        let page = read(&executor, &branches, &plan, after, 1024, TRANSFER_BOUND)
            .await
            .assured("a page the bulk class cannot hold whole ends early instead of being refused");
        assert_eq!(
            page.counts.rows, 8,
            "every page reports the complete counts"
        );
        let returned = returned_fills(&page);
        assert!(!returned.is_empty(), "every page returns at least one row");
        fills.extend(returned);
        pages += 1;
        if !page.more {
            break;
        }
        after = Some((page.last_branch, page.offset));
    }
    assert!(pages > 1, "the room left holds no page of every row");
    assert_eq!(
        fills, expected,
        "the pages return every tensor once, in order"
    );
    drop(held);
}

/// An inferencer page another node reads sets aside, as it admits its rows, the bound on its
/// descriptor, the framing of its header and tensor bodies and every returned row's body, so it
/// ends before the page its owner presents under the same bulk memory.
#[nervix_primitives::test]
async fn an_inferencer_page_another_node_reads_sets_aside_its_reply_and_ends_sooner() {
    let executor = Executor::default();
    let plan = wide_plan(WIDE_ELEMENTS);
    let fixtures = wide_branches(&executor, &plan).await;
    let mut branches = Vec::new();
    for (branch, _) in &fixtures {
        branches.push(branch.clone());
    }
    let held = hold_all_but(&executor, WIDE_WHOLE_PAGE_ROOM);
    let local = read(&executor, &branches, &plan, None, 1024, TRANSFER_BOUND)
        .await
        .assured("the owner presents a first page");
    let local_rows = returned_fills(&local).len();
    assert_eq!(local_rows, 8, "the owner presents every tensor in one page");
    drop(local);
    let reply = crate::runtime::PageReply::remote(&executor);
    let crate::runtime::PageReply::Remote {
        descriptor,
        framing,
    } = reply
    else {
        panic!("a reply to another node sets memory aside");
    };
    let mut remote = read_for(
        &executor,
        &branches,
        &plan,
        None,
        1024,
        TRANSFER_BOUND,
        reply,
    )
    .await
    .assured("the owner answers another node with a first page");
    let remote_rows = returned_fills(&remote).len();
    assert!(remote_rows > 0, "the first page returns at least one row");
    assert!(
        remote_rows < local_rows,
        "a page another node reads ends sooner ({remote_rows} rows) than one its owner presents \
         ({local_rows} rows)"
    );
    let values = u64::try_from(WIDE_ELEMENTS * std::mem::size_of::<f32>())
        .assured("a fixture tensor fits u64");
    let returned = u64::try_from(remote_rows).assured("a bounded row count fits u64");
    let memory = remote
        .memory
        .as_mut()
        .assured("a page keeps its memory with its rows");
    let set_aside = memory
        .take_reply()
        .assured("a page another node reads sets its reply aside");
    assert_eq!(
        set_aside.descriptor.bytes(),
        descriptor,
        "the page sets aside the bound on one descriptor"
    );
    assert!(
        set_aside.bodies.bytes() >= 2 * framing + returned * values,
        "the page sets aside its header and tensor bodies' framing and every returned tensor"
    );
    assert_eq!(set_aside.framing, framing);
    drop(remote);
    drop(held);
}
