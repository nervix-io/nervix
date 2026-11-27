//! Published-generation paging, expiry and live/sealed window equivalence.
//!
//! Layer: test harness.
//! - **Owns.** Exact typed key pages, read-only expiry, window header facts and state-charge parity.
//! - **Depends on.** Production page building, window accounting and bounded snapshot decoding.
//! - **Must not know.** Sessions, cluster placement or test transports.

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch,
    cast::AsArray as _,
    types::{TimestampNanosecondType, UInt64Type},
};
use arrow_schema::{DataType, Field};
use nervix_execution::{ADMISSION_TOKEN_BYTES, Executor};
use nervix_models::ParseAsType;

use super::*;
use crate::{
    runtime::{
        DeduplicatorKey, WindowProcessorState, test_schema, window_plan,
        window_state::{WindowEntrySnapshot, encode_window_processor_snapshot},
    },
    runtime_schema::{
        RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow, RuntimeValue, test_runtime_row,
    },
};

async fn page(
    branches: Vec<OpenBranch>,
    plan: ReadPlan,
    now: Timestamp,
    offset: u64,
    rows: usize,
) -> ProcessorStatePage {
    let executor = Executor::default();
    let memory = executor
        .try_reserve(MemoryClass::Bulk, ADMISSION_TOKEN_BYTES)
        .expect("bounded fixture admission");
    let after = if offset == 0 {
        None
    } else {
        Some((None, offset))
    };
    executor
        .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
            PublishedPage::build(
                branches,
                plan,
                now,
                after,
                ProcessorStatePageBounds {
                    rows,
                    bytes: 1024 * 1024,
                    reply: PageReply::Local,
                },
                memory,
                cancellation,
            )
        })
        .await
        .expect("the bounded job finishes")
        .expect("the fixture page builds")
}

fn keys() -> (Vec<OpenBranch>, ReadPlan) {
    let values = (0..3)
        .map(|index| PublishedDeduplicatorKey {
            key: Arc::new(DeduplicatorKey::new(vec![ReorderKeyPart::UInt64(
                u64::MAX - index,
            )])),
            seen_at: Timestamp::from_unix_nanos(
                i64::try_from(index).expect("three keys") * 1_000_000_000,
            ),
        })
        .collect();
    let generation = StdArc::new(Generation {
        revision: 7,
        value: values,
    });
    (
        vec![OpenBranch {
            key: None,
            registration: [7; 16],
            revision: 7,
            state: OpenState::Keys(generation),
        }],
        ReadPlan::Keys {
            fields: vec![StdArc::new(Field::new("key", DataType::UInt64, true))],
            max_time: std::time::Duration::from_secs(10),
        },
    )
}

#[nervix_primitives::test]
async fn keys_expire_without_mutating_the_publication_and_page_in_exact_types() {
    let (branches, plan) = keys();
    let first = page(
        branches,
        plan,
        Timestamp::from_unix_nanos(10_500_000_000),
        0,
        1,
    )
    .await;
    assert_eq!(first.counts.rows, 2);
    assert_eq!(first.offset, 1);
    assert!(first.more);
    assert_eq!(
        first.sections[0].payload[0]
            .as_primitive::<UInt64Type>()
            .value(0),
        u64::MAX - 1
    );
    assert_eq!(
        first.sections[0].appended[0]
            .as_primitive::<TimestampNanosecondType>()
            .value(0),
        1_000_000_000
    );
    let (branches, plan) = keys();
    let next = page(
        branches,
        plan,
        Timestamp::from_unix_nanos(10_500_000_000),
        1,
        1,
    )
    .await;
    assert_eq!(next.counts, first.counts);
    assert_eq!(next.fingerprint, first.fingerprint);
    assert_eq!(next.offset, 2);
    assert!(!next.more);
    assert_eq!(
        next.sections[0].payload[0]
            .as_primitive::<UInt64Type>()
            .value(0),
        u64::MAX - 2
    );
    let (branches, plan) = keys();
    let earlier = page(
        branches,
        plan,
        Timestamp::from_unix_nanos(9_000_000_000),
        0,
        3,
    )
    .await;
    assert_eq!(earlier.counts.rows, 3);
    assert_ne!(
        earlier.fingerprint, first.fingerprint,
        "expiry fences continuations without a new publication"
    );
}

#[nervix_primitives::test]
async fn a_replaced_state_registration_fences_identical_keys_and_publication_revisions() {
    let (branches, plan) = keys();
    let first = page(
        branches,
        plan,
        Timestamp::from_unix_nanos(9_000_000_000),
        0,
        1,
    )
    .await;
    let (mut branches, plan) = keys();
    branches[0].registration = [8; 16];
    let replacement = page(
        branches,
        plan,
        Timestamp::from_unix_nanos(9_000_000_000),
        0,
        1,
    )
    .await;
    assert_eq!(replacement.generation, first.generation);
    assert_eq!(replacement.counts, first.counts);
    assert_ne!(replacement.fingerprint, first.fingerprint);
}

fn window() -> (
    WindowAccumulatorPlan,
    WindowProcessorStateSnapshot,
    StdArc<ArrowSchema>,
) {
    let plan = window_plan(
        "SET samples = COUNT(input.latency), total = SUM(input.latency)",
        ParseAsType::I64,
        &[("samples", ParseAsType::I64), ("total", ParseAsType::I64)],
    );
    let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(
        Timestamp::from_unix_nanos(42),
        Timestamp::from_unix_nanos(42),
    );
    let record = test_runtime_row([("latency".to_string(), RuntimeValue::I64(10))])
        .with_metadata(metadata.clone());
    let input = record.batch().schema();
    let arguments = WindowArgumentColumns::snapshot_schema(&plan);
    let arrays: Vec<ArrayRef> = vec![
        StdArc::new(Int64Array::from(vec![10])),
        StdArc::new(Int64Array::from(vec![10])),
    ];
    let batch = RecordBatch::try_new(arguments.clone(), arrays).expect("two typed arguments");
    let batch =
        RuntimeRecordBatch::from_record_batch(arguments, batch).expect("declared argument schema");
    let arguments = RuntimeRow::new(Arc::new(batch), 0, metadata).expect("one argument row");
    let snapshot = WindowProcessorStateSnapshot {
        entries: vec![WindowEntrySnapshot {
            sequence: 5,
            timestamp: Timestamp::from_unix_nanos(42),
            key: None,
            record,
            arguments,
        }],
        next_sequence: 6,
        incarnation: Some(7),
        accumulators: vec![
            WindowAccumulatorSnapshot::Retained,
            WindowAccumulatorSnapshot::Retained,
        ],
    };
    (plan, snapshot, input)
}

fn read_plan(plan: WindowAccumulatorPlan, input: StdArc<ArrowSchema>) -> ReadPlan {
    ReadPlan::Window {
        input,
        arguments: WindowArgumentColumns::snapshot_schema(&plan),
        plan,
        width: Some(std::time::Duration::from_secs(1)),
    }
}

#[nervix_primitives::test]
async fn window_header_uses_the_live_admission_charge_and_does_not_advance_the_row_cursor() {
    let (plan, snapshot, input) = window();
    let restored = WindowProcessorState::from_snapshot(
        &plan,
        &test_schema(&[("latency", ParseAsType::I64)]),
        &snapshot,
    )
    .expect("the current snapshot restores");
    let mut charge = WindowStateCharge::new(&plan);
    for entry in &restored.entries {
        charge.admit(&entry.message, &entry.row.arguments);
    }
    let charge =
        u64::try_from(charge.finish().expect("bounded current state")).expect("bounded bytes");
    let generation = StdArc::new(Generation {
        revision: 9,
        value: Some(WindowPublishedSnapshot::Live(snapshot)),
    });
    let page = page(
        vec![OpenBranch {
            key: None,
            registration: [7; 16],
            revision: 9,
            state: OpenState::Window(WindowRead::Published(generation)),
        }],
        read_plan(plan, input),
        Timestamp::from_unix_nanos(43),
        0,
        1,
    )
    .await;
    assert_eq!(page.counts.rows, 1);
    assert_eq!(page.counts.bytes, charge);
    assert_eq!(page.offset, 1);
    assert!(!page.more);
    assert_eq!(
        page.runs
            .iter()
            .filter(|run| !run.section.is_metadata())
            .map(|run| run.rows)
            .sum::<usize>(),
        1
    );
    assert!(
        page.runs[0].section.is_metadata(),
        "metadata is bounded before retained-row frames"
    );
    let header = page
        .sections
        .iter()
        .find(|section| section.section == ProcessorStateSection::Header)
        .expect("branch header");
    assert_eq!(header.payload[2].as_primitive::<UInt64Type>().value(0), 5);
    assert_eq!(header.payload[3].as_primitive::<UInt64Type>().value(0), 6);
    assert_eq!(
        header.payload[6].as_primitive::<UInt64Type>().value(0),
        charge
    );
}

#[nervix_primitives::test]
async fn sealed_window_rows_and_metadata_use_the_same_page_contract_as_resident_state() {
    let (plan, snapshot, input) = window();
    let executor = Executor::default();
    let bytes = encode_window_processor_snapshot(&snapshot, 9, &executor)
        .await
        .expect("current native window encodes");
    let (bytes, layout) = WindowCheckpointLayout::open_owned(
        &executor,
        bytes,
        |bytes| Some(bytes.as_slice()),
        WindowSnapshotLifetime::Recorded,
    )
    .await
    .expect("admitted metadata parses");
    let layout = layout.expect("recorded branch lifetime");
    let arguments = WindowArgumentColumns::snapshot_schema(&plan);
    let mut decoded = decode_window_processor_layout(
        &bytes,
        &executor,
        WindowSnapshotSchemas {
            input: &input,
            arguments: &arguments,
        },
        layout.layout,
    )
    .await
    .expect("bounded native column decoding");
    decoded.metadata = Some(layout._memory);
    let page = page(
        vec![OpenBranch {
            key: None,
            registration: [7; 16],
            revision: 9,
            state: OpenState::Window(WindowRead::Decoded(Box::new(decoded))),
        }],
        read_plan(plan, input),
        Timestamp::from_unix_nanos(43),
        0,
        1,
    )
    .await;
    assert_eq!(page.counts.rows, 1);
    assert_eq!(page.sections.len(), 3);
    assert_eq!(
        page.sections[0].appended[0]
            .as_primitive::<UInt64Type>()
            .value(0),
        5
    );
    assert!(
        page.memory.is_some(),
        "decoded storage remains charged through reporting"
    );
}

/// A live window of `rows` retained inputs, each carrying its own `payload` bytes.
fn large_window(
    rows: u64,
    payload: usize,
) -> (
    WindowAccumulatorPlan,
    WindowProcessorStateSnapshot,
    StdArc<ArrowSchema>,
) {
    let plan = window_plan(
        "SET samples = COUNT(input.latency), total = SUM(input.latency)",
        ParseAsType::I64,
        &[("samples", ParseAsType::I64), ("total", ParseAsType::I64)],
    );
    let arguments_schema = WindowArgumentColumns::snapshot_schema(&plan);
    let mut entries = Vec::new();
    for sequence in 0..rows {
        let latency = i64::try_from(sequence).expect("a fixture row count fits i64");
        let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(
            Timestamp::from_unix_nanos(latency),
            Timestamp::from_unix_nanos(latency),
        );
        let record = test_runtime_row([
            ("latency".to_string(), RuntimeValue::I64(latency)),
            (
                "payload".to_string(),
                RuntimeValue::String("w".repeat(payload)),
            ),
        ])
        .with_metadata(metadata.clone());
        let arrays: Vec<ArrayRef> = vec![
            StdArc::new(Int64Array::from(vec![latency])),
            StdArc::new(Int64Array::from(vec![latency])),
        ];
        let batch =
            RecordBatch::try_new(arguments_schema.clone(), arrays).expect("two typed arguments");
        let batch = RuntimeRecordBatch::from_record_batch(arguments_schema.clone(), batch)
            .expect("declared argument schema");
        let arguments = RuntimeRow::new(Arc::new(batch), 0, metadata).expect("one argument row");
        entries.push(WindowEntrySnapshot {
            sequence,
            timestamp: Timestamp::from_unix_nanos(latency),
            key: None,
            record,
            arguments,
        });
    }
    let input = entries[0].record.batch().schema();
    let snapshot = WindowProcessorStateSnapshot {
        entries,
        next_sequence: rows,
        incarnation: Some(7),
        accumulators: vec![
            WindowAccumulatorSnapshot::Retained,
            WindowAccumulatorSnapshot::Retained,
        ],
    };
    (plan, snapshot, input)
}

#[nervix_primitives::test]
async fn a_window_page_beyond_the_bulk_class_ends_where_it_runs_out_and_the_rest_follow() {
    // 300 inputs of 64 KiB are 19 MiB. Gathering their columns charges twice that, more than the
    // default executor's 32 MiB bulk class, while the page's own bounds admit every row.
    let (plan, snapshot, input) = large_window(300, 64 * 1024);
    let generation = StdArc::new(Generation {
        revision: 9,
        value: Some(WindowPublishedSnapshot::Live(snapshot)),
    });
    let executor = Executor::default();
    let mut after = None;
    let mut sequences = Vec::new();
    let mut pages = 0_usize;
    loop {
        let branch = OpenBranch {
            key: None,
            registration: [7; 16],
            revision: 9,
            state: OpenState::Window(WindowRead::Published(generation.clone())),
        };
        let read = read_plan(plan.clone(), input.clone());
        let memory = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .expect("bounded fixture admission");
        let page = executor
            .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
                PublishedPage::build(
                    vec![branch],
                    read,
                    Timestamp::from_unix_nanos(43),
                    after,
                    ProcessorStatePageBounds {
                        rows: 1024,
                        bytes: 64 * 1024 * 1024,
                        reply: PageReply::Local,
                    },
                    memory,
                    cancellation,
                )
            })
            .await
            .expect("the bounded job finishes")
            .expect("a page ends where the bulk class runs out instead of refusing the read");
        pages += 1;
        assert_eq!(
            page.counts.rows, 300,
            "every page counts the complete window"
        );
        let rows = &page
            .sections
            .iter()
            .find(|section| section.section == ProcessorStateSection::Rows)
            .expect("the retained rows section")
            .appended[0];
        let returned = rows.as_primitive::<UInt64Type>();
        assert!(!returned.is_empty(), "every page returns at least one row");
        sequences.extend(returned.values().iter().copied());
        if !page.more {
            break;
        }
        after = Some((page.last_branch, page.offset));
        assert!(pages < 300, "the pages make progress");
    }
    assert!(
        pages > 1,
        "the bulk class cannot hold every row's columns in one page"
    );
    assert_eq!(
        sequences,
        (0..300).collect::<Vec<u64>>(),
        "the pages return every retained row once, in order"
    );
}

/// A page another node reads sets aside, before any row, what its descriptor and each section
/// body's framing take, and with each row it admits the bytes of the body that carries the row.
/// Under the same bulk class it therefore ends before a page its owner presents, and what it set
/// aside travels with the page to its reply.
#[nervix_primitives::test]
async fn a_page_another_node_reads_sets_aside_its_reply_and_ends_sooner() {
    let (plan, snapshot, input) = large_window(300, 64 * 1024);
    let mut row_bytes = Vec::with_capacity(snapshot.entries.len());
    for entry in &snapshot.entries {
        row_bytes.push(held_row_bytes(&entry.record) + held_row_bytes(&entry.arguments) + 16);
    }
    let metadata_rows = u64::try_from(plan.demands().len()).expect("bounded aggregates") + 1;
    let generation = StdArc::new(Generation {
        revision: 9,
        value: Some(WindowPublishedSnapshot::Live(snapshot)),
    });
    let executor = Executor::default();
    let mut returned = Vec::new();
    for reply in [PageReply::Local, PageReply::remote(&executor)] {
        let branch = OpenBranch {
            key: None,
            registration: [7; 16],
            revision: 9,
            state: OpenState::Window(WindowRead::Published(generation.clone())),
        };
        let read = read_plan(plan.clone(), input.clone());
        let memory = executor
            .try_reserve(MemoryClass::Bulk, 1)
            .expect("bounded fixture admission");
        let mut page = executor
            .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
                PublishedPage::build(
                    vec![branch],
                    read,
                    Timestamp::from_unix_nanos(43),
                    None,
                    ProcessorStatePageBounds {
                        rows: 1024,
                        bytes: 64 * 1024 * 1024,
                        reply,
                    },
                    memory,
                    cancellation,
                )
            })
            .await
            .expect("the bounded job finishes")
            .expect("both pages answer with the rows that fit");
        let rows = page
            .sections
            .iter()
            .find(|section| section.section == ProcessorStateSection::Rows)
            .expect("the retained rows section")
            .appended[0]
            .len();
        let set_aside = page
            .memory
            .as_mut()
            .expect("a published page keeps its memory")
            .take_reply();
        match (reply, set_aside) {
            (PageReply::Local, None) => {}
            (
                PageReply::Remote {
                    descriptor,
                    framing,
                },
                Some(set_aside),
            ) => {
                assert_eq!(
                    set_aside.descriptor.bytes(),
                    descriptor,
                    "the descriptor's encoding is set aside whole"
                );
                assert_eq!(set_aside.framing, framing);
                let rows_bytes: u64 = row_bytes[..rows].iter().sum();
                assert_eq!(
                    set_aside.bodies.bytes(),
                    3 * framing + rows_bytes + 256 * metadata_rows,
                    "the bodies' memory is each section's framing and every returned row's bytes"
                );
            }
            (reply, set_aside) => panic!(
                "a {reply:?} page set aside {:?}",
                set_aside.map(|set_aside| set_aside.bodies.bytes())
            ),
        }
        returned.push(rows);
    }
    let [local, remote] = returned.as_slice() else {
        panic!("one page of each kind");
    };
    assert!(*remote > 0, "the page another node reads returns rows");
    assert!(
        remote < local,
        "setting each body aside ends the page before one its owner presents"
    );
}
