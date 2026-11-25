//! Same-host cost of inspecting a retained window near the session reply bound while its branch
//! task keeps publishing it.
//!
//! Layer: test harness.
//! - **Owns.** The overlap measurement of window publications and published-generation pages.
//! - **Depends on.** The production window publication, the bounded page builder and monotonic
//!   instants.
//! - **Must not know.** Sessions, the interconnect or publication backend internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use nervix_execution::{ExecutionOverrides, ExecutionSettings, Executor};
use nervix_models::{
    DomainName, ModelKind, ModelName, ParseAsType, SchemaFingerprint, SessionBounds,
    StateInspectionSettings, Unit,
};
use nervix_primitives::{
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

use super::*;
use crate::{
    runtime::{
        RuntimeState, RuntimeStatePlacement, WindowProcessorState, test_schema, window_plan,
        window_state::{ReplicatedWindowProcessorState, WindowEntrySnapshot},
    },
    runtime_schema::{
        RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow, RuntimeValue, test_runtime_row,
    },
};

/// Retained rows, each with its own input record. At the default row bound a page returns all.
const ROWS: u64 = 1024;
/// The payload of each retained input: 1024 rows of 63 KiB come to 63 MiB, just under the
/// session's 64 MiB transfer bound, which bounds an inspection reply.
const PAYLOAD_BYTES: usize = 63 * 1024;
/// Publications measured in each sample.
const PUBLICATIONS: usize = 512;
/// Samples of each configuration.
const SAMPLES: usize = 5;
/// The memory of the node whose executor the measurement uses. A page gathers its rows' columns
/// beside the views it selects, so it charges twice its bytes; an 8 GiB node's bulk class, a
/// thirty-second of its memory, holds that charge for a page at the transfer bound.
const NODE_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[test]
#[ignore = "same-host window inspection cost beside the branch task's publications"]
fn state_inspection_cost_window() {
    // The measured owner runs on the calling thread and the spawned reader on a worker, so the
    // reader's polling never takes the measured thread.
    let runtime = nervix_primitives::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .assured("the measurement runtime starts");
    runtime.block_on(measure());
}

/// The production read plan of the measured window.
fn read_plan(plan: &WindowAccumulatorPlan, input: StdArc<ArrowSchema>) -> ReadPlan {
    ReadPlan::Window {
        input,
        arguments: WindowArgumentColumns::snapshot_schema(plan),
        plan: plan.clone(),
        width: None,
    }
}

/// One bounded page of the window published last, built on the bulk workers as an owner does.
async fn page(
    state: &ReplicatedWindowProcessorState,
    plan: &WindowAccumulatorPlan,
    input: StdArc<ArrowSchema>,
    executor: &Executor,
    bounds: ProcessorStatePageBounds,
) -> ProcessorStatePage {
    let generation = state.generations.load();
    let branch = OpenBranch {
        key: None,
        registration: [7; 16],
        revision: generation.revision,
        state: OpenState::Window(WindowRead::Published(generation)),
    };
    let read_plan = read_plan(plan, input);
    let memory = executor
        .try_reserve(MemoryClass::Bulk, 1)
        .assured("the measurement admits its page");
    executor
        .run_cpu(CpuClass::Bulk, memory, move |memory, cancellation| {
            PublishedPage::build(
                vec![branch],
                read_plan,
                Timestamp::from_unix_nanos(1),
                None,
                bounds,
                memory,
                cancellation,
            )
        })
        .await
        .assured("the bounded page job finishes")
        .assured("the published window pages")
}

async fn measure() {
    let plan = window_plan(
        "SET samples = COUNT(input.latency), total = SUM(input.latency)",
        ParseAsType::I64,
        &[("samples", ParseAsType::I64), ("total", ParseAsType::I64)],
    );
    let arguments_schema = WindowArgumentColumns::snapshot_schema(&plan);
    let mut entries = Vec::new();
    for sequence in 0..ROWS {
        let latency = i64::try_from(sequence).assured("a fixed row count fits i64");
        let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(
            Timestamp::from_unix_nanos(latency),
            Timestamp::from_unix_nanos(latency),
        );
        let record = test_runtime_row([
            ("latency".to_string(), RuntimeValue::I64(latency)),
            (
                "payload".to_string(),
                RuntimeValue::String("w".repeat(PAYLOAD_BYTES)),
            ),
        ])
        .with_metadata(metadata.clone());
        let arrays: Vec<ArrayRef> = vec![
            StdArc::new(Int64Array::from(vec![latency])),
            StdArc::new(Int64Array::from(vec![latency])),
        ];
        let batch = RecordBatch::try_new(arguments_schema.clone(), arrays)
            .assured("two typed argument columns");
        let batch = RuntimeRecordBatch::from_record_batch(arguments_schema.clone(), batch)
            .assured("the declared argument schema");
        let arguments = RuntimeRow::new(Arc::new(batch), 0, metadata).assured("one argument row");
        entries.push(WindowEntrySnapshot {
            sequence,
            timestamp: Timestamp::from_unix_nanos(latency),
            key: None,
            record,
            arguments,
        });
    }
    // A page measures each row as the owner does: its input and argument payload and 16 bytes.
    let mut page_bytes = 0_u64;
    for entry in &entries {
        page_bytes += held_row_bytes(&entry.record) + held_row_bytes(&entry.arguments) + 16;
    }
    let input = entries[0].record.batch().schema();
    let snapshot = WindowProcessorStateSnapshot {
        entries,
        next_sequence: ROWS,
        incarnation: Some(7),
        accumulators: vec![
            WindowAccumulatorSnapshot::Retained,
            WindowAccumulatorSnapshot::Retained,
        ],
    };
    let live = WindowProcessorState::from_snapshot(
        &plan,
        &test_schema(&[
            ("latency", ParseAsType::I64),
            ("payload", ParseAsType::String),
        ]),
        &snapshot,
    )
    .assured("the retained window restores");
    let state = StdArc::new(ReplicatedWindowProcessorState::new(
        RuntimeStatePlacement {
            domain: DomainName::parse("inspection_cost").assured("a valid domain name"),
            state: RuntimeState::WindowProcessor {
                schema: SchemaFingerprint::from_digest([3; 32]),
            },
            kind: ModelKind::WindowProcessor,
            identifier: ModelName::parse("latency_window").assured("a valid model name"),
            unit: Unit::SINGLE,
            branch_key: None,
        },
        None,
    ));
    state
        .replace_state(&live)
        .assured("the branch task publishes its window");
    let config = ExecutionSettings::resolve(
        ExecutionOverrides::default(),
        None,
        std::num::NonZeroU64::new(NODE_MEMORY_BYTES),
    )
    .assured("the declared defaults resolve for an 8 GiB node")
    .config();
    let executor = Executor::new(config).assured("the resolved budgets hold their operations");
    let rows = StateInspectionSettings::default().max_rows.value().get();
    let bytes = SessionBounds::default().transfer_bytes().get();
    let bounds = move || ProcessorStatePageBounds {
        rows,
        bytes,
        reply: PageReply::Local,
    };

    // A page the reader holds keeps the window it read however often the task publishes again.
    let held = page(&state, &plan, input.clone(), &executor, bounds()).await;
    let held_revision = state.generations.load().revision;
    let returned = held
        .runs
        .iter()
        .filter(|run| !run.section.is_metadata())
        .map(|run| run.rows)
        .sum::<usize>();
    assert_eq!(
        u64::try_from(returned).assured("a bounded row count fits u64"),
        ROWS,
        "the page returns every retained row at the default row bound"
    );
    assert!(
        page_bytes <= bytes && page_bytes >= bytes / 10 * 9,
        "the page holds {page_bytes} bytes, within a tenth of the {bytes}-byte transfer bound"
    );

    // Each sample measures both configurations back to back, so a change in the host's load
    // between samples reaches both alike.
    for sample in 0..SAMPLES {
        for inspected in [false, true] {
            let stop = StdArc::new(AtomicBool::new(false));
            let reader = inspected.then(|| {
                nervix_primitives::task::spawn({
                    let state = state.clone();
                    let plan = plan.clone();
                    let input = input.clone();
                    let executor = executor.clone();
                    let stop = stop.clone();
                    async move {
                        let mut pages = 0_u64;
                        while !stop.load(Ordering::SeqCst) {
                            std::hint::black_box(
                                page(&state, &plan, input.clone(), &executor, bounds()).await,
                            );
                            pages += 1;
                        }
                        pages
                    }
                })
            });
            let mut timings = Vec::with_capacity(PUBLICATIONS);
            let whole = Instant::now();
            for _ in 0..PUBLICATIONS {
                let started = Instant::now();
                state
                    .replace_state(&live)
                    .assured("the branch task publishes every measured window");
                timings.push(started.elapsed().as_nanos());
                nervix_primitives::task::consume_budget().await;
            }
            let elapsed = whole.elapsed();
            stop.store(true, Ordering::SeqCst);
            let pages = match reader {
                Some(reader) => reader.await.assured("the reader ends with its pages"),
                None => 0,
            };
            timings.sort_unstable();
            println!(
                "state-inspection-window inspected={inspected} sample={sample} \
                 publications={PUBLICATIONS} ns_per_publication={} p50_ns={} p99_ns={} \
                 pages={pages} page_rows={returned} page_bytes={page_bytes} state_charge={} \
                 transfer_bound={bytes}",
                elapsed.as_nanos()
                    / u128::try_from(PUBLICATIONS).assured("a fixed count fits u128"),
                timings[timings.len() / 2],
                timings[timings.len() * 99 / 100],
                held.counts.bytes
            );
            if inspected {
                assert!(pages > 0, "the reader paged while the task published");
            }
        }
    }
    assert!(
        state.generations.load().revision > held_revision,
        "the task published past the window the reader held, without waiting for it"
    );
}
