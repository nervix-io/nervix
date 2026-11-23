//! Bounded copies of the rows a reorderer holds, the columns an owner page gathers from copies, and
//! the page of a unit still restoring its branches.
//!
//! Layer: test harness.
//! - **Owns.** The owner-level contracts of a held-row read: a copy skips the rows earlier pages
//!   returned, keeps the branch's arrival order across batches, stops at its row or byte bound and
//!   never copies part of a run past that bound; gathered sections keep the copies' row order in
//!   runs of one section and one branch, with payload values taken from the held batches and
//!   order keys and deadlines in their exact types; and a unit whose restore is pending refuses
//!   its owner page as unrestored without asking any branch task for its rows.
//! - **Depends on.** The production reorderer buffer, the bounded copy, the section builders, the
//!   unit's production selection answer and the owner page read.
//! - **Must not know.** Sessions, Row frames or the interconnect.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use arrow_array::{
    StringArray,
    cast::AsArray as _,
    types::{TimestampNanosecondType, UInt32Type, UInt64Type},
};
use arrow_schema::{DataType, Field};
use nervix_models::{ParseAsType, Timestamp};
use nervix_primitives::time::timeout;

use super::*;
use crate::{
    runtime_ack::AckSet,
    runtime_schema::{RuntimeValue, test_runtime_row},
};

/// One batch of `rows` rows whose sequences, and order keys, continue at `first`, each with a note
/// of the same length so every row carries the same payload bytes.
fn batch(first: u64, rows: u64) -> (RelayRecordBatch, Arc<Vec<ReordererRowOrder>>) {
    let schema = test_schema(&[
        ("sequence", ParseAsType::U64),
        ("note", ParseAsType::String),
    ]);
    let mut messages = Vec::new();
    let mut order = Vec::new();
    for sequence in first..first + rows {
        messages.push(RelayMessage {
            key: None,
            record: test_runtime_row([
                ("sequence".to_string(), RuntimeValue::U64(sequence)),
                (
                    "note".to_string(),
                    RuntimeValue::String(format!("note-{sequence:04}")),
                ),
            ]),
            acks: AckSet::empty(),
        });
        order.push(ReordererRowOrder {
            key: vec![ReorderKeyPart::UInt64(sequence)],
            arrival_sequence: sequence,
        });
    }
    let batch = RelayRecordBatch::from_messages(schema, messages)
        .assured("every fixture row matches the fixture schema");
    (batch, Arc::new(order))
}

/// A buffer holding batches of the given sizes, whose sequences run on from zero.
fn buffer(sizes: &[u64]) -> ReordererOutputBuffer {
    let mut buffer = ReordererOutputBuffer::default();
    let mut first = 0;
    for size in sizes {
        let (batch, order) = batch(first, *size);
        buffer.push(batch, order, Timestamp::from_unix_nanos(1_000));
        first += size;
    }
    buffer
}

fn copy(
    buffer: &ReordererOutputBuffer,
    read: BranchStateRead,
) -> (Vec<HeldReordererRow>, HeldRowsBudget) {
    let mut budget = HeldRowsBudget::new(read);
    let mut copied = Vec::new();
    buffer
        .copy_held(
            0,
            Timestamp::from_unix_nanos(9_000),
            &mut budget,
            &mut copied,
        )
        .assured("every fixture row has its order key");
    (copied, budget)
}

fn sequences(rows: &[HeldReordererRow]) -> Vec<u64> {
    let mut sequences = Vec::new();
    for row in rows {
        let [ReorderKeyPart::UInt64(sequence)] = row.key.as_slice() else {
            panic!("every fixture key is one unsigned sequence: {:?}", row.key);
        };
        sequences.push(*sequence);
    }
    sequences
}

#[test]
fn a_bounded_copy_skips_returned_rows_and_keeps_arrival_order_across_batches() {
    let buffer = buffer(&[2, 3]);
    assert_eq!(buffer.held_rows(), 5);
    let (copied, budget) = copy(
        &buffer,
        BranchStateRead {
            skip: 1,
            max_rows: 3,
            max_bytes: u64::MAX,
        },
    );
    assert_eq!(sequences(&copied), vec![1, 2, 3]);
    assert!(budget.is_full(), "a copy of its row bound is full");
    for row in &copied {
        assert_eq!(row.received_at, Timestamp::from_unix_nanos(1_000));
        assert_eq!(row.release_at, Timestamp::from_unix_nanos(9_000));
        assert_eq!(row.output, 0);
    }
    let sequence = copied[1]
        .row
        .batch()
        .batch()
        .column(0)
        .as_primitive::<UInt64Type>();
    assert_eq!(
        sequence.value(copied[1].row.row_index()),
        2,
        "a copied row is a view of the held row, not a rebuilt value"
    );
}

#[test]
fn a_copy_stops_at_the_first_row_beyond_its_bytes_and_copies_nothing_after_it() {
    let buffer = buffer(&[3, 2]);
    let (rows, _) = copy(
        &buffer,
        BranchStateRead {
            skip: 0,
            max_rows: usize::MAX,
            max_bytes: u64::MAX,
        },
    );
    let row_bytes = held_row_bytes(&rows[0].row);
    assert!(row_bytes > 0);
    for row in &rows {
        assert_eq!(
            held_row_bytes(&row.row),
            row_bytes,
            "every fixture row is the same size"
        );
    }
    let bound = row_bytes * 2 + row_bytes / 2;
    let (copied, budget) = copy(
        &buffer,
        BranchStateRead {
            skip: 0,
            max_rows: usize::MAX,
            max_bytes: bound,
        },
    );
    assert_eq!(sequences(&copied), vec![0, 1]);
    assert_eq!(budget.copied_bytes, row_bytes * 2);
    assert!(budget.is_full(), "the row that did not fit ends the copy");
}

#[test]
fn a_skip_past_every_held_row_and_a_zero_row_read_copy_nothing() {
    let buffer = buffer(&[2, 2]);
    let (copied, budget) = copy(
        &buffer,
        BranchStateRead {
            skip: 7,
            max_rows: 10,
            max_bytes: u64::MAX,
        },
    );
    assert!(copied.is_empty());
    assert!(!budget.is_full());
    let mut budget = HeldRowsBudget::new(BranchStateRead {
        skip: 7,
        max_rows: 10,
        max_bytes: u64::MAX,
    });
    assert_eq!(budget.skip(4), 4, "a skip consumes every row it can");
    assert_eq!(budget.skip(4), 3, "and carries the rest to the next rows");
    let (copied, budget) = copy(
        &buffer,
        BranchStateRead {
            skip: 0,
            max_rows: 0,
            max_bytes: u64::MAX,
        },
    );
    assert!(copied.is_empty());
    assert!(
        budget.is_full(),
        "a read of no rows is full before it starts"
    );
}

fn held_reorderer_branch(key: Option<BranchKey>, sequences: &[u64], held: u64) -> CopiedBranch {
    let mut rows = Vec::new();
    for (index, sequence) in sequences.iter().enumerate() {
        let (single, _) = batch(*sequence, 1);
        let row = single
            .runtime_row(0)
            .assured("the fixture batch holds its row");
        let at = i64::try_from(*sequence).assured("small fixture sequences fit i64");
        rows.push(HeldReordererRow {
            row,
            output: u32::try_from(index).assured("small fixture positions fit u32"),
            key: vec![ReorderKeyPart::UInt64(*sequence)],
            received_at: Timestamp::from_unix_nanos(at),
            release_at: Timestamp::from_unix_nanos(at + 600),
        });
    }
    CopiedBranch {
        key,
        held: HeldBranchRows {
            rows: held,
            bytes: 0,
            copied_bytes: 0,
            kind: HeldRowsKind::Reorderer {
                key_fields: vec![StdArc::new(Field::new(
                    "order_key",
                    DataType::UInt32,
                    false,
                ))],
                rows,
            },
        },
    }
}

#[test]
fn reorderer_sections_keep_row_order_in_branch_runs_with_typed_keys_and_deadlines() {
    let east = string_branch_key("region", "east");
    let west = string_branch_key("region", "west");
    let gathered = reorderer_sections(vec![
        held_reorderer_branch(east.clone(), &[3, 1], 4),
        held_reorderer_branch(west.clone(), &[2], 2),
    ])
    .assured("the copies hold their order keys in the key program's type");
    let runs = gathered
        .runs
        .iter()
        .map(|run| {
            (
                run.section,
                run.branch.as_ref().map(BranchKey::fingerprint),
                run.rows,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        runs,
        vec![
            (
                ProcessorStateSection::Pending,
                east.as_ref().map(BranchKey::fingerprint),
                2
            ),
            (
                ProcessorStateSection::Pending,
                west.as_ref().map(BranchKey::fingerprint),
                1
            ),
        ]
    );
    let [section] = gathered.sections.as_slice() else {
        panic!("a reorderer has one section");
    };
    assert_eq!(
        section.rows, 6,
        "the section counts every held row, returned or not"
    );
    let sequence = section.payload[0].as_primitive::<UInt64Type>();
    assert_eq!(sequence.values().to_vec(), vec![3, 1, 2]);
    let note = section.payload[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .assured("the note column stays text");
    assert_eq!(note.value(1), "note-0001");
    let [output, key, received_at, release_at] = section.appended.as_slice() else {
        panic!("output, one order key, received and release columns");
    };
    assert_eq!(
        output.as_primitive::<UInt32Type>().values().to_vec(),
        vec![0, 1, 0]
    );
    assert_eq!(
        key.data_type(),
        &DataType::UInt32,
        "a key part is narrowed back to the type its program produces"
    );
    assert_eq!(
        key.as_primitive::<UInt32Type>().values().to_vec(),
        vec![3, 1, 2]
    );
    let received = received_at.as_primitive::<TimestampNanosecondType>();
    assert_eq!(received.values().to_vec(), vec![3, 1, 2]);
    assert_eq!(received.timezone(), Some("+00:00"));
    let release = release_at.as_primitive::<TimestampNanosecondType>();
    assert_eq!(release.values().to_vec(), vec![603, 601, 602]);
}

#[test]
fn correlator_sections_split_each_side_and_keep_its_timeout() {
    let tenant = string_branch_key("tenant", "acme");
    let mut rows = Vec::new();
    for (side, sequence) in [
        (CorrelatorSide::Left, 1),
        (CorrelatorSide::Left, 2),
        (CorrelatorSide::Right, 3),
    ] {
        let (single, _) = batch(sequence, 1);
        let at = i64::try_from(sequence).assured("small fixture sequences fit i64");
        rows.push(HeldCorrelatorRow {
            side,
            row: single
                .runtime_row(0)
                .assured("the fixture batch holds its row"),
            received_at: Timestamp::from_unix_nanos(at),
            timeout_at: Timestamp::from_unix_nanos(at + 100),
        });
    }
    let gathered = correlator_sections(vec![CopiedBranch {
        key: tenant.clone(),
        held: HeldBranchRows {
            rows: 5,
            bytes: 0,
            copied_bytes: 0,
            kind: HeldRowsKind::Correlator { left: 3, rows },
        },
    }])
    .assured("correlator copies gather by side");
    let runs = gathered
        .runs
        .iter()
        .map(|run| (run.section, run.rows))
        .collect::<Vec<_>>();
    assert_eq!(
        runs,
        vec![
            (ProcessorStateSection::Left, 2),
            (ProcessorStateSection::Right, 1)
        ]
    );
    let [left, right] = gathered.sections.as_slice() else {
        panic!("a correlator has a left and a right section");
    };
    assert_eq!((left.section, left.rows), (ProcessorStateSection::Left, 3));
    assert_eq!(
        (right.section, right.rows),
        (ProcessorStateSection::Right, 2)
    );
    assert_eq!(
        left.payload[0]
            .as_primitive::<UInt64Type>()
            .values()
            .to_vec(),
        vec![1, 2]
    );
    let [_, timeout_at] = right.appended.as_slice() else {
        panic!("received and timeout columns");
    };
    assert_eq!(
        timeout_at
            .as_primitive::<TimestampNanosecondType>()
            .values()
            .to_vec(),
        vec![103]
    );
    assert!(left.key_fields.is_empty() && right.key_fields.is_empty());
}

/// How long the page test waits for a unit, a branch or a read that should answer at once, so a
/// regression that leaves one waiting fails the test instead of hanging it.
const ANSWERS_WITHIN: std::time::Duration = std::time::Duration::from_secs(30);

/// A branch task's handle, as a unit's registry keeps it, and the commands that task receives.
fn branch_channels() -> (ProcessorBranchTask, mpsc::Receiver<ProcessorBranchCommand>) {
    let (input, _input) = mpsc::channel(1);
    let (commands, received) = mpsc::channel(1);
    let branch = ProcessorBranchTask {
        input,
        commands,
        delivery_lifetime: nervix_primitives::sync::CancellationToken::new(),
        task: nervix_primitives::sync::blocking::Mutex::new(None),
    };
    (branch, received)
}

/// A reorderer unit task that answers an inspection's branch selection as the production one does,
/// through the production answer from its own registry, which holds `branch` live as its
/// unbranched state, while its restore is pending or once it has installed that branch.
fn selecting_unit(branch: ProcessorBranchTask, restore_pending: bool) -> ScheduledNodeTask {
    let mut instances = ProcessorBranches::new();
    instances.insert_restored(
        None,
        Timestamp::from_unix_nanos(1_000),
        7,
        ProcessorBranchSlot::Live(branch),
    );
    let (commands, mut requests) = mpsc::channel(1);
    let task = nervix_primitives::task::spawn(async move {
        while let Some(command) = requests.recv().await {
            nervix_primitives::task::consume_budget().await;
            let ProcessorNodeCommand::SelectStateBranches {
                selection,
                response,
            } = command
            else {
                panic!("an inspection asks its unit only for the branches it reads");
            };
            SelectedStateBranches::answer(response, &instances, selection, restore_pending);
        }
    });
    ScheduledNodeTask { commands, task }
}

/// A unit whose restore is pending refuses its owner page as unrestored without asking any branch
/// task, so the read never reports branches the restore has not installed as an empty state. Once
/// the unit has installed its branch, the same read asks that branch and answers with its page.
#[nervix_primitives::test]
async fn a_unit_still_restoring_its_branches_refuses_its_page_without_reading_a_branch() {
    let runtime = Runtime::default();
    let domain = domain("restoring");
    install_test_domain_execution(
        &runtime,
        &domain,
        Vec::new(),
        DomainRoutingSnapshot::default(),
    );
    let processor = NodeRef::new(
        ModelKind::Reorderer,
        named::<ModelName>("held_notifications"),
    );
    let unit = NodeUnitRef::new(processor.clone(), Unit::SINGLE);
    let bounds = || ProcessorStatePageBounds {
        rows: 10,
        bytes: u64::MAX,
        reply: PageReply::Local,
    };

    let (branch, mut branch_commands) = branch_channels();
    runtime
        .inner
        .executions
        .get_mut(&domain)
        .assured("the test installed the domain's execution")
        .node_tasks
        .insert(unit.clone(), selecting_unit(branch, true));
    let page = timeout(
        ANSWERS_WITHIN,
        runtime.local_processor_state_page(
            &domain,
            &processor,
            Unit::SINGLE,
            StateInspectionBranchSelection::All.into(),
            None,
            bounds(),
        ),
    )
    .await
    .assured("a unit still restoring its branches refuses the read at once");
    let Err(refused) = page else {
        panic!("a unit still restoring its branches answers no page");
    };
    assert!(
        matches!(
            refused.current_context(),
            ProcessorStatePageError::Unrestored
        ),
        "the page is refused as unrestored, found {refused:?}"
    );
    let restoring = runtime
        .inner
        .executions
        .get_mut(&domain)
        .assured("the test installed the domain's execution")
        .node_tasks
        .remove(&unit)
        .assured("the restoring unit is installed");
    drop(restoring.commands);
    timeout(ANSWERS_WITHIN, restoring.task)
        .await
        .assured("the unit ends once nothing can send it a command")
        .assured("the unit task ends without a panic");
    let asked = timeout(ANSWERS_WITHIN, branch_commands.recv())
        .await
        .assured("the branch's commands close once its unit has ended");
    assert!(
        asked.is_none(),
        "the refused read asked no branch task for its rows"
    );

    let (branch, mut branch_commands) = branch_channels();
    runtime
        .inner
        .executions
        .get_mut(&domain)
        .assured("the test installed the domain's execution")
        .node_tasks
        .insert(unit, selecting_unit(branch, false));
    let answering = nervix_primitives::task::spawn(async move {
        let Some(ProcessorBranchCommand::Inspect { response, .. }) = branch_commands.recv().await
        else {
            panic!("the restored unit's read asks its branch for the rows it holds");
        };
        answer_branch_state(
            response,
            7,
            HandledEvents::default(),
            Ok(HeldBranchRows {
                rows: 0,
                bytes: 0,
                copied_bytes: 0,
                kind: HeldRowsKind::Reorderer {
                    key_fields: Vec::new(),
                    rows: Vec::new(),
                },
            }),
        );
    });
    let page = timeout(
        ANSWERS_WITHIN,
        runtime.local_processor_state_page(
            &domain,
            &processor,
            Unit::SINGLE,
            StateInspectionBranchSelection::All.into(),
            None,
            bounds(),
        ),
    )
    .await
    .assured("a restored unit answers its page once its branch answers")
    .assured("a restored unit answers its page");
    timeout(ANSWERS_WITHIN, answering)
        .await
        .assured("the branch is asked for its rows")
        .assured("the branch answers the read it was asked");
    assert_eq!(
        (page.generation, page.counts.rows, page.offset, page.more),
        (7, 0, 0, false),
        "the page names the lifetime of the branch it read and that branch's complete counts"
    );
}

/// The bytes of every note a wide fixture row carries.
const ROW_NOTE: usize = 1024 * 1024;

/// The byte bound a session transfer gives every page, as a page read at the default limit has.
const TRANSFER_BOUND: u64 = 64 * 1024 * 1024;

/// One batch of `rows` rows whose sequences, and order keys, continue at `first`, each with a note
/// of `width` bytes after its sequence.
fn wide_batch(first: u64, rows: u64, width: usize) -> (RelayRecordBatch, Arc<Vec<ReordererRowOrder>>) {
    let schema = test_schema(&[
        ("sequence", ParseAsType::U64),
        ("note", ParseAsType::String),
    ]);
    let mut messages = Vec::new();
    let mut order = Vec::new();
    for sequence in first..first + rows {
        messages.push(RelayMessage {
            key: None,
            record: test_runtime_row([
                ("sequence".to_string(), RuntimeValue::U64(sequence)),
                (
                    "note".to_string(),
                    RuntimeValue::String(format!("{sequence:08}{}", "n".repeat(width))),
                ),
            ]),
            acks: AckSet::empty(),
        });
        order.push(ReordererRowOrder {
            key: vec![ReorderKeyPart::UInt64(sequence)],
            arrival_sequence: sequence,
        });
    }
    let batch = RelayRecordBatch::from_messages(schema, messages)
        .assured("every fixture row matches the fixture schema");
    (batch, Arc::new(order))
}

/// A buffer holding `rows` rows of `width`-byte notes, one batch each, whose sequences run on from
/// zero.
fn wide_buffer(rows: u64, width: usize) -> ReordererOutputBuffer {
    let mut buffer = ReordererOutputBuffer::default();
    for sequence in 0..rows {
        let (batch, order) = wide_batch(sequence, 1, width);
        buffer.push(batch, order, Timestamp::from_unix_nanos(1_000));
    }
    buffer
}

/// Answers every read its branch is asked with the production bounded copy of `buffer`, as a
/// reorderer branch task copies what it holds, until the branch's commands close.
fn answer_reads(
    buffer: ReordererOutputBuffer,
    mut commands: mpsc::Receiver<ProcessorBranchCommand>,
) -> nervix_primitives::task::JoinHandle<()> {
    nervix_primitives::task::spawn(async move {
        while let Some(command) = commands.recv().await {
            nervix_primitives::task::consume_budget().await;
            let ProcessorBranchCommand::Inspect { read, response } = command else {
                panic!("an inspection asks its branch only for the rows it holds");
            };
            let (copied, budget) = copy(&buffer, read);
            answer_branch_state(
                response,
                7,
                HandledEvents::default(),
                Ok(HeldBranchRows {
                    rows: buffer.held_rows(),
                    bytes: buffer.estimated_bytes(),
                    copied_bytes: budget.copied_bytes,
                    kind: HeldRowsKind::Reorderer {
                        key_fields: vec![StdArc::new(Field::new(
                            "order_key",
                            DataType::UInt64,
                            false,
                        ))],
                        rows: copied,
                    },
                }),
            );
        }
    })
}

/// A running reorderer unit of `runtime` whose one unbranched branch holds what `buffer` holds.
struct HoldingReorderer {
    domain: DomainName,
    processor: NodeRef,
    answering: nervix_primitives::task::JoinHandle<()>,
}

impl HoldingReorderer {
    fn install(runtime: &Runtime, name: &str, buffer: ReordererOutputBuffer) -> Self {
        let domain = domain(name);
        install_test_domain_execution(
            runtime,
            &domain,
            Vec::new(),
            DomainRoutingSnapshot::default(),
        );
        let processor = NodeRef::new(
            ModelKind::Reorderer,
            named::<ModelName>("held_notifications"),
        );
        let (branch, branch_commands) = branch_channels();
        runtime
            .inner
            .executions
            .get_mut(&domain)
            .assured("the test installed the domain's execution")
            .node_tasks
            .insert(
                NodeUnitRef::new(processor.clone(), Unit::SINGLE),
                selecting_unit(branch, false),
            );
        Self {
            domain,
            processor,
            answering: answer_reads(buffer, branch_commands),
        }
    }

    /// One page of the unit's held rows at the default row bound and a session's transfer bound,
    /// for the reader `reply` names.
    async fn page(
        &self,
        runtime: &Runtime,
        after: Option<(Option<BranchKeyFingerprint>, u64)>,
        reply: PageReply,
    ) -> error_stack::Result<ProcessorStatePage, ProcessorStatePageError> {
        timeout(
            ANSWERS_WITHIN,
            runtime.local_processor_state_page(
                &self.domain,
                &self.processor,
                Unit::SINGLE,
                StateInspectionBranchSelection::All.into(),
                after,
                ProcessorStatePageBounds {
                    rows: 1024,
                    bytes: TRANSFER_BOUND,
                    reply,
                },
            ),
        )
        .await
        .assured("an owner page of held rows answers within the inspection deadline")
    }

    /// Ends the unit, and with it the branch it answered for.
    async fn end(self, runtime: &Runtime) {
        let unit = runtime
            .inner
            .executions
            .get_mut(&self.domain)
            .assured("the test installed the domain's execution")
            .node_tasks
            .remove(&NodeUnitRef::new(self.processor.clone(), Unit::SINGLE))
            .assured("the test installed the unit");
        drop(unit.commands);
        timeout(ANSWERS_WITHIN, unit.task)
            .await
            .assured("the unit ends once nothing can send it a command")
            .assured("the unit task ends without a panic");
        timeout(ANSWERS_WITHIN, self.answering)
            .await
            .assured("the branch ends once its unit has ended")
            .assured("the branch answered every read without a panic");
    }
}

/// The sequences of the held rows a reorderer page returned, in page order.
fn returned_sequences(page: &ProcessorStatePage) -> Vec<u64> {
    let [section] = page.sections.as_slice() else {
        panic!("a reorderer page carries its one held-row section");
    };
    match section.payload.first() {
        Some(sequence) => sequence.as_primitive::<UInt64Type>().values().to_vec(),
        None => Vec::new(),
    }
}

/// A reorderer page whose copied rows the bulk class cannot hold whole ends before the row it can
/// no longer hold, with complete counts and a continuation, and the pages that follow return every
/// other held row once, in order. The read is never refused while a row can be returned.
#[nervix_primitives::test]
async fn a_reorderer_page_beyond_the_bulk_class_ends_where_it_runs_out_and_the_rest_follow() {
    let runtime = Runtime::default();
    let capacity = runtime.inner.executor.snapshot().bulk_memory.capacity_bytes;
    let note = u64::try_from(ROW_NOTE).assured("a fixture note fits u64");
    // Rows of 1 MiB notes, eight more of them than the whole bulk class holds.
    let held = capacity / note + 8;
    let reorderer = HoldingReorderer::install(&runtime, "paged", wide_buffer(held, ROW_NOTE));
    let mut after = None;
    let mut sequences = Vec::new();
    let mut pages = 0_usize;
    loop {
        let page = reorderer
            .page(&runtime, after, PageReply::Local)
            .await
            .assured("a page the bulk class cannot hold whole ends early instead of being refused");
        assert_eq!(
            page.counts.rows, held,
            "every page reports the complete counts"
        );
        let returned = returned_sequences(&page);
        assert!(!returned.is_empty(), "every page returns at least one row");
        sequences.extend(returned);
        pages += 1;
        if !page.more {
            break;
        }
        after = Some((page.last_branch, page.offset));
    }
    assert!(pages > 1, "the bulk class holds no page of every row");
    assert_eq!(
        sequences,
        (0..held).collect::<Vec<_>>(),
        "the pages return every held row once, in order"
    );
    reorderer.end(&runtime).await;
}

/// A reorderer page another node reads sets aside, as it admits its rows, the bound on its
/// descriptor, its one section body's framing and every returned row's body, beside the charge of
/// the rows' columns, which it keeps until its rows are carried. It therefore ends before the page
/// its owner presents under the same bulk class.
#[nervix_primitives::test]
async fn a_reorderer_page_another_node_reads_sets_aside_its_reply_and_ends_sooner() {
    let runtime = Runtime::default();
    let capacity = runtime.inner.executor.snapshot().bulk_memory.capacity_bytes;
    let note = u64::try_from(ROW_NOTE).assured("a fixture note fits u64");
    // Rows of 1 MiB notes filling five eighths of the bulk class: its owner presents them in one
    // page, while their bodies as well do not fit.
    let held = capacity / note * 5 / 8;
    let reorderer = HoldingReorderer::install(&runtime, "remote", wide_buffer(held, ROW_NOTE));
    let local = reorderer
        .page(&runtime, None, PageReply::Local)
        .await
        .assured("the owner presents a first page");
    let local_rows = returned_sequences(&local).len();
    assert_eq!(
        u64::try_from(local_rows).assured("a bounded row count fits u64"),
        held,
        "the owner presents every held row in one page"
    );
    let local_memory = local
        .memory
        .as_ref()
        .assured("a page keeps the charge of the columns it gathered");
    assert!(
        local_memory.reply.is_none(),
        "a page its owner presents sets nothing aside for a reply"
    );
    drop(local);
    let reply = PageReply::remote(&runtime.inner.executor);
    let PageReply::Remote {
        descriptor,
        framing,
    } = reply
    else {
        panic!("a reply to another node sets memory aside");
    };
    let mut remote = reorderer
        .page(&runtime, None, reply)
        .await
        .assured("the owner answers another node with a first page");
    let remote_rows = returned_sequences(&remote).len();
    assert!(remote_rows > 0, "the first page returns at least one row");
    assert!(
        remote_rows < local_rows,
        "a page another node reads ends sooner ({remote_rows} rows) than one its owner presents \
         ({local_rows} rows)"
    );
    // Every returned row's body: its payload, and its route, order key and two deadlines.
    let appended = u64::try_from(
        std::mem::size_of::<u32>() + std::mem::size_of::<u64>() + 2 * std::mem::size_of::<i64>(),
    )
    .assured("fixed widths fit u64");
    let [section] = remote.sections.as_slice() else {
        panic!("a reorderer page carries its one held-row section");
    };
    let rows = arrow_array::RecordBatch::try_new(
        StdArc::new(arrow_schema::Schema::new(vec![
            Field::new("sequence", DataType::UInt64, false),
            Field::new("note", DataType::Utf8, false),
        ])),
        section.payload.clone(),
    )
    .assured("the payload columns are the fixture's");
    let mut bodies = framing;
    for row in 0..rows.num_rows() {
        let payload = crate::runtime_schema::batch_payload_bytes(&rows.slice(row, 1));
        bodies = bodies + payload + appended;
    }
    let memory = remote
        .memory
        .as_mut()
        .assured("a page another node reads keeps its memory with the rows");
    let set_aside = memory
        .take_reply()
        .assured("a page another node reads sets its reply aside");
    assert_eq!(
        set_aside.descriptor.bytes(),
        descriptor,
        "the page sets aside the bound on one descriptor"
    );
    assert_eq!(
        set_aside.bodies.bytes(),
        bodies,
        "the page sets aside its section's framing and every returned row's body"
    );
    assert_eq!(set_aside.framing, framing);
    drop(remote);
    reorderer.end(&runtime).await;
}

#[test]
fn a_key_part_outside_its_column_type_fails_the_section() {
    let mut copy = held_reorderer_branch(None, &[1], 1);
    let HeldRowsKind::Reorderer { rows, .. } = &mut copy.held.kind else {
        panic!("a reorderer copy");
    };
    rows[0].key = vec![ReorderKeyPart::UInt64(u64::from(u32::MAX) + 1)];
    let error = reorderer_sections(vec![copy])
        .err()
        .assured("a part no u32 holds was never this column's part");
    assert!(matches!(
        error.current_context(),
        ProcessorStatePageError::Columns
    ));
}
