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
