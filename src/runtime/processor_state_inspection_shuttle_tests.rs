//! Branch-state inspections racing a branch task's batches and its end, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The protocol invariants of a branch-state inspection: a request the branch task
//!   takes is answered once, with a copy of whole batches in the order they arrived; a request the
//!   task no longer takes is refused rather than left waiting; and a task that has ended answers
//!   nothing.
//! - **Depends on.** The production inspection request and answer, the production reorderer buffer
//!   and its bounded copy, the boundary's channels and the server Shuttle runner.
//! - **Must not know.** Owner pages, Row frames, sessions or the interconnect.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

// Unmodeled atomics are not Shuttle scheduling points, so the end the branch task records changes in
// the same scheduling step as the loop exit it records.
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{ParseAsType, Timestamp};
use nervix_primitives::unmodeled::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::{
    runtime::processor_branch_task::ProcessorBranchStopMode,
    runtime_ack::AckSet,
    runtime_schema::{RuntimeValue, test_runtime_row},
};

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";
/// Batches the source hands the branch task while inspections race them.
const BATCHES: u64 = 2;
/// Rows in every batch, so a copy that holds a partial batch is detectable.
const ROWS_PER_BATCH: u64 = 2;
/// Inspections the reader makes while the batches arrive and the task ends.
const INSPECTIONS: usize = 2;

/// One batch of `ROWS_PER_BATCH` rows whose order keys continue the arrival sequence at `first`.
fn batch(first: u64) -> (RelayRecordBatch, Arc<Vec<ReordererRowOrder>>) {
    let schema = test_schema(&[("sequence", ParseAsType::U64)]);
    let mut messages = Vec::new();
    let mut order = Vec::new();
    for sequence in first..first + ROWS_PER_BATCH {
        messages.push(RelayMessage {
            key: None,
            record: test_runtime_row([("sequence".to_string(), RuntimeValue::U64(sequence))]),
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

/// A reorderer branch task served the way the production one serves its channels: it prefers its
/// commands to its next batch, answers an inspection from the production copy of its buffer with
/// the production answer, and ends on `Stop`.
async fn branch_task(
    mut commands: mpsc::Receiver<ProcessorBranchCommand>,
    mut input: mpsc::Receiver<(RelayRecordBatch, Arc<Vec<ReordererRowOrder>>)>,
    ended: StdArc<AtomicBool>,
) {
    let mut buffer = ReordererOutputBuffer::default();
    let mut events = 0_u64;
    let mut receiving = true;
    loop {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::select! {
            biased;
            command = commands.recv() => match command {
                Some(ProcessorBranchCommand::Inspect { read, response }) => {
                    let mut budget = HeldRowsBudget::new(read);
                    let mut copied = Vec::new();
                    let held = buffer
                        .copy_held(0, Timestamp::from_unix_nanos(0), &mut budget, &mut copied)
                        .map(|()| HeldBranchRows {
                            rows: buffer.held_rows(),
                            bytes: buffer.estimated_bytes(),
                            copied_bytes: budget.copied_bytes,
                            kind: HeldRowsKind::Reorderer {
                                key_fields: Vec::new(),
                                rows: copied,
                            },
                        });
                    answer_branch_state(response, 1, events, held);
                }
                Some(ProcessorBranchCommand::Stop(_)) | None => break,
                Some(ProcessorBranchCommand::Checkpoint { .. }) => {
                    panic!("the model never asks its branch for a checkpoint");
                }
            },
            received = input.recv(), if receiving => match received {
                Some((batch, order)) => {
                    buffer.push(batch, order, Timestamp::from_unix_nanos(1));
                    events += 1;
                }
                None => receiving = false,
            },
        }
    }
    ended.store(true, Ordering::Relaxed);
}

/// Inspections racing the batches and the task's end: each one is answered with whole batches in
/// arrival order, or refused once the task no longer takes it, and never answered after the end.
fn inspections_racing_batches_and_the_end() {
    shuttle::future::block_on(async {
        let (commands, command_requests) = mpsc::channel(1);
        let (unused_input, _unused_receiver) = mpsc::channel(1);
        let task = StdArc::new(ProcessorBranchTask {
            input: unused_input,
            commands: commands.clone(),
            task: nervix_primitives::sync::blocking::Mutex::new(None),
        });
        let (batches, batch_requests) = mpsc::channel(1);
        let ended = StdArc::new(AtomicBool::new(false));
        let branch = nervix_primitives::task::spawn(branch_task(
            command_requests,
            batch_requests,
            ended.clone(),
        ));
        let source = nervix_primitives::task::spawn(async move {
            for index in 0..BATCHES {
                batches
                    .send(batch(index * ROWS_PER_BATCH))
                    .await
                    .assured("the branch task takes every batch until it is stopped after them");
            }
        });
        let inspector = {
            let task = task.clone();
            let ended = ended.clone();
            nervix_primitives::task::spawn(async move {
                for _ in 0..INSPECTIONS {
                    let ended_before = ended.load(Ordering::Relaxed);
                    let read = BranchStateRead {
                        skip: 0,
                        max_rows: usize::MAX,
                        max_bytes: u64::MAX,
                    };
                    match read_branch_state(&task, read).await {
                        Ok(copy) => {
                            assert!(
                                !ended_before,
                                "a branch task that ended before the request answered it"
                            );
                            let HeldRowsKind::Reorderer { rows, .. } = copy.held.kind else {
                                panic!("a reorderer answers with reorderer rows");
                            };
                            let copied = u64::try_from(rows.len()).assured("bounded rows fit u64");
                            assert_eq!(copied, copy.held.rows, "an unbounded copy holds every row");
                            assert_eq!(
                                copied,
                                copy.events * ROWS_PER_BATCH,
                                "the copy holds exactly the whole batches the task had taken"
                            );
                            for (position, row) in rows.iter().enumerate() {
                                let arrival =
                                    u64::try_from(position).assured("bounded rows fit u64");
                                assert_eq!(
                                    row.key,
                                    vec![ReorderKeyPart::UInt64(arrival)],
                                    "the copy keeps the arrival order of the batches it holds"
                                );
                            }
                        }
                        Err(error) => assert!(
                            matches!(error.current_context(), BranchStateReadError::Ended),
                            "a request the task no longer takes is refused as ended: {error:?}"
                        ),
                    }
                }
            })
        };
        source.await.assured(MODEL_TASK_JOINS);
        commands
            .send(ProcessorBranchCommand::Stop(
                ProcessorBranchStopMode::Detach,
            ))
            .await
            .assured("the branch task takes commands until it stops");
        branch.await.assured(MODEL_TASK_JOINS);
        assert!(ended.load(Ordering::Relaxed));
        inspector.await.assured(MODEL_TASK_JOINS);
        let late = read_branch_state(
            &task,
            BranchStateRead {
                skip: 0,
                max_rows: 1,
                max_bytes: u64::MAX,
            },
        )
        .await;
        match late {
            Ok(_) => panic!("an inspection after the task ended is refused, never answered"),
            Err(error) => assert!(
                matches!(error.current_context(), BranchStateReadError::Ended),
                "an inspection after the task ended is refused as ended: {error:?}"
            ),
        }
    });
}

#[test]
fn shuttle_a_branch_inspection_answers_whole_batches_in_arrival_order_or_is_refused_after_the_end()
{
    check_interleavings(inspections_racing_batches_and_the_end);
}
