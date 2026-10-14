//! The runtime's ownership of branch snapshot tasks, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The ordering invariant of the snapshot task owner: a runtime that closes it and
//!   waits returns only after every task started under it has made its last write, whether that
//!   task's branch told it to stop, was ended without telling it anything, or is still there.
//! - **Depends on.** The production task owner and the Shuttle runner of the model harness.
//! - **Must not know.** What a snapshot holds, the state store, or the executor.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::{
    sync::{
        StdArc,
        atomic::{AtomicUsize, Ordering},
        watch,
    },
    task::JoinHandle,
};

use super::BranchSnapshotTasks;

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";

/// How many snapshot tasks the check starts, one for each way a branch leaves its task.
const SNAPSHOT_TASKS: usize = 3;

/// One snapshot task as the runtime starts it. It waits for its branch's stop, whether sent or
/// dropped, or for the runtime's, then makes its last write, which yields before it completes as a
/// storage job does, and ends.
fn snapshot_task(
    tasks: &BranchSnapshotTasks,
    branch_stop: &watch::Sender<bool>,
    written: StdArc<AtomicUsize>,
) -> JoinHandle<()> {
    let mut branch_stopped = branch_stop.subscribe();
    let runtime_stopping = tasks.stopping();
    tasks.spawn(async move {
        // The scheduler explores which stop arrives first. A stop that finds both arrived takes
        // them in this order, so the choice needs no randomness the exploration cannot replay.
        nervix_primitives::select! {
            biased;
            _ = branch_stopped.changed() => {}
            _ = runtime_stopping.cancelled() => {}
        }
        nervix_primitives::task::yield_now().await;
        written.fetch_add(1, Ordering::SeqCst);
    })
}

/// A runtime closes its snapshot task owner while one branch stops in time, one is ended at its
/// stop grace and drops what it held, and one never tells its snapshot task anything. Whichever
/// comes first, the wait returns only once every task has made its last write. A task that waited
/// for its branch alone would leave the wait hanging on the third branch, which Shuttle reports as
/// a deadlock.
fn a_closing_runtime_racing_its_branches() {
    shuttle::future::block_on(async {
        let tasks = BranchSnapshotTasks::default();
        let written = StdArc::new(AtomicUsize::new(0));
        let (stopped_in_time, _) = watch::channel(false);
        let (ended_at_its_grace, _) = watch::channel(false);
        let (silent, _) = watch::channel(false);
        let joins = [
            snapshot_task(&tasks, &stopped_in_time, written.clone()),
            snapshot_task(&tasks, &ended_at_its_grace, written.clone()),
            snapshot_task(&tasks, &silent, written.clone()),
        ];
        let branches = nervix_primitives::task::spawn(async move {
            stopped_in_time.send_replace(true);
            drop(ended_at_its_grace);
        });

        tasks.close();
        tasks.wait().await;
        assert_eq!(
            written.load(Ordering::SeqCst),
            SNAPSHOT_TASKS,
            "a stopping runtime finished waiting before every snapshot task had written"
        );

        branches.await.assured(MODEL_TASK_JOINS);
        for join in joins {
            join.await.assured(MODEL_TASK_JOINS);
        }
        drop(silent);
    });
}

#[test]
fn shuttle_a_closed_snapshot_task_owner_is_waited_for_until_every_task_has_written() {
    check_interleavings(a_closing_runtime_racing_its_branches);
}
