//! The runtime's ownership of snapshot tasks and checkpoint writers, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The ordering invariants of the snapshot task owner: a runtime that closes it and
//!   waits returns only after every task started under it has made its last write, whether that
//!   task's branch told it to stop, was ended without telling it anything, or is still there; a
//!   domain's stop returns only after every task of that domain has written, waits for no other
//!   domain's task, and never loses a task registered while it removes the domain's tracker; and a
//!   placement's checkpoint writer makes its last write once its domain's stop tells it to or its
//!   placement is dropped, never once a reassignment aborted it.
//! - **Depends on.** The production task owner and the Shuttle runner of the model harness.
//! - **Must not know.** What a snapshot holds, the state store, or the executor.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::DomainName;
use nervix_primitives::{
    sync::{
        StdArc,
        atomic::{AtomicUsize, Ordering},
        watch,
    },
    task::JoinHandle,
};

use super::{CheckpointWriter, StateSnapshotTasks};

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";

/// How many snapshot tasks the check starts, one for each way a branch leaves its task.
const SNAPSHOT_TASKS: usize = 3;

/// How many snapshot tasks of the stopping domain the domain check starts, one for each way a
/// branch its stop ends leaves its task.
const STOPPED_DOMAIN_TASKS: usize = 2;

/// A domain named `name`.
fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the check names its domains with valid identifiers")
}

/// One snapshot task of a branch of `domain` as the runtime starts it. It waits for its branch's
/// stop, whether sent or dropped, or for the runtime's, then makes its last write, which yields
/// before it completes as a storage job does, and ends.
fn snapshot_task(
    tasks: &StateSnapshotTasks,
    domain: &DomainName,
    branch_stop: &watch::Sender<bool>,
    written: StdArc<AtomicUsize>,
) -> JoinHandle<()> {
    let mut branch_stopped = branch_stop.subscribe();
    let runtime_stopping = tasks.stopping();
    tasks.spawn(domain, async move {
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
        let tasks = StateSnapshotTasks::default();
        let domain = domain("default");
        let written = StdArc::new(AtomicUsize::new(0));
        let (stopped_in_time, _) = watch::channel(false);
        let (ended_at_its_grace, _) = watch::channel(false);
        let (silent, _) = watch::channel(false);
        let joins = [
            snapshot_task(&tasks, &domain, &stopped_in_time, written.clone()),
            snapshot_task(&tasks, &domain, &ended_at_its_grace, written.clone()),
            snapshot_task(&tasks, &domain, &silent, written.clone()),
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

/// A domain stops while one of its branches stops in time and one is ended at its stop grace,
/// another domain's branch keeps running, and a branch of the stopping domain appears and stops
/// while the stop waits and removes the domain's tracker. Whichever comes first, the stop returns
/// only once both tasks of the branches it ended have written; it never waits for the other
/// domain's task, whose branch only stops after the wait, so waiting for it would hang the check,
/// which Shuttle reports as a deadlock. The late task is never lost: the stop waits for it when it
/// registered in time, and a second stop waits for it otherwise, which it could not if the first
/// stop had removed the tracker the registration joined.
fn a_stopping_domain_racing_its_branches() {
    shuttle::future::block_on(async {
        let tasks = StdArc::new(StateSnapshotTasks::default());
        let stopping = domain("stopping");
        let running = domain("running");
        let written = StdArc::new(AtomicUsize::new(0));
        let written_late = StdArc::new(AtomicUsize::new(0));
        let written_elsewhere = StdArc::new(AtomicUsize::new(0));
        let (stopped_in_time, _) = watch::channel(false);
        let (ended_at_its_grace, _) = watch::channel(false);
        let (still_running, _) = watch::channel(false);
        let joins = [
            snapshot_task(&tasks, &stopping, &stopped_in_time, written.clone()),
            snapshot_task(&tasks, &stopping, &ended_at_its_grace, written.clone()),
        ];
        let elsewhere = snapshot_task(&tasks, &running, &still_running, written_elsewhere.clone());
        let branches = nervix_primitives::task::spawn(async move {
            stopped_in_time.send_replace(true);
            drop(ended_at_its_grace);
        });
        let late = nervix_primitives::task::spawn({
            let tasks = tasks.clone();
            let stopping = stopping.clone();
            let written_late = written_late.clone();
            async move {
                let (late_branch, _) = watch::channel(false);
                let late_task = snapshot_task(&tasks, &stopping, &late_branch, written_late);
                late_branch.send_replace(true);
                // Nothing joins the late task, as nothing joins the snapshot task of a branch ended
                // at its grace: only the stops below and the owner's last wait can observe it.
                drop(late_task);
            }
        });

        tasks.wait_for_domain(&stopping).await;
        assert_eq!(
            written.load(Ordering::SeqCst),
            STOPPED_DOMAIN_TASKS,
            "a stopping domain finished waiting before every branch it ended had written"
        );
        assert_eq!(
            written_elsewhere.load(Ordering::SeqCst),
            0,
            "another domain's branch kept running, so its task had nothing to write"
        );

        late.await.assured(MODEL_TASK_JOINS);
        tasks.wait_for_domain(&stopping).await;
        assert_eq!(
            written_late.load(Ordering::SeqCst),
            1,
            "a task registered while a stop removed its domain's tracker was not waited for"
        );

        still_running.send_replace(true);
        branches.await.assured(MODEL_TASK_JOINS);
        for join in joins {
            join.await.assured(MODEL_TASK_JOINS);
        }
        elsewhere.await.assured(MODEL_TASK_JOINS);
        assert_eq!(written_elsewhere.load(Ordering::SeqCst), 1);

        // Shuttle hands a task's result to its join before it drops the task's future, and each
        // tracker token that future holds counts down with a modeled operation. Closing the owner
        // and waiting, as node teardown does, ends the check only once every snapshot task has
        // dropped its tokens, so none is still counting down when the execution ends.
        tasks.close();
        tasks.wait().await;
    });
}

#[test]
fn shuttle_a_stopping_domain_waits_for_its_own_snapshot_tasks_and_keeps_late_ones() {
    check_interleavings(a_stopping_domain_racing_its_branches);
}

/// How many checkpoint writers of the stopping domain the check has write their last state: the
/// one its stop tells and the one whose placement is dropped.
const TOLD_OR_DROPPED_WRITERS: usize = 2;

/// The checkpoint writer of one placement of `domain` as the runtime starts it. It waits for its
/// last write's signal, then makes that write, which yields before it completes as a storage job
/// does, and ends.
fn checkpoint_writer(
    tasks: &StateSnapshotTasks,
    domain: &DomainName,
    written: StdArc<AtomicUsize>,
) -> CheckpointWriter {
    tasks.spawn_checkpoint_writer(domain, move |last_write| async move {
        last_write.arrived().await;
        nervix_primitives::task::yield_now().await;
        written.fetch_add(1, Ordering::SeqCst);
    })
}

/// A domain stops while one of its placements' checkpoint writers is told to write by the stop,
/// another placement is dropped without its stop, as a failed build drops what it started, a third
/// is aborted by a reassignment that replaces its runtime, and another domain's writer keeps
/// running. Whichever comes first, the stop returns only once the told and the dropped writers
/// have made their last write, and never waits for the other domain's writer, whose stop only
/// comes when the runtime closes; a stop that waited for it would hang the check, which Shuttle
/// reports as a deadlock. The aborted writer never writes, even though its guard goes away after
/// it: its signal reaches nobody. A closing runtime then has the other domain's writer write too.
fn a_stopping_domain_racing_its_checkpoint_writers() {
    shuttle::future::block_on(async {
        let tasks = StdArc::new(StateSnapshotTasks::default());
        let stopping = domain("stopping");
        let running = domain("running");
        let written = StdArc::new(AtomicUsize::new(0));
        let written_by_aborted = StdArc::new(AtomicUsize::new(0));
        let written_elsewhere = StdArc::new(AtomicUsize::new(0));
        let told = checkpoint_writer(&tasks, &stopping, written.clone());
        let dropped = checkpoint_writer(&tasks, &stopping, written.clone());
        let aborted = checkpoint_writer(&tasks, &stopping, written_by_aborted.clone());
        let elsewhere = checkpoint_writer(&tasks, &running, written_elsewhere.clone());
        let placements = nervix_primitives::task::spawn(async move {
            drop(dropped);
            aborted.abort().await;
        });

        told.write_last();
        tasks.wait_for_domain(&stopping).await;
        assert_eq!(
            written.load(Ordering::SeqCst),
            TOLD_OR_DROPPED_WRITERS,
            "a stopping domain finished waiting before its told and dropped writers had written"
        );
        assert_eq!(
            written_by_aborted.load(Ordering::SeqCst),
            0,
            "a writer a reassignment aborted made its last write"
        );
        assert_eq!(
            written_elsewhere.load(Ordering::SeqCst),
            0,
            "another domain's writer was told to write by this domain's stop"
        );

        placements.await.assured(MODEL_TASK_JOINS);
        tasks.close();
        tasks.wait().await;
        assert_eq!(
            written_elsewhere.load(Ordering::SeqCst),
            1,
            "a closing runtime finished waiting before a running domain's writer had written"
        );
        drop(elsewhere);
    });
}

#[test]
fn shuttle_a_stopping_domain_waits_for_the_checkpoint_writers_it_tells_and_the_ones_dropped() {
    check_interleavings(a_stopping_domain_racing_its_checkpoint_writers);
}
