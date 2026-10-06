//! The runtime state store's durability barrier, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The coverage, exclusion, liveness and fail-stop invariants the barrier is held to
//!   while writers that applied writes wait for, share, fail and abandon synchronizations.
//! - **Depends on.** The production durability barrier and the model harness's Shuttle runner.
//! - **Must not know.** The database a synchronization flushes, or what the writes hold.

// Unmodeled atomics are not Shuttle scheduling points, so each record below changes in the same
// scheduling step as the operation it records.
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::{
    sync::{Arc, StdArc, blocking::Mutex, oneshot},
    task::JoinHandle,
    unmodeled::sync::atomic::{
        AtomicBool as StdAtomicBool, AtomicU64 as StdAtomicU64, Ordering as StdOrdering,
    },
};
use nervix_recovery::Discarded as _;

use super::{DurabilityBarrier, DurabilityRound};
use crate::runtime::state_store::RuntimePersistenceError;

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";

/// Writers that apply one write each and then ask the barrier for durability.
const WRITERS: u64 = 3;

/// Which synchronization of a model execution fails, as a failing disk would.
#[derive(Clone, Copy)]
enum FailingRound {
    None,
    First,
}

/// What the writers and synchronizations of one model execution record of each other.
struct BarrierRecord {
    /// Writes applied so far, each numbered by the value this reached when it was applied.
    applied: StdAtomicU64,
    /// Every write numbered at or below this is covered by a synchronization that succeeded.
    durable: StdAtomicU64,
    /// Set while a synchronization runs, to catch two running at once.
    running: StdAtomicBool,
    /// Synchronizations started so far.
    started: StdAtomicU64,
    failing: FailingRound,
    /// Set once a synchronization failed.
    failed: StdAtomicBool,
    /// The storage jobs started so far, which the model joins before it ends, as no job outlives
    /// the node that runs it.
    jobs: Mutex<Vec<JoinHandle<()>>>,
}

impl BarrierRecord {
    fn new(failing: FailingRound) -> Self {
        Self {
            applied: StdAtomicU64::new(0),
            durable: StdAtomicU64::new(0),
            running: StdAtomicBool::new(false),
            started: StdAtomicU64::new(0),
            failing,
            failed: StdAtomicBool::new(false),
            jobs: Mutex::new(Vec::new()),
        }
    }

    /// Waits for every storage job started so far to end.
    async fn join_jobs(&self) {
        let jobs = std::mem::take(&mut *self.jobs.lock());
        for job in jobs {
            job.await.assured(MODEL_TASK_JOINS);
        }
    }

    /// One synchronization of the modeled storage, run as `round`. It covers every write applied
    /// when it starts, exactly as the production job starts its round just before it flushes.
    async fn round(
        &self,
        mut round: DurabilityRound,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        // A writer applies its write before it takes its ticket, so every write whose ticket the
        // round covers is among the writes applied by the time the round has started.
        round.start();
        let covered_writes = self.applied.load(StdOrdering::SeqCst);
        let running = RunningRound::enter(&self.running);
        let started = self.started.fetch_add(1, StdOrdering::SeqCst);
        nervix_primitives::task::yield_now().await;
        drop(running);
        if started == 0
            && let FailingRound::First = self.failing
        {
            self.failed.store(true, StdOrdering::SeqCst);
            round.failed();
            return Err(Report::new(RuntimePersistenceError::Synchronize));
        }
        self.durable.fetch_max(covered_writes, StdOrdering::SeqCst);
        round.succeeded();
        Ok(())
    }
}

/// Runs `round` the way the storage workers run a synchronization: as a job of its own that the
/// writer awaits, and that keeps running to its end when the writer is abandoned.
async fn detached_round(
    record: StdArc<BarrierRecord>,
    round: DurabilityRound,
) -> error_stack::Result<(), RuntimePersistenceError> {
    let (outcome, reported) = oneshot::channel();
    let job_record = record.clone();
    let job = nervix_primitives::task::spawn(async move {
        let finished = job_record.round(round).await;
        outcome
            .send(finished)
            .discarded("the writer awaiting the job may have been abandoned");
    });
    record.jobs.lock().push(job);
    reported
        .await
        .assured("a job reports its outcome before it ends")
}

/// A synchronization queued behind the storage workers that has not started. Nothing runs it, so it
/// ends only when the writer awaiting it is abandoned, which drops the job with its round.
async fn queued_round(round: DurabilityRound) -> error_stack::Result<(), RuntimePersistenceError> {
    let _queued = round;
    std::future::pending().await
}

/// Apply one write, then ask the barrier for durability with synchronizations run as detached
/// jobs. A success must be backed by a synchronization that succeeded and covered the write.
async fn write_with_detached_rounds(
    barrier: Arc<DurabilityBarrier>,
    record: StdArc<BarrierRecord>,
) {
    let written = record
        .applied
        .fetch_add(1, StdOrdering::SeqCst)
        .checked_add(1)
        .assured("a model applies a handful of writes");
    let outcome =
        DurabilityBarrier::synchronize(&barrier, |round| detached_round(record.clone(), round))
            .await;
    if outcome.is_ok() {
        assert!(
            record.durable.load(StdOrdering::SeqCst) >= written,
            "write {written} was reported durable before a synchronization covering it succeeded"
        );
    }
}

/// One synchronization in progress. Leaving it, also by being abandoned, clears the record.
struct RunningRound<'a>(&'a StdAtomicBool);

impl<'a> RunningRound<'a> {
    fn enter(running: &'a StdAtomicBool) -> Self {
        let overlapping = running.swap(true, StdOrdering::SeqCst);
        assert!(!overlapping, "two synchronizations ran at the same time");
        Self(running)
    }
}

impl Drop for RunningRound<'_> {
    fn drop(&mut self) {
        self.0.store(false, StdOrdering::SeqCst);
    }
}

/// Apply one write, then ask the barrier for durability. A success must be backed by a
/// synchronization that started after the write was applied and succeeded; a synchronization that
/// failed before the call returned refuses every later promise.
async fn write(barrier: Arc<DurabilityBarrier>, record: StdArc<BarrierRecord>) {
    let written = record
        .applied
        .fetch_add(1, StdOrdering::SeqCst)
        .checked_add(1)
        .assured("a model applies a handful of writes");
    let outcome = DurabilityBarrier::synchronize(&barrier, |round| record.round(round)).await;
    match outcome {
        Ok(()) => {
            assert!(
                record.durable.load(StdOrdering::SeqCst) >= written,
                "write {written} was reported durable before a synchronization covering it \
                 succeeded"
            );
        }
        Err(error) => {
            assert!(
                matches!(
                    error.current_context(),
                    RuntimePersistenceError::Synchronize
                ),
                "a refused durability promise must name the failed synchronization"
            );
            assert!(
                record.failed.load(StdOrdering::SeqCst),
                "write {written} was refused although no synchronization failed"
            );
        }
    }
}

/// Writers that ask at the same time share synchronizations and each is reported durable only
/// once a synchronization that covers its write succeeded. No two synchronizations overlap, and no
/// writer waits for a synchronization nobody will run, which Shuttle would report as a deadlock.
/// Once one synchronization fails, every writer it did not already cover is refused.
fn writers_share_synchronizations(failing: FailingRound) {
    shuttle::future::block_on(async {
        let barrier = Arc::new(DurabilityBarrier::new());
        let record = StdArc::new(BarrierRecord::new(failing));
        let mut writers = Vec::new();
        for _ in 0..WRITERS {
            writers.push(nervix_primitives::task::spawn(write(
                barrier.clone(),
                record.clone(),
            )));
        }
        for writer in writers {
            writer.await.assured(MODEL_TASK_JOINS);
        }
        if let FailingRound::First = failing {
            let refused =
                DurabilityBarrier::synchronize(&barrier, |round| record.round(round)).await;
            assert!(
                refused.is_err(),
                "a write after a failed synchronization was reported durable"
            );
        }
    });
}

fn concurrent_writers_are_reported_durable_only_after_a_covering_synchronization() {
    writers_share_synchronizations(FailingRound::None);
}

fn a_failed_synchronization_refuses_every_uncovered_writer() {
    writers_share_synchronizations(FailingRound::First);
}

/// Apply one write, then ask the barrier for durability with every synchronization this writer
/// starts still queued behind the storage workers, as if they never reached a worker.
async fn write_with_queued_rounds(barrier: Arc<DurabilityBarrier>, record: StdArc<BarrierRecord>) {
    let written = record
        .applied
        .fetch_add(1, StdOrdering::SeqCst)
        .checked_add(1)
        .assured("a model applies a handful of writes");
    let outcome = DurabilityBarrier::synchronize(&barrier, queued_round).await;
    if outcome.is_ok() {
        assert!(
            record.durable.load(StdOrdering::SeqCst) >= written,
            "write {written} was reported durable before a synchronization covering it succeeded"
        );
    }
}

/// A writer abandoned while the synchronization it started still waits for the storage workers
/// frees the barrier: the queued job is dropped with its round before the round starts, and the
/// writers waiting on it elect another runner instead of waiting for a synchronization nobody will
/// run. The writer is abandoned the way a cancelled caller abandons it: its synchronization future
/// is dropped wherever it is waiting.
fn an_abandoned_synchronization_frees_the_barrier() {
    shuttle::future::block_on(async {
        let barrier = Arc::new(DurabilityBarrier::new());
        let record = StdArc::new(BarrierRecord::new(FailingRound::None));
        let (abandon, abandoned) = oneshot::channel::<()>();
        let abandoning = nervix_primitives::task::spawn({
            let barrier = barrier.clone();
            let record = record.clone();
            async move {
                // Shuttle's depth-first search supplies no random data, so the branches are
                // polled in order rather than in tokio's random order.
                nervix_primitives::select! {
                    biased;
                    () = write_with_queued_rounds(barrier, record) => {}
                    _ = abandoned => {}
                }
            }
        });
        let waiting = nervix_primitives::task::spawn(write(barrier.clone(), record.clone()));
        nervix_primitives::task::yield_now().await;
        abandon
            .send(())
            .discarded("the abandoning writer may already have finished on its own");
        abandoning.await.assured(MODEL_TASK_JOINS);
        waiting.await.assured(MODEL_TASK_JOINS);
    });
}

/// A writer abandoned while the synchronization it started still runs leaves that synchronization
/// running, as a storage job keeps running when the writer awaiting it is dropped. The barrier must
/// not start another synchronization beside it, and the outcome of the abandoned one must still
/// decide the writers it covers: a write it did not flush is never reported durable.
fn a_synchronization_outliving_its_abandoned_writer_keeps_the_barrier(failing: FailingRound) {
    shuttle::future::block_on(async {
        let barrier = Arc::new(DurabilityBarrier::new());
        let record = StdArc::new(BarrierRecord::new(failing));
        let (abandon, abandoned) = oneshot::channel::<()>();
        let abandoning = nervix_primitives::task::spawn({
            let barrier = barrier.clone();
            let record = record.clone();
            async move {
                // Shuttle's depth-first search supplies no random data, so the branches are
                // polled in order rather than in tokio's random order.
                nervix_primitives::select! {
                    biased;
                    () = write_with_detached_rounds(barrier, record) => {}
                    _ = abandoned => {}
                }
            }
        });
        let waiting = nervix_primitives::task::spawn(write_with_detached_rounds(
            barrier.clone(),
            record.clone(),
        ));
        nervix_primitives::task::yield_now().await;
        abandon
            .send(())
            .discarded("the abandoning writer may already have finished on its own");
        abandoning.await.assured(MODEL_TASK_JOINS);
        waiting.await.assured(MODEL_TASK_JOINS);
        record.join_jobs().await;
    });
}

fn an_abandoned_writers_synchronization_keeps_the_barrier_until_it_ends() {
    a_synchronization_outliving_its_abandoned_writer_keeps_the_barrier(FailingRound::None);
}

fn an_abandoned_writers_failed_synchronization_still_refuses_what_it_covered() {
    a_synchronization_outliving_its_abandoned_writer_keeps_the_barrier(FailingRound::First);
}

#[test]
fn shuttle_an_abandoned_writers_synchronization_keeps_the_barrier_until_it_ends() {
    check_interleavings(an_abandoned_writers_synchronization_keeps_the_barrier_until_it_ends);
}

#[test]
fn shuttle_an_abandoned_writers_failed_synchronization_still_refuses_what_it_covered() {
    check_interleavings(an_abandoned_writers_failed_synchronization_still_refuses_what_it_covered);
}

#[test]
fn shuttle_concurrent_writers_are_reported_durable_only_after_a_covering_synchronization() {
    check_interleavings(
        concurrent_writers_are_reported_durable_only_after_a_covering_synchronization,
    );
}

#[test]
fn shuttle_a_failed_synchronization_refuses_every_uncovered_writer() {
    check_interleavings(a_failed_synchronization_refuses_every_uncovered_writer);
}

#[test]
fn shuttle_an_abandoned_synchronization_frees_the_barrier() {
    check_interleavings(an_abandoned_synchronization_frees_the_barrier);
}
