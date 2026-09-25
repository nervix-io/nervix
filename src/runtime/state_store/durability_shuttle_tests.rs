//! The runtime state store's durability barrier, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The coverage, exclusion, liveness and fail-stop invariants the barrier is held to
//!   while writers that applied writes wait for, share, fail and abandon synchronizations.
//! - **Depends on.** The production durability barrier and the server Shuttle runner.
//! - **Must not know.** The database a synchronization flushes, or what the writes hold.

// The standard library's atomics are not Shuttle scheduling points, so each record below changes in
// the same scheduling step as the operation it records.
use std::sync::{
    Arc as StdArc,
    atomic::{AtomicBool as StdAtomicBool, AtomicU64 as StdAtomicU64, Ordering as StdOrdering},
};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_recovery::Discarded as _;

use super::DurabilityBarrier;
use crate::{runtime::state_store::RuntimePersistenceError, shuttle_test::check_interleavings};

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
        }
    }

    /// One synchronization of the modeled storage. It covers every write applied when it starts,
    /// exactly as the production round reads the barrier's last ticket just before it flushes.
    async fn round(
        &self,
        barrier: &DurabilityBarrier,
    ) -> error_stack::Result<u64, RuntimePersistenceError> {
        // A writer applies its write before it takes its ticket, so every write whose ticket the
        // round covers is among the writes applied by the time the round has read its ticket.
        let covered = barrier.covered_by_a_round_starting_now();
        let covered_writes = self.applied.load(StdOrdering::SeqCst);
        let running = RunningRound::enter(&self.running);
        let round = self.started.fetch_add(1, StdOrdering::SeqCst);
        tokio::task::yield_now().await;
        drop(running);
        if round == 0
            && let FailingRound::First = self.failing
        {
            self.failed.store(true, StdOrdering::SeqCst);
            return Err(Report::new(RuntimePersistenceError::Synchronize));
        }
        self.durable.fetch_max(covered_writes, StdOrdering::SeqCst);
        Ok(covered)
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
async fn write(barrier: StdArc<DurabilityBarrier>, record: StdArc<BarrierRecord>) {
    let written = record
        .applied
        .fetch_add(1, StdOrdering::SeqCst)
        .checked_add(1)
        .assured("a model applies a handful of writes");
    let outcome = barrier.synchronize(|| record.round(&barrier)).await;
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
        let barrier = StdArc::new(DurabilityBarrier::new());
        let record = StdArc::new(BarrierRecord::new(failing));
        let mut writers = Vec::new();
        for _ in 0..WRITERS {
            writers.push(tokio::spawn(write(barrier.clone(), record.clone())));
        }
        for writer in writers {
            writer.await.assured(MODEL_TASK_JOINS);
        }
        if let FailingRound::First = failing {
            let refused = barrier.synchronize(|| record.round(&barrier)).await;
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

/// A writer abandoned while it runs a synchronization frees the barrier, and the writers waiting
/// on it elect another runner instead of waiting for a synchronization nobody will finish. The
/// writer is abandoned the way a cancelled caller abandons it: its synchronization future is
/// dropped wherever it is waiting.
fn an_abandoned_synchronization_frees_the_barrier() {
    shuttle::future::block_on(async {
        let barrier = StdArc::new(DurabilityBarrier::new());
        let record = StdArc::new(BarrierRecord::new(FailingRound::None));
        let (abandon, abandoned) = tokio::sync::oneshot::channel::<()>();
        let abandoning = tokio::spawn({
            let barrier = barrier.clone();
            let record = record.clone();
            async move {
                // Shuttle's depth-first search supplies no random data, so the branches are
                // polled in order rather than in tokio's random order.
                tokio::select! {
                    biased;
                    () = write(barrier, record) => {}
                    _ = abandoned => {}
                }
            }
        });
        let waiting = tokio::spawn(write(barrier.clone(), record.clone()));
        tokio::task::yield_now().await;
        abandon
            .send(())
            .discarded("the abandoning writer may already have finished on its own");
        abandoning.await.assured(MODEL_TASK_JOINS);
        waiting.await.assured(MODEL_TASK_JOINS);
    });
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
