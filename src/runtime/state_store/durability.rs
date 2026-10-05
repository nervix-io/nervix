//! When the writes a runtime state store applied are on stable storage.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Synchronizing the store's database on the storage workers, sharing one
//!   synchronization among every writer waiting at the same time, and refusing every durability
//!   promise once a synchronization failed.
//! - **Depends on.** The store's database and executor.
//! - **Must not know.** What the writes it makes durable hold, who waits for them, or replicas.

use error_stack::{Report, ResultExt as _};
use fjall::PersistMode;
use meticulous::OptionExt as _;
use nervix_execution::{MemoryClass, StorageClass};
use nervix_primitives::sync::{
    Arc, Notify,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use super::{RuntimePersistenceError, RuntimeStateStore};

/// Makes the writes a store already applied durable, sharing one synchronization among every
/// writer that asks while it runs or before it starts.
///
/// A writer applies its write and then takes a ticket. A synchronization covers every ticket issued
/// before it starts, because each of those writes was applied before its ticket was taken. At most
/// one synchronization runs at a time; the writers that asked meanwhile wait for it, and one of
/// them starts the next synchronization when the finished one did not cover them. Every branch that
/// checkpoints at the same time therefore shares a synchronization of the node's storage instead of
/// queuing one each behind the storage workers.
///
/// A synchronization is a [`DurabilityRound`] that the job flushing the storage owns and that
/// records its own outcome. The writer that started it may stop waiting, but the flush runs on, so
/// the round, not that writer, holds the barrier until the flush has ended and its outcome is
/// recorded.
#[derive(Debug)]
pub(super) struct DurabilityBarrier {
    /// The last ticket issued.
    issued: AtomicU64,
    /// Every ticket at or below this is covered by a synchronization that succeeded.
    synchronized: AtomicU64,
    /// Set when a synchronization fails. The operating system may drop the writes it failed to
    /// flush, so a later synchronization cannot prove they reached storage, and the database
    /// refuses every later one anyway: from then on no write is reported durable.
    failed: AtomicBool,
    /// Whether one writer is running a synchronization.
    running: AtomicBool,
    /// How many synchronizations have run.
    rounds: AtomicU64,
    /// Signals the end of every synchronization.
    finished: Notify,
}

impl DurabilityBarrier {
    pub(super) fn new() -> Self {
        Self {
            issued: AtomicU64::new(0),
            synchronized: AtomicU64::new(0),
            failed: AtomicBool::new(false),
            running: AtomicBool::new(false),
            rounds: AtomicU64::new(0),
            finished: Notify::new(),
        }
    }

    /// How many synchronizations have run.
    #[cfg(test)]
    pub(super) fn rounds(&self) -> u64 {
        self.rounds.load(Ordering::SeqCst)
    }

    /// The ticket of a write the caller has just applied.
    fn issue(&self) -> u64 {
        self.issued
            .fetch_add(1, Ordering::SeqCst)
            .checked_add(1)
            .assured("a store cannot issue 2^64 synchronization tickets in the lifetime of a node")
    }

    /// Claim the one synchronization `barrier` runs at a time, or `None` while another one runs.
    fn claim(barrier: &Arc<Self>) -> Option<DurabilityRound> {
        if barrier
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        Some(DurabilityRound {
            barrier: Arc::clone(barrier),
            covered: None,
        })
    }
}

/// The one synchronization a barrier runs at a time, owned by the job that flushes the storage.
///
/// The job starts it just before the flush, then records whether the flush succeeded. Dropping it
/// frees the barrier and wakes the waiting writers. A round dropped before it started, because its
/// job was refused or never ran, records nothing. One dropped after it started without an outcome,
/// as a panic in the flush drops it, refuses every later durability promise, because the flush may
/// have failed.
struct DurabilityRound {
    barrier: Arc<DurabilityBarrier>,
    /// The last ticket this round covers, read when it started.
    covered: Option<u64>,
}

impl DurabilityRound {
    /// Starts the flush, and returns the last ticket it covers: the last one issued, which is read
    /// just before the flush so that it covers every write whose ticket was issued by then.
    fn start(&mut self) -> u64 {
        let covered = self.barrier.issued.load(Ordering::SeqCst);
        self.barrier.rounds.fetch_add(1, Ordering::SeqCst);
        self.covered = Some(covered);
        covered
    }

    /// Records that the flush succeeded: every ticket it covers is durable.
    fn succeeded(mut self) {
        if let Some(covered) = self.covered.take() {
            self.barrier
                .synchronized
                .fetch_max(covered, Ordering::SeqCst);
        }
    }

    /// Records that the flush failed, which refuses this and every later durability promise.
    fn failed(mut self) {
        self.covered = None;
        self.barrier.failed.store(true, Ordering::SeqCst);
    }
}

impl Drop for DurabilityRound {
    fn drop(&mut self) {
        if self.covered.is_some() {
            self.barrier.failed.store(true, Ordering::SeqCst);
        }
        self.barrier.running.store(false, Ordering::SeqCst);
        self.barrier.finished.notify_waiters();
    }
}

impl DurabilityBarrier {
    /// Return once every write the caller applied before the call is covered by a synchronization
    /// that succeeded.
    ///
    /// `run` runs one synchronization of the storage, the round it receives, and that round
    /// records its own outcome. The barrier runs at most one round at a time and lets every caller
    /// waiting meanwhile share the next one. A round that failed refuses this and every later
    /// durability promise.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the external storage driver owns the admitted durability action"
        )
    )]
    async fn synchronize<Run, Ran>(
        barrier: &Arc<Self>,
        run: Run,
    ) -> error_stack::Result<(), RuntimePersistenceError>
    where
        Run: Fn(DurabilityRound) -> Ran,
        Ran: Future<Output = error_stack::Result<(), RuntimePersistenceError>>,
    {
        let ticket = barrier.issue();
        loop {
            nervix_primitives::task::consume_budget().await;
            let finished = barrier.finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            if barrier.synchronized.load(Ordering::SeqCst) >= ticket {
                return Ok(());
            }
            if barrier.failed.load(Ordering::SeqCst) {
                return Err(Report::new(RuntimePersistenceError::Synchronize));
            }
            let Some(round) = Self::claim(barrier) else {
                finished.await;
                continue;
            };
            run(round).await?;
        }
    }
}

impl RuntimeStateStore {
    /// Return once every write this store applied before the call is on stable storage.
    ///
    /// Callers waiting at the same time share synchronizations, and a synchronization runs on the
    /// storage workers, never on the async worker that awaits it.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the external storage driver owns the admitted durability action"
        )
    )]
    pub(in crate::runtime) async fn synchronize(
        &self,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        DurabilityBarrier::synchronize(&self.durability, |round| self.synchronize_storage(round))
            .await
    }

    /// Synchronize the database on a storage worker as `round`, which the storage job owns and
    /// records its outcome in, also when the caller stops waiting for it.
    async fn synchronize_storage(
        &self,
        round: DurabilityRound,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let db = self.db.clone();
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(RuntimePersistenceError::StorageAdmission)?;
        self.executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, _cancellation| {
                    let mut round = round;
                    // Every ticket issued so far belongs to a write that was applied before it was
                    // issued, so starting the round just before the flush is what lets it cover
                    // them.
                    round.start();
                    match db.persist(PersistMode::SyncAll) {
                        Ok(()) => {
                            round.succeeded();
                            Ok(())
                        }
                        Err(error) => {
                            round.failed();
                            Err(Report::new(RuntimePersistenceError::Synchronize)
                                .attach_printable(error.to_string()))
                        }
                    }
                },
            )
            .await
            .change_context(RuntimePersistenceError::StorageExecution)?
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "durability_shuttle_tests.rs"]
mod shuttle_tests;

#[cfg(test)]
mod tests {
    use super::{super::tests::open_store, *};

    /// Writers that ask for durability at the same time share synchronizations instead of each
    /// queuing one behind the storage workers. A writer that asks while a synchronization runs is
    /// covered by the next one at the latest.
    #[nervix_primitives::test]
    async fn concurrent_writers_share_synchronizations() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let store = open_store(&dir);
        let writers = (0..16).map(|_| store.synchronize()).collect::<Vec<_>>();

        for outcome in futures_util::future::join_all(writers).await {
            outcome.expect("every writer should be synchronized");
        }

        let rounds = store.durability.rounds();
        assert!(
            (1..=2).contains(&rounds),
            "sixteen concurrent writers needed {rounds} synchronizations"
        );
        assert_eq!(store.durability.synchronized.load(Ordering::SeqCst), 16);
    }

    /// A synchronization that fails leaves every write it covered, and every later one, without a
    /// durability promise: the database refuses later synchronizations, and none of them could
    /// prove that writes the failed one did not flush reached storage.
    #[nervix_primitives::test]
    async fn a_failed_synchronization_refuses_every_later_durability_promise() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let store = open_store(&dir);
        store
            .synchronize()
            .await
            .expect("the first synchronization should succeed");
        store.durability.failed.store(true, Ordering::SeqCst);

        let refused = store
            .synchronize()
            .await
            .expect_err("a write after a failed synchronization is never reported durable");
        assert!(matches!(
            refused.current_context(),
            RuntimePersistenceError::Synchronize
        ));
    }

    /// Cancelling the writer that runs a synchronization frees the barrier for the others: the next
    /// writer runs its own synchronization instead of waiting for one nobody will finish.
    #[nervix_primitives::test]
    async fn a_cancelled_synchronization_frees_the_barrier() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let store = open_store(&dir);
        let abandoned = DurabilityBarrier::claim(&store.durability)
            .expect("nothing else runs a synchronization");
        drop(abandoned);

        nervix_primitives::time::timeout(std::time::Duration::from_secs(5), store.synchronize())
            .await
            .expect("a freed barrier must not keep writers waiting")
            .expect("the synchronization should succeed");
    }
}
