//! When the writes a runtime state store applied are on stable storage.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Synchronizing the store's database on the storage workers, sharing one
//!   synchronization among every writer waiting at the same time, and refusing every durability
//!   promise once a synchronization failed.
//! - **Depends on.** The store's database and executor.
//! - **Must not know.** What the writes it makes durable hold, who waits for them, or replicas.

#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use error_stack::{Report, ResultExt as _};
use fjall::PersistMode;
use meticulous::OptionExt as _;
use nervix_execution::{MemoryClass, StorageClass};
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::Notify;

use super::{RuntimePersistenceError, RuntimeStateStore};

/// Makes the writes a store already applied durable, sharing one synchronization among every
/// writer that asks while it runs or before it starts.
///
/// A writer applies its write and then takes a ticket. A synchronization covers every ticket issued
/// before it starts, because each of those writes was applied before its ticket was taken. At most
/// one writer runs a synchronization at a time; the others wait for it, and one of them runs the
/// next synchronization when the finished one did not cover them. Every branch that checkpoints at
/// the same time therefore shares a synchronization of the node's storage instead of queuing one
/// each behind the storage workers.
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

    /// Claim the one synchronization the barrier runs at a time, or `None` while another writer
    /// runs it.
    fn claim(&self) -> Option<DurabilitySynchronization<'_>> {
        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        Some(DurabilitySynchronization { barrier: self })
    }
}

/// The one synchronization a barrier runs at a time. Dropping it frees the slot and wakes the
/// waiting writers, also when the writer running it is cancelled, so they elect another runner
/// instead of waiting for one that is gone.
struct DurabilitySynchronization<'a> {
    barrier: &'a DurabilityBarrier,
}

impl Drop for DurabilitySynchronization<'_> {
    fn drop(&mut self) {
        self.barrier.running.store(false, Ordering::SeqCst);
        self.barrier.finished.notify_waiters();
    }
}

impl RuntimeStateStore {
    /// Return once every write this store applied before the call is on stable storage.
    ///
    /// Callers waiting at the same time share synchronizations, and a synchronization runs on the
    /// storage workers, never on the async worker that awaits it.
    pub(in crate::runtime) async fn synchronize(
        &self,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let barrier = &self.durability;
        let ticket = barrier.issue();
        loop {
            tokio::task::consume_budget().await;
            let finished = barrier.finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            if barrier.synchronized.load(Ordering::SeqCst) >= ticket {
                return Ok(());
            }
            if barrier.failed.load(Ordering::SeqCst) {
                return Err(Report::new(RuntimePersistenceError::Synchronize));
            }
            let Some(synchronization) = barrier.claim() else {
                finished.await;
                continue;
            };
            let round = self.synchronize_storage().await;
            match round {
                Ok(covered) => {
                    barrier.synchronized.fetch_max(covered, Ordering::SeqCst);
                }
                Err(error) => {
                    if let RuntimePersistenceError::Synchronize = error.current_context() {
                        barrier.failed.store(true, Ordering::SeqCst);
                    }
                    return Err(error);
                }
            }
            drop(synchronization);
        }
    }

    /// Synchronize the database on a storage worker, and return the last ticket that
    /// synchronization covers.
    async fn synchronize_storage(&self) -> error_stack::Result<u64, RuntimePersistenceError> {
        let db = self.db.clone();
        let barrier = self.durability.clone();
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
                    // Every ticket issued so far belongs to a write that was applied before it was
                    // issued, so reading the last one just before the synchronization starts is
                    // what lets the synchronization cover it.
                    let covered = barrier.issued.load(Ordering::SeqCst);
                    barrier.rounds.fetch_add(1, Ordering::SeqCst);
                    db.persist(PersistMode::SyncAll).map_err(|error| {
                        Report::new(RuntimePersistenceError::Synchronize)
                            .attach_printable(error.to_string())
                    })?;
                    Ok(covered)
                },
            )
            .await
            .change_context(RuntimePersistenceError::StorageExecution)?
    }
}

#[cfg(test)]
mod tests {
    use super::{super::tests::open_store, *};

    /// Writers that ask for durability at the same time share synchronizations instead of each
    /// queuing one behind the storage workers. A writer that asks while a synchronization runs is
    /// covered by the next one at the latest.
    #[tokio::test]
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
    #[tokio::test]
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
    #[tokio::test]
    async fn a_cancelled_synchronization_frees_the_barrier() {
        let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
        let store = open_store(&dir);
        let abandoned = store
            .durability
            .claim()
            .expect("nothing else runs a synchronization");
        drop(abandoned);

        tokio::time::timeout(std::time::Duration::from_secs(5), store.synchronize())
            .await
            .expect("a freed barrier must not keep writers waiting")
            .expect("the synchronization should succeed");
    }
}
