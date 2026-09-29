//! The submissions one producer holds, from the moment it takes a batch until the application takes
//! the batch's outcome, and the credit each of them holds.
//!
//! - **Owns.** The granted batch and byte credit, submission identities, each submission's pending,
//!   resolved or released state with the credit a resolved one still holds, and waking every
//!   caller that waits for an outcome.
//! - **Depends on.** Tokio's semaphores and notification, and the producer's outcome vocabulary.
//! - **Must not know.** Exchanges, frames, or how an outcome was decided.
//!
//! A submission's credit comes back exactly once: when the application takes its outcome, when it
//! releases a resolved submission, or when the outcome of a submission it released earlier arrives.

use std::{collections::BTreeMap, num::NonZeroU64, sync::Arc as StdArc};

use error_stack::Report;
use meticulous::OptionExt as _;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::{PendingSubmission, ProducerError, ProducerOutcome, SubmissionId};

/// The submissions of one producer and the credit it was granted.
pub(super) struct SubmissionSlots {
    /// The granted batches. A submission holds one permit until its outcome is taken. Tokio's owned
    /// permits take the standard `Arc`.
    batches: StdArc<Semaphore>,
    /// The granted bytes. A submission holds its size until its outcome is taken.
    bytes: StdArc<Semaphore>,
    submissions: SyncMutex<Submissions>,
    /// Woken whenever a submission resolves.
    resolved: Notify,
}

#[derive(Default)]
struct Submissions {
    next: u64,
    slots: BTreeMap<SubmissionId, Slot>,
}

enum Slot {
    /// The batch waits to be sent, or for its outcome.
    Pending,
    /// The outcome, holding the credit the batch took until the application takes it.
    Resolved {
        outcome: ProducerOutcome,
        _credit: Credit,
    },
    /// The application released the submission before it resolved; its outcome and credit are
    /// dropped when it arrives.
    Released,
}

/// The credit one submission holds.
pub(super) struct Credit {
    _batch: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

impl SubmissionSlots {
    /// Slots granted `batches` batches and `bytes` bytes at once.
    pub(super) fn new(batches: usize, bytes: usize) -> Self {
        Self {
            batches: StdArc::new(Semaphore::new(batches)),
            bytes: StdArc::new(Semaphore::new(bytes)),
            submissions: SyncMutex::new(Submissions::default()),
            resolved: Notify::new(),
        }
    }

    /// Waits for one batch and `bytes` bytes of credit, or `None` once the slots are closed.
    pub(super) async fn credit(&self, bytes: u32) -> Option<Credit> {
        let batch = self.batches.clone().acquire_owned().await.ok()?;
        let bytes = self.bytes.clone().acquire_many_owned(bytes).await.ok()?;
        Some(Credit {
            _batch: batch,
            _bytes: bytes,
        })
    }

    /// Holds a new submission, pending until it resolves.
    pub(super) fn hold(&self) -> SubmissionId {
        let mut submissions = self.submissions.lock();
        submissions.next = submissions
            .next
            .checked_add(1)
            .assured("a producer submits fewer than u64::MAX batches");
        let id = SubmissionId(
            NonZeroU64::new(submissions.next).verified("the counter was advanced above"),
        );
        submissions.slots.insert(id, Slot::Pending);
        id
    }

    /// Records a submission's outcome with the credit it holds until the application takes it.
    pub(super) fn resolve(&self, id: SubmissionId, outcome: ProducerOutcome, credit: Credit) {
        let mut submissions = self.submissions.lock();
        let Some(slot) = submissions.slots.get_mut(&id) else {
            return;
        };
        match slot {
            Slot::Pending => {
                *slot = Slot::Resolved {
                    outcome,
                    _credit: credit,
                };
            }
            // Dropping the outcome and its credit is what releasing it before it resolved meant.
            Slot::Released => {
                submissions.slots.remove(&id);
            }
            Slot::Resolved { .. } => {}
        }
        drop(submissions);
        self.resolved.notify_waiters();
    }

    /// Takes a resolved submission's outcome, which returns its credit.
    fn take_resolved(
        &self,
        id: SubmissionId,
    ) -> error_stack::Result<Option<ProducerOutcome>, ProducerError> {
        let mut submissions = self.submissions.lock();
        match submissions.slots.get(&id) {
            None | Some(Slot::Released) => Err(Report::new(ProducerError::UnknownSubmission(id))),
            Some(Slot::Pending) => Ok(None),
            Some(Slot::Resolved { .. }) => {
                let Some(Slot::Resolved { outcome, .. }) = submissions.slots.remove(&id) else {
                    return Err(Report::new(ProducerError::UnknownSubmission(id)));
                };
                Ok(Some(outcome))
            }
        }
    }

    /// Waits for a submission's outcome and takes it, which returns its credit. The wait registers
    /// for the next resolution before it looks, so an outcome that arrives in between wakes it.
    pub(super) async fn rejoin(
        &self,
        id: SubmissionId,
    ) -> error_stack::Result<ProducerOutcome, ProducerError> {
        loop {
            tokio::task::consume_budget().await;
            let resolved = self.resolved.notified();
            let mut resolved = std::pin::pin!(resolved);
            resolved.as_mut().enable();
            if let Some(outcome) = self.take_resolved(id)? {
                return Ok(outcome);
            }
            resolved.await;
        }
    }

    /// Every submission held, in submission order, with the outcome of each that has one.
    pub(super) fn pending(&self) -> Vec<PendingSubmission> {
        let submissions = self.submissions.lock();
        let mut pending = Vec::with_capacity(submissions.slots.len());
        for (id, slot) in &submissions.slots {
            let outcome = match slot {
                Slot::Pending => None,
                Slot::Resolved { outcome, .. } => Some(outcome.clone()),
                Slot::Released => continue,
            };
            pending.push(PendingSubmission { id: *id, outcome });
        }
        pending
    }

    /// Lets go of a submission. A resolved one returns its outcome and its credit now; an
    /// unresolved one returns its credit once its outcome arrives, and nobody observes that outcome.
    pub(super) fn release(
        &self,
        id: SubmissionId,
    ) -> error_stack::Result<Option<ProducerOutcome>, ProducerError> {
        let mut submissions = self.submissions.lock();
        match submissions.slots.get(&id) {
            None | Some(Slot::Released) => Err(Report::new(ProducerError::UnknownSubmission(id))),
            Some(Slot::Pending) => {
                submissions.slots.insert(id, Slot::Released);
                Ok(None)
            }
            Some(Slot::Resolved { .. }) => {
                let Some(Slot::Resolved { outcome, .. }) = submissions.slots.remove(&id) else {
                    return Err(Report::new(ProducerError::UnknownSubmission(id)));
                };
                Ok(Some(outcome))
            }
        }
    }

    /// Stops granting credit: every wait for it ends, and so does every later one.
    pub(super) fn close(&self) {
        self.batches.close();
        self.bytes.close();
    }

    /// The granted batches no submission holds right now.
    #[cfg(all(test, feature = "shuttle"))]
    pub(super) fn available_batches(&self) -> usize {
        self.batches.available_permits()
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "slots_shuttle_tests.rs"]
mod shuttle_tests;
