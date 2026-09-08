//! One class's bounded pool of workers, and the admission that keeps its queue finite.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc as StdArc,
        atomic::{AtomicUsize, Ordering},
    },
};

use meticulous::OptionExt as _;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::{
    SemaphoreRef,
    cancellation::{CancelOnDrop, Cancellation},
    limits::WorkerClassName,
    memory::Reservation,
};

/// Why a job did not produce a value.
#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("the {class} pool already holds {pending} jobs waiting for a worker")]
    QueueFull { class: &'static str, pending: usize },
    #[error("the {class} pool was shut down")]
    PoolClosed { class: &'static str },
    #[error("the {class} job panicked")]
    JobPanicked { class: &'static str },
}

/// What one worker class is currently doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerClassSnapshot {
    pub workers: usize,
    pub running: usize,
    pub pending: usize,
}

/// A fixed number of workers, with a bounded number of jobs allowed to wait for one. Each class
/// admits independently, so work saturating one class leaves the others' capacity intact.
#[derive(Debug)]
pub(crate) struct WorkerPool {
    class: WorkerClassName,
    workers: usize,
    worker_permits: SemaphoreRef,
    queue_permits: SemaphoreRef,
    pending: StdArc<AtomicUsize>,
}

impl WorkerPool {
    pub(crate) fn new(
        class: WorkerClassName,
        workers: NonZeroUsize,
        pending_jobs: NonZeroUsize,
    ) -> Self {
        Self {
            class,
            workers: workers.get(),
            worker_permits: StdArc::new(Semaphore::new(workers.get())),
            queue_permits: StdArc::new(Semaphore::new(pending_jobs.get())),
            pending: StdArc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn snapshot(&self) -> WorkerClassSnapshot {
        let available = self.worker_permits.available_permits();
        WorkerClassSnapshot {
            workers: self.workers,
            running: self
                .workers
                .checked_sub(available)
                .verified("worker permits are only taken and returned by this pool's own jobs"),
            pending: self.pending.load(Ordering::Acquire),
        }
    }

    /// Admit one job, then run it off the async workers.
    ///
    /// The order is deliberate. The queue slot is taken first, so a class can never accumulate an
    /// unbounded number of futures waiting in front of the blocking pool. `reservation` then moves
    /// into the job itself: a submission dropped while it is still waiting releases the charge with
    /// it, while a submission already running keeps the charge until the work actually exits.
    pub(crate) async fn run<T>(
        &self,
        reservation: Reservation,
        job: impl FnOnce(&Cancellation) -> T + Send + 'static,
    ) -> Result<T, ExecutionError>
    where
        T: Send + 'static,
    {
        let queued = self.enter_queue()?;
        let worker = StdArc::clone(&self.worker_permits)
            .acquire_owned()
            .await
            .map_err(|_| ExecutionError::PoolClosed {
                class: self.class.as_str(),
            })?;
        drop(queued);
        let cancellation = Cancellation::new();
        let signal = CancelOnDrop::new(cancellation.clone());
        let handle = tokio::task::spawn_blocking(move || {
            let value = job(&cancellation);
            // The allocation this job made is live until here, so its charge is released here and
            // not when the caller stopped waiting.
            drop(reservation);
            drop(worker);
            value
        });
        let value = handle.await.map_err(|_| ExecutionError::JobPanicked {
            class: self.class.as_str(),
        })?;
        signal.disarm();
        Ok(value)
    }

    fn enter_queue(&self) -> Result<QueueSlot, ExecutionError> {
        match StdArc::clone(&self.queue_permits).try_acquire_owned() {
            Ok(permit) => {
                self.pending.fetch_add(1, Ordering::AcqRel);
                Ok(QueueSlot {
                    pending: StdArc::clone(&self.pending),
                    permit: Some(permit),
                })
            }
            Err(TryAcquireError::NoPermits) => Err(ExecutionError::QueueFull {
                class: self.class.as_str(),
                pending: self.pending.load(Ordering::Acquire),
            }),
            Err(TryAcquireError::Closed) => Err(ExecutionError::PoolClosed {
                class: self.class.as_str(),
            }),
        }
    }
}

/// One job's place in a class's finite wait queue, given up as soon as it holds a worker.
struct QueueSlot {
    pending: StdArc<AtomicUsize>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for QueueSlot {
    fn drop(&mut self) {
        if self.permit.take().is_some() {
            self.pending.fetch_sub(1, Ordering::AcqRel);
        }
    }
}
