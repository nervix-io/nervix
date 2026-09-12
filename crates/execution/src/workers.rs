//! One class's bounded pool of workers, and the admission that keeps its queue finite.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc as StdArc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
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

/// What one worker class is currently doing, and how much it has done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerClassSnapshot {
    pub workers: usize,
    pub running: usize,
    pub pending: usize,
    /// Jobs this class has admitted since the node started. A caller that must prove it submitted
    /// one job rather than several reads the difference across its own operation.
    pub admitted: u64,
    /// Jobs refused because the class already held its whole wait queue.
    pub refused: u64,
    /// Jobs that have left a worker, whether they produced a value or the caller stopped waiting.
    pub completed: u64,
    /// Time admitted jobs spent waiting for a worker of this class. Divided by `admitted` it is
    /// the queueing an operation of this class currently pays before it starts.
    pub queued: Duration,
    /// Time completed jobs spent holding a worker of this class.
    pub worked: Duration,
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
    admitted: AtomicU64,
    refused: AtomicU64,
    /// Also held by every running job, which records its own service time as it exits.
    completed: StdArc<AtomicU64>,
    queued_nanos: AtomicU64,
    /// Also held by every running job, alongside `completed`.
    worked_nanos: StdArc<AtomicU64>,
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
            admitted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            completed: StdArc::new(AtomicU64::new(0)),
            queued_nanos: AtomicU64::new(0),
            worked_nanos: StdArc::new(AtomicU64::new(0)),
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
            admitted: self.admitted.load(Ordering::Acquire),
            refused: self.refused.load(Ordering::Acquire),
            completed: self.completed.load(Ordering::Acquire),
            queued: Duration::from_nanos(self.queued_nanos.load(Ordering::Acquire)),
            worked: Duration::from_nanos(self.worked_nanos.load(Ordering::Acquire)),
        }
    }

    /// Admit one job, then run it off the async workers.
    ///
    /// The order is deliberate. The queue slot is taken first, so a class can never accumulate an
    /// unbounded number of futures waiting in front of the blocking pool. `reservation` then moves
    /// into the job itself, which allocates under it: a submission dropped while it is still
    /// waiting releases the charge with it, while a submission already running keeps the charge
    /// until the work actually exits.
    pub(crate) async fn run<T>(
        &self,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        let queued = self.enter_queue()?;
        self.admitted.fetch_add(1, Ordering::AcqRel);
        let requested_at = Instant::now();
        let worker = StdArc::clone(&self.worker_permits)
            .acquire_owned()
            .await
            .map_err(|_| {
                Report::new(ExecutionError::PoolClosed {
                    class: self.class.as_str(),
                })
            })?;
        self.queued_nanos
            .fetch_add(elapsed_nanos(requested_at), Ordering::AcqRel);
        drop(queued);
        let cancellation = Cancellation::new();
        let signal = CancelOnDrop::new(cancellation.clone());
        let completed = StdArc::clone(&self.completed);
        let worked_nanos = StdArc::clone(&self.worked_nanos);
        let handle = tokio::task::spawn_blocking(move || {
            // The job owns its charge while it runs, so the allocation it made is released when
            // the work actually exits and not when the caller stopped waiting.
            let started_at = Instant::now();
            let value = job(reservation, &cancellation);
            worked_nanos.fetch_add(elapsed_nanos(started_at), Ordering::AcqRel);
            completed.fetch_add(1, Ordering::AcqRel);
            drop(worker);
            value
        });
        let value = handle.await.map_err(|_| {
            Report::new(ExecutionError::JobPanicked {
                class: self.class.as_str(),
            })
        })?;
        signal.disarm();
        Ok(value)
    }

    fn enter_queue(&self) -> Result<QueueSlot, Report<ExecutionError>> {
        match StdArc::clone(&self.queue_permits).try_acquire_owned() {
            Ok(permit) => {
                self.pending.fetch_add(1, Ordering::AcqRel);
                Ok(QueueSlot {
                    pending: StdArc::clone(&self.pending),
                    permit: Some(permit),
                })
            }
            Err(TryAcquireError::NoPermits) => {
                self.refused.fetch_add(1, Ordering::AcqRel);
                Err(Report::new(ExecutionError::QueueFull {
                    class: self.class.as_str(),
                    pending: self.pending.load(Ordering::Acquire),
                }))
            }
            Err(TryAcquireError::Closed) => Err(Report::new(ExecutionError::PoolClosed {
                class: self.class.as_str(),
            })),
        }
    }
}

/// Nanoseconds since `started_at`, for the cumulative service counters this pool exposes.
fn elapsed_nanos(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).assured(
        "a node would have to run for 584 years for one wait or one job to overflow nanosecond \
         counting",
    )
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
