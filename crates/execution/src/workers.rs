//! One class's bounded pool of workers, and the admission that keeps its queue finite.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc as StdArc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use thiserror::Error;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError},
    time::Instant,
};

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

    /// Admit one job, then dispatch it through this class's execution strategy.
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
        let RunningJob {
            handle,
            cancellation,
        } = self.start(reservation, job).await?;
        let signal = CancelOnDrop::new(cancellation);
        let value = handle.await.map_err(|_| {
            Report::new(ExecutionError::JobPanicked {
                class: self.class.as_str(),
            })
        })?;
        signal.disarm();
        Ok(value)
    }

    /// Admit and submit one job, leaving it to run after the async submitter returns.
    pub(crate) async fn submit(
        &self,
        reservation: Reservation,
        job: impl FnOnce(Reservation) + Send + 'static,
    ) -> Result<(), Report<ExecutionError>> {
        let running = self
            .start(reservation, move |reservation, _| job(reservation))
            .await?;
        drop(running);
        Ok(())
    }

    /// Take this class's ordered worker and submit the job while retaining its charge.
    async fn start<T>(
        &self,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<RunningJob<T>, Report<ExecutionError>>
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
        let job_cancellation = cancellation.clone();
        let completed = StdArc::clone(&self.completed);
        let worked_nanos = StdArc::clone(&self.worked_nanos);
        let work = move || {
            // The job owns its charge while it runs, so the allocation it made is released when
            // the work actually exits and not when the caller stopped waiting.
            let started_at = Instant::now();
            let value = job(reservation, &job_cancellation);
            worked_nanos.fetch_add(elapsed_nanos(started_at), Ordering::AcqRel);
            completed.fetch_add(1, Ordering::AcqRel);
            drop(worker);
            value
        };
        #[cfg(feature = "turmoil")]
        let handle = match self.class {
            // Bounded CPU work in the simulation target is one scheduler task. Its synchronous
            // body is one scheduling step; instruction-level races need Shuttle or real threads.
            WorkerClassName::Cpu(_) => tokio::task::spawn(async move { work() }),
            WorkerClassName::Storage(_) => tokio::task::spawn_blocking(work),
        };
        #[cfg(not(feature = "turmoil"))]
        let handle = tokio::task::spawn_blocking(work);
        Ok(RunningJob {
            handle,
            cancellation,
        })
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

/// A job already submitted to its worker, with the signal an awaiting caller may cancel.
struct RunningJob<T> {
    handle: tokio::task::JoinHandle<T>,
    cancellation: Cancellation,
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

#[cfg(all(test, feature = "turmoil"))]
mod simulation_checks {
    use std::{
        future::{Future as _, poll_fn},
        num::NonZeroUsize,
        sync::Arc as StdArc,
        task::Poll,
    };

    use super::*;
    use crate::{CpuClass, Executor, MemoryClass};

    fn one() -> NonZeroUsize {
        NonZeroUsize::MIN
    }

    #[tokio::test]
    async fn saturation_and_dropped_waiters_restore_the_queue_and_charge() {
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Data), one(), one());
        let executor = Executor::default();
        let occupied = StdArc::clone(&pool.worker_permits)
            .acquire_owned()
            .await
            .assured("the new pool has its one worker permit");

        let queued_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the untouched relay budget has room");
        let mut queued = Box::pin(pool.start(queued_charge, |_, _| ()));
        let state = poll_fn(|context| Poll::Ready(queued.as_mut().poll(context))).await;
        assert!(matches!(state, Poll::Pending));
        assert_eq!(pool.snapshot().pending, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 1024);

        let refused_charge = executor
            .try_reserve(MemoryClass::Relay, 2048)
            .assured("worker queue pressure does not consume memory admission");
        let refused = pool.start(refused_charge, |_, _| ()).await;
        assert!(matches!(
            refused,
            Err(error) if matches!(error.current_context(), ExecutionError::QueueFull { pending: 1, .. })
        ));
        assert_eq!(pool.snapshot().refused, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 1024);

        drop(queued);
        assert_eq!(pool.snapshot().pending, 0);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
        drop(occupied);

        let next_charge = executor
            .try_reserve(MemoryClass::Relay, 4096)
            .assured("the dropped waiter returned its charge");
        let next = pool
            .start(next_charge, |_, _| ())
            .await
            .assured("the dropped waiter returned its queue place");
        next.handle.await.assured("the replacement job completes");
        assert_eq!(pool.snapshot().running, 0);
        assert_eq!(pool.snapshot().pending, 0);
        assert_eq!(pool.snapshot().completed, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    }

    #[tokio::test]
    async fn queued_jobs_take_the_worker_in_admission_order() {
        let two = NonZeroUsize::new(2).assured("2 is nonzero");
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Data), one(), two);
        let executor = Executor::default();
        let occupied = StdArc::clone(&pool.worker_permits)
            .acquire_owned()
            .await
            .assured("the new pool has its one worker permit");
        let (completed, mut observed) = tokio::sync::mpsc::unbounded_channel();

        let first_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the untouched relay budget admits the first job");
        let first_completed = completed.clone();
        let mut first = Box::pin(pool.start(first_charge, move |_, _| {
            first_completed
                .send(1)
                .assured("the test holds the completion receiver");
        }));
        let first_state = poll_fn(|context| Poll::Ready(first.as_mut().poll(context))).await;
        assert!(matches!(first_state, Poll::Pending));

        let second_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the relay budget admits the second job");
        let mut second = Box::pin(pool.start(second_charge, move |_, _| {
            completed
                .send(2)
                .assured("the test holds the completion receiver");
        }));
        let second_state = poll_fn(|context| Poll::Ready(second.as_mut().poll(context))).await;
        assert!(matches!(second_state, Poll::Pending));
        assert_eq!(pool.snapshot().pending, 2);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 2048);

        drop(occupied);
        let first_job = first
            .await
            .assured("the first waiter holds the next permit");
        assert_eq!(pool.snapshot().pending, 1);
        first_job.handle.await.assured("the first job completes");
        let second_job = second
            .await
            .assured("the second waiter holds the returned permit");
        second_job.handle.await.assured("the second job completes");
        assert_eq!(observed.recv().await, Some(1));
        assert_eq!(observed.recv().await, Some(2));
        assert_eq!(pool.snapshot().running, 0);
        assert_eq!(pool.snapshot().pending, 0);
        assert_eq!(pool.snapshot().completed, 2);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    }

    #[tokio::test]
    async fn cancelled_running_job_keeps_its_charge_until_exit() {
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Control), one(), one());
        let executor = Executor::default();
        let charge = executor
            .try_reserve(MemoryClass::Management, 8192)
            .assured("the untouched management budget has room");
        let observed = executor.clone();
        let running = pool
            .start(charge, move |_, cancellation| {
                (
                    cancellation.check().is_err(),
                    observed.snapshot().management_memory.reserved_bytes,
                )
            })
            .await
            .assured("the running job took its worker permit");
        assert_eq!(pool.snapshot().running, 1);
        assert_eq!(executor.snapshot().management_memory.reserved_bytes, 8192);

        let signal = CancelOnDrop::new(running.cancellation.clone());
        drop(signal);
        let (cancelled, charged_at_exit) = running
            .handle
            .await
            .assured("the cancelled job exits cooperatively");
        assert!(cancelled);
        assert_eq!(charged_at_exit, 8192);
        assert_eq!(pool.snapshot().running, 0);
        assert_eq!(pool.snapshot().completed, 1);
        assert_eq!(executor.snapshot().management_memory.reserved_bytes, 0);
    }
}
