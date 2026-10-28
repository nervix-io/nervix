//! One class's bounded FIFO, worker drain, and cancellation at the queued-to-running boundary.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** A class-local admission fence, queued work and workers that drain it in order.
//! - **Depends on.** Execution charges and cancellation, and the primitive boundary's mechanisms.
//! - **Must not know.** What a job computes or which graph, domain or peer submitted it.

use std::{collections::BTreeMap, fmt, num::NonZeroUsize, panic::AssertUnwindSafe, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    sync::{
        Arc, OwnedSemaphorePermit, Semaphore, StdArc, TryAcquireError,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        blocking::Mutex,
        oneshot,
    },
    time::Instant,
};
use nervix_recovery::NoReceiver as _;
use thiserror::Error;

use crate::{
    cancellation::{ArmedCancellation, CancelOnDrop, Cancellation},
    executor::SemaphoreRef,
    limits::WorkerClassName,
    memory::Reservation,
};

/// What a class whose wait queue is full does with the next job it is handed.
///
/// The wait queue bounds how much work a class may hold in front of its workers. Work that answers
/// a request is refused when it is full, because its sender can present the work again and the
/// caller must answer rather than wait behind a queue it cannot bound. Work the node already
/// accepted and keeps, such as a payload a quiesce buffer retained, cannot be presented again, so
/// it waits for a place instead. Either way the job then waits for a worker like every admitted
/// job, and its charge is released when it exits or when its caller stops waiting for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueAdmission {
    /// Refuse the job at once with [`ExecutionError::QueueFull`].
    RefuseWhenFull,
    /// Hold the job until a place frees. Places go to waiting jobs in the order they asked, ahead
    /// of any job asking afterwards, so a job refused when full never overtakes one that waits.
    /// The caller bounds the wait by dropping the future, which gives up its place in line and
    /// returns its charge; nothing else ends it while the class's jobs keep their workers.
    WaitForPlace,
}

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
    /// How many jobs the class may hold waiting for a worker. A job submitted while `pending` is
    /// at this capacity is refused, unless its caller waits for a place.
    pub queue_capacity: usize,
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

/// A fixed number of workers sharing one bounded FIFO. Cloning a worker or cancellation guard
/// retains the complete class with one reference count.
#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "one class retains a finite FIFO and cancels its exact queued entries",
        key = "one executor worker class",
        bound = "configured worker and queue counts; one insert, removal or selection per fence; \
                 no callback or await under the fence"
    )
)]
pub(crate) struct WorkerPool {
    inner: Arc<PoolShared>,
}

#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "a class serializes submission with its workers' empty-to-idle transition",
        key = "one executor worker class",
        bound = "configured worker and queue counts; one synchronous queue transition without \
                 callbacks"
    )
)]
struct PoolShared {
    class: WorkerClassName,
    workers: usize,
    queue_capacity: usize,
    queue_permits: SemaphoreRef,
    // This bounded admission fence only inserts, removes or selects one job. No callback, result
    // destruction, semaphore release or notification runs while it is held. It also makes choosing
    // the next job and relinquishing the last worker one transition with respect to submission.
    queue: Mutex<WorkQueue>,
    pending: AtomicUsize,
    admitted: AtomicU64,
    refused: AtomicU64,
    completed: AtomicU64,
    queued_nanos: AtomicU64,
    worked_nanos: AtomicU64,
    #[cfg(test)]
    dispatches: AtomicU64,
}

#[derive(Debug, Default)]
struct WorkQueue {
    next_id: u64,
    running: usize,
    jobs: BTreeMap<u64, QueuedJob>,
}

impl WorkerPool {
    pub(crate) fn new(
        class: WorkerClassName,
        workers: NonZeroUsize,
        pending_jobs: NonZeroUsize,
    ) -> Self {
        Self {
            inner: Arc::new(PoolShared {
                class,
                workers: workers.get(),
                queue_capacity: pending_jobs.get(),
                queue_permits: StdArc::new(Semaphore::new(pending_jobs.get())),
                queue: Mutex::new(WorkQueue::default()),
                pending: AtomicUsize::new(0),
                admitted: AtomicU64::new(0),
                refused: AtomicU64::new(0),
                completed: AtomicU64::new(0),
                queued_nanos: AtomicU64::new(0),
                worked_nanos: AtomicU64::new(0),
                #[cfg(test)]
                dispatches: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn snapshot(&self) -> WorkerClassSnapshot {
        let running = self.inner.queue.lock().running;
        WorkerClassSnapshot {
            workers: self.inner.workers,
            running,
            pending: self.inner.pending.load(Ordering::Acquire),
            queue_capacity: self.inner.queue_capacity,
            admitted: self.inner.admitted.load(Ordering::Acquire),
            refused: self.inner.refused.load(Ordering::Acquire),
            completed: self.inner.completed.load(Ordering::Acquire),
            queued: Duration::from_nanos(self.inner.queued_nanos.load(Ordering::Acquire)),
            worked: Duration::from_nanos(self.inner.worked_nanos.load(Ordering::Acquire)),
        }
    }

    pub(crate) async fn run<T>(
        &self,
        admission: QueueAdmission,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        let RunningJob { handle, obligation } = self.start(admission, reservation, job).await?;
        let outcome = handle.await.map_err(|cause| {
            Report::new(cause).change_context(ExecutionError::PoolClosed {
                class: self.inner.class.as_str(),
            })
        })?;
        obligation.disarm();
        outcome
    }

    /// Return once the job has been handed to a worker. Until then the submitting future still
    /// owns queued cancellation; afterwards the job runs independently of its submitter.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "adapts the caller's synchronous callback for its admitted \
                                   worker; callback effects belong to the caller and execute \
                                   outside the FIFO fence")
    )]
    pub(crate) async fn submit(
        &self,
        reservation: Reservation,
        job: impl FnOnce(Reservation) + Send + 'static,
    ) -> Result<(), Report<ExecutionError>> {
        let RunningJob { handle, obligation } = self
            .start(
                QueueAdmission::RefuseWhenFull,
                reservation,
                move |reservation, _| job(reservation),
            )
            .await?;
        obligation.disarm();
        drop(handle);
        Ok(())
    }

    async fn start<T>(
        &self,
        admission: QueueAdmission,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<RunningJob<T>, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        let slot = self.enter_queue(admission).await?;
        let ArmedCancellation { obligation, signal } = Cancellation::armed();
        let (result, handle) = oneshot::channel();
        let (started, has_started) = oneshot::channel();
        let work = QueuedJob {
            requested_at: Instant::now(),
            slot,
            started,
            work: Box::new(Job {
                job,
                reservation,
                cancellation: signal,
                result,
            }),
        };
        let id;
        let selected;
        {
            let mut queue = self.inner.queue.lock();
            id = queue.next_id;
            queue.next_id = id
                .checked_add(1)
                .assured("a class cannot submit 2^64 jobs during one node lifetime");
            assert!(
                queue.jobs.insert(id, work).is_none(),
                "monotonic job identities are unique"
            );
            self.inner.admitted.fetch_add(1, Ordering::AcqRel);
            if queue.running < self.inner.workers {
                queue.running = queue
                    .running
                    .checked_add(1)
                    .verified("the configured worker count bounds the active count");
                selected = queue.jobs.pop_first();
            } else {
                selected = None;
            }
        }
        let obligation = QueueCancellation {
            pool: Arc::clone(&self.inner),
            id,
            cancellation: Some(obligation),
        };
        if let Some((_, first)) = selected {
            let worker = Worker {
                pool: Arc::clone(&self.inner),
                state: WorkerState::Selected(first.start()),
            };
            self.inner.dispatch(worker);
        }
        has_started.await.map_err(|cause| {
            Report::new(cause).change_context(ExecutionError::PoolClosed {
                class: self.inner.class.as_str(),
            })
        })?;
        Ok(RunningJob { handle, obligation })
    }

    async fn enter_queue(
        &self,
        admission: QueueAdmission,
    ) -> Result<QueueSlot, Report<ExecutionError>> {
        let permit = match admission {
            QueueAdmission::RefuseWhenFull => {
                match StdArc::clone(&self.inner.queue_permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(TryAcquireError::NoPermits) => {
                        self.inner.refused.fetch_add(1, Ordering::AcqRel);
                        return Err(Report::new(ExecutionError::QueueFull {
                            class: self.inner.class.as_str(),
                            pending: self.inner.pending.load(Ordering::Acquire),
                        }));
                    }
                    Err(TryAcquireError::Closed) => {
                        return Err(Report::new(ExecutionError::PoolClosed {
                            class: self.inner.class.as_str(),
                        }));
                    }
                }
            }
            QueueAdmission::WaitForPlace => StdArc::clone(&self.inner.queue_permits)
                .acquire_owned()
                .await
                .map_err(|_| {
                    Report::new(ExecutionError::PoolClosed {
                        class: self.inner.class.as_str(),
                    })
                })?,
        };
        self.inner.pending.fetch_add(1, Ordering::AcqRel);
        Ok(QueueSlot {
            pool: Arc::clone(&self.inner),
            _permit: permit,
        })
    }
}

impl PoolShared {
    fn dispatch(&self, mut worker: Worker) {
        #[cfg(test)]
        self.dispatches.fetch_add(1, Ordering::Relaxed);
        let handle = match self.class {
            WorkerClassName::Cpu(_) => {
                nervix_primitives::task::spawn_cpu(move || worker.run_next())
            }
            WorkerClassName::Storage(_) => {
                nervix_primitives::task::spawn_blocking(move || while worker.run_next() {})
            }
        };
        // Each callback catches its own panic and completes its own result; the worker is detached.
        drop(handle);
    }
}

struct QueuedJob {
    requested_at: Instant,
    slot: QueueSlot,
    started: oneshot::Sender<()>,
    work: Box<dyn Work>,
}

impl fmt::Debug for QueuedJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueuedJob").finish_non_exhaustive()
    }
}

impl QueuedJob {
    fn start(self) -> Box<dyn Work> {
        self.slot
            .pool
            .queued_nanos
            .fetch_add(elapsed_nanos(self.requested_at), Ordering::AcqRel);
        drop(self.slot);
        self.started
            .send(())
            .means_peer_left("the caller awaiting execution admission");
        self.work
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "the class worker invokes exactly its selected synchronous callback",
        key = "one admitted executor job",
        bound = "one callback outside the FIFO fence, followed by one queue transition"
    )
)]
trait Work: Send {
    fn execute(self: Box<Self>, worker: &mut Worker);
}

struct Job<T, F> {
    job: F,
    reservation: Reservation,
    cancellation: Cancellation,
    result: oneshot::Sender<Result<T, Report<ExecutionError>>>,
}

impl<T, F> Work for Job<T, F>
where
    T: Send + 'static,
    F: FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
{
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "invokes one caller-owned synchronous callback outside the \
                                   FIFO fence; the worker owns admission, cancellation and \
                                   completion, not callback effects")
    )]
    fn execute(self: Box<Self>, worker: &mut Worker) {
        let Self {
            job,
            reservation,
            cancellation,
            result,
        } = *self;
        let started_at = Instant::now();
        let outcome =
            match std::panic::catch_unwind(AssertUnwindSafe(|| job(reservation, &cancellation))) {
                Ok(value) => Ok(value),
                Err(_panic) => Err(Report::new(ExecutionError::JobPanicked {
                    class: worker.pool.class.as_str(),
                })),
            };
        worker
            .pool
            .worked_nanos
            .fetch_add(elapsed_nanos(started_at), Ordering::AcqRel);
        worker.pool.completed.fetch_add(1, Ordering::AcqRel);
        // Publish the next selected job or the idle transition before the completed result. An
        // observer of completion therefore sees the worker returned when no backlog remains.
        worker.advance();
        result
            .send(outcome)
            .means_peer_left("the caller awaiting an execution result");
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "each worker takes the next retained job or returns its active place atomically",
        key = "one executor worker class",
        bound = "one FIFO selection per bounded callback; no callback under the queue fence"
    )
)]
struct Worker {
    pool: Arc<PoolShared>,
    state: WorkerState,
}

enum WorkerState {
    Selected(Box<dyn Work>),
    Executing,
    Idle,
}

impl Worker {
    fn run_next(&mut self) -> bool {
        let state = std::mem::replace(&mut self.state, WorkerState::Executing);
        match state {
            WorkerState::Selected(work) => work.execute(self),
            WorkerState::Idle => {
                self.state = WorkerState::Idle;
                return false;
            }
            WorkerState::Executing => unreachable!("a worker runs one callback at a time"),
        }
        matches!(self.state, WorkerState::Selected(_))
    }

    fn advance(&mut self) {
        let next;
        {
            let mut queue = self.pool.queue.lock();
            next = queue.jobs.pop_first();
            if next.is_none() {
                queue.running = queue
                    .running
                    .checked_sub(1)
                    .verified("this worker relinquishes its one active place exactly once");
                self.state = WorkerState::Idle;
            }
        }
        if let Some((_, next)) = next {
            self.state = WorkerState::Selected(next.start());
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let interrupted = std::mem::replace(&mut self.state, WorkerState::Idle);
        if let WorkerState::Idle = interrupted {
            return;
        }
        let abandoned;
        {
            let mut queue = self.pool.queue.lock();
            queue.running = queue
                .running
                .checked_sub(1)
                .verified("an interrupted worker still owns its active place");
            abandoned = std::mem::take(&mut queue.jobs);
        }
        // Runtime teardown can drop a simulated worker between callbacks, or a blocking task
        // before it starts. Release every queued charge and close its start/result channels rather
        // than retaining an unreachable FIFO. Other active callbacks keep their own charges.
        drop(interrupted);
        drop(abandoned);
    }
}

struct RunningJob<T> {
    handle: oneshot::Receiver<Result<T, Report<ExecutionError>>>,
    obligation: QueueCancellation,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "a caller removes only its exact retained queued job when it withdraws",
        key = "one admitted executor job",
        bound = "one keyed removal; destruction and charge release happen after the fence"
    )
)]
struct QueueCancellation {
    pool: Arc<PoolShared>,
    id: u64,
    cancellation: Option<CancelOnDrop>,
}

impl QueueCancellation {
    fn disarm(mut self) {
        self.cancellation
            .take()
            .verified("only an armed submission is disarmed")
            .disarm();
    }
}

impl Drop for QueueCancellation {
    fn drop(&mut self) {
        if self.cancellation.is_some() {
            let cancelled = self.pool.queue.lock().jobs.remove(&self.id);
            // Destroy callbacks and release their charges and slots outside the queue fence.
            drop(cancelled);
        }
    }
}

fn elapsed_nanos(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).assured(
        "a node would have to run for 584 years for one wait or job to overflow nanosecond \
         counting",
    )
}

struct QueueSlot {
    pool: Arc<PoolShared>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for QueueSlot {
    fn drop(&mut self) {
        self.pool.pending.fetch_sub(1, Ordering::AcqRel);
        // The permit is returned after pending decreases, so a woken waiter sees the freed slot.
    }
}

// Owner checks can retain an active worker without parking a thread, including under Turmoil.
// Releasing it uses the same advance/dispatch transition as the last callback of a real worker.
#[cfg(all(test, not(feature = "loom")))]
struct HeldWorker(Worker);

#[cfg(all(test, not(feature = "loom")))]
impl HeldWorker {
    fn new(pool: &WorkerPool) -> Self {
        let mut queue = pool.inner.queue.lock();
        assert_eq!(queue.running, 0);
        assert_eq!(pool.inner.workers, 1);
        queue.running = 1;
        Self(Worker {
            pool: Arc::clone(&pool.inner),
            state: WorkerState::Executing,
        })
    }
}

#[cfg(all(test, not(feature = "loom")))]
impl Drop for HeldWorker {
    fn drop(&mut self) {
        if let WorkerState::Idle = self.0.state {
            return;
        }
        self.0.advance();
        let state = std::mem::replace(&mut self.0.state, WorkerState::Idle);
        if let WorkerState::Selected(current) = state {
            let worker = Worker {
                pool: Arc::clone(&self.0.pool),
                state: WorkerState::Selected(current),
            };
            self.0.pool.dispatch(worker);
        }
    }
}

#[cfg(all(
    test,
    not(any(feature = "loom", feature = "shuttle", feature = "turmoil"))
))]
mod dispatch_checks;

// The owner hold keeps a worker active without blocking a thread, so these admission checks also
// run under Turmoil's single-threaded simulated CPU strategy.
#[cfg(all(test, not(feature = "loom")))]
mod queue_admission_checks {
    use std::{
        future::{Future as _, poll_fn},
        num::NonZeroUsize,
        task::Poll,
    };

    use super::*;
    use crate::{CpuClass, Executor, MemoryClass};

    /// A class whose wait queue is full refuses a job that answers a request, and holds a job the
    /// node keeps until a place frees, then runs it like any other admitted job.
    #[nervix_primitives::test]
    async fn a_full_wait_queue_holds_a_job_that_waits_for_a_place_until_one_frees() {
        let pool = WorkerPool::new(
            WorkerClassName::Cpu(CpuClass::Extension),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
        );
        let executor = Executor::default();
        let occupied = HeldWorker::new(&pool);

        let queued_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the untouched relay budget has room");
        let mut queued =
            Box::pin(pool.start(QueueAdmission::RefuseWhenFull, queued_charge, |_, _| ()));
        let queued_state = poll_fn(|context| Poll::Ready(queued.as_mut().poll(context))).await;
        assert!(
            queued_state.is_pending(),
            "the queued job waits for the held worker"
        );
        assert_eq!(pool.snapshot().pending, 1);

        let refused_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the relay budget has room for the refused job");
        let refused = pool
            .start(QueueAdmission::RefuseWhenFull, refused_charge, |_, _| ())
            .await;
        assert!(
            matches!(
                refused,
                Err(error) if matches!(
                    error.current_context(),
                    ExecutionError::QueueFull { pending: 1, .. }
                )
            ),
            "a full wait queue refuses a job that answers a request"
        );

        let waiting_charge = executor
            .try_reserve(MemoryClass::Relay, 4096)
            .assured("the relay budget has room for the waiting job");
        let mut waiting =
            Box::pin(pool.start(QueueAdmission::WaitForPlace, waiting_charge, |_, _| 7));
        let waiting_state = poll_fn(|context| Poll::Ready(waiting.as_mut().poll(context))).await;
        assert!(
            waiting_state.is_pending(),
            "a full wait queue holds a job that waits for a place"
        );
        let held = pool.snapshot();
        assert_eq!(
            held.pending, 1,
            "a waiting job takes no place until one frees"
        );
        assert_eq!(held.refused, 1, "a waiting job is not refused");
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 1024 + 4096);

        drop(occupied);
        let queued_job = queued
            .await
            .assured("the queued job takes the returned worker permit");
        queued_job
            .handle
            .await
            .assured("the queued job reports its outcome")
            .assured("the queued job runs once it holds the worker");
        queued_job.obligation.disarm();
        let waiting_job = waiting
            .await
            .assured("the waiting job takes the place the queued job gave up");
        let value = waiting_job
            .handle
            .await
            .assured("the waiting job reports its outcome")
            .assured("the waiting job runs once it holds the worker");
        waiting_job.obligation.disarm();
        assert_eq!(value, 7);

        let drained = pool.snapshot();
        assert_eq!(drained.admitted, 2);
        assert_eq!(drained.refused, 1);
        assert_eq!(drained.pending, 0);
        assert_eq!(drained.running, 0);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    }
}

#[cfg(all(test, feature = "turmoil"))]
mod simulation_checks {
    use std::{
        future::{Future as _, poll_fn},
        num::NonZeroUsize,
        task::Poll,
    };

    use super::*;
    use crate::{CpuClass, Executor, MemoryClass};

    fn one() -> NonZeroUsize {
        NonZeroUsize::MIN
    }

    #[nervix_primitives::test]
    async fn saturation_and_dropped_waiters_restore_the_queue_and_charge() {
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Data), one(), one());
        let executor = Executor::default();
        let occupied = HeldWorker::new(&pool);

        let queued_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the untouched relay budget has room");
        let mut queued =
            Box::pin(pool.start(QueueAdmission::RefuseWhenFull, queued_charge, |_, _| ()));
        let state = poll_fn(|context| Poll::Ready(queued.as_mut().poll(context))).await;
        assert!(matches!(state, Poll::Pending));
        assert_eq!(pool.snapshot().pending, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 1024);

        let refused_charge = executor
            .try_reserve(MemoryClass::Relay, 2048)
            .assured("worker queue pressure does not consume memory admission");
        let refused = pool
            .start(QueueAdmission::RefuseWhenFull, refused_charge, |_, _| ())
            .await;
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
            .start(QueueAdmission::RefuseWhenFull, next_charge, |_, _| ())
            .await
            .assured("the dropped waiter returned its queue place");
        next.handle
            .await
            .assured("the replacement job reports")
            .assured("the replacement job completes");
        assert_eq!(pool.snapshot().running, 0);
        assert_eq!(pool.snapshot().pending, 0);
        assert_eq!(pool.snapshot().completed, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    }

    #[nervix_primitives::test]
    async fn queued_jobs_take_the_worker_in_admission_order() {
        let two = NonZeroUsize::new(2).assured("2 is nonzero");
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Data), one(), two);
        let executor = Executor::default();
        let occupied = HeldWorker::new(&pool);
        let (completed, mut observed) = nervix_primitives::sync::mpsc::unbounded_channel();

        let first_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the untouched relay budget admits the first job");
        let first_completed = completed.clone();
        let mut first =
            Box::pin(
                pool.start(QueueAdmission::RefuseWhenFull, first_charge, move |_, _| {
                    first_completed
                        .send(1)
                        .assured("the test holds the completion receiver");
                }),
            );
        let first_state = poll_fn(|context| Poll::Ready(first.as_mut().poll(context))).await;
        assert!(matches!(first_state, Poll::Pending));

        let second_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the relay budget admits the second job");
        let mut second = Box::pin(pool.start(
            QueueAdmission::RefuseWhenFull,
            second_charge,
            move |_, _| {
                completed
                    .send(2)
                    .assured("the test holds the completion receiver");
            },
        ));
        let second_state = poll_fn(|context| Poll::Ready(second.as_mut().poll(context))).await;
        assert!(matches!(second_state, Poll::Pending));
        assert_eq!(pool.snapshot().pending, 2);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 2048);

        drop(occupied);
        let first_job = first
            .await
            .assured("the first waiter holds the next permit");
        assert_eq!(pool.snapshot().pending, 1);
        first_job
            .handle
            .await
            .assured("the first job reports")
            .assured("the first job completes");
        let second_job = second
            .await
            .assured("the second waiter holds the returned permit");
        second_job
            .handle
            .await
            .assured("the second job reports")
            .assured("the second job completes");
        assert_eq!(observed.recv().await, Some(1));
        assert_eq!(observed.recv().await, Some(2));
        assert_eq!(pool.snapshot().running, 0);
        assert_eq!(pool.snapshot().pending, 0);
        assert_eq!(pool.snapshot().completed, 2);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    }

    #[nervix_primitives::test]
    async fn cancelled_running_job_keeps_its_charge_until_exit() {
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Control), one(), one());
        let executor = Executor::default();
        let charge = executor
            .try_reserve(MemoryClass::Management, 8192)
            .assured("the untouched management budget has room");
        let observed = executor.clone();
        let RunningJob { handle, obligation } = pool
            .start(
                QueueAdmission::RefuseWhenFull,
                charge,
                move |_, cancellation| {
                    (
                        cancellation.check().is_err(),
                        observed.snapshot().management_memory.reserved_bytes,
                    )
                },
            )
            .await
            .assured("the running job took its worker permit");
        assert_eq!(pool.snapshot().running, 1);
        assert_eq!(executor.snapshot().management_memory.reserved_bytes, 8192);

        drop(obligation);
        let (cancelled, charged_at_exit) = handle
            .await
            .assured("the cancelled job reports its outcome")
            .assured("the cancelled job exits cooperatively");
        assert!(cancelled);
        assert_eq!(charged_at_exit, 8192);
        assert_eq!(pool.snapshot().running, 0);
        assert_eq!(pool.snapshot().completed, 1);
        assert_eq!(executor.snapshot().management_memory.reserved_bytes, 0);
    }
}
