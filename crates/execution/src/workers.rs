//! One class's workers, the jobs waiting for them in the order they asked, and cancellation at the
//! waiting-to-running boundary.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** A class's fixed workers, like the permits of a semaphore; the jobs waiting for one;
//!   workers that take the next waiting job themselves, in the order the jobs asked; and the state a
//!   job carries for its caller, handed back when the job never runs.
//! - **Depends on.** Execution charges and cancellation, and the primitive boundary's mechanisms.
//! - **Must not know.** What a job computes or which graph, domain or peer submitted it.

use std::{
    any::Any, collections::BTreeMap, fmt, num::NonZeroUsize, panic::AssertUnwindSafe,
    time::Duration,
};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        blocking::Mutex,
        oneshot,
    },
    time::Instant,
};
use nervix_recovery::NoReceiver as _;
use thiserror::Error;

use crate::{
    CapacityWaits,
    cancellation::{ArmedCancellation, CancelOnDrop, Cancellation},
    limits::WorkerClassName,
    memory::Reservation,
};

/// Why a job did not produce a value.
///
/// Busy workers are not among the reasons. A class is a fixed number of workers, like the permits
/// of a semaphore: a job that finds every worker busy waits for one, workers take waiting jobs in
/// the order they asked, and nothing bounds how many jobs wait. The caller bounds its job's wait
/// by dropping the future, which withdraws the job and returns its charge.
#[derive(Debug, Error)]
pub enum ExecutionError {
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
    /// Jobs waiting for a worker: each was handed to the class while every worker was busy, and no
    /// worker has taken it yet. A job stops counting here once a worker takes it or its caller stops
    /// waiting.
    pub waiting: usize,
    /// Jobs this class has admitted since the node started. A caller that must prove it submitted
    /// one job rather than several reads the difference across its own operation.
    pub admitted: u64,
    /// Jobs that have left a worker, whether they produced a value or the caller stopped waiting.
    pub completed: u64,
    /// Time admitted jobs spent waiting for a worker of this class. Divided by `admitted` it is
    /// the wait an operation of this class currently pays before it starts.
    pub waited: Duration,
    /// Time completed jobs spent holding a worker of this class.
    pub worked: Duration,
}

/// A fixed number of workers and the jobs waiting for them. Cloning a worker or cancellation guard
/// retains the complete class with one reference count.
#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "one class retains the jobs waiting for its workers and cancels its exact \
                  waiting entries",
        key = "one executor worker class",
        bound = "configured worker count; one insert, removal or selection per fence; no callback \
                 or await under the fence"
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
        bound = "configured worker count; one synchronous transition of the waiting jobs without \
                 callbacks"
    )
)]
struct PoolShared {
    class: WorkerClassName,
    workers: usize,
    // This fence only inserts, removes or selects one job. No callback, result destruction or
    // notification runs while it is held. It also makes choosing the next job and relinquishing the
    // last worker one transition with respect to submission.
    state: Mutex<Workers>,
    admitted: AtomicU64,
    completed: AtomicU64,
    waited_nanos: AtomicU64,
    worked_nanos: AtomicU64,
    #[cfg(test)]
    dispatches: AtomicU64,
}

/// How many of a class's workers are taken, and the jobs waiting for one in the order they asked.
#[derive(Debug, Default)]
struct Workers {
    next_id: u64,
    running: usize,
    waiting: BTreeMap<u64, WaitingJob>,
}

impl WorkerPool {
    pub(crate) fn new(class: WorkerClassName, workers: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(PoolShared {
                class,
                workers: workers.get(),
                state: Mutex::new(Workers::default()),
                admitted: AtomicU64::new(0),
                completed: AtomicU64::new(0),
                waited_nanos: AtomicU64::new(0),
                worked_nanos: AtomicU64::new(0),
                #[cfg(test)]
                dispatches: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn snapshot(&self) -> WorkerClassSnapshot {
        let (running, waiting) = {
            let state = self.inner.state.lock();
            (state.running, state.waiting.len())
        };
        WorkerClassSnapshot {
            workers: self.inner.workers,
            running,
            waiting,
            admitted: self.inner.admitted.load(Ordering::Acquire),
            completed: self.inner.completed.load(Ordering::Acquire),
            waited: Duration::from_nanos(self.inner.waited_nanos.load(Ordering::Acquire)),
            worked: Duration::from_nanos(self.inner.worked_nanos.load(Ordering::Acquire)),
        }
    }

    pub(crate) async fn run<T>(
        &self,
        waits: Option<&CapacityWaits>,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        let RunningJob { handle, obligation } = self.start(waits, reservation, job).await?;
        self.finish(handle.await, obligation)
    }

    fn finish<T>(
        &self,
        result: Result<Result<T, Report<ExecutionError>>, oneshot::error::RecvError>,
        obligation: WaitingCancellation,
    ) -> Result<T, Report<ExecutionError>> {
        let outcome = result.map_err(|cause| self.closed(cause))?;
        obligation.disarm();
        outcome
    }

    fn closed(&self, cause: oneshot::error::RecvError) -> Report<ExecutionError> {
        Report::new(cause).change_context(ExecutionError::PoolClosed {
            class: self.inner.class.as_str(),
        })
    }

    /// Return once the job has been handed to a worker, waiting for one like every job when every
    /// worker is busy. Until then the submitting future still owns the waiting job's cancellation;
    /// afterwards the job runs independently of its submitter.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "adapts the caller's synchronous callback for its admitted \
                                   worker; callback effects belong to the caller and execute \
                                   outside the waiting jobs' fence")
    )]
    pub(crate) async fn submit(
        &self,
        waits: Option<&CapacityWaits>,
        reservation: Reservation,
        job: impl FnOnce(Reservation) + Send + 'static,
    ) -> Result<(), Report<ExecutionError>> {
        let RunningJob { handle, obligation } = self
            .start(waits, reservation, move |reservation, _| job(reservation))
            .await?;
        obligation.disarm();
        drop(handle);
        Ok(())
    }

    /// Hand the job to a free worker, or, when every worker is busy, wait until a worker takes it:
    /// workers take waiting jobs in the order they asked. Only a job that finds every worker busy
    /// counts as waiting, to the owner its caller reports waits to, from then until a worker takes
    /// it or its caller drops the wait. A job a free worker takes at once does not wait for
    /// capacity, although its worker still has to start it, however idle the node is.
    async fn start<T>(
        &self,
        waits: Option<&CapacityWaits>,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<RunningJob<T>, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        let StartingJob {
            running,
            started,
            admission,
        } = self.enqueue(reservation, job);
        let told = match (admission, waits) {
            (Admission::Waiting, Some(waits)) => Some(waits.begin_wait()),
            (Admission::Waiting, None) | (Admission::Taken, _) => None,
        };
        started.await.map_err(|cause| self.closed(cause))?;
        drop(told);
        Ok(running)
    }

    /// Run `job` with `state`, which the job hands back with its outcome, waiting for a worker like
    /// every job when every worker is busy. The state is the caller's again in `home` whenever the
    /// job ends without the caller losing it: when the caller withdraws the job before a worker
    /// takes it, and when the job returns. Only a job that panicked, or one a stopping runtime
    /// abandoned, takes the state with it.
    pub(crate) async fn run_carrying<S, T>(
        &self,
        waits: Option<&CapacityWaits>,
        reservation: Reservation,
        state: S,
        home: &mut Option<S>,
        job: impl FnOnce(S, Reservation, &Cancellation) -> (S, T) + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        S: Send + 'static,
        T: Send + 'static,
    {
        let ArmedCancellation { obligation, signal } = Cancellation::armed();
        let (result, handle) = oneshot::channel();
        let (started, has_started) = oneshot::channel();
        let work = WaitingJob {
            requested_at: Instant::now(),
            started,
            work: Box::new(CarryingJob {
                state,
                job,
                reservation,
                cancellation: signal,
                result,
            }),
        };
        let Admitted { id, admission } = self.admit(work);
        // The withdrawal below removes the waiting job and takes its state back, so this guard only
        // raises the job's cancellation once a worker has it.
        let obligation = WaitingCancellation {
            pool: Arc::clone(&self.inner),
            id: None,
            cancellation: Some(obligation),
        };
        let withdrawal = CarriedWithdrawal {
            pool: &self.inner,
            id,
            home: Some(home),
        };
        let told = match (admission, waits) {
            (Admission::Waiting, Some(waits)) => Some(waits.begin_wait()),
            (Admission::Waiting, None) | (Admission::Taken, _) => None,
        };
        has_started.await.map_err(|cause| self.closed(cause))?;
        drop(told);
        let home = withdrawal.disarm();
        let outcome = handle.await.map_err(|cause| self.closed(cause))?;
        obligation.disarm();
        let (state, value) = outcome?;
        *home = Some(state);
        Ok(value)
    }

    fn enqueue<T>(
        &self,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> StartingJob<T>
    where
        T: Send + 'static,
    {
        let ArmedCancellation { obligation, signal } = Cancellation::armed();
        let (result, handle) = oneshot::channel();
        let (started, has_started) = oneshot::channel();
        let work = WaitingJob {
            requested_at: Instant::now(),
            started,
            work: Box::new(Job {
                job,
                reservation,
                cancellation: signal,
                result,
            }),
        };
        let Admitted { id, admission } = self.admit(work);
        let obligation = WaitingCancellation {
            pool: Arc::clone(&self.inner),
            id: Some(id),
            cancellation: Some(obligation),
        };
        StartingJob {
            running: RunningJob { handle, obligation },
            started: has_started,
            admission,
        }
    }

    /// Hand `work` to a free worker, or enter it among the jobs waiting for one.
    fn admit(&self, work: WaitingJob) -> Admitted {
        let id;
        let selected;
        {
            let mut state = self.inner.state.lock();
            id = state.next_id;
            state.next_id = id
                .checked_add(1)
                .assured("a class cannot submit 2^64 jobs during one node lifetime");
            self.inner.admitted.fetch_add(1, Ordering::AcqRel);
            // A worker goes idle only once nothing waits, so a free worker means nothing waits
            // ahead of this job and it takes that worker at once.
            if state.running < self.inner.workers {
                state.running = state
                    .running
                    .checked_add(1)
                    .verified("the configured worker count bounds the active count");
                selected = Some(work);
            } else {
                assert!(
                    state.waiting.insert(id, work).is_none(),
                    "monotonic job identities are unique"
                );
                selected = None;
            }
        }
        let admission = match selected {
            Some(first) => {
                let worker = Worker {
                    pool: Arc::clone(&self.inner),
                    state: WorkerState::Selected(first.start(&self.inner)),
                };
                self.inner.dispatch(worker);
                Admission::Taken
            }
            None => Admission::Waiting,
        };
        Admitted { id, admission }
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

/// Whether a submitted job took a free worker at once or waits for one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    Taken,
    Waiting,
}

/// A submitted job's entry among those waiting, and whether a free worker took it at once.
struct Admitted {
    id: u64,
    admission: Admission,
}

struct WaitingJob {
    requested_at: Instant,
    started: oneshot::Sender<()>,
    work: Box<dyn Work>,
}

impl fmt::Debug for WaitingJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitingJob").finish_non_exhaustive()
    }
}

impl WaitingJob {
    /// Hand the job to the worker of `pool` that took it, counting how long it waited.
    fn start(self, pool: &PoolShared) -> Box<dyn Work> {
        pool.waited_nanos
            .fetch_add(elapsed_nanos(self.requested_at), Ordering::AcqRel);
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
        bound = "one callback outside the waiting jobs' fence, followed by one selection"
    )
)]
trait Work: Send {
    fn execute(self: Box<Self>, worker: &mut Worker);

    /// The state the job carries for its caller, handed back because the job will never run.
    fn into_carried(self: Box<Self>) -> Option<Box<dyn Any + Send>>;
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
                                   waiting jobs' fence; the worker owns admission, cancellation \
                                   and completion, not callback effects")
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
        // observer of completion therefore sees the worker returned when no job waits.
        worker.advance();
        result
            .send(outcome)
            .means_peer_left("the caller awaiting an execution result");
    }

    fn into_carried(self: Box<Self>) -> Option<Box<dyn Any + Send>> {
        None
    }
}

/// A job that carries state for its caller: the job hands it back with its outcome, and a job that
/// never runs hands it back from its entry among those waiting.
struct CarryingJob<S, T, F> {
    state: S,
    job: F,
    reservation: Reservation,
    cancellation: Cancellation,
    result: oneshot::Sender<Result<(S, T), Report<ExecutionError>>>,
}

impl<S, T, F> Work for CarryingJob<S, T, F>
where
    S: Send + 'static,
    T: Send + 'static,
    F: FnOnce(S, Reservation, &Cancellation) -> (S, T) + Send + 'static,
{
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "invokes one caller-owned synchronous callback outside the \
                                   waiting jobs' fence; the worker owns admission, cancellation \
                                   and completion, not callback effects")
    )]
    fn execute(self: Box<Self>, worker: &mut Worker) {
        let Self {
            state,
            job,
            reservation,
            cancellation,
            result,
        } = *self;
        let started_at = Instant::now();
        let outcome = match std::panic::catch_unwind(AssertUnwindSafe(|| {
            job(state, reservation, &cancellation)
        })) {
            Ok(carried) => Ok(carried),
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
        // observer of completion therefore sees the worker returned when no job waits.
        worker.advance();
        result
            .send(outcome)
            .means_peer_left("the caller awaiting an execution result");
    }

    fn into_carried(self: Box<Self>) -> Option<Box<dyn Any + Send>> {
        Some(Box::new(self.state))
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "each worker takes the next waiting job or returns its active place atomically",
        key = "one executor worker class",
        bound = "one selection per bounded callback; no callback under the waiting jobs' fence"
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

    /// Take the next waiting job, or go idle.
    fn advance(&mut self) {
        let next;
        {
            let mut state = self.pool.state.lock();
            next = state.waiting.pop_first();
            if next.is_none() {
                state.running = state
                    .running
                    .checked_sub(1)
                    .verified("this worker relinquishes its one active place exactly once");
                self.state = WorkerState::Idle;
            }
        }
        if let Some((_, next)) = next {
            self.state = WorkerState::Selected(next.start(&self.pool));
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
            let mut state = self.pool.state.lock();
            state.running = state
                .running
                .checked_sub(1)
                .verified("an interrupted worker still owns its active place");
            abandoned = std::mem::take(&mut state.waiting);
        }
        // Runtime teardown can drop a simulated worker between callbacks, or a blocking task
        // before it starts. Release every waiting job's charge and close its start/result channels
        // rather than retaining jobs no worker will take. Other active callbacks keep their own
        // charges.
        drop(interrupted);
        drop(abandoned);
    }
}

struct RunningJob<T> {
    handle: oneshot::Receiver<Result<T, Report<ExecutionError>>>,
    obligation: WaitingCancellation,
}

/// A submitted job still owns cancellation until its worker reports that it started.
struct StartingJob<T> {
    running: RunningJob<T>,
    started: oneshot::Receiver<()>,
    admission: Admission,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "a caller removes only its exact waiting job when it withdraws",
        key = "one admitted executor job",
        bound = "one keyed removal; destruction and charge release happen after the fence"
    )
)]
struct WaitingCancellation {
    pool: Arc<PoolShared>,
    /// The job's entry among those waiting, absent for a job handed straight to a taken worker.
    id: Option<u64>,
    cancellation: Option<CancelOnDrop>,
}

impl WaitingCancellation {
    fn disarm(mut self) {
        self.cancellation
            .take()
            .verified("only an armed submission is disarmed")
            .disarm();
    }
}

impl Drop for WaitingCancellation {
    fn drop(&mut self) {
        if self.cancellation.is_some()
            && let Some(id) = self.id
        {
            let cancelled = self.pool.state.lock().waiting.remove(&id);
            // Destroy callbacks and release their charges outside the waiting jobs' fence.
            drop(cancelled);
        }
    }
}

/// A carrying job's entry among those waiting, removed when its caller stops waiting before a
/// worker takes it; the state the job carried then goes back to the caller's `home`. Once a worker
/// has the job, the state travels with it.
struct CarriedWithdrawal<'a, S: 'static> {
    pool: &'a Arc<PoolShared>,
    id: u64,
    /// The caller's home for the state, held until a worker has the job.
    home: Option<&'a mut Option<S>>,
}

impl<'a, S: 'static> CarriedWithdrawal<'a, S> {
    /// The worker has the job: nothing waits to be withdrawn, and `home` is the caller's again.
    fn disarm(mut self) -> &'a mut Option<S> {
        self.home
            .take()
            .verified("a withdrawal holds the caller's home until it is disarmed, once")
    }
}

impl<S: 'static> Drop for CarriedWithdrawal<'_, S> {
    fn drop(&mut self) {
        let Some(home) = self.home.take() else {
            return;
        };
        let withdrawn = self.pool.state.lock().waiting.remove(&self.id);
        // Destroy the entry, and take back the state it carried, outside the fence.
        if let Some(withdrawn) = withdrawn
            && let Some(carried) = withdrawn.work.into_carried()
        {
            let state = carried
                .downcast::<S>()
                .verified("the withdrawn entry is the carrying job this guard admitted");
            *home = Some(*state);
        }
    }
}

fn elapsed_nanos(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).assured(
        "a node would have to run for 584 years for one wait or job to overflow nanosecond \
         counting",
    )
}

// Owner checks can retain an active worker without parking a thread, including under Turmoil, where
// a CPU job is one step of the only scheduler thread. Releasing it uses the same advance/dispatch
// transition as the last callback of a real worker. The crate's checks that fill a class hold its
// worker this way, never with a job that blocks until the check releases it.
#[cfg(all(test, not(feature = "loom")))]
pub(crate) struct HeldWorker(Worker);

#[cfg(all(test, not(feature = "loom")))]
impl HeldWorker {
    pub(crate) fn new(pool: &WorkerPool) -> Self {
        let mut state = pool.inner.state.lock();
        assert_eq!(state.running, 0);
        assert_eq!(pool.inner.workers, 1);
        state.running = 1;
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
mod worker_admission_checks {
    use std::{
        future::{Future as _, poll_fn},
        num::NonZeroUsize,
        task::Poll,
    };

    use super::*;
    use crate::{CpuClass, Executor, MemoryClass};

    /// A job that finds every worker busy waits for one, however many jobs already wait, and runs
    /// once a worker takes it. It keeps its charge while it waits, and its owner is told it waits
    /// for exactly as long as it does.
    #[nervix_primitives::test]
    async fn a_job_that_finds_every_worker_busy_waits_for_one_and_its_owner_is_told() {
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Extension), NonZeroUsize::MIN);
        let executor = Executor::default();
        let waits = CapacityWaits::new();
        let occupied = HeldWorker::new(&pool);

        let first_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the untouched relay budget has room");
        let mut first = Box::pin(pool.start(Some(&waits), first_charge, |_, _| ()));
        let first_state = poll_fn(|context| Poll::Ready(first.as_mut().poll(context))).await;
        assert!(
            first_state.is_pending(),
            "the first job waits for the held worker"
        );
        assert!(waits.is_waiting(), "the first job's owner is told it waits");

        let second_charge = executor
            .try_reserve(MemoryClass::Relay, 4096)
            .assured("the relay budget has room for the second job");
        let mut second = Box::pin(pool.start(Some(&waits), second_charge, |_, _| 7));
        let second_state = poll_fn(|context| Poll::Ready(second.as_mut().poll(context))).await;
        assert!(
            second_state.is_pending(),
            "a job behind a waiting job waits too, and is never refused"
        );
        let held = pool.snapshot();
        assert_eq!(held.waiting, 2, "both jobs wait for the one worker");
        assert_eq!(held.running, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 1024 + 4096);

        drop(occupied);
        let first_job = first
            .await
            .assured("the first job takes the returned worker");
        first_job
            .handle
            .await
            .assured("the first job reports its outcome")
            .assured("the first job runs once it holds the worker");
        first_job.obligation.disarm();
        let second_job = second
            .await
            .assured("the second job takes the worker after the first");
        let value = second_job
            .handle
            .await
            .assured("the second job reports its outcome")
            .assured("the second job runs once it holds the worker");
        second_job.obligation.disarm();
        assert_eq!(value, 7);
        assert!(
            !waits.is_waiting(),
            "the owner is told the wait ended once a worker took each job"
        );

        let drained = pool.snapshot();
        assert_eq!(drained.admitted, 2);
        assert_eq!(drained.waiting, 0);
        assert_eq!(drained.running, 0);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    }

    /// A job a free worker takes at once is not a wait for capacity: its owner is never told.
    #[nervix_primitives::test]
    async fn a_job_a_free_worker_takes_at_once_is_not_counted_as_waiting() {
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Extension), NonZeroUsize::MIN);
        let executor = Executor::default();
        let waits = CapacityWaits::new();
        let charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the untouched relay budget has room");
        let mut job = Box::pin(pool.run(Some(&waits), charge, |_, _| 3));
        let state = poll_fn(|context| Poll::Ready(job.as_mut().poll(context))).await;
        if state.is_pending() {
            assert!(
                !waits.is_waiting(),
                "a job its free worker has yet to start does not wait for capacity"
            );
        }
        let value = match state {
            Poll::Ready(outcome) => outcome,
            Poll::Pending => job.await,
        }
        .assured("the job runs on its free worker");
        assert_eq!(value, 3);
        assert!(!waits.is_waiting());
        assert_eq!(pool.snapshot().waiting, 0);
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
    async fn dropped_waiting_jobs_return_their_charges_and_leave_no_waiting_job() {
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Data), one());
        let executor = Executor::default();
        let occupied = HeldWorker::new(&pool);

        let first_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the untouched relay budget has room");
        let mut first = Box::pin(pool.start(None, first_charge, |_, _| ()));
        let state = poll_fn(|context| Poll::Ready(first.as_mut().poll(context))).await;
        assert!(matches!(state, Poll::Pending));
        assert_eq!(pool.snapshot().waiting, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 1024);

        let second_charge = executor
            .try_reserve(MemoryClass::Relay, 2048)
            .assured("busy workers do not consume memory admission");
        let mut second = Box::pin(pool.start(None, second_charge, |_, _| ()));
        let state = poll_fn(|context| Poll::Ready(second.as_mut().poll(context))).await;
        assert!(
            matches!(state, Poll::Pending),
            "the next job waits for the busy worker"
        );
        assert_eq!(pool.snapshot().waiting, 2);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 1024 + 2048);

        drop(second);
        assert_eq!(pool.snapshot().waiting, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 1024);
        drop(first);
        assert_eq!(pool.snapshot().waiting, 0);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
        drop(occupied);

        let next_charge = executor
            .try_reserve(MemoryClass::Relay, 4096)
            .assured("the dropped jobs returned their charges");
        let next = pool
            .start(None, next_charge, |_, _| ())
            .await
            .assured("the returned worker takes the next job");
        next.handle
            .await
            .assured("the next job reports")
            .assured("the next job completes");
        assert_eq!(pool.snapshot().running, 0);
        assert_eq!(pool.snapshot().waiting, 0);
        assert_eq!(pool.snapshot().completed, 1);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    }

    #[nervix_primitives::test]
    async fn waiting_jobs_take_the_worker_in_the_order_they_asked() {
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Data), one());
        let executor = Executor::default();
        let occupied = HeldWorker::new(&pool);
        let (completed, mut observed) = nervix_primitives::sync::mpsc::unbounded_channel();

        let first_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the untouched relay budget admits the first job");
        let first_completed = completed.clone();
        let mut first = Box::pin(pool.start(None, first_charge, move |_, _| {
            first_completed
                .send(1)
                .assured("the test holds the completion receiver");
        }));
        let first_state = poll_fn(|context| Poll::Ready(first.as_mut().poll(context))).await;
        assert!(matches!(first_state, Poll::Pending));

        let second_charge = executor
            .try_reserve(MemoryClass::Relay, 1024)
            .assured("the relay budget admits the second job");
        let mut second = Box::pin(pool.start(None, second_charge, move |_, _| {
            completed
                .send(2)
                .assured("the test holds the completion receiver");
        }));
        let second_state = poll_fn(|context| Poll::Ready(second.as_mut().poll(context))).await;
        assert!(matches!(second_state, Poll::Pending));
        assert_eq!(pool.snapshot().waiting, 2);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 2048);

        drop(occupied);
        let first_job = first
            .await
            .assured("the first waiting job takes the returned worker");
        assert_eq!(pool.snapshot().waiting, 1);
        first_job
            .handle
            .await
            .assured("the first job reports")
            .assured("the first job completes");
        let second_job = second
            .await
            .assured("the second waiting job takes the worker next");
        second_job
            .handle
            .await
            .assured("the second job reports")
            .assured("the second job completes");
        assert_eq!(observed.recv().await, Some(1));
        assert_eq!(observed.recv().await, Some(2));
        assert_eq!(pool.snapshot().running, 0);
        assert_eq!(pool.snapshot().waiting, 0);
        assert_eq!(pool.snapshot().completed, 2);
        assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
    }

    #[nervix_primitives::test]
    async fn cancelled_running_job_keeps_its_charge_until_exit() {
        let pool = WorkerPool::new(WorkerClassName::Cpu(CpuClass::Control), one());
        let executor = Executor::default();
        let charge = executor
            .try_reserve(MemoryClass::Management, 8192)
            .assured("the untouched management budget has room");
        let observed = executor.clone();
        let RunningJob { handle, obligation } = pool
            .start(None, charge, move |_, cancellation| {
                (
                    cancellation.check().is_err(),
                    observed.snapshot().management_memory.reserved_bytes,
                )
            })
            .await
            .assured("the running job took the free worker");
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
