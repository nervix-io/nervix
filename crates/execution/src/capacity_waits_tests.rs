//! An owner is told exactly while its work waits for a worker of a busy class or for room in a full
//! memory class, and never for work that is admitted at once.

use std::{future::Future, num::NonZeroUsize, pin::pin, task::Poll, time::Duration};

use error_stack::Report;
use nervix_primitives::{task::JoinHandle, time::timeout};

use super::*;
use crate::{
    ConnectorStorageWorkers, CpuClass, ExecutionConfig, Executor, MemoryClass, StorageClass,
    WorkerCounts,
    workers::{HeldWorker, WorkerPool},
};

const WAIT: Duration = Duration::from_secs(30);

/// An executor with one worker in every class.
fn single_worker_executor() -> Executor {
    let one = NonZeroUsize::MIN;
    Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            control_cpu: one,
            credentials_cpu: one,
            data_cpu: one,
            extension_cpu: one,
            bulk_cpu: one,
            consensus_storage: one,
            filesystem_storage: one,
            connector_storage: ConnectorStorageWorkers::resolve(Some(one))
                .expect("one worker fits the primitive permit bound"),
        },
        ..ExecutionConfig::default()
    })
    .expect("the default budgets hold the default operation limits")
}

/// Which class a test fills.
#[derive(Clone, Copy)]
enum Class {
    Cpu(CpuClass),
    Storage(StorageClass),
}

impl Class {
    async fn run(
        self,
        executor: &Executor,
        job: impl FnOnce() + Send + 'static,
    ) -> Result<(), Report<crate::ExecutionError>> {
        let charge = executor.charge_nothing(MemoryClass::Bulk);
        match self {
            Self::Cpu(class) => executor.run_cpu(class, charge, move |_, _| job()).await,
            Self::Storage(class) => executor.run_storage(class, charge, move |_, _| job()).await,
        }
    }

    /// The workers this class runs its jobs on.
    fn pool(self, executor: &Executor) -> &WorkerPool {
        match self {
            Self::Cpu(class) => executor.cpu_pool(class),
            Self::Storage(class) => executor.storage_pool(class),
        }
    }

    fn waiting(self, executor: &Executor) -> usize {
        self.pool(executor).snapshot().waiting
    }
}

/// The only worker of one class, held by the pool's owner hold until the test releases it, with a
/// job already waiting behind it, so the job a test submits next waits behind that one too.
///
/// The hold parks no thread. In the Turmoil build a CPU job runs as one step of the simulated
/// scheduler, so a job that blocked until the test released it would stop the only thread the
/// release can run on.
struct BusyClass {
    worker: HeldWorker,
    waiting: JoinHandle<()>,
}

impl BusyClass {
    async fn fill(executor: &Executor, class: Class) -> Self {
        let worker = HeldWorker::new(class.pool(executor));
        let submitting = executor.clone();
        let waiting = nervix_primitives::task::spawn(async move {
            class
                .run(&submitting, || ())
                .await
                .expect("the waiting job runs once the held worker is released");
        });
        while class.waiting(executor) < 1 {
            nervix_primitives::task::yield_now().await;
        }
        Self { worker, waiting }
    }

    /// Release the held worker, which then runs the waiting jobs in the order they asked, and wait
    /// for the first of them to end.
    async fn release(self) {
        drop(self.worker);
        timeout(WAIT, self.waiting)
            .await
            .expect("the waiting job ends once the held worker is released")
            .expect("the waiting job is joined");
    }
}

/// Poll `future` once, and return it with whether that first poll found it not ready.
async fn first_poll<F: Future + Unpin>(mut future: F) -> (F, bool) {
    let pending = std::future::poll_fn(|context| {
        Poll::Ready(std::pin::Pin::new(&mut future).poll(context).is_pending())
    })
    .await;
    (future, pending)
}

async fn waits_for_a_worker_in(class: Class) {
    let executor = single_worker_executor();
    let busy = BusyClass::fill(&executor, class).await;
    let waits = CapacityWaits::new();
    let reporting = executor.reporting_waits_to(&waits);
    let waiting = nervix_primitives::task::spawn(async move { class.run(&reporting, || ()).await });

    timeout(WAIT, waits.began())
        .await
        .expect("the owner is told its job waits for a worker of the busy class");
    assert!(
        waits.is_waiting(),
        "the wait lasts while every worker is busy"
    );
    busy.release().await;
    timeout(WAIT, waits.ended())
        .await
        .expect("the owner is told the wait ended once a worker took its job");
    timeout(WAIT, waiting)
        .await
        .expect("the job runs once a worker takes it")
        .expect("the waiting job is joined")
        .expect("a job that waits for a worker runs once one takes it");
    assert!(!waits.is_waiting());
}

#[nervix_primitives::test]
async fn a_cpu_job_waiting_for_a_worker_tells_its_owner_until_one_takes_it() {
    waits_for_a_worker_in(Class::Cpu(CpuClass::Bulk)).await;
}

#[nervix_primitives::test]
async fn a_storage_job_waiting_for_a_worker_tells_its_owner_until_one_takes_it() {
    waits_for_a_worker_in(Class::Storage(StorageClass::Filesystem)).await;
}

/// A job that carries state for its caller reports the same wait.
#[nervix_primitives::test]
async fn a_carrying_cpu_job_tells_its_owner_while_it_waits() {
    let executor = single_worker_executor();
    let busy = BusyClass::fill(&executor, Class::Cpu(CpuClass::Data)).await;
    let waits = CapacityWaits::new();
    let reporting = executor.reporting_waits_to(&waits);
    let charge = executor
        .try_reserve(MemoryClass::Relay, 1)
        .expect("the relay class has room");
    let carrying = nervix_primitives::task::spawn(async move {
        let mut home = None;
        reporting
            .run_cpu_carrying(CpuClass::Data, charge, 5_u8, &mut home, |state, _, _| {
                (state, ())
            })
            .await
            .map(|()| home)
    });

    timeout(WAIT, waits.began())
        .await
        .expect("the owner is told its carrying job waits for a worker");
    busy.release().await;
    let home = timeout(WAIT, carrying)
        .await
        .expect("the carrying job runs")
        .expect("the carrying task is joined")
        .expect("the carrying job runs once a worker takes it");
    assert_eq!(home, Some(5));
    assert!(!waits.is_waiting());
}

/// A job a free worker takes at once never waited, so it tells the owner nothing, and a waiting job
/// dropped before a worker takes it ends its wait.
#[nervix_primitives::test]
async fn only_a_job_that_actually_waits_tells_its_owner_and_dropping_it_ends_the_wait() {
    let executor = single_worker_executor();
    let waits = CapacityWaits::new();
    let changes = waits.waiting.subscribe();
    let reporting = executor.reporting_waits_to(&waits);

    Class::Cpu(CpuClass::Bulk)
        .run(&reporting, || ())
        .await
        .expect("an idle class admits the job at once");
    assert!(
        !changes
            .has_changed()
            .expect("the count's sender lives in the waits"),
        "a job admitted at once did not tell the owner it waited"
    );
    let busy = BusyClass::fill(&executor, Class::Cpu(CpuClass::Bulk)).await;

    let waiting = Box::pin(Class::Cpu(CpuClass::Bulk).run(&reporting, || {
        panic!("a job dropped while it waits never runs")
    }));
    let (waiting, pending) = first_poll(waiting).await;
    assert!(pending, "the job waits for a worker of the busy class");
    assert!(waits.is_waiting(), "the owner is told while the job waits");
    drop(waiting);
    assert!(
        !waits.is_waiting(),
        "dropping the waiting job ends its wait at once"
    );
    busy.release().await;
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.bulk_cpu.waiting, 0);
    assert_eq!(snapshot.bulk_cpu.running, 0);
}

/// A charge that a memory class cannot grant yet tells the owner until the room it waits for is
/// returned.
#[nervix_primitives::test]
async fn a_charge_waiting_for_memory_room_tells_its_owner_until_it_is_granted() {
    let executor = single_worker_executor();
    let capacity = executor.memory(MemoryClass::Management).capacity_bytes;
    let held = executor
        .try_reserve(MemoryClass::Management, capacity)
        .expect("the untouched class grants its whole capacity");
    let waits = CapacityWaits::new();
    let reporting = executor.reporting_waits_to(&waits);
    let mut charge = pin!(reporting.reserve(MemoryClass::Management, 1));
    let pending =
        std::future::poll_fn(|context| Poll::Ready(charge.as_mut().poll(context).is_pending()))
            .await;
    assert!(pending, "a full class holds the charge back");
    assert!(
        waits.is_waiting(),
        "the owner is told while the charge waits"
    );

    drop(held);
    let granted = timeout(WAIT, charge)
        .await
        .expect("the charge is granted once room returns")
        .expect("the class grants the charge");
    assert!(!waits.is_waiting());
    assert_eq!(granted.bytes(), 1);
}
