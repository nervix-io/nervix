//! An owner is told exactly while its work waits for a place in a full worker class or for room in
//! a full memory class, and never for work that is admitted at once or refused.

use std::{future::Future, num::NonZeroUsize, pin::pin, task::Poll, time::Duration};

use error_stack::Report;
use nervix_primitives::{task::JoinHandle, time::timeout};

use super::*;
use crate::{
    ConnectorStorageWorkers, CpuClass, ExecutionConfig, Executor, MemoryClass, PendingJobCounts,
    QueueAdmission, StorageClass, WorkerCounts,
    workers::{HeldWorker, WorkerPool},
};

const WAIT: Duration = Duration::from_secs(30);

/// An executor with one worker and one waiting place in every class.
fn single_place_executor() -> Executor {
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
            pending_jobs: PendingJobCounts::uniform(one),
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
        admission: QueueAdmission,
        job: impl FnOnce() + Send + 'static,
    ) -> Result<(), Report<crate::ExecutionError>> {
        let charge = executor
            .try_reserve(MemoryClass::Bulk, 0)
            .expect("the bulk class admits an empty charge");
        match self {
            Self::Cpu(class) => {
                executor
                    .run_cpu_with(class, admission, charge, move |_, _| job())
                    .await
            }
            Self::Storage(class) => {
                executor
                    .run_storage_with(class, admission, charge, move |_, _| job())
                    .await
            }
        }
    }

    /// The workers this class runs its jobs on.
    fn pool(self, executor: &Executor) -> &WorkerPool {
        match self {
            Self::Cpu(class) => executor.cpu_pool(class),
            Self::Storage(class) => executor.storage_pool(class),
        }
    }

    fn pending(self, executor: &Executor) -> usize {
        self.pool(executor).snapshot().pending
    }
}

/// The only worker and the only waiting place of one class, held until the test releases them:
/// the worker by the pool's owner hold, the place by a job queued behind it.
///
/// The hold parks no thread. In the Turmoil build a CPU job runs as one step of the simulated
/// scheduler, so a job that blocked until the test released it would stop the only thread the
/// release can run on.
struct FullClass {
    worker: HeldWorker,
    queued: JoinHandle<()>,
}

impl FullClass {
    async fn fill(executor: &Executor, class: Class) -> Self {
        let worker = HeldWorker::new(class.pool(executor));
        let queueing = executor.clone();
        let queued = nervix_primitives::task::spawn(async move {
            class
                .run(&queueing, QueueAdmission::RefuseWhenFull, || ())
                .await
                .expect("the queued job runs once the held worker is released");
        });
        while class.pending(executor) < 1 {
            nervix_primitives::task::yield_now().await;
        }
        Self { worker, queued }
    }

    /// Release the held worker, which then runs the queued job, and wait for that job to end, which
    /// frees the class's place.
    async fn release(self) {
        drop(self.worker);
        timeout(WAIT, self.queued)
            .await
            .expect("the queued job ends once the held worker is released")
            .expect("the queued job is joined");
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

async fn waits_for_a_place_in(class: Class) {
    let executor = single_place_executor();
    let full = FullClass::fill(&executor, class).await;
    let waits = CapacityWaits::new();
    let reporting = executor.reporting_waits_to(&waits);
    let waiting = nervix_primitives::task::spawn(async move {
        class
            .run(&reporting, QueueAdmission::WaitForPlace, || ())
            .await
    });

    timeout(WAIT, waits.began())
        .await
        .expect("the owner is told its job waits for a place in the full class");
    assert!(waits.is_waiting(), "the wait lasts while the class is full");
    full.release().await;
    timeout(WAIT, waits.ended())
        .await
        .expect("the owner is told the wait ended once its job took the freed place");
    timeout(WAIT, waiting)
        .await
        .expect("the job runs once it has its place")
        .expect("the waiting job is joined")
        .expect("a job that waits for a place is never refused");
    assert!(!waits.is_waiting());
}

#[nervix_primitives::test]
async fn a_cpu_job_waiting_for_a_place_tells_its_owner_until_it_takes_one() {
    waits_for_a_place_in(Class::Cpu(CpuClass::Bulk)).await;
}

#[nervix_primitives::test]
async fn a_storage_job_waiting_for_a_place_tells_its_owner_until_it_takes_one() {
    waits_for_a_place_in(Class::Storage(StorageClass::Filesystem)).await;
}

/// Accepted work that prepares its place before it hands its state over reports the same wait.
#[nervix_primitives::test]
async fn accepted_work_preparing_its_place_tells_its_owner_while_it_waits() {
    let executor = single_place_executor();
    let full = FullClass::fill(&executor, Class::Cpu(CpuClass::Data)).await;
    let waits = CapacityWaits::new();
    let reporting = executor.reporting_waits_to(&waits);
    let charge = executor
        .try_reserve(MemoryClass::Relay, 1)
        .expect("the relay class has room");
    let preparing = nervix_primitives::task::spawn(async move {
        let place = reporting
            .accepted_work()
            .prepare_cpu(CpuClass::Data, charge)
            .await
            .expect("accepted work waits for its place");
        place.run(|_, _| ()).await
    });

    timeout(WAIT, waits.began())
        .await
        .expect("the owner is told its preparation waits for a place");
    full.release().await;
    timeout(WAIT, preparing)
        .await
        .expect("the prepared job runs")
        .expect("the preparing task is joined")
        .expect("the prepared job runs once it has its place");
    assert!(!waits.is_waiting());
}

/// A job admitted at once and a job refused for the full queue never waited, so neither tells the
/// owner anything, and a waiting job dropped before it takes its place ends its wait.
#[nervix_primitives::test]
async fn only_a_job_that_actually_waits_tells_its_owner_and_dropping_it_ends_the_wait() {
    let executor = single_place_executor();
    let waits = CapacityWaits::new();
    let changes = waits.waiting.subscribe();
    let reporting = executor.reporting_waits_to(&waits);

    Class::Cpu(CpuClass::Bulk)
        .run(&reporting, QueueAdmission::WaitForPlace, || ())
        .await
        .expect("an idle class admits the job at once");
    let full = FullClass::fill(&executor, Class::Cpu(CpuClass::Bulk)).await;
    let refused = Class::Cpu(CpuClass::Bulk)
        .run(&reporting, QueueAdmission::RefuseWhenFull, || ())
        .await
        .expect_err("a full class refuses a job that answers a request");
    assert!(matches!(
        refused.current_context(),
        crate::ExecutionError::QueueFull { .. }
    ));
    assert!(
        !changes
            .has_changed()
            .expect("the count's sender lives in the waits"),
        "neither an admitted nor a refused job told the owner it waited"
    );

    let waiting = Box::pin(Class::Cpu(CpuClass::Bulk).run(
        &reporting,
        QueueAdmission::WaitForPlace,
        || panic!("a job dropped while it waits never runs"),
    ));
    let (waiting, pending) = first_poll(waiting).await;
    assert!(pending, "the job waits for a place in the full class");
    assert!(waits.is_waiting(), "the owner is told while the job waits");
    drop(waiting);
    assert!(
        !waits.is_waiting(),
        "dropping the waiting job ends its wait at once"
    );
    full.release().await;
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.bulk_cpu.pending, 0);
    assert_eq!(snapshot.bulk_cpu.running, 0);
}

/// A charge that a memory class cannot grant yet tells the owner until the room it waits for is
/// returned.
#[nervix_primitives::test]
async fn a_charge_waiting_for_memory_room_tells_its_owner_until_it_is_granted() {
    let executor = single_place_executor();
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
