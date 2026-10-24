//! The connector workers' view of a retirement, shared by the ingestor's and the emitter's regressions.
//!
//! Outside the layer order: a harness.
//! - **Owns.** Waiting until the connector workers' counters show a retirement, an executor with a
//!   single connector worker, and a connector queue the test fills and releases.
//! - **Depends on.** The bounded executor and the primitive boundary.
//! - **Must not know.** Which native client a retirement destroys, graph ownership or placement.

use std::{num::NonZeroUsize, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_execution::{
    ConnectorStorageWorkers, ExecutionConfig, ExecutionError, Executor, MemoryClass, StorageClass,
    WorkerClassSnapshot, WorkerCounts,
};
use nervix_primitives::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};

/// How long a test waits for the connector workers to show what it awaits.
pub(crate) const WORKER_DEADLINE: Duration = Duration::from_secs(60);

/// How often a test reads the connector workers' counters while it waits for a retirement.
const COUNTER_POLL: Duration = Duration::from_millis(10);

/// An executor with one connector worker and one place in the connector queue.
pub(crate) fn single_connector_worker() -> Executor {
    Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            connector_storage: ConnectorStorageWorkers::resolve(Some(NonZeroUsize::MIN))
                .assured("one worker fits the primitive permit bound"),
            pending_jobs: NonZeroUsize::MIN,
            ..WorkerCounts::default()
        },
        ..ExecutionConfig::default()
    })
    .assured("the test changes only worker counts")
}

/// Waits until the connector workers' counters show `reached`, and fails naming `awaited` once
/// [`WORKER_DEADLINE`] passes.
pub(crate) async fn connector_workers_reach(
    executor: &Executor,
    awaited: &str,
    reached: impl Fn(&WorkerClassSnapshot) -> bool,
) {
    let waited = connector_workers_reached(executor, reached).await;
    assert!(
        waited,
        "{awaited}; connector workers: {:?}",
        executor.snapshot().connector_storage
    );
}

/// Whether the connector workers' counters show `reached` before [`WORKER_DEADLINE`] passes.
pub(crate) async fn connector_workers_reached(
    executor: &Executor,
    reached: impl Fn(&WorkerClassSnapshot) -> bool,
) -> bool {
    let waited = timeout(WORKER_DEADLINE, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if reached(&executor.snapshot().connector_storage) {
                return;
            }
            sleep(COUNTER_POLL).await;
        }
    })
    .await;
    waited.is_ok()
}

type ConnectorJob = JoinHandle<Result<(), Report<ExecutionError>>>;

/// The only connector worker, held by a job the test releases, and the only place in the queue,
/// taken by a job waiting behind it.
pub(crate) struct FullConnectorQueue {
    release: oneshot::Sender<()>,
    occupying: ConnectorJob,
    queued: ConnectorJob,
}

impl FullConnectorQueue {
    pub(crate) async fn fill(executor: &Executor) -> Self {
        let (started, has_started) = oneshot::channel();
        let (release, released) = oneshot::channel::<()>();
        let occupier = executor.clone();
        let charge = executor
            .try_reserve(MemoryClass::Commands, 1)
            .assured("the occupying job's envelope fits");
        let occupying = nervix_primitives::task::spawn(async move {
            occupier
                .run_storage(StorageClass::Connector, charge, move |_, _| {
                    started.send(()).assured("the test awaits worker admission");
                    released
                        .blocking_recv()
                        .assured("the test releases the only connector worker");
                })
                .await
        });
        has_started
            .await
            .assured("the occupying job holds the only connector worker");
        let waiter = executor.clone();
        let charge = executor
            .try_reserve(MemoryClass::Commands, 1)
            .assured("the queued job's envelope fits");
        let queued = nervix_primitives::task::spawn(async move {
            waiter
                .run_storage(StorageClass::Connector, charge, |_, _| ())
                .await
        });
        connector_workers_reach(
            executor,
            "the second job takes the only queue place",
            |workers| workers.pending == 1,
        )
        .await;
        Self {
            release,
            occupying,
            queued,
        }
    }

    pub(crate) async fn release(self) {
        self.release
            .send(())
            .assured("the occupying job waits for its release");
        self.occupying
            .await
            .assured("the occupying job's supervisor is joined")
            .assured("the occupying job exits");
        self.queued
            .await
            .assured("the queued job's supervisor is joined")
            .assured("the queued job runs once the worker is free");
    }
}
