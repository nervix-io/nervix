//! The executor each node registers with the fault harness, and the workers and bulk memory a
//! scenario takes from it.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Each node's registered executor and what a scenario holds in it: occupied CPU and
//!   storage workers and held bulk memory.
//! - **Depends on.** The bounded executor's admission and snapshot API, typed node names, and the
//!   primitive boundary's tasks, atomics and blocking channels.
//! - **Must not know.** Which product work runs on the executor, how a node sizes its classes, or
//!   connector and runtime behavior.

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{
    Cancellation, ExecutionError, Executor, MemoryClass, Reservation, WorkerClassName,
};
use nervix_models::ClusterNodeName;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    blocking::Mutex,
};
use nervix_recovery::Discarded as _;

use super::FaultInjection;

/// One worker a scenario holds. Its job runs until the sender is dropped, which is how the
/// scenario releases it; nothing is ever sent.
#[derive(Debug)]
struct HeldWorker {
    class: WorkerClassName,
    _release: nervix_primitives::sync::blocking::mpsc::Sender<()>,
}

#[derive(Debug)]
pub(super) struct NodeExecution {
    executor: Executor,
    /// Occupying jobs outlive the map guard while they run, so their release senders are shared.
    holders: Arc<Mutex<Vec<HeldWorker>>>,
    /// The bulk memory a scenario holds on the node, absent while it holds none.
    bulk_memory: Option<Reservation>,
}

impl FaultInjection {
    /// The registered node executor, for a scenario that holds its real admission capacity.
    pub fn executor_on(&self, node_id: &ClusterNodeName) -> Executor {
        let Some(node) = self.inner.executions.get(node_id) else {
            panic!("node '{node_id}' has not registered its executor");
        };
        node.executor.clone()
    }

    /// Record the executor whose workers a scenario may fill.
    pub(crate) fn register_executor(&self, node_id: ClusterNodeName, executor: Executor) {
        self.inner.executions.insert(
            node_id,
            NodeExecution {
                executor,
                holders: Arc::default(),
                bulk_memory: None,
            },
        );
    }

    /// Fill every worker of `class`, a CPU or storage class, on `node_id` and return once every
    /// occupying job is running.
    pub async fn occupy_execution(
        &self,
        node_id: &ClusterNodeName,
        class: impl Into<WorkerClassName>,
    ) {
        let class = class.into();
        let node = match self.inner.executions.get(node_id) {
            Some(node) => node,
            None => panic!("node '{node_id}' has not registered its executor"),
        };
        let executor = node.executor.clone();
        let holders = node.holders.clone();
        drop(node);
        let workers = executor.snapshot().worker_class(class).workers;
        let started = Arc::new(AtomicUsize::new(0));
        for _ in 0..workers {
            let reservation = executor
                .try_reserve(MemoryClass::Bulk, 0)
                .unwrap_or_else(|error| {
                    panic!("bulk admission must accept a zero charge: {error}")
                });
            let (holder, held) = nervix_primitives::sync::blocking::mpsc::channel();
            holders.lock().push(HeldWorker {
                class,
                _release: holder,
            });
            let executor = executor.clone();
            let started = started.clone();
            nervix_primitives::task::spawn(async move {
                Self::run_holding(
                    &executor,
                    class,
                    reservation,
                    move |_charge, _cancellation| {
                        started.fetch_add(1, Ordering::AcqRel);
                        // Park until the scenario drops the holder so occupancy consumes no
                        // CPU. Nothing is ever sent, so the disconnect is the wake-up rather
                        // than a failure.
                        held.recv().discarded(
                            "dropping the holder is how the scenario releases this worker",
                        );
                    },
                )
                .await
                .discarded(
                    "this job exists to hold a worker until the scenario releases it; its outcome \
                     is not what the scenario reads",
                );
            });
        }
        while started.load(Ordering::Acquire) < workers {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
    }

    /// Run `job` on `class`'s workers. A scenario submits exactly as many holding jobs as the
    /// class has free workers, so they take free ones at once and never count among the jobs
    /// waiting for a worker.
    async fn run_holding<T: Send + 'static>(
        executor: &Executor,
        class: WorkerClassName,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>> {
        match class {
            WorkerClassName::Cpu(class) => executor.run_cpu(class, reservation, job).await,
            WorkerClassName::Storage(class) => executor.run_storage(class, reservation, job).await,
        }
    }

    /// Release every worker a scenario occupied on `node_id`, of every class. The jobs waiting for
    /// them then take the freed workers in the order they asked.
    pub fn release_execution(&self, node_id: &ClusterNodeName) {
        let node = match self.inner.executions.get(node_id) {
            Some(node) => node,
            None => panic!("node '{node_id}' has not registered its executor"),
        };
        node.holders.lock().clear();
    }

    /// Release the workers of `class` a scenario occupied on `node_id`, keeping its holds of every
    /// other class. The jobs waiting for them then take the freed workers in the order they asked.
    pub fn release_execution_class(
        &self,
        node_id: &ClusterNodeName,
        class: impl Into<WorkerClassName>,
    ) {
        let class = class.into();
        let node = match self.inner.executions.get(node_id) {
            Some(node) => node,
            None => panic!("node '{node_id}' has not registered its executor"),
        };
        node.holders.lock().retain(|held| held.class != class);
    }

    /// Release every worker a scenario occupied on any node, of every class. Scenario cleanup
    /// calls this before it stops nodes, so a step that failed while a class was held never leaves
    /// a node's workers parked behind its shutdown.
    pub fn release_all_execution(&self) {
        for node in self.inner.executions.iter() {
            node.holders.lock().clear();
        }
    }

    /// Hold every byte of `node_id`'s bulk memory but `room`, so that a charge needing more room
    /// than that waits until the scenario releases the hold. A node that already holds bulk
    /// memory for the scenario adds what is free beyond `room` to its hold.
    pub fn hold_bulk_memory_leaving_room_on(&self, node_id: &ClusterNodeName, room: u64) {
        let Some(mut node) = self.inner.executions.get_mut(node_id) else {
            panic!("node '{node_id}' has not registered its executor");
        };
        let executor = node.executor.clone();
        loop {
            let memory = executor.memory(MemoryClass::Bulk);
            let free = memory
                .capacity_bytes
                .checked_sub(memory.reserved_bytes)
                .assured("a budget never reserves more than its capacity");
            let Some(excess) = free.checked_sub(room) else {
                panic!(
                    "bulk memory on '{node_id}' has {free} bytes free, fewer than the {room} the \
                     scenario leaves"
                );
            };
            if excess == 0 {
                return;
            }
            // Other bulk work can take memory between the read and the charge, so a charge the
            // class refuses reads what is free again.
            let Ok(taken) = executor.try_reserve(MemoryClass::Bulk, excess) else {
                continue;
            };
            let held = match node.bulk_memory.take() {
                Some(held) => held
                    .merge(taken)
                    .assured("both charges come from the same node's bulk class"),
                None => taken,
            };
            node.bulk_memory = Some(held);
        }
    }

    /// Return all the bulk memory a scenario held on `node_id`.
    pub fn release_bulk_memory_on(&self, node_id: &ClusterNodeName) {
        let Some(mut node) = self.inner.executions.get_mut(node_id) else {
            panic!("node '{node_id}' has not registered its executor");
        };
        node.bulk_memory = None;
    }

    /// How many bytes of `node_id`'s bulk memory the scenario holds.
    pub fn held_bulk_memory_on(&self, node_id: &ClusterNodeName) -> u64 {
        let Some(node) = self.inner.executions.get(node_id) else {
            panic!("node '{node_id}' has not registered its executor");
        };
        match &node.bulk_memory {
            Some(held) => held.bytes(),
            None => 0,
        }
    }
}
