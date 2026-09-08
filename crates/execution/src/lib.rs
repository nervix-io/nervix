//! The bounded execution and transient-memory admission a Nervix node runs its variable-size work
//! through.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The worker classes a job is admitted into, the byte budgets that back the
//!   allocations that job performs, the cancellation boundary it checks between bounded units, and
//!   the incremental writer that fails at its budget boundary instead of growing past it. It is
//!   the only entry point for variable-size encoding, decoding, validation, hashing, snapshot
//!   construction, and synchronous filesystem or database work.
//! - **Depends on.** Tokio's runtime, and the byte vocabulary its budgets are configured in.
//! - **Must not know.** What a job computes. It admits, charges, runs and cancels; it decides
//!   nothing about relays, branches, domains, peers, graphs or the cluster.
//!
//! Admission is always in one order: reserve the memory an operation will allocate, then take a
//! job slot, then submit. A submission that is dropped before it reaches a worker releases its
//! reservation with it; a submission already running keeps its reservation until the work actually
//! exits, because the memory is still allocated until then.

mod cancellation;
mod limits;
mod memory;
mod workers;

use std::sync::Arc as StdArc;

use meticulous::ResultExt as _;
use thiserror::Error;
use triomphe::Arc;

pub use crate::{
    cancellation::{Cancellation, Cancelled},
    limits::{
        CpuClass, ExecutionConfig, ExecutionConfigError, MemoryBudgets, MemoryClass,
        OperationLimits, StorageClass, WorkerCounts,
    },
    memory::{
        AdmissionError, BudgetedBuffer, BufferLimitExceeded, ChargedBytes, MemoryBudgetSnapshot,
        Reservation,
    },
    workers::{ExecutionError, WorkerClassSnapshot},
};
use crate::{memory::MemoryBudget, workers::WorkerPool};

/// The node's execution and memory admission, shared by every owner that runs variable-size work.
///
/// Cloning it is one refcount: every class, budget and counter lives behind the single inner
/// allocation.
#[derive(Clone, Debug)]
pub struct Executor {
    inner: Arc<ExecutorInner>,
}

#[derive(Debug)]
struct ExecutorInner {
    limits: OperationLimits,
    control_cpu: WorkerPool,
    data_cpu: WorkerPool,
    bulk_cpu: WorkerPool,
    consensus_storage: WorkerPool,
    filesystem_storage: WorkerPool,
    management_memory: MemoryBudget,
    commands_memory: MemoryBudget,
    relay_memory: MemoryBudget,
    bulk_memory: MemoryBudget,
}

/// Everything one class of the executor is currently doing, for metrics and for tests that need a
/// progress guarantee rather than a sleep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorSnapshot {
    pub control_cpu: WorkerClassSnapshot,
    pub data_cpu: WorkerClassSnapshot,
    pub bulk_cpu: WorkerClassSnapshot,
    pub consensus_storage: WorkerClassSnapshot,
    pub filesystem_storage: WorkerClassSnapshot,
    pub management_memory: MemoryBudgetSnapshot,
    pub commands_memory: MemoryBudgetSnapshot,
    pub relay_memory: MemoryBudgetSnapshot,
    pub bulk_memory: MemoryBudgetSnapshot,
}

impl Default for Executor {
    /// The limits this proposal ships with, which are validated against each other by the crate's
    /// own tests.
    fn default() -> Self {
        Self::new(ExecutionConfig::default())
            .assured("the default budgets are defined to hold the default operation limits")
    }
}

impl Executor {
    /// Build the executor from limits that are validated together, so a node whose budgets cannot
    /// hold the largest operation it is configured to accept fails at startup instead of stalling
    /// on the first one.
    pub fn new(config: ExecutionConfig) -> Result<Self, ExecutionConfigError> {
        let validated = config.validate()?;
        Ok(Self {
            inner: Arc::new(ExecutorInner {
                control_cpu: WorkerPool::new(
                    CpuClass::Control.into(),
                    validated.workers.control_cpu,
                    validated.workers.pending_jobs,
                ),
                data_cpu: WorkerPool::new(
                    CpuClass::Data.into(),
                    validated.workers.data_cpu,
                    validated.workers.pending_jobs,
                ),
                bulk_cpu: WorkerPool::new(
                    CpuClass::Bulk.into(),
                    validated.workers.bulk_cpu,
                    validated.workers.pending_jobs,
                ),
                consensus_storage: WorkerPool::new(
                    StorageClass::Consensus.into(),
                    validated.workers.consensus_storage,
                    validated.workers.pending_jobs,
                ),
                filesystem_storage: WorkerPool::new(
                    StorageClass::Filesystem.into(),
                    validated.workers.filesystem_storage,
                    validated.workers.pending_jobs,
                ),
                management_memory: MemoryBudget::new(
                    MemoryClass::Management,
                    validated.budgets.management,
                ),
                commands_memory: MemoryBudget::new(
                    MemoryClass::Commands,
                    validated.budgets.commands,
                ),
                relay_memory: MemoryBudget::new(MemoryClass::Relay, validated.budgets.relay),
                bulk_memory: MemoryBudget::new(MemoryClass::Bulk, validated.budgets.bulk),
                limits: validated.limits,
            }),
        })
    }

    /// The per-operation maxima every decoder and writer bounds itself by.
    pub fn limits(&self) -> &OperationLimits {
        &self.inner.limits
    }

    /// Charge `bytes` to `class` without waiting. An operation that cannot be charged is refused
    /// here, before it allocates anything.
    pub fn try_reserve(
        &self,
        class: MemoryClass,
        bytes: u64,
    ) -> Result<Reservation, AdmissionError> {
        self.budget(class).try_reserve(bytes)
    }

    /// Charge `bytes` to `class`, waiting behind the operations already holding it. The caller's
    /// own deadline bounds the wait: dropping this future releases the place in line.
    pub async fn reserve(
        &self,
        class: MemoryClass,
        bytes: u64,
    ) -> Result<Reservation, AdmissionError> {
        self.budget(class).reserve(bytes).await
    }

    /// Run one CPU job on `class`'s workers, holding `reservation` until the work actually exits.
    ///
    /// The job runs off the async workers entirely. It is handed a [`Cancellation`] to check
    /// between its own bounded units: when the caller stops awaiting, the flag is raised, but the
    /// reservation stays charged until the job returns, because its allocation is still live.
    pub async fn run_cpu<T>(
        &self,
        class: CpuClass,
        reservation: Reservation,
        job: impl FnOnce(&Cancellation) -> T + Send + 'static,
    ) -> Result<T, ExecutionError>
    where
        T: Send + 'static,
    {
        self.cpu_pool(class).run(reservation, job).await
    }

    /// Run one storage job on `class`'s workers. The consensus class has a single worker, so its
    /// jobs execute in the order they were admitted.
    pub async fn run_storage<T>(
        &self,
        class: StorageClass,
        reservation: Reservation,
        job: impl FnOnce(&Cancellation) -> T + Send + 'static,
    ) -> Result<T, ExecutionError>
    where
        T: Send + 'static,
    {
        self.storage_pool(class).run(reservation, job).await
    }

    pub fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            control_cpu: self.inner.control_cpu.snapshot(),
            data_cpu: self.inner.data_cpu.snapshot(),
            bulk_cpu: self.inner.bulk_cpu.snapshot(),
            consensus_storage: self.inner.consensus_storage.snapshot(),
            filesystem_storage: self.inner.filesystem_storage.snapshot(),
            management_memory: self.inner.management_memory.snapshot(),
            commands_memory: self.inner.commands_memory.snapshot(),
            relay_memory: self.inner.relay_memory.snapshot(),
            bulk_memory: self.inner.bulk_memory.snapshot(),
        }
    }

    fn budget(&self, class: MemoryClass) -> &MemoryBudget {
        match class {
            MemoryClass::Management => &self.inner.management_memory,
            MemoryClass::Commands => &self.inner.commands_memory,
            MemoryClass::Relay => &self.inner.relay_memory,
            MemoryClass::Bulk => &self.inner.bulk_memory,
        }
    }

    fn cpu_pool(&self, class: CpuClass) -> &WorkerPool {
        match class {
            CpuClass::Control => &self.inner.control_cpu,
            CpuClass::Data => &self.inner.data_cpu,
            CpuClass::Bulk => &self.inner.bulk_cpu,
        }
    }

    fn storage_pool(&self, class: StorageClass) -> &WorkerPool {
        match class {
            StorageClass::Consensus => &self.inner.consensus_storage,
            StorageClass::Filesystem => &self.inner.filesystem_storage,
        }
    }
}

/// Why a job could not be admitted or did not produce a value.
#[derive(Debug, Error)]
pub enum ExecutionFailure {
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
}

/// The standard-library shared pointer the Tokio semaphores require. Every other shared value in
/// this crate uses `triomphe::Arc`.
type SemaphoreRef = StdArc<tokio::sync::Semaphore>;

#[cfg(test)]
mod tests;
