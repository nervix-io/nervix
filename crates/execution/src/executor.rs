//! The executor: every worker class and memory budget of a node behind one shared handle.

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_primitives::sync::{Arc, StdArc};
use thiserror::Error;

use crate::{
    AdmissionError, Cancellation, ChargedBytes, CpuClass, ExecutionConfig, ExecutionConfigError,
    ExecutionError, MemoryBudgetSnapshot, MemoryClass, OperationLimits, QueueAdmission,
    Reservation, StorageClass, WorkerClassSnapshot, memory::MemoryBudget, workers::WorkerPool,
};

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
    credentials_cpu: WorkerPool,
    data_cpu: WorkerPool,
    extension_cpu: WorkerPool,
    bulk_cpu: WorkerPool,
    consensus_storage: WorkerPool,
    filesystem_storage: WorkerPool,
    management_memory: MemoryBudget,
    commands_memory: MemoryBudget,
    relay_memory: MemoryBudget,
    bulk_memory: MemoryBudget,
    restore_metadata_memory: MemoryBudget,
    credentials_memory: MemoryBudget,
}

/// Everything one class of the executor is currently doing, for metrics and for tests that need a
/// progress guarantee rather than a sleep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorSnapshot {
    pub control_cpu: WorkerClassSnapshot,
    pub credentials_cpu: WorkerClassSnapshot,
    pub data_cpu: WorkerClassSnapshot,
    pub extension_cpu: WorkerClassSnapshot,
    pub bulk_cpu: WorkerClassSnapshot,
    pub consensus_storage: WorkerClassSnapshot,
    pub filesystem_storage: WorkerClassSnapshot,
    pub management_memory: MemoryBudgetSnapshot,
    pub commands_memory: MemoryBudgetSnapshot,
    pub relay_memory: MemoryBudgetSnapshot,
    pub bulk_memory: MemoryBudgetSnapshot,
    pub restore_metadata_memory: MemoryBudgetSnapshot,
    pub credentials_memory: MemoryBudgetSnapshot,
}

impl ExecutorSnapshot {
    /// What one CPU class is doing.
    pub fn cpu_class(&self, class: CpuClass) -> WorkerClassSnapshot {
        match class {
            CpuClass::Control => self.control_cpu,
            CpuClass::Credentials => self.credentials_cpu,
            CpuClass::Data => self.data_cpu,
            CpuClass::Extension => self.extension_cpu,
            CpuClass::Bulk => self.bulk_cpu,
        }
    }
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
    pub fn new(config: ExecutionConfig) -> Result<Self, Report<ExecutionConfigError>> {
        let validated = config.validate()?;
        Ok(Self {
            inner: Arc::new(ExecutorInner {
                control_cpu: WorkerPool::new(
                    CpuClass::Control.into(),
                    validated.workers.control_cpu,
                    validated.workers.pending_jobs,
                ),
                credentials_cpu: WorkerPool::new(
                    CpuClass::Credentials.into(),
                    validated.workers.credentials_cpu,
                    validated.workers.pending_jobs,
                ),
                data_cpu: WorkerPool::new(
                    CpuClass::Data.into(),
                    validated.workers.data_cpu,
                    validated.workers.pending_jobs,
                ),
                extension_cpu: WorkerPool::new(
                    CpuClass::Extension.into(),
                    validated.workers.extension_cpu,
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
                restore_metadata_memory: MemoryBudget::new(
                    MemoryClass::RestoreMetadata,
                    validated.budgets.restore_metadata,
                ),
                credentials_memory: MemoryBudget::new(
                    MemoryClass::Credentials,
                    validated.budgets.credentials,
                ),
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
    ) -> Result<Reservation, Report<AdmissionError>> {
        self.budget(class).try_reserve(bytes)
    }

    /// Charge `bytes` to `class`, waiting behind the operations already holding it. The caller's
    /// own deadline bounds the wait: dropping this future releases the place in line.
    pub async fn reserve(
        &self,
        class: MemoryClass,
        bytes: u64,
    ) -> Result<Reservation, Report<AdmissionError>> {
        self.budget(class).reserve(bytes).await
    }

    /// Charge `class` for an allocation the caller already holds, and take ownership of it. The
    /// bytes are not copied: the charge simply starts covering them.
    pub async fn charge_owned(
        &self,
        class: MemoryClass,
        bytes: Vec<u8>,
    ) -> Result<ChargedBytes, Report<AdmissionError>> {
        let reservation = self.reserve(class, bytes.len().arch_into()).await?;
        Ok(ChargedBytes::from_owned(bytes, reservation))
    }

    /// The same, refusing rather than waiting when the class is full.
    pub fn try_charge_owned(
        &self,
        class: MemoryClass,
        bytes: Vec<u8>,
    ) -> Result<ChargedBytes, Report<AdmissionError>> {
        let reservation = self.try_reserve(class, bytes.len().arch_into())?;
        Ok(ChargedBytes::from_owned(bytes, reservation))
    }

    /// Run one CPU job on `class`'s workers, holding `reservation` until the work actually exits.
    ///
    /// In production and Shuttle builds the job runs off the async workers. In Turmoil builds a
    /// bounded, simulation-approved CPU job runs as one task on the simulated scheduler. It is
    /// handed the reservation it allocates under, and a [`Cancellation`] to check between its own
    /// bounded units: when the caller stops awaiting, the flag is raised, but the charge stays held
    /// until the job returns, because its allocation is still live.
    ///
    /// A class whose wait queue is full refuses the job, which is what work answering a request
    /// needs; [`Self::run_cpu_with`] states another [`QueueAdmission`].
    pub async fn run_cpu<T>(
        &self,
        class: CpuClass,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        self.run_cpu_with(class, QueueAdmission::RefuseWhenFull, reservation, job)
            .await
    }

    /// Run one CPU job like [`Self::run_cpu`], with `admission` saying what a full wait queue does
    /// with it.
    pub async fn run_cpu_with<T>(
        &self,
        class: CpuClass,
        admission: QueueAdmission,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        self.cpu_pool(class).run(admission, reservation, job).await
    }

    /// Run one storage job on `class`'s workers. The consensus class has a single worker, so its
    /// jobs execute in the order they were admitted.
    pub async fn run_storage<T>(
        &self,
        class: StorageClass,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        self.storage_pool(class)
            .run(QueueAdmission::RefuseWhenFull, reservation, job)
            .await
    }

    /// Submit one storage job and return once it is running off the async workers.
    ///
    /// Unlike [`Self::run_storage`], dropping the async caller after this returns does not cancel
    /// the job. The job owns its reservation until it exits.
    pub async fn submit_storage(
        &self,
        class: StorageClass,
        reservation: Reservation,
        job: impl FnOnce(Reservation) + Send + 'static,
    ) -> Result<(), Report<ExecutionError>> {
        self.storage_pool(class).submit(reservation, job).await
    }

    pub fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            control_cpu: self.inner.control_cpu.snapshot(),
            credentials_cpu: self.inner.credentials_cpu.snapshot(),
            data_cpu: self.inner.data_cpu.snapshot(),
            extension_cpu: self.inner.extension_cpu.snapshot(),
            bulk_cpu: self.inner.bulk_cpu.snapshot(),
            consensus_storage: self.inner.consensus_storage.snapshot(),
            filesystem_storage: self.inner.filesystem_storage.snapshot(),
            management_memory: self.inner.management_memory.snapshot(),
            commands_memory: self.inner.commands_memory.snapshot(),
            relay_memory: self.inner.relay_memory.snapshot(),
            bulk_memory: self.inner.bulk_memory.snapshot(),
            restore_metadata_memory: self.inner.restore_metadata_memory.snapshot(),
            credentials_memory: self.inner.credentials_memory.snapshot(),
        }
    }

    fn budget(&self, class: MemoryClass) -> &MemoryBudget {
        match class {
            MemoryClass::Management => &self.inner.management_memory,
            MemoryClass::Commands => &self.inner.commands_memory,
            MemoryClass::Relay => &self.inner.relay_memory,
            MemoryClass::Bulk => &self.inner.bulk_memory,
            MemoryClass::RestoreMetadata => &self.inner.restore_metadata_memory,
            MemoryClass::Credentials => &self.inner.credentials_memory,
        }
    }

    fn cpu_pool(&self, class: CpuClass) -> &WorkerPool {
        match class {
            CpuClass::Control => &self.inner.control_cpu,
            CpuClass::Credentials => &self.inner.credentials_cpu,
            CpuClass::Data => &self.inner.data_cpu,
            CpuClass::Extension => &self.inner.extension_cpu,
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
pub(crate) type SemaphoreRef = StdArc<nervix_primitives::sync::Semaphore>;
