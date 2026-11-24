//! The executor: every worker class and memory budget of a node behind one shared handle.

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::{Arc, StdArc};
use thiserror::Error;

use crate::{
    AdmissionError, Cancellation, CapacityWaits, ChargedBytes, CpuClass, ExecutionConfig,
    ExecutionConfigError, ExecutionError, MemoryBudgetSnapshot, MemoryClass, OperationLimits,
    PreparedCpu, Reservation, StorageClass, WorkerClassSnapshot, memory::MemoryBudget,
    workers::WorkerPool,
};

/// The node's execution and memory admission, shared by every owner that runs variable-size work.
///
/// Cloning it is one refcount: every class, budget and counter lives behind the single inner
/// allocation. An executor within an allowance ([`Executor::reserve_allowance`]) also carries the
/// allowances it draws from, and a handle from [`Self::reporting_waits_to`] holds its owner's
/// [`CapacityWaits`], a second refcount, because that owner keeps the count to watch it.
#[derive(Clone, Debug)]
pub struct Executor {
    inner: Arc<ExecutorInner>,
    scope: Option<StdArc<AllowanceScope>>,
    /// The owner told while work submitted through this handle waits for capacity. Only a handle
    /// from [`Self::reporting_waits_to`], and the handles drawn from it, have one.
    waits: Option<CapacityWaits>,
}

/// The allowances an executor draws from, innermost first: each is the budget of its class for
/// every charge taken through that executor.
#[derive(Debug)]
struct AllowanceScope {
    budget: MemoryBudget,
    outer: Option<StdArc<AllowanceScope>>,
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
    connector_storage: WorkerPool,
    management_memory: MemoryBudget,
    commands_memory: MemoryBudget,
    relay_memory: MemoryBudget,
    state_read_memory: MemoryBudget,
    state_read_cache_memory: MemoryBudget,
    bulk_memory: MemoryBudget,
    bulk_window_memory: MemoryBudget,
    restore_metadata_memory: MemoryBudget,
    credentials_memory: MemoryBudget,
    intake_memory: MemoryBudget,
}

/// Everything one class of the executor is currently doing, for metrics and for tests that need a
/// progress guarantee rather than a sleep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorSnapshot {
    pub limits: OperationLimits,
    pub control_cpu: WorkerClassSnapshot,
    pub credentials_cpu: WorkerClassSnapshot,
    pub data_cpu: WorkerClassSnapshot,
    pub extension_cpu: WorkerClassSnapshot,
    pub bulk_cpu: WorkerClassSnapshot,
    pub consensus_storage: WorkerClassSnapshot,
    pub filesystem_storage: WorkerClassSnapshot,
    pub connector_storage: WorkerClassSnapshot,
    pub management_memory: MemoryBudgetSnapshot,
    pub commands_memory: MemoryBudgetSnapshot,
    pub relay_memory: MemoryBudgetSnapshot,
    pub state_read_memory: MemoryBudgetSnapshot,
    pub state_read_cache_memory: MemoryBudgetSnapshot,
    pub bulk_memory: MemoryBudgetSnapshot,
    pub bulk_window_memory: MemoryBudgetSnapshot,
    pub restore_metadata_memory: MemoryBudgetSnapshot,
    pub credentials_memory: MemoryBudgetSnapshot,
    pub intake_memory: MemoryBudgetSnapshot,
}

impl ExecutorSnapshot {
    /// The configured admission and queue capacity of every independently owned worker class.
    pub fn worker_classes(&self) -> [(crate::limits::WorkerClassName, WorkerClassSnapshot); 8] {
        [
            (
                crate::limits::WorkerClassName::Cpu(CpuClass::Control),
                self.control_cpu,
            ),
            (
                crate::limits::WorkerClassName::Cpu(CpuClass::Credentials),
                self.credentials_cpu,
            ),
            (
                crate::limits::WorkerClassName::Cpu(CpuClass::Data),
                self.data_cpu,
            ),
            (
                crate::limits::WorkerClassName::Cpu(CpuClass::Extension),
                self.extension_cpu,
            ),
            (
                crate::limits::WorkerClassName::Cpu(CpuClass::Bulk),
                self.bulk_cpu,
            ),
            (
                crate::limits::WorkerClassName::Storage(StorageClass::Consensus),
                self.consensus_storage,
            ),
            (
                crate::limits::WorkerClassName::Storage(StorageClass::Filesystem),
                self.filesystem_storage,
            ),
            (
                crate::limits::WorkerClassName::Storage(StorageClass::Connector),
                self.connector_storage,
            ),
        ]
    }

    /// All independent memory pools, including active and retained materialized reads.
    pub fn memory_classes(&self) -> [(MemoryClass, MemoryBudgetSnapshot); 10] {
        [
            (MemoryClass::Management, self.management_memory),
            (MemoryClass::Intake, self.intake_memory),
            (MemoryClass::Commands, self.commands_memory),
            (MemoryClass::Relay, self.relay_memory),
            (MemoryClass::StateRead, self.state_read_memory),
            (MemoryClass::StateReadCache, self.state_read_cache_memory),
            (MemoryClass::Bulk, self.bulk_memory),
            (MemoryClass::BulkWindow, self.bulk_window_memory),
            (MemoryClass::RestoreMetadata, self.restore_metadata_memory),
            (MemoryClass::Credentials, self.credentials_memory),
        ]
    }

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

    /// What one storage class is doing.
    pub fn storage_class(&self, class: StorageClass) -> WorkerClassSnapshot {
        match class {
            StorageClass::Consensus => self.consensus_storage,
            StorageClass::Filesystem => self.filesystem_storage,
            StorageClass::Connector => self.connector_storage,
        }
    }

    /// What one worker class, CPU or storage, is doing.
    pub fn worker_class(&self, class: crate::limits::WorkerClassName) -> WorkerClassSnapshot {
        match class {
            crate::limits::WorkerClassName::Cpu(class) => self.cpu_class(class),
            crate::limits::WorkerClassName::Storage(class) => self.storage_class(class),
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
    /// A handle on the same classes, budgets and allowances that tells `waits` while work submitted
    /// through it waits for a place in a full wait queue or for room in a memory class. It admits
    /// and queues exactly as this handle does; only the telling is added.
    pub fn reporting_waits_to(&self, waits: &CapacityWaits) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            scope: self.scope.clone(),
            waits: Some(waits.clone()),
        }
    }

    /// Build the executor from limits that are validated together, so a node whose budgets cannot
    /// hold the largest operation it is configured to accept fails at startup instead of stalling
    /// on the first one.
    pub fn new(config: ExecutionConfig) -> Result<Self, Report<ExecutionConfigError>> {
        let validated = config.validate()?;
        Ok(Self {
            scope: None,
            inner: Arc::new(ExecutorInner {
                control_cpu: WorkerPool::new(
                    CpuClass::Control.into(),
                    validated.workers.control_cpu,
                    validated.workers.pending_jobs.control_cpu,
                ),
                credentials_cpu: WorkerPool::new(
                    CpuClass::Credentials.into(),
                    validated.workers.credentials_cpu,
                    validated.workers.pending_jobs.credentials_cpu,
                ),
                data_cpu: WorkerPool::new(
                    CpuClass::Data.into(),
                    validated.workers.data_cpu,
                    validated.workers.pending_jobs.data_cpu,
                ),
                extension_cpu: WorkerPool::new(
                    CpuClass::Extension.into(),
                    validated.workers.extension_cpu,
                    validated.workers.pending_jobs.extension_cpu,
                ),
                bulk_cpu: WorkerPool::new(
                    CpuClass::Bulk.into(),
                    validated.workers.bulk_cpu,
                    validated.workers.pending_jobs.bulk_cpu,
                ),
                consensus_storage: WorkerPool::new(
                    StorageClass::Consensus.into(),
                    validated.workers.consensus_storage,
                    validated.workers.pending_jobs.consensus_storage,
                ),
                filesystem_storage: WorkerPool::new(
                    StorageClass::Filesystem.into(),
                    validated.workers.filesystem_storage,
                    validated.workers.pending_jobs.filesystem_storage,
                ),
                connector_storage: WorkerPool::new(
                    StorageClass::Connector.into(),
                    *validated.workers.connector_storage.value(),
                    validated.workers.pending_jobs.connector_storage,
                ),
                management_memory: MemoryBudget::new(
                    MemoryClass::Management,
                    validated.budgets.management,
                ),
                commands_memory: MemoryBudget::new(
                    MemoryClass::Commands,
                    validated.budgets.commands,
                ),
                intake_memory: MemoryBudget::new(MemoryClass::Intake, validated.budgets.intake),
                relay_memory: MemoryBudget::new(MemoryClass::Relay, validated.budgets.relay),
                state_read_memory: MemoryBudget::new(
                    MemoryClass::StateRead,
                    validated.budgets.state_read,
                ),
                state_read_cache_memory: MemoryBudget::new(
                    MemoryClass::StateReadCache,
                    validated.budgets.state_read_cache,
                ),
                bulk_memory: MemoryBudget::new(MemoryClass::Bulk, validated.budgets.bulk),
                bulk_window_memory: MemoryBudget::new(
                    MemoryClass::BulkWindow,
                    validated.budgets.bulk_window,
                ),
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
            waits: None,
        })
    }

    /// The per-operation maxima every decoder and writer bounds itself by.
    pub fn limits(&self) -> &OperationLimits {
        &self.inner.limits
    }

    /// Groups bulk quanta for one read while leaving room for a preceding batch still in flight.
    /// The capacity is immutable; deriving this bound reads no admission counters or locks.
    pub fn bulk_read_batch_bytes(&self, chunks: std::num::NonZeroU32) -> u64 {
        let pair_capacity = self.budget(MemoryClass::Bulk).capacity_bytes() / 2;
        self.limits()
            .bulk_chunk_bytes
            .as_u64()
            .checked_mul(u64::from(chunks.get()))
            .assured("the validated bulk quantum and batch count each fit u32")
            .min(pair_capacity)
    }

    /// Charge `bytes` to `class` only if the class has room for them now, without waiting.
    ///
    /// This is for an optional extra that proceeds without its charge, such as a read cache that
    /// retains a snapshot only when room is free. Work that needs its charge waits for it with
    /// [`Self::reserve`]: a full class is backpressure, never a reason to refuse work.
    pub fn try_reserve(
        &self,
        class: MemoryClass,
        bytes: u64,
    ) -> Result<Reservation, Report<AdmissionError>> {
        self.budget(class).try_reserve(bytes)
    }

    /// A charge of no bytes in `class`, for a job that allocates nothing beyond input that carries
    /// its own charge. It is granted at once whatever the class holds, so an owner that holds part
    /// of the class never waits for it.
    pub fn charge_nothing(&self, class: MemoryClass) -> Reservation {
        self.budget(class).nothing()
    }

    /// Charge `bytes` to `class`, waiting behind the operations already holding it. The caller's
    /// own deadline bounds the wait: dropping this future releases the place in line.
    pub async fn reserve(
        &self,
        class: MemoryClass,
        bytes: u64,
    ) -> Result<Reservation, Report<AdmissionError>> {
        self.budget(class).reserve(bytes, self.waits.as_ref()).await
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
    /// A class whose wait queue is full holds the job until a place frees: places go to waiting
    /// jobs in the order they asked. The caller bounds that wait by dropping the future, which
    /// gives up its place in line and returns its charge.
    pub async fn run_cpu<T>(
        &self,
        class: CpuClass,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        self.cpu_pool(class)
            .run(self.waits.as_ref(), reservation, job)
            .await
    }

    /// Wait for a charged place in `class`'s queue before the caller transfers mutable state into
    /// a job. While this future waits, the caller still owns that state. Dropping the future, or
    /// the returned place, returns the charge and the place. After this returns,
    /// [`PreparedCpu::run`] runs like any other admitted job.
    pub async fn prepare_cpu(
        &self,
        class: CpuClass,
        reservation: Reservation,
    ) -> Result<PreparedCpu<'_>, Report<ExecutionError>> {
        let place = self.cpu_pool(class).prepare(self.waits.as_ref()).await?;
        Ok(PreparedCpu { place, reservation })
    }

    /// Run one storage job on `class`'s workers. The consensus class has a single worker, so its
    /// jobs execute in the order they were admitted.
    ///
    /// A class whose wait queue is full holds the job until a place frees, as [`Self::run_cpu`]
    /// does.
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
            .run(self.waits.as_ref(), reservation, job)
            .await
    }

    /// Submit one storage job and return once it is running off the async workers.
    ///
    /// The job waits for a place in the class's queue like every job. Unlike
    /// [`Self::run_storage`], dropping the async caller after this returns does not cancel the job.
    /// The job owns its reservation until it exits.
    pub async fn submit_storage(
        &self,
        class: StorageClass,
        reservation: Reservation,
        job: impl FnOnce(Reservation) + Send + 'static,
    ) -> Result<(), Report<ExecutionError>> {
        self.storage_pool(class)
            .submit(self.waits.as_ref(), reservation, job)
            .await
    }

    pub fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            limits: self.inner.limits,
            control_cpu: self.inner.control_cpu.snapshot(),
            credentials_cpu: self.inner.credentials_cpu.snapshot(),
            data_cpu: self.inner.data_cpu.snapshot(),
            extension_cpu: self.inner.extension_cpu.snapshot(),
            bulk_cpu: self.inner.bulk_cpu.snapshot(),
            consensus_storage: self.inner.consensus_storage.snapshot(),
            filesystem_storage: self.inner.filesystem_storage.snapshot(),
            connector_storage: self.inner.connector_storage.snapshot(),
            management_memory: self.inner.management_memory.snapshot(),
            commands_memory: self.inner.commands_memory.snapshot(),
            relay_memory: self.inner.relay_memory.snapshot(),
            state_read_memory: self.inner.state_read_memory.snapshot(),
            state_read_cache_memory: self.inner.state_read_cache_memory.snapshot(),
            bulk_memory: self.inner.bulk_memory.snapshot(),
            bulk_window_memory: self.inner.bulk_window_memory.snapshot(),
            restore_metadata_memory: self.inner.restore_metadata_memory.snapshot(),
            credentials_memory: self.inner.credentials_memory.snapshot(),
            intake_memory: self.inner.intake_memory.snapshot(),
        }
    }

    /// What one memory class holds now.
    ///
    /// An owner that must not take a place in the class's queue reads this to tell whether its
    /// charge fits before it asks for one. A place in the queue takes every byte the class releases
    /// until that place is satisfied, so an owner whose room is released by other work of the same
    /// class would hold that work up behind itself. While another charge waits in the queue, the
    /// class reports no room, so reading it never overtakes that charge. It reports the class
    /// itself, not an allowance this executor draws from.
    pub fn memory(&self, class: MemoryClass) -> MemoryBudgetSnapshot {
        self.class_budget(class).snapshot()
    }

    /// Wait for a share of `bytes` of `class` for one operation's whole working set, and return
    /// an executor through which every charge of that class the operation takes is drawn from the
    /// share.
    ///
    /// Nothing waits within the share: a charge it has no room for fails with the
    /// [`Shortfall`](crate::Shortfall)
    /// the operation needs, everything it holds included, and the operation's owner gives
    /// everything back and waits for a share that large. So the operation waits for room once,
    /// while it holds nothing of the class, and never behind its own charges. Other classes, the
    /// workers and the limits are this executor's. The share is held until the returned executor
    /// and everything drawn through it are gone.
    pub async fn reserve_allowance(
        &self,
        class: MemoryClass,
        bytes: u64,
    ) -> Result<Executor, Report<AdmissionError>> {
        let share = self.reserve(class, bytes).await?;
        Ok(Self {
            inner: Arc::clone(&self.inner),
            scope: Some(StdArc::new(AllowanceScope {
                budget: MemoryBudget::allowance(share),
                outer: self.scope.clone(),
            })),
            // Work drawn from the allowance still tells this handle's owner while it waits.
            waits: self.waits.clone(),
        })
    }

    /// The budget a charge of `class` taken through this executor comes from: the innermost
    /// allowance of that class it draws from, or the class itself.
    fn budget(&self, class: MemoryClass) -> &MemoryBudget {
        let mut scope = self.scope.as_deref();
        while let Some(allowance) = scope {
            if allowance.budget.class() == class {
                return &allowance.budget;
            }
            scope = allowance.outer.as_deref();
        }
        self.class_budget(class)
    }

    /// The most a charge of `class` taken through this executor can ever be: the share of the
    /// innermost allowance of that class it draws from, or the whole class.
    pub(crate) fn budget_capacity(&self, class: MemoryClass) -> u64 {
        self.budget(class).capacity_bytes()
    }

    /// The largest share a required charge of `class` taken through this executor found missing,
    /// when this executor is within an allowance of that class and one did.
    pub(crate) fn allowance_shortfall(&self, class: MemoryClass) -> Option<u64> {
        let budget = self.budget(class);
        budget.allowance_hold()?.shortfall()
    }

    fn class_budget(&self, class: MemoryClass) -> &MemoryBudget {
        match class {
            MemoryClass::Management => &self.inner.management_memory,
            MemoryClass::Intake => &self.inner.intake_memory,
            MemoryClass::Commands => &self.inner.commands_memory,
            MemoryClass::Relay => &self.inner.relay_memory,
            MemoryClass::StateRead => &self.inner.state_read_memory,
            MemoryClass::StateReadCache => &self.inner.state_read_cache_memory,
            MemoryClass::Bulk => &self.inner.bulk_memory,
            MemoryClass::BulkWindow => &self.inner.bulk_window_memory,
            MemoryClass::RestoreMetadata => &self.inner.restore_metadata_memory,
            MemoryClass::Credentials => &self.inner.credentials_memory,
        }
    }

    pub(crate) fn cpu_pool(&self, class: CpuClass) -> &WorkerPool {
        match class {
            CpuClass::Control => &self.inner.control_cpu,
            CpuClass::Credentials => &self.inner.credentials_cpu,
            CpuClass::Data => &self.inner.data_cpu,
            CpuClass::Extension => &self.inner.extension_cpu,
            CpuClass::Bulk => &self.inner.bulk_cpu,
        }
    }

    pub(crate) fn storage_pool(&self, class: StorageClass) -> &WorkerPool {
        match class {
            StorageClass::Consensus => &self.inner.consensus_storage,
            StorageClass::Filesystem => &self.inner.filesystem_storage,
            StorageClass::Connector => &self.inner.connector_storage,
        }
    }
}

/// Why a job run through [`Executor::run_cpu_growing`] produced no outcome. The cause beneath says
/// which charge or which execution failed.
#[derive(Debug, Error)]
pub enum ExecutionFailure {
    /// A charge the job needed was not granted: it is larger than the whole class, or the class
    /// was closed. The [`AdmissionError`] is beneath.
    #[error("a charge the job needed was not granted")]
    Admission,
    /// The job was not run to its end. The [`ExecutionError`] is beneath.
    #[error("the job was not run to its end")]
    Execution,
}

/// The standard-library shared pointer the Tokio semaphores require. Every other shared value in
/// this crate uses `triomphe::Arc`.
pub(crate) type SemaphoreRef = StdArc<nervix_primitives::sync::Semaphore>;
