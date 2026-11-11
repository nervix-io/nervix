//! The bounded execution and transient-memory admission a Nervix node runs its variable-size work
//! through.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The worker classes a job is admitted into, the byte budgets that back the
//!   allocations that job performs, the cancellation boundary it checks between bounded units, and
//!   the incremental writer that fails at its budget boundary instead of growing past it. It is
//!   the only entry point for variable-size encoding, decoding, validation, hashing, compilation,
//!   program execution, snapshot construction, synchronous filesystem or database work and admitted
//!   connector control I/O. It also declares the caller-driven native runtime profile, validating
//!   its threads and stacks against the same worker configuration before constructing a runtime.
//!   Operator-supplied code, password hashing and connector driver calls
//!   have isolated admission classes: those operations can retain a worker beyond other work's
//!   latency or cancellation budget.
//! - **Depends on.** The primitive boundary for its atomics, semaphores and clock and for the
//!   mechanisms that run a job: the runtime's blocking pool for storage work, and the boundary's
//!   CPU-job mechanism, which is the same pool except in the Turmoil build. Also the byte
//!   vocabulary its budgets are configured in and the vocabulary's typed setting declarations.
//! - **Must not know.** What a job computes. It admits, charges, runs and cancels; it decides
//!   nothing about relays, branches, domains, peers, graphs or the cluster.
//!
//! Admission is always in one order: reserve the memory an operation will allocate, then take a
//! job slot, then submit. A submission that is dropped before it reaches a worker releases its
//! reservation with it; a submission already running keeps its reservation until the work actually
//! exits, because the memory is still allocated until then. A class whose wait queue is full refuses
//! a job that answers a request, whose sender can present it again, and holds a job the node already
//! accepted and keeps until a place frees, ahead of any job asking afterwards: the caller states
//! which with a [`QueueAdmission`].
//!
//! A class bounds admission, not threads. Jobs run on the process-wide Tokio blocking pool, and a
//! class's worker count is the number of its jobs that may be on that pool at once — taken before
//! the job is submitted, so a class can never hand the pool more than it is allowed. What a class
//! guarantees is therefore that no other class can consume its share of admission. It does not
//! guarantee a free thread: the pool is shared with the few owners the primitive boundary lets run
//! there without admission, each declared in `crates/primitives/blocking-permissions.toml` with
//! what bounds it, and a class holding a free permit still queues behind whatever is already
//! running there. Reserving threads per class would isolate them physically, at the cost of a pool
//! per class; the node deliberately does not do that.
//!
//! A class worker drains its admitted FIFO directly, retaining one blocking-pool dispatch until
//! it becomes idle. Submission and the last worker's idle transition share a bounded queue fence;
//! callbacks and charge release run outside it. Queued cancellation removes the exact entry.
//!
//! In the Turmoil test build, the boundary runs a CPU worker on the simulated scheduler and yields
//! between callbacks, so each bounded synchronous job body is one scheduling step. Storage jobs still use
//! the blocking pool and are outside the simulated target. The same admission, cancellation and
//! charge ownership apply in every build mode, and this executor is the only caller of the CPU-job
//! mechanism.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

mod cancellation;
mod executor;
mod limits;
mod memory;
mod runtime_capacity;
mod settings;
mod workers;

pub use runtime_capacity::{
    RuntimeMaxBlockingThreads, RuntimeThreadError, RuntimeThreadOverrides, RuntimeThreadSettings,
    RuntimeThreadStackSize, RuntimeWorkerThreads,
};
pub use settings::{
    BulkChunkBytes, BulkCpuPendingJobs, BulkCpuWorkers, BulkMemory, BulkWindowMemory,
    CommandsMemory, ConnectorStoragePendingJobs, ConsensusStoragePendingJobs,
    ConsensusStorageWorkers, ControlCpuPendingJobs, ControlCpuWorkers, CredentialsCpuPendingJobs,
    CredentialsCpuWorkers, CredentialsMemory, DataCpuPendingJobs, DataCpuWorkers,
    ExecutionOverrides, ExecutionSettings, ExtensionCpuPendingJobs, ExtensionCpuWorkers,
    FilesystemStoragePendingJobs, FilesystemStorageWorkers, IntakeMemory, ManagementMemory,
    PendingJobCounts, RelayDecodedBytes, RelayEncodedBytes, RelayMemory, RelayScratchBytes,
    ResidentReplicationBatches, RestoreMetadataMemory, StateReadCacheMemory, StateReadMemory,
    WorkerSettings,
};

pub use crate::{
    cancellation::{Cancellation, Cancelled},
    executor::{ExecutionFailure, Executor, ExecutorSnapshot},
    limits::{
        ConnectorStorageWorkers, CpuClass, ExecutionConfig, ExecutionConfigError, MemoryBudgets,
        MemoryClass, OperationLimits, StorageClass, WorkerClassName, WorkerCounts,
    },
    memory::{
        AdmissionError, BudgetedBuffer, BufferGrowthError, BufferLimitExceeded, ChargedBytes,
        MemoryBudgetSnapshot, Reservation,
    },
    workers::{ExecutionError, QueueAdmission, WorkerClassSnapshot},
};

// The ordinary tests build executors whose atomics would be Loom's in a Loom build, outside any
// model. That build runs only the cancellation models.
#[cfg(all(test, not(feature = "loom")))]
mod tests;

// These capacity tests construct complete executors outside a model execution. Modeled admission
// is exercised by the registered production-owner checks.
#[cfg(all(test, not(any(feature = "loom", feature = "shuttle"))))]
mod settings_tests;
