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
//! - **Depends on.** Tokio's runtime and its blocking pool, the primitive boundary for its atomics,
//!   the simulation scheduler when enabled, and the byte vocabulary its budgets are configured in.
//! - **Must not know.** What a job computes. It admits, charges, runs and cancels; it decides
//!   nothing about relays, branches, domains, peers, graphs or the cluster.
//!
//! Admission is always in one order: reserve the memory an operation will allocate, then take a
//! job slot, then submit. A submission that is dropped before it reaches a worker releases its
//! reservation with it; a submission already running keeps its reservation until the work actually
//! exits, because the memory is still allocated until then.
//!
//! A class bounds admission, not threads. Jobs run on the process-wide Tokio blocking pool, and a
//! class's worker count is the number of its jobs that may be on that pool at once — taken before
//! the job is submitted, so a class can never hand the pool more than it is allowed. What a class
//! guarantees is therefore that no other class can consume its share of admission. It does not
//! guarantee a free thread: the pool is shared with every other `spawn_blocking` caller in the
//! process, and a class holding a free permit still queues behind whatever is already running
//! there. Reserving threads per class would isolate them physically, at the cost of a pool per
//! class; the node deliberately does not do that.
//!
//! In the Turmoil test build, admitted CPU jobs run as tasks on the simulated scheduler. Each
//! bounded synchronous job body is one scheduling step. Storage jobs still use the blocking pool
//! and are outside the simulated target. The same admission, cancellation and charge ownership
//! apply in either build mode.
//!
//! A Loom build is the cancellation protocol and the models that explore it, and nothing else. Loom
//! models synchronous owners, and the primitive boundary gives a Loom build no async runtime, so the
//! executor that runs jobs on Tokio is not part of it; outside the models' test build, a Loom build
//! of this crate is empty.
#![cfg(any(test, not(feature = "loom")))]

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

mod cancellation;
#[cfg(not(feature = "loom"))]
mod executor;
#[cfg(not(feature = "loom"))]
mod limits;
#[cfg(not(feature = "loom"))]
mod memory;
#[cfg(not(feature = "loom"))]
mod workers;

pub use crate::cancellation::{Cancellation, Cancelled};
#[cfg(not(feature = "loom"))]
pub use crate::{
    executor::{ExecutionFailure, Executor, ExecutorSnapshot},
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

// The ordinary tests build executors whose atomics would be Loom's in a Loom build, outside any
// model. That build runs only the cancellation models.
#[cfg(all(test, not(feature = "loom")))]
mod tests;
