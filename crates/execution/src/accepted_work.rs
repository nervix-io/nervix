//! Execution of work its caller has accepted and continues to own.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Waiting admission for retained work, and a charged queue place acquired before
//!   its caller transfers mutable state into a job.
//! - **Depends on.** The executor's existing memory budgets and FIFO worker admission.
//! - **Must not know.** Requests, graphs, branches, input formats or error policies.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the execution contract of caller-retained work")
)]

use error_stack::Report;

use crate::{
    AdmissionError, Cancellation, CpuClass, ExecutionError, Executor, MemoryClass, OperationLimits,
    QueueAdmission, Reservation, workers::PreparedWork,
};

/// The execution contract of data the caller has accepted and keeps until it finishes or ends.
///
/// A full class applies backpressure: it never turns a lack of worker room into a data failure.
/// The owner bounds the wait with its existing lifecycle deadline or by dropping the future.
/// This is a borrowed capability, with no allocation or reference-count change.
#[derive(Clone, Copy, Debug)]
pub struct AcceptedWork<'a> {
    pub(crate) executor: &'a Executor,
}

impl<'a> AcceptedWork<'a> {
    pub fn limits(self) -> &'a OperationLimits {
        self.executor.limits()
    }

    pub async fn reserve(
        self,
        class: MemoryClass,
        bytes: u64,
    ) -> Result<Reservation, Report<AdmissionError>> {
        self.executor.reserve(class, bytes).await
    }

    /// Run accepted work through the same bounded FIFO as request work, waiting for a place
    /// ahead of later arrivals. Dropping the future removes its wait or exact queued job.
    pub async fn run_cpu<T>(
        self,
        class: CpuClass,
        reservation: Reservation,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        self.executor
            .run_cpu_with(class, QueueAdmission::WaitForPlace, reservation, job)
            .await
    }

    /// Wait for a charged queue place before transferring caller-owned mutable state into a job.
    /// While this future waits, the caller still owns that state. Dropping it returns the charge
    /// and its place in line. After this returns, [`AcceptedCpu::run`] is ordinary admitted work.
    pub async fn prepare_cpu(
        self,
        class: CpuClass,
        reservation: Reservation,
    ) -> Result<AcceptedCpu<'a>, Report<ExecutionError>> {
        let place = self
            .executor
            .cpu_pool(class)
            .prepare(QueueAdmission::WaitForPlace)
            .await?;
        Ok(AcceptedCpu { place, reservation })
    }
}

/// A charged place in one CPU class. It borrows the class and releases both obligations on drop.
pub struct AcceptedCpu<'a> {
    place: PreparedWork<'a>,
    reservation: Reservation,
}

impl AcceptedCpu<'_> {
    pub async fn run<T>(
        self,
        job: impl FnOnce(Reservation, &Cancellation) -> T + Send + 'static,
    ) -> Result<T, Report<ExecutionError>>
    where
        T: Send + 'static,
    {
        self.place.run(self.reservation, job).await
    }
}
