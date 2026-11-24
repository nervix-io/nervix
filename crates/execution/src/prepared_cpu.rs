//! A charged CPU queue place taken before its caller hands mutable state to a job.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** A place in one CPU class's queue, held with the charge its job will run under.
//! - **Depends on.** The executor's memory budgets and FIFO worker admission.
//! - **Must not know.** Requests, graphs, branches, input formats or error policies.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns a CPU queue place taken ahead of its job")
)]

use error_stack::Report;

use crate::{Cancellation, ExecutionError, Reservation, workers::PreparedWork};

/// A charged place in one CPU class, taken by [`crate::Executor::prepare_cpu`] before its caller
/// moves state into the job. It borrows the class and releases both the place and the charge on
/// drop, so a caller that stops before running the job keeps whatever it would have moved.
pub struct PreparedCpu<'a> {
    pub(crate) place: PreparedWork<'a>,
    pub(crate) reservation: Reservation,
}

impl PreparedCpu<'_> {
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
