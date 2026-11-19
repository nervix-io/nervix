//! The deadline of one public call, which the server's progress reports restart.
//!
//! A client bounds how long a call waits. While the server reports that it still serves the call's
//! request, the call is not given up: each report starts the call's whole budget again, so a call
//! ends only once it has waited its whole budget with neither an answer nor a report.

use std::{future::Future, time::Duration};

use nervix_primitives::{
    sync::blocking::Mutex,
    time::{Instant, sleep_until},
};

/// The deadline of one call: its budget, counted from when it started or from the last report
/// that the server still serves its request.
///
/// A restart only ever moves the deadline later, because a report always comes after the moment
/// the budget was last started. A wait therefore needs no wakeup when the deadline moves: it reads
/// the deadline again whenever it wakes, and sleeps on until the latest one.
pub(crate) struct CallDeadline {
    budget: Duration,
    /// When the call gives up unless a report restarts it, or `None` when no instant can hold it.
    /// A restart keeps the same budget, so a deadline no instant can hold never gets one.
    until: Mutex<Option<Instant>>,
}

/// The call waited its whole budget with neither an answer nor a progress report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CallDeadlineElapsed;

impl CallDeadline {
    pub(crate) fn starting_now(budget: Duration) -> Self {
        Self {
            budget,
            until: Mutex::new(Instant::now().checked_add(budget)),
        }
    }

    /// The call's whole budget, which every report starts again.
    pub(crate) fn budget(&self) -> Duration {
        self.budget
    }

    /// Starts the call's whole budget again from now, at a report that the server still serves
    /// its request.
    pub(crate) fn restart(&self) {
        *self.until.lock() = Instant::now().checked_add(self.budget);
    }

    /// When the call gives up unless a report restarts it, or `None` when no instant can hold it.
    pub(crate) fn current(&self) -> Option<Instant> {
        *self.until.lock()
    }

    /// Resolves once the call has waited its whole budget since it started or was last restarted.
    pub(crate) async fn elapsed(&self) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(until) = self.current() else {
                std::future::pending::<()>().await;
                return;
            };
            if Instant::now() >= until {
                return;
            }
            sleep_until(until).await;
        }
    }

    /// Runs `future` until it ends, or until the call has waited its whole budget.
    pub(crate) async fn bound<F: Future>(
        &self,
        future: F,
    ) -> Result<F::Output, CallDeadlineElapsed> {
        nervix_primitives::select! {
            biased;
            output = future => Ok(output),
            () = self.elapsed() => Err(CallDeadlineElapsed),
        }
    }
}
