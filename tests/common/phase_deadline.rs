//! The deadline that bounds one phase of the integration-test harness.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The one deadline a harness phase must finish by, the budget left before it, and
//!   bounding every operation, retried attempt and poll pause of that phase by it.
//! - **Depends on.** Tokio's monotonic clock and timers.
//! - **Must not know.** What a phase does, which scenario runs it, or production deadlines.

use std::future::Future;

use tokio::time::{Duration, Instant};

/// The one monotonic deadline a harness phase must finish by, fixed when the phase starts.
///
/// The deadline is the instant `budget` after the phase started. Every operation of the phase
/// receives only the time left before it, so a retry inside the phase never restarts the phase's
/// timeout. Like the product's shutdown deadline, it keeps the start and the budget rather than
/// their sum, so a budget of any length cannot overflow the monotonic clock.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PhaseDeadline {
    started_at: Instant,
    budget: Duration,
}

/// Whether an operation finished before its phase deadline passed.
#[derive(Debug)]
pub(crate) enum BeforeDeadline<T> {
    Finished(T),
    Passed,
}

/// What a poll last saw before its deadline passed.
#[derive(Debug)]
pub(crate) struct PollExpired<T, E> {
    /// The last output that did not satisfy the poll.
    pub(crate) last_output: Option<T>,
    /// The last attempt that failed.
    pub(crate) last_failure: Option<E>,
}

impl PhaseDeadline {
    /// Starts a phase that must finish within `budget` from now.
    pub(crate) fn after(budget: Duration) -> Self {
        Self {
            started_at: Instant::now(),
            budget,
        }
    }

    /// Starts an operation inside this phase that may take at most `budget` itself. Its deadline
    /// is whichever comes first: `budget` from now, or this phase's deadline.
    pub(crate) fn nested(self, budget: Duration) -> Self {
        Self::after(budget.min(self.remaining()))
    }

    /// The whole budget the phase started with.
    pub(crate) fn budget(self) -> Duration {
        self.budget
    }

    /// How long ago the phase started.
    pub(crate) fn elapsed(self) -> Duration {
        self.started_at.elapsed()
    }

    /// The time left before the deadline, which is zero once it has passed.
    pub(crate) fn remaining(self) -> Duration {
        self.budget
            .checked_sub(self.started_at.elapsed())
            .unwrap_or(Duration::ZERO)
    }

    pub(crate) fn has_passed(self) -> bool {
        self.started_at.elapsed() >= self.budget
    }

    /// Runs `operation` until it finishes or the deadline passes, whichever comes first.
    pub(crate) async fn bound<F>(self, operation: F) -> BeforeDeadline<F::Output>
    where
        F: Future,
    {
        // `timeout` may complete an already-ready future after its duration has elapsed. Check the
        // phase explicitly so a stream of immediately available work cannot keep a phase alive
        // forever.
        if self.has_passed() {
            return BeforeDeadline::Passed;
        }
        let bounded = tokio::time::timeout(self.remaining(), operation).await;
        match bounded {
            Ok(output) => BeforeDeadline::Finished(output),
            Err(_) => BeforeDeadline::Passed,
        }
    }

    /// Waits one poll interval, or only until the deadline when that comes first.
    pub(crate) async fn pause(self, interval: Duration) {
        tokio::time::sleep(interval.min(self.remaining())).await;
    }

    /// Repeats `attempt` until `accepts` holds for one of its outputs or the deadline passes.
    ///
    /// Every attempt receives this deadline unchanged and must finish by it, so an attempt that
    /// never completes ends the poll at the deadline, and no retry restarts the phase's budget.
    pub(crate) async fn poll_until<T, E, Attempt, Output, Accepts>(
        self,
        interval: Duration,
        mut attempt: Attempt,
        accepts: Accepts,
    ) -> Result<T, PollExpired<T, E>>
    where
        Attempt: FnMut(Self) -> Output,
        Output: Future<Output = Result<T, E>>,
        Accepts: Fn(&T) -> bool,
    {
        let mut expired = PollExpired {
            last_output: None,
            last_failure: None,
        };
        loop {
            tokio::task::consume_budget().await;
            if self.has_passed() {
                return Err(expired);
            }
            let result = attempt(self).await;
            match result {
                Ok(output) if accepts(&output) => return Ok(output),
                Ok(output) => expired.last_output = Some(output),
                Err(failure) => expired.last_failure = Some(failure),
            }
            self.pause(interval).await;
        }
    }
}
