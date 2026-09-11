//! Physical monotonic deadlines and actual-UTC observation for the data plane.
//!
//! Layer: data plane.
//!
//! - **Owns.** Converting physical timeout policy into monotonic deadlines and waiting for them.
//! - **Depends on.** The vocabulary timestamp and Tokio's monotonic timer.
//! - **Must not know.** Domains, logical clock mappings, execution graphs or connector policy.

#[cfg(any(test, feature = "testing"))]
use std::time::Duration;

#[cfg(any(test, feature = "testing"))]
use error_stack::Report;
use nervix_models::Timestamp;
#[cfg(any(test, feature = "testing"))]
use thiserror::Error;
#[cfg(any(test, feature = "testing"))]
use tokio::time::{Instant, sleep_until};

#[cfg(any(test, feature = "testing"))]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalDeadlineError {
    #[error("physical deadline exceeds the monotonic clock range")]
    OutOfRange,
}

#[cfg(any(test, feature = "testing"))]
pub type PhysicalDeadlineResult<T> = Result<T, Report<PhysicalDeadlineError>>;

/// A deadline in the process-local monotonic time coordinate.
///
/// Its instant is private so logical-clock policy cannot construct or inspect it as a domain
/// timestamp. The policy that owns an operational timeout obtains one through
/// [`PhysicalDeadlineCapability`].
#[cfg(any(test, feature = "testing"))]
#[derive(Debug, Clone, Copy)]
pub struct PhysicalDeadline(Instant);

/// The capability used by operational timeout, retry and cancellation policy.
#[cfg(any(test, feature = "testing"))]
#[derive(Debug, Clone, Copy)]
pub struct PhysicalDeadlineCapability {
    _private: (),
}

#[cfg(any(test, feature = "testing"))]
impl PhysicalDeadlineCapability {
    pub(super) const fn new() -> Self {
        Self { _private: () }
    }

    pub fn after(self, timeout: Duration) -> PhysicalDeadlineResult<PhysicalDeadline> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Report::new(PhysicalDeadlineError::OutOfRange))?;
        Ok(PhysicalDeadline(deadline))
    }

    pub async fn wait_until(self, deadline: PhysicalDeadline) {
        sleep_until(deadline.0).await;
    }

    pub(super) fn is_reached(self, deadline: PhysicalDeadline) -> bool {
        Instant::now() >= deadline.0
    }
}

/// Actual UTC enters the data plane only through the physical-time owner and is returned in the
/// vocabulary timestamp type.
pub(super) fn actual_utc_now() -> Timestamp {
    Timestamp::now()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn physical_deadline_waits_on_the_monotonic_timer() {
        let capability = PhysicalDeadlineCapability::new();
        let deadline = capability
            .after(Duration::from_secs(2))
            .expect("the fixture timeout fits the monotonic clock");
        let waiter = tokio::spawn(async move {
            capability.wait_until(deadline).await;
        });

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!waiter.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        waiter.await.expect("the deadline task must finish");
    }
}
