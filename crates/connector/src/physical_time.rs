//! Physical monotonic deadlines and actual-UTC observation for the data plane.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Converting physical timeout policy into monotonic deadlines and waiting for them,
//!   and the one read of actual UTC that the runtime and every connector share.
//! - **Depends on.** The vocabulary timestamp and Tokio's monotonic timer.
//! - **Must not know.** Domains, logical clock mappings, execution graphs or connector policy.
//!
//! The runtime reaches this module across the crate boundary, so its constructor and its UTC read
//! are public and visibility cannot confine them. `scripts/check_clock_boundaries.py` confines them
//! instead: it rejects both outside their declared owners anywhere in the workspace.

use std::time::Duration;

use error_stack::Report;
use nervix_models::Timestamp;
use thiserror::Error;
use tokio::time::{Instant, sleep_until};

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalDeadlineError {
    #[error("physical deadline exceeds the monotonic clock range")]
    OutOfRange,
}

pub type PhysicalDeadlineResult<T> = Result<T, Report<PhysicalDeadlineError>>;

/// A deadline in the process-local monotonic time coordinate.
///
/// Its instant is private so logical-clock policy cannot construct or inspect it as a domain
/// timestamp. The policy that owns an operational timeout obtains one through
/// [`PhysicalDeadlineCapability`]. Two monotonic deadlines order against each other, so a task
/// holding several of them waits for the earliest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PhysicalDeadline(Instant);

/// The capability used by operational timeout, retry and cancellation policy.
#[derive(Debug, Clone, Copy)]
pub struct PhysicalDeadlineCapability {
    _private: (),
}

impl PhysicalDeadlineCapability {
    /// The capability an operational timeout, retry or cancellation owner waits with.
    ///
    /// Only the owners `scripts/check_clock_boundaries.py` declares may call this. The type has no
    /// `Default` on purpose, because a default could be taken anywhere.
    pub const fn operational() -> Self {
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

    pub fn is_reached(self, deadline: PhysicalDeadline) -> bool {
        Instant::now() >= deadline.0
    }
}

/// Actual UTC enters the data plane only through the physical-time owner and is returned in the
/// vocabulary timestamp type.
pub fn actual_utc_now() -> Timestamp {
    Timestamp::now()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn physical_deadline_waits_on_the_monotonic_timer() {
        let capability = PhysicalDeadlineCapability::operational();
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
