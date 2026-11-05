//! Layer: engines and infrastructure.
//! - **Owns.** Governor's reference adapter over a captured boundary instant.
//! - **Depends on.** Governor's clock contract and the primitive clock.
//! - **Must not know.** UTC, domain clocks, tasks or graph policy.

use std::ops::Add;

use governor::{
    clock::{Clock, Reference},
    nanos::Nanos,
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ClockPoint(pub(crate) Instant);

impl Add<Nanos> for ClockPoint {
    type Output = Self;
    fn add(self, value: Nanos) -> Self {
        Self(
            self.0
                .checked_add(value.into())
                .verified("admission validates its maximum governor wait against this instant"),
        )
    }
}

impl Reference for ClockPoint {
    fn duration_since(&self, earlier: Self) -> Nanos {
        let duration = self.0.saturating_duration_since(earlier.0);
        u64::try_from(duration.as_nanos())
            .verified("governor references belong to one validated admission window")
            .into()
    }

    fn saturating_sub(&self, duration: Nanos) -> Self {
        let point = match self.0.checked_sub(duration.into()) {
            Some(point) => point,
            None => self.0,
        };
        Self(point)
    }
}

/// Capturing once keeps every clause in one physical-clock coordinate, including modeled builds.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AdmissionClock(pub(crate) Instant);

impl Clock for AdmissionClock {
    type Instant = ClockPoint;
    fn now(&self) -> ClockPoint {
        ClockPoint(self.0)
    }
}
