//! The opaque byte cadence used by the paced interconnect fixture.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** Deadlines for bounded inbound chunks at a configured byte rate.
//! - **Depends on.** The monotonic test clock and bounded duration arithmetic.
//! - **Must not know.** TLS, HTTP/2, Raft or the product's progress deadlines.

use std::time::Duration;

use nervix_primitives::time::Instant;

#[derive(Debug)]
pub(super) struct BytePace {
    bytes_per_second: u64,
    next_due: Option<Instant>,
}
impl BytePace {
    pub(super) fn new(bytes_per_second: u64) -> Self {
        assert!(bytes_per_second > 0, "the pacer needs a nonzero rate");
        Self {
            bytes_per_second,
            next_due: None,
        }
    }

    pub(super) fn deadline(&mut self, bytes: usize, observed_at: Instant) -> Instant {
        let bytes = u64::try_from(bytes).expect("one bounded read fits in u64");
        let nanos = bytes
            .checked_mul(1_000_000_000)
            .expect("one bounded chunk's nanoseconds fit in u64")
            .div_ceil(self.bytes_per_second);
        let interval = Duration::from_nanos(nanos);
        let deadline = self.next_due.map_or(observed_at + interval, |due| {
            (due + interval).max(observed_at)
        });
        self.next_due = Some(deadline);
        deadline
    }
}
