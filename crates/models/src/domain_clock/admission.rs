//! Logical ingestion admission arithmetic.
//!
//! Layer: vocabulary.
//! - **Owns.** The newest 256 reached logical tick centers and inclusive event-time tolerance.
//! - **Depends on.** Validated clock periods, timestamps and durations.
//! - **Must not know.** Runtime lifecycle, notification delivery or connector behavior.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};

use super::DomainClockPeriod;
use crate::Timestamp;

/// A bounded set of reached tick centers, reconstructed from logical time in constant space.
#[derive(Debug, Clone)]
pub struct DomainAdmissionWindow {
    first: Timestamp,
    last: Timestamp,
    period: DomainClockPeriod,
    skew: Duration,
}

impl DomainAdmissionWindow {
    /// The number of reached logical tick centers retained for ingestion admission.
    pub const RETAINED_POSITION_COUNT: u32 = 256;

    const RETAINED_PRECEDING_POSITION_COUNT: u32 = Self::RETAINED_POSITION_COUNT - 1;

    /// Before the origin there are no eligible centers. At the origin position zero is eligible.
    pub fn reached(
        origin: Timestamp,
        now: Timestamp,
        period: DomainClockPeriod,
        skew: Duration,
    ) -> Option<Self> {
        let elapsed = now.duration_since(origin)?;
        let period_nanos = u128::from(period.as_nanos());
        let frontier = elapsed.as_nanos() / period_nanos;
        let retained_position_count = u128::from(Self::RETAINED_POSITION_COUNT);
        let retained_preceding_position_count = u128::from(Self::RETAINED_PRECEDING_POSITION_COUNT);
        // Retention clamps at the origin until the complete nonnegative history exists.
        let first_position = if frontier < retained_position_count {
            0
        } else {
            frontier
                .checked_sub(retained_preceding_position_count)
                .verified("the frontier has reached the complete retained history")
        };
        let center = |position: u128| {
            let offset = position
                .checked_mul(period_nanos)
                .assured("each retained position is at most elapsed / period");
            let offset = u64::try_from(offset)
                .assured("the distance between two signed nanosecond timestamps fits u64");
            origin
                .checked_add(Duration::from_nanos(offset))
                .assured("each reached center lies between origin and the representable now")
        };
        Some(Self {
            first: center(first_position),
            last: center(frontier),
            period,
            skew,
        })
    }

    pub fn contains(&self, event: Timestamp) -> bool {
        if event < self.first {
            return self
                .first
                .duration_since(event)
                .verified("event precedes the first retained center")
                <= self.skew;
        }
        if event > self.last {
            return event
                .duration_since(self.last)
                .verified("event follows the last reached center")
                <= self.skew;
        }
        let elapsed = event
            .duration_since(self.first)
            .verified("event is between the first and last centers");
        let period = u128::from(self.period.as_nanos());
        let remainder = elapsed.as_nanos() % period;
        let distance_to_next = period
            .checked_sub(remainder)
            .assured("the remainder is strictly less than the positive period");
        remainder <= self.skew.as_nanos() || distance_to_next <= self.skew.as_nanos()
    }
}

const _: () = assert!(DomainAdmissionWindow::RETAINED_POSITION_COUNT > 0);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_and_skew_boundaries_are_inclusive() {
        let window = DomainAdmissionWindow::reached(
            Timestamp::from_unix_nanos(1000),
            Timestamp::from_unix_nanos(1099),
            Duration::from_nanos(100)
                .try_into()
                .assured("period is positive"),
            Duration::from_nanos(10),
        )
        .assured("now has reached the origin");
        for nanos in [990, 1000, 1010] {
            assert!(
                window.contains(Timestamp::from_unix_nanos(nanos)),
                "{nanos}"
            );
        }
        for nanos in [989, 1011, 1090, 1100, 1110] {
            assert!(
                !window.contains(Timestamp::from_unix_nanos(nanos)),
                "{nanos}"
            );
        }
    }

    #[test]
    fn exactly_256_reached_positions_are_retained() {
        let period = Duration::from_nanos(100)
            .try_into()
            .assured("period is positive");
        let retained_position_count = i64::from(DomainAdmissionWindow::RETAINED_POSITION_COUNT);
        let retained_preceding_position_count = retained_position_count
            .checked_sub(1)
            .assured("a nonnegative retained position count is above i64::MIN");
        let frontier_after_full_history = retained_position_count
            .checked_add(1)
            .assured("the retained position count is far below i64::MAX");
        for frontier in [
            0_i64,
            retained_preceding_position_count,
            retained_position_count,
            frontier_after_full_history,
            10000,
        ] {
            let reached = frontier
                .checked_mul(100)
                .assured("fixture frontier is at most 10000");
            let window = DomainAdmissionWindow::reached(
                Timestamp::from_unix_nanos(0),
                Timestamp::from_unix_nanos(reached),
                period,
                Duration::from_nanos(10),
            )
            .assured("fixture time is nonnegative");
            let first = frontier
                .checked_sub(retained_preceding_position_count)
                .assured("a nonnegative frontier minus a u32-sized count fits i64")
                .max(0);
            for position in 0..=frontier
                .checked_add(1)
                .assured("fixture frontier is at most 10000")
            {
                let center = position
                    .checked_mul(100)
                    .assured("fixture position is at most 10001");
                assert_eq!(
                    window.contains(Timestamp::from_unix_nanos(center)),
                    position >= first && position <= frontier,
                    "frontier={frontier}, position={position}"
                );
            }
            let edge = first
                .checked_mul(100)
                .assured("fixture first position is bounded")
                .checked_sub(10)
                .assured("fixture values are far from the timestamp minimum");
            assert!(window.contains(Timestamp::from_unix_nanos(edge)));
            assert!(!window.contains(Timestamp::from_unix_nanos(
                edge.checked_sub(1).assured("fixture edge is bounded")
            )));
        }
    }

    #[test]
    fn zero_skew_gaps_and_overlapping_windows_use_exact_distances() {
        let period = Duration::from_nanos(100)
            .try_into()
            .assured("period is positive");
        for skew in [0_u64, 10, 50, 100, 1000] {
            let window = DomainAdmissionWindow::reached(
                Timestamp::from_unix_nanos(-100),
                Timestamp::from_unix_nanos(100),
                period,
                Duration::from_nanos(skew),
            )
            .assured("now follows origin");
            for event in -1200_i64..=1200 {
                // Three centers are the entire eligible set in this bounded oracle.
                let expected = [-100_i64, 0, 100]
                    .iter()
                    .any(|center| event.abs_diff(*center) <= skew);
                assert_eq!(
                    window.contains(Timestamp::from_unix_nanos(event)),
                    expected,
                    "skew={skew}, event={event}"
                );
            }
        }
    }

    #[test]
    fn full_timestamp_range_and_maximum_period_do_not_overflow() {
        for nanos in [1, u64::MAX] {
            let window = DomainAdmissionWindow::reached(
                Timestamp::from_unix_nanos(i64::MIN),
                Timestamp::from_unix_nanos(i64::MAX),
                Duration::from_nanos(nanos)
                    .try_into()
                    .assured("period is positive and fits u64"),
                Duration::ZERO,
            )
            .assured("maximum follows minimum");
            assert!(window.contains(Timestamp::from_unix_nanos(i64::MAX)));
            assert_eq!(
                window.contains(Timestamp::from_unix_nanos(i64::MIN)),
                nanos == u64::MAX
            );
        }
    }
}
