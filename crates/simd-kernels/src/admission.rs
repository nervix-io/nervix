//! Exact paced-domain admission over a contiguous column of signed nanosecond instants.
//!
//! The reached first and last centers bound the column. A timestamp outside them may still be
//! within skew of an edge center. Between them, its distance to the nearest period boundary
//! decides admission. The period reduction is prepared once for the window, before reading rows.

use std::num::NonZeroU64;

use fearless_simd::{Level, dispatch, prelude::*};
use meticulous::ResultExt as _;

use crate::LEVEL;

/// A reached window's lane operands and prepared exact period reduction.
#[derive(Debug, Clone)]
pub struct AdmissionKernel {
    first: i64,
    last: i64,
    skew: u64,
    period: u64,
    reduction: PeriodReduction,
}

#[derive(Debug, Clone)]
enum PeriodReduction {
    /// Every timestamp between the edge centers is close enough to some center.
    NoGaps,
    PowerOfTwo {
        mask: u64,
    },
    /// `floor(2^64 / period)`. The estimated quotient needs at most one correction.
    Reciprocal {
        multiplier: u64,
    },
}

struct WindowLanes<S: Simd> {
    first: S::i64s,
    last: S::i64s,
    skew: S::u64s,
}

impl AdmissionKernel {
    /// Prepares the reached centers, inclusive skew, and period for one admission window.
    pub fn new(first: i64, last: i64, period: NonZeroU64, skew: u64) -> Self {
        let period = period.get();
        let reduction = if period == 1 || skew >= period.div_ceil(2) {
            PeriodReduction::NoGaps
        } else if period.is_power_of_two() {
            PeriodReduction::PowerOfTwo { mask: period - 1 }
        } else {
            let multiplier = (1_u128 << 64) / u128::from(period);
            PeriodReduction::Reciprocal {
                multiplier: u64::try_from(multiplier)
                    .assured("a period above one has a reciprocal below 2^64"),
            }
        };
        Self {
            first,
            last,
            skew,
            period,
            reduction,
        }
    }

    /// One bit per timestamp, least-significant bit first within each byte. The final byte's
    /// unused high bits are clear. The kernel reads the column once and retains no row payloads.
    pub fn admit(&self, timestamps: &[i64]) -> Vec<u8> {
        let level = *LEVEL.get_or_init(Level::new);
        self.with_level(level, timestamps)
    }

    fn with_level(&self, level: Level, timestamps: &[i64]) -> Vec<u8> {
        dispatch!(level, simd => self.admit_lanes(simd, timestamps))
    }

    #[inline(always)]
    fn admit_lanes<S: Simd>(&self, simd: S, timestamps: &[i64]) -> Vec<u8> {
        simd.vectorize(
            #[inline(always)]
            || {
                let mut bits = vec![0_u8; timestamps.len().div_ceil(8)];
                let width = S::i64s::LEN;
                let bounds = WindowLanes {
                    first: S::i64s::splat(simd, self.first),
                    last: S::i64s::splat(simd, self.last),
                    skew: S::u64s::splat(simd, self.skew),
                };
                let mut chunks = timestamps.chunks_exact(width);
                let mut offset = 0;
                for chunk in chunks.by_ref() {
                    let lanes = S::i64s::from_slice(simd, chunk);
                    self.admit_chunk::<S>(simd, lanes, &bounds, width, offset, &mut bits);
                    offset += width;
                }
                let tail = chunks.remainder();
                if !tail.is_empty() {
                    let mut lanes = S::i64s::splat(simd, self.first);
                    lanes.as_mut_slice()[..tail.len()].copy_from_slice(tail);
                    self.admit_chunk::<S>(simd, lanes, &bounds, tail.len(), offset, &mut bits);
                }
                bits
            },
        )
    }

    #[inline(always)]
    fn admit_chunk<S: Simd>(
        &self,
        simd: S,
        lanes: S::i64s,
        bounds: &WindowLanes<S>,
        count: usize,
        offset: usize,
        bits: &mut [u8],
    ) {
        let before = lanes.simd_lt(bounds.first).to_bitmask();
        let after = lanes.simd_gt(bounds.last).to_bitmask();
        // The wrapping difference of two ordered signed instants is their exact unsigned
        // distance, including the full i64::MIN..=i64::MAX range.
        let distance_before: S::u64s = (bounds.first - lanes).bitcast();
        let distance_after: S::u64s = (lanes - bounds.last).bitcast();
        let mut admitted = (before & distance_before.simd_le(bounds.skew).to_bitmask())
            | (after & distance_after.simd_le(bounds.skew).to_bitmask());
        let interior = !(before | after);
        match self.reduction {
            PeriodReduction::NoGaps => admitted |= interior,
            PeriodReduction::PowerOfTwo { mask } => {
                let elapsed: S::u64s = (lanes - bounds.first).bitcast();
                let remainder = elapsed & S::u64s::splat(simd, mask);
                admitted |= self.admitted_remainders(simd, remainder, bounds.skew) & interior;
            }
            PeriodReduction::Reciprocal { multiplier } => {
                let elapsed: S::u64s = (lanes - bounds.first).bitcast();
                let divisor = S::u64s::splat(simd, self.period);
                let reciprocal = S::u64s::splat(simd, multiplier);
                let quotient = product_high(simd, elapsed, reciprocal);
                let provisional = elapsed - quotient * divisor;
                // floor(2^64 / period) underestimates by at most one quotient unit.
                let remainder = provisional
                    .simd_ge(divisor)
                    .select(provisional - divisor, provisional);
                admitted |= self.admitted_remainders(simd, remainder, bounds.skew) & interior;
            }
        }
        for lane in 0..count {
            if admitted & (1_u64 << lane) != 0 {
                let row = offset + lane;
                bits[row / 8] |= 1_u8 << (row % 8);
            }
        }
    }

    #[inline(always)]
    fn admitted_remainders<S: Simd>(&self, simd: S, remainder: S::u64s, skew: S::u64s) -> u64 {
        let period = S::u64s::splat(simd, self.period);
        remainder.simd_le(skew).to_bitmask() | (period - remainder).simd_le(skew).to_bitmask()
    }
}

/// High 64 bits of each unsigned 64 × 64 product, using four 32-bit partial products. This
/// works at every fearless-simd level, including AVX2 where no native u64 high multiply exists.
#[inline(always)]
fn product_high<S: Simd>(simd: S, left: S::u64s, right: S::u64s) -> S::u64s {
    let mask = S::u64s::splat(simd, u64::from(u32::MAX));
    let left_low = left & mask;
    let right_low = right & mask;
    let left_high = left >> 32;
    let right_high = right >> 32;
    let low = left_low * right_low;
    let cross_left = left_high * right_low;
    let cross_right = left_low * right_high;
    let high = left_high * right_high;
    let middle = (low >> 32) + (cross_left & mask) + (cross_right & mask);
    high + (cross_left >> 32) + (cross_right >> 32) + (middle >> 32)
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;

    use super::*;
    use crate::supported_levels;

    fn bit(bits: &[u8], row: usize) -> bool {
        bits[row / 8] & (1_u8 << (row % 8)) != 0
    }

    fn scalar(first: i64, last: i64, period: u64, skew: u64, event: i64) -> bool {
        if event < first {
            return event.abs_diff(first) <= skew;
        }
        if event > last {
            return event.abs_diff(last) <= skew;
        }
        let remainder = event.abs_diff(first) % period;
        remainder <= skew || period - remainder <= skew
    }

    #[test]
    fn every_level_matches_scalar_admission_at_edges_and_across_the_full_timestamp_range() {
        let mut random = fastrand::Rng::with_seed(0x00A1_1D17);
        for period in [1_u64, 2, 3, 7, 100, 1_000_000_000, u64::MAX] {
            for skew in [0_u64, 1, 2, 49, 100, u64::MAX] {
                for (first, last) in [
                    (1_000_i64, 2_000_i64),
                    (i64::MIN, i64::MAX),
                    (i64::MIN, i64::MIN),
                    (i64::MAX, i64::MAX),
                ] {
                    let kernel = AdmissionKernel::new(
                        first,
                        last,
                        NonZeroU64::new(period).assured("fixture periods are positive"),
                        skew,
                    );
                    let mut events = vec![
                        i64::MIN,
                        i64::MIN + 1,
                        first,
                        first.checked_sub(1).unwrap_or(first),
                        first.checked_add(1).unwrap_or(first),
                        last.checked_sub(1).unwrap_or(last),
                        last,
                        last.checked_add(1).unwrap_or(last),
                        i64::MAX - 1,
                        i64::MAX,
                    ];
                    events.extend((0..77).map(|_| random.i64(..)));
                    for level in supported_levels() {
                        let bits = kernel.with_level(level, &events);
                        assert_eq!(bits.len(), events.len().div_ceil(8));
                        for (row, event) in events.iter().enumerate() {
                            assert_eq!(
                                bit(&bits, row),
                                scalar(first, last, period, skew, *event),
                                "period={period}, skew={skew}, first={first}, last={last}, \
                                 event={event}, level={level:?}"
                            );
                        }
                        let last_byte = *bits.last().assured("the fixture has timestamp rows");
                        assert_eq!(last_byte >> 7, 0);
                    }
                }
            }
        }
    }

    #[test]
    fn empty_column_has_no_bits() {
        let kernel = AdmissionKernel::new(0, 0, NonZeroU64::MIN, 0);
        assert!(kernel.admit(&[]).is_empty());
    }

    /// A generated instant: anywhere on the timeline, or beside one of its ends.
    #[derive(Debug, bolero::TypeGenerator)]
    enum Instant {
        Any(i64),
        AfterMin(u16),
        BeforeMax(u16),
        AroundZero(i16),
    }

    impl Instant {
        fn value(&self) -> i64 {
            match self {
                Self::Any(value) => *value,
                Self::AfterMin(distance) => i64::MIN + i64::from(*distance),
                Self::BeforeMax(distance) => i64::MAX - i64::from(*distance),
                Self::AroundZero(offset) => i64::from(*offset),
            }
        }
    }

    /// A generated period, from one nanosecond to the whole unsigned range: every reduction the
    /// kernel prepares.
    #[derive(Debug, bolero::TypeGenerator)]
    enum Period {
        Any(u64),
        Small(u8),
        PowerOfTwo(u8),
        WholeSeconds(u16),
    }

    impl Period {
        fn value(&self) -> NonZeroU64 {
            let period = match self {
                Self::Any(period) => *period,
                Self::Small(period) => u64::from(*period % 16),
                Self::PowerOfTwo(exponent) => 1_u64 << (exponent % 64),
                Self::WholeSeconds(seconds) => u64::from(*seconds) * 1_000_000_000,
            };
            NonZeroU64::new(period).unwrap_or(NonZeroU64::MIN)
        }
    }

    /// A generated skew: none, a few nanoseconds, any amount, or one relative to the period.
    #[derive(Debug, bolero::TypeGenerator)]
    enum Skew {
        Any(u64),
        Small(u8),
        PeriodFraction { numerator: u8, denominator: u8 },
    }

    impl Skew {
        fn value(&self, period: NonZeroU64) -> u64 {
            match self {
                Self::Any(skew) => *skew,
                Self::Small(skew) => u64::from(*skew % 8),
                Self::PeriodFraction {
                    numerator,
                    denominator,
                } => {
                    let denominator = u128::from(*denominator % 8) + 1;
                    let scaled = u128::from(period.get()) * u128::from(*numerator % 9);
                    u64::try_from(scaled / denominator).unwrap_or(u64::MAX)
                }
            }
        }
    }

    /// A generated timestamp: anywhere, or beside an edge center or an interior center.
    #[derive(Debug, bolero::TypeGenerator)]
    enum Event {
        Any(i64),
        NearFirst(i16),
        NearLast(i16),
        NearCenter { periods: u16, offset: i16 },
    }

    impl Event {
        /// The timestamp of the event relative to the first and last reached centers.
        fn instant(&self, first: i64, last: i64, period: NonZeroU64) -> i64 {
            // Saturation is the meaning here: an instant beside an end of the timeline is clamped
            // to that end, as any instant a column holds is.
            match self {
                Self::Any(value) => *value,
                Self::NearFirst(offset) => first.saturating_add(i64::from(*offset)),
                Self::NearLast(offset) => last.saturating_add(i64::from(*offset)),
                Self::NearCenter { periods, offset } => {
                    let distance = i128::from(period.get()) * i128::from(*periods);
                    let center = i128::from(first) + distance + i128::from(*offset);
                    let clamped = center.clamp(i128::from(i64::MIN), i128::from(i64::MAX));
                    i64::try_from(clamped).assured("the clamp keeps the instant in i64")
                }
            }
        }
    }

    #[derive(Debug, bolero::TypeGenerator)]
    struct AdmissionCase {
        first: Instant,
        last: Instant,
        period: Period,
        skew: Skew,
        #[generator(bolero::generator::produce_with::<Vec<Event>>().len(0_usize..=200))]
        events: Vec<Event>,
    }

    impl AdmissionCase {
        fn events(&self, first: i64, last: i64, period: NonZeroU64) -> Vec<i64> {
            self.events
                .iter()
                .map(|event| event.instant(first, last, period))
                .collect()
        }
    }

    #[test]
    fn bolero_admission_matches_the_scalar_definition_at_every_level() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(2048)
            .with_type::<AdmissionCase>()
            .for_each(|case| {
                let ends = [case.first.value(), case.last.value()];
                let first = ends[0].min(ends[1]);
                let last = ends[0].max(ends[1]);
                let period = case.period.value();
                let skew = case.skew.value(period);
                let events = case.events(first, last, period);
                let mut expected = vec![0_u8; events.len().div_ceil(8)];
                for (row, event) in events.iter().enumerate() {
                    if scalar(first, last, period.get(), skew, *event) {
                        expected[row / 8] |= 1_u8 << (row % 8);
                    }
                }
                let kernel = AdmissionKernel::new(first, last, period, skew);
                for level in supported_levels() {
                    assert_eq!(
                        kernel.with_level(level, &events),
                        expected,
                        "level={level:?} first={first} last={last} period={period} skew={skew}"
                    );
                }
            });
    }
}
