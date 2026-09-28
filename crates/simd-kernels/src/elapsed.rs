//! Elapsed time from a column of instants to one reference instant, folded into log-linear buckets.
//!
//! An instant and the reference are signed nanoseconds on one timeline. An instant at or before
//! the reference has an elapsed time; an instant after it has none and takes part only in the
//! column's latest instant.

use std::num::NonZeroU64;

use arch_into::ArchInto as _;
use error_stack::Report;
use fearless_simd::{Level, dispatch, prelude::*};
use thiserror::Error;

use crate::LEVEL;

/// The most significant figures a layout resolves, the limit HdrHistogram accepts.
const MAX_SIGNIFICANT_FIGURES: u8 = 5;
/// Every non-negative integer below this bound is exact in an `f64`. The lanes divide a doubled
/// elapsed time in `f64`, so a layout keeps that numerator below it.
const EXACT_F64_INTEGERS: u64 = 1 << f64::MANTISSA_DIGITS;

/// How elapsed nanoseconds become recorded units and log-linear buckets.
///
/// An elapsed time is rounded to the nearest whole unit, halves rounding up, and a time beyond the
/// highest unit records as the highest unit. Units below `2^magnitude` each have a bucket of their
/// own. Above that, every power of two is split into `2^(magnitude - 1)` equal buckets, where
/// `magnitude` is the smallest one giving unit resolution through `2 × 10^significant_figures`.
/// This is the layout of an HdrHistogram whose lowest discernible value is one unit: a bucket's
/// index comes from the leading zeros of its units exactly as HdrHistogram computes it, so
/// recording a bucket's lowest units there lands in the same count as recording any of its units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElapsedLayout {
    unit_nanos: u64,
    highest_units: u64,
    /// `highest_units` whole units: every longer elapsed time records as the highest unit.
    highest_nanos: u64,
    /// Log2 of the number of single-unit buckets below the first doubling.
    sub_bucket_magnitude: u32,
}

/// An [`ElapsedLayout`] was asked for a range it cannot bucket exactly.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ElapsedLayoutError {
    #[error("an elapsed-time layout needs a highest unit of at least 2, found {highest_units}")]
    HighestUnitsBelowTwo { highest_units: u64 },
    #[error(
        "an elapsed-time layout resolves at most {MAX_SIGNIFICANT_FIGURES} significant figures, \
         found {significant_figures}"
    )]
    SignificantFigures { significant_figures: u8 },
    #[error(
        "{highest_units} units of {unit_nanos} ns are too long to round exactly in 64-bit float \
         lanes"
    )]
    InexactRange { unit_nanos: u64, highest_units: u64 },
}

/// The samples of one bucket: every elapsed time that recorded as `lowest_units` through the
/// bucket's last unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElapsedBucket {
    pub lowest_units: u64,
    pub count: u64,
}

/// The elapsed times of one column of instants against one reference instant.
///
/// It is built by one pass over the column. The latest instant and each lane's rounded, clamped
/// units are computed in SIMD lanes at the level the process selected; every lane with an elapsed
/// time is then counted into its bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElapsedHistogram {
    layout: ElapsedLayout,
    latest: Option<i64>,
    samples: Option<ElapsedSamples>,
}

/// The counted buckets of a column in which at least one instant had an elapsed time.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ElapsedSamples {
    /// One count per bucket index of the layout, so a sample is counted without a lookup.
    counts: Vec<u64>,
    first_index: usize,
    last_index: usize,
    total: u64,
}

/// What one pass over the lanes leaves behind before it becomes an [`ElapsedHistogram`].
struct LaneFold {
    latest: i64,
    counts: Vec<u64>,
    counted: Option<CountedRange>,
}

/// The buckets a fold counted into and how many samples it counted.
#[derive(Clone, Copy)]
struct CountedRange {
    first_index: usize,
    last_index: usize,
    total: u64,
}

/// The lane-wide operands every chunk of one column shares.
struct ElapsedLanes<S: Simd> {
    now: S::i64s,
    highest_nanos: S::u64s,
    unit: S::u64s,
    doubled_unit: S::f64s,
}

impl ElapsedLayout {
    /// A layout recording elapsed time in units of `unit_nanos` nanoseconds up to `highest_units`,
    /// resolving `significant_figures` decimal digits as HdrHistogram does.
    pub fn new(
        unit_nanos: NonZeroU64,
        highest_units: u64,
        significant_figures: u8,
    ) -> error_stack::Result<Self, ElapsedLayoutError> {
        if highest_units < 2 {
            return Err(Report::new(ElapsedLayoutError::HighestUnitsBelowTwo {
                highest_units,
            }));
        }
        if significant_figures > MAX_SIGNIFICANT_FIGURES {
            return Err(Report::new(ElapsedLayoutError::SignificantFigures {
                significant_figures,
            }));
        }
        let unit_nanos = unit_nanos.get();
        let inexact = Report::new(ElapsedLayoutError::InexactRange {
            unit_nanos,
            highest_units,
        });
        // The lanes divide `2 × elapsed + unit` by `2 × unit` in `f64`. With the elapsed time
        // clamped to the highest unit, that numerator is at most `(2 × highest + 1) × unit`.
        let Some(largest_numerator) = highest_units
            .checked_mul(2)
            .and_then(|doubled| doubled.checked_add(1))
            .and_then(|numerator_units| numerator_units.checked_mul(unit_nanos))
        else {
            return Err(inexact);
        };
        if largest_numerator >= EXACT_F64_INTEGERS {
            return Err(inexact);
        }
        let highest_nanos = highest_units * unit_nanos;
        let single_unit_resolution = 2 * 10_u64.pow(u32::from(significant_figures));
        let sub_bucket_magnitude = single_unit_resolution.next_power_of_two().trailing_zeros();
        Ok(Self {
            unit_nanos,
            highest_units,
            highest_nanos,
            sub_bucket_magnitude,
        })
    }

    /// The bucket index of `units`, which is at most the highest unit.
    ///
    /// The bucket is the number of doublings `units` lies above the single-unit range, read from
    /// its leading zeros; the sub-bucket is `units` with that many low bits shifted off.
    fn index_of(&self, units: u64) -> usize {
        let sub_bucket_mask = (1_u64 << self.sub_bucket_magnitude) - 1;
        let leading_zero_count_base = u64::BITS - self.sub_bucket_magnitude;
        let bucket = leading_zero_count_base - (units | sub_bucket_mask).leading_zeros();
        let sub_bucket = units >> bucket;
        let half_magnitude = self.sub_bucket_magnitude - 1;
        let bucket_base = u64::from(bucket + 1) << half_magnitude;
        // The first bucket also indexes the lower half that every later bucket leaves unused, so
        // its sub-bucket may be below the half count the base is offset by: add before subtracting.
        (bucket_base + sub_bucket - (1_u64 << half_magnitude)).arch_into()
    }

    /// The lowest units that record into bucket `index`, which [`ElapsedLayout::index_of`] made.
    fn lowest_units(&self, index: usize) -> u64 {
        let index: u64 = index.arch_into();
        let half_magnitude = self.sub_bucket_magnitude - 1;
        let half_count = 1_u64 << half_magnitude;
        let offset = index & (half_count - 1);
        match (index >> half_magnitude).checked_sub(1) {
            Some(bucket) => (offset + half_count) << bucket,
            None => offset,
        }
    }

    /// The number of bucket indices through the highest unit.
    fn bucket_count(&self) -> usize {
        self.index_of(self.highest_units) + 1
    }
}

impl ElapsedHistogram {
    /// Folds the elapsed time from each of `instants` to `now`.
    ///
    /// Runtime CPU detection is cached once for the process.
    pub fn new(layout: &ElapsedLayout, now: i64, instants: &[i64]) -> Self {
        let level = *LEVEL.get_or_init(Level::new);
        Self::with_level(level, layout, now, instants)
    }

    fn with_level(level: Level, layout: &ElapsedLayout, now: i64, instants: &[i64]) -> Self {
        let Some((&first, _)) = instants.split_first() else {
            return Self {
                layout: *layout,
                latest: None,
                samples: None,
            };
        };
        let fold = dispatch!(level, simd => Self::fold_lanes(simd, layout, now, first, instants));
        let samples = match fold.counted {
            Some(counted) => Some(ElapsedSamples {
                counts: fold.counts,
                first_index: counted.first_index,
                last_index: counted.last_index,
                total: counted.total,
            }),
            None => None,
        };
        Self {
            layout: *layout,
            latest: Some(fold.latest),
            samples,
        }
    }

    /// One pass over `instants`, whose first element is `first`: the latest instant and every
    /// lane's units in SIMD lanes, then a count per lane with an elapsed time.
    #[inline(always)]
    fn fold_lanes<S: Simd>(
        simd: S,
        layout: &ElapsedLayout,
        now: i64,
        first: i64,
        instants: &[i64],
    ) -> LaneFold {
        simd.vectorize(
            #[inline(always)]
            || {
                let width = S::i64s::LEN;
                let operands = ElapsedLanes::<S> {
                    now: S::i64s::splat(simd, now),
                    highest_nanos: S::u64s::splat(simd, layout.highest_nanos),
                    unit: S::u64s::splat(simd, layout.unit_nanos),
                    doubled_unit: S::u64s::splat(simd, 2 * layout.unit_nanos).to_float(),
                };
                let mut latest = S::i64s::splat(simd, first);
                let mut fold = LaneFold {
                    latest: first,
                    counts: vec![0; layout.bucket_count()],
                    counted: None,
                };
                let mut chunks = instants.chunks_exact(width);
                for chunk in chunks.by_ref() {
                    let lanes = S::i64s::from_slice(simd, chunk);
                    latest = latest.max(lanes);
                    Self::count_lanes(layout, &operands, lanes, width, &mut fold);
                }
                let tail = chunks.remainder();
                if !tail.is_empty() {
                    // The padding repeats the column's first instant, so it cannot raise the
                    // latest instant, and it lies beyond the counted lanes, so it is never counted.
                    let mut lanes = S::i64s::splat(simd, first);
                    lanes.as_mut_slice()[..tail.len()].copy_from_slice(tail);
                    latest = latest.max(lanes);
                    Self::count_lanes(layout, &operands, lanes, tail.len(), &mut fold);
                }
                fold.latest = latest.reduce_max();
                fold
            },
        )
    }

    /// Counts each of the first `counted_lanes` lanes that has an elapsed time into its bucket.
    #[inline(always)]
    fn count_lanes<S: Simd>(
        layout: &ElapsedLayout,
        operands: &ElapsedLanes<S>,
        lanes: S::i64s,
        counted_lanes: usize,
        fold: &mut LaneFold,
    ) {
        let at_or_before_now = lanes.simd_le(operands.now).to_bitmask();
        // The difference is taken modulo 2^64. For a lane at or before `now` that is the exact
        // distance, which fits in `u64` across the whole `i64` range.
        let elapsed: S::u64s = (operands.now - lanes).bitcast();
        let clamped = elapsed.min(operands.highest_nanos);
        // `ElapsedLayout::new` keeps `2 × clamped + unit` below 2^53, so it converts to `f64`
        // exactly. A correctly rounded quotient of two exact integers never crosses the integer
        // its true value lies below, so the floor is the exact nearest unit, halves rounding up.
        let doubled = clamped + clamped + operands.unit;
        let quotient = doubled.to_float::<S::f64s>() / operands.doubled_unit;
        let units: S::u64s = quotient.floor().to_int();
        let units = units.to_array();
        for (lane, units) in units.as_ref().iter().enumerate().take(counted_lanes) {
            if at_or_before_now & (1_u64 << lane) == 0 {
                continue;
            }
            let index = layout.index_of(*units);
            fold.counts[index] += 1;
            fold.counted = Some(match fold.counted {
                Some(counted) => CountedRange {
                    first_index: counted.first_index.min(index),
                    last_index: counted.last_index.max(index),
                    total: counted.total + 1,
                },
                None => CountedRange {
                    first_index: index,
                    last_index: index,
                    total: 1,
                },
            });
        }
    }

    /// The latest instant of the column, whether or not it had an elapsed time, or `None` for an
    /// empty column.
    pub fn latest(&self) -> Option<i64> {
        self.latest
    }

    /// How many instants had an elapsed time.
    pub fn total(&self) -> u64 {
        match &self.samples {
            Some(samples) => samples.total,
            None => 0,
        }
    }

    /// Every bucket that counted a sample, from the shortest elapsed time to the longest.
    pub fn buckets(&self) -> impl Iterator<Item = ElapsedBucket> + '_ {
        let (first_index, counts) = match &self.samples {
            Some(samples) => (
                samples.first_index,
                &samples.counts[samples.first_index..=samples.last_index],
            ),
            None => (0, &[][..]),
        };
        counts
            .iter()
            .enumerate()
            .filter(|(_, count)| **count != 0)
            .map(move |(offset, count)| ElapsedBucket {
                lowest_units: self.layout.lowest_units(first_index + offset),
                count: *count,
            })
    }
}

/// The latest of `instants`, or `None` when there are none.
///
/// Runtime CPU detection is cached once for the process.
pub fn latest_instant(instants: &[i64]) -> Option<i64> {
    let level = *LEVEL.get_or_init(Level::new);
    latest_instant_with_level(level, instants)
}

fn latest_instant_with_level(level: Level, instants: &[i64]) -> Option<i64> {
    let (&first, _) = instants.split_first()?;
    Some(dispatch!(level, simd => latest_lanes(simd, first, instants)))
}

#[inline(always)]
fn latest_lanes<S: Simd>(simd: S, first: i64, instants: &[i64]) -> i64 {
    simd.vectorize(
        #[inline(always)]
        || {
            let width = S::i64s::LEN;
            let mut latest = S::i64s::splat(simd, first);
            let mut chunks = instants.chunks_exact(width);
            for chunk in chunks.by_ref() {
                latest = latest.max(S::i64s::from_slice(simd, chunk));
            }
            let mut latest = latest.reduce_max();
            for instant in chunks.remainder() {
                latest = latest.max(*instant);
            }
            latest
        },
    )
}

/// The elapsed nanoseconds from each of `instants` that is at or before `now`, in column order.
///
/// This is the scalar statement of what [`ElapsedHistogram`] folds, for a consumer that needs every
/// sample rather than its bucket.
pub fn elapsed_nanos(now: i64, instants: &[i64]) -> impl Iterator<Item = u64> + '_ {
    instants
        .iter()
        .filter(move |instant| **instant <= now)
        .map(move |instant| now.abs_diff(*instant))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, num::NonZeroU64};

    use hdrhistogram::Histogram;
    use meticulous::ResultExt as _;

    use super::{
        ElapsedBucket, ElapsedHistogram, ElapsedLayout, ElapsedLayoutError, elapsed_nanos,
        latest_instant_with_level,
    };
    use crate::supported_levels;

    const MILLISECOND: NonZeroU64 = match NonZeroU64::new(1_000_000) {
        Some(unit) => unit,
        None => panic!("one million is not zero"),
    };

    fn millisecond_layout() -> ElapsedLayout {
        ElapsedLayout::new(MILLISECOND, 30_000, 2).assured("30 s of milliseconds rounds exactly")
    }

    /// The units an elapsed time records as, by integer arithmetic rather than the lanes' `f64`.
    fn reference_units(layout: &ElapsedLayout, elapsed_nanos: u64) -> u64 {
        let clamped = u128::from(elapsed_nanos.min(layout.highest_nanos));
        let unit = u128::from(layout.unit_nanos);
        let units = (2 * clamped + unit) / (2 * unit);
        u64::try_from(units).assured("the clamp keeps the units at or below the highest unit")
    }

    struct ReferenceFold {
        latest: Option<i64>,
        buckets: Vec<ElapsedBucket>,
        total: u64,
    }

    fn reference_fold(layout: &ElapsedLayout, now: i64, instants: &[i64]) -> ReferenceFold {
        let mut counts = BTreeMap::<u64, u64>::new();
        let mut total = 0;
        for &instant in instants {
            if instant > now {
                continue;
            }
            let units = reference_units(layout, now.abs_diff(instant));
            let lowest_units = layout.lowest_units(layout.index_of(units));
            *counts.entry(lowest_units).or_default() += 1;
            total += 1;
        }
        let buckets = counts
            .into_iter()
            .map(|(lowest_units, count)| ElapsedBucket {
                lowest_units,
                count,
            })
            .collect();
        ReferenceFold {
            latest: instants.iter().copied().max(),
            buckets,
            total,
        }
    }

    #[test]
    fn layout_buckets_units_as_hdrhistogram_does() {
        for significant_figures in 0..=3 {
            for highest_units in [2, 3, 255, 256, 1_000, 30_000, 65_536] {
                let layout =
                    ElapsedLayout::new(NonZeroU64::MIN, highest_units, significant_figures)
                        .assured("a nanosecond unit keeps these ranges exact");
                let hdr = Histogram::<u64>::new_with_max(highest_units, significant_figures)
                    .assured("these bounds are valid HdrHistogram bounds");
                let mut previous = None;
                for units in 0..=highest_units {
                    let index = layout.index_of(units);
                    assert!(index < layout.bucket_count());
                    assert_eq!(
                        layout.lowest_units(index),
                        hdr.lowest_equivalent(units),
                        "sigfig={significant_figures} highest={highest_units} units={units}"
                    );
                    if let Some((previous_units, previous_index)) = previous {
                        let same_bucket =
                            hdr.lowest_equivalent(previous_units) == hdr.lowest_equivalent(units);
                        let expected = if same_bucket {
                            previous_index
                        } else {
                            previous_index + 1
                        };
                        assert_eq!(index, expected, "units={units}");
                    }
                    previous = Some((units, index));
                }
            }
        }
    }

    #[test]
    fn elapsed_histogram_matches_the_scalar_reference_at_every_supported_level() {
        let layout = millisecond_layout();
        let mut rng = fastrand::Rng::with_seed(0x8503);
        let half = i64::try_from(layout.unit_nanos / 2).assured("half a millisecond fits i64");
        let clamp = i64::try_from(layout.highest_nanos).assured("30 s of nanoseconds fits i64");
        for now in [0_i64, 1_790_000_000_000_000_000, i64::MIN, i64::MAX, -7] {
            for length in (0..=70).chain([255, 1_024]) {
                let instants = (0..length)
                    .map(|_| match rng.u8(..8) {
                        0 => now.saturating_sub(rng.i64(0..=clamp + 5 * half)),
                        1 => now.saturating_sub(rng.i64(0..=64) * half + rng.i64(-1..=1)),
                        2 => now.saturating_add(rng.i64(1..=clamp)),
                        3 => i64::MIN,
                        4 => i64::MAX,
                        5 => now,
                        _ => rng.i64(..),
                    })
                    .collect::<Vec<_>>();
                let expected = reference_fold(&layout, now, &instants);
                for level in supported_levels() {
                    let folded = ElapsedHistogram::with_level(level, &layout, now, &instants);
                    let context = format!("level={level:?} now={now} length={length}");
                    assert_eq!(folded.latest(), expected.latest, "{context}");
                    assert_eq!(folded.total(), expected.total, "{context}");
                    assert_eq!(
                        folded.buckets().collect::<Vec<_>>(),
                        expected.buckets,
                        "{context}"
                    );
                }
            }
        }
    }

    #[test]
    fn units_round_to_the_nearest_unit_with_halves_up_and_clamp_at_the_highest() {
        let layout = millisecond_layout();
        let instants = [
            -499_999,
            -500_000,
            -1_499_999,
            -1_500_000,
            -29_999_499_999,
            -30_000_000_000,
            -30_000_500_000,
            i64::MIN,
            1,
        ];
        for level in supported_levels() {
            let folded = ElapsedHistogram::with_level(level, &layout, 0, &instants);
            assert_eq!(folded.latest(), Some(1));
            assert_eq!(folded.total(), 8);
            assert_eq!(
                folded.buckets().collect::<Vec<_>>(),
                [
                    ElapsedBucket {
                        lowest_units: 0,
                        count: 1,
                    },
                    ElapsedBucket {
                        lowest_units: 1,
                        count: 2,
                    },
                    ElapsedBucket {
                        lowest_units: 2,
                        count: 1,
                    },
                    ElapsedBucket {
                        lowest_units: 29_952,
                        count: 4,
                    },
                ],
                "level={level:?}"
            );
        }
    }

    #[test]
    fn instants_after_the_reference_count_only_toward_the_latest_instant() {
        let layout = millisecond_layout();
        for level in supported_levels() {
            let folded = ElapsedHistogram::with_level(level, &layout, 10, &[11, 12, 20]);
            assert_eq!(folded.latest(), Some(20));
            assert_eq!(folded.total(), 0);
            assert_eq!(folded.buckets().count(), 0);

            let empty = ElapsedHistogram::with_level(level, &layout, 10, &[]);
            assert_eq!(empty.latest(), None);
            assert_eq!(empty.total(), 0);
        }
    }

    #[test]
    fn latest_instant_matches_the_maximum_at_every_supported_level() {
        let mut rng = fastrand::Rng::with_seed(0x8504);
        for length in 0..=70 {
            let instants = (0..length).map(|_| rng.i64(..)).collect::<Vec<_>>();
            for level in supported_levels() {
                assert_eq!(
                    latest_instant_with_level(level, &instants),
                    instants.iter().copied().max(),
                    "level={level:?} length={length}"
                );
            }
        }
        for level in supported_levels() {
            assert_eq!(
                latest_instant_with_level(level, &[i64::MIN, i64::MIN]),
                Some(i64::MIN)
            );
        }
    }

    #[test]
    fn elapsed_nanos_spans_the_whole_instant_range_and_skips_later_instants() {
        let samples = elapsed_nanos(i64::MAX, &[i64::MIN, i64::MAX, 0]).collect::<Vec<_>>();
        assert_eq!(samples, [u64::MAX, 0, i64::MAX.unsigned_abs()]);
        assert_eq!(elapsed_nanos(0, &[1, 2]).count(), 0);
    }

    #[test]
    fn layouts_refuse_ranges_they_cannot_bucket_exactly() {
        let Err(too_low) = ElapsedLayout::new(MILLISECOND, 1, 2) else {
            panic!("a highest unit of one is below two");
        };
        assert_eq!(
            too_low.current_context(),
            &ElapsedLayoutError::HighestUnitsBelowTwo { highest_units: 1 }
        );
        let Err(too_precise) = ElapsedLayout::new(MILLISECOND, 30_000, 6) else {
            panic!("six significant figures exceed five");
        };
        assert_eq!(
            too_precise.current_context(),
            &ElapsedLayoutError::SignificantFigures {
                significant_figures: 6,
            }
        );
        let Err(too_long) = ElapsedLayout::new(MILLISECOND, 1 << 33, 2) else {
            panic!("twice 2^33 milliseconds exceed the exact 2^53 nanosecond range");
        };
        assert_eq!(
            too_long.current_context(),
            &ElapsedLayoutError::InexactRange {
                unit_nanos: 1_000_000,
                highest_units: 1 << 33,
            }
        );
        let Err(overflowing) = ElapsedLayout::new(NonZeroU64::MAX, u64::MAX, 2) else {
            panic!("the largest numerator does not fit u64");
        };
        assert_eq!(
            overflowing.current_context(),
            &ElapsedLayoutError::InexactRange {
                unit_nanos: u64::MAX,
                highest_units: u64::MAX,
            }
        );
    }
}
