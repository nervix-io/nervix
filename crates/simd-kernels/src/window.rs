//! Typed, validity-aware folds over a consecutive run of window argument values.
//!
//! Layer: primitives.
//!
//! - **Owns.** Bitmap counting, exact integer folds, floating-point run statistics, finite-value
//!   classification, and fixed-range bucket indexes over plain typed slices.
//! - **Depends on.** Portable SIMD selection and primitive numeric types.
//! - **Must not know.** Arrow, window plans, branches, or aggregate output types.

use fearless_simd::{Level, dispatch, prelude::*};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};

use crate::LEVEL;

/// The validity bitmap of a typed slice. `offset` is the first value's bit in `bits`.
#[derive(Debug, Clone, Copy)]
pub struct RunValidity<'a> {
    bits: Option<&'a [u8]>,
    offset: usize,
}

impl<'a> RunValidity<'a> {
    /// An optional Arrow-style validity bitmap and the first value's bit offset.
    pub fn new(bits: Option<&'a [u8]>, offset: usize) -> Self {
        Self { bits, offset }
    }

    /// Whether one value contributes.
    #[inline]
    pub fn contains(self, index: usize) -> bool {
        let Some(bits) = self.bits else {
            return true;
        };
        let position = self.offset + index;
        bits[position / 8] & (1 << (position % 8)) != 0
    }

    /// The number of present values in a run, counted by bitmap words.
    pub fn count(self, len: usize) -> u64 {
        if self.bits.is_none() {
            return u64::try_from(len).assured("a slice cannot hold 2^64 values");
        }
        let mut count = 0_u64;
        for start in (0..len).step_by(64) {
            count += u64::from(self.word(start, (len - start).min(64)).count_ones());
        }
        count
    }

    /// Up to 64 consecutive validity bits, low bit first.
    fn word(self, start: usize, len: usize) -> u64 {
        let Some(bits) = self.bits else {
            return if len == 64 {
                u64::MAX
            } else {
                (1_u64 << len) - 1
            };
        };
        bitmap_word(bits, self.offset + start, len)
    }
}

/// Read up to 64 consecutive bits without assuming byte alignment.
fn bitmap_word(bits: &[u8], start: usize, len: usize) -> u64 {
    let byte = start / 8;
    let shift = start % 8;
    let mut lower = 0_u64;
    for (index, value) in bits[byte..].iter().take(8).enumerate() {
        lower |= u64::from(*value) << (index * 8);
    }
    let mut word = lower >> shift;
    if shift != 0
        && let Some(upper) = bits.get(byte + 8)
    {
        word |= u64::from(*upper) << (64 - shift);
    }
    if len < 64 {
        word &= (1_u64 << len) - 1;
    }
    word
}

/// Count true and false present values of a packed boolean run.
pub fn count_booleans(
    values: &[u8],
    values_offset: usize,
    validity: RunValidity<'_>,
    len: usize,
) -> (u64, u64) {
    let mut true_rows = 0_u64;
    let mut false_rows = 0_u64;
    for start in (0..len).step_by(64) {
        let width = (len - start).min(64);
        let present = validity.word(start, width);
        let trues = bitmap_word(values, values_offset + start, width) & present;
        true_rows += u64::from(trues.count_ones());
        false_rows += u64::from((present & !trues).count_ones());
    }
    (true_rows, false_rows)
}

/// An exact signed 64-bit run sum, with the low and signed high 32-bit halves accumulated in
/// separate SIMD lanes. A lane is reduced before its halves can overflow, then widened to i128.
pub fn sum_i64(values: &[i64], validity: RunValidity<'_>) -> (u64, i128) {
    let level = *LEVEL.get_or_init(Level::new);
    dispatch!(level, simd => sum_i64_lanes(simd, values, validity))
}

#[inline(always)]
fn sum_i64_lanes<S: Simd>(simd: S, values: &[i64], validity: RunValidity<'_>) -> (u64, i128) {
    simd.vectorize(
        #[inline(always)]
        || {
            let width = S::i64s::LEN;
            let mut low = S::i64s::splat(simd, 0);
            let mut high = S::i64s::splat(simd, 0);
            let mask = S::i64s::splat(simd, i64::from(u32::MAX));
            let mut total = 0_i128;
            let mut blocks = 0_usize;
            for (block, chunk) in values.chunks(width).enumerate() {
                let mut lanes = S::i64s::splat(simd, 0);
                let start = block * width;
                for (lane, &value) in chunk.iter().enumerate() {
                    if validity.contains(start + lane) {
                        lanes.as_mut_slice()[lane] = value;
                    }
                }
                low += lanes & mask;
                high += lanes >> 32;
                blocks += 1;
                // Every half-lane stays within 2^52 over this many blocks.
                if blocks == 1 << 20 {
                    total += reduce_halves(low.as_slice(), high.as_slice());
                    low = S::i64s::splat(simd, 0);
                    high = S::i64s::splat(simd, 0);
                    blocks = 0;
                }
            }
            total += reduce_halves(low.as_slice(), high.as_slice());
            (validity.count(values.len()), total)
        },
    )
}

fn reduce_halves(low: &[i64], high: &[i64]) -> i128 {
    low.iter().zip(high).fold(0_i128, |total, (&lo, &hi)| {
        total + (i128::from(hi) << 32) + i128::from(lo)
    })
}

/// An exact unsigned 64-bit run sum, split into high and low 32-bit SIMD lanes.
pub fn sum_u64(values: &[u64], validity: RunValidity<'_>) -> (u64, i128) {
    let level = *LEVEL.get_or_init(Level::new);
    dispatch!(level, simd => sum_u64_lanes(simd, values, validity))
}

#[inline(always)]
fn sum_u64_lanes<S: Simd>(simd: S, values: &[u64], validity: RunValidity<'_>) -> (u64, i128) {
    simd.vectorize(
        #[inline(always)]
        || {
            let width = S::u64s::LEN;
            let mut low = S::u64s::splat(simd, 0);
            let mut high = S::u64s::splat(simd, 0);
            let mask = S::u64s::splat(simd, u64::from(u32::MAX));
            let mut total = 0_i128;
            let mut blocks = 0_usize;
            for (block, chunk) in values.chunks(width).enumerate() {
                let mut lanes = S::u64s::splat(simd, 0);
                let start = block * width;
                for (lane, &value) in chunk.iter().enumerate() {
                    if validity.contains(start + lane) {
                        lanes.as_mut_slice()[lane] = value;
                    }
                }
                low += lanes & mask;
                high += lanes >> 32;
                blocks += 1;
                if blocks == 1 << 20 {
                    total += reduce_unsigned_halves(low.as_slice(), high.as_slice());
                    low = S::u64s::splat(simd, 0);
                    high = S::u64s::splat(simd, 0);
                    blocks = 0;
                }
            }
            total += reduce_unsigned_halves(low.as_slice(), high.as_slice());
            (validity.count(values.len()), total)
        },
    )
}

fn reduce_unsigned_halves(low: &[u64], high: &[u64]) -> i128 {
    low.iter().zip(high).fold(0_i128, |total, (&lo, &hi)| {
        total + (i128::from(hi) << 32) + i128::from(lo)
    })
}

/// Exact fold of a narrower integer type without a per-value type dispatch.
pub fn sum_integer<T: Copy + Into<i128>>(values: &[T], validity: RunValidity<'_>) -> (u64, i128) {
    let mut sum = 0_i128;
    for (index, value) in values.iter().enumerate() {
        if validity.contains(index) {
            sum += (*value).into();
        }
    }
    (validity.count(values.len()), sum)
}

/// The first masked minimum and maximum of a typed run, preserving the earliest tie.
pub fn min_max<T: Copy + PartialOrd>(
    values: &[T],
    validity: RunValidity<'_>,
) -> Option<((usize, T), (usize, T))> {
    let mut smallest: Option<(usize, T)> = None;
    let mut largest: Option<(usize, T)> = None;
    for (index, &value) in values.iter().enumerate() {
        if !validity.contains(index) {
            continue;
        }
        if smallest.is_none_or(|(_, current)| value < current) {
            smallest = Some((index, value));
        }
        if largest.is_none_or(|(_, current)| value > current) {
            largest = Some((index, value));
        }
    }
    smallest.zip(largest)
}

/// One bit per present, non-finite f64 value, low bit first.
pub fn non_finite_f64(values: &[f64], validity: RunValidity<'_>) -> Vec<u8> {
    let level = *LEVEL.get_or_init(Level::new);
    dispatch!(level, simd => non_finite_f64_lanes(simd, values, validity))
}

/// One bit per present, non-finite f32 value, low bit first.
pub fn non_finite_f32(values: &[f32], validity: RunValidity<'_>) -> Vec<u8> {
    let level = *LEVEL.get_or_init(Level::new);
    dispatch!(level, simd => non_finite_f32_lanes(simd, values, validity))
}

#[inline(always)]
fn non_finite_f64_lanes<S: Simd>(simd: S, values: &[f64], validity: RunValidity<'_>) -> Vec<u8> {
    simd.vectorize(
        #[inline(always)]
        || {
            let mut bits = vec![0_u8; values.len().div_ceil(8)];
            let width = S::f64s::LEN;
            let finite_limit = S::f64s::splat(simd, f64::MAX);
            let mut start = 0;
            while start + width <= values.len() {
                let lanes = S::f64s::from_slice(simd, &values[start..start + width]);
                let non_finite =
                    !lanes.abs().simd_le(finite_limit).to_bitmask() & validity.word(start, width);
                mark_bits(&mut bits, start, non_finite);
                start += width;
            }
            for (index, value) in values[start..].iter().enumerate() {
                if validity.contains(start + index) && !value.is_finite() {
                    mark_bits(&mut bits, start + index, 1);
                }
            }
            bits
        },
    )
}

#[inline(always)]
fn non_finite_f32_lanes<S: Simd>(simd: S, values: &[f32], validity: RunValidity<'_>) -> Vec<u8> {
    simd.vectorize(
        #[inline(always)]
        || {
            let mut bits = vec![0_u8; values.len().div_ceil(8)];
            let width = S::f32s::LEN;
            let finite_limit = S::f32s::splat(simd, f32::MAX);
            let mut start = 0;
            while start + width <= values.len() {
                let lanes = S::f32s::from_slice(simd, &values[start..start + width]);
                let non_finite =
                    !lanes.abs().simd_le(finite_limit).to_bitmask() & validity.word(start, width);
                mark_bits(&mut bits, start, non_finite);
                start += width;
            }
            for (index, value) in values[start..].iter().enumerate() {
                if validity.contains(start + index) && !value.is_finite() {
                    mark_bits(&mut bits, start + index, 1);
                }
            }
            bits
        },
    )
}

fn mark_bits(bits: &mut [u8], start: usize, mut mask: u64) {
    while mask != 0 {
        let lane = usize::try_from(mask.trailing_zeros()).assured("a u64 has at most 64 bits");
        let position = start + lane;
        bits[position / 8] |= 1 << (position % 8);
        mask &= mask - 1;
    }
}

#[cfg(test)]
fn scalar_non_finite<T: Copy>(
    values: &[T],
    validity: RunValidity<'_>,
    finite: impl Fn(T) -> bool,
) -> Vec<u8> {
    let mut bits = vec![0_u8; values.len().div_ceil(8)];
    for (index, &value) in values.iter().enumerate() {
        if validity.contains(index) && !finite(value) {
            bits[index / 8] |= 1 << (index % 8);
        }
    }
    bits
}

/// Bucket indexes for present finite values. The scalar caller scatters these indexes into its
/// buckets; the typed run is classified once with no per-row type dispatch.
pub fn bucket_indices<T: Copy>(
    values: &[T],
    validity: RunValidity<'_>,
    convert: impl Fn(T) -> f64,
    min: f64,
    max: f64,
    width: f64,
    buckets: usize,
) -> Vec<Option<usize>> {
    let last = buckets - 1;
    values
        .iter()
        .enumerate()
        .map(|(index, &value)| {
            if !validity.contains(index) {
                return None;
            }
            let value = convert(value);
            let bucket = if value <= min {
                0
            } else if value >= max {
                last
            } else {
                // The value is finite and inside the validated finite range.
                let index = ((value - min) / width).floor();
                index
                    .checked_approx_into::<usize>()
                    .assured("a finite interior bucket fits usize")
                    .min(last)
            };
            Some(bucket)
        })
        .collect()
}

/// The count, rounded sum, and exact addition errors of one typed run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSum {
    pub count: u64,
    pub sum: f64,
    pub compensation: f64,
}

impl RunSum {
    pub const EMPTY: Self = Self {
        count: 0,
        sum: 0.0,
        compensation: 0.0,
    };

    fn add(&mut self, value: f64) {
        let rounded = self.sum + value;
        let right = rounded - self.sum;
        let left = rounded - right;
        self.compensation += (self.sum - left) + (value - right);
        self.sum = rounded;
        self.count += 1;
    }

    fn merge(&mut self, other: Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other;
            return;
        }
        let rounded = self.sum + other.sum;
        let right = rounded - self.sum;
        let left = rounded - right;
        self.compensation += other.compensation + (self.sum - left) + (other.sum - right);
        self.sum = rounded;
        self.count += other.count;
    }
}

/// Per-lane TwoSum folds, merged once for a run. Nulls contribute to no lane.
pub fn compensated_sum<T: Copy>(
    values: &[T],
    validity: RunValidity<'_>,
    convert: impl Fn(T) -> f64,
) -> RunSum {
    let mut lanes = [RunSum::EMPTY; 8];
    for (index, &value) in values.iter().enumerate() {
        if validity.contains(index) {
            lanes[index % lanes.len()].add(convert(value));
        }
    }
    let mut total = RunSum::EMPTY;
    for lane in lanes {
        total.merge(lane);
    }
    total
}

/// Keep the two-pass mean finite when summing finite inputs overflows before division. The
/// weighted fallback is needed only for magnitudes near the floating-point limit.
fn mean_from_sum<T: Copy>(
    values: &[T],
    sum: RunSum,
    convert: impl Fn(T) -> f64,
    contributes: impl Fn(usize) -> bool,
) -> f64 {
    let total = sum.sum + sum.compensation;
    if total.is_finite() {
        return total / sum.count.approx_into::<f64>();
    }
    let mut mean = 0.0;
    let mut count = 0_u64;
    for (index, &value) in values.iter().enumerate() {
        if contributes(index) {
            count += 1;
            let n: f64 = count.approx_into();
            mean = mean * ((n - 1.0) / n) + convert(value) / n;
        }
    }
    debug_assert_eq!(count, sum.count);
    mean
}

/// The centered mean and second moment of one run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunMoments {
    pub count: u64,
    pub mean: f64,
    pub squares: f64,
}

/// Two passes: per-lane compensated sums establish one mean; centered differences accumulate M2.
pub fn moments<T: Copy>(
    values: &[T],
    validity: RunValidity<'_>,
    convert: impl Fn(T) -> f64 + Copy,
) -> RunMoments {
    let sum = compensated_sum(values, validity, convert);
    if sum.count == 0 {
        return RunMoments {
            count: 0,
            mean: 0.0,
            squares: 0.0,
        };
    }
    let mean = mean_from_sum(values, sum, convert, |index| validity.contains(index));
    let mut squares = [0.0_f64; 8];
    for (index, &value) in values.iter().enumerate() {
        if validity.contains(index) {
            let delta = convert(value) - mean;
            squares[index % squares.len()] += delta * delta;
        }
    }
    RunMoments {
        count: sum.count,
        mean,
        squares: squares.into_iter().sum(),
    }
}

/// The centered means, second moments, and cross moment of two typed runs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunCoMoments {
    pub count: u64,
    pub first_mean: f64,
    pub second_mean: f64,
    pub first_squares: f64,
    pub second_squares: f64,
    pub cross_products: f64,
}

/// Two-pass co-moments over rows where both arguments are present.
pub fn co_moments<A: Copy, B: Copy>(
    first: &[A],
    first_validity: RunValidity<'_>,
    first_value: impl Fn(A) -> f64 + Copy,
    second: &[B],
    second_validity: RunValidity<'_>,
    second_value: impl Fn(B) -> f64 + Copy,
) -> RunCoMoments {
    assert_eq!(
        first.len(),
        second.len(),
        "co-moment arguments describe the same run"
    );
    let mut first_lanes = [RunSum::EMPTY; 8];
    let mut second_lanes = [RunSum::EMPTY; 8];
    for index in 0..first.len() {
        if first_validity.contains(index) && second_validity.contains(index) {
            let lane = index % first_lanes.len();
            first_lanes[lane].add(first_value(first[index]));
            second_lanes[lane].add(second_value(second[index]));
        }
    }
    let mut first_sum = RunSum::EMPTY;
    let mut second_sum = RunSum::EMPTY;
    for lane in first_lanes {
        first_sum.merge(lane);
    }
    for lane in second_lanes {
        second_sum.merge(lane);
    }
    if first_sum.count == 0 {
        return RunCoMoments {
            count: 0,
            first_mean: 0.0,
            second_mean: 0.0,
            first_squares: 0.0,
            second_squares: 0.0,
            cross_products: 0.0,
        };
    }
    let paired = |index| first_validity.contains(index) && second_validity.contains(index);
    let first_mean = mean_from_sum(first, first_sum, first_value, paired);
    let second_mean = mean_from_sum(second, second_sum, second_value, paired);
    let mut first_squares = [0.0_f64; 8];
    let mut second_squares = [0.0_f64; 8];
    let mut cross_products = [0.0_f64; 8];
    for index in 0..first.len() {
        if first_validity.contains(index) && second_validity.contains(index) {
            let lane = index % first_squares.len();
            let first_delta = first_value(first[index]) - first_mean;
            let second_delta = second_value(second[index]) - second_mean;
            first_squares[lane] += first_delta * first_delta;
            second_squares[lane] += second_delta * second_delta;
            cross_products[lane] += first_delta * second_delta;
        }
    }
    RunCoMoments {
        count: first_sum.count,
        first_mean,
        second_mean,
        first_squares: first_squares.into_iter().sum(),
        second_squares: second_squares.into_iter().sum(),
        cross_products: cross_products.into_iter().sum(),
    }
}

/// Visit a typed run from newest to oldest for a two-stack refold. The caller owns the mergeable
/// aggregate and records one suffix after each visit; type and validity selection stay run-level.
pub fn reverse_values<T: Copy>(
    values: &[T],
    validity: RunValidity<'_>,
    convert: impl Fn(T) -> f64,
    mut visit: impl FnMut(Option<f64>),
) {
    for index in (0..values.len()).rev() {
        let value = if validity.contains(index) {
            Some(convert(values[index]))
        } else {
            None
        };
        visit(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitmap_counts_handle_sliced_bits_and_nulls() {
        let validity = RunValidity::new(Some(&[0b1010_1100, 0b0000_0011]), 2);
        assert_eq!(validity.count(8), 6);
        let (yes, no) = count_booleans(&[0b1100_1010, 0], 2, validity, 8);
        assert_eq!((yes, no), (2, 4));
    }

    #[test]
    fn exact_i64_halves_handle_extremes_and_unaligned_nulls_at_every_level() {
        let values = [i64::MAX, i64::MIN, -9, 7, i64::MAX, i64::MIN, 3, -1, 11];
        let validity = RunValidity::new(Some(&[0b1111_1011, 0b0000_0111]), 1);
        let expected = values
            .iter()
            .enumerate()
            .filter(|(index, _)| validity.contains(*index))
            .fold((0_u64, 0_i128), |(count, sum), (_, value)| {
                (count + 1, sum + i128::from(*value))
            });
        for level in crate::supported_levels() {
            assert_eq!(sum_i64_lanes_for_level(level, &values, validity), expected);
        }
    }

    fn sum_i64_lanes_for_level(
        level: Level,
        values: &[i64],
        validity: RunValidity<'_>,
    ) -> (u64, i128) {
        dispatch!(level, simd => sum_i64_lanes(simd, values, validity))
    }

    #[test]
    fn two_pass_centering_preserves_small_spread_beside_large_means() {
        let values = [1e16, 1e16 + 2.0, 1e16 + 4.0];
        let run = moments(&values, RunValidity::new(None, 0), |value| value);
        assert_eq!(run.mean, 1e16 + 2.0);
        assert_eq!(run.squares / 3.0, 8.0 / 3.0);
    }

    #[test]
    fn two_pass_means_stay_finite_when_a_finite_run_sum_overflows() {
        let values = [1e308, 1e308, 1e308];
        let valid = RunValidity::new(None, 0);
        let run = moments(&values, valid, |value| value);
        assert_eq!((run.count, run.mean, run.squares), (3, 1e308, 0.0));

        let partners = [2.0, 4.0, 6.0];
        let paired = co_moments(
            &values,
            valid,
            |value| value,
            &partners,
            valid,
            |value| value,
        );
        assert_eq!((paired.count, paired.first_mean), (3, 1e308));
        assert_eq!((paired.second_mean, paired.first_squares), (4.0, 0.0));
    }

    #[test]
    fn bitmap_popcounts_match_scalar_checks_across_unaligned_word_boundaries() {
        let mut rng = fastrand::Rng::with_seed(5_019);
        let validity = (0..29).map(|_| rng.u8(..)).collect::<Vec<_>>();
        let values = (0..29).map(|_| rng.u8(..)).collect::<Vec<_>>();
        for offset in 0..16 {
            for len in [0, 1, 7, 8, 63, 64, 65, 120, 160] {
                let valid = RunValidity::new(Some(&validity), offset);
                let expected = (0..len).fold((0_u64, 0_u64), |(yes, no), index| {
                    if !valid.contains(index) {
                        return (yes, no);
                    }
                    let bit = offset + index;
                    if values[bit / 8] & (1 << (bit % 8)) != 0 {
                        (yes + 1, no)
                    } else {
                        (yes, no + 1)
                    }
                });
                assert_eq!(valid.count(len), expected.0 + expected.1);
                assert_eq!(count_booleans(&values, offset, valid, len), expected);
            }
        }
        assert_eq!(RunValidity::new(None, 0).count(123), 123);
    }

    #[test]
    fn exact_unsigned_halves_and_narrow_integers_keep_null_and_overflow_boundaries() {
        let values = [u64::MAX, 0, u64::MAX, 7, 1, u64::MAX, 9];
        let valid = RunValidity::new(Some(&[0b1101_0101]), 0);
        let expected = values
            .iter()
            .enumerate()
            .filter(|(index, _)| valid.contains(*index))
            .fold((0_u64, 0_i128), |(count, sum), (_, value)| {
                (count + 1, sum + i128::from(*value))
            });
        for level in crate::supported_levels() {
            let actual = dispatch!(level, simd => sum_u64_lanes(simd, &values, valid));
            assert_eq!(actual, expected);
        }
        assert_eq!(
            sum_integer(&[-3_i16, 4, 9], RunValidity::new(Some(&[0b0000_0101]), 0)),
            (2, 6)
        );
        assert_eq!(sum_i64(&[], RunValidity::new(None, 0)), (0, 0));
    }

    #[test]
    fn masked_extremes_keep_first_ties_and_non_finite_bits_ignore_nulls() {
        let valid = RunValidity::new(Some(&[0b0001_1101]), 0);
        assert_eq!(
            min_max(&[3_i64, -9, 1, 1, 3], valid),
            Some(((2, 1), (0, 3)))
        );
        assert_eq!(min_max::<i64>(&[], valid), None);
        assert_eq!(min_max(&[1_i64, 2], RunValidity::new(Some(&[0]), 0)), None);
        let floats = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, 1.0];
        assert_eq!(non_finite_f64(&floats, valid), vec![0b0000_0101]);
        let floats32 = [f32::INFINITY, f32::NAN, 1.0, f32::NEG_INFINITY];
        assert_eq!(
            non_finite_f32(&floats32, RunValidity::new(Some(&[0b0000_1101]), 0)),
            vec![0b0000_1001]
        );
    }

    #[test]
    fn non_finite_simd_masks_match_scalar_classification_at_every_level() {
        let values = (0..129)
            .map(|index| match index % 7 {
                0 => f64::NAN,
                1 => f64::INFINITY,
                2 => f64::NEG_INFINITY,
                _ => f64::from(index),
            })
            .collect::<Vec<_>>();
        let narrow = values
            .iter()
            .map(|value| (*value).approx_into::<f32>())
            .collect::<Vec<_>>();
        let validity_bits = (0..17)
            .map(|index| {
                if index % 2 == 0 {
                    0b1101_1011
                } else {
                    0b0111_0110
                }
            })
            .collect::<Vec<_>>();
        let validity = RunValidity::new(Some(&validity_bits), 3);
        let expected_f64 = scalar_non_finite(&values, validity, |value| value.is_finite());
        let expected_f32 = scalar_non_finite(&narrow, validity, |value| value.is_finite());
        for level in crate::supported_levels() {
            assert_eq!(
                dispatch!(level, simd => non_finite_f64_lanes(simd, &values, validity)),
                expected_f64
            );
            assert_eq!(
                dispatch!(level, simd => non_finite_f32_lanes(simd, &narrow, validity)),
                expected_f32
            );
        }
    }

    #[test]
    fn buckets_clamp_and_skip_nulls_before_scatter() {
        let values = [-10_i64, 0, 1, 2, 3, 4, 100];
        let valid = RunValidity::new(Some(&[0b0111_1101]), 0);
        let buckets = bucket_indices(
            &values,
            valid,
            |value| value.approx_into::<f64>(),
            0.0,
            4.0,
            1.0,
            4,
        );
        assert_eq!(
            buckets,
            vec![Some(0), None, Some(1), Some(2), Some(3), Some(3), Some(3)]
        );
    }

    #[test]
    fn compensated_lanes_and_paired_moments_skip_missing_values() {
        let values = [1e16, 1.0, -1e16, 7.0];
        let valid = RunValidity::new(Some(&[0b0000_0111]), 0);
        let sum = compensated_sum(&values, valid, |value| value);
        assert_eq!(sum.count, 3);
        assert_eq!(sum.sum + sum.compensation, 1.0);
        let first = [2_i64, 4, 6];
        let second = [10_i64, 20, 30];
        let paired = co_moments(
            &first,
            RunValidity::new(None, 0),
            |value| value.approx_into::<f64>(),
            &second,
            RunValidity::new(Some(&[0b0000_0101]), 0),
            |value| value.approx_into::<f64>(),
        );
        assert_eq!(paired.count, 2);
        assert_eq!((paired.first_mean, paired.second_mean), (4.0, 20.0));
        assert_eq!(
            (
                paired.first_squares,
                paired.second_squares,
                paired.cross_products
            ),
            (8.0, 200.0, 40.0)
        );
        let empty = co_moments(
            &first,
            RunValidity::new(Some(&[0]), 0),
            |value| value.approx_into::<f64>(),
            &second,
            RunValidity::new(None, 0),
            |value| value.approx_into::<f64>(),
        );
        assert_eq!(empty.count, 0);
    }

    /// A generated window run: raw value bits read as every typed slice, an optional validity
    /// bitmap and the bit offset of the run's first value in it, and a packed boolean run with its
    /// own offset.
    #[derive(Debug, bolero::TypeGenerator)]
    struct WindowRun {
        #[generator(bolero::generator::produce_with::<Vec<u64>>().len(0_usize..=300))]
        values: Vec<u64>,
        validity: Option<Vec<u8>>,
        validity_offset: u8,
        booleans: Vec<u8>,
        booleans_offset: u8,
    }

    /// `bits` repeated, or a fixed pattern when it is empty, until it holds `needed` bits.
    fn bitmap_of(bits: &[u8], needed: usize) -> Vec<u8> {
        let bytes = needed.div_ceil(8);
        if bits.is_empty() {
            return vec![0b1011_0110; bytes];
        }
        bits.iter().copied().cycle().take(bytes).collect()
    }

    fn bit(bits: &[u8], position: usize) -> bool {
        bits[position / 8] & (1 << (position % 8)) != 0
    }

    /// The exact sum of `values`, as Shewchuk's non-overlapping partials, which add without error.
    fn exact_partials(values: impl IntoIterator<Item = f64>) -> Vec<f64> {
        let mut partials: Vec<f64> = Vec::new();
        for value in values {
            let mut carried = value;
            let mut kept = 0;
            for index in 0..partials.len() {
                let mut other = partials[index];
                if carried.abs() < other.abs() {
                    std::mem::swap(&mut carried, &mut other);
                }
                let high = carried + other;
                let low = other - (high - carried);
                if low != 0.0 {
                    partials[kept] = low;
                    kept += 1;
                }
                carried = high;
            }
            partials.truncate(kept);
            partials.push(carried);
        }
        partials
    }

    /// The value of non-overlapping partials, added from the smallest so it rounds once.
    fn partials_value(partials: &[f64]) -> f64 {
        partials.iter().sum()
    }

    impl WindowRun {
        fn validity_bits(&self) -> Option<Vec<u8>> {
            match &self.validity {
                Some(bits) => {
                    let needed = usize::from(self.validity_offset) + self.values.len();
                    Some(bitmap_of(bits, needed))
                }
                None => None,
            }
        }

        fn present(&self, validity: Option<&[u8]>, index: usize) -> bool {
            match validity {
                Some(bits) => bit(bits, usize::from(self.validity_offset) + index),
                None => true,
            }
        }

        /// A float run whose exponents keep every partial sum far from overflow, so the error of a
        /// compensated sum is bounded by the magnitude of its inputs.
        fn bounded_floats(&self) -> Vec<f64> {
            let mut floats = Vec::with_capacity(self.values.len());
            for raw in &self.values {
                let significand = raw & ((1_u64 << 52) - 1);
                let exponent = i32::try_from((raw >> 52) % 401).assured("below 401") - 200;
                let negative = raw >> 63 == 1;
                let significand: f64 = (significand | (1 << 52)).approx_into();
                let magnitude = significand * 2_f64.powi(exponent - 52);
                floats.push(if negative { -magnitude } else { magnitude });
            }
            floats
        }
    }

    #[test]
    fn bolero_window_runs_match_scalar_folds_at_every_level() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(4096)
            .with_type::<WindowRun>()
            .for_each(|run| {
                let len = run.values.len();
                let validity_bits = run.validity_bits();
                let validity_slice = validity_bits.as_deref();
                let validity = RunValidity::new(validity_slice, usize::from(run.validity_offset));
                let present = (0..len)
                    .map(|index| run.present(validity_slice, index))
                    .collect::<Vec<_>>();
                let mut present_count = 0_u64;
                for (index, kept) in present.iter().enumerate() {
                    assert_eq!(validity.contains(index), *kept, "index {index}");
                    if *kept {
                        present_count += 1;
                    }
                }
                assert_eq!(validity.count(len), present_count);

                let booleans_offset = usize::from(run.booleans_offset);
                let booleans = bitmap_of(&run.booleans, booleans_offset + len);
                let mut expected_booleans = (0_u64, 0_u64);
                for (index, kept) in present.iter().enumerate() {
                    if !*kept {
                        continue;
                    }
                    if bit(&booleans, booleans_offset + index) {
                        expected_booleans.0 += 1;
                    } else {
                        expected_booleans.1 += 1;
                    }
                }
                assert_eq!(
                    count_booleans(&booleans, booleans_offset, validity, len),
                    expected_booleans
                );

                let signed = run
                    .values
                    .iter()
                    .map(|raw| raw.cast_signed())
                    .collect::<Vec<_>>();
                let mut signed_sum = 0_i128;
                let mut unsigned_sum = 0_i128;
                for (index, raw) in run.values.iter().enumerate() {
                    if present[index] {
                        signed_sum += i128::from(signed[index]);
                        unsigned_sum += i128::from(*raw);
                    }
                }
                for level in crate::supported_levels() {
                    assert_eq!(
                        sum_i64_lanes_for_level(level, &signed, validity),
                        (present_count, signed_sum),
                        "level={level:?}"
                    );
                    let unsigned =
                        dispatch!(level, simd => sum_u64_lanes(simd, &run.values, validity));
                    assert_eq!(unsigned, (present_count, unsigned_sum), "level={level:?}");
                }
                assert_eq!(sum_i64(&signed, validity), (present_count, signed_sum));
                assert_eq!(
                    sum_u64(&run.values, validity),
                    (present_count, unsigned_sum)
                );

                macro_rules! narrow_sum {
                    ($($native:ty),+) => {$(
                        let narrow = run
                            .values
                            .iter()
                            .map(|raw| <$native>::from_le_bytes(
                                raw.to_le_bytes()[..size_of::<$native>()]
                                    .try_into()
                                    .assured("the slice has the type's byte width"),
                            ))
                            .collect::<Vec<_>>();
                        let mut sum = 0_i128;
                        for (index, value) in narrow.iter().enumerate() {
                            if present[index] {
                                sum += i128::from(*value);
                            }
                        }
                        assert_eq!(sum_integer(&narrow, validity), (present_count, sum));
                    )+};
                }
                narrow_sum!(i8, u8, i16, u16, i32, u32);

                assert_eq!(
                    min_max(&signed, validity),
                    first_extremes(&signed, &present)
                );
                assert_eq!(
                    min_max(&run.values, validity),
                    first_extremes(&run.values, &present)
                );

                let wide = run
                    .values
                    .iter()
                    .map(|raw| f64::from_bits(*raw))
                    .collect::<Vec<_>>();
                let narrow = run
                    .values
                    .iter()
                    .map(|raw| f32::from_bits(u32::try_from(raw >> 32).assured("32 bits")))
                    .collect::<Vec<_>>();
                let mut expected_wide = vec![0_u8; len.div_ceil(8)];
                let mut expected_narrow = vec![0_u8; len.div_ceil(8)];
                for index in 0..len {
                    if present[index] && !wide[index].is_finite() {
                        expected_wide[index / 8] |= 1 << (index % 8);
                    }
                    if present[index] && !narrow[index].is_finite() {
                        expected_narrow[index / 8] |= 1 << (index % 8);
                    }
                }
                for level in crate::supported_levels() {
                    let wide_bits =
                        dispatch!(level, simd => non_finite_f64_lanes(simd, &wide, validity));
                    assert_eq!(wide_bits, expected_wide, "level={level:?}");
                    let narrow_bits =
                        dispatch!(level, simd => non_finite_f32_lanes(simd, &narrow, validity));
                    assert_eq!(narrow_bits, expected_narrow, "level={level:?}");
                }

                let mut visited = Vec::with_capacity(len);
                reverse_values(
                    &signed,
                    validity,
                    |value| value.approx_into::<f64>(),
                    |value| {
                        visited.push(value);
                    },
                );
                let mut expected_visits = Vec::with_capacity(len);
                for index in (0..len).rev() {
                    if present[index] {
                        expected_visits.push(Some(signed[index].approx_into::<f64>()));
                    } else {
                        expected_visits.push(None);
                    }
                }
                assert_eq!(visited, expected_visits);

                assert_compensated_sums(run, validity, &present);
            });
    }

    /// The first present minimum and maximum by the integers' total order, with their positions.
    fn first_extremes<T: Copy + Ord>(
        values: &[T],
        present: &[bool],
    ) -> Option<((usize, T), (usize, T))> {
        let mut smallest: Option<(usize, T)> = None;
        let mut largest: Option<(usize, T)> = None;
        for (index, value) in values.iter().enumerate() {
            if !present[index] {
                continue;
            }
            match smallest {
                Some((_, current)) if current <= *value => {}
                _ => smallest = Some((index, *value)),
            }
            match largest {
                Some((_, current)) if current >= *value => {}
                _ => largest = Some((index, *value)),
            }
        }
        match (smallest, largest) {
            (Some(smallest), Some(largest)) => Some((smallest, largest)),
            _ => None,
        }
    }

    /// A compensated sum of 32-bit integers is exact with no compensation left, and of any
    /// floats is within the bound of compensated summation of the exact sum: `2u|s|` plus
    /// `(2(n + 16)u)^2` times the sum of the magnitudes.
    fn assert_compensated_sums(run: &WindowRun, validity: RunValidity<'_>, present: &[bool]) {
        let integers = run
            .values
            .iter()
            .map(|raw| u32::try_from(raw >> 32).assured("32 bits").cast_signed())
            .collect::<Vec<_>>();
        let mut exact = 0_i64;
        let mut count = 0_u64;
        for (index, value) in integers.iter().enumerate() {
            if present[index] {
                exact += i64::from(*value);
                count += 1;
            }
        }
        let summed = compensated_sum(&integers, validity, f64::from);
        let exact: f64 = exact.approx_into();
        assert_eq!(summed.count, count);
        assert_eq!(summed.sum.to_bits(), exact.to_bits());
        assert_eq!(summed.compensation, 0.0);

        let floats = run.bounded_floats();
        let mut kept = Vec::with_capacity(floats.len());
        for (index, value) in floats.iter().enumerate() {
            if present[index] {
                kept.push(*value);
            }
        }
        let summed = compensated_sum(&floats, validity, |value| value);
        assert_eq!(summed.count, count);
        let total = summed.sum + summed.compensation;
        let mut difference = exact_partials(kept.iter().copied());
        let exact_sum = partials_value(&difference);
        difference = exact_partials(difference.into_iter().chain([-total]));
        let error = partials_value(&difference).abs();
        let unit = f64::EPSILON / 2.0;
        let terms: f64 = (count + 16).approx_into();
        let magnitudes: f64 = kept.iter().map(|value| value.abs()).sum();
        let bound = 2.0 * unit * exact_sum.abs()
            + (2.0 * terms * unit).powi(2) * magnitudes * 2.0
            + f64::from_bits(1);
        assert!(
            error <= bound,
            "compensated sum {total:e} is {error:e} from the exact {exact_sum:e}, beyond {bound:e}"
        );
    }

    #[test]
    fn reverse_typed_visits_keep_null_positions() {
        let mut values = Vec::new();
        reverse_values(
            &[2_i32, 4, 6],
            RunValidity::new(Some(&[0b0000_0101]), 0),
            f64::from,
            |value| values.push(value),
        );
        assert_eq!(values, vec![Some(6.0), None, Some(2.0)]);
    }
}
