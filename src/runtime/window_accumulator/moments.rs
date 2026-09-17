//! Mergeable centered moments for the averaging and dispersion statistics of a window.
//!
//! Layer: data plane.
//!
//! - **Owns.** The count, means and centered second moments of one or two arguments, how two row
//!   sets' moments combine, and the `AVG`, variance, standard deviation, covariance and correlation
//!   results derived from them.
//! - **Depends on.** The argument columns of admitted rows and the mergeable-aggregate contract.
//! - **Must not know.** Which rows a window retains, how retraction recomputes them, or when a
//!   window emits.
//!
//! Every moment is kept centered on its own mean. Combining two row sets adds their centered
//! moments and one non-negative correction for the distance between their means, so a variance is
//! never the small difference of two large sums of squares.

use std::ops::Range;

use arrow_array::Float64Array;

use super::{two_stacks::MergeableAggregate, *};

/// The moments of the present values of one numeric argument, each converted to the nearest F64.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::runtime) struct Moments {
    count: u64,
    mean: f64,
    /// `Σ (x − mean)²` over the contributing values.
    squares: f64,
}

/// The moments of two numeric arguments over the rows where both are present.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::runtime) struct CoMoments {
    count: u64,
    first_mean: f64,
    second_mean: f64,
    /// `Σ (first − first_mean)²` over the contributing rows.
    first_squares: f64,
    /// `Σ (second − second_mean)²` over the contributing rows.
    second_squares: f64,
    /// `Σ (first − first_mean)(second − second_mean)` over the contributing rows.
    cross_products: f64,
}

/// How two contributing row sets combine: their total count and the weight `na·nb/n` that scales
/// the correction for the distance between their means.
struct Combined {
    count: u64,
    older_count: f64,
    newer_count: f64,
    total: f64,
}

impl Combined {
    fn of(older: u64, newer: u64) -> Self {
        let count = older
            .checked_add(newer)
            .assured("a window cannot retain 2^64 rows in memory");
        let older_count: f64 = older.approx_into();
        let newer_count: f64 = newer.approx_into();
        let total: f64 = count.approx_into();
        Self {
            count,
            older_count,
            newer_count,
            total,
        }
    }

    /// The mean of the combined set, moved from the older mean toward the newer one.
    fn mean(&self, older_mean: f64, newer_mean: f64) -> f64 {
        older_mean + (newer_mean - older_mean) * self.newer_count / self.total
    }

    /// The correction a centered second moment gains when two sets whose means differ by
    /// `first_delta` and `second_delta` combine.
    fn correction(&self, first_delta: f64, second_delta: f64) -> f64 {
        first_delta * second_delta * self.older_count * self.newer_count / self.total
    }
}

impl MergeableAggregate for Moments {
    const EMPTY: Self = Self {
        count: 0,
        mean: 0.0,
        squares: 0.0,
    };

    fn merge(older: Self, newer: Self) -> Self {
        if newer.count == 0 {
            return older;
        }
        if older.count == 0 {
            return newer;
        }
        let combined = Combined::of(older.count, newer.count);
        let delta = newer.mean - older.mean;
        Self {
            count: combined.count,
            mean: combined.mean(older.mean, newer.mean),
            squares: older.squares + newer.squares + combined.correction(delta, delta),
        }
    }
}

impl MergeableAggregate for CoMoments {
    const EMPTY: Self = Self {
        count: 0,
        first_mean: 0.0,
        second_mean: 0.0,
        first_squares: 0.0,
        second_squares: 0.0,
        cross_products: 0.0,
    };

    fn merge(older: Self, newer: Self) -> Self {
        if newer.count == 0 {
            return older;
        }
        if older.count == 0 {
            return newer;
        }
        let combined = Combined::of(older.count, newer.count);
        let first_delta = newer.first_mean - older.first_mean;
        let second_delta = newer.second_mean - older.second_mean;
        Self {
            count: combined.count,
            first_mean: combined.mean(older.first_mean, newer.first_mean),
            second_mean: combined.mean(older.second_mean, newer.second_mean),
            first_squares: older.first_squares
                + newer.first_squares
                + combined.correction(first_delta, first_delta),
            second_squares: older.second_squares
                + newer.second_squares
                + combined.correction(second_delta, second_delta),
            cross_products: older.cross_products
                + newer.cross_products
                + combined.correction(first_delta, second_delta),
        }
    }
}

/// Whether a dispersion statistic describes the contributing rows themselves or estimates the
/// population they were sampled from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Estimator {
    /// Divides by the number of contributing rows, and is defined from one row.
    Population,
    /// Divides by one less than the number of contributing rows, and needs two.
    Sample,
}

impl Estimator {
    /// The divisor for `count` contributing rows, or `None` when the statistic is undefined.
    fn divisor(self, count: u64) -> Option<f64> {
        let divisor = match self {
            Self::Population => count,
            Self::Sample => count.checked_sub(1)?,
        };
        if divisor == 0 {
            return None;
        }
        Some(divisor.approx_into())
    }
}

/// A statistic as a one-row F64 array: null when undefined, an error when not finite.
fn float_statistic(
    function: WindowAggregateFunction,
    statistic: Option<f64>,
) -> error_stack::Result<ArrayRef, WindowProcessorError> {
    if let Some(value) = statistic
        && !value.is_finite()
    {
        return Err(Report::new(WindowProcessorError::StatisticNotFinite {
            function,
        }));
    }
    let array: ArrayRef = StdArc::new(Float64Array::from(vec![statistic]));
    Ok(array)
}

impl Moments {
    fn of_value(value: f64) -> Self {
        Self {
            count: 1,
            mean: value,
            squares: 0.0,
        }
    }

    pub(super) fn of_row(column: &ArgumentColumn, row: usize) -> Self {
        match column.number_at(row) {
            Some(value) => Self::of_value(value),
            None => Self::EMPTY,
        }
    }

    pub(super) fn of_rows(column: &ArgumentColumn, rows: Range<usize>) -> Self {
        let mut moments = Self::EMPTY;
        for row in rows {
            moments = Self::merge(moments, Self::of_row(column, row));
        }
        moments
    }

    fn mean(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        Some(self.mean)
    }

    fn variance(&self, estimator: Estimator) -> Option<f64> {
        let divisor = estimator.divisor(self.count)?;
        Some(self.squares / divisor)
    }

    pub(super) fn evaluate(
        &self,
        function: WindowAggregateFunction,
    ) -> error_stack::Result<ArrayRef, WindowProcessorError> {
        let statistic = match function {
            WindowAggregateFunction::Avg => Some(self.mean()),
            WindowAggregateFunction::VarPop => Some(self.variance(Estimator::Population)),
            WindowAggregateFunction::VarSamp => Some(self.variance(Estimator::Sample)),
            WindowAggregateFunction::StddevPop => {
                Some(self.variance(Estimator::Population).map(f64::sqrt))
            }
            WindowAggregateFunction::StddevSamp => {
                Some(self.variance(Estimator::Sample).map(f64::sqrt))
            }
            _ => None,
        };
        let statistic = statistic
            .verified("moments serve only AVG, VAR_POP, VAR_SAMP, STDDEV_POP and STDDEV_SAMP");
        float_statistic(function, statistic)
    }
}

impl CoMoments {
    pub(super) fn of_row(arguments: &WindowArguments<ArgumentColumn>, row: usize) -> Self {
        let second = arguments
            .second()
            .verified("co-moments are planned only for two-argument functions");
        let (Some(first), Some(second)) = (arguments.first().number_at(row), second.number_at(row))
        else {
            return Self::EMPTY;
        };
        Self {
            count: 1,
            first_mean: first,
            second_mean: second,
            first_squares: 0.0,
            second_squares: 0.0,
            cross_products: 0.0,
        }
    }

    pub(super) fn of_rows(arguments: &WindowArguments<ArgumentColumn>, rows: Range<usize>) -> Self {
        let mut moments = Self::EMPTY;
        for row in rows {
            moments = Self::merge(moments, Self::of_row(arguments, row));
        }
        moments
    }

    fn is_finite(&self) -> bool {
        self.first_mean.is_finite()
            && self.second_mean.is_finite()
            && self.first_squares.is_finite()
            && self.second_squares.is_finite()
            && self.cross_products.is_finite()
    }

    fn covariance(&self, estimator: Estimator) -> Option<f64> {
        let divisor = estimator.divisor(self.count)?;
        Some(self.cross_products / divisor)
    }

    /// The Pearson correlation, undefined unless two rows contributed and both arguments vary.
    fn correlation(&self) -> Option<f64> {
        if self.count < 2 || self.first_squares <= 0.0 || self.second_squares <= 0.0 {
            return None;
        }
        let product = self.first_squares * self.second_squares;
        let denominator = if product.is_normal() {
            product.sqrt()
        } else {
            self.first_squares.sqrt() * self.second_squares.sqrt()
        };
        Some((self.cross_products / denominator).clamp(-1.0, 1.0))
    }

    pub(super) fn evaluate(
        &self,
        function: WindowAggregateFunction,
    ) -> error_stack::Result<ArrayRef, WindowProcessorError> {
        if !self.is_finite() {
            return Err(Report::new(WindowProcessorError::StatisticNotFinite {
                function,
            }));
        }
        let statistic = match function {
            WindowAggregateFunction::CovarPop => Some(self.covariance(Estimator::Population)),
            WindowAggregateFunction::CovarSamp => Some(self.covariance(Estimator::Sample)),
            WindowAggregateFunction::Corr => Some(self.correlation()),
            _ => None,
        };
        let statistic = statistic.verified("co-moments serve only COVAR_POP, COVAR_SAMP and CORR");
        float_statistic(function, statistic)
    }
}

#[cfg(test)]
mod tests {
    use super::{super::two_stacks::TwoStacks, *};

    fn moments_of(values: &[f64]) -> Moments {
        let mut moments = Moments::EMPTY;
        for value in values {
            moments = Moments::merge(moments, Moments::of_value(*value));
        }
        moments
    }

    /// The exact population and sample variance of `values`, computed in integers. Every value is
    /// a multiple of the smallest unit in last place among them, so `Σk²` and `(Σk)²` are exact and
    /// only the final conversion rounds.
    struct ExactVariance {
        mean: f64,
        population: f64,
        sample: f64,
    }

    impl ExactVariance {
        fn of(values: &[f64]) -> Self {
            let decomposed = values
                .iter()
                .map(|value| decompose(*value))
                .collect::<Vec<_>>();
            let exponent = decomposed
                .iter()
                .map(|(_, exponent)| *exponent)
                .min()
                .expect("a reference needs at least one value");
            let units = decomposed
                .iter()
                .map(|(mantissa, value_exponent)| mantissa << (value_exponent - exponent))
                .collect::<Vec<i128>>();
            let count = i128::try_from(units.len()).expect("the test windows are small");
            let sum = units.iter().sum::<i128>();
            let squares = units.iter().map(|unit| unit * unit).sum::<i128>();
            let numerator = count
                .checked_mul(squares)
                .and_then(|scaled| scaled.checked_sub(sum * sum))
                .expect("the reference datasets fit 128-bit arithmetic");
            let scale = 2f64.powi(exponent);
            let numerator: f64 = numerator.approx_into();
            let count_f64: f64 = count.approx_into();
            let sum_f64: f64 = sum.approx_into();
            Self {
                mean: sum_f64 / count_f64 * scale,
                population: numerator / (count_f64 * count_f64) * scale * scale,
                sample: numerator / (count_f64 * (count_f64 - 1.0)) * scale * scale,
            }
        }
    }

    /// `value` as an integer mantissa and a binary exponent.
    fn decompose(value: f64) -> (i128, i32) {
        if value == 0.0 {
            return (0, 0);
        }
        let bits = value.to_bits();
        let sign = if bits >> 63 == 0 { 1 } else { -1 };
        let biased = i32::try_from((bits >> 52) & 0x7ff).expect("an 11-bit exponent fits");
        let fraction = i128::from(bits & ((1 << 52) - 1));
        assert!(
            biased > 0,
            "the reference datasets hold no subnormal values"
        );
        let mantissa = fraction | (1 << 52);
        let trailing = mantissa.trailing_zeros();
        let shift = i32::try_from(trailing).expect("a 53-bit mantissa has few trailing zeros");
        (sign * (mantissa >> trailing), biased - 1075 + shift)
    }

    fn relative_error(actual: f64, expected: f64) -> f64 {
        ((actual - expected) / expected).abs()
    }

    /// NIST StRD univariate datasets whose values differ only far past their leading digits, which
    /// is exactly where a variance computed from sums of squares loses every significant digit.
    fn nist_numerical_accuracy(first: f64, low: f64, high: f64) -> Vec<f64> {
        let mut values = vec![first];
        for _ in 0..500 {
            values.push(low);
            values.push(high);
        }
        values
    }

    #[test]
    fn moments_match_nist_certified_values_for_numerical_accuracy_datasets() {
        let num_acc_1 = moments_of(&[10_000_001.0, 10_000_003.0, 10_000_002.0]);
        assert_eq!(num_acc_1.mean(), Some(10_000_002.0));
        assert_eq!(num_acc_1.variance(Estimator::Sample), Some(1.0));

        // The bounds sit just below what the algorithm measures on each dataset: 15.5, 9.4 and 8.2
        // correct digits of deviation, and a variance within 1e-15, 6e-12 and 4e-11 of the exact
        // variance of the binary values. A variance taken as the difference of sums of squares keeps
        // almost none of those digits on the last two datasets.
        for (values, certified_mean, minimum_digits, exact_bound) in [
            (nist_numerical_accuracy(1.2, 1.1, 1.3), 1.2, 15.0, 1e-14),
            (
                nist_numerical_accuracy(1_000_000.2, 1_000_000.1, 1_000_000.3),
                1_000_000.2,
                9.0,
                1e-10,
            ),
            (
                nist_numerical_accuracy(10_000_000.2, 10_000_000.1, 10_000_000.3),
                10_000_000.2,
                8.0,
                5e-10,
            ),
        ] {
            let moments = moments_of(&values);
            let deviation = moments
                .variance(Estimator::Sample)
                .map(f64::sqrt)
                .expect("1001 values define a sample deviation");
            // Log relative error against the certified values, which describe the decimal data
            // rather than its nearest binary values, so the bound also absorbs input rounding.
            let deviation_digits = -relative_error(deviation, 0.1).log10();
            assert!(
                deviation_digits >= minimum_digits,
                "standard deviation {deviation} keeps only {deviation_digits} correct digits"
            );
            let mean = moments.mean().expect("the dataset is not empty");
            assert!(relative_error(mean, certified_mean) < 1e-14, "mean {mean}");

            // Against the exact variance of the binary values the only error left is the
            // algorithm's own.
            let exact = ExactVariance::of(&values);
            let variance = moments
                .variance(Estimator::Sample)
                .expect("the dataset has 1001 values");
            assert!(
                relative_error(variance, exact.sample) < exact_bound,
                "sample variance {variance} against exact {}",
                exact.sample
            );
        }
    }

    #[test]
    fn sliding_moments_track_exact_references_and_forget_evicted_outliers() {
        let mut rng = fastrand::Rng::with_seed(1_511);
        let values = (0..3_000)
            .map(|index| {
                if index % 97 == 0 {
                    let sign = if rng.bool() { 1.0 } else { -1.0 };
                    sign * 1e9
                } else {
                    f64::from(rng.i32(-1_024_000..1_024_000)) / 1_024.0
                }
            })
            .collect::<Vec<f64>>();
        let width = 40;
        let mut stacks = TwoStacks::<Moments>::new();
        let mut retained = std::collections::VecDeque::new();
        for value in &values {
            if retained.len() == width {
                let survivors = retained.iter().copied().collect::<Vec<f64>>();
                stacks.retract_oldest(1, width, |position| Moments::of_value(survivors[position]));
                retained.pop_front();
            }
            retained.push_back(*value);
            stacks.admit(Moments::of_value(*value));
            if retained.len() < 2 {
                continue;
            }
            let window = retained.iter().copied().collect::<Vec<f64>>();
            let exact = ExactVariance::of(&window);
            let aggregate = stacks.aggregate();
            let variance = aggregate
                .variance(Estimator::Population)
                .expect("the window holds values");
            let sample = aggregate
                .variance(Estimator::Sample)
                .expect("the window holds two values");
            let mean = aggregate.mean().expect("the window holds values");
            // Measured at below 1e-15 across every window, outliers included.
            assert!(
                relative_error(variance, exact.population) < 1e-14,
                "population variance {variance} against exact {} over {window:?}",
                exact.population
            );
            assert!(relative_error(sample, exact.sample) < 1e-14);
            assert!((mean - exact.mean).abs() <= 1e-12 * exact.mean.abs().max(1.0));
        }
    }

    #[test]
    fn an_evicted_outlier_leaves_exact_statistics_behind() {
        let mut stacks = TwoStacks::<Moments>::new();
        let rows = [1e16, 1.0, 3.0];
        stacks.admit(Moments::of_value(rows[0]));
        stacks.admit(Moments::of_value(rows[1]));
        stacks.retract_oldest(1, 2, |position| Moments::of_value(rows[position]));
        stacks.admit(Moments::of_value(rows[2]));
        let aggregate = stacks.aggregate();
        assert_eq!(aggregate.mean(), Some(2.0));
        assert_eq!(aggregate.variance(Estimator::Population), Some(1.0));
        assert_eq!(aggregate.variance(Estimator::Sample), Some(2.0));
    }

    #[test]
    fn undefined_statistics_are_typed_nulls() {
        let one = moments_of(&[4.0]);
        assert_eq!(one.variance(Estimator::Population), Some(0.0));
        assert_eq!(one.variance(Estimator::Sample), None);
        assert_eq!(Moments::EMPTY.mean(), None);

        let constant = [(100.0, 1.0), (100.0, 2.0), (100.0, 3.0)].into_iter().fold(
            CoMoments::EMPTY,
            |moments, (first, second)| {
                CoMoments::merge(
                    moments,
                    CoMoments {
                        count: 1,
                        first_mean: first,
                        second_mean: second,
                        first_squares: 0.0,
                        second_squares: 0.0,
                        cross_products: 0.0,
                    },
                )
            },
        );
        assert_eq!(constant.covariance(Estimator::Population), Some(0.0));
        assert_eq!(constant.correlation(), None);
    }

    #[test]
    fn perfectly_linear_arguments_correlate_exactly() {
        let moments = [(2.0, 10.0), (8.0, 40.0), (5.0, 25.0)].into_iter().fold(
            CoMoments::EMPTY,
            |moments, (first, second)| {
                CoMoments::merge(
                    moments,
                    CoMoments {
                        count: 1,
                        first_mean: first,
                        second_mean: second,
                        first_squares: 0.0,
                        second_squares: 0.0,
                        cross_products: 0.0,
                    },
                )
            },
        );
        assert_eq!(moments.covariance(Estimator::Sample), Some(45.0));
        assert_eq!(moments.covariance(Estimator::Population), Some(30.0));
        assert_eq!(moments.correlation(), Some(1.0));
    }

    #[test]
    fn statistics_that_overflow_are_errors_rather_than_values() {
        let moments = moments_of(&[f64::MAX, -f64::MAX]);
        let error = moments
            .evaluate(WindowAggregateFunction::VarPop)
            .expect_err("the spread of the extremes overflows");
        assert!(matches!(
            error.current_context(),
            WindowProcessorError::StatisticNotFinite {
                function: WindowAggregateFunction::VarPop
            }
        ));
    }
}
