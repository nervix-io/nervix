//! Statistics over the values of one list: its variances and standard deviations, its median and
//! exact quantiles, and the positions of its least and greatest values.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The names `list_var_pop`, `list_var_samp`, `list_stddev_pop`, `list_stddev_samp`,
//!   `list_median`, `list_quantile`, `list_argmin` and `list_argmax`, the statistic each one
//!   computes, the percentile a quantile selects, which statistics can fail a row, and how a
//!   conditional arm runs each of them.
//! - **Depends on.** The estimator the window statistics divide their centered moments by, and the
//!   semantic catalog's vocabulary of operation semantics.
//! - **Must not know.** Registers, instructions, Arrow arrays, or how a failed row is recorded.
//!
//! Every name carries the `list_` prefix, so none collides with a window aggregate: a window route
//! reads `VAR_POP`, `STDDEV_SAMP` and the rest as aggregates over its retained rows, and these
//! functions as statistics over the one list each row holds.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use nervix_simd_kernels::Estimator;
use strum::{EnumString, IntoStaticStr};

use crate::semantics::{
    ArmExecution, DependencyScope, NullPropagation, OperationSemantics, Volatility,
};

/// The name of a list statistic, before a quantile's percentile is read from its literal.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    EnumString,
    IntoStaticStr,
    strum::Display,
    strum::EnumIter,
)]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum ListStatisticName {
    ListVarPop,
    ListVarSamp,
    ListStddevPop,
    ListStddevSamp,
    ListMedian,
    ListQuantile,
    ListArgmin,
    ListArgmax,
}

/// A list statistic, with the percentile a quantile selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ListStatistic {
    /// A variance or standard deviation of the elements, each read as the nearest `F64`.
    Dispersion(ListDispersion),
    /// The exact quantile of the elements at a percentile.
    Quantile(ListQuantile),
    /// The position of the first least or the first greatest element.
    ExtremeIndex(ListExtremum),
}

impl ListStatistic {
    /// The name the statistic is called by.
    pub const fn name(self) -> ListStatisticName {
        match self {
            Self::Dispersion(dispersion) => dispersion.name(),
            Self::Quantile(quantile) => quantile.name(),
            Self::ExtremeIndex(ListExtremum::Least) => ListStatisticName::ListArgmin,
            Self::ExtremeIndex(ListExtremum::Greatest) => ListStatisticName::ListArgmax,
        }
    }

    /// What the catalog records for the statistic. A null list gives null, and a statistic read in
    /// floating point fails a row whose list holds a non-finite element or whose result is not
    /// finite; an index compares values without reading them as numbers, so it never fails.
    pub const fn semantics(self) -> OperationSemantics {
        let can_error = match self {
            Self::Dispersion(_) | Self::Quantile(_) => true,
            Self::ExtremeIndex(_) => false,
        };
        OperationSemantics {
            volatility: Volatility::Immutable,
            dependency_scope: DependencyScope::Constant,
            has_side_effects: false,
            can_error,
            null_propagation: NullPropagation::Strict,
        }
    }

    /// How a conditional arm runs the statistic.
    pub const fn arm_execution(self) -> ArmExecution {
        match self {
            // Two passes over each list's values, the cost class of `mean`.
            Self::Dispersion(_) => ArmExecution::WholeBatch,
            // Copying and selecting within each list costs far more than narrowing the batch.
            Self::Quantile(_) => ArmExecution::SelectedRows,
            // An index never fails, so it never runs under a selection.
            Self::ExtremeIndex(_) => ArmExecution::WholeBatch,
        }
    }
}

/// A variance or a standard deviation, derived from the centered moments of a list's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ListDispersion {
    /// `list_var_pop`.
    VarPop,
    /// `list_var_samp`.
    VarSamp,
    /// `list_stddev_pop`.
    StddevPop,
    /// `list_stddev_samp`.
    StddevSamp,
}

impl ListDispersion {
    const fn name(self) -> ListStatisticName {
        match self {
            Self::VarPop => ListStatisticName::ListVarPop,
            Self::VarSamp => ListStatisticName::ListVarSamp,
            Self::StddevPop => ListStatisticName::ListStddevPop,
            Self::StddevSamp => ListStatisticName::ListStddevSamp,
        }
    }

    /// The statistic, as it names itself in the error of a row it fails.
    pub const fn float_statistic(self) -> FloatListStatistic {
        match self {
            Self::VarPop => FloatListStatistic::ListVarPop,
            Self::VarSamp => FloatListStatistic::ListVarSamp,
            Self::StddevPop => FloatListStatistic::ListStddevPop,
            Self::StddevSamp => FloatListStatistic::ListStddevSamp,
        }
    }

    /// Whether the statistic describes the values themselves or the population they sample.
    pub const fn estimator(self) -> Estimator {
        match self {
            Self::VarPop | Self::StddevPop => Estimator::Population,
            Self::VarSamp | Self::StddevSamp => Estimator::Sample,
        }
    }

    /// The statistic, from the variance its estimator defines: the variance itself, or its square
    /// root.
    pub fn from_variance(self, variance: f64) -> f64 {
        match self {
            Self::VarPop | Self::VarSamp => variance,
            Self::StddevPop | Self::StddevSamp => variance.sqrt(),
        }
    }
}

/// `list_median` or `list_quantile`, with the percentile it selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ListQuantile {
    /// `list_median`, the quantile at the 50th percentile.
    Median,
    /// `list_quantile`, at the percentile its literal names.
    Percentile(ListPercentile),
}

impl ListQuantile {
    const fn name(self) -> ListStatisticName {
        match self {
            Self::Median => ListStatisticName::ListMedian,
            Self::Percentile(_) => ListStatisticName::ListQuantile,
        }
    }

    /// The statistic, as it names itself in the error of a row it fails.
    pub const fn float_statistic(self) -> FloatListStatistic {
        match self {
            Self::Median => FloatListStatistic::ListMedian,
            Self::Percentile(_) => FloatListStatistic::ListQuantile,
        }
    }

    /// The percentile the quantile selects.
    pub const fn percentile(self) -> ListPercentile {
        match self {
            Self::Median => ListPercentile::MEDIAN,
            Self::Percentile(percentile) => percentile,
        }
    }
}

/// A percentile from 0 to 100, which `list_quantile` reads from its literal argument.
#[derive(Debug, Clone, Copy)]
pub struct ListPercentile(f64);

impl ListPercentile {
    /// The 50th percentile, which `list_median` selects.
    pub const MEDIAN: Self = Self(50.0);

    /// The percentile `value` names, or `None` when it lies outside 0 to 100. Both zeros name the
    /// same percentile.
    pub fn new(value: f64) -> Option<Self> {
        if !(0.0..=100.0).contains(&value) {
            return None;
        }
        if value == 0.0 {
            return Some(Self(0.0));
        }
        Some(Self(value))
    }

    /// The percentile, from 0 to 100.
    pub const fn value(self) -> f64 {
        self.0
    }
}

// A percentile is never NaN and never negative zero, so its bits identify it and order it as its
// value does.
impl PartialEq for ListPercentile {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for ListPercentile {}

impl PartialOrd for ListPercentile {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ListPercentile {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl Hash for ListPercentile {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

/// Which extreme element `list_argmin` or `list_argmax` locates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ListExtremum {
    /// `list_argmin`: the first least element.
    Least,
    /// `list_argmax`: the first greatest element.
    Greatest,
}

impl ListExtremum {
    /// Whether an element that compares to the extreme found so far as `ordering` replaces it. Only
    /// a strictly more extreme element does, so the first of equal elements is kept.
    pub const fn replaces(self, ordering: Ordering) -> bool {
        match self {
            Self::Least => matches!(ordering, Ordering::Less),
            Self::Greatest => matches!(ordering, Ordering::Greater),
        }
    }
}

/// A list statistic read in floating point, which fails a row whose list holds a non-finite
/// element or whose result is not finite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum FloatListStatistic {
    ListVarPop,
    ListVarSamp,
    ListStddevPop,
    ListStddevSamp,
    ListMedian,
    ListQuantile,
}

#[cfg(test)]
mod tests {
    use std::hash::DefaultHasher;

    use meticulous::OptionExt as _;
    use strum::IntoEnumIterator as _;

    use super::*;

    fn hash_of(percentile: ListPercentile) -> u64 {
        let mut hasher = DefaultHasher::new();
        percentile.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn a_percentile_reads_zero_to_one_hundred_and_names_one_value() {
        assert_eq!(ListPercentile::new(-0.5), None);
        assert_eq!(ListPercentile::new(100.5), None);
        assert_eq!(ListPercentile::new(f64::NAN), None);
        let zero = ListPercentile::new(0.0).verified("zero is a percentile");
        let negative_zero = ListPercentile::new(-0.0).verified("negative zero reads as zero");
        assert_eq!(zero, negative_zero);
        assert_eq!(hash_of(zero), hash_of(negative_zero));
        assert_eq!(negative_zero.value().to_bits(), 0.0_f64.to_bits());
        let quartile = ListPercentile::new(25.0).verified("25 is a percentile");
        assert!(quartile < ListPercentile::MEDIAN);
        assert_eq!(quartile.cmp(&quartile), Ordering::Equal);
        assert_eq!(ListQuantile::Median.percentile(), ListPercentile::MEDIAN);
        assert_eq!(ListQuantile::Percentile(quartile).percentile(), quartile);
    }

    #[test]
    fn floating_statistics_fail_rows_and_an_index_never_does() {
        for dispersion in [
            ListDispersion::VarPop,
            ListDispersion::VarSamp,
            ListDispersion::StddevPop,
            ListDispersion::StddevSamp,
        ] {
            let statistic = ListStatistic::Dispersion(dispersion);
            assert!(statistic.semantics().can_error);
            assert_eq!(statistic.arm_execution(), ArmExecution::WholeBatch);
        }
        let median = ListStatistic::Quantile(ListQuantile::Median);
        assert!(median.semantics().can_error);
        assert_eq!(median.arm_execution(), ArmExecution::SelectedRows);
        for extremum in [ListExtremum::Least, ListExtremum::Greatest] {
            let statistic = ListStatistic::ExtremeIndex(extremum);
            assert!(!statistic.semantics().can_error);
            assert_eq!(statistic.arm_execution(), ArmExecution::WholeBatch);
        }
    }

    #[test]
    fn every_name_reads_back_without_regard_to_letter_case() {
        let names = ListStatisticName::iter().collect::<Vec<_>>();
        assert_eq!(names.len(), 8);
        for name in names {
            let written = name.to_string();
            assert!(written.starts_with("list_"), "{written}");
            assert_eq!(
                written.to_ascii_uppercase().parse::<ListStatisticName>(),
                Ok(name)
            );
        }
    }
}
