//! The kernels of the statistics over one list, each computed per list over the list's child value
//! buffer.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The centered moments, exact quantiles and first extreme positions of each row's
//!   list, which rows hold a non-finite element or produce a non-finite result, and the scratch
//!   values one batch's quantiles select within.
//! - **Depends on.** The run moments, estimators and quantile rank the window statistics share,
//!   Arrow's order of native values, and the runtime's list column view.
//! - **Must not know.** Registers, programs, conditional arms, or window aggregates.
//!
//! No kernel builds a value per row. A variance reads each list's slice of the child buffer in
//! place, a quantile copies a list's values into one scratch buffer the whole batch reuses and
//! selects within it without sorting the column, and an index compares the child values where they
//! lie. Elements order as `min` and `max` order them: floating-point values by the IEEE 754 total
//! order, `BOOL` with `false` first, `STRING` by its bytes, and `DATETIME` by its instant.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{cmp::Ordering, ops::Range};

use arrow_array::{
    Array, ArrowNativeTypeOp, BooleanArray, Float64Array, Int64Array, PrimitiveArray, StringArray,
    types::{
        ArrowPrimitiveType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, TimeUnit};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use nervix_simd_kernels::{QuantileRank, RunValidity};

use super::{ListColumn, collection_backing_mismatch};
use crate::{
    batch::TypedArray,
    error::{FloatOperation, RowErrors, RuntimeError, SideError, SideErrorReason},
    list_statistics::{
        FloatListStatistic, ListDispersion, ListExtremum, ListQuantile, ListStatistic,
    },
    program::Span,
};

/// Runs `$body` with `$Element` naming the Arrow type of the list's numeric elements, or refuses a
/// list of any other elements.
macro_rules! with_numeric_element {
    ($list:expr, $operation:expr, $Element:ident => $body:expr) => {
        match $list.element_data_type() {
            DataType::UInt8 => {
                type $Element = UInt8Type;
                $body
            }
            DataType::Int8 => {
                type $Element = Int8Type;
                $body
            }
            DataType::UInt16 => {
                type $Element = UInt16Type;
                $body
            }
            DataType::Int16 => {
                type $Element = Int16Type;
                $body
            }
            DataType::UInt32 => {
                type $Element = UInt32Type;
                $body
            }
            DataType::Int32 => {
                type $Element = Int32Type;
                $body
            }
            DataType::UInt64 => {
                type $Element = UInt64Type;
                $body
            }
            DataType::Int64 => {
                type $Element = Int64Type;
                $body
            }
            DataType::Float32 => {
                type $Element = Float32Type;
                $body
            }
            DataType::Float64 => {
                type $Element = Float64Type;
                $body
            }
            other => Err(Report::new(RuntimeError::CollectionNonNumericElement {
                operation: $operation,
                actual: other.clone(),
            })),
        }
    };
}

/// Computes `statistic` over the list each row holds. A null list gives null, as does a list too
/// short for the statistic to be defined.
pub(super) fn execute_list_statistic(
    statistic: ListStatistic,
    input: &TypedArray,
    row_errors: &mut RowErrors,
    span: Span,
) -> error_stack::Result<TypedArray, RuntimeError> {
    let list = ListColumn::from_typed(input)?;
    let operation: &'static str = statistic.name().into();
    match statistic {
        ListStatistic::Dispersion(dispersion) => {
            let failures = RowFailures::new(dispersion.float_statistic(), row_errors, span);
            let output = with_numeric_element!(list, operation, Element => {
                dispersion_of::<Element>(list, dispersion, failures)
            })?;
            Ok(TypedArray::Float64(output))
        }
        ListStatistic::Quantile(quantile) => {
            let failures = RowFailures::new(quantile.float_statistic(), row_errors, span);
            let output = with_numeric_element!(list, operation, Element => {
                quantile_of::<Element>(list, quantile, failures)
            })?;
            Ok(TypedArray::Float64(output))
        }
        ListStatistic::ExtremeIndex(extremum) => Ok(TypedArray::Int64(extreme_positions(
            list, extremum, operation,
        )?)),
    }
}

/// A numeric element type, with how one of its values reads as the nearest `F64` and which of its
/// values are not finite.
trait NumericElement: ArrowPrimitiveType {
    fn nearest(value: Self::Native) -> f64;

    /// One bit per present value that is not finite, or `None` for a type whose every value is.
    fn non_finite(values: &[Self::Native], validity: RunValidity<'_>) -> Option<Vec<u8>>;
}

macro_rules! integer_element {
    ($($element:ty => $nearest:expr),+ $(,)?) => {$(
        impl NumericElement for $element {
            fn nearest(value: Self::Native) -> f64 {
                $nearest(value)
            }

            fn non_finite(_values: &[Self::Native], _validity: RunValidity<'_>) -> Option<Vec<u8>> {
                None
            }
        }
    )+};
}

integer_element!(
    UInt8Type => f64::from,
    Int8Type => f64::from,
    UInt16Type => f64::from,
    Int16Type => f64::from,
    UInt32Type => f64::from,
    Int32Type => f64::from,
    UInt64Type => |value: u64| -> f64 { value.approx_into() },
    Int64Type => |value: i64| -> f64 { value.approx_into() },
);

impl NumericElement for Float32Type {
    fn nearest(value: f32) -> f64 {
        f64::from(value)
    }

    fn non_finite(values: &[f32], validity: RunValidity<'_>) -> Option<Vec<u8>> {
        Some(nervix_simd_kernels::non_finite_f32(values, validity))
    }
}

impl NumericElement for Float64Type {
    fn nearest(value: f64) -> f64 {
        value
    }

    fn non_finite(values: &[f64], validity: RunValidity<'_>) -> Option<Vec<u8>> {
        Some(nervix_simd_kernels::non_finite_f64(values, validity))
    }
}

/// The child values of a list of `T` elements, with the validity of each.
struct Elements<'a, T: ArrowPrimitiveType> {
    values: &'a PrimitiveArray<T>,
}

impl<'a, T: ArrowPrimitiveType> Elements<'a, T> {
    fn of(list: ListColumn<'a>) -> error_stack::Result<Self, RuntimeError> {
        let values = list
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| collection_backing_mismatch(list.element_data_type()))?;
        Ok(Self { values })
    }

    /// The values of the child positions `range`, present or not.
    fn slice(&self, range: Range<usize>) -> &'a [T::Native] {
        &self.values.values()[range]
    }

    /// The validity of the child values from position `start` on.
    fn validity_from(&self, start: usize) -> RunValidity<'a> {
        match self.values.nulls() {
            Some(nulls) => RunValidity::new(Some(nulls.validity()), nulls.offset() + start),
            None => RunValidity::new(None, 0),
        }
    }
}

impl<T: NumericElement> Elements<'_, T> {
    /// One bit per child position whose value is present and not finite, over the whole child
    /// buffer at once, or `None` when every value of the type is finite.
    fn non_finite(&self) -> Option<Vec<u8>> {
        T::non_finite(self.values.values(), self.validity_from(0))
    }
}

/// Whether the child positions `range` hold a value the non-finite bits mark.
fn holds_non_finite(non_finite: Option<&[u8]>, range: &Range<usize>) -> bool {
    let Some(bits) = non_finite else {
        return false;
    };
    RunValidity::new(Some(bits), range.start).count(range.len()) != 0
}

/// Records the rows a floating-point list statistic fails, under the statistic's name.
struct RowFailures<'a> {
    statistic: FloatListStatistic,
    row_errors: &'a mut RowErrors,
    span: Span,
}

impl<'a> RowFailures<'a> {
    fn new(statistic: FloatListStatistic, row_errors: &'a mut RowErrors, span: Span) -> Self {
        Self {
            statistic,
            row_errors,
            span,
        }
    }

    fn non_finite_element(&mut self, row: usize) {
        self.row_errors.push(
            row,
            SideError {
                reason: SideErrorReason::NonFiniteListElement(self.statistic),
                span: self.span,
            },
        );
    }

    fn non_finite_result(&mut self, row: usize) {
        self.row_errors.push(
            row,
            SideError {
                reason: SideErrorReason::NonFiniteResult(FloatOperation::ListStatistic(
                    self.statistic,
                )),
                span: self.span,
            },
        );
    }

    /// The finite value a row computed, or null with the row's error when it is not finite.
    fn finite(&mut self, row: usize, value: f64) -> Option<f64> {
        if value.is_finite() {
            return Some(value);
        }
        self.non_finite_result(row);
        None
    }
}

/// A variance or standard deviation of each list, from the centered moments the window statistics
/// compute: each value is read as the nearest `F64`, and every list is centered on its own mean.
fn dispersion_of<T: NumericElement>(
    list: ListColumn<'_>,
    dispersion: ListDispersion,
    mut failures: RowFailures<'_>,
) -> error_stack::Result<Float64Array, RuntimeError> {
    let elements = Elements::<T>::of(list)?;
    let non_finite = elements.non_finite();
    let mut output = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            output.push(None);
            continue;
        }
        let range = list.value_range(row);
        if holds_non_finite(non_finite.as_deref(), &range) {
            failures.non_finite_element(row);
            output.push(None);
            continue;
        }
        let moments = nervix_simd_kernels::moments(
            elements.slice(range.clone()),
            elements.validity_from(range.start),
            T::nearest,
        );
        let Some(variance) = moments.variance(dispersion.estimator()) else {
            output.push(None);
            continue;
        };
        output.push(failures.finite(row, dispersion.from_variance(variance)));
    }
    Ok(Float64Array::from(output))
}

/// The exact quantile of each list at the call's percentile, interpolated between the values at
/// the whole ranks around it as the window quantiles interpolate within their exact range.
fn quantile_of<T: NumericElement>(
    list: ListColumn<'_>,
    quantile: ListQuantile,
    mut failures: RowFailures<'_>,
) -> error_stack::Result<Float64Array, RuntimeError> {
    let elements = Elements::<T>::of(list)?;
    let non_finite = elements.non_finite();
    let percentile = quantile.percentile().value();
    // One list's present values at a time, so the column itself is never reordered.
    let mut present = Vec::new();
    let mut output = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            output.push(None);
            continue;
        }
        let range = list.value_range(row);
        if holds_non_finite(non_finite.as_deref(), &range) {
            failures.non_finite_element(row);
            output.push(None);
            continue;
        }
        present.clear();
        let validity = elements.validity_from(range.start);
        for (offset, value) in elements.slice(range).iter().enumerate() {
            if validity.contains(offset) {
                present.push(*value);
            }
        }
        let count = u64::try_from(present.len()).assured("a list holds fewer than 2^64 values");
        let Some(rank) = QuantileRank::new(percentile, count) else {
            output.push(None);
            continue;
        };
        let value = select_quantile::<T>(&mut present, rank);
        output.push(failures.finite(row, value));
    }
    Ok(Float64Array::from(output))
}

/// The quantile at `rank` among `values`, which it reorders. The value at the lower rank is
/// selected in place, and the value at the rank above it is the least of the values selection
/// left above it.
fn select_quantile<T: NumericElement>(values: &mut [T::Native], rank: QuantileRank) -> f64 {
    let lower_rank = list_rank(rank.lower());
    let upper_rank = list_rank(rank.upper());
    let (_, lower, above) =
        values.select_nth_unstable_by(lower_rank, |left, right| left.compare(*right));
    let lower = *lower;
    if upper_rank == lower_rank {
        return T::nearest(lower);
    }
    let upper = above
        .iter()
        .copied()
        .min_by(|left, right| left.compare(*right))
        .verified("an upper rank above the lower one lies inside the list");
    rank.interpolate(T::nearest(lower), T::nearest(upper))
}

/// A whole rank as a position within the list it ranks.
fn list_rank(rank: f64) -> usize {
    rank.checked_approx_into::<usize>().verified(
        "a list holds fewer than 2^31 values, so a percentile from 0 to 100 ranks it from zero to \
         its last position",
    )
}

/// The position within each row's list of its first least or greatest present element, counted
/// from zero as `nth` counts.
fn extreme_positions(
    list: ListColumn<'_>,
    extremum: ListExtremum,
    operation: &'static str,
) -> error_stack::Result<Int64Array, RuntimeError> {
    match list.element_data_type() {
        DataType::Boolean => {
            let values = list
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| collection_backing_mismatch(list.element_data_type()))?;
            Ok(first_extremes(list, extremum, values, |left, right| {
                values.value(left).cmp(&values.value(right))
            }))
        }
        DataType::Utf8 => {
            let values = list
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| collection_backing_mismatch(list.element_data_type()))?;
            Ok(first_extremes(list, extremum, values, |left, right| {
                values.value(left).cmp(values.value(right))
            }))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive_extremes::<TimestampNanosecondType>(list, extremum)
        }
        _ => with_numeric_element!(list, operation, Element => {
            primitive_extremes::<Element>(list, extremum)
        }),
    }
}

fn primitive_extremes<T: ArrowPrimitiveType>(
    list: ListColumn<'_>,
    extremum: ListExtremum,
) -> error_stack::Result<Int64Array, RuntimeError> {
    let elements = Elements::<T>::of(list)?;
    let values = elements.values.values();
    Ok(first_extremes(
        list,
        extremum,
        elements.values,
        |left, right| values[left].compare(values[right]),
    ))
}

/// Scans each row's list for the first present element no later element is strictly more extreme
/// than, comparing child positions with `compare`.
fn first_extremes(
    list: ListColumn<'_>,
    extremum: ListExtremum,
    values: &dyn Array,
    compare: impl Fn(usize, usize) -> Ordering,
) -> Int64Array {
    let mut positions = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            positions.push(None);
            continue;
        }
        // The extreme so far, as its position in the list and its child index.
        let mut extreme: Option<(usize, usize)> = None;
        for (position, index) in list.value_range(row).enumerate() {
            if values.is_null(index) {
                continue;
            }
            let replaces = match extreme {
                Some((_, current)) => extremum.replaces(compare(index, current)),
                None => true,
            };
            if replaces {
                extreme = Some((position, index));
            }
        }
        let Some((position, _)) = extreme else {
            positions.push(None);
            continue;
        };
        let position = i64::try_from(position)
            .assured("a list holds fewer than 2^31 values, so its positions fit I64");
        positions.push(Some(position));
    }
    Int64Array::from(positions)
}

#[cfg(test)]
#[path = "list_statistics_tests.rs"]
mod tests;
