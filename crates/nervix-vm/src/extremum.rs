//! Scalar extrema: `greatest`, `least` and `clamp`, computed from comparison and validity bitmaps
//! and Arrow selection.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The order `greatest` and `least` select by, how their null arguments are skipped,
//!   the comparisons `clamp` bounds a value with, and which rows' bounds are invalid.
//! - **Depends on.** Arrow comparison and selection kernels and the numeric comparison the VM's
//!   operators apply.
//! - **Must not know.** Registers, instructions, programs, spans, or how a failed row is recorded.
//!
//! `greatest` and `least` order values the way the window `MIN` and `MAX` aggregates do: `BOOL`
//! orders `false` before `true`, `STRING` orders by bytes, and floating-point values order NaN
//! above every other value and treat both zeros as equal. Among equal values the earliest argument
//! is selected. `clamp` is a range operation instead, so it compares exactly as `<` and `>` do.

use arrow_array::{Array, ArrayRef, ArrowPrimitiveType, BooleanArray, PrimitiveArray};
use arrow_buffer::BooleanBuffer;
use arrow_ord::cmp::{gt, lt};
use arrow_schema::ArrowError;
use arrow_select::{nullif::nullif, zip::zip};

use crate::{
    batch::TypedArray,
    numeric::Comparison,
    operand::Operand,
    semantics::{ClassifiedFloat, FloatClass},
};

/// Which extreme of its arguments a call selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Extremum {
    /// `greatest`: the largest argument.
    Greatest,
    /// `least`: the smallest argument.
    Least,
}

impl Extremum {
    /// Selects, for every row, the extreme of the arguments that are present there.
    ///
    /// Null arguments are skipped, so a row is null only where every argument is. `first` is the
    /// running selection and each later argument replaces it only where it is present and strictly
    /// more extreme, or where nothing is selected yet, which keeps the earliest of equal values.
    pub(crate) fn select(
        self,
        first: &TypedArray,
        rest: &[Operand<'_, dyn Array>],
    ) -> Result<ArrayRef, ArrowError> {
        let mut selected = first.to_array_ref();
        for argument in rest {
            let rows = selected.len();
            let more_extreme = self.more_extreme(*argument, &selected)?;
            let argument_present = argument.validity(rows);
            let selected_present = Operand::Column(selected.as_ref()).validity(rows);
            let replaces = &(&more_extreme & &selected_present) | &!&selected_present;
            let takes_argument = BooleanArray::new(&replaces & &argument_present, None);
            selected = zip(&takes_argument, argument, &selected.as_ref())?;
        }
        Ok(selected)
    }

    /// Where `candidate` is strictly more extreme than `selected`. Rows where either is null hold
    /// no meaningful bit, since the caller masks them with both validities.
    fn more_extreme(
        self,
        candidate: Operand<'_, dyn Array>,
        selected: &ArrayRef,
    ) -> Result<BooleanBuffer, ArrowError> {
        if let Some(candidate) = candidate.downcast::<arrow_array::Float32Array>() {
            return self.more_extreme_float(candidate, selected);
        }
        if let Some(candidate) = candidate.downcast::<arrow_array::Float64Array>() {
            return self.more_extreme_float(candidate, selected);
        }
        let selected: &dyn Array = selected.as_ref();
        let compared = match self {
            Self::Greatest => gt(&candidate, &selected)?,
            Self::Least => lt(&candidate, &selected)?,
        };
        Ok(compared.values().clone())
    }

    /// Floats order NaN above every other value and treat both zeros as equal, the order the window
    /// `MIN` and `MAX` aggregates use. The Arrow comparison kernels order floats by the IEEE 754
    /// total order instead, which splits the zeros, so floats are compared here lane by lane.
    fn more_extreme_float<T>(
        self,
        candidate: Operand<'_, PrimitiveArray<T>>,
        selected: &ArrayRef,
    ) -> Result<BooleanBuffer, ArrowError>
    where
        T: ArrowPrimitiveType,
        T::Native: ClassifiedFloat + PartialOrd,
    {
        let selected = selected
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "extremum arguments must share one floating-point type".to_string(),
                )
            })?;
        let rows = selected.len();
        let current = &selected.values()[..rows];
        let candidates = candidate.array().values();
        let more_extreme = match self {
            Self::Greatest => BooleanBuffer::collect_bool(rows, |row| {
                let candidate = candidates[candidate.index(row)];
                let current = current[row];
                let current_is_nan = current.is_nan_value();
                (candidate.is_nan_value() && !current_is_nan)
                    || (!current_is_nan && candidate > current)
            }),
            Self::Least => BooleanBuffer::collect_bool(rows, |row| {
                let candidate = candidates[candidate.index(row)];
                let current = current[row];
                !candidate.is_nan_value() && (current.is_nan_value() || candidate < current)
            }),
        };
        Ok(more_extreme)
    }
}

/// Why a row's `clamp` bounds are invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum ClampBoundsDefect {
    /// The low bound is above the high bound.
    #[strum(to_string = "clamp lower bound is above its upper bound")]
    LowerAboveUpper,
    /// A floating-point bound is NaN, which bounds nothing.
    #[strum(to_string = "clamp bound is NaN")]
    NanBound,
}

/// What `clamp` computed for a batch: every row's value, and the rows whose bounds are invalid.
pub(crate) struct Clamped {
    /// The clamped values. A row whose bounds are invalid is null.
    pub(crate) values: ArrayRef,
    /// Rows whose bounds are present and invalid because a bound is NaN.
    pub(crate) nan_bound: BooleanBuffer,
    /// Rows whose bounds are present and numbers, and whose low bound is above the high bound.
    pub(crate) lower_above_upper: BooleanBuffer,
}

/// The comparisons one clamp is decided by, as bitmaps over the rows where all three arguments are
/// present.
struct ClampComparisons {
    below_low: BooleanBuffer,
    above_high: BooleanBuffer,
    lower_above_upper: BooleanBuffer,
    nan_bound: BooleanBuffer,
}

/// `clamp(value, low, high)`: the low bound where the value is below it, the high bound where the
/// value is above it, and the value otherwise, comparing as `<` and `>` do.
///
/// A null argument makes its row null. A row whose bounds are present but invalid, because the low
/// bound is above the high bound or a bound is NaN, is null and reported by the caller. A NaN value
/// is below and above nothing, so it is returned as it is.
pub(crate) fn clamp(
    value: &TypedArray,
    low: Operand<'_, dyn Array>,
    high: Operand<'_, dyn Array>,
) -> Result<Clamped, ArrowError> {
    let rows = value.len();
    let comparisons = match value {
        TypedArray::UInt8(values) => numeric_clamp_comparisons(values, low, high)?,
        TypedArray::Int8(values) => numeric_clamp_comparisons(values, low, high)?,
        TypedArray::UInt16(values) => numeric_clamp_comparisons(values, low, high)?,
        TypedArray::Int16(values) => numeric_clamp_comparisons(values, low, high)?,
        TypedArray::UInt32(values) => numeric_clamp_comparisons(values, low, high)?,
        TypedArray::Int32(values) => numeric_clamp_comparisons(values, low, high)?,
        TypedArray::UInt64(values) => numeric_clamp_comparisons(values, low, high)?,
        TypedArray::Int64(values) => numeric_clamp_comparisons(values, low, high)?,
        TypedArray::Float32(values) => float_clamp_comparisons(values, low, high)?,
        TypedArray::Float64(values) => float_clamp_comparisons(values, low, high)?,
        TypedArray::Utf8(_) | TypedArray::Datetime(_) => {
            let value: &dyn Array = value.as_array();
            ClampComparisons {
                below_low: lt(&value, &low)?.values().clone(),
                above_high: gt(&value, &high)?.values().clone(),
                lower_above_upper: per_row(&gt(&low, &high)?, rows),
                nan_bound: BooleanBuffer::new_unset(rows),
            }
        }
        TypedArray::Boolean(_) | TypedArray::Generic(_) | TypedArray::Uninitialized { .. } => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "clamp does not order values of type {}",
                value.data_type()
            )));
        }
    };

    let present = &(&Operand::Column(value.as_array()).validity(rows) & &low.validity(rows))
        & &high.validity(rows);
    let nan_bound = &comparisons.nan_bound & &present;
    let lower_above_upper = &(&comparisons.lower_above_upper & &present) & &!&nan_bound;
    let failed = &nan_bound | &lower_above_upper;

    let value: &dyn Array = value.as_array();
    let above = BooleanArray::new(comparisons.above_high, None);
    let bounded_above = zip(&above, &high, &value)?;
    let below = BooleanArray::new(comparisons.below_low, None);
    let bounded = zip(&below, &low, &bounded_above.as_ref())?;
    let absent = BooleanArray::new(&!&present | &failed, None);
    let values = nullif(bounded.as_ref(), &absent)?;
    Ok(Clamped {
        values,
        nan_bound,
        lower_above_upper,
    })
}

/// The clamp comparisons of an integer column, which every integer type orders naturally.
fn numeric_clamp_comparisons<T>(
    values: &PrimitiveArray<T>,
    low: Operand<'_, dyn Array>,
    high: Operand<'_, dyn Array>,
) -> Result<ClampComparisons, ArrowError>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd,
{
    let rows = values.len();
    let low = typed_bound::<T>(low)?;
    let high = typed_bound::<T>(high)?;
    let value = Operand::Column(values);
    Ok(ClampComparisons {
        below_low: Comparison::Lt.evaluate(value, low).values().clone(),
        above_high: Comparison::Gt.evaluate(value, high).values().clone(),
        lower_above_upper: per_row(&Comparison::Gt.evaluate(low, high), rows),
        nan_bound: BooleanBuffer::new_unset(rows),
    })
}

/// A comparison of the two bounds as a bitmap over the batch's `rows` rows. Two scalar bounds
/// compare once, and their one answer holds for every row.
fn per_row(compared: &BooleanArray, rows: usize) -> BooleanBuffer {
    if compared.len() == rows {
        return compared.values().clone();
    }
    if compared.is_valid(0) && compared.value(0) {
        return BooleanBuffer::new_set(rows);
    }
    BooleanBuffer::new_unset(rows)
}

/// The clamp comparisons of a float column, which compare by IEEE 754 as `<` and `>` do, and the
/// rows where a bound is NaN.
fn float_clamp_comparisons<T>(
    values: &PrimitiveArray<T>,
    low: Operand<'_, dyn Array>,
    high: Operand<'_, dyn Array>,
) -> Result<ClampComparisons, ArrowError>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd + ClassifiedFloat,
{
    let mut comparisons = numeric_clamp_comparisons(values, low, high)?;
    let rows = values.len();
    let low = typed_bound::<T>(low)?;
    let high = typed_bound::<T>(high)?;
    comparisons.nan_bound = BooleanBuffer::collect_bool(rows, |row| {
        let low = low.array().value(low.index(row));
        let high = high.array().value(high.index(row));
        FloatClass::Nan.contains(low) || FloatClass::Nan.contains(high)
    });
    Ok(comparisons)
}

/// A bound read as the value's own primitive type.
fn typed_bound<T: ArrowPrimitiveType>(
    bound: Operand<'_, dyn Array>,
) -> Result<Operand<'_, PrimitiveArray<T>>, ArrowError> {
    bound.downcast::<PrimitiveArray<T>>().ok_or_else(|| {
        ArrowError::InvalidArgumentError("clamp bounds must share the value's type".to_string())
    })
}

#[cfg(test)]
#[path = "extremum_tests.rs"]
mod tests;
