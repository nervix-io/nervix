//! Window sums: exact over integers, compensated over floating point.
//!
//! Layer: data plane.
//!
//! - **Owns.** The exact integer sum a window retracts by subtraction, the compensated
//!   floating-point sum a window only merges, and the `SUM` result each produces in the argument's
//!   own type.
//! - **Depends on.** The argument columns of admitted rows.
//! - **Must not know.** Which rows a window retains, or when it emits.

use std::ops::Range;

use arrow_array::{
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array,
    UInt16Array, UInt32Array,
};

use super::{two_stacks::MergeableAggregate, *};

/// The exact sum of the present values of an integer argument.
#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct IntegerSum {
    values: u64,
    sum: i128,
}

const SUM_BOUND: &str =
    "a window cannot retain 2^63 rows in memory, so a sum of 64-bit values stays below 2^127";

impl IntegerSum {
    /// Whether `SUM` over an argument of `data_type` is exact integer summation.
    pub(super) fn reads(data_type: &ArrowDataType) -> bool {
        matches!(
            data_type,
            ArrowDataType::UInt8
                | ArrowDataType::Int8
                | ArrowDataType::UInt16
                | ArrowDataType::Int16
                | ArrowDataType::UInt32
                | ArrowDataType::Int32
                | ArrowDataType::UInt64
                | ArrowDataType::Int64
        )
    }

    pub(super) fn admit(&mut self, column: &ArgumentColumn, rows: Range<usize>) {
        for row in rows {
            let Some(value) = column.integer_at(row) else {
                continue;
            };
            self.values = self
                .values
                .checked_add(1)
                .assured("a window cannot retain 2^64 rows in memory");
            self.sum = self.sum.checked_add(value).assured(SUM_BOUND);
        }
    }

    pub(super) fn retract(&mut self, column: &ArgumentColumn, rows: Range<usize>) {
        for row in rows {
            let Some(value) = column.integer_at(row) else {
                continue;
            };
            self.values = self
                .values
                .checked_sub(1)
                .verified("a retracted value was counted when the window admitted it");
            self.sum = self.sum.checked_sub(value).assured(SUM_BOUND);
        }
    }

    /// `SUM` in the argument's own integer type, which is null when no row contributed and an
    /// error when the sum does not fit that type.
    pub(super) fn evaluate(
        &self,
        output_type: &ArrowDataType,
    ) -> error_stack::Result<ArrayRef, WindowProcessorError> {
        if self.values == 0 {
            return Ok(new_null_array(output_type, 1));
        }
        let sum = self.sum;
        let array: Option<ArrayRef> = match output_type {
            ArrowDataType::UInt8 => Some(StdArc::new(UInt8Array::from(vec![
                u8::try_from(sum).map_err(|_| Self::overflow(output_type))?,
            ]))),
            ArrowDataType::Int8 => Some(StdArc::new(Int8Array::from(vec![
                i8::try_from(sum).map_err(|_| Self::overflow(output_type))?,
            ]))),
            ArrowDataType::UInt16 => Some(StdArc::new(UInt16Array::from(vec![
                u16::try_from(sum).map_err(|_| Self::overflow(output_type))?,
            ]))),
            ArrowDataType::Int16 => Some(StdArc::new(Int16Array::from(vec![
                i16::try_from(sum).map_err(|_| Self::overflow(output_type))?,
            ]))),
            ArrowDataType::UInt32 => Some(StdArc::new(UInt32Array::from(vec![
                u32::try_from(sum).map_err(|_| Self::overflow(output_type))?,
            ]))),
            ArrowDataType::Int32 => Some(StdArc::new(Int32Array::from(vec![
                i32::try_from(sum).map_err(|_| Self::overflow(output_type))?,
            ]))),
            ArrowDataType::UInt64 => Some(StdArc::new(UInt64Array::from(vec![
                u64::try_from(sum).map_err(|_| Self::overflow(output_type))?,
            ]))),
            ArrowDataType::Int64 => Some(StdArc::new(Int64Array::from(vec![
                i64::try_from(sum).map_err(|_| Self::overflow(output_type))?,
            ]))),
            _ => None,
        };
        Ok(array.verified(
            "an integer sum is chosen only for an integer argument, and SUM yields its argument's \
             type",
        ))
    }

    fn overflow(output_type: &ArrowDataType) -> Report<WindowProcessorError> {
        Report::new(WindowProcessorError::SumOverflow {
            data_type: output_type.clone(),
        })
    }
}

/// The sum of the present values of a floating-point argument, with the rounding error of every
/// addition carried beside it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::runtime) struct CompensatedSum {
    values: u64,
    sum: f64,
    /// The exact rounding error of every addition that produced `sum`.
    compensation: f64,
}

/// The rounded sum of two values and the exact error that rounding introduced.
struct TwoSum {
    sum: f64,
    error: f64,
}

impl TwoSum {
    /// Knuth's branch-free two-sum: `sum + error` equals `left + right` exactly whenever `sum` is
    /// finite.
    fn of(left: f64, right: f64) -> Self {
        let sum = left + right;
        let right_part = sum - left;
        let left_part = sum - right_part;
        let error = (left - left_part) + (right - right_part);
        Self { sum, error }
    }
}

impl MergeableAggregate for CompensatedSum {
    const EMPTY: Self = Self {
        values: 0,
        sum: 0.0,
        compensation: 0.0,
    };

    fn merge(older: Self, newer: Self) -> Self {
        if newer.values == 0 {
            return older;
        }
        if older.values == 0 {
            return newer;
        }
        let TwoSum { sum, error } = TwoSum::of(older.sum, newer.sum);
        Self {
            values: older
                .values
                .checked_add(newer.values)
                .assured("a window cannot retain 2^64 rows in memory"),
            sum,
            compensation: older.compensation + newer.compensation + error,
        }
    }
}

impl CompensatedSum {
    pub(super) fn of_row(column: &ArgumentColumn, row: usize) -> Self {
        match column.number_at(row) {
            Some(value) => Self {
                values: 1,
                sum: value,
                compensation: 0.0,
            },
            None => Self::EMPTY,
        }
    }

    pub(super) fn of_rows(column: &ArgumentColumn, rows: Range<usize>) -> Self {
        let mut sum = Self::EMPTY;
        for row in rows {
            sum = Self::merge(sum, Self::of_row(column, row));
        }
        sum
    }

    /// `SUM` in the argument's own floating-point type, which is null when no row contributed and
    /// an error when the sum is not finite in that type.
    pub(super) fn evaluate(
        &self,
        output_type: &ArrowDataType,
    ) -> error_stack::Result<ArrayRef, WindowProcessorError> {
        if self.values == 0 {
            return Ok(new_null_array(output_type, 1));
        }
        let total = self.sum + self.compensation;
        let overflow = Report::new(WindowProcessorError::SumOverflow {
            data_type: output_type.clone(),
        });
        let array: Option<ArrayRef> = match output_type {
            ArrowDataType::Float64 => {
                if !total.is_finite() {
                    return Err(overflow);
                }
                Some(StdArc::new(Float64Array::from(vec![total])))
            }
            ArrowDataType::Float32 => {
                let narrowed: f32 = total.approx_into();
                if !narrowed.is_finite() {
                    return Err(overflow);
                }
                Some(StdArc::new(Float32Array::from(vec![narrowed])))
            }
            _ => None,
        };
        Ok(array.verified(
            "a compensated sum is chosen only for a floating-point argument, and SUM yields its \
             argument's type",
        ))
    }
}
