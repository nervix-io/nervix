//! The per-row aggregate arguments one evaluated input batch brings into a window.
//!
//! Layer: data plane.
//!
//! - **Owns.** Checking every evaluated argument column against the type its demand compiled to,
//!   reading a present value through that type, ordering values and keys, finding the rows whose
//!   floating-point arguments cannot be counted, and carrying argument values in a published window.
//! - **Depends on.** Arrow arrays, the compiled window demands, and runtime values at the snapshot
//!   boundary.
//! - **Must not know.** Which accumulator reads a column, or which rows a window retains.

use std::cmp::Ordering;

use arrow_array::{
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
    cast::AsArray as _,
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema, TimeUnit as ArrowTimeUnit};
use error_stack::ResultExt as _;
use nervix_simd_kernels::{RunCoMoments, RunMoments, RunSum, RunValidity};

use super::*;

/// One evaluated argument column, read through the type its demand compiled to.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ArgumentColumn {
    array: ArrayRef,
    values: ArgumentValues,
}

/// The typed view of an argument column's values.
#[derive(Debug, Clone)]
enum ArgumentValues {
    UInt8(UInt8Array),
    Int8(Int8Array),
    UInt16(UInt16Array),
    Int16(Int16Array),
    UInt32(UInt32Array),
    Int32(Int32Array),
    UInt64(UInt64Array),
    Int64(Int64Array),
    Float32(Float32Array),
    Float64(Float64Array),
    Boolean(BooleanArray),
    Utf8(StringArray),
    Timestamp(TimestampNanosecondArray),
    /// A value the window only passes through, such as an `ARRAY` or `VEC` read by `FIRST`.
    Passthrough,
}

/// One contiguous numeric argument run. Type selection happens once before a kernel sees rows.
pub(super) enum NumberRun<'a> {
    UInt8(&'a [u8], RunValidity<'a>),
    Int8(&'a [i8], RunValidity<'a>),
    UInt16(&'a [u16], RunValidity<'a>),
    Int16(&'a [i16], RunValidity<'a>),
    UInt32(&'a [u32], RunValidity<'a>),
    Int32(&'a [i32], RunValidity<'a>),
    UInt64(&'a [u64], RunValidity<'a>),
    Int64(&'a [i64], RunValidity<'a>),
    Float32(&'a [f32], RunValidity<'a>),
    Float64(&'a [f64], RunValidity<'a>),
}

macro_rules! number_run {
    ($run:expr, $function:ident $(, $argument:expr)*) => {
        match $run {
            NumberRun::UInt8(values, valid) => nervix_simd_kernels::$function(values, valid, |value| f64::from(value) $(, $argument)*),
            NumberRun::Int8(values, valid) => nervix_simd_kernels::$function(values, valid, |value| f64::from(value) $(, $argument)*),
            NumberRun::UInt16(values, valid) => nervix_simd_kernels::$function(values, valid, |value| f64::from(value) $(, $argument)*),
            NumberRun::Int16(values, valid) => nervix_simd_kernels::$function(values, valid, |value| f64::from(value) $(, $argument)*),
            NumberRun::UInt32(values, valid) => nervix_simd_kernels::$function(values, valid, |value| f64::from(value) $(, $argument)*),
            NumberRun::Int32(values, valid) => nervix_simd_kernels::$function(values, valid, |value| f64::from(value) $(, $argument)*),
            NumberRun::UInt64(values, valid) => nervix_simd_kernels::$function(values, valid, |value| value.approx_into() $(, $argument)*),
            NumberRun::Int64(values, valid) => nervix_simd_kernels::$function(values, valid, |value| value.approx_into() $(, $argument)*),
            NumberRun::Float32(values, valid) => nervix_simd_kernels::$function(values, valid, |value| f64::from(value) $(, $argument)*),
            NumberRun::Float64(values, valid) => nervix_simd_kernels::$function(values, valid, |value| value $(, $argument)*),
        }
    };
}

macro_rules! second_run {
    ($first:expr, $first_valid:expr, $first_value:expr, $second:expr) => {
        match $second {
            NumberRun::UInt8(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| f64::from(value),
            ),
            NumberRun::Int8(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| f64::from(value),
            ),
            NumberRun::UInt16(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| f64::from(value),
            ),
            NumberRun::Int16(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| f64::from(value),
            ),
            NumberRun::UInt32(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| f64::from(value),
            ),
            NumberRun::Int32(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| f64::from(value),
            ),
            NumberRun::UInt64(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| value.approx_into(),
            ),
            NumberRun::Int64(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| value.approx_into(),
            ),
            NumberRun::Float32(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| f64::from(value),
            ),
            NumberRun::Float64(values, valid) => nervix_simd_kernels::co_moments(
                $first,
                $first_valid,
                $first_value,
                values,
                valid,
                |value| value,
            ),
        }
    };
}

impl NumberRun<'_> {
    pub(super) fn extreme_offsets(self) -> Option<(usize, usize)> {
        match self {
            Self::UInt8(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
            Self::Int8(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
            Self::UInt16(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
            Self::Int16(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
            Self::UInt32(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
            Self::Int32(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
            Self::UInt64(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
            Self::Int64(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
            Self::Float32(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
            Self::Float64(values, valid) => {
                nervix_simd_kernels::min_max(values, valid).map(|(low, high)| (low.0, high.0))
            }
        }
    }

    pub(super) fn integer_sum(self) -> (u64, i128) {
        match self {
            Self::UInt8(values, valid) => nervix_simd_kernels::sum_integer(values, valid),
            Self::Int8(values, valid) => nervix_simd_kernels::sum_integer(values, valid),
            Self::UInt16(values, valid) => nervix_simd_kernels::sum_integer(values, valid),
            Self::Int16(values, valid) => nervix_simd_kernels::sum_integer(values, valid),
            Self::UInt32(values, valid) => nervix_simd_kernels::sum_integer(values, valid),
            Self::Int32(values, valid) => nervix_simd_kernels::sum_integer(values, valid),
            Self::UInt64(values, valid) => nervix_simd_kernels::sum_u64(values, valid),
            Self::Int64(values, valid) => nervix_simd_kernels::sum_i64(values, valid),
            Self::Float32(..) | Self::Float64(..) => {
                None.verified("integer sums are chosen only for integer arguments")
            }
        }
    }

    pub(super) fn compensated_sum(self) -> RunSum {
        number_run!(self, compensated_sum)
    }

    pub(super) fn moments(self) -> RunMoments {
        number_run!(self, moments)
    }

    pub(super) fn co_moments(self, second: NumberRun<'_>) -> RunCoMoments {
        match self {
            Self::UInt8(values, valid) => {
                second_run!(values, valid, f64::from, second)
            }
            Self::Int8(values, valid) => {
                second_run!(values, valid, f64::from, second)
            }
            Self::UInt16(values, valid) => {
                second_run!(values, valid, f64::from, second)
            }
            Self::Int16(values, valid) => {
                second_run!(values, valid, f64::from, second)
            }
            Self::UInt32(values, valid) => {
                second_run!(values, valid, f64::from, second)
            }
            Self::Int32(values, valid) => {
                second_run!(values, valid, f64::from, second)
            }
            Self::UInt64(values, valid) => {
                second_run!(values, valid, |value| value.approx_into(), second)
            }
            Self::Int64(values, valid) => {
                second_run!(values, valid, |value| value.approx_into(), second)
            }
            Self::Float32(values, valid) => {
                second_run!(values, valid, f64::from, second)
            }
            Self::Float64(values, valid) => second_run!(values, valid, |value| value, second),
        }
    }

    pub(super) fn bucket_indices(
        self,
        min: f64,
        max: f64,
        width: f64,
        buckets: usize,
    ) -> Vec<Option<usize>> {
        number_run!(self, bucket_indices, min, max, width, buckets)
    }

    pub(super) fn visit_reverse(self, visit: impl FnMut(Option<f64>)) {
        number_run!(self, reverse_values, visit)
    }
}

const DOWNCAST: &str = "the arm matched the array's own data type";
const NUMERIC_ARGUMENT: &str = "numeric structures compile only over numeric arguments, and every \
                                argument column was checked against its compiled type";
const SAME_ARGUMENT_TYPE: &str = "keys of one demand argument share the type they compiled to, \
                                  and every argument column was checked against it";

impl ArgumentColumn {
    fn new(array: ArrayRef) -> Self {
        let values = match array.data_type() {
            ArrowDataType::UInt8 => ArgumentValues::UInt8(
                array
                    .as_primitive_opt::<UInt8Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::Int8 => ArgumentValues::Int8(
                array
                    .as_primitive_opt::<Int8Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::UInt16 => ArgumentValues::UInt16(
                array
                    .as_primitive_opt::<UInt16Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::Int16 => ArgumentValues::Int16(
                array
                    .as_primitive_opt::<Int16Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::UInt32 => ArgumentValues::UInt32(
                array
                    .as_primitive_opt::<UInt32Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::Int32 => ArgumentValues::Int32(
                array
                    .as_primitive_opt::<Int32Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::UInt64 => ArgumentValues::UInt64(
                array
                    .as_primitive_opt::<UInt64Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::Int64 => ArgumentValues::Int64(
                array
                    .as_primitive_opt::<Int64Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::Float32 => ArgumentValues::Float32(
                array
                    .as_primitive_opt::<Float32Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::Float64 => ArgumentValues::Float64(
                array
                    .as_primitive_opt::<Float64Type>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            ArrowDataType::Boolean => {
                ArgumentValues::Boolean(array.as_boolean_opt().verified(DOWNCAST).clone())
            }
            ArrowDataType::Utf8 => {
                ArgumentValues::Utf8(array.as_string_opt::<i32>().verified(DOWNCAST).clone())
            }
            ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, _) => ArgumentValues::Timestamp(
                array
                    .as_primitive_opt::<TimestampNanosecondType>()
                    .verified(DOWNCAST)
                    .clone(),
            ),
            _ => ArgumentValues::Passthrough,
        };
        Self { array, values }
    }

    fn validity(&self, rows: &std::ops::Range<usize>) -> RunValidity<'_> {
        match self.array.nulls() {
            Some(nulls) => RunValidity::new(Some(nulls.validity()), nulls.offset() + rows.start),
            None => RunValidity::new(None, 0),
        }
    }

    /// The typed value slice and validity of one consecutive run.
    pub(super) fn number_run(&self, rows: std::ops::Range<usize>) -> NumberRun<'_> {
        let valid = self.validity(&rows);
        let run = match &self.values {
            ArgumentValues::UInt8(values) => {
                Some(NumberRun::UInt8(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::Int8(values) => {
                Some(NumberRun::Int8(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::UInt16(values) => {
                Some(NumberRun::UInt16(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::Int16(values) => {
                Some(NumberRun::Int16(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::UInt32(values) => {
                Some(NumberRun::UInt32(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::Int32(values) => {
                Some(NumberRun::Int32(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::UInt64(values) => {
                Some(NumberRun::UInt64(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::Int64(values) => {
                Some(NumberRun::Int64(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::Float32(values) => {
                Some(NumberRun::Float32(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::Float64(values) => {
                Some(NumberRun::Float64(&values.values()[rows.clone()], valid))
            }
            ArgumentValues::Boolean(_)
            | ArgumentValues::Utf8(_)
            | ArgumentValues::Timestamp(_)
            | ArgumentValues::Passthrough => None,
        };
        run.verified(NUMERIC_ARGUMENT)
    }

    /// Candidate positions of a numeric run's first minimum and maximum.
    pub(super) fn numeric_extreme_rows(
        &self,
        rows: std::ops::Range<usize>,
    ) -> Option<(usize, usize)> {
        if !matches!(
            &self.values,
            ArgumentValues::UInt8(_)
                | ArgumentValues::Int8(_)
                | ArgumentValues::UInt16(_)
                | ArgumentValues::Int16(_)
                | ArgumentValues::UInt32(_)
                | ArgumentValues::Int32(_)
                | ArgumentValues::UInt64(_)
                | ArgumentValues::Int64(_)
                | ArgumentValues::Float32(_)
                | ArgumentValues::Float64(_)
        ) {
            return None;
        }
        let (smallest, largest) = self.number_run(rows.clone()).extreme_offsets()?;
        Some((rows.start + smallest, rows.start + largest))
    }

    pub(super) fn boolean_counts(&self, rows: std::ops::Range<usize>) -> (u64, u64) {
        let valid = self.validity(&rows);
        let bits = match &self.values {
            ArgumentValues::Boolean(values) => Some(values.values()),
            _ => None,
        }
        .verified("truth counters compile only over BOOL arguments");
        nervix_simd_kernels::count_booleans(
            bits.values(),
            bits.offset() + rows.start,
            valid,
            rows.len(),
        )
    }

    pub(super) fn is_present(&self, row: usize) -> bool {
        self.array.is_valid(row)
    }

    /// The present numeric value at `row`, converted to the nearest F64.
    pub(super) fn number_at(&self, row: usize) -> Option<f64> {
        if self.array.is_null(row) {
            return None;
        }
        let number = match &self.values {
            ArgumentValues::UInt8(values) => Some(f64::from(values.value(row))),
            ArgumentValues::Int8(values) => Some(f64::from(values.value(row))),
            ArgumentValues::UInt16(values) => Some(f64::from(values.value(row))),
            ArgumentValues::Int16(values) => Some(f64::from(values.value(row))),
            ArgumentValues::UInt32(values) => Some(f64::from(values.value(row))),
            ArgumentValues::Int32(values) => Some(f64::from(values.value(row))),
            ArgumentValues::UInt64(values) => Some(values.value(row).approx_into()),
            ArgumentValues::Int64(values) => Some(values.value(row).approx_into()),
            ArgumentValues::Float32(values) => Some(f64::from(values.value(row))),
            ArgumentValues::Float64(values) => Some(values.value(row)),
            ArgumentValues::Boolean(_)
            | ArgumentValues::Utf8(_)
            | ArgumentValues::Timestamp(_)
            | ArgumentValues::Passthrough => None,
        };
        Some(number.verified(NUMERIC_ARGUMENT))
    }

    /// The non-finite bitmap of one typed run, if its type is floating point.
    fn non_finite_run(&self, rows: std::ops::Range<usize>) -> Option<Vec<u8>> {
        let valid = self.validity(&rows);
        match &self.values {
            ArgumentValues::Float32(values) => Some(nervix_simd_kernels::non_finite_f32(
                &values.values()[rows],
                valid,
            )),
            ArgumentValues::Float64(values) => Some(nervix_simd_kernels::non_finite_f64(
                &values.values()[rows],
                valid,
            )),
            _ => None,
        }
    }

    /// Order the present value at `row` against the present value at `other_row` of `other`, a
    /// column of the same demand argument. `false` orders before `true`, strings order by their
    /// bytes, and floating-point values order NaN above every other value and both zeros as equal.
    pub(super) fn compare(&self, row: usize, other: &Self, other_row: usize) -> Ordering {
        let ordering = match (&self.values, &other.values) {
            (ArgumentValues::UInt8(left), ArgumentValues::UInt8(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (ArgumentValues::Int8(left), ArgumentValues::Int8(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (ArgumentValues::UInt16(left), ArgumentValues::UInt16(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (ArgumentValues::Int16(left), ArgumentValues::Int16(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (ArgumentValues::UInt32(left), ArgumentValues::UInt32(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (ArgumentValues::Int32(left), ArgumentValues::Int32(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (ArgumentValues::UInt64(left), ArgumentValues::UInt64(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (ArgumentValues::Int64(left), ArgumentValues::Int64(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (ArgumentValues::Float32(left), ArgumentValues::Float32(right)) => {
                Some(OrderedFloat(left.value(row)).cmp(&OrderedFloat(right.value(other_row))))
            }
            (ArgumentValues::Float64(left), ArgumentValues::Float64(right)) => {
                Some(OrderedFloat(left.value(row)).cmp(&OrderedFloat(right.value(other_row))))
            }
            (ArgumentValues::Boolean(left), ArgumentValues::Boolean(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (ArgumentValues::Utf8(left), ArgumentValues::Utf8(right)) => {
                Some(left.value(row).cmp(right.value(other_row)))
            }
            (ArgumentValues::Timestamp(left), ArgumentValues::Timestamp(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            _ => None,
        };
        ordering.verified(SAME_ARGUMENT_TYPE)
    }

    /// The value at `row` as a one-row array of the column's own type.
    pub(super) fn slice(&self, row: usize) -> ArrayRef {
        self.array.slice(row, 1)
    }

    /// A stable, type-separated byte key for a present sketch input. Floating-point zero has
    /// one encoding regardless of sign. Non-finite values are refused before admission.
    pub(super) fn sketch_key(&self, row: usize) -> Option<Vec<u8>> {
        if !self.is_present(row) {
            return None;
        }
        let mut key = Vec::new();
        match &self.values {
            ArgumentValues::UInt8(values) => {
                key.push(1);
                key.push(values.value(row));
            }
            ArgumentValues::Int8(values) => {
                key.push(2);
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            ArgumentValues::UInt16(values) => {
                key.push(3);
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            ArgumentValues::Int16(values) => {
                key.push(4);
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            ArgumentValues::UInt32(values) => {
                key.push(5);
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            ArgumentValues::Int32(values) => {
                key.push(6);
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            ArgumentValues::UInt64(values) => {
                key.push(7);
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            ArgumentValues::Int64(values) => {
                key.push(8);
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            ArgumentValues::Float32(values) => {
                key.push(9);
                key.extend_from_slice(
                    &if values.value(row) == 0.0 {
                        0.0_f32
                    } else {
                        values.value(row)
                    }
                    .to_bits()
                    .to_le_bytes(),
                );
            }
            ArgumentValues::Float64(values) => {
                key.push(10);
                key.extend_from_slice(
                    &if values.value(row) == 0.0 {
                        0.0_f64
                    } else {
                        values.value(row)
                    }
                    .to_bits()
                    .to_le_bytes(),
                );
            }
            ArgumentValues::Boolean(values) => {
                key.push(11);
                key.push(u8::from(values.value(row)));
            }
            ArgumentValues::Utf8(values) => {
                key.push(12);
                key.extend_from_slice(values.value(row).as_bytes());
            }
            ArgumentValues::Timestamp(values) => {
                key.push(13);
                key.extend_from_slice(&values.value(row).to_le_bytes());
            }
            ArgumentValues::Passthrough => return None,
        }
        Some(key)
    }

    /// Visit present typed sketch keys from one run with a reusable byte buffer. A frequency
    /// sketch copies a key only if it must retain that distinct candidate.
    pub(super) fn visit_sketch_keys(
        &self,
        rows: std::ops::Range<usize>,
        mut visit: impl FnMut(usize, &[u8]),
    ) {
        let valid = self.validity(&rows);
        let mut key = Vec::with_capacity(16);
        macro_rules! fixed_keys {
            ($values:expr, $tag:expr, $bytes:expr) => {
                for row in rows.clone() {
                    if !valid.contains(row - rows.start) {
                        continue;
                    }
                    key.clear();
                    key.push($tag);
                    key.extend_from_slice(&($bytes)($values.value(row)));
                    visit(row, &key);
                }
            };
        }
        match &self.values {
            ArgumentValues::UInt8(values) => {
                fixed_keys!(values, 1, |value: u8| value.to_le_bytes())
            }
            ArgumentValues::Int8(values) => fixed_keys!(values, 2, |value: i8| value.to_le_bytes()),
            ArgumentValues::UInt16(values) => {
                fixed_keys!(values, 3, |value: u16| value.to_le_bytes())
            }
            ArgumentValues::Int16(values) => {
                fixed_keys!(values, 4, |value: i16| value.to_le_bytes())
            }
            ArgumentValues::UInt32(values) => {
                fixed_keys!(values, 5, |value: u32| value.to_le_bytes())
            }
            ArgumentValues::Int32(values) => {
                fixed_keys!(values, 6, |value: i32| value.to_le_bytes())
            }
            ArgumentValues::UInt64(values) => {
                fixed_keys!(values, 7, |value: u64| value.to_le_bytes())
            }
            ArgumentValues::Int64(values) => {
                fixed_keys!(values, 8, |value: i64| value.to_le_bytes())
            }
            ArgumentValues::Float32(values) => {
                fixed_keys!(values, 9, |value: f32| if value == 0.0 {
                    0.0_f32.to_bits().to_le_bytes()
                } else {
                    value.to_bits().to_le_bytes()
                })
            }
            ArgumentValues::Float64(values) => {
                fixed_keys!(values, 10, |value: f64| if value == 0.0 {
                    0.0_f64.to_bits().to_le_bytes()
                } else {
                    value.to_bits().to_le_bytes()
                })
            }
            ArgumentValues::Boolean(values) => {
                for row in rows.clone() {
                    if !valid.contains(row - rows.start) {
                        continue;
                    }
                    key.clear();
                    key.push(11);
                    key.push(u8::from(values.value(row)));
                    visit(row, &key);
                }
            }
            ArgumentValues::Utf8(values) => {
                for row in rows.clone() {
                    if !valid.contains(row - rows.start) {
                        continue;
                    }
                    key.clear();
                    key.push(12);
                    key.extend_from_slice(values.value(row).as_bytes());
                    visit(row, &key);
                }
            }
            ArgumentValues::Timestamp(values) => {
                fixed_keys!(values, 13, |value: i64| value.to_le_bytes())
            }
            ArgumentValues::Passthrough => {}
        }
    }
}

/// The argument columns one evaluated input batch produced, one entry per demand of the window.
#[derive(Debug)]
pub(in crate::runtime) struct WindowArgumentColumns {
    demands: Vec<WindowArguments<ArgumentColumn>>,
    rows: usize,
}

impl WindowArgumentColumns {
    /// Share the evaluated Arrow columns as a batch for a window snapshot. This does not extract
    /// per-row scalar values or rebuild columns already held by the live argument batch.
    pub(in crate::runtime) fn snapshot_batch(
        &self,
    ) -> error_stack::Result<RuntimeRecordBatch, WindowProcessorError> {
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for demand in &self.demands {
            for column in demand.iter() {
                fields.push(ArrowField::new(
                    format!("argument_{}", fields.len()),
                    column.array.data_type().clone(),
                    true,
                ));
                columns.push(column.array.clone());
            }
        }
        let schema = StdArc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(self.rows)),
        )
        .map_err(|error| {
            Report::new(WindowProcessorError::EncodeSnapshotEntry).attach_printable(error)
        })?;
        RuntimeRecordBatch::from_record_batch(schema, batch)
            .change_context(WindowProcessorError::EncodeSnapshotEntry)
    }

    /// The column layout a saved argument batch must declare for this compiled window plan.
    pub(in crate::runtime) fn snapshot_schema(plan: &WindowAccumulatorPlan) -> StdArc<ArrowSchema> {
        let mut fields = Vec::new();
        for demand in plan.demands() {
            for column in demand.arguments.iter() {
                fields.push(ArrowField::new(
                    format!("argument_{}", fields.len()),
                    column.data_type.clone(),
                    true,
                ));
            }
        }
        StdArc::new(ArrowSchema::new(fields))
    }

    /// Reuse the decoded Arrow columns directly as one validated argument batch.
    pub(in crate::runtime) fn from_snapshot_batch(
        plan: &WindowAccumulatorPlan,
        batch: &RuntimeRecordBatch,
    ) -> error_stack::Result<Self, WindowProcessorError> {
        if batch.schema() != Self::snapshot_schema(plan) {
            return Err(Report::new(WindowProcessorError::SnapshotArgumentSchema));
        }
        let mut slot = 0;
        let mut arrays = Vec::with_capacity(plan.demands().len());
        for demand in plan.demands() {
            let first = batch.batch().column(slot).clone();
            slot = slot
                .checked_add(1)
                .assured("the snapshot schema has one column for every compiled argument");
            let arguments = match demand.arguments.second() {
                None => WindowArguments::Single(first),
                Some(_) => {
                    let second = batch.batch().column(slot).clone();
                    slot = slot
                        .checked_add(1)
                        .assured("the snapshot schema has one column for every compiled argument");
                    WindowArguments::Pair { first, second }
                }
            };
            arrays.push(arguments);
        }
        Self::new(plan, arrays, batch.batch().num_rows())
    }

    /// Bound each demand's Arrow storage and one typed-key copy per value. Each top-k demand gets
    /// its own charge, even when demands project the same input. The row allowance also covers a
    /// snapshot rebuild as one value per retained row.
    pub(in crate::runtime) fn allocated_bytes(&self) -> u128 {
        let mut bytes = 0_u128;
        for demand in &self.demands {
            for column in demand.iter() {
                let actual = u128::try_from(column.array.get_array_memory_size())
                    .assured("an addressable Arrow array allocation fits u128");
                let logical = u128::try_from(
                    column
                        .array
                        .to_data()
                        .get_slice_memory_size()
                        .assured("a validated Arrow argument has bounded slice memory"),
                )
                .assured("an addressable Arrow slice fits u128");
                let rows = u128::try_from(column.array.len())
                    .assured("an addressable Arrow array length fits u128");
                let payload = logical
                    .checked_mul(2)
                    .assured("an addressable Arrow slice's doubled size fits u128");
                let headers = rows
                    .checked_mul(32)
                    .assured("an addressable Arrow array's row headers fit u128");
                let charge = actual.max(
                    payload
                        .checked_add(headers)
                        .assured("an addressable Arrow array and row headers fit u128"),
                );
                bytes = bytes
                    .checked_add(charge)
                    .assured("addressable argument arrays fit a u128 byte allowance");
            }
        }
        bytes
    }

    /// Check the arrays one batch of `rows` rows evaluated to, one entry per demand of `plan` in
    /// plan order, against the types the demands compiled to.
    pub(in crate::runtime) fn new(
        plan: &WindowAccumulatorPlan,
        arrays: Vec<WindowArguments<ArrayRef>>,
        rows: usize,
    ) -> error_stack::Result<Self, WindowProcessorError> {
        if arrays.len() != plan.demands().len() {
            return Err(Report::new(WindowProcessorError::ArgumentDemandCount {
                evaluated: arrays.len(),
                demands: plan.demands().len(),
            }));
        }
        let mut demands = Vec::with_capacity(arrays.len());
        for (demand, (arrays, compiled)) in arrays.into_iter().zip(plan.demands()).enumerate() {
            let columns = match (arrays, &compiled.arguments) {
                (WindowArguments::Single(array), WindowArguments::Single(column)) => {
                    WindowArguments::Single(Self::checked(demand, array, column, rows)?)
                }
                (
                    WindowArguments::Pair { first, second },
                    WindowArguments::Pair {
                        first: first_column,
                        second: second_column,
                    },
                ) => WindowArguments::Pair {
                    first: Self::checked(demand, first, first_column, rows)?,
                    second: Self::checked(demand, second, second_column, rows)?,
                },
                _ => {
                    return Err(Report::new(WindowProcessorError::ArgumentShape { demand }));
                }
            };
            demands.push(columns);
        }
        Ok(Self { demands, rows })
    }

    fn checked(
        demand: usize,
        array: ArrayRef,
        column: &WindowArgumentColumn,
        rows: usize,
    ) -> error_stack::Result<ArgumentColumn, WindowProcessorError> {
        if array.data_type() != &column.data_type || array.len() != rows {
            return Err(Report::new(WindowProcessorError::ArgumentColumnType {
                demand,
                field: column.field.clone(),
                expected: column.data_type.clone(),
                found: array.data_type().clone(),
            }));
        }
        Ok(ArgumentColumn::new(array))
    }

    /// The argument columns of the demand at index `demand`.
    pub(super) fn demand(&self, demand: usize) -> &WindowArguments<ArgumentColumn> {
        self.demands
            .get(demand)
            .verified("every argument batch holds one entry per demand of the window's plan")
    }

    /// The first function refusing each row whose finite-numeric structure sees a non-finite
    /// argument. Each typed column is classified once, then its bitmap marks affected rows.
    pub(in crate::runtime) fn refused_functions(
        &self,
        plan: &WindowAccumulatorPlan,
        rows: usize,
    ) -> Vec<Option<WindowAggregateFunction>> {
        let mut refused = vec![None; rows];
        for (arguments, demand) in self.demands.iter().zip(plan.demands()) {
            if !demand.storage.reads_finite_numbers() {
                continue;
            }
            for column in arguments.iter() {
                let Some(bits) = column.non_finite_run(0..rows) else {
                    continue;
                };
                for (byte, mask) in bits.into_iter().enumerate() {
                    let mut remaining = mask;
                    while remaining != 0 {
                        let bit = usize::try_from(remaining.trailing_zeros())
                            .assured("a byte has at most eight bits");
                        let row = byte * 8 + bit;
                        if refused[row].is_none() {
                            refused[row] = demand.functions.first().copied();
                        }
                        remaining &= remaining - 1;
                    }
                }
            }
        }
        refused
    }
}

#[cfg(test)]
mod sketch_key_tests {
    use super::*;

    #[test]
    fn sketch_keys_separate_types_normalize_zero_and_ignore_null() {
        let signed = ArgumentColumn::new(StdArc::new(Int64Array::from(vec![Some(7), None])));
        let unsigned = ArgumentColumn::new(StdArc::new(UInt64Array::from(vec![7_u64])));
        let text = ArgumentColumn::new(StdArc::new(StringArray::from(vec!["7"])));
        assert_ne!(signed.sketch_key(0), unsigned.sketch_key(0));
        assert_ne!(signed.sketch_key(0), text.sketch_key(0));
        assert_eq!(signed.sketch_key(1), None);

        let zeros = ArgumentColumn::new(StdArc::new(Float64Array::from(vec![0.0, -0.0])));
        assert_eq!(zeros.sketch_key(0), zeros.sketch_key(1));
    }

    #[test]
    fn typed_run_sketch_keys_match_scalar_keys_for_every_supported_type() {
        let columns: Vec<ArrayRef> = vec![
            StdArc::new(UInt8Array::from(vec![Some(7), None, Some(8)])),
            StdArc::new(Int8Array::from(vec![Some(-7), None, Some(8)])),
            StdArc::new(UInt16Array::from(vec![Some(7), None, Some(8)])),
            StdArc::new(Int16Array::from(vec![Some(-7), None, Some(8)])),
            StdArc::new(UInt32Array::from(vec![Some(7), None, Some(8)])),
            StdArc::new(Int32Array::from(vec![Some(-7), None, Some(8)])),
            StdArc::new(UInt64Array::from(vec![Some(7), None, Some(8)])),
            StdArc::new(Int64Array::from(vec![Some(-7), None, Some(8)])),
            StdArc::new(Float32Array::from(vec![Some(-0.0), None, Some(8.0)])),
            StdArc::new(Float64Array::from(vec![Some(-0.0), None, Some(8.0)])),
            StdArc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            StdArc::new(StringArray::from(vec![Some("seven"), None, Some("eight")])),
            StdArc::new(TimestampNanosecondArray::from(vec![Some(7), None, Some(8)])),
        ];
        for array in columns {
            let column = ArgumentColumn::new(array);
            let mut keys = Vec::new();
            column.visit_sketch_keys(0..3, |row, key| keys.push((row, key.to_vec())));
            let expected = (0..3)
                .filter_map(|row| column.sketch_key(row).map(|key| (row, key)))
                .collect::<Vec<_>>();
            assert_eq!(keys, expected);
        }
    }

    #[test]
    fn typed_runs_honor_arrow_slice_offsets_and_nullable_subranges() {
        let numbers = ArgumentColumn::new(StdArc::new(
            Int64Array::from(vec![Some(99), Some(5), None, Some(-3), Some(8), Some(99)])
                .slice(1, 4),
        ));
        assert_eq!(numbers.number_run(1..4).integer_sum(), (2, 5));
        assert_eq!(numbers.numeric_extreme_rows(1..4), Some((2, 3)));

        let booleans = ArgumentColumn::new(StdArc::new(
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                None,
                Some(false),
                Some(true),
                Some(false),
            ])
            .slice(1, 4),
        ));
        assert_eq!(booleans.boolean_counts(1..4), (1, 1));
    }

    #[test]
    fn numeric_run_dispatch_preserves_nullable_values_for_every_type_pair() {
        let arrays: Vec<ArrayRef> = vec![
            StdArc::new(UInt8Array::from(vec![Some(1), None, Some(3)])),
            StdArc::new(Int8Array::from(vec![Some(1), None, Some(3)])),
            StdArc::new(UInt16Array::from(vec![Some(1), None, Some(3)])),
            StdArc::new(Int16Array::from(vec![Some(1), None, Some(3)])),
            StdArc::new(UInt32Array::from(vec![Some(1), None, Some(3)])),
            StdArc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            StdArc::new(UInt64Array::from(vec![Some(1), None, Some(3)])),
            StdArc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            StdArc::new(Float32Array::from(vec![Some(1.0), None, Some(3.0)])),
            StdArc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
        ];
        let columns = arrays
            .into_iter()
            .map(ArgumentColumn::new)
            .collect::<Vec<_>>();
        for column in &columns {
            assert_eq!(column.number_run(0..3).extreme_offsets(), Some((0, 2)));
            let sum = column.number_run(0..3).compensated_sum();
            assert_eq!((sum.count, sum.sum + sum.compensation), (2, 4.0));
            let moments = column.number_run(0..3).moments();
            assert_eq!(
                (moments.count, moments.mean, moments.squares),
                (2, 2.0, 2.0)
            );
            assert_eq!(
                column.number_run(0..3).bucket_indices(0.0, 4.0, 1.0, 4),
                vec![Some(1), None, Some(3)]
            );
            let mut reversed = Vec::new();
            column
                .number_run(0..3)
                .visit_reverse(|value| reversed.push(value));
            assert_eq!(reversed, vec![Some(3.0), None, Some(1.0)]);
            if IntegerSum::reads(column.array.data_type()) {
                assert_eq!(column.number_run(0..3).integer_sum(), (2, 4));
            }
            for second in &columns {
                let paired = column.number_run(0..3).co_moments(second.number_run(0..3));
                assert_eq!(paired.count, 2);
                assert_eq!((paired.first_mean, paired.second_mean), (2.0, 2.0));
                assert_eq!(
                    (
                        paired.first_squares,
                        paired.second_squares,
                        paired.cross_products
                    ),
                    (2.0, 2.0, 2.0)
                );
            }
        }
    }
}
