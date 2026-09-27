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

    /// The present integer value at `row`, exactly.
    pub(super) fn integer_at(&self, row: usize) -> Option<i128> {
        if self.array.is_null(row) {
            return None;
        }
        let integer = match &self.values {
            ArgumentValues::UInt8(values) => Some(i128::from(values.value(row))),
            ArgumentValues::Int8(values) => Some(i128::from(values.value(row))),
            ArgumentValues::UInt16(values) => Some(i128::from(values.value(row))),
            ArgumentValues::Int16(values) => Some(i128::from(values.value(row))),
            ArgumentValues::UInt32(values) => Some(i128::from(values.value(row))),
            ArgumentValues::Int32(values) => Some(i128::from(values.value(row))),
            ArgumentValues::UInt64(values) => Some(i128::from(values.value(row))),
            ArgumentValues::Int64(values) => Some(i128::from(values.value(row))),
            ArgumentValues::Float32(_)
            | ArgumentValues::Float64(_)
            | ArgumentValues::Boolean(_)
            | ArgumentValues::Utf8(_)
            | ArgumentValues::Timestamp(_)
            | ArgumentValues::Passthrough => None,
        };
        Some(integer.verified(
            "integer sums are chosen only for integer arguments, and every argument column was \
             checked against its compiled type",
        ))
    }

    /// The present boolean value at `row`.
    pub(super) fn boolean_at(&self, row: usize) -> Option<bool> {
        if self.array.is_null(row) {
            return None;
        }
        let boolean = match &self.values {
            ArgumentValues::Boolean(values) => Some(values.value(row)),
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
            | ArgumentValues::Utf8(_)
            | ArgumentValues::Timestamp(_)
            | ArgumentValues::Passthrough => None,
        };
        Some(boolean.verified(
            "boolean counts compile only over BOOL arguments, and every argument column was \
             checked against its compiled type",
        ))
    }

    /// Whether the present value at `row` is a floating-point value that is not finite.
    fn is_non_finite(&self, row: usize) -> bool {
        if self.array.is_null(row) {
            return false;
        }
        match &self.values {
            ArgumentValues::Float32(values) => !values.value(row).is_finite(),
            ArgumentValues::Float64(values) => !values.value(row).is_finite(),
            ArgumentValues::UInt8(_)
            | ArgumentValues::Int8(_)
            | ArgumentValues::UInt16(_)
            | ArgumentValues::Int16(_)
            | ArgumentValues::UInt32(_)
            | ArgumentValues::Int32(_)
            | ArgumentValues::UInt64(_)
            | ArgumentValues::Int64(_)
            | ArgumentValues::Boolean(_)
            | ArgumentValues::Utf8(_)
            | ArgumentValues::Timestamp(_)
            | ArgumentValues::Passthrough => false,
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

    /// The function `row` is refused for, when a structure that reads its arguments as numbers
    /// would read a floating-point argument of the row that is not finite.
    pub(in crate::runtime) fn refused_function(
        &self,
        plan: &WindowAccumulatorPlan,
        row: usize,
    ) -> Option<WindowAggregateFunction> {
        for (arguments, demand) in self.demands.iter().zip(plan.demands()) {
            if !demand.storage.reads_finite_numbers() {
                continue;
            }
            for column in arguments.iter() {
                if column.is_non_finite(row) {
                    return demand.functions.first().copied();
                }
            }
        }
        None
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
}
