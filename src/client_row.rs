//! Shared Arrow-column encoding at the native client boundary.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Exact typed column validation and writing values and redacted cells into Row frames.
//! - **Depends on.** Arrow arrays, runtime-schema values, schema vocabulary and the wire cell writer.
//! - **Must not know.** Subscriptions, sessions, runtime branches, graph placement or state ownership.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns shared Arrow-to-Row encoding")
)]

use arrow_array::{
    Array, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType as ArrowDataType;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{CellWriter, RowSchema, WireEncodeError};
use nervix_models::{BranchName, FieldName, ParseAsType, SchemaField, Timestamp};
use thiserror::Error;

use crate::runtime_schema::{RuntimeValue, RuntimeValueKind, parse_as_type_from_arrow};

pub(crate) struct ArrowRowBatch<'a> {
    columns: Vec<ArrowFieldColumn<'a>>,
}

impl<'a> ArrowRowBatch<'a> {
    pub(crate) fn new(
        schema: &'a RowSchema,
        batch: &'a RecordBatch,
    ) -> Result<Self, Report<SubscriptionRowEncodingError>> {
        if schema.fields.len() != batch.num_columns() {
            return Err(Report::new(SubscriptionRowEncodingError::ColumnCount {
                schema_fields: schema.fields.len(),
                arrow_columns: batch.num_columns(),
            }));
        }
        let arrow_fields = batch.schema_ref().fields();
        let mut columns = Vec::with_capacity(schema.fields.len());
        for index in 0..schema.fields.len() {
            let field = &schema.fields[index];
            let arrow_field = arrow_fields
                .get(index)
                .verified("the Arrow batch has one schema field per column");
            if field.name.as_str() != arrow_field.name() {
                return Err(Report::new(SubscriptionRowEncodingError::FieldName {
                    column: index,
                    expected: field.name.clone(),
                    actual: arrow_field.name().clone(),
                }));
            }
            if field.optional != arrow_field.is_nullable() {
                return Err(Report::new(SubscriptionRowEncodingError::Nullability {
                    field: field.name.clone(),
                    expected_nullable: field.optional,
                    actual_nullable: arrow_field.is_nullable(),
                }));
            }
            let array = batch
                .columns()
                .get(index)
                .verified("the Arrow batch reported this column count");
            let values = ArrowColumn::new(&field.name, &field.ty, array.as_ref())?;
            if !field.optional && array.null_count() > 0 {
                return Err(Report::new(
                    SubscriptionRowEncodingError::RequiredFieldNulls {
                        field: field.name.clone(),
                        nulls: array.null_count(),
                    },
                ));
            }
            columns.push(ArrowFieldColumn { field, values });
        }
        Ok(Self { columns })
    }

    pub(crate) fn write_row(
        &self,
        row: usize,
        writer: &mut CellWriter<'_, 'static>,
    ) -> Result<(), Report<WireEncodeError>> {
        for column in &self.columns {
            if column.field.sensitive {
                writer.push_redacted()?;
            } else {
                column.values.write(row, column.field.optional, writer)?;
            }
        }
        Ok(())
    }
}

struct ArrowFieldColumn<'a> {
    field: &'a SchemaField,
    values: ArrowColumn<'a>,
}

enum ArrowColumn<'a> {
    U8(&'a UInt8Array),
    I8(&'a Int8Array),
    U16(&'a UInt16Array),
    I16(&'a Int16Array),
    U32(&'a UInt32Array),
    I32(&'a Int32Array),
    U64(&'a UInt64Array),
    I64(&'a Int64Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Bool(&'a BooleanArray),
    String(&'a StringArray),
    Bytes(&'a BinaryArray),
    Datetime(&'a TimestampNanosecondArray),
    FixedList {
        array: &'a FixedSizeListArray,
        element: Box<ArrowColumn<'a>>,
    },
    List {
        array: &'a ListArray,
        element: Box<ArrowColumn<'a>>,
    },
}

impl<'a> ArrowColumn<'a> {
    fn new(
        field: &FieldName,
        expected: &ParseAsType,
        array: &'a dyn Array,
    ) -> Result<Self, Report<SubscriptionRowEncodingError>> {
        let actual = match parse_as_type_from_arrow(array.data_type()) {
            Ok(actual) => actual,
            Err(error) => {
                return Err(error.change_context(
                    SubscriptionRowEncodingError::UnsupportedArrowType {
                        field: field.clone(),
                        data_type: array.data_type().clone(),
                    },
                ));
            }
        };
        if &actual != expected {
            return Err(Report::new(SubscriptionRowEncodingError::ColumnType {
                field: field.clone(),
                expected: expected.clone(),
                actual: array.data_type().clone(),
            }));
        }
        Ok(match expected {
            ParseAsType::U8 => Self::U8(downcast(array)),
            ParseAsType::I8 => Self::I8(downcast(array)),
            ParseAsType::U16 => Self::U16(downcast(array)),
            ParseAsType::I16 => Self::I16(downcast(array)),
            ParseAsType::U32 => Self::U32(downcast(array)),
            ParseAsType::I32 => Self::I32(downcast(array)),
            ParseAsType::U64 => Self::U64(downcast(array)),
            ParseAsType::I64 => Self::I64(downcast(array)),
            ParseAsType::F32 => Self::F32(downcast(array)),
            ParseAsType::F64 => Self::F64(downcast(array)),
            ParseAsType::Bool => Self::Bool(downcast(array)),
            ParseAsType::String => Self::String(downcast(array)),
            ParseAsType::Bytes => Self::Bytes(downcast(array)),
            ParseAsType::Datetime => Self::Datetime(downcast(array)),
            ParseAsType::Array { element, .. } => {
                let array: &FixedSizeListArray = downcast(array);
                let element_nullable = match array.data_type() {
                    ArrowDataType::FixedSizeList(element, _) => element.is_nullable(),
                    _ => false,
                };
                Self::list_element(field, element_nullable, array.values().as_ref())?;
                Self::FixedList {
                    array,
                    element: Box::new(Self::new(field, element, array.values().as_ref())?),
                }
            }
            ParseAsType::Vec { element } => {
                let array: &ListArray = downcast(array);
                let element_nullable = match array.data_type() {
                    ArrowDataType::List(element) => element.is_nullable(),
                    _ => false,
                };
                Self::list_element(field, element_nullable, array.values().as_ref())?;
                Self::List {
                    array,
                    element: Box::new(Self::new(field, element, array.values().as_ref())?),
                }
            }
        })
    }

    fn list_element(
        field: &FieldName,
        nullable: bool,
        values: &dyn Array,
    ) -> Result<(), Report<SubscriptionRowEncodingError>> {
        if nullable {
            return Err(Report::new(
                SubscriptionRowEncodingError::NullableListElements {
                    field: field.clone(),
                },
            ));
        }
        if values.null_count() > 0 {
            return Err(Report::new(
                SubscriptionRowEncodingError::ListElementNulls {
                    field: field.clone(),
                    nulls: values.null_count(),
                },
            ));
        }
        Ok(())
    }

    fn is_null(&self, index: usize) -> bool {
        match self {
            Self::U8(array) => array.is_null(index),
            Self::I8(array) => array.is_null(index),
            Self::U16(array) => array.is_null(index),
            Self::I16(array) => array.is_null(index),
            Self::U32(array) => array.is_null(index),
            Self::I32(array) => array.is_null(index),
            Self::U64(array) => array.is_null(index),
            Self::I64(array) => array.is_null(index),
            Self::F32(array) => array.is_null(index),
            Self::F64(array) => array.is_null(index),
            Self::Bool(array) => array.is_null(index),
            Self::String(array) => array.is_null(index),
            Self::Bytes(array) => array.is_null(index),
            Self::Datetime(array) => array.is_null(index),
            Self::FixedList { array, .. } => array.is_null(index),
            Self::List { array, .. } => array.is_null(index),
        }
    }

    fn write(
        &self,
        index: usize,
        nullable: bool,
        writer: &mut CellWriter<'_, 'static>,
    ) -> Result<(), Report<WireEncodeError>> {
        if self.is_null(index) {
            nullable
                .then_some(())
                .verified("required columns were checked for nulls before encoding");
            return writer.push_null();
        }
        self.write_present(index, writer)
    }

    fn write_required(
        &self,
        index: usize,
        writer: &mut CellWriter<'_, 'static>,
    ) -> Result<(), Report<WireEncodeError>> {
        (!self.is_null(index))
            .then_some(())
            .verified("list child columns were checked for nulls before encoding");
        self.write_present(index, writer)
    }

    fn write_present(
        &self,
        index: usize,
        writer: &mut CellWriter<'_, 'static>,
    ) -> Result<(), Report<WireEncodeError>> {
        match self {
            Self::U8(array) => writer.push_u8(array.value(index)),
            Self::I8(array) => writer.push_i8(array.value(index)),
            Self::U16(array) => writer.push_u16(array.value(index)),
            Self::I16(array) => writer.push_i16(array.value(index)),
            Self::U32(array) => writer.push_u32(array.value(index)),
            Self::I32(array) => writer.push_i32(array.value(index)),
            Self::U64(array) => writer.push_u64(array.value(index)),
            Self::I64(array) => writer.push_i64(array.value(index)),
            Self::F32(array) => writer.push_f32(array.value(index)),
            Self::F64(array) => writer.push_f64(array.value(index)),
            Self::Bool(array) => writer.push_bool(array.value(index)),
            Self::String(array) => writer.push_string(array.value(index)),
            Self::Bytes(array) => writer.push_bytes(array.value(index)),
            Self::Datetime(array) => {
                writer.push_datetime(Timestamp::from_unix_nanos(array.value(index)))
            }
            Self::FixedList { array, element } => {
                let start = usize::try_from(array.value_offset(index))
                    .verified("Arrow validates fixed-list offsets as nonnegative");
                let length = usize::try_from(array.value_length())
                    .verified("the schema admitted a positive fixed-list length");
                let end = start
                    .checked_add(length)
                    .verified("Arrow validates fixed-list offsets within the child array");
                writer.push_list(|writer| {
                    for element_index in start..end {
                        element.write_required(element_index, writer)?;
                    }
                    Ok(())
                })
            }
            Self::List { array, element } => {
                let offsets = array.value_offsets();
                let start = usize::try_from(offsets[index])
                    .verified("Arrow validates list offsets as nonnegative");
                let end = usize::try_from(offsets[index + 1])
                    .verified("Arrow validates list offsets as nonnegative");
                writer.push_list(|writer| {
                    for element_index in start..end {
                        element.write_required(element_index, writer)?;
                    }
                    Ok(())
                })
            }
        }
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .verified("the exact Arrow data type was checked before downcasting")
}

pub(crate) fn write_runtime_value(
    value: &RuntimeValue,
    writer: &mut CellWriter<'_, 'static>,
) -> Result<(), Report<WireEncodeError>> {
    match value {
        RuntimeValue::U8(value) => writer.push_u8(*value),
        RuntimeValue::I8(value) => writer.push_i8(*value),
        RuntimeValue::U16(value) => writer.push_u16(*value),
        RuntimeValue::I16(value) => writer.push_i16(*value),
        RuntimeValue::U32(value) => writer.push_u32(*value),
        RuntimeValue::I32(value) => writer.push_i32(*value),
        RuntimeValue::U64(value) => writer.push_u64(*value),
        RuntimeValue::I64(value) => writer.push_i64(*value),
        RuntimeValue::F32(value) => writer.push_f32(value.0),
        RuntimeValue::F64(value) => writer.push_f64(value.0),
        RuntimeValue::Bool(value) => writer.push_bool(*value),
        RuntimeValue::String(value) => writer.push_string(value),
        RuntimeValue::Datetime(value) => {
            let unix_nanos = value.timestamp_nanos_opt().verified(
                "branch datetimes came from a nanosecond Arrow value or validated literal",
            );
            writer.push_datetime(Timestamp::from_unix_nanos(unix_nanos))
        }
        RuntimeValue::Array(values) | RuntimeValue::Vec(values) => writer.push_list(|writer| {
            for value in values {
                write_runtime_value(value, writer)?;
            }
            Ok(())
        }),
    }
}

/// Why an Arrow subscription batch could not become typed Row frames.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SubscriptionRowEncodingError {
    #[error("branch schema contains duplicate field `{field}`")]
    DuplicateBranchSchemaField { field: FieldName },
    #[error("branch declaration repeats field `{field}`")]
    DuplicateBranchField { field: FieldName },
    #[error("branch `{branch}` references missing schema field `{field}`")]
    MissingBranchSchemaField {
        branch: BranchName,
        field: FieldName,
    },
    #[error("branch `{branch}` has no key fields")]
    EmptyBranchFields { branch: BranchName },
    #[error("row limit {rows_per_frame} exceeds the session collection limit {collection_entries}")]
    RowsPerFrameAboveCollectionLimit {
        rows_per_frame: usize,
        collection_entries: usize,
    },
    #[error("row schema has {schema_fields} fields, but Arrow has {arrow_columns} columns")]
    ColumnCount {
        schema_fields: usize,
        arrow_columns: usize,
    },
    #[error("Arrow column {column} is `{actual}`, expected field `{expected}`")]
    FieldName {
        column: usize,
        expected: FieldName,
        actual: String,
    },
    #[error(
        "field `{field}` nullable={actual_nullable} in Arrow, expected \
         nullable={expected_nullable}"
    )]
    Nullability {
        field: FieldName,
        expected_nullable: bool,
        actual_nullable: bool,
    },
    #[error("field `{field}` uses unsupported Arrow type {data_type}")]
    UnsupportedArrowType {
        field: FieldName,
        data_type: ArrowDataType,
    },
    #[error("field `{field}` expects {expected}, but Arrow has {actual}")]
    ColumnType {
        field: FieldName,
        expected: ParseAsType,
        actual: ArrowDataType,
    },
    #[error("required field `{field}` contains {nulls} null values")]
    RequiredFieldNulls { field: FieldName, nulls: usize },
    #[error("list field `{field}` contains {nulls} null elements")]
    ListElementNulls { field: FieldName, nulls: usize },
    #[error("list field `{field}` declares nullable elements")]
    NullableListElements { field: FieldName },
    #[error("Arrow batch has {batch_rows} rows, but {branch_keys} branch keys")]
    BranchKeyRowCount {
        batch_rows: usize,
        branch_keys: usize,
    },
    #[error("selected row {row} is outside an Arrow batch with {batch_rows} rows")]
    SelectedRowOutOfBounds { row: usize, batch_rows: usize },
    #[error("selected rows are not increasing at {previous} followed by {next}")]
    SelectedRowsNotIncreasing { previous: usize, next: usize },
    #[error("row {row} has a branch key for an unbranched subscription")]
    UnexpectedBranchKey { row: usize },
    #[error("row {row} has no branch key for a branched subscription")]
    MissingBranchKey { row: usize },
    #[error("row {row} branch key has {actual} fields, expected {expected}")]
    BranchFieldCount {
        row: usize,
        expected: usize,
        actual: usize,
    },
    #[error("row {row} branch key is missing field `{field}`")]
    MissingBranchField { row: usize, field: FieldName },
    #[error("row {row} branch field `{field}` expects {expected}, found {actual}")]
    BranchFieldType {
        row: usize,
        field: FieldName,
        expected: ParseAsType,
        actual: RuntimeValueKind,
    },
    #[error(
        "row {row} branch field `{field}` has {actual} elements, expected fixed length {expected}"
    )]
    BranchArrayLength {
        row: usize,
        field: FieldName,
        expected: u32,
        actual: usize,
    },
    #[error("failed to start a Row frame at Arrow row {row}")]
    StartFrame { row: usize },
    #[error("Arrow row {row} exceeds the Row frame limit of {frame_bytes} bytes")]
    RowExceedsFrame { row: usize, frame_bytes: usize },
    #[error("failed to encode Arrow row {row}")]
    EncodeRow { row: usize },
    #[error("failed to finish a Row frame")]
    FinishFrame,
}
