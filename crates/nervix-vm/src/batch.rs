use std::sync::Arc;

use arch_into::ArchInto as _;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray, TimestampNanosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{DataType, Schema, TimeUnit};
use error_stack::Report;
use meticulous::OptionExt as _;

use crate::{RowErrors, RuntimeError};

macro_rules! declare_typed_arrays {
    ($($Variant:ident => $field:ident, $setter:ident, $accessor:ident, $Array:ty, $data_type:path;)+) => {
        #[derive(Debug, Clone, PartialEq)]
        pub enum TypedArray {
            $($Variant($Array),)+
            Datetime(TimestampNanosecondArray),
            Generic(ArrayRef),
            Uninitialized { data_type: DataType, len: usize },
        }

        impl TypedArray {
            pub fn len(&self) -> usize {
                match self {
                    $(Self::$Variant(array) => array.len(),)+
                    Self::Datetime(array) => array.len(),
                    Self::Generic(array) => array.len(),
                    Self::Uninitialized { len, .. } => *len,
                }
            }

            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }

            pub fn data_type(&self) -> DataType {
                match self {
                    $(Self::$Variant(_) => $data_type,)+
                    Self::Datetime(_) => {
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
                    }
                    Self::Generic(array) => array.data_type().clone(),
                    Self::Uninitialized { data_type, .. } => data_type.clone(),
                }
            }

            $(pub fn $accessor(&self) -> Option<&$Array> {
                match self {
                    Self::$Variant(array) => Some(array),
                    _ => None,
                }
            })+

            pub fn as_datetime(&self) -> Option<&TimestampNanosecondArray> {
                match self {
                    Self::Datetime(array) => Some(array),
                    _ => None,
                }
            }

            pub fn to_array_ref(&self) -> ArrayRef {
                match self {
                    $(Self::$Variant(array) => Arc::new(array.clone()),)+
                    Self::Datetime(array) => Arc::new(array.clone()),
                    Self::Generic(array) => array.clone(),
                    Self::Uninitialized { data_type, len } => new_null_array(data_type, *len),
                }
            }

            /// The bytes the array's values and validity hold, not the capacity its buffers were
            /// allocated with, or `None` when Arrow cannot measure its layout. An uninitialized
            /// column holds nothing yet.
            pub fn payload_bytes(&self) -> Option<usize> {
                if let Self::Uninitialized { .. } = self {
                    return Some(0);
                }
                self.as_array().to_data().get_slice_memory_size().ok()
            }

            pub(crate) fn as_array(&self) -> &dyn Array {
                match self {
                    $(Self::$Variant(array) => array,)+
                    Self::Datetime(array) => array,
                    Self::Generic(array) => array.as_ref(),
                    Self::Uninitialized { .. } => {
                        unreachable!(
                            "uninitialized arrays must be materialized before Arrow kernel access"
                        )
                    }
                }
            }

            pub(crate) fn into_array_ref(self) -> ArrayRef {
                match self {
                    $(Self::$Variant(array) => Arc::new(array),)+
                    Self::Datetime(array) => Arc::new(array),
                    Self::Generic(array) => array,
                    Self::Uninitialized { data_type, len } => new_null_array(&data_type, len),
                }
            }

            pub fn try_from_array_ref(array: ArrayRef) -> error_stack::Result<Self, RuntimeError> {
                let converted = match array.data_type() {
                    $($data_type => array
                        .as_any()
                        .downcast_ref::<$Array>()
                        .map(|array| Self::$Variant(array.clone())),)+
                    DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .map(|array| Self::Datetime(array.clone())),
                    DataType::List(_) | DataType::FixedSizeList(_, _) => {
                        Some(Self::Generic(array.clone()))
                    }
                    _ => None,
                };
                converted.ok_or_else(|| Report::new(RuntimeError::UnsupportedColumnType {
                    data_type: array.data_type().clone(),
                }))
            }

            pub fn uninitialized(data_type: DataType, len: usize) -> Self {
                Self::Uninitialized { data_type, len }
            }

            pub const fn is_uninitialized(&self) -> bool {
                matches!(self, Self::Uninitialized { .. })
            }

            pub fn null_count(&self) -> usize {
                match self {
                    $(Self::$Variant(array) => array.null_count(),)+
                    Self::Datetime(array) => array.null_count(),
                    Self::Generic(array) => array.null_count(),
                    Self::Uninitialized { len, .. } => *len,
                }
            }
        }
    };
}

with_typed_registers!(declare_typed_arrays);

macro_rules! declare_typed_array_conversions {
    ($($Variant:ident => $field:ident, $setter:ident, $accessor:ident, $Array:ty, $data_type:path;)+) => {
        $(
            impl From<$Array> for TypedArray {
                fn from(array: $Array) -> Self {
                    Self::$Variant(array)
                }
            }
        )+
    };
}

with_typed_registers!(declare_typed_array_conversions);

/// One columnar batch a program runs over: the schema of its fields and one array per field.
///
/// Every array already knows its own type, so the schema beside them is a second description of
/// the same columns. It is carried rather than derived because this is the VM's per-batch path: a
/// program's schema is one `Arc` that every batch it runs on shares, and rebuilding it from the
/// columns would allocate a `Schema` and a `Field` per column for every batch, on top of losing
/// the field names and nullability the arrays do not carry. [`TypedBatch::try_new`] checks the two
/// descriptions agree once, when the batch is built.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedBatch {
    schema: Arc<Schema>,
    columns: Vec<TypedArray>,
    errors: RowErrors,
    row_count: usize,
}

impl TypedBatch {
    pub fn try_new(
        schema: Arc<Schema>,
        columns: Vec<TypedArray>,
    ) -> error_stack::Result<Self, RuntimeError> {
        let row_count = validate_batch(&schema, &columns)?;
        Ok(Self {
            schema,
            columns,
            errors: RowErrors::new(row_count),
            row_count,
        })
    }

    /// Builds a typed batch whose row count cannot be inferred from a column.
    ///
    /// Arrow permits zero-column batches with a non-zero row count. The VM needs the
    /// same representation for programs made entirely from constants.
    pub fn try_new_with_row_count(
        schema: Arc<Schema>,
        columns: Vec<TypedArray>,
        row_count: usize,
    ) -> error_stack::Result<Self, RuntimeError> {
        let inferred_row_count = validate_batch(&schema, &columns)?;
        if !columns.is_empty() && inferred_row_count != row_count {
            return Err(Report::new(RuntimeError::InvalidBatch {
                message: format!(
                    "provided row count {row_count} does not match column row count \
                     {inferred_row_count}"
                ),
            }));
        }
        Ok(Self {
            schema,
            columns,
            errors: RowErrors::new(row_count),
            row_count,
        })
    }

    pub fn with_errors(
        schema: Arc<Schema>,
        columns: Vec<TypedArray>,
        errors: RowErrors,
    ) -> error_stack::Result<Self, RuntimeError> {
        let row_count = validate_batch(&schema, &columns)?;
        if errors.row_count() != row_count {
            return Err(Report::new(RuntimeError::InvalidBatch {
                message: format!(
                    "error row count {} does not match batch row count {}",
                    errors.row_count(),
                    row_count
                ),
            }));
        }
        Ok(Self {
            schema,
            columns,
            errors,
            row_count,
        })
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn columns(&self) -> &[TypedArray] {
        &self.columns
    }

    pub fn column(&self, index: usize) -> &TypedArray {
        &self.columns[index]
    }

    pub fn errors(&self) -> &RowErrors {
        &self.errors
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// The bytes the batch's columns hold, which is also the usual order of what a program builds
    /// from them, or `None` when Arrow cannot measure one of them.
    pub fn payload_bytes(&self) -> Option<u64> {
        let mut total = 0_u64;
        for column in &self.columns {
            let column_bytes: u64 = column.payload_bytes()?.arch_into();
            total = total
                .checked_add(column_bytes)
                .assured("the columns of one batch hold far less than u64::MAX bytes");
        }
        Some(total)
    }

    /// The rows on which some required field holds no value, because no route initialized it or
    /// it holds a null, as a bitmap over the batch, or `None` when every required field holds a
    /// value on every row.
    ///
    /// It is the OR of the inverted validity bitmaps of the required columns. A required column
    /// without nulls is skipped on its null count, so a batch whose required columns all hold
    /// values answers without reading a bitmap.
    pub fn rows_missing_required_values(&self) -> Option<BooleanBuffer> {
        let mut missing: Option<BooleanBuffer> = None;
        for (column, field) in self.columns.iter().zip(self.schema.fields()) {
            if field.is_nullable() || column.null_count() == 0 {
                continue;
            }
            if column.is_uninitialized() {
                return Some(BooleanBuffer::new_set(self.row_count));
            }
            // Arrow counts a column's nulls from its null buffer, and a row is null exactly where
            // that buffer says so, which is also what `Array::is_null` reads.
            let nulls = column
                .as_array()
                .nulls()
                .verified("the null count checked above is read from the column's null buffer");
            let column_missing = !nulls.inner();
            if let Some(rows) = missing.as_mut() {
                *rows |= &column_missing;
            } else {
                missing = Some(column_missing);
            }
        }
        missing
    }

    /// Exports the batch at a node boundary, where every required field must finally hold a value.
    ///
    /// Fields are checked in schema order and the first failing one ends the export: a required
    /// field no route ever wrote reports that it is uninitialized, and a required field written
    /// with a null reports the null. Optional fields materialize their nulls, uninitialized
    /// included.
    pub fn to_record_batch(&self) -> error_stack::Result<RecordBatch, RuntimeError> {
        let mut arrays = Vec::with_capacity(self.columns.len());
        for (column, field) in self.columns.iter().zip(self.schema.fields()) {
            let field_is_required = !field.is_nullable();
            if field_is_required && column.is_uninitialized() {
                return Err(Report::new(RuntimeError::UninitializedRequiredColumn {
                    column: field.name().clone(),
                }));
            }
            if field_is_required && column.null_count() > 0 {
                return Err(Report::new(RuntimeError::NullForRequiredColumn {
                    column: field.name().clone(),
                }));
            }
            arrays.push(column.to_array_ref());
        }

        let exported = if arrays.is_empty() {
            // Arrow reads a batch's row count off its columns, so a batch built entirely from
            // constants has to state the count it carries.
            let options = RecordBatchOptions::new().with_row_count(Some(self.row_count));
            RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)
        } else {
            RecordBatch::try_new(self.schema.clone(), arrays)
        };
        exported.map_err(|error| {
            Report::new(RuntimeError::InvalidBatch {
                message: error.to_string(),
            })
        })
    }
}

fn validate_batch(
    schema: &Schema,
    columns: &[TypedArray],
) -> error_stack::Result<usize, RuntimeError> {
    if schema.fields().len() != columns.len() {
        return Err(Report::new(RuntimeError::InvalidBatch {
            message: format!(
                "column count {} does not match schema field count {}",
                columns.len(),
                schema.fields().len()
            ),
        }));
    }

    let row_count = match columns.first() {
        Some(column) => column.len(),
        None => 0,
    };
    for (field, column) in schema.fields().iter().zip(columns) {
        if field.data_type() != &column.data_type() {
            return Err(Report::new(RuntimeError::InvalidBatch {
                message: format!(
                    "column '{}' has type {:?}, expected {:?}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ),
            }));
        }
        if column.len() != row_count {
            return Err(Report::new(RuntimeError::InvalidBatch {
                message: format!(
                    "column '{}' has row count {}, expected {}",
                    field.name(),
                    column.len(),
                    row_count
                ),
            }));
        }
    }

    Ok(row_count)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn sample_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int64, true),
            Field::new("floats", DataType::Float64, true),
            Field::new("flags", DataType::Boolean, true),
            Field::new("names", DataType::Utf8, true),
        ]))
    }

    fn sample_columns() -> Vec<TypedArray> {
        vec![
            TypedArray::Int64(Int64Array::from(vec![Some(1), None])),
            TypedArray::Float64(Float64Array::from(vec![Some(1.5), Some(2.5)])),
            TypedArray::Boolean(BooleanArray::from(vec![Some(true), Some(false)])),
            TypedArray::Utf8(StringArray::from(vec![Some("a"), None])),
        ]
    }

    #[test]
    fn typed_array_accessors_match_variants() {
        let arrays = sample_columns();

        let int64 = arrays[0].as_int64().expect("int accessor must succeed");
        assert_eq!(int64.value(0), 1);
        assert!(arrays[0].as_float64().is_none());

        let float64 = arrays[1].as_float64().expect("float accessor must succeed");
        assert_eq!(float64.value(1), 2.5);
        assert!(arrays[1].as_boolean().is_none());

        let boolean = arrays[2].as_boolean().expect("bool accessor must succeed");
        assert!(!boolean.value(1));
        assert!(arrays[2].as_utf8().is_none());

        let utf8 = arrays[3].as_utf8().expect("utf8 accessor must succeed");
        assert_eq!(utf8.value(0), "a");
        assert!(arrays[3].as_int64().is_none());
    }

    #[test]
    fn required_rows_without_values_are_the_or_of_the_required_columns_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("optional", DataType::Int64, true),
            Field::new("first", DataType::Int64, false),
            Field::new("second", DataType::Utf8, false),
            Field::new("unset_optional", DataType::Float64, true),
        ]));
        let rows = 130;
        let optional = Int64Array::from_iter((0..rows).map(|row| (row % 2 == 0).then_some(1)));
        // A sliced column reads its nulls from its own offset.
        let first = Int64Array::from_iter((0..rows + 3).map(|row| (row % 64 != 6).then_some(2)))
            .slice(3, rows);
        let second = StringArray::from_iter((0..rows).map(|row| (row != 129).then_some("x")));
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Int64(optional),
                TypedArray::Int64(first),
                TypedArray::Utf8(second),
                TypedArray::uninitialized(DataType::Float64, rows),
            ],
        )
        .expect("batch must build");

        let missing = batch
            .rows_missing_required_values()
            .expect("two required columns hold nulls");

        assert_eq!(missing.len(), rows);
        assert_eq!(missing.set_indices().collect::<Vec<_>>(), [3, 67, 129]);
    }

    #[test]
    fn required_columns_that_hold_every_value_need_no_bitmap() {
        let batch =
            TypedBatch::try_new(sample_schema(), sample_columns()).expect("batch must build");
        assert!(batch.rows_missing_required_values().is_none());

        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int64, false),
            Field::new("names", DataType::Utf8, true),
        ]));
        let batch = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Int64(Int64Array::from(vec![1, 2])),
                TypedArray::Utf8(StringArray::from(vec![None::<&str>, None])),
            ],
        )
        .expect("batch must build");
        assert!(batch.rows_missing_required_values().is_none());
    }

    #[test]
    fn an_uninitialized_required_column_misses_every_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int64, false),
            Field::new("unset", DataType::Utf8, false),
        ]));
        let batch = TypedBatch::try_new(
            schema.clone(),
            vec![
                TypedArray::Int64(Int64Array::from(vec![Some(1), None, Some(3)])),
                TypedArray::uninitialized(DataType::Utf8, 3),
            ],
        )
        .expect("batch must build");

        let missing = batch
            .rows_missing_required_values()
            .expect("an uninitialized required column holds no value");
        assert_eq!(missing.set_indices().collect::<Vec<_>>(), [0, 1, 2]);

        let empty = TypedBatch::try_new(
            schema,
            vec![
                TypedArray::Int64(Int64Array::from(Vec::<i64>::new())),
                TypedArray::uninitialized(DataType::Utf8, 0),
            ],
        )
        .expect("an empty batch must build");
        assert!(empty.rows_missing_required_values().is_none());
    }

    #[test]
    fn typed_batch_exposes_row_count_and_columns() {
        let batch =
            TypedBatch::try_new(sample_schema(), sample_columns()).expect("batch must build");

        assert_eq!(batch.row_count(), 2);
        assert_eq!(batch.columns().len(), 4);
        assert_eq!(batch.column(1).data_type(), DataType::Float64);
        assert_eq!(batch.errors().row_count(), 2);
        assert!(batch.errors().is_error_free());
    }

    #[test]
    fn typed_batch_preserves_explicit_row_count_without_columns() {
        let batch = TypedBatch::try_new_with_row_count(Arc::new(Schema::empty()), Vec::new(), 3)
            .expect("zero-column batch must build");

        assert_eq!(batch.row_count(), 3);
        assert_eq!(
            batch
                .to_record_batch()
                .expect("Arrow batch must build")
                .num_rows(),
            3
        );
    }

    #[test]
    fn typed_batch_rejects_wrong_error_row_count() {
        let error = TypedBatch::with_errors(sample_schema(), sample_columns(), RowErrors::new(1))
            .expect_err("batch must reject mismatched error rows");

        match error.current_context() {
            RuntimeError::InvalidBatch { message } => {
                assert!(message.contains("error row count 1"));
                assert!(message.contains("batch row count 2"));
            }
            other => panic!("expected invalid batch, got {other:?}"),
        }
    }

    #[test]
    fn typed_batch_rejects_wrong_column_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("ints", DataType::Int64, true)]));
        let columns = vec![TypedArray::Boolean(BooleanArray::from(vec![Some(true)]))];

        let error = TypedBatch::try_new(schema, columns).expect_err("batch must reject wrong type");

        match error.current_context() {
            RuntimeError::InvalidBatch { message } => {
                assert!(message.contains("column 'ints'"));
                assert!(message.contains("Boolean"));
                assert!(message.contains("Int64"));
            }
            other => panic!("expected invalid batch, got {other:?}"),
        }
    }

    #[test]
    fn optional_uninitialized_column_materializes_as_typed_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let batch =
            TypedBatch::try_new(schema, vec![TypedArray::uninitialized(DataType::Int64, 2)])
                .expect("uninitialized batch must build");

        let materialized = batch
            .to_record_batch()
            .expect("optional uninitialized output must materialize");

        assert_eq!(materialized.column(0).data_type(), &DataType::Int64);
        assert_eq!(materialized.column(0).null_count(), 2);
    }

    #[test]
    fn required_uninitialized_column_fails_materialization() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            TypedBatch::try_new(schema, vec![TypedArray::uninitialized(DataType::Int64, 1)])
                .expect("uninitialized batch must build before its node boundary");

        let error = batch
            .to_record_batch()
            .expect_err("required uninitialized output must fail");

        if let RuntimeError::UninitializedRequiredColumn { column } = error.current_context() {
            assert_eq!(column, "value");
        } else {
            panic!("expected required uninitialized column error, got {error:?}");
        }
    }

    #[test]
    fn written_null_in_required_column_fails_materialization() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let columns = vec![TypedArray::Int64(Int64Array::from(vec![Some(1), None]))];
        let batch = TypedBatch::try_new(schema, columns)
            .expect("written batch must build before its node boundary");

        let error = batch
            .to_record_batch()
            .expect_err("a null written into a required output must fail");

        if let RuntimeError::NullForRequiredColumn { column } = error.current_context() {
            assert_eq!(column, "value");
        } else {
            panic!("expected null for required column error, got {error:?}");
        }
    }

    #[test]
    fn typed_batch_rejects_wrong_column_length() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int64, true),
            Field::new("names", DataType::Utf8, true),
        ]));
        let columns = vec![
            TypedArray::Int64(Int64Array::from(vec![Some(1), Some(2)])),
            TypedArray::Utf8(StringArray::from(vec![Some("only-one")])),
        ];

        let error =
            TypedBatch::try_new(schema, columns).expect_err("batch must reject wrong row count");

        match error.current_context() {
            RuntimeError::InvalidBatch { message } => {
                assert!(message.contains("column 'names'"));
                assert!(message.contains("row count 1"));
                assert!(message.contains("expected 2"));
            }
            other => panic!("expected invalid batch, got {other:?}"),
        }
    }
}
