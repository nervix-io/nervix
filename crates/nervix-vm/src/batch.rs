use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, RecordBatchOptions, StringArray, TimestampNanosecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow_schema::{DataType, Schema, TimeUnit};

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

            pub fn try_from_array_ref(array: ArrayRef) -> Result<Self, RuntimeError> {
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
                converted.ok_or_else(|| RuntimeError::UnsupportedColumnType {
                    data_type: array.data_type().clone(),
                })
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

            pub(crate) fn is_null(&self, row: usize) -> bool {
                match self {
                    $(Self::$Variant(array) => array.is_null(row),)+
                    Self::Datetime(array) => array.is_null(row),
                    Self::Generic(array) => array.is_null(row),
                    Self::Uninitialized { .. } => true,
                }
            }
        }
    };
}

with_typed_registers!(declare_typed_arrays);

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
    pub fn try_new(schema: Arc<Schema>, columns: Vec<TypedArray>) -> Result<Self, RuntimeError> {
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
    ) -> Result<Self, RuntimeError> {
        let inferred_row_count = validate_batch(&schema, &columns)?;
        if !columns.is_empty() && inferred_row_count != row_count {
            return Err(RuntimeError::InvalidBatch {
                message: format!(
                    "provided row count {row_count} does not match column row count \
                     {inferred_row_count}"
                ),
            });
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
    ) -> Result<Self, RuntimeError> {
        let row_count = validate_batch(&schema, &columns)?;
        if errors.row_count() != row_count {
            return Err(RuntimeError::InvalidBatch {
                message: format!(
                    "error row count {} does not match batch row count {}",
                    errors.row_count(),
                    row_count
                ),
            });
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

    pub fn to_record_batch(&self) -> Result<RecordBatch, RuntimeError> {
        let columns = self
            .columns
            .iter()
            .zip(self.schema.fields())
            .map(|(column, field)| {
                if column.is_uninitialized() && !field.is_nullable() {
                    return Err(RuntimeError::UninitializedRequiredColumn {
                        column: field.name().clone(),
                    });
                }
                if !field.is_nullable() && column.null_count() > 0 {
                    return Err(RuntimeError::NullForRequiredColumn {
                        column: field.name().clone(),
                    });
                }
                Ok(column.to_array_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let result = if columns.is_empty() {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(self.row_count)),
            )
        } else {
            RecordBatch::try_new(self.schema.clone(), columns)
        };
        result.map_err(|error| RuntimeError::InvalidBatch {
            message: error.to_string(),
        })
    }
}

fn validate_batch(schema: &Schema, columns: &[TypedArray]) -> Result<usize, RuntimeError> {
    if schema.fields().len() != columns.len() {
        return Err(RuntimeError::InvalidBatch {
            message: format!(
                "column count {} does not match schema field count {}",
                columns.len(),
                schema.fields().len()
            ),
        });
    }

    let row_count = columns.first().map(TypedArray::len).unwrap_or(0);
    for (field, column) in schema.fields().iter().zip(columns) {
        if field.data_type() != &column.data_type() {
            return Err(RuntimeError::InvalidBatch {
                message: format!(
                    "column '{}' has type {:?}, expected {:?}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ),
            });
        }
        if column.len() != row_count {
            return Err(RuntimeError::InvalidBatch {
                message: format!(
                    "column '{}' has row count {}, expected {}",
                    field.name(),
                    column.len(),
                    row_count
                ),
            });
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

        match error {
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

        match error {
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

        if let RuntimeError::UninitializedRequiredColumn { column } = error {
            assert_eq!(column, "value");
        } else {
            panic!("expected required uninitialized column error, got {error:?}");
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

        match error {
            RuntimeError::InvalidBatch { message } => {
                assert!(message.contains("column 'names'"));
                assert!(message.contains("row count 1"));
                assert!(message.contains("expected 2"));
            }
            other => panic!("expected invalid batch, got {other:?}"),
        }
    }
}
