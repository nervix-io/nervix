use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Schema, TimeUnit};

use crate::{RuntimeError, SideError};

#[derive(Debug, Clone, PartialEq)]
pub enum TypedArray {
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
    Datetime(TimestampNanosecondArray),
    Generic(ArrayRef),
}

impl TypedArray {
    pub fn len(&self) -> usize {
        match self {
            Self::UInt8(array) => array.len(),
            Self::Int8(array) => array.len(),
            Self::UInt16(array) => array.len(),
            Self::Int16(array) => array.len(),
            Self::UInt32(array) => array.len(),
            Self::Int32(array) => array.len(),
            Self::UInt64(array) => array.len(),
            Self::Int64(array) => array.len(),
            Self::Float32(array) => array.len(),
            Self::Float64(array) => array.len(),
            Self::Boolean(array) => array.len(),
            Self::Utf8(array) => array.len(),
            Self::Datetime(array) => array.len(),
            Self::Generic(array) => array.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Self::UInt8(_) => DataType::UInt8,
            Self::Int8(_) => DataType::Int8,
            Self::UInt16(_) => DataType::UInt16,
            Self::Int16(_) => DataType::Int16,
            Self::UInt32(_) => DataType::UInt32,
            Self::Int32(_) => DataType::Int32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Int64(_) => DataType::Int64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Boolean(_) => DataType::Boolean,
            Self::Utf8(_) => DataType::Utf8,
            Self::Datetime(_) => DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            Self::Generic(array) => array.data_type().clone(),
        }
    }

    pub fn as_uint8(&self) -> Option<&UInt8Array> {
        match self {
            Self::UInt8(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_int8(&self) -> Option<&Int8Array> {
        match self {
            Self::Int8(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_uint16(&self) -> Option<&UInt16Array> {
        match self {
            Self::UInt16(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_int16(&self) -> Option<&Int16Array> {
        match self {
            Self::Int16(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_uint32(&self) -> Option<&UInt32Array> {
        match self {
            Self::UInt32(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_int32(&self) -> Option<&Int32Array> {
        match self {
            Self::Int32(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_uint64(&self) -> Option<&UInt64Array> {
        match self {
            Self::UInt64(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_int64(&self) -> Option<&Int64Array> {
        match self {
            Self::Int64(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_float32(&self) -> Option<&Float32Array> {
        match self {
            Self::Float32(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_float64(&self) -> Option<&Float64Array> {
        match self {
            Self::Float64(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<&BooleanArray> {
        match self {
            Self::Boolean(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_utf8(&self) -> Option<&StringArray> {
        match self {
            Self::Utf8(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<&TimestampNanosecondArray> {
        match self {
            Self::Datetime(array) => Some(array),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedBatch {
    schema: Arc<Schema>,
    columns: Vec<TypedArray>,
    errors: Vec<Vec<SideError>>,
    row_count: usize,
}

impl TypedBatch {
    pub fn try_new(schema: Arc<Schema>, columns: Vec<TypedArray>) -> Result<Self, RuntimeError> {
        let row_count = validate_batch(&schema, &columns)?;
        Ok(Self {
            schema,
            columns,
            errors: vec![Vec::new(); row_count],
            row_count,
        })
    }

    pub fn with_errors(
        schema: Arc<Schema>,
        columns: Vec<TypedArray>,
        errors: Vec<Vec<SideError>>,
    ) -> Result<Self, RuntimeError> {
        let row_count = validate_batch(&schema, &columns)?;
        if errors.len() != row_count {
            return Err(RuntimeError::InvalidBatch {
                message: format!(
                    "error row count {} does not match batch row count {}",
                    errors.len(),
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

    pub fn errors(&self) -> &[Vec<SideError>] {
        &self.errors
    }

    pub fn row_count(&self) -> usize {
        self.row_count
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
        assert_eq!(batch.errors(), &[Vec::new(), Vec::new()]);
    }

    #[test]
    fn typed_batch_rejects_wrong_error_row_count() {
        let error = TypedBatch::with_errors(sample_schema(), sample_columns(), vec![Vec::new()])
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
