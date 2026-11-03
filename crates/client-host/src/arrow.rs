//! A batch exchanged with a host's Arrow library through the Arrow C data interface, and Arrow
//! data a host brought held to an endpoint's schema.
//!
//! - **Owns.** Exporting a batch's row schema, its rows as one struct array, and a stream of it,
//!   importing a record batch from an array or a stream a host's Arrow library exported, and
//!   holding imported data to a schema: the same field names in the same order, exactly its types,
//!   and no null in a field or a list element it requires.
//! - **Depends on.** Arrow's C data and stream interfaces, and the batch and schema.
//! - **Must not know.** How a binding hands the exported structures to its host, or takes them
//!   back.
//!
//! An exported structure owns what it exports until its consumer moves it out, and releases it
//! when it is dropped otherwise.

use arrow_array::{
    Array, ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader as _, StructArray,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
    make_array,
};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};
use nervix_models::SchemaField;
use nervix_primitives::sync::StdArc;

use crate::{
    batch::Batch,
    failure::Failure,
    schema::{Part, Schema},
};

/// A batch's rows exported as one struct array: the array, and the schema of its struct type.
pub struct ExportedArray {
    pub array: FFI_ArrowArray,
    pub schema: FFI_ArrowSchema,
}

/// Arrow data a host passed that the library cannot read.
fn unreadable(problem: &ArrowError) -> Failure {
    Failure::invalid_argument(
        "data",
        &format!("is not Arrow data the library can read: {problem}"),
    )
}

impl Batch {
    /// The batch's row schema, as the struct type of its rows.
    pub fn export_schema(&self) -> Result<FFI_ArrowSchema, Failure> {
        let rows = DataType::Struct(self.record_batch().schema().fields().clone());
        FFI_ArrowSchema::try_from(&rows).map_err(|problem| unreadable(&problem))
    }

    /// The batch's rows as one struct array.
    pub fn export_array(&self) -> Result<ExportedArray, Failure> {
        let rows = StructArray::from(self.record_batch().clone());
        let (array, schema) = to_ffi(&rows.to_data()).map_err(|problem| unreadable(&problem))?;
        Ok(ExportedArray { array, schema })
    }

    /// The batch as a stream of one record batch.
    pub fn export_stream(&self) -> FFI_ArrowArrayStream {
        let batch = self.record_batch().clone();
        let schema = batch.schema();
        let reader = RecordBatchIterator::new([Ok(batch)], schema);
        FFI_ArrowArrayStream::new(Box::new(reader))
    }

    /// A batch of `target`'s rows from the struct array a host's Arrow library exported.
    pub fn import_array(
        array: FFI_ArrowArray,
        schema: &FFI_ArrowSchema,
        target: Schema,
    ) -> Result<Self, Failure> {
        // SAFETY: the caller hands over an array its exporter filled for `schema`; `from_ffi`
        // moves the array out and releases it when the imported data is dropped.
        let data = unsafe { from_ffi(array, schema) }.map_err(|problem| unreadable(&problem))?;
        let DataType::Struct(_) = data.data_type() else {
            return Err(Failure::invalid_argument(
                "data",
                "is an array that is not a struct of columns",
            ));
        };
        let rows = StructArray::from(data);
        if rows.null_count() != 0 {
            return Err(Failure::invalid_argument(
                "data",
                "is a struct array with a null row",
            ));
        }
        Self::conform(RecordBatch::from(rows), target)
    }

    /// A batch of `target`'s rows from every record batch of the stream a host's Arrow library
    /// exported, concatenated.
    pub fn import_stream(stream: FFI_ArrowArrayStream, target: Schema) -> Result<Self, Failure> {
        let reader =
            ArrowArrayStreamReader::try_new(stream).map_err(|problem| unreadable(&problem))?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|problem| unreadable(&problem))?);
        }
        let batch = arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|problem| unreadable(&problem))?;
        Self::conform(batch, target)
    }

    /// A batch of `target`'s rows from Arrow data a host brought: the same field names in the
    /// same order, exactly the types of `target`, and no null in a field or a list element it
    /// requires. Only the nullability and names of list elements may differ from the canonical
    /// schema; the batch is rebuilt on it.
    pub fn conform(batch: RecordBatch, target: Schema) -> Result<Self, Failure> {
        let fields = target.fields(Part::Rows);
        let canonical = SchemaField::arrow_schema(fields);
        let brought = batch.schema();
        if brought.fields().len() != canonical.fields().len() {
            return Err(Failure::invalid_argument(
                "data",
                &format!(
                    "has {} columns where the schema has {} fields",
                    brought.fields().len(),
                    canonical.fields().len()
                ),
            ));
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(canonical.fields().len());
        for (index, (field, column)) in canonical.fields().iter().zip(batch.columns()).enumerate() {
            let named = brought.field(index).name();
            if named != field.name() {
                return Err(Failure::invalid_argument(
                    "data",
                    &format!(
                        "names column {index} '{named}' where the schema names '{}'",
                        field.name()
                    ),
                ));
            }
            let data = conform_level(column.to_data(), field.data_type(), field.name(), 0)?;
            columns.push(make_array(data));
        }
        let rebuilt = RecordBatch::try_new(StdArc::new(canonical), columns).map_err(|problem| {
            Failure::invalid_argument("data", &format!("does not fit the schema: {problem}"))
        })?;
        Ok(Self::built(rebuilt, target))
    }
}

/// `data` rebuilt as `expected`, the canonical type of level `level` of column `name`: a list's
/// elements are conformed in turn and may not be null.
fn conform_level(
    data: ArrayData,
    expected: &DataType,
    name: &str,
    level: usize,
) -> Result<ArrayData, Failure> {
    let element = match (data.data_type(), expected) {
        (DataType::List(_), DataType::List(element)) => element,
        (DataType::FixedSizeList(_, brought), DataType::FixedSizeList(element, len))
            if brought == len =>
        {
            element
        }
        (brought, expected) if brought == expected => return Ok(data),
        (brought, expected) => {
            return Err(Failure::invalid_argument(
                "data",
                &format!(
                    "holds {brought} at level {level} of column '{name}' where the schema has \
                     {expected}"
                ),
            ));
        }
    };
    let Some(child) = data.child_data().first() else {
        return Err(Failure::invalid_argument(
            "data",
            &format!("holds a list without elements at level {level} of column '{name}'"),
        ));
    };
    if child.null_count() != 0 {
        return Err(Failure::invalid_argument(
            "data",
            &format!("holds a null list element at level {level} of column '{name}'"),
        ));
    }
    let next = level.checked_add(1).ok_or_else(|| {
        Failure::invalid_argument("data", "nests more levels than a usize counts")
    })?;
    let child = conform_level(child.clone(), element.data_type(), name, next)?;
    data.into_builder()
        .data_type(expected.clone())
        .child_data(vec![child])
        .build()
        .map_err(|problem| unreadable(&problem))
}
