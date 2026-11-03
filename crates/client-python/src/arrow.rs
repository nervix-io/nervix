//! Exchanging batches with Python's Arrow libraries through the Arrow PyCapsule interface.
//!
//! - **Owns.** Wrapping a batch's exported Arrow schema, array and stream in the `arrow_schema`,
//!   `arrow_array` and `arrow_array_stream` capsules the interface names, and taking the
//!   structures out of the capsules an object exports to import a batch.
//! - **Depends on.** The host session runtime's Arrow C data exchange, which exports and imports
//!   the structures and holds imported data to a schema, and pyo3's capsules.
//! - **Must not know.** What the structures hold; the host session runtime reads and writes them.
//!
//! An exported capsule owns its structure until a consumer moves it out, and releases it when
//! Python collects the capsule otherwise, so a capsule nobody reads leaks nothing.

use std::ffi::CStr;

use arrow_array::{
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::FFI_ArrowArrayStream,
};
use nervix_client_host as host;
use pyo3::{prelude::*, types::PyCapsule};

use crate::errors;

/// The capsule name of an exported Arrow schema.
const SCHEMA: &CStr = c"arrow_schema";

/// The capsule name of an exported Arrow array.
const ARRAY: &CStr = c"arrow_array";

/// The capsule name of an exported Arrow array stream.
const STREAM: &CStr = c"arrow_array_stream";

/// The row schema of `batch` as an `arrow_schema` capsule, of the struct type of its rows.
pub(crate) fn schema_capsule<'py>(
    py: Python<'py>,
    batch: &host::Batch,
) -> PyResult<Bound<'py, PyCapsule>> {
    let schema = batch
        .export_schema()
        .map_err(|failure| errors::raise(py, failure))?;
    PyCapsule::new_with_value(py, schema, SCHEMA)
}

/// The rows of `batch` as one struct array: an `arrow_schema` and an `arrow_array` capsule.
pub(crate) fn array_capsules<'py>(
    py: Python<'py>,
    batch: &host::Batch,
) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
    let exported = batch
        .export_array()
        .map_err(|failure| errors::raise(py, failure))?;
    let schema = PyCapsule::new_with_value(py, exported.schema, SCHEMA)?;
    let array = PyCapsule::new_with_value(py, exported.array, ARRAY)?;
    Ok((schema, array))
}

/// `batch` as a stream of one record batch: an `arrow_array_stream` capsule.
pub(crate) fn stream_capsule<'py>(
    py: Python<'py>,
    batch: &host::Batch,
) -> PyResult<Bound<'py, PyCapsule>> {
    PyCapsule::new_with_value(py, batch.export_stream(), STREAM)
}

/// Whether `data` exports itself through the Arrow PyCapsule interface.
pub(crate) fn exports(data: &Bound<'_, PyAny>) -> PyResult<bool> {
    if data.hasattr("__arrow_c_stream__")? {
        return Ok(true);
    }
    data.hasattr("__arrow_c_array__")
}

/// `exported` as a capsule. Reading its pointer holds it to the name the interface gives what it
/// holds.
fn capsule(exported: Bound<'_, PyAny>) -> PyResult<Bound<'_, PyCapsule>> {
    Ok(exported.cast_into::<PyCapsule>()?)
}

/// A batch of `schema`'s rows from the Arrow data `data` exports: its stream when it exports one,
/// and its struct array otherwise.
pub(crate) fn import(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    schema: host::Schema,
) -> PyResult<host::Batch> {
    let imported = if data.hasattr("__arrow_c_stream__")? {
        let stream = capsule(data.call_method0("__arrow_c_stream__")?)?;
        let pointer = stream.pointer_checked(Some(STREAM))?;
        // SAFETY: the capsule is named `arrow_array_stream`, so by the interface it holds a live
        // `ArrowArrayStream`; `from_raw` moves it out and leaves the capsule's copy released.
        let stream = unsafe { FFI_ArrowArrayStream::from_raw(pointer.cast().as_ptr()) };
        host::Batch::import_stream(stream, schema)
    } else {
        let exported = data.call_method0("__arrow_c_array__")?;
        let (schema_capsule, array_capsule): (Bound<'_, PyAny>, Bound<'_, PyAny>) =
            exported.extract()?;
        let schema_capsule = capsule(schema_capsule)?;
        let array_capsule = capsule(array_capsule)?;
        let schema_pointer = schema_capsule.pointer_checked(Some(SCHEMA))?;
        let array_pointer = array_capsule.pointer_checked(Some(ARRAY))?;
        // SAFETY: the capsules are named `arrow_schema` and `arrow_array`, so by the interface
        // they hold a live `ArrowSchema`, which stays in its capsule and is only borrowed here,
        // and a live `ArrowArray`, which `from_raw` moves out and leaves released in its capsule.
        let (array, exported_schema) = unsafe {
            let array = FFI_ArrowArray::from_raw(array_pointer.cast().as_ptr());
            let exported_schema: &FFI_ArrowSchema = schema_pointer.cast().as_ref();
            (array, exported_schema)
        };
        host::Batch::import_array(array, exported_schema, schema)
    };
    imported.map_err(|failure| errors::raise(py, failure))
}
