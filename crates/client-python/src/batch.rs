//! Typed Arrow batches of client endpoints as a Python host holds them, and building one from a
//! host's columns.
//!
//! - **Owns.** A batch's rows, schema and canonical Arrow IPC stream exported through the buffer
//!   protocol, reading its columns as Python values or one type level at a time, exporting it
//!   through the Arrow PyCapsule interface, and building one from a host's states, list offsets
//!   and values.
//! - **Depends on.** The host session runtime's batch and builder, the schema class, and the Arrow
//!   exchange.
//! - **Must not know.** Which producer a batch is for or which delivery carried it.
//!
//! A column is read and built one level of its type at a time. Level 0 holds one cell per row; a
//! `LIST` or `FIXED_LIST` level holds lists whose elements are the cells of the next level, and
//! the last level holds the values. Only a row can be null: list elements never are.

use std::ffi::c_int;

use arrow_array::{Array, cast::AsArray as _, types};
use meticulous::OptionExt as _;
use nervix_client_host as host;
use nervix_models::ParseAsType;
use nervix_primitives::sync::Arc;
use pyo3::{
    IntoPyObjectExt as _, ffi,
    prelude::*,
    types::{PyBytes, PyCapsule, PyList, PyMemoryView},
};

use crate::{arrow, enums::CellState, errors, schema::Schema, values};

/// One typed batch of an endpoint.
///
/// It exports its canonical Arrow IPC stream through the buffer protocol: `memoryview(batch)`, or
/// `batch.ipc`, borrows it without a copy and keeps the batch alive. It also exports itself
/// through the Arrow PyCapsule interface, so an Arrow library reads it without a copy.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Batch {
    batch: Arc<host::Batch>,
}

impl Batch {
    pub(crate) fn new(batch: Arc<host::Batch>) -> Self {
        Self { batch }
    }

    pub(crate) fn host(&self) -> &Arc<host::Batch> {
        &self.batch
    }
}

/// The Python value of row `row` of `array`, whose type is `ty`. The batch's arrays have its
/// schema's types, so every downcast holds.
fn arrow_value<'py>(
    py: Python<'py>,
    array: &dyn Array,
    row: usize,
    ty: &ParseAsType,
) -> PyResult<Bound<'py, PyAny>> {
    if array.is_null(row) {
        return Ok(py.None().into_bound(py));
    }
    const TYPED: &str = "a batch's arrays have the types of its schema";
    match ty {
        ParseAsType::U8 => array
            .as_primitive_opt::<types::UInt8Type>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::I8 => array
            .as_primitive_opt::<types::Int8Type>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::U16 => array
            .as_primitive_opt::<types::UInt16Type>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::I16 => array
            .as_primitive_opt::<types::Int16Type>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::U32 => array
            .as_primitive_opt::<types::UInt32Type>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::I32 => array
            .as_primitive_opt::<types::Int32Type>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::U64 => array
            .as_primitive_opt::<types::UInt64Type>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::I64 => array
            .as_primitive_opt::<types::Int64Type>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::F32 => f64::from(
            array
                .as_primitive_opt::<types::Float32Type>()
                .assured(TYPED)
                .value(row),
        )
        .into_bound_py_any(py),
        ParseAsType::F64 => array
            .as_primitive_opt::<types::Float64Type>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::Datetime => array
            .as_primitive_opt::<types::TimestampNanosecondType>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::Bool => array
            .as_boolean_opt()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::String => array
            .as_string_opt::<i32>()
            .assured(TYPED)
            .value(row)
            .into_bound_py_any(py),
        ParseAsType::Bytes => {
            Ok(PyBytes::new(py, array.as_binary_opt::<i32>().assured(TYPED).value(row)).into_any())
        }
        ParseAsType::Vec { element } => {
            let elements = array.as_list_opt::<i32>().assured(TYPED).value(row);
            list(py, elements.as_ref(), element)
        }
        ParseAsType::Array { element, .. } => {
            let elements = array.as_fixed_size_list_opt().assured(TYPED).value(row);
            list(py, elements.as_ref(), element)
        }
    }
}

/// The Python list of every element of `elements`, whose type is `element`.
fn list<'py>(
    py: Python<'py>,
    elements: &dyn Array,
    element: &ParseAsType,
) -> PyResult<Bound<'py, PyAny>> {
    let mut values = Vec::with_capacity(elements.len());
    for index in 0..elements.len() {
        values.push(arrow_value(py, elements, index, element)?);
    }
    Ok(PyList::new(py, values)?.into_any())
}

#[pymethods]
impl Batch {
    #[getter]
    fn row_count(&self) -> usize {
        self.batch.row_count()
    }

    #[getter]
    fn schema(&self) -> Schema {
        Schema::new(self.batch.schema().clone())
    }

    /// The batch's canonical Arrow IPC stream, borrowed without a copy. The view keeps the batch
    /// alive. A batch a builder finished writes its stream the first time it is read.
    #[getter]
    fn ipc<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        let stream = match slf.get().batch.ipc() {
            Ok(stream) => stream,
            Err(failure) => return Err(errors::raise(py, failure)),
        };
        // SAFETY: Python passes a view to fill; the stream is kept by the batch `slf` holds, which
        // the view keeps alive until it is released.
        unsafe { values::export_bytes(slf.as_any(), stream, view, flags) }
    }

    /// The rows of column `index` as Python values: an `int`, `float`, `bool`, `str` or `bytes`
    /// for a scalar, the nanoseconds since the Unix epoch for a `DATETIME`, a `list` for a list,
    /// and `None` for a null.
    fn column<'py>(&self, py: Python<'py>, index: usize) -> PyResult<Bound<'py, PyList>> {
        let field = match self.batch.schema().field(host::Part::Rows, index) {
            Ok(field) => field,
            Err(failure) => return Err(errors::raise(py, failure)),
        };
        let array = self.batch.record_batch().column(index);
        let mut values = Vec::with_capacity(array.len());
        for row in 0..array.len() {
            values.push(arrow_value(py, array.as_ref(), row, &field.ty)?);
        }
        PyList::new(py, values)
    }

    /// How many cells `level` of `column` holds.
    fn cells(&self, py: Python<'_>, column: usize, level: usize) -> PyResult<usize> {
        self.batch
            .cells(column, level)
            .map_err(|failure| errors::raise(py, failure))
    }

    /// The state of every row of `column`.
    fn states(&self, py: Python<'_>, column: usize) -> PyResult<Vec<CellState>> {
        let rows = self.batch.row_count();
        let mut states = vec![0_u8; rows];
        if let Err(failure) = self.batch.states(column, &mut states) {
            return Err(errors::raise(py, failure));
        }
        let mut read = Vec::with_capacity(rows);
        for state in states {
            match host::CellState::from_host(state) {
                Ok(state) => read.push(CellState::from(state)),
                Err(failure) => return Err(errors::raise(py, failure)),
            }
        }
        Ok(read)
    }

    /// The offsets, starting at zero, of the lists `level` of `column` holds into the level below
    /// it: one per list and a final end.
    fn offsets(&self, py: Python<'_>, column: usize, level: usize) -> PyResult<Vec<u64>> {
        let cells = self.cells(py, column, level)?;
        let Some(ends) = cells.checked_add(1) else {
            return Err(errors::invalid_argument(py, "level", "overflows"));
        };
        let mut offsets = vec![0_u64; ends];
        match self.batch.offsets(column, level, &mut offsets) {
            Ok(()) => Ok(offsets),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The fixed-width values of `level` of `column`, in native byte order.
    fn fixed<'py>(
        &self,
        py: Python<'py>,
        column: usize,
        level: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let len = match self.batch.fixed_len(column, level) {
            Ok(len) => len,
            Err(failure) => return Err(errors::raise(py, failure)),
        };
        PyBytes::new_with(py, len, |buffer| {
            self.batch
                .fixed(column, level, buffer)
                .map_err(|failure| errors::raise(py, failure))
        })
    }

    /// The string or bytes values of `level` of `column`: one offset per cell and a final end,
    /// starting at zero, and the concatenated values.
    fn varlen<'py>(
        &self,
        py: Python<'py>,
        column: usize,
        level: usize,
    ) -> PyResult<(Vec<u64>, Bound<'py, PyBytes>)> {
        let len = match self.batch.varlen_len(column, level) {
            Ok(len) => len,
            Err(failure) => return Err(errors::raise(py, failure)),
        };
        let cells = self.cells(py, column, level)?;
        let Some(ends) = cells.checked_add(1) else {
            return Err(errors::invalid_argument(py, "level", "overflows"));
        };
        let mut offsets = vec![0_u64; ends];
        let data = PyBytes::new_with(py, len, |buffer| {
            self.batch
                .varlen(column, level, &mut offsets, buffer)
                .map_err(|failure| errors::raise(py, failure))
        })?;
        Ok((offsets, data))
    }

    /// The batch's schema, exported through the Arrow PyCapsule interface.
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        arrow::schema_capsule(py, &self.batch)
    }

    /// The batch as one Arrow struct array of its rows, exported through the Arrow PyCapsule
    /// interface. A requested schema is not honoured: the batch is exported as it is.
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
        drop(requested_schema);
        arrow::array_capsules(py, &self.batch)
    }

    /// The batch as a stream of one Arrow record batch, exported through the Arrow PyCapsule
    /// interface. A requested schema is not honoured: the batch is exported as it is.
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        drop(requested_schema);
        arrow::stream_capsule(py, &self.batch)
    }

    /// A batch of `schema`'s rows from Arrow data that exports itself through the Arrow PyCapsule
    /// interface: the same field names in the same order, exactly the types of `schema`, and no
    /// null in a field or a list element it requires.
    #[staticmethod]
    fn from_arrow(
        py: Python<'_>,
        schema: PyRef<'_, Schema>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let batch = arrow::import(py, data, schema.host().clone())?;
        Ok(Self::new(Arc::new(batch)))
    }
}

/// A batch a host is building, one level of one column at a time. Every call copies what it is
/// given, so a host may reuse its buffers as soon as the call returns.
#[pyclass(module = "nervix_client")]
pub(crate) struct BatchBuilder {
    builder: host::BatchBuilder,
}

#[pymethods]
impl BatchBuilder {
    /// Starts a batch of `rows` rows of `schema`'s fields.
    #[new]
    fn new(schema: PyRef<'_, Schema>, rows: usize) -> Self {
        Self {
            builder: host::BatchBuilder::new(schema.host().clone(), rows),
        }
    }

    /// Sets whether each row of `column` holds a value: `True` for a value, `False` for a null,
    /// which only an optional field takes. Unset, every row holds one.
    fn states(&mut self, py: Python<'_>, column: usize, present: Vec<bool>) -> PyResult<()> {
        self.builder
            .presence(column, &present)
            .map_err(|failure| errors::raise(py, failure))
    }

    /// Sets the offsets of the variable-length lists `level` of `column` holds into the level
    /// below it: one per list and a final end, starting at zero.
    fn offsets(
        &mut self,
        py: Python<'_>,
        column: usize,
        level: usize,
        offsets: Vec<u64>,
    ) -> PyResult<()> {
        self.builder
            .offsets(column, level, &offsets)
            .map_err(|failure| errors::raise(py, failure))
    }

    /// Sets the fixed-width values of the innermost `level` of `column`, in native byte order. A
    /// `BOOL` value is the byte 0 or 1.
    fn fixed(
        &mut self,
        py: Python<'_>,
        column: usize,
        level: usize,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let values = values::host_bytes(values)?;
        self.builder
            .fixed(column, level, &values)
            .map_err(|failure| errors::raise(py, failure))
    }

    /// Sets the string or bytes values of the innermost `level` of `column`: one offset per value
    /// and a final end, starting at zero, into `data`.
    fn varlen(
        &mut self,
        py: Python<'_>,
        column: usize,
        level: usize,
        offsets: Vec<u64>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let data = values::host_bytes(data)?;
        self.builder
            .varlen(column, level, &offsets, &data)
            .map_err(|failure| errors::raise(py, failure))
    }

    /// Finishes the batch from every column set, leaving the builder empty for another batch of
    /// the same rows.
    fn finish(&mut self, py: Python<'_>) -> PyResult<Batch> {
        match self.builder.finish() {
            Ok(batch) => Ok(Batch::new(Arc::new(batch))),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }
}
