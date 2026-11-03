//! What the package's classes share at the boundary with Python: exporting borrowed bytes through
//! the buffer protocol, reading a host's bytes, and turning cells into Python values.
//!
//! - **Owns.** Filling a read-only buffer view that keeps its owner alive, the marker of a value
//!   the server withheld, and the Python value of a cell.
//! - **Depends on.** pyo3's buffer protocol and conversions, and the wire contract's cell views.
//! - **Must not know.** Which object a buffer belongs to, or which column a cell came from.

use std::{borrow::Cow, ffi::c_int};

use nervix_client_core::wire::CellView;
use pyo3::{
    IntoPyObjectExt as _,
    buffer::PyBuffer,
    exceptions::PyBufferError,
    ffi,
    prelude::*,
    types::{PyBytes, PyList},
};

/// A value the server withheld because the field is sensitive. Every one compares equal to
/// `REDACTED`.
#[pyclass(frozen, eq, hash, module = "nervix_client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Redacted;

#[pymethods]
impl Redacted {
    fn __repr__(&self) -> &'static str {
        "REDACTED"
    }
}

/// Fills `view` with `bytes`, read only, owned by `owner`, which the view holds a reference to
/// until the host releases it.
///
/// # Safety
///
/// `view` is the view Python asked `owner` to fill, and `bytes` stays valid and unchanged for as
/// long as `owner` lives.
pub(crate) unsafe fn export_bytes(
    owner: &Bound<'_, PyAny>,
    bytes: &[u8],
    view: *mut ffi::Py_buffer,
    flags: c_int,
) -> PyResult<()> {
    if flags & ffi::PyBUF_WRITABLE == ffi::PyBUF_WRITABLE {
        return Err(PyBufferError::new_err("the buffer is read only"));
    }
    let Ok(len) = isize::try_from(bytes.len()) else {
        return Err(PyBufferError::new_err(
            "the buffer is longer than Python can address",
        ));
    };
    // SAFETY: the caller guarantees `view` is the view to fill and that `bytes` outlives `owner`;
    // `PyBuffer_FillInfo` takes its own reference to `owner`, which the view releases.
    let filled = unsafe {
        ffi::PyBuffer_FillInfo(
            view,
            owner.as_ptr(),
            bytes.as_ptr().cast_mut().cast(),
            len,
            1,
            flags,
        )
    };
    if filled != 0 {
        return Err(PyErr::fetch(owner.py()));
    }
    Ok(())
}

/// The bytes a host passed: borrowed from `bytes`, and copied from any other object that exports
/// a buffer of bytes.
pub(crate) fn host_bytes<'a>(data: &'a Bound<'_, PyAny>) -> PyResult<Cow<'a, [u8]>> {
    if let Ok(bytes) = data.cast::<PyBytes>() {
        return Ok(Cow::Borrowed(bytes.as_bytes()));
    }
    let buffer = PyBuffer::<u8>::get(data)?;
    Ok(Cow::Owned(buffer.to_vec(data.py())?))
}

/// The Python value of a cell: an `int`, `float`, `bool`, `str` or `bytes` for a scalar, the
/// nanoseconds since the Unix epoch for a `DATETIME`, a `list` for a list, `None` for a null, and
/// a `Redacted` for a value the server withheld.
pub(crate) fn cell<'py>(py: Python<'py>, cell: CellView<'_>) -> PyResult<Bound<'py, PyAny>> {
    match cell {
        CellView::Null => Ok(py.None().into_bound(py)),
        CellView::Redacted => Redacted.into_bound_py_any(py),
        CellView::U8(value) => value.into_bound_py_any(py),
        CellView::I8(value) => value.into_bound_py_any(py),
        CellView::U16(value) => value.into_bound_py_any(py),
        CellView::I16(value) => value.into_bound_py_any(py),
        CellView::U32(value) => value.into_bound_py_any(py),
        CellView::I32(value) => value.into_bound_py_any(py),
        CellView::U64(value) => value.into_bound_py_any(py),
        CellView::I64(value) => value.into_bound_py_any(py),
        CellView::F32(value) => f64::from(value).into_bound_py_any(py),
        CellView::F64(value) => value.into_bound_py_any(py),
        CellView::Bool(value) => value.into_bound_py_any(py),
        CellView::String(value) => value.into_bound_py_any(py),
        CellView::Bytes(value) => Ok(PyBytes::new(py, value).into_any()),
        CellView::Datetime(value) => value.unix_nanos().into_bound_py_any(py),
        CellView::List(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items.iter() {
                values.push(self::cell(py, item)?);
            }
            Ok(PyList::new(py, values)?.into_any())
        }
    }
}
