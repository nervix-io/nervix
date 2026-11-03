//! A subscription event a Python host holds, and its rows.
//!
//! - **Owns.** An event's kind, subscription, counts and restoration failure, its verified frame
//!   exported through the buffer protocol, and its columns read as typed values, copied as
//!   native-order values and offsets, or exported as Arrow arrays.
//! - **Depends on.** The host session runtime's event and schema, the wire contract's cell views,
//!   and the Arrow exchange of this package.
//! - **Must not know.** How events are routed or restored; the session decides that.
//!
//! A rows event owns the verified frame it was decoded from, and a `memoryview` of it keeps the
//! event alive for as long as the view exists. Columns are read through the wire contract's
//! checked views; the batch was held to its schema when the event was created.

use std::{ffi::c_int, time::Duration};

use nervix_client_core::{SubscriptionEvent, SubscriptionHandle, wire::CellView};
use nervix_client_host as host;
use pyo3::{
    ffi,
    prelude::*,
    types::{PyBytes, PyList, PyMemoryView},
};

use crate::{
    batch::Batch,
    enums::{CellState, EventKind, Part},
    errors,
    schema::Schema,
    values,
};

/// The subscription an event or a command concerns: its name and the generation it opened.
#[pyclass(frozen, eq, hash, module = "nervix_client")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Subscription {
    name: String,
    generation: u64,
}

impl Subscription {
    pub(crate) fn of(handle: &SubscriptionHandle) -> Self {
        Self {
            name: handle.name.as_str().to_owned(),
            generation: handle.generation.get(),
        }
    }
}

#[pymethods]
impl Subscription {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn generation(&self) -> u64 {
        self.generation
    }

    fn __repr__(&self) -> String {
        format!("Subscription({:?}, {})", self.name, self.generation)
    }
}

/// A restoration the session's server refused: why, and how long the session waits before its
/// next attempt, in nanoseconds.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct RestorationFailure {
    message: String,
    retry_after: Duration,
}

impl RestorationFailure {
    pub(crate) fn new(message: &str, retry_after: Duration) -> Self {
        Self {
            message: message.to_owned(),
            retry_after,
        }
    }
}

#[pymethods]
impl RestorationFailure {
    #[getter]
    fn message(&self) -> &str {
        &self.message
    }

    /// How long, in nanoseconds, the session waits before it tries again.
    #[getter]
    fn retry_after_nanos(&self) -> u64 {
        host::nanos(self.retry_after)
    }
}

/// One subscription event.
///
/// A rows event exports its verified frame through the buffer protocol: `memoryview(event)`, or
/// `event.frame`, borrows it without a copy and keeps the event alive.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Event {
    event: host::Event,
}

impl Event {
    pub(crate) fn new(event: host::Event) -> Self {
        Self { event }
    }

    fn cells(&self, py: Python<'_>, part: Part, column: usize) -> PyResult<Vec<CellView<'_>>> {
        match self.event.cells(host::Part::from(part), column) {
            Ok(cells) => Ok(cells),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }
}

#[pymethods]
impl Event {
    #[getter]
    fn kind(&self) -> EventKind {
        EventKind::from(self.event.kind())
    }

    /// The rows a rows event carries, or the rows a loss or a skip counts.
    #[getter]
    fn row_count(&self) -> u64 {
        self.event.row_count()
    }

    /// The subscription the event concerns.
    #[getter]
    fn subscription(&self) -> Subscription {
        Subscription::of(self.event.subscription_event().subscription())
    }

    /// The refusal of a restoration the server refused, or `None` for every other event.
    #[getter]
    fn restoration_failure(&self) -> Option<RestorationFailure> {
        let SubscriptionEvent::RestorationFailed(failure) = self.event.subscription_event() else {
            return None;
        };
        Some(RestorationFailure::new(
            &failure.message,
            failure.retry_after,
        ))
    }

    /// The schema of a rows event's rows.
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Schema> {
        match self.event.schema() {
            Ok(schema) => Ok(Schema::new(schema)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// A rows event's verified frame, borrowed without a copy. The view keeps the event alive.
    #[getter]
    fn frame<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        let frame = match slf.get().event.frame() {
            Ok(frame) => frame,
            Err(failure) => return Err(errors::raise(py, failure)),
        };
        // SAFETY: Python passes a view to fill; the frame is owned by the event `slf` holds,
        // which the view keeps alive until it is released.
        unsafe { values::export_bytes(slf.as_any(), frame, view, flags) }
    }

    /// The state of every cell of `column` of `part`.
    #[pyo3(signature = (column, part = Part::Rows))]
    fn states(&self, py: Python<'_>, column: usize, part: Part) -> PyResult<Vec<CellState>> {
        let cells = self.cells(py, part, column)?;
        let states = cells
            .iter()
            .map(|cell| CellState::from(host::CellState::from(*cell)))
            .collect();
        Ok(states)
    }

    /// The cells of `column` of `part` as Python values: an `int`, `float`, `bool`, `str` or
    /// `bytes` for a scalar, the nanoseconds since the Unix epoch for a `DATETIME`, a `list` for a
    /// list, `None` for a null, and `REDACTED` for a value the server withheld.
    #[pyo3(signature = (column, part = Part::Rows))]
    fn column<'py>(
        &self,
        py: Python<'py>,
        column: usize,
        part: Part,
    ) -> PyResult<Bound<'py, PyList>> {
        let cells = self.cells(py, part, column)?;
        let mut converted = Vec::with_capacity(cells.len());
        for cell in cells {
            converted.push(values::cell(py, cell)?);
        }
        PyList::new(py, converted)
    }

    /// The values of a fixed-width `column` of `part`, in native byte order, one width per cell; a
    /// null or redacted cell is zero bytes.
    #[pyo3(signature = (column, part = Part::Rows))]
    fn fixed<'py>(
        &self,
        py: Python<'py>,
        column: usize,
        part: Part,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let part = host::Part::from(part);
        let len = match self.event.fixed_len(part, column) {
            Ok(len) => len,
            Err(failure) => return Err(errors::raise(py, failure)),
        };
        PyBytes::new_with(py, len, |buffer| {
            match self.event.column_fixed(part, column, buffer) {
                Ok(()) => Ok(()),
                Err(failure) => Err(errors::raise(py, failure)),
            }
        })
    }

    /// The values of a `STRING` or `BYTES` `column` of `part`: one offset per cell and a final
    /// end, starting at zero, and the concatenated values.
    #[pyo3(signature = (column, part = Part::Rows))]
    fn varlen<'py>(
        &self,
        py: Python<'py>,
        column: usize,
        part: Part,
    ) -> PyResult<(Vec<u64>, Bound<'py, PyBytes>)> {
        let part = host::Part::from(part);
        let len = match self.event.column_varlen_len(part, column) {
            Ok(len) => len,
            Err(failure) => return Err(errors::raise(py, failure)),
        };
        let cells = self.cells(py, Part::from(part), column)?.len();
        let Some(ends) = cells.checked_add(1) else {
            return Err(errors::invalid_argument(py, "column", "overflows"));
        };
        let mut offsets = vec![0_u64; ends];
        let data = PyBytes::new_with(py, len, |buffer| {
            match self.event.column_varlen(part, column, &mut offsets, buffer) {
                Ok(()) => Ok(()),
                Err(failure) => Err(errors::raise(py, failure)),
            }
        })?;
        Ok((offsets, data))
    }

    /// A rows event's rows as one batch of its row fields, which reads its columns as Python
    /// values and exports itself through the Arrow PyCapsule interface. A sensitive field is
    /// nullable in it, and a value the server withheld is null there; `states` tells the two
    /// apart.
    fn rows(&self, py: Python<'_>) -> PyResult<Batch> {
        match self.event.rows_batch() {
            Ok(batch) => Ok(Batch::new(nervix_primitives::sync::Arc::new(batch))),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }
}

/// Adds the subscription event classes to the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Event>()?;
    module.add_class::<Subscription>()?;
    module.add_class::<RestorationFailure>()?;
    Ok(())
}
