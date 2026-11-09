//! Python's typed inspection metadata and independently retained verified Row frames.
//!
//! Layer: edges.
//! - **Owns.** Read-only Python inspection views, their schemas and buffer lifetimes.
//! - **Depends on.** The shared inspection owner, vocabulary and pyo3's buffer protocol.
//! - **Must not know.** State storage, paging decisions, cursor encoding or command routing.

use std::ffi::c_int;

use meticulous::ResultExt as _;
use nervix_client_core::wire::{
    StateInspectionCounts, StateInspectionSection, StateInspectionUnit,
};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;
use pyo3::{
    ffi,
    prelude::*,
    types::{PyBytes, PyMemoryView},
};

use crate::{
    enums::{InspectionRejection, InspectionStage, InspectionUnitKind},
    errors,
    schema::Schema,
    values,
};

/// One retained envelope. Child metadata views share this exact owner.
#[pyclass(frozen, module = "nervix_client", weakref)]
pub(crate) struct Inspection {
    owner: Arc<host::Inspection>,
}

impl Inspection {
    pub(crate) fn new(inspection: host::Inspection) -> Self {
        Self {
            owner: Arc::new(inspection),
        }
    }
}

#[pymethods]
impl Inspection {
    #[getter]
    fn domain(&self) -> &str {
        self.owner.domain()
    }
    #[getter]
    fn entity_kind(&self) -> &str {
        self.owner.entity_kind()
    }
    #[getter]
    fn entity_name(&self) -> &str {
        self.owner.entity_name()
    }
    #[getter]
    fn json(&self) -> &str {
        self.owner.json()
    }
    #[getter]
    fn rejection(&self) -> Option<InspectionRejection> {
        self.owner.rejection().map(InspectionRejection::from)
    }
    #[getter]
    fn counts(&self) -> Option<InspectionCounts> {
        self.owner
            .counts()
            .map(|counts| InspectionCounts { counts })
    }
    #[getter]
    fn unit_count(&self) -> Option<u64> {
        self.owner.unit_count()
    }
    #[getter]
    fn continuation(&self) -> Option<&str> {
        self.owner.continuation()
    }
    #[getter]
    fn units(&self) -> Vec<InspectionUnit> {
        self.owner
            .units()
            .iter()
            .enumerate()
            .map(|(unit, _)| InspectionUnit {
                owner: self.owner.clone(),
                unit,
            })
            .collect()
    }
}

#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct InspectionCounts {
    counts: StateInspectionCounts,
}

#[pymethods]
impl InspectionCounts {
    #[getter]
    fn branches(&self) -> u64 {
        self.counts.branches
    }
    #[getter]
    fn rows(&self) -> u64 {
        self.counts.rows
    }
    #[getter]
    fn bytes(&self) -> u64 {
        self.counts.bytes
    }
}

/// An index into immutable metadata, retaining its inspection after its parent is collected.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct InspectionUnit {
    owner: Arc<host::Inspection>,
    unit: usize,
}

impl InspectionUnit {
    fn data(&self) -> &StateInspectionUnit {
        self.owner
            .unit(self.unit)
            .assured("the view enumerated this immutable inspection's units")
    }
}

#[pymethods]
impl InspectionUnit {
    #[getter]
    fn kind(&self) -> InspectionUnitKind {
        self.owner
            .unit_kind(self.unit)
            .assured("the view's unit exists")
            .into()
    }
    #[getter]
    fn index(&self) -> Option<u64> {
        self.owner
            .unit_index(self.unit)
            .assured("the view's unit exists")
    }
    #[getter]
    fn owner(&self) -> &str {
        self.data().owner.as_str()
    }
    #[getter]
    fn replicas(&self) -> Vec<String> {
        self.data()
            .replicas
            .iter()
            .map(ToString::to_string)
            .collect()
    }
    #[getter]
    fn origin_node(&self) -> &str {
        self.data().origin.node.node_id().as_str()
    }
    #[getter]
    fn origin_incarnation(&self) -> u64 {
        self.data().origin.node.incarnation().get()
    }
    #[getter]
    fn origin_fingerprint<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data().origin.fingerprint)
    }
    #[getter]
    fn origin_generation(&self) -> u64 {
        self.data().origin.generation
    }
    #[getter]
    fn origin_revision(&self) -> u64 {
        self.data().origin.revision
    }
    #[getter]
    fn origin_stage(&self) -> InspectionStage {
        self.data().origin.stage.into()
    }
    #[getter]
    fn counts(&self) -> InspectionCounts {
        InspectionCounts {
            counts: self.data().counts,
        }
    }
    #[getter]
    fn sections(&self) -> Vec<InspectionSection> {
        self.data()
            .sections
            .iter()
            .enumerate()
            .map(|(section, _)| InspectionSection {
                owner: self.owner.clone(),
                unit: self.unit,
                section,
            })
            .collect()
    }
}

#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct InspectionSection {
    owner: Arc<host::Inspection>,
    unit: usize,
    section: usize,
}

impl InspectionSection {
    fn data(&self) -> &StateInspectionSection {
        self.owner
            .section(self.unit, self.section)
            .assured("the view enumerated this immutable unit's sections")
    }
}

#[pymethods]
impl InspectionSection {
    #[getter]
    fn name(&self) -> &str {
        &self.data().name
    }
    #[getter]
    fn omitted_rows(&self) -> u64 {
        self.data().omitted_rows
    }
    #[getter]
    fn watermark_low(&self) -> Option<u32> {
        self.data().watermarks.map(|columns| columns.low)
    }
    #[getter]
    fn watermark_high(&self) -> Option<u32> {
        self.data().watermarks.map(|columns| columns.high)
    }
    #[getter]
    fn schema(&self) -> Schema {
        Schema::new(
            self.owner
                .schema(self.unit, self.section)
                .assured("the view's section exists"),
        )
    }
    #[getter]
    fn frames(&self, py: Python<'_>) -> PyResult<Vec<InspectionFrame>> {
        let mut frames = Vec::with_capacity(self.data().frames.len());
        for index in 0..self.data().frames.len() {
            let frame = self
                .owner
                .row_frame(self.unit, self.section, index)
                .map_err(|failure| errors::raise(py, failure))?;
            frames.push(InspectionFrame { frame });
        }
        Ok(frames)
    }
}

/// A buffer retains precisely this verified frame and its schema, without its whole page.
#[pyclass(frozen, module = "nervix_client", weakref)]
pub(crate) struct InspectionFrame {
    frame: host::InspectionFrame,
}

#[pymethods]
impl InspectionFrame {
    #[getter]
    fn row_count(&self) -> u64 {
        self.frame.row_count()
    }
    #[getter]
    fn schema(&self) -> Schema {
        Schema::new(self.frame.schema())
    }
    #[getter]
    fn frame<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        // SAFETY: the buffer keeps this immutable object alive, including the exact frame it owns.
        unsafe { values::export_bytes(slf.as_any(), slf.get().frame.bytes(), view, flags) }
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Inspection>()?;
    module.add_class::<InspectionCounts>()?;
    module.add_class::<InspectionUnit>()?;
    module.add_class::<InspectionSection>()?;
    module.add_class::<InspectionFrame>()?;
    Ok(())
}
