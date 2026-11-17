//! Native access to an independently retained entity metadata view.
//!
//! Layer: edges.
//! - **Owns.** Typed host metadata getters and independent value ownership.
//! - **Depends on.** The shared host snapshot and the host binding library.
//! - **Must not know.** Display parsing, registry storage or execution behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns native metadata views")
)]

use std::collections::BTreeMap;

use nervix_client_host as host;
use pyo3::{prelude::*, types::PyBytes};

use crate::enums::EntityLabelViewKind;

#[pyclass(frozen, module = "nervix_client", weakref)]
pub(crate) struct EntityLabelsInspection {
    owner: host::EntityLabelsInspection,
}
impl EntityLabelsInspection {
    pub(crate) fn new(owner: host::EntityLabelsInspection) -> Self {
        Self { owner }
    }
}

#[pymethods]
impl EntityLabelsInspection {
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
    fn revision(&self) -> Option<u64> {
        self.owner.revision()
    }
    #[getter]
    fn transaction_id(&self) -> Option<&str> {
        self.owner.transaction_id()
    }
    #[getter]
    fn position(&self) -> Option<u64> {
        self.owner.position()
    }
    #[getter]
    fn policy_revision(&self) -> u64 {
        self.owner.policy_revision()
    }
    #[getter]
    fn max_count(&self) -> u64 {
        self.owner.max_count()
    }
    #[getter]
    fn max_key_bytes(&self) -> u64 {
        self.owner.max_key_bytes()
    }
    #[getter]
    fn max_value_bytes(&self) -> u64 {
        self.owner.max_value_bytes()
    }
    #[getter]
    fn max_bytes(&self) -> u64 {
        self.owner.max_bytes()
    }
    #[getter]
    fn labels(&self) -> BTreeMap<String, String> {
        self.owner
            .labels()
            .iter()
            .map(|(key, value)| (key.as_str().to_owned(), value.to_owned()))
            .collect()
    }
    #[getter]
    fn view(&self) -> EntityLabelViewKind {
        self.owner.view().into()
    }
    #[getter]
    fn planning_basis<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.owner
            .planning_basis()
            .map(|bytes| PyBytes::new(py, bytes))
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<EntityLabelsInspection>()
}
