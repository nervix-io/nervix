//! Python access to a typed cluster node inspection.
//!
//! Layer: edges.
//! - **Owns.** The independently retained Python inspection object.
//! - **Depends on.** The shared host inspection and PyO3.
//! - **Must not know.** Command display text or record storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "projects node inspections to Python")
)]

use pyo3::prelude::*;

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NodeInspection>()
}

#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct NodeInspection {
    owner: nervix_client_host::NodeInspection,
}

impl NodeInspection {
    pub(crate) fn new(owner: nervix_client_host::NodeInspection) -> Self {
        Self { owner }
    }
}

#[pymethods]
impl NodeInspection {
    #[getter]
    fn names(&self) -> Vec<&str> {
        self.owner
            .nodes()
            .iter()
            .map(|node| node.name.as_str())
            .collect()
    }

    #[getter]
    fn json(&self) -> &str {
        self.owner.json()
    }
}
