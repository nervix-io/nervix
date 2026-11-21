//! A server notice, as a Python host reads it.
//!
//! Layer: edges.
//! - **Owns.** Reading a notice's level, kind and display text, and the class, node kind and node
//!   name of the error a runtime notice reports.
//! - **Depends on.** The host session runtime's notice, which reads each of them without
//!   allocating.
//! - **Must not know.** How notices are queued, coalesced or routed, which the session decides,
//!   or what a notice's text says; a host routes a notice by its kind, never by its text.

use nervix_client_host as host;
use pyo3::prelude::*;

use crate::enums::{NoticeKind, NoticeLevel};

/// One server notice: display text with its level and kind and, on a runtime notice that reports
/// an error of a graph node, that error's class and the kind and name of the node that produced
/// it. A host routes a notice by its `kind`, never by its text.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Notice {
    notice: host::Notice,
}

impl Notice {
    pub(crate) fn new(notice: host::Notice) -> Self {
        Self { notice }
    }
}

#[pymethods]
impl Notice {
    /// How urgently the notice asks for an operator's attention.
    #[getter]
    fn level(&self) -> NoticeLevel {
        NoticeLevel::from(self.notice.level())
    }

    /// What the notice reports, which a host routes it by.
    #[getter]
    fn kind(&self) -> NoticeKind {
        NoticeKind::from(self.notice.kind())
    }

    /// The notice's display text, which a host shows and never routes by.
    #[getter]
    fn message(&self) -> &str {
        self.notice.message()
    }

    /// The class of the error a runtime notice reports, as its lower-case name such as
    /// `evaluation`, or `None` for a notice that reports no classified error of a graph node.
    #[getter]
    fn error_class(&self) -> Option<&'static str> {
        self.notice.error_class()
    }

    /// The kind of the graph node that produced the reported error, as its snake_case name such as
    /// `junction`, or `None` for a notice that reports no classified error of a graph node.
    #[getter]
    fn error_node_kind(&self) -> Option<&'static str> {
        self.notice.error_node_kind()
    }

    /// The name of the graph node that produced the reported error, or `None` for a notice that
    /// reports no classified error of a graph node.
    #[getter]
    fn error_node(&self) -> Option<&str> {
        self.notice.error_node()
    }
}

/// Adds the notice class to the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Notice>()?;
    Ok(())
}
