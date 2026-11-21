//! Host access to an independently retained cluster node inspection.
//!
//! Layer: edges.
//! - **Owns.** Retaining the verified report and its lossless JSON projection.
//! - **Depends on.** The Rust client's typed node inspection.
//! - **Must not know.** Command text, selection or cluster membership.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "retains host node inspections")
)]

use nervix_client_core::NodeInspectionRow;

#[derive(Debug)]
pub struct NodeInspection {
    report: nervix_client_core::NodeInspection,
    json: String,
}

impl NodeInspection {
    pub(crate) fn new(report: nervix_client_core::NodeInspection) -> Self {
        let json = nervix_client_core::wire::node_inspection_json(&report);
        Self { report, json }
    }

    pub fn nodes(&self) -> &[NodeInspectionRow] {
        &self.report.nodes
    }

    pub fn json(&self) -> &str {
        &self.json
    }
}
