//! Publication of a node's completed public-service initialization.
//!
//! Layer: control plane.
//! - **Owns.** The one-way publication that policy-dependent public work may begin.
//! - **Depends on.** The primitive once-publication contract.
//! - **Must not know.** Graphs, cluster parameter values, or transports.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns node initialization publication")
)]

use meticulous::ResultExt as _;
use nervix_primitives::sync::blocking::OnceLock;

#[derive(Default)]
pub(in crate::application) struct NodeInitialization {
    completed: OnceLock<()>,
}

impl NodeInitialization {
    pub(in crate::application) fn complete(&self) {
        self.completed
            .set(())
            .assured("a node completes its initialization exactly once");
    }

    pub(in crate::application) fn is_ready(&self) -> bool {
        self.completed.get().is_some()
    }
}
