//! C access to an independently retained cluster node inspection.
//!
//! Layer: edges.
//! - **Owns.** The C handle and borrowed inspection bytes.
//! - **Depends on.** The shared host inspection and checked C ABI helpers.
//! - **Must not know.** Command rendering or cluster membership.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "projects node inspections to C")
)]

use nervix_client_host::{Failure, FailureKind, NodeInspection, Outcome};

use crate::abi;

/// # Safety
/// The outcome is live and out is writable. The returned handle is independently owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_node_inspection(
    outcome: *const Outcome,
    out: *mut *mut NodeInspection,
) -> *mut Failure {
    abi::outcome((|| unsafe {
        abi::require_out(out, "out")?;
        let outcome = abi::handle(outcome, "outcome")?;
        let inspection = outcome.node_inspection().ok_or_else(|| {
            Failure::new(
                FailureKind::InvalidArgument,
                "the command did not inspect cluster nodes",
            )
        })?;
        abi::write(out, abi::into_handle(inspection));
        Ok(())
    })())
}

/// # Safety
/// The inspection is a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_node_inspection_count(inspection: *const NodeInspection) -> usize {
    // SAFETY: the caller guarantees a live handle.
    unsafe { abi::accessor(inspection).nodes().len() }
}

/// # Safety
/// The inspection is live and non-null out-parameters are writable. Bytes borrow the handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_node_inspection_name(
    inspection: *const NodeInspection,
    index: usize,
    data: *mut *const u8,
    len: *mut usize,
) -> bool {
    // SAFETY: the caller guarantees a live handle.
    let nodes = unsafe { abi::accessor(inspection) }.nodes();
    let Some(node) = nodes.get(index) else {
        return false;
    };
    // SAFETY: the caller guarantees writable out-parameters when non-null.
    unsafe {
        abi::write_bytes(data, len, node.name.as_str().as_bytes());
    }
    true
}

/// # Safety
/// The inspection is live and non-null out-parameters are writable. Bytes borrow the handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_node_inspection_json(
    inspection: *const NodeInspection,
    data: *mut *const u8,
    len: *mut usize,
) {
    // SAFETY: the caller guarantees a live handle and writable out-parameters.
    unsafe {
        abi::write_bytes(data, len, abi::accessor(inspection).json().as_bytes());
    }
}

/// # Safety
/// The caller transfers the live handle, or passes null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_node_inspection_free(inspection: *mut NodeInspection) {
    // SAFETY: the caller transfers the handle.
    unsafe {
        abi::release(inspection);
    }
}
