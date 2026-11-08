//! `nx_inspection`: a retained typed state inspection.
//!
//! Layer: edges.
//! - **Owns.** Inspection handles, complete counts, schemas and borrowed verified Row frame bytes.
//! - **Depends on.** The shared inspection handle and C ABI helpers.
//! - **Must not know.** Command rendering, state storage, owner selection or row encoding.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the boundary declared above")
)]

use nervix_client_host::{Failure, FailureKind, Inspection, Outcome, Schema};

use crate::abi;

/// # Safety
/// `outcome` is live; `out` is writable. The returned handle is independently owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_inspection(
    outcome: *const Outcome,
    out: *mut *mut Inspection,
) -> *mut Failure {
    abi::outcome((|| unsafe {
        abi::require_out(out, "out")?;
        let state = abi::handle(outcome, "outcome")?
            .state_inspection()
            .ok_or_else(|| {
                Failure::new(
                    FailureKind::InvalidArgument,
                    "the command did not inspect state",
                )
            })?;
        abi::write(out, abi::into_handle(state));
        Ok(())
    })())
}

/// # Safety
/// `inspection` is live; non-null out-parameters are writable. Bytes borrow this handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_inspection_json(
    inspection: *const Inspection,
    data: *mut *const u8,
    len: *mut usize,
) {
    // SAFETY: the caller guarantees a live handle and writable out-parameters.
    unsafe {
        abi::write_bytes(data, len, abi::accessor(inspection).json().as_bytes());
    }
}

/// # Safety
/// `inspection` is live; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_inspection_counts(
    inspection: *const Inspection,
    branches: *mut u64,
    rows: *mut u64,
    bytes: *mut u64,
) -> bool {
    // SAFETY: the caller guarantees a live handle.
    let Some(counts) = (unsafe { abi::accessor(inspection) }).counts() else {
        return false;
    };
    // SAFETY: the caller guarantees writable non-null out-parameters.
    unsafe {
        abi::write(branches, counts.branches);
        abi::write(rows, counts.rows);
        abi::write(bytes, counts.bytes);
    }
    true
}

/// # Safety
/// `inspection` is live; `out` is writable. The returned schema is independently owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_inspection_schema(
    inspection: *const Inspection,
    unit: usize,
    section: usize,
    out: *mut *mut Schema,
) -> *mut Failure {
    abi::outcome((|| unsafe {
        abi::require_out(out, "out")?;
        let schema = abi::handle(inspection, "inspection")?.schema(unit, section)?;
        abi::write(out, abi::into_handle(schema));
        Ok(())
    })())
}

/// # Safety
/// `inspection` is live; `data` and `len` are writable. Bytes borrow this handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_inspection_row_frame(
    inspection: *const Inspection,
    unit: usize,
    section: usize,
    frame: usize,
    data: *mut *const u8,
    len: *mut usize,
) -> *mut Failure {
    abi::outcome((|| unsafe {
        abi::require_out(data, "data")?;
        abi::require_out(len, "len")?;
        let frame = abi::handle(inspection, "inspection")?.frame(unit, section, frame)?;
        abi::write_bytes(data, len, frame.bytes());
        Ok(())
    })())
}

/// # Safety
/// A non-null handle was returned by this library and has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_inspection_free(inspection: *mut Inspection) {
    // SAFETY: the caller transfers its one independently owned handle.
    unsafe {
        abi::release(inspection);
    }
}
