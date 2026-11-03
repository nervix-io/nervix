//! `nx_delivery`: one delivery of a client emitter's output, as a host holds it.
//!
//! - **Owns.** The shared ownership of a delivery a host retains and releases, borrowing its
//!   identity, reference, metadata and stream, handing out its batch, and settling it.
//! - **Depends on.** The shared delivery, the batch and cancellation handles, and the C ABI
//!   boundary.
//! - **Must not know.** Which attachment may settle it; the Rust delivery decides that.
//!
//! Releasing a delivery settles nothing: an unsettled attempt stays with its consumer until the
//! emitter's ACK timeout or its attachment ends, and is then delivered again.

use std::mem::ManuallyDrop;

use nervix_client_host::{Batch, Cancel, Delivery, Failure, Settlement};
use nervix_primitives::sync::Arc;

use crate::abi;

/// # Safety
///
/// `delivery` is a live delivery; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_identity(
    delivery: *const Delivery,
    identity: *mut *const u8,
    identity_len: *mut usize,
) {
    // SAFETY: the header requires a live delivery and writable out-parameters.
    unsafe {
        let delivery = abi::accessor(delivery);
        abi::write_bytes(identity, identity_len, delivery.identity());
    }
}

/// # Safety
///
/// `delivery` is a live delivery; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_reference(
    delivery: *const Delivery,
    reference: *mut *const u8,
    reference_len: *mut usize,
) {
    // SAFETY: the header requires a live delivery and writable out-parameters.
    unsafe {
        let delivery = abi::accessor(delivery);
        abi::write_bytes(reference, reference_len, delivery.reference());
    }
}

/// # Safety
///
/// `delivery` is a live delivery; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_source_relay(
    delivery: *const Delivery,
    name: *mut *const u8,
    name_len: *mut usize,
) {
    // SAFETY: the header requires a live delivery and writable out-parameters.
    unsafe {
        let delivery = abi::accessor(delivery);
        abi::write_bytes(name, name_len, delivery.source_relay().as_bytes());
    }
}

/// # Safety
///
/// `delivery` is a live delivery; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_branch_fingerprint(
    delivery: *const Delivery,
    fingerprint: *mut *const u8,
    fingerprint_len: *mut usize,
) -> bool {
    // SAFETY: the header requires a live delivery.
    let Some(branch) = unsafe { abi::accessor(delivery) }.branch_fingerprint() else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe { abi::write_bytes(fingerprint, fingerprint_len, branch) };
    true
}

/// # Safety
///
/// `delivery` is a live delivery.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_members(delivery: *const Delivery) -> u32 {
    // SAFETY: the header requires a live delivery.
    unsafe { abi::accessor(delivery) }.members()
}

/// # Safety
///
/// `delivery` is a live delivery.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_execution_now(delivery: *const Delivery) -> i64 {
    // SAFETY: the header requires a live delivery.
    unsafe { abi::accessor(delivery) }.execution_now()
}

/// # Safety
///
/// `delivery` is a live delivery; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_ipc(
    delivery: *const Delivery,
    ipc: *mut *const u8,
    ipc_len: *mut usize,
) {
    // SAFETY: the header requires a live delivery and writable out-parameters.
    unsafe {
        let delivery = abi::accessor(delivery);
        abi::write_bytes(ipc, ipc_len, delivery.ipc());
    }
}

/// # Safety
///
/// `delivery` is a live delivery; a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_batch(
    delivery: *const Delivery,
    out: *mut *mut Batch,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_batch(delivery, out) })
}

/// # Safety
///
/// As [`nx_delivery_batch`].
unsafe fn write_batch(delivery: *const Delivery, out: *mut *mut Batch) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live delivery.
    let shared = unsafe { abi::handle(delivery, "delivery") }?.batch()?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::share(shared)) };
    Ok(())
}

/// Settles a delivery one way and writes what the server did.
///
/// # Safety
///
/// `delivery` is live, a non-null `cancel` is a live token, and a non-null `settlement` is
/// writable.
unsafe fn write_settled(
    delivery: *const Delivery,
    cancel: *const Cancel,
    settlement: *mut Settlement,
    settle: impl FnOnce(&Delivery, Option<&Cancel>) -> Result<Settlement, Failure>,
) -> Result<(), Failure> {
    abi::require_out(settlement, "settlement")?;
    // SAFETY: the caller guarantees a live delivery and a live token or null.
    let (delivery, cancel) = unsafe { (abi::handle(delivery, "delivery")?, cancel.as_ref()) };
    let settled = settle(delivery, cancel)?;
    // SAFETY: `settlement` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(settlement, settled) };
    Ok(())
}

/// # Safety
///
/// `delivery` is live, a non-null `cancel` is a live token, and a non-null `settlement` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_ack(
    delivery: *const Delivery,
    cancel: *const Cancel,
    settlement: *mut Settlement,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_settled(delivery, cancel, settlement, Delivery::ack) })
}

/// # Safety
///
/// `delivery` is live, a non-null `cancel` is a live token, and a non-null `settlement` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_retry(
    delivery: *const Delivery,
    cancel: *const Cancel,
    settlement: *mut Settlement,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_settled(delivery, cancel, settlement, Delivery::retry) })
}

/// # Safety
///
/// `delivery` is live, `reason` addresses `reason_len` readable bytes, a non-null `cancel` is a
/// live token, and a non-null `settlement` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_reject(
    delivery: *const Delivery,
    reason: *const u8,
    reason_len: usize,
    cancel: *const Cancel,
    settlement: *mut Settlement,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_rejected(delivery, reason, reason_len, cancel, settlement) })
}

/// # Safety
///
/// As [`nx_delivery_reject`].
unsafe fn write_rejected(
    delivery: *const Delivery,
    reason: *const u8,
    reason_len: usize,
    cancel: *const Cancel,
    settlement: *mut Settlement,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees `reason` addresses its length in readable bytes.
    let reason = unsafe { abi::text(reason, reason_len, "reason") }?;
    // SAFETY: the caller upholds the rest of this function's contract.
    unsafe {
        write_settled(delivery, cancel, settlement, |delivery, cancel| {
            delivery.reject(reason, cancel)
        })
    }
}

/// # Safety
///
/// `delivery` is a live reference to a delivery this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_retain(delivery: *mut Delivery) -> *mut Delivery {
    // SAFETY: the header requires a live reference, which `into_shared` or this function made
    // from an `Arc`. Wrapping it in `ManuallyDrop` leaves the caller's reference in place.
    let shared = ManuallyDrop::new(unsafe { Arc::from_raw(delivery.cast_const()) });
    Arc::into_raw(Arc::clone(&shared)).cast_mut()
}

/// # Safety
///
/// A non-null `delivery` is a reference this library returned that has not been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_delivery_release(delivery: *mut Delivery) {
    if delivery.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased reference, which `into_shared` or
    // `nx_delivery_retain` made from an `Arc`.
    drop(unsafe { Arc::from_raw(delivery.cast_const()) });
}
