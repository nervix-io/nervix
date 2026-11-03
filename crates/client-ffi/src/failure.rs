//! `nx_error`: the failure a host reads.
//!
//! Layer: edges.
//!
//! - **Owns.** The error handle a fallible call returns, reading its kind, message, execution
//!   reference and open refusal, and freeing it.
//! - **Depends on.** The shared failure and the C ABI boundary.
//! - **Must not know.** How a client error is classified; the shared failure decides that.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_client_host::{Failure, FailureKind, OpenRefusal};

use crate::abi;

/// # Safety
///
/// `error` is a live error this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_kind_of(error: *const Failure) -> FailureKind {
    // SAFETY: the header requires a live error.
    unsafe { abi::accessor(error) }.kind()
}

/// # Safety
///
/// `error` is a live error this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_message(
    error: *const Failure,
    message: *mut *const u8,
    message_len: *mut usize,
) {
    // SAFETY: the header requires a live error and writable out-parameters.
    unsafe {
        let error = abi::accessor(error);
        abi::write_bytes(message, message_len, error.message().as_bytes());
    }
}

/// # Safety
///
/// `error` is a live error this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_execution_reference(
    error: *const Failure,
    reference: *mut *const u8,
    reference_len: *mut usize,
) -> bool {
    // SAFETY: the header requires a live error.
    let error = unsafe { abi::accessor(error) };
    let Some(execution_reference) = error.execution_reference() else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe {
        abi::write_bytes(
            reference,
            reference_len,
            execution_reference.as_str().as_bytes(),
        )
    };
    true
}

/// # Safety
///
/// `error` is a live error this library returned; a non-null `refusal` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_open_refusal(
    error: *const Failure,
    refusal: *mut OpenRefusal,
) -> bool {
    // SAFETY: the header requires a live error.
    let error = unsafe { abi::accessor(error) };
    let Some(open_refusal) = error.open_refusal() else {
        return false;
    };
    // SAFETY: the header requires a writable out-parameter.
    unsafe { abi::write(refusal, open_refusal) };
    true
}

/// # Safety
///
/// A non-null `error` is an error this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_free(error: *mut Failure) {
    // SAFETY: the header requires an unreleased error or null.
    unsafe { abi::release(error) };
}
