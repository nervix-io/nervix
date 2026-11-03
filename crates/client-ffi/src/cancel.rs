//! `nx_cancel`: cancellation and deadlines of blocking calls, as a host holds them.
//!
//! - **Owns.** The token handle a host creates, triggers from any thread and frees.
//! - **Depends on.** The shared cancellation token and the C ABI boundary.
//! - **Must not know.** What the bounded call does; the session decides that.

use std::time::Duration;

use nervix_client_host::{Cancel, Failure};

use crate::abi;

#[unsafe(no_mangle)]
pub extern "C" fn nx_cancel_new() -> *mut Cancel {
    abi::into_handle(Cancel::default())
}

/// # Safety
///
/// A non-null `out` is writable for a handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_cancel_with_deadline(
    deadline_millis: u64,
    out: *mut *mut Cancel,
) -> *mut Failure {
    // SAFETY: the header requires a writable `out`.
    abi::outcome(unsafe { write_with_deadline(deadline_millis, out) })
}

/// # Safety
///
/// A non-null `out` is writable for a handle.
unsafe fn write_with_deadline(deadline_millis: u64, out: *mut *mut Cancel) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    let cancel = Cancel::with_deadline(Duration::from_millis(deadline_millis))?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(cancel)) };
    Ok(())
}

/// # Safety
///
/// `cancel` is a live token this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_cancel_trigger(cancel: *const Cancel) {
    // SAFETY: the header requires a live token.
    unsafe { abi::accessor(cancel) }.trigger();
}

/// # Safety
///
/// A non-null `cancel` is a token this library returned that has not been freed, and no call is
/// still waiting on it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_cancel_free(cancel: *mut Cancel) {
    // SAFETY: the header requires an unreleased token or null.
    unsafe { abi::release(cancel) };
}
