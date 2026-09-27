//! The C ABI boundary: reading what a host passes in and writing what it reads back.
//!
//! - **Owns.** Turning host pointers into borrowed Rust values, handing owned values out as
//!   handles and taking them back, and writing out-parameters.
//! - **Depends on.** Nothing beyond the failure a refused argument becomes.
//! - **Must not know.** What any handle holds.
//!
//! Every function here trusts the header's contract for the pointers it is given: a non-null
//! pointer points to what the header says, for as long as the header says. A null pointer is
//! checked wherever the contract allows a host to pass one by mistake.

use std::{ptr, slice};

use crate::failure::Failure;

/// Borrows a required UTF-8 argument.
///
/// # Safety
///
/// A non-null `text` points to `len` readable bytes that outlive `'a`.
pub(crate) unsafe fn text<'a>(
    text: *const u8,
    len: usize,
    argument: &'static str,
) -> Result<&'a str, Failure> {
    // SAFETY: the caller upholds this function's contract.
    let Some(text) = (unsafe { optional_text(text, len, argument) })? else {
        return Err(Failure::invalid_argument(argument, "is required"));
    };
    Ok(text)
}

/// Borrows an optional UTF-8 argument, absent when `text` is null.
///
/// # Safety
///
/// A non-null `text` points to `len` readable bytes that outlive `'a`.
pub(crate) unsafe fn optional_text<'a>(
    text: *const u8,
    len: usize,
    argument: &'static str,
) -> Result<Option<&'a str>, Failure> {
    if text.is_null() {
        return Ok(None);
    }
    // SAFETY: `text` is non-null, and the caller guarantees it addresses `len` readable bytes
    // that outlive `'a`.
    let bytes = unsafe { slice::from_raw_parts(text, len) };
    match std::str::from_utf8(bytes) {
        Ok(text) => Ok(Some(text)),
        Err(_) => Err(Failure::invalid_argument(argument, "is not UTF-8")),
    }
}

/// Borrows a handle the host passed in.
///
/// # Safety
///
/// A non-null `handle` points to a live `T` this library handed out, which outlives `'a`.
pub(crate) unsafe fn handle<'a, T>(
    handle: *const T,
    argument: &'static str,
) -> Result<&'a T, Failure> {
    // SAFETY: the caller upholds this function's contract.
    match unsafe { handle.as_ref() } {
        Some(handle) => Ok(handle),
        None => Err(Failure::invalid_argument(argument, "is required")),
    }
}

/// Borrows a handle an infallible accessor reads, which the header requires to be valid.
///
/// # Safety
///
/// `handle` points to a live `T` this library handed out, which outlives `'a`.
pub(crate) unsafe fn accessor<'a, T>(handle: *const T) -> &'a T {
    // SAFETY: the caller upholds this function's contract.
    unsafe { &*handle }
}

/// Hands an owned value to the host as a handle it releases exactly once.
pub(crate) fn into_handle<T>(value: T) -> *mut T {
    Box::into_raw(Box::new(value))
}

/// Takes back and drops a handle, doing nothing for a null one.
///
/// # Safety
///
/// A non-null `handle` came from [`into_handle`] and has not been released.
pub(crate) unsafe fn release<T>(handle: *mut T) {
    if handle.is_null() {
        return;
    }
    // SAFETY: the caller guarantees `handle` came from `into_handle` and is released once.
    drop(unsafe { Box::from_raw(handle) });
}

/// Writes an out-parameter.
///
/// # Safety
///
/// A non-null `out` points to writable memory for a `T`.
pub(crate) unsafe fn write<T>(out: *mut T, value: T) {
    if out.is_null() {
        return;
    }
    // SAFETY: `out` is non-null and the caller guarantees it is writable for a `T`.
    unsafe { ptr::write(out, value) };
}

/// Writes a borrowed byte string as a pointer and a length.
///
/// # Safety
///
/// Non-null `data` and `len` point to writable memory for their types.
pub(crate) unsafe fn write_bytes(data: *mut *const u8, len: *mut usize, bytes: &[u8]) {
    // SAFETY: the caller upholds this function's contract.
    unsafe {
        write(data, bytes.as_ptr());
        write(len, bytes.len());
    }
}

/// Checks that a fallible call's result pointer can be written before doing any work.
pub(crate) fn require_out<T>(out: *mut T, argument: &'static str) -> Result<(), Failure> {
    if out.is_null() {
        return Err(Failure::invalid_argument(argument, "is required"));
    }
    Ok(())
}

/// Borrows caller memory a column is written into.
///
/// # Safety
///
/// A non-null `data` points to `len` writable bytes that outlive `'a` and nothing else reads.
pub(crate) unsafe fn output_slice<'a, T>(
    data: *mut T,
    len: usize,
    argument: &'static str,
) -> Result<&'a mut [T], Failure> {
    if data.is_null() {
        return Err(Failure::invalid_argument(argument, "is required"));
    }
    // SAFETY: `data` is non-null and the caller guarantees `len` writable elements.
    Ok(unsafe { slice::from_raw_parts_mut(data, len) })
}

/// The return value of a fallible call: null on success, otherwise the failure the host owns.
pub(crate) fn outcome(result: Result<(), Failure>) -> *mut Failure {
    match result {
        Ok(()) => ptr::null_mut(),
        Err(failure) => into_handle(failure),
    }
}
