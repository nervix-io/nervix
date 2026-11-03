//! `nx_batch`: a typed Arrow batch of a client endpoint as a host holds it, and copying its
//! columns one level at a time into memory the host passes.
//!
//! - **Owns.** The shared ownership of a batch a host retains and releases, and the calls that
//!   copy the states, list offsets and values of one level of one column.
//! - **Depends on.** The shared batch and the C ABI boundary.
//! - **Must not know.** How a batch is decoded, built or read; the shared batch decides that.

use std::mem::ManuallyDrop;

use nervix_client_host::{Batch, Failure, Schema};
use nervix_primitives::sync::Arc;

use crate::abi;

/// # Safety
///
/// `batch` is a live batch this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_row_count(batch: *const Batch) -> usize {
    // SAFETY: the header requires a live batch.
    unsafe { abi::accessor(batch) }.row_count()
}

/// # Safety
///
/// `batch` is a live batch; a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_schema(
    batch: *const Batch,
    out: *mut *mut Schema,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_schema(batch, out) })
}

/// # Safety
///
/// As [`nx_batch_schema`].
unsafe fn write_schema(batch: *const Batch, out: *mut *mut Schema) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live batch.
    let schema = unsafe { abi::handle(batch, "batch") }?.schema().clone();
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(schema)) };
    Ok(())
}

/// # Safety
///
/// `batch` is a live batch; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_ipc(
    batch: *const Batch,
    ipc: *mut *const u8,
    ipc_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_ipc(batch, ipc, ipc_len) })
}

/// # Safety
///
/// As [`nx_batch_ipc`].
unsafe fn write_ipc(
    batch: *const Batch,
    ipc: *mut *const u8,
    ipc_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(ipc, "ipc")?;
    // SAFETY: the caller guarantees a live batch.
    let bytes = unsafe { abi::handle(batch, "batch") }?.ipc()?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe { abi::write_bytes(ipc, ipc_len, bytes) };
    Ok(())
}

/// # Safety
///
/// `batch` is a live batch; a non-null `cells` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_cells(
    batch: *const Batch,
    column: usize,
    level: usize,
    cells: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_cells(batch, column, level, cells) })
}

/// # Safety
///
/// As [`nx_batch_cells`].
unsafe fn write_cells(
    batch: *const Batch,
    column: usize,
    level: usize,
    cells: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(cells, "cells")?;
    // SAFETY: the caller guarantees a live batch.
    let count = unsafe { abi::handle(batch, "batch") }?.cells(column, level)?;
    // SAFETY: `cells` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(cells, count) };
    Ok(())
}

/// # Safety
///
/// `batch` is a live batch and a non-null `states` addresses `states_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_states(
    batch: *const Batch,
    column: usize,
    states: *mut u8,
    states_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_states(batch, column, states, states_len) })
}

/// # Safety
///
/// As [`nx_batch_states`].
unsafe fn write_states(
    batch: *const Batch,
    column: usize,
    states: *mut u8,
    states_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live batch and writable states.
    let (batch, states) = unsafe {
        (
            abi::handle(batch, "batch")?,
            abi::output_slice(states, states_len, "states")?,
        )
    };
    batch.states(column, states)
}

/// # Safety
///
/// `batch` is a live batch and a non-null `offsets` addresses `offsets_len` writable entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_offsets(
    batch: *const Batch,
    column: usize,
    level: usize,
    offsets: *mut u64,
    offsets_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_offsets(batch, column, level, offsets, offsets_len) })
}

/// # Safety
///
/// As [`nx_batch_offsets`].
unsafe fn write_offsets(
    batch: *const Batch,
    column: usize,
    level: usize,
    offsets: *mut u64,
    offsets_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live batch and writable offsets.
    let (batch, offsets) = unsafe {
        (
            abi::handle(batch, "batch")?,
            abi::output_slice(offsets, offsets_len, "offsets")?,
        )
    };
    batch.offsets(column, level, offsets)
}

/// # Safety
///
/// `batch` is a live batch and a non-null `values` addresses `values_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_fixed(
    batch: *const Batch,
    column: usize,
    level: usize,
    values: *mut std::ffi::c_void,
    values_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_fixed(batch, column, level, values.cast::<u8>(), values_len) })
}

/// # Safety
///
/// As [`nx_batch_fixed`].
unsafe fn write_fixed(
    batch: *const Batch,
    column: usize,
    level: usize,
    values: *mut u8,
    values_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live batch and writable values.
    let (batch, values) = unsafe {
        (
            abi::handle(batch, "batch")?,
            abi::output_slice(values, values_len, "values")?,
        )
    };
    batch.fixed(column, level, values)
}

/// # Safety
///
/// `batch` is a live batch; a non-null `offsets` addresses `offsets_len` writable entries, a
/// non-null `data` addresses `data_capacity` writable bytes, and a non-null `data_len` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_varlen(
    batch: *const Batch,
    column: usize,
    level: usize,
    offsets: *mut u64,
    offsets_len: usize,
    data: *mut u8,
    data_capacity: usize,
    data_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_varlen(
            batch,
            column,
            level,
            offsets,
            offsets_len,
            data,
            data_capacity,
            data_len,
        )
    })
}

/// # Safety
///
/// As [`nx_batch_varlen`].
#[expect(
    clippy::too_many_arguments,
    reason = "the C ABI passes each caller buffer as a pointer and a length"
)]
unsafe fn write_varlen(
    batch: *const Batch,
    column: usize,
    level: usize,
    offsets: *mut u64,
    offsets_len: usize,
    data: *mut u8,
    data_capacity: usize,
    data_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(data_len, "data_len")?;
    // SAFETY: the caller guarantees a live batch.
    let batch = unsafe { abi::handle(batch, "batch") }?;
    let needed = batch.varlen_len(column, level)?;
    // SAFETY: `data_len` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(data_len, needed) };
    if data.is_null() {
        return Ok(());
    }
    if data_capacity < needed {
        return Err(Failure::invalid_argument(
            "data_capacity",
            &format!("is {data_capacity} bytes, and the level needs {needed}"),
        ));
    }
    // SAFETY: the caller guarantees writable offsets, and `data` addresses at least `needed`
    // writable bytes.
    let (offsets, data) = unsafe {
        (
            abi::output_slice(offsets, offsets_len, "offsets")?,
            abi::output_slice(data, needed, "data")?,
        )
    };
    batch.varlen(column, level, offsets, data)
}

/// # Safety
///
/// `batch` is a live reference to a batch this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_retain(batch: *mut Batch) -> *mut Batch {
    // SAFETY: the header requires a live reference, which `into_shared` or this function made
    // from an `Arc`. Wrapping it in `ManuallyDrop` leaves the caller's reference in place.
    let shared = ManuallyDrop::new(unsafe { Arc::from_raw(batch.cast_const()) });
    Arc::into_raw(Arc::clone(&shared)).cast_mut()
}

/// # Safety
///
/// A non-null `batch` is a reference this library returned that has not been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_release(batch: *mut Batch) {
    if batch.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased reference, which `into_shared` or
    // `nx_batch_retain` made from an `Arc`.
    drop(unsafe { Arc::from_raw(batch.cast_const()) });
}
