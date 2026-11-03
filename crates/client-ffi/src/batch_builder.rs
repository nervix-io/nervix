//! `nx_batch_builder`: building a typed Arrow batch for a client endpoint from a host's columns.
//!
//! - **Owns.** The builder handle a host holds and frees, and the calls that pass it one level of
//!   one column at a time.
//! - **Depends on.** The shared batch builder, the batch handle and the C ABI boundary.
//! - **Must not know.** What a level may hold; the shared builder refuses what a column cannot.

use nervix_client_host::{Batch, BatchBuilder, Failure, Schema};

use crate::abi;

/// # Safety
///
/// `schema` is a live schema and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_new(
    schema: *const Schema,
    rows: usize,
    out: *mut *mut BatchBuilder,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_new(schema, rows, out) })
}

/// # Safety
///
/// As [`nx_batch_builder_new`].
unsafe fn write_new(
    schema: *const Schema,
    rows: usize,
    out: *mut *mut BatchBuilder,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live schema.
    let schema = unsafe { abi::handle(schema, "schema") }?.clone();
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(BatchBuilder::new(schema, rows))) };
    Ok(())
}

/// # Safety
///
/// `builder` is a live builder no other thread uses, and a non-null `states` addresses
/// `states_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_states(
    builder: *mut BatchBuilder,
    column: usize,
    states: *const u8,
    states_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { set_states(builder, column, states, states_len) })
}

/// # Safety
///
/// As [`nx_batch_builder_states`].
unsafe fn set_states(
    builder: *mut BatchBuilder,
    column: usize,
    states: *const u8,
    states_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live builder no other thread uses and readable states.
    let (builder, states) = unsafe {
        (
            abi::handle_mut(builder, "builder")?,
            abi::input_slice(states, states_len, "states")?,
        )
    };
    builder.states(column, states)
}

/// # Safety
///
/// `builder` is a live builder no other thread uses, and a non-null `offsets` addresses
/// `offsets_len` readable entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_offsets(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    offsets: *const u64,
    offsets_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { set_offsets(builder, column, level, offsets, offsets_len) })
}

/// # Safety
///
/// As [`nx_batch_builder_offsets`].
unsafe fn set_offsets(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    offsets: *const u64,
    offsets_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live builder no other thread uses and readable offsets.
    let (builder, offsets) = unsafe {
        (
            abi::handle_mut(builder, "builder")?,
            abi::input_slice(offsets, offsets_len, "offsets")?,
        )
    };
    builder.offsets(column, level, offsets)
}

/// # Safety
///
/// `builder` is a live builder no other thread uses, and a non-null `values` addresses
/// `values_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_fixed(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    values: *const std::ffi::c_void,
    values_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { set_fixed(builder, column, level, values.cast::<u8>(), values_len) })
}

/// # Safety
///
/// As [`nx_batch_builder_fixed`].
unsafe fn set_fixed(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    values: *const u8,
    values_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live builder no other thread uses and readable values.
    let (builder, values) = unsafe {
        (
            abi::handle_mut(builder, "builder")?,
            abi::input_slice(values, values_len, "values")?,
        )
    };
    builder.fixed(column, level, values)
}

/// # Safety
///
/// `builder` is a live builder no other thread uses; a non-null `offsets` addresses
/// `offsets_len` readable entries and a non-null `data` addresses `data_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_varlen(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    offsets: *const u64,
    offsets_len: usize,
    data: *const u8,
    data_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        set_varlen(builder, column, level, offsets, offsets_len, data, data_len)
    })
}

/// # Safety
///
/// As [`nx_batch_builder_varlen`].
unsafe fn set_varlen(
    builder: *mut BatchBuilder,
    column: usize,
    level: usize,
    offsets: *const u64,
    offsets_len: usize,
    data: *const u8,
    data_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live builder no other thread uses and readable buffers.
    let (builder, offsets, data) = unsafe {
        (
            abi::handle_mut(builder, "builder")?,
            abi::input_slice(offsets, offsets_len, "offsets")?,
            abi::input_slice(data, data_len, "data")?,
        )
    };
    builder.varlen(column, level, offsets, data)
}

/// # Safety
///
/// `builder` is a live builder no other thread uses, and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_finish(
    builder: *mut BatchBuilder,
    out: *mut *mut Batch,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_finished(builder, out) })
}

/// # Safety
///
/// As [`nx_batch_builder_finish`].
unsafe fn write_finished(builder: *mut BatchBuilder, out: *mut *mut Batch) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live builder no other thread uses.
    let batch = unsafe { abi::handle_mut(builder, "builder") }?.finish()?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_shared(batch)) };
    Ok(())
}

/// # Safety
///
/// A non-null `builder` is a builder this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_batch_builder_free(builder: *mut BatchBuilder) {
    // SAFETY: the header requires an unreleased builder or null.
    unsafe { abi::release(builder) };
}
