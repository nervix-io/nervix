//! `nx_event`: a subscription event a host holds, and bulk access to the rows it carries.
//!
//! Layer: edges.
//!
//! - **Owns.** The shared ownership of an event a host retains and releases, and the calls that
//!   copy a column into memory the host passes or borrow one value from the frame.
//! - **Depends on.** The shared event, the schema handle and the C ABI boundary.
//! - **Must not know.** How events are routed or restored; the session decides that.
//!
//! A rows event owns the verified frame it was decoded from and nothing else, so retaining one
//! keeps exactly that frame alive.

use std::mem::ManuallyDrop;

use nervix_client_host::{Event, EventKind, Failure, Schema};
use nervix_primitives::sync::Arc;

use crate::abi;

/// # Safety
///
/// `event` is a live event this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_kind_of(event: *const Event) -> EventKind {
    // SAFETY: the header requires a live event.
    unsafe { abi::accessor(event) }.kind()
}

/// # Safety
///
/// `event` is a live event this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_subscription(
    event: *const Event,
    name: *mut *const u8,
    name_len: *mut usize,
    generation: *mut u64,
) {
    // SAFETY: the header requires a live event and writable out-parameters.
    unsafe {
        let subscription = abi::accessor(event).subscription_event().subscription();
        abi::write_bytes(name, name_len, subscription.name.as_str().as_bytes());
        abi::write(generation, subscription.generation.get());
    }
}

/// # Safety
///
/// `event` is a live event this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_row_count(event: *const Event) -> u64 {
    // SAFETY: the header requires a live event.
    unsafe { abi::accessor(event) }.row_count()
}

/// # Safety
///
/// `event` is live; `message` and `message_len` are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_restoration_message(
    event: *const Event,
    message: *mut *const u8,
    message_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the caller guarantees a live event and writable out-parameters.
    abi::outcome((|| unsafe {
        abi::require_out(message, "message")?;
        abi::require_out(message_len, "message_len")?;
        let value = abi::handle(event, "event")?.restoration_message()?;
        abi::write_bytes(message, message_len, value.as_bytes());
        Ok(())
    })())
}

/// # Safety
///
/// `event` is live; `retry_after_nanos` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_restoration_retry_after(
    event: *const Event,
    retry_after_nanos: *mut u64,
) -> *mut Failure {
    // SAFETY: the caller guarantees a live event and a writable out-parameter.
    abi::outcome((|| unsafe {
        abi::require_out(retry_after_nanos, "retry_after_nanos")?;
        let value = abi::handle(event, "event")?.restoration_retry_after()?;
        let nanos = u64::try_from(value.as_nanos()).map_err(|_| {
            Failure::new(
                nervix_client_host::FailureKind::Protocol,
                "restoration wait exceeds u64 nanoseconds",
            )
        })?;
        abi::write(retry_after_nanos, nanos);
        Ok(())
    })())
}

/// # Safety
///
/// `event` is a live reference to an event this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_retain(event: *mut Event) -> *mut Event {
    // SAFETY: the header requires a live reference, which `into_shared` or this function made
    // from an `Arc`. Wrapping it in `ManuallyDrop` leaves the caller's reference in place.
    let shared = ManuallyDrop::new(unsafe { Arc::from_raw(event.cast_const()) });
    Arc::into_raw(Arc::clone(&shared)).cast_mut()
}

/// # Safety
///
/// A non-null `event` is a reference this library returned that has not been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_release(event: *mut Event) {
    if event.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased reference, which `into_shared` or
    // `nx_event_retain` made from an `Arc`.
    drop(unsafe { Arc::from_raw(event.cast_const()) });
}

/// # Safety
///
/// `event` is a live event; a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_schema(
    event: *const Event,
    out: *mut *mut Schema,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_schema(event, out) })
}

/// # Safety
///
/// As [`nx_event_schema`].
unsafe fn write_schema(event: *const Event, out: *mut *mut Schema) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live event.
    let schema = unsafe { abi::handle(event, "event") }?.schema()?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(schema)) };
    Ok(())
}

/// # Safety
///
/// `event` is a live event; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_frame(
    event: *const Event,
    frame: *mut *const u8,
    frame_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_frame(event, frame, frame_len) })
}

/// # Safety
///
/// As [`nx_event_frame`].
unsafe fn write_frame(
    event: *const Event,
    frame: *mut *const u8,
    frame_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(frame, "frame")?;
    // SAFETY: the caller guarantees a live event.
    let bytes = unsafe { abi::handle(event, "event") }?.frame()?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe { abi::write_bytes(frame, frame_len, bytes) };
    Ok(())
}

/// # Safety
///
/// `event` is a live event and a non-null `states` addresses `states_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_column_states(
    event: *const Event,
    part: i32,
    column: usize,
    states: *mut u8,
    states_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_states(event, part, column, states, states_len) })
}

/// # Safety
///
/// As [`nx_event_column_states`].
unsafe fn write_states(
    event: *const Event,
    part: i32,
    column: usize,
    states: *mut u8,
    states_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live event and writable states.
    let (event, states) = unsafe {
        (
            abi::handle(event, "event")?,
            abi::output_slice(states, states_len, "states")?,
        )
    };
    event.column_states(abi::part(part)?, column, states)
}

/// # Safety
///
/// `event` is a live event and a non-null `values` addresses `values_len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_column_fixed(
    event: *const Event,
    part: i32,
    column: usize,
    values: *mut std::ffi::c_void,
    values_len: usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_fixed(event, part, column, values.cast::<u8>(), values_len) })
}

/// # Safety
///
/// As [`nx_event_column_fixed`].
unsafe fn write_fixed(
    event: *const Event,
    part: i32,
    column: usize,
    values: *mut u8,
    values_len: usize,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live event and writable values.
    let (event, values) = unsafe {
        (
            abi::handle(event, "event")?,
            abi::output_slice(values, values_len, "values")?,
        )
    };
    event.column_fixed(abi::part(part)?, column, values)
}

/// # Safety
///
/// `event` is a live event; a non-null `offsets` addresses `offsets_len` writable entries, a
/// non-null `data` addresses `data_capacity` writable bytes, and a non-null `data_len` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_column_varlen(
    event: *const Event,
    part: i32,
    column: usize,
    offsets: *mut u64,
    offsets_len: usize,
    data: *mut u8,
    data_capacity: usize,
    data_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_varlen(
            event,
            part,
            column,
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
/// As [`nx_event_column_varlen`].
#[expect(
    clippy::too_many_arguments,
    reason = "the C ABI passes each caller buffer as a pointer and a length"
)]
unsafe fn write_varlen(
    event: *const Event,
    part: i32,
    column: usize,
    offsets: *mut u64,
    offsets_len: usize,
    data: *mut u8,
    data_capacity: usize,
    data_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(data_len, "data_len")?;
    // SAFETY: the caller guarantees a live event.
    let event = unsafe { abi::handle(event, "event") }?;
    let part = abi::part(part)?;
    let needed = event.column_varlen_len(part, column)?;
    // SAFETY: `data_len` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(data_len, needed) };
    if data.is_null() {
        return Ok(());
    }
    if data_capacity < needed {
        return Err(Failure::invalid_argument(
            "data_capacity",
            &format!("is {data_capacity} bytes, and the column needs {needed}"),
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
    event.column_varlen(part, column, offsets, data)
}

/// # Safety
///
/// `event` is a live event; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_event_cell_varlen(
    event: *const Event,
    part: i32,
    row: usize,
    column: usize,
    value: *mut *const u8,
    value_len: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_cell(event, part, row, column, value, value_len) })
}

/// # Safety
///
/// As [`nx_event_cell_varlen`].
unsafe fn write_cell(
    event: *const Event,
    part: i32,
    row: usize,
    column: usize,
    value: *mut *const u8,
    value_len: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(value, "value")?;
    // SAFETY: the caller guarantees a live event.
    let event = unsafe { abi::handle(event, "event") }?;
    let bytes = event.cell_varlen(abi::part(part)?, row, column)?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe { abi::write_bytes(value, value_len, bytes) };
    Ok(())
}
