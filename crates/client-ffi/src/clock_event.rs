//! `nx_clock_event`: a domain clock event a host holds, and the typed fields each kind reports.
//!
//! Layer: edges.
//!
//! - **Owns.** The shared ownership of an event a host retains and releases, and writing an
//!   event's scalars to out-parameters and borrowing its domain name for as long as a reference to
//!   it is held.
//! - **Depends on.** The shared clock event and the C ABI boundary.
//! - **Must not know.** What an event reports; the shared clock event reads it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::mem::ManuallyDrop;

use nervix_client_host::{
    ClockEndReason, ClockEvent, ClockEventKind, ClockState, Failure, RestorationCause,
};
use nervix_primitives::sync::Arc;

use crate::abi;

/// # Safety
///
/// `event` is a live event this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_kind_of(event: *const ClockEvent) -> ClockEventKind {
    // SAFETY: the header requires a live event.
    unsafe { abi::accessor(event) }.kind()
}

/// # Safety
///
/// `event` is a live event this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_domain(
    event: *const ClockEvent,
    domain: *mut *const u8,
    domain_len: *mut usize,
) {
    // SAFETY: the header requires a live event and writable out-parameters.
    unsafe {
        let event = abi::accessor(event);
        abi::write_bytes(domain, domain_len, event.domain().as_str().as_bytes());
    }
}

/// # Safety
///
/// `event` is a live event; a non-null `generation` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_generation(
    event: *const ClockEvent,
    generation: *mut u64,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_generation(event, generation) })
}

/// # Safety
///
/// As [`nx_clock_event_generation`].
unsafe fn write_generation(event: *const ClockEvent, generation: *mut u64) -> Result<(), Failure> {
    abi::require_out(generation, "generation")?;
    // SAFETY: the caller guarantees a live event.
    let value = unsafe { abi::handle(event, "event") }?.generation()?;
    // SAFETY: `generation` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(generation, value) };
    Ok(())
}

/// # Safety
///
/// `event` is a live event; a non-null `state` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_state(
    event: *const ClockEvent,
    state: *mut ClockState,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_state(event, state) })
}

/// # Safety
///
/// As [`nx_clock_event_state`].
unsafe fn write_state(event: *const ClockEvent, state: *mut ClockState) -> Result<(), Failure> {
    abi::require_out(state, "state")?;
    // SAFETY: the caller guarantees a live event.
    let value = unsafe { abi::handle(event, "event") }?.state()?;
    // SAFETY: `state` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(state, value) };
    Ok(())
}

/// # Safety
///
/// `event` is a live event; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_paced(
    event: *const ClockEvent,
    period_nanos: *mut u64,
    skew_nanos: *mut u64,
    logical_origin: *mut i64,
    utc_anchor: *mut i64,
    time_rate: *mut f64,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_paced(
            event,
            period_nanos,
            skew_nanos,
            logical_origin,
            utc_anchor,
            time_rate,
        )
    })
}

/// # Safety
///
/// As [`nx_clock_event_paced`].
unsafe fn write_paced(
    event: *const ClockEvent,
    period_nanos: *mut u64,
    skew_nanos: *mut u64,
    logical_origin: *mut i64,
    utc_anchor: *mut i64,
    time_rate: *mut f64,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live event.
    let paced = unsafe { abi::handle(event, "event") }?.paced()?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe {
        abi::write(period_nanos, paced.period.as_nanos());
        abi::write(skew_nanos, paced.skew.as_nanos());
        abi::write(logical_origin, paced.mapping.logical_start().unix_nanos());
        abi::write(utc_anchor, paced.mapping.wall_started_at().unix_nanos());
        abi::write(time_rate, paced.mapping.time_rate().get());
    }
    Ok(())
}

/// # Safety
///
/// `event` is a live event; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_tick(
    event: *const ClockEvent,
    tick_id: *mut u64,
    logical_boundary: *mut i64,
    authority_utc: *mut i64,
    serving_logical: *mut i64,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_tick(
            event,
            tick_id,
            logical_boundary,
            authority_utc,
            serving_logical,
        )
    })
}

/// # Safety
///
/// As [`nx_clock_event_tick`].
unsafe fn write_tick(
    event: *const ClockEvent,
    tick_id: *mut u64,
    logical_boundary: *mut i64,
    authority_utc: *mut i64,
    serving_logical: *mut i64,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live event.
    let tick = unsafe { abi::handle(event, "event") }?.tick()?;
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe {
        abi::write(tick_id, tick.tick_id);
        abi::write(logical_boundary, tick.logical_boundary.unix_nanos());
        abi::write(authority_utc, tick.authority_utc.unix_nanos());
        abi::write(serving_logical, tick.serving_logical.unix_nanos());
    }
    Ok(())
}

/// # Safety
///
/// `event` is a live event; a non-null `reason` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_end_reason(
    event: *const ClockEvent,
    reason: *mut ClockEndReason,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_end_reason(event, reason) })
}

/// # Safety
///
/// As [`nx_clock_event_end_reason`].
unsafe fn write_end_reason(
    event: *const ClockEvent,
    reason: *mut ClockEndReason,
) -> Result<(), Failure> {
    abi::require_out(reason, "reason")?;
    // SAFETY: the caller guarantees a live event.
    let value = unsafe { abi::handle(event, "event") }?.end_reason()?;
    // SAFETY: `reason` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(reason, value) };
    Ok(())
}

/// # Safety
///
/// `event` is a live event; a non-null `cause` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_restoration_cause(
    event: *const ClockEvent,
    cause: *mut RestorationCause,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_restoration_cause(event, cause) })
}

/// # Safety
///
/// As [`nx_clock_event_restoration_cause`].
unsafe fn write_restoration_cause(
    event: *const ClockEvent,
    cause: *mut RestorationCause,
) -> Result<(), Failure> {
    abi::require_out(cause, "cause")?;
    // SAFETY: the caller guarantees a live event.
    let value = unsafe { abi::handle(event, "event") }?.restoration_cause()?;
    // SAFETY: `cause` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(cause, value) };
    Ok(())
}

/// # Safety
///
/// `event` is a live reference to an event this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_retain(event: *mut ClockEvent) -> *mut ClockEvent {
    // SAFETY: the header requires a live reference, which `into_shared` or this function made
    // from an `Arc`. Wrapping it in `ManuallyDrop` leaves the caller's reference in place.
    let shared = ManuallyDrop::new(unsafe { Arc::from_raw(event.cast_const()) });
    Arc::into_raw(Arc::clone(&shared)).cast_mut()
}

/// # Safety
///
/// A non-null `event` is a reference this library returned that has not been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_clock_event_release(event: *mut ClockEvent) {
    if event.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased reference, which `into_shared` or
    // `nx_clock_event_retain` made from an `Arc`.
    drop(unsafe { Arc::from_raw(event.cast_const()) });
}
