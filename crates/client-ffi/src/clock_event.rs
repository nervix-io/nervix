//! A domain clock event a host holds: `nx_clock_event`, and the typed fields each kind reports.
//!
//! Layer: edges.
//!
//! - **Owns.** The clock event kinds, clock states and end reasons the header names, the shared
//!   ownership a host retains and releases, and reading an event's domain, generation, state,
//!   committed mapping, tick and end reason.
//! - **Depends on.** The Rust client's domain clock events and the vocabulary's clock
//!   observations.
//! - **Must not know.** How attachments are restored or events coalesced, which the session
//!   decides, or relay subscriptions, whose events are a separate stream.
//!
//! An event owns the clock it reports and nothing else, so a host reads every field without
//! allocating: scalars are written to its out-parameters and the domain name is borrowed from the
//! event for as long as a reference to it is held.

use std::mem::ManuallyDrop;

use nervix_client_core::{
    DomainClockAttachmentEndReason, DomainClockEvent, DomainClockObservedState,
    DomainClockTickObservation, DomainName, PacedDomainClock,
};
use nervix_primitives::sync::Arc;

use crate::{
    abi,
    failure::{Failure, FailureKind},
};

/// What a domain clock event reports, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ClockEventKind {
    State = 1,
    Tick = 2,
    Ended = 3,
    Interrupted = 4,
    RestorationFailed = 5,
}

/// The installation state of one clock generation, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ClockState {
    Stopped = 1,
    Uninstalled = 2,
    Unpaced = 3,
    Paced = 4,
}

impl From<&DomainClockObservedState> for ClockState {
    fn from(state: &DomainClockObservedState) -> Self {
        match state {
            DomainClockObservedState::Stopped => Self::Stopped,
            DomainClockObservedState::Uninstalled => Self::Uninstalled,
            DomainClockObservedState::Unpaced => Self::Unpaced,
            DomainClockObservedState::Paced(_) => Self::Paced,
        }
    }
}

/// Why the server ended an attachment, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ClockEndReason {
    DomainRemoved = 1,
}

impl From<DomainClockAttachmentEndReason> for ClockEndReason {
    fn from(reason: DomainClockAttachmentEndReason) -> Self {
        match reason {
            DomainClockAttachmentEndReason::DomainRemoved => Self::DomainRemoved,
        }
    }
}

/// One domain clock event, shared by every reference a host holds.
#[derive(Debug)]
pub struct ClockEvent {
    event: DomainClockEvent,
}

impl ClockEvent {
    pub(crate) fn new(event: DomainClockEvent) -> Self {
        Self { event }
    }

    /// Hands the event to a host as its first reference.
    pub(crate) fn into_shared(self) -> *mut Self {
        Arc::into_raw(Arc::new(self)).cast_mut()
    }

    pub fn kind(&self) -> ClockEventKind {
        match &self.event {
            DomainClockEvent::Observed(_) => ClockEventKind::State,
            DomainClockEvent::Ticked(_) => ClockEventKind::Tick,
            DomainClockEvent::Ended(_) => ClockEventKind::Ended,
            DomainClockEvent::Interrupted(_) => ClockEventKind::Interrupted,
            DomainClockEvent::RestorationFailed(_) => ClockEventKind::RestorationFailed,
        }
    }

    /// The domain whose clock the event concerns.
    pub fn domain(&self) -> &DomainName {
        self.event.domain()
    }

    /// The `START` generation a state or tick event belongs to.
    pub fn generation(&self) -> Result<u64, Failure> {
        match &self.event {
            DomainClockEvent::Observed(observed) => Ok(observed.clock.generation),
            DomainClockEvent::Ticked(ticked) => Ok(ticked.tick.generation),
            DomainClockEvent::Ended(_)
            | DomainClockEvent::Interrupted(_)
            | DomainClockEvent::RestorationFailed(_) => Err(self.carries_no("clock generation")),
        }
    }

    /// The installation state a state event reports.
    pub fn state(&self) -> Result<ClockState, Failure> {
        Ok(ClockState::from(self.observed_state()?))
    }

    /// The committed clock of a state event whose generation is paced.
    pub fn paced(&self) -> Result<&PacedDomainClock, Failure> {
        match self.observed_state()? {
            DomainClockObservedState::Paced(paced) => Ok(paced),
            other => Err(Failure::new(
                FailureKind::Type,
                format!(
                    "the event reports a {:?} clock, which has no committed mapping",
                    ClockState::from(other)
                ),
            )),
        }
    }

    /// The progress a tick event reports.
    pub fn tick(&self) -> Result<&DomainClockTickObservation, Failure> {
        match &self.event {
            DomainClockEvent::Ticked(ticked) => Ok(&ticked.tick),
            DomainClockEvent::Observed(_)
            | DomainClockEvent::Ended(_)
            | DomainClockEvent::Interrupted(_)
            | DomainClockEvent::RestorationFailed(_) => Err(self.carries_no("tick")),
        }
    }

    /// Why the server ended the attachment an end event reports.
    pub fn end_reason(&self) -> Result<ClockEndReason, Failure> {
        match &self.event {
            DomainClockEvent::Ended(ended) => Ok(ClockEndReason::from(ended.reason)),
            DomainClockEvent::Observed(_)
            | DomainClockEvent::Ticked(_)
            | DomainClockEvent::Interrupted(_)
            | DomainClockEvent::RestorationFailed(_) => Err(self.carries_no("end reason")),
        }
    }

    fn observed_state(&self) -> Result<&DomainClockObservedState, Failure> {
        match &self.event {
            DomainClockEvent::Observed(observed) => Ok(&observed.clock.state),
            DomainClockEvent::Ticked(_)
            | DomainClockEvent::Ended(_)
            | DomainClockEvent::Interrupted(_)
            | DomainClockEvent::RestorationFailed(_) => Err(self.carries_no("clock state")),
        }
    }

    /// The failure of reading a field the event's kind does not carry.
    fn carries_no(&self, field: &str) -> Failure {
        Failure::new(
            FailureKind::Type,
            format!("a {:?} clock event carries no {field}", self.kind()),
        )
    }
}

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
