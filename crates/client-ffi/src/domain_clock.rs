//! The clock of a domain a session follows, as a host reads it: `nx_domain_clock`, its typed
//! fields, and the arithmetic that projects it.
//!
//! Layer: edges.
//!
//! - **Owns.** One read of the clock the session keeps for an attached domain, the shared
//!   ownership a host retains and releases, reading its domain, generation, state, committed
//!   mapping and latest tick, and the logical time, physical waits and admission it projects.
//! - **Depends on.** The Rust client's attached domain clock, whose arithmetic is the one the
//!   ingestor admits by, and the clock states and failures the header names.
//! - **Must not know.** How the session keeps the clock current or restores its attachment, which
//!   the Rust client decides, or the clock events a host reads beside it.
//!
//! A read owns the clock it copied and never changes, so a host reads every field without
//! allocating: scalars are written to out-parameters and the domain name is borrowed from the read
//! for as long as a reference to it is held.

use std::mem::ManuallyDrop;

use meticulous::ResultExt as _;
use nervix_client_core::{
    AttachedDomainClock, DomainAdmissionWindow, DomainClockObservedState,
    DomainClockTickObservation, DomainName, PacedDomainClock, Timestamp,
};
use triomphe::Arc;

use crate::{
    abi,
    clock_event::ClockState,
    failure::{Failure, FailureKind},
};

/// The clock of one domain as the session held it when a host read it, shared by every reference
/// the host holds.
#[derive(Debug)]
pub struct DomainClock {
    clock: AttachedDomainClock,
}

impl DomainClock {
    pub(crate) fn new(clock: AttachedDomainClock) -> Self {
        Self { clock }
    }

    /// Hands the read to a host as its first reference.
    pub(crate) fn into_shared(self) -> *mut Self {
        Arc::into_raw(Arc::new(self)).cast_mut()
    }

    /// The domain whose clock this is.
    pub fn domain(&self) -> &DomainName {
        self.clock.domain()
    }

    /// The `START` generation the clock belongs to.
    pub fn generation(&self) -> u64 {
        self.clock.clock().generation
    }

    /// The installation state of the generation on the serving node.
    pub fn state(&self) -> ClockState {
        ClockState::from(&self.clock.clock().state)
    }

    /// The committed clock of a paced generation.
    pub fn paced(&self) -> Result<&PacedDomainClock, Failure> {
        match &self.clock.clock().state {
            DomainClockObservedState::Paced(paced) => Ok(paced),
            other => Err(Failure::new(
                FailureKind::Type,
                format!(
                    "the domain clock is {:?}, which has no committed mapping",
                    ClockState::from(other)
                ),
            )),
        }
    }

    /// The newest tick the session accepted for the clock's generation, if it holds one.
    pub fn tick(&self) -> Option<&DomainClockTickObservation> {
        self.clock.latest_tick()
    }

    /// The domain's logical time at the UTC instant `utc`.
    pub fn logical_time_at(&self, utc: Timestamp) -> Result<Timestamp, Failure> {
        self.clock.logical_time_at(utc).map_err(Failure::from)
    }

    /// The nanoseconds after the UTC instant `utc` at which the domain's logical time reaches
    /// `target`, rounded up.
    pub fn wall_nanos_until(&self, utc: Timestamp, target: Timestamp) -> Result<u64, Failure> {
        let wait = self
            .clock
            .wall_duration_until(utc, target)
            .map_err(Failure::from)?;
        let nanos = u64::try_from(wait.as_nanos()).assured(
            "a paced wait is built from u64 nanoseconds, and an unpaced one is the distance \
             between two signed 64-bit nanosecond instants, which u64 holds",
        );
        Ok(nanos)
    }

    /// The tick centers a `TIMESTAMP AT` ingestor of the domain admits events around at the UTC
    /// instant `utc`, or `None` for an unpaced clock, whose ingestors admit every timestamp.
    pub fn admission_window(
        &self,
        utc: Timestamp,
    ) -> Result<Option<DomainAdmissionWindow>, Failure> {
        self.clock.admission_window(utc).map_err(Failure::from)
    }

    /// Whether a `TIMESTAMP AT` ingestor of the domain admits an event timestamp at the UTC
    /// instant `utc`.
    pub fn admits(&self, utc: Timestamp, event: Timestamp) -> Result<bool, Failure> {
        let window = self.admission_window(utc)?;
        match window {
            Some(window) => Ok(window.contains(event)),
            None => Ok(true),
        }
    }
}

/// # Safety
///
/// `clock` is a live clock this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_domain(
    clock: *const DomainClock,
    domain: *mut *const u8,
    domain_len: *mut usize,
) {
    // SAFETY: the header requires a live clock and writable out-parameters.
    unsafe {
        let clock = abi::accessor(clock);
        abi::write_bytes(domain, domain_len, clock.domain().as_str().as_bytes());
    }
}

/// # Safety
///
/// `clock` is a live clock this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_generation(clock: *const DomainClock) -> u64 {
    // SAFETY: the header requires a live clock.
    unsafe { abi::accessor(clock) }.generation()
}

/// # Safety
///
/// `clock` is a live clock this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_state(clock: *const DomainClock) -> ClockState {
    // SAFETY: the header requires a live clock.
    unsafe { abi::accessor(clock) }.state()
}

/// # Safety
///
/// `clock` is a live clock; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_paced(
    clock: *const DomainClock,
    period_nanos: *mut u64,
    skew_nanos: *mut u64,
    logical_origin: *mut i64,
    utc_anchor: *mut i64,
    time_rate: *mut f64,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_paced(
            clock,
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
/// As [`nx_domain_clock_paced`].
unsafe fn write_paced(
    clock: *const DomainClock,
    period_nanos: *mut u64,
    skew_nanos: *mut u64,
    logical_origin: *mut i64,
    utc_anchor: *mut i64,
    time_rate: *mut f64,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live clock.
    let paced = unsafe { abi::handle(clock, "clock") }?.paced()?;
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
/// `clock` is a live clock this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_tick(
    clock: *const DomainClock,
    tick_id: *mut u64,
    logical_boundary: *mut i64,
    authority_utc: *mut i64,
    serving_logical: *mut i64,
) -> bool {
    // SAFETY: the header requires a live clock.
    let clock = unsafe { abi::accessor(clock) };
    let Some(tick) = clock.tick() else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe {
        abi::write(tick_id, tick.tick_id);
        abi::write(logical_boundary, tick.logical_boundary.unix_nanos());
        abi::write(authority_utc, tick.authority_utc.unix_nanos());
        abi::write(serving_logical, tick.serving_logical.unix_nanos());
    }
    true
}

/// # Safety
///
/// `clock` is a live clock; a non-null `logical` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_logical_time_at(
    clock: *const DomainClock,
    utc: i64,
    logical: *mut i64,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_logical_time_at(clock, utc, logical) })
}

/// # Safety
///
/// As [`nx_domain_clock_logical_time_at`].
unsafe fn write_logical_time_at(
    clock: *const DomainClock,
    utc: i64,
    logical: *mut i64,
) -> Result<(), Failure> {
    abi::require_out(logical, "logical")?;
    // SAFETY: the caller guarantees a live clock.
    let clock = unsafe { abi::handle(clock, "clock") }?;
    let projected = clock.logical_time_at(Timestamp::from_unix_nanos(utc))?;
    // SAFETY: `logical` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(logical, projected.unix_nanos()) };
    Ok(())
}

/// # Safety
///
/// `clock` is a live clock; a non-null `wait_nanos` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_wall_duration_until(
    clock: *const DomainClock,
    utc: i64,
    target: i64,
    wait_nanos: *mut u64,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_wall_duration_until(clock, utc, target, wait_nanos) })
}

/// # Safety
///
/// As [`nx_domain_clock_wall_duration_until`].
unsafe fn write_wall_duration_until(
    clock: *const DomainClock,
    utc: i64,
    target: i64,
    wait_nanos: *mut u64,
) -> Result<(), Failure> {
    abi::require_out(wait_nanos, "wait_nanos")?;
    // SAFETY: the caller guarantees a live clock.
    let clock = unsafe { abi::handle(clock, "clock") }?;
    let wait = clock.wall_nanos_until(
        Timestamp::from_unix_nanos(utc),
        Timestamp::from_unix_nanos(target),
    )?;
    // SAFETY: `wait_nanos` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(wait_nanos, wait) };
    Ok(())
}

/// # Safety
///
/// `clock` is a live clock; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_admission_window(
    clock: *const DomainClock,
    utc: i64,
    has_window: *mut bool,
    earliest_center: *mut i64,
    latest_center: *mut i64,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_admission_window(clock, utc, has_window, earliest_center, latest_center)
    })
}

/// # Safety
///
/// As [`nx_domain_clock_admission_window`].
unsafe fn write_admission_window(
    clock: *const DomainClock,
    utc: i64,
    has_window: *mut bool,
    earliest_center: *mut i64,
    latest_center: *mut i64,
) -> Result<(), Failure> {
    abi::require_out(has_window, "has_window")?;
    // SAFETY: the caller guarantees a live clock.
    let clock = unsafe { abi::handle(clock, "clock") }?;
    let window = clock.admission_window(Timestamp::from_unix_nanos(utc))?;
    let Some(window) = window else {
        // SAFETY: `has_window` is non-null, and the caller guarantees it is writable.
        unsafe { abi::write(has_window, false) };
        return Ok(());
    };
    // SAFETY: `has_window` is non-null, and the caller guarantees every out-parameter is
    // writable.
    unsafe {
        abi::write(has_window, true);
        abi::write(earliest_center, window.earliest_center().unix_nanos());
        abi::write(latest_center, window.latest_center().unix_nanos());
    }
    Ok(())
}

/// # Safety
///
/// `clock` is a live clock; a non-null `admitted` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_admits(
    clock: *const DomainClock,
    utc: i64,
    event: i64,
    admitted: *mut bool,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_admits(clock, utc, event, admitted) })
}

/// # Safety
///
/// As [`nx_domain_clock_admits`].
unsafe fn write_admits(
    clock: *const DomainClock,
    utc: i64,
    event: i64,
    admitted: *mut bool,
) -> Result<(), Failure> {
    abi::require_out(admitted, "admitted")?;
    // SAFETY: the caller guarantees a live clock.
    let clock = unsafe { abi::handle(clock, "clock") }?;
    let admits = clock.admits(
        Timestamp::from_unix_nanos(utc),
        Timestamp::from_unix_nanos(event),
    )?;
    // SAFETY: `admitted` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(admitted, admits) };
    Ok(())
}

/// # Safety
///
/// `clock` is a live reference to a clock this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_retain(clock: *mut DomainClock) -> *mut DomainClock {
    // SAFETY: the header requires a live reference, which `into_shared` or this function made
    // from an `Arc`. Wrapping it in `ManuallyDrop` leaves the caller's reference in place.
    let shared = ManuallyDrop::new(unsafe { Arc::from_raw(clock.cast_const()) });
    Arc::into_raw(Arc::clone(&shared)).cast_mut()
}

/// # Safety
///
/// A non-null `clock` is a reference this library returned that has not been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_domain_clock_release(clock: *mut DomainClock) {
    if clock.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased reference, which `into_shared` or
    // `nx_domain_clock_retain` made from an `Arc`.
    drop(unsafe { Arc::from_raw(clock.cast_const()) });
}
