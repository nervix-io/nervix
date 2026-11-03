//! The clock of a domain a session follows, as a host reads it: its typed fields, and the
//! arithmetic that projects it.
//!
//! Layer: edges.
//!
//! - **Owns.** One read of the clock the session keeps for an attached domain, reading its
//!   domain, generation, state, committed mapping and latest tick, and the logical time, physical
//!   waits and admission it projects.
//! - **Depends on.** The Rust client's attached domain clock, whose arithmetic is the one the
//!   ingestor admits by, and the clock states and failures a host reads.
//! - **Must not know.** How the session keeps the clock current or restores its attachment, which
//!   the Rust client decides, or the clock events a host reads beside it.
//!
//! A read owns the clock it copied and never changes, so every field is read without allocating,
//! and the domain name is borrowed from the read.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_client_core::{
    AttachedDomainClock, DomainAdmissionWindow, DomainClockObservedState,
    DomainClockTickObservation, DomainName, PacedDomainClock, Timestamp,
};

use crate::{
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
    pub fn new(clock: AttachedDomainClock) -> Self {
        Self { clock }
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
