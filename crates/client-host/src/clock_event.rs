//! A domain clock event a host reads, and the typed fields each kind reports.
//!
//! Layer: edges.
//!
//! - **Owns.** The clock event kinds, clock states and end reasons a host reads, and reading an
//!   event's domain, generation, state, committed mapping, tick and end reason.
//! - **Depends on.** The Rust client's domain clock events and the vocabulary's clock
//!   observations.
//! - **Must not know.** How attachments are restored or events coalesced, which the session
//!   decides, or relay subscriptions, whose events are a separate stream.
//!
//! An event owns the clock it reports and nothing else, so every field is read without
//! allocating, and the domain name is borrowed from the event.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_client_core::{
    DomainClockAttachmentEndReason, DomainClockEvent, DomainClockObservedState,
    DomainClockTickObservation, DomainName, PacedDomainClock,
};

use crate::failure::{Failure, FailureKind};

/// What a domain clock event reports, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ClockEventKind {
    State = 1,
    Tick = 2,
    Ended = 3,
    Interrupted = 4,
    RestorationFailed = 5,
}

/// The installation state of one clock generation, with its stable values.
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

/// Why the server ended an attachment, with its stable values.
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
    pub fn new(event: DomainClockEvent) -> Self {
        Self { event }
    }

    /// The event as the Rust client reported it, with what a host's accessors do not name, such
    /// as the message and wait of a refused restoration.
    pub fn domain_clock_event(&self) -> &DomainClockEvent {
        &self.event
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
