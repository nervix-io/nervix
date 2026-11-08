//! Ruby observations and projections of attached domain clocks.
//!
//! Layer: edges.
//! - **Owns.** Clock/event metadata and Ruby access to the shared clock's exact projections.
//! - **Depends on.** Host clocks, semantic timestamps and magnus typed data.
//! - **Must not know.** Clock authority, attachment restoration or time-establishment policy.

use magnus::{DataTypeFunctions, Error, RModule, Ruby, TypedData, method, prelude::*};
use nervix_client_core::{
    DomainClockEvent, DomainClockTickObservation, PacedDomainClock, Timestamp,
};
use nervix_client_host as host;

use crate::{enums, errors, subscriptions::RestorationFailure};

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::PacedClock", free_immediately)]
struct PacedClock {
    generation: u64,
    period: u64,
    skew: u64,
    origin: i64,
    anchor: i64,
    rate: f64,
}

impl PacedClock {
    fn new(generation: u64, paced: &PacedDomainClock) -> Self {
        Self {
            generation,
            period: paced.period.as_nanos(),
            skew: paced.skew.as_nanos(),
            origin: paced.mapping.logical_start().unix_nanos(),
            anchor: paced.mapping.wall_started_at().unix_nanos(),
            rate: paced.mapping.time_rate().get(),
        }
    }
    fn generation(&self) -> u64 {
        self.generation
    }
    fn period_nanos(&self) -> u64 {
        self.period
    }
    fn skew_nanos(&self) -> u64 {
        self.skew
    }
    fn origin(&self) -> i64 {
        self.origin
    }
    fn anchor(&self) -> i64 {
        self.anchor
    }
    fn rate(&self) -> f64 {
        self.rate
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Tick", free_immediately)]
struct Tick {
    tick: DomainClockTickObservation,
}

impl Tick {
    fn new(tick: &DomainClockTickObservation) -> Self {
        Self { tick: tick.clone() }
    }
    fn generation(&self) -> u64 {
        self.tick.generation
    }
    fn id(&self) -> u64 {
        self.tick.tick_id
    }
    fn boundary(&self) -> i64 {
        self.tick.logical_boundary.unix_nanos()
    }
    fn authority_utc(&self) -> i64 {
        self.tick.authority_utc.unix_nanos()
    }
    fn serving_logical(&self) -> i64 {
        self.tick.serving_logical.unix_nanos()
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::ClockEvent", free_immediately)]
pub(crate) struct ClockEvent {
    event: host::ClockEvent,
}

impl ClockEvent {
    pub(crate) fn new(event: host::ClockEvent) -> Self {
        Self { event }
    }
    fn kind(&self) -> i32 {
        enums::ClockEventKind::code(self.event.kind())
    }
    fn domain(&self) -> &str {
        self.event.domain().as_str()
    }
    fn generation(&self) -> Result<u64, Error> {
        self.event.generation().map_err(errors::raise)
    }
    fn state(&self) -> Result<i32, Error> {
        let state = self.event.state().map_err(errors::raise)?;
        Ok(enums::ClockState::code(state))
    }
    fn paced(&self) -> Result<PacedClock, Error> {
        let generation = self.event.generation().map_err(errors::raise)?;
        let paced = self.event.paced().map_err(errors::raise)?;
        Ok(PacedClock::new(generation, paced))
    }
    fn tick(&self) -> Result<Tick, Error> {
        let tick = self.event.tick().map_err(errors::raise)?;
        Ok(Tick::new(tick))
    }
    fn end_reason(&self) -> Result<i32, Error> {
        let reason = self.event.end_reason().map_err(errors::raise)?;
        Ok(enums::ClockEndReason::code(reason))
    }
    fn restoration_failure(&self) -> Option<RestorationFailure> {
        let DomainClockEvent::RestorationFailed(failure) = self.event.domain_clock_event() else {
            return None;
        };
        Some(RestorationFailure::new(
            failure.cause.into(),
            &failure.message,
            failure.retry_after,
        ))
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::DomainClock", free_immediately)]
pub(crate) struct DomainClock {
    clock: host::DomainClock,
}

impl DomainClock {
    pub(crate) fn new(clock: host::DomainClock) -> Self {
        Self { clock }
    }
    fn domain(&self) -> &str {
        self.clock.domain().as_str()
    }
    fn generation(&self) -> u64 {
        self.clock.generation()
    }
    fn state(&self) -> i32 {
        enums::ClockState::code(self.clock.state())
    }
    fn paced(&self) -> Result<PacedClock, Error> {
        let paced = self.clock.paced().map_err(errors::raise)?;
        Ok(PacedClock::new(self.clock.generation(), paced))
    }
    fn tick(&self) -> Option<Tick> {
        self.clock.tick().map(Tick::new)
    }
    fn logical_time_at(&self, utc: i64) -> Result<i64, Error> {
        let logical = self
            .clock
            .logical_time_at(Timestamp::from_unix_nanos(utc))
            .map_err(errors::raise)?;
        Ok(logical.unix_nanos())
    }
    fn wall_duration_until(&self, utc: i64, target: i64) -> Result<u64, Error> {
        self.clock
            .wall_nanos_until(
                Timestamp::from_unix_nanos(utc),
                Timestamp::from_unix_nanos(target),
            )
            .map_err(errors::raise)
    }
    fn admission_window(&self, utc: i64) -> Result<Option<(i64, i64)>, Error> {
        let window = self
            .clock
            .admission_window(Timestamp::from_unix_nanos(utc))
            .map_err(errors::raise)?;
        let Some(window) = window else {
            return Ok(None);
        };
        Ok(Some((
            window.earliest_center().unix_nanos(),
            window.latest_center().unix_nanos(),
        )))
    }
    fn admits(&self, utc: i64, event: i64) -> Result<bool, Error> {
        self.clock
            .admits(
                Timestamp::from_unix_nanos(utc),
                Timestamp::from_unix_nanos(event),
            )
            .map_err(errors::raise)
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let paced = crate::class(ruby, module, "PacedClock")?;
    paced.define_method("generation", method!(PacedClock::generation, 0))?;
    paced.define_method("period_nanos", method!(PacedClock::period_nanos, 0))?;
    paced.define_method("skew_nanos", method!(PacedClock::skew_nanos, 0))?;
    paced.define_method("origin", method!(PacedClock::origin, 0))?;
    paced.define_method("anchor", method!(PacedClock::anchor, 0))?;
    paced.define_method("rate", method!(PacedClock::rate, 0))?;
    let tick = crate::class(ruby, module, "Tick")?;
    tick.define_method("generation", method!(Tick::generation, 0))?;
    tick.define_method("id", method!(Tick::id, 0))?;
    tick.define_method("boundary", method!(Tick::boundary, 0))?;
    tick.define_method("authority_utc", method!(Tick::authority_utc, 0))?;
    tick.define_method("serving_logical", method!(Tick::serving_logical, 0))?;
    let event = crate::class(ruby, module, "ClockEvent")?;
    event.define_method("kind", method!(ClockEvent::kind, 0))?;
    event.define_method("domain", method!(ClockEvent::domain, 0))?;
    event.define_method("generation", method!(ClockEvent::generation, 0))?;
    event.define_method("state", method!(ClockEvent::state, 0))?;
    event.define_method("paced", method!(ClockEvent::paced, 0))?;
    event.define_method("tick", method!(ClockEvent::tick, 0))?;
    event.define_method("end_reason", method!(ClockEvent::end_reason, 0))?;
    event.define_method(
        "restoration_failure",
        method!(ClockEvent::restoration_failure, 0),
    )?;
    let clock = crate::class(ruby, module, "DomainClock")?;
    clock.define_method("domain", method!(DomainClock::domain, 0))?;
    clock.define_method("generation", method!(DomainClock::generation, 0))?;
    clock.define_method("state", method!(DomainClock::state, 0))?;
    clock.define_method("paced", method!(DomainClock::paced, 0))?;
    clock.define_method("tick", method!(DomainClock::tick, 0))?;
    clock.define_method("logical_time_at", method!(DomainClock::logical_time_at, 1))?;
    clock.define_method(
        "wall_duration_until",
        method!(DomainClock::wall_duration_until, 2),
    )?;
    clock.define_method(
        "admission_window",
        method!(DomainClock::admission_window, 1),
    )?;
    clock.define_method("admits", method!(DomainClock::admits, 2))?;
    Ok(())
}
