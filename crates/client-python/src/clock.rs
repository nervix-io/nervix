//! Domain clock events, and the clock of a domain a session follows, as a Python host reads them.
//!
//! - **Owns.** Reading a clock event's kind, domain, generation, state, committed mapping, tick,
//!   end reason and restoration failure, and a followed clock's fields and the logical time,
//!   physical waits and admission it projects.
//! - **Depends on.** The host session runtime's clock event and domain clock, whose arithmetic is
//!   the one the ingestor admits by.
//! - **Must not know.** How the session keeps a clock current or restores its attachment.
//!
//! Every instant is in nanoseconds since the Unix epoch, and every duration in nanoseconds, as
//! the session carries them.

use nervix_client_core::{
    DomainClockEvent, DomainClockTickObservation, PacedDomainClock, Timestamp,
};
use nervix_client_host as host;
use pyo3::prelude::*;

use crate::{
    enums::{ClockEndReason, ClockEventKind, ClockState},
    errors,
    event::RestorationFailure,
};

/// The committed mapping of a paced clock generation.
#[pyclass(frozen, eq, module = "nervix_client")]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PacedClock {
    generation: u64,
    period: u64,
    skew: u64,
    origin: i64,
    anchor: i64,
    rate: f64,
}

impl PacedClock {
    fn of(generation: u64, paced: &PacedDomainClock) -> Self {
        Self {
            generation,
            period: paced.period.as_nanos(),
            skew: paced.skew.as_nanos(),
            origin: paced.mapping.logical_start().unix_nanos(),
            anchor: paced.mapping.wall_started_at().unix_nanos(),
            rate: paced.mapping.time_rate().get(),
        }
    }
}

#[pymethods]
impl PacedClock {
    /// The generation the mapping belongs to.
    #[getter]
    fn generation(&self) -> u64 {
        self.generation
    }

    /// The logical length of one tick, in nanoseconds.
    #[getter]
    fn period(&self) -> u64 {
        self.period
    }

    /// The admission skew around a tick, in nanoseconds.
    #[getter]
    fn skew(&self) -> u64 {
        self.skew
    }

    /// The logical instant the generation started at.
    #[getter]
    fn origin(&self) -> i64 {
        self.origin
    }

    /// The UTC instant the generation started at.
    #[getter]
    fn anchor(&self) -> i64 {
        self.anchor
    }

    /// The logical nanoseconds that pass for every UTC nanosecond.
    #[getter]
    fn rate(&self) -> f64 {
        self.rate
    }

    fn __repr__(&self) -> String {
        format!(
            "PacedClock(generation={}, period={}, skew={}, origin={}, anchor={}, rate={})",
            self.generation, self.period, self.skew, self.origin, self.anchor, self.rate
        )
    }
}

/// One tick of a paced clock generation.
#[pyclass(frozen, eq, module = "nervix_client")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Tick {
    generation: u64,
    id: u64,
    boundary: i64,
    authority_utc: i64,
    serving_logical: i64,
}

impl Tick {
    fn of(tick: &DomainClockTickObservation) -> Self {
        Self {
            generation: tick.generation,
            id: tick.tick_id,
            boundary: tick.logical_boundary.unix_nanos(),
            authority_utc: tick.authority_utc.unix_nanos(),
            serving_logical: tick.serving_logical.unix_nanos(),
        }
    }
}

#[pymethods]
impl Tick {
    #[getter]
    fn generation(&self) -> u64 {
        self.generation
    }

    /// The tick's number in its generation, from one.
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    /// The logical instant the tick ends.
    #[getter]
    fn boundary(&self) -> i64 {
        self.boundary
    }

    /// The UTC instant the clock's authority reported the tick at.
    #[getter]
    fn authority_utc(&self) -> i64 {
        self.authority_utc
    }

    /// The logical time the serving node read when it sent the tick.
    #[getter]
    fn serving_logical(&self) -> i64 {
        self.serving_logical
    }

    fn __repr__(&self) -> String {
        format!(
            "Tick(generation={}, id={}, boundary={})",
            self.generation, self.id, self.boundary
        )
    }
}

/// One event about a domain clock the session follows.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct ClockEvent {
    event: host::ClockEvent,
}

impl ClockEvent {
    pub(crate) fn new(event: host::ClockEvent) -> Self {
        Self { event }
    }
}

#[pymethods]
impl ClockEvent {
    #[getter]
    fn kind(&self) -> ClockEventKind {
        ClockEventKind::from(self.event.kind())
    }

    #[getter]
    fn domain(&self) -> &str {
        self.event.domain().as_str()
    }

    /// The generation a state or a tick reports.
    #[getter]
    fn generation(&self, py: Python<'_>) -> PyResult<u64> {
        self.event
            .generation()
            .map_err(|failure| errors::raise(py, failure))
    }

    /// The state a state event reports.
    #[getter]
    fn state(&self, py: Python<'_>) -> PyResult<ClockState> {
        match self.event.state() {
            Ok(state) => Ok(ClockState::from(state)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The committed mapping a state event reports for a paced clock.
    #[getter]
    fn paced(&self, py: Python<'_>) -> PyResult<PacedClock> {
        let generation = self
            .event
            .generation()
            .map_err(|failure| errors::raise(py, failure))?;
        match self.event.paced() {
            Ok(paced) => Ok(PacedClock::of(generation, paced)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The tick a tick event reports.
    #[getter]
    fn tick(&self, py: Python<'_>) -> PyResult<Tick> {
        match self.event.tick() {
            Ok(tick) => Ok(Tick::of(tick)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Why the server ended the attachment an ended event reports.
    #[getter]
    fn end_reason(&self, py: Python<'_>) -> PyResult<ClockEndReason> {
        match self.event.end_reason() {
            Ok(reason) => Ok(ClockEndReason::from(reason)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The refusal of a restoration the server refused, or `None` for every other event.
    #[getter]
    fn restoration_failure(&self) -> Option<RestorationFailure> {
        let DomainClockEvent::RestorationFailed(failure) = self.event.domain_clock_event() else {
            return None;
        };
        Some(RestorationFailure::new(
            &failure.message,
            failure.retry_after,
        ))
    }
}

/// The clock of a domain the session follows, as the session held it when it was read. A read
/// never changes.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct DomainClock {
    clock: host::DomainClock,
}

impl DomainClock {
    pub(crate) fn new(clock: host::DomainClock) -> Self {
        Self { clock }
    }
}

#[pymethods]
impl DomainClock {
    #[getter]
    fn domain(&self) -> &str {
        self.clock.domain().as_str()
    }

    #[getter]
    fn generation(&self) -> u64 {
        self.clock.generation()
    }

    #[getter]
    fn state(&self) -> ClockState {
        ClockState::from(self.clock.state())
    }

    /// The committed mapping of a paced clock.
    #[getter]
    fn paced(&self, py: Python<'_>) -> PyResult<PacedClock> {
        match self.clock.paced() {
            Ok(paced) => Ok(PacedClock::of(self.clock.generation(), paced)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The newest tick the read holds, or `None` before the generation's first tick.
    #[getter]
    fn tick(&self) -> Option<Tick> {
        let tick = self.clock.tick()?;
        Some(Tick::of(tick))
    }

    /// The logical time the clock reads at UTC instant `utc`.
    fn logical_time_at(&self, py: Python<'_>, utc: i64) -> PyResult<i64> {
        match self.clock.logical_time_at(Timestamp::from_unix_nanos(utc)) {
            Ok(logical) => Ok(logical.unix_nanos()),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The UTC nanoseconds from `utc` until the clock reaches logical instant `target`.
    fn wall_duration_until(&self, py: Python<'_>, utc: i64, target: i64) -> PyResult<u64> {
        let waited = self.clock.wall_nanos_until(
            Timestamp::from_unix_nanos(utc),
            Timestamp::from_unix_nanos(target),
        );
        waited.map_err(|failure| errors::raise(py, failure))
    }

    /// The `(earliest, latest)` logical instants the domain admits at UTC instant `utc`, or `None`
    /// when it admits every instant.
    fn admission_window(&self, py: Python<'_>, utc: i64) -> PyResult<Option<(i64, i64)>> {
        let window = self
            .clock
            .admission_window(Timestamp::from_unix_nanos(utc))
            .map_err(|failure| errors::raise(py, failure))?;
        let Some(window) = window else {
            return Ok(None);
        };
        Ok(Some((
            window.earliest_center().unix_nanos(),
            window.latest_center().unix_nanos(),
        )))
    }

    /// Whether the domain admits an event at logical instant `event` at UTC instant `utc`.
    fn admits(&self, py: Python<'_>, utc: i64, event: i64) -> PyResult<bool> {
        let admitted = self.clock.admits(
            Timestamp::from_unix_nanos(utc),
            Timestamp::from_unix_nanos(event),
        );
        admitted.map_err(|failure| errors::raise(py, failure))
    }
}

/// Adds the domain clock classes to the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ClockEvent>()?;
    module.add_class::<DomainClock>()?;
    module.add_class::<PacedClock>()?;
    module.add_class::<Tick>()?;
    Ok(())
}
