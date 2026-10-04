//! Following the domain clock the session attached to, and pacing the simulation by it.
//!
//! - **Owns.** Attaching the session to the domain's clock, the pace the latest clock observation
//!   allows, printing clock observations, the tick centers of a generation, and waiting until the
//!   clock reaches one.
//! - **Depends on.** The Rust client's clock attachment, its events and the projections of its
//!   attached clock.
//! - **Must not know.** Producers, consumers or files.
//!
//! The simulation never computes domain time itself: every wait and every admission check comes
//! from the attached clock's projections. A gap in the session, a stopped or uninstalled clock, or
//! a new START generation pauses the simulation, because the clock it read before can no longer
//! say which readings the ingestor admits.

use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_client_core::{
    Client, DomainClockAttachDisposition, DomainClockAttachmentEndReason, DomainClockEvent,
    DomainClockObservation, DomainClockObservedState, DomainClockReadError, DomainName, Timestamp,
};
use nervix_models::{DomainAdmissionWindow, PacedDomainClock};
use nervix_primitives::{
    sync::{CancellationToken, watch},
    task::JoinHandle,
};
use thiserror::Error;

use crate::report::{self, Report as Counters};

/// How long the clock follower waits before asking again for events after the session could not
/// be reopened.
const UNAVAILABLE_RETRY: Duration = Duration::from_secs(1);

/// Why the simulation cannot pace itself by the domain's clock.
#[derive(Debug, Error)]
pub(crate) enum ClockError {
    #[error("cannot attach to the clock of domain '{domain}': {message}")]
    Attach { domain: DomainName, message: String },
    #[error(
        "the clock of domain '{domain}' is stopped at generation {generation}; START the domain \
         before running the simulation"
    )]
    Stopped { domain: DomainName, generation: u64 },
    #[error(
        "the clock of domain '{domain}' is unpaced at generation {generation}; the simulation \
         paces its readings by the tick centers of a paced domain"
    )]
    Unpaced { domain: DomainName, generation: u64 },
    #[error(
        "the clock of domain '{domain}' is not installed on the serving node within {}",
        duration_text(*.waited)
    )]
    Uninstalled {
        domain: DomainName,
        waited: Duration,
    },
    #[error(
        "the clock of domain '{domain}' changed while the simulation started; run the simulation \
         again"
    )]
    Changed { domain: DomainName },
    #[error("the clock of domain '{domain}' leaves the range of timestamps")]
    Arithmetic { domain: DomainName },
}

/// What the latest clock observation lets the simulation do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pace {
    /// The session observed this generation installed and paced since its last gap.
    Paced { generation: u64 },
    /// The serving node holds this generation without its mapping, or the session's attachment is
    /// being restored after a gap. The clock read before cannot be trusted until it is observed
    /// again.
    Paused { generation: u64 },
    /// The generation is stopped and runs no domain work.
    Stopped { generation: u64 },
    /// The generation reads actual UTC: it has no tick centers to pace by.
    Unpaced { generation: u64 },
    /// The server ended the attachment because the domain was removed.
    Ended,
}

impl Pace {
    fn of(clock: &DomainClockObservation) -> Self {
        let generation = clock.generation;
        match clock.state {
            DomainClockObservedState::Paced(_) => Self::Paced { generation },
            DomainClockObservedState::Uninstalled => Self::Paused { generation },
            DomainClockObservedState::Stopped => Self::Stopped { generation },
            DomainClockObservedState::Unpaced => Self::Unpaced { generation },
        }
    }

    /// The pace after the session holding the attachment ended.
    const fn interrupted(self) -> Self {
        match self {
            Self::Paced { generation } | Self::Paused { generation } => Self::Paused { generation },
            Self::Stopped { .. } | Self::Unpaced { .. } | Self::Ended => self,
        }
    }
}

/// The text of a duration the way NSPL writes it, in the largest unit that divides it.
pub(crate) fn duration_text(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos.is_multiple_of(1_000_000_000) {
        return format!("{}s", nanos / 1_000_000_000);
    }
    if nanos.is_multiple_of(1_000_000) {
        return format!("{}ms", nanos / 1_000_000);
    }
    if nanos.is_multiple_of(1_000) {
        return format!("{}us", nanos / 1_000);
    }
    format!("{nanos}ns")
}

/// The report line of a clock observation.
pub(crate) fn describe(clock: &DomainClockObservation) -> String {
    let generation = clock.generation;
    match &clock.state {
        DomainClockObservedState::Paced(paced) => format!(
            "CLOCK generation={generation} state=paced period={} skew={} origin={} anchor={} \
             rate={}",
            duration_text(paced.period.as_duration()),
            duration_text(paced.skew.as_duration()),
            paced.mapping.logical_start().to_rfc3339(),
            paced.mapping.wall_started_at().to_rfc3339(),
            paced.mapping.time_rate(),
        ),
        DomainClockObservedState::Stopped => format!("CLOCK generation={generation} state=stopped"),
        DomainClockObservedState::Uninstalled => {
            format!("CLOCK generation={generation} state=uninstalled")
        }
        DomainClockObservedState::Unpaced => format!("CLOCK generation={generation} state=unpaced"),
    }
}

/// Attaches the session to the clock of `domain` and returns the clock the attach reported.
pub(crate) async fn attach(
    client: &Client,
    domain: &DomainName,
) -> error_stack::Result<DomainClockObservation, ClockError> {
    let refused = |message: String| {
        Report::new(ClockError::Attach {
            domain: domain.clone(),
            message,
        })
    };
    let outcome = match client.attach_domain_clock(domain.clone()).await {
        Ok(outcome) => outcome,
        Err(report) => return Err(refused(report.current_context().to_string())),
    };
    match outcome.disposition {
        DomainClockAttachDisposition::Attached { clock, .. } => Ok(clock),
        DomainClockAttachDisposition::AlreadyAttached(_) => {
            let Some(attached) = client.domain_clock(domain) else {
                return Err(refused(outcome.message));
            };
            Ok(attached.clock().clone())
        }
        DomainClockAttachDisposition::DomainNotFound(_) | DomainClockAttachDisposition::Failed => {
            Err(refused(outcome.message))
        }
    }
}

/// The tick centers of one paced generation: `origin + n × period` for every nonnegative `n`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TickGrid {
    pub(crate) generation: u64,
    origin: Timestamp,
    period: Duration,
}

impl TickGrid {
    pub(crate) fn of(generation: u64, paced: &PacedDomainClock) -> Self {
        Self {
            generation,
            origin: paced.mapping.logical_start(),
            period: paced.period.as_duration(),
        }
    }

    /// The newest tick center at or before the logical instant `logical`.
    pub(crate) fn reached(&self, logical: Timestamp) -> u64 {
        let Some(elapsed) = logical.duration_since(self.origin) else {
            return 0;
        };
        let period = self.period.as_nanos();
        u64::try_from(elapsed.as_nanos() / period).assured(
            "a span between two signed nanosecond timestamps holds fewer than 2^64 periods",
        )
    }

    /// The tick center at position `tick`, or `None` past the representable timestamps.
    pub(crate) fn center(&self, tick: u64) -> Option<Timestamp> {
        let offset = u128::from(tick).checked_mul(self.period.as_nanos())?;
        let offset = u64::try_from(offset).ok()?;
        self.origin.checked_add(Duration::from_nanos(offset)).ok()
    }
}

/// Where the clock is, relative to a tick center the simulation waits for.
#[derive(Debug)]
pub(crate) enum Reached {
    /// The clock reached the center; the window is the one the ingestor admits event times in now.
    Center(DomainAdmissionWindow),
    /// The generation the simulation paced by ended, stopped or was replaced.
    Moved(Pace),
    /// The simulation is stopping.
    Stopping,
}

/// The domain clock the session follows, and the pace its latest observation allows.
pub(crate) struct Clock {
    client: Client,
    domain: DomainName,
    pace: watch::Receiver<Pace>,
}

impl Clock {
    /// Starts following the clock whose attach reported `attached`, printing every observation.
    pub(crate) fn follow(
        client: Client,
        domain: DomainName,
        attached: &DomainClockObservation,
        counters: nervix_primitives::sync::Arc<Counters>,
        stop: CancellationToken,
    ) -> (Self, JoinHandle<()>) {
        let (sender, pace) = watch::channel(Pace::of(attached));
        let follower = Follower {
            client: client.clone(),
            sender,
            counters,
            stop,
        };
        let task = nervix_primitives::task::spawn(follower.run());
        (
            Self {
                client,
                domain,
                pace,
            },
            task,
        )
    }

    pub(crate) fn pace(&self) -> Pace {
        *self.pace.borrow()
    }

    /// Waits for the next change of pace, or for the simulation to stop.
    pub(crate) async fn changed(&mut self, stop: &CancellationToken) -> bool {
        nervix_primitives::select! {
            changed = self.pace.changed() => changed.is_ok(),
            () = stop.cancelled() => false,
        }
    }

    /// The paced clock of `generation` as the session last observed it, if it still has it.
    pub(crate) fn paced(&self, generation: u64) -> Option<PacedDomainClock> {
        let attached = self.client.domain_clock(&self.domain)?;
        let clock = attached.clock();
        if clock.generation != generation {
            return None;
        }
        match &clock.state {
            DomainClockObservedState::Paced(paced) => Some(paced.clone()),
            DomainClockObservedState::Stopped
            | DomainClockObservedState::Uninstalled
            | DomainClockObservedState::Unpaced => None,
        }
    }

    /// The domain's logical time at the host's UTC now, by the clock of `generation`.
    pub(crate) fn logical_now(&self, generation: u64) -> Option<Timestamp> {
        let attached = self.client.domain_clock(&self.domain)?;
        if attached.clock().generation != generation {
            return None;
        }
        attached.logical_time_at(Timestamp::now()).ok()
    }

    /// The window the ingestor admits event times in at the host's UTC now, by the clock of
    /// `generation`.
    pub(crate) fn window(&self, generation: u64) -> Option<DomainAdmissionWindow> {
        let attached = self.client.domain_clock(&self.domain)?;
        if attached.clock().generation != generation {
            return None;
        }
        // A stopped or uninstalled clock has no window to admit event times in.
        let Ok(window) = attached.admission_window(Timestamp::now()) else {
            return None;
        };
        window
    }

    /// Waits until the domain's clock of `generation` reaches `center`.
    pub(crate) async fn reach(
        &mut self,
        generation: u64,
        center: Timestamp,
        stop: &CancellationToken,
    ) -> error_stack::Result<Reached, ClockError> {
        loop {
            nervix_primitives::task::consume_budget().await;
            if stop.is_cancelled() {
                return Ok(Reached::Stopping);
            }
            let pace = *self.pace.borrow_and_update();
            match pace {
                Pace::Paced {
                    generation: current,
                } if current == generation => {}
                Pace::Paused {
                    generation: current,
                } if current == generation => {
                    if !self.changed(stop).await {
                        return Ok(Reached::Stopping);
                    }
                    continue;
                }
                Pace::Paced { .. }
                | Pace::Paused { .. }
                | Pace::Stopped { .. }
                | Pace::Unpaced { .. }
                | Pace::Ended => return Ok(Reached::Moved(pace)),
            }
            let Some(attached) = self.client.domain_clock(&self.domain) else {
                return Ok(Reached::Moved(Pace::Ended));
            };
            if attached.clock().generation != generation {
                // The snapshot follows the event the pace came from; wait for them to agree.
                if !self.changed(stop).await {
                    return Ok(Reached::Stopping);
                }
                continue;
            }
            let now = Timestamp::now();
            let wait = match attached.wall_duration_until(now, center) {
                Ok(wait) => wait,
                Err(report) => match report.current_context() {
                    DomainClockReadError::Stopped { .. }
                    | DomainClockReadError::Uninstalled { .. } => {
                        if !self.changed(stop).await {
                            return Ok(Reached::Stopping);
                        }
                        continue;
                    }
                    DomainClockReadError::Arithmetic { .. } => {
                        return Err(report.change_context(ClockError::Arithmetic {
                            domain: self.domain.clone(),
                        }));
                    }
                },
            };
            if !wait.is_zero() {
                nervix_primitives::select! {
                    () = nervix_primitives::time::sleep(wait) => {}
                    changed = self.pace.changed() => {
                        if changed.is_err() {
                            return Ok(Reached::Moved(Pace::Ended));
                        }
                    }
                    () = stop.cancelled() => return Ok(Reached::Stopping),
                }
                continue;
            }
            match attached.admission_window(now) {
                Ok(Some(window)) => return Ok(Reached::Center(window)),
                Ok(None) => return Ok(Reached::Moved(Pace::Unpaced { generation })),
                Err(report) => {
                    return Err(report.change_context(ClockError::Arithmetic {
                        domain: self.domain.clone(),
                    }));
                }
            }
        }
    }
}

/// The task that reads the clock events of the session and publishes the pace they allow.
struct Follower {
    client: Client,
    sender: watch::Sender<Pace>,
    counters: nervix_primitives::sync::Arc<Counters>,
    stop: CancellationToken,
}

impl Follower {
    async fn run(self) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = nervix_primitives::select! {
                event = self.client.next_domain_clock_event() => event,
                () = self.stop.cancelled() => return,
            };
            let event = match event {
                Ok(event) => event,
                Err(failure) => {
                    report::line(format!(
                        "CLOCK unavailable reason={}",
                        failure.current_context()
                    ));
                    nervix_primitives::select! {
                        () = nervix_primitives::time::sleep(UNAVAILABLE_RETRY) => {}
                        () = self.stop.cancelled() => return,
                    }
                    continue;
                }
            };
            match event {
                DomainClockEvent::Observed(observed) => {
                    report::line(describe(&observed.clock));
                    self.sender.send_replace(Pace::of(&observed.clock));
                }
                DomainClockEvent::Ticked(_) => self.counters.tick(),
                DomainClockEvent::Interrupted(interrupted) => {
                    report::line(format!("INTERRUPTED clock domain={}", interrupted.domain));
                    let paused = self.sender.borrow().interrupted();
                    self.sender.send_replace(paused);
                }
                DomainClockEvent::RestorationFailed(failure) => {
                    report::line(format!(
                        "CLOCK restoration_failed domain={}",
                        failure.domain
                    ));
                }
                DomainClockEvent::Ended(ended) => {
                    let reason = match ended.reason {
                        DomainClockAttachmentEndReason::DomainRemoved => "domain_removed",
                    };
                    report::line(format!(
                        "CLOCK ended domain={} reason={reason}",
                        ended.domain
                    ));
                    self.sender.send_replace(Pace::Ended);
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{DomainClockPeriod, DomainClockSkew, DomainClockState, DomainTimeRate};

    use super::*;

    fn paced(origin: Timestamp) -> PacedDomainClock {
        PacedDomainClock {
            period: DomainClockPeriod::try_from(Duration::from_millis(100))
                .assured("one hundred milliseconds is a valid period"),
            skew: DomainClockSkew::try_from(Duration::from_millis(50))
                .assured("fifty milliseconds is a valid skew"),
            mapping: DomainClockState::new(
                origin,
                origin,
                DomainTimeRate::try_from(4.0).assured("four is a valid rate"),
            ),
        }
    }

    #[test]
    fn durations_print_in_the_largest_unit_that_divides_them() {
        assert_eq!(duration_text(Duration::from_secs(2)), "2s");
        assert_eq!(duration_text(Duration::from_millis(100)), "100ms");
        assert_eq!(duration_text(Duration::from_micros(1_500)), "1500us");
        assert_eq!(duration_text(Duration::from_nanos(7)), "7ns");
        assert_eq!(duration_text(Duration::ZERO), "0s");
    }

    #[test]
    fn tick_centers_step_by_the_period_from_the_logical_origin() {
        let origin = Timestamp::from_unix_nanos(1_000_000_000);
        let grid = TickGrid::of(3, &paced(origin));
        assert_eq!(grid.center(0), Some(origin));
        assert_eq!(
            grid.center(5),
            Some(Timestamp::from_unix_nanos(1_500_000_000))
        );
        assert_eq!(grid.reached(Timestamp::from_unix_nanos(1_549_999_999)), 5);
        assert_eq!(grid.reached(Timestamp::from_unix_nanos(1_550_000_000)), 5);
        assert_eq!(grid.reached(Timestamp::from_unix_nanos(999_999_999)), 0);
        assert_eq!(grid.center(u64::MAX), None);
    }

    #[test]
    fn a_session_gap_pauses_a_running_generation_and_keeps_a_stopped_one() {
        assert_eq!(
            Pace::Paced { generation: 2 }.interrupted(),
            Pace::Paused { generation: 2 }
        );
        assert_eq!(
            Pace::Paused { generation: 2 }.interrupted(),
            Pace::Paused { generation: 2 }
        );
        assert_eq!(
            Pace::Stopped { generation: 2 }.interrupted(),
            Pace::Stopped { generation: 2 }
        );
        assert_eq!(Pace::Ended.interrupted(), Pace::Ended);
    }

    #[test]
    fn every_state_has_its_report_line() {
        let origin = "2030-01-01T00:00:00Z"
            .parse::<Timestamp>()
            .assured("the fixture origin is RFC 3339");
        let paced = DomainClockObservation {
            generation: 2,
            state: DomainClockObservedState::Paced(paced(origin)),
        };
        assert_eq!(
            describe(&paced),
            "CLOCK generation=2 state=paced period=100ms skew=50ms origin=2030-01-01T00:00:00Z \
             anchor=2030-01-01T00:00:00Z rate=4"
        );
        let stopped = DomainClockObservation {
            generation: 3,
            state: DomainClockObservedState::Stopped,
        };
        assert_eq!(describe(&stopped), "CLOCK generation=3 state=stopped");
        assert_eq!(Pace::of(&stopped), Pace::Stopped { generation: 3 });
    }
}
