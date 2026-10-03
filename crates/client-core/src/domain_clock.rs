//! The domain clocks a client follows across exchange replacements, their observed states and
//! ticks, and the arithmetic a participant in a domain's time runs against the latest clock.
//!
//! - **Owns.** The domains the client asked to follow, their latest clock and accepted tick,
//!   which attachments wait to be restored on a new exchange and why an attempt to restore one
//!   failed, the pending clock events coalesced per domain, and the helper that projects an
//!   attached clock.
//! - **Depends on.** The vocabulary's clock models and arithmetic, and the wire contract's clock
//!   replies and frames.
//! - **Must not know.** How a request is transported, how an exchange routes its frames, or relay
//!   subscriptions.
//!
//! The exchange reader applies every clock reply and frame here before it routes a later frame, so
//! the frames of an attachment always find it. A caller that reads no events never holds the
//! reader up: pending events are coalesced per domain, so they are bounded by the domains the
//! client follows. The newest state and tick of a domain replace older unread ones, and an
//! interruption or an end replaces the observations before it.

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use indexmap::IndexMap;
use meticulous::OptionExt as _;
use nervix_client_wire::{
    DomainClockAttachDisposition, DomainClockAttachOutcome, DomainClockAttachmentEndReason,
    DomainClockAttachmentEnded, DomainClockDetachDisposition, DomainClockDetachOutcome,
    DomainClockObserved, DomainClockTicked,
};
use nervix_models::{
    DomainAdmissionWindow, DomainClockObservation, DomainClockObservedState,
    DomainClockTickObservation, DomainName, PacedDomainClock, Timestamp,
};
use nervix_primitives::sync::{Arc, blocking::Mutex, watch};
use nervix_recovery::Discarded as _;
use thiserror::Error;

/// What a caller reads about the domain clocks the client follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainClockEvent {
    /// The serving node's installation of the clock changed, or the attachment was restored on a
    /// new session and reported the clock again.
    Observed(DomainClockObserved),
    /// The newest tick accepted by the serving node for an installed paced generation.
    Ticked(DomainClockTicked),
    /// The server ended the attachment. Nothing more follows about it unless the client attaches
    /// to the domain's clock again.
    Ended(DomainClockAttachmentEnded),
    /// The session holding the attachment ended. The client attaches to the clock again on its
    /// next session, and the clock that attachment reports follows as an observation; changes in
    /// between are not reported.
    Interrupted(DomainClockInterruption),
    /// The current session refused to attach the interrupted clock again, or did not answer. The
    /// attachment stays interrupted, and the client tries again later.
    RestorationFailed(DomainClockRestorationFailure),
}

impl DomainClockEvent {
    /// The domain the event concerns.
    pub fn domain(&self) -> &DomainName {
        match self {
            Self::Observed(observed) => &observed.domain,
            Self::Ticked(ticked) => &ticked.domain,
            Self::Ended(ended) => &ended.domain,
            Self::Interrupted(interrupted) => &interrupted.domain,
            Self::RestorationFailed(failure) => &failure.domain,
        }
    }
}

/// A gap in a followed clock's delivery after the session holding its attachment ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainClockInterruption {
    pub domain: DomainName,
}

/// An attempt to attach an interrupted clock again that the current session refused or did not
/// answer. The client tries again after `retry_after` for as long as it follows the clock and the
/// session stays open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainClockRestorationFailure {
    pub domain: DomainName,
    /// The server's refusal, or why the request got no answer.
    pub message: String,
    /// How long the client waits before its next attempt.
    pub retry_after: Duration,
}

/// Why an attached clock cannot answer a question about logical time.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainClockReadError {
    #[error("domain '{domain}' clock generation {generation} is stopped")]
    Stopped { domain: DomainName, generation: u64 },
    #[error("domain '{domain}' clock generation {generation} is not installed on the serving node")]
    Uninstalled { domain: DomainName, generation: u64 },
    #[error("domain '{domain}' clock arithmetic leaves the supported range")]
    Arithmetic { domain: DomainName },
}

/// The latest clock the client observed for one attached domain, and the arithmetic a participant
/// in the domain's time runs against it.
///
/// It projects the committed mapping the way every node of the cluster does, so the answers hold
/// for the caller's own UTC. A host whose UTC is not disciplined like the cluster's projects a
/// different logical time by the same offset, multiplied by the rate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachedDomainClock {
    domain: DomainName,
    clock: DomainClockObservation,
    tick: Option<DomainClockTickObservation>,
}

impl AttachedDomainClock {
    pub fn domain(&self) -> &DomainName {
        &self.domain
    }

    /// The clock as the serving node last reported it installed.
    pub fn clock(&self) -> &DomainClockObservation {
        &self.clock
    }

    /// The latest accepted tick, if this node has one for the installed generation.
    pub fn latest_tick(&self) -> Option<&DomainClockTickObservation> {
        self.tick.as_ref()
    }

    /// The logical boundary of the latest accepted tick.
    pub fn frontier(&self) -> Option<Timestamp> {
        self.tick.as_ref().map(|tick| tick.logical_boundary)
    }

    /// The domain's logical time at the UTC instant `utc`. An unpaced domain reads UTC itself.
    pub fn logical_time_at(
        &self,
        utc: Timestamp,
    ) -> error_stack::Result<Timestamp, DomainClockReadError> {
        match self.readable()? {
            ReadableClock::Unpaced => Ok(utc),
            ReadableClock::Paced(paced) => paced.mapping.logical_time_at(utc).change_context(
                DomainClockReadError::Arithmetic {
                    domain: self.domain.clone(),
                },
            ),
        }
    }

    /// How long after the UTC instant `utc` the domain's logical time reaches `target`: zero once
    /// it has. The duration is rounded up, so waiting it never arrives early.
    pub fn wall_duration_until(
        &self,
        utc: Timestamp,
        target: Timestamp,
    ) -> error_stack::Result<Duration, DomainClockReadError> {
        match self.readable()? {
            ReadableClock::Unpaced => Ok(target.duration_since(utc).unwrap_or(Duration::ZERO)),
            ReadableClock::Paced(paced) => {
                let now = self.logical_time_at(utc)?;
                paced
                    .mapping
                    .wall_duration_until(now, target)
                    .change_context(DomainClockReadError::Arithmetic {
                        domain: self.domain.clone(),
                    })
            }
        }
    }

    /// The event timestamps a `TIMESTAMP AT` ingestor of the domain admits at the UTC instant
    /// `utc`, reconstructed the way the ingestor reconstructs them. `None` for an unpaced domain,
    /// whose ingestors admit every timestamp.
    pub fn admission_window(
        &self,
        utc: Timestamp,
    ) -> error_stack::Result<Option<DomainAdmissionWindow>, DomainClockReadError> {
        match self.readable()? {
            ReadableClock::Unpaced => Ok(None),
            ReadableClock::Paced(paced) => {
                let now = self.logical_time_at(utc)?;
                let window = DomainAdmissionWindow::reached(
                    paced.mapping.logical_start(),
                    now,
                    paced.period,
                    paced.skew,
                )
                .assured("a projection never precedes the logical origin of its mapping");
                Ok(Some(window))
            }
        }
    }

    /// The clock's time source, or why a stopped or uninstalled clock cannot be read.
    fn readable(&self) -> error_stack::Result<ReadableClock<'_>, DomainClockReadError> {
        let domain = self.domain.clone();
        let generation = self.clock.generation;
        match &self.clock.state {
            DomainClockObservedState::Unpaced => Ok(ReadableClock::Unpaced),
            DomainClockObservedState::Paced(paced) => Ok(ReadableClock::Paced(paced)),
            DomainClockObservedState::Stopped => Err(Report::new(DomainClockReadError::Stopped {
                domain,
                generation,
            })),
            DomainClockObservedState::Uninstalled => {
                Err(Report::new(DomainClockReadError::Uninstalled {
                    domain,
                    generation,
                }))
            }
        }
    }
}

/// The time source of a clock that can be read.
enum ReadableClock<'clock> {
    Unpaced,
    Paced(&'clock PacedDomainClock),
}

/// One domain clock the client follows.
struct FollowedClock {
    /// The clock the attachment last reported.
    clock: DomainClockObservation,
    tick: Option<DomainClockTickObservation>,
    /// The exchange the attachment is held on.
    generation: Arc<()>,
    /// The exchange holding the attachment ended, and the attachment waits to be restored.
    interrupted: bool,
}

/// What a caller has not read yet about one domain's clock, in the order it happened.
#[derive(Default)]
struct PendingClockEvents {
    /// The end of an attachment, before anything about a later attachment to the same clock.
    ended: Option<DomainClockAttachmentEndReason>,
    interrupted: bool,
    /// The newest failed attempt to attach the interrupted clock again, before the observation
    /// that a later attempt reports.
    restoration_failed: Option<DomainClockRestorationFailure>,
    /// The newest observation.
    observed: Option<DomainClockObservation>,
    /// The newest tick of the pending observation's generation, taken after that observation.
    tick: Option<DomainClockTickObservation>,
}

impl PendingClockEvents {
    /// Takes the earliest pending event about `domain`.
    fn take(&mut self, domain: &DomainName) -> Option<DomainClockEvent> {
        if let Some(reason) = self.ended.take() {
            return Some(DomainClockEvent::Ended(DomainClockAttachmentEnded {
                domain: domain.clone(),
                reason,
            }));
        }
        if self.interrupted {
            self.interrupted = false;
            return Some(DomainClockEvent::Interrupted(DomainClockInterruption {
                domain: domain.clone(),
            }));
        }
        if let Some(failure) = self.restoration_failed.take() {
            return Some(DomainClockEvent::RestorationFailed(failure));
        }
        if let Some(clock) = self.observed.take() {
            return Some(DomainClockEvent::Observed(DomainClockObserved {
                domain: domain.clone(),
                clock,
            }));
        }
        let tick = self.tick.take()?;
        Some(DomainClockEvent::Ticked(DomainClockTicked {
            domain: domain.clone(),
            tick,
        }))
    }

    fn is_empty(&self) -> bool {
        self.ended.is_none()
            && !self.interrupted
            && self.restoration_failed.is_none()
            && self.observed.is_none()
            && self.tick.is_none()
    }
}

#[derive(Default)]
struct State {
    followed: IndexMap<DomainName, FollowedClock>,
    /// Domains with events the caller has not read, in the order their first pending event
    /// happened.
    pending: IndexMap<DomainName, PendingClockEvents>,
}

impl State {
    fn pending(&mut self, domain: &DomainName) -> &mut PendingClockEvents {
        self.pending.entry(domain.clone()).or_default()
    }
}

struct Inner {
    state: Mutex<State>,
    changed: watch::Sender<()>,
}

/// Shared by the client and its exchange readers. An exchange generation fences the frames of an
/// ended exchange from the attachment restored on the next one.
#[derive(Clone)]
pub(crate) struct DomainClockAttachments {
    inner: Arc<Inner>,
}

impl DomainClockAttachments {
    pub(crate) fn new() -> Self {
        let (changed, _) = watch::channel(());
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                changed,
            }),
        }
    }

    pub(crate) fn watch(&self) -> watch::Receiver<()> {
        self.inner.changed.subscribe()
    }

    /// The latest clock of a followed domain.
    pub(crate) fn latest(&self, domain: &DomainName) -> Option<AttachedDomainClock> {
        let state = self.inner.state.lock();
        let followed = state.followed.get(domain)?;
        Some(AttachedDomainClock {
            domain: domain.clone(),
            clock: followed.clock.clone(),
            tick: followed.tick.clone(),
        })
    }

    /// Whether any followed clock waits to be restored on a new exchange.
    pub(crate) fn awaits_restoration(&self) -> bool {
        let state = self.inner.state.lock();
        state.followed.values().any(|followed| followed.interrupted)
    }

    /// Whether the client follows the clock of `domain` and waits to attach it again.
    pub(crate) fn awaits_restoration_of(&self, domain: &DomainName) -> bool {
        let state = self.inner.state.lock();
        match state.followed.get(domain) {
            Some(followed) => followed.interrupted,
            None => false,
        }
    }

    /// Every followed domain, in the order the client attached to its clock.
    pub(crate) fn followed_domains(&self) -> Vec<DomainName> {
        let state = self.inner.state.lock();
        state.followed.keys().cloned().collect()
    }

    /// Every followed domain whose clock waits to be attached again, in the order the client
    /// attached to it.
    pub(crate) fn interrupted_domains(&self) -> Vec<DomainName> {
        let state = self.inner.state.lock();
        let mut interrupted = Vec::new();
        for (domain, followed) in &state.followed {
            if followed.interrupted {
                interrupted.push(domain.clone());
            }
        }
        interrupted
    }

    /// Takes the earliest event a caller has not read.
    pub(crate) fn take_event(&self) -> Option<DomainClockEvent> {
        let mut state = self.inner.state.lock();
        let (domain, pending) = state.pending.get_index_mut(0)?;
        let domain = domain.clone();
        let event = pending.take(&domain);
        if pending.is_empty() {
            state
                .pending
                .shift_remove_index(0)
                .discarded("the entry removed is the one the event was just taken from");
        }
        event
    }

    /// Applies an attach reply the exchange of `generation` received.
    ///
    /// The reader calls this before the reply reaches its waiter and before it routes any later
    /// frame, and the server queues an attachment's frames only after its reply, so every frame of
    /// the attachment finds it here. A reply for a clock the client already follows moves the
    /// attachment to a new exchange. It reports the clock as an observation when the attachment
    /// was interrupted or the clock changed, because no caller waits for that reply. A reply that
    /// the session already follows the clock moves an interrupted attachment to that session too,
    /// whose later frames report the clock.
    pub(crate) fn apply_attach(&self, outcome: &DomainClockAttachOutcome, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        match &outcome.disposition {
            DomainClockAttachDisposition::Attached { domain, clock } => {
                let reported = match state.followed.get(domain) {
                    Some(followed) => followed.interrupted || followed.clock != *clock,
                    None => false,
                };
                state.followed.insert(
                    domain.clone(),
                    FollowedClock {
                        clock: clock.clone(),
                        tick: None,
                        generation: generation.clone(),
                        interrupted: false,
                    },
                );
                if !reported {
                    return;
                }
                let pending = state.pending(domain);
                pending.tick = None;
                pending.observed = Some(clock.clone());
            }
            DomainClockAttachDisposition::DomainNotFound(domain) => {
                // A followed clock is attached again only when the client moves to a new
                // exchange; its domain no longer exists on the new exchange's node.
                if state.followed.shift_remove(domain).is_none() {
                    return;
                }
                let pending = state.pending(domain);
                *pending = PendingClockEvents::default();
                pending.ended = Some(DomainClockAttachmentEndReason::DomainRemoved);
            }
            DomainClockAttachDisposition::AlreadyAttached(domain) => {
                let Some(followed) = state.followed.get_mut(domain) else {
                    return;
                };
                if !followed.interrupted {
                    return;
                }
                followed.generation = generation.clone();
                followed.interrupted = false;
            }
            DomainClockAttachDisposition::Failed => return,
        }
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Reports that an attempt to attach the clock of `domain` again failed, which the client
    /// repeats after `retry_after`. `false` once the clock no longer waits to be attached again:
    /// the client stopped following it, or another attach restored it.
    pub(crate) fn restoration_failed(
        &self,
        domain: &DomainName,
        message: String,
        retry_after: Duration,
    ) -> bool {
        let mut state = self.inner.state.lock();
        let awaiting = match state.followed.get(domain) {
            Some(followed) => followed.interrupted,
            None => false,
        };
        if !awaiting {
            return false;
        }
        state.pending(domain).restoration_failed = Some(DomainClockRestorationFailure {
            domain: domain.clone(),
            message,
            retry_after,
        });
        drop(state);
        self.inner.changed.send_replace(());
        true
    }

    /// Applies a detach reply: the client no longer follows the domain's clock.
    pub(crate) fn apply_detach(&self, outcome: &DomainClockDetachOutcome) {
        let domain = match &outcome.disposition {
            DomainClockDetachDisposition::Detached(domain)
            | DomainClockDetachDisposition::NotAttached(domain) => domain,
            DomainClockDetachDisposition::Failed => return,
        };
        let mut state = self.inner.state.lock();
        let removed = state.followed.shift_remove(domain);
        if removed.is_none() {
            return;
        }
        state.pending.shift_remove(domain);
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Applies a clock frame the exchange of `generation` received.
    pub(crate) fn apply_observed(&self, observed: DomainClockObserved, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let Some(followed) = state.followed.get_mut(&observed.domain) else {
            return;
        };
        if !Arc::ptr_eq(&followed.generation, generation) || followed.interrupted {
            return;
        }
        if followed.clock.generation != observed.clock.generation
            || !matches!(observed.clock.state, DomainClockObservedState::Paced(_))
        {
            followed.tick = None;
        }
        followed.clock = observed.clock.clone();
        let pending = state.pending(&observed.domain);
        pending.tick = None;
        pending.observed = Some(observed.clock);
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Applies newer progress from the exchange holding this attachment. A slow event reader
    /// keeps one tick per domain; the observation of its generation is read first.
    pub(crate) fn apply_ticked(&self, ticked: DomainClockTicked, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let Some(followed) = state.followed.get_mut(&ticked.domain) else {
            return;
        };
        if !Arc::ptr_eq(&followed.generation, generation)
            || followed.interrupted
            || followed.clock.generation != ticked.tick.generation
            || !matches!(followed.clock.state, DomainClockObservedState::Paced(_))
            || followed
                .tick
                .as_ref()
                .is_some_and(|latest| latest.tick_id >= ticked.tick.tick_id)
        {
            return;
        }
        followed.tick = Some(ticked.tick.clone());
        state.pending(&ticked.domain).tick = Some(ticked.tick);
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Applies the end of an attachment the exchange of `generation` received.
    pub(crate) fn apply_ended(&self, ended: DomainClockAttachmentEnded, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let held = match state.followed.get(&ended.domain) {
            Some(followed) => Arc::ptr_eq(&followed.generation, generation),
            None => false,
        };
        if !held {
            return;
        }
        state.followed.shift_remove(&ended.domain);
        let pending = state.pending(&ended.domain);
        *pending = PendingClockEvents::default();
        pending.ended = Some(ended.reason);
        drop(state);
        self.inner.changed.send_replace(());
    }

    /// Interrupts every attachment the exchange of `generation` held, when that exchange ends.
    ///
    /// Whenever any attachment still waits to be restored afterwards, the caller waiting for
    /// clock events is woken to reopen the session. That includes an attachment the ended exchange
    /// was restoring whose reply never arrived: it is still held by an earlier exchange, so this
    /// end interrupts nothing new, and only the wake tells the caller that the session it waited
    /// on is gone.
    pub(crate) fn exchange_ended(&self, generation: &Arc<()>) {
        let mut state = self.inner.state.lock();
        let mut interrupted = Vec::new();
        for (domain, followed) in &mut state.followed {
            if Arc::ptr_eq(&followed.generation, generation) && !followed.interrupted {
                followed.interrupted = true;
                followed.tick = None;
                interrupted.push(domain.clone());
            }
        }
        for domain in interrupted {
            let pending = state.pending(&domain);
            pending.observed = None;
            pending.restoration_failed = None;
            pending.tick = None;
            pending.interrupted = true;
        }
        // Bounded by the domains the client follows.
        let awaiting_restoration = state.followed.values().any(|followed| followed.interrupted);
        drop(state);
        if !awaiting_restoration {
            return;
        }
        self.inner.changed.send_replace(());
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use meticulous::ResultExt as _;
    use nervix_models::{DomainClockPeriod, DomainClockSkew, DomainClockState, DomainTimeRate};

    use super::*;

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain name is valid")
    }

    fn paced(generation: u64, rate: f64) -> DomainClockObservation {
        DomainClockObservation {
            generation,
            state: DomainClockObservedState::Paced(PacedDomainClock {
                period: DomainClockPeriod::from_nanos(
                    NonZeroU64::new(1_000).assured("a positive period"),
                ),
                skew: DomainClockSkew::from_nanos(100),
                mapping: DomainClockState::new(
                    Timestamp::from_unix_nanos(10_000),
                    Timestamp::from_unix_nanos(1_000_000),
                    DomainTimeRate::try_from(rate).assured("a positive finite rate"),
                ),
            }),
        }
    }

    fn state(generation: u64, state: DomainClockObservedState) -> DomainClockObservation {
        DomainClockObservation { generation, state }
    }

    fn attached(domain_name: &str, clock: DomainClockObservation) -> AttachedDomainClock {
        AttachedDomainClock {
            domain: domain(domain_name),
            clock,
            tick: None,
        }
    }

    #[test]
    fn a_paced_clock_projects_utc_waits_and_admits_the_way_the_ingestor_does() {
        let clock = attached("sim", paced(3, 2.0));
        assert_eq!(clock.domain(), &domain("sim"));
        assert_eq!(clock.clock().generation, 3);
        // Before the anchor the domain sits at its logical origin.
        assert_eq!(
            clock
                .logical_time_at(Timestamp::from_unix_nanos(0))
                .assured("the projection is in range"),
            Timestamp::from_unix_nanos(1_000_000)
        );
        // 500 ns after the anchor at rate 2 is 1000 logical nanoseconds after the origin.
        assert_eq!(
            clock
                .logical_time_at(Timestamp::from_unix_nanos(10_500))
                .assured("the projection is in range"),
            Timestamp::from_unix_nanos(1_001_000)
        );
        assert_eq!(
            clock
                .wall_duration_until(
                    Timestamp::from_unix_nanos(10_500),
                    Timestamp::from_unix_nanos(1_002_001),
                )
                .assured("the wait is in range"),
            Duration::from_nanos(501),
            "a wait of 1001 logical nanoseconds at rate 2 rounds up to 501"
        );
        assert_eq!(
            clock
                .wall_duration_until(
                    Timestamp::from_unix_nanos(10_500),
                    Timestamp::from_unix_nanos(1_000_000),
                )
                .assured("the wait is in range"),
            Duration::ZERO
        );
        let window = clock
            .admission_window(Timestamp::from_unix_nanos(10_750))
            .assured("the window is in range")
            .assured("a paced clock has an admission window");
        assert_eq!(
            window.earliest_center(),
            Timestamp::from_unix_nanos(1_000_000)
        );
        assert_eq!(
            window.latest_center(),
            Timestamp::from_unix_nanos(1_001_000)
        );
        assert!(window.contains(Timestamp::from_unix_nanos(1_001_100)));
        assert!(!window.contains(Timestamp::from_unix_nanos(1_001_101)));
    }

    #[test]
    fn an_unpaced_clock_reads_utc_and_admits_every_timestamp() {
        let clock = attached("live", state(1, DomainClockObservedState::Unpaced));
        let now = Timestamp::from_unix_nanos(42);
        assert_eq!(clock.logical_time_at(now).assured("UTC is read as is"), now);
        assert_eq!(
            clock
                .wall_duration_until(now, Timestamp::from_unix_nanos(50))
                .assured("the wait is in range"),
            Duration::from_nanos(8)
        );
        assert_eq!(
            clock
                .wall_duration_until(now, Timestamp::from_unix_nanos(40))
                .assured("the wait is in range"),
            Duration::ZERO
        );
        assert!(
            clock
                .admission_window(now)
                .assured("an unpaced clock is readable")
                .is_none()
        );
    }

    #[test]
    fn a_stopped_or_uninstalled_clock_cannot_be_read() {
        let now = Timestamp::from_unix_nanos(0);
        let stopped = attached("sim", state(2, DomainClockObservedState::Stopped));
        let expected = DomainClockReadError::Stopped {
            domain: domain("sim"),
            generation: 2,
        };
        let error = stopped.logical_time_at(now).expect_err("a stopped clock");
        assert_eq!(error.current_context(), &expected);
        let error = stopped
            .wall_duration_until(now, now)
            .expect_err("a stopped clock");
        assert_eq!(error.current_context(), &expected);
        let uninstalled = attached("sim", state(3, DomainClockObservedState::Uninstalled));
        let error = uninstalled
            .admission_window(now)
            .expect_err("an uninstalled clock");
        assert_eq!(
            error.current_context(),
            &DomainClockReadError::Uninstalled {
                domain: domain("sim"),
                generation: 3,
            }
        );
    }

    #[test]
    fn a_projection_past_the_timestamp_range_is_an_arithmetic_error() {
        let mut observation = paced(1, 1.0);
        if let DomainClockObservedState::Paced(clock) = &mut observation.state {
            clock.mapping = DomainClockState::new(
                Timestamp::from_unix_nanos(0),
                Timestamp::from_unix_nanos(i64::MAX),
                DomainTimeRate::ONE,
            );
        }
        let clock = attached("sim", observation);
        let error = clock
            .logical_time_at(Timestamp::from_unix_nanos(1))
            .expect_err("one nanosecond past the maximum origin");
        assert_eq!(
            error.current_context(),
            &DomainClockReadError::Arithmetic {
                domain: domain("sim"),
            }
        );
    }

    fn attach_reply(domain_name: &str, clock: DomainClockObservation) -> DomainClockAttachOutcome {
        DomainClockAttachOutcome {
            disposition: DomainClockAttachDisposition::Attached {
                domain: domain(domain_name),
                clock,
            },
            message: String::new(),
        }
    }

    fn observed(domain_name: &str, clock: DomainClockObservation) -> DomainClockObserved {
        DomainClockObserved {
            domain: domain(domain_name),
            clock,
        }
    }

    fn ticked(domain_name: &str, generation: u64, tick_id: u64) -> DomainClockTicked {
        DomainClockTicked {
            domain: domain(domain_name),
            tick: DomainClockTickObservation {
                generation,
                tick_id,
                logical_boundary: Timestamp::from_unix_nanos(
                    i64::try_from(tick_id).assured("fixture ids fit in timestamps"),
                ),
                authority_utc: Timestamp::from_unix_nanos(5_000),
                serving_logical: Timestamp::from_unix_nanos(6_000),
            },
        }
    }

    #[test]
    fn ticks_coalesce_per_domain_and_follow_their_generations_state() {
        let clocks = DomainClockAttachments::new();
        let exchange = Arc::new(());
        clocks.apply_attach(&attach_reply("sim", paced(1, 1.0)), &exchange);
        clocks.apply_ticked(ticked("sim", 1, 1), &exchange);
        clocks.apply_ticked(ticked("sim", 1, 3), &exchange);
        clocks.apply_ticked(ticked("sim", 1, 2), &exchange);
        assert_eq!(
            clocks.take_event(),
            Some(DomainClockEvent::Ticked(ticked("sim", 1, 3)))
        );
        let helper = clocks
            .latest(&domain("sim"))
            .assured("the clock is followed");
        assert_eq!(helper.latest_tick(), Some(&ticked("sim", 1, 3).tick));
        assert_eq!(helper.frontier(), Some(Timestamp::from_unix_nanos(3)));

        clocks.apply_ticked(ticked("sim", 1, 4), &exchange);
        clocks.apply_observed(observed("sim", paced(2, 1.0)), &exchange);
        clocks.apply_ticked(ticked("sim", 1, 5), &exchange);
        clocks.apply_ticked(ticked("sim", 2, 1), &exchange);
        assert_eq!(
            clocks.take_event(),
            Some(DomainClockEvent::Observed(observed("sim", paced(2, 1.0))))
        );
        assert_eq!(
            clocks.take_event(),
            Some(DomainClockEvent::Ticked(ticked("sim", 2, 1)))
        );
        assert!(clocks.take_event().is_none());
    }

    #[test]
    fn events_are_coalesced_per_domain_and_follow_the_attachment() {
        let clocks = DomainClockAttachments::new();
        let generation = Arc::new(());
        clocks.apply_observed(observed("sim", paced(1, 1.0)), &generation);
        assert!(
            clocks.take_event().is_none(),
            "a frame about a clock the client does not follow is dropped"
        );

        clocks.apply_attach(&attach_reply("sim", paced(1, 1.0)), &generation);
        assert!(
            clocks.take_event().is_none(),
            "the caller of an attachment reads its reply, not an event"
        );
        assert_eq!(clocks.followed_domains(), [domain("sim")]);
        clocks.apply_attach(
            &attach_reply("live", state(1, DomainClockObservedState::Unpaced)),
            &generation,
        );
        clocks.apply_observed(
            observed("sim", state(1, DomainClockObservedState::Stopped)),
            &generation,
        );
        clocks.apply_observed(
            observed("live", state(1, DomainClockObservedState::Stopped)),
            &generation,
        );
        clocks.apply_observed(observed("sim", paced(2, 3.0)), &generation);
        let other_generation = Arc::new(());
        clocks.apply_observed(observed("sim", paced(9, 9.0)), &other_generation);

        assert_eq!(
            clocks.take_event(),
            Some(DomainClockEvent::Observed(observed("sim", paced(2, 3.0)))),
            "the newest clock replaces one the caller has not read"
        );
        assert_eq!(
            clocks.take_event(),
            Some(DomainClockEvent::Observed(observed(
                "live",
                state(1, DomainClockObservedState::Stopped)
            )))
        );
        assert!(clocks.take_event().is_none());
        assert_eq!(
            clocks.latest(&domain("sim")),
            Some(attached("sim", paced(2, 3.0)))
        );

        clocks.apply_detach(&DomainClockDetachOutcome {
            disposition: DomainClockDetachDisposition::Detached(domain("live")),
            message: String::new(),
        });
        assert_eq!(clocks.latest(&domain("live")), None);
        clocks.apply_observed(
            observed("live", state(2, DomainClockObservedState::Unpaced)),
            &generation,
        );
        assert!(clocks.take_event().is_none(), "nothing follows a detach");

        clocks.apply_ticked(ticked("sim", 2, 1), &generation);
        clocks.apply_detach(&DomainClockDetachOutcome {
            disposition: DomainClockDetachDisposition::Detached(domain("sim")),
            message: String::new(),
        });
        assert!(
            clocks.take_event().is_none(),
            "an unread tick is withdrawn by detach"
        );
        clocks.apply_attach(&attach_reply("sim", paced(2, 3.0)), &generation);

        clocks.apply_observed(observed("sim", paced(3, 1.0)), &generation);
        clocks.apply_ended(
            DomainClockAttachmentEnded {
                domain: domain("sim"),
                reason: DomainClockAttachmentEndReason::DomainRemoved,
            },
            &generation,
        );
        assert_eq!(
            clocks.take_event(),
            Some(DomainClockEvent::Ended(DomainClockAttachmentEnded {
                domain: domain("sim"),
                reason: DomainClockAttachmentEndReason::DomainRemoved,
            })),
            "an end replaces the observations before it"
        );
        assert!(clocks.take_event().is_none());
        assert!(clocks.followed_domains().is_empty());
    }

    #[test]
    fn an_ended_exchange_interrupts_its_attachments_until_a_new_exchange_attaches_them() {
        let clocks = DomainClockAttachments::new();
        let first = Arc::new(());
        clocks.apply_attach(&attach_reply("sim", paced(1, 1.0)), &first);
        clocks.apply_attach(&attach_reply("gone", paced(1, 1.0)), &first);
        clocks.apply_attach(&attach_reply("same", paced(1, 1.0)), &first);
        clocks.apply_ticked(ticked("sim", 1, 1), &first);
        assert!(
            clocks
                .latest(&domain("sim"))
                .is_some_and(|clock| clock.frontier().is_some())
        );
        clocks.apply_observed(observed("sim", paced(2, 1.0)), &first);
        clocks.exchange_ended(&first);
        clocks.exchange_ended(&first);
        assert!(clocks.awaits_restoration());
        assert_eq!(
            clocks
                .latest(&domain("sim"))
                .and_then(|clock| clock.frontier()),
            None
        );

        let second = Arc::new(());
        clocks.apply_observed(observed("sim", paced(5, 1.0)), &second);
        clocks.apply_attach(&attach_reply("sim", paced(3, 1.0)), &second);
        clocks.apply_attach(
            &DomainClockAttachOutcome {
                disposition: DomainClockAttachDisposition::DomainNotFound(domain("gone")),
                message: String::new(),
            },
            &second,
        );
        clocks.apply_attach(&attach_reply("same", paced(1, 1.0)), &second);
        assert!(!clocks.awaits_restoration());

        let mut events = Vec::new();
        while let Some(event) = clocks.take_event() {
            events.push(event);
        }
        assert_eq!(
            events,
            [
                DomainClockEvent::Interrupted(DomainClockInterruption {
                    domain: domain("sim")
                }),
                DomainClockEvent::Observed(observed("sim", paced(3, 1.0))),
                DomainClockEvent::Ended(DomainClockAttachmentEnded {
                    domain: domain("gone"),
                    reason: DomainClockAttachmentEndReason::DomainRemoved,
                }),
                DomainClockEvent::Interrupted(DomainClockInterruption {
                    domain: domain("same")
                }),
                DomainClockEvent::Observed(observed("same", paced(1, 1.0))),
            ],
            "the interruption replaces the unread observation, and the restored attachment \
             reports its clock again"
        );
        assert_eq!(clocks.followed_domains(), [domain("sim"), domain("same")]);

        let third = Arc::new(());
        clocks.apply_attach(&attach_reply("same", paced(1, 1.0)), &third);
        assert!(
            clocks.take_event().is_none(),
            "an attachment moved while its clock did not change reports nothing"
        );
        clocks.apply_attach(&attach_reply("same", paced(2, 1.0)), &third);
        assert_eq!(
            clocks.take_event(),
            Some(DomainClockEvent::Observed(observed("same", paced(2, 1.0))))
        );
        clocks.apply_attach(
            &DomainClockAttachOutcome {
                disposition: DomainClockAttachDisposition::Failed,
                message: String::new(),
            },
            &third,
        );
        clocks.apply_detach(&DomainClockDetachOutcome {
            disposition: DomainClockDetachDisposition::Failed,
            message: String::new(),
        });
        assert_eq!(clocks.followed_domains(), [domain("sim"), domain("same")]);
        assert_eq!(
            DomainClockEvent::Interrupted(DomainClockInterruption {
                domain: domain("sim")
            })
            .domain(),
            &domain("sim")
        );
    }

    fn already_attached(domain_name: &str) -> DomainClockAttachOutcome {
        DomainClockAttachOutcome {
            disposition: DomainClockAttachDisposition::AlreadyAttached(domain(domain_name)),
            message: String::new(),
        }
    }

    #[test]
    fn a_session_that_already_follows_an_interrupted_clock_takes_over_its_attachment() {
        let clocks = DomainClockAttachments::new();
        let first = Arc::new(());
        clocks.apply_attach(&attach_reply("sim", paced(1, 1.0)), &first);
        clocks.apply_attach(&attach_reply("held", paced(1, 1.0)), &first);
        let second = Arc::new(());
        clocks.apply_attach(&already_attached("held"), &second);
        clocks.apply_observed(observed("held", paced(2, 1.0)), &second);
        assert!(
            clocks.take_event().is_none(),
            "an attachment still held by its exchange is not moved by another session's reply"
        );

        clocks.exchange_ended(&first);
        assert_eq!(
            clocks.interrupted_domains(),
            [domain("sim"), domain("held")]
        );
        clocks.apply_attach(&already_attached("sim"), &second);
        assert!(!clocks.awaits_restoration_of(&domain("sim")));
        assert!(clocks.awaits_restoration_of(&domain("held")));
        assert!(!clocks.awaits_restoration_of(&domain("unknown")));
        clocks.apply_observed(observed("sim", paced(2, 1.0)), &second);
        let mut events = Vec::new();
        while let Some(event) = clocks.take_event() {
            events.push(event);
        }
        assert_eq!(
            events,
            [
                DomainClockEvent::Interrupted(DomainClockInterruption {
                    domain: domain("sim")
                }),
                DomainClockEvent::Observed(observed("sim", paced(2, 1.0))),
                DomainClockEvent::Interrupted(DomainClockInterruption {
                    domain: domain("held")
                }),
            ],
            "frames of the session that already follows the clock reach the caller"
        );
        assert_eq!(
            clocks.latest(&domain("sim")),
            Some(attached("sim", paced(2, 1.0)))
        );
    }

    #[test]
    fn a_failed_restoration_is_reported_between_the_gap_and_the_restored_clock() {
        let clocks = DomainClockAttachments::new();
        let first = Arc::new(());
        clocks.apply_attach(&attach_reply("sim", paced(1, 1.0)), &first);
        assert!(
            !clocks.restoration_failed(&domain("sim"), "held".to_string(), Duration::ZERO),
            "an attachment its exchange still holds needs no restoration"
        );
        assert!(!clocks.restoration_failed(
            &domain("unknown"),
            "not followed".to_string(),
            Duration::ZERO
        ));
        clocks.exchange_ended(&first);
        let failure = |message: &str, seconds: u64| DomainClockRestorationFailure {
            domain: domain("sim"),
            message: message.to_string(),
            retry_after: Duration::from_secs(seconds),
        };
        assert!(clocks.restoration_failed(
            &domain("sim"),
            "refused".to_string(),
            Duration::from_secs(1)
        ));
        assert!(clocks.restoration_failed(
            &domain("sim"),
            "refused again".to_string(),
            Duration::from_secs(2)
        ));
        let second = Arc::new(());
        clocks.apply_attach(&attach_reply("sim", paced(2, 1.0)), &second);
        assert!(
            !clocks.restoration_failed(&domain("sim"), "late".to_string(), Duration::ZERO),
            "a restored attachment reports no later failure"
        );
        let mut events = Vec::new();
        while let Some(event) = clocks.take_event() {
            events.push(event);
        }
        assert_eq!(
            events,
            [
                DomainClockEvent::Interrupted(DomainClockInterruption {
                    domain: domain("sim")
                }),
                DomainClockEvent::RestorationFailed(failure("refused again", 2)),
                DomainClockEvent::Observed(observed("sim", paced(2, 1.0))),
            ],
            "the newest failure replaces an unread one and precedes the restored clock"
        );
        assert_eq!(
            DomainClockEvent::RestorationFailed(failure("refused", 1)).domain(),
            &domain("sim")
        );

        clocks.exchange_ended(&second);
        assert!(clocks.restoration_failed(
            &domain("sim"),
            "refused".to_string(),
            Duration::from_secs(1)
        ));
        let third = Arc::new(());
        clocks.exchange_ended(&third);
        clocks.apply_attach(
            &DomainClockAttachOutcome {
                disposition: DomainClockAttachDisposition::DomainNotFound(domain("sim")),
                message: String::new(),
            },
            &third,
        );
        let mut events = Vec::new();
        while let Some(event) = clocks.take_event() {
            events.push(event);
        }
        assert_eq!(
            events,
            [DomainClockEvent::Ended(DomainClockAttachmentEnded {
                domain: domain("sim"),
                reason: DomainClockAttachmentEndReason::DomainRemoved,
            })],
            "an end replaces the gap and the failure before it"
        );
    }

    #[test]
    fn an_exchange_that_ends_before_restoring_an_attachment_wakes_the_waiting_caller() {
        let clocks = DomainClockAttachments::new();
        let first = Arc::new(());
        clocks.apply_attach(&attach_reply("sim", paced(1, 1.0)), &first);
        let mut changed = clocks.watch();
        clocks.exchange_ended(&first);
        assert!(
            changed
                .has_changed()
                .assured("the registry holds its sender"),
            "an interrupted attachment wakes the caller"
        );
        drop(changed.borrow_and_update());
        assert_eq!(
            clocks.take_event(),
            Some(DomainClockEvent::Interrupted(DomainClockInterruption {
                domain: domain("sim")
            }))
        );

        // The second exchange ends before the reply that would restore the attachment arrives.
        let second = Arc::new(());
        clocks.exchange_ended(&second);
        assert!(
            changed
                .has_changed()
                .assured("the registry holds its sender"),
            "a caller waiting on the ended exchange reopens the session"
        );
        assert!(clocks.awaits_restoration());
        assert!(
            clocks.take_event().is_none(),
            "the interruption was reported once"
        );

        drop(changed.borrow_and_update());
        let third = Arc::new(());
        clocks.apply_attach(&attach_reply("sim", paced(1, 1.0)), &third);
        assert!(!clocks.awaits_restoration());
        assert!(
            clocks.take_event().is_some(),
            "the restored clock is reported"
        );
        let unrelated = Arc::new(());
        drop(changed.borrow_and_update());
        clocks.exchange_ended(&unrelated);
        assert!(
            !changed
                .has_changed()
                .assured("the registry holds its sender"),
            "an exchange that held nothing wakes no one while nothing awaits restoration"
        );
    }
}
