//! The selected domain's attached clock in the browser console.
//!
//! Layer: edges.
//! - **Owns.** The selected clock's presentation, browser projection cadence, and the one-attach
//!   intent for each domain selection on a connection.
//! - **Depends on.** Typed domain-clock observations and the session's attach outcome.
//! - **Must not know.** Relay subscriptions, graph execution, or how the WebSocket transports a
//!   request.

use std::time::Duration;

use leptos::prelude::*;
use nervix_client_wire::{DomainClockAttachDisposition, DomainClockAttachOutcome};
use nervix_models::{
    DomainClockObservation, DomainClockObservedState, DomainClockTickObservation, DomainName,
    Timestamp,
};

pub(super) const CLOCK_REFRESH: Duration = Duration::from_millis(250);

/// The browser's actual UTC is an external observation used only to display the attached paced
/// mapping. It never enters the server's domain-time read watermark or data plane.
pub(super) fn browser_utc_now() -> Timestamp {
    Timestamp::now()
}

/// One attempt per selected domain on a connection. A refused attempt remains recorded until the
/// selection changes or a new connection opens, so it cannot turn into a retry loop.
#[derive(Clone, Default)]
pub(super) struct ClockSelection {
    connection: Option<u64>,
    selected: Option<DomainName>,
}

pub(super) struct ClockSelectionChange {
    pub detach: Option<DomainName>,
    pub attach: Option<DomainName>,
    pub refresh_display: bool,
}

impl ClockSelection {
    pub fn change(
        &mut self,
        connected: bool,
        connection: u64,
        selected: Option<DomainName>,
    ) -> ClockSelectionChange {
        if !connected {
            self.connection = None;
            self.selected = None;
            return ClockSelectionChange {
                detach: None,
                attach: None,
                refresh_display: true,
            };
        }
        if self.connection != Some(connection) {
            self.connection = Some(connection);
            self.selected = None;
        }
        if self.selected == selected {
            return ClockSelectionChange {
                detach: None,
                attach: None,
                refresh_display: false,
            };
        }
        let detach = self.selected.take();
        self.selected = selected.clone();
        ClockSelectionChange {
            detach,
            attach: selected,
            refresh_display: true,
        }
    }
}

/// The clock the selected domain presents. An interrupted or refused attachment has no projected
/// time, even if an earlier session or selection observed a paced mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ClockDisplay {
    NoDomain,
    Attaching(DomainName),
    Interrupted(DomainName),
    Following {
        domain: DomainName,
        clock: DomainClockObservation,
        tick: Option<DomainClockTickObservation>,
    },
    Refused {
        domain: DomainName,
        reason: String,
    },
    Detached(DomainName),
    Ended {
        domain: DomainName,
        reason: String,
    },
}

impl ClockDisplay {
    pub fn selected(domain: Option<DomainName>, connected: bool) -> Self {
        match (domain, connected) {
            (Some(domain), true) => Self::Attaching(domain),
            (Some(domain), false) => Self::Interrupted(domain),
            (None, _) => Self::NoDomain,
        }
    }

    fn domain(&self) -> Option<&DomainName> {
        match self {
            Self::NoDomain => None,
            Self::Attaching(domain)
            | Self::Interrupted(domain)
            | Self::Detached(domain)
            | Self::Following { domain, .. }
            | Self::Refused { domain, .. }
            | Self::Ended { domain, .. } => Some(domain),
        }
    }

    pub fn attach_outcome(
        &mut self,
        requested: &DomainName,
        outcome: &DomainClockAttachOutcome,
        automatic: bool,
    ) {
        if self.domain() != Some(requested) {
            return;
        }
        match &outcome.disposition {
            DomainClockAttachDisposition::Attached { domain, clock } if domain == requested => {
                *self = Self::Following {
                    domain: domain.clone(),
                    clock: clock.clone(),
                    tick: None,
                };
            }
            DomainClockAttachDisposition::Attached { .. } => {
                self.refuse(requested, "the server attached another domain".to_string());
            }
            DomainClockAttachDisposition::AlreadyAttached(_)
            | DomainClockAttachDisposition::DomainNotFound(_)
            | DomainClockAttachDisposition::Failed
                if automatic =>
            {
                self.refuse(requested, outcome.message.clone());
            }
            DomainClockAttachDisposition::AlreadyAttached(_)
            | DomainClockAttachDisposition::DomainNotFound(_)
            | DomainClockAttachDisposition::Failed => {}
        }
    }

    pub fn refuse(&mut self, requested: &DomainName, reason: String) {
        if self.domain() == Some(requested) {
            *self = Self::Refused {
                domain: requested.clone(),
                reason,
            };
        }
    }

    pub fn observed(&mut self, domain: &DomainName, observed: DomainClockObservation) {
        let Self::Following {
            domain: current,
            clock,
            tick,
        } = self
        else {
            return;
        };
        if current != domain {
            return;
        }
        if clock.generation != observed.generation
            || !matches!(observed.state, DomainClockObservedState::Paced(_))
        {
            *tick = None;
        }
        *clock = observed;
    }

    pub fn ticked(&mut self, domain: &DomainName, observed: DomainClockTickObservation) {
        let Self::Following {
            domain: current,
            clock,
            tick,
        } = self
        else {
            return;
        };
        if current != domain
            || clock.generation != observed.generation
            || !matches!(clock.state, DomainClockObservedState::Paced(_))
        {
            return;
        }
        if let Some(previous) = tick
            && previous.tick_id >= observed.tick_id
        {
            return;
        }
        *tick = Some(observed);
    }

    pub fn detached(&mut self, domain: &DomainName) {
        if self.domain() == Some(domain) {
            *self = Self::Detached(domain.clone());
        }
    }

    pub fn ended(&mut self, domain: &DomainName, reason: String) {
        if self.domain() == Some(domain) {
            *self = Self::Ended {
                domain: domain.clone(),
                reason,
            };
        }
    }

    fn presentation(&self, now: Timestamp) -> ClockPresentation {
        let domain = match self.domain() {
            Some(domain) => domain.to_string(),
            None => "no domain".to_string(),
        };
        let mut shown = ClockPresentation {
            domain,
            state: "NO DOMAIN".to_string(),
            generation: "generation —".to_string(),
            logical: "—".to_string(),
            rate: "rate —".to_string(),
            period: "period —".to_string(),
            skew: "skew —".to_string(),
            tick_id: "—".to_string(),
            boundary: "—".to_string(),
            detail: String::new(),
        };
        match self {
            Self::NoDomain => {}
            Self::Attaching(_) => shown.state = "ATTACHING".to_string(),
            Self::Interrupted(_) => shown.state = "INTERRUPTED".to_string(),
            Self::Detached(_) => shown.state = "DETACHED".to_string(),
            Self::Refused { reason, .. } => {
                shown.state = "REFUSED".to_string();
                shown.detail = reason.clone();
            }
            Self::Ended { reason, .. } => {
                shown.state = "ENDED".to_string();
                shown.detail = reason.clone();
            }
            Self::Following { clock, tick, .. } => {
                shown.generation = format!("generation {}", clock.generation);
                match &clock.state {
                    DomainClockObservedState::Stopped => shown.state = "STOPPED".to_string(),
                    DomainClockObservedState::Uninstalled => {
                        shown.state = "UNINSTALLED".to_string();
                    }
                    DomainClockObservedState::Unpaced => shown.state = "UNPACED".to_string(),
                    DomainClockObservedState::Paced(paced) => {
                        shown.state = "PACED".to_string();
                        shown.logical = match paced.mapping.logical_time_at(now) {
                            Ok(logical) => logical.to_rfc3339(),
                            Err(_) => "outside supported range".to_string(),
                        };
                        shown.rate = format!("rate {}", paced.mapping.time_rate());
                        shown.period = format!("period {}", paced.period);
                        shown.skew = format!("skew {}", paced.skew);
                        shown.detail = format!(
                            "logical origin {}; UTC anchor {}",
                            paced.mapping.logical_start().to_rfc3339(),
                            paced.mapping.wall_started_at().to_rfc3339(),
                        );
                    }
                }
                if let Some(tick) = tick {
                    shown.tick_id = tick.tick_id.to_string();
                    shown.boundary = tick.logical_boundary.to_rfc3339();
                }
            }
        }
        shown
    }
}

#[derive(Clone, PartialEq, Eq)]
struct ClockPresentation {
    domain: String,
    state: String,
    generation: String,
    logical: String,
    rate: String,
    period: String,
    skew: String,
    tick_id: String,
    boundary: String,
    detail: String,
}

#[component]
pub(super) fn ClockPanel(
    display: RwSignal<ClockDisplay>,
    now: RwSignal<Timestamp>,
) -> impl IntoView {
    let shown = Memo::new(move |_| display.get().presentation(now.get()));
    view! {
        <section class="domain-clock" aria-label="Selected domain clock">
            <div class="domain-clock-heading">
                <span>"DOMAIN CLOCK"</span>
                <strong>{move || shown.get().domain}</strong>
            </div>
            <div class="domain-clock-summary">
                <strong class="domain-clock-state">{move || shown.get().state}</strong>
                <span class="domain-clock-generation">{move || shown.get().generation}</span>
            </div>
            <div class="domain-clock-details" title=move || shown.get().detail>
                <span>"logical now"</span>
                <strong class="domain-clock-logical">{move || shown.get().logical}</strong>
                <span>"pace"</span>
                <span class="domain-clock-pace">{move || shown.get().rate} " · " {move || shown.get().period} " · " {move || shown.get().skew}</span>
                <span>"last tick"</span>
                <span><strong class="domain-clock-tick-id">{move || shown.get().tick_id}</strong> " · " <span class="domain-clock-tick-boundary">{move || shown.get().boundary}</span></span>
            </div>
            <Show when=move || matches!(display.get(), ClockDisplay::Refused { .. } | ClockDisplay::Ended { .. }) fallback=|| ()>
                <p class="domain-clock-error">{move || shown.get().detail}</p>
            </Show>
        </section>
    }
}

#[component]
pub(super) fn ClockStatus(display: RwSignal<ClockDisplay>) -> impl IntoView {
    view! {
        <span class="topbar-clock" aria-label="Selected domain clock state">
            "CLOCK "
            <strong>{move || display.get().presentation(Timestamp::now()).state}</strong>
        </span>
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use leptos::prelude::Owner;
    use meticulous::ResultExt as _;
    use nervix_client_wire::DomainClockAttachOutcome;
    use nervix_models::{
        DomainClockPeriod, DomainClockSkew, DomainClockState, DomainTimeRate, PacedDomainClock,
    };

    use super::*;

    fn domain() -> DomainName {
        DomainName::parse("simulation").assured("the fixture domain is valid")
    }

    fn paced(generation: u64) -> DomainClockObservation {
        let origin = "2030-01-01T00:00:00Z"
            .parse()
            .assured("the origin is valid");
        DomainClockObservation {
            generation,
            state: DomainClockObservedState::Paced(PacedDomainClock {
                period: DomainClockPeriod::try_from(Duration::from_millis(100))
                    .assured("the period is valid"),
                skew: DomainClockSkew::try_from(Duration::from_millis(10))
                    .assured("the skew is valid"),
                mapping: DomainClockState::new(
                    Timestamp::from_unix_nanos(1_000_000_000),
                    origin,
                    DomainTimeRate::try_from(2.0).assured("the rate is valid"),
                ),
            }),
        }
    }

    #[test]
    fn selection_attaches_once_and_detaches_before_switching_or_reconnecting() {
        let domain = domain();
        let other = DomainName::parse("other").assured("the other fixture domain is valid");
        let mut selection = ClockSelection::default();
        let first = selection.change(true, 1, Some(domain.clone()));
        assert!(first.refresh_display);
        assert_eq!(first.detach, None);
        assert_eq!(first.attach, Some(domain.clone()));
        let repeated = selection.change(true, 1, Some(domain.clone()));
        assert!(!repeated.refresh_display);
        assert_eq!(repeated.attach, None);
        let switched = selection.change(true, 1, Some(other.clone()));
        assert_eq!(switched.detach, Some(domain.clone()));
        assert_eq!(switched.attach, Some(other.clone()));
        let lost = selection.change(false, 1, Some(other.clone()));
        assert_eq!(lost.detach, None);
        assert_eq!(lost.attach, None);
        let restored = selection.change(true, 2, Some(other.clone()));
        assert_eq!(restored.detach, None);
        assert_eq!(restored.attach, Some(other));
    }

    #[test]
    fn attached_clock_projects_and_advances_only_with_matching_ticks() {
        let domain = domain();
        let mapping = paced(1);
        let mut display = ClockDisplay::selected(Some(domain.clone()), true);
        display.attach_outcome(
            &domain,
            &DomainClockAttachOutcome {
                disposition: DomainClockAttachDisposition::Attached {
                    domain: domain.clone(),
                    clock: mapping.clone(),
                },
                message: "attached".to_string(),
            },
            true,
        );
        let anchor = Timestamp::from_unix_nanos(1_000_000_000);
        let later = Timestamp::from_unix_nanos(1_500_000_000);
        assert!(display.presentation(later).logical > display.presentation(anchor).logical);
        assert_eq!(display.presentation(later).state, "PACED");
        assert_eq!(display.presentation(later).rate, "rate 2");
        assert_eq!(display.presentation(later).period, "period 100ms");
        assert_eq!(display.presentation(later).skew, "skew 10ms");
        let tick = DomainClockTickObservation {
            generation: 1,
            tick_id: 3,
            logical_boundary: Timestamp::from_unix_nanos(2_000_000_000),
            authority_utc: anchor,
            serving_logical: later,
        };
        display.ticked(&domain, tick.clone());
        assert_eq!(display.presentation(later).tick_id, "3");
        display.ticked(&domain, DomainClockTickObservation { tick_id: 2, ..tick });
        assert_eq!(display.presentation(later).tick_id, "3");
        display.observed(
            &domain,
            DomainClockObservation {
                generation: 1,
                state: DomainClockObservedState::Stopped,
            },
        );
        assert_eq!(display.presentation(later).state, "STOPPED");
        assert_eq!(display.presentation(later).logical, "—");
        assert_eq!(display.presentation(later).tick_id, "—");
    }

    #[test]
    fn refused_and_unpaced_clocks_have_no_projection() {
        let domain = domain();
        let mut display = ClockDisplay::selected(Some(domain.clone()), true);
        display.attach_outcome(
            &domain,
            &DomainClockAttachOutcome {
                disposition: DomainClockAttachDisposition::Failed,
                message: "transaction is active".to_string(),
            },
            true,
        );
        assert_eq!(display.presentation(Timestamp::now()).state, "REFUSED");
        assert_eq!(display.presentation(Timestamp::now()).logical, "—");
        display.attach_outcome(
            &domain,
            &DomainClockAttachOutcome {
                disposition: DomainClockAttachDisposition::Attached {
                    domain: domain.clone(),
                    clock: DomainClockObservation {
                        generation: 2,
                        state: DomainClockObservedState::Unpaced,
                    },
                },
                message: "attached".to_string(),
            },
            false,
        );
        assert_eq!(display.presentation(Timestamp::now()).state, "UNPACED");
        assert_eq!(display.presentation(Timestamp::now()).logical, "—");
        display.detached(&domain);
        assert_eq!(display.presentation(Timestamp::now()).state, "DETACHED");
    }

    #[test]
    fn panel_renders_the_attached_clock_and_topbar_state() {
        super::super::initialize_test_executor();
        Owner::new().with(|| {
            let domain = domain();
            let display = RwSignal::new(ClockDisplay::Following {
                domain,
                clock: paced(3),
                tick: Some(DomainClockTickObservation {
                    generation: 3,
                    tick_id: 7,
                    logical_boundary: Timestamp::from_unix_nanos(2_000_000_000),
                    authority_utc: Timestamp::from_unix_nanos(1_500_000_000),
                    serving_logical: Timestamp::from_unix_nanos(2_100_000_000),
                }),
            });
            let now = RwSignal::new(Timestamp::from_unix_nanos(1_500_000_000));
            let markup = view! {
                <ClockPanel display=display now=now />
                <ClockStatus display=display />
            }
            .to_html();
            assert!(markup.contains("simulation"));
            assert!(markup.contains("PACED"));
            assert!(markup.contains("generation 3"));
            assert!(markup.contains("2030-01-01T00:00:01Z"));
            assert!(markup.contains("rate 2"));
            assert!(markup.contains("period 100ms"));
            assert!(markup.contains("skew 10ms"));
            assert!(markup.contains("domain-clock-tick-id"));
            assert!(markup.contains(">7</strong>"));
        });
    }
}
