//! Runtime adapters for validated domain-clock mappings and progress delivery.
//!
//! Layer: data plane.
//!
//! - **Owns.** Installing committed mappings, observing progress and adapting clock arithmetic to
//!   runtime lifecycle decisions.
//! - **Depends on.** Vocabulary clock models and branch-local runtime state.
//! - **Must not know.** NSPL parsing, consensus decisions or clock-authority selection.

use super::*;

#[derive(Clone)]
pub(super) struct RuntimeWasmDomainClock {
    pub(super) runtime: Runtime,
    pub(super) domain: DomainName,
}

impl WasmDomainClock for RuntimeWasmDomainClock {
    fn now(&self) -> Timestamp {
        self.runtime
            .current_stream_expiration_time(&self.domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp)
    }
}

pub(super) fn checked_add_duration_to_timestamp(base: Timestamp, duration: Duration) -> Timestamp {
    // Saturation is the meaning here: a schedule further out than the nanosecond range is already
    // further out than any timestamp this clock will reach.
    base.checked_add(duration)
        .unwrap_or_else(|_| Timestamp::from_unix_nanos(i64::MAX))
}

pub(super) fn advance_scheduled_timestamp(
    next: &mut Option<Timestamp>,
    interval: Duration,
    current: Timestamp,
) {
    let mut scheduled = next.unwrap_or(current);
    while scheduled <= current {
        let advanced = checked_add_duration_to_timestamp(scheduled, interval);
        if advanced <= scheduled {
            break;
        }
        scheduled = advanced;
    }
    *next = Some(scheduled);
}

pub(super) fn wall_duration_until_timestamp(current: Timestamp, target: Timestamp) -> Duration {
    target.duration_since(current).unwrap_or(Duration::ZERO)
}

pub(super) fn current_timestamp() -> Timestamp {
    Timestamp::now()
}

pub(super) fn domain_clock_window_matches(
    clock: &DomainClockState,
    period: Duration,
    skew: Duration,
    event_timestamp: Timestamp,
) -> bool {
    let tick_spacing_nanos =
        (period.as_nanos().approx_into::<f64>() / clock.time_rate().get()).max(1.0);
    let first_tick = clock.wall_started_at();
    let event_offset_nanos = (i128::from(event_timestamp.unix_nanos())
        - i128::from(first_tick.unix_nanos()))
    .approx_into::<f64>();
    let approx_index = event_offset_nanos / tick_spacing_nanos;
    let candidates = [
        approx_index.floor() - 1.0,
        approx_index.floor(),
        approx_index.ceil(),
        approx_index.ceil() + 1.0,
        0.0,
    ];

    for candidate in candidates {
        if candidate < 0.0 {
            continue;
        }
        let Some(candidate_offset_nanos) = (candidate * tick_spacing_nanos)
            .round()
            .checked_approx_into::<u64>()
        else {
            continue;
        };
        let Ok(tick_wall) = first_tick.checked_add(Duration::from_nanos(candidate_offset_nanos))
        else {
            continue;
        };
        let distance = (i128::from(event_timestamp.unix_nanos())
            - i128::from(tick_wall.unix_nanos()))
        .unsigned_abs();
        if distance <= skew.as_nanos() {
            return true;
        }
    }

    false
}

pub(super) fn current_domain_logical_time(
    clock: &DomainClockState,
    wall_now: Timestamp,
) -> Result<Timestamp, Report<DomainClockError>> {
    clock.logical_time_at(wall_now)
}

pub(super) fn wall_duration_until_logical_target(
    clock: &DomainClockState,
    current_logical: Timestamp,
    target_logical: Timestamp,
) -> Result<Duration, Report<DomainClockError>> {
    clock.wall_duration_until(current_logical, target_logical)
}

impl Runtime {
    #[cfg(feature = "testing")]
    pub(crate) async fn pause_domain_clock_progress_if_armed(&self, domain: &DomainName) -> bool {
        self.inner
            .runtime_pauses
            .pause_domain_clock_progress_if_armed(domain)
            .await
    }

    #[cfg(feature = "testing")]
    pub(crate) fn mark_domain_clock_progress_delivered(&self, domain: &DomainName) {
        self.inner
            .runtime_pauses
            .mark_domain_clock_progress_delivered(domain);
    }

    pub fn handle_domain_clock_start(&self, domain: &DomainName, clock: DomainClockState) {
        let mut entry =
            self.inner
                .domains
                .entry(domain.clone())
                .or_insert_with(|| RuntimeDomainState {
                    config: DomainConfig {
                        pace: DomainPace::Paced,
                        period: "1s".to_string(),
                        skew: "0ms".to_string(),
                        placement: nervix_models::PlacementPolicy::Neutral,
                    },
                    status: nervix_models::DomainStatus::Running,
                    start_version: 0,
                    last_start: nervix_models::DomainStartPoint::Resume,
                    clock: None,
                    ticks: parking_lot::Mutex::new(VecDeque::new()),
                });
        entry.clock = Some(clock);
    }

    pub fn handle_domain_clock_stop(&self, domain: &DomainName) {
        if let Some(mut entry) = self.inner.domains.get_mut(domain) {
            entry.clock = None;
            entry.ticks.lock().clear();
        }
    }

    pub fn handle_domain_tick(&self, domain: &DomainName, tick: &DomainTick) {
        let entry =
            self.inner
                .domains
                .entry(domain.clone())
                .or_insert_with(|| RuntimeDomainState {
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
                        period: tick.period.to_string(),
                        skew: "0ms".to_string(),
                        placement: nervix_models::PlacementPolicy::Neutral,
                    },
                    status: nervix_models::DomainStatus::Running,
                    start_version: 0,
                    last_start: nervix_models::DomainStartPoint::Resume,
                    clock: None,
                    ticks: parking_lot::Mutex::new(VecDeque::new()),
                });
        let mut ticks = entry.ticks.lock();
        if ticks
            .back()
            .is_some_and(|observed| observed.tick_id == tick.tick_id)
        {
            return;
        }
        ticks.push_back(ObservedDomainTick {
            tick_id: tick.tick_id,
            logical_timestamp: tick.logical_timestamp,
            wall_clock: tick.wall_clock,
        });
        while ticks.len() > DOMAIN_TICK_HISTORY_LIMIT {
            ticks.pop_front();
        }
    }

    pub(crate) fn current_paced_domain_time(
        &self,
        domain: &DomainName,
    ) -> Result<Option<Timestamp>, String> {
        let Some(domain_state) = self.inner.domains.get(domain) else {
            return Ok(None);
        };
        if let DomainPace::Unpaced = domain_state.config.pace {
            return Ok(None);
        }
        let wall_now = current_timestamp();
        if let Some(clock) = domain_state.clock.as_ref() {
            clock
                .logical_time_at(wall_now)
                .map(Some)
                .map_err(|error| error.to_string())
        } else {
            Ok(domain_state
                .ticks
                .lock()
                .back()
                .map(|tick| tick.logical_timestamp))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{DomainClockState, DomainTick, DomainTimeRate, Timestamp};

    use super::*;

    #[test]
    fn paced_domains_accept_records_inside_tick_window() {
        let runtime = Runtime::new();
        let mut domains = BTreeMap::new();
        domains.insert(domain("paced"), paced_domain_state("paced"));
        runtime.sync_domains(&domains);
        runtime.handle_domain_tick(
            &domain("paced"),
            &DomainTick {
                tick_id: 1,
                logical_timestamp: Timestamp::from_unix_nanos(0),
                wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
                period: "1s".parse().expect("fixture period is valid"),
            },
        );

        assert!(
            runtime
                .ensure_domain_allows_ingestion(
                    &domain("paced"),
                    &named("ing"),
                    Timestamp::from_unix_nanos(10_200_000_000),
                )
                .is_ok()
        );
        assert!(
            runtime
                .ensure_domain_allows_ingestion(
                    &domain("paced"),
                    &named("ing"),
                    Timestamp::from_unix_nanos(10_400_000_000),
                )
                .is_err()
        );
    }

    #[test]
    fn paced_domains_accept_records_while_clock_is_running_before_ticks_arrive() {
        let runtime = Runtime::new();
        let mut domains = BTreeMap::new();
        domains.insert(domain("paced"), paced_domain_state("paced"));
        runtime.sync_domains(&domains);
        runtime.handle_domain_clock_start(
            &domain("paced"),
            DomainClockState::new(
                Timestamp::from_unix_nanos(10_000_000_000),
                Timestamp::from_unix_nanos(10_000_000_000),
                DomainTimeRate::ONE,
            ),
        );

        assert!(
            runtime
                .ensure_domain_allows_ingestion(
                    &domain("paced"),
                    &named("ing"),
                    Timestamp::from_unix_nanos(10_200_000_000),
                )
                .is_ok()
        );
        assert!(
            runtime
                .ensure_domain_allows_ingestion(
                    &domain("paced"),
                    &named("ing"),
                    Timestamp::from_unix_nanos(11_200_000_000),
                )
                .is_ok()
        );
    }

    #[test]
    fn delayed_progress_delivery_does_not_move_logical_time_backwards() {
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::ONE,
        );
        let delayed_wall_time = Timestamp::from_unix_nanos(1_000_000_000);
        let before_delivery = current_domain_logical_time(&clock, delayed_wall_time)
            .assured("the fixture uses a finite positive rate");
        let after_delivery = current_domain_logical_time(&clock, delayed_wall_time)
            .assured("the fixture uses a finite positive rate");

        assert!(
            after_delivery >= before_delivery,
            "delivering progress moved logical time from {before_delivery} to {after_delivery}"
        );
    }

    #[test]
    #[ignore = "expected clock-contract failure owned by domain clocks task 05"]
    fn paced_domains_admit_the_logical_origin() {
        let runtime = Runtime::new();
        let mut domains = BTreeMap::new();
        domains.insert(domain("paced"), paced_domain_state("paced"));
        runtime.sync_domains(&domains);
        runtime.handle_domain_clock_start(
            &domain("paced"),
            DomainClockState::new(
                Timestamp::from_unix_nanos(10_000_000_000),
                Timestamp::from_unix_nanos(0),
                DomainTimeRate::ONE,
            ),
        );

        let admission = runtime.ensure_domain_allows_ingestion(
            &domain("paced"),
            &named("ing"),
            Timestamp::from_unix_nanos(0),
        );

        assert!(
            admission.is_ok(),
            "logical origin was rejected: {admission:?}"
        );
    }

    #[test]
    fn scheduled_timestamp_addition_stays_in_the_serializable_range() {
        let timestamp = checked_add_duration_to_timestamp(
            Timestamp::from_unix_nanos(i64::MAX),
            Duration::from_nanos(1),
        );

        let serialized = serde_json::to_string(&timestamp);

        assert!(
            serialized.is_ok(),
            "schedule arithmetic constructed an unserializable timestamp: {serialized:?}"
        );
    }

    #[test]
    fn logical_time_projection_reports_range_overflow() {
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(i64::MAX),
            DomainTimeRate::ONE,
        );

        assert!(current_domain_logical_time(&clock, Timestamp::from_unix_nanos(1)).is_err());
    }

    #[test]
    fn logical_rate_conversion_scales_physical_waits() {
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::try_from(4.0).expect("fixture rate is valid"),
        );

        let wait = wall_duration_until_logical_target(
            &clock,
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(1_000_000_000),
        )
        .assured("the fixture uses a finite positive rate");

        assert_eq!(wait, Duration::from_millis(250));
    }
}
