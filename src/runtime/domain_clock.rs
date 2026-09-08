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
    let nanos = i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX);
    match base
        .into_datetime()
        .checked_add_signed(TimeDelta::nanoseconds(nanos))
    {
        Some(advanced) => Timestamp::from(advanced),
        None => base,
    }
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
    if target <= current {
        return Duration::ZERO;
    }
    target
        .into_datetime()
        .signed_duration_since(current.into_datetime())
        .to_std()
        .unwrap_or(Duration::ZERO)
}

pub(super) fn current_timestamp() -> Timestamp {
    Timestamp::now()
}

pub(super) fn domain_clock_window_matches(
    clock: &RuntimeDomainClockState,
    period: Duration,
    skew: Duration,
    event_timestamp: Timestamp,
) -> Result<bool, String> {
    let time_rate = clock.time_rate.parse::<f64>().map_err(|error| {
        format!(
            "invalid time rate '{}' for paced domain clock: {error}",
            clock.time_rate
        )
    })?;
    if !time_rate.is_finite() || time_rate <= 0.0 {
        return Err(format!(
            "invalid time rate '{}' for paced domain clock",
            clock.time_rate
        ));
    }

    let tick_spacing_nanos = (period.as_nanos().approx_into::<f64>() / time_rate).max(1.0);
    let first_tick = clock.wall_started_at;
    let event_offset_nanos = event_timestamp
        .into_datetime()
        .signed_duration_since(first_tick.into_datetime())
        .num_nanoseconds()
        .unwrap_or(if event_timestamp >= first_tick {
            i64::MAX
        } else {
            i64::MIN
        })
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
            .checked_approx_into()
        else {
            continue;
        };
        let tick_wall = first_tick
            .into_datetime()
            .checked_add_signed(TimeDelta::nanoseconds(candidate_offset_nanos))
            .map(Timestamp::from);
        let Some(tick_wall) = tick_wall else {
            continue;
        };
        if event_timestamp
            .into_datetime()
            .signed_duration_since(tick_wall.into_datetime())
            .abs()
            .to_std()
            .is_ok_and(|distance| distance <= skew)
        {
            return Ok(true);
        }
    }

    Ok(false)
}

pub(super) fn current_domain_logical_time(
    clock: &RuntimeDomainClockState,
    latest_tick: Option<&ObservedDomainTick>,
    wall_now: Timestamp,
) -> Result<Timestamp, String> {
    let time_rate = clock.time_rate.parse::<f64>().map_err(|error| {
        format!(
            "invalid time rate '{}' for paced domain clock: {error}",
            clock.time_rate
        )
    })?;
    if !time_rate.is_finite() || time_rate <= 0.0 {
        return Err(format!(
            "invalid time rate '{}' for paced domain clock",
            clock.time_rate
        ));
    }

    let (anchor_logical, anchor_wall) = if let Some(tick) = latest_tick {
        (tick.logical_timestamp, tick.wall_clock)
    } else {
        (clock.logical_started_at, clock.wall_started_at)
    };
    let wall_elapsed = wall_now
        .into_datetime()
        .signed_duration_since(anchor_wall.into_datetime());
    let wall_elapsed_nanos =
        wall_elapsed
            .num_nanoseconds()
            .unwrap_or(if wall_elapsed < TimeDelta::zero() {
                i64::MIN
            } else {
                i64::MAX
            });
    // The rate is finite and positive, so the scaled span can only leave the nanosecond range at
    // the far end, where a clock that has run past it pins to the end of the range.
    let logical_elapsed_nanos = (wall_elapsed_nanos.max(0).approx_into::<f64>() * time_rate)
        .round()
        .checked_approx_into()
        .unwrap_or(i64::MAX);
    let advanced = anchor_logical
        .into_datetime()
        .checked_add_signed(TimeDelta::nanoseconds(logical_elapsed_nanos));
    match advanced {
        Some(advanced) => Ok(Timestamp::from(advanced)),
        None => Ok(anchor_logical),
    }
}

pub(super) fn wall_duration_until_logical_target(
    clock: &RuntimeDomainClockState,
    current_logical: Timestamp,
    target_logical: Timestamp,
) -> Result<Duration, String> {
    let time_rate = clock.time_rate.parse::<f64>().map_err(|error| {
        format!(
            "invalid time rate '{}' for paced domain clock: {error}",
            clock.time_rate
        )
    })?;
    if !time_rate.is_finite() || time_rate <= 0.0 {
        return Err(format!(
            "invalid time rate '{}' for paced domain clock",
            clock.time_rate
        ));
    }
    if target_logical <= current_logical {
        return Ok(Duration::ZERO);
    }
    let logical_delta = target_logical
        .into_datetime()
        .signed_duration_since(current_logical.into_datetime())
        .to_std()
        .unwrap_or(Duration::ZERO);
    // As above, only the far end of the range is reachable, and a wall wait that long is capped
    // by the caller's own polling cadence anyway.
    let wall_delta_nanos = (logical_delta.as_nanos().approx_into::<f64>() / time_rate)
        .round()
        .checked_approx_into()
        .unwrap_or(u64::MAX);
    Ok(Duration::from_nanos(wall_delta_nanos.max(1)))
}

impl Runtime {
    pub fn handle_domain_clock_start(
        &self,
        domain: &DomainName,
        logical_started_at: Timestamp,
        wall_started_at: Timestamp,
        time_rate: &str,
    ) {
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
        entry.clock = Some(RuntimeDomainClockState {
            logical_started_at,
            wall_started_at,
            time_rate: time_rate.to_string(),
        });
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
                        period: tick.duration_ms.to_string(),
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
        let latest_tick = domain_state.ticks.lock().back().cloned();
        if let Some(clock) = domain_state.clock.as_ref() {
            current_domain_logical_time(clock, latest_tick.as_ref(), wall_now).map(Some)
        } else {
            Ok(latest_tick.map(|tick| tick.logical_timestamp))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::{DomainTick, Timestamp};

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
                duration_ms: 1_000,
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
            Timestamp::from_unix_nanos(10_000_000_000),
            Timestamp::from_unix_nanos(10_000_000_000),
            "1.0",
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
}
