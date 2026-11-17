//! Public timing fixtures for ingestor rate limits.
//!
//! Layer: test harness.
//! - **Owns.** The instant a rate-limit scenario records before its ingestor can admit a payload,
//!   the arrival of each payload the scenario then receives, and the lower bounds an allowance
//!   puts on those arrivals.
//! - **Depends on.** The scenario's active subscription and its docstring fragment sets.
//! - **Must not know.** The limiter, its clock, or how a host holds its source back.
//!
//! An allowance starts full no earlier than the recorded instant and refills at a fixed rate, so
//! the n-th admitted payload can be admitted no sooner than the allowance's bound after that
//! instant, and is received later still. Load on the host only delays a receipt, so a lower bound
//! measured from that instant holds on a busy machine, and no step asserts that something has not
//! arrived yet.

use super::*;

/// What a rate-limit scenario observed: when it recorded its start, if it bounds arrivals, and
/// when each matched payload arrived, in arrival order.
#[derive(Debug, Default)]
pub(crate) struct RateLimitObservation {
    started_at: Option<Instant>,
    arrivals: Vec<Instant>,
}

impl RateLimitObservation {
    /// The recorded start, which every bound is measured from.
    pub(crate) fn started_at(&self) -> Instant {
        self.started_at
            .assured("the scenario records its rate limit start before it measures an arrival")
    }
}

#[when("the rate limit start is recorded")]
fn record_rate_limit_start(world: &mut ScenarioWorld) {
    world.rate_limit = RateLimitObservation {
        started_at: Some(Instant::now()),
        arrivals: Vec::new(),
    };
}

/// Waits until every docstring line's `|`-separated fragments are found in exactly one payload,
/// failing on a payload that matches no remaining line, and records when each matched payload
/// arrived. A transport that does not keep order may deliver the lines in any order, so the
/// arrivals are ranked rather than tied to a line.
#[then(
    expr = "within {string} the relay subscription receives one payload for each fragment set and \
            records their arrivals"
)]
async fn receive_and_record_arrivals(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).assured("the step names a valid duration");
    let started_at = world.rate_limit.started_at;
    let mut remaining = docstring_fragment_sets(world, step);
    assert!(
        !remaining.is_empty(),
        "the step docstring lists at least one expected payload"
    );
    let deadline = Instant::now() + duration;
    let session = world
        .active_session
        .as_mut()
        .assured("the scenario opened its subscription before it receives payloads");
    let mut observed = Vec::new();
    let mut arrivals = Vec::with_capacity(remaining.len());
    while !remaining.is_empty() {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for payloads; still expected {remaining:?}, observed {observed:?}"
        );
        let event = session
            .try_next_subscription(deadline.saturating_duration_since(now))
            .await
            .assured("the scenario's subscription stays open while it receives payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for payloads; still expected {remaining:?}, observed \
                     {observed:?}"
                )
            });
        let arrived_at = Instant::now();
        let payload = event.payload;
        match started_at {
            Some(started_at) => append_cucumber_log_line(&format!(
                "rate limit arrival after {:?}: {payload}",
                arrived_at.duration_since(started_at)
            )),
            None => append_cucumber_log_line(&format!("rate limit arrival: {payload}")),
        }
        let matched = remaining
            .iter()
            .position(|fragments| fragments.iter().all(|fragment| payload.contains(fragment)));
        let Some(index) = matched else {
            panic!(
                "payload {payload:?} matches no expected payload; still expected {remaining:?}, \
                 observed {observed:?}"
            );
        };
        remaining.remove(index);
        arrivals.push(arrived_at);
        observed.push(payload);
    }
    world.last_subscription_payload = observed.last().cloned();
    world.rate_limit.arrivals = arrivals;
}

#[then(expr = "arrival {int} came no sooner than {string} after the recorded rate limit start")]
fn arrival_came_no_sooner_than(world: &mut ScenarioWorld, arrival: usize, bound: String) {
    let bound = parse_duration_text(&bound).assured("the step names a valid duration");
    let index = arrival
        .checked_sub(1)
        .assured("the step counts arrivals from 1");
    let started_at = world.rate_limit.started_at();
    let arrivals: Vec<Duration> = world
        .rate_limit
        .arrivals
        .iter()
        .map(|arrived_at| arrived_at.duration_since(started_at))
        .collect();
    let Some(arrived_after) = arrivals.get(index) else {
        panic!(
            "only {} arrivals were recorded, not {arrival}",
            arrivals.len()
        );
    };
    assert!(
        *arrived_after >= bound,
        "arrival {arrival} came {arrived_after:?} after the recorded start, sooner than the \
         allowance permits ({bound:?}); every arrival: {arrivals:?}"
    );
}
