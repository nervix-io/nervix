//! The decisions a remote delivery's sender takes after each failed attempt, from typed inputs.
//!
//! Layer: test harness.
//! - **Owns.** Holding a delivery whose grant is refused for capacity or whose send fails in
//!   transport, the waits between its asks, its admission bound, and giving it up on a permanent
//!   refusal.
//! - **Depends on.** The production decision owner and the typed grant refusal.
//! - **Must not know.** The transport, membership, payloads, relays or acknowledgements.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use strum::IntoEnumIterator as _;

use super::*;

fn after(start: Instant, elapsed: Duration) -> Instant {
    start
        .checked_add(elapsed)
        .assured("the fixture's instants lie within minutes of now")
}

fn held(start: Instant) -> RelaySendAttempts {
    RelaySendAttempts::new(
        after(start, REMOTE_RELAY_TOTAL_TIMEOUT),
        nervix_models::RemoteAckTiming::default(),
    )
}

fn capacity_refusals() -> impl Iterator<Item = RelayGrantRefusal> {
    RelayGrantRefusal::iter().filter(|refusal| refusal.is_capacity())
}

fn permanent_refusals() -> impl Iterator<Item = RelayGrantRefusal> {
    RelayGrantRefusal::iter().filter(|refusal| !refusal.is_capacity())
}

#[test]
fn the_capacity_refusals_are_the_room_a_grant_needs_now() {
    let capacity = capacity_refusals().collect::<Vec<_>>();
    assert_eq!(
        capacity,
        [
            RelayGrantRefusal::Memory,
            RelayGrantRefusal::QueueItem,
            RelayGrantRefusal::TerminalOutcome,
            RelayGrantRefusal::ChannelBusy,
        ]
    );
}

#[test]
fn the_default_retry_cadence_is_a_quarter_alive_interval_up_to_twenty_five_of_them() {
    let timing = nervix_models::RemoteAckTiming::default();
    assert_eq!(timing.retry_interval(), Duration::from_millis(25));
    assert_eq!(timing.retry_ceiling(), Duration::from_millis(2500));
}

#[test]
fn a_held_delivery_waits_twice_as_long_after_each_failure_up_to_the_retry_ceiling() {
    for failure in capacity_refusals()
        .map(RelaySendFailure::Refused)
        .chain([RelaySendFailure::Transport])
    {
        let start = Instant::now();
        let mut attempts = held(start);
        let expected =
            [25, 50, 100, 200, 400, 800, 1600, 2500, 2500, 2500].map(Duration::from_millis);
        let mut now = start;
        for expected_wait in expected {
            let step = attempts.after_failure(failure, now);
            let RelaySendStep::RetryAt(retry_at) = step else {
                panic!("a delivery within its bound is held after {failure:?}: {step:?}");
            };
            assert_eq!(retry_at.duration_since(now), expected_wait, "{failure:?}");
            now = retry_at;
        }
    }
}

#[test]
fn a_held_delivery_stays_held_long_after_any_former_stall_bound() {
    // A receiver that keeps refusing for capacity, or a connection that keeps failing while its
    // receiver stays live, costs the delivery latency only: an hour of failures well inside the
    // admission bound still holds it.
    let start = Instant::now();
    let bound = after(start, Duration::from_secs(7200));
    let mut attempts = RelaySendAttempts::new(bound, nervix_models::RemoteAckTiming::default());
    let mut now = start;
    while now < after(start, Duration::from_secs(3600)) {
        let step = attempts.after_failure(RelaySendFailure::Transport, now);
        let RelaySendStep::RetryAt(retry_at) = step else {
            panic!("a delivery inside its admission bound is never given up: {step:?}");
        };
        now = retry_at;
    }
}

#[test]
fn a_permanent_refusal_ends_the_delivery_at_once() {
    let start = Instant::now();
    for refusal in permanent_refusals() {
        let mut attempts = held(start);
        let step = attempts.after_failure(RelaySendFailure::Refused(refusal), start);
        assert_eq!(
            step,
            RelaySendStep::End(RelaySendEnd::Refused(refusal)),
            "{refusal}"
        );
    }
}

#[test]
fn a_held_delivery_asks_once_more_at_its_admission_bound_and_then_ends() {
    let start = Instant::now();
    let bound = after(start, Duration::from_secs(10));
    let mut attempts = RelaySendAttempts::new(bound, nervix_models::RemoteAckTiming::default());
    assert_eq!(attempts.admission_deadline(), bound);

    let mut now = start;
    loop {
        let step = attempts.after_failure(
            RelaySendFailure::Refused(RelayGrantRefusal::ChannelBusy),
            now,
        );
        match step {
            RelaySendStep::RetryAt(retry_at) => {
                assert!(
                    retry_at <= bound,
                    "a held delivery never asks past its bound"
                );
                now = retry_at;
            }
            RelaySendStep::End(end) => {
                assert_eq!(end, RelaySendEnd::AdmissionBound);
                assert_eq!(now, bound, "the last ask happens at the bound itself");
                break;
            }
        }
    }
}

#[test]
fn each_end_names_its_dispatch_failure() {
    let target = ClusterNodeName::parse("node-2").assured("the fixture node name is valid");
    assert_eq!(
        RelaySendEnd::Refused(RelayGrantRefusal::OutOfOrder)
            .dispatch_error(&target, nervix_models::RemoteAckTiming::default()),
        RemoteDispatchError::GrantRefused {
            target: target.clone(),
            refusal: RelayGrantRefusal::OutOfOrder,
        }
    );
    assert_eq!(
        RelaySendEnd::AdmissionBound
            .dispatch_error(&target, nervix_models::RemoteAckTiming::default()),
        RemoteDispatchError::AdmissionTotalTimeout {
            target,
            timeout: REMOTE_RELAY_TOTAL_TIMEOUT,
        }
    );
}
