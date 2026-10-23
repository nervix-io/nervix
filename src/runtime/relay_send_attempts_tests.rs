//! The decisions a remote delivery's sender takes after each failed attempt, from typed inputs.
//!
//! Layer: test harness.
//! - **Owns.** Holding a delivery whose grant is refused for capacity, the waits between its asks,
//!   its stall and admission bounds, and giving it up on a permanent refusal or a stall.
//! - **Depends on.** The production decision owner and the typed grant refusal.
//! - **Must not know.** The transport, payloads, relays or acknowledgements.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use strum::IntoEnumIterator as _;

use super::*;

const RETRY: Duration = RemoteDispatcher::DISPATCH_RETRY_INTERVAL;
const STALL: Duration = RemoteDispatcher::DISPATCH_TIMEOUT;

fn after(start: Instant, elapsed: Duration) -> Instant {
    start
        .checked_add(elapsed)
        .assured("the fixture's instants lie within minutes of now")
}

/// The last instant at which a retry of a transport failure still fits the first stall bound.
fn last_retry_within_stall(start: Instant) -> Instant {
    let margin = STALL
        .checked_sub(RETRY)
        .assured("the stall bound is longer than one retry interval");
    after(start, margin)
}

fn held(start: Instant) -> RelaySendAttempts {
    RelaySendAttempts::new(
        start,
        after(start, REMOTE_RELAY_TOTAL_TIMEOUT),
        GrantCapacityRefusals::Hold,
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
fn a_capacity_refusal_holds_the_delivery_and_restarts_the_stall_bound() {
    let start = Instant::now();
    for refusal in capacity_refusals() {
        let mut attempts = held(start);
        assert_eq!(attempts.attempt_deadline(), after(start, STALL));

        let answered = after(start, Duration::from_secs(4));
        let step = attempts.after_failure(RelaySendFailure::Refused(refusal), answered);

        assert_eq!(
            step,
            RelaySendStep::RetryAt(after(answered, RETRY)),
            "{refusal}"
        );
        assert_eq!(
            attempts.attempt_deadline(),
            after(answered, STALL),
            "{refusal}"
        );
    }
}

#[test]
fn a_held_delivery_waits_twice_as_long_after_each_refusal_up_to_half_the_stall_bound() {
    let start = Instant::now();
    let mut attempts = held(start);
    let ceiling = RelaySendAttempts::capacity_wait_ceiling();
    assert_eq!(ceiling, STALL / 2);

    let expected = [25, 50, 100, 200, 400, 800, 1600, 2500, 2500, 2500].map(Duration::from_millis);
    let mut now = start;
    for expected_wait in expected {
        let step =
            attempts.after_failure(RelaySendFailure::Refused(RelayGrantRefusal::Memory), now);
        let RelaySendStep::RetryAt(retry_at) = step else {
            panic!("a delivery refused for capacity within its bound is held: {step:?}");
        };
        assert_eq!(retry_at.duration_since(now), expected_wait);
        assert!(
            retry_at < attempts.attempt_deadline(),
            "a held delivery asks again before its restarted stall bound passes"
        );
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
    let mut attempts = RelaySendAttempts::new(start, bound, GrantCapacityRefusals::Hold);

    let mut now = start;
    loop {
        assert!(attempts.attempt_deadline() <= bound);
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
fn transport_failures_are_retried_within_the_stall_bound() {
    let start = Instant::now();
    let mut attempts = held(start);

    let early = after(start, Duration::from_secs(1));
    assert_eq!(
        attempts.after_failure(RelaySendFailure::Transport, early),
        RelaySendStep::RetryAt(after(early, RETRY))
    );
    assert_eq!(attempts.attempt_deadline(), after(start, STALL));

    let late = last_retry_within_stall(start);
    assert_eq!(
        attempts.after_failure(RelaySendFailure::Transport, late),
        RelaySendStep::End(RelaySendEnd::Stalled)
    );
}

#[test]
fn a_capacity_answer_keeps_a_held_delivery_past_the_stall_bound_transport_failures_end() {
    let start = Instant::now();
    let mut attempts = held(start);

    let answered = last_retry_within_stall(start);
    let step = attempts.after_failure(
        RelaySendFailure::Refused(RelayGrantRefusal::Memory),
        answered,
    );
    assert!(matches!(step, RelaySendStep::RetryAt(_)));

    let beyond_first_bound = after(start, STALL);
    assert_eq!(
        attempts.after_failure(RelaySendFailure::Transport, beyond_first_bound),
        RelaySendStep::RetryAt(after(beyond_first_bound, RETRY)),
        "the capacity answer restarted the stall bound"
    );
}

#[test]
fn a_delivery_that_gives_capacity_refusals_up_retries_them_only_within_the_stall_bound() {
    let start = Instant::now();
    let mut attempts = RelaySendAttempts::new(
        start,
        after(start, REMOTE_RELAY_TOTAL_TIMEOUT),
        GrantCapacityRefusals::GiveUp,
    );

    let early = after(start, Duration::from_secs(1));
    assert_eq!(
        attempts.after_failure(RelaySendFailure::Refused(RelayGrantRefusal::Memory), early),
        RelaySendStep::RetryAt(after(early, RETRY))
    );
    assert_eq!(
        attempts.attempt_deadline(),
        after(start, STALL),
        "a refusal it does not hold restarts nothing"
    );
    let late = last_retry_within_stall(start);
    assert_eq!(
        attempts.after_failure(RelaySendFailure::Refused(RelayGrantRefusal::Memory), late),
        RelaySendStep::End(RelaySendEnd::Stalled)
    );
    assert_eq!(
        attempts.after_failure(
            RelaySendFailure::Refused(RelayGrantRefusal::OutOfOrder),
            early
        ),
        RelaySendStep::End(RelaySendEnd::Refused(RelayGrantRefusal::OutOfOrder))
    );
}

#[test]
fn an_attempt_still_running_at_its_deadline_times_out_or_ends_at_the_admission_bound() {
    let start = Instant::now();
    let attempts = held(start);
    assert_eq!(
        attempts.after_attempt_deadline(after(start, STALL)),
        RelaySendEnd::TimedOut
    );

    let bound = after(start, Duration::from_secs(1));
    let attempts = RelaySendAttempts::new(start, bound, GrantCapacityRefusals::Hold);
    assert_eq!(attempts.attempt_deadline(), bound);
    assert_eq!(
        attempts.after_attempt_deadline(bound),
        RelaySendEnd::AdmissionBound
    );
}

#[test]
fn each_end_names_its_dispatch_failure() {
    let target = ClusterNodeName::parse("node-2").assured("the fixture node name is valid");
    assert_eq!(
        RelaySendEnd::Refused(RelayGrantRefusal::OutOfOrder).dispatch_error(&target),
        RemoteDispatchError::GrantRefused {
            target: target.clone(),
            refusal: RelayGrantRefusal::OutOfOrder,
        }
    );
    assert_eq!(
        RelaySendEnd::Stalled.dispatch_error(&target),
        RemoteDispatchError::Send {
            target: target.clone()
        }
    );
    assert_eq!(
        RelaySendEnd::TimedOut.dispatch_error(&target),
        RemoteDispatchError::SendTimeout {
            target: target.clone()
        }
    );
    assert_eq!(
        RelaySendEnd::AdmissionBound.dispatch_error(&target),
        RemoteDispatchError::AdmissionTotalTimeout {
            target,
            timeout: REMOTE_RELAY_TOTAL_TIMEOUT,
        }
    );
}
