//! Owner tests of a receiver watch's view of the run that registered its acknowledgement.

use super::*;

fn run(incarnation: u64) -> ClusterNodeIncarnation {
    ClusterNodeIncarnation::new(incarnation)
}

fn after(start: Instant, elapsed: Duration) -> Instant {
    start
        .checked_add(elapsed)
        .assured("a fixture instant minutes ahead fits the monotonic clock")
}

fn admission_bound() -> Duration {
    nervix_models::RemoteAckTiming::default().remote_relay_total_timeout
}

/// A registrar whose run the watch has seen live, and when it saw it.
fn seen_live(registrar: ClusterNodeIncarnation) -> (RegistrarPresence, Instant) {
    let start = Instant::now();
    let mut presence = RegistrarPresence::undiscovered(start, admission_bound());
    assert_eq!(
        presence.look(registrar, Some(registrar), true, start),
        RegistrarLook::Report
    );
    assert_eq!(presence, RegistrarPresence::Seen);
    (presence, start)
}

#[test]
fn a_registrar_the_live_view_misses_while_membership_keeps_its_node_is_reported_to() {
    // A failure detector that suspects a slow node drops its run from the live view while the
    // run goes on. Membership decides that a node left, so the watch keeps reporting, however
    // long the gap lasts.
    let registrar = run(7);
    let (mut presence, start) = seen_live(registrar);
    for elapsed in [100, 15_000, 3_600_000].map(Duration::from_millis) {
        assert_eq!(
            presence.look(registrar, None, true, after(start, elapsed)),
            RegistrarLook::Report,
            "missing for {elapsed:?}"
        );
    }
    assert_eq!(
        presence.look(
            registrar,
            Some(registrar),
            true,
            after(start, Duration::from_secs(3601))
        ),
        RegistrarLook::Report
    );
}

#[test]
fn a_registrar_whose_node_membership_removed_ends_the_watch() {
    let registrar = run(7);
    let (mut presence, start) = seen_live(registrar);
    assert_eq!(
        presence.look(
            registrar,
            None,
            false,
            after(start, Duration::from_millis(1))
        ),
        RegistrarLook::Release
    );
}

#[test]
fn a_newer_run_of_the_registrar_ends_the_watch_at_once() {
    let registrar = run(7);
    let (mut presence, start) = seen_live(registrar);
    assert_eq!(
        presence.look(registrar, Some(run(8)), true, start),
        RegistrarLook::Release
    );
    let mut undiscovered = RegistrarPresence::undiscovered(start, admission_bound());
    assert_eq!(
        undiscovered.look(registrar, Some(run(8)), true, start),
        RegistrarLook::Release
    );
}

#[test]
fn an_earlier_run_in_the_live_view_is_the_registrar_not_yet_learned() {
    // Transport admission can precede gossip learning the registering run, so a live view that
    // still lists the node's earlier run does not end the watch.
    let registrar = run(7);
    let start = Instant::now();
    let mut presence = RegistrarPresence::undiscovered(start, admission_bound());
    assert_eq!(
        presence.look(registrar, Some(run(6)), true, start),
        RegistrarLook::Report
    );
    assert!(matches!(presence, RegistrarPresence::Undiscovered { .. }));
}

#[test]
fn an_undiscovered_registrar_is_reported_to_until_the_delivery_admission_bound() {
    let registrar = run(7);
    let start = Instant::now();
    let mut presence = RegistrarPresence::undiscovered(start, admission_bound());
    let just_inside = admission_bound()
        .checked_sub(Duration::from_millis(1))
        .assured("the admission bound is minutes long");
    assert_eq!(
        presence.look(registrar, None, false, after(start, just_inside)),
        RegistrarLook::Report
    );
    assert_eq!(
        presence.look(registrar, None, false, after(start, admission_bound())),
        RegistrarLook::Release
    );
}

#[test]
fn an_undiscovered_registrar_that_appears_is_seen_and_then_follows_membership() {
    let registrar = run(7);
    let start = Instant::now();
    let mut presence = RegistrarPresence::undiscovered(start, admission_bound());
    assert_eq!(
        presence.look(registrar, Some(registrar), true, start),
        RegistrarLook::Report
    );
    assert_eq!(presence, RegistrarPresence::Seen);
    // Once seen, the admission bound no longer ends the watch: only membership and a newer run do.
    let later = after(start, admission_bound().saturating_mul(2));
    assert_eq!(
        presence.look(registrar, None, true, later),
        RegistrarLook::Report
    );
    assert_eq!(
        presence.look(registrar, None, false, later),
        RegistrarLook::Release
    );
}
