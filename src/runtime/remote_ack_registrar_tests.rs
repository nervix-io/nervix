//! Owner tests of a receiver watch's view of the run that registered its acknowledgement.

use super::*;

fn run(incarnation: u64) -> ClusterNodeIncarnation {
    ClusterNodeIncarnation::new(incarnation)
}

fn after(start: Instant, elapsed: Duration) -> Instant {
    start
        .checked_add(elapsed)
        .assured("a fixture instant a few seconds ahead fits the monotonic clock")
}

/// A registrar whose run the watch has seen live, and when it saw it.
fn seen_live(registrar: ClusterNodeIncarnation) -> (RegistrarPresence, Instant) {
    let start = Instant::now();
    let mut presence = RegistrarPresence::undiscovered(start);
    assert_eq!(
        presence.look(registrar, Some(registrar), start),
        RegistrarLook::Report
    );
    assert_eq!(presence, RegistrarPresence::Present);
    (presence, start)
}

#[test]
fn a_registrar_missing_from_one_look_is_held_for_and_reported_to_again_when_it_is_back() {
    let registrar = run(7);
    let (mut presence, start) = seen_live(registrar);
    let gap = after(start, Duration::from_millis(100));
    assert_eq!(presence.look(registrar, None, gap), RegistrarLook::Hold);
    assert_eq!(presence, RegistrarPresence::Missing { since: gap });
    let back = after(start, Duration::from_millis(200));
    assert_eq!(
        presence.look(registrar, Some(registrar), back),
        RegistrarLook::Report
    );
    assert_eq!(presence, RegistrarPresence::Present);
}

#[test]
fn a_registrar_missing_for_its_silence_bound_and_one_sweep_ends_the_watch() {
    let registrar = run(7);
    let (mut presence, start) = seen_live(registrar);
    assert_eq!(presence.look(registrar, None, start), RegistrarLook::Hold);
    let grace = RegistrarPresence::missing_grace();
    assert!(
        grace > REMOTE_ACK_SILENCE_TIMEOUT,
        "the watch holds until after the registrar's last silent sweep could have failed it"
    );
    let just_inside = grace
        .checked_sub(Duration::from_millis(1))
        .assured("the grace is seconds long");
    assert_eq!(
        presence.look(registrar, None, after(start, just_inside)),
        RegistrarLook::Hold
    );
    assert_eq!(
        presence.look(registrar, None, after(start, grace)),
        RegistrarLook::Release
    );
}

#[test]
fn a_registrar_back_from_a_gap_starts_a_later_gap_afresh() {
    let registrar = run(7);
    let (mut presence, start) = seen_live(registrar);
    assert_eq!(presence.look(registrar, None, start), RegistrarLook::Hold);
    let back = after(start, Duration::from_secs(10));
    assert_eq!(
        presence.look(registrar, Some(registrar), back),
        RegistrarLook::Report
    );
    let second_gap = after(start, Duration::from_secs(12));
    assert_eq!(
        presence.look(registrar, None, second_gap),
        RegistrarLook::Hold
    );
    let grace = RegistrarPresence::missing_grace();
    let within_second_grace = grace
        .checked_sub(Duration::from_millis(1))
        .assured("the grace is seconds long");
    assert_eq!(
        presence.look(registrar, None, after(second_gap, within_second_grace)),
        RegistrarLook::Hold
    );
    assert_eq!(
        presence.look(registrar, None, after(second_gap, grace)),
        RegistrarLook::Release
    );
}

#[test]
fn another_run_of_the_registrar_node_ends_the_watch_at_once() {
    let registrar = run(7);
    let (mut presence, start) = seen_live(registrar);
    assert_eq!(
        presence.look(registrar, Some(run(8)), start),
        RegistrarLook::Release
    );

    let (mut missing, start) = seen_live(registrar);
    assert_eq!(missing.look(registrar, None, start), RegistrarLook::Hold);
    assert_eq!(
        missing.look(
            registrar,
            Some(run(8)),
            after(start, Duration::from_millis(100))
        ),
        RegistrarLook::Release
    );
}

#[test]
fn an_undiscovered_registrar_is_reported_to_until_its_discovery_deadline() {
    let registrar = run(7);
    let start = Instant::now();
    let mut presence = RegistrarPresence::undiscovered(start);
    assert_eq!(presence.look(registrar, None, start), RegistrarLook::Report);
    let just_inside = REMOTE_RELAY_INSTANTIATION_WAIT
        .checked_sub(Duration::from_millis(1))
        .assured("the discovery grace is seconds long");
    assert_eq!(
        presence.look(registrar, None, after(start, just_inside)),
        RegistrarLook::Report
    );
    assert_eq!(
        presence.look(
            registrar,
            None,
            after(start, REMOTE_RELAY_INSTANTIATION_WAIT)
        ),
        RegistrarLook::Release
    );
}
