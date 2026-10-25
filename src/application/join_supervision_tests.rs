//! Tests of how one run's join ends.
//!
//! Layer: test harness.
//!
//! - **Owns.** Coverage of the join judge's decisions: which peers a joining node probes and in
//!   what order, when a newcomer's or a recovered member's join ends, and when a peer's clock or
//!   record refuses it.
//! - **Depends on.** The join judge, the clock skew one probe proves and the prerequisite refusal.
//! - **Must not know.** The transport, gossip and consensus the supervision reads its views from.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_interconnect::JoinRefusal;
use nervix_models::{ClusterNodeName, PrerequisiteMismatch};

use super::{JoinJudge, JoinOutcome, JoinRefusedError, JoinStanding, JoinStep, JoinView};
use crate::cluster::ClockSkew;

const TOLERANCE: Duration = Duration::from_millis(500);
const WITHIN: ClockSkew = ClockSkew::PeerAhead {
    at_least: Duration::from_millis(200),
};
const BEYOND: ClockSkew = ClockSkew::PeerBehind {
    at_least: Duration::from_millis(1_200),
};

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the test's node names are valid")
}

/// A judge for a run whose first outbound handshake completed with node-1.
fn judge(standing: JoinStanding) -> JoinJudge {
    JoinJudge::new(standing, node("node-1"), TOLERANCE)
}

fn view(admitted: bool, connected: &[&str]) -> JoinView {
    JoinView {
        admitted,
        incompatible_member: None,
        connected: connected.iter().map(|name| node(name)).collect(),
    }
}

fn probes(peers: &[&str]) -> JoinStep {
    JoinStep::Probe(peers.iter().map(|name| node(name)).collect())
}

fn clock_refusal(peer: &str, skew: ClockSkew) -> JoinOutcome {
    JoinOutcome::Refused(JoinRefusedError::ClockSkew {
        peer: node(peer),
        skew,
        tolerance: TOLERANCE,
    })
}

fn another_cluster(peer: &str) -> JoinRefusal {
    JoinRefusal {
        peer: node(peer),
        mismatch: PrerequisiteMismatch::ClusterIdentity {
            local: String::from("cluster-a"),
            peer: String::from("cluster-b"),
        },
    }
}

#[test]
fn a_newcomer_probes_the_peer_it_joined_through_first_and_each_peer_until_it_answers_within() {
    let mut judge = judge(JoinStanding::Newcomer);

    assert_eq!(
        judge.step(view(false, &["node-3", "node-1", "node-2"])),
        probes(&["node-1", "node-2", "node-3"])
    );
    assert_eq!(judge.record(node("node-1"), WITHIN, false), None);
    assert_eq!(
        judge.record(node("node-3"), ClockSkew::WithinProbe, false),
        None
    );

    // node-2 did not answer, so only it is probed again.
    assert_eq!(
        judge.step(view(false, &["node-1", "node-2", "node-3"])),
        probes(&["node-2"])
    );
}

#[test]
fn the_peer_a_node_joined_through_is_probed_while_it_is_not_connected() {
    let judge = judge(JoinStanding::Newcomer);

    assert_eq!(
        judge.step(view(false, &["node-2"])),
        probes(&["node-1", "node-2"])
    );
}

#[test]
fn a_newcomer_admitted_before_any_peer_answered_ends_its_join_in_the_cluster() {
    let judge = judge(JoinStanding::Newcomer);

    assert_eq!(
        judge.step(view(true, &["node-1"])),
        JoinStep::Ended(JoinOutcome::Admitted)
    );
}

#[test]
fn a_newcomer_is_refused_by_a_live_member_it_cannot_run_beside() {
    let judge = judge(JoinStanding::Newcomer);
    let seen = JoinView {
        incompatible_member: Some(another_cluster("node-2")),
        ..view(false, &["node-1", "node-2"])
    };

    assert_eq!(
        judge.step(seen),
        JoinStep::Ended(JoinOutcome::Refused(JoinRefusedError::Prerequisite(
            another_cluster("node-2")
        )))
    );
}

#[test]
fn a_newcomer_whose_peer_proves_a_clock_beyond_tolerance_before_admission_is_refused() {
    let mut judge = judge(JoinStanding::Newcomer);

    assert_eq!(
        judge.record(node("node-2"), BEYOND, false),
        Some(clock_refusal("node-2", BEYOND))
    );
}

#[test]
fn a_skew_proven_once_the_membership_admitted_a_newcomer_does_not_refuse_it() {
    let mut judge = judge(JoinStanding::Newcomer);

    assert_eq!(
        judge.record(node("node-1"), BEYOND, true),
        Some(JoinOutcome::Admitted)
    );
}

#[test]
fn a_recovered_member_is_not_ended_by_its_membership_and_ends_at_the_first_answer() {
    let mut judge = judge(JoinStanding::RecoveredMember);

    assert_eq!(
        judge.step(view(true, &["node-2"])),
        probes(&["node-1", "node-2"])
    );
    assert_eq!(
        judge.record(node("node-2"), WITHIN, true),
        Some(JoinOutcome::Admitted)
    );
}

#[test]
fn a_recovered_member_whose_first_answer_is_beyond_tolerance_is_refused() {
    let mut judge = judge(JoinStanding::RecoveredMember);

    assert_eq!(
        judge.record(node("node-1"), BEYOND, true),
        Some(clock_refusal("node-1", BEYOND))
    );
}

#[test]
fn a_skew_equal_to_the_tolerance_is_within_it() {
    let mut judge = judge(JoinStanding::RecoveredMember);
    let at_tolerance = ClockSkew::PeerAhead {
        at_least: TOLERANCE,
    };

    assert_eq!(
        judge.record(node("node-1"), at_tolerance, true),
        Some(JoinOutcome::Admitted)
    );
}
