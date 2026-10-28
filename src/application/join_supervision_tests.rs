//! Tests of how one run's join ends.
//!
//! Layer: test harness.
//!
//! - **Owns.** Coverage of the join judge's decisions: which peers a joining node probes and in
//!   what order, when a newcomer's or a recovered member's join ends, and when a peer's clock, a
//!   record or the leader's verdict refuses it.
//! - **Depends on.** The join judge, the clock skew one probe proves, the leader's verdict and the
//!   prerequisite refusal.
//! - **Must not know.** The transport, gossip and consensus the supervision reads its views from.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_interconnect::JoinRefusal;
use nervix_models::{AdmissionVerdict, ClusterNodeName, PrerequisiteMismatch, TimingPrerequisite};

use super::{
    JoinJudge, JoinOutcome, JoinRefusedError, JoinStanding, JoinStep, JoinView, ProbeAnswer,
};
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
        verdict: None,
        connected: connected.iter().map(|name| node(name)).collect(),
    }
}

fn judged(verdict: AdmissionVerdict, connected: &[&str]) -> JoinView {
    JoinView {
        verdict: Some(verdict),
        ..view(false, connected)
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

/// The verdict of a leader that judged this run's 400ms heartbeat from the side of node-2, a
/// voter whose heartbeat is 166ms.
fn refused_by_node_2() -> AdmissionVerdict {
    AdmissionVerdict::Refused {
        voter: node("node-2"),
        mismatch: PrerequisiteMismatch::Timing {
            prerequisite: TimingPrerequisite::RaftHeartbeatInterval,
            local: Duration::from_millis(166),
            peer: Duration::from_millis(400),
        },
    }
}

/// The refusal the run reads from that verdict: its own heartbeat first.
fn cannot_run_beside_node_2() -> JoinOutcome {
    JoinOutcome::Refused(JoinRefusedError::Prerequisite(JoinRefusal {
        peer: node("node-2"),
        mismatch: PrerequisiteMismatch::Timing {
            prerequisite: TimingPrerequisite::RaftHeartbeatInterval,
            local: Duration::from_millis(400),
            peer: Duration::from_millis(166),
        },
    }))
}

#[test]
fn a_newcomer_probes_the_peer_it_joined_through_first_and_each_peer_until_it_answers_within() {
    let mut judge = judge(JoinStanding::Newcomer);

    assert_eq!(
        judge.step(view(false, &["node-3", "node-1", "node-2"])),
        probes(&["node-1", "node-2", "node-3"])
    );
    assert_eq!(
        judge.record(node("node-1"), WITHIN, false),
        ProbeAnswer::Within
    );
    assert_eq!(
        judge.record(node("node-3"), ClockSkew::WithinProbe, false),
        ProbeAnswer::Within
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
fn a_newcomer_the_leader_refused_stops_naming_the_voter_it_cannot_run_beside() {
    let mut judge = judge(JoinStanding::Newcomer);
    assert_eq!(
        judge.record(node("node-1"), WITHIN, false),
        ProbeAnswer::Within
    );

    assert_eq!(
        judge.step(judged(refused_by_node_2(), &["node-1"])),
        JoinStep::Ended(cannot_run_beside_node_2())
    );
}

#[test]
fn a_newcomer_waits_for_its_admission_whatever_the_leader_accepted() {
    let mut judge = judge(JoinStanding::Newcomer);
    assert_eq!(
        judge.record(node("node-1"), WITHIN, false),
        ProbeAnswer::Within
    );

    assert_eq!(
        judge.step(judged(AdmissionVerdict::Accepted, &["node-1"])),
        probes(&[])
    );
}

#[test]
fn a_newcomer_whose_peer_proves_a_clock_beyond_tolerance_before_admission_is_refused() {
    let mut judge = judge(JoinStanding::Newcomer);

    assert_eq!(
        judge.record(node("node-2"), BEYOND, false),
        ProbeAnswer::Ended(clock_refusal("node-2", BEYOND))
    );
}

#[test]
fn a_skew_proven_once_the_membership_admitted_a_newcomer_does_not_refuse_it() {
    let mut judge = judge(JoinStanding::Newcomer);

    assert_eq!(
        judge.record(node("node-1"), BEYOND, true),
        ProbeAnswer::Ended(JoinOutcome::Admitted)
    );
}

#[test]
fn a_recovered_member_returns_once_the_leader_accepted_its_run_and_a_clock_answered_within() {
    let mut judge = judge(JoinStanding::RecoveredMember);

    // Its own membership holds it from the start, which ends nothing.
    assert_eq!(
        judge.step(view(true, &["node-2"])),
        probes(&["node-1", "node-2"])
    );
    assert_eq!(
        judge.record(node("node-2"), WITHIN, true),
        ProbeAnswer::Within
    );
    // A clock within tolerance is not enough while the leader has not judged the run.
    assert_eq!(judge.step(view(true, &["node-2"])), probes(&["node-1"]));

    assert_eq!(
        judge.step(judged(AdmissionVerdict::Accepted, &["node-2"])),
        JoinStep::Ended(JoinOutcome::Admitted)
    );
}

#[test]
fn a_recovered_member_the_leader_accepted_keeps_probing_until_a_clock_answers_within() {
    let judge = judge(JoinStanding::RecoveredMember);

    assert_eq!(
        judge.step(judged(AdmissionVerdict::Accepted, &["node-1", "node-2"])),
        probes(&["node-1", "node-2"])
    );
}

#[test]
fn a_recovered_member_the_leader_refused_stops_naming_the_voter_whatever_answered_first() {
    let mut judge = judge(JoinStanding::RecoveredMember);
    assert_eq!(
        judge.record(node("node-1"), WITHIN, true),
        ProbeAnswer::Within
    );

    assert_eq!(
        judge.step(judged(refused_by_node_2(), &["node-1"])),
        JoinStep::Ended(cannot_run_beside_node_2())
    );
}

#[test]
fn a_recovered_member_whose_first_answer_is_beyond_tolerance_is_refused() {
    let mut judge = judge(JoinStanding::RecoveredMember);

    assert_eq!(
        judge.record(node("node-1"), BEYOND, true),
        ProbeAnswer::Ended(clock_refusal("node-1", BEYOND))
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
        ProbeAnswer::Within
    );
}
