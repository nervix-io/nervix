//! Tests of the drain a stopping node asks the leader for.
//!
//! Layer: test harness.
//!
//! - **Owns.** Coverage of what the leader does for the node that asks, of how the asking node
//!   reads an answer that never arrives, and of how long and when the asking node waits for the
//!   release of its drain cordon, including the leadership observation an interrupted drain logs.
//! - **Depends on.** The scheduling control plane, the moves' deadline and the server test
//!   fixture.
//! - **Must not know.** Connector implementations or runtime execution details.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{collections::BTreeSet, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_consensus::ConsensusError;
use nervix_interconnect::{
    OwnershipHandoffFailure, RequestError, StoppingNodeDrainAction, StoppingNodeDrainResponse,
    StoppingNodeUnmoved,
};
use nervix_models::ClusterNodeName;
use nervix_primitives::time::Instant;
use nervix_recovery::Discarded as _;

#[cfg(feature = "testing")]
use super::super::test_fixtures::build_test_service_with_fault_injection;
use super::{
    super::{
        command_result::{CommandDisposition, CommandResult, LeaderRedirect, NodeLocation},
        shutdown::{ShutdownDeadline, ShutdownMoveOutcome},
        test_fixtures::{TestService, build_test_service, node_named},
    },
    DrainEligibilityReplans, DrainUnmovedUnits, SessionServiceImpl, ShutdownDrainAnswer,
    ShutdownDrainCordon, ShutdownLeaderRequests, ShutdownMoveBudget, ShutdownOwnershipMove,
    ShutdownPhaseOutcome, ShutdownReleaseAnswer, ShutdownReleaseQuorum,
};
use crate::{
    application::{domain_lifecycle::DomainAlterError, schedule_planning::SchedulePlanningStale},
    captured_logs::CapturedLogs,
    runtime::OwnershipHandoffError,
};

/// The budget of moves that begin now and may take `timeout`.
fn moves_taking(timeout: Duration) -> ShutdownMoveBudget {
    ShutdownMoveBudget::new(ShutdownDeadline::starting_now(timeout), release_grace())
}

/// Every way a stopping node's drain request to `leader` can end, in the order: every unit moved,
/// units left with nowhere to move, units left that could have moved, a failed drain, and no node
/// left that could take the work.
fn every_drain_answer(leader: &ClusterNodeName) -> [ShutdownDrainAnswer; 5] {
    let completed = ShutdownDrainAnswer::Completed {
        leader: leader.clone(),
        report: "drained".to_string(),
    };
    let nowhere_to_move = ShutdownDrainAnswer::Unmoved {
        leader: leader.clone(),
        report: "no live schedulable raft voters remain".to_string(),
        unmoved: StoppingNodeUnmoved::NowhereToMove,
    };
    let could_have_moved = ShutdownDrainAnswer::Unmoved {
        leader: leader.clone(),
        report: "a model or schedule change is already in progress".to_string(),
        unmoved: StoppingNodeUnmoved::CouldHaveMoved,
    };
    let failed = ShutdownDrainAnswer::Failed {
        leader: leader.clone(),
        report: "failed to cordon node before drain".to_string(),
    };
    [
        completed,
        nowhere_to_move,
        could_have_moved,
        failed,
        ShutdownDrainAnswer::NoReplacement,
    ]
}

/// Captures the production drain retry response without starting another drain or election.
fn stopping_node_drain_retry_log(disposition: CommandDisposition) -> String {
    let node = node_named("node-2");
    let drained = CommandResult::new(disposition, "drain report".to_string());
    let logs = CapturedLogs::of(|| {
        assert_eq!(
            SessionServiceImpl::stopping_node_drain_retry_response(&node, &drained),
            StoppingNodeDrainResponse::NotLeader,
        );
    });
    assert!(logs.contains("INFO"), "{logs}");
    assert!(logs.contains("node_id=node-2"), "{logs}");
    assert!(logs.contains("report=drain report"), "{logs}");
    logs
}

#[test]
fn a_stopping_node_drain_with_an_unconfirmed_lease_reports_that_the_same_leader_may_still_lead() {
    let logs = stopping_node_drain_retry_log(CommandDisposition::NotLeader(
        LeaderRedirect::UnconfirmedLease { term: 7 },
    ));
    assert!(
        logs.contains(
            "stopping-node drain interrupted: the leader could not confirm its lease in term 7 \
             and may still lead; remaining work can be retried at the observed leader"
        ),
        "{logs}"
    );
}

#[test]
fn a_stopping_node_drain_redirect_reports_the_observed_leader() {
    let logs = stopping_node_drain_retry_log(CommandDisposition::NotLeader(
        LeaderRedirect::Leader(NodeLocation {
            node: node_named("node-3"),
            grpc_url: None,
            web_console_url: None,
        }),
    ));
    assert!(
        logs.contains(
            "stopping-node drain interrupted: leadership moved to node 'node-3'; remaining work \
             can be retried at the observed leader"
        ),
        "{logs}"
    );
}

#[test]
fn a_stopping_node_drain_redirect_reports_when_no_leader_is_known() {
    let logs =
        stopping_node_drain_retry_log(CommandDisposition::NotLeader(LeaderRedirect::NoLeader));
    assert!(
        logs.contains(
            "stopping-node drain interrupted: leadership moved, and no leader is known yet; \
             remaining work can be retried at the observed leader"
        ),
        "{logs}"
    );
}

#[test]
fn a_stopping_node_drain_reports_a_leadership_change_observed_after_its_failure() {
    let logs = stopping_node_drain_retry_log(CommandDisposition::Failed);
    assert!(
        logs.contains(
            "stopping-node drain interrupted: leadership moved; remaining work can be retried at \
             the observed leader"
        ),
        "{logs}"
    );
}

#[test]
fn units_left_where_no_other_node_could_take_them_are_nowhere_to_move_and_any_other_ending_disrupts()
 {
    let local_node = node_named("node-2");
    let mut outcomes = Vec::new();
    let logs = CapturedLogs::of(|| {
        for answer in every_drain_answer(&node_named("node-1")) {
            outcomes.push(answer.into_drain_outcome(&local_node));
        }
    });
    assert_eq!(
        outcomes,
        [
            ShutdownMoveOutcome::Moved,
            ShutdownMoveOutcome::NowhereToMove,
            ShutdownMoveOutcome::Disrupted,
            ShutdownMoveOutcome::Disrupted,
            ShutdownMoveOutcome::NowhereToMove,
        ]
    );
    assert!(
        logs.contains("the leader found no other node that can take the units left on this node"),
        "{logs}"
    );
    assert!(
        logs.contains(
            "every node that could take this node's units began stopping or left, so it stopped \
             asking the leader to move them"
        ),
        "{logs}"
    );
}

#[test]
fn only_a_completed_answer_completes_the_release_of_the_drain_cordon() {
    let local_node = node_named("node-2");
    let leader = node_named("node-1");
    let answers = [
        ShutdownReleaseAnswer::Completed {
            leader: leader.clone(),
            report: format!("uncordoned node '{local_node}'"),
        },
        ShutdownReleaseAnswer::Failed {
            leader,
            report: "the cordon release did not commit".to_string(),
        },
    ];
    let outcomes = answers.map(|answer| answer.into_cordon_release_outcome(&local_node));
    assert_eq!(
        outcomes,
        [
            ShutdownPhaseOutcome::Completed,
            ShutdownPhaseOutcome::Abandoned
        ]
    );
}

#[test]
fn a_drain_that_left_one_unit_that_could_have_moved_left_units_that_could_have_moved() {
    let mut nowhere_only = DrainUnmovedUnits::default();
    assert!(!nowhere_only.any());
    nowhere_only.record(StoppingNodeUnmoved::NowhereToMove);
    nowhere_only.record(StoppingNodeUnmoved::NowhereToMove);
    assert_eq!(nowhere_only.0, Some(StoppingNodeUnmoved::NowhereToMove));

    let mut could_have_moved_first = DrainUnmovedUnits::default();
    could_have_moved_first.record(StoppingNodeUnmoved::CouldHaveMoved);
    could_have_moved_first.record(StoppingNodeUnmoved::NowhereToMove);
    assert_eq!(
        could_have_moved_first.0,
        Some(StoppingNodeUnmoved::CouldHaveMoved)
    );

    let mut could_have_moved_last = DrainUnmovedUnits::default();
    could_have_moved_last.record(StoppingNodeUnmoved::NowhereToMove);
    could_have_moved_last.record(StoppingNodeUnmoved::CouldHaveMoved);
    assert!(could_have_moved_last.any());
    assert_eq!(
        could_have_moved_last.0,
        Some(StoppingNodeUnmoved::CouldHaveMoved)
    );
}

#[test]
fn a_destination_that_refused_because_it_is_terminating_is_named_beneath_the_gate_failure() {
    let destination = node_named("node-3");
    let gate_failure = |cause: Report<OwnershipHandoffError>| {
        cause.change_context(DomainAlterError::EntityGate {
            domain: "orders".parse().assured("the test names a valid domain"),
            operation: "ownership handoff",
            reason: "failed to prepare junction 'route' from node 'node-1' on node 'node-3'"
                .to_string(),
        })
    };
    // The refusal arrives from the destination over the interconnect.
    let refusal = OwnershipHandoffError::of_remote_rejection(
        OwnershipHandoffFailure::DestinationTerminating {
            node: destination.clone(),
        },
    );
    let refused = gate_failure(refusal);
    let rejected = gate_failure(OwnershipHandoffError::of_remote_rejection(
        OwnershipHandoffFailure::rejected("the destination's schedule moved on"),
    ));

    assert_eq!(
        SessionServiceImpl::terminating_destination_refusal(&refused),
        Some(destination)
    );
    assert_eq!(
        SessionServiceImpl::terminating_destination_refusal(&rejected),
        None
    );
    assert!(
        format!("{refused:#}").ends_with(
            "failed to prepare junction 'route' from node 'node-1' on node 'node-3': destination \
             node 'node-3' is terminating"
        ),
        "{refused:#}"
    );
}

#[test]
fn a_drain_plans_nothing_more_onto_destinations_that_refused_because_they_are_terminating() {
    let candidates = [
        node_named("node-1"),
        node_named("node-2"),
        node_named("node-3"),
    ];
    let terminating = nodes_named(&["node-2"]);

    assert_eq!(
        SessionServiceImpl::drain_replacement_nodes(&candidates, &terminating),
        vec![node_named("node-1"), node_named("node-3")]
    );
    assert_eq!(
        SessionServiceImpl::drain_replacement_nodes(
            &candidates,
            &nodes_named(&["node-1", "node-2", "node-3"])
        ),
        Vec::<ClusterNodeName>::new()
    );
}

#[nervix_primitives::test]
async fn the_leader_drains_and_then_uncordons_the_stopping_node_that_asks() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let local_node = service.inner.consensus.local_node_id().clone();

    let drained = service
        .act_for_stopping_node(
            local_node.clone(),
            StoppingNodeDrainAction::Drain {
                budget: Duration::from_secs(30),
            },
        )
        .await;
    let StoppingNodeDrainResponse::Completed { report } = drained else {
        panic!("the leader did not drain the node that asked: {drained:?}");
    };
    assert!(
        report.contains(&format!("drained node '{local_node}'")),
        "{report}"
    );
    assert!(
        service
            .inner
            .consensus
            .cordoned_node_ids()
            .await
            .contains(&local_node),
        "a drain cordons the node it drains"
    );

    let released = service
        .answer_stopping_node_drain(local_node.clone(), StoppingNodeDrainAction::ReleaseCordon)
        .await;
    assert_eq!(
        released,
        StoppingNodeDrainResponse::Completed {
            report: format!("uncordoned node '{local_node}'"),
        }
    );
    assert!(
        !service
            .inner
            .consensus
            .cordoned_node_ids()
            .await
            .contains(&local_node)
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[nervix_primitives::test]
async fn the_leader_reports_the_drain_of_a_node_outside_its_membership_as_failed() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let stranger = node_named("node-stranger");

    let drained = service
        .act_for_stopping_node(
            stranger.clone(),
            StoppingNodeDrainAction::Drain {
                budget: Duration::from_secs(30),
            },
        )
        .await;
    let StoppingNodeDrainResponse::Failed { report } = drained else {
        panic!("the leader drained a node outside its membership: {drained:?}");
    };
    assert!(
        report.contains(&format!("node '{stranger}' is not a raft member")),
        "{report}"
    );
    assert!(
        !service
            .inner
            .consensus
            .cordoned_node_ids()
            .await
            .contains(&stranger)
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[nervix_primitives::test]
async fn a_leading_node_answers_its_own_drain_request_in_process() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let local_node = service.inner.consensus.local_node_id().clone();
    service
        .inner
        .consensus
        .set_node_cordoned(local_node.clone(), true)
        .await
        .assured("the single-node test leader commits its own cordon");

    let answer = service
        .request_shutdown_cordon_release(&local_node, &mut ShutdownLeaderRequests::default())
        .await;
    let ShutdownReleaseAnswer::Completed { leader, report } = answer else {
        panic!("the leading node did not release its own cordon: {answer:?}");
    };
    assert_eq!(leader, local_node);
    assert_eq!(report, format!("uncordoned node '{local_node}'"));

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[nervix_primitives::test]
async fn a_stopping_node_asks_again_after_an_unanswered_drain_and_stops_once_no_node_can_take_its_work()
 {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let local_node = service.inner.consensus.local_node_id().clone();
    let leader = node_named("node-elsewhere");
    service.inner.interconnect.shutdown().await;
    let logs = CapturedLogs::default();
    let guard = tracing::subscriber::set_default(logs.subscriber());

    // The node is alone in its cluster, so once the leader it asked first does not answer, it
    // finds that no other node can take its work instead of asking anyone else.
    let mut requests = ShutdownLeaderRequests::default();
    let answer = service
        .request_shutdown_drain(
            &local_node,
            leader.clone(),
            Duration::from_secs(30),
            &mut requests,
        )
        .await;
    drop(guard);

    let ShutdownDrainAnswer::NoReplacement = answer else {
        panic!("a lone stopping node kept asking for a drain: {answer:?}");
    };
    let logs = logs.contents();
    assert!(
        logs.contains(
            "the leader did not answer this stopping node's request, so the node asks the leader \
             it observes next within its shutdown timeout"
        ),
        "{logs}"
    );
    assert!(logs.contains("leader=node-elsewhere"), "{logs}");
    assert!(logs.contains("request=\"drain\""), "{logs}");
    assert!(
        requests.departed.is_empty(),
        "a transport that stopped found no departed node"
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[test]
fn a_leader_that_left_while_asked_is_departed_and_logged_once() {
    let local_node = node_named("node-2");
    let leader = node_named("node-1");
    let mut requests = ShutdownLeaderRequests::default();
    let left = Report::new(RequestError::TargetLeft {
        node: leader.clone(),
        request: "stopping_node_drain",
    });
    let shutting_down = Report::new(RequestError::ShuttingDown {
        node: node_named("node-3"),
        request: "stopping_node_drain",
    });

    let logs = CapturedLogs::of(|| {
        requests.record_unanswered(&local_node, &leader, "drain", &left);
        requests.record_unanswered(&local_node, &leader, "drain", &left);
        requests.record_unanswered(&local_node, &node_named("node-3"), "drain", &shutting_down);
    });

    // Only a node the interconnect found had left the cluster counts as departed.
    assert_eq!(requests.departed, nodes_named(&["node-1"]));
    // The capture records `info` and above, so the repeated answer of the same leader, logged at
    // `debug`, does not appear: each leader that stops answering is logged at `info` once.
    assert_eq!(
        logs.matches("the leader did not answer this stopping node's request")
            .count(),
        2,
        "{logs}"
    );
    assert!(logs.contains("leader=node-1"), "{logs}");
    assert!(logs.contains("leader=node-3"), "{logs}");
}

#[test]
fn a_drain_plans_again_after_an_eligibility_change_at_most_once_per_member() {
    let domain = || "orders".parse().assured("the test names a valid domain");
    let eligibility = Report::new(SchedulePlanningStale::Eligibility { domain: domain() });
    let schedule = Report::new(SchedulePlanningStale::Schedule { domain: domain() });
    let mut replans = DrainEligibilityReplans::for_members(2);

    // Only a change of the eligible nodes is planned again; any other staleness fails the move.
    assert!(!replans.take(&schedule));
    assert!(replans.take(&eligibility));
    assert!(replans.take(&eligibility));
    assert!(!replans.take(&eligibility));
}

/// The node names `raw`, as one set.
fn nodes_named(raw: &[&str]) -> BTreeSet<ClusterNodeName> {
    raw.iter().map(|name| node_named(name)).collect()
}

#[nervix_primitives::test]
async fn the_cordon_release_waits_for_what_remains_of_the_shutdown_timeout() {
    let shutdown_timeout = Duration::from_secs(30);
    let started = Instant::now();
    let budget = moves_taking(shutdown_timeout);
    let bound = budget.cordon_release_bound(ShutdownReleaseQuorum::Staying);
    let elapsed = started.elapsed();

    let least = shutdown_timeout
        .checked_sub(elapsed)
        .unwrap_or(Duration::ZERO);
    assert!(bound <= shutdown_timeout, "{bound:?}");
    assert!(bound >= least, "{bound:?} is less than {least:?}");
}

#[nervix_primitives::test]
async fn the_cordon_release_keeps_its_grace_when_the_moves_used_the_shutdown_timeout() {
    let budget = moves_taking(Duration::ZERO);

    assert_eq!(budget.remaining(), Duration::ZERO);
    assert_eq!(
        budget.cordon_release_bound(ShutdownReleaseQuorum::Staying),
        release_grace()
    );
}

#[nervix_primitives::test]
async fn a_cordon_release_only_stopping_voters_could_commit_keeps_only_its_grace() {
    let budget = moves_taking(Duration::from_secs(30));

    assert_eq!(
        budget.cordon_release_bound(ShutdownReleaseQuorum::Leaving),
        release_grace()
    );
}

#[test]
fn a_release_can_commit_with_staying_voters_only_while_they_and_the_stopping_node_form_a_quorum() {
    let stopping_node = node_named("node-2");
    let voters = nodes_named(&["node-1", "node-2", "node-3"]);

    let rolling_restart =
        ShutdownReleaseQuorum::of(&stopping_node, &voters, &nodes_named(&["node-1", "node-3"]));
    let one_peer_stopping_too =
        ShutdownReleaseQuorum::of(&stopping_node, &voters, &nodes_named(&["node-3"]));
    let whole_cluster_stopping =
        ShutdownReleaseQuorum::of(&stopping_node, &voters, &BTreeSet::new());
    let only_a_learner_staying =
        ShutdownReleaseQuorum::of(&stopping_node, &voters, &nodes_named(&["node-4"]));

    assert_eq!(rolling_restart, ShutdownReleaseQuorum::Staying);
    assert_eq!(one_peer_stopping_too, ShutdownReleaseQuorum::Staying);
    assert_eq!(whole_cluster_stopping, ShutdownReleaseQuorum::Leaving);
    assert_eq!(only_a_learner_staying, ShutdownReleaseQuorum::Leaving);
}

#[test]
fn a_stopping_node_outside_the_voters_needs_a_quorum_of_staying_voters() {
    let stopping_learner = node_named("node-4");
    let voters = nodes_named(&["node-1", "node-2", "node-3"]);

    let two_voters_staying = ShutdownReleaseQuorum::of(
        &stopping_learner,
        &voters,
        &nodes_named(&["node-1", "node-2"]),
    );
    let one_voter_staying =
        ShutdownReleaseQuorum::of(&stopping_learner, &voters, &nodes_named(&["node-1"]));

    assert_eq!(two_voters_staying, ShutdownReleaseQuorum::Staying);
    assert_eq!(one_voter_staying, ShutdownReleaseQuorum::Leaving);
}

#[test]
fn a_move_that_could_have_taken_place_and_did_not_complete_disrupts_the_cluster() {
    let stopping_node = node_named("node-2");
    let shutdown_timeout = Duration::from_secs(600);
    let moves = [
        ShutdownOwnershipMove::NoReplacement,
        ShutdownOwnershipMove::NotRequested,
        ShutdownOwnershipMove::Requested(ShutdownMoveOutcome::Moved),
        ShutdownOwnershipMove::Requested(ShutdownMoveOutcome::NowhereToMove),
        ShutdownOwnershipMove::Requested(ShutdownMoveOutcome::Disrupted),
    ];
    let mut outcomes = Vec::with_capacity(moves.len());
    let logs = CapturedLogs::of(|| {
        for ownership_move in &moves {
            outcomes.push(ownership_move.outcome(&stopping_node, shutdown_timeout));
        }
    });

    assert_eq!(
        outcomes,
        [
            ShutdownMoveOutcome::NowhereToMove,
            ShutdownMoveOutcome::Disrupted,
            ShutdownMoveOutcome::Moved,
            ShutdownMoveOutcome::NowhereToMove,
            ShutdownMoveOutcome::Disrupted,
        ]
    );
    assert!(
        logs.contains(
            "no other node can take this node's units, so they stay saved here and a restart \
             restores them"
        ),
        "{logs}"
    );
    assert!(
        logs.contains("every unit of this node moved to another node"),
        "{logs}"
    );
    assert!(logs.contains("ERROR"), "{logs}");
    assert!(logs.contains("a cluster disruption"), "{logs}");
    assert!(logs.contains("shutdown_timeout=10m"), "{logs}");
}

#[test]
fn only_a_requested_drain_of_a_node_no_operator_cordoned_leaves_a_cordon_to_release() {
    let moves = [
        ShutdownOwnershipMove::NoReplacement,
        ShutdownOwnershipMove::NotRequested,
        ShutdownOwnershipMove::Requested(ShutdownMoveOutcome::Moved),
        ShutdownOwnershipMove::Requested(ShutdownMoveOutcome::NowhereToMove),
        ShutdownOwnershipMove::Requested(ShutdownMoveOutcome::Disrupted),
    ];
    let without_operator_cordon = moves
        .each_ref()
        .map(|ownership_move| ownership_move.drain_cordon(false));
    let with_operator_cordon = moves
        .each_ref()
        .map(|ownership_move| ownership_move.drain_cordon(true));
    assert_eq!(
        without_operator_cordon,
        [
            ShutdownDrainCordon::NeverSet,
            ShutdownDrainCordon::NeverSet,
            ShutdownDrainCordon::SetByDrain,
            ShutdownDrainCordon::SetByDrain,
            ShutdownDrainCordon::SetByDrain,
        ]
    );
    assert_eq!(
        with_operator_cordon,
        [
            ShutdownDrainCordon::NeverSet,
            ShutdownDrainCordon::NeverSet,
            ShutdownDrainCordon::Operator,
            ShutdownDrainCordon::Operator,
            ShutdownDrainCordon::Operator,
        ]
    );
}

#[test]
fn a_leader_that_lost_its_leadership_while_releasing_the_cordon_answers_as_not_leading() {
    let stopping_node = node_named("node-2");
    let released = SessionServiceImpl::cordon_release_response(&stopping_node, Ok(()));
    let lost = SessionServiceImpl::cordon_release_response(
        &stopping_node,
        Err(Report::new(ConsensusError::LeadershipLost {
            observed: nervix_consensus::LeadershipObservation::Leader(node_named("node-1")),
        })),
    );
    let failed = SessionServiceImpl::cordon_release_response(
        &stopping_node,
        Err(Report::new(ConsensusError::RaftWrite)),
    );

    assert_eq!(
        released,
        StoppingNodeDrainResponse::Completed {
            report: "uncordoned node 'node-2'".to_string(),
        }
    );
    assert_eq!(lost, StoppingNodeDrainResponse::NotLeader);
    assert_eq!(
        failed,
        StoppingNodeDrainResponse::Failed {
            report: "raft write failed".to_string(),
        }
    );
}

#[nervix_primitives::test]
async fn a_stopping_node_releases_only_the_cordon_its_own_drain_set() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let local_node = service.inner.consensus.local_node_id().clone();
    let budget = moves_taking(Duration::from_secs(30));
    service
        .inner
        .consensus
        .set_node_cordoned(local_node.clone(), true)
        .await
        .assured("the single-node test leader commits its own cordon");

    let operator = service
        .settle_shutdown_drain_cordon(
            &local_node,
            ShutdownDrainCordon::Operator,
            &budget,
            &mut ShutdownLeaderRequests::default(),
        )
        .await;
    assert_eq!(operator, ShutdownPhaseOutcome::Completed);
    assert!(
        service
            .inner
            .consensus
            .cordoned_node_ids()
            .await
            .contains(&local_node),
        "an operator cordon outlives the stop"
    );

    let released = service
        .settle_shutdown_drain_cordon(
            &local_node,
            ShutdownDrainCordon::SetByDrain,
            &budget,
            &mut ShutdownLeaderRequests::default(),
        )
        .await;
    assert_eq!(released, ShutdownPhaseOutcome::Completed);
    assert!(
        !service
            .inner
            .consensus
            .cordoned_node_ids()
            .await
            .contains(&local_node),
        "the stop releases the cordon its own drain set"
    );

    let never_set = service
        .settle_shutdown_drain_cordon(
            &local_node,
            ShutdownDrainCordon::NeverSet,
            &budget,
            &mut ShutdownLeaderRequests::default(),
        )
        .await;
    assert_eq!(never_set, ShutdownPhaseOutcome::Completed);

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[cfg(feature = "testing")]
#[nervix_primitives::test]
async fn a_cordon_release_that_outlasts_its_bound_is_abandoned_and_leaves_the_cordon() {
    let fault_injection = crate::FaultInjection::default();
    let TestService {
        service,
        registry,
        path,
    } = build_test_service_with_fault_injection(true, fault_injection.clone()).await;
    let local_node = service.inner.consensus.local_node_id().clone();
    service
        .inner
        .consensus
        .set_node_cordoned(local_node.clone(), true)
        .await
        .assured("the single-node test leader commits its own cordon");
    fault_injection.delay_shutdown_cordon_release_of(local_node.clone(), Duration::from_secs(60));
    let budget = moves_taking(Duration::ZERO);

    let outcome = service
        .settle_shutdown_drain_cordon(
            &local_node,
            ShutdownDrainCordon::SetByDrain,
            &budget,
            &mut ShutdownLeaderRequests::default(),
        )
        .await;

    assert_eq!(outcome, ShutdownPhaseOutcome::Abandoned);
    assert!(
        service
            .inner
            .consensus
            .cordoned_node_ids()
            .await
            .contains(&local_node),
        "a release that never ran leaves the cordon its drain set"
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

fn release_grace() -> Duration {
    nervix_consensus::RaftTimings::default().heartbeat_interval() * 4
}

#[nervix_primitives::test(start_paused = true)]
async fn a_node_with_no_live_peer_leaves_gossip_without_waiting() {
    let fixture = build_test_service(true).await;
    let cluster = fixture.service.inner.cluster.clone();
    let advertisement = cluster.mark_local_terminating().await;
    let started = Instant::now();

    cluster
        .let_terminating_advertisement_propagate(advertisement, Duration::from_secs(60))
        .await;

    assert_eq!(started.elapsed(), Duration::ZERO);
    let TestService {
        service,
        registry,
        path,
    } = fixture;
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[nervix_primitives::test]
async fn units_that_could_have_moved_had_nowhere_to_move_once_no_other_node_can_take_them() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let local_node = service.inner.consensus.local_node_id().clone();
    let leader = node_named("node-2");
    let departed = BTreeSet::new();

    // The node is alone in its cluster, so once the leader has answered, no node can take its work.
    let failed = service
        .judge_unmoved_at_the_end(
            &local_node,
            ShutdownDrainAnswer::Failed {
                leader: leader.clone(),
                report: "failed ownership handoff gate".to_string(),
            },
            &departed,
        )
        .await;
    let could_have_moved = service
        .judge_unmoved_at_the_end(
            &local_node,
            ShutdownDrainAnswer::Unmoved {
                leader: leader.clone(),
                report: "timed out preparing the ownership-handoff gate".to_string(),
                unmoved: StoppingNodeUnmoved::CouldHaveMoved,
            },
            &departed,
        )
        .await;
    let completed = service
        .judge_unmoved_at_the_end(
            &local_node,
            ShutdownDrainAnswer::Completed {
                leader,
                report: "drained".to_string(),
            },
            &departed,
        )
        .await;

    for answer in [failed, could_have_moved] {
        let ShutdownDrainAnswer::Unmoved {
            unmoved: StoppingNodeUnmoved::NowhereToMove,
            ..
        } = answer
        else {
            panic!("a lone node's unmoved units had nowhere to move: {answer:?}");
        };
    }
    let ShutdownDrainAnswer::Completed { .. } = completed else {
        panic!("a completed drain stays completed: {completed:?}");
    };

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}
