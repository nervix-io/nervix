//! Tests of the drain a stopping node asks the leader for.
//!
//! Layer: test harness.
//!
//! - **Owns.** Coverage of what the leader does for the node that asks, of how the asking node
//!   reads an answer that never arrives, and of how long and when the asking node waits for the
//!   release of its drain cordon.
//! - **Depends on.** The scheduling control plane, the shutdown deadline and the server test
//!   fixture.
//! - **Must not know.** Connector implementations or runtime execution details.

use std::{collections::BTreeSet, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_consensus::ConsensusError;
use nervix_interconnect::{RequestError, StoppingNodeDrainAction, StoppingNodeDrainResponse};
use nervix_models::ClusterNodeName;
use nervix_primitives::time::Instant;
use nervix_recovery::Discarded as _;

#[cfg(feature = "testing")]
use super::super::test_fixtures::build_test_service_with_fault_injection;
use super::{
    super::{
        shutdown::{ShutdownCoordinator, ShutdownDeadline, ShutdownRequestOutcome},
        test_fixtures::{TestService, build_test_service, node_named},
    },
    SHUTDOWN_CORDON_RELEASE_GRACE, SessionServiceImpl, ShutdownDrainAnswer, ShutdownDrainBudget,
    ShutdownDrainCordon, ShutdownOwnershipMove, ShutdownPhaseOutcome, ShutdownReleaseQuorum,
};

/// A shutdown deadline `timeout` from now, from the stop request a fresh coordinator accepts.
fn shutdown_deadline_after(timeout: Duration) -> ShutdownDeadline {
    let coordinator = ShutdownCoordinator::new(timeout);
    let ShutdownRequestOutcome::Accepted(request) = coordinator.request_stop() else {
        panic!("a fresh coordinator accepts its first stop request");
    };
    request.deadline()
}

/// The three ways `leader` can answer a stopping node, in the order completed, failed, unanswered.
fn every_answer(leader: &ClusterNodeName) -> [ShutdownDrainAnswer; 3] {
    let completed = ShutdownDrainAnswer::Completed {
        leader: leader.clone(),
        report: "drained".to_string(),
    };
    let failed = ShutdownDrainAnswer::Failed {
        leader: leader.clone(),
        report: "a model or schedule change is already in progress".to_string(),
    };
    let unanswered = ShutdownDrainAnswer::Unanswered {
        leader: leader.clone(),
        error: Report::new(RequestError::TargetLeft {
            node: leader.clone(),
            request: "stopping_node_drain",
        }),
    };
    [completed, failed, unanswered]
}

#[test]
fn only_a_completed_answer_completes_the_move_of_the_stopping_nodes_work() {
    let local_node = node_named("node-2");
    let outcomes =
        every_answer(&node_named("node-1")).map(|answer| answer.into_drain_outcome(&local_node));
    assert_eq!(
        outcomes,
        [
            ShutdownPhaseOutcome::Completed,
            ShutdownPhaseOutcome::Abandoned,
            ShutdownPhaseOutcome::Abandoned,
        ]
    );
}

#[test]
fn only_a_completed_answer_completes_the_release_of_the_drain_cordon() {
    let local_node = node_named("node-2");
    let outcomes = every_answer(&node_named("node-1"))
        .map(|answer| answer.into_cordon_release_outcome(&local_node));
    assert_eq!(
        outcomes,
        [
            ShutdownPhaseOutcome::Completed,
            ShutdownPhaseOutcome::Abandoned,
            ShutdownPhaseOutcome::Abandoned,
        ]
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
        .act_for_stopping_node(local_node.clone(), StoppingNodeDrainAction::Drain)
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
        .act_for_stopping_node(stranger.clone(), StoppingNodeDrainAction::Drain)
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
        .ask_shutdown_drain_leader(
            &local_node,
            local_node.clone(),
            StoppingNodeDrainAction::ReleaseCordon,
        )
        .await;
    let ShutdownDrainAnswer::Completed { leader, report } = answer else {
        panic!("the leading node did not release its own cordon");
    };
    assert_eq!(leader, local_node);
    assert_eq!(report, format!("uncordoned node '{local_node}'"));

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[nervix_primitives::test]
async fn a_drain_request_the_transport_cannot_carry_is_unanswered() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let local_node = service.inner.consensus.local_node_id().clone();
    let leader = node_named("node-elsewhere");
    service.inner.interconnect.shutdown().await;

    let answer = service
        .ask_shutdown_drain_leader(&local_node, leader.clone(), StoppingNodeDrainAction::Drain)
        .await;
    let ShutdownDrainAnswer::Unanswered {
        leader: asked,
        error,
    } = answer
    else {
        panic!("a request the stopped transport refused was answered");
    };
    assert_eq!(asked, leader);
    assert!(
        matches!(
            error.current_context(),
            nervix_interconnect::RequestError::ShuttingDown { node, .. } if node == &leader
        ),
        "{error:?}"
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

/// The node names `raw`, as one set.
fn nodes_named(raw: &[&str]) -> BTreeSet<ClusterNodeName> {
    raw.iter().map(|name| node_named(name)).collect()
}

#[nervix_primitives::test]
async fn the_cordon_release_waits_for_what_remains_of_the_drain_timeout() {
    let drain_timeout = Duration::from_secs(30);
    let started = Instant::now();
    let budget = ShutdownDrainBudget::start(
        drain_timeout,
        shutdown_deadline_after(Duration::from_secs(50)),
    );
    let bound = budget.cordon_release_bound(ShutdownReleaseQuorum::Staying);
    let elapsed = started.elapsed();

    let least = drain_timeout.checked_sub(elapsed).unwrap_or(Duration::ZERO);
    assert!(bound <= drain_timeout, "{bound:?}");
    assert!(bound >= least, "{bound:?} is less than {least:?}");
}

#[nervix_primitives::test]
async fn the_cordon_release_keeps_its_grace_when_the_move_used_the_drain_timeout() {
    let budget = ShutdownDrainBudget::start(
        Duration::ZERO,
        shutdown_deadline_after(Duration::from_secs(50)),
    );

    assert_eq!(budget.remaining(), Duration::ZERO);
    assert_eq!(
        budget.cordon_release_bound(ShutdownReleaseQuorum::Staying),
        SHUTDOWN_CORDON_RELEASE_GRACE
    );
}

#[nervix_primitives::test]
async fn a_cordon_release_only_stopping_voters_could_commit_keeps_only_its_grace() {
    let budget = ShutdownDrainBudget::start(
        Duration::from_secs(30),
        shutdown_deadline_after(Duration::from_secs(50)),
    );

    assert_eq!(
        budget.cordon_release_bound(ShutdownReleaseQuorum::Leaving),
        SHUTDOWN_CORDON_RELEASE_GRACE
    );
}

#[nervix_primitives::test]
async fn the_cordon_release_never_waits_past_the_shutdown_deadline() {
    let shutdown_timeout = Duration::from_millis(500);
    let budget = ShutdownDrainBudget::start(
        Duration::from_secs(30),
        shutdown_deadline_after(shutdown_timeout),
    );

    assert!(budget.cordon_release_bound(ShutdownReleaseQuorum::Staying) <= shutdown_timeout);
    assert!(budget.cordon_release_bound(ShutdownReleaseQuorum::Leaving) <= shutdown_timeout);
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
fn a_move_reports_what_the_drain_it_requested_came_to() {
    let outcomes = [
        ShutdownOwnershipMove::NoReplacement,
        ShutdownOwnershipMove::NotRequested,
        ShutdownOwnershipMove::Requested(ShutdownPhaseOutcome::Completed),
        ShutdownOwnershipMove::Requested(ShutdownPhaseOutcome::Abandoned),
    ]
    .map(|ownership_move| ownership_move.outcome());
    assert_eq!(
        outcomes,
        [
            ShutdownPhaseOutcome::Completed,
            ShutdownPhaseOutcome::Abandoned,
            ShutdownPhaseOutcome::Completed,
            ShutdownPhaseOutcome::Abandoned,
        ]
    );
}

#[test]
fn only_a_requested_drain_of_a_node_no_operator_cordoned_leaves_a_cordon_to_release() {
    let moves = [
        ShutdownOwnershipMove::NoReplacement,
        ShutdownOwnershipMove::NotRequested,
        ShutdownOwnershipMove::Requested(ShutdownPhaseOutcome::Completed),
        ShutdownOwnershipMove::Requested(ShutdownPhaseOutcome::Abandoned),
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
        ]
    );
    assert_eq!(
        with_operator_cordon,
        [
            ShutdownDrainCordon::NeverSet,
            ShutdownDrainCordon::NeverSet,
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
            leader_id: Some(node_named("node-1")),
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
    let budget = ShutdownDrainBudget::start(
        Duration::from_secs(30),
        shutdown_deadline_after(Duration::from_secs(50)),
    );
    service
        .inner
        .consensus
        .set_node_cordoned(local_node.clone(), true)
        .await
        .assured("the single-node test leader commits its own cordon");

    let operator = service
        .settle_shutdown_drain_cordon(&local_node, ShutdownDrainCordon::Operator, &budget)
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
        .settle_shutdown_drain_cordon(&local_node, ShutdownDrainCordon::SetByDrain, &budget)
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
        .settle_shutdown_drain_cordon(&local_node, ShutdownDrainCordon::NeverSet, &budget)
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
    let budget = ShutdownDrainBudget::start(
        Duration::ZERO,
        shutdown_deadline_after(Duration::from_millis(200)),
    );

    let outcome = service
        .settle_shutdown_drain_cordon(&local_node, ShutdownDrainCordon::SetByDrain, &budget)
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
