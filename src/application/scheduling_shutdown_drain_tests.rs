//! Tests of the drain a stopping node asks the leader for.
//!
//! Layer: test harness.
//!
//! - **Owns.** Coverage of what the leader does for the node that asks, and of how the asking node
//!   reads an answer that never arrives.
//! - **Depends on.** The scheduling control plane and the server test fixture.
//! - **Must not know.** Connector implementations or runtime execution details.

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_interconnect::{RequestError, StoppingNodeDrainAction, StoppingNodeDrainResponse};
use nervix_models::ClusterNodeName;
use nervix_recovery::Discarded as _;

use super::{
    super::test_fixtures::{TestService, build_test_service, node_named},
    ShutdownDrainAnswer, ShutdownPhaseOutcome,
};

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
