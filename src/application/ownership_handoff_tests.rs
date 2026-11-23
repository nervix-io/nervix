//! Tests of what a planned handoff's destination checks before it prepares or activates the moved
//! state.
//!
//! Layer: test harness.
//!
//! - **Owns.** Coverage of a destination's refusal to become a new placement destination while its
//!   own process incarnation is terminating, and of its refusal to activate a move prepared for an
//!   earlier run of itself.
//! - **Depends on.** The ownership handoff control plane and the server test fixture.
//! - **Must not know.** Connector implementations or runtime execution details.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::{
    ActivateOwnershipHandoffStateRequest as RemoteActivateOwnershipHandoffStateRequest,
    PrepareOwnershipHandoffStateRequest as RemotePrepareOwnershipHandoffStateRequest,
};
use nervix_models::{
    ClusterNodeIncarnation, CoordinationIdentity, DomainName, ModelKind, ModelName, NodeRef,
};
use nervix_recovery::Discarded as _;

use super::super::test_fixtures::{TestService, build_test_service};
use crate::runtime::OwnershipHandoffError;

#[nervix_primitives::test]
async fn a_terminating_destination_refuses_to_prepare_a_planned_handoff() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let local_node = service.inner.consensus.local_node_id().clone();
    let incarnation = service
        .inner
        .cluster
        .local_node_identity()
        .await
        .incarnation();
    let request = RemotePrepareOwnershipHandoffStateRequest {
        coordination: CoordinationIdentity::new(local_node.clone(), 1, 1),
        operation_id: "terminating-destination".to_string(),
        source: local_node.clone(),
        destination: local_node.clone(),
        source_incarnation: incarnation,
        destination_incarnation: incarnation,
        domain: DomainName::parse("default").assured("the test domain name is valid"),
        unit: nervix_models::Unit::SINGLE,
        entity: NodeRef::new(
            ModelKind::Junction,
            ModelName::parse("moved_route").assured("the test entity name is valid"),
        ),
        base_schedule_fingerprint: [1; 32],
        target_schedule_fingerprint: [2; 32],
        input: nervix_interconnect::OwnershipHandoffInput::Unit(Vec::new()),
    };

    // Before it terminates, the destination goes on to validate the request and refuses it for
    // what the request names: no committed schedule holds the entity.
    let Err(before) = service
        .prepare_ownership_handoff_state(request.clone())
        .await
    else {
        panic!("a preparation for an entity no schedule holds must fail");
    };
    assert!(
        !matches!(
            before.current_context(),
            OwnershipHandoffError::DestinationTerminating { .. }
        ),
        "a destination that is not terminating must not refuse as one: {before:?}"
    );

    service.inner.cluster.mark_local_terminating().await;
    let Err(refusal) = service.prepare_ownership_handoff_state(request).await else {
        panic!("a terminating destination must refuse the preparation");
    };
    let OwnershipHandoffError::DestinationTerminating { node } = refusal.current_context() else {
        panic!("a terminating destination must refuse as one: {refusal:?}");
    };
    assert_eq!(node, &local_node);

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

/// A preparation that a destination reloads after it restarted names the earlier run of that node,
/// and so does every activation request that matches it. The destination refuses such a request
/// before its runtime is asked to activate anything, so a reloaded preparation activates only
/// through its committed transition, when the node applies the committed schedule.
#[nervix_primitives::test]
async fn a_destination_refuses_to_activate_a_move_prepared_for_an_earlier_run_of_itself() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let local_node = service.inner.consensus.local_node_id().clone();
    let incarnation = service
        .inner
        .cluster
        .local_node_identity()
        .await
        .incarnation();
    let earlier_run = ClusterNodeIncarnation::new(
        incarnation
            .get()
            .checked_sub(1)
            .assured("a run's incarnation counts the nanoseconds since the epoch at its start"),
    );
    let request = RemoteActivateOwnershipHandoffStateRequest {
        coordination: CoordinationIdentity::new(local_node.clone(), 1, 1),
        operation_id: "earlier-run".to_string(),
        source: local_node.clone(),
        destination: local_node.clone(),
        source_incarnation: incarnation,
        destination_incarnation: earlier_run,
        domain: DomainName::parse("default").assured("the test domain name is valid"),
        unit: nervix_models::Unit::SINGLE,
        entity: NodeRef::new(
            ModelKind::Junction,
            ModelName::parse("moved_route").assured("the test entity name is valid"),
        ),
        base_schedule_fingerprint: [1; 32],
        target_schedule_fingerprint: [2; 32],
        activation_budget: Duration::from_secs(5),
    };

    let Err(refusal) = service
        .activate_local_ownership_handoff_state(&request)
        .await
    else {
        panic!("an activation for an earlier run of the destination must be refused");
    };
    // The destination's incarnation is the last participant check before the activation waits for
    // the committed target schedule. No schedule holds the entity here, so a request past that
    // check would fail as a schedule validation instead.
    let OwnershipHandoffError::Participant(reason) = refusal.current_context() else {
        panic!("an earlier run of the destination must be refused as a participant: {refusal:?}");
    };
    assert!(
        reason.contains(&format!(
            "destination node '{local_node}' changed process incarnation"
        )),
        "the refusal must name the destination's changed incarnation: {reason}"
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}
