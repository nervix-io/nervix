//! Tests of what a planned handoff's destination checks before it prepares the moved state.
//!
//! Layer: test harness.
//!
//! - **Owns.** Coverage of a destination's refusal to become a new placement destination while its
//!   own process incarnation is terminating.
//! - **Depends on.** The ownership handoff control plane and the server test fixture.
//! - **Must not know.** Connector implementations or runtime execution details.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_interconnect::PrepareOwnershipHandoffStateRequest as RemotePrepareOwnershipHandoffStateRequest;
use nervix_models::{CoordinationIdentity, DomainName, ModelKind, ModelName, NodeRef};
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
        unit: nervix_models::Unit::Single,
        entity: NodeRef::new(
            ModelKind::Junction,
            ModelName::parse("moved_route").assured("the test entity name is valid"),
        ),
        base_schedule_fingerprint: [1; 32],
        target_schedule_fingerprint: [2; 32],
        checkpoints: Vec::new(),
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
