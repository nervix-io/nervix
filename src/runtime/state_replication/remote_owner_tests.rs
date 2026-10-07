//! Layer: test harness.
//! Owns: replica checkpoint and catalog response deadlines over a real loopback transport.
//! May depend on: the runtime's remote owner, typed interconnect requests and runtime test fixtures.
//! Must not know: production scheduling or control-plane orchestration.

use nervix_interconnect::{
    BranchCheckpointListing, BranchCheckpointListingResponse, StateSnapshotEnvelope,
    StateSyncRequest, StateSyncResponse,
};
use nervix_models::NodeEndpoint;
use nervix_primitives::time::sleep;

use super::*;
use crate::runtime::test_fixtures::{
    attach_loopback_cluster, domain, named, publish_state_identity,
};

/// The real owner answers after the replica's polling cadence, within the operation's deadline.
const OWNER_ANSWER_DELAY: Duration = Duration::from_millis(1200);

async fn remote_owner(answer_delay: Duration) -> (RemoteStateOwner, RuntimeStatePlacement) {
    let runtime = Runtime::default();
    let node = named::<ClusterNodeName>("node-1");
    attach_loopback_cluster(&runtime, &node).await;
    let dispatcher = runtime
        .inner
        .remote_dispatcher
        .load_full()
        .expect("joining the loopback cluster attaches its dispatcher");
    let transport = &dispatcher.interconnect;
    transport
        .register_outbound_target(node.clone(), NodeEndpoint::from(transport.local_addr()))
        .expect("the loopback owner's target fits its peer quota");
    transport
        .register_handler::<StateSyncRequest, _, _>(move |_context, _request| async move {
            sleep(answer_delay).await;
            StateSyncResponse {
                result: Ok(Some(StateSnapshotEnvelope {
                    lsm: 7,
                    payload: vec![1, 2, 3],
                })),
            }
        })
        .expect("the checkpoint handler registers once");
    transport
        .register_handler::<BranchCheckpointListingRequest, _, _>(
            move |_context, _request| async move {
                sleep(answer_delay).await;
                BranchCheckpointListingResponse {
                    result: Ok(BranchCheckpointListing::Absent),
                }
            },
        )
        .expect("the catalog handler registers once");
    let domain = domain("default");
    let identifier = named::<ModelName>("dedup_orders");
    publish_state_identity(
        &runtime,
        &domain,
        ModelKind::Deduplicator,
        identifier.clone(),
    );
    let placement = runtime
        .state_placement(
            &domain,
            RuntimeStateKind::BranchLru,
            ModelKind::Deduplicator,
            identifier,
            None,
        )
        .expect("the published identity places the branch lifecycle");
    let owner = RemoteStateOwner::new(runtime, node);
    (owner, placement)
}

#[nervix_primitives::test]
async fn checkpoint_answer_after_the_poll_interval_uses_its_operation_deadline() {
    let (owner, placement) = remote_owner(OWNER_ANSWER_DELAY).await;
    let checkpoint = owner
        .checkpoint_after(&placement, Some(6))
        .await
        .expect("a checkpoint answer inside its operation deadline is accepted")
        .expect("the owner holds a newer checkpoint");
    assert_eq!(checkpoint.lsm, 7);
    assert_eq!(checkpoint.payload, [1, 2, 3]);
    owner
        .runtime
        .inner
        .remote_dispatcher
        .load_full()
        .expect("attached dispatcher")
        .interconnect
        .shutdown()
        .await;
}

#[nervix_primitives::test]
async fn catalog_answer_after_the_poll_interval_uses_its_operation_deadline() {
    let (owner, placement) = remote_owner(OWNER_ANSWER_DELAY).await;
    let listing = owner
        .checkpoint_listing(&placement, None)
        .await
        .expect("a catalog answer inside its operation deadline is accepted");
    assert_eq!(listing, OwnerCheckpointListing::Absent);
    owner
        .runtime
        .inner
        .remote_dispatcher
        .load_full()
        .expect("attached dispatcher")
        .interconnect
        .shutdown()
        .await;
}

#[nervix_primitives::test]
async fn checkpoint_answer_beyond_its_operation_deadline_retains_the_typed_timeout() {
    let (owner, placement) = remote_owner(Duration::from_secs(6)).await;
    let failure = owner
        .checkpoint_after(&placement, Some(6))
        .await
        .expect_err("an owner that exceeds its operation deadline is refused");
    assert!(matches!(
        failure.current_context(),
        StateReplicationError::Request { .. }
    ));
    assert!(matches!(
        failure.downcast_ref::<nervix_interconnect::RequestError>(),
        Some(nervix_interconnect::RequestError::Timeout { request: "state_sync", timeout, .. })
            if *timeout == StateSyncRequest::TIMEOUT
    ));
    owner
        .runtime
        .inner
        .remote_dispatcher
        .load_full()
        .expect("attached dispatcher")
        .interconnect
        .shutdown()
        .await;
}

#[nervix_primitives::test]
async fn catalog_answer_beyond_its_operation_deadline_retains_the_typed_timeout() {
    let (owner, placement) = remote_owner(Duration::from_secs(6)).await;
    let failure = owner
        .checkpoint_listing(&placement, None)
        .await
        .expect_err("an owner that exceeds its catalog deadline is refused");
    assert!(matches!(
        failure.current_context(),
        StateReplicationError::Request { .. }
    ));
    assert!(matches!(
        failure.downcast_ref::<nervix_interconnect::RequestError>(),
        Some(nervix_interconnect::RequestError::Timeout { request: "branch_checkpoint_listing", timeout, .. })
            if *timeout == BranchCheckpointListingRequest::TIMEOUT
    ));
    owner
        .runtime
        .inner
        .remote_dispatcher
        .load_full()
        .expect("attached dispatcher")
        .interconnect
        .shutdown()
        .await;
}
