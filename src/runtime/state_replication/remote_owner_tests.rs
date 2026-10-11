//! Layer: test harness.
//! Owns: replica checkpoint and catalog response deadlines over a real loopback transport.
//! May depend on: the runtime's remote owner, typed interconnect requests and runtime test fixtures.
//! Must not know: production scheduling or control-plane orchestration.

use nervix_interconnect::{
    BranchCheckpointListing, BranchCheckpointListingResponse, FetchStateCheckpoint,
    StateCheckpointRead, StateSnapshotEnvelope, StateSyncRequest, StateSyncResponse,
    StreamHandlerError, StreamingResponse,
};
use nervix_models::NodeEndpoint;
use nervix_primitives::time::sleep;

use super::*;
use crate::runtime::test_fixtures::{
    attach_loopback_cluster, domain, named, publish_state_identity,
};

/// The real owner answers after the replica's polling cadence, within the operation's deadline.
const OWNER_ANSWER_DELAY: Duration = Duration::from_millis(1200);

#[derive(Clone, Copy)]
enum CheckpointStreamCase {
    Complete,
    WrongDigest,
    WrongOpenedLength,
    Truncated,
    ExcessBytes,
    UnboundedLength,
}

async fn remote_owner(
    answer_delay: Duration,
    stream_case: CheckpointStreamCase,
) -> (RemoteStateOwner, RuntimeStatePlacement) {
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
                    length: if matches!(stream_case, CheckpointStreamCase::UnboundedLength) {
                        u64::MAX
                    } else {
                        3
                    },
                    digest: if matches!(stream_case, CheckpointStreamCase::WrongDigest) {
                        [0; 32]
                    } else {
                        *blake3::hash(&[1, 2, 3]).as_bytes()
                    },
                })),
            }
        })
        .expect("the checkpoint handler registers once");
    let executor = runtime.inner.executor.clone();
    transport
        .register_stream_handler::<FetchStateCheckpoint, _, _>(move |_context, request| {
            let executor = executor.clone();
            async move {
                assert_eq!(request.lsm, 7);
                assert_eq!(request.read, StateCheckpointRead::Published);
                let payload = match stream_case {
                    CheckpointStreamCase::WrongOpenedLength | CheckpointStreamCase::Truncated => {
                        vec![1, 2]
                    }
                    CheckpointStreamCase::ExcessBytes => vec![1, 2, 3, 4],
                    _ => vec![1, 2, 3],
                };
                let chunk = executor
                    .charge_owned(nervix_execution::MemoryClass::Bulk, payload)
                    .await
                    .map_err(StreamHandlerError::with_cause)?;
                Ok(StreamingResponse::new(
                    if matches!(stream_case, CheckpointStreamCase::WrongOpenedLength) {
                        2
                    } else {
                        3
                    },
                    futures_util::stream::once(async move { Ok(chunk) }),
                ))
            }
        })
        .expect("the checkpoint stream handler registers once");
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
    let (owner, placement) = remote_owner(OWNER_ANSWER_DELAY, CheckpointStreamCase::Complete).await;
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
async fn checkpoint_stream_with_a_different_digest_never_becomes_installable() {
    let (owner, placement) = remote_owner(Duration::ZERO, CheckpointStreamCase::WrongDigest).await;
    let failure = owner
        .checkpoint_after(&placement, Some(6))
        .await
        .expect_err("a complete stream with the wrong digest cannot be installed");
    assert!(matches!(
        failure.current_context(),
        StateReplicationError::Request { .. }
    ));
    assert!(format!("{failure:?}").contains("digest verification"));
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
async fn checkpoint_stream_requires_the_complete_declared_byte_count() {
    for (stream_case, cause) in [
        (
            CheckpointStreamCase::WrongOpenedLength,
            Some("opened a 2-byte stream"),
        ),
        (CheckpointStreamCase::Truncated, None),
        (CheckpointStreamCase::ExcessBytes, None),
        (
            CheckpointStreamCase::UnboundedLength,
            Some("above the 268435456-byte transfer bound"),
        ),
    ] {
        let (owner, placement) = remote_owner(Duration::ZERO, stream_case).await;
        let failure = owner
            .checkpoint_after(&placement, Some(6))
            .await
            .expect_err("a mismatched stream cannot become an installable checkpoint");
        assert!(matches!(
            failure.current_context(),
            StateReplicationError::Request { .. }
        ));
        if let Some(cause) = cause {
            assert!(format!("{failure:?}").contains(cause), "{failure:?}");
        } else {
            // The stream transport rejects a short or overlong producer before handing its
            // invalid terminal body to the checkpoint decoder.
            assert!(
                format!("{failure:?}").contains("fetch_state_checkpoint"),
                "{failure:?}"
            );
        }
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
}

#[nervix_primitives::test]
async fn catalog_answer_after_the_poll_interval_uses_its_operation_deadline() {
    let (owner, placement) = remote_owner(OWNER_ANSWER_DELAY, CheckpointStreamCase::Complete).await;
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
    let (owner, placement) =
        remote_owner(Duration::from_secs(6), CheckpointStreamCase::Complete).await;
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
    let (owner, placement) =
        remote_owner(Duration::from_secs(6), CheckpointStreamCase::Complete).await;
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
