//! Model checks for relay-owner fan-out while a consumer schedule changes.
//!
//! Layer: test harness.
//! - **Owns.** The owner-side ACK and consumer-delivery invariant across a relay dispatch fence.
//! - **Depends on.** The production relay boundary, dispatch gate, ACK tree, and Shuttle runner.
//! - **Must not know.** NSPL, placement policy, or connector internals.

use std::time::Duration;

use nervix_model_harness::shuttle::check_random;
use nervix_primitives::{sync::oneshot, time::Instant};

use super::*;
use crate::runtime_ack::AckOutcome;

#[test]
fn shuttle_owner_fanout_fails_its_ack_while_an_attached_consumer_moves() {
    check_random(
        || {
            shuttle::future::block_on(async {
                let owner_domain = domain("default");
                let relay = named("orders");
                let services = test_relay_boundary_services();
                let mut sibling = services.add_local_runtime_consumer(AckMode::Attached);
                let mut moving = services.add_local_runtime_consumer(AckMode::Attached);
                let gate = services.fanout.dispatch_gate();
                let (root, completion) = AckSet::root();
                let mut batch = quiesce_test_batch();
                batch.acks = vec![root];
                let (owner_done, owner_is_done) = oneshot::channel();
                let owner_services = services.clone();
                let owner = nervix_primitives::task::spawn(async move {
                    let result = owner_services
                        .fanout_owner_batch(
                            &owner_domain,
                            &relay,
                            &batch,
                            &ConfiguredFaultInjection::default(),
                        )
                        .await;
                    owner_done
                        .send(())
                        .assured("the moving consumer waits until fan-out returns");
                    result
                });

                let moving_services = services.clone();
                let move_consumer = nervix_primitives::task::spawn(async move {
                    let mut lease = RelayDispatchGateLease::engage(
                        gate,
                        Instant::now() + Duration::from_secs(86_400),
                        "move an attached consumer",
                    );
                    assert!(lease.wait_quiescent().await);
                    let received = match moving.try_recv() {
                        RelayTryRecv::Batch(delivered) => {
                            delivered.ack_success();
                            true
                        }
                        RelayTryRecv::Empty | RelayTryRecv::Closed => false,
                    };
                    drop(moving);
                    moving_services.remove_local_runtime_consumer(AckMode::Attached);
                    owner_is_done
                        .await
                        .assured("the owner finishes before the schedule gate reopens");
                    drop(lease);
                    received
                });

                let owner_result = owner.await.assured("owner task completes");
                let moving_received = move_consumer.await.assured("consumer move completes");
                let sibling_received = match sibling.try_recv() {
                    RelayTryRecv::Batch(delivered) => {
                        delivered.ack_success();
                        true
                    }
                    RelayTryRecv::Empty | RelayTryRecv::Closed => false,
                };
                let outcome = completion.wait().await;
                if outcome == AckOutcome::Ack {
                    assert!(
                        moving_received && sibling_received,
                        "a successful source ACK requires delivery to both attached consumers"
                    );
                    assert!(owner_result.is_ok());
                } else {
                    assert!(matches!(outcome, AckOutcome::NoAck(_)));
                }

                let (retry_root, retry_completion) = AckSet::root();
                let mut retry = quiesce_test_batch();
                retry.acks = vec![retry_root];
                services
                    .fanout_owner_batch(
                        &domain("default"),
                        &named("orders"),
                        &retry,
                        &ConfiguredFaultInjection::default(),
                    )
                    .await
                    .assured("a released gate admits a retry to its live sibling");
                sibling
                    .recv()
                    .await
                    .expect("the sibling receives the retry")
                    .ack_success();
                assert_eq!(retry_completion.wait().await, AckOutcome::Ack);
            });
        },
        1_000,
    );
}
