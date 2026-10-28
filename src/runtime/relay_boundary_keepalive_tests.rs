//! Relay boundary tests for the acknowledgement liveness of a delivery to another node.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests that a delivery to another node reports its records alive while it
//!   waits, before the receiving node admits it and takes over the reports.
//! - **Depends on.** The runtime relay boundary and its test fixtures.
//! - **Must not know.** Production control-plane orchestration or external connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use nervix_interconnect::RelayPayloadKind;
use nervix_models::{AckMode, ClusterNodeName, ParseAsType, RelayName};
use nervix_primitives::{sync::Arc, time::timeout};

use super::*;
use crate::{
    runtime_ack::{AckOutcome, AckProgress, AckSet},
    runtime_schema::{RuntimeValue, test_runtime_row},
};

/// A delivery to the relay's owner holds its records until the owner admits them, and the owner
/// reports them only from then on, so the delivering node reports them alive while the delivery
/// waits. Here it waits at the relay's dispatch gate, as it does while the relay's owner moves.
#[nervix_primitives::test(start_paused = true)]
async fn a_delivery_to_the_relay_owner_reports_its_records_alive_while_it_waits() {
    let services = test_relay_boundary_services();
    services.replace_owner_node(Some(named("node-2")));
    let lease = RelayDispatchGateLease::engage(
        services.fanout.dispatch_gate(),
        Instant::now() + Duration::from_secs(60),
        "relay owner is moving",
    );
    let (acks, mut completion) = AckSet::root();
    let mut batch = quiesce_test_batch();
    batch.acks = vec![acks];
    let domain = domain("default");
    let relay = named::<RelayName>("orders");
    let delivery = services.dispatch_to_owner(&domain, &relay, &batch);
    tokio::pin!(delivery);

    for interval in 1..=3 {
        let progress = nervix_primitives::select! {
            biased;
            _ = &mut delivery => panic!("the delivery waits at the engaged dispatch gate"),
            progress = timeout(REMOTE_ACK_ALIVE_INTERVAL * 2, completion.wait_for_progress()) => {
                progress
            }
        };
        let Ok(progress) = progress else {
            panic!("the delivery held through interval {interval} reports its record alive");
        };
        assert_eq!(progress, AckProgress::Alive);
    }

    drop(lease);
    delivery
        .await
        .expect_err("the isolated test has no remote dispatcher");
    assert!(matches!(completion.wait().await, AckOutcome::NoAck(_)));
}

/// A delivery to an attached consumer on another node holds its records until that node admits
/// them, and that node reports them only from then on, so the relay's owner reports them alive
/// while the delivery waits. Here it waits for the consumer's channel, which an earlier delivery
/// still holds.
#[nervix_primitives::test(start_paused = true)]
async fn a_delivery_to_a_remote_consumer_reports_its_records_alive_while_it_waits() {
    let runtime = Runtime::default();
    attach_loopback_cluster(&runtime, &named("node-1")).await;
    let dispatcher = runtime
        .inner
        .remote_dispatcher
        .load_full()
        .assured("the loopback cluster installs its dispatcher");
    dispatcher.interconnect.shutdown().await;
    let consumer = named::<ClusterNodeName>("node-2");
    let relay = named::<RelayName>("orders");
    let services = RelayBoundaryServices::new(
        RelayBoundaryFanout::direct_with_capacity(NonZeroUsize::MIN),
        0,
        0,
        vec![RemoteRuntimeConsumer {
            node_id: consumer.clone(),
            relay: relay.clone(),
            mode: AckMode::Attached,
        }],
        Some(dispatcher),
        Arc::new(BranchPresence::new()),
        nervix_primitives::sync::Arc::new(crate::runtime_ack::AckRootTracker::default()),
    );
    let (acks, mut completion) = AckSet::root();
    let batch = RelayRecordBatch::single(
        test_schema(&[("value", ParseAsType::I64)]),
        None,
        test_runtime_row([("value".into(), RuntimeValue::I64(1))]),
        acks,
    )
    .assured("the fixture batch carries one valid I64 row");
    let channel = services.outbound_slot(&consumer, &relay, RelayPayloadKind::Routed, &batch.key);
    let earlier_delivery = channel
        .lock_for_delivery()
        .await
        .expect("the open channel serves an earlier delivery");
    let domain = domain("default");
    let delivery = services.dispatch_remote_runtime_consumers(&domain, &batch);
    tokio::pin!(delivery);

    for interval in 1..=3 {
        let progress = nervix_primitives::select! {
            biased;
            _ = &mut delivery => panic!("the delivery waits for the consumer's channel"),
            progress = timeout(REMOTE_ACK_ALIVE_INTERVAL * 2, completion.wait_for_progress()) => {
                progress
            }
        };
        let Ok(progress) = progress else {
            panic!("the delivery held through interval {interval} reports its record alive");
        };
        assert_eq!(progress, AckProgress::Alive);
    }

    drop(earlier_delivery);
    delivery
        .await
        .expect_err("the stopped transport fails the attached delivery");
    assert!(matches!(completion.wait().await, AckOutcome::NoAck(_)));
}
