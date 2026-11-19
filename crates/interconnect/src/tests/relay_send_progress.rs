//! What a relay delivery's sender learns about whether its sends reached the receiver.
//!
//! Layer: test harness.
//!
//! - **Owns.** Assertions that a relay send records the start of an exchange only once its
//!   delivery can have reached the receiver, whether the send succeeds, fails or is abandoned.
//! - **Depends on.** The authenticated interconnect test fixture and the public relay send.
//! - **Must not know.** What a relay payload carries or what the runtime does with it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

fn progress_payload(sender: &ClusterNodeName, sequence: u64) -> RelayPayload {
    RelayPayload {
        ack_timing: nervix_models::RemoteAckTiming::default(),
        decoded_bytes: 0,
        delivery: RelayDelivery {
            channel_incarnation: [93; 16],
            sequence,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("progress").expect("the fixture domain is valid"),
        relay: RelayName::parse("records").expect("the fixture relay is valid"),
        unit: nervix_models::Unit::SINGLE,
        key: nervix_models::RemoteBranchKey::Unbranched,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![7; 8])
            .expect("an eight-byte body fits the relay budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(registration(sequence, sender)),
    }
}

/// A send to a peer this node has no connection to waits for one and sends nothing meanwhile, so
/// abandoning it leaves the delivery with no attempt on the receiver.
#[nervix_primitives::test]
async fn an_abandoned_relay_send_that_found_no_connection_started_no_exchange() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        ..
    } = bound_transports_with_options(TransportOptions::default()).await;
    let progress = RelaySendProgress::default();
    let mut send =
        Box::pin(transport_a.send_relay(&node_b, progress_payload(&node_a, 0), &progress));

    assert!(
        send.as_mut().now_or_never().is_none(),
        "a send to a peer without a target waits for one"
    );
    drop(send);
    assert!(
        !progress.exchange_started(),
        "a send abandoned while it waited for a connection reached no receiver"
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

/// A send that fails while it waits for a connection reached no receiver either.
#[nervix_primitives::test]
async fn a_relay_send_that_failed_before_a_connection_started_no_exchange() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        ..
    } = bound_transports_with_options(TransportOptions {
        request_timeout: Duration::from_millis(50),
        ..TransportOptions::default()
    })
    .await;
    let progress = RelaySendProgress::default();

    let failure = transport_a
        .send_relay(&node_b, progress_payload(&node_a, 0), &progress)
        .await
        .expect_err("a peer without a target cannot be leased a connection");
    assert!(
        matches!(
            failure.current_context(),
            TransportError::RequestTimeout { .. }
        ),
        "the send ended waiting for a connection: {failure:#}"
    );
    assert!(
        !progress.exchange_started(),
        "a send that never leased a connection reached no receiver"
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

/// A send that reaches its receiver records the exchange.
#[nervix_primitives::test]
async fn a_relay_send_that_reached_its_receiver_records_the_exchange() {
    let mut pair = connected_transports().await;
    let progress = RelaySendProgress::default();

    pair.transport_a
        .send_relay(&pair.node_b, progress_payload(&pair.node_a, 0), &progress)
        .await
        .expect("the connected peer receives the relay body");
    let received = timeout(Duration::from_secs(10), pair.incoming_b.recv())
        .await
        .expect("the body arrives within the fixture deadline")
        .expect("the receiver remains connected");
    assert!(matches!(received.envelope, Envelope::RelayPayload(_)));
    assert!(
        progress.exchange_started(),
        "a delivery whose body reached its receiver started an exchange"
    );

    pair.transport_a.shutdown().await;
    pair.transport_b.shutdown().await;
}
