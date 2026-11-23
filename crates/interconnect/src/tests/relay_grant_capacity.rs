//! Grant requests a receiver answers while its relay budget has no room for them.
//!
//! Layer: test harness.
//!
//! - **Owns.** Assertions that a grant request a full relay budget has no room for waits for that
//!   room rather than being refused, and that the delivery is granted once room returns.
//! - **Depends on.** The authenticated interconnect test fixture, the wire codec and the executor
//!   snapshot.
//! - **Must not know.** What a relay payload carries or what the runtime does with it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

#[nervix_primitives::test]
async fn receive_bound_refusal_creates_no_attempt_and_a_revised_claim_is_adopted() {
    let receive = PeerReceiveSettings::resolve(
        Some(ubyte::ByteUnit::Byte(8)),
        None,
        None,
        None,
        &nervix_execution::OperationLimits::default(),
    )
    .expect("positive bounded claim");
    let mut pair = connected_transports_with_options(TransportOptions {
        peer_receive: Some(receive),
        ..TransportOptions::default()
    })
    .await;
    let executor = Executor::default();
    let mut payload = RelayPayload {
        ack_timing: nervix_models::RemoteAckTiming::default(),
        decoded_bytes: 0,
        delivery: RelayDelivery {
            channel_incarnation: [91; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("domain"),
        relay: RelayName::parse("records").expect("relay"),
        unit: nervix_models::Unit::SINGLE,
        key: nervix_models::RemoteBranchKey::Unbranched,
        batch_ipc: executor
            .try_charge_owned(MemoryClass::Relay, vec![1; 16])
            .expect("body charge"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(RemoteAckRegistration {
            registrar: ClusterNodeIdentity::new(
                pair.node_a.clone(),
                ClusterNodeIncarnation::new(1),
            ),
            ack_id: 91,
        }),
    };
    let refusal = pair
        .transport_a
        .send(&pair.node_b, Envelope::RelayPayload(payload.clone()))
        .await
        .expect_err("the receiver cannot accept sixteen bytes");
    assert!(
        matches!(refusal.current_context(), TransportError::RelayBoundsRefused(bounds) if bounds.relay_batch_bytes.get() == 8)
    );
    assert!(matches!(
        pair.incoming_b.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    payload.batch_ipc = executor
        .try_charge_owned(MemoryClass::Relay, vec![1; 8])
        .expect("smaller body charge");
    let mut record = nervix_models::NodeCapabilityRecord {
        operational: Default::default(),
        identity: ClusterNodeIdentity::new(pair.node_b.clone(), ClusterNodeIncarnation::new(1)),
        capabilities: nervix_models::NodeCapabilities {
            runtime: nervix_models::DataPlaneSettings::default().capabilities(),
            shutdown: nervix_models::ShutdownSettings::default().capabilities(),
            interconnect: crate::TransportSettings::default().capabilities(),
            dns: nervix_arbitrary::Arbitrary::new(&[], nervix_arbitrary::Domain::Vocabulary)
                .node_capability_record()
                .capabilities
                .dns,
            node_database: nervix_arbitrary::Arbitrary::new(
                &[],
                nervix_arbitrary::Domain::Vocabulary,
            )
            .node_capability_record()
            .capabilities
            .node_database,
            consensus_database: nervix_arbitrary::Arbitrary::new(
                &[],
                nervix_arbitrary::Domain::Vocabulary,
            )
            .node_capability_record()
            .capabilities
            .consensus_database,
            consensus: nervix_arbitrary::Arbitrary::new(&[], nervix_arbitrary::Domain::Vocabulary)
                .node_capability_record()
                .capabilities
                .consensus,
            engines: nervix_arbitrary::Arbitrary::new(&[], nervix_arbitrary::Domain::Vocabulary)
                .node_engine_capabilities(),
            connector_host: nervix_arbitrary::Arbitrary::new(
                &[],
                nervix_arbitrary::Domain::Vocabulary,
            )
            .node_connector_host_capabilities(),
            membership: nervix_arbitrary::Arbitrary::new(&[], nervix_arbitrary::Domain::Vocabulary)
                .node_membership_capabilities(),
            listeners: nervix_arbitrary::Arbitrary::new(&[], nervix_arbitrary::Domain::Vocabulary)
                .listener_bounds(),
            cpu_count: None,
            memory_bytes: None,
            database_volume_bytes: 1 << 40,
            receive: receive.bounds(),
        },
    };
    record.capabilities.receive.relay_batch_bytes = std::num::NonZeroU64::new(4).expect("positive");
    pair.transport_a
        .publish_peer_capabilities(&BTreeMap::from([(pair.node_b.clone(), record.clone())]));
    let refusal = pair
        .transport_a
        .send(&pair.node_b, Envelope::RelayPayload(payload.clone()))
        .await
        .expect_err("the committed four-byte claim governs this sender");
    assert!(
        matches!(refusal.current_context(), TransportError::RelayBoundsRefused(bounds) if bounds.relay_batch_bytes.get() == 4)
    );
    record.identity = ClusterNodeIdentity::new(pair.node_b.clone(), ClusterNodeIncarnation::new(2));
    record.capabilities.receive = receive.bounds();
    pair.transport_a
        .publish_peer_capabilities(&BTreeMap::from([(pair.node_b.clone(), record)]));
    pair.transport_a
        .send(&pair.node_b, Envelope::RelayPayload(payload))
        .await
        .expect("the raised committed bound admits the same unused delivery sequence");
    let received = timeout(Duration::from_secs(2), pair.incoming_b.recv())
        .await
        .expect("bounded receive")
        .expect("open intake");
    assert!(
        matches!(received.envelope, Envelope::RelayPayload(payload) if payload.batch_ipc.len() == 8)
    );
    pair.transport_a.shutdown().await;
    pair.transport_b.shutdown().await;
}

/// A receiver whose relay budget has no room left at all holds a grant request back until room
/// returns, rather than refusing it for capacity, and grants the delivery then.
#[nervix_primitives::test]
async fn a_grant_asked_while_the_relay_budget_is_full_waits_and_is_granted_once_room_returns() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        executor_b,
        mut incoming_b,
        ..
    } = connected_transports().await;
    let payload = RelayPayload {
        ack_timing: nervix_models::RemoteAckTiming::default(),
        decoded_bytes: 0,
        delivery: RelayDelivery {
            channel_incarnation: [4; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        unit: nervix_models::Unit::SINGLE,
        key: nervix_models::RemoteBranchKey::Unbranched,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .expect("the test relay body should fit its budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(registration(1, &node_a)),
    };
    let relay_memory = executor_b.memory(MemoryClass::Relay);
    let room = relay_memory
        .capacity_bytes
        .checked_sub(relay_memory.reserved_bytes)
        .expect("a budget never reserves more than its capacity");
    let occupied = executor_b
        .try_reserve(MemoryClass::Relay, room)
        .expect("an idle receiver's relay budget takes all of its room");

    let sender = transport_a.clone();
    let sending = nervix_primitives::task::spawn(async move {
        sender.send(&node_b, Envelope::RelayPayload(payload)).await
    });
    nervix_primitives::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !sending.is_finished(),
        "a receiver without any relay room holds the grant back instead of refusing it"
    );

    drop(occupied);
    sending
        .await
        .expect("the sending task is joined")
        .expect("the delivery is granted once the receiver has room");
    let received = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the granted relay should enter the application queue")
        .expect("the application queue should remain open");
    assert!(matches!(received.envelope, Envelope::RelayPayload(_)));
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

/// A grant request larger than the relay room its receiver has left waits for the room it needs
/// together with its decoding, holding none of the class meanwhile, and the delivery is granted
/// once room returns. Every record's metadata and acknowledgement travel in the request, so a large
/// batch asks with a request well past the room the test leaves.
#[nervix_primitives::test]
async fn a_grant_request_larger_than_the_free_relay_room_waits_and_is_granted_once_room_returns() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        executor_b,
        mut incoming_b,
        ..
    } = connected_transports().await;
    let records = 4096;
    let ingested_at = Timestamp::from_unix_nanos(1_000);
    let payload = RelayPayload {
        ack_timing: nervix_models::RemoteAckTiming::default(),
        decoded_bytes: 0,
        delivery: RelayDelivery {
            channel_incarnation: [5; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        unit: nervix_models::Unit::SINGLE,
        key: nervix_models::RemoteBranchKey::Unbranched,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .expect("the test relay body should fit its budget"),
        metadata: vec![
            RemoteRuntimeRecordMetadata {
                ingested_at_low_watermark: ingested_at,
                ingested_at_high_watermark: ingested_at,
            };
            records
        ],
        acks: vec![None; records],
        admission: Some(registration(1, &node_a)),
    };
    // The request and its decoding need more than what is left.
    let left = 8 * 1024;
    let relay_memory = executor_b.memory(MemoryClass::Relay);
    let room = relay_memory
        .capacity_bytes
        .checked_sub(relay_memory.reserved_bytes)
        .expect("a budget never reserves more than its capacity");
    let taken = room
        .checked_sub(left)
        .expect("an idle receiver's relay budget has more room than the test leaves");
    let occupied = executor_b
        .try_reserve(MemoryClass::Relay, taken)
        .expect("an idle receiver's relay budget takes the rest of its room");

    let sender = transport_a.clone();
    let sending = nervix_primitives::task::spawn(async move {
        sender.send(&node_b, Envelope::RelayPayload(payload)).await
    });
    nervix_primitives::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !sending.is_finished(),
        "a receiver without room for the whole request holds the grant back instead of refusing it"
    );

    drop(occupied);
    sending
        .await
        .expect("the sending task is joined")
        .expect("the delivery is granted once the receiver has room");
    let received = timeout(Duration::from_secs(10), incoming_b.recv())
        .await
        .expect("the granted relay should enter the application queue")
        .expect("the application queue should remain open");
    let Envelope::RelayPayload(received) = received.envelope else {
        panic!("the granted delivery is the relay payload");
    };
    assert_eq!(
        received.metadata.len(),
        records,
        "the whole request arrived"
    );
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

/// A grant whose receiver's relay budget stays full for as long as its sender lets the receiver hold
/// it, half the delivery's dispatch timeout, is answered with the capacity refusal the delivery is
/// held back on, which both ends count, and the same delivery is granted once room returns.
#[nervix_primitives::test]
async fn a_grant_still_without_room_when_its_wait_ends_is_refused_for_capacity_and_granted_later() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        executor_b,
        mut incoming_b,
        ..
    } = connected_transports().await;
    let payload = RelayPayload {
        // The receiver may hold this delivery's grant for 200 milliseconds.
        ack_timing: nervix_models::RemoteAckTiming {
            remote_ack_dispatch_timeout: Duration::from_millis(400),
            ..nervix_models::RemoteAckTiming::default()
        },
        decoded_bytes: 0,
        delivery: RelayDelivery {
            channel_incarnation: [7; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        unit: nervix_models::Unit::SINGLE,
        key: nervix_models::RemoteBranchKey::Unbranched,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .expect("the test relay body should fit its budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(registration(1, &node_a)),
    };
    let relay_memory = executor_b.memory(MemoryClass::Relay);
    let room = relay_memory
        .capacity_bytes
        .checked_sub(relay_memory.reserved_bytes)
        .expect("a budget never reserves more than its capacity");
    let occupied = executor_b
        .try_reserve(MemoryClass::Relay, room)
        .expect("an idle receiver's relay budget takes all of its room");
    let refused = |snapshot: TransportSnapshot, direction: ConnectionDirection| {
        snapshot.counters.relay_grant_refusals[direction.index()][RelayGrantRefusal::Memory.index()]
    };

    let error = transport_a
        .send(&node_b, Envelope::RelayPayload(payload.clone()))
        .await
        .expect_err("a grant still without room when its wait ends is answered for capacity");
    assert!(
        matches!(
            error.current_context(),
            TransportError::RelayGrantRefused(RelayGrantRefusal::Memory)
        ),
        "a grant held past its wait is a capacity refusal, not {error:?}"
    );
    assert!(RelayGrantRefusal::Memory.is_capacity());
    assert_eq!(
        refused(transport_b.snapshot(), ConnectionDirection::Inbound),
        1
    );
    assert_eq!(
        refused(transport_a.snapshot(), ConnectionDirection::Outbound),
        1
    );

    drop(occupied);
    transport_a
        .send(&node_b, Envelope::RelayPayload(payload))
        .await
        .expect("the same delivery is granted once the receiver has room");
    let received = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the granted relay should enter the application queue")
        .expect("the application queue should remain open");
    assert!(matches!(received.envelope, Envelope::RelayPayload(_)));
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}
