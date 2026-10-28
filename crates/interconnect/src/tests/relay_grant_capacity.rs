//! Grant requests a receiver answers while its relay budget has no room to read them.
//!
//! Layer: test harness.
//!
//! - **Owns.** Assertions that a grant request is read and decoded only into relay room its
//!   receiver has free, that a request without that room is refused for capacity at once and
//!   keeps none of it, and that the same delivery is granted once room returns.
//! - **Depends on.** The authenticated interconnect test fixture, the wire codec and the executor
//!   snapshot.
//! - **Must not know.** What a relay payload carries or what the runtime does with it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

/// A receiver whose relay budget has no room left at all still answers a grant request with the
/// typed capacity refusal, rather than waiting on that budget to read the request, and grants the
/// same delivery once the room returns.
#[nervix_primitives::test]
async fn a_grant_asked_while_the_relay_budget_is_full_is_refused_for_capacity() {
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
        delivery: RelayDelivery {
            channel_incarnation: [4; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        key: None,
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

    let error = transport_a
        .send(&node_b, Envelope::RelayPayload(payload.clone()))
        .await
        .expect_err("a receiver without any relay room refuses the grant");
    assert!(
        matches!(
            error.current_context(),
            TransportError::RelayGrantRefused(RelayGrantRefusal::Memory)
        ),
        "a full relay budget is a capacity refusal, not {error:?}"
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

/// A grant request larger than the relay room its receiver has left is refused for capacity while
/// it is read, rather than waiting for more room, and keeps none of the room it was read into. The
/// same delivery is granted once room returns. Every record's metadata and acknowledgement travel in
/// the request, so a large batch asks with a request well past the reader's first charge.
#[nervix_primitives::test]
async fn a_grant_request_larger_than_the_free_relay_room_is_refused_for_capacity() {
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
        delivery: RelayDelivery {
            channel_incarnation: [5; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        key: None,
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
    // The reader's first charge fits in what is left, so the request is refused while it grows.
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
    let reserved = executor_b.memory(MemoryClass::Relay).reserved_bytes;

    let error = transport_a
        .send(&node_b, Envelope::RelayPayload(payload.clone()))
        .await
        .expect_err("a receiver without room for the whole request refuses the grant");
    assert!(
        matches!(
            error.current_context(),
            TransportError::RelayGrantRefused(RelayGrantRefusal::Memory)
        ),
        "a request larger than the free relay room is a capacity refusal, not {error:?}"
    );
    assert_eq!(
        executor_b.memory(MemoryClass::Relay).reserved_bytes,
        reserved,
        "the refused request keeps none of the room it was read into"
    );

    drop(occupied);
    transport_a
        .send(&node_b, Envelope::RelayPayload(payload))
        .await
        .expect("the same delivery is granted once the receiver has room");
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

/// A request whose answer is that its relay budget is full decodes only into room that budget has
/// free: with none left it says so at once instead of waiting for room, and decodes the same
/// request once room returns.
#[nervix_primitives::test]
async fn a_free_room_decode_reports_a_full_class_instead_of_waiting() {
    let executor = Executor::default();
    let request = wire::RelayGrantRequest {
        sender_epoch: 7,
        delivery: RelayDelivery {
            channel_incarnation: [6; 16],
            sequence: 3,
        },
        body_bytes: 1,
        metadata: wire::RelayMetadata {
            kind: RelayPayloadKind::Routed,
            domain: DomainName::parse("test").expect("test domain should be valid"),
            relay: RelayName::parse("relay").expect("test relay should be valid"),
            key: None,
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: None,
        },
    };
    let encoded = wire::encode_rkyv(
        &executor,
        MemoryClass::Relay,
        CpuClass::Data,
        executor.limits().relay_encoded_bytes.as_u64(),
        request.clone(),
    )
    .await
    .expect("the test grant request encodes");
    let relay_memory = executor.memory(MemoryClass::Relay);
    let room = relay_memory
        .capacity_bytes
        .checked_sub(relay_memory.reserved_bytes)
        .expect("a budget never reserves more than its capacity");
    let occupied = executor
        .try_reserve(MemoryClass::Relay, room)
        .expect("an idle relay budget takes all of its room");

    let decoded = timeout(
        Duration::from_secs(10),
        wire::decode_rkyv_in_free_room::<wire::RelayGrantRequest>(
            &executor,
            MemoryClass::Relay,
            CpuClass::Data,
            encoded.clone(),
        ),
    )
    .await
    .expect("a full relay budget is reported at once, not waited for")
    .expect("a well-formed request is no decode failure");
    let wire::FreeRoom::NoRoom = decoded else {
        panic!("a full relay budget leaves no room to decode into");
    };

    drop(occupied);
    let decoded = wire::decode_rkyv_in_free_room::<wire::RelayGrantRequest>(
        &executor,
        MemoryClass::Relay,
        CpuClass::Data,
        encoded,
    )
    .await
    .expect("a well-formed request decodes");
    let wire::FreeRoom::Taken(decoded) = decoded else {
        panic!("a relay budget with room decodes the request");
    };
    let (value, _charge) = decoded.into_parts();
    assert_eq!(value, request);
}
