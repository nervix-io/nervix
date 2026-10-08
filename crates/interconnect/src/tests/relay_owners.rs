//! Relay admission capacity at an authenticated peer's lifetime boundary.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** Capacity reclamation and retained intake assertions through public transports.
//! - **Depends on.** The connected transport fixture and current relay admission APIs.
//! - **Must not know.** Runtime graphs, persisted acknowledgements or connector behavior.

use super::*;

#[nervix_primitives::test]
#[ignore = "native authenticated frame cost probe run by bench-remote-owners"]
async fn remote_relay_frame_cost() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
    use arrow_schema::{DataType, Field, Schema};
    use sorted_vec::SortedVec;

    let mut pair = connected_transports().await;
    pair.transport_b
        .register_outbound_target(
            pair.node_a.clone(),
            NodeEndpoint::new("localhost", pair.transport_a.local_addr().port()),
        )
        .assured("the terminal ACK has a reverse authenticated route");
    let schema = StdArc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![StdArc::new(Int32Array::from(vec![7, 11, 13]))],
    )
    .assured("the cost probe has three typed Arrow rows");
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema)
        .assured("the Arrow fixture starts its IPC stream");
    writer.write(&batch).assured("the Arrow fixture encodes");
    writer
        .finish()
        .assured("the Arrow fixture closes its stream");
    drop(writer);
    let executor = Executor::default();
    let mut sequence = 0;
    for sample in 0..5 {
        let mut timings = Vec::with_capacity(200);
        let start = Instant::now();
        for _ in 0..200 {
            let frame_start = Instant::now();
            let registration = registration(sequence, &pair.node_a);
            let payload = RelayPayload {
                delivery: RelayDelivery {
                    channel_incarnation: [92; 16],
                    sequence,
                },
                kind: RelayPayloadKind::Routed,
                domain: DomainName::parse("capacity").assured("the fixture domain is valid"),
                relay: RelayName::parse("records").assured("the fixture relay is valid"),
                key: None,
                batch_ipc: executor
                    .try_charge_owned(MemoryClass::Relay, bytes.clone())
                    .assured("one Arrow frame fits the relay budget"),
                metadata: Vec::new(),
                acks: Vec::new(),
                admission: Some(registration.clone()),
            };
            pair.transport_a
                .send(&pair.node_b, Envelope::RelayPayload(payload))
                .await
                .assured("the frame body reaches the connected peer");
            let received = timeout(Duration::from_secs(10), pair.incoming_b.recv())
                .await
                .assured("the frame arrives within the fixture deadline")
                .assured("the receiver remains connected");
            let Envelope::RelayPayload(ref payload) = received.envelope else {
                panic!("the frame carries the Arrow body");
            };
            let decoded = StreamReader::try_new(std::io::Cursor::new(&*payload.batch_ipc), None)
                .assured("the current frame is valid Arrow IPC")
                .next()
                .assured("the frame retains its batch")
                .assured("the batch decodes");
            assert_eq!(decoded, batch);
            assert_eq!(
                received
                    .relay_admission
                    .as_ref()
                    .assured("the frame has an intake")
                    .admit(),
                RelayAdmissionDecision::Admitted
            );
            pair.transport_b
                .send(
                    &pair.node_a,
                    Envelope::Ack(registration.resolution(RemoteAckOutcome::Ack)),
                )
                .await
                .assured("the terminal reply crosses the authenticated connection");
            timeout(Duration::from_secs(10), async {
                loop {
                    let received = pair
                        ._incoming_a
                        .recv()
                        .await
                        .assured("the sender remains connected");
                    if let Envelope::Ack(ack) = received.envelope
                        && ack == registration.resolution(RemoteAckOutcome::Ack)
                    {
                        break;
                    }
                }
            })
            .await
            .assured("the terminal reply returns within the fixture deadline");
            assert_eq!(pair.transport_b.snapshot().relay_attempts, 0);
            sequence = sequence
                .checked_add(1)
                .assured("one thousand frames fit in u64");
            timings.push(frame_start.elapsed().as_nanos());
        }
        let elapsed = start.elapsed();
        let timings = SortedVec::from_unsorted(timings);
        println!(
            "remote-relay-frame sample={sample} frames={} rows_per_frame=3 ipc_bytes={} \
             ns_per_frame={} p50_ns={} p95_ns={} p99_ns={} retained_attempts=0",
            timings.len(),
            bytes.len(),
            elapsed.as_nanos() / u128::try_from(timings.len()).assured("bounded frame count"),
            timings[timings.len() / 2],
            timings[timings.len() * 95 / 100],
            timings[timings.len() * 99 / 100],
        );
    }
    pair.transport_a.shutdown().await;
    pair.transport_b.shutdown().await;
}

#[nervix_primitives::test]
async fn peer_departure_reclaims_an_admitted_relays_capacity() {
    let mut pair = connected_transports().await;
    let payload = RelayPayload {
        delivery: RelayDelivery {
            channel_incarnation: [91; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("capacity").assured("the fixture domain is valid"),
        relay: RelayName::parse("records").assured("the fixture relay is valid"),
        key: None,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .assured("one byte fits the fixture's relay budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(registration(91, &pair.node_a)),
    };
    pair.transport_a
        .send(&pair.node_b, Envelope::RelayPayload(payload))
        .await
        .assured("the connected fixture accepts the relay body");
    let received = timeout(Duration::from_secs(10), pair.incoming_b.recv())
        .await
        .assured("the sent body reaches its intake within the fixture deadline")
        .assured("the connected fixture retains its intake");
    let admission = received
        .relay_admission
        .assured("a relay body has an intake");
    assert_eq!(admission.admit(), RelayAdmissionDecision::Admitted);
    assert_eq!(pair.transport_b.snapshot().relay_attempts, 1);

    pair.transport_b
        .replace_live_nodes(&BTreeSet::from([pair.node_b.clone()]));

    let snapshot = pair.transport_b.snapshot();
    assert_eq!(
        snapshot.relay_attempts, 0,
        "a departed sender releases its admission capacity"
    );
    assert_eq!(snapshot.relay_channels, 0);
    assert_eq!(snapshot.relay_grants, 0);
    assert_eq!(
        admission.admit(),
        RelayAdmissionDecision::Admitted,
        "an intake already admitted may finish after its sender leaves"
    );
    pair.transport_a.shutdown().await;
    pair.transport_b.shutdown().await;
}
