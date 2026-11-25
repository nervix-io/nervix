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
    pair.transport_b.register_outbound_target(
        pair.node_a.clone(),
        NodeEndpoint::new("localhost", pair.transport_a.local_addr().port()),
    );
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
                ack_timing: nervix_models::RemoteAckTiming::default(),
                decoded_bytes: 0,
                delivery: RelayDelivery {
                    channel_incarnation: [92; 16],
                    sequence,
                },
                kind: RelayPayloadKind::Routed,
                domain: DomainName::parse("capacity").assured("the fixture domain is valid"),
                relay: RelayName::parse("records").assured("the fixture relay is valid"),
                unit: nervix_models::Unit::SINGLE,
                key: nervix_models::RemoteBranchKey::Unbranched,
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
    pair.transport_a
        .shutdown(crate::test_teardown_ceiling())
        .await;
    pair.transport_b
        .shutdown(crate::test_teardown_ceiling())
        .await;
}

#[nervix_primitives::test]
async fn peer_departure_reclaims_an_admitted_relays_capacity() {
    let mut pair = connected_transports().await;
    let payload = RelayPayload {
        ack_timing: nervix_models::RemoteAckTiming::default(),
        decoded_bytes: 0,
        delivery: RelayDelivery {
            channel_incarnation: [91; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("capacity").assured("the fixture domain is valid"),
        relay: RelayName::parse("records").assured("the fixture relay is valid"),
        unit: nervix_models::Unit::SINGLE,
        key: nervix_models::RemoteBranchKey::Unbranched,
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
    pair.transport_a
        .shutdown(crate::test_teardown_ceiling())
        .await;
    pair.transport_b
        .shutdown(crate::test_teardown_ceiling())
        .await;
}

/// One-byte rows on `delivery`, carrying the admission `ack_id` of `registrar`, whose delivery
/// captured `timing`.
fn turnover_payload(
    delivery: RelayDelivery,
    ack_id: u64,
    registrar: &ClusterNodeName,
    timing: nervix_models::RemoteAckTiming,
) -> RelayPayload {
    RelayPayload {
        ack_timing: timing,
        decoded_bytes: 0,
        delivery,
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("turnover").assured("the fixture domain is valid"),
        relay: RelayName::parse("records").assured("the fixture relay is valid"),
        unit: nervix_models::Unit::SINGLE,
        key: nervix_models::RemoteBranchKey::Unbranched,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .assured("one byte fits the fixture's relay budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(registration(ack_id, registrar)),
    }
}

/// The transport takes the relay channel retention the committed cluster parameters give: it starts
/// from the definitions' defaults, follows a committed `REMOTE_RELAY_TOTAL_TIMEOUT` once the
/// parameters are installed, so an owner still awaiting membership ends and the sweep passes by
/// it, and keeps that value when the same parameters are installed again.
#[nervix_primitives::test]
async fn installed_cluster_parameters_give_the_transport_their_relay_channel_retention() {
    let pair = connected_transports().await;
    assert_eq!(
        pair.transport_b.inner.committed_relay_retention(),
        crate::RelayChannelRetention::default()
    );
    let lowered =
        nervix_models::ClusterParameterValues::resolve(nervix_models::ClusterParameterSeedValues {
            remote_relay_total_timeout: Some(Duration::from_secs(20)),
            ..nervix_models::ClusterParameterSeedValues::default()
        })
        .assured("a twenty-second total admission budget fits every acknowledgement relationship");
    pair.transport_b.install_cluster_parameters(lowered);
    assert_eq!(
        pair.transport_b
            .inner
            .committed_relay_retention()
            .duration(),
        Duration::from_secs(40)
    );
    pair.transport_b.install_cluster_parameters(lowered);
    assert_eq!(
        pair.transport_b
            .inner
            .committed_relay_retention()
            .duration(),
        Duration::from_secs(40)
    );
    pair.transport_a
        .shutdown(crate::test_teardown_ceiling())
        .await;
    pair.transport_b
        .shutdown(crate::test_teardown_ceiling())
        .await;
}

/// A validated timing whose total admission budget is one second, so a receiver keeps what its
/// deliveries leave behind for two seconds.
fn one_second_timing() -> nervix_models::RemoteAckTiming {
    let timing = nervix_models::RemoteAckTiming {
        remote_ack_alive_interval: Duration::from_millis(100),
        remote_ack_sweep_interval: Duration::from_millis(100),
        remote_relay_total_timeout: Duration::from_secs(1),
    };
    timing
        .validate()
        .assured("the one-second timing is coherent");
    timing
}

/// A receiver keeps the watermark of every channel a sender opened through the authenticated
/// connection, charged to its relay memory, however many channels turned over, and answers a
/// repeated request for one of them from that watermark instead of admitting it again. Each
/// watermark keeps the retention its delivery captured: once that has passed, the receiver's own
/// sweep forgets the watermarks and returns their charge, and a request for a forgotten channel is
/// answered as retired on the grant, status and cancellation paths.
#[nervix_primitives::test]
async fn a_receiver_charges_every_channel_watermark_until_its_retention_passes() {
    let options = TransportOptions {
        incoming_queue_capacity: 1,
        ..TransportOptions::default()
    };
    let timing = one_second_timing();
    let mut pair = connected_transports_with_options(options).await;
    pair.transport_b.register_outbound_target(
        pair.node_a.clone(),
        NodeEndpoint::new("localhost", pair.transport_a.local_addr().port()),
    );
    for channel in 1..=6_u8 {
        let delivery = RelayDelivery {
            channel_incarnation: [channel; 16],
            sequence: 0,
        };
        let ack_id = u64::from(channel);
        pair.transport_a
            .send(
                &pair.node_b,
                Envelope::RelayPayload(turnover_payload(delivery, ack_id, &pair.node_a, timing)),
            )
            .await
            .assured("every new channel is granted");
        let received = timeout(Duration::from_secs(10), pair.incoming_b.recv())
            .await
            .assured("the body reaches its intake within the fixture deadline")
            .assured("the receiver remains connected");
        assert_eq!(
            received
                .relay_admission
                .as_ref()
                .assured("a relay body has an intake")
                .admit(),
            RelayAdmissionDecision::Admitted
        );
        drop(received);
        // The sender's one-item queue also takes the receiver's progress reports, so the
        // terminal outcome is sent while the sender drains them.
        let resolution = registration(ack_id, &pair.node_a).resolution(RemoteAckOutcome::Ack);
        let sending = pair
            .transport_b
            .send(&pair.node_a, Envelope::Ack(resolution.clone()));
        let incoming_a = &mut pair._incoming_a;
        let draining = timeout(Duration::from_secs(10), async {
            loop {
                nervix_primitives::task::consume_budget().await;
                let received = incoming_a
                    .recv()
                    .await
                    .assured("the sender remains connected");
                if let Envelope::Ack(ack) = received.envelope
                    && ack == resolution
                {
                    break;
                }
            }
        });
        let (sent, drained) = futures_util::future::join(sending, draining).await;
        sent.assured("the terminal outcome crosses the authenticated connection");
        drained.assured("the terminal outcome returns within the fixture deadline");
        assert_eq!(pair.transport_b.snapshot().relay_attempts, 0);
    }
    assert_eq!(pair.transport_b.snapshot().relay_channel_watermarks, 6);
    assert_eq!(
        pair.executor_b.snapshot().relay_memory.reserved_bytes,
        6 * crate::connection::RelayChannelWatermarks::watermark_charge(),
        "the receiver holds exactly the charge of the six watermarks it keeps"
    );

    let first = RelayDelivery {
        channel_incarnation: [1; 16],
        sequence: 0,
    };
    pair.transport_a
        .send(
            &pair.node_b,
            Envelope::RelayPayload(turnover_payload(first, 1, &pair.node_a, timing)),
        )
        .await
        .assured("a repeated request is answered from the channel's retained watermark");
    assert_eq!(
        pair.transport_a
            .relay_admission_status(&pair.node_b, first, timing)
            .await
            .assured("the receiver answers the status of a retained channel"),
        RelayAdmissionStatus::Admitted
    );

    timeout(Duration::from_secs(30), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let watermarks = pair.transport_b.snapshot().relay_channel_watermarks;
            let reserved = pair.executor_b.snapshot().relay_memory.reserved_bytes;
            // The six channels shared one retention, so one retired-channel floor stays charged.
            if watermarks == 0
                && reserved == crate::connection::RelayChannelWatermarks::floor_charge()
            {
                return;
            }
            nervix_primitives::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured(
        "the receiver's sweep forgets every watermark once its two-second retention passes, \
         keeping only the charge of the floor that refuses them",
    );

    let repeated = pair
        .transport_a
        .send(
            &pair.node_b,
            Envelope::RelayPayload(turnover_payload(first, 1, &pair.node_a, timing)),
        )
        .await
        .expect_err("a forgotten channel is never granted again");
    assert!(
        matches!(
            repeated.current_context(),
            TransportError::RelayIndeterminate
        ),
        "{repeated:?}"
    );
    assert_eq!(
        pair.transport_a
            .relay_admission_status(&pair.node_b, first, timing)
            .await
            .assured("the receiver answers the status of a forgotten channel"),
        RelayAdmissionStatus::Retired
    );
    assert_eq!(
        pair.transport_a
            .cancel_relay(&pair.node_b, first, timing)
            .await
            .assured("the receiver answers the cancellation of a forgotten channel"),
        RelayAdmissionStatus::Retired
    );
    assert!(
        pair.incoming_b.try_recv().is_err(),
        "a channel's batch never reaches the application twice"
    );
    pair.transport_a
        .shutdown(crate::test_teardown_ceiling())
        .await;
    pair.transport_b
        .shutdown(crate::test_teardown_ceiling())
        .await;
}

/// A cancellation that fences a position its receiver never granted takes that watermark's relay
/// charge first. While the receiver's relay budget has no room for it, the cancellation waits,
/// holding none of the class, rather than being answered `Unknown`; once room returns it is
/// answered as cancelled, and the fence keeps that position cancelled.
#[nervix_primitives::test]
async fn a_cancellation_that_fences_a_position_waits_for_its_watermark_charge() {
    let pair = connected_transports().await;
    let timing = nervix_models::RemoteAckTiming::default();
    let relay_memory = pair.executor_b.memory(MemoryClass::Relay);
    let room = relay_memory
        .capacity_bytes
        .checked_sub(relay_memory.reserved_bytes)
        .verified("a budget never reserves more than its capacity");
    let occupied = pair
        .executor_b
        .try_reserve(MemoryClass::Relay, room)
        .assured("an idle receiver's relay budget takes all of its room");
    let delivery = RelayDelivery {
        channel_incarnation: [9; 16],
        sequence: 0,
    };
    let canceller = pair.transport_a.clone();
    let receiver = pair.node_b.clone();
    let cancelling = nervix_primitives::task::spawn(async move {
        canceller.cancel_relay(&receiver, delivery, timing).await
    });
    nervix_primitives::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !cancelling.is_finished(),
        "a receiver without relay room holds the cancellation back instead of answering Unknown"
    );

    drop(occupied);
    let answered = cancelling
        .await
        .assured("the cancelling task is joined")
        .assured("the receiver answers the cancellation once it has room");
    assert_eq!(answered, RelayAdmissionStatus::Cancelled);
    assert_eq!(
        pair.transport_a
            .relay_admission_status(&pair.node_b, delivery, timing)
            .await
            .assured("the receiver answers the status of a fenced position"),
        RelayAdmissionStatus::Cancelled
    );
    pair.transport_a
        .shutdown(crate::test_teardown_ceiling())
        .await;
    pair.transport_b
        .shutdown(crate::test_teardown_ceiling())
        .await;
}
