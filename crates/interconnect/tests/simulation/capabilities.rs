//! A receiver restart between a published capacity claim and a relay grant.
//!
//! Layer: test harness.
//! - **Owns.** A bounded mid-roll stale-claim refusal, publication update and ordered byte pieces.
//! - **Depends on.** Production TLS transport, receive settings and the production slice cursor.
//! - **Must not know.** Whole-node Raft application, Arrow row semantics or runtime ACK shares.

use nervix_interconnect::{PeerReceiveSettings, RelaySliceCursor};
use nervix_models::{NodeCapabilities, NodeCapabilityRecord, Unit};
use ubyte::ByteUnit;

use super::*;

#[derive(Debug, Archive, Serialize, Deserialize)]
struct CurrentReceiveBound;

impl InterconnectRequest for CurrentReceiveBound {
    type Response = u64;
    const NAME: &'static str = "simulation_current_receive_bound";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(1);
}

fn receive(bytes: u64) -> PeerReceiveSettings {
    PeerReceiveSettings::resolve(
        Some(ByteUnit::Byte(bytes)),
        None,
        None,
        None,
        &nervix_execution::OperationLimits::default(),
    )
    .assured("positive fixture receive bound")
}

fn claim(incarnation: u64, bytes: u64) -> NodeCapabilityRecord {
    NodeCapabilityRecord {
        identity: ClusterNodeIdentity::new(
            ClusterNodeName::parse("server").assured("fixture name"),
            ClusterNodeIncarnation::new(incarnation),
        ),
        capabilities: NodeCapabilities {
            cpu_count: None,
            memory_bytes: None,
            receive: receive(bytes).bounds(),
        },
    }
}

#[test]
fn a_receiver_roll_refuses_a_stale_claim_and_admits_each_re_sliced_piece_once() {
    Scenario {
        name: "receiver capability roll retains ordered unadmitted pieces",
        fault_plan: "restart the receiver with a smaller bound after its first claim is \
                     published; retain that stale sender claim until its typed refusal, then \
                     publish the current claim and send contiguous pieces",
        seeds: &[805],
    }
    .check(config, capability_roll);
}

fn capability_roll(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let trace = run.trace();
    let (phase, phase_rx) = watch::channel(0_usize);
    let (roll, roll_rx) = watch::channel(false);
    let (done, done_rx) = watch::channel(false);
    let (finished, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let server_finished = finished.clone();
        simulation.host("server", move || {
            let credentials = server_credentials.clone();
            let phase = phase.clone();
            let mut roll = roll_rx.clone();
            let mut done = done_rx.clone();
            let finished = server_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let (initial, initial_incoming) = bind_with_incoming_options(
                        "server",
                        credentials.clone(),
                        seed,
                        TransportOptions {
                            peer_receive: Some(receive(16)),
                            ..TransportOptions::default()
                        },
                    )
                    .await;
                    phase.send_replace(1);
                    wait_for(&mut roll).await;
                    initial.shutdown().await;
                    drop(initial_incoming);
                    let next_seed = seed.checked_add(1).assured("fixture seed fits");
                    let (server, mut incoming) = bind_with_incoming_options(
                        "server",
                        credentials,
                        next_seed,
                        TransportOptions {
                            peer_receive: Some(receive(4)),
                            ..TransportOptions::default()
                        },
                    )
                    .await;
                    server
                        .register_handler::<CurrentReceiveBound, _, _>(|_, _| async { 4 })
                        .assured("one run probe handler");
                    register_peer(&server, "client").await;
                    phase.send_replace(2);
                    let mut observed = Vec::new();
                    for sequence in 0..2 {
                        let received =
                            nervix_primitives::time::timeout(HOST_DEADLINE, incoming.recv())
                                .await
                                .assured("piece arrives within the host bound")
                                .assured("receiver remains open");
                        let Envelope::RelayPayload(payload) = received.envelope else {
                            panic!("relay payload expected");
                        };
                        assert_eq!(payload.delivery.sequence, sequence);
                        assert_eq!(payload.batch_ipc.len(), 4);
                        observed.extend_from_slice(&payload.batch_ipc);
                        assert_eq!(
                            received
                                .relay_admission
                                .assured("relay admission token")
                                .admit(),
                            nervix_interconnect::RelayAdmissionDecision::Admitted
                        );
                    }
                    assert_eq!(observed, (0..8_u8).collect::<Vec<_>>());
                    wait_for(&mut done).await;
                    assert!(
                        incoming.try_recv().is_err(),
                        "no duplicate piece reaches the receiver"
                    );
                    server.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });
        let client_finished = finished.clone();
        simulation.host("client", move || {
            let credentials = client_credentials.clone();
            let mut phase = phase_rx.clone();
            let roll = roll.clone();
            let done = done.clone();
            let finished = client_finished.clone();
            let trace = trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let client_seed = seed.checked_add(2).assured("fixture seed fits");
                    let (client, mut replies) =
                        bind_with_incoming("client", credentials, client_seed).await;
                    wait_for_count(&mut phase, 1).await;
                    register_peer(&client, "server").await;
                    let peer = ClusterNodeName::parse("server").assured("fixture name");
                    client.replace_live_nodes(&BTreeSet::from([peer.clone()]));
                    client.publish_peer_capabilities(&std::collections::BTreeMap::from([(
                        peer.clone(),
                        claim(1, 16),
                    )]));
                    roll.send_replace(true);
                    wait_for_count(&mut phase, 2).await;
                    nervix_primitives::time::timeout(HOST_DEADLINE, async {
                        loop {
                            if matches!(client.request(&peer, CurrentReceiveBound).await, Ok(4)) {
                                break;
                            }
                            nervix_primitives::task::consume_budget().await;
                        }
                    })
                    .await
                    .assured("the restarted peer answers within the host bound");
                    let make_payload = |sequence, bytes: Vec<u8>| RelayPayload {
                        decoded_bytes: 0,
                        delivery: RelayDelivery {
                            channel_incarnation: [85; 16],
                            sequence,
                        },
                        kind: RelayPayloadKind::Routed,
                        domain: DomainName::parse("simulated").assured("fixture domain"),
                        relay: RelayName::parse("records").assured("fixture relay"),
                        unit: Unit::SINGLE,
                        key: None,
                        batch_ipc: Executor::default()
                            .try_charge_owned(MemoryClass::Relay, bytes)
                            .assured("bounded fixture body"),
                        metadata: Vec::new(),
                        acks: Vec::new(),
                        admission: Some(fixture_registration(sequence, &client)),
                    };
                    let refused = client
                        .send(
                            &peer,
                            Envelope::RelayPayload(make_payload(0, (0..8).collect())),
                        )
                        .await
                        .expect_err("the stale claim exceeds the restarted receiver");
                    let TransportError::RelayBoundsRefused(bounds) = refused.current_context()
                    else {
                        panic!("typed receive-bound refusal: {refused:?}");
                    };
                    assert_eq!(bounds.relay_batch_bytes.get(), 4);
                    let mut cursor = RelaySliceCursor::new(8);
                    cursor.refuse(*bounds);
                    client.publish_peer_capabilities(&std::collections::BTreeMap::from([(
                        peer.clone(),
                        claim(2, 4),
                    )]));
                    let mut sequence = 0;
                    while cursor.admitted_rows() < cursor.total_rows() {
                        while cursor.part_rows() > 4 {
                            assert!(cursor.shrink());
                        }
                        let end = cursor
                            .admitted_rows()
                            .checked_add(cursor.part_rows())
                            .assured("eight fixture bytes");
                        let bytes = (cursor.admitted_rows()..end)
                            .map(|value| u8::try_from(value).assured("eight fixture bytes"))
                            .collect();
                        client
                            .send(&peer, Envelope::RelayPayload(make_payload(sequence, bytes)))
                            .await
                            .assured("the unused sequence admits its smaller piece");
                        let admitted =
                            nervix_primitives::time::timeout(HOST_DEADLINE, replies.recv())
                                .await
                                .assured("admission reply is bounded")
                                .assured("client intake remains open");
                        assert!(
                            matches!(admitted.envelope, Envelope::Ack(_)),
                            "confirmed admission precedes advancing the cursor"
                        );
                        cursor.admit();
                        sequence += 1;
                    }
                    trace.record(
                        "client",
                        "a stale receive claim was refused before admission; each contiguous \
                         piece arrived once",
                    );
                    done.send_replace(true);
                    client.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });
        simulation.client("observer", async move {
            let mut finished = finished_rx;
            wait_for_count(&mut finished, 2).await;
            Ok(())
        });
    })
}
