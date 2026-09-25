//! Relay admission and cancellation over the authenticated simulated transport.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** Same-process relay retry, cancellation, and Arrow delivery assertions.
//! - **Depends on.** The transport fixture, production relay APIs, and Turmoil network faults.
//! - **Must not know.** Runtime graphs, persistent ACK stores, or connector behavior.

use nervix_interconnect::{RelayAdmissionDecision, RelayAdmissionStatus};
use nervix_models::{RemoteAckOutcome, RemoteAckResolution};

use super::*;

#[derive(Clone, Copy, Debug)]
enum RelayCase {
    ResponseLostAfterAdmission,
    CancelBeforeGrant,
    CancelWhileReplyIsLost,
    CancelDuringReconnect,
}

impl RelayCase {
    fn name(self) -> &'static str {
        match self {
            Self::ResponseLostAfterAdmission => "relay response lost after admission",
            Self::CancelBeforeGrant => "relay cancellation before grant",
            Self::CancelWhileReplyIsLost => "relay cancellation while reply is lost",
            Self::CancelDuringReconnect => "relay cancellation during reconnect",
        }
    }

    fn delivery(self) -> RelayDelivery {
        let incarnation = match self {
            Self::ResponseLostAfterAdmission => 71,
            Self::CancelBeforeGrant => 72,
            Self::CancelWhileReplyIsLost => 73,
            Self::CancelDuringReconnect => 74,
        };
        RelayDelivery {
            channel_incarnation: [incarnation; 16],
            sequence: 0,
        }
    }

    fn ack_id(self) -> u64 {
        match self {
            Self::ResponseLostAfterAdmission => 71,
            Self::CancelBeforeGrant => 72,
            Self::CancelWhileReplyIsLost => 73,
            Self::CancelDuringReconnect => 74,
        }
    }
}

fn payload(case: RelayCase, sender: &Transport) -> RelayPayload {
    let batch_ipc = Executor::default()
        .try_charge_owned(MemoryClass::Relay, arrow_batch())
        .assured("the Arrow fixture fits the relay memory budget");
    RelayPayload {
        delivery: case.delivery(),
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("simulated").assured("fixture domain is valid"),
        relay: RelayName::parse("records").assured("fixture relay is valid"),
        key: None,
        batch_ipc,
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(RemoteAckRegistration {
            ack_id: case.ack_id(),
            reply_node_id: sender.node_id().clone(),
        }),
    }
}

fn exercise_relay(case: RelayCase, seed: u64) -> Vec<TraceEvent> {
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let (ready_tx, ready_rx) = watch::channel(false);
    let (received_tx, received_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let trace = SemanticTrace::default();
    let server_trace = trace.clone();
    let client_trace = trace.clone();
    let mut scenario_config = config(seed);
    scenario_config.bounds.simulated_duration = Duration::from_secs(60);
    let result = scenario_config.run(case.name(), move |simulation| {
        let server_finished = finished_tx.clone();
        simulation.host("server", move || {
            let credentials = server_credentials.clone();
            let ready = ready_tx.clone();
            let received = received_tx.clone();
            let mut done = done_rx.clone();
            let finished = server_finished.clone();
            let trace = server_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let (server, mut incoming) =
                        bind_with_incoming("server", credentials, seed).await;
                    let live = ["client", "server"]
                        .into_iter()
                        .map(|name| {
                            ClusterNodeName::parse(name).assured("fixture node name is valid")
                        })
                        .collect::<BTreeSet<_>>();
                    server.replace_live_nodes(&live);
                    ready.send_replace(true);
                    if !matches!(case, RelayCase::CancelBeforeGrant) {
                        let envelope = tokio::time::timeout(HOST_DEADLINE, incoming.recv())
                            .await
                            .assured("the relay reaches the receiver before the simulated deadline")
                            .assured("the relay receiver remains open");
                        let Envelope::RelayPayload(ref body) = envelope.envelope else {
                            panic!("the relay channel carries a relay payload");
                        };
                        assert_eq!(body.delivery, case.delivery());
                        assert_eq!(
                            body.admission
                                .as_ref()
                                .assured("relay carries ACK registration")
                                .ack_id,
                            case.ack_id()
                        );
                        assert_eq!(decode_arrow(&body.batch_ipc), 3);
                        turmoil::partition_oneway("server", "client");
                        trace.record(
                            "server",
                            format!(
                                "{:?} Arrow body received; reply partitioned",
                                case.delivery()
                            ),
                        );
                        received.send_replace(true);
                        if matches!(case, RelayCase::ResponseLostAfterAdmission) {
                            assert_eq!(
                                envelope
                                    .relay_admission
                                    .assured("relay has admission token")
                                    .admit(),
                                RelayAdmissionDecision::Admitted
                            );
                            trace.record("server", "runtime admission committed");
                        } else {
                            let mut completion = done.clone();
                            wait_for(&mut completion).await;
                            assert_eq!(
                                envelope
                                    .relay_admission
                                    .assured("relay has admission token")
                                    .admit(),
                                RelayAdmissionDecision::Cancelled
                            );
                            trace.record("server", "cancellation fenced runtime admission");
                        }
                    }
                    wait_for(&mut done).await;
                    assert!(
                        incoming.try_recv().is_err(),
                        "a retained attempt enters the application queue only once"
                    );
                    server.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count.checked_add(1).assured("two fixture hosts finish")
                });
                result
            }
        });
        let client_finished = finished_tx.clone();
        simulation.host("client", move || {
            let credentials = client_credentials.clone();
            let mut ready = ready_rx.clone();
            let mut received = received_rx.clone();
            let done = done_tx.clone();
            let finished = client_finished.clone();
            let trace = client_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let client = bind_with_incoming(
                        "client",
                        credentials,
                        seed.checked_add(1).assured("fixture seed fits"),
                    )
                    .await;
                    let (client, mut incoming) = client;
                    let peer =
                        ClusterNodeName::parse("server").assured("fixture node name is valid");
                    client.replace_live_nodes(&BTreeSet::from([
                        client.node_id().clone(),
                        peer.clone(),
                    ]));
                    wait_for(&mut ready).await;
                    register_peer(&client, "server").await;
                    let body = payload(case, &client);
                    if matches!(case, RelayCase::CancelBeforeGrant) {
                        assert_eq!(
                            client
                                .cancel_relay(&peer, case.delivery())
                                .await
                                .assured("cancellation is acknowledged"),
                            RelayAdmissionStatus::Cancelled
                        );
                        trace.record("client", "cancellation fence established before grant");
                        let error = match client.send(&peer, Envelope::RelayPayload(body)).await {
                            Ok(()) => panic!("the fenced grant must be refused"),
                            Err(error) => error,
                        };
                        assert!(matches!(error, TransportError::RelayCancelled), "{error:?}");
                        assert_eq!(
                            client
                                .relay_admission_status(&peer, case.delivery())
                                .await
                                .assured("the cancellation fence remains known"),
                            RelayAdmissionStatus::Cancelled
                        );
                    } else {
                        let sending = client.clone();
                        let target = peer.clone();
                        let send_task = tokio::spawn(async move {
                            sending.send(&target, Envelope::RelayPayload(body)).await
                        });
                        wait_for(&mut received).await;
                        let cancellation = if matches!(case, RelayCase::CancelWhileReplyIsLost) {
                            let cancelling = client.clone();
                            let target = peer.clone();
                            trace.record(
                                "client",
                                "cancellation requested while body reply is pending",
                            );
                            Some(tokio::spawn(async move {
                                cancelling.cancel_relay(&target, case.delivery()).await
                            }))
                        } else {
                            None
                        };
                        let first = send_task.await.assured("relay sender task joins");
                        let error = match first {
                            Ok(()) => panic!("the partition must hide the relay body response"),
                            Err(error) => error,
                        };
                        assert!(
                            matches!(
                                error,
                                TransportError::RequestTimeout { .. }
                                    | TransportError::ProgressTimeout { .. }
                                    | TransportError::Closed(_)
                                    | TransportError::Http2(_)
                            ),
                            "{error:?}"
                        );
                        trace.record(
                            "client",
                            "body response lost after receiver took the attempt",
                        );
                        if let Some(cancellation) = cancellation {
                            let result = cancellation
                                .await
                                .assured("cancellation request task joins");
                            assert!(
                                result.is_err(),
                                "the partition hides the cancellation response"
                            );
                            trace.record(
                                "client",
                                "cancellation response lost with the body response",
                            );
                        }
                        turmoil::repair_oneway("server", "client");
                        client.replace_outbound_targets(&Default::default());
                        register_peer(&client, "server").await;
                        let cancellation = if matches!(case, RelayCase::CancelDuringReconnect) {
                            let cancelling = client.clone();
                            let target = peer.clone();
                            trace.record("client", "cancellation requested during reconnect");
                            Some(tokio::spawn(async move {
                                cancelling.cancel_relay(&target, case.delivery()).await
                            }))
                        } else {
                            None
                        };
                        wait_for_connection(&client, &peer).await;
                        trace.record("client", "same receiver process reconnected");
                        let body = payload(case, &client);
                        if !matches!(case, RelayCase::ResponseLostAfterAdmission) {
                            if let Some(cancellation) = cancellation {
                                assert_eq!(
                                    cancellation
                                        .await
                                        .assured("reconnect cancellation task joins")
                                        .assured(
                                            "cancellation resolves through the reconnected \
                                             transport"
                                        ),
                                    RelayAdmissionStatus::Cancelled
                                );
                            }
                            assert_eq!(
                                client
                                    .relay_admission_status(&peer, case.delivery())
                                    .await
                                    .assured("fence status is known"),
                                RelayAdmissionStatus::Cancelled
                            );
                            let error = match client.send(&peer, Envelope::RelayPayload(body)).await
                            {
                                Ok(()) => panic!("the cancelled retry must be refused"),
                                Err(error) => error,
                            };
                            assert!(matches!(error, TransportError::RelayCancelled), "{error:?}");
                            trace.record("client", "reconciled cancellation refused the retry");
                        } else {
                            client
                                .send(&peer, Envelope::RelayPayload(body))
                                .await
                                .assured("same-epoch retry reconciles admitted attempt");
                            let outcome = tokio::time::timeout(HOST_DEADLINE, incoming.recv())
                                .await
                                .assured("the semantic ACK arrives")
                                .assured("sender queue remains open");
                            assert!(matches!(
                                outcome.envelope,
                                Envelope::Ack(RemoteAckResolution {
                                    ack_id: 71,
                                    outcome: RemoteAckOutcome::Ack
                                })
                            ));
                            assert_eq!(
                                client
                                    .relay_admission_status(&peer, case.delivery())
                                    .await
                                    .assured("admission status is known"),
                                RelayAdmissionStatus::Admitted
                            );
                            assert_eq!(
                                client
                                    .cancel_relay(&peer, case.delivery())
                                    .await
                                    .assured("late cancellation returns the retained outcome"),
                                RelayAdmissionStatus::Admitted
                            );
                            trace.record(
                                "client",
                                "same-epoch retry and late cancellation returned admitted \
                                 identity and ACK",
                            );
                        }
                    }
                    done.send_replace(true);
                    client.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count.checked_add(1).assured("two fixture hosts finish")
                });
                result
            }
        });
        simulation.client("observer", async move {
            let mut finished = finished_rx;
            wait_for_count(&mut finished, 2).await;
            Ok(())
        });
    });
    assert!(
        result.is_ok(),
        "case {case:?} seed {seed}: {result:?}\n{}",
        trace.render()
    );
    trace.events()
}

#[test]
fn relay_reconciliation_and_cancellation_survive_lost_replies() {
    for (case, seed) in [
        (RelayCase::ResponseLostAfterAdmission, 71),
        (RelayCase::CancelBeforeGrant, 72),
        (RelayCase::CancelWhileReplyIsLost, 73),
        (RelayCase::CancelDuringReconnect, 74),
    ] {
        let first = exercise_relay(case, seed);
        let replay = exercise_relay(case, seed);
        assert_eq!(
            first, replay,
            "relay case {case:?} seed {seed} did not replay"
        );
    }
}
