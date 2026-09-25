//! Relay admission and cancellation over the authenticated simulated transport.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** Relay retry, cancellation, restart fencing, and Arrow delivery assertions.
//! - **Depends on.** The transport fixture, production relay APIs, and Turmoil network faults.
//! - **Must not know.** Runtime graphs, persistent ACK stores, or connector behavior.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use nervix_interconnect::{RelayAdmissionDecision, RelayAdmissionStatus};
use nervix_models::{CoordinationIdentity, RemoteAckOutcome, RemoteAckResolution};

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

    fn plan(self) -> &'static str {
        match self {
            Self::ResponseLostAfterAdmission => {
                "drop the receiver's replies once it takes an Arrow relay and admits it, reconnect \
                 to the same receiver process, retry the relay and cancel it late"
            }
            Self::CancelBeforeGrant => {
                "fence the relay by cancelling it before it is sent, then send it; no fault on the \
                 link"
            }
            Self::CancelWhileReplyIsLost => {
                "drop the receiver's replies once it takes an Arrow relay, cancel while the reply \
                 is lost, reconnect and retry"
            }
            Self::CancelDuringReconnect => {
                "drop the receiver's replies once it takes an Arrow relay, cancel while \
                 reconnecting to the same receiver process, then retry"
            }
        }
    }

    /// The committed regression seeds.
    fn seeds(self) -> &'static [u64] {
        match self {
            Self::ResponseLostAfterAdmission => &[71],
            Self::CancelBeforeGrant => &[72],
            Self::CancelWhileReplyIsLost => &[73],
            Self::CancelDuringReconnect => &[74],
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
    relay_payload(case.delivery(), case.ack_id(), sender)
}

fn relay_payload(delivery: RelayDelivery, ack_id: u64, sender: &Transport) -> RelayPayload {
    let batch_ipc = Executor::default()
        .try_charge_owned(MemoryClass::Relay, arrow_batch())
        .assured("the Arrow fixture fits the relay memory budget");
    RelayPayload {
        delivery,
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("simulated").assured("fixture domain is valid"),
        relay: RelayName::parse("records").assured("fixture relay is valid"),
        key: None,
        batch_ipc,
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(RemoteAckRegistration {
            ack_id,
            reply_node_id: sender.node_id().clone(),
        }),
    }
}

fn exercise_relay(case: RelayCase, run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let server_trace = run.trace();
    let client_trace = run.trace();
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let (ready_tx, ready_rx) = watch::channel(false);
    let (received_tx, received_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
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
    })
}

#[test]
fn relay_reconciliation_and_cancellation_survive_lost_replies() {
    for case in [
        RelayCase::ResponseLostAfterAdmission,
        RelayCase::CancelBeforeGrant,
        RelayCase::CancelWhileReplyIsLost,
        RelayCase::CancelDuringReconnect,
    ] {
        let scenario = Scenario {
            name: case.name(),
            fault_plan: case.plan(),
            seeds: case.seeds(),
        };
        scenario.check(fault_config, |run| exercise_relay(case, run));
    }
}

#[derive(Clone, Copy, Debug)]
enum RestartMilestone {
    BodyReceived,
    RuntimeAdmitted,
    RuntimeAdmittedWithDelayedReply,
}

impl RestartMilestone {
    fn name(self) -> &'static str {
        match self {
            Self::BodyReceived => "receiver restart after relay body receipt",
            Self::RuntimeAdmitted => "receiver restart after runtime admission",
            Self::RuntimeAdmittedWithDelayedReply => "receiver restart with delayed relay response",
        }
    }

    fn plan(self) -> &'static str {
        match self {
            Self::BodyReceived => {
                "drop the receiver's replies once it takes an Arrow relay body, crash and restart \
                 the receiver with a new process epoch, then send fresh work"
            }
            Self::RuntimeAdmitted => {
                "drop the receiver's replies once it admits an Arrow relay, crash and restart the \
                 receiver with a new process epoch, then send fresh work"
            }
            Self::RuntimeAdmittedWithDelayedReply => {
                "hold the receiver's replies once it admits an Arrow relay, crash the receiver, \
                 release the held reply, restart it with a new process epoch, then send fresh work"
            }
        }
    }

    /// The committed regression seeds.
    fn seeds(self) -> &'static [u64] {
        match self {
            Self::BodyReceived => &[81],
            Self::RuntimeAdmitted => &[83],
            // Seed 1036 found that a simulated crash tears a host's tasks down in an order set by
            // every Tokio task the process created before, so its two runs diverged until each
            // attempt ran in a fresh process.
            Self::RuntimeAdmittedWithDelayedReply => &[85, 1036],
        }
    }
}

fn restart_config(seed: u64) -> SimulationConfig {
    let mut scenario_config = config(seed);
    scenario_config.bounds.simulated_duration = Duration::from_secs(90);
    scenario_config
}

fn restarted_receiver_fences_unresolved_relay(
    milestone: RestartMilestone,
    run: ScenarioRun,
) -> Result<(), SimulationError> {
    const FIRST: RelayDelivery = RelayDelivery {
        channel_incarnation: [81; 16],
        sequence: 0,
    };
    const FRESH: RelayDelivery = RelayDelivery {
        channel_incarnation: [82; 16],
        sequence: 0,
    };

    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let (ready_tx, ready_rx) = watch::channel(0_usize);
    let (fresh_tx, fresh_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let epochs = StdArc::new(parking_lot::Mutex::new(Vec::<CoordinationIdentity>::new()));
    let incarnations = StdArc::new(AtomicUsize::new(0));
    let crash_phase = StdArc::new(AtomicU8::new(0));
    let seed = run.seed();
    let server_trace = run.trace();
    let client_trace = run.trace();
    let server_epochs = epochs.clone();
    let client_epochs = epochs.clone();
    let server_incarnations = incarnations.clone();
    let server_crash_phase = crash_phase.clone();
    let control_crash_phase = crash_phase.clone();
    let result = run.simulate_with_control(
        move |simulation| {
            let server_finished = finished_tx.clone();
            simulation.host("server", move || {
                let incarnation = server_incarnations.fetch_add(1, Ordering::SeqCst);
                let credentials = server_credentials.clone();
                let ready = ready_tx.clone();
                let mut fresh = fresh_rx.clone();
                let mut done = done_rx.clone();
                let finished = server_finished.clone();
                let epochs = server_epochs.clone();
                let crash_phase = server_crash_phase.clone();
                let trace = server_trace.clone();
                async move {
                    let result = HostSupervisor::run(async move {
                        let process_seed = seed
                            .checked_add(
                                u64::try_from(incarnation).assured("two incarnations fit in u64"),
                            )
                            .assured("fixture seed fits");
                        let (server, mut incoming) =
                            bind_with_incoming("server", credentials, process_seed).await;
                        let identity = server
                            .next_coordination_identity()
                            .assured("fixture process can allocate its identity");
                        epochs.lock().push(identity);
                        let live = ["client", "server"]
                            .into_iter()
                            .map(|name| {
                                ClusterNodeName::parse(name).assured("fixture node name is valid")
                            })
                            .collect::<BTreeSet<_>>();
                        server.replace_live_nodes(&live);
                        ready.send_modify(|count| {
                            *count = count.checked_add(1).assured("two incarnations start")
                        });
                        if incarnation == 0 {
                            let envelope = tokio::time::timeout(HOST_DEADLINE, incoming.recv())
                                .await
                                .assured("the first relay reaches the receiver")
                                .assured("the first receiver queue remains open");
                            let Envelope::RelayPayload(ref body) = envelope.envelope else {
                                panic!("the first receiver gets a relay body");
                            };
                            assert_eq!(body.delivery, FIRST);
                            assert_eq!(decode_arrow(&body.batch_ipc), 3);
                            if let RestartMilestone::RuntimeAdmittedWithDelayedReply = milestone {
                                turmoil::hold("server", "client");
                            } else {
                                turmoil::partition_oneway("server", "client");
                            }
                            if let RestartMilestone::RuntimeAdmitted
                            | RestartMilestone::RuntimeAdmittedWithDelayedReply = milestone
                            {
                                assert_eq!(
                                    envelope
                                        .relay_admission
                                        .assured("first relay has admission token")
                                        .admit(),
                                    RelayAdmissionDecision::Admitted
                                );
                                trace.record(
                                    "server",
                                    "first Arrow relay admitted; response hidden",
                                );
                            } else {
                                trace.record(
                                    "server",
                                    "first Arrow body received; admission pending",
                                );
                            }
                            crash_phase.store(1, Ordering::SeqCst);
                            std::future::pending::<()>().await;
                        }
                        assert_eq!(incarnation, 1, "only one receiver restart is scheduled");
                        wait_for(&mut fresh).await;
                        let envelope = tokio::time::timeout(HOST_DEADLINE, incoming.recv())
                            .await
                            .assured("fresh relay reaches the restarted receiver")
                            .assured("restarted receiver queue remains open");
                        let Envelope::RelayPayload(ref body) = envelope.envelope else {
                            panic!("restarted receiver gets a relay body");
                        };
                        assert_eq!(body.delivery, FRESH, "stale relay was not replayed");
                        assert_eq!(decode_arrow(&body.batch_ipc), 3);
                        assert_eq!(
                            envelope
                                .relay_admission
                                .assured("fresh relay has admission token")
                                .admit(),
                            RelayAdmissionDecision::Admitted
                        );
                        trace.record("server", "fresh Arrow relay admitted after restart");
                        wait_for(&mut done).await;
                        assert!(
                            incoming.try_recv().is_err(),
                            "only the fresh relay was admitted"
                        );
                        server.shutdown().await;
                        Ok::<(), io::Error>(())
                    })
                    .await;
                    if incarnation == 1 {
                        finished.send_modify(|count| {
                            *count = count.checked_add(1).assured("two hosts finish")
                        });
                    }
                    result
                }
            });
            let client_finished = finished_tx.clone();
            simulation.host("client", move || {
                let credentials = client_credentials.clone();
                let mut ready = ready_rx.clone();
                let fresh = fresh_tx.clone();
                let done = done_tx.clone();
                let finished = client_finished.clone();
                let epochs = client_epochs.clone();
                let trace = client_trace.clone();
                async move {
                    let result = HostSupervisor::run(async move {
                        let (client, _incoming) = bind_with_incoming(
                            "client",
                            credentials,
                            seed.checked_add(2).assured("fixture seed fits"),
                        )
                        .await;
                        let peer =
                            ClusterNodeName::parse("server").assured("fixture node name is valid");
                        client.replace_live_nodes(&BTreeSet::from([
                            client.node_id().clone(),
                            peer.clone(),
                        ]));
                        wait_for_count(&mut ready, 1).await;
                        register_peer(&client, "server").await;
                        let sending = client.clone();
                        let target = peer.clone();
                        let first = tokio::spawn(async move {
                            sending
                                .send(
                                    &target,
                                    Envelope::RelayPayload(relay_payload(FIRST, 81, &sending)),
                                )
                                .await
                        });
                        wait_for_count(&mut ready, 2).await;
                        let first_outcome = first.await.assured("first relay task joins");
                        if let RestartMilestone::RuntimeAdmittedWithDelayedReply = milestone {
                            assert!(
                                first_outcome.is_ok(),
                                "the delayed reply confirms only historical body receipt"
                            );
                            trace.record(
                                "client",
                                "delayed reply confirmed historical body receipt",
                            );
                        } else {
                            assert!(first_outcome.is_err(), "crash hides the first response");
                        }
                        {
                            let identities = epochs.lock();
                            assert_eq!(identities.len(), 2);
                            assert_eq!(identities[0].coordinator(), identities[1].coordinator());
                            assert_ne!(
                                identities[0].process_epoch(),
                                identities[1].process_epoch()
                            );
                        }
                        trace.record(
                            "client",
                            "same node restarted with a distinct process epoch",
                        );
                        client.replace_outbound_targets(&Default::default());
                        register_peer(&client, "server").await;
                        wait_for_connection(&client, &peer).await;
                        if let RestartMilestone::RuntimeAdmittedWithDelayedReply = milestone {
                            assert_eq!(
                                client
                                    .relay_admission_status(&peer, FIRST)
                                    .await
                                    .assured("old admission resolves against the new epoch"),
                                RelayAdmissionStatus::Indeterminate
                            );
                        } else {
                            let error = match client
                                .send(
                                    &peer,
                                    Envelope::RelayPayload(relay_payload(FIRST, 81, &client)),
                                )
                                .await
                            {
                                Ok(()) => {
                                    panic!("the retained relay cannot cross the receiver epoch")
                                }
                                Err(error) => error,
                            };
                            assert!(
                                matches!(error, TransportError::RelayIndeterminate),
                                "{error:?}"
                            );
                        }
                        trace.record("client", "retained relay returned indeterminate");
                        fresh.send_replace(true);
                        client
                            .send(
                                &peer,
                                Envelope::RelayPayload(relay_payload(FRESH, 82, &client)),
                            )
                            .await
                            .assured("fresh relay reaches the restarted listener");
                        trace.record("client", "fresh relay body response received");
                        assert_eq!(
                            client
                                .relay_admission_status(&peer, FRESH)
                                .await
                                .assured("fresh admission status crosses the restarted transport"),
                            RelayAdmissionStatus::Admitted
                        );
                        trace.record("client", "fresh relay and authenticated status succeeded");
                        done.send_replace(true);
                        client.shutdown().await;
                        Ok::<(), io::Error>(())
                    })
                    .await;
                    finished.send_modify(|count| {
                        *count = count.checked_add(1).assured("two hosts finish")
                    });
                    result
                }
            });
            simulation.client("observer", async move {
                let mut finished = finished_rx;
                tokio::time::timeout(Duration::from_secs(60), async {
                    while *finished.borrow() < 2 {
                        tokio::task::consume_budget().await;
                        finished
                            .changed()
                            .await
                            .assured("fixture hosts remain alive");
                    }
                })
                .await
                .assured("restart fixture hosts finish within the simulated deadline");
                Ok(())
            });
        },
        move |simulation| {
            if control_crash_phase
                .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                simulation.crash("server");
                if let RestartMilestone::RuntimeAdmittedWithDelayedReply = milestone {
                    simulation.release("server", "client");
                } else {
                    simulation.repair_oneway("server", "client");
                }
                simulation.bounce("server");
            }
        },
    );
    result?;
    assert_eq!(
        crash_phase.load(Ordering::SeqCst),
        2,
        "the receiver crashed and restarted exactly once"
    );
    Ok(())
}

#[test]
fn relay_restart_fences_unresolved_delivery_and_accepts_fresh_work() {
    for milestone in [
        RestartMilestone::BodyReceived,
        RestartMilestone::RuntimeAdmitted,
        RestartMilestone::RuntimeAdmittedWithDelayedReply,
    ] {
        let scenario = Scenario {
            name: milestone.name(),
            fault_plan: milestone.plan(),
            seeds: milestone.seeds(),
        };
        scenario.check(restart_config, |run| {
            restarted_receiver_fences_unresolved_relay(milestone, run)
        });
    }
}
