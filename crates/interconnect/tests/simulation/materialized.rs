//! Layer: test harness outside the product layer order.
//! Owns: typed materialized branch state transfer across a simulated transport-owner replacement.
//! May depend on: the production interconnect, shared simulation driver and Arrow fixtures.
//! Must not know: server graphs, materialized snapshot decoding or disk durability.

use nervix_models::{RemoteRuntimeField, RemoteRuntimeValue};

use super::*;

fn placement(tenant: &str) -> StatePlacementEnvelope {
    StatePlacementEnvelope {
        domain: DomainName::parse("simulated").assured("valid fixture domain"),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Relay,
        identifier: ModelName::parse("profiles").assured("valid fixture relay"),
        branch_key: Some(vec![RemoteRuntimeField {
            name: "tenant".to_string(),
            value: RemoteRuntimeValue::String(tenant.to_string()),
        }]),
    }
}

fn serve(transport: &Transport, revision: u64) {
    transport
        .register_handler::<StateSyncRequest, _, _>(move |context, request| async move {
            assert_eq!(context.peer_node_id().as_str(), "client");
            assert!(
                request.placement == placement("acme") || request.placement == placement("beta")
            );
            assert_eq!(request.after_lsm, Some(revision - 1));
            StateSyncResponse {
                result: Ok(Some(StateSnapshotEnvelope {
                    lsm: revision,
                    payload: arrow_batch(),
                })),
            }
        })
        .assured("one typed state handler per owner lifetime");
    transport.replace_live_nodes(&BTreeSet::from([
        ClusterNodeName::parse("server").assured("valid fixture node"),
        ClusterNodeName::parse("client").assured("valid fixture node"),
    ]));
}

#[test]
fn materialized_branch_state_transfers_reconnect_to_the_replacement_owner() {
    Scenario {
        name: "materialized state owner replacement",
        fault_plan: "transfer two typed branches, end the serving transport, replace its listener \
                     and handler, then transfer both at the replacement revision",
        seeds: &[51, 53],
    }
    .check(config, exchange);
}

fn exchange(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let (ready_tx, ready_rx) = watch::channel(false);
    let (replaced_tx, replaced_rx) = watch::channel(false);
    let (replace_tx, replace_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let server_trace = run.trace();
    let client_trace = run.trace();
    run.simulate(move |simulation| {
        let server_finished = finished_tx.clone();
        simulation.host("server", move || {
            let credentials = server_credentials.clone();
            let ready = ready_tx.clone();
            let replaced = replaced_tx.clone();
            let mut replace = replace_rx.clone();
            let mut done = done_rx.clone();
            let finished = server_finished.clone();
            let trace = server_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let first = bind("server", credentials.clone(), seed).await;
                    serve(&first, 1);
                    ready.send_replace(true);
                    wait_for(&mut replace).await;
                    first.shutdown().await;
                    trace.record("server", "first materialized transport owner ended");
                    let current = bind("server", credentials, seed + 2).await;
                    serve(&current, 2);
                    replaced.send_replace(true);
                    wait_for(&mut done).await;
                    current.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });
        let client_finished = finished_tx.clone();
        simulation.host("client", move || {
            let credentials = client_credentials.clone();
            let mut ready = ready_rx.clone();
            let mut replaced = replaced_rx.clone();
            let replace = replace_tx.clone();
            let done = done_tx.clone();
            let finished = client_finished.clone();
            let trace = client_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let client = bind("client", credentials, seed + 1).await;
                    let peer = ClusterNodeName::parse("server").assured("valid fixture node");
                    client.replace_live_nodes(&BTreeSet::from([
                        client.node_id().clone(),
                        peer.clone(),
                    ]));
                    wait_for(&mut ready).await;
                    register_peer(&client, "server").await;
                    for revision in [1, 2] {
                        for tenant in ["acme", "beta"] {
                            let snapshot = nervix_primitives::time::timeout(HOST_DEADLINE, async {
                                loop {
                                    let response = client
                                        .request(
                                            &peer,
                                            StateSyncRequest {
                                                placement: placement(tenant),
                                                after_lsm: Some(revision - 1),
                                            },
                                        )
                                        .await;
                                    match response {
                                        Ok(response) => {
                                            break response
                                                .result
                                                .assured("typed state transfer succeeds")
                                                .assured("the current owner has the branch");
                                        }
                                        Err(_) if revision == 2 => {
                                            nervix_primitives::task::yield_now().await
                                        }
                                        Err(error) => panic!(
                                            "the first owner failed to transfer a branch: {error}"
                                        ),
                                    }
                                }
                            })
                            .await
                            .assured("replacement recovery is bounded by simulated time");
                            assert_eq!(snapshot.lsm, revision);
                            assert!(
                                snapshot.payload == arrow_batch(),
                                "the typed state payload transfers unchanged"
                            );
                            assert_eq!(decode_arrow(&snapshot.payload), 3);
                        }
                        trace.record(
                            "client",
                            if revision == 1 {
                                "both materialized branches transferred"
                            } else {
                                "both branches transferred at the replacement revision"
                            },
                        );
                        if revision == 1 {
                            replace.send_replace(true);
                            wait_for(&mut replaced).await;
                        }
                    }
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
