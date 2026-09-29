//! Backup capture and section transfer over the authenticated simulated transport.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** Coordinated capture and fetch assertions across a partition and sender identity
//!   mismatch.
//! - **Depends on.** The transport fixture and production backup wire requests.
//! - **Must not know.** Persistent state stores or an actual server graph.

use error_stack::Report;
use futures_util::{StreamExt as _, stream};
use nervix_execution::ChargedBytes;
use nervix_interconnect::{
    InterconnectStreamRequest, StreamHandlerError, StreamingResponse,
    backup::{
        CaptureDomainStateRequest, CaptureInventoryRequest, CapturedSectionInventory,
        CapturedStateSectionKind, FetchCapturedSection,
    },
};
use nervix_models::CoordinationIdentity;
use parking_lot::Mutex;

use super::*;

fn domain() -> DomainName {
    DomainName::parse("simulated").assured("fixture domain is valid")
}

fn fetch(coordination: CoordinationIdentity) -> FetchCapturedSection {
    FetchCapturedSection {
        coordination,
        domain: domain(),
        path: "state/guest.bin".to_string(),
    }
}

#[test]
fn backup_section_fetch_is_fenced_and_recovers_after_partition() {
    Scenario {
        name: "backup section transfer",
        fault_plan: "reject a sender's forged process identity before capture or fetch, bound a \
                     stalled fetch by its deadline, then partition and repair the link",
        seeds: &[101],
    }
    .check(fault_config, exercise_backup_section);
}

fn exercise_backup_section(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let (ready_tx, ready_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let server_trace = run.trace();
    let client_trace = run.trace();
    run.simulate(move |simulation| {
        let server_finished = finished_tx.clone();
        simulation.host("server", move || {
            let credentials = server_credentials.clone();
            let ready = ready_tx.clone();
            let mut done = done_rx.clone();
            let finished = server_finished.clone();
            let trace = server_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let server = bind("server", credentials, seed).await;
                    let staged = StdArc::new(Mutex::new(None::<Vec<u8>>));
                    let capture_stage = staged.clone();
                    server
                        .register_handler::<CaptureDomainStateRequest, _, _>(move |_, _| {
                            let staged = capture_stage.clone();
                            async move {
                                *staged.lock() = Some(vec![7, 11, 13, 17]);
                                Ok(())
                            }
                        })
                        .assured("capture handler registers");
                    let inventory_stage = staged.clone();
                    server
                        .register_handler::<CaptureInventoryRequest, _, _>(move |_, _| {
                            let staged = inventory_stage.clone();
                            async move {
                                Ok(staged
                                    .lock()
                                    .as_ref()
                                    .map(|bytes| CapturedSectionInventory {
                                        path: "state/guest.bin".to_string(),
                                        length: u64::try_from(bytes.len())
                                            .assured("fixture length fits"),
                                        digest: [7; 32],
                                        kind: CapturedStateSectionKind::WasmGuestBlob,
                                    })
                                    .into_iter()
                                    .collect())
                            }
                        })
                        .assured("inventory handler registers");
                    let fetch_stage = staged.clone();
                    let executor = Executor::default();
                    server
                        .register_stream_handler::<FetchCapturedSection, _, _>(move |_, request| {
                            let staged = fetch_stage.clone();
                            let executor = executor.clone();
                            async move {
                                if request.path == "state/stalled.bin" {
                                    std::future::pending::<()>().await;
                                }
                                let bytes = staged.lock().take().ok_or_else(|| {
                                    StreamHandlerError::new("capture stage is absent")
                                })?;
                                let length =
                                    u64::try_from(bytes.len()).assured("fixture length fits");
                                let charged = executor
                                    .try_charge_owned(MemoryClass::Bulk, bytes)
                                    .map_err(StreamHandlerError::with_cause)?;
                                Ok(StreamingResponse::new(
                                    length,
                                    stream::once(async move { Ok(charged) }),
                                ))
                            }
                        })
                        .assured("section handler registers");
                    server
                        .register_handler::<LivenessRequest, _, _>(|context, _| async move {
                            LivenessResponse {
                                peer: context.peer_node_id().clone(),
                            }
                        })
                        .assured("liveness handler registers");
                    server.replace_live_nodes(&BTreeSet::from([
                        server.node_id().clone(),
                        ClusterNodeName::parse("client").assured("fixture node is valid"),
                    ]));
                    ready.send_replace(true);
                    if !*done.borrow() {
                        tokio::time::timeout(Duration::from_secs(70), done.changed())
                            .await
                            .assured("the partitioned fetch finishes within its deadline")
                            .assured("the fixture client remains alive");
                    }
                    assert!(
                        staged.lock().is_none(),
                        "the verified fetch consumes the stage"
                    );
                    trace.record("server", "captured section consumed once");
                    server.shutdown().await;
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
            let done = done_tx.clone();
            let finished = client_finished.clone();
            let trace = client_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let client = bind("client", credentials, seed + 1).await;
                    let peer = ClusterNodeName::parse("server").assured("fixture node is valid");
                    client.replace_live_nodes(&BTreeSet::from([
                        client.node_id().clone(),
                        peer.clone(),
                    ]));
                    wait_for(&mut ready).await;
                    register_peer(&client, "server").await;
                    wait_for_connection(&client, &peer).await;
                    let coordination = client
                        .next_coordination_identity()
                        .assured("fixture can allocate coordination identity");
                    let forged = CoordinationIdentity::new(
                        client.node_id().clone(),
                        coordination.process_epoch() ^ 1,
                        coordination.sequence(),
                    );
                    let rejected = client
                        .request(
                            &peer,
                            CaptureDomainStateRequest {
                                coordination: forged.clone(),
                                domain: domain(),
                                revision: 1,
                                quiesced: true,
                            },
                        )
                        .await;
                    assert!(
                        rejected.is_err(),
                        "a forged capture must fail at authentication"
                    );
                    let missing = client
                        .request(
                            &peer,
                            CaptureInventoryRequest {
                                coordination: coordination.clone(),
                                domain: domain(),
                            },
                        )
                        .await
                        .assured("authenticated inventory succeeds")
                        .assured("fixture inventory succeeds");
                    assert!(
                        missing.is_empty(),
                        "rejected capture did not stage a section"
                    );
                    client
                        .request(
                            &peer,
                            CaptureDomainStateRequest {
                                coordination: coordination.clone(),
                                domain: domain(),
                                revision: 1,
                                quiesced: true,
                            },
                        )
                        .await
                        .assured("capture request arrives")
                        .assured("fixture capture succeeds");
                    let inventory = client
                        .request(
                            &peer,
                            CaptureInventoryRequest {
                                coordination: coordination.clone(),
                                domain: domain(),
                            },
                        )
                        .await
                        .assured("inventory request arrives")
                        .assured("fixture inventory succeeds");
                    assert_eq!(inventory.len(), 1);
                    assert_eq!(inventory[0].length, 4);
                    assert!(
                        client.request_stream(&peer, fetch(forged)).await.is_err(),
                        "a forged fetch must fail before its handler consumes the section"
                    );
                    trace.record("client", "forged capture and fetch rejected");
                    let stalled = FetchCapturedSection {
                        path: "state/stalled.bin".to_string(),
                        ..fetch(coordination.clone())
                    };
                    let started = turmoil::elapsed();
                    assert!(
                        client.request_stream(&peer, stalled).await.is_err(),
                        "a stalled fetch must fail at its deadline"
                    );
                    assert!(turmoil::elapsed() >= started + FetchCapturedSection::TIMEOUT);
                    trace.record("client", "stalled fetch reached its deadline");
                    turmoil::partition("client", "server");
                    assert!(
                        client
                            .request_stream(&peer, fetch(coordination.clone()))
                            .await
                            .is_err(),
                        "a partitioned fetch reaches its request deadline"
                    );
                    trace.record("client", "partitioned fetch failed");
                    turmoil::repair("client", "server");
                    wait_for_liveness_recovery(&client, &peer).await;
                    let mut stream = client
                        .request_stream(&peer, fetch(coordination))
                        .await
                        .assured("repaired fetch opens");
                    assert_eq!(stream.content_length(), 4);
                    let bytes = stream
                        .next_chunk()
                        .await
                        .assured("section chunk arrives")
                        .assured("section contains a chunk");
                    assert_eq!(bytes.as_ref(), &[7, 11, 13, 17]);
                    assert!(stream.next_chunk().await.assured("stream closes").is_none());
                    trace.record("client", "captured section verified after repair");
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
            tokio::time::timeout(Duration::from_secs(70), async {
                while *finished.borrow() < 2 {
                    tokio::task::consume_budget().await;
                    finished.changed().await.assured("fixture hosts stay alive");
                }
            })
            .await
            .assured("backup transfer finishes within its extended simulated deadline");
            Ok(())
        });
    })
}

#[test]
fn restart_during_backup_transfer_discards_the_process_stage() {
    Scenario {
        name: "backup owner restart",
        fault_plan: "restart the owner after the first streamed chunk, then reconnect; the new \
                     process has no captured section for the preceding cut",
        seeds: &[102],
    }
    .check(fault_config, exercise_backup_restart);
}

fn exercise_backup_restart(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let (ready_tx, ready_rx) = watch::channel(false);
    let (first_chunk_tx, first_chunk_rx) = watch::channel(false);
    let (restarted_tx, restarted_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let server_trace = run.trace();
    let client_trace = run.trace();
    run.simulate(move |simulation| {
        let server_finished = finished_tx.clone();
        simulation.host("server", move || {
            let credentials = server_credentials.clone();
            let ready = ready_tx.clone();
            let mut first_chunk = first_chunk_rx.clone();
            let restarted = restarted_tx.clone();
            let mut done = done_rx.clone();
            let finished = server_finished.clone();
            let trace = server_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let server = bind("server", credentials.clone(), seed).await;
                    let executor = Executor::default();
                    server
                        .register_stream_handler::<FetchCapturedSection, _, _>(move |_, _| {
                            let executor = executor.clone();
                            async move {
                                let charged = executor
                                    .try_charge_owned(MemoryClass::Bulk, vec![7, 11])
                                    .map_err(StreamHandlerError::with_cause)?;
                                let chunks = stream::once(async move {
                                    Ok::<_, Report<StreamHandlerError>>(charged)
                                })
                                .chain(stream::pending::<
                                    Result<ChargedBytes, Report<StreamHandlerError>>,
                                >());
                                Ok(StreamingResponse::new(4, chunks))
                            }
                        })
                        .assured("first owner registers fetch");
                    server.replace_live_nodes(&BTreeSet::from([
                        server.node_id().clone(),
                        ClusterNodeName::parse("client").assured("fixture node is valid"),
                    ]));
                    ready.send_replace(true);
                    wait_for(&mut first_chunk).await;
                    server.shutdown().await;
                    trace.record("server", "owner process ended mid-transfer");
                    let replacement = bind("server", credentials, seed + 2).await;
                    replacement
                        .register_stream_handler::<FetchCapturedSection, _, _>(|_, _| async {
                            Err(Report::new(StreamHandlerError::new(
                                "capture stage was discarded",
                            )))
                        })
                        .assured("replacement owner registers fetch");
                    replacement
                        .register_handler::<LivenessRequest, _, _>(|context, _| async move {
                            LivenessResponse {
                                peer: context.peer_node_id().clone(),
                            }
                        })
                        .assured("replacement owner registers liveness");
                    replacement.replace_live_nodes(&BTreeSet::from([
                        replacement.node_id().clone(),
                        ClusterNodeName::parse("client").assured("fixture node is valid"),
                    ]));
                    restarted.send_replace(true);
                    wait_for(&mut done).await;
                    replacement.shutdown().await;
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
            let first_chunk = first_chunk_tx.clone();
            let mut restarted = restarted_rx.clone();
            let done = done_tx.clone();
            let finished = client_finished.clone();
            let trace = client_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let client = bind("client", credentials, seed + 1).await;
                    let peer = ClusterNodeName::parse("server").assured("fixture node is valid");
                    client.replace_live_nodes(&BTreeSet::from([
                        client.node_id().clone(),
                        peer.clone(),
                    ]));
                    wait_for(&mut ready).await;
                    register_peer(&client, "server").await;
                    let coordination = client
                        .next_coordination_identity()
                        .assured("fixture can allocate coordination identity");
                    let mut body = client
                        .request_stream(&peer, fetch(coordination.clone()))
                        .await
                        .assured("first owner's fetch opens");
                    assert_eq!(body.content_length(), 4);
                    let first = body
                        .next_chunk()
                        .await
                        .assured("first chunk arrives")
                        .assured("first owner produces a chunk");
                    assert_eq!(first.as_ref(), &[7, 11]);
                    first_chunk.send_replace(true);
                    wait_for(&mut restarted).await;
                    assert!(
                        body.next_chunk().await.is_err(),
                        "an interrupted transfer cannot report a complete section"
                    );
                    trace.record("client", "incomplete transfer rejected");
                    wait_for_liveness_recovery(&client, &peer).await;
                    assert!(
                        client
                            .request_stream(&peer, fetch(coordination))
                            .await
                            .is_err(),
                        "the replacement owner has no stage from the first process"
                    );
                    trace.record("client", "replacement owner did not resurrect section");
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
