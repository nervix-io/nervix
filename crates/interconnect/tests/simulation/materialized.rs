//! Layer: test harness outside the product layer order.
//! Owns: typed materialized branch state transfer across a partition, repair and transport-owner
//!   replacement.
//! May depend on: the production interconnect, shared simulation driver and Arrow fixtures.
//! Must not know: server graphs, materialized snapshot decoding or disk durability.

use futures_util::stream;
use nervix_interconnect::{
    FetchStateCheckpoint, StateCheckpointRead, StreamHandlerError, StreamingResponse,
};
use nervix_models::{RemoteRuntimeField, RemoteRuntimeValue};

use super::*;

fn large_arrow_batch() -> Vec<u8> {
    let schema = StdArc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, false),
        Field::new("guest_save", DataType::Utf8, false),
    ]));
    let values: arrow_array::ArrayRef = StdArc::new(Int32Array::from(vec![7, 11, 13]));
    let save = "x".repeat(3 * 1024 * 1024);
    let saves: arrow_array::ArrayRef =
        StdArc::new(arrow_array::StringArray::from(vec![save.as_str(), "", ""]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values, saves])
        .assured("large fixture columns match their Arrow schema");
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema)
        .assured("large fixture Arrow schema can be encoded");
    writer.write(&batch).assured("large fixture can be encoded");
    writer.finish().assured("large fixture stream can finish");
    assert!(bytes.len() > 2 * 1024 * 1024);
    bytes
}

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
                    length: u64::try_from(large_arrow_batch().len())
                        .assured("batch length fits u64"),
                    digest: *blake3::hash(&large_arrow_batch()).as_bytes(),
                })),
            }
        })
        .assured("one typed state handler per owner lifetime");
    let executor = Executor::default();
    transport
        .register_stream_handler::<FetchStateCheckpoint, _, _>(move |context, request| {
            let executor = executor.clone();
            async move {
                assert_eq!(context.peer_node_id().as_str(), "client");
                assert_eq!(request.lsm, revision);
                assert!(
                    request.placement == placement("acme")
                        || request.placement == placement("beta")
                );
                let payload = large_arrow_batch();
                let length = u64::try_from(payload.len()).assured("batch length fits u64");
                let bytes = executor
                    .charge_owned(MemoryClass::RestoreMetadata, payload)
                    .await
                    .map_err(StreamHandlerError::with_cause)?;
                Ok(StreamingResponse::new(
                    length,
                    stream::unfold((bytes, 0), |(bytes, offset)| async move {
                        if offset >= bytes.len() {
                            return None;
                        }
                        if offset != 0 {
                            // Give the client a deterministic turn to interrupt a live stream
                            // after its first chunk, rather than racing a buffered response.
                            nervix_primitives::time::sleep(Duration::from_millis(2)).await;
                        }
                        let end = offset.saturating_add(64 * 1024).min(bytes.len());
                        let chunk = bytes
                            .slice(offset, end)
                            .assured("a bounded chunk is inside the fixture allocation");
                        Some((Ok(chunk), (bytes, end)))
                    }),
                ))
            }
        })
        .assured("one typed checkpoint stream handler per owner lifetime");
    transport.replace_live_nodes(&BTreeSet::from([
        ClusterNodeName::parse("server").assured("valid fixture node"),
        ClusterNodeName::parse("client").assured("valid fixture node"),
    ]));
}

fn checkpoint_config(seed: u64) -> SimulationConfig {
    let mut simulation = config(seed);
    // One bulk fetch can reach its sixty-second deadline before connection recovery begins.
    simulation.bounds.simulated_duration = Duration::from_secs(180);
    simulation.bounds.max_steps = NonZeroUsize::new(200_000).assured("the step bound is nonzero");
    simulation.bounds.wall_duration = Duration::from_secs(120);
    simulation
}

async fn wait_for_transfer(signal: &mut watch::Receiver<bool>) {
    if !*signal.borrow() {
        nervix_primitives::time::timeout(Duration::from_secs(120), signal.changed())
            .await
            .assured("the checkpoint transfer completes within the simulated deadline")
            .assured("the transfer peer remains alive");
    }
}

#[test]
fn materialized_branch_state_transfers_reconnect_to_the_replacement_owner() {
    Scenario {
        name: "materialized state owner replacement",
        fault_plan: "partition the first multi-chunk fetch after one chunk and recover it after \
                     repair, transfer two typed branches, replace the owner transport and repeat",
        seeds: &[51, 53],
    }
    .check(checkpoint_config, exchange);
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
                    wait_for_transfer(&mut replace).await;
                    first.shutdown().await;
                    trace.record("server", "first materialized transport owner ended");
                    let current = bind("server", credentials, seed + 2).await;
                    serve(&current, 2);
                    replaced.send_replace(true);
                    wait_for_transfer(&mut done).await;
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
                        let mut fault_snapshot = None;
                        for tenant in ["acme", "beta"] {
                            let snapshot =
                                nervix_primitives::time::timeout(Duration::from_secs(90), async {
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
                                                "the first owner failed to transfer a branch: \
                                                 {error}"
                                            ),
                                        }
                                    }
                                })
                                .await
                                .assured("replacement recovery is bounded by simulated time");
                            assert_eq!(snapshot.lsm, revision);
                            if revision == 1 && tenant == "acme" {
                                fault_snapshot = Some(snapshot.clone());
                            }
                            let mut body = client
                                .request_stream(
                                    &peer,
                                    FetchStateCheckpoint {
                                        placement: placement(tenant),
                                        lsm: snapshot.lsm,
                                        read: StateCheckpointRead::Published,
                                    },
                                )
                                .await
                                .assured("the selected checkpoint opens as a bulk stream");
                            assert_eq!(body.content_length(), snapshot.length);
                            let mut payload = Vec::new();
                            while let Some(chunk) = body
                                .next_chunk()
                                .await
                                .assured("the checkpoint stream completes")
                            {
                                payload.extend_from_slice(chunk.as_ref());
                            }
                            assert!(
                                payload == large_arrow_batch(),
                                "the typed state payload transfers unchanged"
                            );
                            assert_eq!(*blake3::hash(&payload).as_bytes(), snapshot.digest);
                            assert_eq!(decode_arrow(&payload), 3);
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
                            let snapshot =
                                fault_snapshot.assured("the first branch was transferred");
                            let mut interrupted = client
                                .request_stream(
                                    &peer,
                                    FetchStateCheckpoint {
                                        placement: placement("acme"),
                                        lsm: snapshot.lsm,
                                        read: StateCheckpointRead::Published,
                                    },
                                )
                                .await
                                .assured("the checkpoint stream opens before the fault");
                            let first = interrupted
                                .next_chunk()
                                .await
                                .assured("the first frame arrives")
                                .assured("the checkpoint has more than one frame");
                            assert!(!first.is_empty() && first.len() <= 64 * 1024);
                            turmoil::partition("client", "server");
                            loop {
                                match interrupted.next_chunk().await {
                                    Err(_) => break,
                                    Ok(Some(_)) => {}
                                    Ok(None) => panic!("a partitioned checkpoint cannot complete"),
                                }
                            }
                            drop(interrupted);
                            trace.record("client", "partial checkpoint stream failed");
                            turmoil::repair("client", "server");
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
            nervix_primitives::time::timeout(Duration::from_secs(170), async {
                while *finished.borrow() < 2 {
                    nervix_primitives::task::consume_budget().await;
                    finished.changed().await.assured("both hosts remain alive");
                }
            })
            .await
            .assured("both materialized transfer hosts finish after link repair");
            Ok(())
        });
    })
}
