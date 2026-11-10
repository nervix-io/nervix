//! Layer: test harness outside the product layer order.
//! Owns: typed materialized, collected-window and inferencer branch state transfer across a
//!   partition, repair and transport-owner replacement. Native frame properties qualify their
//!   representation; the window fixture additionally compares complete nested Arrow lists.
//! May depend on: the production interconnect, shared simulation driver and Arrow fixtures.
//! Must not know: server graphs, materialized snapshot decoding or disk durability.

use arrow_array::{Array as _, ArrayRef, FixedSizeListArray, Int64Array, StringArray};
use error_stack::Report;
use futures_util::stream;
use nervix_interconnect::{
    FetchStateCheckpoint, IncomingByteStream, ReadMaterializedStatePage, RequestError,
    StateCheckpointRead, StreamHandlerError, StreamingResponse,
};
use nervix_models::{RemoteRuntimeField, RemoteRuntimeValue, StateInspectionBranchSelection, Unit};

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
        unit: nervix_models::Unit::SINGLE,
        branch_key: Some(vec![RemoteRuntimeField {
            name: "tenant".to_string(),
            value: RemoteRuntimeValue::String(tenant.to_string()),
        }]),
    }
}

/// Transport qualification treats checkpoint bytes as opaque. Native framing has its own
/// production-owner codec properties; this fixture distinguishes both branch and revision.
#[derive(Clone, Copy)]
enum CheckpointFixture {
    Materialized,
    MaterializedPage,
    Inferencer,
    Window,
}
impl CheckpointFixture {
    fn placement(self, tenant: &str) -> StatePlacementEnvelope {
        let mut placement = placement(tenant);
        if let Self::Inferencer = self {
            placement.state = RuntimeState::Inferencer {
                schema: SchemaFingerprint::from_digest([7; 32]),
                model: nervix_models::InferencerModelFingerprint::from_digest([9; 32]),
            };
            placement.kind = ModelKind::Inferencer;
            placement.identifier = ModelName::parse("running").assured("valid inferencer name");
        } else if let Self::Window = self {
            placement.state = RuntimeState::WindowProcessor {
                schema: SchemaFingerprint::from_digest([7; 32]),
            };
            placement.kind = ModelKind::WindowProcessor;
            placement.identifier = ModelName::parse("collected").assured("valid window name");
        }
        placement
    }
    fn payload(self, tenant: &str, revision: u64) -> Vec<u8> {
        match self {
            Self::Materialized | Self::MaterializedPage => large_arrow_batch(),
            Self::Inferencer => {
                let branch = if tenant == "acme" { 11_u8 } else { 110_u8 };
                let mut payload = vec![branch; 3 * 1024 * 1024];
                payload[..size_of::<u64>()].copy_from_slice(&revision.to_le_bytes());
                payload
            }
            Self::Window => {
                let batch = Self::window_batch(tenant, revision);
                let mut payload = Vec::new();
                let mut writer = StreamWriter::try_new(&mut payload, &batch.schema())
                    .assured("the fixture has a complete Arrow schema");
                writer.write(&batch).assured("the fixture lists encode");
                writer.finish().assured("the fixture stream finishes");
                payload
            }
        }
    }

    /// The complete scalar and list-argument sequences, distinct at each branch and generation.
    /// Padding ensures the transport fault interrupts a multi-chunk stream.
    fn window_batch(tenant: &str, revision: u64) -> RecordBatch {
        let values = match (tenant, revision) {
            ("acme", 1) => [10_i64, 20, 30],
            ("acme", 2) => [110, 120, 130],
            ("beta", 1) => [1000, 2000, 3000],
            ("beta", 2) => [1100, 2100, 3100],
            _ => unreachable!("the fixture has two branches and two generations"),
        };
        let pairs = values.map(|value| {
            [
                value,
                value
                    .checked_add(1)
                    .assured("fixture values are at most 3100"),
            ]
        });
        let scalar_values: ArrayRef = StdArc::new(Int64Array::from(values.to_vec()));
        let scalar = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", DataType::Int64, false)),
            3,
            scalar_values,
            None,
        )
        .assured("the scalar sequence has exactly three values");
        let pair_values: ArrayRef = StdArc::new(Int64Array::from(
            pairs.into_iter().flatten().collect::<Vec<_>>(),
        ));
        let pairs = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", DataType::Int64, false)),
            2,
            pair_values,
            None,
        )
        .assured("each retained argument has exactly two elements");
        let pair_type = pairs.data_type().clone();
        let pair_values: ArrayRef = StdArc::new(pairs);
        let nested = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", pair_type, false)),
            3,
            pair_values,
            None,
        )
        .assured("the nested sequence has exactly three retained arguments");
        let schema = StdArc::new(Schema::new(vec![
            Field::new("values", scalar.data_type().clone(), false),
            Field::new("pairs", nested.data_type().clone(), false),
            Field::new("padding", DataType::Utf8, false),
        ]));
        let scalar: ArrayRef = StdArc::new(scalar);
        let nested: ArrayRef = StdArc::new(nested);
        let padding: ArrayRef = StdArc::new(StringArray::from(vec!["x".repeat(3 * 1024 * 1024)]));
        RecordBatch::try_new(schema, vec![scalar, nested, padding])
            .assured("all sequence columns describe one complete window")
    }

    fn assert_window_payload(self, payload: &[u8], tenant: &str, revision: u64) {
        if let Self::Window = self {
            let mut reader = StreamReader::try_new(Cursor::new(payload), None)
                .assured("a transferred window keeps its Arrow framing");
            let batch = reader
                .next()
                .assured("the window has one batch")
                .assured("all scalar and nested columns decode");
            assert_eq!(batch, Self::window_batch(tenant, revision));
            assert!(reader.next().is_none(), "no window rows are duplicated");
        }
    }
}

fn serve(transport: &Transport, revision: u64, fixture: CheckpointFixture) {
    transport
        .register_handler::<StateSyncRequest, _, _>(move |context, request| async move {
            assert_eq!(context.peer_node_id().as_str(), "client");
            assert!(
                request.placement == fixture.placement("acme")
                    || request.placement == fixture.placement("beta")
            );
            assert_eq!(request.after_lsm, Some(revision - 1));
            let tenant = if request.placement == fixture.placement("acme") {
                "acme"
            } else {
                "beta"
            };
            let payload = fixture.payload(tenant, revision);
            StateSyncResponse {
                result: Ok(Some(StateSnapshotEnvelope {
                    lsm: revision,
                    length: u64::try_from(payload.len()).assured("batch length fits u64"),
                    digest: *blake3::hash(&payload).as_bytes(),
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
                    request.placement == fixture.placement("acme")
                        || request.placement == fixture.placement("beta")
                );
                let tenant = if request.placement == fixture.placement("acme") {
                    "acme"
                } else {
                    "beta"
                };
                let payload = fixture.payload(tenant, revision);
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
    let executor = Executor::default();
    transport
        .register_stream_handler::<ReadMaterializedStatePage, _, _>(move |context, request| {
            let executor = executor.clone();
            async move {
                assert_eq!(context.peer_node_id().as_str(), "client");
                assert_eq!(request.control_revision, revision);
                assert_eq!(request.unit, Unit::SINGLE);
                assert_eq!(request.schema, SchemaFingerprint::from_digest([7; 32]));
                assert_eq!(request.max_rows, 3);
                assert_eq!(request.selection, StateInspectionBranchSelection::All);
                let payload = large_arrow_batch();
                assert!(u64::try_from(payload.len()).assured("fixture size") <= request.max_bytes);
                let bytes = executor
                    .charge_owned(MemoryClass::Bulk, payload)
                    .await
                    .map_err(StreamHandlerError::with_cause)?;
                Ok(StreamingResponse::new(
                    u64::try_from(bytes.len()).assured("fixture size"),
                    stream::unfold((bytes, 0), |(bytes, offset)| async move {
                        if offset >= bytes.len() {
                            return None;
                        }
                        if offset != 0 {
                            nervix_primitives::time::sleep(Duration::from_millis(2)).await;
                        }
                        let end = offset.saturating_add(64 * 1024).min(bytes.len());
                        let chunk = bytes.slice(offset, end).assured("bounded fixture chunk");
                        Some((Ok(chunk), (bytes, end)))
                    }),
                ))
            }
        })
        .assured("one inspection page handler per owner lifetime");
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
    exchange_fixture(run, CheckpointFixture::Materialized)
}

#[test]
fn materialized_inspection_pages_recover_after_partition_and_lost_stream_chunks() {
    Scenario {
        name: "bounded materialized inspection page",
        fault_plan: "lose a page stream's remaining chunks under partition, repair, replace the \
                     transport, then read a bounded page again",
        seeds: &[51],
    }
    .check(checkpoint_config, |run| {
        exchange_fixture(run, CheckpointFixture::MaterializedPage)
    });
}

async fn open_state_stream(
    client: &Transport,
    peer: &ClusterNodeName,
    revision: u64,
    tenant: &str,
    fixture: CheckpointFixture,
) -> Result<IncomingByteStream, Report<RequestError>> {
    if let CheckpointFixture::MaterializedPage = fixture {
        client
            .request_stream_with_timeout(
                peer,
                ReadMaterializedStatePage {
                    domain: DomainName::parse("simulated").assured("fixture domain"),
                    relay: RelayName::parse("profiles").assured("fixture relay"),
                    unit: Unit::SINGLE,
                    control_revision: revision,
                    schema: SchemaFingerprint::from_digest([7; 32]),
                    selection: StateInspectionBranchSelection::All,
                    after: None,
                    max_rows: 3,
                    max_bytes: 4 * 1024 * 1024,
                    target: ClusterNodeIdentity::new(
                        peer.clone(),
                        ClusterNodeIncarnation::new(revision),
                    ),
                },
                Duration::from_secs(10),
            )
            .await
    } else {
        client
            .request_stream(
                peer,
                FetchStateCheckpoint {
                    placement: fixture.placement(tenant),
                    lsm: revision,
                    read: StateCheckpointRead::Published,
                },
            )
            .await
    }
}

fn exchange_fixture(run: ScenarioRun, fixture: CheckpointFixture) -> Result<(), SimulationError> {
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
                    serve(&first, 1, fixture);
                    ready.send_replace(true);
                    wait_for_transfer(&mut replace).await;
                    first.shutdown().await;
                    trace.record("server", "first materialized transport owner ended");
                    let current = bind("server", credentials, seed + 2).await;
                    serve(&current, 2, fixture);
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
                                                    placement: fixture.placement(tenant),
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
                            // The partition below drops what the first owner still had in
                            // flight on the bulk connection, and the simulated TCP never resends
                            // it, so that connection stays half-open after the repair. Its first
                            // request at the replacement revision can meet the replaced owner's
                            // reset; the next lease reconnects to the current owner.
                            let mut body =
                                nervix_primitives::time::timeout(Duration::from_secs(90), async {
                                    loop {
                                        let opened = open_state_stream(
                                            &client, &peer, revision, tenant, fixture,
                                        )
                                        .await;
                                        match opened {
                                            Ok(body) => break body,
                                            Err(_) if revision == 2 => {
                                                nervix_primitives::task::yield_now().await
                                            }
                                            Err(error) => panic!(
                                                "the first owner failed to open a checkpoint \
                                                 stream: {error}"
                                            ),
                                        }
                                    }
                                })
                                .await
                                .assured("replacement recovery is bounded by simulated time");
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
                                payload == fixture.payload(tenant, revision),
                                "the typed state payload transfers unchanged"
                            );
                            assert_eq!(*blake3::hash(&payload).as_bytes(), snapshot.digest);
                            fixture.assert_window_payload(&payload, tenant, revision);
                            if let CheckpointFixture::Materialized
                            | CheckpointFixture::MaterializedPage = fixture
                            {
                                assert_eq!(decode_arrow(&payload), 3);
                            }
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
                            let mut interrupted =
                                open_state_stream(&client, &peer, snapshot.lsm, "acme", fixture)
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

#[test]
fn inferencer_branch_state_transfers_reconnect_to_the_replacement_owner() {
    Scenario {
        name: "inferencer state owner replacement",
        fault_plan: "lose a multi-chunk inference checkpoint stream under partition, repair, \
                     replace the transport owner, and verify complete distinct branch and \
                     revision bytes",
        seeds: &[51, 53],
    }
    .check(checkpoint_config, |run| {
        exchange_fixture(run, CheckpointFixture::Inferencer)
    });
}

#[test]
fn collected_window_sequences_survive_partition_and_owner_replacement() {
    Scenario {
        name: "collected window state owner replacement",
        fault_plan: "interrupt a multi-chunk collected-window generation under partition, repair \
                     and replace its transport owner, then compare complete scalar and nested \
                     list columns for both branches at both generations",
        seeds: &[61],
    }
    .check(checkpoint_config, |run| {
        exchange_fixture(run, CheckpointFixture::Window)
    });
}
