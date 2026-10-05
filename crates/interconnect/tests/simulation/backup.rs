//! Backup capture and section transfer over the authenticated simulated transport.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** Bounded multi-section capture and install assertions across a partition, sender
//!   identity mismatch, and receiving-process restart.
//! - **Depends on.** The transport fixture and production backup wire requests.
//! - **Must not know.** Persistent state stores or an actual server graph.

use error_stack::Report;
use futures_util::{StreamExt as _, stream};
use nervix_execution::ChargedBytes;
use nervix_interconnect::{
    InterconnectStreamRequest, StreamHandlerError, StreamingResponse,
    backup::{
        CaptureDomainStateRequest, CaptureInventoryRequest, CapturedSectionInventory,
        CapturedStateSectionKind, FetchCapturedSection, InstallRestoredStateAction,
        InstallRestoredStateRequest, RestoreStateInventory,
    },
};
use nervix_models::{CommandExecutionReference, CoordinationIdentity, RestoreStateAuthority};
use nervix_primitives::sync::blocking::Mutex;

use super::*;

const SECTION_COUNT: usize = 6;
const SECTION_BYTES: usize = 6 * 1024 * 1024;
const CHUNK_BYTES: usize = 64 * 1024;
const CONTAINER_BYTES: usize = SECTION_COUNT * SECTION_BYTES;
const TRANSFER_DEADLINE: Duration = Duration::from_secs(160);

fn backup_transfer_config(seed: u64) -> SimulationConfig {
    let mut config = fault_config(seed);
    config.bounds.simulated_duration = Duration::from_secs(180);
    config.bounds.max_steps =
        NonZeroUsize::new(200_000).assured("the large transfer step bound is nonzero");
    config
}

fn section_path(index: usize) -> String {
    format!("domains/simulated/state/materialized_relay/state/groups/{index:010}/columns.arrow")
}

fn section_byte(index: usize) -> u8 {
    u8::try_from(index + 7).assured("the fixture has six sections")
}

fn container_digest() -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    for index in 0..SECTION_COUNT {
        let chunk = [section_byte(index); CHUNK_BYTES];
        for _ in 0..SECTION_BYTES / CHUNK_BYTES {
            hash.update(&chunk);
        }
    }
    *hash.finalize().as_bytes()
}

fn section_digest(index: usize) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    for _ in 0..SECTION_BYTES / CHUNK_BYTES {
        hash.update(&[section_byte(index); CHUNK_BYTES]);
    }
    *hash.finalize().as_bytes()
}

fn install_request(
    coordination: CoordinationIdentity,
    action: InstallRestoredStateAction,
) -> InstallRestoredStateRequest {
    InstallRestoredStateRequest {
        coordination,
        domain: domain(),
        authority: RestoreStateAuthority {
            leader: ClusterNodeName::parse("client").assured("fixture node is valid"),
            term: 1,
            execution: CommandExecutionReference::parse("restore-materialized")
                .assured("fixture execution is valid"),
            mutation_revision: 13,
            generation: 37,
        },
        action,
    }
}

fn begin_install() -> InstallRestoredStateAction {
    InstallRestoredStateAction::Begin {
        placement: StatePlacementEnvelope {
            domain: domain(),
            state: RuntimeState::MaterializedRelay {
                schema: SchemaFingerprint::from_digest([11; 32]).materialized_at(37),
            },
            kind: ModelKind::Relay,
            identifier: ModelName::parse("state").assured("fixture relay is valid"),
            branch_key: None,
        },
        branch_fingerprint: None,
        revision: 80,
        length: u64::try_from(CONTAINER_BYTES).assured("fixture length fits"),
        digest: container_digest(),
    }
}

#[derive(Default)]
struct InstallReceiver {
    incoming: Option<(u64, blake3::Hasher)>,
    finished: bool,
    published: bool,
}

impl InstallReceiver {
    fn apply(&mut self, action: InstallRestoredStateAction) -> Result<(), RemoteOperationFailure> {
        let reject = || RemoteOperationFailure::rejected(RemoteOperationSubject::domain(&domain()));
        match action {
            action @ InstallRestoredStateAction::Begin { .. } => {
                assert_eq!(action, begin_install());
                self.incoming = Some((0, blake3::Hasher::new()));
                self.finished = false;
            }
            InstallRestoredStateAction::Chunk { offset, payload } => {
                let (received, hash) = self.incoming.as_mut().ok_or_else(reject)?;
                assert_eq!(offset, *received);
                assert!(payload.len() <= CHUNK_BYTES);
                *received += u64::try_from(payload.len()).assured("bounded chunk fits");
                assert!(*received <= u64::try_from(CONTAINER_BYTES).assured("fixture length fits"));
                hash.update(&payload);
            }
            InstallRestoredStateAction::Finish => {
                let (received, hash) = self.incoming.take().ok_or_else(reject)?;
                assert_eq!(
                    received,
                    u64::try_from(CONTAINER_BYTES).assured("fixture length fits")
                );
                assert_eq!(hash.finalize().as_bytes(), &container_digest());
                self.finished = true;
            }
            InstallRestoredStateAction::Publish { inventory } => {
                if !self.finished {
                    return Err(reject());
                }
                assert_eq!(inventory.checkpoints, 1);
                assert_eq!(
                    inventory.payload_bytes,
                    u64::try_from(CONTAINER_BYTES).assured("fixture length fits")
                );
                self.published = true;
            }
        }
        Ok(())
    }
}

fn register_install_receiver(server: &Transport) -> StdArc<Mutex<InstallReceiver>> {
    let receiver = StdArc::new(Mutex::new(InstallReceiver::default()));
    let handler_receiver = receiver.clone();
    server
        .register_handler::<InstallRestoredStateRequest, _, _>(move |_, request| {
            let receiver = handler_receiver.clone();
            async move { receiver.lock().apply(request.action) }
        })
        .assured("materialized install receiver registers");
    receiver
}

async fn install_action(
    client: &Transport,
    peer: &ClusterNodeName,
    coordination: &CoordinationIdentity,
    action: InstallRestoredStateAction,
) {
    client
        .request(peer, install_request(coordination.clone(), action))
        .await
        .assured("bounded install request arrives")
        .assured("bounded install action succeeds");
}

async fn publish_install(
    client: &Transport,
    peer: &ClusterNodeName,
    coordination: &CoordinationIdentity,
) {
    install_action(
        client,
        peer,
        coordination,
        InstallRestoredStateAction::Finish,
    )
    .await;
    install_action(
        client,
        peer,
        coordination,
        InstallRestoredStateAction::Publish {
            inventory: RestoreStateInventory {
                checkpoints: 1,
                payload_bytes: u64::try_from(CONTAINER_BYTES).assured("fixture length fits"),
            },
        },
    )
    .await;
}

fn domain() -> DomainName {
    DomainName::parse("simulated").assured("fixture domain is valid")
}

fn fetch(coordination: CoordinationIdentity) -> FetchCapturedSection {
    FetchCapturedSection {
        coordination,
        domain: domain(),
        path: section_path(0),
    }
}

#[test]
fn backup_section_fetch_is_fenced_and_recovers_after_partition() {
    Scenario {
        name: "backup section transfer",
        fault_plan: "reject forged identities and bound stalled and partitioned fetches, then \
                     install six streamed sections totaling 36 MiB after repairing the link",
        seeds: &[101],
    }
    .check(backup_transfer_config, exercise_backup_section);
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
                    let staged = StdArc::new(Mutex::new(BTreeSet::<usize>::new()));
                    let installed = register_install_receiver(&server);
                    let capture_stage = staged.clone();
                    server
                        .register_handler::<CaptureDomainStateRequest, _, _>(move |_, _| {
                            let staged = capture_stage.clone();
                            async move {
                                *staged.lock() = (0..SECTION_COUNT).collect();
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
                                    .iter()
                                    .map(|index| CapturedSectionInventory {
                                        path: section_path(*index),
                                        length: u64::try_from(SECTION_BYTES)
                                            .assured("fixture length fits"),
                                        digest: section_digest(*index),
                                        kind: CapturedStateSectionKind::MaterializedColumns,
                                    })
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
                                let index = (0..SECTION_COUNT)
                                    .find(|index| section_path(*index) == request.path)
                                    .filter(|index| staged.lock().remove(index))
                                    .ok_or_else(|| {
                                        StreamHandlerError::new("capture stage is absent")
                                    })?;
                                let length =
                                    u64::try_from(SECTION_BYTES).assured("fixture length fits");
                                Ok(StreamingResponse::new(
                                    length,
                                    stream::unfold(
                                        (SECTION_BYTES, executor),
                                        move |(remaining, executor)| async move {
                                            if remaining == 0 {
                                                return None;
                                            }
                                            let size = remaining.min(CHUNK_BYTES);
                                            let chunk = executor
                                                .try_charge_owned(
                                                    MemoryClass::Bulk,
                                                    vec![section_byte(index); size],
                                                )
                                                .map_err(StreamHandlerError::with_cause);
                                            Some((chunk, (remaining - size, executor)))
                                        },
                                    ),
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
                        nervix_primitives::time::timeout(TRANSFER_DEADLINE, done.changed())
                            .await
                            .assured("the partitioned fetch finishes within its deadline")
                            .assured("the fixture client remains alive");
                    }
                    assert!(
                        staged.lock().is_empty(),
                        "the verified fetch consumes the stage"
                    );
                    assert!(installed.lock().published);
                    trace.record(
                        "server",
                        "six captured sections consumed and installed once",
                    );
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
                    assert_eq!(inventory.len(), SECTION_COUNT);
                    assert_eq!(
                        inventory[0].length,
                        u64::try_from(SECTION_BYTES).assured("fixture length fits")
                    );
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
                    let mut offset = 0;
                    for (index, section) in inventory.iter().enumerate() {
                        let mut stream = client
                            .request_stream(
                                &peer,
                                FetchCapturedSection {
                                    path: section.path.clone(),
                                    ..fetch(coordination.clone())
                                },
                            )
                            .await
                            .assured("repaired fetch opens");
                        assert_eq!(stream.content_length(), section.length);
                        let mut hash = blake3::Hasher::new();
                        let mut length = 0;
                        while let Some(bytes) = stream
                            .next_chunk()
                            .await
                            .assured("bounded section chunk arrives")
                        {
                            assert!(bytes.len() <= CHUNK_BYTES);
                            assert!(bytes.iter().all(|byte| *byte == section_byte(index)));
                            hash.update(bytes.as_ref());
                            length += u64::try_from(bytes.len()).assured("bounded chunk fits");
                            offset += u64::try_from(bytes.len()).assured("bounded chunk fits");
                        }
                        assert_eq!(length, section.length);
                        assert_eq!(hash.finalize().as_bytes(), &section.digest);
                    }
                    assert_eq!(
                        offset,
                        u64::try_from(CONTAINER_BYTES).assured("fixture length fits")
                    );
                    // Fetch and install use the same reserved snapshot stream slot. Complete
                    // capture first, as the production archive staging boundary does.
                    install_action(&client, &peer, &coordination, begin_install()).await;
                    let mut offset = 0;
                    for index in 0..SECTION_COUNT {
                        for _ in 0..SECTION_BYTES / CHUNK_BYTES {
                            install_action(
                                &client,
                                &peer,
                                &coordination,
                                InstallRestoredStateAction::Chunk {
                                    offset,
                                    payload: vec![section_byte(index); CHUNK_BYTES],
                                },
                            )
                            .await;
                            offset += u64::try_from(CHUNK_BYTES).assured("bounded chunk fits");
                        }
                    }
                    publish_install(&client, &peer, &coordination).await;
                    trace.record(
                        "client",
                        "36 MiB capture verified and installed after repair",
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
            nervix_primitives::time::timeout(TRANSFER_DEADLINE, async {
                while *finished.borrow() < 2 {
                    nervix_primitives::task::consume_budget().await;
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
        fault_plan: "restart the capture owner and install receiver after the first 64 KiB chunk; \
                     reconnect and install all 36 MiB under a new complete transfer",
        seeds: &[102],
    }
    .check(backup_transfer_config, exercise_backup_restart);
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
                    let interrupted_install = register_install_receiver(&server);
                    let executor = Executor::default();
                    server
                        .register_stream_handler::<FetchCapturedSection, _, _>(move |_, _| {
                            let executor = executor.clone();
                            async move {
                                let charged = executor
                                    .try_charge_owned(
                                        MemoryClass::Bulk,
                                        vec![section_byte(0); CHUNK_BYTES],
                                    )
                                    .map_err(StreamHandlerError::with_cause)?;
                                let chunks = stream::once(async move {
                                    Ok::<_, Report<StreamHandlerError>>(charged)
                                })
                                .chain(stream::pending::<
                                    Result<ChargedBytes, Report<StreamHandlerError>>,
                                >());
                                Ok(StreamingResponse::new(
                                    u64::try_from(CONTAINER_BYTES).assured("fixture length fits"),
                                    chunks,
                                ))
                            }
                        })
                        .assured("first owner registers fetch");
                    server.replace_live_nodes(&BTreeSet::from([
                        server.node_id().clone(),
                        ClusterNodeName::parse("client").assured("fixture node is valid"),
                    ]));
                    ready.send_replace(true);
                    wait_for(&mut first_chunk).await;
                    assert_eq!(
                        interrupted_install
                            .lock()
                            .incoming
                            .as_ref()
                            .assured("the first install chunk reached the receiver")
                            .0,
                        u64::try_from(CHUNK_BYTES).assured("bounded chunk fits")
                    );
                    server.shutdown().await;
                    trace.record("server", "owner and receiver process ended mid-transfer");
                    let replacement = bind("server", credentials, seed + 2).await;
                    let replacement_install = register_install_receiver(&replacement);
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
                    if !*done.borrow() {
                        nervix_primitives::time::timeout(TRANSFER_DEADLINE, done.changed())
                            .await
                            .assured("replacement receives a complete bounded transfer")
                            .assured("fixture client remains alive");
                    }
                    assert!(replacement_install.lock().published);
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
                    install_action(&client, &peer, &coordination, begin_install()).await;
                    install_action(
                        &client,
                        &peer,
                        &coordination,
                        InstallRestoredStateAction::Chunk {
                            offset: 0,
                            payload: vec![section_byte(0); CHUNK_BYTES],
                        },
                    )
                    .await;
                    trace.record("client", "first bounded install chunk acknowledged");
                    let mut body = client
                        .request_stream(&peer, fetch(coordination.clone()))
                        .await
                        .assured("first owner's fetch opens");
                    assert_eq!(
                        body.content_length(),
                        u64::try_from(CONTAINER_BYTES).assured("fixture length fits")
                    );
                    let mut first = Vec::with_capacity(CHUNK_BYTES);
                    while first.len() < CHUNK_BYTES {
                        let chunk = body
                            .next_chunk()
                            .await
                            .assured("first bounded chunk arrives")
                            .assured("first owner produces its declared chunk");
                        first.extend_from_slice(chunk.as_ref());
                    }
                    assert_eq!(first.len(), CHUNK_BYTES);
                    assert!(first.iter().all(|byte| *byte == section_byte(0)));
                    first_chunk.send_replace(true);
                    wait_for(&mut restarted).await;
                    assert!(
                        body.next_chunk().await.is_err(),
                        "an interrupted transfer cannot report a complete section"
                    );
                    drop(body);
                    trace.record("client", "incomplete transfer rejected");
                    wait_for_liveness_recovery(&client, &peer).await;
                    assert!(
                        client
                            .request_stream(&peer, fetch(coordination.clone()))
                            .await
                            .is_err(),
                        "the replacement owner has no stage from the first process"
                    );
                    trace.record("client", "replacement owner did not resurrect section");
                    assert!(
                        client
                            .request(
                                &peer,
                                install_request(
                                    coordination.clone(),
                                    InstallRestoredStateAction::Finish
                                )
                            )
                            .await
                            .assured("replacement receives finish")
                            .is_err(),
                        "a replacement receiver cannot finish the preceding process's partial \
                         install"
                    );
                    install_action(&client, &peer, &coordination, begin_install()).await;
                    let mut offset = 0;
                    for index in 0..SECTION_COUNT {
                        for _ in 0..SECTION_BYTES / CHUNK_BYTES {
                            install_action(
                                &client,
                                &peer,
                                &coordination,
                                InstallRestoredStateAction::Chunk {
                                    offset,
                                    payload: vec![section_byte(index); CHUNK_BYTES],
                                },
                            )
                            .await;
                            offset += u64::try_from(CHUNK_BYTES).assured("bounded chunk fits");
                        }
                    }
                    publish_install(&client, &peer, &coordination).await;
                    trace.record(
                        "client",
                        "replacement receiver published the complete 36 MiB transfer",
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
            nervix_primitives::time::timeout(TRANSFER_DEADLINE, async {
                while *finished.borrow() < 2 {
                    nervix_primitives::task::consume_budget().await;
                    finished
                        .changed()
                        .await
                        .assured("fixture hosts remain alive");
                }
            })
            .await
            .assured("receiver restart and complete transfer finish within their bound");
            Ok(())
        });
    })
}
