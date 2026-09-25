//! Production TLS and HTTP/2 exchanges over Turmoil TCP and DNS.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** Bounded typed handlers, Arrow fixtures, host synchronization, and transport tests.
//! - **Depends on.** The production interconnect API, Turmoil, and the simulation runner.
//! - **Must not know.** Server graphs, persistent stores, gossip, or external connectors.

use std::{
    collections::BTreeSet,
    io::{self, Cursor},
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::Arc as StdArc,
    time::{Duration, SystemTime},
};

use arrow_array::{Int32Array, RecordBatch};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Executor, MemoryClass};
use nervix_interconnect::{
    ConnectionFailureReason, Envelope, InterconnectRequest, PeerTarget, PoolClass,
    ReceivedEnvelope, RelayDelivery, RelayPayload, RelayPayloadKind, RemoteOperationFailure,
    RemoteOperationSubject, RequestError, RequestSubquota, TlsConfigBundle, Transport,
    TransportClock, TransportEntropy, TransportError, TransportOptions,
};
use nervix_models::{ClusterNodeName, DomainName, NodeEndpoint, RelayName, RemoteAckRegistration};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType, date_time_ymd,
};
use rkyv::{Archive, Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use super::runner::{
    ClockSkew, HostSupervisor, SemanticTrace, SimulatedEntropy, SimulatedUtc, SimulationBounds,
    SimulationConfig, Topology, TraceEvent,
};

mod isolation;
mod relay;

const CLUSTER: &str = "simulated";
const PORT: u16 = 7443;
const HOST_DEADLINE: Duration = Duration::from_secs(20);

#[derive(Clone)]
struct Credentials {
    ca: String,
    certificate: String,
    key: String,
}

impl Credentials {
    fn bundle(&self) -> TlsConfigBundle {
        TlsConfigBundle::from_pem(
            self.ca.as_bytes(),
            self.certificate.as_bytes(),
            self.key.as_bytes(),
            TransportClock::from_provider(StdArc::new(SimulatedUtc::new(ClockSkew::Exact))),
        )
        .assured("fixture certificates are valid at the simulated epoch")
    }
}

struct Authority {
    certificate: rcgen::Certificate,
    key: KeyPair,
}

impl Authority {
    fn new() -> Self {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        params.not_before = date_time_ymd(2027, 1, 1);
        params.not_after = date_time_ymd(2028, 1, 1);
        let key = KeyPair::generate().assured("fixture CA key generation succeeds");
        let certificate = params
            .self_signed(&key)
            .assured("fixture CA self-signature succeeds");
        Self { certificate, key }
    }

    fn issue(&self, name: &str) -> Credentials {
        let mut params = CertificateParams::new(vec![name.to_string()])
            .assured("fixture DNS names are valid SANs");
        params.subject_alt_names.push(SanType::URI(
            format!("nervix://cluster/{CLUSTER}/node/{name}")
                .try_into()
                .assured("fixture identity URI is valid"),
        ));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.not_before = date_time_ymd(2027, 1, 1);
        params.not_after = date_time_ymd(2028, 1, 1);
        let key = KeyPair::generate().assured("fixture node key generation succeeds");
        let certificate = params
            .signed_by(&key, &self.certificate, &self.key)
            .assured("fixture CA signs the node certificate");
        Credentials {
            ca: self.certificate.pem(),
            certificate: certificate.pem(),
            key: key.serialize_pem(),
        }
    }
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct BatchRequest {
    ipc: Vec<u8>,
}

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct BatchResponse {
    rows: usize,
    peer: ClusterNodeName,
}

impl InterconnectRequest for BatchRequest {
    type Response = BatchResponse;

    const NAME: &'static str = "simulation_arrow_batch";
    const TIMEOUT: Duration = Duration::from_secs(10);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct LivenessRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct LivenessResponse {
    peer: ClusterNodeName,
}

impl InterconnectRequest for LivenessRequest {
    type Response = LivenessResponse;

    const NAME: &'static str = "simulation_liveness";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const TIMEOUT: Duration = Duration::from_secs(1);
}

#[derive(Clone, Copy, Debug, Archive, Serialize, Deserialize)]
enum RemoteFailureCase {
    Rejected,
    Unavailable,
    NotReady,
    Failed,
}

impl RemoteFailureCase {
    fn failure(self, subject: RemoteOperationSubject) -> RemoteOperationFailure {
        match self {
            Self::Rejected => RemoteOperationFailure::rejected(subject),
            Self::Unavailable => RemoteOperationFailure::unavailable(subject),
            Self::NotReady => RemoteOperationFailure::not_ready(subject),
            Self::Failed => RemoteOperationFailure::failed(subject, "fixture operation failed"),
        }
    }
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct RemoteFailureProbe {
    case: RemoteFailureCase,
}

impl InterconnectRequest for RemoteFailureProbe {
    type Response = Result<(), RemoteOperationFailure>;

    const NAME: &'static str = "simulation_remote_failure_class";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(10);
}

fn arrow_batch() -> Vec<u8> {
    let schema = StdArc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let values = StdArc::new(Int32Array::from(vec![7, 11, 13]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values])
        .assured("fixture column matches its declared Arrow schema");
    let mut bytes = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut bytes, &schema).assured("fixture Arrow schema can be encoded");
    writer.write(&batch).assured("fixture batch can be encoded");
    writer.finish().assured("fixture IPC stream can finish");
    bytes
}

fn decode_arrow(ipc: &[u8]) -> usize {
    let mut reader = StreamReader::try_new(Cursor::new(ipc), None)
        .assured("the fixture IPC stream has a valid header");
    let batch = reader
        .next()
        .assured("the fixture IPC stream contains one batch")
        .assured("the fixture IPC batch is valid");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .assured("the fixture Arrow field is Int32");
    assert_eq!(values.values(), &[7, 11, 13]);
    assert!(reader.next().is_none());
    batch.num_rows()
}

fn config(seed: u64) -> SimulationConfig {
    SimulationConfig {
        seed,
        epoch: SystemTime::UNIX_EPOCH + Duration::from_secs(1_799_971_200),
        topology: Topology::Ipv4,
        bounds: SimulationBounds {
            simulated_duration: Duration::from_secs(30),
            tick: Duration::from_millis(1),
            max_steps: NonZeroUsize::new(50_000).assured("the fixture step limit is nonzero"),
            wall_duration: Duration::from_secs(90),
        },
    }
}

async fn bind_with_incoming(
    name: &'static str,
    credentials: Credentials,
    seed: u64,
) -> (Transport, mpsc::Receiver<ReceivedEnvelope>) {
    let mut options = TransportOptions::default();
    let entropy = SimulatedEntropy::new(seed, name);
    options.entropy = TransportEntropy::from_source(move || entropy.next_u64());
    Transport::bind(
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, PORT)),
        name,
        CLUSTER,
        ClusterNodeName::parse(name).assured("fixture node name is valid"),
        credentials.bundle(),
        options,
        Executor::default(),
    )
    .await
    .assured("fixture transport binds on its simulated host")
}

async fn bind(name: &'static str, credentials: Credentials, seed: u64) -> Transport {
    let (transport, _incoming) = bind_with_incoming(name, credentials, seed).await;
    transport
}

async fn wait_for(signal: &mut watch::Receiver<bool>) {
    if !*signal.borrow() {
        tokio::time::timeout(HOST_DEADLINE, signal.changed())
            .await
            .assured("peer announces readiness within the simulated deadline")
            .assured("the fixture sender remains alive until the peer is ready");
    }
}

async fn wait_for_count(signal: &mut watch::Receiver<usize>, expected: usize) {
    tokio::time::timeout(HOST_DEADLINE, async {
        while *signal.borrow() < expected {
            tokio::task::consume_budget().await;
            signal.changed().await.assured("fixture hosts remain alive");
        }
    })
    .await
    .assured("fixture hosts finish within the simulated deadline");
}

async fn register_peer(transport: &Transport, name: &str) {
    let endpoint = NodeEndpoint::new(name, PORT);
    let targets = PeerTarget::resolve(&endpoint)
        .await
        .assured("simulated DNS resolves the peer");
    for target in targets {
        transport
            .register_outbound_target(
                ClusterNodeName::parse(name).assured("fixture node name is valid"),
                target,
            )
            .assured("fixture target registration succeeds");
    }
}

#[test]
fn production_transport_exchanges_typed_arrow_batch_over_simulated_tcp() {
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let (ready_tx, ready_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let result = config(49).run("typed Arrow exchange", move |simulation| {
        let server_finished = finished_tx.clone();
        simulation.host("server", move || {
            let server_credentials = server_credentials.clone();
            let ready_tx = ready_tx.clone();
            let done_rx = done_rx.clone();
            let finished = server_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let first = bind("server", server_credentials.clone(), 49).await;
                    first.shutdown().await;
                    let (server, mut incoming) =
                        bind_with_incoming("server", server_credentials, 49).await;
                    server
                        .register_handler::<BatchRequest, _, _>(|context, request| async move {
                            BatchResponse {
                                rows: decode_arrow(&request.ipc),
                                peer: context.peer_node_id().clone(),
                            }
                        })
                        .assured("fixture handler is registered once");
                    server.replace_live_nodes(&BTreeSet::from([
                        ClusterNodeName::parse("client").assured("fixture node name is valid"),
                        ClusterNodeName::parse("server").assured("fixture node name is valid"),
                    ]));
                    ready_tx.send_replace(true);
                    let received = tokio::time::timeout(HOST_DEADLINE, incoming.recv())
                        .await
                        .assured("the relay reaches the server within the simulated deadline")
                        .assured("the relay receiver stays open");
                    assert_eq!(
                        received.peer_node_id,
                        ClusterNodeName::parse("client").assured("fixture node name is valid")
                    );
                    let Envelope::RelayPayload(payload) = received.envelope else {
                        panic!("the relay channel must carry the Arrow payload");
                    };
                    assert_eq!(decode_arrow(&payload.batch_ipc), 3);
                    received
                        .relay_admission
                        .assured("a received relay has an admission token")
                        .admit();
                    let mut done = done_rx;
                    wait_for(&mut done).await;
                    server.shutdown().await;
                    Ok::<(), std::io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("only two fixture hosts finish");
                });
                result
            }
        });
        let client_finished = finished_tx.clone();
        simulation.host("client", move || {
            let client_credentials = client_credentials.clone();
            let ready_rx = ready_rx.clone();
            let done_tx = done_tx.clone();
            let finished = client_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let client = bind("client", client_credentials, 50).await;
                    client.replace_live_nodes(&BTreeSet::from([
                        ClusterNodeName::parse("client").assured("fixture node name is valid"),
                        ClusterNodeName::parse("server").assured("fixture node name is valid"),
                    ]));
                    let mut ready = ready_rx;
                    wait_for(&mut ready).await;
                    register_peer(&client, "server").await;
                    let server =
                        ClusterNodeName::parse("server").assured("fixture node name is valid");
                    let response = client
                        .request(&server, BatchRequest { ipc: arrow_batch() })
                        .await
                        .assured("production TLS and HTTP/2 exchange succeeds");
                    assert_eq!(response.rows, 3);
                    assert_eq!(response.peer, *client.node_id());
                    let batch_ipc = Executor::default()
                        .try_charge_owned(MemoryClass::Relay, arrow_batch())
                        .assured("the Arrow fixture fits the relay memory budget");
                    let relay = RelayPayload {
                        delivery: RelayDelivery {
                            channel_incarnation: [49; 16],
                            sequence: 0,
                        },
                        kind: RelayPayloadKind::Routed,
                        domain: DomainName::parse("simulated").assured("fixture domain is valid"),
                        relay: RelayName::parse("records").assured("fixture relay is valid"),
                        key: None,
                        batch_ipc,
                        metadata: Vec::new(),
                        acks: Vec::new(),
                        admission: Some(RemoteAckRegistration {
                            ack_id: 49,
                            reply_node_id: client.node_id().clone(),
                        }),
                    };
                    client
                        .send(&server, Envelope::RelayPayload(relay))
                        .await
                        .assured("the production relay transfers the Arrow IPC batch");
                    done_tx.send_replace(true);
                    client.shutdown().await;
                    Ok::<(), std::io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("only two fixture hosts finish");
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
    assert!(result.is_ok(), "{result:?}");
}

async fn serve_peer(
    name: &'static str,
    credentials: Credentials,
    seed: u64,
    ready: watch::Sender<usize>,
    mut done: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let server = bind(name, credentials, seed).await;
    server
        .register_handler::<BatchRequest, _, _>(|context, request| async move {
            BatchResponse {
                rows: decode_arrow(&request.ipc),
                peer: context.peer_node_id().clone(),
            }
        })
        .assured("each fixture transport registers one typed handler");
    server
        .register_handler::<RemoteFailureProbe, _, _>(|_, request| async move {
            let domain = DomainName::parse("simulated").assured("fixture domain is valid");
            let subject = RemoteOperationSubject::domain(&domain);
            Err(request.case.failure(subject))
        })
        .assured("each fixture transport registers the remote failure probe");
    let live = ["client", "server", "third"]
        .into_iter()
        .map(|name| ClusterNodeName::parse(name).assured("fixture node names are valid"))
        .collect::<BTreeSet<_>>();
    server.replace_live_nodes(&live);
    ready.send_modify(|count| {
        *count = count
            .checked_add(1)
            .assured("only two fixture peers signal readiness");
    });
    wait_for(&mut done).await;
    server.shutdown().await;
    Ok(())
}

#[test]
fn multiple_peers_keep_remote_failure_classes_and_reject_invalid_authentication() {
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let third_credentials = authority.issue("third");
    let client_credentials = authority.issue("client");
    let (ready_tx, ready_rx) = watch::channel(0_usize);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let result = config(57).run("multiple authenticated peers", move |simulation| {
        let server_ready = ready_tx.clone();
        let server_done = done_rx.clone();
        let server_finished = finished_tx.clone();
        simulation.host("server", move || {
            let credentials = server_credentials.clone();
            let ready = server_ready.clone();
            let done = server_done.clone();
            let finished = server_finished.clone();
            async move {
                let result = HostSupervisor::run(serve_peer("server", credentials, 57, ready, done)).await;
                finished.send_modify(|count| {
                    *count = count.checked_add(1).assured("only three fixture hosts finish");
                });
                result
            }
        });
        let third_ready = ready_tx.clone();
        let third_done = done_rx.clone();
        let third_finished = finished_tx.clone();
        simulation.host("third", move || {
            let credentials = third_credentials.clone();
            let ready = third_ready.clone();
            let done = third_done.clone();
            let finished = third_finished.clone();
            async move {
                let result = HostSupervisor::run(serve_peer("third", credentials, 58, ready, done)).await;
                finished.send_modify(|count| {
                    *count = count.checked_add(1).assured("only three fixture hosts finish");
                });
                result
            }
        });
        let client_finished = finished_tx.clone();
        simulation.host("client", move || {
            let credentials = client_credentials.clone();
            let ready = ready_rx.clone();
            let done = done_tx.clone();
            let finished = client_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let client = bind("client", credentials, 59).await;
                    let live = ["client", "server", "third"]
                        .into_iter()
                        .map(|name| {
                            ClusterNodeName::parse(name).assured("fixture node names are valid")
                        })
                        .collect::<BTreeSet<_>>();
                    client.replace_live_nodes(&live);
                    let mut ready = ready;
                    wait_for_count(&mut ready, 2).await;
                    for name in ["server", "third"] {
                        tokio::task::consume_budget().await;
                        register_peer(&client, name).await;
                        let node =
                            ClusterNodeName::parse(name).assured("fixture node name is valid");
                        let response = client
                            .request(&node, BatchRequest { ipc: arrow_batch() })
                            .await
                            .assured("typed Arrow exchange succeeds with each peer");
                        assert_eq!(response.rows, 3);
                        assert_eq!(response.peer, *client.node_id());
                        for case in [
                            RemoteFailureCase::Rejected,
                            RemoteFailureCase::Unavailable,
                            RemoteFailureCase::NotReady,
                            RemoteFailureCase::Failed,
                        ] {
                            tokio::task::consume_budget().await;
                            let response = client
                                .request(&node, RemoteFailureProbe { case })
                                .await
                                .assured("the authenticated remote failure response arrives");
                            let Err(failure) = response else {
                                panic!("the probe must return a remote failure");
                            };
                            let subject = match (case, failure) {
                                (
                                    RemoteFailureCase::Rejected,
                                    RemoteOperationFailure::Rejected { subject },
                                )
                                | (
                                    RemoteFailureCase::Unavailable,
                                    RemoteOperationFailure::Unavailable { subject },
                                )
                                | (
                                    RemoteFailureCase::NotReady,
                                    RemoteOperationFailure::NotReady { subject },
                                )
                                | (
                                    RemoteFailureCase::Failed,
                                    RemoteOperationFailure::Failed { subject, .. },
                                ) => subject,
                                (case, failure) => {
                                    panic!("{name} returned {failure:?} for {case:?}")
                                }
                            };
                            let domain =
                                DomainName::parse("simulated").assured("fixture domain is valid");
                            assert_eq!(subject, RemoteOperationSubject::domain(&domain));
                        }
                    }
                    let target = PeerTarget::resolve(&NodeEndpoint::new("server", PORT))
                        .await
                        .assured("simulated DNS resolves the server")
                        .into_iter()
                        .next()
                        .assured("simulated DNS returned an address");
                    let invalid = PeerTarget::new(target.addr, "incorrect-host");
                    let error = match client.bootstrap_target(invalid).await {
                        Ok(_) => panic!("the certificate must reject an unrelated DNS identity"),
                        Err(error) => error,
                    };
                    assert!(
                        matches!(error, TransportError::Io(ref io_error) if io_error.kind() == io::ErrorKind::InvalidData),
                        "{error:?}"
                    );
                    done.send_replace(true);
                    client.shutdown().await;
                    Ok::<(), std::io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count.checked_add(1).assured("only three fixture hosts finish");
                });
                result
            }
        });
        simulation.client("observer", async move {
            let mut finished = finished_rx;
            wait_for_count(&mut finished, 3).await;
            Ok(())
        });
    });
    assert!(result.is_ok(), "{result:?}");
}

#[derive(Clone, Copy, Debug)]
enum NetworkFault {
    PartitionBeforeConnect,
    AsymmetricBeforeConnect,
    HeldExchange,
    PartitionedExchange,
}

impl NetworkFault {
    fn name(self) -> &'static str {
        match self {
            Self::PartitionBeforeConnect => "partition before connect",
            Self::AsymmetricBeforeConnect => "one-way partition before connect",
            Self::HeldExchange => "held authenticated exchange",
            Self::PartitionedExchange => "partitioned authenticated exchange",
        }
    }

    fn install(self, simulation: &mut turmoil::Sim<'_>) {
        match self {
            Self::PartitionBeforeConnect => simulation.partition("client", "server"),
            Self::AsymmetricBeforeConnect => simulation.partition_oneway("server", "client"),
            Self::HeldExchange | Self::PartitionedExchange => {}
        }
    }

    fn repair(self) {
        match self {
            Self::PartitionBeforeConnect | Self::PartitionedExchange => {
                turmoil::repair("client", "server");
            }
            Self::AsymmetricBeforeConnect => {
                turmoil::repair_oneway("server", "client");
            }
            Self::HeldExchange => turmoil::release("client", "server"),
        }
    }
}

async fn wait_for_connection(transport: &Transport, peer: &ClusterNodeName) {
    tokio::time::timeout(HOST_DEADLINE, async {
        while !transport.is_connected_to(peer) {
            tokio::task::consume_budget().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .assured("the repaired peer becomes transport-ready within the simulated deadline");
}

fn assert_bounded(transport: &Transport) {
    let snapshot = transport.snapshot();
    let connections: usize = snapshot.connections.iter().flatten().sum();
    assert!(
        connections <= 12,
        "one peer has at most six connections in each direction: {snapshot:?}"
    );
    assert!(
        snapshot
            .pending_operations
            .iter()
            .flatten()
            .all(|count| *count <= 1)
    );
    let failures: u64 = snapshot.counters.connection_failures.iter().flatten().sum();
    assert!(
        failures < 100,
        "bounded backoff prevents a retry storm: {snapshot:?}"
    );
    let tcp_streams = turmoil::established_tcp_stream_count();
    assert!(
        tcp_streams <= 64,
        "simulated TCP streams grew without bound: {tcp_streams}"
    );
    let tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    assert!(
        tasks <= 128,
        "the simulated host accumulated {tasks} live tasks"
    );
}

async fn assert_liveness(transport: &Transport, peer: &ClusterNodeName) {
    let response = transport
        .request(peer, LivenessRequest)
        .await
        .assured("the authenticated peer answers the liveness probe");
    assert_eq!(response.peer, *transport.node_id());
}

async fn assert_liveness_timeout(transport: &Transport, peer: &ClusterNodeName) {
    let started = turmoil::elapsed();
    let error = match transport.request(peer, LivenessRequest).await {
        Ok(_) => panic!("disrupted liveness traffic must expire"),
        Err(error) => error,
    };
    assert!(
        matches!(error.current_context(), RequestError::Timeout { timeout, .. } if *timeout == Duration::from_secs(1)),
        "{error:?}"
    );
    assert!(turmoil::elapsed() >= started + Duration::from_secs(1));
}

async fn assert_stopped(transport: &Transport) {
    assert_eq!(transport.active_outbound_connections().await, 0);
    let snapshot = transport.snapshot();
    assert!(
        snapshot
            .connections
            .iter()
            .flatten()
            .all(|count| *count == 0)
    );
    assert!(
        snapshot
            .pending_operations
            .iter()
            .flatten()
            .all(|count| *count == 0)
    );
    assert!(snapshot.leased_streams.iter().all(|count| *count == 0));
}

async fn bind_fault_server(credentials: Credentials, seed: u64) -> Transport {
    let server = bind("server", credentials, seed).await;
    server
        .register_handler::<BatchRequest, _, _>(|context, request| async move {
            BatchResponse {
                rows: decode_arrow(&request.ipc),
                peer: context.peer_node_id().clone(),
            }
        })
        .assured("the fixture handler is registered once per listener");
    server
        .register_handler::<LivenessRequest, _, _>(|context, _| async move {
            LivenessResponse {
                peer: context.peer_node_id().clone(),
            }
        })
        .assured("the fixture liveness handler is registered once per listener");
    server.replace_live_nodes(&BTreeSet::from([
        ClusterNodeName::parse("client").assured("fixture node name is valid"),
        ClusterNodeName::parse("server").assured("fixture node name is valid"),
    ]));
    server
}

fn exercise_fault(fault: NetworkFault, seed: u64) -> Vec<TraceEvent> {
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let (ready_tx, ready_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let (restart_tx, restart_rx) = watch::channel(false);
    let (restarted_tx, restarted_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let trace = SemanticTrace::default();
    let event_trace = trace.clone();
    let mut scenario_config = config(seed);
    scenario_config.bounds.simulated_duration = Duration::from_secs(60);
    let result = scenario_config.run(fault.name(), move |simulation| {
        let server_finished = finished_tx.clone();
        simulation.host("server", move || {
            let credentials = server_credentials.clone();
            let ready = ready_tx.clone();
            let mut done = done_rx.clone();
            let mut restart = restart_rx.clone();
            let restarted = restarted_tx.clone();
            let finished = server_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let server = bind_fault_server(credentials.clone(), seed).await;
                    ready.send_replace(true);
                    if let NetworkFault::PartitionedExchange = fault {
                        wait_for(&mut restart).await;
                        server.shutdown().await;
                        let rebuilt = bind_fault_server(credentials, seed).await;
                        restarted.send_replace(true);
                        wait_for(&mut done).await;
                        assert_bounded(&rebuilt);
                        rebuilt.shutdown().await;
                        assert_stopped(&rebuilt).await;
                        return Ok::<(), io::Error>(());
                    }
                    let completed = async {
                        while !*done.borrow() {
                            tokio::task::consume_budget().await;
                            done.changed()
                                .await
                                .assured("the client remains alive during repair");
                        }
                    };
                    tokio::time::timeout(Duration::from_secs(40), completed)
                        .await
                        .assured("the client completes before the server budget expires");
                    assert_bounded(&server);
                    server.shutdown().await;
                    assert_stopped(&server).await;
                    Ok::<(), io::Error>(())
                }).await;
                finished.send_modify(|count| {
                    *count = count.checked_add(1).assured("only two fixture hosts finish");
                });
                result
            }
        });
        let client_finished = finished_tx.clone();
        let client_trace = event_trace.clone();
        simulation.host("client", move || {
            let credentials = client_credentials.clone();
            let mut ready = ready_rx.clone();
            let done = done_tx.clone();
            let restart = restart_tx.clone();
            let mut restarted = restarted_rx.clone();
            let finished = client_finished.clone();
            let trace = client_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let client_seed = seed
                        .checked_add(1)
                        .assured("fixture seeds are below u64::MAX");
                    let client = bind("client", credentials, client_seed).await;
                    let peer = ClusterNodeName::parse("server").assured("fixture node name is valid");
                    let live = BTreeSet::from([client.node_id().clone(), peer.clone()]);
                    client.replace_live_nodes(&live);
                    wait_for(&mut ready).await;
                    register_peer(&client, "server").await;
                    trace.record("client", format!("{fault:?} installed; peer registered"));
                    match fault {
                        NetworkFault::PartitionBeforeConnect | NetworkFault::AsymmetricBeforeConnect => {
                            let setup_failed = async {
                                loop {
                                    tokio::task::consume_budget().await;
                                    let counters = client.snapshot().counters;
                                    let failures = counters.connection_failures
                                        [PoolClass::Management.index()]
                                        [ConnectionFailureReason::Setup.index()];
                                    if failures > 0 {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                }
                            };
                            tokio::time::timeout(HOST_DEADLINE, setup_failed)
                                .await
                                .assured("the partitioned dial reports a setup failure");
                            assert!(!client.is_connected_to(&peer));
                            assert_liveness_timeout(&client, &peer).await;
                            let disruption_deadline =
                                tokio::time::Instant::now() + Duration::from_secs(12);
                            tokio::time::sleep_until(disruption_deadline).await;
                            assert!(!client.is_connected_to(&peer));
                            assert_bounded(&client);
                            trace.record("client", "setup failed through prolonged partition");
                            fault.repair();
                            trace.record("client", "link repaired");
                        }
                        NetworkFault::HeldExchange | NetworkFault::PartitionedExchange => {
                            wait_for_connection(&client, &peer).await;
                            assert_liveness(&client, &peer).await;
                            let first = client
                                .request(&peer, BatchRequest { ipc: arrow_batch() })
                                .await
                                .assured("the first authenticated request succeeds");
                            assert_eq!(first.rows, 3);
                            trace.record("client", "authenticated exchange completed");
                            if let NetworkFault::HeldExchange = fault {
                                turmoil::hold("client", "server");
                            } else {
                                turmoil::partition("client", "server");
                            }
                            let started = turmoil::elapsed();
                            assert_liveness_timeout(&client, &peer).await;
                            let request = BatchRequest { ipc: arrow_batch() };
                            let deadline = Duration::from_secs(2);
                            let error = match client.request_with_timeout(&peer, request, deadline).await {
                                Ok(_) => panic!("disrupted traffic must hit its physical request deadline"),
                                Err(error) => error,
                            };
                            assert!(
                                matches!(error.current_context(), RequestError::Timeout { timeout, .. } if *timeout == deadline),
                                "{error:?}"
                            );
                            assert!(turmoil::elapsed() >= started + Duration::from_secs(3));
                            assert_bounded(&client);
                            trace.record("client", "request reached its two-second deadline");
                            fault.repair();
                            trace.record("client", "link repaired");
                            if let NetworkFault::PartitionedExchange = fault {
                                // Dropped TCP segments can leave an established HTTP/2 session
                                // unusable. Close the listener and its sockets, then retire the
                                // client pool through the public topology API.
                                restart.send_replace(true);
                                wait_for(&mut restarted).await;
                                client.replace_outbound_targets(&Default::default());
                                assert!(!client.is_connected_to(&peer));
                                register_peer(&client, "server").await;
                                trace.record("client", "listener rebound and pool reconnected");
                            }
                        }
                    }
                    wait_for_connection(&client, &peer).await;
                    assert_liveness(&client, &peer).await;
                    let response = client
                        .request(&peer, BatchRequest { ipc: arrow_batch() })
                        .await
                        .assured("the repaired authenticated peer answers again");
                    assert_eq!(response.rows, 3);
                    assert_eq!(response.peer, *client.node_id());
                    assert_bounded(&client);
                    trace.record("client", "authenticated exchange recovered");
                    done.send_replace(true);
                    client.shutdown().await;
                    assert_stopped(&client).await;
                    Ok::<(), io::Error>(())
                }).await;
                finished.send_modify(|count| {
                    *count = count.checked_add(1).assured("only two fixture hosts finish");
                });
                result
            }
        });
        simulation.client("observer", async move {
            let mut finished = finished_rx;
            tokio::time::timeout(Duration::from_secs(50), async {
                while *finished.borrow() < 2 {
                    tokio::task::consume_budget().await;
                    finished.changed().await.assured("the fixture hosts remain alive");
                }
            }).await.assured("the fault scenario hosts finish within their simulated budget");
            Ok(())
        });
        fault.install(simulation);
    });
    assert!(result.is_ok(), "fault {fault:?} seed {seed}: {result:?}");
    trace.events()
}

#[test]
fn network_disruption_respects_deadlines_and_repairs_authenticated_service() {
    for (fault, seed) in [
        (NetworkFault::PartitionBeforeConnect, 61),
        (NetworkFault::AsymmetricBeforeConnect, 62),
        (NetworkFault::HeldExchange, 63),
        (NetworkFault::PartitionedExchange, 64),
    ] {
        let first = exercise_fault(fault, seed);
        let replay = exercise_fault(fault, seed);
        assert_eq!(first, replay, "fault {fault:?} seed {seed} did not replay");
    }
}
