//! The transport's tests: connection setup, typed requests, duplex and streamed exchanges, relay
//! delivery, pools and quotas, discovery and progress, exercised between in-process transports.
//!
//! Layer: test harness.
//!
//! - **Owns.** Assertions over the transport's public operations and their limits.
//! - **Depends on.** The transport and its test certificates.
//! - **Must not know.** The runtime or control plane that uses the transport.

use std::{
    collections::BTreeSet,
    path::PathBuf,
    process::Command,
    sync::{Arc as StdArc, OnceLock},
};

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{CpuClass, MemoryClass};
use nervix_models::RemoteAckOutcome;
use nervix_primitives::sync::atomic::{AtomicUsize, Ordering};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};
use tempfile::{TempDir, tempdir};
use tokio::{
    sync::{Notify, watch},
    time::{Instant, timeout, timeout_at},
};

use super::*;

/// Counts allocations per thread, so a test can prove that an operation allocates nothing.
#[global_allocator]
static ALLOCATIONS: alloc_count::AllocCounter = alloc_count::AllocCounter(std::alloc::System);

fn tls_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tls/dev")
        .join(name)
}

pub(crate) fn test_tls() -> TlsConfigBundle {
    static GENERATED: OnceLock<()> = OnceLock::new();
    GENERATED.get_or_init(|| {
        let status = Command::new("bash")
            .arg("scripts/generate_dev_tls.sh")
            .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .status()
            .expect("dev TLS generation command should run");
        assert!(status.success(), "dev TLS generation should succeed");
    });
    TlsConfigBundle::from_pem_files(
        tls_path("ca.pem"),
        tls_path("node.pem"),
        tls_path("node-key.pem"),
        TransportClock::system(),
    )
    .expect("test TLS should load")
}

struct TestCertificateAuthority {
    _directory: TempDir,
    certificate: rcgen::Certificate,
    key: KeyPair,
    path: PathBuf,
}

impl TestCertificateAuthority {
    fn new() -> Self {
        let directory = tempdir().expect("test certificate directory should be created");
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().expect("test CA key should be generated");
        let certificate = params
            .self_signed(&key)
            .expect("test CA certificate should be generated");
        let path = directory.path().join("ca.pem");
        std::fs::write(&path, certificate.pem()).expect("test CA certificate should be written");
        Self {
            _directory: directory,
            certificate,
            key,
            path,
        }
    }

    fn issue(&self, cluster: &str, node: &ClusterNodeName) -> TlsConfigBundle {
        let mut params = CertificateParams::new(vec!["localhost".to_string()])
            .expect("test endpoint SAN should be valid");
        params.subject_alt_names.push(SanType::URI(
            format!("nervix://cluster/{cluster}/node/{node}")
                .try_into()
                .expect("test identity URI should be valid"),
        ));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().expect("test node key should be generated");
        let certificate = params
            .signed_by(&key, &self.certificate, &self.key)
            .expect("test node certificate should be signed");
        let certificate_path = self
            ._directory
            .path()
            .join(format!("{node}-certificate.pem"));
        let key_path = self._directory.path().join(format!("{node}-key.pem"));
        std::fs::write(&certificate_path, certificate.pem())
            .expect("test node certificate should be written");
        std::fs::write(&key_path, key.serialize_pem()).expect("test node key should be written");
        TlsConfigBundle::from_pem_files(
            &self.path,
            certificate_path,
            key_path,
            TransportClock::system(),
        )
        .expect("test node TLS identity should load")
    }
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct EchoRequest {
    value: String,
}

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct EchoResponse {
    value: String,
    peer: ClusterNodeName,
    advertised_host: String,
}

impl InterconnectRequest for EchoRequest {
    type Response = EchoResponse;

    const NAME: &'static str = "test_echo";
    const TIMEOUT: Duration = Duration::from_secs(2);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct BlockingBulkRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct BlockingBulkResponse;

impl InterconnectRequest for BlockingBulkRequest {
    type Response = BlockingBulkResponse;

    const NAME: &'static str = "test_blocking_bulk";
    const CLASS: PoolClass = PoolClass::Bulk;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct ReplicationRequest {
    wait: bool,
}

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct ReplicationResponse;

impl InterconnectRequest for ReplicationRequest {
    type Response = ReplicationResponse;

    const NAME: &'static str = "test_replication";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct ManagementRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct ManagementResponse;

impl InterconnectRequest for ManagementRequest {
    type Response = ManagementResponse;

    const NAME: &'static str = "test_management";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct BlockingManagementRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct BlockingManagementResponse;

impl InterconnectRequest for BlockingManagementRequest {
    type Response = BlockingManagementResponse;

    const NAME: &'static str = "test_blocking_management";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct CancellationRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct CancellationResponse;

impl InterconnectRequest for CancellationRequest {
    type Response = CancellationResponse;

    const NAME: &'static str = "test_cancellation";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Cancellation;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct BlockingDiscoveryRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct BlockingDiscoveryResponse;

impl InterconnectRequest for BlockingDiscoveryRequest {
    type Response = BlockingDiscoveryResponse;

    const NAME: &'static str = "test_blocking_discovery";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Discovery;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct BlockingProgressRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct BlockingProgressResponse;

const LIVENESS_QUOTA_EVENT_FAILSAFE_SECONDS: u64 = 30;
const LIVENESS_QUOTA_REQUEST_TIMEOUT_MULTIPLIER: u64 = 2;
const LIVENESS_QUOTA_REQUEST_TIMEOUT_SECONDS: u64 = match LIVENESS_QUOTA_EVENT_FAILSAFE_SECONDS
    .checked_mul(LIVENESS_QUOTA_REQUEST_TIMEOUT_MULTIPLIER)
{
    Some(seconds) => seconds,
    None => panic!("the fixed liveness quota test deadlines fit in u64"),
};
const LIVENESS_QUOTA_EVENT_FAILSAFE: Duration =
    Duration::from_secs(LIVENESS_QUOTA_EVENT_FAILSAFE_SECONDS);
const LIVENESS_QUOTA_REQUEST_TIMEOUT: Duration =
    Duration::from_secs(LIVENESS_QUOTA_REQUEST_TIMEOUT_SECONDS);
const _: () = assert!(
    LIVENESS_QUOTA_REQUEST_TIMEOUT_SECONDS > LIVENESS_QUOTA_EVENT_FAILSAFE_SECONDS,
    "blocked progress requests must remain active throughout the liveness observation"
);

impl InterconnectRequest for BlockingProgressRequest {
    type Response = BlockingProgressResponse;

    const NAME: &'static str = "test_blocking_progress";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Progress;
    const TIMEOUT: Duration = LIVENESS_QUOTA_REQUEST_TIMEOUT;
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct LivenessRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct LivenessResponse;

impl InterconnectRequest for LivenessRequest {
    type Response = LivenessResponse;

    const NAME: &'static str = "test_liveness";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const TIMEOUT: Duration = LIVENESS_QUOTA_REQUEST_TIMEOUT;
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct HangingRequest;

#[derive(Debug, Archive, Serialize, Deserialize)]
struct HangingResponse;

impl InterconnectRequest for HangingRequest {
    type Response = HangingResponse;

    const NAME: &'static str = "test_hanging";
    const TIMEOUT: Duration = Duration::from_secs(30);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct ResourceStreamRequest;

impl InterconnectStreamRequest for ResourceStreamRequest {
    const NAME: &'static str = "test_resource_stream";
    const SUBQUOTA: RequestSubquota = RequestSubquota::Resource;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct SnapshotStreamRequest;

impl InterconnectStreamRequest for SnapshotStreamRequest {
    const NAME: &'static str = "test_snapshot_stream";
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct DeadlineStreamRequest {
    response_delay_ms: u64,
}

impl InterconnectStreamRequest for DeadlineStreamRequest {
    const NAME: &'static str = "test_deadline_stream";
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_millis(200);
}

struct ConnectedTransports {
    _authority: TestCertificateAuthority,
    transport_a: Transport,
    transport_b: Transport,
    node_a: ClusterNodeName,
    node_b: ClusterNodeName,
    /// The responder's budgets, so a test can observe what its handlers are holding charged.
    executor_b: Executor,
    _incoming_a: mpsc::Receiver<ReceivedEnvelope>,
    incoming_b: mpsc::Receiver<ReceivedEnvelope>,
}

async fn connected_transports() -> ConnectedTransports {
    connected_transports_with_options(TransportOptions::default()).await
}

async fn connected_transports_with_options(options: TransportOptions) -> ConnectedTransports {
    let transports = bound_transports_with_options(options).await;
    transports
        .transport_a
        .register_outbound_target(
            transports.node_b.clone(),
            NodeEndpoint::new("localhost", transports.transport_b.local_addr().port()),
        )
        .expect("authenticated test target should register");
    transports
}

async fn bound_transports_with_options(options: TransportOptions) -> ConnectedTransports {
    let authority = TestCertificateAuthority::new();
    let node_a = ClusterNodeName::parse("node-a").expect("test node name should be valid");
    let node_b = ClusterNodeName::parse("node-b").expect("test node name should be valid");
    let (transport_a, incoming_a) = Transport::bind(
        "127.0.0.1:0".parse().expect("test address should be valid"),
        localhost_identity("test-cluster", node_a.clone()),
        authority.issue("test-cluster", &node_a),
        options.clone(),
        Executor::default(),
        test_resolver().await,
    )
    .await
    .expect("first test transport should bind");
    let executor_b = Executor::default();
    let (transport_b, incoming_b) = Transport::bind(
        "127.0.0.1:0".parse().expect("test address should be valid"),
        localhost_identity("test-cluster", node_b.clone()),
        authority.issue("test-cluster", &node_b),
        options,
        executor_b.clone(),
        test_resolver().await,
    )
    .await
    .expect("second test transport should bind");
    let live_nodes = BTreeSet::from([node_a.clone(), node_b.clone()]);
    transport_a.replace_live_nodes(&live_nodes);
    transport_b.replace_live_nodes(&live_nodes);
    ConnectedTransports {
        _authority: authority,
        transport_a,
        transport_b,
        node_a,
        node_b,
        executor_b,
        _incoming_a: incoming_a,
        incoming_b,
    }
}

#[tokio::test]
async fn send_waits_for_a_target_registered_after_the_operation_starts() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        mut incoming_b,
        ..
    } = bound_transports_with_options(TransportOptions::default()).await;
    let mut send =
        Box::pin(transport_a.send(&node_b, Envelope::Control(ControlEnvelope::Terminate)));

    assert!(
        send.as_mut().now_or_never().is_none(),
        "send should await the transport's target notification"
    );
    transport_a
        .register_outbound_target(
            node_b.clone(),
            NodeEndpoint::new("localhost", transport_b.local_addr().port()),
        )
        .expect("authenticated test target should register");

    send.await.expect("control delivery should succeed");
    let received = incoming_b
        .recv()
        .now_or_never()
        .expect("the control is queued before the successful response")
        .expect("the peer's incoming queue should remain open");
    assert!(matches!(
        received.envelope,
        Envelope::Control(ControlEnvelope::Terminate)
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[test]
fn internal_tls_negotiates_only_http2() {
    let tls = test_tls();

    assert_eq!(tls.client_config.alpn_protocols, [b"h2".to_vec()]);
    assert_eq!(tls.server_config.alpn_protocols, [b"h2".to_vec()]);
}

#[test]
fn connection_limit_reserves_both_preconnected_directions() {
    let mut options = TransportOptions {
        max_peers: 2,
        max_connections: 20,
        ..TransportOptions::default()
    };
    assert!(matches!(
        options.validate(),
        Err(error) if matches!(error.current_context(), TransportError::InvalidOptions { .. })
    ));

    options.max_connections = 21;
    options
        .validate()
        .expect("one on-demand connection should fit after both preconnected directions");
}

#[test]
fn invalid_transport_options_report_the_failed_contract() {
    let cases = [
        (
            "max_peers",
            TransportOptions {
                max_peers: 0,
                ..TransportOptions::default()
            },
        ),
        (
            "HTTP/2 windows",
            TransportOptions {
                initial_stream_window_bytes: 0,
                ..TransportOptions::default()
            },
        ),
        (
            "transport deadlines",
            TransportOptions {
                request_timeout: Duration::ZERO,
                ..TransportOptions::default()
            },
        ),
        (
            "max_reconnect_backoff",
            TransportOptions {
                max_reconnect_backoff: Duration::ZERO,
                ..TransportOptions::default()
            },
        ),
    ];
    for (contract, options) in cases {
        let error = options
            .validate()
            .expect_err("invalid options must be refused");
        let TransportError::InvalidOptions { reason } = error.current_context() else {
            panic!("wrong failure class: {error:?}");
        };
        assert!(reason.contains(contract), "{reason}");
    }
}

#[tokio::test]
async fn peer_quota_refuses_a_new_target_but_retains_the_registered_peer() {
    let options = TransportOptions {
        max_peers: 1,
        ..TransportOptions::default()
    };
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = bound_transports_with_options(options).await;
    let endpoint = NodeEndpoint::new("localhost", transport_b.local_addr().port());
    transport_a
        .register_outbound_target(node_b.clone(), endpoint.clone())
        .expect("the first peer should fit the quota");
    let node_c = ClusterNodeName::parse("node-c").expect("test node name should be valid");
    let error = transport_a
        .register_outbound_target(node_c, endpoint.clone())
        .expect_err("another peer must exceed the quota");
    assert!(matches!(
        error.current_context(),
        TransportError::PoolExhausted
    ));
    transport_a
        .register_outbound_target(node_b, endpoint)
        .expect("re-registering the current peer must remain possible");

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn bootstrap_respects_the_peer_quota_before_registering_a_new_identity() {
    let options = TransportOptions {
        max_peers: 1,
        ..TransportOptions::default()
    };
    let ConnectedTransports {
        _authority: authority,
        transport_a,
        transport_b,
        node_b,
        ..
    } = bound_transports_with_options(options).await;
    transport_a
        .register_outbound_target(
            node_b.clone(),
            NodeEndpoint::new("localhost", transport_b.local_addr().port()),
        )
        .expect("the first peer should fit the quota");

    let node_c = ClusterNodeName::parse("node-c").expect("test node name should be valid");
    let (transport_c, _incoming_c) = Transport::bind(
        "127.0.0.1:0".parse().expect("test address should be valid"),
        localhost_identity("test-cluster", node_c.clone()),
        authority.issue("test-cluster", &node_c),
        TransportOptions::default(),
        Executor::default(),
        test_resolver().await,
    )
    .await
    .expect("third test transport should bind");
    let error = transport_a
        .bootstrap_target(PeerTarget::new(transport_c.local_addr(), "localhost"))
        .await
        .expect_err("bootstrap must respect the existing peer quota");
    assert!(matches!(
        error.current_context(),
        TransportError::PoolExhausted
    ));
    assert!(!transport_a.is_connected_to(&node_c));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
    transport_c.shutdown().await;
}

#[tokio::test]
async fn one_way_control_send_refuses_a_typed_request() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let error = transport_a
        .send(
            &node_b,
            Envelope::Control(ControlEnvelope::Request(RequestEnvelope {
                class: PoolClass::Management,
                request: "test_echo".to_string(),
                payload: Vec::new(),
            })),
        )
        .await
        .expect_err("typed requests require the request protocol");
    assert!(matches!(
        error.current_context(),
        TransportError::Decode(reason)
            if reason.contains("typed request envelopes cannot be sent as one-way controls")
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[test]
fn relay_acknowledgements_use_the_management_pool() {
    let envelope = Envelope::Ack(RemoteAckResolution {
        ack_id: 1,
        outcome: RemoteAckOutcome::Ack,
    });

    assert_eq!(envelope.pool_class(), PoolClass::Management);
}

#[tokio::test]
async fn connection_binding_drives_response_flow_control() {
    let options = TransportOptions {
        initial_stream_window_bytes: 1,
        ..TransportOptions::default()
    };
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports_with_options(options).await;

    timeout(Duration::from_secs(2), async {
        loop {
            tokio::task::consume_budget().await;
            if transport_a.is_connected_to(&node_b) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("binding responses should advance beyond the one-byte stream window");

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[test]
fn certificate_binds_cluster_node_and_endpoint() {
    let tls = test_tls();
    let node = ClusterNodeName::parse("node-1").expect("valid node name");

    tls.certificate
        .validate_local("default", &node, "localhost")
        .expect("certificate identity should match");
    assert!(
        tls.certificate
            .validate_local("another-cluster", &node, "localhost")
            .is_err()
    );
}

#[tokio::test]
async fn rejected_tls_replacement_keeps_the_previous_identity_usable() {
    let ConnectedTransports {
        _authority: authority,
        transport_a,
        transport_b,
        node_a,
        node_b,
        mut incoming_b,
        ..
    } = connected_transports().await;
    let error = transport_a
        .replace_tls(authority.issue("another-cluster", &node_a))
        .await
        .expect_err("a replacement certificate for another cluster must be rejected");
    assert!(matches!(
        error.current_context(),
        TransportError::InvalidHandshake(_)
    ));
    assert!(error.contains::<TlsConfigError>());

    transport_a
        .send(&node_b, Envelope::Control(ControlEnvelope::Terminate))
        .await
        .expect("the original certificate must still authenticate the transport");
    let received = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the peer should receive the control message")
        .expect("the peer should remain available");
    assert!(matches!(
        received.envelope,
        Envelope::Control(ControlEnvelope::Terminate)
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

mod coordination;
mod resolver;

use resolver::{localhost_identity, test_resolver};

mod lease;

#[tokio::test]
async fn resource_streams_leave_the_reserved_snapshot_slot_responsive() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let release = watch::channel(false).0;
    let resource_executor = Executor::default();
    transport_b
        .register_stream_handler::<ResourceStreamRequest, _, _>({
            let release = release.clone();
            move |_context, _request| {
                let mut release = release.subscribe();
                let executor = resource_executor.clone();
                async move {
                    let chunk = executor
                        .charge_owned(MemoryClass::Bulk, b"resource".to_vec())
                        .await
                        .map_err(StreamHandlerError::with_cause)?;
                    let chunks = futures_util::stream::once(async move {
                        if !*release.borrow() {
                            release
                                .changed()
                                .await
                                .expect("the release sender lives through the test");
                        }
                        Ok(chunk)
                    });
                    Ok(StreamingResponse::new(8, chunks))
                }
            }
        })
        .expect("resource stream handler should register");
    let snapshot_executor = Executor::default();
    transport_b
        .register_stream_handler::<SnapshotStreamRequest, _, _>(move |_context, _request| {
            let executor = snapshot_executor.clone();
            async move {
                let chunk = executor
                    .charge_owned(MemoryClass::Bulk, b"snapshot".to_vec())
                    .await
                    .map_err(StreamHandlerError::with_cause)?;
                Ok(StreamingResponse::new(
                    8,
                    futures_util::stream::iter([Ok(chunk)]),
                ))
            }
        })
        .expect("snapshot stream handler should register");
    timeout(Duration::from_secs(5), async {
        while !transport_a.is_connected_to(&node_b) {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the target should become ready");

    let mut first = transport_a
        .request_stream(&node_b, ResourceStreamRequest)
        .await
        .expect("first resource stream should open");
    let mut second = transport_a
        .request_stream(&node_b, ResourceStreamRequest)
        .await
        .expect("second resource stream should open");
    let mut snapshot = timeout(
        Duration::from_secs(1),
        transport_a.request_stream(&node_b, SnapshotStreamRequest),
    )
    .await
    .expect("resource streams must not occupy the snapshot slot")
    .expect("snapshot stream should open");
    let snapshot_chunk = snapshot
        .next_chunk()
        .await
        .expect("snapshot chunk should be readable")
        .expect("snapshot stream should contain one chunk");
    assert_eq!(snapshot_chunk.as_ref(), b"snapshot");
    assert!(
        snapshot
            .next_chunk()
            .await
            .expect("snapshot completion should be readable")
            .is_none()
    );

    release.send_replace(true);
    for resource in [&mut first, &mut second] {
        let chunk = resource
            .next_chunk()
            .await
            .expect("resource chunk should be readable")
            .expect("resource stream should contain one chunk");
        assert_eq!(chunk.as_ref(), b"resource");
        assert!(
            resource
                .next_chunk()
                .await
                .expect("resource completion should be readable")
                .is_none()
        );
    }

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn streamed_response_times_out_when_its_producer_stops_making_progress() {
    let options = TransportOptions {
        progress_timeout: Duration::from_millis(50),
        ..TransportOptions::default()
    };
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports_with_options(options).await;
    transport_b
        .register_stream_handler::<ResourceStreamRequest, _, _>(|_context, _request| async {
            Ok(StreamingResponse::new(
                1,
                futures_util::stream::pending::<Result<ChargedBytes, Report<StreamHandlerError>>>(),
            ))
        })
        .expect("resource stream handler should register");
    timeout(Duration::from_secs(5), async {
        while !transport_a.is_connected_to(&node_b) {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the target should become ready");

    let mut response = transport_a
        .request_stream(&node_b, ResourceStreamRequest)
        .await
        .expect("stream response headers should arrive");
    let error = timeout(Duration::from_secs(1), response.next_chunk())
        .await
        .expect("the stalled response should honor its progress timeout")
        .expect_err("a stalled response must fail");
    assert!(matches!(
        error.current_context(),
        RequestError::Stream { .. }
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn streamed_response_reports_a_producer_failure_to_the_reader() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let release = StdArc::new(Notify::new());
    transport_b
        .register_stream_handler::<ResourceStreamRequest, _, _>({
            let release = StdArc::clone(&release);
            move |_context, _request| {
                let release = StdArc::clone(&release);
                async move {
                    Ok(StreamingResponse::new(
                        1,
                        futures_util::stream::once(async move {
                            release.notified().await;
                            Err(StreamHandlerError::with_cause(Report::new(
                                io::Error::other("resource read failed"),
                            )))
                        }),
                    ))
                }
            }
        })
        .expect("resource stream handler should register");
    let mut response = transport_a
        .request_stream(&node_b, ResourceStreamRequest)
        .await
        .expect("response headers should arrive before producer failure");
    release.notify_one();
    let error = timeout(Duration::from_secs(2), response.next_chunk())
        .await
        .expect("the failed producer should reset the response promptly")
        .expect_err("the reader must observe a failed stream");
    assert!(matches!(
        error.current_context(),
        RequestError::Stream { .. }
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn rejected_stream_opening_retains_the_transport_cause() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    transport_b
        .register_stream_handler::<ResourceStreamRequest, _, _>(|_context, _request| async {
            Err::<StreamingResponse, _>(StreamHandlerError::with_cause(Report::new(
                io::Error::other("stream source unavailable"),
            )))
        })
        .expect("resource stream handler should register");
    timeout(Duration::from_secs(5), async {
        while !transport_a.is_connected_to(&node_b) {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the target should become ready");

    let Err(error) = transport_a
        .request_stream(&node_b, ResourceStreamRequest)
        .await
    else {
        panic!("a rejected opening must fail before returning a stream");
    };
    assert!(matches!(
        error.current_context(),
        RequestError::Stream { .. }
    ));
    assert!(error.contains::<TransportError>());

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn stream_slot_queueing_consumes_the_request_deadline() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    transport_b
        .register_stream_handler::<DeadlineStreamRequest, _, _>(|_context, request| async move {
            if request.response_delay_ms != 0 {
                tokio::time::sleep(Duration::from_millis(request.response_delay_ms)).await;
            }
            Ok(StreamingResponse::new(
                1,
                futures_util::stream::pending::<Result<ChargedBytes, Report<StreamHandlerError>>>(),
            ))
        })
        .assured("the deadline stream handler has a unique test name");
    timeout(Duration::from_secs(5), async {
        while !transport_a.is_connected_to(&node_b) {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .assured("the connected test transports become ready");

    let held_stream = transport_a
        .request_stream(
            &node_b,
            DeadlineStreamRequest {
                response_delay_ms: 0,
            },
        )
        .await
        .assured("the first request holds the reserved snapshot stream slot");

    let queued_transport = transport_a.clone();
    let queued_node = node_b.clone();
    let queued = tokio::spawn(async move {
        queued_transport
            .request_stream(
                &queued_node,
                DeadlineStreamRequest {
                    response_delay_ms: 150,
                },
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(held_stream);

    let result = queued
        .await
        .assured("the queued stream request task remains attached");
    let error = match result {
        Ok(_) => panic!("queueing must consume the stream setup deadline"),
        Err(error) => error,
    };
    assert!(matches!(
        error.current_context(),
        RequestError::Stream { reason, .. } if reason.contains("timed out")
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn queued_replication_request_wakes_when_the_stream_slot_is_released() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let started = StdArc::new(Notify::new());
    let release = StdArc::new(Notify::new());
    transport_b
        .register_handler::<ReplicationRequest, _, _>({
            let started = StdArc::clone(&started);
            let release = StdArc::clone(&release);
            move |_context, request| {
                let started = StdArc::clone(&started);
                let release = StdArc::clone(&release);
                async move {
                    if request.wait {
                        started.notify_one();
                        release.notified().await;
                    }
                    ReplicationResponse
                }
            }
        })
        .expect("replication handler should register");
    timeout(Duration::from_secs(5), async {
        loop {
            tokio::task::consume_budget().await;
            if transport_a.is_connected_to(&node_b) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the target should become ready");

    let first_requester = transport_a.clone();
    let first_target = node_b.clone();
    let first = tokio::spawn(async move {
        first_requester
            .request(&first_target, ReplicationRequest { wait: true })
            .await
    });
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("the first replication request should hold the only stream slot");

    let second_requester = transport_a.clone();
    let second_target = node_b.clone();
    let second_started = StdArc::new(Notify::new());
    let second_started_in_task = StdArc::clone(&second_started);
    let second = tokio::spawn(async move {
        second_started_in_task.notify_one();
        second_requester
            .request(&second_target, ReplicationRequest { wait: false })
            .await
    });
    second_started.notified().await;
    tokio::task::yield_now().await;
    release.notify_one();

    timeout(Duration::from_secs(2), async {
        first
            .await
            .expect("first replication request task should join")
            .expect("first replication request should succeed");
        second
            .await
            .expect("second replication request task should join")
            .expect("queued replication request should wake and succeed");
    })
    .await
    .expect("queued replication work should advance without connection churn");

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn bulk_work_does_not_block_the_management_pool() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let started = StdArc::new(Notify::new());
    let release = StdArc::new(Notify::new());
    transport_b
        .register_handler::<BlockingBulkRequest, _, _>({
            let started = StdArc::clone(&started);
            let release = StdArc::clone(&release);
            move |_context, _request| {
                let started = StdArc::clone(&started);
                let release = StdArc::clone(&release);
                async move {
                    started.notify_one();
                    release.notified().await;
                    BlockingBulkResponse
                }
            }
        })
        .expect("bulk handler should register");
    transport_b
        .register_handler::<ManagementRequest, _, _>(|_context, _request| async move {
            ManagementResponse
        })
        .expect("management handler should register");

    let requester = transport_a.clone();
    let bulk_target = node_b.clone();
    let bulk =
        tokio::spawn(async move { requester.request(&bulk_target, BlockingBulkRequest).await });
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("bulk handler should start");
    let management = timeout(
        Duration::from_secs(2),
        transport_a.request(&node_b, ManagementRequest),
    )
    .await
    .expect("management request should not wait for bulk work")
    .expect("management request should succeed");
    assert_eq!(management, ManagementResponse);
    release.notify_one();
    assert_eq!(
        bulk.await
            .expect("bulk request task should join")
            .expect("bulk request should succeed"),
        BlockingBulkResponse
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn slow_management_work_cannot_consume_cancellation_streams() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let started = StdArc::new(AtomicUsize::new(0));
    let (release, release_rx) = watch::channel(false);
    transport_b
        .register_handler::<BlockingManagementRequest, _, _>({
            let started = StdArc::clone(&started);
            let release_rx = release_rx.clone();
            move |_context, _request| {
                let started = StdArc::clone(&started);
                let mut release_rx = release_rx.clone();
                async move {
                    started.fetch_add(1, Ordering::AcqRel);
                    release_rx
                        .wait_for(|released| *released)
                        .await
                        .expect("test release sender should remain open");
                    BlockingManagementResponse
                }
            }
        })
        .expect("blocking management handler should register");
    transport_b
        .register_handler::<CancellationRequest, _, _>(|_context, _request| async move {
            CancellationResponse
        })
        .expect("cancellation handler should register");

    let mut blocked = Vec::new();
    for _ in 0..connection::stream_slots::MANAGEMENT_SHARED_STREAMS {
        let requester = transport_a.clone();
        let target = node_b.clone();
        blocked.push(tokio::spawn(async move {
            requester.request(&target, BlockingManagementRequest).await
        }));
    }
    timeout(Duration::from_secs(2), async {
        loop {
            tokio::task::consume_budget().await;
            if started.load(Ordering::Acquire)
                == connection::stream_slots::MANAGEMENT_SHARED_STREAMS
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all shared management streams should become occupied");

    assert_eq!(
        timeout(
            Duration::from_secs(2),
            transport_a.request(&node_b, CancellationRequest),
        )
        .await
        .expect("cancellation must retain a physical management stream")
        .expect("cancellation request should succeed"),
        CancellationResponse
    );

    release.send_replace(true);
    for request in blocked {
        request
            .await
            .expect("blocking management request should join")
            .expect("blocking management request should finish after release");
    }
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

mod duplex;

mod progress;

#[tokio::test]
async fn discovery_subquota_cannot_crowd_out_management_requests() {
    let options = TransportOptions {
        incoming_queue_capacity: 1,
        ..TransportOptions::default()
    };
    let ConnectedTransports {
        _authority: authority,
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports_with_options(options.clone()).await;
    let node_c = ClusterNodeName::parse("node-c").expect("test node name should be valid");
    let (transport_c, _incoming_c) = Transport::bind(
        "127.0.0.1:0".parse().expect("test address should be valid"),
        localhost_identity("test-cluster", node_c.clone()),
        authority.issue("test-cluster", &node_c),
        options,
        Executor::default(),
        test_resolver().await,
    )
    .await
    .expect("third test transport should bind");
    transport_c
        .register_outbound_target(
            node_b.clone(),
            NodeEndpoint::new("localhost", transport_b.local_addr().port()),
        )
        .expect("third test transport should register its authenticated target");
    let started = StdArc::new(Notify::new());
    let release = StdArc::new(Notify::new());
    transport_b
        .register_handler::<BlockingDiscoveryRequest, _, _>({
            let started = StdArc::clone(&started);
            let release = StdArc::clone(&release);
            move |_context, _request| {
                let started = StdArc::clone(&started);
                let release = StdArc::clone(&release);
                async move {
                    started.notify_one();
                    release.notified().await;
                    BlockingDiscoveryResponse
                }
            }
        })
        .expect("discovery handler should register");
    transport_b
        .register_handler::<ManagementRequest, _, _>(|_context, _request| async move {
            ManagementResponse
        })
        .expect("management handler should register");

    let requester = transport_a.clone();
    let target = node_b.clone();
    let discovery =
        tokio::spawn(async move { requester.request(&target, BlockingDiscoveryRequest).await });
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("the first discovery handler should start");

    let error = transport_a
        .request_with_timeout(
            &node_b,
            BlockingDiscoveryRequest,
            Duration::from_millis(250),
        )
        .await
        .expect_err("the discovery subquota should reject excess work");
    assert!(matches!(
        error.current_context(),
        RequestError::AdmissionFull {
            request,
            subquota: RequestSubquota::Discovery,
        } if request == &BlockingDiscoveryRequest::NAME
    ));

    let error = transport_c
        .request_with_timeout(
            &node_b,
            BlockingDiscoveryRequest,
            Duration::from_millis(250),
        )
        .await
        .expect_err("the receiver should reject discovery beyond its subquota");
    assert!(matches!(
        error.current_context(),
        RequestError::RemoteRejected {
            failure: RemoteRequestFailure::AdmissionFull {
                subquota: RequestSubquota::Discovery,
            },
            ..
        }
    ));

    let management = transport_c
        .request(&node_b, ManagementRequest)
        .await
        .expect("reserved management capacity should remain available");
    assert_eq!(management, ManagementResponse);

    release.notify_waiters();
    assert_eq!(
        discovery
            .await
            .expect("discovery request task should join")
            .expect("the admitted discovery request should succeed"),
        BlockingDiscoveryResponse
    );
    transport_a.shutdown().await;
    transport_c.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn relay_terminal_capacity_is_held_until_the_application_finishes() {
    let options = TransportOptions {
        incoming_queue_capacity: 1,
        ..TransportOptions::default()
    };
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        _incoming_a: mut incoming_a,
        mut incoming_b,
        ..
    } = connected_transports_with_options(options).await;
    transport_b
        .register_outbound_target(
            node_a.clone(),
            NodeEndpoint::new("localhost", transport_a.local_addr().port()),
        )
        .expect("the response target should register");
    let payload = |ack_id, sequence, reply_node_id| RelayPayload {
        delivery: RelayDelivery {
            channel_incarnation: [1; 16],
            sequence,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        key: None,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .expect("the test relay body should fit its budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(RemoteAckRegistration {
            ack_id,
            reply_node_id,
        }),
    };

    let error = transport_a
        .send(
            &node_b,
            Envelope::RelayPayload(payload(0, 0, node_b.clone())),
        )
        .await
        .expect_err("the authenticated sender must own the declared admission reply");
    assert!(matches!(
        error.current_context(),
        TransportError::RemoteRejected { status: 403, .. }
    ));
    transport_a
        .send(
            &node_b,
            Envelope::RelayPayload(payload(1, 0, node_a.clone())),
        )
        .await
        .expect("the first relay should consume the sole terminal slot");
    let first = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the first relay should enter the application queue")
        .expect("the application queue should remain open");

    let error = transport_a
        .send(
            &node_b,
            Envelope::RelayPayload(payload(2, 1, node_a.clone())),
        )
        .await
        .expect_err("a second grant must wait until the first terminal outcome is sent");
    assert!(matches!(
        error.current_context(),
        TransportError::RemoteRejected { status: 429, .. }
    ));

    drop(first);
    transport_b
        .send(
            &node_a,
            Envelope::Ack(RemoteAckResolution {
                ack_id: 1,
                outcome: RemoteAckOutcome::Ack,
            }),
        )
        .await
        .expect("the first relay terminal outcome should be sent");
    let _terminal = timeout(Duration::from_secs(2), incoming_a.recv())
        .await
        .expect("the terminal outcome should enter the sender application queue")
        .expect("the sender application queue should remain open");
    transport_a
        .send(
            &node_b,
            Envelope::RelayPayload(payload(2, 1, node_a.clone())),
        )
        .await
        .expect("the next relay grant should fit after the terminal outcome");

    timeout(Duration::from_secs(1), async {
        transport_a.shutdown().await;
        transport_b.shutdown().await;
    })
    .await
    .expect("redeemed grant expiry work should not delay transport shutdown");
}

#[tokio::test]
async fn terminal_relay_outcome_waits_for_application_queue_capacity() {
    let options = TransportOptions {
        incoming_queue_capacity: 1,
        ..TransportOptions::default()
    };
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        _incoming_a: mut incoming_a,
        mut incoming_b,
        ..
    } = connected_transports_with_options(options).await;
    transport_b
        .register_outbound_target(
            node_a.clone(),
            NodeEndpoint::new("localhost", transport_a.local_addr().port()),
        )
        .expect("the response target should register");
    transport_b
        .send(&node_a, Envelope::Control(ControlEnvelope::Terminate))
        .await
        .expect("the general message should fill the sender application queue");

    transport_a
        .send(
            &node_b,
            Envelope::RelayPayload(RelayPayload {
                delivery: RelayDelivery {
                    channel_incarnation: [11; 16],
                    sequence: 0,
                },
                kind: RelayPayloadKind::Routed,
                domain: DomainName::parse("test").expect("test domain should be valid"),
                relay: RelayName::parse("relay").expect("test relay should be valid"),
                key: None,
                batch_ipc: Executor::default()
                    .try_charge_owned(MemoryClass::Relay, vec![1])
                    .expect("the test relay body should fit its budget"),
                metadata: Vec::new(),
                acks: Vec::new(),
                admission: Some(RemoteAckRegistration {
                    ack_id: 52,
                    reply_node_id: node_a.clone(),
                }),
            }),
        )
        .await
        .expect("the relay body should reach the receiver admission queue");
    let received = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the relay body should enter the receiver application queue")
        .expect("the receiver application queue should remain open");
    assert_eq!(
        received
            .relay_admission
            .expect("a relay body must carry its reserved admission")
            .admit(),
        RelayAdmissionDecision::Admitted
    );

    let outcome_sender = transport_b.clone();
    let outcome_target = node_a.clone();
    let mut outcome_task = tokio::spawn(async move {
        outcome_sender
            .send(
                &outcome_target,
                Envelope::Ack(RemoteAckResolution {
                    ack_id: 52,
                    outcome: RemoteAckOutcome::Ack,
                }),
            )
            .await
    });
    assert!(
        timeout(Duration::from_millis(100), &mut outcome_task)
            .await
            .is_err(),
        "a terminal outcome should wait while the general application queue is full"
    );
    let queued = incoming_a
        .recv()
        .await
        .expect("the sender application queue should contain the general message");
    assert!(matches!(
        queued.envelope,
        Envelope::Control(ControlEnvelope::Terminate)
    ));
    outcome_task
        .await
        .expect("the terminal outcome task should join")
        .expect("the terminal outcome should send after capacity becomes available");
    let outcome = incoming_a
        .recv()
        .await
        .expect("the terminal outcome must remain queued for the application");
    assert!(matches!(
        outcome.envelope,
        Envelope::Ack(RemoteAckResolution {
            ack_id: 52,
            outcome: RemoteAckOutcome::Ack,
        })
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn confirmed_cancellation_fences_attempt_before_grant_arrives() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        _incoming_a: _,
        mut incoming_b,
        ..
    } = connected_transports().await;
    let delivery = RelayDelivery {
        channel_incarnation: [6; 16],
        sequence: 0,
    };

    assert_eq!(
        transport_a
            .cancel_relay(&node_b, delivery)
            .await
            .expect("relay cancellation should be answered"),
        RelayAdmissionStatus::Cancelled
    );

    let error = transport_a
        .send(
            &node_b,
            Envelope::RelayPayload(RelayPayload {
                delivery,
                kind: RelayPayloadKind::Routed,
                domain: DomainName::parse("test").expect("test domain should be valid"),
                relay: RelayName::parse("relay").expect("test relay should be valid"),
                key: None,
                batch_ipc: Executor::default()
                    .try_charge_owned(MemoryClass::Relay, vec![1])
                    .expect("the test relay body should fit its budget"),
                metadata: Vec::new(),
                acks: Vec::new(),
                admission: Some(RemoteAckRegistration {
                    ack_id: 40,
                    reply_node_id: node_a,
                }),
            }),
        )
        .await
        .expect_err("a confirmed cancellation must fence a later grant");
    assert!(matches!(
        error.current_context(),
        TransportError::RelayCancelled
    ));
    assert!(
        timeout(Duration::from_millis(100), incoming_b.recv())
            .await
            .is_err(),
        "a delivery cancelled before its grant must never enter the application queue"
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn cancelled_relay_admission_can_never_reach_runtime() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        _incoming_a: _,
        mut incoming_b,
        ..
    } = connected_transports().await;
    let delivery = RelayDelivery {
        channel_incarnation: [7; 16],
        sequence: 0,
    };
    let payload = RelayPayload {
        delivery,
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        key: None,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .expect("the test relay body should fit its budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(RemoteAckRegistration {
            ack_id: 41,
            reply_node_id: node_a.clone(),
        }),
    };

    transport_a
        .send(&node_b, Envelope::RelayPayload(payload.clone()))
        .await
        .expect("the relay body should reach the receiver admission queue");
    let received = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the relay body should enter the application queue")
        .expect("the application queue should remain open");

    assert_eq!(
        transport_a
            .relay_admission_status(&node_b, delivery)
            .await
            .expect("relay status should be answered"),
        RelayAdmissionStatus::BodyReceived
    );
    transport_a
        .send(&node_b, Envelope::RelayPayload(payload.clone()))
        .await
        .expect("a same-epoch retry should reconcile the received body");
    assert!(
        timeout(Duration::from_millis(100), incoming_b.recv())
            .await
            .is_err(),
        "reconciliation must not enqueue the same delivery twice"
    );

    assert_eq!(
        transport_a
            .cancel_relay(&node_b, delivery)
            .await
            .expect("relay cancellation should be answered"),
        RelayAdmissionStatus::Cancelled
    );
    assert_eq!(
        received
            .relay_admission
            .expect("a relay body must carry its reserved admission")
            .admit(),
        RelayAdmissionDecision::Cancelled
    );
    assert_eq!(
        transport_a
            .relay_admission_status(&node_b, delivery)
            .await
            .expect("relay status should be answered"),
        RelayAdmissionStatus::Cancelled
    );

    let error = transport_a
        .send(&node_b, Envelope::RelayPayload(payload))
        .await
        .expect_err("a cancelled delivery identity must remain fenced");
    assert!(matches!(
        error.current_context(),
        TransportError::RelayCancelled
    ));
    let next_delivery = RelayDelivery {
        channel_incarnation: [7; 16],
        sequence: 1,
    };
    let next_payload = RelayPayload {
        delivery: next_delivery,
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        key: None,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .expect("the test relay body should fit its budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(RemoteAckRegistration {
            ack_id: 42,
            reply_node_id: node_a.clone(),
        }),
    };
    transport_a
        .send(&node_b, Envelope::RelayPayload(next_payload))
        .await
        .expect("the next channel sequence should reach the receiver");
    let next_received = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the next relay body should enter the application queue")
        .expect("the application queue should remain open");
    assert_eq!(
        transport_a
            .cancel_relay(&node_b, next_delivery)
            .await
            .expect("the next relay cancellation should be answered"),
        RelayAdmissionStatus::Cancelled
    );
    assert_eq!(
        next_received
            .relay_admission
            .expect("the next relay body must carry its reserved admission")
            .admit(),
        RelayAdmissionDecision::Cancelled
    );

    let retired_payload = RelayPayload {
        delivery: RelayDelivery {
            channel_incarnation: [7; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        key: None,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .expect("the test relay body should fit its budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(RemoteAckRegistration {
            ack_id: 43,
            reply_node_id: node_a.clone(),
        }),
    };
    let error = transport_a
        .send(&node_b, Envelope::RelayPayload(retired_payload.clone()))
        .await
        .expect_err("a delivery below the reconciled watermark is indeterminate");
    assert!(matches!(
        error.current_context(),
        TransportError::RelayIndeterminate
    ));
    assert!(
        timeout(Duration::from_millis(100), incoming_b.recv())
            .await
            .is_err(),
        "a cancelled relay delivery must not enter the application queue again"
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn same_epoch_retry_of_admitted_relay_does_not_enqueue_twice() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        _incoming_a: mut incoming_a,
        mut incoming_b,
        ..
    } = connected_transports().await;
    let delivery = RelayDelivery {
        channel_incarnation: [10; 16],
        sequence: 0,
    };
    let payload = RelayPayload {
        delivery,
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        key: None,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .expect("the test relay body should fit its budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(RemoteAckRegistration {
            ack_id: 44,
            reply_node_id: node_a,
        }),
    };

    transport_a
        .send(&node_b, Envelope::RelayPayload(payload.clone()))
        .await
        .expect("the relay body should reach the receiver admission queue");
    let received = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the relay body should enter the application queue")
        .expect("the application queue should remain open");
    assert_eq!(
        received
            .relay_admission
            .expect("a relay body must carry its reserved admission")
            .admit(),
        RelayAdmissionDecision::Admitted
    );

    transport_a
        .send(&node_b, Envelope::RelayPayload(payload))
        .await
        .expect("a same-epoch retry should reconcile the admitted delivery");
    assert!(
        timeout(Duration::from_millis(100), incoming_b.recv())
            .await
            .is_err(),
        "an admitted relay retry must not enter the application queue twice"
    );
    let outcome = timeout(Duration::from_secs(2), incoming_a.recv())
        .await
        .expect("the reconciled admission should return its terminal outcome")
        .expect("the sender application queue should remain open");
    assert!(matches!(
        outcome.envelope,
        Envelope::Ack(RemoteAckResolution {
            ack_id: 44,
            outcome: RemoteAckOutcome::Ack,
        })
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn receiver_process_restart_makes_unresolved_relay_indeterminate() {
    let ConnectedTransports {
        _authority: authority,
        transport_a,
        transport_b,
        node_a,
        node_b,
        executor_b: _,
        _incoming_a: _,
        mut incoming_b,
    } = connected_transports().await;
    let delivery = RelayDelivery {
        channel_incarnation: [8; 16],
        sequence: 0,
    };
    transport_a
        .send(
            &node_b,
            Envelope::RelayPayload(RelayPayload {
                delivery,
                kind: RelayPayloadKind::Routed,
                domain: DomainName::parse("test").expect("test domain should be valid"),
                relay: RelayName::parse("relay").expect("test relay should be valid"),
                key: None,
                batch_ipc: Executor::default()
                    .try_charge_owned(MemoryClass::Relay, vec![1])
                    .expect("the test relay body should fit its budget"),
                metadata: Vec::new(),
                acks: Vec::new(),
                admission: Some(RemoteAckRegistration {
                    ack_id: 45,
                    reply_node_id: node_a.clone(),
                }),
            }),
        )
        .await
        .expect("the unresolved relay should reach the receiver process");
    let unresolved = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the unresolved relay should enter the application queue")
        .expect("the application queue should remain open");

    transport_b.shutdown().await;
    drop(unresolved);
    let (replacement_b, _replacement_incoming) = Transport::bind(
        "127.0.0.1:0".parse().expect("test address should be valid"),
        localhost_identity("test-cluster", node_b.clone()),
        authority.issue("test-cluster", &node_b),
        TransportOptions::default(),
        Executor::default(),
        test_resolver().await,
    )
    .await
    .expect("the replacement receiver process should bind");
    replacement_b.replace_live_nodes(&BTreeSet::from([node_a, node_b.clone()]));
    transport_a
        .register_outbound_target(
            node_b.clone(),
            NodeEndpoint::new("localhost", replacement_b.local_addr().port()),
        )
        .expect("the replacement receiver target should register");

    assert_eq!(
        timeout(
            Duration::from_secs(5),
            transport_a.relay_admission_status(&node_b, delivery),
        )
        .await
        .expect("the sender should reconnect to the replacement process")
        .expect("the replacement process should answer relay status"),
        RelayAdmissionStatus::Indeterminate
    );

    transport_a.shutdown().await;
    replacement_b.shutdown().await;
}

#[tokio::test]
async fn reserved_relay_work_reports_progress_before_runtime_admission() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        _incoming_a: mut incoming_a,
        mut incoming_b,
        ..
    } = connected_transports().await;
    transport_b
        .register_outbound_target(
            node_a.clone(),
            NodeEndpoint::new("localhost", transport_a.local_addr().port()),
        )
        .expect("the progress response target should register");
    transport_a
        .send(
            &node_b,
            Envelope::RelayPayload(RelayPayload {
                delivery: RelayDelivery {
                    channel_incarnation: [9; 16],
                    sequence: 0,
                },
                kind: RelayPayloadKind::Routed,
                domain: DomainName::parse("test").expect("test domain should be valid"),
                relay: RelayName::parse("relay").expect("test relay should be valid"),
                key: None,
                batch_ipc: Executor::default()
                    .try_charge_owned(MemoryClass::Relay, vec![1])
                    .expect("the test relay body should fit its budget"),
                metadata: Vec::new(),
                acks: Vec::new(),
                admission: Some(RemoteAckRegistration {
                    ack_id: 51,
                    reply_node_id: node_a.clone(),
                }),
            }),
        )
        .await
        .expect("the relay body should reach the receiver admission queue");
    let _reserved = timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .expect("the relay body should enter the application queue")
        .expect("the application queue should remain open");

    let progress = timeout(Duration::from_secs(1), incoming_a.recv())
        .await
        .expect("reserved relay work should report queue-time progress")
        .expect("the sender application queue should remain open");
    assert!(matches!(
        progress.envelope,
        Envelope::Ack(RemoteAckResolution {
            ack_id: 51,
            outcome: RemoteAckOutcome::Alive,
        })
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn bootstrap_rejects_a_certificate_from_another_cluster() {
    let authority = TestCertificateAuthority::new();
    let node_a = ClusterNodeName::parse("node-a").expect("test node name should be valid");
    let node_b = ClusterNodeName::parse("node-b").expect("test node name should be valid");
    let (transport_a, _incoming_a) = Transport::bind(
        "127.0.0.1:0".parse().expect("test address should be valid"),
        localhost_identity("cluster-a", node_a.clone()),
        authority.issue("cluster-a", &node_a),
        TransportOptions::default(),
        Executor::default(),
        test_resolver().await,
    )
    .await
    .expect("first test transport should bind");
    let (transport_b, _incoming_b) = Transport::bind(
        "127.0.0.1:0".parse().expect("test address should be valid"),
        localhost_identity("cluster-b", node_b.clone()),
        authority.issue("cluster-b", &node_b),
        TransportOptions::default(),
        Executor::default(),
        test_resolver().await,
    )
    .await
    .expect("second test transport should bind");

    let error = transport_a
        .bootstrap_target(PeerTarget::new(transport_b.local_addr(), "localhost"))
        .await
        .expect_err("a peer certificate from another cluster must be rejected");
    assert!(matches!(
        error.current_context(),
        TransportError::InvalidHandshake(_)
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn bootstrap_times_out_when_a_tcp_peer_never_completes_tls() {
    let authority = TestCertificateAuthority::new();
    let node_a = ClusterNodeName::parse("node-a").expect("test node name should be valid");
    let options = TransportOptions {
        connection_setup_timeout: Duration::from_millis(100),
        ..TransportOptions::default()
    };
    let (transport_a, _incoming_a) = Transport::bind(
        "127.0.0.1:0".parse().expect("test address should be valid"),
        localhost_identity("test-cluster", node_a.clone()),
        authority.issue("test-cluster", &node_a),
        options,
        Executor::default(),
        test_resolver().await,
    )
    .await
    .expect("test transport should bind");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("unresponsive TCP peer should bind");
    let error = timeout(
        Duration::from_secs(2),
        transport_a.bootstrap_target(PeerTarget::new(
            listener
                .local_addr()
                .expect("test peer should have an address"),
            "localhost",
        )),
    )
    .await
    .expect("bootstrap must honor its setup deadline")
    .expect_err("an unresponsive TLS peer must fail");
    assert!(matches!(
        error.current_context(),
        TransportError::ConnectionSetupTimeout { timeout, .. }
            if *timeout == Duration::from_millis(100)
    ));

    transport_a.shutdown().await;
}

#[tokio::test]
async fn shutdown_refuses_bootstrap_before_opening_a_connection() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        ..
    } = bound_transports_with_options(TransportOptions::default()).await;
    transport_a.shutdown().await;
    let error = transport_a
        .bootstrap_target(PeerTarget::new(transport_b.local_addr(), "localhost"))
        .await
        .expect_err("shutdown must fence new bootstrap attempts");
    assert!(matches!(
        error.current_context(),
        TransportError::ShuttingDown
    ));
    transport_b.shutdown().await;
}

#[tokio::test]
async fn invalid_rkyv_is_rejected_before_dispatch() {
    let executor = Executor::default();
    let bytes = executor
        .try_charge_owned(MemoryClass::Commands, vec![0xff; 32])
        .expect("test payload should fit the command budget");

    let result = wire::decode_rkyv::<ControlEnvelope>(
        &executor,
        MemoryClass::Commands,
        CpuClass::Control,
        bytes,
    )
    .await;

    let Err(error) = result else {
        panic!("invalid rkyv must be rejected before dispatch");
    };
    assert!(matches!(error.current_context(), TransportError::Decode(_)));
    assert!(error.contains::<rkyv::rancor::Error>());
}

#[test]
fn stream_handler_failure_keeps_its_producer_cause() {
    let cause = Report::new(io::Error::other("producer failed"));
    let error = StreamHandlerError::with_cause(cause);
    assert_eq!(error.current_context().to_string(), "producer failed");
    assert!(error.contains::<io::Error>());
}

#[tokio::test]
async fn membership_removal_cancels_an_active_request() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        ..
    } = connected_transports().await;
    let started = StdArc::new(Notify::new());
    transport_b
        .register_handler::<HangingRequest, _, _>({
            let started = StdArc::clone(&started);
            move |_context, _request| {
                let started = StdArc::clone(&started);
                async move {
                    started.notify_one();
                    std::future::pending().await
                }
            }
        })
        .expect("hanging handler should register");
    let requester = transport_a.clone();
    let target = node_b.clone();
    let request = tokio::spawn(async move { requester.request(&target, HangingRequest).await });
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("hanging request should reach its handler");

    transport_a.replace_live_nodes(&BTreeSet::from([node_a]));
    let error = timeout(Duration::from_secs(2), request)
        .await
        .expect("membership removal should cancel the request")
        .expect("request task should join")
        .expect_err("request should report target departure");
    assert!(matches!(
        error.current_context(),
        RequestError::TargetLeft { node, request }
            if node == &node_b && request == &HangingRequest::NAME
    ));

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn shutdown_cancels_an_active_request() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let started = StdArc::new(Notify::new());
    transport_b
        .register_handler::<HangingRequest, _, _>({
            let started = StdArc::clone(&started);
            move |_context, _request| {
                let started = StdArc::clone(&started);
                async move {
                    started.notify_one();
                    std::future::pending().await
                }
            }
        })
        .expect("hanging handler should register");
    let requester = transport_a.clone();
    let target = node_b.clone();
    let request = tokio::spawn(async move { requester.request(&target, HangingRequest).await });
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("hanging request should reach its handler");

    timeout(Duration::from_secs(2), transport_a.shutdown())
        .await
        .expect("transport shutdown should respect its drain bound");
    let error = timeout(Duration::from_secs(2), request)
        .await
        .expect("shutdown should cancel the request")
        .expect("request task should join")
        .expect_err("request should report shutdown");
    assert!(matches!(
        error.current_context(),
        RequestError::ShuttingDown { node, request }
            if node == &node_b && request == &HangingRequest::NAME
    ));

    transport_b.shutdown().await;
}
