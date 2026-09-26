use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::{Arc as StdArc, LazyLock, OnceLock},
    time::{Duration, Instant, SystemTime},
};

use arch_into::ArchInto as _;
use async_nats::Client as NatsClient;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::{Client as SqsClient, types::QueueAttributeName};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use error_stack::Report;
use fjall::Database;
use futures_util::SinkExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use meticulous::ResultExt as _;
use nervix_approx_into::ApproxInto as _;
use nervix_client_core::{Client, ConnectOptions, TlsRequirement};
use nervix_client_wire::UploadReply;
use nervix_connector_kafka::testing_rdkafka::{
    admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    error::RDKafkaErrorCode,
    message::{Header as KafkaHeader, Headers, Message, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
};
use nervix_consensus::RaftRetentionPolicy;
use nervix_dns::{DnsConfiguration, DnsResolver};
use nervix_execution::Executor;
use nervix_interconnect::{
    ControlEnvelope, Envelope, PeerResolver, PeerTarget, RuntimeErrorEvent, TlsConfigBundle,
    Transport, TransportClock, TransportIdentity, TransportOptions,
};
use nervix_models::{ClusterNodeName, NodeEndpoint};

/// Cucumber node ids are fixed strings from the feature files, so they always parse.
pub(crate) fn node_name(raw: &str) -> ClusterNodeName {
    ClusterNodeName::parse(raw).expect("cucumber node ids are valid cluster node names")
}
use nervix_server::{
    FaultInjection, SchedulerMode,
    application::{
        Application, CommandExecutionPolicy, InternalTransportMode, ShutdownCoordinator,
        init_tracing_to_file,
    },
    memory_pressure::MemoryPressureConfig,
    runtime::{DEFAULT_DOMAIN_DRAIN_TIMEOUT, DEFAULT_TEMP_DIR, branch_task_stop_timeout},
};
use parking_lot::Mutex;
use pulsar::{
    ConsumerOptions as PulsarConsumerOptions, Pulsar, SubType as PulsarSubType, TokioExecutor,
    consumer::InitialPosition as PulsarInitialPosition,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType, date_time_ymd,
};
use redis::AsyncCommands;
use rumqttc::{
    AsyncClient, Event as MqttEvent, Incoming, MqttOptions, PublishOptions, QoS, SessionMode,
};
use rustls::{
    ClientConfig as RustlsClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use rustls_pki_types::pem::PemObject;
use tempfile::{TempDir, tempdir};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener as TokioTcpListener, TcpStream},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_rustls::TlsConnector;
use tokio_stream::StreamExt;
use tokio_tungstenite::{
    WebSocketStream, client_async, connect_async,
    tungstenite::{Message as WsMessage, client::IntoClientRequest, http::HeaderValue},
};
use tokio_util::sync::CancellationToken;
use triomphe::Arc;
use uuid::Uuid;
use zeromq::{PullSocket, PushSocket, Socket, SocketRecv, SocketSend};

pub(crate) use super::raw_session::{TestSession, open_raw_session};
use super::{
    cluster_teardown::{CLUSTER_TEARDOWN_BUDGET, ClusterTeardown, TeardownNode},
    dependencies::{
        DependencyEndpoints, KAFKA_ADDR, MQTT_ADDR, NATS_ADDR, NATS_TLS_ADDR, PULSAR_ADDR,
        PULSAR_TLS_ADDR, RABBITMQ_ADDR, REDIS_ADDR, SQS_ENDPOINT, SQS_TLS_ENDPOINT,
    },
    node_liveness::{
        NodeStartupError, NodeTaskTerminalOutcome, NodeTaskWaitOutcome, OwnedNodeTask,
        ReadinessProbeOutcome,
    },
    node_startup::{
        ATTEMPT_READINESS_BUDGET, AttemptCleanup, NODE_STARTUP_BUDGET, NodeStartup, StartableNode,
        cluster_startup_budget,
    },
    peer_addressing::{
        ClusterDns, FixtureAnswer, InterconnectAddress, PeerAddressing, PublishedNode,
        qualified_name,
    },
    phase_deadline::PhaseDeadline,
    port_pool::{next_ports, release_test_ports},
    raw_session::{TestUpload, send_upload},
    redis_client::TestRedisClient,
    scenario_phase::ScenarioIdentity,
    status_request::{
        STATUS_DIAGNOSTIC_BUDGET, STATUS_REQUEST_TIMEOUT, STATUS_WAIT_BUDGET, StatusEndpoint,
        StatusRequestError, StatusTransport,
    },
    suite_watchdog::{LiveClusterHandle, LiveClusterRegistration, NodeStop},
};

const HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const TEST_NODE_UNAVAILABILITY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_TEST_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DOMAIN_CLOCK_AUTHORITY_OBSERVATION_TIMEOUT: Duration =
    match TEST_NODE_UNAVAILABILITY_TIMEOUT.checked_add(STATUS_WAIT_BUDGET) {
        Some(timeout) => timeout,
        None => panic!(
            "the test node-unavailability timeout and cluster status observation budget must fit \
             in Duration"
        ),
    };
const BROKER_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RUNTIME_BRANCH_STOP_TIMEOUT: Duration =
    branch_task_stop_timeout(DEFAULT_DOMAIN_DRAIN_TIMEOUT);
const DEFAULT_GRACEFUL_SHUTDOWN_PHASE_FLOOR: Duration =
    match DEFAULT_TEST_DRAIN_TIMEOUT.checked_add(DEFAULT_RUNTIME_BRANCH_STOP_TIMEOUT) {
        Some(timeout) => timeout,
        None => panic!("the default node shutdown phase budgets must fit in Duration"),
    };
/// Runtime shutdown waits through a data-dependent number of task deadlines sequentially, and
/// consensus and database finalization are event-driven. This is therefore an outer liveness
/// policy, not an upper bound on valid shutdown. Configured bounded phases may raise its floor.
const NODE_SHUTDOWN_LIVENESS_WATCHDOG: Duration = Duration::from_secs(5 * 60);
const _: () = assert!(
    DEFAULT_RUNTIME_BRANCH_STOP_TIMEOUT.as_nanos() > DEFAULT_DOMAIN_DRAIN_TIMEOUT.as_nanos(),
    "the runtime branch stop budget must include its post-drain grace"
);
const _: () = assert!(
    NODE_SHUTDOWN_LIVENESS_WATCHDOG.as_nanos() >= DEFAULT_GRACEFUL_SHUTDOWN_PHASE_FLOOR.as_nanos(),
    "the node shutdown watchdog must cover the default bounded shutdown phases"
);
/// A node's shutdown deadline unless a scenario configures its own. It leaves the bounded phases
/// that scenarios configure by default room to finish, so only a scenario about the deadline
/// reaches it.
const DEFAULT_TEST_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(4 * 60);
const _: () = assert!(
    DEFAULT_GRACEFUL_SHUTDOWN_PHASE_FLOOR.as_nanos() < DEFAULT_TEST_SHUTDOWN_TIMEOUT.as_nanos(),
    "the default test shutdown timeout must cover the default bounded shutdown phases"
);
const _: () = assert!(
    DEFAULT_TEST_SHUTDOWN_TIMEOUT.as_nanos() < NODE_SHUTDOWN_LIVENESS_WATCHDOG.as_nanos(),
    "the node shutdown watchdog must outlast the default test shutdown timeout"
);
const _: () = assert!(
    CLUSTER_TEARDOWN_BUDGET.as_nanos() < NODE_SHUTDOWN_LIVENESS_WATCHDOG.as_nanos(),
    "scenario cleanup must end well before the watchdog of a node a scenario stops itself"
);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const SQS_REGION: &str = "us-east-1";
const TEST_LOG_DIR: &str = "tests/logs";
const TEST_LOG_FILE: &str = "tests/logs/scenarios.log";
const TEST_RAFT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const TEST_RAFT_ELECTION_TIMEOUT_MIN: Duration = Duration::from_secs(1);
const TEST_RAFT_ELECTION_TIMEOUT_MAX: Duration = Duration::from_secs(2);
const TEST_REPLICA_COUNT: usize = 0;
const TEST_STATE_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const TEST_AUTH_USERNAME: &str = "default";
pub(crate) const TEST_AUTH_PASSWORD: &str = "nervix-test-password";
static DEV_TLS_READY: OnceLock<io::Result<()>> = OnceLock::new();
static TEST_LOG_TRUNCATED: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

#[derive(Debug)]
pub(crate) struct StallableTcpProxy {
    local_addr: std::net::SocketAddr,
    paused: watch::Sender<bool>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl StallableTcpProxy {
    pub(crate) async fn start(target_host: String, target_port: u16) -> io::Result<Self> {
        let listener = TokioTcpListener::bind((HOST, 0)).await?;
        let local_addr = listener.local_addr()?;
        let (paused, paused_rx) = watch::channel(false);
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                let accepted = tokio::select! {
                    _ = task_cancellation.cancelled() => return,
                    accepted = listener.accept() => accepted,
                };
                let Ok((downstream, _)) = accepted else {
                    return;
                };
                let target_host = target_host.clone();
                let paused = paused_rx.clone();
                let connection_cancellation = task_cancellation.child_token();
                tokio::spawn(async move {
                    let Ok(upstream) =
                        TcpStream::connect((target_host.as_str(), target_port)).await
                    else {
                        return;
                    };
                    let (downstream_read, downstream_write) = downstream.into_split();
                    let (upstream_read, upstream_write) = upstream.into_split();
                    let upstream_copy = copy_through_stall_gate(
                        downstream_read,
                        upstream_write,
                        paused.clone(),
                        connection_cancellation.clone(),
                    );
                    let downstream_copy = copy_through_stall_gate(
                        upstream_read,
                        downstream_write,
                        paused,
                        connection_cancellation,
                    );
                    let _ = tokio::join!(upstream_copy, downstream_copy);
                });
            }
        });
        Ok(Self {
            local_addr,
            paused,
            cancellation,
            task,
        })
    }

    pub(crate) fn local_port(&self) -> u16 {
        self.local_addr.port()
    }

    pub(crate) fn set_paused(&self, paused: bool) {
        self.paused.send_replace(paused);
    }
}

impl Drop for StallableTcpProxy {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

async fn copy_through_stall_gate<Reader, Writer>(
    mut reader: Reader,
    mut writer: Writer,
    mut paused: watch::Receiver<bool>,
    cancellation: CancellationToken,
) -> io::Result<()>
where
    Reader: AsyncRead + Unpin,
    Writer: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        tokio::task::consume_budget().await;
        wait_for_proxy_resume(&mut paused, &cancellation).await?;
        let read = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            changed = paused.changed() => {
                changed.map_err(io::Error::other)?;
                continue;
            }
            read = reader.read(&mut buffer) => read?,
        };
        if read == 0 {
            writer.shutdown().await?;
            return Ok(());
        }
        wait_for_proxy_resume(&mut paused, &cancellation).await?;
        writer.write_all(&buffer[..read]).await?;
    }
}

async fn wait_for_proxy_resume(
    paused: &mut watch::Receiver<bool>,
    cancellation: &CancellationToken,
) -> io::Result<()> {
    while *paused.borrow_and_update() {
        tokio::task::consume_budget().await;
        tokio::select! {
            _ = cancellation.cancelled() => return Err(io::Error::other("TCP proxy stopped")),
            changed = paused.changed() => changed.map_err(io::Error::other)?,
        }
    }
    Ok(())
}

pub(crate) fn test_basic_auth_token_for_password(password: &str) -> String {
    BASE64_STANDARD.encode(format!("{TEST_AUTH_USERNAME}:{password}"))
}

pub(crate) fn test_basic_authorization_for_password(password: &str) -> String {
    format!("Basic {}", test_basic_auth_token_for_password(password))
}

pub(crate) fn test_basic_auth_token() -> String {
    test_basic_auth_token_for_password(TEST_AUTH_PASSWORD)
}

pub(crate) fn test_basic_authorization() -> String {
    test_basic_authorization_for_password(TEST_AUTH_PASSWORD)
}

fn truncate_test_log_once() -> io::Result<()> {
    let mut truncated = TEST_LOG_TRUNCATED.lock();
    if !*truncated {
        std::fs::create_dir_all(TEST_LOG_DIR)?;
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(TEST_LOG_FILE)?;
        *truncated = true;
    }
    Ok(())
}

fn parse_addr(input: &str) -> io::Result<std::net::SocketAddr> {
    input.parse().map_err(io::Error::other)
}

async fn database_opens(path: PathBuf) -> io::Result<()> {
    tokio::task::spawn_blocking(move || {
        let database = Database::builder(path).open().map_err(io::Error::other)?;
        drop(database);
        Ok(())
    })
    .await
    .map_err(io::Error::other)?
}

fn observability_metric_has_value(
    body: &str,
    metric_name: &str,
    label_fragments: &[String],
    expected_value: i64,
    matching_lines: &mut Vec<String>,
) -> bool {
    matching_lines.clear();
    for line in body.lines() {
        if line.starts_with('#') || !line_starts_with_metric(line, metric_name) {
            continue;
        }
        if !label_fragments
            .iter()
            .all(|fragment| line.contains(fragment.as_str()))
        {
            continue;
        }
        matching_lines.push(line.to_string());
        if let Some(value) = parse_prometheus_sample_value(line)
            && (value - expected_value.approx_into::<f64>()).abs() < f64::EPSILON
        {
            return true;
        }
    }
    false
}

fn observability_metric_reaches(
    body: &str,
    metric_name: &str,
    label_fragments: &[String],
    minimum_value: i64,
    matching_lines: &mut Vec<String>,
) -> bool {
    matching_lines.clear();
    for line in body.lines() {
        if line.starts_with('#') || !line_starts_with_metric(line, metric_name) {
            continue;
        }
        if !label_fragments
            .iter()
            .all(|fragment| line.contains(fragment.as_str()))
        {
            continue;
        }
        matching_lines.push(line.to_string());
        if let Some(value) = parse_prometheus_sample_value(line)
            && value >= minimum_value.approx_into::<f64>()
        {
            return true;
        }
    }
    false
}

/// The highest value one series reached for as long as it was sampled.
///
/// A gauge that has to stay inside a bound is not proved by one reading: the value a step happens
/// to catch says nothing about the peak the work under test reached between readings. This samples
/// for the whole time the work runs and keeps the highest value seen, so the assertion is about the
/// window rather than an instant inside it. Scrape failures are skipped rather than ending the
/// sampling, because a node that is busy answering the work under test is the case being measured.
pub(crate) async fn sample_peak_observability_metric(
    url: String,
    metric_name: String,
    label_fragments: Vec<String>,
    cancellation: tokio_util::sync::CancellationToken,
) -> f64 {
    let client = reqwest::Client::new();
    let mut peak = 0.0_f64;
    loop {
        tokio::task::consume_budget().await;
        if let Ok(response) = client.get(&url).send().await
            && let Ok(body) = response.text().await
        {
            for line in body.lines() {
                if line.starts_with('#') || !line_starts_with_metric(line, &metric_name) {
                    continue;
                }
                if !label_fragments
                    .iter()
                    .all(|fragment| line.contains(fragment.as_str()))
                {
                    continue;
                }
                if let Some(value) = parse_prometheus_sample_value(line) {
                    peak = peak.max(value);
                }
            }
        }
        tokio::select! {
            () = cancellation.cancelled() => return peak,
            () = sleep(POLL_INTERVAL) => {}
        }
    }
}

/// The label names an interconnection series may carry. Every dimension here is a closed set of
/// values fixed at compile time, so no branch key, peer identity or operation id can widen one.
const BOUNDED_INTERCONNECTION_LABELS: &[&str] =
    &["class", "direction", "reason", "outcome", "operation"];

/// The metric name prefixes the interconnection qualification owns.
const INTERCONNECTION_METRIC_PREFIXES: &[&str] = &[
    "nervix_interconnect_",
    "nervix_execution_",
    "nervix_consensus_",
    "nervix_node_scheduler_",
];

/// Every interconnection sample whose label set is outside [`BOUNDED_INTERCONNECTION_LABELS`].
fn unbounded_interconnection_samples(body: &str) -> Vec<String> {
    let mut offending = Vec::new();
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        if !INTERCONNECTION_METRIC_PREFIXES
            .iter()
            .any(|prefix| line.starts_with(prefix))
        {
            continue;
        }
        for label in sample_label_names(line) {
            if !BOUNDED_INTERCONNECTION_LABELS.contains(&label.as_str()) {
                offending.push(line.to_string());
                break;
            }
        }
    }
    offending
}

fn sample_label_names(line: &str) -> Vec<String> {
    let Some(open) = line.find('{') else {
        return Vec::new();
    };
    let Some(close) = line.rfind('}') else {
        return Vec::new();
    };
    let Some(labels) = line.get(open + 1..close) else {
        return Vec::new();
    };
    labels
        .split(',')
        .filter_map(|pair| pair.split('=').next())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

fn line_starts_with_metric(line: &str, metric_name: &str) -> bool {
    let Some(remainder) = line.strip_prefix(metric_name) else {
        return false;
    };
    if remainder.starts_with('{') {
        return true;
    }
    if let Some(next) = remainder.chars().next()
        && next.is_whitespace()
    {
        return true;
    }
    remainder.is_empty()
}

fn parse_prometheus_sample_value(line: &str) -> Option<f64> {
    line.split_whitespace()
        .last()
        .and_then(|value| value.parse::<f64>().ok())
}

fn ensure_dev_tls_assets() -> io::Result<()> {
    DEV_TLS_READY
        .get_or_init(|| {
            let status = std::process::Command::new("bash")
                .arg("scripts/generate_dev_tls.sh")
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .status();
            let status = match status {
                Ok(status) => status,
                Err(error) => return Err(io::Error::other(error)),
            };
            if status.success() {
                Ok(())
            } else {
                Err(io::Error::other(format!(
                    "failed to generate dev tls assets with exit status {status}"
                )))
            }
        })
        .as_ref()
        .map(|_| ())
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

pub(crate) fn dev_tls_ca_pem() -> io::Result<Vec<u8>> {
    ensure_dev_tls_assets()?;
    nervix_interconnect::install_rustls_crypto_provider();
    std::fs::read(dev_tls_ca_path()?).map_err(io::Error::other)
}

fn dev_tls_ca_path() -> io::Result<PathBuf> {
    ensure_dev_tls_assets()?;
    Ok(dev_tls_ca_file())
}

/// Where the dev TLS certificate authority is written. A node serving HTTPS has already generated
/// it, because starting such a node runs `ensure_dev_tls_assets` first.
fn dev_tls_ca_file() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tls/dev")
        .join("ca.pem")
}

fn kafka_client_config(dependencies: &DependencyEndpoints) -> io::Result<ClientConfig> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", dependencies.get(KAFKA_ADDR)?)
        .set("broker.address.family", "v4");
    Ok(config)
}

fn dependency_host_port(
    dependencies: &DependencyEndpoints,
    key: &str,
    default_port: u16,
) -> io::Result<(String, u16)> {
    let endpoint = url::Url::parse(dependencies.get(key)?).map_err(io::Error::other)?;
    let host = endpoint
        .host_str()
        .ok_or_else(|| io::Error::other(format!("dependency endpoint '{endpoint}' has no host")))?;
    Ok((host.to_string(), endpoint.port().unwrap_or(default_port)))
}

pub(crate) fn client_connect_options(server: &str) -> io::Result<ConnectOptions> {
    if server.starts_with("https://") {
        Ok(ConnectOptions {
            tls_requirement: Some(TlsRequirement::Required),
            ca_certificate_pem: Some(dev_tls_ca_pem()?),
            username: Some(TEST_AUTH_USERNAME.to_string()),
            password: Some(TEST_AUTH_PASSWORD.to_string()),
            ..ConnectOptions::default()
        })
    } else {
        Ok(ConnectOptions::default().with_basic_auth(TEST_AUTH_USERNAME, TEST_AUTH_PASSWORD))
    }
}

pub(crate) struct InterconnectTestCa {
    certificate: rcgen::Certificate,
    key: KeyPair,
    pub(crate) path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TestCertificateValidity {
    Current,
    Expired,
}

impl std::fmt::Debug for InterconnectTestCa {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InterconnectTestCa")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl InterconnectTestCa {
    pub(crate) fn new(root: &TempDir) -> io::Result<Self> {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().map_err(io::Error::other)?;
        let certificate = params.self_signed(&key).map_err(io::Error::other)?;
        let path = root.path().join("interconnect-ca.pem");
        std::fs::write(&path, certificate.pem())?;
        Ok(Self {
            certificate,
            key,
            path,
        })
    }

    fn issue_node(
        &self,
        node_id: &str,
        directory: &std::path::Path,
    ) -> io::Result<(PathBuf, PathBuf)> {
        self.issue_node_with_identity(
            "cucumber",
            node_id,
            TestCertificateValidity::Current,
            directory,
        )
    }

    pub(crate) fn issue_node_with_identity(
        &self,
        cluster_id: &str,
        node_id: &str,
        validity: TestCertificateValidity,
        directory: &std::path::Path,
    ) -> io::Result<(PathBuf, PathBuf)> {
        let subject_names = vec![
            "localhost".to_string(),
            HOST.to_string(),
            Ipv6Addr::LOCALHOST.to_string(),
            node_id.to_string(),
            qualified_name(node_id),
        ];
        let mut params = CertificateParams::new(subject_names).map_err(io::Error::other)?;
        params.subject_alt_names.push(SanType::URI(
            format!("nervix://cluster/{cluster_id}/node/{node_id}")
                .try_into()
                .map_err(io::Error::other)?,
        ));
        if validity == TestCertificateValidity::Expired {
            params.not_before = date_time_ymd(2000, 1, 1);
            params.not_after = date_time_ymd(2001, 1, 1);
        }
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().map_err(io::Error::other)?;
        let certificate = params
            .signed_by(&key, &self.certificate, &self.key)
            .map_err(io::Error::other)?;
        let certificate_path = directory.join("interconnect.pem");
        let key_path = directory.join("interconnect-key.pem");
        std::fs::write(&certificate_path, certificate.pem())?;
        std::fs::write(&key_path, key.serialize_pem())?;
        Ok((certificate_path, key_path))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum InterconnectCredentialFault {
    UntrustedClient,
    WrongClusterIdentity,
    WrongNodeIdentity,
    MismatchedEndpoint,
    ExpiredCertificate,
}

#[derive(Debug)]
pub(crate) struct Cluster {
    _root_dir: TempDir,
    interconnect_ca: InterconnectTestCa,
    peer_addressing: PeerAddressing,
    /// The DNS a named cluster resolves peers through; a literally addressed cluster has none.
    dns: Option<ClusterDns>,
    nodes: BTreeMap<String, NodeHandle>,
    fault_injection: FaultInjection,
    dependencies: DependencyEndpoints,
    /// Publishes this cluster to the suite watchdog for as long as the scenario holds it, so a
    /// suite timeout can name its nodes and ask every one of them to stop.
    live: LiveClusterRegistration,
}

#[derive(Debug, Clone)]
pub(crate) struct TestClusterConfig {
    pub replica_count: usize,
    #[cfg(feature = "testing")]
    pub scheduler_mode: Option<SchedulerMode>,
    pub state_snapshot_interval: Duration,
    pub transaction_idle_timeout: Duration,
    pub transaction_tombstone_retention: Duration,
    pub command_retry_validity: Duration,
    pub command_execution_capacity: usize,
    pub transaction_max_statements: usize,
    pub transaction_max_source_bytes: u64,
    pub transaction_max_open: usize,
    pub grpc_mode: InternalTransportMode,
    pub graceful_shutdown_drain: bool,
    pub drain_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub memory_pressure: Option<MemoryPressureConfig>,
    pub raft_election_timeout_min: Duration,
    pub raft_election_timeout_max: Duration,
    pub raft_retention: RaftRetentionPolicy,
    pub temp_dir: Option<PathBuf>,
    pub dependencies: DependencyEndpoints,
    pub peer_addressing: PeerAddressing,
}

impl Default for TestClusterConfig {
    fn default() -> Self {
        Self {
            replica_count: TEST_REPLICA_COUNT,
            #[cfg(feature = "testing")]
            scheduler_mode: None,
            state_snapshot_interval: TEST_STATE_SNAPSHOT_INTERVAL,
            transaction_idle_timeout: Duration::from_secs(15 * 60),
            transaction_tombstone_retention: Duration::from_secs(15 * 60),
            command_retry_validity: Duration::from_secs(15 * 60),
            command_execution_capacity: 65_536,
            transaction_max_statements: 256,
            transaction_max_source_bytes: 1024 * 1024,
            transaction_max_open: 1024,
            grpc_mode: InternalTransportMode::Http,
            graceful_shutdown_drain: false,
            drain_timeout: DEFAULT_TEST_DRAIN_TIMEOUT,
            shutdown_timeout: DEFAULT_TEST_SHUTDOWN_TIMEOUT,
            memory_pressure: None,
            raft_election_timeout_min: TEST_RAFT_ELECTION_TIMEOUT_MIN,
            raft_election_timeout_max: TEST_RAFT_ELECTION_TIMEOUT_MAX,
            raft_retention: RaftRetentionPolicy::default(),
            temp_dir: None,
            dependencies: DependencyEndpoints::default(),
            peer_addressing: PeerAddressing::default(),
        }
    }
}

impl Cluster {
    pub(crate) async fn start_with_config(
        node_count: usize,
        fault_injection: FaultInjection,
        config: TestClusterConfig,
        scenario: ScenarioIdentity,
    ) -> io::Result<Self> {
        assert!(node_count >= 1, "cluster must contain at least one node");
        #[cfg(feature = "testing")]
        let config = {
            let mut config = config;
            let scheduler_mode = *config.scheduler_mode.get_or_insert(if node_count == 3 {
                SchedulerMode::Random
            } else {
                SchedulerMode::Sticky
            });
            fault_injection.set_scheduler_mode(scheduler_mode);
            config
        };
        truncate_test_log_once()?;
        init_tracing_to_file(std::path::Path::new(TEST_LOG_FILE))?;
        let root_dir = tempdir()?;
        let interconnect_ca = InterconnectTestCa::new(&root_dir)?;
        let live = LiveClusterRegistration::start(scenario);
        /// One node of the cluster being built, before its spec exists.
        struct PlannedNode {
            node_id: String,
            index: u8,
            address: InterconnectAddress,
        }
        let peer_addressing = config.peer_addressing;
        let mut planned = Vec::with_capacity(node_count);
        for position in 1..=node_count {
            let node_id = format!("node-{position}");
            let index = NodeSpec::index(&node_id)?;
            let address = peer_addressing.address(&node_id, index);
            planned.push(PlannedNode {
                node_id,
                index,
                address,
            });
        }
        let mut published = Vec::with_capacity(planned.len());
        for node in &planned {
            published.push(PublishedNode {
                node_id: &node.node_id,
                index: node.index,
                listen_ip: node.address.listen_ip,
            });
        }
        let dns = ClusterDns::start(peer_addressing, root_dir.path(), &published).await?;
        let mut nodes = BTreeMap::new();

        for PlannedNode {
            node_id,
            index,
            address,
        } in planned
        {
            let mut spec = NodeSpec::new(&root_dir, &interconnect_ca, &node_id, index == 1)?;
            spec.set_interconnect_address(address);
            spec.dns = dns.as_ref().map(ClusterDns::configuration);
            fault_injection
                .set_syslog_ingestor_bind_ip(node_name(&node_id), spec.syslog_ingestor_host);
            nodes.insert(
                node_id.clone(),
                NodeHandle::new(spec, fault_injection.clone(), config.clone(), live.handle()),
            );
        }

        let mut cluster = Self {
            _root_dir: root_dir,
            interconnect_ca,
            peer_addressing,
            dns,
            fault_injection,
            nodes,
            dependencies: config.dependencies,
            live,
        };

        if let Err(error) = cluster.start_nodes_and_wait(node_count).await {
            let cleanup_error = cluster.shutdown().await.err();
            return Err(if let Some(cleanup_error) = cleanup_error {
                io::Error::other(format!("{error}; cleanup failed: {cleanup_error}"))
            } else {
                error
            });
        }

        Ok(cluster)
    }

    async fn start_nodes_and_wait(&mut self, node_count: usize) -> io::Result<()> {
        let nodes_to_start = u32::try_from(node_count)
            .assured("a test cluster is built from a handful of nodes, not billions");
        let construction = PhaseDeadline::after(cluster_startup_budget(nodes_to_start));
        self.start_node_within("node-1", construction).await?;
        let bootstrap_cluster_addr = self
            .nodes
            .get("node-1")
            .expect("bootstrap node exists")
            .spec
            .interconnect_endpoint();
        for (node_id, node) in &mut self.nodes {
            if node_id != "node-1" {
                node.spec.bootstrap_host = Some(bootstrap_cluster_addr.clone());
            }
        }
        for index in 2..=node_count {
            self.start_node_within(&format!("node-{index}"), construction)
                .await?;
        }
        self.wait_for_any_leader("node-1").await?;
        if node_count > 1 {
            let expected_nodes = (1..=node_count)
                .map(|index| format!("node-{index}"))
                .collect::<Vec<_>>();
            for node_id in &expected_nodes {
                self.wait_for_any_leader(node_id).await?;
            }
            let voter_refs = expected_nodes
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            self.wait_for_voters("node-1", &voter_refs).await?;
            self.wait_for_consistent_leader_on_all_nodes().await?;
            self.wait_for_full_interconnect(&expected_nodes).await?;
        }
        Ok(())
    }

    async fn wait_for_full_interconnect(&self, node_ids: &[String]) -> io::Result<()> {
        for node_id in node_ids {
            for peer_node_id in node_ids {
                if node_id != peer_node_id {
                    self.wait_for_interconnect_status(node_id, peer_node_id, "connected")
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Start a node without waiting for it to catch up with the leader.
    ///
    /// A scenario whose subject is a node that cannot apply what the leader committed waits for
    /// its own condition instead, and the retry [`Cluster::start_node`] performs would restart a
    /// node that had already consumed the failure the scenario armed. Its budget is therefore one
    /// attempt's readiness rather than the whole per-node startup budget, which exists for the
    /// retries this path must not perform.
    pub(crate) async fn start_node_without_catching_up(&mut self, node_id: &str) -> io::Result<()> {
        let handle = self
            .nodes
            .get_mut(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle.launch()?;
        handle
            .readiness(1, PhaseDeadline::after(ATTEMPT_READINESS_BUDGET))
            .await
            .map_err(io::Error::other)
    }

    pub(crate) async fn start_node(&mut self, node_id: &str) -> io::Result<()> {
        self.start_node_within(node_id, PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await
    }

    /// Start one node of a cluster the harness is building, so its startup budget cannot outlive
    /// the whole construction.
    async fn start_node_within(
        &mut self,
        node_id: &str,
        construction: PhaseDeadline,
    ) -> io::Result<()> {
        self.start_node_without_waiting_for_raft_catch_up_within(node_id, construction)
            .await?;
        let leader_lookup = PhaseDeadline::after(STATUS_WAIT_BUDGET);
        let mut leader_applied = None;
        for probe_node_id in self
            .nodes
            .keys()
            .filter(|existing_id| existing_id.as_str() != node_id)
        {
            tokio::task::consume_budget().await;
            if leader_lookup.has_passed() {
                break;
            }
            let Ok(probe_status) = self.cluster_status(probe_node_id, leader_lookup).await else {
                continue;
            };
            let Some(leader_id) = probe_status
                .current_leader
                .filter(|leader_id| leader_id != node_id)
            else {
                continue;
            };
            let Ok(leader_status) = self.cluster_status(&leader_id, leader_lookup).await else {
                continue;
            };
            leader_applied = leader_status.last_applied.filter(|value| *value > 0);
            if leader_applied.is_some() {
                break;
            }
        }
        if let Some(leader_applied) = leader_applied {
            self.wait_for_last_applied_at_least(node_id, leader_applied)
                .await?;
        }
        Ok(())
    }

    /// Start a stopped node and wait only until its server is ready to answer requests.
    ///
    /// The node may still be catching up its Raft log when this returns.
    pub(crate) async fn start_node_without_waiting_for_raft_catch_up(
        &mut self,
        node_id: &str,
    ) -> io::Result<()> {
        self.start_node_without_waiting_for_raft_catch_up_within(
            node_id,
            PhaseDeadline::after(NODE_STARTUP_BUDGET),
        )
        .await
    }

    async fn start_node_without_waiting_for_raft_catch_up_within(
        &mut self,
        node_id: &str,
        construction: PhaseDeadline,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get_mut(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let node = node_name(&handle.spec.node_id);
        let budget = construction.nested(NODE_STARTUP_BUDGET);
        NodeStartup::start(&node, handle, budget)
            .await
            .map_err(io::Error::other)
    }

    pub(crate) async fn add_node(&mut self, node_id: &str) -> io::Result<()> {
        if self.nodes.contains_key(node_id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("node '{node_id}' already exists"),
            ));
        }
        let bootstrap_host = self
            .nodes
            .values()
            .find(|node| node.task.is_running())
            .map(|node| node.spec.interconnect_endpoint())
            .ok_or_else(|| io::Error::other("a running bootstrap node is required"))?;
        let config = self
            .nodes
            .values()
            .next()
            .expect("an existing cluster has at least one node")
            .config
            .clone();
        let mut spec = NodeSpec::new(&self._root_dir, &self.interconnect_ca, node_id, false)?;
        let index = NodeSpec::index(node_id)?;
        let address = self.peer_addressing.address(node_id, index);
        if let Some(dns) = &self.dns {
            dns.publish(node_id, index, address.listen_ip);
        }
        spec.set_interconnect_address(address);
        spec.dns = self.dns.as_ref().map(ClusterDns::configuration);
        spec.bootstrap_host = Some(bootstrap_host);
        self.fault_injection
            .set_syslog_ingestor_bind_ip(node_name(node_id), spec.syslog_ingestor_host);
        self.nodes.insert(
            node_id.to_string(),
            NodeHandle::new(
                spec,
                self.fault_injection.clone(),
                config,
                self.live.handle(),
            ),
        );
        self.start_node(node_id).await?;

        let node_ids = self.node_ids();
        self.wait_for_any_leader(node_id).await?;
        let voter_refs = node_ids.iter().map(String::as_str).collect::<Vec<_>>();
        self.wait_for_voters(node_id, &voter_refs).await?;
        self.wait_for_consistent_leader_on_all_nodes().await?;
        self.wait_for_full_interconnect(&node_ids).await
    }

    pub(crate) fn syslog_ingestor_addr(
        &self,
        node_id: &str,
        configured: &str,
    ) -> io::Result<String> {
        let configured = configured.parse::<SocketAddr>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid server ingestor address '{configured}': {error}"),
            )
        })?;
        let host = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"))
            .spec
            .syslog_ingestor_host;
        Ok(SocketAddr::new(host, configured.port()).to_string())
    }

    /// The error the node's last run of the application returned, if it returned one.
    pub(crate) fn node_run_failure(&self, node_id: &str) -> Option<String> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle
            .task
            .application_error()
            .map(|error| format!("{error:?}"))
    }

    pub(crate) async fn stop_node(&mut self, node_id: &str) -> io::Result<()> {
        let handle = self
            .nodes
            .get_mut(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle.stop().await
    }

    /// Move a stopped node's interconnect to another loopback address behind the same advertised
    /// name, and answer that name with the new address from the next question on.
    pub(crate) fn move_behind_its_name(&mut self, node_id: &str) -> io::Result<()> {
        let Some(dns) = &self.dns else {
            return Err(io::Error::other(
                "only a cluster addressed by names can move a node behind its name",
            ));
        };
        let handle = self.nodes.get_mut(node_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("unknown node '{node_id}'"))
        })?;
        if handle.task.is_running() {
            return Err(io::Error::other(format!(
                "node '{node_id}' must be stopped before it moves"
            )));
        }
        let index = NodeSpec::index(node_id)?;
        let address = self.peer_addressing.moved_address(node_id, index);
        dns.publish(node_id, index, address.listen_ip);
        handle.spec.set_interconnect_address(address);
        Ok(())
    }

    /// Answer `node_id`'s name with `answer` in place of its address.
    pub(crate) fn answer_node_name(&self, node_id: &str, answer: FixtureAnswer) -> io::Result<()> {
        let Some(dns) = &self.dns else {
            return Err(io::Error::other(
                "only a cluster addressed by names has a DNS fixture to answer with",
            ));
        };
        if !self.nodes.contains_key(node_id) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("unknown node '{node_id}'"),
            ));
        }
        dns.answer(node_id, answer);
        Ok(())
    }

    /// Questions the cluster's DNS fixture received for the names of its nodes.
    pub(crate) fn dns_questions_for_node_names(&self) -> io::Result<u64> {
        let Some(dns) = &self.dns else {
            return Err(io::Error::other(
                "only a cluster addressed by names has a DNS fixture",
            ));
        };
        let node_ids = self.node_ids();
        Ok(dns.questions_for(&node_ids))
    }

    pub(crate) async fn restart_node_with_new_interconnect_address(
        &mut self,
        node_id: &str,
    ) -> io::Result<()> {
        self.stop_node(node_id).await?;
        let bootstrap_host = self
            .nodes
            .iter()
            .find(|(candidate_id, candidate)| {
                candidate_id.as_str() != node_id && candidate.task.is_running()
            })
            .map(|(_, candidate)| candidate.spec.interconnect_endpoint());
        let handle = self.nodes.get_mut(node_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("unknown node '{node_id}'"))
        })?;
        handle.spec.reallocate_interconnect_ports()?;
        if let Some(bootstrap_host) = bootstrap_host {
            handle.spec.bootstrap_host = Some(bootstrap_host);
        }
        self.start_node(node_id).await?;

        let node_ids = self.node_ids();
        self.wait_for_consistent_leader_on_all_nodes().await?;
        self.wait_for_full_interconnect(&node_ids).await
    }

    pub(crate) async fn rotate_interconnect_certificates(&mut self) -> io::Result<()> {
        let interconnect_ca = InterconnectTestCa::new(&self._root_dir)?;
        for (node_id, node) in &mut self.nodes {
            tokio::task::consume_budget().await;
            let (certificate, key) = interconnect_ca.issue_node(node_id, &node.spec.base_dir)?;
            node.spec.interconnect_tls_ca = interconnect_ca.path.clone();
            node.spec.interconnect_tls_cert = certificate;
            node.spec.interconnect_tls_key = key;
        }
        self.interconnect_ca = interconnect_ca;
        sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    pub(crate) async fn open_silent_interconnect_handshake(
        &self,
        node_id: &str,
    ) -> io::Result<TcpStream> {
        let handle = self.nodes.get(node_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("unknown node '{node_id}'"))
        })?;
        TcpStream::connect(handle.spec.interconnect_listen_addr()).await
    }

    pub(crate) async fn attempt_interconnect_with_invalid_credentials(
        &self,
        target_node_id: &str,
        fault: InterconnectCredentialFault,
    ) -> io::Result<()> {
        let target = self.nodes.get(target_node_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("unknown node '{target_node_id}'"),
            )
        })?;
        let target_addr = target.spec.interconnect_listen_addr();
        let probe_directory = tempfile::tempdir_in(self._root_dir.path())?;
        let untrusted_authority = if let InterconnectCredentialFault::UntrustedClient = fault {
            Some(InterconnectTestCa::new(&probe_directory)?)
        } else {
            None
        };
        let certificate_authority = untrusted_authority
            .as_ref()
            .unwrap_or(&self.interconnect_ca);
        let certificate_cluster_id =
            if let InterconnectCredentialFault::WrongClusterIdentity = fault {
                "another-cluster"
            } else {
                "cucumber"
            };
        let validity = if let InterconnectCredentialFault::ExpiredCertificate = fault {
            TestCertificateValidity::Expired
        } else {
            TestCertificateValidity::Current
        };
        let probe_node_id = ClusterNodeName::parse("probe-node").map_err(io::Error::other)?;
        let (certificate_path, key_path) = certificate_authority.issue_node_with_identity(
            certificate_cluster_id,
            probe_node_id.as_ref(),
            validity,
            probe_directory.path(),
        )?;
        let tls = TlsConfigBundle::from_pem_files(
            &self.interconnect_ca.path,
            certificate_path,
            key_path,
            TransportClock::system(),
        )
        .map_err(|error| io::Error::other(error.to_string()))?;
        let options = TransportOptions {
            connection_setup_timeout: Duration::from_millis(750),
            request_timeout: Duration::from_millis(750),
            reconnect_backoff: Duration::from_millis(25),
            max_reconnect_backoff: Duration::from_millis(50),
            shutdown_drain_timeout: Duration::from_millis(250),
            ..TransportOptions::default()
        };
        let dns = DnsResolver::load(DnsConfiguration::system())
            .await
            .map_err(|report| io::Error::other(format!("{report:?}")))?;
        let (transport, _incoming) = Transport::bind(
            "127.0.0.1:0".parse().expect("probe address is valid"),
            TransportIdentity {
                cluster_id: certificate_cluster_id.to_string(),
                node_id: probe_node_id,
                advertised_host: "localhost".to_string(),
            },
            tls,
            options,
            Executor::default(),
            PeerResolver::new(dns),
        )
        .await
        .map_err(io::Error::other)?;
        let server_name = if let InterconnectCredentialFault::MismatchedEndpoint = fault {
            "mismatched.invalid"
        } else {
            "localhost"
        };
        let peer_target = PeerTarget::new(target_addr, server_name);
        let attempt = if let InterconnectCredentialFault::WrongNodeIdentity = fault {
            let addressed_node = ClusterNodeName::parse("node-254").map_err(io::Error::other)?;
            transport
                .register_outbound_target(addressed_node.clone(), peer_target.endpoint())
                .map_err(io::Error::other)?;
            transport
                .send(
                    &addressed_node,
                    Envelope::Control(ControlEnvelope::RuntimeErrorEvent(RuntimeErrorEvent {
                        message: "interconnect identity probe".to_string(),
                    })),
                )
                .await
                .map_err(io::Error::other)
        } else {
            match transport.bootstrap_target(peer_target).await {
                Ok(peer_node_id) => transport
                    .send(
                        &peer_node_id,
                        Envelope::Control(ControlEnvelope::RuntimeErrorEvent(RuntimeErrorEvent {
                            message: "interconnect credential probe".to_string(),
                        })),
                    )
                    .await
                    .map_err(io::Error::other),
                Err(error) => Err(io::Error::other(error)),
            }
        };
        transport.shutdown().await;
        attempt
    }

    /// Signals a node to exit without waiting for the process. Scenarios that observe a
    /// transient reaction to owner loss must start observing while the node is still on its way
    /// down, because the leader reacts as soon as it sees the node go.
    pub(crate) fn begin_stopping_node(&mut self, node_id: &str) {
        let handle = self
            .nodes
            .get_mut(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle.request_stop();
    }

    pub(crate) async fn shutdown(&mut self) -> io::Result<()> {
        let node_ids = self.nodes.keys().cloned().collect::<Vec<_>>();
        for node_id in &node_ids {
            if let Some(handle) = self.nodes.get_mut(node_id) {
                handle.request_stop();
            }
        }
        let mut first_error = None;
        for node_id in node_ids {
            let handle = self
                .nodes
                .get_mut(&node_id)
                .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
            if let Err(error) = handle.wait_stopped().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        for node in self.nodes.values_mut() {
            node.spec.release_ports();
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    /// Stops every node of this cluster within the one scenario-cleanup budget and gives back the
    /// harness state they hold.
    ///
    /// Cleanup follows the scenario's assertions, so it uses the harness budget rather than the
    /// product shutdown deadlines a scenario configured: a scenario that asserts on shutdown,
    /// drain or deadline expiry stops its nodes itself, before it reaches here.
    pub(crate) async fn shutdown_for_teardown(&mut self) -> ClusterTeardown {
        ClusterTeardown::stop_all(self.nodes.values_mut(), CLUSTER_TEARDOWN_BUDGET).await
    }

    pub(crate) async fn restart(&mut self) -> io::Result<()> {
        let node_ids = self.nodes.keys().cloned().collect::<Vec<_>>();
        let mut first_error = None;

        for node_id in &node_ids {
            let handle = self
                .nodes
                .get_mut(node_id)
                .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
            handle.request_stop();
        }

        for node_id in &node_ids {
            let handle = self
                .nodes
                .get_mut(node_id)
                .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
            if let Err(error) = handle.wait_stopped().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        let nodes_to_start = u32::try_from(node_ids.len())
            .assured("a test cluster is built from a handful of nodes, not billions");
        let construction = PhaseDeadline::after(cluster_startup_budget(nodes_to_start));
        self.start_node_within("node-1", construction).await?;
        for node_id in node_ids
            .iter()
            .filter(|node_id| node_id.as_str() != "node-1")
        {
            self.start_node_within(node_id, construction).await?;
        }

        self.wait_for_any_leader("node-1").await?;
        if node_ids.len() > 1 {
            for node_id in &node_ids {
                self.wait_for_any_leader(node_id).await?;
            }
            let voter_refs = node_ids.iter().map(String::as_str).collect::<Vec<_>>();
            self.wait_for_voters("node-1", &voter_refs).await?;
            self.wait_for_consistent_leader_on_all_nodes().await?;
            self.wait_for_full_interconnect(&node_ids).await?;
        }

        Ok(())
    }

    pub(crate) async fn wait_for_leader(
        &self,
        node_id: &str,
        expected: Option<&str>,
    ) -> io::Result<()> {
        self.wait_until(node_id, |status| {
            status.current_leader.as_deref() == expected
        })
        .await
    }

    pub(crate) async fn wait_for_leader_not(
        &self,
        node_id: &str,
        not_expected: &str,
    ) -> io::Result<()> {
        self.wait_until(node_id, |status| {
            status
                .current_leader
                .as_deref()
                .is_some_and(|leader| leader != not_expected)
        })
        .await
    }

    pub(crate) async fn wait_for_any_leader(&self, node_id: &str) -> io::Result<()> {
        self.wait_until(node_id, |status| status.current_leader.is_some())
            .await
    }

    pub(crate) async fn wait_for_raft_state(
        &self,
        node_id: &str,
        expected: &str,
    ) -> io::Result<()> {
        self.wait_until(node_id, |status| {
            status.raft_state.as_deref() == Some(expected)
        })
        .await
    }

    pub(crate) async fn wait_for_voters(&self, node_id: &str, expected: &[&str]) -> io::Result<()> {
        let expected = expected
            .iter()
            .map(|value| (*value).to_string())
            .collect::<BTreeSet<_>>();
        self.wait_until(node_id, |status| {
            status
                .membership
                .iter()
                .filter(|(_, role)| role == &&"voter".to_string())
                .map(|(node, _)| node.clone())
                .collect::<BTreeSet<_>>()
                == expected
        })
        .await
    }

    pub(crate) async fn wait_for_kafka_consumer_group_members(
        &self,
        group: &str,
        expected: usize,
    ) -> io::Result<()> {
        let deadline = Instant::now() + STATUS_WAIT_BUDGET;
        loop {
            tokio::task::consume_budget().await;
            match kafka_consumer_group_member_count(&self.dependencies, group) {
                Ok(actual) if actual == expected => return Ok(()),
                Ok(_) => {}
                Err(error) if Instant::now() >= deadline => return Err(error),
                Err(_) => {}
            }
            if Instant::now() >= deadline {
                let actual = kafka_consumer_group_member_count(&self.dependencies, group)?;
                return Err(io::Error::other(format!(
                    "timed out waiting for kafka consumer group '{group}' to have {expected} \
                     members, got {actual}"
                )));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    pub(crate) async fn assert_kafka_consumer_group_next_offset(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
        threshold: i64,
        should_reach: bool,
        duration: Duration,
    ) -> io::Result<()> {
        let deadline = Instant::now() + duration;
        loop {
            tokio::task::consume_budget().await;
            let dependencies = self.dependencies.clone();
            let query_group = group.to_string();
            let query_topic = topic.to_string();
            let actual = tokio::task::spawn_blocking(move || {
                kafka_consumer_group_next_offset(
                    &dependencies,
                    &query_group,
                    &query_topic,
                    partition,
                )
            })
            .await
            .map_err(io::Error::other)??;
            let reached = actual.is_some_and(|offset| offset >= threshold);
            if should_reach && reached {
                return Ok(());
            }
            if !should_reach && reached {
                return Err(io::Error::other(format!(
                    "Kafka consumer group '{group}' unexpectedly committed next offset {actual:?} \
                     for topic '{topic}' partition {partition}; expected it to remain below \
                     {threshold}"
                )));
            }
            if Instant::now() >= deadline {
                if should_reach {
                    return Err(io::Error::other(format!(
                        "timed out waiting for Kafka consumer group '{group}' to commit next \
                         offset at least {threshold} for topic '{topic}' partition {partition}; \
                         observed {actual:?}"
                    )));
                }
                return Ok(());
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    pub(crate) async fn wait_for_rabbitmq_queue_consumers(
        &self,
        queue: &str,
        expected: usize,
    ) -> io::Result<()> {
        let deadline = Instant::now() + STATUS_WAIT_BUDGET;
        loop {
            tokio::task::consume_budget().await;
            match rabbitmq_queue_consumer_count(&self.dependencies, queue).await {
                Ok(actual) if actual == expected => return Ok(()),
                Ok(_) => {}
                Err(error) if Instant::now() >= deadline => return Err(error),
                Err(_) => {}
            }
            if Instant::now() >= deadline {
                let actual = rabbitmq_queue_consumer_count(&self.dependencies, queue).await?;
                return Err(io::Error::other(format!(
                    "timed out waiting for rabbitmq queue '{queue}' to have {expected} consumers, \
                     got {actual}"
                )));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    pub(crate) async fn wait_for_redis_channel_subscribers(
        &self,
        channel: &str,
        expected: usize,
    ) -> io::Result<()> {
        wait_for_redis_channel_subscribers(&self.dependencies, channel, expected).await
    }

    pub(crate) async fn open_session(
        &self,
        node_id: &str,
        domain: &str,
    ) -> io::Result<TestSession> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        open_raw_session(&handle.spec.grpc_uri(handle.config.grpc_mode), domain).await
    }

    pub(crate) fn grpc_uri(&self, node_id: &str) -> io::Result<String> {
        let handle = self
            .nodes
            .get(node_id)
            .ok_or_else(|| io::Error::other(format!("unknown node '{node_id}'")))?;
        Ok(handle.spec.grpc_uri(handle.config.grpc_mode))
    }

    /// Streams an upload whose frames a scenario shaped to `node_id` and returns its reply.
    pub(crate) async fn send_shaped_resource_upload(
        &self,
        node_id: &str,
        upload: TestUpload<'_>,
    ) -> io::Result<UploadReply> {
        let server = self.grpc_uri(node_id)?;
        send_upload(&server, upload).await
    }

    pub(crate) fn web_console_url(&self, node_id: &str) -> io::Result<String> {
        let handle = self
            .nodes
            .get(node_id)
            .ok_or_else(|| io::Error::other(format!("unknown node '{node_id}'")))?;
        Ok(handle.spec.web_console_url())
    }

    pub(crate) fn http_uri(&self, node_id: &str, path: &str) -> io::Result<String> {
        let handle = self
            .nodes
            .get(node_id)
            .ok_or_else(|| io::Error::other(format!("unknown node '{node_id}'")))?;
        Ok(handle.spec.http_uri(path))
    }

    pub(crate) fn web_console_url_with_password(
        &self,
        node_id: &str,
        password: &str,
    ) -> io::Result<String> {
        let handle = self
            .nodes
            .get(node_id)
            .ok_or_else(|| io::Error::other(format!("unknown node '{node_id}'")))?;
        Ok(handle.spec.web_console_url_with_password(password))
    }

    pub(crate) fn node_base_dir(&self, node_id: &str) -> io::Result<PathBuf> {
        let handle = self
            .nodes
            .get(node_id)
            .ok_or_else(|| io::Error::other(format!("unknown node '{node_id}'")))?;
        Ok(handle.spec.base_dir.clone())
    }

    /// Where one node exposes its metrics, as an owned address a sampling task can keep after the
    /// step that started it has returned.
    pub(crate) fn observability_metrics_url(&self, node_id: &str) -> io::Result<String> {
        let handle = self
            .nodes
            .get(node_id)
            .ok_or_else(|| io::Error::other(format!("unknown node '{node_id}'")))?;
        Ok(format!(
            "http://{}/metrics",
            handle.spec.observability_addr()
        ))
    }

    /// Read one series once, for a value a scenario compares others against rather than waits for.
    pub(crate) async fn read_observability_metric(
        &self,
        node_id: &str,
        metric_name: &str,
        label_fragments: &[String],
    ) -> io::Result<f64> {
        let url = self.observability_metrics_url(node_id)?;
        let client = reqwest::Client::new();
        let response = timeout(STATUS_WAIT_BUDGET, async {
            let response = client.get(&url).send().await?;
            response.text().await
        })
        .await
        .map_err(io::Error::other)?
        .map_err(io::Error::other)?;
        for line in response.lines() {
            if line.starts_with('#') || !line_starts_with_metric(line, metric_name) {
                continue;
            }
            if !label_fragments
                .iter()
                .all(|fragment| line.contains(fragment.as_str()))
            {
                continue;
            }
            if let Some(value) = parse_prometheus_sample_value(line) {
                return Ok(value);
            }
        }
        Err(io::Error::other(format!(
            "node '{node_id}' exposed no sample of '{metric_name}' with labels {label_fragments:?}"
        )))
    }

    pub(crate) fn node_ids(&self) -> Vec<String> {
        let mut node_ids = self.nodes.keys().cloned().collect::<Vec<_>>();
        node_ids.sort();
        node_ids
    }

    pub(crate) async fn run_command(
        &self,
        node_id: &str,
        domain: &str,
        query: &str,
    ) -> io::Result<String> {
        let grpc_uri = self.grpc_uri(node_id)?;
        run_command_via_client(&grpc_uri, domain, query).await
    }

    pub(crate) async fn publish_mqtt(&self, topic: &str, payload: &str) -> io::Result<()> {
        publish_mqtt(&self.dependencies, topic, payload).await
    }

    pub(crate) async fn publish_mqtt_qos1(&self, topic: &str, payload: &str) -> io::Result<()> {
        publish_mqtt_with_qos(&self.dependencies, topic, payload, QoS::AtLeastOnce, true).await
    }

    pub(crate) async fn publish_mqtt_burst(
        &self,
        topic: &str,
        payload: &str,
        count: usize,
    ) -> io::Result<()> {
        publish_mqtt_burst(&self.dependencies, topic, payload, count).await
    }

    pub(crate) async fn publish_mqtt_qos1_burst(
        &self,
        topic: &str,
        payload: &str,
        count: usize,
    ) -> io::Result<()> {
        publish_mqtt_burst_with_qos(&self.dependencies, topic, payload, count, QoS::AtLeastOnce)
            .await
    }

    pub(crate) async fn ensure_rabbitmq_queue(&self, queue: &str) -> io::Result<()> {
        ensure_rabbitmq_queue(&self.dependencies, queue).await
    }

    pub(crate) async fn publish_rabbitmq(&self, queue: &str, payload: &str) -> io::Result<()> {
        publish_rabbitmq(&self.dependencies, queue, payload).await
    }

    pub(crate) async fn publish_redis(&self, channel: &str, payload: &str) -> io::Result<()> {
        publish_redis(&self.dependencies, channel, payload).await
    }

    pub(crate) async fn publish_redis_burst(
        &self,
        channel: &str,
        payload: &str,
        count: usize,
    ) -> io::Result<()> {
        publish_redis_burst(&self.dependencies, channel, payload, count).await
    }

    pub(crate) async fn publish_pulsar(&self, topic: &str, payload: &str) -> io::Result<()> {
        publish_pulsar(&self.dependencies, topic, payload).await
    }

    pub(crate) async fn publish_pulsar_tls(&self, topic: &str, payload: &str) -> io::Result<()> {
        publish_pulsar_tls(&self.dependencies, topic, payload).await
    }

    pub(crate) async fn publish_kafka(&self, topic: &str, payload: &str) -> io::Result<()> {
        publish_kafka(&self.dependencies, topic, payload).await
    }

    pub(crate) async fn publish_kafka_with_headers(
        &self,
        topic: &str,
        payload: &str,
        headers: &[(&str, &str)],
    ) -> io::Result<()> {
        publish_kafka_with_headers(&self.dependencies, topic, payload, headers).await
    }

    pub(crate) async fn publish_kafka_partition_with_headers(
        &self,
        topic: &str,
        partition: i32,
        payload: &str,
        headers: &[(&str, &str)],
    ) -> io::Result<()> {
        publish_kafka_record(&self.dependencies, topic, Some(partition), payload, headers).await
    }

    pub(crate) async fn publish_kafka_payloads(
        &self,
        topic: &str,
        payloads: &[String],
    ) -> io::Result<()> {
        publish_kafka_payloads(&self.dependencies, topic, payloads).await
    }

    pub(crate) async fn publish_kafka_partition(
        &self,
        topic: &str,
        partition: i32,
        payload: &str,
    ) -> io::Result<()> {
        publish_kafka_partition(&self.dependencies, topic, partition, payload).await
    }

    pub(crate) async fn ensure_kafka_topic_partitions(
        &self,
        topic: &str,
        partitions: i32,
    ) -> io::Result<()> {
        ensure_kafka_topic_partitions(&self.dependencies, topic, partitions).await
    }

    pub(crate) async fn reset_kafka_topic_partitions(
        &self,
        topic: &str,
        partitions: i32,
    ) -> io::Result<()> {
        reset_kafka_topic_partitions(&self.dependencies, topic, partitions).await
    }

    pub(crate) async fn ensure_sqs_queue(&self, queue: &str) -> io::Result<()> {
        ensure_sqs_queue(&self.dependencies, queue).await
    }

    pub(crate) async fn ensure_sqs_queue_tls(&self, queue: &str) -> io::Result<()> {
        ensure_sqs_queue_tls(&self.dependencies, queue).await
    }

    pub(crate) async fn publish_sqs(&self, queue: &str, payload: &str) -> io::Result<()> {
        publish_sqs(&self.dependencies, queue, payload).await
    }

    pub(crate) async fn publish_sqs_tls(&self, queue: &str, payload: &str) -> io::Result<()> {
        publish_sqs_tls(&self.dependencies, queue, payload).await
    }

    pub(crate) async fn publish_nats(&self, subject: &str, payload: &str) -> io::Result<()> {
        publish_nats(&self.dependencies, subject, payload).await
    }

    pub(crate) async fn provision_nats_stream(
        &self,
        stream: &str,
        subject: &str,
    ) -> io::Result<()> {
        provision_nats_stream(&self.dependencies, stream, subject).await
    }

    pub(crate) async fn wait_for_nats_stream_payload(
        &self,
        stream: &str,
        subject: &str,
        expected: &str,
    ) -> io::Result<()> {
        wait_for_nats_stream_payload(&self.dependencies, stream, subject, expected).await
    }

    pub(crate) async fn publish_nats_payloads(
        &self,
        subject: &str,
        payloads: &[String],
    ) -> io::Result<()> {
        publish_nats_payloads(&self.dependencies, subject, payloads).await
    }

    pub(crate) async fn publish_nats_with_headers(
        &self,
        subject: &str,
        payload: &str,
        headers: &[(&str, &str)],
    ) -> io::Result<()> {
        publish_nats_with_headers(&self.dependencies, subject, payload, headers).await
    }

    pub(crate) async fn publish_nats_tls(&self, subject: &str, payload: &str) -> io::Result<()> {
        publish_nats_tls(&self.dependencies, subject, payload).await
    }

    pub(crate) async fn publish_zeromq(&self, addr: &str, payload: &str) -> io::Result<()> {
        publish_zeromq(addr, payload).await
    }

    pub(crate) async fn publish_websocket(
        &self,
        node_id: &str,
        host: &str,
        path: &str,
        payload: &str,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        publish_websocket(&handle.spec, host, path, payload).await
    }

    pub(crate) async fn exchange_websocket(
        &self,
        node_id: &str,
        host: &str,
        path: &str,
        actions: &[WebsocketExchangeAction],
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        exchange_websocket(&handle.spec, host, path, actions).await
    }

    pub(crate) async fn publish_secure_websocket(
        &self,
        node_id: &str,
        host: &str,
        path: &str,
        payload: &str,
        ca_cert_pem: &str,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        publish_secure_websocket(&handle.spec, host, path, payload, ca_cert_pem).await
    }

    pub(crate) async fn publish_http(
        &self,
        node_id: &str,
        host: &str,
        path: &str,
        payload: &str,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        publish_http(&handle.spec, host, path, payload).await
    }

    pub(crate) fn spawn_http_publish(
        &self,
        node_id: &str,
        host: String,
        path: String,
        payload: String,
    ) -> tokio::task::JoinHandle<io::Result<()>> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let uri = handle.spec.http_uri(&path);
        tokio::spawn(async move {
            publish_http_uri_with_headers(uri, &host, payload.as_bytes(), "application/json", &[])
                .await
        })
    }

    /// Posts `payload` to the HTTPS listener of every node in turn until `stop` fires, opening a
    /// new TLS connection for every post so each one performs its own handshake.
    pub(crate) fn spawn_https_publish_loop(
        &self,
        host: String,
        path: String,
        payload: String,
        trusted_ca_pems: &[String],
        stop: CancellationToken,
    ) -> io::Result<JoinHandle<HttpsPublishLoopOutcome>> {
        struct NodeTarget {
            node_id: String,
            uri: String,
            client: reqwest::Client,
        }

        let mut targets = Vec::new();
        for node_id in self.node_ids() {
            let handle = self
                .nodes
                .get(&node_id)
                .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
            let mut builder = reqwest::Client::builder()
                .resolve(&host, parse_addr(&handle.spec.https_addr())?)
                .pool_max_idle_per_host(0);
            for ca_pem in trusted_ca_pems {
                let certificate =
                    reqwest::Certificate::from_pem(ca_pem.as_bytes()).map_err(io::Error::other)?;
                builder = builder.add_root_certificate(certificate);
            }
            let client = builder.build().map_err(io::Error::other)?;
            targets.push(NodeTarget {
                uri: handle.spec.https_uri(&host, &path),
                node_id,
                client,
            });
        }

        Ok(tokio::spawn(async move {
            let mut outcome = HttpsPublishLoopOutcome::default();
            while !stop.is_cancelled() {
                tokio::task::consume_budget().await;
                for target in &targets {
                    let response = target
                        .client
                        .post(target.uri.as_str())
                        .body(payload.clone())
                        .send()
                        .await;
                    match response {
                        Ok(response) if response.status() == reqwest::StatusCode::ACCEPTED => {
                            outcome.accepted = outcome
                                .accepted
                                .checked_add(1)
                                .expect("a scenario posts fewer than usize::MAX payloads");
                        }
                        Ok(response) => outcome.failures.push(format!(
                            "node '{}' answered with status {}",
                            target.node_id,
                            response.status()
                        )),
                        Err(error) => outcome
                            .failures
                            .push(format!("node '{}' failed: {error}", target.node_id)),
                    }
                }
            }
            outcome
        }))
    }

    pub(crate) async fn publish_http_with_headers(
        &self,
        node_id: &str,
        host: &str,
        path: &str,
        payload: &str,
        headers: &[(&str, &str)],
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        publish_http_with_headers(&handle.spec, host, path, payload, headers).await
    }

    pub(crate) async fn publish_http_bytes(
        &self,
        node_id: &str,
        host: &str,
        path: &str,
        payload: &[u8],
        content_type: &str,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        publish_http_bytes(&handle.spec, host, path, payload, content_type).await
    }

    pub(crate) async fn publish_https(
        &self,
        node_id: &str,
        host: &str,
        path: &str,
        payload: &str,
        ca_cert_pem: &str,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        publish_https(&handle.spec, host, path, payload, ca_cert_pem).await
    }

    pub(crate) async fn connect_https(
        &self,
        node_id: &str,
        host: &str,
        ca_cert_pem: &str,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        connect_https(&handle.spec, host, ca_cert_pem).await
    }

    pub(crate) async fn observe_mqtt(&self, topic: &str) -> io::Result<BrokerObserver> {
        observe_mqtt(&self.dependencies, topic).await
    }

    pub(crate) async fn observe_rabbitmq(&self, queue: &str) -> io::Result<BrokerObserver> {
        observe_rabbitmq(&self.dependencies, queue).await
    }

    pub(crate) async fn observe_redis(&self, channel: &str) -> io::Result<BrokerObserver> {
        observe_redis(&self.dependencies, channel).await
    }

    pub(crate) async fn observe_kafka(&self, topic: &str) -> io::Result<BrokerObserver> {
        observe_kafka(&self.dependencies, topic).await
    }

    pub(crate) async fn observe_pulsar(&self, topic: &str) -> io::Result<BrokerObserver> {
        observe_pulsar(&self.dependencies, topic).await
    }

    pub(crate) async fn observe_pulsar_tls(&self, topic: &str) -> io::Result<BrokerObserver> {
        observe_pulsar_tls(&self.dependencies, topic).await
    }

    pub(crate) async fn observe_sqs(&self, queue: &str) -> io::Result<BrokerObserver> {
        observe_sqs(&self.dependencies, queue).await
    }

    pub(crate) async fn observe_nats(&self, subject: &str) -> io::Result<BrokerObserver> {
        observe_nats(&self.dependencies, subject).await
    }

    pub(crate) async fn observe_zeromq(&self, addr: &str) -> io::Result<BrokerObserver> {
        observe_zeromq(addr).await
    }

    pub(crate) fn arm_health_response_pause(
        &self,
        probing_node_id: &str,
        responding_node_id: &str,
    ) {
        self.fault_injection
            .arm_health_response_pause(node_name(probing_node_id), node_name(responding_node_id));
    }

    pub(crate) fn fail_health_responses_from(&self, responding_node_id: &str) {
        self.fault_injection
            .fail_health_responses_from(node_name(responding_node_id));
    }

    pub(crate) async fn wait_for_health_response_pause(
        &self,
        probing_node_id: &str,
        responding_node_id: &str,
    ) {
        self.fault_injection
            .wait_for_health_response_pause(
                &node_name(probing_node_id),
                &node_name(responding_node_id),
            )
            .await;
    }

    pub(crate) fn release_health_response_pause(
        &self,
        probing_node_id: &str,
        responding_node_id: &str,
    ) {
        self.fault_injection.release_health_response_pause(
            &node_name(probing_node_id),
            &node_name(responding_node_id),
        );
    }

    pub(crate) fn fail_emitter_on_all_nodes(&self, emitter: &str) {
        self.fault_injection.fail_emitter(emitter);
    }

    pub(crate) fn stall_emitter_on_all_nodes(&self, emitter: &str) {
        self.fault_injection.stall_emitter(emitter);
    }

    pub(crate) fn clear_emitter_fault_on_all_nodes(&self, emitter: &str) {
        self.fault_injection.clear_emitter_fault(emitter);
    }

    pub(crate) fn fail_sink_client_unavailable_on_all_nodes(&self, emitter: &str) {
        self.fault_injection.fail_sink_client_unavailable(emitter);
    }

    pub(crate) fn clear_sink_client_fault_on_all_nodes(&self, emitter: &str) {
        self.fault_injection.clear_sink_client_fault(emitter);
    }

    pub(crate) fn fail_next_schedule_publication_on_all_nodes(&self, domain: &str) {
        self.fault_injection.fail_next_schedule_publication(domain);
    }

    pub(crate) fn fail_ingestor_on_all_nodes(&self, ingestor: &str) {
        self.fault_injection.fail_ingestor(ingestor);
    }

    pub(crate) fn clear_ingestor_fault_on_all_nodes(&self, ingestor: &str) {
        self.fault_injection.clear_ingestor_fault(ingestor);
    }

    pub(crate) async fn wait_for_interconnect_status(
        &self,
        node_id: &str,
        peer_node_id: &str,
        expected_status: &str,
    ) -> io::Result<()> {
        self.wait_until(node_id, |status| {
            status
                .interconnect
                .get(peer_node_id)
                .is_some_and(|value| value == expected_status)
        })
        .await
    }

    pub(crate) async fn wait_for_status_contains(
        &self,
        node_id: &str,
        fragment: &str,
    ) -> io::Result<()> {
        self.wait_until(node_id, |status| status.raw.contains(fragment))
            .await
    }

    pub(crate) async fn wait_for_observability_response(
        &self,
        node_id: &str,
        path: &str,
        expected_status: u16,
        expected_body: &str,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        let url = format!("http://{}{}", handle.spec.observability_addr(), path);
        let client = reqwest::Client::new();
        let start = Instant::now();
        let mut last_response = None;
        let mut last_error = None;

        while start.elapsed() < STATUS_WAIT_BUDGET {
            tokio::task::consume_budget().await;
            match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    match response.text().await {
                        Ok(body) => {
                            if status == expected_status && body.trim() == expected_body {
                                return Ok(());
                            }
                            last_response = Some((status, body));
                        }
                        Err(error) => {
                            last_error = Some(io::Error::other(error));
                        }
                    }
                }
                Err(error) => {
                    last_error = Some(io::Error::other(error));
                }
            }
            sleep(POLL_INTERVAL).await;
        }

        let mut message = format!(
            "timed out waiting for observability response from node '{node_id}' at {url}; \
             expected status {expected_status} body {expected_body:?}"
        );
        if let Some((status, body)) = last_response {
            message.push_str(&format!("\nlast response: status={status} body={body:?}"));
        }
        if let Some(err) = last_error {
            message.push_str(&format!("\nlast error: {err}"));
        }
        Err(io::Error::other(message))
    }

    pub(crate) async fn wait_for_observability_response_containing(
        &self,
        node_id: &str,
        path: &str,
        expected_status: u16,
        expected_body_fragment: &str,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        let url = format!("http://{}{}", handle.spec.observability_addr(), path);
        let client = reqwest::Client::new();
        let start = Instant::now();
        let mut last_response = None;
        let mut last_error = None;

        while start.elapsed() < STATUS_WAIT_BUDGET {
            tokio::task::consume_budget().await;
            match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    match response.text().await {
                        Ok(body) => {
                            if status == expected_status && body.contains(expected_body_fragment) {
                                return Ok(());
                            }
                            last_response = Some((status, body));
                        }
                        Err(error) => {
                            last_error = Some(io::Error::other(error));
                        }
                    }
                }
                Err(error) => {
                    last_error = Some(io::Error::other(error));
                }
            }
            sleep(POLL_INTERVAL).await;
        }

        let mut message = format!(
            "timed out waiting for observability response from node '{node_id}' at {url}; \
             expected status {expected_status} body containing {expected_body_fragment:?}"
        );
        if let Some((status, body)) = last_response {
            message.push_str(&format!("\nlast response: status={status} body={body:?}"));
        }
        if let Some(err) = last_error {
            message.push_str(&format!("\nlast error: {err}"));
        }
        Err(io::Error::other(message))
    }

    pub(crate) async fn wait_for_observability_metric_value(
        &self,
        node_id: &str,
        metric_name: &str,
        label_fragments: &[String],
        expected_value: i64,
        wait: Option<Duration>,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let url = format!("http://{}/metrics", handle.spec.observability_addr());
        let client = reqwest::Client::new();
        let mut last_response = None;
        let mut last_error = None;
        let mut last_matching_lines = Vec::new();
        let deadline = Instant::now() + wait.unwrap_or(STATUS_WAIT_BUDGET);

        while Instant::now() < deadline {
            tokio::task::consume_budget().await;
            let remaining = deadline.saturating_duration_since(Instant::now());
            let response = timeout(remaining, async {
                let response = client.get(&url).send().await?;
                let status = response.status().as_u16();
                response.text().await.map(|body| (status, body))
            })
            .await;
            match response {
                Ok(Ok((status, body))) => {
                    if status == 200
                        && observability_metric_has_value(
                            &body,
                            metric_name,
                            label_fragments,
                            expected_value,
                            &mut last_matching_lines,
                        )
                    {
                        return Ok(());
                    }
                    last_response = Some((status, body));
                }
                Ok(Err(error)) => {
                    last_error = Some(io::Error::other(error));
                }
                Err(_) => break,
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            sleep(POLL_INTERVAL.min(remaining)).await;
        }

        let mut message = format!(
            "timed out waiting for observability metric '{metric_name}' from node '{node_id}' at \
             {url}; expected labels {label_fragments:?} and value {expected_value}"
        );
        if !last_matching_lines.is_empty() {
            message.push_str(&format!("\nlast matching lines: {last_matching_lines:?}"));
        }
        if let Some((status, body)) = last_response {
            message.push_str(&format!("\nlast response: status={status} body={body:?}"));
        }
        if let Some(err) = last_error {
            message.push_str(&format!("\nlast error: {err}"));
        }
        Err(io::Error::other(message))
    }

    /// Wait until one series reaches `minimum_value`, which is how a scenario reads a counter that
    /// keeps rising while it is being observed.
    pub(crate) async fn wait_for_observability_metric_at_least(
        &self,
        node_id: &str,
        metric_name: &str,
        label_fragments: &[String],
        minimum_value: i64,
        wait: Option<Duration>,
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let url = format!("http://{}/metrics", handle.spec.observability_addr());
        let client = reqwest::Client::new();
        let mut last_response = None;
        let mut last_error = None;
        let mut last_matching_lines = Vec::new();
        let deadline = Instant::now() + wait.unwrap_or(STATUS_WAIT_BUDGET);

        while Instant::now() < deadline {
            tokio::task::consume_budget().await;
            let remaining = deadline.saturating_duration_since(Instant::now());
            let response = timeout(remaining, async {
                let response = client.get(&url).send().await?;
                let status = response.status().as_u16();
                response.text().await.map(|body| (status, body))
            })
            .await;
            match response {
                Ok(Ok((status, body))) => {
                    if status == 200
                        && observability_metric_reaches(
                            &body,
                            metric_name,
                            label_fragments,
                            minimum_value,
                            &mut last_matching_lines,
                        )
                    {
                        return Ok(());
                    }
                    last_response = Some((status, body));
                }
                Ok(Err(error)) => {
                    last_error = Some(io::Error::other(error));
                }
                Err(_) => break,
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            sleep(POLL_INTERVAL.min(remaining)).await;
        }

        let mut message = format!(
            "timed out waiting for observability metric '{metric_name}' from node '{node_id}' at \
             {url}; expected labels {label_fragments:?} and a value of at least {minimum_value}"
        );
        if !last_matching_lines.is_empty() {
            message.push_str(&format!("\nlast matching lines: {last_matching_lines:?}"));
        }
        if let Some((status, body)) = last_response {
            message.push_str(&format!("\nlast response: status={status} body={body:?}"));
        }
        if let Some(err) = last_error {
            message.push_str(&format!("\nlast error: {err}"));
        }
        Err(io::Error::other(message))
    }

    /// Read every interconnection series once and report the samples carrying a label outside the
    /// bounded set the proposal allows.
    pub(crate) async fn unbounded_interconnection_metric_samples(
        &self,
        node_id: &str,
    ) -> io::Result<Vec<String>> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let url = format!("http://{}/metrics", handle.spec.observability_addr());
        let client = reqwest::Client::new();
        let response = timeout(STATUS_WAIT_BUDGET, async {
            let response = client.get(&url).send().await?;
            let status = response.status().as_u16();
            response.text().await.map(|body| (status, body))
        })
        .await
        .map_err(io::Error::other)?
        .map_err(io::Error::other)?;
        let (status, body) = response;
        if status != 200 {
            return Err(io::Error::other(format!(
                "observability endpoint at {url} answered {status}"
            )));
        }
        Ok(unbounded_interconnection_samples(&body))
    }

    pub(crate) async fn current_leader(&self, node_id: &str) -> io::Result<Option<String>> {
        let status = self
            .cluster_status(node_id, PhaseDeadline::after(STATUS_REQUEST_TIMEOUT))
            .await
            .map_err(|error| {
                io::Error::other(format!("node '{node_id}' status request failed: {error:#}"))
            })?;
        Ok(status.current_leader)
    }

    /// The leader every reachable node agrees on, ignoring nodes a scenario has stopped.
    pub(crate) async fn wait_for_leader_among_running(&self) -> io::Result<String> {
        let deadline = PhaseDeadline::after(STATUS_WAIT_BUDGET);
        while !deadline.has_passed() {
            tokio::task::consume_budget().await;
            let mut reported = BTreeMap::new();
            for (node_id, status) in self.cluster_statuses(deadline).await {
                if let Ok(status) = status {
                    reported.insert(node_id, status);
                }
            }
            let mut leaders = reported
                .values()
                .map(|status| status.current_leader.clone())
                .collect::<Vec<_>>();
            leaders.dedup();
            if let [Some(leader)] = leaders.as_slice()
                && reported
                    .get(leader)
                    .is_some_and(|status| status.raft_state.as_deref() == Some("Leader"))
            {
                return Ok(leader.clone());
            }
            deadline.pause(POLL_INTERVAL).await;
        }
        Err(io::Error::other(format!(
            "timed out after {:?} waiting for a leader the running nodes agree on",
            deadline.elapsed()
        )))
    }

    pub(crate) async fn wait_for_consistent_leader_on_all_nodes(&self) -> io::Result<String> {
        let deadline = PhaseDeadline::after(STATUS_WAIT_BUDGET);
        let mut last_statuses = BTreeMap::new();
        let mut last_failures = BTreeMap::new();
        let mut stable_leader = None;
        let mut stable_count = 0u8;

        while !deadline.has_passed() {
            tokio::task::consume_budget().await;
            let mut leader = None;
            let mut consistent = true;

            for (node_id, status) in self.cluster_statuses(deadline).await {
                match status {
                    Ok(status) => {
                        if let Some(current) = status.current_leader.clone() {
                            if let Some(expected) = leader.as_ref() {
                                if expected != &current {
                                    consistent = false;
                                }
                            } else {
                                leader = Some(current);
                            }
                        } else {
                            consistent = false;
                        }
                        last_statuses.insert(node_id, status);
                    }
                    Err(failure) => {
                        consistent = false;
                        last_failures.insert(node_id, failure);
                    }
                }
            }

            if consistent
                && let Some(leader_id) = leader
                && let Some(leader_status) = last_statuses.get(&leader_id)
                && leader_status.raft_state.as_deref() == Some("Leader")
            {
                if stable_leader.as_deref() == Some(leader_id.as_str()) {
                    stable_count = stable_count
                        .checked_add(1)
                        .expect("a leader is polled a bounded number of times");
                } else {
                    stable_leader = Some(leader_id.clone());
                    stable_count = 1;
                }
                if stable_count >= 3 {
                    return Ok(leader_id);
                }
            } else {
                stable_leader = None;
                stable_count = 0;
            }

            deadline.pause(POLL_INTERVAL).await;
        }

        let mut message = format!(
            "timed out after {:?} waiting for a consistent cluster leader",
            deadline.elapsed()
        );
        for (node_id, status) in last_statuses {
            message.push_str(&format!("\nlast status for '{node_id}':\n{}", status.raw));
        }
        for (node_id, failure) in last_failures {
            message.push_str(&format!("\nlast error for '{node_id}': {failure:#}"));
        }
        Err(io::Error::other(message))
    }

    /// Every node's status text, requested from all nodes at once under the short diagnostic
    /// budget. Each node keeps its own outcome, so a node that never replies costs the caller at
    /// most that budget and cannot hide another node's status.
    pub(crate) async fn collect_status_snapshots(
        &self,
    ) -> BTreeMap<String, Result<String, Report<StatusRequestError>>> {
        let endpoints = self.status_endpoints();
        StatusEndpoint::cluster_statuses(&endpoints, PhaseDeadline::after(STATUS_DIAGNOSTIC_BUDGET))
            .await
    }

    /// One node's status text, requested within `phase`. Scenario steps that wait on a status
    /// fragment use it so no status request outlives the step's own deadline.
    pub(crate) async fn status_text(
        &self,
        node_id: &str,
        phase: PhaseDeadline,
    ) -> Result<String, Report<StatusRequestError>> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle.status_endpoint().cluster_status(phase).await
    }

    async fn cluster_status(
        &self,
        node_id: &str,
        phase: PhaseDeadline,
    ) -> Result<ClusterStatus, Report<StatusRequestError>> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle.cluster_status(phase).await
    }

    /// Every node's parsed status, requested from all nodes at once within `phase`.
    async fn cluster_statuses(
        &self,
        phase: PhaseDeadline,
    ) -> BTreeMap<String, Result<ClusterStatus, Report<StatusRequestError>>> {
        let endpoints = self.status_endpoints();
        let statuses = StatusEndpoint::cluster_statuses(&endpoints, phase).await;
        statuses
            .into_iter()
            .map(|(node_id, status)| (node_id, status.map(ClusterStatus::parse)))
            .collect()
    }

    fn status_endpoints(&self) -> BTreeMap<String, StatusEndpoint> {
        self.nodes
            .iter()
            .map(|(node_id, handle)| (node_id.clone(), handle.status_endpoint()))
            .collect()
    }

    async fn wait_for_last_applied_at_least(
        &self,
        node_id: &str,
        expected_min: u64,
    ) -> io::Result<()> {
        self.wait_until(node_id, |status| {
            status
                .last_applied
                .is_some_and(|value| value >= expected_min)
        })
        .await
    }

    pub(crate) async fn any_follower_node(&self, node_id: &str) -> io::Result<String> {
        let leader = self
            .current_leader(node_id)
            .await?
            .ok_or_else(|| io::Error::other("cluster has no elected leader"))?;
        self.nodes
            .keys()
            .find(|candidate| candidate.as_str() != leader)
            .cloned()
            .ok_or_else(|| io::Error::other("cluster has no follower node"))
    }

    pub(crate) fn node_other_than(&self, excluded_node_id: &str) -> io::Result<String> {
        self.nodes
            .keys()
            .find(|candidate| candidate.as_str() != excluded_node_id)
            .cloned()
            .ok_or_else(|| io::Error::other("cluster has no alternate node"))
    }

    pub(crate) fn transfer_leadership(&self, from_node_id: &str, to_node_id: &str) {
        self.fault_injection
            .request_leadership_transfer(node_name(from_node_id), node_name(to_node_id));
    }
    async fn wait_until<F>(&self, node_id: &str, predicate: F) -> io::Result<()>
    where
        F: Fn(&ClusterStatus) -> bool,
    {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let deadline = PhaseDeadline::after(STATUS_WAIT_BUDGET);
        let waited = deadline
            .poll_until(
                POLL_INTERVAL,
                |phase| handle.cluster_status(phase),
                predicate,
            )
            .await;
        let expired = match waited {
            Ok(_) => return Ok(()),
            Err(expired) => expired,
        };

        let mut message = format!(
            "timed out after {:?} waiting for cluster state via node '{node_id}'",
            deadline.elapsed()
        );
        if let Some(status) = expired.last_output {
            message.push_str(&format!("\nlast status:\n{}", status.raw));
        }
        if let Some(failure) = expired.last_failure {
            message.push_str(&format!("\nlast error: {failure:#}"));
        }
        Err(io::Error::other(message))
    }
}

/// One node in a test cluster. Every fault is reached through the shared injection handle, so
/// arming a fault and the node that observes it can never drift apart.
#[derive(Debug)]
struct NodeHandle {
    spec: NodeSpec,
    fault_injection: FaultInjection,
    config: TestClusterConfig,
    task: OwnedNodeTask,
    shutdown: Option<ShutdownCoordinator>,
    live: LiveClusterHandle,
}

impl NodeHandle {
    fn new(
        spec: NodeSpec,
        fault_injection: FaultInjection,
        config: TestClusterConfig,
        live: LiveClusterHandle,
    ) -> Self {
        Self {
            spec,
            fault_injection,
            config,
            task: OwnedNodeTask::not_started(),
            shutdown: None,
            live,
        }
    }

    fn start(&mut self) -> io::Result<()> {
        if self.task.is_running() {
            return Ok(());
        }
        if self.config.grpc_mode == InternalTransportMode::Https {
            ensure_dev_tls_assets()?;
        }

        let shutdown = ShutdownCoordinator::new(self.config.shutdown_timeout);
        let db_path = self.spec.db_path()?;
        let application_builder = Application::builder()
            .addr(parse_addr(&self.spec.grpc_addr())?)
            .grpc_mode(self.config.grpc_mode)
            .grpc_https_listen_addr(Some(parse_addr(&self.spec.grpc_https_addr())?))
            .http_listen_addr(parse_addr(&self.spec.http_addr())?)
            .https_listen_addr(parse_addr(&self.spec.https_addr())?)
            .observability_listen_addr(parse_addr(&self.spec.observability_addr())?)
            .web_console_listen_addr(parse_addr(&self.spec.web_console_addr())?)
            .web_console_advertise_addr(Some(parse_addr(&self.spec.web_console_addr())?.into()))
            .cluster_id("cucumber".to_string())
            .node_id(node_name(&self.spec.node_id))
            .grpc_advertise_addr(parse_addr(&self.spec.grpc_addr())?.into())
            .grpc_https_advertise_addr(Some(parse_addr(&self.spec.grpc_https_addr())?.into()))
            .interconnect_listen_addr(self.spec.interconnect_listen_addr())
            .interconnect_advertise_addr(
                self.spec
                    .interconnect_endpoint()
                    .parse::<NodeEndpoint>()
                    .map_err(io::Error::other)?,
            )
            .interconnect_tls_ca(self.spec.interconnect_tls_ca.clone())
            .interconnect_tls_cert(self.spec.interconnect_tls_cert.clone())
            .interconnect_tls_key(self.spec.interconnect_tls_key.clone())
            .allow_bootstrap(self.spec.allow_bootstrap)
            .default_user(TEST_AUTH_USERNAME.to_string())
            .init_default_user_password(Some(TEST_AUTH_PASSWORD.to_string()))
            .node_unavailability_timeout(TEST_NODE_UNAVAILABILITY_TIMEOUT)
            .raft_heartbeat_interval(TEST_RAFT_HEARTBEAT_INTERVAL)
            .raft_election_timeout_min(self.config.raft_election_timeout_min)
            .raft_election_timeout_max(self.config.raft_election_timeout_max)
            .replica_count(self.config.replica_count);
        let application = application_builder
            .state_snapshot_interval(self.config.state_snapshot_interval)
            .transaction_idle_timeout(self.config.transaction_idle_timeout)
            .transaction_tombstone_retention(self.config.transaction_tombstone_retention)
            .command_execution(CommandExecutionPolicy::new(
                self.config.command_retry_validity,
                self.config.command_execution_capacity,
            ))
            .transaction_max_statements(self.config.transaction_max_statements)
            .transaction_max_source_bytes(self.config.transaction_max_source_bytes)
            .transaction_max_open(self.config.transaction_max_open)
            .memory_pressure(self.config.memory_pressure)
            .raft_retention(self.config.raft_retention)
            .cluster_bootstrap_host(self.spec.bootstrap_host.clone())
            .dns(
                self.spec
                    .dns
                    .clone()
                    .unwrap_or_else(DnsConfiguration::system),
            )
            .db_path(db_path)
            .temp_dir(
                self.config
                    .temp_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(DEFAULT_TEMP_DIR)),
            )
            .fault_injection(self.fault_injection.clone())
            .shutdown(shutdown.clone())
            .graceful_shutdown_drain(self.config.graceful_shutdown_drain)
            .drain_timeout(self.config.drain_timeout)
            .build();
        // Published before the task is spawned and handed to that task, so the watchdog sees the
        // node for exactly as long as it runs: the registration leaves the registry when the task
        // ends, whether it returned, failed, panicked or was aborted.
        let live = self
            .live
            .node_started(&self.spec.node_id, StdArc::new(shutdown.clone()));
        self.shutdown = Some(shutdown);
        self.task = OwnedNodeTask::spawn(async move {
            let _live = live;
            application.run().await
        });
        Ok(())
    }

    async fn stop(&mut self) -> io::Result<()> {
        self.request_stop();
        self.wait_stopped().await
    }

    fn shutdown_watchdog_timeout(&self) -> io::Result<Duration> {
        let application_drain_timeout = if self.config.graceful_shutdown_drain {
            self.config.drain_timeout
        } else {
            Duration::ZERO
        };
        let domain_drain_timeout = self
            .fault_injection
            .domain_drain_timeout()
            .unwrap_or(DEFAULT_DOMAIN_DRAIN_TIMEOUT);
        let configured_phase_floor = application_drain_timeout
            .checked_add(branch_task_stop_timeout(domain_drain_timeout))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "configured bounded node shutdown phases exceed Duration::MAX",
                )
            })?;
        Ok(NODE_SHUTDOWN_LIVENESS_WATCHDOG
            .max(configured_phase_floor)
            .max(self.config.shutdown_timeout))
    }

    /// Waits for a node a scenario stopped itself, within the product shutdown deadlines that
    /// scenario configured. Scenario cleanup uses the harness cleanup budget instead.
    async fn wait_stopped(&mut self) -> io::Result<()> {
        let shutdown_timeout = self.shutdown_watchdog_timeout()?;
        let task_result = match self.task.wait(PhaseDeadline::after(shutdown_timeout)).await {
            NodeTaskWaitOutcome::NotStarted => return Ok(()),
            NodeTaskWaitOutcome::AlreadyObserved(outcome)
            | NodeTaskWaitOutcome::Joined(outcome) => {
                if let NodeTaskTerminalOutcome::Panic(_) = outcome.as_ref() {
                    Err(io::Error::other(NodeShutdownError::Task {
                        node: node_name(&self.spec.node_id),
                        outcome,
                    }))
                } else {
                    Ok(())
                }
            }
            NodeTaskWaitOutcome::AbortedAtDeadline(outcome) => {
                Err(io::Error::other(NodeShutdownError::Deadline {
                    node: node_name(&self.spec.node_id),
                    timeout: shutdown_timeout,
                    outcome,
                }))
            }
        };
        self.shutdown = None;
        self.fault_injection
            .unregister_consensus(&node_name(&self.spec.node_id));
        if task_result.is_ok() {
            self.ensure_database_unlocked().await?;
        }
        task_result
    }

    async fn ensure_database_unlocked(&self) -> io::Result<()> {
        let db_path = self.spec.db_path()?;
        database_opens(db_path.clone()).await.map_err(|error| {
            io::Error::other(format!(
                "node '{}' returned before releasing its node database lock: {error}",
                self.spec.node_id
            ))
        })?;
        database_opens(db_path.join("consensus"))
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "node '{}' returned before releasing its consensus database lock: {error}",
                    self.spec.node_id
                ))
            })
    }

    /// Where status requests reach this node: the session endpoint of its configured client
    /// transport, authenticated as the default test user.
    fn status_endpoint(&self) -> StatusEndpoint {
        let authorization = test_basic_authorization();
        match self.config.grpc_mode {
            InternalTransportMode::Http => StatusEndpoint::new(
                SocketAddr::new(HOST, self.spec.grpc_port),
                StatusTransport::Plaintext,
                authorization,
            ),
            InternalTransportMode::Https => StatusEndpoint::new(
                SocketAddr::new(HOST, self.spec.grpc_https_port),
                StatusTransport::Tls {
                    authority: dev_tls_ca_file(),
                },
                authorization,
            ),
        }
    }

    async fn cluster_status(
        &self,
        phase: PhaseDeadline,
    ) -> Result<ClusterStatus, Report<StatusRequestError>> {
        let status = self.status_endpoint().cluster_status(phase).await?;
        Ok(ClusterStatus::parse(status))
    }

    fn abort(&mut self) {
        self.shutdown = None;
        self.task.abort();
    }
}

impl StartableNode for NodeHandle {
    fn launch(&mut self) -> io::Result<()> {
        self.start()
    }

    async fn readiness(
        &mut self,
        attempt: u32,
        readiness: PhaseDeadline,
    ) -> error_stack::Result<(), NodeStartupError> {
        let endpoint = self.status_endpoint();
        let node = node_name(&self.spec.node_id);
        self.task
            .wait_until_ready(&node, attempt, readiness, POLL_INTERVAL, |phase| {
                ReadinessProbeOutcome::probe(&endpoint, phase)
            })
            .await
    }

    /// A node that never became ready has no drain to finish, so its cleanup is the short slice
    /// its startup budget can spare rather than the product's shutdown watchdog. The stop is
    /// requested first, and the task is aborted and joined when the slice ends. A database lock
    /// that outlives the abort surfaces as the next attempt's application error, which ends the
    /// startup with both attempts in its history.
    async fn clean_up(&mut self, cleanup: PhaseDeadline) -> AttemptCleanup {
        self.request_stop();
        let outcome = self.task.wait(cleanup).await;
        self.shutdown = None;
        self.fault_injection
            .unregister_consensus(&node_name(&self.spec.node_id));
        AttemptCleanup::from(outcome)
    }

    fn move_to_fresh_ports(&mut self) -> io::Result<()> {
        self.spec.reallocate_ports()
    }
}

impl TeardownNode for NodeHandle {
    fn node_name(&self) -> String {
        self.spec.node_id.clone()
    }

    fn request_stop(&mut self) {
        if let Some(shutdown) = &self.shutdown {
            shutdown.request_stop();
        }
    }

    fn owned_task(&mut self) -> &mut OwnedNodeTask {
        &mut self.task
    }

    fn release(&mut self) {
        self.shutdown = None;
        self.fault_injection
            .unregister_consensus(&node_name(&self.spec.node_id));
        self.spec.release_ports();
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

/// The stop a live node publishes to the suite watchdog is the one its own teardown uses, so a
/// node the watchdog ends stops exactly the way a scenario's cleanup would have stopped it.
impl NodeStop for ShutdownCoordinator {
    fn request_stop(&self) {
        // Whether this request or an earlier one started the shutdown does not change what the
        // watchdog does next: it waits for the node's task to end either way.
        ShutdownCoordinator::request_stop(self);
    }
}

#[derive(Debug, thiserror::Error)]
enum NodeShutdownError {
    #[error("node '{node}' task terminated with {outcome}")]
    Task {
        node: ClusterNodeName,
        outcome: Arc<NodeTaskTerminalOutcome>,
    },
    #[error(
        "timed out after {timeout:?} waiting for node '{node}' shutdown; terminal task outcome: \
         {outcome}"
    )]
    Deadline {
        node: ClusterNodeName,
        timeout: Duration,
        outcome: Arc<NodeTaskTerminalOutcome>,
    },
}

#[derive(Debug)]
struct NodeSpec {
    node_id: String,
    base_dir: PathBuf,
    interconnect_tls_ca: PathBuf,
    interconnect_tls_cert: PathBuf,
    interconnect_tls_key: PathBuf,
    syslog_ingestor_host: IpAddr,
    allow_bootstrap: bool,
    bootstrap_host: Option<String>,
    grpc_port: u16,
    grpc_https_port: u16,
    http_port: u16,
    https_port: u16,
    observability_port: u16,
    web_console_port: u16,
    interconnect_port: u16,
    /// The loopback address the interconnect listens on.
    interconnect_ip: IpAddr,
    /// The host the node advertises for its interconnect endpoint.
    interconnect_host: String,
    /// The resolver configuration the node loads, or `None` for the host's own.
    dns: Option<DnsConfiguration>,
}

struct NodePorts {
    grpc: u16,
    grpc_https: u16,
    http: u16,
    https: u16,
    observability: u16,
    web_console: u16,
    interconnect: u16,
}

impl NodePorts {
    fn allocate() -> io::Result<Self> {
        let mut ports = next_ports(7)?.into_iter();
        Ok(Self {
            grpc: ports.next().expect("allocated gRPC port"),
            grpc_https: ports.next().expect("allocated gRPC HTTPS port"),
            http: ports.next().expect("allocated HTTP port"),
            https: ports.next().expect("allocated HTTPS port"),
            observability: ports.next().expect("allocated observability port"),
            web_console: ports.next().expect("allocated web console port"),
            interconnect: ports.next().expect("allocated interconnect port"),
        })
    }
}

impl NodeSpec {
    fn new(
        root: &TempDir,
        interconnect_ca: &InterconnectTestCa,
        node_id: &str,
        allow_bootstrap: bool,
    ) -> io::Result<Self> {
        let base_dir = root.path().join(node_id);
        std::fs::create_dir_all(&base_dir)?;
        let (interconnect_tls_cert, interconnect_tls_key) =
            interconnect_ca.issue_node(node_id, &base_dir)?;
        let ports = NodePorts::allocate()?;
        let syslog_ingestor_host = Self::syslog_ingestor_host(node_id)?;

        Ok(Self {
            node_id: node_id.to_string(),
            base_dir,
            interconnect_tls_ca: interconnect_ca.path.clone(),
            interconnect_tls_cert,
            interconnect_tls_key,
            syslog_ingestor_host,
            allow_bootstrap,
            bootstrap_host: None,
            grpc_port: ports.grpc,
            grpc_https_port: ports.grpc_https,
            http_port: ports.http,
            https_port: ports.https,
            observability_port: ports.observability,
            web_console_port: ports.web_console,
            interconnect_port: ports.interconnect,
            interconnect_ip: HOST,
            interconnect_host: HOST.to_string(),
            dns: None,
        })
    }

    /// The `<n>` of a test node id `node-<n>`.
    fn index(node_id: &str) -> io::Result<u8> {
        node_id
            .strip_prefix("node-")
            .and_then(|value| value.parse::<u8>().ok())
            .filter(|value| (1..=254).contains(value))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("test node id '{node_id}' must have the form node-1 through node-254"),
                )
            })
    }

    fn syslog_ingestor_host(node_id: &str) -> io::Result<IpAddr> {
        let node_index = Self::index(node_id)?;
        Ok(IpAddr::V4(Ipv4Addr::new(127, 0, 0, node_index)))
    }

    fn set_interconnect_address(&mut self, address: InterconnectAddress) {
        self.interconnect_ip = address.listen_ip;
        self.interconnect_host = address.advertised_host;
    }

    fn reallocate_ports(&mut self) -> io::Result<()> {
        self.release_ports();
        let ports = NodePorts::allocate()?;
        self.grpc_port = ports.grpc;
        self.grpc_https_port = ports.grpc_https;
        self.http_port = ports.http;
        self.https_port = ports.https;
        self.observability_port = ports.observability;
        self.web_console_port = ports.web_console;
        self.interconnect_port = ports.interconnect;
        Ok(())
    }

    /// Move this node to a fresh interconnect address, leaving the address it is giving up
    /// reserved for the rest of the run.
    ///
    /// The reservation set is shared by every scenario running concurrently, and `next_ports`
    /// releases its probe listener as soon as it has read the port number, so the set is the only
    /// thing stopping two scenarios from landing on the same port. Returning this node's old port
    /// to it lets another scenario bind the address a peer is still dialling: every scenario names
    /// its nodes `node-1`, `node-2`, `node-3`, so the impostor passes the peer-identity check and
    /// is only caught when its introduction fails to verify against the expected key. The dialling
    /// node then reports its peer unavailable until gossip carries the new address, which is long
    /// enough to fail the scenario. Only this call site retires ports, and only a few times per
    /// run, so keeping them costs a handful of entries.
    fn reallocate_interconnect_ports(&mut self) -> io::Result<()> {
        let mut ports = next_ports(1)?.into_iter();
        let interconnect_port = ports
            .next()
            .ok_or_else(|| io::Error::other("interconnect port allocation returned no port"))?;
        self.interconnect_port = interconnect_port;
        Ok(())
    }

    /// Return this node's ports to the pool. Every caller reaches here with the node down: two
    /// teardown paths, and the startup retry for a node that never bound them. That is what makes
    /// the release safe, not the stop itself, so a caller that releases while a peer may still dial
    /// the address belongs elsewhere.
    fn release_ports(&mut self) {
        release_test_ports(&[
            self.grpc_port,
            self.grpc_https_port,
            self.http_port,
            self.https_port,
            self.observability_port,
            self.web_console_port,
            self.interconnect_port,
        ]);
    }

    fn grpc_addr(&self) -> String {
        format!("{HOST}:{}", self.grpc_port)
    }

    fn grpc_uri(&self, mode: InternalTransportMode) -> String {
        match mode {
            InternalTransportMode::Http => format!("http://{}", self.grpc_addr()),
            InternalTransportMode::Https => format!("https://{}", self.grpc_https_addr()),
        }
    }

    fn grpc_https_addr(&self) -> String {
        format!("{HOST}:{}", self.grpc_https_port)
    }

    fn websocket_uri(&self, path: &str) -> String {
        format!("ws://{}:{}{path}", HOST, self.http_port)
    }

    fn http_uri(&self, path: &str) -> String {
        format!("http://{}:{}{path}", HOST, self.http_port)
    }

    fn web_console_url(&self) -> String {
        format!(
            "http://{}:{}/console/?auth={}",
            HOST,
            self.web_console_port,
            test_basic_auth_token()
        )
    }

    fn web_console_url_with_password(&self, password: &str) -> String {
        format!(
            "http://{}:{}/console/?auth={}",
            HOST,
            self.web_console_port,
            test_basic_auth_token_for_password(password)
        )
    }

    fn secure_websocket_uri(&self, host: &str, path: &str) -> String {
        format!("wss://{host}:{}{path}", self.https_port)
    }

    fn https_uri(&self, host: &str, path: &str) -> String {
        format!("https://{host}:{}{path}", self.https_port)
    }

    fn http_addr(&self) -> String {
        format!("{HOST}:{}", self.http_port)
    }

    fn https_addr(&self) -> String {
        format!("{HOST}:{}", self.https_port)
    }

    fn observability_addr(&self) -> String {
        format!("{HOST}:{}", self.observability_port)
    }

    fn web_console_addr(&self) -> String {
        format!("{HOST}:{}", self.web_console_port)
    }

    fn interconnect_listen_addr(&self) -> SocketAddr {
        SocketAddr::new(self.interconnect_ip, self.interconnect_port)
    }

    /// The `host:port` endpoint the node advertises and a joining node bootstraps from.
    fn interconnect_endpoint(&self) -> String {
        NodeEndpoint::new(self.interconnect_host.clone(), self.interconnect_port).to_string()
    }

    fn db_path(&self) -> io::Result<PathBuf> {
        let db_path = self.base_dir.join("db");
        std::fs::create_dir_all(&db_path)?;
        Ok(db_path)
    }
}

async fn publish_websocket(
    spec: &NodeSpec,
    host: &str,
    path: &str,
    payload: &str,
) -> io::Result<()> {
    let mut request = spec
        .websocket_uri(path)
        .into_client_request()
        .map_err(io::Error::other)?;
    request.headers_mut().insert(
        "Host",
        HeaderValue::from_str(host).map_err(io::Error::other)?,
    );
    let (mut relay, _) = connect_async(request).await.map_err(io::Error::other)?;
    relay
        .send(WsMessage::Text(payload.to_string()))
        .await
        .map_err(io::Error::other)?;
    let _ = relay.close(None).await;
    Ok(())
}

/// What the HTTPS listeners answered while a background publish loop ran.
#[derive(Debug, Default)]
pub(crate) struct HttpsPublishLoopOutcome {
    pub(crate) accepted: usize,
    pub(crate) failures: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum WebsocketExchangeAction {
    ExpectText(String),
    SendText(String),
    ExpectBinary(Vec<u8>),
    SendBinary(Vec<u8>),
    ExpectClose,
    ExpectSilence(Duration),
}

async fn exchange_websocket(
    spec: &NodeSpec,
    host: &str,
    path: &str,
    actions: &[WebsocketExchangeAction],
) -> io::Result<()> {
    let mut request = spec
        .websocket_uri(path)
        .into_client_request()
        .map_err(io::Error::other)?;
    request.headers_mut().insert(
        "Host",
        HeaderValue::from_str(host).map_err(io::Error::other)?,
    );
    let (mut relay, _) = connect_async(request).await.map_err(io::Error::other)?;
    for action in actions {
        match action {
            WebsocketExchangeAction::ExpectText(expected) => {
                let message = next_exchange_message(&mut relay)
                    .await?
                    .ok_or_else(|| io::Error::other("websocket closed before expected text frame"))?
                    .map_err(io::Error::other)?;
                let WsMessage::Text(actual) = message else {
                    return Err(io::Error::other(format!(
                        "expected websocket text frame {expected:?}, got {message:?}"
                    )));
                };
                if actual != *expected {
                    return Err(io::Error::other(format!(
                        "expected websocket text frame {expected:?}, got {actual:?}"
                    )));
                }
            }
            WebsocketExchangeAction::SendText(payload) => {
                relay
                    .send(WsMessage::Text(payload.clone()))
                    .await
                    .map_err(io::Error::other)?;
            }
            WebsocketExchangeAction::ExpectBinary(expected) => {
                let message = next_exchange_message(&mut relay)
                    .await?
                    .ok_or_else(|| {
                        io::Error::other("websocket closed before expected binary frame")
                    })?
                    .map_err(io::Error::other)?;
                let WsMessage::Binary(actual) = message else {
                    return Err(io::Error::other(format!(
                        "expected websocket binary frame {expected:02x?}, got {message:?}"
                    )));
                };
                if actual != *expected {
                    return Err(io::Error::other(format!(
                        "expected websocket binary frame {expected:02x?}, got {actual:02x?}"
                    )));
                }
            }
            WebsocketExchangeAction::SendBinary(payload) => {
                relay
                    .send(WsMessage::Binary(payload.clone()))
                    .await
                    .map_err(io::Error::other)?;
            }
            WebsocketExchangeAction::ExpectSilence(window) => {
                // Proves a frame is withheld rather than merely delivered later: the peer must
                // stay quiet for the whole window.
                match timeout(*window, futures_util::StreamExt::next(&mut relay)).await {
                    Err(_) => {}
                    Ok(None) => {
                        return Err(io::Error::other(
                            "websocket closed while silence was expected",
                        ));
                    }
                    Ok(Some(Ok(message))) => {
                        return Err(io::Error::other(format!(
                            "expected no websocket frame for {window:?}, got {message:?}"
                        )));
                    }
                    Ok(Some(Err(error))) => return Err(io::Error::other(error)),
                }
            }
            WebsocketExchangeAction::ExpectClose => {
                // The server aborts a rejected session without a close handshake, so a
                // transport error is as valid an outcome as a close frame or clean EOF.
                match next_exchange_message(&mut relay).await? {
                    None | Some(Ok(WsMessage::Close(_))) | Some(Err(_)) => {}
                    Some(Ok(message)) => {
                        return Err(io::Error::other(format!(
                            "expected the websocket to close, got {message:?}"
                        )));
                    }
                }
                return Ok(());
            }
        }
    }
    let _ = relay.close(None).await;
    Ok(())
}

type ExchangeMessage = Result<WsMessage, tokio_tungstenite::tungstenite::Error>;

async fn next_exchange_message<S>(
    relay: &mut WebSocketStream<S>,
) -> io::Result<Option<ExchangeMessage>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    timeout(Duration::from_secs(5), futures_util::StreamExt::next(relay))
        .await
        .map_err(|_| io::Error::other("timed out waiting for a websocket frame"))
}

async fn publish_http(spec: &NodeSpec, host: &str, path: &str, payload: &str) -> io::Result<()> {
    publish_http_bytes(spec, host, path, payload.as_bytes(), "application/json").await
}

async fn publish_http_with_headers(
    spec: &NodeSpec,
    host: &str,
    path: &str,
    payload: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    publish_http_bytes_with_headers(
        spec,
        host,
        path,
        payload.as_bytes(),
        "application/json",
        headers,
    )
    .await
}

async fn publish_http_bytes(
    spec: &NodeSpec,
    host: &str,
    path: &str,
    payload: &[u8],
    content_type: &str,
) -> io::Result<()> {
    publish_http_bytes_with_headers(spec, host, path, payload, content_type, &[]).await
}

async fn publish_http_bytes_with_headers(
    spec: &NodeSpec,
    host: &str,
    path: &str,
    payload: &[u8],
    content_type: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    publish_http_uri_with_headers(spec.http_uri(path), host, payload, content_type, headers).await
}

pub(crate) async fn publish_http_uri_with_headers(
    uri: String,
    host: &str,
    payload: &[u8],
    content_type: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    let client = reqwest::Client::new();
    let mut request = client
        .post(uri)
        .header("Host", host)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(payload.to_vec());
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = request.send().await.map_err(io::Error::other)?;

    if response.status() != reqwest::StatusCode::ACCEPTED {
        return Err(io::Error::other(format!(
            "unexpected http status {}",
            response.status()
        )));
    }

    Ok(())
}

async fn publish_https(
    spec: &NodeSpec,
    host: &str,
    path: &str,
    payload: &str,
    ca_cert_pem: &str,
) -> io::Result<()> {
    let certificate =
        reqwest::Certificate::from_pem(ca_cert_pem.as_bytes()).map_err(io::Error::other)?;
    let client = reqwest::Client::builder()
        .add_root_certificate(certificate)
        .resolve(host, parse_addr(&spec.https_addr())?)
        .build()
        .map_err(io::Error::other)?;
    let response = client
        .post(spec.https_uri(host, path))
        .body(payload.to_string())
        .send()
        .await
        .map_err(io::Error::other)?;

    if response.status() != reqwest::StatusCode::ACCEPTED {
        return Err(io::Error::other(format!(
            "unexpected https status {}",
            response.status()
        )));
    }

    Ok(())
}

async fn connect_https(spec: &NodeSpec, host: &str, ca_cert_pem: &str) -> io::Result<()> {
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(ca_cert_pem.as_bytes()) {
        roots
            .add(certificate.map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    }
    let client_config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(StdArc::new(client_config));
    let tcp_stream = TcpStream::connect(parse_addr(&spec.https_addr())?)
        .await
        .map_err(io::Error::other)?;
    let server_name = ServerName::try_from(host.to_string()).map_err(io::Error::other)?;
    connector
        .connect(server_name, tcp_stream)
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

async fn publish_secure_websocket(
    spec: &NodeSpec,
    host: &str,
    path: &str,
    payload: &str,
    ca_cert_pem: &str,
) -> io::Result<()> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_cert_pem.as_bytes()) {
        roots
            .add(cert.map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    }

    let client_config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(StdArc::new(client_config));
    let tcp_stream = TcpStream::connect(parse_addr(&spec.https_addr())?)
        .await
        .map_err(io::Error::other)?;
    let server_name = ServerName::try_from(host.to_string()).map_err(io::Error::other)?;
    let tls_stream = connector
        .connect(server_name, tcp_stream)
        .await
        .map_err(io::Error::other)?;

    let mut request = spec
        .secure_websocket_uri(host, path)
        .into_client_request()
        .map_err(io::Error::other)?;
    request.headers_mut().insert(
        "Host",
        HeaderValue::from_str(host).map_err(io::Error::other)?,
    );

    let (mut relay, _) = client_async(request, tls_stream)
        .await
        .map_err(io::Error::other)?;
    relay
        .send(WsMessage::Text(payload.to_string()))
        .await
        .map_err(io::Error::other)?;
    let _ = relay.close(None).await;
    Ok(())
}

#[derive(Debug, Clone)]
struct ClusterStatus {
    raw: String,
    current_leader: Option<String>,
    raft_state: Option<String>,
    last_applied: Option<u64>,
    membership: BTreeMap<String, String>,
    interconnect: BTreeMap<String, String>,
}

#[derive(Debug)]
pub(crate) struct BrokerMessage {
    /// The payload as text, with any byte that is not valid UTF-8 replaced.
    pub(crate) payload: String,
    /// The payload exactly as the broker delivered it.
    pub(crate) bytes: Vec<u8>,
    pub(crate) headers: Vec<(String, String)>,
}

impl BrokerMessage {
    fn payload(payload: String) -> Self {
        Self {
            bytes: payload.as_bytes().to_vec(),
            payload,
            headers: Vec::new(),
        }
    }

    fn from_bytes(bytes: &[u8], headers: Vec<(String, String)>) -> Self {
        Self {
            payload: String::from_utf8_lossy(bytes).to_string(),
            bytes: bytes.to_vec(),
            headers,
        }
    }
}

#[derive(Debug)]
pub(crate) struct BrokerObserver {
    payload_rx: mpsc::Receiver<BrokerMessage>,
    task: Option<JoinHandle<()>>,
}

impl BrokerObserver {
    pub(crate) async fn next_message(&mut self) -> io::Result<BrokerMessage> {
        timeout(BROKER_TIMEOUT, self.payload_rx.recv())
            .await
            .map_err(|_| io::Error::other("timed out waiting for broker payload"))?
            .ok_or_else(|| io::Error::other("broker observer closed before delivering payload"))
    }

    pub(crate) async fn try_next_payload(
        &mut self,
        duration: Duration,
    ) -> io::Result<Option<String>> {
        self.try_next_message(duration)
            .await
            .map(|message| message.map(|message| message.payload))
    }

    pub(crate) async fn try_next_message(
        &mut self,
        duration: Duration,
    ) -> io::Result<Option<BrokerMessage>> {
        match timeout(duration, self.payload_rx.recv()).await {
            Ok(Some(message)) => Ok(Some(message)),
            Ok(None) => Err(io::Error::other(
                "broker observer closed before delivering payload",
            )),
            Err(_) => Ok(None),
        }
    }
}

impl Drop for BrokerObserver {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl ClusterStatus {
    fn parse(raw: String) -> Self {
        let mut current_leader = None;
        let mut raft_state = None;
        let mut last_applied = None;
        let mut membership = BTreeMap::new();
        let mut interconnect = BTreeMap::new();
        let mut in_membership = false;
        let mut in_interconnect = false;

        for line in raw.lines() {
            if let Some(value) = line.strip_prefix("raft.current_leader: ") {
                let trimmed = value.trim();
                if trimmed != "(none)" {
                    current_leader = Some(trimmed.to_string());
                }
            }

            if let Some(value) = line.strip_prefix("raft.state: ") {
                raft_state = Some(value.trim().to_string());
            }

            if let Some(value) = line.strip_prefix("raft.last_applied: ") {
                let trimmed = value.trim();
                if trimmed != "(none)" {
                    last_applied = trimmed.parse::<u64>().ok();
                }
            }

            if line == "raft.membership:" {
                in_membership = true;
                in_interconnect = false;
                continue;
            }

            if line == "[interconnect]" {
                in_interconnect = true;
                in_membership = false;
                continue;
            }

            if in_membership {
                if !line.starts_with("- ") {
                    in_membership = false;
                } else if let Some((node_id, role)) = parse_membership_line(line) {
                    membership.insert(node_id, role);
                }
            }

            if in_interconnect {
                if !line.starts_with("- ") {
                    in_interconnect = false;
                } else if let Some((node_id, status)) = parse_interconnect_line(line) {
                    interconnect.insert(node_id, status);
                }
            }
        }

        Self {
            raw,
            current_leader,
            raft_state,
            last_applied,
            membership,
            interconnect,
        }
    }
}

fn parse_membership_line(line: &str) -> Option<(String, String)> {
    let item = line.strip_prefix("- ")?;
    let (node_id, rest) = item.split_once(" [")?;
    let (role, _) = rest.split_once(']')?;
    Some((node_id.to_string(), role.to_string()))
}

fn parse_interconnect_line(line: &str) -> Option<(String, String)> {
    let item = line.strip_prefix("- ")?;
    let (node_id, rest) = item.split_once(": ")?;
    let (_, status) = rest.split_once("status=")?;
    Some((node_id.to_string(), status.to_string()))
}

/// Renders a client outcome as its aggregate message followed by every non-empty statement
/// message.
pub(crate) fn flatten_outcome_messages(outcome: &nervix_client_core::CommandOutcome) -> String {
    let mut messages = Vec::new();
    if !outcome.message.is_empty() {
        messages.push(outcome.message.clone());
    }
    for statement in &outcome.statements {
        if !statement.message.is_empty() {
            messages.push(statement.message.clone());
        }
    }
    messages.join("\n")
}

/// A scenario domain as a client selects it; the empty text selects none.
pub(crate) fn client_domain(domain: &str) -> Option<nervix_client_core::DomainName> {
    if domain.is_empty() {
        return None;
    }
    Some(
        nervix_client_core::DomainName::parse(domain)
            .expect("scenario domains are valid domain names"),
    )
}

pub(crate) async fn run_command_via_client(
    server: &str,
    domain: &str,
    query: &str,
) -> io::Result<String> {
    let client = Client::connect_with_options(
        server,
        client_domain(domain),
        client_connect_options(server)?,
    )
    .await
    .map_err(io::Error::other)?;
    let outcome = client
        .execute(query.to_string())
        .await
        .map_err(io::Error::other)?;
    if outcome.succeeded() {
        Ok(flatten_outcome_messages(&outcome))
    } else {
        Err(io::Error::other(format!(
            "command failed: {}\ndiagnostics: {:?}",
            outcome.message, outcome.diagnostics
        )))
    }
}

async fn publish_mqtt(
    dependencies: &DependencyEndpoints,
    topic: &str,
    payload: &str,
) -> io::Result<()> {
    publish_mqtt_with_qos(dependencies, topic, payload, QoS::AtMostOnce, true).await
}

async fn publish_mqtt_with_qos(
    dependencies: &DependencyEndpoints,
    topic: &str,
    payload: &str,
    qos: QoS,
    retain: bool,
) -> io::Result<()> {
    let client_id = format!("nervix-cucumber-{}", Uuid::now_v7().as_simple());
    let (host, port) = dependency_host_port(dependencies, MQTT_ADDR, 1883)?;
    let options = MqttOptions::new(client_id, (host, port));
    let (client, mut eventloop) = AsyncClient::builder(options).capacity(16).build();
    let driver = tokio::spawn(async move {
        loop {
            tokio::task::consume_budget().await;
            if eventloop.poll().await.is_err() {
                break;
            }
        }
    });

    let mut last_error = None;
    for attempt in 0..5 {
        tokio::task::consume_budget().await;
        // Runtime ingestors can still be attaching to the broker immediately after START.
        // Retaining the per-test input payload makes MQTT publishes deterministic without
        // changing application-level topic reuse, because scenario topics are unique.
        match client
            .publish(topic, payload, PublishOptions::new(qos).retain(retain))
            .await
        {
            Ok(()) => {
                sleep(POLL_INTERVAL).await;
                driver.abort();
                let _ = driver.await;
                return Ok(());
            }
            Err(error) => {
                last_error = Some(io::Error::other(format!(
                    "failed to publish mqtt message to topic '{topic}': {error}"
                )));
                if attempt < 4 {
                    sleep(POLL_INTERVAL).await;
                }
            }
        }
    }

    sleep(POLL_INTERVAL).await;
    driver.abort();
    let _ = driver.await;
    Err(last_error.unwrap_or_else(|| {
        io::Error::other(format!("failed to publish mqtt message to topic '{topic}'"))
    }))
}

async fn publish_mqtt_burst(
    dependencies: &DependencyEndpoints,
    topic: &str,
    payload: &str,
    count: usize,
) -> io::Result<()> {
    publish_mqtt_burst_with_qos(dependencies, topic, payload, count, QoS::AtMostOnce).await
}

async fn publish_mqtt_burst_with_qos(
    dependencies: &DependencyEndpoints,
    topic: &str,
    payload: &str,
    count: usize,
    qos: QoS,
) -> io::Result<()> {
    let client_id = format!("nervix-cucumber-burst-{}", Uuid::now_v7().as_simple());
    let (host, port) = dependency_host_port(dependencies, MQTT_ADDR, 1883)?;
    let options = MqttOptions::new(client_id, (host, port));
    let (client, mut eventloop) = AsyncClient::builder(options)
        .capacity(count.max(16))
        .build();
    let driver = tokio::spawn(async move {
        loop {
            tokio::task::consume_budget().await;
            if eventloop.poll().await.is_err() {
                break;
            }
        }
    });

    for _ in 0..count {
        tokio::task::consume_budget().await;
        client
            .publish(topic, payload, PublishOptions::new(qos))
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "failed to publish mqtt burst message to topic '{topic}': {error}"
                ))
            })?;
    }
    sleep(POLL_INTERVAL).await;
    driver.abort();
    let _ = driver.await;
    Ok(())
}

async fn ensure_rabbitmq_queue(dependencies: &DependencyEndpoints, queue: &str) -> io::Result<()> {
    let connection = Connection::connect(
        dependencies.get(RABBITMQ_ADDR)?,
        ConnectionProperties::default(),
    )
    .await
    .map_err(io::Error::other)?;
    let channel = connection
        .create_channel()
        .await
        .map_err(io::Error::other)?;
    declare_rabbitmq_queue(&channel, queue).await
}

async fn publish_rabbitmq(
    dependencies: &DependencyEndpoints,
    queue: &str,
    payload: &str,
) -> io::Result<()> {
    let connection = Connection::connect(
        dependencies.get(RABBITMQ_ADDR)?,
        ConnectionProperties::default(),
    )
    .await
    .map_err(io::Error::other)?;
    let channel = connection
        .create_channel()
        .await
        .map_err(io::Error::other)?;
    declare_rabbitmq_queue(&channel, queue).await?;
    channel
        .basic_publish(
            "".into(),
            queue.into(),
            BasicPublishOptions::default(),
            payload.as_bytes(),
            BasicProperties::default(),
        )
        .await
        .map_err(io::Error::other)?
        .await
        .map_err(io::Error::other)?;
    sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn declare_rabbitmq_queue(channel: &lapin::Channel, queue: &str) -> io::Result<()> {
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

async fn rabbitmq_queue_consumer_count(
    dependencies: &DependencyEndpoints,
    queue: &str,
) -> io::Result<usize> {
    let connection = Connection::connect(
        dependencies.get(RABBITMQ_ADDR)?,
        ConnectionProperties::default(),
    )
    .await
    .map_err(io::Error::other)?;
    let channel = connection
        .create_channel()
        .await
        .map_err(io::Error::other)?;
    let declared = channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(io::Error::other)?;
    Ok(declared.consumer_count().arch_into())
}

async fn publish_redis(
    dependencies: &DependencyEndpoints,
    channel: &str,
    payload: &str,
) -> io::Result<()> {
    let client = TestRedisClient::open(dependencies.get(REDIS_ADDR)?)?;
    let mut connection = client.connect().await?;
    publish_redis_to_subscriber(&mut connection, channel, payload).await?;
    sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn publish_redis_burst(
    dependencies: &DependencyEndpoints,
    channel: &str,
    payload: &str,
    count: usize,
) -> io::Result<()> {
    let client = TestRedisClient::open(dependencies.get(REDIS_ADDR)?)?;
    let mut connection = client.connect().await?;
    for _ in 0..count {
        tokio::task::consume_budget().await;
        publish_redis_to_subscriber(&mut connection, channel, payload).await?;
    }
    sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn publish_redis_to_subscriber(
    connection: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    payload: &str,
) -> io::Result<()> {
    let deadline = Instant::now() + BROKER_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        let subscriber_count: usize = connection
            .publish(channel, payload)
            .await
            .map_err(io::Error::other)?;
        if subscriber_count > 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out publishing redis message to channel '{channel}' with at least one \
                 subscriber"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn redis_channel_subscriber_count(
    dependencies: &DependencyEndpoints,
    channel: &str,
) -> io::Result<usize> {
    let client = TestRedisClient::open(dependencies.get(REDIS_ADDR)?)?;
    let mut connection = client.connect().await?;
    let counts: Vec<(String, usize)> = redis::cmd("PUBSUB")
        .arg("NUMSUB")
        .arg(channel)
        .query_async(&mut connection)
        .await
        .map_err(io::Error::other)?;
    Ok(counts
        .into_iter()
        .find_map(|(observed_channel, count)| {
            if observed_channel == channel {
                Some(count)
            } else {
                None
            }
        })
        .unwrap_or(0))
}

async fn wait_for_redis_channel_subscribers(
    dependencies: &DependencyEndpoints,
    channel: &str,
    expected: usize,
) -> io::Result<()> {
    let deadline = Instant::now() + STATUS_WAIT_BUDGET;
    loop {
        tokio::task::consume_budget().await;
        match redis_channel_subscriber_count(dependencies, channel).await {
            Ok(actual) if actual == expected => return Ok(()),
            Ok(_) => {}
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            let actual = redis_channel_subscriber_count(dependencies, channel).await?;
            return Err(io::Error::other(format!(
                "timed out waiting for redis channel '{channel}' to have {expected} subscribers, \
                 got {actual}"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

fn pulsar_topic(topic: &str) -> String {
    format!("persistent://public/default/{topic}")
}

async fn publish_pulsar(
    dependencies: &DependencyEndpoints,
    topic: &str,
    payload: &str,
) -> io::Result<()> {
    publish_pulsar_with_addr(dependencies.get(PULSAR_ADDR)?, None, topic, payload).await
}

async fn publish_pulsar_tls(
    dependencies: &DependencyEndpoints,
    topic: &str,
    payload: &str,
) -> io::Result<()> {
    publish_pulsar_with_addr(
        dependencies.get(PULSAR_TLS_ADDR)?,
        Some(dependencies.tls_ca_pem()?),
        topic,
        payload,
    )
    .await
}

async fn publish_pulsar_with_addr(
    addr: &str,
    ca_certificate_chain: Option<Vec<u8>>,
    topic: &str,
    payload: &str,
) -> io::Result<()> {
    let topic = pulsar_topic(topic);
    let mut last_error = None;
    for attempt in 0..5 {
        let result: io::Result<()> = async {
            let mut builder = Pulsar::builder(addr, TokioExecutor);
            if let Some(ca_certificate_chain) = ca_certificate_chain.clone() {
                builder = builder.with_certificate_chain(ca_certificate_chain);
            }
            let pulsar: Pulsar<_> = builder.build().await.map_err(io::Error::other)?;
            let mut producer = pulsar
                .producer()
                .with_topic(&topic)
                .build()
                .await
                .map_err(io::Error::other)?;
            producer
                .send_non_blocking(payload)
                .await
                .map_err(io::Error::other)?
                .await
                .map_err(io::Error::other)?;
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                sleep(POLL_INTERVAL).await;
                return Ok(());
            }
            Err(error) => {
                last_error = Some(io::Error::new(
                    error.kind(),
                    format!("failed to publish pulsar message to topic '{topic}': {error}"),
                ));
                if attempt < 4 {
                    sleep(POLL_INTERVAL).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::other(format!(
            "failed to publish pulsar message to topic '{topic}'"
        ))
    }))
}

async fn publish_kafka(
    dependencies: &DependencyEndpoints,
    topic: &str,
    payload: &str,
) -> io::Result<()> {
    publish_kafka_record(dependencies, topic, None, payload, &[]).await
}

async fn publish_kafka_with_headers(
    dependencies: &DependencyEndpoints,
    topic: &str,
    payload: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    publish_kafka_record(dependencies, topic, None, payload, headers).await
}

/// Publishes every payload through one producer, so the ingestor polls them as one group.
async fn publish_kafka_payloads(
    dependencies: &DependencyEndpoints,
    topic: &str,
    payloads: &[String],
) -> io::Result<()> {
    let mut client_config = kafka_client_config(dependencies)?;
    let producer: FutureProducer = client_config
        .set("message.timeout.ms", "5000")
        .set("delivery.timeout.ms", "5000")
        .set("request.timeout.ms", "5000")
        .create()
        .map_err(io::Error::other)?;
    let mut deliveries = Vec::with_capacity(payloads.len());
    for payload in payloads {
        tokio::task::consume_budget().await;
        deliveries.push(producer.send(
            FutureRecord::<(), str>::to(topic).payload(payload.as_str()),
            Duration::from_secs(5),
        ));
    }
    let results = tokio::time::timeout(
        Duration::from_secs(10),
        futures_util::future::join_all(deliveries),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("timed out publishing kafka burst to topic '{topic}'"),
        )
    })?;
    for result in results {
        result.map_err(|(error, _)| io::Error::other(error))?;
    }
    Ok(())
}

async fn publish_kafka_partition(
    dependencies: &DependencyEndpoints,
    topic: &str,
    partition: i32,
    payload: &str,
) -> io::Result<()> {
    publish_kafka_record(dependencies, topic, Some(partition), payload, &[]).await
}

async fn publish_kafka_record(
    dependencies: &DependencyEndpoints,
    topic: &str,
    partition: Option<i32>,
    payload: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    let mut client_config = kafka_client_config(dependencies)?;
    let producer: FutureProducer = client_config
        .set("message.timeout.ms", "5000")
        .set("delivery.timeout.ms", "5000")
        .set("request.timeout.ms", "5000")
        .create()
        .map_err(io::Error::other)?;
    // Kafka topic creation and consumer assignment can lag slightly behind setup.
    let mut last_error = None;
    for attempt in 0..3 {
        let delivery = tokio::time::timeout(
            Duration::from_secs(6),
            producer.send(
                {
                    let mut record = FutureRecord::<(), str>::to(topic).payload(payload);
                    if !headers.is_empty() {
                        let owned_headers = headers.iter().fold(
                            OwnedHeaders::new_with_capacity(headers.len()),
                            |owned_headers, (key, value)| {
                                owned_headers.insert(KafkaHeader {
                                    key,
                                    value: Some(*value),
                                })
                            },
                        );
                        record = record.headers(owned_headers);
                    }
                    if let Some(partition) = partition {
                        record.partition(partition)
                    } else {
                        record
                    }
                },
                Duration::from_secs(5),
            ),
        )
        .await;
        match delivery {
            Err(_) => {
                last_error = Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("timed out publishing kafka message to topic '{topic}'"),
                ));
                if attempt < 2 {
                    sleep(POLL_INTERVAL).await;
                }
            }
            Ok(Err((error, _))) => {
                last_error = Some(io::Error::other(error));
                if attempt < 2 {
                    sleep(POLL_INTERVAL).await;
                }
            }
            Ok(Ok(_)) => return Ok(()),
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("failed to publish kafka message")))
}

fn kafka_admin_client(
    dependencies: &DependencyEndpoints,
) -> io::Result<AdminClient<DefaultClientContext>> {
    let client_config = kafka_client_config(dependencies)?;
    client_config.create().map_err(io::Error::other)
}

fn kafka_topic_partition_count(
    dependencies: &DependencyEndpoints,
    topic: &str,
) -> io::Result<Option<usize>> {
    let mut client_config = kafka_client_config(dependencies)?;
    let consumer: BaseConsumer = client_config
        .set(
            "group.id",
            format!("nervix-cucumber-admin-{}", Uuid::now_v7().as_simple()),
        )
        .create()
        .map_err(io::Error::other)?;
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .map_err(io::Error::other)?;
    let Some(entry) = metadata.topics().iter().find(|entry| entry.name() == topic) else {
        return Ok(None);
    };
    let partitions = entry.partitions().len();
    if partitions == 0 {
        Ok(None)
    } else {
        Ok(Some(partitions))
    }
}

async fn wait_for_kafka_topic_partitions(
    dependencies: &DependencyEndpoints,
    topic: &str,
    expected: usize,
) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match kafka_topic_partition_count(dependencies, topic)? {
            Some(observed) if observed == expected => return Ok(()),
            _ if Instant::now() < deadline => sleep(POLL_INTERVAL).await,
            observed => {
                return Err(io::Error::other(format!(
                    "timed out waiting for kafka topic '{topic}' to reach {expected} partitions, \
                     observed {observed:?}"
                )));
            }
        }
    }
}

async fn wait_for_kafka_topic_absent(
    dependencies: &DependencyEndpoints,
    topic: &str,
) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match kafka_topic_partition_count(dependencies, topic)? {
            None => return Ok(()),
            _ if Instant::now() < deadline => sleep(POLL_INTERVAL).await,
            observed => {
                return Err(io::Error::other(format!(
                    "timed out waiting for kafka topic '{topic}' to disappear, observed \
                     {observed:?}"
                )));
            }
        }
    }
}

async fn ensure_kafka_topic_partitions(
    dependencies: &DependencyEndpoints,
    topic: &str,
    partitions: i32,
) -> io::Result<()> {
    if partitions <= 0 {
        return Err(io::Error::other(format!(
            "kafka topic '{topic}' must have at least one partition"
        )));
    }

    let admin = kafka_admin_client(dependencies)?;
    let created = admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .map_err(io::Error::other)?;
    let mut created_new = false;
    for result in created {
        match result {
            Ok(_) => created_new = true,
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic_name, code)) => {
                return Err(io::Error::other(format!(
                    "failed to create kafka topic '{topic_name}': {code:?}"
                )));
            }
        }
    }

    let expected = usize::try_from(partitions)
        .verified("the partition count was checked to be positive above");
    if created_new {
        return wait_for_kafka_topic_partitions(dependencies, topic, expected).await;
    }
    let current = kafka_topic_partition_count(dependencies, topic)?.unwrap_or(0);
    if current > expected {
        return Err(io::Error::other(format!(
            "kafka topic '{topic}' already has {current} partitions, cannot shrink to {expected}"
        )));
    }
    if current < expected {
        let expanded = admin
            .create_partitions(&[NewPartitions::new(topic, expected)], &AdminOptions::new())
            .await
            .map_err(io::Error::other)?;
        for result in expanded {
            match result {
                Ok(_) => {}
                Err((topic_name, code)) => {
                    return Err(io::Error::other(format!(
                        "failed to expand kafka topic '{topic_name}' to {expected} partitions: \
                         {code:?}"
                    )));
                }
            }
        }
    }

    wait_for_kafka_topic_partitions(dependencies, topic, expected).await
}

async fn reset_kafka_topic_partitions(
    dependencies: &DependencyEndpoints,
    topic: &str,
    partitions: i32,
) -> io::Result<()> {
    if partitions <= 0 {
        return Err(io::Error::other(format!(
            "kafka topic '{topic}' must have at least one partition"
        )));
    }

    let admin = kafka_admin_client(dependencies)?;
    let deleted = admin
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .map_err(io::Error::other)?;
    for result in deleted {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::UnknownTopicOrPartition)) => {}
            Err((topic_name, code)) => {
                return Err(io::Error::other(format!(
                    "failed to delete kafka topic '{topic_name}': {code:?}"
                )));
            }
        }
    }
    wait_for_kafka_topic_absent(dependencies, topic).await?;
    ensure_kafka_topic_partitions(dependencies, topic, partitions).await
}

fn kafka_consumer_group_member_count(
    dependencies: &DependencyEndpoints,
    group: &str,
) -> io::Result<usize> {
    let mut client_config = kafka_client_config(dependencies)?;
    let consumer: BaseConsumer = client_config
        .set(
            "group.id",
            format!("nervix-cucumber-admin-{}", Uuid::now_v7().as_simple()),
        )
        .create()
        .map_err(io::Error::other)?;
    let group_list = consumer
        .fetch_group_list(Some(group), Duration::from_secs(5))
        .map_err(io::Error::other)?;
    let info = group_list
        .groups()
        .iter()
        .find(|info| info.name() == group)
        .ok_or_else(|| io::Error::other(format!("kafka consumer group '{group}' not found")))?;
    Ok(info.members().len())
}

fn kafka_consumer_group_next_offset(
    dependencies: &DependencyEndpoints,
    group: &str,
    topic: &str,
    partition: i32,
) -> io::Result<Option<i64>> {
    let mut client_config = kafka_client_config(dependencies)?;
    let consumer: BaseConsumer = client_config
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .map_err(io::Error::other)?;
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, partition);
    let committed = consumer
        .committed_offsets(partitions, Duration::from_secs(1))
        .map_err(io::Error::other)?;
    let Some(element) = committed.find_partition(topic, partition) else {
        return Ok(None);
    };
    match element.offset() {
        Offset::Offset(offset) => Ok(Some(offset)),
        Offset::Beginning
        | Offset::End
        | Offset::Stored
        | Offset::Invalid
        | Offset::OffsetTail(_) => Ok(None),
    }
}

async fn ensure_sqs_queue(dependencies: &DependencyEndpoints, queue: &str) -> io::Result<()> {
    let client = sqs_client(dependencies).await?;
    create_sqs_queue(&client, queue).await
}

async fn ensure_sqs_queue_tls(dependencies: &DependencyEndpoints, queue: &str) -> io::Result<()> {
    let client = sqs_tls_client(dependencies).await?;
    create_sqs_queue(&client, queue).await
}

async fn create_sqs_queue(client: &SqsClient, queue: &str) -> io::Result<()> {
    let mut request = client.create_queue().queue_name(queue);
    if queue.ends_with(".fifo") {
        request = request
            .attributes(QueueAttributeName::FifoQueue, "true")
            .attributes(QueueAttributeName::ContentBasedDeduplication, "true");
    }
    request.send().await.map_err(io::Error::other)?;
    Ok(())
}

async fn publish_sqs(
    dependencies: &DependencyEndpoints,
    queue: &str,
    payload: &str,
) -> io::Result<()> {
    let client = sqs_client(dependencies).await?;
    let queue_url = sqs_queue_url(&client, queue).await?;
    client
        .send_message()
        .queue_url(queue_url)
        .message_body(payload)
        .send()
        .await
        .map_err(io::Error::other)?;
    sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn publish_sqs_tls(
    dependencies: &DependencyEndpoints,
    queue: &str,
    payload: &str,
) -> io::Result<()> {
    let client = sqs_tls_client(dependencies).await?;
    let queue_url = sqs_queue_url(&client, queue).await?;
    client
        .send_message()
        .queue_url(queue_url)
        .message_body(payload)
        .send()
        .await
        .map_err(io::Error::other)?;
    sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn publish_nats(
    dependencies: &DependencyEndpoints,
    subject: &str,
    payload: &str,
) -> io::Result<()> {
    let client = nats_client(dependencies).await?;
    for attempt in 0..2 {
        client
            .publish(subject.to_string(), payload.as_bytes().to_vec().into())
            .await
            .map_err(io::Error::other)?;
        client.flush().await.map_err(io::Error::other)?;
        if attempt == 0 {
            sleep(POLL_INTERVAL).await;
        }
    }
    sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn publish_nats_payloads(
    dependencies: &DependencyEndpoints,
    subject: &str,
    payloads: &[String],
) -> io::Result<()> {
    let client = nats_client(dependencies).await?;
    let subject = async_nats::Subject::from(subject.to_string());
    for payload in payloads {
        tokio::task::consume_budget().await;
        client
            .publish(subject.clone(), payload.as_bytes().to_vec().into())
            .await
            .map_err(io::Error::other)?;
    }
    client.flush().await.map_err(io::Error::other)
}

async fn publish_nats_with_headers(
    dependencies: &DependencyEndpoints,
    subject: &str,
    payload: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    let client = nats_client(dependencies).await?;
    let mut header_map = async_nats::HeaderMap::new();
    for (name, value) in headers {
        header_map.append(*name, *value);
    }
    client
        .publish_with_headers(
            subject.to_string(),
            header_map,
            payload.as_bytes().to_vec().into(),
        )
        .await
        .map_err(io::Error::other)?;
    client.flush().await.map_err(io::Error::other)?;
    Ok(())
}

async fn publish_nats_tls(
    dependencies: &DependencyEndpoints,
    subject: &str,
    payload: &str,
) -> io::Result<()> {
    let client = nats_tls_client(dependencies).await?;
    for attempt in 0..2 {
        client
            .publish(subject.to_string(), payload.as_bytes().to_vec().into())
            .await
            .map_err(io::Error::other)?;
        client.flush().await.map_err(io::Error::other)?;
        if attempt == 0 {
            sleep(POLL_INTERVAL).await;
        }
    }
    sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn publish_zeromq(addr: &str, payload: &str) -> io::Result<()> {
    let mut socket = PushSocket::new();
    socket.connect(addr).await.map_err(io::Error::other)?;
    sleep(POLL_INTERVAL).await;
    socket
        .send(payload.as_bytes().to_vec().into())
        .await
        .map_err(io::Error::other)?;
    sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn observe_mqtt(
    dependencies: &DependencyEndpoints,
    topic: &str,
) -> io::Result<BrokerObserver> {
    let client_id = format!(
        "nervix-cucumber-observer-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock must be after unix epoch")
            .as_nanos()
    );
    let (host, port) = dependency_host_port(dependencies, MQTT_ADDR, 1883)?;
    let mut options = MqttOptions::new(client_id, (host, port));
    options.set_session_mode(SessionMode::Clean);
    let (client, mut eventloop) = AsyncClient::builder(options).capacity(16).build();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (payload_tx, payload_rx) = mpsc::channel(16);
    let topic = topic.to_string();

    client
        .subscribe(topic.as_str(), QoS::AtMostOnce)
        .await
        .map_err(io::Error::other)?;

    let task = tokio::spawn(async move {
        let _client = client;
        let mut ready_tx = Some(ready_tx);

        loop {
            match eventloop.poll().await {
                Ok(MqttEvent::Incoming(Incoming::SubAck(_))) => {
                    if let Some(ready_tx) = ready_tx.take() {
                        let _ = ready_tx.send(());
                    }
                }
                Ok(MqttEvent::Incoming(Incoming::Publish(publish))) => {
                    if publish.topic.as_ref() == topic.as_bytes() {
                        let payload = String::from_utf8_lossy(publish.payload.as_ref()).to_string();
                        let _ = payload_tx.send(BrokerMessage::payload(payload)).await;
                        break;
                    }
                }
                Ok(MqttEvent::Incoming(_))
                | Ok(MqttEvent::Outgoing(_))
                | Ok(MqttEvent::Auth(_)) => {}
                Err(_) => break,
            }
        }
    });

    timeout(BROKER_TIMEOUT, ready_rx)
        .await
        .map_err(|_| io::Error::other("timed out waiting for mqtt observer subscription"))?
        .map_err(io::Error::other)?;

    Ok(BrokerObserver {
        payload_rx,
        task: Some(task),
    })
}

async fn observe_rabbitmq(
    dependencies: &DependencyEndpoints,
    queue: &str,
) -> io::Result<BrokerObserver> {
    let connection = Connection::connect(
        dependencies.get(RABBITMQ_ADDR)?,
        ConnectionProperties::default(),
    )
    .await
    .map_err(io::Error::other)?;
    let channel = connection
        .create_channel()
        .await
        .map_err(io::Error::other)?;
    declare_rabbitmq_queue(&channel, queue).await?;
    let mut consumer = channel
        .basic_consume(
            queue.into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(io::Error::other)?;
    let (payload_tx, payload_rx) = mpsc::channel(16);

    let task = tokio::spawn(async move {
        let _connection = connection;
        if let Some(delivery) = consumer.next().await
            && let Ok(delivery) = delivery
        {
            let payload = String::from_utf8_lossy(&delivery.data).to_string();
            let _ = delivery.ack(BasicAckOptions::default()).await;
            let _ = payload_tx.send(BrokerMessage::payload(payload)).await;
        }
    });

    Ok(BrokerObserver {
        payload_rx,
        task: Some(task),
    })
}

async fn observe_redis(
    dependencies: &DependencyEndpoints,
    channel: &str,
) -> io::Result<BrokerObserver> {
    let client = redis::Client::open(dependencies.get(REDIS_ADDR)?).map_err(io::Error::other)?;
    let mut pubsub = client.get_async_pubsub().await.map_err(io::Error::other)?;
    pubsub.subscribe(channel).await.map_err(io::Error::other)?;
    let (payload_tx, payload_rx) = mpsc::channel(16);

    let task = tokio::spawn(async move {
        let mut messages = pubsub.on_message();
        while let Some(message) = messages.next().await {
            tokio::task::consume_budget().await;
            let payload = String::from_utf8_lossy(message.get_payload_bytes()).to_string();
            if payload_tx
                .send(BrokerMessage::payload(payload))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    sleep(POLL_INTERVAL).await;
    Ok(BrokerObserver {
        payload_rx,
        task: Some(task),
    })
}

async fn observe_kafka(
    dependencies: &DependencyEndpoints,
    topic: &str,
) -> io::Result<BrokerObserver> {
    let consumer_group = format!(
        "nervix-cucumber-observer-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock must be after unix epoch")
            .as_nanos()
    );
    let mut client_config = kafka_client_config(dependencies)?;
    let consumer: StreamConsumer = client_config
        .set("group.id", &consumer_group)
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(io::Error::other)?;
    consumer.subscribe(&[topic]).map_err(io::Error::other)?;
    let (payload_tx, payload_rx) = mpsc::channel(16);

    let task = tokio::spawn(async move {
        let mut messages = consumer.stream();
        while let Some(message) = messages.next().await {
            tokio::task::consume_budget().await;
            match message {
                Ok(message) => {
                    let bytes = message.payload().unwrap_or_default();
                    let headers = message
                        .headers()
                        .map(|headers| {
                            let mut values = Vec::new();
                            for index in 0..headers.count() {
                                let Some(header) = headers.try_get(index) else {
                                    continue;
                                };
                                values.push((
                                    header.key.to_string(),
                                    header
                                        .value
                                        .map(|value| String::from_utf8_lossy(value).to_string())
                                        .unwrap_or_default(),
                                ));
                            }
                            values
                        })
                        .unwrap_or_default();
                    let _ = payload_tx
                        .send(BrokerMessage::from_bytes(bytes, headers))
                        .await;
                }
                Err(_) => continue,
            }
        }
    });

    Ok(BrokerObserver {
        payload_rx,
        task: Some(task),
    })
}

async fn observe_pulsar(
    dependencies: &DependencyEndpoints,
    topic: &str,
) -> io::Result<BrokerObserver> {
    observe_pulsar_with_addr(dependencies.get(PULSAR_ADDR)?, None, topic).await
}

async fn observe_pulsar_tls(
    dependencies: &DependencyEndpoints,
    topic: &str,
) -> io::Result<BrokerObserver> {
    observe_pulsar_with_addr(
        dependencies.get(PULSAR_TLS_ADDR)?,
        Some(dependencies.tls_ca_pem()?),
        topic,
    )
    .await
}

async fn observe_pulsar_with_addr(
    addr: &str,
    ca_certificate_chain: Option<Vec<u8>>,
    topic: &str,
) -> io::Result<BrokerObserver> {
    let subscription = format!(
        "nervix-cucumber-observer-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock must be after unix epoch")
            .as_nanos()
    );
    let topic = pulsar_topic(topic);
    let mut last_error = None;

    for attempt in 0..5 {
        let result: io::Result<BrokerObserver> = async {
            let mut builder = Pulsar::builder(addr, TokioExecutor);
            if let Some(ca_certificate_chain) = ca_certificate_chain.clone() {
                builder = builder.with_certificate_chain(ca_certificate_chain);
            }
            let pulsar: Pulsar<_> = builder.build().await.map_err(io::Error::other)?;
            let mut consumer = pulsar
                .consumer()
                .with_topic(&topic)
                .with_subscription(&subscription)
                .with_subscription_type(PulsarSubType::Exclusive)
                .with_options(
                    PulsarConsumerOptions::default()
                        .with_initial_position(PulsarInitialPosition::Earliest),
                )
                .build::<Vec<u8>>()
                .await
                .map_err(io::Error::other)?;
            let (payload_tx, payload_rx) = mpsc::channel(16);

            let task = tokio::spawn(async move {
                while let Some(message) = consumer.next().await {
                    tokio::task::consume_budget().await;
                    match message {
                        Ok(message) => {
                            let payload = message.payload.data.to_vec();
                            let payload = String::from_utf8_lossy(&payload).to_string();
                            let _ = consumer.ack(&message).await;
                            let _ = payload_tx.send(BrokerMessage::payload(payload)).await;
                            break;
                        }
                        Err(_) => continue,
                    }
                }
            });

            Ok(BrokerObserver {
                payload_rx,
                task: Some(task),
            })
        }
        .await;

        match result {
            Ok(observer) => return Ok(observer),
            Err(error) => {
                last_error = Some(io::Error::new(
                    error.kind(),
                    format!("failed to observe pulsar topic '{topic}': {error}"),
                ));
                if attempt < 4 {
                    sleep(POLL_INTERVAL).await;
                }
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| io::Error::other(format!("failed to observe pulsar topic '{topic}'"))))
}

async fn observe_sqs(
    dependencies: &DependencyEndpoints,
    queue: &str,
) -> io::Result<BrokerObserver> {
    ensure_sqs_queue(dependencies, queue).await?;
    let client = sqs_client(dependencies).await?;
    let queue_url = sqs_queue_url(&client, queue).await?;
    let (payload_tx, payload_rx) = mpsc::channel(16);

    let task = tokio::spawn(async move {
        while let Ok(response) = client
            .receive_message()
            .queue_url(queue_url.clone())
            .max_number_of_messages(1)
            .wait_time_seconds(1)
            .send()
            .await
        {
            tokio::task::consume_budget().await;
            let Some(message) = response.messages().first() else {
                continue;
            };
            let payload = message.body().unwrap_or_default().to_string();
            if let Some(receipt_handle) = message.receipt_handle() {
                let _ = client
                    .delete_message()
                    .queue_url(queue_url.clone())
                    .receipt_handle(receipt_handle)
                    .send()
                    .await;
            }
            if payload_tx
                .send(BrokerMessage::payload(payload))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    Ok(BrokerObserver {
        payload_rx,
        task: Some(task),
    })
}

async fn observe_nats(
    dependencies: &DependencyEndpoints,
    subject: &str,
) -> io::Result<BrokerObserver> {
    let client = nats_client(dependencies).await?;
    let mut subscriber = client
        .subscribe(subject.to_string())
        .await
        .map_err(io::Error::other)?;
    let (payload_tx, payload_rx) = mpsc::channel(1);

    let task = tokio::spawn(async move {
        while let Some(message) = subscriber.next().await {
            tokio::task::consume_budget().await;
            let bytes = message.payload.as_ref();
            let headers = message
                .headers
                .as_ref()
                .map(|headers| {
                    headers
                        .iter()
                        .flat_map(|(name, values)| {
                            values
                                .iter()
                                .map(|value| (name.to_string(), value.as_str().to_string()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            if payload_tx
                .send(BrokerMessage::from_bytes(bytes, headers))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    Ok(BrokerObserver {
        payload_rx,
        task: Some(task),
    })
}

async fn observe_zeromq(addr: &str) -> io::Result<BrokerObserver> {
    let mut socket = PullSocket::new();
    socket.bind(addr).await.map_err(io::Error::other)?;
    let (payload_tx, payload_rx) = mpsc::channel(1);

    let task = tokio::spawn(async move {
        while let Ok(message) = socket.recv().await {
            tokio::task::consume_budget().await;
            let frames = message.into_vec();
            if let Some(frame) = frames.first() {
                let payload = String::from_utf8_lossy(frame).to_string();
                if payload_tx
                    .send(BrokerMessage::payload(payload))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });

    sleep(POLL_INTERVAL).await;
    Ok(BrokerObserver {
        payload_rx,
        task: Some(task),
    })
}

async fn nats_client(dependencies: &DependencyEndpoints) -> io::Result<NatsClient> {
    async_nats::connect(dependencies.get(NATS_ADDR)?)
        .await
        .map_err(io::Error::other)
}

async fn provision_nats_stream(
    dependencies: &DependencyEndpoints,
    stream: &str,
    subject: &str,
) -> io::Result<()> {
    let client = nats_client(dependencies).await?;
    async_nats::jetstream::new(client)
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream.to_string(),
            subjects: vec![subject.to_string()],
            storage: async_nats::jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .map(|_| ())
        .map_err(io::Error::other)
}

async fn wait_for_nats_stream_payload(
    dependencies: &DependencyEndpoints,
    stream: &str,
    subject: &str,
    expected: &str,
) -> io::Result<()> {
    let client = nats_client(dependencies).await?;
    let stream_handle = async_nats::jetstream::new(client)
        .get_stream_no_info(stream)
        .await
        .map_err(io::Error::other)?;
    let deadline = Instant::now() + BROKER_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        let last_observation = match stream_handle.get_last_raw_message_by_subject(subject).await {
            Ok(message) => {
                let payload = String::from_utf8_lossy(&message.payload).to_string();
                if payload.contains(expected.trim()) {
                    return Ok(());
                }
                format!("payload {payload:?}")
            }
            Err(error) => format!("error {error}"),
        };
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for NATS JetStream stream '{stream}' subject '{subject}' to \
                 contain {expected:?}; last observation: {last_observation}"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn nats_tls_client(dependencies: &DependencyEndpoints) -> io::Result<NatsClient> {
    let ca_path = dependencies.tls_ca_path()?;
    async_nats::ConnectOptions::new()
        .add_root_certificates(ca_path.to_path_buf())
        .require_tls(true)
        .connect(dependencies.get(NATS_TLS_ADDR)?)
        .await
        .map_err(io::Error::other)
}

async fn sqs_client(dependencies: &DependencyEndpoints) -> io::Result<SqsClient> {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_sdk_sqs::config::Region::new(SQS_REGION))
        .endpoint_url(dependencies.get(SQS_ENDPOINT)?)
        .credentials_provider(Credentials::new("x", "x", None, None, "nervix-cucumber"))
        .load()
        .await;
    Ok(SqsClient::new(&sdk_config))
}

async fn sqs_tls_client(dependencies: &DependencyEndpoints) -> io::Result<SqsClient> {
    let ca_pem = dependencies.tls_ca_pem()?;
    let tls_context = aws_smithy_http_client::tls::TlsContext::builder()
        .with_trust_store(
            aws_smithy_http_client::tls::TrustStore::empty().with_pem_certificate(ca_pem),
        )
        .build()
        .map_err(io::Error::other)?;
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
        ))
        .tls_context(tls_context)
        .build_https();
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_sdk_sqs::config::Region::new(SQS_REGION))
        .endpoint_url(dependencies.get(SQS_TLS_ENDPOINT)?)
        .http_client(http_client)
        .credentials_provider(Credentials::new("x", "x", None, None, "nervix-cucumber"))
        .load()
        .await;
    Ok(SqsClient::new(&sdk_config))
}

async fn sqs_queue_url(client: &SqsClient, queue: &str) -> io::Result<String> {
    client
        .get_queue_url()
        .queue_name(queue)
        .send()
        .await
        .map_err(io::Error::other)?
        .queue_url()
        .map(ToOwned::to_owned)
        .ok_or_else(|| io::Error::other(format!("queue '{queue}' has no URL")))
}
