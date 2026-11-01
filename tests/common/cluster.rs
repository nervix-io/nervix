mod byte_pace;
mod paced_interconnect;
mod snapshot_receive;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    time::{Duration, SystemTime},
};

use arch_into::ArchInto as _;
use async_nats::Client as NatsClient;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::{
    Client as SqsClient,
    types::{MessageSystemAttributeName, QueueAttributeName},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use error_stack::Report;
use fjall::Database;
use futures_util::{SinkExt, future::join_all};
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
    },
    types::FieldTable,
};
use meticulous::{OptionExt as _, ResultExt as _};
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
use nervix_consensus::{RaftRetentionSettings, RaftTimings};
use nervix_dns::{DnsConfiguration, DnsResolver};
use nervix_execution::Executor;
use nervix_interconnect::{
    BootstrapExpectation, ControlEnvelope, Envelope, PeerResolver, PeerTarget, RuntimeErrorEvent,
    TlsConfigBundle, Transport, TransportClock, TransportIdentity, TransportOptions,
};
use nervix_models::{ClusterNodeName, NodeEndpoint, TrustRootFingerprint};
use nervix_plexus::client_statement::{ClientStatement, parse_client_statement_sources};
use nervix_primitives::{
    runtime::Handle,
    sync::{
        StdArc,
        blocking::{LazyLock, OnceLock},
    },
    time::Instant,
};
use nervix_test_environment::workspace_root;
use paced_interconnect::PacedInterconnect;

/// Cucumber node ids are fixed strings from the feature files, so they always parse.
pub(crate) fn node_name(raw: &str) -> ClusterNodeName {
    ClusterNodeName::parse(raw).expect("cucumber node ids are valid cluster node names")
}
use nervix_primitives::{
    net::{TcpListener as TokioTcpListener, TcpStream},
    stream::StreamExt,
    sync::{Arc, CancellationToken, blocking::Mutex, mpsc, oneshot, watch},
    task::{AbortOnDropHandle, JoinHandle},
    time::{sleep, timeout},
};
use nervix_server::{
    FaultInjection, SchedulerMode,
    application::{
        Application, CommandExecutionPolicy, InternalTransportMode, NodeIdentity, NodeSettings,
        SchedulingSettings, SettingOverride, ShutdownCoordinator, ShutdownOutcome,
        ShutdownSettings, TransactionBounds, TransactionLimits, init_tracing_to_file,
    },
    cluster::{MembershipSettings, MembershipValues},
    memory_pressure::{MemoryPressureConfig, MemoryPressureSettings},
    runtime::{
        DEFAULT_DOMAIN_DRAIN_TIMEOUT, DEFAULT_TEMP_DIR, RuntimeSettings, branch_task_stop_timeout,
    },
};
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
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async, connect_async,
    tungstenite::{Message as WsMessage, client::IntoClientRequest, http::HeaderValue},
};
use uuid::Uuid;
use zeromq::{PullSocket, PushSocket, Socket, SocketRecv, SocketSend};

use super::{
    broker_observer::BROKER_TIMEOUT,
    cluster_teardown::{CLUSTER_TEARDOWN_BUDGET, ClusterTeardown, TeardownNode},
    command_recovery::LeaderRoutes,
    dependencies::{
        DependencyEndpoints, KAFKA_ADDR, MQTT_ADDR, NATS_ADDR, NATS_TLS_ADDR, PULSAR_ADDR,
        PULSAR_ADMIN_ADDR, PULSAR_TLS_ADDR, RABBITMQ_ADDR, REDIS_ADDR, SQS_ENDPOINT,
        SQS_TLS_ENDPOINT,
    },
    kafka_group_member::ExternalKafkaGroupMember,
    nats_observer::NatsObserver,
    node_liveness::{
        NodeStartupError, NodeTaskState, NodeTaskTerminalOutcome, NodeTaskWaitOutcome,
        OwnedNodeTask, ReadinessProbeOutcome,
    },
    node_startup::{
        ATTEMPT_READINESS_BUDGET, AttemptCleanup, NODE_STARTUP_BUDGET, NodeStartup, StartableNode,
        StartupPorts, StartupRetryPolicy, cluster_startup_budget,
    },
    node_stop::{NodeStopClock, NodeStopEnding, NodeStopProgress, NodeStopRecord},
    peer_addressing::{
        ClusterDns, FixtureAnswer, InterconnectAddress, PeerAddressing, PublishedNode,
        qualified_name,
    },
    phase_deadline::{BeforeDeadline, PhaseDeadline},
    port_pool::{next_ports, release_test_ports},
    raw_session::{TestUpload, send_upload},
    redis_client::{TestRedisClient, publish_burst},
    scenario_phase::ScenarioIdentity,
    status_request::{
        STATUS_DIAGNOSTIC_BUDGET, STATUS_REQUEST_TIMEOUT, STATUS_WAIT_BUDGET, StatusEndpoint,
        StatusRequestError, StatusTransport,
    },
    suite_watchdog::{LiveClusterRegistration, NodeStop, SUITE_BUDGET},
};
pub(crate) use super::{
    broker_observer::{BrokerMessage, BrokerObserver, TypedBrokerHeader},
    raw_session::{TestSession, open_raw_session},
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
/// How long provisioning a Pulsar subscription repeats a request the broker refuses as a
/// conflict while an ingestor's check holds a non-durable cursor of the same name.
const PULSAR_PROVISION_TIMEOUT: Duration = Duration::from_secs(30);
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
/// A sixth of a second, so the election timeouts consensus derives from it stay just inside the one
/// and two seconds scenarios size their leader waits for.
const TEST_RAFT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(166);
/// The cluster every test node claims unless a scenario names another for a node that joins.
const TEST_CLUSTER_ID: &str = "cucumber";
const TEST_REPLICA_COUNT: usize = 0;
const TEST_STATE_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const TEST_AUTH_USERNAME: &str = "default";
pub(crate) const TEST_AUTH_PASSWORD: &str = "nervix-test-password";
static DEV_TLS_READY: OnceLock<io::Result<()>> = OnceLock::new();
static TEST_LOG_TRUNCATED: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

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
    nervix_primitives::task::spawn_blocking(move || {
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
    mut url: watch::Receiver<String>,
    metric_name: String,
    label_fragments: Vec<String>,
    cancellation: nervix_primitives::sync::CancellationToken,
) -> f64 {
    let client = reqwest::Client::new();
    let mut peak = 0.0_f64;
    loop {
        nervix_primitives::task::consume_budget().await;
        let current_url = url.borrow_and_update().clone();
        if let Ok(response) = client.get(&current_url).send().await
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
        nervix_primitives::select! {
            () = cancellation.cancelled() => return peak,
            changed = url.changed() => {
                if changed.is_err() {
                    return peak;
                }
            }
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
                .current_dir(workspace_root())
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
    workspace_root().join("tls/dev").join("ca.pem")
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

/// The slowest healthy `UPLOAD RESOURCE` of a 48 MiB archive measured beside other large uploads.
/// With 48 of them on one shared, disk-backed host, those that completed did so just inside the
/// client's 120-second window and 35 others did not; on an idle host 72 of them took at most 52
/// seconds.
const SLOWEST_HEALTHY_UPLOAD: Duration = Duration::from_secs(120);
/// A policy input: an upload's pace follows the storage and CPU the rest of the suite leaves it,
/// and both transfers of its archive slow down together.
const UPLOAD_WAIT_HEADROOM_FACTOR: u32 = 5;
/// How long a native client the harness runs an `UPLOAD RESOURCE` through waits for the upload,
/// every attempt and redirect included. The leader answers an upload only once every live node has
/// fetched, verified and installed its archive, so the wait is two transfers of the archive at the
/// pace the suite's load allows, which the client's default request and retry windows do not
/// always cover.
pub(crate) const UPLOAD_COMMAND_WAIT: Duration =
    match SLOWEST_HEALTHY_UPLOAD.checked_mul(UPLOAD_WAIT_HEADROOM_FACTOR) {
        Some(wait) => wait,
        None => panic!("the upload command wait must fit a duration"),
    };
const _: () = assert!(
    UPLOAD_COMMAND_WAIT.as_nanos() < SUITE_BUDGET.as_nanos(),
    "an upload the harness waits for must end inside the suite budget"
);

/// The options of a native client the harness runs `commands` through. A client that runs an
/// `UPLOAD RESOURCE` waits [`UPLOAD_COMMAND_WAIT`] for its requests and their retries; every other
/// client keeps the client's own defaults.
pub(crate) fn command_connect_options(server: &str, commands: &str) -> io::Result<ConnectOptions> {
    let mut options = client_connect_options(server)?;
    if uploads_resource(commands) {
        options.request_timeout = UPLOAD_COMMAND_WAIT;
        options.retry_timeout = UPLOAD_COMMAND_WAIT;
    }
    Ok(options)
}

/// Whether `commands` holds an `UPLOAD RESOURCE`. Commands that do not parse upload nothing, and
/// the client reports them as it would any other.
fn uploads_resource(commands: &str) -> bool {
    let Ok(statements) = parse_client_statement_sources(commands) else {
        return false;
    };
    statements
        .iter()
        .any(|parsed| matches!(parsed.statement, ClientStatement::UploadResource(_)))
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
        Self::new_in(root.path())
    }

    /// A fresh authority whose certificate is written into `directory`.
    fn new_in(directory: &std::path::Path) -> io::Result<Self> {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().map_err(io::Error::other)?;
        let certificate = params.self_signed(&key).map_err(io::Error::other)?;
        let path = directory.join("interconnect-ca.pem");
        std::fs::write(&path, certificate.pem())?;
        Ok(Self {
            certificate,
            key,
            path,
        })
    }

    fn issue_node(
        &self,
        cluster_id: &str,
        node_id: &str,
        directory: &std::path::Path,
    ) -> io::Result<(PathBuf, PathBuf)> {
        self.issue_node_with_identity(
            cluster_id,
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

/// Which nodes of a test cluster start with the default user's initial password.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum DefaultUserPasswordSeed {
    /// Every node starts with it.
    #[default]
    EveryNode,
    /// Only the node that bootstraps the cluster starts with it, as in the documented Docker
    /// Compose deployment. The other nodes learn the default user from the replicated user records.
    BootstrapNodeOnly,
}

impl DefaultUserPasswordSeed {
    /// The initial password a node starts with, given whether it bootstraps the cluster.
    fn initial_password(self, bootstraps: bool) -> Option<String> {
        match self {
            Self::EveryNode => Some(TEST_AUTH_PASSWORD.to_string()),
            Self::BootstrapNodeOnly if bootstraps => Some(TEST_AUTH_PASSWORD.to_string()),
            Self::BootstrapNodeOnly => None,
        }
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

/// What a whole-cluster restart arms after every node has stopped and before any starts again.
#[derive(Debug, Clone, Copy)]
enum RestartArrangement<'a> {
    /// Every node starts again as it stopped.
    Unchanged,
    /// The bootstrap node holds the first heartbeat this voter relays and fails its health probes,
    /// so the scenario observes scheduling while the voter looks unavailable.
    RelayedVoter(&'a str),
    /// The first runtime installation this node begins waits until the scenario releases it.
    HeldRuntimeInstallation(&'a str),
    /// This node runs and gossips as soon as it starts, but the restart counts it ready only once
    /// the restarted cluster's leader has ended its startup voter observation, as a loaded host can
    /// keep a live node from reporting ready.
    ReadyAfterStartupObservation(&'a str),
}

impl<'a> RestartArrangement<'a> {
    /// The voter whose relayed heartbeat the restart holds, which the cluster cannot wait to catch
    /// up until the scenario releases it.
    fn relayed_voter(self) -> Option<&'a str> {
        match self {
            Self::RelayedVoter(voter) => Some(voter),
            Self::Unchanged
            | Self::HeldRuntimeInstallation(_)
            | Self::ReadyAfterStartupObservation(_) => None,
        }
    }

    /// The node the restart counts ready only once the leader's startup voter observation ended.
    fn ready_after_startup_observation(self) -> Option<&'a str> {
        match self {
            Self::ReadyAfterStartupObservation(node) => Some(node),
            Self::Unchanged | Self::RelayedVoter(_) | Self::HeldRuntimeInstallation(_) => None,
        }
    }
}

/// What every node of one cluster uses for as long as it runs. The cluster and each of its nodes
/// share it, so a node that is still stopping when its cluster is dropped keeps its storage and
/// stays published until its own task has ended.
#[derive(Debug)]
struct ClusterRoots {
    /// The temporary storage every node's databases and certificates live in.
    storage: TempDir,
    /// Publishes this cluster to the suite watchdog for as long as the scenario holds it or one of
    /// its nodes still runs, so a suite timeout can name its nodes and ask every one of them to
    /// stop.
    live: LiveClusterRegistration,
}

#[derive(Debug)]
pub(crate) struct Cluster {
    roots: Arc<ClusterRoots>,
    interconnect_ca: InterconnectTestCa,
    peer_addressing: PeerAddressing,
    /// The DNS a named cluster resolves peers through; a literally addressed cluster has none.
    dns: Option<ClusterDns>,
    nodes: BTreeMap<String, NodeHandle>,
    fault_injection: FaultInjection,
    dependencies: DependencyEndpoints,
    /// What every node's name begins with: the n-th node is `<prefix>-<n>`, and the first one
    /// bootstraps the cluster.
    node_name_prefix: String,
    /// What a scenario named for nodes that are not members yet, applied when each is registered.
    pending_members: BTreeMap<String, PendingMember>,
}

/// What a node that is not a member yet brings when it first starts, as a scenario named it.
#[derive(Debug, Default)]
struct PendingMember {
    /// The cluster it claims in its configuration and its certificate, when not the test cluster.
    cluster_id: Option<String>,
    /// Whether it trusts a second certificate authority beside the cluster's own.
    additional_trust_anchor: bool,
    /// The settings it starts with over the cluster's own.
    setting_overrides: Vec<SettingOverride>,
}

/// How a node's attempt to join a running cluster ended.
#[derive(Debug)]
pub(crate) enum JoinAttempt {
    /// The node became a voter of the membership it reports.
    Admitted,
    /// The node stopped before it was admitted; the text is how its start or its task ended.
    Stopped(String),
}

/// How a stopped member's return to a running cluster ended.
#[derive(Debug)]
pub(crate) enum RejoinAttempt {
    /// The node was still running when its startup budget ended.
    KeptRunning,
    /// The node stopped; the text is how its start or its task ended.
    Stopped(String),
}

/// Where a leadership move holds the next persistent command of the node it moves leadership away
/// from, until that node knows another node leads.
#[derive(Clone, Copy, Debug)]
pub(crate) enum CommandHold {
    /// After the command's execution reference is looked up and before its admission is proposed.
    /// The proposal then reaches a node that no longer leads, which cannot tell whether the
    /// cluster admitted the command.
    BeforeAdmissionProposal,
    /// After the command's applying record is committed and before its effect begins. The node
    /// that admitted the command then cannot know its outcome.
    AfterDurableAdmission,
}

impl CommandHold {
    /// Holds the next persistent command `node` handles at this point.
    fn arm(self, fault_injection: &FaultInjection, node: ClusterNodeName) {
        match self {
            Self::BeforeAdmissionProposal => {
                fault_injection.pause_command_reference_lookup_on(node);
            }
            Self::AfterDurableAdmission => {
                fault_injection.pause_command_after_durable_admission_on(node);
            }
        }
    }

    /// Waits until a command of `node` is held at this point.
    async fn reached(self, fault_injection: &FaultInjection, node: &ClusterNodeName) {
        match self {
            Self::BeforeAdmissionProposal => {
                fault_injection
                    .wait_for_command_reference_lookup_pause(node)
                    .await;
            }
            Self::AfterDurableAdmission => {
                fault_injection
                    .wait_for_command_durable_admission_pause(node)
                    .await;
            }
        }
    }

    /// Lets the command `node` holds at this point continue.
    fn release(self, fault_injection: &FaultInjection, node: &ClusterNodeName) {
        match self {
            Self::BeforeAdmissionProposal => {
                fault_injection.release_command_reference_lookup_pause(node);
            }
            Self::AfterDurableAdmission => {
                fault_injection.release_command_durable_admission_pause(node);
            }
        }
    }
}

/// A node whose own status a leadership move waits on before the command it holds goes on.
struct LeaderWitness {
    node_id: String,
    endpoint: StatusEndpoint,
}

impl LeaderWitness {
    /// Waits within `deadline` until the node reports `leader` as the leader, and otherwise names
    /// the status it last reported.
    async fn reports_leader(&self, deadline: PhaseDeadline, leader: &str) -> io::Result<()> {
        let reported = deadline
            .poll_until(
                POLL_INTERVAL,
                |phase| async move {
                    let status = self.endpoint.cluster_status(phase).await?;
                    Ok::<_, Report<StatusRequestError>>(ClusterStatus::parse(status))
                },
                |status| status.current_leader.as_deref() == Some(leader),
            )
            .await;
        match reported {
            Ok(_) => Ok(()),
            Err(expired) => Err(io::Error::other(format!(
                "node '{}' did not report '{leader}' as the leader within {:?}; last status: {:?}",
                self.node_id,
                deadline.elapsed(),
                expired.last_output.map(|status| status.raw)
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TestClusterConfig {
    pub replica_count: usize,
    pub scheduler_mode: Option<SchedulerMode>,
    pub state_snapshot_interval: Duration,
    /// The restore staging quota a scenario gives every node; without one each node derives it
    /// from its database volume.
    pub restore_staging_max_bytes: Option<u64>,
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
    pub raft_heartbeat_interval: Duration,
    pub raft_retention: RaftRetentionSettings,
    pub temp_dir: Option<PathBuf>,
    /// Settings a scenario names for one node, applied over the cluster's own when that node
    /// next starts.
    pub node_setting_overrides: BTreeMap<String, Vec<SettingOverride>>,
    pub dependencies: DependencyEndpoints,
    pub peer_addressing: PeerAddressing,
    /// What every node's name begins with: the n-th node is `<prefix>-<n>`. A scenario that runs a
    /// second cluster names its nodes apart from the first one's.
    pub node_name_prefix: String,
    pub default_user_password: DefaultUserPasswordSeed,
}

impl Default for TestClusterConfig {
    fn default() -> Self {
        Self {
            replica_count: TEST_REPLICA_COUNT,
            scheduler_mode: None,
            state_snapshot_interval: TEST_STATE_SNAPSHOT_INTERVAL,
            restore_staging_max_bytes: None,
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
            raft_heartbeat_interval: TEST_RAFT_HEARTBEAT_INTERVAL,
            raft_retention: RaftRetentionSettings::default(),
            temp_dir: None,
            node_setting_overrides: BTreeMap::new(),
            dependencies: DependencyEndpoints::default(),
            peer_addressing: PeerAddressing::default(),
            node_name_prefix: "node".to_string(),
            default_user_password: DefaultUserPasswordSeed::default(),
        }
    }
}

impl TestClusterConfig {
    /// The settings `node_id` starts with: the cluster's own, with the settings a scenario named
    /// for that node applied over them exactly as the server's command line reads them.
    fn node_settings(&self, node_id: &str) -> io::Result<NodeSettings> {
        let settings = NodeSettings {
            raft: RaftTimings::resolve(Some(self.raft_heartbeat_interval))
                .map_err(|report| io::Error::other(format!("{report:#}")))?,
            raft_retention: self.raft_retention,
            membership: MembershipSettings::resolve(MembershipValues {
                node_unavailability_timeout: Some(TEST_NODE_UNAVAILABILITY_TIMEOUT),
                ..MembershipValues::default()
            })
            .map_err(|report| io::Error::other(format!("{report:#}")))?,
            scheduling: SchedulingSettings::resolve(Some(self.replica_count))
                .map_err(|report| io::Error::other(format!("{report:#}")))?,
            runtime: RuntimeSettings::resolve(
                Some(self.state_snapshot_interval),
                self.restore_staging_max_bytes.map(ubyte::ByteUnit::Byte),
            )
            .map_err(|report| io::Error::other(format!("{report:#}")))?,
            transactions: TransactionLimits::resolve(TransactionBounds {
                idle_timeout: Some(self.transaction_idle_timeout),
                tombstone_retention: Some(self.transaction_tombstone_retention),
                max_statements: Some(self.transaction_max_statements),
                max_source_bytes: Some(self.transaction_max_source_bytes),
                max_open: Some(self.transaction_max_open),
            })
            .map_err(|report| io::Error::other(format!("{report:#}")))?,
            commands: CommandExecutionPolicy::resolve(
                Some(self.command_retry_validity),
                Some(self.command_execution_capacity),
            )
            .map_err(|report| io::Error::other(format!("{report:#}")))?,
            memory_pressure: self.memory_pressure_settings()?,
            shutdown: ShutdownSettings::resolve(
                Some(self.shutdown_timeout),
                Some(self.drain_timeout),
            )
            .map_err(|report| io::Error::other(format!("{report:#}")))?,
            ..NodeSettings::default()
        };
        let Some(overrides) = self.node_setting_overrides.get(node_id) else {
            return Ok(settings);
        };
        settings
            .with_overrides(overrides)
            .map_err(|report| io::Error::other(format!("{report:#}")))
    }

    fn memory_pressure_settings(&self) -> io::Result<MemoryPressureSettings> {
        let Some(config) = self.memory_pressure else {
            return Ok(MemoryPressureSettings::default());
        };
        MemoryPressureSettings::resolve(
            Some(config.high_watermark),
            Some(config.low_watermark),
            Some(config.check_interval),
            Some(config.resume_jitter),
        )
        .map_err(|report| io::Error::other(format!("{report:#}")))
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
        let storage = tempdir()?;
        let interconnect_ca = InterconnectTestCa::new(&storage)?;
        let roots = Arc::new(ClusterRoots {
            storage,
            live: LiveClusterRegistration::start(scenario),
        });
        /// One node of the cluster being built, before its spec exists.
        struct PlannedNode {
            node_id: String,
            index: u8,
            address: InterconnectAddress,
        }
        let peer_addressing = config.peer_addressing;
        let mut planned = Vec::with_capacity(node_count);
        for position in 1..=node_count {
            let node_id = format!("{}-{position}", config.node_name_prefix);
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
        let dns = ClusterDns::start(peer_addressing, roots.storage.path(), &published).await?;
        let mut nodes = BTreeMap::new();

        for PlannedNode {
            node_id,
            index,
            address,
        } in planned
        {
            let mut spec = NodeSpec::new(&roots.storage, &interconnect_ca, &node_id, index == 1)?;
            spec.set_interconnect_address(address);
            spec.dns = dns.as_ref().map(ClusterDns::configuration);
            fault_injection
                .set_syslog_ingestor_bind_ip(node_name(&node_id), spec.syslog_ingestor_host);
            nodes.insert(
                node_id.clone(),
                NodeHandle::new(spec, fault_injection.clone(), config.clone(), roots.clone()),
            );
        }

        let mut cluster = Self {
            roots,
            interconnect_ca,
            peer_addressing,
            dns,
            fault_injection,
            nodes,
            dependencies: config.dependencies,
            node_name_prefix: config.node_name_prefix,
            pending_members: BTreeMap::new(),
        };

        if let Err(error) = cluster.start_nodes_and_wait(node_count).await {
            // A construction that failed has no assertion left to serve, so the nodes it started
            // stop together under the one cleanup budget rather than one after another under the
            // product shutdown deadlines a scenario-driven stop observes.
            let deadline = PhaseDeadline::after(CLUSTER_TEARDOWN_BUDGET);
            let teardown = cluster.shutdown_for_teardown(deadline).await;
            let Some(unclean) = teardown.unclean() else {
                return Err(error);
            };
            return Err(io::Error::other(format!(
                "{error}; cleanup {teardown}: {unclean}"
            )));
        }

        Ok(cluster)
    }

    async fn start_nodes_and_wait(&mut self, node_count: usize) -> io::Result<()> {
        let nodes_to_start = u32::try_from(node_count)
            .assured("a test cluster is built from a handful of nodes, not billions");
        let construction = PhaseDeadline::after(cluster_startup_budget(nodes_to_start));
        let bootstrap = self.node_id(1);
        self.start_node_within(&bootstrap, construction).await?;
        let bootstrap_cluster_addr = self
            .nodes
            .get(&bootstrap)
            .expect("bootstrap node exists")
            .spec
            .interconnect_endpoint();
        for (node_id, node) in &mut self.nodes {
            if *node_id != bootstrap {
                node.spec.bootstrap_host = Some(bootstrap_cluster_addr.clone());
            }
        }
        for index in 2..=node_count {
            self.start_node_within(&self.node_id(index), construction)
                .await?;
        }
        self.wait_for_any_leader(&bootstrap).await?;
        if node_count > 1 {
            let expected_nodes = (1..=node_count)
                .map(|index| self.node_id(index))
                .collect::<Vec<_>>();
            for node_id in &expected_nodes {
                self.wait_for_any_leader(node_id).await?;
            }
            let voter_refs = expected_nodes
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            self.wait_for_voters(&bootstrap, &voter_refs).await?;
            self.wait_for_consistent_leader_on_all_nodes().await?;
            self.wait_for_full_interconnect(&expected_nodes).await?;
        }
        Ok(())
    }

    /// The name of the cluster's `position`-th node, counted from 1.
    fn node_id(&self, position: usize) -> String {
        format!("{}-{position}", self.node_name_prefix)
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
    /// its own condition. Only a lost early listener bind permits another launch, before runtime
    /// initialization could consume the failure it armed. Every launch, failed-task cleanup and
    /// pause shares one readiness attempt's budget; a readiness timeout is terminal here.
    pub(crate) async fn start_node_without_catching_up(&mut self, node_id: &str) -> io::Result<()> {
        let handle = self
            .nodes
            .get_mut(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let node = node_name(&handle.spec.node_id);
        NodeStartup::start(
            &node,
            handle,
            PhaseDeadline::after(ATTEMPT_READINESS_BUDGET),
            StartupRetryPolicy::BindBeforeRuntime,
        )
        .await
        .map_err(io::Error::other)
    }

    /// Names a setting `node_id` starts with from its next launch on, over the cluster's own. A
    /// node that is not a member yet starts with it when it is registered.
    pub(crate) fn override_node_setting(&mut self, node_id: &str, setting: SettingOverride) {
        let Some(handle) = self.nodes.get_mut(node_id) else {
            self.pending_member(node_id).setting_overrides.push(setting);
            return;
        };
        handle
            .config
            .node_setting_overrides
            .entry(node_id.to_string())
            .or_default()
            .push(setting);
    }

    /// Makes `node_id`, which is not a member yet, claim `cluster_id` in its configuration and in
    /// the certificate it presents.
    pub(crate) fn claim_cluster_for_new_node(&mut self, node_id: &str, cluster_id: &str) {
        self.pending_member(node_id).cluster_id = Some(cluster_id.to_string());
    }

    /// Makes `node_id`, which is not a member yet, trust a second certificate authority beside
    /// the cluster's own, so every certificate it meets still verifies while its trust root
    /// differs from every member's.
    pub(crate) fn trust_additional_authority_for_new_node(&mut self, node_id: &str) {
        self.pending_member(node_id).additional_trust_anchor = true;
    }

    fn pending_member(&mut self, node_id: &str) -> &mut PendingMember {
        assert!(
            !self.nodes.contains_key(node_id),
            "node '{node_id}' is already a member of the cluster"
        );
        self.pending_members.entry(node_id.to_string()).or_default()
    }

    /// The trust root `node_id` presents, read from its trust bundle the way its transport reads
    /// it.
    pub(crate) fn trust_root_of(&self, node_id: &str) -> io::Result<TrustRootFingerprint> {
        let handle = self
            .nodes
            .get(node_id)
            .ok_or_else(|| io::Error::other(format!("unknown node '{node_id}'")))?;
        let bundle = TlsConfigBundle::from_pem_files(
            &handle.spec.interconnect_tls_ca,
            &handle.spec.interconnect_tls_cert,
            &handle.spec.interconnect_tls_key,
            TransportClock::system(),
        )
        .map_err(|error| io::Error::other(format!("{error:#}")))?;
        Ok(bundle.trust_root())
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
        self.wait_for_node_raft_catch_up(node_id).await
    }

    /// Compare public applied positions after a startup fault has been released.
    pub(crate) async fn wait_for_node_raft_catch_up(&self, node_id: &str) -> io::Result<()> {
        let leader_lookup = PhaseDeadline::after(STATUS_WAIT_BUDGET);
        let mut leader_applied = None;
        for probe_node_id in self
            .nodes
            .keys()
            .filter(|existing_id| existing_id.as_str() != node_id)
        {
            nervix_primitives::task::consume_budget().await;
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
        NodeStartup::start(&node, handle, budget, StartupRetryPolicy::Transient)
            .await
            .map_err(io::Error::other)
    }

    pub(crate) async fn add_node(&mut self, node_id: &str) -> io::Result<()> {
        self.register_new_node(node_id)?;
        self.start_node(node_id).await?;

        let node_ids = self.node_ids();
        self.wait_for_any_leader(node_id).await?;
        let voter_refs = node_ids.iter().map(String::as_str).collect::<Vec<_>>();
        self.wait_for_voters(node_id, &voter_refs).await?;
        self.wait_for_consistent_leader_on_all_nodes().await?;
        self.wait_for_full_interconnect(&node_ids).await
    }

    /// Starts `node_id`, registering it as a new node that joins through a running member when
    /// the cluster has none by that name, and waits until the node is a voter of the membership
    /// it reports or its task has ended. A node that ended reports how, which is how a refused
    /// join reads.
    pub(crate) async fn attempt_join(&mut self, node_id: &str) -> io::Result<JoinAttempt> {
        if !self.nodes.contains_key(node_id) {
            self.register_new_node(node_id)?;
        }
        self.wait_for_join_decision(node_id).await
    }

    /// Starts `node_id` as a new node whose only bootstrap host is the running member `through`,
    /// and waits for the cluster's decision as [`Self::attempt_join`] does.
    pub(crate) async fn attempt_join_through(
        &mut self,
        node_id: &str,
        through: &str,
    ) -> io::Result<JoinAttempt> {
        let Some(member) = self.nodes.get(through) else {
            return Err(io::Error::other(format!("unknown node '{through}'")));
        };
        if !member.task.is_running() {
            return Err(io::Error::other(format!(
                "node '{through}' must be running to bootstrap another node"
            )));
        }
        let bootstrap_host = member.spec.interconnect_endpoint();
        self.register_new_node_through(node_id, bootstrap_host)?;
        self.wait_for_join_decision(node_id).await
    }

    /// Starts `node_id`, a stopped member that the stored membership already counts among the
    /// voters, and waits until its task ends or its whole startup budget has passed. Such a node
    /// is a voter from its first moment, so only a stop tells a refused return apart from one the
    /// cluster accepted.
    pub(crate) async fn attempt_rejoin(&mut self, node_id: &str) -> RejoinAttempt {
        if let Err(error) = self
            .start_node_without_waiting_for_raft_catch_up(node_id)
            .await
        {
            return RejoinAttempt::Stopped(format!("{error:#}"));
        }
        // The cluster judges the returning node once its gossip reaches the leader, and a refused
        // node stops through its whole shutdown, so the decision gets a node's startup budget.
        let deadline = PhaseDeadline::after(NODE_STARTUP_BUDGET);
        loop {
            nervix_primitives::task::consume_budget().await;
            let handle = self
                .nodes
                .get_mut(node_id)
                .verified("the node was registered before it was started");
            if let NodeTaskState::Terminal(outcome) = handle.task.inspect().await {
                return RejoinAttempt::Stopped(outcome.to_string());
            }
            if deadline.has_passed() {
                return RejoinAttempt::KeptRunning;
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    /// Starts a registered node and waits until it is a voter of the membership it reports or its
    /// task has ended.
    async fn wait_for_join_decision(&mut self, node_id: &str) -> io::Result<JoinAttempt> {
        if let Err(error) = self
            .start_node_without_waiting_for_raft_catch_up(node_id)
            .await
        {
            return Ok(JoinAttempt::Stopped(format!("{error:#}")));
        }
        // A refused node stops through its whole shutdown, and an admitted one waits for the
        // leader to see its record, so the decision gets a node's whole startup budget.
        let deadline = PhaseDeadline::after(NODE_STARTUP_BUDGET);
        loop {
            nervix_primitives::task::consume_budget().await;
            let handle = self
                .nodes
                .get_mut(node_id)
                .verified("the node was registered before it was started");
            if let NodeTaskState::Terminal(outcome) = handle.task.inspect().await {
                return Ok(JoinAttempt::Stopped(outcome.to_string()));
            }
            if let Ok(status) = handle.cluster_status(deadline).await
                && status.is_voter(node_id)
            {
                return Ok(JoinAttempt::Admitted);
            }
            if deadline.has_passed() {
                return Err(io::Error::other(format!(
                    "node '{node_id}' neither became a voter nor stopped within {:?}",
                    deadline.elapsed()
                )));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    /// Adds `node_id` to the nodes the harness owns, joining through a running member, with what
    /// a scenario named for it before it existed.
    fn register_new_node(&mut self, node_id: &str) -> io::Result<()> {
        let bootstrap_host = self
            .nodes
            .values()
            .find(|node| node.task.is_running())
            .map(|node| node.spec.interconnect_endpoint())
            .ok_or_else(|| io::Error::other("a running bootstrap node is required"))?;
        self.register_new_node_through(node_id, bootstrap_host)
    }

    /// Adds `node_id` to the nodes the harness owns, with `bootstrap_host` as the one member it
    /// dials first and what a scenario named for it before it existed.
    fn register_new_node_through(
        &mut self,
        node_id: &str,
        bootstrap_host: String,
    ) -> io::Result<()> {
        if self.nodes.contains_key(node_id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("node '{node_id}' already exists"),
            ));
        }
        let mut config = self
            .nodes
            .values()
            .next()
            .expect("an existing cluster has at least one node")
            .config
            .clone();
        let pending = self.pending_members.remove(node_id).unwrap_or_default();
        if !pending.setting_overrides.is_empty() {
            config
                .node_setting_overrides
                .entry(node_id.to_string())
                .or_default()
                .extend(pending.setting_overrides);
        }
        let mut spec = NodeSpec::new(&self.roots.storage, &self.interconnect_ca, node_id, false)?;
        if let Some(cluster_id) = &pending.cluster_id {
            spec.claim_cluster(&self.interconnect_ca, cluster_id)?;
        }
        if pending.additional_trust_anchor {
            spec.trust_additional_authority(&self.interconnect_ca)?;
        }
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
                self.roots.clone(),
            ),
        );
        Ok(())
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

    /// Hold a stopped node's metrics listener until its next failed startup has been cleaned up.
    pub(crate) async fn occupy_observability_port_for_start(
        &mut self,
        node_id: &str,
    ) -> io::Result<()> {
        let handle = self.nodes.get_mut(node_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("unknown node '{node_id}'"))
        })?;
        if !matches!(
            handle.task.state(),
            NodeTaskState::NotStarted | NodeTaskState::Terminal(_)
        ) || handle.startup_listener_collision.is_some()
        {
            return Err(io::Error::other(
                "a startup port collision needs one stopped node",
            ));
        }
        handle.startup_listener_collision =
            Some(TokioTcpListener::bind((HOST, handle.spec.observability_port)).await?);
        Ok(())
    }

    /// How the last stop of `node_id` that a scenario requested and waited for ended, or `None`
    /// when no such stop has finished its phases.
    pub(crate) fn last_shutdown_outcome(&self, node_id: &str) -> Option<ShutdownOutcome> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle.last_shutdown
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

    /// Make an explicitly started external service visible to this cluster's DNS fixture under
    /// `name`, answered with `addresses` in that order.
    pub(crate) fn publish_dns_service(&self, name: &str, addresses: Vec<IpAddr>) -> io::Result<()> {
        let Some(dns) = &self.dns else {
            return Err(io::Error::other("the cluster has no DNS fixture"));
        };
        dns.publish_service(name, addresses);
        Ok(())
    }

    pub(crate) fn dns_configuration(&self) -> Option<DnsConfiguration> {
        self.dns.as_ref().map(ClusterDns::configuration)
    }

    /// Answer a service's `name` with `answer` in place of its addresses.
    pub(crate) fn answer_dns_service(&self, name: &str, answer: FixtureAnswer) -> io::Result<()> {
        let Some(dns) = &self.dns else {
            return Err(io::Error::other("the cluster has no DNS fixture"));
        };
        dns.answer_name(name, answer);
        Ok(())
    }

    pub(crate) fn dns_questions_for_name(&self, name: &str) -> io::Result<u64> {
        let Some(dns) = &self.dns else {
            return Err(io::Error::other("the cluster has no DNS fixture"));
        };
        Ok(dns.questions_for_name(name))
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

    pub(crate) fn interconnect_endpoint(&self, node_id: &str) -> io::Result<String> {
        let handle = self.nodes.get(node_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("unknown node '{node_id}'"))
        })?;
        Ok(handle.spec.interconnect_endpoint())
    }

    pub(crate) async fn rotate_interconnect_certificates(&mut self) -> io::Result<()> {
        let interconnect_ca = InterconnectTestCa::new(&self.roots.storage)?;
        for (node_id, node) in &mut self.nodes {
            nervix_primitives::task::consume_budget().await;
            let (certificate, key) =
                interconnect_ca.issue_node(&node.spec.cluster_id, node_id, &node.spec.base_dir)?;
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
        let probe_directory = tempfile::tempdir_in(self.roots.storage.path())?;
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
                TEST_CLUSTER_ID
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
                join_terms: target.config.node_settings(target_node_id)?.join_terms(),
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
            // The probe dials a bootstrap host of a cluster the probing credentials claim.
            let bootstrap =
                transport.bootstrap_target(peer_target, BootstrapExpectation::AnyMember);
            match bootstrap.await {
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
            node.release_ports_once_down();
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
    pub(crate) async fn shutdown_for_teardown(
        &mut self,
        deadline: PhaseDeadline,
    ) -> ClusterTeardown {
        ClusterTeardown::stop_all(self.nodes.values_mut(), deadline).await
    }

    pub(crate) async fn restart(&mut self) -> io::Result<()> {
        self.restart_arranged(RestartArrangement::Unchanged).await
    }

    pub(crate) async fn restart_with_relayed_voter(&mut self, voter: &str) -> io::Result<()> {
        self.restart_arranged(RestartArrangement::RelayedVoter(voter))
            .await
    }

    /// Restarts every node with the first runtime installation `node` begins held until the
    /// scenario releases it, so the restarted cluster answers requests that reach `node` before it
    /// has installed any committed domain again.
    pub(crate) async fn restart_holding_runtime_installation(
        &mut self,
        node: &str,
    ) -> io::Result<()> {
        self.restart_arranged(RestartArrangement::HeldRuntimeInstallation(node))
            .await
    }

    /// Restarts every node with `node` counted ready only once the restarted cluster's leader has
    /// ended its startup voter observation, as when a loaded host keeps a node that already runs
    /// and gossips from reporting ready.
    pub(crate) async fn restart_with_node_ready_after_startup_observation(
        &mut self,
        node: &str,
    ) -> io::Result<()> {
        self.restart_arranged(RestartArrangement::ReadyAfterStartupObservation(node))
            .await
    }

    /// Starts every node of a restarted cluster at once, as a restart of the whole cluster does,
    /// and returns once each of them is ready, each inside its own startup budget nested in the
    /// restart's. Starting each node only once the one before it is ready would hold the last voter
    /// back by every earlier node's startup, which on a loaded host can outlast the grace a newly
    /// started leader gives voters it has not yet observed before it fails their work over. The
    /// node `ready_after_startup_observation` names counts as ready only once a leader of the
    /// restarted cluster has ended that observation, so a restart that waited for it before
    /// starting the next node would start that node only after the grace.
    async fn start_restarted_nodes_together(
        &mut self,
        construction: PhaseDeadline,
        ready_after_startup_observation: Option<&str>,
    ) -> io::Result<()> {
        let mut startups = Vec::with_capacity(self.nodes.len());
        for handle in self.nodes.values_mut() {
            let ready_after_observation_end =
                ready_after_startup_observation == Some(handle.spec.node_id.as_str());
            startups.push(async move {
                let node = node_name(&handle.spec.node_id);
                let budget = construction.nested(NODE_STARTUP_BUDGET);
                NodeStartup::start(&node, handle, budget, StartupRetryPolicy::Transient)
                    .await
                    .map_err(io::Error::other)?;
                if ready_after_observation_end {
                    handle
                        .wait_for_startup_voter_observation_end(construction)
                        .await?;
                }
                Ok::<(), io::Error>(())
            });
        }
        for startup in join_all(startups).await {
            startup?;
        }
        Ok(())
    }

    async fn restart_arranged(&mut self, arrangement: RestartArrangement<'_>) -> io::Result<()> {
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
        let bootstrap = self.node_id(1);
        // Every node has stopped, so a fault armed here reaches only the nodes that start next.
        match arrangement {
            RestartArrangement::Unchanged => {}
            RestartArrangement::RelayedVoter(voter) => {
                self.fault_injection
                    .hold_restarted_voter_heartbeat(node_name(&bootstrap), node_name(voter));
                self.fault_injection
                    .fail_health_responses_between(node_name(&bootstrap), node_name(voter));
            }
            RestartArrangement::HeldRuntimeInstallation(node) => {
                self.fault_injection
                    .pause_runtime_installation_on(node_name(node));
            }
            RestartArrangement::ReadyAfterStartupObservation(_) => {
                self.fault_injection.await_startup_voter_observation_end();
            }
        }
        if arrangement.relayed_voter().is_some() {
            // The relayed voter's scenario observes scheduling while the bootstrap node leads, so
            // that node starts first. The held heartbeat can keep replication from catching the
            // voter up, so the others start without waiting for catch-up: the scenario reaches and
            // releases the observation barrier before it waits for that.
            self.start_node_within(&bootstrap, construction).await?;
            for node_id in node_ids.iter().filter(|node_id| **node_id != bootstrap) {
                self.start_node_without_waiting_for_raft_catch_up_within(node_id, construction)
                    .await?;
            }
            self.wait_for_any_leader(&bootstrap).await?;
            return self.wait_for_leader(&bootstrap, Some(&bootstrap)).await;
        }
        self.start_restarted_nodes_together(
            construction,
            arrangement.ready_after_startup_observation(),
        )
        .await?;
        for node_id in &node_ids {
            self.wait_for_node_raft_catch_up(node_id).await?;
        }

        self.wait_for_any_leader(&bootstrap).await?;
        if node_ids.len() > 1 {
            for node_id in &node_ids {
                self.wait_for_any_leader(node_id).await?;
            }
            let voter_refs = node_ids.iter().map(String::as_str).collect::<Vec<_>>();
            self.wait_for_voters(&bootstrap, &voter_refs).await?;
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
            nervix_primitives::task::consume_budget().await;
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

    /// Whether the Kafka broker knows `topic`. The metadata request names the topic from a
    /// consumer, which never lets the broker create it.
    pub(crate) fn kafka_topic_exists(&self, topic: &str) -> io::Result<bool> {
        let partitions = kafka_topic_partition_count(&self.dependencies, topic)?;
        Ok(partitions.is_some())
    }

    /// Waits until the broker lists every group of `groups`, when `listed` is true, or none of
    /// them, when it is false.
    pub(crate) async fn wait_for_kafka_consumer_groups_listed(
        &self,
        groups: &BTreeSet<String>,
        listed: bool,
        within: Duration,
    ) -> io::Result<()> {
        let deadline = Instant::now() + within;
        loop {
            nervix_primitives::task::consume_budget().await;
            let broker_groups = kafka_listed_consumer_groups(&self.dependencies)?;
            let present = groups
                .intersection(&broker_groups)
                .cloned()
                .collect::<BTreeSet<_>>();
            let settled = if listed {
                present == *groups
            } else {
                present.is_empty()
            };
            if settled {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let expected = if listed { "all" } else { "none" };
                return Err(io::Error::other(format!(
                    "expected Kafka to list {expected} of consumer groups {groups:?} within \
                     {within:?}; it lists {present:?} of them"
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
        assert_kafka_consumer_group_next_offset_through(
            kafka_client_config(&self.dependencies)?,
            group,
            topic,
            partition,
            threshold,
            should_reach,
            duration,
        )
        .await
    }

    pub(crate) async fn wait_for_rabbitmq_queue_consumers(
        &self,
        queue: &str,
        expected: usize,
    ) -> io::Result<()> {
        let deadline = Instant::now() + STATUS_WAIT_BUDGET;
        loop {
            nervix_primitives::task::consume_budget().await;
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

    /// The messages `queue` holds ready for delivery now.
    pub(crate) async fn rabbitmq_queue_messages(&self, queue: &str) -> io::Result<usize> {
        let state = rabbitmq_queue_state(&self.dependencies, queue).await?;
        Ok(state.messages)
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

    /// The gRPC endpoint of every node, by node id, which a command whose leader moved is sent to
    /// again until the node that leads now answers it.
    pub(crate) fn leader_routes(&self) -> LeaderRoutes {
        let mut endpoints = BTreeMap::new();
        for (node_id, handle) in &self.nodes {
            endpoints.insert(
                node_id.clone(),
                handle.spec.grpc_uri(handle.config.grpc_mode),
            );
        }
        LeaderRoutes::new(endpoints)
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

    /// Follow the actual metrics listener through a bounded startup retry.
    pub(crate) fn observability_metrics_stream(
        &self,
        node_id: &str,
    ) -> io::Result<watch::Receiver<String>> {
        let handle = self.nodes.get(node_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("unknown node '{node_id}'"))
        })?;
        Ok(handle.metrics_endpoint.subscribe())
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

    pub(crate) async fn limit_pulsar_topic_message_size(
        &self,
        topic: &str,
        max_message_size: u32,
    ) -> io::Result<()> {
        limit_pulsar_topic_message_size(&self.dependencies, topic, max_message_size).await
    }

    /// The number of messages the broker holds for `subscription` of `topic` that no consumer has
    /// acknowledged, as its admin API reports the subscription's backlog.
    pub(crate) async fn pulsar_subscription_backlog(
        &self,
        topic: &str,
        subscription: &str,
    ) -> io::Result<u64> {
        pulsar_subscription_backlog(&self.dependencies, topic, subscription).await
    }

    /// What the broker's admin API reports of `subscription` of `topic` now, absent when the topic
    /// holds no subscription of that name.
    pub(crate) async fn pulsar_subscription(
        &self,
        topic: &str,
        subscription: &str,
    ) -> io::Result<Option<PulsarSubscriptionObservation>> {
        pulsar_subscription(&self.dependencies, topic, subscription).await
    }

    /// Provisions `subscription` of `topic` as an operator does, through the broker's admin API:
    /// the topic is created when it is absent, and the subscription starts at the topic's latest
    /// message, so it holds what is published from then on.
    pub(crate) async fn provision_pulsar_subscription(
        &self,
        topic: &str,
        subscription: &str,
    ) -> io::Result<()> {
        provision_pulsar_subscription(&self.dependencies, topic, subscription).await
    }

    /// Waits until the admin API's report of `subscription` of `topic` satisfies `holds`.
    pub(crate) async fn wait_for_pulsar_subscription(
        &self,
        topic: &str,
        subscription: &str,
        within: Duration,
        holds: impl Fn(Option<&PulsarSubscriptionObservation>) -> bool,
    ) -> io::Result<()> {
        let deadline = Instant::now() + within;
        loop {
            nervix_primitives::task::consume_budget().await;
            let observed = pulsar_subscription(&self.dependencies, topic, subscription).await;
            if let Ok(observation) = &observed
                && holds(observation.as_ref())
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other(format!(
                    "timed out waiting for Pulsar subscription '{subscription}' of topic \
                     '{topic}'; last observation: {observed:?}"
                )));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    /// Waits until the backlog of `subscription` of `topic` holds `expected` messages, as the
    /// acknowledgements a consumer sends reach the broker.
    pub(crate) async fn wait_for_pulsar_subscription_backlog(
        &self,
        topic: &str,
        subscription: &str,
        expected: u64,
        within: Duration,
    ) -> io::Result<()> {
        let deadline = Instant::now() + within;
        loop {
            nervix_primitives::task::consume_budget().await;
            let observed =
                pulsar_subscription_backlog(&self.dependencies, topic, subscription).await;
            if let Ok(backlog) = &observed
                && *backlog == expected
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other(format!(
                    "timed out waiting for Pulsar subscription '{subscription}' of topic \
                     '{topic}' to hold {expected} unacknowledged messages; last observation: \
                     {observed:?}"
                )));
            }
            sleep(POLL_INTERVAL).await;
        }
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
        payloads: &[Vec<u8>],
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

    /// Subscribes a consumer group member that Nervix does not run to `topic` in `group`.
    pub(crate) fn join_external_kafka_group_member(
        &self,
        group: &str,
        topic: &str,
    ) -> io::Result<ExternalKafkaGroupMember> {
        ExternalKafkaGroupMember::join(kafka_client_config(&self.dependencies)?, group, topic)
    }

    pub(crate) async fn reset_kafka_topic_partitions(
        &self,
        topic: &str,
        partitions: i32,
    ) -> io::Result<()> {
        reset_kafka_topic_partitions(&self.dependencies, topic, partitions).await
    }

    pub(crate) async fn ensure_sqs_queue_tls(&self, queue: &str) -> io::Result<()> {
        ensure_sqs_queue_tls(&self.dependencies, queue).await
    }

    /// Creates `queue` with `visibility_timeout`: how long a message a receive took stays invisible
    /// to every other receive unless it is deleted first.
    pub(crate) async fn ensure_sqs_queue_with_visibility_timeout(
        &self,
        queue: &str,
        visibility_timeout: Duration,
    ) -> io::Result<()> {
        let client = sqs_client(&self.dependencies).await?;
        let request = client.create_queue().queue_name(queue).attributes(
            QueueAttributeName::VisibilityTimeout,
            visibility_timeout.as_secs().to_string(),
        );
        request.send().await.map_err(io::Error::other)?;
        Ok(())
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

    /// How many messages JetStream `stream` holds.
    pub(crate) async fn nats_stream_message_count(&self, stream: &str) -> io::Result<u64> {
        nats_stream_message_count(&self.dependencies, stream).await
    }

    /// The integer `field` of the JSON payload of every message JetStream `stream` holds, in the
    /// order the stream stored them.
    pub(crate) async fn nats_stream_field_values(
        &self,
        stream: &str,
        field: &str,
    ) -> io::Result<Vec<i64>> {
        nats_stream_field_values(&self.dependencies, stream, field).await
    }

    /// Provisions the durable pull consumer `consumer` on JetStream `stream`, as an operator does.
    pub(crate) async fn provision_nats_pull_consumer(
        &self,
        stream: &str,
        consumer: &str,
    ) -> io::Result<()> {
        provision_nats_pull_consumer(&self.dependencies, stream, consumer).await
    }

    /// Publishes `payload` to `subject` once, through JetStream, and returns once the stream that
    /// captures the subject stored it.
    pub(crate) async fn publish_nats_jetstream(
        &self,
        subject: &str,
        payload: &str,
    ) -> io::Result<()> {
        publish_nats_jetstream(&self.dependencies, subject, payload).await
    }

    /// Waits up to `duration` for JetStream consumer `consumer` of `stream` to stand at `expected`.
    pub(crate) async fn wait_for_nats_consumer_position(
        &self,
        stream: &str,
        consumer: &str,
        expected: NatsConsumerPosition,
        duration: Duration,
    ) -> io::Result<()> {
        wait_for_nats_consumer_position(&self.dependencies, stream, consumer, expected, duration)
            .await
    }

    /// Whether the NATS server holds JetStream stream `stream`.
    pub(crate) async fn nats_stream_exists(&self, stream: &str) -> io::Result<bool> {
        nats_stream_exists(&self.dependencies, stream).await
    }

    /// Whether JetStream `stream` holds consumer `consumer`.
    pub(crate) async fn nats_consumer_exists(
        &self,
        stream: &str,
        consumer: &str,
    ) -> io::Result<bool> {
        nats_consumer_exists(&self.dependencies, stream, consumer).await
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

    pub(crate) async fn open_endpoint_websocket(
        &self,
        node_id: &str,
        host: &str,
        path: &str,
    ) -> io::Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let handle = self
            .nodes
            .get(node_id)
            .assured("the scenario names a running node");
        let mut request = handle
            .spec
            .websocket_uri(path)
            .into_client_request()
            .map_err(io::Error::other)?;
        request.headers_mut().insert(
            "Host",
            HeaderValue::from_str(host).map_err(io::Error::other)?,
        );
        let (websocket, _) = connect_async(request).await.map_err(io::Error::other)?;
        Ok(websocket)
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

    /// A publisher to the acknowledged HTTP endpoint at `host` and `path` on `node_id`, which
    /// waits at most `wait` for each payload's graph acknowledgement.
    pub(crate) fn acknowledged_http_publisher(
        &self,
        node_id: &str,
        host: &str,
        path: &str,
        wait: Duration,
    ) -> AcknowledgedHttpPublisher {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        AcknowledgedHttpPublisher {
            client: reqwest::Client::new(),
            uri: handle.spec.http_uri(path),
            host: host.to_owned(),
            wait,
        }
    }

    pub(crate) fn spawn_http_publish(
        &self,
        node_id: &str,
        host: String,
        path: String,
        payload: String,
    ) -> nervix_primitives::task::JoinHandle<io::Result<()>> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let uri = handle.spec.http_uri(&path);
        nervix_primitives::task::spawn(async move {
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

        Ok(nervix_primitives::task::spawn(async move {
            let mut outcome = HttpsPublishLoopOutcome::default();
            while !stop.is_cancelled() {
                nervix_primitives::task::consume_budget().await;
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

    pub(crate) fn pause_application_revision_responses_from(&self, node_id: &str) {
        self.fault_injection
            .pause_application_revision_responses_from(node_name(node_id));
    }

    pub(crate) async fn wait_for_application_revision_response_pause(&self, node_id: &str) {
        self.fault_injection
            .wait_for_application_revision_response_pause(&node_name(node_id))
            .await;
    }

    pub(crate) fn release_application_revision_responses_from(&self, node_id: &str) {
        self.fault_injection
            .release_application_revision_responses_from(&node_name(node_id));
    }

    pub(crate) fn fail_health_responses_from(&self, responding_node_id: &str) {
        self.fault_injection
            .fail_health_responses_from(node_name(responding_node_id));
    }

    pub(crate) fn fail_health_responses_between(
        &self,
        probing_node_id: &str,
        responding_node_id: &str,
    ) {
        self.fault_injection.fail_health_responses_between(
            node_name(probing_node_id),
            node_name(responding_node_id),
        );
    }

    pub(crate) fn restore_health_responses_between(
        &self,
        probing_node_id: &str,
        responding_node_id: &str,
    ) {
        self.fault_injection.restore_health_responses_between(
            &node_name(probing_node_id),
            &node_name(responding_node_id),
        );
    }

    pub(crate) fn block_gossip_for_node(&self, node_id: &str, send_delay: Duration) {
        self.fault_injection
            .block_gossip_for_node(node_name(node_id), send_delay);
    }

    pub(crate) fn restore_gossip_for_node(&self, node_id: &str) {
        self.fault_injection
            .restore_gossip_for_node(&node_name(node_id));
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

    pub(crate) fn fail_emitter_client_unavailable_on_all_nodes(&self, emitter: &str) {
        self.fault_injection
            .fail_emitter_client_unavailable(emitter);
    }

    pub(crate) fn clear_emitter_client_fault_on_all_nodes(&self, emitter: &str) {
        self.fault_injection.clear_emitter_client_fault(emitter);
    }

    pub(crate) fn stall_emitter_connector_after_resolving_on_all_nodes(
        &self,
        emitter: &str,
        resolved: usize,
    ) {
        self.fault_injection
            .stall_emitter_connector_after_resolving(emitter, resolved);
    }

    pub(crate) fn fail_next_schedule_publication_on_all_nodes(&self, domain: &str) {
        self.fault_injection.fail_next_schedule_publication(domain);
    }

    pub(crate) fn lose_wasm_reset_publication_authority(&self, domain: &str, count: usize) {
        self.fault_injection.lose_wasm_reset_publication_authority(
            nervix_models::DomainName::parse(domain).expect("a scenario domain is valid"),
            std::num::NonZeroUsize::new(count).expect("a refusal count is positive"),
        );
    }

    pub(crate) fn fail_wasm_reset_publications(&self, domain: &str) {
        self.fault_injection.fail_wasm_reset_publications(
            nervix_models::DomainName::parse(domain).expect("a scenario domain is valid"),
        );
    }

    pub(crate) fn clear_wasm_reset_publication_failure(&self, domain: &str) {
        self.fault_injection.clear_wasm_reset_publication_failure(
            &nervix_models::DomainName::parse(domain).expect("a scenario domain is valid"),
        );
    }

    pub(crate) fn wasm_reset_publication_failure_delivered(&self, domain: &str) -> Option<bool> {
        self.fault_injection
            .wasm_reset_publication_failure_delivered(
                &nervix_models::DomainName::parse(domain).expect("a scenario domain is valid"),
            )
    }

    pub(crate) fn wasm_reset_publication_authority_losses(&self, domain: &str) -> Option<usize> {
        self.fault_injection
            .wasm_reset_publication_authority_losses(
                &nervix_models::DomainName::parse(domain).expect("a scenario domain is valid"),
            )
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
        self.wait_for_status_contains_with_budget(node_id, fragment, STATUS_WAIT_BUDGET)
            .await
    }

    pub(crate) async fn wait_for_status_contains_with_budget(
        &self,
        node_id: &str,
        fragment: &str,
        budget: Duration,
    ) -> io::Result<()> {
        self.wait_until_with_budget(node_id, budget, |status| status.raw.contains(fragment))
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
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
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

    /// A live connector may retain its retirement envelope. Check room for a control operation
    /// rather than requiring an otherwise idle Commands class to contain no live allocations.
    pub(crate) async fn wait_for_commands_memory_headroom(
        &self,
        node_id: &str,
        bytes: u64,
        wait: Duration,
    ) -> io::Result<()> {
        let labels = ["class=\"commands\"".to_string()];
        let mut last_capacity = None;
        let mut last_reserved = None;
        let observed = timeout(wait, async {
            let capacity = self
                .read_observability_metric(
                    node_id,
                    "nervix_execution_memory_capacity_bytes",
                    &labels,
                )
                .await?;
            last_capacity = Some(capacity);
            let requested: f64 = bytes.approx_into();
            loop {
                nervix_primitives::task::consume_budget().await;
                let reserved = self
                    .read_observability_metric(
                        node_id,
                        "nervix_execution_memory_reserved_bytes",
                        &labels,
                    )
                    .await?;
                last_reserved = Some(reserved);
                if reserved + requested <= capacity {
                    return Ok(());
                }
                sleep(POLL_INTERVAL).await;
            }
        })
        .await;
        match observed {
            Ok(result) => result,
            Err(_) => Err(io::Error::other(format!(
                "node '{node_id}' did not leave {bytes} Commands bytes available within {wait:?}; \
                 last capacity={last_capacity:?}, reserved={last_reserved:?}"
            ))),
        }
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
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
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

    /// Moves leadership from `from_node_id` to `to_node_id` underneath the next persistent command
    /// that `from_node_id` handles. `hold` keeps that command where it is until `to_node_id`
    /// reports itself the leader and `from_node_id` reports `to_node_id` as its leader, so the node
    /// handling the command knows it no longer leads when the command goes on. The returned task
    /// ends once the command is released.
    pub(crate) fn move_leadership_under_next_command(
        &self,
        hold: CommandHold,
        from_node_id: &str,
        to_node_id: &str,
    ) -> AbortOnDropHandle<io::Result<()>> {
        let from = node_name(from_node_id);
        let to = node_name(to_node_id);
        let successor = self.leader_witness(to_node_id);
        let predecessor = self.leader_witness(from_node_id);
        let fault_injection = self.fault_injection.clone();
        hold.arm(&fault_injection, from.clone());
        AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
            hold.reached(&fault_injection, &from).await;
            fault_injection.request_leadership_transfer(from.clone(), to);
            let deadline = PhaseDeadline::after(STATUS_WAIT_BUDGET);
            let elected = successor.reports_leader(deadline, &successor.node_id).await;
            let known = match elected {
                Ok(()) => {
                    predecessor
                        .reports_leader(deadline, &successor.node_id)
                        .await
                }
                Err(error) => Err(error),
            };
            hold.release(&fault_injection, &from);
            known
        }))
    }

    /// The status of `node_id`, which a leadership move reads to learn whom the node reports as
    /// the leader.
    fn leader_witness(&self, node_id: &str) -> LeaderWitness {
        let endpoint = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"))
            .status_endpoint();
        LeaderWitness {
            node_id: node_id.to_string(),
            endpoint,
        }
    }
    async fn wait_until<F>(&self, node_id: &str, predicate: F) -> io::Result<()>
    where
        F: Fn(&ClusterStatus) -> bool,
    {
        self.wait_until_with_budget(node_id, STATUS_WAIT_BUDGET, predicate)
            .await
    }

    async fn wait_until_with_budget<F>(
        &self,
        node_id: &str,
        budget: Duration,
        predicate: F,
    ) -> io::Result<()>
    where
        F: Fn(&ClusterStatus) -> bool,
    {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let deadline = PhaseDeadline::after(budget);
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
    metrics_endpoint: watch::Sender<String>,
    startup_listener_collision: Option<TokioTcpListener>,
    fault_injection: FaultInjection,
    config: TestClusterConfig,
    task: OwnedNodeTask,
    shutdown: Option<ShutdownCoordinator>,
    /// How the last stop a scenario requested and waited for ended, kept after its coordinator
    /// is released so the scenario can read the outcome of each shutdown phase.
    last_shutdown: Option<ShutdownOutcome>,
    /// When the current launch was first asked to stop and when its task ended, for the record of
    /// a stop a scenario waits for. `None` until the node has been launched.
    stop_clock: Option<NodeStopClock>,
    /// The cluster's storage and watchdog registration, which a node left stopping keeps until its
    /// task has ended.
    roots: Arc<ClusterRoots>,
    /// The settings of the current launch, which bound how long its stop may take. `None` until
    /// the node has been launched.
    launched_settings: Option<NodeSettings>,
}

impl NodeHandle {
    fn new(
        spec: NodeSpec,
        fault_injection: FaultInjection,
        config: TestClusterConfig,
        roots: Arc<ClusterRoots>,
    ) -> Self {
        let (metrics_endpoint, _) =
            watch::channel(format!("http://{}/metrics", spec.observability_addr()));
        Self {
            spec,
            metrics_endpoint,
            startup_listener_collision: None,
            fault_injection,
            config,
            task: OwnedNodeTask::not_started(),
            shutdown: None,
            last_shutdown: None,
            stop_clock: None,
            roots,
            launched_settings: None,
        }
    }

    fn start(&mut self) -> io::Result<()> {
        match self.task.state() {
            NodeTaskState::Running => return Ok(()),
            // The node this handle launched before is still stopping under its keeper, with the
            // database and the listeners a fresh launch would need.
            NodeTaskState::LeftStopping => {
                return Err(io::Error::other(format!(
                    "node '{}' was left stopping and cannot be started until it has ended",
                    self.spec.node_id
                )));
            }
            NodeTaskState::NotStarted | NodeTaskState::Terminal(_) => {}
        }
        if self.config.grpc_mode == InternalTransportMode::Https {
            ensure_dev_tls_assets()?;
        }

        let settings = self.config.node_settings(&self.spec.node_id)?;
        let db_path = self.spec.db_path()?;
        let identity = NodeIdentity::builder()
            .addr(parse_addr(&self.spec.grpc_addr())?)
            .grpc_mode(self.config.grpc_mode)
            .grpc_https_listen_addr(Some(parse_addr(&self.spec.grpc_https_addr())?))
            .http_listen_addr(parse_addr(&self.spec.http_addr())?)
            .https_listen_addr(parse_addr(&self.spec.https_addr())?)
            .observability_listen_addr(parse_addr(&self.spec.observability_addr())?)
            .web_console_listen_addr(parse_addr(&self.spec.web_console_addr())?)
            .web_console_advertise_addr(Some(parse_addr(&self.spec.web_console_addr())?.into()))
            .cluster_id(self.spec.cluster_id.clone())
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
            .init_default_user_password(
                self.config
                    .default_user_password
                    .initial_password(self.spec.allow_bootstrap),
            )
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
            .build();
        let application = Application::builder()
            .identity(identity)
            .settings(settings)
            .fault_injection(self.fault_injection.clone())
            .graceful_shutdown_drain(self.config.graceful_shutdown_drain)
            .build();
        let shutdown = application.shutdown.clone();
        self.launched_settings = Some(settings);
        // Published before the task is spawned and handed to that task, so the watchdog sees the
        // node for exactly as long as it runs: the registration leaves the registry when the task
        // ends, whether it returned, failed or panicked.
        let live = self
            .roots
            .live
            .handle()
            .node_started(&self.spec.node_id, StdArc::new(shutdown.clone()));
        let (stop_clock, ended) = NodeStopClock::start();
        self.shutdown = Some(shutdown);
        self.stop_clock = Some(stop_clock);
        self.task = OwnedNodeTask::spawn(async move {
            let _live = live;
            // Dropped when this task ends, however it ends, which records when that was.
            let _ended = ended;
            application.run().await
        });
        Ok(())
    }

    async fn stop(&mut self) -> io::Result<()> {
        self.request_stop();
        self.wait_stopped().await
    }

    fn shutdown_watchdog_timeout(&self) -> io::Result<Duration> {
        let shutdown_settings = match &self.launched_settings {
            Some(settings) => settings.shutdown,
            None => self.config.node_settings(&self.spec.node_id)?.shutdown,
        };
        let application_drain_timeout = if self.config.graceful_shutdown_drain {
            shutdown_settings.drain_timeout()
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
            .max(shutdown_settings.shutdown_timeout()))
    }

    /// Waits for a node a scenario stopped itself, within the product shutdown deadlines that
    /// scenario configured. Scenario cleanup uses the harness cleanup budget instead.
    ///
    /// A node still stopping when the watchdog passes fails the step and keeps its task: it holds
    /// its databases and its consensus registration until it has ended, so nothing of it is given
    /// back here.
    async fn wait_stopped(&mut self) -> io::Result<()> {
        let shutdown_timeout = self.shutdown_watchdog_timeout()?;
        let waited = self.task.wait(PhaseDeadline::after(shutdown_timeout)).await;
        // A stop is printed by the wait that observed the node's ending, and only by that one.
        let observed_by_this_wait = matches!(waited, NodeTaskWaitOutcome::Joined(_));
        let outcome = match waited {
            NodeTaskWaitOutcome::NotStarted => return Ok(()),
            NodeTaskWaitOutcome::AlreadyObserved(outcome)
            | NodeTaskWaitOutcome::Joined(outcome) => outcome,
            NodeTaskWaitOutcome::StillRunning | NodeTaskWaitOutcome::LeftStopping => {
                let ending = match &self.shutdown {
                    Some(shutdown) => {
                        NodeStopEnding::StillStopping(NodeStopProgress::observe(shutdown))
                    }
                    // A node left stopping before this wait went to its keeper with its
                    // coordinator.
                    None => NodeStopEnding::LeftToKeeper,
                };
                let record = self.stop_record(shutdown_timeout, ending);
                eprintln!("{record}");
                return Err(io::Error::other(NodeShutdownError::Deadline(record)));
            }
        };
        let shutdown_outcome = match self.shutdown.take() {
            Some(shutdown) => {
                self.last_shutdown = shutdown.outcome();
                self.last_shutdown
            }
            None => None,
        };
        if observed_by_this_wait {
            let record = self.stop_record(
                shutdown_timeout,
                NodeStopEnding::Ended {
                    task: outcome.clone(),
                    shutdown: shutdown_outcome,
                },
            );
            eprintln!("{record}");
        }
        let task_result = if let NodeTaskTerminalOutcome::Panic(_) = outcome.as_ref() {
            Err(io::Error::other(NodeShutdownError::Task {
                node: node_name(&self.spec.node_id),
                outcome,
            }))
        } else {
            Ok(())
        };
        self.fault_injection
            .unregister_consensus(&node_name(&self.spec.node_id));
        if task_result.is_ok() {
            self.ensure_database_unlocked().await?;
        }
        task_result
    }

    /// The record of a stop this handle waited for under `watchdog`.
    fn stop_record(&self, watchdog: Duration, ending: NodeStopEnding) -> NodeStopRecord {
        let stopping_for = match &self.stop_clock {
            Some(clock) => clock.stopping_for(),
            None => None,
        };
        NodeStopRecord {
            node: self.spec.node_id.clone(),
            stopping_for,
            watchdog,
            ending,
        }
    }

    /// Returns this node's ports to the pool unless its task still runs, here or under a keeper,
    /// which gives them back itself once the node has ended.
    fn release_ports_once_down(&mut self) {
        match self.task.state() {
            NodeTaskState::NotStarted | NodeTaskState::Terminal(_) => self.spec.release_ports(),
            NodeTaskState::Running | NodeTaskState::LeftStopping => {}
        }
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

    /// Waits, inside the restart's budget, until a leader of the restarted cluster has ended its
    /// startup voter observation, for a node the restart counts as ready only after that.
    async fn wait_for_startup_voter_observation_end(
        &self,
        construction: PhaseDeadline,
    ) -> io::Result<()> {
        let ended = construction
            .bound(
                self.fault_injection
                    .wait_for_startup_voter_observation_end(),
            )
            .await;
        match ended {
            BeforeDeadline::Finished(()) => Ok(()),
            BeforeDeadline::Passed => Err(io::Error::other(format!(
                "no leader of the restarted cluster ended its startup voter observation within \
                 the restart's budget, so node '{}' never counted as ready",
                self.spec.node_id
            ))),
        }
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

    /// Failed-startup cleanup shares the node's remaining absolute startup budget. The stop is
    /// requested first, and the product's shutdown watchdog does not restart the harness budget.
    /// A node still stopping when that budget ends keeps its task, its coordinator and its
    /// consensus registration; a replacement cannot take the database or listeners it still holds.
    async fn clean_up(&mut self, cleanup: PhaseDeadline) -> AttemptCleanup {
        self.request_stop();
        let outcome = AttemptCleanup::from(self.task.wait(cleanup).await);
        self.startup_listener_collision = None;
        match outcome {
            AttemptCleanup::StillStopping => {}
            AttemptCleanup::NothingLaunched | AttemptCleanup::Stopped(_) => {
                self.shutdown = None;
                self.fault_injection
                    .unregister_consensus(&node_name(&self.spec.node_id));
            }
        }
        outcome
    }

    fn move_to_fresh_ports(&mut self, ports: StartupPorts) -> io::Result<()> {
        self.spec.reallocate_ports(ports)?;
        self.metrics_endpoint
            .send_replace(format!("http://{}/metrics", self.spec.observability_addr()));
        Ok(())
    }
}

impl TeardownNode for NodeHandle {
    fn node_name(&self) -> String {
        self.spec.node_id.clone()
    }

    fn request_stop(&mut self) {
        if let Some(shutdown) = &self.shutdown {
            shutdown.request_stop();
            if let Some(stop_clock) = &mut self.stop_clock {
                stop_clock.stop_requested();
            }
        }
    }

    fn owned_task(&mut self) -> &mut OwnedNodeTask {
        &mut self.task
    }

    fn stop_progress(&self) -> Option<NodeStopProgress> {
        self.shutdown.as_ref().map(NodeStopProgress::observe)
    }

    fn release(&mut self) {
        self.shutdown = None;
        self.fault_injection
            .unregister_consensus(&node_name(&self.spec.node_id));
        self.spec.release_ports();
    }

    fn leave_stopping(&mut self) {
        let node = self.spec.node_id.clone();
        let ports = self.spec.ports();
        let fault_injection = self.fault_injection.clone();
        let roots = self.roots.clone();
        let left = Instant::now();
        let kept = self.task.keep_until_ended(move |outcome| {
            fault_injection.unregister_consensus(&node_name(&node));
            release_test_ports(&ports);
            eprintln!(
                "node '{node}' ended {:?} after it was left stopping: {outcome}",
                left.elapsed()
            );
            // The storage and the watchdog registration go last: the node used both until now.
            drop(roots);
        });
        if kept {
            self.shutdown = None;
        }
    }
}

impl Drop for NodeHandle {
    /// A node dropped while it runs is asked to stop and left to end itself with what it holds.
    /// Aborting its task would end only the future the node was started as and orphan the tasks
    /// the node spawned, with its databases and its listeners.
    fn drop(&mut self) {
        if !self.task.is_running() {
            return;
        }
        // Without a runtime nothing can run the node's task or a keeper for it, so there is
        // nothing left to hand over.
        if Handle::try_current().is_err() {
            return;
        }
        TeardownNode::request_stop(self);
        self.leave_stopping();
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
    /// The watchdog of a stop a scenario waited for passed with the node still stopping. The
    /// record names how long it had been stopping and where its own shutdown stood.
    #[error("{0}")]
    Deadline(NodeStopRecord),
}

#[derive(Debug)]
struct NodeSpec {
    node_id: String,
    /// The cluster the node claims in its configuration and its certificate.
    cluster_id: String,
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
    /// A scenario-owned byte pacer at this node's advertised endpoint.
    paced_interconnect: Option<PacedInterconnect>,
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
            interconnect_ca.issue_node(TEST_CLUSTER_ID, node_id, &base_dir)?;
        let ports = NodePorts::allocate()?;
        let syslog_ingestor_host = Self::syslog_ingestor_host(node_id)?;

        Ok(Self {
            node_id: node_id.to_string(),
            cluster_id: TEST_CLUSTER_ID.to_string(),
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
            paced_interconnect: None,
            dns: None,
        })
    }

    /// Makes this node claim `cluster_id`, with a certificate the cluster's authority issues for
    /// that cluster.
    fn claim_cluster(
        &mut self,
        interconnect_ca: &InterconnectTestCa,
        cluster_id: &str,
    ) -> io::Result<()> {
        let (certificate, key) =
            interconnect_ca.issue_node(cluster_id, &self.node_id, &self.base_dir)?;
        self.cluster_id = cluster_id.to_string();
        self.interconnect_tls_cert = certificate;
        self.interconnect_tls_key = key;
        Ok(())
    }

    /// Makes this node trust a second, freshly created certificate authority beside the cluster's
    /// own, through a bundle holding both.
    fn trust_additional_authority(
        &mut self,
        interconnect_ca: &InterconnectTestCa,
    ) -> io::Result<()> {
        let directory = self.base_dir.join("additional-authority");
        std::fs::create_dir_all(&directory)?;
        let additional = InterconnectTestCa::new_in(&directory)?;
        let mut bundle = std::fs::read_to_string(&interconnect_ca.path)?;
        bundle.push_str(&std::fs::read_to_string(&additional.path)?);
        let bundle_path = self.base_dir.join("interconnect-ca-bundle.pem");
        std::fs::write(&bundle_path, bundle)?;
        self.interconnect_tls_ca = bundle_path;
        Ok(())
    }

    /// The `<n>` of a test node id `node-<n>`.
    /// The number a test node's name ends with, which places it on its own loopback address.
    fn index(node_id: &str) -> io::Result<u8> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "test node id '{node_id}' must have the form <prefix>-1 through <prefix>-254"
                ),
            )
        };
        let Some((_, number)) = node_id.rsplit_once('-') else {
            return Err(invalid());
        };
        let Ok(index) = number.parse::<u8>() else {
            return Err(invalid());
        };
        if !(1..=254).contains(&index) {
            return Err(invalid());
        }
        Ok(index)
    }

    fn syslog_ingestor_host(node_id: &str) -> io::Result<IpAddr> {
        let node_index = Self::index(node_id)?;
        Ok(IpAddr::V4(Ipv4Addr::new(127, 0, 0, node_index)))
    }

    fn set_interconnect_address(&mut self, address: InterconnectAddress) {
        self.interconnect_ip = address.listen_ip;
        self.interconnect_host = address.advertised_host;
    }

    fn reallocate_ports(&mut self, selection: StartupPorts) -> io::Result<()> {
        let prior_interconnect = self.interconnect_port;
        let listeners = match selection {
            StartupPorts::All => vec![
                &mut self.grpc_port,
                &mut self.grpc_https_port,
                &mut self.http_port,
                &mut self.https_port,
                &mut self.observability_port,
                &mut self.web_console_port,
                &mut self.interconnect_port,
            ],
            StartupPorts::Grpc => vec![&mut self.grpc_port, &mut self.grpc_https_port],
            StartupPorts::Http => vec![&mut self.http_port],
            StartupPorts::Https => vec![&mut self.https_port],
            StartupPorts::Observability => vec![&mut self.observability_port],
            StartupPorts::WebConsole => vec![&mut self.web_console_port],
            StartupPorts::Interconnect => vec![&mut self.interconnect_port],
        };
        let replacements = next_ports(listeners.len())?;
        for (listener, replacement) in listeners.into_iter().zip(replacements) {
            // A peer can still dial a published address after this node has stopped. Retain its
            // reservation, as an explicit interconnect address move does, until the run ends.
            if *listener != prior_interconnect {
                release_test_ports(&[*listener]);
            }
            *listener = replacement;
        }
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
    /// teardown paths. That is what makes
    /// the release safe, not the stop itself, so a caller that releases while a peer may still dial
    /// the address belongs elsewhere.
    fn release_ports(&mut self) {
        release_test_ports(&self.ports());
    }

    /// The seven ports this node listens on.
    fn ports(&self) -> [u16; 7] {
        [
            self.grpc_port,
            self.grpc_https_port,
            self.http_port,
            self.https_port,
            self.observability_port,
            self.web_console_port,
            self.interconnect_port,
        ]
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
        let port = self
            .paced_interconnect
            .as_ref()
            .map_or(self.interconnect_port, PacedInterconnect::port);
        NodeEndpoint::new(self.interconnect_host.clone(), port).to_string()
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

/// The retry delay of an acknowledged endpoint's refusal that names none.
const ACKNOWLEDGED_HTTP_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Posts payloads to one acknowledged HTTP endpoint through one client, so the posts a step keeps
/// in flight together share its connections.
pub(crate) struct AcknowledgedHttpPublisher {
    client: reqwest::Client,
    uri: String,
    host: String,
    /// How long one payload may take to be acknowledged, its retries included.
    wait: Duration,
}

impl AcknowledgedHttpPublisher {
    /// Posts `payload` until the endpoint answers 200 with a completed outcome.
    ///
    /// The sender owns the retry of a 503: a full ACK window, a downstream refusal, an expired ACK
    /// timeout and a quiesced intake are all answered that way, with the delay to wait. A retried
    /// payload may be delivered twice, because a timed-out attempt is not rolled back. The wait
    /// bounds only an endpoint that keeps refusing the payload; every attempt is answered within
    /// the endpoint's ACK timeout.
    pub(crate) async fn publish(&self, payload: &str) -> io::Result<()> {
        let deadline = Instant::now() + self.wait;
        loop {
            nervix_primitives::task::consume_budget().await;
            let response = self
                .client
                .post(&self.uri)
                .header("Host", &self.host)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(payload.to_owned())
                .send()
                .await
                .map_err(io::Error::other)?;
            let status = response.status();
            let retry_delay = match response.headers().get(reqwest::header::RETRY_AFTER) {
                Some(value) => {
                    let seconds = value
                        .to_str()
                        .map_err(io::Error::other)?
                        .parse::<u64>()
                        .map_err(io::Error::other)?;
                    Duration::from_secs(seconds)
                }
                None => ACKNOWLEDGED_HTTP_RETRY_DELAY,
            };
            let body = response.bytes().await.map_err(io::Error::other)?;
            let outcome: serde_json::Value = serde_json::from_slice(&body).map_err(|error| {
                io::Error::other(format!(
                    "the endpoint answered {status} outside its outcome vocabulary: {error}"
                ))
            })?;
            if status == reqwest::StatusCode::OK {
                if outcome != serde_json::json!({ "outcome": "completed", "cause": "none" }) {
                    return Err(io::Error::other(format!(
                        "the endpoint answered 200 without completing the payload: {outcome}"
                    )));
                }
                return Ok(());
            }
            if status != reqwest::StatusCode::SERVICE_UNAVAILABLE {
                return Err(io::Error::other(format!(
                    "the endpoint refused the payload for good: {status} {outcome}"
                )));
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "the payload was not acknowledged within {:?}; the last answer was \
                         {status} {outcome}",
                        self.wait
                    ),
                ));
            }
            sleep(retry_delay).await;
        }
    }
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
        let kind = if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            io::ErrorKind::WouldBlock
        } else {
            io::ErrorKind::Other
        };
        return Err(io::Error::new(
            kind,
            format!("unexpected http status {}", response.status()),
        ));
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

impl ClusterStatus {
    /// Whether the membership this status reports holds `node_id` as a voter.
    fn is_voter(&self, node_id: &str) -> bool {
        match self.membership.get(node_id) {
            Some(role) => role == "voter",
            None => false,
        }
    }
}

/// Reads a retained status observation through the same parser as the cluster's status waits.
pub(crate) fn interconnect_status_from_text(raw: &str, peer: &str) -> Option<String> {
    ClusterStatus::parse(raw.to_string())
        .interconnect
        .get(peer)
        .cloned()
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
        command_connect_options(server, query)?,
    )
    .await
    .map_err(|report| io::Error::other(format!("{report:#}")))?;
    let outcome = client
        .execute(query.to_string())
        .await
        .map_err(|report| io::Error::other(format!("{report:#}")))?;
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
    let (host, port) = dependency_host_port(dependencies, MQTT_ADDR, 1883)?;
    publish_mqtt_to_address(host, port, topic, payload, qos, retain).await
}

pub(crate) async fn publish_mqtt_to_address(
    host: String,
    port: u16,
    topic: &str,
    payload: &str,
    qos: QoS,
    retain: bool,
) -> io::Result<()> {
    let client_id = format!("nervix-cucumber-{}", Uuid::now_v7().as_simple());
    let options = MqttOptions::new(client_id, (host, port));
    let (client, mut eventloop) = AsyncClient::builder(options).capacity(16).build();
    let driver = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
        loop {
            nervix_primitives::task::consume_budget().await;
            if eventloop.poll().await.is_err() {
                break;
            }
        }
    }));

    let published = timeout(BROKER_TIMEOUT, async {
        // Runtime ingestors can still be attaching immediately after START. Retain the input on
        // its unique scenario topic, and keep driving until it is sent (QoS 0) or answered (QoS 1).
        let notice = client
            .publish_tracked(topic, payload, PublishOptions::new(qos).retain(retain))
            .await
            .map_err(io::Error::other)?;
        notice
            .wait_completion_async()
            .await
            .map_err(io::Error::other)
    })
    .await;

    driver.abort();
    let _ = driver.await;
    published.map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?
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
    let (host, port) = dependency_host_port(dependencies, MQTT_ADDR, 1883)?;
    publish_mqtt_burst_to_address(host, port, topic, payload, count, qos).await
}

pub(crate) async fn publish_mqtt_burst_to_address(
    host: String,
    port: u16,
    topic: &str,
    payload: &str,
    count: usize,
    qos: QoS,
) -> io::Result<()> {
    let client_id = format!("nervix-cucumber-burst-{}", Uuid::now_v7().as_simple());
    let options = MqttOptions::new(client_id, (host, port));
    let (client, mut eventloop) = AsyncClient::builder(options)
        .capacity(count.max(16))
        .build();
    let driver = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
        loop {
            nervix_primitives::task::consume_budget().await;
            if eventloop.poll().await.is_err() {
                break;
            }
        }
    }));

    // The broker-operation timeout bounds each publication's queueing and completion rather than
    // the whole burst, so a burst fails only once the broker answers none of its publications for
    // that long. A broker the parallel suite loads answers a burst of 2,000 QoS 1 publications in
    // more than one timeout while it answers each of them well within it.
    let published = async {
        let mut notices = Vec::with_capacity(count);
        for _ in 0..count {
            nervix_primitives::task::consume_budget().await;
            let queued = timeout(
                BROKER_TIMEOUT,
                client.publish_tracked(topic, payload, PublishOptions::new(qos)),
            )
            .await
            .map_err(|elapsed| io::Error::new(io::ErrorKind::TimedOut, elapsed))?;
            notices.push(queued.map_err(io::Error::other)?);
        }
        // Queue the complete burst before waiting so completion tracking preserves its demand.
        for notice in notices {
            nervix_primitives::task::consume_budget().await;
            let completed = timeout(BROKER_TIMEOUT, notice.wait_completion_async())
                .await
                .map_err(|elapsed| io::Error::new(io::ErrorKind::TimedOut, elapsed))?;
            completed.map_err(io::Error::other)?;
        }
        Ok::<_, io::Error>(())
    }
    .await;
    driver.abort();
    let _ = driver.await;
    published
}

/// Declares `queue` on the scenario's broker. The queue belongs to the broker, so a scenario whose
/// nodes are server processes provisions it the same way as one with an in-process cluster.
pub(crate) async fn ensure_rabbitmq_queue(
    dependencies: &DependencyEndpoints,
    queue: &str,
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
    declare_rabbitmq_queue(&channel, queue).await
}

/// Declares `exchange` as an exchange of `kind`, as an operator provisions it before an emitter
/// publishes to it.
pub(crate) async fn ensure_rabbitmq_exchange(
    dependencies: &DependencyEndpoints,
    exchange: &str,
    kind: lapin::ExchangeKind,
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
    channel
        .exchange_declare(
            exchange.into(),
            kind,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(io::Error::other)
}

/// Declares `queue` and binds it to `exchange` under `routing_key`, so the exchange routes to it
/// every message its binding matches.
pub(crate) async fn bind_rabbitmq_queue(
    dependencies: &DependencyEndpoints,
    queue: &str,
    exchange: &str,
    routing_key: &str,
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
        .queue_bind(
            queue.into(),
            exchange.into(),
            routing_key.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(io::Error::other)
}

/// Deletes `queue` from the scenario's broker with whatever it holds, as an operator would, so a
/// message later published to it has no route.
pub(crate) async fn delete_rabbitmq_queue(
    dependencies: &DependencyEndpoints,
    queue: &str,
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
    channel
        .queue_delete(queue.into(), QueueDeleteOptions::default())
        .await
        .map_err(io::Error::other)?;
    Ok(())
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

/// What a passive declaration of a RabbitMQ queue reports about it.
struct RabbitMqQueueState {
    /// The messages ready for delivery. A delivery a consumer still holds unacknowledged is not one
    /// of them until the consumer leaves.
    messages: usize,
    consumers: usize,
}

async fn rabbitmq_queue_consumer_count(
    dependencies: &DependencyEndpoints,
    queue: &str,
) -> io::Result<usize> {
    let state = rabbitmq_queue_state(dependencies, queue).await?;
    Ok(state.consumers)
}

async fn rabbitmq_queue_state(
    dependencies: &DependencyEndpoints,
    queue: &str,
) -> io::Result<RabbitMqQueueState> {
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
    Ok(RabbitMqQueueState {
        messages: declared.message_count().arch_into(),
        consumers: declared.consumer_count().arch_into(),
    })
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
    let deadline = Instant::now() + BROKER_TIMEOUT;
    loop {
        nervix_primitives::task::consume_budget().await;
        let counts: Vec<(String, usize)> = redis::cmd("PUBSUB")
            .arg("NUMSUB")
            .arg(channel)
            .query_async(&mut connection)
            .await
            .map_err(io::Error::other)?;
        if counts.iter().any(|(_, subscribers)| *subscribers > 0) {
            break;
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for a Redis burst subscriber on channel '{channel}'"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
    publish_burst(&mut connection, channel, payload, count).await?;
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
        nervix_primitives::task::consume_budget().await;
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
        nervix_primitives::task::consume_budget().await;
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

/// Sets the `maxMessageSize` policy of an existing topic through the broker's admin API, and waits
/// until the broker reads the policy back. The broker updates a loaded topic's policies before it
/// serves them, so the read-back is the evidence that the next message is measured against it.
async fn limit_pulsar_topic_message_size(
    dependencies: &DependencyEndpoints,
    topic: &str,
    max_message_size: u32,
) -> io::Result<()> {
    const POLICY_TIMEOUT: Duration = Duration::from_secs(60);
    let admin = dependencies.get(PULSAR_ADMIN_ADDR)?;
    let url = format!("{admin}/admin/v2/persistent/public/default/{topic}/maxMessageSize");
    let client = reqwest::Client::new();
    client
        .post(&url)
        .json(&max_message_size)
        .send()
        .await
        .map_err(io::Error::other)?
        .error_for_status()
        .map_err(io::Error::other)?;

    let expected = max_message_size.to_string();
    let deadline = Instant::now() + POLICY_TIMEOUT;
    loop {
        nervix_primitives::task::consume_budget().await;
        let observed = read_pulsar_admin_value(&client, &url).await;
        if observed.trim() == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for Pulsar topic '{topic}' to report maxMessageSize \
                 {expected}; last observation: {observed}"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// The backlog the Pulsar admin API reports for `subscription` of `topic`: the messages the broker
/// holds for it that no consumer acknowledged, delivered or not.
async fn pulsar_subscription_backlog(
    dependencies: &DependencyEndpoints,
    topic: &str,
    subscription: &str,
) -> io::Result<u64> {
    let admin = dependencies.get(PULSAR_ADMIN_ADDR)?;
    let url = format!("{admin}/admin/v2/persistent/public/default/{topic}/stats");
    let stats: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .map_err(io::Error::other)?
        .error_for_status()
        .map_err(io::Error::other)?
        .json()
        .await
        .map_err(io::Error::other)?;
    let Some(backlog) = stats["subscriptions"][subscription]["msgBacklog"].as_u64() else {
        return Err(io::Error::other(format!(
            "the stats of Pulsar topic '{topic}' report no backlog for subscription \
             '{subscription}': {stats}"
        )));
    };
    Ok(backlog)
}

/// One subscription of a topic as the Pulsar admin API reports it: whether its cursor is
/// durable, which a probe's non-durable cursor is not, and the names its consumers registered
/// under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PulsarSubscriptionObservation {
    pub(crate) durable: bool,
    pub(crate) consumer_names: Vec<String>,
}

/// The subscription `subscription` of `topic` as the admin API's topic stats report it, absent
/// when the topic holds no subscription of that name. A topic the broker does not have holds no
/// subscription either: the broker deletes an inactive topic without subscriptions.
async fn pulsar_subscription(
    dependencies: &DependencyEndpoints,
    topic: &str,
    subscription: &str,
) -> io::Result<Option<PulsarSubscriptionObservation>> {
    let admin = dependencies.get(PULSAR_ADMIN_ADDR)?;
    let url = format!("{admin}/admin/v2/persistent/public/default/{topic}/stats");
    let response = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .map_err(io::Error::other)?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let stats: serde_json::Value = response
        .error_for_status()
        .map_err(io::Error::other)?
        .json()
        .await
        .map_err(io::Error::other)?;
    let reported = &stats["subscriptions"][subscription];
    if reported.is_null() {
        return Ok(None);
    }
    let Some(durable) = reported["isDurable"].as_bool() else {
        return Err(io::Error::other(format!(
            "the stats of Pulsar topic '{topic}' report no durability for subscription \
             '{subscription}': {stats}"
        )));
    };
    let mut consumer_names = Vec::new();
    for consumer in reported["consumers"].as_array().into_iter().flatten() {
        let Some(name) = consumer["consumerName"].as_str() else {
            return Err(io::Error::other(format!(
                "the stats of Pulsar topic '{topic}' report a consumer of subscription \
                 '{subscription}' without a name: {stats}"
            )));
        };
        consumer_names.push(name.to_string());
    }
    consumer_names.sort();
    Ok(Some(PulsarSubscriptionObservation {
        durable,
        consumer_names,
    }))
}

/// Creates `topic` when the broker does not have it, then `subscription` of it at the topic's
/// latest message, through the admin API.
async fn provision_pulsar_subscription(
    dependencies: &DependencyEndpoints,
    topic: &str,
    subscription: &str,
) -> io::Result<()> {
    let admin = dependencies.get(PULSAR_ADMIN_ADDR)?;
    let client = reqwest::Client::new();
    let topic_url = format!("{admin}/admin/v2/persistent/public/default/{topic}");
    let created = client
        .put(&topic_url)
        .send()
        .await
        .map_err(io::Error::other)?;
    // A topic that already exists, such as one a probe of the subscription created, answers
    // conflict and is used as it is.
    if created.status() != reqwest::StatusCode::CONFLICT {
        created.error_for_status().map_err(io::Error::other)?;
    }
    // An ingestor that checks for the missing subscription holds a non-durable cursor of its name
    // for the moment of each check, and the broker refuses to create the subscription as a
    // conflict meanwhile. An operator repeats the request, and so does this, until the broker
    // holds the subscription durably.
    let subscription_url = format!("{topic_url}/subscription/{subscription}");
    let deadline = Instant::now() + PULSAR_PROVISION_TIMEOUT;
    loop {
        nervix_primitives::task::consume_budget().await;
        let created = client
            .put(&subscription_url)
            .send()
            .await
            .map_err(io::Error::other)?;
        if created.status() != reqwest::StatusCode::CONFLICT {
            created.error_for_status().map_err(io::Error::other)?;
            return Ok(());
        }
        let observed = pulsar_subscription(dependencies, topic, subscription).await?;
        if let Some(PulsarSubscriptionObservation { durable: true, .. }) = observed {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "the broker refused to create Pulsar subscription '{subscription}' of topic \
                 '{topic}' as a conflict for {PULSAR_PROVISION_TIMEOUT:?} without holding it \
                 durably; last observation: {observed:?}"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// The body the admin API answers `url` with, or what kept it from answering.
async fn read_pulsar_admin_value(client: &reqwest::Client, url: &str) -> String {
    let response = match client.get(url).send().await {
        Ok(response) => response,
        Err(error) => return format!("request failed: {error}"),
    };
    match response.text().await {
        Ok(body) => body,
        Err(error) => format!("unreadable response: {error}"),
    }
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
    payloads: &[Vec<u8>],
) -> io::Result<()> {
    let mut client_config = kafka_client_config(dependencies)?;
    let producer: FutureProducer = client_config
        .set("delivery.timeout.ms", "5000")
        .set("request.timeout.ms", "5000")
        .create()
        .map_err(io::Error::other)?;
    let mut deliveries = Vec::with_capacity(payloads.len());
    for payload in payloads {
        nervix_primitives::task::consume_budget().await;
        deliveries.push(producer.send(
            FutureRecord::<(), [u8]>::to(topic).payload(payload.as_slice()),
            Duration::from_secs(5),
        ));
    }
    let results = nervix_primitives::time::timeout(
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
    publish_kafka_record_through(
        kafka_client_config(dependencies)?,
        topic,
        partition,
        payload,
        headers,
    )
    .await
}

/// Publishes one record to `topic` through a producer that `client_config` connects to its broker,
/// such as the mock broker a scenario owns.
pub(crate) async fn publish_kafka_record_through(
    mut client_config: ClientConfig,
    topic: &str,
    partition: Option<i32>,
    payload: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    let producer: FutureProducer = client_config
        .set("delivery.timeout.ms", "5000")
        .set("request.timeout.ms", "5000")
        .create()
        .map_err(io::Error::other)?;
    // Kafka topic creation and consumer assignment can lag slightly behind setup.
    let mut last_error = None;
    for attempt in 0..3 {
        let delivery = nervix_primitives::time::timeout(
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

/// The consumer groups the Kafka broker lists. A group with neither members nor committed offsets
/// does not exist on the broker, so the listing leaves it out.
fn kafka_listed_consumer_groups(
    dependencies: &DependencyEndpoints,
) -> io::Result<BTreeSet<String>> {
    let mut client_config = kafka_client_config(dependencies)?;
    let consumer: BaseConsumer = client_config
        .set(
            "group.id",
            format!("nervix-cucumber-admin-{}", Uuid::now_v7().as_simple()),
        )
        .create()
        .map_err(io::Error::other)?;
    let group_list = consumer
        .fetch_group_list(None, Duration::from_secs(10))
        .map_err(io::Error::other)?;
    let mut listed = BTreeSet::new();
    for group in group_list.groups() {
        listed.insert(group.name().to_string());
    }
    Ok(listed)
}

fn kafka_consumer_group_next_offset(
    mut client_config: ClientConfig,
    group: &str,
    topic: &str,
    partition: i32,
) -> io::Result<Option<i64>> {
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

/// Asserts, through a consumer that `client_config` connects to its broker, that consumer group
/// `group` commits a next offset of at least `threshold` for `topic` partition `partition` within
/// `duration`, or, when `should_reach` is false, that it stays below it for all of `duration`.
pub(crate) async fn assert_kafka_consumer_group_next_offset_through(
    client_config: ClientConfig,
    group: &str,
    topic: &str,
    partition: i32,
    threshold: i64,
    should_reach: bool,
    duration: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + duration;
    loop {
        nervix_primitives::task::consume_budget().await;
        let query_config = client_config.clone();
        let query_group = group.to_string();
        let query_topic = topic.to_string();
        let observation = nervix_primitives::task::spawn_blocking(move || {
            kafka_consumer_group_next_offset(query_config, &query_group, &query_topic, partition)
        })
        .await
        .map_err(io::Error::other)?;
        let actual = match observation {
            Ok(actual) => actual,
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => {
                sleep(POLL_INTERVAL).await;
                continue;
            }
        };
        let reached = actual.is_some_and(|offset| offset >= threshold);
        if should_reach && reached {
            return Ok(());
        }
        if !should_reach && reached {
            return Err(io::Error::other(format!(
                "Kafka consumer group '{group}' unexpectedly committed next offset {actual:?} for \
                 topic '{topic}' partition {partition}; expected it to remain below {threshold}"
            )));
        }
        if Instant::now() >= deadline {
            if should_reach {
                return Err(io::Error::other(format!(
                    "timed out waiting for Kafka consumer group '{group}' to commit next offset \
                     at least {threshold} for topic '{topic}' partition {partition}; observed \
                     {actual:?}"
                )));
            }
            return Ok(());
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// Creates `queue` on the scenario's SQS service, which serves in-process and real-process
/// clusters alike.
pub(crate) async fn ensure_sqs_queue(
    dependencies: &DependencyEndpoints,
    queue: &str,
) -> io::Result<()> {
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

/// Sends `payload` to `queue` on the scenario's SQS service.
pub(crate) async fn publish_sqs(
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
        nervix_primitives::task::consume_budget().await;
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

pub(crate) async fn observe_mqtt(
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

    let task = nervix_primitives::task::spawn(async move {
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
                        let message =
                            BrokerMessage::from_bytes(publish.payload.as_ref(), Vec::new());
                        if payload_tx.send(message).await.is_err() {
                            break;
                        }
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

    Ok(BrokerObserver::forwarded(payload_rx, task))
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

    let task = nervix_primitives::task::spawn(async move {
        let _connection = connection;
        while let Some(delivery) = consumer.next().await {
            nervix_primitives::task::consume_budget().await;
            let Ok(delivery) = delivery else {
                break;
            };
            let headers = rabbitmq_delivery_headers(&delivery.properties);
            let message = BrokerMessage::from_bytes(&delivery.data, headers);
            let _ = delivery.ack(BasicAckOptions::default()).await;
            if payload_tx.send(message).await.is_err() {
                break;
            }
        }
    });

    Ok(BrokerObserver::forwarded(payload_rx, task))
}

/// The string headers of one RabbitMQ delivery in name order, which is the order the AMQP field
/// table keeps. The emitter writes every header as a long string, so any other value type is one
/// no Nervix emitter wrote and is reported as its debug form.
fn rabbitmq_delivery_headers(properties: &BasicProperties) -> Vec<(String, String)> {
    let Some(table) = properties.headers().as_ref() else {
        return Vec::new();
    };
    let mut headers = Vec::with_capacity(table.inner().len());
    for (name, value) in table {
        let value = match value {
            lapin::types::AMQPValue::LongString(value) => {
                String::from_utf8_lossy(value.as_bytes()).to_string()
            }
            other => format!("{other:?}"),
        };
        headers.push((name.as_str().to_string(), value));
    }
    headers
}

async fn observe_redis(
    dependencies: &DependencyEndpoints,
    channel: &str,
) -> io::Result<BrokerObserver> {
    let client = redis::Client::open(dependencies.get(REDIS_ADDR)?).map_err(io::Error::other)?;
    let mut pubsub = client.get_async_pubsub().await.map_err(io::Error::other)?;
    pubsub.subscribe(channel).await.map_err(io::Error::other)?;
    let (payload_tx, payload_rx) = mpsc::channel(16);

    let task = nervix_primitives::task::spawn(async move {
        let mut messages = pubsub.on_message();
        while let Some(message) = messages.next().await {
            nervix_primitives::task::consume_budget().await;
            let message = BrokerMessage::from_bytes(message.get_payload_bytes(), Vec::new());
            if payload_tx.send(message).await.is_err() {
                break;
            }
        }
    });

    sleep(POLL_INTERVAL).await;
    Ok(BrokerObserver::forwarded(payload_rx, task))
}

async fn observe_kafka(
    dependencies: &DependencyEndpoints,
    topic: &str,
) -> io::Result<BrokerObserver> {
    observe_kafka_through(kafka_client_config(dependencies)?, topic).await
}

/// Observes `topic` through a consumer that `client_config` connects to its broker, such as the mock
/// broker a scenario owns.
pub(crate) async fn observe_kafka_through(
    mut client_config: ClientConfig,
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
    let consumer: StreamConsumer = client_config
        .set("group.id", &consumer_group)
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(io::Error::other)?;
    consumer.subscribe(&[topic]).map_err(io::Error::other)?;
    let (payload_tx, payload_rx) = mpsc::channel(16);

    let task = nervix_primitives::task::spawn(async move {
        let mut messages = consumer.stream();
        while let Some(message) = messages.next().await {
            nervix_primitives::task::consume_budget().await;
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
                    let key = message
                        .key()
                        .map(|key| String::from_utf8_lossy(key).to_string());
                    let _ = payload_tx
                        .send(BrokerMessage::from_bytes(bytes, headers).with_key(key))
                        .await;
                }
                Err(_) => continue,
            }
        }
    });

    Ok(BrokerObserver::forwarded(payload_rx, task))
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

            let task = nervix_primitives::task::spawn(async move {
                while let Some(message) = consumer.next().await {
                    nervix_primitives::task::consume_budget().await;
                    let Ok(message) = message else {
                        continue;
                    };
                    // Pulsar keeps message properties as an unordered map, so they are compared
                    // in name order.
                    let mut properties = message
                        .metadata()
                        .properties
                        .iter()
                        .map(|property| (property.key.clone(), property.value.clone()))
                        .collect::<Vec<_>>();
                    properties.sort();
                    let observed = BrokerMessage::from_bytes(&message.payload.data, properties)
                        .with_key(message.key());
                    let _ = consumer.ack(&message).await;
                    if payload_tx.send(observed).await.is_err() {
                        break;
                    }
                }
            });

            Ok(BrokerObserver::forwarded(payload_rx, task))
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

/// Creates `queue` on the scenario's SQS service and receives every message sent to it.
pub(crate) async fn observe_sqs(
    dependencies: &DependencyEndpoints,
    queue: &str,
) -> io::Result<BrokerObserver> {
    ensure_sqs_queue(dependencies, queue).await?;
    observe_created_sqs(dependencies, queue).await
}

/// Creates FIFO `queue` on the scenario's SQS service without content-based deduplication and
/// receives every message sent to it, so a message the service accepts names its deduplication id.
pub(crate) async fn observe_sqs_fifo_without_content_deduplication(
    dependencies: &DependencyEndpoints,
    queue: &str,
) -> io::Result<BrokerObserver> {
    let client = sqs_client(dependencies).await?;
    client
        .create_queue()
        .queue_name(queue)
        .attributes(QueueAttributeName::FifoQueue, "true")
        .send()
        .await
        .map_err(io::Error::other)?;
    observe_created_sqs(dependencies, queue).await
}

/// Observes `queue`, which already exists as the scenario created it.
async fn observe_created_sqs(
    dependencies: &DependencyEndpoints,
    queue: &str,
) -> io::Result<BrokerObserver> {
    let client = sqs_client(dependencies).await?;
    let queue_url = sqs_queue_url(&client, queue).await?;
    let (payload_tx, payload_rx) = mpsc::channel(16);

    let task = nervix_primitives::task::spawn(async move {
        while let Ok(response) = client
            .receive_message()
            .queue_url(queue_url.clone())
            .max_number_of_messages(1)
            .wait_time_seconds(1)
            .message_attribute_names("All")
            .message_system_attribute_names(MessageSystemAttributeName::MessageGroupId)
            .send()
            .await
        {
            nervix_primitives::task::consume_budget().await;
            let Some(message) = response.messages().first() else {
                continue;
            };
            let payload = message.body().unwrap_or_default().to_string();
            // SQS keeps message attributes as an unordered map, so they are compared in name
            // order.
            let mut attributes = Vec::new();
            let mut typed_attributes = Vec::new();
            if let Some(message_attributes) = message.message_attributes() {
                for (name, value) in message_attributes {
                    let text = value.string_value().unwrap_or_default().to_string();
                    attributes.push((name.clone(), text.clone()));
                    let typed_value = match value.binary_value() {
                        Some(bytes) => sqs_binary_attribute_hex(bytes.as_ref()),
                        None => text,
                    };
                    typed_attributes.push(TypedBrokerHeader {
                        name: name.clone(),
                        data_type: value.data_type().to_string(),
                        value: typed_value,
                    });
                }
            }
            attributes.sort();
            typed_attributes.sort();
            let group = match message.attributes() {
                Some(system) => system
                    .get(&MessageSystemAttributeName::MessageGroupId)
                    .cloned(),
                None => None,
            };
            if let Some(receipt_handle) = message.receipt_handle() {
                let _ = client
                    .delete_message()
                    .queue_url(queue_url.clone())
                    .receipt_handle(receipt_handle)
                    .send()
                    .await;
            }
            let observed = BrokerMessage::from_bytes(payload.as_bytes(), attributes)
                .with_group(group)
                .with_typed_headers(typed_attributes);
            if payload_tx.send(observed).await.is_err() {
                break;
            }
        }
    });

    Ok(BrokerObserver::forwarded(payload_rx, task))
}

/// A binary SQS attribute value as its bytes in lowercase hex, the way a scenario writes it.
fn sqs_binary_attribute_hex(bytes: &[u8]) -> String {
    let mut hex = String::new();
    for byte in bytes {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

async fn observe_nats(
    dependencies: &DependencyEndpoints,
    subject: &str,
) -> io::Result<BrokerObserver> {
    let observer = NatsObserver::connect(dependencies.get(NATS_ADDR)?, subject).await?;
    Ok(BrokerObserver::nats(observer))
}

async fn observe_zeromq(addr: &str) -> io::Result<BrokerObserver> {
    let mut socket = PullSocket::new();
    socket.bind(addr).await.map_err(io::Error::other)?;
    let (payload_tx, payload_rx) = mpsc::channel(1);

    let task = nervix_primitives::task::spawn(async move {
        loop {
            nervix_primitives::task::consume_budget().await;
            // zmq.rs's pull socket stops polling its peers as soon as one of them has nothing to
            // read, so a message another peer already delivered can wait until some peer sends
            // again. Abandoning a pending receive consumes nothing, so a receive that stays pending
            // for a poll interval starts over and reads what the stalled peer left behind.
            let received = match timeout(POLL_INTERVAL, socket.recv()).await {
                Ok(received) => received,
                Err(_) => continue,
            };
            let Ok(message) = received else {
                break;
            };
            let frames = message.into_vec();
            if let Some(frame) = frames.first()
                && payload_tx
                    .send(BrokerMessage::from_bytes(frame, Vec::new()))
                    .await
                    .is_err()
            {
                break;
            }
        }
    });

    sleep(POLL_INTERVAL).await;
    Ok(BrokerObserver::forwarded(payload_rx, task))
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
        nervix_primitives::task::consume_budget().await;
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

async fn nats_stream_message_count(
    dependencies: &DependencyEndpoints,
    stream: &str,
) -> io::Result<u64> {
    let client = nats_client(dependencies).await?;
    let mut stream_handle = async_nats::jetstream::new(client)
        .get_stream(stream)
        .await
        .map_err(io::Error::other)?;
    let info = stream_handle.info().await.map_err(io::Error::other)?;
    Ok(info.state.messages)
}

/// Reads every message JetStream `stream` holds when the read begins, through an ordered consumer
/// of its own, and returns the integer `field` of each message's JSON payload in the order the
/// stream stored them.
async fn nats_stream_field_values(
    dependencies: &DependencyEndpoints,
    stream: &str,
    field: &str,
) -> io::Result<Vec<i64>> {
    let client = nats_client(dependencies).await?;
    let mut stream_handle = async_nats::jetstream::new(client)
        .get_stream(stream)
        .await
        .map_err(io::Error::other)?;
    let held = stream_handle
        .info()
        .await
        .map_err(io::Error::other)?
        .state
        .messages;
    let mut values = Vec::new();
    if held == 0 {
        return Ok(values);
    }
    let consumer = stream_handle
        .create_consumer(async_nats::jetstream::consumer::pull::OrderedConfig::default())
        .await
        .map_err(io::Error::other)?;
    let mut messages = consumer.messages().await.map_err(io::Error::other)?;
    for read in 0..held {
        nervix_primitives::task::consume_budget().await;
        let Ok(next) = timeout(BROKER_TIMEOUT, messages.next()).await else {
            return Err(io::Error::other(format!(
                "timed out reading message {read} of the {held} NATS JetStream stream '{stream}' \
                 held"
            )));
        };
        let Some(message) = next else {
            return Err(io::Error::other(format!(
                "the ordered consumer of NATS JetStream stream '{stream}' ended after {read} of \
                 its {held} messages"
            )));
        };
        let message = message.map_err(io::Error::other)?;
        let payload: serde_json::Value =
            serde_json::from_slice(&message.payload).map_err(io::Error::other)?;
        let Some(value) = payload.get(field).and_then(serde_json::Value::as_i64) else {
            return Err(io::Error::other(format!(
                "a message of NATS JetStream stream '{stream}' carries no integer '{field}': \
                 {payload}"
            )));
        };
        values.push(value);
    }
    Ok(values)
}

/// Where a JetStream consumer stands: the messages it still holds for delivery, the deliveries it
/// awaits an acknowledgement for, and the stream sequence through which every message is
/// acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NatsConsumerPosition {
    pub(crate) pending: u64,
    pub(crate) awaiting_acknowledgement: usize,
    pub(crate) acknowledged_through: u64,
}

async fn provision_nats_pull_consumer(
    dependencies: &DependencyEndpoints,
    stream: &str,
    consumer: &str,
) -> io::Result<()> {
    let client = nats_client(dependencies).await?;
    let stream_handle = async_nats::jetstream::new(client)
        .get_stream(stream)
        .await
        .map_err(io::Error::other)?;
    stream_handle
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(consumer.to_string()),
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

async fn publish_nats_jetstream(
    dependencies: &DependencyEndpoints,
    subject: &str,
    payload: &str,
) -> io::Result<()> {
    let client = nats_client(dependencies).await?;
    // The context holds the step's only client, whose connection carries the stream's answer, so
    // it lives until that answer arrives.
    let context = async_nats::jetstream::new(client);
    let stored = context
        .publish(subject.to_string(), payload.as_bytes().to_vec().into())
        .await
        .map_err(io::Error::other)?;
    stored.await.map_err(io::Error::other)?;
    Ok(())
}

async fn nats_consumer_position(
    dependencies: &DependencyEndpoints,
    stream: &str,
    consumer: &str,
) -> io::Result<NatsConsumerPosition> {
    let client = nats_client(dependencies).await?;
    let info = async_nats::jetstream::new(client)
        .get_stream_no_info(stream)
        .await
        .map_err(io::Error::other)?
        .consumer_info(consumer)
        .await
        .map_err(io::Error::other)?;
    Ok(NatsConsumerPosition {
        pending: info.num_pending,
        awaiting_acknowledgement: info.num_ack_pending,
        acknowledged_through: info.ack_floor.stream_sequence,
    })
}

async fn wait_for_nats_consumer_position(
    dependencies: &DependencyEndpoints,
    stream: &str,
    consumer: &str,
    expected: NatsConsumerPosition,
    duration: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + duration;
    loop {
        nervix_primitives::task::consume_budget().await;
        let last_observation = match nats_consumer_position(dependencies, stream, consumer).await {
            Ok(position) if position == expected => return Ok(()),
            Ok(position) => format!("{position:?}"),
            Err(error) => format!("error {error}"),
        };
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for NATS JetStream consumer '{consumer}' of stream '{stream}' \
                 to stand at {expected:?}; last observation: {last_observation}"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn nats_stream_exists(dependencies: &DependencyEndpoints, stream: &str) -> io::Result<bool> {
    let client = nats_client(dependencies).await?;
    let mut names = async_nats::jetstream::new(client).stream_names();
    while let Some(name) = names.next().await {
        nervix_primitives::task::consume_budget().await;
        if name.map_err(io::Error::other)? == stream {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn nats_consumer_exists(
    dependencies: &DependencyEndpoints,
    stream: &str,
    consumer: &str,
) -> io::Result<bool> {
    let client = nats_client(dependencies).await?;
    let stream_handle = async_nats::jetstream::new(client)
        .get_stream_no_info(stream)
        .await
        .map_err(io::Error::other)?;
    let mut names = stream_handle.consumer_names();
    while let Some(name) = names.next().await {
        nervix_primitives::task::consume_budget().await;
        if name.map_err(io::Error::other)? == consumer {
            return Ok(true);
        }
    }
    Ok(false)
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
