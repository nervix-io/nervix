use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::OpenOptions,
    io,
    net::{IpAddr, Ipv4Addr, TcpListener},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, LazyLock, OnceLock},
    time::{Duration, Instant, SystemTime},
};

use async_nats::Client as NatsClient;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::Client as SqsClient;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use fjall::Database;
use futures_util::SinkExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use nervix::{
    application::{Application, InternalTransportMode, init_tracing_to_file},
    memory_pressure::MemoryPressureConfig,
    runtime::{DEFAULT_TEMP_DIR, EmitterFaultInjector, IngestorFaultInjector, RuntimeTestHooks},
};
use nervix_client_core::{Client, CommandOutcomeKind, ConnectOptions, TlsRequirement};
pub use nervix_proto as proto;
use parking_lot::Mutex;
use proto::{
    CommandRequest, ServerEventLevel, SessionRequest, session_response::Event,
    session_service_client::SessionServiceClient,
};
use pulsar::{
    ConsumerOptions as PulsarConsumerOptions, Pulsar, SubType as PulsarSubType, TokioExecutor,
    consumer::InitialPosition as PulsarInitialPosition,
};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    error::RDKafkaErrorCode,
    message::{Header as KafkaHeader, Headers, Message, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};
use redis::AsyncCommands;
use rumqttc::{AsyncClient, Event as MqttEvent, Incoming, MqttOptions, QoS};
use rustls::{
    ClientConfig as RustlsClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use rustls_pki_types::pem::PemObject;
use tempfile::{TempDir, tempdir};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_rustls::TlsConnector;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_tungstenite::{
    client_async, connect_async,
    tungstenite::{Message as WsMessage, client::IntoClientRequest, http::HeaderValue},
};
use tokio_util::sync::CancellationToken;
use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Certificate, ClientTlsConfig, Endpoint},
};
use uuid::Uuid;
use zeromq::{PullSocket, PushSocket, Socket, SocketRecv, SocketSend};

const HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const STATUS_TIMEOUT: Duration = Duration::from_secs(40);
const BROKER_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(45);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const NODE_START_ATTEMPTS: usize = 8;
const KAFKA_ADDR: &str = "127.0.0.1:9092";
const PULSAR_ADDR: &str = "pulsar://127.0.0.1:6650";
const PULSAR_TLS_ADDR: &str = "pulsar+ssl://127.0.0.1:6651";
const RABBITMQ_ADDR: &str = "amqp://guest:guest@127.0.0.1:5672/%2f";
const REDIS_ADDR: &str = "redis://127.0.0.1:6379/";
const MQTT_HOST: &str = "127.0.0.1";
const MQTT_PORT: u16 = 1883;
const NATS_ADDR: &str = "nats://127.0.0.1:4222";
const NATS_TLS_ADDR: &str = "tls://127.0.0.1:4223";
const SQS_ENDPOINT: &str = "http://127.0.0.1:9324";
const SQS_TLS_ENDPOINT: &str = "https://127.0.0.1:9325";
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
static RESERVED_TEST_PORTS: LazyLock<Mutex<BTreeSet<u16>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));
static TEST_LOG_TRUNCATED: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

pub(crate) fn next_port() -> io::Result<u16> {
    let mut ports = next_ports(1)?;
    Ok(ports.remove(0))
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

fn next_ports(count: usize) -> io::Result<Vec<u16>> {
    let mut listeners = Vec::with_capacity(count);
    let mut ports = Vec::with_capacity(count);
    while ports.len() < count {
        let listener = TcpListener::bind((HOST, 0))?;
        let port = listener.local_addr()?.port();
        let mut reserved = RESERVED_TEST_PORTS.lock();
        if !reserved.insert(port) {
            continue;
        }
        drop(reserved);
        ports.push(port);
        listeners.push(listener);
    }
    Ok(ports)
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
            && (value - expected_value as f64).abs() < f64::EPSILON
        {
            return true;
        }
    }
    false
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

fn dev_tls_ca_pem() -> io::Result<Vec<u8>> {
    ensure_dev_tls_assets()?;
    nervix_interconnect::install_rustls_crypto_provider();
    std::fs::read(dev_tls_ca_path()?).map_err(io::Error::other)
}

fn dev_tls_ca_path() -> io::Result<PathBuf> {
    ensure_dev_tls_assets()?;
    Ok(std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tls/dev")
        .join("ca.pem"))
}

fn kafka_client_config() -> io::Result<ClientConfig> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", KAFKA_ADDR)
        .set("broker.address.family", "v4");
    Ok(config)
}

pub(crate) fn client_connect_options(server: &str) -> io::Result<ConnectOptions> {
    if server.starts_with("https://") {
        Ok(ConnectOptions {
            tls_requirement: Some(TlsRequirement::Required),
            ca_certificate_pem: Some(dev_tls_ca_pem()?),
            username: Some(TEST_AUTH_USERNAME.to_string()),
            password: Some(TEST_AUTH_PASSWORD.to_string()),
        })
    } else {
        Ok(ConnectOptions::default().with_basic_auth(TEST_AUTH_USERNAME, TEST_AUTH_PASSWORD))
    }
}

#[derive(Debug)]
pub(crate) struct Cluster {
    _root_dir: TempDir,
    nodes: BTreeMap<String, NodeHandle>,
    runtime_test_hooks: RuntimeTestHooks,
}

#[derive(Debug, Clone)]
pub(crate) struct TestClusterConfig {
    pub replica_count: usize,
    pub state_snapshot_interval: Duration,
    pub grpc_mode: InternalTransportMode,
    pub cluster_api_mode: InternalTransportMode,
    pub interconnect_mode: InternalTransportMode,
    pub graceful_shutdown_drain: bool,
    pub drain_timeout: Duration,
    pub memory_pressure: Option<MemoryPressureConfig>,
    pub temp_dir: Option<PathBuf>,
}

impl Default for TestClusterConfig {
    fn default() -> Self {
        Self {
            replica_count: TEST_REPLICA_COUNT,
            state_snapshot_interval: TEST_STATE_SNAPSHOT_INTERVAL,
            grpc_mode: InternalTransportMode::Http,
            cluster_api_mode: InternalTransportMode::Http,
            interconnect_mode: InternalTransportMode::Http,
            graceful_shutdown_drain: false,
            drain_timeout: Duration::from_secs(30),
            memory_pressure: None,
            temp_dir: None,
        }
    }
}

impl Cluster {
    pub(crate) async fn start_with_config(
        node_count: usize,
        runtime_test_hooks: RuntimeTestHooks,
        config: TestClusterConfig,
    ) -> io::Result<Self> {
        assert!(node_count >= 1, "cluster must contain at least one node");
        truncate_test_log_once()?;
        init_tracing_to_file(std::path::Path::new(TEST_LOG_FILE))?;
        let root_dir = tempdir()?;
        let mut nodes = BTreeMap::new();

        for index in 1..=node_count {
            let node_id = format!("node-{index}");
            let spec = NodeSpec::new(&root_dir, &node_id, index == 1)?;
            nodes.insert(
                node_id.clone(),
                NodeHandle::new(spec, runtime_test_hooks.clone(), config.clone()),
            );
        }

        let mut cluster = Self {
            _root_dir: root_dir,
            runtime_test_hooks,
            nodes,
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
        self.start_node("node-1").await?;
        let bootstrap_cluster_addr = self
            .nodes
            .get("node-1")
            .expect("bootstrap node exists")
            .spec
            .cluster_addr();
        for (node_id, node) in &mut self.nodes {
            if node_id != "node-1" {
                node.spec.bootstrap_host = Some(bootstrap_cluster_addr.clone());
            }
        }
        for index in 2..=node_count {
            self.start_node(&format!("node-{index}")).await?;
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
        }
        Ok(())
    }

    pub(crate) async fn start_node(&mut self, node_id: &str) -> io::Result<()> {
        let handle = self
            .nodes
            .get_mut(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let mut last_error = None;
        for attempt in 1..=NODE_START_ATTEMPTS {
            handle.start()?;
            match handle.wait_until_ready().await {
                Ok(()) => {
                    last_error = None;
                    break;
                }
                Err(error) => {
                    let message = error.to_string();
                    handle.stop().await?;
                    last_error = Some(error);
                    if attempt == NODE_START_ATTEMPTS {
                        break;
                    }
                    handle.spec.reallocate_ports()?;
                    eprintln!(
                        "retrying node '{node_id}' startup after attempt {attempt} failed: \
                         {message}"
                    );
                }
            }
        }
        if let Some(error) = last_error {
            return Err(error);
        }
        let mut leader_applied = None;
        for probe_node_id in self
            .nodes
            .keys()
            .filter(|existing_id| existing_id.as_str() != node_id)
        {
            let Ok(probe_status) = self.show_status(probe_node_id).await else {
                continue;
            };
            let Some(leader_id) = probe_status
                .current_leader
                .filter(|leader_id| leader_id != node_id)
            else {
                continue;
            };
            let Ok(leader_status) = self.show_status(&leader_id).await else {
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

    pub(crate) async fn stop_node(&mut self, node_id: &str) -> io::Result<()> {
        let handle = self
            .nodes
            .get_mut(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle.stop().await
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

    pub(crate) async fn shutdown_for_teardown(&mut self) -> Vec<String> {
        let node_ids = self.nodes.keys().cloned().collect::<Vec<_>>();
        for node_id in &node_ids {
            if let Some(handle) = self.nodes.get_mut(node_id) {
                handle.request_stop();
            }
        }
        let mut errors = Vec::new();
        for node_id in node_ids {
            let handle = self
                .nodes
                .get_mut(&node_id)
                .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
            if let Err(error) = handle.wait_stopped().await {
                errors.push(format!("node '{node_id}': {error}"));
            }
        }
        for node in self.nodes.values_mut() {
            node.spec.release_ports();
        }
        errors
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

        self.start_node("node-1").await?;
        for node_id in node_ids
            .iter()
            .filter(|node_id| node_id.as_str() != "node-1")
        {
            self.start_node(node_id).await?;
        }

        self.wait_for_any_leader("node-1").await?;
        if node_ids.len() > 1 {
            for node_id in &node_ids {
                self.wait_for_any_leader(node_id).await?;
            }
            let voter_refs = node_ids.iter().map(String::as_str).collect::<Vec<_>>();
            self.wait_for_voters("node-1", &voter_refs).await?;
            self.wait_for_consistent_leader_on_all_nodes().await?;
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
        let deadline = Instant::now() + STATUS_TIMEOUT;
        loop {
            tokio::task::consume_budget().await;
            match kafka_consumer_group_member_count(group) {
                Ok(actual) if actual == expected => return Ok(()),
                Ok(_) => {}
                Err(error) if Instant::now() >= deadline => return Err(error),
                Err(_) => {}
            }
            if Instant::now() >= deadline {
                let actual = kafka_consumer_group_member_count(group)?;
                return Err(io::Error::other(format!(
                    "timed out waiting for kafka consumer group '{group}' to have {expected} \
                     members, got {actual}"
                )));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    pub(crate) async fn wait_for_rabbitmq_queue_consumers(
        &self,
        queue: &str,
        expected: usize,
    ) -> io::Result<()> {
        let deadline = Instant::now() + STATUS_TIMEOUT;
        loop {
            tokio::task::consume_budget().await;
            match rabbitmq_queue_consumer_count(queue).await {
                Ok(actual) if actual == expected => return Ok(()),
                Ok(_) => {}
                Err(error) if Instant::now() >= deadline => return Err(error),
                Err(_) => {}
            }
            if Instant::now() >= deadline {
                let actual = rabbitmq_queue_consumer_count(queue).await?;
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
        wait_for_redis_channel_subscribers(channel, expected).await
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

    pub(crate) fn web_console_url(&self, node_id: &str) -> io::Result<String> {
        let handle = self
            .nodes
            .get(node_id)
            .ok_or_else(|| io::Error::other(format!("unknown node '{node_id}'")))?;
        Ok(handle.spec.web_console_url())
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
        publish_mqtt(topic, payload).await
    }

    pub(crate) async fn publish_mqtt_qos1(&self, topic: &str, payload: &str) -> io::Result<()> {
        publish_mqtt_with_qos(topic, payload, QoS::AtLeastOnce, true).await
    }

    pub(crate) async fn publish_mqtt_burst(
        &self,
        topic: &str,
        payload: &str,
        count: usize,
    ) -> io::Result<()> {
        publish_mqtt_burst(topic, payload, count).await
    }

    pub(crate) async fn publish_mqtt_qos1_burst(
        &self,
        topic: &str,
        payload: &str,
        count: usize,
    ) -> io::Result<()> {
        publish_mqtt_burst_with_qos(topic, payload, count, QoS::AtLeastOnce).await
    }

    pub(crate) async fn ensure_rabbitmq_queue(&self, queue: &str) -> io::Result<()> {
        ensure_rabbitmq_queue(queue).await
    }

    pub(crate) async fn publish_rabbitmq(&self, queue: &str, payload: &str) -> io::Result<()> {
        publish_rabbitmq(queue, payload).await
    }

    pub(crate) async fn publish_redis(&self, channel: &str, payload: &str) -> io::Result<()> {
        publish_redis(channel, payload).await
    }

    pub(crate) async fn publish_redis_burst(
        &self,
        channel: &str,
        payload: &str,
        count: usize,
    ) -> io::Result<()> {
        publish_redis_burst(channel, payload, count).await
    }

    pub(crate) async fn publish_pulsar(&self, topic: &str, payload: &str) -> io::Result<()> {
        publish_pulsar(topic, payload).await
    }

    pub(crate) async fn publish_pulsar_tls(&self, topic: &str, payload: &str) -> io::Result<()> {
        publish_pulsar_tls(topic, payload).await
    }

    pub(crate) async fn publish_kafka(&self, topic: &str, payload: &str) -> io::Result<()> {
        publish_kafka(topic, payload).await
    }

    pub(crate) async fn publish_kafka_with_headers(
        &self,
        topic: &str,
        payload: &str,
        headers: &[(&str, &str)],
    ) -> io::Result<()> {
        publish_kafka_with_headers(topic, payload, headers).await
    }

    pub(crate) async fn publish_kafka_burst(
        &self,
        topic: &str,
        payload: &str,
        count: usize,
    ) -> io::Result<()> {
        publish_kafka_burst(topic, payload, count).await
    }

    pub(crate) async fn publish_kafka_partition(
        &self,
        topic: &str,
        partition: i32,
        payload: &str,
    ) -> io::Result<()> {
        publish_kafka_partition(topic, partition, payload).await
    }

    pub(crate) async fn ensure_kafka_topic_partitions(
        &self,
        topic: &str,
        partitions: i32,
    ) -> io::Result<()> {
        ensure_kafka_topic_partitions(topic, partitions).await
    }

    pub(crate) async fn reset_kafka_topic_partitions(
        &self,
        topic: &str,
        partitions: i32,
    ) -> io::Result<()> {
        reset_kafka_topic_partitions(topic, partitions).await
    }

    pub(crate) async fn ensure_sqs_queue(&self, queue: &str) -> io::Result<()> {
        ensure_sqs_queue(queue).await
    }

    pub(crate) async fn ensure_sqs_queue_tls(&self, queue: &str) -> io::Result<()> {
        ensure_sqs_queue_tls(queue).await
    }

    pub(crate) async fn publish_sqs(&self, queue: &str, payload: &str) -> io::Result<()> {
        publish_sqs(queue, payload).await
    }

    pub(crate) async fn publish_sqs_tls(&self, queue: &str, payload: &str) -> io::Result<()> {
        publish_sqs_tls(queue, payload).await
    }

    pub(crate) async fn publish_nats(&self, subject: &str, payload: &str) -> io::Result<()> {
        publish_nats(subject, payload).await
    }

    pub(crate) async fn publish_nats_with_headers(
        &self,
        subject: &str,
        payload: &str,
        headers: &[(&str, &str)],
    ) -> io::Result<()> {
        publish_nats_with_headers(subject, payload, headers).await
    }

    pub(crate) async fn publish_nats_tls(&self, subject: &str, payload: &str) -> io::Result<()> {
        publish_nats_tls(subject, payload).await
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

    pub(crate) async fn exchange_websocket_text(
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
        exchange_websocket_text(&handle.spec, host, path, actions).await
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

    pub(crate) async fn observe_mqtt(&self, topic: &str) -> io::Result<BrokerObserver> {
        observe_mqtt(topic).await
    }

    pub(crate) async fn observe_rabbitmq(&self, queue: &str) -> io::Result<BrokerObserver> {
        observe_rabbitmq(queue).await
    }

    pub(crate) async fn observe_redis(&self, channel: &str) -> io::Result<BrokerObserver> {
        observe_redis(channel).await
    }

    pub(crate) async fn observe_kafka(&self, topic: &str) -> io::Result<BrokerObserver> {
        observe_kafka(topic).await
    }

    pub(crate) async fn observe_pulsar(&self, topic: &str) -> io::Result<BrokerObserver> {
        observe_pulsar(topic).await
    }

    pub(crate) async fn observe_pulsar_tls(&self, topic: &str) -> io::Result<BrokerObserver> {
        observe_pulsar_tls(topic).await
    }

    pub(crate) async fn observe_sqs(&self, queue: &str) -> io::Result<BrokerObserver> {
        observe_sqs(queue).await
    }

    pub(crate) async fn observe_nats(&self, subject: &str) -> io::Result<BrokerObserver> {
        observe_nats(subject).await
    }

    pub(crate) async fn observe_zeromq(&self, addr: &str) -> io::Result<BrokerObserver> {
        observe_zeromq(addr).await
    }

    pub(crate) fn fail_emitter_on_all_nodes(&self, emitter: &str) {
        for handle in self.nodes.values() {
            handle.fail_emitter(emitter);
        }
    }

    pub(crate) fn stall_emitter_on_all_nodes(&self, emitter: &str) {
        for handle in self.nodes.values() {
            handle.stall_emitter(emitter);
        }
    }

    pub(crate) fn clear_emitter_fault_on_all_nodes(&self, emitter: &str) {
        for handle in self.nodes.values() {
            handle.clear_emitter_fault(emitter);
        }
    }

    pub(crate) fn fail_ingestor_on_all_nodes(&self, ingestor: &str) {
        for handle in self.nodes.values() {
            handle.fail_ingestor(ingestor);
        }
    }

    pub(crate) fn clear_ingestor_fault_on_all_nodes(&self, ingestor: &str) {
        for handle in self.nodes.values() {
            handle.clear_ingestor_fault(ingestor);
        }
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

        while start.elapsed() < STATUS_TIMEOUT {
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

        while start.elapsed() < STATUS_TIMEOUT {
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
    ) -> io::Result<()> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let url = format!("http://{}/metrics", handle.spec.observability_addr());
        let client = reqwest::Client::new();
        let start = Instant::now();
        let mut last_response = None;
        let mut last_error = None;
        let mut last_matching_lines = Vec::new();

        while start.elapsed() < STATUS_TIMEOUT {
            tokio::task::consume_budget().await;
            match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    match response.text().await {
                        Ok(body) => {
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

    pub(crate) async fn current_leader(&self, node_id: &str) -> io::Result<Option<String>> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle
            .show_cluster_status()
            .await
            .map(|status| status.current_leader)
    }

    pub(crate) async fn wait_for_consistent_leader_on_all_nodes(&self) -> io::Result<String> {
        let start = Instant::now();
        let mut last_statuses = BTreeMap::new();
        let mut stable_leader = None;
        let mut stable_count = 0u8;

        while start.elapsed() < STATUS_TIMEOUT {
            tokio::task::consume_budget().await;
            let mut leader = None;
            let mut consistent = true;

            for node_id in self.nodes.keys() {
                match self.show_status(node_id).await {
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
                        last_statuses.insert(node_id.clone(), status);
                    }
                    Err(_) => {
                        consistent = false;
                    }
                }
            }

            if consistent
                && let Some(leader_id) = leader
                && let Some(leader_status) = last_statuses.get(&leader_id)
                && leader_status.raft_state.as_deref() == Some("Leader")
            {
                if stable_leader.as_deref() == Some(leader_id.as_str()) {
                    stable_count = stable_count.saturating_add(1);
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

            sleep(POLL_INTERVAL).await;
        }

        let mut message = "timed out waiting for a consistent cluster leader".to_string();
        for (node_id, status) in last_statuses {
            message.push_str(&format!("\nlast status for '{node_id}':\n{}", status.raw));
        }
        Err(io::Error::other(message))
    }

    pub(crate) async fn collect_status_snapshots(
        &self,
    ) -> BTreeMap<String, Result<String, String>> {
        let mut snapshots = BTreeMap::new();
        for node_id in self.nodes.keys() {
            let result = match self.show_status(node_id).await {
                Ok(status) => Ok(status.raw),
                Err(error) => Err(error.to_string()),
            };
            snapshots.insert(node_id.clone(), result);
        }
        snapshots
    }

    async fn show_status(&self, node_id: &str) -> io::Result<ClusterStatus> {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        handle.show_cluster_status().await
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
        self.runtime_test_hooks
            .request_leadership_transfer(from_node_id.to_string(), to_node_id.to_string());
    }
    async fn wait_until<F>(&self, node_id: &str, predicate: F) -> io::Result<()>
    where
        F: Fn(&ClusterStatus) -> bool,
    {
        let handle = self
            .nodes
            .get(node_id)
            .unwrap_or_else(|| panic!("unknown node '{node_id}'"));
        let start = Instant::now();
        let mut last_status = None;
        let mut last_error = None;

        while start.elapsed() < STATUS_TIMEOUT {
            match handle.show_cluster_status().await {
                Ok(status) => {
                    if predicate(&status) {
                        return Ok(());
                    }
                    last_status = Some(status);
                }
                Err(err) => last_error = Some(err),
            }
            sleep(POLL_INTERVAL).await;
        }

        let mut message = format!("timed out waiting for cluster state via node '{node_id}'");
        if let Some(status) = last_status {
            message.push_str(&format!("\nlast status:\n{}", status.raw));
        }
        if let Some(err) = last_error {
            message.push_str(&format!("\nlast error: {err}"));
        }
        Err(io::Error::other(message))
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for handle in self.nodes.values_mut() {
            handle.abort();
        }
    }
}

#[derive(Debug)]
struct NodeHandle {
    spec: NodeSpec,
    runtime_test_hooks: RuntimeTestHooks,
    config: TestClusterConfig,
    emitter_faults: Arc<EmitterFaultInjector>,
    ingestor_faults: Arc<IngestorFaultInjector>,
    failure: Arc<Mutex<Option<String>>>,
    task: Option<JoinHandle<()>>,
    shutdown: Option<CancellationToken>,
}

impl NodeHandle {
    fn new(
        spec: NodeSpec,
        runtime_test_hooks: RuntimeTestHooks,
        config: TestClusterConfig,
    ) -> Self {
        let emitter_faults = runtime_test_hooks.emitter_faults.clone();
        let ingestor_faults = runtime_test_hooks.ingestor_faults.clone();
        Self {
            spec,
            runtime_test_hooks,
            config,
            emitter_faults,
            ingestor_faults,
            failure: Arc::new(Mutex::new(None)),
            task: None,
            shutdown: None,
        }
    }

    fn start(&mut self) -> io::Result<()> {
        if self.task.is_some() {
            return Ok(());
        }
        if self.config.grpc_mode == InternalTransportMode::Https
            || self.config.cluster_api_mode == InternalTransportMode::Https
            || self.config.interconnect_mode == InternalTransportMode::Https
        {
            ensure_dev_tls_assets()?;
        }

        *self.failure.lock() = None;
        let shutdown = CancellationToken::new();
        let db_path = self.spec.db_path()?;
        let application = Application::builder()
            .addr(parse_addr(&self.spec.grpc_addr())?)
            .grpc_mode(self.config.grpc_mode)
            .grpc_https_listen_addr(Some(parse_addr(&self.spec.grpc_https_addr())?))
            .http_listen_addr(parse_addr(&self.spec.http_addr())?)
            .https_listen_addr(parse_addr(&self.spec.https_addr())?)
            .observability_listen_addr(parse_addr(&self.spec.observability_addr())?)
            .web_console_listen_addr(parse_addr(&self.spec.web_console_addr())?)
            .web_console_advertise_addr(Some(parse_addr(&self.spec.web_console_addr())?.into()))
            .cluster_id("cucumber".to_string())
            .node_id(self.spec.node_id.clone())
            .grpc_advertise_addr(parse_addr(&self.spec.grpc_addr())?.into())
            .grpc_https_advertise_addr(Some(parse_addr(&self.spec.grpc_https_addr())?.into()))
            .cluster_listen_addr(parse_addr(&self.spec.cluster_addr())?)
            .cluster_advertise_addr(parse_addr(&self.spec.cluster_addr())?.into())
            .cluster_api_mode(self.config.cluster_api_mode)
            .cluster_api_listen_addr(parse_addr(&self.spec.cluster_api_addr())?)
            .cluster_api_advertise_addr(parse_addr(&self.spec.cluster_api_addr())?.into())
            .cluster_api_https_listen_addr(Some(parse_addr(&self.spec.cluster_api_https_addr())?))
            .cluster_api_https_advertise_addr(Some(
                parse_addr(&self.spec.cluster_api_https_addr())?.into(),
            ))
            .interconnect_mode(self.config.interconnect_mode)
            .interconnect_listen_addr(parse_addr(&self.spec.interconnect_addr())?)
            .interconnect_advertise_addr(parse_addr(&self.spec.interconnect_addr())?.into())
            .interconnect_https_listen_addr(Some(parse_addr(&self.spec.interconnect_https_addr())?))
            .interconnect_https_advertise_addr(Some(
                parse_addr(&self.spec.interconnect_https_addr())?.into(),
            ))
            .allow_bootstrap(self.spec.allow_bootstrap)
            .default_user(TEST_AUTH_USERNAME.to_string())
            .init_default_user_password(Some(TEST_AUTH_PASSWORD.to_string()))
            .node_unavailability_timeout(Duration::from_secs(10))
            .raft_heartbeat_interval(TEST_RAFT_HEARTBEAT_INTERVAL)
            .raft_election_timeout_min(TEST_RAFT_ELECTION_TIMEOUT_MIN)
            .raft_election_timeout_max(TEST_RAFT_ELECTION_TIMEOUT_MAX)
            .replica_count(self.config.replica_count)
            .state_snapshot_interval(self.config.state_snapshot_interval)
            .memory_pressure(self.config.memory_pressure)
            .cluster_bootstrap_host(self.spec.bootstrap_host.clone())
            .db_path(db_path)
            .temp_dir(
                self.config
                    .temp_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(DEFAULT_TEMP_DIR)),
            )
            .runtime_test_hooks(self.runtime_test_hooks.clone())
            .shutdown(shutdown.clone())
            .graceful_shutdown_drain(self.config.graceful_shutdown_drain)
            .drain_timeout(self.config.drain_timeout)
            .build();
        let failure = self.failure.clone();
        self.shutdown = Some(shutdown);
        self.task = Some(tokio::spawn(async move {
            if let Err(err) = application.run().await {
                *failure.lock() = Some(format!("{err:?}"));
            }
        }));
        Ok(())
    }

    async fn stop(&mut self) -> io::Result<()> {
        self.request_stop();
        self.wait_stopped().await
    }

    fn request_stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.cancel();
        }
    }

    async fn wait_stopped(&mut self) -> io::Result<()> {
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        let task_result = match timeout(SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) if err.is_cancelled() => Ok(()),
            Ok(Err(err)) => Err(io::Error::other(err)),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(io::Error::other("timed out waiting for node shutdown"))
            }
        };
        if task_result.is_ok() {
            self.ensure_database_unlocked().await?;
        }
        task_result
    }

    async fn ensure_database_unlocked(&self) -> io::Result<()> {
        let db_path = self.spec.db_path()?;
        database_opens(db_path).await.map_err(|error| {
            io::Error::other(format!(
                "node '{}' returned before releasing its database lock: {error}",
                self.spec.node_id
            ))
        })
    }

    async fn wait_until_ready(&mut self) -> io::Result<()> {
        let grpc_uri = self.spec.grpc_uri(self.config.grpc_mode);
        timeout(STARTUP_TIMEOUT, async {
            loop {
                tokio::task::consume_budget().await;
                if self.exited() {
                    return Err(io::Error::other(format!(
                        "node '{}' exited during startup\nfailure:\n{}",
                        self.spec.node_id,
                        self.failure_message()
                    )));
                }

                if server_accepts_commands(&grpc_uri).await? {
                    return Ok(());
                }

                sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .map_err(|_| {
            io::Error::other(format!(
                "node '{}' did not become ready in time\nfailure:\n{}",
                self.spec.node_id,
                self.failure_message()
            ))
        })?
    }

    async fn show_cluster_status(&self) -> io::Result<ClusterStatus> {
        let output = run_command(
            &self.spec.grpc_uri(self.config.grpc_mode),
            "SHOW CLUSTER STATUS;",
        )
        .await?;
        Ok(ClusterStatus::parse(output))
    }

    fn exited(&self) -> bool {
        self.task.as_ref().is_some_and(JoinHandle::is_finished)
    }

    fn failure_message(&self) -> String {
        self.failure
            .lock()
            .clone()
            .unwrap_or_else(|| "node task stopped without error details".to_string())
    }

    fn abort(&mut self) {
        self.shutdown = None;
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }

    fn fail_emitter(&self, emitter: &str) {
        self.emitter_faults.fail_emitter(emitter);
    }

    fn stall_emitter(&self, emitter: &str) {
        self.emitter_faults.stall_emitter(emitter);
    }

    fn clear_emitter_fault(&self, emitter: &str) {
        self.emitter_faults.clear_emitter(emitter);
    }

    fn fail_ingestor(&self, ingestor: &str) {
        self.ingestor_faults.fail_ingestor(ingestor);
    }

    fn clear_ingestor_fault(&self, ingestor: &str) {
        self.ingestor_faults.clear_ingestor(ingestor);
    }
}

#[derive(Debug)]
struct NodeSpec {
    node_id: String,
    base_dir: PathBuf,
    allow_bootstrap: bool,
    bootstrap_host: Option<String>,
    grpc_port: u16,
    grpc_https_port: u16,
    http_port: u16,
    https_port: u16,
    observability_port: u16,
    web_console_port: u16,
    cluster_port: u16,
    cluster_api_port: u16,
    cluster_api_https_port: u16,
    interconnect_port: u16,
    interconnect_https_port: u16,
}

struct NodePorts {
    grpc: u16,
    grpc_https: u16,
    http: u16,
    https: u16,
    observability: u16,
    web_console: u16,
    cluster: u16,
    cluster_api: u16,
    cluster_api_https: u16,
    interconnect: u16,
    interconnect_https: u16,
}

impl NodePorts {
    fn allocate() -> io::Result<Self> {
        let mut ports = next_ports(11)?.into_iter();
        Ok(Self {
            grpc: ports.next().expect("allocated gRPC port"),
            grpc_https: ports.next().expect("allocated gRPC HTTPS port"),
            http: ports.next().expect("allocated HTTP port"),
            https: ports.next().expect("allocated HTTPS port"),
            observability: ports.next().expect("allocated observability port"),
            web_console: ports.next().expect("allocated web console port"),
            cluster: ports.next().expect("allocated cluster port"),
            cluster_api: ports.next().expect("allocated cluster API port"),
            cluster_api_https: ports.next().expect("allocated cluster API HTTPS port"),
            interconnect: ports.next().expect("allocated interconnect port"),
            interconnect_https: ports.next().expect("allocated interconnect HTTPS port"),
        })
    }
}

impl NodeSpec {
    fn new(root: &TempDir, node_id: &str, allow_bootstrap: bool) -> io::Result<Self> {
        let base_dir = root.path().join(node_id);
        std::fs::create_dir_all(&base_dir)?;
        let ports = NodePorts::allocate()?;

        Ok(Self {
            node_id: node_id.to_string(),
            base_dir,
            allow_bootstrap,
            bootstrap_host: None,
            grpc_port: ports.grpc,
            grpc_https_port: ports.grpc_https,
            http_port: ports.http,
            https_port: ports.https,
            observability_port: ports.observability,
            web_console_port: ports.web_console,
            cluster_port: ports.cluster,
            cluster_api_port: ports.cluster_api,
            cluster_api_https_port: ports.cluster_api_https,
            interconnect_port: ports.interconnect,
            interconnect_https_port: ports.interconnect_https,
        })
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
        self.cluster_port = ports.cluster;
        self.cluster_api_port = ports.cluster_api;
        self.cluster_api_https_port = ports.cluster_api_https;
        self.interconnect_port = ports.interconnect;
        self.interconnect_https_port = ports.interconnect_https;
        Ok(())
    }

    fn release_ports(&mut self) {
        let mut reserved = RESERVED_TEST_PORTS.lock();
        for port in [
            self.grpc_port,
            self.grpc_https_port,
            self.http_port,
            self.https_port,
            self.observability_port,
            self.web_console_port,
            self.cluster_port,
            self.cluster_api_port,
            self.cluster_api_https_port,
            self.interconnect_port,
            self.interconnect_https_port,
        ] {
            reserved.remove(&port);
        }
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

    fn cluster_addr(&self) -> String {
        format!("{HOST}:{}", self.cluster_port)
    }

    fn cluster_api_addr(&self) -> String {
        format!("{HOST}:{}", self.cluster_api_port)
    }

    fn cluster_api_https_addr(&self) -> String {
        format!("{HOST}:{}", self.cluster_api_https_port)
    }

    fn interconnect_addr(&self) -> String {
        format!("{HOST}:{}", self.interconnect_port)
    }

    fn interconnect_https_addr(&self) -> String {
        format!("{HOST}:{}", self.interconnect_https_port)
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

#[derive(Debug, Clone)]
pub(crate) enum WebsocketExchangeAction {
    ExpectText(String),
    SendText(String),
}

async fn exchange_websocket_text(
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
                let message = timeout(
                    Duration::from_secs(5),
                    futures_util::StreamExt::next(&mut relay),
                )
                .await
                .map_err(|_| io::Error::other("timed out waiting for websocket text frame"))?
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
        }
    }
    let _ = relay.close(None).await;
    Ok(())
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
    let client = reqwest::Client::new();
    let mut request = client
        .post(spec.http_uri(path))
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
    let connector = TlsConnector::from(Arc::new(client_config));
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

#[derive(Debug, Clone)]
pub(crate) struct TestSubscriptionEvent {
    pub payload: String,
}

#[derive(Debug)]
pub(crate) struct RawTestSession {
    domain: String,
    request_tx: mpsc::Sender<SessionRequest>,
    response: tonic::Streaming<proto::SessionResponse>,
    pending_subscriptions: VecDeque<proto::SubscriptionEvent>,
}

pub(crate) enum TestSession {
    Raw(Box<RawTestSession>),
}

#[derive(Debug, Clone)]
pub(crate) struct TestServerEvent {
    pub(crate) level: i32,
    pub(crate) message: String,
}

impl std::fmt::Debug for TestSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Raw(_) => f.write_str("TestSession::Raw(..)"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct BrokerMessage {
    pub(crate) payload: String,
    pub(crate) headers: Vec<(String, String)>,
}

impl BrokerMessage {
    fn payload(payload: String) -> Self {
        Self {
            payload,
            headers: Vec::new(),
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
        match timeout(duration, self.payload_rx.recv()).await {
            Ok(Some(message)) => Ok(Some(message.payload)),
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

impl TestSession {
    pub(crate) fn set_domain(&mut self, domain: String) {
        match self {
            Self::Raw(session) => session.domain = domain,
        }
    }

    pub(crate) async fn run_command(&mut self, query: &str) -> io::Result<String> {
        match self {
            Self::Raw(session) => session.run_command(query).await,
        }
    }

    pub(crate) async fn try_next_subscription(
        &mut self,
        timeout_duration: Duration,
    ) -> io::Result<Option<TestSubscriptionEvent>> {
        match self {
            Self::Raw(session) => session.try_next_subscription(timeout_duration).await,
        }
    }

    pub(crate) async fn try_next_server_error(
        &mut self,
        timeout_duration: Duration,
    ) -> io::Result<Option<TestServerEvent>> {
        match self {
            Self::Raw(session) => session.try_next_server_error(timeout_duration).await,
        }
    }
}

impl RawTestSession {
    async fn run_command(&mut self, query: &str) -> io::Result<String> {
        self.request_tx
            .send(SessionRequest {
                request: Some(proto::session_request::Request::Command(CommandRequest {
                    query: query.to_string(),
                    domain: self.domain.clone(),
                })),
            })
            .await
            .map_err(io::Error::other)?;

        loop {
            tokio::task::consume_budget().await;
            match self.response.message().await.map_err(io::Error::other)? {
                Some(proto::SessionResponse {
                    event: Some(Event::Result(result)),
                }) => {
                    if result.success {
                        return Ok(result.message);
                    }
                    return Err(io::Error::other(format!(
                        "command failed: {}\ndiagnostics: {:?}",
                        result.message, result.diagnostics
                    )));
                }
                Some(proto::SessionResponse {
                    event: Some(Event::Subscription(event)),
                }) => {
                    self.pending_subscriptions.push_back(event);
                }
                Some(proto::SessionResponse {
                    event: Some(Event::Server(_)),
                }) => {}
                Some(proto::SessionResponse {
                    event: Some(Event::Suggest(_)),
                }) => {}
                Some(proto::SessionResponse {
                    event: Some(Event::Snapshot(_)),
                }) => {}
                Some(proto::SessionResponse {
                    event: Some(Event::Domains(_)),
                }) => {}
                Some(proto::SessionResponse { event: None }) => {}
                None => {
                    return Err(io::Error::other(
                        "session relay closed before command result",
                    ));
                }
            }
        }
    }

    async fn try_next_subscription(
        &mut self,
        timeout_duration: Duration,
    ) -> io::Result<Option<TestSubscriptionEvent>> {
        if let Some(event) = self.pending_subscriptions.pop_front() {
            return Ok(Some(TestSubscriptionEvent {
                payload: event.payload,
            }));
        }

        let deadline = sleep(timeout_duration);
        tokio::pin!(deadline);
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                _ = &mut deadline => return Ok(None),
                message = self.response.message() => {
                    match message.map_err(io::Error::other)? {
                        Some(proto::SessionResponse {
                            event: Some(Event::Subscription(event)),
                        }) => {
                            return Ok(Some(TestSubscriptionEvent {
                                payload: event.payload,
                            }));
                        }
                        Some(proto::SessionResponse {
                            event: Some(Event::Server(_)),
                        }) => {}
                        Some(proto::SessionResponse {
                            event: Some(Event::Suggest(_)),
                        })
                        | Some(proto::SessionResponse {
                            event: Some(Event::Snapshot(_)),
                        })
                        | Some(proto::SessionResponse {
                            event: Some(Event::Domains(_)),
                        })
                        | Some(proto::SessionResponse {
                            event: Some(Event::Result(_)),
                        })
                        | Some(proto::SessionResponse { event: None }) => {}
                        None => {
                            return Err(io::Error::other(
                                "session relay closed before subscription event",
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn try_next_server_error(
        &mut self,
        timeout_duration: Duration,
    ) -> io::Result<Option<TestServerEvent>> {
        let deadline = sleep(timeout_duration);
        tokio::pin!(deadline);
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                _ = &mut deadline => return Ok(None),
                message = self.response.message() => {
                    match message.map_err(io::Error::other)? {
                        Some(proto::SessionResponse {
                            event: Some(Event::Server(event)),
                        }) => {
                            if event.level == ServerEventLevel::Error as i32 {
                                return Ok(Some(TestServerEvent {
                                    level: event.level,
                                    message: event.message,
                                }));
                            }
                        }
                        Some(proto::SessionResponse {
                            event: Some(Event::Subscription(event)),
                        }) => {
                            self.pending_subscriptions.push_back(event);
                        }
                        Some(proto::SessionResponse {
                            event: Some(Event::Suggest(_)),
                        })
                        | Some(proto::SessionResponse {
                            event: Some(Event::Snapshot(_)),
                        })
                        | Some(proto::SessionResponse {
                            event: Some(Event::Domains(_)),
                        })
                        | Some(proto::SessionResponse {
                            event: Some(Event::Result(_)),
                        })
                        | Some(proto::SessionResponse { event: None }) => {}
                        None => {
                            return Err(io::Error::other(
                                "session relay closed before server event",
                            ));
                        }
                    }
                }
            }
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

async fn run_command(server: &str, query: &str) -> io::Result<String> {
    let mut session = open_raw_session(server, "default").await?;
    let output = session.run_command(query).await?;
    drop(session);
    Ok(output)
}

async fn run_command_via_client(server: &str, domain: &str, query: &str) -> io::Result<String> {
    let client =
        Client::connect_with_options(server, domain.to_string(), client_connect_options(server)?)
            .await
            .map_err(io::Error::other)?;
    let outcome = client
        .execute(query.to_string())
        .await
        .map_err(io::Error::other)?;
    if outcome.success {
        Ok(outcome.message)
    } else {
        Err(io::Error::other(format!(
            "command failed: {}\ndiagnostics: {:?}",
            outcome.message, outcome.diagnostics
        )))
    }
}

async fn server_accepts_commands(server: &str) -> io::Result<bool> {
    let client = match Client::connect_with_options(
        server,
        "default".to_string(),
        client_connect_options(server)?,
    )
    .await
    {
        Ok(client) => client,
        Err(_) => return Ok(false),
    };
    let outcome = match client.execute("SHOW CLUSTER STATUS;".to_string()).await {
        Ok(outcome) => outcome,
        Err(_) => return Ok(false),
    };
    Ok(outcome.success || outcome.kind == CommandOutcomeKind::NotLeader)
}

async fn open_raw_session(server: &str, domain: &str) -> io::Result<TestSession> {
    let mut endpoint = Endpoint::from_shared(server.to_string()).map_err(io::Error::other)?;
    if server.starts_with("https://") {
        endpoint = endpoint
            .tls_config(
                ClientTlsConfig::new().ca_certificate(Certificate::from_pem(dev_tls_ca_pem()?)),
            )
            .map_err(io::Error::other)?;
    }
    let channel = endpoint.connect().await.map_err(io::Error::other)?;
    let mut client = SessionServiceClient::new(channel);
    let (request_tx, request_rx) = mpsc::channel(16);
    let mut request = Request::new(ReceiverStream::new(request_rx));
    let authorization =
        MetadataValue::from_str(&test_basic_authorization()).map_err(io::Error::other)?;
    request
        .metadata_mut()
        .insert("authorization", authorization);
    let response = client
        .session(request)
        .await
        .map_err(io::Error::other)?
        .into_inner();

    Ok(TestSession::Raw(Box::new(RawTestSession {
        domain: domain.to_string(),
        request_tx,
        response,
        pending_subscriptions: VecDeque::new(),
    })))
}

async fn publish_mqtt(topic: &str, payload: &str) -> io::Result<()> {
    publish_mqtt_with_qos(topic, payload, QoS::AtMostOnce, true).await
}

async fn publish_mqtt_with_qos(
    topic: &str,
    payload: &str,
    qos: QoS,
    retain: bool,
) -> io::Result<()> {
    let client_id = format!("nervix-cucumber-{}", Uuid::now_v7().as_simple());
    let options = MqttOptions::new(client_id, MQTT_HOST, MQTT_PORT);
    let (client, mut eventloop) = AsyncClient::new(options, 16);
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
        match client.publish(topic, qos, retain, payload).await {
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

async fn publish_mqtt_burst(topic: &str, payload: &str, count: usize) -> io::Result<()> {
    publish_mqtt_burst_with_qos(topic, payload, count, QoS::AtMostOnce).await
}

async fn publish_mqtt_burst_with_qos(
    topic: &str,
    payload: &str,
    count: usize,
    qos: QoS,
) -> io::Result<()> {
    let client_id = format!("nervix-cucumber-burst-{}", Uuid::now_v7().as_simple());
    let options = MqttOptions::new(client_id, MQTT_HOST, MQTT_PORT);
    let (client, mut eventloop) = AsyncClient::new(options, count.max(16));
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
            .publish(topic, qos, false, payload)
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

async fn ensure_rabbitmq_queue(queue: &str) -> io::Result<()> {
    let connection = Connection::connect(RABBITMQ_ADDR, ConnectionProperties::default())
        .await
        .map_err(io::Error::other)?;
    let channel = connection
        .create_channel()
        .await
        .map_err(io::Error::other)?;
    declare_rabbitmq_queue(&channel, queue).await
}

async fn publish_rabbitmq(queue: &str, payload: &str) -> io::Result<()> {
    let connection = Connection::connect(RABBITMQ_ADDR, ConnectionProperties::default())
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

async fn rabbitmq_queue_consumer_count(queue: &str) -> io::Result<usize> {
    let connection = Connection::connect(RABBITMQ_ADDR, ConnectionProperties::default())
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
    Ok(declared.consumer_count() as usize)
}

async fn publish_redis(channel: &str, payload: &str) -> io::Result<()> {
    let client = redis::Client::open(REDIS_ADDR).map_err(io::Error::other)?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(io::Error::other)?;
    publish_redis_to_subscriber(&mut connection, channel, payload).await?;
    sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn publish_redis_burst(channel: &str, payload: &str, count: usize) -> io::Result<()> {
    let client = redis::Client::open(REDIS_ADDR).map_err(io::Error::other)?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(io::Error::other)?;
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

async fn redis_channel_subscriber_count(channel: &str) -> io::Result<usize> {
    let client = redis::Client::open(REDIS_ADDR).map_err(io::Error::other)?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(io::Error::other)?;
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

async fn wait_for_redis_channel_subscribers(channel: &str, expected: usize) -> io::Result<()> {
    let deadline = Instant::now() + STATUS_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        match redis_channel_subscriber_count(channel).await {
            Ok(actual) if actual == expected => return Ok(()),
            Ok(_) => {}
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            let actual = redis_channel_subscriber_count(channel).await?;
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

async fn publish_pulsar(topic: &str, payload: &str) -> io::Result<()> {
    publish_pulsar_with_addr(PULSAR_ADDR, None, topic, payload).await
}

async fn publish_pulsar_tls(topic: &str, payload: &str) -> io::Result<()> {
    publish_pulsar_with_addr(PULSAR_TLS_ADDR, Some(dev_tls_ca_pem()?), topic, payload).await
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

async fn publish_kafka(topic: &str, payload: &str) -> io::Result<()> {
    publish_kafka_record(topic, None, payload, &[]).await
}

async fn publish_kafka_with_headers(
    topic: &str,
    payload: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    publish_kafka_record(topic, None, payload, headers).await
}

async fn publish_kafka_burst(topic: &str, payload: &str, count: usize) -> io::Result<()> {
    let mut client_config = kafka_client_config()?;
    let producer: FutureProducer = client_config
        .set("message.timeout.ms", "5000")
        .set("delivery.timeout.ms", "5000")
        .set("request.timeout.ms", "5000")
        .create()
        .map_err(io::Error::other)?;
    let mut deliveries = Vec::with_capacity(count);
    for _ in 0..count {
        tokio::task::consume_budget().await;
        deliveries.push(producer.send(
            FutureRecord::<(), str>::to(topic).payload(payload),
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

async fn publish_kafka_partition(topic: &str, partition: i32, payload: &str) -> io::Result<()> {
    publish_kafka_record(topic, Some(partition), payload, &[]).await
}

async fn publish_kafka_record(
    topic: &str,
    partition: Option<i32>,
    payload: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    let mut client_config = kafka_client_config()?;
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
                                    key: *key,
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

fn kafka_admin_client() -> io::Result<AdminClient<DefaultClientContext>> {
    let client_config = kafka_client_config()?;
    client_config.create().map_err(io::Error::other)
}

fn kafka_topic_partition_count(topic: &str) -> io::Result<Option<usize>> {
    let mut client_config = kafka_client_config()?;
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
    Ok(metadata
        .topics()
        .iter()
        .find(|entry| entry.name() == topic)
        .and_then(|entry| {
            let partitions = entry.partitions().len();
            if partitions == 0 {
                None
            } else {
                Some(partitions)
            }
        }))
}

async fn wait_for_kafka_topic_partitions(topic: &str, expected: usize) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match kafka_topic_partition_count(topic)? {
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

async fn wait_for_kafka_topic_absent(topic: &str) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match kafka_topic_partition_count(topic)? {
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

async fn ensure_kafka_topic_partitions(topic: &str, partitions: i32) -> io::Result<()> {
    if partitions <= 0 {
        return Err(io::Error::other(format!(
            "kafka topic '{topic}' must have at least one partition"
        )));
    }

    let admin = kafka_admin_client()?;
    let created = admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .map_err(io::Error::other)?;
    for result in created {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic_name, code)) => {
                return Err(io::Error::other(format!(
                    "failed to create kafka topic '{topic_name}': {code:?}"
                )));
            }
        }
    }

    let current = kafka_topic_partition_count(topic)?.unwrap_or(0);
    let expected = usize::try_from(partitions).expect("partition count must fit usize");
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

    wait_for_kafka_topic_partitions(topic, expected).await
}

async fn reset_kafka_topic_partitions(topic: &str, partitions: i32) -> io::Result<()> {
    if partitions <= 0 {
        return Err(io::Error::other(format!(
            "kafka topic '{topic}' must have at least one partition"
        )));
    }

    let admin = kafka_admin_client()?;
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
    wait_for_kafka_topic_absent(topic).await?;
    ensure_kafka_topic_partitions(topic, partitions).await
}

fn kafka_consumer_group_member_count(group: &str) -> io::Result<usize> {
    let mut client_config = kafka_client_config()?;
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

async fn ensure_sqs_queue(queue: &str) -> io::Result<()> {
    let client = sqs_client().await;
    client
        .create_queue()
        .queue_name(queue)
        .send()
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

async fn ensure_sqs_queue_tls(queue: &str) -> io::Result<()> {
    let client = sqs_tls_client().await?;
    client
        .create_queue()
        .queue_name(queue)
        .send()
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

async fn publish_sqs(queue: &str, payload: &str) -> io::Result<()> {
    let client = sqs_client().await;
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

async fn publish_sqs_tls(queue: &str, payload: &str) -> io::Result<()> {
    let client = sqs_tls_client().await?;
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

async fn publish_nats(subject: &str, payload: &str) -> io::Result<()> {
    let client = nats_client().await?;
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

async fn publish_nats_with_headers(
    subject: &str,
    payload: &str,
    headers: &[(&str, &str)],
) -> io::Result<()> {
    let client = nats_client().await?;
    let mut header_map = async_nats::HeaderMap::new();
    for (name, value) in headers {
        header_map.insert(*name, *value);
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

async fn publish_nats_tls(subject: &str, payload: &str) -> io::Result<()> {
    let client = nats_tls_client().await?;
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

async fn observe_mqtt(topic: &str) -> io::Result<BrokerObserver> {
    let client_id = format!(
        "nervix-cucumber-observer-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock must be after unix epoch")
            .as_nanos()
    );
    let mut options = MqttOptions::new(client_id, MQTT_HOST, MQTT_PORT);
    options.set_clean_session(true);
    let (client, mut eventloop) = AsyncClient::new(options, 16);
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
                    if publish.topic == topic {
                        let payload = String::from_utf8_lossy(publish.payload.as_ref()).to_string();
                        let _ = payload_tx.send(BrokerMessage::payload(payload)).await;
                        break;
                    }
                }
                Ok(MqttEvent::Incoming(_)) | Ok(MqttEvent::Outgoing(_)) => {}
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

async fn observe_rabbitmq(queue: &str) -> io::Result<BrokerObserver> {
    let connection = Connection::connect(RABBITMQ_ADDR, ConnectionProperties::default())
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

async fn observe_redis(channel: &str) -> io::Result<BrokerObserver> {
    let client = redis::Client::open(REDIS_ADDR).map_err(io::Error::other)?;
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

async fn observe_kafka(topic: &str) -> io::Result<BrokerObserver> {
    let consumer_group = format!(
        "nervix-cucumber-observer-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock must be after unix epoch")
            .as_nanos()
    );
    let mut client_config = kafka_client_config()?;
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
                    let payload =
                        String::from_utf8_lossy(message.payload().unwrap_or_default()).to_string();
                    let headers = message
                        .headers()
                        .map(|headers| {
                            (0..headers.count())
                                .filter_map(|index| {
                                    let header = headers.try_get(index)?;
                                    Some((
                                        header.key.to_string(),
                                        header
                                            .value
                                            .map(|value| String::from_utf8_lossy(value).to_string())
                                            .unwrap_or_default(),
                                    ))
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    let _ = payload_tx.send(BrokerMessage { payload, headers }).await;
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

async fn observe_pulsar(topic: &str) -> io::Result<BrokerObserver> {
    observe_pulsar_with_addr(PULSAR_ADDR, None, topic).await
}

async fn observe_pulsar_tls(topic: &str) -> io::Result<BrokerObserver> {
    observe_pulsar_with_addr(PULSAR_TLS_ADDR, Some(dev_tls_ca_pem()?), topic).await
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

async fn observe_sqs(queue: &str) -> io::Result<BrokerObserver> {
    ensure_sqs_queue(queue).await?;
    let client = sqs_client().await;
    let queue_url = sqs_queue_url(&client, queue).await?;
    let (payload_tx, payload_rx) = mpsc::channel(1);

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
            let _ = payload_tx.send(BrokerMessage::payload(payload)).await;
            break;
        }
    });

    Ok(BrokerObserver {
        payload_rx,
        task: Some(task),
    })
}

async fn observe_nats(subject: &str) -> io::Result<BrokerObserver> {
    let client = nats_client().await?;
    let mut subscriber = client
        .subscribe(subject.to_string())
        .await
        .map_err(io::Error::other)?;
    let (payload_tx, payload_rx) = mpsc::channel(1);

    let task = tokio::spawn(async move {
        if let Some(message) = subscriber.next().await {
            tokio::task::consume_budget().await;
            let payload = String::from_utf8_lossy(message.payload.as_ref()).to_string();
            let _ = payload_tx.send(BrokerMessage::payload(payload)).await;
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

async fn nats_client() -> io::Result<NatsClient> {
    async_nats::connect(NATS_ADDR)
        .await
        .map_err(io::Error::other)
}

async fn nats_tls_client() -> io::Result<NatsClient> {
    let ca_path = dev_tls_ca_path()?;
    async_nats::ConnectOptions::new()
        .add_root_certificates(ca_path)
        .require_tls(true)
        .connect(NATS_TLS_ADDR)
        .await
        .map_err(io::Error::other)
}

async fn sqs_client() -> SqsClient {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_sdk_sqs::config::Region::new(SQS_REGION))
        .endpoint_url(SQS_ENDPOINT)
        .credentials_provider(Credentials::new("x", "x", None, None, "nervix-cucumber"))
        .load()
        .await;
    SqsClient::new(&sdk_config)
}

async fn sqs_tls_client() -> io::Result<SqsClient> {
    let ca_pem = std::fs::read(dev_tls_ca_path()?).map_err(io::Error::other)?;
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
        .endpoint_url(SQS_TLS_ENDPOINT)
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
