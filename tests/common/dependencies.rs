use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsString,
    fmt, fs, io,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, OnceLock},
    time::Duration,
};

use tempfile::{TempDir, tempdir, tempdir_in};
use testcontainers::{
    ContainerAsync, ContainerRequest, CopyTargetOptions, GenericBuildableImage, GenericImage,
    Image, ImageExt, ReuseDirective, TestcontainersError,
    bollard::{Docker, query_parameters::RemoveContainerOptionsBuilder},
    core::{
        BuildImageOptions, ContainerPort, ContainerState, ExecCommand, IntoContainerPort, WaitFor,
        wait::HttpWaitStrategy,
    },
    runners::{AsyncBuilder, AsyncRunner},
};
use tokio::sync::Mutex;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const REUSABLE_READY_TIMEOUT: Duration = Duration::from_secs(30);
const TESTCONTAINERS_MODE_ENV: &str = "NERVIX_TESTCONTAINERS_MODE";
const TESTCONTAINERS_COMMAND_ENV: &str = "TESTCONTAINERS_COMMAND";
const TESTCONTAINERS_CONFIG_LABEL: &str = "com.nervix.testcontainers.config-hash";
const TESTCONTAINERS_ROLE_LABEL: &str = "com.nervix.testcontainers.role";
const TESTCONTAINERS_REUSABLE_LABEL: &str = "com.nervix.testcontainers.reusable";
const KAFKA_PLAINTEXT_PORT: ContainerPort = ContainerPort::Tcp(9092);
const KAFKA_TLS_PORT: ContainerPort = ContainerPort::Tcp(9094);
const KAFKA_START_SCRIPT: &str = "/opt/kafka/nervix_testcontainers_start.sh";
const KAFKA_KEYSTORE_PASSWORD: &str = "nervix-test-kafka";
const DEPENDENCY_CONFIGURATION_SOURCE: &[u8] = include_bytes!("dependencies.rs");
const DEPENDENCY_CONFIGURATION_FILES: &[&[u8]] = &[
    include_bytes!("../../docker/clickhouse/https.xml"),
    include_bytes!("../../docker/mock-server/Dockerfile"),
    include_bytes!("../../docker/mock-server/app.py"),
    include_bytes!("../../docker/nats/nats-tls.conf"),
    include_bytes!("../../docker/prometheus/prometheus.yml"),
    include_bytes!("../../docker/prometheus/web.yml"),
    include_bytes!("../../docker/rabbitmq/rabbitmq.conf"),
    include_bytes!("../../docker/redis/redis.conf"),
];

pub(crate) const KAFKA_ADDR: &str = "kafka_addr";
pub(crate) const KAFKA_TLS_ADDR: &str = "kafka_tls_addr";
pub(crate) const PULSAR_ADDR: &str = "pulsar_addr";
pub(crate) const PULSAR_TLS_ADDR: &str = "pulsar_tls_addr";
pub(crate) const RABBITMQ_ADDR: &str = "rabbitmq_addr";
pub(crate) const RABBITMQ_TLS_ADDR: &str = "rabbitmq_tls_addr";
pub(crate) const REDIS_ADDR: &str = "redis_addr";
pub(crate) const REDIS_TLS_ADDR: &str = "redis_tls_addr";
pub(crate) const MQTT_ADDR: &str = "mqtt_addr";
pub(crate) const MQTT_TLS_ADDR: &str = "mqtt_tls_addr";
pub(crate) const NATS_ADDR: &str = "nats_addr";
pub(crate) const NATS_TLS_ADDR: &str = "nats_tls_addr";
pub(crate) const SQS_ENDPOINT: &str = "sqs_endpoint";
pub(crate) const SQS_TLS_ENDPOINT: &str = "sqs_tls_endpoint";
pub(crate) const CLICKHOUSE_ADDR: &str = "clickhouse_addr";
pub(crate) const CLICKHOUSE_TLS_ADDR: &str = "clickhouse_tls_addr";
pub(crate) const POSTGRES_ADDR: &str = "postgres_addr";
pub(crate) const POSTGRES_TLS_ADDR: &str = "postgres_tls_addr";
pub(crate) const MYSQL_ADDR: &str = "mysql_addr";
pub(crate) const MYSQL_TLS_ADDR: &str = "mysql_tls_addr";
pub(crate) const MONGODB_ADDR: &str = "mongodb_addr";
pub(crate) const MONGODB_TLS_ADDR: &str = "mongodb_tls_addr";
pub(crate) const PROMETHEUS_ADDR: &str = "prometheus_addr";
pub(crate) const PROMETHEUS_TLS_ADDR: &str = "prometheus_tls_addr";
pub(crate) const MOCK_HTTP_ADDR: &str = "mock_http_addr";
pub(crate) const MOCK_HTTPS_ADDR: &str = "mock_https_addr";
pub(crate) const MOCK_WS_ADDR: &str = "mock_ws_addr";
pub(crate) const MOCK_WSS_ADDR: &str = "mock_wss_addr";
pub(crate) const RUSTFS_ADDR: &str = "rustfs_addr";
pub(crate) const ICEBERG_REST_ADDR: &str = "iceberg_rest_addr";
pub(crate) const GCS_ADDR: &str = "gcs_addr";
pub(crate) const AZURITE_ADDR: &str = "azurite_addr";
pub(crate) const QUICKWIT_ADDR: &str = "quickwit_addr";
pub(crate) const OTLP_ADDR: &str = "otlp_addr";
pub(crate) const JAEGER_ADDR: &str = "jaeger_addr";

#[derive(Clone, Default)]
pub(crate) struct DependencyEndpoints {
    values: BTreeMap<String, String>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_ca_path: Option<PathBuf>,
}

impl fmt::Debug for DependencyEndpoints {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DependencyEndpoints")
            .field("endpoint_keys", &self.values.keys().collect::<Vec<_>>())
            .field("tls_configured", &self.tls_ca_pem.is_some())
            .finish()
    }
}

impl DependencyEndpoints {
    pub(crate) fn get(&self, key: &str) -> io::Result<&str> {
        self.values.get(key).map(String::as_str).ok_or_else(|| {
            io::Error::other(format!(
                "scenario dependency endpoint '{key}' is unavailable; add its Given step before \
                 starting the Nervix cluster"
            ))
        })
    }

    pub(crate) fn tls_ca_pem(&self) -> io::Result<Vec<u8>> {
        self.tls_ca_pem.clone().ok_or_else(|| {
            io::Error::other("scenario TLS materials are unavailable; start a TLS dependency first")
        })
    }

    pub(crate) fn tls_ca_path(&self) -> io::Result<&Path> {
        self.tls_ca_path.as_deref().ok_or_else(|| {
            io::Error::other("scenario TLS materials are unavailable; start a TLS dependency first")
        })
    }

    pub(crate) fn apply_placeholders(&self, placeholders: &mut BTreeMap<String, String>) {
        placeholders.extend(self.values.clone());
    }

    fn insert(&mut self, key: &str, value: String) {
        self.values.insert(key.to_string(), value);
    }

    fn set_tls(&mut self, tls: &TlsMaterials) {
        self.tls_ca_pem = Some(tls.ca_pem.clone());
        self.tls_ca_path = Some(tls.ca_path.clone());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContainerMode {
    Ephemeral,
    Reusable,
}

impl ContainerMode {
    fn from_environment() -> Self {
        match env::var(TESTCONTAINERS_MODE_ENV).as_deref() {
            Err(env::VarError::NotPresent) | Ok("") | Ok("ephemeral") => Self::Ephemeral,
            Ok("reusable") => Self::Reusable,
            Ok(value) => panic!(
                "{TESTCONTAINERS_MODE_ENV} must be 'ephemeral' or 'reusable', found {value:?}"
            ),
            Err(env::VarError::NotUnicode(_)) => {
                panic!("{TESTCONTAINERS_MODE_ENV} must contain valid UTF-8")
            }
        }
    }

    fn is_reusable(self) -> bool {
        self == Self::Reusable
    }
}

static SUITE_DEPENDENCIES: OnceLock<Mutex<DependencySuite>> = OnceLock::new();

#[derive(Clone, Default)]
pub(crate) struct TestDependencies {
    endpoints: DependencyEndpoints,
    tls_dir: Option<PathBuf>,
}

impl fmt::Debug for TestDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestDependencies")
            .field("endpoints", &self.endpoints)
            .field("tls_materials_generated", &self.tls_dir.is_some())
            .finish()
    }
}

macro_rules! dependency_starters {
    ($($method:ident => $dependency:literal),+ $(,)?) => {
        $(
            pub(crate) async fn $method(&mut self, scope: &str) -> io::Result<()> {
                let mut suite = suite_dependencies().lock().await;
                let result = suite.$method(scope).await;
                if result.is_err() {
                    suite.running.remove($dependency);
                } else {
                    self.refresh_from(&suite);
                }
                result
            }
        )+
    };
}

impl TestDependencies {
    pub(crate) fn configure_process_lifecycle() {
        let command = if ContainerMode::from_environment().is_reusable() {
            "keep"
        } else {
            "remove"
        };
        // SAFETY: main calls this before creating the Tokio runtime or any other worker threads.
        // The Nervix reuse flag is the single lifecycle control: normal suites must keep
        // Testcontainers' drop and signal cleanup enabled, while reusable suites must not let the
        // watchdog delete containers intended for later processes.
        unsafe { env::set_var(TESTCONTAINERS_COMMAND_ENV, command) };
    }

    pub(crate) fn endpoints(&self) -> &DependencyEndpoints {
        &self.endpoints
    }

    pub(crate) fn tls_dir(&self) -> io::Result<&Path> {
        self.tls_dir.as_deref().ok_or_else(|| {
            io::Error::other("scenario TLS materials are unavailable; start a TLS dependency first")
        })
    }

    dependency_starters! {
        start_kafka => "kafka",
        start_pulsar => "pulsar",
        start_rabbitmq => "rabbitmq",
        start_redis => "redis",
        start_mqtt => "mqtt",
        start_clickhouse => "clickhouse",
        start_postgres => "postgres",
        start_mysql => "mysql",
        start_mongodb => "mongodb",
        start_nats => "nats",
        start_nats_tls => "nats-tls",
        start_prometheus => "prometheus",
        start_prometheus_tls => "prometheus-tls",
        start_sqs => "sqs",
        start_mock_server => "mock-server",
        start_iceberg => "iceberg",
        start_gcs => "gcs",
        start_azurite => "azurite",
        start_quickwit => "quickwit",
        start_jaeger => "jaeger",
    }

    pub(crate) async fn shutdown_suite() -> Vec<String> {
        let Some(suite) = SUITE_DEPENDENCIES.get() else {
            return Vec::new();
        };
        suite.lock().await.shutdown().await
    }

    pub(crate) async fn suite_container_ids() -> Vec<String> {
        let Some(suite) = SUITE_DEPENDENCIES.get() else {
            return Vec::new();
        };
        suite
            .lock()
            .await
            .containers
            .iter()
            .map(RunningContainer::id)
            .collect()
    }

    pub(crate) async fn container_exists(id: &str) -> io::Result<bool> {
        container_exists_by_name_or_id(id).await
    }

    pub(crate) async fn force_remove_container(id: &str) -> io::Result<()> {
        remove_container_by_name_or_id(id).await
    }

    fn refresh_from(&mut self, suite: &DependencySuite) {
        self.endpoints = suite.endpoints.clone();
        self.tls_dir = suite.tls.as_ref().map(|tls| tls.dir.path().to_path_buf());
    }
}

fn suite_dependencies() -> &'static Mutex<DependencySuite> {
    SUITE_DEPENDENCIES.get_or_init(|| Mutex::new(DependencySuite::default()))
}

#[derive(Debug)]
struct DependencySuite {
    running: BTreeSet<&'static str>,
    containers: Vec<RunningContainer>,
    endpoints: DependencyEndpoints,
    tls: Option<TlsMaterials>,
    tls_configuration_hash: Option<String>,
    scope: Option<String>,
    mode: ContainerMode,
}

impl Default for DependencySuite {
    fn default() -> Self {
        Self {
            running: BTreeSet::new(),
            containers: Vec::new(),
            endpoints: DependencyEndpoints::default(),
            tls: None,
            tls_configuration_hash: None,
            scope: None,
            mode: ContainerMode::from_environment(),
        }
    }
}

impl DependencySuite {
    pub(crate) async fn start_kafka(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("kafka", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let container = self
            .start_container("kafka", KAFKA_PLAINTEXT_PORT, "Kafka", || {
                KafkaImage::default()
                    .with_copy_to(
                        "/etc/kafka/secrets/kafka.keystore.p12",
                        tls.kafka_keystore.clone(),
                    )
                    .with_copy_to(
                        "/etc/kafka/secrets/kafka_keystore_creds",
                        KAFKA_KEYSTORE_PASSWORD.as_bytes().to_vec(),
                    )
                    .with_copy_to(
                        "/etc/kafka/secrets/kafka_key_creds",
                        KAFKA_KEYSTORE_PASSWORD.as_bytes().to_vec(),
                    )
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let plaintext_port = container
            .get_host_port_ipv4(KAFKA_PLAINTEXT_PORT)
            .await
            .map_err(testcontainers_error("Kafka plaintext port"))?;
        let tls_port = container
            .get_host_port_ipv4(KAFKA_TLS_PORT)
            .await
            .map_err(testcontainers_error("Kafka TLS port"))?;
        self.endpoints
            .insert(KAFKA_ADDR, format!("127.0.0.1:{plaintext_port}"));
        self.endpoints
            .insert(KAFKA_TLS_ADDR, format!("localhost:{tls_port}"));
        self.containers.push(RunningContainer::Kafka(container));
        Ok(())
    }

    pub(crate) async fn start_pulsar(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("pulsar", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let start_script = br#"#!/bin/sh
set -eu
config=/tmp/standalone-tls.conf
cp /pulsar/conf/standalone.conf "$config"
cat >>"$config" <<'EOF'
brokerServicePortTls=6651
webServicePortTls=8443
tlsEnabled=true
tlsCertificateFilePath=/pulsar/certs/node.pem
tlsKeyFilePath=/pulsar/certs/node-key.pem
tlsTrustCertsFilePath=/pulsar/certs/ca.pem
tlsAllowInsecureConnection=false
tlsRequireTrustedClientCertOnConnect=false
EOF
exec /pulsar/bin/pulsar standalone --no-functions-worker --no-stream-storage -c "$config"
"#;
        let container = self
            .start_container("pulsar", 6650.tcp(), "Pulsar", || {
                GenericImage::new("apachepulsar/pulsar", "4.1.0")
                    .with_exposed_port(6650.tcp())
                    .with_exposed_port(6651.tcp())
                    .with_exposed_port(8080.tcp())
                    .with_wait_for(WaitFor::message_on_stdout("messaging service is ready"))
                    .with_copy_to(
                        CopyTargetOptions::new("/pulsar/bin/nervix-start-tls.sh").with_mode(0o755),
                        start_script.to_vec(),
                    )
                    .with_copy_to("/pulsar/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/pulsar/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/pulsar/certs/node-key.pem").with_mode(0o644),
                        tls.node_key_pem.clone(),
                    )
                    .with_cmd(["sh", "/pulsar/bin/nervix-start-tls.sh"])
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let plaintext_port = mapped_port(&container, 6650, "Pulsar").await?;
        let tls_port = mapped_port(&container, 6651, "Pulsar TLS").await?;
        self.endpoints
            .insert(PULSAR_ADDR, format!("pulsar://127.0.0.1:{plaintext_port}"));
        self.endpoints.insert(
            PULSAR_TLS_ADDR,
            format!("pulsar+ssl://127.0.0.1:{tls_port}"),
        );
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_rabbitmq(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("rabbitmq", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let container = self
            .start_container("rabbitmq", 5672.tcp(), "RabbitMQ", || {
                GenericImage::new("rabbitmq", "4.1-management")
                    .with_exposed_port(5672.tcp())
                    .with_exposed_port(5671.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("Server startup complete"))
                    .with_env_var("RABBITMQ_DEFAULT_USER", "guest")
                    .with_env_var("RABBITMQ_DEFAULT_PASS", "guest")
                    .with_copy_to(
                        "/etc/rabbitmq/rabbitmq.conf",
                        include_bytes!("../../docker/rabbitmq/rabbitmq.conf").to_vec(),
                    )
                    .with_copy_to("/etc/rabbitmq/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/etc/rabbitmq/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/etc/rabbitmq/certs/node-key.pem").with_mode(0o644),
                        tls.node_key_pem.clone(),
                    )
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 5672, "RabbitMQ").await?;
        let tls_port = mapped_port(&container, 5671, "RabbitMQ TLS").await?;
        self.endpoints.insert(
            RABBITMQ_ADDR,
            format!("amqp://guest:guest@127.0.0.1:{port}/%2f"),
        );
        self.endpoints.insert(
            RABBITMQ_TLS_ADDR,
            format!("amqps://guest:guest@127.0.0.1:{tls_port}/%2f"),
        );
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_redis(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("redis", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let container = self
            .start_container("redis", 6379.tcp(), "Redis", || {
                GenericImage::new("redis", "7")
                    .with_exposed_port(6379.tcp())
                    .with_exposed_port(6380.tcp())
                    .with_wait_for(WaitFor::message_on_either_std(
                        "Ready to accept connections",
                    ))
                    .with_copy_to(
                        "/usr/local/etc/redis/redis.conf",
                        include_bytes!("../../docker/redis/redis.conf").to_vec(),
                    )
                    .with_copy_to("/etc/redis/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/etc/redis/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/etc/redis/certs/node-key.pem").with_mode(0o644),
                        tls.node_key_pem.clone(),
                    )
                    .with_cmd(["redis-server", "/usr/local/etc/redis/redis.conf"])
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 6379, "Redis").await?;
        let tls_port = mapped_port(&container, 6380, "Redis TLS").await?;
        self.endpoints
            .insert(REDIS_ADDR, format!("redis://127.0.0.1:{port}/"));
        self.endpoints
            .insert(REDIS_TLS_ADDR, format!("rediss://127.0.0.1:{tls_port}/"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_mqtt(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("mqtt", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let container = self
            .start_container("mqtt", 1883.tcp(), "MQTT", || {
                GenericImage::new("emqx/emqx", "5.8.4")
                    .with_exposed_port(1883.tcp())
                    .with_exposed_port(8883.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("EMQX 5.8.4 is running now"))
                    .with_env_var("EMQX_LISTENERS__TCP__DEFAULT__BIND", "0.0.0.0:1883")
                    .with_env_var("EMQX_LISTENERS__SSL__DEFAULT__BIND", "0.0.0.0:8883")
                    .with_env_var(
                        "EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__CACERTFILE",
                        "/certs/ca.pem",
                    )
                    .with_env_var(
                        "EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__CERTFILE",
                        "/certs/node.pem",
                    )
                    .with_env_var(
                        "EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__KEYFILE",
                        "/certs/node-key.pem",
                    )
                    .with_env_var(
                        "EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__VERIFY",
                        "verify_none",
                    )
                    .with_env_var(
                        "EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__FAIL_IF_NO_PEER_CERT",
                        "false",
                    )
                    .with_copy_to("/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/certs/node-key.pem").with_mode(0o644),
                        tls.node_key_pem.clone(),
                    )
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 1883, "MQTT").await?;
        let tls_port = mapped_port(&container, 8883, "MQTT TLS").await?;
        self.endpoints
            .insert(MQTT_ADDR, format!("mqtt://127.0.0.1:{port}"));
        self.endpoints
            .insert(MQTT_TLS_ADDR, format!("mqtts://127.0.0.1:{tls_port}"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_clickhouse(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("clickhouse", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let container = self
            .start_container("clickhouse", 8123.tcp(), "ClickHouse", || {
                GenericImage::new("clickhouse/clickhouse-server", "25.3")
                    .with_exposed_port(8123.tcp())
                    .with_exposed_port(8443.tcp())
                    .with_wait_for(WaitFor::http(
                        HttpWaitStrategy::new("/ping")
                            .with_port(8123.tcp())
                            .with_expected_status_code(200_u16),
                    ))
                    .with_env_var("CLICKHOUSE_USER", "default")
                    .with_env_var("CLICKHOUSE_PASSWORD", "nervix")
                    .with_env_var("CLICKHOUSE_DB", "default")
                    .with_copy_to(
                        "/etc/clickhouse-server/config.d/https.xml",
                        include_bytes!("../../docker/clickhouse/https.xml").to_vec(),
                    )
                    .with_copy_to("/etc/clickhouse-server/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to(
                        "/etc/clickhouse-server/certs/node.pem",
                        tls.node_pem.clone(),
                    )
                    .with_copy_to(
                        CopyTargetOptions::new("/etc/clickhouse-server/certs/node-key.pem")
                            .with_mode(0o644),
                        tls.node_key_pem.clone(),
                    )
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 8123, "ClickHouse").await?;
        let tls_port = mapped_port(&container, 8443, "ClickHouse TLS").await?;
        self.endpoints
            .insert(CLICKHOUSE_ADDR, format!("http://127.0.0.1:{port}"));
        self.endpoints
            .insert(CLICKHOUSE_TLS_ADDR, format!("https://127.0.0.1:{tls_port}"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_postgres(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("postgres", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let command = [
            "-c",
            "set -euo pipefail; cp /certs/node.pem /tmp/server.crt; cp /certs/node-key.pem \
             /tmp/server.key; cp /certs/ca.pem /tmp/ca.pem; chown postgres:postgres \
             /tmp/server.crt /tmp/server.key /tmp/ca.pem; chmod 600 /tmp/server.key; exec \
             docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tmp/server.crt -c \
             ssl_key_file=/tmp/server.key -c ssl_ca_file=/tmp/ca.pem",
        ];
        let container = self
            .start_container("postgres", 5432.tcp(), "Postgres", || {
                GenericImage::new("postgres", "17")
                    .with_exposed_port(5432.tcp())
                    .with_entrypoint("bash")
                    .with_wait_for(WaitFor::message_on_either_std(
                        "database system is ready to accept connections",
                    ))
                    .with_env_var("POSTGRES_USER", "postgres")
                    .with_env_var("POSTGRES_PASSWORD", "nervix")
                    .with_env_var("POSTGRES_DB", "postgres")
                    .with_copy_to("/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/certs/node-key.pem").with_mode(0o600),
                        tls.node_key_pem.clone(),
                    )
                    .with_cmd(command)
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 5432, "Postgres").await?;
        self.endpoints.insert(
            POSTGRES_ADDR,
            format!("host=127.0.0.1 port={port} user=postgres password=nervix dbname=postgres"),
        );
        self.endpoints.insert(
            POSTGRES_TLS_ADDR,
            format!(
                "host=127.0.0.1 port={port} user=postgres password=nervix dbname=postgres \
                 sslmode=require"
            ),
        );
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_mysql(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("mysql", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let command = [
            "-c",
            "set -euo pipefail; cp /certs/node.pem /tmp/server.crt; cp /certs/node-key.pem \
             /tmp/server.key; cp /certs/ca.pem /tmp/ca.pem; chown mysql:mysql /tmp/server.crt \
             /tmp/server.key /tmp/ca.pem; chmod 600 /tmp/server.key; exec docker-entrypoint.sh \
             mysqld --ssl-ca=/tmp/ca.pem --ssl-cert=/tmp/server.crt --ssl-key=/tmp/server.key",
        ];
        let container = self
            .start_container("mysql", 3306.tcp(), "MySQL", || {
                GenericImage::new("mysql", "8.4")
                    .with_exposed_port(3306.tcp())
                    .with_entrypoint("bash")
                    .with_wait_for(WaitFor::message_on_either_std("port: 3306"))
                    .with_env_var("MYSQL_ROOT_PASSWORD", "nervix")
                    .with_env_var("MYSQL_DATABASE", "nervix")
                    .with_env_var("MYSQL_USER", "nervix")
                    .with_env_var("MYSQL_PASSWORD", "nervix")
                    .with_copy_to("/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/certs/node-key.pem").with_mode(0o600),
                        tls.node_key_pem.clone(),
                    )
                    .with_cmd(command)
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 3306, "MySQL").await?;
        self.endpoints.insert(
            MYSQL_ADDR,
            format!("mysql://nervix:nervix@127.0.0.1:{port}/nervix"),
        );
        self.endpoints.insert(
            MYSQL_TLS_ADDR,
            format!("mysql://nervix:nervix@127.0.0.1:{port}/nervix?require_ssl=true"),
        );
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_mongodb(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("mongodb", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let command = [
            "-c",
            "set -euo pipefail; cat /certs/node-key.pem /certs/node.pem > /tmp/server.pem; cp \
             /certs/ca.pem /tmp/ca.pem; chown mongodb:mongodb /tmp/server.pem /tmp/ca.pem; chmod \
             600 /tmp/server.pem; exec docker-entrypoint.sh mongod --bind_ip_all --tlsMode \
             preferTLS --tlsCertificateKeyFile /tmp/server.pem --tlsCAFile /tmp/ca.pem \
             --tlsAllowConnectionsWithoutCertificates",
        ];
        let container = self
            .start_container("mongodb", 27017.tcp(), "MongoDB", || {
                GenericImage::new("mongo", "7.0")
                    .with_exposed_port(27017.tcp())
                    .with_entrypoint("bash")
                    .with_wait_for(WaitFor::message_on_either_std("Waiting for connections"))
                    .with_env_var("MONGO_INITDB_ROOT_USERNAME", "root")
                    .with_env_var("MONGO_INITDB_ROOT_PASSWORD", "nervix")
                    .with_copy_to("/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/certs/node-key.pem").with_mode(0o600),
                        tls.node_key_pem.clone(),
                    )
                    .with_cmd(command)
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 27017, "MongoDB").await?;
        self.endpoints.insert(
            MONGODB_ADDR,
            format!("mongodb://root:nervix@127.0.0.1:{port}/nervix?authSource=admin"),
        );
        self.endpoints.insert(
            MONGODB_TLS_ADDR,
            format!("mongodb://root:nervix@127.0.0.1:{port}/nervix?authSource=admin&tls=true"),
        );
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_nats(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("nats", scope).await? {
            return Ok(());
        }
        let container = self
            .start_container("nats", 4222.tcp(), "NATS", || {
                GenericImage::new("nats", "2.11-alpine")
                    .with_exposed_port(4222.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("Server is ready"))
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 4222, "NATS").await?;
        self.endpoints
            .insert(NATS_ADDR, format!("nats://127.0.0.1:{port}"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_nats_tls(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("nats-tls", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let container = self
            .start_container("nats-tls", 4223.tcp(), "NATS TLS", || {
                GenericImage::new("nats", "2.11-alpine")
                    .with_exposed_port(4223.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("Server is ready"))
                    .with_copy_to(
                        "/etc/nats/nats-tls.conf",
                        include_bytes!("../../docker/nats/nats-tls.conf").to_vec(),
                    )
                    .with_copy_to("/etc/nats/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/etc/nats/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/etc/nats/certs/node-key.pem").with_mode(0o600),
                        tls.node_key_pem.clone(),
                    )
                    .with_cmd(["-c", "/etc/nats/nats-tls.conf"])
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 4223, "NATS TLS").await?;
        self.endpoints
            .insert(NATS_TLS_ADDR, format!("tls://127.0.0.1:{port}"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_prometheus(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("prometheus", scope).await? {
            return Ok(());
        }
        let container = self
            .start_container("prometheus", 9090.tcp(), "Prometheus", || {
                GenericImage::new("prom/prometheus", "v3.3.1")
                    .with_exposed_port(9090.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("Server is ready to receive"))
                    .with_copy_to(
                        "/etc/prometheus/prometheus.yml",
                        include_bytes!("../../docker/prometheus/prometheus.yml").to_vec(),
                    )
                    .with_cmd([
                        "--config.file=/etc/prometheus/prometheus.yml",
                        "--web.enable-lifecycle",
                    ])
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 9090, "Prometheus").await?;
        self.endpoints
            .insert(PROMETHEUS_ADDR, format!("http://127.0.0.1:{port}"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_prometheus_tls(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("prometheus-tls", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let container = self
            .start_container("prometheus-tls", 9443.tcp(), "Prometheus TLS", || {
                GenericImage::new("prom/prometheus", "v3.3.1")
                    .with_exposed_port(9443.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("Server is ready to receive"))
                    .with_copy_to(
                        "/etc/prometheus/prometheus.yml",
                        include_bytes!("../../docker/prometheus/prometheus.yml").to_vec(),
                    )
                    .with_copy_to(
                        "/etc/prometheus/web.yml",
                        include_bytes!("../../docker/prometheus/web.yml").to_vec(),
                    )
                    .with_copy_to("/etc/prometheus/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/etc/prometheus/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/etc/prometheus/certs/node-key.pem")
                            .with_mode(0o644),
                        tls.node_key_pem.clone(),
                    )
                    .with_cmd([
                        "--config.file=/etc/prometheus/prometheus.yml",
                        "--web.enable-lifecycle",
                        "--web.listen-address=:9443",
                        "--web.config.file=/etc/prometheus/web.yml",
                    ])
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 9443, "Prometheus TLS").await?;
        self.endpoints
            .insert(PROMETHEUS_TLS_ADDR, format!("https://127.0.0.1:{port}"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_sqs(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("sqs", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let network = self.network_name("sqs-stack");
        let elasticmq_name = self.container_name("elasticmq");
        let elasticmq = self
            .start_container("elasticmq", 9324.tcp(), "ElasticMQ", || {
                GenericImage::new("softwaremill/elasticmq-native", "1.6.12")
                    .with_exposed_port(9324.tcp())
                    .with_wait_for(WaitFor::message_on_either_std(
                        "ElasticMQ server (1.6.12) started",
                    ))
                    .with_container_name(&elasticmq_name)
                    .with_network(&network)
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&elasticmq, 9324, "ElasticMQ").await?;

        let nginx_config = format!(
            "server {{\n  listen 9325 ssl;\n  server_name localhost;\n  ssl_certificate \
             /etc/nginx/certs/node.pem;\n  ssl_certificate_key \
             /etc/nginx/certs/node-key.pem;\n  location / {{\n    proxy_pass \
             http://{elasticmq_name}:9324;\n    proxy_set_header Host $host;\n    \
             proxy_set_header X-Forwarded-Proto https;\n  }}\n}}\n"
        );
        let proxy = self
            .start_container("elasticmq-tls", 9325.tcp(), "ElasticMQ TLS proxy", || {
                GenericImage::new("nginx", "1.27-alpine")
                    .with_exposed_port(9325.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("start worker processes"))
                    .with_network(&network)
                    .with_copy_to(
                        "/etc/nginx/conf.d/default.conf",
                        nginx_config.clone().into_bytes(),
                    )
                    .with_copy_to("/etc/nginx/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/etc/nginx/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/etc/nginx/certs/node-key.pem").with_mode(0o600),
                        tls.node_key_pem.clone(),
                    )
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let tls_port = mapped_port(&proxy, 9325, "ElasticMQ TLS").await?;
        self.endpoints
            .insert(SQS_ENDPOINT, format!("http://127.0.0.1:{port}"));
        self.endpoints
            .insert(SQS_TLS_ENDPOINT, format!("https://127.0.0.1:{tls_port}"));
        self.containers.push(RunningContainer::Generic(elasticmq));
        self.containers.push(RunningContainer::Generic(proxy));
        Ok(())
    }

    pub(crate) async fn start_mock_server(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("mock-server", scope).await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let image = GenericBuildableImage::new("nervix-cucumber-mock-server", "v1")
            .with_dockerfile(Path::new("docker/mock-server/Dockerfile"))
            .with_file(
                Path::new("docker/mock-server/app.py"),
                "docker/mock-server/app.py",
            )
            .build_image_with(BuildImageOptions::new().with_skip_if_exists(true))
            .await
            .map_err(testcontainers_error("mock server image build"))?;
        let container = self
            .start_container("mock-server", 8080.tcp(), "mock server", || {
                image
                    .clone()
                    .with_exposed_port(8080.tcp())
                    .with_exposed_port(8443.tcp())
                    .with_wait_for(WaitFor::message_on_stdout("mock server ready"))
                    .with_copy_to("/certs/ca.pem", tls.ca_pem.clone())
                    .with_copy_to("/certs/node.pem", tls.node_pem.clone())
                    .with_copy_to(
                        CopyTargetOptions::new("/certs/node-key.pem").with_mode(0o600),
                        tls.node_key_pem.clone(),
                    )
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 8080, "mock HTTP server").await?;
        let tls_port = mapped_port(&container, 8443, "mock HTTPS server").await?;
        self.endpoints
            .insert(MOCK_HTTP_ADDR, format!("http://127.0.0.1:{port}"));
        self.endpoints
            .insert(MOCK_HTTPS_ADDR, format!("https://127.0.0.1:{tls_port}"));
        self.endpoints
            .insert(MOCK_WS_ADDR, format!("ws://127.0.0.1:{port}"));
        self.endpoints
            .insert(MOCK_WSS_ADDR, format!("wss://127.0.0.1:{tls_port}"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_iceberg(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("iceberg", scope).await? {
            return Ok(());
        }
        let network = self.network_name("iceberg-stack");
        let rustfs_name = self.container_name("rustfs");
        let rustfs = self
            .start_container("rustfs", 9000.tcp(), "RustFS", || {
                GenericImage::new("rustfs/rustfs", "latest")
                    .with_exposed_port(9000.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("Starting: /usr/bin/rustfs"))
                    .with_container_name(&rustfs_name)
                    .with_network(&network)
                    .with_env_var("RUSTFS_ADDRESS", ":9000")
                    .with_env_var("RUSTFS_ACCESS_KEY", "rustfsadmin")
                    .with_env_var("RUSTFS_SECRET_KEY", "rustfsadmin")
                    .with_cmd(["/data"])
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let rustfs_port = mapped_port(&rustfs, 9000, "RustFS").await?;

        let init = GenericImage::new("amazon/aws-cli", "2.17.65")
            .with_entrypoint("/bin/sh")
            .with_wait_for(WaitFor::exit(
                testcontainers::core::wait::ExitWaitStrategy::default().with_exit_code(0),
            ))
            .with_network(&network)
            .with_env_var("AWS_ACCESS_KEY_ID", "rustfsadmin")
            .with_env_var("AWS_SECRET_ACCESS_KEY", "rustfsadmin")
            .with_env_var("AWS_DEFAULT_REGION", "us-east-1")
            .with_cmd([
                "-c",
                &format!(
                    "until aws --endpoint-url http://{rustfs_name}:9000 s3api create-bucket \
                     --bucket nervix-iceberg 2>/tmp/error || grep -q BucketAlready /tmp/error; do \
                     sleep 1; done"
                ),
            ])
            .with_startup_timeout(STARTUP_TIMEOUT)
            .start()
            .await
            .map_err(testcontainers_error("RustFS bucket initialization"))?;
        init.rm()
            .await
            .map_err(testcontainers_error("RustFS bucket initializer removal"))?;

        let iceberg_name = self.container_name("iceberg-rest");
        let iceberg = self
            .start_container("iceberg-rest", 8181.tcp(), "Iceberg REST", || {
                GenericImage::new("apache/iceberg-rest-fixture", "1.10.1")
                    .with_exposed_port(8181.tcp())
                    .with_wait_for(WaitFor::healthcheck())
                    .with_container_name(&iceberg_name)
                    .with_network(&network)
                    .with_env_var("AWS_ACCESS_KEY_ID", "rustfsadmin")
                    .with_env_var("AWS_SECRET_ACCESS_KEY", "rustfsadmin")
                    .with_env_var("AWS_REGION", "us-east-1")
                    .with_env_var("CATALOG_WAREHOUSE", "s3://nervix-iceberg/warehouse")
                    .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
                    .with_env_var("CATALOG_S3_ENDPOINT", format!("http://{rustfs_name}:9000"))
                    .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let iceberg_port = mapped_port(&iceberg, 8181, "Iceberg REST").await?;
        self.endpoints
            .insert(RUSTFS_ADDR, format!("http://127.0.0.1:{rustfs_port}"));
        self.endpoints.insert(
            ICEBERG_REST_ADDR,
            format!("http://127.0.0.1:{iceberg_port}"),
        );
        self.containers.push(RunningContainer::Generic(rustfs));
        self.containers.push(RunningContainer::Generic(iceberg));
        Ok(())
    }

    pub(crate) async fn start_gcs(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("gcs", scope).await? {
            return Ok(());
        }
        let container = self
            .start_container("gcs", 4443.tcp(), "fake GCS", || {
                GenericImage::new("fsouza/fake-gcs-server", "latest")
                    .with_exposed_port(4443.tcp())
                    .with_cmd(["-scheme", "http", "-port", "4443", "-backend", "memory"])
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 4443, "fake GCS").await?;
        let endpoint = format!("http://127.0.0.1:{port}");
        provision_gcs_bucket(&endpoint).await?;
        self.endpoints.insert(GCS_ADDR, endpoint);
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_azurite(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("azurite", scope).await? {
            return Ok(());
        }
        let network = self.network_name("azurite-stack");
        let azurite_name = self.container_name("azurite");
        let container = self
            .start_container("azurite", 10000.tcp(), "Azurite", || {
                GenericImage::new("mcr.microsoft.com/azure-storage/azurite", "latest")
                    .with_exposed_port(10000.tcp())
                    .with_wait_for(WaitFor::message_on_either_std(
                        "Azurite Blob service successfully listens",
                    ))
                    .with_container_name(&azurite_name)
                    .with_network(&network)
                    .with_cmd([
                        "azurite-blob",
                        "--blobHost",
                        "0.0.0.0",
                        "--loose",
                        "--skipApiVersionCheck",
                        "--location",
                        "/data",
                    ])
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 10000, "Azurite").await?;
        let connection_string = format!(
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;\
             AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/\
             K1SZFPTOtr/KBHBeksoGMGw==;\
             BlobEndpoint=http://{azurite_name}:10000/devstoreaccount1;"
        );
        let init = GenericImage::new("mcr.microsoft.com/azure-cli", "latest")
            .with_entrypoint("/bin/sh")
            .with_wait_for(WaitFor::exit(
                testcontainers::core::wait::ExitWaitStrategy::default().with_exit_code(0),
            ))
            .with_network(&network)
            .with_env_var("AZURE_STORAGE_CONNECTION_STRING", connection_string)
            .with_cmd([
                "-c",
                "set -e; for attempt in $(seq 1 60); do if az storage container create --name \
                 nervix-iceberg --connection-string \"$AZURE_STORAGE_CONNECTION_STRING\"; then \
                 exit 0; fi; sleep 1; done; exit 1",
            ])
            .with_startup_timeout(STARTUP_TIMEOUT)
            .start()
            .await
            .map_err(testcontainers_error("Azurite container initialization"))?;
        init.rm()
            .await
            .map_err(testcontainers_error("Azurite initializer removal"))?;
        self.endpoints.insert(
            AZURITE_ADDR,
            format!("http://127.0.0.1:{port}/devstoreaccount1"),
        );
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_quickwit(&mut self, scope: &str) -> io::Result<()> {
        if !self.mark_starting("quickwit", scope).await? {
            return Ok(());
        }
        let network = self.network_name("observability-stack");
        let name = self.container_name("quickwit");
        let container = self
            .start_container("quickwit", 7280.tcp(), "Quickwit", || {
                GenericImage::new("quickwit/quickwit", "v0.8.2")
                    .with_exposed_port(7280.tcp())
                    .with_exposed_port(7281.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("REST server is ready"))
                    .with_container_name(&name)
                    .with_network(&network)
                    .with_env_var("QW_ENABLE_OTLP_ENDPOINT", "true")
                    .with_env_var("QW_ENABLE_JAEGER_ENDPOINT", "true")
                    .with_env_var("QW_DISABLE_TELEMETRY", "1")
                    .with_cmd(["run"])
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 7280, "Quickwit").await?;
        let otlp_port = mapped_port(&container, 7281, "Quickwit OTLP").await?;
        self.endpoints
            .insert(QUICKWIT_ADDR, format!("http://127.0.0.1:{port}"));
        self.endpoints
            .insert(OTLP_ADDR, format!("http://127.0.0.1:{otlp_port}"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn start_jaeger(&mut self, scope: &str) -> io::Result<()> {
        if let Err(error) = self.start_quickwit(scope).await {
            self.running.remove("quickwit");
            return Err(error);
        }
        if !self.mark_starting("jaeger", scope).await? {
            return Ok(());
        }
        let quickwit_name = self.container_name("quickwit");
        let network = self.network_name("observability-stack");
        let container = self
            .start_container("jaeger", 16686.tcp(), "Jaeger", || {
                GenericImage::new("jaegertracing/jaeger-query", "1.60")
                    .with_exposed_port(16686.tcp())
                    .with_wait_for(WaitFor::message_on_either_std("Query server started"))
                    .with_network(&network)
                    .with_env_var("SPAN_STORAGE_TYPE", "grpc")
                    .with_env_var("GRPC_STORAGE_SERVER", format!("{quickwit_name}:7281"))
                    .with_env_var("GRPC_STORAGE_TLS", "false")
                    .with_startup_timeout(STARTUP_TIMEOUT)
            })
            .await?;
        let port = mapped_port(&container, 16686, "Jaeger").await?;
        self.endpoints
            .insert(JAEGER_ADDR, format!("http://127.0.0.1:{port}"));
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }

    pub(crate) async fn shutdown(&mut self) -> Vec<String> {
        let mut errors = Vec::new();
        for container in std::mem::take(&mut self.containers).into_iter().rev() {
            if self.mode.is_reusable() {
                drop(container);
            } else if let Err(error) = container.stop_and_remove().await {
                errors.push(error);
            }
        }
        self.running.clear();
        self.endpoints = DependencyEndpoints::default();
        self.tls = None;
        self.tls_configuration_hash = None;
        self.scope = None;
        errors
    }

    async fn mark_starting(&mut self, dependency: &'static str, scope: &str) -> io::Result<bool> {
        if self.scope.is_none() {
            self.scope = Some(scope.to_string());
        }
        Ok(self.running.insert(dependency))
    }

    fn ensure_tls(&mut self) -> io::Result<&TlsMaterials> {
        if self.tls.is_none() {
            let tls = TlsMaterials::generate(self.mode)?;
            self.tls_configuration_hash = Some(blake3::hash(&tls.ca_pem).to_hex().to_string());
            self.endpoints.set_tls(&tls);
            self.tls = Some(tls);
        }
        Ok(self.tls.as_ref().expect("TLS materials were initialized"))
    }

    async fn start_container<I, Build>(
        &self,
        role: &'static str,
        ready_port: ContainerPort,
        operation: &'static str,
        mut build: Build,
    ) -> io::Result<ContainerAsync<I>>
    where
        I: Image,
        Build: FnMut() -> ContainerRequest<I>,
    {
        let _startup_lock = if self.mode.is_reusable() {
            Some(ReusableStartupLock::acquire(&self.configuration_hash(role), operation).await?)
        } else {
            None
        };
        let mut start_retries = 0_u8;
        let mut replaced_failed_start = false;
        let mut replaced_unhealthy = false;
        loop {
            let name = self.container_name(role);
            let reusable_was_running =
                self.mode.is_reusable() && container_is_running_by_name(&name).await?;
            let container = match self.configure_container(role, build()).start().await {
                Ok(container) => container,
                Err(_error)
                    if self.mode.is_reusable() && reusable_was_running && start_retries < 3 =>
                {
                    start_retries += 1;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                Err(error) if self.mode.is_reusable() && reusable_was_running => {
                    return Err(io::Error::other(format!(
                        "{operation} could not attach to running reusable container {name} after \
                         {} attempts: {error}; the container was preserved because another test \
                         suite may be using it",
                        start_retries + 1
                    )));
                }
                Err(error)
                    if self.mode.is_reusable()
                        && !replaced_failed_start
                        && container_was_created(&error) =>
                {
                    remove_container_by_name_or_id(&name)
                        .await
                        .map_err(|remove_error| {
                            io::Error::other(format!(
                                "{operation} reusable container {name} failed during startup \
                                 ({error}) and could not be removed: {remove_error}"
                            ))
                        })?;
                    replaced_failed_start = true;
                    start_retries = 0;
                    continue;
                }
                Err(_error) if self.mode.is_reusable() && start_retries < 3 => {
                    // Another process may have won the deterministic-name race after our
                    // preflight inspect. Let it finish startup, then attach on the next attempt.
                    start_retries += 1;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                Err(error) => return Err(testcontainers_error(operation)(error)),
            };

            match wait_for_container_tcp(&container, ready_port, operation).await {
                Ok(()) => return Ok(container),
                Err(probe_error)
                    if self.mode.is_reusable() && !reusable_was_running && !replaced_unhealthy =>
                {
                    let id = container.id().to_string();
                    container.rm().await.map_err(|remove_error| {
                        io::Error::other(format!(
                            "{operation} reusable container {id} failed its readiness probe \
                             ({probe_error}) and could not be removed: {remove_error}"
                        ))
                    })?;
                    replaced_unhealthy = true;
                }
                Err(error) if self.mode.is_reusable() && reusable_was_running => {
                    return Err(io::Error::other(format!(
                        "{operation} reusable container {name} failed its readiness probe \
                         ({error}); the container was preserved because another test suite may be \
                         using it"
                    )));
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn configure_container<I: Image>(
        &self,
        role: &'static str,
        request: ContainerRequest<I>,
    ) -> ContainerRequest<I> {
        let config_hash = self.configuration_hash(role);
        let request = request.with_labels([
            (TESTCONTAINERS_CONFIG_LABEL, config_hash.clone()),
            (TESTCONTAINERS_ROLE_LABEL, role.to_string()),
            (
                TESTCONTAINERS_REUSABLE_LABEL,
                self.mode.is_reusable().to_string(),
            ),
        ]);
        if self.mode.is_reusable() {
            request
                .with_container_name(reusable_container_name(role, &config_hash))
                .with_reuse(ReuseDirective::Always)
        } else {
            // The suite registry retains this handle for the complete Cucumber run. Never gives
            // every ownership path Testcontainers' removal-on-drop fallback; shutdown() still
            // performs an explicit graceful stop and removal so teardown failures are reported.
            request.with_reuse(ReuseDirective::Never)
        }
    }

    fn network_name(&self, stack: &'static str) -> String {
        if self.mode.is_reusable() {
            let hash = self.configuration_hash(stack);
            format!("nervix-cucumber-{stack}-{}", short_hash(&hash))
        } else {
            format!("nervix-cucumber-{}", self.scope_token())
        }
    }

    fn container_name(&self, role: &'static str) -> String {
        if self.mode.is_reusable() {
            let hash = self.configuration_hash(role);
            reusable_container_name(role, &hash)
        } else {
            format!("nervix-{role}-{}", self.scope_token())
        }
    }

    fn scope_token(&self) -> &str {
        self.scope
            .as_deref()
            .expect("dependency scope must be initialized")
    }

    fn configuration_hash(&self, role: &str) -> String {
        let base_hash = dependency_configuration_hash(role);
        let Some(tls_hash) = &self.tls_configuration_hash else {
            return base_hash;
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(base_hash.as_bytes());
        hasher.update(b"\0tls-ca\0");
        hasher.update(tls_hash.as_bytes());
        hasher.finalize().to_hex().to_string()
    }
}

#[derive(Debug)]
struct TlsDirectory {
    path: PathBuf,
    _temporary: Option<TempDir>,
}

impl TlsDirectory {
    fn temporary(directory: TempDir) -> Self {
        Self {
            path: directory.path().to_path_buf(),
            _temporary: Some(directory),
        }
    }

    fn persistent(path: PathBuf) -> Self {
        Self {
            path,
            _temporary: None,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone, Debug)]
struct TlsMaterials {
    dir: Arc<TlsDirectory>,
    ca_path: PathBuf,
    ca_pem: Vec<u8>,
    node_pem: Vec<u8>,
    node_key_pem: Vec<u8>,
    kafka_keystore: Vec<u8>,
}

impl TlsMaterials {
    fn generate(mode: ContainerMode) -> io::Result<Self> {
        if !mode.is_reusable() {
            let directory = Arc::new(TlsDirectory::temporary(tempdir()?));
            Self::generate_at(directory.path())?;
            return Self::load(directory);
        }

        let cache_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(".nervix-deps")
            .join("testcontainers")
            .join("tls");
        fs::create_dir_all(&cache_root)?;
        let cache_path = cache_root.join(dependency_configuration_hash("tls-materials"));
        if cache_path.exists() {
            return Self::load(Arc::new(TlsDirectory::persistent(cache_path)));
        }

        let staging = tempdir_in(&cache_root)?;
        Self::generate_at(staging.path())?;
        match fs::rename(staging.path(), &cache_path) {
            Ok(()) => {}
            Err(error) if cache_path.exists() => {
                // A concurrent test process won the race. Its directory was atomically renamed
                // only after every certificate and key had been written.
                drop(error);
            }
            Err(error) => return Err(error),
        }
        Self::load(Arc::new(TlsDirectory::persistent(cache_path)))
    }

    fn generate_at(directory: &Path) -> io::Result<()> {
        let ca_path = directory.join("ca.pem");
        let ca_key_path = directory.join("ca-key.pem");
        let node_path = directory.join("node.pem");
        let node_key_path = directory.join("node-key.pem");
        let node_csr_path = directory.join("node.csr");
        let ca_config_path = directory.join("ca.cnf");
        let leaf_config_path = directory.join("leaf.cnf");
        let keystore_path = directory.join("kafka.keystore.p12");

        fs::write(
            &ca_config_path,
            r#"[ req ]
default_bits = 2048
distinguished_name = req_distinguished_name
prompt = no
x509_extensions = v3_ca

[ req_distinguished_name ]
CN = nervix-cucumber-ca

[ v3_ca ]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
"#,
        )?;
        fs::write(
            &leaf_config_path,
            r#"[ req ]
default_bits = 2048
distinguished_name = req_leaf_distinguished_name
prompt = no
req_extensions = v3_req

[ req_leaf_distinguished_name ]
CN = localhost

[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1
"#,
        )?;

        run_openssl(
            vec![
                "req".into(),
                "-x509".into(),
                "-newkey".into(),
                "rsa:2048".into(),
                "-nodes".into(),
                "-keyout".into(),
                path_argument(&ca_key_path),
                "-out".into(),
                path_argument(&ca_path),
                "-days".into(),
                "3650".into(),
                "-config".into(),
                path_argument(&ca_config_path),
            ],
            "generate the scenario certificate authority",
        )?;
        run_openssl(
            vec![
                "req".into(),
                "-new".into(),
                "-newkey".into(),
                "rsa:2048".into(),
                "-nodes".into(),
                "-keyout".into(),
                path_argument(&node_key_path),
                "-out".into(),
                path_argument(&node_csr_path),
                "-config".into(),
                path_argument(&leaf_config_path),
            ],
            "generate the scenario server certificate request",
        )?;
        run_openssl(
            vec![
                "x509".into(),
                "-req".into(),
                "-in".into(),
                path_argument(&node_csr_path),
                "-CA".into(),
                path_argument(&ca_path),
                "-CAkey".into(),
                path_argument(&ca_key_path),
                "-CAcreateserial".into(),
                "-out".into(),
                path_argument(&node_path),
                "-days".into(),
                "3650".into(),
                "-extensions".into(),
                "v3_req".into(),
                "-extfile".into(),
                path_argument(&leaf_config_path),
            ],
            "sign the scenario server certificate",
        )?;
        run_openssl(
            vec![
                "pkcs12".into(),
                "-export".into(),
                "-in".into(),
                path_argument(&node_path),
                "-inkey".into(),
                path_argument(&node_key_path),
                "-name".into(),
                "kafka".into(),
                "-certfile".into(),
                path_argument(&ca_path),
                "-out".into(),
                path_argument(&keystore_path),
                "-passout".into(),
                format!("pass:{KAFKA_KEYSTORE_PASSWORD}").into(),
            ],
            "generate the Kafka PKCS#12 keystore",
        )?;

        Ok(())
    }

    fn load(dir: Arc<TlsDirectory>) -> io::Result<Self> {
        let ca_path = dir.path().join("ca.pem");
        let ca_pem = fs::read(&ca_path)?;
        let node_pem = fs::read(dir.path().join("node.pem"))?;
        let node_key_pem = fs::read(dir.path().join("node-key.pem"))?;
        let kafka_keystore = fs::read(dir.path().join("kafka.keystore.p12"))?;
        Ok(Self {
            dir,
            ca_path,
            ca_pem,
            node_pem,
            node_key_pem,
            kafka_keystore,
        })
    }
}

fn dependency_configuration_hash(role: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"nervix-cucumber-testcontainer-v1\0");
    hasher.update(role.as_bytes());
    hasher.update(&[0]);
    hasher.update(DEPENDENCY_CONFIGURATION_SOURCE);
    for file in DEPENDENCY_CONFIGURATION_FILES {
        hasher.update(&(file.len() as u64).to_le_bytes());
        hasher.update(file);
    }
    hasher.finalize().to_hex().to_string()
}

fn short_hash(hash: &str) -> &str {
    hash.get(..16)
        .expect("BLAKE3 hashes contain at least 16 ASCII characters")
}

fn reusable_container_name(role: &str, config_hash: &str) -> String {
    format!("nervix-cucumber-{role}-{}", short_hash(config_hash))
}

fn container_was_created(error: &TestcontainersError) -> bool {
    matches!(
        error,
        TestcontainersError::WaitContainer(_)
            | TestcontainersError::PortNotExposed { .. }
            | TestcontainersError::MissingInfo(_)
            | TestcontainersError::Exec(_)
    )
}

struct ReusableStartupLock(fs::File);

impl ReusableStartupLock {
    async fn acquire(configuration_hash: &str, operation: &str) -> io::Result<Self> {
        let path = env::temp_dir().join(format!(
            "nervix-testcontainers-{}.lock",
            short_hash(configuration_hash)
        ));
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            tokio::task::consume_budget().await;
            match file.try_lock() {
                Ok(()) => return Ok(Self(file)),
                Err(fs::TryLockError::WouldBlock) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(fs::TryLockError::WouldBlock) => {
                    return Err(io::Error::other(format!(
                        "timed out waiting for another process to finish starting {operation} \
                         reusable container"
                    )));
                }
                Err(fs::TryLockError::Error(error)) => return Err(error),
            }
        }
    }
}

impl Drop for ReusableStartupLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

async fn container_is_running_by_name(name: &str) -> io::Result<bool> {
    let docker = Docker::connect_with_defaults().map_err(io::Error::other)?;
    match docker.inspect_container(name, None).await {
        Ok(container) => Ok(container
            .state
            .and_then(|state| state.running)
            .unwrap_or(false)),
        Err(testcontainers::bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }) => Ok(false),
        Err(error) => Err(io::Error::other(error)),
    }
}

async fn container_exists_by_name_or_id(name_or_id: &str) -> io::Result<bool> {
    let docker = Docker::connect_with_defaults().map_err(io::Error::other)?;
    match docker.inspect_container(name_or_id, None).await {
        Ok(_) => Ok(true),
        Err(testcontainers::bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }) => Ok(false),
        Err(error) => Err(io::Error::other(error)),
    }
}

async fn remove_container_by_name_or_id(name_or_id: &str) -> io::Result<()> {
    let docker = Docker::connect_with_defaults().map_err(io::Error::other)?;
    let options = RemoveContainerOptionsBuilder::new()
        .force(true)
        .v(true)
        .build();
    match docker.remove_container(name_or_id, Some(options)).await {
        Ok(()) => Ok(()),
        Err(testcontainers::bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }) => Ok(()),
        Err(error) => Err(io::Error::other(error)),
    }
}

fn path_argument(path: &Path) -> OsString {
    path.as_os_str().to_owned()
}

fn run_openssl(arguments: Vec<OsString>, operation: &str) -> io::Result<()> {
    let output = Command::new("openssl")
        .args(arguments)
        .output()
        .map_err(io::Error::other)?;
    if output.status.success() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "openssl failed to {operation}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

#[derive(Debug)]
enum RunningContainer {
    Generic(ContainerAsync<GenericImage>),
    Kafka(ContainerAsync<KafkaImage>),
}

impl RunningContainer {
    fn id(&self) -> String {
        match self {
            Self::Generic(container) => container.id().to_string(),
            Self::Kafka(container) => container.id().to_string(),
        }
    }

    async fn stop_and_remove(self) -> Result<(), String> {
        match self {
            Self::Generic(container) => stop_and_remove(container).await,
            Self::Kafka(container) => stop_and_remove(container).await,
        }
    }
}

async fn stop_and_remove<I: Image>(container: ContainerAsync<I>) -> Result<(), String> {
    let id = container.id().to_string();
    let stop_error = container
        .stop_with_timeout(Some(10))
        .await
        .err()
        .map(|error| error.to_string());
    let remove_error = container.rm().await.err().map(|error| error.to_string());
    match (stop_error, remove_error) {
        (None, None) => Ok(()),
        (Some(stop), None) => Err(format!("failed to stop test container {id}: {stop}")),
        (None, Some(remove)) => Err(format!("failed to remove test container {id}: {remove}")),
        (Some(stop), Some(remove)) => Err(format!(
            "failed to stop test container {id}: {stop}; failed to remove it: {remove}"
        )),
    }
}

async fn wait_for_container_tcp<I: Image>(
    container: &ContainerAsync<I>,
    port: ContainerPort,
    dependency: &'static str,
) -> io::Result<()> {
    let host_port = container
        .get_host_port_ipv4(port)
        .await
        .map_err(testcontainers_error(dependency))?;
    let deadline = tokio::time::Instant::now() + REUSABLE_READY_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        let connection_error = match tokio::net::TcpStream::connect(("127.0.0.1", host_port)).await
        {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(error) => error,
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "{dependency} container {} did not accept TCP connections on random host port \
                 {host_port} within {} seconds: {}",
                container.id(),
                REUSABLE_READY_TIMEOUT.as_secs(),
                connection_error
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn mapped_port<I: Image>(
    container: &ContainerAsync<I>,
    port: u16,
    dependency: &'static str,
) -> io::Result<u16> {
    container
        .get_host_port_ipv4(port.tcp())
        .await
        .map_err(testcontainers_error(dependency))
}

async fn provision_gcs_bucket(endpoint: &str) -> io::Result<()> {
    let client = reqwest::Client::new();
    let create_url = format!("{endpoint}/storage/v1/b?project=nervix");
    let inspect_url = format!("{endpoint}/storage/v1/b/nervix-iceberg");
    let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        let attempt_error = match client
            .post(&create_url)
            .json(&serde_json::json!({ "name": "nervix-iceberg" }))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => {
                let create_status = response.status();
                match client.get(&inspect_url).send().await {
                    Ok(response) if response.status().is_success() => return Ok(()),
                    Ok(response) => format!(
                        "create returned {create_status}; lookup returned {}",
                        response.status()
                    ),
                    Err(error) => {
                        format!("create returned {create_status}; lookup failed: {error}")
                    }
                }
            }
            Err(error) => error.to_string(),
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "fake GCS did not provision bucket 'nervix-iceberg' before timeout: \
                 {attempt_error}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn testcontainers_error(operation: &'static str) -> impl FnOnce(TestcontainersError) -> io::Error {
    move |error| io::Error::other(format!("{operation} test container failed: {error}"))
}

#[derive(Clone, Debug)]
struct KafkaImage {
    env: BTreeMap<String, String>,
}

impl Default for KafkaImage {
    fn default() -> Self {
        let env = [
            ("KAFKA_NODE_ID", "1"),
            ("KAFKA_PROCESS_ROLES", "broker,controller"),
            (
                "KAFKA_LISTENERS",
                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,SSL://0.0.0.0:9094,CONTROLLER://0.\
                 0.0.0:9095",
            ),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,SSL:SSL,CONTROLLER:PLAINTEXT",
            ),
            ("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
            ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
            ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9095"),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
            ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
            ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
            ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
            ("KAFKA_SSL_KEYSTORE_FILENAME", "kafka.keystore.p12"),
            ("KAFKA_SSL_KEYSTORE_CREDENTIALS", "kafka_keystore_creds"),
            ("KAFKA_SSL_KEY_CREDENTIALS", "kafka_key_creds"),
            ("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        Self { env }
    }
}

impl Image for KafkaImage {
    fn name(&self) -> &str {
        "apache/kafka"
    }

    fn tag(&self) -> &str {
        "3.9.1"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        Vec::new()
    }

    fn entrypoint(&self) -> Option<&str> {
        Some("bash")
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        [
            "-c".to_string(),
            format!(
                "while [ ! -f {KAFKA_START_SCRIPT} ]; do sleep 0.1; done; chmod 755 \
                 {KAFKA_START_SCRIPT}; exec {KAFKA_START_SCRIPT}"
            ),
        ]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[KAFKA_PLAINTEXT_PORT, KAFKA_TLS_PORT]
    }

    fn exec_after_start(
        &self,
        state: ContainerState,
    ) -> Result<Vec<ExecCommand>, TestcontainersError> {
        let plaintext_port = state.host_port_ipv4(KAFKA_PLAINTEXT_PORT)?;
        let tls_port = state.host_port_ipv4(KAFKA_TLS_PORT)?;
        let script = format!(
            "#!/usr/bin/env bash\nset -eu\nexport \
             KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{plaintext_port},BROKER://localhost:\
             9093,SSL://localhost:{tls_port}\nexec /etc/kafka/docker/run\n"
        );
        Ok(vec![
            ExecCommand::new(vec![
                "sh".to_string(),
                "-c".to_string(),
                format!(
                    "printf '%s' '{}' > {KAFKA_START_SCRIPT}.tmp && chmod 755 \
                     {KAFKA_START_SCRIPT}.tmp && mv {KAFKA_START_SCRIPT}.tmp {KAFKA_START_SCRIPT}",
                    script.replace('\'', "'\\''")
                ),
            ])
            .with_container_ready_conditions(vec![WaitFor::message_on_stdout(
                "Kafka Server started",
            )]),
        ])
    }
}
