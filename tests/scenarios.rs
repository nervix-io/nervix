#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;
#[cfg(feature = "shuttle")]
extern crate shuttle_tokio_util as tokio_util;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    fs::{OpenOptions, create_dir_all},
    io::Write,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU64,
    os::unix::process::ExitStatusExt as _,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::{Arc as StdArc, Mutex as StdMutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arch_into::ArchInto as _;
use arrow_array::{
    Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    TimeUnit as ArrowTimeUnit,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cucumber::{
    World as _, WriterExt,
    event::ScenarioFinished,
    gherkin::Step,
    given, then, when,
    writer::{self, Stats as _},
};
use futures_util::{
    TryStreamExt,
    future::{join_all, try_join_all},
};
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    arrow::arrow_schema_to_schema_auto_assign_ids,
    io::{
        FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA,
        S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
    },
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use meticulous::{OptionExt as _, ResultExt as _};
use mongodb::{
    Client as MongoDbClient,
    bson::{Bson as MongoDbBson, Document as MongoDbDocument, doc as mongodb_doc},
    options::{
        ClientOptions as MongoDbClientOptions, Tls as MongoDbTls, TlsOptions as MongoDbTlsOptions,
    },
};
use mysql_async::{
    Opts as MySqlOpts, OptsBuilder as MySqlOptsBuilder, Pool as MySqlPool, SslOpts as MySqlSslOpts,
    prelude::Queryable as MySqlQueryable,
};
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use nervix_client_core::{Client, CommandOutcome as ClientCommandOutcome};
use nervix_recovery::Discarded as _;
use nervix_server::{
    FaultInjection, SchedulerMode, WasmStateResetRequestError, application::InternalTransportMode,
    memory_pressure::MemoryPressureConfig,
};
use nervix_test_environment::{TestParallelism, TestParallelismArgs};
use nervix_wasm::{
    WasmAckSidecar, WasmAckToken, WasmEnvelope, WasmOutputColumnRef, WasmOutputRow,
    WasmRoutedOutput,
};
use playwright_rs::{
    FilePayload, LaunchOptions, Playwright, Viewport, WaitForOptions, WaitForState,
};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject};
use sqlx::{
    AssertSqlSafe as SqlxAssertSqlSafe, Row as _,
    postgres::{
        PgConnectOptions as SqlxPgConnectOptions, PgPool as SqlxPgPool,
        PgPoolOptions as SqlxPgPoolOptions, PgSslMode as SqlxPgSslMode,
    },
};
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt as _;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use uuid::Uuid;

use crate::common::{
    client_conformance::{ClientProbe, ProbeRuntime, ProbeTarget, SUBSCRIBED_LINE, corpus_report},
    cluster::{
        BrokerObserver, Cluster, DOMAIN_CLOCK_AUTHORITY_OBSERVATION_TIMEOUT,
        HttpsPublishLoopOutcome, InterconnectCredentialFault, StallableTcpProxy,
        TEST_AUTH_PASSWORD, TEST_AUTH_USERNAME, TestClusterConfig, TestSession,
        WebsocketExchangeAction, client_connect_options, client_domain,
    },
    cluster_teardown::CLUSTER_TEARDOWN_BUDGET,
    dependencies::{
        CLICKHOUSE_ADDR, CLICKHOUSE_TLS_ADDR, DependencyEndpoints, ICEBERG_REST_ADDR, KAFKA_ADDR,
        KAFKA_DOCKER_ADDR, KAFKA_DOCKER_NETWORK, MOCK_HTTP_ADDR, MONGODB_ADDR, MONGODB_TLS_ADDR,
        MQTT_ADDR, MYSQL_ADDR, MYSQL_TLS_ADDR, POSTGRES_ADDR, POSTGRES_TLS_ADDR, PULSAR_ADDR,
        RABBITMQ_ADDR, REDIS_ADDR, RUSTFS_ADDR, TestDependencies,
    },
    http_receiver::{
        ClientCertificatePolicy, HttpReceiver, RECEIVER_STOP_BUDGET, ReceiverFault,
        ReceiverResponse, ReceiverTlsOptions, ReceiverTransport,
    },
    phase_deadline::{BeforeDeadline, PhaseDeadline},
    raw_session::{TestUpload, TestUploadPart, WireOutcome as _},
    scenario_phase::{ActiveScenario, ActiveScenarioRegistration, ScenarioIdentity, ScenarioPhase},
    server_process::{
        HeldResourceUpload, HeldUploadProgress, ServerProcess, ServerProcessHttpLoad,
        ServerProcessLaunch, ServerProcessOption, describe_exit,
    },
    status_request::{STATUS_DIAGNOSTIC_BUDGET, STATUS_REQUEST_TIMEOUT, StatusRequestError},
    suite_watchdog::{
        RUNTIME_SHUTDOWN_BUDGET, SuiteOutcome, SuiteRun, SuiteTeardown, SuiteWatchdogArgs,
    },
};

mod common;
mod ingestion_time;
mod session_protocol;

const SCENARIOS_PATH: &str = "tests/features";
const TEST_LOG_DIR: &str = "tests/logs";
const CUCUMBER_LOG_FILE: &str = "tests/logs/cucumber.log";
static ONNX_RUNTIME_INIT: OnceLock<Result<(), String>> = OnceLock::new();
static ICEBERG_TABLE_PROVISION_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
static SUITE_DEPENDENCY_ENDPOINTS: OnceLock<StdMutex<BTreeMap<String, String>>> = OnceLock::new();
// Every scenario holds a read guard; `@exclusive` scenarios hold the write guard.
static SCENARIO_EXECUTION_LOCK: OnceLock<StdArc<tokio::sync::RwLock<()>>> = OnceLock::new();
static WEB_CONSOLE_SCENARIO_PERMITS: OnceLock<StdArc<tokio::sync::Semaphore>> = OnceLock::new();
static WASM_STATE_RESET_SCENARIO_PERMITS: OnceLock<StdArc<tokio::sync::Semaphore>> =
    OnceLock::new();
const MAX_CONCURRENT_WEB_CONSOLE_SCENARIOS: usize = 2;
const MAX_CONCURRENT_WASM_STATE_RESET_SCENARIOS: usize = 1;
const WEB_CONSOLE_ASSERTION_TIMEOUT: Duration = Duration::from_secs(30);
const ZEROMQ_OBSERVER_BIND_ATTEMPTS: usize = 8;
const DURABLE_CATCH_UP_STORAGE_COMMITS_PER_ENTRY: u32 = 2;
const DURABLE_CATCH_UP_MARGIN: Duration = Duration::from_secs(5);
const DURABLE_CATCH_UP_WRITE_CADENCE: Duration = Duration::from_millis(100);
const MAX_DURABLE_CATCH_UP_WRITES: usize = 128;
/// The execution class a follower charges its decoded append batches to.
const COMMANDS_MEMORY_LABEL: &str = "class=\"commands\"";
const BULK_MEMORY_LABEL: &str = "class=\"bulk\"";
const WEB_CONSOLE_FEATURE_NAMES: [&str; 2] =
    ["Web console NSPL REPL", "Web console execution graph"];
const WASM_STATE_RESET_FEATURE_NAME: &str = "Coordinated WASM processor state reset";
const DEPENDENCY_LIFECYCLE_HELPER_ENV: &str = "NERVIX_DEPENDENCY_LIFECYCLE_HELPER";
const DEPENDENCY_LIFECYCLE_STARTED: &str = "NERVIX_DEPENDENCY_LIFECYCLE_STARTED=";

#[derive(Debug)]
enum ScenarioExecutionPermit {
    Concurrent {
        _permit: tokio::sync::OwnedRwLockReadGuard<()>,
    },
    Exclusive {
        _permit: tokio::sync::OwnedRwLockWriteGuard<()>,
    },
}

/// What a scenario's own steps did, as its after hook sees it.
///
/// It is the result of the scenario body alone. Everything the after hook does afterwards is
/// cleanup, and cleanup reports itself separately, so a log line never blames a scenario for what
/// its teardown found.
#[derive(Debug)]
enum ScenarioBodyResult {
    Passed,
    Skipped,
    BeforeHookFailed,
    StepFailed(String),
}

impl From<&ScenarioFinished> for ScenarioBodyResult {
    fn from(finished: &ScenarioFinished) -> Self {
        match finished {
            ScenarioFinished::StepPassed => Self::Passed,
            ScenarioFinished::StepSkipped => Self::Skipped,
            ScenarioFinished::BeforeHookFailed(_) => Self::BeforeHookFailed,
            ScenarioFinished::StepFailed(_, _, error) => {
                Self::StepFailed(error.to_string().replace('\n', "\\n"))
            }
        }
    }
}

impl fmt::Display for ScenarioBodyResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Passed => formatter.write_str("passed"),
            Self::Skipped => formatter.write_str("skipped"),
            Self::BeforeHookFailed => formatter.write_str("before hook failed"),
            Self::StepFailed(error) => write!(formatter, "step failed: {error}"),
        }
    }
}

#[derive(Clone, Debug)]
struct DurableCatchUpObservation {
    follower: String,
    initial_leader: String,
    commit_delay: Duration,
    started_at: Instant,
    append_stream_opens_at_start: BTreeMap<String, u64>,
}

struct DurableCatchUpWriter {
    cancellation: CancellationToken,
    prefix: String,
    task: AbortOnDropHandle<Result<usize, String>>,
}

/// HTTPS posts to every node that keep running until a step collects what the listeners answered.
struct BackgroundHttpsPublish {
    stop: CancellationToken,
    task: AbortOnDropHandle<HttpsPublishLoopOutcome>,
}

/// A follower's commands-class memory, sampled for the whole time it spends catching up.
///
/// The batches it has decoded and not yet answered are charged to that class, and they are held
/// only while its Raft core is behind. Sampling throughout catches the peak the catch-up reached
/// rather than whatever the class happened to hold once it was over.
struct FollowerCommandsMemoryObservation {
    node_id: String,
    cancellation: CancellationToken,
    task: AbortOnDropHandle<f64>,
}

#[derive(cucumber::World, Default)]
struct ScenarioWorld {
    scenario_execution_permit: Option<ScenarioExecutionPermit>,
    /// Publishes which phase this scenario is in for as long as its world lives, so a reader of
    /// the registry sees the work in flight rather than the last work that finished.
    active_scenario: Option<ActiveScenarioRegistration>,
    cluster: Option<Cluster>,
    active_session: Option<TestSession>,
    active_session_node: Option<String>,
    active_session_has_subscription: bool,
    transaction_clients: BTreeMap<String, Client>,
    /// Rows a named client received and a step has not taken yet, as the client displays them.
    client_subscription_rows: BTreeMap<String, VecDeque<String>>,
    /// Requests the active session sent under names a scenario gave them.
    session_requests: BTreeMap<String, nervix_client_wire::RequestId>,
    /// The reply to the last upload stream a scenario shaped itself.
    last_upload_reply: Option<nervix_client_wire::UploadReply>,
    last_subscription_payload: Option<String>,
    /// When the message a delivery-delay assertion is about was published. Load moves this
    /// instant and the arrival together, which is what makes such an assertion hold on a
    /// busy machine where a fixed wall-clock window does not.
    last_publish_at: Option<Instant>,
    last_command_error: Option<String>,
    last_command_output: Option<String>,
    last_cli_output: Option<Output>,
    cli_subscription_process: Option<tokio::process::Child>,
    cli_subscription_lines: Option<StdArc<StdMutex<VecDeque<String>>>>,
    cli_subscription_reader: Option<AbortOnDropHandle<()>>,
    /// The whole outcome of the last command a named client ran, for assertions that read more
    /// than its message.
    last_client_outcome: Option<ClientCommandOutcome>,
    /// Reused when a coordinated WASM reset is retried after an ambiguous or failed response.
    wasm_state_reset_reference: Option<nervix_models::CommandExecutionReference>,
    /// The plan block `DESCRIBE RELOCATION` returned, so the executing `RELOCATE` can be compared
    /// against it verbatim.
    saved_relocation_plan: Option<String>,
    last_server_error: Option<String>,
    last_auth_attempts_elapsed: Option<Duration>,
    broker_observer: Option<BrokerObserver>,
    last_broker_payload: Option<String>,
    last_broker_headers: Vec<(String, String)>,
    clickhouse_table: Option<String>,
    clickhouse_tls: bool,
    postgres_table: Option<String>,
    postgres_tls: bool,
    /// Releases the Postgres table lock a contention scenario is holding, if one is held. The
    /// lock lives in a spawned task because it must outlive the step that took it.
    postgres_lock_release: Option<tokio::sync::oneshot::Sender<()>>,
    mysql_table: Option<String>,
    mysql_tls: bool,
    mysql_insert_command_baseline: Option<u64>,
    mongodb_collection: Option<String>,
    mongodb_tls: bool,
    domain: String,
    test_id: String,
    zeromq_ingest_addr: String,
    zeromq_emit_addr: String,
    syslog_ingest_addr: String,
    syslog_emit_addr: String,
    /// Every port this scenario drew for fixtures of its own: the ZeroMQ and syslog addresses its
    /// nodes and its observers bind. Given back once cleanup has stopped both.
    scenario_ports: Vec<u16>,
    syslog_udp_observer: Option<tokio::net::UdpSocket>,
    placeholders: BTreeMap<String, String>,
    /// Human-readable references in scenarios map to UUIDv7 identities so retries retain one
    /// stable creation timestamp while feature text remains legible.
    command_execution_references: BTreeMap<String, String>,
    mqtt_ingestors_by_domain: BTreeMap<String, BTreeSet<String>>,
    avro_http_field_order: Vec<String>,
    avro_http_optional_fields: BTreeSet<String>,
    fault_injection: FaultInjection,
    consensus_commit_delays: BTreeMap<String, Duration>,
    burst_raft_retention_peak: Option<nervix_consensus::RaftLogRetention>,
    durable_catch_up: Option<DurableCatchUpObservation>,
    durable_catch_up_writer: Option<DurableCatchUpWriter>,
    follower_commands_memory: Option<FollowerCommandsMemoryObservation>,
    cluster_config: TestClusterConfig,
    temp_root: Option<TempDir>,
    formatter_root: Option<TempDir>,
    formatter_exit_code: Option<i32>,
    /// Contents each NSPL file was given, so "is unchanged" has something to compare against.
    formatter_original_files: BTreeMap<String, String>,
    last_cluster_operation_elapsed: Option<Duration>,
    browser_page: Option<playwright_rs::Page>,
    browser_context: Option<playwright_rs::BrowserContext>,
    browser: Option<playwright_rs::Browser>,
    playwright: Option<Playwright>,
    web_console_scenario_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    wasm_state_reset_scenario_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    dependencies: TestDependencies,
    background_nspl: Option<AbortOnDropHandle<Result<String, String>>>,
    background_command_result:
        Option<AbortOnDropHandle<std::io::Result<nervix_client_wire::CommandOutcome>>>,
    background_http_publish: Option<AbortOnDropHandle<std::io::Result<()>>>,
    background_https_publish: Option<BackgroundHttpsPublish>,
    stallable_tcp_proxies: BTreeMap<String, StallableTcpProxy>,
    /// The HTTP receivers a scenario started, by the name its steps give them.
    http_receivers: BTreeMap<String, HttpReceiver>,
    silent_interconnect_peers: Vec<tokio::net::TcpStream>,
    last_interconnect_attempt_error: Option<String>,
    server_process: Option<ServerProcess>,
    server_process_http_load: Option<ServerProcessHttpLoad>,
    held_resource_upload: Option<HeldResourceUpload>,
    /// When the last signal was sent to the server process, taken before the signal is delivered
    /// so an exit measured against it can only look later, never earlier.
    last_server_signal_at: Option<Instant>,
    /// The cross-language client probe a scenario started, until a step reads its report.
    client_probe: Option<ClientProbe>,
}

impl fmt::Debug for ScenarioWorld {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScenarioWorld")
            .field("domain", &self.domain)
            .field("test_id", &self.test_id)
            .field("cluster_initialized", &self.cluster.is_some())
            .field("active_session", &self.active_session.is_some())
            .field("active_session_node", &self.active_session_node)
            .field(
                "active_session_has_subscription",
                &self.active_session_has_subscription,
            )
            .field("transaction_client_count", &self.transaction_clients.len())
            .field("last_command_error", &self.last_command_error)
            .field(
                "last_command_output_bytes",
                &self.last_command_output.as_ref().map(|value| value.len()),
            )
            .field("last_server_error", &self.last_server_error)
            .field(
                "last_subscription_payload_bytes",
                &self
                    .last_subscription_payload
                    .as_ref()
                    .map(|value| value.len()),
            )
            .field(
                "last_broker_payload_bytes",
                &self.last_broker_payload.as_ref().map(|value| value.len()),
            )
            .field("last_broker_header_count", &self.last_broker_headers.len())
            .field(
                "last_auth_attempts_elapsed",
                &self.last_auth_attempts_elapsed,
            )
            .field(
                "last_cluster_operation_elapsed",
                &self.last_cluster_operation_elapsed,
            )
            .field("clickhouse_table", &self.clickhouse_table)
            .field("clickhouse_tls", &self.clickhouse_tls)
            .field("postgres_table", &self.postgres_table)
            .field("postgres_tls", &self.postgres_tls)
            .field("postgres_lock_held", &self.postgres_lock_release.is_some())
            .field("mysql_table", &self.mysql_table)
            .field("mysql_tls", &self.mysql_tls)
            .field(
                "mysql_insert_command_baseline",
                &self.mysql_insert_command_baseline,
            )
            .field("mongodb_collection", &self.mongodb_collection)
            .field("mongodb_tls", &self.mongodb_tls)
            .field("syslog_udp_observer", &self.syslog_udp_observer.is_some())
            .field("placeholder_count", &self.placeholders.len())
            .field(
                "mqtt_ingestor_domain_count",
                &self.mqtt_ingestors_by_domain.len(),
            )
            .field("avro_http_field_count", &self.avro_http_field_order.len())
            .field(
                "avro_http_optional_field_count",
                &self.avro_http_optional_fields.len(),
            )
            .field("burst_raft_retention_peak", &self.burst_raft_retention_peak)
            .field("temp_root_initialized", &self.temp_root.is_some())
            .field("browser_initialized", &self.browser.is_some())
            .field(
                "web_console_permit_acquired",
                &self.web_console_scenario_permit.is_some(),
            )
            .field(
                "wasm_state_reset_permit_acquired",
                &self.wasm_state_reset_scenario_permit.is_some(),
            )
            .field("dependencies", &self.dependencies)
            .field(
                "stallable_tcp_proxy_count",
                &self.stallable_tcp_proxies.len(),
            )
            .field("http_receivers", &self.http_receivers)
            .field(
                "silent_interconnect_peer_count",
                &self.silent_interconnect_peers.len(),
            )
            .field(
                "last_interconnect_attempt_error",
                &self.last_interconnect_attempt_error,
            )
            .field("server_process", &self.server_process)
            .field("server_process_http_load", &self.server_process_http_load)
            .field("held_resource_upload", &self.held_resource_upload.is_some())
            .field("last_server_signal_at", &self.last_server_signal_at)
            .field("client_probe", &self.client_probe)
            .finish()
    }
}

impl ScenarioWorld {
    fn ingestor_dispatch_ref(&self, ingestor: &str) -> nervix_models::DomainNodeRef {
        let ingestor = expand_placeholders(self, ingestor);
        let domain = nervix_models::DomainName::try_from(self.domain.as_str())
            .assured("the scenario domain is an identifier-shaped name");
        let ingestor = nervix_models::ModelName::try_from(ingestor.as_str())
            .assured("the scenario ingestor is an identifier-shaped name");
        nervix_models::DomainNodeRef::node_in(domain, nervix_models::ModelKind::Ingestor, ingestor)
    }

    /// Which scenario this world belongs to, as every registry that groups work by scenario names
    /// it.
    fn scenario_identity(&self) -> ScenarioIdentity {
        self.active_scenario
            .as_ref()
            .verified("the before hook registers a scenario before its first step runs")
            .identity()
            .clone()
    }

    /// Publishes the phase this scenario is entering and writes the marker that names it.
    ///
    /// The marker is written as the phase begins, so a scenario whose log ends at one of them is a
    /// scenario still inside that phase.
    fn enter_phase(&self, phase: ScenarioPhase, detail: &str) {
        let Some(registration) = &self.active_scenario else {
            return;
        };
        let published = registration.enter(phase);
        let marker = format!(
            "scenario {phase}: {} age={:?} {detail}",
            published.identity,
            published.age()
        );
        append_cucumber_log_line(marker.trim_end());
    }

    fn stop_durable_catch_up_work(&mut self) {
        if let Some(writer) = self.durable_catch_up_writer.take() {
            writer.cancellation.cancel();
            drop(writer.task);
        }
        for node_id in self.consensus_commit_delays.keys() {
            self.fault_injection.set_consensus_storage_commit_delay(
                &crate::common::cluster::node_name(node_id),
                Duration::ZERO,
            );
        }
    }

    fn cluster_mut(&mut self) -> &mut Cluster {
        self.cluster
            .as_mut()
            .expect("cluster must be created before using it")
    }

    fn cluster(&self) -> &Cluster {
        self.cluster
            .as_ref()
            .expect("cluster must be created before using it")
    }

    async fn wait_for_observability_metric_value(
        &self,
        node_id: &str,
        metric_name: &str,
        expected_value: i64,
        wait: Option<Duration>,
        step: &Step,
    ) {
        let node_id = expand_placeholders(self, node_id);
        let metric_name = expand_placeholders(self, metric_name);
        let label_fragments = docstring(step)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| expand_placeholders(self, line))
            .collect::<Vec<_>>();
        self.cluster()
            .wait_for_observability_metric_value(
                &node_id,
                &metric_name,
                &label_fragments,
                expected_value,
                wait,
            )
            .await
            .expect("observability endpoint did not report the expected metric value");
    }

    async fn wait_for_observability_metric_at_least(
        &self,
        node_id: &str,
        metric_name: &str,
        minimum_value: i64,
        wait: Option<Duration>,
        step: &Step,
    ) {
        let node_id = expand_placeholders(self, node_id);
        let metric_name = expand_placeholders(self, metric_name);
        let label_fragments = docstring(step)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| expand_placeholders(self, line))
            .collect::<Vec<_>>();
        self.cluster()
            .wait_for_observability_metric_at_least(
                &node_id,
                &metric_name,
                &label_fragments,
                minimum_value,
                wait,
            )
            .await
            .expect("observability endpoint did not report the expected metric value");
    }

    async fn wait_for_domain_clock_progress_pause_on(
        &self,
        duration: Duration,
        domain: &str,
        node_id: &str,
    ) {
        let domain = expand_placeholders(self, domain);
        let node_id = expand_placeholders(self, node_id);
        tokio::time::timeout(
            duration,
            self.fault_injection
                .wait_for_domain_clock_progress_pause_on(
                    &domain,
                    &crate::common::cluster::node_name(&node_id),
                ),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "domain clock progress for '{domain}' on '{node_id}' did not reach its delivery \
                 pause within {duration:?}: {error}"
            )
        });
    }
}

fn initialize_scenario_identity(world: &mut ScenarioWorld) {
    if !world.test_id.is_empty() {
        return;
    }
    world.domain = format!("d{}", Uuid::now_v7().as_simple());
    world.test_id = format!("t{}", Uuid::now_v7().as_simple());
    world.zeromq_ingest_addr = format!(
        "tcp://127.0.0.1:{}",
        draw_scenario_port(world, "ZeroMQ ingest")
    );
    world.zeromq_emit_addr = format!(
        "tcp://127.0.0.1:{}",
        draw_scenario_port(world, "ZeroMQ emit")
    );
    world.syslog_ingest_addr = format!("127.0.0.1:{}", draw_scenario_port(world, "Syslog ingest"));
    world.syslog_emit_addr = format!("127.0.0.1:{}", draw_scenario_port(world, "Syslog emit"));
}

/// Draws one port for a fixture this scenario binds itself, and records it so the scenario's
/// cleanup gives it back once that fixture is gone.
fn draw_scenario_port(world: &mut ScenarioWorld, purpose: &str) -> u16 {
    let port = match crate::common::port_pool::next_port() {
        Ok(port) => port,
        Err(error) => panic!("failed to allocate the scenario's {purpose} port: {error}"),
    };
    world.scenario_ports.push(port);
    port
}

fn refresh_dependency_configuration(world: &mut ScenarioWorld) {
    world.cluster_config.dependencies = world.dependencies.endpoints().clone();
    world
        .dependencies
        .endpoints()
        .apply_placeholders(&mut world.placeholders);
}

#[given("Kafka is running")]
async fn given_kafka_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_kafka(&world.test_id)
        .await
        .expect("Kafka test container should start");
    refresh_dependency_configuration(world);
}

#[given("Pulsar is running")]
async fn given_pulsar_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_pulsar(&world.test_id)
        .await
        .expect("Pulsar test container should start");
    refresh_dependency_configuration(world);
}

#[given("RabbitMQ is running")]
async fn given_rabbitmq_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_rabbitmq(&world.test_id)
        .await
        .expect("RabbitMQ test container should start");
    refresh_dependency_configuration(world);
}

#[given("Redis is running")]
async fn given_redis_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_redis(&world.test_id)
        .await
        .expect("Redis test container should start");
    refresh_dependency_configuration(world);
}

#[given("MQTT is running")]
async fn given_mqtt_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_mqtt(&world.test_id)
        .await
        .expect("MQTT test container should start");
    refresh_dependency_configuration(world);
}

async fn configure_stallable_tcp_endpoint(
    world: &mut ScenarioWorld,
    name: &str,
    endpoint_key: &str,
    placeholder: &str,
) {
    let endpoint = world
        .dependencies
        .endpoints()
        .get(endpoint_key)
        .unwrap_or_else(|error| panic!("{name} endpoint is unavailable: {error}"));
    let mut endpoint_url = url::Url::parse(endpoint)
        .unwrap_or_else(|error| panic!("failed to parse {name} endpoint '{endpoint}': {error}"));
    let target_host = endpoint_url
        .host_str()
        .unwrap_or_else(|| panic!("{name} endpoint '{endpoint}' has no host"))
        .to_string();
    let target_port = endpoint_url
        .port()
        .unwrap_or_else(|| panic!("{name} endpoint '{endpoint}' has no explicit port"));
    let proxy = StallableTcpProxy::start(target_host, target_port)
        .await
        .unwrap_or_else(|error| panic!("failed to start {name} stallable TCP endpoint: {error}"));
    endpoint_url
        .set_host(Some("127.0.0.1"))
        .unwrap_or_else(|_| panic!("failed to replace host in {name} endpoint '{endpoint}'"));
    endpoint_url
        .set_port(Some(proxy.local_port()))
        .unwrap_or_else(|_| panic!("failed to replace port in {name} endpoint '{endpoint}'"));
    world
        .placeholders
        .insert(placeholder.to_string(), endpoint_url.to_string());
    world.stallable_tcp_proxies.insert(name.to_string(), proxy);
}

#[given("a stallable RabbitMQ endpoint is configured")]
async fn given_stallable_rabbitmq_endpoint(world: &mut ScenarioWorld) {
    configure_stallable_tcp_endpoint(world, "rabbitmq", RABBITMQ_ADDR, "rabbitmq_stallable_addr")
        .await;
}

#[given("a stallable MQTT endpoint is configured")]
async fn given_stallable_mqtt_endpoint(world: &mut ScenarioWorld) {
    configure_stallable_tcp_endpoint(world, "mqtt", MQTT_ADDR, "mqtt_stallable_addr").await;
}

#[given("a stallable Pulsar endpoint is configured")]
async fn given_stallable_pulsar_endpoint(world: &mut ScenarioWorld) {
    configure_stallable_tcp_endpoint(world, "pulsar", PULSAR_ADDR, "pulsar_stallable_addr").await;
}

#[when(expr = "the stallable endpoint {string} is paused")]
fn when_stallable_endpoint_is_paused(world: &mut ScenarioWorld, name: String) {
    world
        .stallable_tcp_proxies
        .get(&name)
        .unwrap_or_else(|| panic!("stallable endpoint '{name}' is not configured"))
        .set_paused(true);
}

#[when(expr = "the stallable endpoint {string} is resumed")]
fn when_stallable_endpoint_is_resumed(world: &mut ScenarioWorld, name: String) {
    world
        .stallable_tcp_proxies
        .get(&name)
        .unwrap_or_else(|| panic!("stallable endpoint '{name}' is not configured"))
        .set_paused(false);
}

#[given("ClickHouse is running")]
async fn given_clickhouse_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_clickhouse(&world.test_id)
        .await
        .expect("ClickHouse test container should start");
    refresh_dependency_configuration(world);
}

#[given("Postgres is running")]
async fn given_postgres_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_postgres(&world.test_id)
        .await
        .expect("Postgres test container should start");
    refresh_dependency_configuration(world);
}

#[given("MySQL is running")]
async fn given_mysql_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_mysql(&world.test_id)
        .await
        .expect("MySQL test container should start");
    refresh_dependency_configuration(world);
}

#[given("MongoDB is running")]
async fn given_mongodb_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_mongodb(&world.test_id)
        .await
        .expect("MongoDB test container should start");
    refresh_dependency_configuration(world);
}

#[given("NATS is running")]
async fn given_nats_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_nats(&world.test_id)
        .await
        .expect("NATS test container should start");
    refresh_dependency_configuration(world);
}

#[given("NATS TLS is running")]
async fn given_nats_tls_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_nats_tls(&world.test_id)
        .await
        .expect("NATS TLS test container should start");
    refresh_dependency_configuration(world);
}

#[given("Prometheus is running")]
async fn given_prometheus_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_prometheus(&world.test_id)
        .await
        .expect("Prometheus test container should start");
    refresh_dependency_configuration(world);
}

#[given("Prometheus TLS is running")]
async fn given_prometheus_tls_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_prometheus_tls(&world.test_id)
        .await
        .expect("Prometheus TLS test container should start");
    refresh_dependency_configuration(world);
}

#[given("SQS is running")]
async fn given_sqs_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_sqs(&world.test_id)
        .await
        .expect("SQS test containers should start");
    refresh_dependency_configuration(world);
}

#[given("the HTTP mock server is running")]
async fn given_http_mock_server_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_mock_server(&world.test_id)
        .await
        .expect("HTTP mock server test container should start");
    refresh_dependency_configuration(world);
}

/// How long a step waits for an HTTP receiver to observe what a node sends it. Generous, because a
/// wait for something to happen ends as soon as it does.
const HTTP_RECEIVER_WAIT: Duration = Duration::from_secs(60);

async fn start_http_receiver(
    world: &mut ScenarioWorld,
    name: String,
    transport: ReceiverTransport,
) {
    initialize_scenario_identity(world);
    let name = expand_placeholders(world, &name);
    assert!(
        !world.http_receivers.contains_key(&name),
        "HTTP receiver '{name}' is already running"
    );
    let port = draw_scenario_port(world, "HTTP receiver");
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let receiver = match HttpReceiver::start(address, transport).await {
        Ok(receiver) => receiver,
        Err(error) => panic!("HTTP receiver '{name}' failed to start: {error:?}"),
    };
    world
        .placeholders
        .insert(format!("http_receiver.{name}"), receiver.origin());
    world.placeholders.insert(
        format!("http_receiver_port.{name}"),
        receiver.port().to_string(),
    );
    world.http_receivers.insert(name, receiver);
}

fn http_receiver<'world>(world: &'world ScenarioWorld, name: &str) -> &'world HttpReceiver {
    let name = expand_placeholders(world, name);
    match world.http_receivers.get(&name) {
        Some(receiver) => receiver,
        None => panic!("HTTP receiver '{name}' is not running"),
    }
}

fn certificate_hosts(world: &ScenarioWorld, hosts: &str) -> Vec<String> {
    let hosts = expand_placeholders(world, hosts)
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !hosts.is_empty(),
        "an HTTPS receiver needs at least one certificate host"
    );
    hosts
}

#[given(expr = "HTTP receiver {string} is running")]
async fn given_http_receiver_is_running(world: &mut ScenarioWorld, name: String) {
    start_http_receiver(world, name, ReceiverTransport::Plain).await;
}

#[given(expr = "HTTPS receiver {string} is running with a certificate for {string}")]
async fn given_https_receiver_is_running(world: &mut ScenarioWorld, name: String, hosts: String) {
    let options = ReceiverTlsOptions {
        certificate_hosts: certificate_hosts(world, &hosts),
        client_certificate: ClientCertificatePolicy::NotRequested,
    };
    start_http_receiver(world, name, ReceiverTransport::Tls(options)).await;
}

#[given(
    expr = "HTTPS receiver {string} is running with a certificate for {string} and requires a \
            client certificate"
)]
async fn given_https_receiver_requiring_client_certificates_is_running(
    world: &mut ScenarioWorld,
    name: String,
    hosts: String,
) {
    let options = ReceiverTlsOptions {
        certificate_hosts: certificate_hosts(world, &hosts),
        client_certificate: ClientCertificatePolicy::Required,
    };
    start_http_receiver(world, name, ReceiverTransport::Tls(options)).await;
}

/// Places the receiver's CA certificate and the client identity it issued where the node can mount
/// them as a resource directory: `ca.pem`, `client.pem`, and `client-key.pem`.
#[given(
    expr = "node {string} has the TLS files of HTTP receiver {string} in resource directory \
            {string}"
)]
async fn given_node_has_http_receiver_tls_files(
    world: &mut ScenarioWorld,
    node_id: String,
    name: String,
    placeholder: String,
) {
    let files = match http_receiver(world, &name).tls_files() {
        Some(files) => files.to_path_buf(),
        None => panic!("HTTP receiver '{name}' does not serve TLS"),
    };
    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("old fixture directory should be removed");
    }
    std::fs::create_dir_all(&resource_dir).expect("fixture directory should be created");
    for file in HttpReceiver::tls_file_names() {
        std::fs::copy(files.join(file), resource_dir.join(file)).unwrap_or_else(|error| {
            panic!("failed to copy HTTP receiver TLS file '{file}': {error}")
        });
    }
    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

/// Each line is one response, taken by the next request in order. The forms are those
/// `ReceiverResponse` parses: `respond <status>` with `;`-separated clauses, `lose response`,
/// `hold response`, and `raw <bytes>`.
#[given(expr = "HTTP receiver {string} answers with")]
async fn given_http_receiver_answers_with(
    world: &mut ScenarioWorld,
    name: String,
    #[step] step: &Step,
) {
    let script = expand_placeholders(world, docstring(step));
    let mut responses = Vec::new();
    for line in script
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        match line.parse::<ReceiverResponse>() {
            Ok(response) => responses.push(response),
            Err(error) => panic!("invalid HTTP receiver script line: {error}"),
        }
    }
    http_receiver(world, &name).script(responses);
}

#[given(expr = "HTTP receiver {string} answers unscripted requests with {string}")]
async fn given_http_receiver_answers_unscripted_requests_with(
    world: &mut ScenarioWorld,
    name: String,
    response: String,
) {
    let response = match expand_placeholders(world, &response).parse::<ReceiverResponse>() {
        Ok(response) => response,
        Err(error) => panic!("invalid HTTP receiver response: {error}"),
    };
    http_receiver(world, &name).answer_unscripted_requests_with(response);
}

#[then(expr = "HTTP receiver {string} eventually receives at least {int} request(s)")]
async fn then_http_receiver_eventually_receives_requests(
    world: &mut ScenarioWorld,
    name: String,
    expected: usize,
) {
    let receiver = http_receiver(world, &name);
    if let Err(error) = receiver
        .wait_for_requests(expected, HTTP_RECEIVER_WAIT)
        .await
    {
        panic!("HTTP receiver '{name}': {error}");
    }
}

/// Compares one captured request, counted from 1, with the docstring: `<METHOD> <target>`, then
/// the headers the request must carry with exactly these values, then an empty line, then the
/// exact body. Header names compare without case; headers the docstring does not name are not
/// checked. A docstring without a body requires a request with zero content bytes.
#[then(expr = "HTTP receiver {string} request {int} is")]
async fn then_http_receiver_request_is(
    world: &mut ScenarioWorld,
    name: String,
    position: usize,
    #[step] step: &Step,
) {
    // Cucumber keeps the newlines that open and close a docstring; neither is part of a request.
    let expected = expand_placeholders(world, docstring(step))
        .trim_matches('\n')
        .to_string();
    let receiver = http_receiver(world, &name);
    let captured = receiver.captured();
    let Some(index) = position.checked_sub(1) else {
        panic!("HTTP receiver requests are counted from 1");
    };
    let Some(request) = captured.get(index) else {
        panic!(
            "HTTP receiver '{name}' captured {} request(s), not request {position}",
            captured.len()
        );
    };
    // Without an empty line the docstring names no body, which is a request with zero content
    // bytes.
    let (head, body) = match expected.split_once("\n\n") {
        Some((head, body)) => (head, body),
        None => (expected.as_str(), ""),
    };
    let mut head_lines = head.lines();
    let request_line = head_lines.next().unwrap_or_default();
    assert_eq!(
        request_line,
        format!("{} {}", request.method, request.target),
        "HTTP receiver '{name}' request {position} has another request line:\n{request}"
    );
    for header in head_lines {
        let Some((header_name, value)) = header.split_once(':') else {
            panic!("expected header line '{header}' has no ':'");
        };
        let values = request.header_values(header_name.trim());
        assert_eq!(
            values,
            vec![value.trim().as_bytes()],
            "HTTP receiver '{name}' request {position} does not carry exactly one '{}' header \
             with the expected value:\n{request}",
            header_name.trim()
        );
    }
    assert_eq!(
        request.body,
        body.as_bytes(),
        "HTTP receiver '{name}' request {position} has another body:\n{request}"
    );
}

#[then(expr = "HTTP receiver {string} eventually records a failed TLS handshake")]
async fn then_http_receiver_records_failed_tls_handshake(world: &mut ScenarioWorld, name: String) {
    let receiver = http_receiver(world, &name);
    let waited = receiver
        .wait_for_fault(
            "failed TLS handshake",
            ReceiverFault::is_tls_handshake,
            HTTP_RECEIVER_WAIT,
        )
        .await;
    if let Err(error) = waited {
        panic!("HTTP receiver '{name}': {error}");
    }
}

#[given(expr = "clock source recorder {string} is reset")]
async fn given_clock_source_recorder_is_reset(world: &mut ScenarioWorld, name: String) {
    let name = expand_placeholders(world, &name);
    world
        .dependencies
        .reset_clock_source(&name)
        .await
        .unwrap_or_else(|error| panic!("failed to reset clock source recorder '{name}': {error}"));
}

#[then(expr = "within {string} clock source recorder {string} records {int} requests")]
async fn then_clock_source_recorder_records_requests(
    world: &mut ScenarioWorld,
    duration: String,
    name: String,
    expected_count: u64,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let name = expand_placeholders(world, &name);
    let deadline = Instant::now() + duration;
    loop {
        tokio::task::consume_budget().await;
        let observations = world
            .dependencies
            .clock_source_observations(&name)
            .await
            .unwrap_or_else(|error| {
                panic!("failed to read clock source recorder '{name}': {error}")
            });
        let Some(count) = observations.get("count") else {
            panic!("clock source recorder '{name}' returned no count: {observations}");
        };
        let Some(observed_count) = count.as_u64() else {
            panic!("clock source recorder '{name}' returned a nonnumeric count: {observations}");
        };
        if observed_count == expected_count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "clock source recorder '{name}' expected {expected_count} requests, observed \
             {observed_count}: {observations}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[then(expr = "within {string} clock source recorder {string} records at least {int} requests")]
async fn then_clock_source_recorder_records_at_least_requests(
    world: &mut ScenarioWorld,
    duration: String,
    name: String,
    expected_count: u64,
) {
    let duration = humantime::parse_duration(&duration)
        .assured("the Cucumber expression supplies a valid step duration");
    let name = expand_placeholders(world, &name);
    let deadline = Instant::now()
        .checked_add(duration)
        .assured("the Cucumber fixture duration fits the monotonic clock range");
    loop {
        tokio::task::consume_budget().await;
        let observations = world
            .dependencies
            .clock_source_observations(&name)
            .await
            .unwrap_or_else(|error| {
                panic!("failed to read clock source recorder '{name}': {error}")
            });
        let Some(count) = observations.get("count") else {
            panic!("clock source recorder '{name}' returned no count: {observations}");
        };
        let Some(observed_count) = count.as_u64() else {
            panic!("clock source recorder '{name}' returned a nonnumeric count: {observations}");
        };
        if observed_count >= expected_count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "clock source recorder '{name}' expected at least {expected_count} requests, observed \
             {observed_count}: {observations}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[then(
    expr = "the first {int} requests recorded by clock source recorder {string} are separated by \
            at least {string}"
)]
async fn then_clock_source_requests_have_minimum_physical_gap(
    world: &mut ScenarioWorld,
    count: usize,
    name: String,
    minimum_gap: String,
) {
    let minimum_gap = humantime::parse_duration(&minimum_gap)
        .assured("the Cucumber expression supplies a valid minimum gap duration");
    let name = expand_placeholders(world, &name);
    let observations = world
        .dependencies
        .clock_source_observations(&name)
        .await
        .unwrap_or_else(|error| panic!("failed to read clock source recorder '{name}': {error}"));
    let Some(requests) = observations
        .get("requests")
        .and_then(serde_json::Value::as_array)
    else {
        panic!("clock source recorder '{name}' returned no request list: {observations}");
    };
    assert!(
        requests.len() >= count,
        "clock source recorder '{name}' expected at least {count} requests, observed {}: \
         {observations}",
        requests.len()
    );
    let mut received_at = Vec::with_capacity(count);
    for (index, request) in requests.iter().take(count).enumerate() {
        let Some(received_at_monotonic_nanos) = request
            .get("received_at_monotonic_nanos")
            .and_then(serde_json::Value::as_u64)
        else {
            panic!(
                "clock source recorder '{name}' request {index} has no monotonic timestamp: \
                 {request}"
            );
        };
        received_at.push(received_at_monotonic_nanos);
    }
    for pair in received_at.windows(2) {
        let gap = pair[1]
            .checked_sub(pair[0])
            .verified("the recorder returns requests in monotonic receipt order");
        assert!(
            u128::from(gap) >= minimum_gap.as_nanos(),
            "clock source recorder '{name}' expected its first {count} requests to be at least \
             {minimum_gap:?} apart, observed a {gap}ns gap in {received_at:?}"
        );
    }
}

fn decimal_seconds_to_unix_nanos(value: &str) -> i128 {
    let value = value.trim();
    let (negative, magnitude) = if let Some(magnitude) = value.strip_prefix('-') {
        (true, magnitude)
    } else {
        (false, value)
    };
    let (seconds, fraction) = if let Some((seconds, fraction)) = magnitude.split_once('.') {
        (seconds, fraction)
    } else {
        (magnitude, "")
    };
    if seconds.is_empty()
        || fraction.len() > 9
        || !fraction.bytes().all(|digit| digit.is_ascii_digit())
    {
        panic!("'{value}' is not a decimal Unix timestamp");
    }
    let seconds = match seconds.parse::<i128>() {
        Ok(seconds) => seconds,
        Err(error) => panic!("invalid seconds in Unix timestamp '{value}': {error}"),
    };
    let mut fraction_nanos = fraction.to_string();
    while fraction_nanos.len() < 9 {
        fraction_nanos.push('0');
    }
    let fraction_nanos = match fraction_nanos.parse::<i128>() {
        Ok(fraction_nanos) => fraction_nanos,
        Err(error) => panic!("invalid fraction in Unix timestamp '{value}': {error}"),
    };
    let Some(seconds) = seconds.checked_mul(1_000_000_000) else {
        panic!("Unix timestamp '{value}' is outside the test representation");
    };
    let Some(magnitude) = seconds.checked_add(fraction_nanos) else {
        panic!("Unix timestamp '{value}' is outside the test representation");
    };
    if negative {
        let Some(magnitude) = magnitude.checked_neg() else {
            panic!("Unix timestamp '{value}' is outside the test representation");
        };
        magnitude
    } else {
        magnitude
    }
}

#[then(
    expr = "within {string} clock source recorder {string} and relay subscription observe {int} \
            fresh executions on {string} due cadence separated by at least {string}"
)]
async fn then_clock_source_and_subscription_observe_fresh_cadence(
    world: &mut ScenarioWorld,
    duration: String,
    name: String,
    expected_count: usize,
    cadence: String,
    minimum_gap: String,
) {
    struct RecordedDue {
        rendered: String,
        unix_nanos: i128,
    }

    let duration = match humantime::parse_duration(&duration) {
        Ok(duration) => duration,
        Err(error) => panic!("step duration must be valid: {error}"),
    };
    let cadence = match humantime::parse_duration(&cadence) {
        Ok(cadence) => cadence,
        Err(error) => panic!("cadence duration must be valid: {error}"),
    };
    let minimum_gap = match humantime::parse_duration(&minimum_gap) {
        Ok(minimum_gap) => minimum_gap,
        Err(error) => panic!("minimum gap duration must be valid: {error}"),
    };
    let cadence_nanos = match i128::try_from(cadence.as_nanos()) {
        Ok(cadence_nanos) => cadence_nanos,
        Err(error) => panic!("cadence does not fit the test representation: {error}"),
    };
    let minimum_gap_nanos = match i128::try_from(minimum_gap.as_nanos()) {
        Ok(minimum_gap_nanos) => minimum_gap_nanos,
        Err(error) => panic!("minimum gap does not fit the test representation: {error}"),
    };
    let name = expand_placeholders(world, &name);
    let deadline = Instant::now()
        .checked_add(duration)
        .assured("scenario durations fit Tokio's monotonic instant range");

    let observations = loop {
        tokio::task::consume_budget().await;
        let observations = match world.dependencies.clock_source_observations(&name).await {
            Ok(observations) => observations,
            Err(error) => panic!("failed to read clock source recorder '{name}': {error}"),
        };
        let Some(requests) = observations
            .get("requests")
            .and_then(serde_json::Value::as_array)
        else {
            panic!("clock source recorder '{name}' returned no request list: {observations}");
        };
        // Polling continues while this observer request is in flight, so a loaded suite can pass
        // the target count before the response arrives. The first expected occurrences remain the
        // exact sample validated below.
        if requests.len() >= expected_count {
            break observations;
        }
        assert!(
            Instant::now() < deadline,
            "clock source recorder '{name}' expected {expected_count} requests, observed {}: \
             {observations}",
            requests.len(),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    let requests = observations["requests"]
        .as_array()
        .assured("the recorder response was validated before leaving the wait loop");
    let mut recorded_due = Vec::with_capacity(expected_count);
    for (index, request) in requests.iter().take(expected_count).enumerate() {
        let Some(query) = request.get("query") else {
            panic!("recorded request {index} has no Prometheus query: {request}");
        };
        let Some(time) = query.get("time") else {
            panic!("recorded request {index} has no Prometheus time query: {request}");
        };
        let Some(rendered) = time.as_str() else {
            panic!("recorded request {index} has a non-string Prometheus time query: {request}");
        };
        let unix_nanos = decimal_seconds_to_unix_nanos(rendered);
        recorded_due.push(RecordedDue {
            rendered: rendered.to_string(),
            unix_nanos,
        });
    }
    for pair in recorded_due.windows(2) {
        let gap = pair[1]
            .unix_nanos
            .checked_sub(pair[0].unix_nanos)
            .assured("ordered signed timestamps have a representable difference in i128");
        assert!(
            gap >= minimum_gap_nanos,
            "expected due timestamps at least {minimum_gap:?} apart, got {} then {}",
            pair[0].rendered,
            pair[1].rendered,
        );
        assert_eq!(
            gap % cadence_nanos,
            0,
            "due timestamps must remain on the {cadence:?} anchored cadence: {} then {}",
            pair[0].rendered,
            pair[1].rendered,
        );
    }

    let session = world
        .active_session
        .as_mut()
        .assured("an active session with subscription must exist");
    let mut observed_payloads = Vec::with_capacity(expected_count);
    for (index, due) in recorded_due.iter().enumerate() {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out after receiving {index} of {expected_count} subscription payloads: \
             {observed_payloads:?}"
        );
        let remaining = deadline
            .checked_duration_since(now)
            .verified("the deadline comparison above established remaining scenario time");
        let event = match session.try_next_subscription(remaining).await {
            Ok(Some(event)) => event,
            Ok(None) => panic!(
                "timed out after receiving {index} of {expected_count} subscription payloads: \
                 {observed_payloads:?}"
            ),
            Err(error) => panic!("failed while waiting for subscription payloads: {error}"),
        };
        let payload = event.payload;
        let parsed = match serde_json::from_str::<serde_json::Value>(&payload) {
            Ok(parsed) => parsed,
            Err(error) => panic!("subscription payload is not valid JSON: {error}"),
        };
        let Some(output_due) = parsed.get("due").and_then(serde_json::Value::as_str) else {
            panic!("subscription payload has no due timestamp: {payload}");
        };
        assert_eq!(
            output_due, due.rendered,
            "subscription output did not preserve request {index}'s due timestamp"
        );
        let Some(executed_at) = parsed
            .get("executed_at")
            .and_then(serde_json::Value::as_str)
        else {
            panic!("subscription payload has no execution timestamp: {payload}");
        };
        let executed_at = match chrono::DateTime::parse_from_rfc3339(executed_at) {
            Ok(executed_at) => executed_at,
            Err(error) => panic!("invalid execution timestamp '{executed_at}': {error}"),
        };
        let executed_at = executed_at
            .timestamp_nanos_opt()
            .assured("the scenario's historical execution timestamp fits signed nanoseconds");
        assert!(
            i128::from(executed_at) > due.unix_nanos,
            "execution timestamp must be sampled after slow request {index} completed: {payload}"
        );
        world.last_subscription_payload = Some(payload.clone());
        observed_payloads.push(payload);
    }
}

#[given("Iceberg dependencies are running")]
async fn given_iceberg_dependencies_are_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_iceberg(&world.test_id)
        .await
        .expect("Iceberg test containers should start");
    refresh_dependency_configuration(world);
}

#[given("GCS is running")]
async fn given_gcs_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_gcs(&world.test_id)
        .await
        .expect("GCS test container should start");
    refresh_dependency_configuration(world);
}

#[given("Azure Blob is running")]
async fn given_azure_blob_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_azurite(&world.test_id)
        .await
        .expect("Azurite test container should start");
    refresh_dependency_configuration(world);
}

#[given("Quickwit is running")]
async fn given_quickwit_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_quickwit(&world.test_id)
        .await
        .expect("Quickwit test container should start");
    refresh_dependency_configuration(world);
}

#[given("OpenTelemetry Collector is running")]
async fn given_otel_collector_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_otel_collector(&world.test_id)
        .await
        .expect("OpenTelemetry Collector test container should start");
    refresh_dependency_configuration(world);
}

#[given("Jaeger is running")]
async fn given_jaeger_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_jaeger(&world.test_id)
        .await
        .expect("Jaeger test containers should start");
    refresh_dependency_configuration(world);
}

#[given("Sentry is running")]
async fn given_sentry_is_running(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_sentry(&world.test_id)
        .await
        .expect("Sentry test container should start");
    refresh_dependency_configuration(world);
}

#[then(expr = "dependency endpoint {string} responds with 200")]
async fn then_dependency_endpoint_responds_with_200(
    world: &mut ScenarioWorld,
    endpoint_key: String,
) {
    let endpoint = world
        .dependencies
        .endpoints()
        .get(&endpoint_key)
        .unwrap_or_else(|error| panic!("{error}"))
        .to_string();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::task::consume_budget().await;
        let attempt_error = match reqwest::get(&endpoint).await {
            Ok(response) if response.status() == reqwest::StatusCode::OK => return,
            Ok(response) => format!("HTTP {}", response.status()),
            Err(error) => error.to_string(),
        };
        assert!(
            Instant::now() < deadline,
            "dependency endpoint '{endpoint_key}' at '{endpoint}' did not respond with 200: \
             {attempt_error}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then("Kafka exposes host and Docker network benchmark endpoints")]
fn then_kafka_exposes_host_and_docker_network_benchmark_endpoints(world: &mut ScenarioWorld) {
    let endpoints = world.dependencies.endpoints();
    let host = endpoints
        .get(KAFKA_ADDR)
        .expect("Kafka should expose its host endpoint");
    let docker = endpoints
        .get(KAFKA_DOCKER_ADDR)
        .expect("Kafka should expose its Docker endpoint");
    let network = endpoints
        .get(KAFKA_DOCKER_NETWORK)
        .expect("Kafka should expose its Docker network");
    assert!(
        host.starts_with("127.0.0.1:"),
        "unexpected host endpoint: {host}"
    );
    assert!(
        docker.ends_with(":9093") && !docker.starts_with("localhost:"),
        "unexpected Docker endpoint: {docker}"
    );
    assert!(
        !network.is_empty(),
        "Kafka Docker network must not be empty"
    );
}

#[then("an ephemeral dependency is cleaned up when its test process is killed")]
async fn then_ephemeral_dependency_is_cleaned_up_when_test_process_is_killed(
    _world: &mut ScenarioWorld,
) {
    use tokio::io::AsyncBufReadExt as _;

    let scope = format!("lifecycle-{}", Uuid::now_v7().as_simple());
    let mut child = tokio::process::Command::new(
        std::env::current_exe().expect("scenario executable path should be available"),
    )
    .env(DEPENDENCY_LIFECYCLE_HELPER_ENV, &scope)
    .env("NERVIX_TESTCONTAINERS_MODE", "ephemeral")
    .env("TESTCONTAINERS_COMMAND", "keep")
    .kill_on_drop(true)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .expect("dependency lifecycle helper should start");
    let stdout = child
        .stdout
        .take()
        .expect("dependency lifecycle helper stdout should be piped");
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    let container_id = tokio::time::timeout(Duration::from_secs(180), async {
        while let Some(line) = lines
            .next_line()
            .await
            .expect("dependency lifecycle helper stdout should be readable")
        {
            if let Some(container_id) = line.strip_prefix(DEPENDENCY_LIFECYCLE_STARTED) {
                return container_id.to_string();
            }
        }
        panic!("dependency lifecycle helper exited before reporting its container")
    })
    .await
    .expect("dependency lifecycle helper should start its container before the timeout");

    child
        .kill()
        .await
        .expect("dependency lifecycle helper should accept SIGKILL");
    let output = child
        .wait_with_output()
        .await
        .expect("dependency lifecycle helper should exit after SIGKILL");
    assert!(
        !output.status.success(),
        "dependency lifecycle helper must be killed to exercise Ryuk cleanup"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let remains = TestDependencies::container_exists(&container_id)
            .await
            .expect("Docker should report whether the lifecycle container remains");
        if !remains {
            return;
        }
        if Instant::now() >= deadline {
            TestDependencies::force_remove_container(&container_id)
                .await
                .expect("leaked lifecycle reproducer container should be removed");
            panic!(
                "ephemeral dependency container {container_id} remained after its process was \
                 killed\nhelper stderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Clone, Copy, Debug)]
enum IngestorLogicTransportFixture {
    HttpEndpoint,
    Kafka,
    Mqtt,
    Nats,
    WebsocketEndpoint,
    ZeroMq,
}

#[when("the nervix-server help is requested")]
fn when_nervix_server_help_is_requested(world: &mut ScenarioWorld) {
    let binary = env!("CARGO_BIN_EXE_nervix-server");
    let output = Command::new(binary)
        .arg("--help")
        .output()
        .expect("nervix-server help command must run");
    assert!(
        output.status.success(),
        "nervix-server help command failed with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    world.last_command_output =
        Some(String::from_utf8(output.stdout).expect("nervix-server help output must be UTF-8"));
}

#[given("a nervix-server process is started")]
async fn given_nervix_server_process_is_started(world: &mut ScenarioWorld) {
    start_ready_server_process(world, &[]).await;
}

#[given("a release nervix-server process is started for the client-wire baseline")]
async fn given_release_server_process_is_started_for_client_wire_baseline(
    world: &mut ScenarioWorld,
) {
    let executable = std::env::var_os("NERVIX_CLIENT_WIRE_BASELINE_SERVER")
        .verified("the client-wire baseline recipe supplies its release server executable");
    start_ready_server_process_with_launch(
        world,
        ServerProcessLaunch::Executable(PathBuf::from(executable)),
        &[],
    )
    .await;
}

#[given(expr = "a nervix-server process is started with drain timeout {string}")]
async fn given_nervix_server_process_is_started_with_drain_timeout(
    world: &mut ScenarioWorld,
    drain_timeout: String,
) {
    let drain_timeout =
        humantime::parse_duration(&drain_timeout).expect("drain timeout must be a valid duration");
    start_ready_server_process(world, &[ServerProcessOption::DrainTimeout(drain_timeout)]).await;
}

#[given(expr = "a nervix-server process is started with state snapshot interval {string}")]
async fn given_nervix_server_process_is_started_with_state_snapshot_interval(
    world: &mut ScenarioWorld,
    interval: String,
) {
    let interval = humantime::parse_duration(&interval)
        .expect("state snapshot interval must be a valid duration");
    start_ready_server_process(
        world,
        &[ServerProcessOption::StateSnapshotInterval(interval)],
    )
    .await;
}

#[given(
    expr = "a nervix-server process is started with transaction idle timeout {string} and \
            tombstone retention {string}"
)]
async fn given_nervix_server_process_is_started_with_transaction_retention(
    world: &mut ScenarioWorld,
    idle_timeout: String,
    tombstone_retention: String,
) {
    let idle_timeout = humantime::parse_duration(&idle_timeout)
        .expect("transaction idle timeout must be a valid duration");
    let tombstone_retention = humantime::parse_duration(&tombstone_retention)
        .expect("transaction tombstone retention must be a valid duration");
    let options = [
        ServerProcessOption::TransactionIdleTimeout(idle_timeout),
        ServerProcessOption::TransactionTombstoneRetention(tombstone_retention),
    ];
    start_ready_server_process(world, &options).await;
}

#[given(
    expr = "a nervix-server process is started with drain timeout {string} and shutdown timeout \
            {string}"
)]
async fn given_nervix_server_process_is_started_with_shutdown_timeouts(
    world: &mut ScenarioWorld,
    drain_timeout: String,
    shutdown_timeout: String,
) {
    let drain_timeout =
        humantime::parse_duration(&drain_timeout).expect("drain timeout must be a valid duration");
    let shutdown_timeout = humantime::parse_duration(&shutdown_timeout)
        .expect("shutdown timeout must be a valid duration");
    let options = [
        ServerProcessOption::DrainTimeout(drain_timeout),
        ServerProcessOption::ShutdownTimeout(shutdown_timeout),
    ];
    start_ready_server_process(world, &options).await;
}

/// Starts the scenario's server process with `options` and waits until it accepts commands.
async fn start_ready_server_process(world: &mut ScenarioWorld, options: &[ServerProcessOption]) {
    start_ready_server_process_with_launch(world, ServerProcessLaunch::Direct, options).await;
}

async fn start_ready_server_process_with_launch(
    world: &mut ScenarioWorld,
    launch: ServerProcessLaunch,
    options: &[ServerProcessOption],
) {
    assert!(
        world.server_process.is_none(),
        "a scenario starts at most one nervix-server process"
    );
    initialize_scenario_identity(world);
    let mut process = ServerProcess::start(launch, options)
        .unwrap_or_else(|error| panic!("failed to launch nervix-server: {error}"));
    process
        .wait_until_ready()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    world.server_process = Some(process);
}

#[given(expr = "a nervix-server process is started with an open file limit of {int}")]
async fn given_nervix_server_process_is_started_with_open_file_limit(
    world: &mut ScenarioWorld,
    limit: u32,
) {
    assert!(
        world.server_process.is_none(),
        "a scenario starts at most one nervix-server process"
    );
    let process = ServerProcess::start(ServerProcessLaunch::OpenFileLimit(limit), &[])
        .unwrap_or_else(|error| panic!("failed to launch nervix-server: {error}"));
    world.server_process = Some(process);
}

#[given("the server process is configured with these NSPL commands")]
async fn given_server_process_is_configured_with_nspl_commands(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let commands = expand_placeholders(world, docstring(step));
    let process = world
        .server_process
        .as_ref()
        .verified("the preceding step started a nervix-server process");
    for statement in nspl_statements(&commands) {
        if let Err(error) = process.run_commands(&world.domain, &statement).await {
            panic!(
                "nervix-server rejected {statement:?}: {error}\n{}",
                process.log_tail()
            );
        }
    }
}

#[when(expr = "an open transaction is held on the server process as placeholder {string}")]
async fn when_open_transaction_is_held_on_server_process(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let domain = world.domain.clone();
    let mut session = world
        .server_process
        .as_ref()
        .verified("the preceding step started a nervix-server process")
        .open_session(&domain)
        .await
        .unwrap_or_else(|error| panic!("failed to open the server process session: {error}"));
    let result = session
        .run_command_result("BEGIN;")
        .await
        .unwrap_or_else(|error| panic!("failed to open the retained transaction: {error}"));
    assert!(
        result.succeeded(),
        "the retained transaction must open: {}",
        result.message
    );
    let transaction = result
        .transaction
        .verified("a successful BEGIN returns its transaction identity");
    world
        .placeholders
        .insert(placeholder, transaction.transaction_id().to_string());
    world.last_command_output = Some(result.message);
    world.active_session = Some(session);
}

#[when("these NSPL commands are executed on the server process")]
async fn when_nspl_commands_are_executed_on_server_process(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let commands = expand_placeholders(world, docstring(step));
    let process = world
        .server_process
        .as_ref()
        .verified("the preceding step started a nervix-server process");
    world.last_command_output = None;
    for statement in nspl_statements(&commands) {
        let output = process.run_commands(&world.domain, &statement).await;
        world.last_command_output = Some(output.unwrap_or_else(|error| {
            panic!(
                "nervix-server rejected {statement:?}: {error}\n{}",
                process.log_tail()
            )
        }));
    }
}

#[then(expr = "server process transaction {string} eventually has state {string}")]
async fn then_server_process_transaction_eventually_has_state(
    world: &mut ScenarioWorld,
    transaction_id: String,
    expected_state: String,
) {
    let transaction_id = expand_placeholders(world, &transaction_id);
    let expected_state = expand_placeholders(world, &expected_state).to_ascii_uppercase();
    let expected_id = format!("id={transaction_id}");
    let expected_state_fragment = format!("state={expected_state}");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_output = String::new();
    loop {
        tokio::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "server process transaction '{transaction_id}' did not reach state \
             '{expected_state}'; last output: {last_output}"
        );
        let output = world
            .server_process
            .as_ref()
            .verified("the preceding step started a nervix-server process")
            .run_commands(&world.domain, "SHOW TRANSACTIONS;")
            .await;
        match output {
            Ok(output)
                if output.lines().any(|line| {
                    line.contains(&expected_id) && line.contains(&expected_state_fragment)
                }) =>
            {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => last_output = output,
            Err(error) => last_output = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[when(expr = "http payload is posted to the server process with host {string} path {string}")]
async fn when_http_payload_is_posted_to_server_process(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let process = world
        .server_process
        .as_ref()
        .expect("a nervix-server process must be started first");
    if let Err(error) = process.publish_http(&host, &path, &payload).await {
        panic!(
            "nervix-server did not admit the http payload: {error}\n{}",
            process.log_tail()
        );
    }
}

#[when(
    expr = "the server process eventually accepts http payload with host {string} path {string}"
)]
async fn when_server_process_eventually_accepts_http_payload(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    world
        .server_process
        .as_mut()
        .expect("a nervix-server process must be started first")
        .publish_http_eventually(&host, &path, &payload)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
}

#[when(
    expr = "HTTP load against the server process begins at id {int} with host {string} path \
            {string}"
)]
fn when_http_load_against_server_process_begins(
    world: &mut ScenarioWorld,
    first_id: u64,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    assert!(
        world.server_process_http_load.is_none(),
        "a scenario starts at most one server process HTTP load"
    );
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload_templates = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let load = world
        .server_process
        .as_ref()
        .expect("a nervix-server process must be started first")
        .start_http_load(&host, &path, first_id, payload_templates)
        .unwrap_or_else(|error| panic!("failed to start server process HTTP load: {error}"));
    world.server_process_http_load = Some(load);
}

#[then(expr = "the server process load admits at least {int} payloads")]
async fn then_server_process_load_admits_at_least(world: &mut ScenarioWorld, expected: u64) {
    world
        .server_process_http_load
        .as_mut()
        .expect("server process HTTP load must be started first")
        .wait_for_admissions(expected)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
}

#[given(
    regex = r#"^an authenticated upload of resource "([^"]+)" (waiting for its first message|waiting for its next chunk|sending chunks slowly) is held open on the server process$"#
)]
async fn given_authenticated_resource_upload_is_held_open(
    world: &mut ScenarioWorld,
    resource: String,
    progress: String,
) {
    let progress = match progress.as_str() {
        "waiting for its first message" => HeldUploadProgress::AwaitingFirstMessage,
        "waiting for its next chunk" => HeldUploadProgress::AwaitingNextChunk,
        "sending chunks slowly" => HeldUploadProgress::SendingSlowly,
        other => panic!("unsupported held upload progress '{other}'"),
    };
    let domain = world.domain.clone();
    let process = world
        .server_process
        .as_mut()
        .expect("a nervix-server process must be started first");
    let upload = process
        .hold_resource_upload(&domain, &resource, progress)
        .await
        .unwrap_or_else(|error| panic!("failed to hold a resource upload open: {error}"));
    world.held_resource_upload = Some(upload);
}

#[when(expr = "the server process receives {word}")]
async fn when_server_process_receives_signal(world: &mut ScenarioWorld, signal: String) {
    let signal = signal
        .parse::<nix::sys::signal::Signal>()
        .expect("the scenario names a signal such as SIGTERM");
    let process = world
        .server_process
        .as_ref()
        .expect("a nervix-server process must be started first");
    let signalled_at = Instant::now();
    process
        .send_signal(signal)
        .unwrap_or_else(|error| panic!("{error}"));
    world.last_server_signal_at = Some(signalled_at);
}

#[then(expr = "the server process exits because of {word}")]
async fn then_server_process_exits_because_of_signal(world: &mut ScenarioWorld, expected: String) {
    let expected = expected
        .parse::<nix::sys::signal::Signal>()
        .assured("the scenario names a recognized signal such as SIGKILL");
    world.server_process_http_load = None;
    let process = world
        .server_process
        .as_mut()
        .verified("the preceding step started a nervix-server process");
    let status = process
        .wait_for_exit()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let observed = status
        .signal()
        .and_then(|signal| nix::sys::signal::Signal::try_from(signal).ok());
    assert_eq!(
        observed,
        Some(expected),
        "nervix-server ended with {}, expected {expected}\n{}",
        describe_exit(status),
        process.log_tail()
    );
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
}

#[then(expr = "the server process is terminated by {word} within {string} of the last signal")]
async fn then_server_process_is_terminated_by_signal_within(
    world: &mut ScenarioWorld,
    expected_signal: String,
    bound: String,
) {
    let expected_signal = expected_signal
        .parse::<nix::sys::signal::Signal>()
        .assured("the scenario names a recognized signal such as SIGKILL");
    let bound = humantime::parse_duration(&bound)
        .assured("the scenario termination bound is a valid duration");
    let signalled_at = world
        .last_server_signal_at
        .verified("the preceding step delivered a signal to the server process");
    world.server_process_http_load = None;
    let process = world
        .server_process
        .as_mut()
        .verified("the preceding step started a nervix-server process");
    let status = process
        .wait_for_exit()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let elapsed = signalled_at.elapsed();
    let actual_signal = match status.signal() {
        Some(signal) => nix::sys::signal::Signal::try_from(signal).ok(),
        None => None,
    };
    assert_eq!(
        actual_signal,
        Some(expected_signal),
        "nervix-server ended with {}, expected termination by {expected_signal}\n{}",
        describe_exit(status),
        process.log_tail()
    );
    assert!(
        elapsed <= bound,
        "nervix-server exited {elapsed:?} after the last signal, beyond {bound:?}\n{}",
        process.log_tail()
    );
}

#[when("the server process is restarted")]
async fn when_server_process_is_restarted(world: &mut ScenarioWorld) {
    assert!(
        world.server_process_http_load.is_none(),
        "server process HTTP load must stop before the process restarts"
    );
    world
        .server_process
        .as_mut()
        .verified("the preceding step started a nervix-server process")
        .restart()
        .await
        .unwrap_or_else(|error| panic!("failed to restart nervix-server: {error}"));
}

#[when("the server process is restarted from its existing database")]
async fn when_server_process_is_restarted_from_existing_database(world: &mut ScenarioWorld) {
    assert!(
        world.server_process_http_load.is_none(),
        "server process HTTP load must stop before the process restarts"
    );
    world
        .server_process
        .as_mut()
        .verified("the preceding step started a nervix-server process")
        .restart()
        .await
        .unwrap_or_else(|error| panic!("failed to restart nervix-server: {error}"));
}

#[when("the current client-wire baseline is captured")]
async fn when_current_client_wire_baseline_is_captured(world: &mut ScenarioWorld) {
    let domain = world.domain.clone();
    let test_id = world.test_id.clone();
    let process = world
        .server_process
        .as_ref()
        .verified("the preceding step started a nervix-server process");
    let artifact = crate::common::client_wire_baseline::capture(process, &domain, &test_id)
        .await
        .unwrap_or_else(|error| panic!("client-wire baseline failed: {error:#}"));
    world.placeholders.insert(
        "client_wire_baseline_artifact".to_string(),
        artifact.display().to_string(),
    );
}

#[then("the client-wire baseline artifact exists")]
fn then_client_wire_baseline_artifact_exists(world: &mut ScenarioWorld) {
    let artifact = world
        .placeholders
        .get("client_wire_baseline_artifact")
        .verified("the preceding step captured the baseline artifact");
    assert!(
        Path::new(artifact).is_file(),
        "client-wire baseline artifact was not written to {artifact}"
    );
}

/// How long a probe may take to open its session and subscription. Starting a JVM or compiling
/// nothing still costs seconds on a loaded machine, so this bounds a wait, not a race.
const CLIENT_PROBE_SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(180);

#[when(
    expr = "the {string} client probe subscribes as {string} to relay {string} on node {string} \
            expecting {int} rows"
)]
async fn when_client_probe_subscribes(
    world: &mut ScenarioWorld,
    runtime: String,
    subscription: String,
    relay: String,
    node_id: String,
    rows: usize,
) {
    let runtime: ProbeRuntime = runtime
        .parse()
        .expect("the step names a known probe runtime");
    let node_id = expand_placeholders(world, &node_id);
    let cluster = world.cluster();
    let grpc_uri = cluster
        .grpc_uri(&node_id)
        .expect("the probe's node belongs to the cluster");
    let console = cluster
        .web_console_url(&node_id)
        .expect("the probe's node belongs to the cluster");
    let mut websocket_uri =
        url::Url::parse(&console).expect("the harness builds a valid console URL");
    websocket_uri
        .set_scheme("ws")
        .expect("an http URL can take the ws scheme");
    websocket_uri.set_path("/console/ws");
    let target = ProbeTarget {
        grpc_uri,
        websocket_uri: websocket_uri.to_string(),
        username: TEST_AUTH_USERNAME.to_string(),
        password: TEST_AUTH_PASSWORD.to_string(),
        domain: world.domain.clone(),
        relay: expand_placeholders(world, &relay),
        subscription: expand_placeholders(world, &subscription),
        rows,
    };
    append_cucumber_log_line(&format!(
        "client probe {runtime:?}: node={node_id} target={target:?}"
    ));
    let mut probe = ClientProbe::start(runtime, target)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    probe
        .wait_for_line(SUBSCRIBED_LINE, CLIENT_PROBE_SUBSCRIBE_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    world.client_probe = Some(probe);
}

#[when(expr = "the {string} client probe decodes the conformance corpus")]
async fn when_client_probe_decodes_the_corpus(world: &mut ScenarioWorld, runtime: String) {
    let runtime: ProbeRuntime = runtime
        .parse()
        .expect("the step names a known probe runtime");
    let probe = ClientProbe::start_corpus(runtime)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    world.client_probe = Some(probe);
}

#[then(expr = "within {string} the client probe reports the conformance corpus")]
async fn then_client_probe_reports_the_corpus(world: &mut ScenarioWorld, within: String) {
    let within =
        humantime::parse_duration(&within).expect("step duration must be a valid duration");
    let probe = world
        .client_probe
        .take()
        .verified("a preceding step started a client probe");
    let report = probe
        .finish(within)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let expected = corpus_report().expect("the conformance corpus report is checked in");
    report
        .check(&expected)
        .unwrap_or_else(|error| panic!("{error}"));
}

#[then(expr = "within {string} the client probe reports")]
async fn then_client_probe_reports(world: &mut ScenarioWorld, within: String, #[step] step: &Step) {
    let within =
        humantime::parse_duration(&within).expect("step duration must be a valid duration");
    let probe = world
        .client_probe
        .take()
        .verified("a preceding step started a client probe");
    let report = probe
        .finish(within)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let expected = expand_placeholders(world, docstring(step));
    report
        .check(&expected)
        .unwrap_or_else(|error| panic!("{error}"));
}

#[then(expr = "the server process exits with status {int}")]
async fn then_server_process_exits_with_status(world: &mut ScenarioWorld, expected: i32) {
    let process = world
        .server_process
        .as_mut()
        .expect("a nervix-server process must be started first");
    let status = process
        .wait_for_exit()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        status.code(),
        Some(expected),
        "nervix-server ended with {}, expected exit status {expected}\n{}",
        describe_exit(status),
        process.log_tail()
    );
}

#[then(expr = "the server process exits with status {int} within {string} of the last signal")]
async fn then_server_process_exits_with_status_within(
    world: &mut ScenarioWorld,
    expected: i32,
    bound: String,
) {
    let bound = humantime::parse_duration(&bound).expect("step duration must be a valid duration");
    let elapsed = server_process_exit_after_last_signal(world, expected).await;
    let process = world
        .server_process
        .as_ref()
        .expect("a nervix-server process must be started first");
    assert!(
        elapsed <= bound,
        "nervix-server exited {elapsed:?} after the last signal, beyond {bound:?}\n{}",
        process.log_tail()
    );
}

#[then(
    expr = "the server process exits with status {int} no sooner than {string} and within \
            {string} of the last signal"
)]
async fn then_server_process_exits_with_status_between(
    world: &mut ScenarioWorld,
    expected: i32,
    earliest: String,
    bound: String,
) {
    let earliest =
        humantime::parse_duration(&earliest).expect("step duration must be a valid duration");
    let bound = humantime::parse_duration(&bound).expect("step duration must be a valid duration");
    let elapsed = server_process_exit_after_last_signal(world, expected).await;
    let process = world
        .server_process
        .as_ref()
        .expect("a nervix-server process must be started first");
    assert!(
        elapsed >= earliest,
        "nervix-server exited {elapsed:?} after the last signal, sooner than {earliest:?}\n{}",
        process.log_tail()
    );
    assert!(
        elapsed <= bound,
        "nervix-server exited {elapsed:?} after the last signal, beyond {bound:?}\n{}",
        process.log_tail()
    );
}

/// Waits for the server process to exit with `expected` and returns how long after the last
/// signal it exited.
///
/// The signal instant was taken before the signal was delivered, so the interval can only read
/// longer than the process actually took, never shorter.
async fn server_process_exit_after_last_signal(
    world: &mut ScenarioWorld,
    expected: i32,
) -> Duration {
    let signalled_at = world
        .last_server_signal_at
        .expect("a signal must be delivered to the server process first");
    let process = world
        .server_process
        .as_mut()
        .expect("a nervix-server process must be started first");
    let status = process
        .wait_for_exit()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let elapsed = signalled_at.elapsed();
    assert_eq!(
        status.code(),
        Some(expected),
        "nervix-server ended with {}, expected exit status {expected}\n{}",
        describe_exit(status),
        process.log_tail()
    );
    elapsed
}

#[then(expr = "the server process log eventually contains {string}")]
async fn then_server_process_log_eventually_contains(world: &mut ScenarioWorld, fragment: String) {
    world
        .server_process
        .as_mut()
        .expect("a nervix-server process must be started first")
        .wait_for_log(&fragment)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
}

#[then(expr = "the server process log contains {string}")]
fn then_server_process_log_contains(world: &mut ScenarioWorld, fragment: String) {
    let process = world
        .server_process
        .as_ref()
        .expect("a nervix-server process must be started first");
    let log = process
        .log()
        .unwrap_or_else(|error| panic!("the server process log is unreadable: {error}"));
    assert!(
        log.contains(&fragment),
        "nervix-server did not log {fragment:?}\n{}",
        process.log_tail()
    );
}

#[then(expr = "the server process log does not contain {string}")]
fn then_server_process_log_does_not_contain(world: &mut ScenarioWorld, fragment: String) {
    let process = world
        .server_process
        .as_ref()
        .expect("a nervix-server process must be started first");
    assert!(
        process.has_exited(),
        "a line is only known to be absent from the log of a process that has exited"
    );
    let log = process
        .log()
        .unwrap_or_else(|error| panic!("the server process log is unreadable: {error}"));
    assert!(
        !log.contains(&fragment),
        "nervix-server logged {fragment:?}\n{}",
        process.log_tail()
    );
}

/// Resolves the formatter beside the server binary.
///
/// `CARGO_BIN_EXE_` is only defined for binaries of the test target's own package, and the
/// formatter lives in its own crate, so it is located by its neighbour in the same target
/// directory. `just test-scenarios` builds it through `tests-deps`.
fn nspl_format_binary() -> PathBuf {
    let candidate = Path::new(env!("CARGO_BIN_EXE_nervix-server"))
        .parent()
        .expect("the server binary must live in a directory")
        .join("nervix-nspl-format");
    assert!(
        candidate.exists(),
        "nervix-nspl-format must be built beside nervix-server; run `just test-scenarios`"
    );
    candidate
}

/// Resolves the public CLI built by `tests-deps` beside the server binary.
fn scenario_cli_binary() -> PathBuf {
    let candidate = Path::new(env!("CARGO_BIN_EXE_nervix-server"))
        .parent()
        .assured("the server binary has a parent directory")
        .join("nervix-cli");
    assert!(candidate.exists(), "tests-deps must build nervix-cli");
    candidate
}

impl ScenarioWorld {
    async fn execute_cli(&mut self, command: String, node: String, password: &str) {
        let command = expand_placeholders(self, &command);
        let node = expand_placeholders(self, &node);
        let grpc_uri = self
            .cluster()
            .grpc_uri(&node)
            .assured("the scenario names a cluster node");
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            tokio::process::Command::new(scenario_cli_binary())
                .args([
                    "--server",
                    &grpc_uri,
                    "--domain",
                    &self.domain,
                    "--username",
                    TEST_AUTH_USERNAME,
                    "--password",
                    password,
                    "--command",
                    &command,
                ])
                .output(),
        )
        .await;
        let output = match result {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => panic!("the scenario CLI process failed to start: {error}"),
            Err(_) => panic!("the CLI command did not complete within 60 seconds"),
        };
        self.last_cli_output = Some(output);
    }
}

#[when(expr = "the CLI executes {string} on node {string}")]
async fn when_cli_executes_on_node(world: &mut ScenarioWorld, command: String, node: String) {
    world.execute_cli(command, node, TEST_AUTH_PASSWORD).await;
}

#[when(expr = "the CLI executes {string} on node {string} with password {string}")]
async fn when_cli_executes_with_password(
    world: &mut ScenarioWorld,
    command: String,
    node: String,
    password: String,
) {
    world.execute_cli(command, node, &password).await;
}

#[then(expr = "the CLI output contains {string}")]
fn then_cli_output_contains(world: &mut ScenarioWorld, expected: String) {
    let expected = expand_placeholders(world, &expected);
    let output = world
        .last_cli_output
        .as_ref()
        .verified("the preceding step ran the CLI");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains(&expected),
        "CLI status: {}; stdout: {stdout}; stderr: {stderr}",
        output.status
    );
}

#[then(expr = "the CLI fails with {string}")]
fn then_cli_fails_with(world: &mut ScenarioWorld, expected: String) {
    let output = world
        .last_cli_output
        .as_ref()
        .verified("the preceding step ran the CLI");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && stderr.contains(&expected),
        "CLI status: {}; stderr: {stderr}",
        output.status
    );
}

#[when(expr = "the CLI subscribes to relay {string} on node {string}")]
async fn when_cli_subscribes_to_relay(world: &mut ScenarioWorld, relay: String, node: String) {
    let relay = expand_placeholders(world, &relay);
    let node = expand_placeholders(world, &node);
    let grpc_uri = world
        .cluster()
        .grpc_uri(&node)
        .assured("the scenario names a cluster node");
    let mut process = tokio::process::Command::new(scenario_cli_binary())
        .args([
            "--server",
            &grpc_uri,
            "--domain",
            &world.domain,
            "--username",
            TEST_AUTH_USERNAME,
            "--password",
            TEST_AUTH_PASSWORD,
            "subscribe",
            "watch",
            &relay,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap_or_else(|error| panic!("the scenario CLI process failed to start: {error}"));
    let stdout = process
        .stdout
        .take()
        .verified("the CLI process was started with piped stdout");
    let lines = StdArc::new(StdMutex::new(VecDeque::new()));
    let reader_lines = lines.clone();
    let task = tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            tokio::task::consume_budget().await;
            let mut retained = reader_lines
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if retained.len() == 256 {
                retained.pop_front();
            }
            retained.push_back(line);
        }
    });
    world.cli_subscription_reader = Some(AbortOnDropHandle::new(task));
    world.cli_subscription_lines = Some(lines);
    world.cli_subscription_process = Some(process);
}

#[then(expr = "the CLI subscription output eventually contains {string}")]
async fn then_cli_subscription_output_contains(world: &mut ScenarioWorld, expected: String) {
    let expected = expand_placeholders(world, &expected);
    let lines = world
        .cli_subscription_lines
        .as_ref()
        .verified("the preceding step started the CLI subscription");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::task::consume_budget().await;
        let found = {
            let retained = lines
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            retained.iter().any(|line| line.contains(&expected))
        };
        if found {
            return;
        }
        if Instant::now() >= deadline {
            let retained = lines
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            panic!("CLI subscription output did not contain {expected:?}: {retained:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The directory holding the NSPL files a formatter scenario writes.
fn formatter_root(world: &mut ScenarioWorld) -> PathBuf {
    if world.formatter_root.is_none() {
        world.formatter_root = Some(
            tempfile::Builder::new()
                .prefix("nervix-nspl-format-")
                .tempdir()
                .expect("formatter scenarios need a temporary directory"),
        );
    }
    world
        .formatter_root
        .as_ref()
        .expect("formatter directory must exist")
        .path()
        .to_path_buf()
}

fn record_formatter_output(world: &mut ScenarioWorld, output: std::process::Output) {
    world.formatter_exit_code = output.status.code();
    world.last_command_output =
        Some(String::from_utf8(output.stdout).expect("formatter output must be UTF-8"));
    world.last_command_error =
        Some(String::from_utf8(output.stderr).expect("formatter errors must be UTF-8"));
}

#[given(regex = r#"^an NSPL file "([^"]+)" containing$"#)]
fn given_an_nspl_file_containing(world: &mut ScenarioWorld, name: String, #[step] step: &Step) {
    let root = formatter_root(world);
    let path = root.join(&name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("NSPL file directory must be created");
    }
    let contents = format!("{}\n", docstring(step).trim());
    std::fs::write(&path, &contents).expect("NSPL file must be written");
    world.formatter_original_files.insert(name, contents);
}

#[when("the nervix-nspl-format help is requested")]
fn when_nspl_format_help_is_requested(world: &mut ScenarioWorld) {
    let output = Command::new(nspl_format_binary())
        .arg("--help")
        .output()
        .expect("nervix-nspl-format help must run");
    record_formatter_output(world, output);
}

#[when(regex = r#"^nervix-nspl-format formats the NSPL file "([^"]+)"$"#)]
fn when_nspl_format_formats_file(world: &mut ScenarioWorld, name: String) {
    let root = formatter_root(world);
    let output = Command::new(nspl_format_binary())
        .arg(&name)
        .current_dir(&root)
        .output()
        .expect("nervix-nspl-format must run");
    record_formatter_output(world, output);
}

#[when("nervix-nspl-format formats the NSPL directory")]
fn when_nspl_format_formats_directory(world: &mut ScenarioWorld) {
    let root = formatter_root(world);
    let output = Command::new(nspl_format_binary())
        .arg(".")
        .current_dir(&root)
        .output()
        .expect("nervix-nspl-format must run");
    record_formatter_output(world, output);
}

#[when("nervix-nspl-format checks the NSPL directory")]
fn when_nspl_format_checks_directory(world: &mut ScenarioWorld) {
    let root = formatter_root(world);
    let output = Command::new(nspl_format_binary())
        .args([".", "--check"])
        .current_dir(&root)
        .output()
        .expect("nervix-nspl-format must run");
    record_formatter_output(world, output);
}

#[when(regex = r#"^nervix-nspl-format checks the NSPL file "([^"]+)"$"#)]
fn when_nspl_format_checks_file(world: &mut ScenarioWorld, name: String) {
    let root = formatter_root(world);
    let output = Command::new(nspl_format_binary())
        .args(["--check", &name])
        .current_dir(&root)
        .output()
        .expect("nervix-nspl-format must run");
    record_formatter_output(world, output);
}

#[when("nervix-nspl-format formats the standard input")]
fn when_nspl_format_formats_stdin(world: &mut ScenarioWorld, #[step] step: &Step) {
    use std::process::Stdio;

    let contents = format!("{}\n", docstring(step).trim());
    let mut child = Command::new(nspl_format_binary())
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("nervix-nspl-format must run");
    child
        .stdin
        .as_mut()
        .expect("standard input must be piped")
        .write_all(contents.as_bytes())
        .expect("standard input must be written");
    let output = child
        .wait_with_output()
        .expect("nervix-nspl-format must finish");
    record_formatter_output(world, output);
}

#[then(regex = r"^the formatter exits with code (\d+)$")]
fn then_the_formatter_exits_with_code(world: &mut ScenarioWorld, expected: i32) {
    let actual = world
        .formatter_exit_code
        .expect("the formatter must have run before its exit code is asserted");
    assert_eq!(
        actual,
        expected,
        "formatter exit code; stdout: {:?}, stderr: {:?}",
        world.last_command_output.as_deref().unwrap_or_default(),
        world.last_command_error.as_deref().unwrap_or_default()
    );
}

#[then(regex = r#"^the NSPL file "([^"]+)" contains$"#)]
fn then_the_nspl_file_contains(world: &mut ScenarioWorld, name: String, #[step] step: &Step) {
    let root = formatter_root(world);
    let actual = std::fs::read_to_string(root.join(&name)).expect("NSPL file must be readable");
    let expected = docstring(step).trim();
    assert!(
        actual.contains(expected),
        "expected {name} to contain:\n{expected}\ngot:\n{actual}"
    );
}

#[then(regex = r#"^the NSPL file "([^"]+)" is unchanged$"#)]
fn then_the_nspl_file_is_unchanged(world: &mut ScenarioWorld, name: String) {
    let expected = world
        .formatter_original_files
        .get(&name)
        .expect("the file must have been given before it is compared")
        .clone();
    let root = formatter_root(world);
    let actual = std::fs::read_to_string(root.join(&name)).expect("NSPL file must be readable");
    assert_eq!(actual, expected, "{name} was rewritten");
}

#[then("the legacy nervix server executable is absent")]
fn then_legacy_nervix_server_executable_is_absent(_world: &mut ScenarioWorld) {
    assert!(
        option_env!("CARGO_BIN_EXE_nervix").is_none(),
        "the legacy nervix binary target must not be built"
    );
}

impl IngestorLogicTransportFixture {
    fn parse(value: &str) -> Self {
        match value {
            "http_endpoint" => Self::HttpEndpoint,
            "kafka" => Self::Kafka,
            "mqtt" => Self::Mqtt,
            "nats" => Self::Nats,
            "websocket_endpoint" => Self::WebsocketEndpoint,
            "zeromq" => Self::ZeroMq,
            other => panic!("unsupported ingestor logic transport fixture '{other}'"),
        }
    }

    async fn prepare(self, world: &mut ScenarioWorld) {
        if let Self::Kafka = self {
            let topic = expand_placeholders(world, "logic_notifications_{{test_id}}");
            world
                .cluster()
                .ensure_kafka_topic_partitions(&topic, 1)
                .await
                .expect("failed to prepare ingestor logic kafka topic");
        }
    }

    async fn await_ready(self, world: &mut ScenarioWorld) {
        let _ = world;
    }

    fn executes_on_scheduled_owner(self) -> bool {
        matches!(self, Self::Kafka | Self::Mqtt | Self::Nats | Self::ZeroMq)
    }

    fn setup_fragment(self) -> &'static str {
        match self {
            Self::HttpEndpoint => {
                r#"
      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT logic_endpoint
        ON edge
        PATH '/logic'
        TYPE HTTP;
"#
            }
            Self::Kafka => {
                r#"
      CREATE CLIENT logic_kafka
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}'
        };
"#
            }
            Self::Mqtt => {
                r#"
      CREATE CLIENT logic_mqtt
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-logic-{{test_id}}'
        };
"#
            }
            Self::Nats => {
                r#"
      CREATE CLIENT logic_nats
        TYPE NATS
        CONFIG {
          'addr' = '{{nats_addr}}'
        };
"#
            }
            Self::WebsocketEndpoint => {
                r#"
      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE ENDPOINT logic_endpoint
        ON edge
        PATH '/logic'
        TYPE WEBSOCKETS;
"#
            }
            Self::ZeroMq => {
                r#"
      CREATE CLIENT logic_zeromq
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_ingest_addr}}',
          'bind' = 'true'
        };
"#
            }
        }
    }

    fn source_fragment(self) -> &'static str {
        match self {
            Self::HttpEndpoint | Self::WebsocketEndpoint => {
                "FROM ENDPOINT logic_endpoint MODE NO_ACK SEQUENTIAL"
            }
            Self::Kafka => {
                r#"FROM KAFKA logic_kafka
        TOPIC logic_notifications_{{test_id}}
        OFFSET BY DOMAIN
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms"#
            }
            Self::Mqtt => {
                r#"FROM MQTT logic_mqtt
        TOPIC logic_notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL"#
            }
            Self::Nats => {
                r#"FROM NATS logic_nats
        SUBJECT logic_notifications_{{test_id}}
        QUEUE GROUP logic_notifications_group_{{test_id}}
        INSTANCES 1
        MODE NO_ACK SEQUENTIAL"#
            }
            Self::ZeroMq => "FROM ZEROMQ logic_zeromq MODE NO_ACK SEQUENTIAL",
        }
    }

    fn quiesce_fragment(self) -> &'static str {
        match self {
            Self::HttpEndpoint | Self::WebsocketEndpoint => "BUFFER MAX SIZE 1MiB",
            Self::Kafka | Self::ZeroMq => "SUSPEND",
            Self::Mqtt | Self::Nats => "DROP",
        }
    }

    async fn deliver(self, world: &mut ScenarioWorld, payload: &str) {
        match self {
            Self::HttpEndpoint => {
                let host = expand_placeholders(world, "http-{{test_id}}.example.com");
                world
                    .cluster()
                    .publish_http("node-1", &host, "/logic", payload)
                    .await
                    .expect("failed to post ingestor logic http payload");
            }
            Self::Kafka => {
                let topic = expand_placeholders(world, "logic_notifications_{{test_id}}");
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    tokio::task::consume_budget().await;
                    world
                        .cluster()
                        .publish_kafka(&topic, payload)
                        .await
                        .expect("failed to publish ingestor logic kafka payload");
                    if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await
                    {
                        return;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for ingestor logic kafka payload to reach the relay \
                         subscription"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Self::Mqtt => {
                let topic = expand_placeholders(world, "logic_notifications_{{test_id}}");
                world
                    .cluster()
                    .publish_mqtt(&topic, payload)
                    .await
                    .expect("failed to publish ingestor logic mqtt payload");
            }
            Self::Nats => {
                let subject = expand_placeholders(world, "logic_notifications_{{test_id}}");
                world
                    .cluster()
                    .publish_nats(&subject, payload)
                    .await
                    .expect("failed to publish ingestor logic nats payload");
            }
            Self::WebsocketEndpoint => {
                let host = expand_placeholders(world, "ws-{{test_id}}.example.com");
                world
                    .cluster()
                    .publish_websocket("node-1", &host, "/logic", payload)
                    .await
                    .expect("failed to publish ingestor logic websocket payload");
            }
            Self::ZeroMq => {
                world
                    .cluster()
                    .publish_zeromq(&world.zeromq_ingest_addr, payload)
                    .await
                    .expect("failed to publish ingestor logic zeromq payload");
            }
        }
    }

    async fn deliver_with_headers(self, world: &mut ScenarioWorld, payload: &str) {
        let headers = [
            ("tenant", "acme"),
            ("route", "header-route"),
            ("route", "fallback-route"),
        ];
        match self {
            Self::HttpEndpoint => {
                let host = expand_placeholders(world, "http-{{test_id}}.example.com");
                world
                    .cluster()
                    .publish_http_with_headers("node-1", &host, "/logic", payload, &headers)
                    .await
                    .expect("failed to post ingestor logic http payload with headers");
            }
            Self::Kafka => {
                let topic = expand_placeholders(world, "logic_notifications_{{test_id}}");
                world
                    .cluster()
                    .publish_kafka_with_headers(&topic, payload, &headers)
                    .await
                    .expect("failed to publish ingestor logic kafka payload with headers");
                let delivered =
                    try_capture_any_subscription_payload(world, Duration::from_secs(5)).await;
                assert!(
                    delivered,
                    "timed out waiting for ingestor logic kafka payload with headers to reach the \
                     relay subscription"
                );
            }
            Self::Nats => {
                let subject = expand_placeholders(world, "logic_notifications_{{test_id}}");
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    tokio::task::consume_budget().await;
                    world
                        .cluster()
                        .publish_nats_with_headers(&subject, payload, &headers)
                        .await
                        .expect("failed to publish ingestor logic nats payload with headers");
                    if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await
                    {
                        return;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for ingestor logic nats payload with headers to reach \
                         the relay subscription"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Self::Mqtt | Self::WebsocketEndpoint | Self::ZeroMq => {
                panic!("ingestor logic transport fixture '{self:?}' does not support headers")
            }
        }
    }
}

#[then("failure diagnostics redact dependency certificate bytes and endpoint values")]
fn then_failure_diagnostics_redact_dependency_secrets(world: &mut ScenarioWorld) {
    let diagnostics = format!("{world:#?}");
    world
        .dependencies
        .endpoints()
        .tls_ca_pem()
        .expect("Redis dependency should generate TLS certificate material");
    let endpoint = world
        .dependencies
        .endpoints()
        .get(REDIS_ADDR)
        .expect("Redis dependency endpoint should be available");

    assert!(
        !diagnostics.contains("tls_ca_pem"),
        "failure diagnostics must not contain raw certificate bytes"
    );
    assert!(
        !diagnostics.contains(endpoint),
        "failure diagnostics must not contain concrete dependency endpoint values"
    );
    assert!(
        diagnostics.contains(REDIS_ADDR),
        "failure diagnostics should identify configured dependency endpoint keys"
    );
    assert!(
        diagnostics.contains("tls_configured: true"),
        "failure diagnostics should report that TLS is configured"
    );
    assert!(
        diagnostics.len() < 8 * 1024,
        "failure diagnostics should remain compact"
    );
}

#[then(expr = "dependency endpoint {string} remains stable for the test suite")]
fn then_dependency_endpoint_remains_stable_for_the_test_suite(
    world: &mut ScenarioWorld,
    endpoint_key: String,
) {
    let endpoint = world
        .dependencies
        .endpoints()
        .get(&endpoint_key)
        .unwrap_or_else(|error| panic!("{error}"))
        .to_string();
    let mut observed = SUITE_DEPENDENCY_ENDPOINTS
        .get_or_init(|| StdMutex::new(BTreeMap::new()))
        .lock()
        .expect("suite dependency endpoint observations must not be poisoned");
    match observed.get(&endpoint_key) {
        Some(existing) => assert_eq!(
            existing, &endpoint,
            "dependency endpoint '{endpoint_key}' changed within one test suite"
        ),
        None => {
            observed.insert(endpoint_key, endpoint);
        }
    }
}

#[given(expr = "ingestor logic dependency {string} is running")]
async fn given_ingestor_logic_dependency_is_running(world: &mut ScenarioWorld, fixture: String) {
    initialize_scenario_identity(world);
    match IngestorLogicTransportFixture::parse(&fixture) {
        IngestorLogicTransportFixture::Kafka => world
            .dependencies
            .start_kafka(&world.test_id)
            .await
            .expect("Kafka test container should start"),
        IngestorLogicTransportFixture::Mqtt => world
            .dependencies
            .start_mqtt(&world.test_id)
            .await
            .expect("MQTT test container should start"),
        IngestorLogicTransportFixture::Nats => world
            .dependencies
            .start_nats(&world.test_id)
            .await
            .expect("NATS test container should start"),
        IngestorLogicTransportFixture::HttpEndpoint
        | IngestorLogicTransportFixture::WebsocketEndpoint
        | IngestorLogicTransportFixture::ZeroMq => {}
    }
    refresh_dependency_configuration(world);
}

#[derive(Clone, Copy, Debug)]
enum IngestorLogicOutputSchemaFixture {
    Input,
    Rewritten,
    HeaderRouted,
    Parsed,
    FunctionMatrix,
    ExtendedBuiltinMatrix,
    CastMatrix,
    ArithmeticMatrix,
    MathBuiltinMatrix,
    InternalTypes,
    ListOperations,
}

impl IngestorLogicOutputSchemaFixture {
    fn parse(value: &str) -> Self {
        match value {
            "input" => Self::Input,
            "rewritten" => Self::Rewritten,
            "header_routed" => Self::HeaderRouted,
            "parsed" => Self::Parsed,
            "function_matrix" => Self::FunctionMatrix,
            "extended_builtin_matrix" => Self::ExtendedBuiltinMatrix,
            "cast_matrix" => Self::CastMatrix,
            "arithmetic_matrix" => Self::ArithmeticMatrix,
            "math_builtin_matrix" => Self::MathBuiltinMatrix,
            "internal_types" => Self::InternalTypes,
            "list_operations" => Self::ListOperations,
            other => panic!("unsupported ingestor logic output schema fixture '{other}'"),
        }
    }

    fn schema_name(self) -> &'static str {
        match self {
            Self::Input => "logic_notification_ingest",
            Self::Rewritten => "logic_notification_rewritten",
            Self::HeaderRouted => "logic_notification_header_routed",
            Self::Parsed => "logic_notification_parsed",
            Self::FunctionMatrix => "logic_notification_function_matrix",
            Self::ExtendedBuiltinMatrix => "logic_notification_extended_builtin_matrix",
            Self::CastMatrix => "logic_notification_cast_matrix",
            Self::ArithmeticMatrix => "logic_notification_arithmetic_matrix",
            Self::MathBuiltinMatrix => "logic_notification_math_builtin_matrix",
            Self::InternalTypes => "logic_notification_internal_types",
            Self::ListOperations => "logic_notification_list_operations",
        }
    }

    fn input_codec_name(self) -> &'static str {
        match self {
            Self::InternalTypes => "logic_notification_internal_types_ingest_codec",
            Self::ListOperations => "logic_notification_list_operations_ingest_codec",
            Self::Input
            | Self::Rewritten
            | Self::HeaderRouted
            | Self::Parsed
            | Self::FunctionMatrix
            | Self::ExtendedBuiltinMatrix
            | Self::CastMatrix
            | Self::ArithmeticMatrix
            | Self::MathBuiltinMatrix => "logic_notification_ingest_codec",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum IngestorLogicPayloadFixture {
    MixedFilterMessages,
    HeaderMessage,
    RuntimeFailureMessage,
    FunctionMatrixMessage,
    ExtendedBuiltinMessage,
    CastMatrixMessage,
    ArithmeticMessage,
    MathBuiltinMessage,
    InternalTypesMessage,
    ListOperationsMessage,
}

impl IngestorLogicPayloadFixture {
    fn parse(value: &str) -> Self {
        match value {
            "mixed_filter_messages" => Self::MixedFilterMessages,
            "header_message" => Self::HeaderMessage,
            "runtime_failure_message" => Self::RuntimeFailureMessage,
            "function_matrix_message" => Self::FunctionMatrixMessage,
            "extended_builtin_message" => Self::ExtendedBuiltinMessage,
            "cast_matrix_message" => Self::CastMatrixMessage,
            "arithmetic_message" => Self::ArithmeticMessage,
            "math_builtin_message" => Self::MathBuiltinMessage,
            "internal_types_message" => Self::InternalTypesMessage,
            "list_operations_message" => Self::ListOperationsMessage,
            other => panic!("unsupported ingestor logic payload fixture '{other}'"),
        }
    }

    fn payloads(self) -> &'static [&'static str] {
        match self {
            Self::MixedFilterMessages => &[
                r#"{"tenant":"acme","active":true,"amount":7,"raw":"URGENT"}"#,
                r#"{"tenant":"acme","active":false,"amount":8,"raw":"DROP"}"#,
            ],
            Self::HeaderMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"ignored"}"#]
            }
            Self::RuntimeFailureMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"not-a-number"}"#]
            }
            Self::FunctionMatrixMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"  KeepMe  "}"#]
            }
            Self::ExtendedBuiltinMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"  hello.world  "}"#]
            }
            Self::CastMatrixMessage => {
                &[r#"{"tenant":"acme","active":false,"amount":42,"raw":"42"}"#]
            }
            Self::ArithmeticMessage => {
                &[r#"{"tenant":"acme","active":false,"amount":20,"raw":"6"}"#]
            }
            Self::MathBuiltinMessage => {
                &[r#"{"tenant":"acme","active":true,"amount":7,"raw":"ignored"}"#]
            }
            Self::InternalTypesMessage => &[
                r#"{"tenant":"acme","active":true,"u8":5,"i8":-7,"u16":9,"i16":12,"u32":42,"i32":-11,"u64":100,"i64":-64,"f32":2.5,"f64":7.25,"occurred_at":"2026-04-07T12:34:56Z","raw":"ignored"}"#,
            ],
            Self::ListOperationsMessage => &[
                r#"{"tenant":"acme","values":[1,2,3],"fixed":[10,20],"labels":["prod","api","edge"]}"#,
            ],
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum IngestorLogicExpectationFixture {
    CompileError,
    RewrittenFilteredOnce,
    HeaderRoutedOnce,
    RuntimeErrorDrop,
}

impl IngestorLogicExpectationFixture {
    fn parse(value: &str) -> Self {
        match value {
            "compile_error" => Self::CompileError,
            "rewritten_filtered_once" => Self::RewrittenFilteredOnce,
            "header_routed_once" => Self::HeaderRoutedOnce,
            "runtime_error_drop" => Self::RuntimeErrorDrop,
            other => panic!("unsupported ingestor logic expectation fixture '{other}'"),
        }
    }

    async fn assert_observed(self, world: &mut ScenarioWorld) {
        match self {
            Self::CompileError => {
                let error = world
                    .last_command_error
                    .as_deref()
                    .expect("logic compile failure should populate last_command_error");
                append_cucumber_log_line(&format!("logic compile error observed: {error}"));
            }
            Self::RewrittenFilteredOnce => {
                capture_and_assert_subscription_payload(
                    world,
                    "\"normalized\":\"urgent\"",
                    false,
                    Duration::from_secs(10),
                )
                .await;
                let payload = world
                    .last_subscription_payload
                    .as_deref()
                    .expect("logic rewrite payload must be captured");
                assert!(
                    payload.contains("\"amount\":8"),
                    "expected rewritten payload to contain incremented amount, got: {payload}"
                );
                assert!(
                    payload.contains(r#"key={"tenant":"acme"}"#),
                    "expected rewritten payload to preserve tenant key, got: {payload}"
                );
                assert!(
                    !payload.contains("\"raw\""),
                    "expected rewritten payload to omit raw field, got: {payload}"
                );
                assert_no_subscription_payload_within(world, Duration::from_secs(1)).await;
            }
            Self::HeaderRoutedOnce => {
                capture_and_assert_subscription_payload(
                    world,
                    "\"normalized\":\"header-route\"",
                    false,
                    Duration::from_secs(10),
                )
                .await;
                let payload = world
                    .last_subscription_payload
                    .as_deref()
                    .expect("header rewrite payload must be captured");
                assert!(
                    payload.contains("\"amount\":9"),
                    "expected header-routed payload to count ordered route headers, got: {payload}"
                );
                assert!(
                    payload.contains(r#"key={"tenant":"acme"}"#),
                    "expected header-routed payload to preserve tenant key, got: {payload}"
                );
                assert!(
                    !payload.contains("\"raw\""),
                    "expected header-routed payload to omit raw field, got: {payload}"
                );
                assert_no_subscription_payload_within(world, Duration::from_secs(1)).await;
            }
            Self::RuntimeErrorDrop => {
                then_within_duration_the_active_session_observes_a_server_error(
                    world,
                    "2s".to_string(),
                )
                .await;
                assert_no_subscription_payload_within(world, Duration::from_secs(1)).await;
            }
        }
    }
}

fn build_ingestor_logic_commands(
    transport: IngestorLogicTransportFixture,
    output_schema: IngestorLogicOutputSchemaFixture,
    logic_program: &str,
    include_subscription: bool,
) -> String {
    let subscription_commands = if include_subscription {
        let start_command = if let IngestorLogicTransportFixture::Kafka = transport {
            "START AT NOW;"
        } else {
            "START;"
        };
        format!(
            r#"
      CREATE SUBSCRIPTION logic_notifications_subscription TO logic_notifications;
      {start_command}
"#
        )
    } else {
        String::new()
    };
    let timestamp_clause = if let IngestorLogicTransportFixture::Kafka = transport {
        "TIMESTAMP NOW"
    } else {
        ""
    };
    let logic_program = logic_program.trim().trim_end_matches(';');
    format!(
        r#"
      CREATE SCHEMA logic_notification_ingest (
        tenant STRING,
        active BOOL,
        amount I64,
        raw STRING
      );

      CREATE SCHEMA logic_notification_internal_types_ingest (
        tenant STRING,
        active BOOL,
        u8 U8,
        i8 I8,
        u16 U16,
        i16 I16,
        u32 U32,
        i32 I32,
        u64 U64,
        i64 I64,
        f32 F32,
        f64 F64,
        occurred_at DATETIME,
        raw STRING
      );

      CREATE SCHEMA logic_notification_list_operations_ingest (
        tenant STRING,
        values VEC<I64>,
        fixed ARRAY<I64, 2>,
        labels VEC<STRING>
      );

      CREATE SCHEMA logic_notification_rewritten (
        tenant STRING,
        active BOOL,
        amount I64,
        normalized STRING
      );

      CREATE SCHEMA logic_notification_header_routed (
        tenant STRING,
        active BOOL,
        amount I64,
        normalized STRING OPTIONAL
      );

      CREATE SCHEMA logic_notification_parsed (
        tenant STRING,
        parsed I64
      );

      CREATE SCHEMA logic_notification_function_matrix (
        tenant STRING,
        amount_abs I64,
        trimmed STRING,
        lowered STRING,
        uppered STRING,
        raw_len I64,
        contains_keep BOOL,
        starts_keep BOOL,
        ends_me BOOL,
        fallback STRING,
        was_keep BOOL
      );

      CREATE SCHEMA logic_notification_extended_builtin_matrix (
        tenant STRING,
        now_text STRING,
        uuid4 STRING,
        uuid7 STRING,
        bit_len I64,
        ascii_value I64,
        btrimmed STRING,
        char_len I64,
        joined STRING,
        titled STRING,
        lefted STRING,
        lowered STRING,
        lpaded STRING,
        ltrimmed STRING,
        digest STRING,
        repeated STRING,
        replaced STRING,
        reversed STRING,
        righted STRING,
        rpaded STRING,
        rtrimmed STRING,
        part STRING,
        starts BOOL,
        pos I64,
        piece STRING,
        hexed STRING,
        translated STRING,
        trimmed2 STRING,
        uppered STRING,
        regex_ok BOOL,
        regex_replaced STRING,
        regex_piece STRING,
        unicode_chars I64,
        unicode_trimmed STRING,
        empty_replaced STRING
      );

      CREATE SCHEMA logic_notification_cast_matrix (
        tenant STRING,
        parsed I64,
        amount_text STRING,
        amount_float F64,
        truthy BOOL,
        not_active BOOL,
        literal_bool BOOL,
        literal_float F64,
        literal_int I64,
        label STRING,
        is_exact BOOL,
        negated I64
      );

      CREATE SCHEMA logic_notification_arithmetic_matrix (
        tenant STRING,
        parsed I64,
        sum I64,
        difference I64,
        product I64,
        quotient I64,
        remainder I64,
        complex I64,
        comparison BOOL,
        chained STRING
      );

      CREATE SCHEMA logic_notification_math_builtin_matrix (
        tenant STRING,
        absolute I64,
        acos_value F64,
        asin_value F64,
        atan_value F64,
        ceil_value F64,
        cos_value F64,
        exp_value F64,
        floor_value F64,
        ln_value F64,
        log_value F64,
        log_base_value F64,
        pow_value F64,
        round_value F64,
        sqrt_value F64,
        tan_value F64
      );

      CREATE SCHEMA logic_notification_internal_types (
        tenant STRING,
        u8_next U8,
        i8_abs I8,
        u16_keep U16,
        i16_prev I16,
        u32_same U32,
        i32_neg I32,
        u64_next U64,
        i64_keep I64,
        f32_next F32,
        f64_keep F64,
        bool_copy BOOL,
        occurred_text STRING,
        occurred_copy DATETIME
      );

      CREATE SCHEMA logic_notification_list_operations (
        tenant STRING,
        total I64 OPTIONAL,
        first_value I64 OPTIONAL,
        last_value I64 OPTIONAL,
        second_value I64 OPTIONAL,
        value_count I64,
        fixed_first I64 OPTIONAL,
        fixed_last I64 OPTIONAL,
        first_label STRING OPTIONAL,
        last_label STRING OPTIONAL
      );

      CREATE WIRE JSON SCHEMA logic_notification_ingest_wire MODE STRICT (
        tenant string,
        active boolean,
        amount integer,
        raw string
      );

      CREATE WIRE JSON SCHEMA logic_notification_internal_types_ingest_wire MODE STRICT (
        tenant string,
        active boolean,
        u8 integer,
        i8 integer,
        u16 integer,
        i16 integer,
        u32 integer,
        i32 integer,
        u64 integer,
        i64 integer,
        f32 number,
        f64 number,
        occurred_at string,
        raw string
      );

      CREATE WIRE JSON SCHEMA logic_notification_list_operations_ingest_wire MODE STRICT (
        tenant string,
        values array,
        fixed array,
        labels array
      );

      CREATE CODEC logic_notification_ingest_codec
        FROM WIRE JSON SCHEMA logic_notification_ingest_wire
        TO SCHEMA logic_notification_ingest;

      CREATE CODEC logic_notification_internal_types_ingest_codec
        FROM WIRE JSON SCHEMA logic_notification_internal_types_ingest_wire
        TO SCHEMA logic_notification_internal_types_ingest
        ENCODE occurred_at AS RFC3339;

      CREATE CODEC logic_notification_list_operations_ingest_codec
        FROM WIRE JSON SCHEMA logic_notification_list_operations_ingest_wire
        TO SCHEMA logic_notification_list_operations_ingest;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_logic_ingestor
        SCHEMA tenant_branch
        TTL 5m;
      CREATE RELAY logic_notifications SCHEMA {} BRANCHED BY by_logic_ingestor;
      {}
      CREATE INGESTOR logic_ingestor
        {}
        ON QUIESCE {}
        DECODE USING {}
        {}
        TO logic_notifications
        {}
        BRANCHED BY by_logic_ingestor
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      {}
"#,
        output_schema.schema_name(),
        transport.setup_fragment(),
        transport.source_fragment(),
        transport.quiesce_fragment(),
        output_schema.input_codec_name(),
        timestamp_clause,
        logic_program,
        subscription_commands
    )
}

fn append_cucumber_log_line(line: &str) {
    let _ = create_dir_all(TEST_LOG_DIR);
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(CUCUMBER_LOG_FILE)
    {
        let _ = writeln!(file, "{line}");
    }
}

fn truncate_cucumber_log() {
    let _ = create_dir_all(TEST_LOG_DIR);
    let _ = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(CUCUMBER_LOG_FILE);
}

async fn clickhouse_post_to(
    url: &str,
    ca_file: Option<PathBuf>,
    query: &str,
) -> Result<String, String> {
    let mut builder = reqwest::Client::builder();
    if let Some(ca_file) = ca_file {
        let ca_pem = std::fs::read(ca_file)
            .map_err(|source| format!("failed to read ClickHouse TLS CA: {source}"))?;
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_pem(&ca_pem)
                .map_err(|source| format!("failed to parse ClickHouse TLS CA: {source}"))?,
        );
    }
    let response = builder
        .build()
        .map_err(|source| source.to_string())?
        .post(url)
        .basic_auth("default", Some("nervix"))
        .body(query.to_string())
        .send()
        .await
        .map_err(|source| source.to_string())?;
    let status = response.status();
    let body = response.text().await.map_err(|source| source.to_string())?;
    if status.is_success() {
        Ok(body)
    } else {
        Err(format!("ClickHouse query failed with {status}: {body}"))
    }
}

async fn clickhouse_post(
    dependencies: &DependencyEndpoints,
    query: &str,
) -> Result<String, String> {
    let addr = dependencies
        .get(CLICKHOUSE_ADDR)
        .map_err(|error| error.to_string())?;
    clickhouse_post_to(&format!("{addr}/"), None, query).await
}

async fn clickhouse_tls_post(
    dependencies: &DependencyEndpoints,
    query: &str,
) -> Result<String, String> {
    let addr = dependencies
        .get(CLICKHOUSE_TLS_ADDR)
        .map_err(|error| error.to_string())?;
    let ca_file = dependencies
        .tls_ca_path()
        .map_err(|error| error.to_string())?
        .to_path_buf();
    clickhouse_post_to(&format!("{addr}/"), Some(ca_file), query).await
}

async fn clickhouse_post_for_world(world: &ScenarioWorld, query: &str) -> Result<String, String> {
    if world.clickhouse_tls {
        clickhouse_tls_post(world.dependencies.endpoints(), query).await
    } else {
        clickhouse_post(world.dependencies.endpoints(), query).await
    }
}

/// A verification client for the Postgres the scenario wrote to.
///
/// The same driver the product uses, so a scenario cannot pass against a connection contract the
/// runtime does not actually speak.
async fn postgres_client(
    dependencies: &DependencyEndpoints,
    tls: bool,
) -> Result<SqlxPgPool, String> {
    let addr = if tls {
        dependencies.get(POSTGRES_TLS_ADDR)
    } else {
        dependencies.get(POSTGRES_ADDR)
    }
    .map_err(|error| error.to_string())?;
    let mut options: SqlxPgConnectOptions = addr.parse().map_err(|source| format!("{source}"))?;
    if tls {
        let ca_file = dependencies
            .tls_ca_path()
            .map_err(|error| error.to_string())?;
        options = options
            .ssl_mode(SqlxPgSslMode::VerifyFull)
            .ssl_root_cert(ca_file);
    }
    SqlxPgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .map_err(|source| source.to_string())
}

fn mysql_pool(dependencies: &DependencyEndpoints, tls: bool) -> Result<MySqlPool, String> {
    let addr = if tls {
        dependencies.get(MYSQL_TLS_ADDR)
    } else {
        dependencies.get(MYSQL_ADDR)
    }
    .map_err(|error| error.to_string())?;
    let opts = MySqlOpts::from_url(addr).map_err(|source| source.to_string())?;
    let opts = if tls {
        let ca_file = dependencies
            .tls_ca_path()
            .map_err(|error| error.to_string())?
            .to_path_buf();
        let ssl_opts = MySqlSslOpts::default()
            .with_root_certs(vec![ca_file.into()])
            .with_disable_built_in_roots(true);
        MySqlOptsBuilder::from_opts(opts).ssl_opts(Some(ssl_opts))
    } else {
        MySqlOptsBuilder::from_opts(opts)
    };
    Ok(MySqlPool::new(opts))
}

fn mysql_root_pool(dependencies: &DependencyEndpoints) -> Result<MySqlPool, String> {
    let addr = dependencies
        .get(MYSQL_ADDR)
        .map_err(|error| error.to_string())?;
    let root_addr = addr.replacen("mysql://nervix:nervix@", "mysql://root:nervix@", 1);
    if root_addr == addr {
        return Err(format!(
            "MySQL test endpoint does not use the expected nervix credentials: {addr}"
        ));
    }
    let opts = MySqlOpts::from_url(&root_addr).map_err(|source| source.to_string())?;
    Ok(MySqlPool::new(opts))
}

async fn mongodb_client(
    dependencies: &DependencyEndpoints,
    tls: bool,
) -> Result<MongoDbClient, String> {
    let addr = if tls {
        dependencies.get(MONGODB_TLS_ADDR)
    } else {
        dependencies.get(MONGODB_ADDR)
    }
    .map_err(|error| error.to_string())?;
    let mut options = MongoDbClientOptions::parse(addr)
        .await
        .map_err(|source| source.to_string())?;
    if tls {
        let ca_file = dependencies
            .tls_ca_path()
            .map_err(|error| error.to_string())?
            .to_path_buf();
        options.tls = Some(MongoDbTls::Enabled(
            MongoDbTlsOptions::builder().ca_file_path(ca_file).build(),
        ));
    }
    MongoDbClient::with_options(options).map_err(|source| source.to_string())
}

async fn append_cluster_statuses(world: &ScenarioWorld, prefix: &str) {
    let Some(cluster) = world.cluster.as_ref() else {
        append_cucumber_log_line(&format!("{prefix}: cluster not initialized"));
        return;
    };
    let snapshots = cluster.collect_status_snapshots().await;
    if snapshots.is_empty() {
        append_cucumber_log_line(&format!("{prefix}: no cluster nodes"));
        return;
    }
    for (node_id, snapshot) in snapshots {
        match snapshot {
            Ok(status) => append_cucumber_log_line(&format!(
                "{prefix}: node={node_id} status={}",
                status.replace('\n', "\\n")
            )),
            Err(error) => match error.current_context() {
                StatusRequestError::DeadlinePassed { operation, budget } => {
                    append_cucumber_log_line(&format!(
                        "{prefix}: node={node_id} status_timeout={operation} pending after \
                         {budget:?}"
                    ));
                }
                _ => append_cucumber_log_line(&format!(
                    "{prefix}: node={node_id} status_error={error:#}"
                )),
            },
        }
    }
}

/// Shrink the snapshot and retention bounds so a scenario can reach compaction with a small
/// number of committed entries instead of the production thresholds.
#[given(expr = "raft snapshots after {int} entries retaining {int} covered entries")]
async fn given_raft_retention_bounds(
    world: &mut ScenarioWorld,
    snapshot_entries: u64,
    covered_entries: u64,
) {
    assert!(
        world.cluster.is_none(),
        "raft retention must be configured before the cluster starts"
    );
    world.cluster_config.raft_retention = nervix_consensus::RaftRetentionPolicy {
        snapshot_entry_threshold: snapshot_entries,
        covered_entries_retained: covered_entries,
        ..nervix_consensus::RaftRetentionPolicy::default()
    };
}

#[given(expr = "consensus commits on node {string} take {string}")]
fn given_consensus_commits_take(world: &mut ScenarioWorld, node_id: String, duration: String) {
    let node_id = expand_placeholders(world, &node_id);
    let duration = humantime::parse_duration(&duration)
        .assured("the Cucumber expression supplies a valid consensus commit duration");
    world
        .fault_injection
        .set_consensus_storage_commit_delay(&crate::common::cluster::node_name(&node_id), duration);
    world.consensus_commit_delays.insert(node_id, duration);
}

#[when(expr = "{int} domains named {string} are created on the leader node")]
async fn when_domains_are_created_in_a_burst(
    world: &mut ScenarioWorld,
    count: usize,
    prefix: String,
) {
    let leader = running_leader_node(world).await;
    let observer = world
        .fault_injection
        .consensus_observer(&crate::common::cluster::node_name(&leader));
    let mut retention_peak = observer.raft_log_retention();
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .unwrap_or_else(|error| panic!("failed to open burst NSPL session: {error}"));
    for index in 0..count {
        tokio::task::consume_budget().await;
        let name = burst_domain_name(&prefix, index);
        session
            .run_command(&format!("CREATE DOMAIN {name};"))
            .await
            .unwrap_or_else(|error| panic!("creating domain '{name}' failed: {error}"));
        let retention = observer.raft_log_retention();
        if retention.retained_bytes > retention_peak.retained_bytes {
            retention_peak = retention;
        }
    }
    world.burst_raft_retention_peak = Some(retention_peak);
}

fn burst_domain_name(prefix: &str, index: usize) -> String {
    format!("{prefix}_{index:03}")
}

async fn applied_burst_domains(world: &ScenarioWorld, node_id: &str, prefix: &str) -> usize {
    let observer = world
        .fault_injection
        .consensus_observer(&crate::common::cluster::node_name(node_id));
    let domains = observer.current_domains().await;
    let burst_prefix = format!("{prefix}_");
    let mut applied = 0_usize;
    for domain in domains.keys() {
        if !domain.as_str().starts_with(&burst_prefix) {
            continue;
        }
        applied = applied
            .checked_add(1)
            .assured("a scenario creates a bounded number of burst domains");
    }
    applied
}

fn durable_catch_up_bound(commit_delay: Duration, entries: usize) -> Duration {
    let entries = u32::try_from(entries)
        .assured("a durable catch-up scenario creates at most u32::MAX entries");
    let delayed_commits = entries
        .checked_mul(DURABLE_CATCH_UP_STORAGE_COMMITS_PER_ENTRY)
        .assured("twice the bounded scenario entry count fits in u32");
    let storage_delay = commit_delay
        .checked_mul(delayed_commits)
        .assured("the configured test delay times its bounded commit count fits in Duration");
    storage_delay
        .checked_add(DURABLE_CATCH_UP_MARGIN)
        .assured("the bounded storage delay plus the test load margin fits in Duration")
}

fn durable_catch_up_append_stream_opens(
    world: &ScenarioWorld,
    observation: &DurableCatchUpObservation,
) -> u64 {
    let mut opened = 0_u64;
    for (source, baseline) in &observation.append_stream_opens_at_start {
        let total = world.fault_injection.consensus_append_stream_open_count(
            &crate::common::cluster::node_name(source),
            &crate::common::cluster::node_name(&observation.follower),
        );
        let source_opened = total
            .checked_sub(*baseline)
            .verified("the append stream counter only increases after the catch-up baseline");
        // A saturated total still proves that any small scenario maximum was exceeded.
        opened = opened.saturating_add(source_opened);
    }
    opened
}

async fn finish_durable_catch_up_writer(world: &mut ScenarioWorld) -> (String, usize) {
    let writer = world
        .durable_catch_up_writer
        .take()
        .verified("durable catch-up started its continuous client writer");
    writer.cancellation.cancel();
    let prefix = writer.prefix;
    let joined = tokio::time::timeout(Duration::from_secs(5), writer.task)
        .await
        .unwrap_or_else(|error| panic!("durable catch-up client writer did not stop: {error}"));
    let result = joined
        .unwrap_or_else(|error| panic!("durable catch-up client writer task failed: {error}"));
    let written = result
        .unwrap_or_else(|error| panic!("a leader write failed during follower catch-up: {error}"));
    (prefix, written)
}

#[when(
    expr = "node {string} starts catching up while the leader keeps creating domains named \
            {string}"
)]
async fn when_node_starts_durable_catch_up(
    world: &mut ScenarioWorld,
    node_id: String,
    live_prefix: String,
) {
    assert!(
        world.durable_catch_up.is_none(),
        "a durable follower catch-up is already being observed"
    );
    assert!(
        world.durable_catch_up_writer.is_none(),
        "a durable follower catch-up client writer is already active"
    );

    let follower = expand_placeholders(world, &node_id);
    let live_prefix = expand_placeholders(world, &live_prefix);
    let leader = running_leader_node(world).await;
    let commit_delay = *world
        .consensus_commit_delays
        .get(&follower)
        .verified("the scenario configured this follower's consensus commit delay");
    let mut append_stream_opens_at_start = BTreeMap::new();
    for source in world.cluster().node_ids() {
        if source == follower {
            continue;
        }
        let count = world.fault_injection.consensus_append_stream_open_count(
            &crate::common::cluster::node_name(&source),
            &crate::common::cluster::node_name(&follower),
        );
        append_stream_opens_at_start.insert(source, count);
    }
    let leader_uri = world
        .cluster()
        .grpc_uri(&leader)
        .unwrap_or_else(|error| panic!("failed to resolve leader '{leader}': {error}"));
    let connect_options = client_connect_options(&leader_uri)
        .unwrap_or_else(|error| panic!("failed to configure the durable catch-up client: {error}"));
    let client =
        Client::connect_with_options(&leader_uri, client_domain(&world.domain), connect_options)
            .await
            .unwrap_or_else(|error| {
                panic!("failed to open the durable catch-up client session: {error}")
            });
    let started_at = Instant::now();
    let cancellation = CancellationToken::new();
    let writer_cancellation = cancellation.clone();
    let writer_prefix = live_prefix.clone();
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut written = 0_usize;
        loop {
            tokio::task::consume_budget().await;
            if writer_cancellation.is_cancelled() {
                return Ok(written);
            }

            let name = burst_domain_name(&writer_prefix, written);
            let request = client.execute(format!("CREATE DOMAIN {name};"));
            let outcome = tokio::select! {
                () = writer_cancellation.cancelled() => return Ok(written),
                outcome = request => outcome,
            };
            let outcome = outcome.map_err(|error| error.to_string())?;
            if !outcome.succeeded() {
                return Err(format!(
                    "command failed with {:?}: {}; diagnostics: {:?}",
                    outcome.disposition, outcome.message, outcome.diagnostics
                ));
            }
            written = written
                .checked_add(1)
                .assured("the durable catch-up writer has a fixed entry limit");

            if writer_cancellation.is_cancelled() {
                return Ok(written);
            }
            if written >= MAX_DURABLE_CATCH_UP_WRITES {
                return Err(format!(
                    "follower catch-up did not finish while {written} leader writes succeeded"
                ));
            }
            tokio::select! {
                () = writer_cancellation.cancelled() => return Ok(written),
                () = tokio::time::sleep(DURABLE_CATCH_UP_WRITE_CADENCE) => {}
            }
        }
    }));

    // Sampling starts before the follower does, so the whole catch-up is inside the window. The
    // sampler skips scrapes the starting node has not answered yet.
    let metrics_url = world
        .cluster()
        .observability_metrics_url(&follower)
        .unwrap_or_else(|error| {
            panic!("failed to resolve the catch-up follower's metrics endpoint: {error}")
        });
    let memory_cancellation = CancellationToken::new();
    let sampler_cancellation = memory_cancellation.clone();
    let memory_task = AbortOnDropHandle::new(tokio::spawn(
        crate::common::cluster::sample_peak_observability_metric(
            metrics_url,
            "nervix_execution_memory_reserved_bytes".to_string(),
            vec![COMMANDS_MEMORY_LABEL.to_string()],
            sampler_cancellation,
        ),
    ));
    world.follower_commands_memory = Some(FollowerCommandsMemoryObservation {
        node_id: follower.clone(),
        cancellation: memory_cancellation,
        task: memory_task,
    });

    world.durable_catch_up = Some(DurableCatchUpObservation {
        follower: follower.clone(),
        initial_leader: leader,
        commit_delay,
        started_at,
        append_stream_opens_at_start,
    });
    world.durable_catch_up_writer = Some(DurableCatchUpWriter {
        cancellation,
        prefix: live_prefix,
        task,
    });
    world
        .cluster_mut()
        .start_node_without_waiting_for_raft_catch_up(&follower)
        .await
        .unwrap_or_else(|error| panic!("failed to start catch-up follower '{follower}': {error}"));
}

#[then(
    expr = "node {string} applies {int} domains named {string} and the concurrent writes within \
            its durable storage bound using at most {int} append streams"
)]
async fn then_node_catches_up_within_durable_storage_bound(
    world: &mut ScenarioWorld,
    node_id: String,
    backlog_count: usize,
    backlog_prefix: String,
    maximum_append_streams: usize,
) {
    let node_id = expand_placeholders(world, &node_id);
    let backlog_prefix = expand_placeholders(world, &backlog_prefix);
    let observation = world
        .durable_catch_up
        .as_ref()
        .verified("the scenario started a durable follower catch-up")
        .clone();
    assert_eq!(
        node_id, observation.follower,
        "the durable catch-up assertion must observe the follower it started"
    );

    let maximum_entries = backlog_count
        .checked_add(MAX_DURABLE_CATCH_UP_WRITES)
        .assured("the bounded backlog and live-write limit fit in usize");
    let maximum_bound = durable_catch_up_bound(observation.commit_delay, maximum_entries);
    let maximum_deadline = observation
        .started_at
        .checked_add(maximum_bound)
        .assured("the durable catch-up test bound fits the monotonic clock range");

    let backlog_applied = loop {
        tokio::task::consume_budget().await;
        let applied = applied_burst_domains(world, &node_id, &backlog_prefix).await;
        if applied >= backlog_count || Instant::now() >= maximum_deadline {
            break applied;
        }
        tokio::time::sleep(DURABLE_CATCH_UP_WRITE_CADENCE).await;
    };

    let (live_prefix, live_writes) = finish_durable_catch_up_writer(world).await;
    assert!(
        live_writes > 0,
        "the leader must complete a client write while the durable follower catches up"
    );
    let total_entries = backlog_count
        .checked_add(live_writes)
        .assured("the bounded backlog and completed live writes fit in usize");
    let bound = durable_catch_up_bound(observation.commit_delay, total_entries);
    let deadline = observation
        .started_at
        .checked_add(bound)
        .assured("the durable catch-up test bound fits the monotonic clock range");

    let live_applied = loop {
        tokio::task::consume_budget().await;
        let applied = applied_burst_domains(world, &node_id, &live_prefix).await;
        let all_applied = backlog_applied >= backlog_count && applied >= live_writes;
        if all_applied || Instant::now() >= deadline {
            break applied;
        }
        tokio::time::sleep(DURABLE_CATCH_UP_WRITE_CADENCE).await;
    };

    let elapsed = observation.started_at.elapsed();
    let append_stream_opens = durable_catch_up_append_stream_opens(world, &observation);
    let maximum_append_streams = u64::try_from(maximum_append_streams)
        .assured("a Cucumber append stream limit fits in the observation counter");
    assert!(
        append_stream_opens <= maximum_append_streams,
        "leaders opened {append_stream_opens} append streams to catch-up follower '{}'; the \
         initial leader was '{}', the expected maximum was {maximum_append_streams}, and the \
         follower applied {backlog_applied} of {backlog_count} '{}' entries plus {live_applied} \
         of {live_writes} '{}' concurrent writes after {:?} against a {:?} bound",
        observation.follower,
        observation.initial_leader,
        backlog_prefix,
        live_prefix,
        elapsed,
        bound,
    );
    assert!(
        backlog_applied >= backlog_count && live_applied >= live_writes && elapsed <= bound,
        "follower '{}' durable catch-up exceeded {:?}: applied {backlog_applied} of \
         {backlog_count} '{}' entries and {live_applied} of {live_writes} '{}' concurrent writes \
         after {:?} with {:?} per consensus commit; the initial leader was '{}' and leaders \
         opened {append_stream_opens} append streams",
        observation.follower,
        bound,
        backlog_prefix,
        live_prefix,
        elapsed,
        observation.commit_delay,
        observation.initial_leader,
    );
}

#[then(
    expr = "node {string} held its queued append batches inside its commands memory budget while \
            catching up"
)]
async fn then_follower_held_append_batches_inside_its_commands_budget(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let observation = world
        .follower_commands_memory
        .take()
        .verified("the scenario started a follower commands-memory observation");
    assert_eq!(
        node_id, observation.node_id,
        "the memory assertion must observe the follower it sampled"
    );
    observation.cancellation.cancel();
    let peak = observation
        .task
        .await
        .unwrap_or_else(|error| panic!("the follower memory sampler task failed: {error}"));
    let capacity = world
        .cluster()
        .read_observability_metric(
            &node_id,
            "nervix_execution_memory_capacity_bytes",
            &[COMMANDS_MEMORY_LABEL.to_string()],
        )
        .await
        .unwrap_or_else(|error| {
            panic!("failed to read the follower's commands memory budget: {error}")
        });

    assert!(
        peak > 0.0,
        "follower '{node_id}' must charge the append batches it holds while catching up, but its \
         commands class never reported a reservation against its {capacity} byte budget"
    );
    assert!(
        peak < capacity,
        "follower '{node_id}' must hold its queued append batches inside its commands budget: the \
         class peaked at {peak} bytes against a {capacity} byte budget while it caught up"
    );
}

#[then(expr = "within {string} node {string} has applied {int} domains named {string}")]
async fn then_node_has_applied_burst_domains(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    count: usize,
    prefix: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    await_burst_domains(world, &[node_id], &duration, count, &prefix).await;
}

#[then(expr = "within {string} every node has applied {int} domains named {string}")]
async fn then_every_node_has_applied_burst_domains(
    world: &mut ScenarioWorld,
    duration: String,
    count: usize,
    prefix: String,
) {
    let nodes = world.cluster().node_ids();
    await_burst_domains(world, &nodes, &duration, count, &prefix).await;
}

async fn await_burst_domains(
    world: &ScenarioWorld,
    nodes: &[String],
    duration: &str,
    count: usize,
    prefix: &str,
) {
    let deadline = Instant::now()
        + humantime::parse_duration(duration).expect("step duration must be a valid duration");
    for node_id in nodes {
        let applied = loop {
            tokio::task::consume_budget().await;
            let applied = applied_burst_domains(world, node_id, prefix).await;
            if applied >= count || Instant::now() >= deadline {
                break applied;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(
            applied, count,
            "node '{node_id}' applied {applied} of {count} domains named '{prefix}' within \
             {duration}"
        );
    }
}

#[then(expr = "within {string} the leader node has purged its covered raft log")]
async fn then_leader_purged_covered_log(world: &mut ScenarioWorld, duration: String) {
    let leader = running_leader_node(world).await;
    let observer = world
        .fault_injection
        .consensus_observer(&crate::common::cluster::node_name(&leader));
    await_covered_log_purge(&observer, &duration).await;
}

#[then(
    expr = "within {string} the leader node has purged its covered raft log and reports fewer \
            retained bytes"
)]
async fn then_leader_purged_covered_log_and_reduced_retained_bytes(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let leader = running_leader_node(world).await;
    let observer = world
        .fault_injection
        .consensus_observer(&crate::common::cluster::node_name(&leader));
    let retention_peak = world
        .burst_raft_retention_peak
        .take()
        .verified("the preceding domain burst recorded its Raft retention peak");
    await_purge_beyond_retention_peak(&observer, &duration, &retention_peak).await;
}

#[then(
    expr = "within {string} the leader node released its snapshot-section bulk-memory reservation"
)]
async fn then_leader_released_snapshot_bulk_memory(world: &mut ScenarioWorld, duration: String) {
    let leader = running_leader_node(world).await;
    let deadline = Instant::now()
        + humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let section_reservation = nervix_execution::OperationLimits::default()
        .snapshot_section_working_bytes()
        .verified("the default snapshot-section working set fits in u64");
    let section_reservation: f64 = section_reservation.approx_into();
    loop {
        tokio::task::consume_budget().await;
        let reserved = world
            .cluster()
            .read_observability_metric(
                &leader,
                "nervix_execution_memory_reserved_bytes",
                &[BULK_MEMORY_LABEL.to_string()],
            )
            .await
            .unwrap_or_else(|error| {
                panic!("failed to read the leader's bulk-memory reservation: {error}")
            });
        if reserved < section_reservation {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "within {duration} leader '{leader}' still held {reserved} bytes of bulk memory, at \
             least the {section_reservation}-byte working set for one snapshot section"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until the leader has purged past `peak` and reports fewer retained bytes than it did then.
///
/// A purge waits while any follower's replication is in flight. An attempt to open a stream to a
/// stopped follower that is still listed as live stays in flight until its setup deadline, so the
/// purge can trail the burst's last snapshot.
async fn await_purge_beyond_retention_peak(
    observer: &nervix_consensus::Observer,
    duration: &str,
    peak: &nervix_consensus::RaftLogRetention,
) {
    let deadline = Instant::now()
        + humantime::parse_duration(duration).expect("step duration must be a valid duration");
    loop {
        tokio::task::consume_budget().await;
        let retention = observer.raft_log_retention();
        assert!(
            retention.snapshot_index >= retention.purged_index,
            "the leader purged entries its snapshot does not cover: {retention:?}"
        );
        let purged_beyond_peak = retention.purged_index > peak.purged_index;
        let retained_fewer_bytes = retention.retained_bytes < peak.retained_bytes;
        if purged_beyond_peak && retained_fewer_bytes {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "within {duration} the leader did not both purge past its retained-byte peak \
             ({purged_beyond_peak}) and report fewer retained bytes ({retained_fewer_bytes}): \
             peak={peak:?} last={retention:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn await_covered_log_purge(
    observer: &nervix_consensus::Observer,
    duration: &str,
) -> nervix_consensus::RaftLogRetention {
    let deadline = Instant::now()
        + humantime::parse_duration(duration).expect("step duration must be a valid duration");
    loop {
        tokio::task::consume_budget().await;
        let retention = observer.raft_log_retention();
        if retention.purged_index.is_some() {
            assert!(
                retention.snapshot_index >= retention.purged_index,
                "the leader purged entries its snapshot does not cover: {retention:?}"
            );
            return retention;
        }
        assert!(
            Instant::now() < deadline,
            "the leader did not purge its covered raft log within {duration}: {retention:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[when(expr = "leadership is transferred to node {string}")]
async fn when_leadership_is_transferred_to_node(world: &mut ScenarioWorld, to_node_id: String) {
    let to_node_id = expand_placeholders(world, &to_node_id);
    let leader = running_leader_node(world).await;
    world.cluster().transfer_leadership(&leader, &to_node_id);
    world
        .cluster()
        .wait_for_leader(&to_node_id, Some(&to_node_id))
        .await
        .unwrap_or_else(|error| panic!("leadership did not move to '{to_node_id}': {error}"));
}

/// Arm the failure that interrupts the next snapshot installation the node performs.
///
/// The node has to be stopped: its storage, and with it the failure, is built as the node starts
/// and before it answers the Raft traffic that asks it to install a snapshot.
#[given(expr = "node {string} interrupts its next raft snapshot installation")]
async fn given_node_interrupts_its_next_snapshot_installation(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world.fault_injection.fail_consensus_storage_on_start(
        &crate::common::cluster::node_name(&node_id),
        "snapshot_clear".to_owned(),
        nervix_consensus::StorageBoundary::AfterSync,
    );
}

#[when(expr = "node {string} is started without waiting for it to catch up")]
async fn when_node_is_started_without_catching_up(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster_mut()
        .start_node_without_catching_up(&node_id)
        .await
        .expect("failed to start node");
}

#[then(expr = "within {string} node {string} has interrupted a raft snapshot installation")]
async fn then_node_interrupted_a_snapshot_installation(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let node = crate::common::cluster::node_name(&node_id);
    let deadline = Instant::now()
        + humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    loop {
        tokio::task::consume_budget().await;
        if world.fault_injection.consensus_storage_failure_fired(&node) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node '{node_id}' did not interrupt a raft snapshot installation within {duration}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "within {string} node {string} recovers by installing a raft snapshot")]
async fn then_node_recovers_by_snapshot(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let deadline = Instant::now()
        + humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    loop {
        tokio::task::consume_budget().await;
        let observer = world
            .fault_injection
            .consensus_observer(&crate::common::cluster::node_name(&node_id));
        let retention = observer.raft_log_retention();
        if retention.snapshot_index.is_some() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node '{node_id}' did not install a raft snapshot within {duration}: {retention:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[given(expr = "a {int} node nervix cluster is started")]
async fn given_cluster_is_started(world: &mut ScenarioWorld, node_count: usize) {
    assert!(world.cluster.is_none(), "cluster is already started");
    initialize_scenario_identity(world);
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    world.last_subscription_payload = None;
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.broker_observer = None;
    world.last_broker_payload = None;
    world.last_broker_headers.clear();
    world.burst_raft_retention_peak = None;
    append_cucumber_log_line(&format!(
        "cluster start requested: nodes={node_count} domain={} test_id={}",
        world.domain, world.test_id
    ));
    match Cluster::start_with_config(
        node_count,
        world.fault_injection.clone(),
        world.cluster_config.clone(),
        world.scenario_identity(),
    )
    .await
    {
        Ok(cluster) => {
            world.cluster = Some(cluster);
        }
        Err(error) => {
            append_cucumber_log_line(&format!("cluster start failed: {error}"));
            panic!("failed to start cluster: {error}");
        }
    }
}

#[given(
    expr = "runtime replication is configured with replica count {int} and snapshot interval \
            {string}"
)]
async fn given_runtime_replication_is_configured(
    world: &mut ScenarioWorld,
    replica_count: usize,
    snapshot_interval: String,
) {
    assert!(
        world.cluster.is_none(),
        "replication must be configured before cluster startup"
    );
    world.cluster_config.replica_count = replica_count;
    world.cluster_config.state_snapshot_interval = humantime::parse_duration(&snapshot_interval)
        .expect("snapshot interval must be a valid duration");
}

#[given(expr = "raft election timeout is configured from {string} to {string}")]
fn given_raft_election_timeout_is_configured(
    world: &mut ScenarioWorld,
    minimum: String,
    maximum: String,
) {
    assert!(
        world.cluster.is_none(),
        "raft election timeout must be configured before cluster startup"
    );
    let minimum = humantime::parse_duration(&minimum)
        .assured("the Cucumber scenario supplies a valid minimum election timeout");
    let maximum = humantime::parse_duration(&maximum)
        .assured("the Cucumber scenario supplies a valid maximum election timeout");
    assert!(
        minimum <= maximum,
        "minimum raft election timeout must not exceed its maximum"
    );
    world.cluster_config.raft_election_timeout_min = minimum;
    world.cluster_config.raft_election_timeout_max = maximum;
}

#[given("runtime state replica polling is paused")]
async fn given_runtime_state_replica_polling_is_paused(world: &mut ScenarioWorld) {
    assert!(
        world.cluster.is_none(),
        "replica polling must be paused before cluster startup"
    );
    world.fault_injection.pause_state_replica_polling();
}

#[when("WASM guest-state checkpoints fail to reach stable storage on every node")]
async fn when_wasm_checkpoints_fail_to_reach_stable_storage(world: &mut ScenarioWorld) {
    world.fault_injection.fail_wasm_checkpoint_storage();
}

#[when("WASM guest-state checkpoints reach stable storage again on every node")]
async fn when_wasm_checkpoints_reach_stable_storage_again(world: &mut ScenarioWorld) {
    world.fault_injection.restore_wasm_checkpoint_storage();
}

/// The checkpoint window of one WASM processor in the scenario's domain that a pause selects.
struct WasmCheckpointPauseTarget {
    domain: nervix_models::DomainName,
    processor: nervix_models::ModelName,
    window: nervix_server::WasmCheckpointWindow,
}

impl WasmCheckpointPauseTarget {
    fn of(world: &ScenarioWorld, processor: &str, window: &str) -> Self {
        let domain = nervix_models::DomainName::try_from(world.domain.as_str())
            .expect("the scenario domain must be valid");
        let processor = nervix_models::ModelName::try_from(processor)
            .expect("the scenario WASM processor name must be valid");
        let window = window
            .parse::<nervix_server::WasmCheckpointWindow>()
            .unwrap_or_else(|_| panic!("unknown WASM checkpoint window '{window}'"));
        Self {
            domain,
            processor,
            window,
        }
    }
}

#[given(expr = "the next guest-state checkpoint of WASM processor {string} pauses {word}")]
#[when(expr = "the next guest-state checkpoint of WASM processor {string} pauses {word}")]
async fn when_next_wasm_checkpoint_pauses(
    world: &mut ScenarioWorld,
    processor: String,
    window: String,
) {
    let target = WasmCheckpointPauseTarget::of(world, &processor, &window);
    world
        .fault_injection
        .pause_wasm_checkpoint(target.domain, target.processor, target.window);
}

#[then(expr = "a guest-state checkpoint of WASM processor {string} is held {word}")]
async fn then_wasm_checkpoint_is_held(
    world: &mut ScenarioWorld,
    processor: String,
    window: String,
) {
    let target = WasmCheckpointPauseTarget::of(world, &processor, &window);
    let reached = tokio::time::timeout(
        Duration::from_secs(30),
        world.fault_injection.wait_for_wasm_checkpoint_pause(
            target.domain,
            target.processor.clone(),
            target.window,
        ),
    )
    .await;
    assert!(
        reached.is_ok(),
        "no guest-state checkpoint of WASM processor '{}' reached the armed {} window within \
         thirty seconds",
        target.processor.as_str(),
        target.window.as_ref()
    );
}

#[when("every held WASM guest-state checkpoint is released")]
async fn when_every_held_wasm_checkpoint_is_released(world: &mut ScenarioWorld) {
    world.fault_injection.release_all_wasm_checkpoint_pauses();
}

#[when("fresh WASM reset guest initialization fails on every node")]
async fn when_fresh_wasm_reset_guest_initialization_fails(world: &mut ScenarioWorld) {
    world
        .fault_injection
        .fail_wasm_state_reset_fresh_initialization();
}

#[when("fresh WASM reset guest initialization succeeds again on every node")]
async fn when_fresh_wasm_reset_guest_initialization_succeeds_again(world: &mut ScenarioWorld) {
    world
        .fault_injection
        .restore_wasm_state_reset_fresh_initialization();
}

fn wasm_state_reset_reference(
    world: &mut ScenarioWorld,
) -> nervix_models::CommandExecutionReference {
    world
        .wasm_state_reset_reference
        .get_or_insert_with(|| {
            nervix_models::CommandExecutionReference::parse(format!(
                "wasm-state-reset-{}",
                uuid::Uuid::now_v7()
            ))
            .assured("the generated UUID uses only execution-reference characters")
        })
        .clone()
}

/// The prefix every guest-requested reset is coordinated under, which is what tells a guest's
/// request apart from an operator's in the committed schedule.
const GUEST_WASM_STATE_RESET_PREFIX: &str = "wasm-guest-reset.";

/// Whether the committed schedule reports a completed guest-requested reset for `processor`.
async fn completed_guest_wasm_state_reset(world: &ScenarioWorld, processor: &str) -> bool {
    let leader = running_leader_node(world).await;
    let observer = world
        .fault_injection
        .consensus_observer(&crate::common::cluster::node_name(&leader));
    let schedule = observer.current_schedule().await;
    let domain = nervix_models::DomainName::try_from(world.domain.as_str())
        .expect("the scenario domain name must be valid");
    let entity = nervix_models::NodeRef::new(
        nervix_models::ModelKind::WasmProcessor,
        nervix_models::ModelName::try_from(processor).expect("the processor name must be valid"),
    );
    let Some(domain_schedule) = schedule.domains.get(&domain) else {
        return false;
    };
    let Some(node) = domain_schedule.nodes.get(&entity) else {
        return false;
    };
    let Some(reset) = node.wasm_state_reset() else {
        return false;
    };
    reset
        .request()
        .as_str()
        .starts_with(GUEST_WASM_STATE_RESET_PREFIX)
        && reset.phase() == nervix_models::WasmStateResetPhase::Ready
}

#[then(expr = "within {string} WASM processor {string} completes a guest-requested state reset")]
async fn then_wasm_processor_completes_a_guest_requested_state_reset(
    world: &mut ScenarioWorld,
    timeout: String,
    processor: String,
) {
    let timeout =
        humantime::parse_duration(&timeout).expect("step duration must be a valid duration");
    let deadline = Instant::now() + timeout;
    loop {
        tokio::task::consume_budget().await;
        if completed_guest_wasm_state_reset(world, &processor).await {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "wasm processor '{processor}' did not complete a guest-requested state reset within \
             {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn wasm_state_reset_branch(step: &Step) -> Vec<nervix_models::RemoteRuntimeField> {
    let value = serde_json::from_str::<serde_json::Value>(docstring(step))
        .expect("WASM reset branch must be a JSON object");
    let object = value
        .as_object()
        .expect("WASM reset branch must be a JSON object");
    object
        .iter()
        .map(|(name, value)| {
            let value = match value {
                serde_json::Value::String(value) => {
                    nervix_models::RemoteRuntimeValue::String(value.clone())
                }
                serde_json::Value::Bool(value) => nervix_models::RemoteRuntimeValue::Bool(*value),
                serde_json::Value::Number(value) if value.as_i64().is_some() => {
                    nervix_models::RemoteRuntimeValue::I64(
                        value
                            .as_i64()
                            .verified("this match arm already established an i64 value"),
                    )
                }
                serde_json::Value::Number(value) if value.as_u64().is_some() => {
                    nervix_models::RemoteRuntimeValue::U64(
                        value
                            .as_u64()
                            .verified("this match arm already established a u64 value"),
                    )
                }
                serde_json::Value::Number(value) if value.as_f64().is_some() => {
                    nervix_models::RemoteRuntimeValue::F64(
                        value
                            .as_f64()
                            .verified("this match arm already established an f64 value"),
                    )
                }
                _ => panic!("WASM reset branch fields must be scalar JSON values"),
            };
            nervix_models::RemoteRuntimeField {
                name: name.clone(),
                value,
            }
        })
        .collect()
}

async fn reset_wasm_processor_branch(
    world: &mut ScenarioWorld,
    processor: String,
    branch: Vec<nervix_models::RemoteRuntimeField>,
) -> error_stack::Result<(), WasmStateResetRequestError> {
    let reference = wasm_state_reset_reference(world);
    reset_wasm_processor_branch_with_reference(world, processor, reference, branch).await
}

async fn reset_wasm_processor_branch_with_reference(
    world: &mut ScenarioWorld,
    processor: String,
    reference: nervix_models::CommandExecutionReference,
    branch: Vec<nervix_models::RemoteRuntimeField>,
) -> error_stack::Result<(), WasmStateResetRequestError> {
    let leader = current_leader_node(world).await;
    reset_wasm_processor_branch_through_node(world, leader, processor, reference, branch).await
}

async fn reset_wasm_processor_branch_through_node(
    world: &mut ScenarioWorld,
    node: String,
    processor: String,
    reference: nervix_models::CommandExecutionReference,
    branch: Vec<nervix_models::RemoteRuntimeField>,
) -> error_stack::Result<(), WasmStateResetRequestError> {
    let domain = nervix_models::DomainName::try_from(world.domain.as_str())
        .expect("the scenario domain must be valid");
    let processor = nervix_models::ModelName::try_from(processor.as_str())
        .expect("the scenario WASM processor name must be valid");
    world
        .fault_injection
        .reset_wasm_processor_branch(
            &crate::common::cluster::node_name(&node),
            domain,
            processor,
            reference,
            branch,
        )
        .await
}

#[when(expr = "WASM processor {string} state is reset for branch")]
async fn when_wasm_processor_state_is_reset_for_branch(
    world: &mut ScenarioWorld,
    processor: String,
    step: &Step,
) {
    let branch = wasm_state_reset_branch(step);
    reset_wasm_processor_branch(world, processor, branch)
        .await
        .unwrap_or_else(|error| panic!("WASM processor state reset failed: {error}"));
}

#[when(expr = "WASM processor {string} state reset for branch fails")]
async fn when_wasm_processor_state_reset_for_branch_fails(
    world: &mut ScenarioWorld,
    processor: String,
    step: &Step,
) {
    let branch = wasm_state_reset_branch(step);
    let error = reset_wasm_processor_branch(world, processor, branch)
        .await
        .expect_err("the armed WASM processor state reset must fail");
    world.last_command_error = Some(format!("{error:#}"));
}

#[when(expr = "WASM processor {string} state is reset through node {string} for branch")]
async fn when_wasm_processor_state_is_reset_through_node_for_branch(
    world: &mut ScenarioWorld,
    processor: String,
    node: String,
    step: &Step,
) {
    let node = expand_placeholders(world, &node);
    let reference = wasm_state_reset_reference(world);
    let branch = wasm_state_reset_branch(step);
    reset_wasm_processor_branch_through_node(world, node, processor, reference, branch)
        .await
        .unwrap_or_else(|error| panic!("WASM processor state reset failed: {error:#}"));
}

#[when(expr = "WASM processor {string} state reset through node {string} for branch fails")]
async fn when_wasm_processor_state_reset_through_node_for_branch_fails(
    world: &mut ScenarioWorld,
    processor: String,
    node: String,
    step: &Step,
) {
    let node = expand_placeholders(world, &node);
    let reference = wasm_state_reset_reference(world);
    let branch = wasm_state_reset_branch(step);
    let error = reset_wasm_processor_branch_through_node(world, node, processor, reference, branch)
        .await
        .expect_err("the WASM processor state reset through the selected node must fail");
    world.last_command_error = Some(format!("{error:#}"));
}

#[when(expr = "WASM processor {string} state reset for branch with a different request fails")]
async fn when_wasm_processor_state_reset_for_branch_with_a_different_request_fails(
    world: &mut ScenarioWorld,
    processor: String,
    step: &Step,
) {
    let reference = nervix_models::CommandExecutionReference::parse(format!(
        "different-wasm-state-reset-{}",
        uuid::Uuid::now_v7()
    ))
    .assured("the generated UUID uses only execution-reference characters");
    let branch = wasm_state_reset_branch(step);
    let error = reset_wasm_processor_branch_with_reference(world, processor, reference, branch)
        .await
        .expect_err("a different request must conflict with the reset already publishing");
    world.last_command_error = Some(format!("{error:#}"));
}

#[when(expr = "WASM processor {string} state is reset for all branches")]
async fn when_wasm_processor_state_is_reset_for_all_branches(
    world: &mut ScenarioWorld,
    processor: String,
) {
    let reference = wasm_state_reset_reference(world);
    let leader = current_leader_node(world).await;
    let domain = nervix_models::DomainName::try_from(world.domain.as_str())
        .expect("the scenario domain must be valid");
    let processor = nervix_models::ModelName::try_from(processor.as_str())
        .expect("the scenario WASM processor name must be valid");
    world
        .fault_injection
        .reset_all_wasm_processor_branches(
            &crate::common::cluster::node_name(&leader),
            domain,
            processor,
            reference,
        )
        .await
        .unwrap_or_else(|error| panic!("WASM processor state reset failed: {error:#}"));
}

#[when(expr = "WASM processor {string} unbranched state is reset")]
async fn when_unbranched_wasm_processor_state_is_reset(
    world: &mut ScenarioWorld,
    processor: String,
) {
    let reference = wasm_state_reset_reference(world);
    let leader = current_leader_node(world).await;
    let domain = nervix_models::DomainName::try_from(world.domain.as_str())
        .expect("the scenario domain must be valid");
    let processor = nervix_models::ModelName::try_from(processor.as_str())
        .expect("the scenario WASM processor name must be valid");
    world
        .fault_injection
        .reset_unbranched_wasm_processor(
            &crate::common::cluster::node_name(&leader),
            domain,
            processor,
            reference,
        )
        .await
        .unwrap_or_else(|error| panic!("WASM processor state reset failed: {error:#}"));
}

#[given("a branched state-counting WASM reset graph is running")]
async fn given_branched_state_counting_wasm_reset_graph_is_running(world: &mut ScenarioWorld) {
    configure_wasm_state_reset_graph(world, WasmStateResetGraph::branched()).await;
}

#[given("a branched state-counting WASM reset graph is running in the existing domain")]
async fn given_branched_state_counting_wasm_reset_graph_is_running_in_the_existing_domain(
    world: &mut ScenarioWorld,
) {
    configure_wasm_state_reset_graph(
        world,
        WasmStateResetGraph {
            create_domain: false,
            ..WasmStateResetGraph::branched()
        },
    )
    .await;
}

#[given("a node-1-owned branched state-counting WASM reset graph is running")]
async fn given_node_one_owned_branched_state_counting_wasm_reset_graph_is_running(
    world: &mut ScenarioWorld,
) {
    configure_wasm_state_reset_graph(
        world,
        WasmStateResetGraph {
            placement: WasmStateResetGraphPlacement::NodeOne,
            ..WasmStateResetGraph::branched()
        },
    )
    .await;
}

#[given("a non-node-1-owned branched state-counting WASM reset graph is running")]
async fn given_non_node_one_owned_branched_state_counting_wasm_reset_graph_is_running(
    world: &mut ScenarioWorld,
) {
    configure_wasm_state_reset_graph(
        world,
        WasmStateResetGraph {
            placement: WasmStateResetGraphPlacement::AwayFromNodeOne,
            ..WasmStateResetGraph::branched()
        },
    )
    .await;
}

#[given("an unbranched state-counting WASM reset graph is running")]
async fn given_unbranched_state_counting_wasm_reset_graph_is_running(world: &mut ScenarioWorld) {
    configure_wasm_state_reset_graph(
        world,
        WasmStateResetGraph {
            branched: false,
            ..WasmStateResetGraph::branched()
        },
    )
    .await;
}

#[given("a branched timeout-buffering WASM reset graph is running")]
async fn given_branched_timeout_buffering_wasm_reset_graph_is_running(world: &mut ScenarioWorld) {
    configure_wasm_state_reset_graph(
        world,
        WasmStateResetGraph {
            timeout_buffering: true,
            ..WasmStateResetGraph::branched()
        },
    )
    .await;
}

#[given(
    "a branched state-counting WASM reset graph with a second usage of its resource is running"
)]
async fn given_branched_state_counting_wasm_reset_graph_with_a_second_usage_is_running(
    world: &mut ScenarioWorld,
) {
    configure_wasm_state_reset_graph(
        world,
        WasmStateResetGraph {
            second_wasm_usage: true,
            ..WasmStateResetGraph::branched()
        },
    )
    .await;
}

#[given("an unbranched timeout-buffering WASM reset graph is running")]
async fn given_unbranched_timeout_buffering_wasm_reset_graph_is_running(world: &mut ScenarioWorld) {
    configure_wasm_state_reset_graph(
        world,
        WasmStateResetGraph {
            branched: false,
            timeout_buffering: true,
            ..WasmStateResetGraph::branched()
        },
    )
    .await;
}

#[derive(Clone, Copy)]
enum WasmStateResetGraphPlacement {
    Unconstrained,
    NodeOne,
    AwayFromNodeOne,
}

/// The shape of the guest-state graph a WASM reset or rebind scenario runs on.
#[derive(Clone, Copy)]
struct WasmStateResetGraph {
    branched: bool,
    timeout_buffering: bool,
    placement: WasmStateResetGraphPlacement,
    create_domain: bool,
    /// Also bind the processor's resource version from a second WASM processor, so a rebinding of
    /// that resource moves more than one usage in one batch. The second processor filters every
    /// row away, so it holds no guest state of its own.
    second_wasm_usage: bool,
}

impl WasmStateResetGraph {
    fn branched() -> Self {
        Self {
            branched: true,
            timeout_buffering: false,
            placement: WasmStateResetGraphPlacement::Unconstrained,
            create_domain: true,
            second_wasm_usage: false,
        }
    }
}

async fn configure_wasm_state_reset_graph(world: &mut ScenarioWorld, graph: WasmStateResetGraph) {
    let WasmStateResetGraph {
        branched,
        timeout_buffering,
        placement,
        create_domain,
        second_wasm_usage,
    } = graph;
    let leader = current_leader_node(world).await;
    if create_domain {
        let domain_commands = format!("CREATE UNPACED DOMAIN {};", world.domain);
        execute_nspl_commands_on_node(world, &leader, &domain_commands)
            .await
            .expect("the WASM reset scenario domain must be created");
    }

    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .expect("failed to resolve leader gRPC URI");
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(&world.domain),
        client_connect_options(&grpc_uri).expect("failed to build client TLS options"),
    )
    .await
    .expect("failed to connect the WASM reset resource client");
    let is_clustered = world.cluster().grpc_uri("node-2").is_ok();
    let placement_commands = match placement {
        WasmStateResetGraphPlacement::NodeOne if is_clustered => {
            vec!["CORDON NODE node-2;", "CORDON NODE node-3;"]
        }
        WasmStateResetGraphPlacement::AwayFromNodeOne if is_clustered => {
            vec!["CORDON NODE node-1;"]
        }
        _ => Vec::new(),
    };
    if !placement_commands.is_empty() {
        for command in &placement_commands {
            let outcome = client
                .execute((*command).to_string())
                .await
                .expect("WASM reset placement command must complete");
            assert!(
                outcome.succeeded(),
                "WASM reset placement command must succeed: {command}: {}",
                outcome.message
            );
        }
    }
    let resource_commands = expand_placeholders(
        world,
        "CREATE RESOURCE wasm_reset_guest;\nUPLOAD RESOURCE wasm_reset_guest VERSION \
         '{{wasm_processor}}';",
    );
    for command in nspl_statements(&resource_commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .expect("WASM reset resource command must complete");
        assert!(
            outcome.succeeded(),
            "WASM reset resource command must succeed: {command}: {}",
            outcome.message
        );
    }

    let output_relay = if timeout_buffering {
        "released_events"
    } else {
        "counted_events"
    };
    let subscription = if timeout_buffering {
        "released_events_subscription"
    } else {
        "counted_events_subscription"
    };
    let output_note = if timeout_buffering {
        "released"
    } else {
        "even"
    };
    let branch_schema = if branched {
        "CREATE SCHEMA tenant_branch ( tenant STRING );\nCREATE BRANCH by_tenant SCHEMA \
         tenant_branch TTL 5m;"
    } else {
        ""
    };
    let relay_branching = if branched {
        " BRANCHED BY by_tenant"
    } else {
        " UNBRANCHED"
    };
    let ingestor_branching = if branched {
        "BRANCHED BY by_tenant\n        SET tenant = message.tenant"
    } else {
        "UNBRANCHED"
    };
    let processor_branching = if branched {
        "BRANCHED BY by_tenant"
    } else {
        "UNBRANCHED"
    };
    let tenant_expression = if branched {
        "branch.tenant"
    } else {
        "\"unbranched\""
    };
    let second_usage_commands = if second_wasm_usage {
        format!(
            r#"
        CREATE RELAY counted_secondary_events SCHEMA counted_output_event{relay_branching};
        CREATE WASM PROCESSOR secondary_guest FROM counted_input_events
          FILTER WHERE input.sequence < 0 AS I32
          USING RESOURCE wasm_reset_guest VERSION 1
          FILE 'processors/filter_even.wasm'
          MAX FUEL 1000000000
          MAX MEMORY 64MiB
          {processor_branching}
          TO counted_secondary_events
          SET tenant = {tenant_expression},
              note = coalesce(note, "{output_note}")
          ON MESSAGE ERROR LOG
          ON GLOBAL ERROR LOG;
        "#
        )
    } else {
        String::new()
    };
    let commands = format!(
        r#"
        CREATE SCHEMA counted_input_event ( tenant STRING, sequence I32 );
        CREATE SCHEMA counted_output_event ( tenant STRING OPTIONAL, note STRING OPTIONAL );
        CREATE WIRE JSON SCHEMA counted_input_wire MODE STRICT ( tenant string, sequence integer );
        CREATE CODEC counted_input_codec FROM WIRE JSON SCHEMA counted_input_wire TO SCHEMA counted_input_event;
        {branch_schema}
        CREATE RELAY counted_input_events SCHEMA counted_input_event{relay_branching};
        CREATE RELAY {output_relay} SCHEMA counted_output_event{relay_branching};
        CREATE VHOST edge wasm-reset-{{{{test_id}}}}.example.com;
        CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
        CREATE INGESTOR counted_source
          FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
          ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING counted_input_codec
          TO counted_input_events
          INHERIT ALL
          {ingestor_branching}
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
          ON GENERAL ERROR LOG;
        CREATE WASM PROCESSOR counting_guest FROM counted_input_events
          USING RESOURCE wasm_reset_guest VERSION 1
          FILE 'processors/filter_even.wasm'
          MAX FUEL 1000000000
          MAX MEMORY 64MiB
          {processor_branching}
          TO {output_relay}
          SET tenant = {tenant_expression},
              note = coalesce(note, "{output_note}")
          ON MESSAGE ERROR LOG
          ON GLOBAL ERROR LOG;
        {second_usage_commands}
        CREATE SUBSCRIPTION {subscription} TO {output_relay};
        START;
        "#
    );
    let commands = expand_placeholders(world, &commands);
    let session = execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .expect("the WASM reset graph must start");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = true;

    if !placement_commands.is_empty() {
        for command in placement_commands {
            let command = command.replace("CORDON", "UNCORDON");
            let outcome = client
                .execute(command.clone())
                .await
                .expect("WASM reset placement command must complete");
            assert!(
                outcome.succeeded(),
                "WASM reset placement command must succeed: {command}: {}",
                outcome.message
            );
        }
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("the scenario placement deadline must fit in the monotonic clock");
        loop {
            tokio::task::consume_budget().await;
            let outcome = client
                .execute("SHOW CLUSTER STATUS;".to_string())
                .await
                .expect("WASM reset placement status must complete");
            assert!(
                outcome.succeeded(),
                "WASM reset placement status must succeed: {}",
                outcome.message
            );
            let placed = scheduled_node_placement_from_status(
                &outcome.message,
                &world.domain,
                "wasm_processor",
                "counting_guest",
            )
            .is_some_and(|(owner, replicas)| match placement {
                WasmStateResetGraphPlacement::NodeOne => owner == "node-1",
                WasmStateResetGraphPlacement::AwayFromNodeOne => {
                    owner != "node-1" && !replicas.is_empty()
                }
                WasmStateResetGraphPlacement::Unconstrained => true,
            });
            if placed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "WASM reset processor did not reach its requested test placement: {}",
                outcome.message
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[when("runtime state replica installations fail on every node")]
async fn when_runtime_state_replica_installations_fail(world: &mut ScenarioWorld) {
    world.fault_injection.fail_state_replica_installation();
}

#[when("runtime state replica installations succeed again on every node")]
async fn when_runtime_state_replica_installations_succeed_again(world: &mut ScenarioWorld) {
    world.fault_injection.restore_state_replica_installation();
}

#[given(expr = "the transaction idle timeout is configured as {string}")]
async fn given_transaction_idle_timeout_is_configured(world: &mut ScenarioWorld, timeout: String) {
    assert!(
        world.cluster.is_none(),
        "transaction idle timeout must be configured before cluster startup"
    );
    world.cluster_config.transaction_idle_timeout =
        humantime::parse_duration(&timeout).expect("transaction idle timeout must be valid");
}

#[given(expr = "the transaction tombstone retention is configured as {string}")]
async fn given_transaction_tombstone_retention_is_configured(
    world: &mut ScenarioWorld,
    retention: String,
) {
    assert!(
        world.cluster.is_none(),
        "transaction tombstone retention must be configured before cluster startup"
    );
    world.cluster_config.transaction_tombstone_retention = humantime::parse_duration(&retention)
        .expect("transaction tombstone retention must be valid");
}

#[given(expr = "command retry identities are valid for {string}")]
async fn given_command_retry_identities_are_valid_for(world: &mut ScenarioWorld, validity: String) {
    assert!(
        world.cluster.is_none(),
        "command retry validity must be configured before cluster startup"
    );
    world.cluster_config.command_retry_validity = humantime::parse_duration(&validity)
        .assured("the configured command retry validity is a duration");
}

#[given(expr = "the command execution capacity is configured as {int}")]
async fn given_command_execution_capacity_is_configured(
    world: &mut ScenarioWorld,
    capacity: usize,
) {
    assert!(
        world.cluster.is_none(),
        "command execution capacity must be configured before cluster startup"
    );
    world.cluster_config.command_execution_capacity = capacity;
}

#[given(expr = "the transaction statement limit is configured as {int}")]
async fn given_transaction_statement_limit_is_configured(world: &mut ScenarioWorld, limit: usize) {
    assert!(
        world.cluster.is_none(),
        "transaction statement limit must be configured before cluster startup"
    );
    world.cluster_config.transaction_max_statements = limit;
}

#[given(expr = "the transaction source byte limit is configured as {int}")]
async fn given_transaction_source_byte_limit_is_configured(
    world: &mut ScenarioWorld,
    limit: usize,
) {
    assert!(
        world.cluster.is_none(),
        "transaction source byte limit must be configured before cluster startup"
    );
    world.cluster_config.transaction_max_source_bytes = limit.arch_into();
}

#[given(expr = "the concurrent transaction limit is configured as {int}")]
async fn given_concurrent_transaction_limit_is_configured(world: &mut ScenarioWorld, limit: usize) {
    assert!(
        world.cluster.is_none(),
        "concurrent transaction limit must be configured before cluster startup"
    );
    world.cluster_config.transaction_max_open = limit;
}

#[cfg(feature = "testing")]
#[given("the production sticky scheduler is configured")]
async fn given_production_sticky_scheduler_is_configured(world: &mut ScenarioWorld) {
    assert!(
        world.cluster.is_none(),
        "the scheduler must be configured before cluster startup"
    );
    world.cluster_config.scheduler_mode = Some(SchedulerMode::Sticky);
}

#[given("temporary files use a custom temp directory")]
async fn given_temporary_files_use_custom_temp_directory(world: &mut ScenarioWorld) {
    assert!(
        world.cluster.is_none(),
        "temporary file directory must be configured before cluster startup"
    );
    let temp_root = tempfile::Builder::new()
        .prefix("nervix-temp-")
        .tempdir()
        .expect("failed to create temp root");
    world.cluster_config.temp_dir = Some(temp_root.path().to_path_buf());
    world.temp_root = Some(temp_root);
}

#[given(
    expr = "memory pressure is configured with high watermark {string} and low watermark {string}"
)]
async fn given_memory_pressure_is_configured(
    world: &mut ScenarioWorld,
    high_watermark: String,
    low_watermark: String,
) {
    assert!(
        world.cluster.is_none(),
        "memory pressure must be configured before cluster startup"
    );
    let config = MemoryPressureConfig::builder()
        .high_watermark(
            high_watermark
                .parse::<ubyte::ByteUnit>()
                .expect("high watermark must be valid bytes"),
        )
        .low_watermark(
            low_watermark
                .parse::<ubyte::ByteUnit>()
                .expect("low watermark must be valid bytes"),
        )
        .check_interval(Duration::from_millis(50))
        .resume_jitter(Duration::from_millis(10))
        .build();
    config
        .validate()
        .expect("memory pressure watermarks must be valid");
    world.cluster_config.memory_pressure = Some(config);
}

#[given(expr = "drain timeout is configured as {string}")]
async fn given_drain_timeout_is_configured(world: &mut ScenarioWorld, timeout: String) {
    assert!(
        world.cluster.is_none(),
        "drain timeout must be configured before cluster startup"
    );
    world.cluster_config.drain_timeout =
        humantime::parse_duration(&timeout).expect("shutdown drain timeout must be valid");
}

#[given(expr = "shutdown timeout is configured as {string}")]
async fn given_shutdown_timeout_is_configured(world: &mut ScenarioWorld, timeout: String) {
    assert!(
        world.cluster.is_none(),
        "shutdown timeout must be configured before cluster startup"
    );
    world.cluster_config.shutdown_timeout =
        humantime::parse_duration(&timeout).expect("shutdown timeout must be valid");
}

#[given(expr = "schema change drain timeout is configured as {string}")]
async fn given_schema_change_drain_timeout_is_configured(
    world: &mut ScenarioWorld,
    timeout: String,
) {
    assert!(
        world.cluster.is_none(),
        "schema change drain timeout must be configured before cluster startup"
    );
    world.fault_injection.set_domain_drain_timeout(
        humantime::parse_duration(&timeout).expect("schema drain timeout must be valid"),
    );
}

#[given(expr = "entity gate deadline is configured as {string}")]
async fn given_entity_gate_deadline_is_configured(world: &mut ScenarioWorld, timeout: String) {
    assert!(
        world.cluster.is_none(),
        "entity gate deadline must be configured before cluster startup"
    );
    world.fault_injection.set_entity_gate_deadline(
        humantime::parse_duration(&timeout).expect("entity gate deadline must be valid"),
    );
}

#[given(expr = "the next pending entity drain in domain {string} is forced to time out")]
async fn given_next_pending_entity_drain_is_forced_to_time_out(
    world: &mut ScenarioWorld,
    domain: String,
) {
    let domain = expand_placeholders(world, &domain);
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    world
        .fault_injection
        .force_next_entity_drain_timeout(domain);
}

#[given(expr = "the next entity gate engagement in domain {string} is rejected")]
async fn given_next_entity_gate_engagement_is_rejected(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    world
        .fault_injection
        .fail_next_entity_gate_engagement(domain);
}

#[given(expr = "the next domain drain in domain {string} is forced to time out")]
async fn given_next_domain_drain_is_forced_to_time_out(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    world
        .fault_injection
        .force_next_domain_drain_timeout(domain);
}

#[given(expr = "the next entity schedule swap in domain {string} on the leader is forced to fail")]
async fn given_next_entity_schedule_swap_on_leader_fails(
    world: &mut ScenarioWorld,
    domain: String,
) {
    let leader = current_leader_node(world).await;
    let domain = expand_placeholders(world, &domain);
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    world
        .fault_injection
        .fail_next_entity_schedule_swap_on(crate::common::cluster::node_name(&leader), domain);
}

#[given("graceful shutdown drain is enabled")]
async fn given_graceful_shutdown_drain_is_enabled(world: &mut ScenarioWorld) {
    assert!(
        world.cluster.is_none(),
        "graceful shutdown drain must be configured before cluster startup"
    );
    world.cluster_config.graceful_shutdown_drain = true;
}

#[given(expr = "client grpc transport is configured with mode {string}")]
async fn given_client_grpc_transport_is_configured(world: &mut ScenarioWorld, grpc_mode: String) {
    assert!(
        world.cluster.is_none(),
        "grpc transport mode must be configured before cluster startup"
    );
    world.cluster_config.grpc_mode = parse_internal_transport_mode(&grpc_mode);
}

fn parse_internal_transport_mode(value: &str) -> InternalTransportMode {
    match value {
        "http" => InternalTransportMode::Http,
        "https" => InternalTransportMode::Https,
        other => panic!("unsupported internal transport mode '{other}'"),
    }
}

#[given(expr = "branched relay expiration scan interval is configured as {string}")]
async fn given_branched_relay_expiration_scan_interval_is_configured(
    world: &mut ScenarioWorld,
    scan_interval: String,
) {
    assert!(
        world.cluster.is_none(),
        "expiration must be configured before cluster startup"
    );
    world
        .fault_injection
        .set_branch_instance_expiration_scan_interval(
            humantime::parse_duration(&scan_interval)
                .expect("scan interval must be a valid duration"),
        );
}

#[given(expr = "node {string} has resource directory {string} containing")]
async fn given_node_has_resource_directory_containing(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
    #[step] step: &Step,
) {
    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("old fixture directory should be removed");
    }
    std::fs::create_dir_all(&resource_dir).expect("fixture directory should be created");

    let files: BTreeMap<String, String> =
        serde_json::from_str(docstring(step)).expect("fixture docstring must be valid JSON");
    for (relative_path, contents) in files {
        let destination = resource_dir.join(PathBuf::from(relative_path));
        let parent = destination
            .parent()
            .expect("fixture file must have a parent directory");
        std::fs::create_dir_all(parent).expect("fixture parent directory should be created");
        std::fs::write(destination, contents).expect("fixture file should be written");
    }

    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

#[given(expr = "resource directory {string} additionally contains")]
async fn given_resource_directory_additionally_contains(
    world: &mut ScenarioWorld,
    placeholder: String,
    #[step] step: &Step,
) {
    let resource_dir = world
        .placeholders
        .get(&placeholder)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("resource directory placeholder '{placeholder}' must exist"));
    let files: BTreeMap<String, String> =
        serde_json::from_str(docstring(step)).expect("fixture docstring must be valid JSON");
    for (relative_path, contents) in files {
        let destination = resource_dir.join(PathBuf::from(relative_path));
        let parent = destination
            .parent()
            .expect("fixture file must have a parent directory");
        std::fs::create_dir_all(parent).expect("fixture parent directory should be created");
        std::fs::write(destination, contents).expect("fixture file should be written");
    }
}

#[given(expr = "node {string} has resource directory {string} with file {string} of {int} MiB")]
async fn given_node_has_large_resource_file(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
    relative_path: String,
    mebibytes: usize,
) {
    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("fixture directory should be removed");
    }
    let destination = resource_dir.join(relative_path);
    let parent = destination
        .parent()
        .expect("fixture file must have a parent directory");
    std::fs::create_dir_all(parent).expect("fixture parent directory should be created");
    let total_bytes = mebibytes
        .checked_mul(1024 * 1024)
        .expect("fixture size must fit the target pointer width");
    let mut file =
        std::fs::File::create(&destination).expect("large fixture file should be created");
    let chunk = vec![0x5a_u8; 64 * 1024];
    let full_chunks = total_bytes / chunk.len();
    let remainder = total_bytes % chunk.len();
    for _ in 0..full_chunks {
        file.write_all(&chunk)
            .expect("large fixture chunk should be written");
    }
    file.write_all(&chunk[..remainder])
        .expect("large fixture remainder should be written");

    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

/// The generated ONNX models a fixture resource directory carries under `models/`.
const ONNX_FIXTURE_MODELS: [&str; 7] = [
    "simple_score.onnx",
    "alternate_score.onnx",
    "batch_score.onnx",
    "dynamic_batch_score.onnx",
    "matrix_identity.onnx",
    "scalar_identity.onnx",
    "f64_score.onnx",
];

fn onnx_fixture_path(fixture: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("onnx")
        .join(fixture)
}

/// Writes every generated ONNX model into a fresh resource directory for `node_id` and registers
/// the directory under `placeholder`.
fn place_onnx_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: &str,
    placeholder: &str,
) -> PathBuf {
    ensure_onnx_runtime_loaded();

    let base_dir = world
        .cluster()
        .node_base_dir(node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("old fixture directory should be removed");
    }
    std::fs::create_dir_all(resource_dir.join("models"))
        .expect("fixture model directory should be created");
    for fixture in ONNX_FIXTURE_MODELS {
        let source_path = onnx_fixture_path(fixture);
        let destination_path = resource_dir.join("models").join(fixture);
        std::fs::copy(&source_path, &destination_path).unwrap_or_else(|error| {
            panic!(
                "failed to copy ONNX fixture '{}' to '{}': {error}",
                source_path.display(),
                destination_path.display()
            )
        });
    }

    world
        .placeholders
        .insert(placeholder.to_string(), resource_dir.display().to_string());
    resource_dir
}

#[given(expr = "node {string} has ONNX fixture resource directory {string}")]
async fn given_node_has_onnx_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_onnx_fixture_resource_directory(world, &node_id, &placeholder);
}

#[given(
    expr = "node {string} has ONNX fixture resource directory {string} with {string} copied from \
            fixture {string}"
)]
async fn given_node_has_onnx_fixture_resource_directory_with_replaced_model(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
    relative_path: String,
    fixture: String,
) {
    let resource_dir = place_onnx_fixture_resource_directory(world, &node_id, &placeholder);
    let source_path = onnx_fixture_path(&fixture);
    let destination_path = resource_dir.join(&relative_path);
    std::fs::copy(&source_path, &destination_path).unwrap_or_else(|error| {
        panic!(
            "failed to copy ONNX fixture '{}' over '{}': {error}",
            source_path.display(),
            destination_path.display()
        )
    });
}

#[given(expr = "node {string} has WASM processor fixture resource directory {string}")]
async fn given_node_has_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_wasm_processor_fixture(world, &node_id, &placeholder, "rust").await;
}

#[given(expr = "node {string} has {string} WASM processor fixture resource directory {string}")]
async fn given_node_has_named_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    guest: String,
    placeholder: String,
) {
    place_wasm_processor_fixture(world, &node_id, &placeholder, &guest).await;
}

#[given(expr = "node {string} has {string} example WASM processor resource directory {string}")]
async fn given_node_has_example_wasm_processor_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    guest: String,
    placeholder: String,
) {
    place_wasm_processor_fixture_with_layout(
        world,
        &node_id,
        &placeholder,
        &guest,
        WasmProcessorFixtureLayout::ExampleRoot,
    )
    .await;
}

enum WasmProcessorFixtureLayout {
    FixtureProcessorFile,
    ExampleRoot,
}

async fn place_wasm_processor_fixture(
    world: &mut ScenarioWorld,
    node_id: &str,
    placeholder: &str,
    guest: &str,
) {
    place_wasm_processor_fixture_with_layout(
        world,
        node_id,
        placeholder,
        guest,
        WasmProcessorFixtureLayout::FixtureProcessorFile,
    )
    .await;
}

async fn place_wasm_processor_fixture_with_layout(
    world: &mut ScenarioWorld,
    node_id: &str,
    placeholder: &str,
    guest: &str,
    layout: WasmProcessorFixtureLayout,
) {
    let base_dir = world
        .cluster()
        .node_base_dir(node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(placeholder);
    if tokio::fs::try_exists(&resource_dir)
        .await
        .expect("fixture directory existence check should succeed")
    {
        tokio::fs::remove_dir_all(&resource_dir)
            .await
            .expect("old fixture directory should be removed");
    }
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let (source_path, artifact_file) = match guest {
        "rust" => (
            repo_root
                .join("examples")
                .join("wasm-processors")
                .join("rust-guest")
                .join("target")
                .join("wasm32-unknown-unknown")
                .join("release")
                .join("nervix_wasm_processor_rust_guest.wasm"),
            "nervix_wasm_processor_rust_guest.wasm",
        ),
        "go" => (
            repo_root
                .join("examples")
                .join("wasm-processors")
                .join("go-guest")
                .join("nervix_wasm_processor_go_guest.wasm"),
            "nervix_wasm_processor_go_guest.wasm",
        ),
        other => panic!("unsupported WASM processor fixture guest '{other}'"),
    };
    let destination_path = match layout {
        WasmProcessorFixtureLayout::FixtureProcessorFile => {
            tokio::fs::create_dir_all(resource_dir.join("processors"))
                .await
                .expect("fixture processor directory should be created");
            resource_dir.join("processors").join("filter_even.wasm")
        }
        WasmProcessorFixtureLayout::ExampleRoot => {
            tokio::fs::create_dir_all(&resource_dir)
                .await
                .expect("fixture resource directory should be created");
            resource_dir.join(artifact_file)
        }
    };
    tokio::fs::copy(&source_path, &destination_path)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to copy WASM processor fixture '{}' to '{}': {error}",
                source_path.display(),
                destination_path.display()
            )
        });

    world
        .placeholders
        .insert(placeholder.to_string(), resource_dir.display().to_string());
}

#[given(expr = "node {string} has invalid WASM processor fixture resource directory {string}")]
async fn given_node_has_invalid_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        b"not a wasm module".to_vec(),
    )
    .await;
}

#[given(
    expr = "node {string} has malformed-output WASM processor fixture resource directory {string}"
)]
async fn given_node_has_malformed_output_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        malformed_output_wasm_fixture().to_vec(),
    )
    .await;
}

#[given(
    expr = "node {string} has a WASM fixture returning an uninitialized column to relay {string} \
            in resource directory {string}"
)]
async fn given_node_has_uninitialized_output_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    output_relay: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        uninitialized_output_wasm_fixture(&output_relay),
    )
    .await;
}

#[given(
    expr = "node {string} has historical-time tokenless WASM processor fixture resource directory \
            {string}"
)]
async fn given_node_has_historical_time_tokenless_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        historical_time_tokenless_wasm_fixture("generated_events"),
    )
    .await;
}

#[given(expr = "node {string} has trapping WASM processor fixture resource directory {string}")]
async fn given_node_has_trapping_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        trapping_wasm_fixture().to_vec(),
    )
    .await;
}

#[given(
    expr = "node {string} has state-rejecting WASM processor fixture resource directory {string}"
)]
async fn given_node_has_state_rejecting_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        state_rejecting_wasm_fixture("restored_events"),
    )
    .await;
}

#[given(
    expr = "node {string} has state-counting WASM processor fixture resource directory {string}"
)]
async fn given_node_has_state_counting_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        state_counting_wasm_fixture("counted_events"),
    )
    .await;
}

#[given(
    expr = "node {string} has guest-requested-reset WASM processor fixture resource directory \
            {string}"
)]
async fn given_node_has_guest_requested_reset_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        guest_requested_reset_wasm_fixture("released_events"),
    )
    .await;
}

#[given(
    expr = "node {string} has timeout-buffering WASM processor fixture resource directory {string}"
)]
async fn given_node_has_timeout_buffering_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        timeout_buffering_wasm_fixture("released_events"),
    )
    .await;
}

#[given(
    expr = "node {string} has {string} failing WASM processor fixture resource directory {string}"
)]
async fn given_node_has_failing_wasm_processor_fixture_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    failure: String,
    placeholder: String,
) {
    place_generated_wasm_processor_fixture(
        world,
        &node_id,
        &placeholder,
        WasmLifecycleFailureFixture::parse(&failure).wasm("limited_events"),
    )
    .await;
}

async fn place_generated_wasm_processor_fixture(
    world: &mut ScenarioWorld,
    node_id: &str,
    placeholder: &str,
    wasm: Vec<u8>,
) {
    let base_dir = world
        .cluster()
        .node_base_dir(node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(placeholder);
    if tokio::fs::try_exists(&resource_dir)
        .await
        .expect("fixture directory existence check should succeed")
    {
        tokio::fs::remove_dir_all(&resource_dir)
            .await
            .expect("old fixture directory should be removed");
    }
    tokio::fs::create_dir_all(resource_dir.join("processors"))
        .await
        .expect("fixture processor directory should be created");
    tokio::fs::write(
        resource_dir.join("processors").join("filter_even.wasm"),
        wasm,
    )
    .await
    .expect("generated WASM fixture should be written");
    world
        .placeholders
        .insert(placeholder.to_string(), resource_dir.display().to_string());
}

fn malformed_output_wasm_fixture() -> &'static [u8] {
    br#"(module
      (import "env" "nervix_domain_time_nanos" (func $domain_time (result i64)))
      (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
      (memory (export "memory") 1)
      (global $emitted (mut i32) (i32.const 0))
      (data (i32.const 0) "\01")
      (func (export "nervix_buffer_ptr") (result i32) (i32.const 0))
      (func (export "nervix_buffer_len") (result i32) (i32.const 1))
      (func (export "nervix_buffer_capacity") (result i32) (i32.const 65536))
      (func (export "nervix_alloc") (param i32) (result i32) (i32.const 0))
      (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_current_domain_time_nanos") (result i64) call $domain_time)
      (func (export "nervix_process_batch") (param i32 i32) (result i32)
        i32.const 1
        global.set $emitted
        i32.const 0)
      (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
      (func (export "nervix_flush") (result i32) (i32.const 0))
      (func (export "nervix_read_emit") (result i32)
        global.get $emitted
        if (result i32)
          i32.const 0
          global.set $emitted
          i32.const 1
        else
          i32.const 0
        end)
      (func (export "nervix_dump_state") (result i32) (i32.const 0))
      (func (export "nervix_load_state") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_reset_state") (result i32) (i32.const 0))
    )"#
}

fn historical_time_tokenless_wasm_fixture(output_relay: &str) -> Vec<u8> {
    let schema = StdArc::new(ArrowSchema::new(vec![ArrowField::new(
        "",
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![StdArc::new(Int64Array::from(vec![42_i64]))],
    )
    .expect("historical-time WASM generated batch must build");
    let mut generated_arrow_ipc_batch = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut generated_arrow_ipc_batch, &schema)
            .expect("historical-time WASM Arrow writer must build");
        writer
            .write(&batch)
            .expect("historical-time WASM generated batch must encode");
        writer
            .finish()
            .expect("historical-time WASM Arrow stream must finish");
    }
    let encoded = WasmEnvelope::output(
        generated_arrow_ipc_batch,
        vec![WasmRoutedOutput::new(
            output_relay,
            vec![WasmOutputColumnRef::generated(0)],
            WasmAckSidecar {
                rows: vec![WasmOutputRow::default()],
                ..WasmAckSidecar::default()
            },
        )],
    )
    .encode()
    .expect("historical-time WASM output fixture must encode");
    let encoded_wat = encoded
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let encoded_len = encoded.len();

    format!(
        r#"(module
          (import "env" "nervix_domain_time_nanos" (func $domain_time (result i64)))
          (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
          (memory (export "memory") 2)
          (global $emitted (mut i32) (i32.const 0))
          (data (i32.const 32768) "{encoded_wat}")
          (func (export "nervix_buffer_ptr") (result i32) (i32.const 32768))
          (func (export "nervix_buffer_len") (result i32) (i32.const {encoded_len}))
          (func (export "nervix_buffer_capacity") (result i32) (i32.const 131072))
          (func (export "nervix_alloc") (param i32) (result i32) (i32.const 0))
          (func (export "nervix_init") (param i32 i32) (result i32)
            call $domain_time
            i64.const 978307200000000000
            i64.lt_s
            if
              i32.const 0
              return
            end
            i32.const -1)
          (func (export "nervix_current_domain_time_nanos") (result i64) call $domain_time)
          (func (export "nervix_process_batch") (param i32 i32) (result i32)
            call $domain_time
            i64.const 978307200000000000
            i64.lt_s
            if
              i64.const 1000000000
              call $timeout
              drop
            end
            i32.const 0)
          (func (export "nervix_on_timeout") (param i64) (result i32)
            call $domain_time
            i64.const 978307200000000000
            i64.lt_s
            if
              i32.const 1
              global.set $emitted
            end
            i32.const 0)
          (func (export "nervix_flush") (result i32) (i32.const 0))
          (func (export "nervix_read_emit") (result i32)
            global.get $emitted
            if (result i32)
              i32.const 0
              global.set $emitted
              i32.const {encoded_len}
            else
              i32.const 0
            end)
          (func (export "nervix_dump_state") (result i32) (i32.const 0))
          (func (export "nervix_load_state") (param i32 i32) (result i32) (i32.const 0))
          (func (export "nervix_reset_state") (result i32)
            i32.const 0
            global.set $emitted
            i32.const 0)
        )"#
    )
    .into_bytes()
}

fn uninitialized_output_wasm_fixture(output_relay: &str) -> Vec<u8> {
    let encoded = WasmEnvelope::output(
        Vec::new(),
        vec![WasmRoutedOutput::new(
            output_relay,
            vec![WasmOutputColumnRef::uninitialized()],
            WasmAckSidecar {
                rows: vec![WasmOutputRow::default()],
                ..WasmAckSidecar::default()
            },
        )],
    )
    .encode()
    .expect("uninitialized WASM output fixture must encode");
    let encoded_wat = encoded
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let encoded_len = encoded.len();

    format!(
        r#"(module
          (memory (export "memory") 2)
          (global $emitted (mut i32) (i32.const 0))
          (data (i32.const 32768) "{encoded_wat}")
          (func (export "nervix_buffer_ptr") (result i32) (i32.const 32768))
          (func (export "nervix_buffer_len") (result i32) (i32.const {encoded_len}))
          (func (export "nervix_buffer_capacity") (result i32) (i32.const 131072))
          (func (export "nervix_alloc") (param i32) (result i32) (i32.const 0))
          (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
          (func (export "nervix_current_domain_time_nanos") (result i64) (i64.const 0))
          (func (export "nervix_process_batch") (param i32 i32) (result i32)
            i32.const 1
            global.set $emitted
            i32.const 0)
          (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
          (func (export "nervix_flush") (result i32) (i32.const 0))
          (func (export "nervix_read_emit") (result i32)
            global.get $emitted
            if (result i32)
              i32.const 0
              global.set $emitted
              i32.const {encoded_len}
            else
              i32.const 0
            end)
          (func (export "nervix_dump_state") (result i32) (i32.const 0))
          (func (export "nervix_load_state") (param i32 i32) (result i32) (i32.const 0))
          (func (export "nervix_reset_state") (result i32)
            i32.const 0
            global.set $emitted
            i32.const 0)
        )"#
    )
    .into_bytes()
}

fn trapping_wasm_fixture() -> &'static [u8] {
    br#"(module
      (import "env" "nervix_domain_time_nanos" (func $domain_time (result i64)))
      (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
      (memory (export "memory") 1)
      (func (export "nervix_buffer_ptr") (result i32) (i32.const 0))
      (func (export "nervix_buffer_len") (result i32) (i32.const 0))
      (func (export "nervix_buffer_capacity") (result i32) (i32.const 65536))
      (func (export "nervix_alloc") (param i32) (result i32) (i32.const 0))
      (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_current_domain_time_nanos") (result i64) call $domain_time)
      (func (export "nervix_process_batch") (param i32 i32) (result i32)
        unreachable)
      (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
      (func (export "nervix_flush") (result i32) (i32.const 0))
      (func (export "nervix_read_emit") (result i32) (i32.const 0))
      (func (export "nervix_dump_state") (result i32) (i32.const 0))
      (func (export "nervix_load_state") (param i32 i32) (result i32) (i32.const 0))
      (func (export "nervix_reset_state") (result i32) (i32.const 0))
    )"#
}

/// A guest whose only saved state is how many batches its branch has processed. It emits one row
/// with uninitialized columns for every even-numbered batch, so whether the next batch produces a
/// row tells which saved count a recreated instance restored. Written as text, it compiles quickly
/// enough for a forced ownership recovery to validate its restore within the recovery budget.
fn state_counting_wasm_fixture(output_relay: &str) -> Vec<u8> {
    let encoded = WasmEnvelope::output(
        Vec::new(),
        vec![WasmRoutedOutput::new(
            output_relay,
            vec![
                WasmOutputColumnRef::uninitialized(),
                WasmOutputColumnRef::uninitialized(),
            ],
            WasmAckSidecar {
                rows: vec![WasmOutputRow::default()],
                ..WasmAckSidecar::default()
            },
        )],
    )
    .encode()
    .expect("state-counting WASM output fixture must encode");
    let encoded_wat = encoded
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let encoded_len = encoded.len();

    format!(
        r#"(module
          (memory (export "memory") 1)
          (global $count (mut i32) (i32.const 0))
          (global $emitted (mut i32) (i32.const 0))
          (global $read_ptr (mut i32) (i32.const 0))
          (data (i32.const 32768) "{encoded_wat}")
          (func (export "nervix_buffer_ptr") (result i32) global.get $read_ptr)
          (func (export "nervix_buffer_len") (result i32) (i32.const {encoded_len}))
          (func (export "nervix_buffer_capacity") (result i32) (i32.const 16384))
          (func (export "nervix_alloc") (param i32) (result i32)
            i32.const 0
            global.set $read_ptr
            i32.const 0)
          (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
          (func (export "nervix_current_domain_time_nanos") (result i64) (i64.const 0))
          (func (export "nervix_process_batch") (param i32 i32) (result i32)
            global.get $count
            i32.const 1
            i32.add
            global.set $count
            global.get $count
            i32.const 2
            i32.rem_u
            i32.eqz
            global.set $emitted
            i32.const 0)
          (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
          (func (export "nervix_flush") (result i32) (i32.const 0))
          (func (export "nervix_read_emit") (result i32)
            global.get $emitted
            if (result i32)
              i32.const 0
              global.set $emitted
              i32.const 32768
              global.set $read_ptr
              i32.const {encoded_len}
            else
              i32.const 0
            end)
          (func (export "nervix_dump_state") (result i32)
            i32.const 16
            global.get $count
            i32.store
            i32.const 16
            global.set $read_ptr
            i32.const 4)
          (func (export "nervix_load_state") (param $ptr i32) (param $len i32) (result i32)
            local.get $len
            i32.const 4
            i32.ne
            if (result i32)
              i32.const {rejected}
            else
              local.get $ptr
              i32.load
              global.set $count
              i32.const 0
            end)
          (func (export "nervix_reset_state") (result i32)
            i32.const 0
            global.set $count
            i32.const 0
            global.set $emitted
            i32.const 0)
        )"#,
        rejected = nervix_wasm::SavedStateRejection::ApplicationState.code()
    )
    .into_bytes()
}

/// A guest that buffers one output behind a logical timeout. An instance the host created without
/// saved state asks, from that timeout callback, for a new guest-state lifetime instead of
/// releasing what it buffered; an instance restored from saved state releases it. The buffered
/// output belongs to the callback that asked, so nothing the asking instance holds may ever reach
/// the relay, and a payload can only come from an instance restored from the lifetime the reset
/// published.
fn guest_requested_reset_wasm_fixture(output_relay: &str) -> Vec<u8> {
    let encoded = WasmEnvelope::output(
        Vec::new(),
        vec![WasmRoutedOutput::new(
            output_relay,
            vec![
                WasmOutputColumnRef::uninitialized(),
                WasmOutputColumnRef::uninitialized(),
            ],
            WasmAckSidecar {
                rows: vec![WasmOutputRow::default()],
                ..WasmAckSidecar::default()
            },
        )],
    )
    .encode()
    .expect("guest-requested reset WASM output fixture must encode");
    let encoded_wat = encoded
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let encoded_len = encoded.len();

    format!(
        r#"(module
          (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
          (import "env" "nervix_request_state_reset" (func $request_state_reset (result i32)))
          (memory (export "memory") 1)
          (global $count (mut i32) (i32.const 0))
          (global $restored (mut i32) (i32.const 0))
          (global $pending (mut i32) (i32.const 0))
          (global $emitted (mut i32) (i32.const 0))
          (global $read_ptr (mut i32) (i32.const 0))
          (data (i32.const 32768) "{encoded_wat}")
          (func (export "nervix_buffer_ptr") (result i32) global.get $read_ptr)
          (func (export "nervix_buffer_len") (result i32) (i32.const {encoded_len}))
          (func (export "nervix_buffer_capacity") (result i32) (i32.const 16384))
          (func (export "nervix_alloc") (param i32) (result i32)
            i32.const 0
            global.set $read_ptr
            i32.const 0)
          (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
          (func (export "nervix_current_domain_time_nanos") (result i64) (i64.const 0))
          (func (export "nervix_process_batch") (param i32 i32) (result i32)
            global.get $count
            i32.const 1
            i32.add
            global.set $count
            i32.const 1
            global.set $pending
            i64.const 1000000000
            call $timeout
            drop
            i32.const 0)
          (func $release (result i32)
            global.get $pending
            if
              i32.const 0
              global.set $pending
              i32.const 1
              global.set $emitted
            end
            i32.const 0)
          (func (export "nervix_on_timeout") (param i64) (result i32)
            global.get $restored
            if (result i32)
              call $release
            else
              ;; The counter this lifetime saved is unusable, so instead of releasing what it
              ;; buffered this instance asks for the whole state lifetime. Asking twice in one
              ;; callback must still replace that lifetime exactly once.
              call $request_state_reset
              drop
              call $request_state_reset
              drop
              i32.const 0
            end)
          (func (export "nervix_flush") (result i32) call $release)
          (func (export "nervix_read_emit") (result i32)
            global.get $emitted
            if (result i32)
              i32.const 0
              global.set $emitted
              i32.const 32768
              global.set $read_ptr
              i32.const {encoded_len}
            else
              i32.const 0
            end)
          (func (export "nervix_dump_state") (result i32)
            i32.const 16
            global.get $count
            i32.store
            i32.const 16
            global.set $read_ptr
            i32.const 4)
          (func (export "nervix_load_state") (param $ptr i32) (param $len i32) (result i32)
            local.get $len
            i32.const 4
            i32.ne
            if (result i32)
              i32.const {rejected}
            else
              local.get $ptr
              i32.load
              global.set $count
              i32.const 1
              global.set $restored
              i32.const 0
            end)
          (func (export "nervix_reset_state") (result i32)
            i32.const 0
            global.set $count
            i32.const 0
            global.set $restored
            i32.const 0
            global.set $pending
            i32.const 0
            global.set $emitted
            i32.const 0)
        )"#,
        rejected = nervix_wasm::SavedStateRejection::ApplicationState.code()
    )
    .into_bytes()
}

/// A guest that holds one output behind a logical timeout. A force flush releases that output,
/// which lets reset scenarios prove that accepted work settles once and that the old timer cannot
/// publish again after its branch lifetime is replaced.
fn timeout_buffering_wasm_fixture(output_relay: &str) -> Vec<u8> {
    let encoded = WasmEnvelope::output(
        Vec::new(),
        vec![WasmRoutedOutput::new(
            output_relay,
            vec![
                WasmOutputColumnRef::uninitialized(),
                WasmOutputColumnRef::uninitialized(),
            ],
            WasmAckSidecar {
                rows: vec![WasmOutputRow::default()],
                ..WasmAckSidecar::default()
            },
        )],
    )
    .encode()
    .expect("timeout-buffering WASM output fixture must encode");
    let encoded_wat = encoded
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let encoded_len = encoded.len();

    format!(
        r#"(module
          (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
          (memory (export "memory") 1)
          (global $pending (mut i32) (i32.const 0))
          (global $emitted (mut i32) (i32.const 0))
          (global $read_ptr (mut i32) (i32.const 0))
          (data (i32.const 32768) "{encoded_wat}")
          (func (export "nervix_buffer_ptr") (result i32) global.get $read_ptr)
          (func (export "nervix_buffer_len") (result i32) (i32.const {encoded_len}))
          (func (export "nervix_buffer_capacity") (result i32) (i32.const 16384))
          (func (export "nervix_alloc") (param i32) (result i32)
            i32.const 0
            global.set $read_ptr
            i32.const 0)
          (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
          (func (export "nervix_current_domain_time_nanos") (result i64) (i64.const 0))
          (func (export "nervix_process_batch") (param i32 i32) (result i32)
            i32.const 1
            global.set $pending
            i64.const 3000000000
            call $timeout
            drop
            i32.const 0)
          (func $release (result i32)
            global.get $pending
            if
              i32.const 0
              global.set $pending
              i32.const 1
              global.set $emitted
            end
            i32.const 0)
          (func (export "nervix_on_timeout") (param i64) (result i32) call $release)
          (func (export "nervix_flush") (result i32) call $release)
          (func (export "nervix_read_emit") (result i32)
            global.get $emitted
            if (result i32)
              i32.const 0
              global.set $emitted
              i32.const 32768
              global.set $read_ptr
              i32.const {encoded_len}
            else
              i32.const 0
            end)
          (func (export "nervix_dump_state") (result i32)
            i32.const 16
            global.get $pending
            i32.store
            i32.const 16
            global.set $read_ptr
            i32.const 4)
          (func (export "nervix_load_state") (param $ptr i32) (param $len i32) (result i32)
            local.get $len
            i32.const 4
            i32.ne
            if (result i32)
              i32.const {rejected}
            else
              local.get $ptr
              i32.load
              global.set $pending
              i32.const 0
            end)
          (func (export "nervix_reset_state") (result i32)
            i32.const 0
            global.set $pending
            i32.const 0
            global.set $emitted
            i32.const 0)
        )"#,
        rejected = nervix_wasm::SavedStateRejection::ApplicationState.code()
    )
    .into_bytes()
}

fn state_rejecting_wasm_fixture(output_relay: &str) -> Vec<u8> {
    let encoded = WasmEnvelope::output(
        Vec::new(),
        vec![WasmRoutedOutput::new(
            output_relay,
            vec![WasmOutputColumnRef::input(0)],
            WasmAckSidecar {
                rows: vec![WasmOutputRow {
                    tokens: vec![WasmAckToken(1)],
                    source_token: Some(WasmAckToken(1)),
                }],
                ..WasmAckSidecar::default()
            },
        )],
    )
    .encode()
    .expect("state-rejecting WASM output fixture must encode");
    let encoded_wat = encoded
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let encoded_len = encoded.len();

    format!(
        r#"(module
          (memory (export "memory") 2)
          (global $emitted (mut i32) (i32.const 0))
          (global $read_ptr (mut i32) (i32.const 0))
          (data (i32.const 16) "\2a")
          (data (i32.const 32768) "{encoded_wat}")
          (func (export "nervix_buffer_ptr") (result i32) global.get $read_ptr)
          (func (export "nervix_buffer_len") (result i32) (i32.const {encoded_len}))
          (func (export "nervix_buffer_capacity") (result i32) (i32.const 131072))
          (func (export "nervix_alloc") (param i32) (result i32)
            i32.const 0
            global.set $read_ptr
            i32.const 0)
          (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
          (func (export "nervix_current_domain_time_nanos") (result i64) (i64.const 0))
          (func (export "nervix_process_batch") (param i32 i32) (result i32)
            i32.const 1
            global.set $emitted
            i32.const 0)
          (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
          (func (export "nervix_flush") (result i32) (i32.const 0))
          (func (export "nervix_read_emit") (result i32)
            global.get $emitted
            if (result i32)
              i32.const 0
              global.set $emitted
              i32.const 32768
              global.set $read_ptr
              i32.const {encoded_len}
            else
              i32.const 0
            end)
          (func (export "nervix_dump_state") (result i32)
            i32.const 16
            global.set $read_ptr
            i32.const 1)
          (func (export "nervix_load_state") (param i32 i32) (result i32)
            (i32.const {rejected}))
          (func (export "nervix_reset_state") (result i32) (i32.const 0))
        )"#,
        rejected = nervix_wasm::SavedStateRejection::ApplicationState.code()
    )
    .into_bytes()
}

/// Which guest operation a generated lifecycle fixture fails, for the branches that trigger it.
///
/// Every variant emits one row with uninitialized columns per processed input, so a route can
/// construct its output from the branch alone. A trigger is either an input envelope larger than
/// the thresholds below, which a large message produces, or a branch key naming
/// [`INIT_REFUSED_TENANT`].
#[derive(Clone, Copy, Debug)]
enum WasmLifecycleFailureFixture {
    /// An input over the large threshold spins until `MAX FUEL` runs out.
    Fuel,
    /// An input over the large threshold grows linear memory past `MAX MEMORY`.
    Memory,
    /// A branch whose key names [`INIT_REFUSED_TENANT`] refuses its branch configuration.
    Initialization,
    /// An input over the large threshold requests a timeout whose callback fails.
    Timeout,
    /// An input over the large threshold leaves the guest unable to serialize its state.
    StateSnapshot,
    /// An input over the large threshold saves state the guest refuses to restore, and an input
    /// over the huge threshold exhausts `MAX FUEL` so the instance is recreated from that state.
    StateRestore,
}

/// The tenant whose branch configuration the initialization fixture refuses.
const INIT_REFUSED_TENANT: &str = "init-refused";

impl WasmLifecycleFailureFixture {
    const LARGE_INPUT_BYTES: usize = 2_048;
    const HUGE_INPUT_BYTES: usize = 8_192;
    const INIT_REFUSAL: &str = "guest refuses its branch configuration";
    const TIMEOUT_REFUSAL: &str = "guest refuses its timeout";
    const SNAPSHOT_REFUSAL: &str = "guest cannot serialize its state";
    const RESTORE_REFUSAL: &str = "guest refuses its saved counters";

    fn parse(name: &str) -> Self {
        match name {
            "fuel" => Self::Fuel,
            "memory" => Self::Memory,
            "initialization" => Self::Initialization,
            "timeout" => Self::Timeout,
            "state snapshot" => Self::StateSnapshot,
            "state restore" => Self::StateRestore,
            other => panic!("unsupported failing WASM processor fixture '{other}'"),
        }
    }

    fn init_body(self) -> String {
        match self {
            Self::Initialization => format!(
                "local.get $ptr local.get $size call $contains_init_refused_tenant
                 if
                   i32.const 40100 i32.const {} call $report
                   i32.const -6
                   return
                 end
                 i32.const 0",
                Self::INIT_REFUSAL.len()
            ),
            Self::Fuel
            | Self::Memory
            | Self::Timeout
            | Self::StateSnapshot
            | Self::StateRestore => "i32.const 0".to_string(),
        }
    }

    fn process_body(self) -> String {
        let large = Self::LARGE_INPUT_BYTES;
        let huge = Self::HUGE_INPUT_BYTES;
        match self {
            Self::Fuel => format!(
                "local.get $size i32.const {large} i32.gt_u
                 if (loop $spin br $spin) end"
            ),
            Self::Memory => format!(
                "local.get $size i32.const {large} i32.gt_u
                 if i32.const 1 memory.grow drop end"
            ),
            Self::Timeout => format!(
                "local.get $size i32.const {large} i32.gt_u
                 if i64.const 1000000 call $timeout drop end"
            ),
            Self::StateSnapshot => format!(
                "local.get $size i32.const {large} i32.gt_u
                 if i32.const 1 global.set $poisoned end"
            ),
            Self::StateRestore => format!(
                "local.get $size i32.const {huge} i32.gt_u
                 if (loop $spin br $spin) end
                 local.get $size i32.const {large} i32.gt_u
                 if i32.const 1 global.set $poisoned end"
            ),
            Self::Initialization => String::new(),
        }
    }

    fn on_timeout_body(self) -> String {
        match self {
            Self::Timeout => format!(
                "i32.const 40300 i32.const {} call $report i32.const -6",
                Self::TIMEOUT_REFUSAL.len()
            ),
            Self::Fuel
            | Self::Memory
            | Self::Initialization
            | Self::StateSnapshot
            | Self::StateRestore => "i32.const 0".to_string(),
        }
    }

    fn dump_state_body(self) -> String {
        match self {
            Self::StateSnapshot => format!(
                "global.get $poisoned
                 if (result i32)
                   i32.const 40200 i32.const {} call $report
                   i32.const -6
                 else
                   i32.const 0
                 end",
                Self::SNAPSHOT_REFUSAL.len()
            ),
            Self::StateRestore => "global.get $poisoned
                 if (result i32)
                   i32.const 40600 global.set $read_ptr
                   i32.const 1
                 else
                   i32.const 0
                 end"
            .to_string(),
            Self::Fuel | Self::Memory | Self::Initialization | Self::Timeout => {
                "i32.const 0".to_string()
            }
        }
    }

    fn load_state_body(self) -> String {
        match self {
            Self::StateRestore => format!(
                "i32.const 40400 i32.const {} call $report i32.const {}",
                Self::RESTORE_REFUSAL.len(),
                nervix_wasm::SavedStateRejection::ApplicationState.code()
            ),
            Self::Fuel
            | Self::Memory
            | Self::Initialization
            | Self::Timeout
            | Self::StateSnapshot => "i32.const 0".to_string(),
        }
    }

    fn wasm(self, output_relay: &str) -> Vec<u8> {
        let encoded = WasmEnvelope::output(
            Vec::new(),
            vec![WasmRoutedOutput::new(
                output_relay,
                vec![
                    WasmOutputColumnRef::uninitialized(),
                    WasmOutputColumnRef::uninitialized(),
                ],
                WasmAckSidecar {
                    rows: vec![WasmOutputRow::default()],
                    ..WasmAckSidecar::default()
                },
            )],
        )
        .encode()
        .expect("failing WASM output fixture must encode");
        let encoded_wat = encoded
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let encoded_len = encoded.len();
        let marker_len = INIT_REFUSED_TENANT.len();
        let init_body = self.init_body();
        let process_body = self.process_body();
        let on_timeout_body = self.on_timeout_body();
        let dump_state_body = self.dump_state_body();
        let load_state_body = self.load_state_body();
        let init_refusal = Self::INIT_REFUSAL;
        let snapshot_refusal = Self::SNAPSHOT_REFUSAL;
        let timeout_refusal = Self::TIMEOUT_REFUSAL;
        let restore_refusal = Self::RESTORE_REFUSAL;

        format!(
            r#"(module
          (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
          (memory (export "memory") 1)
          (global $emitted (mut i32) (i32.const 0))
          (global $read_ptr (mut i32) (i32.const 0))
          (global $poisoned (mut i32) (i32.const 0))
          (global $reason_ptr (mut i32) (i32.const 0))
          (global $reason_len (mut i32) (i32.const 0))
          (data (i32.const 32768) "{encoded_wat}")
          (data (i32.const 40000) "{INIT_REFUSED_TENANT}")
          (data (i32.const 40100) "{init_refusal}")
          (data (i32.const 40200) "{snapshot_refusal}")
          (data (i32.const 40300) "{timeout_refusal}")
          (data (i32.const 40400) "{restore_refusal}")
          (data (i32.const 40600) "\2a")
          (func $report (param $ptr i32) (param $len i32)
            local.get $ptr
            global.set $reason_ptr
            local.get $len
            global.set $reason_len)
          (func $contains_init_refused_tenant (param $ptr i32) (param $size i32) (result i32)
            (local $start i32)
            (local $last i32)
            (local $offset i32)
            local.get $size
            i32.const {marker_len}
            i32.lt_u
            if
              i32.const 0
              return
            end
            local.get $ptr
            local.get $size
            i32.add
            i32.const {marker_len}
            i32.sub
            local.set $last
            local.get $ptr
            local.set $start
            block $absent
              loop $scan
                local.get $start
                local.get $last
                i32.gt_u
                br_if $absent
                i32.const 0
                local.set $offset
                block $mismatch
                  loop $compare
                    local.get $offset
                    i32.const {marker_len}
                    i32.eq
                    if
                      i32.const 1
                      return
                    end
                    local.get $start
                    local.get $offset
                    i32.add
                    i32.load8_u
                    local.get $offset
                    i32.const 40000
                    i32.add
                    i32.load8_u
                    i32.ne
                    br_if $mismatch
                    local.get $offset
                    i32.const 1
                    i32.add
                    local.set $offset
                    br $compare
                  end
                end
                local.get $start
                i32.const 1
                i32.add
                local.set $start
                br $scan
              end
            end
            i32.const 0)
          (func (export "nervix_buffer_ptr") (result i32) global.get $read_ptr)
          (func (export "nervix_buffer_len") (result i32) (i32.const {encoded_len}))
          (func (export "nervix_buffer_capacity") (result i32) (i32.const 65536))
          (func (export "nervix_alloc") (param i32) (result i32)
            i32.const 0
            global.set $read_ptr
            i32.const 0)
          (func (export "nervix_global_error_ptr") (result i32) global.get $reason_ptr)
          (func (export "nervix_global_error_len") (result i32) global.get $reason_len)
          (func (export "nervix_clear_global_error") (result i32)
            i32.const 0
            global.set $reason_len
            i32.const 0)
          (func (export "nervix_init") (param $ptr i32) (param $size i32) (result i32)
            {init_body})
          (func (export "nervix_current_domain_time_nanos") (result i64) (i64.const 0))
          (func (export "nervix_process_batch") (param $ptr i32) (param $size i32) (result i32)
            {process_body}
            i32.const 1
            global.set $emitted
            i32.const 0)
          (func (export "nervix_on_timeout") (param i64) (result i32)
            {on_timeout_body})
          (func (export "nervix_flush") (result i32) (i32.const 0))
          (func (export "nervix_read_emit") (result i32)
            global.get $emitted
            if (result i32)
              i32.const 0
              global.set $emitted
              i32.const 32768
              global.set $read_ptr
              i32.const {encoded_len}
            else
              i32.const 0
            end)
          (func (export "nervix_dump_state") (result i32)
            {dump_state_body})
          (func (export "nervix_load_state") (param i32 i32) (result i32)
            {load_state_body})
          (func (export "nervix_reset_state") (result i32)
            i32.const 0
            global.set $emitted
            i32.const 0)
        )"#
        )
        .into_bytes()
    }
}

fn ensure_onnx_runtime_loaded() {
    let result = ONNX_RUNTIME_INIT.get_or_init(|| {
        let dylib_path = resolve_onnxruntime_dylib()?;
        ort::init_from(&dylib_path)
            .map_err(|error| {
                format!(
                    "failed to initialize ONNX Runtime from '{}': {error}",
                    dylib_path.display()
                )
            })?
            .commit();
        Ok(())
    });

    if let Err(error) = result {
        panic!("{error}");
    }
}

fn resolve_onnxruntime_dylib() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("ORT_DYLIB_PATH").map(PathBuf::from) {
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "ORT_DYLIB_PATH points to missing ONNX Runtime library '{}'",
            path.display()
        ));
    }

    Err(
        "ORT_DYLIB_PATH must be set before running ONNX inferencer scenarios; use `just test` or \
         `just test-scenarios --input tests/features/runtime/inferencer.feature --concurrency 1`"
            .to_string(),
    )
}

#[given(expr = "node {string} has TLS resource directory {string} for hosts {string}")]
async fn given_node_has_tls_resource_directory_for_hosts(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
    hosts: String,
) {
    let hosts = expand_placeholders(world, &hosts)
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !hosts.is_empty(),
        "TLS resource directory requires at least one hostname"
    );

    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("old fixture directory should be removed");
    }
    std::fs::create_dir_all(&resource_dir).expect("fixture directory should be created");

    let ca_key = KeyPair::generate().expect("ca key should generate");
    let mut ca_params = CertificateParams::new(Vec::new()).expect("empty CA SAN must be valid");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "nervix test ca");
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .expect("ca certificate should generate");

    let leaf_key = KeyPair::generate().expect("leaf key should generate");
    let mut leaf_params =
        CertificateParams::new(hosts.clone()).expect("leaf SAN names should be valid");
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, hosts[0].clone());
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .expect("leaf certificate should generate");

    std::fs::write(resource_dir.join("ca.crt"), ca_cert.pem()).expect("ca cert should be written");
    std::fs::write(resource_dir.join("tls.crt"), leaf_cert.pem())
        .expect("leaf cert should be written");
    std::fs::write(resource_dir.join("tls.key"), leaf_key.serialize_pem())
        .expect("leaf key should be written");

    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

#[given(expr = "node {string} has dev TLS resource directory {string}")]
async fn given_node_has_dev_tls_resource_directory(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    let base_dir = world
        .cluster()
        .node_base_dir(&node_id)
        .expect("node base dir should exist");
    let resource_dir = base_dir.join("fixtures").join(&placeholder);
    if resource_dir.exists() {
        std::fs::remove_dir_all(&resource_dir).expect("old fixture directory should be removed");
    }
    std::fs::create_dir_all(&resource_dir).expect("fixture directory should be created");

    for filename in ["ca.pem", "node.pem", "node-key.pem"] {
        let source = world
            .dependencies
            .tls_dir()
            .expect("a TLS dependency must be running")
            .join(filename);
        let destination = resource_dir.join(filename);
        std::fs::copy(&source, &destination)
            .unwrap_or_else(|error| panic!("failed to copy dev TLS asset '{filename}': {error}"));
    }

    world
        .placeholders
        .insert(placeholder, resource_dir.display().to_string());
}

#[given(expr = "node {string} is stopped")]
#[when(expr = "node {string} is stopped")]
async fn when_node_is_stopped(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster_mut()
        .stop_node(&node_id)
        .await
        .expect("failed to stop node");
}

#[when(expr = "node {string} is stopped while timing shutdown")]
async fn when_node_is_stopped_while_timing_shutdown(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    let started = Instant::now();
    world
        .cluster_mut()
        .stop_node(&node_id)
        .await
        .expect("failed to stop node");
    world.last_cluster_operation_elapsed = Some(started.elapsed());
}

#[when(expr = "node {string} is restarted {int} times with a new interconnect address")]
async fn when_node_is_restarted_with_new_interconnect_addresses(
    world: &mut ScenarioWorld,
    node_id: String,
    repetitions: usize,
) {
    let node_id = expand_placeholders(world, &node_id);
    for _ in 0..repetitions {
        tokio::task::consume_budget().await;
        world
            .cluster_mut()
            .restart_node_with_new_interconnect_address(&node_id)
            .await
            .expect("failed to restart node with a new interconnect address");
    }
}

#[when("interconnect certificates are rotated to a new certificate authority")]
async fn when_interconnect_certificates_are_rotated(world: &mut ScenarioWorld) {
    world
        .cluster_mut()
        .rotate_interconnect_certificates()
        .await
        .expect("failed to rotate interconnect certificates");
}

#[when(
    expr = "an interconnect peer with {string} credentials attempts to connect to node {string}"
)]
async fn when_interconnect_peer_with_invalid_credentials_attempts_to_connect(
    world: &mut ScenarioWorld,
    fault: String,
    node_id: String,
) {
    let fault = match fault.as_str() {
        "untrusted client" => InterconnectCredentialFault::UntrustedClient,
        "wrong cluster identity" => InterconnectCredentialFault::WrongClusterIdentity,
        "wrong node identity" => InterconnectCredentialFault::WrongNodeIdentity,
        "mismatched endpoint" => InterconnectCredentialFault::MismatchedEndpoint,
        "expired certificate" => InterconnectCredentialFault::ExpiredCertificate,
        other => panic!("unknown interconnect credential fault '{other}'"),
    };
    let result = world
        .cluster()
        .attempt_interconnect_with_invalid_credentials(&node_id, fault)
        .await;
    world.last_interconnect_attempt_error = result.err().map(|error| error.to_string());
}

#[then("the interconnect peer is rejected")]
fn then_interconnect_peer_is_rejected(world: &mut ScenarioWorld) {
    assert!(
        world.last_interconnect_attempt_error.is_some(),
        "the invalid interconnect peer unexpectedly connected"
    );
}

#[when(expr = "a silent peer starts an interconnect handshake with node {string}")]
async fn when_silent_peer_starts_interconnect_handshake(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let peer = world
        .cluster()
        .open_silent_interconnect_handshake(&node_id)
        .await
        .expect("failed to open silent interconnect handshake");
    world.silent_interconnect_peers.push(peer);
}

#[given(expr = "health requests from node {string} to node {string} pause before responding")]
async fn given_health_responses_are_paused(
    world: &mut ScenarioWorld,
    probing_node_id: String,
    responding_node_id: String,
) {
    let probing_node_id = expand_placeholders(world, &probing_node_id);
    let responding_node_id = expand_placeholders(world, &responding_node_id);
    world
        .cluster()
        .arm_health_response_pause(&probing_node_id, &responding_node_id);
}

#[given(expr = "application health responses from node {string} fail")]
#[when(expr = "application health responses from node {string} fail")]
async fn given_health_responses_fail(world: &mut ScenarioWorld, responding_node_id: String) {
    let responding_node_id = expand_placeholders(world, &responding_node_id);
    world
        .cluster()
        .fail_health_responses_from(&responding_node_id);
}

#[then(expr = "the health response pause from node {string} to node {string} is reached")]
async fn then_health_response_pause_is_reached(
    world: &mut ScenarioWorld,
    probing_node_id: String,
    responding_node_id: String,
) {
    let probing_node_id = expand_placeholders(world, &probing_node_id);
    let responding_node_id = expand_placeholders(world, &responding_node_id);
    tokio::time::timeout(
        Duration::from_secs(10),
        world
            .cluster()
            .wait_for_health_response_pause(&probing_node_id, &responding_node_id),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "health response pause from '{probing_node_id}' to '{responding_node_id}' was not \
             reached: {error}"
        )
    });
}

#[then(
    expr = "within {string} the health response pause from node {string} to node {string} is \
            reached"
)]
async fn then_health_response_pause_is_reached_within(
    world: &mut ScenarioWorld,
    duration: String,
    probing_node_id: String,
    responding_node_id: String,
) {
    let limit = humantime::parse_duration(&duration).assured("the scenario duration is valid");
    let probing_node_id = expand_placeholders(world, &probing_node_id);
    let responding_node_id = expand_placeholders(world, &responding_node_id);
    tokio::time::timeout(
        limit,
        world
            .cluster()
            .wait_for_health_response_pause(&probing_node_id, &responding_node_id),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "health response pause from '{probing_node_id}' to '{responding_node_id}' was not \
             reached within {limit:?}: {error}"
        )
    });
}

#[when(expr = "the health response pause from node {string} to node {string} is released")]
async fn when_health_response_pause_is_released(
    world: &mut ScenarioWorld,
    probing_node_id: String,
    responding_node_id: String,
) {
    let probing_node_id = expand_placeholders(world, &probing_node_id);
    let responding_node_id = expand_placeholders(world, &responding_node_id);
    world
        .cluster()
        .release_health_response_pause(&probing_node_id, &responding_node_id);
}

#[when(expr = "node {string} begins stopping")]
async fn when_node_begins_stopping(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world.cluster_mut().begin_stopping_node(&node_id);
}

#[when(expr = "node {string} is gracefully stopped")]
async fn when_node_is_gracefully_stopped(world: &mut ScenarioWorld, node_id: String) {
    assert!(
        world.cluster_config.graceful_shutdown_drain,
        "graceful shutdown drain must be configured before cluster startup"
    );
    let node_id = expand_placeholders(world, &node_id);
    let started = Instant::now();
    world
        .cluster_mut()
        .stop_node(&node_id)
        .await
        .expect("failed to gracefully stop node");
    world.last_cluster_operation_elapsed = Some(started.elapsed());
}

#[when("all nodes are stopped")]
async fn when_all_nodes_are_stopped(world: &mut ScenarioWorld) {
    world
        .cluster_mut()
        .shutdown()
        .await
        .expect("failed to stop all nodes");
}

#[when("all nodes are gracefully stopped")]
async fn when_all_nodes_are_gracefully_stopped(world: &mut ScenarioWorld) {
    assert!(
        world.cluster_config.graceful_shutdown_drain,
        "graceful shutdown drain must be configured before cluster startup"
    );
    let started = Instant::now();
    world
        .cluster_mut()
        .shutdown()
        .await
        .expect("failed to gracefully stop all nodes");
    world.last_cluster_operation_elapsed = Some(started.elapsed());
}

#[given(expr = "node {string} is started")]
#[when(expr = "node {string} is started")]
async fn when_node_is_started(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster_mut()
        .start_node(&node_id)
        .await
        .expect("failed to start node");
}

#[when(expr = "node {string} is started while consensus connectivity is blocked")]
async fn when_node_is_started_while_consensus_connectivity_is_blocked(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .block_consensus_connectivity(crate::common::cluster::node_name(&node_id));
    world
        .cluster_mut()
        .start_node_without_waiting_for_raft_catch_up(&node_id)
        .await
        .expect("failed to start node with blocked consensus connectivity");
}

#[when(expr = "consensus connectivity for node {string} is restored")]
async fn when_consensus_connectivity_for_node_is_restored(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .restore_consensus_connectivity(&crate::common::cluster::node_name(&node_id));
}

#[when(expr = "node {string} is added to the cluster")]
async fn when_node_is_added_to_the_cluster(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster_mut()
        .add_node(&node_id)
        .await
        .expect("failed to add node");
}

#[when(expr = "leadership is transferred from node {string} to node {string}")]
async fn when_leadership_is_transferred_from_node_to_node(
    world: &mut ScenarioWorld,
    from_node_id: String,
    to_node_id: String,
) {
    let from_node_id = expand_placeholders(world, &from_node_id);
    let to_node_id = expand_placeholders(world, &to_node_id);
    world
        .cluster()
        .transfer_leadership(&from_node_id, &to_node_id);
}

#[given("the leader node forgets its transaction session bindings")]
async fn given_leader_forgets_transaction_bindings(world: &mut ScenarioWorld) {
    let leader = current_leader_node(world).await;
    world
        .fault_injection
        .drop_transaction_bindings_on(crate::common::cluster::node_name(&leader));
}

#[given(expr = "command admission on node {string} pauses before proposal")]
async fn given_command_admission_pause(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .pause_command_admission_on(crate::common::cluster::node_name(&node_id));
}

#[then(expr = "the command admission pause on node {string} is reached")]
async fn then_command_admission_pause_is_reached(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    let fault_injection = world.fault_injection.clone();
    tokio::time::timeout(Duration::from_secs(30), async {
        if let Some(task) = world.background_command_result.as_mut() {
            let node_name = crate::common::cluster::node_name(&node_id);
            tokio::select! {
                () = fault_injection.wait_for_command_admission_pause(&node_name) => {},
                result = task => panic!(
                    "command on '{node_id}' returned before reaching its admission pause: \
                     {result:?}"
                ),
            }
            return;
        }
        if let Some(task) = world.background_nspl.as_mut() {
            let node_name = crate::common::cluster::node_name(&node_id);
            tokio::select! {
                () = fault_injection.wait_for_command_admission_pause(&node_name) => {},
                result = task => panic!(
                    "command on '{node_id}' returned before reaching its admission pause: \
                     {result:?}"
                ),
            }
            return;
        }
        // A request the active session sent under a name answers only when a later step reads
        // it, so its pause is awaited on its own.
        assert!(
            !world.session_requests.is_empty(),
            "a background command request or a named session request must be active"
        );
        let node_name = crate::common::cluster::node_name(&node_id);
        fault_injection
            .wait_for_command_admission_pause(&node_name)
            .await;
    })
    .await
    .unwrap_or_else(|error| {
        panic!("command admission pause on '{node_id}' was not reached: {error}")
    });
}

#[when(expr = "the command admission pause on node {string} is released")]
async fn when_command_admission_pause_is_released(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .release_command_admission_pause(&crate::common::cluster::node_name(&node_id));
}

#[given(expr = "command execution on node {string} pauses after durable admission")]
async fn given_command_durable_admission_pause(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .pause_command_after_durable_admission_on(crate::common::cluster::node_name(&node_id));
}

#[then(expr = "the durable command admission pause on node {string} is reached")]
async fn then_command_durable_admission_pause_is_reached(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let node_name = crate::common::cluster::node_name(&node_id);
    let fault_injection = world.fault_injection.clone();
    let task = world.background_command_result.as_mut();
    assert!(
        task.is_some() || !world.session_requests.is_empty(),
        "a background command request or a named session request must be active"
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        let Some(task) = task else {
            // A request the active session sent under a name answers only when a later step
            // reads it, so its pause is awaited on its own.
            fault_injection
                .wait_for_command_durable_admission_pause(&node_name)
                .await;
            return;
        };
        tokio::select! {
            () = fault_injection.wait_for_command_durable_admission_pause(&node_name) => {},
            result = task => panic!(
                "command on '{node_id}' returned before its durable admission pause: {result:?}"
            ),
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!("durable command admission pause on '{node_id}' was not reached: {error}")
    });
}

#[when(expr = "the durable command admission pause on node {string} is released")]
async fn when_command_durable_admission_pause_is_released(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .release_command_durable_admission_pause(&crate::common::cluster::node_name(&node_id));
}

#[given(expr = "relocation publication for domain {string} pauses after planning")]
async fn given_relocation_publication_pauses_after_planning(
    world: &mut ScenarioWorld,
    domain: String,
) {
    let domain = expand_placeholders(world, &domain);
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    world.fault_injection.pause_relocation_publication(domain);
}

#[then(expr = "the relocation publication pause for domain {string} is reached")]
async fn then_relocation_publication_pause_is_reached(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    let fault_injection = world.fault_injection.clone();
    let task = world
        .background_nspl
        .as_mut()
        .verified("the preceding step started a background relocation");
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            () = fault_injection.wait_for_relocation_publication_pause(&domain) => {},
            result = task => panic!(
                "relocation for domain '{domain}' returned before its publication pause: \
                 {result:?}"
            ),
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!("relocation publication pause for domain '{domain}' was not reached: {error}")
    });
}

#[when(expr = "the relocation publication pause for domain {string} is released")]
async fn when_relocation_publication_pause_is_released(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    world
        .fault_injection
        .release_relocation_publication_pause(&domain);
}

#[given(expr = "command response delivery on node {string} pauses after execution")]
async fn given_command_response_delivery_pause(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .pause_command_response_delivery_on(crate::common::cluster::node_name(&node_id));
}

#[then(expr = "the command response delivery pause on node {string} is reached")]
async fn then_command_response_delivery_pause_is_reached(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let node_name = crate::common::cluster::node_name(&node_id);
    let fault_injection = world.fault_injection.clone();
    tokio::select! {
        () = fault_injection.wait_for_command_response_delivery_pause(&node_name) => {},
        () = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("command response delivery pause on node '{node_id}' was not reached");
        }
    }
}

#[when(expr = "the command response delivery pause on node {string} is released")]
async fn when_command_response_delivery_pause_is_released(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .release_command_response_delivery_pause(&crate::common::cluster::node_name(&node_id));
}

#[given(expr = "resource installation on node {string} pauses before promotion")]
async fn given_resource_installation_pause(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .pause_resource_installation_on(crate::common::cluster::node_name(&node_id));
}

#[given(expr = "resource installation on node {string} fails before promotion")]
async fn given_resource_installation_failure(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .fail_next_resource_installation_on(crate::common::cluster::node_name(&node_id));
}

#[given(expr = "the next HTTPS listener installation on node {string} fails")]
async fn given_https_listener_installation_failure(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .fail_next_https_listener_installation_on(crate::common::cluster::node_name(&node_id));
}

#[then(expr = "the resource installation pause on node {string} is reached")]
async fn then_resource_installation_pause_is_reached(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    let node_name = crate::common::cluster::node_name(&node_id);
    tokio::time::timeout(
        Duration::from_secs(30),
        world
            .fault_injection
            .wait_for_resource_installation_pause(&node_name),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("resource installation pause on '{node_id}' was not reached: {error}")
    });
    let background = world
        .background_nspl
        .as_ref()
        .verified("the preceding step started a background resource upload");
    assert!(
        !background.is_finished(),
        "resource upload returned before '{node_id}' installed its archive"
    );
}

#[when(expr = "the resource installation pause on node {string} is released")]
async fn when_resource_installation_pause_is_released(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .release_resource_installation_pause(&crate::common::cluster::node_name(&node_id));
}

#[given(expr = "transaction commit on node {string} pauses after {int} statement")]
async fn given_transaction_commit_pause(
    world: &mut ScenarioWorld,
    node_id: String,
    completed_statements: usize,
) {
    let node_id = expand_placeholders(world, &node_id);
    world.fault_injection.pause_transaction_commit_after(
        crate::common::cluster::node_name(&node_id),
        world.domain.clone(),
        completed_statements,
    );
}

#[given(expr = "the entity gate for domain {string} pauses after engagement")]
async fn given_entity_gate_pause(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world.fault_injection.pause_entity_gate(domain);
}

#[given(expr = "ingestor {string} pauses inside dispatch")]
async fn given_ingestor_dispatch_pause(world: &mut ScenarioWorld, ingestor: String) {
    let ingestor = world.ingestor_dispatch_ref(&ingestor);
    world.fault_injection.pause_ingestor_dispatch(ingestor);
}

#[then(expr = "ingestor {string} reaches the dispatch pause")]
async fn then_ingestor_dispatch_pause_is_reached(world: &mut ScenarioWorld, ingestor: String) {
    let ingestor = world.ingestor_dispatch_ref(&ingestor);
    let reached = tokio::time::timeout(
        Duration::from_secs(10),
        world
            .fault_injection
            .wait_for_ingestor_dispatch_pause(&ingestor),
    )
    .await;
    assert!(
        reached.is_ok(),
        "the scenario's ingestor did not reach its armed dispatch pause within ten seconds"
    );
}

#[when(expr = "ingestor {string} leaves the dispatch pause")]
async fn when_ingestor_dispatch_pause_is_released(world: &mut ScenarioWorld, ingestor: String) {
    let ingestor = world.ingestor_dispatch_ref(&ingestor);
    world
        .fault_injection
        .release_ingestor_dispatch_pause(&ingestor);
}

#[given(
    expr = "the entity gate response from node {string} for domain {string} pauses after \
            engagement"
)]
async fn given_entity_gate_response_pause(
    world: &mut ScenarioWorld,
    node_id: String,
    domain: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let domain = expand_placeholders(world, &domain);
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    world
        .fault_injection
        .pause_entity_gate_response_on(crate::common::cluster::node_name(&node_id), domain);
}

#[given(expr = "remote relay admission for domain {string} is paused")]
async fn given_remote_relay_admission_pause(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world.fault_injection.pause_remote_relay_admission(domain);
}

#[given(expr = "remote relay admission for branch {string} in domain {string} is paused")]
async fn given_remote_relay_branch_admission_pause(
    world: &mut ScenarioWorld,
    branch: String,
    domain: String,
) {
    let branch = expand_placeholders(world, &branch);
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .pause_remote_relay_admission_for_branch(domain, Some(branch));
}

#[then(expr = "the remote relay admission pause for domain {string} is reached")]
async fn then_remote_relay_admission_pause_is_reached(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    tokio::time::timeout(
        Duration::from_secs(10),
        world
            .fault_injection
            .wait_for_remote_relay_admission_pause(&domain),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("remote relay admission pause for domain '{domain}' was not reached: {error}")
    });
}

#[then(expr = "the remote relay admission pause for branch {string} in domain {string} is reached")]
async fn then_remote_relay_branch_admission_pause_is_reached(
    world: &mut ScenarioWorld,
    branch: String,
    domain: String,
) {
    let branch = expand_placeholders(world, &branch);
    let domain = expand_placeholders(world, &domain);
    tokio::time::timeout(
        Duration::from_secs(10),
        world
            .fault_injection
            .wait_for_remote_relay_admission_pause_for_branch(&domain, Some(&branch)),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "remote relay admission pause for domain '{domain}' and branch '{branch}' was not \
             reached: {error}"
        )
    });
}

#[when(expr = "the remote relay admission pause for domain {string} is released")]
async fn when_remote_relay_admission_pause_is_released(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .release_remote_relay_admission_pause(&domain);
}

#[when(
    expr = "the remote relay admission pause for branch {string} in domain {string} is released"
)]
async fn when_remote_relay_branch_admission_pause_is_released(
    world: &mut ScenarioWorld,
    branch: String,
    domain: String,
) {
    let branch = expand_placeholders(world, &branch);
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .release_remote_relay_admission_pause_for_branch(&domain, Some(&branch));
}

#[given(expr = "ownership handoff for domain {string} pauses after preparation")]
async fn given_ownership_handoff_preparation_pause(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .pause_ownership_handoff_after_preparation(domain);
}

#[given(expr = "ownership handoff for domain {string} pauses before its prepare response")]
async fn given_ownership_handoff_prepare_response_pause(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .pause_ownership_handoff_prepare_response(domain);
}

/// How long a gated cluster operation is given to engage its entity gates.
///
/// The wait also ends the moment the command it gates finishes, so a command that failed before
/// engaging reports its own error rather than this deadline. Only genuine slowness can reach the
/// deadline, which is why it is generous: engaging a gate on a three-node cluster runs a schedule
/// through consensus while the rest of the suite competes for the machine.
const ENTITY_GATE_PAUSE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a delivery-delay assertion waits beyond the delay it requires. The delay itself
/// is the cadence under test; this is the liveness budget on top of it.
const SUBSCRIPTION_DELIVERY_BUDGET: Duration = Duration::from_secs(30);

#[then(expr = "the entity gate pause for domain {string} is reached")]
async fn then_entity_gate_pause_is_reached(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    let fault_injection = world.fault_injection.clone();
    let deadline = Instant::now() + ENTITY_GATE_PAUSE_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        if tokio::time::timeout(
            Duration::from_millis(50),
            fault_injection.wait_for_entity_gate_pause(&domain),
        )
        .await
        .is_ok()
        {
            return;
        }
        // A gated command that already returned will never engage a gate, so report what it did
        // instead of waiting out a deadline it can no longer meet.
        if let Some(background) = world.background_nspl.as_ref()
            && background.is_finished()
        {
            let outcome = world
                .background_nspl
                .take()
                .verified("the branch above already observed the background execution")
                .await
                .expect("background NSPL task must not panic");
            panic!(
                "entity gate did not reach the armed pause for domain '{domain}': the gated \
                 command finished first with {outcome:?}"
            );
        }
        assert!(
            Instant::now() < deadline,
            "entity gate did not reach the armed pause for domain '{domain}' within \
             {ENTITY_GATE_PAUSE_TIMEOUT:?}; the gated command is still running"
        );
    }
}

#[when(expr = "the entity gate pause for domain {string} is released")]
async fn when_entity_gate_pause_is_released(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world.fault_injection.release_entity_gate_pause(&domain);
}

#[then(expr = "the entity gate response pause from node {string} for domain {string} is reached")]
async fn then_entity_gate_response_pause_is_reached(
    world: &mut ScenarioWorld,
    node_id: String,
    domain: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let domain = expand_placeholders(world, &domain);
    let parsed_domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    tokio::time::timeout(
        ENTITY_GATE_PAUSE_TIMEOUT,
        world.fault_injection.wait_for_entity_gate_response_pause(
            &crate::common::cluster::node_name(&node_id),
            &parsed_domain,
        ),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "entity gate response pause from node '{node_id}' for domain '{domain}' was not \
             reached: {error}"
        )
    });
}

#[when(expr = "the entity gate response pause from node {string} for domain {string} is released")]
async fn when_entity_gate_response_pause_is_released(
    world: &mut ScenarioWorld,
    node_id: String,
    domain: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let domain = expand_placeholders(world, &domain);
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    world
        .fault_injection
        .release_entity_gate_response_pause(&crate::common::cluster::node_name(&node_id), &domain);
}

#[then(expr = "the ownership handoff preparation pause for domain {string} is reached")]
async fn then_ownership_handoff_preparation_pause_is_reached(
    world: &mut ScenarioWorld,
    domain: String,
) {
    let domain = expand_placeholders(world, &domain);
    let fault_injection = world.fault_injection.clone();
    let deadline = Instant::now() + ENTITY_GATE_PAUSE_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        if tokio::time::timeout(
            Duration::from_millis(50),
            fault_injection.wait_for_ownership_handoff_preparation_pause(&domain),
        )
        .await
        .is_ok()
        {
            return;
        }
        if let Some(background) = world.background_nspl.as_ref()
            && background.is_finished()
        {
            let outcome = world
                .background_nspl
                .take()
                .verified("the branch above already observed the background execution")
                .await
                .expect("background NSPL task must not panic");
            panic!(
                "ownership handoff did not reach the armed preparation pause for domain \
                 '{domain}': the command finished first with {outcome:?}"
            );
        }
        assert!(
            Instant::now() < deadline,
            "ownership handoff did not reach the armed preparation pause for domain '{domain}' \
             within {ENTITY_GATE_PAUSE_TIMEOUT:?}; the command is still running"
        );
    }
}

#[when(expr = "the ownership handoff preparation pause for domain {string} is released")]
async fn when_ownership_handoff_preparation_pause_is_released(
    world: &mut ScenarioWorld,
    domain: String,
) {
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .release_ownership_handoff_preparation_pause(&domain);
}

#[then(expr = "the ownership handoff prepare response pause for domain {string} is reached")]
async fn then_ownership_handoff_prepare_response_pause_is_reached(
    world: &mut ScenarioWorld,
    domain: String,
) {
    let domain = expand_placeholders(world, &domain);
    tokio::time::timeout(
        ENTITY_GATE_PAUSE_TIMEOUT,
        world
            .fault_injection
            .wait_for_ownership_handoff_prepare_response_pause(&domain),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "ownership handoff prepare response pause for domain '{domain}' was not reached: \
             {error}"
        )
    });
}

#[when(expr = "the ownership handoff prepare response pause for domain {string} is released")]
async fn when_ownership_handoff_prepare_response_pause_is_released(
    world: &mut ScenarioWorld,
    domain: String,
) {
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .release_ownership_handoff_prepare_response_pause(&domain);
}

#[given(expr = "domain clock progress for domain {string} is paused before delivery")]
async fn given_domain_clock_progress_is_paused(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world.fault_injection.pause_domain_clock_progress(domain);
}

#[given(
    expr = "domain clock progress for domain {string} on node {string} is paused before delivery"
)]
async fn given_domain_clock_progress_is_paused_on_node(
    world: &mut ScenarioWorld,
    domain: String,
    node_id: String,
) {
    let domain = expand_placeholders(world, &domain);
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .pause_domain_clock_progress_on(domain, crate::common::cluster::node_name(&node_id));
}

#[then(
    expr = "within {string} domain clock progress for domain {string} reaches the delivery pause"
)]
async fn then_domain_clock_progress_reaches_pause(
    world: &mut ScenarioWorld,
    duration: String,
    domain: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let domain = expand_placeholders(world, &domain);
    tokio::time::timeout(
        duration,
        world
            .fault_injection
            .wait_for_domain_clock_progress_pause(&domain),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("domain clock progress for '{domain}' did not reach its delivery pause: {error}")
    });
}

#[then(
    expr = "within {string} domain clock progress for domain {string} on node {string} reaches \
            the delivery pause"
)]
async fn then_domain_clock_progress_reaches_pause_on_node(
    world: &mut ScenarioWorld,
    duration: String,
    domain: String,
    node_id: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    world
        .wait_for_domain_clock_progress_pause_on(duration, &domain, &node_id)
        .await;
}

#[then(
    expr = "domain clock progress for domain {string} on node {string} reaches the delivery pause \
            within the authority observation budget"
)]
async fn then_domain_clock_progress_reaches_pause_within_authority_observation_budget(
    world: &mut ScenarioWorld,
    domain: String,
    node_id: String,
) {
    world
        .wait_for_domain_clock_progress_pause_on(
            DOMAIN_CLOCK_AUTHORITY_OBSERVATION_TIMEOUT,
            &domain,
            &node_id,
        )
        .await;
}

#[when(expr = "domain clock progress for domain {string} resumes")]
async fn when_domain_clock_progress_resumes(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    tokio::time::timeout(
        Duration::from_secs(10),
        world.fault_injection.release_domain_clock_progress(&domain),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("domain clock progress for '{domain}' was not delivered after release: {error}")
    });
}

#[when(expr = "domain clock progress for domain {string} on node {string} resumes")]
async fn when_domain_clock_progress_resumes_on_node(
    world: &mut ScenarioWorld,
    domain: String,
    node_id: String,
) {
    let domain = expand_placeholders(world, &domain);
    let node_id = expand_placeholders(world, &node_id);
    tokio::time::timeout(
        Duration::from_secs(5),
        world.fault_injection.release_domain_clock_progress_on(
            &domain,
            &crate::common::cluster::node_name(&node_id),
        ),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "domain clock progress for '{domain}' on '{node_id}' was not delivered after release: \
             {error}"
        )
    });
}

#[when(expr = "physical time passes for {string}")]
async fn when_physical_time_passes(_world: &mut ScenarioWorld, duration: String) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    tokio::time::sleep(duration).await;
}

#[then(expr = "the transaction commit pause on node {string} after {int} statement is reached")]
async fn then_transaction_commit_pause_is_reached(
    world: &mut ScenarioWorld,
    node_id: String,
    completed_statements: usize,
) {
    let node_id = expand_placeholders(world, &node_id);
    tokio::time::timeout(
        Duration::from_secs(10),
        world.fault_injection.wait_for_transaction_commit_pause(
            &crate::common::cluster::node_name(&node_id),
            &world.domain,
            completed_statements,
        ),
    )
    .await
    .expect("transaction commit did not reach the armed pause");
}

#[when(expr = "the transaction commit pause on node {string} after {int} statement is released")]
async fn when_transaction_commit_pause_is_released(
    world: &mut ScenarioWorld,
    node_id: String,
    completed_statements: usize,
) {
    let node_id = expand_placeholders(world, &node_id);
    world.fault_injection.release_transaction_commit_pause(
        &crate::common::cluster::node_name(&node_id),
        &world.domain,
        completed_statements,
    );
}

#[given("transaction commit admission on the leader node pauses before execution")]
async fn given_transaction_commit_admission_pause(world: &mut ScenarioWorld) {
    let leader = current_leader_node(world).await;
    world.placeholders.insert(
        "transaction_commit_admission_node".to_string(),
        leader.clone(),
    );
    world.fault_injection.pause_transaction_commit_after(
        crate::common::cluster::node_name(&leader),
        world.domain.clone(),
        0,
    );
}

#[then("the transaction commit admission pause on the leader node is reached")]
async fn then_transaction_commit_admission_pause_is_reached(world: &mut ScenarioWorld) {
    let leader = world
        .placeholders
        .get("transaction_commit_admission_node")
        .cloned()
        .verified("the preceding admission-pause step saved its leader");
    tokio::time::timeout(
        Duration::from_secs(10),
        world.fault_injection.wait_for_transaction_commit_pause(
            &crate::common::cluster::node_name(&leader),
            &world.domain,
            0,
        ),
    )
    .await
    .expect("transaction commit did not reach the admission pause");
}

#[when("the transaction commit admission pause on the leader node is released")]
async fn when_transaction_commit_admission_pause_is_released(world: &mut ScenarioWorld) {
    let leader = world
        .placeholders
        .get("transaction_commit_admission_node")
        .cloned()
        .verified("the preceding admission-pause step saved its leader");
    world.fault_injection.release_transaction_commit_pause(
        &crate::common::cluster::node_name(&leader),
        &world.domain,
        0,
    );
}

#[given(expr = "consensus storage on the leader fails {word} committing domain {string}")]
async fn given_consensus_storage_failure(
    world: &mut ScenarioWorld,
    boundary: String,
    domain: String,
) {
    let leader = current_leader_node(world).await;
    world
        .placeholders
        .insert("storage_node".into(), leader.clone());
    world.fault_injection.fail_consensus_storage(
        &crate::common::cluster::node_name(&leader),
        format!("put-domain:{domain}"),
        match boundary.as_str() {
            "before" => nervix_consensus::StorageBoundary::BeforeCommit,
            "after" => nervix_consensus::StorageBoundary::AfterSync,
            _ => panic!("the fixture names a before or after storage boundary"),
        },
    );
}

#[then(expr = "the storage-failed node has no published domain {string}")]
async fn then_failed_consensus_domain_is_unpublished(world: &mut ScenarioWorld, domain: String) {
    use meticulous::OptionExt as _;
    let node = world
        .placeholders
        .get("storage_node")
        .verified("the preceding storage fault selected this node");
    let observer = world
        .fault_injection
        .consensus_observer(&crate::common::cluster::node_name(node));
    let domain = nervix_models::DomainName::try_from(domain.as_str())
        .assured("the scenario uses an identifier-shaped domain name");
    assert!(
        observer.current_domain(&domain).await.is_none(),
        "failed state application published the domain before durable success"
    );
}

#[when("the cluster is restarted")]
async fn when_the_cluster_is_restarted(world: &mut ScenarioWorld) {
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    world.last_subscription_payload = None;
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world
        .cluster_mut()
        .restart()
        .await
        .expect("failed to restart cluster");
}

#[then(expr = "the last cluster operation completes within {string}")]
async fn then_last_cluster_operation_completes_within(world: &mut ScenarioWorld, duration: String) {
    let max_duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let elapsed = world
        .last_cluster_operation_elapsed
        .expect("a timed cluster operation must run before assertion");
    assert!(
        elapsed <= max_duration,
        "expected cluster operation to complete within {:?}, took {:?}",
        max_duration,
        elapsed
    );
}

#[then(expr = "the last cluster operation takes at least {string}")]
async fn then_last_cluster_operation_takes_at_least(world: &mut ScenarioWorld, duration: String) {
    let min_duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let elapsed = world
        .last_cluster_operation_elapsed
        .expect("a timed cluster operation must run before assertion");
    assert!(
        elapsed >= min_duration,
        "expected cluster operation to take at least {min_duration:?}, took {elapsed:?}"
    );
}

#[then(expr = "node {string} reports that its last shutdown passed its deadline")]
async fn then_node_reports_that_its_last_shutdown_passed_its_deadline(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let Some(failure) = world.cluster().node_run_failure(&node_id) else {
        panic!("node '{node_id}' stopped without reporting that its shutdown deadline passed");
    };
    assert!(
        failure.contains("did not finish before its shutdown deadline"),
        "node '{node_id}' stopped with an unrelated failure: {failure}"
    );
}

#[then(expr = "node {string} reports that its last shutdown finished before its deadline")]
async fn then_node_reports_that_its_last_shutdown_finished_before_its_deadline(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    if let Some(failure) = world.cluster().node_run_failure(&node_id) {
        panic!("node '{node_id}' did not finish its shutdown cleanly: {failure}");
    }
}

#[then(expr = "the last authentication attempts take at least {string}")]
async fn then_last_authentication_attempts_take_at_least(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let min_duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let elapsed = world
        .last_auth_attempts_elapsed
        .expect("an authentication attempt step must run first");
    assert!(
        elapsed >= min_duration,
        "expected authentication attempts to take at least {:?}, took {:?}",
        min_duration,
        elapsed
    );
}

#[when(expr = "the next schedule publication for domain {string} fails")]
async fn when_next_schedule_publication_fails(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world
        .cluster()
        .fail_next_schedule_publication_on_all_nodes(&domain);
}

#[when(expr = "emitter {string} enters fault mode")]
async fn when_emitter_enters_fault_mode(world: &mut ScenarioWorld, emitter: String) {
    let emitter = expand_placeholders(world, &emitter);
    world.cluster().fail_emitter_on_all_nodes(&emitter);
}

#[when(expr = "emitter {string} enters stall mode")]
async fn when_emitter_enters_stall_mode(world: &mut ScenarioWorld, emitter: String) {
    let emitter = expand_placeholders(world, &emitter);
    world.cluster().stall_emitter_on_all_nodes(&emitter);
}

#[when(expr = "emitter {string} leaves fault mode")]
#[then(expr = "emitter {string} leaves fault mode")]
#[when(expr = "emitter {string} leaves stall mode")]
#[then(expr = "emitter {string} leaves stall mode")]
async fn when_emitter_leaves_fault_mode(world: &mut ScenarioWorld, emitter: String) {
    let emitter = expand_placeholders(world, &emitter);
    world.cluster().clear_emitter_fault_on_all_nodes(&emitter);
}

#[when(expr = "sink client for emitter {string} enters unavailable fault mode")]
async fn when_sink_client_enters_unavailable_fault_mode(
    world: &mut ScenarioWorld,
    emitter: String,
) {
    let emitter = expand_placeholders(world, &emitter);
    world
        .cluster()
        .fail_sink_client_unavailable_on_all_nodes(&emitter);
}

#[when(expr = "sink client for emitter {string} leaves fault mode")]
#[then(expr = "sink client for emitter {string} leaves fault mode")]
async fn when_sink_client_leaves_fault_mode(world: &mut ScenarioWorld, emitter: String) {
    let emitter = expand_placeholders(world, &emitter);
    world
        .cluster()
        .clear_sink_client_fault_on_all_nodes(&emitter);
}

#[when(expr = "ingestor {string} enters fault mode")]
async fn when_ingestor_enters_fault_mode(world: &mut ScenarioWorld, ingestor: String) {
    let ingestor = expand_placeholders(world, &ingestor);
    world.cluster().fail_ingestor_on_all_nodes(&ingestor);
}

#[when(expr = "ingestor {string} leaves fault mode")]
async fn when_ingestor_leaves_fault_mode(world: &mut ScenarioWorld, ingestor: String) {
    let ingestor = expand_placeholders(world, &ingestor);
    world.cluster().clear_ingestor_fault_on_all_nodes(&ingestor);
}

#[given(expr = "node {string} eventually reports leader {string}")]
#[then(expr = "node {string} eventually reports leader {string}")]
async fn then_node_eventually_reports_leader(
    world: &mut ScenarioWorld,
    node_id: String,
    leader_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let leader_id = expand_placeholders(world, &leader_id);
    world
        .cluster()
        .wait_for_leader(&node_id, Some(&leader_id))
        .await
        .expect("leader did not converge");
}

#[then(expr = "node {string} eventually reports a leader other than {string}")]
async fn then_node_eventually_reports_other_leader(
    world: &mut ScenarioWorld,
    node_id: String,
    old_leader_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let old_leader_id = expand_placeholders(world, &old_leader_id);
    world
        .cluster()
        .wait_for_leader_not(&node_id, &old_leader_id)
        .await
        .expect("new leader was not elected");
}

#[then(expr = "node {string} eventually observes a stable leader")]
async fn then_node_eventually_observes_stable_leader(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster()
        .wait_for_any_leader(&node_id)
        .await
        .expect("leader did not appear");
}

#[then(expr = "node {string} eventually reports raft state {string}")]
async fn then_node_eventually_reports_raft_state(
    world: &mut ScenarioWorld,
    node_id: String,
    expected_state: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster()
        .wait_for_raft_state(&node_id, &expected_state)
        .await
        .expect("raft state did not converge");
}

#[then(expr = "node {string} eventually reports raft voters {string}")]
async fn then_node_eventually_reports_voters(
    world: &mut ScenarioWorld,
    node_id: String,
    expected: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let voters = expected
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    world
        .cluster()
        .wait_for_voters(&node_id, &voters)
        .await
        .expect("voter set did not converge");
}

#[then(expr = "Kafka consumer group {string} eventually has {int} consumers")]
async fn then_kafka_consumer_group_eventually_has_consumers(
    world: &mut ScenarioWorld,
    group: String,
    expected: usize,
) {
    let group = expand_placeholders(world, &group);
    world
        .cluster()
        .wait_for_kafka_consumer_group_members(&group, expected)
        .await
        .expect("kafka consumer group did not reach expected member count");
}

#[then(
    expr = "within {string} Kafka consumer group {string} next offset for topic {string} \
            partition {int} is {string}"
)]
async fn then_kafka_consumer_group_next_offset_is(
    world: &mut ScenarioWorld,
    duration: String,
    group: String,
    topic: String,
    partition: i32,
    condition: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let group = expand_placeholders(world, &group);
    let topic = expand_placeholders(world, &topic);
    let condition = expand_placeholders(world, &condition);
    let (should_reach, threshold) = if let Some(threshold) = condition.strip_prefix("at least ") {
        (true, threshold)
    } else if let Some(threshold) = condition.strip_prefix("below ") {
        (false, threshold)
    } else {
        panic!("Kafka offset condition must be 'at least <n>' or 'below <n>', got {condition:?}");
    };
    let threshold = threshold
        .parse::<i64>()
        .expect("Kafka offset threshold must be an integer");
    world
        .cluster()
        .assert_kafka_consumer_group_next_offset(
            &group,
            &topic,
            partition,
            threshold,
            should_reach,
            duration,
        )
        .await
        .expect("Kafka consumer group committed-offset assertion failed");
}

#[then(expr = "RabbitMQ queue {string} eventually has {int} consumers")]
async fn then_rabbitmq_queue_eventually_has_consumers(
    world: &mut ScenarioWorld,
    queue: String,
    expected: usize,
) {
    let queue = expand_placeholders(world, &queue);
    world
        .cluster()
        .wait_for_rabbitmq_queue_consumers(&queue, expected)
        .await
        .expect("rabbitmq queue did not reach expected consumer count");
}

#[then(expr = "Redis channel {string} eventually has {int} subscribers")]
async fn then_redis_channel_eventually_has_subscribers(
    world: &mut ScenarioWorld,
    channel: String,
    expected: usize,
) {
    let channel = expand_placeholders(world, &channel);
    world
        .cluster()
        .wait_for_redis_channel_subscribers(&channel, expected)
        .await
        .expect("redis channel did not reach expected subscriber count");
}

fn docstring(step: &Step) -> &str {
    step.docstring
        .as_deref()
        .expect("step docstring is required")
}

fn expand_placeholders(world: &ScenarioWorld, input: &str) -> String {
    let mut output = input
        .replace("{{test_id}}", &world.test_id)
        .replace("{{domain}}", &world.domain)
        .replace("{{zeromq_ingest_addr}}", &world.zeromq_ingest_addr)
        .replace("{{zeromq_emit_addr}}", &world.zeromq_emit_addr)
        .replace("{{syslog_ingest_addr}}", &world.syslog_ingest_addr)
        .replace("{{syslog_emit_addr}}", &world.syslog_emit_addr)
        .replace("{{syslog_pri}}", "<34>");
    for (key, value) in &world.placeholders {
        output = output.replace(&format!("{{{{{key}}}}}"), value);
    }
    output
}

fn command_execution_reference(world: &mut ScenarioWorld, input: &str) -> String {
    let expanded = expand_placeholders(world, input);
    let is_uuid_v7 = nervix_models::CommandExecutionReference::parse(expanded.clone())
        .is_ok_and(|reference| reference.retry_issued_at().is_ok());
    if is_uuid_v7 {
        return expanded;
    }
    world
        .command_execution_references
        .entry(expanded)
        .or_insert_with(|| Uuid::now_v7().to_string())
        .clone()
}

fn encode_http_payload_for_codec(
    wire_format: &str,
    payload: &str,
    avro_field_order: &[String],
    avro_optional_fields: &BTreeSet<String>,
) -> Vec<u8> {
    let json_value = serde_json::from_str::<serde_json::Value>(payload).unwrap_or_else(|error| {
        panic!("http payload must be valid JSON for {wire_format}: {error}")
    });

    match wire_format.to_ascii_uppercase().as_str() {
        "JSON" => serde_json::to_vec(&json_value)
            .unwrap_or_else(|error| panic!("failed to encode JSON payload: {error}")),
        "AVRO" => encode_avro_http_payload(&json_value, avro_field_order, avro_optional_fields),
        "CBOR" => {
            let mut encoded = Vec::new();
            ciborium::into_writer(&json_value, &mut encoded)
                .unwrap_or_else(|error| panic!("failed to encode CBOR payload: {error}"));
            encoded
        }
        other => panic!("unsupported codec wire format '{other}'"),
    }
}

fn encode_avro_http_payload(
    json_value: &serde_json::Value,
    field_order: &[String],
    optional_fields: &BTreeSet<String>,
) -> Vec<u8> {
    use apache_avro::{Schema as AvroSchema, to_avro_datum, types::Value as AvroValue};

    let serde_json::Value::Object(object) = json_value else {
        panic!("avro http payload must be a JSON object");
    };

    let mut schema_fields = Vec::with_capacity(object.len());
    let mut value_fields = Vec::with_capacity(object.len());
    let mut ordered_fields = Vec::new();
    for name in field_order {
        if let Some(value) = object.get(name) {
            ordered_fields.push((name.as_str(), value));
        }
    }
    for (name, value) in object {
        if !field_order.iter().any(|field| field == name) {
            ordered_fields.push((name.as_str(), value));
        }
    }

    for (name, value) in ordered_fields {
        let (schema_ty, avro_value) = avro_field_from_json(value);
        let schema_ty = if schema_ty.starts_with('{') {
            schema_ty
        } else {
            format!(r#""{schema_ty}""#)
        };
        let (schema_ty, avro_value) = if optional_fields.contains(name) {
            (
                format!(r#"["null",{schema_ty}]"#),
                AvroValue::Union(1, Box::new(avro_value)),
            )
        } else {
            (schema_ty, avro_value)
        };
        schema_fields.push(format!(r#"{{"name":"{name}","type":{schema_ty}}}"#));
        value_fields.push((name.to_string(), avro_value));
    }

    let schema_json = format!(
        r#"{{"type":"record","name":"HttpPayload","fields":[{}]}}"#,
        schema_fields.join(",")
    );
    let schema = AvroSchema::parse_str(&schema_json)
        .unwrap_or_else(|error| panic!("failed to build avro payload schema: {error}"));
    to_avro_datum(&schema, AvroValue::Record(value_fields))
        .unwrap_or_else(|error| panic!("failed to encode avro payload: {error}"))
}

fn avro_field_from_json(value: &serde_json::Value) -> (String, apache_avro::types::Value) {
    use apache_avro::types::Value as AvroValue;

    match value {
        serde_json::Value::Bool(v) => ("boolean".to_string(), AvroValue::Boolean(*v)),
        serde_json::Value::Number(v) => {
            if let Some(integer) = v.as_i64() {
                ("long".to_string(), AvroValue::Long(integer))
            } else if let Some(float) = v.as_f64() {
                ("double".to_string(), AvroValue::Double(float))
            } else {
                panic!("unsupported avro numeric value {v}");
            }
        }
        serde_json::Value::String(v) => ("string".to_string(), AvroValue::String(v.clone())),
        serde_json::Value::Null => ("null".to_string(), AvroValue::Null),
        serde_json::Value::Array(values) => {
            let Some(first) = values.first() else {
                panic!("avro http payload arrays must not be empty");
            };
            let (item_schema, _) = avro_array_item_from_json(first);
            let avro_values = values
                .iter()
                .map(|value| {
                    let (schema, avro_value) = avro_array_item_from_json(value);
                    assert_eq!(
                        schema, item_schema,
                        "avro http payload arrays must have homogeneous item types"
                    );
                    avro_value
                })
                .collect::<Vec<_>>();
            (
                format!(r#"{{"type":"array","items":"{item_schema}"}}"#),
                AvroValue::Array(avro_values),
            )
        }
        serde_json::Value::Object(_) => {
            panic!("avro http payload only supports flat scalar or array fields")
        }
    }
}

fn avro_array_item_from_json(value: &serde_json::Value) -> (String, apache_avro::types::Value) {
    use apache_avro::types::Value as AvroValue;

    match value {
        serde_json::Value::Number(v) if v.as_f64().is_some() => (
            "float".to_string(),
            AvroValue::Float(v.as_f64().expect("checked above").approx_into()),
        ),
        other => avro_field_from_json(other),
    }
}

fn http_content_type_for_codec(wire_format: &str) -> &'static str {
    match wire_format.to_ascii_uppercase().as_str() {
        "JSON" => "application/json",
        "AVRO" => "application/avro",
        "CBOR" => "application/cbor",
        other => panic!("unsupported codec wire format '{other}'"),
    }
}

fn jaq_native_payload_fixture(fixture: &str) -> (Vec<u8>, &'static str) {
    match fixture {
        "json_wrapped_notification" => (
            br#"{"payload":{"user_id":42,"payload":"aligned"}}"#.to_vec(),
            "application/json",
        ),
        "yaml_wrapped_notification" => (
            b"payload:\n  user_id: 42\n  payload: aligned\n".to_vec(),
            "application/yaml",
        ),
        "toml_wrapped_notification" => (
            b"[payload]\nuser_id = 42\npayload = \"aligned\"\n".to_vec(),
            "application/toml",
        ),
        "xml_wrapped_notification" => (
            b"<notification><user_id>42</user_id><payload>aligned</payload></notification>"
                .to_vec(),
            "application/xml",
        ),
        "cbor_wrapped_notification" => {
            let value = serde_json::json!({
                "payload": {
                    "user_id": 42,
                    "payload": "aligned"
                }
            });
            let mut encoded = Vec::new();
            ciborium::into_writer(&value, &mut encoded)
                .unwrap_or_else(|error| panic!("failed to encode CBOR fixture: {error}"));
            (encoded, "application/cbor")
        }
        "yaml_notification_stream" => (
            b"user_id: 41\npayload: first\n---\nuser_id: 42\npayload: second\n".to_vec(),
            "application/yaml",
        ),
        "cbor_notification_sequence" => {
            let mut encoded = Vec::new();
            for (user_id, payload) in [(41, "first"), (42, "second")] {
                let value = serde_json::json!({"user_id": user_id, "payload": payload});
                ciborium::into_writer(&value, &mut encoded)
                    .unwrap_or_else(|error| panic!("failed to encode CBOR fixture: {error}"));
            }
            (encoded, "application/cbor")
        }
        "xml_declared_notification_batch" => (
            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!-- partner feed -->\n\
              <notifications><notification><user_id>41</user_id><payload>first</payload>\
              </notification><notification><user_id>42</user_id><payload>second</payload>\
              </notification></notifications>\n"
                .to_vec(),
            "application/xml",
        ),
        other => panic!("unknown JAQ native payload fixture '{other}'"),
    }
}

fn protobuf_payload_fixture(fixture: &str) -> (Vec<u8>, &'static str) {
    match fixture {
        "notification" => (
            vec![
                0x08, 42, 0x12, 4, b'a', b'c', b'm', b'e', 0x1a, 7, b'a', b'l', b'i', b'g', b'n',
                b'e', b'd',
            ],
            "application/x-protobuf",
        ),
        other => panic!("unknown protobuf payload fixture '{other}'"),
    }
}

fn resource_directory_path(world: &ScenarioWorld, placeholder: &str) -> PathBuf {
    PathBuf::from(
        world
            .placeholders
            .get(placeholder)
            .unwrap_or_else(|| panic!("unknown resource directory placeholder '{placeholder}'")),
    )
}

fn resource_directory_ca_pem(world: &ScenarioWorld, placeholder: &str) -> String {
    std::fs::read_to_string(resource_directory_path(world, placeholder).join("ca.crt"))
        .unwrap_or_else(|error| {
            panic!("failed to read ca.crt from resource directory '{placeholder}': {error}")
        })
}

fn nspl_statements(input: &str) -> Vec<String> {
    if let Ok(statements) = nervix_nspl::client_statement::parse_client_statement_sources(input) {
        return statements
            .iter()
            .map(|statement| statement.source(input).to_string())
            .collect();
    }

    input
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(|statement| format!("{statement};"))
        .collect()
}

fn payload_matches_expected(actual: &str, expected: &str) -> bool {
    let expected = expected.trim();
    if let Ok(expected_json) = serde_json::from_str::<serde_json::Value>(expected)
        && let Ok(actual_json) = serde_json::from_str::<serde_json::Value>(actual.trim())
    {
        return actual_json == expected_json;
    }
    actual.contains(expected)
}

fn requires_persistent_session(commands: &str) -> bool {
    nspl_statements(commands).iter().any(|statement| {
        let normalized = statement.trim().to_ascii_uppercase();
        normalized.starts_with("CREATE SUBSCRIPTION ")
            || normalized == "BEGIN;"
            || normalized == "COMMIT;"
            || normalized == "REVERT;"
    })
}

fn command_updates_subscription_state(current: bool, command: &str) -> bool {
    let normalized = command.trim_start().to_ascii_uppercase();
    if normalized.starts_with("CREATE SUBSCRIPTION ") {
        return true;
    }
    if normalized.starts_with("DELETE SUBSCRIPTION ") {
        return false;
    }
    current
}

fn commands_update_subscription_state(current: bool, commands: &str) -> bool {
    nspl_statements(commands)
        .into_iter()
        .fold(current, |state, command| {
            command_updates_subscription_state(state, &command)
        })
}

fn record_avro_wire_optional_fields(world: &mut ScenarioWorld, commands: &str) {
    for statement in nspl_statements(commands) {
        let normalized = statement.trim_start().to_ascii_uppercase();
        if !normalized.starts_with("CREATE WIRE AVRO SCHEMA ") {
            continue;
        }
        let Some((_, fields)) = statement.split_once('(') else {
            continue;
        };
        let Some((fields, _)) = fields.rsplit_once(')') else {
            continue;
        };
        for field in fields.split(',') {
            let field = field.trim();
            if let Some(name) = field.split_whitespace().next() {
                world.avro_http_field_order.push(name.to_string());
                if field.to_ascii_uppercase().contains(" OPTIONAL") {
                    world.avro_http_optional_fields.insert(name.to_string());
                }
            }
        }
    }
}

fn record_mqtt_ingestors(world: &mut ScenarioWorld, commands: &str) {
    let Ok(parsed) = nervix_nspl::client_statement::parse_client_statement_sources(commands) else {
        return;
    };
    for statement in parsed {
        let nervix_nspl::client_statement::ClientStatement::Server(
            nervix_models::Statement::Create(create),
        ) = statement.statement
        else {
            continue;
        };
        let nervix_models::Model::Ingestor(ingestor) = *create.body else {
            continue;
        };
        let nervix_models::IngestSource::Mqtt { .. } = ingestor.source else {
            continue;
        };
        world
            .mqtt_ingestors_by_domain
            .entry(world.domain.clone())
            .or_default()
            .insert(ingestor.name.as_str().to_string());
    }
}

async fn execute_nspl_commands_on_node(
    world: &mut ScenarioWorld,
    node_id: &str,
    commands: &str,
) -> Result<TestSession, String> {
    record_avro_wire_optional_fields(world, commands);
    append_cucumber_log_line(&format!(
        "nspl commands on node {node_id}: {}",
        commands.replace('\n', "\\n")
    ));
    if !requires_persistent_session(commands) {
        for command in nspl_statements(commands) {
            append_cucumber_log_line(&format!("nspl command on node {node_id}: {command}"));
            let output = world
                .cluster()
                .run_command(node_id, &world.domain, &command)
                .await
                .map_err(|error| error.to_string())?;
            world.last_command_output = Some(output);
        }
        record_mqtt_ingestors(world, commands);
        return world
            .cluster()
            .open_session(node_id, &world.domain)
            .await
            .map_err(|error| error.to_string());
    }

    let mut session = world
        .cluster()
        .open_session(node_id, &world.domain)
        .await
        .map_err(|error| error.to_string())?;

    for command in nspl_statements(commands) {
        append_cucumber_log_line(&format!("nspl command on session {node_id}: {command}"));
        match session.run_command(&command).await {
            Ok(output) => world.last_command_output = Some(output),
            Err(error) => return Err(error.to_string()),
        }
    }

    record_mqtt_ingestors(world, commands);
    Ok(session)
}

#[given(expr = "the active domain is {string}")]
async fn given_the_active_domain_is(world: &mut ScenarioWorld, raw_domain: String) {
    world.domain = expand_placeholders(world, &raw_domain);
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    world.last_command_error = None;
    world.last_command_output = None;
}

async fn run_nspl_commands_on_node(
    world: &ScenarioWorld,
    node_id: &str,
    commands: &str,
) -> Result<String, String> {
    let mut last_output = String::new();
    for command in nspl_statements(commands) {
        last_output = world
            .cluster()
            .run_command(node_id, &world.domain, &command)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(last_output)
}

#[when(expr = "these NSPL commands are attempted on node {string}")]
async fn when_these_nspl_commands_are_attempted_on_node(
    world: &mut ScenarioWorld,
    node_id: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let node_id = expand_placeholders(world, &node_id);
    let commands = expand_placeholders(world, docstring(step));
    match run_nspl_commands_on_node(world, &node_id, &commands).await {
        Ok(output) => world.last_command_output = Some(output),
        Err(error) => world.last_command_error = Some(error),
    }
}

#[when("these NSPL commands begin executing in the background")]
async fn when_these_nspl_commands_begin_executing_in_the_background(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    assert!(
        world.background_nspl.is_none(),
        "a background NSPL execution is already active"
    );
    let commands = expand_placeholders(world, docstring(step));
    let statements = nspl_statements(&commands);
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .expect("failed to open background NSPL session");
    world.background_nspl = Some(AbortOnDropHandle::new(tokio::spawn(async move {
        let mut last_output = String::new();
        for command in statements {
            tokio::task::consume_budget().await;
            last_output = session
                .run_command(&command)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(last_output)
    })));
}

#[when("this NSPL command request begins executing in the background on the leader node")]
async fn when_command_request_begins_in_background(world: &mut ScenarioWorld, #[step] step: &Step) {
    assert!(
        world.background_command_result.is_none(),
        "a background command request is already active"
    );
    let query = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .unwrap_or_else(|error| panic!("failed to open the background command session: {error}"));
    world.background_command_result = Some(AbortOnDropHandle::new(tokio::spawn(async move {
        session.run_command_result(&query).await
    })));
}

#[when(
    expr = "this NSPL command request with execution reference {string} begins executing in the \
            background on the leader node"
)]
async fn when_referenced_command_request_begins_in_background(
    world: &mut ScenarioWorld,
    execution_reference: String,
    #[step] step: &Step,
) {
    assert!(
        world.background_command_result.is_none(),
        "a background command request is already active"
    );
    let query = expand_placeholders(world, docstring(step));
    let execution_reference = command_execution_reference(world, &execution_reference);
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .unwrap_or_else(|error| panic!("failed to open the background command session: {error}"));
    world.background_command_result = Some(AbortOnDropHandle::new(tokio::spawn(async move {
        session
            .run_command_result_with_reference(&query, &execution_reference)
            .await
    })));
}

#[when(
    expr = "an exact retry of this NSPL command request with execution reference {string} begins \
            in parallel on the leader node"
)]
async fn when_exact_command_retry_begins_in_parallel(
    world: &mut ScenarioWorld,
    execution_reference: String,
    #[step] step: &Step,
) {
    assert!(
        world.background_nspl.is_none(),
        "a background NSPL execution is already active"
    );
    let query = expand_placeholders(world, docstring(step));
    let execution_reference = command_execution_reference(world, &execution_reference);
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .unwrap_or_else(|error| panic!("failed to open the exact retry session: {error}"));
    world.background_nspl = Some(AbortOnDropHandle::new(tokio::spawn(async move {
        let result = session
            .run_command_result_with_reference(&query, &execution_reference)
            .await
            .map_err(|error| error.to_string())?;
        if result.succeeded() {
            Ok(result.message)
        } else {
            Err(result.message)
        }
    })));
}

#[when(
    expr = "the active session begins this NSPL command request with execution reference {string} \
            in the background"
)]
async fn when_active_session_referenced_command_begins_in_background(
    world: &mut ScenarioWorld,
    execution_reference: String,
    #[step] step: &Step,
) {
    assert!(
        world.background_command_result.is_none(),
        "a background command request is already active"
    );
    let query = expand_placeholders(world, docstring(step));
    let execution_reference = command_execution_reference(world, &execution_reference);
    let mut session = world
        .active_session
        .take()
        .verified("the preceding setup created an active session");
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    world.background_command_result = Some(AbortOnDropHandle::new(tokio::spawn(async move {
        session
            .run_command_result_with_reference(&query, &execution_reference)
            .await
    })));
}

#[when(
    expr = "the active session sends this NSPL command request with execution reference {string} \
            without reading its response"
)]
async fn when_active_session_sends_referenced_command_without_reading_response(
    world: &mut ScenarioWorld,
    execution_reference: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let query = expand_placeholders(world, docstring(step));
    let execution_reference = command_execution_reference(world, &execution_reference);
    let session = world
        .active_session
        .as_mut()
        .verified("the preceding setup created an active session");
    session
        .send_command_request_with_reference(&query, &execution_reference)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to send command without reading its response: {error}")
        });
}

#[when("the background command request connection is dropped")]
async fn when_background_command_request_connection_is_dropped(world: &mut ScenarioWorld) {
    let request = world
        .background_command_result
        .take()
        .verified("the preceding step started a background command request");
    drop(request);
    tokio::task::yield_now().await;
}

#[when(expr = "the background command caller deadline expires after {string}")]
async fn when_background_command_caller_deadline_expires(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let duration =
        humantime::parse_duration(&duration).assured("the scenario caller deadline is valid");
    let mut request = world
        .background_command_result
        .take()
        .verified("the preceding step started a background command request");
    let outcome = tokio::time::timeout(duration, &mut request).await;
    assert!(
        outcome.is_err(),
        "the command request returned before its caller deadline: {outcome:?}"
    );
    drop(request);
    tokio::task::yield_now().await;
}

#[when(
    expr = "this NSPL command request with execution reference {string} is executed on the leader \
            node"
)]
async fn when_referenced_command_request_is_executed_on_leader(
    world: &mut ScenarioWorld,
    execution_reference: String,
    #[step] step: &Step,
) {
    let execution_reference = command_execution_reference(world, &execution_reference);
    execute_command_request_with_reference_on_leader(world, &execution_reference, step).await;
}

#[when(
    expr = "this NSPL command request with an execution reference created {string} before now is \
            executed on the leader node"
)]
async fn when_command_request_with_reference_created_before_now_is_executed(
    world: &mut ScenarioWorld,
    age: String,
    #[step] step: &Step,
) {
    let age = humantime::parse_duration(&age).assured("the scenario reference age is a duration");
    let created_at = SystemTime::now()
        .checked_sub(age)
        .assured("the scenario reference age lies after the Unix epoch");
    let execution_reference = uuid_v7_created_at(created_at);
    execute_command_request_with_reference_on_leader(world, &execution_reference, step).await;
}

#[when(
    expr = "this NSPL command request with an execution reference created {string} after now is \
            executed on the leader node"
)]
async fn when_command_request_with_reference_created_after_now_is_executed(
    world: &mut ScenarioWorld,
    lead: String,
    #[step] step: &Step,
) {
    let lead =
        humantime::parse_duration(&lead).assured("the scenario reference lead is a duration");
    let created_at = SystemTime::now()
        .checked_add(lead)
        .assured("the scenario reference lead stays within the system clock range");
    let execution_reference = uuid_v7_created_at(created_at);
    execute_command_request_with_reference_on_leader(world, &execution_reference, step).await;
}

#[when(
    "this NSPL command request with an execution reference without a creation time is executed on \
     the leader node"
)]
async fn when_command_request_with_timeless_reference_is_executed(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    // A caller-selected reference is valid reference text but carries no UUIDv7 creation time.
    let execution_reference = format!("caller-selected-{}", world.test_id);
    execute_command_request_with_reference_on_leader(world, &execution_reference, step).await;
}

/// A UUIDv7 retry identity whose embedded creation time is `created_at`.
fn uuid_v7_created_at(created_at: SystemTime) -> String {
    let since_epoch = created_at
        .duration_since(UNIX_EPOCH)
        .assured("scenario reference times lie after the Unix epoch");
    let timestamp = uuid::Timestamp::from_unix(
        uuid::NoContext,
        since_epoch.as_secs(),
        since_epoch.subsec_nanos(),
    );
    Uuid::new_v7(timestamp).to_string()
}

async fn execute_command_request_with_reference_on_leader(
    world: &mut ScenarioWorld,
    execution_reference: &str,
    step: &Step,
) {
    let query = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .unwrap_or_else(|error| panic!("failed to open the resumed command session: {error}"));
    let result = session
        .run_command_result_with_reference(&query, execution_reference)
        .await
        .unwrap_or_else(|error| panic!("resumed command request failed: {error}"));
    if result.succeeded() {
        world.last_command_error = None;
        world.last_command_output = Some(result.message);
    } else {
        world.last_command_output = None;
        world.last_command_error = Some(result.message);
    }
}

#[then(expr = "command execution reference {string} is eventually reclaimed")]
async fn then_command_execution_reference_is_eventually_reclaimed(
    world: &mut ScenarioWorld,
    execution_reference: String,
) {
    let execution_reference = command_execution_reference(world, &execution_reference);
    let execution_reference = nervix_models::CommandExecutionReference::parse(execution_reference)
        .assured("scenario command references are valid UUIDv7 values");
    let leader = current_leader_node(world).await;
    let observer = world
        .fault_injection
        .consensus_observer(&crate::common::cluster::node_name(&leader));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        tokio::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "command execution reference '{execution_reference}' was not reclaimed"
        );
        if observer
            .current_command_execution(&execution_reference)
            .await
            .is_none()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the background command request is rejected with a redirect to node {string}")]
async fn then_background_command_request_redirects(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    let task = world
        .background_command_result
        .take()
        .unwrap_or_else(|| panic!("a background command request must be active"));
    let result = tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .unwrap_or_else(|error| panic!("background command request did not finish: {error}"))
        .unwrap_or_else(|error| panic!("background command request task failed: {error}"))
        .unwrap_or_else(|error| panic!("background command request transport failed: {error}"));
    let nervix_client_wire::CommandDisposition::NotLeader(redirect) = &result.disposition else {
        panic!("leadership loss must produce a typed redirect: {result:?}");
    };
    let leader = redirect
        .leader
        .as_ref()
        .unwrap_or_else(|| panic!("the redirect must name the current leader: {result:?}"));
    assert_eq!(
        leader.node.as_str(),
        node_id,
        "redirect must name the current leader"
    );
    let grpc_uri = world
        .cluster()
        .grpc_uri(&node_id)
        .unwrap_or_else(|error| panic!("the redirect target must be a cluster node: {error}"));
    let grpc_uri = url::Url::parse(&grpc_uri).expect("cluster gRPC URIs are URLs");
    assert_eq!(
        leader.grpc_uri.as_ref(),
        Some(&grpc_uri),
        "redirect must carry the leader endpoint"
    );
}

#[then("the background command request succeeds")]
async fn then_background_command_request_succeeds(world: &mut ScenarioWorld) {
    let task = world
        .background_command_result
        .take()
        .verified("the preceding step started a background command request");
    let result = tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .unwrap_or_else(|error| panic!("background command request did not finish: {error}"))
        .assured("the background command request task is owned by this scenario")
        .unwrap_or_else(|error| panic!("background command request transport failed: {error}"));
    assert!(
        result.succeeded(),
        "background command request failed: {}",
        result.message
    );
    world.last_command_error = None;
    world.last_command_output = Some(result.message);
}

#[when(expr = "client {string} begins executing these NSPL commands in the background")]
async fn when_named_client_begins_executing_in_the_background(
    world: &mut ScenarioWorld,
    name: String,
    #[step] step: &Step,
) {
    assert!(
        world.background_nspl.is_none(),
        "a background NSPL execution is already active"
    );
    let name = expand_placeholders(world, &name);
    let commands = expand_placeholders(world, docstring(step));
    let statements = nspl_statements(&commands);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    world.background_nspl = Some(AbortOnDropHandle::new(tokio::spawn(async move {
        let mut last_output = String::new();
        for command in statements {
            tokio::task::consume_budget().await;
            let outcome = client
                .execute(command)
                .await
                .map_err(|error| error.to_string())?;
            if !outcome.succeeded() {
                return Err(outcome.message);
            }
            last_output = outcome.message;
        }
        Ok(last_output)
    })));
}

#[when(
    expr = "client {string} begins uploading resource {string} from {string} with identity \
            {string} in the background"
)]
async fn when_named_client_begins_resource_upload_in_the_background(
    world: &mut ScenarioWorld,
    name: String,
    resource: String,
    directory: String,
    identity: String,
) {
    assert!(
        world.background_nspl.is_none(),
        "a background NSPL execution is already active"
    );
    let name = expand_placeholders(world, &name);
    let resource = expand_placeholders(world, &resource);
    let directory = PathBuf::from(expand_placeholders(world, &directory));
    let identity = expand_placeholders(world, &identity);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let identity = nervix_client_core::ResourceUploadIdentity::parse(identity)
        .assured("the scenario identity is an identifier-shaped literal");
    world.background_nspl = Some(AbortOnDropHandle::new(tokio::spawn(async move {
        let upload_domain = client
            .domain()
            .await
            .assured("the upload client selected a domain");
        let outcome = client
            .upload_resource_from_directory_with_identity(
                &resource,
                directory,
                upload_domain,
                identity,
                |_| {},
            )
            .await
            .map_err(|error| error.to_string())?;
        if outcome.succeeded() {
            Ok(outcome.message)
        } else {
            Err(outcome.message)
        }
    })));
}

#[when("the background resource upload connection is dropped")]
async fn when_background_resource_upload_connection_is_dropped(world: &mut ScenarioWorld) {
    let upload = world
        .background_nspl
        .take()
        .verified("the preceding step started a background resource upload");
    drop(upload);
    tokio::task::yield_now().await;
}

#[then("the background NSPL execution succeeds")]
async fn then_the_background_nspl_execution_succeeds(world: &mut ScenarioWorld) {
    let task = world
        .background_nspl
        .take()
        .expect("a background NSPL execution must be active");
    let output = task
        .await
        .expect("background NSPL task must not panic")
        .expect("background NSPL execution must succeed");
    world.last_command_output = Some(output);
}

#[then("the background NSPL execution does not report success")]
async fn then_background_nspl_execution_does_not_report_success(world: &mut ScenarioWorld) {
    let task = world
        .background_nspl
        .take()
        .verified("a background NSPL execution must be active");
    let result = task
        .await
        .assured("the background NSPL task is owned by this scenario");
    assert!(
        result.is_err(),
        "a command absent from the committed transaction was reported as successful"
    );
}

#[then("the last command request succeeded")]
fn then_last_command_request_succeeded(world: &mut ScenarioWorld) {
    assert!(
        world.last_command_error.is_none() && world.last_command_output.is_some(),
        "command request failed: {:?}",
        world.last_command_error
    );
}

#[then(expr = "the background NSPL execution fails with {string}")]
async fn then_the_background_nspl_execution_fails_with(
    world: &mut ScenarioWorld,
    expected: String,
) {
    let expected = expand_placeholders(world, &expected);
    let task = world
        .background_nspl
        .take()
        .expect("a background NSPL execution must be active");
    let error = task
        .await
        .expect("background NSPL task must not panic")
        .expect_err("background NSPL execution must fail");
    assert!(
        error.contains(&expected),
        "background NSPL error must contain '{expected}', got: {error}"
    );
    world.last_command_error = Some(error);
}

#[then("the background NSPL execution is discarded")]
async fn then_the_background_nspl_execution_is_discarded(world: &mut ScenarioWorld) {
    let task = world
        .background_nspl
        .take()
        .expect("a background NSPL execution must be active");
    drop(task);
}

#[when("the background NSPL execution is canceled")]
async fn when_the_background_nspl_execution_is_canceled(world: &mut ScenarioWorld) {
    let task = world
        .background_nspl
        .take()
        .expect("a background NSPL execution must be active");
    drop(task);
    tokio::task::yield_now().await;
}

fn commands_are_retry_safe_session_ops(commands: &str) -> bool {
    nspl_statements(commands).into_iter().all(|command| {
        let normalized = command.trim().to_ascii_uppercase();
        normalized == "DESCRIBE DOMAIN;"
            || normalized.starts_with("DESCRIBE RELOCATION ")
            || normalized.starts_with("DESCRIBE ENDPOINT ")
            || normalized.starts_with("DESCRIBE RESOURCE ")
            || normalized.starts_with("DESCRIBE RELAY ")
            || normalized.starts_with("DESCRIBE JUNCTION ")
            || normalized.starts_with("DESCRIBE DEDUPLICATOR ")
            || normalized.starts_with("DESCRIBE REINGESTOR ")
            || normalized.starts_with("DESCRIBE EMITTER ")
            || normalized.starts_with("DESCRIBE WASM PROCESSOR ")
            || normalized.starts_with("DESCRIBE WINDOW PROCESSOR ")
    })
}

async fn run_nspl_commands_on_active_session(
    world: &mut ScenarioWorld,
    commands: &str,
) -> Result<(), String> {
    record_avro_wire_optional_fields(world, commands);
    append_cucumber_log_line(&format!(
        "nspl commands on active session: {}",
        commands.replace('\n', "\\n")
    ));
    let session = world
        .active_session
        .as_mut()
        .expect("an active session must exist");
    for command in nspl_statements(commands) {
        append_cucumber_log_line(&format!("nspl command on active session: {command}"));
        match session.run_command(&command).await {
            Ok(output) => world.last_command_output = Some(output),
            Err(error) => return Err(error.to_string()),
        }
        world.active_session_has_subscription =
            command_updates_subscription_state(world.active_session_has_subscription, &command);
    }
    record_mqtt_ingestors(world, commands);
    Ok(())
}

#[when(expr = "the active session targets domain {string}")]
async fn when_the_active_session_targets_domain(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world
        .active_session
        .as_mut()
        .expect("an active session must exist")
        .set_domain(domain);
}

#[when("these NSPL commands are executed on the active session")]
async fn when_these_nspl_commands_are_executed_on_the_active_session(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    run_nspl_commands_on_active_session(world, &commands)
        .await
        .expect("failed to execute NSPL commands on active session");
}

#[when("these NSPL commands fail on the active session")]
async fn when_these_nspl_commands_fail_on_the_active_session(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    match run_nspl_commands_on_active_session(world, &commands).await {
        Ok(()) => panic!("expected NSPL commands to fail on active session"),
        Err(error) => world.last_command_error = Some(error),
    }
}

#[when("a new session executes these NSPL commands")]
async fn when_a_new_session_executes_these_nspl_commands(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    let session = execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .expect("failed to execute NSPL commands on a new session");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

/// The leader the nodes a scenario still runs agree on. Unlike [`current_leader_node`] this
/// tolerates nodes the scenario stopped on purpose.
async fn running_leader_node(world: &ScenarioWorld) -> String {
    world
        .cluster()
        .wait_for_leader_among_running()
        .await
        .expect("the running nodes did not agree on a leader")
}

async fn current_leader_node(world: &ScenarioWorld) -> String {
    world
        .cluster()
        .wait_for_consistent_leader_on_all_nodes()
        .await
        .expect("cluster leader did not appear")
}

async fn wait_for_mqtt_ingestors_ready(world: &mut ScenarioWorld) {
    let ingestors = world
        .mqtt_ingestors_by_domain
        .get(&world.domain)
        .cloned()
        .unwrap_or_default();
    if ingestors.is_empty() {
        return;
    }
    let leader = current_leader_node(world).await;
    for ingestor in ingestors {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            tokio::task::consume_budget().await;
            let output = run_nspl_commands_on_node(
                world,
                &leader,
                &format!("DESCRIBE INGESTOR {ingestor};"),
            )
            .await
            .unwrap_or_else(|error| error);
            if output.contains("ready: true") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for MQTT ingestor '{ingestor}' to become ready. last output: {}",
                output
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn open_web_console_page(world: &mut ScenarioWorld, url: &str) -> Result<(), String> {
    let playwright = Playwright::launch()
        .await
        .map_err(|error| error.to_string())?;
    let browser = playwright
        .chromium()
        .launch_with_options(chromium_launch_options())
        .await
        .map_err(|error| error.to_string())?;
    let context = browser
        .new_context()
        .await
        .map_err(|error| error.to_string())?;
    let page = context
        .new_page()
        .await
        .map_err(|error| error.to_string())?;
    page.set_default_timeout(10_000.0).await;
    page.goto(url, None)
        .await
        .map_err(|error| error.to_string())?;
    world.browser_page = Some(page);
    world.browser_context = Some(context);
    world.browser = Some(browser);
    world.playwright = Some(playwright);
    Ok(())
}

async fn close_browser(world: &mut ScenarioWorld) {
    world.browser_page = None;
    if let Some(context) = world.browser_context.take() {
        let _ = context.close().await;
    }
    if let Some(browser) = world.browser.take() {
        let _ = browser.close().await;
    }
    world.playwright = None;
}

fn chromium_launch_options() -> LaunchOptions {
    let mut options = LaunchOptions::default().headless(true).args(vec![
        "--no-sandbox".to_string(),
        "--disable-dev-shm-usage".to_string(),
    ]);
    if let Some(path) = chrome_executable_path() {
        options = options.executable_path(path.to_string_lossy().into_owned());
    }
    options
}

fn chrome_executable_path() -> Option<&'static Path> {
    [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ]
    .into_iter()
    .map(Path::new)
    .find(|path| path.exists())
}

#[when("these NSPL commands are executed")]
async fn when_these_nspl_commands_are_executed(world: &mut ScenarioWorld, #[step] step: &Step) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    if world.active_session_has_subscription
        && world.active_session.is_some()
        && world.active_session_node.as_deref() == Some(leader.as_str())
    {
        run_nspl_commands_on_active_session(world, &commands)
            .await
            .expect("failed to execute NSPL command on active session");
        return;
    }
    let session = execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .expect("failed to execute NSPL setup command");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[when("these NSPL commands are executed through the client on a follower node")]
async fn when_these_nspl_commands_are_executed_through_the_client_on_a_follower_node(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let follower = world
        .cluster()
        .any_follower_node("node-1")
        .await
        .expect("failed to resolve follower node");
    let grpc_uri = world
        .cluster()
        .grpc_uri(&follower)
        .expect("failed to resolve follower gRPC URI");
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(&world.domain),
        client_connect_options(&grpc_uri).expect("failed to build client tls options"),
    )
    .await
    .expect("failed to connect follower client");
    let commands = expand_placeholders(world, docstring(step));
    for command in nspl_statements(&commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .expect("client command should complete");
        assert!(
            outcome.succeeded(),
            "client command must succeed: {command}: {}",
            outcome.message
        );
        world.last_command_output = Some(outcome.message);
    }
}

async fn connect_named_client_to_node(
    world: &mut ScenarioWorld,
    name: String,
    node_id: String,
    seed_nodes: Vec<String>,
) {
    let name = expand_placeholders(world, &name);
    let node_id = expand_placeholders(world, &node_id);
    let grpc_uri = world
        .cluster()
        .grpc_uri(&node_id)
        .expect("failed to resolve client node gRPC URI");
    let mut options =
        client_connect_options(&grpc_uri).expect("failed to build client tls options");
    for seed_node in seed_nodes {
        let seed_uri = world
            .cluster()
            .grpc_uri(&seed_node)
            .expect("failed to resolve seed node gRPC URI");
        options
            .seed_servers
            .push(url::Url::parse(&seed_uri).expect("cluster gRPC seed URIs are valid URLs"));
    }
    let client = Client::connect_with_options(&grpc_uri, client_domain(&world.domain), options)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to connect client '{name}' to '{node_id}': {error}")
        });
    assert!(
        world
            .transaction_clients
            .insert(name.clone(), client)
            .is_none(),
        "client '{name}' is already connected"
    );
}

#[given(expr = "client {string} is connected to node {string}")]
async fn given_named_client_is_connected_to_node(
    world: &mut ScenarioWorld,
    name: String,
    node_id: String,
) {
    connect_named_client_to_node(world, name, node_id, Vec::new()).await;
}

#[given(expr = "client {string} is connected to node {string} with cluster seeds")]
async fn given_named_client_is_connected_with_cluster_seeds(
    world: &mut ScenarioWorld,
    name: String,
    node_id: String,
) {
    let seeds = world.cluster().node_ids();
    connect_named_client_to_node(world, name, node_id, seeds).await;
}

#[given(expr = "client {string} is connected to the leader node")]
async fn given_named_client_is_connected_to_leader(world: &mut ScenarioWorld, name: String) {
    let leader = current_leader_node(world).await;
    connect_named_client_to_node(world, name, leader, Vec::new()).await;
}

#[given(
    expr = "client {string} is connected to the leader node as user {string} with password \
            {string}"
)]
async fn given_named_client_is_connected_to_leader_as_user(
    world: &mut ScenarioWorld,
    name: String,
    username: String,
    password: String,
) {
    let name = expand_placeholders(world, &name);
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .expect("failed to resolve leader gRPC URI");
    let mut options =
        client_connect_options(&grpc_uri).expect("failed to build client tls options");
    options.username = Some(expand_placeholders(world, &username));
    options.password = Some(expand_placeholders(world, &password));
    let client = Client::connect_with_options(&grpc_uri, client_domain(&world.domain), options)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to connect client '{name}' as '{username}': {error}")
        });
    assert!(
        world
            .transaction_clients
            .insert(name.clone(), client)
            .is_none(),
        "client '{name}' is already connected"
    );
}

#[when(expr = "client {string} closes its session cleanly")]
async fn when_named_client_closes_cleanly(world: &mut ScenarioWorld, name: String) {
    let name = expand_placeholders(world, &name);
    let client = world
        .transaction_clients
        .remove(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"));
    drop(client);
}

#[when(expr = "client {string} executes these NSPL commands")]
async fn when_named_client_executes_commands(
    world: &mut ScenarioWorld,
    name: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_client_outcome = None;
    let name = expand_placeholders(world, &name);
    let commands = expand_placeholders(world, docstring(step));
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    for command in nspl_statements(&commands) {
        tokio::task::consume_budget().await;
        let outcome = client
            .execute(command.clone())
            .await
            .unwrap_or_else(|error| panic!("client '{name}' command failed: {command}: {error}"));
        assert!(
            outcome.succeeded(),
            "client '{name}' command must succeed: {command}: {}",
            outcome.message
        );
        world.last_command_output = Some(outcome.message.clone());
        world.last_client_outcome = Some(outcome);
    }
}

#[when(expr = "client {string} submits this NSPL command request")]
async fn when_named_client_submits_command_request(
    world: &mut ScenarioWorld,
    name: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_client_outcome = None;
    let name = expand_placeholders(world, &name);
    let request = expand_placeholders(world, docstring(step));
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let outcome = client
        .execute(request.clone())
        .await
        .unwrap_or_else(|error| panic!("client '{name}' request failed: {request}: {error}"));
    if outcome.succeeded() {
        world.last_command_output = Some(outcome.message.clone());
    } else {
        world.last_command_error = Some(outcome.message.clone());
    }
    world.last_client_outcome = Some(outcome);
}

#[when(expr = "client {string} fails to execute these NSPL commands")]
async fn when_named_client_fails_to_execute_commands(
    world: &mut ScenarioWorld,
    name: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let name = expand_placeholders(world, &name);
    let commands = expand_placeholders(world, docstring(step));
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    for command in nspl_statements(&commands) {
        tokio::task::consume_budget().await;
        match client.execute(command.clone()).await {
            Ok(outcome) if outcome.succeeded() => {
                world.last_command_output = Some(outcome.message);
            }
            Ok(outcome) => {
                world.last_command_error = Some(outcome.message);
                return;
            }
            Err(error) => {
                world.last_command_error = Some(error.to_string());
                return;
            }
        }
    }
    panic!("client '{name}' commands unexpectedly succeeded");
}

#[when(expr = "client {string} selects domain {string}")]
async fn when_named_client_selects_domain(world: &mut ScenarioWorld, name: String, domain: String) {
    let name = expand_placeholders(world, &name);
    let domain = expand_placeholders(world, &domain);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    client.set_domain(client_domain(&domain)).await;
}

#[when(expr = "client {string} uploads resource {string} from {string} with identity {string}")]
async fn when_named_client_uploads_resource_with_identity(
    world: &mut ScenarioWorld,
    name: String,
    resource: String,
    directory: String,
    identity: String,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let name = expand_placeholders(world, &name);
    let resource = expand_placeholders(world, &resource);
    let directory = PathBuf::from(expand_placeholders(world, &directory));
    let identity = expand_placeholders(world, &identity);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let identity = nervix_client_core::ResourceUploadIdentity::parse(identity)
        .expect("scenario upload identity must be valid");
    let upload_domain = client
        .domain()
        .await
        .assured("the upload client selected a domain");
    let outcome = client
        .upload_resource_from_directory_with_identity(
            &resource,
            directory,
            upload_domain,
            identity,
            |_| {},
        )
        .await
        .unwrap_or_else(|error| panic!("client '{name}' resource upload failed: {error}"));
    assert!(
        outcome.succeeded(),
        "client '{name}' resource upload must succeed: {}",
        outcome.message
    );
    world.last_command_output = Some(outcome.message.clone());
    world.last_client_outcome = Some(outcome);
}

fn assert_last_client_upload(
    world: &ScenarioWorld,
    version: u64,
    origin: nervix_client_core::OutcomeOrigin,
) {
    let outcome = world
        .last_client_outcome
        .as_ref()
        .expect("a client upload must have been made");
    let upload = outcome
        .resource_upload
        .as_ref()
        .unwrap_or_else(|| panic!("the last client outcome was not an upload: {outcome:?}"));
    let installed = upload.version.map(std::num::NonZeroU64::get);
    assert_eq!(
        installed,
        Some(version),
        "unexpected upload version: {upload:?}"
    );
    assert_eq!(
        upload.origin,
        Some(origin),
        "unexpected upload origin: {upload:?}"
    );
}

#[then(expr = "the last client upload installed version {int}")]
async fn then_last_client_upload_installed_version(world: &mut ScenarioWorld, version: u64) {
    assert_last_client_upload(world, version, nervix_client_core::OutcomeOrigin::Executed);
}

#[then(expr = "the last client upload recovered version {int} from an earlier upload")]
async fn then_last_client_upload_recovered_version(world: &mut ScenarioWorld, version: u64) {
    assert_last_client_upload(world, version, nervix_client_core::OutcomeOrigin::Recovered);
}

#[when(
    expr = "an incomplete upload of resource {string} with identity {string} is sent to the \
            leader node"
)]
async fn when_incomplete_resource_upload_is_sent(
    world: &mut ScenarioWorld,
    resource: String,
    identity: String,
) {
    let resource = expand_placeholders(world, &resource);
    let identity = expand_placeholders(world, &identity);
    let leader = current_leader_node(world).await;
    // Declares two bytes and carries one, so the server must refuse it.
    let upload = TestUpload {
        domain: &world.domain,
        resource: &resource,
        identity: &identity,
        parts: vec![
            TestUploadPart::Start {
                declared_bytes: NonZeroU64::new(2).assured("two is non-zero"),
            },
            TestUploadPart::Chunk(vec![0]),
        ],
    };
    let result = world
        .cluster()
        .send_shaped_resource_upload(&leader, upload)
        .await
        .unwrap_or_else(|error| panic!("incomplete upload request failed: {error}"));
    let nervix_client_wire::UploadDisposition::Failed {
        upload_identity: Some(upload_identity),
        ..
    } = &result.disposition
    else {
        panic!("incomplete upload unexpectedly succeeded: {result:?}");
    };
    assert_eq!(upload_identity.as_str(), identity);
    world.last_command_error = Some(result.message.clone());
    world.last_command_output = Some(result.message);
}

#[when(
    expr = "client {string} upload of resource {string} from {string} with identity {string} \
            fails with {string}"
)]
async fn when_named_client_resource_upload_fails_with(
    world: &mut ScenarioWorld,
    name: String,
    resource: String,
    directory: String,
    identity: String,
    expected: String,
) {
    let name = expand_placeholders(world, &name);
    let resource = expand_placeholders(world, &resource);
    let directory = PathBuf::from(expand_placeholders(world, &directory));
    let identity = expand_placeholders(world, &identity);
    let expected = expand_placeholders(world, &expected);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let identity = nervix_client_core::ResourceUploadIdentity::parse(identity)
        .expect("scenario upload identity must be valid");
    let upload_domain = client
        .domain()
        .await
        .assured("the upload client selected a domain");
    let outcome = client
        .upload_resource_from_directory_with_identity(
            &resource,
            directory,
            upload_domain,
            identity,
            |_| {},
        )
        .await
        .unwrap_or_else(|error| panic!("client '{name}' resource upload failed: {error}"));
    assert!(
        !outcome.succeeded(),
        "resource upload unexpectedly succeeded"
    );
    assert!(
        outcome.message.contains(&expected),
        "resource upload error did not contain '{expected}': {}",
        outcome.message
    );
    world.last_command_error = Some(outcome.message.clone());
    world.last_command_output = Some(outcome.message);
}

#[then(expr = "client {string} active domain is {string}")]
async fn then_named_client_active_domain_is(
    world: &mut ScenarioWorld,
    name: String,
    expected: String,
) {
    let name = expand_placeholders(world, &name);
    let expected = expand_placeholders(world, &expected);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let actual = client.domain().await;
    assert_eq!(
        actual,
        client_domain(&expected),
        "client '{name}' active domain must be '{expected}'"
    );
}

#[then(expr = "client {string} transaction id is saved as placeholder {string}")]
async fn then_named_client_transaction_id_is_saved(
    world: &mut ScenarioWorld,
    name: String,
    placeholder: String,
) {
    let name = expand_placeholders(world, &name);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let status = client
        .transaction_status()
        .await
        .unwrap_or_else(|| panic!("client '{name}' does not have a transaction status"));
    world
        .placeholders
        .insert(placeholder, status.transaction_id().to_string());
}

#[then(expr = "client {string} transaction state is {string} with failing step {int}")]
async fn then_named_client_transaction_failed_at_step(
    world: &mut ScenarioWorld,
    name: String,
    expected_state: String,
    failing_step: u64,
) {
    let name = expand_placeholders(world, &name);
    let expected_state = expand_placeholders(world, &expected_state).to_ascii_uppercase();
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let status = client
        .transaction_status()
        .await
        .unwrap_or_else(|| panic!("client '{name}' does not have a transaction status"));
    let nervix_client_core::TransactionLifecycle::Failed {
        failing_operation, ..
    } = status.lifecycle()
    else {
        panic!("client '{name}' transaction must have failed: {status:?}");
    };
    assert_eq!(status.lifecycle().as_ref(), expected_state);
    let failing_step = usize::try_from(failing_step).expect("a step number fits in usize");
    assert_eq!(failing_operation.get(), failing_step);
}

#[when(expr = "client {string} attempts to commit its transaction")]
async fn when_named_client_attempts_commit(world: &mut ScenarioWorld, name: String) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_client_outcome = None;
    let name = expand_placeholders(world, &name);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let outcome = client
        .execute("COMMIT;")
        .await
        .unwrap_or_else(|error| panic!("client '{name}' COMMIT did not reach the server: {error}"));
    if outcome.succeeded() {
        world.last_command_output = Some(outcome.message.clone());
    } else {
        world.last_command_error = Some(outcome.message.clone());
    }
    world.last_client_outcome = Some(outcome);
}

#[then(expr = "client {string} commit was refused because its expected preview is stale")]
async fn then_named_client_commit_refused_as_stale(world: &mut ScenarioWorld, name: String) {
    let name = expand_placeholders(world, &name);
    let outcome = world
        .last_client_outcome
        .as_ref()
        .unwrap_or_else(|| panic!("client '{name}' has not attempted a commit"));
    assert!(
        !outcome.succeeded(),
        "client '{name}' commit must be refused: {}",
        outcome.message
    );
    let nervix_client_core::CommandDisposition::PreviewStale { expected, current } =
        &outcome.disposition
    else {
        panic!(
            "client '{name}' commit must report a stale preview: {}",
            outcome.message
        );
    };
    assert_eq!(
        expected.transaction_id, current.transaction_id,
        "a stale preview describes the same transaction the commit named"
    );
    assert_eq!(
        expected.position, current.position,
        "nothing was appended, so only the planning basis moved"
    );
    assert_ne!(
        expected.planning_basis, current.planning_basis,
        "a stale preview names a planning basis the transaction has outgrown"
    );
}

#[then(expr = "client {string} transaction state is {string}")]
async fn then_named_client_transaction_state_is(
    world: &mut ScenarioWorld,
    name: String,
    expected_state: String,
) {
    let name = expand_placeholders(world, &name);
    let expected_state = expand_placeholders(world, &expected_state).to_ascii_uppercase();
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let status = client
        .transaction_status()
        .await
        .unwrap_or_else(|| panic!("client '{name}' does not have a transaction status"));
    assert_eq!(status.lifecycle().as_ref(), expected_state);
}

#[then(expr = "client {string} has no transaction")]
async fn then_named_client_has_no_transaction(world: &mut ScenarioWorld, name: String) {
    let name = expand_placeholders(world, &name);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let status = client.transaction_status().await;
    assert!(
        status.is_none(),
        "client '{name}' must not be bound to a transaction, found {status:?}"
    );
}

#[then(expr = "the last accepted operation is {int}")]
async fn then_last_accepted_operation_is(world: &mut ScenarioWorld, expected: usize) {
    let outcome = world
        .last_client_outcome
        .as_ref()
        .expect("a named client command must have run before its admission is read");
    let admission = outcome.transaction_admission.as_ref().unwrap_or_else(|| {
        panic!(
            "the last command accepted no operation: {}",
            outcome.message
        )
    });
    assert_eq!(admission.operation.get(), expected);
}

#[then(expr = "the last client request failed with one diagnostic spanning bytes {int} to {int}")]
fn then_last_client_request_has_diagnostic_span(
    world: &mut ScenarioWorld,
    expected_start: u32,
    expected_end: u32,
) {
    let outcome = world
        .last_client_outcome
        .as_ref()
        .verified("the scenario submitted a named client request above");
    assert_eq!(
        outcome.disposition,
        nervix_client_core::CommandDisposition::Failed
    );
    assert_eq!(outcome.diagnostics.len(), 1);
    let span = outcome.diagnostics[0]
        .span
        .verified("the parser diagnostic for this malformed command has a source span");
    assert_eq!(span.start(), expected_start);
    assert_eq!(span.end(), expected_end);
}

/// Compares the typed inspection the last named client command carried with `field: value` lines.
#[then("the last inspection reports")]
async fn then_last_inspection_reports(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step));
    let outcome = world
        .last_client_outcome
        .as_ref()
        .expect("a named client command must have run before its inspection is read");
    let inspection = outcome.inspection.as_ref().unwrap_or_else(|| {
        panic!(
            "the last command carried no inspection: {}",
            outcome.message
        )
    });
    for line in expected.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (field, value) = line
            .split_once(':')
            .unwrap_or_else(|| panic!("inspection expectation '{line}' must read 'field: value'"));
        let field = field.trim();
        let actual = match field {
            "transaction" => inspection.transaction.transaction_id().to_string(),
            "domain" => inspection.transaction.domain().to_string(),
            "state" => inspection.transaction.lifecycle().as_ref().to_string(),
            "accepted operations" => inspection
                .transaction
                .accepted_operations()
                .accepted_operations()
                .to_string(),
            "applied operations" => inspection.transaction.applied_operations().to_string(),
            "selected operation" => match inspection.operation {
                Some(operation) => operation.to_string(),
                None => "none".to_string(),
            },
            "report operations" => inspection.report.operations().len().to_string(),
            "quiesce level" => inspection.report.summary().level().as_str().to_string(),
            other => panic!("unknown inspection field '{other}'"),
        };
        assert_eq!(actual, value.trim(), "inspection field '{field}'");
    }
}

#[when("the CLI successfully executes this JSON inspection")]
async fn when_cli_executes_json_inspection(world: &mut ScenarioWorld, #[step] step: &Step) {
    run_cli_inspection(world, step, true, CliConnectionCase::Leader).await;
}

#[when("the CLI successfully executes this text inspection")]
async fn when_cli_executes_text_inspection(world: &mut ScenarioWorld, #[step] step: &Step) {
    run_cli_inspection(world, step, true, CliConnectionCase::Leader).await;
}

#[when("the CLI refuses this JSON inspection")]
async fn when_cli_refuses_json_inspection(world: &mut ScenarioWorld, #[step] step: &Step) {
    run_cli_inspection(world, step, false, CliConnectionCase::Leader).await;
}

#[when("the CLI executes this JSON inspection with a missing CA file")]
async fn when_cli_json_inspection_has_missing_ca(world: &mut ScenarioWorld, #[step] step: &Step) {
    run_cli_inspection(world, step, false, CliConnectionCase::MissingCa).await;
}

#[when("the CLI executes this JSON inspection with an invalid server URL")]
async fn when_cli_json_inspection_has_invalid_server(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    run_cli_inspection(world, step, false, CliConnectionCase::InvalidServer).await;
}

#[derive(Clone, Copy)]
enum CliConnectionCase {
    Leader,
    MissingCa,
    InvalidServer,
}

async fn run_cli_inspection(
    world: &mut ScenarioWorld,
    step: &Step,
    succeeds: bool,
    connection: CliConnectionCase,
) {
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .expect("the leader has a gRPC URI");
    let executable = std::env::var_os("NERVIX_TEST_CLI_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let target_dir = std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
            target_dir.join("debug/nervix-cli")
        });
    let query = expand_placeholders(world, docstring(step));
    let server = match connection {
        CliConnectionCase::InvalidServer => "not-a-server-url",
        CliConnectionCase::Leader | CliConnectionCase::MissingCa => &grpc_uri,
    };
    let mut command = tokio::process::Command::new(executable);
    command
        .arg("--server")
        .arg(server)
        .arg("--domain")
        .arg(&world.domain)
        .arg("--username")
        .arg(TEST_AUTH_USERNAME)
        .arg("--password")
        .arg(TEST_AUTH_PASSWORD)
        .arg("--command")
        .arg(query);
    if let CliConnectionCase::MissingCa = connection {
        command
            .arg("--tls-ca-cert")
            .arg("/nonexistent/nervix-ca.pem");
    }
    let output = tokio::time::timeout(Duration::from_secs(60), command.output())
        .await
        .expect("the standalone CLI inspection finishes within one minute")
        .expect("the standalone CLI process starts and returns");
    let stdout = String::from_utf8(output.stdout).expect("JSON stdout is UTF-8");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.success(),
        succeeds,
        "CLI exit status for JSON inspection; stdout: {stdout}; stderr: {stderr}"
    );
    world.last_command_output = Some(stdout);
    world.last_command_error = Some(stderr.into_owned());
}

/// Compares values in the JSON document the last command printed, one `pointer = literal` per line.
#[then("the last command output is a JSON document where")]
async fn then_last_command_output_is_a_json_document_where(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    let document: serde_json::Value = serde_json::from_str(output).unwrap_or_else(|error| {
        panic!("the last command output is not one JSON document: {error}\n{output}")
    });
    for line in expected.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (pointer, literal) = line
            .split_once(" = ")
            .unwrap_or_else(|| panic!("JSON expectation '{line}' must read 'pointer = literal'"));
        let pointer = pointer.trim();
        let expected_value: serde_json::Value = serde_json::from_str(literal.trim())
            .unwrap_or_else(|error| panic!("'{literal}' is not a JSON literal: {error}"));
        let actual = document
            .pointer(pointer)
            .unwrap_or_else(|| panic!("the JSON document has no value at {pointer}: {output}"));
        assert_eq!(actual, &expected_value, "JSON value at {pointer}");
    }
}

#[when(expr = "client {string} attaches to transaction {string}")]
async fn when_named_client_attaches_to_transaction(
    world: &mut ScenarioWorld,
    name: String,
    transaction_id: String,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let name = expand_placeholders(world, &name);
    let transaction_id = expand_placeholders(world, &transaction_id);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let outcome = client
        .attach_transaction(transaction_id.clone())
        .await
        .unwrap_or_else(|error| {
            panic!("client '{name}' failed to attach transaction '{transaction_id}': {error}")
        });
    assert!(
        outcome.succeeded(),
        "client '{name}' must attach transaction '{transaction_id}': {}",
        outcome.message
    );
    world.last_command_output = Some(outcome.message);
}

#[when(expr = "client {string} fails to attach to transaction {string}")]
async fn when_named_client_fails_to_attach_to_transaction(
    world: &mut ScenarioWorld,
    name: String,
    transaction_id: String,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let name = expand_placeholders(world, &name);
    let transaction_id = expand_placeholders(world, &transaction_id);
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    match client.attach_transaction(transaction_id.clone()).await {
        Ok(outcome) => {
            assert!(
                !outcome.succeeded(),
                "client '{name}' unexpectedly attached transaction '{transaction_id}'"
            );
            world.last_command_output = Some(
                outcome
                    .statements
                    .iter()
                    .map(|statement| statement.message.as_str())
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
            world.last_command_error = Some(outcome.message);
        }
        Err(error) => world.last_command_error = Some(error.to_string()),
    }
}

#[when("these NSPL commands are executed through the client on the leader node")]
async fn when_these_nspl_commands_are_executed_through_the_client_on_the_leader_node(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .expect("failed to resolve leader gRPC URI");
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(&world.domain),
        client_connect_options(&grpc_uri).expect("failed to build client tls options"),
    )
    .await
    .expect("failed to connect leader client");
    let commands = expand_placeholders(world, docstring(step));
    for command in nspl_statements(&commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .expect("client command should complete");
        assert!(
            outcome.succeeded(),
            "client command must succeed: {command}: {}",
            outcome.message
        );
        world.last_command_output = Some(outcome.message);
    }
}

#[when(expr = "the client connects to the leader node with password {string}")]
async fn when_the_client_connects_to_the_leader_node_with_password(
    world: &mut ScenarioWorld,
    password: String,
) {
    connect_to_leader_with_credentials(world, TEST_AUTH_USERNAME.to_string(), password).await;
}

#[when(expr = "the client connects to the leader node as user {string} with password {string}")]
async fn when_the_client_connects_to_the_leader_node_as_user_with_password(
    world: &mut ScenarioWorld,
    username: String,
    password: String,
) {
    connect_to_leader_with_credentials(world, username, password).await;
}

#[when(
    expr = "the client attempts to connect to the leader node as user {string} with password \
            {string} {int} times"
)]
async fn when_the_client_attempts_to_connect_to_the_leader_node_as_user_with_password_times(
    world: &mut ScenarioWorld,
    username: String,
    password: String,
    attempts: usize,
) {
    assert!(attempts > 0, "auth attempt count must be positive");
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.last_auth_attempts_elapsed = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let username = expand_placeholders(world, &username);
    let password = expand_placeholders(world, &password);
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .expect("failed to resolve leader gRPC URI");
    let started = Instant::now();
    for attempt in 1..=attempts {
        let mut options =
            client_connect_options(&grpc_uri).expect("failed to build client tls options");
        options.username = Some(username.clone());
        options.password = Some(password.clone());
        match Client::connect_with_options(&grpc_uri, client_domain(&world.domain), options).await {
            Ok(client) => match client.execute("SHOW CLUSTER STATUS;".to_string()).await {
                Ok(outcome) if outcome.succeeded() => {
                    panic!("auth attempt {attempt} unexpectedly succeeded");
                }
                Ok(outcome) => {
                    world.last_command_error = Some(outcome.message);
                }
                Err(error) => {
                    world.last_command_error = Some(error.to_string());
                }
            },
            Err(error) => {
                world.last_command_error = Some(error.to_string());
            }
        }
    }
    world.last_auth_attempts_elapsed = Some(started.elapsed());
}

async fn connect_to_leader_with_credentials(
    world: &mut ScenarioWorld,
    username: String,
    password: String,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let username = expand_placeholders(world, &username);
    let password = expand_placeholders(world, &password);
    let leader = current_leader_node(world).await;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&leader)
        .expect("failed to resolve leader gRPC URI");
    let mut options =
        client_connect_options(&grpc_uri).expect("failed to build client tls options");
    options.username = Some(username);
    options.password = Some(password);
    match Client::connect_with_options(&grpc_uri, client_domain(&world.domain), options).await {
        Ok(client) => match client.execute("SHOW CLUSTER STATUS;".to_string()).await {
            Ok(outcome) if outcome.succeeded() => {
                world.last_command_output = Some(outcome.message);
            }
            Ok(outcome) => {
                world.last_command_error = Some(outcome.message);
            }
            Err(error) => {
                world.last_command_error = Some(error.to_string());
            }
        },
        Err(error) => {
            world.last_command_error = Some(error.to_string());
        }
    }
}

#[when(expr = "these NSPL commands are executed through the client on node {string}")]
async fn when_these_nspl_commands_are_executed_through_the_client_on_node(
    world: &mut ScenarioWorld,
    node_id: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let node_id = expand_placeholders(world, &node_id);
    let grpc_uri = world
        .cluster()
        .grpc_uri(&node_id)
        .expect("failed to resolve node gRPC URI");
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(&world.domain),
        client_connect_options(&grpc_uri).expect("failed to build client tls options"),
    )
    .await
    .expect("failed to connect node client");
    let commands = expand_placeholders(world, docstring(step));
    for command in nspl_statements(&commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .expect("client command should complete");
        assert!(
            outcome.succeeded(),
            "client command must succeed: {command}: {}",
            outcome.message
        );
        world.last_command_output = Some(outcome.message);
    }
}

#[when(expr = "these NSPL commands fail through the client on node {string} with {string}")]
async fn when_these_nspl_commands_fail_through_the_client_on_node_with(
    world: &mut ScenarioWorld,
    node_id: String,
    expected_error: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let grpc_uri = world
        .cluster()
        .grpc_uri(&node_id)
        .expect("failed to resolve node gRPC URI");
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(&world.domain),
        client_connect_options(&grpc_uri).expect("failed to build client tls options"),
    )
    .await
    .expect("failed to connect node client");
    let commands = expand_placeholders(world, docstring(step));
    for command in nspl_statements(&commands) {
        let outcome = client
            .execute(command.clone())
            .await
            .expect("client command should complete");
        assert!(
            !outcome.succeeded(),
            "client command must fail: {command}: {}",
            outcome.message
        );
        assert!(
            outcome.message.contains(&expected_error),
            "expected error containing {:?}, got: {}",
            expected_error,
            outcome.message
        );
        world.last_command_error = Some(outcome.message);
    }
}

#[when("these NSPL commands are executed on the leader node")]
async fn when_these_nspl_commands_are_executed_on_leader_node(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    let retry_safe = commands_are_retry_safe_session_ops(&commands);
    if world.active_session_has_subscription
        && world.active_session.is_some()
        && world.active_session_node.as_deref() == Some(leader.as_str())
    {
        if retry_safe {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                tokio::task::consume_budget().await;
                match run_nspl_commands_on_active_session(world, &commands).await {
                    Ok(()) => break,
                    Err(error) => {
                        assert!(
                            Instant::now() < deadline,
                            "failed to execute NSPL setup command on active session: {error:?}"
                        );
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        } else {
            run_nspl_commands_on_active_session(world, &commands)
                .await
                .expect("failed to execute NSPL setup command on active session");
        }
        return;
    }
    let session = if retry_safe {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::task::consume_budget().await;
            match execute_nspl_commands_on_node(world, &leader, &commands).await {
                Ok(session) => break session,
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "failed to execute NSPL setup command on leader: {error:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    } else {
        execute_nspl_commands_on_node(world, &leader, &commands)
            .await
            .expect("failed to execute NSPL setup command on leader")
    };
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[when("this NSPL command request is executed on the leader node")]
async fn when_this_nspl_command_request_is_executed_on_leader_node(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    match world
        .cluster()
        .run_command(&leader, &world.domain, &commands)
        .await
    {
        Ok(output) => world.last_command_output = Some(output),
        Err(error) => world.last_command_error = Some(error.to_string()),
    }
}

#[when(
    expr = "a new session attaches to transaction {string} and executes this NSPL command with \
            execution reference {string} at transaction position {int}"
)]
async fn when_new_session_attaches_and_executes_referenced_command(
    world: &mut ScenarioWorld,
    transaction_id: String,
    execution_reference: String,
    expected_transaction_position: u64,
    #[step] step: &Step,
) {
    let transaction_id = expand_placeholders(world, &transaction_id);
    let execution_reference = command_execution_reference(world, &execution_reference);
    let query = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .unwrap_or_else(|error| panic!("failed to open replay session: {error}"));
    let attached = session
        .attach_transaction(&transaction_id)
        .await
        .unwrap_or_else(|error| panic!("failed to attach replay session: {error}"));
    assert!(
        attached.succeeded(),
        "failed to attach replay session: {}",
        attached.message
    );
    let expected_transaction_position = usize::try_from(expected_transaction_position)
        .expect("a scenario transaction position fits in usize");
    let result = session
        .run_command_result_with_reference_at_position(
            &query,
            &execution_reference,
            expected_transaction_position,
        )
        .await
        .unwrap_or_else(|error| panic!("replayed command request failed: {error}"));
    if result.succeeded() {
        world.last_command_error = None;
        world.last_command_output = Some(result.message);
    } else {
        world.last_command_output = None;
        world.last_command_error = Some(result.message);
    }
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = false;
}

#[then(expr = "the current leader node is saved as placeholder {string}")]
async fn then_current_leader_node_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let leader = current_leader_node(world).await;
    world.placeholders.insert(placeholder, leader);
}

#[then(expr = "the only transaction id is saved as placeholder {string}")]
async fn then_only_transaction_id_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let output = world
        .last_command_output
        .as_deref()
        .verified("the preceding SHOW TRANSACTIONS command produced output");
    let ids = output
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter_map(|field| field.strip_prefix("id="))
        .collect::<Vec<_>>();
    assert_eq!(
        ids.len(),
        1,
        "expected exactly one transaction, got output:\n{output}"
    );
    world.placeholders.insert(placeholder, ids[0].to_string());
}

#[then(expr = "transaction {string} eventually has state {string}")]
async fn then_transaction_eventually_has_state(
    world: &mut ScenarioWorld,
    transaction_id: String,
    expected_state: String,
) {
    let transaction_id = expand_placeholders(world, &transaction_id);
    let expected_state = expand_placeholders(world, &expected_state).to_ascii_uppercase();
    let leader = current_leader_node(world).await;
    let expected_id = format!("id={transaction_id}");
    let expected_state_fragment = format!("state={expected_state}");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_output = String::new();
    loop {
        tokio::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "transaction '{transaction_id}' did not reach state '{expected_state}'; last output: \
             {last_output}"
        );
        match world
            .cluster()
            .run_command(&leader, &world.domain, "SHOW TRANSACTIONS;")
            .await
        {
            Ok(output) => {
                if output.lines().any(|line| {
                    line.contains(&expected_id) && line.contains(&expected_state_fragment)
                }) {
                    world.last_command_output = Some(output);
                    return;
                }
                last_output = output;
            }
            Err(error) => last_output = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn quiescence_outcome_name(outcome: &nervix_models::QuiescenceOutcome) -> &'static str {
    match outcome {
        nervix_models::QuiescenceOutcome::Requested => "REQUESTED",
        nervix_models::QuiescenceOutcome::Confirmed => "CONFIRMED",
        nervix_models::QuiescenceOutcome::Failed { .. } => "FAILED",
        nervix_models::QuiescenceOutcome::Uncertain { .. } => "UNCERTAIN",
        nervix_models::QuiescenceOutcome::Released => "RELEASED",
    }
}

fn execution_outcome_name(outcome: &nervix_models::ExecutionStepOutcome) -> &'static str {
    match outcome {
        nervix_models::ExecutionStepOutcome::Unattempted => "UNATTEMPTED",
        nervix_models::ExecutionStepOutcome::Applying => "APPLYING",
        nervix_models::ExecutionStepOutcome::Applied => "APPLIED",
        nervix_models::ExecutionStepOutcome::Failed { .. } => "FAILED",
    }
}

async fn read_transaction_impact_report(
    world: &ScenarioWorld,
    transaction_id: &str,
) -> Result<nervix_models::TransactionImpactReport, String> {
    let leader = current_leader_node(world).await;
    let observer = world
        .fault_injection
        .consensus_observer(&crate::common::cluster::node_name(&leader));
    let transaction = observer
        .current_transaction(transaction_id)
        .await
        .ok_or_else(|| format!("transaction '{transaction_id}' is not retained"))?;
    let preview = transaction
        .latest_preview()
        .ok_or_else(|| format!("transaction '{transaction_id}' has no retained preview"))?;
    observer
        .current_transaction_report(preview)
        .await
        .map_err(|error| error.to_string())
}

#[then(
    expr = "transaction {string} report step {int} eventually records planned quiesce {string}, \
            actual quiesce {string}, execution {string}, and outcomes {string}"
)]
async fn then_transaction_report_records_actual_quiescence(
    world: &mut ScenarioWorld,
    transaction_id: String,
    step_number: usize,
    planned: String,
    actual: String,
    execution: String,
    outcomes: String,
) {
    let transaction_id = expand_placeholders(world, &transaction_id);
    let planned = expand_placeholders(world, &planned).to_ascii_uppercase();
    let actual = expand_placeholders(world, &actual).to_ascii_uppercase();
    let execution = expand_placeholders(world, &execution).to_ascii_uppercase();
    let outcomes = expand_placeholders(world, &outcomes).to_ascii_uppercase();
    let index = step_number
        .checked_sub(1)
        .assured("transaction report step numbers are one-based");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut observed = String::new();
    loop {
        tokio::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "transaction '{transaction_id}' report step {step_number} did not record planned \
             '{planned}', actual '{actual}', execution '{execution}', and outcomes '{outcomes}'; \
             last observation: {observed}"
        );
        match read_transaction_impact_report(world, &transaction_id).await {
            Ok(report) => {
                let Some(step) = report.execution_steps().get(index) else {
                    observed =
                        format!("report contains {} step(s)", report.execution_steps().len());
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                let observed_planned = step.planned().pause.level().as_str();
                let observed_actual = step.actual_quiesce_level().as_str();
                let observed_execution = execution_outcome_name(&step.actual().outcome);
                let observed_outcomes = step
                    .actual()
                    .quiescence
                    .iter()
                    .map(|engagement| {
                        engagement
                            .outcomes
                            .iter()
                            .map(quiescence_outcome_name)
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                observed = format!(
                    "planned={observed_planned}, actual={observed_actual}, \
                     execution={observed_execution}, outcomes={observed_outcomes}"
                );
                if observed_planned == planned
                    && observed_actual == actual
                    && observed_execution == execution
                    && observed_outcomes == outcomes
                {
                    return;
                }
            }
            Err(error) => observed = error,
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "transaction {string} report step {int} eventually records recovery rebuild effects")]
async fn then_transaction_report_records_recovery_rebuilds(
    world: &mut ScenarioWorld,
    transaction_id: String,
    step_number: usize,
) {
    let transaction_id = expand_placeholders(world, &transaction_id);
    let index = step_number
        .checked_sub(1)
        .assured("transaction report step numbers are one-based");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        tokio::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "transaction '{transaction_id}' report step {step_number} did not record recovery \
             rebuild effects"
        );
        if let Ok(report) = read_transaction_impact_report(world, &transaction_id).await
            && let Some(step) = report.execution_steps().get(index)
        {
            let rebuilds = step.actual().effects.rebuilds.as_slice();
            if rebuilds.len() > step.planned().effects.rebuilds.len()
                && rebuilds
                    .iter()
                    .any(|rebuild| rebuild.reason == nervix_models::RebuildReason::Recovery)
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "transaction {string} is eventually removed")]
async fn then_transaction_is_eventually_removed(world: &mut ScenarioWorld, transaction_id: String) {
    let transaction_id = expand_placeholders(world, &transaction_id);
    let leader = current_leader_node(world).await;
    let expected_id = format!("id={transaction_id}");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_output = String::new();
    loop {
        tokio::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "transaction '{transaction_id}' tombstone was not removed; last output: {last_output}"
        );
        match world
            .cluster()
            .run_command(&leader, &world.domain, "SHOW TRANSACTIONS;")
            .await
        {
            Ok(output) if !output.lines().any(|line| line.contains(&expected_id)) => {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => last_output = output,
            Err(error) => last_output = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the leader reported by node {string} is saved as placeholder {string}")]
async fn then_leader_reported_by_node_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    node_id: String,
    placeholder: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let leader = world
        .cluster()
        .current_leader(&node_id)
        .await
        .expect("failed to read node leader")
        .unwrap_or_else(|| panic!("node '{node_id}' does not report a leader"));
    world.placeholders.insert(placeholder, leader);
}

#[when(expr = "the web console is opened on node {string}")]
async fn when_web_console_is_opened_on_node(world: &mut ScenarioWorld, node_id: String) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    close_browser(world).await;
    let node_id = expand_placeholders(world, &node_id);
    let url = world
        .cluster()
        .web_console_url(&node_id)
        .expect("failed to resolve web console URL");
    open_web_console_page(world, &url)
        .await
        .expect("failed to open web console");
}

#[when("the web console is opened on the leader node")]
async fn when_web_console_is_opened_on_leader_node(world: &mut ScenarioWorld) {
    let leader = current_leader_node(world).await;
    when_web_console_is_opened_on_node(world, leader).await;
}

#[when(expr = "the web console is opened on the leader node with password {string}")]
async fn when_web_console_is_opened_on_leader_node_with_password(
    world: &mut ScenarioWorld,
    password: String,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    close_browser(world).await;
    let password = expand_placeholders(world, &password);
    let leader = current_leader_node(world).await;
    let url = world
        .cluster()
        .web_console_url_with_password(&leader, &password)
        .expect("failed to resolve web console URL");
    open_web_console_page(world, &url)
        .await
        .expect("failed to open web console");
}

#[when(expr = "the browser viewport is resized to {int} by {int}")]
async fn when_browser_viewport_is_resized(world: &mut ScenarioWorld, width: usize, height: usize) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before viewport changes");
    page.set_viewport_size(Viewport {
        width: u32::try_from(width).assured("browser viewport widths in cucumber features fit u32"),
        height: u32::try_from(height)
            .assured("browser viewport heights in cucumber features fit u32"),
    })
    .await
    .expect("browser viewport must be resizable");
}

#[when(expr = "selector {string} is filled with {string}")]
async fn when_selector_is_filled_with(world: &mut ScenarioWorld, selector: String, value: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    let value = expand_placeholders(world, &value);
    let locator = page.locator(&selector);
    locator
        .fill(&value, None)
        .await
        .expect("selector must be fillable");
}

#[when(expr = "selector {string} is pressed with {string}")]
async fn when_selector_is_pressed_with(world: &mut ScenarioWorld, selector: String, key: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    let key = expand_placeholders(world, &key);
    let locator = page.locator(&selector);
    locator
        .press(&key, None)
        .await
        .expect("selector must accept key press");
}

#[when(expr = "the web console submits {string} {int} times")]
async fn when_web_console_submits_repeatedly(
    world: &mut ScenarioWorld,
    command: String,
    count: usize,
) {
    let page = world
        .browser_page
        .as_ref()
        .assured("the scenario opened the console before submitting commands");
    let command = expand_placeholders(world, &command);
    let input = page.locator(".prompt-row input");
    for _ in 0..count {
        tokio::task::consume_budget().await;
        input
            .fill(&command, None)
            .await
            .unwrap_or_else(|error| panic!("console input could not be filled: {error}"));
        input
            .press("Enter", None)
            .await
            .unwrap_or_else(|error| panic!("console input could not be submitted: {error}"));
    }
}

#[when(expr = "selector {string} is typed with {string}")]
async fn when_selector_is_typed_with(world: &mut ScenarioWorld, selector: String, value: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    let value = expand_placeholders(world, &value);
    let locator = page.locator(&selector);
    locator
        .press_sequentially(&value, None)
        .await
        .expect("selector must accept typed text");
}

#[when(expr = "selector {string} is clicked")]
async fn when_selector_is_clicked(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    page.locator(&selector)
        .click(None)
        .await
        .expect("selector must be clickable");
}

#[when(expr = "selector {string} is clicked by script")]
async fn when_selector_is_clicked_by_script(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    page.locator(&selector)
        .evaluate::<(), ()>("element => element.click()", None::<()>)
        .await
        .expect("selector must be script-clickable");
}

#[when(expr = "selector {string} uploads resource directory {string}")]
async fn when_selector_uploads_resource_directory(
    world: &mut ScenarioWorld,
    selector: String,
    placeholder: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector actions");
    let selector = expand_placeholders(world, &selector);
    let resource_dir = resource_directory_path(world, &placeholder);
    let mut files = Vec::new();
    collect_regular_files(&resource_dir, &mut files);
    assert!(
        !files.is_empty(),
        "resource directory '{}' should contain files",
        resource_dir.display()
    );
    let payloads = files
        .iter()
        .map(|file| {
            let name = file
                .strip_prefix(&resource_dir)
                .expect("uploaded file must be under resource directory")
                .to_string_lossy()
                .replace('\\', "/");
            FilePayload::new(
                name,
                "application/octet-stream",
                std::fs::read(file).expect("uploaded file should be readable"),
            )
        })
        .collect::<Vec<_>>();
    page.locator(&selector)
        .set_input_files_payload_multiple(&payloads, None)
        .await
        .expect("selector must accept uploaded files");
}

fn collect_regular_files(directory: &Path, files: &mut Vec<PathBuf>) {
    let mut entries = std::fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read '{}': {error}", directory.display()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| panic!("failed to collect '{}': {error}", directory.display()));
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .unwrap_or_else(|error| panic!("failed to inspect '{}': {error}", path.display()));
        if file_type.is_dir() {
            collect_regular_files(&path, files);
        } else if file_type.is_file() {
            files.push(path);
        }
    }
}

#[when(expr = "these NSPL commands are executed on node {string}")]
async fn when_these_nspl_commands_are_executed_on_node(
    world: &mut ScenarioWorld,
    node_id: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let node_id = expand_placeholders(world, &node_id);
    let commands = expand_placeholders(world, docstring(step));
    let session = if commands_are_retry_safe_session_ops(&commands) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match execute_nspl_commands_on_node(world, &node_id, &commands).await {
                Ok(session) => break session,
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "failed to execute NSPL setup command on requested node: {error:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    } else {
        execute_nspl_commands_on_node(world, &node_id, &commands)
            .await
            .expect("failed to execute NSPL setup command on requested node")
    };
    world.active_session = Some(session);
    world.active_session_node = Some(node_id);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

/// Fill every bulk worker on a node and hold them, so the scenario can measure management work
/// against a class that is genuinely full rather than one that merely looks busy.
#[when(expr = "bulk execution on node {string} is occupied")]
async fn when_bulk_execution_is_occupied(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    let node_name = crate::common::cluster::node_name(&node_id);
    let fault_injection = world.fault_injection.clone();
    tokio::time::timeout(
        Duration::from_secs(30),
        fault_injection.occupy_bulk_execution(&node_name),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("bulk execution on '{node_id}' never filled its workers: {error}")
    });
}

#[when(expr = "bulk execution on node {string} is released")]
async fn when_bulk_execution_is_released(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .release_bulk_execution(&crate::common::cluster::node_name(&node_id));
}

/// Run NSPL on a node and require it to finish inside a bound, which is how a scenario states that
/// one class of work was not admitted behind another.
#[then(expr = "within {string} these NSPL commands complete on node {string}")]
async fn then_nspl_commands_complete_within(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    #[step] step: &Step,
) {
    let limit =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let commands = expand_placeholders(world, docstring(step));
    world.last_command_error = None;
    world.last_command_output = None;
    let started = Instant::now();
    let session = tokio::time::timeout(
        limit,
        execute_nspl_commands_on_node(world, &node_id, &commands),
    )
    .await
    .unwrap_or_else(|_| panic!("NSPL commands on '{node_id}' did not complete within {limit:?}"))
    .unwrap_or_else(|error| panic!("failed to execute NSPL commands on '{node_id}': {error:?}"));
    world.last_cluster_operation_elapsed = Some(started.elapsed());
    world.active_session = Some(session);
    world.active_session_node = Some(node_id);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[then(expr = "within {string} these NSPL commands complete on the leader node")]
#[when(expr = "within {string} these NSPL commands complete on the leader node")]
async fn then_nspl_commands_complete_on_leader_within(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let leader = current_leader_node(world).await;
    then_nspl_commands_complete_within(world, duration, leader, step).await;
}

#[given("the leader node is configured with these NSPL commands")]
async fn given_the_leader_node_is_configured_with_these_nspl_commands(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    let leader = current_leader_node(world).await;
    let commands = expand_placeholders(world, docstring(step));
    let session = execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .expect("failed to execute NSPL setup command on leader");
    world.active_session = Some(session);
    world.active_session_node = Some(leader);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[when(expr = "these NSPL commands fail with {string}")]
async fn when_these_nspl_commands_fail_with(
    world: &mut ScenarioWorld,
    expected_error: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;

    let expected_error = expand_placeholders(world, &expected_error);
    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    match execute_nspl_commands_on_node(world, &leader, &commands).await {
        Ok(_) => panic!("expected commands to fail with {:?}", expected_error),
        Err(error) => {
            assert!(
                error.contains(&expected_error),
                "expected error containing {:?}, got: {error}",
                expected_error
            );
            world.last_command_error = Some(error);
        }
    }
}

#[when(expr = "within {string} these NSPL commands on node {string} eventually fail with {string}")]
#[then(expr = "within {string} these NSPL commands on node {string} eventually fail with {string}")]
async fn when_within_these_nspl_commands_on_node_eventually_fail_with(
    world: &mut ScenarioWorld,
    within: String,
    node_id: String,
    expected_error: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;

    let timeout = humantime::parse_duration(&within).expect("within must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let expected_error = expand_placeholders(world, &expected_error);
    let commands = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + timeout;
    let mut last_outcome;
    loop {
        match run_nspl_commands_on_node(world, &node_id, &commands).await {
            Ok(output) => last_outcome = format!("command succeeded: {output}"),
            Err(error) => {
                if error.contains(&expected_error) {
                    world.last_command_error = Some(error);
                    return;
                }
                last_outcome = error;
            }
        }
        assert!(
            Instant::now() < deadline,
            "expected an error containing {expected_error:?} within {within}, last outcome: \
             {last_outcome}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[when("these NSPL commands fail")]
async fn when_these_nspl_commands_fail(world: &mut ScenarioWorld, #[step] step: &Step) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;

    let commands = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    match execute_nspl_commands_on_node(world, &leader, &commands).await {
        Ok(_) => panic!("expected commands to fail"),
        Err(error) => {
            append_cucumber_log_line(&format!("expected command failure observed: {error}"));
            world.last_command_error = Some(error);
        }
    }
}

#[when(expr = "the ingestor logic fixture {string} starts with output schema {string} and program")]
async fn when_the_ingestor_logic_fixture_starts_with_output_schema_and_program(
    world: &mut ScenarioWorld,
    transport_fixture: String,
    output_schema_fixture: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.last_subscription_payload = None;

    let transport = IngestorLogicTransportFixture::parse(&transport_fixture);
    let output_schema = IngestorLogicOutputSchemaFixture::parse(&output_schema_fixture);
    transport.prepare(world).await;
    let commands = expand_placeholders(
        world,
        &build_ingestor_logic_commands(transport, output_schema, docstring(step), true),
    );
    let leader = current_leader_node(world).await;
    let session = execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .expect("failed to start ingestor logic fixture on leader");
    if transport.executes_on_scheduled_owner() {
        let placement = PhaseDeadline::after(Duration::from_secs(5));
        let mut last_output = None;
        let owner = loop {
            tokio::task::consume_budget().await;
            assert!(
                !placement.has_passed(),
                "timed out waiting for logic_ingestor schedule placement; last output: \
                 {last_output:?}"
            );
            let output = world
                .cluster()
                .status_text(&leader, placement)
                .await
                .unwrap_or_else(|error| {
                    panic!("failed to inspect ingestor logic schedule: {error:#}")
                });
            if let Some((owner, _)) = scheduled_node_placement_from_status(
                &output,
                &world.domain,
                "ingestor",
                "logic_ingestor",
            ) {
                break owner.to_string();
            }
            last_output = Some(output);
            placement.pause(Duration::from_millis(50)).await;
        };
        if owner == leader {
            world.active_session = Some(session);
        } else {
            let owner_session = execute_nspl_commands_on_node(
                world,
                &owner,
                "CREATE SUBSCRIPTION logic_notifications_owner_subscription TO \
                 logic_notifications;",
            )
            .await
            .expect("failed to create ingestor logic subscription on scheduled owner");
            world.active_session = Some(owner_session);
        }
        world.active_session_node = Some(owner);
    } else {
        world.active_session = Some(session);
        world.active_session_node = Some(leader);
    }
    world.active_session_has_subscription = true;
    transport.await_ready(world).await;
}

#[when(
    expr = "the ingestor logic fixture {string} fails to start with output schema {string} and \
            program"
)]
async fn when_the_ingestor_logic_fixture_fails_to_start_with_output_schema_and_program(
    world: &mut ScenarioWorld,
    transport_fixture: String,
    output_schema_fixture: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.last_server_error = None;
    world.last_subscription_payload = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;

    let transport = IngestorLogicTransportFixture::parse(&transport_fixture);
    let output_schema = IngestorLogicOutputSchemaFixture::parse(&output_schema_fixture);
    transport.prepare(world).await;
    let commands = expand_placeholders(
        world,
        &build_ingestor_logic_commands(transport, output_schema, docstring(step), false),
    );
    let leader = current_leader_node(world).await;
    match execute_nspl_commands_on_node(world, &leader, &commands).await {
        Ok(_) => panic!("expected ingestor logic fixture to fail during leader validation"),
        Err(error) => {
            append_cucumber_log_line(&format!("logic fixture start failure observed: {error}"));
            world.last_command_error = Some(error);
        }
    }
}

#[when(expr = "the ingestor logic transport {string} delivers payload fixture {string}")]
async fn when_the_ingestor_logic_transport_delivers_payload_fixture(
    world: &mut ScenarioWorld,
    transport_fixture: String,
    payload_fixture: String,
) {
    let transport = IngestorLogicTransportFixture::parse(&transport_fixture);
    let payload_fixture = IngestorLogicPayloadFixture::parse(&payload_fixture);
    for payload in payload_fixture.payloads() {
        transport.deliver(world, payload).await;
    }
}

#[when(
    expr = "the ingestor logic transport {string} delivers payload fixture {string} with headers"
)]
async fn when_the_ingestor_logic_transport_delivers_payload_fixture_with_headers(
    world: &mut ScenarioWorld,
    transport_fixture: String,
    payload_fixture: String,
) {
    let transport = IngestorLogicTransportFixture::parse(&transport_fixture);
    let payload_fixture = IngestorLogicPayloadFixture::parse(&payload_fixture);
    for payload in payload_fixture.payloads() {
        transport.deliver_with_headers(world, payload).await;
    }
}

#[when(expr = "these NSPL commands fail on a follower node with {string}")]
async fn when_these_nspl_commands_fail_on_a_follower_node_with(
    world: &mut ScenarioWorld,
    expected_error: String,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_command_output = None;
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    world.last_server_error = None;
    let follower = world
        .cluster()
        .any_follower_node("node-1")
        .await
        .expect("failed to resolve follower node");
    let commands = expand_placeholders(world, docstring(step));
    let mut session = world
        .cluster()
        .open_session(&follower, &world.domain)
        .await
        .expect("failed to open raw follower session");
    for command in nspl_statements(&commands) {
        match session.run_command(&command).await {
            Ok(_) => panic!(
                "expected follower commands to fail with {:?}",
                expected_error
            ),
            Err(error) => {
                let error = error.to_string();
                assert!(
                    error.contains(&expected_error),
                    "expected error containing {:?}, got: {error}",
                    expected_error
                );
                world.last_command_error = Some(error);
            }
        }
    }
}

#[then(expr = "the ingestor logic expectation {string} is observed")]
async fn then_the_ingestor_logic_expectation_is_observed(
    world: &mut ScenarioWorld,
    expectation_fixture: String,
) {
    let expectation = IngestorLogicExpectationFixture::parse(&expectation_fixture);
    expectation.assert_observed(world).await;
}

#[then("the last command output contains")]
async fn then_last_command_output_contains(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step));
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    assert!(
        output.contains(expected.trim()),
        "expected command output fragment {} in output, got: {output}",
        expected.trim()
    );
}

/// Runs `SHOW CREATE EMITTER` for every emitter the table names and requires its rendering to
/// contain the clause beside it. The first row names the columns.
#[then("SHOW CREATE EMITTER on the leader node renders these clauses")]
async fn then_show_create_emitter_renders_these_clauses(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let table = step
        .table
        .as_ref()
        .expect("the step lists each emitter with the clause it must render");
    let leader = current_leader_node(world).await;
    for row in table.rows.iter().skip(1) {
        let [emitter, clause] = row.as_slice() else {
            panic!("each row names an emitter and one clause, got {row:?}");
        };
        let output = world
            .cluster()
            .run_command(
                &leader,
                &world.domain,
                &format!("SHOW CREATE EMITTER {emitter};"),
            )
            .await
            .unwrap_or_else(|error| panic!("SHOW CREATE EMITTER {emitter} failed: {error}"));
        assert!(
            output.contains(clause.as_str()),
            "expected SHOW CREATE EMITTER {emitter} to contain {clause:?}, got: {output}"
        );
    }
}

#[then("the last command output is saved as the relocation plan")]
async fn then_last_command_output_is_saved_as_the_relocation_plan(world: &mut ScenarioWorld) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a relocation plan must exist before it can be saved");
    world.saved_relocation_plan = Some(output.trim().to_string());
}

#[then("the last command output contains the saved relocation plan")]
async fn then_last_command_output_contains_the_saved_relocation_plan(world: &mut ScenarioWorld) {
    let expected = world
        .saved_relocation_plan
        .as_deref()
        .expect("a relocation plan must be saved before assertion");
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    assert!(
        output.contains(expected),
        "expected the executed relocation output to contain the described plan\n{expected}\ngot: \
         {output}"
    );
}

#[then("the last command error contains")]
async fn then_last_command_error_contains(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step));
    let error = world
        .last_command_error
        .as_deref()
        .expect("a command error must exist before assertion");
    assert!(
        error.contains(expected.trim()),
        "expected command error fragment {} in error, got: {error}",
        expected.trim()
    );
}

#[then(expr = "selector {string} contains {string} exactly {int} times")]
async fn then_selector_contains_text_exactly_times(
    world: &mut ScenarioWorld,
    selector: String,
    expected: String,
    expected_count: usize,
) {
    let page = world
        .browser_page
        .as_ref()
        .assured("the scenario opened the console before asserting its elements");
    let selector = expand_placeholders(world, &selector);
    let expected = expand_placeholders(world, &expected);
    let locator = page.locator(&selector);
    let deadline = Instant::now() + WEB_CONSOLE_ASSERTION_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        let text = texts.join("\n");
        let count = text.matches(&expected).count();
        if count == expected_count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to contain '{expected}' {expected_count} times, got \
             {count}: {text}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} has at most {int} elements")]
async fn then_selector_has_at_most_elements(
    world: &mut ScenarioWorld,
    selector: String,
    limit: usize,
) {
    let page = world
        .browser_page
        .as_ref()
        .assured("the scenario opened the console before asserting its elements");
    let selector = expand_placeholders(world, &selector);
    let count = page
        .locator(&selector)
        .all_inner_texts()
        .await
        .unwrap_or_else(|error| panic!("selector text could not be read: {error}"))
        .len();
    assert!(
        count <= limit,
        "expected at most {limit} elements for '{selector}', got {count}"
    );
}

#[then(expr = "selector {string} contains {string}")]
async fn then_selector_contains_text(
    world: &mut ScenarioWorld,
    selector: String,
    expected: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let expected = expand_placeholders(world, &expected);
    let locator = page.locator(&selector);
    let deadline = Instant::now() + WEB_CONSOLE_ASSERTION_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        let text = texts.join("\n");
        if text.contains(&expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to contain '{expected}', got '{text}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(regex = r#"^selector "([^"]+)" contains$"#)]
async fn then_selector_contains_docstring(
    world: &mut ScenarioWorld,
    selector: String,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step).trim());
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let locator = page.locator(&selector);
    let deadline = Instant::now() + WEB_CONSOLE_ASSERTION_TIMEOUT;
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        let text = texts.join("\n");
        if text.contains(&expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to contain:\n{expected}\ngot:\n{text}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} contains {string} for {int} milliseconds")]
async fn then_selector_contains_text_for_milliseconds(
    world: &mut ScenarioWorld,
    selector: String,
    expected: String,
    duration_milliseconds: usize,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let expected = expand_placeholders(world, &expected);
    let locator = page.locator(&selector);
    let deadline = Instant::now() + Duration::from_millis(duration_milliseconds.arch_into());
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        let text = texts.join("\n");
        assert!(
            text.contains(&expected),
            "expected selector '{selector}' to keep containing '{expected}', got '{text}'"
        );
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} does not contain {string}")]
async fn then_selector_does_not_contain_text(
    world: &mut ScenarioWorld,
    selector: String,
    unexpected: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let unexpected = expand_placeholders(world, &unexpected);
    let locator = page.locator(&selector);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        let text = texts.join("\n");
        assert!(
            !text.contains(&unexpected),
            "expected selector '{selector}' not to contain '{unexpected}', got '{text}'"
        );
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} does not exist")]
async fn then_selector_does_not_exist(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let script = format!(
        r#"
        () => document.querySelectorAll({selector:?}).length === 0
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        tokio::task::consume_budget().await;
        let missing = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("selector existence must be readable");
        assert!(missing, "expected selector '{selector}' not to exist");
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} eventually disappears")]
async fn then_selector_eventually_disappears(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let locator = page.locator(&selector);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let texts = locator
            .all_inner_texts()
            .await
            .expect("selector text must be readable");
        if texts.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to disappear after its server acknowledgement, still \
             showing {texts:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} has value {string}")]
async fn then_selector_has_value(world: &mut ScenarioWorld, selector: String, expected: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let expected = expand_placeholders(world, &expected);
    let locator = page.locator(&selector);
    locator
        .wait_for(Some(
            WaitForOptions::builder()
                .state(WaitForState::Visible)
                .timeout(10_000.0)
                .build(),
        ))
        .await
        .expect("selector must become visible");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let value = locator
            .input_value(None)
            .await
            .expect("selector value must be readable");
        if value == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to have value '{expected}', got '{value}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} is scrolled to bottom")]
async fn then_selector_is_scrolled_to_bottom(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let script = format!(
        r#"
        async () => {{
            const el = document.querySelector({selector:?});
            if (!el) {{
                return false;
            }}
            return Math.abs(el.scrollHeight - el.clientHeight - el.scrollTop) <= 2;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let is_scrolled_to_bottom = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("selector scroll position must be readable");
        if is_scrolled_to_bottom {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to be scrolled to bottom"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} is pinned to viewport bottom")]
async fn then_selector_is_pinned_to_viewport_bottom(world: &mut ScenarioWorld, selector: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let selector = expand_placeholders(world, &selector);
    let script = format!(
        r#"
        () => {{
            const el = document.querySelector({selector:?});
            if (!el) {{
                return false;
            }}
            const style = window.getComputedStyle(el);
            const rect = el.getBoundingClientRect();
            return style.display !== 'none'
                && style.visibility !== 'hidden'
                && rect.width > 0
                && rect.height > 0
                && rect.top >= 0
                && rect.left >= 0
                && rect.right <= window.innerWidth + 2
                && Math.abs(rect.bottom - window.innerHeight) <= 2;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let is_pinned = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("selector viewport position must be readable");
        if is_pinned {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{selector}' to be pinned to viewport bottom"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "selector {string} does not overlap selector {string}")]
async fn then_selector_does_not_overlap_selector(
    world: &mut ScenarioWorld,
    first: String,
    second: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before selector assertions");
    let first = expand_placeholders(world, &first);
    let second = expand_placeholders(world, &second);
    let script = format!(
        r#"
        () => {{
            const first = document.querySelector({first:?});
            const second = document.querySelector({second:?});
            if (!first || !second) {{
                return false;
            }}
            const a = first.getBoundingClientRect();
            const b = second.getBoundingClientRect();
            return a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let does_not_overlap = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("selector positions must be readable");
        if does_not_overlap {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected selector '{first}' not to overlap selector '{second}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph item {string} does not overlap graph item {string}")]
async fn then_graph_item_does_not_overlap_graph_item(
    world: &mut ScenarioWorld,
    first: String,
    second: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let first = expand_placeholders(world, &first);
    let second = expand_placeholders(world, &second);
    let script = format!(
        r#"
        () => {{
            const itemByLabel = (label) => Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === label);
            const first = itemByLabel({first:?});
            const second = itemByLabel({second:?});
            if (!first || !second) {{
                return false;
            }}
            const a = first.getBoundingClientRect();
            const b = second.getBoundingClientRect();
            return a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let does_not_overlap = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("graph item positions must be readable");
        if does_not_overlap {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph item '{first}' not to overlap graph item '{second}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph item {string} has graph width at least {int} pixels")]
async fn then_graph_item_has_graph_width_at_least(
    world: &mut ScenarioWorld,
    item: String,
    expected_width: i32,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            if (!item) {{
                return null;
            }}
            return Number.parseFloat(item.style.width || getComputedStyle(item).width);
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let actual_width = page
            .evaluate::<(), Option<f64>>(&script, None::<&()>)
            .await
            .expect("graph item width must be readable");
        if actual_width.is_some_and(|width| width >= f64::from(expected_width)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph item '{item}' to have graph width at least {expected_width}px, got \
             {actual_width:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph item {string} has status {string}")]
async fn then_graph_item_has_status(world: &mut ScenarioWorld, item: String, expected: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let item = expand_placeholders(world, &item);
    let expected = expand_placeholders(world, &expected);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            return item ? item.dataset.status : null;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let status = page
            .evaluate::<(), Option<String>>(&script, None::<&()>)
            .await
            .expect("graph item status must be readable");
        if status.as_deref() == Some(expected.as_str()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph item '{item}' to have status '{expected}', got '{status:?}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph item {string} is highlighted by graph search")]
async fn then_graph_item_is_highlighted_by_graph_search(world: &mut ScenarioWorld, item: String) {
    then_graph_item_search_highlight_matches(world, item, true).await;
}

#[then(expr = "graph item {string} is not highlighted by graph search")]
async fn then_graph_item_is_not_highlighted_by_graph_search(
    world: &mut ScenarioWorld,
    item: String,
) {
    then_graph_item_search_highlight_matches(world, item, false).await;
}

async fn then_graph_item_search_highlight_matches(
    world: &mut ScenarioWorld,
    item: String,
    expected: bool,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            return item ? item.dataset.searchHighlight === "true" : null;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let highlighted = page
            .evaluate::<(), Option<bool>>(&script, None::<&()>)
            .await
            .expect("graph search highlight state must be readable");
        if highlighted == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph item '{item}' search highlight to be {expected}, got {highlighted:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph search highlights exactly {int} graph items")]
async fn then_graph_search_highlights_exactly_graph_items(
    world: &mut ScenarioWorld,
    expected_count: i32,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = r#"
        () => Array
            .from(document.querySelectorAll(".graph-hit-layer button"))
            .filter((element) => element.dataset.searchHighlight === "true")
            .length
    "#;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let count = page
            .evaluate::<(), i32>(script, None::<&()>)
            .await
            .expect("graph search highlight count must be readable");
        if count == expected_count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph search to highlight {expected_count} graph items, got {count}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Shared preamble for graph geometry probes: item boxes, edge sample points, and badge boxes,
/// all in screen space so assertions read the same picture the user sees.
const GRAPH_GEOMETRY_HELPERS: &str = r#"
    const itemBoxes = () => Array
        .from(document.querySelectorAll(".graph-hit-layer button"))
        .map((element) => ({
            label: element.dataset.label,
            box: element.getBoundingClientRect(),
        }));
    const badgeBoxes = () => Array
        .from(document.querySelectorAll(".graph-edge-metric"))
        .map((element) => ({
            source: element.dataset.source,
            target: element.dataset.target,
            box: element.getBoundingClientRect(),
        }));
    const edgePaths = () => Array.from(document.querySelectorAll("path.graph-edge"));
    const edgePath = (source, target) => edgePaths()
        .find((path) => path.dataset.source.endsWith(":" + source)
            && path.dataset.target.endsWith(":" + target));
    const samplePath = (path) => {
        const matrix = path.getScreenCTM();
        const total = path.getTotalLength();
        const points = [];
        for (let at = 0; at <= total; at += 6) {
            const raw = path.getPointAtLength(at);
            points.push({
                x: raw.x * matrix.a + raw.y * matrix.c + matrix.e,
                y: raw.x * matrix.b + raw.y * matrix.d + matrix.f,
            });
        }
        return points;
    };
    const inside = (point, box, margin) => point.x > box.left + margin
        && point.x < box.right - margin
        && point.y > box.top + margin
        && point.y < box.bottom - margin;
    const overlaps = (a, b) => a.left < b.right && b.left < a.right
        && a.top < b.bottom && b.top < a.bottom;
"#;

#[then(expr = "graph items {string} are horizontally aligned")]
async fn then_graph_items_are_horizontally_aligned(world: &mut ScenarioWorld, items: String) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let items = expand_placeholders(world, &items);
    let labels = items
        .split(',')
        .map(|label| label.trim().to_string())
        .collect::<Vec<_>>();
    let script = format!(
        r#"
        () => {{
            {GRAPH_GEOMETRY_HELPERS}
            const wanted = {labels:?};
            const boxes = itemBoxes();
            const centres = [];
            for (const label of wanted) {{
                const found = boxes.find((entry) => entry.label === label);
                if (!found) {{
                    return `missing item ${{label}}`;
                }}
                centres.push(Math.round((found.box.top + found.box.bottom) / 2));
            }}
            const first = centres[0];
            if (centres.every((centre) => Math.abs(centre - first) <= 1)) {{
                return "OK";
            }}
            return `not aligned: ${{JSON.stringify(wanted.map((l, i) => [l, centres[i]]))}}`;
        }}
        "#
    );
    assert_graph_probe(page, &script, "items should share one horizontal axis").await;
}

#[then(expr = "graph item {string} is left of graph item {string}")]
async fn then_graph_item_is_left_of_graph_item(
    world: &mut ScenarioWorld,
    first: String,
    second: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let first = expand_placeholders(world, &first);
    let second = expand_placeholders(world, &second);
    let script = format!(
        r#"
        () => {{
            {GRAPH_GEOMETRY_HELPERS}
            const boxes = itemBoxes();
            const left = boxes.find((entry) => entry.label === {first:?});
            const right = boxes.find((entry) => entry.label === {second:?});
            if (!left || !right) {{
                return `missing item left=${{Boolean(left)}} right=${{Boolean(right)}}`;
            }}
            if (left.box.right <= right.box.left) {{
                return "OK";
            }}
            return `not left of: ${{Math.round(left.box.right)}} vs ${{Math.round(right.box.left)}}`;
        }}
        "#
    );
    assert_graph_probe(page, &script, "records must read left to right").await;
}

#[then("no graph edge crosses any graph item")]
async fn then_no_graph_edge_crosses_any_graph_item(world: &mut ScenarioWorld) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = format!(
        r#"
        () => {{
            {GRAPH_GEOMETRY_HELPERS}
            const boxes = itemBoxes();
            if (boxes.length === 0) {{
                return "no graph items rendered";
            }}
            for (const path of edgePaths()) {{
                const source = path.dataset.source;
                const target = path.dataset.target;
                for (const point of samplePath(path)) {{
                    for (const entry of boxes) {{
                        if (source.endsWith(":" + entry.label) || target.endsWith(":" + entry.label)) {{
                            continue;
                        }}
                        if (inside(point, entry.box, 1)) {{
                            return `edge ${{source}} -> ${{target}} crosses ${{entry.label}}`;
                        }}
                    }}
                }}
            }}
            return "OK";
        }}
        "#
    );
    assert_graph_probe(page, &script, "edges must stay clear of items").await;
}

#[then("no graph badge overlaps another graph item or badge")]
async fn then_no_graph_badge_overlaps(world: &mut ScenarioWorld) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = format!(
        r#"
        () => {{
            {GRAPH_GEOMETRY_HELPERS}
            const badges = badgeBoxes();
            const boxes = itemBoxes();
            for (let index = 0; index < badges.length; index += 1) {{
                for (const entry of boxes) {{
                    if (overlaps(badges[index].box, entry.box)) {{
                        return `badge ${{badges[index].source}} -> ${{badges[index].target}} covers ${{entry.label}}`;
                    }}
                }}
                for (let other = index + 1; other < badges.length; other += 1) {{
                    if (overlaps(badges[index].box, badges[other].box)) {{
                        return `badges ${{badges[index].target}} and ${{badges[other].target}} overlap`;
                    }}
                }}
            }}
            return "OK";
        }}
        "#
    );
    assert_graph_probe(page, &script, "badges must not cover anything").await;
}

#[then(
    expr = "graph edge from {string} to {string} departs at a different port than graph edge from \
            {string} to {string}"
)]
async fn then_graph_edges_depart_at_different_ports(
    world: &mut ScenarioWorld,
    first_source: String,
    first_target: String,
    second_source: String,
    second_target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let first_source = expand_placeholders(world, &first_source);
    let first_target = expand_placeholders(world, &first_target);
    let second_source = expand_placeholders(world, &second_source);
    let second_target = expand_placeholders(world, &second_target);
    let script = format!(
        r#"
        () => {{
            {GRAPH_GEOMETRY_HELPERS}
            const first = edgePath({first_source:?}, {first_target:?});
            const second = edgePath({second_source:?}, {second_target:?});
            if (!first || !second) {{
                return `missing edge first=${{Boolean(first)}} second=${{Boolean(second)}}`;
            }}
            const start = (path) => samplePath(path)[0];
            const a = start(first);
            const b = start(second);
            if (Math.abs(a.y - b.y) >= 8) {{
                return "OK";
            }}
            return `shared port at y=${{Math.round(a.y)}} and ${{Math.round(b.y)}}`;
        }}
        "#
    );
    assert_graph_probe(page, &script, "fan-out must leave through distinct ports").await;
}

#[then(expr = "graph action edges {string} and {string} from {string} to {string} are drawn apart")]
async fn then_parallel_graph_edges_are_drawn_apart(
    world: &mut ScenarioWorld,
    first_kind: String,
    second_kind: String,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let first_kind = first_kind.replace(' ', "_").to_ascii_uppercase();
    let second_kind = second_kind.replace(' ', "_").to_ascii_uppercase();
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            {GRAPH_GEOMETRY_HELPERS}
            const relation = (kind) => edgePaths()
                .find((path) => path.dataset.kind === kind
                    && path.dataset.source.endsWith(":" + {source:?})
                    && path.dataset.target.endsWith(":" + {target:?}));
            const first = relation({first_kind:?});
            const second = relation({second_kind:?});
            if (!first || !second) {{
                return `missing edge first=${{Boolean(first)}} second=${{Boolean(second)}}`;
            }}
            const firstPoints = samplePath(first);
            const secondPoints = samplePath(second);
            const departures = Math.abs(firstPoints[0].y - secondPoints[0].y);
            const arrivals = Math.abs(
                firstPoints[firstPoints.length - 1].y - secondPoints[secondPoints.length - 1].y
            );
            if (departures >= 8 && arrivals >= 8) {{
                return "OK";
            }}
            return `drawn on one line: departures ${{Math.round(departures)}}px apart, arrivals ${{Math.round(arrivals)}}px apart`;
        }}
        "#
    );
    assert_graph_probe(
        page,
        &script,
        "two relations between one pair of items to leave and arrive through their own ports",
    )
    .await;
}

#[then(expr = "graph edge from {string} to {string} is a return path")]
async fn then_graph_edge_is_a_return_path(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            {GRAPH_GEOMETRY_HELPERS}
            const path = edgePath({source:?}, {target:?});
            if (!path) {{
                return "edge is not drawn";
            }}
            return path.dataset.feedback === "true" ? "OK" : "edge is not marked as a return path";
        }}
        "#
    );
    assert_graph_probe(page, &script, "a backwards edge must be marked").await;
}

#[then(expr = "branch group {string} header shows key fields {string}")]
async fn then_branch_group_header_shows_key_fields(
    world: &mut ScenarioWorld,
    branch: String,
    fields: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let branch = expand_placeholders(world, &branch);
    let fields = expand_placeholders(world, &fields);
    let expected = fields
        .split(',')
        .map(|field| field.trim().to_string())
        .collect::<Vec<_>>();
    let script = format!(
        r#"
        () => {{
            const body = Array
                .from(document.querySelectorAll("path.graph-branch-body"))
                .find((element) => element.dataset.branch === {branch:?});
            if (!body) {{
                return "branch group is not drawn";
            }}
            const declared = (body.dataset.keyFields || "")
                .split(",")
                .map((field) => field.trim())
                .filter((field) => field.length > 0);
            const wanted = {expected:?};
            if (declared.length === wanted.length
                && wanted.every((field, index) => declared[index] === field)) {{
                return "OK";
            }}
            return `key fields are ${{JSON.stringify(declared)}}`;
        }}
        "#
    );
    assert_graph_probe(page, &script, "a branch group names its own key fields").await;
}

#[then(expr = "branch group {string} contains graph item {string}")]
async fn then_branch_group_contains_graph_item(
    world: &mut ScenarioWorld,
    branch: String,
    item: String,
) {
    then_branch_group_containment(world, branch, item, true).await;
}

#[then(expr = "branch group {string} does not contain graph item {string}")]
async fn then_branch_group_does_not_contain_graph_item(
    world: &mut ScenarioWorld,
    branch: String,
    item: String,
) {
    then_branch_group_containment(world, branch, item, false).await;
}

async fn then_branch_group_containment(
    world: &mut ScenarioWorld,
    branch: String,
    item: String,
    expected: bool,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let branch = expand_placeholders(world, &branch);
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            {GRAPH_GEOMETRY_HELPERS}
            const body = Array
                .from(document.querySelectorAll("path.graph-branch-body"))
                .find((element) => element.dataset.branch === {branch:?});
            const entry = itemBoxes().find((candidate) => candidate.label === {item:?});
            if (!body || !entry) {{
                return `missing group=${{Boolean(body)}} item=${{Boolean(entry)}}`;
            }}
            const region = body.getBoundingClientRect();
            const contained = overlaps(region, entry.box);
            return contained === {expected} ? "OK" : `containment is ${{contained}}`;
        }}
        "#
    );
    assert_graph_probe(page, &script, "a branch group holds exactly its members").await;
}

#[then("the whole graph is visible in the graph viewport")]
async fn then_the_whole_graph_is_visible(world: &mut ScenarioWorld) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = format!(
        r#"
        () => {{
            {GRAPH_GEOMETRY_HELPERS}
            const stage = document.querySelector(".graph-stage");
            const boxes = itemBoxes();
            if (!stage || boxes.length === 0) {{
                return "graph is not rendered";
            }}
            const view = stage.getBoundingClientRect();
            for (const entry of boxes) {{
                if (entry.box.left < view.left || entry.box.right > view.right
                    || entry.box.top < view.top || entry.box.bottom > view.bottom) {{
                    return `${{entry.label}} is outside the viewport`;
                }}
            }}
            return "OK";
        }}
        "#
    );
    assert_graph_probe(page, &script, "the initial view frames the whole graph").await;
}

#[then(expr = "graph zoom stays within {int} and {int} percent")]
async fn then_graph_zoom_stays_within(world: &mut ScenarioWorld, minimum: i32, maximum: i32) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = format!(
        r#"
        async () => {{
            const label = () => Array
                .from(document.querySelectorAll(".zoom-group button"))
                .find((button) => button.getAttribute("title") === "Reset zoom");
            const press = (title) => {{
                const button = Array
                    .from(document.querySelectorAll(".zoom-group button"))
                    .find((candidate) => candidate.getAttribute("title") === title);
                if (button) {{
                    button.click();
                }}
            }};
            const reading = () => Number.parseInt(label().textContent.replace("%", ""), 10);
            for (let step = 0; step < 40; step += 1) {{
                press("Zoom out");
            }}
            const lowest = reading();
            for (let step = 0; step < 80; step += 1) {{
                press("Zoom in");
            }}
            const highest = reading();
            if (lowest < {minimum} || highest > {maximum}) {{
                return `zoom ranged ${{lowest}}%..${{highest}}%`;
            }}
            return "OK";
        }}
        "#
    );
    assert_graph_probe(page, &script, "zoom shares one range everywhere").await;
}

#[then("graph geometry does not change while snapshots arrive")]
async fn then_graph_geometry_does_not_change(world: &mut ScenarioWorld) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    // Canvas coordinates, not screen ones: this asserts that the arrangement holds still, which
    // is independent of where the viewport happens to be panned or zoomed.
    let sample = r#"
        () => JSON.stringify(Array
            .from(document.querySelectorAll(".graph-hit-layer button"))
            .map((element) => [
                element.dataset.label,
                element.style.left,
                element.style.top,
            ]))
    "#
    .to_string();
    let first = page
        .evaluate::<(), String>(&sample, None::<&()>)
        .await
        .expect("graph geometry must be readable");
    // Several leader snapshots land in this window, so an unchanged topology has been redrawn
    // repeatedly by the time the second reading is taken.
    tokio::time::sleep(Duration::from_millis(1600)).await;
    let second = page
        .evaluate::<(), String>(&sample, None::<&()>)
        .await
        .expect("graph geometry must be readable");
    assert_eq!(
        first, second,
        "statistics updates must not move the drawing"
    );
}

/// Poll a probe that returns "OK" or a description of what is wrong.
async fn assert_graph_probe(page: &playwright_rs::Page, script: &str, expectation: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let last = page
            .evaluate::<(), String>(script, None::<&()>)
            .await
            .expect("graph probe must be readable");
        if last == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expectation}, but {last}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph search result {string} is visible in the graph viewport")]
async fn then_graph_search_result_is_visible_in_the_graph_viewport(
    world: &mut ScenarioWorld,
    item: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const stage = document.querySelector(".graph-stage");
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            if (!stage || !item) {{
                return `missing stage=${{Boolean(stage)}} item=${{Boolean(item)}}`;
            }}
            const stageBox = stage.getBoundingClientRect();
            const itemBox = item.getBoundingClientRect();
            if (item.dataset.searchHighlight !== "true") {{
                return "item is not highlighted";
            }}
            if (boxContained(itemBox, stageBox)) {{
                return "OK";
            }}
            return `outside viewport item=${{JSON.stringify(boxSummary(itemBox))}} stage=${{JSON.stringify(boxSummary(stageBox))}}`;

            function boxContained(box, stageBox) {{
                const margin = 8;
                return box.left >= stageBox.left + margin
                    && box.right <= stageBox.right - margin
                    && box.top >= stageBox.top + margin
                    && box.bottom <= stageBox.bottom - margin;
            }}

            function boxSummary(box) {{
                return {{
                    left: Math.round(box.left),
                    right: Math.round(box.right),
                    top: Math.round(box.top),
                    bottom: Math.round(box.bottom),
                }};
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph search result visibility must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph search result '{item}' to be visible in the graph viewport: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph relay item {string} has buffer statistics")]
async fn then_graph_relay_item_has_buffer_statistics(
    world: &mut ScenarioWorld,
    relay: String,
    #[step] step: &Step,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let relay = expand_placeholders(world, &relay);
    let assertions = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(parse_numeric_metric_assertion)
        .collect::<Vec<_>>();
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".relay-hit"))
                .find((element) => element.dataset.label === {relay:?});
            if (!item) {{
                return null;
            }}
            return {{
                capacity: item.dataset.bufferCapacity || "",
                p50: item.dataset.bufferP50 || "",
                p90: item.dataset.bufferP90 || "",
                p99: item.dataset.bufferP99 || ""
            }};
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let statistics = page
            .evaluate::<(), Option<BTreeMap<String, String>>>(&script, None::<&()>)
            .await
            .expect("graph relay buffer statistics must be readable");
        if let Some(statistics) = &statistics {
            let matches = assertions.iter().all(|assertion| {
                statistics
                    .get(&assertion.field)
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some_and(|actual| assertion.op.matches(actual, assertion.expected))
            });
            if matches {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "expected relay '{relay}' buffer statistics to satisfy {:?}, got {:?}",
            assertions,
            statistics
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} is visible")]
async fn then_graph_edge_from_to_is_visible(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    then_graph_edge_with_kind_from_to_is_visible(world, "DATA".to_string(), source, target).await;
}

#[when(
    expr = "graph edge from {string} to {string} is clicked with viewport focused on its middle"
)]
async fn when_graph_edge_from_to_is_clicked_with_viewport_focused_on_its_middle(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph interactions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            return (async () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = findEdge(".graph-edge", source, target);
            const hit = findEdge(".graph-edge-hit", source, target);
            const stage = document.querySelector(".graph-stage");
            if (!edge || !hit || !stage) {{
                return failure(`missing edge=${{Boolean(edge)}} hit=${{Boolean(hit)}} stage=${{Boolean(stage)}}`);
            }}
            const reset = Array
                .from(document.querySelectorAll(".zoom-group button"))
                .find((button) => button.getAttribute("title") === "Reset zoom");
            const zoomIn = Array
                .from(document.querySelectorAll(".zoom-group button"))
                .find((button) => button.getAttribute("title") === "Zoom in");
            if (!reset || !zoomIn) {{
                return failure(`missing zoom controls reset=${{Boolean(reset)}} zoomIn=${{Boolean(zoomIn)}}`);
            }}
            reset.click();
            // Zoom to the top of the range: the arrangement is compact, so an edge's middle only
            // clears both of its endpoints well past the default framing.
            for (let index = 0; index < 20; index += 1) {{
                zoomIn.click();
            }}
            await waitForStableTransform();
            const rect = stage.getBoundingClientRect();
            const middle = edgeScreenPoint(edge, 0.5);
            if (!middle) {{
                return failure("edge middle is unreadable");
            }}
            const centerX = Math.round(rect.left + rect.width / 2);
            const centerY = Math.round(rect.top + rect.height / 2);
            const deltaX = Math.round(centerX - middle.x);
            const deltaY = Math.round(centerY - middle.y);
            stage.dispatchEvent(new MouseEvent("mousedown", {{
                bubbles: true,
                cancelable: true,
                button: 0,
                clientX: centerX,
                clientY: centerY
            }}));
            stage.dispatchEvent(new MouseEvent("mousemove", {{
                bubbles: true,
                cancelable: true,
                button: 0,
                clientX: centerX + deltaX,
                clientY: centerY + deltaY
            }}));
            stage.dispatchEvent(new MouseEvent("mouseup", {{
                bubbles: true,
                cancelable: true,
                button: 0,
                clientX: centerX + deltaX,
                clientY: centerY + deltaY
            }}));
            await waitForStableTransform();
            if (endpointsVisibleInStage(source, target)) {{
                return failure("setup did not isolate the edge middle from both endpoints");
            }}
            const clickPoint = edgeScreenPoint(findEdge(".graph-edge", source, target), 0.5);
            if (!clickPoint) {{
                return failure("edge click point is unreadable");
            }}
            const element = document.elementFromPoint(clickPoint.x, clickPoint.y);
            const clickHit = findEdge(".graph-edge-hit", source, target);
            if (
                element !== clickHit
                && element?.closest?.(".graph-edge-group") !== clickHit?.closest?.(".graph-edge-group")
            ) {{
                return failure(`edge click point is not owned by target edge: tag=${{element?.tagName ?? ""}} class=${{element?.getAttribute?.("class") ?? ""}}`);
            }}
            return {{
                status: "OK",
                x: String(Math.round(clickPoint.x)),
                y: String(Math.round(clickPoint.y)),
            }};

            function failure(message) {{
                return {{
                    status: message,
                    x: "0",
                    y: "0",
                }};
            }}

            function nextFrame() {{
                return new Promise((resolve) => requestAnimationFrame(() => resolve()));
            }}

            async function waitForStableTransform() {{
                const layer = document.querySelector(".graph-zoom-layer");
                if (!layer) {{
                    await nextFrame();
                    return;
                }}
                let stableFrames = 0;
                let previous = "";
                for (let index = 0; index < 30; index += 1) {{
                    await nextFrame();
                    const current = getComputedStyle(layer).transform;
                    if (current === previous) {{
                        stableFrames += 1;
                        if (stableFrames >= 3) {{
                            return;
                        }}
                    }} else {{
                        stableFrames = 0;
                        previous = current;
                    }}
                }}
            }}

            function findEdge(selector, source, target) {{
                return Array
                    .from(document.querySelectorAll(selector))
                    .find((path) =>
                        path.dataset.kind === "DATA"
                        && path.dataset.source.endsWith(`:${{source}}`)
                        && path.dataset.target.endsWith(`:${{target}}`)
                    );
            }}

            function edgeScreenPoint(path, ratio) {{
                if (!path) {{
                    return null;
                }}
                const length = path.getTotalLength();
                const matrix = path.getScreenCTM();
                if (length <= 0 || !matrix) {{
                    return null;
                }}
                const local = path.getPointAtLength(length * ratio);
                return new DOMPoint(local.x, local.y).matrixTransform(matrix);
            }}

            function endpointsVisibleInStage(source, target) {{
                const sourceItem = graphItem(source);
                const targetItem = graphItem(target);
                if (!sourceItem || !targetItem) {{
                    return false;
                }}
                const stageBox = stage.getBoundingClientRect();
                return boxContained(sourceItem.getBoundingClientRect(), stageBox)
                    && boxContained(targetItem.getBoundingClientRect(), stageBox);
            }}

            function graphItem(label) {{
                return Array
                    .from(document.querySelectorAll(".graph-hit-layer button"))
                    .find((element) => element.dataset.label === label);
            }}

            function boxContained(box, stageBox) {{
                const margin = 8;
                return box.left >= stageBox.left + margin
                    && box.right <= stageBox.right - margin
                    && box.top >= stageBox.top + margin
                    && box.bottom <= stageBox.bottom - margin;
            }}
            }})();
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), BTreeMap<String, String>>(&script, None::<&()>)
            .await
            .expect("graph edge click setup must be executable");
        let status = result
            .get("status")
            .expect("graph edge click setup must return status");
        if status == "OK" {
            let x = result
                .get("x")
                .and_then(|value| value.parse::<f64>().ok())
                .expect("graph edge click setup must return x coordinate");
            let y = result
                .get("y")
                .and_then(|value| value.parse::<f64>().ok())
                .expect("graph edge click setup must return y coordinate");
            page.mouse()
                .click(x, y, None)
                .await
                .expect("graph edge click must be executable");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to be clickable with a focused \
             middle viewport: {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(
    expr = "graph edge from {string} to {string} has both endpoints visible in the graph viewport"
)]
async fn then_graph_edge_from_to_has_both_endpoints_visible_in_the_graph_viewport(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const stage = document.querySelector(".graph-stage");
            const sourceItem = graphItem(source);
            const targetItem = graphItem(target);
            if (!stage || !sourceItem || !targetItem) {{
                return `missing stage=${{Boolean(stage)}} source=${{Boolean(sourceItem)}} target=${{Boolean(targetItem)}}`;
            }}
            const stageBox = stage.getBoundingClientRect();
            const sourceBox = sourceItem.getBoundingClientRect();
            const targetBox = targetItem.getBoundingClientRect();
            if (boxContained(sourceBox, stageBox) && boxContained(targetBox, stageBox)) {{
                return "OK";
            }}
            return `outside viewport source=${{JSON.stringify(boxSummary(sourceBox))}} target=${{JSON.stringify(boxSummary(targetBox))}} stage=${{JSON.stringify(boxSummary(stageBox))}}`;

            function graphItem(label) {{
                return Array
                    .from(document.querySelectorAll(".graph-hit-layer button"))
                    .find((element) => element.dataset.label === label);
            }}

            function boxContained(box, stageBox) {{
                const margin = 8;
                return box.left >= stageBox.left + margin
                    && box.right <= stageBox.right - margin
                    && box.top >= stageBox.top + margin
                    && box.bottom <= stageBox.bottom - margin;
            }}

            function boxSummary(box) {{
                return {{
                    left: Math.round(box.left),
                    right: Math.round(box.right),
                    top: Math.round(box.top),
                    bottom: Math.round(box.bottom),
                }};
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge endpoint visibility must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to focus both endpoints in the \
             graph viewport: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} does not intersect graph item {string}")]
async fn then_graph_edge_from_to_does_not_intersect_graph_item(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    item: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            if (!edge || !item) {{
                return false;
            }}
            const itemBox = item.getBoundingClientRect();
            return !pathIntersectsScreenBox(edge, itemBox, 2);

            function pathIntersectsScreenBox(path, box, inset) {{
                const matrix = path.getScreenCTM();
                const svg = path.ownerSVGElement;
                if (!matrix || !svg) {{
                    return true;
                }}
                const point = svg.createSVGPoint();
                const length = path.getTotalLength();
                const samples = Math.max(2, Math.ceil(length / 4));
                for (let index = 0; index <= samples; index += 1) {{
                    const local = path.getPointAtLength(length * index / samples);
                    point.x = local.x;
                    point.y = local.y;
                    const screen = point.matrixTransform(matrix);
                    if (
                        screen.x > box.left + inset
                        && screen.x < box.right - inset
                        && screen.y > box.top + inset
                        && screen.y < box.bottom - inset
                    ) {{
                        return true;
                    }}
                }}
                return false;
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let does_not_intersect = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("graph edge and item positions must be readable");
        if does_not_intersect {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' not to intersect graph item \
             '{item}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} does not intersect branch group {string} body")]
async fn then_graph_edge_from_to_does_not_intersect_branch_group_body(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    branch: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let branch = expand_placeholders(world, &branch);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            const bodies = Array
                .from(document.querySelectorAll(".graph-branch-body"))
                .filter((path) => path.dataset.branch === {branch:?});
            if (!edge || bodies.length === 0) {{
                return `missing edge=${{Boolean(edge)}} bodies=${{bodies.length}}`;
            }}
            const path = edge.getAttribute("d") ?? "";
            for (const body of bodies) {{
                const box = bodyScreenBox(body);
                if (pathIntersectsScreenBox(edge, box, 2)) {{
                    return `intersects path=${{path}} body=${{JSON.stringify({{
                        branch: body.dataset.branch,
                        box,
                    }})}}`;
                }}
            }}
            return "OK";

            function bodyScreenBox(body) {{
                return body.getBoundingClientRect();
            }}

            function pathIntersectsScreenBox(path, box, inset) {{
                if (!box) {{
                    return true;
                }}
                const matrix = path.getScreenCTM();
                const svg = path.ownerSVGElement;
                if (!matrix || !svg) {{
                    return true;
                }}
                const point = svg.createSVGPoint();
                const length = path.getTotalLength();
                const samples = Math.max(2, Math.ceil(length / 4));
                for (let index = 0; index <= samples; index += 1) {{
                    const local = path.getPointAtLength(length * index / samples);
                    point.x = local.x;
                    point.y = local.y;
                    const screen = point.matrixTransform(matrix);
                    if (
                        screen.x > box.left + inset
                        && screen.x < box.right - inset
                        && screen.y > box.top + inset
                        && screen.y < box.bottom - inset
                    ) {{
                        return true;
                    }}
                }}
                return false;
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge and branch group positions must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' not to intersect branch group \
             '{branch}' body: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(
    expr = "graph edge from {string} to {string} does not intersect graph edge from {string} to \
            {string}"
)]
async fn then_graph_edge_from_to_does_not_intersect_graph_edge_from_to(
    world: &mut ScenarioWorld,
    first_source: String,
    first_target: String,
    second_source: String,
    second_target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let first_source = expand_placeholders(world, &first_source);
    let first_target = expand_placeholders(world, &first_target);
    let second_source = expand_placeholders(world, &second_source);
    let second_target = expand_placeholders(world, &second_target);
    let script = format!(
        r#"
        () => {{
            const firstSource = {first_source:?};
            const firstTarget = {first_target:?};
            const secondSource = {second_source:?};
            const secondTarget = {second_target:?};
            const first = findEdge(firstSource, firstTarget);
            const second = findEdge(secondSource, secondTarget);
            if (!first || !second) {{
                return `missing first=${{Boolean(first)}} second=${{Boolean(second)}}`;
            }}
            const firstPath = first.getAttribute("d") ?? "";
            const secondPath = second.getAttribute("d") ?? "";
            const firstPoints = pathScreenPoints(first);
            const secondPoints = pathScreenPoints(second);
            if (!firstPoints || !secondPoints) {{
                return `unreadable first=${{firstPath}} second=${{secondPath}}`;
            }}
            for (let firstIndex = 0; firstIndex < firstPoints.length - 1; firstIndex += 1) {{
                const a = firstPoints[firstIndex];
                const b = firstPoints[firstIndex + 1];
                for (let secondIndex = 0; secondIndex < secondPoints.length - 1; secondIndex += 1) {{
                    const c = secondPoints[secondIndex];
                    const d = secondPoints[secondIndex + 1];
                    if (shareEndpoint(a, b, c, d)) {{
                        continue;
                    }}
                    if (segmentsIntersect(a, b, c, d)) {{
                        return `intersects first=${{firstPath}} second=${{secondPath}} firstSegment=${{firstIndex}} secondSegment=${{secondIndex}}`;
                    }}
                }}
            }}
            return "OK";

            function findEdge(source, target) {{
                return Array
                    .from(document.querySelectorAll(".graph-edge"))
                    .find((path) =>
                        path.dataset.kind === "DATA"
                        && path.dataset.source.endsWith(`:${{source}}`)
                        && path.dataset.target.endsWith(`:${{target}}`)
                    );
            }}

            function pathScreenPoints(path) {{
                const matrix = path.getScreenCTM();
                const svg = path.ownerSVGElement;
                if (!matrix || !svg) {{
                    return null;
                }}
                const point = svg.createSVGPoint();
                const length = path.getTotalLength();
                const samples = Math.max(2, Math.ceil(length / 6));
                const points = [];
                for (let index = 0; index <= samples; index += 1) {{
                    const local = path.getPointAtLength(length * index / samples);
                    point.x = local.x;
                    point.y = local.y;
                    const screen = point.matrixTransform(matrix);
                    points.push({{ x: screen.x, y: screen.y }});
                }}
                return points;
            }}

            function shareEndpoint(a, b, c, d) {{
                return pointDistance(a, c) < 6
                    || pointDistance(a, d) < 6
                    || pointDistance(b, c) < 6
                    || pointDistance(b, d) < 6;
            }}

            function pointDistance(a, b) {{
                return Math.hypot(a.x - b.x, a.y - b.y);
            }}

            function segmentsIntersect(a, b, c, d) {{
                const epsilon = 0.1;
                if (
                    Math.max(a.x, b.x) + epsilon < Math.min(c.x, d.x)
                    || Math.max(c.x, d.x) + epsilon < Math.min(a.x, b.x)
                    || Math.max(a.y, b.y) + epsilon < Math.min(c.y, d.y)
                    || Math.max(c.y, d.y) + epsilon < Math.min(a.y, b.y)
                ) {{
                    return false;
                }}
                const abC = cross(a, b, c);
                const abD = cross(a, b, d);
                const cdA = cross(c, d, a);
                const cdB = cross(c, d, b);
                if (Math.abs(abC) <= epsilon && onSegment(a, b, c, epsilon)) {{
                    return true;
                }}
                if (Math.abs(abD) <= epsilon && onSegment(a, b, d, epsilon)) {{
                    return true;
                }}
                if (Math.abs(cdA) <= epsilon && onSegment(c, d, a, epsilon)) {{
                    return true;
                }}
                if (Math.abs(cdB) <= epsilon && onSegment(c, d, b, epsilon)) {{
                    return true;
                }}
                return (
                    (abC > epsilon && abD < -epsilon || abC < -epsilon && abD > epsilon)
                    && (cdA > epsilon && cdB < -epsilon || cdA < -epsilon && cdB > epsilon)
                );
            }}

            function cross(a, b, c) {{
                return (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x);
            }}

            function onSegment(a, b, c, epsilon) {{
                return c.x >= Math.min(a.x, b.x) - epsilon
                    && c.x <= Math.max(a.x, b.x) + epsilon
                    && c.y >= Math.min(a.y, b.y) - epsilon
                    && c.y <= Math.max(a.y, b.y) + epsilon;
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge paths must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{first_source}' to '{first_target}' not to intersect graph \
             edge from '{second_source}' to '{second_target}': {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(
    expr = "graph edge from {string} to {string} does not share horizontal lane with graph edge \
            from {string} to {string}"
)]
async fn then_graph_edge_from_to_does_not_share_horizontal_lane_with_graph_edge_from_to(
    world: &mut ScenarioWorld,
    first_source: String,
    first_target: String,
    second_source: String,
    second_target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let first_source = expand_placeholders(world, &first_source);
    let first_target = expand_placeholders(world, &first_target);
    let second_source = expand_placeholders(world, &second_source);
    let second_target = expand_placeholders(world, &second_target);
    let script = format!(
        r#"
        () => {{
            const first = findEdge({first_source:?}, {first_target:?});
            const second = findEdge({second_source:?}, {second_target:?});
            if (!first || !second) {{
                return `missing first=${{Boolean(first)}} second=${{Boolean(second)}}`;
            }}
            const firstLane = dominantHorizontalLane(first);
            const secondLane = dominantHorizontalLane(second);
            if (!firstLane || !secondLane) {{
                return `missing lane first=${{JSON.stringify(firstLane)}} second=${{JSON.stringify(secondLane)}}`;
            }}
            if (Math.abs(firstLane.y - secondLane.y) >= 8) {{
                return "OK";
            }}
            return `shared lane first=${{JSON.stringify(firstLane)}} second=${{JSON.stringify(secondLane)}} firstPath=${{first.getAttribute("d")}} secondPath=${{second.getAttribute("d")}}`;

            function findEdge(source, target) {{
                return Array
                    .from(document.querySelectorAll(".graph-edge"))
                    .find((path) =>
                        path.dataset.kind === "DATA"
                        && path.dataset.source.endsWith(`:${{source}}`)
                        && path.dataset.target.endsWith(`:${{target}}`)
                    );
            }}

            function dominantHorizontalLane(path) {{
                const length = path.getTotalLength();
                if (length <= 0) {{
                    return null;
                }}
                const samples = Math.max(4, Math.ceil(length / 4));
                let best = null;
                let active = null;
                let previous = path.getPointAtLength(0);
                for (let index = 1; index <= samples; index += 1) {{
                    const current = path.getPointAtLength(length * index / samples);
                    const dx = current.x - previous.x;
                    const dy = current.y - previous.y;
                    const segment = Math.hypot(dx, dy);
                    if (segment > 0 && Math.abs(dy) <= 1 && Math.abs(dx) >= Math.abs(dy) * 4) {{
                        const y = (previous.y + current.y) / 2;
                        if (active && Math.abs(active.y - y) <= 2) {{
                            active.length += segment;
                            active.y = (active.y + y) / 2;
                        }} else {{
                            if (!best || active && active.length > best.length) {{
                                best = active;
                            }}
                            active = {{ y, length: segment }};
                        }}
                    }} else {{
                        if (!best || active && active.length > best.length) {{
                            best = active;
                        }}
                        active = null;
                    }}
                    previous = current;
                }}
                if (!best || active && active.length > best.length) {{
                    best = active;
                }}
                return best;
            }}
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge horizontal lanes must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{first_source}' to '{first_target}' not to share a \
             horizontal lane with graph edge from '{second_source}' to '{second_target}': {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} starts horizontally")]
async fn then_graph_edge_from_to_starts_horizontally(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const length = edge.getTotalLength();
            if (length <= 0) {{
                return `empty path=${{path}}`;
            }}
            const start = edge.getPointAtLength(0);
            const end = edge.getPointAtLength(length);
            const sample = edge.getPointAtLength(Math.min(16, length));
            const expectedDirection = Math.sign(end.x - start.x);
            const dx = sample.x - start.x;
            const dy = sample.y - start.y;
            if (
                Math.abs(dx) > 1
                && Math.abs(dx) >= Math.abs(dy) * 2
                && (expectedDirection === 0 || Math.sign(dx) === expectedDirection)
            ) {{
                return "OK";
            }}
            return `non-horizontal start path=${{path}} dx=${{dx}} dy=${{dy}} expected=${{expectedDirection}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge path must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to start horizontally: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} ends horizontally")]
async fn then_graph_edge_from_to_ends_horizontally(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const length = edge.getTotalLength();
            if (length <= 0) {{
                return `empty path=${{path}}`;
            }}
            const sample = edge.getPointAtLength(Math.max(0, length - 16));
            const start = edge.getPointAtLength(0);
            const end = edge.getPointAtLength(length);
            const expectedDirection = Math.sign(end.x - start.x);
            const dx = end.x - sample.x;
            const dy = end.y - sample.y;
            if (
                Math.abs(dx) > 1
                && Math.abs(dx) >= Math.abs(dy) * 2
                && (expectedDirection === 0 || Math.sign(dx) === expectedDirection)
            ) {{
                return "OK";
            }}
            return `non-horizontal end path=${{path}} dx=${{dx}} dy=${{dy}} expected=${{expectedDirection}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge path must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to end horizontally: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} has target plug at least {int} pixels")]
async fn then_graph_edge_from_to_has_target_plug_at_least(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    expected_pixels: i32,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const expectedPixels = {expected_pixels};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const length = edge.getTotalLength();
            if (length <= 0) {{
                return `empty path=${{path}}`;
            }}
            const start = edge.getPointAtLength(0);
            const end = edge.getPointAtLength(length);
            const expectedDirection = Math.sign(end.x - start.x);
            let plug = 0;
            const limit = Math.min(length, expectedPixels + 24);
            for (let offset = 1; offset <= limit; offset += 1) {{
                const current = edge.getPointAtLength(length - offset);
                const dx = end.x - current.x;
                const dy = end.y - current.y;
                if (Math.abs(dy) > 1.5) {{
                    break;
                }}
                if (expectedDirection !== 0 && Math.sign(dx) !== expectedDirection) {{
                    break;
                }}
                plug = offset;
            }}
            if (plug >= expectedPixels) {{
                return "OK";
            }}
            return `short target plug length=${{plug}} expected=${{expectedPixels}} path=${{path}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge target plug must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to have a target plug at least \
             {expected_pixels}px: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} has at most {int} rounded turns")]
async fn then_graph_edge_from_to_has_at_most_rounded_turns(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    expected_turns: usize,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const turns = (path.match(/ Q/g) ?? []).length;
            if (turns <= {expected_turns}) {{
                return "OK";
            }}
            return `too many rounded turns=${{turns}} path=${{path}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge path must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to have at most {expected_turns} \
             rounded turns: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} has source plug at least {int} pixels")]
async fn then_graph_edge_from_to_has_source_plug_at_least(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    expected_pixels: i32,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const expectedPixels = {expected_pixels};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge) {{
                return "missing edge";
            }}
            const path = edge.getAttribute("d") ?? "";
            const length = edge.getTotalLength();
            if (length <= 0) {{
                return `empty path=${{path}}`;
            }}
            const start = edge.getPointAtLength(0);
            const end = edge.getPointAtLength(length);
            const expectedDirection = Math.sign(end.x - start.x);
            let plug = 0;
            const limit = Math.min(length, expectedPixels + 24);
            for (let offset = 1; offset <= limit; offset += 1) {{
                const current = edge.getPointAtLength(offset);
                const dx = current.x - start.x;
                const dy = current.y - start.y;
                if (Math.abs(dy) > 1.5) {{
                    break;
                }}
                if (expectedDirection !== 0 && Math.sign(dx) !== expectedDirection) {{
                    break;
                }}
                plug = offset;
            }}
            if (plug >= expectedPixels) {{
                return "OK";
            }}
            return `short source plug length=${{plug}} expected=${{expectedPixels}} path=${{path}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge source plug must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to have a source plug at least \
             {expected_pixels}px: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph action edge {string} from {string} to {string} is visible")]
async fn then_graph_action_edge_from_to_is_visible(
    world: &mut ScenarioWorld,
    kind: String,
    source: String,
    target: String,
) {
    let kind = kind.replace(' ', "_").to_ascii_uppercase();
    then_graph_edge_with_kind_from_to_is_visible(world, kind, source, target).await;
}

#[then(expr = "graph edge from {string} to {string} has traffic statistics")]
async fn then_graph_edge_from_to_has_traffic_statistics(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    #[step] step: &Step,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let assertions = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(parse_numeric_metric_assertion)
        .collect::<Vec<_>>();
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const item = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((element) =>
                    element.dataset.kind === "DATA"
                    &&
                    element.dataset.source.endsWith(`:${{source}}`)
                    && element.dataset.target.endsWith(`:${{target}}`)
                );
            if (!item) {{
                return null;
            }}
            return {{
                messages_total: item.dataset.messagesTotal || "",
                bytes_total: item.dataset.bytesTotal || "",
                batches_total: item.dataset.batchesTotal || "",
                messages_per_second: item.dataset.messagesPerSecond || "",
                bytes_per_second: item.dataset.bytesPerSecond || "",
                batches_per_second: item.dataset.batchesPerSecond || ""
            }};
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let statistics = page
            .evaluate::<(), Option<BTreeMap<String, String>>>(&script, None::<&()>)
            .await
            .expect("graph edge traffic statistics must be readable");
        if let Some(statistics) = &statistics {
            let matches = assertions.iter().all(|assertion| {
                statistics
                    .get(&assertion.field)
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some_and(|actual| assertion.op.matches(actual, assertion.expected))
            });
            if matches {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to satisfy traffic assertions \
             {assertions:?}, got {statistics:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[when("graph topology render count observation starts")]
async fn when_graph_topology_render_count_observation_starts(world: &mut ScenarioWorld) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = r##"
        () => {
            const layer = document.querySelector(".graph-zoom-layer");
            const renderCount = Number(layer?.dataset?.renderCount ?? NaN);
            if (!layer || !Number.isFinite(renderCount) || renderCount <= 0) {
                return false;
            }
            window.__nervixGraphTopologyObservedRenderCount = renderCount;
            return true;
        }
    "##;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let observing = page
            .evaluate::<(), bool>(script, None::<&()>)
            .await
            .expect("graph topology mutation observer must be installable");
        if observing {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected execution graph chart render count to be available for topology observation"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("graph topology render count does not change during observed traffic")]
async fn then_graph_topology_render_count_does_not_change_during_observed_traffic(
    world: &mut ScenarioWorld,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let script = r##"
        () => {
            const layer = document.querySelector(".graph-zoom-layer");
            const observed = Number(window.__nervixGraphTopologyObservedRenderCount ?? NaN);
            const current = Number(layer?.dataset?.renderCount ?? NaN);
            if (!Number.isFinite(observed) || !Number.isFinite(current)) {
                return `missing render count observed=${observed} current=${current}`;
            }
            if (current === observed) {
                return "OK";
            }
            return `render count changed from ${observed} to ${current}`;
        }
    "##;
    let deadline = Instant::now() + Duration::from_millis(600);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(script, None::<&()>)
            .await
            .expect("graph topology render count must be readable");
        assert!(
            result == "OK",
            "expected no execution graph chart renders during traffic-only updates: {result}"
        );
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn then_graph_edge_with_kind_from_to_is_visible(
    world: &mut ScenarioWorld,
    kind: String,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const kind = {kind:?};
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === kind
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            const svg = document.querySelector(".graph-pulse-layer");
            if (!edge || !svg) {{
                return false;
            }}
            const box = edge.getBBox();
            const viewBox = svg.viewBox.baseVal;
            return box.width > 0
                && box.height >= 0
                && box.x >= viewBox.x
                && box.y >= viewBox.y
                && box.x + box.width <= viewBox.x + viewBox.width
                && box.y + box.height <= viewBox.y + viewBox.height;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let is_visible = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("graph edge position must be readable");
        if is_visible {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge kind '{kind}' from '{source}' to '{target}' to be visible"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "graph edge from {string} to {string} has exact hover target")]
async fn then_graph_edge_from_to_has_exact_hover_target(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let source = expand_placeholders(world, &source);
    let target = expand_placeholders(world, &target);
    let script = format!(
        r#"
        () => {{
            const source = {source:?};
            const target = {target:?};
            const edge = Array
                .from(document.querySelectorAll(".graph-edge"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            const hit = Array
                .from(document.querySelectorAll(".graph-edge-hit"))
                .find((path) =>
                    path.dataset.kind === "DATA"
                    && path.dataset.source.endsWith(`:${{source}}`)
                    && path.dataset.target.endsWith(`:${{target}}`)
                );
            if (!edge || !hit) {{
                return `missing edge=${{Boolean(edge)}} hit=${{Boolean(hit)}}`;
            }}
            const length = edge.getTotalLength();
            const matrix = edge.getScreenCTM();
            if (length <= 0 || !matrix) {{
                return `invalid length=${{length}} matrix=${{Boolean(matrix)}}`;
            }}
            const samples = [0.35, 0.5, 0.65].map((ratio) => {{
                const point = edge.getPointAtLength(length * ratio);
                const screen = new DOMPoint(point.x, point.y).matrixTransform(matrix);
                return {{ ratio, screen }};
            }});
            const viewportWidth = window.innerWidth;
            const viewportHeight = window.innerHeight;
            const onScreen = samples.some((sample) =>
                sample.screen.x >= 0
                && sample.screen.y >= 0
                && sample.screen.x < viewportWidth
                && sample.screen.y < viewportHeight
            );
            if (!onScreen) {{
                const stage = document.querySelector(".graph-stage");
                const rect = stage?.getBoundingClientRect();
                if (!stage || !rect) {{
                    return "missing graph stage";
                }}
                const sample = samples[Math.floor(samples.length / 2)].screen;
                const startX = Math.round(rect.left + rect.width / 2);
                const startY = Math.round(rect.top + rect.height / 2);
                const deltaX = Math.round(startX - sample.x);
                const deltaY = Math.round(startY - sample.y);
                stage.dispatchEvent(new MouseEvent("mousedown", {{
                    bubbles: true,
                    cancelable: true,
                    button: 0,
                    clientX: startX,
                    clientY: startY
                }}));
                stage.dispatchEvent(new MouseEvent("mousemove", {{
                    bubbles: true,
                    cancelable: true,
                    button: 0,
                    clientX: startX + deltaX,
                    clientY: startY + deltaY
                }}));
                stage.dispatchEvent(new MouseEvent("mouseup", {{
                    bubbles: true,
                    cancelable: true,
                    button: 0,
                    clientX: startX + deltaX,
                    clientY: startY + deltaY
                }}));
                return `panned by ${{deltaX}},${{deltaY}}`;
            }}
            for (const {{screen}} of samples) {{
                const element = document.elementFromPoint(screen.x, screen.y);
                if (element === hit || element?.closest?.(".graph-edge-hit") === hit) {{
                    return "OK";
                }}
            }}
            const details = samples.map(({{ratio, screen}}) => {{
                const element = document.elementFromPoint(screen.x, screen.y);
                return {{
                    ratio,
                    x: Math.round(screen.x),
                    y: Math.round(screen.y),
                    tag: element?.tagName ?? "",
                    className: element?.getAttribute?.("class") ?? "",
                    source: element?.dataset?.source ?? "",
                    target: element?.dataset?.target ?? ""
                }};
            }});
            return `hover target mismatch samples=${{JSON.stringify(details)}} path=${{edge.getAttribute("d")}}`;
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let result = page
            .evaluate::<(), String>(&script, None::<&()>)
            .await
            .expect("graph edge hover target must be readable");
        if result == "OK" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected graph edge from '{source}' to '{target}' to own its hover target: {result}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "branch group {string} body does not overlap graph item {string}")]
async fn then_branch_group_body_does_not_overlap_graph_item(
    world: &mut ScenarioWorld,
    branch: String,
    item: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let branch = expand_placeholders(world, &branch);
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            const paths = Array
                .from(document.querySelectorAll(".graph-branch-body"))
                .filter((path) => path.dataset.branch === {branch:?});
            if (!item || paths.length === 0) {{
                return false;
            }}
            const itemBox = item.getBoundingClientRect();
            const bodyBox = (path) => path.getBoundingClientRect();
            return paths.some((path) => {{
                const body = bodyBox(path);
                return body
                    && (
                        body.right <= itemBox.left
                        || itemBox.right <= body.left
                        || body.bottom <= itemBox.top
                        || itemBox.bottom <= body.top
                    );
            }});
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let does_not_overlap = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("branch group body position must be readable");
        if does_not_overlap {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected branch group '{branch}' body not to overlap graph item '{item}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "branch group {string} body overlaps graph item {string}")]
async fn then_branch_group_body_overlaps_graph_item(
    world: &mut ScenarioWorld,
    branch: String,
    item: String,
) {
    let page = world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before graph assertions");
    let branch = expand_placeholders(world, &branch);
    let item = expand_placeholders(world, &item);
    let script = format!(
        r#"
        () => {{
            const item = Array
                .from(document.querySelectorAll(".graph-hit-layer button"))
                .find((element) => element.dataset.label === {item:?});
            const bodies = Array
                .from(document.querySelectorAll(".graph-branch-body"))
                .filter((path) => path.dataset.branch === {branch:?});
            if (!item || bodies.length === 0) {{
                return false;
            }}
            const itemBox = item.getBoundingClientRect();
            const bodyBox = (path) => path.getBoundingClientRect();
            return bodies.some((path) => {{
                const body = bodyBox(path);
                return body
                    && body.right > itemBox.left
                    && itemBox.right > body.left
                    && body.bottom > itemBox.top
                    && itemBox.bottom > body.top;
            }});
        }}
        "#
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let overlaps = page
            .evaluate::<(), bool>(&script, None::<&()>)
            .await
            .expect("branch group body position must be readable");
        if overlaps {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected branch group '{branch}' body to overlap graph item '{item}'"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(
    expr = "the last command output metric {string} {string} relay {string} physical node \
            {string} has values"
)]
async fn then_last_command_output_metric_has_values(
    world: &mut ScenarioWorld,
    metric: String,
    direction: String,
    relay: String,
    physical_node: String,
    #[step] step: &Step,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    let metric = expand_placeholders(world, &metric);
    let direction = expand_placeholders(world, &direction);
    let relay = expand_placeholders(world, &relay);
    let physical_node = expand_placeholders(world, &physical_node);
    let prefix = format!("{metric} {direction} relay={relay} physical_node={physical_node}");
    let line = output
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("expected metric line starting with '{prefix}', got: {output}"));
    for assertion in expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let Some((field, expected)) = assertion.split_once('=') else {
            panic!("unsupported metric value assertion '{assertion}'");
        };
        let Some(actual) = metric_line_value(line, field.trim()) else {
            panic!(
                "expected field '{}' in metric line '{}'",
                field.trim(),
                line
            );
        };
        assert_eq!(
            actual,
            expected.trim(),
            "expected field '{}' to equal '{}' in metric line '{}'",
            field.trim(),
            expected.trim(),
            line
        );
    }
}

#[then(
    expr = "the last command output metric {string} {string} relay {string} physical node \
            {string} has numeric values"
)]
async fn then_last_command_output_metric_has_numeric_values(
    world: &mut ScenarioWorld,
    metric: String,
    direction: String,
    relay: String,
    physical_node: String,
    #[step] step: &Step,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    let metric = expand_placeholders(world, &metric);
    let direction = expand_placeholders(world, &direction);
    let relay = expand_placeholders(world, &relay);
    let physical_node = expand_placeholders(world, &physical_node);
    let prefix = format!("{metric} {direction} relay={relay} physical_node={physical_node}");
    let line = output
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("expected metric line starting with '{prefix}', got: {output}"));
    let assertions = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(parse_numeric_metric_assertion)
        .collect::<Vec<_>>();

    assert!(
        assertions
            .iter()
            .all(|assertion| assertion.matches_metric_line(line)),
        "metric line starting with '{prefix}' did not satisfy numeric assertions {:?}: {line}",
        assertions
    );
}

#[then(
    expr = "the last command output metric {string} {string} relay {string} on any physical node \
            has values"
)]
async fn then_last_command_output_metric_on_any_physical_node_has_values(
    world: &mut ScenarioWorld,
    metric: String,
    direction: String,
    relay: String,
    #[step] step: &Step,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    let metric = expand_placeholders(world, &metric);
    let direction = expand_placeholders(world, &direction);
    let relay = expand_placeholders(world, &relay);
    let prefix = format!("{metric} {direction} relay={relay} physical_node=");
    let line = output
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("expected metric line starting with '{prefix}', got: {output}"));
    for assertion in expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let Some((field, expected)) = assertion.split_once('=') else {
            panic!("unsupported metric value assertion '{assertion}'");
        };
        let Some(actual) = metric_line_value(line, field.trim()) else {
            panic!(
                "expected field '{}' in metric line '{}'",
                field.trim(),
                line
            );
        };
        assert_eq!(
            actual,
            expected.trim(),
            "expected field '{}' to equal '{}' in metric line '{}'",
            field.trim(),
            expected.trim(),
            line
        );
    }
}

#[then(
    expr = "the last command output metric {string} {string} relay {string} on any physical node \
            has numeric values"
)]
async fn then_last_command_output_metric_on_any_physical_node_has_numeric_values(
    world: &mut ScenarioWorld,
    metric: String,
    direction: String,
    relay: String,
    #[step] step: &Step,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    let metric = expand_placeholders(world, &metric);
    let direction = expand_placeholders(world, &direction);
    let relay = expand_placeholders(world, &relay);
    let prefix = format!("{metric} {direction} relay={relay} physical_node=");
    let assertions = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(parse_numeric_metric_assertion)
        .collect::<Vec<_>>();
    let lines = output
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with(&prefix))
        .collect::<Vec<_>>();

    assert!(
        !lines.is_empty(),
        "expected metric line starting with '{prefix}', got: {output}"
    );

    for line in lines {
        if assertions
            .iter()
            .all(|assertion| assertion.matches_metric_line(line))
        {
            return;
        }
    }

    panic!(
        "no metric line starting with '{prefix}' satisfied numeric assertions {:?}. output: \
         {output}",
        assertions
    );
}

#[then(
    expr = "within {string} DESCRIBE DOMAIN section {string} metric {string} {string} relay \
            {string} across physical nodes totals {int}"
)]
async fn then_within_duration_describe_domain_section_metric_across_physical_nodes_totals(
    world: &mut ScenarioWorld,
    duration: String,
    section: String,
    metric: String,
    direction: String,
    relay: String,
    expected_total: u64,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let section = expand_placeholders(world, &section);
    let metric = expand_placeholders(world, &metric);
    let direction = expand_placeholders(world, &direction);
    let relay = expand_placeholders(world, &relay);
    let prefix = format!("{metric} {direction} relay={relay} physical_node=");
    let deadline = Instant::now() + duration;
    let mut last_totals = std::collections::BTreeMap::<String, u64>::new();

    loop {
        tokio::task::consume_budget().await;
        let mut outputs = Vec::new();
        let mut command_error = None;
        last_totals.clear();
        for node_id in world.cluster().node_ids() {
            tokio::task::consume_budget().await;
            match run_nspl_commands_on_node(world, &node_id, "DESCRIBE DOMAIN;").await {
                Ok(output) => {
                    for (physical_node, total) in
                        metric_totals_in_indented_section(&output, &section, &prefix)
                    {
                        // Two nodes reporting the same physical node's counter are reporting one
                        // counter, so the highest value each has seen stands for it rather than
                        // both being added together.
                        let seen = last_totals.entry(physical_node).or_default();
                        *seen = (*seen).max(total);
                    }
                    outputs.push(output);
                }
                Err(error) => {
                    command_error = Some(format!("{node_id}: {error}"));
                    break;
                }
            }
        }
        world.last_command_error = command_error;
        world.last_command_output = Some(outputs.join("\n"));
        if world.last_command_error.is_none()
            && !last_totals.is_empty()
            && last_totals.values().sum::<u64>() == expected_total
        {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for DESCRIBE DOMAIN section '{section}' metric lines starting with \
             '{prefix}' to total {expected_total}; last totals: {last_totals:?}, last output: \
             {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The counters in one section, keyed by the physical node each counter belongs to.
///
/// A node's `DESCRIBE DOMAIN` can report a counter owned by a different physical node, so the same
/// counter appears in more than one node's output. The line names its owner, so keying by that
/// name lets a caller polling every node count each counter once. Returning a bare list instead
/// invites summing one message's counter once per node that happens to have seen it.
fn metric_totals_in_indented_section(
    output: &str,
    section: &str,
    prefix: &str,
) -> std::collections::BTreeMap<String, u64> {
    let header = format!("{section}:");
    let mut lines = output.lines();
    let Some(header_line) = lines.find(|line| line.trim() == header) else {
        return std::collections::BTreeMap::new();
    };
    let header_indent = header_line.len() - header_line.trim_start().len();

    lines
        .take_while(|line| {
            line.trim().is_empty() || line.len() - line.trim_start().len() > header_indent
        })
        .map(str::trim)
        .filter(|line| line.starts_with(prefix))
        .map(|line| {
            let physical_node = metric_line_value(line, "physical_node")
                .unwrap_or_else(|| panic!("expected physical_node in metric line '{line}'"))
                .to_string();
            let total = metric_line_value(line, "total")
                .unwrap_or_else(|| panic!("expected total in metric line '{line}'"))
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("invalid total in metric line '{line}': {error}"));
            (physical_node, total)
        })
        .collect()
}

fn metric_line_value<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    line.split_whitespace()
        .filter_map(|part| part.split_once('='))
        .find_map(|(name, value)| (name == field).then_some(value))
}

#[derive(Debug)]
struct NumericMetricAssertion {
    field: String,
    op: NumericMetricOperator,
    expected: f64,
}

impl NumericMetricAssertion {
    fn matches_metric_line(&self, line: &str) -> bool {
        let Some(value) = metric_line_value(line, &self.field) else {
            return false;
        };
        let actual = if value == "-" {
            f64::NAN
        } else {
            let Ok(actual) = value.parse::<f64>() else {
                return false;
            };
            actual
        };
        self.op.matches(actual, self.expected)
    }
}

#[derive(Debug)]
enum NumericMetricOperator {
    Equal,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

impl NumericMetricOperator {
    fn matches(&self, actual: f64, expected: f64) -> bool {
        match self {
            Self::Equal => (actual - expected).abs() < f64::EPSILON,
            Self::GreaterThan => actual > expected,
            Self::LessThan => actual < expected,
            Self::GreaterThanOrEqual => actual >= expected,
            Self::LessThanOrEqual => actual <= expected,
        }
    }
}

fn parse_numeric_metric_assertion(assertion: &str) -> NumericMetricAssertion {
    let operators = [
        (">=", NumericMetricOperator::GreaterThanOrEqual),
        ("<=", NumericMetricOperator::LessThanOrEqual),
        (">", NumericMetricOperator::GreaterThan),
        ("<", NumericMetricOperator::LessThan),
        ("=", NumericMetricOperator::Equal),
    ];
    for (symbol, op) in operators {
        if let Some((field, expected)) = assertion.split_once(symbol) {
            return NumericMetricAssertion {
                field: field.trim().to_string(),
                op,
                expected: expected.trim().parse().unwrap_or_else(|error| {
                    panic!("invalid numeric assertion '{assertion}': {error}")
                }),
            };
        }
    }
    panic!("unsupported numeric metric assertion '{assertion}'");
}

#[then("the last command output does not contain")]
async fn then_last_command_output_does_not_contain(world: &mut ScenarioWorld, #[step] step: &Step) {
    let unexpected = expand_placeholders(world, docstring(step));
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before assertion");
    assert!(
        !output.contains(unexpected.trim()),
        "did not expect command output fragment {} in output, got: {output}",
        unexpected.trim()
    );
}

fn owner_from_describe_output(output: &str) -> Option<&str> {
    output
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("owner: ").map(str::trim))
}

fn replica_nodes_from_describe_output(output: &str) -> Vec<&str> {
    output
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("replicas: ").map(str::trim))
        .map(|replicas| {
            replicas
                .split(',')
                .map(str::trim)
                .filter(|replica| !replica.is_empty() && *replica != "-")
                .collect()
        })
        .unwrap_or_default()
}

fn scheduled_node_placement_from_status<'a>(
    status: &'a str,
    domain: &str,
    kind: &str,
    name: &str,
) -> Option<(&'a str, Vec<&'a str>)> {
    status.lines().map(str::trim).find_map(|line| {
        let line = line.strip_prefix("- ")?;
        let mut line_domain = None;
        let mut line_kind = None;
        let mut line_name = None;
        let mut owner = None;
        let mut replicas = None;

        for field in line.split_whitespace() {
            if let Some(value) = field.strip_prefix("domain=") {
                line_domain = Some(value);
            } else if let Some(value) = field.strip_prefix("kind=") {
                line_kind = Some(value);
            } else if let Some(value) = field.strip_prefix("name=") {
                line_name = Some(value);
            } else if let Some(value) = field.strip_prefix("owner=") {
                owner = Some(value);
            } else if let Some(value) = field.strip_prefix("replicas=") {
                replicas = Some(value);
            }
        }

        if line_domain == Some(domain) && line_kind == Some(kind) && line_name == Some(name) {
            let replica_nodes = replicas
                .filter(|value| *value != "-")
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|replica| !replica.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            owner.map(|owner| (owner, replica_nodes))
        } else {
            None
        }
    })
}

#[then(expr = "the last cluster status schedules nodes on at least {int} distinct owners")]
async fn then_last_cluster_status_uses_at_least_distinct_owners(
    world: &mut ScenarioWorld,
    expected_owner_count: usize,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a cluster status output must exist before checking scheduled owners");
    let domain_field = format!("domain={}", world.domain);
    let owners = output
        .lines()
        .map(str::trim)
        .filter(|line| {
            line.starts_with("- ") && line.split_whitespace().any(|field| field == domain_field)
        })
        .filter_map(|line| {
            line.split_whitespace()
                .find_map(|field| field.strip_prefix("owner="))
        })
        .filter(|owner| *owner != "-")
        .collect::<BTreeSet<_>>();
    assert!(
        owners.len() >= expected_owner_count,
        "expected at least {expected_owner_count} distinct scheduled owners for domain '{}', got \
         {owners:?} in: {output}",
        world.domain
    );
}

#[then(expr = "the last command output owner is saved as placeholder {string}")]
async fn then_last_command_output_owner_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before saving its owner");
    let owner = owner_from_describe_output(output)
        .unwrap_or_else(|| panic!("last command output must contain an owner line, got: {output}"))
        .to_string();
    world.placeholders.insert(placeholder, owner);
}

#[then(expr = "the first replica in the last command output is saved as placeholder {string}")]
async fn then_first_replica_in_last_command_output_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before saving its replica");
    let replica = replica_nodes_from_describe_output(output)
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            panic!("last command output must contain at least one replica, got: {output}")
        })
        .to_string();
    world.placeholders.insert(placeholder, replica);
}

#[then(
    expr = "the last cluster status owner for scheduled {string} {string} is saved as placeholder \
            {string}"
)]
async fn then_last_cluster_status_scheduled_owner_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    kind: String,
    name: String,
    placeholder: String,
) {
    let kind = expand_placeholders(world, &kind);
    let name = expand_placeholders(world, &name);
    let output = world
        .last_command_output
        .as_deref()
        .expect("a cluster status output must exist before saving its scheduled owner");
    let (owner, _) = scheduled_node_placement_from_status(output, &world.domain, &kind, &name)
        .unwrap_or_else(|| {
            panic!(
                "last command output must contain scheduled {kind} {name} placement for domain \
                 '{}', got: {output}",
                world.domain
            )
        });
    assert_ne!(
        owner, "-",
        "scheduled {kind} {name} in domain '{}' must have an owner, got: {output}",
        world.domain
    );
    world.placeholders.insert(placeholder, owner.to_string());
}

#[then(
    expr = "the first replica for scheduled {string} {string} in the last cluster status is saved \
            as placeholder {string}"
)]
async fn then_last_cluster_status_scheduled_first_replica_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    kind: String,
    name: String,
    placeholder: String,
) {
    let kind = expand_placeholders(world, &kind);
    let name = expand_placeholders(world, &name);
    let output = world
        .last_command_output
        .as_deref()
        .expect("a cluster status output must exist before saving its scheduled replica");
    let (_, replicas) = scheduled_node_placement_from_status(output, &world.domain, &kind, &name)
        .unwrap_or_else(|| {
            panic!(
                "last command output must contain scheduled {kind} {name} placement for domain \
                 '{}', got: {output}",
                world.domain
            )
        });
    let replica = replicas
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("scheduled {kind} {name} must contain at least one replica"));
    world.placeholders.insert(placeholder, replica.to_string());
}

#[then(expr = "a node other than placeholder {string} is saved as placeholder {string}")]
async fn then_a_node_other_than_placeholder_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    excluded_placeholder: String,
    placeholder: String,
) {
    let excluded = world
        .placeholders
        .get(&excluded_placeholder)
        .unwrap_or_else(|| {
            panic!("placeholder '{excluded_placeholder}' must be saved before assertion")
        });
    let node_id = world
        .cluster()
        .node_ids()
        .into_iter()
        .find(|node_id| node_id != excluded)
        .unwrap_or_else(|| {
            panic!("no node exists other than placeholder '{excluded_placeholder}'")
        });
    world.placeholders.insert(placeholder, node_id);
}

#[then(
    expr = "a node other than placeholders {string} and {string} is saved as placeholder {string}"
)]
async fn then_a_node_other_than_two_placeholders_is_saved_as_placeholder(
    world: &mut ScenarioWorld,
    first_excluded_placeholder: String,
    second_excluded_placeholder: String,
    placeholder: String,
) {
    let first_excluded = world
        .placeholders
        .get(&first_excluded_placeholder)
        .unwrap_or_else(|| {
            panic!("placeholder '{first_excluded_placeholder}' must be saved before assertion")
        });
    let second_excluded = world
        .placeholders
        .get(&second_excluded_placeholder)
        .unwrap_or_else(|| {
            panic!("placeholder '{second_excluded_placeholder}' must be saved before assertion")
        });
    let node_id = world
        .cluster()
        .node_ids()
        .into_iter()
        .find(|node_id| node_id != first_excluded && node_id != second_excluded)
        .unwrap_or_else(|| {
            panic!(
                "no node exists other than placeholders '{first_excluded_placeholder}' and \
                 '{second_excluded_placeholder}'"
            )
        });
    world.placeholders.insert(placeholder, node_id);
}

#[then(expr = "the last command output owner equals placeholder {string}")]
async fn then_last_command_output_owner_equals_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let expected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"));
    let output = world
        .last_command_output
        .as_deref()
        .expect("a command output must exist before owner assertion");
    let actual = owner_from_describe_output(output)
        .unwrap_or_else(|| panic!("last command output must contain an owner line, got: {output}"));
    assert_eq!(
        actual, expected,
        "expected owner placeholder '{placeholder}' to match last command output owner"
    );
}

#[when("these NSPL commands are executed on a node that is not the last described hash map owner")]
async fn when_these_nspl_commands_are_executed_on_non_hash_map_owner(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_server_error = None;
    let owner = world
        .last_command_output
        .as_deref()
        .and_then(owner_from_describe_output)
        .expect("last command output must be DESCRIBE HASH MAP output with an owner")
        .to_string();
    let node_id = world
        .cluster()
        .node_other_than(&owner)
        .expect("cluster must contain a node other than the hash map owner");
    let commands = expand_placeholders(world, docstring(step));
    let session = execute_nspl_commands_on_node(world, &node_id, &commands)
        .await
        .expect("failed to execute NSPL command on non-owner node");
    world.active_session = Some(session);
    world.active_session_node = Some(node_id);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[when(
    "these NSPL commands are executed on a node that is not a holder of the last described hash \
     map"
)]
async fn when_these_nspl_commands_are_executed_on_non_hash_map_holder(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    world.last_command_error = None;
    world.last_server_error = None;
    let output = world
        .last_command_output
        .as_deref()
        .expect("last command output must be DESCRIBE HASH MAP output");
    let owner = owner_from_describe_output(output)
        .expect("last command output must be DESCRIBE HASH MAP output with an owner")
        .to_string();
    let replicas = replica_nodes_from_describe_output(output)
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let node_id = world
        .cluster()
        .node_ids()
        .into_iter()
        .find(|node_id| node_id != &owner && !replicas.contains(node_id))
        .unwrap_or_else(|| {
            panic!(
                "cluster must contain a node that is neither hash map owner '{owner}' nor \
                 replicas {:?}",
                replicas
            )
        });
    let commands = expand_placeholders(world, docstring(step));
    let session = execute_nspl_commands_on_node(world, &node_id, &commands)
        .await
        .expect("failed to execute NSPL command on non-holder node");
    world.active_session = Some(session);
    world.active_session_node = Some(node_id);
    world.active_session_has_subscription = commands_update_subscription_state(false, &commands);
}

#[then(expr = "within {string} DESCRIBE INGESTOR {string} on the leader node contains")]
async fn then_within_duration_describe_ingestor_on_leader_contains(
    world: &mut ScenarioWorld,
    duration: String,
    ingestor: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let ingestor = expand_placeholders(world, &ingestor);
    let expected = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let leader = current_leader_node(world).await;
        let output =
            run_nspl_commands_on_node(world, &leader, &format!("DESCRIBE INGESTOR {ingestor};"))
                .await
                .expect("describe ingestor command must succeed");
        world.last_command_output = Some(output.clone());
        if output.contains(expected.trim()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for DESCRIBE INGESTOR {ingestor} to contain {}. last output: \
             {output}",
            expected.trim()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "within {string} DESCRIBE WASM PROCESSOR {string} on the leader node contains")]
async fn then_within_duration_describe_wasm_processor_on_leader_contains(
    world: &mut ScenarioWorld,
    duration: String,
    processor: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let processor = expand_placeholders(world, &processor);
    let expected = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let leader = current_leader_node(world).await;
        let output = run_nspl_commands_on_node(
            world,
            &leader,
            &format!("DESCRIBE WASM PROCESSOR {processor};"),
        )
        .await
        .expect("describe wasm processor command must succeed");
        world.last_command_output = Some(output.clone());
        if output.contains(expected.trim()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for DESCRIBE WASM PROCESSOR {processor} to contain {}. last \
             output: {output}",
            expected.trim()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "the last client outcome reports WASM reset phase {string} at generation {int}")]
fn then_last_client_outcome_reports_wasm_reset(
    world: &mut ScenarioWorld,
    phase: String,
    generation: u64,
) {
    let outcome = world
        .last_client_outcome
        .as_ref()
        .assured("the preceding step executed a client command");
    let state = outcome
        .wasm_state
        .as_ref()
        .assured("the preceding command described a WASM processor");
    let reset = state
        .reset
        .as_ref()
        .assured("the preceding transaction published a WASM state reset");
    assert_eq!(reset.reset.phase().as_ref(), phase);
    assert_eq!(u64::from(reset.generation), generation);
}

/// Assert that one of two emitters reports the given text.
///
/// Which of two peers contending for the last connection ends up holding it and which ends up
/// waiting is a race, and the claim under test is about the waiter rather than about a particular
/// name, so naming one of them would test the race instead of the behaviour.
#[then(expr = "within {string} DESCRIBE EMITTER {string} or {string} on the leader node contains")]
async fn then_describe_one_of_two_emitters_contains(
    world: &mut ScenarioWorld,
    duration: String,
    first: String,
    second: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let emitters = [
        expand_placeholders(world, &first),
        expand_placeholders(world, &second),
    ];
    let expected = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let leader = current_leader_node(world).await;
        let mut outputs = Vec::with_capacity(emitters.len());
        for emitter in &emitters {
            let output =
                run_nspl_commands_on_node(world, &leader, &format!("DESCRIBE EMITTER {emitter};"))
                    .await
                    .expect("describe emitter command must succeed");
            if output.contains(expected.trim()) {
                world.last_command_output = Some(output);
                return;
            }
            outputs.push(output);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {} or {} to contain {}. last outputs: {outputs:?}",
            emitters[0],
            emitters[1],
            expected.trim()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "within {string} DESCRIBE EMITTER {string} on the leader node contains")]
async fn then_within_duration_describe_emitter_on_leader_contains(
    world: &mut ScenarioWorld,
    duration: String,
    emitter: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let emitter = expand_placeholders(world, &emitter);
    let expected = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let leader = current_leader_node(world).await;
        let output =
            run_nspl_commands_on_node(world, &leader, &format!("DESCRIBE EMITTER {emitter};"))
                .await
                .expect("describe emitter command must succeed");
        world.last_command_output = Some(output.clone());
        if output.contains(expected.trim()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for DESCRIBE EMITTER {emitter} to contain {}. last output: {output}",
            expected.trim()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "node {string} eventually reports status containing {string}")]
async fn then_node_eventually_reports_status_containing(
    world: &mut ScenarioWorld,
    node_id: String,
    fragment: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .cluster()
        .wait_for_status_contains(&node_id, &expand_placeholders(world, &fragment))
        .await
        .expect("cluster status fragment did not appear");
}

#[then(
    expr = "within {string} node {string} eventually reports deduplicator {string} owner equals \
            placeholder {string}"
)]
async fn then_within_duration_node_eventually_reports_deduplicator_owner_equals_placeholder(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    deduplicator: String,
    placeholder: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let deduplicator = expand_placeholders(world, &deduplicator);
    let expected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"))
        .clone();
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(
            world,
            &node_id,
            &format!("DESCRIBE DEDUPLICATOR {deduplicator};"),
        )
        .await
        {
            Ok(output) => {
                world.last_command_output = Some(output.clone());
                if owner_from_describe_output(&output) == Some(expected.as_str()) {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(error),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for deduplicator '{deduplicator}' owner to equal '{expected}'. \
             last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports deduplicator {string} owner \
            different from placeholder {string}"
)]
async fn then_within_duration_node_eventually_reports_deduplicator_owner_different_from_placeholder(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    deduplicator: String,
    placeholder: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let deduplicator = expand_placeholders(world, &deduplicator);
    let unexpected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"))
        .clone();
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(
            world,
            &node_id,
            &format!("DESCRIBE DEDUPLICATOR {deduplicator};"),
        )
        .await
        {
            Ok(output) => {
                world.last_command_output = Some(output.clone());
                if owner_from_describe_output(&output)
                    .is_some_and(|owner| owner != unexpected.as_str())
                {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(error),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for deduplicator '{deduplicator}' owner to differ from \
             '{unexpected}'. last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports scheduled {string} {string} owner \
            equals placeholder {string}"
)]
async fn then_within_duration_node_eventually_reports_scheduled_owner_equals_placeholder(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    kind: String,
    name: String,
    placeholder: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let kind = expand_placeholders(world, &kind);
    let name = expand_placeholders(world, &name);
    let expected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"))
        .clone();
    let placement = PhaseDeadline::after(duration);

    loop {
        tokio::task::consume_budget().await;
        assert!(
            !placement.has_passed(),
            "timed out waiting for scheduled {kind} {name} owner to equal '{expected}'. last \
             output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        match world.cluster().status_text(&node_id, placement).await {
            Ok(output) => {
                world.last_command_output = Some(output.clone());
                if scheduled_node_placement_from_status(&output, &world.domain, &kind, &name)
                    .is_some_and(|(owner, _)| owner == expected)
                {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(format!("{error:#}")),
        }
        placement.pause(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "for {string} node {string} keeps reporting scheduled {string} {string} owner equal to \
            placeholder {string}"
)]
async fn then_for_duration_node_keeps_reporting_scheduled_owner_equal_to_placeholder(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    kind: String,
    name: String,
    placeholder: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let kind = expand_placeholders(world, &kind);
    let name = expand_placeholders(world, &name);
    let expected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"))
        .clone();
    let observation = PhaseDeadline::after(duration);

    while !observation.has_passed() {
        tokio::task::consume_budget().await;
        // The observation window bounds how long the owner is watched, not how long one read may
        // take, so every read keeps a full request budget even near the end of the window.
        let output = world
            .cluster()
            .status_text(&node_id, PhaseDeadline::after(STATUS_REQUEST_TIMEOUT))
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "cluster status must be readable while observing assignment stability: \
                     {error:#}"
                )
            });
        world.last_command_output = Some(output.clone());
        let owner = scheduled_node_placement_from_status(&output, &world.domain, &kind, &name)
            .map(|(owner, _)| owner.to_string())
            .unwrap_or_else(|| {
                panic!("scheduled {kind} {name} must remain in the schedule, got: {output}")
            });
        assert_eq!(
            owner, expected,
            "scheduled {kind} {name} must keep owner '{expected}', got: {output}"
        );
        observation.pause(Duration::from_millis(250)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports scheduled {string} {string} owner \
            different from placeholder {string}"
)]
async fn then_within_duration_node_eventually_reports_scheduled_owner_different_from_placeholder(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    kind: String,
    name: String,
    placeholder: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let kind = expand_placeholders(world, &kind);
    let name = expand_placeholders(world, &name);
    let unexpected = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before assertion"))
        .clone();
    let placement = PhaseDeadline::after(duration);

    loop {
        tokio::task::consume_budget().await;
        assert!(
            !placement.has_passed(),
            "timed out waiting for scheduled {kind} {name} owner to differ from '{unexpected}'. \
             last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error
        );
        match world.cluster().status_text(&node_id, placement).await {
            Ok(output) => {
                world.last_command_output = Some(output.clone());
                if scheduled_node_placement_from_status(&output, &world.domain, &kind, &name)
                    .is_some_and(|(owner, _)| owner != unexpected)
                {
                    return;
                }
            }
            Err(error) => world.last_command_error = Some(format!("{error:#}")),
        }
        placement.pause(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "node {string} observability path {string} eventually responds with {int} and {string}"
)]
async fn then_node_observability_path_eventually_responds(
    world: &mut ScenarioWorld,
    node_id: String,
    path: String,
    expected_status: u16,
    expected_body: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let path = expand_placeholders(world, &path);
    let expected_body = expand_placeholders(world, &expected_body);
    world
        .cluster()
        .wait_for_observability_response(&node_id, &path, expected_status, &expected_body)
        .await
        .expect("observability endpoint did not return the expected response");
}

#[then(
    expr = "node {string} observability path {string} eventually responds with {int} and contains \
            {string}"
)]
async fn then_node_observability_path_eventually_responds_containing(
    world: &mut ScenarioWorld,
    node_id: String,
    path: String,
    expected_status: u16,
    expected_body_fragment: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let path = expand_placeholders(world, &path);
    let expected_body_fragment = expand_placeholders(world, &expected_body_fragment);
    world
        .cluster()
        .wait_for_observability_response_containing(
            &node_id,
            &path,
            expected_status,
            &expected_body_fragment,
        )
        .await
        .expect("observability endpoint did not return the expected response");
}

#[then(expr = "node {string} observability metric {string} with labels eventually equals {int}")]
async fn then_node_observability_metric_with_labels_eventually_equals(
    world: &mut ScenarioWorld,
    node_id: String,
    metric_name: String,
    expected_value: i64,
    #[step] step: &Step,
) {
    world
        .wait_for_observability_metric_value(&node_id, &metric_name, expected_value, None, step)
        .await;
}

#[then(
    expr = "within {string} node {string} observability metric {string} with labels eventually \
            equals {int}"
)]
async fn then_within_duration_node_observability_metric_with_labels_eventually_equals(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    metric_name: String,
    expected_value: i64,
    #[step] step: &Step,
) {
    let wait =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    world
        .wait_for_observability_metric_value(
            &node_id,
            &metric_name,
            expected_value,
            Some(wait),
            step,
        )
        .await;
}

#[then(
    expr = "node {string} observability metric {string} with labels eventually reaches at least \
            {int}"
)]
async fn then_node_observability_metric_with_labels_eventually_reaches(
    world: &mut ScenarioWorld,
    node_id: String,
    metric_name: String,
    minimum_value: i64,
    #[step] step: &Step,
) {
    world
        .wait_for_observability_metric_at_least(&node_id, &metric_name, minimum_value, None, step)
        .await;
}

#[then(
    expr = "within {string} node {string} observability metric {string} with labels eventually \
            reaches at least {int}"
)]
async fn then_within_duration_node_observability_metric_with_labels_eventually_reaches(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    metric_name: String,
    minimum_value: i64,
    #[step] step: &Step,
) {
    let wait =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    world
        .wait_for_observability_metric_at_least(
            &node_id,
            &metric_name,
            minimum_value,
            Some(wait),
            step,
        )
        .await;
}

#[then(expr = "node {string} interconnection metrics use only bounded dimensions")]
async fn then_node_interconnection_metrics_use_bounded_dimensions(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let offending = world
        .cluster()
        .unbounded_interconnection_metric_samples(&node_id)
        .await
        .expect("observability endpoint did not answer with its metric exposition");
    assert!(
        offending.is_empty(),
        "node '{node_id}' exposed interconnection samples with unbounded dimensions: {offending:?}"
    );
}

#[then(expr = "within {string} node {string} eventually reports describe relay as {string}")]
async fn then_within_duration_node_eventually_reports_describe_stream_as(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    expected: String,
    #[step] step: &Step,
) {
    let timeout =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let commands = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + timeout;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, &commands).await {
            Ok(output) if output.contains(expected.as_str()) => {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => {
                world.last_command_output = Some(output);
            }
            Err(error) => {
                world.last_command_error = Some(error.clone());
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' to report describe relay as '{expected}'. \
             last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports describe ingestor {string} as \
            {string}"
)]
async fn then_within_duration_node_eventually_reports_describe_ingestor_as(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    ingestor: String,
    expected: String,
) {
    let timeout =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let ingestor = expand_placeholders(world, &ingestor);
    let expected = expand_placeholders(world, &expected);
    let commands = format!("DESCRIBE INGESTOR {ingestor};");
    let deadline = Instant::now() + timeout;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, &commands).await {
            Ok(output) if output.contains(expected.as_str()) => {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => {
                world.last_command_output = Some(output);
            }
            Err(error) => {
                world.last_command_error = Some(error.clone());
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' to report describe ingestor '{ingestor}' as \
             '{expected}'. last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "within {string} node {string} eventually reports describe resource as {string}")]
async fn then_within_duration_node_eventually_reports_describe_resource_as(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    expected: String,
    #[step] step: &Step,
) {
    let timeout =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let commands = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + timeout;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, &commands).await {
            Ok(output) if output.contains(expected.as_str()) => {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => {
                world.last_command_output = Some(output);
            }
            Err(error) => {
                world.last_command_error = Some(error.clone());
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' to report describe resource as '{expected}'. \
             last output: {:?}, last error: {:?}",
            world.last_command_output,
            world.last_command_error,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} node {string} eventually reports materialized state for relay \
            {string} containing"
)]
async fn then_within_duration_node_eventually_reports_materialized_state_containing(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    relay: String,
    #[step] step: &Step,
) {
    let timeout =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let expected = expand_placeholders(world, docstring(step));
    let command = format!(
        "SHOW RELAY {} MATERIALIZED STATE;",
        expand_placeholders(world, &relay)
    );
    let deadline = Instant::now() + timeout;

    loop {
        tokio::task::consume_budget().await;
        match run_nspl_commands_on_node(world, &node_id, &command).await {
            Ok(output) if output.contains(expected.trim()) => {
                world.last_command_output = Some(output);
                return;
            }
            Ok(output) => {
                world.last_command_output = Some(output);
            }
            Err(error) => {
                world.last_command_error = Some(error.clone());
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' to report materialized state containing {}. \
             last output: {:?}, last error: {:?}",
            expected.trim(),
            world.last_command_output,
            world.last_command_error,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[given(expr = "RabbitMQ queue {string} exists")]
async fn given_rabbitmq_queue_exists(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world
        .cluster()
        .ensure_rabbitmq_queue(&queue)
        .await
        .expect("failed to declare rabbitmq queue");
}

#[given(expr = "Kafka topic {string} exists with {int} partitions")]
async fn given_kafka_topic_exists_with_partitions(
    world: &mut ScenarioWorld,
    topic: String,
    partitions: usize,
) {
    let topic = expand_placeholders(world, &topic);
    let partitions =
        i32::try_from(partitions).assured("Kafka partition counts in cucumber features fit i32");
    world
        .cluster()
        .ensure_kafka_topic_partitions(&topic, partitions)
        .await
        .expect("failed to create kafka topic");
}

#[given(expr = "SQS queue {string} exists")]
async fn given_sqs_queue_exists(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world
        .cluster()
        .ensure_sqs_queue(&queue)
        .await
        .expect("failed to declare sqs queue");
}

#[given(expr = "TLS SQS queue {string} exists")]
async fn given_tls_sqs_queue_exists(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world
        .cluster()
        .ensure_sqs_queue_tls(&queue)
        .await
        .expect("failed to declare tls sqs queue");
}

#[given(expr = "Iceberg table {string} exists at {string} with columns")]
async fn given_iceberg_table_exists_at_with_columns(
    world: &mut ScenarioWorld,
    table: String,
    location: String,
    #[step] step: &Step,
) {
    let fixture = IcebergTableFixture::from_step(world, table, location, docstring(step));
    fixture
        .ensure()
        .await
        .expect("failed to create Iceberg table");
}

#[given(expr = "Kafka topic {string} is observed")]
async fn given_kafka_topic_is_observed(world: &mut ScenarioWorld, topic: String) {
    let topic = expand_placeholders(world, &topic);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_kafka(&topic)
            .await
            .expect("failed to observe kafka topic"),
    );
}

#[given(expr = "Pulsar topic {string} is observed")]
async fn given_pulsar_topic_is_observed(world: &mut ScenarioWorld, topic: String) {
    let topic = expand_placeholders(world, &topic);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_pulsar(&topic)
            .await
            .expect("failed to observe pulsar topic"),
    );
}

#[given(expr = "Pulsar TLS topic {string} is observed")]
async fn given_pulsar_tls_topic_is_observed(world: &mut ScenarioWorld, topic: String) {
    let topic = expand_placeholders(world, &topic);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_pulsar_tls(&topic)
            .await
            .expect("failed to observe pulsar tls topic"),
    );
}

#[given(expr = "RabbitMQ queue {string} is observed")]
async fn given_rabbitmq_queue_is_observed(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_rabbitmq(&queue)
            .await
            .expect("failed to observe rabbitmq queue"),
    );
}

#[given(expr = "Redis channel {string} is observed")]
async fn given_redis_channel_is_observed(world: &mut ScenarioWorld, channel: String) {
    let channel = expand_placeholders(world, &channel);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_redis(&channel)
            .await
            .expect("failed to observe redis channel"),
    );
}

#[given(expr = "MQTT topic {string} is observed")]
async fn given_mqtt_topic_is_observed(world: &mut ScenarioWorld, topic: String) {
    let topic = expand_placeholders(world, &topic);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_mqtt(&topic)
            .await
            .expect("failed to observe mqtt topic"),
    );
}

#[given(expr = "SQS queue {string} is observed")]
async fn given_sqs_queue_is_observed(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_sqs(&queue)
            .await
            .expect("failed to observe sqs queue"),
    );
}

#[given(expr = "NATS subject {string} is observed")]
async fn given_nats_subject_is_observed(world: &mut ScenarioWorld, subject: String) {
    let subject = expand_placeholders(world, &subject);
    world.broker_observer = Some(
        world
            .cluster()
            .observe_nats(&subject)
            .await
            .expect("failed to observe nats subject"),
    );
}

#[given(expr = "ZeroMQ emission endpoint {string} is observed")]
async fn given_zeromq_emission_endpoint_is_observed(world: &mut ScenarioWorld, addr: String) {
    let addr = expand_placeholders(world, &addr);
    match world.cluster().observe_zeromq(&addr).await {
        Ok(observer) => {
            world.broker_observer = Some(observer);
            return;
        }
        Err(error) if addr != world.zeromq_emit_addr => {
            panic!("failed to observe zeromq endpoint '{addr}': {error}");
        }
        Err(_) => {}
    }

    for _ in 0..ZEROMQ_OBSERVER_BIND_ATTEMPTS {
        tokio::task::consume_budget().await;
        let replacement = format!(
            "tcp://127.0.0.1:{}",
            draw_scenario_port(world, "replacement ZeroMQ emit")
        );
        if let Ok(observer) = world.cluster().observe_zeromq(&replacement).await {
            world.zeromq_emit_addr = replacement;
            world.broker_observer = Some(observer);
            return;
        }
    }

    panic!(
        "failed to observe a generated ZeroMQ endpoint after {ZEROMQ_OBSERVER_BIND_ATTEMPTS} \
         fresh port allocations"
    );
}

#[given(expr = "Syslog UDP emission endpoint {string} is observed")]
async fn given_syslog_udp_emission_endpoint_is_observed(world: &mut ScenarioWorld, addr: String) {
    initialize_scenario_identity(world);
    let addr = expand_placeholders(world, &addr);
    world.syslog_udp_observer = Some(
        tokio::net::UdpSocket::bind(&addr)
            .await
            .unwrap_or_else(|error| {
                panic!("failed to observe Syslog UDP endpoint '{addr}': {error}")
            }),
    );
}

#[given(expr = "ClickHouse table {string} exists")]
async fn given_clickhouse_table_exists(world: &mut ScenarioWorld, table: String) {
    let table = expand_placeholders(world, &table);
    clickhouse_post(
        world.dependencies.endpoints(),
        &format!("DROP TABLE IF EXISTS {table}"),
    )
    .await
    .expect("failed to drop ClickHouse table");
    clickhouse_post(
        world.dependencies.endpoints(),
        &format!(
            "CREATE TABLE {table} (clickhouse_user_id UInt32, clickhouse_now String, \
             clickhouse_action String) ENGINE = Memory"
        ),
    )
    .await
    .expect("failed to create ClickHouse table");
    world.clickhouse_table = Some(table);
    world.clickhouse_tls = false;
}

#[given(expr = "ClickHouse TLS table {string} exists")]
async fn given_clickhouse_tls_table_exists(world: &mut ScenarioWorld, table: String) {
    let table = expand_placeholders(world, &table);
    clickhouse_tls_post(
        world.dependencies.endpoints(),
        &format!("DROP TABLE IF EXISTS {table}"),
    )
    .await
    .expect("failed to drop ClickHouse TLS table");
    clickhouse_tls_post(
        world.dependencies.endpoints(),
        &format!(
            "CREATE TABLE {table} (clickhouse_user_id UInt32, clickhouse_now String, \
             clickhouse_action String) ENGINE = Memory"
        ),
    )
    .await
    .expect("failed to create ClickHouse TLS table");
    world.clickhouse_table = Some(table);
    world.clickhouse_tls = true;
}

#[given(expr = "ClickHouse MergeTree table {string} with merges stopped exists")]
async fn given_clickhouse_merge_tree_table_with_merges_stopped_exists(
    world: &mut ScenarioWorld,
    table: String,
) {
    let table = expand_placeholders(world, &table);
    clickhouse_post(
        world.dependencies.endpoints(),
        &format!("DROP TABLE IF EXISTS {table}"),
    )
    .await
    .expect("failed to drop ClickHouse table");
    clickhouse_post(
        world.dependencies.endpoints(),
        &format!(
            "CREATE TABLE {table} (clickhouse_user_id UInt32) ENGINE = MergeTree ORDER BY tuple()"
        ),
    )
    .await
    .expect("failed to create ClickHouse MergeTree table");
    clickhouse_post(
        world.dependencies.endpoints(),
        &format!("SYSTEM STOP MERGES {table}"),
    )
    .await
    .expect("failed to stop ClickHouse table merges");
    world.clickhouse_table = Some(table);
    world.clickhouse_tls = false;
}

#[given(expr = "Postgres table {string} exists")]
async fn given_postgres_table_exists(world: &mut ScenarioWorld, table: String) {
    prepare_postgres_table(world, table, false).await;
}

#[given(expr = "Postgres table {string} with primary key exists")]
async fn given_postgres_table_with_primary_key_exists(world: &mut ScenarioWorld, table: String) {
    prepare_postgres_table_with_primary_key(world, table, false).await;
}

#[given(expr = "Postgres table {string} rejecting poison actions exists")]
async fn given_postgres_table_rejecting_poison_actions_exists(
    world: &mut ScenarioWorld,
    table: String,
) {
    prepare_postgres_table(world, table, false).await;
    let table = world
        .postgres_table
        .as_ref()
        .expect("Postgres table should be recorded after preparation");
    let client = postgres_client(world.dependencies.endpoints(), false)
        .await
        .expect("failed to connect to Postgres");
    sqlx::raw_sql(SqlxAssertSqlSafe(format!(
        "ALTER TABLE {table} ADD CONSTRAINT reject_poison_action CHECK (postgres_action <> \
         'poison')"
    )))
    .execute(&client)
    .await
    .expect("failed to add Postgres poison-record constraint");
}

#[given(expr = "Postgres table {string} recording insert statement sizes exists")]
async fn given_postgres_table_recording_insert_statement_sizes_exists(
    world: &mut ScenarioWorld,
    table: String,
) {
    prepare_postgres_table(world, table, false).await;
    let table = world
        .postgres_table
        .as_ref()
        .expect("Postgres table should be recorded after preparation")
        .clone();
    let audit_table = format!("{table}_insert_audit");
    let trigger_function = format!("{table}_record_insert");
    let client = postgres_client(world.dependencies.endpoints(), false)
        .await
        .expect("failed to connect to Postgres");
    sqlx::raw_sql(SqlxAssertSqlSafe(format!(
        "DROP TABLE IF EXISTS {audit_table};
         DROP FUNCTION IF EXISTS {trigger_function}();
         CREATE TABLE {audit_table} (row_count bigint NOT NULL);
         CREATE FUNCTION {trigger_function}() RETURNS trigger AS $$
         BEGIN
           INSERT INTO {audit_table} (row_count)
           SELECT count(*) FROM inserted_rows;
           RETURN NULL;
         END;
         $$ LANGUAGE plpgsql;
         CREATE TRIGGER record_insert_statement_size
         AFTER INSERT ON {table}
         REFERENCING NEW TABLE AS inserted_rows
         FOR EACH STATEMENT EXECUTE FUNCTION {trigger_function}();"
    )))
    .execute(&client)
    .await
    .expect("failed to install Postgres insert statement recorder");
}

#[given(expr = "Postgres TLS table {string} exists")]
async fn given_postgres_tls_table_exists(world: &mut ScenarioWorld, table: String) {
    prepare_postgres_table(world, table, true).await;
}

async fn prepare_postgres_table(world: &mut ScenarioWorld, table: String, tls: bool) {
    prepare_postgres_table_schema(world, table, tls, false).await;
}

async fn prepare_postgres_table_with_primary_key(
    world: &mut ScenarioWorld,
    table: String,
    tls: bool,
) {
    prepare_postgres_table_schema(world, table, tls, true).await;
}

async fn prepare_postgres_table_schema(
    world: &mut ScenarioWorld,
    table: String,
    tls: bool,
    primary_key: bool,
) {
    let table = expand_placeholders(world, &table);
    let client = postgres_client(world.dependencies.endpoints(), tls)
        .await
        .expect("failed to connect to Postgres");
    sqlx::raw_sql(SqlxAssertSqlSafe(format!("DROP TABLE IF EXISTS {table}")))
        .execute(&client)
        .await
        .expect("failed to drop Postgres table");
    sqlx::raw_sql(SqlxAssertSqlSafe(format!(
        "CREATE TABLE {table} (postgres_user_id integer, postgres_now text, postgres_action \
         text{})",
        if primary_key {
            ", PRIMARY KEY (postgres_user_id)"
        } else {
            ""
        }
    )))
    .execute(&client)
    .await
    .expect("failed to create Postgres table");
    world.postgres_table = Some(table);
    world.postgres_tls = tls;
}

#[given(expr = "MySQL table {string} exists")]
async fn given_mysql_table_exists(world: &mut ScenarioWorld, table: String) {
    prepare_mysql_table(world, table, false).await;
}

#[given(expr = "MySQL TLS table {string} exists")]
async fn given_mysql_tls_table_exists(world: &mut ScenarioWorld, table: String) {
    prepare_mysql_table(world, table, true).await;
}

#[given(expr = "MySQL table {string} with primary key exists")]
async fn given_mysql_table_with_primary_key_exists(world: &mut ScenarioWorld, table: String) {
    prepare_mysql_table_schema(world, table, false, true).await;
}

#[given(expr = "MySQL table {string} recording insert commands exists")]
async fn given_mysql_table_recording_insert_commands_exists(
    world: &mut ScenarioWorld,
    table: String,
) {
    prepare_mysql_table(world, table, false).await;
    let table = world
        .mysql_table
        .as_ref()
        .expect("MySQL table should be recorded after preparation")
        .clone();
    let pool =
        mysql_root_pool(world.dependencies.endpoints()).expect("failed to build MySQL root pool");
    let mut conn = pool
        .get_conn()
        .await
        .expect("failed to connect to MySQL as root");
    conn.query_drop("SET GLOBAL log_output = 'TABLE'")
        .await
        .expect("failed to direct the MySQL general log to its table");
    conn.query_drop("SET GLOBAL general_log = 'ON'")
        .await
        .expect("failed to enable the MySQL general log");
    let insert_prefix = format!("INSERT INTO `{table}`%");
    world.mysql_insert_command_baseline = Some(
        conn.exec_first::<u64, _, _>(
            "SELECT COUNT(*) FROM mysql.general_log
             WHERE command_type IN ('Execute', 'Query') AND argument LIKE ?",
            (&insert_prefix,),
        )
        .await
        .expect("failed to read the MySQL insert-command baseline")
        .unwrap_or(0),
    );
    drop(conn);
    pool.disconnect()
        .await
        .expect("failed to disconnect MySQL root pool");
}

async fn prepare_mysql_table(world: &mut ScenarioWorld, table: String, tls: bool) {
    prepare_mysql_table_schema(world, table, tls, false).await;
}

async fn prepare_mysql_table_schema(
    world: &mut ScenarioWorld,
    table: String,
    tls: bool,
    primary_key: bool,
) {
    let table = expand_placeholders(world, &table);
    let pool = mysql_pool(world.dependencies.endpoints(), tls).expect("failed to build MySQL pool");
    let mut conn = pool.get_conn().await.expect("failed to connect to MySQL");
    conn.query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("failed to drop MySQL table");
    conn.query_drop(format!(
        "CREATE TABLE `{table}` (mysql_user_id integer{}, mysql_now text, mysql_action text)",
        if primary_key { " PRIMARY KEY" } else { "" }
    ))
    .await
    .expect("failed to create MySQL table");
    drop(conn);
    pool.disconnect()
        .await
        .expect("failed to disconnect MySQL pool");
    world.mysql_table = Some(table);
    world.mysql_tls = tls;
}

#[given(expr = "MongoDB collection {string} exists")]
async fn given_mongodb_collection_exists(world: &mut ScenarioWorld, collection: String) {
    prepare_mongodb_collection(world, collection, false).await;
}

#[given(expr = "MongoDB collection {string} with unique user id exists")]
async fn given_mongodb_collection_with_unique_user_id_exists(
    world: &mut ScenarioWorld,
    collection: String,
) {
    prepare_mongodb_collection_schema(world, collection, false, true).await;
}

#[given(expr = "MongoDB collection {string} rejecting poison actions exists")]
async fn given_mongodb_collection_rejecting_poison_actions_exists(
    world: &mut ScenarioWorld,
    collection: String,
) {
    prepare_mongodb_collection(world, collection, false).await;
    let collection = world
        .mongodb_collection
        .as_ref()
        .expect("MongoDB collection should be recorded after preparation")
        .clone();
    let client = mongodb_client(world.dependencies.endpoints(), false)
        .await
        .expect("failed to connect to MongoDB");
    client
        .database("nervix")
        .run_command(mongodb_doc! {
            "collMod": &collection,
            "validator": {
                "mongodb_action": { "$ne": "poison" },
            },
            "validationLevel": "strict",
            "validationAction": "error",
        })
        .await
        .expect("failed to install MongoDB poison-record validator");
}

#[given(expr = "MongoDB collection {string} recording insert command sizes exists")]
async fn given_mongodb_collection_recording_insert_command_sizes_exists(
    world: &mut ScenarioWorld,
    collection: String,
) {
    prepare_mongodb_collection(world, collection, false).await;
    let client = mongodb_client(world.dependencies.endpoints(), false)
        .await
        .expect("failed to connect to MongoDB");
    client
        .database("nervix")
        .run_command(mongodb_doc! {
            "profile": 2,
            "slowms": 0,
            "sampleRate": 1.0,
        })
        .await
        .expect("failed to enable MongoDB command profiling");
}

#[given(expr = "MongoDB TLS collection {string} exists")]
async fn given_mongodb_tls_collection_exists(world: &mut ScenarioWorld, collection: String) {
    prepare_mongodb_collection(world, collection, true).await;
}

async fn prepare_mongodb_collection(world: &mut ScenarioWorld, collection: String, tls: bool) {
    prepare_mongodb_collection_schema(world, collection, tls, false).await;
}

async fn prepare_mongodb_collection_schema(
    world: &mut ScenarioWorld,
    collection: String,
    tls: bool,
    unique_user_id: bool,
) {
    let collection = expand_placeholders(world, &collection);
    let client = mongodb_client(world.dependencies.endpoints(), tls)
        .await
        .expect("failed to connect to MongoDB");
    client
        .database("nervix")
        .collection::<MongoDbDocument>(&collection)
        .drop()
        .await
        .expect("failed to drop MongoDB collection");
    client
        .database("nervix")
        .create_collection(&collection)
        .await
        .expect("failed to create MongoDB collection");
    if unique_user_id {
        client
            .database("nervix")
            .run_command(mongodb_doc! {
                "createIndexes": &collection,
                "indexes": [{
                    "key": { "mongodb_user_id": 1 },
                    "name": "mongodb_user_id_unique",
                    "unique": true,
                }],
            })
            .await
            .expect("failed to create MongoDB unique user id index");
    }
    world.mongodb_collection = Some(collection);
    world.mongodb_tls = tls;
}

#[when(expr = "MQTT message is published to topic {string}")]
async fn when_mqtt_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    wait_for_mqtt_ingestors_ready(world).await;
    world
        .cluster()
        .publish_mqtt(&topic, &payload)
        .await
        .expect("failed to publish mqtt message");
}

#[when(expr = "these MQTT messages are rapidly published to topic {string}")]
async fn when_these_mqtt_messages_are_rapidly_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payloads = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !payloads.is_empty(),
        "at least one MQTT payload is required"
    );
    wait_for_mqtt_ingestors_ready(world).await;
    for payload in payloads {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_mqtt(&topic, &payload)
            .await
            .expect("failed to publish mqtt message");
    }
}

#[when(expr = "MQTT QoS 1 message is published to topic {string}")]
async fn when_mqtt_qos1_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    wait_for_mqtt_ingestors_ready(world).await;
    world
        .cluster()
        .publish_mqtt_qos1(&topic, &payload)
        .await
        .expect("failed to publish mqtt QoS 1 message");
}

#[when(expr = "RabbitMQ message is published to queue {string}")]
async fn when_rabbitmq_message_is_published(
    world: &mut ScenarioWorld,
    queue: String,
    #[step] step: &Step,
) {
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_rabbitmq(&queue, &payload)
        .await
        .expect("failed to publish rabbitmq message");
}

#[when(expr = "Redis message is published to channel {string}")]
async fn when_redis_message_is_published(
    world: &mut ScenarioWorld,
    channel: String,
    #[step] step: &Step,
) {
    let channel = expand_placeholders(world, &channel);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_redis(&channel, &payload)
        .await
        .expect("failed to publish redis message");
}

#[when(expr = "Kafka message is published to topic {string}")]
async fn when_kafka_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    world.last_publish_at = Some(Instant::now());
    world
        .cluster()
        .publish_kafka(&topic, &payload)
        .await
        .expect("failed to publish kafka message");
}

#[when(
    expr = "{int} JSON messages with user id {int} are rapidly published to {string} input \
            {string}"
)]
async fn when_json_messages_with_user_id_are_rapidly_published_to_input(
    world: &mut ScenarioWorld,
    count: u64,
    user_id: u32,
    source_kind: String,
    input: String,
) {
    let input = expand_placeholders(world, &input);
    let source_kind = source_kind.to_ascii_uppercase();
    let payload = format!(r#"{{"user_id":{user_id}}}"#);

    let count = count
        .try_into()
        .expect("rapid publish count must fit into usize");
    if source_kind == "MQTT" || source_kind == "MQTT_QOS1" {
        wait_for_mqtt_ingestors_ready(world).await;
    }
    match source_kind.as_str() {
        "KAFKA" => world
            .cluster()
            .publish_kafka_payloads(&input, &vec![payload.clone(); count])
            .await
            .expect("failed to publish kafka message burst"),
        "MQTT" => world
            .cluster()
            .publish_mqtt_burst(&input, &payload, count)
            .await
            .expect("failed to publish mqtt message burst"),
        "MQTT_QOS1" => world
            .cluster()
            .publish_mqtt_qos1_burst(&input, &payload, count)
            .await
            .expect("failed to publish mqtt QoS 1 message burst"),
        "REDIS" => world
            .cluster()
            .publish_redis_burst(&input, &payload, count)
            .await
            .expect("failed to publish redis message burst"),
        unsupported => panic!("unsupported rapid ingestor input source kind '{unsupported}'"),
    }
}

#[when(expr = "these Kafka messages are rapidly published to topic {string}")]
async fn when_these_kafka_messages_are_rapidly_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payloads = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !payloads.is_empty(),
        "at least one Kafka payload is required"
    );
    world
        .cluster()
        .publish_kafka_payloads(&topic, &payloads)
        .await
        .expect("failed to publish kafka messages");
}

#[when(expr = "Pulsar message is published to topic {string}")]
async fn when_pulsar_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_pulsar(&topic, &payload)
        .await
        .expect("failed to publish pulsar message");
}

#[when(expr = "Pulsar TLS message is published to topic {string}")]
async fn when_pulsar_tls_message_is_published(
    world: &mut ScenarioWorld,
    topic: String,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_pulsar_tls(&topic, &payload)
        .await
        .expect("failed to publish pulsar tls message");
}

#[when(expr = "Kafka message is published to topic {string} partition {int}")]
async fn when_kafka_message_is_published_to_partition(
    world: &mut ScenarioWorld,
    topic: String,
    partition: usize,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let partition =
        i32::try_from(partition).assured("Kafka partition ids in cucumber features fit i32");
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_kafka_partition(&topic, partition, &payload)
        .await
        .expect("failed to publish kafka message");
}

#[when(expr = "Kafka message with headers {string} is published to topic {string} partition {int}")]
async fn when_kafka_message_with_headers_is_published_to_partition(
    world: &mut ScenarioWorld,
    headers: String,
    topic: String,
    partition: usize,
    #[step] step: &Step,
) {
    let topic = expand_placeholders(world, &topic);
    let partition =
        i32::try_from(partition).assured("Kafka partition ids in cucumber features fit i32");
    let payload = expand_placeholders(world, docstring(step));
    let headers = headers
        .split(',')
        .map(str::trim)
        .filter(|header| !header.is_empty())
        .map(|header| {
            header
                .split_once('=')
                .expect("kafka header must be written as 'name=value'")
        })
        .collect::<Vec<_>>();
    world
        .cluster()
        .publish_kafka_partition_with_headers(&topic, partition, &payload, &headers)
        .await
        .expect("failed to publish kafka message with headers");
}

#[when(expr = "Kafka topic {string} partition count is changed to {int}")]
async fn when_kafka_topic_partition_count_is_changed_to(
    world: &mut ScenarioWorld,
    topic: String,
    partitions: usize,
) {
    let topic = expand_placeholders(world, &topic);
    let partitions =
        i32::try_from(partitions).assured("Kafka partition counts in cucumber features fit i32");
    world
        .cluster()
        .ensure_kafka_topic_partitions(&topic, partitions)
        .await
        .expect("failed to change kafka topic partition count");
}

#[when(expr = "Kafka topic {string} is reset to {int} partitions")]
async fn when_kafka_topic_is_reset_to_partitions(
    world: &mut ScenarioWorld,
    topic: String,
    partitions: usize,
) {
    let topic = expand_placeholders(world, &topic);
    let partitions =
        i32::try_from(partitions).assured("Kafka partition counts in cucumber features fit i32");
    world
        .cluster()
        .reset_kafka_topic_partitions(&topic, partitions)
        .await
        .expect("failed to reset kafka topic partition count");
}

#[when(expr = "SQS message is published to queue {string}")]
async fn when_sqs_message_is_published(
    world: &mut ScenarioWorld,
    queue: String,
    #[step] step: &Step,
) {
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_sqs(&queue, &payload)
        .await
        .expect("failed to publish sqs message");
}

#[when(expr = "TLS SQS message is published to queue {string}")]
async fn when_tls_sqs_message_is_published(
    world: &mut ScenarioWorld,
    queue: String,
    #[step] step: &Step,
) {
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_sqs_tls(&queue, &payload)
        .await
        .expect("failed to publish tls sqs message");
}

#[when(expr = "NATS message is published to subject {string}")]
async fn when_nats_message_is_published(
    world: &mut ScenarioWorld,
    subject: String,
    #[step] step: &Step,
) {
    let subject = expand_placeholders(world, &subject);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_nats(&subject, &payload)
        .await
        .expect("failed to publish nats message");
}

#[when(expr = "NATS JetStream stream {string} is provisioned for subject {string}")]
async fn when_nats_jetstream_stream_is_provisioned(
    world: &mut ScenarioWorld,
    stream: String,
    subject: String,
) {
    let stream = expand_placeholders(world, &stream);
    let subject = expand_placeholders(world, &subject);
    world
        .cluster()
        .provision_nats_stream(&stream, &subject)
        .await
        .expect("failed to provision NATS JetStream stream");
}

#[then(expr = "NATS JetStream stream {string} eventually contains a payload on subject {string}")]
async fn then_nats_jetstream_stream_eventually_contains_payload(
    world: &mut ScenarioWorld,
    stream: String,
    subject: String,
    #[step] step: &Step,
) {
    let stream = expand_placeholders(world, &stream);
    let subject = expand_placeholders(world, &subject);
    let expected = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .wait_for_nats_stream_payload(&stream, &subject, &expected)
        .await
        .expect("NATS JetStream stream did not receive expected payload");
}

#[when(expr = "{int} sequential NATS messages are published to subject {string}")]
async fn when_sequential_nats_messages_are_published(
    world: &mut ScenarioWorld,
    count: usize,
    subject: String,
    #[step] step: &Step,
) {
    let subject = expand_placeholders(world, &subject);
    let template = expand_placeholders(world, docstring(step));
    assert!(
        template.contains("{{sequence}}"),
        "sequential NATS payload template must contain {{{{sequence}}}}"
    );
    let payloads = (1..=count)
        .map(|sequence| template.replace("{{sequence}}", &sequence.to_string()))
        .collect::<Vec<_>>();
    world
        .cluster()
        .publish_nats_payloads(&subject, &payloads)
        .await
        .expect("failed to publish sequential nats messages");
}

#[when("ZeroMQ message is published")]
async fn when_zeromq_message_is_published(world: &mut ScenarioWorld, #[step] step: &Step) {
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_zeromq(&world.zeromq_ingest_addr, &payload)
        .await
        .expect("failed to publish zeromq message");
}

#[when(expr = "Syslog UDP message is published to {string}")]
async fn when_syslog_udp_message_is_published_to(
    world: &mut ScenarioWorld,
    addr: String,
    #[step] step: &Step,
) {
    let addr = expand_placeholders(world, &addr);
    let payload = expand_placeholders(world, docstring(step));
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("failed to bind Syslog UDP test sender");
    socket
        .send_to(payload.trim().as_bytes(), &addr)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to publish Syslog UDP message to '{addr}': {error}")
        });
}

#[then(
    expr = "node {string} eventually forwards Syslog UDP message {string} at {string} to the \
            observed endpoint"
)]
async fn then_node_eventually_forwards_syslog_udp_message(
    world: &mut ScenarioWorld,
    node_id: String,
    message: String,
    addr: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let message = expand_placeholders(world, &message);
    let configured_addr = expand_placeholders(world, &addr);
    let addr = world
        .cluster()
        .syslog_ingestor_addr(&node_id, &configured_addr)
        .expect("failed to resolve node-local Syslog ingestor address");
    let payload =
        format!("<34>1 2003-10-11T22:14:15.003Z lifecycle.example test 1 ID47 - {message}");
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("failed to bind Syslog UDP test sender");
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut received = vec![0_u8; 65_535];

    loop {
        tokio::task::consume_budget().await;
        let _ = socket.send_to(payload.as_bytes(), &addr).await;
        let next = tokio::time::timeout(
            Duration::from_millis(250),
            world
                .syslog_udp_observer
                .as_ref()
                .expect("a Syslog UDP observer must exist before assertion")
                .recv_from(&mut received),
        )
        .await;
        if let Ok(Ok((length, _))) = next
            && let Ok(actual) = std::str::from_utf8(&received[..length])
            && actual.contains(&message)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for node '{node_id}' Syslog UDP traffic to reach the observed \
             endpoint"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[when(expr = "Syslog TCP messages are published with mixed framing to {string}")]
async fn when_syslog_tcp_messages_are_published_with_mixed_framing_to(
    world: &mut ScenarioWorld,
    addr: String,
    #[step] step: &Step,
) {
    use tokio::io::AsyncWriteExt as _;

    let addr = expand_placeholders(world, &addr);
    let messages = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(
        messages.len(),
        2,
        "mixed Syslog TCP framing step requires exactly two messages"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        match tokio::net::TcpStream::connect(&addr).await {
            Ok(stream) => break stream,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out connecting to Syslog TCP listener '{addr}': {error}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };
    let octet_counted = messages[0].as_bytes();
    stream
        .write_all(octet_counted.len().to_string().as_bytes())
        .await
        .expect("failed to write Syslog octet count");
    stream
        .write_all(b" ")
        .await
        .expect("failed to write Syslog octet-count delimiter");
    stream
        .write_all(octet_counted)
        .await
        .expect("failed to write octet-counted Syslog message");
    stream
        .write_all(messages[1].as_bytes())
        .await
        .expect("failed to write non-transparent Syslog message");
    stream
        .write_all(b"\r\n")
        .await
        .expect("failed to terminate non-transparent Syslog message");
    stream
        .shutdown()
        .await
        .expect("failed to close Syslog TCP test sender");
}

#[when(
    expr = "Syslog TLS message is published to {string} using identity and CA from resource \
            directory {string}"
)]
async fn when_syslog_tls_message_is_published_to(
    world: &mut ScenarioWorld,
    addr: String,
    ca_resource_directory: String,
    #[step] step: &Step,
) {
    use tokio::io::AsyncWriteExt as _;

    nervix_interconnect::install_rustls_crypto_provider();
    let addr = expand_placeholders(world, &addr);
    let parsed = url::Url::parse(&format!("syslog://{addr}"))
        .unwrap_or_else(|error| panic!("invalid Syslog TLS test address '{addr}': {error}"));
    let server_name = parsed
        .host_str()
        .expect("Syslog TLS test address must have a host")
        .to_string();
    let ca_pem = resource_directory_ca_pem(world, &ca_resource_directory);
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots
            .add(certificate.expect("Syslog TLS test CA must contain a valid certificate"))
            .expect("Syslog TLS test CA certificate must be accepted");
    }
    let identity_dir = resource_directory_path(world, &ca_resource_directory);
    let certificate_pem = std::fs::read(identity_dir.join("tls.crt"))
        .expect("Syslog TLS test client certificate must be readable");
    let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
        .collect::<Result<Vec<_>, _>>()
        .expect("Syslog TLS test client certificate must be valid PEM");
    let key_pem = std::fs::read(identity_dir.join("tls.key"))
        .expect("Syslog TLS test client key must be readable");
    let key = PrivateKeyDer::from_pem_slice(&key_pem)
        .expect("Syslog TLS test client key must be valid PEM");
    let config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certificates, key)
        .expect("Syslog TLS test client identity must be valid");
    let connector = tokio_rustls::TlsConnector::from(StdArc::new(config));
    let deadline = Instant::now() + Duration::from_secs(5);
    let stream = loop {
        match tokio::net::TcpStream::connect(&addr).await {
            Ok(stream) => break stream,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out connecting to Syslog TLS listener '{addr}': {error}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };
    let server_name =
        ServerName::try_from(server_name).expect("Syslog TLS test server name must be valid");
    let mut stream = connector
        .connect(server_name, stream)
        .await
        .expect("failed to establish Syslog TLS test session");
    let payload = expand_placeholders(world, docstring(step));
    let payload = payload.trim().as_bytes();
    stream
        .write_all(format!("{} ", payload.len()).as_bytes())
        .await
        .expect("failed to write Syslog TLS octet count");
    stream
        .write_all(payload)
        .await
        .expect("failed to write Syslog TLS payload");
    stream
        .shutdown()
        .await
        .expect("failed to close Syslog TLS test sender");
}

#[when(expr = "websocket message is published to host {string} path {string}")]
async fn when_websocket_message_is_published(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_websocket("node-1", &host, &path, &payload)
        .await
        .expect("failed to publish websocket message");
}

#[when("the websocket client test server sends a payload")]
async fn when_websocket_client_test_server_sends_a_payload(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let base = world
        .dependencies
        .endpoints()
        .get(MOCK_HTTP_ADDR)
        .expect("HTTP mock server endpoint must be available");
    let mut url = url::Url::parse(base).expect("HTTP mock server endpoint must be a valid URL");
    url.set_path(&format!("/ws/{}", world.test_id));
    let payload = expand_placeholders(world, docstring(step));
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        tokio::task::consume_budget().await;
        match client.post(url.clone()).body(payload.clone()).send().await {
            Ok(response) if response.status().is_success() => {
                world.last_server_error = None;
                return;
            }
            Ok(response) => {
                world.last_server_error = Some(format!(
                    "websocket client test server returned {}",
                    response.status()
                ))
            }
            Err(error) => world.last_server_error = Some(error.to_string()),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for an outbound WebSocket client connection. last error: {:?}",
            world.last_server_error
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[when(expr = "websocket frames are exchanged with host {string} path {string}")]
async fn when_websocket_frames_are_exchanged(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let actions = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            if line == "EXPECT CLOSE" {
                WebsocketExchangeAction::ExpectClose
            } else if let Some(window) = line.strip_prefix("EXPECT SILENCE ") {
                WebsocketExchangeAction::ExpectSilence(
                    humantime::parse_duration(window.trim()).unwrap_or_else(|error| {
                        panic!("invalid silence window '{window}': {error}")
                    }),
                )
            } else if let Some(payload) = line.strip_prefix("EXPECT BASE64 ") {
                WebsocketExchangeAction::ExpectBinary(decode_base64_frame(payload))
            } else if let Some(payload) = line.strip_prefix("SEND BASE64 ") {
                WebsocketExchangeAction::SendBinary(decode_base64_frame(payload))
            } else if let Some(payload) = line.strip_prefix("EXPECT ") {
                WebsocketExchangeAction::ExpectText(expand_placeholders(world, payload))
            } else if let Some(payload) = line.strip_prefix("SEND ") {
                WebsocketExchangeAction::SendText(expand_placeholders(world, payload))
            } else {
                panic!(
                    "websocket exchange lines must start with EXPECT, SEND, EXPECT BASE64, SEND \
                     BASE64, or be EXPECT CLOSE: {line}"
                );
            }
        })
        .collect::<Vec<_>>();
    world
        .cluster()
        .exchange_websocket("node-1", &host, &path, &actions)
        .await
        .expect("failed to exchange websocket frames");
}

fn decode_base64_frame(payload: &str) -> Vec<u8> {
    BASE64_STANDARD
        .decode(payload.trim())
        .unwrap_or_else(|error| panic!("invalid base64 websocket frame '{payload}': {error}"))
}

#[when(expr = "websocket message is published to host {string} path {string} and fails")]
async fn when_websocket_message_is_published_and_fails(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let result = world
        .cluster()
        .publish_websocket("node-1", &host, &path, &payload)
        .await;
    assert!(result.is_err(), "expected websocket publish to fail");
}

#[when(
    expr = "secure websocket message is published to host {string} path {string} using CA from \
            resource directory {string}"
)]
async fn when_secure_websocket_message_is_published(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    ca_resource_directory: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let ca_pem = resource_directory_ca_pem(world, &ca_resource_directory);
    world
        .cluster()
        .publish_secure_websocket("node-1", &host, &path, &payload, &ca_pem)
        .await
        .expect("failed to publish secure websocket message");
}

#[when(expr = "websocket message is published to node {string} host {string} path {string}")]
async fn when_websocket_message_is_published_to_node(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .publish_websocket(&node_id, &host, &path, &payload)
        .await
        .expect("failed to publish websocket message");
}

#[when(
    expr = "websocket message is published to node {string} host {string} path {string} and fails"
)]
async fn when_websocket_message_is_published_to_node_and_fails(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let result = world
        .cluster()
        .publish_websocket(&node_id, &host, &path, &payload)
        .await;
    assert!(
        result.is_err(),
        "expected websocket publish to node '{node_id}' to fail"
    );
}

#[when(expr = "JAQ native payload fixture {string} is posted to host {string} path {string}")]
async fn when_jaq_native_payload_fixture_is_posted(
    world: &mut ScenarioWorld,
    fixture: String,
    host: String,
    path: String,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let (payload, content_type) = jaq_native_payload_fixture(&fixture);
    append_cucumber_log_line(&format!(
        "http publish: node=node-1 host={host} path={path} fixture={fixture} \
         content_type={content_type}"
    ));
    world
        .cluster()
        .publish_http_bytes("node-1", &host, &path, &payload, content_type)
        .await
        .expect("failed to post JAQ native payload fixture");
}

#[when(expr = "protobuf payload fixture {string} is posted to host {string} path {string}")]
async fn when_protobuf_payload_fixture_is_posted(
    world: &mut ScenarioWorld,
    fixture: String,
    host: String,
    path: String,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let (payload, content_type) = protobuf_payload_fixture(&fixture);
    append_cucumber_log_line(&format!(
        "http publish: node=node-1 host={host} path={path} fixture={fixture} \
         content_type={content_type}"
    ));
    world
        .cluster()
        .publish_http_bytes("node-1", &host, &path, &payload, content_type)
        .await
        .expect("failed to post protobuf payload fixture");
}

#[when(expr = "http payload is posted to host {string} path {string}")]
async fn when_http_payload_is_posted(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    append_cucumber_log_line(&format!(
        "http publish: node=node-1 host={host} path={path} payload={payload}"
    ));
    world.last_publish_at = Some(Instant::now());
    world
        .cluster()
        .publish_http("node-1", &host, &path, &payload)
        .await
        .expect("failed to post http payload");
}

#[when(
    expr = "http payload with value {int} and a {int} byte message is posted to host {string} \
            path {string}"
)]
async fn when_large_http_payload_is_posted(
    world: &mut ScenarioWorld,
    value: i64,
    message_size: usize,
    host: String,
    path: String,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = serde_json::json!({
        "value": value,
        "message": "x".repeat(message_size),
    })
    .to_string();
    append_cucumber_log_line(&format!(
        "http publish large payload: node=node-1 host={host} path={path} value={value} \
         message_size={message_size}"
    ));
    world
        .cluster()
        .publish_http("node-1", &host, &path, &payload)
        .await
        .expect("failed to post large http payload");
}

#[when(
    expr = "http payload for tenant {string} with a {int} byte message is posted to host {string} \
            path {string}"
)]
async fn when_large_tenant_http_payload_is_posted(
    world: &mut ScenarioWorld,
    tenant: String,
    message_size: usize,
    host: String,
    path: String,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = serde_json::json!({
        "tenant": tenant,
        "message": "x".repeat(message_size),
    })
    .to_string();
    append_cucumber_log_line(&format!(
        "http publish large tenant payload: node=node-1 host={host} path={path} \
         message_size={message_size}"
    ));
    world
        .cluster()
        .publish_http("node-1", &host, &path, &payload)
        .await
        .expect("failed to post large tenant http payload");
}

#[when(expr = "http payloads are posted concurrently to host {string} path {string}")]
async fn when_http_payloads_are_posted_concurrently(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payloads = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !payloads.is_empty(),
        "concurrent http publish step must include at least one payload line"
    );
    append_cucumber_log_line(&format!(
        "http publish concurrent: node=node-1 host={host} path={path} payloads={payloads:?}"
    ));
    let cluster = world.cluster();
    try_join_all(
        payloads
            .iter()
            .map(|payload| cluster.publish_http("node-1", &host, &path, payload)),
    )
    .await
    .expect("failed to post concurrent http payloads");
}

#[when(expr = "{int} sequential metric http payloads are posted to host {string} path {string}")]
async fn when_sequential_metric_http_payloads_are_posted(
    world: &mut ScenarioWorld,
    count: u64,
    host: String,
    path: String,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    append_cucumber_log_line(&format!(
        "http publish sequential metrics: node=node-1 host={host} path={path} count={count}"
    ));
    for value in 1..=count {
        tokio::task::consume_budget().await;
        let payload = format!(r#"{{"value":{value}}}"#);
        world
            .cluster()
            .publish_http("node-1", &host, &path, &payload)
            .await
            .unwrap_or_else(|error| panic!("failed to post http payload {value}: {error}"));
    }
}

#[when(expr = "http payload encoded as {string} is posted to host {string} path {string}")]
async fn when_encoded_http_payload_is_posted(
    world: &mut ScenarioWorld,
    wire_format: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let encoded_payload = encode_http_payload_for_codec(
        &wire_format,
        &payload,
        &world.avro_http_field_order,
        &world.avro_http_optional_fields,
    );
    append_cucumber_log_line(&format!(
        "http publish: node=node-1 host={host} path={path} wire_format={wire_format} \
         payload={payload}"
    ));
    world
        .cluster()
        .publish_http_bytes(
            "node-1",
            &host,
            &path,
            &encoded_payload,
            http_content_type_for_codec(&wire_format),
        )
        .await
        .expect("failed to post encoded http payload");
}

#[when(expr = "http payload is posted to host {string} path {string} and fails")]
async fn when_http_payload_is_posted_and_fails(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    append_cucumber_log_line(&format!(
        "http publish expect-fail: node=node-1 host={host} path={path} payload={payload}"
    ));
    let result = world
        .cluster()
        .publish_http("node-1", &host, &path, &payload)
        .await;
    assert!(result.is_err(), "expected http post to fail");
}

#[when(expr = "http payload is posted to host {string} path {string} and is not routed")]
async fn when_http_payload_is_posted_and_is_not_routed(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    append_cucumber_log_line(&format!(
        "http publish expect-unrouted: node=node-1 host={host} path={path} payload={payload}"
    ));
    let error = world
        .cluster()
        .publish_http("node-1", &host, &path, &payload)
        .await
        .expect_err("expected http post to be unrouted");
    let reported = error.to_string();
    assert!(
        reported.contains("404"),
        "expected http post to be unrouted, got: {reported}"
    );
}

#[when(
    expr = "https payload is posted to host {string} path {string} using CA from resource \
            directory {string}"
)]
async fn when_https_payload_is_posted(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    ca_resource_directory: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let ca_pem = resource_directory_ca_pem(world, &ca_resource_directory);
    append_cucumber_log_line(&format!(
        "https publish: node=node-1 host={host} path={path} payload={payload}"
    ));
    world
        .cluster()
        .publish_https("node-1", &host, &path, &payload, &ca_pem)
        .await
        .expect("failed to post https payload");
}

#[then(
    expr = "the leader HTTPS listener for host {string} presents the certificate from resource \
            directory {string}"
)]
async fn then_leader_https_listener_presents_resource_certificate(
    world: &mut ScenarioWorld,
    host: String,
    resource_directory: String,
) {
    let host = expand_placeholders(world, &host);
    let ca_pem = resource_directory_ca_pem(world, &resource_directory);
    let leader = current_leader_node(world).await;
    world
        .cluster()
        .connect_https(&leader, &host, &ca_pem)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "leader '{leader}' did not present the certificate trusted by resource directory \
                 '{resource_directory}' for host '{host}': {error}"
            )
        });
}

/// Connects once to every node: a command that changed the listener's certificate has already
/// waited for every listener to install it, so the check neither polls nor retries.
#[then(
    expr = "the HTTPS listener of every node for host {string} presents the certificate from \
            resource directory {string}"
)]
async fn then_every_https_listener_presents_resource_certificate(
    world: &mut ScenarioWorld,
    host: String,
    resource_directory: String,
) {
    let host = expand_placeholders(world, &host);
    let ca_pem = resource_directory_ca_pem(world, &resource_directory);
    for node_id in world.cluster().node_ids() {
        world
            .cluster()
            .connect_https(&node_id, &host, &ca_pem)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "node '{node_id}' did not present the certificate trusted by resource \
                     directory '{resource_directory}' for host '{host}': {error}"
                )
            });
    }
}

#[when(
    expr = "https payloads begin posting in the background to every node with host {string} path \
            {string} trusting resource directories {string} and {string}"
)]
async fn when_https_payloads_begin_posting_in_the_background(
    world: &mut ScenarioWorld,
    host: String,
    path: String,
    first_resource_directory: String,
    second_resource_directory: String,
    #[step] step: &Step,
) {
    assert!(
        world.background_https_publish.is_none(),
        "a background https publish is already active"
    );
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let trusted_ca_pems = [
        resource_directory_ca_pem(world, &first_resource_directory),
        resource_directory_ca_pem(world, &second_resource_directory),
    ];
    append_cucumber_log_line(&format!(
        "https background publish: every node host={host} path={path} payload={payload}"
    ));
    let stop = CancellationToken::new();
    let task = world
        .cluster()
        .spawn_https_publish_loop(host, path, payload, &trusted_ca_pems, stop.clone())
        .expect("failed to prepare the background https publish");
    world.background_https_publish = Some(BackgroundHttpsPublish {
        stop,
        task: AbortOnDropHandle::new(task),
    });
}

#[then("the background https publishing accepted every payload")]
async fn then_the_background_https_publishing_accepted_every_payload(world: &mut ScenarioWorld) {
    let BackgroundHttpsPublish { stop, task } = world
        .background_https_publish
        .take()
        .expect("a background https publish must be active");
    stop.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .expect("background https publish did not stop")
        .expect("background https publish task failed");
    let node_count = world.cluster().node_ids().len();
    assert!(
        outcome.accepted >= node_count,
        "expected every node to accept at least one background https payload, got {} accepted",
        outcome.accepted
    );
    assert!(
        outcome.failures.is_empty(),
        "background https publishing was rejected {} time(s) after {} accepted payload(s): {:?}",
        outcome.failures.len(),
        outcome.accepted,
        outcome.failures
    );
}

#[when(expr = "http payload is posted to node {string} with host {string} path {string}")]
async fn when_http_payload_is_posted_to_node(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    append_cucumber_log_line(&format!(
        "http publish: node={node_id} host={host} path={path} payload={payload}"
    ));
    world.last_publish_at = Some(Instant::now());
    world
        .cluster()
        .publish_http(&node_id, &host, &path, &payload)
        .await
        .expect("failed to post http payload");
}

#[when(
    expr = "http payload begins posting in the background to node {string} with host {string} \
            path {string}"
)]
async fn when_http_payload_begins_posting_in_the_background(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    assert!(
        world.background_http_publish.is_none(),
        "a background http publish is already active"
    );
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let task = world
        .cluster()
        .spawn_http_publish(&node_id, host, path, payload);
    world.background_http_publish = Some(AbortOnDropHandle::new(task));
}

#[then("the background http publish succeeds")]
async fn then_the_background_http_publish_succeeds(world: &mut ScenarioWorld) {
    let task = world
        .background_http_publish
        .take()
        .expect("a background http publish must be active");
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("background http publish did not finish")
        .expect("background http publish task failed")
        .expect("background http publish failed");
}

#[when(
    expr = "http payload is posted to node {string} with host {string} path {string} and header \
            {string} value {string}"
)]
async fn when_http_payload_is_posted_to_node_with_header(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    header_name: String,
    header_value: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let header_name = expand_placeholders(world, &header_name);
    let header_value = expand_placeholders(world, &header_value);
    append_cucumber_log_line(&format!(
        "http publish: node={node_id} host={host} path={path} header={header_name} \
         payload={payload}"
    ));
    world.last_publish_at = Some(Instant::now());
    world
        .cluster()
        .publish_http_with_headers(
            &node_id,
            &host,
            &path,
            &payload,
            &[(header_name.as_str(), header_value.as_str())],
        )
        .await
        .expect("failed to post http payload with header");
}

#[when(expr = "http payload is posted to node {string} with host {string} path {string} and fails")]
async fn when_http_payload_is_posted_to_node_and_fails(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        tokio::task::consume_budget().await;
        let result = world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await;
        if result.is_err() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected http post to node '{node_id}' to fail"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[when(
    expr = "http payload is posted to node {string} with host {string} path {string} and is not \
            routed"
)]
async fn when_http_payload_is_posted_to_node_and_is_not_routed(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let error = world
        .cluster()
        .publish_http(&node_id, &host, &path, &payload)
        .await
        .expect_err("expected http post to be unrouted");
    let reported = error.to_string();
    assert!(
        reported.contains("404"),
        "expected http post to node '{node_id}' to be unrouted, got: {reported}"
    );
}

#[then("the relay subscription receives a payload")]
async fn then_stream_subscription_receives_payload(world: &mut ScenarioWorld, #[step] step: &Step) {
    append_cucumber_log_line(&format!(
        "awaiting subscription payload containing {}",
        docstring(step).replace('\n', "\\n")
    ));
    capture_and_assert_subscription_payload(world, docstring(step), false, Duration::from_secs(10))
        .await;
}

#[then(expr = "within {string} the relay subscription receives a payload")]
async fn then_within_stream_subscription_receives_payload(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    append_cucumber_log_line(&format!(
        "awaiting subscription payload within {:?} containing {}",
        duration,
        docstring(step).replace('\n', "\\n")
    ));
    capture_and_assert_subscription_payload(world, docstring(step), false, duration).await;
}

#[then(expr = "within {string} client {string} receives a subscription payload")]
async fn then_named_client_receives_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    client_name: String,
    #[step] step: &Step,
) {
    let duration = humantime::parse_duration(&duration)
        .assured("the scenario subscription deadline is a valid duration");
    let client_name = expand_placeholders(world, &client_name);
    let expected = expand_placeholders(world, docstring(step));
    let client = world
        .transaction_clients
        .get(&client_name)
        .unwrap_or_else(|| panic!("client '{client_name}' must be connected"))
        .clone();
    let deadline = Instant::now() + duration;
    let payload = loop {
        tokio::task::consume_budget().await;
        let pending = world
            .client_subscription_rows
            .entry(client_name.clone())
            .or_default();
        if let Some(payload) = pending.pop_front() {
            break payload;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = tokio::time::timeout(remaining, client.next_subscription())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "client '{client_name}' did not receive a subscription payload within \
                     {duration:?}"
                )
            })
            .unwrap_or_else(|error| {
                panic!("client '{client_name}' subscription stream closed: {error}")
            });
        let nervix_client_core::SubscriptionEvent::Rows(rows) = event else {
            continue;
        };
        let lines = rows
            .display_lines()
            .unwrap_or_else(|error| panic!("client '{client_name}' rows do not render: {error}"));
        pending.extend(lines);
    };
    assert!(
        payload_matches_expected(&payload, &expected),
        "client '{client_name}' expected subscription payload {expected:?}, got {payload:?}"
    );
    world.last_subscription_payload = Some(payload);
}

#[when(
    expr = "within {string} client {string} receives a subscription payload from repeated http \
            posts to node {string} with host {string} path {string}"
)]
async fn when_named_client_receives_from_repeated_http_posts(
    world: &mut ScenarioWorld,
    duration: String,
    client_name: String,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration = humantime::parse_duration(&duration)
        .assured("the scenario delivery deadline is a valid duration");
    let client_name = expand_placeholders(world, &client_name);
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let client = world
        .transaction_clients
        .get(&client_name)
        .unwrap_or_else(|| panic!("client '{client_name}' must be connected"))
        .clone();
    let deadline = Instant::now() + duration;
    loop {
        tokio::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "client '{client_name}' did not receive a row from repeated posts within {duration:?}"
        );
        world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await
            .unwrap_or_else(|error| panic!("failed to post http payload: {error}"));
        let remaining = deadline.saturating_duration_since(Instant::now());
        let wait = remaining.min(Duration::from_secs(1));
        match tokio::time::timeout(wait, client.next_subscription()).await {
            Ok(Ok(nervix_client_core::SubscriptionEvent::Rows(rows))) => {
                let lines = rows.display_lines().unwrap_or_else(|error| {
                    panic!("client '{client_name}' rows do not render: {error}")
                });
                if let Some(line) = lines.into_iter().next() {
                    world.last_subscription_payload = Some(line);
                    return;
                }
            }
            Ok(Ok(_)) | Err(_) => {}
            Ok(Err(error)) => panic!("client '{client_name}' subscription stream closed: {error}"),
        }
    }
}

#[then(expr = "within {string} client {string} observes subscription {string} interrupted")]
async fn then_named_client_observes_subscription_interrupted(
    world: &mut ScenarioWorld,
    duration: String,
    client_name: String,
    subscription_name: String,
) {
    let duration = humantime::parse_duration(&duration)
        .assured("the interruption deadline is a valid duration");
    let client_name = expand_placeholders(world, &client_name);
    let subscription_name = expand_placeholders(world, &subscription_name);
    let client = world
        .transaction_clients
        .get(&client_name)
        .unwrap_or_else(|| panic!("client '{client_name}' must be connected"))
        .clone();
    let event = tokio::time::timeout(duration, client.next_subscription())
        .await
        .unwrap_or_else(|_| panic!("client '{client_name}' did not report an interruption"))
        .unwrap_or_else(|error| panic!("client '{client_name}' event failed: {error}"));
    let nervix_client_core::SubscriptionEvent::Interrupted(interrupted) = event else {
        panic!("client '{client_name}' did not report an interruption: {event:?}");
    };
    assert_eq!(interrupted.subscription.name.as_str(), subscription_name);
}

#[then(expr = "client {string} subscription {string} is active")]
async fn then_named_client_subscription_is_active(
    world: &mut ScenarioWorld,
    client_name: String,
    subscription_name: String,
) {
    let client_name = expand_placeholders(world, &client_name);
    let subscription_name = expand_placeholders(world, &subscription_name);
    let client = world
        .transaction_clients
        .get(&client_name)
        .unwrap_or_else(|| panic!("client '{client_name}' must be connected"));
    let name = nervix_models::SubscriptionName::parse(&subscription_name)
        .assured("the scenario subscription name is valid");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::task::consume_budget().await;
        let lifecycle = client.subscription_lifecycle(&name);
        if let Some(nervix_client_core::SubscriptionLifecycle::Active(_)) = lifecycle {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "client '{client_name}' subscription '{subscription_name}' did not become active: \
             {lifecycle:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "node {string} eventually accepts websocket traffic for host {string} path {string}")]
async fn then_node_eventually_accepts_websocket_traffic(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        tokio::task::consume_budget().await;
        match world
            .cluster()
            .publish_websocket(&node_id, &host, &path, &payload)
            .await
        {
            Ok(()) => return,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for websocket endpoint on node '{node_id}': {error}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[then(expr = "node {string} eventually accepts http traffic for host {string} path {string}")]
async fn then_node_eventually_accepts_http_traffic(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        tokio::task::consume_budget().await;
        match world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await
        {
            Ok(()) => return,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for http endpoint on node '{node_id}': {error}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[then(
    expr = "within {string} repeatedly posting http payload to host {string} path {string} yields \
            a relay subscription payload"
)]
async fn then_within_duration_repeatedly_posting_http_payload_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let _ = world
            .cluster()
            .publish_http("node-1", &host, &path, &payload)
            .await;

        if try_capture_any_subscription_payload(world, Duration::from_millis(350)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for http payload posted to host '{host}' path '{path}' to reach \
             the relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly posting https payload to host {string} path {string} using \
            CA from resource directory {string} yields a relay subscription payload"
)]
async fn then_within_duration_repeatedly_posting_https_payload_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    host: String,
    path: String,
    ca_resource_directory: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let ca_pem = resource_directory_ca_pem(world, &ca_resource_directory);
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let publish = world
            .cluster()
            .publish_https("node-1", &host, &path, &payload, &ca_pem)
            .await;

        if try_capture_any_subscription_payload(world, Duration::from_millis(350)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for https payload posted to host '{host}' path '{path}' with the \
             CA from '{ca_resource_directory}' to reach the relay subscription; last publish: \
             {publish:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly posting http payload encoded as {string} to host {string} \
            path {string} yields a relay subscription payload"
)]
async fn then_within_duration_repeatedly_posting_encoded_http_payload_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    wire_format: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let encoded_payload = encode_http_payload_for_codec(
        &wire_format,
        &payload,
        &world.avro_http_field_order,
        &world.avro_http_optional_fields,
    );
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let _ = world
            .cluster()
            .publish_http_bytes(
                "node-1",
                &host,
                &path,
                &encoded_payload,
                http_content_type_for_codec(&wire_format),
            )
            .await;

        if try_capture_any_subscription_payload(world, Duration::from_millis(350)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {wire_format} payload posted to host '{host}' path '{path}' to \
             reach the relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing Kafka message to topic {string} yields a relay \
            subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_kafka_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    topic: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_kafka(&topic, &payload)
            .await
            .expect("failed to publish kafka message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Kafka message published to topic '{topic}' to reach the relay \
             subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing Kafka message to topic {string} partition {int} \
            yields a relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_kafka_message_to_partition_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    topic: String,
    partition: usize,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let topic = expand_placeholders(world, &topic);
    let partition =
        i32::try_from(partition).assured("Kafka partition ids in cucumber features fit i32");
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_kafka_partition(&topic, partition, &payload)
            .await
            .expect("failed to publish kafka message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Kafka message published to topic '{topic}' partition \
             '{partition}' to reach the relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing MQTT message to topic {string} yields a relay \
            subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_mqtt_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    topic: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_mqtt(&topic, &payload)
            .await
            .expect("failed to publish mqtt message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for MQTT message published to topic '{topic}' to reach the relay \
             subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing Pulsar TLS message to topic {string} yields a \
            relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_pulsar_tls_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    topic: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let topic = expand_placeholders(world, &topic);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_pulsar_tls(&topic, &payload)
            .await
            .expect("failed to publish pulsar tls message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Pulsar TLS message published to topic '{topic}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing Redis message to channel {string} yields a \
            relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_redis_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    channel: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let channel = expand_placeholders(world, &channel);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_redis(&channel, &payload)
            .await
            .expect("failed to publish redis message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Redis message published to channel '{channel}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing NATS message to subject {string} yields a relay \
            subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_nats_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    subject: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let subject = expand_placeholders(world, &subject);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_nats(&subject, &payload)
            .await
            .expect("failed to publish nats message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for NATS message published to subject '{subject}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing NATS TLS message to subject {string} yields a \
            relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_nats_tls_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    subject: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let subject = expand_placeholders(world, &subject);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_nats_tls(&subject, &payload)
            .await
            .expect("failed to publish nats tls message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for NATS TLS message published to subject '{subject}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing SQS message to queue {string} yields a relay \
            subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_sqs_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    queue: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_sqs(&queue, &payload)
            .await
            .expect("failed to publish sqs message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for SQS message published to queue '{queue}' to reach the relay \
             subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "within {string} repeatedly publishing TLS SQS message to queue {string} yields a \
            relay subscription payload"
)]
async fn then_within_duration_repeatedly_publishing_tls_sqs_message_yields_subscription_payload(
    world: &mut ScenarioWorld,
    duration: String,
    queue: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let queue = expand_placeholders(world, &queue);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        world
            .cluster()
            .publish_sqs_tls(&queue, &payload)
            .await
            .expect("failed to publish tls sqs message");

        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for TLS SQS message published to queue '{queue}' to reach the \
             relay subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(
    expr = "node {string} eventually forwards websocket traffic for host {string} path {string} \
            to the observed broker"
)]
async fn then_node_eventually_forwards_websocket_traffic_to_observed_broker(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(15);

    loop {
        let _ = world
            .cluster()
            .publish_websocket(&node_id, &host, &path, &payload)
            .await;
        let next_payload = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_payload(Duration::from_millis(250))
            .await;
        match next_payload {
            Ok(Some(observed)) if observed.contains(payload.trim()) => {
                world.last_broker_payload = Some(observed);
                return;
            }
            Ok(_) | Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for node '{node_id}' websocket traffic to reach observed \
                     broker"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[then(
    expr = "node {string} eventually forwards http traffic for host {string} path {string} to the \
            observed broker"
)]
async fn then_node_eventually_forwards_http_traffic_to_observed_broker(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + Duration::from_secs(15);

    loop {
        let _ = world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await;
        let next_payload = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_payload(Duration::from_millis(250))
            .await;
        match next_payload {
            Ok(Some(observed)) if observed.contains(payload.trim()) => {
                world.last_broker_payload = Some(observed);
                return;
            }
            Ok(_) | Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for node '{node_id}' http traffic to reach observed broker"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[then(
    expr = "within {string} repeatedly posting http payload to node {string} with host {string} \
            path {string} yields an observed broker payload"
)]
async fn then_within_duration_repeatedly_posting_http_payload_yields_observed_broker_payload(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        let _ = world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await;
        let next_payload = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_payload(Duration::from_millis(250))
            .await;
        if let Ok(Some(observed)) = next_payload
            && payload_matches_expected(&observed, &payload)
        {
            world.last_broker_payload = Some(observed);
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for http payload posted to node '{node_id}' host '{host}' path \
             '{path}' to reach the observed broker"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "within {string} the relay subscription receives payloads")]
async fn then_within_duration_the_stream_subscription_receives_payloads(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected_fragments = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| expand_placeholders(world, line))
        .collect::<Vec<_>>();

    assert!(
        !expected_fragments.is_empty(),
        "step docstring must contain at least one expected payload fragment"
    );

    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + duration;
    let mut remaining = expected_fragments
        .iter()
        .fold(BTreeMap::new(), |mut counts, fragment| {
            *counts.entry(fragment.clone()).or_insert(0usize) += 1;
            counts
        });
    let mut observed = Vec::with_capacity(expected_fragments.len());

    while !remaining.is_empty() {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payloads. expected remaining {:?}, observed {:?}",
            remaining,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let event = session
            .try_next_subscription(wait)
            .await
            .expect("failed while waiting for subscription payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for subscription payloads. expected remaining {:?}, \
                     observed {:?}",
                    remaining, observed
                )
            });
        let payload = event.payload;
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload.clone());

        if let Some(fragment) = remaining
            .keys()
            .find(|fragment| payload.contains(fragment.as_str()))
            .cloned()
        {
            let count = remaining
                .get_mut(&fragment)
                .expect("matched fragment must be present in remaining set");
            *count -= 1;
            if *count == 0 {
                remaining.remove(&fragment);
            }
        }
    }
}

#[then(expr = "within {string} {int} relay subscription payloads share field {string}")]
async fn then_relay_subscription_payloads_share_field(
    world: &mut ScenarioWorld,
    duration: String,
    expected_count: usize,
    field: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + duration;
    let mut shared_values = BTreeSet::new();
    let mut observed = Vec::with_capacity(expected_count);

    while observed.len() < expected_count {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out after receiving {} of {expected_count} subscription payloads; observed \
             {observed:?}",
            observed.len()
        );
        let event = session
            .try_next_subscription(deadline.saturating_duration_since(now))
            .await
            .expect("failed while waiting for subscription payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out after receiving {} of {expected_count} subscription payloads; \
                     observed {observed:?}",
                    observed.len()
                )
            });
        let payload = event.payload;
        let value = serde_json::from_str::<serde_json::Value>(&payload)
            .unwrap_or_else(|error| panic!("subscription payload is not valid JSON: {error}"));
        let shared_value = value
            .get(&field)
            .unwrap_or_else(|| panic!("subscription payload {value} has no field '{field}'"))
            .to_string();
        shared_values.insert(shared_value);
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload);
    }

    assert_eq!(
        shared_values.len(),
        1,
        "expected {expected_count} subscription payloads to share field '{field}', got \
         {shared_values:?} from {observed:?}"
    );
}

#[then(
    expr = "within {string} {int} generator occurrences preserve branches {string} in field \
            {string} with shared timestamp field {string}"
)]
async fn then_generator_occurrences_preserve_branches(
    world: &mut ScenarioWorld,
    duration: String,
    expected_occurrences: usize,
    branches: String,
    branch_field: String,
    timestamp_field: String,
) {
    let duration = match humantime::parse_duration(&duration) {
        Ok(duration) => duration,
        Err(error) => panic!("step duration must be valid: {error}"),
    };
    let branches = expand_placeholders(world, &branches);
    let expected_branches = branches
        .split(',')
        .map(str::trim)
        .filter(|branch| !branch.is_empty())
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    assert!(
        expected_branches.len() >= 2,
        "generator cadence step requires at least two distinct branches"
    );
    let branch_field = expand_placeholders(world, &branch_field);
    let timestamp_field = expand_placeholders(world, &timestamp_field);
    let session = world
        .active_session
        .as_mut()
        .assured("an active session with subscription must exist");
    let deadline = Instant::now()
        .checked_add(duration)
        .assured("scenario durations fit Tokio's monotonic instant range");
    let mut branches_by_timestamp = BTreeMap::<_, BTreeSet<String>>::new();
    let mut latest_timestamp = None;
    let mut observed = Vec::new();

    loop {
        tokio::task::consume_budget().await;
        let complete_occurrences = branches_by_timestamp
            .values()
            .filter(|branches| *branches == &expected_branches)
            .count();
        if complete_occurrences >= expected_occurrences {
            assert_eq!(
                complete_occurrences, expected_occurrences,
                "generator produced more complete occurrences than the assertion consumed"
            );
            return;
        }
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out after observing {complete_occurrences} of {expected_occurrences} complete \
             generator occurrences for branches {expected_branches:?}: {observed:?}"
        );
        let remaining = deadline
            .checked_duration_since(now)
            .verified("the deadline comparison above established remaining scenario time");
        let event = match session.try_next_subscription(remaining).await {
            Ok(Some(event)) => event,
            Ok(None) => panic!(
                "timed out after observing {complete_occurrences} of {expected_occurrences} \
                 complete generator occurrences for branches {expected_branches:?}: {observed:?}"
            ),
            Err(error) => panic!("failed while waiting for subscription payloads: {error}"),
        };
        let payload = event.payload;
        let (key, payload_json) = if let Some((key, payload_json)) = payload.split_once(" payload=")
        {
            (key.strip_prefix("key="), payload_json)
        } else {
            (None, payload.as_str())
        };
        let parsed = match serde_json::from_str::<serde_json::Value>(payload_json) {
            Ok(parsed) => parsed,
            Err(error) => panic!("subscription payload is not valid JSON: {error}"),
        };
        let Some(branch) = parsed.get(&branch_field) else {
            panic!("subscription payload has no field '{branch_field}': {payload}");
        };
        let Some(branch) = branch.as_str() else {
            panic!("subscription payload field '{branch_field}' is not a string: {payload}");
        };
        assert!(
            expected_branches.contains(branch),
            "generator produced unexpected branch '{branch}': {payload}"
        );
        if let Some(key) = key {
            let key = match serde_json::from_str::<serde_json::Value>(key) {
                Ok(key) => key,
                Err(error) => panic!("subscription key is not valid JSON: {error}"),
            };
            assert_eq!(
                key.get(&branch_field),
                parsed.get(&branch_field),
                "generator output did not preserve branch field '{branch_field}': {payload}"
            );
        }
        let Some(timestamp) = parsed.get(&timestamp_field) else {
            panic!("subscription payload has no field '{timestamp_field}': {payload}");
        };
        let Some(timestamp) = timestamp.as_str() else {
            panic!("subscription payload field '{timestamp_field}' is not a string: {payload}");
        };
        let timestamp = match chrono::DateTime::parse_from_rfc3339(timestamp) {
            Ok(timestamp) => timestamp,
            Err(error) => {
                panic!("subscription payload field '{timestamp_field}' is not RFC 3339: {error}")
            }
        };
        let timestamp = timestamp
            .timestamp_nanos_opt()
            .assured("generator scenario timestamps fit signed Unix nanoseconds");
        if !branches_by_timestamp.contains_key(&timestamp) {
            if let Some(latest_timestamp) = latest_timestamp.as_ref() {
                assert!(
                    &timestamp > latest_timestamp,
                    "generator occurrence timestamps arrived out of order: {observed:?}, {payload}"
                );
            }
            latest_timestamp = Some(timestamp);
        }
        branches_by_timestamp
            .entry(timestamp)
            .or_default()
            .insert(branch.to_string());
        world.last_subscription_payload = Some(payload.clone());
        observed.push(payload);
    }
}

#[then(
    expr = "within {string} generated routes {string} value {int} and {string} value {int} share \
            field {string}"
)]
async fn then_generated_routes_share_field(
    world: &mut ScenarioWorld,
    duration: String,
    first_route: String,
    first_value: i64,
    second_route: String,
    second_value: i64,
    shared_field: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + duration;
    let mut routes_by_shared_value = BTreeMap::<String, BTreeSet<String>>::new();
    let mut observed = Vec::new();

    loop {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for generated routes '{first_route}' and '{second_route}' to share \
             field '{shared_field}'. observed {observed:?}"
        );
        let event = session
            .try_next_subscription(deadline.saturating_duration_since(now))
            .await
            .expect("failed while waiting for generated route payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for generated routes '{first_route}' and '{second_route}' \
                     to share field '{shared_field}'. observed {observed:?}"
                )
            });
        let payload = event.payload;
        world.last_subscription_payload = Some(payload.clone());
        observed.push(payload.clone());
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&payload) else {
            continue;
        };
        let Some(route) = value.get("route").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(route_value) = value.get("value").and_then(serde_json::Value::as_i64) else {
            continue;
        };
        let expected_value = if route == first_route {
            first_value
        } else if route == second_route {
            second_value
        } else {
            continue;
        };
        if route_value != expected_value {
            continue;
        }
        let Some(shared_value) = value.get(&shared_field) else {
            continue;
        };
        let shared_value = shared_value.to_string();
        let routes = routes_by_shared_value.entry(shared_value).or_default();
        routes.insert(route.to_string());
        if routes.contains(&first_route) && routes.contains(&second_route) {
            return;
        }
    }
}

#[then(expr = "within {string} the relay subscription receives payloads in order")]
async fn then_within_duration_the_stream_subscription_receives_payloads_in_order(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected_fragments = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| expand_placeholders(world, line))
        .collect::<Vec<_>>();

    assert!(
        !expected_fragments.is_empty(),
        "step docstring must contain at least one expected payload fragment"
    );

    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + duration;
    let mut observed = Vec::with_capacity(expected_fragments.len());

    for expected_fragment in &expected_fragments {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payload fragment {:?}. observed {:?}",
            expected_fragment,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let event = session
            .try_next_subscription(wait)
            .await
            .expect("failed while waiting for subscription payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for subscription payload fragment {:?}. observed {:?}",
                    expected_fragment, observed
                )
            });
        let payload = event.payload;
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload.clone());
        assert!(
            payload.contains(expected_fragment),
            "expected next subscription payload to contain {:?}, got {:?}; observed {:?}",
            expected_fragment,
            payload,
            observed
        );
    }
}

#[then(expr = "within {string} the relay subscription receives payloads containing all fragments")]
async fn then_within_duration_the_stream_subscription_receives_payloads_containing_all_fragments(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    receive_subscription_fragment_sets(world, &duration, step).await;
}

/// Like the step above, and every payload matching a fragment set carries the same value in the
/// named JSON field, such as the one error reference the members of a failed batch share.
#[then(
    expr = "within {string} the relay subscription receives payloads containing all fragments \
            that share one {string}"
)]
async fn then_within_duration_the_stream_subscription_receives_fragments_sharing_one_field(
    world: &mut ScenarioWorld,
    duration: String,
    field: String,
    #[step] step: &Step,
) {
    let matched = receive_subscription_fragment_sets(world, &duration, step).await;
    let values = matched
        .iter()
        .map(|payload| {
            let payload: serde_json::Value =
                serde_json::from_str(payload).unwrap_or_else(|error| {
                    panic!("subscription payload {payload:?} is not JSON: {error}")
                });
            payload
                .get(&field)
                .cloned()
                .unwrap_or_else(|| panic!("subscription payload {payload} has no field {field:?}"))
        })
        .collect::<Vec<_>>();
    let Some(first) = values.first() else {
        panic!("no subscription payload matched the expected fragment sets");
    };
    assert!(
        values.iter().all(|value| value == first),
        "payloads matching the fragment sets carry different {field:?} values: {values:?}"
    );
}

/// Waits until every docstring line's `|`-separated fragments are all found in one subscription
/// payload, and returns the payloads that matched, in the order they arrived.
async fn receive_subscription_fragment_sets(
    world: &mut ScenarioWorld,
    duration: &str,
    step: &Step,
) -> Vec<String> {
    let duration =
        humantime::parse_duration(duration).expect("step duration must be a valid duration");
    let expected_fragment_sets = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.split('|')
                .map(str::trim)
                .filter(|fragment| !fragment.is_empty())
                .map(|fragment| expand_placeholders(world, fragment))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert!(
        !expected_fragment_sets.is_empty(),
        "step docstring must contain at least one expected payload fragment set"
    );

    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + duration;
    let mut remaining = expected_fragment_sets;
    let mut observed = Vec::new();
    let mut matched = Vec::new();

    while !remaining.is_empty() {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payload fragment sets. expected remaining {:?}, \
             observed {:?}",
            remaining,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let event = session
            .try_next_subscription(wait)
            .await
            .expect("failed while waiting for subscription payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for subscription payload fragment sets. expected remaining \
                     {:?}, observed {:?}",
                    remaining, observed
                )
            });
        let payload = event.payload;
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload.clone());

        if let Some(index) = remaining
            .iter()
            .position(|fragments| fragments.iter().all(|fragment| payload.contains(fragment)))
        {
            remaining.remove(index);
            matched.push(payload);
        }
    }
    matched
}

#[then("the relay subscription does not receive a payload")]
async fn then_stream_subscription_does_not_receive_a_payload(world: &mut ScenarioWorld) {
    assert_no_subscription_payload_within(world, Duration::from_secs(3)).await;
}

#[then(expr = "the relay subscription does not receive a payload within {string}")]
async fn then_stream_subscription_does_not_receive_a_payload_within(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    assert_no_subscription_payload_within(world, duration).await;
}

/// Assert that nothing the subscription delivers within `duration` carries every named fragment.
///
/// Sibling branches keep publishing while the branch under test must stay silent, so the window
/// has to be drained rather than closed on the first payload that arrives.
#[then(
    expr = "the relay subscription does not receive a payload containing fragments within {string}"
)]
async fn then_stream_subscription_does_not_receive_a_payload_containing_fragments_within(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let fragments = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| expand_placeholders(world, line))
        .collect::<Vec<_>>();
    assert!(
        !fragments.is_empty(),
        "step docstring must contain at least one forbidden payload fragment"
    );
    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let deadline = Instant::now() + duration;

    loop {
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        let wait = deadline.saturating_duration_since(now);
        let event = session
            .try_next_subscription(wait)
            .await
            .expect("failed while waiting for absence of a subscription payload");
        let Some(event) = event else {
            return;
        };
        assert!(
            !fragments
                .iter()
                .all(|fragment| event.payload.contains(fragment)),
            "expected no subscription payload containing {fragments:?}, got: {}",
            event.payload
        );
    }
}

async fn assert_no_subscription_payload_within(world: &mut ScenarioWorld, duration: Duration) {
    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    let event = session
        .try_next_subscription(duration)
        .await
        .expect("failed while waiting for absence of subscription payload");

    assert!(
        event.is_none(),
        "expected no subscription payload, got: {:?}",
        event.map(|value| value.payload)
    );
}

#[then("the relay subscription receives a payload with topic key")]
async fn then_stream_subscription_receives_payload_with_topic_key(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    capture_and_assert_subscription_payload(world, docstring(step), true, Duration::from_secs(10))
        .await;
}

#[then(expr = "the last relay subscription payload contains key fragment {string}")]
async fn then_last_stream_subscription_payload_contains_key_fragment(
    world: &mut ScenarioWorld,
    expected_key_fragment: String,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");

    assert!(
        payload.contains(&format!("key={expected_key_fragment}")),
        "expected key fragment {:?} in payload, got: {payload}",
        expected_key_fragment
    );
}

#[then(expr = "the last relay subscription payload contains {string}")]
async fn then_last_stream_subscription_payload_contains(
    world: &mut ScenarioWorld,
    expected_fragment: String,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");

    assert!(
        payload.contains(&expected_fragment),
        "expected fragment {:?} in payload, got: {payload}",
        expected_fragment
    );
}

#[then(
    expr = "the last relay subscription payload field {string} is saved as timestamp placeholder \
            {string}"
)]
async fn then_subscription_timestamp_field_is_saved(
    world: &mut ScenarioWorld,
    field: String,
    placeholder: String,
) {
    let field = expand_placeholders(world, &field);
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before saving a timestamp field");
    let parsed = serde_json::from_str::<serde_json::Value>(payload)
        .unwrap_or_else(|error| panic!("subscription payload is not valid JSON: {error}"));
    let Some(value) = parsed.get(&field).and_then(serde_json::Value::as_str) else {
        panic!("subscription payload has no string field '{field}': {payload}");
    };
    if chrono::DateTime::parse_from_rfc3339(value).is_err() {
        panic!("subscription payload field '{field}' is not an RFC 3339 timestamp: {value}");
    }
    world.placeholders.insert(placeholder, value.to_string());
}

#[given(expr = "a repeated text placeholder {string} of {int} bytes is prepared")]
async fn given_repeated_text_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
    bytes: usize,
) {
    world.placeholders.insert(placeholder, "a".repeat(bytes));
}

#[then(expr = "timestamp placeholder {string} is not before timestamp placeholder {string}")]
async fn then_timestamp_placeholder_is_not_before(
    world: &mut ScenarioWorld,
    later_placeholder: String,
    earlier_placeholder: String,
) {
    let later = world
        .placeholders
        .get(&later_placeholder)
        .unwrap_or_else(|| panic!("timestamp placeholder '{later_placeholder}' is not defined"));
    let earlier = world
        .placeholders
        .get(&earlier_placeholder)
        .unwrap_or_else(|| panic!("timestamp placeholder '{earlier_placeholder}' is not defined"));
    let Ok(later_timestamp) = chrono::DateTime::parse_from_rfc3339(later) else {
        panic!("timestamp placeholder '{later_placeholder}' is not RFC 3339: {later}");
    };
    let Ok(earlier_timestamp) = chrono::DateTime::parse_from_rfc3339(earlier) else {
        panic!("timestamp placeholder '{earlier_placeholder}' is not RFC 3339: {earlier}");
    };

    assert!(
        later_timestamp >= earlier_timestamp,
        "timestamp moved backwards from {earlier_timestamp} to {later_timestamp}"
    );
}

#[then(expr = "timestamp placeholder {string} equals timestamp placeholder {string}")]
async fn then_timestamp_placeholder_equals(
    world: &mut ScenarioWorld,
    actual_placeholder: String,
    expected_placeholder: String,
) {
    let actual = world
        .placeholders
        .get(&actual_placeholder)
        .unwrap_or_else(|| panic!("timestamp placeholder '{actual_placeholder}' is not defined"));
    let expected = world
        .placeholders
        .get(&expected_placeholder)
        .unwrap_or_else(|| panic!("timestamp placeholder '{expected_placeholder}' is not defined"));
    let Ok(actual_timestamp) = chrono::DateTime::parse_from_rfc3339(actual) else {
        panic!("timestamp placeholder '{actual_placeholder}' is not RFC 3339: {actual}");
    };
    let Ok(expected_timestamp) = chrono::DateTime::parse_from_rfc3339(expected) else {
        panic!("timestamp placeholder '{expected_placeholder}' is not RFC 3339: {expected}");
    };

    assert_eq!(
        actual_timestamp, expected_timestamp,
        "timestamp placeholder '{actual_placeholder}' was {actual_timestamp}, expected the same \
         instant as '{expected_placeholder}' ({expected_timestamp})"
    );
}

#[then(expr = "timestamp placeholders {string} and {string} differ by no more than {string}")]
async fn then_timestamp_placeholders_differ_by_no_more_than(
    world: &mut ScenarioWorld,
    first_placeholder: String,
    second_placeholder: String,
    maximum_difference: String,
) {
    let first = world
        .placeholders
        .get(&first_placeholder)
        .unwrap_or_else(|| panic!("timestamp placeholder '{first_placeholder}' is not defined"));
    let second = world
        .placeholders
        .get(&second_placeholder)
        .unwrap_or_else(|| panic!("timestamp placeholder '{second_placeholder}' is not defined"));
    let first = chrono::DateTime::parse_from_rfc3339(first).unwrap_or_else(|error| {
        panic!("timestamp placeholder '{first_placeholder}' is invalid: {error}")
    });
    let second = chrono::DateTime::parse_from_rfc3339(second).unwrap_or_else(|error| {
        panic!("timestamp placeholder '{second_placeholder}' is invalid: {error}")
    });
    let first_nanos = first
        .timestamp_nanos_opt()
        .unwrap_or_else(|| panic!("timestamp placeholder '{first_placeholder}' is out of range"));
    let second_nanos = second
        .timestamp_nanos_opt()
        .unwrap_or_else(|| panic!("timestamp placeholder '{second_placeholder}' is out of range"));
    let measured_nanos = first_nanos.abs_diff(second_nanos);
    let measured = Duration::from_nanos(measured_nanos);
    let maximum = humantime::parse_duration(&maximum_difference)
        .unwrap_or_else(|error| panic!("maximum timestamp difference is invalid: {error}"));

    append_cucumber_log_line(&format!(
        "measured cross-node logical-clock separation between '{first_placeholder}' and \
         '{second_placeholder}': {} from sequential node-local samples; this is not a \
         simultaneous clock-equality measurement",
        humantime::format_duration(measured)
    ));
    assert!(
        measured <= maximum,
        "timestamp placeholders '{first_placeholder}' and '{second_placeholder}' differed by {}, \
         expected no more than {}",
        humantime::format_duration(measured),
        humantime::format_duration(maximum)
    );
}

#[then(expr = "timestamp placeholder {string} is before {string}")]
async fn then_timestamp_placeholder_is_before(
    world: &mut ScenarioWorld,
    placeholder: String,
    upper_bound: String,
) {
    let value = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("timestamp placeholder '{placeholder}' is not defined"));
    let value = chrono::DateTime::parse_from_rfc3339(value).unwrap_or_else(|error| {
        panic!("timestamp placeholder '{placeholder}' is invalid: {error}")
    });
    let upper_bound = chrono::DateTime::parse_from_rfc3339(&upper_bound)
        .unwrap_or_else(|error| panic!("timestamp upper bound is invalid: {error}"));

    assert!(
        value < upper_bound,
        "timestamp placeholder '{placeholder}' was {value}, expected a value before {upper_bound}"
    );
}

#[then(expr = "timestamp placeholder {string} is not before {string}")]
async fn then_timestamp_placeholder_is_not_before_fixed_time(
    world: &mut ScenarioWorld,
    placeholder: String,
    lower_bound: String,
) {
    let value = world
        .placeholders
        .get(&placeholder)
        .unwrap_or_else(|| panic!("timestamp placeholder '{placeholder}' is not defined"));
    let value = chrono::DateTime::parse_from_rfc3339(value).unwrap_or_else(|error| {
        panic!("timestamp placeholder '{placeholder}' is invalid: {error}")
    });
    let lower_bound = chrono::DateTime::parse_from_rfc3339(&lower_bound)
        .unwrap_or_else(|error| panic!("timestamp lower bound is invalid: {error}"));
    assert!(
        value >= lower_bound,
        "timestamp placeholder '{placeholder}' was {value}, expected a value at or after \
         {lower_bound}"
    );
}

#[then(expr = "the last relay subscription payload masks field {string}")]
async fn then_last_stream_subscription_payload_masks_field(
    world: &mut ScenarioWorld,
    field: String,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");
    let expected_fragment = format!("\"{field}\":\"<masked>\"");

    assert!(
        payload.contains(&expected_fragment),
        "expected masked field fragment {:?} in payload, got: {payload}",
        expected_fragment
    );
}

#[then("the last relay subscription payload contains")]
async fn then_last_stream_subscription_payload_contains_docstring(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");

    for expected_fragment in docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        assert!(
            payload.contains(expected_fragment),
            "expected fragment {:?} in payload, got: {payload}",
            expected_fragment
        );
    }
}

#[then(expr = "the last relay subscription payload does not contain {string}")]
async fn then_last_stream_subscription_payload_does_not_contain(
    world: &mut ScenarioWorld,
    unexpected_fragment: String,
) {
    let payload = world
        .last_subscription_payload
        .as_deref()
        .expect("subscription payload must be captured before assertion");

    assert!(
        !payload.contains(&unexpected_fragment),
        "did not expect fragment {:?} in payload, got: {payload}",
        unexpected_fragment
    );
}

#[then(expr = "within {string} the active session observes a server error")]
async fn then_within_duration_the_active_session_observes_a_server_error(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let session = world
        .active_session
        .as_mut()
        .expect("an active session must exist");
    let event = session
        .try_next_server_error(duration)
        .await
        .expect("failed while waiting for server error")
        .unwrap_or_else(|| panic!("timed out waiting for a server error within {:?}", duration));
    append_cucumber_log_line(&format!(
        "observed runtime server error level={:?} message={}",
        event.level, event.message
    ));
    world.last_server_error = Some(event.message);
}

/// Waits for the server error that carries the docstring, passing over the errors reported before
/// it. A failing guest may report further errors from later callbacks on the same instance, so the
/// next error is not necessarily the one the scenario expects.
#[then(expr = "within {string} the active session observes a server error containing")]
async fn then_within_duration_the_active_session_observes_a_server_error_containing(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected = expand_placeholders(world, docstring(step).trim());
    let session = world
        .active_session
        .as_mut()
        .expect("an active session must exist");
    let deadline = Instant::now() + duration;
    let mut passed_over = Vec::new();
    loop {
        tokio::task::consume_budget().await;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = session
            .try_next_server_error(remaining)
            .await
            .expect("failed while waiting for server error");
        let Some(event) = event else {
            panic!(
                "timed out waiting for a server error containing {expected:?} within \
                 {duration:?}; passed over: {passed_over:?}"
            );
        };
        append_cucumber_log_line(&format!(
            "observed runtime server error level={:?} message={}",
            event.level, event.message
        ));
        if event.message.contains(&expected) {
            world.last_server_error = Some(event.message);
            return;
        }
        passed_over.push(event.message);
    }
}

#[then("the last server error contains")]
async fn then_last_server_error_contains(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step).trim());
    let error = world
        .last_server_error
        .as_deref()
        .expect("server error must be captured before assertion");
    assert!(
        error.contains(&expected),
        "expected server error to contain {expected:?}, got: {error}"
    );
}

#[then("the last server error does not contain")]
async fn then_last_server_error_does_not_contain(world: &mut ScenarioWorld, #[step] step: &Step) {
    let unexpected = expand_placeholders(world, docstring(step).trim());
    let error = world
        .last_server_error
        .as_deref()
        .expect("server error must be captured before assertion");
    assert!(
        !error.contains(&unexpected),
        "expected server error not to contain {unexpected:?}, got: {error}"
    );
}

#[then("the observed broker receives a payload")]
async fn then_observed_broker_receives_payload(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected_payload = expand_placeholders(world, docstring(step));
    let observer = world
        .broker_observer
        .as_mut()
        .expect("a broker observer must exist before assertion");
    let message = observer
        .next_message()
        .await
        .expect("failed to receive broker payload");
    world.last_broker_payload = Some(message.payload);
    world.last_broker_headers = message.headers;

    let actual = world
        .last_broker_payload
        .as_deref()
        .expect("broker payload must be captured before assertion");
    assert!(
        payload_matches_expected(actual, &expected_payload),
        "expected payload fragment {} in broker payload, got: {actual}",
        expected_payload.trim()
    );
}

#[then("the observed Syslog UDP endpoint receives a payload")]
async fn then_observed_syslog_udp_endpoint_receives_payload(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let observer = world
        .syslog_udp_observer
        .as_ref()
        .expect("a Syslog UDP observer must exist before assertion");
    let mut payload = vec![0_u8; 65_535];
    let (len, _) = tokio::time::timeout(Duration::from_secs(10), observer.recv_from(&mut payload))
        .await
        .expect("timed out waiting for Syslog UDP payload")
        .expect("failed to receive Syslog UDP payload");
    let actual = std::str::from_utf8(&payload[..len])
        .expect("emitted Syslog UDP payload must be valid UTF-8");
    assert!(
        payload_matches_expected(actual, &expected),
        "expected Syslog UDP payload fragment {}, got: {actual}",
        expected.trim()
    );
}

#[then("the last observed broker message has headers")]
async fn then_last_observed_broker_message_has_headers(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected_headers = expand_placeholders(world, docstring(step));
    for line in expected_headers
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let (name, value) = line.split_once('=').unwrap_or_else(|| {
            panic!("expected broker header assertion '{line}' to use name=value")
        });
        assert!(
            world
                .last_broker_headers
                .iter()
                .any(|(actual_name, actual_value)| actual_name == name && actual_value == value),
            "expected broker header {name}={value}, got {:?}",
            world.last_broker_headers
        );
    }
}

#[then("Sentry eventually receives an event")]
async fn then_sentry_eventually_receives_event(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected =
        serde_json::from_str::<serde_json::Value>(&expand_placeholders(world, docstring(step)))
            .expect("expected Sentry event must be valid JSON");
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        tokio::task::consume_budget().await;
        if let Some(event) = world
            .dependencies
            .sentry_event(&world.test_id)
            .await
            .expect("Sentry event query must succeed")
        {
            for (field, expected_value) in expected
                .as_object()
                .expect("expected Sentry event must be a JSON object")
            {
                assert_eq!(
                    event.get(field),
                    Some(expected_value),
                    "Sentry event field {field:?} did not match; full event: {event}"
                );
            }
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Sentry to receive {expected}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[then(expr = "the Sentry event timestamp is before {string}")]
async fn then_sentry_event_timestamp_is_before(world: &mut ScenarioWorld, expected: String) {
    let event = world
        .dependencies
        .sentry_event(&world.test_id)
        .await
        .expect("Sentry event query must succeed")
        .expect("the preceding Sentry assertion must have observed an event");
    let timestamp = event
        .get("timestamp")
        .and_then(serde_json::Value::as_str)
        .expect("Sentry event timestamp must be an RFC 3339 string");
    let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp)
        .expect("Sentry event timestamp must parse as RFC 3339");
    let expected = chrono::DateTime::parse_from_rfc3339(&expected)
        .expect("expected Sentry timestamp boundary must parse as RFC 3339");

    assert!(
        timestamp < expected,
        "Sentry event timestamp {timestamp} was not before {expected}"
    );
}

#[then(expr = "Quickwit index {string} eventually contains {string}")]
async fn then_quickwit_index_eventually_contains(
    world: &mut ScenarioWorld,
    index: String,
    expected: String,
) {
    let index = expand_placeholders(world, &index);
    let expected = expand_placeholders(world, &expected);
    let deadline = Instant::now() + Duration::from_secs(45);

    loop {
        tokio::task::consume_budget().await;
        if world
            .dependencies
            .quickwit_index_contains(&index, &expected)
            .await
            .expect("Quickwit search must succeed")
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for Quickwit index {index:?} to contain {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[then(expr = "OpenTelemetry Collector eventually contains {string}")]
async fn then_otel_collector_eventually_contains(world: &mut ScenarioWorld, expected: String) {
    let expected = expand_placeholders(world, &expected);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        if world
            .dependencies
            .otel_collector_contains(&expected)
            .await
            .expect("OpenTelemetry Collector logs must be readable")
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for OpenTelemetry Collector to contain {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the last observed broker payload contains")]
async fn then_last_observed_broker_payload_contains(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected_payload = expand_placeholders(world, docstring(step));
    let payload = world
        .last_broker_payload
        .as_deref()
        .expect("broker payload must be captured before assertion");

    for expected_fragment in expected_payload
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        assert!(
            payload.contains(expected_fragment),
            "expected fragment {expected_fragment:?} in broker payload, got: {payload}"
        );
    }
}

#[then("the ClickHouse table eventually contains a row")]
async fn then_clickhouse_table_eventually_contains_row(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let table = world
        .clickhouse_table
        .as_ref()
        .expect("a ClickHouse table must be prepared before assertion")
        .clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let query =
            format!("SELECT clickhouse_user_id, clickhouse_action FROM {table} FORMAT JSONEachRow");
        let payload = clickhouse_post_for_world(world, &query)
            .await
            .expect("failed to query ClickHouse table");
        let observed = payload.lines().map(str::to_string).collect::<Vec<_>>();
        if observed
            .iter()
            .any(|row| payload_matches_expected(row, &expected))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for ClickHouse row. expected {expected}, observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the ClickHouse table eventually contains {int} rows in at least {int} parts")]
async fn then_clickhouse_table_eventually_contains_rows_in_parts(
    world: &mut ScenarioWorld,
    expected_rows: u64,
    expected_parts: u64,
) {
    let table = world
        .clickhouse_table
        .as_ref()
        .expect("a ClickHouse table must be prepared before assertion")
        .clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let query = format!("SELECT count(), uniqExact(_part) FROM {table} FORMAT TabSeparatedRaw");
        let payload = clickhouse_post_for_world(world, &query)
            .await
            .expect("failed to query ClickHouse table parts");
        let mut values = payload.trim().split('\t');
        let observed_rows = values
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default();
        let observed_parts = values
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default();
        if observed_rows == expected_rows && observed_parts >= expected_parts {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected_rows} ClickHouse rows in at least {expected_parts} \
             parts; observed rows={observed_rows} parts={observed_parts}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The connections one Nervix client holds on the database, counted server-side.
///
/// `application_name` is what distinguishes them from the harness's own connections and from any
/// other application, which is how an operator checks a declared budget too.
async fn postgres_application_connections(world: &ScenarioWorld, application: &str) -> i64 {
    let client = postgres_client(world.dependencies.endpoints(), world.postgres_tls)
        .await
        .expect("failed to connect to Postgres");
    sqlx::query("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1")
        .bind(application)
        .fetch_one(&client)
        .await
        .expect("failed to count Postgres connections")
        .get(0)
}

#[then(expr = "Postgres never reports more than {int} connections for application {string}")]
async fn then_postgres_connections_stay_within(
    world: &mut ScenarioWorld,
    maximum: i64,
    application: String,
) {
    let application = expand_placeholders(world, &application);
    // Sampled over a window rather than once: a single reading could miss a pool that briefly
    // opened more connections than it declared.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed_peak = 0;
    while Instant::now() < deadline {
        let observed = postgres_application_connections(world, &application).await;
        observed_peak = observed_peak.max(observed);
        assert!(
            observed <= maximum,
            "expected at most {maximum} Postgres connections for {application}, observed \
             {observed}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        observed_peak > 0,
        "expected {application} to hold at least one Postgres connection, observed none"
    );
}

#[then(expr = "Postgres eventually reports at least {int} connections for application {string}")]
async fn then_postgres_eventually_reports_at_least_connections(
    world: &mut ScenarioWorld,
    expected: i64,
    application: String,
) {
    let application = expand_placeholders(world, &application);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let observed = postgres_application_connections(world, &application).await;
        if observed >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for at least {expected} Postgres connections for {application}; \
             observed {observed}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[then(expr = "Postgres eventually reports {int} connections for application {string}")]
async fn then_postgres_eventually_reports_connections(
    world: &mut ScenarioWorld,
    expected: i64,
    application: String,
) {
    let application = expand_placeholders(world, &application);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let observed = postgres_application_connections(world, &application).await;
        if observed == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected} Postgres connections for {application}; observed \
             {observed}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[given(expr = "the Postgres table is locked against inserts")]
async fn given_postgres_table_is_locked(world: &mut ScenarioWorld) {
    let table = world
        .postgres_table
        .as_ref()
        .expect("a Postgres table must be prepared before locking it")
        .clone();
    let client = postgres_client(world.dependencies.endpoints(), world.postgres_tls)
        .await
        .expect("failed to connect to Postgres");
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut transaction = client
            .begin()
            .await
            .expect("failed to open the Postgres locking transaction");
        sqlx::raw_sql(SqlxAssertSqlSafe(format!(
            "LOCK TABLE {table} IN EXCLUSIVE MODE"
        )))
        .execute(&mut *transaction)
        .await
        .expect("failed to lock the Postgres table");
        locked_tx
            .send(())
            .expect("the locking step must still be waiting for its lock");
        // Held until the scenario releases it, which is what keeps one pooled connection busy.
        // A dropped sender means the scenario ended without releasing, and the lock goes with it.
        release_rx
            .await
            .discarded("a scenario that ends without releasing drops the lock anyway");
        transaction
            .commit()
            .await
            .expect("failed to release the Postgres table lock");
    });
    locked_rx
        .await
        .expect("the Postgres locking task must acquire its lock");
    world.postgres_lock_release = Some(release_tx);
}

#[when("the Postgres table lock is released")]
async fn when_postgres_table_lock_is_released(world: &mut ScenarioWorld) {
    let release = world
        .postgres_lock_release
        .take()
        .expect("a Postgres table lock must be held before releasing it");
    release
        .send(())
        .expect("the Postgres locking task must still be holding its lock");
}

#[then("the Postgres table eventually contains a row")]
async fn then_postgres_table_eventually_contains_row(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let table = world
        .postgres_table
        .as_ref()
        .expect("a Postgres table must be prepared before assertion")
        .clone();
    let client = postgres_client(world.dependencies.endpoints(), world.postgres_tls)
        .await
        .expect("failed to connect to Postgres");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = sqlx::query(SqlxAssertSqlSafe(format!(
            "SELECT postgres_user_id, postgres_action FROM {table}"
        )))
        .fetch_all(&client)
        .await
        .expect("failed to query Postgres table");
        let observed = rows
            .iter()
            .map(|row| {
                let user_id: i32 = row.get(0);
                let action: String = row.get(1);
                serde_json::json!({
                    "postgres_user_id": user_id,
                    "postgres_action": action,
                })
                .to_string()
            })
            .collect::<Vec<_>>();
        if observed
            .iter()
            .any(|row| payload_matches_expected(row, &expected))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for Postgres row. expected {expected}, observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the Postgres table eventually contains exactly {int} rows")]
async fn then_postgres_table_eventually_contains_exactly_rows(
    world: &mut ScenarioWorld,
    expected_rows: i64,
) {
    let table = world
        .postgres_table
        .as_ref()
        .expect("a Postgres table must be prepared before assertion")
        .clone();
    let client = postgres_client(world.dependencies.endpoints(), world.postgres_tls)
        .await
        .expect("failed to connect to Postgres");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observed_rows: i64 =
            sqlx::query(SqlxAssertSqlSafe(format!("SELECT count(*) FROM {table}")))
                .fetch_one(&client)
                .await
                .expect("failed to count Postgres rows")
                .get(0);
        if observed_rows == expected_rows {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for exactly {expected_rows} Postgres rows; observed {observed_rows}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the Postgres table contains exactly one row for each of these user ids")]
async fn then_postgres_table_contains_exactly_one_row_for_each_user_id(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected_ids = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.parse::<i32>()
                .unwrap_or_else(|error| panic!("invalid Postgres user id {line:?}: {error}"))
        })
        .collect::<BTreeSet<_>>();
    assert!(
        !expected_ids.is_empty(),
        "the Postgres user id assertion requires at least one id"
    );
    let table = world
        .postgres_table
        .as_ref()
        .expect("a Postgres table must be prepared before assertion")
        .clone();
    let client = postgres_client(world.dependencies.endpoints(), world.postgres_tls)
        .await
        .expect("failed to connect to Postgres");
    let rows = sqlx::query(SqlxAssertSqlSafe(format!(
        "SELECT postgres_user_id, count(*) FROM {table} GROUP BY postgres_user_id"
    )))
    .fetch_all(&client)
    .await
    .expect("failed to count Postgres rows by user id");
    let observed_counts = rows
        .iter()
        .map(|row| {
            let user_id: i32 = row.get(0);
            let count: i64 = row.get(1);
            (user_id, count)
        })
        .collect::<BTreeMap<_, _>>();
    for user_id in expected_ids {
        let observed = observed_counts.get(&user_id).copied().unwrap_or(0);
        assert_eq!(
            observed, 1,
            "expected exactly one Postgres row for user id {user_id}; observed counts: \
             {observed_counts:?}"
        );
    }
}

#[then(
    expr = "the Postgres table eventually contains {int} rows across at least {int} inserts of at \
            most {int} rows"
)]
async fn then_postgres_table_eventually_contains_rows_across_bounded_inserts(
    world: &mut ScenarioWorld,
    expected_rows: i64,
    expected_inserts: i64,
    maximum_rows_per_insert: i64,
) {
    let table = world
        .postgres_table
        .as_ref()
        .expect("a Postgres table must be prepared before assertion")
        .clone();
    let audit_table = format!("{table}_insert_audit");
    let client = postgres_client(world.dependencies.endpoints(), world.postgres_tls)
        .await
        .expect("failed to connect to Postgres");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let row = sqlx::query(SqlxAssertSqlSafe(format!(
            "SELECT (SELECT count(*) FROM {table}),
                    (SELECT count(*) FROM {audit_table}),
                    COALESCE((SELECT max(row_count) FROM {audit_table}), 0)"
        )))
        .fetch_one(&client)
        .await
        .expect("failed to query Postgres insert statement recorder");
        let observed_rows: i64 = row.get(0);
        let observed_inserts: i64 = row.get(1);
        let largest_insert: i64 = row.get(2);
        if observed_rows == expected_rows
            && observed_inserts >= expected_inserts
            && largest_insert <= maximum_rows_per_insert
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected_rows} Postgres rows across at least \
             {expected_inserts} inserts of at most {maximum_rows_per_insert} rows; observed \
             rows={observed_rows} inserts={observed_inserts} largest_insert={largest_insert}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the MySQL table eventually contains a row")]
async fn then_mysql_table_eventually_contains_row(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step));
    let table = world
        .mysql_table
        .as_ref()
        .expect("a MySQL table must be prepared before assertion")
        .clone();
    let pool = mysql_pool(world.dependencies.endpoints(), world.mysql_tls)
        .expect("failed to build MySQL pool");
    let mut conn = pool.get_conn().await.expect("failed to connect to MySQL");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observed = conn
            .query_map(
                format!("SELECT mysql_user_id, mysql_action FROM `{table}`"),
                |(user_id, action): (i32, String)| {
                    serde_json::json!({
                        "mysql_user_id": user_id,
                        "mysql_action": action,
                    })
                    .to_string()
                },
            )
            .await
            .expect("failed to query MySQL table");
        if observed
            .iter()
            .any(|row| payload_matches_expected(row, &expected))
        {
            drop(conn);
            pool.disconnect()
                .await
                .expect("failed to disconnect MySQL pool");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for MySQL row. expected {expected}, observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the MySQL table eventually contains {int} rows from at least {int} insert commands")]
async fn then_mysql_table_eventually_contains_rows_from_insert_commands(
    world: &mut ScenarioWorld,
    expected_rows: u64,
    expected_commands: u64,
) {
    let table = world
        .mysql_table
        .as_ref()
        .expect("a MySQL table must be prepared before assertion")
        .clone();
    let baseline = world
        .mysql_insert_command_baseline
        .expect("MySQL insert command recording must be enabled before assertion");
    let insert_prefix = format!("INSERT INTO `{table}`%");
    let pool =
        mysql_root_pool(world.dependencies.endpoints()).expect("failed to build MySQL root pool");
    let mut conn = pool
        .get_conn()
        .await
        .expect("failed to connect to MySQL as root");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let observed_rows = conn
            .query_first::<u64, _>(format!("SELECT COUNT(*) FROM `{table}`"))
            .await
            .expect("failed to count MySQL rows")
            .unwrap_or(0);
        let recorded_commands = conn
            .exec_first::<u64, _, _>(
                "SELECT COUNT(*) FROM mysql.general_log
                 WHERE command_type IN ('Execute', 'Query') AND argument LIKE ?",
                (&insert_prefix,),
            )
            .await
            .expect("failed to count MySQL insert commands")
            .unwrap_or(0)
            .checked_sub(baseline)
            .expect("the MySQL command log only grows while a scenario runs");
        if observed_rows == expected_rows && recorded_commands >= expected_commands {
            drop(conn);
            pool.disconnect()
                .await
                .expect("failed to disconnect MySQL root pool");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected_rows} MySQL rows from at least {expected_commands} \
             insert commands; observed rows={observed_rows} commands={recorded_commands}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The identifier one MongoDB document carries, keeping a written BSON null distinct from a number
/// so a scenario can assert that a genuine null was published as null.
fn mongodb_document_user_id(document: &MongoDbDocument) -> serde_json::Value {
    match document.get("mongodb_user_id") {
        Some(MongoDbBson::Int32(value)) => serde_json::json!(i64::from(*value)),
        Some(MongoDbBson::Int64(value)) => serde_json::json!(*value),
        Some(MongoDbBson::Double(value)) => {
            let value: i64 = (*value).checked_approx_into().unwrap_or_default();
            serde_json::json!(value)
        }
        Some(MongoDbBson::Null) => serde_json::Value::Null,
        _ => serde_json::json!(0),
    }
}

#[then("the MongoDB collection eventually contains a document")]
async fn then_mongodb_collection_eventually_contains_document(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let collection = world
        .mongodb_collection
        .as_ref()
        .expect("a MongoDB collection must be prepared before assertion")
        .clone();
    let client = mongodb_client(world.dependencies.endpoints(), world.mongodb_tls)
        .await
        .expect("failed to connect to MongoDB");
    let collection = client
        .database("nervix")
        .collection::<MongoDbDocument>(&collection);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let cursor = collection
            .find(mongodb_doc! {})
            .await
            .expect("failed to query MongoDB collection");
        let documents = cursor
            .try_collect::<Vec<_>>()
            .await
            .expect("failed to read MongoDB documents");
        let observed = documents
            .into_iter()
            .map(|document| {
                let user_id = mongodb_document_user_id(&document);
                let action = document.get_str("mongodb_action").unwrap_or_default();
                serde_json::json!({
                    "mongodb_user_id": user_id,
                    "mongodb_action": action,
                })
                .to_string()
            })
            .collect::<Vec<_>>();
        if observed
            .iter()
            .any(|row| payload_matches_expected(row, &expected))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for MongoDB document. expected {expected}, observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the MongoDB collection eventually contains exactly {int} documents")]
async fn then_mongodb_collection_eventually_contains_exactly_documents(
    world: &mut ScenarioWorld,
    expected_documents: u64,
) {
    let collection = world
        .mongodb_collection
        .as_ref()
        .expect("a MongoDB collection must be prepared before assertion")
        .clone();
    let client = mongodb_client(world.dependencies.endpoints(), world.mongodb_tls)
        .await
        .expect("failed to connect to MongoDB");
    let collection = client
        .database("nervix")
        .collection::<MongoDbDocument>(&collection);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observed_documents = collection
            .count_documents(mongodb_doc! {})
            .await
            .expect("failed to count MongoDB documents");
        if observed_documents == expected_documents {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for exactly {expected_documents} MongoDB documents; observed \
             {observed_documents}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(
    expr = "the MongoDB collection eventually contains {int} documents across at least {int} \
            inserts of at most {int} documents"
)]
async fn then_mongodb_collection_eventually_contains_documents_across_bounded_inserts(
    world: &mut ScenarioWorld,
    expected_documents: u64,
    expected_inserts: usize,
    maximum_documents_per_insert: usize,
) {
    let collection_name = world
        .mongodb_collection
        .as_ref()
        .expect("a MongoDB collection must be prepared before assertion")
        .clone();
    let client = mongodb_client(world.dependencies.endpoints(), world.mongodb_tls)
        .await
        .expect("failed to connect to MongoDB");
    let database = client.database("nervix");
    let collection = database.collection::<MongoDbDocument>(&collection_name);
    let profile = database.collection::<MongoDbDocument>("system.profile");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::task::consume_budget().await;
        let observed_documents = collection
            .count_documents(mongodb_doc! {})
            .await
            .expect("failed to count MongoDB documents");
        let profile_documents = profile
            .find(mongodb_doc! { "command.insert": &collection_name })
            .await
            .expect("failed to query MongoDB insert command profiles")
            .try_collect::<Vec<_>>()
            .await
            .expect("failed to read MongoDB insert command profiles");
        let mut command_sizes = Vec::with_capacity(profile_documents.len());
        for profile_document in &profile_documents {
            let size = match profile_document.get("ninserted") {
                Some(MongoDbBson::Int32(value)) => usize::try_from(*value).ok(),
                Some(MongoDbBson::Int64(value)) => usize::try_from(*value).ok(),
                Some(MongoDbBson::Double(value)) if value.fract() == 0.0 => {
                    (*value).checked_approx_into()
                }
                _ => continue,
            };
            if let Some(size) = size {
                command_sizes.push(size);
            }
        }
        let observed_inserts = profile_documents.len();
        let largest_insert = command_sizes.iter().copied().max().unwrap_or(0);
        if observed_documents == expected_documents
            && observed_inserts >= expected_inserts
            && command_sizes.len() == observed_inserts
            && largest_insert <= maximum_documents_per_insert
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected_documents} MongoDB documents across at least \
             {expected_inserts} inserts of at most {maximum_documents_per_insert} documents; \
             observed documents={observed_documents} inserts={observed_inserts} command \
             sizes={command_sizes:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "within {string} the Iceberg table {string} contains a row")]
async fn then_within_duration_iceberg_table_contains_row(
    world: &mut ScenarioWorld,
    duration: String,
    table: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let table = expand_placeholders(world, &table);
    let domain = world.domain.clone();
    let expected = expand_placeholders(world, docstring(step));
    let expected = serde_json::from_str::<serde_json::Value>(&expected)
        .expect("Iceberg expected row must be valid JSON");
    let dependencies = world.dependencies.endpoints().clone();
    let deadline = Instant::now() + duration;
    let mut observed = Vec::new();

    loop {
        tokio::task::consume_budget().await;
        match iceberg_table_rows(&dependencies, &domain, &table).await {
            Ok(rows) => {
                if rows
                    .iter()
                    .any(|row| iceberg_row_matches_expected(row, &expected))
                {
                    append_cucumber_log_line(&format!(
                        "observed searchable Iceberg row in table {table}: {expected}"
                    ));
                    return;
                }
                observed.push(format!("{rows:?}"));
            }
            Err(error) => observed.push(error),
        }

        assert!(
            Instant::now() < deadline,
            "timed out after {duration:?} waiting for Iceberg table {table} to contain \
             {expected}. observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[then(expr = "the Iceberg table {string} eventually contains a row")]
async fn then_iceberg_table_eventually_contains_row(
    world: &mut ScenarioWorld,
    table: String,
    #[step] step: &Step,
) {
    let table = expand_placeholders(world, &table);
    let domain = world.domain.clone();
    let expected = expand_placeholders(world, docstring(step));
    let expected = serde_json::from_str::<serde_json::Value>(&expected)
        .expect("Iceberg expected row must be valid JSON");
    let dependencies = world.dependencies.endpoints().clone();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut observed = Vec::new();

    loop {
        tokio::task::consume_budget().await;
        match iceberg_table_rows(&dependencies, &domain, &table).await {
            Ok(rows) => {
                if rows
                    .iter()
                    .any(|row| iceberg_row_matches_expected(row, &expected))
                {
                    append_cucumber_log_line(&format!(
                        "observed searchable Iceberg row in table {table}: {expected}"
                    ));
                    return;
                }
                observed.push(format!("{rows:?}"));
            }
            Err(error) => observed.push(error),
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Iceberg table {table} to contain {expected}. observed \
             {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the Iceberg table {string} does not contain a row within {string}")]
async fn then_iceberg_table_does_not_contain_row_within(
    world: &mut ScenarioWorld,
    table: String,
    duration: String,
    #[step] step: &Step,
) {
    let table = expand_placeholders(world, &table);
    let domain = world.domain.clone();
    let expected = expand_placeholders(world, docstring(step));
    let expected = serde_json::from_str::<serde_json::Value>(&expected)
        .expect("Iceberg expected row must be valid JSON");
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let dependencies = world.dependencies.endpoints().clone();
    let deadline = Instant::now() + duration;

    loop {
        tokio::task::consume_budget().await;
        match iceberg_table_rows(&dependencies, &domain, &table).await {
            Ok(rows) => {
                assert!(
                    !rows
                        .iter()
                        .any(|row| iceberg_row_matches_expected(row, &expected)),
                    "expected Iceberg table {table} not to contain {expected}, observed {rows:?}"
                );
            }
            Err(error) => append_cucumber_log_line(&format!(
                "Iceberg absence check for table {table} could not scan yet: {error}"
            )),
        }
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then(expr = "the Iceberg table {string} metadata does not contain {string}")]
async fn then_iceberg_table_metadata_does_not_contain(
    world: &mut ScenarioWorld,
    table: String,
    fragment: String,
) {
    let table = expand_placeholders(world, &table);
    let fragment = expand_placeholders(world, &fragment);
    let domain = world.domain.clone();
    let dependencies = world.dependencies.endpoints().clone();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut observed = Vec::new();

    loop {
        tokio::task::consume_budget().await;
        match iceberg_table_metadata(&dependencies, &domain, &table).await {
            Ok(metadata) => {
                assert!(
                    !metadata.contains(&fragment),
                    "expected Iceberg table {table} metadata not to contain {fragment:?}, got \
                     {metadata}"
                );
                return;
            }
            Err(error) => observed.push(error),
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for Iceberg table {table} metadata. observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the temp directory eventually contains an Iceberg Arrow IPC staged batch")]
async fn then_temp_directory_contains_iceberg_arrow_ipc_staged_batch(world: &mut ScenarioWorld) {
    let temp_root = world
        .temp_root
        .as_ref()
        .expect("temp root must be configured by the scenario");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        tokio::task::consume_budget().await;
        if path_contains_staged_iceberg_arrow_ipc_batch(temp_root.path()) {
            append_cucumber_log_line(&format!(
                "observed Iceberg Arrow IPC staged batch under {}",
                temp_root.path().display()
            ));
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for Iceberg Arrow IPC staged batch under {}",
            temp_root.path().display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[then("the temp directory does not contain an Iceberg Parquet staged batch")]
async fn then_temp_directory_does_not_contain_iceberg_parquet_staged_batch(
    world: &mut ScenarioWorld,
) {
    let temp_root = world
        .temp_root
        .as_ref()
        .expect("temp root must be configured by the scenario");
    assert!(
        !path_contains_staged_iceberg_parquet_batch(temp_root.path()),
        "observed local Iceberg Parquet staged batch under {}",
        temp_root.path().display()
    );
}

#[then(expr = "the object storage path {string} does not exist")]
async fn then_object_storage_path_does_not_exist(world: &mut ScenarioWorld, path: String) {
    let path = expand_placeholders(world, &path);
    let exists = rustfs_iceberg_file_io(world.dependencies.endpoints())
        .exists(&path)
        .await
        .unwrap_or_else(|source| panic!("failed to check object storage path {path}: {source}"));
    assert!(!exists, "object storage path {path} exists");
}

fn path_contains_staged_iceberg_arrow_ipc_batch(root: &Path) -> bool {
    path_contains_staged_iceberg_batch(root, |path, name| {
        if !name.starts_with("batch-") || !name.ends_with(".arrow") {
            return false;
        }
        let Ok(file) = std::fs::File::open(path) else {
            return false;
        };
        let Ok(reader) = StreamReader::try_new(file, None) else {
            return false;
        };
        let Ok(batches) = reader.collect::<Result<Vec<_>, _>>() else {
            return false;
        };
        !batches.is_empty()
    })
}

fn path_contains_staged_iceberg_parquet_batch(root: &Path) -> bool {
    path_contains_staged_iceberg_batch(root, |_path, name| {
        name.starts_with("batch-") && name.ends_with(".parquet")
    })
}

fn path_contains_staged_iceberg_batch(
    root: &Path,
    predicate: impl Copy + Fn(&Path, &str) -> bool,
) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path_contains_staged_iceberg_batch(&path, predicate) {
                return true;
            }
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| predicate(&path, name))
        {
            return true;
        }
    }
    false
}

async fn iceberg_table_rows(
    dependencies: &DependencyEndpoints,
    domain: &str,
    table: &str,
) -> Result<Vec<serde_json::Value>, String> {
    let table = rustfs_iceberg_table(dependencies, domain, table).await?;
    let stream = table
        .scan()
        .build()
        .map_err(|source| format!("failed to build Iceberg table scan: {source}"))?
        .to_arrow()
        .await
        .map_err(|source| format!("failed to open Iceberg Arrow scan: {source}"))?;
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|source| format!("failed to read Iceberg Arrow batches: {source}"))?;
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(iceberg_record_batch_rows(&batch)?);
    }
    Ok(rows)
}

async fn iceberg_table_metadata(
    dependencies: &DependencyEndpoints,
    domain: &str,
    table: &str,
) -> Result<String, String> {
    let metadata_location = iceberg_table_metadata_location(dependencies, domain, table).await?;
    let bytes = rustfs_iceberg_file_io(dependencies)
        .new_input(&metadata_location)
        .map_err(|source| format!("failed to open Iceberg table metadata: {source}"))?
        .read()
        .await
        .map_err(|source| format!("failed to read Iceberg table metadata: {source}"))?;
    String::from_utf8(bytes.to_vec())
        .map_err(|source| format!("Iceberg table metadata is not UTF-8 JSON: {source}"))
}

async fn iceberg_table_metadata_location(
    dependencies: &DependencyEndpoints,
    domain: &str,
    table: &str,
) -> Result<String, String> {
    let table = rustfs_iceberg_table(dependencies, domain, table).await?;
    table
        .metadata_location_result()
        .map(|location| location.to_string())
        .map_err(|source| format!("Iceberg table metadata location is unavailable: {source}"))
}

fn rustfs_iceberg_file_io(dependencies: &DependencyEndpoints) -> FileIO {
    FileIOBuilder::new(rustfs_iceberg_storage_factory())
        .with_props(rustfs_iceberg_props(dependencies))
        .build()
}

async fn rustfs_iceberg_table(
    dependencies: &DependencyEndpoints,
    domain: &str,
    table: &str,
) -> Result<iceberg::table::Table, String> {
    let catalog = rustfs_rest_catalog(dependencies).await?;
    let table_ident = TableIdent::new(NamespaceIdent::new(domain.to_string()), table.to_string());
    catalog
        .load_table(&table_ident)
        .await
        .map_err(|source| format!("failed to load Iceberg table {domain}.{table}: {source}"))
}

async fn rustfs_rest_catalog(dependencies: &DependencyEndpoints) -> Result<RestCatalog, String> {
    let props = rustfs_iceberg_props(dependencies)
        .into_iter()
        .chain([
            (
                REST_CATALOG_PROP_URI.to_string(),
                dependencies
                    .get(ICEBERG_REST_ADDR)
                    .map_err(|error| error.to_string())?
                    .to_string(),
            ),
            (
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                "s3://nervix-iceberg/warehouse".to_string(),
            ),
        ])
        .collect();
    RestCatalogBuilder::default()
        .with_storage_factory(rustfs_iceberg_storage_factory())
        .load("iceberg_catalog", props)
        .await
        .map_err(|source| format!("{source}"))
}

fn rustfs_iceberg_props(dependencies: &DependencyEndpoints) -> [(String, String); 7] {
    [
        (
            S3_ENDPOINT.to_string(),
            dependencies
                .get(RUSTFS_ADDR)
                .expect("RustFS dependency endpoint must be configured")
                .to_string(),
        ),
        (S3_REGION.to_string(), "us-east-1".to_string()),
        (S3_ACCESS_KEY_ID.to_string(), "rustfsadmin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "rustfsadmin".to_string()),
        (S3_PATH_STYLE_ACCESS.to_string(), "true".to_string()),
        (S3_DISABLE_EC2_METADATA.to_string(), "true".to_string()),
        (S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string()),
    ]
}

fn rustfs_iceberg_storage_factory() -> StdArc<dyn iceberg::io::StorageFactory> {
    StdArc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    })
}

struct IcebergTableFixture {
    dependencies: DependencyEndpoints,
    domain: String,
    table: String,
    location: String,
    fields: Vec<arrow_schema::Field>,
}

const ICEBERG_TABLE_PROVISION_TIMEOUT: Duration = Duration::from_secs(15);
const ICEBERG_TABLE_PROVISION_RETRY_INTERVAL: Duration = Duration::from_millis(100);

impl IcebergTableFixture {
    fn from_step(world: &ScenarioWorld, table: String, location: String, columns: &str) -> Self {
        Self {
            dependencies: world.dependencies.endpoints().clone(),
            domain: world.domain.clone(),
            table: expand_placeholders(world, &table),
            location: expand_placeholders(world, &location),
            fields: Self::parse_fields(columns),
        }
    }

    async fn ensure(&self) -> Result<(), String> {
        let _guard = ICEBERG_TABLE_PROVISION_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;
        let deadline = Instant::now() + ICEBERG_TABLE_PROVISION_TIMEOUT;
        loop {
            tokio::task::consume_budget().await;
            match self.ensure_once().await {
                Ok(()) => return Ok(()),
                Err(error) if Self::is_transient_catalog_lock(&error) => {
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "timed out retrying Iceberg table setup after transient catalog lock: \
                             {error}"
                        ));
                    }
                    tokio::time::sleep(ICEBERG_TABLE_PROVISION_RETRY_INTERVAL).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn ensure_once(&self) -> Result<(), String> {
        let catalog = rustfs_rest_catalog(&self.dependencies).await?;
        let namespace = NamespaceIdent::new(self.domain.clone());
        if !catalog
            .namespace_exists(&namespace)
            .await
            .map_err(|source| format!("failed to check Iceberg namespace: {source}"))?
        {
            catalog
                .create_namespace(&namespace, Default::default())
                .await
                .map_err(|source| format!("failed to create Iceberg namespace: {source}"))?;
        }

        let table_ident = TableIdent::new(namespace, self.table.clone());
        if catalog
            .table_exists(&table_ident)
            .await
            .map_err(|source| format!("failed to check Iceberg table: {source}"))?
        {
            let table = catalog
                .load_table(&table_ident)
                .await
                .map_err(|source| format!("failed to load Iceberg table: {source}"))?;
            if table.metadata().location() != self.location {
                return Err(format!(
                    "Iceberg table {table_ident} exists at '{}' instead of '{}'",
                    table.metadata().location(),
                    self.location
                ));
            }
            return Ok(());
        }

        let schema =
            arrow_schema_to_schema_auto_assign_ids(&arrow_schema::Schema::new(self.fields.clone()))
                .map_err(|source| format!("failed to build Iceberg table schema: {source}"))?;
        let creation = TableCreation::builder()
            .name(self.table.clone())
            .location(self.location.clone())
            .schema(schema)
            .build();
        catalog
            .create_table(table_ident.namespace(), creation)
            .await
            .map_err(|source| format!("failed to create Iceberg table {table_ident}: {source}"))?;
        Ok(())
    }

    fn is_transient_catalog_lock(error: &str) -> bool {
        error.contains("SQLITE_BUSY") || error.contains("database is locked")
    }

    fn parse_fields(columns: &str) -> Vec<arrow_schema::Field> {
        columns
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(Self::parse_field)
            .collect()
    }

    fn parse_field(line: &str) -> arrow_schema::Field {
        let normalized = line.trim_end_matches(',');
        let mut parts = normalized.split_whitespace();
        let name = parts
            .next()
            .unwrap_or_else(|| panic!("Iceberg column line is missing a name: {line}"));
        let column_type = parts
            .next()
            .unwrap_or_else(|| panic!("Iceberg column line is missing a type: {line}"));
        assert!(
            parts.next().is_none(),
            "Iceberg column line must be '<name> <type>': {line}"
        );
        arrow_schema::Field::new(name, Self::parse_data_type(column_type), true)
    }

    fn parse_data_type(column_type: &str) -> ArrowDataType {
        match column_type {
            "STRING" => ArrowDataType::Utf8,
            "I64" => ArrowDataType::Int64,
            "F64" => ArrowDataType::Float64,
            "BOOLEAN" => ArrowDataType::Boolean,
            "DATETIME" => {
                ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some("+00:00".into()))
            }
            other => panic!("unsupported Iceberg fixture column type '{other}'"),
        }
    }
}

fn iceberg_record_batch_rows(batch: &RecordBatch) -> Result<Vec<serde_json::Value>, String> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let mut row = serde_json::Map::new();
        for column_index in 0..batch.num_columns() {
            let field = schema.field(column_index);
            row.insert(
                field.name().to_string(),
                iceberg_cell_value(batch, column_index, row_index)?,
            );
        }
        rows.push(serde_json::Value::Object(row));
    }
    Ok(rows)
}

fn iceberg_cell_value(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
) -> Result<serde_json::Value, String> {
    let schema = batch.schema();
    let field = schema.field(column_index);
    let array = batch.column(column_index);
    if array.is_null(row_index) {
        return Ok(serde_json::Value::Null);
    }
    match field.data_type() {
        ArrowDataType::Int64 => Ok(serde_json::Value::from(
            iceberg_column::<Int64Array>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::UInt64 => Ok(serde_json::Value::from(
            iceberg_column::<UInt64Array>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::Boolean => Ok(serde_json::Value::from(
            iceberg_column::<BooleanArray>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::Utf8 => Ok(serde_json::Value::from(
            iceberg_column::<StringArray>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::LargeUtf8 => Ok(serde_json::Value::from(
            iceberg_column::<LargeStringArray>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::Utf8View => Ok(serde_json::Value::from(
            iceberg_column::<StringViewArray>(batch, column_index)?.value(row_index),
        )),
        ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, _) => Ok(serde_json::Value::from(
            iceberg_column::<TimestampMicrosecondArray>(batch, column_index)?.value(row_index),
        )),
        unsupported => Err(format!(
            "unsupported Iceberg assertion field '{}' Arrow type {unsupported:?}",
            field.name()
        )),
    }
}

fn iceberg_column<T: 'static>(batch: &RecordBatch, column_index: usize) -> Result<&T, String> {
    let schema = batch.schema();
    batch
        .column(column_index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            format!(
                "Iceberg assertion column '{}' did not contain expected Arrow array {}",
                schema.field(column_index).name(),
                std::any::type_name::<T>()
            )
        })
}

fn iceberg_row_matches_expected(row: &serde_json::Value, expected: &serde_json::Value) -> bool {
    let (Some(row), Some(expected)) = (row.as_object(), expected.as_object()) else {
        return row == expected;
    };
    expected
        .iter()
        .all(|(key, value)| row.get(key).is_some_and(|row_value| row_value == value))
}

#[then(expr = "within {string} the observed broker receives payloads")]
async fn then_within_duration_the_observed_broker_receives_payloads(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected_fragments = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| expand_placeholders(world, line))
        .collect::<Vec<_>>();

    assert!(
        !expected_fragments.is_empty(),
        "step docstring must contain at least one expected payload fragment"
    );

    let deadline = Instant::now() + duration;
    let mut remaining = expected_fragments
        .iter()
        .fold(BTreeMap::new(), |mut counts, fragment| {
            *counts.entry(fragment.clone()).or_insert(0usize) += 1;
            counts
        });
    let mut observed = Vec::with_capacity(expected_fragments.len());

    while !remaining.is_empty() {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for broker payloads. expected remaining {:?}, observed {:?}",
            remaining,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let payload = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_payload(wait)
            .await
            .expect("failed while waiting for broker payloads");
        let Some(payload) = payload else {
            // Every diagnostic below is bounded, so a node that never answers cannot keep this
            // step from reporting its failure.
            let status_snapshots = world.cluster().collect_status_snapshots().await;
            let descriptions = PhaseDeadline::after(STATUS_DIAGNOSTIC_BUDGET);
            let mut runtime_diagnostics = Vec::new();
            for (node_id, status) in status_snapshots {
                tokio::task::consume_budget().await;
                let mut node_diagnostics = Vec::new();
                match status {
                    Ok(status) => {
                        node_diagnostics.push(format!("SHOW CLUSTER STATUS; => Ok({status:?})"));
                    }
                    Err(error) => {
                        node_diagnostics.push(format!("SHOW CLUSTER STATUS; => Err({error:#})"));
                    }
                }
                for command in [
                    "DESCRIBE DOMAIN;",
                    "DESCRIBE INGESTOR ws_notifications;",
                    "DESCRIBE EMITTER kafka_forward;",
                ] {
                    tokio::task::consume_budget().await;
                    let described = descriptions
                        .bound(
                            world
                                .cluster()
                                .run_command(&node_id, &world.domain, command),
                        )
                        .await;
                    match described {
                        BeforeDeadline::Finished(result) => {
                            node_diagnostics.push(format!("{command} => {result:?}"));
                        }
                        BeforeDeadline::Passed => node_diagnostics.push(format!(
                            "{command} => still pending when the {STATUS_DIAGNOSTIC_BUDGET:?} \
                             diagnostic budget ran out"
                        )),
                    }
                }
                runtime_diagnostics.push(format!("{node_id}: {node_diagnostics:?}"));
            }
            panic!(
                "timed out waiting for broker payloads. expected remaining {:?}, observed {:?}. \
                 runtime diagnostics: {:?}",
                remaining, observed, runtime_diagnostics
            );
        };
        observed.push(payload.clone());
        world.last_broker_payload = Some(payload.clone());

        if let Some(fragment) = remaining
            .keys()
            .find(|fragment| payload.contains(fragment.as_str()))
            .cloned()
        {
            let count = remaining
                .get_mut(&fragment)
                .expect("matched fragment must be present in remaining set");
            *count -= 1;
            if *count == 0 {
                remaining.remove(&fragment);
            }
        }
    }
}

#[then(
    expr = "within {string} the observed broker receives JSON payloads preserving {string} group \
            order"
)]
async fn then_observed_broker_receives_json_payloads_preserving_group_order(
    world: &mut ScenarioWorld,
    duration: String,
    group_field: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected = docstring(step)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(&expand_placeholders(world, line))
                .unwrap_or_else(|error| panic!("invalid expected broker JSON payload: {error}"))
        })
        .collect::<Vec<_>>();
    assert!(
        !expected.is_empty(),
        "step docstring must contain at least one expected JSON payload"
    );

    let deadline = Instant::now() + duration;
    let mut actual = Vec::with_capacity(expected.len());
    while actual.len() < expected.len() {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {} grouped broker payloads; observed {actual:?}",
            expected.len()
        );
        let payload = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_payload(deadline.saturating_duration_since(now))
            .await
            .expect("failed while waiting for grouped broker payloads")
            .unwrap_or_else(|| {
                panic!(
                    "timed out waiting for {} grouped broker payloads; observed {actual:?}",
                    expected.len()
                )
            });
        actual.push(
            serde_json::from_str::<serde_json::Value>(&payload)
                .unwrap_or_else(|error| panic!("broker payload is not valid JSON: {error}")),
        );
    }

    let group_payloads = |payloads: Vec<serde_json::Value>| {
        let mut grouped = BTreeMap::<String, Vec<serde_json::Value>>::new();
        for payload in payloads {
            let group = payload
                .get(&group_field)
                .unwrap_or_else(|| {
                    panic!("broker payload {payload} has no group field '{group_field}'")
                })
                .to_string();
            grouped.entry(group).or_default().push(payload);
        }
        grouped
    };
    assert_eq!(
        group_payloads(actual),
        group_payloads(expected),
        "broker payload order changed within at least one '{group_field}' group"
    );
}

#[then(expr = "within {string} the observed broker receives exactly {int} messages")]
async fn then_within_duration_the_observed_broker_receives_exactly_messages(
    world: &mut ScenarioWorld,
    duration: String,
    count: usize,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let deadline = Instant::now() + duration;
    let mut payload_counts = BTreeMap::new();

    for received in 0..count {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out after receiving {received} of {count} broker messages within {duration:?}; \
             observed payload counts: {payload_counts:?}"
        );
        let message = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_message(deadline.saturating_duration_since(now))
            .await
            .expect("failed while waiting for an exact broker message count")
            .unwrap_or_else(|| {
                panic!(
                    "timed out after receiving {received} of {count} broker messages within \
                     {duration:?}; observed payload counts: {payload_counts:?}"
                )
            });
        *payload_counts
            .entry(message.payload.clone())
            .or_insert(0usize) += 1;
        world.last_broker_payload = Some(message.payload);
        world.last_broker_headers = message.headers;
    }

    let duplicate = world
        .broker_observer
        .as_mut()
        .expect("a broker observer must exist before assertion")
        .try_next_message(Duration::from_millis(500))
        .await
        .expect("failed while checking for a duplicate broker message");
    assert!(
        duplicate.is_none(),
        "observed an extra broker message after receiving exactly {count}: {duplicate:?}; \
         observed payload counts: {payload_counts:?}"
    );
}

/// Every docstring line is one exact payload. The broker must deliver exactly those payloads, in
/// any order, and nothing else: a payload the emitter was required to withhold must never arrive,
/// so the closing window only strengthens the assertion.
#[then(expr = "within {string} the observed broker receives exactly these payloads")]
async fn then_within_duration_the_observed_broker_receives_exactly_these_payloads(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let mut expected = BTreeMap::<Vec<u8>, usize>::new();
    for line in docstring(step).lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let payload = expand_placeholders(world, line);
        *expected.entry(payload.into_bytes()).or_insert(0) += 1;
    }
    receive_exactly_these_broker_payloads(world, &duration, expected).await;
}

/// Like the step above, for payloads that are not all text. Each docstring line is `text:`
/// followed by the exact payload text, or `hex:` followed by the exact payload bytes in hex.
#[then(expr = "within {string} the observed broker receives exactly these encoded payloads")]
async fn then_within_duration_the_observed_broker_receives_exactly_these_encoded_payloads(
    world: &mut ScenarioWorld,
    duration: String,
    #[step] step: &Step,
) {
    let mut expected = BTreeMap::<Vec<u8>, usize>::new();
    for line in docstring(step).lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let payload = if let Some(text) = line.strip_prefix("text:") {
            expand_placeholders(world, text).into_bytes()
        } else if let Some(hex) = line.strip_prefix("hex:") {
            decode_hex_payload(hex)
        } else {
            panic!("encoded payload line must start with 'text:' or 'hex:', found {line:?}");
        };
        *expected.entry(payload).or_insert(0) += 1;
    }
    receive_exactly_these_broker_payloads(world, &duration, expected).await;
}

fn decode_hex_payload(hex: &str) -> Vec<u8> {
    let digits = hex
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<Vec<_>>();
    assert!(
        digits.len() % 2 == 0,
        "hex payload must hold an even number of digits: {hex:?}"
    );
    digits
        .chunks(2)
        .map(|pair| {
            let pair = pair.iter().collect::<String>();
            u8::from_str_radix(&pair, 16)
                .unwrap_or_else(|error| panic!("invalid hex byte {pair:?} in {hex:?}: {error}"))
        })
        .collect()
}

fn describe_broker_payload(payload: &[u8]) -> String {
    let hex = payload
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!(
        "{:?} (hex {hex}, {} bytes)",
        String::from_utf8_lossy(payload),
        payload.len()
    )
}

async fn receive_exactly_these_broker_payloads(
    world: &mut ScenarioWorld,
    duration: &str,
    mut remaining: BTreeMap<Vec<u8>, usize>,
) {
    let duration =
        humantime::parse_duration(duration).expect("step duration must be a valid duration");
    assert!(
        !remaining.is_empty(),
        "step docstring must contain at least one expected payload"
    );
    let describe_remaining = |remaining: &BTreeMap<Vec<u8>, usize>| {
        remaining
            .iter()
            .map(|(payload, count)| format!("{count} x {}", describe_broker_payload(payload)))
            .collect::<Vec<_>>()
    };

    let deadline = Instant::now() + duration;
    let mut observed = Vec::new();
    while !remaining.is_empty() {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for broker payloads; expected remaining {:?}, observed {observed:?}",
            describe_remaining(&remaining)
        );
        let message = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_message(deadline.saturating_duration_since(now))
            .await
            .expect("failed while waiting for exact broker payloads");
        let Some(message) = message else {
            panic!(
                "timed out waiting for broker payloads; expected remaining {:?}, observed \
                 {observed:?}",
                describe_remaining(&remaining)
            );
        };
        let Some(count) = remaining.get_mut(&message.bytes) else {
            panic!(
                "observed an unexpected broker payload {}; expected remaining {:?}, observed \
                 before it {observed:?}",
                describe_broker_payload(&message.bytes),
                describe_remaining(&remaining)
            );
        };
        *count -= 1;
        if *count == 0 {
            remaining.remove(&message.bytes);
        }
        world.last_broker_payload = Some(message.payload.clone());
        observed.push(describe_broker_payload(&message.bytes));
    }

    let extra = world
        .broker_observer
        .as_mut()
        .expect("a broker observer must exist before assertion")
        .try_next_message(Duration::from_secs(2))
        .await
        .expect("failed while checking for an unexpected broker payload");
    assert!(
        extra.is_none(),
        "observed a broker payload beyond the expected ones: {:?}; observed {observed:?}",
        extra.map(|message| describe_broker_payload(&message.bytes))
    );
}

#[then(
    expr = "within {string} the observed broker receives {int} messages in sequence by field \
            {string} with headers"
)]
async fn then_observed_broker_receives_sequential_messages_with_headers(
    world: &mut ScenarioWorld,
    duration: String,
    count: usize,
    field: String,
    #[step] step: &Step,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let expected_headers = expand_placeholders(world, docstring(step))
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.split_once('=').unwrap_or_else(|| {
                panic!("expected broker header assertion '{line}' to use name=value")
            })
        })
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect::<Vec<_>>();
    assert!(
        !expected_headers.is_empty(),
        "sequential broker assertion must include at least one header"
    );

    let deadline = Instant::now() + duration;
    for expected_sequence in 1..=count {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out after receiving {} of {count} sequential broker messages",
            expected_sequence - 1
        );
        let message = world
            .broker_observer
            .as_mut()
            .expect("a broker observer must exist before assertion")
            .try_next_message(deadline.saturating_duration_since(now))
            .await
            .expect("failed while waiting for sequential broker message")
            .unwrap_or_else(|| {
                panic!("timed out waiting for broker message {expected_sequence} of {count}")
            });
        let payload = serde_json::from_str::<serde_json::Value>(&message.payload)
            .unwrap_or_else(|error| panic!("broker payload is not valid JSON: {error}"));
        let actual_sequence = payload
            .get(&field)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_else(|| {
                panic!(
                    "broker payload field '{field}' is not an unsigned integer: {}",
                    message.payload
                )
            });
        let expected_u64 = expected_sequence.arch_into();
        assert_eq!(
            actual_sequence,
            expected_u64,
            "broker messages were duplicated, lost, or reordered: expected sequence \
             {expected_u64} but observed {actual_sequence} ({} messages behind, {} of {} consumed \
             so far)",
            actual_sequence.abs_diff(expected_u64),
            expected_sequence,
            count
        );
        assert_eq!(
            message.headers, expected_headers,
            "broker message {expected_sequence} headers changed"
        );
        world.last_broker_payload = Some(message.payload);
        world.last_broker_headers = message.headers;
    }

    let duplicate = world
        .broker_observer
        .as_mut()
        .expect("a broker observer must exist before assertion")
        .try_next_message(Duration::from_millis(500))
        .await
        .expect("failed while checking for a duplicate broker message");
    assert!(
        duplicate.is_none(),
        "observed a duplicate broker message after the expected sequence: {duplicate:?}"
    );
}

#[then(expr = "the observed broker does not receive a payload within {string}")]
async fn then_the_observed_broker_does_not_receive_a_payload_within(
    world: &mut ScenarioWorld,
    duration: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let observer = world
        .broker_observer
        .as_mut()
        .expect("a broker observer must exist before assertion");
    let payload = observer
        .try_next_payload(duration)
        .await
        .expect("failed while waiting for absence of broker payload");

    assert!(
        payload.is_none(),
        "expected no broker payload, got: {:?}",
        payload
    );
}

async fn capture_and_assert_subscription_payload(
    world: &mut ScenarioWorld,
    expected_payload: &str,
    expect_topic_key: bool,
    timeout: Duration,
) {
    let expected_payload = expected_payload.trim().to_string();
    if let Some(payload) = world.last_subscription_payload.as_deref()
        && (!expect_topic_key || payload.contains("key=notifications"))
        && payload.contains(&expected_payload)
    {
        return;
    }
    let deadline = Instant::now() + timeout;
    let mut observed = Vec::new();

    loop {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payload containing {}. observed {:?}",
            expected_payload,
            observed
        );
        let wait = deadline.saturating_duration_since(now);
        let event = world
            .active_session
            .as_mut()
            .expect("an active session with subscription must exist")
            .try_next_subscription(wait)
            .await
            .expect("failed to receive subscription event");
        let Some(event) = event else {
            let mut domain_descriptions = Vec::new();
            for node_id in world.cluster().node_ids() {
                tokio::task::consume_budget().await;
                let result = world
                    .cluster()
                    .run_command(&node_id, &world.domain, "DESCRIBE DOMAIN;")
                    .await;
                domain_descriptions.push(format!("{node_id}: {result:?}"));
            }
            panic!(
                "timed out waiting for subscription payload containing {}. observed {:?}. domain \
                 descriptions: {:?}",
                expected_payload, observed, domain_descriptions
            );
        };
        let payload = event.payload;
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload.clone());

        if expect_topic_key && !payload.contains("key=notifications") {
            continue;
        }
        if payload.contains(&expected_payload) {
            break;
        }
    }
}

#[then(
    expr = "the relay subscription receives a payload no sooner than {string} after it was \
            published"
)]
async fn then_stream_subscription_receives_payload_no_sooner_than(
    world: &mut ScenarioWorld,
    delay: String,
    #[step] step: &Step,
) {
    let expected_delay =
        humantime::parse_duration(&delay).expect("step duration must be a valid duration");
    let published_at = world
        .last_publish_at
        .expect("a delivery-delay assertion must follow a publishing step");
    let expected_payload = expand_placeholders(world, docstring(step))
        .trim()
        .to_string();
    append_cucumber_log_line(&format!(
        "awaiting subscription payload containing {} at least {:?} after publishing",
        expected_payload.replace('\n', "\\n"),
        expected_delay
    ));

    let deadline = Instant::now() + expected_delay + SUBSCRIPTION_DELIVERY_BUDGET;
    let mut observed = Vec::new();
    loop {
        tokio::task::consume_budget().await;
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for subscription payload containing {expected_payload}. observed \
             {observed:?}"
        );
        let event = world
            .active_session
            .as_mut()
            .expect("an active session with subscription must exist")
            .try_next_subscription(deadline.saturating_duration_since(now))
            .await
            .expect("failed to receive subscription event");
        let Some(event) = event else {
            panic!(
                "timed out waiting for subscription payload containing {expected_payload}. \
                 observed {observed:?}"
            );
        };
        let arrived_after = published_at.elapsed();
        let payload = event.payload;
        observed.push(payload.clone());
        world.last_subscription_payload = Some(payload.clone());
        if !payload.contains(&expected_payload) {
            continue;
        }
        assert!(
            arrived_after >= expected_delay,
            "expected the payload no sooner than {expected_delay:?} after publishing, but it \
             arrived after {arrived_after:?}"
        );
        break;
    }
}

async fn try_capture_any_subscription_payload(
    world: &mut ScenarioWorld,
    duration: Duration,
) -> bool {
    let session = world
        .active_session
        .as_mut()
        .expect("an active session with subscription must exist");
    tokio::task::consume_budget().await;
    let Some(event) = session
        .try_next_subscription(duration)
        .await
        .expect("failed to receive subscription event")
    else {
        return false;
    };
    world.last_subscription_payload = Some(event.payload);
    true
}
#[then(expr = "node {string} eventually reports interconnect to {string} as {string}")]
async fn then_node_eventually_reports_interconnect_status(
    world: &mut ScenarioWorld,
    node_id: String,
    peer_node_id: String,
    expected_status: String,
) {
    world
        .cluster()
        .wait_for_interconnect_status(&node_id, &peer_node_id, &expected_status)
        .await
        .expect("interconnect status did not converge");
}

fn main() {
    TestDependencies::configure_process_lifecycle();
    let parallelism = TestParallelism::detect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(parallelism.tokio_worker_threads())
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("scenario runtime should build");
    let execution = catch_unwind(AssertUnwindSafe(|| {
        if let Some(scope) = std::env::var_os(DEPENDENCY_LIFECYCLE_HELPER_ENV) {
            runtime.block_on(run_dependency_lifecycle_helper(
                scope.to_string_lossy().into_owned(),
            ))
        } else {
            runtime.block_on(run_scenarios(parallelism))
        }
    }));
    // Both bounded, because a run whose result is already decided must still end in time for that
    // result to be uploaded. Stopping containers and dropping a multi-threaded runtime both wait
    // without a bound of their own, and a suite that has finished every scenario has been lost to
    // the workflow's own timeout right here.
    let dependency_teardown =
        runtime.block_on(SuiteTeardown::bounded(TestDependencies::shutdown_suite()));
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_BUDGET);

    let outcome = match execution {
        Ok(outcome) => outcome,
        Err(payload) => resume_unwind(payload),
    };
    // A run the watchdog ended was dropped mid-scenario, so whatever its teardown then found is a
    // consequence of that ending rather than a separate failure. The status that says the budget
    // ended the run is reported first, and the teardown's own report goes out with it.
    if let SuiteOutcome::TimedOut(_) = &outcome {
        eprintln!("{dependency_teardown}");
        outcome.end_process();
        return;
    }

    assert!(dependency_teardown.is_clean(), "{dependency_teardown}");
    outcome.end_process();
}

/// Holds one dependency container open until the parent scenario kills this process.
///
/// The helper never finishes on its own, so it never produces an outcome: the pending future
/// carries the caller's return type rather than a value this function could never reach.
async fn run_dependency_lifecycle_helper(scope: String) -> SuiteOutcome {
    let mut dependencies = TestDependencies::default();
    dependencies
        .start_redis(&scope)
        .await
        .expect("lifecycle helper Redis container should start");
    let container_ids = TestDependencies::suite_container_ids().await;
    assert_eq!(
        container_ids.len(),
        1,
        "lifecycle helper should own exactly one dependency container"
    );
    println!("{DEPENDENCY_LIFECYCLE_STARTED}{}", container_ids[0]);
    std::io::stdout()
        .flush()
        .expect("lifecycle helper marker should flush");
    std::future::pending::<SuiteOutcome>().await
}

/// Stops every HTTP receiver the scenario started, together under one budget, and records how
/// each stop went. A receiver's port goes back with the scenario's other fixture ports at the end
/// of cleanup.
async fn stop_http_receivers(world: &mut ScenarioWorld) {
    let receivers = std::mem::take(&mut world.http_receivers);
    let stops = join_all(receivers.into_iter().map(|(name, receiver)| async move {
        let stop = receiver.stop().await;
        (name, stop)
    }))
    .await;
    for (name, stop) in stops {
        append_cucumber_log_line(&format!("HTTP receiver cleanup: {name}: {stop}"));
        if stop.was_forced() {
            append_cucumber_log_line(&format!(
                "scenario cleanup forced: HTTP receiver {name}: {stop}"
            ));
        }
    }
}

const _: () = assert!(
    RECEIVER_STOP_BUDGET.as_nanos() < CLUSTER_TEARDOWN_BUDGET.as_nanos(),
    "stopping the HTTP receivers must cost less than stopping the cluster"
);

/// Everything a scenario run may be configured with beyond cucumber's own options.
#[derive(Clone, Copy, Debug, clap::Args)]
struct ScenarioRunArgs {
    #[command(flatten)]
    parallelism: TestParallelismArgs,
    #[command(flatten)]
    watchdog: SuiteWatchdogArgs,
}

async fn run_scenarios(parallelism: TestParallelism) -> SuiteOutcome {
    let mut cli =
        cucumber::cli::Opts::<_, cucumber::runner::basic::Cli, _, ScenarioRunArgs>::parsed();
    if cli.tags_filter.is_none() {
        cli.tags_filter = Some(
            "(not @client_wire_expected_failure) and (not @client_wire_baseline) and (not \
             @http_emitter_expected_failure) and (not @client_conformance_toolchain)"
                .parse()
                .assured("the built-in opt-in scenario tag expression is valid"),
        );
    }
    let watchdog = cli.custom.watchdog.watchdog();
    let concurrency_factor = cli.custom.parallelism.concurrency_factor();
    let default_max_concurrent_scenarios = parallelism.max_concurrent_scenarios(concurrency_factor);
    let effective_max_concurrent_scenarios = cli
        .runner
        .concurrency
        .unwrap_or(default_max_concurrent_scenarios);
    truncate_cucumber_log();
    append_cucumber_log_line(&format!(
        "scenario parallelism: max_concurrent_scenarios={effective_max_concurrent_scenarios} \
         concurrency_factor={concurrency_factor} tokio_worker_threads={} suite_budget={:?}",
        parallelism.tokio_worker_threads(),
        watchdog.budget()
    ));
    let writer = writer::Basic::raw(
        std::io::stdout(), // Output to stdout
        writer::Coloring::Auto,
        writer::Verbosity::ShowWorldAndDocString,
    )
    .summarized()
    .normalized()
    .repeat_failed();
    let run = ScenarioWorld::cucumber()
        .max_concurrent_scenarios(default_max_concurrent_scenarios)
        .retries(2)
        .before(|feature, rule, scenario, world| {
            let feature_name = feature.name.clone();
            let scenario_name = scenario.name.clone();
            let scenario_line = scenario.position.line;
            let exclusive = scenario
                .tags
                .iter()
                .chain(rule.iter().flat_map(|rule| &rule.tags))
                .chain(&feature.tags)
                .any(|tag| tag == "exclusive");
            Box::pin(async move {
                // Published before the permits below, so a scenario the suite has taken up is
                // visible while it waits for them rather than only once it runs.
                world.active_scenario = Some(ActiveScenarioRegistration::start(
                    &feature_name,
                    &scenario_name,
                    scenario_line,
                ));
                let wasm_state_reset_scenario_permit =
                    if feature_name == WASM_STATE_RESET_FEATURE_NAME {
                        // Every reset scenario starts a cluster and compiles WASM. Running more
                        // than one with the suite's coverage concurrency starves unrelated
                        // scenario nodes, stretching subsecond assertions into tens of seconds.
                        // Acquire this before the shared execution guard so queued reset scenarios
                        // cannot keep an exclusive scenario from taking that guard.
                        Some(
                            WASM_STATE_RESET_SCENARIO_PERMITS
                                .get_or_init(|| {
                                    StdArc::new(tokio::sync::Semaphore::new(
                                        MAX_CONCURRENT_WASM_STATE_RESET_SCENARIOS,
                                    ))
                                })
                                .clone()
                                .acquire_owned()
                                .await
                                .expect("WASM state reset scenario semaphore must remain open"),
                        )
                    } else {
                        None
                    };
                let execution_lock = SCENARIO_EXECUTION_LOCK
                    .get_or_init(|| StdArc::new(tokio::sync::RwLock::new(())))
                    .clone();
                let execution_permit = if exclusive {
                    ScenarioExecutionPermit::Exclusive {
                        _permit: execution_lock.write_owned().await,
                    }
                } else {
                    ScenarioExecutionPermit::Concurrent {
                        _permit: execution_lock.read_owned().await,
                    }
                };
                world.wasm_state_reset_scenario_permit = wasm_state_reset_scenario_permit;
                world.scenario_execution_permit = Some(execution_permit);
                if WEB_CONSOLE_FEATURE_NAMES
                    .iter()
                    .any(|name| *name == feature_name)
                {
                    // Starting many three-node clusters and optimized WASM consoles together can
                    // starve Chromium renderer event loops under the suite's global concurrency.
                    world.web_console_scenario_permit = Some(
                        WEB_CONSOLE_SCENARIO_PERMITS
                            .get_or_init(|| {
                                StdArc::new(tokio::sync::Semaphore::new(
                                    MAX_CONCURRENT_WEB_CONSOLE_SCENARIOS,
                                ))
                            })
                            .clone()
                            .acquire_owned()
                            .await
                            .expect("web console scenario semaphore must remain open"),
                    );
                }
                world.enter_phase(ScenarioPhase::Body, "");
            })
        })
        .after(|_feature, _rule, _scenario, finished, world| {
            let body = ScenarioBodyResult::from(finished);
            Box::pin(async move {
                let Some(world) = world else {
                    return;
                };
                world.enter_phase(ScenarioPhase::BodyComplete, &format!("result={body}"));

                world.enter_phase(ScenarioPhase::TeardownStarted, "");
                world.stop_durable_catch_up_work();
                world.fault_injection.release_all_health_responses();
                world.fault_injection.release_all_domain_clock_progress();
                world.fault_injection.release_all_command_pauses();
                world.fault_injection.release_all_wasm_checkpoint_pauses();

                world.enter_phase(ScenarioPhase::Diagnostics, "");
                // Scenarios run many at a time, so a status line says which scenario left it.
                let statuses = match &world.active_scenario {
                    Some(registration) => format!("scenario teardown {}", registration.identity()),
                    None => "scenario teardown".to_string(),
                };
                append_cluster_statuses(world, &statuses).await;
                append_cucumber_log_line(&format!(
                    "scenario context: domain={} test_id={} last_command_error={:?} \
                     last_command_output={:?} last_server_error={:?} \
                     last_subscription_payload={:?} last_broker_payload={:?}",
                    world.domain,
                    world.test_id,
                    world.last_command_error,
                    world.last_command_output,
                    world.last_server_error,
                    world.last_subscription_payload,
                    world.last_broker_payload
                ));

                world.enter_phase(ScenarioPhase::Stopping, "");
                world.cli_subscription_reader = None;
                world.cli_subscription_process = None;
                world.cli_subscription_lines = None;
                world.server_process_http_load = None;
                world.held_resource_upload = None;
                world.server_process = None;
                world.broker_observer = None;
                world.syslog_udp_observer = None;
                stop_http_receivers(world).await;
                close_browser(world).await;
                world.active_session = None;
                world.active_session_node = None;
                world.active_session_has_subscription = false;
                world.last_server_error = None;
                let cluster_cleanup = match world.cluster.take() {
                    Some(mut cluster) => {
                        let teardown = cluster.shutdown_for_teardown().await;
                        for panicked in teardown.panics() {
                            append_cucumber_log_line(&format!(
                                "scenario teardown failed: {panicked}"
                            ));
                        }
                        for forced in teardown.forced() {
                            append_cucumber_log_line(&format!("scenario cleanup forced: {forced}"));
                        }
                        if teardown.was_forced() {
                            // Cleanup that has to take a node apart is usually cleanup that ran
                            // beside work heavy enough to starve it, so name what else was live.
                            for active in ActiveScenario::active() {
                                append_cucumber_log_line(&format!(
                                    "scenario live during forced cleanup: {active}"
                                ));
                            }
                        }
                        // Dropping the cluster gives back the temporary storage its nodes wrote to.
                        drop(cluster);
                        format!("teardown={teardown}")
                    }
                    None => "teardown=no cluster".to_string(),
                };
                // The faults a scenario injected into the network outlast its nodes otherwise: a
                // proxy standing in front of a node and a socket held silently open against it are
                // harness state, and they are given back once the nodes they fronted have ended.
                world.stallable_tcp_proxies.clear();
                world.silent_interconnect_peers.clear();
                world.web_console_scenario_permit = None;
                world.wasm_state_reset_scenario_permit = None;
                world.scenario_execution_permit = None;
                // The ZeroMQ and syslog ports the scenario drew for itself were bound by its nodes
                // and its observers, and both are gone by now, so the ports go back to the pool
                // the next scenario draws from.
                let scenario_ports = std::mem::take(&mut world.scenario_ports);
                crate::common::port_pool::release_test_ports(&scenario_ports);

                world.enter_phase(
                    ScenarioPhase::Finished,
                    &format!("body={body} {cluster_cleanup}"),
                );
            })
        })
        .with_writer(writer)
        // Must wrap the configured writer, not precede it: `with_writer` replaces the writer it is
        // given, so calling this first silently discards the guard and lets an unmatched step skip
        // its scenario while the suite still reports green.
        .fail_on_skipped()
        .with_cli(cli)
        .run(SCENARIOS_PATH);

    // The run is bounded rather than awaited: a step, a teardown diagnostic or a node stop that
    // never returns would otherwise keep the whole suite alive until the workflow job is killed
    // mid-scenario, leaving logs without the suite's own diagnostic. Cucumber's fail-fast is not
    // this guarantee — it stops scheduling and leaves the scenarios already running exactly where
    // they are — so the retry coverage below keeps running until the budget itself expires.
    let writer = match watchdog.bound(run).await {
        SuiteRun::Completed(writer) => writer,
        SuiteRun::TimedOut(timeout) => {
            for line in timeout.to_string().lines() {
                append_cucumber_log_line(line);
            }
            if timeout.cleanup.was_forced() {
                // A node the watchdog could not stop is aborted with the run, so the record of
                // what it was is this line and nothing else.
                append_cucumber_log_line(
                    "suite timeout cleanup forced: the run was dropped with nodes still running",
                );
            }
            return SuiteOutcome::TimedOut(timeout);
        }
    };

    let execution_failure = if writer.execution_has_failed() {
        let mut messages = Vec::new();
        let failed_steps = writer.failed_steps();
        if failed_steps > 0 {
            messages.push(format!("{failed_steps} step(s) failed"));
        }
        let parsing_errors = writer.parsing_errors();
        if parsing_errors > 0 {
            messages.push(format!("{parsing_errors} parsing error(s)"));
        }
        let hook_errors = writer.hook_errors();
        if hook_errors > 0 {
            messages.push(format!("{hook_errors} hook error(s)"));
        }
        SuiteOutcome::Failed(messages.join(", "))
    } else {
        SuiteOutcome::Passed
    };
    drop(writer);

    execution_failure
}
