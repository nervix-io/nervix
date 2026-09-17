//! The control plane and the edges it is reached through.
//!
//! Layer: control plane and edges.
//!
//! - **Owns.** The session service, transaction lifecycle, domain lifecycle commands, users and
//!   authentication, resource upload and replication, subscription management, the domain clock,
//!   leader-side coordination, and the listeners for HTTP endpoints, the authenticated
//!   interconnect, metrics and the console.
//! - **Depends on.** The registry for decisions, the runtime for execution, consensus and the
//!   interconnect for cluster state, the proto wire types, and the language layer: this module is
//!   the session adapter and the one place in the server that may name the parser.
//! - **Must not know.** Connector internals, VM IR, Arrow batches or branch-local runtime state. It
//!   commands the data plane and reads what the data plane reports.
//!
//! This module breaks its own contract: it is a single file in which one `SessionServiceImpl` is at
//! once the gRPC adapter, the transaction manager, the leader-side scheduler, the describe fan-out,
//! the domain-clock owner, the subscription manager, the resource uploader and four HTTP servers.
//! Separating the use cases from the adapters is a later move; every piece it is split into
//! inherits the contract above.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc as StdArc,
};

use admitted_connection::AdmittingListener;
use ahash::{HashMap, HashMapExt, HashSet, RandomState};
use authentication::{BasicAuthCredentials, DEFAULT_USER, user_credentials};
use background_task::{
    await_background_task_shutdown, join_public_listeners, request_shutdown_on_completion,
};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{Shell, generate};
use domain_clock::{
    DomainClockRetirements, DomainClockTask, reconcile_domain_clock_tasks,
    run_domain_clock_authority_reconciliation,
};
use error_stack::{Report, ResultExt};
use futures_util::{
    StreamExt,
    future::join_all,
    stream::{self},
};
use http_endpoint::{serve_http, serve_https};
use interconnect_relay::InterconnectRelayPayloadLane;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{ConsensusSettings, RaftRetentionPolicy, TransactionState};
use nervix_execution::{Executor, sync::DashMap};
use nervix_interconnect::{
    ActivateOwnershipHandoffStateRequest as RemoteActivateOwnershipHandoffStateRequest,
    ApplicationHealthProbe,
    CaptureOwnershipHandoffStateRequest as RemoteCaptureOwnershipHandoffStateRequest,
    ConfirmOwnershipHandoffStateRequest as RemoteConfirmOwnershipHandoffStateRequest,
    ControlEnvelope, DataflowNodeStatusRequest as RemoteDataflowNodeStatusRequest,
    DataflowNodeStatusResponse as RemoteDataflowNodeStatusResponse,
    DescribeIngestorRequest as RemoteDescribeIngestorRequest,
    DescribeLookupRequest as RemoteDescribeLookupRequest,
    DescribeLookupResponse as RemoteDescribeLookupResponse,
    DescribeMetricsRequest as RemoteDescribeMetricsRequest,
    DescribeMetricsResponse as RemoteDescribeMetricsResponse,
    DescribeRelayRequest as RemoteDescribeRelayRequest,
    DescribeRelayResponse as RemoteDescribeRelayResponse,
    DiscardOwnershipHandoffStateRequest as RemoteDiscardOwnershipHandoffStateRequest,
    DomainClockProgressRequest as RemoteDomainClockProgressRequest,
    DomainDrainStatusRequest as RemoteDomainDrainStatusRequest,
    DomainDrainStatusResponse as RemoteDomainDrainStatusResponse,
    EntityDrainStatusRequest as RemoteEntityDrainStatusRequest,
    EntityDrainStatusResponse as RemoteEntityDrainStatusResponse,
    EntityGateReleaseRequest as RemoteEntityGateReleaseRequest,
    EntityGateReleaseResponse as RemoteEntityGateReleaseResponse,
    EntityGateRequest as RemoteEntityGateRequest, EntityGateResponse as RemoteEntityGateResponse,
    Envelope, LookupRequest as RemoteLookupRequest, LookupResponse as RemoteLookupResponse,
    MAX_CONCURRENT_HEALTH_PROBES, OwnershipHandoffFailure, PeerTarget,
    PrepareForcedOwnershipRecoveryRequest as RemotePrepareForcedOwnershipRecoveryRequest,
    RemoteOperationFailure, RemoteOperationSubject, RuntimeErrorEvent as RemoteRuntimeErrorEvent,
    StateSyncRequest as RemoteStateSyncRequest, StateSyncResponse as RemoteStateSyncResponse,
    StreamHandlerError, StreamingResponse,
    SubscriptionInterestVisibilityRequest as RemoteSubscriptionInterestVisibilityRequest,
    SubscriptionInterestVisibilityResponse as RemoteSubscriptionInterestVisibilityResponse,
    Transport,
};
use nervix_models::{ClusterNodeName, DomainName, DomainStatus, ModelKind, UserName};
use observability_http::serve_observability_http;
use ownership_handoff::{FORCED_OWNERSHIP_RECOVERY_BUDGET, ForcedOwnershipRecoveryCoordinator};
use parking_lot::RwLock;
use peer_grpc::grpc_base_url;
use scheduling::{
    KafkaPartitionWatcherKey, KafkaPartitionWatcherTask, LEADER_KAFKA_PARTITION_WATCH_INTERVAL,
};
use session_service::{
    SESSION_EVENT_CAPACITY, SessionEvents, SessionServiceImpl, SessionServiceInner,
    apply_current_cluster_runtime_state,
};
use startup::ApplicationStartup;
use tls::{
    InterconnectTlsPaths, load_grpc_tls_server_config, load_web_console_tls_server_config,
    reload_interconnect_tls,
};
use tokio::{
    net::TcpListener,
    sync::broadcast,
    time::{Duration, sleep},
};
use transaction::{
    DEFAULT_TRANSACTION_IDLE_TIMEOUT, DEFAULT_TRANSACTION_MAX_OPEN,
    DEFAULT_TRANSACTION_MAX_SOURCE_BYTES, DEFAULT_TRANSACTION_MAX_STATEMENTS,
    DEFAULT_TRANSACTION_TOMBSTONE_RETENTION,
};
use web_console::{serve_web_console_http, serve_web_console_https, web_console_advertise_url};

use crate::{
    metrics::NodeObservations,
    registry::Registry,
    resource_interconnect::{
        FetchResourceArchive, PublishResourceReplica, ResourceInterconnectError,
    },
    runtime::{DescribeStateSnapshot, FetchStateSnapshot},
};

mod admitted_connection;
mod authentication;
mod background_task;
mod cluster_status;
mod command_execution;
mod completion;
mod describe_output;
mod domain_clock;
mod domain_lifecycle;
mod entity_gate;
mod error;
mod http_endpoint;
mod interconnect_relay;
mod model_mutation;
mod model_validation;
mod observability_http;
mod observation;
mod ownership_handoff;
mod peer_grpc;
mod relocation;
mod resource;
mod runtime_admission;
mod schedule_planning;
mod scheduling;
mod service_tasks;
mod session_service;
mod shutdown;
mod startup;
mod subscription;
mod termination_signals;
#[cfg(test)]
pub(crate) mod test_fixtures;
mod tls;
mod tracing_setup;
mod transaction;
mod web_console;

use service_tasks::ServiceTasks;
use shutdown::BeforeDeadline;
pub use shutdown::{
    ShutdownCoordinator, ShutdownDeadline, ShutdownOutcome, ShutdownPhase, ShutdownPhaseOutcome,
    ShutdownRequest, ShutdownRequestOutcome,
};
use tonic::transport::Server;
use tracing::{debug, error, info, warn};
use triomphe::Arc;
use typed_builder::TypedBuilder;

use crate::{
    ConfiguredFaultInjection, cluster,
    memory_pressure::{MemoryPressureConfig, MemoryPressureController},
    proto::session_service_server::SessionServiceServer,
    resource::{ResourceStore, ResourceStoreLimits},
    runtime::{
        EntityGateLease, OwnershipHandoffError, OwnershipHandoffResult, Runtime, RuntimeEvent,
    },
};

#[cfg(not(test))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(test)]
#[global_allocator]
static GLOBAL: alloc_count::AllocCounter<tikv_jemallocator::Jemalloc> =
    alloc_count::AllocCounter(tikv_jemallocator::Jemalloc);

#[derive(Parser, Debug, Clone)]
#[command(name = "nervix-server")]
#[command(about = "NSPL gRPC server")]
pub struct Args {
    #[arg(long, env = "NERVIX_ADDR", default_value = "127.0.0.1:47391")]
    pub addr: String,
    #[arg(long, env = "NERVIX_GRPC_MODE", value_enum, default_value_t = InternalTransportMode::Http)]
    pub grpc_mode: InternalTransportMode,
    #[arg(long, env = "NERVIX_GRPC_HTTPS_LISTEN_ADDR")]
    pub grpc_https_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_GRPC_HTTPS_ADVERTISE_ADDR")]
    pub grpc_https_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_HTTP_LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    pub http_listen_addr: String,
    #[arg(long, env = "NERVIX_HTTPS_LISTEN_ADDR", default_value = "0.0.0.0:8443")]
    pub https_listen_addr: String,
    #[arg(
        long,
        env = "NERVIX_OBSERVABILITY_LISTEN_ADDR",
        default_value = "0.0.0.0:9090"
    )]
    pub observability_listen_addr: String,
    #[arg(
        long,
        env = "NERVIX_WEB_CONSOLE_LISTEN_ADDR",
        default_value = "0.0.0.0:47420"
    )]
    pub web_console_listen_addr: String,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_ADVERTISE_ADDR")]
    pub web_console_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_HTTPS_LISTEN_ADDR")]
    pub web_console_https_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_TLS_CERT")]
    pub web_console_tls_cert: Option<PathBuf>,
    #[arg(long, env = "NERVIX_WEB_CONSOLE_TLS_KEY")]
    pub web_console_tls_key: Option<PathBuf>,
    #[arg(long, env = "NERVIX_CLUSTER_ID", default_value = "default")]
    pub cluster_id: String,
    #[arg(long, env = "NERVIX_NODE_ID")]
    pub node_id: ClusterNodeName,
    #[arg(long, env = "NERVIX_GRPC_ADVERTISE_ADDR")]
    pub grpc_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_LISTEN_ADDR")]
    pub interconnect_listen_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_ADVERTISE_ADDR")]
    pub interconnect_advertise_addr: Option<String>,
    #[arg(long, env = "NERVIX_INTERCONNECT_TLS_CA")]
    pub interconnect_tls_ca: PathBuf,
    #[arg(long, env = "NERVIX_INTERCONNECT_TLS_CERT")]
    pub interconnect_tls_cert: PathBuf,
    #[arg(long, env = "NERVIX_INTERCONNECT_TLS_KEY")]
    pub interconnect_tls_key: PathBuf,
    #[arg(long, env = "NERVIX_ALLOW_BOOTSTRAP", default_value_t = false)]
    pub allow_bootstrap: bool,
    #[arg(long, env = "NERVIX_DEFAULT_USER", default_value = DEFAULT_USER)]
    pub default_user: String,
    #[arg(
        long,
        env = "NERVIX_INIT_DEFAULT_USER_PASSWORD",
        hide_env_values = true
    )]
    pub init_default_user_password: Option<String>,
    #[arg(
        long,
        env = "NERVIX_NODE_UNAVAILABILITY_TIMEOUT",
        default_value = "10s",
        value_parser = parse_human_duration
    )]
    pub node_unavailability_timeout: Duration,
    #[arg(
        long,
        env = "NERVIX_RAFT_HEARTBEAT_INTERVAL",
        default_value = "250ms",
        value_parser = parse_human_duration
    )]
    pub raft_heartbeat_interval: Duration,
    #[arg(
        long,
        env = "NERVIX_RAFT_ELECTION_TIMEOUT_MIN",
        default_value = "1500ms",
        value_parser = parse_human_duration
    )]
    pub raft_election_timeout_min: Duration,
    #[arg(
        long,
        env = "NERVIX_RAFT_ELECTION_TIMEOUT_MAX",
        default_value = "3000ms",
        value_parser = parse_human_duration
    )]
    pub raft_election_timeout_max: Duration,
    #[arg(
        long,
        env = "NERVIX_RAFT_SNAPSHOT_ENTRY_THRESHOLD",
        default_value = "10000"
    )]
    pub raft_snapshot_entry_threshold: u64,
    #[arg(
        long,
        env = "NERVIX_RAFT_SNAPSHOT_BYTE_THRESHOLD",
        default_value = "64MiB",
        value_parser = parse_human_bytes,
    )]
    pub raft_snapshot_byte_threshold: ubyte::ByteUnit,
    #[arg(
        long,
        env = "NERVIX_RAFT_COVERED_LOG_ENTRIES_RETAINED",
        default_value = "1000"
    )]
    pub raft_covered_log_entries_retained: u64,
    #[arg(
        long,
        env = "NERVIX_RAFT_COVERED_LOG_BYTES_RETAINED",
        default_value = "64MiB",
        value_parser = parse_human_bytes,
    )]
    pub raft_covered_log_bytes_retained: ubyte::ByteUnit,
    #[arg(
        long,
        env = "NERVIX_RAFT_RETAINED_LOG_CAP",
        default_value = "1GiB",
        value_parser = parse_human_bytes,
    )]
    pub raft_retained_log_cap: ubyte::ByteUnit,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_IDLE_TIMEOUT",
        default_value = "15m",
        value_parser = parse_human_duration
    )]
    pub transaction_idle_timeout: Duration,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_TOMBSTONE_RETENTION",
        default_value = "15m",
        value_parser = parse_human_duration
    )]
    pub transaction_tombstone_retention: Duration,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_MAX_STATEMENTS",
        default_value_t = DEFAULT_TRANSACTION_MAX_STATEMENTS
    )]
    pub transaction_max_statements: usize,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_MAX_SOURCE_BYTES",
        default_value_t = DEFAULT_TRANSACTION_MAX_SOURCE_BYTES
    )]
    pub transaction_max_source_bytes: u64,
    #[arg(
        long,
        env = "NERVIX_TRANSACTION_MAX_OPEN",
        default_value_t = DEFAULT_TRANSACTION_MAX_OPEN
    )]
    pub transaction_max_open: usize,
    #[arg(long, env = "NERVIX_REPLICA_COUNT", default_value_t = 0)]
    pub replica_count: usize,
    #[arg(
        long,
        env = "NERVIX_STATE_SNAPSHOT_INTERVAL",
        default_value = "30s",
        value_parser = parse_human_duration
    )]
    pub state_snapshot_interval: Duration,
    #[arg(
        long,
        env = "NERVIX_MEMORY_HIGH_WATERMARK",
        value_parser = parse_human_bytes,
        help = "Allocated jemalloc bytes that pause all ingestors"
    )]
    pub memory_high_watermark: Option<ubyte::ByteUnit>,
    #[arg(
        long,
        env = "NERVIX_MEMORY_LOW_WATERMARK",
        value_parser = parse_human_bytes,
        help = "Allocated jemalloc bytes that allow paused ingestors to resume"
    )]
    pub memory_low_watermark: Option<ubyte::ByteUnit>,
    #[arg(
        long,
        env = "NERVIX_MEMORY_PRESSURE_CHECK_INTERVAL",
        default_value = "500ms",
        value_parser = parse_human_duration,
        help = "Interval between jemalloc memory pressure checks"
    )]
    pub memory_pressure_check_interval: Duration,
    #[arg(
        long,
        env = "NERVIX_MEMORY_PRESSURE_RESUME_JITTER",
        default_value = "1s",
        value_parser = parse_human_duration,
        help = "Maximum jitter before each paused ingestor resume attempt"
    )]
    pub memory_pressure_resume_jitter: Duration,
    #[arg(
        long,
        env = "NERVIX_DRAIN_TIMEOUT",
        default_value = "30s",
        value_parser = parse_human_duration,
        help = "Maximum time to wait for drain operations before continuing"
    )]
    pub drain_timeout: Duration,
    #[arg(
        long,
        env = "NERVIX_SHUTDOWN_TIMEOUT",
        default_value = "50s",
        value_parser = parse_human_duration,
        help = "Maximum time from the first stop request until the process exits"
    )]
    pub shutdown_timeout: Duration,
    #[arg(long, env = "NERVIX_CLUSTER_BOOTSTRAP_HOST")]
    pub cluster_bootstrap_host: Option<String>,
    #[arg(long, env = "NERVIX_DB_PATH", default_value = "./.nervix-db")]
    pub db_path: String,
    #[arg(
        long,
        env = "NERVIX_TEMP_DIR",
        default_value = crate::runtime::DEFAULT_TEMP_DIR,
        help = "Directory used for local temporary files such as Iceberg emitter staging"
    )]
    pub temp_dir: PathBuf,
    #[arg(
        long,
        env = "NERVIX_RESOURCE_MAX_ARCHIVE_BYTES",
        default_value = "4GiB",
        value_parser = parse_human_bytes,
        help = "Maximum staged archive bytes for one resource version"
    )]
    pub resource_max_archive_bytes: ubyte::ByteUnit,
    #[arg(
        long,
        env = "NERVIX_RESOURCE_MAX_EXTRACTED_BYTES",
        default_value = "16GiB",
        value_parser = parse_human_bytes,
        help = "Maximum extracted bytes for one resource version"
    )]
    pub resource_max_extracted_bytes: ubyte::ByteUnit,
    #[arg(
        long,
        env = "NERVIX_RESOURCE_MAX_FILE_COUNT",
        default_value_t = 1_000_000,
        help = "Maximum extracted files for one resource version"
    )]
    pub resource_max_file_count: u64,
    #[arg(
        long,
        env = "NERVIX_OTEL_ENABLED",
        default_value_t = false,
        help = "Enable optional OpenTelemetry OTLP trace export"
    )]
    pub otel_enabled: bool,
    #[arg(
        long,
        env = "NERVIX_OTEL_OTLP_ENDPOINT",
        default_value = "http://127.0.0.1:4317",
        help = "OpenTelemetry OTLP gRPC endpoint used when trace export is enabled"
    )]
    pub otel_otlp_endpoint: String,
    #[arg(
        long,
        env = "NERVIX_OTEL_SERVICE_NAME",
        default_value = "nervix",
        help = "OpenTelemetry service name used when trace export is enabled"
    )]
    pub otel_service_name: String,
    #[arg(
        long,
        env = "NERVIX_OTEL_TRACE_SAMPLE_RATIO",
        default_value_t = 1.0,
        value_parser = parse_trace_sample_ratio,
        help = "OpenTelemetry parent-based trace sample ratio used when trace export is enabled"
    )]
    pub otel_trace_sample_ratio: f64,
    #[command(subcommand)]
    pub subcommand: Option<Command>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Generate shell completion scripts
    Completions {
        /// Target shell
        shell: Shell,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lower")]
pub enum InternalTransportMode {
    Http,
    Https,
}

impl InternalTransportMode {
    fn scheme(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    fn is_tls(self) -> bool {
        matches!(self, Self::Https)
    }
}

#[derive(Debug, Clone, TypedBuilder)]
pub struct Application {
    pub addr: SocketAddr,
    #[builder(default = InternalTransportMode::Http)]
    pub grpc_mode: InternalTransportMode,
    #[builder(default)]
    pub grpc_https_listen_addr: Option<SocketAddr>,
    #[builder(default)]
    pub grpc_https_advertise_addr: Option<cluster::HostPort>,
    pub http_listen_addr: SocketAddr,
    pub https_listen_addr: SocketAddr,
    pub observability_listen_addr: SocketAddr,
    #[builder(default = SocketAddr::from(([127, 0, 0, 1], 0)))]
    pub web_console_listen_addr: SocketAddr,
    #[builder(default)]
    pub web_console_advertise_addr: Option<cluster::HostPort>,
    #[builder(default)]
    pub web_console_https_listen_addr: Option<SocketAddr>,
    #[builder(default)]
    pub web_console_tls_cert: Option<PathBuf>,
    #[builder(default)]
    pub web_console_tls_key: Option<PathBuf>,
    pub cluster_id: String,
    pub node_id: ClusterNodeName,
    pub grpc_advertise_addr: cluster::HostPort,
    pub interconnect_listen_addr: SocketAddr,
    pub interconnect_advertise_addr: cluster::HostPort,
    pub interconnect_tls_ca: PathBuf,
    pub interconnect_tls_cert: PathBuf,
    pub interconnect_tls_key: PathBuf,
    pub allow_bootstrap: bool,
    #[builder(default = DEFAULT_USER.to_string())]
    pub default_user: String,
    #[builder(default)]
    pub init_default_user_password: Option<String>,
    pub node_unavailability_timeout: Duration,
    pub raft_heartbeat_interval: Duration,
    pub raft_election_timeout_min: Duration,
    pub raft_election_timeout_max: Duration,
    #[builder(default)]
    pub raft_retention: RaftRetentionPolicy,
    #[builder(default = DEFAULT_TRANSACTION_IDLE_TIMEOUT)]
    pub transaction_idle_timeout: Duration,
    #[builder(default = DEFAULT_TRANSACTION_TOMBSTONE_RETENTION)]
    pub transaction_tombstone_retention: Duration,
    #[builder(default = DEFAULT_TRANSACTION_MAX_STATEMENTS)]
    pub transaction_max_statements: usize,
    #[builder(default = DEFAULT_TRANSACTION_MAX_SOURCE_BYTES)]
    pub transaction_max_source_bytes: u64,
    #[builder(default = DEFAULT_TRANSACTION_MAX_OPEN)]
    pub transaction_max_open: usize,
    #[builder(default = 0)]
    pub replica_count: usize,
    #[builder(default = Duration::from_secs(30))]
    pub state_snapshot_interval: Duration,
    #[builder(default)]
    pub memory_pressure: Option<MemoryPressureConfig>,
    pub cluster_bootstrap_host: Option<String>,
    pub db_path: PathBuf,
    #[builder(default = PathBuf::from(crate::runtime::DEFAULT_TEMP_DIR))]
    pub temp_dir: PathBuf,
    #[builder(default)]
    pub resource_store_limits: ResourceStoreLimits,
    #[builder(default)]
    #[doc(hidden)]
    pub fault_injection: ConfiguredFaultInjection,
    #[builder(default)]
    pub shutdown: ShutdownCoordinator,
    #[builder(default = true)]
    pub graceful_shutdown_drain: bool,
    #[builder(default = shutdown::DEFAULT_DRAIN_TIMEOUT)]
    pub drain_timeout: Duration,
}

fn parse_human_duration(input: &str) -> Result<Duration, String> {
    humantime::parse_duration(input).map_err(|err| err.to_string())
}

fn parse_human_bytes(input: &str) -> Result<ubyte::ByteUnit, String> {
    input
        .parse::<ubyte::ByteUnit>()
        .map_err(|err| err.to_string())
}

fn parse_trace_sample_ratio(input: &str) -> Result<f64, String> {
    let ratio = input
        .parse::<f64>()
        .map_err(|err| format!("invalid trace sample ratio: {err}"))?;
    if (0.0..=1.0).contains(&ratio) {
        Ok(ratio)
    } else {
        Err("trace sample ratio must be between 0.0 and 1.0".to_string())
    }
}

#[cfg(test)]
fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").assured("writing a byte into a String cannot fail");
    }
    out
}

/// Runs the server command line with termination signals the caller has already registered, so
/// neither signal can end the process by its default action while the application starts.
pub async fn run_cli(
    args: Args,
    termination_signals: TerminationSignals,
) -> Result<(), Report<AppError>> {
    if let Some(Command::Completions { shell }) = args.subcommand.clone() {
        print_completions(shell);
        return Ok(());
    }

    let application = Application::try_from(args)?;
    termination_signals.supervise(application.shutdown.clone())?;
    application.run().await
}

impl Application {
    pub async fn run(self) -> Result<(), Report<AppError>> {
        let addr = self.addr;
        let grpc_mode = self.grpc_mode;
        let grpc_listen_addr = match grpc_mode {
            InternalTransportMode::Http => addr,
            InternalTransportMode::Https => self
                .grpc_https_listen_addr
                .ok_or_else(|| Report::new(AppError::MissingGrpcHttpsListenAddress))?,
        };
        let http_listen_addr = self.http_listen_addr;
        let https_listen_addr = self.https_listen_addr;
        let observability_listen_addr = self.observability_listen_addr;
        let web_console_listen_addr = self.web_console_listen_addr;
        let web_console_https_listen_addr = self.web_console_https_listen_addr;
        let web_console_advertise_url = web_console_advertise_url(
            self.web_console_advertise_addr,
            web_console_listen_addr,
            web_console_https_listen_addr,
        );
        let graceful_shutdown_drain = self.graceful_shutdown_drain;
        let drain_timeout = self.drain_timeout;
        let cluster_id = self.cluster_id.clone();
        let node_id = self.node_id.clone();
        let grpc_advertise_addr = match grpc_mode {
            InternalTransportMode::Http => self.grpc_advertise_addr,
            InternalTransportMode::Https => self
                .grpc_https_advertise_addr
                .ok_or_else(|| Report::new(AppError::MissingGrpcHttpsAdvertiseAddress))?,
        };
        let grpc_advertise_url = grpc_base_url(grpc_mode, &grpc_advertise_addr);
        let interconnect_listen_addr = self.interconnect_listen_addr;
        let interconnect_advertise_addr = self.interconnect_advertise_addr.clone();
        let interconnect_tls_paths = InterconnectTlsPaths {
            ca: self.interconnect_tls_ca.clone(),
            certificate: self.interconnect_tls_cert.clone(),
            private_key: self.interconnect_tls_key.clone(),
        };
        let allow_bootstrap = self.allow_bootstrap;
        let cluster_bootstrap_host = self.cluster_bootstrap_host.clone();
        let default_user = self.default_user.clone();
        let init_default_user_password = self.init_default_user_password.clone();
        let configured_basic_auth =
            init_default_user_password
                .as_ref()
                .map(|password| BasicAuthCredentials {
                    username: default_user.clone(),
                    password: password.clone(),
                });
        let node_unavailability_timeout = self.node_unavailability_timeout;
        let raft_heartbeat_interval = self.raft_heartbeat_interval;
        let raft_election_timeout_min = self.raft_election_timeout_min;
        let raft_election_timeout_max = self.raft_election_timeout_max;
        let raft_retention = self.raft_retention;
        let transaction_idle_timeout = self.transaction_idle_timeout;
        let transaction_tombstone_retention = self.transaction_tombstone_retention;
        let transaction_max_statements = self.transaction_max_statements;
        let transaction_max_source_bytes = self.transaction_max_source_bytes;
        let transaction_max_open = self.transaction_max_open;
        let replica_count = self.replica_count;
        let state_snapshot_interval = self.state_snapshot_interval;
        let memory_pressure_controller = self
            .memory_pressure
            .map(MemoryPressureController::new)
            .transpose()
            .map_err(|error| {
                error!(?error, "failed to initialize memory pressure monitor");
                Report::new(AppError::InitMemoryPressureMonitor).attach_printable(error)
            })?;
        let db_path = self.db_path.clone();
        let temp_dir = self.temp_dir.clone();
        let resource_store_limits = self.resource_store_limits;
        let shutdown_coordinator = self.shutdown.clone();
        let shutdown = shutdown_coordinator.drain_support_token();
        let fault_injection = self.fault_injection.clone();
        let grpc_tls_server_config = if grpc_mode.is_tls() {
            Some(load_grpc_tls_server_config().await.map_err(|error| {
                error!(?error, "failed to build grpc tls server config");
                error
            })?)
        } else {
            None
        };
        let web_console_tls_server_config =
            match (&self.web_console_tls_cert, &self.web_console_tls_key) {
                (Some(cert_path), Some(key_path)) => {
                    let Some(_) = web_console_https_listen_addr else {
                        return Err(Report::new(AppError::MissingWebConsoleHttpsListenAddress));
                    };
                    Some(
                        load_web_console_tls_server_config(cert_path, key_path).map_err(
                            |error| {
                                error!(?error, "failed to build web console tls server config");
                                error
                            },
                        )?,
                    )
                }
                (Some(_), None) => {
                    return Err(Report::new(AppError::MissingWebConsoleTlsPrivateKey));
                }
                (None, Some(_)) => {
                    return Err(Report::new(AppError::MissingWebConsoleTlsCertificate));
                }
                (None, None) => {
                    if web_console_https_listen_addr.is_some() {
                        return Err(Report::new(AppError::MissingWebConsoleTlsCertificate));
                    }
                    None
                }
            };
        let grpc_listener = TcpListener::bind(grpc_listen_addr)
            .await
            .change_context(AppError::BindGrpcListenAddress)?;
        let http_listener = TcpListener::bind(http_listen_addr)
            .await
            .change_context(AppError::BindHttpListenAddress)?;
        let https_listener = TcpListener::bind(https_listen_addr)
            .await
            .change_context(AppError::BindHttpsListenAddress)?;
        let observability_listener = TcpListener::bind(observability_listen_addr)
            .await
            .change_context(AppError::BindObservabilityListenAddress)?;
        let web_console_listener = TcpListener::bind(web_console_listen_addr)
            .await
            .change_context(AppError::BindWebConsoleListenAddress)?;
        let web_console_https_listener = match (
            web_console_https_listen_addr,
            web_console_tls_server_config.as_ref(),
        ) {
            (Some(addr), Some(_)) => Some(
                TcpListener::bind(addr)
                    .await
                    .change_context(AppError::BindWebConsoleHttpsListenAddress)?,
            ),
            _ => None,
        };
        let interconnect_tls_material = interconnect_tls_paths
            .read()
            .await
            .change_context(AppError::LoadInterconnectTls)?;
        let interconnect_tls_fingerprint = interconnect_tls_material.fingerprint();
        let interconnect_tls = interconnect_tls_material
            .tls_bundle()
            .change_context(AppError::LoadInterconnectTls)?;

        info!(
            grpc_mode = grpc_mode.scheme(),
            grpc_listen_addr = %grpc_listen_addr,
            grpc_advertise_addr = %grpc_advertise_url,
            http_listen_addr = %http_listen_addr,
            https_listen_addr = %https_listen_addr,
            observability_listen_addr = %observability_listen_addr,
            web_console_listen_addr = %web_console_listen_addr,
            interconnect_listen_addr = %interconnect_listen_addr,
            interconnect_advertise_addr = %interconnect_advertise_addr,
            allow_bootstrap,
            node_unavailability_timeout = ?node_unavailability_timeout,
            raft_heartbeat_interval = ?raft_heartbeat_interval,
            raft_election_timeout_min = ?raft_election_timeout_min,
            raft_election_timeout_max = ?raft_election_timeout_max,
            replica_count,
            state_snapshot_interval = ?state_snapshot_interval,
            cluster_id,
            %node_id,
            bootstrap = cluster_bootstrap_host.as_deref().unwrap_or(""),
            db_path = db_path.display().to_string(),
            temp_dir = temp_dir.display().to_string(),
            resource_max_archive_bytes = resource_store_limits.max_archive_bytes,
            resource_max_extracted_bytes = resource_store_limits.max_extracted_bytes,
            resource_max_file_count = resource_store_limits.max_file_count,
            "starting nervix server"
        );

        let executor = Executor::default();
        let db = ApplicationStartup::open_node_database(db_path.clone(), &executor).await?;
        let registry = Arc::new(
            match Registry::from_database(db.clone(), Some(db_path.as_path())) {
                Ok(registry) => registry,
                Err(err) => {
                    error!(db_path = db_path.display().to_string(), error = %err, "failed to open registry");
                    return Err(err.change_context(AppError::OpenRegistry));
                }
            },
        );
        let runtime = Runtime::with_persistence_and_temp_dir(
            executor,
            Some(db.clone()),
            state_snapshot_interval,
            fault_injection.clone(),
            temp_dir.clone(),
        )
        .map_err(|error| {
            error!(error = %error, "failed to initialize runtime persistence");
            Report::new(AppError::OpenRuntimeState)
        })?;
        let resource_store = StdArc::new(
            ResourceStore::open_with_limits(
                db_path.join("resources"),
                runtime.executor().clone(),
                resource_store_limits,
            ).map_err(|err| {
                error!(db_path = db_path.display().to_string(), error = %err, "failed to open resource store");
                Report::new(AppError::OpenResourceStore)
            })?,
        );
        resource_store.cleanup_abandoned_staging().await.map_err(|err| {
            error!(db_path = db_path.display().to_string(), error = %err, "failed to clean resource staging paths");
            Report::new(AppError::OpenResourceStore)
        })?;
        let mut startup = ApplicationStartup {
            db,
            consensus_path: ApplicationStartup::consensus_database_path(&db_path),
            resource_store,
            registry,
            runtime,
            consensus: None,
            interconnect: None,
        };
        startup
            .runtime
            .attach_resource_store(startup.resource_store.clone());
        info!(
            runtime_state_store_enabled = startup.runtime.has_state_store(),
            runtime_state_snapshot_interval = ?startup.runtime.state_snapshot_interval(),
            "initialized runtime persistence"
        );
        let startup_runtime_changes = match startup.registry.startup_runtime_changes() {
            Ok(changes) => changes,
            Err(error) => {
                let error = Report::new(AppError::ApplyStartupRuntime(error.to_string()));
                startup.terminate().await;
                return Err(error);
            }
        };
        for changes in startup_runtime_changes {
            if let Err(error) = startup.runtime.apply_changes(changes).await {
                error!(error = %error, "failed to apply startup runtime changes");
                let error = Report::new(AppError::ApplyStartupRuntime(error.to_string()));
                startup.terminate().await;
                return Err(error);
            }
        }
        let interconnect_result = Transport::bind(
            interconnect_listen_addr,
            interconnect_advertise_addr.host(),
            cluster_id.clone(),
            node_id.clone(),
            interconnect_tls,
            Default::default(),
            startup.runtime.executor().clone(),
        )
        .await
        .change_context(AppError::StartInterconnect);
        let (interconnect, interconnect_rx) = match interconnect_result {
            Ok(interconnect) => interconnect,
            Err(error) => {
                startup.terminate().await;
                return Err(error);
            }
        };
        startup.interconnect = Some(interconnect.clone());

        let consensus_settings = ConsensusSettings {
            cluster_name: cluster_id.clone(),
            node_id: node_id.clone(),
            interconnect_advertise_addr: interconnect_advertise_addr.to_string(),
            interconnect: interconnect.clone(),
            executor: startup.runtime.executor().clone(),
            raft_heartbeat_interval,
            raft_election_timeout_min,
            raft_election_timeout_max,
            raft_retention,
        };
        let consensus_result = startup
            .open_consensus(consensus_settings, &fault_injection)
            .await;
        let consensus = match consensus_result {
            Ok(consensus) => consensus,
            Err(error) => {
                startup.terminate().await;
                return Err(error);
            }
        };
        startup.consensus = Some(consensus);
        let cluster_result = cluster::start_cluster(cluster::ClusterSettings {
            cluster_id,
            node_id: node_id.clone(),
            grpc_listen_addr,
            grpc_advertise_addr: grpc_advertise_url.clone(),
            web_console_advertise_addr: web_console_advertise_url.clone(),
            interconnect_advertise_addr,
            bootstrap_host: cluster_bootstrap_host.clone(),
            interconnect: interconnect.clone(),
            node_unavailability_timeout,
        })
        .await
        .change_context(AppError::StartCluster);
        let cluster = match cluster_result {
            Ok(cluster) => cluster,
            Err(error) => {
                startup.terminate().await;
                return Err(error);
            }
        };
        let cluster = Arc::new(cluster);
        let local_health_identity = cluster.local_node_identity().await;
        #[cfg(feature = "testing")]
        let health_fault_injection = fault_injection.clone();
        let health_handler = interconnect.register_handler::<ApplicationHealthProbe, _, _>({
            move |context, _request| {
                let local_health_identity = local_health_identity.clone();
                #[cfg(feature = "testing")]
                let health_fault_injection = health_fault_injection.clone();
                async move {
                    #[cfg(feature = "testing")]
                    health_fault_injection
                        .pause_health_response_if_armed(
                            context.peer_node_id(),
                            local_health_identity.node_id(),
                        )
                        .await;
                    #[cfg(not(feature = "testing"))]
                    drop(context);
                    #[cfg(feature = "testing")]
                    let local_health_identity =
                        health_fault_injection.health_response_identity(local_health_identity);
                    local_health_identity
                }
            }
        });
        let startup = startup
            .require_handler_registration(&cluster, health_handler)
            .await?;
        let application_revision_handler =
            completion::register_application_revision_handler(cluster.clone(), &interconnect);
        let startup = startup
            .require_handler_registration(&cluster, application_revision_handler)
            .await?;
        let ApplicationStartup {
            db,
            consensus_path: _,
            resource_store,
            registry,
            runtime,
            consensus,
            interconnect,
        } = startup;
        let consensus =
            consensus.verified("startup assigns this handle before it reaches this point");
        let interconnect =
            interconnect.verified("startup assigns this handle before it reaches this point");
        let mut interconnect_rx = interconnect_rx;
        let runtime_admission = Arc::new(runtime_admission::RuntimeAdmission::new());
        runtime.attach_remote_dispatcher(cluster.clone(), interconnect.clone());
        let node_observations = NodeObservations::new(
            runtime.executor().clone(),
            interconnect.clone(),
            consensus.observer(),
        );
        runtime
            .metrics()
            .install_node_observations(node_observations.clone());
        let mut background_tasks = Vec::new();
        let scheduler_delay_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            node_observations
                .sample_scheduler_delay(scheduler_delay_shutdown)
                .await;
        }));
        #[cfg(feature = "testing")]
        fault_injection.register_bulk_executor(node_id.clone(), runtime.executor().clone());
        #[cfg(feature = "testing")]
        let scheduler_mode = runtime.scheduler_mode();

        let cluster_for_reconcile = cluster.clone();
        let consensus_for_reconcile = consensus.proposer();
        let registry_for_reconcile = registry.clone();
        let runtime_for_reconcile = runtime.clone();
        let interconnect_for_reconcile = interconnect.clone();
        let local_node_for_reconcile = node_id.clone();
        let reconcile_shutdown = shutdown.clone();
        let cluster_for_membership_reconcile = cluster.clone();
        let administrator_for_membership_reconcile = consensus.administrator();
        let membership_reconcile_shutdown = shutdown.clone();
        let interconnect_tls_transport = interconnect.clone();
        let interconnect_tls_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            reload_interconnect_tls(
                interconnect_tls_transport,
                interconnect_tls_paths,
                interconnect_tls_fingerprint,
                interconnect_tls_shutdown,
            )
            .await;
        }));
        if let Some(controller) = memory_pressure_controller {
            let memory_runtime = runtime.clone();
            let memory_shutdown = shutdown.clone();
            background_tasks.push(tokio::spawn(async move {
                controller.run(memory_runtime, memory_shutdown).await;
            }));
        }
        background_tasks.push(tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            let mut initialized = false;
            loop {
                tokio::task::consume_budget().await;
                if membership_reconcile_shutdown.is_cancelled() {
                    break;
                }
                if allow_bootstrap && !initialized {
                    match administrator_for_membership_reconcile
                        .maybe_initialize()
                        .await
                    {
                        Ok(did_initialize) => {
                            initialized = did_initialize;
                        }
                        Err(error) => {
                            warn!(%error, "raft bootstrap attempt failed");
                        }
                    }
                }
                if let Err(error) = administrator_for_membership_reconcile
                    .reconcile_nodes(cluster_for_membership_reconcile.gossip_state())
                    .await
                {
                    warn!(%error, "raft membership reconciliation failed");
                }
                tokio::select! {
                    _ = membership_reconcile_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        #[cfg(feature = "testing")]
        {
            let mut leadership_transfer_rx = runtime.subscribe_leadership_transfers();
            let consensus_for_leadership_transfer = consensus.administrator();
            let leadership_transfer_shutdown = shutdown.clone();
            let leadership_transfer_local_node_id = node_id.clone();
            background_tasks.push(tokio::spawn(async move {
                loop {
                    tokio::task::consume_budget().await;
                    tokio::select! {
                        _ = leadership_transfer_shutdown.cancelled() => break,
                        request = leadership_transfer_rx.recv() => {
                            match request {
                                Ok(request)
                                    if request.from_node_id == leadership_transfer_local_node_id =>
                                {
                                    if let Err(error) = consensus_for_leadership_transfer
                                        .transfer_leadership_to(request.to_node_id.clone())
                                        .await
                                    {
                                        warn!(
                                            from_node_id = %request.from_node_id,
                                            to_node_id = %request.to_node_id,
                                            error = %error,
                                            "test leadership transfer request failed"
                                        );
                                    }
                                }
                                Ok(_) => {}
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    warn!(
                                        skipped,
                                        "test leadership transfer request receiver lagged"
                                    );
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                    }
                }
            }));
        }
        background_tasks.push(tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            let mut default_user_resolved = false;
            let mut missing_init_default_user_password_warned = false;
            loop {
                tokio::task::consume_budget().await;
                // A pass waits on consensus writes and on requests to peers, and neither completes
                // once the peers have stopped. The pass therefore ends with drain support rather
                // than holding terminal teardown until the grace period aborts the whole task.
                let reconcile_pass = async {
                    if consensus_for_reconcile.current_leader().await.as_ref()
                        != Some(consensus_for_reconcile.local_node_id())
                    {
                        return;
                    }
                    let orphaned_alter_committing_domains = consensus_for_reconcile
                        .current_transactions()
                        .await
                        .into_values()
                        .filter(|transaction| {
                            matches!(transaction.state, TransactionState::Committing(_))
                        })
                        .map(|transaction| transaction.domain)
                        .collect::<HashSet<_>>();
                    for (domain, state) in consensus_for_reconcile.current_domains().await {
                        if let DomainStatus::Paused = state.status
                            && !orphaned_alter_committing_domains.contains(&domain)
                            && !runtime_for_reconcile.domain_alter_is_active(&domain)
                        {
                            match consensus_for_reconcile
                                .resume_domain(domain.clone(), None)
                                .await
                            {
                                Ok(()) => {
                                    info!(
                                        domain = domain.as_str(),
                                        "resumed orphaned ALTER pause after leadership \
                                         acquisition"
                                    );
                                }
                                Err(error) => {
                                    warn!(
                                        domain = domain.as_str(),
                                        error = %error,
                                        "failed to resume orphaned ALTER pause"
                                    );
                                }
                            }
                        }
                    }
                    if !default_user_resolved {
                        match UserName::parse(&default_user) {
                            Ok(default_user_id)
                                if consensus_for_reconcile
                                    .current_user(&default_user_id)
                                    .await
                                    .is_none() =>
                            {
                                if let Some(password) = init_default_user_password.clone() {
                                    match user_credentials(default_user_id.clone(), password).await {
                                        Ok(user) => {
                                            let user_name = user.name.clone();
                                            if let Err(error) =
                                                consensus_for_reconcile.create_user(user).await
                                            {
                                                warn!(
                                                    error = %error,
                                                    "failed to create configured default user"
                                                );
                                            } else {
                                                default_user_resolved = true;
                                                info!(
                                                    user = user_name.as_str(),
                                                    "created configured default user"
                                                );
                                            }
                                        }
                                        Err(error) => {
                                            warn!(
                                                error = %error,
                                                "failed to prepare configured default user"
                                            );
                                            default_user_resolved = true;
                                        }
                                    }
                                } else if !missing_init_default_user_password_warned {
                                    warn!(
                                        user = default_user.as_str(),
                                        "default user is not configured; set \
                                         --init-default-user-password or \
                                         NERVIX_INIT_DEFAULT_USER_PASSWORD before first startup"
                                    );
                                    missing_init_default_user_password_warned = true;
                                }
                            }
                            Ok(_) => {
                                default_user_resolved = true;
                            }
                            Err(_) => {
                                if !missing_init_default_user_password_warned {
                                    warn!(
                                        user = default_user.as_str(),
                                        "configured default user name is invalid"
                                    );
                                    missing_init_default_user_password_warned = true;
                                }
                            }
                        }
                    }
                    loop {
                        tokio::task::consume_budget().await;
                        let Ok(automatic_schedule_input) =
                            consensus_for_reconcile.automatic_schedule_input().await
                        else {
                            break;
                        };
                        let committing_domains = consensus_for_reconcile
                            .current_transactions()
                            .await
                            .into_values()
                            .filter(|transaction| {
                                matches!(transaction.state, TransactionState::Committing(_))
                            })
                            .map(|transaction| transaction.domain)
                            .collect::<HashSet<_>>();
                        let health_snapshot = cluster_for_reconcile.peer_health_snapshot();
                        let health_scheduling_revision = health_snapshot.scheduling_revision();
                        let mut scheduling_availability =
                            cluster_for_reconcile.gossip_state().await;
                        scheduling_availability
                            .dead_node_ids
                            .extend(health_snapshot.unavailable_nodes());
                        let live_node_ids = scheduling_availability.live_node_ids();
                        let placement_candidate_node_ids =
                            scheduling_availability.placement_candidate_node_ids();
                        let mut live_node_incarnations = BTreeMap::new();
                        for node in &scheduling_availability.live_nodes {
                            if scheduling_availability
                                .dead_node_ids
                                .contains(&node.node_id)
                            {
                                continue;
                            }
                            live_node_incarnations
                                .insert(node.node_id.clone(), node.incarnation);
                        }
                        let topology = automatic_schedule_input.topology();
                        let live_voters = live_node_ids
                            .into_iter()
                            .filter(|node| topology.voters().contains(node))
                            .collect::<Vec<_>>();
                        let schedulable_node_ids = placement_candidate_node_ids
                            .into_iter()
                            .filter(|node| {
                                topology.voters().contains(node)
                                    && !topology.cordoned().contains(node)
                            })
                            .collect::<Vec<_>>();
                        let current_schedule = &automatic_schedule_input.runtime_state().schedule;
                        let live_voter_set = live_voters.iter().cloned().collect::<BTreeSet<_>>();
                        let schedulable_node_set = schedulable_node_ids
                            .iter()
                            .cloned()
                            .collect::<BTreeSet<_>>();
                        let active_graphs = registry_for_reconcile.active_graphs();
                        let active_domains = active_graphs
                            .iter()
                            .map(|(domain, _)| domain.clone())
                            .collect::<HashSet<_>>();
                        let mut automatic_decision_selected = false;
                        let mut automatic_decision_published = false;
                        for domain_schedule in current_schedule.domains.values() {
                            tokio::task::consume_budget().await;
                            if active_domains.contains(&domain_schedule.domain)
                                || committing_domains.contains(&domain_schedule.domain)
                                || runtime_for_reconcile
                                    .domain_alter_is_active(&domain_schedule.domain)
                            {
                                continue;
                            }
                            let mut failover_schedule = domain_schedule.clone();
                            let failover_moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
                                &mut failover_schedule,
                                None,
                                &live_voter_set,
                                &schedulable_node_set,
                            );
                            if failover_moves.is_empty() {
                                continue;
                            }
                            for failover_move in &failover_moves {
                                if let Some(replica) = failover_move.promoted_replica.as_ref() {
                                    info!(
                                        domain = domain_schedule.domain.as_str(),
                                        node = failover_move.label,
                                        promoted_replica = %replica,
                                        "failover promoted live replica to primary"
                                    );
                                } else if let Some(fallback_node) =
                                    failover_move.fallback_node.as_ref()
                                {
                                    warn!(
                                        domain = domain_schedule.domain.as_str(),
                                        node = failover_move.label,
                                        %fallback_node,
                                        "failover found no live replica; moving scheduled node without \
                                         local replicated state"
                                    );
                                }
                            }
                            ForcedOwnershipRecoveryCoordinator {
                                runtime: &runtime_for_reconcile,
                                interconnect: &interconnect_for_reconcile,
                                local_node_id: &local_node_for_reconcile,
                                node_incarnations: &live_node_incarnations,
                            }
                            .prepare_schedule(domain_schedule, &mut failover_schedule)
                            .await;
                            automatic_decision_selected = true;
                            if !cluster_for_reconcile.peer_health_scheduling_revision_is_current(
                                health_scheduling_revision,
                            )
                            {
                                break;
                            }
                            let current_availability =
                                cluster_for_reconcile.availability_state().await;
                            if !schedule_planning::DomainSchedulePlanningSnapshot::same_eligibility(
                                &scheduling_availability,
                                &current_availability,
                                automatic_schedule_input.topology().voters(),
                            ) {
                                break;
                            }
                            match consensus_for_reconcile
                                .apply_automatic_domain_schedule(
                                    automatic_schedule_input.fence(),
                                    automatic_schedule_input
                                        .planning_inputs(&domain_schedule.domain)
                                        .cloned()
                                        .verified(
                                            "the automatic runtime snapshot and planning inputs \
                                             contain the same domains",
                                        ),
                                    Some(failover_schedule),
                                )
                                .await
                            {
                                Ok(()) => automatic_decision_published = true,
                                Err(error) => {
                                    warn!(%error, "failed to republish domain schedule after node failover");
                                }
                            }
                            break;
                        }
                        if !automatic_decision_selected {
                            for (domain, graph) in active_graphs {
                                tokio::task::consume_budget().await;
                                if committing_domains.contains(&domain)
                                    || runtime_for_reconcile.domain_alter_is_active(&domain)
                                {
                                    continue;
                                }
                                let Some(domain_state) =
                                    automatic_schedule_input.runtime_state().domains.get(&domain)
                                else {
                                    continue;
                                };
                                #[cfg(feature = "testing")]
                                let mut schedule = graph.schedule_for_domain_with_mode(
                                    &domain,
                                    &schedulable_node_ids,
                                    replica_count,
                                    domain_state.config.placement,
                                    scheduler_mode,
                                );
                                #[cfg(not(feature = "testing"))]
                                let mut schedule = graph.schedule_for_domain(
                                    &domain,
                                    &schedulable_node_ids,
                                    replica_count,
                                    domain_state.config.placement,
                                );
                                let current_domain = current_schedule.domain(&domain);
                                let mut failover_existing = current_domain.cloned();
                                if let Some(existing) = &mut failover_existing {
                                    let failover_moves =
                                        SessionServiceImpl::failover_unavailable_scheduled_nodes(
                                            existing,
                                            Some(&schedule),
                                            &live_voter_set,
                                            &schedulable_node_set,
                                        );
                                    for failover_move in &failover_moves {
                                        if let Some(replica) = failover_move.promoted_replica.as_ref() {
                                            info!(
                                                domain = domain.as_str(),
                                                node = failover_move.label,
                                                promoted_replica = %replica,
                                                "failover promoted live replica to primary"
                                            );
                                        } else if let Some(fallback_node) =
                                            failover_move.fallback_node.as_ref()
                                        {
                                            warn!(
                                                domain = domain.as_str(),
                                                node = failover_move.label,
                                                %fallback_node,
                                                "failover found no live replica; moving scheduled node \
                                                 without local replicated state"
                                            );
                                        }
                                    }
                                }
                                SessionServiceImpl::merge_existing_schedule_data(
                                    &mut schedule,
                                    failover_existing.as_ref(),
                                    &live_voters,
                                );
                                if current_domain == Some(&schedule) {
                                    continue;
                                }
                                if let Some(current_domain) = current_domain {
                                    ForcedOwnershipRecoveryCoordinator {
                                        runtime: &runtime_for_reconcile,
                                        interconnect: &interconnect_for_reconcile,
                                        local_node_id: &local_node_for_reconcile,
                                        node_incarnations: &live_node_incarnations,
                                    }
                                    .prepare_schedule(current_domain, &mut schedule)
                                    .await;
                                }
                                if !cluster_for_reconcile.peer_health_scheduling_revision_is_current(
                                    health_scheduling_revision,
                                )
                                {
                                    break;
                                }
                                let current_availability =
                                    cluster_for_reconcile.availability_state().await;
                                if !schedule_planning::DomainSchedulePlanningSnapshot::same_eligibility(
                                    &scheduling_availability,
                                    &current_availability,
                                    automatic_schedule_input.topology().voters(),
                                ) {
                                    break;
                                }
                                match consensus_for_reconcile
                                    .apply_automatic_domain_schedule(
                                        automatic_schedule_input.fence(),
                                        automatic_schedule_input
                                            .planning_inputs(&domain)
                                            .cloned()
                                            .verified(
                                                "the automatic runtime snapshot and planning \
                                                 inputs contain the same domains",
                                            ),
                                        Some(schedule),
                                    )
                                    .await
                                {
                                    Ok(()) => automatic_decision_published = true,
                                    Err(error) => {
                                        warn!(%error, "failed to republish domain schedule after membership or schedulability change");
                                    }
                                }
                                break;
                            }
                        }
                        if !automatic_decision_published {
                            break;
                        }
                    }
                };
                let Some(()) = reconcile_shutdown.run_until_cancelled(reconcile_pass).await else {
                    break;
                };
                tokio::select! {
                    _ = reconcile_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        let interconnect_for_health = interconnect.clone();
        let cluster_for_health = cluster.clone();
        let mut health_topology = cluster.subscribe_live_node_states().await;
        let local_node_id = node_id;
        let mut awaiting_initial_bootstrap_peer = cluster_bootstrap_host.is_some();
        let health_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            loop {
                tokio::task::consume_budget().await;
                if health_shutdown.is_cancelled() {
                    break;
                }
                drop(health_topology.borrow_and_update());
                let gossip = cluster_for_health.gossip_state().await;
                let live_node_ids = gossip
                    .live_nodes
                    .iter()
                    .map(|node| node.node_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                let peer_nodes = gossip
                    .live_nodes
                    .into_iter()
                    .filter(|node| node.node_id != local_node_id)
                    .collect::<Vec<_>>();
                let health_endpoints = peer_nodes.iter().map(|node| {
                    cluster::PeerHealthEndpoint::new(
                        node.identity(),
                        node.interconnect_advertise_addr.clone(),
                    )
                });
                let health_targets = cluster_for_health
                    .replace_peer_health_endpoints(health_endpoints)
                    .into_iter()
                    .map(|target| (target.node_id().clone(), target))
                    .collect::<BTreeMap<_, _>>();

                let mut outbound_targets = BTreeMap::new();
                let mut scheduled_probes = Vec::new();
                let mut topology_changed = false;
                for node in peer_nodes {
                    tokio::task::consume_budget().await;
                    let Some(health_target) = health_targets.get(&node.node_id).cloned() else {
                        continue;
                    };
                    let Ok(target_addr) = node
                        .interconnect_advertise_addr
                        .parse::<cluster::HostPort>()
                    else {
                        cluster_for_health
                            .record_peer_health_result(cluster::PeerHealthProbeResult::new(
                                health_target,
                                cluster::PeerHealthProbeOutcome::Unscheduled,
                                std::time::Instant::now(),
                            ))
                            .await;
                        continue;
                    };
                    let resolution = tokio::select! {
                        _ = health_shutdown.cancelled() => return,
                        changed = health_topology.changed() => {
                            changed.assured(
                                "the cluster handle retains its Chitchat state sender for the server lifetime",
                            );
                            topology_changed = true;
                            break;
                        }
                        resolution = target_addr.resolve_all() => resolution,
                    };
                    let targets = match resolution {
                        Ok(addrs) => addrs
                            .into_iter()
                            .map(|addr| PeerTarget::new(addr, target_addr.host()))
                            .collect::<BTreeSet<_>>(),
                        Err(_) => {
                            cluster_for_health
                                .record_peer_health_result(cluster::PeerHealthProbeResult::new(
                                    health_target,
                                    cluster::PeerHealthProbeOutcome::Unscheduled,
                                    std::time::Instant::now(),
                                ))
                                .await;
                            continue;
                        }
                    };
                    outbound_targets.insert(node.node_id, targets);
                    scheduled_probes.push(health_target);
                }
                if topology_changed {
                    continue;
                }
                if !outbound_targets.is_empty() {
                    awaiting_initial_bootstrap_peer = false;
                }
                if !awaiting_initial_bootstrap_peer {
                    interconnect_for_health.replace_live_nodes(&live_node_ids);
                    interconnect_for_health.replace_outbound_targets(&outbound_targets);
                }

                let probe_results = stream::iter(scheduled_probes.into_iter().map(|target| {
                    let interconnect = interconnect_for_health.clone();
                    async move {
                        let outcome = match interconnect
                            .request(target.node_id(), ApplicationHealthProbe)
                            .await
                        {
                            Ok(identity) if &identity == target.identity() => {
                                cluster::PeerHealthProbeOutcome::Healthy(identity)
                            }
                            Ok(_) => cluster::PeerHealthProbeOutcome::Failure,
                            Err(error) if error.current_context().is_capacity_exhaustion() => {
                                cluster::PeerHealthProbeOutcome::CapacityExhausted
                            }
                            Err(_) => cluster::PeerHealthProbeOutcome::Failure,
                        };
                        cluster::PeerHealthProbeResult::new(
                            target,
                            outcome,
                            std::time::Instant::now(),
                        )
                    }
                }))
                .buffer_unordered(MAX_CONCURRENT_HEALTH_PROBES);
                tokio::pin!(probe_results);
                let topology_changed = loop {
                    tokio::task::consume_budget().await;
                    tokio::select! {
                        _ = health_shutdown.cancelled() => return,
                        changed = health_topology.changed() => {
                            changed.assured(
                                "the cluster handle retains its Chitchat state sender for the server lifetime",
                            );
                            break true;
                        }
                        result = probe_results.next() => match result {
                            Some(result) => {
                                cluster_for_health.record_peer_health_result(result).await;
                            }
                            None => break false,
                        }
                    }
                };
                if topology_changed {
                    continue;
                }
                tokio::select! {
                    _ = health_shutdown.cancelled() => break,
                    changed = health_topology.changed() => {
                        changed.assured(
                            "the cluster handle retains its Chitchat state sender for the server lifetime",
                        );
                    }
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        background_tasks.push(completion::spawn_authoritative_revision_reporting(
            cluster.clone(),
            consensus.observer(),
            shutdown.clone(),
        ));

        let runtime_for_schedule = runtime.clone();
        let registry_for_schedule = registry.clone();
        let mut schedule_rx = consensus.observer().subscribe_schedule();
        let consensus_for_schedule = consensus.observer();
        let cluster_for_schedule = cluster.clone();
        let interconnect_for_schedule = interconnect.clone();
        let schedule_shutdown = shutdown.clone();
        let schedule_runtime_admission = runtime_admission.clone();
        background_tasks.push(tokio::spawn(async move {
            if let Err(error) = apply_current_cluster_runtime_state(
                &runtime_for_schedule,
                &cluster_for_schedule,
                &interconnect_for_schedule,
                &registry_for_schedule,
                &consensus_for_schedule,
                &schedule_runtime_admission,
                &schedule_shutdown,
            )
            .await
            {
                warn!(error = %error, "failed to apply initial cluster schedule");
            }
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = schedule_shutdown.cancelled() => break,
                    changed = schedule_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        if let Err(error) = apply_current_cluster_runtime_state(
                            &runtime_for_schedule,
                            &cluster_for_schedule,
                            &interconnect_for_schedule,
                            &registry_for_schedule,
                            &consensus_for_schedule,
                            &schedule_runtime_admission,
                            &schedule_shutdown,
                        )
                        .await
                        {
                            warn!(error = %error, "failed to apply updated cluster schedule");
                        }
                    }
                }
            }
        }));
        let events = SessionEvents::new(SESSION_EVENT_CAPACITY);
        let service = SessionServiceImpl {
            inner: Arc::new(SessionServiceInner {
                cluster: cluster.clone(),
                consensus: consensus.proposer(),
                consensus_administrator: consensus.administrator(),
                registry,
                resource_store,
                http_tls_server_config: Arc::new(RwLock::new(None)),
                runtime: runtime.clone(),
                runtime_admission: runtime_admission.clone(),
                replica_count,
                admission_shutdown: shutdown_coordinator.admission_token(),
                drain_support_shutdown: shutdown.clone(),
                events: events.clone(),
                subscription_interest_counts: DashMap::with_hasher(RandomState::new()),
                interconnect: interconnect.clone(),
                service_tasks: ServiceTasks::default(),
                configured_basic_auth,
                auth_rate_limiter: SessionServiceImpl::new_auth_rate_limiter(),
                failed_auth_rate_limit_keys: DashMap::with_hasher(RandomState::new()),
                transaction_idle_timeout,
                transaction_tombstone_retention,
                transaction_max_statements,
                transaction_max_source_bytes,
                transaction_max_open,
                transaction_bindings: DashMap::with_hasher(RandomState::new()),
                command_executions: DashMap::with_hasher(RandomState::new()),
                transaction_executions: DashMap::with_hasher(RandomState::new()),
                ownership_handoff_operations: tokio::sync::Mutex::new(()),
                resource_upload_executions: DashMap::with_hasher(RandomState::new()),
                resource_replication_executions: DashMap::with_hasher(RandomState::new()),
            }),
        };
        let domain_clock_progress_service = service.clone();
        interconnect
            .register_handler::<RemoteDomainClockProgressRequest, _, _>(move |context, request| {
                let service = domain_clock_progress_service.clone();
                async move {
                    service.handle_domain_clock_progress(context.peer_node_id(), request);
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let resource_archive_service = service.clone();
        interconnect
            .register_stream_handler::<FetchResourceArchive, _, _>(move |_context, request| {
                let service = resource_archive_service.clone();
                async move {
                    let reader = service
                        .inner
                        .resource_store
                        .open_archive(&request.id)
                        .await
                        .map_err(|error| StreamHandlerError::new(error.to_string()))?;
                    let archive_bytes = reader.archive_bytes();
                    let chunks = stream::unfold(Some(reader), |reader| async move {
                        let mut reader = reader?;
                        match reader.next_chunk().await {
                            Ok(Some(chunk)) => Some((Ok(chunk), Some(reader))),
                            Ok(None) => None,
                            Err(error) => {
                                Some((Err(StreamHandlerError::new(error.to_string())), None))
                            }
                        }
                    });
                    Ok(StreamingResponse::new(archive_bytes, chunks))
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let resource_replica_service = service.clone();
        interconnect
            .register_handler::<PublishResourceReplica, _, _>(move |context, request| {
                let service = resource_replica_service.clone();
                async move {
                    if request.replica.key.node.node_id() != context.peer_node_id() {
                        return Err(ResourceInterconnectError::ReplicaOrigin {
                            authenticated: context.peer_node_id().clone(),
                            declared: request.replica.key.node.node_id().clone(),
                        });
                    }
                    service
                        .inner
                        .consensus
                        .put_resource_replica(request.replica)
                        .await
                        .map_err(ResourceInterconnectError::replica_publish)
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let describe_ingestor_service = service.clone();
        interconnect
            .register_handler::<RemoteDescribeIngestorRequest, _, _>(move |_context, request| {
                let service = describe_ingestor_service.clone();
                async move { service.handle_describe_ingestor_request(request).await }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let describe_snapshot_service = service.clone();
        interconnect
            .register_handler::<DescribeStateSnapshot, _, _>(move |_context, request| {
                let service = describe_snapshot_service.clone();
                async move {
                    let subject = RemoteOperationSubject::state(&request.placement);
                    match crate::runtime::RuntimeStatePlacement::from_remote(request.placement) {
                        Ok(placement) => {
                            service
                                .inner
                                .runtime
                                .describe_sealed_materialized_snapshot(
                                    &placement,
                                    request.after_revision,
                                )
                                .await
                        }
                        Err(error) => {
                            Err(RemoteOperationFailure::failed(subject, error.to_string()))
                        }
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let fetch_snapshot_service = service.clone();
        interconnect
            .register_stream_handler::<FetchStateSnapshot, _, _>(move |_context, request| {
                let service = fetch_snapshot_service.clone();
                async move {
                    service
                        .inner
                        .runtime
                        .stream_sealed_materialized_snapshot(request)
                        .await
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;
        let state_sync_service = service.clone();
        interconnect
            .register_handler::<RemoteStateSyncRequest, _, _>(move |_context, request| {
                let service = state_sync_service.clone();
                async move {
                    let subject = RemoteOperationSubject::state(&request.placement);
                    let placement =
                        match crate::runtime::RuntimeStatePlacement::from_remote(request.placement)
                        {
                            Ok(placement) => placement,
                            Err(reason) => {
                                return RemoteStateSyncResponse {
                                    result: Err(RemoteOperationFailure::failed(
                                        subject,
                                        reason.to_string(),
                                    )),
                                };
                            }
                        };
                    if !service
                        .inner
                        .runtime
                        .runtime_state_placement_is_assigned_locally(&placement)
                    {
                        return RemoteStateSyncResponse {
                            result: Err(RemoteOperationFailure::rejected(subject)),
                        };
                    }
                    let snapshot = service
                        .inner
                        .runtime
                        .handle_state_sync_request(&placement, request.after_lsm)
                        .await
                        .map_err(|error| error.current_context().as_remote_failure(subject));
                    RemoteStateSyncResponse {
                        result: snapshot.map(|snapshot| {
                            snapshot.map(|snapshot| nervix_interconnect::StateSnapshotEnvelope {
                                lsm: snapshot.lsm,
                                schema_fingerprint: snapshot.schema_fingerprint,
                                payload: snapshot.payload,
                            })
                        }),
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let describe_relay_service = service.clone();
        interconnect
            .register_handler::<RemoteDescribeRelayRequest, _, _>(move |_context, request| {
                let service = describe_relay_service.clone();
                async move {
                    RemoteDescribeRelayResponse {
                        result: service.handle_describe_stream_request(request).await,
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let dataflow_status_service = service.clone();
        interconnect
            .register_handler::<RemoteDataflowNodeStatusRequest, _, _>(move |_context, request| {
                let service = dataflow_status_service.clone();
                async move {
                    RemoteDataflowNodeStatusResponse {
                        result: service.handle_dataflow_node_status_request(request).await,
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let domain_drain_service = service.clone();
        interconnect
            .register_handler::<RemoteDomainDrainStatusRequest, _, _>(move |_context, request| {
                let service = domain_drain_service.clone();
                async move {
                    let result = match service.apply_current_cluster_state().await {
                        Ok(()) => {
                            service
                                .inner
                                .runtime
                                .force_flush_domain_if_idle(&request.domain);
                            Ok(service.local_domain_drain_status(&request.domain))
                        }
                        Err(error) => Err(RemoteOperationFailure::failed(
                            RemoteOperationSubject::domain(&request.domain),
                            error.to_string(),
                        )),
                    };
                    RemoteDomainDrainStatusResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let entity_gate_service = service.clone();
        interconnect
            .register_handler::<RemoteEntityGateRequest, _, _>(move |_context, request| {
                let service = entity_gate_service.clone();
                async move {
                    let subject = RemoteOperationSubject::domain(&request.domain);
                    let deadline = tokio::time::Instant::now()
                        .checked_add(Duration::from_millis(request.deadline_millis));
                    let result = match deadline {
                        Some(deadline) => service
                            .inner
                            .runtime
                            .engage_entity_gate_operation(
                                &request.coordination,
                                &request.domain,
                                &request.relays,
                                &request.affected_entities,
                                request.purpose,
                                EntityGateLease {
                                    deadline,
                                    reason: &request.reason,
                                },
                            )
                            .await
                            .map_err(|error| {
                                error.current_context().as_remote_failure(subject.clone())
                            }),
                        None => Err(RemoteOperationFailure::failed(
                            subject,
                            "entity gate deadline exceeds the monotonic clock",
                        )),
                    };
                    RemoteEntityGateResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let entity_drain_service = service.clone();
        interconnect
            .register_handler::<RemoteEntityDrainStatusRequest, _, _>(move |_context, request| {
                let service = entity_drain_service.clone();
                async move {
                    let result = service.local_entity_drain_status(
                        &request.coordination,
                        &request.domain,
                        &request.relays,
                        &request.affected_entities,
                        request.purpose,
                    );
                    if let Ok(status) = &result
                        && (status.buffered_relay_batches != 0
                            || status.node_work_items != 0
                            || status.outstanding_acks != 0)
                    {
                        service
                            .inner
                            .runtime
                            .force_flush_domain_if_idle(&request.domain);
                    }
                    RemoteEntityDrainStatusResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let entity_gate_release_service = service.clone();
        interconnect
            .register_handler::<RemoteEntityGateReleaseRequest, _, _>(move |_context, request| {
                let service = entity_gate_release_service.clone();
                async move {
                    RemoteEntityGateReleaseResponse {
                        result: service
                            .inner
                            .runtime
                            .release_entity_gate_operation(&request.coordination, &request.domain)
                            .await
                            .map_err(|error| {
                                error.current_context().as_remote_failure(
                                    RemoteOperationSubject::domain(&request.domain),
                                )
                            }),
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let describe_metrics_service = service.clone();
        interconnect
            .register_handler::<RemoteDescribeMetricsRequest, _, _>(move |_context, request| {
                let service = describe_metrics_service.clone();
                async move {
                    RemoteDescribeMetricsResponse {
                        result: service.handle_describe_metrics_request(request).await,
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let describe_lookup_service = service.clone();
        interconnect
            .register_handler::<RemoteDescribeLookupRequest, _, _>(move |_context, request| {
                let service = describe_lookup_service.clone();
                async move {
                    RemoteDescribeLookupResponse {
                        result: service.handle_describe_lookup_request(request).await,
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let lookup_service = service.clone();
        interconnect
            .register_handler::<RemoteLookupRequest, _, _>(move |_context, request| {
                let service = lookup_service.clone();
                async move {
                    let subject = RemoteOperationSubject::entity(
                        &request.domain,
                        ModelKind::Lookup,
                        request.name.clone(),
                    );
                    let result = match service.handle_lookup_request(request).await {
                        Ok(Some(record)) => record
                            .encode_arrow_ipc(service.inner.runtime.executor())
                            .await
                            .map(|body| Some(body.to_vec()))
                            .map_err(|error| {
                                RemoteOperationFailure::failed(subject, error.to_string())
                            }),
                        Ok(None) => Ok(None),
                        Err(failure) => Err(failure),
                    };
                    RemoteLookupResponse { result }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let subscription_interest_service = service.clone();
        interconnect
            .register_handler::<RemoteSubscriptionInterestVisibilityRequest, _, _>(
                move |context, request| {
                    let service = subscription_interest_service.clone();
                    async move {
                        let result = if request.subscriber.node_id() != context.peer_node_id() {
                            Err(RemoteOperationFailure::rejected(
                                RemoteOperationSubject::subscription_interest(
                                    &request.domain,
                                    &request.relay,
                                    &request.subscriber,
                                ),
                            ))
                        } else {
                            service
                                .inner
                                .cluster
                                .wait_for_subscription_interest(
                                    &request.subscriber,
                                    request.domain.as_str(),
                                    request.relay.as_str(),
                                )
                                .await;
                            Ok(())
                        };
                        RemoteSubscriptionInterestVisibilityResponse { result }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let capture_handoff_service = service.clone();
        interconnect
            .register_handler::<RemoteCaptureOwnershipHandoffStateRequest, _, _>(
                move |_context, request| {
                    let service = capture_handoff_service.clone();
                    async move {
                        let result: OwnershipHandoffResult<_> = async {
                            if request.source != *service.inner.consensus.local_node_id() {
                                return Err(OwnershipHandoffError::participant(format!(
                                    "ownership handoff capture targets source node '{}' but \
                                     reached '{}'",
                                    request.source,
                                    service.inner.consensus.local_node_id()
                                )));
                            }
                            SessionServiceImpl::verify_ownership_handoff_node_incarnation(
                                &service.live_node_incarnations().await,
                                &request.source,
                                request.source_incarnation,
                                "source",
                            )?;
                            let scheduled = service
                                .prepare_owner_control_request(
                                    &request.domain,
                                    request.entity.kind,
                                    request.entity.identifier.clone(),
                                )
                                .await
                                .map_err(|failure| {
                                    OwnershipHandoffError::participant(failure.to_string())
                                })?;
                            if !scheduled.is_primary_on(service.inner.consensus.local_node_id()) {
                                return Err(OwnershipHandoffError::participant(format!(
                                    "{} '{}' is not owned by this source node",
                                    request.entity.kind.as_str(),
                                    request.entity.identifier.as_str()
                                )));
                            }
                            service
                                .inner
                                .runtime
                                .capture_ownership_handoff_state(
                                    &request.coordination,
                                    &request.domain,
                                    &request.entity,
                                    request.base_schedule_fingerprint,
                                )
                                .await
                        }
                        .await;
                        match result {
                            Ok(checkpoints) => Ok(checkpoints),
                            Err(error) => Err(OwnershipHandoffError::remote_rejection(&error)),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        service
            .register_prepare_ownership_handoff_handler(&interconnect)
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let forced_recovery_service = service.clone();
        interconnect
            .register_handler::<RemotePrepareForcedOwnershipRecoveryRequest, _, _>(
                move |_context, request| {
                    let service = forced_recovery_service.clone();
                    async move {
                        let result: OwnershipHandoffResult<_> = async {
                            if request.destination != *service.inner.consensus.local_node_id() {
                                return Err(OwnershipHandoffError::participant(format!(
                                    "forced ownership recovery for {} '{}' targets node '{}' but \
                                     reached '{}'",
                                    request.entity.kind.as_str(),
                                    request.entity.identifier.as_str(),
                                    request.destination,
                                    service.inner.consensus.local_node_id()
                                )));
                            }
                            SessionServiceImpl::verify_ownership_handoff_node_incarnation(
                                &service.live_node_incarnations().await,
                                &request.destination,
                                request.destination_incarnation,
                                "destination",
                            )?;
                            let deadline =
                                tokio::time::Instant::now() + FORCED_OWNERSHIP_RECOVERY_BUDGET;
                            service
                                .inner
                                .runtime
                                .prepare_forced_ownership_recovery(request, deadline)
                                .await
                        }
                        .await;
                        match result {
                            Ok(preparation) => Ok(preparation),
                            Err(error) => Err(OwnershipHandoffError::remote_rejection(&error)),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let confirm_handoff_service = service.clone();
        interconnect
            .register_handler::<RemoteConfirmOwnershipHandoffStateRequest, _, _>(
                move |_context, request| {
                    let service = confirm_handoff_service.clone();
                    async move {
                        match service.confirm_local_ownership_handoff_state(request).await {
                            Ok(()) => Ok(()),
                            Err(error) => Err(OwnershipHandoffError::remote_rejection(&error)),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let activate_handoff_service = service.clone();
        interconnect
            .register_handler::<RemoteActivateOwnershipHandoffStateRequest, _, _>(
                move |_context, request| {
                    let service = activate_handoff_service.clone();
                    async move {
                        let result = service
                            .activate_local_ownership_handoff_state(&request)
                            .await;
                        match result {
                            Ok(()) => Ok(()),
                            Err(error) => Err(OwnershipHandoffError::remote_rejection(&error)),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let discard_handoff_service = service.clone();
        interconnect
            .register_handler::<RemoteDiscardOwnershipHandoffStateRequest, _, _>(
                move |_context, request| {
                    let service = discard_handoff_service.clone();
                    async move {
                        let result = service
                            .inner
                            .runtime
                            .discard_prepared_ownership_handoff_state(
                                &request.coordination,
                                &request.operation_id,
                                &request.domain,
                                &request.entity,
                            );
                        match result {
                            Ok(()) => Ok(()),
                            Err(error) => Err(OwnershipHandoffFailure::rejected(error.to_string())),
                        }
                    }
                },
            )
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        service
            .register_ownership_handoff_reconciliation_handler(&interconnect)
            .change_context(AppError::RegisterInterconnectRequestHandler)?;

        let ownership_handoff_reconciliation_service = service.clone();
        background_tasks.push(tokio::spawn(async move {
            ownership_handoff_reconciliation_service
                .run_ownership_handoff_preparation_reconciliation()
                .await;
        }));

        let transaction_service = service.clone();
        let transaction_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let mut observed_leadership = None;
            loop {
                tokio::task::consume_budget().await;
                let is_leader = transaction_service
                    .inner
                    .consensus
                    .current_leader()
                    .await
                    .as_ref()
                    == Some(transaction_service.inner.consensus.local_node_id());
                if observed_leadership != Some(is_leader) {
                    transaction_service.inner.transaction_bindings.clear();
                    let schedule = transaction_service.inner.consensus.current_schedule().await;
                    match transaction_service
                        .inner
                        .registry
                        .synchronize_cluster_schedule(&schedule)
                    {
                        Ok(()) => observed_leadership = Some(is_leader),
                        Err(error) => {
                            warn!(
                                is_leader,
                                error = %error,
                                "failed to synchronize registry after leadership state change"
                            );
                            tokio::select! {
                                _ = transaction_shutdown.cancelled() => break,
                                _ = sleep(Duration::from_millis(250)) => {}
                            }
                            continue;
                        }
                    }
                }
                if is_leader {
                    transaction_service.reconcile_transactions_once().await;
                }
                tokio::select! {
                    _ = transaction_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_millis(250)) => {}
                }
            }
        }));

        let clock_authority_service = service.clone();
        let clock_authority_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            run_domain_clock_authority_reconciliation(
                clock_authority_service,
                clock_authority_shutdown,
            )
            .await;
        }));

        let runtime_event_service = service.clone();
        let runtime_event_shutdown = shutdown.clone();
        let mut runtime_event_rx = runtime.subscribe_events();
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = runtime_event_shutdown.cancelled() => break,
                    received = runtime_event_rx.recv() => match received {
                        Ok(RuntimeEvent::Error(message)) => {
                            let mut node_ids =
                                runtime_event_service.inner.cluster.live_node_ids().await;
                            node_ids.sort();
                            node_ids.dedup();
                            for node_id in node_ids {
                                tokio::task::consume_budget().await;
                                if node_id == runtime_event_service.inner.consensus.local_node_id().clone() {
                                    continue;
                                }
                                if let Err(error) = runtime_event_service
                                    .dispatch_interconnect_control(
                                        &node_id,
                                        ControlEnvelope::RuntimeErrorEvent(
                                            RemoteRuntimeErrorEvent {
                                                message: message.clone(),
                                            },
                                        ),
                                    )
                                    .await
                                {
                                    warn!(
                                        %node_id,
                                        error,
                                        "failed to fan out runtime error event"
                                    );
                                }
                            }
                        }
                        // The peers never learn about the errors this node skipped, so the count
                        // is logged here to keep the gap attributable to load rather than silence.
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "runtime error fan-out fell behind the event bus");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }));

        let resource_service = service.clone();
        let resource_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            loop {
                tokio::task::consume_budget().await;
                if resource_shutdown.is_cancelled() {
                    break;
                }
                resource_service.reconcile_resources_once().await;
                tokio::select! {
                    _ = resource_shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        service.initialize_http_tls_server_config().await;

        let domain_apply_service = service.clone();
        let domain_apply_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let mut domains_rx = domain_apply_service.inner.consensus.subscribe_domains();
            if let Err(error) = domain_apply_service.apply_current_cluster_state().await {
                warn!(error = %error, "failed to apply cluster schedule after initial domain sync");
            }

            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = domain_apply_shutdown.cancelled() => break,
                    changed = domains_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        if let Err(error) = domain_apply_service.apply_current_cluster_state().await {
                            warn!(error = %error, "failed to apply cluster schedule after domain sync");
                        }
                    }
                }
            }
        }));

        let clock_service = service.clone();
        let clock_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let mut domain_state_rx = clock_service.inner.runtime.subscribe_domain_state();
            let mut tasks: HashMap<DomainName, DomainClockTask> = HashMap::new();
            let mut retirements = DomainClockRetirements::default();

            loop {
                tokio::task::consume_budget().await;
                reconcile_domain_clock_tasks(
                    &clock_service,
                    &clock_shutdown,
                    &mut tasks,
                    &mut retirements,
                )
                .await;
                tokio::select! {
                    _ = clock_shutdown.cancelled() => break,
                    changed = domain_state_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = retirements.join_next(), if !retirements.is_empty() => {}
                }
            }

            for (_, task) in tasks {
                task.task.stop().await;
            }
            retirements.stop_all().await;
        }));

        let kafka_schedule_service = service.clone();
        let kafka_schedule_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            let mut schedule_rx = kafka_schedule_service.inner.consensus.subscribe_schedule();
            let mut tasks: HashMap<KafkaPartitionWatcherKey, KafkaPartitionWatcherTask> =
                HashMap::new();

            loop {
                tokio::task::consume_budget().await;
                let schedule = kafka_schedule_service
                    .inner
                    .consensus
                    .current_schedule()
                    .await;
                kafka_schedule_service
                    .reconcile_kafka_partition_watchers(&schedule, &mut tasks)
                    .await;
                tokio::select! {
                    _ = kafka_schedule_shutdown.cancelled() => break,
                    changed = schedule_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = sleep(LEADER_KAFKA_PARTITION_WATCH_INTERVAL) => {}
                }
            }

            for (_, watcher) in tasks {
                watcher.task.stop().await;
            }
        }));

        let (interconnect_relay_payload_lane, interconnect_relay_payload_rx) =
            InterconnectRelayPayloadLane::new();
        let relay_payload_shutdown = shutdown.clone();
        let runtime_for_relay_payloads = runtime.clone();
        background_tasks.push(tokio::spawn(async move {
            InterconnectRelayPayloadLane::run(
                interconnect_relay_payload_rx,
                runtime_for_relay_payloads,
                relay_payload_shutdown,
            )
            .await;
        }));

        let interconnect_shutdown = shutdown.clone();
        let runtime_for_interconnect = runtime.clone();
        let service_for_interconnect = service.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = interconnect_shutdown.cancelled() => break,
                    message = interconnect_rx.recv() => {
                        let Some(message) = message else {
                            break;
                        };
                        debug!(
                            peer_addr = %message.peer_addr,
                            peer_node_id = %message.peer_node_id,
                            "received interconnect envelope"
                        );
                        let Some(envelope) = interconnect_relay_payload_lane
                            .route(
                                &message.peer_node_id,
                                message.envelope,
                                message.relay_admission,
                            )
                        else {
                            continue;
                        };
                        match envelope {
                            Envelope::RelayPayload(_) => {
                                unreachable!("relay payloads are routed to their dedicated lane")
                            }
                            Envelope::Ack(ack) => {
                                runtime_for_interconnect.handle_remote_ack_resolution(ack);
                            }
                            Envelope::Control(ControlEnvelope::Terminate) => {}
                            Envelope::Control(
                                ControlEnvelope::Request(_) | ControlEnvelope::Response(_),
                            ) => {
                                unreachable!("typed requests are consumed by the interconnect")
                            }
                            Envelope::Control(ControlEnvelope::StateReplicationAck(ack)) => {
                                let placement = match crate::runtime::RuntimeStatePlacement::from_remote(
                                    ack.placement,
                                ) {
                                    Ok(placement) => placement,
                                    Err(error) => {
                                        warn!(error = %error, "failed to decode state replication ack placement");
                                        continue;
                                    }
                                };
                                service_for_interconnect.inner.runtime.handle_state_replication_ack(
                                    &message.peer_node_id,
                                    crate::runtime::StateSyncAck {
                                        placement,
                                        lsm: ack.lsm,
                                    },
                                );
                            }
                            Envelope::Control(ControlEnvelope::StateCheckpointAvailable(
                                checkpoint,
                            )) => {
                                service_for_interconnect
                                    .inner
                                    .runtime
                                    .handle_state_checkpoint_available(
                                        &message.peer_node_id,
                                        checkpoint,
                                    );
                            }
                            Envelope::Control(ControlEnvelope::RuntimeErrorEvent(event)) => {
                                service_for_interconnect.broadcast_error(event.message);
                            }
                        }
                    }
                }
            }
        }));
        let cluster_events = events.clone();
        let mut cluster_event_rx = cluster.subscribe_events();
        let cluster_events_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = cluster_events_shutdown.cancelled() => break,
                    received = cluster_event_rx.recv() => match received {
                        Ok(message) => cluster_events.relay_info(message),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "cluster event relay fell behind the event bus");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }));
        let consensus_events = events.clone();
        let mut consensus_event_rx = consensus.observer().subscribe_events();
        let consensus_events_shutdown = shutdown.clone();
        background_tasks.push(tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    _ = consensus_events_shutdown.cancelled() => break,
                    received = consensus_event_rx.recv() => match received {
                        Ok(message) => consensus_events.relay_info(message),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "consensus event relay fell behind the event bus");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }));

        info!(mode = grpc_mode.scheme(), addr = %grpc_listen_addr, "nervix gRPC server listening");
        info!(addr = %http_listen_addr, "nervix HTTP server listening");
        info!(addr = %https_listen_addr, "nervix HTTPS server listening");
        info!(addr = %observability_listen_addr, "nervix observability server listening");
        info!(addr = %web_console_listen_addr, "nervix web console server listening");
        if let Some(addr) = web_console_https_listen_addr {
            info!(addr = %addr, "nervix web console TLS server listening");
        }

        let listener_shutdown = shutdown_coordinator.admission_token();

        let grpc_service = service.clone();
        let grpc_shutdown = listener_shutdown.clone();
        let api_server = async move {
            let mut builder = Server::builder();
            builder = if grpc_mode.is_tls() {
                builder
                    .tls_config(
                        grpc_tls_server_config.ok_or_else(|| Report::new(AppError::LoadGrpcTls))?,
                    )
                    .change_context(AppError::LoadGrpcTls)?
            } else {
                builder
            };
            // Closing admission ends every connection this listener accepted, so the server's
            // graceful shutdown never waits for a client to finish or abandon a request.
            let grpc_incoming =
                AdmittingListener::new(grpc_listener, grpc_shutdown.clone()).into_connections();
            builder
                .add_service(SessionServiceServer::new(grpc_service.clone()))
                .serve_with_incoming_shutdown(grpc_incoming, grpc_shutdown.cancelled_owned())
                .await
                .map_err(|e| Report::new(e).change_context(AppError::Serve))
        };
        let http_server = serve_http(
            runtime.clone(),
            service.inner.service_tasks.clone(),
            http_listener,
            listener_shutdown.clone(),
        );
        let https_server = serve_https(
            runtime.clone(),
            service.inner.service_tasks.clone(),
            service.inner.http_tls_server_config.clone(),
            https_listener,
            listener_shutdown.clone(),
        );
        let observability_server = serve_observability_http(
            consensus.observer(),
            runtime.clone(),
            observability_listener,
            listener_shutdown.clone(),
        );
        let web_console_server = serve_web_console_http(
            service.clone(),
            web_console_listener,
            listener_shutdown.clone(),
        );
        let web_console_https_shutdown = listener_shutdown;
        let web_console_https_service = service.clone();
        let web_console_https_server = async move {
            if let (Some(config), Some(listener)) =
                (web_console_tls_server_config, web_console_https_listener)
            {
                serve_web_console_https(
                    web_console_https_service,
                    config,
                    listener,
                    web_console_https_shutdown.clone(),
                )
                .await
            } else {
                web_console_https_shutdown.cancelled().await;
                Ok(())
            }
        };

        let public_listeners_shutdown = shutdown_coordinator.clone();
        let public_listeners = tokio::spawn(async move {
            let (
                api_result,
                http_result,
                https_result,
                observability_result,
                web_console_result,
                web_console_https_result,
            ) = tokio::join!(
                request_shutdown_on_completion(api_server, public_listeners_shutdown.clone()),
                request_shutdown_on_completion(http_server, public_listeners_shutdown.clone()),
                request_shutdown_on_completion(https_server, public_listeners_shutdown.clone()),
                request_shutdown_on_completion(
                    observability_server,
                    public_listeners_shutdown.clone()
                ),
                request_shutdown_on_completion(
                    web_console_server,
                    public_listeners_shutdown.clone()
                ),
                request_shutdown_on_completion(
                    web_console_https_server,
                    public_listeners_shutdown.clone()
                ),
            );
            api_result
                .and(http_result)
                .and(https_result)
                .and(observability_result)
                .and(web_console_result)
                .and(web_console_https_result)
        });

        // The node serves until its first stop request, whether a termination signal, the caller
        // embedding the application, or a failed listener made it. That request's deadline bounds
        // everything that follows.
        let deadline = shutdown_coordinator.requested().await.deadline();

        let advertisement = deadline.bound(cluster.mark_local_terminating()).await;
        let advertisement_outcome = match advertisement {
            BeforeDeadline::Finished(()) => ShutdownPhaseOutcome::Completed,
            BeforeDeadline::Expired => {
                warn!(
                    "shutdown deadline expired before this node advertised that it is terminating"
                );
                ShutdownPhaseOutcome::Forced
            }
        };
        shutdown_coordinator.stop_admission();
        let listeners = join_public_listeners(public_listeners, deadline).await;
        let stop_admission_outcome = advertisement_outcome
            .combine(listeners.outcome)
            .unless_deadline_passed(deadline);
        shutdown_coordinator.begin_drain_support(stop_admission_outcome);

        let drain_support_outcome = if graceful_shutdown_drain {
            let drain = service.drain_local_node_before_shutdown(drain_timeout, deadline);
            let drained = deadline.bound(drain).await;
            match drained {
                BeforeDeadline::Finished(outcome) => outcome.unless_deadline_passed(deadline),
                BeforeDeadline::Expired => {
                    warn!("shutdown deadline expired while this node drained its admitted work");
                    ShutdownPhaseOutcome::Forced
                }
            }
        } else {
            ShutdownPhaseOutcome::Completed.unless_deadline_passed(deadline)
        };
        shutdown_coordinator.begin_terminal_teardown(drain_support_outcome);

        let background_task_shutdowns = background_tasks.into_iter().map(|task| {
            await_background_task_shutdown(task, "application background task", deadline)
        });
        let background_task_outcomes = join_all(background_task_shutdowns).await;
        let mut terminal_teardown_outcome = ShutdownPhaseOutcome::Completed;
        for task_outcome in background_task_outcomes {
            terminal_teardown_outcome = terminal_teardown_outcome.combine(task_outcome);
        }
        let service_tasks_outcome = service.inner.service_tasks.shut_down(deadline).await;
        terminal_teardown_outcome = terminal_teardown_outcome.combine(service_tasks_outcome);
        // The services below own this node's remaining tasks, connections, and storage, and
        // stopping them is what releases those, so each stop runs to completion rather than being
        // abandoned at the deadline. A server process whose deadline passes meanwhile is ended by
        // its deadline supervision.
        runtime.shutdown().await;
        consensus.shutdown().await;
        let cluster_shutdown_result = cluster
            .shutdown()
            .await
            .change_context(AppError::ShutdownCluster);
        interconnect.shutdown().await;

        let database_owner_outcome = tokio::task::spawn_blocking(move || {
            drop(service);
            drop(runtime);
            drop(consensus);
            drop(cluster);
            drop(interconnect);
            drop(db);
        })
        .await
        .map_err(|error| {
            error!(error = %error, "failed to join database owner shutdown task");
            Report::new(AppError::OpenRegistry)
        });

        if cluster_shutdown_result.is_err() || database_owner_outcome.is_err() {
            terminal_teardown_outcome =
                terminal_teardown_outcome.combine(ShutdownPhaseOutcome::Abandoned);
        }
        shutdown_coordinator.finish(terminal_teardown_outcome.unless_deadline_passed(deadline));

        listeners.result?;
        cluster_shutdown_result?;
        database_owner_outcome?;
        let outcome = shutdown_coordinator
            .outcome()
            .verified("the terminal-teardown phase finished above");
        if outcome.deadline_expired() {
            return Err(Report::new(AppError::ShutdownDeadlineExpired));
        }

        Ok(())
    }
}

fn print_completions(shell: Shell) {
    let mut command = Args::command();
    let bin_name = command.get_name().to_string();
    generate(shell, &mut command, bin_name, &mut std::io::stdout());
}

/// What the edges expose. Everything else this module and its submodules declare is
/// `pub(in crate::application)` or narrower, so the binary and the test harness reach the server
/// only through the names below.
pub use error::AppError;
pub use termination_signals::TerminationSignals;
pub use tracing_setup::{TracingGuard, init_tracing, init_tracing_to_file};

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use session_service::{current_word_prefix, word_start};
    use test_fixtures::{test_addr, test_args, try_test_args};

    use super::*;

    #[test]
    fn args_parse_observability_listen_addr() {
        let args = test_args(&["--observability-listen-addr", "127.0.0.1:19090"]);
        let app = Application::try_from(args).expect("args should parse");
        assert_eq!(app.observability_listen_addr, test_addr(19090));
    }

    #[test]
    fn args_default_to_the_application_shutdown_timeouts() {
        let args = test_args(&[]);

        assert_eq!(args.drain_timeout, shutdown::DEFAULT_DRAIN_TIMEOUT);
        assert_eq!(args.shutdown_timeout, shutdown::DEFAULT_SHUTDOWN_TIMEOUT);
    }

    #[test]
    fn args_parse_shutdown_timeout() {
        let args = test_args(&["--shutdown-timeout", "2m"]);

        assert_eq!(args.shutdown_timeout, Duration::from_secs(120));
    }

    #[test]
    fn args_parse_temp_dir() {
        let args = test_args(&["--temp-dir", "/tmp/nervix-temp"]);
        let app = Application::try_from(args).expect("args should parse");
        assert_eq!(app.temp_dir, PathBuf::from("/tmp/nervix-temp"));
    }

    #[test]
    fn args_parse_resource_store_limits() {
        let args = test_args(&[
            "--resource-max-archive-bytes",
            "8MiB",
            "--resource-max-extracted-bytes",
            "32MiB",
            "--resource-max-file-count",
            "2048",
        ]);
        let app = Application::try_from(args).expect("args should parse");

        assert_eq!(app.resource_store_limits.max_archive_bytes, 8 * 1024 * 1024);
        assert_eq!(
            app.resource_store_limits.max_extracted_bytes,
            32 * 1024 * 1024
        );
        assert_eq!(app.resource_store_limits.max_file_count, 2048);
    }

    #[test]
    fn args_parse_web_console_listen_addr() {
        let args = test_args(&[
            "--web-console-listen-addr",
            "127.0.0.1:17420",
            "--web-console-https-listen-addr",
            "127.0.0.1:17443",
            "--web-console-tls-cert",
            "tls/dev/node.pem",
            "--web-console-tls-key",
            "tls/dev/node-key.pem",
        ]);
        let app = Application::try_from(args).expect("args should parse");
        assert_eq!(app.web_console_listen_addr, test_addr(17420));
        assert_eq!(app.web_console_https_listen_addr, Some(test_addr(17443)));
        assert_eq!(
            app.web_console_tls_cert,
            Some(PathBuf::from("tls/dev/node.pem"))
        );
        assert_eq!(
            app.web_console_tls_key,
            Some(PathBuf::from("tls/dev/node-key.pem"))
        );
    }

    #[test]
    fn parse_and_text_encoding_helpers_roundtrip() {
        assert_eq!(current_word_prefix("CREATE SCHE", 11), "sche");
        assert_eq!(word_start("CREATE SCHE", 11), 7);
        assert_eq!(
            parse_human_duration("1500ms").expect("valid duration"),
            Duration::from_millis(1500)
        );
        assert_eq!(
            parse_human_bytes("1.5MiB").expect("valid bytes"),
            ubyte::ByteUnit::Mebibyte(1) + ubyte::ByteUnit::Kibibyte(512)
        );
        assert_eq!(parse_trace_sample_ratio("0.25"), Ok(0.25));
        assert!(parse_trace_sample_ratio("1.25").is_err());

        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let hex = encode_hex(&bytes);
        assert_eq!(hex, "deadbeef");

        assert!(parse_human_duration("oops").is_err());
        assert!(parse_human_bytes("oops").is_err());
    }

    #[test]
    fn server_args_parse_memory_pressure_options() {
        let args = test_args(&[
            "--memory-high-watermark",
            "2MiB",
            "--memory-low-watermark",
            "1MiB",
            "--memory-pressure-check-interval",
            "250ms",
            "--memory-pressure-resume-jitter",
            "500ms",
        ]);
        let app = Application::try_from(args).expect("args should parse");
        let memory_pressure = app.memory_pressure.expect("memory pressure configured");

        assert_eq!(memory_pressure.high_watermark, ubyte::ByteUnit::Mebibyte(2));
        assert_eq!(memory_pressure.low_watermark, ubyte::ByteUnit::Mebibyte(1));
        assert_eq!(memory_pressure.check_interval, Duration::from_millis(250));
        assert_eq!(memory_pressure.resume_jitter, Duration::from_millis(500));
    }

    #[test]
    fn server_args_reject_incomplete_memory_pressure_watermarks() {
        let args = test_args(&["--memory-high-watermark", "2MiB"]);

        let error = Application::try_from(args).expect_err("low watermark is required");
        assert!(format!("{error:?}").contains("memory high watermark requires"));
    }

    #[test]
    fn server_args_parse_opentelemetry_options() {
        let args = test_args(&[
            "--otel-enabled",
            "--otel-otlp-endpoint",
            "http://collector:4317",
            "--otel-service-name",
            "nervix-test",
            "--otel-trace-sample-ratio",
            "0.5",
        ]);

        assert!(args.otel_enabled);
        assert_eq!(args.otel_otlp_endpoint, "http://collector:4317");
        assert_eq!(args.otel_service_name, "nervix-test");
        assert_eq!(args.otel_trace_sample_ratio, 0.5);
    }

    #[test]
    fn server_args_do_not_require_opentelemetry_options() {
        let args = test_args(&[]);

        assert!(!args.otel_enabled);
        assert_eq!(args.otel_otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(args.otel_service_name, "nervix");
        assert_eq!(args.otel_trace_sample_ratio, 1.0);
    }

    #[test]
    fn server_args_only_require_opentelemetry_enable_flag_to_enable_export() {
        let args = test_args(&["--otel-enabled"]);

        assert!(args.otel_enabled);
        assert_eq!(args.otel_otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(args.otel_service_name, "nervix");
        assert_eq!(args.otel_trace_sample_ratio, 1.0);
    }

    #[test]
    fn server_args_reject_invalid_opentelemetry_sample_ratio() {
        let result = try_test_args(&["--otel-trace-sample-ratio", "2.0"]);

        assert!(result.is_err());
    }
}
