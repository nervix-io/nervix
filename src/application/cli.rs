//! Layer: edges.
//! Owns: node CLI arguments and command variants.
//! Depends on: control-plane configuration values, Clap and the shared vocabulary.
//! Must not know: graph execution or connector internals.
#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns node command-line configuration")
)]

use clap::Subcommand;

use super::{
    cli_values::{parse_human_bytes, parse_human_duration, parse_trace_sample_ratio},
    *,
};

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
    #[command(flatten)]
    pub command_execution: CommandExecutionPolicy,
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
        env = "NERVIX_RESTORE_STAGING_MAX_BYTES",
        default_value = "128GiB",
        value_parser = parse_human_bytes,
        help = "Maximum unpublished restore checkpoint key and value bytes per node"
    )]
    pub restore_staging_max_bytes: ubyte::ByteUnit,
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
    #[arg(
        long,
        env = "NERVIX_DNS_RESOLVER_CONFIG",
        default_value = nervix_dns::SYSTEM_RESOLVER_CONFIGURATION,
        help = "A resolv.conf-format file naming the name servers, search list and options the \
                node's resolver uses for peers, connectors and sessions"
    )]
    pub dns_resolver_config: PathBuf,
    #[arg(
        long,
        env = "NERVIX_DNS_HOSTS_FILE",
        default_value = nervix_dns::SYSTEM_HOSTS_FILE,
        help = "A hosts-format file consulted before DNS"
    )]
    pub dns_hosts_file: PathBuf,
    #[arg(
        long = "dns-name-server",
        env = "NERVIX_DNS_NAME_SERVERS",
        value_delimiter = ',',
        help = "A name server address with its port, replacing the resolver configuration's name \
                servers; repeatable or comma-separated"
    )]
    pub dns_name_servers: Vec<SocketAddr>,
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
    /// Where a diagnostic node records its deadlock evidence. Without it, a deadlock the detector
    /// reports is described on standard error only. Exists only in a `deloxide` build.
    #[cfg(feature = "deloxide")]
    #[arg(
        long,
        env = "NERVIX_DEADLOCK_EVIDENCE",
        help = "Existing directory a diagnostic node records its deadlock evidence in"
    )]
    pub deadlock_evidence: Option<PathBuf>,

    /// Disable runtime order checking in an order-instrumented diagnostic artifact.
    #[cfg(feature = "deloxide")]
    #[clap(
        long,
        help = "Check active cycles only; compiled order instrumentation retains its cost"
    )]
    pub deadlock_active_only: bool,
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
