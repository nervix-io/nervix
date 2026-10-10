use std::{
    collections::BTreeMap,
    fs, io,
    net::{Ipv4Addr, SocketAddrV4},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use clap::{Parser, Subcommand, ValueEnum};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_benchmark::{
    AbArm, AbSummary, BenchmarkCatalog, BenchmarkDependency, BenchmarkRunFailure,
    BenchmarkSuiteReport, ContainerImplementation, Implementation, KafkaRenderInputs, LoadShape,
    LoadedBenchmark, NERVIX_METRICS_PROMETHEUS_FILE, NERVIX_METRICS_REPORT_FILE,
    NervixImplementation, NervixMetricsReport, RunSettings, provision_topics,
};
use nervix_client_core::{
    Client, CommandOutcome, ConnectOptions, DomainName, split_query_statements,
};
use nervix_models::ClusterNodeName;
use nervix_primitives::unmodeled::net::TcpListener;
use nervix_test_environment::{
    ContainerMode, ContainerReadiness, DependencyEnvironment, KAFKA_ADDR, KAFKA_DOCKER_ADDR,
    KAFKA_DOCKER_NETWORK, ManagedContainerInfo, TeardownFailures, configure_process_lifecycle,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, Ia5String, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use testcontainers::{
    CopyTargetOptions, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor, wait::HttpWaitStrategy},
};
use thiserror::Error;
use tokio::process::{Child, Command};
use uuid::Uuid;

const DEFAULT_BENCHMARKS_ROOT: &str = "benches/benchmarks";
const DEFAULT_ARTIFACTS_ROOT: &str = "target/benchmarks";
const DEFAULT_SERVER_BINARY: &str = "target/release/nervix-server";
const DEFAULT_DOMAIN: &str = "default";
const DEFAULT_USERNAME: &str = "default";
const NERVIX_GRPC_PORT: ContainerPort = ContainerPort::Tcp(47391);
const NERVIX_OBSERVABILITY_PORT: ContainerPort = ContainerPort::Tcp(9090);
const NERVIX_INTERCONNECT_PORT: u16 = 47395;
const NERVIX_HTTP_PORT: u16 = 8080;
const NERVIX_HTTPS_PORT: u16 = 8443;
const NERVIX_WEB_CONSOLE_PORT: u16 = 47420;
const NERVIX_CONTAINER_ROLES: [&str; 3] = [
    "benchmark-subject-node-1",
    "benchmark-subject-node-2",
    "benchmark-subject-node-3",
];
/// The largest value payload a benchmark run may generate.
const MAX_VALUE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Parser)]
#[command(about = "Run declarative end-to-end streaming benchmarks")]
struct Args {
    #[arg(long, default_value = ".")]
    repository_root: PathBuf,
    #[command(subcommand)]
    command: BenchmarkCommand,
}

#[derive(Debug, Subcommand)]
enum BenchmarkCommand {
    /// List benchmark definitions and their implementations.
    List,
    /// Execute one implementation of one benchmark.
    Run(Box<RunArgs>),
    /// Execute every declared implementation of every benchmark in catalog order.
    RunAll(Box<RunOptions>),
    /// Execute the local nervix implementation of one benchmark for two server binaries,
    /// interleaved, and summarize per-arm statistics.
    RunAb(Box<RunAbArgs>),
}

#[derive(Debug, clap::Args)]
struct RunArgs {
    benchmark: String,
    #[arg(long, default_value = "nervix")]
    implementation: String,
    #[command(flatten)]
    options: RunOptions,
}

#[derive(Clone, Debug, clap::Args)]
struct RunOptions {
    #[arg(long, value_enum, default_value_t = NervixMode::Local)]
    nervix_mode: NervixMode,
    #[arg(long)]
    nervix_image: Option<String>,
    #[arg(long)]
    server_binary: Option<PathBuf>,
    #[command(flatten)]
    workload: WorkloadOptions,
}

#[derive(Clone, Debug, clap::Args)]
struct WorkloadOptions {
    #[arg(long)]
    load_driver: Option<PathBuf>,
    #[arg(long)]
    artifacts_root: Option<PathBuf>,
    #[arg(long)]
    duration_seconds: Option<u64>,
    #[arg(long)]
    warmup_seconds: Option<u64>,
    #[arg(long)]
    partitions: Option<u32>,
    #[arg(long)]
    value_bytes: Option<u64>,
    #[arg(long)]
    max_backlog_messages: Option<u64>,
    #[arg(long)]
    wait_timeout_seconds: Option<u64>,
    #[arg(long = "parameter", value_name = "NAME=VALUE")]
    parameter_overrides: Vec<String>,
}

impl WorkloadOptions {
    fn resolve_artifacts_root(&self, repository_root: &Path) -> PathBuf {
        match self.artifacts_root.as_deref() {
            Some(path) => absolute_or_repository_path(repository_root, path),
            None => repository_root.join(DEFAULT_ARTIFACTS_ROOT),
        }
    }
}

const DEFAULT_AB_RUNS: NonZeroUsize = match NonZeroUsize::new(3) {
    Some(runs) => runs,
    None => panic!("three is nonzero"),
};

#[derive(Debug, clap::Args)]
struct RunAbArgs {
    benchmark: String,
    /// Server binary measured as the baseline arm.
    #[arg(long)]
    baseline_binary: PathBuf,
    /// Server binary measured as the candidate arm.
    #[arg(long)]
    candidate_binary: PathBuf,
    /// Interleaved measurement runs per arm.
    #[arg(long, default_value_t = DEFAULT_AB_RUNS)]
    runs: NonZeroUsize,
    /// Display label for the baseline arm; defaults to its binary path.
    #[arg(long)]
    baseline_label: Option<String>,
    /// Display label for the candidate arm; defaults to its binary path.
    #[arg(long)]
    candidate_label: Option<String>,
    #[command(flatten)]
    workload: WorkloadOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum NervixMode {
    Local,
    Image,
}

#[derive(Debug)]
struct ResolvedRun {
    slug: String,
    implementation: String,
    partitions: u32,
    value_bytes: u64,
    max_backlog_messages: u64,
    wait_timeout: Duration,
    duration_seconds: u64,
    warmup_seconds: u64,
    shape: LoadShape,
    parameters: toml::Table,
    input_topic: String,
    output_topic: String,
    consumer_group: String,
    domain: String,
    run_token: String,
    run_directory: PathBuf,
}

/// The workload bounds one run resolved from its benchmark and the command line.
struct WorkloadBounds {
    partitions: u32,
    value_bytes: u64,
    max_backlog_messages: u64,
    wait_timeout_seconds: u64,
    warmup_seconds: u64,
}

impl WorkloadBounds {
    fn validate(&self) -> error_stack::Result<(), WorkloadError> {
        if self.partitions == 0 {
            return Err(Report::new(WorkloadError::ZeroPartitions));
        }
        if i32::try_from(self.partitions).is_err() {
            return Err(Report::new(WorkloadError::PartitionsBeyondKafka {
                partitions: self.partitions,
            }));
        }
        if self.value_bytes == 0 {
            return Err(Report::new(WorkloadError::ZeroValueBytes));
        }
        if self.value_bytes > MAX_VALUE_BYTES {
            return Err(Report::new(WorkloadError::ValueBytesAboveLimit {
                value_bytes: self.value_bytes,
            }));
        }
        if self.max_backlog_messages == 0 {
            return Err(Report::new(WorkloadError::ZeroBacklog));
        }
        if self.wait_timeout_seconds == 0 {
            return Err(Report::new(WorkloadError::ZeroWaitTimeout));
        }
        if self.warmup_seconds == 0 {
            return Err(Report::new(WorkloadError::ZeroWarmup));
        }
        Ok(())
    }
}

/// One local Nervix process of the benchmark subject.
struct LocalNode {
    name: ClusterNodeName,
    child: Child,
}

enum SubjectRuntime {
    Local { nodes: Vec<LocalNode> },
    Container { infos: Vec<ManagedContainerInfo> },
}

struct Subject {
    runtime: SubjectRuntime,
    control_url: Option<String>,
    metrics_urls: Vec<reqwest::Url>,
    password: Option<String>,
    node_count: usize,
}

/// Why a benchmark command failed. Beneath each context is the report of the catalog,
/// comparison, client, dependency, process or file operation that failed, so the rendered chain
/// names every cause once.
#[derive(Debug, Error)]
enum BenchmarkCliError {
    #[error("failed to build the benchmark runtime")]
    BuildRuntime,

    #[error("failed to resolve repository root {}", path.display())]
    ResolveRepositoryRoot { path: PathBuf },

    #[error("failed to load the benchmark catalog")]
    Catalog,

    #[error("benchmark catalog is empty")]
    EmptyCatalog,

    #[error("failed to load benchmark '{benchmark}'")]
    LoadBenchmark { benchmark: String },

    #[error("benchmark '{benchmark}' has no implementation named '{implementation}'")]
    UnknownImplementation {
        benchmark: String,
        implementation: String,
    },

    #[error("invalid run settings for benchmark '{benchmark}'")]
    RunSettings { benchmark: String },

    /// The [`WorkloadError`] beneath names the bound that does not hold.
    #[error("invalid workload for benchmark '{benchmark}'")]
    Workload { benchmark: String },

    #[error("failed to create directory {}", path.display())]
    CreateDirectory { path: PathBuf },

    #[error("failed to create file {}", path.display())]
    CreateFile { path: PathBuf },

    #[error("failed to duplicate the log file handle of {}", path.display())]
    DuplicateLog { path: PathBuf },

    #[error("failed to write {}", path.display())]
    WriteFile { path: PathBuf },

    #[error("failed to read {}", path.display())]
    ReadFile { path: PathBuf },

    #[error("path {} is not valid UTF-8", path.display())]
    NonUtf8Path { path: PathBuf },

    #[error("run manifest field '{field}' exceeds the TOML integer range")]
    ManifestField { field: &'static str },

    #[error("failed to serialize the run manifest")]
    SerializeRunManifest,

    #[error("failed to compare benchmark runs")]
    Comparison,

    #[error(
        "{failed} of {total} benchmark implementations failed; all catalog entries were attempted"
    )]
    FailedImplementations { failed: usize, total: usize },

    #[error("benchmark dependency teardown failed: {failures}")]
    DependencyTeardown { failures: TeardownFailures },

    #[error("benchmark dependency teardown also failed: {failures}")]
    DependencyTeardownAfterFailure { failures: TeardownFailures },

    #[error("{arm} server binary does not exist at {}", path.display())]
    MissingArmServerBinary { arm: &'static str, path: PathBuf },

    #[error("A/B {arm} arm ({label}) failed on run {run}/{runs}")]
    AbRun {
        arm: &'static str,
        label: String,
        run: usize,
        runs: usize,
    },

    #[error("failed to summarize the A/B comparison")]
    AbComparison,

    #[error("failed to start benchmark dependency {dependency:?}")]
    StartDependency { dependency: BenchmarkDependency },

    #[error("benchmark dependency endpoint '{key}' is unavailable")]
    DependencyEndpoint { key: &'static str },

    #[error("failed to provision benchmark Kafka topics")]
    ProvisionTopics,

    #[error("failed to render benchmark implementation '{implementation}'")]
    RenderImplementation { implementation: String },

    #[error("failed to render post-start statements of implementation '{implementation}'")]
    RenderAfterStart { implementation: String },

    #[error("benchmark dependency returned an invalid container id '{container}'")]
    InvalidContainerId { container: String },

    #[error("failed to run docker {}", .arguments.join(" "))]
    RunDocker { arguments: Vec<String> },

    #[error("docker {} failed with {status}: {stderr}", .arguments.join(" "))]
    DockerFailed {
        arguments: Vec<String>,
        status: ExitStatus,
        stderr: String,
    },

    #[error("failed to collect the logs of benchmark container {container}")]
    CollectContainerLogs { container: String },

    #[error("Nervix server binary does not exist at {}", path.display())]
    MissingServerBinary { path: PathBuf },

    #[error("failed to reserve local ports for the benchmark subject")]
    ReservePorts,

    #[error(
        "each benchmark node must have one TLS endpoint name: {nodes} nodes, {endpoints} names"
    )]
    TlsEndpointCount { nodes: usize, endpoints: usize },

    #[error("failed to generate the benchmark interconnect certificate authority")]
    GenerateAuthority,

    #[error("failed to generate the interconnect certificate of benchmark node {node}")]
    GenerateNodeCertificate { node: ClusterNodeName },

    #[error("failed to start local Nervix {node}")]
    StartLocalNode { node: ClusterNodeName },

    #[error("failed to poll local Nervix {node}")]
    WatchLocalNode { node: ClusterNodeName },

    #[error("local Nervix {node} exited before readiness with {status}")]
    LocalNodeExitedBeforeReady {
        node: ClusterNodeName,
        status: ExitStatus,
    },

    #[error("local Nervix {node} did not become ready at {url} before timeout")]
    LocalNodeNotReady { node: ClusterNodeName, url: String },

    #[error("local Nervix {node} exited unexpectedly with {status}")]
    LocalNodeExited {
        node: ClusterNodeName,
        status: ExitStatus,
    },

    #[error("failed to stop local Nervix {node}")]
    StopLocalNode { node: ClusterNodeName },

    #[error("invalid metrics URL {url}")]
    MetricsUrl { url: String },

    #[error("--nervix-image is required with --nervix-mode image")]
    MissingNervixImage,

    /// The [`ImageReferenceError`] beneath says what the reference lacks.
    #[error("invalid image reference '{image}'")]
    ImageReference { image: String },

    #[error("failed to start Nervix benchmark image {node}")]
    StartNervixImage { node: ClusterNodeName },

    #[error("failed to start benchmark container image {image}")]
    StartContainerImage { image: String },

    #[error("Nervix image did not expose container port {port}")]
    UnexposedImagePort { port: u16 },

    #[error("failed to inspect image {image}")]
    InspectImage { image: String },

    #[error("failed to inspect image {image}: {stderr}")]
    ImageInspectionFailed { image: String, stderr: String },

    #[error("Nervix control plane did not become ready")]
    ControlPlaneNotReady,

    #[error(
        "Nervix control plane did not report all {expected} benchmark nodes: {}\ndiagnostics: {:?}",
        .outcome.message,
        .outcome.diagnostics
    )]
    ClusterIncomplete {
        expected: usize,
        outcome: Box<CommandOutcome>,
    },

    #[error("failed to query Nervix cluster status")]
    QueryClusterStatus,

    #[error("benchmark subject has no Nervix control endpoint")]
    MissingControlEndpoint,

    #[error("benchmark subject has no Nervix password")]
    MissingPassword,

    #[error("benchmark domain '{domain}' is invalid")]
    InvalidDomain { domain: String },

    #[error("failed to connect to Nervix at {url}")]
    Connect { url: String },

    #[error("failed to split the benchmark graph into statements")]
    SplitGraph,

    #[error("benchmark graph contains no statements")]
    EmptyGraph,

    #[error("failed to execute Nervix benchmark command: {statement}")]
    ExecuteCommand { statement: String },

    #[error(
        "Nervix benchmark command failed: {}\nstatement: {statement}\ndiagnostics: {:?}",
        .outcome.message,
        .outcome.diagnostics
    )]
    CommandRejected {
        statement: String,
        outcome: Box<CommandOutcome>,
    },

    #[error("failed to build the benchmark metrics client")]
    BuildMetricsClient,

    #[error("failed to scrape Nervix metrics from {url}")]
    ScrapeMetrics { url: reqwest::Url },

    #[error("failed to produce the Nervix benchmark metrics report")]
    MetricsReport,

    #[error("failed to resolve the benchmark executable")]
    LocateExecutable,

    #[error("benchmark executable {} has no parent directory", path.display())]
    ExecutableWithoutDirectory { path: PathBuf },

    #[error("benchmark load driver does not exist at {}", path.display())]
    MissingLoadDriver { path: PathBuf },

    #[error("failed to start load driver {}", path.display())]
    StartLoadDriver { path: PathBuf },

    #[error("failed to poll the load driver")]
    WatchLoadDriver,

    #[error("load driver exited before warmup with {status}:\n{diagnostics}")]
    LoadDriverExitedEarly {
        status: ExitStatus,
        diagnostics: String,
    },

    #[error("load driver did not complete consumer stabilization and warmup before timeout")]
    LoadDriverWarmupTimeout,

    #[error("load driver exceeded its bounded completion timeout")]
    LoadDriverCompletionTimeout,

    #[error("load driver failed with {status}:\n{diagnostics}")]
    LoadDriverFailed {
        status: ExitStatus,
        diagnostics: String,
    },
}

/// Which workload bound a run violates, beneath the [`BenchmarkCliError::Workload`] that names
/// the benchmark.
#[derive(Debug, Error)]
enum WorkloadError {
    #[error("partition count must be positive")]
    ZeroPartitions,

    #[error("partition count {partitions} exceeds Kafka's supported range")]
    PartitionsBeyondKafka { partitions: u32 },

    #[error("value byte count must be positive")]
    ZeroValueBytes,

    #[error("value byte count {value_bytes} must not exceed 1 MiB")]
    ValueBytesAboveLimit { value_bytes: u64 },

    #[error("maximum backlog must be positive")]
    ZeroBacklog,

    #[error("wait timeout must be positive")]
    ZeroWaitTimeout,

    #[error("warm-up duration must be positive")]
    ZeroWarmup,
}

/// What an image reference lacks, beneath the [`BenchmarkCliError::ImageReference`] that names
/// it.
#[derive(Debug, Error, PartialEq, Eq)]
enum ImageReferenceError {
    #[error("digest image references are not yet supported")]
    Digest,

    #[error("an image reference must include an explicit tag")]
    MissingTag,

    #[error("an image reference needs both a name and a tag")]
    Incomplete,
}

fn main() -> Result<(), Report<BenchmarkCliError>> {
    configure_process_lifecycle(ContainerMode::Ephemeral);
    let runtime = nervix_primitives::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .change_context(BenchmarkCliError::BuildRuntime)?;
    runtime.block_on(run())
}

async fn run() -> error_stack::Result<(), BenchmarkCliError> {
    run_command(Args::parse()).await
}

/// Runs one parsed benchmark command against the catalog of its repository root.
async fn run_command(args: Args) -> error_stack::Result<(), BenchmarkCliError> {
    let repository_root = args
        .repository_root
        .canonicalize()
        .change_context_lazy(|| BenchmarkCliError::ResolveRepositoryRoot {
            path: args.repository_root.clone(),
        })?;
    let catalog =
        BenchmarkCatalog::from_benchmarks_root(repository_root.join(DEFAULT_BENCHMARKS_ROOT));
    match args.command {
        BenchmarkCommand::List => list_benchmarks(&catalog),
        BenchmarkCommand::Run(run_args) => {
            run_benchmark(&repository_root, &catalog, *run_args).await?;
            Ok(())
        }
        BenchmarkCommand::RunAll(options) => {
            run_all_benchmarks(&repository_root, &catalog, *options).await
        }
        BenchmarkCommand::RunAb(ab_args) => {
            run_ab_benchmark(&repository_root, &catalog, *ab_args).await
        }
    }
}

fn list_benchmarks(catalog: &BenchmarkCatalog) -> error_stack::Result<(), BenchmarkCliError> {
    let benchmarks = catalog
        .discover()
        .change_context(BenchmarkCliError::Catalog)?;
    for benchmark in benchmarks {
        let implementations = benchmark
            .definition()
            .implementations
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "{}\t{}\t{}",
            benchmark.slug(),
            implementations,
            benchmark.definition().description
        );
    }
    Ok(())
}

async fn run_all_benchmarks(
    repository_root: &Path,
    catalog: &BenchmarkCatalog,
    options: RunOptions,
) -> error_stack::Result<(), BenchmarkCliError> {
    let benchmarks = catalog
        .discover()
        .change_context(BenchmarkCliError::Catalog)?;
    if benchmarks.is_empty() {
        return Err(Report::new(BenchmarkCliError::EmptyCatalog));
    }
    let artifacts_root = options.workload.resolve_artifacts_root(repository_root);
    fs::create_dir_all(&artifacts_root).change_context_lazy(|| {
        BenchmarkCliError::CreateDirectory {
            path: artifacts_root.clone(),
        }
    })?;
    let mut run_directories = Vec::new();
    let mut failures = Vec::new();
    for benchmark in benchmarks {
        let implementations = benchmark
            .definition()
            .implementations
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for implementation in implementations {
            nervix_primitives::task::consume_budget().await;
            let result = run_benchmark(
                repository_root,
                catalog,
                RunArgs {
                    benchmark: benchmark.slug().to_string(),
                    implementation: implementation.clone(),
                    options: options.clone(),
                },
            )
            .await;
            match result {
                Ok(run_directory) => run_directories.push(run_directory),
                Err(report) => {
                    let message = format!("{report:#}");
                    eprintln!(
                        "Benchmark '{}' implementation '{}' failed: {message}",
                        benchmark.slug(),
                        implementation
                    );
                    failures.push(BenchmarkRunFailure::new(
                        benchmark.slug(),
                        implementation,
                        message,
                    ));
                }
            }
        }
    }
    let report = BenchmarkSuiteReport::from_run_directories(&run_directories, failures)
        .change_context(BenchmarkCliError::Comparison)?;
    let comparison_path = artifacts_root.join("benchmark-comparison.md");
    report
        .write_markdown(&comparison_path)
        .change_context(BenchmarkCliError::Comparison)?;
    println!("comparison={}", comparison_path.display());
    let failed = report.failed_runs();
    if failed != 0 {
        return Err(Report::new(BenchmarkCliError::FailedImplementations {
            failed,
            total: report.total_runs(),
        }));
    }
    Ok(())
}

async fn run_benchmark(
    repository_root: &Path,
    catalog: &BenchmarkCatalog,
    args: RunArgs,
) -> error_stack::Result<PathBuf, BenchmarkCliError> {
    let benchmark =
        catalog
            .load(&args.benchmark)
            .change_context_lazy(|| BenchmarkCliError::LoadBenchmark {
                benchmark: args.benchmark.clone(),
            })?;
    let Some(implementation) = benchmark
        .definition()
        .implementations
        .get(&args.implementation)
    else {
        return Err(Report::new(BenchmarkCliError::UnknownImplementation {
            benchmark: benchmark.slug().to_string(),
            implementation: args.implementation.clone(),
        }));
    };
    let resolved = ResolvedRun::new(repository_root, &benchmark, &args)?;
    fs::create_dir_all(&resolved.run_directory).change_context_lazy(|| {
        BenchmarkCliError::CreateDirectory {
            path: resolved.run_directory.clone(),
        }
    })?;
    write_run_manifest(
        &resolved,
        benchmark.definition().description.as_str(),
        implementation,
        &args,
        repository_root,
    )?;

    println!(
        "Starting benchmark '{}' implementation '{}' ({}s, {} partitions)",
        resolved.slug, resolved.implementation, resolved.duration_seconds, resolved.partitions
    );
    let mut environment = DependencyEnvironment::new(
        format!("benchmark-{}", resolved.run_token),
        ContainerMode::Ephemeral,
    );
    let execution = execute_run(
        repository_root,
        &benchmark,
        implementation,
        &args,
        &resolved,
        &mut environment,
    )
    .await;
    let teardown = environment.shutdown().await;
    let outcome = match teardown {
        Ok(()) => execution,
        Err(failures) => match execution {
            Ok(()) => Err(Report::new(BenchmarkCliError::DependencyTeardown {
                failures,
            })),
            Err(report) => Err(report
                .change_context(BenchmarkCliError::DependencyTeardownAfterFailure { failures })),
        },
    };
    let status = if outcome.is_ok() { "pass\n" } else { "fail\n" };
    let status_path = resolved.run_directory.join("status.txt");
    fs::write(&status_path, status)
        .change_context(BenchmarkCliError::WriteFile { path: status_path })?;
    outcome?;
    Ok(resolved.run_directory)
}

struct AbRunArm {
    name: &'static str,
    label: String,
    server_binary: PathBuf,
    artifacts_root: PathBuf,
    run_directories: Vec<PathBuf>,
}

impl AbRunArm {
    fn new(
        repository_root: &Path,
        ab_root: &Path,
        name: &'static str,
        binary: &Path,
        label: Option<String>,
    ) -> error_stack::Result<Self, BenchmarkCliError> {
        let server_binary = absolute_or_repository_path(repository_root, binary);
        if !server_binary.is_file() {
            return Err(Report::new(BenchmarkCliError::MissingArmServerBinary {
                arm: name,
                path: server_binary,
            }));
        }
        Ok(Self {
            name,
            label: label.unwrap_or_else(|| server_binary.display().to_string()),
            server_binary,
            artifacts_root: ab_root.join(name),
            run_directories: Vec::new(),
        })
    }

    fn into_ab_arm(self) -> AbArm {
        AbArm {
            label: self.label,
            server_binary: self.server_binary,
            run_directories: self.run_directories,
        }
    }
}

async fn run_ab_benchmark(
    repository_root: &Path,
    catalog: &BenchmarkCatalog,
    args: RunAbArgs,
) -> error_stack::Result<(), BenchmarkCliError> {
    let ab_root = args
        .workload
        .resolve_artifacts_root(repository_root)
        .join("ab");
    let mut arms = [
        AbRunArm::new(
            repository_root,
            &ab_root,
            "baseline",
            &args.baseline_binary,
            args.baseline_label,
        )?,
        AbRunArm::new(
            repository_root,
            &ab_root,
            "candidate",
            &args.candidate_binary,
            args.candidate_label,
        )?,
    ];

    let runs = args.runs.get();
    for index in 0..runs {
        let run = index
            .checked_add(1)
            .assured("the run index stays below the requested run count, a usize");
        for arm in &mut arms {
            nervix_primitives::task::consume_budget().await;
            println!(
                "A/B run {run}/{runs} for the {} arm ({})",
                arm.name, arm.label
            );
            let options = RunOptions {
                nervix_mode: NervixMode::Local,
                nervix_image: None,
                server_binary: Some(arm.server_binary.clone()),
                workload: WorkloadOptions {
                    artifacts_root: Some(arm.artifacts_root.clone()),
                    ..args.workload.clone()
                },
            };
            let run_directory = run_benchmark(
                repository_root,
                catalog,
                RunArgs {
                    benchmark: args.benchmark.clone(),
                    implementation: "nervix".to_string(),
                    options,
                },
            )
            .await
            .change_context_lazy(|| BenchmarkCliError::AbRun {
                arm: arm.name,
                label: arm.label.clone(),
                run,
                runs,
            })?;
            arm.run_directories.push(run_directory);
        }
    }

    let [baseline, candidate] = arms;
    let summary = AbSummary::from_arms(baseline.into_ab_arm(), candidate.into_ab_arm())
        .change_context(BenchmarkCliError::AbComparison)?;
    println!("{}", summary.render_markdown());
    let summary_path = ab_root.join("ab-comparison.md");
    summary
        .write_markdown(&summary_path)
        .change_context(BenchmarkCliError::AbComparison)?;
    println!("ab-comparison={}", summary_path.display());
    Ok(())
}

async fn execute_run(
    repository_root: &Path,
    benchmark: &LoadedBenchmark,
    implementation: &Implementation,
    args: &RunArgs,
    resolved: &ResolvedRun,
    environment: &mut DependencyEnvironment,
) -> error_stack::Result<(), BenchmarkCliError> {
    start_declared_dependencies(environment, &benchmark.definition().dependencies).await?;
    let mut dependency_endpoints = BTreeMap::new();
    environment
        .endpoints()
        .apply_placeholders(&mut dependency_endpoints);
    let host_bootstrap = dependency_endpoint(environment, KAFKA_ADDR)?;
    let docker_bootstrap = dependency_endpoint(environment, KAFKA_DOCKER_ADDR)?;
    let docker_network = dependency_endpoint(environment, KAFKA_DOCKER_NETWORK)?;
    provision_topics(
        &host_bootstrap,
        &resolved.input_topic,
        &resolved.output_topic,
        resolved.partitions,
        resolved.wait_timeout,
    )
    .await
    .change_context(BenchmarkCliError::ProvisionTopics)?;

    let subject_bootstrap = match implementation {
        Implementation::Nervix(_) if args.options.nervix_mode == NervixMode::Local => {
            &host_bootstrap
        }
        Implementation::Nervix(_) | Implementation::Container(_) => &docker_bootstrap,
    };
    let render_inputs = KafkaRenderInputs {
        kafka_bootstrap_servers: subject_bootstrap,
        input_topic: &resolved.input_topic,
        output_topic: &resolved.output_topic,
        consumer_group: &resolved.consumer_group,
        lane_count: resolved.partitions,
        dependency_endpoints: &dependency_endpoints,
    };
    let rendered = benchmark
        .render_implementation_with_parameters(
            &resolved.implementation,
            render_inputs,
            &resolved.parameters,
        )
        .change_context_lazy(|| BenchmarkCliError::RenderImplementation {
            implementation: resolved.implementation.clone(),
        })?;
    let after_start = benchmark
        .render_after_start_with_parameters(
            &resolved.implementation,
            render_inputs,
            &resolved.parameters,
        )
        .change_context_lazy(|| BenchmarkCliError::RenderAfterStart {
            implementation: resolved.implementation.clone(),
        })?;
    let rendered_path = match implementation {
        Implementation::Nervix(_) => resolved.run_directory.join("graph.nspl"),
        Implementation::Container(container) => resolved.run_directory.join(
            container
                .config_path
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("subject.conf")),
        ),
    };
    fs::write(&rendered_path, &rendered).change_context(BenchmarkCliError::WriteFile {
        path: rendered_path,
    })?;
    if let Some(after_start) = &after_start {
        let path = resolved.run_directory.join("after-start.nspl");
        fs::write(&path, after_start).change_context(BenchmarkCliError::WriteFile { path })?;
    }

    let mut subject = match implementation {
        Implementation::Nervix(nervix) => {
            start_nervix(
                repository_root,
                nervix,
                args,
                resolved,
                environment,
                &docker_network,
            )
            .await?
        }
        Implementation::Container(container) => {
            start_container_subject(container, &rendered, resolved, environment, &docker_network)
                .await?
        }
    };

    let benchmark_result = async {
        if let Implementation::Nervix(_) = implementation {
            subject
                .configure_nervix(
                    &resolved.domain,
                    &rendered,
                    after_start.as_deref(),
                    resolved.wait_timeout,
                    resolved,
                )
                .await?;
        }
        run_load_driver(
            repository_root,
            args,
            resolved,
            &host_bootstrap,
            &mut subject,
            match implementation {
                Implementation::Nervix(_) => resolved.partitions,
                Implementation::Container(container)
                    if container.require_consumer_group_membership =>
                {
                    resolved.partitions
                }
                Implementation::Container(_) => 0,
            },
        )
        .await
    }
    .await;
    let container_diagnostics_result =
        capture_container_diagnostics(&environment.container_ids(), &resolved.run_directory).await;
    // A failed load is the run that most needs the subject's final metrics. Keep the benchmark
    // failure primary below, but attempt the scrape before logs and shutdown on every path.
    let metrics_result = subject
        .capture_nervix_metrics(
            &resolved.domain,
            &resolved.run_directory,
            resolved.wait_timeout,
        )
        .await;
    let log_result = subject.capture_logs(&resolved.run_directory).await;
    let stop_result = subject.stop().await;

    benchmark_result?;
    metrics_result?;
    log_result?;
    container_diagnostics_result?;
    stop_result?;
    let report_path = resolved.run_directory.join("load-report.txt");
    let report = fs::read_to_string(&report_path)
        .change_context(BenchmarkCliError::ReadFile { path: report_path })?;
    println!("{report}");
    println!("artifacts={}", resolved.run_directory.display());
    Ok(())
}

/// The address or name the started dependencies publish under `key`.
fn dependency_endpoint(
    environment: &DependencyEnvironment,
    key: &'static str,
) -> error_stack::Result<String, BenchmarkCliError> {
    let endpoint = environment
        .endpoints()
        .get(key)
        .change_context(BenchmarkCliError::DependencyEndpoint { key })?;
    Ok(endpoint.to_string())
}

async fn capture_container_diagnostics(
    container_ids: &[String],
    run_directory: &Path,
) -> error_stack::Result<(), BenchmarkCliError> {
    let directory = run_directory.join("container-diagnostics");
    fs::create_dir_all(&directory).change_context_lazy(|| BenchmarkCliError::CreateDirectory {
        path: directory.clone(),
    })?;
    for (index, container_id) in container_ids.iter().enumerate() {
        nervix_primitives::task::consume_budget().await;
        let short_id = container_id.chars().take(12).collect::<String>();
        if short_id.is_empty() || !short_id.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(Report::new(BenchmarkCliError::InvalidContainerId {
                container: container_id.clone(),
            }));
        }
        let prefix = directory.join(format!("{index:02}-{short_id}"));
        capture_docker_output(
            &["inspect", container_id],
            &prefix.with_extension("inspect.json"),
        )
        .await?;
        capture_docker_output(
            &[
                "stats",
                "--no-stream",
                "--format",
                "{{json .}}",
                container_id,
            ],
            &prefix.with_extension("stats.json"),
        )
        .await?;
        capture_docker_output(&["logs", container_id], &prefix.with_extension("log")).await?;
    }
    Ok(())
}

async fn capture_docker_output(
    arguments: &[&str],
    path: &Path,
) -> error_stack::Result<(), BenchmarkCliError> {
    let owned_arguments = || {
        arguments
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    };
    let output = Command::new("docker")
        .args(arguments)
        .output()
        .await
        .change_context_lazy(|| BenchmarkCliError::RunDocker {
            arguments: owned_arguments(),
        })?;
    if !output.status.success() {
        return Err(Report::new(BenchmarkCliError::DockerFailed {
            arguments: owned_arguments(),
            status: output.status,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        }));
    }
    let mut captured = output.stdout;
    if !output.stderr.is_empty() {
        captured.extend_from_slice(&output.stderr);
    }
    fs::write(path, captured).change_context_lazy(|| BenchmarkCliError::WriteFile {
        path: path.to_path_buf(),
    })
}

async fn start_declared_dependencies(
    environment: &mut DependencyEnvironment,
    dependencies: &[BenchmarkDependency],
) -> error_stack::Result<(), BenchmarkCliError> {
    for dependency in dependencies {
        nervix_primitives::task::consume_budget().await;
        let result = match dependency {
            BenchmarkDependency::Kafka => environment.start_kafka().await,
        };
        result.change_context(BenchmarkCliError::StartDependency {
            dependency: *dependency,
        })?;
    }
    Ok(())
}

impl ResolvedRun {
    fn new(
        repository_root: &Path,
        benchmark: &LoadedBenchmark,
        args: &RunArgs,
    ) -> error_stack::Result<Self, BenchmarkCliError> {
        let settings = RunSettings::resolve(
            benchmark.definition(),
            &args.options.workload.parameter_overrides,
            args.options.workload.duration_seconds,
        )
        .change_context_lazy(|| BenchmarkCliError::RunSettings {
            benchmark: benchmark.slug().to_string(),
        })?;
        let bounds = WorkloadBounds {
            partitions: args
                .options
                .workload
                .partitions
                .unwrap_or(benchmark.definition().load.partitions),
            value_bytes: args
                .options
                .workload
                .value_bytes
                .unwrap_or(benchmark.definition().load.value_bytes),
            max_backlog_messages: args
                .options
                .workload
                .max_backlog_messages
                .unwrap_or(benchmark.definition().load.max_backlog_messages),
            wait_timeout_seconds: args
                .options
                .workload
                .wait_timeout_seconds
                .unwrap_or(benchmark.definition().load.wait_timeout_seconds),
            warmup_seconds: args
                .options
                .workload
                .warmup_seconds
                .unwrap_or(benchmark.definition().load.warmup_seconds),
        };
        bounds
            .validate()
            .change_context_lazy(|| BenchmarkCliError::Workload {
                benchmark: benchmark.slug().to_string(),
            })?;

        let run_token = Uuid::now_v7()
            .as_simple()
            .to_string()
            .chars()
            .take(16)
            .collect::<String>();
        let topic_prefix = format!(
            "nervix_bench_{}_{}",
            benchmark.slug().replace('-', "_"),
            run_token
        );
        let artifacts_root = args
            .options
            .workload
            .resolve_artifacts_root(repository_root);
        let run_directory = artifacts_root
            .join(benchmark.slug())
            .join(&args.implementation)
            .join(&run_token);
        Ok(Self {
            slug: benchmark.slug().to_string(),
            implementation: args.implementation.clone(),
            partitions: bounds.partitions,
            value_bytes: bounds.value_bytes,
            max_backlog_messages: bounds.max_backlog_messages,
            wait_timeout: Duration::from_secs(bounds.wait_timeout_seconds),
            duration_seconds: settings.duration_seconds,
            warmup_seconds: bounds.warmup_seconds,
            shape: benchmark.definition().load.shape.clone(),
            parameters: settings.parameters,
            input_topic: format!("{topic_prefix}_input"),
            output_topic: format!("{topic_prefix}_output"),
            consumer_group: format!("{topic_prefix}_consumer"),
            domain: format!("benchmark_{run_token}"),
            run_token,
            run_directory,
        })
    }
}

async fn start_nervix(
    repository_root: &Path,
    implementation: &NervixImplementation,
    args: &RunArgs,
    resolved: &ResolvedRun,
    environment: &mut DependencyEnvironment,
    docker_network: &str,
) -> error_stack::Result<Subject, BenchmarkCliError> {
    let password = format!("benchmark-{}", resolved.run_token);
    let cluster_id = format!("benchmark-{}", resolved.run_token);
    let node_count = usize::from(implementation.nodes);
    match args.options.nervix_mode {
        NervixMode::Local => {
            let server_binary = absolute_or_repository_path(
                repository_root,
                args.options
                    .server_binary
                    .as_deref()
                    .unwrap_or(Path::new(DEFAULT_SERVER_BINARY)),
            );
            if !server_binary.is_file() {
                return Err(Report::new(BenchmarkCliError::MissingServerBinary {
                    path: server_binary,
                }));
            }
            let ports = (0..node_count)
                .map(|_| LocalPorts::reserve())
                .collect::<io::Result<Vec<_>>>()
                .change_context(BenchmarkCliError::ReservePorts)?;
            let node_names = benchmark_node_names(node_count);
            let endpoint_names = node_names
                .iter()
                .map(|node| node.as_str().to_string())
                .collect::<Vec<_>>();
            let tls = InterconnectTlsFiles::generate(
                &resolved.run_directory.join("interconnect-tls"),
                &cluster_id,
                &node_names,
                &endpoint_names,
            )?;
            let bootstrap_addr = format!("127.0.0.1:{}", ports[0].interconnect);
            let mut nodes = Vec::with_capacity(node_count);
            for index in 0..node_count {
                nervix_primitives::task::consume_budget().await;
                let node_id = &node_names[index];
                let state_directory = resolved
                    .run_directory
                    .join("nervix-state")
                    .join(node_id.as_str());
                fs::create_dir_all(&state_directory).change_context_lazy(|| {
                    BenchmarkCliError::CreateDirectory {
                        path: state_directory.clone(),
                    }
                })?;
                let log_name = if node_count == 1 {
                    "subject.log".to_string()
                } else {
                    format!("subject-{}.log", node_id.as_str())
                };
                let log_path = resolved.run_directory.join(log_name);
                let log = fs::File::create(&log_path).change_context_lazy(|| {
                    BenchmarkCliError::CreateFile {
                        path: log_path.clone(),
                    }
                })?;
                let stderr = log
                    .try_clone()
                    .change_context(BenchmarkCliError::DuplicateLog { path: log_path })?;
                let mut command = Command::new(&server_binary);
                command
                    .current_dir(repository_root)
                    .env("NERVIX_INIT_DEFAULT_USER_PASSWORD", &password)
                    .env("NERVIX_DB_PATH", state_directory.join("db"))
                    .env("RUST_LOG", "info")
                    .args(ports[index].server_arguments(
                        node_id,
                        &cluster_id,
                        &tls.ca,
                        &tls.nodes[index],
                        (index != 0).then_some(bootstrap_addr.as_str()),
                    ))
                    .stdout(Stdio::from(log))
                    .stderr(Stdio::from(stderr))
                    .kill_on_drop(true);
                let child =
                    command
                        .spawn()
                        .change_context_lazy(|| BenchmarkCliError::StartLocalNode {
                            node: node_id.clone(),
                        })?;
                let mut node = LocalNode {
                    name: node_id.clone(),
                    child,
                };
                node.wait_until_ready(ports[index].observability, resolved.wait_timeout)
                    .await?;
                nodes.push(node);
            }
            let mut metrics_urls = Vec::with_capacity(node_count);
            for ports in &ports {
                let url = format!("http://127.0.0.1:{}/metrics", ports.observability);
                let metrics_url = reqwest::Url::parse(&url)
                    .change_context(BenchmarkCliError::MetricsUrl { url })?;
                metrics_urls.push(metrics_url);
            }
            Ok(Subject {
                runtime: SubjectRuntime::Local { nodes },
                control_url: Some(format!("http://127.0.0.1:{}", ports[0].grpc)),
                metrics_urls,
                password: Some(password),
                node_count,
            })
        }
        NervixMode::Image => {
            let Some(image) = args.options.nervix_image.as_deref() else {
                return Err(Report::new(BenchmarkCliError::MissingNervixImage));
            };
            let (image_name, image_tag) =
                split_image_reference(image).change_context_lazy(|| {
                    BenchmarkCliError::ImageReference {
                        image: image.to_string(),
                    }
                })?;
            let timeout = resolved.wait_timeout;
            let node_names = benchmark_node_names(node_count);
            let container_names = node_names
                .iter()
                .map(|node| format!("nervix-benchmark-{}-{}", resolved.run_token, node.as_str()))
                .collect::<Vec<_>>();
            let tls = InterconnectTlsFiles::generate(
                &resolved.run_directory.join("interconnect-tls"),
                &cluster_id,
                &node_names,
                &container_names,
            )?;
            let bootstrap_addr = format!("{}:{NERVIX_INTERCONNECT_PORT}", container_names[0]);
            let mut infos = Vec::with_capacity(node_count);
            for index in 0..node_count {
                nervix_primitives::task::consume_budget().await;
                let node_id = node_names[index].clone();
                let container_name = container_names[index].clone();
                let server_args = container_server_arguments(
                    &node_id,
                    &cluster_id,
                    &container_name,
                    (index != 0).then_some(bootstrap_addr.as_str()),
                );
                let network = docker_network.to_string();
                let password_for_container = password.clone();
                let ca = tls.ca.clone();
                let certificate = tls.nodes[index].certificate.clone();
                let private_key = tls.nodes[index].private_key.clone();
                let image_name = image_name.clone();
                let image_tag = image_tag.clone();
                let info = environment
                    .start_generic(
                        NERVIX_CONTAINER_ROLES[index],
                        "Nervix benchmark image",
                        ContainerReadiness::Running,
                        &[NERVIX_GRPC_PORT, NERVIX_OBSERVABILITY_PORT],
                        move || {
                            GenericImage::new(image_name.clone(), image_tag.clone())
                                .with_exposed_port(NERVIX_GRPC_PORT)
                                .with_exposed_port(NERVIX_OBSERVABILITY_PORT)
                                .with_wait_for(WaitFor::http(
                                    HttpWaitStrategy::new("/readyz")
                                        .with_port(NERVIX_OBSERVABILITY_PORT)
                                        .with_expected_status_code(200_u16),
                                ))
                                .with_network(network.clone())
                                .with_container_name(container_name.clone())
                                .with_copy_to("/tmp/nervix-interconnect-ca.pem", ca.clone())
                                .with_copy_to(
                                    "/tmp/nervix-interconnect-node.pem",
                                    certificate.clone(),
                                )
                                .with_copy_to(
                                    "/tmp/nervix-interconnect-node-key.pem",
                                    private_key.clone(),
                                )
                                .with_env_var(
                                    "NERVIX_INIT_DEFAULT_USER_PASSWORD",
                                    password_for_container.clone(),
                                )
                                .with_env_var("RUST_LOG", "info")
                                .with_cmd(server_args.clone())
                                .with_startup_timeout(timeout)
                        },
                    )
                    .await
                    .change_context(BenchmarkCliError::StartNervixImage { node: node_id })?;
                infos.push(info);
            }
            write_image_identity(&resolved.run_directory, image).await?;
            let Some(grpc_port) = infos[0].host_port(NERVIX_GRPC_PORT) else {
                return Err(Report::new(BenchmarkCliError::UnexposedImagePort {
                    port: NERVIX_GRPC_PORT.as_u16(),
                }));
            };
            let mut metrics_urls = Vec::with_capacity(infos.len());
            for info in &infos {
                let Some(observability_port) = info.host_port(NERVIX_OBSERVABILITY_PORT) else {
                    return Err(Report::new(BenchmarkCliError::UnexposedImagePort {
                        port: NERVIX_OBSERVABILITY_PORT.as_u16(),
                    }));
                };
                let url = format!("http://127.0.0.1:{observability_port}/metrics");
                let metrics_url = reqwest::Url::parse(&url)
                    .change_context(BenchmarkCliError::MetricsUrl { url })?;
                metrics_urls.push(metrics_url);
            }
            Ok(Subject {
                runtime: SubjectRuntime::Container { infos },
                control_url: Some(format!("http://127.0.0.1:{grpc_port}")),
                metrics_urls,
                password: Some(password),
                node_count,
            })
        }
    }
}

async fn start_container_subject(
    implementation: &ContainerImplementation,
    rendered: &str,
    resolved: &ResolvedRun,
    environment: &mut DependencyEnvironment,
    docker_network: &str,
) -> error_stack::Result<Subject, BenchmarkCliError> {
    let (image_name, image_tag) = split_image_reference(&implementation.image)
        .change_context_lazy(|| BenchmarkCliError::ImageReference {
            image: implementation.image.clone(),
        })?;
    let config_path = utf8_path(&implementation.config_path)?.to_string();
    let readiness_port = implementation.readiness_port.map(ContainerPort::Tcp);
    let mapped_ports = readiness_port.into_iter().collect::<Vec<_>>();
    let readiness_path = implementation.readiness_path.clone();
    let readiness_log = implementation.readiness_log.clone();
    let command = implementation.command.clone();
    let network = docker_network.to_string();
    let configuration = rendered.as_bytes().to_vec();
    let timeout = resolved.wait_timeout;
    let info = environment
        .start_generic(
            "benchmark-subject",
            "benchmark subject",
            ContainerReadiness::Running,
            &mapped_ports,
            move || {
                let mut image = GenericImage::new(image_name.clone(), image_tag.clone());
                if let (Some(port), Some(path)) = (readiness_port, readiness_path.as_deref()) {
                    image = image.with_exposed_port(port).with_wait_for(WaitFor::http(
                        HttpWaitStrategy::new(path)
                            .with_port(port)
                            .with_expected_status_code(200_u16),
                    ));
                }
                if let Some(message) = readiness_log.as_deref() {
                    image = image.with_wait_for(WaitFor::message_on_stdout(message));
                }
                let request = image
                    .with_network(network.clone())
                    .with_copy_to(
                        CopyTargetOptions::new(config_path.clone()).with_mode(0o644),
                        configuration.clone(),
                    )
                    .with_startup_timeout(timeout);
                if let Some(command) = &command {
                    request.with_cmd(command.clone())
                } else {
                    request
                }
            },
        )
        .await
        .change_context_lazy(|| BenchmarkCliError::StartContainerImage {
            image: implementation.image.clone(),
        })?;
    write_image_identity(&resolved.run_directory, &implementation.image).await?;
    Ok(Subject {
        runtime: SubjectRuntime::Container { infos: vec![info] },
        control_url: None,
        metrics_urls: Vec::new(),
        password: None,
        node_count: 1,
    })
}

impl LocalNode {
    async fn wait_until_ready(
        &mut self,
        observability_port: u16,
        timeout: Duration,
    ) -> error_stack::Result<(), BenchmarkCliError> {
        let url = format!("http://127.0.0.1:{observability_port}/readyz");
        let client = reqwest::Client::new();
        let deadline = nervix_primitives::time::Instant::now() + timeout;
        loop {
            nervix_primitives::task::consume_budget().await;
            let exited = self.child.try_wait().change_context_lazy(|| {
                BenchmarkCliError::WatchLocalNode {
                    node: self.name.clone(),
                }
            })?;
            if let Some(status) = exited {
                return Err(Report::new(BenchmarkCliError::LocalNodeExitedBeforeReady {
                    node: self.name.clone(),
                    status,
                }));
            }
            if let Ok(response) = client.get(&url).send().await
                && response.status().is_success()
            {
                return Ok(());
            }
            if nervix_primitives::time::Instant::now() >= deadline {
                return Err(Report::new(BenchmarkCliError::LocalNodeNotReady {
                    node: self.name.clone(),
                    url,
                }));
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Subject {
    async fn configure_nervix(
        &mut self,
        domain: &str,
        graph: &str,
        after_start: Option<&str>,
        timeout: Duration,
        resolved: &ResolvedRun,
    ) -> error_stack::Result<(), BenchmarkCliError> {
        let deadline = nervix_primitives::time::Instant::now() + timeout;
        let client = loop {
            nervix_primitives::task::consume_budget().await;
            let connection = async {
                let client = self.connect_client(DEFAULT_DOMAIN).await?;
                let outcome = client
                    .execute("SHOW CLUSTER STATUS;")
                    .await
                    .change_context(BenchmarkCliError::QueryClusterStatus)?;
                Ok::<_, Report<BenchmarkCliError>>((client, outcome))
            }
            .await;
            match connection {
                Ok((client, outcome))
                    if outcome.succeeded()
                        && cluster_status_is_ready(&outcome.message, self.node_count) =>
                {
                    let status_path = resolved.run_directory.join("cluster-status.txt");
                    fs::write(&status_path, format!("{outcome:#?}\n"))
                        .change_context(BenchmarkCliError::WriteFile { path: status_path })?;
                    break client;
                }
                Ok(_) | Err(_) if nervix_primitives::time::Instant::now() < deadline => {
                    self.ensure_running()?;
                    nervix_primitives::time::sleep(Duration::from_millis(100)).await;
                }
                Ok((_, outcome)) => {
                    return Err(Report::new(BenchmarkCliError::ClusterIncomplete {
                        expected: self.node_count,
                        outcome: Box::new(outcome),
                    }));
                }
                Err(report) => {
                    return Err(report.change_context(BenchmarkCliError::ControlPlaneNotReady));
                }
            }
        };
        self.execute_checked(
            &client,
            &format!("CREATE UNPACED DOMAIN {domain};"),
            &resolved.run_directory.join("create-domain.txt"),
        )
        .await?;
        let selected =
            DomainName::parse(domain).change_context_lazy(|| BenchmarkCliError::InvalidDomain {
                domain: domain.to_string(),
            })?;
        client.set_domain(Some(selected)).await;
        self.execute_graph_statements(
            &client,
            graph,
            &resolved.run_directory.join("create-graph.txt"),
        )
        .await?;
        self.execute_checked(
            &client,
            "START;",
            &resolved.run_directory.join("start-domain.txt"),
        )
        .await?;
        if let Some(after_start) = after_start {
            self.execute_graph_statements(
                &client,
                after_start,
                &resolved.run_directory.join("after-start.txt"),
            )
            .await?;
        }
        Ok(())
    }

    /// Submits the graph one statement at a time.
    ///
    /// A leader that holds no binding for the session reports the transaction as detached, which
    /// is a routing condition the client recovers from by attaching again. That recovery only
    /// applies to a statement the client has already adopted a transaction from, so submitting
    /// the whole graph as a single command leaves a detach unrecoverable, and recovering one
    /// mid-command could leave the graph partially queued before `COMMIT`.
    async fn execute_graph_statements(
        &self,
        client: &Client,
        graph: &str,
        output_path: &Path,
    ) -> error_stack::Result<(), BenchmarkCliError> {
        let statements = graph_statements(graph)?;
        let mut transcript = String::new();
        for statement in statements {
            nervix_primitives::task::consume_budget().await;
            let source = statement.trim();
            let outcome = client.execute(source).await.change_context_lazy(|| {
                BenchmarkCliError::ExecuteCommand {
                    statement: source.to_string(),
                }
            })?;
            transcript.push_str(&format!("{source}\n{outcome:#?}\n\n"));
            if !outcome.succeeded() {
                fs::write(output_path, &transcript).change_context_lazy(|| {
                    BenchmarkCliError::WriteFile {
                        path: output_path.to_path_buf(),
                    }
                })?;
                return Err(Report::new(BenchmarkCliError::CommandRejected {
                    statement: source.to_string(),
                    outcome: Box::new(outcome),
                }));
            }
        }
        fs::write(output_path, transcript).change_context_lazy(|| BenchmarkCliError::WriteFile {
            path: output_path.to_path_buf(),
        })
    }

    async fn connect_client(&self, domain: &str) -> error_stack::Result<Client, BenchmarkCliError> {
        let Some(control_url) = self.control_url.as_deref() else {
            return Err(Report::new(BenchmarkCliError::MissingControlEndpoint));
        };
        let Some(password) = self.password.as_deref() else {
            return Err(Report::new(BenchmarkCliError::MissingPassword));
        };
        let selected =
            DomainName::parse(domain).change_context_lazy(|| BenchmarkCliError::InvalidDomain {
                domain: domain.to_string(),
            })?;
        Client::connect_with_options(
            control_url,
            Some(selected),
            ConnectOptions::default().with_basic_auth(DEFAULT_USERNAME, password),
        )
        .await
        .change_context_lazy(|| BenchmarkCliError::Connect {
            url: control_url.to_string(),
        })
    }

    async fn execute_checked(
        &self,
        client: &Client,
        query: &str,
        output_path: &Path,
    ) -> error_stack::Result<(), BenchmarkCliError> {
        let outcome = client.execute(query).await.change_context_lazy(|| {
            BenchmarkCliError::ExecuteCommand {
                statement: query.to_string(),
            }
        })?;
        fs::write(output_path, format!("{outcome:#?}\n")).change_context_lazy(|| {
            BenchmarkCliError::WriteFile {
                path: output_path.to_path_buf(),
            }
        })?;
        if !outcome.succeeded() {
            return Err(Report::new(BenchmarkCliError::CommandRejected {
                statement: query.to_string(),
                outcome: Box::new(outcome),
            }));
        }
        Ok(())
    }

    fn ensure_running(&mut self) -> error_stack::Result<(), BenchmarkCliError> {
        if let SubjectRuntime::Local { nodes } = &mut self.runtime {
            for node in nodes.iter_mut() {
                let exited = node.child.try_wait().change_context_lazy(|| {
                    BenchmarkCliError::WatchLocalNode {
                        node: node.name.clone(),
                    }
                })?;
                if let Some(status) = exited {
                    return Err(Report::new(BenchmarkCliError::LocalNodeExited {
                        node: node.name.clone(),
                        status,
                    }));
                }
            }
        }
        Ok(())
    }

    async fn capture_nervix_metrics(
        &self,
        domain: &str,
        run_directory: &Path,
        timeout: Duration,
    ) -> error_stack::Result<(), BenchmarkCliError> {
        if self.metrics_urls.is_empty() {
            return Ok(());
        }
        let client = reqwest::Client::builder()
            .timeout(timeout.min(Duration::from_secs(30)))
            .build()
            .change_context(BenchmarkCliError::BuildMetricsClient)?;
        let mut prometheus_scrapes = Vec::with_capacity(self.metrics_urls.len());
        for metrics_url in &self.metrics_urls {
            nervix_primitives::task::consume_budget().await;
            let scrape_failed = || BenchmarkCliError::ScrapeMetrics {
                url: metrics_url.clone(),
            };
            let response = client
                .get(metrics_url.clone())
                .send()
                .await
                .change_context_lazy(scrape_failed)?;
            let response = response
                .error_for_status()
                .change_context_lazy(scrape_failed)?;
            let scrape = response.text().await.change_context_lazy(scrape_failed)?;
            prometheus_scrapes.push(scrape);
        }
        let prometheus = prometheus_scrapes.join("\n");
        let prometheus_path = run_directory.join(NERVIX_METRICS_PROMETHEUS_FILE);
        fs::write(&prometheus_path, &prometheus).change_context(BenchmarkCliError::WriteFile {
            path: prometheus_path,
        })?;
        let report = NervixMetricsReport::from_prometheus_scrapes(
            prometheus_scrapes.iter().map(String::as_str),
            domain,
        )
        .change_context(BenchmarkCliError::MetricsReport)?;
        report
            .write(run_directory.join(NERVIX_METRICS_REPORT_FILE))
            .change_context(BenchmarkCliError::MetricsReport)
    }

    async fn capture_logs(
        &self,
        run_directory: &Path,
    ) -> error_stack::Result<(), BenchmarkCliError> {
        let SubjectRuntime::Container { infos } = &self.runtime else {
            return Ok(());
        };
        for (index, info) in infos.iter().enumerate() {
            nervix_primitives::task::consume_budget().await;
            let output = Command::new("docker")
                .args(["logs", info.id()])
                .output()
                .await
                .change_context_lazy(|| BenchmarkCliError::CollectContainerLogs {
                    container: info.id().to_string(),
                })?;
            let mut logs = output.stdout;
            logs.extend_from_slice(&output.stderr);
            let node_number = index
                .checked_add(1)
                .assured("a benchmark cluster has at most three nodes");
            let name = if infos.len() == 1 {
                "subject.log".to_string()
            } else {
                format!("subject-node-{node_number}.log")
            };
            let path = run_directory.join(name);
            fs::write(&path, logs).change_context(BenchmarkCliError::WriteFile { path })?;
        }
        Ok(())
    }

    async fn stop(&mut self) -> error_stack::Result<(), BenchmarkCliError> {
        if let SubjectRuntime::Local { nodes } = &mut self.runtime {
            for node in nodes.iter_mut().rev() {
                nervix_primitives::task::consume_budget().await;
                let stop_failed = || BenchmarkCliError::StopLocalNode {
                    node: node.name.clone(),
                };
                let exited = node.child.try_wait().change_context_lazy(stop_failed)?;
                if exited.is_none() {
                    node.child.start_kill().change_context_lazy(stop_failed)?;
                    node.child.wait().await.change_context_lazy(stop_failed)?;
                }
            }
        }
        Ok(())
    }
}

async fn run_load_driver(
    repository_root: &Path,
    args: &RunArgs,
    resolved: &ResolvedRun,
    bootstrap_servers: &str,
    subject: &mut Subject,
    minimum_consumers: u32,
) -> error_stack::Result<(), BenchmarkCliError> {
    let load_driver = match &args.options.workload.load_driver {
        Some(path) => absolute_or_repository_path(repository_root, path),
        None => sibling_binary("nervix-benchmark-load")?,
    };
    if !load_driver.is_file() {
        return Err(Report::new(BenchmarkCliError::MissingLoadDriver {
            path: load_driver,
        }));
    }
    let ready_file = resolved.run_directory.join("load-ready");
    let go_file = resolved.run_directory.join("load-go");
    let output_diagnostics_file = resolved.run_directory.join("output-diagnostics.json");
    let stdout_path = resolved.run_directory.join("load-report.txt");
    let stderr_path = resolved.run_directory.join("load-driver.log");
    let stdout =
        fs::File::create(&stdout_path).change_context_lazy(|| BenchmarkCliError::CreateFile {
            path: stdout_path.clone(),
        })?;
    let stderr =
        fs::File::create(&stderr_path).change_context_lazy(|| BenchmarkCliError::CreateFile {
            path: stderr_path.clone(),
        })?;
    let mut child = Command::new(&load_driver)
        .args([
            "--bootstrap-servers",
            bootstrap_servers,
            "--input-topic",
            &resolved.input_topic,
            "--output-topic",
            &resolved.output_topic,
            "--consumer-group",
            &resolved.consumer_group,
            "--minimum-consumers",
            &minimum_consumers.to_string(),
            "--duration-seconds",
            &resolved.duration_seconds.to_string(),
            "--warmup-seconds",
            &resolved.warmup_seconds.to_string(),
            "--value-bytes",
            &resolved.value_bytes.to_string(),
            "--max-backlog-messages",
            &resolved.max_backlog_messages.to_string(),
            "--wait-timeout-seconds",
            &resolved.wait_timeout.as_secs().to_string(),
            "--ready-file",
            utf8_path(&ready_file)?,
            "--go-file",
            utf8_path(&go_file)?,
            "--output-diagnostics-file",
            utf8_path(&output_diagnostics_file)?,
        ])
        .args(shape_arguments(&resolved.shape))
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .change_context_lazy(|| BenchmarkCliError::StartLoadDriver {
            path: load_driver.clone(),
        })?;

    let ready_deadline = nervix_primitives::time::Instant::now() + resolved.wait_timeout;
    loop {
        nervix_primitives::task::consume_budget().await;
        if ready_file.exists() {
            break;
        }
        let exited = child
            .try_wait()
            .change_context(BenchmarkCliError::WatchLoadDriver)?;
        if let Some(status) = exited {
            let diagnostics = fs::read_to_string(&stderr_path).unwrap_or_default();
            return Err(Report::new(BenchmarkCliError::LoadDriverExitedEarly {
                status,
                diagnostics,
            }));
        }
        subject.ensure_running()?;
        if nervix_primitives::time::Instant::now() >= ready_deadline {
            return Err(Report::new(BenchmarkCliError::LoadDriverWarmupTimeout));
        }
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    }
    fs::write(&go_file, b"go\n").change_context(BenchmarkCliError::WriteFile { path: go_file })?;

    const COMPLETION_GRACE_SECONDS: u64 = 30;
    const SECONDS_FIT: &str =
        "a benchmark run is configured in seconds, far below the u64 second range";

    let waited = resolved
        .wait_timeout
        .as_secs()
        .checked_mul(4)
        .assured(SECONDS_FIT);
    let run = waited
        .checked_add(resolved.duration_seconds)
        .assured(SECONDS_FIT);
    let completion_timeout = Duration::from_secs(
        run.checked_add(COMPLETION_GRACE_SECONDS)
            .assured(SECONDS_FIT),
    );
    let completion_deadline = nervix_primitives::time::Instant::now() + completion_timeout;
    let status = loop {
        nervix_primitives::task::consume_budget().await;
        let exited = child
            .try_wait()
            .change_context(BenchmarkCliError::WatchLoadDriver)?;
        if let Some(status) = exited {
            break status;
        }
        subject.ensure_running()?;
        if nervix_primitives::time::Instant::now() >= completion_deadline {
            return Err(Report::new(BenchmarkCliError::LoadDriverCompletionTimeout));
        }
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    };
    if !status.success() {
        let diagnostics = fs::read_to_string(&stderr_path).unwrap_or_default();
        return Err(Report::new(BenchmarkCliError::LoadDriverFailed {
            status,
            diagnostics,
        }));
    }
    Ok(())
}

struct LocalPorts {
    grpc: u16,
    http: u16,
    https: u16,
    observability: u16,
    web_console: u16,
    interconnect: u16,
}

impl LocalPorts {
    fn reserve() -> io::Result<Self> {
        let ports = reserve_available_ports(6)?;
        Ok(Self {
            grpc: ports[0],
            http: ports[1],
            https: ports[2],
            observability: ports[3],
            web_console: ports[4],
            interconnect: ports[5],
        })
    }

    fn server_arguments(
        &self,
        node_id: &ClusterNodeName,
        cluster_id: &str,
        ca: &Path,
        tls: &InterconnectNodeTlsFiles,
        bootstrap_addr: Option<&str>,
    ) -> Vec<String> {
        let mut arguments = vec![
            "--node-id".to_string(),
            node_id.as_str().to_string(),
            "--cluster-id".to_string(),
            cluster_id.to_string(),
            "--addr".to_string(),
            format!("127.0.0.1:{}", self.grpc),
            "--http-listen-addr".to_string(),
            format!("127.0.0.1:{}", self.http),
            "--https-listen-addr".to_string(),
            format!("127.0.0.1:{}", self.https),
            "--observability-listen-addr".to_string(),
            format!("127.0.0.1:{}", self.observability),
            "--web-console-listen-addr".to_string(),
            format!("127.0.0.1:{}", self.web_console),
            "--interconnect-listen-addr".to_string(),
            format!("127.0.0.1:{}", self.interconnect),
            "--interconnect-advertise-addr".to_string(),
            format!("127.0.0.1:{}", self.interconnect),
            "--interconnect-tls-ca".to_string(),
            ca.display().to_string(),
            "--interconnect-tls-cert".to_string(),
            tls.certificate.display().to_string(),
            "--interconnect-tls-key".to_string(),
            tls.private_key.display().to_string(),
        ];
        match bootstrap_addr {
            Some(bootstrap_addr) => {
                arguments.push("--cluster-bootstrap-host".to_string());
                arguments.push(bootstrap_addr.to_string());
            }
            None => arguments.push("--allow-bootstrap".to_string()),
        }
        arguments
    }
}

#[derive(Clone)]
struct InterconnectTlsFiles {
    ca: PathBuf,
    nodes: Vec<InterconnectNodeTlsFiles>,
}

#[derive(Clone)]
struct InterconnectNodeTlsFiles {
    certificate: PathBuf,
    private_key: PathBuf,
}

impl InterconnectTlsFiles {
    fn generate(
        directory: &Path,
        cluster_id: &str,
        node_ids: &[ClusterNodeName],
        endpoint_names: &[String],
    ) -> error_stack::Result<Self, BenchmarkCliError> {
        if node_ids.len() != endpoint_names.len() {
            return Err(Report::new(BenchmarkCliError::TlsEndpointCount {
                nodes: node_ids.len(),
                endpoints: endpoint_names.len(),
            }));
        }
        fs::create_dir_all(directory).change_context_lazy(|| {
            BenchmarkCliError::CreateDirectory {
                path: directory.to_path_buf(),
            }
        })?;

        let mut authority_parameters = CertificateParams::default();
        authority_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        authority_parameters.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let authority_key =
            KeyPair::generate().change_context(BenchmarkCliError::GenerateAuthority)?;
        let authority = authority_parameters
            .self_signed(&authority_key)
            .change_context(BenchmarkCliError::GenerateAuthority)?;
        let ca = directory.join("ca.pem");
        fs::write(&ca, authority.pem())
            .change_context_lazy(|| BenchmarkCliError::WriteFile { path: ca.clone() })?;

        let mut nodes = Vec::with_capacity(node_ids.len());
        for (node_id, endpoint_name) in node_ids.iter().zip(endpoint_names) {
            let node_directory = directory.join(node_id.as_str());
            fs::create_dir_all(&node_directory).change_context_lazy(|| {
                BenchmarkCliError::CreateDirectory {
                    path: node_directory.clone(),
                }
            })?;
            let certificate_failed = || BenchmarkCliError::GenerateNodeCertificate {
                node: node_id.clone(),
            };
            let mut parameters = CertificateParams::new(vec![
                "localhost".to_string(),
                "127.0.0.1".to_string(),
                endpoint_name.clone(),
            ])
            .change_context_lazy(certificate_failed)?;
            let identity = Ia5String::try_from(format!(
                "nervix://cluster/{cluster_id}/node/{}",
                node_id.as_str()
            ))
            .change_context_lazy(certificate_failed)?;
            parameters.subject_alt_names.push(SanType::URI(identity));
            parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            parameters.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let key = KeyPair::generate().change_context_lazy(certificate_failed)?;
            let certificate = parameters
                .signed_by(&key, &authority, &authority_key)
                .change_context_lazy(certificate_failed)?;
            let certificate_path = node_directory.join("node.pem");
            let private_key_path = node_directory.join("node-key.pem");
            fs::write(&certificate_path, certificate.pem()).change_context_lazy(|| {
                BenchmarkCliError::WriteFile {
                    path: certificate_path.clone(),
                }
            })?;
            fs::write(&private_key_path, key.serialize_pem()).change_context_lazy(|| {
                BenchmarkCliError::WriteFile {
                    path: private_key_path.clone(),
                }
            })?;
            nodes.push(InterconnectNodeTlsFiles {
                certificate: certificate_path,
                private_key: private_key_path,
            });
        }
        Ok(Self { ca, nodes })
    }
}

fn benchmark_node_names(node_count: usize) -> Vec<ClusterNodeName> {
    (1..=node_count)
        .map(|node_number| {
            ClusterNodeName::parse(&format!("node-{node_number}"))
                .assured("numbers from one through three produce valid benchmark node names")
        })
        .collect()
}

fn container_server_arguments(
    node_id: &ClusterNodeName,
    cluster_id: &str,
    container_name: &str,
    bootstrap_addr: Option<&str>,
) -> Vec<String> {
    let mut arguments = vec![
        "/usr/local/bin/nervix-server".to_string(),
        "--node-id".to_string(),
        node_id.as_str().to_string(),
        "--cluster-id".to_string(),
        cluster_id.to_string(),
        "--addr".to_string(),
        "0.0.0.0:47391".to_string(),
        "--http-listen-addr".to_string(),
        format!("0.0.0.0:{NERVIX_HTTP_PORT}"),
        "--https-listen-addr".to_string(),
        format!("0.0.0.0:{NERVIX_HTTPS_PORT}"),
        "--observability-listen-addr".to_string(),
        "0.0.0.0:9090".to_string(),
        "--web-console-listen-addr".to_string(),
        format!("0.0.0.0:{NERVIX_WEB_CONSOLE_PORT}"),
        "--interconnect-listen-addr".to_string(),
        format!("0.0.0.0:{NERVIX_INTERCONNECT_PORT}"),
        "--interconnect-advertise-addr".to_string(),
        format!("{container_name}:{NERVIX_INTERCONNECT_PORT}"),
        "--interconnect-tls-ca".to_string(),
        "/tmp/nervix-interconnect-ca.pem".to_string(),
        "--interconnect-tls-cert".to_string(),
        "/tmp/nervix-interconnect-node.pem".to_string(),
        "--interconnect-tls-key".to_string(),
        "/tmp/nervix-interconnect-node-key.pem".to_string(),
    ];
    match bootstrap_addr {
        Some(bootstrap_addr) => {
            arguments.push("--cluster-bootstrap-host".to_string());
            arguments.push(bootstrap_addr.to_string());
        }
        None => arguments.push("--allow-bootstrap".to_string()),
    }
    arguments
}

/// The statements of a rendered graph, in the order they are submitted.
fn graph_statements(graph: &str) -> error_stack::Result<Vec<&str>, BenchmarkCliError> {
    let statements = split_query_statements(graph).change_context(BenchmarkCliError::SplitGraph)?;
    if statements.is_empty() {
        return Err(Report::new(BenchmarkCliError::EmptyGraph));
    }
    Ok(statements)
}

fn cluster_status_is_ready(status: &str, expected: usize) -> bool {
    let mut in_membership = false;
    let mut voters = 0_usize;
    let mut last_log_index = None;
    let mut last_applied = None;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("raft.last_log_index: ") {
            last_log_index = value.parse::<u64>().ok();
        }
        if let Some(value) = line.strip_prefix("raft.last_applied: ") {
            last_applied = value.parse::<u64>().ok();
        }
        if line == "raft.membership:" {
            in_membership = true;
            continue;
        }
        if !in_membership {
            continue;
        }
        if line.starts_with("- ") && line.contains(" [voter] ") {
            voters = voters
                .checked_add(1)
                .assured("a benchmark cluster has at most three members");
        } else {
            in_membership = false;
        }
    }
    voters == expected && last_log_index.is_some() && last_log_index == last_applied
}

fn write_run_manifest(
    resolved: &ResolvedRun,
    description: &str,
    implementation: &Implementation,
    args: &RunArgs,
    repository_root: &Path,
) -> error_stack::Result<(), BenchmarkCliError> {
    let mut table = toml::Table::new();
    table.insert("benchmark".to_string(), resolved.slug.clone().into());
    table.insert(
        "implementation".to_string(),
        resolved.implementation.clone().into(),
    );
    table.insert("description".to_string(), description.to_string().into());
    let (subject, image) = match implementation {
        Implementation::Nervix(_) if args.options.nervix_mode == NervixMode::Local => {
            ("nervix-local", None)
        }
        Implementation::Nervix(_) => ("nervix-image", args.options.nervix_image.as_deref()),
        Implementation::Container(container) => ("container", Some(container.image.as_str())),
    };
    table.insert("subject".to_string(), subject.into());
    let subject_nodes = match implementation {
        Implementation::Nervix(nervix) => nervix.nodes,
        Implementation::Container(_) => 1,
    };
    table.insert("subject_nodes".to_string(), i64::from(subject_nodes).into());
    if let Some(image) = image {
        table.insert("image".to_string(), image.into());
    }
    table.insert(
        "duration_seconds".to_string(),
        manifest_integer("duration_seconds", resolved.duration_seconds)?.into(),
    );
    table.insert(
        "warmup_seconds".to_string(),
        manifest_integer("warmup_seconds", resolved.warmup_seconds)?.into(),
    );
    table.insert(
        "partitions".to_string(),
        i64::from(resolved.partitions).into(),
    );
    table.insert(
        "value_bytes".to_string(),
        manifest_integer("value_bytes", resolved.value_bytes)?.into(),
    );
    table.insert(
        "max_backlog_messages".to_string(),
        manifest_integer("max_backlog_messages", resolved.max_backlog_messages)?.into(),
    );
    table.insert(
        "wait_timeout_seconds".to_string(),
        manifest_integer("wait_timeout_seconds", resolved.wait_timeout.as_secs())?.into(),
    );
    table.insert(
        "input_topic".to_string(),
        resolved.input_topic.clone().into(),
    );
    table.insert(
        "output_topic".to_string(),
        resolved.output_topic.clone().into(),
    );
    table.insert(
        "consumer_group".to_string(),
        resolved.consumer_group.clone().into(),
    );
    if let Ok(output) = std::process::Command::new("git")
        .current_dir(repository_root)
        .args(["rev-parse", "HEAD"])
        .output()
        && output.status.success()
    {
        table.insert(
            "git_revision".to_string(),
            String::from_utf8_lossy(&output.stdout)
                .trim()
                .to_string()
                .into(),
        );
    }
    if let Ok(status) = std::process::Command::new("git")
        .current_dir(repository_root)
        .args(["status", "--short"])
        .output()
        && status.status.success()
    {
        table.insert("git_dirty".to_string(), (!status.stdout.is_empty()).into());
    }
    table.insert(
        "parameters".to_string(),
        toml::Value::Table(resolved.parameters.clone()),
    );
    let manifest =
        toml::to_string_pretty(&table).change_context(BenchmarkCliError::SerializeRunManifest)?;
    let manifest_path = resolved.run_directory.join("run.toml");
    fs::write(&manifest_path, manifest).change_context(BenchmarkCliError::WriteFile {
        path: manifest_path,
    })?;
    Ok(())
}

/// A run manifest integer, which TOML stores as an `i64`.
fn manifest_integer(
    field: &'static str,
    value: u64,
) -> error_stack::Result<i64, BenchmarkCliError> {
    i64::try_from(value).change_context(BenchmarkCliError::ManifestField { field })
}

async fn write_image_identity(
    run_directory: &Path,
    image: &str,
) -> error_stack::Result<(), BenchmarkCliError> {
    let output = Command::new("docker")
        .args(["image", "inspect", "--format={{.Id}}", image])
        .output()
        .await
        .change_context_lazy(|| BenchmarkCliError::InspectImage {
            image: image.to_string(),
        })?;
    if !output.status.success() {
        return Err(Report::new(BenchmarkCliError::ImageInspectionFailed {
            image: image.to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }));
    }
    let identity_path = run_directory.join("image.txt");
    fs::write(
        &identity_path,
        format!(
            "image={image}\nid={}\n",
            String::from_utf8_lossy(&output.stdout).trim()
        ),
    )
    .change_context(BenchmarkCliError::WriteFile {
        path: identity_path,
    })?;
    Ok(())
}

fn split_image_reference(
    image: &str,
) -> error_stack::Result<(String, String), ImageReferenceError> {
    if image.contains('@') {
        return Err(Report::new(ImageReferenceError::Digest));
    }
    let slash = image.rfind('/');
    let colon = image
        .rfind(':')
        .filter(|colon| slash.is_none_or(|slash| *colon > slash));
    let Some(colon) = colon else {
        return Err(Report::new(ImageReferenceError::MissingTag));
    };
    let (name, tag) = image.split_at(colon);
    let tag = &tag[1..];
    if name.is_empty() || tag.is_empty() {
        return Err(Report::new(ImageReferenceError::Incomplete));
    }
    Ok((name.to_string(), tag.to_string()))
}

fn absolute_or_repository_path(repository_root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        repository_root.join(path)
    }
}

/// A path passed to a child process as a UTF-8 argument.
fn utf8_path(path: &Path) -> error_stack::Result<&str, BenchmarkCliError> {
    path.to_str().ok_or_else(|| {
        Report::new(BenchmarkCliError::NonUtf8Path {
            path: path.to_path_buf(),
        })
    })
}

/// The load driver's subcommand and arguments for the workload's declared shape.
fn shape_arguments(shape: &LoadShape) -> Vec<String> {
    match shape {
        LoadShape::UniformPassthrough => vec!["uniform-passthrough".to_string()],
        LoadShape::UniformUppercase => vec!["uniform-uppercase".to_string()],
        LoadShape::UniformFilterMap => vec!["uniform-filter-map".to_string()],
        LoadShape::UniformFanout { outputs_per_input } => vec![
            "uniform-fanout".to_string(),
            "--outputs-per-input".to_string(),
            outputs_per_input.to_string(),
        ],
        LoadShape::KeyedWindowed {
            keys_per_cycle,
            retained_keys,
            copies_per_key,
            count_field,
        } => vec![
            "keyed-windowed".to_string(),
            "--keys-per-cycle".to_string(),
            keys_per_cycle.to_string(),
            "--retained-keys".to_string(),
            retained_keys.to_string(),
            "--copies-per-key".to_string(),
            copies_per_key.to_string(),
            "--count-field".to_string(),
            count_field.clone(),
        ],
    }
}

fn sibling_binary(name: &str) -> error_stack::Result<PathBuf, BenchmarkCliError> {
    let executable = std::env::current_exe().change_context(BenchmarkCliError::LocateExecutable)?;
    let Some(directory) = executable.parent() else {
        return Err(Report::new(BenchmarkCliError::ExecutableWithoutDirectory {
            path: executable,
        }));
    };
    Ok(directory.join(name))
}

fn reserve_available_ports(count: usize) -> io::Result<Vec<u16>> {
    let mut listeners = Vec::with_capacity(count);
    for _ in 0..count {
        listeners.push(TcpListener::bind(SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            0,
        ))?);
    }
    listeners
        .iter()
        .map(|listener| listener.local_addr().map(|address| address.port()))
        .collect()
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
