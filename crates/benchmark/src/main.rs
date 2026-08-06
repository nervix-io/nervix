use std::{
    fs, io,
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail, ensure};
use clap::{Parser, Subcommand, ValueEnum};
use nervix_benchmark::{
    BenchmarkCatalog, BenchmarkComparison, BenchmarkDependency, ContainerImplementation,
    Implementation, KafkaRenderInputs, LoadedBenchmark, RunSettings, provision_topics,
};
use nervix_client_core::{Client, ConnectOptions};
use nervix_test_environment::{
    ContainerMode, ContainerReadiness, DependencyEnvironment, KAFKA_ADDR, KAFKA_DOCKER_ADDR,
    KAFKA_DOCKER_NETWORK, ManagedContainerInfo, configure_process_lifecycle,
};
use testcontainers::{
    CopyTargetOptions, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor, wait::HttpWaitStrategy},
};
use tokio::process::{Child, Command};
use uuid::Uuid;

const DEFAULT_BENCHMARKS_ROOT: &str = "benches/benchmarks";
const DEFAULT_ARTIFACTS_ROOT: &str = "target/benchmarks";
const DEFAULT_SERVER_BINARY: &str = "target/release/nervix-server";
const DEFAULT_DOMAIN: &str = "default";
const DEFAULT_USERNAME: &str = "default";
const NERVIX_GRPC_PORT: ContainerPort = ContainerPort::Tcp(47391);
const NERVIX_OBSERVABILITY_PORT: ContainerPort = ContainerPort::Tcp(9090);
const NERVIX_CLUSTER_API_PORT: u16 = 47397;
const NERVIX_HTTP_PORT: u16 = 8080;
const NERVIX_HTTPS_PORT: u16 = 8443;
const NERVIX_WEB_CONSOLE_PORT: u16 = 47420;

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
    #[arg(long)]
    load_driver: Option<PathBuf>,
    #[arg(long)]
    artifacts_root: Option<PathBuf>,
    #[arg(long)]
    duration_seconds: Option<u64>,
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
    parameters: toml::Table,
    input_topic: String,
    output_topic: String,
    consumer_group: String,
    domain: String,
    run_token: String,
    run_directory: PathBuf,
}

enum SubjectRuntime {
    Local { child: Child },
    Container { info: ManagedContainerInfo },
}

struct Subject {
    runtime: SubjectRuntime,
    control_url: Option<String>,
    password: Option<String>,
}

fn main() -> Result<()> {
    configure_process_lifecycle(ContainerMode::Ephemeral);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build the benchmark runtime")?;
    runtime.block_on(run())
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let repository_root = args.repository_root.canonicalize().with_context(|| {
        format!(
            "failed to resolve repository root {}",
            args.repository_root.display()
        )
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
    }
}

fn list_benchmarks(catalog: &BenchmarkCatalog) -> Result<()> {
    for benchmark in catalog.discover()? {
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
) -> Result<()> {
    let benchmarks = catalog.discover()?;
    ensure!(!benchmarks.is_empty(), "benchmark catalog is empty");
    let artifacts_root = options
        .artifacts_root
        .as_deref()
        .map(|path| absolute_or_repository_path(repository_root, path))
        .unwrap_or_else(|| repository_root.join(DEFAULT_ARTIFACTS_ROOT));
    let mut run_directories = Vec::new();
    for benchmark in benchmarks {
        for implementation in benchmark.definition().implementations.keys() {
            tokio::task::consume_budget().await;
            let run_directory = run_benchmark(
                repository_root,
                catalog,
                RunArgs {
                    benchmark: benchmark.slug().to_string(),
                    implementation: implementation.clone(),
                    options: options.clone(),
                },
            )
            .await?;
            run_directories.push(run_directory);
        }
    }
    let comparison = BenchmarkComparison::from_run_directories(&run_directories)?;
    let comparison_path = artifacts_root.join("benchmark-comparison.md");
    comparison.write_markdown(&comparison_path)?;
    println!("comparison={}", comparison_path.display());
    Ok(())
}

async fn run_benchmark(
    repository_root: &Path,
    catalog: &BenchmarkCatalog,
    args: RunArgs,
) -> Result<PathBuf> {
    let benchmark = catalog.load(&args.benchmark)?;
    let implementation = benchmark
        .definition()
        .implementations
        .get(&args.implementation)
        .ok_or_else(|| {
            anyhow!(
                "benchmark '{}' has no implementation named '{}'",
                benchmark.slug(),
                args.implementation
            )
        })?;
    let resolved = ResolvedRun::new(repository_root, &benchmark, &args)?;
    fs::create_dir_all(&resolved.run_directory).with_context(|| {
        format!(
            "failed to create benchmark artifact directory {}",
            resolved.run_directory.display()
        )
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
    let teardown_errors = environment.shutdown().await;
    let outcome = if !teardown_errors.is_empty() {
        let teardown = teardown_errors.join("; ");
        match execution {
            Ok(()) => Err(anyhow!("benchmark dependency teardown failed: {teardown}")),
            Err(error) => Err(error.context(format!(
                "benchmark dependency teardown also failed: {teardown}"
            ))),
        }
    } else {
        execution
    };
    let status = if outcome.is_ok() { "pass\n" } else { "fail\n" };
    fs::write(resolved.run_directory.join("status.txt"), status)?;
    outcome?;
    Ok(resolved.run_directory)
}

async fn execute_run(
    repository_root: &Path,
    benchmark: &LoadedBenchmark,
    implementation: &Implementation,
    args: &RunArgs,
    resolved: &ResolvedRun,
    environment: &mut DependencyEnvironment,
) -> Result<()> {
    start_declared_dependencies(environment, &benchmark.definition().dependencies).await?;
    let mut dependency_endpoints = std::collections::BTreeMap::new();
    environment
        .endpoints()
        .apply_placeholders(&mut dependency_endpoints);
    let host_bootstrap = environment.endpoints().get(KAFKA_ADDR)?.to_string();
    let docker_bootstrap = environment.endpoints().get(KAFKA_DOCKER_ADDR)?.to_string();
    let docker_network = environment
        .endpoints()
        .get(KAFKA_DOCKER_NETWORK)?
        .to_string();
    provision_topics(
        &host_bootstrap,
        &resolved.input_topic,
        &resolved.output_topic,
        resolved.partitions,
        resolved.wait_timeout,
    )
    .await
    .context("failed to provision benchmark Kafka topics")?;

    let subject_bootstrap = match implementation {
        Implementation::Nervix(_) if args.options.nervix_mode == NervixMode::Local => {
            &host_bootstrap
        }
        Implementation::Nervix(_) | Implementation::Container(_) => &docker_bootstrap,
    };
    let rendered = benchmark.render_implementation_with_parameters(
        &resolved.implementation,
        KafkaRenderInputs {
            kafka_bootstrap_servers: subject_bootstrap,
            input_topic: &resolved.input_topic,
            output_topic: &resolved.output_topic,
            consumer_group: &resolved.consumer_group,
            lane_count: resolved.partitions,
            dependency_endpoints: &dependency_endpoints,
        },
        &resolved.parameters,
    )?;
    let rendered_path = match implementation {
        Implementation::Nervix(_) => resolved.run_directory.join("graph.nspl"),
        Implementation::Container(container) => resolved.run_directory.join(
            container
                .config_path
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("subject.conf")),
        ),
    };
    fs::write(&rendered_path, &rendered).with_context(|| {
        format!(
            "failed to write rendered configuration {}",
            rendered_path.display()
        )
    })?;

    let mut subject = match implementation {
        Implementation::Nervix(_) => {
            start_nervix(
                repository_root,
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
                .configure_nervix(&resolved.domain, &rendered, resolved.wait_timeout, resolved)
                .await?;
        }
        run_load_driver(
            repository_root,
            args,
            resolved,
            &host_bootstrap,
            &mut subject,
        )
        .await
    }
    .await;
    let log_result = subject.capture_logs(&resolved.run_directory).await;
    let stop_result = subject.stop().await;

    benchmark_result?;
    log_result?;
    stop_result?;
    let report = fs::read_to_string(resolved.run_directory.join("load-report.txt"))
        .context("failed to read the completed load report")?;
    println!("{report}");
    println!("artifacts={}", resolved.run_directory.display());
    Ok(())
}

async fn start_declared_dependencies(
    environment: &mut DependencyEnvironment,
    dependencies: &[BenchmarkDependency],
) -> Result<()> {
    for dependency in dependencies {
        tokio::task::consume_budget().await;
        let result = match dependency {
            BenchmarkDependency::Kafka => environment.start_kafka().await,
        };
        result.with_context(|| format!("failed to start benchmark dependency {dependency:?}"))?;
    }
    Ok(())
}

impl ResolvedRun {
    fn new(repository_root: &Path, benchmark: &LoadedBenchmark, args: &RunArgs) -> Result<Self> {
        let settings = RunSettings::resolve(
            benchmark.definition(),
            &args.options.parameter_overrides,
            args.options.duration_seconds,
        )?;
        let partitions = args
            .options
            .partitions
            .unwrap_or(benchmark.definition().load.partitions);
        let value_bytes = args
            .options
            .value_bytes
            .unwrap_or(benchmark.definition().load.value_bytes);
        let max_backlog_messages = args
            .options
            .max_backlog_messages
            .unwrap_or(benchmark.definition().load.max_backlog_messages);
        let wait_timeout_seconds = args
            .options
            .wait_timeout_seconds
            .unwrap_or(benchmark.definition().load.wait_timeout_seconds);
        ensure!(partitions > 0, "partition count must be positive");
        ensure!(
            partitions <= i32::MAX as u32,
            "partition count exceeds Kafka's supported range"
        );
        ensure!(value_bytes > 0, "value byte count must be positive");
        ensure!(
            value_bytes <= 1024 * 1024,
            "value byte count must not exceed 1 MiB"
        );
        ensure!(max_backlog_messages > 0, "maximum backlog must be positive");
        ensure!(wait_timeout_seconds > 0, "wait timeout must be positive");

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
            .artifacts_root
            .as_deref()
            .map(|path| absolute_or_repository_path(repository_root, path))
            .unwrap_or_else(|| repository_root.join(DEFAULT_ARTIFACTS_ROOT));
        let run_directory = artifacts_root
            .join(benchmark.slug())
            .join(&args.implementation)
            .join(&run_token);
        Ok(Self {
            slug: benchmark.slug().to_string(),
            implementation: args.implementation.clone(),
            partitions,
            value_bytes,
            max_backlog_messages,
            wait_timeout: Duration::from_secs(wait_timeout_seconds),
            duration_seconds: settings.duration_seconds,
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
    args: &RunArgs,
    resolved: &ResolvedRun,
    environment: &mut DependencyEnvironment,
    docker_network: &str,
) -> Result<Subject> {
    let password = format!("benchmark-{}", resolved.run_token);
    match args.options.nervix_mode {
        NervixMode::Local => {
            let server_binary = absolute_or_repository_path(
                repository_root,
                args.options
                    .server_binary
                    .as_deref()
                    .unwrap_or(Path::new(DEFAULT_SERVER_BINARY)),
            );
            ensure!(
                server_binary.is_file(),
                "Nervix server binary does not exist at {}",
                server_binary.display()
            );
            let ports = LocalPorts::reserve()?;
            let state_directory = resolved.run_directory.join("nervix-state");
            fs::create_dir_all(&state_directory)?;
            let log_path = resolved.run_directory.join("subject.log");
            let log = fs::File::create(&log_path)?;
            let stderr = log.try_clone()?;
            let mut command = Command::new(server_binary);
            command
                .current_dir(repository_root)
                .env("NERVIX_INIT_DEFAULT_USER_PASSWORD", &password)
                .env("NERVIX_DB_PATH", state_directory.join("db"))
                .env("RUST_LOG", "info")
                .args(ports.server_arguments(&resolved.run_token))
                .stdout(Stdio::from(log))
                .stderr(Stdio::from(stderr))
                .kill_on_drop(true);
            let mut child = command
                .spawn()
                .context("failed to start local nervix-server")?;
            wait_for_local_nervix(&mut child, ports.observability, resolved.wait_timeout).await?;
            Ok(Subject {
                runtime: SubjectRuntime::Local { child },
                control_url: Some(format!("http://127.0.0.1:{}", ports.grpc)),
                password: Some(password),
            })
        }
        NervixMode::Image => {
            let image =
                args.options.nervix_image.as_deref().ok_or_else(|| {
                    anyhow!("--nervix-image is required with --nervix-mode image")
                })?;
            let (image_name, image_tag) = split_image_reference(image)?;
            let server_args = vec![
                "/usr/local/bin/nervix-server".to_string(),
                "--node-id".to_string(),
                format!("bench-node-{}", resolved.run_token),
                "--cluster-id".to_string(),
                format!("bench-cluster-{}", resolved.run_token),
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
                "--cluster-api-listen-addr".to_string(),
                format!("0.0.0.0:{NERVIX_CLUSTER_API_PORT}"),
                "--cluster-api-advertise-addr".to_string(),
                format!("127.0.0.1:{NERVIX_CLUSTER_API_PORT}"),
                "--allow-bootstrap".to_string(),
            ];
            let timeout = resolved.wait_timeout;
            let network = docker_network.to_string();
            let password_for_container = password.clone();
            let info = environment
                .start_generic(
                    "benchmark-subject",
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
                .context("failed to start Nervix benchmark image")?;
            write_image_identity(&resolved.run_directory, image).await?;
            let grpc_port = info
                .host_port(NERVIX_GRPC_PORT)
                .ok_or_else(|| anyhow!("Nervix image did not expose its gRPC port"))?;
            Ok(Subject {
                runtime: SubjectRuntime::Container { info },
                control_url: Some(format!("http://127.0.0.1:{grpc_port}")),
                password: Some(password),
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
) -> Result<Subject> {
    let (image_name, image_tag) = split_image_reference(&implementation.image)?;
    let config_path = implementation
        .config_path
        .to_str()
        .ok_or_else(|| anyhow!("container configuration path is not valid UTF-8"))?
        .to_string();
    let readiness_port = implementation.readiness_port.map(ContainerPort::Tcp);
    let mapped_ports = readiness_port.into_iter().collect::<Vec<_>>();
    let readiness_path = implementation.readiness_path.clone();
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
        .with_context(|| {
            format!(
                "failed to start benchmark container image {}",
                implementation.image
            )
        })?;
    write_image_identity(&resolved.run_directory, &implementation.image).await?;
    Ok(Subject {
        runtime: SubjectRuntime::Container { info },
        control_url: None,
        password: None,
    })
}

impl Subject {
    async fn configure_nervix(
        &mut self,
        domain: &str,
        graph: &str,
        timeout: Duration,
        resolved: &ResolvedRun,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let client = loop {
            tokio::task::consume_budget().await;
            let connection = async {
                let client = self.connect_client(DEFAULT_DOMAIN).await?;
                let outcome = client
                    .execute("SHOW CLUSTER STATUS;")
                    .await
                    .context("failed to query Nervix cluster status")?;
                Ok::<_, anyhow::Error>((client, outcome))
            }
            .await;
            match connection {
                Ok((client, outcome)) if outcome.success => {
                    fs::write(
                        resolved.run_directory.join("cluster-status.txt"),
                        format!("{outcome:#?}\n"),
                    )?;
                    break client;
                }
                Ok(_) | Err(_) if tokio::time::Instant::now() < deadline => {
                    self.ensure_running()?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok((_, outcome)) => bail!(
                    "Nervix control plane did not become ready: {}\ndiagnostics: {:?}",
                    outcome.message,
                    outcome.diagnostics
                ),
                Err(error) => {
                    return Err(error.context("Nervix control plane did not become ready"));
                }
            }
        };
        self.execute_checked(
            &client,
            &format!("CREATE UNPACED DOMAIN {domain};"),
            &resolved.run_directory.join("create-domain.txt"),
        )
        .await?;
        client.set_domain(domain).await;
        self.execute_checked(
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
        .await
    }

    async fn connect_client(&self, domain: &str) -> Result<Client> {
        let control_url = self
            .control_url
            .as_deref()
            .ok_or_else(|| anyhow!("benchmark subject has no Nervix control endpoint"))?;
        let password = self
            .password
            .as_deref()
            .ok_or_else(|| anyhow!("benchmark subject has no Nervix password"))?;
        Client::connect_with_options(
            control_url,
            domain.to_string(),
            ConnectOptions::default().with_basic_auth(DEFAULT_USERNAME, password),
        )
        .await
        .with_context(|| format!("failed to connect to Nervix at {control_url}"))
    }

    async fn execute_checked(
        &self,
        client: &Client,
        query: &str,
        output_path: &Path,
    ) -> Result<()> {
        let outcome = client
            .execute(query)
            .await
            .context("failed to execute a Nervix benchmark command")?;
        fs::write(output_path, format!("{outcome:#?}\n"))?;
        ensure!(
            outcome.success,
            "Nervix benchmark command failed: {}\ndiagnostics: {:?}",
            outcome.message,
            outcome.diagnostics
        );
        Ok(())
    }

    fn ensure_running(&mut self) -> Result<()> {
        if let SubjectRuntime::Local { child } = &mut self.runtime
            && let Some(status) = child.try_wait()?
        {
            bail!("local nervix-server exited unexpectedly with {status}");
        }
        Ok(())
    }

    async fn capture_logs(&self, run_directory: &Path) -> Result<()> {
        let SubjectRuntime::Container { info } = &self.runtime else {
            return Ok(());
        };
        let output = Command::new("docker")
            .args(["logs", info.id()])
            .output()
            .await
            .context("failed to collect benchmark container logs")?;
        let mut logs = output.stdout;
        logs.extend_from_slice(&output.stderr);
        fs::write(run_directory.join("subject.log"), logs)?;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let SubjectRuntime::Local { child } = &mut self.runtime
            && child.try_wait()?.is_none()
        {
            child.start_kill()?;
            child.wait().await?;
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
) -> Result<()> {
    let load_driver = match &args.options.load_driver {
        Some(path) => absolute_or_repository_path(repository_root, path),
        None => sibling_binary("nervix-benchmark-load")?,
    };
    ensure!(
        load_driver.is_file(),
        "benchmark load driver does not exist at {}",
        load_driver.display()
    );
    let ready_file = resolved.run_directory.join("load-ready");
    let go_file = resolved.run_directory.join("load-go");
    let stdout_path = resolved.run_directory.join("load-report.txt");
    let stderr_path = resolved.run_directory.join("load-driver.log");
    let stdout = fs::File::create(&stdout_path)?;
    let stderr = fs::File::create(&stderr_path)?;
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
            &resolved.partitions.to_string(),
            "--duration-seconds",
            &resolved.duration_seconds.to_string(),
            "--value-bytes",
            &resolved.value_bytes.to_string(),
            "--max-backlog-messages",
            &resolved.max_backlog_messages.to_string(),
            "--wait-timeout-seconds",
            &resolved.wait_timeout.as_secs().to_string(),
            "--ready-file",
            ready_file
                .to_str()
                .ok_or_else(|| anyhow!("ready path is not UTF-8"))?,
            "--go-file",
            go_file
                .to_str()
                .ok_or_else(|| anyhow!("go path is not UTF-8"))?,
        ])
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start load driver {}", load_driver.display()))?;

    let ready_deadline = tokio::time::Instant::now() + resolved.wait_timeout;
    loop {
        tokio::task::consume_budget().await;
        if ready_file.exists() {
            break;
        }
        if let Some(status) = child.try_wait()? {
            let diagnostics = fs::read_to_string(&stderr_path).unwrap_or_default();
            bail!("load driver exited before warmup with {status}:\n{diagnostics}");
        }
        subject.ensure_running()?;
        ensure!(
            tokio::time::Instant::now() < ready_deadline,
            "load driver did not complete consumer stabilization and warmup before timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    fs::write(&go_file, b"go\n")?;

    let completion_timeout = Duration::from_secs(
        resolved
            .duration_seconds
            .saturating_add(resolved.wait_timeout.as_secs().saturating_mul(4))
            .saturating_add(30),
    );
    let completion_deadline = tokio::time::Instant::now() + completion_timeout;
    let status = loop {
        tokio::task::consume_budget().await;
        if let Some(status) = child.try_wait()? {
            break status;
        }
        subject.ensure_running()?;
        ensure!(
            tokio::time::Instant::now() < completion_deadline,
            "load driver exceeded its bounded completion timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    if !status.success() {
        let diagnostics = fs::read_to_string(&stderr_path).unwrap_or_default();
        bail!("load driver failed with {status}:\n{diagnostics}");
    }
    Ok(())
}

struct LocalPorts {
    grpc: u16,
    http: u16,
    https: u16,
    observability: u16,
    web_console: u16,
    cluster_api: u16,
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
            cluster_api: ports[5],
        })
    }

    fn server_arguments(&self, token: &str) -> Vec<String> {
        vec![
            "--node-id".to_string(),
            format!("bench-node-{token}"),
            "--cluster-id".to_string(),
            format!("bench-cluster-{token}"),
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
            "--cluster-api-listen-addr".to_string(),
            format!("127.0.0.1:{}", self.cluster_api),
            "--cluster-api-advertise-addr".to_string(),
            format!("127.0.0.1:{}", self.cluster_api),
            "--allow-bootstrap".to_string(),
        ]
    }
}

async fn wait_for_local_nervix(
    child: &mut Child,
    observability_port: u16,
    timeout: Duration,
) -> Result<()> {
    let url = format!("http://127.0.0.1:{observability_port}/readyz");
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        tokio::task::consume_budget().await;
        if let Some(status) = child.try_wait()? {
            bail!("local nervix-server exited before readiness with {status}");
        }
        if let Ok(response) = client.get(&url).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "local nervix-server did not become ready at {url} before timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn write_run_manifest(
    resolved: &ResolvedRun,
    description: &str,
    implementation: &Implementation,
    args: &RunArgs,
    repository_root: &Path,
) -> Result<()> {
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
    if let Some(image) = image {
        table.insert("image".to_string(), image.into());
    }
    table.insert(
        "duration_seconds".to_string(),
        i64::try_from(resolved.duration_seconds)?.into(),
    );
    table.insert(
        "partitions".to_string(),
        i64::from(resolved.partitions).into(),
    );
    table.insert(
        "value_bytes".to_string(),
        i64::try_from(resolved.value_bytes)?.into(),
    );
    table.insert(
        "max_backlog_messages".to_string(),
        i64::try_from(resolved.max_backlog_messages)?.into(),
    );
    table.insert(
        "wait_timeout_seconds".to_string(),
        i64::try_from(resolved.wait_timeout.as_secs())?.into(),
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
    fs::write(
        resolved.run_directory.join("run.toml"),
        toml::to_string_pretty(&table)?,
    )?;
    Ok(())
}

async fn write_image_identity(run_directory: &Path, image: &str) -> Result<()> {
    let output = Command::new("docker")
        .args(["image", "inspect", "--format={{.Id}}", image])
        .output()
        .await
        .with_context(|| format!("failed to inspect image {image}"))?;
    ensure!(
        output.status.success(),
        "failed to inspect image {image}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::write(
        run_directory.join("image.txt"),
        format!(
            "image={image}\nid={}\n",
            String::from_utf8_lossy(&output.stdout).trim()
        ),
    )?;
    Ok(())
}

fn split_image_reference(image: &str) -> Result<(String, String)> {
    ensure!(
        !image.contains('@'),
        "digest image references are not yet supported"
    );
    let slash = image.rfind('/');
    let colon = image
        .rfind(':')
        .filter(|colon| slash.is_none_or(|slash| *colon > slash));
    let colon =
        colon.ok_or_else(|| anyhow!("image reference '{image}' must include an explicit tag"))?;
    let (name, tag) = image.split_at(colon);
    let tag = &tag[1..];
    ensure!(
        !name.is_empty() && !tag.is_empty(),
        "invalid image reference '{image}'"
    );
    Ok((name.to_string(), tag.to_string()))
}

fn absolute_or_repository_path(repository_root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        repository_root.join(path)
    }
}

fn sibling_binary(name: &str) -> Result<PathBuf> {
    let executable = std::env::current_exe().context("failed to resolve benchmark executable")?;
    let directory = executable
        .parent()
        .ok_or_else(|| anyhow!("benchmark executable has no parent directory"))?;
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
