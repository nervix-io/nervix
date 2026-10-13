use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};

use nervix_benchmark::{BenchmarkError, SettingsError};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

/// A benchmark with a local Nervix and a container implementation and no flush parameters, so its
/// automatic duration is the default minimum.
const MANIFEST: &str = r#"
name = "Kafka forwarding"
description = "Forwards records through one implementation."
dependencies = ["kafka"]

[load]
duration = "auto"
partitions = 3
value_bytes = 128
max_backlog_messages = 4096
wait_timeout_seconds = 30
warmup_seconds = 1

[load.shape]
kind = "uniform-passthrough"

[parameters]
batch_bytes = 1048576

[implementations.nervix]
kind = "nervix"
template = "graph.nspl.upon"

[implementations.vector]
kind = "container"
require_consumer_group_membership = false
image = "timberio/vector:0.57.0-debian"
template = "vector.yaml.upon"
config_path = "/etc/vector/vector.yaml"
"#;

/// One Prometheus scrape holding a complete metrics report for domain `benchmark_run`.
const SCRAPE: &str = r#"nervix_messages_total{direction="sent",domain="benchmark_run",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6144
nervix_batches_total{direction="sent",domain="benchmark_run",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6
nervix_messages_per_batch_bucket{direction="sent",domain="benchmark_run",le="1024",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6
nervix_messages_per_batch_bucket{direction="sent",domain="benchmark_run",le="+Inf",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6
nervix_messages_per_batch_count{direction="sent",domain="benchmark_run",peer="ingested",peer_kind="RELAY",physical_node_id="node-1",relay="ingested",target="kafka_in",target_kind="INGESTOR"} 6
nervix_relay_buffer_len_bucket{direction="concrete",domain="benchmark_run",le="1",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY"} 4
nervix_relay_buffer_len_bucket{direction="concrete",domain="benchmark_run",le="+Inf",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY"} 4
nervix_relay_buffer_len_count{direction="concrete",domain="benchmark_run",peer="",peer_kind="",physical_node_id="node-1",relay="ingested",target="ingested",target_kind="RELAY"} 4
"#;

/// A repository whose catalog holds the benchmarks `slugs`.
fn repository_with(slugs: &[&str]) -> tempfile::TempDir {
    let repository = tempfile::tempdir().expect("a temporary repository should be created");
    let catalog = repository.path().join(DEFAULT_BENCHMARKS_ROOT);
    fs::create_dir_all(&catalog).expect("the catalog should be created");
    for slug in slugs {
        let directory = catalog.join(slug);
        fs::create_dir_all(&directory).expect("the benchmark directory should be created");
        fs::write(directory.join("benchmark.toml"), MANIFEST)
            .expect("the manifest should be written");
        fs::write(
            directory.join("graph.nspl.upon"),
            "CREATE UNPACED DOMAIN {{ input_topic }};\n",
        )
        .expect("the Nervix template should be written");
        fs::write(
            directory.join("vector.yaml.upon"),
            "brokers: {{ kafka_bootstrap_servers }}\n",
        )
        .expect("the container template should be written");
    }
    repository
}

/// The parsed command line `nervix-benchmark --repository-root <repository> <arguments>`.
fn command(repository: &Path, arguments: &[&str]) -> Args {
    let mut command_line = vec![
        "nervix-benchmark".to_string(),
        "--repository-root".to_string(),
        repository.display().to_string(),
    ];
    for argument in arguments {
        command_line.push((*argument).to_string());
    }
    Args::try_parse_from(command_line).expect("the test command line should parse")
}

/// A local Nervix run of `kafka-forward` below the relative artifacts root `runs`, with 256-byte
/// values.
fn run_args() -> RunArgs {
    RunArgs {
        benchmark: "kafka-forward".to_string(),
        implementation: "nervix".to_string(),
        options: RunOptions {
            nervix_mode: NervixMode::Local,
            nervix_image: None,
            server_binary: None,
            workload: WorkloadOptions {
                load_driver: None,
                artifacts_root: Some(PathBuf::from("runs")),
                duration_seconds: None,
                warmup_seconds: None,
                partitions: None,
                value_bytes: Some(256),
                max_backlog_messages: None,
                wait_timeout_seconds: None,
                parameter_overrides: Vec::new(),
            },
        },
    }
}

fn valid_bounds() -> WorkloadBounds {
    WorkloadBounds {
        partitions: 3,
        value_bytes: 128,
        max_backlog_messages: 4096,
        wait_timeout_seconds: 30,
        warmup_seconds: 1,
    }
}

fn subject(runtime: SubjectRuntime) -> Subject {
    Subject {
        runtime,
        control_url: None,
        metrics_urls: Vec::new(),
        password: None,
        node_count: 1,
    }
}

#[test]
fn splits_a_tagged_image_reference() {
    let (name, tag) = split_image_reference("ghcr.io/nervix-io/nervix:pr-109")
        .assured("the reference names an image and an explicit tag");

    assert_eq!(name, "ghcr.io/nervix-io/nervix");
    assert_eq!(tag, "pr-109");
}

#[test]
fn refuses_image_references_without_an_explicit_tag() {
    for (image, expected) in [
        ("nervix@sha256:0123", ImageReferenceError::Digest),
        ("localhost:5000/nervix", ImageReferenceError::MissingTag),
        ("nervix:", ImageReferenceError::Incomplete),
    ] {
        let report = split_image_reference(image).expect_err("the image reference is refused");
        assert_eq!(report.current_context(), &expected, "{image}");
    }
}

#[test]
fn names_the_benchmark_and_the_violated_bound_of_an_invalid_workload() {
    let bounds = WorkloadBounds {
        value_bytes: MAX_VALUE_BYTES + 1,
        ..valid_bounds()
    };

    let report = bounds
        .validate()
        .change_context(BenchmarkCliError::Workload {
            benchmark: "kafka-filter-map".to_string(),
        })
        .expect_err("a value larger than 1 MiB must be refused");

    assert_eq!(
        format!("{report:#}"),
        "invalid workload for benchmark 'kafka-filter-map': value byte count 1048577 must not \
         exceed 1 MiB"
    );
}

#[test]
fn refuses_each_workload_bound_and_accepts_the_largest_value_payload() {
    let cases = [
        (
            WorkloadBounds {
                partitions: 0,
                ..valid_bounds()
            },
            "partition count must be positive",
        ),
        (
            WorkloadBounds {
                partitions: u32::MAX,
                ..valid_bounds()
            },
            "partition count 4294967295 exceeds Kafka's supported range",
        ),
        (
            WorkloadBounds {
                value_bytes: 0,
                ..valid_bounds()
            },
            "value byte count must be positive",
        ),
        (
            WorkloadBounds {
                max_backlog_messages: 0,
                ..valid_bounds()
            },
            "maximum backlog must be positive",
        ),
        (
            WorkloadBounds {
                wait_timeout_seconds: 0,
                ..valid_bounds()
            },
            "wait timeout must be positive",
        ),
        (
            WorkloadBounds {
                warmup_seconds: 0,
                ..valid_bounds()
            },
            "warm-up duration must be positive",
        ),
    ];

    for (bounds, expected) in cases {
        let report = bounds.validate().expect_err(expected);
        assert_eq!(report.to_string(), expected);
    }
    WorkloadBounds {
        value_bytes: MAX_VALUE_BYTES,
        ..valid_bounds()
    }
    .validate()
    .expect("1 MiB is the largest value payload a run may generate");
}

#[nervix_primitives::test]
async fn lists_the_catalog_of_a_repository() {
    let repository = repository_with(&["kafka-forward", "kafka-filter"]);

    run_command(command(repository.path(), &["list"]))
        .await
        .expect("the catalog should be listed");
}

#[nervix_primitives::test]
async fn names_a_repository_root_that_does_not_exist() {
    let repository = tempfile::tempdir().expect("a temporary directory should be created");
    let missing = repository.path().join("missing");

    let report = run_command(command(&missing, &["list"]))
        .await
        .expect_err("a missing repository root cannot be resolved");

    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::ResolveRepositoryRoot { path } if *path == missing
    ));
}

#[nervix_primitives::test]
async fn keeps_the_catalog_failure_beneath_the_command() {
    let repository = tempfile::tempdir().expect("a temporary repository should be created");

    let report = run_command(command(repository.path(), &["list"]))
        .await
        .expect_err("a repository without a catalog cannot be listed");

    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::Catalog
    ));
    assert!(matches!(
        report.downcast_ref::<BenchmarkError>(),
        Some(BenchmarkError::OpenCatalog { .. })
    ));
}

#[nervix_primitives::test]
async fn refuses_a_run_before_starting_anything_when_its_request_is_invalid() {
    let repository = repository_with(&["kafka-forward"]);

    let report = run_command(command(repository.path(), &["run", "absent-benchmark"]))
        .await
        .expect_err("a benchmark the catalog does not hold cannot run");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::LoadBenchmark { benchmark } if benchmark == "absent-benchmark"
    ));
    assert!(matches!(
        report.downcast_ref::<BenchmarkError>(),
        Some(BenchmarkError::OpenBenchmark { .. })
    ));

    let report = run_command(command(
        repository.path(),
        &["run", "kafka-forward", "--implementation", "spark"],
    ))
    .await
    .expect_err("an implementation the benchmark does not declare cannot run");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::UnknownImplementation { benchmark, implementation }
            if benchmark == "kafka-forward" && implementation == "spark"
    ));

    let report = run_command(command(
        repository.path(),
        &["run", "kafka-forward", "--parameter", "window_seconds=2"],
    ))
    .await
    .expect_err("a parameter the benchmark does not declare cannot be overridden");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::RunSettings { benchmark } if benchmark == "kafka-forward"
    ));
    assert!(matches!(
        report.downcast_ref::<SettingsError>(),
        Some(SettingsError::UnknownParameter { name }) if name == "window_seconds"
    ));

    let report = run_command(command(
        repository.path(),
        &["run", "kafka-forward", "--partitions", "0"],
    ))
    .await
    .expect_err("a run without partitions cannot start");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::Workload { benchmark } if benchmark == "kafka-forward"
    ));
    assert!(matches!(
        report.downcast_ref::<WorkloadError>(),
        Some(WorkloadError::ZeroPartitions)
    ));
}

#[nervix_primitives::test]
async fn reports_every_failed_catalog_execution_and_fails_the_suite() {
    let repository = repository_with(&["kafka-forward"]);
    let artifacts = repository.path().join("artifacts");
    let artifacts_root = artifacts.display().to_string();

    let report = run_command(command(
        repository.path(),
        &[
            "run-all",
            "--partitions",
            "0",
            "--artifacts-root",
            &artifacts_root,
        ],
    ))
    .await
    .expect_err("a suite whose every execution fails must fail");

    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::FailedImplementations {
            failed: 2,
            total: 2
        }
    ));
    let comparison = fs::read_to_string(artifacts.join("benchmark-comparison.md"))
        .expect("the suite report should be written");
    assert!(comparison.contains("| Kafka Forward | Nervix | ❌ Failed |"));
    assert!(comparison.contains("| Kafka Forward | Vector | ❌ Failed |"));
    assert!(comparison.contains(
        "| Kafka Forward | Vector | invalid workload for benchmark 'kafka-forward': partition \
         count must be positive |"
    ));
}

#[nervix_primitives::test]
async fn refuses_to_run_an_empty_catalog() {
    let repository = repository_with(&[]);

    let report = run_command(command(repository.path(), &["run-all"]))
        .await
        .expect_err("an empty catalog has nothing to run");

    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::EmptyCatalog
    ));
}

#[nervix_primitives::test]
async fn names_the_arm_whose_server_binary_does_not_exist() {
    let repository = repository_with(&["kafka-forward"]);
    let present = std::env::current_exe().expect("the test executable should have a path");
    let present = present.display().to_string();

    let report = run_command(command(
        repository.path(),
        &[
            "run-ab",
            "kafka-forward",
            "--baseline-binary",
            "missing/nervix-server",
            "--candidate-binary",
            &present,
        ],
    ))
    .await
    .expect_err("an arm without a server binary cannot run");

    let canonical_root = repository
        .path()
        .canonicalize()
        .expect("the repository root should resolve");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::MissingArmServerBinary { arm: "baseline", path }
            if *path == canonical_root.join("missing/nervix-server")
    ));
}

#[nervix_primitives::test]
async fn names_the_arm_and_run_of_a_failed_comparison_run() {
    let repository = repository_with(&["kafka-forward"]);
    let binary = std::env::current_exe().expect("the test executable should have a path");
    let binary_path = binary.display().to_string();
    let artifacts = repository.path().join("artifacts").display().to_string();

    let report = run_command(command(
        repository.path(),
        &[
            "run-ab",
            "kafka-forward",
            "--baseline-binary",
            &binary_path,
            "--candidate-binary",
            &binary_path,
            "--runs",
            "2",
            "--partitions",
            "0",
            "--artifacts-root",
            &artifacts,
        ],
    ))
    .await
    .expect_err("a comparison whose first run fails must fail");

    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::AbRun { arm: "baseline", label, run: 1, runs: 2 }
            if *label == binary_path
    ));
    assert!(matches!(
        report.downcast_ref::<WorkloadError>(),
        Some(WorkloadError::ZeroPartitions)
    ));
}

#[test]
fn keeps_an_arm_label_and_its_runs_for_the_summary() {
    let repository = tempfile::tempdir().expect("a temporary repository should be created");
    let binary = std::env::current_exe().expect("the test executable should have a path");
    let ab_root = repository.path().join("ab");

    let mut arm = AbRunArm::new(
        repository.path(),
        &ab_root,
        "candidate",
        &binary,
        Some("working tree".to_string()),
    )
    .expect("an existing server binary makes an arm");
    assert_eq!(arm.artifacts_root, ab_root.join("candidate"));
    arm.run_directories.push(ab_root.join("candidate/run-1"));

    let ab_arm = arm.into_ab_arm();
    assert_eq!(ab_arm.label, "working tree");
    assert_eq!(ab_arm.server_binary, binary);
    assert_eq!(
        ab_arm.run_directories,
        vec![ab_root.join("candidate/run-1")]
    );
}

#[test]
fn resolves_a_run_below_its_artifacts_root_and_records_its_manifest() {
    let repository = repository_with(&["kafka-forward"]);
    let catalog =
        BenchmarkCatalog::from_benchmarks_root(repository.path().join(DEFAULT_BENCHMARKS_ROOT));
    let benchmark = catalog
        .load("kafka-forward")
        .expect("the fixture benchmark should load");
    let args = run_args();

    let resolved =
        ResolvedRun::new(repository.path(), &benchmark, &args).expect("a valid run should resolve");

    assert_eq!(resolved.slug, "kafka-forward");
    assert_eq!(resolved.partitions, 3);
    assert_eq!(resolved.value_bytes, 256);
    assert_eq!(resolved.wait_timeout, Duration::from_secs(30));
    assert!(
        resolved
            .run_directory
            .starts_with(repository.path().join("runs/kafka-forward/nervix"))
    );
    assert_eq!(
        resolved.input_topic,
        format!("nervix_bench_kafka_forward_{}_input", resolved.run_token)
    );
    assert_eq!(resolved.domain, format!("benchmark_{}", resolved.run_token));

    fs::create_dir_all(&resolved.run_directory).expect("the run directory should be created");
    let implementation = &benchmark.definition().implementations["nervix"];
    write_run_manifest(
        &resolved,
        "Forwards records.",
        implementation,
        &args,
        repository.path(),
    )
    .expect("the run manifest should be written");
    let manifest: toml::Table = toml::from_str(
        &fs::read_to_string(resolved.run_directory.join("run.toml"))
            .expect("the run manifest should be readable"),
    )
    .expect("the run manifest should be TOML");
    assert_eq!(manifest["benchmark"].as_str(), Some("kafka-forward"));
    assert_eq!(manifest["subject"].as_str(), Some("nervix-local"));
    assert_eq!(manifest["subject_nodes"].as_integer(), Some(1));
    assert_eq!(manifest["value_bytes"].as_integer(), Some(256));
    assert_eq!(manifest["partitions"].as_integer(), Some(3));
    assert!(!manifest.contains_key("image"));

    let container = &benchmark.definition().implementations["vector"];
    write_run_manifest(
        &resolved,
        "Forwards records.",
        container,
        &args,
        repository.path(),
    )
    .expect("the container run manifest should be written");
    let manifest: toml::Table = toml::from_str(
        &fs::read_to_string(resolved.run_directory.join("run.toml"))
            .expect("the run manifest should be readable"),
    )
    .expect("the run manifest should be TOML");
    assert_eq!(manifest["subject"].as_str(), Some("container"));
    assert_eq!(
        manifest["image"].as_str(),
        Some("timberio/vector:0.57.0-debian")
    );
}

#[test]
fn names_a_run_manifest_value_toml_cannot_hold() {
    assert_eq!(
        manifest_integer("value_bytes", 7).expect("seven fits a TOML integer"),
        7
    );

    let report = manifest_integer("value_bytes", u64::MAX)
        .expect_err("u64::MAX exceeds the TOML integer range");

    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::ManifestField {
            field: "value_bytes"
        }
    ));
    assert!(report.contains::<std::num::TryFromIntError>());
}

#[test]
fn passes_paths_to_child_processes_only_as_text() {
    assert_eq!(
        utf8_path(Path::new("/tmp/load-ready")).expect("an ASCII path is text"),
        "/tmp/load-ready"
    );

    let binary = PathBuf::from(OsStr::from_bytes(b"/tmp/load-\xff"));
    let report = utf8_path(&binary).expect_err("a path that is not UTF-8 cannot be an argument");

    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::NonUtf8Path { path } if *path == binary
    ));
}

#[test]
fn finds_the_load_driver_beside_the_running_executable() {
    let executable = std::env::current_exe().expect("the test executable should have a path");

    let load_driver =
        sibling_binary("nervix-benchmark-load").expect("the executable has a directory");

    assert_eq!(
        load_driver.parent(),
        executable.parent(),
        "{}",
        load_driver.display()
    );
    assert!(load_driver.ends_with("nervix-benchmark-load"));
}

#[test]
fn resolves_relative_paths_against_the_repository_root() {
    let root = Path::new("/srv/nervix");

    assert_eq!(
        absolute_or_repository_path(root, Path::new("target/release/nervix-server")),
        root.join("target/release/nervix-server")
    );
    assert_eq!(
        absolute_or_repository_path(root, Path::new("/opt/nervix-server")),
        PathBuf::from("/opt/nervix-server")
    );
}

#[test]
fn passes_each_load_shape_to_the_load_driver() {
    let cases = [
        (LoadShape::UniformPassthrough, vec!["uniform-passthrough"]),
        (LoadShape::UniformUppercase, vec!["uniform-uppercase"]),
        (LoadShape::UniformFilterMap, vec!["uniform-filter-map"]),
        (
            LoadShape::UniformFanout {
                outputs_per_input: 3,
            },
            vec!["uniform-fanout", "--outputs-per-input", "3"],
        ),
        (
            LoadShape::KeyedWindowed {
                keys_per_cycle: 8,
                retained_keys: 4,
                copies_per_key: 2,
                count_field: "record_count".to_string(),
            },
            vec![
                "keyed-windowed",
                "--keys-per-cycle",
                "8",
                "--retained-keys",
                "4",
                "--copies-per-key",
                "2",
                "--count-field",
                "record_count",
            ],
        ),
    ];

    for (shape, expected) in cases {
        assert_eq!(shape_arguments(&shape), expected);
    }
}

#[test]
fn starts_local_and_container_nodes_with_their_own_listeners_and_bootstrap() {
    let ports = LocalPorts::reserve().expect("six local ports should be reserved");
    let reserved = [
        ports.grpc,
        ports.http,
        ports.https,
        ports.observability,
        ports.web_console,
        ports.interconnect,
    ];
    assert!(reserved.iter().all(|port| *port != 0));
    let node = ClusterNodeName::parse("node-2").expect("node-2 is a cluster node name");
    let tls = InterconnectNodeTlsFiles {
        certificate: PathBuf::from("/tls/node.pem"),
        private_key: PathBuf::from("/tls/node-key.pem"),
    };

    let first =
        ports.server_arguments(&node, "benchmark-run", Path::new("/tls/ca.pem"), &tls, None);
    assert!(first.ends_with(&["--allow-bootstrap".to_string()]));
    assert!(first.contains(&format!("127.0.0.1:{}", ports.observability)));
    assert!(first.contains(&"/tls/node-key.pem".to_string()));

    let joining = ports.server_arguments(
        &node,
        "benchmark-run",
        Path::new("/tls/ca.pem"),
        &tls,
        Some("127.0.0.1:47395"),
    );
    assert!(joining.ends_with(&[
        "--cluster-bootstrap-host".to_string(),
        "127.0.0.1:47395".to_string()
    ]));

    let container = container_server_arguments(&node, "benchmark-run", "subject-node-2", None);
    assert_eq!(container[0], "/usr/local/bin/nervix-server");
    assert!(container.contains(&format!("subject-node-2:{NERVIX_INTERCONNECT_PORT}")));
    assert!(container.ends_with(&["--allow-bootstrap".to_string()]));
    let container = container_server_arguments(
        &node,
        "benchmark-run",
        "subject-node-2",
        Some("subject-node-1:47395"),
    );
    assert!(container.ends_with(&[
        "--cluster-bootstrap-host".to_string(),
        "subject-node-1:47395".to_string()
    ]));

    assert_eq!(
        reserve_available_ports(3)
            .expect("three local ports should be reserved")
            .len(),
        3
    );
}

#[test]
fn generates_one_interconnect_certificate_per_benchmark_node() {
    let directory = tempfile::tempdir().expect("a temporary directory should be created");
    let tls_directory = directory.path().join("interconnect-tls");
    let nodes = benchmark_node_names(2);
    let endpoints = vec!["subject-node-1".to_string(), "subject-node-2".to_string()];

    let files = InterconnectTlsFiles::generate(&tls_directory, "benchmark-run", &nodes, &endpoints)
        .expect("the benchmark CA should sign both node certificates");

    assert_eq!(files.ca, tls_directory.join("ca.pem"));
    let ca = fs::read_to_string(&files.ca).expect("the CA certificate should be written");
    assert!(ca.starts_with("-----BEGIN CERTIFICATE-----"));
    assert_eq!(files.nodes.len(), 2);
    for (node, written) in nodes.iter().zip(&files.nodes) {
        assert_eq!(
            written.certificate,
            tls_directory.join(node.as_str()).join("node.pem")
        );
        let key = fs::read_to_string(&written.private_key).expect("the node key should be written");
        assert!(key.starts_with("-----BEGIN PRIVATE KEY-----"));
    }

    let report =
        InterconnectTlsFiles::generate(&tls_directory, "benchmark-run", &nodes, &endpoints[..1])
            .err()
            .assured("two nodes cannot share one endpoint name");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::TlsEndpointCount {
            nodes: 2,
            endpoints: 1
        }
    ));
}

#[test]
fn splits_a_graph_into_the_statements_it_submits() {
    assert_eq!(
        graph_statements("CREATE UNPACED DOMAIN bench;\nSTART;\n")
            .expect("two statements should split"),
        vec!["CREATE UNPACED DOMAIN bench;", "START;"]
    );

    let report = graph_statements("  \n").expect_err("a blank graph has no statements");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::EmptyGraph
    ));

    let report = graph_statements("CREATE RELAY \"unterminated;")
        .expect_err("a graph the client cannot split is refused");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::SplitGraph
    ));
}

#[nervix_primitives::test]
async fn names_the_dependency_endpoint_an_environment_has_not_published() {
    let mut environment = DependencyEnvironment::new("benchmark-test", ContainerMode::Ephemeral);

    start_declared_dependencies(&mut environment, &[])
        .await
        .expect("no declared dependency starts nothing");
    let report = dependency_endpoint(&environment, KAFKA_ADDR)
        .expect_err("an environment that started nothing publishes no endpoint");

    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::DependencyEndpoint { key } if *key == KAFKA_ADDR
    ));
    assert!(report.contains::<io::Error>());
}

#[nervix_primitives::test]
async fn refuses_a_container_id_docker_could_not_have_returned() {
    let run_directory = tempfile::tempdir().expect("a temporary directory should be created");

    capture_container_diagnostics(&[], run_directory.path())
        .await
        .expect("no containers leave nothing to capture");
    assert!(run_directory.path().join("container-diagnostics").is_dir());

    let report =
        capture_container_diagnostics(&["not a container".to_string()], run_directory.path())
            .await
            .expect_err("a container id with spaces cannot be inspected");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::InvalidContainerId { container } if container == "not a container"
    ));
}

#[nervix_primitives::test]
async fn refuses_to_connect_without_an_endpoint_a_password_or_a_valid_domain() {
    let mut connecting = subject(SubjectRuntime::Container { infos: Vec::new() });

    let report = connecting
        .connect_client(DEFAULT_DOMAIN)
        .await
        .err()
        .assured("a subject without a control endpoint has nowhere to connect");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::MissingControlEndpoint
    ));

    connecting.control_url = Some("http://127.0.0.1:1".to_string());
    let report = connecting
        .connect_client(DEFAULT_DOMAIN)
        .await
        .err()
        .assured("a subject without a password cannot authenticate");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::MissingPassword
    ));

    connecting.password = Some("benchmark".to_string());
    let report = connecting
        .connect_client("Not A Domain")
        .await
        .err()
        .assured("the domain name is refused before connecting");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::InvalidDomain { domain } if domain == "Not A Domain"
    ));

    connecting.control_url = Some("not a server address".to_string());
    let report = connecting
        .connect_client(DEFAULT_DOMAIN)
        .await
        .err()
        .assured("a control endpoint that is not a URL cannot be connected to");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::Connect { url } if url == "not a server address"
    ));
    assert!(report.contains::<nervix_client_core::ClientError>());
}

#[nervix_primitives::test]
async fn leaves_a_container_subject_without_metrics_or_local_processes_alone() {
    let run_directory = tempfile::tempdir().expect("a temporary directory should be created");
    let mut container = subject(SubjectRuntime::Container { infos: Vec::new() });

    container
        .ensure_running()
        .expect("a container subject has no local process to exit");
    container
        .capture_nervix_metrics(
            "benchmark_run",
            run_directory.path(),
            Duration::from_secs(1),
        )
        .await
        .expect("a subject without metrics endpoints scrapes nothing");
    assert!(
        !run_directory
            .path()
            .join(NERVIX_METRICS_PROMETHEUS_FILE)
            .exists()
    );

    let mut local = subject(SubjectRuntime::Local { nodes: Vec::new() });
    local
        .capture_logs(run_directory.path())
        .await
        .expect("a local subject writes its logs directly");
    local
        .stop()
        .await
        .expect("a subject without processes has nothing to stop");
}

#[nervix_primitives::test]
async fn names_the_local_node_that_exited_and_stops_the_ones_still_running() {
    let executable = std::env::current_exe().expect("the test executable should have a path");
    let mut exited = Command::new(&executable)
        .arg("--list")
        .stdout(Stdio::null())
        .spawn()
        .expect("the test executable should start");
    let status = exited.wait().await.expect("the listing should finish");
    let node = ClusterNodeName::parse("node-1").expect("node-1 is a cluster node name");
    let mut finished = subject(SubjectRuntime::Local {
        nodes: vec![LocalNode {
            name: node.clone(),
            child: exited,
        }],
    });

    let report = finished
        .ensure_running()
        .expect_err("a node whose process exited is no longer running");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::LocalNodeExited { node: exited_node, status: exited_status }
            if *exited_node == node && *exited_status == status
    ));

    let running = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("a process waiting on its input should start");
    let mut serving = subject(SubjectRuntime::Local {
        nodes: vec![LocalNode {
            name: node,
            child: running,
        }],
    });
    serving
        .ensure_running()
        .expect("a node whose process still runs is running");

    serving
        .stop()
        .await
        .expect("a running node should be stopped");

    let report = serving
        .ensure_running()
        .expect_err("a stopped node is no longer running");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::LocalNodeExited { status, .. } if !status.success()
    ));
}

/// Answers one HTTP request on `listener` with `status` and `body`.
async fn answer_once(listener: nervix_primitives::net::TcpListener, status: &str, body: &str) {
    let (mut stream, _) = listener.accept().await.expect("the scrape should connect");
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        nervix_primitives::task::consume_budget().await;
        let read = stream
            .read(&mut buffer)
            .await
            .expect("the request should be readable");
        request.extend_from_slice(&buffer[..read]);
        let complete = request.windows(4).any(|window| window == b"\r\n\r\n");
        if read == 0 || complete {
            break;
        }
    }
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: \
         close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("the response should be written");
}

async fn metrics_endpoint(status: &'static str, body: &'static str) -> MetricsEndpoint {
    let listener = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a local metrics endpoint should bind");
    let address = listener
        .local_addr()
        .expect("the metrics endpoint should have an address");
    let url = reqwest::Url::parse(&format!("http://{address}/metrics"))
        .expect("the metrics endpoint address is a URL");
    let server = nervix_primitives::task::spawn(answer_once(listener, status, body));
    MetricsEndpoint { url, server }
}

/// A one-request HTTP endpoint standing in for a node's metrics listener.
struct MetricsEndpoint {
    url: reqwest::Url,
    server: nervix_primitives::task::JoinHandle<()>,
}

#[nervix_primitives::test]
async fn scrapes_every_metrics_endpoint_into_the_raw_and_derived_reports() {
    let endpoint = metrics_endpoint("200 OK", SCRAPE).await;
    let run_directory = tempfile::tempdir().expect("a temporary directory should be created");
    let mut scraped = subject(SubjectRuntime::Container { infos: Vec::new() });
    scraped.metrics_urls = vec![endpoint.url.clone()];

    scraped
        .capture_nervix_metrics(
            "benchmark_run",
            run_directory.path(),
            Duration::from_secs(30),
        )
        .await
        .expect("a complete scrape should produce a metrics report");
    endpoint.server.await.expect("the endpoint should answer");

    let raw = fs::read_to_string(run_directory.path().join(NERVIX_METRICS_PROMETHEUS_FILE))
        .expect("the raw scrape should be written");
    assert_eq!(raw, SCRAPE);
    let report = NervixMetricsReport::read(run_directory.path().join(NERVIX_METRICS_REPORT_FILE))
        .expect("the derived report should be written");
    assert_eq!(report.batch_targets[0].batches_total, 6);
    assert_eq!(report.relay_buffers[0].observations, 4);
}

#[nervix_primitives::test]
async fn names_the_endpoint_of_a_failed_scrape_and_a_scrape_without_a_report() {
    let endpoint = metrics_endpoint("503 Service Unavailable", "").await;
    let run_directory = tempfile::tempdir().expect("a temporary directory should be created");
    let mut scraped = subject(SubjectRuntime::Container { infos: Vec::new() });
    scraped.metrics_urls = vec![endpoint.url.clone()];

    let report = scraped
        .capture_nervix_metrics(
            "benchmark_run",
            run_directory.path(),
            Duration::from_secs(30),
        )
        .await
        .expect_err("an unsuccessful scrape cannot produce a report");
    endpoint.server.await.expect("the endpoint should answer");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::ScrapeMetrics { url } if *url == endpoint.url
    ));
    assert!(report.contains::<reqwest::Error>());

    let endpoint = metrics_endpoint("200 OK", "nervix_messages_total 1\n").await;
    scraped.metrics_urls = vec![endpoint.url.clone()];
    let report = scraped
        .capture_nervix_metrics(
            "benchmark_run",
            run_directory.path(),
            Duration::from_secs(30),
        )
        .await
        .expect_err("a scrape without the benchmark's series cannot produce a report");
    endpoint.server.await.expect("the endpoint should answer");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::MetricsReport
    ));
}

#[nervix_primitives::test]
async fn names_the_directory_a_run_cannot_create() {
    let repository = repository_with(&["kafka-forward"]);
    let occupied = repository.path().join("occupied");
    fs::write(&occupied, "a file where the artifacts would go")
        .expect("the occupying file should be written");
    let artifacts_root = occupied.join("runs");
    let artifacts_argument = artifacts_root.display().to_string();

    let report = run_command(command(
        repository.path(),
        &["run-all", "--artifacts-root", &artifacts_argument],
    ))
    .await
    .expect_err("an artifacts root below a file cannot be created");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::CreateDirectory { path } if *path == artifacts_root
    ));

    let report = run_command(command(
        repository.path(),
        &[
            "run",
            "kafka-forward",
            "--artifacts-root",
            &artifacts_argument,
        ],
    ))
    .await
    .expect_err("a run directory below a file cannot be created");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::CreateDirectory { path } if path.starts_with(&artifacts_root)
    ));

    let report = capture_container_diagnostics(&[], &occupied)
        .await
        .expect_err("container diagnostics below a file cannot be captured");
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::CreateDirectory { path }
            if *path == occupied.join("container-diagnostics")
    ));
}

#[test]
fn names_the_interconnect_certificate_material_it_cannot_create() {
    let directory = tempfile::tempdir().expect("a temporary directory should be created");
    let nodes = benchmark_node_names(1);
    let endpoints = vec!["subject-node-1".to_string()];
    let failure = |tls_directory: &Path, endpoints: &[String]| {
        InterconnectTlsFiles::generate(tls_directory, "benchmark-run", &nodes, endpoints)
            .err()
            .assured("the interconnect material cannot be created")
    };

    let occupied = directory.path().join("occupied");
    fs::write(&occupied, "a file where the material would go")
        .expect("the occupying file should be written");
    let report = failure(&occupied.join("interconnect-tls"), &endpoints);
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::CreateDirectory { path } if *path == occupied.join("interconnect-tls")
    ));

    let node_occupied = directory.path().join("node-occupied");
    fs::create_dir_all(&node_occupied).expect("the TLS directory should be created");
    fs::write(
        node_occupied.join("node-1"),
        "a file where the node directory would go",
    )
    .expect("the occupying file should be written");
    let report = failure(&node_occupied, &endpoints);
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::CreateDirectory { path } if *path == node_occupied.join("node-1")
    ));

    let report = failure(
        &directory.path().join("non-ascii"),
        &["sübject-node-1".to_string()],
    );
    assert!(matches!(
        report.current_context(),
        BenchmarkCliError::GenerateNodeCertificate { node } if *node == nodes[0]
    ));

    for written in ["node.pem", "node-key.pem"] {
        let tls_directory = directory.path().join(format!("occupied-{written}"));
        let occupied = tls_directory.join("node-1").join(written);
        fs::create_dir_all(&occupied).expect("a directory should take the file's place");

        let report = failure(&tls_directory, &endpoints);

        assert!(
            matches!(
                report.current_context(),
                BenchmarkCliError::WriteFile { path } if *path == occupied
            ),
            "{written}: {report:?}"
        );
    }
}
