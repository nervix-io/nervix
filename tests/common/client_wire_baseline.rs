//! End-to-end measurements of the public client protocol against a real server process.
//!
//! Outside the layer order: a benchmark harness. Product code must not name it.
//!
//! - **Owns.** The fixed client-wire workload, the exact frame bytes it exchanges over gRPC and the
//!   console WebSocket, process counters, Prometheus scrape, and reproducible JSON artifact.
//! - **Depends on.** Public gRPC, HTTP, WebSocket, metrics, and process interfaces plus the shared
//!   real-process fixture.
//! - **Must not know.** Registry, consensus, runtime, or connector implementation internals.

use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow, ensure};
use futures_util::{SinkExt as _, StreamExt as _};
use hdrhistogram::Histogram;
use nervix_client_core::{
    Client, CommandOutcome, ResourceUploadIdentity, SubscriptionEvent, SubscriptionRequest,
};
use nervix_client_wire::{
    ClientMessage, ClientRequest, DomainSelection, OutcomeOrigin, ReplyBody, RequestId,
    SelectDomainRequest, ServerEvent, ServerMessage, SessionLimits, UploadChunk, UploadDisposition,
    UploadReply, UploadStart, VerifiedFrame,
};
use nervix_models::{DomainName, ResourceName};
use serde::{Deserialize, Serialize};
use tikv_jemalloc_ctl::{epoch, stats};
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message as WebSocketMessage, client::IntoClientRequest as _, http::HeaderValue},
};
use triomphe::Arc;
use uuid::Uuid;

use super::{
    cluster::{client_connect_options, client_domain, test_basic_authorization},
    raw_session::outcome_succeeded,
    server_process::ServerProcess,
};

const DEFAULT_SAMPLES: usize = 100;
const DEFAULT_UPLOAD_SAMPLES: usize = 5;
const DEFAULT_PAYLOAD_BYTES: usize = 1024;
const CONTROL_COMMAND_SAMPLES: usize = 20;
const RECORDS_PER_CONTROL_COMMAND: usize = 8;
const MAX_SAMPLES: usize = 10_000;
const MAX_UPLOAD_BYTES: usize = 16 * 1024 * 1024;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const COMMAND_QUERY: &str = "DESCRIBE DOMAIN;";
const METRICS_FILE: &str = "metrics.prom";
const REPORT_FILE: &str = "client-wire-baseline.json";

#[derive(Clone, Debug, Serialize)]
struct Workload {
    command_samples: usize,
    subscription_samples: usize,
    graph_snapshot_samples: usize,
    upload_samples: usize,
    subscription_detail_bytes: usize,
    upload_file_bytes: usize,
    paused_subscriber_records: usize,
    slow_subscriber_records: usize,
    concrete_branches: Vec<&'static str>,
}

impl Workload {
    async fn publish_control_burst(
        &self,
        process: &ServerProcess,
        host: &str,
        sample: usize,
    ) -> Result<()> {
        let detail = "x".repeat(self.subscription_detail_bytes);
        for offset in 0..RECORDS_PER_CONTROL_COMMAND {
            tokio::task::consume_budget().await;
            let index = sample * RECORDS_PER_CONTROL_COMMAND + offset;
            let tenant = if index.is_multiple_of(2) {
                "acme"
            } else {
                "beta"
            };
            let sequence = i64::try_from(index).context("control record index does not fit i64")?;
            let payload = serde_json::json!({
                "tenant": tenant,
                "sequence": sequence,
                "detail": detail,
            })
            .to_string();
            timeout(
                OPERATION_TIMEOUT,
                process.publish_http(host, "/records", &payload),
            )
            .await
            .context("subscriber control publication timed out")??;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Settings {
    output_directory: PathBuf,
    workload: Workload,
}

impl Settings {
    fn from_environment() -> Result<Self> {
        let samples = environment_usize("NERVIX_CLIENT_WIRE_BASELINE_SAMPLES", DEFAULT_SAMPLES)?;
        let upload_samples = environment_usize(
            "NERVIX_CLIENT_WIRE_BASELINE_UPLOAD_SAMPLES",
            DEFAULT_UPLOAD_SAMPLES,
        )?;
        let payload_bytes = environment_usize(
            "NERVIX_CLIENT_WIRE_BASELINE_PAYLOAD_BYTES",
            DEFAULT_PAYLOAD_BYTES,
        )?;
        ensure!(
            samples > 0,
            "client-wire baseline sample count must be positive"
        );
        ensure!(
            samples <= MAX_SAMPLES,
            "client-wire baseline sample count exceeds {MAX_SAMPLES}"
        );
        ensure!(
            upload_samples > 0 && upload_samples <= MAX_SAMPLES,
            "client-wire upload sample count must be in 1..={MAX_SAMPLES}"
        );
        ensure!(
            payload_bytes > 0 && payload_bytes <= MAX_UPLOAD_BYTES,
            "client-wire payload bytes must be in 1..={MAX_UPLOAD_BYTES}"
        );
        let output_directory = std::env::var_os("NERVIX_CLIENT_WIRE_BASELINE_OUTPUT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/client-wire-baseline"));
        Ok(Self {
            output_directory,
            workload: Workload {
                command_samples: samples,
                subscription_samples: samples,
                graph_snapshot_samples: samples,
                upload_samples,
                subscription_detail_bytes: payload_bytes,
                upload_file_bytes: payload_bytes,
                paused_subscriber_records: CONTROL_COMMAND_SAMPLES * RECORDS_PER_CONTROL_COMMAND,
                slow_subscriber_records: CONTROL_COMMAND_SAMPLES * RECORDS_PER_CONTROL_COMMAND,
                concrete_branches: vec!["acme", "beta"],
            },
        })
    }
}

#[derive(Debug, Serialize)]
struct BaselineReport {
    schema_version: u32,
    captured_at_utc: String,
    git_commit: String,
    git_worktree_status: String,
    protocol: &'static str,
    workload: Workload,
    environment: Environment,
    configured_limits: BTreeMap<&'static str, u64>,
    operations: Operations,
    client_process: ProcessReport,
    server_process: ProcessReport,
    process_phases: Vec<ProcessPhase>,
    memory_phases: Vec<MemoryPhase>,
    queue_occupancy: Vec<QueueOccupancyPhase>,
    raw_metrics_file: &'static str,
}

#[derive(Debug, Serialize)]
struct Environment {
    nervix_version: &'static str,
    rustc: String,
    kernel: String,
    target_os: &'static str,
    target_arch: &'static str,
    server_build_profile: String,
    harness_build_profile: &'static str,
    cpu_model: String,
    logical_cpus: usize,
    physical_memory_bytes: u64,
    clock_ticks_per_second: u64,
}

#[derive(Debug, Serialize)]
struct Operations {
    command: OperationReport,
    typed_json_subscription: OperationReport,
    graph_snapshot: OperationReport,
    resource_upload: OperationReport,
    prepared_native_command: PreparedCommandReport,
    control_with_paused_subscriber: OperationReport,
    control_with_slow_subscriber: SlowControlReport,
}

#[derive(Debug, Serialize)]
struct PreparedCommandReport {
    prepare_microseconds: Distribution,
    execute_microseconds: Distribution,
}

#[derive(Debug, Serialize)]
struct SlowControlReport {
    operation: OperationReport,
    row_events: u64,
    consumer_overflow_events: u64,
    delivery_lost_events: u64,
    rows_skipped_events: u64,
    drain_timeouts: u64,
}

#[derive(Debug, Serialize)]
struct OperationReport {
    transport: &'static str,
    elapsed_seconds: f64,
    latency_microseconds: Distribution,
    request_encoded_bytes: Distribution,
    response_encoded_bytes: Distribution,
}

#[derive(Debug, Serialize)]
struct Distribution {
    samples: u64,
    total: u64,
    min: u64,
    p50: u64,
    p90: u64,
    p95: u64,
    p99: u64,
    max: u64,
    values: Vec<u64>,
}

struct Samples {
    histogram: Histogram<u64>,
    total: u64,
    values: Vec<u64>,
}

impl Samples {
    fn new() -> Result<Self> {
        Ok(Self {
            histogram: Histogram::new(3).context("failed to create baseline histogram")?,
            total: 0,
            values: Vec::new(),
        })
    }

    fn record(&mut self, value: u64) -> Result<()> {
        let recorded = value.max(1);
        self.histogram
            .record(recorded)
            .context("baseline observation exceeded histogram range")?;
        self.total = self
            .total
            .checked_add(value)
            .ok_or_else(|| anyhow!("baseline observation total overflowed"))?;
        self.values.push(value);
        Ok(())
    }

    fn record_usize(&mut self, value: usize) -> Result<()> {
        self.record(u64::try_from(value).context("encoded size does not fit u64")?)
    }

    fn record_duration(&mut self, duration: Duration) -> Result<()> {
        let micros = u64::try_from(duration.as_micros())
            .context("operation duration does not fit microseconds in u64")?;
        self.record(micros)
    }

    fn finish(self) -> Distribution {
        Distribution {
            samples: self.histogram.len(),
            total: self.total,
            min: self.histogram.min(),
            p50: self.histogram.value_at_quantile(0.50),
            p90: self.histogram.value_at_quantile(0.90),
            p95: self.histogram.value_at_quantile(0.95),
            p99: self.histogram.value_at_quantile(0.99),
            max: self.histogram.max(),
            values: self.values,
        }
    }
}

struct OperationSamples {
    started: Instant,
    latency: Samples,
    request_bytes: Samples,
    response_bytes: Samples,
}

impl OperationSamples {
    fn new() -> Result<Self> {
        Ok(Self {
            started: Instant::now(),
            latency: Samples::new()?,
            request_bytes: Samples::new()?,
            response_bytes: Samples::new()?,
        })
    }

    fn finish(self, transport: &'static str) -> OperationReport {
        OperationReport {
            transport,
            elapsed_seconds: self.started.elapsed().as_secs_f64(),
            latency_microseconds: self.latency.finish(),
            request_encoded_bytes: self.request_bytes.finish(),
            response_encoded_bytes: self.response_bytes.finish(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ProcessCounters {
    user_cpu_ticks: u64,
    system_cpu_ticks: u64,
    resident_bytes: u64,
    peak_resident_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ProcessReport {
    pid: u32,
    before: ProcessCounters,
    after: ProcessCounters,
    user_cpu_ticks: u64,
    system_cpu_ticks: u64,
}

#[derive(Debug, Serialize)]
struct ProcessPhase {
    phase: &'static str,
    client: ProcessCounters,
    server: ProcessCounters,
}

impl ProcessReport {
    fn from_counters(pid: u32, before: ProcessCounters, after: ProcessCounters) -> Result<Self> {
        let user_cpu_ticks = after
            .user_cpu_ticks
            .checked_sub(before.user_cpu_ticks)
            .ok_or_else(|| anyhow!("process user CPU counter moved backwards"))?;
        let system_cpu_ticks = after
            .system_cpu_ticks
            .checked_sub(before.system_cpu_ticks)
            .ok_or_else(|| anyhow!("process system CPU counter moved backwards"))?;
        Ok(Self {
            pid,
            before,
            after,
            user_cpu_ticks,
            system_cpu_ticks,
        })
    }
}

#[derive(Debug, Serialize)]
struct MemoryPhase {
    phase: &'static str,
    client_allocated_bytes: u64,
    client_resident_bytes: u64,
    server_allocated_bytes: f64,
    server_resident_bytes: f64,
}

#[derive(Debug, Serialize)]
struct PrometheusSample {
    series: String,
    value: f64,
}

#[derive(Debug, Serialize)]
struct QueueOccupancyPhase {
    phase: &'static str,
    samples: Vec<PrometheusSample>,
}

#[derive(Debug, Deserialize)]
struct SubscriptionPayload {
    tenant: String,
    sequence: i64,
    detail: String,
}

pub(crate) async fn capture(
    process: &ServerProcess,
    domain: &str,
    test_id: &str,
) -> Result<PathBuf> {
    let settings = Settings::from_environment()?;
    fs::create_dir_all(&settings.output_directory).with_context(|| {
        format!(
            "failed to create client-wire baseline directory {}",
            settings.output_directory.display()
        )
    })?;
    let host = format!("client-wire-baseline-{test_id}.example.com");
    configure_graph(process, domain, &host).await?;

    let client_pid = std::process::id();
    let server_pid = process.process_id()?;
    let client_before = read_process_counters(client_pid)?;
    let server_before = read_process_counters(server_pid)?;
    let mut process_phases = vec![ProcessPhase {
        phase: "before",
        client: client_before.clone(),
        server: server_before.clone(),
    }];
    let mut memory_phases = Vec::new();
    let mut queue_occupancy = Vec::new();
    let initial_metrics = scrape_metrics(process).await?;
    record_memory_phase("before", &initial_metrics, &mut memory_phases)?;
    record_queue_occupancy("before", &initial_metrics, &mut queue_occupancy)?;

    let command = measure_commands(process, domain, settings.workload.command_samples).await?;
    let command_metrics = scrape_metrics(process).await?;
    record_memory_phase("commands", &command_metrics, &mut memory_phases)?;
    record_process_phase("commands", client_pid, server_pid, &mut process_phases)?;
    record_queue_occupancy("commands", &command_metrics, &mut queue_occupancy)?;

    let subscription = measure_subscriptions(
        process,
        domain,
        &host,
        settings.workload.subscription_samples,
        settings.workload.subscription_detail_bytes,
    )
    .await?;
    let subscription_metrics = scrape_metrics(process).await?;
    record_memory_phase("subscriptions", &subscription_metrics, &mut memory_phases)?;
    record_process_phase("subscriptions", client_pid, server_pid, &mut process_phases)?;
    record_queue_occupancy("subscriptions", &subscription_metrics, &mut queue_occupancy)?;

    let graph_snapshot =
        measure_graph_snapshots(process, domain, settings.workload.graph_snapshot_samples).await?;
    let snapshot_metrics = scrape_metrics(process).await?;
    record_memory_phase("graph_snapshots", &snapshot_metrics, &mut memory_phases)?;
    record_process_phase(
        "graph_snapshots",
        client_pid,
        server_pid,
        &mut process_phases,
    )?;
    record_queue_occupancy("graph_snapshots", &snapshot_metrics, &mut queue_occupancy)?;

    let resource_upload = measure_uploads(
        process,
        domain,
        settings.workload.upload_samples,
        settings.workload.upload_file_bytes,
    )
    .await?;
    let upload_metrics = scrape_metrics(process).await?;
    record_memory_phase("resource_uploads", &upload_metrics, &mut memory_phases)?;
    record_process_phase(
        "resource_uploads",
        client_pid,
        server_pid,
        &mut process_phases,
    )?;
    record_queue_occupancy("resource_uploads", &upload_metrics, &mut queue_occupancy)?;

    let prepared_native_command =
        measure_prepared_native_commands(process, domain, settings.workload.command_samples)
            .await?;
    let prepared_metrics = scrape_metrics(process).await?;
    record_memory_phase("prepared_commands", &prepared_metrics, &mut memory_phases)?;
    record_process_phase(
        "prepared_commands",
        client_pid,
        server_pid,
        &mut process_phases,
    )?;
    record_queue_occupancy("prepared_commands", &prepared_metrics, &mut queue_occupancy)?;

    let control_with_paused_subscriber =
        measure_control_with_paused_subscriber(process, domain, &host, &settings.workload).await?;
    let paused_metrics = scrape_metrics(process).await?;
    record_memory_phase("paused_subscriber", &paused_metrics, &mut memory_phases)?;
    record_process_phase(
        "paused_subscriber",
        client_pid,
        server_pid,
        &mut process_phases,
    )?;
    record_queue_occupancy("paused_subscriber", &paused_metrics, &mut queue_occupancy)?;

    let control_with_slow_subscriber =
        measure_control_with_slow_subscriber(process, domain, &host, &settings.workload).await?;
    let final_metrics = scrape_metrics(process).await?;
    record_memory_phase("slow_subscriber", &final_metrics, &mut memory_phases)?;
    let client_after = read_process_counters(client_pid)?;
    let server_after = read_process_counters(server_pid)?;
    process_phases.push(ProcessPhase {
        phase: "slow_subscriber",
        client: client_after.clone(),
        server: server_after.clone(),
    });
    record_queue_occupancy("slow_subscriber", &final_metrics, &mut queue_occupancy)?;
    ensure!(
        queue_occupancy
            .iter()
            .any(|phase| !phase.samples.is_empty()),
        "metrics scrapes contained no relay queue occupancy samples"
    );
    let metrics_path = settings.output_directory.join(METRICS_FILE);
    fs::write(&metrics_path, &final_metrics)
        .with_context(|| format!("failed to write raw metrics to {}", metrics_path.display()))?;

    let report = BaselineReport {
        schema_version: 2,
        captured_at_utc: chrono::Utc::now().to_rfc3339(),
        git_commit: command_output("git", &["rev-parse", "HEAD"])?,
        git_worktree_status: command_output("git", &["status", "--short"])?,
        protocol: "FlatBuffers frames over native gRPC and console WebSocket",
        workload: settings.workload,
        environment: capture_environment()?,
        configured_limits: configured_limits(),
        operations: Operations {
            command,
            typed_json_subscription: subscription,
            graph_snapshot,
            resource_upload,
            prepared_native_command,
            control_with_paused_subscriber,
            control_with_slow_subscriber,
        },
        client_process: ProcessReport::from_counters(client_pid, client_before, client_after)?,
        server_process: ProcessReport::from_counters(server_pid, server_before, server_after)?,
        process_phases,
        memory_phases,
        queue_occupancy,
        raw_metrics_file: METRICS_FILE,
    };
    let report_path = settings.output_directory.join(REPORT_FILE);
    let report_bytes = serde_json::to_vec_pretty(&report)
        .context("failed to serialize client-wire baseline report")?;
    fs::write(&report_path, report_bytes).with_context(|| {
        format!(
            "failed to write client-wire baseline report {}",
            report_path.display()
        )
    })?;
    Ok(report_path)
}

async fn configure_graph(process: &ServerProcess, domain: &str, host: &str) -> Result<()> {
    let graph = format!(
        r#"
        CREATE UNPACED DOMAIN {domain};
        CREATE SCHEMA client_wire_record (
          tenant STRING,
          sequence I64,
          detail STRING
        );
        CREATE WIRE JSON SCHEMA client_wire_json MODE STRICT (
          tenant string,
          sequence integer,
          detail string
        );
        CREATE CODEC client_wire_codec
          FROM WIRE JSON SCHEMA client_wire_json
          TO SCHEMA client_wire_record;
        CREATE SCHEMA client_wire_tenant ( tenant STRING );
        CREATE BRANCH client_wire_by_tenant SCHEMA client_wire_tenant TTL 5m;
        CREATE RELAY client_wire_records
          SCHEMA client_wire_record BRANCHED BY client_wire_by_tenant;
        CREATE VHOST client_wire_edge {host};
        CREATE ENDPOINT client_wire_ingress
          ON client_wire_edge PATH '/records' TYPE HTTP;
        CREATE INGESTOR client_wire_source
          FROM ENDPOINT client_wire_ingress MODE NO_ACK SEQUENTIAL
          ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING client_wire_codec
          TO client_wire_records INHERIT ALL
          BRANCHED BY client_wire_by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE RESOURCE client_wire_resource;
        START;
        "#
    );
    let statements = nervix_client_core::split_query_statements(&graph)
        .map_err(|error| anyhow!("failed to split baseline graph: {error:#}"))?;
    for statement in statements {
        tokio::task::consume_budget().await;
        process
            .run_commands(domain, statement)
            .await
            .with_context(|| format!("baseline setup command failed: {statement}"))?;
    }
    Ok(())
}

async fn measure_commands(
    process: &ServerProcess,
    domain: &str,
    samples: usize,
) -> Result<OperationReport> {
    let mut session = process.open_session(domain).await?;
    let warmup = session.observe_command(COMMAND_QUERY).await?;
    ensure!(
        outcome_succeeded(&warmup.result),
        "command warm-up failed: {}",
        warmup.result.message
    );
    let mut observations = OperationSamples::new()?;
    for _ in 0..samples {
        tokio::task::consume_budget().await;
        let started = Instant::now();
        let observed = timeout(OPERATION_TIMEOUT, session.observe_command(COMMAND_QUERY))
            .await
            .context("command baseline timed out")??;
        observations.latency.record_duration(started.elapsed())?;
        observations
            .request_bytes
            .record_usize(observed.request_frame_bytes)?;
        observations
            .response_bytes
            .record_usize(observed.response_frame_bytes)?;
        ensure!(
            outcome_succeeded(&observed.result),
            "command baseline failed: {}",
            observed.result.message
        );
    }
    Ok(observations.finish("native gRPC bidirectional stream"))
}

/// Separates client parsing/preparation from the same command's request and reply against the
/// release server. Each sample uses a fresh execution identity, as the regular command does.
async fn measure_prepared_native_commands(
    process: &ServerProcess,
    domain: &str,
    samples: usize,
) -> Result<PreparedCommandReport> {
    let grpc_uri = process.grpc_uri();
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(domain),
        client_connect_options(&grpc_uri)?,
    )
    .await
    .context("failed to connect prepared command client")?;
    let warmup = client.execute(COMMAND_QUERY).await?;
    ensure!(warmup.succeeded(), "prepared command warm-up failed");
    let mut prepare = Samples::new()?;
    let mut execute = Samples::new()?;
    for _ in 0..samples {
        tokio::task::consume_budget().await;
        let started = Instant::now();
        let prepared = client.prepare_execution(COMMAND_QUERY).await;
        prepare.record_duration(started.elapsed())?;
        let started = Instant::now();
        let outcome = timeout(OPERATION_TIMEOUT, client.execute_prepared(&prepared))
            .await
            .context("prepared command timed out")??;
        execute.record_duration(started.elapsed())?;
        ensure!(outcome.succeeded(), "prepared command failed");
    }
    Ok(PreparedCommandReport {
        prepare_microseconds: prepare.finish(),
        execute_microseconds: execute.finish(),
    })
}

async fn measure_subscriptions(
    process: &ServerProcess,
    domain: &str,
    host: &str,
    samples: usize,
    detail_bytes: usize,
) -> Result<OperationReport> {
    let grpc_uri = process.grpc_uri();
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(domain),
        client_connect_options(&grpc_uri)?,
    )
    .await
    .context("failed to connect native subscription client")?;
    let subscription_name = "client_wire_seen";
    let outcome = client
        .subscribe(&SubscriptionRequest::new(
            subscription_name,
            "client_wire_records",
        ))
        .await
        .context("failed to create native subscription")?;
    ensure!(
        outcome.succeeded(),
        "failed to create subscription: {}",
        outcome.message
    );
    let detail = "x".repeat(detail_bytes);
    let mut observations = OperationSamples::new()?;
    for index in 0..samples {
        tokio::task::consume_budget().await;
        let tenant = if index % 2 == 0 { "acme" } else { "beta" };
        let sequence = i64::try_from(index).context("subscription index does not fit i64")?;
        let payload = serde_json::json!({
            "tenant": tenant,
            "sequence": sequence,
            "detail": detail,
        })
        .to_string();
        let started = Instant::now();
        process
            .publish_http(host, "/records", &payload)
            .await
            .context("failed to publish subscription baseline payload")?;
        let event = timeout(OPERATION_TIMEOUT, client.next_subscription())
            .await
            .context("subscription baseline timed out")??;
        observations.latency.record_duration(started.elapsed())?;
        observations.request_bytes.record_usize(payload.len())?;
        let SubscriptionEvent::Rows(rows) = event else {
            return Err(anyhow!(
                "the subscription delivered {event:?} instead of rows"
            ));
        };
        let lines = rows
            .display_lines()
            .map_err(|report| anyhow!("subscription rows do not render: {report}"))?;
        let [line] = lines.as_slice() else {
            return Err(anyhow!("one record delivered {} rows", lines.len()));
        };
        let (_, record_json) = line
            .split_once(" payload=")
            .ok_or_else(|| anyhow!("branched subscription row omitted its branch key"))?;
        let decoded: SubscriptionPayload = serde_json::from_str(record_json)
            .context("subscription payload is not current JSON")?;
        ensure!(
            decoded.tenant == tenant
                && decoded.sequence == sequence
                && decoded.detail.len() == detail_bytes,
            "typed subscription payload changed during the baseline"
        );
        observations
            .response_bytes
            .record_usize(rows.rows.frame().len())?;
    }
    Ok(observations.finish("HTTP admission to native gRPC typed Row subscription frames"))
}

/// Measures command replies while an independent gRPC subscriber leaves its response stream
/// unread. Publishing is interleaved with control requests so the subscriber stays active during
/// every measured command rather than being a historical load phase.
async fn measure_control_with_paused_subscriber(
    process: &ServerProcess,
    domain: &str,
    host: &str,
    workload: &Workload,
) -> Result<OperationReport> {
    let mut paused = process.open_session(domain).await?;
    let opened = paused
        .observe_command("CREATE SUBSCRIPTION client_wire_paused TO client_wire_records;")
        .await
        .context("failed to create paused subscriber")?;
    ensure!(
        outcome_succeeded(&opened.result),
        "paused subscriber did not open"
    );

    let mut control = process.open_session(domain).await?;
    let warmup = control.observe_command(COMMAND_QUERY).await?;
    ensure!(
        outcome_succeeded(&warmup.result),
        "paused control warm-up failed"
    );
    let mut observations = OperationSamples::new()?;
    for sample in 0..CONTROL_COMMAND_SAMPLES {
        tokio::task::consume_budget().await;
        workload
            .publish_control_burst(process, host, sample)
            .await?;
        let started = Instant::now();
        let observed = timeout(OPERATION_TIMEOUT, control.observe_command(COMMAND_QUERY))
            .await
            .context("paused subscriber control request timed out")??;
        observations.latency.record_duration(started.elapsed())?;
        observations
            .request_bytes
            .record_usize(observed.request_frame_bytes)?;
        observations
            .response_bytes
            .record_usize(observed.response_frame_bytes)?;
        ensure!(
            outcome_succeeded(&observed.result),
            "paused control command failed"
        );
    }
    drop(paused);
    Ok(observations.finish("native gRPC commands with an unread Row subscriber"))
}

/// The application drains only one event after each burst of records, leaving a growing bounded
/// backlog in its Rust client while independent control requests complete.
async fn measure_control_with_slow_subscriber(
    process: &ServerProcess,
    domain: &str,
    host: &str,
    workload: &Workload,
) -> Result<SlowControlReport> {
    let grpc_uri = process.grpc_uri();
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(domain),
        client_connect_options(&grpc_uri)?,
    )
    .await
    .context("failed to connect slow native subscription client")?;
    let outcome = client
        .subscribe(&SubscriptionRequest::new(
            "client_wire_slow",
            "client_wire_records",
        ))
        .await
        .context("failed to create slow native subscription")?;
    ensure!(outcome.succeeded(), "slow subscriber did not open");

    let mut control = process.open_session(domain).await?;
    let warmup = control.observe_command(COMMAND_QUERY).await?;
    ensure!(
        outcome_succeeded(&warmup.result),
        "slow control warm-up failed"
    );
    let mut observations = OperationSamples::new()?;
    let mut row_events = 0_u64;
    let mut consumer_overflow_events = 0_u64;
    let mut delivery_lost_events = 0_u64;
    let mut rows_skipped_events = 0_u64;
    let mut drain_timeouts = 0_u64;
    for sample in 0..CONTROL_COMMAND_SAMPLES {
        tokio::task::consume_budget().await;
        workload
            .publish_control_burst(process, host, sample)
            .await?;
        let started = Instant::now();
        let observed = timeout(OPERATION_TIMEOUT, control.observe_command(COMMAND_QUERY))
            .await
            .context("slow subscriber control request timed out")??;
        observations.latency.record_duration(started.elapsed())?;
        observations
            .request_bytes
            .record_usize(observed.request_frame_bytes)?;
        observations
            .response_bytes
            .record_usize(observed.response_frame_bytes)?;
        ensure!(
            outcome_succeeded(&observed.result),
            "slow control command failed"
        );
        match timeout(Duration::from_millis(100), client.next_subscription()).await {
            Ok(Ok(SubscriptionEvent::Rows(_))) => row_events += 1,
            Ok(Ok(SubscriptionEvent::ConsumerOverflow(_))) => consumer_overflow_events += 1,
            Ok(Ok(SubscriptionEvent::DeliveryLost(_))) => delivery_lost_events += 1,
            Ok(Ok(SubscriptionEvent::RowsSkipped(_))) => rows_skipped_events += 1,
            Ok(Ok(SubscriptionEvent::Ended(_) | SubscriptionEvent::Interrupted(_))) => {
                return Err(anyhow!("slow subscriber ended during control measurement"));
            }
            Ok(Err(error)) => return Err(error).context("slow subscriber read failed"),
            Err(_) => drain_timeouts += 1,
        }
    }
    Ok(SlowControlReport {
        operation: observations.finish("native gRPC commands with a slowly drained Row subscriber"),
        row_events,
        consumer_overflow_events,
        delivery_lost_events,
        rows_skipped_events,
        drain_timeouts,
    })
}

async fn measure_graph_snapshots(
    process: &ServerProcess,
    domain: &str,
    samples: usize,
) -> Result<OperationReport> {
    let mut request = process
        .web_console_websocket_uri()
        .into_client_request()
        .context("failed to build console WebSocket request")?;
    request.headers_mut().insert(
        "authorization",
        HeaderValue::from_str(&test_basic_authorization())
            .context("test authorization is not an HTTP header")?,
    );
    let (mut websocket, _) = connect_async(request)
        .await
        .context("failed to connect console WebSocket")?;
    let limits = SessionLimits::DEFAULT;
    let domain_name = DomainName::parse(domain)
        .map_err(|report| anyhow!("the baseline domain is invalid: {report:?}"))?;
    let mut observations = OperationSamples::new()?;
    for sample in 1..=samples {
        tokio::task::consume_budget().await;
        let request_id = u64::try_from(sample)
            .ok()
            .and_then(std::num::NonZeroU64::new)
            .map(RequestId::new)
            .ok_or_else(|| anyhow!("a sample number is not a request identity"))?;
        let request = ClientMessage {
            request_id,
            request: ClientRequest::SelectDomain(SelectDomainRequest {
                domain: domain_name.clone(),
            }),
        };
        let request_bytes = request
            .encode(&limits)
            .map_err(|report| anyhow!("a domain selection does not fit a frame: {report:?}"))?
            .into_bytes();
        let started = Instant::now();
        websocket
            .send(WebSocketMessage::Binary(request_bytes.to_vec()))
            .await
            .context("failed to request graph snapshot")?;
        let response_bytes = timeout(
            OPERATION_TIMEOUT,
            next_domain_snapshot(&mut websocket, domain),
        )
        .await
        .context("graph snapshot baseline timed out")??;
        observations.latency.record_duration(started.elapsed())?;
        observations
            .request_bytes
            .record_usize(request_bytes.len())?;
        observations.response_bytes.record_usize(response_bytes)?;
    }
    websocket
        .close(None)
        .await
        .context("failed to close console WebSocket")?;
    Ok(observations.finish("console WebSocket FlatBuffers frames"))
}

async fn next_domain_snapshot<S>(
    websocket: &mut tokio_tungstenite::WebSocketStream<S>,
    domain: &str,
) -> Result<usize>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        tokio::task::consume_budget().await;
        let message = websocket
            .next()
            .await
            .ok_or_else(|| anyhow!("console WebSocket closed before a graph snapshot"))??;
        let WebSocketMessage::Binary(payload) = message else {
            continue;
        };
        let payload_bytes = payload.len();
        let frame = VerifiedFrame::verify(payload.into(), &SessionLimits::DEFAULT)
            .map_err(|report| anyhow!("console message is not a verified frame: {report:?}"))?;
        let message = ServerMessage::decode(&frame)
            .map_err(|report| anyhow!("console frame does not decode: {report:?}"))?;
        let snapshot = match message {
            ServerMessage::Event(ServerEvent::DomainSnapshot(snapshot)) => snapshot,
            ServerMessage::Reply(reply) => {
                if let ReplyBody::DomainSelection(DomainSelection::NotFound(missing)) = reply.body {
                    return Err(anyhow!("the console did not find domain '{missing}'"));
                }
                continue;
            }
            _ => continue,
        };
        if snapshot.domain().as_str() != domain {
            continue;
        }
        ensure!(
            !snapshot.graph_json().is_empty(),
            "console returned an empty graph snapshot"
        );
        return Ok(payload_bytes);
    }
}

async fn measure_uploads(
    process: &ServerProcess,
    domain: &str,
    samples: usize,
    file_bytes: usize,
) -> Result<OperationReport> {
    let grpc_uri = process.grpc_uri();
    let client = Client::connect_with_options(
        &grpc_uri,
        client_domain(domain),
        client_connect_options(&grpc_uri)?,
    )
    .await
    .context("failed to connect resource upload client")?;
    let directory = tempfile::Builder::new()
        .prefix("client-wire-upload-")
        .tempdir()
        .context("failed to create upload input directory")?;
    fs::write(directory.path().join("payload.bin"), vec![b'x'; file_bytes])
        .context("failed to create upload input")?;
    let mut observations = OperationSamples::new()?;
    for _ in 0..samples {
        tokio::task::consume_budget().await;
        let identity = ResourceUploadIdentity::parse(Uuid::now_v7().to_string())
            .map_err(|report| anyhow!("failed to create upload identity: {report}"))?;
        let chunks = Arc::new(parking_lot::Mutex::new(Vec::<u64>::new()));
        let recorded_chunks = chunks.clone();
        let upload_domain = client
            .domain()
            .await
            .context("upload client has an active domain")?;
        let started = Instant::now();
        let outcome = timeout(
            OPERATION_TIMEOUT,
            client.upload_resource_from_directory_with_identity(
                "client_wire_resource",
                directory.path(),
                upload_domain,
                identity.clone(),
                move |bytes| recorded_chunks.lock().push(bytes),
            ),
        )
        .await
        .context("resource upload baseline timed out")??;
        observations.latency.record_duration(started.elapsed())?;
        ensure!(
            outcome.succeeded(),
            "resource upload failed: {}",
            outcome.message
        );
        let chunk_sizes = chunks.lock().clone();
        let archive_bytes = checked_sum(&chunk_sizes)?;
        observations.request_bytes.record(upload_request_bytes(
            domain,
            identity.as_str(),
            archive_bytes,
            &chunk_sizes,
        )?)?;
        observations
            .response_bytes
            .record_usize(upload_response_bytes(&outcome)?)?;
    }
    Ok(observations.finish("native gRPC client-streaming FlatBuffers frames"))
}

/// The bytes of the upload frames the client sent: its start and one chunk frame per chunk.
fn upload_request_bytes(
    domain: &str,
    identity: &str,
    archive_bytes: u64,
    chunks: &[u64],
) -> Result<u64> {
    let limits = SessionLimits::DEFAULT;
    let start = UploadStart {
        request_id: RequestId::new(std::num::NonZeroU64::MIN),
        domain: DomainName::parse(domain)
            .map_err(|report| anyhow!("the baseline domain is invalid: {report:?}"))?,
        resource: ResourceName::parse("client_wire_resource")
            .map_err(|report| anyhow!("the baseline resource name is invalid: {report:?}"))?,
        upload_identity: ResourceUploadIdentity::parse(identity)
            .map_err(|report| anyhow!("the upload identity is invalid: {report}"))?,
        total_bytes: std::num::NonZeroU64::new(archive_bytes)
            .ok_or_else(|| anyhow!("an uploaded archive is never empty"))?,
    };
    let start_bytes = start
        .encode(&limits)
        .map_err(|report| anyhow!("an upload start does not fit a frame: {report:?}"))?
        .len();
    let mut encoded = u64::try_from(start_bytes).context("frame size does not fit u64")?;
    for chunk in chunks {
        let chunk_len = usize::try_from(*chunk).context("upload chunk size does not fit usize")?;
        let chunk_bytes = UploadChunk::encode(&vec![0; chunk_len], &limits)
            .map_err(|report| anyhow!("an upload chunk does not fit a frame: {report:?}"))?
            .len();
        let chunk_bytes = u64::try_from(chunk_bytes).context("frame size does not fit u64")?;
        encoded = encoded
            .checked_add(chunk_bytes)
            .ok_or_else(|| anyhow!("upload frame byte count overflowed"))?;
    }
    Ok(encoded)
}

/// The bytes of the upload reply frame that answered the upload.
fn upload_response_bytes(outcome: &CommandOutcome) -> Result<usize> {
    let upload = outcome
        .resource_upload
        .as_ref()
        .ok_or_else(|| anyhow!("resource upload outcome omitted its identity and version"))?;
    let version = upload
        .version
        .ok_or_else(|| anyhow!("an installed upload reports its version"))?;
    let reply = UploadReply {
        request_id: Some(RequestId::new(std::num::NonZeroU64::MIN)),
        disposition: UploadDisposition::Installed {
            upload_identity: upload.identity.clone(),
            version,
            origin: upload.origin.unwrap_or(OutcomeOrigin::Executed),
        },
        message: outcome.message.clone(),
        diagnostics: outcome.diagnostics.clone(),
    };
    Ok(reply
        .encode(&SessionLimits::DEFAULT)
        .map_err(|report| anyhow!("an upload reply does not fit a frame: {report:?}"))?
        .len())
}

fn checked_sum(values: &[u64]) -> Result<u64> {
    let mut total = 0_u64;
    for value in values {
        total = total
            .checked_add(*value)
            .ok_or_else(|| anyhow!("baseline byte count overflowed"))?;
    }
    Ok(total)
}

async fn scrape_metrics(process: &ServerProcess) -> Result<String> {
    reqwest::get(process.observability_uri("/metrics"))
        .await
        .context("failed to scrape baseline metrics")?
        .error_for_status()
        .context("baseline metrics endpoint returned an error")?
        .text()
        .await
        .context("failed to read baseline metrics")
}

fn record_memory_phase(
    phase: &'static str,
    metrics: &str,
    phases: &mut Vec<MemoryPhase>,
) -> Result<()> {
    epoch::advance()
        .map_err(|error| anyhow!("failed to refresh client jemalloc statistics: {error}"))?;
    let client_allocated = stats::allocated::read()
        .map_err(|error| anyhow!("failed to read client allocated bytes: {error}"))?;
    let client_resident = stats::resident::read()
        .map_err(|error| anyhow!("failed to read client resident bytes: {error}"))?;
    phases.push(MemoryPhase {
        phase,
        client_allocated_bytes: u64::try_from(client_allocated)
            .context("client allocated bytes do not fit u64")?,
        client_resident_bytes: u64::try_from(client_resident)
            .context("client resident bytes do not fit u64")?,
        server_allocated_bytes: prometheus_scalar(metrics, "nervix_jemalloc_allocated_bytes")?,
        server_resident_bytes: prometheus_scalar(metrics, "nervix_jemalloc_resident_bytes")?,
    });
    Ok(())
}

fn record_process_phase(
    phase: &'static str,
    client_pid: u32,
    server_pid: u32,
    phases: &mut Vec<ProcessPhase>,
) -> Result<()> {
    phases.push(ProcessPhase {
        phase,
        client: read_process_counters(client_pid)?,
        server: read_process_counters(server_pid)?,
    });
    Ok(())
}

fn record_queue_occupancy(
    phase: &'static str,
    metrics: &str,
    phases: &mut Vec<QueueOccupancyPhase>,
) -> Result<()> {
    phases.push(QueueOccupancyPhase {
        phase,
        samples: prometheus_samples(metrics, "nervix_relay_buffer_len_")?,
    });
    Ok(())
}

fn prometheus_scalar(metrics: &str, name: &str) -> Result<f64> {
    let prefix = format!("{name} ");
    let line = metrics
        .lines()
        .find(|line| line.starts_with(&prefix))
        .ok_or_else(|| anyhow!("metrics scrape omitted '{name}'"))?;
    let (_, value) = line
        .split_once(' ')
        .ok_or_else(|| anyhow!("metric '{name}' has no value"))?;
    value
        .parse::<f64>()
        .with_context(|| format!("metric '{name}' has an invalid value"))
}

fn prometheus_samples(metrics: &str, prefix: &str) -> Result<Vec<PrometheusSample>> {
    let mut samples = Vec::new();
    for line in metrics.lines().filter(|line| line.starts_with(prefix)) {
        let (series, value) = line
            .rsplit_once(' ')
            .ok_or_else(|| anyhow!("Prometheus sample has no value: {line}"))?;
        samples.push(PrometheusSample {
            series: series.to_string(),
            value: value
                .parse::<f64>()
                .with_context(|| format!("Prometheus sample has invalid value: {line}"))?,
        });
    }
    Ok(samples)
}

fn read_process_counters(pid: u32) -> Result<ProcessCounters> {
    let stat_path = format!("/proc/{pid}/stat");
    let stat =
        fs::read_to_string(&stat_path).with_context(|| format!("failed to read {stat_path}"))?;
    let command_end = stat
        .rfind(')')
        .ok_or_else(|| anyhow!("{stat_path} has no command terminator"))?;
    let after_command = stat
        .get(command_end..)
        .ok_or_else(|| anyhow!("{stat_path} command terminator is outside the record"))?;
    let fields = after_command
        .strip_prefix(')')
        .ok_or_else(|| anyhow!("{stat_path} command terminator is malformed"))?
        .split_whitespace()
        .collect::<Vec<_>>();
    let user_cpu_ticks = parse_process_stat_field(&fields, 11, "user CPU")?;
    let system_cpu_ticks = parse_process_stat_field(&fields, 12, "system CPU")?;
    let status_path = format!("/proc/{pid}/status");
    let status = fs::read_to_string(&status_path)
        .with_context(|| format!("failed to read {status_path}"))?;
    Ok(ProcessCounters {
        user_cpu_ticks,
        system_cpu_ticks,
        resident_bytes: status_kib(&status, "VmRSS")?,
        peak_resident_bytes: status_kib(&status, "VmHWM")?,
    })
}

fn parse_process_stat_field(fields: &[&str], index: usize, name: &str) -> Result<u64> {
    fields
        .get(index)
        .ok_or_else(|| anyhow!("process stat omitted {name}"))?
        .parse::<u64>()
        .with_context(|| format!("process stat has invalid {name}"))
}

fn status_kib(status: &str, name: &str) -> Result<u64> {
    let prefix = format!("{name}:");
    let line = status
        .lines()
        .find(|line| line.starts_with(&prefix))
        .ok_or_else(|| anyhow!("process status omitted {name}"))?;
    let kib = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("process status {name} has no value"))?
        .parse::<u64>()
        .with_context(|| format!("process status {name} has an invalid value"))?;
    kib.checked_mul(1024)
        .ok_or_else(|| anyhow!("process status {name} byte count overflowed"))
}

fn capture_environment() -> Result<Environment> {
    let server_build_profile = std::env::var("NERVIX_CLIENT_WIRE_BASELINE_SERVER_PROFILE")
        .context("client-wire baseline server profile is not set")?;
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").context("failed to read /proc/cpuinfo")?;
    let cpu_model = cpuinfo
        .lines()
        .find_map(|line| line.strip_prefix("model name\t: "))
        .unwrap_or("unknown")
        .to_string();
    let meminfo = fs::read_to_string("/proc/meminfo").context("failed to read /proc/meminfo")?;
    let physical_memory_bytes = status_kib(&meminfo, "MemTotal")?;
    let logical_cpus = std::thread::available_parallelism()
        .context("failed to determine logical CPU count")?
        .get();
    let clock_ticks_per_second = command_output("getconf", &["CLK_TCK"])?
        .parse::<u64>()
        .context("getconf CLK_TCK returned an invalid value")?;
    Ok(Environment {
        nervix_version: env!("CARGO_PKG_VERSION"),
        rustc: command_output("rustc", &["-Vv"])?,
        kernel: command_output("uname", &["-srmo"])?,
        target_os: std::env::consts::OS,
        target_arch: std::env::consts::ARCH,
        server_build_profile,
        harness_build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        cpu_model,
        logical_cpus,
        physical_memory_bytes,
        clock_ticks_per_second,
    })
}

fn command_output(program: &str, arguments: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .with_context(|| format!("failed to run {program}"))?;
    ensure!(
        output.status.success(),
        "{program} failed with {}",
        output.status
    );
    String::from_utf8(output.stdout)
        .with_context(|| format!("{program} output is not UTF-8"))
        .map(|output| output.trim().to_string())
}

fn configured_limits() -> BTreeMap<&'static str, u64> {
    // These are the current public-edge queue sizes. Recording them beside each result prevents a
    // later protocol comparison from silently comparing different bounds.
    BTreeMap::from([
        ("client_request_queue_messages", 32),
        ("client_subscription_queue_messages", 128),
        ("client_server_event_queue_messages", 128),
        ("client_domain_event_queue_messages", 16),
        ("client_upload_queue_messages", 8),
        ("server_session_event_queue_messages", 256),
        ("web_console_request_queue_messages", 16),
        ("web_console_response_queue_messages", 16),
        ("ingestor_quiesce_buffer_bytes", 1024 * 1024),
        ("operation_timeout_milliseconds", 30_000),
    ])
}

fn environment_usize(name: &str, default: usize) -> Result<usize> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    value
        .to_str()
        .ok_or_else(|| anyhow!("{name} is not UTF-8"))?
        .parse::<usize>()
        .with_context(|| format!("{name} must be an unsigned integer"))
}
