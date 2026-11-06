//! Paired command and connection timing over plaintext and TLS sessions of two release servers.
//!
//! Outside the layer order: a benchmark harness. Product code must not name it.
//!
//! - **Owns.** Starting a plaintext and a TLS server from one release executable with otherwise
//!   identical options, and raw paired samples of the same command and of fresh authenticated
//!   connections through both public client transports, with the order each pair ran in and the
//!   CPU the client and each server spent on each transport, including their procfs counters.
//! - **Depends on.** The native client, the server process fixture and Linux procfs.
//! - **Must not know.** Server scheduling, execution plans or transport internals.

use std::{fs, future::Future, path::PathBuf, time::Duration};

use anyhow::{Context as _, Result, anyhow, ensure};
use nervix_client_core::{Client, ClientError, CommandOutcome};
use nervix_primitives::time::{Instant, timeout};
use serde::Serialize;

use super::{
    cluster::{client_connect_options, client_domain},
    server_process::{ServerProcess, ServerProcessLaunch, ServerProcessOption},
};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ProcessCounters {
    pub(crate) user_cpu_ticks: u64,
    pub(crate) system_cpu_ticks: u64,
    resident_bytes: u64,
    peak_resident_bytes: u64,
}

pub(crate) fn read_process_counters(pid: u32) -> Result<ProcessCounters> {
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

pub(crate) fn status_kib(status: &str, name: &str) -> Result<u64> {
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

/// Pairs of the same command, one on each transport, run back to back so that load the host adds
/// during a pair reaches both of its commands.
const COMMAND_PAIRS: usize = 400;
/// Pairs of fresh connections. Each connection also opens and authenticates its session.
const CONNECTION_PAIRS: usize = 50;
/// Untimed commands on each transport before the pairs, so first-use costs stay out of them.
const WARMUP_COMMANDS: usize = 20;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const COMMAND: &str = "DESCRIBE DOMAIN;";
const DOMAIN: &str = "wire_tls";
const REPORT_FILE: &str = "transport-pairs.json";

/// A plaintext and a TLS server started from one release executable with otherwise identical
/// options, until the scenario ends and drops them.
#[derive(Debug)]
pub(crate) struct TransportCostServers {
    plaintext: ServerProcess,
    tls: ServerProcess,
}

impl TransportCostServers {
    pub(crate) async fn start(executable: PathBuf) -> Result<Self> {
        let plaintext =
            start_server(ServerProcessLaunch::Executable(executable.clone()), &[]).await?;
        let tls = start_server(
            ServerProcessLaunch::Executable(executable),
            &[ServerProcessOption::GrpcTls],
        )
        .await?;
        Ok(Self { plaintext, tls })
    }
}

async fn start_server(
    launch: ServerProcessLaunch,
    options: &[ServerProcessOption],
) -> Result<ServerProcess> {
    let mut process = ServerProcess::start(launch, options)
        .await
        .context("failed to launch a transport cost server")?;
    process
        .wait_until_ready()
        .await
        .context("a transport cost server did not accept commands")?;
    process
        .run_commands(DOMAIN, &format!("CREATE UNPACED DOMAIN {DOMAIN};"))
        .await
        .context("failed to create the transport cost domain")?;
    Ok(process)
}

/// Which transport runs first in a pair. The pairs alternate in the order plaintext, TLS, TLS,
/// plaintext, so a cost that drifts over the run, or the cost of going second, reaches both
/// transports equally.
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum PairOrder {
    PlaintextFirst,
    TlsFirst,
}

impl PairOrder {
    fn of_pair(index: usize) -> Self {
        match index % 4 {
            0 | 3 => Self::PlaintextFirst,
            _ => Self::TlsFirst,
        }
    }
}

#[derive(Serialize)]
struct Distribution {
    p50: i64,
    p95: i64,
    p99: i64,
}

impl Distribution {
    fn of(samples: &[i64]) -> Result<Self> {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Ok(Self {
            p50: quantile(&sorted, 50)?,
            p95: quantile(&sorted, 95)?,
            p99: quantile(&sorted, 99)?,
        })
    }
}

/// The nearest-rank quantile of `sorted`.
fn quantile(sorted: &[i64], percent: usize) -> Result<i64> {
    let rank = sorted
        .len()
        .checked_mul(percent)
        .context("a sample count times a percentage does not fit usize")?
        .div_ceil(100);
    let index = rank
        .checked_sub(1)
        .context("a quantile of no samples has no rank")?;
    sorted
        .get(index)
        .copied()
        .context("a nearest rank lies inside its samples")
}

/// The clock ticks the client process and the answering server spent on one transport's side of
/// a series. The kernel charges a tick to whichever process holds a CPU when the tick fires, so one
/// operation's share is coarse while a series' total over its operations is not.
#[derive(Default, Serialize)]
struct CpuTicks {
    client: u64,
    server: u64,
}

impl CpuTicks {
    fn add(&mut self, cost: &OperationCost) -> Result<()> {
        self.client = self
            .client
            .checked_add(cost.client_ticks)
            .context("a series' client CPU ticks do not fit u64")?;
        self.server = self
            .server
            .checked_add(cost.server_ticks)
            .context("a series' server CPU ticks do not fit u64")?;
        Ok(())
    }
}

/// One paired series: the same operation on each transport, microseconds, in pair order.
#[derive(Serialize)]
struct PairedSeries {
    plaintext_microseconds: Vec<i64>,
    tls_microseconds: Vec<i64>,
    order: Vec<PairOrder>,
    plaintext: Distribution,
    tls: Distribution,
    /// TLS minus plaintext within each pair. Its median does not drift with host load, which
    /// reaches both samples of a pair.
    tls_minus_plaintext: Distribution,
    pairs_where_tls_was_slower: usize,
    plaintext_cpu_ticks: CpuTicks,
    tls_cpu_ticks: CpuTicks,
}

/// The samples of a paired series while it is being recorded.
struct PairedSamples {
    plaintext_microseconds: Vec<i64>,
    tls_microseconds: Vec<i64>,
    order: Vec<PairOrder>,
    plaintext_cpu_ticks: CpuTicks,
    tls_cpu_ticks: CpuTicks,
}

impl PairedSamples {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            plaintext_microseconds: Vec::with_capacity(capacity),
            tls_microseconds: Vec::with_capacity(capacity),
            order: Vec::with_capacity(capacity),
            plaintext_cpu_ticks: CpuTicks::default(),
            tls_cpu_ticks: CpuTicks::default(),
        }
    }

    fn record(
        &mut self,
        order: PairOrder,
        plaintext: &OperationCost,
        tls: &OperationCost,
    ) -> Result<()> {
        self.order.push(order);
        self.plaintext_microseconds.push(
            i64::try_from(plaintext.latency.as_micros())
                .context("a sample does not fit i64 microseconds")?,
        );
        self.tls_microseconds.push(
            i64::try_from(tls.latency.as_micros())
                .context("a sample does not fit i64 microseconds")?,
        );
        self.plaintext_cpu_ticks.add(plaintext)?;
        self.tls_cpu_ticks.add(tls)?;
        Ok(())
    }

    fn finish(self) -> Result<PairedSeries> {
        let mut differences = Vec::with_capacity(self.order.len());
        for (plaintext, tls) in self
            .plaintext_microseconds
            .iter()
            .zip(&self.tls_microseconds)
        {
            let difference = tls
                .checked_sub(*plaintext)
                .context("a paired difference does not fit i64")?;
            differences.push(difference);
        }
        let pairs_where_tls_was_slower = differences
            .iter()
            .filter(|difference| **difference > 0)
            .count();
        Ok(PairedSeries {
            plaintext: Distribution::of(&self.plaintext_microseconds)?,
            tls: Distribution::of(&self.tls_microseconds)?,
            tls_minus_plaintext: Distribution::of(&differences)?,
            pairs_where_tls_was_slower,
            plaintext_microseconds: self.plaintext_microseconds,
            tls_microseconds: self.tls_microseconds,
            order: self.order,
            plaintext_cpu_ticks: self.plaintext_cpu_ticks,
            tls_cpu_ticks: self.tls_cpu_ticks,
        })
    }
}

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    command: &'static str,
    clock_ticks_per_second: u64,
    load_average_before: String,
    load_average_after: String,
    /// Each fresh connection's connect, which includes opening and authenticating its session.
    connections: PairedSeries,
    /// `Client::execute`: preparing the command and its round trip.
    commands: PairedSeries,
    /// `Client::execute_prepared` of a command prepared outside the timing: the round trip alone.
    prepared_commands: PairedSeries,
}

/// One transport's server and the client connected to it.
struct Side<'a> {
    server: &'a ServerProcess,
    server_pid: u32,
    client: Client,
}

/// What one operation cost: its latency, and the clock ticks the client process and the server
/// spent while it ran.
struct OperationCost {
    latency: Duration,
    client_ticks: u64,
    server_ticks: u64,
}

fn process_ticks(pid: u32) -> Result<u64> {
    let counters = read_process_counters(pid)?;
    counters
        .user_cpu_ticks
        .checked_add(counters.system_cpu_ticks)
        .context("a process's CPU ticks do not fit u64")
}

/// Runs `operation`, timing it and counting the CPU the client process and the server spend on it.
/// The client's own counter is read last before the operation and first after it, so reading the
/// counters stays outside its share.
async fn costed<T>(
    server_pid: u32,
    operation: impl Future<Output = Result<T>>,
) -> Result<(T, OperationCost)> {
    let client_pid = std::process::id();
    let server_before = process_ticks(server_pid)?;
    let client_before = process_ticks(client_pid)?;
    let started = Instant::now();
    let output = operation.await?;
    let latency = started.elapsed();
    let client_after = process_ticks(client_pid)?;
    let server_after = process_ticks(server_pid)?;
    let cost = OperationCost {
        latency,
        client_ticks: client_after
            .checked_sub(client_before)
            .context("the client's CPU ticks decreased")?,
        server_ticks: server_after
            .checked_sub(server_before)
            .context("a server's CPU ticks decreased")?,
    };
    Ok((output, cost))
}

async fn connect(server: &ServerProcess) -> Result<Client> {
    let uri = server.grpc_uri();
    let options = client_connect_options(&uri)?;
    timeout(
        OPERATION_TIMEOUT,
        Client::connect_with_options(&uri, client_domain(DOMAIN), options),
    )
    .await
    .context("a transport cost client connection timed out")?
    .map_err(|report| anyhow!("a transport cost client did not connect: {report:#}"))
}

async fn succeeded(
    command: impl Future<Output = error_stack::Result<CommandOutcome, ClientError>>,
) -> Result<()> {
    let outcome = timeout(OPERATION_TIMEOUT, command)
        .await
        .context("a transport cost command timed out")?
        .map_err(|report| anyhow!("a transport cost command did not run: {report:#}"))?;
    ensure!(outcome.succeeded(), "a transport cost command failed");
    Ok(())
}

impl Side<'_> {
    async fn costed_connect(&self) -> Result<OperationCost> {
        let (client, cost) = costed(self.server_pid, connect(self.server)).await?;
        drop(client);
        Ok(cost)
    }

    /// `Client::execute`: preparing the command and its round trip.
    async fn costed_execute(&self) -> Result<OperationCost> {
        let ((), cost) = costed(self.server_pid, succeeded(self.client.execute(COMMAND))).await?;
        Ok(cost)
    }

    /// The round trip of a command prepared before the timing starts.
    async fn costed_execute_prepared(&self) -> Result<OperationCost> {
        let prepared = self.client.prepare_execution(COMMAND).await;
        let ((), cost) = costed(
            self.server_pid,
            succeeded(self.client.execute_prepared(&prepared)),
        )
        .await?;
        Ok(cost)
    }
}

fn load_average() -> Result<String> {
    let text = fs::read_to_string("/proc/loadavg").context("failed to read /proc/loadavg")?;
    Ok(text.trim().to_owned())
}

/// The kernel's clock ticks per second, which its CPU counters count in.
fn clock_ticks_per_second() -> Result<u64> {
    let output = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .context("failed to run getconf")?;
    ensure!(output.status.success(), "getconf CLK_TCK failed");
    let text = String::from_utf8(output.stdout).context("getconf printed no UTF-8")?;
    text.trim()
        .parse()
        .context("getconf printed no clock tick rate")
}

pub(crate) async fn capture(servers: &TransportCostServers) -> Result<PathBuf> {
    let load_average_before = load_average()?;
    let plaintext = Side {
        server: &servers.plaintext,
        server_pid: servers.plaintext.process_id()?,
        client: connect(&servers.plaintext).await?,
    };
    let tls = Side {
        server: &servers.tls,
        server_pid: servers.tls.process_id()?,
        client: connect(&servers.tls).await?,
    };

    let mut connections = PairedSamples::with_capacity(CONNECTION_PAIRS);
    for index in 0..CONNECTION_PAIRS {
        nervix_primitives::task::consume_budget().await;
        let order = PairOrder::of_pair(index);
        match order {
            PairOrder::PlaintextFirst => {
                let plaintext_cost = plaintext.costed_connect().await?;
                let tls_cost = tls.costed_connect().await?;
                connections.record(order, &plaintext_cost, &tls_cost)?;
            }
            PairOrder::TlsFirst => {
                let tls_cost = tls.costed_connect().await?;
                let plaintext_cost = plaintext.costed_connect().await?;
                connections.record(order, &plaintext_cost, &tls_cost)?;
            }
        }
    }

    for _ in 0..WARMUP_COMMANDS {
        nervix_primitives::task::consume_budget().await;
        plaintext.costed_execute().await?;
        tls.costed_execute().await?;
    }

    let mut commands = PairedSamples::with_capacity(COMMAND_PAIRS);
    let mut prepared_commands = PairedSamples::with_capacity(COMMAND_PAIRS);
    for index in 0..COMMAND_PAIRS {
        nervix_primitives::task::consume_budget().await;
        let order = PairOrder::of_pair(index);
        match order {
            PairOrder::PlaintextFirst => {
                let plaintext_cost = plaintext.costed_execute().await?;
                let tls_cost = tls.costed_execute().await?;
                commands.record(order, &plaintext_cost, &tls_cost)?;
                let plaintext_cost = plaintext.costed_execute_prepared().await?;
                let tls_cost = tls.costed_execute_prepared().await?;
                prepared_commands.record(order, &plaintext_cost, &tls_cost)?;
            }
            PairOrder::TlsFirst => {
                let tls_cost = tls.costed_execute().await?;
                let plaintext_cost = plaintext.costed_execute().await?;
                commands.record(order, &plaintext_cost, &tls_cost)?;
                let tls_cost = tls.costed_execute_prepared().await?;
                let plaintext_cost = plaintext.costed_execute_prepared().await?;
                prepared_commands.record(order, &plaintext_cost, &tls_cost)?;
            }
        }
    }

    let report = Report {
        schema_version: 1,
        command: COMMAND,
        clock_ticks_per_second: clock_ticks_per_second()?,
        load_average_before,
        load_average_after: load_average()?,
        connections: connections.finish()?,
        commands: commands.finish()?,
        prepared_commands: prepared_commands.finish()?,
    };
    let output_dir = std::env::var_os("NERVIX_CLIENT_WIRE_TLS_OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/client-wire-tls-cost"));
    fs::create_dir_all(&output_dir).context("failed to create the transport cost directory")?;
    let output = output_dir.join(REPORT_FILE);
    fs::write(&output, serde_json::to_vec_pretty(&report)?)
        .context("failed to write the transport cost report")?;
    Ok(output)
}
