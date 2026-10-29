//! What each node's control-class executor and reactor did across one timed probe.
//!
//! Layer: test harness.
//! - **Owns.** Reading a node's cumulative control-class queue, work and completed-job counters and
//!   its reactor scheduler delay from its metrics endpoint, and describing what changed between a
//!   reading taken as a probe's window opens and one taken after it ends.
//! - **Depends on.** Node names, each node's metrics URL, an HTTP client, primitive tasks and
//!   timers, and the execution and reactor series a node exports.
//! - **Must not know.** The probe's statements, its deadline, or how a node schedules its work.

use std::{fmt, io, time::Duration};

use futures_util::future::join_all;
use nervix_primitives::{task::AbortOnDropHandle, time::timeout};

const CONTROL_QUEUED: &str = "nervix_execution_job_queue_seconds_total{class=\"control_cpu\"}";
const CONTROL_WORKED: &str = "nervix_execution_job_work_seconds_total{class=\"control_cpu\"}";
const CONTROL_COMPLETED: &str = "nervix_execution_jobs_completed_total{class=\"control_cpu\"}";
const SCHEDULER_DELAY: &str = "nervix_node_scheduler_delay_seconds_total";
/// How long one reading may take. A node too busy to answer within it is reported as such rather
/// than holding the probe's report.
const READING_BUDGET: Duration = Duration::from_secs(2);

/// Where one node exposes the counters a probe reads.
#[derive(Clone, Debug)]
pub(crate) struct NodeExecutionEndpoint {
    node: String,
    metrics_url: String,
}

impl NodeExecutionEndpoint {
    pub(crate) fn new(node: String, metrics_url: String) -> Self {
        Self { node, metrics_url }
    }

    async fn read(&self, client: &reqwest::Client) -> io::Result<NodeExecutionReading> {
        let response = timeout(READING_BUDGET, async {
            let response = client
                .get(&self.metrics_url)
                .send()
                .await
                .map_err(io::Error::other)?;
            let status = response.status();
            let body = response.text().await.map_err(io::Error::other)?;
            if !status.is_success() {
                return Err(io::Error::other(format!("metrics answered {status}")));
            }
            Ok(body)
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no metrics within {READING_BUDGET:?}"),
            )
        })??;
        NodeExecutionReading::parse(&response)
    }
}

/// One node's cumulative control-class and reactor counters at one instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NodeExecutionReading {
    control_queued: Duration,
    control_worked: Duration,
    control_completed: u64,
    scheduler_delay: Duration,
}

impl NodeExecutionReading {
    /// Read the four counters from a node's metrics exposition.
    pub(crate) fn parse(metrics: &str) -> io::Result<Self> {
        Ok(Self {
            control_queued: seconds(metrics, CONTROL_QUEUED)?,
            control_worked: seconds(metrics, CONTROL_WORKED)?,
            control_completed: count(metrics, CONTROL_COMPLETED)?,
            scheduler_delay: seconds(metrics, SCHEDULER_DELAY)?,
        })
    }

    /// What changed since `earlier`, or `None` when a counter went back, which happens only when the
    /// node restarted between the two readings and the difference would mean nothing.
    pub(crate) fn since(&self, earlier: &Self) -> Option<NodeExecutionWindow> {
        Some(NodeExecutionWindow {
            control_queued: self.control_queued.checked_sub(earlier.control_queued)?,
            control_worked: self.control_worked.checked_sub(earlier.control_worked)?,
            control_completed: self
                .control_completed
                .checked_sub(earlier.control_completed)?,
            scheduler_delay: self.scheduler_delay.checked_sub(earlier.scheduler_delay)?,
        })
    }
}

/// What one node's control class and reactor did between two readings.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NodeExecutionWindow {
    control_queued: Duration,
    control_worked: Duration,
    control_completed: u64,
    scheduler_delay: Duration,
}

impl fmt::Display for NodeExecutionWindow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "control_jobs={} control_queued={:?} control_worked={:?}",
            self.control_completed, self.control_queued, self.control_worked
        )?;
        // The queue counter adds each job's wait when it takes its worker, so dividing by the jobs
        // that finished in the same window is the mean wait those jobs paid.
        if let Ok(completed) = u32::try_from(self.control_completed)
            && let Some(per_job) = self.control_queued.checked_div(completed)
        {
            write!(formatter, " queued_per_job={per_job:?}")?;
        }
        write!(formatter, " scheduler_delay={:?}", self.scheduler_delay)
    }
}

/// One node's reading before the probe's window, kept until the probe describes the window.
#[derive(Debug)]
struct NodeExecutionStart {
    endpoint: NodeExecutionEndpoint,
    reading: io::Result<NodeExecutionReading>,
}

/// What the reading task hands to the probe's description: the client it read with and each
/// node's reading as the probe began.
#[derive(Debug)]
struct ProbeExecutionStart {
    client: reqwest::Client,
    nodes: Vec<NodeExecutionStart>,
}

/// Readings of every node of a cluster taken around one probe window.
#[derive(Debug)]
pub(crate) struct ProbeExecution {
    start: AbortOnDropHandle<ProbeExecutionStart>,
}

impl ProbeExecution {
    /// Start reading every node as the probe's window opens. The readings run beside the probe, so
    /// they never delay its first dispatch, and dropping the probe cancels them.
    pub(crate) fn begin(endpoints: Vec<NodeExecutionEndpoint>) -> Self {
        let start = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
            // Building a client reads and parses the platform's trust roots: tens of milliseconds
            // of blocking work that an oversubscribed host stretches to seconds. The reading task
            // builds it, so the probe's window does not include it.
            let client = reqwest::Client::new();
            let readings = join_all(endpoints.iter().map(|endpoint| endpoint.read(&client))).await;
            let nodes = endpoints
                .into_iter()
                .zip(readings)
                .map(|(endpoint, reading)| NodeExecutionStart { endpoint, reading })
                .collect::<Vec<_>>();
            ProbeExecutionStart { client, nodes }
        }));
        Self { start }
    }

    /// Read every node again and describe, one line per node, what it did since `begin`.
    pub(crate) async fn describe(self) -> Vec<String> {
        let start = match self.start.await {
            Ok(start) => start,
            Err(failure) => {
                return vec![format!(
                    "the readings taken as the probe began ended without a result: {failure}"
                )];
            }
        };
        let readings = join_all(
            start
                .nodes
                .iter()
                .map(|node| node.endpoint.read(&start.client)),
        )
        .await;
        let mut lines = Vec::new();
        for (node, finish) in start.nodes.iter().zip(readings) {
            lines.push(node.describe(finish));
        }
        lines
    }
}

impl NodeExecutionStart {
    fn describe(&self, finish: io::Result<NodeExecutionReading>) -> String {
        let node = &self.endpoint.node;
        let start = match &self.reading {
            Ok(reading) => reading,
            Err(error) => return format!("node={node} reading before the probe failed: {error}"),
        };
        let finish = match finish {
            Ok(reading) => reading,
            Err(error) => return format!("node={node} reading after the probe failed: {error}"),
        };
        match finish.since(start) {
            Some(window) => format!("node={node} {window}"),
            None => format!("node={node} counters went back between the readings"),
        }
    }
}

/// The value of the one sample of `series`, its first field after the series name.
fn sample<'a>(metrics: &'a str, series: &str) -> io::Result<&'a str> {
    for line in metrics.lines() {
        let Some(fields) = line.strip_prefix(series) else {
            continue;
        };
        let Some(fields) = fields.strip_prefix(' ') else {
            continue;
        };
        if let Some(value) = fields.split_whitespace().next() {
            return Ok(value);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("metrics have no sample of {series}"),
    ))
}

fn seconds(metrics: &str, series: &str) -> io::Result<Duration> {
    let value = sample(metrics, series)?;
    let seconds = value.parse::<f64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{series} is not a number: {value}: {error}"),
        )
    })?;
    Duration::try_from_secs_f64(seconds).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{series} is not a duration: {value}: {error}"),
        )
    })
}

fn count(metrics: &str, series: &str) -> io::Result<u64> {
    let value = sample(metrics, series)?;
    value.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{series} is not a count: {value}: {error}"),
        )
    })
}
