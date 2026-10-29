//! Regressions for the control-class and reactor readings a timed probe reports per node.
//!
//! Layer: test harness.
//! - **Owns.** Scripted metrics expositions, a stand-in metrics endpoint and the expected
//!   per-node descriptions.
//! - **Depends on.** The node execution reading owner, primitive sockets and Tokio's I/O traits.
//! - **Must not know.** Scenario state or how a node schedules its work.

use std::io;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{net::TcpListener, task::AbortOnDropHandle};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::node_execution::{NodeExecutionEndpoint, NodeExecutionReading, ProbeExecution};

/// An exposition with the counters a reading takes, beside series that share their prefixes and a
/// labelled series that shares the reactor delay's whole name.
fn exposition(queued: &str, worked: &str, completed: u64, scheduler_delay: &str) -> String {
    let lines = [
        "# HELP nervix_execution_job_queue_seconds_total Time admitted jobs spent waiting."
            .to_string(),
        "# TYPE nervix_execution_job_queue_seconds_total counter".to_string(),
        "nervix_execution_job_queue_seconds_total{class=\"consensus_storage\"} 9.5".to_string(),
        format!("nervix_execution_job_queue_seconds_total{{class=\"control_cpu\"}} {queued}"),
        format!("nervix_execution_job_work_seconds_total{{class=\"control_cpu\"}} {worked}"),
        "nervix_execution_jobs_completed_total{class=\"consensus_storage\"} 77".to_string(),
        format!("nervix_execution_jobs_completed_total{{class=\"control_cpu\"}} {completed}"),
        "nervix_node_scheduler_delay_peak_seconds 4.25".to_string(),
        "nervix_node_scheduler_delay_seconds_total{worker=\"0\"} 9.75".to_string(),
        format!("nervix_node_scheduler_delay_seconds_total {scheduler_delay}"),
    ];
    lines.join("\n")
}

fn reading(
    queued: &str,
    worked: &str,
    completed: u64,
    scheduler_delay: &str,
) -> NodeExecutionReading {
    NodeExecutionReading::parse(&exposition(queued, worked, completed, scheduler_delay))
        .assured("the scripted exposition holds every series a reading takes")
}

#[test]
fn a_window_reports_the_jobs_their_queue_and_work_and_the_reactor_delay() {
    let before = reading("1.5", "0.25", 300, "0.5");
    let after = reading("31.5", "0.75", 1300, "1.75");
    let window = after
        .since(&before)
        .assured("every counter advanced between the two readings");
    assert_eq!(
        window.to_string(),
        "control_jobs=1000 control_queued=30s control_worked=500ms queued_per_job=30ms \
         scheduler_delay=1.25s"
    );
}

#[test]
fn a_window_without_completed_jobs_reports_no_mean_wait() {
    let before = reading("1.5", "0.25", 300, "0.5");
    let after = reading("2.5", "0.25", 300, "0.5");
    let window = after
        .since(&before)
        .assured("no counter went back between the two readings");
    assert_eq!(
        window.to_string(),
        "control_jobs=0 control_queued=1s control_worked=0ns scheduler_delay=0ns"
    );
}

#[test]
fn a_counter_that_went_back_means_the_node_restarted_and_yields_no_window() {
    let before = reading("1.5", "0.25", 300, "0.5");
    let restarted = reading("0.1", "0.01", 4, "0.02");
    assert_eq!(restarted.since(&before), None);
}

#[test]
fn a_missing_series_is_named() {
    let without_reactor = exposition("1", "1", 1, "1")
        .lines()
        .filter(|line| !line.starts_with("nervix_node_scheduler_delay_seconds_total"))
        .collect::<Vec<_>>()
        .join("\n");
    let error = NodeExecutionReading::parse(&without_reactor)
        .expect_err("a reading needs the reactor delay series");
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert_eq!(
        error.to_string(),
        "metrics have no sample of nervix_node_scheduler_delay_seconds_total"
    );
}

#[test]
fn a_sample_that_is_not_a_count_is_refused() {
    let error = NodeExecutionReading::parse(&exposition("1", "1", 1, "1").replace(
        "nervix_execution_jobs_completed_total{class=\"control_cpu\"} 1",
        "nervix_execution_jobs_completed_total{class=\"control_cpu\"} 1.5",
    ))
    .expect_err("a completed-job count is a whole number");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn seconds_that_are_not_a_number_or_not_a_duration_are_refused() {
    let not_a_number = NodeExecutionReading::parse(&exposition("soon", "1", 1, "1"))
        .expect_err("queued time is a number of seconds");
    assert_eq!(not_a_number.kind(), io::ErrorKind::InvalidData);
    assert!(
        not_a_number.to_string().starts_with(
            "nervix_execution_job_queue_seconds_total{class=\"control_cpu\"} is not a number: \
             soon: "
        ),
        "unexpected refusal of a queued time that is not a number: {not_a_number}"
    );

    let negative = NodeExecutionReading::parse(&exposition("1", "1", 1, "-0.5"))
        .expect_err("a reactor delay is never negative");
    assert_eq!(negative.kind(), io::ErrorKind::InvalidData);
    assert!(
        negative
            .to_string()
            .starts_with("nervix_node_scheduler_delay_seconds_total is not a duration: -0.5: "),
        "unexpected refusal of a negative reactor delay: {negative}"
    );
}

/// One answer of the stand-in metrics endpoint: its HTTP status and body.
struct ScriptedAnswer {
    status: &'static str,
    body: String,
}

impl ScriptedAnswer {
    fn metrics(body: String) -> Self {
        Self {
            status: "200 OK",
            body,
        }
    }

    fn unavailable() -> Self {
        Self {
            status: "503 Service Unavailable",
            body: "draining".to_string(),
        }
    }
}

/// A stand-in metrics endpoint that gives each of its scripted answers to one request, in order.
struct ScriptedMetricsEndpoint {
    url: String,
    /// Ends the stand-in with the test that owns it.
    _server: AbortOnDropHandle<()>,
}

impl ScriptedMetricsEndpoint {
    async fn serve(answers: Vec<ScriptedAnswer>) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
            for answer in answers {
                nervix_primitives::task::consume_budget().await;
                let (mut stream, _) = listener
                    .accept()
                    .await
                    .assured("the test's client connects once per scripted answer");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    nervix_primitives::task::consume_budget().await;
                    let read = stream
                        .read(&mut buffer)
                        .await
                        .assured("the client writes its whole request");
                    assert_ne!(read, 0, "the client closed before finishing its request");
                    request.extend_from_slice(&buffer[..read]);
                }
                let response = format!(
                    "HTTP/1.1 {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    answer.status,
                    answer.body.len(),
                    answer.body
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .assured("the client reads the whole response");
            }
        }));
        Ok(Self {
            url: format!("http://{address}/metrics"),
            _server: server,
        })
    }
}

#[nervix_primitives::test]
async fn a_probe_describes_each_node_from_its_readings_around_the_window() {
    let answering = ScriptedMetricsEndpoint::serve(vec![
        ScriptedAnswer::metrics(exposition("1.5", "0.25", 300, "0.5")),
        ScriptedAnswer::metrics(exposition("11.5", "0.5", 800, "0.75")),
    ])
    .await
    .assured("a loopback listener binds");
    let refused = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("a loopback listener binds");
    let refused_url = format!(
        "http://{}/metrics",
        refused
            .local_addr()
            .assured("a bound listener has an address")
    );
    drop(refused);

    let execution = ProbeExecution::begin(vec![
        NodeExecutionEndpoint::new("node-1".to_string(), answering.url.clone()),
        NodeExecutionEndpoint::new("node-2".to_string(), refused_url),
    ]);
    let lines = execution.describe().await;

    assert_eq!(lines.len(), 2);
    assert_eq!(
        lines[0],
        "node=node-1 control_jobs=500 control_queued=10s control_worked=250ms queued_per_job=20ms \
         scheduler_delay=250ms"
    );
    assert!(
        lines[1].starts_with("node=node-2 reading before the probe failed: "),
        "unexpected description of an unreachable node: {}",
        lines[1]
    );
}

#[nervix_primitives::test]
async fn a_probe_names_a_node_unreadable_after_the_window_and_one_whose_counters_went_back() {
    let unavailable_after = ScriptedMetricsEndpoint::serve(vec![
        ScriptedAnswer::metrics(exposition("1.5", "0.25", 300, "0.5")),
        ScriptedAnswer::unavailable(),
    ])
    .await
    .assured("a loopback listener binds");
    let restarted = ScriptedMetricsEndpoint::serve(vec![
        ScriptedAnswer::metrics(exposition("1.5", "0.25", 300, "0.5")),
        ScriptedAnswer::metrics(exposition("0.1", "0.01", 4, "0.02")),
    ])
    .await
    .assured("a loopback listener binds");

    let execution = ProbeExecution::begin(vec![
        NodeExecutionEndpoint::new("node-1".to_string(), unavailable_after.url.clone()),
        NodeExecutionEndpoint::new("node-2".to_string(), restarted.url.clone()),
    ]);
    let lines = execution.describe().await;

    assert_eq!(
        lines,
        [
            "node=node-1 reading after the probe failed: metrics answered 503 Service Unavailable",
            "node=node-2 counters went back between the readings",
        ]
    );
}

/// The listener completes connections but never answers them, so only the reading budget ends a
/// reading. The paused clock reaches that budget as soon as the runtime idles.
#[nervix_primitives::test(start_paused = true)]
async fn a_node_that_does_not_answer_within_the_reading_budget_is_reported() {
    let silent = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("a loopback listener binds");
    let silent_url = format!(
        "http://{}/metrics",
        silent
            .local_addr()
            .assured("a bound listener has an address")
    );

    let execution = ProbeExecution::begin(vec![NodeExecutionEndpoint::new(
        "node-1".to_string(),
        silent_url,
    )]);
    let lines = execution.describe().await;

    assert_eq!(
        lines,
        ["node=node-1 reading before the probe failed: no metrics within 2s"]
    );
    drop(silent);
}
