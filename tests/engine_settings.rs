//! Public witnesses for local VM admission and CLI connection capabilities.
//!
//! Outside the layer order: a harness module.
//! - **Owns.** Observing execution admission and CLI connection options at their public edges.
//! - **Depends on.** The scenario cluster, its metrics endpoint and the CLI terminal fixture.
//! - **Must not know.** Worker pool internals, compiler state or client exchange internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "observes engine settings through public interfaces")
)]

use std::time::Duration;

use cucumber::{then, when};
use meticulous::ResultExt as _;
use nervix_execution::{CpuClass, WorkerClassName};
use nervix_primitives::time::{sleep, timeout};

use crate::{
    CLI_TERMINAL_DISPLAY_TIMEOUT, CliTerminal, ScenarioWorld, TEST_AUTH_PASSWORD,
    TEST_AUTH_USERNAME, expand_placeholders, scenario_cli_binary,
};

#[then(expr = "the data execution count on node {string} is saved")]
async fn save_data_executions(world: &mut ScenarioWorld, node: String) {
    let value = world
        .cluster()
        .read_observability_metric(
            &node,
            "nervix_execution_jobs_completed_total",
            &[format!(
                "class=\"{}\"",
                WorkerClassName::Cpu(CpuClass::Data).as_str()
            )],
        )
        .await
        .assured("the running node exposes data-worker completions");
    world
        .placeholders
        .insert(format!("data-executions-{node}"), value.to_string());
}

#[then(expr = "the data execution count on node {string} eventually increases")]
async fn data_executions_increase(world: &mut ScenarioWorld, node: String) {
    let before: f64 = world.placeholders[&format!("data-executions-{node}")]
        .parse()
        .assured("the preceding step saved a numeric counter");
    timeout(Duration::from_secs(30), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let value = world
                .cluster()
                .read_observability_metric(
                    &node,
                    "nervix_execution_jobs_completed_total",
                    &[format!(
                        "class=\"{}\"",
                        WorkerClassName::Cpu(CpuClass::Data).as_str()
                    )],
                )
                .await
                .assured("the running node exposes data-worker completions");
            if value > before {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .assured("the transformed small batch completes on the data workers");
}

#[when(expr = "the CLI REPL starts on node {string} with explicit connection capabilities")]
async fn repl_with_capabilities(world: &mut ScenarioWorld, node: String) {
    let node = expand_placeholders(world, &node);
    let uri = world
        .cluster()
        .grpc_uri(&node)
        .assured("the scenario names a node");
    let terminal = CliTerminal::start(
        &scenario_cli_binary(),
        &[
            "--server",
            &uri,
            "--domain",
            &world.domain,
            "--username",
            TEST_AUTH_USERNAME,
            "--password",
            TEST_AUTH_PASSWORD,
            "--connect-timeout",
            "20s",
            "--request-timeout",
            "30s",
            "--retry-timeout",
            "45s",
            "--backup-wait-timeout",
            "60s",
            "--retry-backoff",
            "20ms",
            "--retry-max-backoff",
            "200ms",
            "--subscription-record-capacity",
            "48",
            "--subscription-retained-frames",
            "2",
            "--subscriptions-retained-in-full",
            "3",
            "--upload-frame-capacity",
            "4",
            "--restore-frame-capacity",
            "5",
        ],
    )
    .assured("the CLI starts on its terminal");
    if let Err(error) = terminal
        .wait_for_display("nervix-cli connected to", CLI_TERMINAL_DISPLAY_TIMEOUT)
        .await
    {
        panic!(
            "CLI capabilities did not connect: {error}; transcript: {}",
            terminal.transcript()
        );
    }
    world.cli_terminal = Some(terminal);
}
