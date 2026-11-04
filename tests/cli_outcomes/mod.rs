//! Steps that run `nervix-cli` the way an operator or a script does, and read how each run ends: its
//! exit status, what it printed, and what Ctrl-C does to a command it is waiting for.
//!
//! Layer: test harness.
//! - **Owns.** The CLI processes a scenario starts in the background and interrupts, the exit
//!   status and output of each CLI run, and the execution references and upload identities a run
//!   prints for a scenario to repeat.
//! - **Depends on.** The scenario cluster, the CLI binary `tests-deps` builds, and the REPL
//!   terminal fixture.
//! - **Must not know.** How the CLI decides an outcome, or how the server admits or cancels a
//!   command.

use cucumber::{then, when};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};

use super::*;

/// How long one CLI run may take before a step fails it. The command a run sends waits for its
/// leader within the CLI's own deadlines, so the bound is generous rather than tight.
const CLI_RUN_TIMEOUT: Duration = Duration::from_secs(60);

/// A CLI process a scenario started and has not reaped yet.
pub(crate) struct StartedCli {
    child: tokio::process::Child,
}

impl StartedCli {
    /// Waits for `reached` while the CLI keeps running. A CLI that ends first never reached what
    /// the step waits for.
    pub(crate) async fn keeps_running_until(
        &mut self,
        reached: impl Future<Output = ()>,
        what: &str,
    ) {
        nervix_primitives::select! {
            () = reached => {}
            ended = self.child.wait() => panic!("the CLI ended before {what}: {ended:?}"),
        }
    }

    /// Delivers SIGINT, as a terminal does when its user presses Ctrl-C.
    fn interrupt(&self) {
        let raw = self
            .child
            .id()
            .verified("a preceding step started the CLI and it still runs");
        let pid = Pid::from_raw(
            i32::try_from(raw).assured("a process id fits the operating system's pid type"),
        );
        kill(pid, Signal::SIGINT)
            .unwrap_or_else(|error| panic!("the CLI could not be interrupted: {error}"));
    }

    /// Waits for the CLI to end and returns everything it printed.
    async fn finish(self) -> Output {
        nervix_primitives::time::timeout(CLI_RUN_TIMEOUT, self.child.wait_with_output())
            .await
            .unwrap_or_else(|_| panic!("the CLI did not end within {CLI_RUN_TIMEOUT:?}"))
            .unwrap_or_else(|error| panic!("the CLI could not be reaped: {error}"))
    }
}

/// The arguments that connect a CLI to `node` as the scenario's user, in the scenario's domain,
/// followed by `arguments`.
fn cli_arguments(world: &ScenarioWorld, node: &str, arguments: &[String]) -> Vec<String> {
    let grpc_uri = world
        .cluster()
        .grpc_uri(node)
        .assured("the scenario names a cluster node");
    let mut all = vec![
        "--server".to_string(),
        grpc_uri,
        "--domain".to_string(),
        world.domain.clone(),
        "--username".to_string(),
        TEST_AUTH_USERNAME.to_string(),
        "--password".to_string(),
        TEST_AUTH_PASSWORD.to_string(),
    ];
    all.extend_from_slice(arguments);
    all
}

/// Runs the CLI against `node` with `arguments` to its end, and keeps what it printed for the steps
/// that follow.
async fn run_cli(world: &mut ScenarioWorld, node: &str, arguments: &[String]) {
    let node = expand_placeholders(world, node);
    let mut process = tokio::process::Command::new(scenario_cli_binary());
    process
        .args(cli_arguments(world, &node, arguments))
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let output = nervix_primitives::time::timeout(CLI_RUN_TIMEOUT, process.output())
        .await
        .unwrap_or_else(|_| panic!("the CLI did not end within {CLI_RUN_TIMEOUT:?}"))
        .unwrap_or_else(|error| panic!("the CLI did not start: {error}"));
    world.last_cli_output = Some(output);
}

/// Everything after `option` up to the next space in what the last CLI run printed, without the
/// punctuation that ends a sentence.
fn printed_value_of(world: &ScenarioWorld, option: &str) -> String {
    let output = world
        .last_cli_output
        .as_ref()
        .verified("a preceding step ran the CLI");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let marker = format!("{option} ");
    let Some(start) = stdout.find(&marker) else {
        panic!("the CLI printed no {option}: {stdout}");
    };
    let rest = stdout
        .get(start + marker.len()..)
        .verified("the marker was found inside the output");
    let value = rest
        .split_whitespace()
        .next()
        .unwrap_or_else(|| panic!("the CLI printed {option} without a value: {stdout}"));
    value.trim_end_matches(['.', ',', ';', ')']).to_string()
}

#[when(expr = "the CLI runs {string} on node {string}")]
async fn when_cli_runs(world: &mut ScenarioWorld, arguments: String, node: String) {
    let arguments = expand_placeholders(world, &arguments);
    let arguments: Vec<String> = arguments.split_whitespace().map(str::to_string).collect();
    run_cli(world, &node, &arguments).await;
}

#[when(expr = "the CLI executes {string} under execution reference {string} on node {string}")]
async fn when_cli_executes_under_reference(
    world: &mut ScenarioWorld,
    command: String,
    reference: String,
    node: String,
) {
    let command = expand_placeholders(world, &command);
    let reference = expand_placeholders(world, &reference);
    let arguments = [
        "--command".to_string(),
        command,
        "--execution-reference".to_string(),
        reference,
    ];
    run_cli(world, &node, &arguments).await;
}

#[when(expr = "the CLI executes {string} under upload identity {string} on node {string}")]
async fn when_cli_executes_under_upload_identity(
    world: &mut ScenarioWorld,
    command: String,
    identity: String,
    node: String,
) {
    let command = expand_placeholders(world, &command);
    let identity = expand_placeholders(world, &identity);
    let arguments = [
        "--command".to_string(),
        command,
        "--upload-identity".to_string(),
        identity,
    ];
    run_cli(world, &node, &arguments).await;
}

/// Asserts that the last CLI run exited with `status` and printed `expected` on standard output.
fn assert_cli_ended(world: &ScenarioWorld, status: i32, expected: &str) {
    let expected = expand_placeholders(world, expected);
    let output = world
        .last_cli_output
        .as_ref()
        .verified("a preceding step ran the CLI");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.code() == Some(status) && stdout.contains(&expected),
        "the CLI must exit with status {status} and print {expected:?}; it exited with {}; \
         stdout: {stdout}; stderr: {stderr}",
        output.status
    );
}

#[then(expr = "the CLI exited with status {int} and printed {string}")]
fn then_cli_exited_with_status_and_printed(
    world: &mut ScenarioWorld,
    status: i32,
    expected: String,
) {
    assert_cli_ended(world, status, &expected);
}

#[when(expr = "the CLI starts executing {string} on node {string}")]
async fn when_cli_starts_executing(world: &mut ScenarioWorld, command: String, node: String) {
    let command = expand_placeholders(world, &command);
    let node = expand_placeholders(world, &node);
    let arguments = ["--command".to_string(), command];
    let child = tokio::process::Command::new(scenario_cli_binary())
        .args(cli_arguments(world, &node, &arguments))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap_or_else(|error| panic!("the CLI did not start: {error}"));
    world.started_cli = Some(StartedCli { child });
}

#[when("the started CLI receives Ctrl-C")]
fn when_started_cli_receives_ctrl_c(world: &mut ScenarioWorld) {
    world
        .started_cli
        .as_ref()
        .verified("a preceding step started the CLI")
        .interrupt();
}

#[then(expr = "the started CLI exits with status {int} and prints {string}")]
async fn then_started_cli_exits_with_status_and_prints(
    world: &mut ScenarioWorld,
    status: i32,
    expected: String,
) {
    let started = world
        .started_cli
        .take()
        .verified("a preceding step started the CLI");
    world.last_cli_output = Some(started.finish().await);
    assert_cli_ended(world, status, &expected);
}

#[then(expr = "the execution reference the CLI printed is saved as placeholder {string}")]
fn then_printed_execution_reference_is_saved(world: &mut ScenarioWorld, placeholder: String) {
    let reference = printed_value_of(world, "--execution-reference");
    world.placeholders.insert(placeholder, reference);
}

#[then(expr = "the upload identity the CLI printed is saved as placeholder {string}")]
fn then_printed_upload_identity_is_saved(world: &mut ScenarioWorld, placeholder: String) {
    let identity = printed_value_of(world, "--upload-identity");
    world.placeholders.insert(placeholder, identity);
}

#[when("the CLI REPL receives Ctrl-C")]
fn when_cli_repl_receives_ctrl_c(world: &mut ScenarioWorld) {
    let terminal = world
        .cli_terminal
        .as_ref()
        .verified("a preceding step started the CLI REPL");
    if let Err(error) = terminal.interrupt() {
        panic!(
            "the CLI REPL could not be interrupted: {error}; it displayed:{}",
            terminal.transcript()
        );
    }
}
