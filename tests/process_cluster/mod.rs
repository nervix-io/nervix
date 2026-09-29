//! Steps that fault one member of a real-process cluster at a time and observe the cluster through
//! the members that keep running.
//!
//! Layer: test harness.
//! - **Owns.** Starting a plain three-process cluster, naming its members by role in placeholders,
//!   delivering a signal to one member and observing how it exited, restarting it from its own
//!   database, connecting a named client to one member, and reading `DESCRIBE INGESTOR` through
//!   the running leader.
//! - **Depends on.** The real-process cluster fixture, the scenario's placeholders, its named
//!   clients and its NSPL statement splitting.
//! - **Must not know.** How the server reacts to a fault; scenarios observe that through the
//!   public surfaces these steps reach.

use cucumber::{given, then, when};
use nix::sys::signal::Signal;

use super::*;

/// How long a step reads `DESCRIBE INGESTOR` again while waiting for the lines it expects.
const DESCRIBE_POLL_INTERVAL: Duration = Duration::from_millis(100);

impl ScenarioWorld {
    fn process_cluster(&self) -> &ServerProcessCluster {
        self.server_process_cluster
            .as_ref()
            .verified("a preceding step started a real-process cluster")
    }

    fn process_cluster_mut(&mut self) -> &mut ServerProcessCluster {
        self.server_process_cluster
            .as_mut()
            .verified("a preceding step started a real-process cluster")
    }
}

/// The signals a scenario delivers to one member, by the name its steps use.
fn member_signal(name: &str) -> Signal {
    match name {
        "SIGKILL" => Signal::SIGKILL,
        "SIGSTOP" => Signal::SIGSTOP,
        "SIGCONT" => Signal::SIGCONT,
        "SIGTERM" => Signal::SIGTERM,
        other => panic!("no step delivers signal '{other}' to a server process node"),
    }
}

/// The value a placeholder a step names holds.
fn saved_placeholder<'world>(world: &'world ScenarioWorld, placeholder: &str) -> &'world str {
    world
        .placeholders
        .get(placeholder)
        .unwrap_or_else(|| panic!("placeholder '{placeholder}' must be saved before it is used"))
}

/// Saves under `placeholder` the first member, in node order, that no excluded placeholder names.
fn save_other_member(world: &mut ScenarioWorld, excluded: &[&str], placeholder: String) {
    let mut excluded_members = BTreeSet::new();
    for excluded_placeholder in excluded {
        excluded_members.insert(saved_placeholder(world, excluded_placeholder).to_string());
    }
    let node_ids = world.process_cluster().node_ids();
    let mut chosen = None;
    for node_id in node_ids {
        if !excluded_members.contains(&node_id) {
            chosen = Some(node_id);
            break;
        }
    }
    let node_id =
        chosen.unwrap_or_else(|| panic!("no server process node exists other than {excluded:?}"));
    world.placeholders.insert(placeholder, node_id);
}

#[given("a 3 node nervix-server process cluster is started")]
async fn given_plain_server_process_cluster_is_started(world: &mut ScenarioWorld) {
    assert!(
        world.server_process_cluster.is_none(),
        "a scenario starts at most one real-process cluster"
    );
    initialize_scenario_identity(world);
    world.server_process_cluster = Some(
        ServerProcessCluster::start(&[])
            .await
            .unwrap_or_else(|error| panic!("failed to start the real-process cluster: {error}")),
    );
}

#[given(expr = "the server process cluster leader is saved as placeholder {string}")]
async fn given_server_process_cluster_leader_is_saved(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let leader = world
        .process_cluster()
        .leader_id()
        .await
        .unwrap_or_else(|error| panic!("the real-process cluster has no leader: {error}"));
    world.placeholders.insert(placeholder, leader);
}

#[given(
    expr = "a server process cluster node other than placeholder {string} is saved as placeholder \
            {string}"
)]
async fn given_other_server_process_node_is_saved(
    world: &mut ScenarioWorld,
    excluded: String,
    placeholder: String,
) {
    save_other_member(world, &[excluded.as_str()], placeholder);
}

#[given(
    expr = "a server process cluster node other than placeholders {string} and {string} is saved \
            as placeholder {string}"
)]
async fn given_third_server_process_node_is_saved(
    world: &mut ScenarioWorld,
    first_excluded: String,
    second_excluded: String,
    placeholder: String,
) {
    save_other_member(
        world,
        &[first_excluded.as_str(), second_excluded.as_str()],
        placeholder,
    );
}

#[when("these NSPL commands are executed on the server process cluster")]
async fn when_nspl_commands_are_executed_on_server_process_cluster(
    world: &mut ScenarioWorld,
    #[step] step: &Step,
) {
    let commands = expand_placeholders(world, docstring(step));
    for statement in nspl_statements(&commands) {
        tokio::task::consume_budget().await;
        let output = world
            .process_cluster()
            .run_commands(&world.domain, &statement)
            .await
            .unwrap_or_else(|error| panic!("real-process cluster rejected {statement:?}: {error}"));
        world.last_command_output = Some(output);
    }
}

/// Connects a named Rust client to one member, whose session serves every producer the client
/// opens.
#[given(expr = "client {string} is connected to server process node {string}")]
#[when(expr = "client {string} is connected to server process node {string}")]
async fn given_client_is_connected_to_server_process_node(
    world: &mut ScenarioWorld,
    name: String,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let grpc_uri = world
        .process_cluster()
        .grpc_uri(&node_id)
        .unwrap_or_else(|error| panic!("server process node '{node_id}': {error}"));
    connect_named_client(world, name, grpc_uri, Vec::new(), false).await;
}

#[when(expr = "server process node {string} receives {word}")]
async fn when_server_process_node_receives_signal(
    world: &mut ScenarioWorld,
    node_id: String,
    signal: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let signal = member_signal(&signal);
    world
        .process_cluster_mut()
        .signal(&node_id, signal)
        .unwrap_or_else(|error| {
            panic!("server process node '{node_id}' took no {signal}: {error}")
        });
}

#[then(expr = "server process node {string} is terminated by SIGKILL")]
async fn then_server_process_node_is_terminated_by_sigkill(
    world: &mut ScenarioWorld,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let status = world
        .process_cluster_mut()
        .wait_for_exit(&node_id)
        .await
        .unwrap_or_else(|error| panic!("server process node '{node_id}' did not exit: {error}"));
    assert_eq!(
        status.signal(),
        Some(nix::libc::SIGKILL),
        "server process node '{node_id}' exited with {status} rather than by SIGKILL"
    );
}

#[when(expr = "server process node {string} restarts from its existing database")]
async fn when_server_process_node_restarts(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .process_cluster_mut()
        .restart(&node_id)
        .await
        .unwrap_or_else(|error| panic!("server process node '{node_id}' did not rejoin: {error}"));
}

/// Runs `DESCRIBE INGESTOR` through the running leader until its output holds every line of the
/// docstring. The leader reads a client ingestor's producer counts from the node that executes it.
#[then(expr = "within {string} the server process cluster describes ingestor {string} with")]
async fn then_server_process_cluster_describes_ingestor_with(
    world: &mut ScenarioWorld,
    within: String,
    ingestor: String,
    #[step] step: &Step,
) {
    let within = humantime::parse_duration(&within).expect("the step names a valid duration");
    let expected = expand_placeholders(world, docstring(step));
    let ingestor = expand_placeholders(world, &ingestor);
    let command = format!("DESCRIBE INGESTOR {ingestor};");
    let deadline = Instant::now() + within;
    loop {
        tokio::task::consume_budget().await;
        let described = world
            .process_cluster()
            .run_commands(&world.domain, &command)
            .await;
        let output = match described {
            Ok(output) => output,
            Err(error) => format!("DESCRIBE failed: {error}"),
        };
        let mut missing = Vec::new();
        for line in expected.lines().map(str::trim) {
            if !line.is_empty() && !output.contains(line) {
                missing.push(line.to_string());
            }
        }
        if missing.is_empty() {
            world.last_command_output = Some(output);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "DESCRIBE INGESTOR {ingestor} never reported {missing:?}; last output:\n{output}"
        );
        tokio::time::sleep(DESCRIBE_POLL_INTERVAL).await;
    }
}
