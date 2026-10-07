//! Public ownership checks while a restarted voter has only one relayed gossip heartbeat.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** The restart action, the first-sample barrier, and public status observations.
//! - **Depends on.** The scenario world, cluster lifecycle, and the typed fault controls.
//! - **Must not know.** Scheduling decisions or runtime state storage.

use std::time::Duration;

use cucumber::{then, when};
use nervix_primitives::time::timeout;

use crate::{ScenarioWorld, common::cluster::node_name, run_nspl_commands_on_node};

#[when(expr = "the cluster restarts with voter {string}'s first heartbeat relayed to node-1")]
async fn restart_with_relayed_voter(world: &mut ScenarioWorld, voter: String) {
    world.active_session = None;
    world.active_session_node = None;
    world.active_session_has_subscription = false;
    let restarted = world.cluster_mut().restart_with_relayed_voter(&voter).await;
    assert!(restarted.is_ok(), "cluster restart failed: {restarted:?}");
}

#[then("node-1 reaches automatic scheduling with the relayed voter marked unavailable")]
async fn observe_first_heartbeat(world: &mut ScenarioWorld) {
    let observation = timeout(
        Duration::from_secs(30),
        world
            .fault_injection
            .wait_for_startup_voter_observation(&node_name("node-1")),
    )
    .await;
    assert!(
        observation.is_ok(),
        "the leader did not evaluate the relayed heartbeat during startup grace"
    );
    let status = run_nspl_commands_on_node(world, "node-1", "SHOW CLUSTER STATUS;").await;
    let status = match status {
        Ok(status) => status,
        Err(error) => panic!("public cluster status failed at the observation barrier: {error}"),
    };
    assert!(
        status.contains("raft member 'node-3' is marked unavailable by chitchat"),
        "the first relayed heartbeat must reproduce the initial failure-detector verdict: {status}"
    );
    world.last_command_output = Some(status);
}

#[when("node-1 completes that scheduling pass and receives further voter heartbeats")]
async fn release_first_heartbeat(world: &mut ScenarioWorld) {
    let completion = timeout(
        Duration::from_secs(30),
        world
            .fault_injection
            .release_startup_voter_observation(&node_name("node-1")),
    )
    .await;
    assert!(
        completion.is_ok(),
        "the first scheduling pass did not finish after its observation barrier released"
    );
    world
        .fault_injection
        .restore_health_responses_between(&node_name("node-1"), &node_name("node-3"));
    let caught_up = world.cluster().wait_for_node_raft_catch_up("node-3").await;
    assert!(
        caught_up.is_ok(),
        "the restarted voter did not catch up after release: {caught_up:?}"
    );
}
