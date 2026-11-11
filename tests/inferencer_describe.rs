//! Layer: harness.
//! Owns: bounded public assertions for inferencer state summaries.
//! Depends on: the scenario cluster and its public command transport.
//! Must not know: runtime tensor owners or interconnect implementation details.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "observes inferencers through public sessions")
)]

use std::time::Duration;

use cucumber::then;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::task::consume_budget;

use crate::{
    ScenarioWorld, common::phase_deadline::PhaseDeadline, expand_placeholders,
    run_plexus_commands_on_node,
};

#[then(
    expr = "within {string} node {string} describes inferencer {string} with {int} branches and \
            {int} state bytes and {int} replicas"
)]
async fn inferencer_state_summary(
    world: &mut ScenarioWorld,
    within: String,
    node: String,
    inferencer: String,
    branches: u64,
    bytes: u64,
    replicas: usize,
) {
    let duration = nervix_models::parse_duration_text(&within)
        .assured("the scenario declares a valid duration");
    let deadline = PhaseDeadline::after(duration);
    let node = expand_placeholders(world, &node);
    let inferencer = expand_placeholders(world, &inferencer);
    let command = format!("DESCRIBE INFERENCER {inferencer};");
    let expected_branches = format!("branches: {branches}");
    let expected_bytes = format!("state bytes: {bytes}");
    let mut last = String::new();
    while !deadline.has_passed() {
        consume_budget().await;
        match run_plexus_commands_on_node(world, &node, &command).await {
            Ok(output) => {
                let has_branches = output.lines().any(|line| line.trim() == expected_branches);
                let has_bytes = output.lines().any(|line| line.trim() == expected_bytes);
                let progress = has_positive_revision(&output, "published revision: ")
                    && has_positive_revision(&output, "persisted revision: ");
                let mut replica_count = 0;
                let mut replicas_ready = true;
                for line in output.lines() {
                    let Some(replica) = line.trim().strip_prefix("replica ") else {
                        continue;
                    };
                    let Some((_, revision)) = replica.split_once(" replicated revision: ") else {
                        continue;
                    };
                    replica_count += 1;
                    let Ok(revision) = revision.parse::<u64>() else {
                        replicas_ready = false;
                        continue;
                    };
                    replicas_ready &= revision > 0;
                }
                replicas_ready &= replica_count == replicas;
                if has_branches && has_bytes && progress && replicas_ready {
                    world.last_command_output = Some(output);
                    return;
                }
                last = output;
            }
            Err(error) => last = error,
        }
        deadline.pause(Duration::from_millis(50)).await;
    }
    panic!(
        "inferencer summary on {node} did not report {branches} branches / {bytes} bytes / \
         {replicas} replicas with progress: {last}"
    );
}

#[then(
    expr = "within {string} every node describes inferencer {string} with {int} branches and \
            {int} state bytes and {int} replicas"
)]
async fn every_node_describes_inferencer(
    world: &mut ScenarioWorld,
    within: String,
    inferencer: String,
    branches: u64,
    bytes: u64,
    replicas: usize,
) {
    let nodes = world.cluster().node_ids();
    for node in nodes {
        consume_budget().await;
        inferencer_state_summary(
            world,
            within.clone(),
            node,
            inferencer.clone(),
            branches,
            bytes,
            replicas,
        )
        .await;
    }
}

#[then("the inferencer summary reports published and persisted revisions")]
fn inferencer_summary_has_progress(world: &mut ScenarioWorld) {
    let output = world
        .last_command_output
        .as_deref()
        .verified("the preceding summary step stored a public description");
    for label in ["published revision: ", "persisted revision: "] {
        assert!(
            has_positive_revision(output, label),
            "missing positive {label} in {output}"
        );
    }
}

fn has_positive_revision(output: &str, label: &str) -> bool {
    for line in output.lines() {
        let Some(value) = line.trim().strip_prefix(label) else {
            continue;
        };
        let Ok(revision) = value.parse::<u64>() else {
            return false;
        };
        return revision > 0;
    }
    false
}
