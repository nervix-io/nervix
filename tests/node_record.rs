//! Public observations of a node's committed record: its detected properties and its tags.
//!
//! Outside the layer order: a harness module.
//! - **Owns.** Naming what a scenario node's detection double reports, and waiting for record
//!   facts through `DESCRIBE CLUSTER` on running nodes.
//! - **Depends on.** The scenario world, the cluster harness and the public session interface.
//! - **Must not know.** Consensus stores or how a deployed node probes its host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "observes committed node records through public sessions"
    )
)]

use cucumber::{given, then};

use crate::{ScenarioWorld, cluster_description_block, expand_placeholders};

/// Names a property a node's detection double reports from its next start: `gpu.count`,
/// `onnx.cuda` or `cpu.model`, as `DESCRIBE CLUSTER` names them.
#[given(regex = r"^(\S+) reports (\S+) = (\S+)$")]
fn given_node_reports(world: &mut ScenarioWorld, node_id: String, property: String, value: String) {
    let node_id = expand_placeholders(world, &node_id);
    match world.cluster.as_mut() {
        Some(cluster) => cluster.report_node_property(&node_id, property, value),
        None => {
            world
                .cluster_config
                .node_reports
                .entry(node_id)
                .or_default()
                .insert(property, value);
        }
    }
}

#[then(expr = "every node eventually describes node {string} with tags {string}")]
async fn every_node_describes_tags(world: &mut ScenarioWorld, node: String, tags: String) {
    let node = expand_placeholders(world, &node);
    for reader in cluster_nodes(world) {
        wait_for_tags(world, &reader, &node, &tags).await;
    }
}

#[then(expr = "node {string} eventually describes node {string} with tags {string}")]
async fn node_describes_tags(
    world: &mut ScenarioWorld,
    reader: String,
    node: String,
    tags: String,
) {
    let reader = expand_placeholders(world, &reader);
    let node = expand_placeholders(world, &node);
    wait_for_tags(world, &reader, &node, &tags).await;
}

#[then(expr = "every node eventually describes node {string} with property {string} as {string}")]
async fn every_node_describes_property(
    world: &mut ScenarioWorld,
    node: String,
    property: String,
    value: String,
) {
    let node = expand_placeholders(world, &node);
    let exact = format!("    {property}: {value}");
    let explained = format!("{exact} (");
    for reader in cluster_nodes(world) {
        world
            .wait_for_cluster_description(&reader, |description| {
                cluster_description_block(description, &node)
                    .iter()
                    .any(|line| *line == exact || line.starts_with(&explained))
            })
            .await;
    }
}

#[then(expr = "node {string} eventually describes no record of node {string}")]
async fn node_describes_no_record(world: &mut ScenarioWorld, reader: String, node: String) {
    let reader = expand_placeholders(world, &reader);
    let node = expand_placeholders(world, &node);
    world
        .wait_for_cluster_description(&reader, |description| {
            let block = cluster_description_block(description, &node);
            !block
                .iter()
                .any(|line| line.starts_with("  record: revision "))
        })
        .await;
}

/// Every node of the running cluster, by name.
fn cluster_nodes(world: &ScenarioWorld) -> Vec<String> {
    world
        .cluster
        .as_ref()
        .expect("the scenario started a cluster")
        .node_ids()
}

/// Waits until `reader` describes exactly `tags`, written as `--node-tags` takes them or `none`,
/// in the record of `node`.
async fn wait_for_tags(world: &mut ScenarioWorld, reader: &str, node: &str, tags: &str) {
    let expected = expected_tag_lines(tags);
    world
        .wait_for_cluster_description(reader, |description| {
            described_tag_lines(&cluster_description_block(description, node)) == expected
        })
        .await;
}

/// The lines `DESCRIBE CLUSTER` prints for `tags`: one `key=value` line per tag in key order under
/// `tags:`, or `tags: none`.
fn expected_tag_lines(tags: &str) -> Vec<String> {
    if tags == "none" {
        return vec!["  tags: none".to_string()];
    }
    let mut assignments = tags.split(',').collect::<Vec<_>>();
    assignments.sort_by_key(|assignment| tag_key(assignment));
    let mut lines = vec!["  tags:".to_string()];
    for assignment in assignments {
        lines.push(format!("    {assignment}"));
    }
    lines
}

/// The key of a `key=value` assignment.
fn tag_key(assignment: &str) -> &str {
    match assignment.split_once('=') {
        Some((key, _)) => key,
        None => assignment,
    }
}

/// The tag lines of one node's block, from its `tags` line to the end of its tags.
fn described_tag_lines(block: &[&str]) -> Vec<String> {
    let mut lines = Vec::new();
    for line in block {
        if line.starts_with("  tags:") {
            lines.push((*line).to_string());
            continue;
        }
        if lines.is_empty() {
            continue;
        }
        if !line.starts_with("    ") {
            break;
        }
        lines.push((*line).to_string());
    }
    lines
}
