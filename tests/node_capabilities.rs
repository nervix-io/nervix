//! Public observations of a node's committed capacity claim.
//!
//! Outside the layer order: a harness module.
//! - **Owns.** Waiting for capability facts through `DESCRIBE CLUSTER` on a running node.
//! - **Depends on.** The scenario world and the public session interface.
//! - **Must not know.** Consensus stores, scheduler internals or transport admission state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "observes committed capabilities through public sessions"
    )
)]

use cucumber::{given, then, when};
use nervix_models::{ClusterNodeIncarnation, ClusterNodeName};

use crate::{ScenarioWorld, cluster_description_block, expand_placeholders};

#[given(expr = "node {string} holds its ACK view of node {string} at its current incarnation")]
async fn hold_registrar_view(world: &mut ScenarioWorld, observer: String, registrar: String) {
    let observer = expand_placeholders(world, &observer);
    let registrar = expand_placeholders(world, &registrar);
    world
        .wait_for_cluster_description(&observer, |description| {
            cluster_description_block(description, &registrar)
                .iter()
                .any(|line| line.starts_with("  incarnation: "))
        })
        .await;
    let description = world
        .last_command_output
        .as_deref()
        .expect("the public description was read");
    let incarnation = cluster_description_block(description, &registrar)
        .iter()
        .find_map(|line| line.strip_prefix("  incarnation: "))
        .expect("the observed node has an incarnation")
        .parse::<u64>()
        .expect("numeric incarnation");
    world.fault_injection.hold_live_view_incarnation(
        ClusterNodeName::parse(&observer).expect("valid observing node"),
        ClusterNodeName::parse(&registrar).expect("valid registrar node"),
        ClusterNodeIncarnation::new(incarnation),
    );
}

#[when(expr = "node {string} releases its held ACK view of node {string}")]
async fn release_registrar_view(world: &mut ScenarioWorld, observer: String, registrar: String) {
    let observer = ClusterNodeName::parse(&expand_placeholders(world, &observer))
        .expect("valid observing node");
    let registrar = ClusterNodeName::parse(&expand_placeholders(world, &registrar))
        .expect("valid registrar node");
    world
        .fault_injection
        .close_live_view_gap(&observer, &registrar);
}

#[given(expr = "relay payloads for domain {string} are held before their grant requests")]
async fn hold_before_grant(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world.fault_injection.pause_relay_before_grant(domain);
}

#[then(expr = "a relay payload for domain {string} is waiting before its grant request")]
async fn waiting_before_grant(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    nervix_primitives::time::timeout(
        std::time::Duration::from_secs(30),
        world
            .fault_injection
            .wait_for_relay_before_grant_pause(&domain),
    )
    .await
    .expect("the held relay payload reaches its grant boundary");
}

#[when(expr = "relay payload grant requests for domain {string} are released")]
async fn release_before_grant(world: &mut ScenarioWorld, domain: String) {
    let domain = expand_placeholders(world, &domain);
    world
        .fault_injection
        .release_relay_before_grant_pause(&domain);
}

#[then(
    expr = "relay payloads to node {string} in domain {string} respect {string} with largest \
            slice {int}"
)]
fn admitted_slice_bounds(
    world: &mut ScenarioWorld,
    node: String,
    domain: String,
    bound: String,
    rows: usize,
) {
    let node = expand_placeholders(world, &node);
    let domain = expand_placeholders(world, &domain);
    let bound = bound
        .parse::<ubyte::ByteUnit>()
        .expect("receive bound")
        .as_u64();
    let observations = world.fault_injection.capability_relay_admissions(&domain);
    let selected = observations
        .iter()
        .filter(|(peer, _, _)| peer.as_str() == node)
        .collect::<Vec<_>>();
    assert_eq!(
        selected.iter().map(|(_, count, _)| *count).sum::<usize>(),
        8
    );
    assert_eq!(
        selected.iter().map(|(_, count, _)| *count).max(),
        Some(rows)
    );
    assert!(
        selected
            .iter()
            .all(|(_, _, bytes)| u64::try_from(*bytes).expect("bounded body") <= bound)
    );
}

#[then(expr = "node {string} eventually describes committed capabilities for node {string}")]
async fn committed_capabilities(world: &mut ScenarioWorld, reader: String, node: String) {
    let reader = expand_placeholders(world, &reader);
    let node = expand_placeholders(world, &node);
    world
        .wait_for_cluster_description(&reader, |description| {
            cluster_description_block(description, &node)
                .iter()
                .any(|line| {
                    line.starts_with("  capabilities: committed ")
                        && line.contains(" cpu-count=")
                        && line.contains(" memory-bytes=")
                        && line.contains(" relay-batch-bytes=")
                        && line.contains(" bulk-chunk-bytes=")
                })
        })
        .await;
}

#[then(
    expr = "node {string} eventually describes node {string} accepting relay bodies of {string}"
)]
async fn committed_relay_bound(
    world: &mut ScenarioWorld,
    reader: String,
    node: String,
    bound: String,
) {
    let reader = expand_placeholders(world, &reader);
    let node = expand_placeholders(world, &node);
    let bound = bound
        .parse::<ubyte::ByteUnit>()
        .expect("valid relay body bound");
    let expected = format!(" relay-batch-bytes={}", bound.as_u64());
    let adopted = format!("adopted peer receive bounds: node={node} ");
    world
        .wait_for_cluster_description(&reader, |description| {
            cluster_description_block(description, &node)
                .iter()
                .any(|line| {
                    line.starts_with("  capabilities: committed ") && line.contains(&expected)
                })
                && description
                    .lines()
                    .any(|line| line.starts_with(&adopted) && line.contains(&expected))
        })
        .await;
}

#[then(
    expr = "node {string} eventually describes node {string} with CPU count {int} and memory \
            {string}"
)]
async fn configured_capabilities(
    world: &mut ScenarioWorld,
    reader: String,
    node: String,
    cpu_count: u32,
    memory: String,
) {
    let reader = expand_placeholders(world, &reader);
    let node = expand_placeholders(world, &node);
    let memory = memory
        .parse::<ubyte::ByteUnit>()
        .expect("valid memory quantity");
    let expected = format!(" cpu-count={cpu_count} memory-bytes={}", memory.as_u64());
    world
        .wait_for_cluster_description(&reader, |description| {
            cluster_description_block(description, &node)
                .iter()
                .any(|line| {
                    line.starts_with("  capabilities: committed ") && line.contains(&expected)
                })
        })
        .await;
}
