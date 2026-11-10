//! Public observations of a node's committed capacity claim.
//!
//! Outside the layer order: a harness module.
//! - **Owns.** Public capability observations and controlled relay capability fixtures.
//! - **Depends on.** The scenario world, public sessions and typed fault-injection probes.
//! - **Must not know.** Consensus stores, scheduler internals or admission decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "observes committed capabilities through public sessions"
    )
)]

use cucumber::{given, then, when};
use nervix_models::{BranchKeyFingerprint, ClusterNodeIncarnation, ClusterNodeName};

use crate::{ScenarioWorld, cluster_description_block, expand_placeholders};

#[given(expr = "ingestor {string} pauses before buffering branch {string}")]
fn hold_branch_buffer(world: &mut ScenarioWorld, ingestor: String, branch: String) {
    let branch = BranchKeyFingerprint::of_canonical_text(&branch);
    world
        .fault_injection
        .pause_ingestor_branch_buffer(world.ingestor_dispatch_ref(&ingestor), branch);
}

#[then(expr = "ingestor {string} reaches its buffer pause for branch {string}")]
async fn waiting_branch_buffer(world: &mut ScenarioWorld, ingestor: String, branch: String) {
    let branch = BranchKeyFingerprint::of_canonical_text(&branch);
    let ingestor = world.ingestor_dispatch_ref(&ingestor);
    let reached = nervix_primitives::time::timeout(
        std::time::Duration::from_secs(30),
        world
            .fault_injection
            .wait_for_ingestor_branch_buffer_pause(&ingestor, branch),
    )
    .await;
    assert!(reached.is_ok(), "the branch reaches its buffer boundary");
}

#[when(expr = "ingestor {string} releases its buffer pause for branch {string}")]
fn release_branch_buffer(world: &mut ScenarioWorld, ingestor: String, branch: String) {
    let branch = BranchKeyFingerprint::of_canonical_text(&branch);
    world
        .fault_injection
        .release_ingestor_branch_buffer_pause(&world.ingestor_dispatch_ref(&ingestor), branch);
}

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

#[then(expr = "node {string} has refused {int} oversized branch rows in domain {string}")]
async fn refused_branch_rows(
    world: &mut ScenarioWorld,
    node: String,
    count: usize,
    domain: String,
) {
    let node = expand_placeholders(world, &node);
    let domain = expand_placeholders(world, &domain);
    let ready = nervix_primitives::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let refused = world.fault_injection.capability_relay_refusals(&domain);
            let selected = refused
                .into_iter()
                .filter(|refusal| refusal.node.as_str() == node)
                .collect::<Vec<_>>();
            if selected.len() >= count {
                return selected;
            }
            nervix_primitives::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    let Ok(refused) = ready else {
        panic!(
            "all {count} oversized branch buffers are refused before submitting fitting rows: {:?}",
            world.fault_injection.capability_relay_refusals(&domain)
        );
    };
    assert_eq!(refused.len(), count);
    assert!(refused.iter().all(|refusal| refusal.rows == 1));
    let channels = refused
        .iter()
        .map(|refusal| refusal.delivery.channel_incarnation)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(channels.len(), count, "each concrete branch was refused");
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
