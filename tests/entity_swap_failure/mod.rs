//! Public proofs that a failed entity swap keeps the running domain, outside the product layer
//! order.
//!
//! Layer: test harness.
//! - **Owns.** The swap faults a scenario arms on one node, at the point where its swap replaces a
//!   named processor: a pause there, and a failure there that lasts until the scenario clears it.
//! - **Depends on.** The scenario's fault injector, placeholders, background command and node names.
//! - **Must not know.** The swap's ordering or the runtime's execution state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use std::{str::FromStr as _, time::Duration};

use cucumber::{given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName, NodeRef};
use nervix_primitives::time::Instant;

use super::{ScenarioWorld, expand_placeholders};
use crate::common::cluster::node_name;

/// How long a scenario waits for a swap to reach its replacement pause. The swap runs once the
/// change committed, which on a three-node cluster goes through consensus while the rest of the
/// suite competes for the machine.
const ENTITY_SWAP_PAUSE_TIMEOUT: Duration = Duration::from_secs(60);

/// The node and domain whose entity swaps a step arms or observes.
struct SwapTarget {
    node: ClusterNodeName,
    domain: DomainName,
}

impl SwapTarget {
    fn new(world: &ScenarioWorld, domain: &str, node: &str) -> Self {
        let domain = expand_placeholders(world, domain);
        let node = expand_placeholders(world, node);
        Self {
            node: node_name(&node),
            domain: DomainName::try_from(domain.as_str())
                .assured("the scenario uses an identifier-shaped domain name"),
        }
    }
}

fn processor_ref(world: &ScenarioWorld, kind: &str, name: &str) -> NodeRef {
    let Ok(kind) = ModelKind::from_str(kind) else {
        panic!("'{kind}' is not a model kind");
    };
    let name = expand_placeholders(world, name);
    NodeRef::new(
        kind,
        ModelName::try_from(name.as_str()).assured("the scenario names its processor validly"),
    )
}

#[given(
    expr = "every entity swap in domain {string} on node {string} fails while replacing {word} \
            {string}"
)]
async fn given_entity_swaps_fail_while_replacing(
    world: &mut ScenarioWorld,
    domain: String,
    node: String,
    kind: String,
    name: String,
) {
    let target = SwapTarget::new(world, &domain, &node);
    let processor = processor_ref(world, &kind, &name);
    world.fault_injection.fail_entity_swaps_while_replacing_on(
        &target.node,
        &target.domain,
        processor,
    );
}

#[when(expr = "entity swaps in domain {string} on node {string} stop failing")]
async fn when_entity_swaps_stop_failing(world: &mut ScenarioWorld, domain: String, node: String) {
    let target = SwapTarget::new(world, &domain, &node);
    world
        .fault_injection
        .clear_entity_swap_failures_on(&target.node, &target.domain);
}

#[given(
    expr = "the entity swap in domain {string} on node {string} pauses while replacing {word} \
            {string}"
)]
async fn given_entity_swap_pauses_while_replacing(
    world: &mut ScenarioWorld,
    domain: String,
    node: String,
    kind: String,
    name: String,
) {
    let target = SwapTarget::new(world, &domain, &node);
    let processor = processor_ref(world, &kind, &name);
    world.fault_injection.pause_entity_swap_while_replacing_on(
        &target.node,
        &target.domain,
        processor,
    );
}

#[then(expr = "the entity swap in domain {string} on node {string} reaches its replacement pause")]
async fn then_entity_swap_reaches_replacement_pause(
    world: &mut ScenarioWorld,
    domain: String,
    node: String,
) {
    let target = SwapTarget::new(world, &domain, &node);
    let fault_injection = world.fault_injection.clone();
    let deadline = Instant::now() + ENTITY_SWAP_PAUSE_TIMEOUT;
    loop {
        nervix_primitives::task::consume_budget().await;
        if nervix_primitives::time::timeout(
            Duration::from_millis(50),
            fault_injection.wait_for_entity_swap_replacement_pause(&target.node, &target.domain),
        )
        .await
        .is_ok()
        {
            return;
        }
        // A change that already answered will never reach the swap, so report what it answered
        // instead of waiting out a deadline it can no longer meet.
        if let Some(background) = world.background_plexus.as_ref()
            && background.is_finished()
        {
            let outcome = world
                .background_plexus
                .take()
                .verified("the branch above already observed the background execution")
                .await
                .expect("background Plexus task must not panic");
            panic!(
                "the entity swap in domain '{}' on node '{}' did not reach its replacement pause: \
                 the change finished first with {outcome:?}",
                target.domain.as_str(),
                target.node.as_str()
            );
        }
        assert!(
            Instant::now() < deadline,
            "the entity swap in domain '{}' on node '{}' did not reach its replacement pause \
             within {ENTITY_SWAP_PAUSE_TIMEOUT:?}",
            target.domain.as_str(),
            target.node.as_str()
        );
    }
}

#[when(
    expr = "the entity swap in domain {string} on node {string} is released from its replacement \
            pause"
)]
async fn when_entity_swap_is_released_from_replacement_pause(
    world: &mut ScenarioWorld,
    domain: String,
    node: String,
) {
    let target = SwapTarget::new(world, &domain, &node);
    world
        .fault_injection
        .release_entity_swap_replacement_pause(&target.node, &target.domain);
}
