//! Public command recovery at controlled standalone transaction preparation boundaries.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Standalone preparation scenario barriers.
//! - **Depends on.** The scenario world, fault controls and typed vocabulary identities.
//! - **Must not know.** Transaction application internals or mutation decisions.

use std::time::Duration;

use cucumber::{given, then, when};
use meticulous::ResultExt as _;
use nervix_models::DomainName;
use nervix_server::StandaloneTransactionBoundary;

use super::{ScenarioWorld, common::cluster::node_name, expand_placeholders};

fn preparation_boundary(boundary: &str) -> StandaloneTransactionBoundary {
    match boundary {
        "lookup" => StandaloneTransactionBoundary::Lookup,
        "preflight" => StandaloneTransactionBoundary::Preflight,
        _ => panic!("unknown standalone preparation boundary '{boundary}'"),
    }
}

#[given(expr = "standalone preparation on node {string} pauses at its {word} boundary")]
async fn arm_preparation_pause(world: &mut ScenarioWorld, node: String, boundary: String) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = DomainName::parse(&world.domain).assured("the scenario domain is valid");
    world.fault_injection.pause_standalone_transaction(
        node,
        domain,
        preparation_boundary(&boundary),
    );
}

#[then(expr = "standalone preparation on node {string} reaches its {word} boundary")]
async fn observe_preparation_pause(world: &mut ScenarioWorld, node: String, boundary: String) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = DomainName::parse(&world.domain).assured("the scenario domain is valid");
    nervix_primitives::time::timeout(
        Duration::from_secs(30),
        world.fault_injection.wait_for_standalone_transaction_pause(
            &node,
            &domain,
            preparation_boundary(&boundary),
        ),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("standalone preparation on '{node}' missed '{boundary}': {error}")
    });
}

#[when(expr = "standalone preparation on node {string} leaves its {word} boundary")]
async fn release_preparation_pause(world: &mut ScenarioWorld, node: String, boundary: String) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = DomainName::parse(&world.domain).assured("the scenario domain is valid");
    world.fault_injection.release_standalone_transaction_pause(
        &node,
        &domain,
        preparation_boundary(&boundary),
    );
}
