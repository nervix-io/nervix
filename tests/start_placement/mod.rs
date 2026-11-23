//! Public START placement at controlled boundaries of a START that places a stopped domain's units
//! under its node rules.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The steps that arm, observe and release a START's placement barriers in the active
//!   domain.
//! - **Depends on.** The scenario world, fault controls and typed vocabulary identities.
//! - **Must not know.** Node rule judgement, schedules, ownership handoffs or how a START answers.

use std::time::Duration;

use cucumber::{given, then, when};
use meticulous::ResultExt as _;
use nervix_models::DomainName;
use nervix_server::StartPlacementBoundary;

use super::{ScenarioWorld, common::cluster::node_name, expand_placeholders};

/// How long a START may take to reach an armed placement boundary. It reaches it after its
/// admission, its first judgement of the domain's units and, for a deferral, a move whose
/// ownership handoff could not begin, each of which a loaded host slows.
const START_PLACEMENT_BOUNDARY_BUDGET: Duration = Duration::from_secs(90);

fn placement_boundary(boundary: &str) -> StartPlacementBoundary {
    match boundary {
        "wait" => StartPlacementBoundary::Waiting,
        "deferral" => StartPlacementBoundary::Deferred,
        _ => panic!("unknown START placement boundary '{boundary}'"),
    }
}

#[given(expr = "START placement on node {string} pauses at its {word} boundary")]
async fn arm_start_placement_pause(world: &mut ScenarioWorld, node: String, boundary: String) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = DomainName::parse(&world.domain).assured("the scenario domain is valid");
    world
        .fault_injection
        .pause_start_placement(node, domain, placement_boundary(&boundary));
}

#[then(expr = "START placement on node {string} reaches its {word} boundary")]
async fn observe_start_placement_pause(world: &mut ScenarioWorld, node: String, boundary: String) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = DomainName::parse(&world.domain).assured("the scenario domain is valid");
    nervix_primitives::time::timeout(
        START_PLACEMENT_BOUNDARY_BUDGET,
        world.fault_injection.wait_for_start_placement_pause(
            &node,
            &domain,
            placement_boundary(&boundary),
        ),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("START placement on '{node}' did not reach its {boundary} boundary: {error}")
    });
}

#[when(expr = "START placement on node {string} leaves its {word} boundary")]
async fn release_start_placement_pause(world: &mut ScenarioWorld, node: String, boundary: String) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = DomainName::parse(&world.domain).assured("the scenario domain is valid");
    world.fault_injection.release_start_placement_pause(
        &node,
        &domain,
        placement_boundary(&boundary),
    );
}
