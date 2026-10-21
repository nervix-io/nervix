//! Public transaction recovery at controlled domain-resume boundaries.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Domain-resume scenario barriers and exact transaction request replays.
//! - **Depends on.** The scenario world, fault controls and typed vocabulary identities.
//! - **Must not know.** Transaction application internals or mutation decisions.

use std::time::Duration;

use cucumber::{gherkin::Step, given, then, when};
use meticulous::ResultExt as _;
use nervix_models::DomainName;
use nervix_server::DomainResumeBoundary;

use super::{
    ScenarioWorld, command_execution_reference,
    common::{cluster::node_name, raw_session::WireOutcome as _},
    current_leader_node, docstring, expand_placeholders,
};

fn resume_boundary(boundary: &str) -> DomainResumeBoundary {
    match boundary {
        "proposal" => DomainResumeBoundary::Proposal,
        "failure" => DomainResumeBoundary::Failure,
        _ => panic!("unknown domain resume boundary '{boundary}'"),
    }
}

#[given(expr = "domain resume on node {string} pauses at its {word} boundary")]
async fn arm_resume_pause(world: &mut ScenarioWorld, node: String, boundary: String) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = DomainName::parse(&world.domain).assured("the scenario domain is valid");
    world
        .fault_injection
        .pause_domain_resume(node, domain, resume_boundary(&boundary));
}

#[then(expr = "domain resume on node {string} reaches its {word} boundary")]
async fn observe_resume_pause(world: &mut ScenarioWorld, node: String, boundary: String) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = DomainName::parse(&world.domain).assured("the scenario domain is valid");
    nervix_primitives::time::timeout(
        Duration::from_secs(30),
        world.fault_injection.wait_for_domain_resume_pause(
            &node,
            &domain,
            resume_boundary(&boundary),
        ),
    )
    .await
    .unwrap_or_else(|error| panic!("domain resume on '{node}' missed '{boundary}': {error}"));
}

#[when(expr = "domain resume on node {string} leaves its {word} boundary")]
async fn release_resume_pause(world: &mut ScenarioWorld, node: String, boundary: String) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = DomainName::parse(&world.domain).assured("the scenario domain is valid");
    world
        .fault_injection
        .release_domain_resume_pause(&node, &domain, resume_boundary(&boundary));
}

#[when(
    expr = "this Plexus command request with execution reference {string} and transaction \
            position {int} is replayed on the leader node"
)]
async fn replay_transaction_request(
    world: &mut ScenarioWorld,
    reference: String,
    position: u64,
    #[step] step: &Step,
) {
    let reference = command_execution_reference(world, &reference);
    let query = expand_placeholders(world, docstring(step));
    let position = usize::try_from(position).expect("a scenario position fits in usize");
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .assured("the replay session opens on the current leader");
    let result = session
        .run_command_result_with_reference_at_position(&query, &reference, position)
        .await
        .assured("the exact transaction request receives a reply");
    world.last_command_disposition = Some(result.disposition.clone());
    if result.succeeded() {
        world.last_command_error = None;
        world.last_command_output = Some(result.message);
    } else {
        world.last_command_output = None;
        world.last_command_error = Some(result.message);
    }
}
