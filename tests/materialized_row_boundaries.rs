//! Scenario controls at a materialized relay's row boundaries: the two sides of a row a partition
//! count change moves, and the relay's checkpoint writer.
//!
//! Outside the layer order: a harness module.
//! - **Owns.** Arming, awaiting and releasing the one-shot pauses before a moved row's holder
//!   encodes it, before its claimant decodes it, and before the checkpoint writer seals rows that
//!   changed.
//! - **Depends on.** The scenario's fault injector and its domain.
//! - **Must not know.** Partition transitions, checkpoint cadence or how rows are encoded.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "controls testing-only pauses at materialized row boundaries"
    )
)]

use std::time::Duration;

use cucumber::{given, then, when};
use meticulous::ResultExt as _;
use nervix_models::{DomainName, DomainNodeRef, ModelKind, ModelName};

use crate::{ScenarioWorld, expand_placeholders};

/// How long a boundary may take to be reached once the scenario triggered the work that reaches it.
const BOUNDARY_BUDGET: Duration = Duration::from_secs(60);

/// The relay a boundary step names, in the scenario's domain.
fn relay(world: &ScenarioWorld, relay: &str) -> DomainNodeRef {
    let domain = world
        .domain
        .parse::<DomainName>()
        .assured("the scenario creates a valid domain");
    let relay = expand_placeholders(world, relay)
        .parse::<ModelName>()
        .assured("the scenario names a valid relay");
    DomainNodeRef::node_in(domain, ModelKind::Relay, relay)
}

/// Holds one side of the next row a partition count change moves for the relay: its `holder`
/// before it encodes the row it releases, or its `claimant` before it decodes the row it took.
#[given(expr = "the {word} of a moved row of relay {string} pauses before handling it")]
fn given_moved_row_side_pauses(world: &mut ScenarioWorld, side: String, relay_name: String) {
    let relay = relay(world, &relay_name);
    match side.as_str() {
        "holder" => world.fault_injection.pause_materialized_row_release(relay),
        "claimant" => world.fault_injection.pause_materialized_row_take(relay),
        other => panic!("a moved row has a holder and a claimant, not a '{other}'"),
    }
}

#[then(expr = "the {word} of a moved row of relay {string} reaches its pause")]
async fn then_moved_row_side_reaches_pause(
    world: &mut ScenarioWorld,
    side: String,
    relay_name: String,
) {
    let relay = relay(world, &relay_name);
    let fault_injection = world.fault_injection.clone();
    let reached = async {
        match side.as_str() {
            "holder" => {
                fault_injection
                    .wait_for_materialized_row_release(&relay)
                    .await
            }
            "claimant" => fault_injection.wait_for_materialized_row_take(&relay).await,
            other => panic!("a moved row has a holder and a claimant, not a '{other}'"),
        }
    };
    nervix_primitives::time::timeout(BOUNDARY_BUDGET, reached)
        .await
        .unwrap_or_else(|_| panic!("the {side} of a moved row of relay {relay:?} never paused"));
}

#[when(expr = "the {word} of a moved row of relay {string} is released from its pause")]
fn when_moved_row_side_is_released(world: &mut ScenarioWorld, side: String, relay_name: String) {
    let relay = relay(world, &relay_name);
    match side.as_str() {
        "holder" => world
            .fault_injection
            .release_materialized_row_release(&relay),
        "claimant" => world.fault_injection.release_materialized_row_take(&relay),
        other => panic!("a moved row has a holder and a claimant, not a '{other}'"),
    }
}

/// Holds the next checkpoint the relay's writer makes of rows that changed, before it seals them
/// into its staging files and publishes them.
#[given(expr = "the next checkpoint of relay {string} pauses before it seals its rows")]
fn given_checkpoint_pauses(world: &mut ScenarioWorld, relay_name: String) {
    let relay = relay(world, &relay_name);
    world.fault_injection.pause_materialized_checkpoint(relay);
}

#[then(expr = "the checkpoint of relay {string} reaches its pause")]
async fn then_checkpoint_reaches_pause(world: &mut ScenarioWorld, relay_name: String) {
    let relay = relay(world, &relay_name);
    let fault_injection = world.fault_injection.clone();
    nervix_primitives::time::timeout(
        BOUNDARY_BUDGET,
        fault_injection.wait_for_materialized_checkpoint(&relay),
    )
    .await
    .unwrap_or_else(|_| panic!("no checkpoint of relay {relay:?} paused"));
}

#[when(expr = "the checkpoint of relay {string} is released from its pause")]
fn when_checkpoint_is_released(world: &mut ScenarioWorld, relay_name: String) {
    let relay = relay(world, &relay_name);
    world
        .fault_injection
        .release_materialized_checkpoint(&relay);
}
