//! Public graph assertions for retained emitter lifetimes.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The scenario's construction observation and exact lifetime assertion.
//! - **Depends on.** The public scenario world and its explicitly armed lifecycle probe.
//! - **Must not know.** Connector implementations or emitter recovery decisions.

use cucumber::{given, then};

use crate::{ScenarioWorld, expand_placeholders};

#[given(expr = "constructions of emitter {string} are observed")]
fn observe_emitter_constructions(world: &mut ScenarioWorld, emitter: String) {
    let emitter = expand_placeholders(world, &emitter);
    world.cluster().observe_emitter_opens(&emitter);
}

#[then(expr = "emitter {string} has been constructed exactly {int} time(s)")]
fn emitter_constructions(world: &mut ScenarioWorld, emitter: String, expected: usize) {
    let emitter = expand_placeholders(world, &emitter);
    assert_eq!(
        world.cluster().emitter_open_count(&emitter),
        expected,
        "emitter {emitter} must retain its connector across transient failures"
    );
}
