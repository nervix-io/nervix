//! Public native-client and CLI recovery after an answered command names another execution.
//!
//! Layer: test harness.
//! - **Owns.** Bounded reply substitution and assertions over public command recovery.
//! - **Depends on.** The scenario cluster, native client, CLI and test fault injector.
//! - **Must not know.** Pending-request implementation or server command execution.

use cucumber::{given, then};

use super::*;

#[given(expr = "the next successful reply to native command {string} names another execution")]
async fn arm(world: &mut ScenarioWorld, query: String) {
    let domain = nervix_models::DomainName::parse(&world.domain).assured("a valid test domain");
    let query = expand_placeholders(world, &query);
    world
        .fault_injection
        .answer_command_with_foreign_execution(domain, query);
}

#[then("the native command recovered only its own execution reference")]
async fn assert_recovered(world: &mut ScenarioWorld) {
    let domain = nervix_models::DomainName::parse(&world.domain).assured("a valid test domain");
    let observation = world
        .fault_injection
        .foreign_execution_observation(&domain)
        .assured("the script observed its command");
    assert_eq!(
        observation.answered.len(),
        2,
        "the client repeats after the untrusted reply"
    );
    assert_eq!(
        observation.answered[0], observation.answered[1],
        "the repeat keeps the own reference"
    );
    assert_ne!(observation.answered[0], observation.foreign);
}
