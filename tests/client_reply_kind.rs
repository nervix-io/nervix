//! Public native-client, CLI and web console recovery after a command is answered with a reply of
//! another kind.
//!
//! Layer: test harness.
//! - **Owns.** Arming the bounded reply-kind substitution and assertions over public command
//!   recovery.
//! - **Depends on.** The scenario cluster, native client, CLI, web console and test fault injector.
//! - **Must not know.** Pending-request implementation or server command execution.

use cucumber::{given, then};

use super::*;

/// Answers the next successful reply to `query` in the scenario's domain with a reply of another
/// kind, whichever client sends it.
fn arm(world: &mut ScenarioWorld, query: &str) {
    let domain = nervix_models::DomainName::parse(&world.domain).assured("a valid test domain");
    let query = expand_placeholders(world, query);
    world
        .fault_injection
        .answer_command_with_another_reply_kind(domain, query);
}

/// Checks that the client repeated the command once, under the execution reference the reply of
/// another kind answered.
fn assert_recovered(world: &ScenarioWorld) {
    let domain = nervix_models::DomainName::parse(&world.domain).assured("a valid test domain");
    let answered = world
        .fault_injection
        .another_reply_kind_references(&domain)
        .assured("the script observed its command");
    assert_eq!(
        answered.len(),
        2,
        "the client repeats the command after the reply of another kind: {answered:?}"
    );
    assert_eq!(
        answered[0], answered[1],
        "the repeat keeps the command's own reference"
    );
}

#[given(expr = "the next successful reply to native command {string} is a reply of another kind")]
async fn arm_native(world: &mut ScenarioWorld, query: String) {
    arm(world, &query);
}

#[given(expr = "the next successful reply to console command {string} is a reply of another kind")]
async fn arm_console(world: &mut ScenarioWorld, query: String) {
    arm(world, &query);
}

#[then("the native command recovered its own execution reference after a reply of another kind")]
async fn assert_native_recovered(world: &mut ScenarioWorld) {
    assert_recovered(world);
}

#[then("the console command recovered its own execution reference after a reply of another kind")]
async fn assert_console_recovered(world: &mut ScenarioWorld) {
    assert_recovered(world);
}
