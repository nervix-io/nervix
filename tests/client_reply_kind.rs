//! Public native-client and CLI recovery after a command is answered with a reply of another kind.
//!
//! Layer: test harness.
//! - **Owns.** Arming the bounded reply-kind substitution and assertions over public command
//!   recovery.
//! - **Depends on.** The scenario cluster, native client, CLI and test fault injector.
//! - **Must not know.** Pending-request implementation or server command execution.

use cucumber::{given, then};

use super::*;

#[given(expr = "the next successful reply to native command {string} is a reply of another kind")]
async fn arm(world: &mut ScenarioWorld, query: String) {
    let domain = nervix_models::DomainName::parse(&world.domain).assured("a valid test domain");
    let query = expand_placeholders(world, &query);
    world
        .fault_injection
        .answer_command_with_another_reply_kind(domain, query);
}

#[then("the native command recovered its own execution reference after a reply of another kind")]
async fn assert_recovered(world: &mut ScenarioWorld) {
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
