//! A browser command recovering scripted replies: three uncertain ones on its existing session, or
//! one naming another execution, after which it is repeated on a new connection.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Bounded reply scripts and observations of real connection/setup identities.
//! - **Depends on.** The public browser, session codec, scenario cluster and test fault controls.
//! - **Must not know.** Browser queues, retry state or server command implementation.

use std::time::Duration;

use cucumber::{given, then};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::DomainName;

use super::{ScenarioWorld, expand_placeholders};
use crate::common::console_connections::{ConsoleConnectionRecord, ConsoleSetupObservation};

async fn observation(world: &ScenarioWorld) -> ConsoleSetupObservation {
    let record = world
        .browser_page
        .as_ref()
        .assured("the scenario opened its console")
        .evaluate::<(), ConsoleConnectionRecord>(
            "() => window.__nervixConsoleConnections.record()",
            None::<&()>,
        )
        .await
        .assured("the page retains its bounded connection record");
    record
        .setup_observation()
        .assured("the node's public session frames decode")
}

#[given(expr = "the next three successful replies to console command {string} are uncertain")]
async fn arm(world: &mut ScenarioWorld, query: String) {
    let before = observation(world).await;
    assert!(!before.connections.is_empty(), "the console has connected");
    assert!(before.domain_selections > 0, "the domain has been selected");
    assert!(before.clock_attachments > 0, "the domain clock is attached");
    assert!(before.subscriptions > 0, "the relay tab has opened");
    world.console_before_command_retry = Some(before);
    let domain = DomainName::parse(&world.domain).assured("the scenario has a valid domain");
    let query = expand_placeholders(world, &query);
    world
        .fault_injection
        .make_command_replies_uncertain(domain, query);
}

#[then("the uncertain console command kept its connection and existing attachments")]
async fn recovered(world: &mut ScenarioWorld) {
    let domain = DomainName::parse(&world.domain).assured("the scenario has a valid domain");
    let references =
        nervix_primitives::time::timeout(super::WEB_CONSOLE_ASSERTION_TIMEOUT, async {
            loop {
                nervix_primitives::task::consume_budget().await;
                let references = world.fault_injection.uncertain_command_references(&domain);
                if references.len() == 4 {
                    return references;
                }
                nervix_primitives::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the command did not reach its fourth, terminal reply"));
    assert_eq!(
        references.len(),
        4,
        "three uncertain replies precede the recovered outcome"
    );
    let first = references.first().assured("four references were observed");
    assert!(references.iter().all(|reference| reference == first));
    let after = observation(world).await;
    let before = world
        .console_before_command_retry
        .as_ref()
        .assured("the scenario saved its existing connection and attachments");
    assert_eq!(
        &after, before,
        "the command recovers on the same session with its original setup"
    );
}

#[given(expr = "the next successful reply to console command {string} names another execution")]
async fn arm_foreign_execution(world: &mut ScenarioWorld, query: String) {
    let before = observation(world).await;
    assert!(!before.connections.is_empty(), "the console has connected");
    world.console_before_command_retry = Some(before);
    let domain = DomainName::parse(&world.domain).assured("the scenario has a valid domain");
    let query = expand_placeholders(world, &query);
    world
        .fault_injection
        .answer_command_with_foreign_execution(domain, query);
}

/// Saves the command's own execution reference as placeholder `own_execution` and the foreign one
/// its first reply named as `foreign_execution`.
#[then("the console repeated the command under its own execution reference on a new connection")]
async fn repeated_under_own_execution(world: &mut ScenarioWorld) {
    let domain = DomainName::parse(&world.domain).assured("the scenario has a valid domain");
    let observed = nervix_primitives::time::timeout(super::WEB_CONSOLE_ASSERTION_TIMEOUT, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let observed = world.fault_injection.foreign_execution_observation(&domain);
            if let Some(observed) = observed
                && observed.answered.len() == 2
            {
                return observed;
            }
            nervix_primitives::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("the command was not sent again after its reply named another execution")
    });
    let own = observed
        .answered
        .first()
        .assured("two references were observed");
    assert!(
        observed.answered.iter().all(|reference| reference == own),
        "the command is repeated under its own execution reference, got {:?}",
        observed.answered
    );
    assert_ne!(
        &observed.foreign, own,
        "the substituted reply named another execution"
    );
    let after = observation(world).await;
    let before = world
        .console_before_command_retry
        .as_ref()
        .assured("the scenario saved the console's connections before the command");
    let expected = before
        .connections
        .len()
        .checked_add(1)
        .assured("a page holds far fewer than usize::MAX connections");
    assert_eq!(
        after.connections.len(),
        expected,
        "the console stops reading the connection whose reply named another execution and repeats \
         the command on one new connection"
    );
    world
        .placeholders
        .insert("own_execution".to_string(), own.to_string());
    world.placeholders.insert(
        "foreign_execution".to_string(),
        observed.foreign.to_string(),
    );
}
