//! Public native-client and CLI restore uncertainty after a real restore completed.
//!
//! Layer: test harness.
//! - **Owns.** Invalid-reply observations and recovery using the original archive and reference.
//! - **Depends on.** Public clients, the real cluster, and the bounded gRPC reply peer.
//! - **Must not know.** Client session ownership or server restore implementation.

use nervix_client_host::{Failure, FailureKind};
use nervix_models::{Restore, RestoreLifecycle, RestoreMode, RestoreScope, RestoreState};

use super::*;
use crate::common::{
    raw_session::{fresh_execution_reference, session_channel},
    restore_reply_peer::{RestoreReceipt, RestoreReplyPeer, frames::ReplyFault},
};

enum InitialResult {
    Native(Result<Box<nervix_client_core::CommandOutcome>, Failure>),
    Cli(Output),
}

pub(crate) struct RestoreValidation {
    reference: CommandExecutionReference,
    query: String,
    node: String,
    receipt: RestoreReceipt,
    initial: InitialResult,
    recovered: Option<nervix_client_core::CommandOutcome>,
}

#[when(
    expr = "the {word} restores archive {string} through a peer answering with {string} on node \
            {string}"
)]
async fn when_restore_receives_invalid_reply(
    world: &mut ScenarioWorld,
    implementation: String,
    file: String,
    fault: String,
    node: String,
) {
    let fault = match fault.as_str() {
        "an undecodable reply" => ReplyFault::Undecodable,
        "another request's reply" => ReplyFault::AnotherRequest,
        other => panic!("unknown restore reply fixture {other}"),
    };
    let node = expand_placeholders(world, &node);
    let uri = world
        .cluster()
        .grpc_uri(&node)
        .assured("the restore node has a gRPC endpoint");
    let channel = session_channel(&uri)
        .await
        .assured("the peer connects to the real cluster");
    let peer = RestoreReplyPeer::start(channel, fault).await;
    let archive = archive_path(world, &file);
    let archive_bytes = tokio::fs::read(&archive)
        .await
        .assured("the real backup archive is readable");
    let restore = Restore {
        source: archive.display().to_string(),
        scope: RestoreScope::Domain {
            domain: scenario_domain(world, "{{domain}}"),
            target: Some(scenario_domain(world, "{{domain}}_copy")),
        },
        mode: RestoreMode::Apply,
        state: RestoreState::All,
        lifecycle: RestoreLifecycle::Stopped,
    };
    let query = restore.to_canonical_plexus();
    let reference = CommandExecutionReference::parse(fresh_execution_reference())
        .assured("the fixture creates a valid execution reference");
    let initial = match implementation.as_str() {
        "native-client" => {
            let client = Client::connect_with_options(
                &peer.uri,
                client_domain(&world.domain),
                client_connect_options(&peer.uri)
                    .assured("the fixture client has normal authentication"),
            )
            .await
            .assured("the native client connects through the peer");
            let execution = client
                .prepare_execution(query.clone())
                .await
                .with_reference(reference.clone());
            let result = nervix_primitives::time::timeout(
                CLI_BACKUP_TIMEOUT,
                client.execute_prepared(&execution),
            )
            .await
            .assured("the native restore ends within the scenario budget");
            drop(client);
            InitialResult::Native(result.map(Box::new).map_err(Failure::from))
        }
        "CLI" => {
            let mut command = tokio::process::Command::new(scenario_cli_binary());
            command.args([
                "--server",
                &peer.uri,
                "--username",
                TEST_AUTH_USERNAME,
                "--password",
                TEST_AUTH_PASSWORD,
                "--domain",
                &world.domain,
                "--command",
                &query,
                "--execution-reference",
                reference.as_str(),
            ]);
            command.kill_on_drop(true);
            let output = nervix_primitives::time::timeout(CLI_BACKUP_TIMEOUT, command.output())
                .await
                .assured("the CLI restore ends within its budget")
                .assured("the CLI starts");
            InitialResult::Cli(output)
        }
        other => panic!("unknown restore client {other}"),
    };
    let receipt = nervix_primitives::time::timeout(Duration::from_secs(60), peer.finish())
        .await
        .assured("the completed restore's peer finishes");
    assert_eq!(receipt.start.execution_reference, reference);
    assert_eq!(receipt.start.statement, query);
    assert_eq!(receipt.archive, archive_bytes);
    assert_eq!(
        receipt.start.archive.digest.as_bytes(),
        blake3::hash(&archive_bytes).as_bytes()
    );
    world.restore_reply_validation = Some(Box::new(RestoreValidation {
        reference,
        query,
        node,
        receipt,
        initial,
        recovered: None,
    }));
}

#[then("the completed restore is reported as uncertain under its original execution reference")]
fn then_restore_is_uncertain(world: &mut ScenarioWorld) {
    let observed = world
        .restore_reply_validation
        .as_ref()
        .assured("the scenario ran its restore through the peer");
    assert!(
        matches!(
            observed.receipt.outcome.disposition,
            nervix_client_wire::CommandDisposition::Completed { .. }
        ),
        "the substituted reply answers an actually completed restore: {:?}",
        observed.receipt.outcome
    );
    match &observed.initial {
        InitialResult::Native(Err(failure)) => {
            assert_eq!(
                failure.kind(),
                FailureKind::Uncertain,
                "{}",
                failure.message()
            );
            assert_eq!(failure.execution_reference(), Some(&observed.reference));
        }
        InitialResult::Native(Ok(outcome)) => {
            panic!("an invalid reply cannot establish a known restore outcome: {outcome:?}")
        }
        InitialResult::Cli(output) => {
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(output.status.code(), Some(3), "{text}");
            assert!(
                text.contains("outcome: not known yet")
                    && text.contains(observed.reference.as_str()),
                "{text}"
            );
        }
    }
}

#[when("the restore is repeated directly under its original execution reference")]
async fn when_restore_is_repeated(world: &mut ScenarioWorld) {
    let observed = world
        .restore_reply_validation
        .as_ref()
        .assured("the initial restore is recorded");
    let query = observed.query.clone();
    let reference = observed.reference.clone();
    let uri = world
        .cluster()
        .grpc_uri(&observed.node)
        .assured("the real restore node exists");
    let client = Client::connect_with_options(
        &uri,
        client_domain(&world.domain),
        client_connect_options(&uri).assured("the recovery client has normal authentication"),
    )
    .await
    .assured("the recovery client connects directly to the real cluster");
    let execution = client
        .prepare_execution(query)
        .await
        .with_reference(reference);
    let outcome =
        nervix_primitives::time::timeout(CLI_BACKUP_TIMEOUT, client.execute_prepared(&execution))
            .await
            .assured("restore recovery ends within its budget")
            .assured("the original restore outcome is recovered");
    world
        .restore_reply_validation
        .as_mut()
        .assured("the initial restore remains recorded")
        .recovered = Some(outcome);
}

#[then("the restore's completed outcome is recovered without another execution")]
fn then_restore_was_recovered(world: &mut ScenarioWorld) {
    let observed = world
        .restore_reply_validation
        .as_ref()
        .assured("the initial restore is recorded");
    let outcome = observed
        .recovered
        .as_ref()
        .assured("the original restore was repeated");
    assert!(outcome.succeeded(), "{}", outcome.message);
    assert_eq!(
        outcome.execution_reference.as_ref(),
        Some(&observed.reference)
    );
    assert_eq!(
        outcome.origin,
        Some(nervix_client_wire::OutcomeOrigin::Recovered)
    );
}
