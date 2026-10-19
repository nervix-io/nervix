//! Standalone command transaction tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that a refusal decided against a coordinator's stale observation of a
//!   command's transaction recovers the progress recorded since, and that a refusal against an
//!   unchanged transaction keeps its failure.
//! - **Depends on.** The session command path, the session test fixtures and, in a testing build,
//!   the standalone preparation barriers.
//! - **Must not know.** Production ownership beyond the parent module under test.

use nervix_client_wire::CommandRequest;
use nervix_recovery::Discarded as _;

use crate::application::{
    command_result::CommandDisposition,
    subscription::SessionSubscriptions,
    test_fixtures::{
        TestService, build_test_service, test_command_request, test_execution_reference,
    },
};

/// The command transaction `command` applies through, as the session command path names it.
fn command_transaction_id(request: &CommandRequest) -> String {
    format!("command.{}", request.execution_reference.as_str())
}

#[nervix_primitives::test]
async fn a_refusal_against_an_absent_transaction_keeps_its_failure() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    let request = CommandRequest {
        execution_reference: test_execution_reference(),
        ..test_command_request(
            "ALTER SCHEMA missing ADD FIELD note STRING OPTIONAL;",
            "default",
        )
    };
    let transaction_id = command_transaction_id(&request);

    let result = service.test_command(request, &mut subscriptions).await;

    assert!(
        matches!(result.disposition, CommandDisposition::Failed),
        "a refused preflight against no transaction fails the command: {result:?}"
    );
    assert!(
        result.message.contains("failed preflight"),
        "the command keeps its preflight refusal: {}",
        result.message
    );
    assert!(
        service
            .inner
            .consensus
            .current_transaction(&transaction_id)
            .await
            .is_none(),
        "a refused preflight opens no transaction"
    );

    subscriptions.stop_all().await;
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

/// The original coordinator of a standalone command looks its transaction up and finds none, and is
/// held there while another coordinator applies the same command to completion, as a leader that
/// takes over the applying execution does. Its preflight is then refused by the command's own
/// effect; it must recover the committed transaction instead of recording that refusal.
#[cfg(feature = "testing")]
#[nervix_primitives::test]
async fn a_coordinator_recovers_the_command_another_coordinator_committed_after_its_lookup() {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::DomainName;

    use crate::fault_injection::StandaloneTransactionBoundary;

    let fault_injection = crate::FaultInjection::default();
    let TestService {
        service,
        registry,
        path,
    } = crate::application::test_fixtures::build_test_service_with_fault_injection(
        true,
        fault_injection.clone(),
    )
    .await;
    let node = service.inner.consensus.local_node_id().clone();
    let domain =
        DomainName::parse("default").assured("the fixture domain name is an accepted literal");
    fault_injection.pause_standalone_transaction(
        node.clone(),
        domain.clone(),
        StandaloneTransactionBoundary::Lookup,
    );
    let request = CommandRequest {
        execution_reference: test_execution_reference(),
        ..test_command_request("CREATE SCHEMA recovered ( key I64 );", "default")
    };
    let reference = request.execution_reference.clone();
    let transaction_id = command_transaction_id(&request);
    let original = {
        let service = service.clone();
        nervix_primitives::task::spawn(async move {
            let mut subscriptions = SessionSubscriptions::new();
            let result = service.test_command(request, &mut subscriptions).await;
            subscriptions.stop_all().await;
            result
        })
    };
    fault_injection
        .wait_for_standalone_transaction_pause(
            &node,
            &domain,
            StandaloneTransactionBoundary::Lookup,
        )
        .await;

    let execution = service
        .inner
        .consensus
        .current_command_execution(&reference)
        .await
        .assured("the command was admitted before its coordinator looked its transaction up");
    let Some(nervix_consensus::CommandExecutionEffect::Transaction {
        transaction_id: applied_transaction_id,
        source,
        statement,
    }) = execution.effect().cloned()
    else {
        panic!("a model command applies through a standalone transaction: {execution:?}");
    };
    assert_eq!(applied_transaction_id, transaction_id);
    let owner = execution
        .owner()
        .verified("an applying execution retains its owner")
        .clone();
    let successor = service
        .execute_standalone_transaction(
            transaction_id.clone(),
            reference.clone(),
            owner,
            domain.clone(),
            source,
            *statement,
        )
        .await;
    assert!(
        successor.succeeded(),
        "the second coordinator applies the command: {successor:?}"
    );

    fault_injection.release_standalone_transaction_pause(
        &node,
        &domain,
        StandaloneTransactionBoundary::Lookup,
    );
    let result = original
        .await
        .assured("the original command task runs to its end");

    assert!(
        result.succeeded(),
        "the original coordinator recovers the committed command: {result:?}"
    );
    let transaction = service
        .inner
        .consensus
        .current_transaction(&transaction_id)
        .await
        .verified("the committed command transaction keeps its tombstone");
    assert_eq!(transaction.state.as_str(), "COMMITTED");
    let recorded = service
        .inner
        .consensus
        .current_command_execution(&reference)
        .await
        .verified("the finished command keeps its outcome under its reference");
    assert!(
        !recorded.is_applying(),
        "the command records one terminal outcome: {recorded:?}"
    );
    let replayed = {
        let mut subscriptions = SessionSubscriptions::new();
        let replay = CommandRequest {
            execution_reference: reference,
            ..test_command_request("CREATE SCHEMA recovered ( key I64 );", "default")
        };
        let replayed = service.test_command(replay, &mut subscriptions).await;
        subscriptions.stop_all().await;
        replayed
    };
    assert!(
        replayed.succeeded(),
        "an exact replay returns the recorded completion: {replayed:?}"
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}
