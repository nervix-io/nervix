//! Transaction inspection tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that a read answers the right revision and changes nothing.
//! - **Depends on.** The transaction control plane and the session test fixtures.
//! - **Must not know.** Production ownership beyond the parent module under test.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::CommandRequest;
use nervix_consensus::{ReplicatedTransaction, TransactionState};
use nervix_models::{
    DomainName, ExecutionStepOutcome, TransactionInspection, TransactionInspectionRejection,
    TransactionInspectionRequest, TransactionInspectionTarget, TransactionLifecycle,
    TransactionOperationNumber, TransactionPosition, UserName,
};

use super::{super::DEFAULT_TRANSACTION_MAX_OPEN, InspectingSession, TransactionInspectionOutcome};
use crate::application::{
    SessionServiceImpl,
    command_result::CommandResult,
    subscription::SessionSubscriptions,
    test_fixtures::{TestService, build_test_service, test_command_request},
};

fn inspect(target: TransactionInspectionTarget) -> TransactionInspectionRequest {
    TransactionInspectionRequest {
        target,
        operation: None,
    }
}

fn by_id(transaction_id: &str) -> TransactionInspectionTarget {
    TransactionInspectionTarget::Transaction {
        transaction_id: transaction_id.to_string(),
    }
}

fn operation(number: usize) -> TransactionOperationNumber {
    TransactionOperationNumber::from_index(
        number
            .checked_sub(1)
            .assured("a test operation number is one-based"),
    )
    .assured("a test operation number is addressable")
}

fn inspected(outcome: TransactionInspectionOutcome) -> TransactionInspection {
    match outcome {
        TransactionInspectionOutcome::Inspected(inspection) => *inspection,
        other => panic!("the inspection should have read a report, found {other:?}"),
    }
}

fn rejection(outcome: TransactionInspectionOutcome) -> TransactionInspectionRejection {
    match outcome {
        TransactionInspectionOutcome::Rejected { rejection, .. } => rejection,
        other => panic!("the inspection should have been refused, found {other:?}"),
    }
}

/// Runs `query` through the session, asserting it was accepted.
async fn run(
    service: &SessionServiceImpl,
    subscriptions: &mut SessionSubscriptions,
    query: &str,
    expected_transaction_position: Option<usize>,
) -> CommandResult {
    let result = service
        .test_command(
            CommandRequest {
                expected_transaction_position: expected_transaction_position
                    .map(TransactionPosition::new),
                ..test_command_request(query, "default")
            },
            subscriptions,
        )
        .await;
    assert!(
        result.succeeded(),
        "{query:?} should be accepted: {result:?}"
    );
    result
}

/// An open transaction holding two queued schema operations.
async fn two_operation_transaction(
    service: &SessionServiceImpl,
    subscriptions: &mut SessionSubscriptions,
) {
    run(service, subscriptions, "BEGIN;", None).await;
    run(
        service,
        subscriptions,
        "CREATE SCHEMA inspected_first ( user_id U32 );",
        Some(0),
    )
    .await;
    run(
        service,
        subscriptions,
        "CREATE SCHEMA inspected_second ( order_id U32 );",
        Some(1),
    )
    .await;
}

#[tokio::test]
async fn inspecting_the_attached_transaction_reads_its_open_report() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    two_operation_transaction(&service, &mut subscriptions).await;

    let inspection = inspected(
        service
            .inspect_transaction(
                &inspect(TransactionInspectionTarget::Attached),
                InspectingSession::from(&subscriptions),
            )
            .await,
    );

    assert_eq!(
        inspection.transaction.lifecycle(),
        &TransactionLifecycle::Open
    );
    assert_eq!(inspection.transaction.applied_operations(), 0);
    assert_eq!(
        inspection
            .transaction
            .accepted_operations()
            .accepted_operations(),
        2
    );
    assert_eq!(inspection.operation, None);
    assert_eq!(inspection.report.operations().len(), 2);
    assert_eq!(inspection.report.position().accepted_operations(), 2);
    assert!(inspection.report.completeness().is_complete());
    assert_eq!(
        inspection.report.domain(),
        &DomainName::parse("default").assured("the fixture domain is an accepted literal")
    );

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn repeated_inspection_of_an_unchanged_transaction_reads_the_same_basis() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    two_operation_transaction(&service, &mut subscriptions).await;
    let request = inspect(TransactionInspectionTarget::Attached);
    let session = InspectingSession::from(&subscriptions);

    let first = inspected(service.inspect_transaction(&request, session).await);
    let second = inspected(service.inspect_transaction(&request, session).await);

    assert_eq!(
        first.report.planning_basis(),
        second.report.planning_basis()
    );
    assert_eq!(first.report, second.report);

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn an_unfinished_model_run_reads_as_an_incomplete_open_report() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    run(&service, &mut subscriptions, "BEGIN;", None).await;
    run(
        &service,
        &mut subscriptions,
        "CREATE RELAY inspected_relay SCHEMA inspected_missing UNBRANCHED;",
        Some(0),
    )
    .await;

    let inspection = inspected(
        service
            .inspect_transaction(
                &inspect(TransactionInspectionTarget::Attached),
                InspectingSession::from(&subscriptions),
            )
            .await,
    );

    assert!(
        !inspection.report.completeness().is_complete(),
        "a model run still missing a referenced schema is not a complete scope"
    );
    assert!(
        !inspection.report.completeness().diagnostics().is_empty(),
        "an incomplete report says what it could not resolve"
    );
    assert_eq!(inspection.report.position().accepted_operations(), 1);

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn inspecting_another_owned_transaction_changes_no_binding_or_queue_position() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    two_operation_transaction(&service, &mut subscriptions).await;
    let attached = subscriptions
        .transaction_id()
        .verified("the fixture bound its transaction to this session")
        .to_string();

    let other_id = "inspected-by-identity".to_string();
    let other = ReplicatedTransaction::open(
        other_id.clone(),
        DomainName::parse("default").assured("the fixture domain is an accepted literal"),
        subscriptions.user.clone(),
        service.transaction_activity(),
    );
    service
        .inner
        .consensus
        .open_transaction(other, DEFAULT_TRANSACTION_MAX_OPEN)
        .await
        .assured("the second test transaction is unique and below the admission limit");
    let before = service
        .inner
        .consensus
        .current_transaction(&other_id)
        .await
        .verified("the second transaction was just opened");

    let inspection = inspected(
        service
            .inspect_transaction(
                &inspect(by_id(&other_id)),
                InspectingSession::from(&subscriptions),
            )
            .await,
    );

    assert_eq!(inspection.transaction.transaction_id(), other_id);
    assert_eq!(inspection.report.position().accepted_operations(), 0);
    assert_eq!(
        subscriptions.transaction_id(),
        Some(attached.as_str()),
        "inspecting by identity must not take the session's binding"
    );
    let after = service
        .inner
        .consensus
        .current_transaction(&other_id)
        .await
        .verified("the inspected transaction is still open");
    assert_eq!(after.statements.len(), before.statements.len());
    assert_eq!(after.statement_count, before.statement_count);
    let (TransactionState::Open(before_activity), TransactionState::Open(after_activity)) =
        (&before.state, &after.state)
    else {
        panic!("both observations must see an open transaction");
    };
    assert_eq!(
        after_activity.last_activity_at(),
        before_activity.last_activity_at(),
        "inspecting by identity must not touch activity time"
    );
    assert!(
        !service.inner.transaction_bindings.contains_key(&other_id),
        "inspecting by identity must not bind the inspected transaction"
    );

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn inspecting_without_an_attached_transaction_is_refused() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();

    let outcome = service
        .inspect_transaction(
            &inspect(TransactionInspectionTarget::Attached),
            InspectingSession::from(&subscriptions),
        )
        .await;

    assert_eq!(
        rejection(outcome),
        TransactionInspectionRejection::NoAttachedTransaction
    );

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn inspecting_an_unknown_transaction_is_refused() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();

    let outcome = service
        .inspect_transaction(
            &inspect(by_id("no-such-transaction")),
            InspectingSession::from(&subscriptions),
        )
        .await;

    assert_eq!(
        rejection(outcome),
        TransactionInspectionRejection::TransactionNotFound
    );

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn inspecting_a_transaction_owned_by_another_user_is_refused() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    let other_id = "owned-elsewhere".to_string();
    let other = ReplicatedTransaction::open(
        other_id.clone(),
        DomainName::parse("default").assured("the fixture domain is an accepted literal"),
        UserName::parse("auditor").assured("the other owner is an accepted literal"),
        service.transaction_activity(),
    );
    service
        .inner
        .consensus
        .open_transaction(other, DEFAULT_TRANSACTION_MAX_OPEN)
        .await
        .assured("the other user's transaction is unique and below the admission limit");

    let outcome = service
        .inspect_transaction(
            &inspect(by_id(&other_id)),
            InspectingSession::from(&subscriptions),
        )
        .await;

    assert_eq!(rejection(outcome), TransactionInspectionRejection::NotOwner);

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn selecting_an_operation_names_it_without_narrowing_the_report() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    two_operation_transaction(&service, &mut subscriptions).await;

    let inspection = inspected(
        service
            .inspect_transaction(
                &TransactionInspectionRequest {
                    target: TransactionInspectionTarget::Attached,
                    operation: Some(operation(2)),
                },
                InspectingSession::from(&subscriptions),
            )
            .await,
    );

    assert_eq!(inspection.operation, Some(operation(2)));
    assert_eq!(
        inspection.report.operations().len(),
        2,
        "selecting an operation must not truncate the report it points into"
    );
    let selected = inspection
        .report
        .operations()
        .iter()
        .find(|report| report.number == operation(2))
        .verified("the selected operation is within the inspected report");
    assert!(selected.execution_step.contains(operation(2)));

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn selecting_an_operation_past_the_accepted_position_is_refused() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    two_operation_transaction(&service, &mut subscriptions).await;

    let outcome = service
        .inspect_transaction(
            &TransactionInspectionRequest {
                target: TransactionInspectionTarget::Attached,
                operation: Some(operation(3)),
            },
            InspectingSession::from(&subscriptions),
        )
        .await;

    assert_eq!(
        rejection(outcome),
        TransactionInspectionRejection::OperationNotFound
    );

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn inspecting_a_committed_transaction_reads_its_frozen_report_and_recorded_outcomes() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    two_operation_transaction(&service, &mut subscriptions).await;
    let committed_id = subscriptions
        .transaction_id()
        .verified("the fixture bound its transaction to this session")
        .to_string();
    run(&service, &mut subscriptions, "COMMIT;", None).await;

    let inspection = inspected(
        service
            .inspect_transaction(
                &inspect(by_id(&committed_id)),
                InspectingSession::from(&subscriptions),
            )
            .await,
    );

    assert_eq!(
        inspection.transaction.lifecycle(),
        &TransactionLifecycle::Committed
    );
    assert_eq!(inspection.transaction.applied_operations(), 2);
    assert_eq!(inspection.transaction.pending_operations(), 0);
    assert_eq!(inspection.report.operations().len(), 2);
    let [step] = inspection.report.execution_steps() else {
        panic!("both consecutive schema operations execute as one atomic step");
    };
    assert!(matches!(
        step.actual().outcome,
        ExecutionStepOutcome::Applied
    ));

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn inspecting_a_transaction_that_never_planned_an_operation_is_refused() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    run(&service, &mut subscriptions, "BEGIN;", None).await;
    let reverted_id = subscriptions
        .transaction_id()
        .verified("the fixture bound its transaction to this session")
        .to_string();
    run(&service, &mut subscriptions, "REVERT;", None).await;

    let outcome = service
        .inspect_transaction(
            &inspect(by_id(&reverted_id)),
            InspectingSession::from(&subscriptions),
        )
        .await;

    assert_eq!(
        rejection(outcome),
        TransactionInspectionRejection::ReportUnavailable
    );

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}
