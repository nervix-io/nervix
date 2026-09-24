//! `DESCRIBE TRANSACTION` session tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that the statement is answered before transaction queueing, leaves the
//!   caller's binding and queue position as they were, and carries the typed report beside its
//!   rendering.
//! - **Depends on.** The session command path and the session test fixtures.
//! - **Must not know.** Production ownership beyond the parent module under test.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::CommandRequest;
use nervix_models::{TransactionLifecycle, TransactionPosition, UserName};

use crate::application::{
    SessionServiceImpl,
    command_result::CommandResult,
    subscription::SessionSubscriptions,
    test_fixtures::{TestService, build_test_service, test_command_request},
};

async fn execute(
    service: &SessionServiceImpl,
    subscriptions: &mut SessionSubscriptions,
    query: &str,
    expected_transaction_position: Option<usize>,
) -> CommandResult {
    service
        .test_command(
            CommandRequest {
                expected_transaction_position: expected_transaction_position
                    .map(TransactionPosition::new),
                ..test_command_request(query, "default")
            },
            subscriptions,
        )
        .await
}

async fn accepted(
    service: &SessionServiceImpl,
    subscriptions: &mut SessionSubscriptions,
    query: &str,
    expected_transaction_position: Option<usize>,
) -> CommandResult {
    let result = execute(service, subscriptions, query, expected_transaction_position).await;
    assert!(
        result.succeeded(),
        "{query:?} should be accepted: {result:?}"
    );
    result
}

/// An open transaction holding two queued schema operations, returning its identity.
async fn two_operation_transaction(
    service: &SessionServiceImpl,
    subscriptions: &mut SessionSubscriptions,
) -> String {
    accepted(service, subscriptions, "BEGIN;", None).await;
    accepted(
        service,
        subscriptions,
        "CREATE SCHEMA described_first ( user_id U32 );",
        Some(0),
    )
    .await;
    accepted(
        service,
        subscriptions,
        "CREATE SCHEMA described_second ( order_id U32 );",
        Some(1),
    )
    .await;
    subscriptions
        .transaction_id()
        .verified("BEGIN bound the transaction to this session")
        .to_string()
}

async fn queued_statements(service: &SessionServiceImpl, transaction_id: &str) -> usize {
    service
        .inner
        .consensus
        .current_transaction(transaction_id)
        .await
        .verified("the fixture transaction stays open")
        .statements
        .len()
}

#[tokio::test]
async fn describing_the_attached_transaction_neither_queues_nor_moves_its_position() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    let transaction_id = two_operation_transaction(&service, &mut subscriptions).await;

    let described = accepted(
        &service,
        &mut subscriptions,
        "DESCRIBE TRANSACTION;",
        Some(2),
    )
    .await;

    assert!(
        described
            .message
            .lines()
            .any(|line| line == format!("transaction: {transaction_id}")),
        "{}",
        described.message
    );
    assert!(described.message.lines().any(|line| line == "state: OPEN"));
    assert!(
        described
            .message
            .lines()
            .any(|line| line == "operations: 2 accepted, 0 applied, 2 pending")
    );
    let inspection = described
        .inspection
        .as_ref()
        .verified("an inspection result carries the typed envelope");
    let inspected = &inspection.transaction;
    assert_eq!(inspected.transaction_id(), transaction_id);
    assert_eq!(inspected.lifecycle(), &TransactionLifecycle::Open);
    assert_eq!(inspected.accepted_operations(), TransactionPosition::new(2));
    assert_eq!(inspection.operation, None);
    let binding = described
        .transaction
        .as_ref()
        .verified("the result reports the caller's own binding");
    assert_eq!(binding.transaction_id(), transaction_id);
    assert_eq!(queued_statements(&service, &transaction_id).await, 2);

    let appended = accepted(
        &service,
        &mut subscriptions,
        "CREATE SCHEMA described_third ( note STRING );",
        Some(2),
    )
    .await;
    let admission = appended
        .transaction_admission
        .verified("an accepted append reports its operation number");
    assert_eq!(
        admission.operation.get(),
        3,
        "an inspection consumes no operation number"
    );

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn a_selected_operation_in_json_carries_the_same_typed_report() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    let transaction_id = two_operation_transaction(&service, &mut subscriptions).await;

    let described = accepted(
        &service,
        &mut subscriptions,
        "describe transaction operation 2 format json;",
        Some(2),
    )
    .await;

    let document: serde_json::Value =
        serde_json::from_str(&described.message).assured("FORMAT JSON prints one JSON document");
    assert_eq!(document["transaction"]["transaction_id"], transaction_id);
    assert_eq!(document["transaction"]["state"], "OPEN");
    assert_eq!(document["operation"], 2);
    let inspection = described
        .inspection
        .as_ref()
        .verified("JSON keeps the typed envelope beside its rendering");
    let operation = inspection.operation.map(|operation| operation.get());
    assert_eq!(operation, Some(2));
    let report =
        serde_json::to_value(&inspection.report).assured("an impact report serializes to JSON");
    assert_eq!(report, document["report"]);

    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn describe_transaction_is_refused_inside_a_multi_statement_request() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    let transaction_id = two_operation_transaction(&service, &mut subscriptions).await;

    let appended_with_inspection = execute(
        &service,
        &mut subscriptions,
        "CREATE SCHEMA described_third ( note STRING ); DESCRIBE TRANSACTION;",
        Some(2),
    )
    .await;
    assert!(!appended_with_inspection.succeeded());
    assert!(
        appended_with_inspection
            .message
            .contains("DESCRIBE TRANSACTION must be executed separately"),
        "{}",
        appended_with_inspection.message
    );
    assert_eq!(
        queued_statements(&service, &transaction_id).await,
        2,
        "a refused request appends nothing"
    );

    let mut fresh = SessionSubscriptions::new();
    let opened_with_inspection =
        execute(&service, &mut fresh, "BEGIN; DESCRIBE TRANSACTION;", None).await;
    assert!(!opened_with_inspection.succeeded());
    assert_eq!(
        fresh.transaction_id(),
        None,
        "a refused request opens no transaction"
    );

    fresh.stop_all(&service).await;
    subscriptions.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn describing_by_identity_leaves_the_inspecting_session_unbound() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut owner = SessionSubscriptions::new();
    let transaction_id = two_operation_transaction(&service, &mut owner).await;
    let mut observer = SessionSubscriptions::new();

    let described = accepted(
        &service,
        &mut observer,
        &format!("DESCRIBE TRANSACTION '{transaction_id}' OPERATION 1;"),
        None,
    )
    .await;

    assert!(
        described
            .message
            .lines()
            .any(|line| line == "inspected operation: 1"),
        "{}",
        described.message
    );
    let inspection = described
        .inspection
        .as_ref()
        .verified("an inspection result carries the typed envelope");
    assert_eq!(inspection.transaction.transaction_id(), transaction_id);
    assert_eq!(
        described.transaction, None,
        "the observer holds no binding, and inspecting by identity does not create one"
    );
    assert_eq!(observer.transaction_id(), None);
    assert_eq!(owner.transaction_id(), Some(transaction_id.as_str()));
    assert_eq!(queued_statements(&service, &transaction_id).await, 2);

    observer.stop_all(&service).await;
    owner.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}

/// Runs an inspection that must be refused and returns why.
async fn refusal(
    service: &SessionServiceImpl,
    subscriptions: &mut SessionSubscriptions,
    query: &str,
    expected_transaction_position: Option<usize>,
) -> String {
    let result = execute(service, subscriptions, query, expected_transaction_position).await;
    assert!(!result.succeeded(), "{query:?} must be refused");
    assert_eq!(result.inspection, None, "a refusal carries no report");
    result.message
}

#[tokio::test]
async fn a_refused_inspection_names_why_nothing_was_read() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut owner = SessionSubscriptions::new();
    let transaction_id = two_operation_transaction(&service, &mut owner).await;
    let mut unbound = SessionSubscriptions::new();
    let mut intruder = SessionSubscriptions::for_user(
        UserName::parse("intruder").assured("the fixture user name is an accepted literal"),
    );

    assert_eq!(
        refusal(&service, &mut unbound, "DESCRIBE TRANSACTION;", None).await,
        "no transaction is attached to this session"
    );
    assert_eq!(
        refusal(
            &service,
            &mut unbound,
            "DESCRIBE TRANSACTION 'reclaimed';",
            None
        )
        .await,
        "transaction 'reclaimed' is unknown"
    );
    assert_eq!(
        refusal(
            &service,
            &mut owner,
            "DESCRIBE TRANSACTION OPERATION 3;",
            Some(2)
        )
        .await,
        format!(
            "transaction '{transaction_id}' accepted 2 operation(s), so operation 3 does not exist"
        )
    );
    assert_eq!(
        refusal(
            &service,
            &mut intruder,
            &format!("DESCRIBE TRANSACTION '{transaction_id}';"),
            None
        )
        .await,
        format!("transaction '{transaction_id}' belongs to another user")
    );
    assert_eq!(queued_statements(&service, &transaction_id).await, 2);
    assert_eq!(owner.transaction_id(), Some(transaction_id.as_str()));

    intruder.stop_all(&service).await;
    unbound.stop_all(&service).await;
    owner.stop_all(&service).await;
    let _ = std::fs::remove_dir_all(&path);
}
