//! Transaction control-plane tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Focused transaction lifecycle and recovery assertions.
//! - **Depends on.** Transaction control-plane internals and test fixtures.
//! - **Must not know.** Production ownership beyond the parent module under test.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::CommandRequest;
use nervix_consensus::{
    ReplicatedTransaction, TransactionActivity, TransactionOutcome, TransactionState,
};
use nervix_models::{
    CreateRelay, CreateSchema, DomainName, ExecutionStepOutcome, ModelName, Timestamp,
    TransactionLifecycle, TransactionOperationNumber, TransactionPosition,
};

use super::{
    super::{
        command_result::CommandDiagnostic,
        subscription::SessionSubscriptions,
        test_fixtures::{
            TestService, build_test_service, command_transaction_state, create_test_domain, named,
            test_execution_reference,
        },
    },
    DEFAULT_TRANSACTION_MAX_OPEN, TransactionAttachment,
};

#[test]
fn planning_error_message_includes_attached_validation_detail() {
    let operation = TransactionOperationNumber::from_index(0)
        .assured("the first test transaction operation is addressable");
    let error =
        error_stack::Report::new(super::TransactionPlanningError::ExternalModelValidation {
            operation,
        })
        .attach("paced ingestor requires TIMESTAMP NOW".to_string());

    assert_eq!(
        super::transaction_planning_error_message(&error),
        "transaction operation 1 failed external model validation: paced ingestor requires \
         TIMESTAMP NOW"
    );
}

#[tokio::test]
async fn attaching_an_overdue_transaction_atomically_expires_it() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    let id = "overdue-attach".to_string();
    let activity =
        TransactionActivity::from_timeout(Timestamp::from_unix_nanos(1), Duration::from_nanos(1));
    let transaction = ReplicatedTransaction::open(
        id.clone(),
        DomainName::parse("default").assured("the test domain is an accepted literal"),
        subscriptions.user.clone(),
        activity,
    );
    service
        .inner
        .consensus
        .open_transaction(transaction, DEFAULT_TRANSACTION_MAX_OPEN)
        .await
        .assured("the overdue test transaction is unique and below the admission limit");
    service
        .inner
        .transaction_bindings
        .insert(id.clone(), "former-session".to_string());

    let attached = service
        .attach_transaction(id.clone(), &mut subscriptions)
        .await;

    let TransactionAttachment::AlreadyFinished {
        transaction,
        message,
        ..
    } = attached
    else {
        panic!("an expired transaction is reported finished, found {attached:?}");
    };
    assert!(message.contains("finished with outcome EXPIRED"));
    assert_eq!(transaction.lifecycle(), &TransactionLifecycle::Expired);
    assert!(!subscriptions.transaction_active());
    assert!(!service.inner.transaction_bindings.contains_key(&id));
    let expired = service
        .inner
        .consensus
        .current_transaction(&id)
        .await
        .verified("the expired transaction remains as a retained tombstone");
    assert!(matches!(
        expired.finished_outcome(),
        Some(TransactionOutcome::Expired)
    ));

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn transaction_recovery_rotates_fairly_after_the_last_considered_identity() {
    let recovery = super::TransactionRecovery::default();
    let owner =
        nervix_models::UserName::parse("operator").assured("the test owner is an accepted literal");
    let domain = DomainName::parse("default").assured("the test domain is an accepted literal");
    let mut transactions = std::collections::BTreeMap::new();
    for id in ["a", "b", "c"] {
        let activity = TransactionActivity::from_timeout(
            Timestamp::from_unix_nanos(1),
            Duration::from_secs(1),
        );
        let mut transaction =
            ReplicatedTransaction::open(id.to_string(), domain.clone(), owner.clone(), activity);
        transaction.state =
            TransactionState::Committing(Box::new(nervix_consensus::TransactionCommitProgress {
                last_activity_at: Timestamp::from_unix_nanos(1),
                next_statement: 0,
                results: Vec::new(),
                applying: None,
                domain_mutation: None,
            }));
        transactions.insert(id.to_string(), transaction);
    }

    assert_eq!(recovery.candidates(&transactions), ["a", "b", "c"]);
    recovery.considered("a".to_string());
    assert_eq!(recovery.candidates(&transactions), ["b", "c", "a"]);
    recovery.considered("c".to_string());
    assert_eq!(recovery.candidates(&transactions), ["a", "b", "c"]);
    assert_eq!(
        recovery.permits.available_permits(),
        super::TRANSACTION_RECOVERY_CONCURRENCY
    );
}

#[tokio::test]
async fn process_command_commits_explicit_transaction_without_trailing_semicolon() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(false).await;
    create_test_domain(&service.inner.consensus, "prod").await;
    let mut subscriptions = SessionSubscriptions::new();

    let result = service
        .test_command(
            CommandRequest {
                query: "BEGIN; CREATE RELAY notifications SCHEMA notification UNBRANCHED; CREATE \
                        SCHEMA notification ( user_id U32 ); COMMIT"
                    .to_string(),
                domain: Some(named("prod")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;

    assert!(
        result.succeeded(),
        "command must succeed: {}",
        result.message
    );
    assert_eq!(
        command_transaction_state(&result),
        Some(TransactionLifecycle::Committed)
    );
    assert!(result.message.contains("quiesce level: DYNAMIC"));
    let commit = result
        .statements
        .last()
        .expect("COMMIT result must be retained");
    assert_eq!(commit.message, "quiesce level: DYNAMIC");
    let Some(status) = &result.transaction else {
        panic!("the committed command must carry its transaction status");
    };
    let Some(transaction) = service
        .inner
        .consensus
        .current_transaction(status.transaction_id())
        .await
    else {
        panic!("the committed transaction must remain as a retained tombstone");
    };
    let [step] = transaction.commit_results() else {
        panic!("both consecutive model operations must execute as one atomic step");
    };
    assert_eq!(step.operation_range().first().get(), 1);
    assert_eq!(step.operation_range().last().get(), 2);
    assert!(step.impact.planned().completeness.is_complete());
    assert!(matches!(
        step.impact.actual().outcome,
        ExecutionStepOutcome::Applied
    ));

    let schema = registry
        .get::<CreateSchema>(
            &DomainName::parse("prod").expect("valid domain"),
            named::<ModelName>("notification"),
        )
        .expect("registry get should succeed");
    assert!(
        schema.is_some(),
        "batch should create schema in prod domain"
    );
    let relay = registry
        .get::<CreateRelay>(
            &DomainName::parse("prod").expect("valid domain"),
            named::<ModelName>("notifications"),
        )
        .expect("registry get should succeed");
    assert!(
        relay.is_some(),
        "model create batch should resolve relay references atomically"
    );

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn process_command_queues_transaction_across_requests_and_reverts() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();

    let begin = service
        .test_command(
            CommandRequest {
                query: "BEGIN;".to_string(),
                domain: Some(named("default")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;
    assert!(begin.succeeded());
    assert_eq!(
        command_transaction_state(&begin),
        Some(TransactionLifecycle::Open)
    );

    let queued = service
        .test_command(
            CommandRequest {
                query: "CREATE SCHEMA queued_event ( user_id U32 );".to_string(),
                domain: Some(named("default")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: Some(TransactionPosition::new(0)),
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;
    assert!(queued.succeeded());
    assert_eq!(queued.message, "quiesce level: DYNAMIC");
    assert_eq!(
        command_transaction_state(&queued),
        Some(TransactionLifecycle::Open)
    );
    assert!(
        registry
            .get::<CreateSchema>(
                &DomainName::parse("default").expect("valid domain"),
                named::<ModelName>("queued_event"),
            )
            .expect("registry get should succeed")
            .is_none(),
        "queued command must not execute before COMMIT"
    );

    let reverted = service
        .test_command(
            CommandRequest {
                query: "REVERT;".to_string(),
                domain: Some(named("default")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;
    assert!(reverted.succeeded());
    assert!(
        reverted
            .message
            .starts_with("transaction reverted: dropped 1 command(s); id '")
    );
    assert_eq!(
        command_transaction_state(&reverted),
        Some(TransactionLifecycle::Reverted)
    );
    let reverted_status = reverted
        .transaction
        .as_ref()
        .verified("a successful revert reports its terminal transaction");
    let replayed = service
        .revert_identified_transaction(
            reverted_status.transaction_id().to_string(),
            subscriptions.user.clone(),
            service.transaction_activity(),
        )
        .await;
    assert!(replayed.succeeded());
    assert_eq!(replayed.message, reverted.message);
    assert_eq!(
        command_transaction_state(&replayed),
        Some(TransactionLifecycle::Reverted)
    );
    assert!(
        registry
            .get::<CreateSchema>(
                &DomainName::parse("default").expect("valid domain"),
                named::<ModelName>("queued_event"),
            )
            .expect("registry get should succeed")
            .is_none(),
        "reverted command must not persist"
    );

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn process_command_rejects_begin_inside_begin() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();

    let begin = service
        .test_command(
            CommandRequest {
                query: "BEGIN;".to_string(),
                domain: Some(named("default")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;
    assert!(begin.succeeded());
    assert_eq!(
        command_transaction_state(&begin),
        Some(TransactionLifecycle::Open)
    );

    let nested = service
        .test_command(
            CommandRequest {
                query: "BEGIN;".to_string(),
                domain: Some(named("default")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;
    assert!(!nested.succeeded());
    assert_eq!(nested.message, "transaction is already active");
    assert_eq!(
        command_transaction_state(&nested),
        Some(TransactionLifecycle::Open)
    );

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn process_command_rejects_domain_and_user_creation_inside_a_transaction() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();

    for query in [
        "BEGIN; CREATE DOMAIN alpha; COMMIT",
        "BEGIN; CREATE USER alpha WITH PASSWORD 'secret'; COMMIT",
    ] {
        let result = service
            .test_command(
                CommandRequest {
                    query: query.to_string(),
                    domain: Some(named("default")),
                    execution_reference: test_execution_reference(),
                    expected_transaction_position: None,
                    expected_preview: None,
                },
                &mut subscriptions,
            )
            .await;

        assert!(!result.succeeded(), "'{query}' must be rejected");
        assert!(
            result.message.contains("cannot be queued in a transaction"),
            "'{query}' produced: {}",
            result.message
        );

        let reverted = service
            .test_command(
                CommandRequest {
                    query: "REVERT;".to_string(),
                    domain: Some(named("default")),
                    execution_reference: test_execution_reference(),
                    expected_transaction_position: None,
                    expected_preview: None,
                },
                &mut subscriptions,
            )
            .await;
        assert!(
            reverted.succeeded(),
            "revert must succeed: {}",
            reverted.message
        );
    }

    assert!(
        service
            .inner
            .consensus
            .current_domain(&DomainName::parse("alpha").expect("valid domain"))
            .await
            .is_none(),
        "a rejected CREATE DOMAIN must not reach the control plane"
    );

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn process_command_rejects_begin_without_an_existing_domain() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    let mut subscriptions = SessionSubscriptions::new();

    let missing = service
        .test_command(
            CommandRequest {
                query: "BEGIN;".to_string(),
                domain: Some(named("absent")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;
    assert!(!missing.succeeded());
    assert_eq!(missing.message, "domain 'absent' does not exist");
    assert!(!subscriptions.transaction_active());

    let unselected = service
        .test_command(
            CommandRequest {
                query: "BEGIN;".to_string(),
                domain: None,
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;
    assert!(!unselected.succeeded());
    assert_eq!(unselected.message, "no active domain selected");
    assert!(!subscriptions.transaction_active());

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn process_command_rejects_statements_selecting_another_domain() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    create_test_domain(&service.inner.consensus, "other").await;
    let mut subscriptions = SessionSubscriptions::new();

    let begin = service
        .test_command(
            CommandRequest {
                query: "BEGIN;".to_string(),
                domain: Some(named("default")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;
    assert!(begin.succeeded(), "begin must succeed: {}", begin.message);
    assert_eq!(
        begin
            .transaction
            .as_ref()
            .map(|status| status.domain().as_str()),
        Some("default")
    );

    let foreign = service
        .test_command(
            CommandRequest {
                query: "CREATE SCHEMA foreign_event ( user_id U32 );".to_string(),
                domain: Some(named("other")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: Some(TransactionPosition::new(0)),
                expected_preview: None,
            },
            &mut subscriptions,
        )
        .await;
    assert!(!foreign.succeeded());
    assert!(
        foreign.message.contains("is bound to domain 'default'"),
        "unexpected message: {}",
        foreign.message
    );
    let transaction = service
        .inner
        .consensus
        .current_transaction(
            subscriptions
                .transaction_id()
                .expect("the transaction must stay attached"),
        )
        .await
        .expect("open transaction must remain replicated");
    assert_eq!(transaction.pending_statement_count(), 0);

    subscriptions.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[tokio::test]
async fn attaching_to_committed_transaction_returns_the_recorded_aggregate() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    create_test_domain(&service.inner.consensus, "attach_results").await;
    let mut owner = SessionSubscriptions::new();

    let committed = service
        .test_command(
            CommandRequest {
                query: "BEGIN; CREATE SCHEMA notification ( user_id U32 ); COMMIT".to_string(),
                domain: Some(named("attach_results")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut owner,
        )
        .await;
    assert!(
        committed.succeeded(),
        "commit must succeed: {}",
        committed.message
    );
    let transaction_id = committed
        .transaction
        .as_ref()
        .expect("commit result must carry transaction status")
        .transaction_id()
        .to_string();

    let mut observer = SessionSubscriptions::new();
    let attached = service
        .attach_transaction(transaction_id, &mut observer)
        .await;

    let TransactionAttachment::AlreadyFinished {
        transaction,
        message,
        diagnostics,
    } = attached
    else {
        panic!("finished transaction attach must be terminal, found {attached:?}");
    };
    assert_eq!(transaction.lifecycle(), &TransactionLifecycle::Committed);
    assert!(message.contains("finished with outcome COMMITTED"));
    assert_eq!(
        diagnostics,
        [CommandDiagnostic::unlocated(
            "quiesce level: DYNAMIC".to_string()
        )]
    );

    owner.stop_all().await;
    observer.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}
