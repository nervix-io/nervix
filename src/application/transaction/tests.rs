//! Transaction control-plane tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Focused transaction lifecycle and recovery assertions.
//! - **Depends on.** Transaction control-plane internals and test fixtures.
//! - **Must not know.** Production ownership beyond the parent module under test.

use std::time::Duration;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::CommandRequest;
use nervix_consensus::{
    ConsensusError, ConsensusTransactionError, ReplicatedTransaction, TransactionActivity,
    TransactionMutationError, TransactionOutcome, TransactionState,
};
#[cfg(feature = "testing")]
use nervix_consensus::{ConsensusTestProbe, StorageBoundary};
use nervix_models::{
    CreateRelay, CreateSchema, DomainName, ExecutionStepOutcome, ImpactPlanningBasis, ModelName,
    Timestamp, TransactionLifecycle, TransactionOperationNumber, TransactionPosition,
    TransactionPreviewIdentity,
};

#[cfg(feature = "testing")]
use super::super::test_fixtures::{build_test_service_with_probe, test_command_request};
use super::{
    super::{
        command_result::CommandDiagnostic,
        subscription::SessionSubscriptions,
        test_fixtures::{
            TestService, build_test_service, command_transaction_state, create_test_domain, named,
            test_execution_reference,
        },
    },
    TransactionAttachment, TransactionLimits,
};
#[cfg(feature = "testing")]
use crate::captured_logs::CapturedLogs;

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

#[test]
fn transaction_commit_proposal_retains_consensus_failure_for_display_and_redirect() {
    let error = Report::new(ConsensusError::LeadershipLost {
        observed: nervix_consensus::LeadershipObservation::NoLeader,
    })
    .change_context(ConsensusTransactionError::Consensus)
    .change_context(super::TransactionCommitError::Proposal);

    assert!(matches!(
        error.downcast_ref::<ConsensusError>(),
        Some(ConsensusError::LeadershipLost {
            observed: nervix_consensus::LeadershipObservation::NoLeader,
        })
    ));
    assert_eq!(
        super::transaction_commit_error_message(&error),
        "this node cannot propose as the leader: no leader is known"
    );
}

#[test]
fn transaction_commit_preserves_mutation_conflict_reason() {
    let error = Report::new(ConsensusTransactionError::Mutation(
        TransactionMutationError::StepConflict {
            id: "tx-conflict".to_string(),
            reason: "schedule changed".to_string(),
        },
    ))
    .change_context(super::TransactionCommitError::Proposal);

    assert_eq!(
        error.current_context().planning_input_conflict(&error),
        Some("schedule changed")
    );
    assert_eq!(
        super::transaction_commit_error_message(&error),
        "transaction 'tx-conflict' commit step conflicted with replicated state: schedule changed"
    );

    let planning = Report::new(super::TransactionCommitError::PlanningInputsChanged {
        id: "tx-conflict".to_string(),
        reason: "schema changed".to_string(),
    });
    assert_eq!(
        planning
            .current_context()
            .planning_input_conflict(&planning),
        Some("schema changed")
    );
}

#[test]
fn transaction_commit_message_uses_the_owning_error_without_a_nested_cause() {
    let error = Report::new(super::TransactionCommitError::TaskJoin {
        id: "tx-join".to_string(),
    });
    assert_eq!(
        error.current_context().planning_input_conflict(&error),
        None
    );
    assert_eq!(
        super::transaction_commit_error_message(&error),
        "transaction 'tx-join' commit task failed"
    );
}

#[nervix_primitives::test]
async fn transaction_consensus_response_keeps_storage_and_mutation_classifications() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;

    let storage =
        Report::new(ConsensusError::Storage).change_context(ConsensusTransactionError::Consensus);
    let response = service.transaction_consensus_error_response(storage).await;
    assert!(!response.succeeded(), "{response:?}");
    assert!(
        response.message.contains("consensus storage"),
        "{response:?}"
    );

    let missing_cause = Report::new(ConsensusTransactionError::Consensus);
    let response = service
        .transaction_consensus_error_response(missing_cause)
        .await;
    assert!(!response.succeeded(), "{response:?}");
    assert!(
        response.message.contains("consensus proposal failed"),
        "{response:?}"
    );

    let mutation = Report::new(ConsensusTransactionError::Mutation(
        TransactionMutationError::StepConflict {
            id: "tx-conflict".to_string(),
            reason: "schedule changed".to_string(),
        },
    ));
    let response = service.transaction_consensus_error_response(mutation).await;
    assert!(!response.succeeded(), "{response:?}");
    assert!(
        response.message.contains("schedule changed"),
        "{response:?}"
    );

    let mutation = Report::new(ConsensusTransactionError::Mutation(
        TransactionMutationError::StepConflict {
            id: "tx-conflict".to_string(),
            reason: "schedule changed".to_string(),
        },
    ));
    let response = service
        .transaction_commit_admission_response(mutation)
        .await;
    assert!(!response.succeeded(), "{response:?}");
    assert!(
        response.message.contains("schedule changed"),
        "{response:?}"
    );

    let expected = Box::new(TransactionPreviewIdentity {
        transaction_id: "tx-preview".to_string(),
        position: TransactionPosition::new(1),
        planning_basis: ImpactPlanningBasis::new([1; 32]),
    });
    let current = Box::new(TransactionPreviewIdentity {
        transaction_id: "tx-preview".to_string(),
        position: TransactionPosition::new(2),
        planning_basis: ImpactPlanningBasis::new([2; 32]),
    });
    let stale = Report::new(ConsensusTransactionError::Mutation(
        TransactionMutationError::PreviewStale { expected, current },
    ));
    let response = service.transaction_commit_admission_response(stale).await;
    assert!(matches!(
        response.disposition,
        super::CommandDisposition::PreviewStale { .. }
    ));
    assert!(
        response.message.contains("nothing was applied"),
        "{response:?}"
    );

    drop(service);
    drop(registry);
    let _ = std::fs::remove_dir_all(path);
}

#[cfg(feature = "testing")]
#[nervix_primitives::test]
async fn empty_transaction_commit_storage_failure_is_reported() {
    let probe = ConsensusTestProbe::default();
    let TestService {
        service,
        registry,
        path,
    } = build_test_service_with_probe(true, probe.clone()).await;
    let id = "empty-storage-failure".to_string();
    let owner = SessionSubscriptions::new().user;
    let activity = service.transaction_activity();
    let transaction = ReplicatedTransaction::open(
        id.clone(),
        DomainName::parse("default").assured("the test domain name is valid"),
        owner.clone(),
        activity,
    );
    service
        .inner
        .consensus
        .open_transaction(transaction, TransactionLimits::default().max_open())
        .await
        .assured("the test transaction opens below the admission limit");
    probe.storage_fault().fail_next(
        format!("finish-empty-transaction-commit:{id}"),
        StorageBoundary::BeforeCommit,
    );

    let result = service
        .commit_identified_transaction(id.clone(), owner, activity, None)
        .await;
    assert!(!result.succeeded(), "{result:?}");
    assert!(
        matches!(
            result.disposition,
            super::CommandDisposition::OutcomeUnknown(
                crate::application::command_result::OutcomeUnknownCause::StillApplying
            )
        ),
        "the unfinished commit retains its execution for recovery: {result:?}"
    );
    assert!(result.message.contains("consensus storage"), "{result:?}");
    assert_eq!(
        result.diagnostics,
        vec![CommandDiagnostic::unlocated(result.message.clone())],
        "the recoverable reply keeps the execution error diagnostic"
    );
    assert!(matches!(
        service
            .inner
            .consensus
            .current_transaction(&id)
            .await
            .map(|tx| tx.state),
        Some(TransactionState::Committing(_))
    ));

    drop(service);
    drop(registry);
    let _ = std::fs::remove_dir_all(path);
}

/// An obsolete failed attempt cannot answer for the transaction after another coordinator has
/// recorded its outcome. The pause is after a real publication refusal, outside the execution lock.
#[cfg(feature = "testing")]
#[nervix_primitives::test]
async fn explicit_commit_recovers_the_result_recorded_after_its_attempt_failed() {
    use crate::fault_injection::ModelPublicationRefusalKind;

    for finish_with_failure in [false, true] {
        let faults = crate::FaultInjection::default();
        let TestService {
            service,
            registry,
            path,
        } = crate::application::test_fixtures::build_test_service_with_fault_injection(
            true,
            faults.clone(),
        )
        .await;
        let mut session = SessionSubscriptions::new();
        let commands = if finish_with_failure {
            "BEGIN; CREATE SCHEMA explicit_recovered (key I64); CREATE RESOURCE \
             explicit_commit_conflict;"
        } else {
            "BEGIN; CREATE SCHEMA explicit_recovered (key I64);"
        };
        let prepared = service
            .test_command(test_command_request(commands, "default"), &mut session)
            .await;
        assert!(
            prepared.succeeded(),
            "the transaction is queued: {prepared:?}"
        );
        let id = prepared
            .transaction
            .as_ref()
            .assured("BEGIN answers with its transaction identity")
            .transaction_id()
            .to_string();
        let node = service.inner.consensus.local_node_id().clone();
        let domain = DomainName::parse("default").assured("the test domain name is valid");
        let owner = session.user.clone();
        let activity = service.transaction_activity();
        faults.refuse_next_model_publication(domain, ModelPublicationRefusalKind::SaturatedLog);
        faults.pause_explicit_commit_failure(node.clone(), id.clone());

        let original = {
            let service = service.clone();
            let id = id.clone();
            let owner = owner.clone();
            nervix_primitives::task::spawn(async move {
                service
                    .commit_identified_transaction(id, owner, activity, None)
                    .await
            })
        };
        nervix_primitives::time::timeout(
            Duration::from_secs(120),
            faults.wait_for_explicit_commit_failure(&node, &id),
        )
        .await
        .assured("the refused commit reaches its result boundary");
        let pending = service
            .inner
            .consensus
            .current_transaction(&id)
            .await
            .assured("the first attempt admitted the transaction");
        assert!(
            matches!(pending.state, TransactionState::Committing(_)),
            "the refused attempt leaves the transaction ready for recovery: {pending:?}"
        );

        if finish_with_failure {
            let mut contender = SessionSubscriptions::new();
            let conflict = nervix_primitives::time::timeout(
                Duration::from_secs(120),
                service.test_command(
                    test_command_request("CREATE RESOURCE explicit_commit_conflict;", "default"),
                    &mut contender,
                ),
            )
            .await
            .assured("a concurrent resource creation completes while the failed reply is held");
            assert!(
                conflict.succeeded(),
                "the frozen plan now has a conflict: {conflict:?}"
            );
            contender.stop_all().await;
        }

        let successor = nervix_primitives::time::timeout(
            Duration::from_secs(120),
            service.commit_identified_transaction(id.clone(), owner, activity, None),
        )
        .await
        .assured("the successor finishes the admitted transaction");
        assert_eq!(successor.succeeded(), !finish_with_failure, "{successor:?}");
        let status = successor
            .transaction
            .as_ref()
            .assured("the successor reports its transaction");
        if finish_with_failure {
            assert!(
                matches!(status.lifecycle(), TransactionLifecycle::Failed { .. }),
                "{successor:?}"
            );
            assert!(
                !successor.diagnostics.is_empty(),
                "the recorded failure has diagnostics: {successor:?}"
            );
        } else {
            assert_eq!(status.lifecycle(), &TransactionLifecycle::Committed);
            assert!(
                successor.succeeded(),
                "the second coordinator commits: {successor:?}"
            );
        }
        faults.release_explicit_commit_failure(&node, &id);
        let original = original
            .await
            .assured("the first coordinator returns its reply");
        assert_eq!(
            original, successor,
            "the original reply recovers the complete recorded outcome"
        );

        session.stop_all().await;
        drop(service);
        drop(registry);
        let _ = std::fs::remove_dir_all(path);
    }
}

/// A clean close whose revert the leader fails to record reports the revert's whole cause chain,
/// so an operator can tell a refused proposal from a storage failure, and leaves the transaction
/// open for a later attach.
#[cfg(feature = "testing")]
#[nervix_primitives::test]
async fn a_clean_close_revert_that_fails_reports_its_cause_and_leaves_the_transaction_open() {
    let probe = ConsensusTestProbe::default();
    let TestService {
        service,
        registry,
        path,
    } = build_test_service_with_probe(true, probe.clone()).await;
    let id = "clean-close-revert-failure".to_string();
    let mut subscriptions = SessionSubscriptions::new();
    let activity = service.transaction_activity();
    let transaction = ReplicatedTransaction::open(
        id.clone(),
        DomainName::parse("default").assured("the test domain name is valid"),
        subscriptions.user.clone(),
        activity,
    );
    service
        .inner
        .consensus
        .open_transaction(transaction, TransactionLimits::default().max_open())
        .await
        .assured("the test transaction opens below the admission limit");
    let attachment = service
        .attach_transaction(id.clone(), &mut subscriptions)
        .await;
    assert!(
        matches!(attachment, TransactionAttachment::Attached { .. }),
        "the owner's session attaches its open transaction: {attachment:?}"
    );
    probe.storage_fault().fail_next(
        format!("revert-transaction:{id}"),
        StorageBoundary::BeforeCommit,
    );

    let logs = CapturedLogs::default();
    // `#[nervix_primitives::test]` runs the whole future on the calling thread, so the thread-local
    // default the guard installs stays in force across the await.
    let guard = tracing::subscriber::set_default(logs.subscriber());
    service.clean_close_transaction(&mut subscriptions).await;
    drop(guard);
    let logs = logs.contents();

    assert!(
        logs.contains("failed to revert transaction during clean session close"),
        "the failed revert must be reported: {logs}"
    );
    assert!(
        logs.contains("consensus proposal failed: consensus storage failed"),
        "the report must carry the revert's cause, not only its outermost context: {logs}"
    );
    assert!(
        matches!(
            service
                .inner
                .consensus
                .current_transaction(&id)
                .await
                .map(|transaction| transaction.state),
            Some(TransactionState::Open(_))
        ),
        "a revert that was not recorded leaves the transaction open"
    );
    assert_eq!(
        subscriptions.transaction_id(),
        None,
        "the closed session releases its binding"
    );

    drop(service);
    drop(registry);
    let _ = std::fs::remove_dir_all(path);
}

#[nervix_primitives::test]
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
        .open_transaction(transaction, TransactionLimits::default().max_open())
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

#[nervix_primitives::test]
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

#[nervix_primitives::test]
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

#[nervix_primitives::test]
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

#[nervix_primitives::test]
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

#[nervix_primitives::test]
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

#[nervix_primitives::test]
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

#[nervix_primitives::test]
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

/// Commits a one-schema transaction for `owner` in `domain` and returns its identity.
async fn commit_owner_transaction(
    service: &crate::application::SessionServiceImpl,
    domain: &str,
    owner: &mut SessionSubscriptions,
) -> String {
    let committed = service
        .test_command(
            CommandRequest {
                query: "BEGIN; CREATE SCHEMA notification ( user_id U32 ); COMMIT".to_string(),
                domain: Some(named(domain)),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            owner,
        )
        .await;
    assert!(
        committed.succeeded(),
        "commit must succeed: {}",
        committed.message
    );
    committed
        .transaction
        .as_ref()
        .assured("a commit result carries its transaction status")
        .transaction_id()
        .to_string()
}

#[nervix_primitives::test]
async fn a_session_that_held_a_transaction_another_commit_finished_commits_it_after_reconciliation()
{
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    create_test_domain(&service.inner.consensus, "held_commit").await;
    let mut owner = SessionSubscriptions::new();
    let transaction_id = commit_owner_transaction(&service, "held_commit", &mut owner).await;
    // A second session attached the transaction before the owner's commit finished it.
    let mut resumed = SessionSubscriptions::new();
    resumed.bind_transaction(transaction_id.clone());
    service
        .inner
        .transaction_bindings
        .insert(transaction_id.clone(), resumed.session_id.clone());

    service.reconcile_transactions_once().await;
    let committed = service
        .test_command(
            CommandRequest {
                query: "COMMIT".to_string(),
                domain: Some(named("held_commit")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut resumed,
        )
        .await;

    assert!(
        committed.succeeded(),
        "the session that held the finished transaction receives its recorded commit: {}",
        committed.message
    );
    let status = committed
        .transaction
        .as_ref()
        .assured("a commit result carries its transaction status");
    assert_eq!(status.lifecycle(), &TransactionLifecycle::Committed);
    assert!(!resumed.transaction_active());
    assert!(
        !service
            .inner
            .transaction_bindings
            .contains_key(&transaction_id)
    );

    owner.stop_all().await;
    resumed.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[nervix_primitives::test]
async fn a_session_releases_a_transaction_after_its_finished_record_is_reclaimed() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    create_test_domain(&service.inner.consensus, "reclaimed_commit").await;
    let mut owner = SessionSubscriptions::new();
    let transaction_id = commit_owner_transaction(&service, "reclaimed_commit", &mut owner).await;

    // A session that attached before another request finished the transaction still holds its
    // binding. Advance the cleanup cutoff beyond the finished record's age, without making the
    // test wait for the configured retention.
    service
        .inner
        .consensus
        .remove_finished_transactions(Timestamp::from_unix_nanos(i64::MAX))
        .await
        .assured("the leader can remove the finished transaction after its retention");
    assert!(
        service
            .inner
            .consensus
            .current_transaction(&transaction_id)
            .await
            .is_none()
    );

    for direct in [false, true] {
        for query in ["COMMIT", "REVERT"] {
            let mut resumed = SessionSubscriptions::new();
            resumed.bind_transaction(transaction_id.clone());
            service
                .inner
                .transaction_bindings
                .insert(transaction_id.clone(), resumed.session_id.clone());
            let unknown = if direct {
                if query == "COMMIT" {
                    service.commit_bound_transaction(&mut resumed, None).await
                } else {
                    service.revert_bound_transaction(&mut resumed).await
                }
            } else {
                service
                    .test_command(
                        CommandRequest {
                            query: query.to_string(),
                            domain: Some(named("reclaimed_commit")),
                            execution_reference: test_execution_reference(),
                            expected_transaction_position: None,
                            expected_preview: None,
                        },
                        &mut resumed,
                    )
                    .await
            };
            assert!(!unknown.succeeded(), "{unknown:?}");
            assert!(unknown.message.contains("is unknown"), "{unknown:?}");
            assert!(
                !resumed.transaction_active(),
                "{query} must end a session's hold on a transaction whose record is gone"
            );
            assert!(
                !service
                    .inner
                    .transaction_bindings
                    .contains_key(&transaction_id)
            );

            let begun = service
                .test_command(
                    CommandRequest {
                        query: "BEGIN".to_string(),
                        domain: Some(named("reclaimed_commit")),
                        execution_reference: test_execution_reference(),
                        expected_transaction_position: None,
                        expected_preview: None,
                    },
                    &mut resumed,
                )
                .await;
            assert!(begun.succeeded(), "{begun:?}");
            service.clean_close_transaction(&mut resumed).await;
            resumed.stop_all().await;
        }
    }

    let mut attaching = SessionSubscriptions::new();
    attaching.bind_transaction(transaction_id.clone());
    service
        .inner
        .transaction_bindings
        .insert(transaction_id.clone(), attaching.session_id.clone());
    let attachment = service
        .attach_transaction(transaction_id.clone(), &mut attaching)
        .await;
    let TransactionAttachment::Refused(refused) = attachment else {
        panic!("the reclaimed transaction cannot be attached: {attachment:?}");
    };
    assert!(refused.message.contains("is unknown"), "{refused:?}");
    assert!(!attaching.transaction_active());
    assert!(
        !service
            .inner
            .transaction_bindings
            .contains_key(&transaction_id)
    );

    let begun = service
        .test_command(
            CommandRequest {
                query: "BEGIN".to_string(),
                domain: Some(named("reclaimed_commit")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut attaching,
        )
        .await;
    assert!(begun.succeeded(), "{begun:?}");
    let new_id = attaching
        .transaction_id()
        .assured("BEGIN binds its new transaction")
        .to_string();
    assert!(matches!(
        service
            .attach_transaction(transaction_id.clone(), &mut attaching)
            .await,
        TransactionAttachment::Refused(_)
    ));
    assert_eq!(attaching.transaction_id(), Some(new_id.as_str()));
    assert!(
        service
            .inner
            .transaction_bindings
            .get(&new_id)
            .is_some_and(|binding| binding.value() == &attaching.session_id)
    );
    service.clean_close_transaction(&mut attaching).await;

    owner.stop_all().await;
    attaching.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}

#[nervix_primitives::test]
async fn reattaching_a_finished_transaction_the_session_still_named_releases_it() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(false).await;
    create_test_domain(&service.inner.consensus, "finished_reattach").await;
    let mut owner = SessionSubscriptions::new();
    let transaction_id = commit_owner_transaction(&service, "finished_reattach", &mut owner).await;
    // The session still names the transaction, but the leader holds no binding for it, as after
    // a leadership change.
    let mut resumed = SessionSubscriptions::new();
    resumed.bind_transaction(transaction_id.clone());

    let attached = service
        .attach_transaction(transaction_id, &mut resumed)
        .await;
    let TransactionAttachment::AlreadyFinished { transaction, .. } = attached else {
        panic!("attaching a finished transaction must be terminal, found {attached:?}");
    };
    assert_eq!(transaction.lifecycle(), &TransactionLifecycle::Committed);
    assert!(
        !resumed.transaction_active(),
        "a finished attach must end the hold of the session that still named the transaction"
    );
    let begun = service
        .test_command(
            CommandRequest {
                query: "BEGIN".to_string(),
                domain: Some(named("finished_reattach")),
                execution_reference: test_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            &mut resumed,
        )
        .await;

    assert!(
        begun.succeeded(),
        "the released session begins a new transaction: {}",
        begun.message
    );

    owner.stop_all().await;
    resumed.stop_all().await;
    let _ = std::fs::remove_dir_all(&path);
}
