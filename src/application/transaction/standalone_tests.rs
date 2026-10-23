//! Standalone command transaction tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that a refusal decided against a coordinator's stale observation of a
//!   command's transaction recovers the progress recorded since, and that a refusal against an
//!   unchanged transaction keeps its failure.
//! - **Depends on.** The parent module's preparation and commit-admission steps, the session
//!   command path, the server statement parser for fixture statements, the session test fixtures
//!   and, in a testing build, the standalone preparation barriers.
//! - **Must not know.** Production ownership beyond the parent module under test.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "tests standalone command transactions with parsed fixture statements"
    )
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::CommandRequest;
use nervix_consensus::{
    ReplicatedTransaction, TransactionOutcome, TransactionQueueLimits, TransactionState,
    TransactionStatementRequest,
};
use nervix_models::{DomainName, UserName};
use nervix_recovery::Discarded as _;

use super::{super::TransactionLimits, StandaloneCommand};
use crate::application::{
    SessionServiceImpl,
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
    assert!(
        matches!(
            transaction.finished_outcome(),
            Some(TransactionOutcome::Committed)
        ),
        "the command transaction commits once: {transaction:?}"
    );
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

/// The leader refuses a standalone command's step publication as one whose retained log is
/// saturated, after the command's transaction started committing. The command must stay applying
/// and keep its domain mutation lease rather than record a terminal failure, so that repeating it
/// under the same reference finishes its transaction once.
#[cfg(feature = "testing")]
#[nervix_primitives::test]
async fn a_command_whose_transaction_is_still_committing_stays_applying_until_it_finishes() {
    use crate::{
        application::command_result::OutcomeUnknownCause,
        fault_injection::ModelPublicationRefusalKind,
    };

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
    let domain =
        DomainName::parse("default").assured("the fixture domain name is an accepted literal");
    fault_injection
        .refuse_next_model_publication(domain.clone(), ModelPublicationRefusalKind::SaturatedLog);
    let request = CommandRequest {
        execution_reference: test_execution_reference(),
        ..test_command_request("CREATE SCHEMA retained ( key I64 );", "default")
    };
    let reference = request.execution_reference.clone();
    let transaction_id = command_transaction_id(&request);
    let mut subscriptions = SessionSubscriptions::new();

    let first = service
        .test_command(request.clone(), &mut subscriptions)
        .await;

    assert!(
        matches!(
            first.disposition,
            CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::StillApplying)
        ),
        "a command whose transaction is still committing answers that it still applies: {first:?}"
    );
    let execution = service
        .inner
        .consensus
        .current_command_execution(&reference)
        .await
        .verified("the command was admitted before its publication was refused");
    assert!(
        execution.is_applying(),
        "the command records no outcome while its transaction commits: {execution:?}"
    );
    assert!(
        execution.domain_mutation(&domain).is_some(),
        "the command keeps the domain mutation lease its transaction commits under: {execution:?}"
    );
    let transaction = service
        .inner
        .consensus
        .current_transaction(&transaction_id)
        .await
        .verified("the refused publication left the command's transaction in place");
    assert!(
        matches!(transaction.state, TransactionState::Committing(_)),
        "the refused publication leaves the transaction committing: {transaction:?}"
    );

    let repeated = service.test_command(request, &mut subscriptions).await;
    subscriptions.stop_all().await;

    assert!(
        repeated.succeeded(),
        "repeating the command under its reference finishes it: {repeated:?}"
    );
    let transaction = service
        .inner
        .consensus
        .current_transaction(&transaction_id)
        .await
        .verified("the committed command transaction keeps its tombstone");
    assert!(
        matches!(
            transaction.finished_outcome(),
            Some(TransactionOutcome::Committed)
        ),
        "the command transaction commits once: {transaction:?}"
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

/// One standalone command request in the fixture's `default` domain, applied through the command
/// transaction its reference names.
struct FixtureCommand {
    transaction_id: String,
    owner: UserName,
    domain: DomainName,
    request: TransactionStatementRequest,
}

impl FixtureCommand {
    fn new(source: &str) -> Self {
        let reference = test_execution_reference();
        let statement = nervix_nspl::server_statement::parse_server_statement(source)
            .assured("the fixture statement is valid Plexus");
        Self {
            transaction_id: format!("command.{}", reference.as_str()),
            owner: UserName::parse("operator").assured("the test owner is an accepted literal"),
            domain: DomainName::parse("default")
                .assured("the fixture domain name is an accepted literal"),
            request: TransactionStatementRequest {
                request_reference: reference,
                expected_position: 0,
                source: source.to_string(),
                statement,
            },
        }
    }

    fn command(&self) -> StandaloneCommand<'_> {
        StandaloneCommand {
            transaction_id: &self.transaction_id,
            owner: &self.owner,
            domain: &self.domain,
            request: &self.request,
            limits: TransactionQueueLimits {
                max_statements: 16,
                max_source_bytes: 4096,
            },
        }
    }
}

/// Opens the fixture command's transaction and queues its statement, as its first coordinator
/// does, answering what that coordinator observed of it.
async fn queued_transaction(
    service: &SessionServiceImpl,
    fixture: &FixtureCommand,
) -> ReplicatedTransaction {
    let queued = service
        .admit_standalone_statement(&fixture.command(), None)
        .await
        .unwrap_or_else(|result| panic!("the fixture statement is admitted: {result:?}"));
    assert!(matches!(queued.state, TransactionState::Open(_)));
    assert_eq!(queued.statements.len(), 1);
    queued
}

/// Applies the fixture command's queued transaction to completion, as another coordinator does.
async fn committed_by_another_coordinator(
    service: &SessionServiceImpl,
    fixture: &FixtureCommand,
    queued: ReplicatedTransaction,
) -> ReplicatedTransaction {
    let committing = service
        .admit_standalone_commit(&fixture.command(), queued)
        .await
        .unwrap_or_else(|result| panic!("the other coordinator admits the commit: {result:?}"));
    assert!(matches!(committing.state, TransactionState::Committing(_)));
    let committed = service
        .execute_replicated_commit(&fixture.transaction_id)
        .await
        .assured("the other coordinator applies the commit");
    assert!(
        matches!(
            committed.finished_outcome(),
            Some(TransactionOutcome::Committed)
        ),
        "the other coordinator commits the transaction: {committed:?}"
    );
    committed
}

#[nervix_primitives::test]
async fn commit_planning_recovers_a_transaction_committed_since_it_was_observed() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let fixture = FixtureCommand::new("CREATE SCHEMA planned ( key I64 );");
    let observed = queued_transaction(&service, &fixture).await;
    let committed = committed_by_another_coordinator(&service, &fixture, observed.clone()).await;

    // Planning from the earlier observation is refused by the command's own committed schema.
    let recovered = service
        .admit_standalone_commit(&fixture.command(), observed)
        .await;

    let recovered = recovered
        .unwrap_or_else(|result| panic!("the stale coordinator recovers the commit: {result:?}"));
    assert_eq!(recovered, committed);

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[nervix_primitives::test]
async fn a_stale_open_observation_recovers_the_transaction_committed_since() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let fixture = FixtureCommand::new("CREATE SCHEMA queued ( key I64 );");
    let opened = service
        .inner
        .consensus
        .open_transaction(
            ReplicatedTransaction::open_for_command(
                fixture.transaction_id.clone(),
                fixture.domain.clone(),
                fixture.owner.clone(),
                service.transaction_activity(),
                fixture.request.request_reference.clone(),
            ),
            TransactionLimits::default().max_open(),
        )
        .await
        .assured("the single-node test leader opens the command transaction");
    let queued = service
        .admit_standalone_statement(&fixture.command(), Some(opened.clone()))
        .await
        .unwrap_or_else(|result| panic!("the other coordinator queues the statement: {result:?}"));
    let committed = committed_by_another_coordinator(&service, &fixture, queued).await;

    // Preflighting against the empty transaction first observed is refused by the command's own
    // committed schema.
    let recovered = service
        .admit_standalone_statement(&fixture.command(), Some(opened))
        .await;

    let recovered = recovered
        .unwrap_or_else(|result| panic!("the stale coordinator recovers the commit: {result:?}"));
    assert_eq!(recovered, committed);

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[nervix_primitives::test]
async fn a_queue_write_refused_by_a_transaction_committing_since_recovers_it() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let fixture = FixtureCommand::new("CREATE SCHEMA committing ( key I64 );");
    let opened = service
        .inner
        .consensus
        .open_transaction(
            ReplicatedTransaction::open_for_command(
                fixture.transaction_id.clone(),
                fixture.domain.clone(),
                fixture.owner.clone(),
                service.transaction_activity(),
                fixture.request.request_reference.clone(),
            ),
            TransactionLimits::default().max_open(),
        )
        .await
        .assured("the single-node test leader opens the command transaction");
    let queued = service
        .admit_standalone_statement(&fixture.command(), Some(opened.clone()))
        .await
        .unwrap_or_else(|result| panic!("the other coordinator queues the statement: {result:?}"));
    let committing = service
        .admit_standalone_commit(&fixture.command(), queued)
        .await
        .unwrap_or_else(|result| panic!("the other coordinator admits the commit: {result:?}"));
    assert!(matches!(committing.state, TransactionState::Committing(_)));

    // The schema is not applied yet, so the preflight against the empty transaction first observed
    // passes, and the ledger refuses the queue write because the transaction is committing.
    let recovered = service
        .admit_standalone_statement(&fixture.command(), Some(opened))
        .await;

    let recovered = recovered
        .unwrap_or_else(|result| panic!("the stale coordinator recovers the commit: {result:?}"));
    assert_eq!(recovered, committing);

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[nervix_primitives::test]
async fn a_commit_start_refused_by_a_transaction_committed_since_recovers_it() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let fixture = FixtureCommand::new("CREATE IF NOT EXISTS SCHEMA started ( key I64 );");
    let observed = queued_transaction(&service, &fixture).await;
    let committed = committed_by_another_coordinator(&service, &fixture, observed.clone()).await;

    // The idempotent creation plans again from the earlier observation, and the ledger refuses
    // starting a commit of the transaction that has already finished.
    let recovered = service
        .admit_standalone_commit(&fixture.command(), observed)
        .await;

    let recovered = recovered
        .unwrap_or_else(|result| panic!("the stale coordinator recovers the commit: {result:?}"));
    assert_eq!(recovered, committed);

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[nervix_primitives::test]
async fn commit_planning_refused_against_an_unchanged_transaction_fails_the_command() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let fixture = FixtureCommand::new("CREATE SCHEMA contested ( key I64 );");
    let observed = queued_transaction(&service, &fixture).await;
    let mut subscriptions = SessionSubscriptions::new();
    let competing = service
        .test_command(
            CommandRequest {
                execution_reference: test_execution_reference(),
                ..test_command_request("CREATE SCHEMA contested ( key I64 );", "default")
            },
            &mut subscriptions,
        )
        .await;
    assert!(
        competing.succeeded(),
        "another command creates the schema first: {competing:?}"
    );

    let refused = service
        .admit_standalone_commit(&fixture.command(), observed.clone())
        .await;

    let Err(result) = refused else {
        panic!("a refusal against an unchanged transaction fails the command");
    };
    assert!(
        matches!(result.disposition, CommandDisposition::Failed),
        "the refused commit fails the command: {result:?}"
    );
    assert!(
        result
            .message
            .contains("model 'contested' already exists in domain 'default'"),
        "the command keeps its planning refusal: {}",
        result.message
    );
    let transaction = service
        .inner
        .consensus
        .current_transaction(&fixture.transaction_id)
        .await
        .verified("a planning refusal leaves the command transaction in place");
    assert_eq!(
        transaction, observed,
        "a refusal against an unchanged transaction neither recovers nor finishes it"
    );

    subscriptions.stop_all().await;
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}
