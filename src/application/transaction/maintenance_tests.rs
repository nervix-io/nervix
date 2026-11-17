//! Tests of how the transaction maintenance pass reports a proposal it could not record.
//!
//! Layer: test harness.
//!
//! - **Owns.** Proof that a failed commit resume, inactive-transaction expiry and tombstone removal
//!   each report the whole cause chain of the proposal that failed, not only its outermost context.
//! - **Depends on.** The transaction maintenance pass, the consensus test probe's storage faults and
//!   the logs a scoped subscriber captures.
//! - **Must not know.** How consensus stores what it records.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_consensus::{
    ConsensusTestProbe, ReplicatedTransaction, StorageBoundary, TransactionActivity,
    TransactionState,
};
use nervix_models::{DomainName, Timestamp, UserName};
use nervix_recovery::Discarded as _;

use super::{
    super::{
        subscription::SessionSubscriptions,
        test_fixtures::{TestService, build_test_service_with_probe},
    },
    SessionServiceImpl, TransactionLimits,
};
use crate::captured_logs::CapturedLogs;

/// The deepest cause a storage fault of the consensus test probe reports.
const INJECTED_STORAGE_FAILURE: &str = "injected consensus storage failure";

/// Runs `pass` with a capturing subscriber as this thread's default and returns what it logged.
///
/// `#[nervix_primitives::test]` runs the whole future, and every task the service spawns, on the
/// calling thread, so the thread-local default the guard installs stays in force across the await.
async fn logged_by(pass: impl Future<Output = ()>) -> String {
    let logs = CapturedLogs::default();
    let guard = tracing::subscriber::set_default(logs.subscriber());
    pass.await;
    drop(guard);
    logs.contents()
}

/// The line of `logs` that reports `warning`. Assertions read that report alone, because the
/// consensus engine also logs the storage fault it met.
fn reported_line<'a>(logs: &'a str, warning: &str) -> &'a str {
    let Some(line) = logs.lines().find(|line| line.contains(warning)) else {
        panic!("the failure must be reported as '{warning}': {logs}");
    };
    line
}

/// Opens transaction `id` for `owner` in the default domain with `activity`.
async fn open_transaction(
    service: &SessionServiceImpl,
    id: &str,
    owner: UserName,
    activity: TransactionActivity,
) {
    let transaction = ReplicatedTransaction::open(
        id.to_string(),
        DomainName::parse("default").assured("the test domain name is valid"),
        owner,
        activity,
    );
    service
        .inner
        .consensus
        .open_transaction(transaction, TransactionLimits::default().max_open())
        .await
        .assured("the test transaction opens below the admission limit");
}

/// An activity whose inactivity deadline passed at the start of the epoch.
fn overdue_activity() -> TransactionActivity {
    TransactionActivity::from_timeout(Timestamp::from_unix_nanos(1), Duration::from_nanos(1))
}

async fn current_state(service: &SessionServiceImpl, id: &str) -> Option<TransactionState> {
    let transaction = service.inner.consensus.current_transaction(id).await?;
    Some(transaction.state)
}

/// Stops `test`'s service and removes its throwaway database.
fn discard(test: TestService) {
    let TestService {
        service,
        registry,
        path,
    } = test;
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

/// A failed expiry of an inactive transaction reports the proposal's whole cause chain, so an
/// operator can tell a storage failure from a refused proposal, and leaves the transaction open for
/// a later pass.
#[nervix_primitives::test]
async fn a_failed_inactive_transaction_expiry_reports_its_whole_cause_chain() {
    let probe = ConsensusTestProbe::default();
    let test = build_test_service_with_probe(true, probe.clone()).await;
    let service = &test.service;
    let id = "maintenance-expiry-failure";
    open_transaction(
        service,
        id,
        SessionSubscriptions::new().user,
        overdue_activity(),
    )
    .await;
    probe.storage_fault().fail_next(
        format!("expire-transaction:{id}"),
        StorageBoundary::BeforeCommit,
    );

    let logs = logged_by(service.reconcile_transactions_once()).await;

    let warning = reported_line(&logs, "failed to expire inactive Plexus transaction");
    assert!(
        warning.contains("error=consensus proposal failed: consensus storage failed: "),
        "the report must carry the proposal's cause, not only its outermost context: {warning}"
    );
    assert!(
        warning.contains(INJECTED_STORAGE_FAILURE),
        "the report must reach the storage failure beneath the proposal: {warning}"
    );
    assert!(
        matches!(
            current_state(service, id).await,
            Some(TransactionState::Open(_))
        ),
        "an expiry that was not recorded leaves the transaction open"
    );

    discard(test);
}

/// A failed removal of aged tombstones reports the proposal's whole cause chain and keeps the
/// tombstone for a later pass.
#[nervix_primitives::test]
async fn a_failed_tombstone_removal_reports_its_whole_cause_chain() {
    let probe = ConsensusTestProbe::default();
    let test = build_test_service_with_probe(true, probe.clone()).await;
    let service = &test.service;
    let id = "maintenance-tombstone-failure";
    open_transaction(
        service,
        id,
        SessionSubscriptions::new().user,
        overdue_activity(),
    )
    .await;
    // Expiring the transaction at the start of the epoch ages its tombstone past any retention.
    service
        .inner
        .consensus
        .expire_transaction(id.to_string(), Timestamp::from_unix_nanos(3))
        .await
        .assured("the overdue test transaction is open and inactive at the expiry instant");
    probe.storage_fault().fail_next(
        "remove-finished-transactions".to_string(),
        StorageBoundary::BeforeCommit,
    );

    let logs = logged_by(service.reconcile_transactions_once()).await;

    let warning = reported_line(&logs, "failed to remove expired transaction tombstones");
    assert!(
        warning.contains("error=consensus storage failed: "),
        "the report must carry the removal's cause, not only its outermost context: {warning}"
    );
    assert!(
        warning.contains(INJECTED_STORAGE_FAILURE),
        "the report must reach the storage failure beneath the removal: {warning}"
    );
    assert!(
        matches!(
            current_state(service, id).await,
            Some(TransactionState::Finished(_))
        ),
        "a removal that was not recorded keeps the tombstone"
    );

    discard(test);
}

/// A commit resume that fails reports the whole cause chain of the proposal it could not record,
/// and leaves the transaction committing for a later resume.
#[nervix_primitives::test]
async fn a_failed_commit_resume_reports_its_whole_cause_chain() {
    let probe = ConsensusTestProbe::default();
    let test = build_test_service_with_probe(true, probe.clone()).await;
    let service = &test.service;
    let id = "maintenance-resume-failure";
    let owner = SessionSubscriptions::new().user;
    let activity = service.transaction_activity();
    open_transaction(service, id, owner.clone(), activity).await;
    // The commit's final proposal fails after the commit started, so the transaction is left
    // committing and the store refuses every later write, including the resume's.
    probe.storage_fault().fail_next(
        format!("finish-empty-transaction-commit:{id}"),
        StorageBoundary::BeforeCommit,
    );
    let committed = service
        .commit_identified_transaction(id.to_string(), owner, activity, None)
        .await;
    assert!(!committed.succeeded(), "{committed:?}");
    assert!(
        matches!(
            current_state(service, id).await,
            Some(TransactionState::Committing(_))
        ),
        "the failed commit leaves the transaction committing"
    );
    let transactions = service.inner.consensus.current_transactions().await;
    let recovery_capacity = u32::try_from(
        service
            .inner
            .operational
            .control_plane
            .transaction_recovery_concurrency()
            .get(),
    )
    .assured("the recovery capability fits semaphore acquisition");

    let logs = logged_by(async {
        service.schedule_transaction_recovery(&transactions);
        // Each resume holds a recovery permit until it has reported its outcome, so the whole
        // capacity is free again only once the scheduled resume has finished.
        let idle = service
            .inner
            .transaction_recovery
            .permits
            .acquire_many(recovery_capacity)
            .await
            .assured("the recovery semaphore is never closed");
        drop(idle);
    })
    .await;

    let warning = reported_line(&logs, "failed to resume replicated Plexus commit");
    assert!(
        warning
            .contains("error=transaction consensus proposal failed: consensus proposal failed: "),
        "the report must carry the resume's cause, not only its outermost context: {warning}"
    );
    assert!(
        matches!(
            current_state(service, id).await,
            Some(TransactionState::Committing(_))
        ),
        "a resume that failed leaves the transaction committing"
    );

    discard(test);
}
