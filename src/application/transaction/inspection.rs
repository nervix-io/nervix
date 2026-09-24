//! Reading one transaction's impact report without changing anything about that transaction.
//!
//! Layer: control plane.
//!
//! - **Owns.** Inspection target resolution, the owner check an inspection shares with attach,
//!   which report each transaction state makes current, and the actual outcomes a report carries
//!   once its commit began.
//! - **Depends on.** Consensus for replicated transaction state and retained report revisions,
//!   and the transaction planner for an open transaction's coherent read.
//! - **Must not know.** How an inspection request arrives, or how its report is rendered.

use std::collections::BTreeMap;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_consensus::{ReplicatedTransaction, TransactionOutcome, TransactionState};
use nervix_models::{
    ActualExecutionStepImpact, ClusterNodeName, ExecutionStepImpactReport, TransactionImpactReport,
    TransactionInspection, TransactionInspectionRejection, TransactionInspectionRequest,
    TransactionInspectionTarget, TransactionLifecycle, TransactionOperationNumber,
    TransactionOperationRange, TransactionPosition, TransactionStatus, UserName,
};
use thiserror::Error;
use tracing::debug;

use crate::application::{session_service::SessionServiceImpl, subscription::SessionSubscriptions};

/// The session identity an inspection is answered for.
///
/// An inspection needs only who is asking and which transaction they are bound to. Taking those
/// two facts rather than the session's whole state is what makes it impossible for a read to
/// change a binding, a domain or a subscription on its way through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InspectingSession<'a> {
    pub user: &'a UserName,
    /// The transaction bound to the session, which [`TransactionInspectionTarget::Attached`]
    /// reads.
    pub attached_transaction: Option<&'a str>,
}

impl<'a> From<&'a SessionSubscriptions> for InspectingSession<'a> {
    fn from(subscriptions: &'a SessionSubscriptions) -> Self {
        Self {
            user: &subscriptions.user,
            attached_transaction: subscriptions.transaction_id(),
        }
    }
}

/// What an inspection read, or why it read nothing.
///
/// A rejection always means the transaction was left exactly as it was found. There is no partial
/// report: a reader either receives the whole revision it asked about or is told why it cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionInspectionOutcome {
    Inspected(Box<TransactionInspection>),
    Rejected {
        rejection: TransactionInspectionRejection,
        message: String,
    },
    NotLeader {
        leader: Option<ClusterNodeName>,
    },
}

impl TransactionInspectionOutcome {
    fn rejected(rejection: TransactionInspectionRejection, message: impl Into<String>) -> Self {
        Self::Rejected {
            rejection,
            message: message.into(),
        }
    }
}

/// Why a transaction has no report an inspection can read.
///
/// Each variant names a state of the transaction rather than a failure of the reader, so the
/// rejection a caller sees says what to do about it: retry once the domain settles, or accept
/// that this transaction never planned anything.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InspectedReportError {
    #[error("transaction '{transaction_id}' cannot be planned from a coherent basis right now")]
    Planning { transaction_id: String },
    #[error("transaction '{transaction_id}' produced no coherent impact report")]
    IncoherentPlan { transaction_id: String },
    #[error(
        "transaction '{transaction_id}' ended before any operation was planned, so it retains no \
         report"
    )]
    NeverPlanned { transaction_id: String },
    #[error(
        "transaction '{transaction_id}' retains no readable revision of the report its commit \
         admitted"
    )]
    UnreadableRevision { transaction_id: String },
    #[error(
        "transaction '{transaction_id}' recorded execution outcomes that do not describe its \
         frozen report"
    )]
    RecordedOutcomes { transaction_id: String },
}

impl SessionServiceImpl {
    /// Reads the report of the attached transaction or of one named by identity.
    ///
    /// Nothing about the inspected transaction changes: no binding is taken, no domain adopted,
    /// no activity recorded and no queue position consumed. Reading an open transaction plans a
    /// fresh coherent basis every time rather than joining the durable admission a mutation would
    /// take, so two inspections of an unchanged transaction agree because the inputs agree, not
    /// because one cached the other.
    pub async fn inspect_transaction(
        &self,
        request: &TransactionInspectionRequest,
        session: InspectingSession<'_>,
    ) -> TransactionInspectionOutcome {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            return TransactionInspectionOutcome::NotLeader { leader };
        }
        let transaction_id = match &request.target {
            TransactionInspectionTarget::Attached => {
                let Some(attached) = session.attached_transaction else {
                    return TransactionInspectionOutcome::rejected(
                        TransactionInspectionRejection::NoAttachedTransaction,
                        "no transaction is attached to this session",
                    );
                };
                attached.to_string()
            }
            TransactionInspectionTarget::Transaction { transaction_id } => transaction_id.clone(),
        };
        let Some(transaction) = self
            .inner
            .consensus
            .current_transaction(&transaction_id)
            .await
        else {
            return TransactionInspectionOutcome::rejected(
                TransactionInspectionRejection::TransactionNotFound,
                format!("transaction '{transaction_id}' is unknown"),
            );
        };
        if &transaction.owner != session.user {
            return TransactionInspectionOutcome::rejected(
                TransactionInspectionRejection::NotOwner,
                format!("transaction '{transaction_id}' belongs to another user"),
            );
        }
        let report = match self.inspected_report(&transaction).await {
            Ok(report) => report,
            Err(error) => {
                debug!(
                    transaction_id,
                    error = ?error,
                    "transaction inspection found no readable report"
                );
                return TransactionInspectionOutcome::rejected(
                    TransactionInspectionRejection::ReportUnavailable,
                    error.current_context().to_string(),
                );
            }
        };
        if let Some(operation) = request.operation
            && operation.get() > report.position().accepted_operations()
        {
            return TransactionInspectionOutcome::rejected(
                TransactionInspectionRejection::OperationNotFound,
                format!(
                    "transaction '{transaction_id}' accepted {} operation(s), so operation \
                     {operation} does not exist",
                    report.position().accepted_operations()
                ),
            );
        }
        let status = transaction_inspection_status(&transaction, report.position());
        TransactionInspectionOutcome::Inspected(Box::new(TransactionInspection {
            transaction: status,
            operation: request.operation,
            report,
        }))
    }

    /// The report that currently describes `transaction`.
    ///
    /// An open transaction has no frozen revision yet, so its report is planned from a coherent
    /// read of the control plane and carries that basis and its completeness. Once a commit is
    /// admitted the report is the frozen revision the commit applies, and the actual outcomes its
    /// steps recorded are layered onto it so applying work and recovery stay visible beside what
    /// was required.
    async fn inspected_report(
        &self,
        transaction: &ReplicatedTransaction,
    ) -> Result<TransactionImpactReport, Report<InspectedReportError>> {
        match &transaction.state {
            TransactionState::Open(_) => self.planned_inspection_report(transaction).await,
            TransactionState::Committing(_) | TransactionState::Finished(_) => {
                self.frozen_inspection_report(transaction).await
            }
        }
    }

    async fn planned_inspection_report(
        &self,
        transaction: &ReplicatedTransaction,
    ) -> Result<TransactionImpactReport, Report<InspectedReportError>> {
        let statements = transaction
            .statements
            .iter()
            .map(|queued| queued.statement.clone())
            .collect::<Vec<_>>();
        let operation_references = transaction
            .statements
            .iter()
            .map(|queued| queued.request_reference.clone())
            .collect::<Vec<_>>();
        let captured = self
            .plan_transaction_statements(
                &transaction.domain,
                &statements,
                &operation_references,
                0,
                true,
            )
            .await
            .change_context(InspectedReportError::Planning {
                transaction_id: transaction.id.clone(),
            })?;
        captured
            .plan
            .report()
            .change_context(InspectedReportError::IncoherentPlan {
                transaction_id: transaction.id.clone(),
            })
    }

    async fn frozen_inspection_report(
        &self,
        transaction: &ReplicatedTransaction,
    ) -> Result<TransactionImpactReport, Report<InspectedReportError>> {
        let preview = transaction.latest_preview().ok_or_else(|| {
            Report::new(InspectedReportError::NeverPlanned {
                transaction_id: transaction.id.clone(),
            })
        })?;
        let frozen = self
            .inner
            .consensus
            .current_transaction_report(preview)
            .await
            .change_context(InspectedReportError::UnreadableRevision {
                transaction_id: transaction.id.clone(),
            })?;
        with_recorded_outcomes(frozen, transaction)
    }
}

/// The public status of `transaction`, as the inspected revision numbers its operations.
///
/// The accepted position comes from the report rather than the queue, so status and report cannot
/// disagree about how many operations the reader was shown.
pub(super) fn transaction_inspection_status(
    transaction: &ReplicatedTransaction,
    accepted_operations: TransactionPosition,
) -> TransactionStatus {
    let lifecycle = match &transaction.state {
        TransactionState::Open(_) => TransactionLifecycle::Open,
        TransactionState::Committing(_) => TransactionLifecycle::Committing,
        TransactionState::Finished(finished) => match &finished.outcome {
            TransactionOutcome::Committed => TransactionLifecycle::Committed,
            TransactionOutcome::Failed {
                failing_step,
                error,
            }
            | TransactionOutcome::PlanningInputsChanged {
                failing_step,
                error,
            } => failed_lifecycle(*failing_step, error),
            TransactionOutcome::Reverted => TransactionLifecycle::Reverted,
            TransactionOutcome::Expired => TransactionLifecycle::Expired,
        },
    };
    TransactionStatus::new(
        transaction.id.clone(),
        transaction.domain.clone(),
        lifecycle,
        accepted_operations,
        transaction.completed_statement_count(),
    )
    .assured(
        "a transaction accepts no operation after its commit admission, so the operations it has \
         completed never exceed the position of the revision being read",
    )
}

/// A failed transaction names the operation whose execution step failed.
fn failed_lifecycle(failing_step: usize, error: &str) -> TransactionLifecycle {
    let failing_operation = TransactionOperationNumber::from_index(failing_step)
        .assured("a failing step index is a queued statement position, bounded by the queue limit");
    TransactionLifecycle::Failed {
        failing_operation,
        error: error.to_string(),
    }
}

/// Layers the actual outcomes recorded by commit application onto a frozen report.
///
/// Commit admission freezes what every step requires; application records what each step actually
/// engaged, released and applied. The frozen revision is the authority for the planned side, and
/// the transaction's own results are the authority for the actual side, so a report shows both
/// without either overwriting the other.
fn with_recorded_outcomes(
    frozen: TransactionImpactReport,
    transaction: &ReplicatedTransaction,
) -> Result<TransactionImpactReport, Report<InspectedReportError>> {
    let recorded = recorded_actual_steps(transaction);
    if recorded.is_empty() {
        return Ok(frozen);
    }
    let domain = frozen.domain().clone();
    let position = frozen.position();
    let planning_basis = frozen.planning_basis();
    let completeness = frozen.completeness().clone();
    let operations = frozen.operations().to_vec();
    let mut execution_steps = frozen.execution_steps().to_vec();
    for step in &mut execution_steps {
        let Some(actual) = recorded.get(&step.operations()) else {
            continue;
        };
        *step.actual_mut() = actual.clone();
    }
    TransactionImpactReport::new(
        domain,
        position,
        planning_basis,
        completeness,
        operations,
        execution_steps,
    )
    .change_context(InspectedReportError::RecordedOutcomes {
        transaction_id: transaction.id.clone(),
    })
}

/// The actual impact each execution step recorded, keyed by the operations it executed.
///
/// A step that is still applying is included: its recorded engagement is what makes applying work
/// visible during a commit rather than only after it finishes.
fn recorded_actual_steps(
    transaction: &ReplicatedTransaction,
) -> BTreeMap<TransactionOperationRange, ActualExecutionStepImpact> {
    let mut recorded = BTreeMap::new();
    for result in transaction.commit_results() {
        record_step(&mut recorded, &result.impact);
    }
    if let TransactionState::Committing(progress) = &transaction.state
        && let Some(applying) = &progress.applying
    {
        record_step(&mut recorded, &applying.result.impact);
    }
    recorded
}

fn record_step(
    recorded: &mut BTreeMap<TransactionOperationRange, ActualExecutionStepImpact>,
    step: &ExecutionStepImpactReport,
) {
    recorded.insert(step.operations(), step.actual().clone());
}

#[cfg(test)]
#[path = "inspection_tests.rs"]
mod tests;
