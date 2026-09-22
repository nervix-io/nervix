//! Answering `DESCRIBE TRANSACTION`.
//!
//! Layer: edges.
//!
//! - **Owns.** Turning the statement into one inspection read, the command result that answers
//!   it, and the typed inspection envelope that result carries whatever its format.
//! - **Depends on.** The inspection service, which decides what is read and refuses what cannot
//!   be, and the report rendering.
//! - **Must not know.** How an inspection resolves its target, checks ownership or plans a report.

use arch_into::ArchInto;
use meticulous::ResultExt as _;
use nervix_models::{DescribeTransaction, TransactionInspection, TransactionLifecycle};

use super::{InspectingSession, TransactionInspectionOutcome, rendering::InspectionRendering};
use crate::{
    application::{
        model_mutation::command_error, session_service::SessionServiceImpl,
        subscription::SessionSubscriptions,
    },
    proto::{self, CommandResult, CommandResultKind, TransactionState as ApiTransactionState},
};

impl SessionServiceImpl {
    /// Answers `DESCRIBE TRANSACTION` with the report the inspection service read.
    ///
    /// The inspected transaction travels in its own typed envelope beside whichever rendering the
    /// statement asked for. The result's transaction status is left for the session to fill in,
    /// because it describes the caller's binding, which inspecting another transaction never
    /// changes. A refusal is an ordinary command error naming why nothing was read.
    pub(in crate::application) async fn describe_transaction(
        &self,
        describe: DescribeTransaction,
        query: &str,
        subscriptions: &SessionSubscriptions,
    ) -> CommandResult {
        let session = InspectingSession::from(subscriptions);
        let outcome = self.inspect_transaction(&describe.request, session).await;
        match outcome {
            TransactionInspectionOutcome::Inspected(inspection) => {
                let message = InspectionRendering::new(&inspection).render(describe.format);
                CommandResult {
                    success: true,
                    message,
                    kind: i32::from(CommandResultKind::Ok),
                    inspection: Some(api_transaction_inspection(&inspection)),
                    ..Default::default()
                }
            }
            TransactionInspectionOutcome::Rejected { message, .. } => command_error(message),
            TransactionInspectionOutcome::NotLeader { leader } => {
                self.not_leader_response(query, leader).await
            }
        }
    }
}

/// The typed inspection envelope as the session API carries it.
fn api_transaction_inspection(inspection: &TransactionInspection) -> proto::TransactionInspection {
    let report = serde_json::to_vec(&inspection.report).assured(
        "an impact report holds only strings, numbers, sequences and tagged enums, all of which \
         have a JSON representation",
    );
    proto::TransactionInspection {
        transaction: Some(api_inspected_status(inspection)),
        operation: inspection
            .operation
            .map(|operation| operation.get().arch_into()),
        report: report.into(),
    }
}

/// The inspected transaction's status, counting the operations its report numbers.
fn api_inspected_status(inspection: &TransactionInspection) -> proto::TransactionStatus {
    let status = &inspection.transaction;
    let state = match status.lifecycle() {
        TransactionLifecycle::Open => ApiTransactionState::Open,
        TransactionLifecycle::Committing => ApiTransactionState::Committing,
        TransactionLifecycle::Committed => ApiTransactionState::Committed,
        TransactionLifecycle::Failed { .. } => ApiTransactionState::Failed,
        TransactionLifecycle::Reverted => ApiTransactionState::Reverted,
        TransactionLifecycle::Expired => ApiTransactionState::Expired,
    };
    let mut api = proto::TransactionStatus {
        id: status.transaction_id().to_string(),
        state: i32::from(state),
        pending_count: status.pending_operations().arch_into(),
        completed_count: status.applied_operations().arch_into(),
        total_count: status
            .accepted_operations()
            .accepted_operations()
            .arch_into(),
        domain: status.domain().to_string(),
        ..Default::default()
    };
    if let TransactionLifecycle::Failed {
        failing_operation,
        error,
    } = status.lifecycle()
    {
        api.error.clone_from(error);
        api.failing_step = Some(failing_operation.get().arch_into());
    }
    api
}

#[cfg(test)]
#[path = "describe_tests.rs"]
mod tests;
