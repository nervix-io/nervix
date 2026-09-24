//! Answering `DESCRIBE TRANSACTION`.
//!
//! Layer: control plane.
//!
//! - **Owns.** Turning the statement into one inspection read, the command result that answers
//!   it, and the typed inspection that result carries whatever its format.
//! - **Depends on.** The inspection service, which decides what is read and refuses what cannot
//!   be, and the report rendering.
//! - **Must not know.** How an inspection resolves its target, checks ownership or plans a report,
//!   or how a result travels to a client.

use nervix_models::DescribeTransaction;

use super::{InspectingSession, TransactionInspectionOutcome, rendering::InspectionRendering};
use crate::application::{
    command_result::CommandResult,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};

impl SessionServiceImpl {
    /// Answers `DESCRIBE TRANSACTION` with the report the inspection service read.
    ///
    /// The inspection travels typed beside whichever rendering the statement asked for. The
    /// result's transaction status is left for the session to fill in, because it describes the
    /// caller's binding, which inspecting another transaction never changes. A refusal is an
    /// ordinary command error naming why nothing was read.
    pub(in crate::application) async fn describe_transaction(
        &self,
        describe: DescribeTransaction,
        query: &str,
        session: InspectingSession<'_>,
    ) -> CommandResult {
        let outcome = self.inspect_transaction(&describe.request, session).await;
        match outcome {
            TransactionInspectionOutcome::Inspected(inspection) => {
                let message = InspectionRendering::new(&inspection).render(describe.format);
                CommandResult {
                    inspection: Some(inspection),
                    ..command_ok(message)
                }
            }
            TransactionInspectionOutcome::Rejected { message, .. } => command_error(message),
            TransactionInspectionOutcome::NotLeader { leader } => {
                self.not_leader_response(query, leader).await
            }
        }
    }
}

#[cfg(test)]
#[path = "describe_tests.rs"]
mod tests;
