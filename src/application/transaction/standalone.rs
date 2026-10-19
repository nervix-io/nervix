//! Persistent model commands applied through a durable transaction of their own.
//!
//! Layer: control plane.
//!
//! - **Owns.** Applying one admitted model command as a standalone transaction, from opening the
//!   transaction and queuing the command's statement to its replicated commit and command result.
//! - **Depends on.** The transaction lifecycle for preflight, commit planning and the replicated
//!   commit, and consensus for the authoritative transaction ledger.
//! - **Must not know.** Session transport state, command parsing or runtime execution details.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use meticulous::OptionExt as _;
use nervix_consensus::{
    ConsensusError, ConsensusTransactionError, ReplicatedTransaction, TransactionMutationError,
    TransactionOutcome, TransactionQueueAdmission, TransactionQueueLimits, TransactionQueueRequest,
    TransactionState, TransactionStatement, TransactionStatementRequest,
};
use nervix_models::{CommandExecutionReference, DomainName, Statement, UserName};

use super::{
    super::{
        command_result::{CommandDiagnostic, CommandDisposition, CommandResult},
        model_mutation::command_error,
        session_service::SessionServiceImpl,
    },
    is_queueable_transaction_statement, transaction_commit_error_message,
    transaction_commit_result, transaction_statement_label,
};

fn standalone_transaction_result(transaction: &ReplicatedTransaction) -> CommandResult {
    if !matches!(
        transaction.finished_outcome(),
        Some(TransactionOutcome::Committed)
    ) {
        let mut result = transaction_commit_result(transaction);
        result.transaction = None;
        return result;
    }

    let Some(step) = transaction.commit_results().last() else {
        return command_error(format!(
            "durable command transaction '{}' completed without a command result",
            transaction.id
        ));
    };
    let disposition = if step.result.success {
        CommandDisposition::Completed {
            already_existed: step.result.already_existed,
        }
    } else {
        CommandDisposition::Failed
    };
    CommandResult {
        diagnostics: step
            .result
            .diagnostics
            .iter()
            .map(CommandDiagnostic::from)
            .collect(),
        transaction_admission: step.result.admission.clone(),
        ..CommandResult::new(disposition, step.result.message.clone())
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn execute_standalone_transaction(
        &self,
        transaction_id: String,
        request_reference: CommandExecutionReference,
        owner: UserName,
        domain: DomainName,
        source: String,
        statement: Statement,
    ) -> CommandResult {
        let root_transaction_id = transaction_id;
        let mut attempt_transaction_id = root_transaction_id.clone();
        loop {
            nervix_primitives::task::consume_budget().await;
            let result = Box::pin(self.execute_standalone_transaction_attempt(
                attempt_transaction_id.clone(),
                request_reference.clone(),
                owner.clone(),
                domain.clone(),
                source.clone(),
                statement.clone(),
            ))
            .await;
            let attempt = self
                .inner
                .consensus
                .current_transaction(&attempt_transaction_id)
                .await;
            let Some(TransactionState::Finished(finished)) =
                attempt.as_ref().map(|transaction| &transaction.state)
            else {
                return result;
            };
            if !matches!(
                &finished.outcome,
                TransactionOutcome::PlanningInputsChanged { .. }
            ) {
                return result;
            }
            attempt_transaction_id = format!(
                "{root_transaction_id}.refresh.{}",
                finished.outcome_revision
            );
        }
    }

    async fn execute_standalone_transaction_attempt(
        &self,
        transaction_id: String,
        request_reference: CommandExecutionReference,
        owner: UserName,
        domain: DomainName,
        source: String,
        statement: Statement,
    ) -> CommandResult {
        if !is_queueable_transaction_statement(&statement) {
            return command_error(format!(
                "{} cannot use durable transaction application",
                transaction_statement_label(&statement)
            ));
        }
        let limits = TransactionQueueLimits {
            max_statements: self.inner.transaction_max_statements,
            max_source_bytes: self.inner.transaction_max_source_bytes,
        };
        let queued = TransactionStatementRequest {
            request_reference,
            expected_position: 0,
            source,
            statement,
        };

        let mut current = self
            .inner
            .consensus
            .current_transaction(&transaction_id)
            .await;
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_standalone_transaction_if_armed(
                self.inner.consensus.local_node_id(),
                &domain,
                crate::fault_injection::StandaloneTransactionBoundary::Lookup,
            )
            .await;
        let mut prepared = None;
        if current.is_none() {
            let candidate = ReplicatedTransaction::open_for_command(
                transaction_id.clone(),
                domain.clone(),
                owner.clone(),
                self.transaction_activity(),
                queued.request_reference.clone(),
            );
            if let Err(error) = candidate.queue_admission(&owner, &domain, &queued, limits) {
                return command_error(error.to_string());
            }
            let admission = self
                .preflight_transaction_statement(&candidate, &queued)
                .await;
            #[cfg(feature = "testing")]
            self.inner
                .runtime
                .pause_standalone_transaction_if_armed(
                    self.inner.consensus.local_node_id(),
                    &domain,
                    crate::fault_injection::StandaloneTransactionBoundary::Preflight,
                )
                .await;
            let prepared_admission = match admission {
                Ok(admission) => admission,
                Err(error) => return command_error(format!("{error:#}")),
            };
            prepared = Some(prepared_admission);
            if let Err(error) = self
                .inner
                .consensus
                .open_transaction(candidate, self.inner.transaction_max_open)
                .await
                && self
                    .inner
                    .consensus
                    .current_transaction(&transaction_id)
                    .await
                    .is_none()
            {
                return self.transaction_consensus_error_response(error).await;
            }
            current = self
                .inner
                .consensus
                .current_transaction(&transaction_id)
                .await;
            if current
                .as_ref()
                .is_some_and(|transaction| transaction.statements.is_empty())
            {
                let prepared_admission = prepared
                    .take()
                    .verified("the newly opened transaction prepared its first admission above");
                let admitted =
                    TransactionStatement::admitted(queued.clone(), prepared_admission.result);
                match self
                    .inner
                    .consensus
                    .queue_transaction_statement(TransactionQueueRequest {
                        id: transaction_id.clone(),
                        owner: owner.clone(),
                        domain: domain.clone(),
                        activity: self.transaction_activity(),
                        statement: admitted,
                        report: prepared_admission.report,
                        limits,
                    })
                    .await
                {
                    Ok(queued_transaction) => current = Some(queued_transaction),
                    Err(error) => {
                        return self.transaction_consensus_error_response(error).await;
                    }
                }
            }
        }
        let mut transaction =
            current.verified("the durable command transaction was opened or observed above");

        if transaction.owner != owner || transaction.domain != domain {
            return command_error(format!(
                "durable command transaction '{}' is bound to a different owner or domain",
                transaction.id
            ));
        }
        if let TransactionState::Open(_) = &transaction.state {
            match transaction.queue_admission(&owner, &domain, &queued, limits) {
                Ok(TransactionQueueAdmission::Existing(_)) => {}
                Ok(TransactionQueueAdmission::New) => {
                    let prepared_admission = match prepared.take() {
                        Some(prepared_admission) => prepared_admission,
                        None => {
                            match self
                                .preflight_transaction_statement(&transaction, &queued)
                                .await
                            {
                                Ok(admission) => admission,
                                Err(error) => return command_error(format!("{error:#}")),
                            }
                        }
                    };
                    let admitted =
                        TransactionStatement::admitted(queued.clone(), prepared_admission.result);
                    transaction = match self
                        .inner
                        .consensus
                        .queue_transaction_statement(TransactionQueueRequest {
                            id: transaction_id.clone(),
                            owner: owner.clone(),
                            domain: domain.clone(),
                            activity: self.transaction_activity(),
                            statement: admitted,
                            report: prepared_admission.report,
                            limits,
                        })
                        .await
                    {
                        Ok(transaction) => transaction,
                        Err(error) => {
                            return self.transaction_consensus_error_response(error).await;
                        }
                    };
                }
                Err(error) => return command_error(error.to_string()),
            }
        }

        let mut current = transaction;
        let committing = loop {
            nervix_primitives::task::consume_budget().await;
            match &current.state {
                TransactionState::Open(_) => {
                    let prepared = match self.prepare_transaction_commit(&current).await {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            let message = transaction_commit_error_message(&error);
                            match self
                                .fail_incomplete_transaction_commit(
                                    &current,
                                    &owner,
                                    self.transaction_activity(),
                                    &error,
                                )
                                .await
                            {
                                Ok(Some(transaction)) => {
                                    return standalone_transaction_result(&transaction);
                                }
                                Ok(None) => return command_error(message),
                                Err(error) => {
                                    return self.transaction_consensus_error_response(error).await;
                                }
                            }
                        }
                    };
                    let admission = self
                        .inner
                        .consensus
                        .start_transaction_commit(
                            transaction_id.clone(),
                            owner.clone(),
                            self.transaction_activity(),
                            prepared.expected_preview,
                            prepared.report,
                            prepared.plan,
                        )
                        .await;
                    match admission {
                        Ok(transaction) => break transaction,
                        Err(error)
                            if matches!(
                                error.current_context(),
                                ConsensusTransactionError::Mutation(
                                    TransactionMutationError::PreviewStale { .. }
                                        | TransactionMutationError::PlanningInputsChanged { .. }
                                )
                            ) =>
                        {
                            let refreshed = self
                                .inner
                                .consensus
                                .current_transaction(&transaction_id)
                                .await;
                            let Some(refreshed) = refreshed else {
                                return self.transaction_consensus_error_response(error).await;
                            };
                            current = refreshed;
                        }
                        Err(error) => {
                            return self.transaction_consensus_error_response(error).await;
                        }
                    }
                }
                TransactionState::Committing(_) | TransactionState::Finished(_) => break current,
            }
        };
        let finished = if matches!(committing.state, TransactionState::Finished(_)) {
            Ok(committing)
        } else {
            Box::pin(self.execute_replicated_commit(&transaction_id)).await
        };
        match finished {
            Ok(transaction) => standalone_transaction_result(&transaction),
            Err(error) => {
                if let Some(ConsensusError::LeadershipLost { leader_id }) =
                    error.downcast_ref::<ConsensusError>()
                {
                    self.not_leader_response("", leader_id.clone()).await
                } else {
                    let message = transaction_commit_error_message(&error);
                    command_error(format!(
                        "durable command transaction '{transaction_id}' remains applying: \
                         {message}"
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "standalone_tests.rs"]
mod tests;
