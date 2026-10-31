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

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_consensus::{
    ConsensusError, ConsensusTransactionError, ReplicatedTransaction, TransactionMutationError,
    TransactionOutcome, TransactionQueueAdmission, TransactionQueueLimits, TransactionQueueRequest,
    TransactionState, TransactionStatement, TransactionStatementRequest,
};
use nervix_models::{CommandExecutionReference, DomainName, Statement, UserName};

use super::{
    super::{
        command_result::{
            CommandDiagnostic, CommandDisposition, CommandResult, OutcomeUnknownCause,
        },
        model_mutation::command_error,
        session_service::SessionServiceImpl,
    },
    PreparedTransactionAdmission, TransactionCommitError, is_queueable_transaction_statement,
    transaction_commit_error_message, transaction_commit_result, transaction_statement_label,
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

/// The command one coordinator applies, which every step of its preparation and commit admission
/// checks against the command's transaction.
struct StandaloneCommand<'a> {
    transaction_id: &'a str,
    owner: &'a UserName,
    domain: &'a DomainName,
    request: &'a TransactionStatementRequest,
    limits: TransactionQueueLimits,
}

/// Why one preparation step did not bring the command's statement into its transaction.
enum StandaloneSetback {
    /// Refused against this coordinator's observation of the transaction, so the refusal stands
    /// only while the transaction has not moved since.
    Refused(Box<CommandResult>),
    /// Ended in an outcome no later observation of the transaction changes, such as lost
    /// leadership or a transaction bound to another owner.
    Ended(Box<CommandResult>),
}

impl StandaloneSetback {
    fn refused(result: CommandResult) -> Self {
        Self::Refused(Box::new(result))
    }

    fn ended(result: CommandResult) -> Self {
        Self::Ended(Box::new(result))
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
        let request = TransactionStatementRequest {
            request_reference,
            expected_position: 0,
            source,
            statement,
        };
        let command = StandaloneCommand {
            transaction_id: &transaction_id,
            owner: &owner,
            domain: &domain,
            request: &request,
            limits: TransactionQueueLimits {
                max_statements: self.inner.transaction_max_statements,
                max_source_bytes: self.inner.transaction_max_source_bytes,
            },
        };
        let observed = self
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
        let admitted = match self.admit_standalone_statement(&command, observed).await {
            Ok(transaction) => transaction,
            Err(result) => return *result,
        };
        let committing = match self.admit_standalone_commit(&command, admitted).await {
            Ok(transaction) => transaction,
            Err(result) => return *result,
        };
        let finished = if matches!(committing.state, TransactionState::Finished(_)) {
            Ok(committing)
        } else {
            Box::pin(self.execute_replicated_commit(&transaction_id)).await
        };
        let error = match finished {
            Ok(transaction) => return standalone_transaction_result(&transaction),
            Err(error) => error,
        };
        if let Some(ConsensusError::LeadershipLost { observed }) =
            error.downcast_ref::<ConsensusError>()
        {
            return self.not_leader_response("", observed.clone()).await;
        }
        let message = transaction_commit_error_message(&error);
        let attempt = self
            .inner
            .consensus
            .current_transaction(&transaction_id)
            .await;
        if let Some(attempt) = attempt
            && let TransactionState::Committing(_) = attempt.state
        {
            // The committing transaction holds the command's domain mutation lease, and a pause
            // its step engaged stays its own until that step's application completes. A terminal
            // result would release the lease the transaction needs to finish, so the command stays
            // applying and recovery under the same reference finishes the transaction first.
            return CommandResult::new(
                CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::StillApplying),
                format!(
                    "durable command transaction '{transaction_id}' remains applying: {message}; \
                     retry it with the same reference to learn its outcome"
                ),
            );
        }
        command_error(format!(
            "durable command transaction '{transaction_id}' did not complete: {message}"
        ))
    }

    /// Brings the command's statement into its transaction, starting from `observed`, what this
    /// coordinator last saw of it, and opening the transaction when it saw none.
    ///
    /// Answers the transaction once it holds the statement or has moved past queuing. After a
    /// leadership change another coordinator can open and advance the same transaction while this
    /// one still acts on an earlier observation, so a refusal is the command's outcome only while
    /// the transaction stands where this coordinator observed it. Progress recorded since is
    /// recovered under the same execution reference instead.
    async fn admit_standalone_statement(
        &self,
        command: &StandaloneCommand<'_>,
        mut observed: Option<ReplicatedTransaction>,
    ) -> Result<ReplicatedTransaction, Box<CommandResult>> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let admission = match &observed {
                None => self.open_standalone_transaction(command).await,
                Some(transaction) => {
                    self.queue_standalone_statement(command, transaction, None)
                        .await
                }
            };
            let refusal = match admission {
                Ok(transaction) => return Ok(transaction),
                Err(StandaloneSetback::Ended(result)) => return Err(result),
                Err(StandaloneSetback::Refused(result)) => result,
            };
            let Some(progressed) = self
                .standalone_transaction_progress(command, observed.as_ref())
                .await
            else {
                return Err(refusal);
            };
            observed = Some(progressed);
        }
    }

    /// Opens the command's transaction and queues its statement, preflighted against the empty
    /// transaction this coordinator opens.
    async fn open_standalone_transaction(
        &self,
        command: &StandaloneCommand<'_>,
    ) -> Result<ReplicatedTransaction, StandaloneSetback> {
        let candidate = ReplicatedTransaction::open_for_command(
            command.transaction_id.to_owned(),
            command.domain.clone(),
            command.owner.clone(),
            self.transaction_activity(),
            command.request.request_reference.clone(),
        );
        if let Err(error) = candidate.queue_admission(
            command.owner,
            command.domain,
            command.request,
            command.limits,
        ) {
            return Err(StandaloneSetback::refused(command_error(error.to_string())));
        }
        let preflight = self
            .preflight_transaction_statement(&candidate, command.request)
            .await;
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_standalone_transaction_if_armed(
                self.inner.consensus.local_node_id(),
                command.domain,
                crate::fault_injection::StandaloneTransactionBoundary::Preflight,
            )
            .await;
        let prepared = match preflight {
            Ok(prepared) => prepared,
            Err(error) => {
                return Err(StandaloneSetback::refused(command_error(
                    super::TransactionPreflightError::public_message(&error),
                )));
            }
        };
        if let Err(error) = self
            .inner
            .consensus
            .open_transaction(candidate, self.inner.transaction_max_open)
            .await
            && self
                .inner
                .consensus
                .current_transaction(command.transaction_id)
                .await
                .is_none()
        {
            return Err(StandaloneSetback::ended(
                self.transaction_consensus_error_response(error).await,
            ));
        }
        let opened = self
            .inner
            .consensus
            .current_transaction(command.transaction_id)
            .await
            .verified("the durable command transaction was opened or observed above");
        self.queue_standalone_statement(command, &opened, Some(prepared))
            .await
    }

    /// Queues the command's statement into `transaction` unless the transaction already holds it
    /// or has moved past queuing.
    ///
    /// `prepared` is the admission preflighted while opening the transaction; without one, the
    /// statement is preflighted against `transaction`.
    async fn queue_standalone_statement(
        &self,
        command: &StandaloneCommand<'_>,
        transaction: &ReplicatedTransaction,
        prepared: Option<PreparedTransactionAdmission>,
    ) -> Result<ReplicatedTransaction, StandaloneSetback> {
        if transaction.owner != *command.owner || transaction.domain != *command.domain {
            return Err(StandaloneSetback::ended(command_error(format!(
                "durable command transaction '{}' is bound to a different owner or domain",
                transaction.id
            ))));
        }
        let TransactionState::Open(_) = &transaction.state else {
            return Ok(transaction.clone());
        };
        let admission = transaction.queue_admission(
            command.owner,
            command.domain,
            command.request,
            command.limits,
        );
        match admission {
            Ok(TransactionQueueAdmission::Existing(_)) => return Ok(transaction.clone()),
            Ok(TransactionQueueAdmission::New) => {}
            Err(error) => {
                return Err(StandaloneSetback::refused(command_error(error.to_string())));
            }
        }
        let prepared = match prepared {
            Some(prepared) => prepared,
            None => {
                let preflight = self
                    .preflight_transaction_statement(transaction, command.request)
                    .await;
                match preflight {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        return Err(StandaloneSetback::refused(command_error(
                            super::TransactionPreflightError::public_message(&error),
                        )));
                    }
                }
            }
        };
        let admitted = TransactionStatement::admitted(command.request.clone(), prepared.result);
        let queued = self
            .inner
            .consensus
            .queue_transaction_statement(TransactionQueueRequest {
                id: command.transaction_id.to_owned(),
                owner: command.owner.clone(),
                domain: command.domain.clone(),
                activity: self.transaction_activity(),
                statement: admitted,
                report: prepared.report,
                limits: command.limits,
            })
            .await;
        let error = match queued {
            Ok(transaction) => return Ok(transaction),
            Err(error) => error,
        };
        // The ledger refuses a mutation against the transaction as it now stands, which another
        // coordinator may have moved; any other failure ends the step whatever its progress.
        let ConsensusTransactionError::Mutation(_) = error.current_context() else {
            let result = self.transaction_consensus_error_response(error).await;
            return Err(StandaloneSetback::ended(result));
        };
        let result = self.transaction_consensus_error_response(error).await;
        Err(StandaloneSetback::refused(result))
    }

    /// Starts the commit of the command's transaction once the transaction holds its statement.
    ///
    /// Answers the transaction once it is committing or finished. The commit is planned against
    /// this coordinator's observation of the transaction, so a planning refusal is the command's
    /// outcome only while the transaction stands where it was planned; progress another
    /// coordinator recorded since is recovered instead.
    async fn admit_standalone_commit(
        &self,
        command: &StandaloneCommand<'_>,
        admitted: ReplicatedTransaction,
    ) -> Result<ReplicatedTransaction, Box<CommandResult>> {
        let mut current = admitted;
        loop {
            nervix_primitives::task::consume_budget().await;
            let TransactionState::Open(_) = &current.state else {
                return Ok(current);
            };
            let prepared = match self.prepare_transaction_commit(&current).await {
                Ok(prepared) => prepared,
                Err(refusal) => {
                    let progressed = self
                        .standalone_transaction_progress(command, Some(&current))
                        .await;
                    if let Some(progressed) = progressed {
                        current = progressed;
                        continue;
                    }
                    let result = self
                        .refuse_standalone_commit(command, &current, &refusal)
                        .await;
                    return Err(Box::new(result));
                }
            };
            let admission = self
                .inner
                .consensus
                .start_transaction_commit(
                    command.transaction_id.to_owned(),
                    command.owner.clone(),
                    self.transaction_activity(),
                    prepared.expected_preview,
                    prepared.report,
                    prepared.plan,
                )
                .await;
            match admission {
                Ok(transaction) => return Ok(transaction),
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
                        .current_transaction(command.transaction_id)
                        .await;
                    let Some(refreshed) = refreshed else {
                        let result = self.transaction_consensus_error_response(error).await;
                        return Err(Box::new(result));
                    };
                    current = refreshed;
                }
                // The ledger refused the commit against the transaction as it now stands, which
                // another coordinator may have moved past the one this commit was planned from.
                Err(error)
                    if matches!(
                        error.current_context(),
                        ConsensusTransactionError::Mutation(_)
                    ) =>
                {
                    let progressed = self
                        .standalone_transaction_progress(command, Some(&current))
                        .await;
                    let Some(progressed) = progressed else {
                        let result = self.transaction_consensus_error_response(error).await;
                        return Err(Box::new(result));
                    };
                    current = progressed;
                }
                Err(error) => {
                    let result = self.transaction_consensus_error_response(error).await;
                    return Err(Box::new(result));
                }
            }
        }
    }

    /// The command's outcome when its commit planning is refused against a transaction that has
    /// not moved: the refusal fails the commit when the plan names the operation it fails at, and
    /// is answered as it stands otherwise.
    async fn refuse_standalone_commit(
        &self,
        command: &StandaloneCommand<'_>,
        transaction: &ReplicatedTransaction,
        refusal: &Report<TransactionCommitError>,
    ) -> CommandResult {
        let failed = self
            .fail_incomplete_transaction_commit(
                transaction,
                command.owner,
                self.transaction_activity(),
                refusal,
            )
            .await;
        match failed {
            Ok(Some(transaction)) => standalone_transaction_result(&transaction),
            Ok(None) => command_error(transaction_commit_error_message(refusal)),
            Err(error) => self.transaction_consensus_error_response(error).await,
        }
    }

    /// The command's transaction as it now stands, when it moved since `observed`.
    ///
    /// The comparison covers the whole durable transaction, so its opening, an accepted statement,
    /// a commit admission, commit progress and a recorded outcome each count as progress that a
    /// refusal decided against `observed` did not see.
    async fn standalone_transaction_progress(
        &self,
        command: &StandaloneCommand<'_>,
        observed: Option<&ReplicatedTransaction>,
    ) -> Option<ReplicatedTransaction> {
        let current = self
            .inner
            .consensus
            .current_transaction(command.transaction_id)
            .await?;
        if observed == Some(&current) {
            return None;
        }
        Some(current)
    }
}

#[cfg(test)]
#[path = "standalone_tests.rs"]
mod tests;
