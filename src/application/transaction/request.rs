//! Exact durable transaction requests applied to the replicated transaction ledger.
//!
//! Layer: control plane.
//!
//! - **Owns.** Idempotent application of identified transaction opens, statement appends and
//!   reverts.
//! - **Depends on.** The transaction lifecycle for preflight and result construction, and consensus
//!   for the authoritative transaction ledger.
//! - **Must not know.** Session transport state, command parsing or runtime execution details.

use meticulous::OptionExt as _;
use nervix_consensus::{
    ReplicatedTransaction, TransactionActivity, TransactionOutcome, TransactionQueueAdmission,
    TransactionQueueLimits, TransactionQueueRequest, TransactionState, TransactionStatement,
    TransactionStatementRequest,
};
use nervix_models::{DomainName, UserName};

use super::{
    super::{
        command_result::CommandResult,
        model_mutation::{command_error, command_ok},
        session_service::SessionServiceImpl,
    },
    admitted_command_result, is_queueable_transaction_statement, transaction_commit_result,
    transaction_statement_label, transaction_status,
};

impl SessionServiceImpl {
    pub(in crate::application) async fn begin_identified_transaction(
        &self,
        id: String,
        domain: DomainName,
        owner: UserName,
        activity: TransactionActivity,
    ) -> CommandResult {
        if let Some(existing) = self.inner.consensus.current_transaction(&id).await {
            if existing.owner != owner || existing.domain != domain {
                return command_error(format!(
                    "transaction '{id}' is bound to a different owner or domain"
                ));
            }
            let mut result = command_ok(format!("transaction started: id '{id}'"));
            result.transaction = Some(transaction_status(&existing));
            return result;
        }

        let transaction =
            ReplicatedTransaction::open(id.clone(), domain.clone(), owner.clone(), activity);
        match self
            .inner
            .consensus
            .open_transaction(transaction, self.inner.transaction_max_open)
            .await
        {
            Ok(transaction) => {
                let mut result = command_ok(format!("transaction started: id '{id}'"));
                result.transaction = Some(transaction_status(&transaction));
                result
            }
            Err(error) => {
                let Some(existing) = self.inner.consensus.current_transaction(&id).await else {
                    return self.transaction_consensus_error_response(error).await;
                };
                if existing.owner != owner || existing.domain != domain {
                    return command_error(format!(
                        "transaction '{id}' is bound to a different owner or domain"
                    ));
                }
                let mut result = command_ok(format!("transaction started: id '{id}'"));
                result.transaction = Some(transaction_status(&existing));
                result
            }
        }
    }

    pub(in crate::application) async fn queue_identified_transaction_statement(
        &self,
        id: String,
        owner: UserName,
        domain: DomainName,
        queued: TransactionStatementRequest,
        activity: TransactionActivity,
    ) -> CommandResult {
        if !is_queueable_transaction_statement(&queued.statement) {
            return command_error(format!(
                "{} cannot be queued in a transaction; a transaction applies to one existing \
                 domain and queues only that domain's configuration statements",
                transaction_statement_label(&queued.statement)
            ));
        }
        let Some(transaction) = self.inner.consensus.current_transaction(&id).await else {
            return command_error(format!("transaction '{id}' is unknown"));
        };
        let limits = TransactionQueueLimits {
            max_statements: self.inner.transaction_max_statements,
            max_source_bytes: self.inner.transaction_max_source_bytes,
        };
        match transaction.queue_admission(&owner, &domain, &queued, limits) {
            Ok(TransactionQueueAdmission::Existing(admission)) => {
                return admitted_command_result(&admission, &transaction);
            }
            Ok(TransactionQueueAdmission::New) => {}
            Err(error) => return command_error(error.to_string()),
        }
        let prepared = match self
            .preflight_transaction_statement(&transaction, &queued)
            .await
        {
            Ok(admission) => admission,
            Err(error) => return command_error(error),
        };
        let request_reference = queued.request_reference.clone();
        let queued = TransactionStatement::admitted(queued, prepared.result);
        match self
            .inner
            .consensus
            .queue_transaction_statement(TransactionQueueRequest {
                id,
                owner,
                domain,
                activity,
                statement: queued,
                report: prepared.report,
                limits,
            })
            .await
        {
            Ok(transaction) => {
                if matches!(transaction.state, TransactionState::Finished(_)) {
                    return transaction_commit_result(&transaction);
                }
                // A transaction contains at most `transaction_max_statements` entries, so this
                // lookup is bounded by the queue limit checked before the proposal.
                let admitted = transaction
                    .statements
                    .iter()
                    .find(|statement| statement.request_reference == request_reference)
                    .verified("a successful queue proposal retains the admitted statement");
                admitted_command_result(&admitted.admission, &transaction)
            }
            Err(error) => self.transaction_consensus_error_response(error).await,
        }
    }

    pub(in crate::application) async fn revert_identified_transaction(
        &self,
        id: String,
        owner: UserName,
        activity: TransactionActivity,
    ) -> CommandResult {
        let previous = self.inner.consensus.current_transaction(&id).await;
        if let Some(transaction) = previous.as_ref()
            && transaction.owner == owner
            && matches!(
                transaction.finished_outcome(),
                Some(TransactionOutcome::Reverted)
            )
        {
            return reverted_transaction_result(transaction);
        }
        match self
            .inner
            .consensus
            .revert_transaction(id.clone(), owner.clone(), activity)
            .await
        {
            Ok(transaction) => {
                if !matches!(
                    transaction.finished_outcome(),
                    Some(TransactionOutcome::Reverted)
                ) {
                    return transaction_commit_result(&transaction);
                }
                reverted_transaction_result(&transaction)
            }
            Err(error) => {
                if let Some(transaction) = self.inner.consensus.current_transaction(&id).await
                    && transaction.owner == owner
                    && matches!(
                        transaction.finished_outcome(),
                        Some(TransactionOutcome::Reverted)
                    )
                {
                    return reverted_transaction_result(&transaction);
                }
                self.transaction_consensus_error_response(error).await
            }
        }
    }
}

fn reverted_transaction_result(transaction: &ReplicatedTransaction) -> CommandResult {
    let dropped = transaction.statement_count;
    let mut result = command_ok(format!(
        "transaction reverted: dropped {dropped} command(s); id '{}'",
        transaction.id
    ));
    result.transaction = Some(transaction_status(transaction));
    result
}
