//! Admission and retained outcomes for persistent session commands.
//!
//! Layer: control plane.
//! - **Owns.** Binding a command reference to one semantic request and publishing its final result.
//! - **Depends on.** Consensus command records and the authoritative visibility barrier.
//! - **Must not know.** Parser recovery, transport reconnect policy, or runtime implementation.

use std::{
    collections::BTreeSet,
    sync::{Arc as StdArc, Weak as StdWeak},
    time::Duration,
};

use ahash::RandomState;
use blake3::Hasher;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{
    CommandExecution, CommandExecutionAdmissionPolicy, CommandExecutionDiagnostic,
    CommandExecutionDisposition, CommandExecutionEffect, CommandExecutionPreviewStale,
    CommandExecutionRequestConflict, CommandExecutionResult, CommandExecutionState,
    CommandExecutionStatementDisposition, CommandExecutionStatementResult,
    CommandExecutionTransactionOperation, CommandExecutionTransactionRequest,
    CommandExecutionTransactionStatus, CommandExecutionTransactionTarget, ConsensusError,
};
use nervix_execution::sync::DashMap;
use nervix_models::{
    CommandExecutionReference, DomainName, DomainStartPoint, DomainState, DomainStatus, Statement,
    Timestamp, TransactionPosition, TransactionStatus, UserName,
};
use nervix_nspl::client_statement::ClientStatement;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tracing::warn;

use super::{
    authentication::{user_credentials, verify_password_hash},
    command_result::{CommandDiagnostic, CommandDisposition, CommandResult, OutcomeUnknownCause},
    domain_clock::current_timestamp,
    model_mutation::{command_error, is_persistent_statement},
    session_service::{SessionServiceImpl, conflicting_reference},
    subscription::{PendingSessionCommand, SessionCommandOperation, SessionSubscriptions},
    transaction::is_queueable_transaction_statement,
};

const DEFAULT_COMMAND_RETRY_VALIDITY: Duration = Duration::from_secs(15 * 60);
const DEFAULT_COMMAND_EXECUTION_CAPACITY: usize = 65_536;

#[derive(Debug, Clone, Copy, clap::Args)]
pub struct CommandExecutionPolicy {
    #[arg(
        long = "command-retry-validity",
        env = "NERVIX_COMMAND_RETRY_VALIDITY",
        default_value = "15m",
        value_parser = super::parse_human_duration
    )]
    retry_validity: Duration,
    #[arg(
        long = "command-execution-capacity",
        env = "NERVIX_COMMAND_EXECUTION_CAPACITY",
        default_value_t = DEFAULT_COMMAND_EXECUTION_CAPACITY
    )]
    capacity: usize,
}

impl CommandExecutionPolicy {
    pub fn new(retry_validity: Duration, capacity: usize) -> Self {
        Self {
            retry_validity,
            capacity,
        }
    }

    pub(in crate::application) fn retry_validity(self) -> Duration {
        self.retry_validity
    }

    pub(in crate::application) fn admission_at(
        self,
        now: Timestamp,
    ) -> CommandExecutionAdmissionPolicy {
        CommandExecutionAdmissionPolicy::at(now, self.retry_validity, self.capacity)
    }
}

impl Default for CommandExecutionPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_COMMAND_RETRY_VALIDITY,
            DEFAULT_COMMAND_EXECUTION_CAPACITY,
        )
    }
}

/// The leader-local owner of each durable execution reference.
///
/// An entry lives exactly as long as a request holds or waits for its owner, so a finished
/// reference leaves no lock behind while a late waiter still joins the owner it found.
pub(in crate::application) struct CommandExecutionOwners {
    inner: StdArc<CommandExecutionOwnersInner>,
}

struct CommandExecutionOwnersInner {
    locks: DashMap<CommandExecutionReference, StdWeak<CommandExecutionLock>, RandomState>,
}

struct CommandExecutionLock {
    owners: StdWeak<CommandExecutionOwnersInner>,
    reference: CommandExecutionReference,
    mutex: StdArc<AsyncMutex<()>>,
}

impl Drop for CommandExecutionLock {
    fn drop(&mut self) {
        let Some(owners) = self.owners.upgrade() else {
            return;
        };
        owners.locks.remove_if(&self.reference, |_, current| {
            current.as_ptr() == std::ptr::from_ref(self)
        });
    }
}

impl Default for CommandExecutionOwners {
    fn default() -> Self {
        Self {
            inner: StdArc::new(CommandExecutionOwnersInner {
                locks: DashMap::with_hasher(RandomState::new()),
            }),
        }
    }
}

impl CommandExecutionOwners {
    pub(in crate::application) async fn lock(
        &self,
        reference: CommandExecutionReference,
    ) -> CommandExecutionOwnerGuard {
        let lock = self.lock_for(&reference);
        let guard = lock.mutex.clone().lock_owned().await;
        CommandExecutionOwnerGuard {
            _guard: guard,
            _lock: lock,
        }
    }

    pub(in crate::application) fn try_lock(
        &self,
        reference: CommandExecutionReference,
    ) -> Option<CommandExecutionOwnerGuard> {
        let lock = self.lock_for(&reference);
        let guard = lock.mutex.clone().try_lock_owned().ok()?;
        Some(CommandExecutionOwnerGuard {
            _guard: guard,
            _lock: lock,
        })
    }

    fn lock_for(&self, reference: &CommandExecutionReference) -> StdArc<CommandExecutionLock> {
        let mut entry = self.inner.locks.entry(reference.clone()).or_default();
        if let Some(lock) = entry.upgrade() {
            return lock;
        }
        let lock = StdArc::new(CommandExecutionLock {
            owners: StdArc::downgrade(&self.inner),
            reference: reference.clone(),
            mutex: StdArc::new(AsyncMutex::new(())),
        });
        *entry = StdArc::downgrade(&lock);
        lock
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.locks.len()
    }
}

/// Fields drop in declaration order, so the owner is released before this guard stops keeping
/// the reference's entry alive.
pub(in crate::application) struct CommandExecutionOwnerGuard {
    _guard: OwnedMutexGuard<()>,
    _lock: StdArc<CommandExecutionLock>,
}

pub(in crate::application) struct PersistentCommandRequest {
    pub(in crate::application) domain: Option<DomainName>,
    pub(in crate::application) expected_transaction_position: Option<TransactionPosition>,
    pub(in crate::application) digest: [u8; 32],
    body: PersistentCommandRequestBody,
}

enum PersistentCommandRequestBody {
    Statement {
        source: String,
        statement: Statement,
    },
    Transaction(CommandExecutionTransactionRequest),
}

#[derive(Debug, Error)]
pub(in crate::application) enum PersistentCommandRequestError {
    #[error("one ordinary command execution reference cannot own multiple persistent statements")]
    MultiplePersistentStatements,
    #[error("failed to identify command semantics: {message}")]
    Encoding { message: String },
    #[error("command semantics exceed the supported size")]
    SemanticsTooLarge,
    #[error("a durable transaction request contains a non-transaction operation")]
    InvalidTransactionOperation,
    #[error("session-scoped and client-local statements cannot be queued in a transaction")]
    NonServerTransactionStatement,
    #[error("a durable transaction append is missing its expected queue position")]
    MissingTransactionPosition,
}

impl PersistentCommandRequest {
    pub(in crate::application) fn from_operations(
        operations: &[SessionCommandOperation],
        request_domain: Option<&DomainName>,
    ) -> Result<Option<Self>, Report<PersistentCommandRequestError>> {
        let mut persistent_command = None;
        for operation in operations {
            let SessionCommandOperation::Execute(pending) = operation else {
                continue;
            };
            let nervix_nspl::client_statement::ClientStatement::Server(statement) =
                &pending.statement
            else {
                continue;
            };
            if is_persistent_statement(statement) {
                if persistent_command.is_some() {
                    return Err(Report::new(
                        PersistentCommandRequestError::MultiplePersistentStatements,
                    ));
                }
                persistent_command = Some((pending.source.clone(), statement.clone()));
            }
        }
        let Some((source, statement)) = persistent_command else {
            return Ok(None);
        };

        let domain = request_domain.cloned();
        let mut hasher = Hasher::new();
        match &domain {
            Some(domain) => hasher.update(domain.as_str().as_bytes()),
            None => hasher.update(&[]),
        };
        let mut digest_statement = statement.clone();
        if let Statement::CreateUser(create) = &mut digest_statement {
            create.body.password.clear();
        }
        let encoded =
            rkyv::to_bytes::<rkyv::rancor::Error>(&digest_statement).map_err(|error| {
                Report::new(PersistentCommandRequestError::Encoding {
                    message: error.to_string(),
                })
            })?;
        let encoded_bytes = encoded.as_slice();
        let encoded_length = u64::try_from(encoded_bytes.len())
            .map_err(|_| Report::new(PersistentCommandRequestError::SemanticsTooLarge))?;
        hasher.update(&encoded_length.to_le_bytes());
        hasher.update(encoded_bytes);
        Ok(Some(Self {
            domain,
            expected_transaction_position: None,
            digest: *hasher.finalize().as_bytes(),
            body: PersistentCommandRequestBody::Statement { source, statement },
        }))
    }

    pub(in crate::application) fn transaction(
        operations: &[SessionCommandOperation],
        domain: DomainName,
        query: &str,
        expected_transaction_position: Option<usize>,
        target: CommandExecutionTransactionTarget,
    ) -> Result<Self, Report<PersistentCommandRequestError>> {
        let mut durable_operations = Vec::with_capacity(operations.len());
        for operation in operations {
            match operation {
                SessionCommandOperation::Begin { .. } => {
                    if !target.opens_transaction() {
                        return Err(Report::new(
                            PersistentCommandRequestError::InvalidTransactionOperation,
                        ));
                    }
                }
                SessionCommandOperation::Queue(command) => {
                    let ClientStatement::Server(statement) = &command.statement else {
                        return Err(Report::new(
                            PersistentCommandRequestError::NonServerTransactionStatement,
                        ));
                    };
                    let Some(position) = command.expected_transaction_position else {
                        return Err(Report::new(
                            PersistentCommandRequestError::MissingTransactionPosition,
                        ));
                    };
                    durable_operations.push(CommandExecutionTransactionOperation::Queue(Box::new(
                        nervix_consensus::TransactionStatementRequest {
                            request_reference: command.request_reference.clone(),
                            expected_position: position,
                            source: command.source.clone(),
                            statement: statement.clone(),
                        },
                    )));
                }
                SessionCommandOperation::Commit { expected_preview } => {
                    durable_operations.push(CommandExecutionTransactionOperation::Commit {
                        expected_preview: expected_preview.clone(),
                    });
                }
                SessionCommandOperation::Revert => {
                    durable_operations.push(CommandExecutionTransactionOperation::Revert);
                }
                SessionCommandOperation::Execute(_) => {
                    return Err(Report::new(
                        PersistentCommandRequestError::InvalidTransactionOperation,
                    ));
                }
            }
        }

        let digest = Self::transaction_digest(query)?;
        Ok(Self {
            domain: Some(domain),
            expected_transaction_position: expected_transaction_position
                .map(TransactionPosition::new),
            digest,
            body: PersistentCommandRequestBody::Transaction(CommandExecutionTransactionRequest {
                target,
                operations: durable_operations,
            }),
        })
    }

    pub(in crate::application) fn transaction_digest(
        query: &str,
    ) -> Result<[u8; 32], Report<PersistentCommandRequestError>> {
        let mut hasher = Hasher::new();
        let query_length = u64::try_from(query.len())
            .map_err(|_| Report::new(PersistentCommandRequestError::SemanticsTooLarge))?;
        hasher.update(&query_length.to_le_bytes());
        hasher.update(query.as_bytes());
        Ok(*hasher.finalize().as_bytes())
    }

    fn mutation_domains(&self) -> BTreeSet<DomainName> {
        let PersistentCommandRequestBody::Statement { statement, .. } = &self.body else {
            return BTreeSet::new();
        };
        match statement {
            Statement::CreateDomain(create) => BTreeSet::from([create.id.clone()]),
            Statement::DrainNode(_) => BTreeSet::new(),
            statement if statement.requires_domain_mutation_ownership() => {
                self.domain.iter().cloned().collect()
            }
            _ => BTreeSet::new(),
        }
    }
}

fn transaction_targets_match(
    existing: &CommandExecution,
    requested: &PersistentCommandRequest,
) -> bool {
    let requested = match &requested.body {
        PersistentCommandRequestBody::Transaction(request) => Some(request),
        PersistentCommandRequestBody::Statement { .. } => None,
    };
    match (existing.transaction_target(), requested) {
        (Some(existing), Some(requested)) => existing.identifies_same_request(&requested.target),
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn reconcile_persistent_commands(
        &self,
        finished_before: Timestamp,
        retry_fence: Timestamp,
    ) {
        let reconciliation = self
            .inner
            .consensus
            .command_execution_reconciliation(finished_before, retry_fence)
            .await;
        let maintenance_due = reconciliation.maintenance_due;
        for reference in reconciliation.applying {
            tokio::task::consume_budget().await;
            let Some(execution_guard) = self.inner.command_executions.try_lock(reference.clone())
            else {
                continue;
            };
            let Some(execution) = self
                .inner
                .consensus
                .current_command_execution(&reference)
                .await
            else {
                continue;
            };
            if !execution.is_applying() {
                continue;
            }
            let owner = execution
                .owner()
                .verified("the reconciliation index contains only applying executions")
                .clone();
            let request_digest = execution
                .request_digest()
                .verified("the reconciliation index contains only applying executions");
            let service = self.clone();
            self.inner.service_tasks.spawn(async move {
                let _execution_guard = execution_guard;
                let mut subscriptions = SessionSubscriptions::for_user(owner.clone());
                let result = service
                    .execute_persistent_command(&execution, &mut subscriptions)
                    .await;
                if result.is_not_leader() {
                    return;
                }
                if let Err(error) = service
                    .finish_persistent_command(
                        execution.reference.clone(),
                        owner,
                        request_digest,
                        &result,
                    )
                    .await
                {
                    service.broadcast_error(format!(
                        "failed to record resumed command execution '{}': {}",
                        execution.reference, error.message
                    ));
                }
            });
        }
        if maintenance_due
            && let Err(error) = self
                .inner
                .consensus
                .reclaim_command_executions(finished_before, retry_fence)
                .await
        {
            warn!(error = %error, "failed to reclaim retained command execution history");
        }
    }

    pub(in crate::application) async fn admit_persistent_command(
        &self,
        reference: CommandExecutionReference,
        owner: UserName,
        request: &PersistentCommandRequest,
    ) -> Result<CommandAdmission, Box<CommandResult>> {
        if let Some(existing) = self
            .inner
            .consensus
            .current_command_execution(&reference)
            .await
        {
            if existing.is_expired() {
                let message = format!("command execution reference '{reference}' has expired");
                return Err(Box::new(CommandResult {
                    diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
                    ..CommandResult::new(CommandDisposition::ExecutionReferenceExpired, message)
                }));
            }
            if let Some(conflict) = existing.request_conflict(
                &owner,
                request.domain.as_ref(),
                request.expected_transaction_position,
                request.digest,
            ) {
                return Err(Box::new(conflicting_reference(&reference, conflict)));
            }
            if !transaction_targets_match(&existing, request) {
                return Err(Box::new(conflicting_reference(
                    &reference,
                    CommandExecutionRequestConflict::Position,
                )));
            }
            if let (
                Some(password_hash),
                PersistentCommandRequestBody::Statement {
                    statement: Statement::CreateUser(create),
                    ..
                },
            ) = (existing.password_hash(), &request.body)
                && !verify_password_hash(password_hash.to_string(), create.body.password.clone())
                    .await
            {
                // The password is left out of the request digest, so a retry that changed it is
                // a different command under the same reference.
                return Err(Box::new(conflicting_reference(
                    &reference,
                    CommandExecutionRequestConflict::Content,
                )));
            }
            return Ok(CommandAdmission::Existing(existing));
        }

        let effect = match &request.body {
            PersistentCommandRequestBody::Transaction(transaction) => {
                CommandExecutionEffect::TransactionRequest(Box::new(transaction.clone()))
            }
            PersistentCommandRequestBody::Statement {
                source: _,
                statement: Statement::CreateDomain(create),
            } => {
                let existing = self.inner.consensus.current_domain(&create.id).await;
                let existed_at_admission = existing.is_some();
                let state = match existing {
                    Some(existing) => existing,
                    None => DomainState {
                        id: create.id.clone(),
                        config: create.config.clone(),
                        status: DomainStatus::Stopped,
                        start_version: 0,
                        last_start: DomainStartPoint::Resume,
                        clock: None,
                    },
                };
                CommandExecutionEffect::CreateDomain {
                    if_not_exists: create.if_not_exists,
                    existed_at_admission,
                    state: Box::new(state),
                }
            }
            PersistentCommandRequestBody::Statement {
                source: _,
                statement: Statement::CreateUser(create),
            } => {
                let user = user_credentials(create.body.name.clone(), create.body.password.clone())
                    .await
                    .map_err(|error| Box::new(command_error(error.to_string())))?;
                CommandExecutionEffect::CreateUser {
                    if_not_exists: create.if_not_exists,
                    name: user.name,
                    password_hash: user.password_hash,
                }
            }
            PersistentCommandRequestBody::Statement {
                source: _,
                statement: Statement::DropNode(drop),
            } => {
                let availability = self.inner.cluster.availability_state().await;
                let mut latest_nodes = availability.latest_nodes_by_id();
                let Some(node) = latest_nodes.remove(&drop.node_id) else {
                    return Err(Box::new(command_error(format!(
                        "cannot identify the current incarnation of raft member '{}'",
                        drop.node_id
                    ))));
                };
                let membership = self.inner.consensus.membership_nodes().await;
                CommandExecutionEffect::DropNode {
                    identity: node.identity(),
                    member_at_admission: membership.contains_key(&drop.node_id),
                }
            }
            PersistentCommandRequestBody::Statement { source, statement }
                if is_queueable_transaction_statement(statement) =>
            {
                request.domain.as_ref().ok_or_else(|| {
                    Box::new(command_error("no active domain selected".to_string()))
                })?;
                CommandExecutionEffect::Transaction {
                    transaction_id: format!("command.{}", reference.as_str()),
                    source: source.clone(),
                    statement: Box::new(statement.clone()),
                }
            }
            PersistentCommandRequestBody::Statement { source, statement } => {
                CommandExecutionEffect::Statement {
                    source: source.clone(),
                    statement: Box::new(statement.clone()),
                }
            }
        };
        let admitted_at = current_timestamp();
        let policy = self
            .inner
            .command_execution_policy
            .admission_at(admitted_at);
        let execution = CommandExecution::applying_at_position(
            reference.clone(),
            owner,
            request.domain.clone(),
            request.expected_transaction_position,
            request.digest,
            admitted_at,
            effect,
        );
        match self
            .inner
            .consensus
            .admit_command_execution(execution, request.mutation_domains(), policy)
            .await
        {
            Ok(execution) => Ok(CommandAdmission::Admitted(execution)),
            Err(error) => {
                if let ConsensusError::LeadershipLost { .. } = error.current_context() {
                    // The admission proposal may have reached the log before leadership moved,
                    // so whether the command was admitted is not known here.
                    return Err(Box::new(outcome_unknown(
                        OutcomeUnknownCause::LeadershipLost,
                        format!(
                            "leadership moved while command execution reference '{reference}' was \
                             being admitted; retry it with the same reference to learn its outcome"
                        ),
                    )));
                }
                let message = error.to_string();
                let result = self
                    .consensus_error_response(error.current_context(), message)
                    .await;
                Err(Box::new(result))
            }
        }
    }

    pub(in crate::application) async fn execute_persistent_command(
        &self,
        execution: &CommandExecution,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        // Keep the independently owned command paths out of this dispatcher's poll frame. In a
        // debug build their combined state exceeds the stack available to an ordinary session.
        let Some(effect) = execution.effect().cloned() else {
            return command_error(format!(
                "command execution reference '{}' is no longer applying",
                execution.reference
            ));
        };
        match effect {
            CommandExecutionEffect::CreateDomain {
                if_not_exists,
                existed_at_admission,
                state,
            } => {
                let Some(mutation) = execution.domain_mutation(&state.id) else {
                    return command_error(format!(
                        "durable domain creation lost mutation ownership for '{}'",
                        state.id.as_str()
                    ));
                };
                Box::pin(self.apply_persistent_domain_creation(
                    if_not_exists,
                    existed_at_admission,
                    *state,
                    Some(mutation),
                ))
                .await
            }
            CommandExecutionEffect::Transaction {
                transaction_id,
                source,
                statement,
            } => {
                let Some(domain) = execution.domain().cloned() else {
                    return command_error(
                        "durable configuration application lost its domain".to_string(),
                    );
                };
                Box::pin(
                    self.execute_standalone_transaction(
                        transaction_id,
                        execution.reference.clone(),
                        execution
                            .owner()
                            .verified("an applying execution retains its owner")
                            .clone(),
                        domain,
                        source,
                        *statement,
                    ),
                )
                .await
            }
            CommandExecutionEffect::TransactionRequest(request) => {
                let Some(domain) = execution.domain().cloned() else {
                    return command_error(
                        "durable transaction request lost its target domain".to_string(),
                    );
                };
                Box::pin(
                    self.execute_durable_transaction_request(
                        execution
                            .owner()
                            .verified("an applying execution retains its owner")
                            .clone(),
                        domain,
                        *request,
                    ),
                )
                .await
            }
            CommandExecutionEffect::Statement { source, statement } => match *statement {
                Statement::Relocate(relocation) => {
                    let Some(domain) = execution.domain() else {
                        return command_error("durable relocation lost its domain".to_string());
                    };
                    let Some(mutation) = execution.domain_mutation(domain) else {
                        return command_error(format!(
                            "durable relocation lost mutation ownership for '{}'",
                            domain.as_str()
                        ));
                    };
                    return Box::pin(self.relocate(domain, relocation, Some(mutation))).await;
                }
                Statement::DrainNode(drain) => {
                    return Box::pin(self.drain_node(drain.node_id, Some(execution))).await;
                }
                statement => {
                    let command = PendingSessionCommand {
                        request_reference: execution.reference.clone(),
                        expected_transaction_position: None,
                        source,
                        statement: ClientStatement::Server(statement),
                        domain: execution.domain().cloned(),
                    };
                    return Box::pin(self.process_session_command_operations(
                        vec![SessionCommandOperation::Execute(command)],
                        subscriptions,
                    ))
                    .await;
                }
            },
            CommandExecutionEffect::CreateUser {
                if_not_exists,
                name,
                password_hash,
            } => {
                Box::pin(self.apply_persistent_user_creation(
                    if_not_exists,
                    nervix_consensus::UserCredentials {
                        name,
                        password_hash,
                    },
                ))
                .await
            }
            CommandExecutionEffect::DropNode {
                identity,
                member_at_admission,
            } => Box::pin(self.drop_admitted_node(identity, member_at_admission)).await,
        }
    }

    pub(in crate::application) async fn finish_persistent_command(
        &self,
        reference: CommandExecutionReference,
        owner: UserName,
        request_digest: [u8; 32],
        result: &CommandResult,
    ) -> Result<CommandResult, Box<CommandResult>> {
        let durable_result = durable_command_result(result);
        let execution = match self
            .inner
            .consensus
            .finish_command_execution(
                reference.clone(),
                owner,
                request_digest,
                current_timestamp(),
                durable_result,
            )
            .await
        {
            Ok(execution) => execution,
            Err(error) => {
                if let ConsensusError::LeadershipLost { .. } = error.current_context() {
                    // The effects applied, but recording their outcome may not have reached the
                    // log before leadership moved. The next leader finishes the command.
                    return Err(Box::new(outcome_unknown(
                        OutcomeUnknownCause::LeadershipLost,
                        format!(
                            "leadership moved while command execution reference '{reference}' was \
                             being finalized; retry it with the same reference to learn its \
                             outcome"
                        ),
                    )));
                }
                let message = error.to_string();
                let result = self
                    .consensus_error_response(error.current_context(), message)
                    .await;
                return Err(Box::new(result));
            }
        };
        self.result_from_finished_execution(&reference, execution)
            .await
    }

    pub(in crate::application) async fn result_from_finished_execution(
        &self,
        reference: &CommandExecutionReference,
        execution: CommandExecution,
    ) -> Result<CommandResult, Box<CommandResult>> {
        match execution.state {
            CommandExecutionState::Applying { .. } => Err(Box::new(outcome_unknown(
                OutcomeUnknownCause::StillApplying,
                format!("command execution reference '{reference}' is still applying"),
            ))),
            CommandExecutionState::Expired => {
                let message = format!("command execution reference '{reference}' has expired");
                Err(Box::new(CommandResult {
                    diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
                    ..CommandResult::new(CommandDisposition::ExecutionReferenceExpired, message)
                }))
            }
            CommandExecutionState::Finished {
                outcome_revision,
                result,
                ..
            } => {
                if let Err(error) = self.wait_for_authoritative_revision(outcome_revision).await {
                    return Err(Box::new(outcome_unknown(
                        OutcomeUnknownCause::NotYetAuthoritative,
                        format!(
                            "command execution reference '{reference}' is durable but its result \
                             is not yet authoritative on every live node: {error}"
                        ),
                    )));
                }
                Ok(command_result(*result))
            }
        }
    }

    pub(in crate::application) async fn complete_persistent_command_request(
        &self,
        execution: CommandExecution,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let mut result = match &execution.state {
            CommandExecutionState::Applying { .. } => {
                let result =
                    Box::pin(self.execute_persistent_command(&execution, subscriptions)).await;
                let result = self
                    .command_with_transaction_status(result, subscriptions)
                    .await;
                if result.is_not_leader() {
                    // The command was admitted before leadership moved, so it is not refused: the
                    // next leader resumes it, and its outcome is not known yet.
                    CommandResult {
                        diagnostics: result.diagnostics,
                        transaction: result.transaction,
                        ..CommandResult::new(
                            CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::LeadershipLost),
                            format!(
                                "leadership moved while command execution reference '{}' was \
                                 applying; retry it with the same reference to learn its outcome",
                                execution.reference
                            ),
                        )
                    }
                } else {
                    match self
                        .finish_persistent_command(
                            execution.reference.clone(),
                            execution
                                .owner()
                                .verified("an applying execution retains its owner")
                                .clone(),
                            execution
                                .request_digest()
                                .verified("an applying execution retains its request digest"),
                            &result,
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(result) => *result,
                    }
                }
            }
            CommandExecutionState::Finished { .. } | CommandExecutionState::Expired => {
                match self
                    .result_from_finished_execution(&execution.reference, execution.clone())
                    .await
                {
                    Ok(result) => result,
                    Err(result) => *result,
                }
            }
        };
        self.restore_transaction_binding_for_execution(&execution, &mut result, subscriptions);
        result
    }

    fn restore_transaction_binding_for_execution(
        &self,
        execution: &CommandExecution,
        result: &mut CommandResult,
        subscriptions: &mut SessionSubscriptions,
    ) {
        let Some(target) = execution.transaction_target() else {
            return;
        };
        let Some(transaction) = result.transaction.as_ref() else {
            return;
        };
        let target = target.id();
        if transaction.transaction_id() != target {
            let message = format!(
                "command execution reference '{}' recovered transaction '{}' instead of its \
                 durable target '{target}'",
                execution.reference,
                transaction.transaction_id()
            );
            result.fail(message);
            return;
        }
        if transaction.lifecycle().is_active() {
            self.release_session_transaction_binding(subscriptions);
            self.inner
                .transaction_bindings
                .insert(target.to_string(), subscriptions.session_id.clone());
            subscriptions.bind_transaction(target.to_string());
        } else if subscriptions.transaction_id() == Some(target) {
            self.release_session_transaction_binding(subscriptions);
        }
    }
}

/// How admitting a persistent command's execution reference turned out.
pub(in crate::application) enum CommandAdmission {
    /// This request admitted the reference, and its effects are this request's to run.
    Admitted(CommandExecution),
    /// An earlier request with the same identity admitted it; this one recovers its outcome.
    Existing(CommandExecution),
}

/// An admitted command whose outcome is not known yet, for `cause`.
fn outcome_unknown(cause: OutcomeUnknownCause, message: String) -> CommandResult {
    CommandResult {
        diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
        ..CommandResult::new(CommandDisposition::OutcomeUnknown(cause), message)
    }
}

/// The record the execution ledger keeps of a finished command.
///
/// Only a completion, a failure and a refused commit are ever recorded: a command that was
/// redirected or whose outcome is unknown is not finished. Such a disposition reaching this point
/// is kept as a failure.
fn durable_command_result(result: &CommandResult) -> CommandExecutionResult {
    let disposition = match &result.disposition {
        CommandDisposition::Completed { already_existed } => {
            CommandExecutionDisposition::Completed {
                already_existed: *already_existed,
            }
        }
        CommandDisposition::PreviewStale { expected, current } => {
            CommandExecutionDisposition::PreviewStale(CommandExecutionPreviewStale {
                expected: expected.clone(),
                current: current.clone(),
            })
        }
        CommandDisposition::Failed
        | CommandDisposition::NotLeader(_)
        | CommandDisposition::TransactionDetached { .. }
        | CommandDisposition::TransactionTakenOver { .. }
        | CommandDisposition::OutcomeUnknown(_)
        | CommandDisposition::ExecutionReferenceConflict(_)
        | CommandDisposition::ExecutionReferenceExpired => CommandExecutionDisposition::Failed,
    };
    CommandExecutionResult {
        disposition,
        message: result.message.clone(),
        diagnostics: result
            .diagnostics
            .iter()
            .map(CommandExecutionDiagnostic::from)
            .collect(),
        statements: result
            .statements
            .iter()
            .map(durable_statement_result)
            .collect(),
        transaction: result.transaction.as_ref().map(durable_transaction_status),
        transaction_admission: result.transaction_admission.clone(),
    }
}

fn durable_transaction_status(status: &TransactionStatus) -> CommandExecutionTransactionStatus {
    CommandExecutionTransactionStatus {
        transaction_id: status.transaction_id().to_string(),
        domain: status.domain().clone(),
        lifecycle: status.lifecycle().clone(),
        accepted_operations: status.accepted_operations(),
        applied_operations: status.applied_operations(),
    }
}

fn command_result(result: CommandExecutionResult) -> CommandResult {
    let disposition = match result.disposition {
        CommandExecutionDisposition::Completed { already_existed } => {
            CommandDisposition::Completed { already_existed }
        }
        CommandExecutionDisposition::Failed => CommandDisposition::Failed,
        // A refused commit is recorded with both previews. Restoring the typed disposition is what
        // lets a recovered outcome tell a client to read the transaction again rather than only
        // that its commit failed.
        CommandExecutionDisposition::PreviewStale(stale) => CommandDisposition::PreviewStale {
            expected: stale.expected,
            current: stale.current,
        },
    };
    let transaction = match result.transaction {
        Some(status) => Some(
            TransactionStatus::new(
                status.transaction_id,
                status.domain,
                status.lifecycle,
                status.accepted_operations,
                status.applied_operations,
            )
            .assured(
                "a recorded status was written from a status that held its applied operations to \
                 its accepted ones",
            ),
        ),
        None => None,
    };
    CommandResult {
        diagnostics: result
            .diagnostics
            .iter()
            .map(CommandDiagnostic::from)
            .collect(),
        statements: result
            .statements
            .into_iter()
            .map(statement_result)
            .collect(),
        transaction,
        transaction_admission: result.transaction_admission,
        ..CommandResult::new(disposition, result.message)
    }
}

fn durable_statement_result(result: &CommandResult) -> CommandExecutionStatementResult {
    let disposition = match result.disposition {
        CommandDisposition::Completed { already_existed } => {
            CommandExecutionStatementDisposition::Completed { already_existed }
        }
        _ => CommandExecutionStatementDisposition::Failed,
    };
    CommandExecutionStatementResult {
        disposition,
        message: result.message.clone(),
        diagnostics: result
            .diagnostics
            .iter()
            .map(CommandExecutionDiagnostic::from)
            .collect(),
    }
}

fn statement_result(result: CommandExecutionStatementResult) -> CommandResult {
    let disposition = match result.disposition {
        CommandExecutionStatementDisposition::Completed { already_existed } => {
            CommandDisposition::Completed { already_existed }
        }
        CommandExecutionStatementDisposition::Failed => CommandDisposition::Failed,
    };
    CommandResult {
        diagnostics: result
            .diagnostics
            .iter()
            .map(CommandDiagnostic::from)
            .collect(),
        ..CommandResult::new(disposition, result.message)
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        CreateStatement, CreateUser, ImpactPlanningBasis, TransactionLifecycle,
        TransactionOperationAdmission, TransactionOperationNumber, TransactionPosition,
        TransactionPreviewIdentity,
    };

    use super::*;

    fn domain() -> DomainName {
        DomainName::parse("default").assured("the test domain is an identifier-shaped literal")
    }

    fn preview(position: usize, basis: u8) -> TransactionPreviewIdentity {
        TransactionPreviewIdentity {
            transaction_id: "transaction-1".to_string(),
            position: TransactionPosition::new(position),
            planning_basis: ImpactPlanningBasis::new([basis; 32]),
        }
    }

    fn persistent_user_operation(password: &str) -> SessionCommandOperation {
        let statement = Statement::CreateUser(CreateStatement::new(
            CreateUser {
                name: UserName::parse("operator")
                    .assured("the test user name is an identifier-shaped literal"),
                password: password.to_string(),
            },
            false,
        ));
        SessionCommandOperation::Execute(PendingSessionCommand {
            request_reference: CommandExecutionReference::parse("request.0")
                .assured("the test command reference is an identifier-shaped literal"),
            expected_transaction_position: None,
            source: "CREATE USER operator WITH PASSWORD 'secret'".to_string(),
            statement: ClientStatement::Server(statement),
            domain: None,
        })
    }

    #[tokio::test]
    async fn command_execution_owner_entry_survives_a_waiter_and_leaves_with_its_last_guard() {
        let owners = CommandExecutionOwners::default();
        let reference = CommandExecutionReference::parse("request.owners")
            .assured("the test command reference is an identifier-shaped literal");
        let first = owners.lock(reference.clone()).await;
        let late_lock = owners.lock_for(&reference);
        let waiter_lock = late_lock.clone();
        let waiter = tokio::spawn(async move {
            let guard = waiter_lock.mutex.clone().lock_owned().await;
            (waiter_lock, guard)
        });

        drop(first);
        let (lock, guard) = waiter
            .await
            .assured("the command execution lock waiter does not panic");
        let second = CommandExecutionOwnerGuard {
            _guard: guard,
            _lock: lock,
        };
        drop(late_lock);
        assert_eq!(owners.len(), 1);

        drop(second);
        assert_eq!(owners.len(), 0);
    }

    #[test]
    fn persistent_request_digest_omits_user_password_and_rejects_multiple_effects() {
        let first = PersistentCommandRequest::from_operations(
            &[persistent_user_operation("first-secret")],
            Some(&domain()),
        )
        .assured("the test statement has serializable semantics")
        .assured("the test includes one persistent statement");
        let retried = PersistentCommandRequest::from_operations(
            &[persistent_user_operation("changed-secret")],
            Some(&domain()),
        )
        .assured("the test statement has serializable semantics")
        .assured("the test includes one persistent statement");

        assert_eq!(first.digest, retried.digest);

        let duplicate = PersistentCommandRequest::from_operations(
            &[
                persistent_user_operation("first-secret"),
                persistent_user_operation("first-secret"),
            ],
            Some(&domain()),
        );
        let error = match duplicate {
            Ok(_) => panic!("two persistent effects must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(
            error.current_context(),
            PersistentCommandRequestError::MultiplePersistentStatements
        ));
    }

    #[test]
    fn durable_command_results_preserve_statements_diagnostics_and_transaction_status() {
        let admission = TransactionOperationAdmission {
            operation: TransactionOperationNumber::from_index(1)
                .assured("the second operation is addressable"),
            preview: preview(2, 7),
        };
        let successful_statement = CommandResult {
            diagnostics: vec![CommandDiagnostic {
                message: "success detail".to_string(),
                span: Some(2..7),
            }],
            ..CommandResult::new(
                CommandDisposition::Completed {
                    already_existed: true,
                },
                "created schema".to_string(),
            )
        };
        let failed_statement = CommandResult {
            diagnostics: vec![CommandDiagnostic::unlocated("failure detail".to_string())],
            ..CommandResult::new(CommandDisposition::Failed, "invalid relay".to_string())
        };
        let transaction = TransactionStatus::new(
            "command.request".to_string(),
            domain(),
            TransactionLifecycle::Failed {
                failing_operation: TransactionOperationNumber::from_index(0)
                    .assured("the first operation is addressable"),
                error: "retained detail".to_string(),
            },
            TransactionPosition::new(3),
            2,
        )
        .assured("two applied operations fit three accepted ones");
        let result = CommandResult {
            diagnostics: vec![CommandDiagnostic {
                message: "commit detail".to_string(),
                span: Some(0..9),
            }],
            statements: vec![successful_statement, failed_statement],
            transaction: Some(transaction),
            transaction_admission: Some(admission),
            ..CommandResult::new(
                CommandDisposition::Completed {
                    already_existed: false,
                },
                "committed".to_string(),
            )
        };

        let restored = command_result(durable_command_result(&result));

        assert_eq!(restored, result);
    }

    #[test]
    fn durable_command_results_keep_only_finished_dispositions() {
        for disposition in [
            CommandDisposition::NotLeader(crate::application::command_result::LeaderRedirect {
                leader: None,
            }),
            CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::LeadershipLost),
            CommandDisposition::ExecutionReferenceExpired,
        ] {
            let result = CommandResult::new(disposition, "not finished".to_string());
            let durable = durable_command_result(&result);
            assert_eq!(durable.disposition, CommandExecutionDisposition::Failed);
            let restored = command_result(durable);
            assert_eq!(restored.disposition, CommandDisposition::Failed);
        }
    }

    #[test]
    fn a_recovered_stale_commit_still_reports_both_previews() {
        let result = CommandResult::new(
            CommandDisposition::PreviewStale {
                expected: preview(1, 3),
                current: preview(1, 9),
            },
            "transaction 'transaction-1' was planned from different inputs".to_string(),
        );

        let restored = command_result(durable_command_result(&result));

        assert_eq!(restored, result);
    }
}
