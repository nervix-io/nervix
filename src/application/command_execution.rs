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
    CommandExecution, CommandExecutionAdmissionPolicy, CommandExecutionChildResult,
    CommandExecutionDiagnostic, CommandExecutionEffect, CommandExecutionResult,
    CommandExecutionResultKind, CommandExecutionState, CommandExecutionTransactionOperation,
    CommandExecutionTransactionRequest, CommandExecutionTransactionStatus,
    CommandExecutionTransactionTarget,
};
use nervix_execution::sync::DashMap;
use nervix_models::{
    CommandExecutionReference, DomainName, DomainStartPoint, DomainState, DomainStatus,
    ImpactPlanningBasis, Statement, Timestamp, TransactionOperationAdmission,
    TransactionOperationNumber, TransactionPosition, TransactionPreviewIdentity, UserName,
};
use nervix_nspl::client_statement::ClientStatement;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, mpsc};
use tonic::Status;
use tracing::warn;

use super::{
    authentication::{user_credentials, verify_password_hash},
    domain_clock::current_timestamp,
    model_mutation::{command_error, is_persistent_statement, parse_request_domain},
    session_service::SessionServiceImpl,
    subscription::{PendingSessionCommand, SessionCommandOperation, SessionSubscriptions},
    transaction::is_queueable_transaction_statement,
};
use crate::proto::{CommandResult, CommandResultKind, SessionResponse};

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

    fn admission_at(self, now: Timestamp) -> CommandExecutionAdmissionPolicy {
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

#[derive(Clone)]
pub(in crate::application) struct CommandExecutionOwners {
    policy: CommandExecutionPolicy,
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
            policy: CommandExecutionPolicy::default(),
            inner: StdArc::new(CommandExecutionOwnersInner {
                locks: DashMap::with_hasher(RandomState::new()),
            }),
        }
    }
}

impl From<CommandExecutionPolicy> for CommandExecutionOwners {
    fn from(policy: CommandExecutionPolicy) -> Self {
        Self::new(policy)
    }
}

impl CommandExecutionOwners {
    pub(in crate::application) fn new(policy: CommandExecutionPolicy) -> Self {
        Self {
            policy,
            inner: StdArc::new(CommandExecutionOwnersInner {
                locks: DashMap::with_hasher(RandomState::new()),
            }),
        }
    }

    pub(in crate::application) fn retry_validity(&self) -> Duration {
        self.policy.retry_validity()
    }

    fn admission_at(&self, now: Timestamp) -> CommandExecutionAdmissionPolicy {
        self.policy.admission_at(now)
    }

    pub(in crate::application) async fn lock(
        &self,
        reference: CommandExecutionReference,
    ) -> CommandExecutionOwnerGuard {
        let lock = self.lock_for(&reference);
        let guard = lock.mutex.clone().lock_owned().await;
        CommandExecutionOwnerGuard {
            _lock: lock,
            guard: Some(guard),
        }
    }

    pub(in crate::application) fn try_lock(
        &self,
        reference: CommandExecutionReference,
    ) -> Option<CommandExecutionOwnerGuard> {
        let lock = self.lock_for(&reference);
        let guard = lock.mutex.clone().try_lock_owned().ok()?;
        Some(CommandExecutionOwnerGuard {
            _lock: lock,
            guard: Some(guard),
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

pub(in crate::application) struct CommandExecutionOwnerGuard {
    _lock: StdArc<CommandExecutionLock>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for CommandExecutionOwnerGuard {
    fn drop(&mut self) {
        drop(self.guard.take());
    }
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
        request_domain: &str,
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

        let domain = parse_request_domain(request_domain).ok();
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
                SessionCommandOperation::Commit => {
                    durable_operations.push(CommandExecutionTransactionOperation::Commit);
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
                let (response_tx, response_rx) = mpsc::channel(1);
                let mut subscriptions = SessionSubscriptions::for_user(owner.clone());
                let result = service
                    .execute_persistent_command(&execution, &response_tx, &mut subscriptions)
                    .await;
                drop(response_rx);
                if result.kind == i32::from(CommandResultKind::NotLeader) {
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
            warn!(error = %error, "failed to expire retained command results");
        }
    }

    pub(in crate::application) async fn admit_persistent_command(
        &self,
        reference: CommandExecutionReference,
        owner: UserName,
        request: &PersistentCommandRequest,
    ) -> Result<CommandExecution, Box<CommandResult>> {
        if let Some(existing) = self
            .inner
            .consensus
            .current_command_execution(&reference)
            .await
        {
            if existing.is_expired() {
                return Err(Box::new(command_error(format!(
                    "command execution reference '{reference}' has expired"
                ))));
            }
            if let Some(conflict) = existing.request_conflict(
                &owner,
                request.domain.as_ref(),
                request.expected_transaction_position,
                request.digest,
            ) {
                return Err(Box::new(command_error(format!(
                    "command execution reference '{reference}' conflicts by {conflict}"
                ))));
            }
            if !transaction_targets_match(&existing, request) {
                return Err(Box::new(command_error(format!(
                    "command execution reference '{reference}' conflicts by position"
                ))));
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
                return Err(Box::new(command_error(format!(
                    "command execution reference '{reference}' is bound to different user \
                     credentials"
                ))));
            }
            return Ok(existing);
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
                    .map_err(|error| Box::new(command_error(error)))?;
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
        let policy = self.inner.command_executions.admission_at(admitted_at);
        let execution = CommandExecution::applying_at_position(
            reference,
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
            Ok(execution) => Ok(execution),
            Err(error) => {
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
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
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
                    let domain = match execution.domain() {
                        Some(domain) => domain.to_string(),
                        None => String::new(),
                    };
                    let command = PendingSessionCommand {
                        request_reference: execution.reference.clone(),
                        expected_transaction_position: None,
                        source,
                        statement: ClientStatement::Server(statement),
                        domain,
                    };
                    return Box::pin(self.process_session_command_operations(
                        vec![SessionCommandOperation::Execute(command)],
                        tx,
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
            CommandExecutionState::Applying { .. } => Err(Box::new(command_error(format!(
                "command execution reference '{reference}' is still applying"
            )))),
            CommandExecutionState::Expired => Err(Box::new(command_error(format!(
                "command execution reference '{reference}' has expired"
            )))),
            CommandExecutionState::Finished {
                outcome_revision,
                result,
                ..
            } => {
                self.wait_for_authoritative_revision(outcome_revision)
                    .await
                    .map_err(|error| {
                        Box::new(command_error(format!(
                            "command execution reference '{reference}' is durable but its result \
                             is not yet authoritative on every live node: {error}"
                        )))
                    })?;
                Ok(command_result(*result))
            }
        }
    }

    pub(in crate::application) async fn complete_persistent_command_request(
        &self,
        execution: CommandExecution,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let mut result = match &execution.state {
            CommandExecutionState::Applying { .. } => {
                let result =
                    Box::pin(self.execute_persistent_command(&execution, tx, subscriptions)).await;
                let result = self
                    .command_with_transaction_status(result, subscriptions)
                    .await;
                if result.kind == i32::from(CommandResultKind::NotLeader) {
                    result
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
        if transaction.id != target {
            result.success = false;
            result.kind = i32::from(CommandResultKind::Error);
            result.message = format!(
                "command execution reference '{}' recovered transaction '{}' instead of its \
                 durable target '{target}'",
                execution.reference, transaction.id
            );
            return;
        }
        let state = crate::proto::TransactionState::try_from(transaction.state);
        if matches!(
            state,
            Ok(crate::proto::TransactionState::Open | crate::proto::TransactionState::Committing)
        ) {
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

fn durable_command_result(result: &CommandResult) -> CommandExecutionResult {
    let kind = if result.success && result.kind == i32::from(CommandResultKind::Ok) {
        CommandExecutionResultKind::Ok
    } else {
        CommandExecutionResultKind::Error
    };
    CommandExecutionResult {
        success: result.success,
        kind,
        message: result.message.clone(),
        diagnostics: result
            .diagnostics
            .iter()
            .map(|diagnostic| CommandExecutionDiagnostic {
                message: diagnostic.message.clone(),
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        already_existed: result.already_existed,
        results: result.results.iter().map(durable_child_result).collect(),
        transaction: result.transaction.as_ref().map(|transaction| {
            CommandExecutionTransactionStatus {
                id: transaction.id.clone(),
                domain: transaction.domain.clone(),
                state: transaction.state,
                pending_count: transaction.pending_count,
                completed_count: transaction.completed_count,
                total_count: transaction.total_count,
                error: transaction.error.clone(),
                failing_step: transaction.failing_step,
            }
        }),
        transaction_admission: result
            .transaction_admission
            .as_ref()
            .map(durable_transaction_admission),
    }
}

fn command_result(result: CommandExecutionResult) -> CommandResult {
    CommandResult {
        success: result.success,
        message: result.message,
        diagnostics: result
            .diagnostics
            .into_iter()
            .map(|diagnostic| crate::proto::Diagnostic {
                message: diagnostic.message,
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        kind: match result.kind {
            CommandExecutionResultKind::Ok => i32::from(CommandResultKind::Ok),
            CommandExecutionResultKind::Error => i32::from(CommandResultKind::Error),
        },
        already_existed: result.already_existed,
        results: result.results.into_iter().map(child_result).collect(),
        transaction: result
            .transaction
            .map(|transaction| crate::proto::TransactionStatus {
                id: transaction.id,
                domain: transaction.domain,
                state: transaction.state,
                pending_count: transaction.pending_count,
                completed_count: transaction.completed_count,
                total_count: transaction.total_count,
                error: transaction.error,
                failing_step: transaction.failing_step,
            }),
        transaction_admission: result.transaction_admission.map(api_transaction_admission),
        ..Default::default()
    }
}

fn durable_child_result(result: &CommandResult) -> CommandExecutionChildResult {
    let kind = if result.success && result.kind == i32::from(CommandResultKind::Ok) {
        CommandExecutionResultKind::Ok
    } else {
        CommandExecutionResultKind::Error
    };
    CommandExecutionChildResult {
        success: result.success,
        kind,
        message: result.message.clone(),
        diagnostics: result
            .diagnostics
            .iter()
            .map(|diagnostic| CommandExecutionDiagnostic {
                message: diagnostic.message.clone(),
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        already_existed: result.already_existed,
        transaction_admission: result
            .transaction_admission
            .as_ref()
            .map(durable_transaction_admission),
    }
}

fn child_result(result: CommandExecutionChildResult) -> CommandResult {
    CommandResult {
        success: result.success,
        message: result.message,
        diagnostics: result
            .diagnostics
            .into_iter()
            .map(|diagnostic| crate::proto::Diagnostic {
                message: diagnostic.message,
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        kind: match result.kind {
            CommandExecutionResultKind::Ok => i32::from(CommandResultKind::Ok),
            CommandExecutionResultKind::Error => i32::from(CommandResultKind::Error),
        },
        already_existed: result.already_existed,
        transaction_admission: result.transaction_admission.map(api_transaction_admission),
        ..Default::default()
    }
}

fn durable_transaction_admission(
    admission: &crate::proto::TransactionOperationAdmission,
) -> TransactionOperationAdmission {
    let operation = usize::try_from(admission.operation)
        .assured("server-produced transaction operation numbers fit the target pointer width");
    let operation_index = operation
        .checked_sub(1)
        .assured("server-produced transaction operation numbers are one-based");
    let operation = TransactionOperationNumber::from_index(operation_index)
        .assured("server-produced transaction operation numbers are addressable");
    let preview = admission
        .preview
        .as_ref()
        .assured("server-produced transaction admissions always carry a preview");
    let position = usize::try_from(preview.position)
        .assured("server-produced transaction positions fit the target pointer width");
    let planning_basis = <[u8; 32]>::try_from(preview.planning_basis.as_ref())
        .assured("server-produced transaction planning bases contain 32 bytes");
    TransactionOperationAdmission {
        operation,
        preview: TransactionPreviewIdentity {
            transaction_id: preview.transaction_id.clone(),
            position: TransactionPosition::new(position),
            planning_basis: ImpactPlanningBasis::new(planning_basis),
        },
    }
}

fn api_transaction_admission(
    admission: TransactionOperationAdmission,
) -> crate::proto::TransactionOperationAdmission {
    crate::proto::TransactionOperationAdmission {
        operation: u64::try_from(admission.operation.get())
            .assured("supported targets have a pointer width no larger than u64"),
        preview: Some(crate::proto::TransactionPreviewIdentity {
            transaction_id: admission.preview.transaction_id,
            position: u64::try_from(admission.preview.position.accepted_operations())
                .assured("supported targets have a pointer width no larger than u64"),
            planning_basis: admission
                .preview
                .planning_basis
                .fingerprint()
                .to_vec()
                .into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{CreateStatement, CreateUser};

    use super::*;
    use crate::proto::{Diagnostic, TransactionState, TransactionStatus};

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
            domain: String::new(),
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
            _lock: lock,
            guard: Some(guard),
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
            "default",
        )
        .assured("the test statement has serializable semantics")
        .assured("the test includes one persistent statement");
        let retried = PersistentCommandRequest::from_operations(
            &[persistent_user_operation("changed-secret")],
            "default",
        )
        .assured("the test statement has serializable semantics")
        .assured("the test includes one persistent statement");

        assert_eq!(first.digest, retried.digest);

        let duplicate = PersistentCommandRequest::from_operations(
            &[
                persistent_user_operation("first-secret"),
                persistent_user_operation("first-secret"),
            ],
            "default",
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
    fn durable_command_results_preserve_nested_diagnostics_and_transaction_status() {
        let successful_child = CommandResult {
            success: true,
            message: "created schema".to_string(),
            diagnostics: vec![Diagnostic {
                message: "success detail".to_string(),
                span_start: 2,
                span_end: 7,
            }],
            kind: i32::from(CommandResultKind::Ok),
            already_existed: true,
            transaction_admission: Some(crate::proto::TransactionOperationAdmission {
                operation: 1,
                preview: Some(crate::proto::TransactionPreviewIdentity {
                    transaction_id: "transaction-1".to_string(),
                    position: 1,
                    planning_basis: vec![3; 32].into(),
                }),
            }),
            ..Default::default()
        };
        let failed_child = CommandResult {
            success: false,
            message: "invalid relay".to_string(),
            diagnostics: vec![Diagnostic {
                message: "failure detail".to_string(),
                span_start: 11,
                span_end: 19,
            }],
            kind: i32::from(CommandResultKind::Error),
            ..Default::default()
        };
        let result = CommandResult {
            success: true,
            message: "committed".to_string(),
            diagnostics: vec![Diagnostic {
                message: "commit detail".to_string(),
                span_start: 0,
                span_end: 9,
            }],
            kind: i32::from(CommandResultKind::Ok),
            results: vec![successful_child, failed_child],
            transaction: Some(TransactionStatus {
                id: "command.request".to_string(),
                domain: "default".to_string(),
                state: i32::from(TransactionState::Committed),
                pending_count: 1,
                completed_count: 2,
                total_count: 3,
                error: "retained detail".to_string(),
                failing_step: Some(1),
            }),
            transaction_admission: Some(crate::proto::TransactionOperationAdmission {
                operation: 2,
                preview: Some(crate::proto::TransactionPreviewIdentity {
                    transaction_id: "transaction-1".to_string(),
                    position: 2,
                    planning_basis: vec![7; 32].into(),
                }),
            }),
            ..Default::default()
        };

        let restored = command_result(durable_command_result(&result));

        assert_eq!(restored, result);
    }

    #[test]
    fn durable_command_results_normalize_non_success_kinds_to_error() {
        let result = CommandResult {
            success: false,
            message: "redirect".to_string(),
            kind: i32::from(CommandResultKind::NotLeader),
            ..Default::default()
        };

        let durable = durable_command_result(&result);
        assert_eq!(durable.kind, CommandExecutionResultKind::Error);
        let restored = command_result(durable);
        assert_eq!(restored.kind, i32::from(CommandResultKind::Error));
    }
}
