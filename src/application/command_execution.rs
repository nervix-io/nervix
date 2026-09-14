//! Admission and retained outcomes for persistent session commands.
//!
//! Layer: control plane.
//! - **Owns.** Binding a command reference to one semantic request and publishing its final result.
//! - **Depends on.** Consensus command records and the authoritative visibility barrier.
//! - **Must not know.** Parser recovery, transport reconnect policy, or runtime implementation.

use blake3::Hasher;
use error_stack::Report;
use nervix_consensus::{
    CommandExecution, CommandExecutionChildResult, CommandExecutionDiagnostic,
    CommandExecutionEffect, CommandExecutionResult, CommandExecutionResultKind,
    CommandExecutionState, CommandExecutionTransactionStatus,
};
use nervix_models::{
    CommandExecutionReference, DomainName, DomainStartPoint, DomainState, DomainStatus, Statement,
    UserName,
};
use nervix_nspl::client_statement::ClientStatement;
use thiserror::Error;
use tokio::sync::mpsc;
use tonic::Status;

use super::{
    authentication::{user_credentials, verify_password_hash},
    domain_clock::current_timestamp,
    model_mutation::{command_error, is_persistent_statement, parse_request_domain},
    session_service::SessionServiceImpl,
    subscription::{PendingSessionCommand, SessionCommandOperation, SessionSubscriptions},
    transaction::is_queueable_transaction_statement,
};
use crate::proto::{CommandResult, CommandResultKind, SessionResponse};

pub(in crate::application) struct PersistentCommandRequest {
    pub(in crate::application) domain: Option<DomainName>,
    pub(in crate::application) digest: [u8; 32],
    source: String,
    statement: Statement,
}

#[derive(Debug, Error)]
pub(in crate::application) enum PersistentCommandRequestError {
    #[error("one ordinary command execution reference cannot own multiple persistent statements")]
    MultiplePersistentStatements,
    #[error("failed to identify command semantics: {message}")]
    Encoding { message: String },
    #[error("command semantics exceed the supported size")]
    SemanticsTooLarge,
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
            digest: *hasher.finalize().as_bytes(),
            source,
            statement,
        }))
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn resume_persistent_commands(&self) {
        let executions = self.inner.consensus.current_command_executions().await;
        for execution in executions.into_values() {
            tokio::task::consume_budget().await;
            if !matches!(execution.state, CommandExecutionState::Applying) {
                continue;
            }
            let lock = self
                .inner
                .command_executions
                .entry(execution.reference.clone())
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
                .clone();
            let Ok(execution_guard) = lock.try_lock_owned() else {
                continue;
            };
            let service = self.clone();
            self.inner.service_tasks.spawn(async move {
                let _execution_guard = execution_guard;
                let (response_tx, response_rx) = mpsc::channel(1);
                let mut subscriptions = SessionSubscriptions::for_user(execution.owner.clone());
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
                        execution.owner.clone(),
                        execution.request_digest,
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
            if existing.owner != owner
                || existing.domain != request.domain
                || existing.request_digest != request.digest
            {
                return Err(Box::new(command_error(format!(
                    "command execution reference '{reference}' is bound to a different owner, \
                     domain, or request"
                ))));
            }
            if let (
                CommandExecutionEffect::CreateUser { password_hash, .. },
                Statement::CreateUser(create),
            ) = (&existing.effect, &request.statement)
                && !verify_password_hash(password_hash.clone(), create.body.password.clone()).await
            {
                return Err(Box::new(command_error(format!(
                    "command execution reference '{reference}' is bound to different user \
                     credentials"
                ))));
            }
            return Ok(existing);
        }

        let effect = match &request.statement {
            Statement::CreateDomain(create) => {
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
            Statement::CreateUser(create) => {
                let user = user_credentials(create.body.name.clone(), create.body.password.clone())
                    .await
                    .map_err(|error| Box::new(command_error(error)))?;
                CommandExecutionEffect::CreateUser {
                    if_not_exists: create.if_not_exists,
                    name: user.name,
                    password_hash: user.password_hash,
                }
            }
            Statement::DropNode(drop) => {
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
            statement if is_queueable_transaction_statement(statement) => {
                request.domain.as_ref().ok_or_else(|| {
                    Box::new(command_error("no active domain selected".to_string()))
                })?;
                CommandExecutionEffect::Transaction {
                    transaction_id: format!("command.{}", reference.as_str()),
                    source: request.source.clone(),
                    statement: Box::new(statement.clone()),
                }
            }
            statement => CommandExecutionEffect::Statement {
                source: request.source.clone(),
                statement: Box::new(statement.clone()),
            },
        };
        let execution = CommandExecution::applying(
            reference,
            owner,
            request.domain.clone(),
            request.digest,
            current_timestamp(),
            effect,
        );
        self.inner
            .consensus
            .admit_command_execution(execution)
            .await
            .map_err(|error| Box::new(command_error(error.to_string())))
    }

    pub(in crate::application) async fn execute_persistent_command(
        &self,
        execution: &CommandExecution,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        match execution.effect.clone() {
            CommandExecutionEffect::CreateDomain {
                if_not_exists,
                existed_at_admission,
                state,
            } => {
                self.apply_persistent_domain_creation(if_not_exists, existed_at_admission, *state)
                    .await
            }
            CommandExecutionEffect::Transaction {
                transaction_id,
                source,
                statement,
            } => {
                let Some(domain) = execution.domain.clone() else {
                    return command_error(
                        "durable configuration application lost its domain".to_string(),
                    );
                };
                Box::pin(self.execute_standalone_transaction(
                    transaction_id,
                    execution.reference.clone(),
                    execution.owner.clone(),
                    domain,
                    source,
                    *statement,
                ))
                .await
            }
            CommandExecutionEffect::Statement { source, statement } => {
                let domain = match &execution.domain {
                    Some(domain) => domain.to_string(),
                    None => String::new(),
                };
                let command = PendingSessionCommand {
                    request_reference: execution.reference.clone(),
                    expected_transaction_position: None,
                    source,
                    statement: ClientStatement::Server(*statement),
                    domain,
                };
                Box::pin(self.process_session_command_operations(
                    vec![SessionCommandOperation::Execute(command)],
                    tx,
                    subscriptions,
                ))
                .await
            }
            CommandExecutionEffect::CreateUser {
                if_not_exists,
                name,
                password_hash,
            } => {
                self.apply_persistent_user_creation(
                    if_not_exists,
                    nervix_consensus::UserCredentials {
                        name,
                        password_hash,
                    },
                )
                .await
            }
            CommandExecutionEffect::DropNode {
                identity,
                member_at_admission,
            } => self.drop_admitted_node(identity, member_at_admission).await,
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
        let execution = self
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
            .map_err(|error| Box::new(command_error(error.to_string())))?;
        self.result_from_finished_execution(&reference, execution)
            .await
    }

    pub(in crate::application) async fn result_from_finished_execution(
        &self,
        reference: &CommandExecutionReference,
        execution: CommandExecution,
    ) -> Result<CommandResult, Box<CommandResult>> {
        match execution.state {
            CommandExecutionState::Applying => Err(Box::new(command_error(format!(
                "command execution reference '{reference}' is still applying"
            )))),
            CommandExecutionState::Expired { .. } => Err(Box::new(command_error(format!(
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
        ..Default::default()
    }
}
