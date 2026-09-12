//! A session's staged configuration, from the first queued statement to the replicated commit.
//!
//! Layer: control plane.
//!
//! - **Owns.** The transaction lifecycle, the binding a session holds, statement preflight, the
//!   replicated commit and the quiescence it recovers from.
//! - **Depends on.** Consensus for the replicated transaction and the registry for mutation plans.
//! - **Must not know.** How the models a commit applies are executed.

use std::collections::BTreeSet;

use ahash::RandomState;
use arch_into::ArchInto;
use dashmap::DashMap;
use error_stack::{Report, ResultExt};
use meticulous::OptionExt as _;
use nervix_consensus::{
    ConsensusError, ConsensusTransactionError, ReplicatedTransaction, TransactionCommandResult,
    TransactionCommitAdvance, TransactionDiagnostic, TransactionOutcome, TransactionQueueLimits,
    TransactionState, TransactionStatement, TransactionStepEffect, TransactionStepResult,
};
use nervix_models::{DomainName, DomainPace, DomainStatus, QuiesceLevel, ResourceName, Statement};
use nervix_nspl::client_statement::ClientStatement;
use parking_lot::Mutex as ParkingMutex;
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{Duration, interval},
};
use tonic::Status;
use tracing::{info, warn};
use triomphe::Arc;

use super::{
    domain_clock::{current_timestamp, subtract_timestamp_duration},
    domain_lifecycle::DomainAlterError,
    model_mutation::{
        RequestDomainError, append_command_output, command_error, command_ok,
        command_ok_already_existed, parse_request_domain, quiesce_level_message,
    },
    model_validation::validate_domain_config,
    ownership_handoff::{mark_complete_ownership_transitions, planned_ownership_moves},
    scheduling::PreparedDomainSchedule,
    session_service::SessionServiceImpl,
    subscription::{PendingSessionCommand, SessionSubscriptions},
};
use crate::{
    proto,
    proto::{
        CommandResult, CommandResultKind, Diagnostic, SessionResponse,
        TransactionState as ApiTransactionState, TransactionStatus as ApiTransactionStatus,
    },
    registry::RegistryMutation,
};
pub(in crate::application) const DEFAULT_TRANSACTION_IDLE_TIMEOUT: Duration =
    Duration::from_secs(15 * 60);

pub(in crate::application) const DEFAULT_TRANSACTION_TOMBSTONE_RETENTION: Duration =
    Duration::from_secs(15 * 60);

pub(in crate::application) const DEFAULT_TRANSACTION_MAX_STATEMENTS: usize = 256;

pub(in crate::application) const DEFAULT_TRANSACTION_MAX_SOURCE_BYTES: u64 = 1024 * 1024;

pub(in crate::application) const DEFAULT_TRANSACTION_MAX_OPEN: usize = 1024;

struct TransactionExecutionLease {
    executions: Arc<DashMap<String, (), RandomState>>,
    id: String,
}

#[derive(Debug, Error)]
pub(in crate::application) enum TransactionCommitError {
    #[error(transparent)]
    Proposal(#[from] ConsensusTransactionError),
    #[error("transaction '{id}' is unknown")]
    UnknownTransaction { id: String },
    #[error("transaction '{id}' is still open")]
    TransactionOpen { id: String },
    #[error("transaction '{id}' model step completed without recording progress")]
    MissingProgress { id: String },
    #[error("transaction '{id}' has invalid recorded commit progress")]
    InvalidProgress { id: String },
    #[error("failed to synchronize registry before resuming transaction '{id}'")]
    SynchronizeRegistry { id: String },
    #[error("failed to recover transaction '{id}' domain quiescence")]
    RecoverQuiescence { id: String },
    #[error("failed to prepare schedule for domain '{domain}': {reason}")]
    PrepareSchedule { domain: DomainName, reason: String },
    #[error("transaction '{id}' commit task failed")]
    TaskJoin { id: String },
}

impl TransactionCommitError {
    fn consensus_error(&self) -> Option<&ConsensusError> {
        match self {
            Self::Proposal(ConsensusTransactionError::Consensus(error)) => Some(error),
            _ => None,
        }
    }
}

pub(in crate::application) struct TransactionModelStepContext<'a> {
    pub(in crate::application) transaction: &'a ReplicatedTransaction,
    pub(in crate::application) first_statement: usize,
    pub(in crate::application) statement_count: usize,
    pub(in crate::application) outcome:
        &'a ParkingMutex<Option<Result<ReplicatedTransaction, Report<TransactionCommitError>>>>,
}

impl Drop for TransactionExecutionLease {
    fn drop(&mut self) {
        self.executions.remove(&self.id);
    }
}

/// Why a session may not act on the transaction it names. `Detached` is a recoverable routing
/// condition rather than a user error: the leader simply has no binding for this session, so the
/// client is told to attach the transaction again and retry.
#[derive(Debug, PartialEq, Eq, Error)]
pub(in crate::application) enum SessionTransactionBindingError {
    #[error("no transaction is attached to this session")]
    Unbound,
    #[error("transaction '{id}' was taken over by another session")]
    TakenOver { id: String },
    #[error("transaction '{id}' is not attached to this leader; attach it before continuing")]
    Detached { id: String },
}

impl SessionTransactionBindingError {
    pub(in crate::application) fn into_command_result(self) -> CommandResult {
        let detached = matches!(self, Self::Detached { .. });
        let mut result = command_error(self.to_string());
        if detached {
            result.kind = i32::from(CommandResultKind::TransactionDetached);
        }
        result
    }
}

/// Configuration a bound transaction has queued but not yet applied. Completion resolves
/// identifiers against it so a session sees the names its own queued statements define, and stops
/// seeing the names they drop, before the transaction commits.
#[derive(Debug, Default)]
pub(in crate::application) struct QueuedConfiguration {
    pub(in crate::application) models: Vec<RegistryMutation>,
    resources: BTreeSet<ResourceName>,
}

impl QueuedConfiguration {
    /// Queued resource names matching `prefix`. A queued resource has no versions to suggest,
    /// because uploading one is not transaction content.
    pub(in crate::application) fn resource_suggestions(&self, prefix: &str) -> Vec<String> {
        let prefix = prefix.to_ascii_lowercase();
        self.resources
            .iter()
            .filter(|identifier| identifier.as_str().starts_with(&prefix))
            .map(|name| name.to_string())
            .collect()
    }
}

pub(in crate::application) fn transaction_status(
    transaction: &ReplicatedTransaction,
) -> ApiTransactionStatus {
    /// How a transaction ended, as the API reports it. Only a failure carries an error and the
    /// step it failed on; every other state reports neither.
    struct ReportedOutcome {
        state: ApiTransactionState,
        error: String,
        failing_step: Option<u64>,
    }

    impl ReportedOutcome {
        fn without_error(state: ApiTransactionState) -> Self {
            Self {
                state,
                error: String::new(),
                failing_step: None,
            }
        }
    }

    let ReportedOutcome {
        state,
        error,
        failing_step,
    } = match &transaction.state {
        TransactionState::Open => ReportedOutcome::without_error(ApiTransactionState::Open),
        TransactionState::Committing(_) => {
            ReportedOutcome::without_error(ApiTransactionState::Committing)
        }
        TransactionState::Finished(finished) => match &finished.outcome {
            TransactionOutcome::Committed => {
                ReportedOutcome::without_error(ApiTransactionState::Committed)
            }
            TransactionOutcome::Failed {
                failing_step,
                error,
            } => ReportedOutcome {
                state: ApiTransactionState::Failed,
                error: error.clone(),
                failing_step: failing_step.checked_add(1).map(|step| step.arch_into()),
            },
            TransactionOutcome::Reverted => {
                ReportedOutcome::without_error(ApiTransactionState::Reverted)
            }
            TransactionOutcome::Expired => {
                ReportedOutcome::without_error(ApiTransactionState::Expired)
            }
        },
    };
    ApiTransactionStatus {
        id: transaction.id.clone(),
        domain: transaction.domain.to_string(),
        state: i32::from(state),
        pending_count: transaction.pending_statement_count().arch_into(),
        completed_count: transaction.completed_statement_count().arch_into(),
        total_count: transaction.statement_count.arch_into(),
        error,
        failing_step,
    }
}

fn replicated_command_result(result: &CommandResult) -> TransactionCommandResult {
    TransactionCommandResult {
        success: result.success,
        message: result.message.clone(),
        diagnostics: result
            .diagnostics
            .iter()
            .map(|diagnostic| TransactionDiagnostic {
                message: diagnostic.message.clone(),
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        already_existed: result.already_existed,
    }
}

fn transaction_commit_result(transaction: &ReplicatedTransaction) -> CommandResult {
    let success = matches!(
        transaction.finished_outcome(),
        Some(TransactionOutcome::Committed)
    );
    let quiesce_level = transaction
        .commit_results()
        .iter()
        .filter_map(|step| step.quiesce_level)
        .max();
    let planned_relocations = transaction
        .commit_results()
        .iter()
        .filter_map(|step| step.planned_relocations)
        .sum::<usize>();
    let mut message = match transaction.finished_outcome() {
        Some(TransactionOutcome::Committed) => String::new(),
        Some(TransactionOutcome::Failed { error, .. }) => error.clone(),
        Some(outcome) => format!("transaction finished with outcome {}", outcome.as_str()),
        None => "transaction commit is still in progress".to_string(),
    };
    if let Some(quiesce_level) = quiesce_level {
        append_command_output(&mut message, &quiesce_level_message(quiesce_level));
    }
    if planned_relocations > 0 {
        append_command_output(
            &mut message,
            &format!("planned relocations: {planned_relocations}"),
        );
    }
    let mut reported = transaction
        .commit_results()
        .iter()
        .rev()
        .find(|step| !step.result.success);
    if reported.is_none() {
        reported = transaction.commit_results().last();
    }
    let diagnostics = match reported {
        Some(step) => step
            .result
            .diagnostics
            .iter()
            .map(|diagnostic| Diagnostic {
                message: diagnostic.message.clone(),
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        None => Vec::new(),
    };
    let mut result = CommandResult {
        success,
        message,
        diagnostics,
        kind: if success {
            i32::from(CommandResultKind::Ok)
        } else {
            i32::from(CommandResultKind::Error)
        },
        ..Default::default()
    };
    result.transaction = Some(transaction_status(transaction));
    result
}

fn is_queueable_transaction_statement(statement: &Statement) -> bool {
    statement.is_model_mutation()
        || matches!(
            statement,
            Statement::AlterDomain(_)
                | Statement::StartDomain(_)
                | Statement::StopDomain(_)
                | Statement::CreateResource(_)
        )
}

fn transaction_statement_label(statement: &Statement) -> &'static str {
    match statement {
        Statement::CreateDomain(_) => "CREATE DOMAIN",
        Statement::CreateUser(_) => "CREATE USER",
        Statement::UploadResource(_) => "UPLOAD RESOURCE",
        Statement::DropNode(_) => "DROP NODE",
        Statement::CordonNode(_) => "CORDON",
        Statement::UncordonNode(_) => "UNCORDON",
        Statement::DrainNode(_) => "DRAIN",
        Statement::Relocate(_) => "RELOCATE",
        Statement::LookupQuery(_) => "LOOKUP",
        Statement::ShowCreate(_)
        | Statement::ShowUdfs(_)
        | Statement::ShowPlacements(_)
        | Statement::ShowRelayMaterializedState(_)
        | Statement::ShowClusterStatus(_)
        | Statement::ShowTransactions(_) => "SHOW",
        Statement::DescribeRelay(_)
        | Statement::DescribeDomain(_)
        | Statement::DescribeIngestor(_)
        | Statement::DescribeResource(_)
        | Statement::DescribeLookup(_)
        | Statement::DescribeEndpoint(_)
        | Statement::DescribeJunction(_)
        | Statement::DescribeDeduplicator(_)
        | Statement::DescribeReingestor(_)
        | Statement::DescribeCorrelator(_)
        | Statement::DescribeReorderer(_)
        | Statement::DescribeEmitter(_)
        | Statement::DescribeWindowProcessor(_)
        | Statement::DescribeWasmProcessor(_)
        | Statement::DescribeUdf(_)
        | Statement::DescribePlacement(_)
        | Statement::DescribeRelocation(_) => "DESCRIBE",
        _ => "statement",
    }
}

impl SessionServiceImpl {
    /// The configuration this session's bound transaction has queued for `domain`. Queued
    /// configuration follows the binding, so a session that holds no transaction, one displaced by
    /// a takeover, one whose transaction this node does not hold, and one whose transaction
    /// configures another domain all fall back to committed configuration alone.
    pub(in crate::application) async fn queued_configuration(
        &self,
        subscriptions: &SessionSubscriptions,
        domain: Option<&DomainName>,
    ) -> QueuedConfiguration {
        let (Some(domain), Some(id)) = (domain, subscriptions.transaction_id()) else {
            return QueuedConfiguration::default();
        };
        if self
            .validate_session_transaction_binding(subscriptions)
            .is_err()
        {
            return QueuedConfiguration::default();
        }
        let Some(transaction) = self.inner.consensus.current_transaction(id).await else {
            return QueuedConfiguration::default();
        };
        if &transaction.domain != domain {
            return QueuedConfiguration::default();
        }

        let mut queued = QueuedConfiguration::default();
        for statement in transaction
            .statements
            .iter()
            .map(|queued_statement| &queued_statement.statement)
        {
            if statement.is_model_mutation() {
                queued
                    .models
                    .push(Self::transaction_registry_mutation(statement));
            } else if let Statement::CreateResource(create) = statement {
                queued.resources.insert(create.identifier.clone());
            }
        }
        queued
    }

    pub(in crate::application) async fn transaction_consensus_error_response(
        &self,
        error: ConsensusTransactionError,
    ) -> CommandResult {
        let message = error.to_string();
        match error {
            ConsensusTransactionError::Consensus(error) => {
                self.consensus_error_response(&error, message).await
            }
            _ => command_error(message),
        }
    }

    pub(in crate::application) async fn command_with_transaction_status(
        &self,
        mut result: CommandResult,
        subscriptions: &SessionSubscriptions,
    ) -> CommandResult {
        if result.transaction.is_none()
            && let Some(id) = subscriptions.transaction_id()
            && let Some(transaction) = self.inner.consensus.current_transaction(id).await
        {
            result.transaction = Some(transaction_status(&transaction));
        }
        result
    }

    /// Drops this node's leader-local transaction bindings when a test arms it, reproducing the
    /// soft state a node does not carry across a leadership change.
    pub(in crate::application) fn drop_transaction_bindings_if_armed(&self) {
        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .take_armed_transaction_binding_drop(self.inner.consensus.local_node_id())
        {
            self.inner.transaction_bindings.clear();
        }
    }

    pub(in crate::application) fn validate_session_transaction_binding(
        &self,
        subscriptions: &SessionSubscriptions,
    ) -> Result<(), SessionTransactionBindingError> {
        let Some(id) = subscriptions.transaction_id() else {
            return Err(SessionTransactionBindingError::Unbound);
        };
        match self.inner.transaction_bindings.get(id) {
            Some(binding) if binding.value() == &subscriptions.session_id => Ok(()),
            Some(_) => Err(SessionTransactionBindingError::TakenOver { id: id.to_string() }),
            None => Err(SessionTransactionBindingError::Detached { id: id.to_string() }),
        }
    }

    pub(in crate::application) fn release_session_transaction_binding(
        &self,
        subscriptions: &mut SessionSubscriptions,
    ) {
        let Some(id) = subscriptions.detach_transaction() else {
            return;
        };
        if self
            .inner
            .transaction_bindings
            .get(&id)
            .is_some_and(|binding| binding.value() == &subscriptions.session_id)
        {
            self.inner.transaction_bindings.remove(&id);
        }
    }

    pub(in crate::application) async fn clean_close_transaction(
        &self,
        subscriptions: &mut SessionSubscriptions,
    ) {
        let Some(id) = subscriptions.transaction_id().map(ToOwned::to_owned) else {
            return;
        };
        if self.inner.consensus.current_leader().await.as_ref()
            != Some(self.inner.consensus.local_node_id())
            || self
                .inner
                .transaction_bindings
                .get(&id)
                .is_none_or(|binding| binding.value() != &subscriptions.session_id)
        {
            return;
        }
        if let Some(transaction) = self.inner.consensus.current_transaction(&id).await
            && matches!(transaction.state, TransactionState::Open)
            && let Err(error) = self
                .inner
                .consensus
                .revert_transaction(id.clone(), subscriptions.user.clone(), current_timestamp())
                .await
        {
            warn!(
                transaction_id = id,
                error = %error,
                "failed to revert transaction during clean session close"
            );
        }
        self.release_session_transaction_binding(subscriptions);
    }

    pub(in crate::application) async fn attach_transaction(
        &self,
        request: proto::AttachTransactionRequest,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            return self
                .not_leader_response(&format!("ATTACH TRANSACTION {}", request.id), leader)
                .await;
        }
        let Some(transaction) = self.inner.consensus.current_transaction(&request.id).await else {
            return command_error(format!("transaction '{}' is unknown", request.id));
        };
        if transaction.owner != subscriptions.user {
            return command_error(format!(
                "transaction '{}' belongs to another user",
                request.id
            ));
        }
        if let TransactionState::Finished(finished) = &transaction.state {
            let mut recorded = transaction_commit_result(&transaction);
            recorded.transaction = None;
            let mut result = command_error(format!(
                "transaction '{}' finished with outcome {}",
                request.id,
                finished.outcome.as_str()
            ));
            if !recorded.message.is_empty() {
                result.results.push(recorded);
            }
            result.transaction = Some(transaction_status(&transaction));
            return result;
        }

        let transaction = match self
            .inner
            .consensus
            .touch_transaction(
                request.id.clone(),
                subscriptions.user.clone(),
                current_timestamp(),
            )
            .await
        {
            Ok(transaction) => transaction,
            Err(error) => return self.transaction_consensus_error_response(error).await,
        };
        self.release_session_transaction_binding(subscriptions);
        self.inner
            .transaction_bindings
            .insert(request.id.clone(), subscriptions.session_id.clone());
        subscriptions.bind_transaction(request.id.clone());
        let mut result = command_ok(format!("attached transaction '{}'", request.id));
        result.transaction = Some(transaction_status(&transaction));
        result
    }

    /// Resolves the domain a `BEGIN` binds its transaction to. The domain must already exist,
    /// because a transaction can no longer create one and every statement it queues belongs to it.
    pub(in crate::application) async fn resolve_transaction_domain(
        &self,
        request_domain: &str,
    ) -> Result<DomainName, String> {
        let domain = match parse_request_domain(request_domain) {
            Ok(domain) => domain,
            Err(RequestDomainError::Missing) => {
                return Err("no active domain selected".to_string());
            }
            Err(RequestDomainError::Invalid) => return Err("invalid domain".to_string()),
        };
        if self.inner.consensus.current_domain(&domain).await.is_none() {
            return Err(format!("domain '{}' does not exist", domain.as_str()));
        }
        Ok(domain)
    }

    pub(in crate::application) async fn queue_transaction_statement(
        &self,
        command: PendingSessionCommand,
        subscriptions: &SessionSubscriptions,
    ) -> CommandResult {
        if let Err(error) = self.validate_session_transaction_binding(subscriptions) {
            return error.into_command_result();
        }
        let Some(id) = subscriptions.transaction_id() else {
            return command_error("no transaction is attached to this session".to_string());
        };
        let ClientStatement::Server(statement) = command.statement else {
            return command_error(
                "session-scoped and client-local statements cannot be queued in a transaction"
                    .to_string(),
            );
        };
        if !is_queueable_transaction_statement(&statement) {
            return command_error(format!(
                "{} cannot be queued in a transaction; a transaction applies to one existing \
                 domain and queues only that domain's configuration statements",
                transaction_statement_label(&statement)
            ));
        }
        let domain = match parse_request_domain(&command.domain) {
            Ok(domain) => domain,
            Err(RequestDomainError::Missing) => {
                return command_error("no active domain selected".to_string());
            }
            Err(RequestDomainError::Invalid) => {
                return command_error("invalid domain".to_string());
            }
        };
        let queued = TransactionStatement {
            source: command.source,
            statement,
        };
        let Some(transaction) = self.inner.consensus.current_transaction(id).await else {
            return command_error(format!("transaction '{id}' is unknown"));
        };
        let limits = TransactionQueueLimits {
            max_statements: self.inner.transaction_max_statements,
            max_source_bytes: self.inner.transaction_max_source_bytes,
        };
        if let Err(error) =
            transaction.validate_queue_admission(&subscriptions.user, &domain, &queued, limits)
        {
            return command_error(error.to_string());
        }
        let quiesce_level = match self
            .preflight_transaction_statement(&transaction, &queued)
            .await
        {
            Ok(quiesce_level) => quiesce_level,
            Err(error) => return command_error(error),
        };
        match self
            .inner
            .consensus
            .queue_transaction_statement(
                id.to_string(),
                subscriptions.user.clone(),
                domain,
                current_timestamp(),
                queued,
                limits,
            )
            .await
        {
            Ok(transaction) => {
                let message = match quiesce_level {
                    Some(quiesce_level) => quiesce_level_message(quiesce_level),
                    None => String::new(),
                };
                let mut result = command_ok(message);
                result.transaction = Some(transaction_status(&transaction));
                result
            }
            Err(error) => self.transaction_consensus_error_response(error).await,
        }
    }

    async fn preflight_transaction_statement(
        &self,
        transaction: &ReplicatedTransaction,
        candidate: &TransactionStatement,
    ) -> Result<Option<QuiesceLevel>, String> {
        let (mut domains, resources) = tokio::join!(
            self.inner.consensus.current_domains(),
            self.inner.consensus.current_resources(),
        );
        let mut resource_names = resources
            .next_version_by_resource
            .iter()
            .filter(|counter| counter.domain == transaction.domain)
            .map(|counter| counter.identifier.clone())
            .collect::<BTreeSet<_>>();
        let domain_id = &transaction.domain;
        let mut model_mutations = Vec::<RegistryMutation>::new();
        let candidate_is_model_mutation = candidate.statement.is_model_mutation();
        let mut candidate_quiesce_level =
            candidate_is_model_mutation.then_some(QuiesceLevel::Dynamic);

        for queued in transaction
            .statements
            .iter()
            .chain(std::iter::once(candidate))
        {
            match &queued.statement {
                Statement::AlterDomain(alter) => {
                    let domain = domains
                        .get_mut(domain_id)
                        .ok_or_else(|| format!("domain '{}' does not exist", domain_id.as_str()))?;
                    if let DomainStatus::Paused = domain.status {
                        return Err(format!(
                            "domain '{}' is paused by a model alteration",
                            domain_id.as_str()
                        ));
                    }
                    domain.config.placement = alter.policy;
                }
                Statement::StartDomain(start) => {
                    let domain = domains
                        .get_mut(domain_id)
                        .ok_or_else(|| format!("domain '{}' does not exist", domain_id.as_str()))?;
                    validate_domain_config(&domain.config)?;
                    if let DomainStatus::Running = domain.status {
                        return Err(format!(
                            "domain '{}' is already running",
                            domain_id.as_str()
                        ));
                    }
                    if let DomainStatus::Paused = domain.status {
                        return Err(format!(
                            "domain '{}' is paused for a model alteration",
                            domain_id.as_str()
                        ));
                    }
                    domain.status = DomainStatus::Running;
                    domain.last_start = start.start.clone();
                    domain.start_version = domain.start_version.checked_add(1).assured(
                        "a domain cannot be started 2^64 times in the lifetime of a cluster",
                    );
                }
                Statement::StopDomain(_) => {
                    let domain = domains
                        .get_mut(domain_id)
                        .ok_or_else(|| format!("domain '{}' does not exist", domain_id.as_str()))?;
                    if let DomainStatus::Stopped = domain.status {
                        return Err(format!(
                            "domain '{}' is already stopped",
                            domain_id.as_str()
                        ));
                    }
                    domain.status = DomainStatus::Stopped;
                    domain.clock = None;
                }
                Statement::CreateResource(create) => {
                    if !resource_names.insert(create.identifier.clone()) && !create.if_not_exists {
                        return Err(format!(
                            "resource '{}' already exists",
                            create.identifier.as_str()
                        ));
                    }
                }
                statement if statement.is_model_mutation() => {
                    let domain = domains
                        .get(domain_id)
                        .ok_or_else(|| format!("domain '{}' does not exist", domain_id.as_str()))?;
                    if let DomainStatus::Paused = domain.status {
                        return Err(format!(
                            "domain '{}' is paused by a model alteration",
                            domain_id.as_str()
                        ));
                    }
                    if let Statement::Create(create) = statement
                        && create.if_not_exists
                        && self
                            .inner
                            .registry
                            .contains(domain_id, create.body.kind(), create.body.name())
                            .map_err(|error| error.to_string())?
                    {
                        continue;
                    }
                    model_mutations.push(Self::transaction_registry_mutation(statement));
                }
                _ => {
                    return Err(format!(
                        "{} is not valid transaction content",
                        transaction_statement_label(&queued.statement)
                    ));
                }
            }
        }

        if model_mutations.is_empty() {
            return Ok(candidate_quiesce_level);
        }
        tokio::task::consume_budget().await;
        let preflight = self
            .inner
            .registry
            .preflight_transaction_mutations(domain_id, &model_mutations)
            .map_err(|error| format!("transaction statement failed preflight: {error}"))?;
        if candidate_is_model_mutation {
            candidate_quiesce_level = preflight.mutation_quiesce_levels().last().copied();
        }
        let Some(planned) = preflight.planned() else {
            return Ok(candidate_quiesce_level);
        };
        let domain = domains
            .get(domain_id)
            .verified("replaying the transaction resolved this domain before reaching the step");
        self.validate_changed_model_bindings(domain_id, domain.config.pace, planned)
            .await?;
        self.prepare_planned_domain_udfs(planned).await?;
        self.prepare_domain_schedule(
            domain_id,
            planned.candidate_graph(),
            domain.config.placement,
        )
        .await?;
        Ok(candidate_quiesce_level)
    }

    fn transaction_registry_mutation(statement: &Statement) -> RegistryMutation {
        match statement {
            Statement::Create(create) => RegistryMutation::Create(create.body.clone()),
            Statement::AlterSchema(alter) => RegistryMutation::AlterSchema(alter.clone()),
            Statement::AlterWireJsonSchema(alter) => {
                RegistryMutation::AlterWireJsonSchema(alter.clone())
            }
            Statement::AlterWireCborSchema(alter) => {
                RegistryMutation::AlterWireCborSchema(alter.clone())
            }
            Statement::AlterWireAvroSchema(alter) => {
                RegistryMutation::AlterWireAvroSchema(alter.clone())
            }
            Statement::AlterRelay(alter) => RegistryMutation::AlterRelay(alter.clone()),
            Statement::AlterJunction(alter) => RegistryMutation::AlterJunction(alter.clone()),
            Statement::AlterDeduplicator(alter) => {
                RegistryMutation::AlterDeduplicator(alter.clone())
            }
            Statement::AlterReorderer(alter) => RegistryMutation::AlterReorderer(alter.clone()),
            Statement::AlterEmitter(alter) => RegistryMutation::AlterEmitter(alter.clone()),
            Statement::AlterIngestor(alter) => RegistryMutation::AlterIngestor(alter.clone()),
            Statement::AlterReingestor(alter) => RegistryMutation::AlterReingestor(alter.clone()),
            Statement::AlterGenerator(alter) => RegistryMutation::AlterGenerator(alter.clone()),
            Statement::AlterPlacement(alter) => RegistryMutation::AlterPlacement(alter.clone()),
            Statement::Drop(drop) => RegistryMutation::Drop(drop.clone()),
            _ => unreachable!("transaction registry mutation requires a model mutation"),
        }
    }

    pub(in crate::application) async fn revert_bound_transaction(
        &self,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        if let Err(error) = self.validate_session_transaction_binding(subscriptions) {
            return error.into_command_result();
        }
        let Some(id) = subscriptions.transaction_id().map(ToOwned::to_owned) else {
            return command_error("REVERT requires an active transaction".to_string());
        };
        let previous = self.inner.consensus.current_transaction(&id).await;
        match self
            .inner
            .consensus
            .revert_transaction(id.clone(), subscriptions.user.clone(), current_timestamp())
            .await
        {
            Ok(transaction) => {
                let dropped = match previous {
                    Some(transaction) => transaction.statements.len(),
                    None => 0,
                };
                self.release_session_transaction_binding(subscriptions);
                let mut result = command_ok(format!(
                    "transaction reverted: dropped {dropped} command(s); id '{id}'"
                ));
                result.transaction = Some(transaction_status(&transaction));
                result
            }
            Err(error) => self.transaction_consensus_error_response(error).await,
        }
    }

    pub(in crate::application) async fn commit_bound_transaction(
        &self,
        _tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        if let Err(error) = self.validate_session_transaction_binding(subscriptions) {
            return error.into_command_result();
        }
        let Some(id) = subscriptions.transaction_id().map(ToOwned::to_owned) else {
            return command_error("COMMIT requires an active transaction".to_string());
        };
        let started = match self
            .inner
            .consensus
            .start_transaction_commit(id.clone(), subscriptions.user.clone(), current_timestamp())
            .await
        {
            Ok(transaction) => transaction,
            Err(error) => return self.transaction_consensus_error_response(error).await,
        };
        let finished = if started.statements.is_empty() {
            self.inner
                .consensus
                .finish_empty_transaction_commit(id.clone(), current_timestamp())
                .await
                .map_err(|error| Report::new(TransactionCommitError::Proposal(error)))
        } else {
            // A replicated commit owns its execution independently of the session. Keep its
            // model-mutation future off the session's poll stack as well.
            let service = self.clone();
            let commit_id = id.clone();
            match tokio::spawn(async move { service.execute_replicated_commit(&commit_id).await })
                .await
            {
                Ok(result) => result,
                Err(error) => Err(Report::new(error)
                    .change_context(TransactionCommitError::TaskJoin { id: id.clone() })),
            }
        };
        match finished {
            Ok(transaction) => {
                if matches!(transaction.state, TransactionState::Finished(_)) {
                    self.release_session_transaction_binding(subscriptions);
                }
                transaction_commit_result(&transaction)
            }
            Err(error) => {
                let proposal_error = error
                    .current_context()
                    .consensus_error()
                    .or_else(|| error.downcast_ref::<ConsensusError>());
                let mut result =
                    if let Some(ConsensusError::LeadershipLost { leader_id }) = proposal_error {
                        self.not_leader_response("COMMIT", leader_id.clone()).await
                    } else {
                        command_error(format!(
                            "transaction '{id}' commit remains in progress after an execution \
                             error: {error}"
                        ))
                    };
                if let Some(transaction) = self.inner.consensus.current_transaction(&id).await {
                    result.transaction = Some(transaction_status(&transaction));
                }
                result
            }
        }
    }

    async fn execute_replicated_commit(
        &self,
        id: &str,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let _commit_execution = self.inner.transaction_commit_execution.lock().await;
        let mut wait = interval(Duration::from_millis(100));
        let lease = loop {
            tokio::task::consume_budget().await;
            match self.inner.transaction_executions.entry(id.to_string()) {
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    entry.insert(());
                    break TransactionExecutionLease {
                        executions: self.inner.transaction_executions.clone(),
                        id: id.to_string(),
                    };
                }
                dashmap::mapref::entry::Entry::Occupied(_) => {
                    if let Some(transaction) = self.inner.consensus.current_transaction(id).await
                        && matches!(transaction.state, TransactionState::Finished(_))
                    {
                        return Ok(transaction);
                    }
                    wait.tick().await;
                }
            }
        };

        let result = self.run_replicated_commit(id).await;
        drop(lease);
        result
    }

    async fn run_replicated_commit(
        &self,
        id: &str,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        self.inner
            .registry
            .synchronize_cluster_schedule(&self.inner.consensus.current_schedule().await)
            .change_context(TransactionCommitError::SynchronizeRegistry { id: id.to_string() })?;
        loop {
            tokio::task::consume_budget().await;
            let leader_id = self.inner.consensus.current_leader().await;
            if leader_id.as_ref() != Some(self.inner.consensus.local_node_id()) {
                return Err(Report::new(TransactionCommitError::Proposal(
                    ConsensusTransactionError::Consensus(ConsensusError::LeadershipLost {
                        leader_id,
                    }),
                )));
            }
            let transaction = self
                .inner
                .consensus
                .current_transaction(id)
                .await
                .ok_or_else(|| {
                    Report::new(TransactionCommitError::UnknownTransaction { id: id.to_string() })
                })?;
            let progress = match &transaction.state {
                TransactionState::Committing(progress) => progress,
                TransactionState::Finished(_) => return Ok(transaction),
                TransactionState::Open => {
                    return Err(Report::new(TransactionCommitError::TransactionOpen {
                        id: id.to_string(),
                    }));
                }
            };
            let first_statement = progress.next_statement;
            let Some(first) = transaction.statements.get(first_statement) else {
                return self
                    .inner
                    .consensus
                    .finish_empty_transaction_commit(id.to_string(), current_timestamp())
                    .await
                    .map_err(|error| Report::new(TransactionCommitError::Proposal(error)));
            };
            self.recover_transaction_quiescence(&transaction, first_statement)
                .await?;

            if first.statement.is_model_mutation() {
                let domain = transaction.domain.clone();
                let mut statements = Vec::new();
                let mut sources = Vec::new();
                for queued in transaction.statements.iter().skip(first_statement) {
                    if !queued.statement.is_model_mutation() {
                        break;
                    }
                    statements.push(queued.statement.clone());
                    sources.push(queued.source.clone());
                }
                let statement_count = statements.len();
                let outcome = ParkingMutex::new(None);
                let result = self
                    .process_model_mutation_batch_with_transaction(
                        statements,
                        &sources.join("; "),
                        domain.as_str(),
                        Some(TransactionModelStepContext {
                            transaction: &transaction,
                            first_statement,
                            statement_count,
                            outcome: &outcome,
                        }),
                    )
                    .await;
                let recorded = outcome.lock().take();
                let advanced = match recorded {
                    Some(Ok(transaction)) => transaction,
                    Some(Err(error)) => return Err(error),
                    None if result.kind == i32::from(CommandResultKind::NotLeader) => {
                        return Err(Report::new(TransactionCommitError::Proposal(
                            ConsensusTransactionError::Consensus(ConsensusError::LeadershipLost {
                                leader_id: self.inner.consensus.current_leader().await,
                            }),
                        )));
                    }
                    None if !result.success => {
                        self.record_transaction_step(
                            &transaction,
                            first_statement,
                            statement_count,
                            result,
                            None,
                            None,
                        )
                        .await?
                    }
                    None => {
                        return Err(Report::new(TransactionCommitError::MissingProgress {
                            id: id.to_string(),
                        }));
                    }
                };
                self.pause_transaction_commit_if_armed(&advanced).await;
                if matches!(advanced.state, TransactionState::Finished(_)) {
                    return Ok(advanced);
                }
                continue;
            }

            let advanced = self
                .execute_transaction_configuration_step(&transaction, first_statement)
                .await?;
            self.pause_transaction_commit_if_armed(&advanced).await;
            if matches!(advanced.state, TransactionState::Finished(_)) {
                return Ok(advanced);
            }
        }
    }

    async fn pause_transaction_commit_if_armed(&self, _transaction: &ReplicatedTransaction) {
        #[cfg(feature = "testing")]
        if let TransactionState::Committing(_) = _transaction.state {
            self.inner
                .runtime
                .pause_transaction_commit_after_progress_if_armed(
                    self.inner.consensus.local_node_id(),
                    _transaction.completed_statement_count(),
                )
                .await;
        }
    }

    /// A model-mutation step pauses the transaction's domain and resumes it once the step
    /// finishes. When a new leader adopts a commit mid-flight, that pause may still be recorded in
    /// replicated state; resume it unless the step about to run needs it held.
    async fn recover_transaction_quiescence(
        &self,
        transaction: &ReplicatedTransaction,
        current_statement: usize,
    ) -> Result<(), Report<TransactionCommitError>> {
        if transaction
            .statements
            .get(current_statement)
            .is_some_and(|statement| statement.statement.is_model_mutation())
        {
            return Ok(());
        }
        let mut completed_model_mutation = false;
        for result in transaction.commit_results() {
            tokio::task::consume_budget().await;
            let Some(statements) = result
                .first_statement
                .checked_add(result.statement_count)
                .and_then(|end| transaction.statements.get(result.first_statement..end))
            else {
                return Err(Report::new(TransactionCommitError::InvalidProgress {
                    id: transaction.id.clone(),
                }));
            };
            if !statements.is_empty()
                && statements
                    .iter()
                    .all(|statement| statement.statement.is_model_mutation())
            {
                completed_model_mutation = true;
                break;
            }
        }
        if !completed_model_mutation {
            return Ok(());
        }
        let domain = &transaction.domain;
        let Some(state) = self.inner.consensus.current_domain(domain).await else {
            return Ok(());
        };
        if let DomainStatus::Paused = state.status {
            self.apply_current_cluster_state().await.change_context(
                TransactionCommitError::RecoverQuiescence {
                    id: transaction.id.clone(),
                },
            )?;
            self.wait_for_paused_domain_drain(domain)
                .await
                .change_context(TransactionCommitError::RecoverQuiescence {
                    id: transaction.id.clone(),
                })?;
            self.resume_domain_after_alter(domain)
                .await
                .change_context(TransactionCommitError::RecoverQuiescence {
                    id: transaction.id.clone(),
                })?;
        }
        Ok(())
    }

    pub(in crate::application) async fn record_transaction_step(
        &self,
        transaction: &ReplicatedTransaction,
        first_statement: usize,
        statement_count: usize,
        result: CommandResult,
        quiesce_level: Option<QuiesceLevel>,
        effect: Option<TransactionStepEffect>,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let next_statement = first_statement
            .checked_add(statement_count)
            .assured("a recorded commit step counts statements of the transaction it belongs to");
        let completion = if result.success {
            (next_statement == transaction.statements.len())
                .then_some(TransactionOutcome::Committed)
        } else {
            Some(TransactionOutcome::Failed {
                failing_step: first_statement,
                error: result.message.clone(),
            })
        };
        let effect = if result.success { effect } else { None };
        let planned_relocations = match effect.as_ref() {
            Some(
                TransactionStepEffect::ReplaceDomainSchedule {
                    expected_schedule,
                    schedule,
                    ..
                }
                | TransactionStepEffect::PutDomainAndSchedule {
                    expected_schedule,
                    schedule,
                    ..
                },
            ) => {
                let count =
                    planned_ownership_moves(expected_schedule.as_deref(), schedule.as_deref())
                        .len();
                (count > 0).then_some(count)
            }
            _ => None,
        };
        self.inner
            .consensus
            .advance_transaction_commit(TransactionCommitAdvance {
                id: transaction.id.clone(),
                expected_next_statement: first_statement,
                next_statement,
                at: current_timestamp(),
                result: TransactionStepResult {
                    first_statement,
                    statement_count,
                    quiesce_level,
                    planned_relocations,
                    result: replicated_command_result(&result),
                },
                effect,
                completion,
            })
            .await
            .map_err(|error| Report::new(TransactionCommitError::Proposal(error)))
    }

    async fn execute_transaction_configuration_step(
        &self,
        transaction: &ReplicatedTransaction,
        statement_index: usize,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let queued = transaction.statements.get(statement_index).verified(
            "the caller checked this index against the same statement list before dispatching the \
             step",
        );
        let mut ownership_handoff = None;
        let mut step_quiesce_level = None;
        let domain_id = &transaction.domain;
        let _alter_guard = if let Statement::AlterDomain(_) = &queued.statement {
            let Some(guard) = self.inner.runtime.try_begin_domain_alter(domain_id) else {
                return self
                    .record_transaction_step(
                        transaction,
                        statement_index,
                        1,
                        command_error(
                            DomainAlterError::ConcurrentAlter {
                                domain: domain_id.clone(),
                            }
                            .to_string(),
                        ),
                        None,
                        None,
                    )
                    .await;
            };
            Some(guard)
        } else {
            None
        };
        let (result, effect) = match &queued.statement {
            Statement::AlterDomain(alter) => {
                let Some(previous) = self.inner.consensus.current_domain(domain_id).await else {
                    return self
                        .record_transaction_step(
                            transaction,
                            statement_index,
                            1,
                            command_error(format!(
                                "domain '{}' does not exist",
                                domain_id.as_str()
                            )),
                            None,
                            None,
                        )
                        .await;
                };
                if let DomainStatus::Paused = previous.status {
                    (
                        command_error(format!(
                            "domain '{}' is paused by a model alteration",
                            domain_id.as_str()
                        )),
                        None,
                    )
                } else if previous.config.placement == alter.policy {
                    step_quiesce_level = Some(QuiesceLevel::Dynamic);
                    (
                        command_ok(format!(
                            "domain '{}' placement is already {}; {}\nplanned relocations: 0",
                            domain_id.as_str(),
                            alter.policy.as_ref(),
                            quiesce_level_message(QuiesceLevel::Dynamic)
                        )),
                        None,
                    )
                } else {
                    let graph = self.inner.registry.active_graph(domain_id);
                    let expected_schedule = self
                        .inner
                        .consensus
                        .current_schedule()
                        .await
                        .domain(domain_id)
                        .cloned();
                    let PreparedDomainSchedule {
                        mut schedule,
                        relocations,
                    } = self
                        .prepare_domain_schedule(domain_id, graph, alter.policy)
                        .await
                        .map_err(|reason| {
                            Report::new(TransactionCommitError::PrepareSchedule {
                                domain: domain_id.clone(),
                                reason,
                            })
                        })?;
                    let quiesce_level =
                        if matches!(previous.status, DomainStatus::Running) && relocations > 0 {
                            QuiesceLevel::EntityPause
                        } else {
                            QuiesceLevel::Dynamic
                        };
                    step_quiesce_level = Some(quiesce_level);
                    let mut next = previous.clone();
                    next.config.placement = alter.policy;
                    if let Some(schedule) = schedule.as_mut() {
                        mark_complete_ownership_transitions(expected_schedule.as_ref(), schedule);
                    }
                    let handoff = if relocations > 0 {
                        self.begin_planned_ownership_handoff(
                            domain_id,
                            expected_schedule.as_ref(),
                            schedule.as_ref(),
                        )
                        .await
                    } else {
                        Ok(None)
                    };
                    match handoff {
                        Ok(handoff) => {
                            ownership_handoff = handoff;
                            (
                                command_ok(format!(
                                    "set domain '{}' placement to {}; {}\nplanned relocations: \
                                     {relocations}",
                                    domain_id.as_str(),
                                    alter.policy.as_ref(),
                                    quiesce_level_message(quiesce_level)
                                )),
                                Some(TransactionStepEffect::PutDomainAndSchedule {
                                    expected_domain: Box::new(previous),
                                    expected_schedule: expected_schedule.map(Box::new),
                                    domain: Box::new(next),
                                    schedule: schedule.map(Box::new),
                                }),
                            )
                        }
                        Err(error) => (command_error(error.to_string()), None),
                    }
                }
            }
            Statement::CreateResource(create) => {
                let resources = self.inner.consensus.current_resources().await;
                if resources.is_declared(domain_id, &create.identifier) {
                    if create.if_not_exists {
                        (
                            command_ok_already_existed(format!(
                                "resource '{}' already exists",
                                create.identifier.as_str()
                            )),
                            None,
                        )
                    } else {
                        (
                            command_error(format!(
                                "resource '{}' already exists",
                                create.identifier.as_str()
                            )),
                            None,
                        )
                    }
                } else {
                    (
                        command_ok(format!("created resource '{}'", create.identifier.as_str())),
                        Some(TransactionStepEffect::CreateResourceCatalog {
                            identifier: create.identifier.clone(),
                        }),
                    )
                }
            }
            Statement::StartDomain(start) => {
                let Some(domain) = self.inner.consensus.current_domain(domain_id).await else {
                    return self
                        .record_transaction_step(
                            transaction,
                            statement_index,
                            1,
                            command_error(format!(
                                "domain '{}' does not exist",
                                domain_id.as_str()
                            )),
                            None,
                            None,
                        )
                        .await;
                };
                if let Err(message) = validate_domain_config(&domain.config) {
                    (command_error(message), None)
                } else if let DomainStatus::Running = domain.status {
                    (
                        command_error(format!(
                            "domain '{}' is already running",
                            domain_id.as_str()
                        )),
                        None,
                    )
                } else if let DomainStatus::Paused = domain.status {
                    (
                        command_error(format!(
                            "domain '{}' is paused for a model alteration",
                            domain_id.as_str()
                        )),
                        None,
                    )
                } else {
                    let authority = if let DomainPace::Paced = domain.config.pace {
                        match self.selected_domain_clock_authority(domain_id).await {
                            Some(authority) => Ok(Some(authority)),
                            None => Err(format!(
                                "no live voter is available to own the clock for domain '{}'",
                                domain_id.as_str()
                            )),
                        }
                    } else {
                        Ok(None)
                    };
                    match authority {
                        Ok(authority) => {
                            match self
                                .resolve_domain_start(domain_id, &domain, &start.start)
                                .await
                            {
                                Ok(resolved_start) => (
                                    command_ok(format!("starting domain '{}'", domain_id.as_str())),
                                    Some(TransactionStepEffect::StartDomain {
                                        domain_id: domain_id.clone(),
                                        expected_start_version: domain.start_version,
                                        start: resolved_start.concrete_start,
                                        clock: matches!(domain.config.pace, DomainPace::Paced)
                                            .then_some(resolved_start.clock),
                                        authority,
                                    }),
                                ),
                                Err(error) => (
                                    command_error(format!(
                                        "failed to construct domain clock start for '{}': {error}",
                                        domain_id.as_str()
                                    )),
                                    None,
                                ),
                            }
                        }
                        Err(message) => (command_error(message), None),
                    }
                }
            }
            Statement::StopDomain(_) => {
                let Some(domain) = self.inner.consensus.current_domain(domain_id).await else {
                    return self
                        .record_transaction_step(
                            transaction,
                            statement_index,
                            1,
                            command_error(format!(
                                "domain '{}' does not exist",
                                domain_id.as_str()
                            )),
                            None,
                            None,
                        )
                        .await;
                };
                if let DomainStatus::Stopped = domain.status {
                    (
                        command_error(format!(
                            "domain '{}' is already stopped",
                            domain_id.as_str()
                        )),
                        None,
                    )
                } else {
                    (
                        command_ok(format!("stopped domain '{}'", domain_id.as_str())),
                        Some(TransactionStepEffect::StopDomain {
                            domain_id: domain_id.clone(),
                            expected_start_version: domain.start_version,
                        }),
                    )
                }
            }
            _ => (
                command_error(format!(
                    "{} is not valid transaction content",
                    transaction_statement_label(&queued.statement)
                )),
                None,
            ),
        };

        let succeeded = result.success;
        let advanced = match self
            .record_transaction_step(
                transaction,
                statement_index,
                1,
                result,
                step_quiesce_level,
                effect,
            )
            .await
        {
            Ok(advanced) => advanced,
            Err(error) => {
                if let Some(handoff) = ownership_handoff.take() {
                    self.abort_planned_ownership_handoff(domain_id, handoff)
                        .await;
                }
                return Err(error);
            }
        };
        if succeeded {
            let activation_error = self.apply_current_cluster_state().await.err();
            if let Some(error) = &activation_error {
                self.broadcast_error(format!(
                    "failed to reconcile runtime after transaction '{}' step {}: {error}",
                    transaction.id,
                    statement_index
                        .checked_add(1)
                        .assured("the index names a statement of a transaction held in memory")
                ));
            }
            if let Some(handoff) = ownership_handoff.take() {
                if let Some(error) = &activation_error {
                    self.defer_planned_ownership_handoff_release(domain_id, handoff, error);
                } else if let Err(error) = self
                    .finish_planned_ownership_handoff(domain_id, handoff)
                    .await
                {
                    self.broadcast_error(format!(
                        "failed to confirm ownership state activation after transaction '{}' step \
                         {}: {error}",
                        transaction.id,
                        statement_index
                            .checked_add(1)
                            .assured("the index names a statement of a transaction held in memory")
                    ));
                }
            }
        }
        if let Some(handoff) = ownership_handoff {
            self.abort_planned_ownership_handoff(domain_id, handoff)
                .await;
        }
        Ok(advanced)
    }

    pub(in crate::application) async fn reconcile_transactions_once(&self) {
        if self.inner.consensus.current_leader().await.as_ref()
            != Some(self.inner.consensus.local_node_id())
        {
            return;
        }
        let now = current_timestamp();
        let idle_before = subtract_timestamp_duration(now, self.inner.transaction_idle_timeout);
        let finished_before =
            subtract_timestamp_duration(now, self.inner.transaction_tombstone_retention);
        let transactions = self.inner.consensus.current_transactions().await;

        for transaction in transactions.values() {
            tokio::task::consume_budget().await;
            match &transaction.state {
                TransactionState::Open
                    if !self
                        .inner
                        .transaction_bindings
                        .contains_key(&transaction.id)
                        && transaction.last_activity_at <= idle_before =>
                {
                    match self
                        .inner
                        .consensus
                        .expire_transaction(transaction.id.clone(), now, idle_before)
                        .await
                    {
                        Ok(expired)
                            if matches!(
                                expired.finished_outcome(),
                                Some(TransactionOutcome::Expired)
                            ) =>
                        {
                            self.inner.transaction_bindings.remove(&transaction.id);
                            info!(
                                transaction_id = transaction.id,
                                owner = transaction.owner.as_str(),
                                "expired orphaned NSPL transaction"
                            );
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(
                                transaction_id = transaction.id,
                                error = %error,
                                "failed to expire orphaned NSPL transaction"
                            );
                        }
                    }
                }
                TransactionState::Committing(_) => {
                    if let Err(error) = self.execute_replicated_commit(&transaction.id).await {
                        warn!(
                            transaction_id = transaction.id,
                            error = %error, "failed to resume replicated NSPL commit"
                        );
                    }
                }
                TransactionState::Finished(_) => {
                    self.inner.transaction_bindings.remove(&transaction.id);
                }
                TransactionState::Open => {}
            }
        }
        if let Err(error) = self
            .inner
            .consensus
            .remove_finished_transactions(finished_before)
            .await
        {
            warn!(error = %error, "failed to remove expired transaction tombstones");
        }
    }

    pub(in crate::application) async fn show_transactions(&self) -> CommandResult {
        let now = current_timestamp();
        let transactions = self.inner.consensus.current_transactions().await;
        let message = if transactions.is_empty() {
            "no transactions".to_string()
        } else {
            transactions
                .values()
                .map(|transaction| {
                    let age = now
                        .as_datetime()
                        .signed_duration_since(*transaction.created_at.as_datetime())
                        .to_std()
                        .unwrap_or_default();
                    let idle = now
                        .as_datetime()
                        .signed_duration_since(*transaction.last_activity_at.as_datetime())
                        .to_std()
                        .unwrap_or_default();
                    format!(
                        "id={} owner={} domain={} state={} pending={} progress={}/{} age={} \
                         idle={}",
                        transaction.id,
                        transaction.owner.as_str(),
                        transaction.domain.as_str(),
                        transaction.state.as_str(),
                        transaction.pending_statement_count(),
                        transaction.completed_statement_count(),
                        transaction.statement_count,
                        humantime::format_duration(age),
                        humantime::format_duration(idle),
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        command_ok(message)
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{CreateRelay, CreateSchema, DomainName, ModelName};
    use tokio::sync::mpsc;

    use super::super::{
        subscription::SessionSubscriptions,
        test_fixtures::{
            TestService, build_test_service, command_transaction_state, create_test_domain, named,
        },
    };
    use crate::{
        proto,
        proto::{CommandRequest, TransactionState as ApiTransactionState},
    };

    #[tokio::test]
    async fn process_command_commits_explicit_transaction_without_trailing_semicolon() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(false).await;
        create_test_domain(&service.inner.consensus, "prod").await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let result = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE RELAY notifications SCHEMA notification UNBRANCHED; \
                            CREATE SCHEMA notification ( user_id U32 ); COMMIT"
                        .to_string(),
                    domain: "prod".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(result.success, "command must succeed: {}", result.message);
        assert_eq!(
            command_transaction_state(&result),
            Some(ApiTransactionState::Committed)
        );
        assert!(result.message.contains("quiesce level: DYNAMIC"));
        let commit = result
            .results
            .last()
            .expect("COMMIT result must be retained");
        assert_eq!(commit.message, "quiesce level: DYNAMIC");

        let schema = registry
            .get::<CreateSchema>(
                &DomainName::parse("prod").expect("valid domain"),
                named::<ModelName>("notification"),
            )
            .expect("registry get should succeed");
        assert!(
            schema.is_some(),
            "batch should create schema in prod domain"
        );
        let relay = registry
            .get::<CreateRelay>(
                &DomainName::parse("prod").expect("valid domain"),
                named::<ModelName>("notifications"),
            )
            .expect("registry get should succeed");
        assert!(
            relay.is_some(),
            "model create batch should resolve relay references atomically"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_queues_transaction_across_requests_and_reverts() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let begin = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(begin.success);
        assert_eq!(
            command_transaction_state(&begin),
            Some(ApiTransactionState::Open)
        );

        let queued = service
            .process_command(
                CommandRequest {
                    query: "CREATE SCHEMA queued_event ( user_id U32 );".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(queued.success);
        assert_eq!(queued.message, "quiesce level: DYNAMIC");
        assert_eq!(
            command_transaction_state(&queued),
            Some(ApiTransactionState::Open)
        );
        assert!(
            registry
                .get::<CreateSchema>(
                    &DomainName::parse("default").expect("valid domain"),
                    named::<ModelName>("queued_event"),
                )
                .expect("registry get should succeed")
                .is_none(),
            "queued command must not execute before COMMIT"
        );

        let reverted = service
            .process_command(
                CommandRequest {
                    query: "REVERT;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(reverted.success);
        assert!(
            reverted
                .message
                .starts_with("transaction reverted: dropped 1 command(s); id '")
        );
        assert_eq!(
            command_transaction_state(&reverted),
            Some(ApiTransactionState::Reverted)
        );
        assert!(
            registry
                .get::<CreateSchema>(
                    &DomainName::parse("default").expect("valid domain"),
                    named::<ModelName>("queued_event"),
                )
                .expect("registry get should succeed")
                .is_none(),
            "reverted command must not persist"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_rejects_begin_inside_begin() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let begin = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(begin.success);
        assert_eq!(
            command_transaction_state(&begin),
            Some(ApiTransactionState::Open)
        );

        let nested = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(!nested.success);
        assert_eq!(nested.message, "transaction is already active");
        assert_eq!(
            command_transaction_state(&nested),
            Some(ApiTransactionState::Open)
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_rejects_domain_and_user_creation_inside_a_transaction() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        for query in [
            "BEGIN; CREATE DOMAIN alpha; COMMIT",
            "BEGIN; CREATE USER alpha WITH PASSWORD 'secret'; COMMIT",
        ] {
            let result = service
                .process_command(
                    CommandRequest {
                        query: query.to_string(),
                        domain: "default".to_string(),
                    },
                    &tx,
                    &mut subscriptions,
                )
                .await;

            assert!(!result.success, "'{query}' must be rejected");
            assert!(
                result.message.contains("cannot be queued in a transaction"),
                "'{query}' produced: {}",
                result.message
            );

            let reverted = service
                .process_command(
                    CommandRequest {
                        query: "REVERT;".to_string(),
                        domain: "default".to_string(),
                    },
                    &tx,
                    &mut subscriptions,
                )
                .await;
            assert!(
                reverted.success,
                "revert must succeed: {}",
                reverted.message
            );
        }

        assert!(
            service
                .inner
                .consensus
                .current_domain(&DomainName::parse("alpha").expect("valid domain"))
                .await
                .is_none(),
            "a rejected CREATE DOMAIN must not reach the control plane"
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_rejects_begin_without_an_existing_domain() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let missing = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "absent".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(!missing.success);
        assert_eq!(missing.message, "domain 'absent' does not exist");
        assert!(!subscriptions.transaction_active());

        let unselected = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: String::new(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(!unselected.success);
        assert_eq!(unselected.message, "no active domain selected");
        assert!(!subscriptions.transaction_active());

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_rejects_statements_selecting_another_domain() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        create_test_domain(&service.inner.consensus, "other").await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let begin = service
            .process_command(
                CommandRequest {
                    query: "BEGIN;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(begin.success, "begin must succeed: {}", begin.message);
        assert_eq!(
            begin
                .transaction
                .as_ref()
                .map(|status| status.domain.as_str()),
            Some("default")
        );

        let foreign = service
            .process_command(
                CommandRequest {
                    query: "CREATE SCHEMA foreign_event ( user_id U32 );".to_string(),
                    domain: "other".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(!foreign.success);
        assert!(
            foreign.message.contains("is bound to domain 'default'"),
            "unexpected message: {}",
            foreign.message
        );
        let transaction = service
            .inner
            .consensus
            .current_transaction(
                subscriptions
                    .transaction_id()
                    .expect("the transaction must stay attached"),
            )
            .await
            .expect("open transaction must remain replicated");
        assert_eq!(transaction.pending_statement_count(), 0);

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn attaching_to_committed_transaction_returns_the_recorded_aggregate() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        create_test_domain(&service.inner.consensus, "attach_results").await;
        let (tx, _rx) = mpsc::channel(16);
        let mut owner = SessionSubscriptions::new();

        let committed = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE SCHEMA notification ( user_id U32 ); COMMIT".to_string(),
                    domain: "attach_results".to_string(),
                },
                &tx,
                &mut owner,
            )
            .await;
        assert!(
            committed.success,
            "commit must succeed: {}",
            committed.message
        );
        let transaction_id = committed
            .transaction
            .as_ref()
            .expect("commit result must carry transaction status")
            .id
            .clone();

        let mut observer = SessionSubscriptions::new();
        let attached = service
            .attach_transaction(
                proto::AttachTransactionRequest { id: transaction_id },
                &mut observer,
            )
            .await;

        assert!(
            !attached.success,
            "finished transaction attach must be terminal"
        );
        assert_eq!(
            attached.transaction.as_ref().map(|status| status.state),
            Some(i32::from(ApiTransactionState::Committed))
        );
        assert!(attached.message.contains("finished with outcome COMMITTED"));
        assert_eq!(attached.results.len(), 1);
        assert_eq!(attached.results[0].message, "quiesce level: DYNAMIC");

        owner.stop_all(&service).await;
        observer.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }
}
