//! A session's staged configuration, from the first queued statement to the replicated commit.
//!
//! Layer: control plane.
//!
//! - **Owns.** The transaction lifecycle, the binding a session holds, statement preflight, the
//!   replicated commit and the quiescence it recovers from.
//! - **Depends on.** Consensus for the replicated transaction and the registry for mutation plans.
//! - **Must not know.** How the models a commit applies are executed.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc as StdArc,
};

use arch_into::ArchInto;
use error_stack::{Report, ResultExt};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{
    ConsensusError, ConsensusTransactionError, DomainPlanningInputs, ReplicatedTransaction,
    TransactionActivity, TransactionApplyingStep, TransactionCommandResult,
    TransactionCommitAdmissionFailure, TransactionCommitAdmissionPlan, TransactionCommitAdvance,
    TransactionDiagnostic, TransactionOutcome, TransactionQueueAdmission, TransactionQueueLimits,
    TransactionQueueRequest, TransactionReportArchive, TransactionScheduleEligibility,
    TransactionState, TransactionStatement, TransactionStatementRequest, TransactionStepEffect,
    TransactionStepResult,
};
use nervix_models::{
    ActualExecutionStepImpact, CanonicalImpactSet, CommandExecutionReference, DomainName,
    DomainSchedule, DomainState, DomainStatus, ExecutionStepImpactReport, ImpactNodeCoverage,
    ImpactPlanningBasis, Model, ModelIndex, OwnershipMoveImpact, RequestedResourceVersion,
    ResourceId, ResourceName, ResourceUploads, Statement, TransactionCommitStepKind,
    TransactionOperationAdmission, TransactionOperationNumber, TransactionPreviewIdentity,
    TransactionResolvedDomainStart, UserName,
};
use nervix_nspl::client_statement::ClientStatement;
use parking_lot::Mutex as ParkingMutex;
use serde::Serialize;
use thiserror::Error;
use tokio::{
    sync::{OwnedMutexGuard, Semaphore, mpsc},
    time::{Duration, sleep},
};
use tonic::Status;
use tracing::{info, warn};

use super::{
    domain_clock::{current_timestamp, subtract_timestamp_duration},
    domain_lifecycle::DomainAlterError,
    model_mutation::{
        LatestResolutionReport, RequestDomainError, append_command_output, command_error,
        command_ok, command_ok_already_existed, parse_request_domain, quiesce_level_message,
    },
    ownership_handoff::{mark_complete_ownership_transitions, planned_ownership_moves},
    schedule_planning::DomainSchedulePlanningSnapshot,
    session_service::SessionServiceImpl,
    subscription::{PendingSessionCommand, SessionSubscriptions},
};
use crate::{
    proto,
    proto::{
        CommandResult, CommandResultKind, Diagnostic, SessionResponse,
        TransactionState as ApiTransactionState, TransactionStatus as ApiTransactionStatus,
    },
    registry::{
        PlannedTransaction, PlannedTransactionStep, PlannedTransactionStepKind, Registry,
        RegistryMutation, TransactionPlanningError, TransactionPlanningSnapshot,
        TransactionScheduleDecision,
    },
    runtime::RuntimeError,
};

pub(in crate::application) const DEFAULT_TRANSACTION_IDLE_TIMEOUT: Duration =
    Duration::from_secs(15 * 60);

pub(in crate::application) const DEFAULT_TRANSACTION_TOMBSTONE_RETENTION: Duration =
    Duration::from_secs(15 * 60);

pub(in crate::application) const DEFAULT_TRANSACTION_MAX_STATEMENTS: usize = 256;

pub(in crate::application) const DEFAULT_TRANSACTION_MAX_SOURCE_BYTES: u64 = 1024 * 1024;

pub(in crate::application) const DEFAULT_TRANSACTION_MAX_OPEN: usize = 1024;

/// Recovery is independent across transactions, but its background work remains bounded per node.
/// One held executor still leaves capacity for unrelated domains and for later candidates.
const TRANSACTION_RECOVERY_CONCURRENCY: usize = 8;

pub(in crate::application) struct TransactionRecovery {
    permits: StdArc<Semaphore>,
    cursor: ParkingMutex<Option<String>>,
}

impl Default for TransactionRecovery {
    fn default() -> Self {
        Self {
            permits: StdArc::new(Semaphore::new(TRANSACTION_RECOVERY_CONCURRENCY)),
            cursor: ParkingMutex::new(None),
        }
    }
}

impl TransactionRecovery {
    /// Returns COMMITTING identities after the last considered identity, wrapping once. Repeated
    /// finite reconciliation passes therefore cannot favor the start of the ordered map.
    fn candidates(&self, transactions: &BTreeMap<String, ReplicatedTransaction>) -> Vec<String> {
        let mut candidates = Vec::new();
        for (id, transaction) in transactions {
            if matches!(transaction.state, TransactionState::Committing(_)) {
                candidates.push(id.clone());
            }
        }
        let cursor = self.cursor.lock().clone();
        let Some(cursor) = cursor else {
            return candidates;
        };
        let start = candidates.partition_point(|id| id <= &cursor);
        candidates.rotate_left(start);
        candidates
    }

    fn considered(&self, id: String) {
        *self.cursor.lock() = Some(id);
    }
}

struct PreparedTransactionAdmission {
    result: TransactionCommandResult,
    report: TransactionReportArchive,
}

struct PreparedTransactionCommit {
    expected_preview: TransactionPreviewIdentity,
    report: TransactionReportArchive,
    plan: TransactionCommitAdmissionPlan,
}

#[derive(Serialize)]
struct TransactionPlanningBasisInput<'a> {
    domain: &'a DomainState,
    models: Vec<&'a Model>,
    resources: Vec<&'a ResourceName>,
    completed_resource_versions: Vec<&'a ResourceId>,
    schedule: Option<&'a DomainSchedule>,
    members: &'a [nervix_models::ClusterNodeName],
    voters: &'a [nervix_models::ClusterNodeName],
    cordoned: &'a [nervix_models::ClusterNodeName],
    live_identities: Vec<&'a nervix_models::ClusterNodeIdentity>,
    placement_candidate_identities: Vec<&'a nervix_models::ClusterNodeIdentity>,
    live_voters: &'a [nervix_models::ClusterNodeName],
    cluster_nodes: &'a [nervix_models::ClusterNodeName],
    replica_count: usize,
    scheduler_mode: &'a str,
}

/// The captured control-plane inputs one planning basis identifies.
struct TransactionPlanningBasisSource<'a> {
    domain: &'a DomainState,
    models: &'a ModelIndex,
    resources: &'a BTreeSet<ResourceName>,
    resource_uploads: &'a ResourceUploads,
    schedule: Option<&'a DomainSchedule>,
    authoritative_inputs: &'a DomainPlanningInputs,
    schedule_inputs: &'a DomainSchedulePlanningSnapshot,
}

fn transaction_planning_basis(
    source: TransactionPlanningBasisSource<'_>,
) -> Result<ImpactPlanningBasis, Report<TransactionPlanningError>> {
    let TransactionPlanningBasisSource {
        domain,
        models,
        resources,
        resource_uploads,
        schedule,
        authoritative_inputs,
        schedule_inputs,
    } = source;
    let mut ordered_models = models.models().collect::<Vec<_>>();
    ordered_models.sort_by_key(|model| model.node_ref());
    let resources = resources.iter().collect();
    let input = TransactionPlanningBasisInput {
        domain,
        models: ordered_models,
        resources,
        completed_resource_versions: resource_uploads.completed_versions().collect(),
        schedule,
        members: authoritative_inputs.topology().members(),
        voters: authoritative_inputs.topology().voters(),
        cordoned: authoritative_inputs.topology().cordoned(),
        live_identities: schedule_inputs.live_identities().iter().collect(),
        placement_candidate_identities: schedule_inputs
            .placement_candidate_identities()
            .iter()
            .collect(),
        live_voters: schedule_inputs.live_voters(),
        cluster_nodes: schedule_inputs.cluster_nodes(),
        replica_count: schedule_inputs.replica_count(),
        scheduler_mode: schedule_inputs.mode_name(),
    };
    let mut encoded = Vec::new();
    ciborium::into_writer(&input, &mut encoded).map_err(|error| {
        Report::new(TransactionPlanningError::PlanningBasisEncoding).attach(error)
    })?;
    Ok(ImpactPlanningBasis::new(*blake3::hash(&encoded).as_bytes()))
}

fn transaction_planning_error_message(error: &Report<TransactionPlanningError>) -> String {
    let context = error.to_string();
    match error.downcast_ref::<String>() {
        Some(message) => format!("{context}: {message}"),
        None => context,
    }
}

fn transaction_commit_error_message(error: &Report<TransactionCommitError>) -> String {
    let context = error
        .downcast_ref::<TransactionPlanningError>()
        .map_or_else(|| error.to_string(), ToString::to_string);
    match error.downcast_ref::<String>() {
        Some(message) => format!("{context}: {message}"),
        None => context,
    }
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
    #[error("transaction '{id}' commit task failed")]
    TaskJoin { id: String },
    #[error("failed to prepare the complete commit plan for transaction '{id}'")]
    PreparePlan { id: String },
    #[error("failed to archive the complete commit report for transaction '{id}'")]
    ArchiveReport { id: String },
    #[error("failed to resolve the domain start at operation {operation} for transaction '{id}'")]
    PrepareDomainStart {
        id: String,
        operation: TransactionOperationNumber,
    },
    #[error("transaction '{id}' cannot start paced domain '{domain}' without a live clock voter")]
    PrepareClockAuthority { id: String, domain: DomainName },
    #[error("transaction '{id}' commit was cancelled because the node's shutdown deadline passed")]
    ShutdownCancelled { id: String },
}

impl TransactionCommitError {
    fn consensus_error(&self) -> Option<&ConsensusError> {
        match self {
            Self::Proposal(ConsensusTransactionError::Consensus(error)) => Some(error),
            _ => None,
        }
    }

    fn planning_input_conflict(&self) -> Option<&str> {
        match self {
            Self::Proposal(ConsensusTransactionError::Mutation(
                nervix_consensus::TransactionMutationError::StepConflict { reason, .. },
            )) => Some(reason),
            _ => None,
        }
    }
}

pub(in crate::application) struct TransactionModelStepContext<'a> {
    pub(in crate::application) transaction: &'a ReplicatedTransaction,
    pub(in crate::application) first_statement: usize,
    pub(in crate::application) planned_step: PlannedTransactionStep,
    pub(in crate::application) inputs: DomainPlanningInputs,
    pub(in crate::application) eligibility: TransactionScheduleEligibility,
    pub(in crate::application) outcome:
        &'a ParkingMutex<Option<Result<ReplicatedTransaction, Report<TransactionCommitError>>>>,
}

struct CapturedTransactionPlan {
    plan: PlannedTransaction,
    inputs: DomainPlanningInputs,
    schedule_inputs: DomainSchedulePlanningSnapshot,
}

enum TransactionApplicationAttempt {
    Retry,
    Completed(Box<ReplicatedTransaction>),
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
    /// Queued model mutations as their statements wrote them. Completion only reads names and
    /// kinds, so the resource versions stay unresolved until `COMMIT` plans them.
    pub(in crate::application) models: Vec<RegistryMutation<RequestedResourceVersion>>,
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
        TransactionState::Open(_) => ReportedOutcome::without_error(ApiTransactionState::Open),
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
        admission: None,
    }
}

fn admitted_command_result(
    admission: &TransactionCommandResult,
    transaction: &ReplicatedTransaction,
) -> CommandResult {
    let mut result = CommandResult {
        success: admission.success,
        message: admission.message.clone(),
        diagnostics: admission
            .diagnostics
            .iter()
            .map(|diagnostic| Diagnostic {
                message: diagnostic.message.clone(),
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        kind: if admission.success {
            i32::from(CommandResultKind::Ok)
        } else {
            i32::from(CommandResultKind::Error)
        },
        already_existed: admission.already_existed,
        transaction_admission: admission
            .admission
            .as_ref()
            .map(api_transaction_operation_admission),
        ..Default::default()
    };
    result.transaction = Some(transaction_status(transaction));
    result
}

fn api_transaction_operation_admission(
    admission: &TransactionOperationAdmission,
) -> proto::TransactionOperationAdmission {
    proto::TransactionOperationAdmission {
        operation: admission.operation.get().arch_into(),
        preview: Some(proto::TransactionPreviewIdentity {
            transaction_id: admission.preview.transaction_id.clone(),
            position: admission.preview.position.accepted_operations().arch_into(),
            planning_basis: admission
                .preview
                .planning_basis
                .fingerprint()
                .to_vec()
                .into(),
        }),
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
        .map(|step| step.impact.planned().pause.level())
        .max();
    let planned_relocations = transaction
        .commit_results()
        .iter()
        .map(|step| step.impact.planned().effects.ownership_moves.len())
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
    for step in transaction.commit_results() {
        if !step.result.success {
            continue;
        }
        let bindings = step.impact.planned().effects.resource_bindings.as_slice();
        for resolution in LatestResolutionReport::Applied.messages(bindings) {
            append_command_output(&mut message, &resolution);
        }
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

fn finished_transaction_attach_result(
    transaction: &ReplicatedTransaction,
) -> Option<CommandResult> {
    let TransactionState::Finished(finished) = &transaction.state else {
        return None;
    };
    let mut recorded = transaction_commit_result(transaction);
    recorded.transaction = None;
    let mut result = command_error(format!(
        "transaction '{}' finished with outcome {}",
        transaction.id,
        finished.outcome.as_str()
    ));
    if !recorded.message.is_empty() {
        result.results.push(recorded);
    }
    result.transaction = Some(transaction_status(transaction));
    Some(result)
}
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
    CommandResult {
        success: step.result.success,
        message: step.result.message.clone(),
        diagnostics: step
            .result
            .diagnostics
            .iter()
            .map(|diagnostic| Diagnostic {
                message: diagnostic.message.clone(),
                span_start: diagnostic.span_start,
                span_end: diagnostic.span_end,
            })
            .collect(),
        kind: if step.result.success {
            i32::from(CommandResultKind::Ok)
        } else {
            i32::from(CommandResultKind::Error)
        },
        already_existed: step.result.already_existed,
        transaction_admission: step
            .result
            .admission
            .as_ref()
            .map(api_transaction_operation_admission),
        ..Default::default()
    }
}

pub(in crate::application) fn is_queueable_transaction_statement(statement: &Statement) -> bool {
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
    pub(in crate::application) fn transaction_activity(&self) -> TransactionActivity {
        TransactionActivity::from_timeout(current_timestamp(), self.inner.transaction_idle_timeout)
    }

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
            && matches!(transaction.state, TransactionState::Open(_))
            && let Err(error) = self
                .inner
                .consensus
                .revert_transaction(
                    id.clone(),
                    subscriptions.user.clone(),
                    self.transaction_activity(),
                )
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
        if let Some(result) = finished_transaction_attach_result(&transaction) {
            return result;
        }

        let transaction = match self
            .inner
            .consensus
            .touch_transaction(
                request.id.clone(),
                subscriptions.user.clone(),
                self.transaction_activity(),
            )
            .await
        {
            Ok(transaction) => transaction,
            Err(error) => return self.transaction_consensus_error_response(error).await,
        };
        if let Some(result) = finished_transaction_attach_result(&transaction) {
            self.inner.transaction_bindings.remove(&request.id);
            if subscriptions.transaction_id() == Some(request.id.as_str()) {
                drop(subscriptions.detach_transaction());
            }
            return result;
        }
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
        subscriptions: &mut SessionSubscriptions,
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
        let queued = TransactionStatementRequest {
            request_reference: command.request_reference,
            expected_position: command
                .expected_transaction_position
                .verified("transaction planning requires a queue position for every append"),
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
        match transaction.queue_admission(&subscriptions.user, &domain, &queued, limits) {
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
                id: id.to_string(),
                owner: subscriptions.user.clone(),
                domain,
                activity: self.transaction_activity(),
                statement: queued,
                report: prepared.report,
                limits,
            })
            .await
        {
            Ok(transaction) => {
                if matches!(transaction.state, TransactionState::Finished(_)) {
                    self.release_session_transaction_binding(subscriptions);
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

    pub(in crate::application) async fn execute_standalone_transaction(
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
            let prepared_admission = match self
                .preflight_transaction_statement(&candidate, &queued)
                .await
            {
                Ok(admission) => admission,
                Err(error) => return command_error(error),
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
                                Err(error) => return command_error(error),
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

        let current = transaction;
        let committing = match &current.state {
            TransactionState::Open(_) => {
                let prepared = match self.prepare_transaction_commit(&current).await {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        let message = transaction_commit_error_message(&error);
                        match self
                            .fail_incomplete_transaction_commit(&current, &owner, &error)
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
                match self
                    .inner
                    .consensus
                    .start_transaction_commit(
                        transaction_id.clone(),
                        owner,
                        self.transaction_activity(),
                        prepared.expected_preview,
                        prepared.report,
                        prepared.plan,
                    )
                    .await
                {
                    Ok(transaction) => transaction,
                    Err(error) => return self.transaction_consensus_error_response(error).await,
                }
            }
            TransactionState::Committing(_) | TransactionState::Finished(_) => current,
        };
        self.pause_transaction_commit_if_armed(&committing).await;
        let finished = if matches!(committing.state, TransactionState::Finished(_)) {
            Ok(committing)
        } else {
            Box::pin(self.execute_replicated_commit(&transaction_id)).await
        };
        match finished {
            Ok(transaction) => standalone_transaction_result(&transaction),
            Err(error) => {
                if let Some(ConsensusError::LeadershipLost { leader_id }) = error
                    .current_context()
                    .consensus_error()
                    .or_else(|| error.downcast_ref::<ConsensusError>())
                {
                    self.not_leader_response("", leader_id.clone()).await
                } else {
                    command_error(format!(
                        "durable command transaction '{transaction_id}' remains applying: {error}"
                    ))
                }
            }
        }
    }

    async fn plan_transaction_statements(
        &self,
        domain: &DomainName,
        statements: &[Statement],
        first_operation_index: usize,
        allow_incomplete_final_model_run: bool,
    ) -> Result<CapturedTransactionPlan, Report<TransactionPlanningError>> {
        let mutates_domain = statements
            .iter()
            .any(Statement::requires_domain_mutation_ownership);
        if mutates_domain && self.inner.runtime.domain_alter_is_active(domain) {
            return Err(Report::new(
                TransactionPlanningError::ConcurrentDomainAlter {
                    domain: domain.clone(),
                },
            ));
        }
        let models = self.inner.registry.transaction_planning_models(domain);
        let control = self
            .inner
            .consensus
            .transaction_control_snapshot(domain)
            .await;
        let schedule_inputs = self
            .capture_domain_schedule_planning_snapshot(&control.planning_inputs)
            .await;
        let domain_state = control.planning_inputs.state().cloned().ok_or_else(|| {
            Report::new(TransactionPlanningError::DomainNotFound {
                domain: domain.clone(),
            })
        })?;
        let resources = control
            .resources
            .next_version_by_resource
            .iter()
            .filter(|counter| counter.domain == *domain)
            .map(|counter| counter.identifier.clone())
            .collect::<BTreeSet<_>>();
        let resource_uploads = control.resources.uploads.in_domain(domain);
        let basis = transaction_planning_basis(TransactionPlanningBasisSource {
            domain: &domain_state,
            models: &models,
            resources: &resources,
            resource_uploads: &resource_uploads,
            schedule: control.planning_inputs.schedule(),
            authoritative_inputs: &control.planning_inputs,
            schedule_inputs: &schedule_inputs,
        })?;
        let snapshot = TransactionPlanningSnapshot {
            domain: domain_state.clone(),
            models,
            resources,
            resource_uploads,
            schedule: control.planning_inputs.schedule().cloned(),
            basis,
        };
        let planning_domain = domain.clone();
        let plan = Registry::plan_transaction(
            snapshot,
            statements,
            first_operation_index,
            allow_incomplete_final_model_run,
            |graph, placement, current, attribution| {
                let prepared = schedule_inputs.prepare(
                    &control.planning_inputs,
                    &planning_domain,
                    graph,
                    placement,
                    current,
                );
                let ownership_moves = planned_ownership_moves(current, prepared.schedule.as_ref())
                    .into_iter()
                    .map(|moved| OwnershipMoveImpact {
                        node: ImpactNodeCoverage::all_executions(moved.entity),
                        source: moved.former_owner,
                        destination: moved.destination,
                        attribution: attribution.clone(),
                    });
                TransactionScheduleDecision {
                    schedule: prepared.schedule,
                    ownership_moves: CanonicalImpactSet::new(ownership_moves),
                }
            },
        )?;
        for step in plan.steps() {
            tokio::task::consume_budget().await;
            let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
                continue;
            };
            let Some(planned) = &model_plan.planned else {
                continue;
            };
            self.validate_changed_model_bindings(domain, domain_state.config.pace, planned)
                .await
                .map_err(|message| {
                    Report::new(TransactionPlanningError::ExternalModelValidation {
                        operation: step.impact.operations().first(),
                    })
                    .attach(message)
                })?;
            self.prepare_planned_domain_udfs(planned)
                .await
                .map_err(|message| {
                    Report::new(TransactionPlanningError::UdfPreparation {
                        operation: step.impact.operations().first(),
                    })
                    .attach(message)
                })?;
        }
        Ok(CapturedTransactionPlan {
            plan,
            inputs: control.planning_inputs,
            schedule_inputs,
        })
    }

    fn transaction_admission_result(
        candidate: &Statement,
        plan: &PlannedTransaction,
        candidate_index: usize,
    ) -> CommandResult {
        let number = TransactionOperationNumber::from_index(candidate_index)
            .assured("a candidate index belongs to the transaction prefix just planned");
        // Planning emits at most one step per statement, and transaction admission bounds the
        // prefix by `transaction_max_statements`.
        let step = plan
            .steps()
            .iter()
            .find(|step| step.impact.operations().contains(number))
            .verified("the ordered plan assigns every accepted operation to one execution step");
        let already_existed = match &step.kind {
            PlannedTransactionStepKind::Models { plan } => plan.no_op_operations.contains(&number),
            PlannedTransactionStepKind::CreateResource {
                already_existed, ..
            } => *already_existed,
            PlannedTransactionStepKind::AlterDomain { .. }
            | PlannedTransactionStepKind::StartDomain { .. }
            | PlannedTransactionStepKind::StopDomain => false,
        };
        let mut result = if already_existed {
            let target = match candidate {
                Statement::Create(create) => format!("model '{}'", create.body.name().as_str()),
                Statement::CreateResource(create) => {
                    format!("resource '{}'", create.body.identifier.as_str())
                }
                _ => "configuration".to_string(),
            };
            command_ok_already_existed(format!("{target} already exists"))
        } else {
            command_ok(String::new())
        };
        append_command_output(
            &mut result.message,
            &quiesce_level_message(step.impact.planned().pause.level()),
        );
        let mut candidate_bindings = Vec::new();
        for binding in step.impact.planned().effects.resource_bindings.as_slice() {
            if binding
                .attribution
                .operations()
                .binary_search(&number)
                .is_ok()
            {
                candidate_bindings.push(binding);
            }
        }
        for message in LatestResolutionReport::Provisional.messages(candidate_bindings) {
            append_command_output(&mut result.message, &message);
        }
        result
    }

    async fn preflight_transaction_statement(
        &self,
        transaction: &ReplicatedTransaction,
        candidate: &TransactionStatementRequest,
    ) -> Result<PreparedTransactionAdmission, String> {
        let mut statements = transaction
            .statements
            .iter()
            .map(|queued| queued.statement.clone())
            .collect::<Vec<_>>();
        statements.push(candidate.statement.clone());
        let captured = self
            .plan_transaction_statements(&transaction.domain, &statements, 0, true)
            .await
            .map_err(|error| {
                format!(
                    "transaction statement failed preflight: {}",
                    transaction_planning_error_message(&error)
                )
            })?;
        let report = captured
            .plan
            .report()
            .map_err(|error| format!("transaction statement failed preflight: {error}"))?;
        let report = TransactionReportArchive::new(transaction.id.clone(), report)
            .map_err(|error| format!("transaction statement failed report archival: {error}"))?;
        let result = Self::transaction_admission_result(
            &candidate.statement,
            &captured.plan,
            candidate.expected_position,
        );
        let mut result = replicated_command_result(&result);
        result.admission = Some(TransactionOperationAdmission {
            operation: TransactionOperationNumber::from_index(candidate.expected_position)
                .assured("an admitted candidate index is below the transaction queue limit"),
            preview: report.identity().clone(),
        });
        Ok(PreparedTransactionAdmission { result, report })
    }

    async fn prepare_transaction_commit(
        &self,
        transaction: &ReplicatedTransaction,
    ) -> Result<PreparedTransactionCommit, Report<TransactionCommitError>> {
        let statements = transaction
            .statements
            .iter()
            .map(|queued| queued.statement.clone())
            .collect::<Vec<_>>();
        let captured = self
            .plan_transaction_statements(&transaction.domain, &statements, 0, false)
            .await
            .change_context(TransactionCommitError::PreparePlan {
                id: transaction.id.clone(),
            })?;
        let report =
            captured
                .plan
                .report()
                .change_context(TransactionCommitError::PreparePlan {
                    id: transaction.id.clone(),
                })?;
        let mut resolved_starts = BTreeMap::new();
        for step in captured.plan.steps() {
            tokio::task::consume_budget().await;
            let PlannedTransactionStepKind::StartDomain { previous } = &step.kind else {
                continue;
            };
            let operation = step.impact.operations().first();
            let statement_index = step.impact.operations().first_index();
            let queued = transaction.statements.get(statement_index).ok_or_else(|| {
                Report::new(TransactionCommitError::InvalidProgress {
                    id: transaction.id.clone(),
                })
            })?;
            let Statement::StartDomain(start) = &queued.statement else {
                return Err(Report::new(TransactionCommitError::InvalidProgress {
                    id: transaction.id.clone(),
                }));
            };
            let authority = if previous.config.pace.is_paced() {
                Some(
                    self.selected_domain_clock_authority(&transaction.domain)
                        .await
                        .ok_or_else(|| {
                            Report::new(TransactionCommitError::PrepareClockAuthority {
                                id: transaction.id.clone(),
                                domain: transaction.domain.clone(),
                            })
                        })?,
                )
            } else {
                None
            };
            let resolved = self
                .resolve_domain_start(&transaction.domain, previous, &start.start)
                .await
                .change_context(TransactionCommitError::PrepareDomainStart {
                    id: transaction.id.clone(),
                    operation,
                })?;
            resolved_starts.insert(
                operation,
                TransactionResolvedDomainStart {
                    start: resolved.concrete_start,
                    clock: previous.config.pace.is_paced().then_some(resolved.clock),
                    authority,
                },
            );
        }
        captured
            .schedule_inputs
            .validate_eligibility(self)
            .await
            .change_context(TransactionCommitError::PreparePlan {
                id: transaction.id.clone(),
            })?;
        let commit_plan = captured
            .plan
            .commit_plan(transaction.id.clone(), &resolved_starts);
        let eligibility = captured.schedule_inputs.transaction_eligibility();
        let commit_plan =
            TransactionCommitAdmissionPlan::capture(commit_plan, captured.inputs, eligibility)
                .change_context(TransactionCommitError::PreparePlan {
                    id: transaction.id.clone(),
                })?;
        let report =
            TransactionReportArchive::new(transaction.id.clone(), report).map_err(|error| {
                Report::new(TransactionCommitError::ArchiveReport {
                    id: transaction.id.clone(),
                })
                .attach(error)
            })?;
        Ok(PreparedTransactionCommit {
            expected_preview: report.identity().clone(),
            report,
            plan: commit_plan,
        })
    }

    async fn fail_incomplete_transaction_commit(
        &self,
        transaction: &ReplicatedTransaction,
        owner: &UserName,
        failure: &Report<TransactionCommitError>,
    ) -> Result<Option<ReplicatedTransaction>, ConsensusTransactionError> {
        let statements = transaction
            .statements
            .iter()
            .map(|queued| queued.statement.clone())
            .collect::<Vec<_>>();
        let captured = match self
            .plan_transaction_statements(&transaction.domain, &statements, 0, true)
            .await
        {
            Ok(captured) => captured,
            Err(_) => return Ok(None),
        };
        let report = match captured.plan.report() {
            Ok(report) => report,
            Err(_) => return Ok(None),
        };
        if report.completeness().is_complete() {
            return Ok(None);
        }
        let planned_operation = failure
            .downcast_ref::<TransactionPlanningError>()
            .and_then(TransactionPlanningError::operation);
        let diagnosed_operation = report
            .completeness()
            .diagnostics()
            .iter()
            .find_map(|diagnostic| diagnostic.operation);
        let first_operation = report
            .execution_steps()
            .first()
            .map(|step| step.operations().first());
        let Some(operation) = planned_operation
            .or(diagnosed_operation)
            .or(first_operation)
        else {
            return Ok(None);
        };
        let report = match TransactionReportArchive::new(transaction.id.clone(), report) {
            Ok(report) => report,
            Err(_) => return Ok(None),
        };
        let expected_preview = transaction
            .latest_preview()
            .cloned()
            .unwrap_or_else(|| report.identity().clone());
        let failed = self
            .inner
            .consensus
            .fail_transaction_commit_admission(TransactionCommitAdmissionFailure {
                id: transaction.id.clone(),
                owner: owner.clone(),
                activity: self.transaction_activity(),
                expected_preview,
                report,
                inputs: captured.inputs,
                operation,
                error: transaction_commit_error_message(failure),
            })
            .await?;
        Ok(Some(failed))
    }

    fn transaction_registry_mutation(
        statement: &Statement,
    ) -> RegistryMutation<RequestedResourceVersion> {
        RegistryMutation::try_from(statement)
            .assured("transaction registry mutation requires a model mutation")
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
            .revert_transaction(
                id.clone(),
                subscriptions.user.clone(),
                self.transaction_activity(),
            )
            .await
        {
            Ok(transaction) => {
                if !matches!(
                    transaction.finished_outcome(),
                    Some(TransactionOutcome::Reverted)
                ) {
                    self.release_session_transaction_binding(subscriptions);
                    return transaction_commit_result(&transaction);
                }
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
        let Some(current) = self.inner.consensus.current_transaction(&id).await else {
            return command_error(format!("transaction '{id}' is unknown"));
        };
        let started = match &current.state {
            TransactionState::Open(_) => match self.prepare_transaction_commit(&current).await {
                Ok(prepared) => {
                    match self
                        .inner
                        .consensus
                        .start_transaction_commit(
                            id.clone(),
                            subscriptions.user.clone(),
                            self.transaction_activity(),
                            prepared.expected_preview,
                            prepared.report,
                            prepared.plan,
                        )
                        .await
                    {
                        Ok(transaction) => transaction,
                        Err(error) => {
                            return self.transaction_consensus_error_response(error).await;
                        }
                    }
                }
                Err(error) => {
                    let message = transaction_commit_error_message(&error);
                    match self
                        .fail_incomplete_transaction_commit(&current, &subscriptions.user, &error)
                        .await
                    {
                        Ok(Some(transaction)) => transaction,
                        Ok(None) => return command_error(message),
                        Err(error) => {
                            return self.transaction_consensus_error_response(error).await;
                        }
                    }
                }
            },
            TransactionState::Committing(_) => current,
            TransactionState::Finished(_) => {
                self.release_session_transaction_binding(subscriptions);
                return transaction_commit_result(&current);
            }
        };
        let finished = if matches!(started.state, TransactionState::Finished(_)) {
            Ok(started)
        } else {
            self.pause_transaction_commit_if_armed(&started).await;
            if started.statements.is_empty() {
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
                let commit = self
                    .inner
                    .service_tasks
                    .spawn(async move { service.execute_replicated_commit(&commit_id).await })
                    .await;
                match commit {
                    Ok(Some(result)) => result,
                    Ok(None) => Err(Report::new(TransactionCommitError::ShutdownCancelled {
                        id: id.clone(),
                    })),
                    Err(error) => Err(Report::new(error)
                        .change_context(TransactionCommitError::TaskJoin { id: id.clone() })),
                }
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

    pub(in crate::application) async fn execute_replicated_commit(
        &self,
        id: &str,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let execution = self
            .inner
            .transaction_executions
            .entry(id.to_string())
            .or_insert_with(|| StdArc::new(tokio::sync::Mutex::new(())))
            .clone();
        let execution_guard = execution.lock_owned().await;
        self.execute_replicated_commit_locked(id, execution_guard)
            .await
    }

    async fn execute_replicated_commit_locked(
        &self,
        id: &str,
        _execution_guard: OwnedMutexGuard<()>,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let transaction = self
            .inner
            .consensus
            .current_transaction(id)
            .await
            .ok_or_else(|| {
                Report::new(TransactionCommitError::UnknownTransaction { id: id.to_string() })
            })?;
        let result = if matches!(transaction.state, TransactionState::Finished(_)) {
            Ok(transaction)
        } else {
            Box::pin(self.run_replicated_commit(id)).await
        };
        if result
            .as_ref()
            .is_ok_and(|transaction| matches!(transaction.state, TransactionState::Finished(_)))
        {
            self.inner.transaction_executions.remove(id);
        }
        result
    }

    async fn run_replicated_commit(
        &self,
        id: &str,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        // Each commit phase has its own state machine. Poll them indirectly so this recovery loop
        // does not embed every phase's future state in its own debug poll frame.
        self.inner
            .registry
            .synchronize_cluster_schedule(&Box::pin(self.inner.consensus.current_schedule()).await)
            .change_context(TransactionCommitError::SynchronizeRegistry { id: id.to_string() })?;
        loop {
            tokio::task::consume_budget().await;
            let leader_id = Box::pin(self.inner.consensus.current_leader()).await;
            if leader_id.as_ref() != Some(self.inner.consensus.local_node_id()) {
                return Err(Report::new(TransactionCommitError::Proposal(
                    ConsensusTransactionError::Consensus(ConsensusError::LeadershipLost {
                        leader_id,
                    }),
                )));
            }
            let transaction = Box::pin(self.inner.consensus.current_transaction(id))
                .await
                .ok_or_else(|| {
                    Report::new(TransactionCommitError::UnknownTransaction { id: id.to_string() })
                })?;
            let progress = match &transaction.state {
                TransactionState::Committing(progress) => progress,
                TransactionState::Finished(_) => return Ok(transaction),
                TransactionState::Open(_) => {
                    return Err(Report::new(TransactionCommitError::TransactionOpen {
                        id: id.to_string(),
                    }));
                }
            };
            if let Some(applying) = progress.applying.clone() {
                match Box::pin(self.complete_transaction_application(&transaction, &applying))
                    .await?
                {
                    TransactionApplicationAttempt::Retry => continue,
                    TransactionApplicationAttempt::Completed(completed) => {
                        if matches!(completed.state, TransactionState::Finished(_)) {
                            return Ok(*completed);
                        }
                        continue;
                    }
                }
            }
            let first_statement = progress.next_statement;
            let Some(_) = transaction.statements.get(first_statement) else {
                return Box::pin(
                    self.inner
                        .consensus
                        .finish_empty_transaction_commit(id.to_string(), current_timestamp()),
                )
                .await
                .map_err(|error| Report::new(TransactionCommitError::Proposal(error)));
            };
            Box::pin(self.recover_transaction_quiescence(&transaction, first_statement)).await?;

            let plan_step_index = progress.results.len();
            let stored_step = Box::pin(
                self.inner
                    .consensus
                    .current_transaction_commit_step(id, plan_step_index),
            )
            .await
            .map_err(|_| {
                Report::new(TransactionCommitError::InvalidProgress {
                    id: transaction.id.clone(),
                })
            })?;
            let stored_impact = stored_step.decision.impact.clone();
            let admitted_kind = stored_step.decision.kind.clone();
            let previous = stored_step.inputs.state().cloned().ok_or_else(|| {
                Report::new(TransactionCommitError::InvalidProgress {
                    id: transaction.id.clone(),
                })
            })?;
            let expected_schedule = stored_step.inputs.schedule().cloned();
            let current_models = self
                .inner
                .registry
                .transaction_planning_models(&transaction.domain);
            let planned_step = match Registry::restore_transaction_commit_step(
                &transaction.domain,
                current_models,
                previous,
                expected_schedule,
                stored_step.decision,
            ) {
                Ok(planned_step) => planned_step,
                Err(error) => {
                    let message = format!(
                        "transaction step could not restore its admitted plan: {}",
                        transaction_planning_error_message(&error)
                    );
                    let advanced = Box::pin(self.record_transaction_step(
                        &transaction,
                        stored_impact,
                        command_error(message),
                        None,
                    ))
                    .await?;
                    return Box::pin(
                        self.record_transaction_application_completion(&advanced, None),
                    )
                    .await;
                }
            };
            let planned_range = planned_step.impact.operations();
            if planned_range.first_index() != first_statement {
                return Err(Report::new(TransactionCommitError::InvalidProgress {
                    id: transaction.id.clone(),
                }));
            }

            if matches!(planned_step.kind, PlannedTransactionStepKind::Models { .. }) {
                let domain = transaction.domain.clone();
                let statements = transaction
                    .statements
                    .get(planned_range.first_index()..planned_range.end_index())
                    .verified("the planned model run is a range of this transaction");
                let sources = statements
                    .iter()
                    .map(|queued| queued.source.as_str())
                    .collect::<Vec<_>>();
                let statements = statements
                    .iter()
                    .map(|queued| queued.statement.clone())
                    .collect::<Vec<_>>();
                let outcome = ParkingMutex::new(None);
                let planned_impact = planned_step.impact.clone();
                let result = Box::pin(self.process_model_mutation_batch_with_transaction(
                    statements,
                    &sources.join("; "),
                    domain.as_str(),
                    Some(TransactionModelStepContext {
                        transaction: &transaction,
                        first_statement,
                        planned_step,
                        inputs: stored_step.inputs,
                        eligibility: stored_step.eligibility,
                        outcome: &outcome,
                    }),
                ))
                .await;
                let recorded = outcome.lock().take();
                let advanced = match recorded {
                    Some(Ok(transaction)) => transaction,
                    Some(Err(error)) => match error.current_context().planning_input_conflict() {
                        Some(reason) => {
                            Box::pin(self.record_transaction_step(
                                &transaction,
                                planned_impact,
                                command_error(format!(
                                    "transaction planning inputs changed: {reason}"
                                )),
                                None,
                            ))
                            .await?
                        }
                        None => return Err(error),
                    },
                    None if result.kind == i32::from(CommandResultKind::NotLeader) => {
                        return Err(Report::new(TransactionCommitError::Proposal(
                            ConsensusTransactionError::Consensus(ConsensusError::LeadershipLost {
                                leader_id: Box::pin(self.inner.consensus.current_leader()).await,
                            }),
                        )));
                    }
                    None if !result.success => {
                        Box::pin(self.record_transaction_step(
                            &transaction,
                            planned_impact,
                            result,
                            None,
                        ))
                        .await?
                    }
                    None => {
                        return Err(Report::new(TransactionCommitError::MissingProgress {
                            id: id.to_string(),
                        }));
                    }
                };
                if matches!(advanced.state, TransactionState::Finished(_)) {
                    return Ok(advanced);
                }
                continue;
            }

            let advanced = Box::pin(self.execute_transaction_configuration_step(
                &transaction,
                first_statement,
                planned_step,
                stored_step.inputs,
                stored_step.eligibility,
                admitted_kind,
            ))
            .await?;
            if matches!(advanced.state, TransactionState::Finished(_)) {
                return Ok(advanced);
            }
        }
    }

    async fn complete_transaction_application(
        &self,
        transaction: &ReplicatedTransaction,
        applying: &TransactionApplyingStep,
    ) -> Result<TransactionApplicationAttempt, Report<TransactionCommitError>> {
        let application_failure = if !applying.result.result.success {
            None
        } else {
            match &applying.effect {
                Some(TransactionStepEffect::CreateResourceCatalog { .. }) | None => {
                    if self.wait_for_authoritative_visibility().await.is_err() {
                        sleep(Duration::from_millis(100)).await;
                        return Ok(TransactionApplicationAttempt::Retry);
                    }
                    None
                }
                Some(_) => match self
                    .wait_for_runtime_revision(applying.effect_revision)
                    .await
                {
                    Ok(()) => None,
                    Err(error)
                        if matches!(
                            error.current_context(),
                            RuntimeError::RuntimeRevisionPreparation { .. }
                                | RuntimeError::RuntimeRevisionReadiness { .. }
                        ) =>
                    {
                        match self.apply_current_cluster_state().await {
                            Ok(()) => {
                                if self
                                    .wait_for_runtime_revision(applying.effect_revision)
                                    .await
                                    .is_err()
                                {
                                    sleep(Duration::from_millis(100)).await;
                                    return Ok(TransactionApplicationAttempt::Retry);
                                }
                                None
                            }
                            Err(
                                RuntimeError::RuntimeRevisionPreparation { .. }
                                | RuntimeError::RuntimeRevisionReadiness { .. },
                            ) => {
                                sleep(Duration::from_millis(100)).await;
                                return Ok(TransactionApplicationAttempt::Retry);
                            }
                            Err(error) => Some(format!(
                                "transaction '{}' committed the effect beginning at statement {}, \
                                 but it failed to become usable: {error}",
                                transaction.id,
                                applying.result.operation_range().first()
                            )),
                        }
                    }
                    Err(error) => Some(format!(
                        "transaction '{}' committed the effect beginning at statement {}, but it \
                         failed to become usable: {error}",
                        transaction.id,
                        applying.result.operation_range().first()
                    )),
                },
            }
        };
        let completed = self
            .record_transaction_application_completion(transaction, application_failure)
            .await?;

        Ok(TransactionApplicationAttempt::Completed(Box::new(
            completed,
        )))
    }

    pub(in crate::application) async fn record_transaction_application_completion(
        &self,
        transaction: &ReplicatedTransaction,
        application_failure: Option<String>,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let applying = match &transaction.state {
            TransactionState::Committing(progress) => progress.applying.as_ref(),
            TransactionState::Open(_) | TransactionState::Finished(_) => None,
        }
        .ok_or_else(|| {
            Report::new(TransactionCommitError::InvalidProgress {
                id: transaction.id.clone(),
            })
        })?;
        let completed = self
            .inner
            .consensus
            .complete_transaction_application(
                transaction.id.clone(),
                applying.result.first_statement(),
                current_timestamp(),
                application_failure,
            )
            .await
            .map_err(|error| Report::new(TransactionCommitError::Proposal(error)))?;

        if let TransactionState::Finished(finished) = &completed.state {
            loop {
                tokio::task::consume_budget().await;
                if self
                    .wait_for_authoritative_revision(finished.outcome_revision)
                    .await
                    .is_ok()
                {
                    break;
                }
                if self.inner.consensus.current_leader().await.as_ref()
                    != Some(self.inner.consensus.local_node_id())
                {
                    return Err(Report::new(TransactionCommitError::Proposal(
                        ConsensusTransactionError::Consensus(ConsensusError::LeadershipLost {
                            leader_id: self.inner.consensus.current_leader().await,
                        }),
                    )));
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(completed)
    }

    pub(in crate::application) async fn pause_transaction_commit_if_armed(
        &self,
        _transaction: &ReplicatedTransaction,
    ) {
        #[cfg(feature = "testing")]
        if let TransactionState::Committing(_) = _transaction.state {
            let completed_statements = match &_transaction.state {
                TransactionState::Committing(progress) => match &progress.applying {
                    Some(applying) => applying.next_statement,
                    None => progress.next_statement,
                },
                TransactionState::Open(_) | TransactionState::Finished(_) => 0,
            };
            self.inner
                .runtime
                .pause_transaction_commit_after_progress_if_armed(
                    self.inner.consensus.local_node_id(),
                    &_transaction.domain,
                    completed_statements,
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
                .first_statement()
                .checked_add(result.statement_count())
                .and_then(|end| transaction.statements.get(result.first_statement()..end))
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
            self.resume_domain_after_alter(domain, transaction.domain_mutation())
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
        mut impact: ExecutionStepImpactReport,
        result: CommandResult,
        effect: Option<TransactionStepEffect>,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let operations = impact.operations();
        let first_statement = operations.first_index();
        let statement_count = operations.operation_count().get();
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
        let actual_effects = if result.success {
            impact.planned().effects.clone()
        } else {
            Default::default()
        };
        *impact.actual_mut() = ActualExecutionStepImpact {
            outcome: nervix_models::ExecutionStepOutcome::Applying,
            quiescence: Vec::new(),
            effects: actual_effects,
        };
        let mut replicated_result = replicated_command_result(&result);
        if statement_count == 1 {
            replicated_result.admission = transaction
                .statements
                .get(first_statement)
                .and_then(|statement| statement.admission.admission.clone());
        }
        self.inner
            .consensus
            .advance_transaction_commit(TransactionCommitAdvance {
                id: transaction.id.clone(),
                expected_next_statement: first_statement,
                next_statement,
                at: current_timestamp(),
                result: TransactionStepResult {
                    impact,
                    result: replicated_result,
                },
                effect,
                completion,
            })
            .await
            .map_err(|error| Report::new(TransactionCommitError::Proposal(error)))
    }

    async fn record_transaction_planning_conflict(
        &self,
        transaction: &ReplicatedTransaction,
        impact: ExecutionStepImpactReport,
        reason: impl std::fmt::Display,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        let applying = self
            .record_transaction_step(
                transaction,
                impact,
                command_error(format!("transaction planning inputs changed: {reason}")),
                None,
            )
            .await?;
        self.record_transaction_application_completion(&applying, None)
            .await
    }

    async fn execute_transaction_configuration_step(
        &self,
        transaction: &ReplicatedTransaction,
        statement_index: usize,
        planned_step: PlannedTransactionStep,
        inputs: DomainPlanningInputs,
        eligibility: TransactionScheduleEligibility,
        admitted_kind: TransactionCommitStepKind,
    ) -> Result<ReplicatedTransaction, Report<TransactionCommitError>> {
        transaction.statements.get(statement_index).verified(
            "the caller checked this index against the same statement list before dispatching the \
             step",
        );
        let impact = planned_step.impact;
        let planned_kind = planned_step.kind;
        let resolved_start = match admitted_kind {
            TransactionCommitStepKind::StartDomain { resolved, .. } => Some(resolved),
            _ => None,
        };
        let mut ownership_handoff = None;
        let domain_id = &transaction.domain;
        let _alter_guard = if let PlannedTransactionStepKind::AlterDomain { .. } = &planned_kind {
            let Some(guard) = self.inner.runtime.try_begin_domain_alter(domain_id) else {
                return self
                    .record_transaction_step(
                        transaction,
                        impact,
                        command_error(
                            DomainAlterError::ConcurrentAlter {
                                domain: domain_id.clone(),
                            }
                            .to_string(),
                        ),
                        None,
                    )
                    .await;
            };
            Some(guard)
        } else {
            None
        };
        if matches!(planned_kind, PlannedTransactionStepKind::AlterDomain { .. }) {
            if let Err(error) = self.validate_domain_planning_inputs(&inputs).await {
                return self
                    .record_transaction_planning_conflict(transaction, impact, error)
                    .await;
            }
            if let Err(error) = self
                .validate_transaction_schedule_eligibility(&eligibility)
                .await
            {
                return self
                    .record_transaction_planning_conflict(transaction, impact, error)
                    .await;
            }
        }
        let (result, effect) = match planned_kind {
            PlannedTransactionStepKind::AlterDomain { plan } => {
                let placement = plan.next.config.placement;
                let quiesce_level = impact.planned().pause.level();
                let relocations = impact.planned().effects.ownership_moves.len();
                if plan.previous.config.placement == placement {
                    (
                        command_ok(format!(
                            "domain '{}' placement is already {}; {}\nplanned relocations: 0",
                            domain_id.as_str(),
                            placement.as_ref(),
                            quiesce_level_message(quiesce_level)
                        )),
                        None,
                    )
                } else {
                    let mut schedule = plan.schedule;
                    let ownership_gate = plan.ownership_gate;
                    if let Some(schedule) = schedule.as_mut() {
                        mark_complete_ownership_transitions(
                            plan.expected_schedule.as_ref(),
                            schedule,
                        );
                    }
                    let handoff = if relocations > 0 {
                        self.begin_planned_ownership_handoff_with_exact_gate(
                            domain_id,
                            plan.expected_schedule.as_ref(),
                            schedule.as_ref(),
                            &ownership_gate,
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
                                    placement.as_ref(),
                                    quiesce_level_message(quiesce_level)
                                )),
                                Some(TransactionStepEffect::PutDomainAndSchedule {
                                    inputs: Box::new(inputs),
                                    domain: Box::new(plan.next),
                                    schedule: schedule.map(Box::new),
                                }),
                            )
                        }
                        Err(error) => (command_error(error.to_string()), None),
                    }
                }
            }
            PlannedTransactionStepKind::CreateResource {
                resource,
                already_existed,
            } => {
                if already_existed {
                    (
                        command_ok_already_existed(format!(
                            "resource '{}' already exists",
                            resource.as_str()
                        )),
                        None,
                    )
                } else {
                    (
                        command_ok(format!("created resource '{}'", resource.as_str())),
                        Some(TransactionStepEffect::CreateResourceCatalog {
                            inputs: Box::new(inputs),
                            identifier: resource,
                        }),
                    )
                }
            }
            PlannedTransactionStepKind::StartDomain { .. } => {
                let resolved = resolved_start
                    .verified("a restored START step retains its exact admitted clock decision");
                (
                    command_ok(format!("starting domain '{}'", domain_id.as_str())),
                    Some(TransactionStepEffect::StartDomain {
                        inputs: Box::new(inputs),
                        start: resolved.start,
                        clock: resolved.clock,
                        authority: resolved.authority,
                    }),
                )
            }
            PlannedTransactionStepKind::StopDomain => (
                command_ok(format!("stopped domain '{}'", domain_id.as_str())),
                Some(TransactionStepEffect::StopDomain {
                    inputs: Box::new(inputs),
                }),
            ),
            PlannedTransactionStepKind::Models { .. } => {
                return Err(Report::new(TransactionCommitError::InvalidProgress {
                    id: transaction.id.clone(),
                }));
            }
        };

        let succeeded = result.success;
        let planned_impact = impact.clone();
        let advanced = match self
            .record_transaction_step(transaction, impact, result, effect)
            .await
        {
            Ok(advanced) => advanced,
            Err(error) => {
                if let Some(handoff) = ownership_handoff.take() {
                    self.abort_planned_ownership_handoff(domain_id, handoff)
                        .await;
                }
                let Some(reason) = error.current_context().planning_input_conflict() else {
                    return Err(error);
                };
                return self
                    .record_transaction_planning_conflict(transaction, planned_impact, reason)
                    .await;
            }
        };
        self.pause_transaction_commit_if_armed(&advanced).await;
        let mut application_failure = None;
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
                    application_failure = Some(format!(
                        "transaction '{}' committed step {}, but ownership activation did not \
                         complete: {error}",
                        transaction.id,
                        statement_index
                            .checked_add(1)
                            .assured("the index names a statement of a transaction held in memory")
                    ));
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
            if let Some(error) = activation_error
                && !matches!(
                    error,
                    RuntimeError::RuntimeRevisionPreparation { .. }
                        | RuntimeError::RuntimeRevisionReadiness { .. }
                )
            {
                application_failure = Some(format!(
                    "transaction '{}' committed step {}, but runtime activation did not complete: \
                     {error}",
                    transaction.id,
                    statement_index
                        .checked_add(1)
                        .assured("the index names a statement of a transaction held in memory")
                ));
            }
        }
        if let Some(handoff) = ownership_handoff {
            self.abort_planned_ownership_handoff(domain_id, handoff)
                .await;
        }
        let runtime_revision = match &advanced.state {
            TransactionState::Committing(progress) => match &progress.applying {
                Some(applying)
                    if matches!(
                        applying.effect,
                        Some(TransactionStepEffect::CreateResourceCatalog { .. }) | None
                    ) =>
                {
                    None
                }
                Some(applying) => Some(applying.effect_revision),
                None => None,
            },
            TransactionState::Open(_) | TransactionState::Finished(_) => None,
        };
        if succeeded
            && application_failure.is_none()
            && let Some(runtime_revision) = runtime_revision
            && self
                .wait_for_runtime_revision(runtime_revision)
                .await
                .is_err()
        {
            return Ok(advanced);
        }
        self.record_transaction_application_completion(&advanced, application_failure)
            .await
    }

    fn schedule_transaction_recovery(
        &self,
        transactions: &BTreeMap<String, ReplicatedTransaction>,
    ) {
        for id in self.inner.transaction_recovery.candidates(transactions) {
            let Ok(permit) = self
                .inner
                .transaction_recovery
                .permits
                .clone()
                .try_acquire_owned()
            else {
                break;
            };
            let execution = self
                .inner
                .transaction_executions
                .entry(id.clone())
                .or_insert_with(|| StdArc::new(tokio::sync::Mutex::new(())))
                .clone();
            let execution_guard = execution.try_lock_owned();
            self.inner.transaction_recovery.considered(id.clone());
            let Ok(execution_guard) = execution_guard else {
                continue;
            };
            let service = self.clone();
            self.inner.service_tasks.spawn(async move {
                let _recovery_permit = permit;
                if let Err(error) = service
                    .execute_replicated_commit_locked(&id, execution_guard)
                    .await
                {
                    warn!(
                        transaction_id = id,
                        error = %error,
                        "failed to resume replicated NSPL commit"
                    );
                }
            });
        }
    }

    pub(in crate::application) async fn reconcile_transactions_once(&self) {
        if self.inner.consensus.current_leader().await.as_ref()
            != Some(self.inner.consensus.local_node_id())
        {
            return;
        }
        let now = current_timestamp();
        let finished_before =
            subtract_timestamp_duration(now, self.inner.transaction_tombstone_retention);
        let command_retention = self
            .inner
            .transaction_tombstone_retention
            .max(DEFAULT_TRANSACTION_TOMBSTONE_RETENTION);
        let command_finished_before = subtract_timestamp_duration(now, command_retention);
        self.reconcile_persistent_commands(command_finished_before, now)
            .await;
        let transactions = self.inner.consensus.current_transactions().await;
        let mut tombstone_removal_required = false;

        for transaction in transactions.values() {
            tokio::task::consume_budget().await;
            match &transaction.state {
                // A leader-local binding only routes commands. Keeping a socket open and reading
                // transaction state do not renew this durable administrative deadline.
                TransactionState::Open(activity) if activity.is_inactive_at(now) => {
                    match self
                        .inner
                        .consensus
                        .expire_transaction(transaction.id.clone(), now)
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
                                "expired inactive NSPL transaction"
                            );
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(
                                transaction_id = transaction.id,
                                error = %error,
                                "failed to expire inactive NSPL transaction"
                            );
                        }
                    }
                }
                TransactionState::Committing(_) => {}
                TransactionState::Finished(finished) => {
                    self.inner.transaction_bindings.remove(&transaction.id);
                    if finished.finished_at <= finished_before {
                        tombstone_removal_required = true;
                    }
                }
                TransactionState::Open(_) => {}
            }
        }
        if tombstone_removal_required
            && let Err(error) = self
                .inner
                .consensus
                .remove_finished_transactions(finished_before)
                .await
        {
            warn!(error = %error, "failed to remove expired transaction tombstones");
        }
        self.schedule_transaction_recovery(&transactions);
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
                        .signed_duration_since(*transaction.last_activity_at().as_datetime())
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
#[path = "transaction/tests.rs"]
mod tests;
