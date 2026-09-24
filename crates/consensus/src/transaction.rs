use std::time::Duration;

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::OptionExt as _;
#[cfg(test)]
use meticulous::ResultExt as _;
use nervix_models::{
    ActualExecutionStepImpact, ClusterNodeIdentity, CommandExecutionReference, DomainClockState,
    DomainName, DomainSchedule, DomainStartPoint, DomainState, ExecutionStepImpactReport,
    ExecutionStepOutcome, ImpactDiagnostic, ImpactDiagnosticKind, ResourceName, Statement,
    Timestamp, TransactionOperationAdmission, TransactionOperationNumber,
    TransactionOperationRange, TransactionPreviewIdentity, UserName,
};
pub use nervix_models::{
    TransactionCommitPlan, TransactionCommitPlanHeader, TransactionCommitPlanStep,
    TransactionCommitStepKind, TransactionEntityGatePlan, TransactionModelTransition,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;
use thiserror::Error;

use crate::{
    DiagnosticSpan, DomainMutationLease, DomainMutationOwner, DomainPlanningInputs,
    TransactionReportArchive,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionStatement {
    pub request: TransactionStatementRequest,
    pub admission: TransactionCommandResult,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionStatementRequest {
    pub request_reference: CommandExecutionReference,
    pub expected_position: usize,
    pub source: String,
    pub statement: Statement,
}

impl TransactionStatement {
    pub fn admitted(
        request: TransactionStatementRequest,
        admission: TransactionCommandResult,
    ) -> Self {
        Self { request, admission }
    }

    pub fn source_bytes(&self) -> u64 {
        self.request.source_bytes()
    }

    #[cfg(test)]
    pub(crate) fn test_admitted(request: TransactionStatementRequest) -> Self {
        Self::admitted(
            request,
            TransactionCommandResult {
                success: true,
                message: "admitted".to_string(),
                diagnostics: Vec::new(),
                already_existed: false,
                admission: None,
            },
        )
    }
}

impl TransactionStatementRequest {
    pub fn source_bytes(&self) -> u64 {
        self.source.len().arch_into()
    }
}

impl std::ops::Deref for TransactionStatement {
    type Target = TransactionStatementRequest;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

/// The replicated admission limits a queued statement is checked against. They travel together
/// because every admission check applies both.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct TransactionQueueLimits {
    pub max_statements: usize,
    pub max_source_bytes: u64,
}

/// Everything one consensus proposal needs to admit a transaction statement and its preview.
#[derive(Debug)]
pub struct TransactionQueueRequest {
    pub id: String,
    pub owner: UserName,
    pub domain: DomainName,
    pub activity: TransactionActivity,
    pub statement: TransactionStatement,
    pub report: TransactionReportArchive,
    pub limits: TransactionQueueLimits,
}

/// A complete, side-effect-free planning failure proposed against one identified preview.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionCommitAdmissionFailure {
    pub id: String,
    pub owner: UserName,
    pub activity: TransactionActivity,
    pub expected_preview: TransactionPreviewIdentity,
    pub report: TransactionReportArchive,
    pub inputs: DomainPlanningInputs,
    pub operation: TransactionOperationNumber,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionQueueAdmission {
    New,
    Existing(TransactionCommandResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionQueueDecision {
    Added,
    Existing,
    Expired,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionDiagnostic {
    pub message: String,
    /// Absent when the problem has no location in the statement's source.
    pub span: Option<DiagnosticSpan>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionCommandResult {
    pub success: bool,
    pub message: String,
    pub diagnostics: Vec<TransactionDiagnostic>,
    pub already_existed: bool,
    pub admission: Option<TransactionOperationAdmission>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionStepResult {
    pub impact: ExecutionStepImpactReport,
    pub result: TransactionCommandResult,
}

impl TransactionStepResult {
    pub fn operation_range(&self) -> TransactionOperationRange {
        self.impact.operations()
    }

    pub fn first_statement(&self) -> usize {
        self.operation_range().first_index()
    }

    pub fn statement_count(&self) -> usize {
        self.operation_range().operation_count().get()
    }
}

#[cfg(test)]
pub(crate) fn test_commit_plan(
    transaction_id: &str,
    operation_count: usize,
) -> TransactionCommitPlan {
    let domain = DomainName::parse("tenant")
        .assured("the test transaction domain is an identifier-shaped literal");
    let report = crate::transaction_report::test_report(&domain, operation_count);
    let steps = report
        .execution_steps()
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, impact)| {
            let resource = format!("resource_{index}");
            TransactionCommitPlanStep {
                impact,
                kind: TransactionCommitStepKind::CreateResource {
                    resource: ResourceName::parse(&resource)
                        .assured("the bounded test index produces an identifier-shaped resource"),
                    already_existed: false,
                },
            }
        })
        .collect();
    TransactionCommitPlan {
        preview: TransactionPreviewIdentity {
            transaction_id: transaction_id.to_string(),
            position: nervix_models::TransactionPosition::new(operation_count),
            planning_basis: nervix_models::ImpactPlanningBasis::new([1; 32]),
        },
        steps,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionCommitAdvance {
    pub id: String,
    pub expected_next_statement: usize,
    pub next_statement: usize,
    pub at: Timestamp,
    pub result: TransactionStepResult,
    pub effect: Option<TransactionStepEffect>,
    pub completion: Option<TransactionOutcome>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionCommitProgress {
    /// The latest actual-UTC observation associated with administrative commit progress.
    pub last_activity_at: Timestamp,
    /// The first statement whose application has not completed yet.
    pub next_statement: usize,
    /// Results whose authoritative effects and application obligations both completed.
    pub results: Vec<TransactionStepResult>,
    /// A step whose authoritative effect is durable but whose application is still in progress.
    pub applying: Option<TransactionApplyingStep>,
    /// Authority retained from commit admission through the final application acknowledgement.
    pub domain_mutation: Option<DomainMutationLease>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionApplyingStep {
    pub effect_revision: u64,
    pub next_statement: usize,
    pub result: TransactionStepResult,
    pub effect: Option<TransactionStepEffect>,
    pub completion: Option<TransactionOutcome>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionOutcome {
    Committed,
    Failed {
        failing_step: usize,
        error: String,
    },
    #[strum(serialize = "FAILED")]
    PlanningInputsChanged {
        failing_step: usize,
        error: String,
    },
    Reverted,
    Expired,
}

impl TransactionOutcome {
    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct FinishedTransaction {
    pub outcome: TransactionOutcome,
    /// The exact Raft revision that durably recorded this terminal outcome.
    pub outcome_revision: u64,
    pub finished_at: Timestamp,
    pub results: Vec<TransactionStepResult>,
}

/// The durable actual-UTC activity and inclusive inactivity boundary of an OPEN transaction.
///
/// The boundary is fixed when activity is admitted, so a restart or a different domain clock
/// cannot reinterpret the timeout. A backward wall-clock adjustment cannot move either value
/// backward; a later actual-UTC observation at or beyond `inactivity_deadline` expires the
/// transaction before that observation can renew it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct TransactionActivity {
    last_activity_at: Timestamp,
    inactivity_deadline: Timestamp,
}

impl TransactionActivity {
    pub fn from_timeout(last_activity_at: Timestamp, timeout: Duration) -> Self {
        let inactivity_deadline = match last_activity_at.checked_add(timeout) {
            Ok(deadline) => deadline,
            // A timeout beyond the vocabulary's final actual-UTC instant cannot become due inside
            // that vocabulary. Clamping to the endpoint is therefore the timeout's meaning.
            Err(_) => Timestamp::from_unix_nanos(i64::MAX),
        };
        Self {
            last_activity_at,
            inactivity_deadline,
        }
    }

    pub const fn last_activity_at(self) -> Timestamp {
        self.last_activity_at
    }

    pub const fn inactivity_deadline(self) -> Timestamp {
        self.inactivity_deadline
    }

    pub fn is_inactive_at(self, observed_at: Timestamp) -> bool {
        observed_at >= self.inactivity_deadline
    }

    fn renew(&mut self, activity: Self) {
        self.last_activity_at = self.last_activity_at.max(activity.last_activity_at);
        self.inactivity_deadline = self.inactivity_deadline.max(activity.inactivity_deadline);
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum TransactionState {
    Open(TransactionActivity),
    Committing(Box<TransactionCommitProgress>),
    Finished(FinishedTransaction),
}

enum OpenActivityDecision {
    Active,
    Expired,
    NotOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionCommitFailureDecision {
    Failed,
    Expired,
    Existing,
}

impl TransactionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open(_) => "OPEN",
            Self::Committing(_) => "COMMITTING",
            Self::Finished(finished) => finished.outcome.as_str(),
        }
    }

    pub fn is_live(&self) -> bool {
        match self {
            Self::Open(_) | Self::Committing(_) => true,
            Self::Finished(_) => false,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ReplicatedTransaction {
    pub id: String,
    pub domain: DomainName,
    pub owner: UserName,
    pub created_at: Timestamp,
    pub state: TransactionState,
    pub statement_count: usize,
    pub queued_source_bytes: u64,
    pub statements: Vec<TransactionStatement>,
    latest_preview: Option<TransactionPreviewIdentity>,
    commit_plan: Option<TransactionCommitPlanHeader>,
    mutation_owner: DomainMutationOwner,
}

impl ReplicatedTransaction {
    pub fn open(
        id: String,
        domain: DomainName,
        owner: UserName,
        activity: TransactionActivity,
    ) -> Self {
        let mutation_owner = DomainMutationOwner::transaction(id.clone());
        Self::open_with_mutation_owner(id, domain, owner, activity, mutation_owner)
    }

    pub fn open_for_command(
        id: String,
        domain: DomainName,
        owner: UserName,
        activity: TransactionActivity,
        command: CommandExecutionReference,
    ) -> Self {
        Self::open_with_mutation_owner(
            id,
            domain,
            owner,
            activity,
            DomainMutationOwner::command(command),
        )
    }

    fn open_with_mutation_owner(
        id: String,
        domain: DomainName,
        owner: UserName,
        activity: TransactionActivity,
        mutation_owner: DomainMutationOwner,
    ) -> Self {
        Self {
            id,
            domain,
            owner,
            created_at: activity.last_activity_at(),
            state: TransactionState::Open(activity),
            statement_count: 0,
            queued_source_bytes: 0,
            statements: Vec::new(),
            latest_preview: None,
            commit_plan: None,
            mutation_owner,
        }
    }

    pub fn domain_mutation(&self) -> Option<&DomainMutationLease> {
        match &self.state {
            TransactionState::Committing(progress) => progress.domain_mutation.as_ref(),
            TransactionState::Open(_) | TransactionState::Finished(_) => None,
        }
    }

    pub fn last_activity_at(&self) -> Timestamp {
        match &self.state {
            TransactionState::Open(activity) => activity.last_activity_at(),
            TransactionState::Committing(progress) => progress.last_activity_at,
            TransactionState::Finished(finished) => finished.finished_at,
        }
    }

    pub fn inactivity_deadline(&self) -> Option<Timestamp> {
        match &self.state {
            TransactionState::Open(activity) => Some(activity.inactivity_deadline()),
            TransactionState::Committing(_) | TransactionState::Finished(_) => None,
        }
    }

    pub(crate) fn mutation_owner(&self) -> &DomainMutationOwner {
        &self.mutation_owner
    }

    pub(crate) fn requires_domain_mutation(&self) -> bool {
        self.statements
            .iter()
            .any(|statement| statement.statement.requires_domain_mutation_ownership())
    }

    pub fn pending_statement_count(&self) -> usize {
        match &self.state {
            TransactionState::Open(_) => self.statements.len(),
            TransactionState::Committing(progress) => self
                .statements
                .len()
                .checked_sub(progress.next_statement)
                .verified("commit progress never runs past the statements it commits"),
            TransactionState::Finished(_) => 0,
        }
    }

    pub fn completed_statement_count(&self) -> usize {
        match &self.state {
            TransactionState::Open(_) => 0,
            TransactionState::Committing(progress) => progress.next_statement,
            TransactionState::Finished(finished) => finished
                .results
                .iter()
                .map(TransactionStepResult::statement_count)
                .sum(),
        }
    }

    pub fn commit_results(&self) -> &[TransactionStepResult] {
        match &self.state {
            TransactionState::Committing(progress) => &progress.results,
            TransactionState::Finished(finished) => &finished.results,
            TransactionState::Open(_) => &[],
        }
    }

    pub fn finished_outcome(&self) -> Option<&TransactionOutcome> {
        match &self.state {
            TransactionState::Finished(finished) => Some(&finished.outcome),
            TransactionState::Open(_) | TransactionState::Committing(_) => None,
        }
    }

    pub fn latest_preview(&self) -> Option<&TransactionPreviewIdentity> {
        self.latest_preview.as_ref()
    }

    pub(crate) fn set_latest_preview(&mut self, preview: TransactionPreviewIdentity) {
        self.latest_preview = Some(preview);
    }

    pub fn commit_plan(&self) -> Option<&TransactionCommitPlanHeader> {
        self.commit_plan.as_ref()
    }

    pub(crate) fn has_commit_admission(&self, plan: &TransactionCommitPlanHeader) -> bool {
        self.commit_plan.as_ref() == Some(plan)
            && matches!(
                &self.state,
                TransactionState::Committing(_) | TransactionState::Finished(_)
            )
    }

    pub(crate) fn ensure_owner(&self, owner: &UserName) -> Result<(), TransactionMutationError> {
        if &self.owner == owner {
            Ok(())
        } else {
            Err(TransactionMutationError::OwnerMismatch {
                id: self.id.clone(),
            })
        }
    }

    pub(crate) fn ensure_domain(
        &self,
        domain: &DomainName,
    ) -> Result<(), TransactionMutationError> {
        if &self.domain == domain {
            Ok(())
        } else {
            Err(TransactionMutationError::DomainMismatch {
                id: self.id.clone(),
                expected: self.domain.clone(),
                requested: domain.clone(),
            })
        }
    }

    pub fn queue_admission(
        &self,
        owner: &UserName,
        domain: &DomainName,
        statement: &TransactionStatementRequest,
        limits: TransactionQueueLimits,
    ) -> Result<TransactionQueueAdmission, TransactionMutationError> {
        self.ensure_owner(owner)?;
        self.ensure_domain(domain)?;
        // Open transactions are capped by `limits.max_statements`, so this identity lookup has
        // the same configured bound as queue admission itself.
        if let Some(existing) = self
            .statements
            .iter()
            .find(|existing| existing.request_reference == statement.request_reference)
        {
            if existing.request == *statement {
                return Ok(TransactionQueueAdmission::Existing(
                    existing.admission.clone(),
                ));
            }
            return Err(TransactionMutationError::RequestConflict {
                id: self.id.clone(),
                request_reference: statement.request_reference.clone(),
            });
        }
        if !matches!(self.state, TransactionState::Open(_)) {
            return Err(TransactionMutationError::NotOpen {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        }
        if statement.expected_position != self.statements.len() {
            return Err(TransactionMutationError::PositionConflict {
                id: self.id.clone(),
                expected: statement.expected_position,
                actual: self.statements.len(),
            });
        }
        if self.statements.len() >= limits.max_statements {
            return Err(TransactionMutationError::StatementLimit {
                id: self.id.clone(),
                limit: limits.max_statements,
            });
        }
        // A statement whose bytes cannot even be added to the queued total is past any
        // configured limit, so it reports as the same admission failure.
        let admitted = self
            .queued_source_bytes
            .checked_add(statement.source_bytes())
            .is_some_and(|next| next <= limits.max_source_bytes);
        if !admitted {
            return Err(TransactionMutationError::SourceByteLimit {
                id: self.id.clone(),
                limit: limits.max_source_bytes,
            });
        }
        Ok(TransactionQueueAdmission::New)
    }

    fn not_open_error(&self) -> TransactionMutationError {
        TransactionMutationError::NotOpen {
            id: self.id.clone(),
            state: self.state.as_str().to_string(),
        }
    }

    /// Atomically decides whether an OPEN transaction may record fresh activity.
    ///
    /// `Expired` means the transaction crossed its inclusive inactivity boundary and was made
    /// terminal instead. The caller must not apply the requested OPEN mutation in that case.
    fn expire_open_if_inactive(
        &mut self,
        activity: TransactionActivity,
        outcome_revision: u64,
    ) -> OpenActivityDecision {
        let TransactionState::Open(current) = &mut self.state else {
            return OpenActivityDecision::NotOpen;
        };
        if current.is_inactive_at(activity.last_activity_at()) {
            self.finish(
                activity.last_activity_at(),
                outcome_revision,
                TransactionOutcome::Expired,
                Vec::new(),
            );
            return OpenActivityDecision::Expired;
        }
        OpenActivityDecision::Active
    }

    pub(crate) fn queue(
        &mut self,
        owner: &UserName,
        domain: &DomainName,
        activity: TransactionActivity,
        outcome_revision: u64,
        statement: TransactionStatement,
        limits: TransactionQueueLimits,
    ) -> Result<TransactionQueueDecision, TransactionMutationError> {
        self.ensure_owner(owner)?;
        self.ensure_domain(domain)?;
        match self.expire_open_if_inactive(activity, outcome_revision) {
            OpenActivityDecision::Active => {}
            OpenActivityDecision::Expired => return Ok(TransactionQueueDecision::Expired),
            OpenActivityDecision::NotOpen => return Err(self.not_open_error()),
        }
        match self.queue_admission(owner, domain, &statement.request, limits)? {
            TransactionQueueAdmission::Existing(_) => {
                return Ok(TransactionQueueDecision::Existing);
            }
            TransactionQueueAdmission::New => {}
        }
        let TransactionState::Open(current) = &mut self.state else {
            return Err(self.not_open_error());
        };
        current.renew(activity);
        let next_source_bytes = self
            .queued_source_bytes
            .checked_add(statement.source_bytes())
            .verified("the admission check above rejected a statement that does not fit");
        self.statement_count = self
            .statement_count
            .checked_add(1)
            .verified("the admission check above bounds the count by the statement limit");
        self.queued_source_bytes = next_source_bytes;
        self.statements.push(statement);
        Ok(TransactionQueueDecision::Added)
    }

    pub(crate) fn start_commit(
        &mut self,
        owner: &UserName,
        activity: TransactionActivity,
        outcome_revision: u64,
        domain_mutation: Option<DomainMutationLease>,
        commit_plan: TransactionCommitPlanHeader,
    ) -> Result<(), TransactionMutationError> {
        self.ensure_owner(owner)?;
        match self.expire_open_if_inactive(activity, outcome_revision) {
            OpenActivityDecision::Active => {}
            OpenActivityDecision::Expired => return Ok(()),
            OpenActivityDecision::NotOpen => return Err(self.not_open_error()),
        }
        let TransactionState::Open(current) = &mut self.state else {
            return Err(self.not_open_error());
        };
        current.renew(activity);
        let last_activity_at = current.last_activity_at();
        self.latest_preview = Some(commit_plan.preview.clone());
        self.commit_plan = Some(commit_plan);
        self.state = TransactionState::Committing(Box::new(TransactionCommitProgress {
            last_activity_at,
            next_statement: 0,
            results: Vec::new(),
            applying: None,
            domain_mutation,
        }));
        Ok(())
    }

    pub(crate) fn matches_commit_admission_failure(
        &self,
        preview: &TransactionPreviewIdentity,
        failing_step: usize,
        error: &str,
    ) -> bool {
        self.latest_preview.as_ref() == Some(preview)
            && matches!(
                &self.state,
                TransactionState::Finished(FinishedTransaction {
                    outcome: TransactionOutcome::Failed {
                        failing_step: retained_step,
                        error: retained_error,
                    },
                    ..
                }) if *retained_step == failing_step && retained_error == error
            )
    }

    pub(crate) fn fail_commit_admission(
        &mut self,
        owner: &UserName,
        activity: TransactionActivity,
        outcome_revision: u64,
        preview: &TransactionPreviewIdentity,
        failing_step: usize,
        error: &str,
    ) -> error_stack::Result<TransactionCommitFailureDecision, TransactionMutationError> {
        self.ensure_owner(owner).map_err(Report::new)?;
        if self.matches_commit_admission_failure(preview, failing_step, error) {
            return Ok(TransactionCommitFailureDecision::Existing);
        }
        match self.expire_open_if_inactive(activity, outcome_revision) {
            OpenActivityDecision::Active => {}
            OpenActivityDecision::Expired => {
                return Ok(TransactionCommitFailureDecision::Expired);
            }
            OpenActivityDecision::NotOpen => return Err(Report::new(self.not_open_error())),
        }
        if failing_step >= self.statements.len() {
            return Err(Report::new(TransactionMutationError::InvalidProgress {
                id: self.id.clone(),
                next: failing_step,
                statement_count: self.statements.len(),
            }));
        }
        let TransactionState::Open(current) = &mut self.state else {
            return Err(Report::new(self.not_open_error()));
        };
        current.renew(activity);
        let finished_at = current.last_activity_at();
        self.latest_preview = Some(preview.clone());
        self.finish(
            finished_at,
            outcome_revision,
            TransactionOutcome::Failed {
                failing_step,
                error: error.to_string(),
            },
            Vec::new(),
        );
        Ok(TransactionCommitFailureDecision::Failed)
    }

    pub(crate) fn touch(
        &mut self,
        owner: &UserName,
        activity: TransactionActivity,
        outcome_revision: u64,
    ) -> Result<(), TransactionMutationError> {
        self.ensure_owner(owner)?;
        if matches!(self.state, TransactionState::Open(_)) {
            match self.expire_open_if_inactive(activity, outcome_revision) {
                OpenActivityDecision::Active => {}
                OpenActivityDecision::Expired => return Ok(()),
                OpenActivityDecision::NotOpen => return Err(self.not_open_error()),
            }
            let TransactionState::Open(current) = &mut self.state else {
                return Err(self.not_open_error());
            };
            current.renew(activity);
            return Ok(());
        }
        match &mut self.state {
            TransactionState::Committing(progress) => {
                progress.last_activity_at =
                    progress.last_activity_at.max(activity.last_activity_at());
                Ok(())
            }
            TransactionState::Finished(finished) => Err(TransactionMutationError::Finished {
                id: self.id.clone(),
                outcome: finished.outcome.as_str().to_string(),
            }),
            TransactionState::Open(_) => Ok(()),
        }
    }

    pub(crate) fn begin_application(
        &mut self,
        expected_next_statement: usize,
        at: Timestamp,
        applying: TransactionApplyingStep,
    ) -> Result<(), TransactionMutationError> {
        let TransactionState::Committing(progress) = &mut self.state else {
            return Err(TransactionMutationError::NotCommitting {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        };
        if progress.next_statement != expected_next_statement {
            return Err(TransactionMutationError::ProgressConflict {
                id: self.id.clone(),
                expected: expected_next_statement,
                actual: progress.next_statement,
            });
        }
        if let Some(current) = &progress.applying {
            if current == &applying {
                return Ok(());
            }
            return Err(TransactionMutationError::ApplicationInProgress {
                id: self.id.clone(),
                statement: progress.next_statement,
            });
        }
        if applying.next_statement <= expected_next_statement
            || applying.next_statement > self.statements.len()
        {
            return Err(TransactionMutationError::InvalidProgress {
                id: self.id.clone(),
                next: applying.next_statement,
                statement_count: self.statements.len(),
            });
        }
        if applying.result.first_statement() != expected_next_statement
            || Some(applying.result.statement_count())
                != applying.next_statement.checked_sub(expected_next_statement)
        {
            return Err(TransactionMutationError::InvalidStepResult {
                id: self.id.clone(),
            });
        }
        progress.last_activity_at = progress.last_activity_at.max(at);
        progress.applying = Some(applying);
        Ok(())
    }

    pub(crate) fn complete_application(
        &mut self,
        expected_next_statement: usize,
        at: Timestamp,
        outcome_revision: u64,
        actual: ActualExecutionStepImpact,
        application_failure: Option<String>,
    ) -> Result<(), TransactionMutationError> {
        let TransactionState::Committing(progress) = &mut self.state else {
            return Err(TransactionMutationError::NotCommitting {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        };
        if progress.next_statement != expected_next_statement {
            return Err(TransactionMutationError::ProgressConflict {
                id: self.id.clone(),
                expected: expected_next_statement,
                actual: progress.next_statement,
            });
        }
        let Some(mut applying) = progress.applying.take() else {
            return Err(TransactionMutationError::NoApplicationInProgress {
                id: self.id.clone(),
                statement: expected_next_statement,
            });
        };
        if applying.result.first_statement() != expected_next_statement {
            progress.applying = Some(applying);
            return Err(TransactionMutationError::InvalidStepResult {
                id: self.id.clone(),
            });
        }
        applying.result.impact.actual_mut().quiescence = actual.quiescence;
        applying.result.impact.actual_mut().effects = actual.effects;

        let completion = if let Some(error) = application_failure {
            applying.result.result.success = false;
            applying.result.result.message = error.clone();
            applying.result.result.diagnostics = vec![TransactionDiagnostic {
                message: error.clone(),
                span: None,
            }];
            applying.result.impact.actual_mut().outcome = ExecutionStepOutcome::Failed {
                diagnostic: ImpactDiagnostic {
                    kind: ImpactDiagnosticKind::Application,
                    operation: Some(applying.result.operation_range().first()),
                    message: error.clone(),
                },
            };
            Some(TransactionOutcome::Failed {
                failing_step: expected_next_statement,
                error,
            })
        } else {
            applying.result.impact.actual_mut().outcome = if applying.result.result.success {
                ExecutionStepOutcome::Applied
            } else {
                ExecutionStepOutcome::Failed {
                    diagnostic: ImpactDiagnostic {
                        kind: ImpactDiagnosticKind::Application,
                        operation: Some(applying.result.operation_range().first()),
                        message: applying.result.result.message.clone(),
                    },
                }
            };
            applying.completion
        };
        progress.last_activity_at = progress.last_activity_at.max(at);
        progress.next_statement = applying.next_statement;
        progress.results.push(applying.result);
        if let Some(outcome) = completion {
            let finished_at = progress.last_activity_at;
            let results = std::mem::take(&mut progress.results);
            self.finish(finished_at, outcome_revision, outcome, results);
        }
        Ok(())
    }

    pub(crate) fn finish_empty_commit(
        &mut self,
        at: Timestamp,
        outcome_revision: u64,
    ) -> Result<(), TransactionMutationError> {
        let TransactionState::Committing(progress) = &mut self.state else {
            return Err(TransactionMutationError::NotCommitting {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        };
        if !self.statements.is_empty()
            || progress.next_statement != 0
            || progress.applying.is_some()
        {
            return Err(TransactionMutationError::InvalidProgress {
                id: self.id.clone(),
                next: progress.next_statement,
                statement_count: self.statements.len(),
            });
        }
        let finished_at = progress.last_activity_at.max(at);
        self.finish(
            finished_at,
            outcome_revision,
            TransactionOutcome::Committed,
            Vec::new(),
        );
        Ok(())
    }

    pub(crate) fn revert(
        &mut self,
        owner: &UserName,
        activity: TransactionActivity,
        outcome_revision: u64,
    ) -> Result<(), TransactionMutationError> {
        self.ensure_owner(owner)?;
        match self.expire_open_if_inactive(activity, outcome_revision) {
            OpenActivityDecision::Active => {}
            OpenActivityDecision::Expired => return Ok(()),
            OpenActivityDecision::NotOpen => return Err(self.not_open_error()),
        }
        let TransactionState::Open(current) = &mut self.state else {
            return Err(self.not_open_error());
        };
        current.renew(activity);
        let finished_at = current.last_activity_at();
        self.finish(
            finished_at,
            outcome_revision,
            TransactionOutcome::Reverted,
            Vec::new(),
        );
        Ok(())
    }

    pub(crate) fn expire(
        &mut self,
        at: Timestamp,
        outcome_revision: u64,
    ) -> Result<bool, TransactionMutationError> {
        let TransactionState::Open(activity) = &self.state else {
            return Ok(false);
        };
        if !activity.is_inactive_at(at) {
            return Ok(false);
        }
        self.finish(
            at,
            outcome_revision,
            TransactionOutcome::Expired,
            Vec::new(),
        );
        Ok(true)
    }

    fn finish(
        &mut self,
        at: Timestamp,
        outcome_revision: u64,
        outcome: TransactionOutcome,
        results: Vec<TransactionStepResult>,
    ) {
        self.statements.clear();
        self.queued_source_bytes = 0;
        self.state = TransactionState::Finished(FinishedTransaction {
            outcome,
            outcome_revision,
            finished_at: at,
            results,
        });
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum TransactionStepEffect {
    ReplaceDomainSchedule {
        inputs: Box<DomainPlanningInputs>,
        schedule: Option<Box<DomainSchedule>>,
    },
    PutDomainAndSchedule {
        inputs: Box<DomainPlanningInputs>,
        domain: Box<DomainState>,
        schedule: Option<Box<DomainSchedule>>,
    },
    StartDomain {
        inputs: Box<DomainPlanningInputs>,
        start: DomainStartPoint,
        clock: Option<DomainClockState>,
        authority: Option<ClusterNodeIdentity>,
    },
    StopDomain {
        inputs: Box<DomainPlanningInputs>,
    },
    CreateResourceCatalog {
        inputs: Box<DomainPlanningInputs>,
        identifier: ResourceName,
    },
    ResetWasmState {
        inputs: Box<DomainPlanningInputs>,
        reset: Box<nervix_models::ResetWasmState>,
        request: nervix_models::CommandExecutionReference,
        schedule: Box<nervix_models::DomainSchedule>,
    },
}

impl TransactionStepEffect {
    pub fn inputs(&self) -> &DomainPlanningInputs {
        match self {
            Self::ReplaceDomainSchedule { inputs, .. }
            | Self::PutDomainAndSchedule { inputs, .. }
            | Self::StartDomain { inputs, .. }
            | Self::StopDomain { inputs }
            | Self::CreateResourceCatalog { inputs, .. } => inputs,
            Self::ResetWasmState { inputs, .. } => inputs,
        }
    }
}

/// How the application of the step a transaction is applying ends.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum TransactionApplicationOutcome {
    /// Every obligation of the step's committed effect completed.
    Applied,
    /// The step's committed effect did not become usable, and it stays committed.
    Failed { error: String },
    /// The step's committed model schedule did not become usable. The entry that records the
    /// failure also puts back the schedule the step replaced, which the step's own effect captured
    /// as its planning basis, so the step leaves no model change behind.
    RolledBack {
        error: String,
        /// The domain as the failed step left it. The rollback conflicts when these no longer
        /// describe the domain, so it never discards a change made after the step.
        inputs: Box<DomainPlanningInputs>,
    },
}

impl TransactionApplicationOutcome {
    /// The failure the step's result reports, absent when the step applied.
    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Applied => None,
            Self::Failed { error } | Self::RolledBack { error, .. } => Some(error),
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionMutationResponse {
    pub result: Result<ReplicatedTransaction, TransactionMutationError>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Error,
)]
pub enum TransactionMutationError {
    #[error("transaction '{id}' already exists")]
    AlreadyExists { id: String },
    #[error("transaction '{id}' is unknown")]
    Unknown { id: String },
    #[error("transaction '{id}' belongs to another user")]
    OwnerMismatch { id: String },
    #[error(
        "transaction '{id}' is bound to domain '{expected}'; the statement selected domain \
         '{requested}'"
    )]
    DomainMismatch {
        id: String,
        expected: DomainName,
        requested: DomainName,
    },
    #[error("transaction '{id}' is not open (state {state})")]
    NotOpen { id: String, state: String },
    #[error("transaction '{id}' is not committing (state {state})")]
    NotCommitting { id: String, state: String },
    #[error("transaction '{id}' finished with outcome {outcome}")]
    Finished { id: String, outcome: String },
    #[error("concurrent open transaction limit {limit} reached")]
    OpenLimit { limit: usize },
    #[error("transaction '{id}' queued statement limit {limit} reached")]
    StatementLimit { id: String, limit: usize },
    #[error("transaction '{id}' queued source byte limit {limit} exceeded")]
    SourceByteLimit { id: String, limit: u64 },
    #[error(
        "transaction '{id}' request reference '{request_reference}' was already used for a \
         different append"
    )]
    RequestConflict {
        id: String,
        request_reference: CommandExecutionReference,
    },
    #[error("transaction '{id}' queue position changed: expected {expected}, found {actual}")]
    PositionConflict {
        id: String,
        expected: usize,
        actual: usize,
    },
    #[error("transaction '{id}' report identity does not match its accepted queue state")]
    ReportMismatch { id: String },
    #[error("transaction '{id}' report conflicts with retained report content")]
    ReportConflict { id: String },
    #[error("transaction preview is stale: expected {expected:?}, current {current:?}")]
    PreviewStale {
        expected: Box<TransactionPreviewIdentity>,
        current: Box<TransactionPreviewIdentity>,
    },
    #[error("transaction '{id}' commit plan does not match its complete preview")]
    InvalidCommitPlan { id: String },
    #[error("transaction '{id}' commit failure does not match its incomplete preview")]
    InvalidCommitFailure { id: String },
    #[error("transaction '{id}' planning inputs changed before commit admission: {reason}")]
    PlanningInputsChanged { id: String, reason: String },
    #[error(
        "transaction '{id}' commit progress changed: expected statement {expected}, found {actual}"
    )]
    ProgressConflict {
        id: String,
        expected: usize,
        actual: usize,
    },
    #[error(
        "transaction '{id}' commit progress {next} is invalid for {statement_count} statement(s)"
    )]
    InvalidProgress {
        id: String,
        next: usize,
        statement_count: usize,
    },
    #[error("transaction '{id}' commit step result does not match its progress range")]
    InvalidStepResult { id: String },
    #[error("transaction '{id}' is already applying the step beginning at statement {statement}")]
    ApplicationInProgress { id: String, statement: usize },
    #[error("transaction '{id}' has no applying step beginning at statement {statement}")]
    NoApplicationInProgress { id: String, statement: usize },
    #[error("transaction '{id}' commit step effect does not match its queued statement(s)")]
    EffectMismatch { id: String },
    #[error("transaction '{id}' commit step conflicted with replicated state: {reason}")]
    StepConflict { id: String, reason: String },
    #[error("transaction '{id}' cannot mutate domain '{domain}' while it is owned by {owner}")]
    DomainMutationConflict {
        id: String,
        domain: DomainName,
        owner: DomainMutationOwner,
    },
    #[error("transaction '{id}' lost its mutation lease for domain '{domain}'")]
    DomainMutationFenceLost { id: String, domain: DomainName },
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        ActualExecutionStepImpact, ImpactEffects, ImpactReportCompleteness, PauseRequirement,
        PlannedExecutionStepImpact, ShowTransactions,
    };

    use super::*;

    fn test_owner() -> UserName {
        UserName::parse("operator").assured("the test owner is an identifier-shaped literal")
    }

    fn test_domain() -> DomainName {
        DomainName::parse("default").assured("the test domain is an identifier-shaped literal")
    }

    fn activity(at: i64) -> TransactionActivity {
        TransactionActivity::from_timeout(Timestamp::from_unix_nanos(at), Duration::from_nanos(10))
    }

    fn transaction_with_one_statement() -> ReplicatedTransaction {
        let owner = test_owner();
        let domain = test_domain();
        let mut transaction = ReplicatedTransaction::open(
            "transaction-1".to_string(),
            domain.clone(),
            owner.clone(),
            activity(1),
        );
        transaction
            .queue(
                &owner,
                &domain,
                activity(2),
                2,
                TransactionStatement::test_admitted(TransactionStatementRequest {
                    request_reference: CommandExecutionReference::parse("request.0")
                        .assured("the test command reference is an accepted literal"),
                    expected_position: 0,
                    source: "SHOW TRANSACTIONS".to_string(),
                    statement: Statement::ShowTransactions(ShowTransactions),
                }),
                TransactionQueueLimits {
                    max_statements: 2,
                    max_source_bytes: 1024,
                },
            )
            .assured("the first test statement is within every admission limit");
        transaction
            .start_commit(
                &owner,
                activity(3),
                3,
                None,
                TransactionCommitPlanHeader {
                    preview: test_commit_plan("transaction-1", 1).preview,
                    step_count: 1,
                },
            )
            .assured("an open test transaction can begin committing");
        transaction
    }

    fn applying_step() -> TransactionApplyingStep {
        let operations = TransactionOperationRange::from_index_and_count(0, 1)
            .assured("the first test operation is an addressable single-operation range");
        TransactionApplyingStep {
            effect_revision: 4,
            next_statement: 1,
            result: TransactionStepResult {
                impact: ExecutionStepImpactReport::new(
                    operations,
                    PlannedExecutionStepImpact {
                        completeness: ImpactReportCompleteness::Complete,
                        pause: PauseRequirement::NoPause,
                        effects: ImpactEffects::default(),
                    },
                    ActualExecutionStepImpact::applying(),
                ),
                result: TransactionCommandResult {
                    success: true,
                    message: "listed transactions".to_string(),
                    diagnostics: Vec::new(),
                    already_existed: false,
                    admission: None,
                },
            },
            effect: None,
            completion: Some(TransactionOutcome::Committed),
        }
    }

    #[test]
    fn beginning_application_is_idempotent_and_rejects_conflicting_progress() {
        let mut transaction = transaction_with_one_statement();
        let applying = applying_step();
        transaction
            .begin_application(0, Timestamp::from_unix_nanos(4), applying.clone())
            .assured("the application step spans the queued test statement");
        transaction
            .begin_application(0, Timestamp::from_unix_nanos(5), applying.clone())
            .assured("replaying the identical application step is idempotent");

        let mut conflicting = applying;
        conflicting.effect_revision = 5;
        assert!(matches!(
            transaction.begin_application(0, Timestamp::from_unix_nanos(6), conflicting),
            Err(TransactionMutationError::ApplicationInProgress { statement: 0, .. })
        ));

        let mut invalid = transaction_with_one_statement();
        let mut no_progress = applying_step();
        no_progress.next_statement = 0;
        assert!(matches!(
            invalid.begin_application(0, Timestamp::from_unix_nanos(4), no_progress),
            Err(TransactionMutationError::InvalidProgress {
                next: 0,
                statement_count: 1,
                ..
            })
        ));
    }

    #[test]
    fn completing_application_preserves_in_progress_state_after_conflicts() {
        let mut open = ReplicatedTransaction::open(
            "open".to_string(),
            test_domain(),
            test_owner(),
            activity(1),
        );
        assert!(matches!(
            open.complete_application(
                0,
                Timestamp::from_unix_nanos(2),
                2,
                ActualExecutionStepImpact::applying(),
                None,
            ),
            Err(TransactionMutationError::NotCommitting { .. })
        ));

        let mut without_application = transaction_with_one_statement();
        assert!(matches!(
            without_application.complete_application(
                0,
                Timestamp::from_unix_nanos(4),
                4,
                ActualExecutionStepImpact::applying(),
                None,
            ),
            Err(TransactionMutationError::NoApplicationInProgress { statement: 0, .. })
        ));

        let mut conflicting_progress = transaction_with_one_statement();
        conflicting_progress
            .begin_application(0, Timestamp::from_unix_nanos(4), applying_step())
            .assured("the application step spans the queued test statement");
        assert!(matches!(
            conflicting_progress.complete_application(
                1,
                Timestamp::from_unix_nanos(5),
                5,
                ActualExecutionStepImpact::applying(),
                None,
            ),
            Err(TransactionMutationError::ProgressConflict {
                expected: 1,
                actual: 0,
                ..
            })
        ));

        let mut invalid_result = transaction_with_one_statement();
        invalid_result
            .begin_application(0, Timestamp::from_unix_nanos(4), applying_step())
            .assured("the application step spans the queued test statement");
        let TransactionState::Committing(progress) = &mut invalid_result.state else {
            panic!("the test transaction must still be committing");
        };
        let current = progress
            .applying
            .as_mut()
            .assured("the test installed one applying step above");
        let operations = TransactionOperationRange::from_index_and_count(1, 1)
            .assured("the second test operation is an addressable single-operation range");
        current.result.impact = ExecutionStepImpactReport::new(
            operations,
            PlannedExecutionStepImpact {
                completeness: ImpactReportCompleteness::Complete,
                pause: PauseRequirement::NoPause,
                effects: ImpactEffects::default(),
            },
            ActualExecutionStepImpact::applying(),
        );
        assert!(matches!(
            invalid_result.complete_application(
                0,
                Timestamp::from_unix_nanos(5),
                5,
                ActualExecutionStepImpact::applying(),
                None,
            ),
            Err(TransactionMutationError::InvalidStepResult { .. })
        ));
        let TransactionState::Committing(progress) = &invalid_result.state else {
            panic!("an invalid result must leave the transaction committing");
        };
        assert!(progress.applying.is_some());
    }

    #[test]
    fn finishing_an_empty_commit_requires_empty_progress() {
        let mut open = ReplicatedTransaction::open(
            "open".to_string(),
            test_domain(),
            test_owner(),
            activity(1),
        );
        assert!(matches!(
            open.finish_empty_commit(Timestamp::from_unix_nanos(2), 2),
            Err(TransactionMutationError::NotCommitting { .. })
        ));

        let mut nonempty = transaction_with_one_statement();
        assert!(matches!(
            nonempty.finish_empty_commit(Timestamp::from_unix_nanos(4), 4),
            Err(TransactionMutationError::InvalidProgress {
                next: 0,
                statement_count: 1,
                ..
            })
        ));

        let owner = test_owner();
        let mut empty = ReplicatedTransaction::open(
            "empty".to_string(),
            test_domain(),
            owner.clone(),
            activity(1),
        );
        empty
            .start_commit(
                &owner,
                activity(2),
                2,
                None,
                TransactionCommitPlanHeader {
                    preview: test_commit_plan("empty", 0).preview,
                    step_count: 0,
                },
            )
            .assured("the empty test transaction can begin committing");
        empty
            .finish_empty_commit(Timestamp::from_unix_nanos(3), 7)
            .assured("empty progress can finish as committed");
        let TransactionState::Finished(finished) = &empty.state else {
            panic!("the empty commit must finish the transaction");
        };
        assert_eq!(finished.outcome, TransactionOutcome::Committed);
        assert_eq!(finished.outcome_revision, 7);
        assert!(finished.results.is_empty());
    }

    #[test]
    fn open_activity_uses_an_inclusive_durable_deadline() {
        let owner = test_owner();
        let mut transaction = ReplicatedTransaction::open(
            "deadline".to_string(),
            test_domain(),
            owner.clone(),
            activity(1),
        );

        transaction
            .touch(&owner, activity(11), 7)
            .assured("activity at the deadline resolves to a terminal outcome");

        let TransactionState::Finished(finished) = &transaction.state else {
            panic!("activity at the inclusive inactivity boundary must expire the transaction");
        };
        assert_eq!(finished.outcome, TransactionOutcome::Expired);
        assert_eq!(finished.finished_at, Timestamp::from_unix_nanos(11));
        assert_eq!(finished.outcome_revision, 7);
    }

    #[test]
    fn renewal_wins_over_an_older_sweep_without_moving_backward() {
        let owner = test_owner();
        let mut transaction = ReplicatedTransaction::open(
            "renewed".to_string(),
            test_domain(),
            owner.clone(),
            activity(1),
        );
        transaction
            .touch(&owner, activity(5), 2)
            .assured("activity before the original deadline renews the transaction");

        assert!(
            !transaction
                .expire(Timestamp::from_unix_nanos(11), 3)
                .assured("an older sweep observation is a valid expiry proposal")
        );
        assert_eq!(
            transaction.last_activity_at(),
            Timestamp::from_unix_nanos(5)
        );
        assert_eq!(
            transaction.inactivity_deadline(),
            Some(Timestamp::from_unix_nanos(15))
        );

        transaction
            .touch(&owner, activity(3), 4)
            .assured("a backward clock observation cannot invalidate current activity");
        assert_eq!(
            transaction.last_activity_at(),
            Timestamp::from_unix_nanos(5)
        );
        assert_eq!(
            transaction.inactivity_deadline(),
            Some(Timestamp::from_unix_nanos(15))
        );
        assert!(
            transaction
                .expire(Timestamp::from_unix_nanos(15), 5)
                .assured("the renewed inclusive deadline is a valid expiry proposal")
        );
    }

    #[test]
    fn an_expiry_decision_cannot_be_renewed_or_applied_as_a_mutation() {
        let owner = test_owner();
        let domain = test_domain();
        let mut transaction = ReplicatedTransaction::open(
            "expired-mutation".to_string(),
            domain.clone(),
            owner.clone(),
            activity(1),
        );
        let statement = TransactionStatement::test_admitted(TransactionStatementRequest {
            request_reference: CommandExecutionReference::parse("expired.request")
                .assured("the test command reference is an accepted literal"),
            expected_position: 0,
            source: "SHOW TRANSACTIONS".to_string(),
            statement: Statement::ShowTransactions(ShowTransactions),
        });

        transaction
            .queue(
                &owner,
                &domain,
                activity(11),
                9,
                statement,
                TransactionQueueLimits {
                    max_statements: 2,
                    max_source_bytes: 1024,
                },
            )
            .assured("an overdue mutation resolves authoritatively as expiry");

        assert!(transaction.statements.is_empty());
        assert!(matches!(
            transaction.finished_outcome(),
            Some(TransactionOutcome::Expired)
        ));
        assert!(matches!(
            transaction.touch(&owner, activity(12), 10),
            Err(TransactionMutationError::Finished { .. })
        ));
    }

    #[test]
    fn committing_transactions_are_not_subject_to_open_inactivity() {
        let owner = test_owner();
        let mut transaction = ReplicatedTransaction::open(
            "committing".to_string(),
            test_domain(),
            owner.clone(),
            activity(1),
        );
        transaction
            .start_commit(
                &owner,
                activity(2),
                2,
                None,
                TransactionCommitPlanHeader {
                    preview: test_commit_plan("committing", 0).preview,
                    step_count: 0,
                },
            )
            .assured("the open transaction can enter commit recovery");

        assert!(
            !transaction
                .expire(Timestamp::from_unix_nanos(i64::MAX), 3)
                .assured("expiry ignores a durable committing state")
        );
        assert!(matches!(transaction.state, TransactionState::Committing(_)));
    }
}
