use arch_into::ArchInto as _;
use meticulous::OptionExt as _;
#[cfg(test)]
use meticulous::ResultExt as _;
use nervix_models::{
    ClusterNodeIdentity, CommandExecutionReference, DomainClockState, DomainName, DomainSchedule,
    DomainStartPoint, DomainState, ExecutionStepImpactReport, ExecutionStepOutcome,
    ImpactDiagnostic, ImpactDiagnosticKind, ResourceName, Statement, Timestamp,
    TransactionOperationRange, UserName,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;
use thiserror::Error;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionStatement {
    pub request_reference: CommandExecutionReference,
    pub expected_position: usize,
    pub source: String,
    pub statement: Statement,
}

impl TransactionStatement {
    pub fn source_bytes(&self) -> u64 {
        self.source.len().arch_into()
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionDiagnostic {
    pub message: String,
    pub span_start: u32,
    pub span_end: u32,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionCommandResult {
    pub success: bool,
    pub message: String,
    pub diagnostics: Vec<TransactionDiagnostic>,
    pub already_existed: bool,
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
pub(crate) fn test_step_impact(
    first_statement: usize,
    statement_count: usize,
) -> ExecutionStepImpactReport {
    let operations =
        TransactionOperationRange::from_index_and_count(first_statement, statement_count)
            .assured("test transaction steps use non-empty addressable statement ranges");
    ExecutionStepImpactReport::new(
        operations,
        nervix_models::PlannedExecutionStepImpact {
            completeness: nervix_models::ImpactReportCompleteness::Complete,
            pause: nervix_models::PauseRequirement::NoPause,
            effects: nervix_models::ImpactEffects::default(),
        },
        nervix_models::ActualExecutionStepImpact::applying(),
    )
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
    /// The first statement whose application has not completed yet.
    pub next_statement: usize,
    /// Results whose authoritative effects and application obligations both completed.
    pub results: Vec<TransactionStepResult>,
    /// A step whose authoritative effect is durable but whose application is still in progress.
    pub applying: Option<TransactionApplyingStep>,
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
    Failed { failing_step: usize, error: String },
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

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum TransactionState {
    Open,
    Committing(Box<TransactionCommitProgress>),
    Finished(FinishedTransaction),
}

impl TransactionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Committing(_) => "COMMITTING",
            Self::Finished(finished) => finished.outcome.as_str(),
        }
    }

    pub fn is_live(&self) -> bool {
        match self {
            Self::Open | Self::Committing(_) => true,
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
    pub last_activity_at: Timestamp,
    pub state: TransactionState,
    pub statement_count: usize,
    pub queued_source_bytes: u64,
    pub statements: Vec<TransactionStatement>,
}

impl ReplicatedTransaction {
    pub fn open(id: String, domain: DomainName, owner: UserName, now: Timestamp) -> Self {
        Self {
            id,
            domain,
            owner,
            created_at: now,
            last_activity_at: now,
            state: TransactionState::Open,
            statement_count: 0,
            queued_source_bytes: 0,
            statements: Vec::new(),
        }
    }

    pub fn pending_statement_count(&self) -> usize {
        match &self.state {
            TransactionState::Open => self.statements.len(),
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
            TransactionState::Open => 0,
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
            TransactionState::Open => &[],
        }
    }

    pub fn finished_outcome(&self) -> Option<&TransactionOutcome> {
        match &self.state {
            TransactionState::Finished(finished) => Some(&finished.outcome),
            TransactionState::Open | TransactionState::Committing(_) => None,
        }
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

    pub fn validate_queue_admission(
        &self,
        owner: &UserName,
        domain: &DomainName,
        statement: &TransactionStatement,
        limits: TransactionQueueLimits,
    ) -> Result<(), TransactionMutationError> {
        self.ensure_owner(owner)?;
        self.ensure_domain(domain)?;
        if !matches!(self.state, TransactionState::Open) {
            return Err(TransactionMutationError::NotOpen {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        }
        if let Some(existing) = self
            .statements
            .iter()
            .find(|existing| existing.request_reference == statement.request_reference)
        {
            if existing == statement {
                return Ok(());
            }
            return Err(TransactionMutationError::RequestConflict {
                id: self.id.clone(),
                request_reference: statement.request_reference.clone(),
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
        Ok(())
    }

    pub(crate) fn queue(
        &mut self,
        owner: &UserName,
        domain: &DomainName,
        at: Timestamp,
        statement: TransactionStatement,
        limits: TransactionQueueLimits,
    ) -> Result<(), TransactionMutationError> {
        self.validate_queue_admission(owner, domain, &statement, limits)?;
        if self
            .statements
            .iter()
            .any(|existing| existing == &statement)
        {
            return Ok(());
        }
        let next_source_bytes = self
            .queued_source_bytes
            .checked_add(statement.source_bytes())
            .verified("the admission check above rejected a statement that does not fit");
        self.last_activity_at = at;
        self.statement_count = self
            .statement_count
            .checked_add(1)
            .verified("the admission check above bounds the count by the statement limit");
        self.queued_source_bytes = next_source_bytes;
        self.statements.push(statement);
        Ok(())
    }

    pub(crate) fn start_commit(
        &mut self,
        owner: &UserName,
        at: Timestamp,
    ) -> Result<(), TransactionMutationError> {
        self.ensure_owner(owner)?;
        if !matches!(self.state, TransactionState::Open) {
            return Err(TransactionMutationError::NotOpen {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        }
        self.last_activity_at = at;
        self.state = TransactionState::Committing(Box::new(TransactionCommitProgress {
            next_statement: 0,
            results: Vec::new(),
            applying: None,
        }));
        Ok(())
    }

    pub(crate) fn touch(
        &mut self,
        owner: &UserName,
        at: Timestamp,
    ) -> Result<(), TransactionMutationError> {
        self.ensure_owner(owner)?;
        match self.state {
            TransactionState::Open | TransactionState::Committing(_) => {
                self.last_activity_at = at;
                Ok(())
            }
            TransactionState::Finished(_) => Err(TransactionMutationError::Finished {
                id: self.id.clone(),
                outcome: self.state.as_str().to_string(),
            }),
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
        self.last_activity_at = at;
        progress.applying = Some(applying);
        Ok(())
    }

    pub(crate) fn complete_application(
        &mut self,
        expected_next_statement: usize,
        at: Timestamp,
        outcome_revision: u64,
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

        let completion = if let Some(error) = application_failure {
            applying.result.result.success = false;
            applying.result.result.message = error.clone();
            applying.result.result.diagnostics = vec![TransactionDiagnostic {
                message: error.clone(),
                span_start: 0,
                span_end: 0,
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
        self.last_activity_at = at;
        progress.next_statement = applying.next_statement;
        progress.results.push(applying.result);
        if let Some(outcome) = completion {
            let results = std::mem::take(&mut progress.results);
            self.finish(at, outcome_revision, outcome, results);
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
        self.finish(
            at,
            outcome_revision,
            TransactionOutcome::Committed,
            Vec::new(),
        );
        Ok(())
    }

    pub(crate) fn revert(
        &mut self,
        owner: &UserName,
        at: Timestamp,
        outcome_revision: u64,
    ) -> Result<(), TransactionMutationError> {
        self.ensure_owner(owner)?;
        if !matches!(self.state, TransactionState::Open) {
            return Err(TransactionMutationError::NotOpen {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        }
        self.finish(
            at,
            outcome_revision,
            TransactionOutcome::Reverted,
            Vec::new(),
        );
        Ok(())
    }

    pub(crate) fn expire(
        &mut self,
        at: Timestamp,
        idle_before: Timestamp,
        outcome_revision: u64,
    ) -> Result<bool, TransactionMutationError> {
        if !matches!(self.state, TransactionState::Open) {
            return Ok(false);
        }
        if self.last_activity_at > idle_before {
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
        self.last_activity_at = at;
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
        domain: DomainName,
        expected_schedule: Option<Box<DomainSchedule>>,
        schedule: Option<Box<DomainSchedule>>,
    },
    PutDomainAndSchedule {
        expected_domain: Box<DomainState>,
        expected_schedule: Option<Box<DomainSchedule>>,
        domain: Box<DomainState>,
        schedule: Option<Box<DomainSchedule>>,
    },
    StartDomain {
        domain_id: DomainName,
        expected_start_version: u64,
        start: DomainStartPoint,
        clock: Option<DomainClockState>,
        authority: Option<ClusterNodeIdentity>,
    },
    StopDomain {
        domain_id: DomainName,
        expected_start_version: u64,
    },
    CreateResourceCatalog {
        identifier: ResourceName,
    },
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

    fn transaction_with_one_statement() -> ReplicatedTransaction {
        let owner = test_owner();
        let domain = test_domain();
        let mut transaction = ReplicatedTransaction::open(
            "transaction-1".to_string(),
            domain.clone(),
            owner.clone(),
            Timestamp::from_unix_nanos(1),
        );
        transaction
            .queue(
                &owner,
                &domain,
                Timestamp::from_unix_nanos(2),
                TransactionStatement {
                    request_reference: CommandExecutionReference::parse("request.0")
                        .assured("the test command reference is an accepted literal"),
                    expected_position: 0,
                    source: "SHOW TRANSACTIONS".to_string(),
                    statement: Statement::ShowTransactions(ShowTransactions),
                },
                TransactionQueueLimits {
                    max_statements: 2,
                    max_source_bytes: 1024,
                },
            )
            .assured("the first test statement is within every admission limit");
        transaction
            .start_commit(&owner, Timestamp::from_unix_nanos(3))
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
            Timestamp::from_unix_nanos(1),
        );
        assert!(matches!(
            open.complete_application(0, Timestamp::from_unix_nanos(2), 2, None),
            Err(TransactionMutationError::NotCommitting { .. })
        ));

        let mut without_application = transaction_with_one_statement();
        assert!(matches!(
            without_application.complete_application(0, Timestamp::from_unix_nanos(4), 4, None),
            Err(TransactionMutationError::NoApplicationInProgress { statement: 0, .. })
        ));

        let mut conflicting_progress = transaction_with_one_statement();
        conflicting_progress
            .begin_application(0, Timestamp::from_unix_nanos(4), applying_step())
            .assured("the application step spans the queued test statement");
        assert!(matches!(
            conflicting_progress.complete_application(1, Timestamp::from_unix_nanos(5), 5, None,),
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
            invalid_result.complete_application(0, Timestamp::from_unix_nanos(5), 5, None),
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
            Timestamp::from_unix_nanos(1),
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
            Timestamp::from_unix_nanos(1),
        );
        empty
            .start_commit(&owner, Timestamp::from_unix_nanos(2))
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
}
