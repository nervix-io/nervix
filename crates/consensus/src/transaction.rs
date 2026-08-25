use nervix_models::{
    Domain, DomainClockState, DomainSchedule, DomainStartPoint, DomainState, Identifier, Statement,
    Timestamp,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::UserCredentials;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionStatement {
    pub source: String,
    pub statement: Statement,
    pub domain: Option<Domain>,
}

impl TransactionStatement {
    pub fn source_bytes(&self) -> u64 {
        u64::try_from(self.source.len()).unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionDiagnostic {
    pub message: String,
    pub span_start: u32,
    pub span_end: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionCommandResult {
    pub success: bool,
    pub message: String,
    pub diagnostics: Vec<TransactionDiagnostic>,
    pub already_existed: bool,
    pub results: Vec<TransactionCommandResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionStepResult {
    pub first_statement: usize,
    pub statement_count: usize,
    pub result: TransactionCommandResult,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionCommitProgress {
    pub next_statement: usize,
    pub results: Vec<TransactionStepResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionOutcome {
    Committed,
    Failed { failing_step: usize, error: String },
    Reverted,
    Expired,
}

impl TransactionOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Committed => "COMMITTED",
            Self::Failed { .. } => "FAILED",
            Self::Reverted => "REVERTED",
            Self::Expired => "EXPIRED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishedTransaction {
    pub outcome: TransactionOutcome,
    pub finished_at: Timestamp,
    pub results: Vec<TransactionStepResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Open,
    Committing(TransactionCommitProgress),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedTransaction {
    pub id: String,
    pub owner: Identifier,
    pub created_at: Timestamp,
    pub last_activity_at: Timestamp,
    pub state: TransactionState,
    pub statement_count: usize,
    pub queued_source_bytes: u64,
    pub statements: Vec<TransactionStatement>,
}

impl ReplicatedTransaction {
    pub fn open(id: String, owner: Identifier, now: Timestamp) -> Self {
        Self {
            id,
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
                .saturating_sub(progress.next_statement),
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
                .map(|result| result.statement_count)
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

    pub(crate) fn ensure_owner(&self, owner: &Identifier) -> Result<(), TransactionMutationError> {
        if &self.owner == owner {
            Ok(())
        } else {
            Err(TransactionMutationError::OwnerMismatch {
                id: self.id.clone(),
            })
        }
    }

    pub fn validate_queue_admission(
        &self,
        owner: &Identifier,
        statement: &TransactionStatement,
        max_statements: usize,
        max_source_bytes: u64,
    ) -> Result<(), TransactionMutationError> {
        self.ensure_owner(owner)?;
        if !matches!(self.state, TransactionState::Open) {
            return Err(TransactionMutationError::NotOpen {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        }
        if self.statements.len() >= max_statements {
            return Err(TransactionMutationError::StatementLimit {
                id: self.id.clone(),
                limit: max_statements,
            });
        }
        let next_source_bytes = self
            .queued_source_bytes
            .saturating_add(statement.source_bytes());
        if next_source_bytes > max_source_bytes {
            return Err(TransactionMutationError::SourceByteLimit {
                id: self.id.clone(),
                limit: max_source_bytes,
            });
        }
        Ok(())
    }

    pub(crate) fn queue(
        &mut self,
        owner: &Identifier,
        at: Timestamp,
        statement: TransactionStatement,
        max_statements: usize,
        max_source_bytes: u64,
    ) -> Result<(), TransactionMutationError> {
        self.validate_queue_admission(owner, &statement, max_statements, max_source_bytes)?;
        let next_source_bytes = self
            .queued_source_bytes
            .saturating_add(statement.source_bytes());
        self.last_activity_at = at;
        self.statement_count = self.statement_count.saturating_add(1);
        self.queued_source_bytes = next_source_bytes;
        self.statements.push(statement);
        Ok(())
    }

    pub(crate) fn start_commit(
        &mut self,
        owner: &Identifier,
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
        self.state = TransactionState::Committing(TransactionCommitProgress {
            next_statement: 0,
            results: Vec::new(),
        });
        Ok(())
    }

    pub(crate) fn touch(
        &mut self,
        owner: &Identifier,
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

    pub(crate) fn advance(
        &mut self,
        expected_next_statement: usize,
        next_statement: usize,
        at: Timestamp,
        result: TransactionStepResult,
        completion: Option<TransactionOutcome>,
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
        if next_statement <= expected_next_statement || next_statement > self.statements.len() {
            return Err(TransactionMutationError::InvalidProgress {
                id: self.id.clone(),
                next: next_statement,
                statement_count: self.statements.len(),
            });
        }
        if result.first_statement != expected_next_statement
            || result.statement_count != next_statement.saturating_sub(expected_next_statement)
        {
            return Err(TransactionMutationError::InvalidStepResult {
                id: self.id.clone(),
            });
        }
        self.last_activity_at = at;
        progress.next_statement = next_statement;
        progress.results.push(result);
        if let Some(outcome) = completion {
            let results = std::mem::take(&mut progress.results);
            self.finish(at, outcome, results);
        }
        Ok(())
    }

    pub(crate) fn finish_empty_commit(
        &mut self,
        at: Timestamp,
    ) -> Result<(), TransactionMutationError> {
        let TransactionState::Committing(progress) = &mut self.state else {
            return Err(TransactionMutationError::NotCommitting {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        };
        if !self.statements.is_empty() || progress.next_statement != 0 {
            return Err(TransactionMutationError::InvalidProgress {
                id: self.id.clone(),
                next: progress.next_statement,
                statement_count: self.statements.len(),
            });
        }
        self.finish(at, TransactionOutcome::Committed, Vec::new());
        Ok(())
    }

    pub(crate) fn revert(
        &mut self,
        owner: &Identifier,
        at: Timestamp,
    ) -> Result<(), TransactionMutationError> {
        self.ensure_owner(owner)?;
        if !matches!(self.state, TransactionState::Open) {
            return Err(TransactionMutationError::NotOpen {
                id: self.id.clone(),
                state: self.state.as_str().to_string(),
            });
        }
        self.finish(at, TransactionOutcome::Reverted, Vec::new());
        Ok(())
    }

    pub(crate) fn expire(
        &mut self,
        at: Timestamp,
        idle_before: Timestamp,
    ) -> Result<bool, TransactionMutationError> {
        if !matches!(self.state, TransactionState::Open) {
            return Ok(false);
        }
        if self.last_activity_at > idle_before {
            return Ok(false);
        }
        self.finish(at, TransactionOutcome::Expired, Vec::new());
        Ok(true)
    }

    fn finish(
        &mut self,
        at: Timestamp,
        outcome: TransactionOutcome,
        results: Vec<TransactionStepResult>,
    ) {
        self.last_activity_at = at;
        self.statements.clear();
        self.queued_source_bytes = 0;
        self.state = TransactionState::Finished(FinishedTransaction {
            outcome,
            finished_at: at,
            results,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionStepEffect {
    ReplaceDomainSchedule {
        domain: Domain,
        expected_schedule: Option<Box<DomainSchedule>>,
        schedule: Option<Box<DomainSchedule>>,
    },
    PutDomainAndSchedule {
        expected_domain: Box<DomainState>,
        expected_schedule: Option<Box<DomainSchedule>>,
        domain: Box<DomainState>,
        schedule: Option<Box<DomainSchedule>>,
    },
    PutDomain {
        domain: Box<DomainState>,
    },
    StartDomain {
        domain_id: Domain,
        expected_start_version: u64,
        start: DomainStartPoint,
        clock: Option<DomainClockState>,
    },
    StopDomain {
        domain_id: Domain,
        expected_start_version: u64,
    },
    CreateUser {
        user: Box<UserCredentials>,
    },
    CreateResourceCatalog {
        identifier: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionMutationResponse {
    pub result: Result<ReplicatedTransaction, TransactionMutationError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum TransactionMutationError {
    #[error("transaction '{id}' already exists")]
    AlreadyExists { id: String },
    #[error("transaction '{id}' is unknown")]
    Unknown { id: String },
    #[error("transaction '{id}' belongs to another user")]
    OwnerMismatch { id: String },
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
    #[error("transaction '{id}' commit step effect does not match its queued statement(s)")]
    EffectMismatch { id: String },
    #[error("transaction '{id}' commit step conflicted with replicated state: {reason}")]
    StepConflict { id: String, reason: String },
}
