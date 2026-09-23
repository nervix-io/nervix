//! Durable ownership and terminal outcomes for admitted administrative commands.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Replicated command identities, semantic request digests, and retained outcomes.
//! - **Depends on.** Vocabulary identities and timestamps.
//! - **Must not know.** NSPL parsing, sessions, runtime activation, or transport responses.

use std::{collections::BTreeMap, io, time::Duration};

use error_stack::Report;
use imbl::OrdSet;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClusterNodeIdentity, CommandExecutionReference, CommandExecutionReferenceTimestampError,
    DomainName, DomainState, Statement, Timestamp, TransactionOperationAdmission,
    TransactionPosition, TransactionPreviewIdentity, UserName,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{
    DomainMutationLease, TransactionActivity, TransactionStatementRequest,
    durable_batch::DurableBatch, records::Records,
};

/// Client clocks may lead the serving node by this much when the reference is first admitted.
/// Future-dated references remain represented by a compact tombstone until the retry fence passes
/// their embedded time, so this allowance cannot reopen a reclaimed identity.
const COMMAND_EXECUTION_REFERENCE_FUTURE_SKEW: Duration = Duration::from_secs(5 * 60);

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecutionDiagnostic {
    pub message: String,
    pub span_start: u32,
    pub span_end: u32,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecutionTransactionStatus {
    pub id: String,
    pub domain: String,
    pub state: i32,
    pub pending_count: u64,
    pub completed_count: u64,
    pub total_count: u64,
    pub error: String,
    pub failing_step: Option<u64>,
}

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
pub enum CommandExecutionResultKind {
    Ok,
    Error,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecutionChildResult {
    pub success: bool,
    pub kind: CommandExecutionResultKind,
    pub message: String,
    pub diagnostics: Vec<CommandExecutionDiagnostic>,
    pub already_existed: bool,
    pub transaction_admission: Option<TransactionOperationAdmission>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecutionResult {
    pub success: bool,
    pub kind: CommandExecutionResultKind,
    pub message: String,
    pub diagnostics: Vec<CommandExecutionDiagnostic>,
    pub already_existed: bool,
    pub results: Vec<CommandExecutionChildResult>,
    pub transaction: Option<CommandExecutionTransactionStatus>,
    pub transaction_admission: Option<TransactionOperationAdmission>,
    /// The two previews a refused commit reported, so a recovered outcome still says the
    /// transaction moved rather than only that the commit failed.
    pub preview_stale: Option<CommandExecutionPreviewStale>,
}

/// The preview a refused commit expected, beside the one that now describes the transaction.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecutionPreviewStale {
    pub expected: TransactionPreviewIdentity,
    pub current: TransactionPreviewIdentity,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum CommandExecutionTransactionTarget {
    New {
        id: String,
        activity: TransactionActivity,
    },
    Existing {
        id: String,
        activity: TransactionActivity,
    },
}

impl CommandExecutionTransactionTarget {
    pub fn id(&self) -> &str {
        match self {
            Self::New { id, .. } | Self::Existing { id, .. } => id,
        }
    }

    pub fn activity(&self) -> TransactionActivity {
        match self {
            Self::New { activity, .. } | Self::Existing { activity, .. } => *activity,
        }
    }

    pub fn opens_transaction(&self) -> bool {
        matches!(self, Self::New { .. })
    }

    pub fn identifies_same_request(&self, requested: &Self) -> bool {
        match (self, requested) {
            (Self::New { .. }, Self::New { .. }) => true,
            (Self::Existing { id, .. }, Self::Existing { id: requested, .. }) => id == requested,
            (Self::New { .. }, Self::Existing { .. })
            | (Self::Existing { .. }, Self::New { .. }) => false,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum CommandExecutionTransactionOperation {
    Queue(Box<TransactionStatementRequest>),
    Commit {
        /// The whole-transaction preview the requesting client expected this commit to apply.
        /// Absent when the request itself opened the transaction or appended to it, because
        /// either moves the transaction past the preview the client was holding.
        expected_preview: Option<TransactionPreviewIdentity>,
    },
    Revert,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecutionTransactionRequest {
    pub target: CommandExecutionTransactionTarget,
    pub operations: Vec<CommandExecutionTransactionOperation>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum CommandExecutionEffect {
    CreateDomain {
        if_not_exists: bool,
        existed_at_admission: bool,
        state: Box<DomainState>,
    },
    Transaction {
        transaction_id: String,
        source: String,
        statement: Box<Statement>,
    },
    TransactionRequest(Box<CommandExecutionTransactionRequest>),
    Statement {
        source: String,
        statement: Box<Statement>,
    },
    CreateUser {
        if_not_exists: bool,
        name: UserName,
        password_hash: String,
    },
    DropNode {
        identity: ClusterNodeIdentity,
        member_at_admission: bool,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum CommandExecutionRequestEvidence {
    Statement,
    UserCredentials {
        password_hash: String,
    },
    Transaction {
        target: CommandExecutionTransactionTarget,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecutionRequestIdentity {
    owner: UserName,
    domain: Option<DomainName>,
    expected_transaction_position: Option<TransactionPosition>,
    request_digest: [u8; 32],
    evidence: CommandExecutionRequestEvidence,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum CommandExecutionState {
    Applying {
        owner: UserName,
        domain: Option<DomainName>,
        expected_transaction_position: Option<TransactionPosition>,
        request_digest: [u8; 32],
        admitted_at: Timestamp,
        effect: Box<CommandExecutionEffect>,
        domain_mutations: BTreeMap<DomainName, DomainMutationLease>,
    },
    Finished {
        request: Box<CommandExecutionRequestIdentity>,
        outcome_revision: u64,
        finished_at: Timestamp,
        result: Box<CommandExecutionResult>,
    },
    Expired,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecution {
    pub reference: CommandExecutionReference,
    pub state: CommandExecutionState,
}

impl CommandExecution {
    pub fn applying(
        reference: CommandExecutionReference,
        owner: UserName,
        domain: Option<DomainName>,
        request_digest: [u8; 32],
        admitted_at: Timestamp,
        effect: CommandExecutionEffect,
    ) -> Self {
        Self::applying_at_position(
            reference,
            owner,
            domain,
            None,
            request_digest,
            admitted_at,
            effect,
        )
    }

    pub fn applying_at_position(
        reference: CommandExecutionReference,
        owner: UserName,
        domain: Option<DomainName>,
        expected_transaction_position: Option<TransactionPosition>,
        request_digest: [u8; 32],
        admitted_at: Timestamp,
        effect: CommandExecutionEffect,
    ) -> Self {
        Self {
            reference,
            state: CommandExecutionState::Applying {
                owner,
                domain,
                expected_transaction_position,
                request_digest,
                admitted_at,
                effect: Box::new(effect),
                domain_mutations: BTreeMap::new(),
            },
        }
    }

    pub fn owner(&self) -> Option<&UserName> {
        match &self.state {
            CommandExecutionState::Applying { owner, .. } => Some(owner),
            CommandExecutionState::Finished { request, .. } => Some(&request.owner),
            CommandExecutionState::Expired => None,
        }
    }

    pub fn domain(&self) -> Option<&DomainName> {
        match &self.state {
            CommandExecutionState::Applying { domain, .. } => domain.as_ref(),
            CommandExecutionState::Finished { request, .. } => request.domain.as_ref(),
            CommandExecutionState::Expired => None,
        }
    }

    pub fn expected_transaction_position(&self) -> Option<TransactionPosition> {
        match &self.state {
            CommandExecutionState::Applying {
                expected_transaction_position,
                ..
            } => *expected_transaction_position,
            CommandExecutionState::Finished { request, .. } => {
                request.expected_transaction_position
            }
            CommandExecutionState::Expired => None,
        }
    }

    pub fn request_digest(&self) -> Option<[u8; 32]> {
        match &self.state {
            CommandExecutionState::Applying { request_digest, .. } => Some(*request_digest),
            CommandExecutionState::Finished { request, .. } => Some(request.request_digest),
            CommandExecutionState::Expired => None,
        }
    }

    pub fn effect(&self) -> Option<&CommandExecutionEffect> {
        match &self.state {
            CommandExecutionState::Applying { effect, .. } => Some(effect),
            CommandExecutionState::Finished { .. } | CommandExecutionState::Expired => None,
        }
    }

    pub fn is_applying(&self) -> bool {
        matches!(&self.state, CommandExecutionState::Applying { .. })
    }

    pub fn is_expired(&self) -> bool {
        matches!(&self.state, CommandExecutionState::Expired)
    }

    pub fn domain_mutation(&self, domain: &DomainName) -> Option<&DomainMutationLease> {
        let CommandExecutionState::Applying {
            domain_mutations, ..
        } = &self.state
        else {
            return None;
        };
        domain_mutations.get(domain)
    }

    pub fn domain_mutations(&self) -> impl Iterator<Item = (&DomainName, &DomainMutationLease)> {
        let mutations = match &self.state {
            CommandExecutionState::Applying {
                domain_mutations, ..
            } => Some(domain_mutations),
            CommandExecutionState::Finished { .. } | CommandExecutionState::Expired => None,
        };
        mutations.into_iter().flat_map(BTreeMap::iter)
    }

    pub(crate) fn bind_domain_mutation(&mut self, domain: DomainName, lease: DomainMutationLease) {
        let CommandExecutionState::Applying {
            domain_mutations, ..
        } = &mut self.state
        else {
            return;
        };
        domain_mutations.insert(domain, lease);
    }

    pub fn transaction_request(&self) -> Option<&CommandExecutionTransactionRequest> {
        match self.effect() {
            Some(CommandExecutionEffect::TransactionRequest(request)) => Some(request),
            Some(
                CommandExecutionEffect::CreateDomain { .. }
                | CommandExecutionEffect::Transaction { .. }
                | CommandExecutionEffect::Statement { .. }
                | CommandExecutionEffect::CreateUser { .. }
                | CommandExecutionEffect::DropNode { .. },
            )
            | None => None,
        }
    }

    pub fn transaction_target(&self) -> Option<&CommandExecutionTransactionTarget> {
        match &self.state {
            CommandExecutionState::Applying { effect, .. } => match effect.as_ref() {
                CommandExecutionEffect::TransactionRequest(request) => Some(&request.target),
                CommandExecutionEffect::CreateDomain { .. }
                | CommandExecutionEffect::Transaction { .. }
                | CommandExecutionEffect::Statement { .. }
                | CommandExecutionEffect::CreateUser { .. }
                | CommandExecutionEffect::DropNode { .. } => None,
            },
            CommandExecutionState::Finished { request, .. } => match &request.evidence {
                CommandExecutionRequestEvidence::Transaction { target } => Some(target),
                CommandExecutionRequestEvidence::Statement
                | CommandExecutionRequestEvidence::UserCredentials { .. } => None,
            },
            CommandExecutionState::Expired => None,
        }
    }

    pub fn password_hash(&self) -> Option<&str> {
        match &self.state {
            CommandExecutionState::Applying { effect, .. } => match effect.as_ref() {
                CommandExecutionEffect::CreateUser { password_hash, .. } => Some(password_hash),
                CommandExecutionEffect::CreateDomain { .. }
                | CommandExecutionEffect::Transaction { .. }
                | CommandExecutionEffect::TransactionRequest(_)
                | CommandExecutionEffect::Statement { .. }
                | CommandExecutionEffect::DropNode { .. } => None,
            },
            CommandExecutionState::Finished { request, .. } => match &request.evidence {
                CommandExecutionRequestEvidence::UserCredentials { password_hash } => {
                    Some(password_hash)
                }
                CommandExecutionRequestEvidence::Statement
                | CommandExecutionRequestEvidence::Transaction { .. } => None,
            },
            CommandExecutionState::Expired => None,
        }
    }

    pub fn request_conflict(
        &self,
        owner: &UserName,
        domain: Option<&DomainName>,
        expected_transaction_position: Option<TransactionPosition>,
        request_digest: [u8; 32],
    ) -> Option<CommandExecutionRequestConflict> {
        let existing_owner = self.owner()?;
        if existing_owner != owner {
            return Some(CommandExecutionRequestConflict::Owner);
        }
        if self.domain() != domain {
            return Some(CommandExecutionRequestConflict::Domain);
        }
        if self.expected_transaction_position() != expected_transaction_position {
            return Some(CommandExecutionRequestConflict::Position);
        }
        if self.request_digest() != Some(request_digest) {
            return Some(CommandExecutionRequestConflict::Content);
        }
        None
    }

    pub(crate) fn same_request(&self, requested: &Self) -> bool {
        if self.is_expired()
            || requested.is_expired()
            || self.reference != requested.reference
            || self
                .request_conflict(
                    requested
                        .owner()
                        .verified("a requested command execution is applying"),
                    requested.domain(),
                    requested.expected_transaction_position(),
                    requested
                        .request_digest()
                        .verified("a requested command execution is applying"),
                )
                .is_some()
        {
            return false;
        }
        match (self.transaction_target(), requested.transaction_target()) {
            (Some(existing), Some(requested)) => existing.identifies_same_request(requested),
            (None, None) => true,
            (Some(_), None) | (None, Some(_)) => false,
        }
    }

    pub(crate) fn into_finished(
        self,
        outcome_revision: u64,
        finished_at: Timestamp,
        result: Box<CommandExecutionResult>,
    ) -> Option<Self> {
        let Self { reference, state } = self;
        let CommandExecutionState::Applying {
            owner,
            domain,
            expected_transaction_position,
            request_digest,
            effect,
            ..
        } = state
        else {
            return None;
        };
        let evidence = match effect.as_ref() {
            CommandExecutionEffect::CreateUser { password_hash, .. } => {
                CommandExecutionRequestEvidence::UserCredentials {
                    password_hash: password_hash.clone(),
                }
            }
            CommandExecutionEffect::TransactionRequest(request) => {
                CommandExecutionRequestEvidence::Transaction {
                    target: request.target.clone(),
                }
            }
            CommandExecutionEffect::CreateDomain { .. }
            | CommandExecutionEffect::Transaction { .. }
            | CommandExecutionEffect::Statement { .. }
            | CommandExecutionEffect::DropNode { .. } => CommandExecutionRequestEvidence::Statement,
        };
        Some(Self {
            reference,
            state: CommandExecutionState::Finished {
                request: Box::new(CommandExecutionRequestIdentity {
                    owner,
                    domain,
                    expected_transaction_position,
                    request_digest,
                    evidence,
                }),
                outcome_revision,
                finished_at,
                result,
            },
        })
    }

    fn into_expired(self) -> Option<Self> {
        let Self { reference, state } = self;
        let CommandExecutionState::Finished { .. } = state else {
            return None;
        };
        Some(Self {
            reference,
            state: CommandExecutionState::Expired,
        })
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecutionAdmissionPolicy {
    retry_fence: Timestamp,
    latest_reference: Timestamp,
    capacity: u64,
}

impl CommandExecutionAdmissionPolicy {
    pub fn at(now: Timestamp, retry_validity: Duration, capacity: usize) -> Self {
        let retry_fence = match now.checked_sub(retry_validity) {
            Ok(retry_fence) => retry_fence,
            Err(_) => Timestamp::from_unix_nanos(i64::MIN),
        };
        let latest_reference = match now.checked_add(COMMAND_EXECUTION_REFERENCE_FUTURE_SKEW) {
            Ok(latest_reference) => latest_reference,
            Err(_) => Timestamp::from_unix_nanos(i64::MAX),
        };
        Self {
            retry_fence,
            latest_reference,
            capacity: u64::try_from(capacity)
                .assured("supported targets have a pointer width no larger than u64"),
        }
    }

    pub fn retry_fence(&self) -> Timestamp {
        self.retry_fence
    }

    /// Checks a new identity against the retry fence in force, which is the later of this
    /// policy's fence and the durable one.
    fn validate_reference(
        &self,
        reference: &CommandExecutionReference,
        fence: Timestamp,
    ) -> error_stack::Result<Timestamp, CommandExecutionAdmissionError> {
        let issued_at = match reference.retry_issued_at() {
            Ok(issued_at) => issued_at,
            Err(error) => {
                let refusal = match error.current_context() {
                    CommandExecutionReferenceTimestampError::NotUuidV7 => {
                        CommandExecutionAdmissionError::MissingTimestamp
                    }
                    // A UUIDv7 creation time counts milliseconds after the Unix epoch, so one
                    // beyond the representable range lies centuries ahead of any serving clock.
                    CommandExecutionReferenceTimestampError::OutsideTimestampRange => {
                        CommandExecutionAdmissionError::Future
                    }
                };
                return Err(error.change_context(refusal));
            }
        };
        if issued_at <= fence {
            return Err(Report::new(CommandExecutionAdmissionError::Expired));
        }
        if issued_at > self.latest_reference {
            return Err(Report::new(CommandExecutionAdmissionError::Future));
        }
        Ok(issued_at)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CommandExecutionAdmissionError {
    #[error("has expired")]
    Expired,
    #[error("does not carry a UUID version 7 creation timestamp")]
    MissingTimestamp,
    #[error("has a creation timestamp beyond the accepted client clock-skew boundary")]
    Future,
    #[error("cannot be admitted because the command execution capacity of {capacity} is full")]
    Capacity { capacity: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FinishedCommandExecutionKey {
    finished_at: Timestamp,
    reference: CommandExecutionReference,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExpiredCommandExecutionKey {
    issued_at: Timestamp,
    reference: CommandExecutionReference,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommandExecutionRecords {
    entries: Records<CommandExecutionReference, CommandExecution>,
    applying: OrdSet<CommandExecutionReference>,
    finished: OrdSet<FinishedCommandExecutionKey>,
    expired: OrdSet<ExpiredCommandExecutionKey>,
    retry_fence: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandExecutionReconciliation {
    pub applying: Vec<CommandExecutionReference>,
    pub maintenance_due: bool,
}

impl CommandExecutionRecords {
    pub(crate) fn load(
        keyspace: &fjall::Keyspace,
        retry_fence: Option<Timestamp>,
    ) -> io::Result<Self> {
        let entries: Records<CommandExecutionReference, CommandExecution> =
            Records::load(b'e', keyspace)?;
        let mut applying = OrdSet::new();
        let mut finished = OrdSet::new();
        let mut expired = OrdSet::new();
        for execution in entries.values() {
            match &execution.state {
                CommandExecutionState::Applying { .. } => {
                    applying.insert(execution.reference.clone());
                }
                CommandExecutionState::Finished { finished_at, .. } => {
                    finished.insert(FinishedCommandExecutionKey {
                        finished_at: *finished_at,
                        reference: execution.reference.clone(),
                    });
                }
                CommandExecutionState::Expired => {
                    let issued_at = execution
                        .reference
                        .retry_issued_at()
                        .map_err(io::Error::other)?;
                    expired.insert(ExpiredCommandExecutionKey {
                        issued_at,
                        reference: execution.reference.clone(),
                    });
                }
            }
        }
        Ok(Self {
            entries,
            applying,
            finished,
            expired,
            retry_fence,
        })
    }

    pub(crate) fn write_changes(
        &self,
        preceding: &Self,
        batch: &mut DurableBatch<'_>,
        keyspace: &fjall::Keyspace,
    ) -> io::Result<()> {
        self.entries
            .write_changes(&preceding.entries, b'e', batch, keyspace)
    }

    pub(crate) fn retry_fence(&self) -> Option<Timestamp> {
        self.retry_fence
    }

    pub(crate) fn get(&self, reference: &CommandExecutionReference) -> Option<&CommandExecution> {
        self.entries.get(reference)
    }

    #[cfg(test)]
    pub(crate) fn contains_key(&self, reference: &CommandExecutionReference) -> bool {
        self.entries.contains_key(reference)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn validate_admission(
        &self,
        reference: &CommandExecutionReference,
        policy: &CommandExecutionAdmissionPolicy,
    ) -> error_stack::Result<(), CommandExecutionAdmissionError> {
        let fence = self.effective_retry_fence(policy.retry_fence);
        policy.validate_reference(reference, fence)?;
        let retained = u64::try_from(self.entries.len())
            .assured("supported targets have a pointer width no larger than u64");
        if retained >= policy.capacity {
            return Err(Report::new(CommandExecutionAdmissionError::Capacity {
                capacity: policy.capacity,
            }));
        }
        Ok(())
    }

    pub(crate) fn insert_admitted(
        &mut self,
        execution: CommandExecution,
        policy: &CommandExecutionAdmissionPolicy,
    ) {
        self.retry_fence = Some(self.effective_retry_fence(policy.retry_fence));
        self.applying.insert(execution.reference.clone());
        self.entries.insert(execution.reference.clone(), execution);
    }

    pub(crate) fn replace(&mut self, execution: CommandExecution) {
        let reference = execution.reference.clone();
        let preceding = self
            .entries
            .get(&reference)
            .cloned()
            .verified("a command execution is replaced only after it was read from this set");
        self.remove_index(&preceding);
        self.insert_index(&execution);
        self.entries.insert(reference, execution);
    }

    /// The fence a reclamation applies: the durable fence never moves back, so a leader whose clock
    /// trails the one that advanced it still rejects every identity already reclaimed.
    fn effective_retry_fence(&self, retry_fence: Timestamp) -> Timestamp {
        match self.retry_fence {
            Some(current) => current.max(retry_fence),
            None => retry_fence,
        }
    }

    pub(crate) fn reconciliation(
        &self,
        finished_before: Timestamp,
        retry_fence: Timestamp,
    ) -> CommandExecutionReconciliation {
        let retry_fence = self.effective_retry_fence(retry_fence);
        let finished_due = self
            .finished
            .iter()
            .next()
            .is_some_and(|key| key.finished_at <= finished_before);
        let expired_due = self
            .expired
            .iter()
            .next()
            .is_some_and(|key| key.issued_at <= retry_fence);
        CommandExecutionReconciliation {
            applying: self.applying.iter().cloned().collect(),
            maintenance_due: finished_due || expired_due,
        }
    }

    pub(crate) fn reclaim(&mut self, finished_before: Timestamp, retry_fence: Timestamp) {
        let retry_fence = self.effective_retry_fence(retry_fence);
        self.retry_fence = Some(retry_fence);

        let expired = self
            .expired
            .iter()
            .take_while(|key| key.issued_at <= retry_fence)
            .cloned()
            .collect::<Vec<_>>();
        for key in expired {
            self.expired.remove(&key);
            self.entries.remove(&key.reference);
        }

        let finished = self
            .finished
            .iter()
            .take_while(|key| key.finished_at <= finished_before)
            .cloned()
            .collect::<Vec<_>>();
        for key in finished {
            self.finished.remove(&key);
            let execution = self
                .entries
                .get(&key.reference)
                .cloned()
                .verified("the due index names a retained command execution");
            let issued_at = execution
                .reference
                .retry_issued_at()
                .assured("admission accepted only UUIDv7 command retry identities");
            if issued_at <= retry_fence {
                self.entries.remove(&key.reference);
                continue;
            }
            let expired = execution
                .into_expired()
                .verified("the finished index contains only finished command executions");
            self.expired.insert(ExpiredCommandExecutionKey {
                issued_at,
                reference: key.reference.clone(),
            });
            self.entries.insert(key.reference, expired);
        }
    }

    fn remove_index(&mut self, execution: &CommandExecution) {
        match &execution.state {
            CommandExecutionState::Applying { .. } => {
                self.applying.remove(&execution.reference);
            }
            CommandExecutionState::Finished { finished_at, .. } => {
                self.finished.remove(&FinishedCommandExecutionKey {
                    finished_at: *finished_at,
                    reference: execution.reference.clone(),
                });
            }
            CommandExecutionState::Expired => {
                let issued_at = execution
                    .reference
                    .retry_issued_at()
                    .assured("admission accepted only UUIDv7 command retry identities");
                self.expired.remove(&ExpiredCommandExecutionKey {
                    issued_at,
                    reference: execution.reference.clone(),
                });
            }
        }
    }

    fn insert_index(&mut self, execution: &CommandExecution) {
        match &execution.state {
            CommandExecutionState::Applying { .. } => {
                self.applying.insert(execution.reference.clone());
            }
            CommandExecutionState::Finished { finished_at, .. } => {
                self.finished.insert(FinishedCommandExecutionKey {
                    finished_at: *finished_at,
                    reference: execution.reference.clone(),
                });
            }
            CommandExecutionState::Expired => {
                let issued_at = execution
                    .reference
                    .retry_issued_at()
                    .assured("admission accepted only UUIDv7 command retry identities");
                self.expired.insert(ExpiredCommandExecutionKey {
                    issued_at,
                    reference: execution.reference.clone(),
                });
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandExecutionRequestConflict {
    Content,
    Domain,
    Owner,
    Position,
}

impl std::fmt::Display for CommandExecutionRequestConflict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Content => formatter.write_str("content"),
            Self::Domain => formatter.write_str("domain"),
            Self::Owner => formatter.write_str("owner"),
            Self::Position => formatter.write_str("position"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(index: u64) -> CommandExecutionReference {
        CommandExecutionReference::parse(format!("018bcfe5-6800-7000-8000-{index:012x}"))
            .assured("the test command reference is a UUIDv7 value")
    }

    fn policy(capacity: usize) -> CommandExecutionAdmissionPolicy {
        CommandExecutionAdmissionPolicy::at(
            Timestamp::from_unix_nanos(1_700_000_010_000_000_000),
            Duration::from_secs(60),
            capacity,
        )
    }

    fn execution(index: u64) -> CommandExecution {
        CommandExecution::applying(
            reference(index),
            UserName::parse("operator").assured("the test owner is a valid user name"),
            None,
            [u8::try_from(index).assured("the test index fits in one byte"); 32],
            Timestamp::from_unix_nanos(1_700_000_010_000_000_000),
            CommandExecutionEffect::CreateUser {
                if_not_exists: false,
                name: UserName::parse("created_user")
                    .assured("the test target is a valid user name"),
                password_hash: "argon2-hash".to_string(),
            },
        )
    }

    fn result() -> Box<CommandExecutionResult> {
        Box::new(CommandExecutionResult {
            success: true,
            kind: CommandExecutionResultKind::Ok,
            message: "created".to_string(),
            diagnostics: Vec::new(),
            already_existed: false,
            results: Vec::new(),
            transaction: None,
            transaction_admission: None,
            preview_stale: None,
        })
    }

    #[test]
    fn reconciliation_indexes_only_applying_and_due_executions() {
        let policy = policy(10);
        let applying = execution(1);
        let finishing = execution(2);
        let applying_reference = applying.reference.clone();
        let finished_reference = finishing.reference.clone();
        let mut records = CommandExecutionRecords::default();
        records.insert_admitted(applying, &policy);
        records.insert_admitted(finishing.clone(), &policy);
        let finished = finishing
            .into_finished(
                7,
                Timestamp::from_unix_nanos(1_700_000_011_000_000_000),
                result(),
            )
            .verified("the fixture execution is applying");
        records.replace(finished);

        let current = records.reconciliation(
            Timestamp::from_unix_nanos(1_700_000_010_000_000_000),
            Timestamp::from_unix_nanos(1_699_999_999_000_000_000),
        );
        assert_eq!(current.applying, vec![applying_reference.clone()]);
        assert!(!current.maintenance_due);

        let finished_before = Timestamp::from_unix_nanos(1_700_000_011_000_000_000);
        let before_identity_fence = Timestamp::from_unix_nanos(1_699_999_999_000_000_000);
        assert!(
            records
                .reconciliation(finished_before, before_identity_fence)
                .maintenance_due
        );
        records.reclaim(finished_before, before_identity_fence);
        assert!(
            records
                .get(&finished_reference)
                .is_some_and(CommandExecution::is_expired)
        );
        assert!(
            !records
                .reconciliation(finished_before, before_identity_fence)
                .maintenance_due
        );

        let identity_fence = Timestamp::from_unix_nanos(1_700_000_000_000_000_000);
        assert!(
            records
                .reconciliation(finished_before, identity_fence)
                .maintenance_due
        );
        records.reclaim(finished_before, identity_fence);
        assert!(records.get(&finished_reference).is_none());
        assert!(records.get(&applying_reference).is_some());
    }

    /// A UUIDv7 retry identity whose embedded creation time is `unix_millis`.
    fn reference_issued_at(unix_millis: u64) -> CommandExecutionReference {
        let high = unix_millis >> 16;
        let low = unix_millis & 0xffff;
        CommandExecutionReference::parse(format!("{high:08x}-{low:04x}-7000-8000-000000000001"))
            .assured("the formatted identity uses only execution-reference characters")
    }

    fn admission_error(
        records: &CommandExecutionRecords,
        reference: &CommandExecutionReference,
        policy: &CommandExecutionAdmissionPolicy,
    ) -> CommandExecutionAdmissionError {
        let Err(error) = records.validate_admission(reference, policy) else {
            panic!("the identity '{reference}' must be refused");
        };
        *error.current_context()
    }

    #[test]
    fn admission_accepts_only_identities_created_inside_the_retry_window() {
        // The policy is taken at 1_700_000_010 s with a 60 s retry validity, so its fence sits at
        // 1_699_999_950 s and a creation time may lead it by at most five minutes.
        let policy = policy(10);
        let records = CommandExecutionRecords::default();

        let current = reference_issued_at(1_700_000_005_000);
        records
            .validate_admission(&current, &policy)
            .assured("an identity created five seconds ago is inside the retry window");
        let skewed = reference_issued_at(1_700_000_250_000);
        records
            .validate_admission(&skewed, &policy)
            .assured("a creation time four minutes ahead is inside the clock-skew allowance");

        let at_fence = reference_issued_at(1_699_999_950_000);
        assert_eq!(
            admission_error(&records, &at_fence, &policy),
            CommandExecutionAdmissionError::Expired
        );
        let stale = reference_issued_at(1_699_999_000_000);
        assert_eq!(
            admission_error(&records, &stale, &policy),
            CommandExecutionAdmissionError::Expired
        );
        let future = reference_issued_at(1_700_000_311_000);
        assert_eq!(
            admission_error(&records, &future, &policy),
            CommandExecutionAdmissionError::Future
        );
        let random = CommandExecutionReference::parse("0f0a4bd6-3f1e-4c8a-9d51-2f64a1b0c7de")
            .assured("a random UUID uses only execution-reference characters");
        assert_eq!(
            admission_error(&records, &random, &policy),
            CommandExecutionAdmissionError::MissingTimestamp
        );
    }

    #[test]
    fn a_trailing_leader_clock_cannot_readmit_a_reclaimed_identity() {
        let mut records = CommandExecutionRecords::default();
        let reclaimed = reference_issued_at(1_700_000_005_000);
        records.reclaim(
            Timestamp::from_unix_nanos(1_700_000_020_000_000_000),
            Timestamp::from_unix_nanos(1_700_000_020_000_000_000),
        );

        // This leader's own fence is 1_699_999_950 s, well before the identity was created, but
        // the durable fence already passed it.
        let trailing = policy(10);
        assert_eq!(
            admission_error(&records, &reclaimed, &trailing),
            CommandExecutionAdmissionError::Expired
        );

        records.reclaim(
            Timestamp::from_unix_nanos(1_700_000_010_000_000_000),
            Timestamp::from_unix_nanos(1_700_000_010_000_000_000),
        );
        assert_eq!(
            records.retry_fence(),
            Some(Timestamp::from_unix_nanos(1_700_000_020_000_000_000)),
            "a reclamation from a trailing clock must not move the durable fence back"
        );
    }

    #[test]
    fn admission_capacity_never_evicts_an_active_execution() {
        let policy = policy(1);
        let first = execution(1);
        let second = execution(2);
        let first_reference = first.reference.clone();
        let mut records = CommandExecutionRecords::default();
        records
            .validate_admission(&first.reference, &policy)
            .assured("the first execution fits the configured capacity");
        records.insert_admitted(first, &policy);

        let Err(error) = records.validate_admission(&second.reference, &policy) else {
            panic!("a second execution must exceed the configured capacity");
        };
        assert_eq!(
            error.current_context(),
            &CommandExecutionAdmissionError::Capacity { capacity: 1 }
        );
        assert_eq!(records.len(), 1);
        assert!(records.get(&first_reference).is_some());
    }
}
