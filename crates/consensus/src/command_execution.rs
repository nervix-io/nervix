//! Durable ownership and terminal outcomes for admitted administrative commands.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Replicated command identities, semantic request digests, and retained outcomes.
//! - **Depends on.** Vocabulary identities and timestamps.
//! - **Must not know.** NSPL parsing, sessions, runtime activation, or transport responses.

use std::collections::BTreeMap;

use nervix_models::{
    ClusterNodeIdentity, CommandExecutionReference, DomainName, DomainState, Statement, Timestamp,
    TransactionOperationAdmission, TransactionPosition, TransactionPreviewIdentity, UserName,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{DomainMutationLease, TransactionActivity, TransactionStatementRequest};

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
pub enum CommandExecutionState {
    Applying,
    Finished {
        outcome_revision: u64,
        finished_at: Timestamp,
        result: Box<CommandExecutionResult>,
    },
    Expired {
        expired_at: Timestamp,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommandExecution {
    pub reference: CommandExecutionReference,
    pub owner: UserName,
    pub domain: Option<DomainName>,
    pub expected_transaction_position: Option<TransactionPosition>,
    pub request_digest: [u8; 32],
    pub admitted_at: Timestamp,
    pub effect: CommandExecutionEffect,
    pub state: CommandExecutionState,
    domain_mutations: BTreeMap<DomainName, DomainMutationLease>,
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
            owner,
            domain,
            expected_transaction_position,
            request_digest,
            admitted_at,
            effect,
            state: CommandExecutionState::Applying,
            domain_mutations: BTreeMap::new(),
        }
    }

    pub fn domain_mutation(&self, domain: &DomainName) -> Option<&DomainMutationLease> {
        self.domain_mutations.get(domain)
    }

    pub fn domain_mutations(&self) -> impl Iterator<Item = (&DomainName, &DomainMutationLease)> {
        self.domain_mutations.iter()
    }

    pub(crate) fn bind_domain_mutation(&mut self, domain: DomainName, lease: DomainMutationLease) {
        self.domain_mutations.insert(domain, lease);
    }

    pub fn transaction_request(&self) -> Option<&CommandExecutionTransactionRequest> {
        match &self.effect {
            CommandExecutionEffect::TransactionRequest(request) => Some(request),
            CommandExecutionEffect::CreateDomain { .. }
            | CommandExecutionEffect::Transaction { .. }
            | CommandExecutionEffect::Statement { .. }
            | CommandExecutionEffect::CreateUser { .. }
            | CommandExecutionEffect::DropNode { .. } => None,
        }
    }

    pub fn request_conflict(
        &self,
        owner: &UserName,
        domain: Option<&DomainName>,
        expected_transaction_position: Option<TransactionPosition>,
        request_digest: [u8; 32],
    ) -> Option<CommandExecutionRequestConflict> {
        if &self.owner != owner {
            return Some(CommandExecutionRequestConflict::Owner);
        }
        if self.domain.as_ref() != domain {
            return Some(CommandExecutionRequestConflict::Domain);
        }
        if self.expected_transaction_position != expected_transaction_position {
            return Some(CommandExecutionRequestConflict::Position);
        }
        if self.request_digest != request_digest {
            return Some(CommandExecutionRequestConflict::Content);
        }
        None
    }

    pub(crate) fn same_request(&self, requested: &Self) -> bool {
        if self.reference != requested.reference
            || self
                .request_conflict(
                    &requested.owner,
                    requested.domain.as_ref(),
                    requested.expected_transaction_position,
                    requested.request_digest,
                )
                .is_some()
        {
            return false;
        }
        match (self.transaction_request(), requested.transaction_request()) {
            (Some(existing), Some(requested)) => {
                existing.target.identifies_same_request(&requested.target)
            }
            (None, None) => true,
            (Some(_), None) | (None, Some(_)) => false,
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
