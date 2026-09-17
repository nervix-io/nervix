//! Durable ownership and terminal outcomes for admitted administrative commands.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Replicated command identities, semantic request digests, and retained outcomes.
//! - **Depends on.** Vocabulary identities and timestamps.
//! - **Must not know.** NSPL parsing, sessions, runtime activation, or transport responses.

use std::collections::BTreeMap;

use nervix_models::{
    ClusterNodeIdentity, CommandExecutionReference, DomainName, DomainState, Statement, Timestamp,
    UserName,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::DomainMutationLease;

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
        Self {
            reference,
            owner,
            domain,
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

    pub(crate) fn same_request(&self, requested: &Self) -> bool {
        self.reference == requested.reference
            && self.owner == requested.owner
            && self.domain == requested.domain
            && self.request_digest == requested.request_digest
    }
}
