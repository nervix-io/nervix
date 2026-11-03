//! What became of a command.
//!
//! - **Owns.** The dispositions a host reads, and the schema of a subscription the command opened.
//! - **Depends on.** The Rust client's command outcome.
//! - **Must not know.** How the command was sent, retried or redirected.

use nervix_client_core::{CommandDisposition, CommandOutcome};
use nervix_primitives::sync::Arc;

use crate::{
    failure::{Failure, FailureKind},
    schema::Schema,
};

/// What became of a command, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Disposition {
    Completed = 1,
    Failed = 2,
    NotLeader = 3,
    TransactionDetached = 4,
    TransactionTakenOver = 5,
    OutcomeUnknown = 6,
    ExecutionReferenceConflict = 7,
    ExecutionReferenceExpired = 8,
    PreviewStale = 9,
}

impl From<&CommandDisposition> for Disposition {
    fn from(disposition: &CommandDisposition) -> Self {
        match disposition {
            CommandDisposition::Completed { .. } => Self::Completed,
            CommandDisposition::Failed => Self::Failed,
            CommandDisposition::NotLeader(_) => Self::NotLeader,
            CommandDisposition::TransactionDetached { .. } => Self::TransactionDetached,
            CommandDisposition::TransactionTakenOver { .. } => Self::TransactionTakenOver,
            CommandDisposition::OutcomeUnknown(_) => Self::OutcomeUnknown,
            CommandDisposition::ExecutionReferenceConflict(_) => Self::ExecutionReferenceConflict,
            CommandDisposition::ExecutionReferenceExpired => Self::ExecutionReferenceExpired,
            CommandDisposition::PreviewStale { .. } => Self::PreviewStale,
        }
    }
}

/// A command's outcome, and the schema of the subscription it opened, when it opened one.
#[derive(Debug, Clone)]
pub struct Outcome {
    outcome: CommandOutcome,
    schema: Option<Schema>,
}

impl Outcome {
    pub fn new(outcome: CommandOutcome) -> Self {
        let schema = outcome
            .subscription
            .as_ref()
            .map(|opened| Schema::new(Arc::new(opened.schema.clone())));
        Self { outcome, schema }
    }

    pub fn disposition(&self) -> Disposition {
        Disposition::from(&self.outcome.disposition)
    }

    pub fn command(&self) -> &CommandOutcome {
        &self.outcome
    }

    pub fn schema(&self) -> Result<&Schema, Failure> {
        self.schema.as_ref().ok_or_else(|| {
            Failure::new(
                FailureKind::InvalidArgument,
                "the command did not open a subscription",
            )
        })
    }
}
