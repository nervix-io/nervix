//! What became of a command.
//!
//! Layer: edges.
//! - **Owns.** The dispositions a host reads, the schema of a subscription the command opened,
//!   typed upload failures and the summary of a restore's report.
//! - **Depends on.** The Rust client's command outcome.
//! - **Must not know.** How the command was sent, retried or redirected.

use arch_into::ArchInto as _;
use meticulous::OptionExt as _;
use nervix_client_core::{CommandDisposition, CommandOutcome, RestoreMode};
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

/// Why a resource upload failed, with the stable values every host binding reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum UploadFailure {
    InvalidStream = 1,
    ResourceNotDeclared = 2,
    SizeMismatch = 3,
    QuotaExceeded = 4,
    InstallationFailed = 5,
    IdentityConflict = 6,
}

impl From<nervix_client_core::UploadFailure> for UploadFailure {
    fn from(failure: nervix_client_core::UploadFailure) -> Self {
        match failure {
            nervix_client_core::UploadFailure::InvalidStream => Self::InvalidStream,
            nervix_client_core::UploadFailure::ResourceNotDeclared => Self::ResourceNotDeclared,
            nervix_client_core::UploadFailure::SizeMismatch => Self::SizeMismatch,
            nervix_client_core::UploadFailure::QuotaExceeded => Self::QuotaExceeded,
            nervix_client_core::UploadFailure::InstallationFailed => Self::InstallationFailed,
            nervix_client_core::UploadFailure::IdentityConflict => Self::IdentityConflict,
        }
    }
}

/// What a restore did, or would do for a dry run, summed over the domains its report lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoreSummary {
    pub dry_run: bool,
    pub domains: u64,
    pub resource_versions: u64,
    pub models: u64,
    /// Whether a step of the restore failed.
    pub failed: bool,
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

    /// The typed upload failure, absent for completed uploads and other commands.
    pub fn upload_failure(&self) -> Option<UploadFailure> {
        let upload = self.outcome.resource_upload.as_ref()?;
        upload.failure.map(UploadFailure::from)
    }

    /// The schema of the subscription the command opened, or `None` when it opened none.
    pub fn opened_schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    pub fn schema(&self) -> Result<&Schema, Failure> {
        self.opened_schema().ok_or_else(|| {
            Failure::new(
                FailureKind::InvalidArgument,
                "the command did not open a subscription",
            )
        })
    }

    /// What a restore command's report says, or `None` for every other command.
    pub fn restore_summary(&self) -> Option<RestoreSummary> {
        let report = self.outcome.restore.as_ref()?;
        let mut resource_versions = 0_u64;
        let mut models = 0_u64;
        for domain in &report.domains {
            resource_versions = resource_versions
                .checked_add(domain.resource_versions)
                .assured("a report's versions are counted from lists a node held in memory");
            models = models
                .checked_add(domain.models)
                .assured("a report's models are counted from lists a node held in memory");
        }
        Some(RestoreSummary {
            dry_run: report.mode == RestoreMode::DryRun,
            domains: report.domains.len().arch_into(),
            resource_versions,
            models,
            failed: report.failed_step().is_some(),
        })
    }
}
