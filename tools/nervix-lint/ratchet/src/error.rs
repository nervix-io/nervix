//! Repository tooling, outside the product layer order.
//! Owns: typed refusals of compiler orchestration, evidence validation and audit provenance.
//! Depends on: paths and report-bearing errors.
//! Must not know: rendered driver diagnostics or product failure policy.

use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, error_stack::Report<Error>>;

/// A captured external command diagnostic, retained below the audit's semantic context.
#[derive(Debug, thiserror::Error)]
#[error("{stderr}")]
pub(crate) struct ProcessDiagnostic {
    pub(crate) stderr: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cache qualification failed")]
    Qualification,
    #[error("{operation}: {path}")]
    Io {
        operation: &'static str,
        path: PathBuf,
    },
    #[error("invalid JSON: {path}")]
    Json { path: PathBuf },
    #[error("invalid analysis declaration: {path}")]
    Declaration { path: PathBuf },
    #[error("{configuration}: {issue}: {path}")]
    Evidence {
        configuration: String,
        issue: Issue,
        path: PathBuf,
    },
    #[error("{program} failed with status {status:?}")]
    Process {
        program: String,
        status: Option<i32>,
    },
    #[error("source/configuration/dependency inputs changed during compiler analysis")]
    SourceChanged,
    #[error("RUSTC_WORKSPACE_WRAPPER is already set; cannot compose another driver")]
    WorkspaceWrapper,
    #[error("missing compiler driver; run just typed-ratchet-build: {path}")]
    MissingDriver { path: PathBuf },
    #[error("{issue}")]
    Options { issue: Issue },
    #[error("generated report aggregation failed")]
    Aggregate,
    #[error("cannot operate the host memory admission queue: {path}")]
    Queue { path: PathBuf },
    #[error("the admission queue lock stayed held until its deadline")]
    QueueLocked,
    #[error("cannot start or supervise the analysis driver")]
    Driver,
    #[error("git {arguments:?} failed with status {status:?}")]
    AuditGit {
        arguments: Vec<String>,
        status: Option<i32>,
    },
    #[error("invalid {record} audit record")]
    AuditRecord { record: AuditRecord },
    #[error("origin {remote} is not a GitHub repository")]
    AuditRemote { remote: String },
    #[error("gh api {path} failed after {attempts} attempt(s)")]
    AuditGithub { path: String, attempts: usize },
    #[error("the compiler analysis did not complete")]
    AuditIncomplete,
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum AuditRecord {
    #[error("UTF-8 text")]
    Text,
    #[error("baseline")]
    Baseline,
    #[error("commit")]
    Commit,
    #[error("blame")]
    Blame,
    #[error("hunk")]
    Hunk,
    #[error("candidate")]
    Candidate,
    #[error("report")]
    Report,
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum Issue {
    #[error("the catalog holds no diagnostic configuration; run the full compiler matrix")]
    DiagnosticConfiguration,
    #[error("a tracked acquisition in the catalog names no source file")]
    TrackedOwnerPath,
    #[error("missing or ambiguous complete compiler report")]
    Report,
    #[error("partial, stale or cross-worktree compiler report")]
    Identity,
    #[error("mismatched compiler/configuration report")]
    Compiler,
    #[error("mismatched target report")]
    Crate,
    #[error("mismatched source report")]
    Source,
    #[error("missing Cargo artifact")]
    Artifact,
    #[error("incomplete declared target coverage")]
    Targets,
    #[error("no declared targets")]
    NoTargets,
    #[error("unknown requested packages")]
    Packages,
    #[error(
        "unresolved Nervix diagnostic in required gate; repair it or document one operation with \
         expect"
    )]
    Diagnostics,
    #[error("compiler run failed or did not finish")]
    Compilation,
    #[error("completion disagrees with its compiler reports")]
    Completion,
    #[error("missing or ambiguous compilation record")]
    Measurement,
    #[error("Cargo compiled a target whose compilation record names a failure")]
    FailedMeasurement,
    #[error("analysis compilation exceeded its declared admission reservation")]
    Reservation,
    #[error("analysis compilation exceeded the admission threshold without a reservation")]
    Threshold,
    #[error(
        "selected configurations are inventory only; the policy gate requires the whole declared \
         matrix"
    )]
    PartialGate,
    #[error("unknown requested configuration")]
    Configuration,
    #[error(
        "Turmoil child requires its single declared configuration and the just recipe's \
         tokio_unstable setting"
    )]
    TurmoilChild,
    #[error(
        "Turmoil evidence has different compiler, worktree, configurations, diagnostic mode or \
         source inputs"
    )]
    TurmoilEvidence,
    #[error("a positive number of concurrent configurations is required")]
    Jobs,
}
