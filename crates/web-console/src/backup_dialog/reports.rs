//! The text the dialog shows for a completed backup's summary and for a restore's report and
//! warnings. None names anything the archive holds beyond what its summary or the restore reports:
//! no model, user or secret.

use nervix_client_wire::Diagnostic;
use nervix_models::{
    BackupArchiveSummary, BackupResources, RestoreMode, RestoreReport, RestoreStepOutcome,
    RestoredDomain,
};

/// How the leader begins a diagnostic that warns instead of failing the command, which is how a
/// restore names the archived state it skipped. `nervix-cli` lists a restore's warnings by the
/// same prefix.
const WARNING_PREFIX: &str = "warning:";

/// The lines of a completed backup's summary.
pub(crate) fn summary_lines(summary: &BackupArchiveSummary) -> Vec<String> {
    let mut lines = vec![
        format!(
            "archive: {} bytes · BLAKE3 {}",
            summary.total_bytes, summary.digest
        ),
        format!(
            "captured {} · retained for download until {}",
            summary.captured_at, summary.retained_until
        ),
    ];
    let resources = match summary.resources {
        BackupResources::Included => "resource versions with their bytes",
        BackupResources::Omitted => "resource versions without their bytes",
    };
    lines.push(resources.to_string());
    match summary.users {
        Some(users) => lines.push(format!("{users} users")),
        None => lines.push("no users".to_string()),
    }
    for domain in &summary.domains {
        lines.push(format!(
            "domain {} · cut {} · revision {} · {} sections, {} bytes",
            domain.domain,
            domain.cut.kind().as_str(),
            domain.revision,
            domain.sections,
            domain.section_bytes
        ));
    }
    lines
}

/// The warnings among a restore outcome's diagnostics, each as the leader wrote it.
pub(crate) fn restore_warnings(diagnostics: &[Diagnostic]) -> Vec<String> {
    let mut warnings = Vec::new();
    for diagnostic in diagnostics {
        if diagnostic.message.starts_with(WARNING_PREFIX) {
            warnings.push(diagnostic.message.clone());
        }
    }
    warnings
}

/// The lines of a restore's report: what it restored, or for a dry run what it would restore, and
/// what became of each step.
pub(crate) fn restore_report_lines(report: &RestoreReport) -> Vec<String> {
    let mode = match report.mode {
        RestoreMode::Apply => "restore",
        RestoreMode::DryRun => "dry run",
    };
    let mut lines = vec![format!(
        "{mode} of an archive of {} bytes · BLAKE3 {} · captured {}",
        report.archive.total_bytes, report.archive.digest, report.captured_at
    )];
    if let Some(users) = report.users {
        lines.push(format!(
            "users: {} created, {} skipped, {} replaced",
            users.created, users.skipped, users.replaced
        ));
    }
    for domain in &report.domains {
        lines.push(restored_domain_line(domain));
    }
    for step in &report.steps {
        lines.push(format!(
            "{}: {}",
            step.step,
            step_outcome_label(step.outcome)
        ));
    }
    lines
}

fn restored_domain_line(domain: &RestoredDomain) -> String {
    let name = if domain.source == domain.domain {
        domain.source.to_string()
    } else {
        format!("{} as {}", domain.source, domain.domain)
    };
    format!(
        "{name}: {} at start version {} · {} models · {} resource versions",
        domain.status.as_ref(),
        domain.start_version,
        domain.models,
        domain.resource_versions
    )
}

fn step_outcome_label(outcome: RestoreStepOutcome) -> &'static str {
    match outcome {
        RestoreStepOutcome::Applied => "applied",
        RestoreStepOutcome::Planned => "planned",
        RestoreStepOutcome::Failed => "failed",
        RestoreStepOutcome::NotAttempted => "not attempted",
    }
}
