//! Backups from the terminal: the `backup` subcommand, and `DESCRIBE BACKUP`, which reads an
//! archive on this machine without a server.
//!
//! Layer: edges.
//!
//! - **Owns.** Turning the subcommand's arguments into a `BACKUP` statement, delivering its archive
//!   to a file or to standard output, reporting the backup as text or JSON, and describing a local
//!   archive as text or JSON.
//! - **Depends on.** The client core, which runs the backup and downloads its archive, the archive
//!   format's reader, and the vocabulary.
//! - **Must not know.** How the server assembles or retains an archive.
//!
//! A backup whose archive goes to standard output keeps standard output for the archive alone, so
//! its report goes to standard error. Every failure ends the process with a nonzero status.

use std::{
    fs::File,
    io::{self, BufReader},
    path::{Path, PathBuf},
};

use error_stack::{Report as StackReport, ResultExt as _};
use nervix_backup::{
    ArchiveDescription, ArchiveScope, DescribedDomain, DescribedResourceVersion, DescribedSection,
    ResourceVersionState, describe_archive,
};
use nervix_client_core::{BackupArchiveSummary, Client, CommandOutcome, ConnectOptions};
use nervix_models::{
    Backup, BackupResources, BackupScope, DescribeBackup, DomainName, DomainPace, InspectionFormat,
};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statements};
use serde_json::{Value, json};

use super::{ClientError, expand_user_path};

/// What a backup archive covers, as the subcommand names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lower")]
pub(super) enum CliBackupScope {
    /// Every domain and every user.
    Cluster,
    /// One domain.
    Domain,
}

/// How a report is printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lower")]
pub(super) enum CliReportFormat {
    Text,
    Json,
}

/// The arguments of one `backup` subcommand.
pub(super) struct BackupRequest {
    pub(super) server: String,
    pub(super) connect_options: ConnectOptions,
    pub(super) session_domain: DomainName,
    pub(super) scope: CliBackupScope,
    pub(super) domain: Option<DomainName>,
    pub(super) output: String,
    pub(super) without_resources: bool,
    pub(super) format: CliReportFormat,
}

/// Where the archive of a backup goes.
enum ArchiveOutput {
    /// A file the operator named.
    File(PathBuf),
    /// Standard output, through a private file the download is verified in first.
    Stdout {
        staging: tempfile::TempDir,
        archive: PathBuf,
    },
}

impl ArchiveOutput {
    fn from_argument(output: &str) -> Result<Self, StackReport<ClientError>> {
        if output != "-" {
            return Ok(Self::File(PathBuf::from(output)));
        }
        let staging = tempfile::tempdir().change_context(ClientError::WriteArchive)?;
        let archive = staging.path().join("backup.nvxb");
        Ok(Self::Stdout { staging, archive })
    }

    /// The file the download writes.
    fn download_path(&self) -> &Path {
        match self {
            Self::File(path) => path,
            Self::Stdout { archive, .. } => archive,
        }
    }

    /// Where the report goes: standard output, unless the archive does.
    fn report(&self) -> ReportStream {
        match self {
            Self::File(_) => ReportStream::Stdout,
            Self::Stdout { .. } => ReportStream::Stderr,
        }
    }

    /// Hands a downloaded archive to its output. A file is already in place.
    fn deliver(self) -> Result<(), StackReport<ClientError>> {
        let Self::Stdout { staging, archive } = self else {
            return Ok(());
        };
        let mut file = File::open(&archive).change_context(ClientError::WriteArchive)?;
        let mut stdout = io::stdout().lock();
        io::copy(&mut file, &mut stdout).change_context(ClientError::WriteArchive)?;
        drop(file);
        staging.close().change_context(ClientError::WriteArchive)
    }
}

#[derive(Clone, Copy)]
enum ReportStream {
    Stdout,
    Stderr,
}

impl ReportStream {
    fn print(self, text: &str) {
        match self {
            Self::Stdout => println!("{text}"),
            Self::Stderr => eprintln!("{text}"),
        }
    }
}

/// Runs one backup and delivers its archive.
pub(super) async fn run_backup(request: BackupRequest) -> Result<(), StackReport<ClientError>> {
    let format = request.format;
    let output = ArchiveOutput::from_argument(&request.output)?;
    let report = output.report();
    let scope = match (request.scope, request.domain) {
        (CliBackupScope::Cluster, None) => BackupScope::Cluster,
        (CliBackupScope::Cluster, Some(_)) => {
            let error = ClientError::BackupArguments {
                reason: "a cluster backup covers every domain and names none",
            };
            report_failure(report, format, "INVALID_ARGUMENTS", &error.to_string());
            return Err(StackReport::new(error));
        }
        (CliBackupScope::Domain, domain) => BackupScope::Domain(domain),
    };
    let Some(destination) = output.download_path().to_str() else {
        let error = ClientError::BackupArguments {
            reason: "the archive's path must be valid UTF-8",
        };
        report_failure(report, format, "INVALID_ARGUMENTS", &error.to_string());
        return Err(StackReport::new(error));
    };
    let resources = if request.without_resources {
        BackupResources::Omitted
    } else {
        BackupResources::Included
    };
    let backup = Backup {
        scope,
        destination: destination.to_string(),
        resources,
    };
    let client = match Client::connect_with_options(
        &request.server,
        Some(request.session_domain),
        request.connect_options,
    )
    .await
    {
        Ok(client) => client,
        Err(error) => {
            report_failure(report, format, "CONNECTION_FAILED", &error.to_string());
            return Err(StackReport::new(ClientError::from(error)));
        }
    };
    let outcome = match client.execute(backup.to_canonical_nspl()).await {
        Ok(outcome) => outcome,
        Err(error) => {
            report_failure(report, format, "BACKUP_FAILED", &error_chain(&error));
            return Err(StackReport::new(ClientError::from(error)));
        }
    };
    let summary = match (outcome.succeeded(), outcome.backup.as_deref()) {
        (true, Some(summary)) => summary.clone(),
        (true, None) | (false, _) => {
            report_failure(report, format, "BACKUP_REFUSED", &outcome.message);
            return Err(StackReport::new(ClientError::BackupFailed {
                message: outcome.message,
            }));
        }
    };
    let written_to = match &output {
        ArchiveOutput::File(path) => path.display().to_string(),
        ArchiveOutput::Stdout { .. } => "-".to_string(),
    };
    if let Err(error) = output.deliver() {
        report_failure(report, format, "WRITE_FAILED", &format!("{error:#}"));
        return Err(error);
    }
    match format {
        CliReportFormat::Text => report.print(&outcome.message),
        CliReportFormat::Json => {
            report.print(&backup_report_json(&outcome, &summary, &written_to).to_string());
        }
    }
    Ok(())
}

/// The error and every cause behind it, as one line.
fn error_chain(error: &nervix_client_core::ClientError) -> String {
    let mut message = error.to_string();
    let mut cause = std::error::Error::source(error);
    while let Some(current) = cause {
        message.push_str(": ");
        message.push_str(&current.to_string());
        cause = current.source();
    }
    message
}

fn report_failure(report: ReportStream, format: CliReportFormat, code: &str, message: &str) {
    match format {
        CliReportFormat::Text => report.print(&format!("error: {message}")),
        CliReportFormat::Json => {
            let document = json!({ "error": { "code": code, "message": message } });
            report.print(&document.to_string());
        }
    }
}

fn backup_report_json(
    outcome: &CommandOutcome,
    summary: &BackupArchiveSummary,
    output: &str,
) -> Value {
    let mut domains = Vec::with_capacity(summary.domains.len());
    for domain in &summary.domains {
        domains.push(json!({
            "domain": domain.domain.as_str(),
            "revision": domain.revision,
            "sections": domain.sections,
            "section_bytes": domain.section_bytes,
        }));
    }
    let execution_reference = match &outcome.execution_reference {
        Some(reference) => Value::String(reference.to_string()),
        None => Value::Null,
    };
    json!({
        "execution_reference": execution_reference,
        "output": output,
        "total_bytes": summary.total_bytes.get(),
        "blake3": summary.digest.to_string(),
        "captured_at": summary.captured_at.to_string(),
        "resources": summary.resources.as_ref(),
        "users": summary.users,
        "domains": domains,
    })
}

/// The `DESCRIBE BACKUP` statement `query` consists of, when it is one.
pub(super) fn describe_backup_statement(query: &str) -> Option<DescribeBackup> {
    let Ok(statements) = parse_client_statements(query) else {
        return None;
    };
    let [ClientStatement::DescribeBackup(describe)] = statements.as_slice() else {
        return None;
    };
    Some(describe.clone())
}

/// Reads, verifies and prints the archive `describe` names. Nothing is sent to a server.
pub(super) fn run_describe_backup(
    describe: &DescribeBackup,
) -> Result<(), StackReport<ClientError>> {
    let description = match describe_file(&expand_user_path(&describe.source)) {
        Ok(description) => description,
        Err(report) => {
            let message = format!(
                "backup '{}' cannot be described: {report:#}",
                describe.source
            );
            match describe.format {
                InspectionFormat::Text => eprintln!("error: {message}"),
                InspectionFormat::Json => {
                    let document =
                        json!({ "error": { "code": "INVALID_ARCHIVE", "message": message } });
                    println!("{document}");
                }
            }
            return Err(report);
        }
    };
    match describe.format {
        InspectionFormat::Text => println!("{}", description_text(&describe.source, &description)),
        InspectionFormat::Json => println!("{}", description_json(&describe.source, &description)),
    }
    Ok(())
}

/// Reads and verifies the archive at `path`.
fn describe_file(path: &Path) -> Result<ArchiveDescription, StackReport<ClientError>> {
    let file = File::open(path).change_context(ClientError::DescribeBackup)?;
    describe_archive(BufReader::new(file)).change_context(ClientError::DescribeBackup)
}

fn pace_text(pace: &DomainPace) -> String {
    match pace {
        DomainPace::Paced { period, skew } => format!("paced period={period} skew={skew}"),
        DomainPace::Unpaced => "unpaced".to_string(),
    }
}

fn scope_text(scope: &ArchiveScope) -> String {
    match scope {
        ArchiveScope::Cluster => "cluster".to_string(),
        ArchiveScope::Domain(domain) => format!("domain {domain}"),
    }
}

fn state_text(state: &ResourceVersionState) -> String {
    match state {
        ResourceVersionState::Completed => "completed".to_string(),
        ResourceVersionState::Failed { reason } => format!("failed ({reason})"),
        ResourceVersionState::Unfinished => "unfinished".to_string(),
    }
}

/// An archive's description as text, one `key=value` line per domain and resource version, with
/// the checksums `DESCRIBE RESOURCE` prints for the same version.
fn description_text(source: &str, description: &ArchiveDescription) -> String {
    let manifest = &description.manifest;
    let mut lines = vec![
        format!("backup: {source}"),
        format!("format: {}", nervix_backup::ARCHIVE_FORMAT_MAJOR),
        format!("producer_version: {}", manifest.producer_version),
        format!("language_version: {}", manifest.language_version),
        format!("cluster: {}", manifest.cluster_id),
        format!("captured_at: {}", manifest.captured_at),
        format!("scope: {}", scope_text(&manifest.scope)),
        format!("resources: {}", manifest.resources.as_ref()),
    ];
    match &description.users {
        Some(users) => {
            let mut names = Vec::with_capacity(users.users.len());
            for user in &users.users {
                names.push(user.name.as_str());
            }
            if names.is_empty() {
                lines.push("users: (none)".to_string());
            } else {
                lines.push(format!("users: {}", names.join(",")));
            }
        }
        None => lines.push("users: not included".to_string()),
    }
    lines.push("domains:".to_string());
    if description.domains.is_empty() {
        lines.push("- none".to_string());
    }
    for domain in &description.domains {
        lines.extend(domain_text(domain));
    }
    lines.join("\n")
}

fn domain_text(domain: &DescribedDomain) -> Vec<String> {
    let record = &domain.record;
    let mut lines = vec![
        format!(
            "- domain={} revision={} raft_term={} raft_index={} status={} pace={} start_version={}",
            domain.capture.domain,
            domain.capture.revision,
            domain.capture.raft_log.term,
            domain.capture.raft_log.index,
            record.status.as_ref(),
            pace_text(&record.pace),
            record.start_version,
        ),
        format!(
            "  models: bytes={} blake3={}",
            domain.models.length, domain.models.digest
        ),
        "  resource_versions:".to_string(),
    ];
    if domain.resource_versions.is_empty() {
        lines.push("  - none".to_string());
    }
    for version in &domain.resource_versions {
        lines.push(resource_version_text(version));
    }
    lines
}

fn resource_version_text(version: &DescribedResourceVersion) -> String {
    let record = &version.record;
    let mut line = format!(
        "  - resource={} version={} state={}",
        record.resource,
        record.version,
        state_text(&record.state)
    );
    if let Some(published) = &record.published {
        line.push_str(&format!(
            " root_checksum={} manifest_checksum={} file_count={} total_bytes={} archive_bytes={} \
             created_by_node={} created_at={}",
            published.root_checksum,
            published.manifest_checksum,
            published.file_count,
            published.total_bytes,
            published.archive_bytes,
            published.created_by_node,
            published.created_at,
        ));
    }
    match &version.archive {
        Some(archive) => line.push_str(&format!(" archive=included blake3={}", archive.digest)),
        None => line.push_str(" archive=omitted"),
    }
    line
}

fn section_json(section: &DescribedSection) -> Value {
    json!({
        "path": section.path.to_string(),
        "bytes": section.length,
        "blake3": section.digest.to_string(),
    })
}

fn description_json(source: &str, description: &ArchiveDescription) -> Value {
    let manifest = &description.manifest;
    let users = match &description.users {
        Some(users) => {
            let mut names = Vec::with_capacity(users.users.len());
            for user in &users.users {
                names.push(Value::String(user.name.to_string()));
            }
            Value::Array(names)
        }
        None => Value::Null,
    };
    let scope_domain = match &manifest.scope {
        ArchiveScope::Cluster => Value::Null,
        ArchiveScope::Domain(domain) => Value::String(domain.to_string()),
    };
    let scope = match &manifest.scope {
        ArchiveScope::Cluster => "cluster",
        ArchiveScope::Domain(_) => "domain",
    };
    let mut domains = Vec::with_capacity(description.domains.len());
    for domain in &description.domains {
        domains.push(domain_json(domain));
    }
    json!({
        "backup": source,
        "format": nervix_backup::ARCHIVE_FORMAT_MAJOR,
        "producer_version": manifest.producer_version,
        "language_version": manifest.language_version,
        "cluster_id": manifest.cluster_id,
        "captured_at": manifest.captured_at.to_string(),
        "scope": scope,
        "domain": scope_domain,
        "resources": manifest.resources.as_ref(),
        "users": users,
        "domains": domains,
    })
}

fn domain_json(domain: &DescribedDomain) -> Value {
    let record = &domain.record;
    let mut versions = Vec::with_capacity(domain.resource_versions.len());
    for version in &domain.resource_versions {
        versions.push(resource_version_json(version));
    }
    json!({
        "domain": domain.capture.domain.as_str(),
        "revision": domain.capture.revision,
        "raft_term": domain.capture.raft_log.term,
        "raft_index": domain.capture.raft_log.index,
        "status": record.status.as_ref(),
        "pace": pace_text(&record.pace),
        "start_version": record.start_version,
        "models": section_json(&domain.models),
        "resource_versions": versions,
    })
}

fn resource_version_json(version: &DescribedResourceVersion) -> Value {
    let record = &version.record;
    let published = match &record.published {
        Some(published) => json!({
            "root_checksum": published.root_checksum,
            "manifest_checksum": published.manifest_checksum,
            "file_count": published.file_count,
            "total_bytes": published.total_bytes,
            "archive_bytes": published.archive_bytes,
            "created_by_node": published.created_by_node.to_string(),
            "created_at": published.created_at.to_string(),
        }),
        None => Value::Null,
    };
    let archive = match &version.archive {
        Some(archive) => section_json(archive),
        None => Value::Null,
    };
    json!({
        "resource": record.resource.as_str(),
        "version": record.version.get(),
        "state": state_text(&record.state),
        "published": published,
        "archive": archive,
    })
}
