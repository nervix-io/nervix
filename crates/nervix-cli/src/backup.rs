//! Backups from the terminal: the `backup` subcommand, and `DESCRIBE BACKUP`, which reads an
//! archive on this machine without a server.
//!
//! Layer: edges.
//!
//! - **Owns.** Turning the subcommand's arguments into a `BACKUP` statement, delivering its archive
//!   to a file or to standard output, keeping an archive whose delivery to standard output failed,
//!   reporting the backup as text or JSON, and describing a local archive as text or JSON.
//! - **Depends on.** The client core, which runs the backup and downloads its archive, the archive
//!   format's reader, and the vocabulary.
//! - **Must not know.** How the server assembles or retains an archive.
//!
//! A backup whose archive goes to standard output keeps standard output for the archive alone, so
//! its report goes to standard error. Every failure ends the process with a nonzero status.
//!
//! An archive bound for standard output is downloaded into a staging directory first, and its
//! complete download releases the server's copy. A delivery that fails after that keeps the staged
//! archive, the only copy left, and reports it under the backup's execution reference.

use std::{
    fs::File,
    io::{self, BufReader, Read as _, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use error_stack::{Report as StackReport, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_backup::{
    ArchiveDescription, ArchiveScope, DescribedDomain, DescribedResourceVersion,
    DescribedRuntimeState, DescribedSection, ResourceVersionState, describe_archive,
};
use nervix_client_core::{BackupArchiveSummary, Client, CommandOutcome, ConnectOptions};
use nervix_models::{
    Backup, BackupCapture, BackupResources, BackupScope, CommandExecutionReference, DescribeBackup,
    DomainName, DomainPace, InspectionFormat,
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
    pub(super) without_state: bool,
    pub(super) without_pause: bool,
    pub(super) timeout: Option<Duration>,
    pub(super) execution_reference: Option<CommandExecutionReference>,
    pub(super) format: CliReportFormat,
}

/// Where the archive of a backup goes.
enum ArchiveOutput {
    /// A file the operator named.
    File(PathBuf),
    /// Standard output, through a staging directory the download is verified in first.
    Stdout { staged: StagedArchive },
}

/// The name of the archive in its staging directory.
const STAGED_ARCHIVE: &str = "backup.nvxb";

/// The archive bytes one read of the staged archive hands to standard output, a Linux pipe's
/// default capacity.
const DELIVERY_CHUNK_BYTES: usize = 64 * 1024;

impl ArchiveOutput {
    fn from_argument(output: &str) -> Result<Self, StackReport<ClientError>> {
        if output != "-" {
            return Ok(Self::File(PathBuf::from(output)));
        }
        let staged = StagedArchive::create()?;
        Ok(Self::Stdout { staged })
    }

    /// The file the download writes.
    fn download_path(&self) -> PathBuf {
        match self {
            Self::File(path) => path.clone(),
            Self::Stdout { staged } => staged.path(),
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
    fn deliver(self, stdout: &mut impl Write) -> Result<(), DeliveryFailure> {
        match self {
            Self::File(_) => Ok(()),
            Self::Stdout { staged } => staged.deliver(stdout),
        }
    }
}

/// An archive bound for standard output, downloaded into a private staging directory and verified
/// there before a byte of it is delivered.
struct StagedArchive {
    staging: tempfile::TempDir,
}

impl StagedArchive {
    fn create() -> Result<Self, StackReport<ClientError>> {
        let staging = tempfile::tempdir().change_context(ClientError::StageArchive)?;
        Ok(Self { staging })
    }

    /// The file the archive is staged in.
    fn path(&self) -> PathBuf {
        self.staging.path().join(STAGED_ARCHIVE)
    }

    /// Copies the staged archive to `stdout`.
    ///
    /// The archive's complete download released the server's copy, so the staged archive is the
    /// only one left: a delivery that does not flush every byte to `stdout` keeps it, and only a
    /// complete delivery removes its staging directory.
    fn deliver(self, stdout: &mut impl Write) -> Result<(), DeliveryFailure> {
        let copied = self.copy(stdout);
        if let Err(report) = copied {
            let kept = self.staging.keep();
            return Err(DeliveryFailure::NotDelivered {
                archive: kept.join(STAGED_ARCHIVE),
                report,
            });
        }
        let directory = self.staging.path().to_path_buf();
        match self.staging.close() {
            Ok(()) => Ok(()),
            Err(error) => Err(DeliveryFailure::StagingRemains {
                staging: directory,
                report: StackReport::new(error).change_context(ClientError::RemoveStaging),
            }),
        }
    }

    /// Copies the staged archive to `stdout` and flushes it. Standard output is line-buffered, so
    /// without the flush the archive's last partial line would wait for the flush at exit, which
    /// reports no failure.
    fn copy(&self, stdout: &mut impl Write) -> Result<(), StackReport<ClientError>> {
        let mut file = File::open(self.path()).change_context(ClientError::ReadStagedArchive)?;
        let mut chunk = vec![0_u8; DELIVERY_CHUNK_BYTES];
        loop {
            let read = match file.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Err(
                        StackReport::new(error).change_context(ClientError::ReadStagedArchive)
                    );
                }
            };
            stdout
                .write_all(&chunk[..read])
                .change_context(ClientError::WriteArchive)?;
        }
        stdout.flush().change_context(ClientError::WriteArchive)
    }
}

/// Why a downloaded archive did not reach standard output cleanly.
#[derive(Debug)]
enum DeliveryFailure {
    /// Standard output did not receive the whole archive. The archive stays at `archive`, the only
    /// copy left once its complete download released the server's.
    NotDelivered {
        archive: PathBuf,
        report: StackReport<ClientError>,
    },
    /// Standard output received the whole archive, but the directory it was staged in stays at
    /// `staging`.
    StagingRemains {
        staging: PathBuf,
        report: StackReport<ClientError>,
    },
}

impl DeliveryFailure {
    /// The failure's report as `format` prints it, naming the backup's durable `reference`.
    fn rendered(&self, format: CliReportFormat, reference: &CommandExecutionReference) -> String {
        match self {
            Self::NotDelivered { archive, report } => failure_report(
                format,
                "WRITE_FAILED",
                &format!("{report:#}"),
                Some(Recovery::KeptArchive { reference, archive }),
            ),
            Self::StagingRemains { staging, report } => failure_report(
                format,
                "CLEANUP_FAILED",
                &format!("{report:#}"),
                Some(Recovery::RemoveStaging { reference, staging }),
            ),
        }
    }

    /// The error the process ends with.
    fn into_report(self) -> StackReport<ClientError> {
        match self {
            Self::NotDelivered { report, .. } | Self::StagingRemains { report, .. } => report,
        }
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
    let output = match ArchiveOutput::from_argument(&request.output) {
        Ok(output) => output,
        Err(error) => {
            // Only an archive bound for standard output is staged, so the report goes to standard
            // error. No backup was admitted yet, so it names no execution reference.
            report_failure(
                ReportStream::Stderr,
                format,
                "WRITE_FAILED",
                &format!("{error:#}"),
                None,
            );
            return Err(error);
        }
    };
    let report = output.report();
    let scope = match (request.scope, request.domain) {
        (CliBackupScope::Cluster, None) => BackupScope::Cluster,
        (CliBackupScope::Cluster, Some(_)) => {
            let error = ClientError::BackupArguments {
                reason: "a cluster backup covers every domain and names none",
            };
            report_failure(
                report,
                format,
                "INVALID_ARGUMENTS",
                &error.to_string(),
                None,
            );
            return Err(StackReport::new(error));
        }
        (CliBackupScope::Domain, domain) => BackupScope::Domain(domain),
    };
    let download_path = output.download_path();
    let Some(destination) = download_path.to_str() else {
        let error = ClientError::BackupArguments {
            reason: "the archive's path must be valid UTF-8",
        };
        report_failure(
            report,
            format,
            "INVALID_ARGUMENTS",
            &error.to_string(),
            None,
        );
        return Err(StackReport::new(error));
    };
    let resources = if request.without_resources {
        BackupResources::Omitted
    } else {
        BackupResources::Included
    };
    let capture = if request.without_state {
        BackupCapture::ConfigurationOnly
    } else if request.without_pause {
        BackupCapture::Live
    } else {
        BackupCapture::Quiesced {
            timeout: request.timeout,
        }
    };
    let backup = Backup {
        scope,
        destination: destination.to_string(),
        resources,
        capture,
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
            report_failure(
                report,
                format,
                "CONNECTION_FAILED",
                &error.to_string(),
                None,
            );
            return Err(StackReport::new(ClientError::from(error)));
        }
    };
    let execution = match request.execution_reference {
        Some(reference) => {
            client
                .prepare_backup_with_reference(&backup, &reference)
                .await
        }
        None => client.prepare_execution(backup.to_canonical_nspl()).await,
    };
    let outcome = match client.execute_prepared(&execution).await {
        Ok(outcome) => outcome,
        Err(error) => {
            let recovery = match &error {
                nervix_client_core::ClientError::UncertainCommand { reference, .. }
                | nervix_client_core::ClientError::BackupDownload { reference, .. } => {
                    Some(Recovery::Rerun { reference })
                }
                _ => None,
            };
            report_failure(
                report,
                format,
                "BACKUP_FAILED",
                &error_chain(&error),
                recovery,
            );
            return Err(StackReport::new(ClientError::from(error)));
        }
    };
    let summary = match (outcome.succeeded(), outcome.backup.as_deref()) {
        (true, Some(summary)) => summary.clone(),
        (true, None) | (false, _) => {
            report_failure(report, format, "BACKUP_REFUSED", &outcome.message, None);
            return Err(StackReport::new(ClientError::BackupFailed {
                message: outcome.message,
            }));
        }
    };
    let written_to = match &output {
        ArchiveOutput::File(path) => path.display().to_string(),
        ArchiveOutput::Stdout { .. } => "-".to_string(),
    };
    let delivered = output.deliver(&mut io::stdout().lock());
    if let Err(failure) = delivered {
        report.print(&failure.rendered(format, execution.reference()));
        return Err(failure.into_report());
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

/// What a failure report tells the operator to do about a backup the server admitted, under its
/// durable reference.
#[derive(Clone, Copy)]
enum Recovery<'a> {
    /// Run the backup again under its reference, which returns the backup's outcome and downloads
    /// its archive for as long as the server retains it.
    Rerun {
        reference: &'a CommandExecutionReference,
    },
    /// Take the archive the CLI kept: the backup completed and its complete download released the
    /// server's copy, which running it again cannot download.
    KeptArchive {
        reference: &'a CommandExecutionReference,
        archive: &'a Path,
    },
    /// Remove the staging directory of an archive that reached standard output.
    RemoveStaging {
        reference: &'a CommandExecutionReference,
        staging: &'a Path,
    },
}

impl Recovery<'_> {
    /// The line a text report ends with.
    fn text(self) -> String {
        match self {
            Self::Rerun { reference } => format!(
                "recover using --execution-reference {reference} with the same domain and capture \
                 options"
            ),
            Self::KeptArchive { reference, archive } => format!(
                "recover backup {reference} from its verified archive at '{}'; its complete \
                 download released the server's copy",
                archive.display()
            ),
            Self::RemoveStaging { reference, staging } => format!(
                "backup {reference} reached standard output; remove its staging directory '{}'",
                staging.display()
            ),
        }
    }

    /// Adds what the recovery needs to a JSON report's `error` object.
    fn describe(self, error: &mut Value) {
        match self {
            Self::Rerun { reference } => {
                error["execution_reference"] = json!(reference.as_str());
            }
            Self::KeptArchive { reference, archive } => {
                error["execution_reference"] = json!(reference.as_str());
                error["archive"] = json!(archive.display().to_string());
            }
            Self::RemoveStaging { reference, staging } => {
                error["execution_reference"] = json!(reference.as_str());
                error["staging"] = json!(staging.display().to_string());
            }
        }
    }
}

fn report_failure(
    report: ReportStream,
    format: CliReportFormat,
    code: &str,
    message: &str,
    recovery: Option<Recovery<'_>>,
) {
    report.print(&failure_report(format, code, message, recovery));
}

/// A failure's report as `format` prints it.
fn failure_report(
    format: CliReportFormat,
    code: &str,
    message: &str,
    recovery: Option<Recovery<'_>>,
) -> String {
    match format {
        CliReportFormat::Text => {
            let mut text = format!("error: {message}");
            if let Some(recovery) = recovery {
                text.push('\n');
                text.push_str(&recovery.text());
            }
            text
        }
        CliReportFormat::Json => {
            let mut error = json!({ "code": code, "message": message });
            if let Some(recovery) = recovery {
                recovery.describe(&mut error);
            }
            json!({ "error": error }).to_string()
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
            "cut": cut_json(domain.cut),
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
        format!("  start_point: {:?}", record.start_point),
    ];
    if let Some(mapping) = &record.clock {
        lines.push(format!(
            "  clock: wall_started_at={} logical_start={} time_rate={}",
            mapping.wall_started_at(),
            mapping.logical_start(),
            mapping.time_rate()
        ));
    }
    if let Some(frontier) = record.logical_frontier {
        lines.push(format!("  logical_frontier: {frontier}"));
    }
    lines.push(format!(
        "  models: bytes={} blake3={}",
        domain.models.length, domain.models.digest
    ));
    lines.push(format!("  cut: {}", domain.capture.cut.kind().as_str()));
    if let nervix_models::BackupCut::Quiesced {
        engaged_at,
        released_at,
        quiesce,
    } = domain.capture.cut
    {
        lines.push(format!(
            "  cut_times: engaged_at={} released_at={} buffered_records={} buffered_bytes={} \
             dropped_records={} rejected_records={}",
            engaged_at,
            released_at,
            quiesce.buffered_records,
            quiesce.buffered_bytes,
            quiesce.dropped_records,
            quiesce.rejected_records,
        ));
    }
    lines.push("  resource_versions:".to_string());
    if domain.resource_versions.is_empty() {
        lines.push("  - none".to_string());
    }
    for version in &domain.resource_versions {
        lines.push(resource_version_text(version));
    }
    lines.push("  runtime_state:".to_string());
    if domain.state.is_empty() {
        lines.push("  - none".to_string());
    }
    for state in &domain.state {
        lines.push(runtime_state_text(state));
    }
    lines
}

fn runtime_state_text(state: &DescribedRuntimeState) -> String {
    match state {
        DescribedRuntimeState::Wasm {
            descriptor, guest, ..
        } => format!(
            "  - wasm_processor={} branch={} generation={} revision={} bytes={} blake3={}",
            descriptor.entity,
            branch_text(descriptor.branch_fingerprint.as_ref()),
            u64::from(descriptor.generation),
            descriptor.revision,
            guest.length,
            guest.digest,
        ),
        DescribedRuntimeState::KafkaOffsets { offsets, .. } => format!(
            "  - kafka_ingestor={} partitions={} revision={}",
            offsets.entity,
            offsets.offsets.len(),
            offsets.revision,
        ),
        DescribedRuntimeState::Materialized {
            descriptor, groups, ..
        } => format!(
            "  - materialized_relay={} records={} groups={} revision={} fence={} \
             branch_generation={} schema_fingerprint={}",
            descriptor.entity,
            descriptor.record_count,
            groups.len(),
            descriptor.revision,
            descriptor.fence,
            descriptor.branch_generation,
            digest_hex(descriptor.schema.as_digest()),
        ),
        DescribedRuntimeState::BranchLifecycle { lifecycle, .. } => format!(
            "  - branch_lifecycle={} branches={} revision={}",
            lifecycle.entity,
            lifecycle.branches.len(),
            lifecycle.revision,
        ),
        DescribedRuntimeState::Deduplicator {
            descriptor, groups, ..
        } => format!(
            "  - deduplicator={} branch={} keys={} groups={} revision={} schema_fingerprint={}",
            descriptor.entity,
            branch_text(descriptor.branch_fingerprint.as_ref()),
            descriptor.keys,
            groups.len(),
            descriptor.revision,
            digest_hex(descriptor.schema.as_digest()),
        ),
        DescribedRuntimeState::Window {
            descriptor, groups, ..
        } => format!(
            "  - window_processor={} branch={} rows={} groups={} incarnation={} next_sequence={} \
             delayed_removals={} revision={} schema_fingerprint={} model={}",
            descriptor.entity,
            branch_text(descriptor.branch_fingerprint.as_ref()),
            descriptor.rows.len(),
            groups.len(),
            descriptor.incarnation,
            descriptor.next_sequence,
            delayed_removals(&descriptor.accumulators),
            descriptor.revision,
            digest_hex(descriptor.schema.as_digest()),
            digest_hex(descriptor.model.as_digest()),
        ),
    }
}

/// A branch fingerprint as hexadecimal, or `unbranched`.
fn branch_text(branch: Option<&nervix_models::BranchKeyFingerprint>) -> String {
    match branch {
        Some(fingerprint) => digest_hex(fingerprint.fingerprint()),
        None => "unbranched".to_string(),
    }
}

/// How many stepped rows the window's histograms still count.
fn delayed_removals(accumulators: &[nervix_backup::WindowAccumulatorRecord]) -> usize {
    let mut count = 0_usize;
    for accumulator in accumulators {
        if let nervix_backup::WindowAccumulatorRecord::LinearHistogram { delayed_removals } =
            accumulator
        {
            count = count
                .checked_add(delayed_removals.len())
                .assured("removals a verified archive holds in memory count below usize::MAX");
        }
    }
    count
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
    let state = domain
        .state
        .iter()
        .map(runtime_state_json)
        .collect::<Vec<_>>();
    json!({
        "domain": domain.capture.domain.as_str(),
        "revision": domain.capture.revision,
        "raft_term": domain.capture.raft_log.term,
        "raft_index": domain.capture.raft_log.index,
        "cut": cut_json(domain.capture.cut),
        "status": record.status.as_ref(),
        "pace": pace_text(&record.pace),
        "start_version": record.start_version,
        "start_point": record.start_point,
        "clock": record.clock,
        "logical_frontier": record.logical_frontier,
        "models": section_json(&domain.models),
        "resource_versions": versions,
        "runtime_state": state,
    })
}

fn runtime_state_json(state: &DescribedRuntimeState) -> Value {
    match state {
        DescribedRuntimeState::Wasm {
            descriptor,
            record,
            guest,
        } => json!({
            "kind": "wasm_processor",
            "entity": descriptor.entity.as_str(),
            "schema_fingerprint": digest_hex(descriptor.schema.as_digest()),
            "branch_fingerprint": descriptor.branch_fingerprint.as_ref().map(|fingerprint| digest_hex(fingerprint.fingerprint())),
            "generation": u64::from(descriptor.generation),
            "revision": descriptor.revision,
            "descriptor": section_json(record),
            "guest": section_json(guest),
        }),
        DescribedRuntimeState::KafkaOffsets { offsets, record } => json!({
            "kind": "kafka_offsets",
            "entity": offsets.entity.as_str(),
            "schema_fingerprint": digest_hex(offsets.schema.as_digest()),
            "revision": offsets.revision,
            "positions": offsets.offsets.iter().map(|offset| json!({
                "topic": offset.topic,
                "partition": offset.partition,
                "next_offset": offset.next_offset,
            })).collect::<Vec<_>>(),
            "record": section_json(record),
        }),
        DescribedRuntimeState::Materialized {
            descriptor,
            record,
            groups,
        } => json!({
            "kind": "materialized_relay", "entity": descriptor.entity.as_str(),
            "schema_fingerprint": digest_hex(descriptor.schema.as_digest()), "revision": descriptor.revision,
            "fence": descriptor.fence, "branch_generation": descriptor.branch_generation,
            "record_count": descriptor.record_count, "descriptor": section_json(record),
            "groups": groups.iter().map(|group| json!({"record_count": group.record_count, "identities": section_json(&group.identities), "columns": section_json(&group.columns)})).collect::<Vec<_>>(),
        }),
        DescribedRuntimeState::BranchLifecycle { lifecycle, record } => json!({
            "kind": "branch_lifecycle",
            "owner_kind": lifecycle.owner_kind.as_str(),
            "entity": lifecycle.entity.as_str(),
            "schema_fingerprint": digest_hex(lifecycle.schema.as_digest()),
            "revision": lifecycle.revision,
            "branches": lifecycle.branches.len(),
            "record": section_json(record),
        }),
        DescribedRuntimeState::Deduplicator {
            descriptor,
            record,
            groups,
        } => json!({
            "kind": "deduplicator",
            "entity": descriptor.entity.as_str(),
            "schema_fingerprint": digest_hex(descriptor.schema.as_digest()),
            "branch_fingerprint": descriptor.branch_fingerprint.as_ref().map(|fingerprint| digest_hex(fingerprint.fingerprint())),
            "revision": descriptor.revision,
            "keys": descriptor.keys,
            "descriptor": section_json(record),
            "groups": groups.iter().map(section_json).collect::<Vec<_>>(),
        }),
        DescribedRuntimeState::Window {
            descriptor,
            record,
            groups,
        } => json!({
            "kind": "window_processor",
            "entity": descriptor.entity.as_str(),
            "schema_fingerprint": digest_hex(descriptor.schema.as_digest()),
            "model_digest": digest_hex(descriptor.model.as_digest()),
            "branch_fingerprint": descriptor.branch_fingerprint.as_ref().map(|fingerprint| digest_hex(fingerprint.fingerprint())),
            "revision": descriptor.revision,
            "incarnation": descriptor.incarnation,
            "first_sequence": descriptor.first_sequence,
            "next_sequence": descriptor.next_sequence,
            "rows": descriptor.rows.len(),
            "delayed_removals": delayed_removals(&descriptor.accumulators),
            "descriptor": section_json(record),
            "groups": groups.iter().map(|group| json!({"input": section_json(&group.input), "arguments": section_json(&group.arguments)})).collect::<Vec<_>>(),
        }),
    }
}

fn digest_hex(bytes: &[u8; 32]) -> String {
    let mut hex = String::with_capacity(64);
    for byte in bytes {
        let byte = *byte;
        hex.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        hex.push(char::from(b"0123456789abcdef"[usize::from(byte & 15)]));
    }
    hex
}

fn cut_json(cut: nervix_models::BackupCut) -> Value {
    match cut {
        nervix_models::BackupCut::Quiesced {
            engaged_at,
            released_at,
            quiesce,
        } => json!({
            "kind": "quiesced",
            "engaged_at": engaged_at.to_string(),
            "released_at": released_at.to_string(),
            "buffered_records": quiesce.buffered_records,
            "buffered_bytes": quiesce.buffered_bytes,
            "dropped_records": quiesce.dropped_records,
            "rejected_records": quiesce.rejected_records,
        }),
        other => json!({ "kind": other.kind().as_str() }),
    }
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

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    /// Archive bytes that end in a partial line, which a line-buffered standard output holds back
    /// until it is flushed.
    const ARCHIVE: &[u8] = b"NVXB\narchive bytes\nand a tail without a newline";

    fn reference() -> CommandExecutionReference {
        CommandExecutionReference::parse("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44")
            .assured("the test reference is a hyphenated UUID")
    }

    /// An archive bound for standard output whose staged copy holds `bytes`, as a complete
    /// download leaves it.
    fn staged(bytes: &[u8]) -> StagedArchive {
        let staged = StagedArchive::create()
            .assured("the test stages its archive in the system's temporary directory");
        std::fs::write(staged.path(), bytes).assured("the test writes the staged archive");
        staged
    }

    /// The directory an archive bound for standard output is staged in.
    fn staging_directory(staged: &StagedArchive) -> PathBuf {
        staged
            .path()
            .parent()
            .assured("a staged archive lies in its staging directory")
            .to_path_buf()
    }

    /// Removes the staging directory a failed delivery kept.
    fn remove_kept(archive: &Path) {
        let staging = archive
            .parent()
            .assured("a kept archive lies in its staging directory");
        std::fs::remove_dir_all(staging).assured("the test removes what the delivery kept");
    }

    /// A standard output whose reader closes after it has read `capacity` bytes.
    struct ClosingReader {
        received: Vec<u8>,
        capacity: usize,
    }

    impl Write for ClosingReader {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let room = self
                .capacity
                .checked_sub(self.received.len())
                .verified("the reader never receives more than its capacity");
            if room == 0 {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe));
            }
            let accepted = bytes.len().min(room);
            self.received.extend_from_slice(&bytes[..accepted]);
            Ok(accepted)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A standard output that takes every write but fails to flush, as a buffered output does when
    /// its last bytes meet a full disk.
    struct FailingFlush {
        received: Vec<u8>,
    }

    impl Write for FailingFlush {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.received.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::from(io::ErrorKind::StorageFull))
        }
    }

    /// A standard output whose writes remove the archive's staging directory behind the CLI.
    struct RemovingStaging {
        staging: PathBuf,
        received: Vec<u8>,
    }

    impl Write for RemovingStaging {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.staging.exists() {
                std::fs::remove_dir_all(&self.staging)?;
            }
            self.received.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_complete_delivery_flushes_every_byte_and_removes_the_staging_directory() {
        let staged = staged(ARCHIVE);
        let staging = staging_directory(&staged);
        // Standard output is line-buffered, so a delivery that did not flush would leave the
        // archive's last partial line in the buffer.
        let mut stdout = io::LineWriter::new(Vec::new());
        staged
            .deliver(&mut stdout)
            .assured("a reader that reads everything takes the whole archive");
        assert_eq!(stdout.get_ref().as_slice(), ARCHIVE);
        assert!(!staging.exists(), "a delivered archive leaves no staging");
    }

    #[test]
    fn an_archive_written_to_a_file_is_already_delivered() {
        let output = ArchiveOutput::from_argument("cluster.nvxb")
            .assured("an archive bound for a file needs no staging");
        let mut stdout = Vec::new();
        output
            .deliver(&mut stdout)
            .assured("the download already wrote the file");
        assert!(stdout.is_empty(), "standard output stays untouched");
    }

    #[test]
    fn a_closed_standard_output_keeps_the_verified_archive() {
        let staged = staged(ARCHIVE);
        let staged_archive = staged.path();
        let mut stdout = ClosingReader {
            received: Vec::new(),
            capacity: 5,
        };
        match staged.deliver(&mut stdout) {
            Err(DeliveryFailure::NotDelivered { archive, report }) => {
                assert!(
                    matches!(report.current_context(), ClientError::WriteArchive),
                    "{report:?}"
                );
                assert_eq!(archive, staged_archive);
                assert_eq!(stdout.received.as_slice(), &ARCHIVE[..5]);
                let kept = std::fs::read(&archive).assured("the delivery keeps its archive");
                assert_eq!(kept.as_slice(), ARCHIVE, "the kept archive is complete");
                remove_kept(&archive);
            }
            other => panic!("a closed reader leaves the archive undelivered: {other:?}"),
        }
    }

    #[test]
    fn a_failed_flush_keeps_the_verified_archive() {
        let staged = staged(ARCHIVE);
        let mut stdout = FailingFlush {
            received: Vec::new(),
        };
        match staged.deliver(&mut stdout) {
            Err(DeliveryFailure::NotDelivered { archive, report }) => {
                assert!(
                    matches!(report.current_context(), ClientError::WriteArchive),
                    "{report:?}"
                );
                let kept = std::fs::read(&archive).assured("the delivery keeps its archive");
                assert_eq!(kept.as_slice(), ARCHIVE, "the kept archive is complete");
                remove_kept(&archive);
            }
            other => panic!("an unflushed archive is undelivered: {other:?}"),
        }
    }

    #[test]
    fn a_missing_staged_archive_is_a_read_failure_that_keeps_its_staging() {
        let staged = staged(ARCHIVE);
        let staged_archive = staged.path();
        std::fs::remove_file(&staged_archive).assured("the test removes the staged archive");
        let mut stdout = Vec::new();
        match staged.deliver(&mut stdout) {
            Err(DeliveryFailure::NotDelivered { archive, report }) => {
                assert!(
                    matches!(report.current_context(), ClientError::ReadStagedArchive),
                    "{report:?}"
                );
                assert_eq!(archive, staged_archive);
                assert!(stdout.is_empty(), "nothing reached standard output");
                remove_kept(&archive);
            }
            other => panic!("an archive that cannot be opened is undelivered: {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_staged_archive_that_fails_to_read_is_a_read_failure() {
        let staged = StagedArchive::create()
            .assured("the test stages its archive in the system's temporary directory");
        // A directory opens for reading on Unix, and every read of it fails.
        std::fs::create_dir(staged.path())
            .assured("the test puts a directory where the archive belongs");
        let mut stdout = Vec::new();
        match staged.deliver(&mut stdout) {
            Err(DeliveryFailure::NotDelivered { archive, report }) => {
                assert!(
                    matches!(report.current_context(), ClientError::ReadStagedArchive),
                    "{report:?}"
                );
                assert!(stdout.is_empty(), "nothing reached standard output");
                remove_kept(&archive);
            }
            other => panic!("an archive that cannot be read is undelivered: {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_staging_directory_that_cannot_be_removed_follows_a_complete_delivery() {
        let staged = staged(ARCHIVE);
        let staging = staging_directory(&staged);
        // Unix keeps an open file readable after its directory is removed, so standard output
        // receives every byte and only the removal of the staging directory fails.
        let mut stdout = RemovingStaging {
            staging: staging.clone(),
            received: Vec::new(),
        };
        match staged.deliver(&mut stdout) {
            Err(DeliveryFailure::StagingRemains {
                staging: remaining,
                report,
            }) => {
                assert!(
                    matches!(report.current_context(), ClientError::RemoveStaging),
                    "{report:?}"
                );
                assert_eq!(remaining, staging);
                assert_eq!(stdout.received.as_slice(), ARCHIVE);
            }
            other => panic!("a delivered archive reports the staging it left: {other:?}"),
        }
    }

    #[test]
    fn an_undelivered_archive_is_reported_with_its_reference_and_kept_copy() {
        let reference = reference();
        let failure = DeliveryFailure::NotDelivered {
            archive: PathBuf::from("/tmp/staging/backup.nvxb"),
            report: StackReport::new(io::Error::from(io::ErrorKind::BrokenPipe))
                .change_context(ClientError::WriteArchive),
        };
        let message = "the backup archive could not be written to standard output: broken pipe";
        assert_eq!(
            failure.rendered(CliReportFormat::Text, &reference),
            format!(
                "error: {message}\nrecover backup {reference} from its verified archive at \
                 '/tmp/staging/backup.nvxb'; its complete download released the server's copy"
            )
        );
        let json: Value =
            serde_json::from_str(&failure.rendered(CliReportFormat::Json, &reference))
                .assured("a JSON report is one JSON document");
        assert_eq!(
            json,
            json!({ "error": {
                "code": "WRITE_FAILED",
                "message": message,
                "execution_reference": reference.as_str(),
                "archive": "/tmp/staging/backup.nvxb",
            } })
        );
    }

    #[test]
    fn a_staging_directory_left_after_delivery_is_reported_apart_from_an_undelivered_archive() {
        let reference = reference();
        let failure = DeliveryFailure::StagingRemains {
            staging: PathBuf::from("/tmp/staging"),
            report: StackReport::new(io::Error::from(io::ErrorKind::PermissionDenied))
                .change_context(ClientError::RemoveStaging),
        };
        let message = "the backup archive reached standard output, but its staging directory \
                       could not be removed: permission denied";
        assert_eq!(
            failure.rendered(CliReportFormat::Text, &reference),
            format!(
                "error: {message}\nbackup {reference} reached standard output; remove its staging \
                 directory '/tmp/staging'"
            )
        );
        let json: Value =
            serde_json::from_str(&failure.rendered(CliReportFormat::Json, &reference))
                .assured("a JSON report is one JSON document");
        assert_eq!(
            json,
            json!({ "error": {
                "code": "CLEANUP_FAILED",
                "message": message,
                "execution_reference": reference.as_str(),
                "staging": "/tmp/staging",
            } })
        );
    }

    #[test]
    fn an_uncertain_backup_is_reported_with_the_reference_that_runs_it_again() {
        let reference = reference();
        let recovery = Some(Recovery::Rerun {
            reference: &reference,
        });
        assert_eq!(
            failure_report(CliReportFormat::Text, "BACKUP_FAILED", "unknown", recovery),
            format!(
                "error: unknown\nrecover using --execution-reference {reference} with the same \
                 domain and capture options"
            )
        );
        let json: Value = serde_json::from_str(&failure_report(
            CliReportFormat::Json,
            "BACKUP_FAILED",
            "unknown",
            recovery,
        ))
        .assured("a JSON report is one JSON document");
        assert_eq!(
            json,
            json!({ "error": {
                "code": "BACKUP_FAILED",
                "message": "unknown",
                "execution_reference": reference.as_str(),
            } })
        );
    }

    #[test]
    fn a_failure_before_admission_names_no_reference() {
        assert_eq!(
            failure_report(CliReportFormat::Text, "WRITE_FAILED", "no staging", None),
            "error: no staging"
        );
        let json: Value = serde_json::from_str(&failure_report(
            CliReportFormat::Json,
            "WRITE_FAILED",
            "no staging",
            None,
        ))
        .assured("a JSON report is one JSON document");
        assert_eq!(
            json,
            json!({ "error": { "code": "WRITE_FAILED", "message": "no staging" } })
        );
    }
}
