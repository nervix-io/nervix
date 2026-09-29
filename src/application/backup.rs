//! Backing up the cluster's configuration into a public archive.
//!
//! Layer: control plane.
//!
//! - **Owns.** Executing an admitted `BACKUP`: reading one coherent applied revision of the
//!   replicated configuration, planning the archive's sections from it, measuring every section,
//!   assembling the archive into this node's staging area under the staging quota, retaining it for
//!   download under the backup's execution reference, and the summary the backup reports.
//! - **Depends on.** Consensus for the capture and the command execution record, the registry's
//!   creation order, the resource store for version archives, the runtime's staging area, the
//!   archive format, and the language layer to parse the rendered NSPL back.
//! - **Must not know.** How a download travels to a client, or how a client stores the archive.
//!
//! A configuration backup takes no domain lease and pauses nothing. Every part of it is read from
//! one applied revision, so a change committed while the backup runs appears in every part of the
//! archive or in none, and the manifest records that revision and its Raft log entry per domain.

mod assembly;
pub(in crate::application) mod retained;

use std::{collections::BTreeMap, num::NonZeroU64};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_backup::{
    ArchiveLayout, ArchivePiece, ArchiveScope, BackupManifest, DomainCapture, RaftLogPosition,
    SectionContent, SectionDigester, SectionEntry,
};
use nervix_consensus::{CommandExecution, ConfigurationCapture};
use nervix_execution::{ChargedBytes, Executor, MemoryClass};
use nervix_models::{
    ArchiveDigest, Backup, BackupArchiveSummary, BackupDomainSummary, BackupResources, BackupScope,
    DomainName, NSPL_LANGUAGE_VERSION, ResourceId, ResourceName, Timestamp,
};
use thiserror::Error;
use tracing::info;

use self::{
    assembly::{PlannedContent, PlannedScope, PlannedSection, plan_sections},
    retained::{ArchiveBytes, ArchiveReadFailure, RetainedArtifact, RetainedBackups},
};
use super::{
    command_result::{CommandDiagnostic, CommandResult},
    domain_clock::current_timestamp,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};
use crate::runtime::{StagedArtifact, StagedArtifactReader, StagedSnapshotWriter};

/// The archives this node's backups retain, as the server stages them.
pub(in crate::application) type ServerRetainedBackups = RetainedBackups<StagedArtifact>;

/// Why a backup failed. No variant carries archive contents: an archive holds secrets, password
/// hashes and payload data, and a diagnostic names only where the failure is.
#[derive(Debug, Error)]
pub(in crate::application) enum BackupError {
    #[error("the cluster has applied no configuration to back up yet")]
    NoConfiguration,
    #[error("no active domain selected")]
    NoActiveDomain,
    #[error("domain '{domain}' does not exist")]
    DomainNotFound { domain: DomainName },
    #[error("the committed models of domain '{domain}' do not form a valid graph")]
    InvalidModels { domain: DomainName },
    #[error("the committed models of domain '{domain}' could not be rendered as NSPL")]
    Rendering { domain: DomainName },
    #[error("the NSPL rendered for domain '{domain}' does not parse back to its committed models")]
    Verification { domain: DomainName },
    #[error("the clock mapping of domain '{domain}' cannot be projected to the capture time")]
    ClockProjection { domain: DomainName },
    #[error("the catalog of resource '{resource}' in domain '{domain}' has no next version")]
    InvalidCatalog {
        domain: DomainName,
        resource: ResourceName,
    },
    #[error(
        "version {version} of resource '{resource}' in domain '{domain}' is not installed on this \
         node"
    )]
    ResourceUnavailable {
        domain: DomainName,
        resource: ResourceName,
        version: NonZeroU64,
    },
    #[error(
        "version {version} of resource '{resource}' in domain '{domain}' does not match its \
         catalog entry"
    )]
    ResourceMismatch {
        domain: DomainName,
        resource: ResourceName,
        version: NonZeroU64,
    },
    #[error("an archive record could not be encoded")]
    Encoding,
    #[error("the archive could not be staged on this node")]
    Staging,
    #[error("the backup's execution reference names no time its retry validity starts from")]
    RetryWindow,
    #[error("the backup's command execution no longer names its owner")]
    MissingOwner,
}

impl RetainedArtifact for StagedArtifact {
    fn length(&self) -> u64 {
        StagedArtifact::length(self)
    }

    fn digest(&self) -> [u8; 32] {
        StagedArtifact::digest(self)
    }
}

impl ArchiveBytes for StagedArtifactReader {
    type Chunk = ChargedBytes;

    async fn next_chunk(
        &mut self,
        limit: u64,
    ) -> Result<Option<ChargedBytes>, Report<ArchiveReadFailure>> {
        StagedArtifactReader::next_chunk(self, limit)
            .await
            .change_context(ArchiveReadFailure)
    }
}

/// One planned section, measured: its manifest entry, and where its bytes come from.
struct MeasuredSection {
    entry: SectionEntry,
    domain: Option<DomainName>,
    source: SectionSource,
}

/// Where a measured section's bytes come from when the archive is written.
enum SectionSource {
    Held(Vec<u8>),
    ResourceArchive(ResourceId),
}

impl SessionServiceImpl {
    /// Executes the admitted `backup` that `execution` records, and reports the archive it
    /// assembled.
    pub(in crate::application) async fn execute_backup(
        &self,
        execution: &CommandExecution,
        backup: Backup,
    ) -> CommandResult {
        match self.assemble_backup(execution, &backup).await {
            Ok(summary) => {
                let message = backup_message(&backup, &summary);
                CommandResult {
                    backup: Some(Box::new(summary)),
                    ..command_ok(message)
                }
            }
            Err(report) => {
                let message = format!("backup failed: {}", report.current_context());
                CommandResult {
                    diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
                    ..command_error(message)
                }
            }
        }
    }

    async fn assemble_backup(
        &self,
        execution: &CommandExecution,
        backup: &Backup,
    ) -> error_stack::Result<BackupArchiveSummary, BackupError> {
        let Some(owner) = execution.owner().cloned() else {
            return Err(Report::new(BackupError::MissingOwner));
        };
        let retained_until = self.backup_retained_until(execution)?;
        let Some(capture) = self.inner.consensus.configuration_capture().await else {
            return Err(Report::new(BackupError::NoConfiguration));
        };
        let captured_at = current_timestamp();
        let planned_scope = resolve_scope(&backup.scope, execution, &capture)?;
        let planned = plan_sections(&capture, &planned_scope, backup.resources, captured_at)?;
        let sections = self.measure_sections(planned).await?;

        let manifest = BackupManifest {
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            language_version: NSPL_LANGUAGE_VERSION.to_string(),
            cluster_id: self.inner.cluster.cluster_id().await,
            captured_at,
            scope: planned_scope.scope.clone(),
            resources: backup.resources,
            domains: planned_scope
                .domains
                .iter()
                .map(|domain| DomainCapture {
                    domain: domain.clone(),
                    revision: capture.applied.index,
                    raft_log: RaftLogPosition {
                        term: capture.applied.term,
                        index: capture.applied.index,
                    },
                })
                .collect(),
            sections: sections
                .iter()
                .map(|section| section.entry.clone())
                .collect(),
        };
        let layout = ArchiveLayout::new(manifest).change_context(BackupError::Encoding)?;
        let artifact = self.stage_archive(&layout, sections.as_slice()).await?;
        let Some(total_bytes) = NonZeroU64::new(artifact.length()) else {
            return Err(Report::new(BackupError::Staging));
        };
        let summary = BackupArchiveSummary {
            total_bytes,
            digest: ArchiveDigest::from_bytes(artifact.digest()),
            captured_at,
            retained_until,
            resources: backup.resources,
            users: users_count(&planned_scope, &capture),
            domains: domain_summaries(&planned_scope, capture.applied.index, &sections),
        };
        info!(
            execution_reference = %execution.reference,
            archive_bytes = total_bytes.get(),
            domains = summary.domains.len(),
            "assembled backup archive"
        );
        self.inner.retained_backups.retain(
            execution.reference.clone(),
            owner,
            retained_until,
            artifact,
        );
        Ok(summary)
    }

    /// The instant the backup's archive stops being downloadable: when the retry validity of its
    /// execution reference ends, which is also when its outcome stops being recoverable.
    fn backup_retained_until(
        &self,
        execution: &CommandExecution,
    ) -> error_stack::Result<Timestamp, BackupError> {
        let issued_at = execution
            .reference
            .retry_issued_at()
            .change_context(BackupError::RetryWindow)?;
        issued_at
            .checked_add(self.inner.command_execution_policy.retry_validity())
            .change_context(BackupError::RetryWindow)
    }

    /// Measures every planned section: the length and BLAKE3 digest its manifest entry declares.
    /// A resource version's archive is read once here and once more when it is written, and must
    /// be identical both times.
    async fn measure_sections(
        &self,
        planned: Vec<PlannedSection>,
    ) -> error_stack::Result<Vec<MeasuredSection>, BackupError> {
        let mut measured = Vec::with_capacity(planned.len());
        for section in planned {
            nervix_primitives::task::consume_budget().await;
            let PlannedSection {
                path,
                domain,
                content,
            } = section;
            let measured_section = match content {
                PlannedContent::Held { content, bytes } => MeasuredSection {
                    entry: SectionEntry {
                        path,
                        content,
                        length: bytes.len().arch_into(),
                        digest: SectionDigester::digest_of(&bytes),
                    },
                    domain,
                    source: SectionSource::Held(bytes),
                },
                PlannedContent::ResourceArchive { id, archive_bytes } => {
                    let digester = self.digest_resource_archive(&id).await?;
                    if digester.length() != archive_bytes {
                        return Err(resource_mismatch(&id));
                    }
                    MeasuredSection {
                        entry: SectionEntry {
                            path,
                            content: SectionContent::ResourceArchive,
                            length: digester.length(),
                            digest: digester.finish(),
                        },
                        domain,
                        source: SectionSource::ResourceArchive(id),
                    }
                }
            };
            measured.push(measured_section);
        }
        Ok(measured)
    }

    /// Reads a resource version's original archive through, measuring it.
    async fn digest_resource_archive(
        &self,
        id: &ResourceId,
    ) -> error_stack::Result<SectionDigester, BackupError> {
        let mut reader = self
            .inner
            .resource_store
            .open_archive(id)
            .await
            .change_context_lazy(|| resource_unavailable_error(id))?;
        let mut digester = SectionDigester::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let chunk = reader
                .next_chunk()
                .await
                .change_context_lazy(|| resource_unavailable_error(id))?;
            let Some(chunk) = chunk else {
                break;
            };
            digester.update(chunk.as_ref());
        }
        Ok(digester)
    }

    /// Writes the archive `layout` describes into this node's staging area.
    async fn stage_archive(
        &self,
        layout: &ArchiveLayout,
        sections: &[MeasuredSection],
    ) -> error_stack::Result<StagedArtifact, BackupError> {
        let mut sources = BTreeMap::new();
        for section in sections {
            sources.insert(&section.entry.path, &section.source);
        }
        let writer = self
            .inner
            .runtime
            .stage_artifact(layout.total_bytes())
            .await
            .change_context(BackupError::Staging)?;
        let executor = self.inner.runtime.executor().clone();
        let mut sink = StagingSink::new(writer, executor);
        for piece in layout.pieces() {
            nervix_primitives::task::consume_budget().await;
            match piece {
                ArchivePiece::Bytes(bytes) => sink.write(bytes).await?,
                ArchivePiece::Section(entry) => {
                    let Some(source) = sources.get(&entry.path) else {
                        return Err(Report::new(BackupError::Encoding));
                    };
                    match source {
                        SectionSource::Held(bytes) => sink.write(bytes).await?,
                        SectionSource::ResourceArchive(id) => {
                            self.copy_resource_archive(id, entry, &mut sink).await?;
                        }
                    }
                }
            }
        }
        sink.finish().await
    }

    /// Copies a resource version's original archive into the staged archive, refusing it if it
    /// changed since it was measured.
    async fn copy_resource_archive(
        &self,
        id: &ResourceId,
        entry: &SectionEntry,
        sink: &mut StagingSink,
    ) -> error_stack::Result<(), BackupError> {
        let mut reader = self
            .inner
            .resource_store
            .open_archive(id)
            .await
            .change_context_lazy(|| resource_unavailable_error(id))?;
        let mut digester = SectionDigester::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let chunk = reader
                .next_chunk()
                .await
                .change_context_lazy(|| resource_unavailable_error(id))?;
            let Some(chunk) = chunk else {
                break;
            };
            digester.update(chunk.as_ref());
            if digester.length() > entry.length {
                return Err(resource_mismatch(id));
            }
            sink.write_charged(chunk).await?;
        }
        if digester.length() != entry.length || digester.finish() != entry.digest {
            return Err(resource_mismatch(id));
        }
        Ok(())
    }

    /// Releases every retained archive whose retry validity has ended.
    pub(in crate::application) fn sweep_retained_backups(&self) {
        self.inner.retained_backups.sweep(current_timestamp());
    }
}

/// Resolves what a backup covers against the capture: every domain for the cluster, or the one
/// domain named, or the one the session had selected when the backup was admitted.
fn resolve_scope(
    scope: &BackupScope,
    execution: &CommandExecution,
    capture: &ConfigurationCapture,
) -> error_stack::Result<PlannedScope, BackupError> {
    let domain = match scope {
        BackupScope::Cluster => {
            return Ok(PlannedScope {
                scope: ArchiveScope::Cluster,
                domains: capture.domains.keys().cloned().collect(),
            });
        }
        BackupScope::Domain(Some(domain)) => domain.clone(),
        BackupScope::Domain(None) => match execution.domain() {
            Some(domain) => domain.clone(),
            None => return Err(Report::new(BackupError::NoActiveDomain)),
        },
    };
    if !capture.domains.contains_key(&domain) {
        return Err(Report::new(BackupError::DomainNotFound { domain }));
    }
    Ok(PlannedScope {
        scope: ArchiveScope::Domain(domain.clone()),
        domains: vec![domain],
    })
}

fn users_count(scope: &PlannedScope, capture: &ConfigurationCapture) -> Option<u64> {
    if !scope.with_users() {
        return None;
    }
    Some(capture.users.len().arch_into())
}

/// The sections and bytes the archive holds for each domain, in name order.
fn domain_summaries(
    scope: &PlannedScope,
    revision: u64,
    sections: &[MeasuredSection],
) -> Vec<BackupDomainSummary> {
    let mut totals: BTreeMap<&DomainName, DomainTotals> = BTreeMap::new();
    for domain in &scope.domains {
        totals.insert(domain, DomainTotals::default());
    }
    for section in sections {
        let Some(domain) = &section.domain else {
            continue;
        };
        if let Some(total) = totals.get_mut(domain) {
            total.add(section.entry.length);
        }
    }
    let mut summaries = Vec::with_capacity(totals.len());
    for (domain, total) in totals {
        summaries.push(BackupDomainSummary {
            domain: domain.clone(),
            revision,
            sections: total.sections,
            section_bytes: total.bytes,
        });
    }
    summaries
}

/// The sections and bytes counted for one domain so far.
#[derive(Default)]
struct DomainTotals {
    sections: u64,
    bytes: u64,
}

impl DomainTotals {
    fn add(&mut self, length: u64) {
        self.sections = self
            .sections
            .checked_add(1)
            .assured("a domain has fewer sections than the address space can hold");
        self.bytes = self
            .bytes
            .checked_add(length)
            .verified("the layout already summed every section of the archive within 64 bits");
    }
}

/// The message a completed backup reports.
fn backup_message(backup: &Backup, summary: &BackupArchiveSummary) -> String {
    let scope = match &backup.scope {
        BackupScope::Cluster => "the cluster".to_string(),
        BackupScope::Domain(_) => match summary.domains.first() {
            Some(domain) => format!("domain '{}'", domain.domain),
            None => "no domain".to_string(),
        },
    };
    let users = match summary.users {
        Some(users) => format!(", {users} users"),
        None => String::new(),
    };
    let resources = match summary.resources {
        BackupResources::Included => "",
        BackupResources::Omitted => " without resource bytes",
    };
    format!(
        "backed up {scope}: {} domains{users}, {} bytes{resources}, digest {}",
        summary.domains.len(),
        summary.total_bytes,
        summary.digest,
    )
}

/// The version a planned resource archive names. Planning counts versions from one.
fn planned_version(id: &ResourceId) -> NonZeroU64 {
    NonZeroU64::new(id.version).verified("planning numbers resource versions from one")
}

fn resource_unavailable_error(id: &ResourceId) -> BackupError {
    BackupError::ResourceUnavailable {
        domain: id.domain.clone(),
        resource: id.identifier.clone(),
        version: planned_version(id),
    }
}

fn resource_mismatch(id: &ResourceId) -> Report<BackupError> {
    Report::new(BackupError::ResourceMismatch {
        domain: id.domain.clone(),
        resource: id.identifier.clone(),
        version: planned_version(id),
    })
}

/// Writes an archive into its staging file in chunks of the executor's bulk size, however small
/// the pieces it is handed.
struct StagingSink {
    writer: StagedSnapshotWriter,
    executor: Executor,
    buffer: Vec<u8>,
    chunk_bytes: usize,
}

impl StagingSink {
    fn new(writer: StagedSnapshotWriter, executor: Executor) -> Self {
        let chunk_bytes: usize = executor.limits().bulk_chunk_bytes.as_u64().arch_into();
        let chunk_bytes = chunk_bytes.max(1);
        Self {
            writer,
            executor,
            buffer: Vec::new(),
            chunk_bytes,
        }
    }

    async fn write(&mut self, mut bytes: &[u8]) -> error_stack::Result<(), BackupError> {
        while !bytes.is_empty() {
            nervix_primitives::task::consume_budget().await;
            let room = self
                .chunk_bytes
                .checked_sub(self.buffer.len())
                .verified("the buffer is flushed whenever it reaches the chunk size");
            let taken = room.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..taken]);
            bytes = &bytes[taken..];
            if self.buffer.len() >= self.chunk_bytes {
                self.flush().await?;
            }
        }
        Ok(())
    }

    /// Writes a chunk the caller already holds under a charge, after what is buffered.
    async fn write_charged(&mut self, chunk: ChargedBytes) -> error_stack::Result<(), BackupError> {
        self.flush().await?;
        self.writer
            .write_chunk(chunk)
            .await
            .change_context(BackupError::Staging)
    }

    async fn flush(&mut self) -> error_stack::Result<(), BackupError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.buffer);
        let length: u64 = bytes.len().arch_into();
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, length)
            .await
            .change_context(BackupError::Staging)?;
        self.writer
            .write_chunk(ChargedBytes::from_owned(bytes, reservation))
            .await
            .change_context(BackupError::Staging)
    }

    async fn finish(mut self) -> error_stack::Result<StagedArtifact, BackupError> {
        self.flush().await?;
        self.writer
            .finish_artifact()
            .await
            .change_context(BackupError::Staging)
    }
}
