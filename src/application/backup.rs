//! Capturing domain configuration and runtime checkpoints into a public archive.
//!
//! Layer: control plane.
//!
//! - **Owns.** Executing an admitted `BACKUP`: establishing each domain cut, reading its coherent
//!   applied configuration and owner checkpoints, planning and measuring archive sections,
//!   assembling the archive under the staging quota, retaining it for download under the backup's
//!   execution reference, and reporting the resulting cut and archive summary.
//! - **Depends on.** Consensus for the capture and the command execution record, the registry's
//!   creation order, the resource store for version archives, the runtime's staging area, the
//!   archive format, and the language layer to parse the rendered NSPL back.
//! - **Must not know.** How a download travels to a client, or how a client stores the archive.
//!
//! A normal backup leases one domain at a time and pauses a running one for its state cut. It
//! resumes and releases the lease before transferring that domain's staged sections into the
//! final archive. Configuration-only and live captures have their own explicit cut semantics.

mod assembly;
pub(in crate::application) mod interconnect;
mod materialized_sections;
pub(in crate::application) mod restore_storage;
pub(in crate::application) mod retained;
mod state_sections;

use std::{collections::BTreeMap, num::NonZeroU64, time::Duration};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_backup::{
    ArchiveLayout, ArchivePiece, ArchiveScope, BackupManifest, DomainCapture, RaftLogPosition,
    SectionContent, SectionDigest, SectionDigester, SectionEntry, SectionPath,
};
use nervix_consensus::{CommandExecution, ConfigurationCapture};
use nervix_execution::{ChargedBytes, Executor, MemoryClass};
use nervix_interconnect::InterconnectStreamRequest as _;
use nervix_models::{
    ArchiveDigest, Backup, BackupArchiveSummary, BackupCapture, BackupCut, BackupDomainSummary,
    BackupQuiesceCounters, BackupResources, BackupScope, CoordinationIdentity, DomainName,
    DomainStatus, IngestorName, ModelKind, NSPL_LANGUAGE_VERSION, ResourceId, ResourceName,
    Timestamp,
};
use nervix_primitives::sync::Arc;
use thiserror::Error;
use tracing::info;

use self::{
    assembly::{PlannedContent, PlannedScope, PlannedSection, plan_sections},
    interconnect::{
        CaptureDomainStateRequest, CaptureInventoryRequest, CapturedSectionInventory,
        FetchCapturedSection,
    },
    retained::{ArchiveBytes, ArchiveReadFailure, RetainedArtifact, RetainedBackups},
};
use super::{
    command_result::{CommandDiagnostic, CommandResult},
    domain_clock::current_timestamp,
    domain_lifecycle::DomainDrainMode,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};
use crate::runtime::{StagedArtifact, StagedArtifactReader, StagedSnapshotWriter};

/// The archives this node's backups retain, as the server stages them.
pub(in crate::application) type ServerRetainedBackups = RetainedBackups<StagedArtifact>;

/// One node-owned state section staged for an authenticated coordinator's bulk fetch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::application) struct CaptureSectionKey {
    pub(in crate::application) coordination: CoordinationIdentity,
    pub(in crate::application) domain: DomainName,
    pub(in crate::application) path: String,
}

pub(in crate::application) struct CapturedSectionStage {
    pub(in crate::application) artifact: Arc<StagedArtifact>,
    pub(in crate::application) content: SectionContent,
    pub(in crate::application) expires_at: nervix_primitives::time::Instant,
}

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
    #[error("the backup's command execution has no request digest")]
    MissingRequestDigest,
    #[error("timed out waiting to capture domain '{domain}': {reason}")]
    CaptureTimeout { domain: DomainName, reason: String },
    #[error("failed to capture domain '{domain}': {reason}")]
    CaptureDomain { domain: DomainName, reason: String },
    #[error("backup coordinator lost its leader tenure while capturing domain '{domain}'")]
    CoordinatorChanged { domain: DomainName },
    #[error("quiesce counters of domain '{domain}' overflowed or regressed during its cut")]
    QuiesceCounters { domain: DomainName },
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
    Captured(Arc<StagedArtifact>),
}

struct StateInventory {
    node: nervix_models::ClusterNodeName,
    coordination: CoordinationIdentity,
    domain: DomainName,
    sections: Vec<CapturedSectionInventory>,
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
        let Some(initial_capture) = self.inner.consensus.configuration_capture().await else {
            return Err(Report::new(BackupError::NoConfiguration));
        };
        let captured_at = current_timestamp();
        let planned_scope = resolve_scope(&backup.scope, execution, &initial_capture)?;
        let mut sections = Vec::new();
        if planned_scope.with_users() {
            sections.extend(
                self.measure_sections(plan_sections(
                    &initial_capture,
                    &PlannedScope {
                        scope: ArchiveScope::Cluster,
                        domains: Vec::new(),
                    },
                    backup.resources,
                    captured_at,
                )?)
                .await?,
            );
        }
        let mut captures = Vec::with_capacity(planned_scope.domains.len());
        for domain in &planned_scope.domains {
            let (capture, cut, domain_captured_at, inventories) = self
                .capture_backup_domain(
                    execution,
                    backup.capture,
                    domain,
                    &initial_capture,
                    captured_at,
                )
                .await?;
            let planned = plan_sections(
                &capture,
                &PlannedScope {
                    scope: ArchiveScope::Domain(domain.clone()),
                    domains: vec![domain.clone()],
                },
                backup.resources,
                domain_captured_at,
            )?;
            sections.extend(self.measure_sections(planned).await?);
            for inventory in inventories {
                for section in &inventory.sections {
                    sections.push(self.fetch_captured_section(&inventory, section).await?);
                }
            }
            captures.push(DomainCapture {
                domain: domain.clone(),
                revision: capture.applied.index,
                raft_log: RaftLogPosition {
                    term: capture.applied.term,
                    index: capture.applied.index,
                },
                cut,
            });
        }
        let manifest = BackupManifest {
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            language_version: NSPL_LANGUAGE_VERSION.to_string(),
            cluster_id: self.inner.cluster.cluster_id().await,
            captured_at,
            scope: planned_scope.scope.clone(),
            resources: backup.resources,
            domains: captures.clone(),
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
            users: users_count(&planned_scope, &initial_capture),
            domains: domain_summaries(&captures, &sections),
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

    /// Reads one domain while its mutation lease prevents a concurrent configuration change.
    /// A stopped domain has no intake to quiesce; a live cut reads the current checkpoint without
    /// touching its lifecycle. The quiesced path resumes before archive transfer begins.
    async fn capture_backup_domain(
        &self,
        execution: &CommandExecution,
        mode: BackupCapture,
        domain: &DomainName,
        initial: &ConfigurationCapture,
        initial_captured_at: Timestamp,
    ) -> error_stack::Result<
        (
            ConfigurationCapture,
            BackupCut,
            Timestamp,
            Vec<StateInventory>,
        ),
        BackupError,
    > {
        match mode {
            BackupCapture::ConfigurationOnly => {
                return Ok((
                    initial.clone(),
                    BackupCut::ConfigurationOnly,
                    initial_captured_at,
                    Vec::new(),
                ));
            }
            BackupCapture::Live => {
                let capture = self
                    .inner
                    .consensus
                    .configuration_capture()
                    .await
                    .ok_or_else(|| Report::new(BackupError::NoConfiguration))?;
                let domain_captured_at = current_timestamp();
                let inventories = self
                    .capture_owner_state(domain, capture.applied.index, false)
                    .await?;
                return Ok((capture, BackupCut::Live, domain_captured_at, inventories));
            }
            BackupCapture::Quiesced { .. } => {}
        }
        let BackupCapture::Quiesced { timeout } = mode else {
            unreachable!()
        };
        let timeout = timeout.unwrap_or_else(|| self.inner.runtime.domain_drain_timeout());
        let deadline = nervix_primitives::time::Instant::now() + timeout;
        let owner = execution
            .owner()
            .cloned()
            .ok_or_else(|| Report::new(BackupError::MissingOwner))?;
        let digest = execution
            .request_digest()
            .ok_or_else(|| Report::new(BackupError::MissingRequestDigest))?;
        let coordinator_tenure = match self.inner.consensus.current_leader_tenure() {
            Some(tenure) if tenure.leader_id() == self.inner.consensus.local_node_id() => tenure,
            _ => {
                return Err(Report::new(BackupError::CoordinatorChanged {
                    domain: domain.clone(),
                }));
            }
        };
        let acquired = loop {
            match self
                .inner
                .consensus
                .acquire_command_domain_mutation(
                    execution.reference.clone(),
                    owner.clone(),
                    digest,
                    domain.clone(),
                )
                .await
            {
                Ok(acquired) => break acquired,
                Err(error) if nervix_primitives::time::Instant::now() < deadline => {
                    tracing::debug!(domain = %domain, error = %error, "waiting for backup domain mutation lease");
                    nervix_primitives::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => {
                    return Err(Report::new(BackupError::CaptureTimeout {
                        domain: domain.clone(),
                        reason: error.to_string(),
                    }));
                }
            }
        };
        let Some(mutation) = acquired.domain_mutation(domain).cloned() else {
            return Err(Report::new(BackupError::CaptureDomain {
                domain: domain.clone(),
                reason: "mutation lease was not recorded".to_string(),
            }));
        };
        let captured = async {
            let before_cut = self
                .inner
                .consensus
                .configuration_capture()
                .await
                .ok_or_else(|| Report::new(BackupError::NoConfiguration))?;
            let Some(domain_state) = before_cut.domains.get(domain) else {
                return Err(Report::new(BackupError::DomainNotFound {
                    domain: domain.clone(),
                }));
            };
            if domain_state.status == DomainStatus::Stopped {
                let domain_captured_at = current_timestamp();
                let inventories = self
                    .capture_owner_state(domain, before_cut.applied.index, false)
                    .await?;
                return Ok((
                    before_cut,
                    BackupCut::Stopped,
                    domain_captured_at,
                    inventories,
                ));
            }
            let _alter_guard = loop {
                if let Some(guard) = self.inner.runtime.try_begin_domain_alter(domain) {
                    break guard;
                }
                if nervix_primitives::time::Instant::now() >= deadline {
                    return Err(Report::new(BackupError::CaptureTimeout {
                        domain: domain.clone(),
                        reason: "domain alteration guard is busy".to_string(),
                    }));
                }
                nervix_primitives::time::sleep(std::time::Duration::from_millis(50)).await;
            };
            let schedule = self.inner.consensus.current_schedule().await;
            let timed_out = || {
                Report::new(BackupError::CaptureTimeout {
                    domain: domain.clone(),
                    reason: "quiesced cut exceeded its timeout".to_string(),
                })
            };
            let before = nervix_primitives::time::timeout_at(
                deadline,
                self.read_backup_quiesce_counters(domain, schedule.domain(domain)),
            )
            .await
            .map_err(|_| timed_out())??;
            let engaged_at = current_timestamp();
            let now = nervix_primitives::time::Instant::now();
            let remaining = if deadline > now {
                deadline - now
            } else {
                std::time::Duration::ZERO
            };
            if remaining.is_zero() {
                return Err(timed_out());
            }
            self.pause_and_drain_domain_for_alter_with_timeout(
                domain,
                Some(&mutation),
                None,
                remaining,
                DomainDrainMode::Backup,
            )
            .await
            .map_err(|error| {
                Report::new(BackupError::CaptureDomain {
                    domain: domain.clone(),
                    reason: error.to_string(),
                })
            })?;
            let cut_result = nervix_primitives::time::timeout_at(deadline, async {
                #[cfg(feature = "testing")]
                self.inner.runtime.pause_backup_cut_if_armed(domain).await;
                if self.inner.consensus.current_leader_tenure().as_ref()
                    != Some(&coordinator_tenure)
                {
                    return Err(Report::new(BackupError::CoordinatorChanged {
                        domain: domain.clone(),
                    }));
                }
                let capture = self
                    .inner
                    .consensus
                    .configuration_capture()
                    .await
                    .ok_or_else(|| Report::new(BackupError::NoConfiguration))?;
                let domain_captured_at = current_timestamp();
                let inventories = self
                    .capture_owner_state(domain, capture.applied.index, true)
                    .await?;
                let after = self
                    .read_backup_quiesce_counters(domain, schedule.domain(domain))
                    .await?;
                if self.inner.consensus.current_leader_tenure().as_ref()
                    != Some(&coordinator_tenure)
                {
                    return Err(Report::new(BackupError::CoordinatorChanged {
                        domain: domain.clone(),
                    }));
                }
                Ok::<_, Report<BackupError>>((capture, domain_captured_at, inventories, after))
            })
            .await
            .map_err(|_| timed_out());
            let resumed = self
                .resume_domain_after_alter(domain, Some(&mutation))
                .await
                .map_err(|error| {
                    Report::new(BackupError::CaptureDomain {
                        domain: domain.clone(),
                        reason: error.to_string(),
                    })
                });
            let (capture, domain_captured_at, inventories, after) = cut_result??;
            resumed?;
            if self.inner.consensus.current_leader_tenure().as_ref() != Some(&coordinator_tenure) {
                return Err(Report::new(BackupError::CoordinatorChanged {
                    domain: domain.clone(),
                }));
            }
            Ok((
                capture,
                BackupCut::Quiesced {
                    engaged_at,
                    released_at: current_timestamp(),
                    quiesce: BackupQuiesceCounters {
                        buffered_records: after.buffered_records,
                        buffered_bytes: after.buffered_bytes,
                        dropped_records: backup_counter_delta(
                            after.dropped_records,
                            before.dropped_records,
                            domain,
                        )?,
                        rejected_records: backup_counter_delta(
                            after.rejected_records,
                            before.rejected_records,
                            domain,
                        )?,
                    },
                },
                domain_captured_at,
                inventories,
            ))
        }
        .await;
        self.inner
            .consensus
            .release_command_domain_mutation(
                execution.reference.clone(),
                owner,
                digest,
                domain.clone(),
            )
            .await
            .map_err(|error| {
                Report::new(BackupError::CaptureDomain {
                    domain: domain.clone(),
                    reason: format!("failed to release mutation lease: {error}"),
                })
            })?;
        if captured.is_ok()
            && self.inner.consensus.current_leader_tenure().as_ref() != Some(&coordinator_tenure)
        {
            return Err(Report::new(BackupError::CoordinatorChanged {
                domain: domain.clone(),
            }));
        }
        captured
    }

    async fn read_backup_quiesce_counters(
        &self,
        domain: &DomainName,
        schedule: Option<&nervix_models::DomainSchedule>,
    ) -> error_stack::Result<BackupQuiesceCounters, BackupError> {
        let mut totals = BackupQuiesceCounters::default();
        let Some(schedule) = schedule else {
            return Ok(totals);
        };
        for (reference, node) in &schedule.nodes {
            if reference.kind != ModelKind::Ingestor {
                continue;
            }
            let name = IngestorName::from(&reference.identifier);
            let (summary, _) =
                self.ingestor_summary(domain, &name, node)
                    .await
                    .map_err(|error| {
                        Report::new(BackupError::CaptureDomain {
                            domain: domain.clone(),
                            reason: error.to_string(),
                        })
                    })?;
            let counters = summary.quiesce_counters;
            let buffered_records = u64::try_from(counters.buffered_records).map_err(|_| {
                Report::new(BackupError::QuiesceCounters {
                    domain: domain.clone(),
                })
            })?;
            let buffered_bytes = u64::try_from(counters.buffered_bytes).map_err(|_| {
                Report::new(BackupError::QuiesceCounters {
                    domain: domain.clone(),
                })
            })?;
            add_backup_counter(&mut totals.buffered_records, buffered_records, domain)?;
            add_backup_counter(&mut totals.buffered_bytes, buffered_bytes, domain)?;
            add_backup_counter(&mut totals.dropped_records, counters.dropped_total, domain)?;
            add_backup_counter(
                &mut totals.rejected_records,
                counters.rejected_total,
                domain,
            )?;
        }
        Ok(totals)
    }

    /// Makes every owner, including this leader, stage its state while the domain cut is held.
    /// Inventory arrives in the same publication round; transfer waits until after resume.
    async fn capture_owner_state(
        &self,
        domain: &DomainName,
        revision: u64,
        quiesced: bool,
    ) -> error_stack::Result<Vec<StateInventory>, BackupError> {
        let coordination = self
            .inner
            .interconnect
            .next_coordination_identity()
            .map_err(|error| {
                Report::new(BackupError::CaptureDomain {
                    domain: domain.clone(),
                    reason: error.to_string(),
                })
            })?;
        let mut nodes = self.inner.cluster.live_node_ids().await;
        if !nodes.contains(self.inner.consensus.local_node_id()) {
            nodes.push(self.inner.consensus.local_node_id().clone());
        }
        nodes.sort();
        nodes.dedup();
        let results = futures_util::future::join_all(nodes.into_iter().map(|node| {
            let coordination = coordination.clone();
            let domain = domain.clone();
            async move {
                let failed = |reason: String| {
                    Report::new(BackupError::CaptureDomain {
                        domain: domain.clone(),
                        reason,
                    })
                };
                let capture_request = CaptureDomainStateRequest {
                    coordination: coordination.clone(),
                    domain: domain.clone(),
                    revision,
                    quiesced,
                };
                let captured = if &node == self.inner.consensus.local_node_id() {
                    self.handle_backup_capture_request(&node, capture_request)
                        .await
                } else {
                    self.inner
                        .interconnect
                        .request(&node, capture_request)
                        .await
                        .map_err(|error| failed(error.to_string()))?
                };
                captured.map_err(|error| failed(error.to_string()))?;
                let inventory_request = CaptureInventoryRequest {
                    coordination: coordination.clone(),
                    domain: domain.clone(),
                };
                let inventory = if &node == self.inner.consensus.local_node_id() {
                    self.handle_backup_inventory_request(&node, inventory_request)
                        .await
                } else {
                    self.inner
                        .interconnect
                        .request(&node, inventory_request)
                        .await
                        .map_err(|error| failed(error.to_string()))?
                };
                let sections = inventory.map_err(|error| failed(error.to_string()))?;
                Ok::<_, Report<BackupError>>(StateInventory {
                    node,
                    coordination,
                    domain,
                    sections,
                })
            }
        }))
        .await;
        results.into_iter().collect()
    }

    /// Fetches one owner-staged section over the bulk pool, staging and verifying it before its
    /// bytes become part of the assembled archive.
    async fn fetch_captured_section(
        &self,
        inventory: &StateInventory,
        section: &CapturedSectionInventory,
    ) -> error_stack::Result<MeasuredSection, BackupError> {
        let error = |reason: String| {
            Report::new(BackupError::CaptureDomain {
                domain: inventory.domain.clone(),
                reason,
            })
        };
        let path = SectionPath::parse(&section.path)
            .map_err(|error_read| error(error_read.to_string()))?;
        let entry = SectionEntry {
            path,
            content: interconnect::section_content(section.kind),
            length: section.length,
            digest: SectionDigest::from_bytes(section.digest),
        };
        if &inventory.node == self.inner.consensus.local_node_id() {
            let artifact = self
                .take_captured_backup_section(
                    &inventory.node,
                    &FetchCapturedSection {
                        coordination: inventory.coordination.clone(),
                        domain: inventory.domain.clone(),
                        path: section.path.clone(),
                    },
                )
                .ok_or_else(|| error("locally captured section is unavailable".to_string()))?;
            if artifact.length() != section.length || artifact.digest() != section.digest {
                return Err(error(
                    "locally captured section differs from its inventory".to_string(),
                ));
            }
            return Ok(MeasuredSection {
                entry,
                domain: Some(inventory.domain.clone()),
                source: SectionSource::Captured(artifact),
            });
        }
        // The immutable owner stage has not been consumed when admission refuses the opening.
        // Live materialized readers share this quota, so wait within the opening's one deadline
        // rather than losing a completed cut to temporary capacity pressure.
        let opening = async {
            loop {
                match self
                    .inner
                    .interconnect
                    .request_stream(
                        &inventory.node,
                        FetchCapturedSection {
                            coordination: inventory.coordination.clone(),
                            domain: inventory.domain.clone(),
                            path: section.path.clone(),
                        },
                    )
                    .await
                {
                    Ok(body) => break Ok(body),
                    Err(refused) if refused.current_context().is_capacity_exhaustion() => {
                        nervix_primitives::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(failure) => break Err(failure),
                }
            }
        };
        let mut body = nervix_primitives::time::timeout(FetchCapturedSection::TIMEOUT, opening)
            .await
            .map_err(|_| error("captured section opening exceeded its deadline".to_string()))?
            .map_err(|reason| error(reason.to_string()))?;
        if body.content_length() != section.length {
            return Err(error(
                "captured section length differs from its inventory".to_string(),
            ));
        }
        let mut writer = self
            .inner
            .runtime
            .try_stage_artifact(section.length)
            .await
            .map_err(|reason| error(reason.to_string()))?;
        while let Some(chunk) = body
            .next_chunk()
            .await
            .map_err(|reason| error(reason.to_string()))?
        {
            nervix_primitives::task::consume_budget().await;
            writer
                .write_chunk(chunk)
                .await
                .map_err(|reason| error(reason.to_string()))?;
        }
        drop(body);
        let artifact = writer
            .finish_artifact()
            .await
            .map_err(|reason| error(reason.to_string()))?;
        if artifact.length() != section.length || artifact.digest() != section.digest {
            return Err(error(
                "captured section digest differs from its inventory".to_string(),
            ));
        }
        Ok(MeasuredSection {
            entry,
            domain: Some(inventory.domain.clone()),
            source: SectionSource::Captured(Arc::new(artifact)),
        })
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
                        SectionSource::Captured(artifact) => {
                            self.copy_captured_section(artifact, entry, &mut sink)
                                .await?;
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

    async fn copy_captured_section(
        &self,
        artifact: &StagedArtifact,
        entry: &SectionEntry,
        sink: &mut StagingSink,
    ) -> error_stack::Result<(), BackupError> {
        let mut reader = artifact
            .open_reader()
            .await
            .change_context(BackupError::Staging)?;
        let mut digest = SectionDigester::new();
        while let Some(chunk) = reader
            .next_chunk(
                self.inner
                    .runtime
                    .executor()
                    .limits()
                    .bulk_chunk_bytes
                    .as_u64(),
            )
            .await
            .change_context(BackupError::Staging)?
        {
            digest.update(chunk.as_ref());
            sink.write_charged(chunk).await?;
        }
        if digest.length() != entry.length || digest.finish() != entry.digest {
            return Err(Report::new(BackupError::Staging));
        }
        Ok(())
    }

    /// Releases every retained archive whose retry validity has ended.
    pub(in crate::application) fn sweep_retained_backups(&self) {
        self.inner.retained_backups.sweep(current_timestamp());
        self.sweep_captured_backup_sections();
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
    captures: &[DomainCapture],
    sections: &[MeasuredSection],
) -> Vec<BackupDomainSummary> {
    let mut totals: BTreeMap<&DomainName, DomainTotals> = BTreeMap::new();
    for capture in captures {
        totals.insert(&capture.domain, DomainTotals::default());
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
        let capture = captures
            .iter()
            .find(|capture| &capture.domain == domain)
            .assured("every total was initialized from a domain capture");
        summaries.push(BackupDomainSummary {
            domain: domain.clone(),
            revision: capture.revision,
            cut: capture.cut,
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

fn add_backup_counter(
    total: &mut u64,
    value: u64,
    domain: &DomainName,
) -> error_stack::Result<(), BackupError> {
    *total = total.checked_add(value).ok_or_else(|| {
        Report::new(BackupError::QuiesceCounters {
            domain: domain.clone(),
        })
    })?;
    Ok(())
}

fn backup_counter_delta(
    after: u64,
    before: u64,
    domain: &DomainName,
) -> error_stack::Result<u64, BackupError> {
    after.checked_sub(before).ok_or_else(|| {
        Report::new(BackupError::QuiesceCounters {
            domain: domain.clone(),
        })
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
