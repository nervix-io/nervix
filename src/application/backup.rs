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
//!   archive format, and the language layer to parse the rendered Plexus back.
//! - **Must not know.** How a download travels to a client, or how a client stores the archive.
//!
//! A normal backup leases one domain at a time and pauses a running one for its state cut. It
//! resumes and releases the lease before transferring that domain's staged sections into the
//! final archive. Configuration-only and live captures have their own explicit cut semantics.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

mod assembly;
mod branch_state_sections;
pub(in crate::application) mod cut_progress;
pub(in crate::application) mod cut_reach;
pub(in crate::application) mod interconnect;
mod materialized_sections;
mod native_metadata_sections;
pub(in crate::application) mod restore_storage;
pub(in crate::application) mod retained;
mod state_sections;
mod stored_materialized_sections;

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    time::Duration,
};

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
    BackupQuiesceCounters, BackupResources, BackupScope, ClusterNodeName, CoordinationIdentity,
    DomainName, DomainSchedule, DomainStatus, IngestorName, ModelKind, NodeRef,
    PLEXUS_LANGUAGE_VERSION, ResourceId, ResourceName, Timestamp,
};
use nervix_primitives::sync::Arc;
use thiserror::Error;
use tracing::info;

use self::{
    assembly::{PlannedContent, PlannedScope, PlannedSection, plan_sections},
    cut_progress::{CaptureContext, CompletedPhases, CutPhase, CutProgress},
    cut_reach::{CutReach, RequiredNode},
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

/// One captured section's publication key, content kind, and quota-owned staged bytes.
pub(super) struct CapturedSection {
    key: CaptureSectionKey,
    content: SectionContent,
    artifact: StagedArtifact,
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
    #[error("the committed models of domain '{domain}' could not be rendered as Plexus")]
    Rendering { domain: DomainName },
    #[error(
        "the Plexus rendered for domain '{domain}' does not parse back to its committed models"
    )]
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
    #[error(
        "timed out waiting to capture domain '{domain}': phase={phase}, elapsed={elapsed:?}, \
         phase_elapsed={phase_elapsed:?}, completed={completed}"
    )]
    CaptureTimeout {
        domain: DomainName,
        phase: CutPhase,
        elapsed: Duration,
        phase_elapsed: Duration,
        completed: CompletedPhases,
    },
    #[error("failed to capture domain '{domain}': {reason}")]
    CaptureDomain { domain: DomainName, reason: String },
    #[error(
        "node '{node}', the scheduled owner of {kind} '{name}' in domain '{domain}', is not live, \
         so the cut cannot capture its state",
        kind = .entity.kind.as_str(),
        name = .entity.identifier.as_str()
    )]
    StateOwnerUnreachable {
        domain: DomainName,
        entity: NodeRef,
        node: ClusterNodeName,
    },
    #[error(
        "node '{node}', which runs {kind} '{name}' of domain '{domain}', is not live, so the \
         quiesced cut cannot drain its work",
        kind = .entity.kind.as_str(),
        name = .entity.identifier.as_str()
    )]
    ExecutorUnreachable {
        domain: DomainName,
        entity: NodeRef,
        node: ClusterNodeName,
    },
    #[error(
        "{kind} '{name}' of domain '{domain}' is placed on no node, so the cut cannot capture its \
         state",
        kind = .entity.kind.as_str(),
        name = .entity.identifier.as_str()
    )]
    StateUnplaced { domain: DomainName, entity: NodeRef },
    #[error("backup coordinator lost its leader tenure while capturing domain '{domain}'")]
    CoordinatorChanged { domain: DomainName },
    #[error("quiesce counters of domain '{domain}' overflowed or regressed during its cut")]
    QuiesceCounters { domain: DomainName },
}

impl BackupError {
    fn state_owner_unreachable(domain: &DomainName, unreached: RequiredNode) -> Self {
        Self::StateOwnerUnreachable {
            domain: domain.clone(),
            entity: unreached.entity,
            node: unreached.node,
        }
    }

    fn executor_unreachable(domain: &DomainName, unreached: RequiredNode) -> Self {
        Self::ExecutorUnreachable {
            domain: domain.clone(),
            entity: unreached.entity,
            node: unreached.node,
        }
    }

    fn state_unplaced(domain: &DomainName, entity: &NodeRef) -> Self {
        Self::StateUnplaced {
            domain: domain.clone(),
            entity: entity.clone(),
        }
    }
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

/// What holds a domain still while its owners capture it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaptureHold {
    /// The cut's lease and pause: each owner forces its publications and closes the capture fence.
    Quiesced,
    /// The cut's lease over a stopped domain: each owner reads its stored checkpoints.
    Stopped,
    /// Nothing: the latest published checkpoints, while the schedule may move a placement.
    Live,
}

impl CaptureHold {
    /// Whether the owners capture a quiesced cut.
    fn is_quiesced(self) -> bool {
        match self {
            Self::Quiesced => true,
            Self::Stopped | Self::Live => false,
        }
    }

    /// Whether the cut's mutation lease keeps the domain's schedule from moving a placement.
    fn holds_schedule(self) -> bool {
        match self {
            Self::Quiesced | Self::Stopped => true,
            Self::Live => false,
        }
    }
}

struct StateInventory {
    node: ClusterNodeName,
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
                let message = format!("backup failed: {report:#}");
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
            language_version: PLEXUS_LANGUAGE_VERSION.to_string(),
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
                // A live capture has no cut of its own to bound it, and takes the budget a
                // quiesced cut without TIMEOUT has.
                let deadline = nervix_primitives::time::Instant::now()
                    + self.inner.runtime.domain_drain_timeout();
                let inventories = self
                    .capture_owner_state(domain, &capture, CaptureHold::Live, deadline)
                    .await?;
                return Ok((capture, BackupCut::Live, domain_captured_at, inventories));
            }
            BackupCapture::Quiesced { .. } => {}
        }
        let BackupCapture::Quiesced { timeout } = mode else {
            unreachable!()
        };
        let timeout = timeout.unwrap_or_else(|| self.inner.runtime.domain_drain_timeout());
        let started = nervix_primitives::time::Instant::now();
        let deadline = started + timeout;
        let mut progress = CutProgress::at(
            domain.clone(),
            CaptureContext::Coordinator {
                execution: execution.reference.clone(),
            },
            CutPhase::MutationLease,
            started,
        );
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
            nervix_primitives::task::consume_budget().await;
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
                    tracing::debug!(domain = %domain, error = %format_args!("{error:#}"), "waiting for backup domain mutation lease");
                    nervix_primitives::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => {
                    return Err(error.change_context(progress.timeout()));
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
            progress.enter_within(CutPhase::ConfigurationRead, deadline)?;
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
                progress.enter_within(CutPhase::Capture, deadline)?;
                #[cfg(feature = "testing")]
                self.inner
                    .runtime
                    .pause_backup_cut_if_armed(
                        domain,
                        crate::fault_injection::BackupCutBoundary::Capture,
                    )
                    .await;
                let domain_captured_at = current_timestamp();
                progress.enter_within(CutPhase::OwnerCapture, deadline)?;
                let inventories = self
                    .capture_owner_state(domain, &before_cut, CaptureHold::Stopped, deadline)
                    .await?;
                progress.ensure_within(deadline)?;
                progress.log_completed();
                return Ok((
                    before_cut,
                    BackupCut::Stopped,
                    domain_captured_at,
                    inventories,
                ));
            }
            progress.enter_within(CutPhase::AlterationGuard, deadline)?;
            let _alter_guard = loop {
                nervix_primitives::task::consume_budget().await;
                if let Some(guard) = self.inner.runtime.try_begin_domain_alter(domain) {
                    break guard;
                }
                if nervix_primitives::time::Instant::now() >= deadline {
                    return Err(Report::new(progress.timeout()));
                }
                nervix_primitives::time::sleep(std::time::Duration::from_millis(50)).await;
            };
            #[cfg(feature = "testing")]
            self.inner
                .runtime
                .pause_backup_cut_if_armed(domain, crate::fault_injection::BackupCutBoundary::Lease)
                .await;
            progress.enter_within(CutPhase::Reach, deadline)?;
            let schedule = self.inner.consensus.current_schedule().await;
            self.refuse_unreached_quiesced_cut(domain, schedule.domain(domain))
                .await?;
            progress.enter_within(CutPhase::OpeningCounters, deadline)?;
            let before = nervix_primitives::time::timeout_at(
                deadline,
                self.read_backup_quiesce_counters(domain, schedule.domain(domain)),
            )
            .await
            .map_err(|_| Report::new(progress.timeout()))??;
            progress.enter_within(CutPhase::Drain, deadline)?;
            let engaged_at = current_timestamp();
            let now = nervix_primitives::time::Instant::now();
            let remaining = if deadline > now {
                deadline - now
            } else {
                std::time::Duration::ZERO
            };
            if remaining.is_zero() {
                return Err(Report::new(progress.timeout()));
            }
            self.pause_and_drain_domain_for_alter_with_timeout(
                domain,
                Some(&mutation),
                None,
                remaining,
                DomainDrainMode::Backup,
            )
            .await
            .map_err(|error| progress.drain_failure(error))?;
            let cut_result = nervix_primitives::time::timeout_at(deadline, async {
                progress.enter_within(CutPhase::Capture, deadline)?;
                #[cfg(feature = "testing")]
                self.inner
                    .runtime
                    .pause_backup_cut_if_armed(
                        domain,
                        crate::fault_injection::BackupCutBoundary::Capture,
                    )
                    .await;
                if self.inner.consensus.current_leader_tenure().as_ref()
                    != Some(&coordinator_tenure)
                {
                    return Err(Report::new(BackupError::CoordinatorChanged {
                        domain: domain.clone(),
                    }));
                }
                progress.enter_within(CutPhase::ConfigurationCapture, deadline)?;
                let capture = self
                    .inner
                    .consensus
                    .configuration_capture()
                    .await
                    .ok_or_else(|| Report::new(BackupError::NoConfiguration))?;
                let domain_captured_at = current_timestamp();
                progress.enter_within(CutPhase::OwnerCapture, deadline)?;
                let inventories = self
                    .capture_owner_state(domain, &capture, CaptureHold::Quiesced, deadline)
                    .await;
                let inventories = match inventories {
                    Ok(inventories) => inventories,
                    Err(error) => {
                        if nervix_primitives::time::Instant::now() >= deadline {
                            return Err(error.change_context(progress.timeout()));
                        }
                        return Err(error);
                    }
                };
                progress.enter_within(CutPhase::ClosingCounters, deadline)?;
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
                progress.ensure_within(deadline)?;
                progress.log_completed();
                Ok::<_, Report<BackupError>>((capture, domain_captured_at, inventories, after))
            })
            .await
            .map_err(|_| Report::new(progress.timeout()));
            let resumed = self
                .resume_domain_after_alter(domain, Some(&mutation))
                .await
                .map_err(|error| {
                    error.change_context(BackupError::CaptureDomain {
                        domain: domain.clone(),
                        reason: "domain resume failed".to_string(),
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
                error.change_context(BackupError::CaptureDomain {
                    domain: domain.clone(),
                    reason: "mutation lease release failed".to_string(),
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
                        error.change_context(BackupError::CaptureDomain {
                            domain: domain.clone(),
                            reason: "ingestor inspection failed".to_string(),
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

    /// The nodes a cut can reach: every node the cluster reports live, and this coordinator.
    pub(in crate::application) async fn cut_live_nodes(&self) -> BTreeSet<ClusterNodeName> {
        let mut live = BTreeSet::new();
        live.extend(self.inner.cluster.live_node_ids().await);
        live.insert(self.inner.consensus.local_node_id().clone());
        live
    }

    /// Refuses a quiesced cut, before it pauses anything, when it could not capture or drain all
    /// of the domain: the schedule places an entity whose state the archive captures on no node, or
    /// the cluster does not report live a scheduled state owner, whose state the archive would
    /// lack, or a node that runs the domain's work, whose admitted work the drain could not observe.
    async fn refuse_unreached_quiesced_cut(
        &self,
        domain: &DomainName,
        schedule: Option<&DomainSchedule>,
    ) -> error_stack::Result<(), BackupError> {
        let reach = CutReach::of(schedule);
        if let Some(entity) = reach.unplaced_state() {
            return Err(Report::new(BackupError::state_unplaced(domain, entity)));
        }
        let live = self.cut_live_nodes().await;
        if let Some(unreached) = reach.unreached_state_owner(&live) {
            return Err(Report::new(BackupError::state_owner_unreachable(
                domain, unreached,
            )));
        }
        if let Some(unreached) = reach.unreached_executor(&live) {
            return Err(Report::new(BackupError::executor_unreachable(
                domain, unreached,
            )));
        }
        Ok(())
    }

    /// Makes every scheduled state owner stage its state at the revision `capture` read, and for
    /// a live cut every other live node too. Inventory arrives in the same publication round;
    /// transfer waits until after resume. A remote owner's capture shares the cut's budget, which
    /// ends at `deadline`, rather than a request deadline of its own: staging a domain's state
    /// takes as long as that state is large. A scheduled owner the cluster does not report live,
    /// or state the schedule places on no node, refuses the cut, because the archive would lack it.
    async fn capture_owner_state(
        &self,
        domain: &DomainName,
        capture: &ConfigurationCapture,
        hold: CaptureHold,
        deadline: nervix_primitives::time::Instant,
    ) -> error_stack::Result<Vec<StateInventory>, BackupError> {
        let reach = CutReach::of(capture.schedule.domain(domain));
        if let Some(entity) = reach.unplaced_state() {
            return Err(Report::new(BackupError::state_unplaced(domain, entity)));
        }
        let live = self.cut_live_nodes().await;
        if let Some(unreached) = reach.unreached_state_owner(&live) {
            return Err(Report::new(BackupError::state_owner_unreachable(
                domain, unreached,
            )));
        }
        let mut nodes = BTreeSet::new();
        nodes.extend(reach.state_owners().cloned());
        if !hold.holds_schedule() {
            // Nothing keeps a live cut's schedule from moving a placement while its owners capture,
            // so every live node is asked and stages what its own applied schedule assigns it.
            nodes.extend(live);
        }
        let revision = capture.applied.index;
        let quiesced = hold.is_quiesced();
        let coordination = self
            .inner
            .interconnect
            .next_coordination_identity()
            .map_err(|error| {
                error.change_context(BackupError::CaptureDomain {
                    domain: domain.clone(),
                    reason: "capture coordination allocation failed".to_string(),
                })
            })?;
        let results = futures_util::future::join_all(nodes.into_iter().map(|node| {
            let coordination = coordination.clone();
            let domain = domain.clone();
            async move {
                let mut progress = CutProgress::at(
                    domain.clone(),
                    CaptureContext::Owner {
                        node: node.clone(),
                        coordination: coordination.clone(),
                    },
                    CutPhase::OwnerCapture,
                    nervix_primitives::time::Instant::now(),
                );
                let failed = |reason: &str| BackupError::CaptureDomain {
                    domain: domain.clone(),
                    reason: reason.to_string(),
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
                    let now = nervix_primitives::time::Instant::now();
                    let remaining = if deadline > now {
                        deadline - now
                    } else {
                        Duration::ZERO
                    };
                    self.inner
                        .interconnect
                        .request_with_timeout(&node, capture_request, remaining)
                        .await
                        .change_context_lazy(|| failed("capture request failed"))?
                };
                captured.change_context_lazy(|| failed("owner capture failed"))?;
                progress.enter_within(CutPhase::OwnerInventory, deadline)?;
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
                        .change_context_lazy(|| failed("inventory request failed"))?
                };
                let sections =
                    inventory.change_context_lazy(|| failed("owner inventory failed"))?;
                progress.ensure_within(deadline)?;
                progress.log_completed();
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
        let failure_context = || BackupError::CaptureDomain {
            domain: inventory.domain.clone(),
            reason: "captured state section could not be read or staged".to_string(),
        };
        let path = SectionPath::parse(&section.path).change_context_lazy(failure_context)?;
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
        // The immutable owner stage has not been consumed when admission refuses the opening, so
        // a refusal for capacity alone waits within the opening's one deadline rather than losing
        // a completed cut. The fetch takes the bulk pool's shared stream and admission, apart
        // from the Snapshot subquota that the replica catch-up this cut started fills.
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
            .change_context_lazy(failure_context)?;
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
            .change_context_lazy(failure_context)?;
        while let Some(chunk) = body
            .next_chunk()
            .await
            .change_context_lazy(failure_context)?
        {
            nervix_primitives::task::consume_budget().await;
            writer
                .write_chunk(chunk)
                .await
                .change_context_lazy(failure_context)?;
        }
        drop(body);
        let artifact = writer
            .finish_artifact()
            .await
            .change_context_lazy(failure_context)?;
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
        let mut connector = StagingEmitter::new(writer, executor);
        for piece in layout.pieces() {
            nervix_primitives::task::consume_budget().await;
            match piece {
                ArchivePiece::Bytes(bytes) => connector.write(bytes).await?,
                ArchivePiece::Section(entry) => {
                    let Some(source) = sources.get(&entry.path) else {
                        return Err(Report::new(BackupError::Encoding));
                    };
                    match source {
                        SectionSource::Held(bytes) => connector.write(bytes).await?,
                        SectionSource::ResourceArchive(id) => {
                            self.copy_resource_archive(id, entry, &mut connector)
                                .await?;
                        }
                        SectionSource::Captured(artifact) => {
                            self.copy_captured_section(artifact, entry, &mut connector)
                                .await?;
                        }
                    }
                }
            }
        }
        connector.finish().await
    }

    /// Copies a resource version's original archive into the staged archive, refusing it if it
    /// changed since it was measured.
    async fn copy_resource_archive(
        &self,
        id: &ResourceId,
        entry: &SectionEntry,
        connector: &mut StagingEmitter,
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
            connector.write_charged(chunk).await?;
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
        connector: &mut StagingEmitter,
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
            connector.write_charged(chunk).await?;
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
struct StagingEmitter {
    writer: StagedSnapshotWriter,
    executor: Executor,
    buffer: Vec<u8>,
    chunk_bytes: usize,
}

impl StagingEmitter {
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
