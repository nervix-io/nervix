//! Creating, altering, starting and stopping a domain.
//!
//! Layer: control plane.
//!
//! - **Owns.** The domain lifecycle commands, the pause and drain an alteration or backup needs,
//!   and the rollback when one of its steps fails.
//! - **Depends on.** Consensus for the domain record, the registry for its models, and the entity
//!   gate to hold the domain while it changes.
//! - **Must not know.** How the domain's graph is scheduled or executed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_consensus::{ConsensusError, DomainMutationLease};
use nervix_interconnect::{
    DomainDrainStatusEnvelope, RemoteOperationFailure,
    backup::{BackupDrainAction, BackupDrainStatus, BackupDrainStatusRequest},
};
use nervix_models::{
    AlterDomain, ClusterNodeName, CoordinationIdentity, CreateDomain, CreateStatement,
    DomainClockState, DomainName, DomainStartPoint, DomainState, DomainStatus, QuiesceLevel,
    StartDomain, StopDomain, TimestampError,
};
use nervix_primitives::time::{Instant, interval};
use thiserror::Error;

use super::{
    backup::cut_reach::CutReach,
    command_result::CommandResult,
    domain_clock::current_timestamp,
    entity_gate::DrainOutstanding,
    model_mutation::{
        command_error, command_ok, command_ok_already_existed, quiesce_level_message,
    },
    ownership_handoff::{mark_complete_ownership_transitions, planned_relocation_count},
    session_service::{ApplicationBound, SessionServiceImpl},
    transaction::{QuiescenceAttempt, TransactionStepImpactRecorder},
};
use crate::runtime::RuntimeError;
#[derive(Debug, Error)]
pub(in crate::application) enum DomainAlterError {
    #[error("domain '{domain}' already has a model alteration in progress")]
    ConcurrentAlter { domain: DomainName },
    #[error("{outstanding}")]
    QuiesceTimeout { outstanding: DrainOutstanding },
    #[error("timed out draining domain '{domain}' for backup: {details}")]
    BackupQuiesceTimeout { domain: DomainName, details: String },
    #[error("failed to read backup drain status of domain '{domain}' on node '{node}'")]
    BackupDrainRequest {
        domain: DomainName,
        node: ClusterNodeName,
    },
    #[error("backup drain status of domain '{domain}' on node '{node}' failed: {failure}")]
    BackupDrainRemote {
        domain: DomainName,
        node: ClusterNodeName,
        failure: RemoteOperationFailure,
    },
    #[error(
        "timed out draining domain '{domain}' for {operation}: pending_node={pending_node}, \
         relay_buffers={buffered_relay_batches}, node_work_items={node_work_items}, \
         outstanding_acks={outstanding_acks}{emitter_publishing}"
    )]
    EntityQuiesceTimeout {
        domain: DomainName,
        operation: &'static str,
        pending_node: ClusterNodeName,
        buffered_relay_batches: usize,
        node_work_items: usize,
        outstanding_acks: usize,
        emitter_publishing: String,
    },
    #[error("failed {operation} gate in domain '{domain}': {reason}")]
    EntityGate {
        domain: DomainName,
        operation: &'static str,
        reason: String,
    },
    /// The consensus report of the pause is beneath.
    #[error("failed to pause domain '{domain}' for model alteration")]
    PauseDomain { domain: DomainName },
    /// The runtime's report of the cluster state it could not apply is beneath.
    #[error("failed to stop ingestion in domain '{domain}' for model alteration")]
    StopIngestion { domain: DomainName },
    /// A backup cut's deadline ended the application of its pause before every live node applied
    /// it. The runtime's report of the wait the deadline ended is beneath.
    #[error("timed out applying the backup pause of domain '{domain}'")]
    BackupPauseTimeout { domain: DomainName },
    /// The consensus report of the resume is beneath.
    #[error("failed to resume domain '{domain}' after model alteration")]
    ResumeDomain { domain: DomainName },
    /// The runtime's report of the cluster state it could not apply is beneath.
    #[error("failed to restore ingestion in domain '{domain}' after model alteration")]
    RestoreIngestion { domain: DomainName },
    /// The alteration failed after its domain paused, and resuming the domain failed too. Both
    /// reports are beneath: the alteration's failure, then the resume's.
    #[error("failed to resume domain '{domain}' after its model alteration was abandoned")]
    ResumeAfterAbandonedAlter { domain: DomainName },
    #[error("failed to roll back model alteration in domain '{domain}': {reason}")]
    Rollback { domain: DomainName, reason: String },
}

/// How a paused domain's drain observes its work, and what bounds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum DomainDrainMode {
    /// A model alteration's drain, which gives every node `timeout` from when the drain begins.
    Alteration { timeout: Duration },
    /// A quiesced backup cut's drain, which polls until the cut's own `deadline`. The cut counts
    /// its budget from before its lease, so the steps before the drain have already spent part of
    /// it. The deadline also ends the pause's application: the cut never waits for the cluster to
    /// apply its pause past it.
    Backup { deadline: Instant },
}

impl DomainDrainMode {
    /// What ends the waits of the pause's application. An alteration's pause waits as long as the
    /// cluster's own budget allows, and a backup cut's pause ends at the cut's deadline.
    fn application_bound(self) -> ApplicationBound {
        match self {
            Self::Alteration { .. } => ApplicationBound::ClusterBudget,
            Self::Backup { deadline } => ApplicationBound::Deadline(deadline),
        }
    }
}

pub(in crate::application) struct ResolvedDomainStart {
    pub(in crate::application) concrete_start: DomainStartPoint,
    pub(in crate::application) clock: DomainClockState,
}

/// The retained log cap, in bytes, a test build's saturated-log refusal reports.
#[cfg(feature = "testing")]
const REFUSED_WRITE_LOG_CAP_BYTES: u64 = 64 * 1024 * 1024;

/// The retained log a test build's saturated-log refusal reports: one byte past its cap.
#[cfg(feature = "testing")]
const REFUSED_WRITE_RETAINED_LOG_BYTES: u64 = REFUSED_WRITE_LOG_CAP_BYTES + 1;

/// The wait for reclamation a test build's saturated-log refusal reports.
#[cfg(feature = "testing")]
const REFUSED_WRITE_RECLAMATION_WAIT: Duration = Duration::from_secs(5);

impl SessionServiceImpl {
    /// The error this leader's consensus refuses a write with, before appending it, when a test
    /// build armed a model publication refusal of this kind. A lapsed lease is observed against the
    /// node's current tenure, as a refusal Raft returned would be.
    #[cfg(feature = "testing")]
    pub(in crate::application) fn refused_write(
        &self,
        refusal: crate::fault_injection::ModelPublicationRefusalKind,
    ) -> ConsensusError {
        match refusal {
            crate::fault_injection::ModelPublicationRefusalKind::LapsedLease => {
                self.inner.consensus.lapsed_lease_refusal()
            }
            crate::fault_injection::ModelPublicationRefusalKind::SaturatedLog => {
                ConsensusError::LogRetentionSaturated {
                    retained: REFUSED_WRITE_RETAINED_LOG_BYTES,
                    cap: REFUSED_WRITE_LOG_CAP_BYTES,
                    waited: REFUSED_WRITE_RECLAMATION_WAIT,
                }
            }
        }
    }

    pub(in crate::application) async fn apply_persistent_domain_creation(
        &self,
        if_not_exists: bool,
        existed_at_admission: bool,
        state: DomainState,
        mutation: Option<&DomainMutationLease>,
    ) -> CommandResult {
        if existed_at_admission {
            if !if_not_exists {
                return command_error(format!("domain '{}' already exists", state.id.as_str()));
            }
            return match self.apply_current_cluster_state().await {
                Ok(()) => match self.wait_for_authoritative_visibility().await {
                    Ok(()) => command_ok_already_existed(format!(
                        "domain '{}' already exists",
                        state.id.as_str()
                    )),
                    Err(error) => command_error(format!(
                        "domain '{}' exists, but authoritative visibility did not complete: \
                         {error}",
                        state.id.as_str()
                    )),
                },
                Err(error) => command_error(format!(
                    "domain '{}' exists, but its stopped state is not usable everywhere: {error:#}",
                    state.id.as_str()
                )),
            };
        }

        match self.inner.consensus.current_domain(&state.id).await {
            Some(current) if current != state => {
                return command_error(format!(
                    "domain '{}' changed while its creation was applying",
                    state.id.as_str()
                ));
            }
            Some(_) => {}
            None => {
                if let Err(error) = self
                    .inner
                    .consensus
                    .put_domain(state.clone(), mutation)
                    .await
                {
                    return self
                        .consensus_report_response(
                            &error,
                            format!("failed to create domain '{}'", state.id.as_str()),
                        )
                        .await;
                }
            }
        }
        if let Err(error) = self.apply_current_cluster_state().await {
            return command_error(format!(
                "created domain '{}', but its stopped state failed to become usable: {error:#}",
                state.id.as_str()
            ));
        }
        match self.wait_for_authoritative_visibility().await {
            Ok(()) => command_ok(format!("created domain '{}'", state.id.as_str())),
            Err(error) => command_error(format!(
                "created domain '{}', but authoritative visibility did not complete: {error}",
                state.id.as_str()
            )),
        }
    }

    pub(in crate::application) async fn pause_and_drain_domain_for_alter(
        &self,
        domain: &DomainName,
        mutation: Option<&DomainMutationLease>,
        impact: Option<&TransactionStepImpactRecorder>,
    ) -> Result<Option<QuiescenceAttempt>, Report<DomainAlterError>> {
        self.pause_and_drain_domain(
            domain,
            mutation,
            impact,
            DomainDrainMode::Alteration {
                timeout: self.inner.runtime.domain_drain_timeout(),
            },
        )
        .await
    }

    pub(in crate::application) async fn pause_and_drain_domain(
        &self,
        domain: &DomainName,
        mutation: Option<&DomainMutationLease>,
        impact: Option<&TransactionStepImpactRecorder>,
        mode: DomainDrainMode,
    ) -> Result<Option<QuiescenceAttempt>, Report<DomainAlterError>> {
        let attempt = impact.map(|impact| {
            impact.request(nervix_models::PauseRequirement::Domain {
                domain: domain.clone(),
            })
        });
        if let Err(error) = self
            .inner
            .consensus
            .pause_domain(domain.clone(), mutation)
            .await
        {
            if let (Some(impact), Some(attempt)) = (impact, attempt) {
                let reason = ConsensusError::report_message(&error);
                if matches!(error.current_context(), ConsensusError::Conflict(_)) {
                    impact.fail(
                        attempt,
                        nervix_models::ImpactDiagnosticKind::Quiescence,
                        reason,
                    );
                } else {
                    impact.uncertain(
                        attempt,
                        nervix_models::ImpactDiagnosticKind::Quiescence,
                        reason,
                    );
                }
            }
            return Err(error.change_context(DomainAlterError::PauseDomain {
                domain: domain.clone(),
            }));
        }
        if let (Some(impact), Some(attempt)) = (impact, attempt) {
            impact.confirm(attempt);
        }

        // A backup cut's deadline does not cut the replicated pause short: a write abandoned after
        // it was proposed may still commit and leave the domain paused with nobody to resume it.
        // The deadline ends the pause's application instead, at one of its waits. The domain is
        // then resumed below and the cut refused, while every node goes on to apply the resume.
        let applied = self
            .apply_current_cluster_state_within(mode.application_bound())
            .await;
        if let Err(error) = applied {
            if let (Some(impact), Some(attempt)) = (impact, attempt) {
                impact.fail(
                    attempt,
                    nervix_models::ImpactDiagnosticKind::Quiescence,
                    format!("{error:#}"),
                );
            }
            let context = match error.current_context() {
                RuntimeError::ApplicationDeadline { .. } => DomainAlterError::BackupPauseTimeout {
                    domain: domain.clone(),
                },
                _ => DomainAlterError::StopIngestion {
                    domain: domain.clone(),
                },
            };
            let stopped = error.change_context(context);
            return Err(self
                .abort_domain_alter_pause(domain, mutation, impact.zip(attempt), stopped)
                .await);
        }

        let drained = match mode {
            DomainDrainMode::Alteration { timeout } => {
                self.wait_for_paused_domain_drain_with_timeout(domain, timeout)
                    .await
            }
            DomainDrainMode::Backup { deadline } => {
                self.wait_for_backup_domain_drain(domain, deadline).await
            }
        };
        match drained {
            Ok(()) => Ok(attempt),
            Err(reason) => {
                if let (Some(impact), Some(attempt)) = (impact, attempt) {
                    impact.fail(
                        attempt,
                        nervix_models::ImpactDiagnosticKind::Quiescence,
                        format!("{reason:#}"),
                    );
                }
                Err(self
                    .abort_domain_alter_pause(domain, mutation, impact.zip(attempt), reason)
                    .await)
            }
        }
    }

    /// Drains a pause this transaction engaged for its current step before the coordinator that
    /// engaged it stopped. That coordinator may have stopped before its drain finished, and this
    /// node, a new leader among them, may not have installed the paused state yet.
    pub(in crate::application) async fn drain_adopted_domain_pause(
        &self,
        domain: &DomainName,
    ) -> Result<(), Report<DomainAlterError>> {
        self.apply_current_cluster_state()
            .await
            .change_context_lazy(|| DomainAlterError::StopIngestion {
                domain: domain.clone(),
            })?;
        self.wait_for_paused_domain_drain(domain).await
    }

    pub(in crate::application) async fn wait_for_paused_domain_drain(
        &self,
        domain: &DomainName,
    ) -> Result<(), Report<DomainAlterError>> {
        self.wait_for_paused_domain_drain_with_timeout(
            domain,
            self.inner.runtime.domain_drain_timeout(),
        )
        .await
    }

    async fn backup_drain_status_on_node(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        coordination: &CoordinationIdentity,
        action: BackupDrainAction,
    ) -> error_stack::Result<BackupDrainStatus, DomainAlterError> {
        let request = BackupDrainStatusRequest {
            coordination: coordination.clone(),
            domain: domain.clone(),
            action,
        };
        let result = if node == self.inner.consensus.local_node_id() {
            self.handle_backup_drain_status_request(node, request).await
        } else {
            self.inner
                .interconnect
                .request(node, request)
                .await
                .change_context(DomainAlterError::BackupDrainRequest {
                    domain: domain.clone(),
                    node: node.clone(),
                })?
        };
        result.map_err(|failure| {
            Report::new(DomainAlterError::BackupDrainRemote {
                domain: domain.clone(),
                node: node.clone(),
                failure,
            })
        })
    }

    /// Uses the shutdown drain's admitted-work view. After every node is quiet, a separate
    /// cluster-wide confirming flush catches work moving between status reads. Polls until the
    /// cut's `deadline`, however much of its budget the pause's application spent.
    async fn wait_for_backup_domain_drain(
        &self,
        domain: &DomainName,
        deadline: Instant,
    ) -> Result<(), Report<DomainAlterError>> {
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_backup_cut_if_armed(domain, crate::fault_injection::BackupCutBoundary::Drain)
            .await;
        let coordination = self
            .inner
            .interconnect
            .next_coordination_identity()
            .map_err(|error| {
                Report::new(DomainAlterError::BackupQuiesceTimeout {
                    domain: domain.clone(),
                    details: error.to_string(),
                })
            })?;
        // Beside the live nodes, the drain observes every node the schedule assigns the domain's
        // work to, so a scheduled node that stops answering holds the round open instead of
        // leaving its admitted work unobserved.
        let schedule = self.inner.consensus.current_schedule().await;
        let reach = CutReach::of(schedule.domain(domain));
        let mut nodes = self.cut_live_nodes().await;
        nodes.extend(reach.executors().cloned());
        let mut polling = interval(Duration::from_millis(50));
        polling.set_missed_tick_behavior(nervix_primitives::time::MissedTickBehavior::Skip);
        let mut confirming = false;
        loop {
            nervix_primitives::task::consume_budget().await;
            polling.tick().await;
            let action = if confirming {
                BackupDrainAction::Observe
            } else {
                BackupDrainAction::FlushHeldWork
            };
            let mut quiet = true;
            let mut flushes_complete = true;
            let mut details = Vec::new();
            for node in &nodes {
                match self
                    .backup_drain_status_on_node(node, domain, &coordination, action)
                    .await
                {
                    Ok(status) => {
                        if status.holds_admitted_work() {
                            quiet = false;
                        }
                        if status.force_flush_obligations != 0 {
                            flushes_complete = false;
                        }
                        if status.holds_admitted_work() || status.force_flush_obligations != 0 {
                            details.push(format!("node={node} {status:?}"));
                        }
                    }
                    Err(error) => {
                        quiet = false;
                        details.push(format!("node={node} status_error={error}"));
                    }
                }
            }
            if quiet {
                if confirming && flushes_complete {
                    return Ok(());
                }
                if !confirming {
                    let mut confirmed = true;
                    for node in &nodes {
                        if let Err(error) = self
                            .backup_drain_status_on_node(
                                node,
                                domain,
                                &coordination,
                                BackupDrainAction::Confirm,
                            )
                            .await
                        {
                            confirmed = false;
                            details.push(format!("node={node} confirm_error={error}"));
                        }
                    }
                    confirming = confirmed;
                }
            } else {
                confirming = false;
            }
            if nervix_primitives::time::Instant::now() >= deadline {
                return Err(Report::new(DomainAlterError::BackupQuiesceTimeout {
                    domain: domain.clone(),
                    details: if details.is_empty() {
                        "confirming flush did not finish".to_string()
                    } else {
                        details.join("; ")
                    },
                }));
            }
        }
    }

    pub(in crate::application) async fn wait_for_paused_domain_drain_with_timeout(
        &self,
        domain: &DomainName,
        timeout: Duration,
    ) -> Result<(), Report<DomainAlterError>> {
        let mut nodes = self.inner.cluster.live_node_ids().await;
        if !nodes
            .iter()
            .any(|node| node == self.inner.consensus.local_node_id())
        {
            nodes.push(self.inner.consensus.local_node_id().clone());
        }
        nodes.sort();
        nodes.dedup();

        #[cfg(feature = "testing")]
        if self.inner.runtime.take_forced_domain_drain_timeout(domain) {
            return Err(Report::new(DomainAlterError::QuiesceTimeout {
                outstanding: DrainOutstanding {
                    domain: domain.clone(),
                    node: None,
                    active_ingestors: 0,
                    active_generators: 0,
                    outstanding_acks: 0,
                    buffered_emitter_messages: 0,
                    emitter_publishing: Vec::new(),
                    status_error: Some("injected domain drain timeout".to_string()),
                },
            }));
        }

        let deadline = nervix_primitives::time::Instant::now() + timeout;
        let mut polling = interval(Duration::from_millis(50));
        polling.set_missed_tick_behavior(nervix_primitives::time::MissedTickBehavior::Skip);
        let mut last_pending = None::<(ClusterNodeName, DomainDrainStatusEnvelope)>;
        let mut last_status_error = None;

        loop {
            nervix_primitives::task::consume_budget().await;
            polling.tick().await;
            let mut all_drained = true;
            for node in &nodes {
                nervix_primitives::task::consume_budget().await;
                match self.domain_drain_status_on_node(node, domain).await {
                    Ok(status)
                        if status.active_ingestors == 0
                            && status.active_generators == 0
                            && status.outstanding_acks == 0
                            && status.buffered_emitter_messages == 0 => {}
                    Ok(status) => {
                        all_drained = false;
                        last_pending = Some((node.clone(), status));
                    }
                    Err(error) => {
                        all_drained = false;
                        last_status_error = Some(format!("{error:#}"));
                        if nervix_primitives::time::Instant::now() >= deadline {
                            break;
                        }
                    }
                }
            }
            if all_drained {
                return Ok(());
            }
            if nervix_primitives::time::Instant::now() < deadline {
                continue;
            }

            let outstanding = if let Some((node, status)) = last_pending {
                DrainOutstanding {
                    domain: domain.clone(),
                    node: Some(node),
                    active_ingestors: status.active_ingestors,
                    active_generators: status.active_generators,
                    outstanding_acks: status.outstanding_acks,
                    buffered_emitter_messages: status.buffered_emitter_messages,
                    emitter_publishing: status.emitter_publishing,
                    status_error: last_status_error,
                }
            } else {
                DrainOutstanding {
                    domain: domain.clone(),
                    node: None,
                    active_ingestors: 0,
                    active_generators: 0,
                    outstanding_acks: 0,
                    buffered_emitter_messages: 0,
                    emitter_publishing: Vec::new(),
                    status_error: last_status_error,
                }
            };
            return Err(Report::new(DomainAlterError::QuiesceTimeout {
                outstanding,
            }));
        }
    }

    async fn abort_domain_alter_pause(
        &self,
        domain: &DomainName,
        mutation: Option<&DomainMutationLease>,
        impact: Option<(&TransactionStepImpactRecorder, QuiescenceAttempt)>,
        reason: Report<DomainAlterError>,
    ) -> Report<DomainAlterError> {
        match self.resume_domain_after_alter(domain, mutation).await {
            Ok(()) => {
                if let Some((impact, attempt)) = impact {
                    impact.release(attempt);
                }
                reason
            }
            Err(resume_error) => {
                if let Some((impact, attempt)) = impact {
                    impact.fail(
                        attempt,
                        nervix_models::ImpactDiagnosticKind::Recovery,
                        format!("{resume_error:#}"),
                    );
                }
                let mut abandoned = reason;
                abandoned.extend_one(resume_error);
                abandoned.change_context(DomainAlterError::ResumeAfterAbandonedAlter {
                    domain: domain.clone(),
                })
            }
        }
    }

    pub(in crate::application) async fn resume_domain_after_alter(
        &self,
        domain: &DomainName,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), Report<DomainAlterError>> {
        #[cfg(feature = "testing")]
        Box::pin(self.inner.runtime.pause_domain_resume_if_armed(
            self.inner.consensus.local_node_id(),
            domain,
            crate::fault_injection::DomainResumeBoundary::Proposal,
        ))
        .await;
        // A test build can make the leader refuse the release that follows a refused model step
        // publication, the same way it refused that publication: before appending it.
        #[cfg(feature = "testing")]
        if let Some(refusal) = self.inner.runtime.take_model_pause_release_refusal(domain) {
            return Err(Report::new(self.refused_write(refusal)).change_context(
                DomainAlterError::ResumeDomain {
                    domain: domain.clone(),
                },
            ));
        }
        let resumed = self
            .inner
            .consensus
            .resume_domain(domain.clone(), mutation)
            .await;
        #[cfg(feature = "testing")]
        if resumed.is_err() {
            Box::pin(self.inner.runtime.pause_domain_resume_if_armed(
                self.inner.consensus.local_node_id(),
                domain,
                crate::fault_injection::DomainResumeBoundary::Failure,
            ))
            .await;
        }
        resumed.change_context_lazy(|| DomainAlterError::ResumeDomain {
            domain: domain.clone(),
        })?;
        self.apply_current_cluster_state()
            .await
            .change_context_lazy(|| DomainAlterError::RestoreIngestion {
                domain: domain.clone(),
            })
    }

    pub(in crate::application) async fn resume_domain_after_alter_with_impact(
        &self,
        domain: &DomainName,
        mutation: Option<&DomainMutationLease>,
        impact: Option<(&TransactionStepImpactRecorder, QuiescenceAttempt)>,
    ) -> Result<(), Report<DomainAlterError>> {
        if let Err(error) = self.resume_domain_after_alter(domain, mutation).await {
            if let Some((impact, attempt)) = impact {
                impact.fail(
                    attempt,
                    nervix_models::ImpactDiagnosticKind::Recovery,
                    format!("{error:#}"),
                );
            }
            return Err(error);
        }
        if let Some((impact, attempt)) = impact {
            impact.release(attempt);
        }
        Ok(())
    }

    /// Stops a domain whose start could not be completed, and says so when the stop fails too.
    ///
    /// The caller is on its way to returning the start failure, and this rollback is what keeps
    /// the cluster from holding a domain the operator was told did not start. A rollback that
    /// fails leaves exactly that state, so the reason is appended to the caller's message rather
    /// than dropped: nothing else in the command's answer would mention it.
    async fn roll_back_started_domain(&self, domain_id: &DomainName) -> String {
        let mut failures = Vec::new();
        if let Err(error) = self
            .inner
            .consensus
            .stop_domain(domain_id.clone(), None)
            .await
        {
            failures.push(format!("stopping it again failed: {error}"));
        }
        if let Err(error) = self.apply_current_cluster_state().await {
            failures.push(format!("reapplying the cluster state failed: {error:#}"));
        }
        if failures.is_empty() {
            return String::new();
        }
        format!(
            "; the domain may still be running because {}",
            failures.join(" and ")
        )
    }

    /// Restores the pre-alteration models and schedule after a committed batch failed to reach the
    /// cluster. Every quiesce level needs the restore, because the registry commit already landed;
    /// only a domain-paused alteration additionally has to resume the domain.
    pub(in crate::application) async fn rollback_model_alteration(
        &self,
        domain: &DomainName,
        planned: crate::registry::PlannedMutations,
        classified_level: QuiesceLevel,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), Report<DomainAlterError>> {
        let runtime_changes = self
            .inner
            .registry
            .rollback_committed(planned)
            .map_err(|error| {
                Report::new(DomainAlterError::Rollback {
                    domain: domain.clone(),
                    reason: format!("registry rollback failed: {error}"),
                })
            })?;
        self.publish_domain_schedule(domain, runtime_changes.graph, mutation)
            .await
            .map_err(|error| {
                Report::new(DomainAlterError::Rollback {
                    domain: domain.clone(),
                    reason: format!("old schedule restore failed: {error}"),
                })
            })?;
        if classified_level.requires_domain_pause() {
            self.resume_domain_after_alter(domain, mutation).await
        } else {
            Ok(())
        }
    }

    pub(in crate::application) async fn create_domain(
        &self,
        create: CreateStatement<CreateDomain>,
    ) -> CommandResult {
        if self
            .inner
            .consensus
            .current_domain(&create.id)
            .await
            .is_some()
        {
            if create.if_not_exists {
                return match self.apply_current_cluster_state().await {
                    Ok(()) => match self.wait_for_authoritative_visibility().await {
                        Ok(()) => command_ok_already_existed(format!(
                            "domain '{}' already exists",
                            create.id.as_str()
                        )),
                        Err(error) => command_error(format!(
                            "domain '{}' exists, but authoritative visibility did not complete: \
                             {error}",
                            create.id.as_str()
                        )),
                    },
                    Err(error) => command_error(format!(
                        "domain '{}' exists, but its stopped state is not usable everywhere: \
                         {error:#}",
                        create.id.as_str()
                    )),
                };
            }
            return command_error(format!("domain '{}' already exists", create.id.as_str()));
        }
        let create = create.body;
        let state = DomainState {
            id: create.id.clone(),
            config: create.config,
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: DomainStartPoint::Resume,
            clock: None,
        };
        match self.inner.consensus.put_domain(state, None).await {
            Ok(()) => {
                if let Err(error) = self.apply_current_cluster_state().await {
                    return command_error(format!(
                        "created domain '{}', but its stopped state failed to become usable: \
                         {error:#}",
                        create.id.as_str()
                    ));
                }
                match self.wait_for_authoritative_visibility().await {
                    Ok(()) => command_ok(format!("created domain '{}'", create.id.as_str())),
                    Err(error) => command_error(format!(
                        "created domain '{}', but authoritative visibility did not complete: \
                         {error}",
                        create.id.as_str()
                    )),
                }
            }
            Err(error) => {
                self.consensus_report_response(
                    &error,
                    format!("failed to create domain '{}'", create.id.as_str()),
                )
                .await
            }
        }
    }

    pub(in crate::application) async fn alter_domain(
        &self,
        domain: &DomainName,
        alter: AlterDomain,
    ) -> CommandResult {
        let _alter_guard = match self.inner.runtime.try_begin_domain_alter(domain) {
            Some(guard) => guard,
            None => {
                return command_error(
                    DomainAlterError::ConcurrentAlter {
                        domain: domain.clone(),
                    }
                    .to_string(),
                );
            }
        };
        let inputs = self.inner.consensus.domain_planning_inputs(domain).await;
        let Some(previous_state) = inputs.state().cloned() else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        if let DomainStatus::Paused = previous_state.status {
            return command_error(format!(
                "domain '{}' is paused by a model alteration",
                domain.as_str()
            ));
        }
        if previous_state.config.placement == alter.policy {
            return match self.apply_current_cluster_state().await {
                Ok(()) => command_ok(format!(
                    "domain '{}' placement is already {}; {}\nplanned relocations: 0",
                    domain.as_str(),
                    alter.policy.as_ref(),
                    quiesce_level_message(QuiesceLevel::Dynamic),
                )),
                Err(error) => command_error(format!(
                    "domain '{}' already has placement {}, but it is not usable everywhere: \
                     {error:#}",
                    domain.as_str(),
                    alter.policy.as_ref()
                )),
            };
        }

        let previous_schedule = inputs.schedule().cloned();
        let planning = self
            .capture_domain_schedule_planning_snapshot(&inputs)
            .await;
        let live_voters = planning.live_voters();
        let cluster_nodes = planning.cluster_nodes();
        let mut next_schedule = match self.inner.registry.active_graph(domain) {
            Some(graph) => {
                #[cfg(feature = "testing")]
                let mut schedule = graph.schedule_for_domain_with_mode(
                    domain,
                    cluster_nodes,
                    usize::try_from(self.cluster_parameters().replica_count())
                        .assured("the committed count fits a supported target"),
                    alter.policy,
                    self.inner.runtime.scheduler_mode(),
                );
                #[cfg(not(feature = "testing"))]
                let mut schedule = graph.schedule_for_domain(
                    domain,
                    cluster_nodes,
                    usize::try_from(self.cluster_parameters().replica_count())
                        .assured("the committed count fits a supported target"),
                    alter.policy,
                );
                Self::merge_existing_schedule_data(
                    &mut schedule,
                    previous_schedule.as_ref(),
                    live_voters,
                );
                Some(schedule)
            }
            None => None,
        };
        if let Some(next_schedule) = next_schedule.as_mut() {
            mark_complete_ownership_transitions(previous_schedule.as_ref(), next_schedule);
        }
        let relocations =
            planned_relocation_count(previous_schedule.as_ref(), next_schedule.as_ref());
        let quiesce_level =
            if matches!(previous_state.status, DomainStatus::Running) && relocations > 0 {
                QuiesceLevel::EntityPause
            } else {
                QuiesceLevel::Dynamic
            };
        let mut next_state = previous_state.clone();
        next_state.config.placement = alter.policy;

        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .take_armed_schedule_publication_fault(domain)
        {
            return command_error(format!(
                "injected schedule publication fault for domain '{}'",
                domain.as_str()
            ));
        }
        if let Err(error) = self.validate_domain_planning_inputs(&inputs).await {
            return command_error(error.to_string());
        }
        if let Err(error) = planning.validate_eligibility(self).await {
            return command_error(error.to_string());
        }
        let handoff = if relocations > 0 {
            self.begin_planned_ownership_handoff(
                domain,
                previous_schedule.as_ref(),
                next_schedule.as_ref(),
            )
            .await
        } else {
            Ok(None)
        };
        let handoff = match handoff {
            Ok(handoff) => handoff,
            Err(error) => return command_error(error.to_string()),
        };
        if let Err(error) = self
            .inner
            .consensus
            .put_domain_and_schedule(inputs, next_state, next_schedule, None)
            .await
        {
            if let Some(handoff) = handoff {
                self.abort_planned_ownership_handoff(domain, handoff, None)
                    .await;
            }
            return self
                .consensus_report_response(
                    &error,
                    format!("failed to alter placement for domain '{}'", domain.as_str()),
                )
                .await;
        }
        let activation_error = self.apply_current_cluster_state().await.err();
        if let Some(error) = activation_error {
            if let Some(handoff) = handoff {
                self.defer_planned_ownership_handoff_release(domain, handoff, &error, None);
            }
            return command_error(format!(
                "committed placement and schedule for domain '{}', but the destination failed to \
                 activate: {error:#}",
                domain.as_str()
            ));
        }
        if let Some(handoff) = handoff
            && let Err(error) = self
                .finish_planned_ownership_handoff(domain, handoff, None)
                .await
        {
            return command_error(format!(
                "committed placement and schedule for domain '{}', but ownership state activation \
                 did not complete: {error:#}",
                domain.as_str()
            ));
        }

        command_ok(format!(
            "set domain '{}' placement to {}; {}\nplanned relocations: {relocations}",
            domain.as_str(),
            alter.policy.as_ref(),
            quiesce_level_message(quiesce_level),
        ))
    }

    pub(in crate::application) async fn resolve_domain_start(
        &self,
        domain_id: &DomainName,
        domain: &DomainState,
        requested_start: &DomainStartPoint,
    ) -> Result<ResolvedDomainStart, Report<TimestampError>> {
        let wall_started_at = current_timestamp();
        let (mut logical_start, time_rate) = requested_start.resolve_at(wall_started_at);
        if domain.config.pace.is_paced()
            && let DomainStartPoint::Resume = requested_start
            && let Ok(Some(resume_at)) = self.inner.runtime.current_paced_domain_time(domain_id)
        {
            logical_start = resume_at;
        }
        #[cfg(feature = "testing")]
        let wall_started_at = if domain.config.pace.is_paced()
            && let DomainStartPoint::Now { .. } | DomainStartPoint::At { .. } = requested_start
            && let Some(initial_elapsed) = self
                .inner
                .runtime
                .take_domain_clock_initial_elapsed(domain_id)
        {
            wall_started_at.checked_sub(initial_elapsed)?
        } else {
            wall_started_at
        };
        let concrete_start = match requested_start {
            DomainStartPoint::Resume => DomainStartPoint::Resume,
            DomainStartPoint::Now { .. } => DomainStartPoint::At {
                timestamp: logical_start,
                time_rate,
            },
            DomainStartPoint::At { .. } => requested_start.clone(),
        };
        Ok(ResolvedDomainStart {
            concrete_start,
            clock: DomainClockState::new(wall_started_at, logical_start, time_rate),
        })
    }

    pub(in crate::application) async fn start_domain(
        &self,
        domain_id: &DomainName,
        start: StartDomain,
    ) -> CommandResult {
        let Some(domain) = self.inner.consensus.current_domain(domain_id).await else {
            return command_error(format!("domain '{}' does not exist", domain_id.as_str()));
        };
        if let DomainStatus::Running = domain.status {
            return command_error(format!(
                "domain '{}' is already running",
                domain_id.as_str()
            ));
        }
        if let DomainStatus::Paused = domain.status {
            return command_error(format!(
                "domain '{}' is paused for a model alteration",
                domain_id.as_str()
            ));
        }
        let authority = if domain.config.pace.is_paced() {
            let Some(authority) = self.selected_domain_clock_authority(domain_id).await else {
                return command_error(format!(
                    "no live voter is available to own the clock for domain '{}'",
                    domain_id.as_str()
                ));
            };
            Some(authority)
        } else {
            None
        };
        let resolved_start = match self
            .resolve_domain_start(domain_id, &domain, &start.start)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => {
                return command_error(format!(
                    "failed to construct domain clock start for '{}': {error}",
                    domain_id.as_str()
                ));
            }
        };
        match self
            .inner
            .consensus
            .start_domain(
                domain_id.clone(),
                resolved_start.concrete_start,
                domain
                    .config
                    .pace
                    .is_paced()
                    .then_some(resolved_start.clock.clone()),
                authority,
                None,
            )
            .await
        {
            Ok(()) => {
                if let Err(error) = self.apply_current_cluster_state().await {
                    let rollback = self.roll_back_started_domain(domain_id).await;
                    return command_error(format!(
                        "failed to start domain '{}': {error:#}{rollback}",
                        domain_id.as_str()
                    ));
                }
                command_ok(format!("started domain '{}'", domain_id.as_str()))
            }
            Err(error) => {
                self.consensus_report_response(
                    &error,
                    format!("failed to start domain '{}'", domain_id.as_str()),
                )
                .await
            }
        }
    }

    pub(in crate::application) async fn stop_domain(
        &self,
        domain_id: &DomainName,
        _stop: StopDomain,
    ) -> CommandResult {
        let Some(domain) = self.inner.consensus.current_domain(domain_id).await else {
            return command_error(format!("domain '{}' does not exist", domain_id.as_str()));
        };
        if let DomainStatus::Stopped = domain.status {
            return command_error(format!(
                "domain '{}' is already stopped",
                domain_id.as_str()
            ));
        }
        match self
            .inner
            .consensus
            .stop_domain(domain_id.clone(), None)
            .await
        {
            Ok(()) => {
                if let Err(error) = self.apply_current_cluster_state().await {
                    return command_error(format!(
                        "stopped domain '{}', but remote stopping did not complete: {error:#}",
                        domain_id.as_str()
                    ));
                }
                command_ok(format!("stopped domain '{}'", domain_id.as_str()))
            }
            Err(error) => {
                self.consensus_report_response(
                    &error,
                    format!("failed to stop domain '{}'", domain_id.as_str()),
                )
                .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;
    #[cfg(feature = "testing")]
    use nervix_consensus::{ConsensusError, ConsensusTestProbe, StorageBoundary};
    use nervix_models::{DomainConfig, DomainPace, PlacementPolicy};
    use nervix_recovery::Discarded as _;

    #[cfg(feature = "testing")]
    use super::super::test_fixtures::{
        build_test_service_with_fault_injection, build_test_service_with_probe,
    };
    use super::{
        super::test_fixtures::{TestService, build_test_service},
        *,
    };

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn start_domain_storage_failure_preserves_stopped_state() {
        let probe = ConsensusTestProbe::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_probe(true, probe.clone()).await;
        let domain = DomainName::parse("default").assured("the test domain name is valid");
        let storage_fault = probe.storage_fault();

        storage_fault.fail_next(
            "start-domain:default".to_string(),
            StorageBoundary::BeforeCommit,
        );
        let start = service
            .start_domain(
                &domain,
                StartDomain {
                    start: DomainStartPoint::Resume,
                },
            )
            .await;
        assert!(!start.succeeded(), "{start:?}");
        assert!(start.message.contains("consensus storage"), "{start:?}");
        assert!(matches!(
            service
                .inner
                .consensus
                .current_domain(&domain)
                .await
                .map(|state| state.status),
            Some(DomainStatus::Stopped)
        ));

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn pause_domain_storage_failure_retains_typed_cause_and_running_state() {
        let probe = ConsensusTestProbe::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_probe(true, probe.clone()).await;
        let domain = DomainName::parse("default").assured("the test domain name is valid");
        let start = service
            .start_domain(
                &domain,
                StartDomain {
                    start: DomainStartPoint::Resume,
                },
            )
            .await;
        assert!(start.succeeded(), "{start:?}");
        probe.storage_fault().fail_next(
            "pause-domain:default".to_string(),
            StorageBoundary::BeforeCommit,
        );
        let error = service
            .pause_and_drain_domain_for_alter(&domain, None, None)
            .await
            .err()
            .expect("a failed durable pause must be reported");
        assert!(matches!(
            error.downcast_ref::<ConsensusError>(),
            Some(ConsensusError::Storage | ConsensusError::RaftStorage)
        ));
        assert!(error.to_string().contains("failed to pause domain"));
        assert!(matches!(
            service
                .inner
                .consensus
                .current_domain(&domain)
                .await
                .map(|state| state.status),
            Some(DomainStatus::Running)
        ));

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn stop_domain_storage_failure_preserves_running_state() {
        let probe = ConsensusTestProbe::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_probe(true, probe.clone()).await;
        let domain = DomainName::parse("default").assured("the test domain name is valid");
        let start = service
            .start_domain(
                &domain,
                StartDomain {
                    start: DomainStartPoint::Resume,
                },
            )
            .await;
        assert!(start.succeeded(), "{start:?}");

        probe.storage_fault().fail_next(
            "stop-domain:default".to_string(),
            StorageBoundary::BeforeCommit,
        );
        let stop = service.stop_domain(&domain, StopDomain).await;
        assert!(!stop.succeeded(), "{stop:?}");
        assert!(stop.message.contains("consensus storage"), "{stop:?}");
        assert!(matches!(
            service
                .inner
                .consensus
                .current_domain(&domain)
                .await
                .map(|state| state.status),
            Some(DomainStatus::Running)
        ));

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn domain_placement_storage_failure_preserves_previous_policy() {
        let probe = ConsensusTestProbe::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_probe(true, probe.clone()).await;
        let domain = DomainName::parse("default").assured("the test domain name is valid");
        probe.storage_fault().fail_next(
            "put-domain-and-schedule:default".to_string(),
            StorageBoundary::BeforeCommit,
        );

        let result = service
            .alter_domain(
                &domain,
                AlterDomain {
                    policy: PlacementPolicy::RequireColocation,
                },
            )
            .await;
        assert!(!result.succeeded(), "{result:?}");
        assert!(result.message.contains("consensus storage"), "{result:?}");
        assert_eq!(
            service
                .inner
                .consensus
                .current_domain(&domain)
                .await
                .assured("the domain remains present")
                .config
                .placement,
            PlacementPolicy::Neutral
        );

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn create_domain_storage_failure_keeps_domain_absent() {
        let probe = ConsensusTestProbe::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_probe(false, probe.clone()).await;
        let domain = DomainName::parse("failed_domain").assured("the test domain name is valid");
        probe.storage_fault().fail_next(
            "put-domain:failed_domain".to_string(),
            StorageBoundary::BeforeCommit,
        );

        let result = service
            .create_domain(CreateStatement::new(
                CreateDomain {
                    id: domain.clone(),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
                        placement: PlacementPolicy::Neutral,
                    },
                },
                false,
            ))
            .await;
        assert!(!result.succeeded(), "{result:?}");
        assert!(result.message.contains("consensus storage"), "{result:?}");
        assert!(
            service
                .inner
                .consensus
                .current_domain(&domain)
                .await
                .is_none()
        );

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }

    #[nervix_primitives::test]
    async fn create_domain_if_not_exists_returns_already_existed() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;

        let first = service
            .create_domain(CreateStatement::new(
                CreateDomain {
                    id: DomainName::parse("prod").expect("valid domain"),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
                        placement: nervix_models::PlacementPolicy::Neutral,
                    },
                },
                false,
            ))
            .await;
        assert!(first.succeeded());
        assert!(!first.found_existing());

        let duplicate = service
            .create_domain(CreateStatement::new(
                CreateDomain {
                    id: DomainName::parse("prod").expect("valid domain"),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
                        placement: nervix_models::PlacementPolicy::Neutral,
                    },
                },
                true,
            ))
            .await;
        assert!(duplicate.succeeded());
        assert!(duplicate.found_existing());
        assert!(duplicate.message.contains("already exists"));

        let _ = std::fs::remove_dir_all(&path);
    }

    #[nervix_primitives::test]
    async fn admitted_domain_creation_resumes_after_its_domain_record_exists() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        let state = DomainState {
            id: DomainName::parse("resumed")
                .assured("the test domain is an identifier-shaped literal"),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                placement: nervix_models::PlacementPolicy::Neutral,
            },
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: DomainStartPoint::Resume,
            clock: None,
        };

        let first = service
            .apply_persistent_domain_creation(false, false, state.clone(), None)
            .await;
        assert!(first.succeeded(), "{first:?}");
        let resumed = service
            .apply_persistent_domain_creation(false, false, state, None)
            .await;
        assert_eq!(resumed, first);

        let _ = std::fs::remove_dir_all(&path);
    }

    #[nervix_primitives::test]
    async fn domain_placement_commit_uses_its_captured_planning_basis() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let domain = DomainName::parse("default").assured("the test domain name is valid");

        let result = service
            .alter_domain(
                &domain,
                AlterDomain {
                    policy: PlacementPolicy::RequireColocation,
                },
            )
            .await;

        assert!(
            result.succeeded(),
            "placement alteration failed: {result:?}"
        );
        assert_eq!(
            service
                .inner
                .consensus
                .current_domain(&domain)
                .await
                .assured("the altered domain remains present")
                .config
                .placement,
            PlacementPolicy::RequireColocation
        );

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }

    #[test]
    fn only_a_backup_cut_bounds_its_pause_application_by_its_deadline() {
        let deadline = Instant::now();
        assert_eq!(
            DomainDrainMode::Alteration {
                timeout: Duration::from_secs(30),
            }
            .application_bound(),
            ApplicationBound::ClusterBudget
        );
        assert_eq!(
            DomainDrainMode::Backup { deadline }.application_bound(),
            ApplicationBound::Deadline(deadline)
        );
    }

    /// Starts the default domain of a service whose runtime consults `faults`.
    #[cfg(feature = "testing")]
    async fn running_default_domain(faults: &crate::FaultInjection) -> (TestService, DomainName) {
        let test_service = build_test_service_with_fault_injection(true, faults.clone()).await;
        let domain = DomainName::parse("default").assured("the test domain name is valid");
        let start = test_service
            .service
            .start_domain(
                &domain,
                StartDomain {
                    start: DomainStartPoint::Resume,
                },
            )
            .await;
        assert!(start.succeeded(), "{start:?}");
        (test_service, domain)
    }

    /// The domain's committed status.
    #[cfg(feature = "testing")]
    async fn committed_status(service: &SessionServiceImpl, domain: &DomainName) -> DomainStatus {
        let state = service
            .inner
            .consensus
            .current_domain(domain)
            .await
            .assured("the test domain exists");
        state.status
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn a_backup_pause_that_commits_after_its_deadline_is_resumed_before_any_installation() {
        let faults = crate::FaultInjection::default();
        let (
            TestService {
                service,
                registry,
                path,
            },
            domain,
        ) = running_default_domain(&faults).await;

        // The deadline has passed before the pause commits, so the cut waits for no application.
        let deadline = Instant::now();
        let error = service
            .pause_and_drain_domain(&domain, None, None, DomainDrainMode::Backup { deadline })
            .await
            .err()
            .assured("a pause applied after its cut's deadline is refused");
        let DomainAlterError::BackupPauseTimeout { domain: refused } = error.current_context()
        else {
            panic!("the cut's deadline ended its pause's application: {error:#}");
        };
        assert_eq!(refused, &domain);
        let Some(RuntimeError::ApplicationDeadline { wait }) = error.downcast_ref::<RuntimeError>()
        else {
            panic!("the wait the deadline ended remains beneath: {error:#}");
        };
        assert_eq!(*wait, crate::runtime::ApplicationWait::Installation);
        assert!(matches!(
            committed_status(&service, &domain).await,
            DomainStatus::Running
        ));

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }

    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn a_backup_pause_application_held_past_its_deadline_ends_there_and_the_domain_resumes() {
        let faults = crate::FaultInjection::default();
        let (
            TestService {
                service,
                registry,
                path,
            },
            domain,
        ) = running_default_domain(&faults).await;
        // The gate holds every wait for a revision that pauses the domain until it is released, so
        // the cluster never applies the cut's pause and only the cut's deadline ends the wait.
        faults.pause_backup_cut_on(
            domain.clone(),
            crate::fault_injection::BackupCutBoundary::PauseApplication,
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        let error = service
            .pause_and_drain_domain(&domain, None, None, DomainDrainMode::Backup { deadline })
            .await
            .err()
            .assured("a pause the cluster does not apply by the cut's deadline is refused");
        let DomainAlterError::BackupPauseTimeout { domain: refused } = error.current_context()
        else {
            panic!("the cut's deadline ended its pause's application: {error:#}");
        };
        assert_eq!(refused, &domain);
        assert!(
            matches!(
                error.downcast_ref::<RuntimeError>(),
                Some(RuntimeError::ApplicationDeadline { .. })
            ),
            "the wait the deadline ended remains beneath: {error:#}"
        );
        // The resume applied while the gate still holds any wait for the paused revision.
        assert!(matches!(
            committed_status(&service, &domain).await,
            DomainStatus::Running
        ));
        assert_eq!(faults.release_all_backup_cut_pauses(), 1);

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }
}
