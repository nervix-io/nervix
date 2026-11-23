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
use nervix_consensus::{
    ConsensusConflict, ConsensusError, DomainMutationLease, OrphanedPauseRecovery,
};
use nervix_interconnect::{
    DomainDrainStatusEnvelope, RemoteOperationFailure,
    backup::{BackupDrainAction, BackupDrainStatus, BackupDrainStatusRequest},
};
use nervix_models::{
    ClusterNodeName, CoordinationIdentity, CreateDomain, CreateStatement, DomainClockState,
    DomainName, DomainStartPoint, DomainState, DomainStatus, QuiesceLevel, TimestampError,
};
use nervix_primitives::time::{Instant, interval};
use thiserror::Error;

use super::{
    backup::cut_reach::CutReach,
    command_result::{CommandDisposition, CommandResult, OutcomeUnknownCause},
    domain_clock::current_timestamp,
    entity_gate::DrainOutstanding,
    model_mutation::{command_error, command_ok, command_ok_already_existed},
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
    /// The licensing stop gate refused the resume, and the licensing stop that ends the pause
    /// instead failed. The consensus report of the stop is beneath.
    #[error("failed to stop domain '{domain}' for licensing after model alteration")]
    LicensingStop { domain: DomainName },
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

/// What a command that found its domain paused by an alteration does next.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::application) enum PausedDomainRecovery {
    /// The pause was orphaned and is resumed, or the domain changed meanwhile: look again.
    Replan,
    /// The pause is not the command's to resume; the command keeps its paused-domain refusal.
    RetainRefusal,
}

pub(in crate::application) struct ResolvedDomainStart {
    pub(in crate::application) concrete_start: DomainStartPoint,
    pub(in crate::application) clock: DomainClockState,
}

impl SessionServiceImpl {
    /// The error this leader's consensus refuses a write with, before appending it, when a test
    /// build armed a model publication refusal of this kind. A lapsed lease is observed against the
    /// node's current tenure, as a refusal Raft returned would be. A saturated log is the node's own
    /// retained log cap exceeded by one byte once its retention admission wait has passed, which a
    /// test arms only on a node whose cap a log can exceed.
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
                use meticulous::OptionExt as _;

                let retention = self.inner.consensus.raft_retention_policy();
                let cap = retention.retained_log_cap_bytes;
                let retained = cap.checked_add(1).assured(
                    "a test arms a saturated-log refusal only on a node whose retained log cap a \
                     log can exceed",
                );
                ConsensusError::LogRetentionSaturated {
                    retained,
                    cap,
                    waited: retention.retention_admission_timeout,
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
        domain_quota: crate::licensing::Quota,
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
                    .put_domain(state.clone(), mutation, domain_quota)
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

        #[cfg(feature = "testing")]
        if let DomainDrainMode::Backup { .. } = mode {
            self.inner
                .runtime
                .pause_backup_cut_if_armed(
                    domain,
                    crate::fault_injection::BackupCutBoundary::PauseCommitted,
                )
                .await;
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
        let mut polling = interval(self.cluster_parameters().domain_drain_poll_interval());
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
        let mut polling = interval(self.cluster_parameters().domain_drain_poll_interval());
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
        let resumed = match self
            .inner
            .consensus
            .resume_domain(domain.clone(), mutation)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                if let ConsensusError::Conflict(ConsensusConflict::LicensingStop { reason }) =
                    error.current_context()
                {
                    // The licensing stop gate closed while this operation held the domain paused,
                    // so the domain may not run again. Its pause ends in the licensing stop the
                    // leader would make once the lease is released, under this same lease, and
                    // the operation completes with the domain stopped until an operator starts it.
                    // The pause becomes the licensing stop's request, which drains what is
                    // still in motion and is forced at the fixed time after the expiry.
                    self.inner
                        .consensus
                        .request_domain_stop(
                            domain.clone(),
                            nervix_models::StopRequest::Licensing(*reason),
                            mutation,
                        )
                        .await
                        .change_context_lazy(|| DomainAlterError::LicensingStop {
                            domain: domain.clone(),
                        })
                } else {
                    Err(error.change_context(DomainAlterError::ResumeDomain {
                        domain: domain.clone(),
                    }))
                }
            }
        };
        #[cfg(feature = "testing")]
        if resumed.is_err() {
            Box::pin(self.inner.runtime.pause_domain_resume_if_armed(
                self.inner.consensus.local_node_id(),
                domain,
                crate::fault_injection::DomainResumeBoundary::Failure,
            ))
            .await;
        }
        resumed?;
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
        let domain_quota = match self.admit_domain_creation(&create.id, false) {
            Ok(quota) => quota,
            Err(error) => return command_error(format!("{error:#}")),
        };
        let state = DomainState {
            id: create.id.clone(),
            config: create.config,
            labels: create.labels,
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: DomainStartPoint::Resume,
            clock: None,
            stop: None,
        };
        match self
            .inner
            .consensus
            .put_domain(state, None, domain_quota)
            .await
        {
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

    /// Resumes the pause of `domain` an alteration left behind when no committing transaction and
    /// no alteration on this node owns it any more, under `mutation`, the mutation lease of the
    /// command that found the domain paused. Consensus checks the lease, the restore start gate,
    /// the licensing stop gate and the domain's committing transactions in the same applied
    /// revision.
    pub(in crate::application) async fn recover_paused_domain(
        &self,
        domain: &DomainName,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<PausedDomainRecovery, Box<CommandResult>> {
        if self.inner.runtime.domain_alter_is_active(domain) {
            return Ok(PausedDomainRecovery::RetainRefusal);
        }
        let Some(mutation) = mutation else {
            return Ok(PausedDomainRecovery::RetainRefusal);
        };
        let recovered = self
            .inner
            .consensus
            .recover_orphaned_model_pause(domain.clone(), mutation)
            .await;
        match recovered {
            Ok(OrphanedPauseRecovery::Resumed) => {
                tracing::info!(domain = domain.as_str(), "resumed orphaned ALTER pause");
                if let Err(error) = self.apply_current_cluster_state().await {
                    return Err(Box::new(CommandResult::new(
                        CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::StillApplying),
                        format!(
                            "orphaned pause recovery is committed but runtime application is \
                             incomplete: {error:#}; retry with the same execution reference"
                        ),
                    )));
                }
                Ok(PausedDomainRecovery::Replan)
            }
            Ok(OrphanedPauseRecovery::Unchanged) => Ok(PausedDomainRecovery::Replan),
            Ok(OrphanedPauseRecovery::TransactionActive { transaction }) => {
                Err(Box::new(CommandResult::new(
                    CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::StillApplying),
                    format!(
                        "transaction '{transaction}' owns the domain pause; retry with the same \
                         execution reference"
                    ),
                )))
            }
            Err(error) => {
                // Neither gate lets the pause resume; the command keeps its paused-domain refusal.
                if let ConsensusError::Conflict(
                    ConsensusConflict::RestoreInstallation { .. }
                    | ConsensusConflict::LicensingStop { .. },
                ) = error.current_context()
                {
                    return Ok(PausedDomainRecovery::RetainRefusal);
                }
                let mut response = self
                    .consensus_error_response(error.current_context(), format!("{error:#}"))
                    .await;
                if let CommandDisposition::Failed = response.disposition {
                    response.disposition =
                        CommandDisposition::OutcomeUnknown(OutcomeUnknownCause::StillApplying);
                    response.message.push_str(
                        "; orphaned pause recovery was not confirmed; retry with the same \
                         execution reference",
                    );
                }
                Err(Box::new(response))
            }
        }
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
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "testing")]
    use meticulous::OptionExt as _;
    use meticulous::ResultExt as _;
    #[cfg(feature = "testing")]
    use nervix_consensus::{ConsensusError, ConsensusTestProbe, StorageBoundary};
    use nervix_models::{DomainConfig, DomainPace};
    #[cfg(feature = "testing")]
    use nervix_recovery::Discarded as _;

    #[cfg(feature = "testing")]
    use super::super::test_fixtures::{
        build_test_service_with_fault_injection, build_test_service_with_probe,
    };
    use super::{
        super::test_fixtures::{TestService, build_test_service},
        *,
    };

    /// A test build refuses a write for a saturated log as this node's consensus would: one byte
    /// past the retained log cap the node resolved, once its retention admission wait has passed.
    /// The fixture's consensus runs the default retention settings.
    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn an_injected_saturated_log_refusal_reports_the_nodes_retention_policy() {
        let TestService {
            service,
            registry,
            path,
        } = build_test_service(false).await;
        let policy = nervix_consensus::RaftRetentionSettings::default()
            .policy(&nervix_consensus::RaftOperationSettings::default());

        let refusal = service
            .refused_write(crate::fault_injection::ModelPublicationRefusalKind::SaturatedLog);

        let ConsensusError::LogRetentionSaturated {
            retained,
            cap,
            waited,
        } = refusal
        else {
            panic!("a saturated log refuses the write as a saturated log: {refusal:?}");
        };
        assert_eq!(cap, policy.retained_log_cap_bytes);
        assert_eq!(Some(retained), cap.checked_add(1));
        assert_eq!(waited, policy.retention_admission_timeout);

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
        service.test_start_domain("default").await;
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
        service.test_start_domain("default").await;

        probe.storage_fault().fail_next(
            "request-domain-stop:default".to_string(),
            StorageBoundary::BeforeCommit,
        );
        let request = nervix_models::StopRequest::Operator {
            reference: nervix_models::CommandExecutionReference::parse("stop-default")
                .assured("the test reference is a valid literal"),
            request: nervix_models::OperatorStopRequest {
                requested_at: nervix_models::Timestamp::from_unix_nanos(1),
                limit: None,
            },
        };
        let stop = service
            .inner
            .consensus
            .request_domain_stop(domain.clone(), request, None)
            .await;
        let error = stop.expect_err("a stop request whose write fails is reported");
        assert!(
            matches!(
                error.current_context(),
                ConsensusError::Storage | ConsensusError::RaftStorage
            ),
            "{error:?}"
        );
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
                    labels: nervix_models::EntityLabels::default(),
                    id: domain.clone(),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
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
                    labels: nervix_models::EntityLabels::default(),
                    id: DomainName::parse("prod").expect("valid domain"),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
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
                    labels: nervix_models::EntityLabels::default(),
                    id: DomainName::parse("prod").expect("valid domain"),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
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
            labels: nervix_models::EntityLabels::default(),
            id: DomainName::parse("resumed")
                .assured("the test domain is an identifier-shaped literal"),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
            },
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: DomainStartPoint::Resume,
            clock: None,
            stop: None,
        };

        let first = service
            .apply_persistent_domain_creation(
                false,
                false,
                state.clone(),
                None,
                service.domain_quota(),
            )
            .await;
        assert!(first.succeeded(), "{first:?}");
        let resumed = service
            .apply_persistent_domain_creation(false, false, state, None, service.domain_quota())
            .await;
        assert_eq!(resumed, first);

        let _ = std::fs::remove_dir_all(&path);
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
        test_service.service.test_start_domain("default").await;
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
