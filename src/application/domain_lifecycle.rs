//! Creating, altering, starting and stopping a domain.
//!
//! Layer: control plane.
//!
//! - **Owns.** The domain lifecycle commands, the pause and drain an alteration needs, and the
//!   rollback when one of its steps fails.
//! - **Depends on.** Consensus for the domain record, the registry for its models, and the entity
//!   gate to hold the domain while it changes.
//! - **Must not know.** How the domain's graph is scheduled or executed.

use error_stack::Report;
use nervix_consensus::{ConsensusError, DomainMutationLease};
use nervix_interconnect::DomainDrainStatusEnvelope;
use nervix_models::{
    AlterDomain, ClusterNodeName, CreateDomain, CreateStatement, DomainClockState, DomainName,
    DomainStartPoint, DomainState, DomainStatus, QuiesceLevel, StartDomain, StopDomain,
    TimestampError,
};
use thiserror::Error;
use tokio::time::{Duration, interval};

use super::{
    command_result::CommandResult,
    domain_clock::current_timestamp,
    entity_gate::DrainOutstanding,
    model_mutation::{
        command_error, command_ok, command_ok_already_existed, quiesce_level_message,
    },
    ownership_handoff::{mark_complete_ownership_transitions, planned_relocation_count},
    session_service::SessionServiceImpl,
    transaction::{QuiescenceAttempt, TransactionStepImpactRecorder},
};
#[derive(Debug, Error)]
pub(in crate::application) enum DomainAlterError {
    #[error("domain '{domain}' already has a model alteration in progress")]
    ConcurrentAlter { domain: DomainName },
    #[error("{outstanding}")]
    QuiesceTimeout { outstanding: DrainOutstanding },
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
    #[error("failed to pause domain '{domain}' for model alteration: {reason}")]
    PauseDomain { domain: DomainName, reason: String },
    #[error("failed to stop ingestion in domain '{domain}' for model alteration: {reason}")]
    StopIngestion { domain: DomainName, reason: String },
    #[error("failed to resume domain '{domain}' after model alteration: {reason}")]
    ResumeDomain { domain: DomainName, reason: String },
    #[error("failed to restore ingestion in domain '{domain}' after model alteration: {reason}")]
    RestoreIngestion { domain: DomainName, reason: String },
    #[error("failed to roll back model alteration in domain '{domain}': {reason}")]
    Rollback { domain: DomainName, reason: String },
}

pub(in crate::application) struct ResolvedDomainStart {
    pub(in crate::application) concrete_start: DomainStartPoint,
    pub(in crate::application) clock: DomainClockState,
}

impl SessionServiceImpl {
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
                    "domain '{}' exists, but its stopped state is not usable everywhere: {error}",
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
                        .consensus_error_response(
                            &error,
                            format!("failed to create domain '{}': {error}", state.id.as_str()),
                        )
                        .await;
                }
            }
        }
        if let Err(error) = self.apply_current_cluster_state().await {
            return command_error(format!(
                "created domain '{}', but its stopped state failed to become usable: {error}",
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
            let reason = error.to_string();
            if let (Some(impact), Some(attempt)) = (impact, attempt) {
                if matches!(&error, ConsensusError::Conflict(_)) {
                    impact.fail(
                        attempt,
                        nervix_models::ImpactDiagnosticKind::Quiescence,
                        reason.clone(),
                    );
                } else {
                    impact.uncertain(
                        attempt,
                        nervix_models::ImpactDiagnosticKind::Quiescence,
                        reason.clone(),
                    );
                }
            }
            return Err(
                Report::new(error).change_context(DomainAlterError::PauseDomain {
                    domain: domain.clone(),
                    reason,
                }),
            );
        }
        if let (Some(impact), Some(attempt)) = (impact, attempt) {
            impact.confirm(attempt);
        }

        if let Err(error) = self.apply_current_cluster_state().await {
            if let (Some(impact), Some(attempt)) = (impact, attempt) {
                impact.fail(
                    attempt,
                    nervix_models::ImpactDiagnosticKind::Quiescence,
                    error.to_string(),
                );
            }
            return Err(self
                .abort_domain_alter_pause(
                    domain,
                    mutation,
                    impact.zip(attempt),
                    Report::new(DomainAlterError::StopIngestion {
                        domain: domain.clone(),
                        reason: error.to_string(),
                    }),
                )
                .await);
        }

        match self.wait_for_paused_domain_drain(domain).await {
            Ok(()) => Ok(attempt),
            Err(reason) => {
                if let (Some(impact), Some(attempt)) = (impact, attempt) {
                    impact.fail(
                        attempt,
                        nervix_models::ImpactDiagnosticKind::Quiescence,
                        reason.to_string(),
                    );
                }
                Err(self
                    .abort_domain_alter_pause(domain, mutation, impact.zip(attempt), reason)
                    .await)
            }
        }
    }

    pub(in crate::application) async fn wait_for_paused_domain_drain(
        &self,
        domain: &DomainName,
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

        let deadline = tokio::time::Instant::now() + self.inner.runtime.domain_drain_timeout();
        let mut polling = interval(Duration::from_millis(50));
        polling.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_pending = None::<(ClusterNodeName, DomainDrainStatusEnvelope)>;
        let mut last_status_error = None;

        loop {
            tokio::task::consume_budget().await;
            polling.tick().await;
            let mut all_drained = true;
            for node in &nodes {
                tokio::task::consume_budget().await;
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
                        last_status_error = Some(error);
                        if tokio::time::Instant::now() >= deadline {
                            break;
                        }
                    }
                }
            }
            if all_drained {
                return Ok(());
            }
            if tokio::time::Instant::now() < deadline {
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
                        resume_error.to_string(),
                    );
                }
                let reason = format!("{reason}; automatic resume failed: {resume_error}");
                resume_error.change_context(DomainAlterError::Rollback {
                    domain: domain.clone(),
                    reason,
                })
            }
        }
    }

    pub(in crate::application) async fn resume_domain_after_alter(
        &self,
        domain: &DomainName,
        mutation: Option<&DomainMutationLease>,
    ) -> Result<(), Report<DomainAlterError>> {
        self.inner
            .consensus
            .resume_domain(domain.clone(), mutation)
            .await
            .map_err(|error| {
                let reason = error.to_string();
                Report::new(error).change_context(DomainAlterError::ResumeDomain {
                    domain: domain.clone(),
                    reason,
                })
            })?;
        self.apply_current_cluster_state().await.map_err(|error| {
            Report::new(DomainAlterError::RestoreIngestion {
                domain: domain.clone(),
                reason: error.to_string(),
            })
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
                    error.to_string(),
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
            failures.push(format!("reapplying the cluster state failed: {error}"));
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
                         {error}",
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
                         {error}",
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
                self.consensus_error_response(
                    &error,
                    format!("failed to create domain '{}': {error}", create.id.as_str()),
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
                     {error}",
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
        let mut next_schedule = self.inner.registry.active_graph(domain).map(|graph| {
            #[cfg(feature = "testing")]
            let mut schedule = graph.schedule_for_domain_with_mode(
                domain,
                cluster_nodes,
                self.inner.replica_count,
                alter.policy,
                self.inner.runtime.scheduler_mode(),
            );
            #[cfg(not(feature = "testing"))]
            let mut schedule = graph.schedule_for_domain(
                domain,
                cluster_nodes,
                self.inner.replica_count,
                alter.policy,
            );
            Self::merge_existing_schedule_data(
                &mut schedule,
                previous_schedule.as_ref(),
                live_voters,
            );
            schedule
        });
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
                .consensus_error_response(
                    &error,
                    format!(
                        "failed to alter placement for domain '{}': {error}",
                        domain.as_str()
                    ),
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
                 activate: {error}",
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
                 did not complete: {error}",
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
                        "failed to start domain '{}': {error}{rollback}",
                        domain_id.as_str()
                    ));
                }
                command_ok(format!("started domain '{}'", domain_id.as_str()))
            }
            Err(error) => {
                self.consensus_error_response(
                    &error,
                    format!("failed to start domain '{}': {error}", domain_id.as_str()),
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
                        "stopped domain '{}', but remote stopping did not complete: {error}",
                        domain_id.as_str()
                    ));
                }
                command_ok(format!("stopped domain '{}'", domain_id.as_str()))
            }
            Err(error) => {
                self.consensus_error_response(
                    &error,
                    format!("failed to stop domain '{}': {error}", domain_id.as_str()),
                )
                .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{DomainConfig, DomainPace, PlacementPolicy};
    use nervix_recovery::Discarded as _;

    use super::{
        super::test_fixtures::{TestService, build_test_service},
        *,
    };

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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
}
