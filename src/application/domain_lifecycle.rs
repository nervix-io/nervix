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
use nervix_interconnect::DomainDrainStatusEnvelope;
use nervix_models::{
    AlterDomain, ClusterNodeName, CreateDomain, CreateStatement, DomainClockState, DomainName,
    DomainPace, DomainStartPoint, DomainState, DomainStatus, QuiesceLevel, StartDomain, StopDomain,
    TimestampError,
};
use thiserror::Error;
use tokio::time::{Duration, interval};

use super::{
    domain_clock::current_timestamp,
    entity_gate::DrainOutstanding,
    model_mutation::{
        command_error, command_ok, command_ok_already_existed, quiesce_level_message,
    },
    model_validation::validate_domain_config,
    ownership_handoff::{mark_complete_ownership_transitions, planned_relocation_count},
    session_service::SessionServiceImpl,
};
use crate::proto::CommandResult;
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

#[derive(Debug, Error)]
pub(in crate::application) enum ActiveDomainError {
    #[error("invalid active domain")]
    Invalid,
    #[error("domain '{domain}' does not exist")]
    NotFound { domain: DomainName },
}

pub(in crate::application) struct ResolvedDomainStart {
    pub(in crate::application) concrete_start: DomainStartPoint,
    pub(in crate::application) clock: DomainClockState,
}

impl SessionServiceImpl {
    pub(in crate::application) async fn pause_and_drain_domain_for_alter(
        &self,
        domain: &DomainName,
    ) -> Result<(), Report<DomainAlterError>> {
        self.inner
            .consensus
            .pause_domain(domain.clone())
            .await
            .map_err(|error| {
                let reason = error.to_string();
                Report::new(error).change_context(DomainAlterError::PauseDomain {
                    domain: domain.clone(),
                    reason,
                })
            })?;

        if let Err(error) = self.apply_current_cluster_state().await {
            return Err(self
                .abort_domain_alter_pause(
                    domain,
                    Report::new(DomainAlterError::StopIngestion {
                        domain: domain.clone(),
                        reason: error.to_string(),
                    }),
                )
                .await);
        }

        match self.wait_for_paused_domain_drain(domain).await {
            Ok(()) => Ok(()),
            Err(reason) => Err(self.abort_domain_alter_pause(domain, reason).await),
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
        reason: Report<DomainAlterError>,
    ) -> Report<DomainAlterError> {
        match self.resume_domain_after_alter(domain).await {
            Ok(()) => reason,
            Err(resume_error) => {
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
    ) -> Result<(), Report<DomainAlterError>> {
        self.inner
            .consensus
            .resume_domain(domain.clone())
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

    /// Stops a domain whose start could not be completed, and says so when the stop fails too.
    ///
    /// The caller is on its way to returning the start failure, and this rollback is what keeps
    /// the cluster from holding a domain the operator was told did not start. A rollback that
    /// fails leaves exactly that state, so the reason is appended to the caller's message rather
    /// than dropped: nothing else in the command's answer would mention it.
    async fn roll_back_started_domain(&self, domain_id: &DomainName) -> String {
        let mut failures = Vec::new();
        if let Err(error) = self.inner.consensus.stop_domain(domain_id.clone()).await {
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
        self.publish_domain_schedule(domain, runtime_changes.graph)
            .await
            .map_err(|error| {
                Report::new(DomainAlterError::Rollback {
                    domain: domain.clone(),
                    reason: format!("old schedule restore failed: {error}"),
                })
            })?;
        if classified_level.requires_domain_pause() {
            self.resume_domain_after_alter(domain).await
        } else {
            Ok(())
        }
    }

    pub(in crate::application) async fn reconcile_running_domain_runtime(
        &self,
        domain: &DomainName,
    ) -> Result<(), String> {
        let state = self.inner.consensus.current_runtime_state().await;
        let Some(domain_state) = state.domains.get(domain) else {
            return Ok(());
        };
        if !matches!(domain_state.status, DomainStatus::Running) {
            return Ok(());
        }
        self.inner
            .runtime
            .apply_cluster_state(
                self.inner.consensus.local_node_id(),
                state.revision,
                &state.domains,
                &state.domain_clock_authorities,
                &state.schedule,
            )
            .await
            .map_err(|error| {
                format!(
                    "failed to restore runtime for running domain '{}': {error}",
                    domain.as_str()
                )
            })?;
        self.inner
            .runtime
            .start_running_domain_ingestors()
            .await
            .map_err(|error| {
                format!(
                    "failed to restore runtime for running domain '{}': {error}",
                    domain.as_str()
                )
            })
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
                return command_ok_already_existed(format!(
                    "domain '{}' already exists",
                    create.id.as_str()
                ));
            }
            return command_error(format!("domain '{}' already exists", create.id.as_str()));
        }
        if let Err(message) = validate_domain_config(&create.config) {
            return command_error(message);
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
        match self.inner.consensus.put_domain(state).await {
            Ok(()) => {
                if let Err(error) = self.apply_current_cluster_state().await {
                    self.broadcast_error(format!(
                        "failed to reconcile runtime after creating domain '{}': {error}",
                        create.id.as_str(),
                    ));
                }
                command_ok(format!("created domain '{}'", create.id.as_str()))
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
        let Some(previous_state) = self.inner.consensus.current_domain(domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        if let DomainStatus::Paused = previous_state.status {
            return command_error(format!(
                "domain '{}' is paused by a model alteration",
                domain.as_str()
            ));
        }
        if previous_state.config.placement == alter.policy {
            return command_ok(format!(
                "domain '{}' placement is already {}; {}\nplanned relocations: 0",
                domain.as_str(),
                alter.policy.as_ref(),
                quiesce_level_message(QuiesceLevel::Dynamic),
            ));
        }

        let current_schedule = self.inner.consensus.current_schedule().await;
        let previous_schedule = current_schedule.domain(domain).cloned();
        let live_node_ids = self.inner.cluster.live_node_ids().await;
        let live_voters = self
            .inner
            .consensus
            .live_voter_ids(live_node_ids.clone())
            .await;
        let cluster_nodes = self
            .inner
            .consensus
            .schedulable_live_voter_ids(live_node_ids)
            .await;
        let mut next_schedule = self.inner.registry.active_graph(domain).map(|graph| {
            #[cfg(feature = "testing")]
            let mut schedule = graph.schedule_for_domain_with_mode(
                domain,
                &cluster_nodes,
                self.inner.replica_count,
                alter.policy,
                self.inner.runtime.scheduler_mode(),
            );
            #[cfg(not(feature = "testing"))]
            let mut schedule = graph.schedule_for_domain(
                domain,
                &cluster_nodes,
                self.inner.replica_count,
                alter.policy,
            );
            Self::merge_existing_schedule_data(
                &mut schedule,
                previous_schedule.as_ref(),
                &live_voters,
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
            .put_domain_and_schedule(
                Some(previous_state),
                previous_schedule,
                next_state,
                next_schedule,
            )
            .await
        {
            if let Some(handoff) = handoff {
                self.abort_planned_ownership_handoff(domain, handoff).await;
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
                self.defer_planned_ownership_handoff_release(domain, handoff, &error);
            }
            return command_error(format!(
                "committed placement and schedule for domain '{}', but the destination failed to \
                 activate: {error}",
                domain.as_str()
            ));
        }
        if let Some(handoff) = handoff
            && let Err(error) = self.finish_planned_ownership_handoff(domain, handoff).await
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
        if let DomainPace::Paced = domain.config.pace
            && let DomainStartPoint::Resume = requested_start
            && let Ok(Some(resume_at)) = self.inner.runtime.current_paced_domain_time(domain_id)
        {
            logical_start = resume_at;
        }
        #[cfg(feature = "testing")]
        let wall_started_at = if let DomainPace::Paced = domain.config.pace
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
        if let Err(message) = validate_domain_config(&domain.config) {
            return command_error(message);
        }
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
        let authority = if let DomainPace::Paced = domain.config.pace {
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
                matches!(domain.config.pace, DomainPace::Paced)
                    .then_some(resolved_start.clock.clone()),
                authority,
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
                command_ok(format!("starting domain '{}'", domain_id.as_str()))
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
        match self.inner.consensus.stop_domain(domain_id.clone()).await {
            Ok(()) => {
                if let Err(error) = self.apply_current_cluster_state().await {
                    self.broadcast_error(format!(
                        "failed to reconcile runtime after stopping domain '{}': {error}",
                        domain_id.as_str(),
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
    use nervix_models::{DomainConfig, DomainPace, DomainStartPoint, StartDomain, Statement};

    use super::{
        super::{
            model_mutation::{requires_existing_domain, requires_runtime_reconcile},
            test_fixtures::{TestService, build_test_service},
        },
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
                        period: "0ms".to_string(),
                        skew: "0ms".to_string(),
                        placement: nervix_models::PlacementPolicy::Neutral,
                    },
                },
                false,
            ))
            .await;
        assert!(first.success);
        assert!(!first.already_existed);

        let duplicate = service
            .create_domain(CreateStatement::new(
                CreateDomain {
                    id: DomainName::parse("prod").expect("valid domain"),
                    config: DomainConfig {
                        pace: DomainPace::Unpaced,
                        period: "0ms".to_string(),
                        skew: "0ms".to_string(),
                        placement: nervix_models::PlacementPolicy::Neutral,
                    },
                },
                true,
            ))
            .await;
        assert!(duplicate.success);
        assert!(duplicate.already_existed);
        assert!(duplicate.message.contains("already exists"));

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn start_domain_does_not_reconcile_runtime_in_generic_pre_dispatch() {
        let statement = Statement::StartDomain(StartDomain {
            start: DomainStartPoint::Resume,
        });
        assert!(requires_existing_domain(&statement));
        assert!(!requires_runtime_reconcile(&statement));
    }
}
