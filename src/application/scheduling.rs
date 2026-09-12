//! Where each runtime node runs, and every event that moves it.
//!
//! Layer: control plane.
//!
//! - **Owns.** Publishing a domain schedule, cordon and drain, relocation, failover onto live
//!   members, and the Kafka partition schedule the leader watches.
//! - **Depends on.** The registry to compute a schedule, consensus to publish it, and the ownership
//!   handoff to move state the schedule moves.
//! - **Must not know.** How a scheduled node executes once it is placed.

use std::{collections::BTreeSet, num::NonZeroU64};

use ahash::{HashMap, HashSet};
use meticulous::OptionExt as _;
use nervix_client_core::Client as NervixClient;
use nervix_consensus::ConsensusError;
use nervix_models::{
    ClusterNodeName, DomainName, IngestSource, IngestorName, KafkaOffsetMode,
    KafkaPartitionSchedule, Model, ModelKind, ModelName, NodeRef, PlacementGroupSchedule,
    PlacementPolicy, QuiesceLevel, ScheduledNode,
};
use rdkafka::{config::ClientConfig, consumer::StreamConsumer};
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{
    background_task::BackgroundTask,
    describe_output::{format_placement_runtime_nodes, placement_group_members_equal},
    model_mutation::{command_error, command_ok, quiesce_level_message},
    ownership_handoff::{
        AssignmentRelocation, DrainMove, format_planned_ownership_move,
        mark_complete_ownership_transitions, planned_ownership_moves, planned_relocation_count,
        prefer_former_owners_as_replicas,
    },
    peer_grpc::{grpc_client_connect_options, grpc_uri_from_advertise_addr},
    session_service::SessionServiceImpl,
};
use crate::{proto::CommandResult, registry::ActiveGraph, runtime::KafkaIngestor};

pub(in crate::application) const LEADER_KAFKA_PARTITION_WATCH_INTERVAL: Duration =
    Duration::from_secs(1);

pub(in crate::application) const RUNTIME_REVISION_READINESS_PROPAGATION_BOUND: Duration =
    Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::application) struct KafkaPartitionWatcherKey {
    domain: DomainName,
    ingestor: IngestorName,
}

/// A domain schedule computed from a candidate graph, with how many runtime nodes it moves off
/// the node that owns them today.
pub(in crate::application) struct PreparedDomainSchedule {
    pub(in crate::application) schedule: Option<nervix_models::DomainSchedule>,
    pub(in crate::application) relocations: usize,
}

/// The schedule change one model mutation makes: what the domain is scheduled as now, what it
/// would be scheduled as, and how many runtime nodes that move relocates.
#[derive(Default)]
pub(in crate::application) struct ScheduleTransition {
    pub(in crate::application) expected_schedule: Option<nervix_models::DomainSchedule>,
    pub(in crate::application) prepared_schedule: Option<nervix_models::DomainSchedule>,
    pub(in crate::application) planned_relocations: usize,
}

/// One Kafka partition watcher the leader runs: the ingestor it watches for, and the task doing
/// the watching.
pub(in crate::application) struct KafkaPartitionWatcherTask {
    spec: KafkaPartitionWatcherSpec,
    pub(in crate::application) task: BackgroundTask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KafkaPartitionWatcherSpec {
    domain: DomainName,
    ingestor: IngestorName,
    topic: String,
    instances: NonZeroU64,
    client: nervix_models::CreateClientKafka,
}

impl SessionServiceImpl {
    pub(in crate::application) async fn publish_domain_schedule(
        &self,
        domain: &DomainName,
        graph: Option<ActiveGraph>,
    ) -> Result<usize, String> {
        #[cfg(feature = "testing")]
        if self
            .inner
            .runtime
            .take_armed_schedule_publication_fault(domain)
        {
            return Err(format!(
                "injected schedule publication fault for domain '{}'",
                domain.as_str()
            ));
        }
        let default_policy = self
            .inner
            .consensus
            .current_domain(domain)
            .await
            .ok_or_else(|| format!("domain '{}' does not exist", domain.as_str()))?
            .config
            .placement;
        let expected_schedule = self
            .inner
            .consensus
            .current_schedule()
            .await
            .domain(domain)
            .cloned();
        let PreparedDomainSchedule {
            schedule,
            relocations,
        } = self
            .prepare_domain_schedule(domain, graph, default_policy)
            .await?;
        self.inner
            .consensus
            .replace_domain_schedule(domain.clone(), expected_schedule, schedule)
            .await
            .map_err(|error| error.to_string())?;
        self.apply_current_cluster_state().await.map_err(|error| {
            format!(
                "failed to instantiate runtime schedule for domain '{}': {error}",
                domain.as_str()
            )
        })?;
        Ok(relocations)
    }

    pub(in crate::application) async fn prepare_domain_schedule(
        &self,
        domain: &DomainName,
        graph: Option<ActiveGraph>,
        placement: PlacementPolicy,
    ) -> Result<PreparedDomainSchedule, String> {
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
        let current = self.inner.consensus.current_schedule().await;
        let schedule = match graph {
            Some(graph) => {
                #[cfg(feature = "testing")]
                let mut schedule = graph.schedule_for_domain_with_mode(
                    domain,
                    &cluster_nodes,
                    self.inner.replica_count,
                    placement,
                    self.inner.runtime.scheduler_mode(),
                );
                #[cfg(not(feature = "testing"))]
                let mut schedule = graph.schedule_for_domain(
                    domain,
                    &cluster_nodes,
                    self.inner.replica_count,
                    placement,
                );
                Self::merge_existing_schedule_data(
                    &mut schedule,
                    current.domain(domain),
                    &live_voters,
                );
                prefer_former_owners_as_replicas(
                    current.domain(domain),
                    &mut schedule,
                    &live_voters,
                );
                Some(schedule)
            }
            None => None,
        };
        let relocations = planned_relocation_count(current.domain(domain), schedule.as_ref());
        Ok(PreparedDomainSchedule {
            schedule,
            relocations,
        })
    }

    pub(in crate::application) async fn drop_node(
        &self,
        node_id: ClusterNodeName,
    ) -> CommandResult {
        let gossip = self.inner.cluster.availability_state().await;
        let is_live = gossip
            .live_nodes
            .iter()
            .any(|live_node| live_node.node_id == node_id);
        if is_live && !gossip.dead_node_ids.contains(&node_id) {
            return command_error(format!(
                "cannot drop live node '{node_id}'; stop the node before removing it"
            ));
        }

        let membership_nodes = self.inner.consensus.membership_nodes().await;
        if membership_nodes.contains_key(&node_id) {
            let voters = self.inner.consensus.membership_voter_ids().await;
            let live_node_ids = gossip
                .live_nodes
                .iter()
                .filter(|live_node| !gossip.dead_node_ids.contains(&live_node.node_id))
                .map(|live_node| live_node.node_id.clone())
                .collect::<BTreeSet<_>>();
            if let Some(message) = Self::drop_node_quorum_error(&node_id, &voters, &live_node_ids) {
                return command_error(message);
            }
        }

        let current_schedule = self.inner.consensus.current_schedule().await;
        match self.inner.consensus_administrator.drop_node(&node_id).await {
            Ok(()) => {}
            Err(error) => {
                return self
                    .consensus_error_response(
                        &error,
                        format!("failed to drop node '{node_id}': {error}"),
                    )
                    .await;
            }
        }

        let live_node_ids = self.inner.cluster.live_node_ids().await;
        let live_voters = self
            .inner
            .consensus
            .live_voter_ids(live_node_ids.clone())
            .await;
        let schedulable_nodes = self
            .inner
            .consensus
            .schedulable_live_voter_ids(live_node_ids)
            .await;
        let (cluster_nodes, preservable_nodes) =
            Self::drop_node_schedule_node_sets(&live_voters, &schedulable_nodes);
        for (domain, graph) in self.inner.registry.active_graphs() {
            let Some(domain_state) = self.inner.consensus.current_domain(&domain).await else {
                continue;
            };
            #[cfg(feature = "testing")]
            let mut schedule = graph.schedule_for_domain_with_mode(
                &domain,
                cluster_nodes,
                self.inner.replica_count,
                domain_state.config.placement,
                self.inner.runtime.scheduler_mode(),
            );
            #[cfg(not(feature = "testing"))]
            let mut schedule = graph.schedule_for_domain(
                &domain,
                cluster_nodes,
                self.inner.replica_count,
                domain_state.config.placement,
            );
            Self::merge_existing_schedule_data(
                &mut schedule,
                current_schedule.domain(&domain),
                preservable_nodes,
            );
            if let Err(error) = self
                .inner
                .consensus
                .replace_domain_schedule(
                    domain.clone(),
                    current_schedule.domain(&domain).cloned(),
                    Some(schedule),
                )
                .await
            {
                return command_error(format!(
                    "dropped node '{node_id}', but failed to republish schedule for domain '{}': \
                     {error}",
                    domain.as_str()
                ));
            }
        }

        command_ok(format!("dropped node '{node_id}'"))
    }

    fn drop_node_schedule_node_sets<'nodes>(
        live_voters: &'nodes [ClusterNodeName],
        schedulable_nodes: &'nodes [ClusterNodeName],
    ) -> (&'nodes [ClusterNodeName], &'nodes [ClusterNodeName]) {
        (schedulable_nodes, live_voters)
    }

    fn drop_node_quorum_error(
        node_id: &ClusterNodeName,
        voters: &BTreeSet<ClusterNodeName>,
        live_node_ids: &BTreeSet<ClusterNodeName>,
    ) -> Option<String> {
        if voters.is_empty() {
            return None;
        }

        let live_voters = voters
            .iter()
            .filter(|voter| live_node_ids.contains(*voter))
            .count();
        let required_voters = Self::raft_quorum_size(voters.len());
        if live_voters >= required_voters {
            return None;
        }

        Some(format!(
            "cannot drop node '{node_id}' because raft quorum is unavailable: {live_voters} live \
             voter(s), {required_voters} required from current voters [{}]. Start enough existing \
             voters or restore the StatefulSet before changing membership",
            voters.iter().cloned().collect::<Vec<_>>().join(",")
        ))
    }

    fn raft_quorum_size(voter_count: usize) -> usize {
        voter_count / 2 + 1
    }

    pub(in crate::application) async fn set_node_cordoned(
        &self,
        node_id: ClusterNodeName,
        cordoned: bool,
    ) -> CommandResult {
        let membership = self.inner.consensus.membership_nodes().await;
        if !membership.contains_key(&node_id) {
            return command_error(format!("node '{node_id}' is not a raft member"));
        }

        if let Err(error) = self
            .inner
            .consensus
            .set_node_cordoned(node_id.clone(), cordoned)
            .await
        {
            let action = if cordoned { "cordon" } else { "uncordon" };
            return self
                .consensus_error_response(
                    &error,
                    format!("failed to {action} node '{node_id}': {error}"),
                )
                .await;
        }

        let action = if cordoned { "cordoned" } else { "uncordoned" };
        command_ok(format!("{action} node '{node_id}'"))
    }

    pub(in crate::application) async fn drain_node(
        &self,
        node_id: ClusterNodeName,
    ) -> CommandResult {
        let membership = self.inner.consensus.membership_nodes().await;
        if !membership.contains_key(&node_id) {
            return command_error(format!("node '{node_id}' is not a raft member"));
        }

        if let Err(error) = self
            .inner
            .consensus
            .set_node_cordoned(node_id.clone(), true)
            .await
        {
            return self
                .consensus_error_response(
                    &error,
                    format!("failed to cordon node '{node_id}' before drain: {error}"),
                )
                .await;
        }

        let initial_schedule = self.inner.consensus.current_schedule().await;
        let total = initial_schedule
            .domains
            .values()
            .flat_map(|schedule| schedule.nodes.values())
            .filter(|node| node.execution_node() == Some(&node_id))
            .count();
        let mut moved = 0usize;
        let mut outcomes = Vec::new();
        let mut failed = false;
        let mut failed_units = BTreeSet::<(DomainName, String)>::new();
        let mut failed_domains = BTreeSet::<DomainName>::new();
        loop {
            let live_node_ids = self.inner.cluster.live_node_ids().await;
            let live_voters = self
                .inner
                .consensus
                .live_voter_ids(live_node_ids.clone())
                .await;
            let replacement_nodes = self
                .inner
                .consensus
                .schedulable_live_voter_ids(live_node_ids)
                .await;
            if replacement_nodes.is_empty() {
                failed = true;
                outcomes.push(format!(
                    "- owner={node_id} failed: no live schedulable raft voters remain"
                ));
                break;
            }
            let live_voter_set = live_voters.iter().cloned().collect::<BTreeSet<_>>();
            let replacement_node_set = replacement_nodes.iter().cloned().collect::<BTreeSet<_>>();
            let mut handled_this_iteration = false;

            for (domain, graph) in self.inner.registry.active_graphs() {
                if failed_domains.contains(&domain) {
                    continue;
                }
                let Some(_alter_guard) = self.inner.runtime.try_begin_domain_alter(&domain) else {
                    failed = true;
                    failed_domains.insert(domain.clone());
                    outcomes.push(format!(
                        "- domain={} owner={node_id} failed: a model or schedule change is \
                         already in progress",
                        domain.as_str()
                    ));
                    continue;
                };
                let Some(domain_state) = self.inner.consensus.current_domain(&domain).await else {
                    continue;
                };
                let current_schedule = self.inner.consensus.current_schedule().await;
                #[cfg(feature = "testing")]
                let desired = graph.schedule_for_domain_with_mode(
                    &domain,
                    &replacement_nodes,
                    self.inner.replica_count,
                    domain_state.config.placement,
                    self.inner.runtime.scheduler_mode(),
                );
                #[cfg(not(feature = "testing"))]
                let desired = graph.schedule_for_domain(
                    &domain,
                    &replacement_nodes,
                    self.inner.replica_count,
                    domain_state.config.placement,
                );
                let Some(current_domain) = current_schedule.domain(&domain) else {
                    if let Err(error) = self
                        .inner
                        .consensus
                        .replace_domain_schedule(domain.clone(), None, Some(desired))
                        .await
                    {
                        if let ConsensusError::LeadershipLost { .. } = &error {
                            return self
                                .consensus_error_response(
                                    &error,
                                    format!(
                                        "failed to publish initial drain schedule for domain \
                                         '{domain}': {error}"
                                    ),
                                )
                                .await;
                        }
                        failed = true;
                        failed_domains.insert(domain.clone());
                        outcomes.push(format!(
                            "- domain={} owner={node_id} failed: could not publish initial \
                             schedule: {error}",
                            domain.as_str()
                        ));
                        handled_this_iteration = true;
                        break;
                    }
                    if let Err(error) = self.apply_current_cluster_state().await {
                        failed = true;
                        failed_domains.insert(domain.clone());
                        outcomes.push(format!(
                            "- domain={} owner={node_id} failed: could not activate initial \
                             schedule: {error}",
                            domain.as_str()
                        ));
                    }
                    handled_this_iteration = true;
                    break;
                };

                let mut next = current_domain.clone();
                let excluded = failed_units
                    .iter()
                    .filter_map(|(failed_domain, label)| {
                        (failed_domain == &domain).then_some(label.clone())
                    })
                    .collect::<BTreeSet<_>>();
                let Some(drain_move) = Self::move_next_scheduled_node_for_drain_excluding(
                    &mut next,
                    &desired,
                    &node_id,
                    &live_voter_set,
                    &replacement_node_set,
                    &excluded,
                ) else {
                    continue;
                };
                let unit_key = (domain.clone(), drain_move.label.clone());
                mark_complete_ownership_transitions(Some(current_domain), &mut next);
                let planned_moves = planned_ownership_moves(Some(current_domain), Some(&next));
                let mut handoff = match self
                    .begin_planned_ownership_handoff(&domain, Some(current_domain), Some(&next))
                    .await
                {
                    Ok(handoff) => handoff,
                    Err(error) => {
                        failed = true;
                        failed_units.insert(unit_key);
                        if planned_moves.is_empty() {
                            outcomes.push(format!(
                                "- {} owner={node_id} failed: {error}",
                                drain_move.label
                            ));
                        } else {
                            outcomes.extend(planned_moves.iter().map(|moved| {
                                format!(
                                    "- kind={} name={} owner={} failed: {error}",
                                    moved.entity.kind.as_ref(),
                                    moved.entity.identifier.as_str(),
                                    moved.former_owner
                                )
                            }));
                        }
                        handled_this_iteration = true;
                        break;
                    }
                };
                if let Err(error) = self
                    .inner
                    .consensus
                    .replace_domain_schedule(
                        domain.clone(),
                        Some(current_domain.clone()),
                        Some(next),
                    )
                    .await
                {
                    if let Some(handoff) = handoff.take() {
                        self.abort_planned_ownership_handoff(&domain, handoff).await;
                    }
                    if let ConsensusError::LeadershipLost { .. } = &error {
                        return self
                            .consensus_error_response(
                                &error,
                                format!(
                                    "failed to commit drain schedule for domain '{domain}': \
                                     {error}"
                                ),
                            )
                            .await;
                    }
                    failed = true;
                    failed_units.insert(unit_key);
                    outcomes.extend(planned_moves.iter().map(|moved| {
                        format!(
                            "- kind={} name={} owner={} failed: schedule commit failed: {error}",
                            moved.entity.kind.as_ref(),
                            moved.entity.identifier.as_str(),
                            moved.former_owner
                        )
                    }));
                    handled_this_iteration = true;
                    break;
                }
                moved = moved
                    .checked_add(planned_moves.len())
                    .assured("the moves counted here are schedule entries held in memory");
                let local_activation_error = self.apply_current_cluster_state().await.err();
                let mut handoff_activation_error = None;
                if let Some(handoff) = handoff {
                    debug_assert_eq!(handoff.moves, planned_moves);
                    if let Some(error) = &local_activation_error {
                        self.defer_planned_ownership_handoff_release(&domain, handoff, error);
                    } else if let Err(error) = self
                        .finish_planned_ownership_handoff(&domain, handoff)
                        .await
                    {
                        handoff_activation_error = Some(error.to_string());
                    }
                }
                let activation_error = match local_activation_error {
                    Some(error) => Some(error.to_string()),
                    None => handoff_activation_error,
                };
                for ownership_move in &planned_moves {
                    if activation_error.is_none() {
                        outcomes.push(format_planned_ownership_move(ownership_move));
                    } else if let Some(error) = &activation_error {
                        failed = true;
                        outcomes.push(format!(
                            "- kind={} name={} owner={} failed: destination '{}' did not \
                             activate: {error}",
                            ownership_move.entity.kind.as_ref(),
                            ownership_move.entity.identifier.as_str(),
                            ownership_move.former_owner,
                            ownership_move.destination
                        ));
                    }
                }
                handled_this_iteration = true;
                break;
            }

            if !handled_this_iteration {
                break;
            }
        }

        let level = if total == 0 {
            QuiesceLevel::Dynamic
        } else {
            QuiesceLevel::EntityPause
        };
        let mut message = format!(
            "drained node '{node_id}' (moved {moved} of {total} scheduled graph node(s))\n{}",
            quiesce_level_message(level)
        );
        if !outcomes.is_empty() {
            message.push('\n');
            message.push_str(&outcomes.join("\n"));
        }
        if failed {
            command_error(message)
        } else {
            command_ok(message)
        }
    }

    pub(in crate::application) async fn drain_local_node_before_shutdown(&self) {
        let local_node_id = self.inner.consensus.local_node_id().clone();
        let live_node_ids = self.inner.cluster.live_node_ids().await;
        let drain_targets = self
            .inner
            .consensus
            .schedulable_live_voter_ids(live_node_ids)
            .await;
        if !drain_targets
            .iter()
            .any(|node_id| *node_id != local_node_id)
        {
            warn!(
                node_id = %local_node_id,
                "skipping graceful shutdown drain: no live schedulable replacement nodes remain"
            );
            return;
        }
        let leader = self.inner.consensus.current_leader().await;
        match leader.as_ref() {
            Some(leader_id) if *leader_id == local_node_id => {
                let result = self.drain_node(local_node_id.clone()).await;
                if result.success {
                    info!(
                        node_id = %local_node_id,
                        message = result.message,
                        "drained local node before graceful shutdown"
                    );
                    self.uncordon_local_node_after_shutdown_drain(&local_node_id)
                        .await;
                } else {
                    warn!(
                        node_id = %local_node_id,
                        message = result.message,
                        "failed to drain local node before graceful shutdown"
                    );
                    self.uncordon_local_node_after_shutdown_drain(&local_node_id)
                        .await;
                }
            }
            Some(leader_id) => {
                let Some(leader_grpc_uri) = self.leader_grpc_uri(leader_id).await else {
                    warn!(
                        node_id = %local_node_id,
                        leader = %leader_id,
                        "failed to drain local node before graceful shutdown: leader grpc uri is \
                         unknown"
                    );
                    return;
                };
                match NervixClient::connect_with_options(
                    &leader_grpc_uri,
                    "default",
                    grpc_client_connect_options(
                        &leader_grpc_uri,
                        self.inner.configured_basic_auth.as_ref(),
                    ),
                )
                .await
                {
                    Ok(client) => {
                        match client.execute(format!("DRAIN NODE {local_node_id};")).await {
                            Ok(outcome) if outcome.success => {
                                info!(
                                    node_id = %local_node_id,
                                    leader = %leader_id,
                                    message = outcome.message,
                                    "drained local node through leader before graceful shutdown"
                                );
                                self.uncordon_local_node_through_leader_after_shutdown_drain(
                                    &client,
                                    &local_node_id,
                                    leader_id,
                                )
                                .await;
                            }
                            Ok(outcome) => {
                                warn!(
                                    node_id = %local_node_id,
                                    leader = %leader_id,
                                    message = outcome.message,
                                    "failed to drain local node through leader before graceful \
                                     shutdown"
                                );
                                self.uncordon_local_node_through_leader_after_shutdown_drain(
                                    &client,
                                    &local_node_id,
                                    leader_id,
                                )
                                .await;
                            }
                            Err(error) => {
                                warn!(
                                    node_id = %local_node_id,
                                    leader = %leader_id,
                                    error = %error,
                                    "failed to drain local node through leader before graceful shutdown"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        warn!(
                            node_id = %local_node_id,
                            leader = %leader_id,
                            leader_grpc_uri,
                            error = %error,
                            "failed to connect to leader for graceful shutdown drain"
                        );
                    }
                }
            }
            None => {
                warn!(
                    node_id = %local_node_id,
                    "failed to drain local node before graceful shutdown: raft leader is unknown"
                );
            }
        }
    }

    async fn uncordon_local_node_after_shutdown_drain(&self, local_node_id: &ClusterNodeName) {
        match self
            .inner
            .consensus
            .set_node_cordoned(local_node_id.clone(), false)
            .await
        {
            Ok(()) => {
                info!(
                    node_id = %local_node_id,
                    "cleared shutdown drain cordon before graceful shutdown"
                );
            }
            Err(error) => {
                warn!(
                    node_id = %local_node_id,
                    error = %error,
                    "failed to clear shutdown drain cordon before graceful shutdown"
                );
            }
        }
    }

    async fn uncordon_local_node_through_leader_after_shutdown_drain(
        &self,
        client: &NervixClient,
        local_node_id: &ClusterNodeName,
        leader_id: &ClusterNodeName,
    ) {
        match client
            .execute(format!("UNCORDON NODE {local_node_id};"))
            .await
        {
            Ok(outcome) if outcome.success => {
                info!(
                    node_id = %local_node_id,
                    leader = %leader_id,
                    message = outcome.message,
                    "cleared shutdown drain cordon through leader"
                );
            }
            Ok(outcome) => {
                warn!(
                    node_id = %local_node_id,
                    leader = %leader_id,
                    message = outcome.message,
                    "failed to clear shutdown drain cordon through leader"
                );
            }
            Err(error) => {
                warn!(
                    node_id = %local_node_id,
                    leader = %leader_id,
                    error = %error,
                    "failed to clear shutdown drain cordon through leader"
                );
            }
        }
    }

    async fn leader_grpc_uri(&self, leader_id: &ClusterNodeName) -> Option<String> {
        self.inner
            .cluster
            .gossip_state()
            .await
            .live_nodes
            .into_iter()
            .find(|node| node.node_id == *leader_id)
            .and_then(|node| grpc_uri_from_advertise_addr(&node.grpc_advertise_addr))
    }

    #[cfg(test)]
    fn move_next_scheduled_node_for_drain(
        schedule: &mut nervix_models::DomainSchedule,
        desired: &nervix_models::DomainSchedule,
        node_id: &ClusterNodeName,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
    ) -> Option<DrainMove> {
        Self::move_next_scheduled_node_for_drain_excluding(
            schedule,
            desired,
            node_id,
            live_nodes,
            target_nodes,
            &BTreeSet::new(),
        )
    }

    fn move_next_scheduled_node_for_drain_excluding(
        schedule: &mut nervix_models::DomainSchedule,
        desired: &nervix_models::DomainSchedule,
        node_id: &ClusterNodeName,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
        // Failed units are keyed by their human-readable label, which is a node id for a single
        // node and a bracketed member list for a placement group, so this stays a string set.
        excluded: &BTreeSet<String>,
    ) -> Option<DrainMove> {
        let mut groups = desired.placement_groups.iter().collect::<Vec<_>>();
        groups.sort_by(|left, right| {
            left.members
                .first()
                .map(|member| (member.kind.as_ref(), &member.identifier))
                .cmp(
                    &right
                        .members
                        .first()
                        .map(|member| (member.kind.as_ref(), &member.identifier)),
                )
                .then_with(|| left.members.len().cmp(&right.members.len()))
        });
        for group in groups {
            let label = format!(
                "placement group [{}]",
                format_placement_runtime_nodes(&group.members)
            );
            if excluded.contains(&label) {
                continue;
            }
            let needs_relocation = group.members.iter().any(|member| {
                schedule
                    .nodes
                    .get(member)
                    .is_some_and(|node| node.execution_node() == Some(node_id))
            });
            if needs_relocation {
                return Self::relocate_placement_group_assignment(
                    schedule,
                    desired,
                    group,
                    node_id,
                    live_nodes,
                    target_nodes,
                    AssignmentRelocation::Planned,
                );
            }
        }

        let grouped_members = desired
            .placement_groups
            .iter()
            .flat_map(|group| group.members.iter().cloned())
            .collect::<HashSet<_>>();
        let mut node_indices = (0..schedule.nodes.len()).collect::<Vec<_>>();
        node_indices.sort_by(|left, right| {
            let left = &schedule.nodes[*left];
            let right = &schedule.nodes[*right];
            left.kind()
                .as_ref()
                .cmp(right.kind().as_ref())
                .then_with(|| left.identifier.cmp(&right.identifier))
        });
        for node_index in node_indices {
            let node = &schedule.nodes[node_index];
            if grouped_members.contains(&node.identity()) {
                continue;
            }
            if node.execution_node() != Some(node_id) {
                continue;
            }
            let label = format!("{} {}", node.kind().as_ref(), node.identifier.as_str());
            if excluded.contains(&label) {
                continue;
            }

            let Some(desired_node) = desired.nodes.get(&node.identity()) else {
                continue;
            };
            if desired_node.assigned_nodes.is_empty()
                || desired_node
                    .assigned_nodes
                    .iter()
                    .any(|assigned| assigned == node_id)
            {
                continue;
            }

            let node = &mut schedule.nodes[node_index];
            return Self::relocate_scheduled_node_assignment(
                node,
                desired_node,
                node_id,
                live_nodes,
                target_nodes,
                AssignmentRelocation::Planned,
            );
        }
        None
    }

    fn relocate_placement_group_assignment(
        schedule: &mut nervix_models::DomainSchedule,
        desired: &nervix_models::DomainSchedule,
        group: &PlacementGroupSchedule,
        unavailable_node_id: &ClusterNodeName,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
        relocation: AssignmentRelocation,
    ) -> Option<DrainMove> {
        let retain_former_replica = relocation.retains_former_replica();
        let current_nodes = group
            .members
            .iter()
            .map(|member| schedule.nodes.get(member).cloned())
            .collect::<Option<Vec<_>>>()?;
        let desired_nodes = group
            .members
            .iter()
            .map(|member| desired.nodes.get(member).cloned())
            .collect::<Option<Vec<_>>>()?;
        let old_primary = if let Some(candidate) = schedule
            .placement_groups
            .iter()
            .find(|candidate| placement_group_members_equal(&candidate.members, &group.members))
            && let Some(primary) = candidate.primary_node.as_ref()
        {
            Some(primary.clone())
        } else if let Some(node) = current_nodes.first() {
            node.primary_node.clone()
        } else {
            None
        };
        let preserved_primary = old_primary.as_ref().filter(|primary| {
            *primary != unavailable_node_id
                && live_nodes.contains(*primary)
                && current_nodes.iter().all(|node| {
                    node.primary_node.as_ref() == Some(*primary)
                        && node.assigned_nodes.contains(*primary)
                })
        });
        let desired_target = if let Some(node_id) = group.primary_node.as_ref()
            && target_nodes.contains(node_id)
        {
            Some(node_id.clone())
        } else if let Some(node) = desired_nodes.first()
            && let Some(node_id) = node.primary_node.as_ref()
            && target_nodes.contains(node_id)
        {
            Some(node_id.clone())
        } else if let Some(node) = desired_nodes.first() {
            node.assigned_nodes
                .iter()
                .find(|node_id| target_nodes.contains(*node_id))
                .cloned()
        } else {
            None
        };
        let mut common_replicas = current_nodes
            .first()?
            .assigned_nodes
            .iter()
            .filter(|node_id| *node_id != unavailable_node_id)
            .filter(|node_id| target_nodes.contains(*node_id))
            .cloned()
            .collect::<Vec<_>>();
        common_replicas.retain(|candidate| {
            current_nodes
                .iter()
                .all(|node| node.assigned_nodes.contains(candidate))
        });
        let target = if let Some(primary) = preserved_primary {
            primary.clone()
        } else if let Some(target) =
            relocation.target(desired_target, common_replicas.first().cloned())
        {
            target
        } else {
            target_nodes.first()?.clone()
        };
        let primary_changed = old_primary.as_ref() != Some(&target);
        let promoted_replica = (primary_changed
            && current_nodes
                .iter()
                .all(|node| node.assigned_nodes.contains(&target)))
        .then_some(target.clone());

        for ((member, current_node), desired_node) in
            group.members.iter().zip(current_nodes).zip(desired_nodes)
        {
            let replica_slots = current_node
                .assigned_nodes
                .len()
                .max(desired_node.assigned_nodes.len())
                .max(1);
            let mut assigned_nodes = vec![target.clone()];
            if retain_former_replica
                && live_nodes.contains(unavailable_node_id)
                && unavailable_node_id != &target
            {
                assigned_nodes.push(unavailable_node_id.clone());
            }
            for assigned in desired_node
                .assigned_nodes
                .iter()
                .chain(&current_node.assigned_nodes)
                .chain(target_nodes)
            {
                if (target_nodes.contains(assigned)
                    || retain_former_replica
                        && assigned == unavailable_node_id
                        && live_nodes.contains(assigned))
                    && !assigned_nodes.contains(assigned)
                {
                    assigned_nodes.push(assigned.clone());
                }
            }
            assigned_nodes.truncate(replica_slots);
            let node = schedule.nodes.get_mut(member).verified(
                "the early return above required every group member to resolve in this same \
                 schedule",
            );
            if primary_changed && let Some(source) = old_primary.as_ref() {
                node.ownership_transition = Some(relocation.ownership_transition(
                    source.clone(),
                    target.clone(),
                    node,
                    promoted_replica.is_some(),
                ));
            }
            node.primary_node = Some(target.clone());
            node.assigned_nodes = assigned_nodes;
        }
        if let Some(current_group) = schedule
            .placement_groups
            .iter_mut()
            .find(|candidate| placement_group_members_equal(&candidate.members, &group.members))
        {
            current_group.primary_node = Some(target.clone());
        }
        Some(DrainMove {
            label: format!(
                "placement group [{}]",
                format_placement_runtime_nodes(&group.members)
            ),
            promoted_replica: promoted_replica.clone(),
            fallback_node: (primary_changed && promoted_replica.is_none()).then_some(target),
        })
    }

    fn relocate_scheduled_node_assignment(
        node: &mut ScheduledNode,
        desired_node: &ScheduledNode,
        unavailable_node_id: &ClusterNodeName,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
        relocation: AssignmentRelocation,
    ) -> Option<DrainMove> {
        let retain_former_replica = relocation.retains_former_replica();
        if !node.is_assigned_to(unavailable_node_id) {
            return None;
        }

        let label = format!("{} {}", node.kind().as_ref(), node.identifier.as_str());
        let old_primary = node.primary_node.clone();
        let preserved_primary = old_primary
            .as_ref()
            .filter(|primary| *primary != unavailable_node_id && live_nodes.contains(*primary))
            .cloned();
        let desired_target = if let Some(node_id) = desired_node.primary_node.as_ref()
            && target_nodes.contains(node_id)
        {
            Some(node_id.clone())
        } else {
            desired_node
                .assigned_nodes
                .iter()
                .find(|node_id| target_nodes.contains(*node_id))
                .cloned()
        };
        let existing_replica = node
            .assigned_nodes
            .iter()
            .filter(|assigned| *assigned != unavailable_node_id)
            .find(|assigned| target_nodes.contains(*assigned))
            .cloned();
        let target = if let Some(primary) = preserved_primary {
            primary
        } else if let Some(target) = relocation.target(desired_target, existing_replica) {
            target
        } else {
            target_nodes.first()?.clone()
        };
        let replica_slots = node
            .assigned_nodes
            .len()
            .max(desired_node.assigned_nodes.len())
            .max(1);
        let mut assigned_nodes = vec![target.clone()];
        if retain_former_replica
            && live_nodes.contains(unavailable_node_id)
            && unavailable_node_id != &target
        {
            assigned_nodes.push(unavailable_node_id.clone());
        }
        for assigned in desired_node
            .assigned_nodes
            .iter()
            .chain(&node.assigned_nodes)
            .chain(target_nodes)
        {
            if (target_nodes.contains(assigned)
                || retain_former_replica
                    && assigned == unavailable_node_id
                    && live_nodes.contains(assigned))
                && !assigned_nodes.contains(assigned)
            {
                assigned_nodes.push(assigned.clone());
            }
        }
        assigned_nodes.truncate(replica_slots);
        let primary_changed = old_primary.as_ref() != Some(&target);
        let promoted_replica =
            (primary_changed && node.assigned_nodes.contains(&target)).then_some(target.clone());
        if primary_changed && let Some(source) = old_primary {
            node.ownership_transition = Some(relocation.ownership_transition(
                source,
                target.clone(),
                node,
                promoted_replica.is_some(),
            ));
        }
        node.primary_node = Some(target.clone());
        node.assigned_nodes = assigned_nodes;
        Some(DrainMove {
            label,
            promoted_replica: promoted_replica.clone(),
            fallback_node: (primary_changed && promoted_replica.is_none()).then_some(target),
        })
    }

    pub(in crate::application) fn failover_unavailable_scheduled_nodes(
        schedule: &mut nervix_models::DomainSchedule,
        desired: Option<&nervix_models::DomainSchedule>,
        live_nodes: &BTreeSet<ClusterNodeName>,
        target_nodes: &BTreeSet<ClusterNodeName>,
    ) -> Vec<DrainMove> {
        let mut moves = Vec::new();
        if target_nodes.is_empty() {
            return moves;
        }

        let generated_desired = desired.is_none().then(|| {
            let mut generated = schedule.clone();
            for node in generated.nodes.values_mut() {
                let replica_slots = node.assigned_nodes.len().max(1);
                node.assigned_nodes = target_nodes.iter().take(replica_slots).cloned().collect();
                node.primary_node = node.assigned_nodes.first().cloned();
            }
            for group in &mut generated.placement_groups {
                group.primary_node = target_nodes.first().cloned();
            }
            generated
        });
        let desired = desired
            .or(generated_desired.as_ref())
            .verified("the generated schedule is built exactly when no desired schedule was given");

        let groups = schedule.placement_groups.clone();
        let mut grouped_members = HashSet::default();
        for group in groups {
            grouped_members.extend(group.members.iter().cloned());
            let current_nodes = group
                .members
                .iter()
                .map(|member| schedule.nodes.get(member).cloned())
                .collect::<Option<Vec<_>>>();
            let Some(current_nodes) = current_nodes else {
                continue;
            };
            let has_unavailable_assignment = current_nodes.iter().any(|node| {
                node.assigned_nodes
                    .iter()
                    .any(|node_id| !live_nodes.contains(node_id))
            });
            if !has_unavailable_assignment {
                continue;
            }
            let unavailable_node_id = if let Some(node_id) = group.primary_node.as_ref()
                && !live_nodes.contains(node_id)
            {
                Some(node_id.clone())
            } else {
                current_nodes
                    .iter()
                    .flat_map(|node| node.assigned_nodes.iter())
                    .find(|node_id| !live_nodes.contains(*node_id))
                    .cloned()
            };
            let Some(unavailable_node_id) = unavailable_node_id else {
                continue;
            };
            let desired_group = desired
                .placement_groups
                .iter()
                .find(|candidate| placement_group_members_equal(&candidate.members, &group.members))
                .unwrap_or(&group);
            if let Some(failover_move) = Self::relocate_placement_group_assignment(
                schedule,
                desired,
                desired_group,
                &unavailable_node_id,
                live_nodes,
                target_nodes,
                AssignmentRelocation::Failure,
            ) {
                moves.push(failover_move);
            }
        }

        for node in schedule.nodes.values_mut() {
            if grouped_members.contains(&node.identity()) {
                continue;
            }
            if node.assigned_nodes.is_empty() {
                continue;
            }
            let unavailable_node_ids = node
                .assigned_nodes
                .iter()
                .filter(|node_id| !live_nodes.contains(*node_id))
                .cloned()
                .collect::<Vec<_>>();
            if unavailable_node_ids.is_empty() {
                continue;
            }

            let Some(desired_node) = desired.nodes.get(&node.identity()) else {
                continue;
            };

            for unavailable_node_id in unavailable_node_ids {
                if let Some(failover_move) = Self::relocate_scheduled_node_assignment(
                    node,
                    desired_node,
                    &unavailable_node_id,
                    live_nodes,
                    target_nodes,
                    AssignmentRelocation::Failure,
                ) {
                    moves.push(failover_move);
                }
            }
        }

        moves
    }

    pub(in crate::application) fn merge_existing_schedule_data(
        schedule: &mut nervix_models::DomainSchedule,
        existing: Option<&nervix_models::DomainSchedule>,
        live_node_ids: &[ClusterNodeName],
    ) {
        let Some(existing) = existing else {
            return;
        };
        let live_node_ids = live_node_ids.iter().cloned().collect::<BTreeSet<_>>();

        for node in schedule.nodes.values_mut() {
            if let Some(existing_node) = existing.nodes.get(&node.identity()) {
                node.kafka_partition_schedule = existing_node.kafka_partition_schedule.clone();
            }
        }

        let mut grouped_members = HashSet::default();
        for group_index in 0..schedule.placement_groups.len() {
            let members = schedule.placement_groups[group_index].members.clone();
            grouped_members.extend(members.iter().cloned());
            let existing_nodes = members
                .iter()
                .map(|member| existing.nodes.get(member))
                .collect::<Option<Vec<_>>>();
            let common_primary = if let Some(nodes) = existing_nodes.as_ref()
                && let Some(first) = nodes.first()
                && let Some(primary) = first.primary_node.as_ref()
                && live_node_ids.contains(primary)
                && nodes.iter().all(|node| {
                    node.primary_node.as_ref() == Some(primary)
                        && node.assigned_nodes.contains(primary)
                }) {
                Some(primary.clone())
            } else {
                None
            };

            if let (Some(existing_nodes), Some(primary)) = (existing_nodes, common_primary) {
                for (member, existing_node) in members.iter().zip(existing_nodes) {
                    let Some(node) = schedule.nodes.get_mut(member) else {
                        continue;
                    };
                    let desired_assigned_nodes = node.assigned_nodes.clone();
                    let replica_slots = existing_node
                        .assigned_nodes
                        .len()
                        .max(desired_assigned_nodes.len())
                        .max(1);
                    let mut assigned_nodes = vec![primary.clone()];
                    for assigned in existing_node
                        .assigned_nodes
                        .iter()
                        .chain(&desired_assigned_nodes)
                    {
                        if live_node_ids.contains(assigned) && !assigned_nodes.contains(assigned) {
                            assigned_nodes.push(assigned.clone());
                        }
                    }
                    assigned_nodes.truncate(replica_slots);
                    node.primary_node = Some(primary.clone());
                    node.assigned_nodes = assigned_nodes;
                }
            }
            schedule.placement_groups[group_index].primary_node = if let Some(member) =
                members.first()
                && let Some(node) = schedule.nodes.get(member)
            {
                node.primary_node.clone()
            } else {
                None
            };
        }

        for node in schedule.nodes.values_mut() {
            if grouped_members.contains(&node.identity()) {
                continue;
            }
            let Some(existing_node) = existing.nodes.get(&node.identity()) else {
                continue;
            };
            if Self::scheduled_node_should_follow_desired_assignment(node) {
                continue;
            }
            if existing_node.assigned_nodes.is_empty() {
                continue;
            }
            if existing_node
                .assigned_nodes
                .iter()
                .all(|node_id| live_node_ids.contains(node_id))
            {
                node.primary_node = existing_node.primary_node.clone();
                node.assigned_nodes = existing_node.assigned_nodes.clone();
                continue;
            }

            let desired_node = node.clone();
            let target_nodes = desired_node
                .assigned_nodes
                .iter()
                .filter(|node_id| live_node_ids.contains(*node_id))
                .cloned()
                .collect::<BTreeSet<_>>();
            let desired_primary_node = node.primary_node.clone();
            let desired_assigned_nodes = node.assigned_nodes.clone();
            node.primary_node = existing_node.primary_node.clone();
            node.assigned_nodes = existing_node.assigned_nodes.clone();
            let unavailable_nodes = existing_node
                .assigned_nodes
                .iter()
                .filter(|node_id| !live_node_ids.contains(*node_id))
                .cloned()
                .collect::<Vec<_>>();
            let mut relocated = false;
            for unavailable_node_id in unavailable_nodes {
                if Self::relocate_scheduled_node_assignment(
                    node,
                    &desired_node,
                    &unavailable_node_id,
                    &live_node_ids,
                    &target_nodes,
                    AssignmentRelocation::Failure,
                )
                .is_some()
                {
                    relocated = true;
                }
            }
            if !relocated {
                node.primary_node = desired_primary_node;
                node.assigned_nodes = desired_assigned_nodes;
            }
        }

        for node in schedule.nodes.values_mut() {
            let Some(existing_node) = existing.nodes.get(&node.identity()) else {
                continue;
            };
            if node.primary_node == existing_node.primary_node {
                node.ownership_transition = existing_node.ownership_transition.clone();
            }
        }
    }

    fn scheduled_node_should_follow_desired_assignment(node: &ScheduledNode) -> bool {
        node.config.executes_on_every_cluster_node()
    }

    fn kafka_partition_watcher_specs(
        &self,
        schedule: &nervix_models::ClusterSchedule,
    ) -> Vec<KafkaPartitionWatcherSpec> {
        let mut specs = Vec::new();
        for domain_schedule in schedule.domains.values() {
            for node in domain_schedule.nodes.values() {
                let Model::Ingestor(ingestor) = node.config.as_ref() else {
                    continue;
                };
                let IngestSource::Kafka {
                    client,
                    topic,
                    offset_mode: KafkaOffsetMode::Domain,
                    instances,
                    ..
                } = &ingestor.source
                else {
                    continue;
                };
                let Some(client_node) = domain_schedule
                    .nodes
                    .get(&NodeRef::new(ModelKind::Client, ModelName::from(client)))
                else {
                    continue;
                };
                let Model::ClientKafka(client_model) = client_node.config.as_ref() else {
                    continue;
                };
                specs.push(KafkaPartitionWatcherSpec {
                    domain: domain_schedule.domain.clone(),
                    ingestor: ingestor.name.clone(),
                    topic: topic.as_str().to_string(),
                    instances: *instances,
                    client: client_model.clone(),
                });
            }
        }
        specs.sort_by(|left, right| {
            left.domain
                .cmp(&right.domain)
                .then_with(|| left.ingestor.cmp(&right.ingestor))
        });
        specs
    }

    async fn publish_kafka_partition_schedule(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        topic: &str,
        instances: NonZeroU64,
        observed_partitions: Vec<i32>,
    ) -> Result<(), String> {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            return Ok(());
        }

        let current = self.inner.consensus.current_schedule().await;
        let Some(existing_domain_schedule) = current.domain(domain) else {
            return Ok(());
        };
        let mut next_domain_schedule = existing_domain_schedule.clone();
        let Some(ingestor_node) = next_domain_schedule.nodes.get_mut(&NodeRef::new(
            ModelKind::Ingestor,
            ModelName::from(ingestor),
        )) else {
            return Ok(());
        };
        let Model::Ingestor(ingestor_model) = ingestor_node.config.as_ref() else {
            return Ok(());
        };
        let IngestSource::Kafka {
            topic: scheduled_topic,
            offset_mode: KafkaOffsetMode::Domain,
            instances: scheduled_instances,
            ..
        } = &ingestor_model.source
        else {
            return Ok(());
        };
        if scheduled_topic.as_str() != topic || *scheduled_instances != instances {
            return Ok(());
        }

        let mut next_schedule =
            KafkaPartitionSchedule::new(*scheduled_instances, observed_partitions, 0);
        if let Some(existing_schedule) = ingestor_node.kafka_partition_schedule.as_ref() {
            if existing_schedule.observed_partitions == next_schedule.observed_partitions
                && existing_schedule.instance_assignments == next_schedule.instance_assignments
            {
                return Ok(());
            }
            next_schedule.rebalance_epoch = existing_schedule
                .rebalance_epoch
                .checked_add(1)
                .assured("a cluster cannot observe 2^64 partition rebalances");
        }
        ingestor_node.kafka_partition_schedule = Some(next_schedule);
        self.inner
            .consensus
            .replace_domain_schedule(
                domain.clone(),
                Some(existing_domain_schedule.clone()),
                Some(next_domain_schedule),
            )
            .await
            .map_err(|error| error.to_string())
    }

    pub(in crate::application) async fn reconcile_kafka_partition_watchers(
        &self,
        schedule: &nervix_models::ClusterSchedule,
        tasks: &mut HashMap<KafkaPartitionWatcherKey, KafkaPartitionWatcherTask>,
    ) {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            for (_, watcher) in tasks.drain() {
                watcher.task.stop().await;
            }
            return;
        }

        let desired = self
            .kafka_partition_watcher_specs(schedule)
            .into_iter()
            .map(|spec| {
                (
                    KafkaPartitionWatcherKey {
                        domain: spec.domain.clone(),
                        ingestor: spec.ingestor.clone(),
                    },
                    spec,
                )
            })
            .collect::<HashMap<_, _>>();

        let mut stale_keys = Vec::new();
        for (key, watcher) in tasks.iter() {
            if desired.get(key) != Some(&watcher.spec) {
                stale_keys.push(key.clone());
            }
        }
        for key in stale_keys {
            if let Some(watcher) = tasks.remove(&key) {
                watcher.task.stop().await;
            }
        }

        for (key, spec) in desired {
            if tasks.get(&key).is_some_and(|watcher| watcher.spec == spec) {
                continue;
            }
            let cancel = CancellationToken::new();
            let cancel_child = cancel.clone();
            let service = self.clone();
            let spec_for_task = spec.clone();
            let handle = tokio::spawn(async move {
                let resolved = match service.inner.runtime.resolve_client_config(
                    &spec_for_task.domain,
                    spec_for_task.client.mount.as_ref(),
                    &spec_for_task.client.config,
                ) {
                    Ok(resolved) => resolved,
                    Err(error) => {
                        service.broadcast_error(format!(
                            "failed to resolve Kafka partition watcher client config for ingestor \
                             '{}' in domain '{}': {}",
                            spec_for_task.ingestor.as_str(),
                            spec_for_task.domain.as_str(),
                            error
                        ));
                        return;
                    }
                };
                let _mounts = resolved.mounts;
                let mut client_config = ClientConfig::new();
                for entry in &resolved.entries {
                    client_config.set(&entry.key, &entry.value);
                }
                client_config.set(
                    "group.id",
                    format!(
                        "nervix_schedule_watch_{}_{}",
                        spec_for_task.domain.as_str(),
                        spec_for_task.ingestor.as_str()
                    ),
                );
                client_config.set("enable.partition.eof", "false");
                client_config.set("enable.auto.commit", "false");
                let consumer: StreamConsumer = match client_config.create() {
                    Ok(consumer) => consumer,
                    Err(error) => {
                        service.broadcast_error(format!(
                            "failed to create Kafka partition watcher for ingestor '{}' in domain \
                             '{}': {}",
                            spec_for_task.ingestor.as_str(),
                            spec_for_task.domain.as_str(),
                            error
                        ));
                        return;
                    }
                };
                let mut last_observed = None::<Vec<i32>>;
                loop {
                    tokio::task::consume_budget().await;
                    let mut partitions = match KafkaIngestor::topic_partitions(
                        &consumer,
                        spec_for_task.topic.as_str(),
                    ) {
                        Ok(partitions) => partitions,
                        Err(error) => {
                            service.broadcast_error(format!(
                                "failed to inspect Kafka partitions for ingestor '{}' in domain \
                                 '{}': {}",
                                spec_for_task.ingestor.as_str(),
                                spec_for_task.domain.as_str(),
                                error
                            ));
                            tokio::select! {
                                _ = service.inner.shutdown.cancelled() => break,
                                _ = cancel_child.cancelled() => break,
                                _ = sleep(LEADER_KAFKA_PARTITION_WATCH_INTERVAL) => continue,
                            }
                        }
                    };
                    partitions.sort_unstable();
                    if last_observed.as_ref() != Some(&partitions) {
                        if let Err(error) = service
                            .publish_kafka_partition_schedule(
                                &spec_for_task.domain,
                                &spec_for_task.ingestor,
                                spec_for_task.topic.as_str(),
                                spec_for_task.instances,
                                partitions.clone(),
                            )
                            .await
                        {
                            service.broadcast_error(format!(
                                "failed to publish Kafka partition schedule for ingestor '{}' in \
                                 domain '{}': {}",
                                spec_for_task.ingestor.as_str(),
                                spec_for_task.domain.as_str(),
                                error
                            ));
                        } else {
                            last_observed = Some(partitions);
                        }
                    }
                    tokio::select! {
                        _ = service.inner.shutdown.cancelled() => break,
                        _ = cancel_child.cancelled() => break,
                        _ = sleep(LEADER_KAFKA_PARTITION_WATCH_INTERVAL) => {}
                    }
                }
            });
            tasks.insert(
                key,
                KafkaPartitionWatcherTask {
                    spec,
                    task: BackgroundTask { cancel, handle },
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nervix_models::{
        ClusterNodeName, DomainName, DomainSchedule, KafkaPartitionSchedule, ModelKind,
    };
    use nonzero_ext::nonzero;

    use super::super::{
        ownership_handoff::{DrainMove, prefer_former_owners_as_replicas},
        session_service::SessionServiceImpl,
        test_fixtures::{
            named, node_named, placement_group, placement_member, scheduled_node, scheduled_node_on,
        },
    };

    #[test]
    fn drop_node_quorum_error_allows_available_current_quorum() {
        let voters = BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ]);
        let live_node_ids = BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]);

        assert!(
            SessionServiceImpl::drop_node_quorum_error(
                &ClusterNodeName::parse("node-2").expect("valid name"),
                &voters,
                &live_node_ids
            )
            .is_none()
        );
    }

    #[test]
    fn drop_node_rebuild_uses_only_schedulable_nodes_for_new_assignments() {
        let live_voters = vec![
            named::<ClusterNodeName>("node-live"),
            named::<ClusterNodeName>("node-cordoned"),
        ];
        let schedulable_nodes = vec![named::<ClusterNodeName>("node-live")];

        let (new_assignment_candidates, preservable_nodes) =
            SessionServiceImpl::drop_node_schedule_node_sets(&live_voters, &schedulable_nodes);

        assert_eq!(new_assignment_candidates, schedulable_nodes);
        assert_eq!(preservable_nodes, live_voters);
    }

    #[test]
    fn move_next_scheduled_node_for_drain_moves_only_one_node_in_canonical_order() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("ingest_notifications", ModelKind::Ingestor, "node-2"),
                scheduled_node_on("emit_notifications", ModelKind::Emitter, "node-2"),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("ingest_notifications", ModelKind::Ingestor, "node-1"),
                scheduled_node_on("emit_notifications", ModelKind::Emitter, "node-3"),
            ],
            Vec::new(),
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "emitter emit_notifications".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-3").expect("valid name")),
            })
        );
        assert_eq!(
            schedule.nodes[0].assigned_nodes,
            vec![named::<ClusterNodeName>("node-2")]
        );
        assert_eq!(
            schedule.nodes[1].assigned_nodes,
            vec![named::<ClusterNodeName>("node-3")]
        );
    }

    #[test]
    fn planned_drain_uses_canonical_runtime_node_order() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("zeta", ModelKind::Junction, "node-2"),
                scheduled_node_on("alpha", ModelKind::Junction, "node-2"),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("zeta", ModelKind::Junction, "node-1"),
                scheduled_node_on("alpha", ModelKind::Junction, "node-1"),
            ],
            Vec::new(),
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
            ]),
            &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "junction alpha".to_string(),
                promoted_replica: None,
                fallback_node: Some(named::<ClusterNodeName>("node-1")),
            })
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-2"))
        );
        assert_eq!(
            schedule.nodes[1].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn planned_drain_uses_the_first_member_to_order_placement_groups() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let alpha_members = vec![placement_member("alpha", ModelKind::Junction)];
        let zeta_members = vec![placement_member("zeta", ModelKind::Junction)];
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("zeta", ModelKind::Junction, "node-2"),
                scheduled_node_on("alpha", ModelKind::Junction, "node-2"),
            ],
            vec![
                placement_group(
                    zeta_members.clone(),
                    &ClusterNodeName::parse("node-2").expect("valid name"),
                ),
                placement_group(
                    alpha_members.clone(),
                    &ClusterNodeName::parse("node-2").expect("valid name"),
                ),
            ],
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("zeta", ModelKind::Junction, "node-1"),
                scheduled_node_on("alpha", ModelKind::Junction, "node-1"),
            ],
            vec![
                placement_group(
                    zeta_members,
                    &ClusterNodeName::parse("node-1").expect("valid name"),
                ),
                placement_group(
                    alpha_members,
                    &ClusterNodeName::parse("node-1").expect("valid name"),
                ),
            ],
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
            ]),
            &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "placement group [alpha]".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
            })
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-2"))
        );
        assert_eq!(
            schedule.nodes[1].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn planned_drain_prefers_policy_target_and_retains_former_owner_as_first_replica() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![
                        node_named("node-2"),
                        node_named("node-3"),
                        node_named("node-4"),
                    ],
                ),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "deduplicator dedup_notifications".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
            })
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            schedule.nodes[0].assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]
        );
    }

    #[test]
    fn drain_require_group_prefers_policy_target_over_common_replica() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("corridor_source", ModelKind::Junction).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
                scheduled_node("corridor_sink", ModelKind::Junction).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            vec![placement_group(
                members.clone(),
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("corridor_source", ModelKind::Junction).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
                scheduled_node("corridor_sink", ModelKind::Junction).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-1").expect("valid name"),
            )],
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "placement group [corridor_source, corridor_sink]".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
            })
        );
        assert!(
            schedule
                .nodes
                .values()
                .all(|node| node.primary_node.as_ref()
                    == Some(&ClusterNodeName::parse("node-1").expect("valid name")))
        );
        assert_eq!(
            schedule.placement_groups[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert!(schedule.nodes.values().all(|node| {
            node.assigned_nodes
                == vec![
                    named::<ClusterNodeName>("node-1"),
                    named::<ClusterNodeName>("node-2"),
                ]
        }));
    }

    #[test]
    fn planned_drain_replaces_an_unavailable_replica_with_the_former_owner() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![scheduled_node_on(
                "dedup_notifications",
                ModelKind::Deduplicator,
                "node-1",
            )],
            Vec::new(),
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
            ]),
            &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
        );

        assert_eq!(
            moved,
            Some(DrainMove {
                label: "deduplicator dedup_notifications".to_string(),
                promoted_replica: None,
                fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
            })
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            schedule.nodes[0].assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2")
            ]
        );
    }

    #[test]
    fn planned_schedule_move_prefers_the_former_owner_for_the_first_replica_slot() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let current = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );
        let mut planned = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        prefer_former_owners_as_replicas(
            Some(&current),
            &mut planned,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
        );

        assert_eq!(
            planned.nodes[0].assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2")
            ]
        );
    }

    #[test]
    fn merge_existing_schedule_data_prefers_policy_target_when_primary_dies() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-4")],
                ),
            ],
            Vec::new(),
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![
                        node_named("node-2"),
                        node_named("node-3"),
                        node_named("node-4"),
                    ],
                ),
            ],
            Vec::new(),
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
        );

        assert_eq!(
            next.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            next.nodes[0].assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );
    }

    #[test]
    fn merge_existing_schedule_data_falls_back_to_fresh_assignment_without_live_replica() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![scheduled_node_on(
                "dedup_notifications",
                ModelKind::Deduplicator,
                "node-1",
            )],
            Vec::new(),
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[ClusterNodeName::parse("node-1").expect("valid name")],
        );

        assert_eq!(
            next.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            next.nodes[0].assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );
    }

    #[test]
    fn merge_existing_schedule_data_preserves_matching_ingestor_schedule_and_assignment() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let preserved_schedule = KafkaPartitionSchedule::new(nonzero!(2u64), vec![0, 1], 7);
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("ingest_notifications", ModelKind::Ingestor).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("ingest_notifications", ModelKind::Ingestor, "node-1")
                    .with_kafka_partitions(preserved_schedule.clone()),
            ],
            Vec::new(),
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
        );

        assert_eq!(
            next.nodes[0].kafka_partition_schedule,
            Some(preserved_schedule)
        );
        assert_eq!(
            next.nodes[0].assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );
    }

    #[test]
    fn merge_existing_schedule_data_ignores_non_matching_nodes() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("ingest_notifications", ModelKind::Ingestor),
                scheduled_node("kafka_main", ModelKind::Client),
            ],
            Vec::new(),
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("other_ingestor", ModelKind::Ingestor).with_kafka_partitions(
                    KafkaPartitionSchedule::new(nonzero!(2u64), vec![0, 1], 3),
                ),
                scheduled_node("ingest_notifications", ModelKind::Client)
                    .with_kafka_partitions(KafkaPartitionSchedule::new(nonzero!(1u64), vec![0], 2)),
            ],
            Vec::new(),
        );

        SessionServiceImpl::merge_existing_schedule_data(&mut next, Some(&existing), &[]);

        assert_eq!(next.nodes[0].kafka_partition_schedule, None);
        assert_eq!(next.nodes[1].kafka_partition_schedule, None);
    }

    #[test]
    fn merge_existing_schedule_data_rejects_a_split_require_group() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-1"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-1"),
            ],
            vec![placement_group(
                members.clone(),
                &ClusterNodeName::parse("node-1").expect("valid name"),
            )],
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-2"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-3"),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
        );

        assert!(next.nodes.values().all(|node| node.primary_node.as_ref()
            == Some(&ClusterNodeName::parse("node-1").expect("valid name"))));
        assert_eq!(
            next.placement_groups[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn merge_existing_schedule_data_preserves_an_intact_require_group() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut next = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-1"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-1"),
            ],
            vec![placement_group(
                members.clone(),
                &ClusterNodeName::parse("node-1").expect("valid name"),
            )],
        );
        let existing = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-2"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-2"),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );

        SessionServiceImpl::merge_existing_schedule_data(
            &mut next,
            Some(&existing),
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
            ],
        );

        assert!(next.nodes.values().all(|node| node.primary_node.as_ref()
            == Some(&ClusterNodeName::parse("node-2").expect("valid name"))));
        assert_eq!(
            next.placement_groups[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-2"))
        );
    }

    #[test]
    fn drain_relocates_a_require_group_as_one_unit() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-2"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-2"),
            ],
            vec![placement_group(
                members.clone(),
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node_on("corridor_source", ModelKind::Junction, "node-1"),
                scheduled_node_on("corridor_sink", ModelKind::Junction, "node-1"),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-1").expect("valid name"),
            )],
        );

        let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
            &mut schedule,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert!(moved.is_some());
        assert!(
            schedule
                .nodes
                .values()
                .all(|node| node.primary_node.as_ref()
                    == Some(&ClusterNodeName::parse("node-1").expect("valid name")))
        );
        assert_eq!(
            schedule.placement_groups[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn failover_relocates_a_require_group_to_one_target() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let members = vec![
            placement_member("corridor_source", ModelKind::Junction),
            placement_member("corridor_sink", ModelKind::Junction),
        ];
        let mut schedule = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("corridor_source", ModelKind::Junction).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
                scheduled_node("corridor_sink", ModelKind::Junction).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-1")],
                ),
            ],
            vec![placement_group(
                members,
                &ClusterNodeName::parse("node-2").expect("valid name"),
            )],
        );

        let moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
            &mut schedule,
            None,
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert!(!moves.is_empty());
        let group_host = schedule.placement_groups[0]
            .primary_node
            .as_ref()
            .expect("require group must retain a host");
        assert!(
            schedule
                .nodes
                .values()
                .all(|node| node.primary_node.as_ref() == Some(group_host))
        );
    }

    #[test]
    fn failover_does_not_promote_a_cordoned_live_replica() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        let moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
            &mut schedule,
            None,
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
        );

        assert!(!moves.is_empty());
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
    }

    #[test]
    fn failover_promotes_live_replica_before_policy_target() {
        let domain = DomainName::parse("payments").expect("valid domain");
        let mut schedule = DomainSchedule::new(
            domain.clone(),
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-2")),
                    vec![node_named("node-2"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );
        let desired = DomainSchedule::new(
            domain,
            vec![
                scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                    Some(node_named("node-1")),
                    vec![node_named("node-1"), node_named("node-3")],
                ),
            ],
            Vec::new(),
        );

        let moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
            &mut schedule,
            Some(&desired),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
            &BTreeSet::from([
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3"),
            ]),
        );

        assert_eq!(
            moves,
            vec![DrainMove {
                label: "deduplicator dedup_notifications".to_string(),
                promoted_replica: Some(ClusterNodeName::parse("node-3").expect("valid name")),
                fallback_node: None,
            }]
        );
        assert_eq!(
            schedule.nodes[0].primary_node.as_ref(),
            Some(&named::<ClusterNodeName>("node-3"))
        );
        assert_eq!(
            schedule.nodes[0].assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-3"),
                named::<ClusterNodeName>("node-1")
            ]
        );
    }
}
