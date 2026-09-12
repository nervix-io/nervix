//! Moving a runtime entity's owned state from one node to another without losing it.
//!
//! Layer: control plane.
//!
//! - **Owns.** The planned handoff, the incarnations it is valid for, the capture, prepare,
//!   confirm, activate and finish steps, and the forced recovery when a source node is gone.
//! - **Depends on.** The interconnect to drive each step and the entity gate to hold the entity.
//! - **Must not know.** Which schedule change asked for the move.

use std::collections::{BTreeMap, BTreeSet};

use error_stack::Report;
use futures_util::{StreamExt, stream::FuturesUnordered};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::{
    ActivateOwnershipHandoffStateRequest as RemoteActivateOwnershipHandoffStateRequest,
    CaptureOwnershipHandoffStateRequest as RemoteCaptureOwnershipHandoffStateRequest,
    ConfirmOwnershipHandoffStateRequest as RemoteConfirmOwnershipHandoffStateRequest,
    DiscardOwnershipHandoffStateRequest as RemoteDiscardOwnershipHandoffStateRequest,
    EntityGatePurpose,
    PrepareForcedOwnershipRecoveryRequest as RemotePrepareForcedOwnershipRecoveryRequest,
    PrepareOwnershipHandoffStateRequest as RemotePrepareOwnershipHandoffStateRequest, Transport,
};
use nervix_models::{
    ClusterNodeIncarnation, ClusterNodeName, DomainName, NodeRef, OwnershipStateRecoveryOutcome,
    OwnershipStateReset, OwnershipStateResetCause, OwnershipTransition, ScheduledNode,
};
use tokio::time::{Duration, sleep};
use tracing::{debug, info, warn};

use super::{
    domain_lifecycle::DomainAlterError,
    entity_gate::{ClusterEntityGate, ENTITY_GATE_RELEASE_RETRY_INTERVAL},
    session_service::SessionServiceImpl,
};
use crate::runtime::{OwnershipHandoffError, OwnershipHandoffResult, Runtime};
pub(in crate::application) const FORCED_OWNERSHIP_RECOVERY_BUDGET: Duration =
    Duration::from_secs(5);

pub(in crate::application) struct PlannedOwnershipHandoff {
    operation_id: String,
    base_schedule_fingerprint: [u8; 32],
    target_schedule_fingerprint: [u8; 32],
    node_incarnations: BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
    gate: ClusterEntityGate,
    pub(in crate::application) moves: Vec<PlannedOwnershipMove>,
    pub(in crate::application) started_at: tokio::time::Instant,
    preparation_deadline: tokio::time::Instant,
    activation_deadline: tokio::time::Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct DrainMove {
    pub(in crate::application) label: String,
    pub(in crate::application) promoted_replica: Option<ClusterNodeName>,
    pub(in crate::application) fallback_node: Option<ClusterNodeName>,
}

#[derive(Clone, Copy)]
pub(in crate::application) enum AssignmentRelocation {
    Planned,
    Failure,
}

impl AssignmentRelocation {
    pub(in crate::application) fn target(
        self,
        desired_target: Option<ClusterNodeName>,
        existing_replica: Option<ClusterNodeName>,
    ) -> Option<ClusterNodeName> {
        match self {
            Self::Planned => desired_target.or(existing_replica),
            Self::Failure => existing_replica.or(desired_target),
        }
    }

    pub(in crate::application) fn retains_former_replica(self) -> bool {
        match self {
            Self::Planned => true,
            Self::Failure => false,
        }
    }

    pub(in crate::application) fn ownership_transition(
        self,
        source: ClusterNodeName,
        destination: ClusterNodeName,
        node: &ScheduledNode,
        promoted_replica: bool,
    ) -> OwnershipTransition {
        let state_recovery = match self {
            Self::Planned => OwnershipStateRecoveryOutcome::Complete,
            Self::Failure if promoted_replica => OwnershipStateRecoveryOutcome::Unverified,
            Self::Failure => OwnershipStateRecoveryOutcome::Reset,
        };
        let resets = if state_recovery == OwnershipStateRecoveryOutcome::Reset {
            node.ownership_state_components()
                .into_iter()
                .map(|component| OwnershipStateReset {
                    component,
                    cause: OwnershipStateResetCause::MissingCheckpoint,
                })
                .collect()
        } else {
            Vec::new()
        };
        OwnershipTransition {
            id: uuid::Uuid::now_v7().to_string(),
            source,
            destination,
            state_recovery,
            resets,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct PlannedOwnershipMove {
    pub(in crate::application) entity: NodeRef,
    pub(in crate::application) former_owner: ClusterNodeName,
    pub(in crate::application) destination: ClusterNodeName,
    replicas: Vec<ClusterNodeName>,
    promoted_replica: bool,
}

pub(in crate::application) struct ForcedOwnershipRecoveryCoordinator<'a> {
    pub(in crate::application) runtime: &'a Runtime,
    pub(in crate::application) interconnect: &'a Transport,
    pub(in crate::application) local_node_id: &'a ClusterNodeName,
    pub(in crate::application) node_incarnations:
        &'a BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
}

impl ForcedOwnershipRecoveryCoordinator<'_> {
    pub(in crate::application) async fn prepare_schedule(
        &self,
        current: &nervix_models::DomainSchedule,
        target: &mut nervix_models::DomainSchedule,
    ) {
        let base_schedule_fingerprint =
            match Runtime::ownership_handoff_schedule_fingerprint(current) {
                Ok(fingerprint) => fingerprint,
                Err(reason) => {
                    self.reset_every_move(
                        current,
                        target,
                        OwnershipStateResetCause::InvalidCheckpoint,
                    );
                    warn!(
                        domain = current.domain.as_str(),
                        error = %reason,
                        "forced ownership recovery could not fingerprint the committed schedule"
                    );
                    return;
                }
            };
        let target_schedule_fingerprint =
            match Runtime::ownership_handoff_schedule_fingerprint(target) {
                Ok(fingerprint) => fingerprint,
                Err(reason) => {
                    self.reset_every_move(
                        current,
                        target,
                        OwnershipStateResetCause::InvalidCheckpoint,
                    );
                    warn!(
                        domain = current.domain.as_str(),
                        error = %reason,
                        "forced ownership recovery could not fingerprint the target schedule"
                    );
                    return;
                }
            };
        struct PreparedForcedMove {
            moved: PlannedOwnershipMove,
            transition_id: String,
            result: OwnershipHandoffResult<nervix_interconnect::ForcedOwnershipRecoveryPreparation>,
        }

        let moves = planned_ownership_moves(Some(current), Some(target));
        let mut preparations = FuturesUnordered::new();
        for moved in moves {
            tokio::task::consume_budget().await;
            let Some(transition) = target
                .nodes
                .get(&moved.entity)
                .and_then(|node| node.ownership_transition.as_ref())
            else {
                continue;
            };
            if transition.state_recovery == OwnershipStateRecoveryOutcome::Complete {
                continue;
            }
            let transition_id = transition.id.clone();
            let destination_incarnation = self.node_incarnations.get(&moved.destination).copied();
            preparations.push(async move {
                let result = match destination_incarnation {
                    Some(destination_incarnation) => {
                        let deadline =
                            tokio::time::Instant::now() + FORCED_OWNERSHIP_RECOVERY_BUDGET;
                        let preparation = async {
                            let request = RemotePrepareForcedOwnershipRecoveryRequest {
                                operation_id: transition_id.clone(),
                                source: moved.former_owner.clone(),
                                destination: moved.destination.clone(),
                                destination_incarnation,
                                domain: current.domain.clone(),
                                entity: moved.entity.clone(),
                                base_schedule_fingerprint,
                                target_schedule_fingerprint,
                            };
                            if moved.destination == *self.local_node_id {
                                return self
                                    .runtime
                                    .prepare_forced_ownership_recovery(request, deadline)
                                    .await;
                            }
                            let response = self
                                .interconnect
                                .request(&moved.destination, request)
                                .await
                                .map_err(|error| {
                                    OwnershipHandoffError::transport(error.to_string())
                                })?;
                            response.map_err(|failure| {
                                OwnershipHandoffError::participant(failure.to_string())
                            })
                        };
                        match tokio::time::timeout_at(deadline, preparation).await {
                            Ok(result) => result,
                            Err(_) => Err(OwnershipHandoffError::deadline(
                                "state preparation exceeded its five-second budget",
                            )),
                        }
                    }
                    None => Err(OwnershipHandoffError::participant(format!(
                        "destination node '{}' has no live process incarnation",
                        moved.destination
                    ))),
                };
                PreparedForcedMove {
                    moved,
                    transition_id,
                    result,
                }
            });
        }
        while let Some(preparation) = preparations.next().await {
            tokio::task::consume_budget().await;
            let moved = preparation.moved;
            let node = target
                .nodes
                .get_mut(&moved.entity)
                .verified("the forced recovery move was derived from this target schedule");
            match preparation.result {
                Ok(prepared) => {
                    node.ownership_transition = Some(OwnershipTransition {
                        id: preparation.transition_id,
                        source: moved.former_owner.clone(),
                        destination: moved.destination.clone(),
                        state_recovery: prepared.state_recovery,
                        resets: prepared.resets,
                    });
                    warn!(
                        domain = current.domain.as_str(),
                        kind = moved.entity.kind.as_str(),
                        name = moved.entity.identifier.as_str(),
                        source = %moved.former_owner,
                        destination = %moved.destination,
                        state_recovery = prepared.state_recovery.as_ref(),
                        "forced ownership recovery prepared destination state"
                    );
                }
                Err(reason) => {
                    Self::mark_reset(
                        node,
                        preparation.transition_id,
                        moved.former_owner.clone(),
                        moved.destination.clone(),
                        OwnershipStateResetCause::MissingCheckpoint,
                    );
                    warn!(
                        domain = current.domain.as_str(),
                        kind = moved.entity.kind.as_str(),
                        name = moved.entity.identifier.as_str(),
                        source = %moved.former_owner,
                        destination = %moved.destination,
                        error = %reason,
                        "forced ownership recovery is publishing with recreated runtime state"
                    );
                }
            }
        }
    }

    fn reset_every_move(
        &self,
        current: &nervix_models::DomainSchedule,
        target: &mut nervix_models::DomainSchedule,
        cause: OwnershipStateResetCause,
    ) {
        for moved in planned_ownership_moves(Some(current), Some(target)) {
            let node = target
                .nodes
                .get_mut(&moved.entity)
                .verified("the forced recovery move was derived from this target schedule");
            Self::mark_reset(
                node,
                uuid::Uuid::now_v7().to_string(),
                moved.former_owner,
                moved.destination,
                cause,
            );
        }
    }

    fn mark_reset(
        node: &mut ScheduledNode,
        id: String,
        source: ClusterNodeName,
        destination: ClusterNodeName,
        cause: OwnershipStateResetCause,
    ) {
        node.ownership_transition = Some(OwnershipTransition {
            id,
            source,
            destination,
            state_recovery: OwnershipStateRecoveryOutcome::Reset,
            resets: node
                .ownership_state_components()
                .into_iter()
                .map(|component| OwnershipStateReset { component, cause })
                .collect(),
        });
    }
}

pub(in crate::application) fn prefer_former_owners_as_replicas(
    current: Option<&nervix_models::DomainSchedule>,
    planned: &mut nervix_models::DomainSchedule,
    live_nodes: &[ClusterNodeName],
) {
    let Some(current) = current else {
        return;
    };
    let live_nodes = live_nodes.iter().collect::<BTreeSet<_>>();
    for (identity, planned_node) in &mut planned.nodes {
        let Some(current_node) = current.nodes.get(identity) else {
            continue;
        };
        let (Some(former_owner), Some(destination)) = (
            current_node.execution_node(),
            planned_node.execution_node().cloned(),
        ) else {
            continue;
        };
        let replica_slots = planned_node.assigned_nodes.len();
        if *former_owner == destination || replica_slots < 2 || !live_nodes.contains(former_owner) {
            continue;
        }
        let mut assigned_nodes = vec![destination, former_owner.clone()];
        for assigned in &planned_node.assigned_nodes {
            if !assigned_nodes.contains(assigned) {
                assigned_nodes.push(assigned.clone());
            }
        }
        assigned_nodes.truncate(replica_slots);
        planned_node.assigned_nodes = assigned_nodes;
    }
}

pub(in crate::application) fn planned_ownership_moves(
    current: Option<&nervix_models::DomainSchedule>,
    planned: Option<&nervix_models::DomainSchedule>,
) -> Vec<PlannedOwnershipMove> {
    let (Some(current), Some(planned)) = (current, planned) else {
        return Vec::new();
    };
    let mut moves = Vec::new();
    for (identity, planned_node) in &planned.nodes {
        let Some(current_node) = current.nodes.get(identity) else {
            continue;
        };
        let Some(former_owner) = current_node.execution_node() else {
            continue;
        };
        let Some(destination) = planned_node.execution_node() else {
            continue;
        };
        if former_owner == destination {
            continue;
        }
        moves.push(PlannedOwnershipMove {
            entity: NodeRef {
                kind: planned_node.kind(),
                identifier: planned_node.identifier.clone(),
            },
            former_owner: former_owner.clone(),
            destination: destination.clone(),
            replicas: planned_node.replica_nodes().into_iter().cloned().collect(),
            promoted_replica: current_node.is_assigned_to(destination),
        });
    }
    moves.sort_by(|left, right| left.entity.cmp(&right.entity));
    moves
}

pub(in crate::application) fn mark_complete_ownership_transitions(
    current: Option<&nervix_models::DomainSchedule>,
    planned: &mut nervix_models::DomainSchedule,
) {
    let transition_id = uuid::Uuid::now_v7().to_string();
    for moved in planned_ownership_moves(current, Some(planned)) {
        let node = planned
            .nodes
            .get_mut(&moved.entity)
            .verified("every planned ownership move was derived from this target schedule");
        node.ownership_transition = Some(OwnershipTransition {
            id: transition_id.clone(),
            source: moved.former_owner,
            destination: moved.destination,
            state_recovery: OwnershipStateRecoveryOutcome::Complete,
            resets: Vec::new(),
        });
    }
}

pub(in crate::application) fn format_planned_ownership_move(
    moved: &PlannedOwnershipMove,
) -> String {
    let replicas = if moved.replicas.is_empty() {
        "none".to_string()
    } else {
        moved.replicas.join(",")
    };
    format!(
        "- kind={} name={} from={} to={} replicas={} promoted_replica={}",
        moved.entity.kind.as_ref(),
        moved.entity.identifier.as_str(),
        moved.former_owner,
        moved.destination,
        replicas,
        if moved.promoted_replica { "yes" } else { "no" }
    )
}

pub(in crate::application) fn planned_relocation_count(
    current: Option<&nervix_models::DomainSchedule>,
    planned: Option<&nervix_models::DomainSchedule>,
) -> usize {
    planned_ownership_moves(current, planned).len()
}

impl SessionServiceImpl {
    /// Cluster nodes the cluster considers usable: gossip peers that are not marked unavailable.
    ///
    /// Scheduling and failover read liveness this way, so every leader-orchestrated hold must too.
    /// A node marked unavailable cannot answer a gate request, and contacting it only spends the
    /// request deadline before the hold fails.
    pub(in crate::application) async fn available_node_ids(&self) -> Vec<ClusterNodeName> {
        self.available_node_incarnations()
            .await
            .into_keys()
            .collect()
    }

    async fn available_node_incarnations(
        &self,
    ) -> BTreeMap<ClusterNodeName, ClusterNodeIncarnation> {
        let gossip = self.inner.cluster.availability_state().await;
        gossip
            .live_nodes
            .into_iter()
            .filter(|node| !gossip.dead_node_ids.contains(&node.node_id))
            .map(|node| (node.node_id, node.incarnation))
            .collect()
    }

    pub(in crate::application) async fn live_node_incarnations(
        &self,
    ) -> BTreeMap<ClusterNodeName, ClusterNodeIncarnation> {
        self.inner
            .cluster
            .gossip_state()
            .await
            .live_nodes
            .into_iter()
            .map(|node| (node.node_id, node.incarnation))
            .collect()
    }

    pub(in crate::application) fn verify_ownership_handoff_node_incarnation(
        current: &BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
        node: &ClusterNodeName,
        expected: ClusterNodeIncarnation,
        role: &str,
    ) -> OwnershipHandoffResult<()> {
        let Some(actual) = current.get(node) else {
            return Err(OwnershipHandoffError::participant(format!(
                "{role} node '{node}' is unavailable during ownership handoff"
            )));
        };
        if *actual != expected {
            return Err(OwnershipHandoffError::participant(format!(
                "{role} node '{node}' changed process incarnation during ownership handoff"
            )));
        }
        Ok(())
    }

    async fn verify_planned_ownership_handoff_incarnations(
        &self,
        handoff: &PlannedOwnershipHandoff,
    ) -> OwnershipHandoffResult<()> {
        let current = self.live_node_incarnations().await;
        for moved in &handoff.moves {
            tokio::task::consume_budget().await;
            let source = *handoff
                .node_incarnations
                .get(&moved.former_owner)
                .verified("every planned former owner has a bound incarnation");
            Self::verify_ownership_handoff_node_incarnation(
                &current,
                &moved.former_owner,
                source,
                "source",
            )?;
            let destination = *handoff
                .node_incarnations
                .get(&moved.destination)
                .verified("every planned destination has a bound incarnation");
            Self::verify_ownership_handoff_node_incarnation(
                &current,
                &moved.destination,
                destination,
                "destination",
            )?;
        }
        Ok(())
    }

    pub(in crate::application) async fn begin_planned_ownership_handoff(
        &self,
        domain: &DomainName,
        current: Option<&nervix_models::DomainSchedule>,
        planned: Option<&nervix_models::DomainSchedule>,
    ) -> Result<Option<PlannedOwnershipHandoff>, Report<DomainAlterError>> {
        let moves = planned_ownership_moves(current, planned);
        if moves.is_empty() {
            return Ok(None);
        }
        let current = current
            .verified("an ownership move can only be derived from a current domain schedule");
        let planned = planned
            .verified("an ownership move can only be derived from a planned domain schedule");
        let base_schedule_fingerprint = Runtime::ownership_handoff_schedule_fingerprint(current)
            .map_err(|reason| {
                Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                    reason: reason.to_string(),
                })
            })?;
        let target_schedule_fingerprint = Runtime::ownership_handoff_schedule_fingerprint(planned)
            .map_err(|reason| {
                Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                    reason: reason.to_string(),
                })
            })?;
        let first_move = moves
            .first()
            .verified("the empty ownership move set returned before planning a handoff");
        let first_node = planned
            .nodes
            .get(&first_move.entity)
            .verified("every ownership move was derived from the planned schedule");
        let Some(first_transition) = first_node.ownership_transition.as_ref() else {
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: "planned schedule does not identify its ownership handoff transition"
                    .to_string(),
            }));
        };
        let operation_id = first_transition.id.clone();
        for moved in &moves {
            let node = planned
                .nodes
                .get(&moved.entity)
                .verified("every ownership move was derived from the planned schedule");
            let Some(transition) = node.ownership_transition.as_ref() else {
                return Err(Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                    reason: format!(
                        "planned {} '{}' does not identify its ownership handoff transition",
                        moved.entity.kind.as_str(),
                        moved.entity.identifier.as_str()
                    ),
                }));
            };
            if transition.id != operation_id
                || transition.source != moved.former_owner
                || transition.destination != moved.destination
                || transition.state_recovery != OwnershipStateRecoveryOutcome::Complete
                || !transition.resets.is_empty()
            {
                return Err(Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                    reason: format!(
                        "planned {} '{}' has an inconsistent ownership handoff transition",
                        moved.entity.kind.as_str(),
                        moved.entity.identifier.as_str()
                    ),
                }));
            }
        }
        let node_incarnations = self.available_node_incarnations().await;
        if let Some(moved) = moves
            .iter()
            .find(|moved| !node_incarnations.contains_key(&moved.former_owner))
        {
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: format!(
                    "former owner '{}' is unavailable before ownership handoff",
                    moved.former_owner
                ),
            }));
        }
        if let Some(moved) = moves
            .iter()
            .find(|moved| !node_incarnations.contains_key(&moved.destination))
        {
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: format!(
                    "destination node '{}' is unavailable before ownership handoff",
                    moved.destination
                ),
            }));
        }
        let affected_entities = moves
            .iter()
            .map(|moved| moved.entity.clone())
            .collect::<Vec<_>>();
        let relays = Runtime::ownership_handoff_relays_for_schedule(current, &affected_entities);
        let former_owners = moves
            .iter()
            .map(|moved| moved.former_owner.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let started_at = tokio::time::Instant::now();
        let phase_budget = self.inner.runtime.entity_gate_deadline();
        let preparation_deadline = started_at.checked_add(phase_budget).ok_or_else(|| {
            Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: "ownership handoff preparation deadline exceeds the runtime instant range"
                    .to_string(),
            })
        })?;
        let activation_deadline =
            preparation_deadline
                .checked_add(phase_budget)
                .ok_or_else(|| {
                    Report::new(DomainAlterError::EntityGate {
                        domain: domain.clone(),
                        operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                        reason: "ownership handoff activation deadline exceeds the runtime \
                                 instant range"
                            .to_string(),
                    })
                })?;
        let gate = self
            .engage_cluster_entity_gates(
                domain,
                &relays,
                &affected_entities,
                EntityGatePurpose::OwnershipHandoff,
                activation_deadline,
            )
            .await?;
        #[cfg(feature = "testing")]
        self.inner.runtime.pause_entity_gate_if_armed(domain).await;
        if let Err(error) = self
            .wait_for_cluster_entity_drain(
                &gate,
                &relays,
                &affected_entities,
                EntityGatePurpose::OwnershipHandoff,
                &former_owners,
                preparation_deadline,
            )
            .await
        {
            self.release_cluster_entity_gates(gate).await;
            return Err(error);
        }
        let mut prepared = Vec::new();
        for moved in &moves {
            tokio::task::consume_budget().await;
            let result = tokio::time::timeout_at(preparation_deadline, async {
                let checkpoints = self
                    .capture_ownership_handoff_state(
                        &operation_id,
                        domain,
                        moved,
                        *node_incarnations
                            .get(&moved.former_owner)
                            .verified("every former owner was found in live gossip above"),
                        base_schedule_fingerprint,
                    )
                    .await?;
                self.prepare_ownership_handoff_state(RemotePrepareOwnershipHandoffStateRequest {
                    operation_id: operation_id.clone(),
                    source: moved.former_owner.clone(),
                    destination: moved.destination.clone(),
                    source_incarnation: *node_incarnations
                        .get(&moved.former_owner)
                        .verified("every former owner was found in live gossip above"),
                    destination_incarnation: *node_incarnations
                        .get(&moved.destination)
                        .verified("every destination was found in live gossip above"),
                    domain: domain.clone(),
                    entity: moved.entity.clone(),
                    base_schedule_fingerprint,
                    target_schedule_fingerprint,
                    checkpoints,
                })
                .await
            })
            .await;
            match result {
                Ok(Ok(())) => prepared.push(moved.clone()),
                Ok(Err(reason)) => {
                    self.discard_ownership_handoff_state(&operation_id, domain, &prepared)
                        .await;
                    self.release_cluster_entity_gates(gate).await;
                    return Err(Report::new(DomainAlterError::EntityGate {
                        domain: domain.clone(),
                        operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                        reason: format!(
                            "failed to prepare {} '{}' from node '{}' on node '{}': {reason}",
                            moved.entity.kind.as_str(),
                            moved.entity.identifier.as_str(),
                            moved.former_owner,
                            moved.destination
                        ),
                    }));
                }
                Err(_) => {
                    self.discard_ownership_handoff_state(&operation_id, domain, &prepared)
                        .await;
                    self.release_cluster_entity_gates(gate).await;
                    return Err(Report::new(DomainAlterError::EntityGate {
                        domain: domain.clone(),
                        operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                        reason: format!(
                            "timed out preparing {} '{}' from node '{}' on node '{}'",
                            moved.entity.kind.as_str(),
                            moved.entity.identifier.as_str(),
                            moved.former_owner,
                            moved.destination
                        ),
                    }));
                }
            }
        }
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_ownership_handoff_after_preparation_if_armed(domain)
            .await;
        let handoff = PlannedOwnershipHandoff {
            operation_id,
            base_schedule_fingerprint,
            target_schedule_fingerprint,
            node_incarnations,
            gate,
            moves,
            started_at,
            preparation_deadline,
            activation_deadline,
        };
        if let Err(reason) = self
            .verify_planned_ownership_handoff_incarnations(&handoff)
            .await
        {
            self.abort_planned_ownership_handoff(domain, handoff).await;
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: reason.to_string(),
            }));
        }
        if let Err(reason) = self.confirm_planned_ownership_handoff(&handoff).await {
            self.abort_planned_ownership_handoff(domain, handoff).await;
            return Err(Report::new(DomainAlterError::EntityGate {
                domain: domain.clone(),
                operation: EntityGatePurpose::OwnershipHandoff.operation_name(),
                reason: reason.to_string(),
            }));
        }
        Ok(Some(handoff))
    }

    async fn capture_ownership_handoff_state(
        &self,
        operation_id: &str,
        domain: &DomainName,
        moved: &PlannedOwnershipMove,
        source_incarnation: ClusterNodeIncarnation,
        base_schedule_fingerprint: [u8; 32],
    ) -> OwnershipHandoffResult<Vec<nervix_interconnect::OwnershipHandoffCheckpoint>> {
        Self::verify_ownership_handoff_node_incarnation(
            &self.live_node_incarnations().await,
            &moved.former_owner,
            source_incarnation,
            "source",
        )?;
        if moved.former_owner == *self.inner.consensus.local_node_id() {
            let scheduled = self
                .prepare_owner_control_request(
                    domain,
                    moved.entity.kind,
                    moved.entity.identifier.clone(),
                )
                .await
                .map_err(OwnershipHandoffError::participant)?;
            if !scheduled.is_primary_on(self.inner.consensus.local_node_id()) {
                return Err(OwnershipHandoffError::participant(format!(
                    "{} '{}' is not owned by source node '{}'",
                    moved.entity.kind.as_str(),
                    moved.entity.identifier.as_str(),
                    moved.former_owner
                )));
            }
            return self
                .inner
                .runtime
                .capture_ownership_handoff_state(domain, &moved.entity, base_schedule_fingerprint)
                .await;
        }
        let response = self
            .inner
            .interconnect
            .request(
                &moved.former_owner,
                RemoteCaptureOwnershipHandoffStateRequest {
                    operation_id: operation_id.to_string(),
                    source: moved.former_owner.clone(),
                    source_incarnation,
                    domain: domain.clone(),
                    entity: moved.entity.clone(),
                    base_schedule_fingerprint,
                },
            )
            .await
            .map_err(|error| OwnershipHandoffError::transport(error.to_string()))?;
        response.map_err(|failure| OwnershipHandoffError::participant(failure.to_string()))
    }

    async fn prepare_ownership_handoff_state(
        &self,
        request: RemotePrepareOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        let current_incarnations = self.live_node_incarnations().await;
        Self::verify_ownership_handoff_node_incarnation(
            &current_incarnations,
            &request.source,
            request.source_incarnation,
            "source",
        )?;
        Self::verify_ownership_handoff_node_incarnation(
            &current_incarnations,
            &request.destination,
            request.destination_incarnation,
            "destination",
        )?;
        let scheduled = self
            .scheduled_model_node(
                &request.domain,
                request.entity.kind,
                request.entity.identifier.clone(),
            )
            .await
            .ok_or_else(|| {
                OwnershipHandoffError::schedule(format!(
                    "{} '{}' is absent from the committed schedule",
                    request.entity.kind.as_str(),
                    request.entity.identifier.as_str()
                ))
            })?;
        if scheduled.execution_node() != Some(&request.source) {
            return Err(OwnershipHandoffError::participant(format!(
                "{} '{}' is no longer owned by source node '{}'",
                request.entity.kind.as_str(),
                request.entity.identifier.as_str(),
                request.source
            )));
        }
        if request.destination == *self.inner.consensus.local_node_id() {
            return self
                .inner
                .runtime
                .prepare_ownership_handoff_state(request)
                .await;
        }
        let destination = request.destination.clone();
        let response = self
            .inner
            .interconnect
            .request(&destination, request)
            .await
            .map_err(|error| OwnershipHandoffError::transport(error.to_string()))?;
        response.map_err(|failure| OwnershipHandoffError::participant(failure.to_string()))
    }

    async fn confirm_planned_ownership_handoff(
        &self,
        handoff: &PlannedOwnershipHandoff,
    ) -> OwnershipHandoffResult<()> {
        for moved in &handoff.moves {
            tokio::task::consume_budget().await;
            let request = RemoteConfirmOwnershipHandoffStateRequest {
                operation_id: handoff.operation_id.clone(),
                source: moved.former_owner.clone(),
                destination: moved.destination.clone(),
                source_incarnation: *handoff
                    .node_incarnations
                    .get(&moved.former_owner)
                    .verified("every planned former owner has a bound incarnation"),
                destination_incarnation: *handoff
                    .node_incarnations
                    .get(&moved.destination)
                    .verified("every planned destination has a bound incarnation"),
                domain: handoff.gate.domain.clone(),
                entity: moved.entity.clone(),
                base_schedule_fingerprint: handoff.base_schedule_fingerprint,
                target_schedule_fingerprint: handoff.target_schedule_fingerprint,
            };
            self.confirm_ownership_handoff_on_node(
                &moved.former_owner,
                request.clone(),
                handoff.preparation_deadline,
            )
            .await?;
            tokio::task::consume_budget().await;
            self.confirm_ownership_handoff_on_node(
                &moved.destination,
                request,
                handoff.preparation_deadline,
            )
            .await?;
        }
        Ok(())
    }

    async fn confirm_ownership_handoff_on_node(
        &self,
        node: &ClusterNodeName,
        request: RemoteConfirmOwnershipHandoffStateRequest,
        deadline: tokio::time::Instant,
    ) -> OwnershipHandoffResult<()> {
        let confirmation = async {
            if node == self.inner.consensus.local_node_id() {
                return self.confirm_local_ownership_handoff_state(request).await;
            }
            loop {
                tokio::task::consume_budget().await;
                match self.inner.interconnect.request(node, request.clone()).await {
                    Ok(Ok(())) => return Ok(()),
                    Ok(Err(failure)) => {
                        return Err(OwnershipHandoffError::participant(failure.to_string()));
                    }
                    Err(error) => {
                        debug!(
                            %node,
                            error = %error,
                            "ownership handoff participant confirmation is waiting for interconnect"
                        );
                        sleep(ENTITY_GATE_RELEASE_RETRY_INTERVAL).await;
                    }
                }
            }
        };
        match tokio::time::timeout_at(deadline, confirmation).await {
            Ok(result) => result,
            Err(_) => Err(OwnershipHandoffError::deadline(format!(
                "timed out confirming ownership handoff participant node '{node}'"
            ))),
        }
    }

    pub(in crate::application) async fn confirm_local_ownership_handoff_state(
        &self,
        request: RemoteConfirmOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        let local_node = self.inner.consensus.local_node_id();
        if local_node == &request.source {
            Self::verify_ownership_handoff_node_incarnation(
                &self.live_node_incarnations().await,
                local_node,
                request.source_incarnation,
                "source",
            )?;
            let schedule = self.inner.consensus.current_schedule().await;
            let current = schedule.domain(&request.domain).ok_or_else(|| {
                OwnershipHandoffError::schedule(format!(
                    "domain '{}' has no committed schedule while confirming ownership handoff",
                    request.domain.as_str()
                ))
            })?;
            if Runtime::ownership_handoff_schedule_fingerprint(current)?
                != request.base_schedule_fingerprint
            {
                return Err(OwnershipHandoffError::schedule(format!(
                    "domain '{}' changed schedule before ownership handoff publication",
                    request.domain.as_str()
                )));
            }
            let scheduled = current.nodes.get(&request.entity).ok_or_else(|| {
                OwnershipHandoffError::schedule(format!(
                    "{} '{}' is absent from the ownership handoff base schedule",
                    request.entity.kind.as_str(),
                    request.entity.identifier.as_str()
                ))
            })?;
            if !scheduled.is_primary_on(&request.source) {
                return Err(OwnershipHandoffError::participant(format!(
                    "{} '{}' is no longer owned by source node '{}'",
                    request.entity.kind.as_str(),
                    request.entity.identifier.as_str(),
                    request.source
                )));
            }
            let entity = nervix_models::DomainNodeRef::node_in(
                request.domain,
                request.entity.kind,
                request.entity.identifier,
            );
            if !self
                .inner
                .runtime
                .ownership_handoff_entity_is_frozen(&entity)
            {
                return Err(OwnershipHandoffError::participant(format!(
                    "source node '{}' no longer holds the ownership handoff freeze",
                    request.source
                )));
            }
            return Ok(());
        }
        if local_node == &request.destination {
            Self::verify_ownership_handoff_node_incarnation(
                &self.live_node_incarnations().await,
                local_node,
                request.destination_incarnation,
                "destination",
            )?;
            return self
                .inner
                .runtime
                .verify_ownership_handoff_preparation(&request);
        }
        Err(OwnershipHandoffError::participant(format!(
            "ownership handoff confirmation names nodes '{}' and '{}' but reached '{}'",
            request.source, request.destination, local_node
        )))
    }

    async fn discard_ownership_handoff_state(
        &self,
        operation_id: &str,
        domain: &DomainName,
        moves: &[PlannedOwnershipMove],
    ) {
        for moved in moves {
            tokio::task::consume_budget().await;
            if moved.destination == *self.inner.consensus.local_node_id() {
                if let Err(error) = self.inner.runtime.discard_prepared_ownership_handoff_state(
                    operation_id,
                    domain,
                    &moved.entity,
                ) {
                    warn!(
                        domain = domain.as_str(),
                        destination = %moved.destination,
                        error = %error,
                        "failed to discard abandoned ownership handoff state"
                    );
                }
                continue;
            }
            let result = self
                .inner
                .interconnect
                .request(
                    &moved.destination,
                    RemoteDiscardOwnershipHandoffStateRequest {
                        operation_id: operation_id.to_string(),
                        domain: domain.clone(),
                        entity: moved.entity.clone(),
                    },
                )
                .await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) => Some(error.to_string()),
            };
            if let Some(error) = error {
                warn!(
                    domain = domain.as_str(),
                    destination = %moved.destination,
                    error = %error,
                    "failed to discard abandoned ownership handoff state"
                );
            }
        }
    }

    pub(in crate::application) async fn abort_planned_ownership_handoff(
        &self,
        domain: &DomainName,
        handoff: PlannedOwnershipHandoff,
    ) {
        self.discard_ownership_handoff_state(&handoff.operation_id, domain, &handoff.moves)
            .await;
        self.release_cluster_entity_gates(handoff.gate).await;
    }

    pub(in crate::application) async fn activate_local_ownership_handoff_state(
        &self,
        request: &RemoteActivateOwnershipHandoffStateRequest,
    ) -> OwnershipHandoffResult<()> {
        let deadline = tokio::time::Instant::now()
            .checked_add(request.activation_budget)
            .ok_or_else(|| {
                OwnershipHandoffError::deadline(
                    "ownership handoff activation deadline exceeds the runtime instant range",
                )
            })?;
        let activation = async {
            if request.destination != *self.inner.consensus.local_node_id() {
                return Err(OwnershipHandoffError::participant(format!(
                    "ownership handoff for {} '{}' targets node '{}' but reached '{}'",
                    request.entity.kind.as_str(),
                    request.entity.identifier.as_str(),
                    request.destination,
                    self.inner.consensus.local_node_id()
                )));
            }
            let current_incarnations = self.live_node_incarnations().await;
            Self::verify_ownership_handoff_node_incarnation(
                &current_incarnations,
                &request.source,
                request.source_incarnation,
                "source",
            )?;
            Self::verify_ownership_handoff_node_incarnation(
                &current_incarnations,
                &request.destination,
                request.destination_incarnation,
                "destination",
            )?;

            let mut schedule_rx = self.inner.consensus.subscribe_schedule();
            let target_schedule = loop {
                tokio::task::consume_budget().await;
                let schedule = self.inner.consensus.current_schedule().await;
                let current = schedule.domain(&request.domain).ok_or_else(|| {
                    OwnershipHandoffError::schedule(format!(
                        "domain '{}' has no committed schedule while activating ownership handoff",
                        request.domain.as_str()
                    ))
                })?;
                let fingerprint = Runtime::ownership_handoff_schedule_fingerprint(current)?;
                if fingerprint == request.target_schedule_fingerprint {
                    let node = current.nodes.get(&request.entity).ok_or_else(|| {
                        OwnershipHandoffError::schedule(format!(
                            "{} '{}' is absent from the ownership handoff target schedule",
                            request.entity.kind.as_str(),
                            request.entity.identifier.as_str()
                        ))
                    })?;
                    if !node.is_primary_on(&request.destination) {
                        return Err(OwnershipHandoffError::participant(format!(
                            "{} '{}' is not owned by destination node '{}' in the committed \
                             target schedule",
                            request.entity.kind.as_str(),
                            request.entity.identifier.as_str(),
                            request.destination
                        )));
                    }
                    break current.clone();
                }
                if fingerprint != request.base_schedule_fingerprint {
                    return Err(OwnershipHandoffError::schedule(format!(
                        "domain '{}' advanced to a different schedule before ownership handoff \
                         activation",
                        request.domain.as_str()
                    )));
                }
                schedule_rx.changed().await.assured(
                    "the consensus observer retains its schedule sender for the server lifetime",
                );
            };

            self.inner
                .runtime
                .activate_persisted_ownership_handoff(
                    self.inner.consensus.local_node_id(),
                    request,
                    target_schedule,
                )
                .await
        };

        tokio::time::timeout_at(deadline, activation)
            .await
            .map_err(|_| {
                OwnershipHandoffError::deadline(format!(
                    "timed out waiting for {} '{}' to activate on node '{}'",
                    request.entity.kind.as_str(),
                    request.entity.identifier.as_str(),
                    request.destination
                ))
            })?
    }

    async fn activate_planned_ownership_handoff(
        &self,
        domain: &DomainName,
        handoff: &PlannedOwnershipHandoff,
    ) -> OwnershipHandoffResult<()> {
        self.verify_planned_ownership_handoff_incarnations(handoff)
            .await?;
        for moved in &handoff.moves {
            tokio::task::consume_budget().await;
            let source_incarnation = *handoff
                .node_incarnations
                .get(&moved.former_owner)
                .verified("every planned former owner has a bound incarnation");
            let destination_incarnation = *handoff
                .node_incarnations
                .get(&moved.destination)
                .verified("every planned destination has a bound incarnation");
            let remaining = handoff
                .activation_deadline
                .saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(OwnershipHandoffError::deadline(format!(
                    "timed out waiting for {} '{}' to activate on node '{}'",
                    moved.entity.kind.as_str(),
                    moved.entity.identifier.as_str(),
                    moved.destination
                )));
            }
            let request = RemoteActivateOwnershipHandoffStateRequest {
                operation_id: handoff.operation_id.clone(),
                source: moved.former_owner.clone(),
                destination: moved.destination.clone(),
                source_incarnation,
                destination_incarnation,
                domain: domain.clone(),
                entity: moved.entity.clone(),
                base_schedule_fingerprint: handoff.base_schedule_fingerprint,
                target_schedule_fingerprint: handoff.target_schedule_fingerprint,
                activation_budget: remaining,
            };
            if request.destination == *self.inner.consensus.local_node_id() {
                self.activate_local_ownership_handoff_state(&request)
                    .await?;
            } else {
                let destination = request.destination.clone();
                let response = self
                    .inner
                    .interconnect
                    .request_with_timeout(&destination, request, remaining)
                    .await
                    .map_err(|error| OwnershipHandoffError::transport(error.to_string()))?;
                response
                    .map_err(|failure| OwnershipHandoffError::participant(failure.to_string()))?;
            }
        }
        Ok(())
    }

    pub(in crate::application) async fn finish_planned_ownership_handoff(
        &self,
        domain: &DomainName,
        handoff: PlannedOwnershipHandoff,
    ) -> OwnershipHandoffResult<()> {
        let activation = self
            .activate_planned_ownership_handoff(domain, &handoff)
            .await;
        if let Err(error) = activation {
            let hold_duration = handoff.started_at.elapsed();
            for moved in &handoff.moves {
                warn!(
                    domain = domain.as_str(),
                    kind = moved.entity.kind.as_ref(),
                    name = moved.entity.identifier.as_str(),
                    former_owner = %moved.former_owner,
                    destination = %moved.destination,
                    hold_duration_millis = hold_duration.as_millis(),
                    promoted_replica = moved.promoted_replica,
                    error = %error,
                    "planned ownership handoff destination did not confirm activation"
                );
            }
            handoff.gate.defer_release_to_lease_deadline();
            return Err(error);
        }
        let hold_duration = handoff.started_at.elapsed();
        for moved in &handoff.moves {
            info!(
                domain = domain.as_str(),
                kind = moved.entity.kind.as_ref(),
                name = moved.entity.identifier.as_str(),
                former_owner = %moved.former_owner,
                destination = %moved.destination,
                hold_duration_millis = hold_duration.as_millis(),
                promoted_replica = moved.promoted_replica,
                "planned ownership handoff completed"
            );
            if !moved.promoted_replica {
                warn!(
                    domain = domain.as_str(),
                    kind = moved.entity.kind.as_ref(),
                    name = moved.entity.identifier.as_str(),
                    former_owner = %moved.former_owner,
                    destination = %moved.destination,
                    "planned ownership handoff moved a runtime node without replicated state"
                );
            }
        }
        self.discard_ownership_handoff_state(&handoff.operation_id, domain, &handoff.moves)
            .await;
        self.release_cluster_entity_gates(handoff.gate).await;
        Ok(())
    }

    pub(in crate::application) fn defer_planned_ownership_handoff_release(
        &self,
        domain: &DomainName,
        handoff: PlannedOwnershipHandoff,
        error: &crate::runtime::RuntimeError,
    ) {
        let hold_duration = handoff.started_at.elapsed();
        for moved in &handoff.moves {
            warn!(
                domain = domain.as_str(),
                kind = moved.entity.kind.as_ref(),
                name = moved.entity.identifier.as_str(),
                former_owner = %moved.former_owner,
                destination = %moved.destination,
                hold_duration_millis = hold_duration.as_millis(),
                promoted_replica = moved.promoted_replica,
                error = %error,
                "planned ownership handoff activation failed; gate remains held until its deadline"
            );
        }
        handoff.gate.defer_release_to_lease_deadline();
    }
}
