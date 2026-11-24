//! Placing a stopped domain's units on nodes their node rules admit before START runs them.
//!
//! Layer: control plane.
//!
//! - **Owns.** Judging a stopped domain's committed schedule against its node rules before its
//!   START commits, moving each unit whose stored node a rule does not admit through the planned
//!   ownership handoff, and holding the START while some unit has no node its rules admit.
//! - **Depends on.** The registry's node rule judgement and scheduler, the schedule planning
//!   snapshot, the planned ownership handoff and the consensus schedule publication.
//! - **Must not know.** How the START resolves or commits its start, or how a client renders the
//!   wait.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_consensus::DomainPlanningInputs;
use nervix_models::{ClusterNodeName, DomainName, DomainSchedule, DomainStatus, NodeUnitRef};
use nervix_primitives::time::sleep;
use thiserror::Error;
use tracing::{debug, info, warn};

use super::{
    command_result::CommandResult, model_mutation::command_error,
    ownership_handoff::mark_complete_ownership_transitions,
    schedule_planning::DomainSchedulePlanningSnapshot, session_service::SessionServiceImpl,
};
use crate::registry::{ActiveGraph, ScheduleActivity, UnitRuleState};

/// Why a stopped domain's START could not place its units on nodes its node rules admit.
#[derive(Debug, Error)]
pub(in crate::application) enum StartPlacementError {
    #[error("this node stopped leading before START of domain '{domain}' placed its units")]
    NotLeader {
        domain: DomainName,
        #[cfg_attr(
            nervix_lint,
            nervix::identity(node, reason = "the current leader the START is redirected to")
        )]
        leader: Option<ClusterNodeName>,
    },
    #[error(
        "START of domain '{domain}' stopped waiting for its node rules because the node is \
         shutting down"
    )]
    ShuttingDown { domain: DomainName },
    #[error("START of domain '{domain}' could not judge its units against its node rules")]
    Judge { domain: DomainName },
    #[error("START of domain '{domain}' could not move a unit to a node its node rules admit")]
    Move { domain: DomainName },
}

/// Whether moving a stopped domain's misplaced units changed its schedule.
enum StartPlacementMove {
    Moved,
    /// The scheduler placed them where they are: no admitted node it may use can take them now.
    Unchanged,
    /// The move could not begin or publish yet, as when the destination has just restarted and
    /// does not serve its gates, or the planning inputs changed meanwhile. Nothing moved, and
    /// START looks again once the cluster may have changed.
    Deferred(Report<StartPlacementError>),
}

/// What one look at a stopped domain found before its START.
enum StartPlacement {
    /// Every constrained unit is on a node its rules admit, or nothing constrains the domain.
    Placed,
    /// Some unit has no node its rules admit; START waits for the cluster to change.
    Waiting { units: Vec<NodeUnitRef> },
    /// These units sit on nodes their rules do not admit, and every one has a node that does.
    Misplaced { units: BTreeSet<NodeUnitRef> },
}

impl SessionServiceImpl {
    /// Before the START of the stopped `domain` commits: moves every unit whose stored node a node
    /// rule does not admit to one that does, carrying its state through the planned ownership
    /// handoff, and holds the START, with the domain's mutation lease, while some unit has no node
    /// its rules admit. A domain that is not stopped, or that no node rule constrains, is left to
    /// the START as it is.
    pub(in crate::application) async fn place_units_for_start(
        &self,
        domain: &DomainName,
    ) -> error_stack::Result<(), StartPlacementError> {
        let mut announced_wait = false;
        let mut announced_deferral = false;
        loop {
            nervix_primitives::task::consume_budget().await;
            let leader = self.inner.consensus.current_leader().await;
            if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
                return Err(Report::new(StartPlacementError::NotLeader {
                    domain: domain.clone(),
                    leader,
                }));
            }
            let inputs = self.inner.consensus.domain_planning_inputs(domain).await;
            let stopped = inputs
                .state()
                .is_some_and(|state| matches!(state.status, DomainStatus::Stopped));
            let (true, Some(graph), Some(schedule)) = (
                stopped,
                self.inner.registry.active_graph(domain),
                inputs.schedule().cloned(),
            ) else {
                return Ok(());
            };
            let planning = self
                .capture_domain_schedule_planning_snapshot(&inputs)
                .await;
            match Self::start_placement(domain, &graph, &schedule, &planning) {
                Ok(StartPlacement::Placed) => return Ok(()),
                Ok(StartPlacement::Waiting { units }) => {
                    if !announced_wait {
                        announced_wait = true;
                        info!(
                            domain = domain.as_str(),
                            units = %super::describe_output::format_placement_unit_nodes(
                                &units
                            ),
                            "START waits until a cluster node satisfies the node rules of every unit"
                        );
                    }
                    #[cfg(feature = "testing")]
                    self.inner
                        .runtime
                        .pause_start_placement_if_armed(
                            self.inner.consensus.local_node_id(),
                            domain,
                            crate::fault_injection::StartPlacementBoundary::Waiting,
                        )
                        .await;
                    self.wait_for_start_placement(domain).await?;
                }
                Ok(StartPlacement::Misplaced { units }) => {
                    let moved = self
                        .move_misplaced_units(domain, &graph, schedule, &inputs, &planning, &units)
                        .await?;
                    match moved {
                        StartPlacementMove::Moved => {}
                        // The scheduler found no admitted node the judgement saw: look again once
                        // the cluster may have changed, rather than planning the same schedule.
                        StartPlacementMove::Unchanged => {
                            self.wait_for_start_placement(domain).await?;
                        }
                        StartPlacementMove::Deferred(error) => {
                            if announced_deferral {
                                debug!(
                                    domain = domain.as_str(),
                                    error = %format_args!("{error:#}"),
                                    "START still cannot move units to nodes their node rules admit"
                                );
                            } else {
                                announced_deferral = true;
                                warn!(
                                    domain = domain.as_str(),
                                    error = %format_args!("{error:#}"),
                                    "START cannot move units to nodes their node rules admit yet \
                                     and looks again"
                                );
                            }
                            #[cfg(feature = "testing")]
                            self.inner
                                .runtime
                                .pause_start_placement_if_armed(
                                    self.inner.consensus.local_node_id(),
                                    domain,
                                    crate::fault_injection::StartPlacementBoundary::Deferred,
                                )
                                .await;
                            self.wait_for_start_placement(domain).await?;
                        }
                    }
                }
                Err(error) => {
                    return Err(error.change_context(StartPlacementError::Judge {
                        domain: domain.clone(),
                    }));
                }
            }
        }
    }

    /// Waits one scheduling interval before START looks at its units again, unless the node
    /// begins shutting down.
    async fn wait_for_start_placement(
        &self,
        domain: &DomainName,
    ) -> error_stack::Result<(), StartPlacementError> {
        nervix_primitives::select! {
            _ = self.inner.drain_support_shutdown.cancelled() => {
                Err(Report::new(StartPlacementError::ShuttingDown {
                    domain: domain.clone(),
                }))
            }
            _ = sleep(self.inner.operational.control_plane.scheduling_interval()) => Ok(()),
        }
    }

    /// The answer a START gives when placing its units failed: a redirect to the current leader,
    /// or the failure.
    pub(in crate::application) async fn start_placement_refusal(
        &self,
        error: Report<StartPlacementError>,
    ) -> CommandResult {
        if let StartPlacementError::NotLeader { leader, .. } = error.current_context() {
            return self.not_leader_response("", leader.clone().into()).await;
        }
        command_error(format!("{error:#}"))
    }

    /// Judges a stopped domain's committed schedule for its START.
    fn start_placement(
        domain: &DomainName,
        graph: &ActiveGraph,
        schedule: &DomainSchedule,
        planning: &DomainSchedulePlanningSnapshot,
    ) -> error_stack::Result<StartPlacement, crate::registry::RegistryError> {
        let candidates = planning.cluster_nodes().iter().cloned().collect();
        let judgement = graph.judge_node_rules(
            domain,
            schedule,
            planning.records(),
            &candidates,
            ScheduleActivity::Stopped,
        )?;
        let mut waiting = Vec::new();
        let mut misplaced = BTreeSet::new();
        for report in &judgement.units {
            match &report.state {
                UnitRuleState::WaitingFor(_) => waiting.push(report.unit.clone()),
                UnitRuleState::Violates(_) => {
                    misplaced.insert(report.unit.clone());
                }
                UnitRuleState::Satisfied | UnitRuleState::Unplaced => {}
            }
        }
        if !waiting.is_empty() {
            return Ok(StartPlacement::Waiting { units: waiting });
        }
        if misplaced.is_empty() {
            return Ok(StartPlacement::Placed);
        }
        Ok(StartPlacement::Misplaced { units: misplaced })
    }

    /// Moves `units`, and the hard colocation groups they belong to, to the nodes the scheduler
    /// chooses under the node rules, through the planned ownership handoff that carries each
    /// unit's newest checkpoint from the node it leaves.
    async fn move_misplaced_units(
        &self,
        domain: &DomainName,
        graph: &ActiveGraph,
        current: DomainSchedule,
        inputs: &DomainPlanningInputs,
        planning: &DomainSchedulePlanningSnapshot,
        units: &BTreeSet<NodeUnitRef>,
    ) -> error_stack::Result<StartPlacementMove, StartPlacementError> {
        let moving = || StartPlacementError::Move {
            domain: domain.clone(),
        };
        let desired = self
            .start_placement_desired_schedule(domain, graph, &current, planning)
            .change_context_lazy(moving)?;
        let mut planned = start_placement_schedule(&current, &desired, units);
        if planned == current {
            return Ok(StartPlacementMove::Unchanged);
        }
        mark_complete_ownership_transitions(Some(&current), &mut planned);
        let handoff = match self
            .begin_planned_ownership_handoff(domain, Some(&current), Some(&planned))
            .await
        {
            Ok(handoff) => handoff,
            Err(error) => return Ok(StartPlacementMove::Deferred(error.change_context(moving()))),
        };
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_start_placement_if_armed(
                self.inner.consensus.local_node_id(),
                domain,
                crate::fault_injection::StartPlacementBoundary::Moving,
            )
            .await;
        let validated = match self.validate_domain_planning_inputs(inputs).await {
            Ok(()) => planning.validate_eligibility(self).await,
            Err(error) => Err(error),
        };
        if let Err(error) = validated {
            if let Some(handoff) = handoff {
                self.abort_planned_ownership_handoff(domain, handoff, None)
                    .await;
            }
            return Ok(StartPlacementMove::Deferred(error.change_context(moving())));
        }
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_start_placement_if_armed(
                self.inner.consensus.local_node_id(),
                domain,
                crate::fault_injection::StartPlacementBoundary::Publishing,
            )
            .await;
        let lease = self.inner.consensus.current_domain_mutation(domain).await;
        if let Err(error) = self
            .inner
            .consensus
            .replace_domain_schedule(inputs.clone(), Some(planned), lease.as_ref())
            .await
        {
            if let Some(handoff) = handoff {
                self.abort_planned_ownership_handoff(domain, handoff, None)
                    .await;
            }
            return Ok(StartPlacementMove::Deferred(error.change_context(moving())));
        }
        info!(
            domain = domain.as_str(),
            units = %super::describe_output::format_placement_unit_nodes(
                &units.iter().cloned().collect::<Vec<_>>()
            ),
            "START moved units to nodes their node rules admit"
        );
        let activation = self.apply_current_cluster_state().await;
        if let Some(handoff) = handoff {
            match &activation {
                Ok(()) => self
                    .finish_planned_ownership_handoff(domain, handoff, None)
                    .await
                    .change_context_lazy(moving)?,
                Err(error) => {
                    self.hold_planned_ownership_handoff_until_applied(domain, handoff, error, None);
                }
            }
        }
        activation.change_context_lazy(moving)?;
        Ok(StartPlacementMove::Moved)
    }

    /// The schedule the scheduler chooses for the stopped domain today under its node rules.
    fn start_placement_desired_schedule(
        &self,
        domain: &DomainName,
        graph: &ActiveGraph,
        current: &DomainSchedule,
        planning: &DomainSchedulePlanningSnapshot,
    ) -> error_stack::Result<DomainSchedule, crate::registry::RegistryError> {
        #[cfg(feature = "testing")]
        {
            graph.schedule_for_domain_with_mode(
                domain,
                planning.cluster_nodes(),
                planning.replica_count(),
                Some(current),
                self.inner.runtime.scheduler_mode(),
                planning.records(),
            )
        }
        #[cfg(not(feature = "testing"))]
        {
            graph.schedule_for_domain(
                domain,
                planning.cluster_nodes(),
                planning.replica_count(),
                Some(current),
                planning.records(),
            )
        }
    }
}

/// `current` with every unit of `units`, and every member of a hard colocation group one of them
/// belongs to, placed as `desired` places it. Every other unit keeps its committed assignment.
fn start_placement_schedule(
    current: &DomainSchedule,
    desired: &DomainSchedule,
    units: &BTreeSet<NodeUnitRef>,
) -> DomainSchedule {
    let mut moving = units.clone();
    for group in &current.placement_groups {
        if group.members.iter().any(|member| units.contains(member)) {
            moving.extend(group.members.iter().cloned());
        }
    }
    let mut planned = current.clone();
    for member in &moving {
        let Some(desired_unit) = desired
            .nodes
            .get(&member.node)
            .and_then(|node| node.unit(member.unit))
        else {
            continue;
        };
        if desired_unit.assigned_nodes.is_empty() {
            continue;
        }
        let Some(node) = planned.nodes.get_mut(&member.node) else {
            continue;
        };
        node.place_unit(
            member.unit,
            desired_unit.primary_node.clone(),
            desired_unit.assigned_nodes.clone(),
        )
        .then_some(())
        .verified("the desired schedule places the units the committed one schedules");
    }
    let mut primaries = Vec::with_capacity(planned.placement_groups.len());
    for group in &planned.placement_groups {
        let mut primary = None;
        if let Some(member) = group.members.first()
            && let Some(node) = planned.nodes.get(&member.node)
            && let Some(unit) = node.unit(member.unit)
        {
            primary.clone_from(&unit.primary_node);
        }
        primaries.push(primary);
    }
    for (group, primary) in planned.placement_groups.iter_mut().zip(primaries) {
        group.primary_node = primary;
    }
    planned
}

#[cfg(test)]
mod tests;
