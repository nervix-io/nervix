//! `RELOCATE` and `DESCRIBE RELOCATION`.
//!
//! Both statements compute the same plan: the unit of hard groups the statement moves, the owner
//! and replicas each member gets, the quiesce level, the relays a hold would gate, and the
//! preferences the move leaves unsatisfied. `DESCRIBE RELOCATION` returns that plan; `RELOCATE`
//! executes it as one gated handoff and returns it with the executed outcome.

use std::collections::BTreeSet;

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_consensus::{DomainMutationLease, DomainPlanningInputs};
use nervix_models::{
    ClusterNodeName, DomainName, DomainSchedule, DomainStatus, Model, NodeRef, PlacementPolicy,
    QuiesceLevel, RelayName, Relocation, RelocationPreferenceStrategy, ScheduledNode,
};

use super::{
    command_result::CommandResult,
    describe_output::{
        format_millis_duration, format_placement_runtime_node, placement_claim_owner,
    },
    domain_lifecycle::DomainAlterError,
    model_mutation::{command_error, command_ok, quiesce_level_message},
    ownership_handoff::mark_complete_ownership_transitions,
    schedule_planning::DomainSchedulePlanningSnapshot,
    session_service::SessionServiceImpl,
};
use crate::registry::{
    ActiveGraph, RelocationCoverage, RelocationMemberReason, RelocationPlanError, RelocationUnit,
    ownership_handoff_relays_for_schedule,
};

#[derive(Debug, thiserror::Error)]
enum RelocationError {
    #[error("domain '{domain}' does not exist")]
    DomainNotFound { domain: DomainName },
    #[error("domain '{domain}' is paused by a model alteration")]
    DomainPaused { domain: DomainName },
    #[error("domain '{domain}' has no active schedule")]
    NoActiveSchedule { domain: DomainName },
    #[error("relocation plan failed: {source}")]
    Graph { source: RelocationPlanError },
    #[error("node '{node}' is not a raft member")]
    DestinationNotMember { node: ClusterNodeName },
    #[error("node '{node}' is not a live raft voter")]
    DestinationNotLiveVoter { node: ClusterNodeName },
    #[error("node '{node}' is terminating")]
    DestinationTerminating { node: ClusterNodeName },
    #[error("node '{node}' is cordoned")]
    DestinationCordoned { node: ClusterNodeName },
    #[error("{kind} '{name}' is not scheduled in domain '{domain}'", kind = .member.kind.as_str(), name = .member.identifier.as_str())]
    MemberNotScheduled { domain: DomainName, member: NodeRef },
    #[error("{kind} '{name}' has no owner in domain '{domain}'", kind = .member.kind.as_str(), name = .member.identifier.as_str())]
    MemberWithoutOwner { domain: DomainName, member: NodeRef },
    #[error("{kind} '{name}' is owned by unavailable node '{owner}'; relocate it after failover reassigns it", kind = .member.kind.as_str(), name = .member.identifier.as_str())]
    MemberOwnerUnavailable {
        member: NodeRef,
        owner: ClusterNodeName,
    },
}

/// One unit member with the assignment the relocation gives it.
struct RelocationPlanMember {
    runtime_node: NodeRef,
    group: usize,
    strategy: RelocationPreferenceStrategy,
    reason: RelocationMemberReason,
    owner: ClusterNodeName,
    moves: bool,
    replicas: Vec<ClusterNodeName>,
    promoted_replica: bool,
}

/// The plan `DESCRIBE RELOCATION` shows and `RELOCATE` executes.
struct RelocationPlan {
    inputs: DomainPlanningInputs,
    planning: DomainSchedulePlanningSnapshot,
    destination: ClusterNodeName,
    level: QuiesceLevel,
    gated_relays: Vec<RelayName>,
    coverage: Vec<RelocationCoverage>,
    members: Vec<RelocationPlanMember>,
    unsatisfied: Vec<String>,
    /// The schedule to commit, absent when the plan moves nothing.
    schedule: Option<DomainSchedule>,
}

impl RelocationPlan {
    fn moved_count(&self) -> usize {
        self.members.iter().filter(|member| member.moves).count()
    }

    /// The block both statements print, identical for the same plan.
    fn render(&self) -> String {
        let mut lines = vec![
            format!("relocation onto node '{}'", self.destination),
            quiesce_level_message(self.level),
            format!(
                "gated relays: {}",
                if self.gated_relays.is_empty() {
                    "-".to_string()
                } else {
                    self.gated_relays
                        .iter()
                        .map(|name| name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            ),
        ];
        if !self.coverage.is_empty() {
            lines.push("coverage:".to_string());
            for pair in &self.coverage {
                lines.push(format!(
                    "- {} {} -> {} {} connected={} covered={}",
                    pair.source.kind.as_str(),
                    pair.source.identifier.as_str(),
                    pair.destination.kind.as_str(),
                    pair.destination.identifier.as_str(),
                    if pair.connected { "yes" } else { "no" },
                    pair.covered
                ));
            }
        }
        lines.push("unit:".to_string());
        for member in &self.members {
            let mut line = format!(
                "- kind={} name={} group={} strategy={} reason={} owner={} moves={}",
                member.runtime_node.kind.as_str(),
                member.runtime_node.identifier.as_str(),
                member.group,
                member.strategy.as_ref(),
                member.reason.as_ref(),
                member.owner,
                if member.moves { "yes" } else { "no" }
            );
            line.push_str(&format!(" replicas={}", format_node_list(&member.replicas)));
            if member.moves {
                line.push_str(&format!(
                    " promoted_replica={}",
                    if member.promoted_replica { "yes" } else { "no" }
                ));
            }
            lines.push(line);
        }
        lines.push(format!(
            "unsatisfied preferences: {}",
            self.unsatisfied.len()
        ));
        lines.extend(self.unsatisfied.iter().cloned());
        lines.join("\n")
    }
}

impl SessionServiceImpl {
    pub(super) async fn describe_relocation(
        &self,
        domain: &DomainName,
        relocation: Relocation,
    ) -> CommandResult {
        match self.plan_relocation(domain, &relocation).await {
            Ok(plan) => command_ok(plan.render()),
            Err(error) => command_error(format!("{error:#}")),
        }
    }

    pub(super) async fn relocate(
        &self,
        domain: &DomainName,
        relocation: Relocation,
        mutation: Option<&DomainMutationLease>,
    ) -> CommandResult {
        let Some(_alter_guard) = self.inner.runtime.try_begin_domain_alter(domain) else {
            return command_error(
                DomainAlterError::ConcurrentAlter {
                    domain: domain.clone(),
                }
                .to_string(),
            );
        };

        if let Err(error) = self.apply_current_cluster_state().await {
            return command_error(format!(
                "failed to prepare the current runtime schedule for relocation in domain '{}': \
                 {error}",
                domain.as_str()
            ));
        }

        let plan = match self.plan_relocation(domain, &relocation).await {
            Ok(plan) => plan,
            Err(error) => return command_error(format!("{error:#}")),
        };
        let total = plan.members.len();
        let moved = plan.moved_count();
        let Some(mut planned_schedule) = plan.schedule.clone() else {
            return command_ok(format!(
                "relocated 0 of {total} runtime node(s) onto node '{}'\n{}",
                plan.destination,
                plan.render()
            ));
        };

        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_relocation_publication_if_armed(domain)
            .await;
        if let Err(error) = self.validate_domain_planning_inputs(&plan.inputs).await {
            return command_error(format!(
                "failed to commit the relocation onto node '{}' for domain '{}': {error}",
                plan.destination,
                domain.as_str()
            ));
        }
        if let Err(error) = plan.planning.validate_eligibility(self).await {
            return command_error(format!(
                "failed to commit the relocation onto node '{}' for domain '{}': {error}",
                plan.destination,
                domain.as_str()
            ));
        }
        let current_domain_schedule = plan.inputs.schedule().cloned();
        mark_complete_ownership_transitions(
            current_domain_schedule.as_ref(),
            &mut planned_schedule,
        );
        let mut handoff = if moved > 0 {
            match self
                .begin_planned_ownership_handoff(
                    domain,
                    current_domain_schedule.as_ref(),
                    Some(&planned_schedule),
                )
                .await
            {
                Ok(handoff) => handoff,
                Err(error) => return command_error(error.to_string()),
            }
        } else {
            None
        };
        if let Err(error) = self
            .inner
            .consensus
            .replace_domain_schedule(plan.inputs.clone(), Some(planned_schedule), mutation)
            .await
        {
            if let Some(handoff) = handoff.take() {
                self.abort_planned_ownership_handoff(domain, handoff, None)
                    .await;
            }
            return command_error(format!(
                "failed to commit the relocation onto node '{}' for domain '{}': {error}",
                plan.destination,
                domain.as_str()
            ));
        }

        let local_activation_error = self.apply_current_cluster_state().await.err();
        // The hold spans planning through release, which is what the operator waited for.
        let hold_duration = handoff.as_ref().map(|handoff| handoff.started_at.elapsed());
        let mut handoff_activation_error = None;
        if let Some(handoff) = handoff {
            if let Some(error) = &local_activation_error {
                self.defer_planned_ownership_handoff_release(domain, handoff, error, None);
            } else if let Err(error) = self
                .finish_planned_ownership_handoff(domain, handoff, None)
                .await
            {
                handoff_activation_error = Some(error);
            }
        }

        let mut message = format!(
            "relocated {moved} of {total} runtime node(s) onto node '{}'\n{}",
            plan.destination,
            plan.render()
        );
        if let Some(hold_duration) = hold_duration {
            message.push_str(&format!(
                "\nhold duration: {}",
                format_millis_duration(
                    u64::try_from(hold_duration.as_millis()).unwrap_or(u64::MAX)
                )
            ));
        }
        if let Some(error) = local_activation_error {
            return command_error(format!(
                "relocated {moved} runtime node(s) onto node '{}', but failed to activate the \
                 updated schedule for domain '{}': {error}",
                plan.destination,
                domain.as_str()
            ));
        }
        if let Some(error) = handoff_activation_error {
            return command_error(format!(
                "relocated {moved} runtime node(s) onto node '{}', but ownership state activation \
                 did not complete for domain '{}': {error}",
                plan.destination,
                domain.as_str()
            ));
        }
        command_ok(message)
    }

    /// Computes the plan from the currently active graph, placement plan, schedule, and cluster
    /// state. `RELOCATE` recomputes it under the domain alteration lock before executing.
    async fn plan_relocation(
        &self,
        domain: &DomainName,
        relocation: &Relocation,
    ) -> error_stack::Result<RelocationPlan, RelocationError> {
        let inputs = self.inner.consensus.domain_planning_inputs(domain).await;
        let Some(domain_state) = inputs.state() else {
            return Err(Report::new(RelocationError::DomainNotFound {
                domain: domain.clone(),
            }));
        };
        if let DomainStatus::Paused = domain_state.status {
            return Err(Report::new(RelocationError::DomainPaused {
                domain: domain.clone(),
            }));
        }
        let Some(graph) = self.inner.registry.active_graph(domain) else {
            return Err(Report::new(RelocationError::NoActiveSchedule {
                domain: domain.clone(),
            }));
        };
        let Some(current) = inputs.schedule() else {
            return Err(Report::new(RelocationError::NoActiveSchedule {
                domain: domain.clone(),
            }));
        };

        let unit = graph
            .relocation_unit(
                domain,
                domain_state.config.placement,
                &relocation.selection,
                relocation.strategy,
                &relocation.overrides,
            )
            .map_err(|source| Report::new(RelocationError::Graph { source }))?;

        // Failover reassigns from the same liveness signal, so a relocation must read it the same
        // way or it would plan a handoff from an owner failover is already taking over.
        let planning = self
            .capture_domain_schedule_planning_snapshot(&inputs)
            .await;
        let live_nodes = planning.live_node_ids();
        let placement_candidate_nodes = planning.placement_candidate_node_ids();
        let schedulable_nodes = planning
            .cluster_nodes()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        let mut owners = Vec::with_capacity(unit.members.len());
        for member in &unit.members {
            owners.push(
                relocation_member_owner(domain, current, &member.runtime_node, &live_nodes)?
                    .clone(),
            );
        }

        Self::validate_relocation_destination(
            &relocation.destination,
            &inputs,
            &live_nodes,
            &placement_candidate_nodes,
        )?;

        let moved = unit
            .members
            .iter()
            .zip(&owners)
            .filter(|(_, owner)| **owner != relocation.destination)
            .map(|(member, _)| member.runtime_node.clone())
            .collect::<Vec<_>>();

        let desired = self.desired_domain_schedule(
            domain,
            &graph,
            &schedulable_nodes,
            domain_state.config.placement,
        );
        let planned = (!moved.is_empty()).then(|| {
            planned_relocation_schedule(
                current,
                &desired,
                &relocation.destination,
                &moved,
                self.inner.replica_count,
                &schedulable_nodes,
                &live_nodes,
            )
        });

        let domain_running = matches!(domain_state.status, DomainStatus::Running);
        let level = if planned.is_some() && domain_running {
            QuiesceLevel::EntityPause
        } else {
            QuiesceLevel::Dynamic
        };
        let gated_relays = if let QuiesceLevel::EntityPause = level {
            ownership_handoff_relays_for_schedule(current, &moved)
        } else {
            Vec::new()
        };

        let members = unit
            .members
            .iter()
            .zip(&owners)
            .map(|(member, owner)| {
                let moves = *owner != relocation.destination;
                let mut assignment = None;
                if moves && let Some(planned) = planned.as_ref() {
                    assignment = planned.nodes.get(&member.runtime_node);
                }
                if assignment.is_none() {
                    assignment = current.nodes.get(&member.runtime_node);
                }
                RelocationPlanMember {
                    runtime_node: member.runtime_node.clone(),
                    group: member.group,
                    strategy: member.strategy,
                    reason: member.reason,
                    owner: owner.clone(),
                    moves,
                    replicas: match assignment {
                        Some(node) => node.replica_nodes().into_iter().cloned().collect(),
                        None => Vec::new(),
                    },
                    promoted_replica: moves
                        && current
                            .nodes
                            .get(&member.runtime_node)
                            .is_some_and(|node| node.is_assigned_to(&relocation.destination)),
                }
            })
            .collect::<Vec<_>>();

        let unsatisfied = unsatisfied_preference_lines(
            &unit,
            planned.as_ref().unwrap_or(current),
            &members
                .iter()
                .map(|member| member.runtime_node.clone())
                .collect::<Vec<_>>(),
        );

        Ok(RelocationPlan {
            inputs,
            planning,
            destination: relocation.destination.clone(),
            level,
            gated_relays,
            coverage: unit.coverage,
            members,
            unsatisfied,
            schedule: planned,
        })
    }

    /// The destination must be a cluster node the scheduler could choose for a new assignment.
    fn validate_relocation_destination(
        destination: &ClusterNodeName,
        inputs: &DomainPlanningInputs,
        live_nodes: &BTreeSet<ClusterNodeName>,
        placement_candidate_nodes: &BTreeSet<ClusterNodeName>,
    ) -> error_stack::Result<(), RelocationError> {
        if !inputs.topology().members().contains(destination) {
            return Err(Report::new(RelocationError::DestinationNotMember {
                node: destination.clone(),
            }));
        }
        if !inputs.topology().voters().contains(destination) || !live_nodes.contains(destination) {
            return Err(Report::new(RelocationError::DestinationNotLiveVoter {
                node: destination.clone(),
            }));
        }
        if !placement_candidate_nodes.contains(destination) {
            return Err(Report::new(RelocationError::DestinationTerminating {
                node: destination.clone(),
            }));
        }
        if inputs.topology().cordoned().contains(destination) {
            return Err(Report::new(RelocationError::DestinationCordoned {
                node: destination.clone(),
            }));
        }
        Ok(())
    }

    /// The assignment the scheduler would choose today, used to fill replica slots the former
    /// owner and existing replicas leave open.
    fn desired_domain_schedule(
        &self,
        domain: &DomainName,
        graph: &ActiveGraph,
        schedulable_nodes: &BTreeSet<ClusterNodeName>,
        placement: PlacementPolicy,
    ) -> DomainSchedule {
        let cluster_nodes = schedulable_nodes.iter().cloned().collect::<Vec<_>>();
        #[cfg(feature = "testing")]
        {
            graph.schedule_for_domain_with_mode(
                domain,
                &cluster_nodes,
                self.inner.replica_count,
                placement,
                self.inner.runtime.scheduler_mode(),
            )
        }
        #[cfg(not(feature = "testing"))]
        {
            graph.schedule_for_domain(domain, &cluster_nodes, self.inner.replica_count, placement)
        }
    }
}

/// Rewrites the assignments of every moved member onto the destination, leaving every other
/// runtime node untouched.
fn planned_relocation_schedule(
    current: &DomainSchedule,
    desired: &DomainSchedule,
    destination: &ClusterNodeName,
    moved: &[NodeRef],
    replica_count: usize,
    schedulable_nodes: &BTreeSet<ClusterNodeName>,
    live_nodes: &BTreeSet<ClusterNodeName>,
) -> DomainSchedule {
    let mut planned = current.clone();
    for member in moved {
        let Some(node) = planned.nodes.get_mut(member) else {
            continue;
        };
        let former_owner = node.execution_node().cloned();
        let replica_slots = if relay_without_materialized_state(node) {
            0
        } else {
            replica_count
        };
        let mut candidates = Vec::new();
        if let Some(former_owner) = former_owner
            && schedulable_nodes.contains(&former_owner)
        {
            candidates.push(former_owner);
        }
        candidates.extend(
            node.replica_nodes()
                .into_iter()
                .filter(|replica| live_nodes.contains(*replica))
                .cloned(),
        );
        if let Some(desired_node) = desired.nodes.get(member) {
            candidates.extend(
                desired_node
                    .assigned_nodes
                    .iter()
                    .filter(|node_id| schedulable_nodes.contains(*node_id))
                    .cloned(),
            );
        }

        let mut assigned_nodes = vec![destination.clone()];
        for candidate in candidates {
            if assigned_nodes.len() > replica_slots {
                break;
            }
            if !assigned_nodes.contains(&candidate) {
                assigned_nodes.push(candidate);
            }
        }
        // `replica_slots` is an operator-supplied replica count with no upper bound, so it is
        // clamped to what the loop above could have collected before the primary slot is added.
        assigned_nodes.truncate(
            replica_slots
                .min(assigned_nodes.len())
                .checked_add(1)
                .assured("a slot count clamped to a collected list leaves room for the primary"),
        );
        node.primary_node = Some(destination.clone());
        node.assigned_nodes = assigned_nodes;
    }

    for group in &mut planned.placement_groups {
        if group
            .members
            .iter()
            .any(|group_member| moved.contains(group_member))
        {
            group.primary_node = Some(destination.clone());
        }
    }
    planned
}

/// Every soft preference touching the unit whose owners after the move disagree with its policy.
fn unsatisfied_preference_lines(
    unit: &RelocationUnit,
    schedule: &DomainSchedule,
    context: &[NodeRef],
) -> Vec<String> {
    let mut context = context.to_vec();
    for preference in &unit.preferences {
        for node in [&preference.left, &preference.right] {
            if !context.contains(node) {
                context.push(node.clone());
            }
        }
    }
    let mut lines = Vec::new();
    for preference in &unit.preferences {
        let Some(left_node) = schedule.nodes.get(&preference.left) else {
            continue;
        };
        let Some(left) = left_node.execution_node() else {
            continue;
        };
        let Some(right_node) = schedule.nodes.get(&preference.right) else {
            continue;
        };
        let Some(right) = right_node.execution_node() else {
            continue;
        };
        let unsatisfied = match preference.policy {
            PlacementPolicy::PreferColocation => left != right,
            PlacementPolicy::SuggestSeparation => left == right,
            PlacementPolicy::RequireColocation | PlacementPolicy::Neutral => false,
        };
        if !unsatisfied {
            continue;
        }
        lines.push(format!(
            "- {} {} <-> {} ({})",
            preference.policy.as_ref().to_lowercase(),
            format_placement_runtime_node(&preference.left, &context),
            format_placement_runtime_node(&preference.right, &context),
            placement_claim_owner(&preference.winning_rules)
        ));
    }
    lines
}

/// The owner a unit member is relocated away from.
///
/// A member whose owner is unavailable cannot be relocated: there is nothing to drain from a dead
/// owner, and failover is already reassigning it.
fn relocation_member_owner<'a>(
    domain: &DomainName,
    schedule: &'a DomainSchedule,
    member: &NodeRef,
    live_nodes: &BTreeSet<ClusterNodeName>,
) -> error_stack::Result<&'a ClusterNodeName, RelocationError> {
    let Some(node) = schedule.nodes.get(member) else {
        return Err(Report::new(RelocationError::MemberNotScheduled {
            domain: domain.clone(),
            member: member.clone(),
        }));
    };
    let Some(owner) = node.execution_node() else {
        return Err(Report::new(RelocationError::MemberWithoutOwner {
            domain: domain.clone(),
            member: member.clone(),
        }));
    };
    if !live_nodes.contains(owner) {
        return Err(Report::new(RelocationError::MemberOwnerUnavailable {
            member: member.clone(),
            owner: owner.clone(),
        }));
    }
    Ok(owner)
}

fn relay_without_materialized_state(node: &ScheduledNode) -> bool {
    matches!(node.config.as_ref(), Model::Relay(relay) if relay.materialized_state.is_none())
}

fn format_node_list(nodes: &[ClusterNodeName]) -> String {
    if nodes.is_empty() {
        "-".to_string()
    } else {
        nodes
            .iter()
            .map(|node| node.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{CreateJunction, JunctionName, ModelKind, ModelName, SchemaFingerprint};

    use super::*;

    fn junction_node(name: &str, primary: &str, replicas: &[&str]) -> ScheduledNode {
        let identifier = ModelName::try_from(name).expect("test name must be an identifier");
        let mut assigned_nodes = vec![node_name(primary)];
        assigned_nodes.extend(replicas.iter().map(|node| node_name(node)));
        ScheduledNode::new(
            Model::Junction(CreateJunction {
                name: JunctionName::from(&identifier),
                from: nervix_models::ProcessorInputs::new(Vec::new(), Vec::new()),
                output_routes: nervix_models::ProcessorOutputs::new(Vec::new()),
                branched_by: nervix_models::BranchSelection::unbranched(),
                mode: Default::default(),
                filter_where: None,
                materialized_state: Vec::new(),
            }),
            SchemaFingerprint::from_digest([1; 32]),
        )
        .placed_on(
            Some(ClusterNodeName::parse(primary).expect("valid node name")),
            assigned_nodes,
        )
    }

    fn schedule(nodes: Vec<ScheduledNode>) -> DomainSchedule {
        DomainSchedule::new(
            DomainName::parse("relocation_test").expect("valid domain"),
            nodes,
            Vec::new(),
        )
    }

    fn member(name: &str) -> NodeRef {
        NodeRef::new(
            ModelKind::Junction,
            ModelName::try_from(name).expect("test name must be a model name"),
        )
    }

    fn node_name(raw: &str) -> ClusterNodeName {
        ClusterNodeName::parse(raw).expect("valid node name")
    }

    fn live(nodes: &[&str]) -> BTreeSet<ClusterNodeName> {
        nodes
            .iter()
            .map(|node| ClusterNodeName::parse(node).expect("valid node name"))
            .collect()
    }

    #[test]
    fn resolves_a_live_owner() {
        let schedule = schedule(vec![junction_node("route", "node-1", &["node-2"])]);
        let domain = schedule.domain.clone();
        assert_eq!(
            relocation_member_owner(&domain, &schedule, &member("route"), &live(&["node-1"]))
                .expect("a live owner must resolve"),
            &node_name("node-1")
        );
    }

    #[test]
    fn rejects_a_member_whose_owner_is_unavailable() {
        let schedule = schedule(vec![junction_node("route", "node-3", &[])]);
        let domain = schedule.domain.clone();
        let error = relocation_member_owner(
            &domain,
            &schedule,
            &member("route"),
            &live(&["node-1", "node-2"]),
        )
        .expect_err("an unavailable owner must be rejected");
        let expected_member = member("route");
        assert!(matches!(
            error.current_context(),
            RelocationError::MemberOwnerUnavailable { member, owner }
                if member == &expected_member && owner == &node_name("node-3")
        ));
    }

    #[test]
    fn rejects_a_member_that_is_not_scheduled() {
        let schedule = schedule(Vec::new());
        let domain = schedule.domain.clone();
        let error =
            relocation_member_owner(&domain, &schedule, &member("route"), &live(&["node-1"]))
                .expect_err("an unscheduled member must be rejected");
        let expected_member = member("route");
        assert!(matches!(
            error.current_context(),
            RelocationError::MemberNotScheduled { domain, member }
                if domain == &DomainName::parse("relocation_test").expect("valid domain")
                    && member == &expected_member
        ));
    }

    #[test]
    fn relocation_schedule_puts_the_former_owner_first_among_replicas() {
        let current = schedule(vec![junction_node("route", "node-1", &["node-3"])]);
        let desired = schedule(vec![junction_node("route", "node-2", &["node-3"])]);
        let planned = planned_relocation_schedule(
            &current,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &[member("route")],
            1,
            &live(&["node-1", "node-2", "node-3"]),
            &live(&["node-1", "node-2", "node-3"]),
        );
        let node = planned
            .nodes
            .get(&member("route"))
            .expect("member must remain scheduled");
        assert_eq!(node.primary_node.as_ref(), Some(&node_name("node-2")));
        assert_eq!(
            node.assigned_nodes,
            vec![node_name("node-2"), node_name("node-1")]
        );
    }

    #[test]
    fn a_replica_count_of_zero_leaves_the_former_owner_behind() {
        let current = schedule(vec![junction_node("route", "node-1", &[])]);
        let desired = schedule(vec![junction_node("route", "node-2", &[])]);
        let planned = planned_relocation_schedule(
            &current,
            &desired,
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &[member("route")],
            0,
            &live(&["node-1", "node-2"]),
            &live(&["node-1", "node-2"]),
        );
        let node = planned
            .nodes
            .get(&member("route"))
            .expect("member must remain scheduled");
        assert_eq!(node.assigned_nodes, vec![node_name("node-2")]);
    }
}
