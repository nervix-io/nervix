//! `RELOCATE` and `DESCRIBE RELOCATION`.
//!
//! Both statements compute the same plan: the unit of hard groups the statement moves, the owner
//! and replicas each member gets, the quiesce level, the relays a hold would gate, and the
//! preferences the move leaves unsatisfied. `DESCRIBE RELOCATION` returns that plan; `RELOCATE`
//! executes it as one gated handoff and returns it with the executed outcome.

use std::collections::BTreeSet;

use nervix_models::{
    Domain, DomainSchedule, DomainStatus, Identifier, Model, PlacementPolicy, PlacementRuntimeNode,
    QuiesceLevel, Relocation, RelocationPreferenceStrategy, ScheduledNode,
};

use super::{
    CommandResult, DomainAlterError, SessionServiceImpl, command_error, command_ok,
    format_millis_duration, format_placement_runtime_node, placement_claim_owner,
    quiesce_level_message,
};
use crate::{
    registry::{ActiveGraph, RelocationCoverage, RelocationMemberReason, RelocationUnit},
    runtime::Runtime,
};

/// One unit member with the assignment the relocation gives it.
struct RelocationPlanMember {
    runtime_node: PlacementRuntimeNode,
    group: usize,
    strategy: RelocationPreferenceStrategy,
    reason: RelocationMemberReason,
    owner: String,
    moves: bool,
    replicas: Vec<String>,
    promoted_replica: bool,
}

/// The plan `DESCRIBE RELOCATION` shows and `RELOCATE` executes.
struct RelocationPlan {
    destination: String,
    level: QuiesceLevel,
    gated_relays: Vec<Identifier>,
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
                        .map(Identifier::as_str)
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
        domain: &Domain,
        relocation: Relocation,
    ) -> CommandResult {
        match self.plan_relocation(domain, &relocation).await {
            Ok(plan) => command_ok(plan.render()),
            Err(message) => command_error(message),
        }
    }

    pub(super) async fn relocate(&self, domain: &Domain, relocation: Relocation) -> CommandResult {
        let Some(_alter_guard) = self.runtime.try_begin_domain_alter(domain) else {
            return command_error(
                DomainAlterError::ConcurrentAlter {
                    domain: domain.clone(),
                }
                .to_string(),
            );
        };

        let plan = match self.plan_relocation(domain, &relocation).await {
            Ok(plan) => plan,
            Err(message) => return command_error(message),
        };
        let total = plan.members.len();
        let moved = plan.moved_count();
        let Some(planned_schedule) = plan.schedule.clone() else {
            return command_ok(format!(
                "relocated 0 of {total} runtime node(s) onto node '{}'\n{}",
                plan.destination,
                plan.render()
            ));
        };

        let current_schedule = self.consensus.current_schedule().await;
        let current_domain_schedule = current_schedule.domain(domain);
        let mut handoff = if let QuiesceLevel::EntityPause = plan.level {
            match self
                .begin_planned_ownership_handoff(
                    domain,
                    current_domain_schedule,
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
            .consensus
            .replace_domain_schedule(domain.clone(), Some(planned_schedule))
            .await
        {
            if let Some(handoff) = handoff.take() {
                self.release_cluster_entity_gates(handoff.gate).await;
            }
            return command_error(format!(
                "failed to commit the relocation onto node '{}' for domain '{}': {error}",
                plan.destination,
                domain.as_str()
            ));
        }

        let activation_error = self.apply_current_cluster_state().await.err();
        // The hold spans planning through release, which is what the operator waited for.
        let hold_duration = handoff.as_ref().map(|handoff| handoff.started_at.elapsed());
        if let Some(handoff) = handoff {
            if let Some(error) = &activation_error {
                self.defer_planned_ownership_handoff_release(domain, handoff, error);
            } else {
                self.finish_planned_ownership_handoff(domain, handoff).await;
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
        if let Some(error) = activation_error {
            return command_error(format!(
                "relocated {moved} runtime node(s) onto node '{}', but failed to activate the \
                 updated schedule for domain '{}': {error}",
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
        domain: &Domain,
        relocation: &Relocation,
    ) -> Result<RelocationPlan, String> {
        let Some(domain_state) = self.consensus.current_domain(domain).await else {
            return Err(format!("domain '{}' does not exist", domain.as_str()));
        };
        if let DomainStatus::Paused = domain_state.status {
            return Err(format!(
                "domain '{}' is paused by a model alteration",
                domain.as_str()
            ));
        }
        let Some(graph) = self.registry.active_graph(domain) else {
            return Err(format!(
                "domain '{}' has no active schedule",
                domain.as_str()
            ));
        };
        let cluster_schedule = self.consensus.current_schedule().await;
        let Some(current) = cluster_schedule.domain(domain) else {
            return Err(format!(
                "domain '{}' has no active schedule",
                domain.as_str()
            ));
        };

        let unit = graph
            .relocation_unit(
                domain,
                domain_state.config.placement,
                &relocation.selection,
                relocation.strategy,
                &relocation.overrides,
            )
            .map_err(|error| error.to_string())?;

        // Failover reassigns from the same liveness signal, so a relocation must read it the same
        // way or it would plan a handoff from an owner failover is already taking over.
        let live_nodes = self
            .available_node_ids()
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();
        let schedulable_nodes = self
            .consensus
            .schedulable_live_voter_ids(live_nodes.iter().cloned())
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();

        let mut owners = Vec::with_capacity(unit.members.len());
        for member in &unit.members {
            owners.push(
                relocation_member_owner(domain, current, &member.runtime_node, &live_nodes)?
                    .to_string(),
            );
        }

        self.validate_relocation_destination(&relocation.destination, &live_nodes)
            .await?;

        let moved = unit
            .members
            .iter()
            .zip(&owners)
            .filter(|(_, owner)| owner.as_str() != relocation.destination)
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
                self.replica_count,
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
            let affected = moved
                .iter()
                .map(|node| crate::registry::RegistryEntity {
                    kind: node.kind,
                    identifier: node.identifier.clone(),
                })
                .collect::<Vec<_>>();
            Runtime::ownership_handoff_relays_for_schedule(current, &affected)
        } else {
            Vec::new()
        };

        let members = unit
            .members
            .iter()
            .zip(&owners)
            .map(|(member, owner)| {
                let moves = owner.as_str() != relocation.destination;
                let assignment = planned
                    .as_ref()
                    .filter(|_| moves)
                    .and_then(|planned| scheduled_node(planned, &member.runtime_node))
                    .or_else(|| scheduled_node(current, &member.runtime_node));
                RelocationPlanMember {
                    runtime_node: member.runtime_node.clone(),
                    group: member.group,
                    strategy: member.strategy,
                    reason: member.reason,
                    owner: owner.clone(),
                    moves,
                    replicas: assignment
                        .map(|node| {
                            node.replica_nodes()
                                .into_iter()
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default(),
                    promoted_replica: moves
                        && scheduled_node(current, &member.runtime_node)
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
    async fn validate_relocation_destination(
        &self,
        destination: &str,
        live_nodes: &BTreeSet<String>,
    ) -> Result<(), String> {
        let membership = self.consensus.membership_nodes().await;
        if !membership.contains_key(destination) {
            return Err(format!("node '{destination}' is not a raft member"));
        }
        let live_voters = self
            .consensus
            .live_voter_ids(live_nodes.iter().cloned())
            .await;
        if !live_voters.iter().any(|voter| voter == destination) {
            return Err(format!("node '{destination}' is not a live raft voter"));
        }
        if self
            .consensus
            .cordoned_node_ids()
            .await
            .contains(destination)
        {
            return Err(format!("node '{destination}' is cordoned"));
        }
        Ok(())
    }

    /// The assignment the scheduler would choose today, used to fill replica slots the former
    /// owner and existing replicas leave open.
    fn desired_domain_schedule(
        &self,
        domain: &Domain,
        graph: &ActiveGraph,
        schedulable_nodes: &BTreeSet<String>,
        placement: PlacementPolicy,
    ) -> DomainSchedule {
        let cluster_nodes = schedulable_nodes.iter().cloned().collect::<Vec<_>>();
        #[cfg(feature = "testing")]
        {
            graph.schedule_for_domain_with_mode(
                domain,
                &cluster_nodes,
                self.replica_count,
                placement,
                self.scheduler_mode,
            )
        }
        #[cfg(not(feature = "testing"))]
        {
            graph.schedule_for_domain(domain, &cluster_nodes, self.replica_count, placement)
        }
    }
}

/// Rewrites the assignments of every moved member onto the destination, leaving every other
/// runtime node untouched.
fn planned_relocation_schedule(
    current: &DomainSchedule,
    desired: &DomainSchedule,
    destination: &str,
    moved: &[PlacementRuntimeNode],
    replica_count: usize,
    schedulable_nodes: &BTreeSet<String>,
    live_nodes: &BTreeSet<String>,
) -> DomainSchedule {
    let mut planned = current.clone();
    for member in moved {
        let Some(node) = planned
            .nodes
            .iter_mut()
            .find(|node| node.kind == member.kind && node.identifier == member.identifier)
        else {
            continue;
        };
        let former_owner = node.execution_node().map(str::to_string);
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
                .map(str::to_string),
        );
        if let Some(desired_node) = desired.nodes.iter().find(|candidate| {
            candidate.kind == member.kind && candidate.identifier == member.identifier
        }) {
            candidates.extend(
                desired_node
                    .assigned_nodes
                    .iter()
                    .filter(|node_id| schedulable_nodes.contains(*node_id))
                    .cloned(),
            );
        }

        let mut assigned_nodes = vec![destination.to_string()];
        for candidate in candidates {
            if assigned_nodes.len() > replica_slots {
                break;
            }
            if !assigned_nodes.contains(&candidate) {
                assigned_nodes.push(candidate);
            }
        }
        assigned_nodes.truncate(replica_slots.saturating_add(1));
        node.primary_node = Some(destination.to_string());
        node.assigned_nodes = assigned_nodes;
    }

    for group in &mut planned.placement_groups {
        if group
            .members
            .iter()
            .any(|group_member| moved.contains(group_member))
        {
            group.primary_node = Some(destination.to_string());
        }
    }
    planned
}

/// Every soft preference touching the unit whose owners after the move disagree with its policy.
fn unsatisfied_preference_lines(
    unit: &RelocationUnit,
    schedule: &DomainSchedule,
    context: &[PlacementRuntimeNode],
) -> Vec<String> {
    let mut context = context.to_vec();
    for preference in &unit.preferences {
        for node in [&preference.left, &preference.right] {
            if !context.contains(node) {
                context.push(node.clone());
            }
        }
    }
    unit.preferences
        .iter()
        .filter_map(|preference| {
            let left = scheduled_node(schedule, &preference.left)?.execution_node()?;
            let right = scheduled_node(schedule, &preference.right)?.execution_node()?;
            let unsatisfied = match preference.policy {
                PlacementPolicy::PreferColocation => left != right,
                PlacementPolicy::SuggestSeparation => left == right,
                PlacementPolicy::RequireColocation | PlacementPolicy::Neutral => false,
            };
            unsatisfied.then(|| {
                format!(
                    "- {} {} <-> {} ({})",
                    preference.policy.as_ref().to_lowercase(),
                    format_placement_runtime_node(&preference.left, &context),
                    format_placement_runtime_node(&preference.right, &context),
                    placement_claim_owner(&preference.winning_rules)
                )
            })
        })
        .collect()
}

/// The owner a unit member is relocated away from.
///
/// A member whose owner is unavailable cannot be relocated: there is nothing to drain from a dead
/// owner, and failover is already reassigning it.
fn relocation_member_owner<'a>(
    domain: &Domain,
    schedule: &'a DomainSchedule,
    member: &PlacementRuntimeNode,
    live_nodes: &BTreeSet<String>,
) -> Result<&'a str, String> {
    let Some(node) = scheduled_node(schedule, member) else {
        return Err(format!(
            "{} '{}' is not scheduled in domain '{}'",
            member.kind.as_str(),
            member.identifier.as_str(),
            domain.as_str()
        ));
    };
    let Some(owner) = node.execution_node() else {
        return Err(format!(
            "{} '{}' has no owner in domain '{}'",
            member.kind.as_str(),
            member.identifier.as_str(),
            domain.as_str()
        ));
    };
    if !live_nodes.contains(owner) {
        return Err(format!(
            "{} '{}' is owned by unavailable node '{owner}'; relocate it after failover reassigns \
             it",
            member.kind.as_str(),
            member.identifier.as_str()
        ));
    }
    Ok(owner)
}

fn scheduled_node<'a>(
    schedule: &'a DomainSchedule,
    member: &PlacementRuntimeNode,
) -> Option<&'a ScheduledNode> {
    schedule
        .nodes
        .iter()
        .find(|node| node.kind == member.kind && node.identifier == member.identifier)
}

fn relay_without_materialized_state(node: &ScheduledNode) -> bool {
    matches!(node.config.as_ref(), Model::Relay(relay) if relay.materialized_state.is_none())
}

fn format_node_list(nodes: &[String]) -> String {
    if nodes.is_empty() {
        "-".to_string()
    } else {
        nodes.join(",")
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{CreateJunction, Identifier, ModelKind};

    use super::*;

    fn junction_node(name: &str, primary: &str, replicas: &[&str]) -> ScheduledNode {
        let identifier = Identifier::try_from(name).expect("test name must be an identifier");
        let mut assigned_nodes = vec![primary.to_string()];
        assigned_nodes.extend(replicas.iter().map(|node| (*node).to_string()));
        ScheduledNode {
            identifier: identifier.clone(),
            kind: ModelKind::Junction,
            config: Box::new(Model::Junction(CreateJunction {
                name: identifier,
                from: nervix_models::ProcessorInputs::new(Vec::new(), Vec::new()),
                output_routes: nervix_models::ProcessorOutputs::new(Vec::new()),
                branched_by: nervix_models::BranchSelection::unbranched(),
                mode: Default::default(),
                filter_where: None,
                materialized_state: Vec::new(),
            })),
            effective_branching: None,
            effective_branching_schema: None,
            schema_fingerprint: [0; 32],
            kafka_partition_schedule: None,
            primary_node: Some(primary.to_string()),
            assigned_nodes,
        }
    }

    fn schedule(nodes: Vec<ScheduledNode>) -> DomainSchedule {
        DomainSchedule {
            domain: Domain::parse("relocation_test").expect("valid domain"),
            nodes,
            placement_groups: Vec::new(),
        }
    }

    fn member(name: &str) -> PlacementRuntimeNode {
        PlacementRuntimeNode::new(
            ModelKind::Junction,
            Identifier::try_from(name).expect("test name must be an identifier"),
        )
    }

    fn live(nodes: &[&str]) -> BTreeSet<String> {
        nodes.iter().map(|node| (*node).to_string()).collect()
    }

    #[test]
    fn resolves_a_live_owner() {
        let schedule = schedule(vec![junction_node("route", "node-1", &["node-2"])]);
        let domain = schedule.domain.clone();
        assert_eq!(
            relocation_member_owner(&domain, &schedule, &member("route"), &live(&["node-1"]))
                .expect("a live owner must resolve"),
            "node-1"
        );
    }

    #[test]
    fn rejects_a_member_whose_owner_is_unavailable() {
        let schedule = schedule(vec![junction_node("route", "node-3", &[])]);
        let domain = schedule.domain.clone();
        assert_eq!(
            relocation_member_owner(
                &domain,
                &schedule,
                &member("route"),
                &live(&["node-1", "node-2"])
            )
            .expect_err("an unavailable owner must be rejected"),
            "junction 'route' is owned by unavailable node 'node-3'; relocate it after failover \
             reassigns it"
        );
    }

    #[test]
    fn rejects_a_member_that_is_not_scheduled() {
        let schedule = schedule(Vec::new());
        let domain = schedule.domain.clone();
        assert_eq!(
            relocation_member_owner(&domain, &schedule, &member("route"), &live(&["node-1"]))
                .expect_err("an unscheduled member must be rejected"),
            "junction 'route' is not scheduled in domain 'relocation_test'"
        );
    }

    #[test]
    fn relocation_schedule_puts_the_former_owner_first_among_replicas() {
        let current = schedule(vec![junction_node("route", "node-1", &["node-3"])]);
        let desired = schedule(vec![junction_node("route", "node-2", &["node-3"])]);
        let planned = planned_relocation_schedule(
            &current,
            &desired,
            "node-2",
            &[member("route")],
            1,
            &live(&["node-1", "node-2", "node-3"]),
            &live(&["node-1", "node-2", "node-3"]),
        );
        let node =
            scheduled_node(&planned, &member("route")).expect("member must remain scheduled");
        assert_eq!(node.primary_node.as_deref(), Some("node-2"));
        assert_eq!(node.assigned_nodes, vec!["node-2", "node-1"]);
    }

    #[test]
    fn a_replica_count_of_zero_leaves_the_former_owner_behind() {
        let current = schedule(vec![junction_node("route", "node-1", &[])]);
        let desired = schedule(vec![junction_node("route", "node-2", &[])]);
        let planned = planned_relocation_schedule(
            &current,
            &desired,
            "node-2",
            &[member("route")],
            0,
            &live(&["node-1", "node-2"]),
            &live(&["node-1", "node-2"]),
        );
        let node =
            scheduled_node(&planned, &member("route")).expect("member must remain scheduled");
        assert_eq!(node.assigned_nodes, vec!["node-2"]);
    }
}
