//! Which cluster member each schedulable node runs on.
//!
//! Layer: decisions.
//!
//! - **Owns.** The assignment policy: how many slots a model needs, the order candidates are
//!   ranked in, upstream locality, and the deterministic test mode that replaces all of it.
//! - **Depends on.** The active graph it reads and the placement plan that constrains it.
//! - **Must not know.** How an assignment is applied or executed.

use std::cmp::Reverse;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use meticulous::OptionExt;
use nervix_models::{
    ClusterNodeName, DomainName, DomainSchedule, Model, NodeRef, PlacementGroupSchedule,
    PlacementPolicy, ScheduledNode, ScheduledNodes,
};
use petgraph::{Direction, graph::DiGraph, prelude::NodeIndex, visit::EdgeRef};
use sorted_vec::SortedSet;

use crate::registry::{
    graph::{ActiveGraph, ActiveNode, EdgeKind, is_schedulable_model, schedulable_depth},
    placement::{PlacementPair, ResolvedPlacementPair},
};
#[cfg(feature = "testing")]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerMode {
    #[default]
    Sticky,
    Random,
}

/// How well one cluster node suits the entity being placed. The field order is the order the
/// candidates are ranked in: placement policy first, then operator preference, then the lightest
/// load, then how far the node sits from the round-robin cursor, with the node name breaking ties.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct AssignmentCandidate {
    placement_order: isize,
    preferred_order: usize,
    load: Reverse<usize>,
    round_robin_distance: Reverse<usize>,
    node_id: ClusterNodeName,
}

/// One graph node awaiting placement, ordered by how deep in the dataflow it sits so upstream
/// nodes are assigned before the nodes that read from them.
struct PlacementCandidate {
    index: NodeIndex,
    node: ActiveNode,
    depth: usize,
}

fn locality_affinity_scores(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    index: NodeIndex,
    assigned_by_key: &HashMap<NodeRef, Vec<ClusterNodeName>>,
) -> HashMap<ClusterNodeName, usize> {
    let mut scores = HashMap::<ClusterNodeName, usize>::new();
    collect_locality_affinity(
        graph,
        index,
        assigned_by_key,
        &mut HashSet::new(),
        &mut scores,
    );
    scores
}

fn collect_locality_affinity(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    index: NodeIndex,
    assigned_by_key: &HashMap<NodeRef, Vec<ClusterNodeName>>,
    visited: &mut HashSet<NodeIndex>,
    scores: &mut HashMap<ClusterNodeName, usize>,
) {
    if !visited.insert(index) {
        return;
    }

    for edge in graph.edges_directed(index, Direction::Incoming) {
        if !edge.weight().is_runtime_flow_edge() {
            continue;
        }
        let source = edge.source();
        let source_node = graph
            .node_weight(source)
            .verified("this endpoint comes from an edge of the same graph");
        if is_schedulable_model(source_node.config.as_ref()) {
            if let Some(node_ids) = assigned_by_key.get(&source_node.node_ref()) {
                for node_id in node_ids {
                    *scores.entry(node_id.clone()).or_insert(0) += 1;
                }
            }
        } else {
            collect_locality_affinity(graph, source, assigned_by_key, visited, scores);
        }
    }
}

struct AssignmentPlanner<'a> {
    graph: &'a DiGraph<ActiveNode, EdgeKind>,
    cluster_nodes: &'a [ClusterNodeName],
    assigned_by_key: &'a HashMap<NodeRef, Vec<ClusterNodeName>>,
    placement_pairs: &'a HashMap<PlacementPair, ResolvedPlacementPair>,
    node_load: &'a HashMap<ClusterNodeName, usize>,
    next_assignment: &'a mut usize,
    assignment_slots: usize,
}

#[cfg(feature = "testing")]
struct RandomAssignmentPlanner<'a> {
    cluster_nodes: &'a [ClusterNodeName],
    assignment_slots: usize,
    domain_seed: [u8; 32],
}

#[cfg(feature = "testing")]
impl<'a> RandomAssignmentPlanner<'a> {
    fn new(
        domain: &DomainName,
        cluster_nodes: &'a [ClusterNodeName],
        assignment_slots: usize,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/test-random-scheduler/domain");
        hasher.update(&[0]);
        hasher.update(domain.as_str().as_bytes());
        Self {
            cluster_nodes,
            assignment_slots,
            domain_seed: *hasher.finalize().as_bytes(),
        }
    }

    fn assignment_seed_for(&self, members: &[NodeRef]) -> u64 {
        let mut hasher = blake3::Hasher::new();
        if let [member] = members {
            hasher.update(b"nervix/test-random-scheduler/model");
            hasher.update(&[0]);
            hasher.update(&self.domain_seed);
            hasher.update(member.kind.as_str().as_bytes());
            hasher.update(&[0]);
            hasher.update(member.identifier.as_str().as_bytes());
        } else {
            let mut members = members.to_vec();
            members.sort();
            hasher.update(b"nervix/test-random-scheduler/placement-unit");
            hasher.update(&[0]);
            hasher.update(&self.domain_seed);
            for member in members {
                hasher.update(member.kind.as_str().as_bytes());
                hasher.update(&[0]);
                hasher.update(member.identifier.as_str().as_bytes());
                hasher.update(&[0]);
            }
        }
        let mut seed = [0; 8];
        seed.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
        u64::from_le_bytes(seed)
    }

    fn assignment(&self, members: &[NodeRef]) -> Vec<ClusterNodeName> {
        let mut nodes = self.cluster_nodes.to_vec();
        fastrand::Rng::with_seed(self.assignment_seed_for(members)).shuffle(&mut nodes);
        nodes.truncate(self.assignment_slots);
        nodes
    }

    fn for_model(&self, key: &NodeRef, model: &Model) -> Vec<ClusterNodeName> {
        if let Model::Ingestor(_) = model
            && model.executes_on_every_cluster_node()
        {
            return self.cluster_nodes.to_vec();
        }
        if is_schedulable_model(model) {
            self.assignment(std::slice::from_ref(key))
        } else {
            Vec::new()
        }
    }
}

impl AssignmentPlanner<'_> {
    fn ranked_assignment(
        &mut self,
        preferred_order: &HashMap<ClusterNodeName, usize>,
        placement_order: &HashMap<ClusterNodeName, isize>,
    ) -> Vec<ClusterNodeName> {
        let mut ordered_nodes = self
            .cluster_nodes
            .iter()
            .enumerate()
            .map(|(position, node_id)| AssignmentCandidate {
                placement_order: placement_order.get(node_id).copied().unwrap_or(0),
                preferred_order: preferred_order.get(node_id).copied().unwrap_or(0),
                load: Reverse(self.node_load.get(node_id).copied().unwrap_or(0)),
                round_robin_distance: Reverse(
                    (position + self.cluster_nodes.len()
                        - (*self.next_assignment % self.cluster_nodes.len()))
                        % self.cluster_nodes.len(),
                ),
                node_id: node_id.clone(),
            })
            .collect::<Vec<_>>();
        ordered_nodes.sort_unstable();
        ordered_nodes.reverse();
        *self.next_assignment += 1;
        ordered_nodes
            .into_iter()
            .take(self.assignment_slots)
            .map(|candidate| candidate.node_id)
            .collect()
    }

    fn for_group(&mut self, members: &[NodeRef], indices: &[NodeIndex]) -> Vec<ClusterNodeName> {
        if self.cluster_nodes.is_empty() {
            return Vec::new();
        }

        let mut preferred_order = HashMap::<ClusterNodeName, usize>::new();
        for index in indices {
            for (node_id, score) in
                locality_affinity_scores(self.graph, *index, self.assigned_by_key)
            {
                *preferred_order.entry(node_id).or_insert(0) += score;
            }
        }
        let mut placement_order = HashMap::<ClusterNodeName, isize>::new();
        for member in members {
            for (node_id, score) in
                placement_affinity_scores(member, self.placement_pairs, self.assigned_by_key)
            {
                *placement_order.entry(node_id).or_insert(0) += score;
            }
        }
        self.ranked_assignment(&preferred_order, &placement_order)
    }

    fn for_model(
        &mut self,
        index: NodeIndex,
        key: &NodeRef,
        model: &Model,
    ) -> Vec<ClusterNodeName> {
        if self.cluster_nodes.is_empty() {
            return Vec::new();
        }

        match model {
            Model::Ingestor(_) if model.executes_on_every_cluster_node() => {
                self.cluster_nodes.to_vec()
            }
            Model::Generator(_)
            | Model::Inferencer(_)
            | Model::Ingestor(_)
            | Model::Reingestor(_)
            | Model::Relay(_)
            | Model::Lookup(_)
            | Model::Deduplicator(_)
            | Model::Correlator(_)
            | Model::Reorderer(_)
            | Model::Junction(_)
            | Model::WindowProcessor(_)
            | Model::WasmProcessor(_)
            | Model::Emitter(_) => {
                let preferred_order =
                    locality_affinity_scores(self.graph, index, self.assigned_by_key);
                let placement_order =
                    placement_affinity_scores(key, self.placement_pairs, self.assigned_by_key);
                self.ranked_assignment(&preferred_order, &placement_order)
            }
            _ => Vec::new(),
        }
    }
}

fn assignment_for_model(
    planner: &mut AssignmentPlanner<'_>,
    index: NodeIndex,
    key: &NodeRef,
    model: &Model,
) -> Vec<ClusterNodeName> {
    if planner.cluster_nodes.is_empty() {
        return Vec::new();
    }
    planner.for_model(index, key, model)
}

fn placement_affinity_scores(
    subject: &NodeRef,
    pairs: &HashMap<PlacementPair, ResolvedPlacementPair>,
    assigned_by_key: &HashMap<NodeRef, Vec<ClusterNodeName>>,
) -> HashMap<ClusterNodeName, isize> {
    let mut scores = HashMap::<ClusterNodeName, isize>::new();
    for (pair, resolved) in pairs {
        let other = if pair.left == *subject {
            &pair.right
        } else if pair.right == *subject {
            &pair.left
        } else {
            continue;
        };
        let adjustment = match resolved.policy {
            PlacementPolicy::PreferColocation => 1,
            PlacementPolicy::SuggestSeparation => -1,
            PlacementPolicy::RequireColocation | PlacementPolicy::Neutral => continue,
        };
        let Some(nodes) = assigned_by_key.get(other) else {
            continue;
        };
        let Some(primary) = nodes.first() else {
            continue;
        };
        *scores.entry(primary.clone()).or_insert(0) += adjustment;
    }
    scores
}

impl ActiveGraph {
    /// Product code reaches the scheduler here. A `testing` build routes its own callers through
    /// [`Self::schedule_for_domain_with_mode`] so a scenario can pick the mode, which leaves this
    /// entry point compiled for production builds and for this crate's own tests.
    #[cfg(any(not(feature = "testing"), test))]
    pub(crate) fn schedule_for_domain(
        &self,
        domain: &DomainName,
        cluster_nodes: &[ClusterNodeName],
        replica_count: usize,
        default_policy: PlacementPolicy,
    ) -> DomainSchedule {
        #[cfg(feature = "testing")]
        {
            self.schedule_for_domain_inner(
                domain,
                cluster_nodes,
                replica_count,
                default_policy,
                SchedulerMode::Sticky,
            )
        }
        #[cfg(not(feature = "testing"))]
        {
            self.schedule_for_domain_inner(domain, cluster_nodes, replica_count, default_policy)
        }
    }

    #[cfg(feature = "testing")]
    pub(crate) fn schedule_for_domain_with_mode(
        &self,
        domain: &DomainName,
        cluster_nodes: &[ClusterNodeName],
        replica_count: usize,
        default_policy: PlacementPolicy,
        scheduler_mode: SchedulerMode,
    ) -> DomainSchedule {
        self.schedule_for_domain_inner(
            domain,
            cluster_nodes,
            replica_count,
            default_policy,
            scheduler_mode,
        )
    }

    fn schedule_for_domain_inner(
        &self,
        domain: &DomainName,
        cluster_nodes: &[ClusterNodeName],
        replica_count: usize,
        default_policy: PlacementPolicy,
        #[cfg(feature = "testing")] scheduler_mode: SchedulerMode,
    ) -> DomainSchedule {
        let cluster_nodes = SortedSet::from_unsorted(cluster_nodes.to_vec()).into_vec();
        let placement = self.placement.effective(default_policy);
        let assignment_slots = replica_count
            .min(cluster_nodes.len())
            .checked_add(1)
            .assured("a replica count clamped to the cluster leaves room for the primary slot");
        #[cfg(feature = "testing")]
        let random_assignment_planner = if let SchedulerMode::Random = scheduler_mode {
            Some(RandomAssignmentPlanner::new(
                domain,
                &cluster_nodes,
                assignment_slots,
            ))
        } else {
            None
        };
        let mut next_assignment = 0usize;
        let mut node_load = HashMap::<ClusterNodeName, usize>::new();
        let mut assigned_by_key = HashMap::<NodeRef, Vec<ClusterNodeName>>::new();
        let mut group_assignments = HashMap::<usize, Vec<ClusterNodeName>>::new();
        let mut depth_cache = HashMap::<NodeIndex, usize>::new();
        let mut nodes = self
            .graph
            .node_indices()
            .map(|index| {
                let node = self
                    .graph
                    .node_weight(index)
                    .verified("this index came from the same graph, which is not modified here")
                    .clone();
                let depth = schedulable_depth(&self.graph, index, &mut depth_cache);
                PlacementCandidate { index, node, depth }
            })
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| {
            left.depth
                .cmp(&right.depth)
                .then_with(|| left.node.kind.as_str().cmp(right.node.kind.as_str()))
                .then_with(|| {
                    left.node
                        .identifier
                        .as_str()
                        .cmp(right.node.identifier.as_str())
                })
                .then_with(|| left.index.index().cmp(&right.index.index()))
        });
        let index_by_key = nodes
            .iter()
            .map(|candidate| (candidate.node.node_ref(), candidate.index))
            .collect::<HashMap<_, _>>();

        let mut scheduled_nodes = ScheduledNodes::with_capacity(nodes.len());
        for PlacementCandidate { index, node, .. } in nodes {
            let key = node.node_ref();
            let group_index = placement.group_by_member.get(&key).copied();
            let mut assigned_nodes = if let Some(existing) =
                group_index.and_then(|group_index| group_assignments.get(&group_index))
            {
                existing.clone()
            } else {
                let mut assignment_planner = AssignmentPlanner {
                    graph: &self.graph,
                    cluster_nodes: &cluster_nodes,
                    assigned_by_key: &assigned_by_key,
                    placement_pairs: &placement.pairs,
                    node_load: &node_load,
                    next_assignment: &mut next_assignment,
                    assignment_slots,
                };
                let assignment = if let Some(group_index) = group_index {
                    let members = &placement.require_groups[group_index];
                    let member_indices = members
                        .iter()
                        .map(|member| {
                            index_by_key
                                .get(member)
                                .copied()
                                .verified("index_by_key was built from every node of this graph")
                        })
                        .collect::<Vec<_>>();
                    #[cfg(feature = "testing")]
                    if let Some(random_assignment_planner) = &random_assignment_planner {
                        random_assignment_planner.assignment(members)
                    } else {
                        assignment_planner.for_group(members, &member_indices)
                    }
                    #[cfg(not(feature = "testing"))]
                    assignment_planner.for_group(members, &member_indices)
                } else {
                    #[cfg(feature = "testing")]
                    if let Some(random_assignment_planner) = &random_assignment_planner {
                        random_assignment_planner.for_model(&key, node.config.as_ref())
                    } else {
                        assignment_for_model(
                            &mut assignment_planner,
                            index,
                            &key,
                            node.config.as_ref(),
                        )
                    }
                    #[cfg(not(feature = "testing"))]
                    assignment_for_model(&mut assignment_planner, index, &key, node.config.as_ref())
                };
                if let Some(group_index) = group_index {
                    group_assignments.insert(group_index, assignment.clone());
                }
                assignment
            };
            if let Model::Relay(relay) = node.config.as_ref()
                && relay.materialized_state.is_none()
            {
                assigned_nodes.truncate(1);
            }
            let primary_node = assigned_nodes.first().cloned();
            if !assigned_nodes.is_empty() {
                assigned_by_key.insert(key, assigned_nodes.clone());
                for assigned_node in &assigned_nodes {
                    *node_load.entry(assigned_node.clone()).or_insert(0) += 1;
                }
            }
            let scheduled_node = ScheduledNode::new((*node.config).clone())
                .with_effective_branching(node.effective_branching, node.effective_branching_schema)
                .with_schema_fingerprint(self.schema_fingerprint_for_index(index))
                .placed_on(primary_node, assigned_nodes);
            scheduled_nodes.insert(scheduled_node.identity(), scheduled_node);
        }
        let placement_groups = placement
            .require_groups
            .iter()
            .map(|members| {
                let runtime_members = members.to_vec();
                let primary_node = if let Some(first) = members.first()
                    && let Some(node) = scheduled_nodes.get(first)
                {
                    node.primary_node.clone()
                } else {
                    None
                };
                PlacementGroupSchedule {
                    members: runtime_members,
                    primary_node,
                }
            })
            .collect();
        DomainSchedule {
            domain: domain.clone(),
            nodes: scheduled_nodes,
            placement_groups,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[cfg(feature = "testing")]
    use nervix_models::ModelName;
    use nervix_models::{
        AckMode, BranchSelection, CodecName, CreateIngestor, CreateJunction, EndpointName,
        GeneralErrorPolicy, IngestSource, IngestorName, ModelKind, ProcessorInputs,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::registry::{
        storage::Registry,
        test_fixtures::{
            branch_for_relay, branch_schema, client_model, codec, emitter, endpoint,
            full_graph_batch, ingestor, ingestor_with_params, named, placement, processor,
            reingestor, relay, relay_branched_by_relay_branch, relay_branched_like, scheduled_node,
            schema, syslog_client, temp_db_path, unbranched_transforming_outputs, vhost,
            wire_schema,
        },
    };

    #[cfg(feature = "testing")]
    #[test]
    fn placement_require_binds_the_random_test_scheduler() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_random_require").expect("valid domain");
        let mut models = full_graph_batch();
        models.push(placement(
            "critical_path",
            &["ing"],
            &["emit"],
            PlacementPolicy::RequireColocation,
            Some(nonzero!(1u64)),
        ));
        registry
            .apply_batch(&domain, models)
            .expect("placement should validate");
        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule = graph.schedule_for_domain_with_mode(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
            0,
            PlacementPolicy::Neutral,
            SchedulerMode::Random,
        );

        let owner = scheduled_node(&schedule, ModelKind::Ingestor, "ing")
            .assigned_single_node()
            .expect("ingestor should be assigned");
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Deduplicator, "p99_proc").assigned_single_node(),
            Some(owner)
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Emitter, "emit").assigned_single_node(),
            Some(owner)
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Relay, "notifications").assigned_single_node(),
            Some(owner)
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Relay, "p99").assigned_single_node(),
            Some(owner)
        );
        assert_eq!(schedule.placement_groups.len(), 1);
        assert_eq!(schedule.placement_groups[0].members.len(), 5);
        assert_eq!(
            schedule.placement_groups[0].primary_node.as_ref(),
            Some(owner)
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_suggest_separation_outranks_upstream_locality() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_suggest").expect("valid domain");
        let mut models = full_graph_batch();
        models.push(placement(
            "spread",
            &["ing"],
            &["p99_proc"],
            PlacementPolicy::SuggestSeparation,
            Some(nonzero!(1u64)),
        ));
        registry
            .apply_batch(&domain, models)
            .expect("placement should validate");
        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule = graph.schedule_for_domain(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
            ],
            0,
            PlacementPolicy::Neutral,
        );

        assert_ne!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing").assigned_single_node(),
            scheduled_node(&schedule, ModelKind::Deduplicator, "p99_proc").assigned_single_node()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_prefer_colocation_outranks_majority_upstream_locality() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_prefer").expect("valid domain");
        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_a"),
                    client_model("broker_b"),
                    client_model("broker_c"),
                    relay("source_a", "event_schema"),
                    relay("source_b", "event_schema"),
                    relay("source_c", "event_schema"),
                    relay("joined", "event_schema"),
                    ingestor("ing_a", "source_a", "event_codec", "broker_a"),
                    ingestor("ing_b", "source_b", "event_codec", "broker_b"),
                    ingestor("ing_c", "source_c", "event_codec", "broker_c"),
                    Model::Junction(CreateJunction {
                        name: named("join"),
                        from: ProcessorInputs::new(
                            vec![named("source_a"), named("source_b"), named("source_c")],
                            Vec::new(),
                        ),
                        output_routes: unbranched_transforming_outputs("joined"),
                        branched_by: BranchSelection::unbranched(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                    placement(
                        "follow_b",
                        &["ing_b"],
                        &["join"],
                        PlacementPolicy::PreferColocation,
                        Some(nonzero!(1u64)),
                    ),
                ],
            )
            .expect("placement graph should validate");
        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule = graph.schedule_for_domain(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
            ],
            0,
            PlacementPolicy::Neutral,
        );

        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_a").assigned_single_node(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_b").assigned_single_node(),
            Some(&named::<ClusterNodeName>("node-2"))
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_c").assigned_single_node(),
            Some(&named::<ClusterNodeName>("node-1"))
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Junction, "join").assigned_single_node(),
            Some(&named::<ClusterNodeName>("node-2")),
            "explicit placement preference must beat two upstream-locality votes for node-1"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_spreads_independent_ingestors_before_locality_applies() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_a"),
                    client_model("broker_b"),
                    relay("notifications_a", "event_schema"),
                    relay("notifications_b", "event_schema"),
                    ingestor("ing_a", "notifications_a", "event_codec", "broker_a"),
                    ingestor("ing_b", "notifications_b", "event_codec", "broker_b"),
                ],
            )
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule = graph.schedule_for_domain(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
            ],
            0,
            PlacementPolicy::Neutral,
        );

        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_a").assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_b").assigned_nodes,
            vec![named::<ClusterNodeName>("node-2")]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_prefers_upstream_locality_for_dedicated_chain() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule = graph.schedule_for_domain(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
            0,
            PlacementPolicy::Neutral,
        );

        let ingestor_node = scheduled_node(&schedule, ModelKind::Ingestor, "ing")
            .assigned_single_node()
            .cloned()
            .clone();
        let processor_node = scheduled_node(&schedule, ModelKind::Deduplicator, "p99_proc")
            .assigned_single_node()
            .cloned()
            .clone();
        let emitter_node = scheduled_node(&schedule, ModelKind::Emitter, "emit")
            .assigned_single_node()
            .cloned()
            .clone();

        assert_eq!(processor_node, ingestor_node);
        assert_eq!(emitter_node, processor_node);

        let _ = fs::remove_dir_all(path);
    }

    #[cfg(feature = "testing")]
    #[test]
    fn random_test_scheduler_preserves_singleton_seed_and_assignment() {
        let domain = DomainName::parse("default").expect("valid domain");
        let mut domain_hasher = blake3::Hasher::new();
        domain_hasher.update(b"nervix/test-random-scheduler/domain");
        domain_hasher.update(&[0]);
        domain_hasher.update(domain.as_str().as_bytes());
        let domain_seed = *domain_hasher.finalize().as_bytes();
        let member = NodeRef::new(ModelKind::Ingestor, named::<ModelName>("ing"));

        let mut expected_hasher = blake3::Hasher::new();
        expected_hasher.update(b"nervix/test-random-scheduler/model");
        expected_hasher.update(&[0]);
        expected_hasher.update(&domain_seed);
        expected_hasher.update(member.kind.as_str().as_bytes());
        expected_hasher.update(&[0]);
        expected_hasher.update(member.identifier.as_str().as_bytes());
        let mut expected_seed = [0; 8];
        expected_seed.copy_from_slice(&expected_hasher.finalize().as_bytes()[..8]);
        let expected_seed = u64::from_le_bytes(expected_seed);

        let cluster_nodes = [
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ];
        let planner = super::RandomAssignmentPlanner::new(&domain, &cluster_nodes, 1);

        assert_eq!(
            planner.assignment_seed_for(std::slice::from_ref(&member)),
            expected_seed,
            "a singleton must retain the pre-placement random-scheduler seed"
        );
        let mut expected_assignment = cluster_nodes.to_vec();
        fastrand::Rng::with_seed(expected_seed).shuffle(&mut expected_assignment);
        expected_assignment.truncate(1);
        assert_eq!(
            planner.assignment(std::slice::from_ref(&member)),
            expected_assignment,
            "a singleton must retain the pre-placement randomized assignment"
        );
    }

    #[cfg(feature = "testing")]
    #[test]
    fn random_test_schedule_is_stable_for_unchanged_inputs() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let cluster_nodes = [
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ];
        let expected = graph.schedule_for_domain_with_mode(
            &domain,
            &cluster_nodes,
            0,
            PlacementPolicy::Neutral,
            SchedulerMode::Random,
        );
        for _ in 0..32 {
            assert_eq!(
                graph.schedule_for_domain_with_mode(
                    &domain,
                    &cluster_nodes,
                    0,
                    PlacementPolicy::Neutral,
                    SchedulerMode::Random,
                ),
                expected,
                "periodic reconciliation must not move an unchanged random schedule"
            );
        }

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn syslog_server_ingestor_is_assigned_to_every_cluster_node() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("syslog_cluster_wide").expect("valid domain");
        let mut models = full_graph_batch();
        let ingestor = models
            .iter_mut()
            .find_map(|model| match model {
                Model::Ingestor(ingestor) if ingestor.name == named("ing") => Some(ingestor),
                _ => None,
            })
            .expect("full graph must contain ing");
        ingestor.source = IngestSource::Syslog {
            client: named("syslog_listener"),
            quiesce: nervix_models::IngestQuiesceMode::Suspend,
        };
        models.push(syslog_client("syslog_listener"));
        registry
            .apply_batch(&domain, models)
            .expect("syslog graph should validate");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let cluster_nodes = [
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ];
        let schedule =
            graph.schedule_for_domain(&domain, &cluster_nodes, 0, PlacementPolicy::Neutral);
        let ingestor = scheduled_node(&schedule, ModelKind::Ingestor, "ing");
        assert_eq!(ingestor.assigned_nodes, cluster_nodes);
        assert_eq!(ingestor.execution_node(), None);
        assert!(cluster_nodes.iter().all(|node| ingestor.executes_on(node)));

        let _ = fs::remove_dir_all(path);
    }

    #[cfg(feature = "testing")]
    #[test]
    fn random_test_schedule_ignores_upstream_locality_across_domains() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let cluster_nodes = [
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ];
        let observed_cross_node_path = (0..32).any(|suffix| {
            let scheduled_domain =
                DomainName::parse(&format!("test_{suffix}")).expect("valid test domain");
            let schedule = graph.schedule_for_domain_with_mode(
                &scheduled_domain,
                &cluster_nodes,
                0,
                PlacementPolicy::Neutral,
                SchedulerMode::Random,
            );
            let ingestor =
                scheduled_node(&schedule, ModelKind::Ingestor, "ing").assigned_single_node();
            let processor = scheduled_node(&schedule, ModelKind::Deduplicator, "p99_proc")
                .assigned_single_node();
            let emitter =
                scheduled_node(&schedule, ModelKind::Emitter, "emit").assigned_single_node();
            ingestor != processor || processor != emitter
        });

        assert!(
            observed_cross_node_path,
            "independent random assignments should split paths across test domains"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_prefers_majority_upstream_locality_for_shared_downstream() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_a"),
                    client_model("broker_b"),
                    client_model("broker_c"),
                    client_model("broker_out"),
                    relay_branched_by_relay_branch("root_a", "event_schema"),
                    relay_branched_by_relay_branch("root_b", "event_schema"),
                    relay_branched_by_relay_branch("root_c", "event_schema"),
                    relay_branched_like("branch_a", "event_schema", "root_a"),
                    relay_branched_like("branch_b", "event_schema", "root_b"),
                    relay_branched_like("branch_c", "event_schema", "root_c"),
                    relay_branched_by_relay_branch("shared", "event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("root_a", "value_branch"),
                    branch_for_relay("root_b", "value_branch"),
                    branch_for_relay("root_c", "value_branch"),
                    branch_for_relay("shared", "value_branch"),
                    ingestor_with_params("ing_a", "root_a", "event_codec", "broker_a", &["value"]),
                    ingestor_with_params("ing_b", "root_b", "event_codec", "broker_b", &["value"]),
                    ingestor_with_params("ing_c", "root_c", "event_codec", "broker_c", &["value"]),
                    processor("proc_a", "root_a", "branch_a"),
                    processor("proc_b", "root_b", "branch_b"),
                    processor("proc_c", "root_c", "branch_c"),
                    reingestor("shared_a", "branch_a", "shared", &["value"]),
                    reingestor("shared_b", "branch_b", "shared", &["value"]),
                    reingestor("shared_c", "branch_c", "shared", &["value"]),
                    emitter("emit_shared", "shared", "event_codec", "broker_out"),
                ],
            )
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule = graph.schedule_for_domain(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
            ],
            0,
            PlacementPolicy::Neutral,
        );

        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_a").assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_b").assigned_nodes,
            vec![named::<ClusterNodeName>("node-2")]
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_c").assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );

        assert_eq!(
            scheduled_node(&schedule, ModelKind::Emitter, "emit_shared").assigned_nodes,
            vec![named::<ClusterNodeName>("node-1")]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_places_server_side_ingestors_on_all_live_nodes() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    vhost("public", &["events.example.com"]),
                    endpoint(
                        "ingest_http",
                        "public",
                        "/ingest",
                        nervix_models::EndpointType::Http,
                    ),
                    relay("notifications", "event_schema"),
                    Model::Ingestor(CreateIngestor {
                        name: IngestorName::parse("http_ing").expect("valid identifier"),
                        output_routes: unbranched_transforming_outputs("notifications"),
                        decode_using_codec: CodecName::parse("event_codec")
                            .expect("valid identifier"),
                        timestamp_source: None,
                        source: IngestSource::Endpoint {
                            endpoint: EndpointName::parse("ingest_http").expect("valid identifier"),
                            mode: nervix_models::EndpointIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                                max_size: "1MiB".to_string(),
                            },
                        },
                        general_error_policy: GeneralErrorPolicy::Log,

                        filter_where: None,
                    }),
                ],
            )
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule = graph.schedule_for_domain(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
            0,
            PlacementPolicy::Neutral,
        );

        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "http_ing").assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3")
            ]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_removes_server_side_ingestor_placements_for_missing_nodes() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    vhost("public", &["events.example.com"]),
                    endpoint(
                        "ingest_ws",
                        "public",
                        "/ws",
                        nervix_models::EndpointType::Websockets,
                    ),
                    relay("notifications", "event_schema"),
                    Model::Ingestor(CreateIngestor {
                        name: IngestorName::parse("ws_ing").expect("valid identifier"),
                        output_routes: unbranched_transforming_outputs("notifications"),
                        decode_using_codec: CodecName::parse("event_codec")
                            .expect("valid identifier"),
                        timestamp_source: None,
                        source: IngestSource::Endpoint {
                            endpoint: EndpointName::parse("ingest_ws").expect("valid identifier"),
                            mode: nervix_models::EndpointIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                                max_size: "1MiB".to_string(),
                            },
                        },
                        general_error_policy: GeneralErrorPolicy::Log,

                        filter_where: None,
                    }),
                ],
            )
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let initial_schedule = graph.schedule_for_domain(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
            0,
            PlacementPolicy::Neutral,
        );
        let reduced_schedule = graph.schedule_for_domain(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-3").expect("valid name"),
            ],
            0,
            PlacementPolicy::Neutral,
        );

        assert_eq!(
            scheduled_node(&initial_schedule, ModelKind::Ingestor, "ws_ing").assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
                named::<ClusterNodeName>("node-3")
            ]
        );
        assert_eq!(
            scheduled_node(&reduced_schedule, ModelKind::Ingestor, "ws_ing").assigned_nodes,
            vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-3")
            ]
        );

        let _ = fs::remove_dir_all(path);
    }
}
