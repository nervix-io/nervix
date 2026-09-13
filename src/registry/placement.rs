//! What a placement rule claims, and whether the graph can satisfy it.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving a rule's members, the pairs and corridors it claims, the require groups
//!   it binds together, and the plan a caller reads back.
//! - **Depends on.** The active graph and the Models the rules name.
//! - **Must not know.** Which cluster member a plan ends up assigning.

use std::{cmp::Ordering, collections::VecDeque, num::NonZeroU64};

use ahash::{HashMap, HashMapExt, HashSet};
use error_stack::Report;
use meticulous::OptionExt;
use nervix_models::{
    CreatePlacement, DomainName, Model, ModelIndex, ModelKind, ModelName, NodeRef, PlacementName,
    PlacementPolicy, RelayName,
};
use petgraph::{Direction, graph::DiGraph, prelude::NodeIndex, visit::EdgeRef};

use crate::registry::{
    error::RegistryError,
    graph::{ActiveNode, EdgeKind},
    validation::materialized_state::model_materialized_state_dependencies,
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementPlan {
    pub(crate) rules: Vec<PlacementRulePlan>,
    pub(in crate::registry) effective_pairs: Vec<PlacementEffectivePair>,
    pub(crate) require_groups: Vec<PlacementRequireGroupPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementRulePlan {
    pub(crate) name: ModelName,
    pub(in crate::registry) from: Vec<ModelName>,
    pub(in crate::registry) to: Vec<ModelName>,
    pub(crate) policy: PlacementPolicy,
    pub(crate) rank: Option<NonZeroU64>,
    pub(crate) endpoint_pairs: Vec<PlacementEndpointPairPlan>,
    pub(crate) claims: Vec<PlacementRuleClaimPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementEndpointPairPlan {
    pub(crate) source: NodeRef,
    pub(crate) destination: NodeRef,
    pub(crate) connected: bool,
    pub(crate) corridor: Vec<NodeRef>,
    pub(crate) witnesses: Vec<PlacementCorridorWitness>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementCorridorWitness {
    pub(in crate::registry) captured: NodeRef,
    pub(crate) path: Vec<NodeRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementRuleClaimPlan {
    pub(crate) left: NodeRef,
    pub(crate) right: NodeRef,
    pub(crate) effective: bool,
    pub(crate) effective_policy: PlacementPolicy,
    pub(crate) winning_rules: Vec<PlacementName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementEffectivePair {
    pub(crate) left: NodeRef,
    pub(crate) right: NodeRef,
    pub(in crate::registry) policy: PlacementPolicy,
    pub(crate) winning_rules: Vec<PlacementName>,
    pub(in crate::registry) from_domain_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementRequireGroupPlan {
    pub(crate) members: Vec<NodeRef>,
    pub(crate) bonds: Vec<PlacementEffectivePair>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::registry) struct PlacementPair {
    pub(in crate::registry) left: NodeRef,
    pub(in crate::registry) right: NodeRef,
}

impl PlacementPair {
    fn new(left: NodeRef, right: NodeRef) -> Option<Self> {
        if left == right {
            return None;
        }
        if left <= right {
            Some(Self { left, right })
        } else {
            Some(Self {
                left: right,
                right: left,
            })
        }
    }

    fn runtime_nodes(&self) -> (NodeRef, NodeRef) {
        (self.left.clone(), self.right.clone())
    }
}

#[derive(Debug, Clone)]
struct PlacementRuleAnalysis {
    model: CreatePlacement,
    endpoint_pairs: Vec<PlacementEndpointAnalysis>,
    claimed_pairs: HashSet<PlacementPair>,
}

#[derive(Debug, Clone)]
pub(in crate::registry) struct PlacementEndpointAnalysis {
    source: NodeRef,
    destination: NodeRef,
    pub(in crate::registry) corridor: Vec<NodeRef>,
    witnesses: Vec<(NodeRef, Vec<NodeRef>)>,
}

#[derive(Debug, Clone)]
struct PlacementClaim {
    rule: PlacementName,
    policy: PlacementPolicy,
    rank: Option<NonZeroU64>,
}

#[derive(Debug, Clone)]
pub(in crate::registry) struct ResolvedPlacementPair {
    pub(in crate::registry) policy: PlacementPolicy,
    pub(in crate::registry) winning_rules: Vec<PlacementName>,
    pub(in crate::registry) from_domain_default: bool,
}

#[derive(Debug, Clone, Default)]
pub(in crate::registry) struct PlacementAnalysis {
    rules: Vec<PlacementRuleAnalysis>,
    explicit_pairs: HashMap<PlacementPair, ResolvedPlacementPair>,
    direct_pairs: HashSet<PlacementPair>,
    /// Retained so a relocation corridor can be covered between endpoints no placement rule
    /// names, using the same path-gated coverage the rules use.
    pub(in crate::registry) topology: PlacementTopology,
}

#[derive(Debug, Clone)]
pub(in crate::registry) struct EffectivePlacementPlan {
    pub(in crate::registry) pairs: HashMap<PlacementPair, ResolvedPlacementPair>,
    pub(in crate::registry) require_groups: Vec<Vec<NodeRef>>,
    pub(in crate::registry) group_by_member: HashMap<NodeRef, usize>,
}

impl PlacementAnalysis {
    pub(in crate::registry) fn build(
        domain: &DomainName,
        models: &ModelIndex,
        indices: &HashMap<NodeRef, NodeIndex>,
        graph: &mut DiGraph<ActiveNode, EdgeKind>,
    ) -> Result<Self, Report<RegistryError>> {
        let topology = PlacementTopology::build(models, indices, graph);
        let mut placement_models = models
            .models()
            .filter_map(|model| match model {
                Model::Placement(placement) => Some(placement.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        placement_models.sort_by(|left, right| left.name.as_str().cmp(right.name.as_str()));

        let mut rules = Vec::with_capacity(placement_models.len());
        let mut claims_by_pair = HashMap::<PlacementPair, Vec<PlacementClaim>>::new();
        for placement in placement_models {
            placement.validate().map_err(|error| {
                Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: placement.name.as_str().to_string(),
                    reason: error.to_string(),
                })
            })?;
            let placement_index = indices
                .get(&NodeRef::new(ModelKind::Placement, placement.name.clone()))
                .copied()
                .verified("the pass above added a graph node for every placement");
            let from = resolve_placement_members(domain, &placement, &placement.from, models)?;
            let to = resolve_placement_members(domain, &placement, &placement.to, models)?;

            let mut pinned = HashSet::default();
            for member in from.iter().chain(&to) {
                if pinned.insert(member.pin.clone()) {
                    let pin_index = indices.get(&member.pin).copied().verified(
                        "resolve_placement_members resolved every pin against this same index map",
                    );
                    graph.add_edge(pin_index, placement_index, EdgeKind::RequiredBy);
                }
            }

            let mut endpoint_pairs = Vec::new();
            let mut claimed_pairs = HashSet::default();
            for source in &from {
                for destination in &to {
                    let endpoint = topology
                        .endpoint_analysis(source.runtime.clone(), destination.runtime.clone());
                    for left_index in 0..endpoint.corridor.len() {
                        for right_index in left_index + 1..endpoint.corridor.len() {
                            let pair = PlacementPair::new(
                                endpoint.corridor[left_index].clone(),
                                endpoint.corridor[right_index].clone(),
                            )
                            .verified(
                                "the two indices address different corridor positions, so the \
                                 members differ",
                            );
                            if claimed_pairs.insert(pair.clone()) {
                                claims_by_pair
                                    .entry(pair)
                                    .or_default()
                                    .push(PlacementClaim {
                                        rule: placement.name.clone(),
                                        policy: placement.policy,
                                        rank: placement.rank,
                                    });
                            }
                        }
                    }
                    endpoint_pairs.push(endpoint);
                }
            }
            rules.push(PlacementRuleAnalysis {
                model: placement,
                endpoint_pairs,
                claimed_pairs,
            });
        }

        let mut explicit_pairs = HashMap::default();
        for (pair, claims) in claims_by_pair {
            let strongest = claims
                .iter()
                .map(|claim| placement_rank_key(claim.rank))
                .min()
                .verified("a pair enters claims_by_pair only together with its first claim");
            let mut winners = claims
                .iter()
                .filter(|claim| placement_rank_key(claim.rank) == strongest)
                .collect::<Vec<_>>();
            winners.sort_by(|left, right| left.rule.as_str().cmp(right.rule.as_str()));
            let policy = winners[0].policy;
            if let Some(conflict) = winners.iter().find(|claim| claim.policy != policy) {
                let first = winners
                    .iter()
                    .find(|claim| claim.policy == policy)
                    .verified("policy was read from winners[0], so at least that claim carries it");
                return Err(Report::new(RegistryError::PlacementConflict {
                    domain: domain.as_str().to_string(),
                    left_rule: first.rule.as_str().to_string(),
                    right_rule: conflict.rule.as_str().to_string(),
                    left_kind: pair.left.kind.as_str(),
                    left_identifier: pair.left.identifier.as_str().to_string(),
                    right_kind: pair.right.kind.as_str(),
                    right_identifier: pair.right.identifier.as_str().to_string(),
                }));
            }
            let mut winning_rules = winners
                .into_iter()
                .map(|claim| claim.rule.clone())
                .collect::<Vec<_>>();
            winning_rules.dedup();
            explicit_pairs.insert(
                pair,
                ResolvedPlacementPair {
                    policy,
                    winning_rules,
                    from_domain_default: false,
                },
            );
        }

        Ok(Self {
            rules,
            explicit_pairs,
            direct_pairs: topology.direct_pairs(),
            topology,
        })
    }

    pub(in crate::registry) fn effective(
        &self,
        default_policy: PlacementPolicy,
    ) -> EffectivePlacementPlan {
        let mut pairs = self.explicit_pairs.clone();
        for pair in &self.direct_pairs {
            pairs
                .entry(pair.clone())
                .or_insert_with(|| ResolvedPlacementPair {
                    policy: default_policy,
                    winning_rules: Vec::new(),
                    from_domain_default: true,
                });
        }

        let require_pairs = pairs
            .iter()
            .filter_map(|(pair, resolved)| {
                (resolved.policy == PlacementPolicy::RequireColocation).then_some(pair.clone())
            })
            .collect::<Vec<_>>();
        let require_groups = placement_require_groups(&require_pairs);
        let mut group_by_member = HashMap::default();
        for (group_index, members) in require_groups.iter().enumerate() {
            for member in members {
                group_by_member.insert(member.clone(), group_index);
            }
        }
        EffectivePlacementPlan {
            pairs,
            require_groups,
            group_by_member,
        }
    }

    pub(in crate::registry) fn plan(&self, default_policy: PlacementPolicy) -> PlacementPlan {
        let effective = self.effective(default_policy);
        let mut effective_pairs = effective
            .pairs
            .iter()
            .map(|(pair, resolved)| placement_effective_pair(pair, resolved))
            .collect::<Vec<_>>();
        effective_pairs.sort_by(placement_effective_pair_cmp);

        let mut rules = self
            .rules
            .iter()
            .map(|rule| {
                let mut claims = rule
                    .claimed_pairs
                    .iter()
                    .map(|pair| {
                        let resolved = effective.pairs.get(pair).verified(
                            "every claimed pair of this rule was inserted into the effective map \
                             above",
                        );
                        let (left, right) = pair.runtime_nodes();
                        PlacementRuleClaimPlan {
                            left,
                            right,
                            effective: resolved.winning_rules.contains(&rule.model.name),
                            effective_policy: resolved.policy,
                            winning_rules: resolved.winning_rules.clone(),
                        }
                    })
                    .collect::<Vec<_>>();
                claims.sort_by(placement_rule_claim_cmp);
                PlacementRulePlan {
                    name: ModelName::from(&rule.model.name),
                    from: rule.model.from.clone(),
                    to: rule.model.to.clone(),
                    policy: rule.model.policy,
                    rank: rule.model.rank,
                    endpoint_pairs: rule
                        .endpoint_pairs
                        .iter()
                        .map(placement_endpoint_pair_plan)
                        .collect(),
                    claims,
                }
            })
            .collect::<Vec<_>>();
        rules.sort_by(|left, right| left.name.as_str().cmp(right.name.as_str()));

        let require_groups = effective
            .require_groups
            .iter()
            .map(|members| {
                let member_set = members.iter().cloned().collect::<HashSet<_>>();
                let mut bonds = effective
                    .pairs
                    .iter()
                    .filter(|(pair, resolved)| {
                        resolved.policy == PlacementPolicy::RequireColocation
                            && member_set.contains(&pair.left)
                            && member_set.contains(&pair.right)
                    })
                    .map(|(pair, resolved)| placement_effective_pair(pair, resolved))
                    .collect::<Vec<_>>();
                bonds.sort_by(placement_effective_pair_cmp);
                PlacementRequireGroupPlan {
                    members: members.to_vec(),
                    bonds,
                }
            })
            .collect();

        PlacementPlan {
            rules,
            effective_pairs,
            require_groups,
        }
    }
}

#[derive(Debug, Clone)]
struct ResolvedPlacementMember {
    runtime: NodeRef,
    pin: NodeRef,
}

#[derive(Debug, Clone, Default)]
pub(in crate::registry) struct PlacementTopology {
    adjacency: HashMap<NodeRef, Vec<NodeRef>>,
    reverse: HashMap<NodeRef, Vec<NodeRef>>,
}

impl PlacementTopology {
    fn build(
        models: &ModelIndex,
        indices: &HashMap<NodeRef, NodeIndex>,
        graph: &DiGraph<ActiveNode, EdgeKind>,
    ) -> Self {
        let placement_indices = graph
            .node_indices()
            .filter(|index| {
                graph
                    .node_weight(*index)
                    .is_some_and(|node| is_placement_runtime_model(node.config.as_ref()))
            })
            .collect::<HashSet<_>>();
        let mut adjacency_sets = HashMap::<NodeRef, HashSet<NodeRef>>::new();

        for source in &placement_indices {
            let source_node = graph
                .node_weight(*source)
                .verified("this endpoint comes from an edge of the same graph");
            let source_key = source_node.node_ref();
            adjacency_sets.entry(source_key.clone()).or_default();
            let mut pending = graph
                .edges_directed(*source, Direction::Outgoing)
                .filter(|edge| edge.weight().is_runtime_flow_edge())
                .map(|edge| edge.target())
                .collect::<Vec<_>>();
            let mut visited = HashSet::default();
            while let Some(index) = pending.pop() {
                if !visited.insert(index) {
                    continue;
                }
                if placement_indices.contains(&index) {
                    let target = graph
                        .node_weight(index)
                        .verified("this endpoint comes from an edge of the same graph")
                        .node_ref();
                    adjacency_sets
                        .entry(source_key.clone())
                        .or_default()
                        .insert(target);
                    continue;
                }
                pending.extend(
                    graph
                        .edges_directed(index, Direction::Outgoing)
                        .filter(|edge| edge.weight().is_runtime_flow_edge())
                        .map(|edge| edge.target()),
                );
            }
        }

        for (key, model) in models {
            if !is_placement_runtime_model(model) {
                continue;
            }
            for relay in placement_materialized_relays(model) {
                let relay = NodeRef::new(ModelKind::Relay, relay.clone());
                if indices.contains_key(&relay) {
                    adjacency_sets.entry(relay).or_default().insert(key.clone());
                }
            }
        }

        let mut adjacency = HashMap::default();
        for (source, targets) in adjacency_sets {
            let mut targets = targets.into_iter().collect::<Vec<_>>();
            targets.sort();
            adjacency.insert(source, targets);
        }
        let mut reverse_sets = HashMap::<NodeRef, HashSet<NodeRef>>::new();
        for (source, targets) in &adjacency {
            reverse_sets.entry(source.clone()).or_default();
            for target in targets {
                reverse_sets
                    .entry(target.clone())
                    .or_default()
                    .insert(source.clone());
            }
        }
        let mut reverse = HashMap::default();
        for (target, sources) in reverse_sets {
            let mut sources = sources.into_iter().collect::<Vec<_>>();
            sources.sort();
            reverse.insert(target, sources);
        }
        Self { adjacency, reverse }
    }

    fn direct_pairs(&self) -> HashSet<PlacementPair> {
        self.adjacency
            .iter()
            .flat_map(|(source, targets)| {
                targets
                    .iter()
                    .filter_map(|target| PlacementPair::new(source.clone(), target.clone()))
            })
            .collect()
    }

    pub(in crate::registry) fn endpoint_analysis(
        &self,
        source: NodeRef,
        destination: NodeRef,
    ) -> PlacementEndpointAnalysis {
        let connecting_path = if source == destination {
            self.cycle_path(&source)
        } else {
            self.path(&source, &destination)
        };
        let Some(_connecting_path) = connecting_path else {
            return PlacementEndpointAnalysis {
                source,
                destination,
                corridor: Vec::new(),
                witnesses: Vec::new(),
            };
        };

        let forward = self.reachable(&source, &self.adjacency);
        let backward = self.reachable(&destination, &self.reverse);
        let mut corridor = forward.intersection(&backward).cloned().collect::<Vec<_>>();
        corridor.sort();
        let mut witnesses = Vec::new();
        for captured in corridor
            .iter()
            .filter(|captured| **captured != source && **captured != destination)
        {
            let Some(mut prefix) = self.path(&source, captured) else {
                continue;
            };
            let Some(suffix) = self.path(captured, &destination) else {
                continue;
            };
            prefix.extend(suffix.into_iter().skip(1));
            witnesses.push((captured.clone(), prefix));
        }
        PlacementEndpointAnalysis {
            source,
            destination,
            corridor,
            witnesses,
        }
    }

    fn reachable(
        &self,
        start: &NodeRef,
        edges: &HashMap<NodeRef, Vec<NodeRef>>,
    ) -> HashSet<NodeRef> {
        let mut visited = HashSet::default();
        let mut pending = vec![start.clone()];
        while let Some(node) = pending.pop() {
            if !visited.insert(node.clone()) {
                continue;
            }
            if let Some(targets) = edges.get(&node) {
                pending.extend(targets.iter().rev().cloned());
            }
        }
        visited
    }

    fn path(&self, start: &NodeRef, end: &NodeRef) -> Option<Vec<NodeRef>> {
        if start == end {
            return Some(vec![start.clone()]);
        }
        let mut pending = VecDeque::from([start.clone()]);
        let mut previous = HashMap::<NodeRef, NodeRef>::new();
        let mut visited = HashSet::from_iter([start.clone()]);
        while let Some(node) = pending.pop_front() {
            for target in self.adjacency.get(&node).into_iter().flatten() {
                if !visited.insert(target.clone()) {
                    continue;
                }
                previous.insert(target.clone(), node.clone());
                if target == end {
                    let mut path = vec![end.clone()];
                    let mut cursor = end;
                    while cursor != start {
                        cursor = previous.get(cursor).verified(
                            "the search records a predecessor for a node before it can be reached",
                        );
                        path.push(cursor.clone());
                    }
                    path.reverse();
                    return Some(path);
                }
                pending.push_back(target.clone());
            }
        }
        None
    }

    fn cycle_path(&self, start: &NodeRef) -> Option<Vec<NodeRef>> {
        for target in self.adjacency.get(start).into_iter().flatten() {
            if target == start {
                return Some(vec![start.clone(), start.clone()]);
            }
            if let Some(path) = self.path(target, start) {
                let mut cycle = vec![start.clone()];
                cycle.extend(path);
                return Some(cycle);
            }
        }
        None
    }
}

fn resolve_placement_members(
    domain: &DomainName,
    placement: &CreatePlacement,
    members: &[ModelName],
    models: &ModelIndex,
) -> Result<Vec<ResolvedPlacementMember>, Report<RegistryError>> {
    let mut resolved = Vec::new();
    let mut seen = HashSet::default();
    for member in members {
        let candidate = resolve_placement_member(domain, placement, member, models)?;
        if seen.insert(candidate.runtime.clone()) {
            resolved.push(candidate);
        }
    }
    Ok(resolved)
}

fn resolve_placement_member(
    domain: &DomainName,
    placement: &CreatePlacement,
    member: &ModelName,
    models: &ModelIndex,
) -> Result<ResolvedPlacementMember, Report<RegistryError>> {
    let mut eligible = Vec::new();
    let mut cluster_wide_ingestor = false;
    let mut ineligible_kinds = Vec::new();
    for (key, model) in models.iter().filter(|(key, _)| key.identifier == *member) {
        match model {
            Model::Ingestor(_) if model.executes_on_every_cluster_node() => {
                cluster_wide_ingestor = true;
            }
            _ if is_user_placement_member_model(model) => {
                eligible.push(ResolvedPlacementMember {
                    runtime: key.clone(),
                    pin: key.clone(),
                });
            }
            _ => ineligible_kinds.push(key.kind),
        }
    }
    eligible.sort_by(|left, right| left.runtime.cmp(&right.runtime));
    eligible.dedup_by(|left, right| left.runtime == right.runtime);
    if eligible.len() == 1 {
        return Ok(eligible.remove(0));
    }
    let reason = if eligible.len() > 1 {
        let kinds = eligible
            .iter()
            .map(|candidate| candidate.runtime.kind.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "placement member '{}' is ambiguous across eligible kinds {kinds}",
            member
        )
    } else if cluster_wide_ingestor {
        format!(
            "placement member '{}' is not placement-eligible: server-listener ingestors execute \
             on every cluster node",
            member
        )
    } else if !ineligible_kinds.is_empty() {
        ineligible_kinds.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        ineligible_kinds.dedup();
        format!(
            "placement member '{}' has non-schedulable kind {} and is not placement-eligible",
            member,
            ineligible_kinds
                .iter()
                .map(|kind| kind.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        format!("placement member '{}' does not exist", member)
    };
    Err(Report::new(RegistryError::InvalidModel {
        domain: domain.as_str().to_string(),
        identifier: placement.name.as_str().to_string(),
        reason,
    }))
}

fn is_user_placement_member_model(model: &Model) -> bool {
    matches!(
        model,
        Model::Generator(_)
            | Model::Inferencer(_)
            | Model::Ingestor(_)
            | Model::Reingestor(_)
            | Model::Relay(_)
            | Model::Lookup(_)
            | Model::Junction(_)
            | Model::Deduplicator(_)
            | Model::Correlator(_)
            | Model::Reorderer(_)
            | Model::WindowProcessor(_)
            | Model::WasmProcessor(_)
            | Model::Emitter(_)
    )
}

fn is_placement_eligible_member_model(model: &Model) -> bool {
    match model {
        Model::Ingestor(_) if model.executes_on_every_cluster_node() => false,
        _ => is_user_placement_member_model(model),
    }
}

pub(in crate::registry) fn ensure_placement_member_shape_change_allowed(
    domain: &DomainName,
    before: &Model,
    after: &Model,
    candidate_models: &ModelIndex,
) -> Result<(), Report<RegistryError>> {
    if !is_placement_eligible_member_model(before) || is_placement_eligible_member_model(after) {
        return Ok(());
    }

    let member = after.name();
    let mut placements = Vec::new();
    for model in candidate_models.models() {
        let Model::Placement(placement) = model else {
            continue;
        };
        if placement
            .from
            .iter()
            .chain(&placement.to)
            .any(|candidate| *candidate == member)
        {
            placements.push(placement.name.clone());
        }
    }
    placements.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    placements.dedup();
    if placements.is_empty() {
        return Ok(());
    }

    Err(Report::new(RegistryError::PlacementMemberPinned {
        domain: domain.as_str().to_string(),
        identifier: member.as_str().to_string(),
        placements: placements
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>()
            .join(", "),
    }))
}

fn is_placement_runtime_model(model: &Model) -> bool {
    match model {
        Model::Ingestor(_) if model.executes_on_every_cluster_node() => false,
        _ => is_user_placement_member_model(model),
    }
}

fn placement_materialized_relays(model: &Model) -> Vec<&RelayName> {
    let mut relays = model_materialized_state_dependencies(model)
        .iter()
        .map(|dependency| &dependency.relay)
        .collect::<Vec<_>>();
    if let Model::Generator(generator) = model {
        relays.push(&generator.materialized_relay);
    }
    relays
}

fn placement_rank_key(rank: Option<NonZeroU64>) -> (u8, u64) {
    match rank {
        Some(rank) => (0, rank.get()),
        None => (1, 0),
    }
}

fn placement_endpoint_pair_plan(endpoint: &PlacementEndpointAnalysis) -> PlacementEndpointPairPlan {
    PlacementEndpointPairPlan {
        source: endpoint.source.clone(),
        destination: endpoint.destination.clone(),
        connected: !endpoint.corridor.is_empty(),
        corridor: endpoint.corridor.to_vec(),
        witnesses: endpoint
            .witnesses
            .iter()
            .map(|(captured, path)| PlacementCorridorWitness {
                captured: captured.clone(),
                path: path.to_vec(),
            })
            .collect(),
    }
}

fn placement_effective_pair(
    pair: &PlacementPair,
    resolved: &ResolvedPlacementPair,
) -> PlacementEffectivePair {
    let (left, right) = pair.runtime_nodes();
    PlacementEffectivePair {
        left,
        right,
        policy: resolved.policy,
        winning_rules: resolved.winning_rules.clone(),
        from_domain_default: resolved.from_domain_default,
    }
}

fn placement_effective_pair_cmp(
    left: &PlacementEffectivePair,
    right: &PlacementEffectivePair,
) -> Ordering {
    left.left
        .cmp(&right.left)
        .then_with(|| left.right.cmp(&right.right))
}

fn placement_rule_claim_cmp(
    left: &PlacementRuleClaimPlan,
    right: &PlacementRuleClaimPlan,
) -> Ordering {
    left.left
        .cmp(&right.left)
        .then_with(|| left.right.cmp(&right.right))
}

fn placement_require_groups(require_pairs: &[PlacementPair]) -> Vec<Vec<NodeRef>> {
    let mut parent = HashMap::<NodeRef, NodeRef>::new();
    for pair in require_pairs {
        parent
            .entry(pair.left.clone())
            .or_insert_with(|| pair.left.clone());
        parent
            .entry(pair.right.clone())
            .or_insert_with(|| pair.right.clone());
        placement_union(&mut parent, &pair.left, &pair.right);
    }
    let members = parent.keys().cloned().collect::<Vec<_>>();
    let mut groups = HashMap::<NodeRef, Vec<NodeRef>>::new();
    for member in members {
        let root = placement_find(&mut parent, &member);
        groups.entry(root).or_default().push(member);
    }
    let mut groups = groups.into_values().collect::<Vec<_>>();
    for group in &mut groups {
        group.sort();
    }
    groups.sort_by(|left, right| left[0].cmp(&right[0]));
    groups
}

fn placement_find(parent: &mut HashMap<NodeRef, NodeRef>, member: &NodeRef) -> NodeRef {
    let direct = parent
        .get(member)
        .cloned()
        .verified("every member is inserted into the parent map before find runs over it");
    if direct == *member {
        return direct;
    }
    let root = placement_find(parent, &direct);
    parent.insert(member.clone(), root.clone());
    root
}

fn placement_union(parent: &mut HashMap<NodeRef, NodeRef>, left: &NodeRef, right: &NodeRef) {
    let left_root = placement_find(parent, left);
    let right_root = placement_find(parent, right);
    if left_root == right_root {
        return;
    }
    if left_root <= right_root {
        parent.insert(right_root, left_root);
    } else {
        parent.insert(left_root, right_root);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{
        AlterIngestor, AlterIngestorOperation, AlterPlacement, AlterPlacementOperation,
        ClusterNodeName, DropModel, IngestSource, MaterializedRelayState,
        MaterializedStateDependency, MaterializedStatePolicy,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::registry::{
        mutation::RegistryMutation,
        storage::Registry,
        test_fixtures::{
            client_model, endpoint, full_graph_batch, ingestor, named, placement, relay,
            relay_branched_like, scheduled_node, syslog_client, temp_db_path, vhost,
        },
    };

    #[test]
    fn placement_corridor_claims_every_runtime_pair_and_reports_witnesses() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_corridor").expect("valid domain");
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
            .expect("connected placement should validate");
        let plan = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .placement_plan(PlacementPolicy::Neutral);
        let rule = &plan.rules[0];
        assert_eq!(rule.name, named("critical_path"));
        assert_eq!(rule.endpoint_pairs.len(), 1);
        let endpoint = &rule.endpoint_pairs[0];
        assert!(endpoint.connected);
        assert_eq!(endpoint.source.identifier, named("ing"));
        assert_eq!(endpoint.destination.identifier, named("emit"));
        let mut corridor = endpoint
            .corridor
            .iter()
            .map(|member| member.identifier.as_str())
            .collect::<Vec<_>>();
        corridor.sort_unstable();
        assert_eq!(
            corridor,
            vec!["emit", "ing", "notifications", "p99", "p99_proc"]
        );
        assert_eq!(rule.claims.len(), 10, "a five-member corridor is a clique");
        assert_eq!(endpoint.witnesses.len(), 3);
        let mut captured = endpoint
            .witnesses
            .iter()
            .map(|witness| witness.captured.identifier.as_str())
            .collect::<Vec<_>>();
        captured.sort_unstable();
        assert_eq!(captured, ["notifications", "p99", "p99_proc"]);
        assert!(endpoint.witnesses.iter().all(|witness| {
            witness
                .path
                .iter()
                .map(|member| member.identifier.as_str())
                .eq(["ing", "notifications", "p99_proc", "p99", "emit"])
        }));
        assert_eq!(plan.require_groups.len(), 1);
        assert_eq!(plan.require_groups[0].members.len(), 5);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_disconnected_endpoint_pair_is_valid_with_empty_coverage() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_disconnected").expect("valid domain");
        let mut models = full_graph_batch();
        models.extend([
            client_model("other_broker"),
            relay("other_events", "event_schema"),
            ingestor("other_ing", "other_events", "event_codec", "other_broker"),
            placement(
                "no_path",
                &["emit"],
                &["other_ing"],
                PlacementPolicy::RequireColocation,
                Some(nonzero!(1u64)),
            ),
        ]);

        registry
            .apply_batch(&domain, models)
            .expect("a disconnected placement is valid");
        let plan = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .placement_plan(PlacementPolicy::Neutral);
        assert_eq!(plan.rules.len(), 1);
        assert!(!plan.rules[0].endpoint_pairs[0].connected);
        assert!(plan.rules[0].endpoint_pairs[0].corridor.is_empty());
        assert!(plan.rules[0].claims.is_empty());
        assert!(plan.require_groups.is_empty());

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_stronger_rank_overrides_weaker_policy_without_conflict() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_rank").expect("valid domain");
        let mut models = full_graph_batch();
        models.extend([
            placement(
                "weak_glue",
                &["ing"],
                &["p99_proc"],
                PlacementPolicy::RequireColocation,
                Some(nonzero!(2u64)),
            ),
            placement(
                "strong_cut",
                &["ing"],
                &["p99_proc"],
                PlacementPolicy::SuggestSeparation,
                Some(nonzero!(1u64)),
            ),
        ]);

        registry
            .apply_batch(&domain, models)
            .expect("different-rank claims should resolve");
        let plan = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .placement_plan(PlacementPolicy::Neutral);
        let effective = plan
            .effective_pairs
            .iter()
            .find(|pair| {
                let names = [
                    pair.left.identifier.as_str(),
                    pair.right.identifier.as_str(),
                ];
                names.contains(&"ing") && names.contains(&"p99_proc")
            })
            .expect("rule pair should be effective");
        assert_eq!(effective.policy, PlacementPolicy::SuggestSeparation);
        assert_eq!(effective.winning_rules, vec![named("strong_cut")]);
        let weak = plan
            .rules
            .iter()
            .find(|rule| rule.name == named("weak_glue"))
            .expect("weak rule should remain introspectable");
        assert!(!weak.claims[0].effective);
        assert_eq!(weak.claims[0].winning_rules, vec![named("strong_cut")]);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_equal_rank_different_policies_are_an_activation_conflict() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_conflict").expect("valid domain");
        let mut models = full_graph_batch();
        models.extend([
            placement(
                "glue",
                &["ing"],
                &["p99_proc"],
                PlacementPolicy::RequireColocation,
                Some(nonzero!(1u64)),
            ),
            placement(
                "cut",
                &["ing"],
                &["p99_proc"],
                PlacementPolicy::Neutral,
                Some(nonzero!(1u64)),
            ),
        ]);

        let error = registry
            .apply_batch(&domain, models)
            .expect_err("equal-rank conflicting claims must fail activation");
        let RegistryError::PlacementConflict {
            domain: error_domain,
            left_rule,
            right_rule,
            left_identifier,
            right_identifier,
            ..
        } = error.current_context()
        else {
            panic!("unexpected error: {error:#}");
        };
        assert_eq!(error_domain, domain.as_str());
        assert_eq!([left_rule.as_str(), right_rule.as_str()], ["cut", "glue"]);
        let witness = [left_identifier.as_str(), right_identifier.as_str()];
        assert_ne!(witness[0], witness[1]);
        assert!(
            witness
                .iter()
                .all(|member| { ["ing", "notifications", "p99_proc"].contains(member) })
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_materialized_relay_member_uses_state_delivery_dependency() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_materialized_state").expect("valid domain");
        let mut models = full_graph_batch();
        let Model::Relay(mut profiles) =
            relay_branched_like("profiles", "event_schema", "notifications")
        else {
            unreachable!("relay helper must build a relay")
        };
        profiles.materialized_state = Some(MaterializedRelayState::LastByTimestamp);
        models.push(Model::Relay(profiles));
        let deduplicator = models
            .iter_mut()
            .find_map(|model| match model {
                Model::Deduplicator(deduplicator) if deduplicator.name == named("p99_proc") => {
                    Some(deduplicator)
                }
                _ => None,
            })
            .expect("full graph must contain p99_proc");
        deduplicator
            .materialized_state
            .push(MaterializedStateDependency {
                relay: named("profiles"),
                policy: MaterializedStatePolicy::RequiredSkip,
            });
        models.push(placement(
            "state_local",
            &["profiles"],
            &["p99_proc"],
            PlacementPolicy::RequireColocation,
            Some(nonzero!(1u64)),
        ));

        registry
            .apply_batch(&domain, models)
            .expect("materialized-state placement should validate");
        let plan = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .placement_plan(PlacementPolicy::Neutral);
        let endpoint = &plan.rules[0].endpoint_pairs[0];
        assert!(endpoint.connected);
        assert_eq!(endpoint.source.kind, ModelKind::Relay);
        assert_eq!(endpoint.source.identifier, named("profiles"));
        assert_eq!(endpoint.destination.identifier, named("p99_proc"));
        assert_eq!(endpoint.corridor.len(), 2);
        assert_eq!(plan.require_groups[0].members.len(), 2);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_accepts_relay_and_rejects_cluster_wide_ingestor_members() {
        let relay_path = temp_db_path();
        let relay_registry = Registry::open(&relay_path).expect("registry should open");
        let relay_domain = DomainName::parse("placement_plain_relay").expect("valid domain");
        let mut relay_models = full_graph_batch();
        relay_models.push(placement(
            "plain_relay",
            &["notifications"],
            &["p99_proc"],
            PlacementPolicy::RequireColocation,
            None,
        ));
        relay_registry
            .apply_batch(&relay_domain, relay_models)
            .expect("a relay is a placement member");
        let relay_plan = relay_registry
            .active_graph(&relay_domain)
            .expect("relay graph should be installed")
            .placement_plan(PlacementPolicy::Neutral);
        assert_eq!(
            relay_plan.rules[0].endpoint_pairs[0].source.kind,
            ModelKind::Relay
        );

        let endpoint_path = temp_db_path();
        let endpoint_registry = Registry::open(&endpoint_path).expect("registry should open");
        let endpoint_domain =
            DomainName::parse("placement_endpoint_ingestor").expect("valid domain");
        let mut endpoint_models = full_graph_batch();
        let ingestor = endpoint_models
            .iter_mut()
            .find_map(|model| match model {
                Model::Ingestor(ingestor) if ingestor.name == named("ing") => Some(ingestor),
                _ => None,
            })
            .expect("full graph must contain ing");
        ingestor.source = IngestSource::Endpoint {
            endpoint: named("ingest_http"),
            mode: nervix_models::EndpointIngestMode::NoAckSequential,
            quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                max_size: "1MiB".to_string(),
            },
        };
        endpoint_models.extend([
            vhost("public", &["events.example.com"]),
            endpoint(
                "ingest_http",
                "public",
                "/ingest",
                nervix_models::EndpointType::Http,
            ),
            placement(
                "endpoint_member",
                &["ing"],
                &["emit"],
                PlacementPolicy::RequireColocation,
                None,
            ),
        ]);
        let endpoint_error = endpoint_registry
            .apply_batch(&endpoint_domain, endpoint_models)
            .expect_err("an endpoint-source ingestor is not a placement member");
        assert!(
            format!("{endpoint_error:#}")
                .contains("server-listener ingestors execute on every cluster node"),
            "unexpected endpoint error: {endpoint_error:#}"
        );

        let syslog_path = temp_db_path();
        let syslog_registry = Registry::open(&syslog_path).expect("registry should open");
        let syslog_domain = DomainName::parse("placement_syslog_ingestor").expect("valid domain");
        let mut syslog_models = full_graph_batch();
        let ingestor = syslog_models
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
        syslog_models.extend([
            syslog_client("syslog_listener"),
            placement(
                "syslog_member",
                &["ing"],
                &["emit"],
                PlacementPolicy::RequireColocation,
                None,
            ),
        ]);
        let syslog_error = syslog_registry
            .apply_batch(&syslog_domain, syslog_models)
            .expect_err("a syslog ingestor is not a placement member");
        assert!(
            format!("{syslog_error:#}")
                .contains("server-listener ingestors execute on every cluster node"),
            "unexpected syslog error: {syslog_error:#}"
        );

        let _ = fs::remove_dir_all(relay_path);
        let _ = fs::remove_dir_all(endpoint_path);
        let _ = fs::remove_dir_all(syslog_path);
    }

    #[test]
    fn placement_members_are_pinned_by_every_referencing_rule() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_pins").expect("valid domain");
        let mut models = full_graph_batch();
        models.extend([
            placement(
                "pin_from",
                &["p99_proc"],
                &["emit"],
                PlacementPolicy::PreferColocation,
                None,
            ),
            placement(
                "pin_to",
                &["ing"],
                &["p99_proc"],
                PlacementPolicy::PreferColocation,
                None,
            ),
        ]);
        registry
            .apply_batch(&domain, models)
            .expect("placements should validate");

        let error = registry
            .plan_mutations(
                &domain,
                &[RegistryMutation::Drop(DropModel {
                    kind: ModelKind::Deduplicator,
                    name: named("p99_proc"),
                })],
            )
            .expect_err("referenced placement member must be pinned");
        let RegistryError::DeleteInUse { blockers, .. } = error.current_context() else {
            panic!("unexpected error: {error:#}");
        };
        assert_eq!(blockers, "pin_from, pin_to");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_alter_then_member_drop_uses_ordered_candidate_graph() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_ordered_drop").expect("valid domain");
        let mut models = full_graph_batch();
        models.push(placement(
            "pin_ing",
            &["ing"],
            &["emit"],
            PlacementPolicy::PreferColocation,
            None,
        ));
        registry
            .apply_batch(&domain, models)
            .expect("placement should validate");

        let alter = RegistryMutation::AlterPlacement(AlterPlacement {
            placement: named("pin_ing"),
            operations: vec![AlterPlacementOperation::SetMembers {
                from: vec![named("p99_proc")],
                to: vec![named("emit")],
            }],
        });
        let drop_member = RegistryMutation::Drop(DropModel {
            kind: ModelKind::Ingestor,
            name: named("ing"),
        });
        registry
            .plan_mutations(&domain, &[alter.clone(), drop_member.clone()])
            .expect("an earlier placement alter must release the later drop");

        let error = registry
            .plan_mutations(&domain, &[drop_member, alter])
            .expect_err("dropping before releasing the placement pin must fail");
        let RegistryError::DeleteInUse { blockers, .. } = error.current_context() else {
            panic!("unexpected error: {error:#}");
        };
        assert_eq!(blockers, "pin_ing");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_non_placeable_alter_names_every_pinning_rule() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_pinned_alter").expect("valid domain");
        let mut models = full_graph_batch();
        models.extend([
            vhost("public", &["events.example.com"]),
            endpoint(
                "ingest_http",
                "public",
                "/ingest",
                nervix_models::EndpointType::Http,
            ),
            placement(
                "pin_a",
                &["ing"],
                &["emit"],
                PlacementPolicy::PreferColocation,
                None,
            ),
            placement(
                "pin_b",
                &["ing"],
                &["p99_proc"],
                PlacementPolicy::RequireColocation,
                Some(nonzero!(1u64)),
            ),
        ]);
        registry
            .apply_batch(&domain, models)
            .expect("placements should validate");

        let error = registry
            .plan_mutations(
                &domain,
                &[RegistryMutation::AlterIngestor(AlterIngestor {
                    ingestor: named("ing"),
                    operations: vec![AlterIngestorOperation::SetSource {
                        source: IngestSource::Endpoint {
                            endpoint: named("ingest_http"),
                            mode: nervix_models::EndpointIngestMode::NoAckSequential,
                            quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                                max_size: "1MiB".to_string(),
                            },
                        },
                    }],
                })],
            )
            .expect_err("a pinned member cannot become non-placement-eligible");
        let RegistryError::PlacementMemberPinned {
            identifier,
            placements,
            ..
        } = error.current_context()
        else {
            panic!("unexpected error: {error:#}");
        };
        assert_eq!(identifier, "ing");
        assert_eq!(placements, "pin_a, pin_b");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_default_require_forms_a_connected_component_from_per_hop_claims() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_default_require").expect("valid domain");
        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("graph should validate");
        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let plan = graph.placement_plan(PlacementPolicy::RequireColocation);

        assert_eq!(plan.effective_pairs.len(), 4, "the default is per-hop");
        assert!(
            plan.effective_pairs
                .iter()
                .all(|pair| pair.from_domain_default)
        );
        assert_eq!(plan.require_groups.len(), 1);
        assert_eq!(plan.require_groups[0].members.len(), 5);
        let schedule = graph.schedule_for_domain(
            &domain,
            &[
                ClusterNodeName::parse("node-1").expect("valid name"),
                ClusterNodeName::parse("node-2").expect("valid name"),
            ],
            0,
            PlacementPolicy::RequireColocation,
        );
        let owner = scheduled_node(&schedule, ModelKind::Ingestor, "ing")
            .assigned_single_node()
            .expect("ingestor should be assigned");
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Deduplicator, "p99_proc").assigned_single_node(),
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
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Emitter, "emit").assigned_single_node(),
            Some(owner)
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_cycle_corridor_captures_the_whole_cycle_with_member_witnesses() {
        let cycle_a = NodeRef::new(ModelKind::Reingestor, named::<ModelName>("cycle_a"));
        let cycle_b = NodeRef::new(ModelKind::Reingestor, named::<ModelName>("cycle_b"));
        let cycle_c = NodeRef::new(ModelKind::Reingestor, named::<ModelName>("cycle_c"));
        let tail = NodeRef::new(ModelKind::Emitter, named::<ModelName>("tail"));
        let topology = PlacementTopology {
            adjacency: HashMap::from_iter([
                (cycle_a.clone(), vec![cycle_b.clone(), tail]),
                (cycle_b.clone(), vec![cycle_c.clone()]),
                (cycle_c.clone(), vec![cycle_a.clone()]),
            ]),
            reverse: HashMap::from_iter([
                (cycle_a.clone(), vec![cycle_c.clone()]),
                (cycle_b.clone(), vec![cycle_a.clone()]),
                (cycle_c.clone(), vec![cycle_b.clone()]),
            ]),
        };

        let endpoint = topology.endpoint_analysis(cycle_a.clone(), cycle_a.clone());
        assert_eq!(
            endpoint.corridor,
            vec![cycle_a, cycle_b.clone(), cycle_c.clone()]
        );
        assert_eq!(
            endpoint
                .witnesses
                .iter()
                .map(|(captured, _)| captured)
                .collect::<Vec<_>>(),
            vec![&cycle_b, &cycle_c]
        );
        assert!(endpoint.witnesses.iter().all(|(_, path)| {
            path.first() == path.last()
                && path
                    .first()
                    .is_some_and(|member| member.identifier == named("cycle_a"))
        }));
    }
}
