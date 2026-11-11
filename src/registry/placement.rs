//! What a placement rule claims, and whether the graph can satisfy it.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving a rule's members, the pairs and corridors it claims, the require groups
//!   it binds together, and the plan a caller reads back.
//! - **Depends on.** The active graph and the Models the rules name.
//! - **Must not know.** Which cluster member a plan ends up assigning.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    num::NonZeroU64,
};

use ahash::{HashMap, HashMapExt, HashSet};
use error_stack::Report;
use meticulous::OptionExt;
use nervix_models::{
    CreatePlacement, DomainName, Model, ModelIndex, ModelKind, ModelName, NodeRef, NodeUnitRef,
    PlacementName, PlacementPolicy, PlacementScope, RelayName,
};
use petgraph::{Direction, graph::DiGraph, prelude::NodeIndex, visit::EdgeRef};

use crate::registry::{
    error::RegistryError,
    graph::{ActiveGraph, ActiveNode, EdgeKind},
    unit_placement::{HopClass, UnitCorrespondence, UnitPlacementGroups},
    validation::materialized_state::model_materialized_state_dependencies,
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementPlan {
    pub(crate) rules: Vec<PlacementRulePlan>,
    pub(crate) effective_pairs: Vec<PlacementEffectivePair>,
    pub(crate) require_groups: Vec<PlacementRequireGroupPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementRulePlan {
    pub(crate) name: ModelName,
    pub(crate) scope: PlacementScope,
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
    pub(crate) class: HopClass,
    pub(crate) winning_rules: Vec<PlacementName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementEffectivePair {
    pub(crate) left: NodeRef,
    pub(crate) right: NodeRef,
    pub(crate) policy: PlacementPolicy,
    pub(crate) winning_rules: Vec<PlacementName>,
    pub(crate) class: HopClass,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementRequireGroupPlan {
    pub(crate) members: Vec<NodeUnitRef>,
    pub(crate) bonds: Vec<PlacementEffectivePair>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::registry) struct PlacementPair {
    pub(in crate::registry) left: NodeRef,
    pub(in crate::registry) right: NodeRef,
}

impl PlacementPair {
    fn new(left: NodeRef, right: NodeRef) -> Option<Self> {
        (left != right).then_some(Self { left, right })
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
    pub(crate) policy: PlacementPolicy,
    pub(in crate::registry) winning_rules: Vec<PlacementName>,
    pub(in crate::registry) correspondence: UnitCorrespondence,
}

#[derive(Debug, Clone, Default)]
pub(in crate::registry) struct PlacementAnalysis {
    rules: Vec<PlacementRuleAnalysis>,
    explicit_pairs: HashMap<PlacementPair, ResolvedPlacementPair>,
    /// Retained so a relocation corridor can be covered between endpoints no placement rule
    /// names, using the same path-gated coverage the rules use.
    pub(in crate::registry) topology: PlacementTopology,
}

#[derive(Debug, Clone)]
pub(in crate::registry) struct EffectivePlacementPlan {
    pub(in crate::registry) pairs: HashMap<PlacementPair, ResolvedPlacementPair>,
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

        let direct_pairs = topology.direct_pairs();
        let mut rules = Vec::with_capacity(placement_models.len());
        let mut claims_by_pair = HashMap::<PlacementPair, Vec<PlacementClaim>>::new();
        for placement in placement_models {
            placement.validate().map_err(|rejection| {
                RegistryError::invalid_model(domain, placement.name.as_str(), rejection)
            })?;
            let placement_index = indices
                .get(&NodeRef::new(ModelKind::Placement, placement.name.clone()))
                .copied()
                .verified("the pass above added a graph node for every placement");
            let mut endpoint_pairs = Vec::new();
            let mut claimed_pairs = HashSet::default();
            match &placement.scope {
                PlacementScope::DomainWide => claimed_pairs.extend(direct_pairs.iter().cloned()),
                PlacementScope::Corridor { from, to } => {
                    let from = resolve_placement_members(domain, &placement, from, models)?;
                    let to = resolve_placement_members(domain, &placement, to, models)?;
                    let mut pinned = HashSet::default();
                    for member in from.iter().chain(&to) {
                        if pinned.insert(member.pin.clone()) {
                            let pin_index = indices
                                .get(&member.pin)
                                .copied()
                                .verified("resolved placement pins belong to this graph");
                            graph.add_edge(pin_index, placement_index, EdgeKind::RequiredBy);
                        }
                    }
                    for source in &from {
                        for destination in &to {
                            let endpoint = topology.endpoint_analysis(
                                source.runtime.clone(),
                                destination.runtime.clone(),
                            );
                            let corridor = endpoint.corridor.iter().collect::<HashSet<_>>();
                            claimed_pairs.extend(
                                direct_pairs
                                    .iter()
                                    .filter(|pair| {
                                        corridor.contains(&pair.left)
                                            && corridor.contains(&pair.right)
                                    })
                                    .cloned(),
                            );
                            endpoint_pairs.push(endpoint);
                        }
                    }
                }
            }
            for pair in &claimed_pairs {
                claims_by_pair
                    .entry(pair.clone())
                    .or_default()
                    .push(PlacementClaim {
                        rule: placement.name.clone(),
                        policy: placement.policy,
                        rank: placement.rank,
                    });
            }
            rules.push(PlacementRuleAnalysis {
                model: placement,
                endpoint_pairs,
                claimed_pairs,
            });
        }

        let mut ordered_claims = claims_by_pair.into_iter().collect::<Vec<_>>();
        ordered_claims.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut explicit_pairs = HashMap::default();
        for (pair, claims) in ordered_claims {
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
            let correspondence = hop_correspondence(&pair, indices, graph);
            if !correspondence.class.permits(policy) {
                return Err(Report::new(RegistryError::PlacementHopPolicy {
                    domain: domain.clone(),
                    from: pair.left.clone(),
                    to: pair.right.clone(),
                    class: correspondence.class,
                    policy,
                    rules: winning_rules,
                }));
            }
            explicit_pairs.insert(
                pair,
                ResolvedPlacementPair {
                    policy,
                    winning_rules,
                    correspondence,
                },
            );
        }

        for pair in direct_pairs {
            let correspondence = hop_correspondence(&pair, indices, graph);
            explicit_pairs.entry(pair).or_insert(ResolvedPlacementPair {
                policy: PlacementPolicy::Neutral,
                winning_rules: Vec::new(),
                correspondence,
            });
        }
        Ok(Self {
            rules,
            explicit_pairs,
            topology,
        })
    }

    /// Refresh only unit correspondence after observation resolves an inherited count. Model
    /// corridors and their dependency edges stay as validated; no edge is added a second time.
    pub(in crate::registry) fn refresh_units(
        &mut self,
        domain: &DomainName,
        indices: &HashMap<NodeRef, NodeIndex>,
        graph: &DiGraph<ActiveNode, EdgeKind>,
    ) -> Result<(), Report<RegistryError>> {
        let mut pairs = self.explicit_pairs.iter_mut().collect::<Vec<_>>();
        pairs.sort_by_key(|(left, _)| *left);
        for (pair, resolved) in pairs {
            let correspondence = hop_correspondence(pair, indices, graph);
            if !correspondence.class.permits(resolved.policy) {
                return Err(Report::new(RegistryError::PlacementHopPolicy {
                    domain: domain.clone(),
                    from: pair.left.clone(),
                    to: pair.right.clone(),
                    class: correspondence.class,
                    policy: resolved.policy,
                    rules: resolved.winning_rules.clone(),
                }));
            }
            resolved.correspondence = correspondence;
        }
        Ok(())
    }

    pub(in crate::registry) fn effective(&self) -> EffectivePlacementPlan {
        EffectivePlacementPlan {
            pairs: self.explicit_pairs.clone(),
        }
    }

    pub(in crate::registry) fn plan(&self, graph: &ActiveGraph) -> PlacementPlan {
        let effective = self.effective();
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
                            class: resolved.correspondence.class,
                            winning_rules: resolved.winning_rules.clone(),
                        }
                    })
                    .collect::<Vec<_>>();
                claims.sort_by(placement_rule_claim_cmp);
                PlacementRulePlan {
                    name: ModelName::from(&rule.model.name),
                    scope: rule.model.scope.clone(),
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

        let unit_groups = UnitPlacementGroups::decide(graph, &effective);
        let bonds = placement_group_bonds(&unit_groups, &effective);
        let require_groups = unit_groups
            .groups
            .iter()
            .zip(bonds)
            .map(|(members, mut bonds)| {
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

/// Every left unit on a hard hop is bonded to its corresponding right unit. Index the groups
/// containing that left node once, and emit the hop once for each of those groups.
fn placement_group_bonds(
    groups: &UnitPlacementGroups,
    effective: &EffectivePlacementPlan,
) -> Vec<Vec<PlacementEffectivePair>> {
    let mut by_node = HashMap::<&NodeRef, Vec<usize>>::default();
    for (index, members) in groups.groups.iter().enumerate() {
        for member in members {
            let indices = by_node.entry(&member.node).or_default();
            if indices.last() != Some(&index) {
                indices.push(index);
            }
        }
    }
    let mut bonds = vec![Vec::new(); groups.groups.len()];
    for (pair, resolved) in &effective.pairs {
        if resolved.policy != PlacementPolicy::RequireColocation {
            continue;
        }
        let indices = by_node
            .get(&pair.left)
            .verified("every validated hard hop belongs to a unit component");
        let bond = placement_effective_pair(pair, resolved);
        for index in indices {
            bonds[*index].push(bond.clone());
        }
    }
    bonds
}

fn hop_correspondence(
    pair: &PlacementPair,
    indices: &HashMap<NodeRef, NodeIndex>,
    graph: &DiGraph<ActiveNode, EdgeKind>,
) -> UnitCorrespondence {
    let left = graph
        .node_weight(indices[&pair.left])
        .verified("a direct hop belongs to the validated graph");
    let right = graph
        .node_weight(indices[&pair.right])
        .verified("a direct hop belongs to the validated graph");
    UnitCorrespondence::decide(left, right)
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

    pub(in crate::registry) fn adjacent_hops<'a>(
        &'a self,
        subject: &'a NodeRef,
    ) -> impl Iterator<Item = (&'a NodeRef, bool)> {
        self.adjacency
            .get(subject)
            .into_iter()
            .flatten()
            .map(|other| (other, true))
            .chain(
                self.reverse
                    .get(subject)
                    .into_iter()
                    .flatten()
                    .map(|other| (other, false)),
            )
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
        match PlacementMemberEligibility::of(model) {
            PlacementMemberEligibility::EveryClusterNode => {
                cluster_wide_ingestor = true;
            }
            PlacementMemberEligibility::Eligible => {
                eligible.push(ResolvedPlacementMember {
                    runtime: key.clone(),
                    pin: key.clone(),
                });
            }
            PlacementMemberEligibility::NotRuntime => ineligible_kinds.push(key.kind),
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

/// Whether a placement rule may name a Model as a `FROM` or `TO` member. Validation, the REPL's
/// member completion and the console's member choices all ask this one rule, so none of them
/// offers or accepts a member another refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlacementMemberEligibility {
    /// A schedulable runtime node, whose units a rule can keep together or apart.
    Eligible,
    /// A server-listener ingestor, which executes on every cluster node, so no rule places it.
    EveryClusterNode,
    /// An entity that is not a schedulable runtime node.
    NotRuntime,
}

impl PlacementMemberEligibility {
    /// The eligibility of `model`, whatever version its resource bindings carry, so a stored
    /// graph, a candidate graph and a session's transaction prefix are judged alike. Every kind
    /// is decided here explicitly, so a new kind cannot become a member by default.
    fn of<Version>(model: &Model<Version>) -> Self {
        if let Model::Ingestor(ingestor) = model
            && ingestor.input.executes_on_every_cluster_node()
        {
            return Self::EveryClusterNode;
        }
        match model.kind() {
            ModelKind::Generator
            | ModelKind::Inferencer
            | ModelKind::WasmProcessor
            | ModelKind::Ingestor
            | ModelKind::Reingestor
            | ModelKind::Relay
            | ModelKind::Lookup
            | ModelKind::Junction
            | ModelKind::Deduplicator
            | ModelKind::Correlator
            | ModelKind::Reorderer
            | ModelKind::WindowProcessor
            | ModelKind::Emitter => Self::Eligible,
            ModelKind::Schema
            | ModelKind::WireJsonSchema
            | ModelKind::WireCborSchema
            | ModelKind::WireAvroSchema
            | ModelKind::Codec
            | ModelKind::Client
            | ModelKind::Vhost
            | ModelKind::Branch
            | ModelKind::Endpoint
            | ModelKind::SignalingProtocol
            | ModelKind::Placement
            | ModelKind::Udf => Self::NotRuntime,
        }
    }
}

/// The runtime nodes a placement rule may name, ordered by name. A member is named without its
/// kind, so an eligible name another eligible Model shares is ambiguous and is not offered; the
/// rule's validation refuses it in the same way.
pub(crate) fn placement_members<'a, Version: 'a>(
    models: impl IntoIterator<Item = &'a Model<Version>>,
) -> Vec<NodeRef> {
    let mut by_name = BTreeMap::<ModelName, Vec<NodeRef>>::new();
    for model in models {
        if PlacementMemberEligibility::of(model) == PlacementMemberEligibility::Eligible {
            by_name
                .entry(model.name())
                .or_default()
                .push(model.node_ref());
        }
    }
    let mut members = Vec::with_capacity(by_name.len());
    for candidates in by_name.into_values() {
        if let [member] = candidates.as_slice() {
            members.push(member.clone());
        }
    }
    members
}

fn is_placement_runtime_model(model: &Model) -> bool {
    PlacementMemberEligibility::of(model) == PlacementMemberEligibility::Eligible
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
        class: resolved.correspondence.class,
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

#[cfg(test)]
mod tests {
    use std::fs;

    use meticulous::ResultExt as _;
    use nervix_models::{
        AlterPlacement, AlterPlacementOperation, ClusterNodeName, DropModel, HttpEndpointIngestor,
        HttpIngestor, IngestorEntity, MaterializedRelayState, MaterializedStateDependency,
        MaterializedStatePolicy, SyslogIngestor, SyslogListenerIngestor,
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
    fn placement_corridor_claims_direct_hops_and_reports_witnesses() {
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
            .placement_plan();
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
        assert_eq!(
            rule.claims.len(),
            4,
            "the five runtime nodes are joined by four direct hops"
        );
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
            .placement_plan();
        assert_eq!(plan.rules.len(), 1);
        assert!(!plan.rules[0].endpoint_pairs[0].connected);
        assert!(plan.rules[0].endpoint_pairs[0].corridor.is_empty());
        assert!(plan.rules[0].claims.is_empty());
        assert!(plan.require_groups.is_empty());

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn placement_inspection_keeps_partition_bonds_aligned_and_deduplicates_a_fiber() {
        use std::collections::BTreeSet;

        use nervix_models::{BranchPartitions, UnitKind};

        for target in ["p99", "emit"] {
            let path = temp_db_path();
            let registry = Registry::open(&path).assured("registry opens");
            let domain = DomainName::parse("partition_bonds").assured("valid domain");
            let mut models = full_graph_batch();
            for model in &mut models {
                if let Model::Branch(branch) = model {
                    branch.partitions = BranchPartitions::Direct(nonzero!(2u16));
                }
            }
            models.push(placement(
                "local_units",
                &["notifications"],
                &[target],
                PlacementPolicy::RequireColocation,
                None,
            ));
            registry.apply_batch(&domain, models).assured("valid graph");
            let plan = registry
                .active_graph(&domain)
                .assured("active graph")
                .placement_plan();
            let fiber = target == "emit";
            assert_eq!(plan.require_groups.len(), if fiber { 1 } else { 2 });
            for group in &plan.require_groups {
                assert_eq!(group.members.len(), if fiber { 7 } else { 3 });
                let partitions = group
                    .members
                    .iter()
                    .filter_map(|member| match member.unit.kind() {
                        UnitKind::Partition(index) => Some(index),
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>();
                assert_eq!(partitions.len(), if fiber { 2 } else { 1 });
                let mut expected =
                    BTreeSet::from([("notifications", "p99_proc"), ("p99_proc", "p99")]);
                if fiber {
                    expected.insert(("p99", "emit"));
                }
                let actual = group
                    .bonds
                    .iter()
                    .map(|bond| {
                        (
                            bond.left.identifier.as_str(),
                            bond.right.identifier.as_str(),
                        )
                    })
                    .collect::<BTreeSet<_>>();
                assert_eq!(actual, expected);
                assert_eq!(group.bonds.len(), expected.len());
            }
            drop(registry);
            fs::remove_dir_all(path).assured("remove test database");
        }
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
            .placement_plan();
        let effective = plan
            .effective_pairs
            .iter()
            .find(|pair| {
                let names = [
                    pair.left.identifier.as_str(),
                    pair.right.identifier.as_str(),
                ];
                names.contains(&"ing") && names.contains(&"notifications")
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
            left_kind,
            left_identifier,
            right_kind,
            right_identifier,
        } = error.current_context()
        else {
            panic!("unexpected error: {error:#}");
        };
        assert_eq!(error_domain, domain.as_str());
        assert_eq!([left_rule.as_str(), right_rule.as_str()], ["cut", "glue"]);
        assert_eq!([*left_kind, *right_kind], ["ingestor", "relay"]);
        assert_eq!(
            [left_identifier.as_str(), right_identifier.as_str()],
            ["ing", "notifications"]
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
            .placement_plan();
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
            .placement_plan();
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
        let nervix_models::IngestorInput::Transport(transport) = &mut ingestor.input else {
            panic!("the full graph's ingestor reads a transport");
        };
        transport.source = IngestorEntity::Http(HttpIngestor::Endpoint(HttpEndpointIngestor {
            endpoint: named("ingest_http"),
            mode: nervix_models::EndpointIngestMode::NoAck,
            quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                max_size: "1MiB".to_string(),
            },
        }));
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
        let nervix_models::IngestorInput::Transport(transport) = &mut ingestor.input else {
            panic!("the full graph's ingestor reads a transport");
        };
        transport.source =
            IngestorEntity::Syslog(SyslogIngestor::Listener(SyslogListenerIngestor {
                client: named("syslog_listener"),
                mode: nervix_models::SyslogIngestMode::NoAck,
                quiesce: nervix_models::IngestQuiesceMode::Suspend,
            }));
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
    fn placement_members_list_exactly_the_unambiguous_eligible_runtime_nodes() {
        let mut models = full_graph_batch();
        let mut listener = models
            .iter()
            .find_map(|model| match model {
                Model::Ingestor(ingestor) if ingestor.name == named("ing") => {
                    Some(ingestor.clone())
                }
                _ => None,
            })
            .expect("full graph must contain ing");
        listener.name = named("listener");
        let nervix_models::IngestorInput::Transport(transport) = &mut listener.input else {
            panic!("the full graph's ingestor reads a transport");
        };
        transport.source = IngestorEntity::Http(HttpIngestor::Endpoint(HttpEndpointIngestor {
            endpoint: named("ingest_http"),
            mode: nervix_models::EndpointIngestMode::NoAck,
            quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                max_size: "1MiB".to_string(),
            },
        }));
        let listener = Model::Ingestor(listener);
        assert_eq!(
            PlacementMemberEligibility::of(&listener),
            PlacementMemberEligibility::EveryClusterNode
        );
        models.extend([
            vhost("public", &["events.example.com"]),
            endpoint(
                "ingest_http",
                "public",
                "/ingest",
                nervix_models::EndpointType::Http,
            ),
            listener,
            // A relay named like the junction makes that name ambiguous on both sides.
            relay("p99_proc", "event_schema"),
        ]);

        let members = placement_members(&models);

        assert_eq!(
            members,
            vec![
                NodeRef::new(ModelKind::Emitter, named::<ModelName>("emit")),
                NodeRef::new(ModelKind::Ingestor, named::<ModelName>("ing")),
                NodeRef::new(ModelKind::Relay, named::<ModelName>("notifications")),
                NodeRef::new(ModelKind::Relay, named::<ModelName>("p99")),
            ],
            "the branch sharing a relay's name is no runtime node, so that relay stays listed"
        );
        for model in &models {
            if let Model::Schema(_) | Model::Codec(_) | Model::Vhost(_) | Model::Endpoint(_) = model
            {
                assert_eq!(
                    PlacementMemberEligibility::of(model),
                    PlacementMemberEligibility::NotRuntime
                );
            }
        }
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
    fn domain_wide_require_forms_a_connected_component_from_per_hop_claims() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("placement_domain_require").assured("valid domain");
        let mut models = full_graph_batch();
        models.push(Model::Placement(CreatePlacement::domain_wide(
            named("domain_rule"),
            PlacementPolicy::RequireColocation,
            None,
        )));
        registry
            .apply_batch(&domain, models)
            .expect("graph should validate");
        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let plan = graph.placement_plan();

        assert_eq!(
            plan.effective_pairs.len(),
            4,
            "the named rule covers direct hops"
        );
        assert!(
            plan.effective_pairs
                .iter()
                .all(|pair| pair.winning_rules == vec![named("domain_rule")])
        );
        assert_eq!(plan.require_groups.len(), 1);
        assert_eq!(plan.require_groups[0].members.len(), 5);
        let schedule = graph
            .schedule_for_domain(
                &domain,
                &[
                    ClusterNodeName::parse("node-1").expect("valid name"),
                    ClusterNodeName::parse("node-2").expect("valid name"),
                ],
                0,
                None,
                &crate::registry::uniform_capabilities(&[
                    ClusterNodeName::parse("node-1").expect("valid name"),
                    ClusterNodeName::parse("node-2").expect("valid name"),
                ]),
            )
            .assured("the fixture topology resolves");
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
