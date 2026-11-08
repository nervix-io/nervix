//! Exact execution units selected for a relocation.
//!
//! Layer: decisions.
//! - **Owns.** Selection, hard-group closure, preference capture and override validation.
//! - **Depends on.** Validated topology, unit correspondence and vocabulary identities.
//! - **Must not know.** Cluster liveness, runtime state, locks or transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the boundary declared above")
)]

use std::{collections::BTreeSet, fmt};

use ahash::{HashMap, HashSet};
use error_stack::Report;
use nervix_models::{
    DomainName, ModelName, NodeRef, NodeUnitRef, PlacementName, PlacementPolicy, RelocationMember,
    RelocationPreferenceOverride, RelocationPreferenceStrategy, RelocationSelection,
    RelocationUnitSelection, Unit,
};
use strum::AsRefStr;
use thiserror::Error;

use super::{
    graph::ActiveGraph,
    placement::{EffectivePlacementPlan, PlacementPair, PlacementTopology},
    unit_placement::{UnitLayout, UnitPlacementGroups},
};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum RelocationMemberReason {
    Selected,
    Required,
    Preferred,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelocationCoverage {
    pub(crate) source: NodeRef,
    pub(crate) destination: NodeRef,
    pub(crate) connected: bool,
    pub(crate) covered: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelocationUnitMember {
    pub(crate) runtime_unit: NodeUnitRef,
    pub(crate) group: usize,
    pub(crate) strategy: RelocationPreferenceStrategy,
    pub(crate) reason: RelocationMemberReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelocationPreference {
    pub(crate) policy: PlacementPolicy,
    pub(crate) left: NodeUnitRef,
    pub(crate) right: NodeUnitRef,
    pub(crate) winning_rules: Vec<PlacementName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelocationUnit {
    pub(crate) coverage: Vec<RelocationCoverage>,
    pub(crate) members: Vec<RelocationUnitMember>,
    pub(crate) preferences: Vec<RelocationPreference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RelocationPlanError {
    #[error("{kind} '{name}' does not exist in domain '{domain}'", kind = .member.kind.as_str(), name = .member.identifier.as_str())]
    UnknownRuntimeNode { domain: DomainName, member: NodeRef },
    #[error("{kind} '{name}' has no {selection} unit in domain '{domain}'", kind = .member.kind.as_str(), name = .member.identifier.as_str())]
    UnknownUnit {
        domain: DomainName,
        member: NodeRef,
        selection: RelocationUnitSelection,
    },
    #[error(
        "ingestor '{ingestor}' cannot be relocated: server-listener ingestors execute on every \
         cluster node"
    )]
    ServerListenerIngestor { ingestor: ModelName },
    #[error("relocation covers no runtime node: no FROM/TO pair is connected")]
    DisconnectedCorridor,
    #[error("conflicting preference strategies for hard group [{members}]", members = HardGroupMembers(.members))]
    ConflictingGroupStrategies { members: Vec<NodeUnitRef> },
    #[error("{kind} '{name}'{unit} is not part of the relocation", kind = .member.node.kind.as_str(), name = .member.node.identifier, unit = .member.unit.label_suffix())]
    OverrideOutsideUnit { member: NodeUnitRef },
}

struct HardGroupMembers<'a>(&'a [NodeUnitRef]);
impl fmt::Display for HardGroupMembers<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, member) in self.0.iter().enumerate() {
            if index != 0 {
                formatter.write_str(", ")?;
            }
            write!(
                formatter,
                "{} {}{}",
                member.node.kind.as_str(),
                member.node.identifier,
                member.unit.label_suffix()
            )?;
        }
        Ok(())
    }
}

struct UnitGroup {
    members: Vec<NodeUnitRef>,
    strategy: RelocationPreferenceStrategy,
    reason: RelocationMemberReason,
}

impl ActiveGraph {
    pub(crate) fn relocation_unit(
        &self,
        domain: &DomainName,
        selection: &RelocationSelection,
        default_strategy: RelocationPreferenceStrategy,
        overrides: &[RelocationPreferenceOverride],
    ) -> error_stack::Result<RelocationUnit, RelocationPlanError> {
        let mut override_by_key =
            HashMap::<NodeUnitRef, Vec<RelocationPreferenceStrategy>>::default();
        for clause in overrides {
            for member in self.resolve_relocation_member(domain, &clause.member)? {
                override_by_key
                    .entry(member)
                    .or_default()
                    .push(clause.strategy);
            }
        }
        let (selected, coverage) = match selection {
            RelocationSelection::List(members) => {
                let mut selected = Vec::new();
                let mut seen = HashSet::default();
                for member in members {
                    for unit in self.resolve_relocation_member(domain, member)? {
                        if seen.insert(unit.clone()) {
                            selected.push(unit);
                        }
                    }
                }
                (selected, Vec::new())
            }
            RelocationSelection::Corridor { from, to } => {
                self.relocation_corridor_selection(domain, from, to)?
            }
        };
        let placement = self.placement.effective();
        let hard = UnitPlacementGroups::decide(self, &placement);
        let hard_group = |member: &NodeUnitRef| match hard.by_member.get(member) {
            Some(index) => hard.groups[*index].clone(),
            None => vec![member.clone()],
        };
        let mut groups = Vec::<UnitGroup>::new();
        let mut group_by_member = HashMap::default();
        for member in &selected {
            if !group_by_member.contains_key(member) {
                join_group(
                    &mut groups,
                    &mut group_by_member,
                    hard_group(member),
                    &override_by_key,
                    default_strategy,
                    RelocationMemberReason::Selected,
                )?;
            }
        }
        capture_preferred_groups(
            &placement,
            &self.placement.topology,
            &hard_group,
            &override_by_key,
            default_strategy,
            &mut groups,
            &mut group_by_member,
        )?;
        if let Some(member) = override_by_key
            .keys()
            .filter(|member| !group_by_member.contains_key(*member))
            .min()
        {
            return Err(Report::new(RelocationPlanError::OverrideOutsideUnit {
                member: member.clone(),
            }));
        }
        let selected_set = selected.iter().collect::<HashSet<_>>();
        let selected_set = &selected_set;
        let members = groups
            .iter()
            .enumerate()
            .flat_map(|(index, group)| {
                group
                    .members
                    .iter()
                    .map(move |member| RelocationUnitMember {
                        runtime_unit: member.clone(),
                        group: index + 1,
                        strategy: group.strategy,
                        reason: if group.reason == RelocationMemberReason::Selected
                            && !selected_set.contains(member)
                        {
                            RelocationMemberReason::Required
                        } else {
                            group.reason
                        },
                    })
            })
            .collect();
        let preferences = unit_preferences(&placement, &group_by_member);
        Ok(RelocationUnit {
            coverage,
            members,
            preferences,
        })
    }

    fn resolve_relocation_node(
        &self,
        domain: &DomainName,
        member: &NodeRef,
    ) -> error_stack::Result<UnitLayout, RelocationPlanError> {
        let node = self.node(member.kind, &member.identifier).ok_or_else(|| {
            Report::new(RelocationPlanError::UnknownRuntimeNode {
                domain: domain.clone(),
                member: member.clone(),
            })
        })?;
        if node.config.executes_on_every_cluster_node() {
            return Err(Report::new(RelocationPlanError::ServerListenerIngestor {
                ingestor: member.identifier.clone(),
            }));
        }
        Ok(UnitLayout::of(node))
    }

    fn resolve_relocation_member(
        &self,
        domain: &DomainName,
        member: &RelocationMember,
    ) -> error_stack::Result<Vec<NodeUnitRef>, RelocationPlanError> {
        let layout = self.resolve_relocation_node(domain, &member.node)?;
        let selected = match member.selection {
            RelocationUnitSelection::All => {
                return Ok(layout
                    .units()
                    .map(|unit| NodeUnitRef::new(member.node.clone(), unit))
                    .collect());
            }
            RelocationUnitSelection::Partition(index) => Some(Unit::partition(index)),
            RelocationUnitSelection::Instance(index) => Unit::try_instance(index).ok(),
        };
        if let Some(unit) = selected.filter(|unit| layout.contains(*unit)) {
            return Ok(vec![NodeUnitRef::new(member.node.clone(), unit)]);
        }
        Err(Report::new(RelocationPlanError::UnknownUnit {
            domain: domain.clone(),
            member: member.node.clone(),
            selection: member.selection,
        }))
    }

    fn relocation_corridor_selection(
        &self,
        domain: &DomainName,
        from: &[NodeRef],
        to: &[NodeRef],
    ) -> error_stack::Result<(Vec<NodeUnitRef>, Vec<RelocationCoverage>), RelocationPlanError> {
        // Resolve every endpoint, including a destination after a disconnected source.
        for member in from.iter().chain(to) {
            self.resolve_relocation_node(domain, member)?;
        }
        let mut selected = BTreeSet::new();
        let mut coverage = Vec::new();
        for source in from {
            for destination in to {
                let endpoint = self
                    .placement
                    .topology
                    .endpoint_analysis(source.clone(), destination.clone());
                for member in &endpoint.corridor {
                    let layout = self.resolve_relocation_node(domain, member)?;
                    selected.extend(
                        layout
                            .units()
                            .map(|unit| NodeUnitRef::new(member.clone(), unit)),
                    );
                }
                coverage.push(RelocationCoverage {
                    source: source.clone(),
                    destination: destination.clone(),
                    connected: !endpoint.corridor.is_empty(),
                    covered: endpoint.corridor.len(),
                });
            }
        }
        if !coverage.iter().any(|pair| pair.connected) {
            return Err(Report::new(RelocationPlanError::DisconnectedCorridor));
        }
        Ok((selected.into_iter().collect(), coverage))
    }
}

fn capture_preferred_groups(
    placement: &EffectivePlacementPlan,
    topology: &PlacementTopology,
    hard_group: &impl Fn(&NodeUnitRef) -> Vec<NodeUnitRef>,
    override_by_key: &HashMap<NodeUnitRef, Vec<RelocationPreferenceStrategy>>,
    default_strategy: RelocationPreferenceStrategy,
    groups: &mut Vec<UnitGroup>,
    group_by_member: &mut HashMap<NodeUnitRef, usize>,
) -> error_stack::Result<(), RelocationPlanError> {
    loop {
        let mut candidates = BTreeSet::new();
        for (member, index) in group_by_member.iter() {
            if !groups[*index].strategy.follows_preferences() {
                continue;
            }
            for partner in unit_partners(
                placement,
                topology,
                member,
                PlacementPolicy::PreferColocation,
            ) {
                if !group_by_member.contains_key(&partner) {
                    candidates.insert(partner);
                }
            }
        }
        let mut joined = false;
        for member in candidates {
            if group_by_member.contains_key(&member) {
                continue;
            }
            let candidate = hard_group(&member);
            let separated = candidate.iter().any(|outside| {
                unit_partners(
                    placement,
                    topology,
                    outside,
                    PlacementPolicy::SuggestSeparation,
                )
                .iter()
                .any(|inside| {
                    group_by_member
                        .get(inside)
                        .is_some_and(|index| groups[*index].strategy.follows_preferences())
                })
            });
            if separated {
                continue;
            }
            join_group(
                groups,
                group_by_member,
                candidate,
                override_by_key,
                default_strategy,
                RelocationMemberReason::Preferred,
            )?;
            joined = true;
        }
        if !joined {
            break;
        }
    }
    Ok(())
}

fn unit_preferences(
    placement: &EffectivePlacementPlan,
    group_by_member: &HashMap<NodeUnitRef, usize>,
) -> Vec<RelocationPreference> {
    let mut preferences = Vec::new();
    let mut selected_by_node = HashMap::<&NodeRef, Vec<Unit>>::default();
    for (pair, resolved) in &placement.pairs {
        if matches!(
            resolved.policy,
            PlacementPolicy::PreferColocation | PlacementPolicy::SuggestSeparation
        ) {
            selected_by_node.entry(&pair.left).or_default();
            selected_by_node.entry(&pair.right).or_default();
        }
    }
    if selected_by_node.is_empty() {
        return preferences;
    }
    for member in group_by_member.keys() {
        if let Some(units) = selected_by_node.get_mut(&member.node) {
            units.push(member.unit);
        }
    }
    for (pair, resolved) in &placement.pairs {
        if !matches!(
            resolved.policy,
            PlacementPolicy::PreferColocation | PlacementPolicy::SuggestSeparation
        ) {
            continue;
        }
        let mut append = |left, right| {
            preferences.push(RelocationPreference {
                policy: resolved.policy,
                left: NodeUnitRef::new(pair.left.clone(), left),
                right: NodeUnitRef::new(pair.right.clone(), right),
                winning_rules: resolved.winning_rules.clone(),
            });
        };
        // Enumerate partners of selected units in both directions. A narrow relocation never
        // walks all sibling partitions of every soft hop merely to discard their preferences.
        for left in selected_by_node.get(&pair.left).into_iter().flatten() {
            for right in resolved.correspondence.partners(true, *left) {
                append(*left, right);
            }
        }
        for right in selected_by_node.get(&pair.right).into_iter().flatten() {
            for left in resolved.correspondence.partners(false, *right) {
                if !group_by_member.contains_key(&NodeUnitRef::new(pair.left.clone(), left)) {
                    append(left, *right);
                }
            }
        }
    }
    preferences.sort_by(|a, b| {
        a.policy
            .as_ref()
            .cmp(b.policy.as_ref())
            .then_with(|| a.left.cmp(&b.left))
            .then_with(|| a.right.cmp(&b.right))
    });
    preferences
}

fn join_group(
    groups: &mut Vec<UnitGroup>,
    by_member: &mut HashMap<NodeUnitRef, usize>,
    members: Vec<NodeUnitRef>,
    overrides: &HashMap<NodeUnitRef, Vec<RelocationPreferenceStrategy>>,
    default: RelocationPreferenceStrategy,
    reason: RelocationMemberReason,
) -> error_stack::Result<(), RelocationPlanError> {
    let strategies = members
        .iter()
        .flat_map(|member| overrides.get(member).into_iter().flatten())
        .copied()
        .collect::<HashSet<_>>();
    if strategies.len() > 1 {
        return Err(Report::new(
            RelocationPlanError::ConflictingGroupStrategies { members },
        ));
    }
    let strategy = strategies.into_iter().next().unwrap_or(default);
    for member in &members {
        by_member.insert(member.clone(), groups.len());
    }
    groups.push(UnitGroup {
        members,
        strategy,
        reason,
    });
    Ok(())
}

fn unit_partners(
    placement: &EffectivePlacementPlan,
    topology: &PlacementTopology,
    member: &NodeUnitRef,
    policy: PlacementPolicy,
) -> Vec<NodeUnitRef> {
    let mut partners = BTreeSet::new();
    for (other, from_left) in topology.adjacent_hops(&member.node) {
        let pair = if from_left {
            PlacementPair {
                left: member.node.clone(),
                right: other.clone(),
            }
        } else {
            PlacementPair {
                left: other.clone(),
                right: member.node.clone(),
            }
        };
        let Some(resolved) = placement
            .pairs
            .get(&pair)
            .filter(|resolved| resolved.policy == policy)
        else {
            continue;
        };
        partners.extend(
            resolved
                .correspondence
                .partners(from_left, member.unit)
                .map(|unit| NodeUnitRef::new(other.clone(), unit)),
        );
    }
    partners.into_iter().collect()
}
