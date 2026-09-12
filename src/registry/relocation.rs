//! Building the unit a relocation moves.
//!
//! The unit is a graph question: which runtime nodes the statement selects, which hard groups
//! those selections belong to, and which further groups their preferences capture. Owners,
//! replicas, cluster liveness, and execution belong to the command that consumes this plan.

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use meticulous::OptionExt as _;
use nervix_models::{
    Model, NodeRef, PlacementName, PlacementPolicy, RelocationMember, RelocationPreferenceOverride,
    RelocationPreferenceStrategy, RelocationSelection,
};
use strum::AsRefStr;
use thiserror::Error;

use super::graph::ActiveGraph;
use crate::registry::placement::{EffectivePlacementPlan, PlacementPair, ResolvedPlacementPair};

/// Why a runtime node is part of the unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum RelocationMemberReason {
    /// Named by the selection, directly or through corridor coverage.
    Selected,
    /// A hard-group mate of a selected runtime node.
    Required,
    /// Captured through a `PREFER COLOCATION` partner of a `FOLLOW PREFERENCES` group.
    Preferred,
}

/// One `FROM`/`TO` endpoint pair of a corridor selection and what it covered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelocationCoverage {
    pub(crate) source: NodeRef,
    pub(crate) destination: NodeRef,
    pub(crate) connected: bool,
    pub(crate) covered: usize,
}

/// One runtime node in the unit, with the hard group it belongs to and the strategy that group
/// carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelocationUnitMember {
    pub(crate) runtime_node: NodeRef,
    pub(crate) group: usize,
    pub(crate) strategy: RelocationPreferenceStrategy,
    pub(crate) reason: RelocationMemberReason,
}

/// An effective soft preference with at least one endpoint in the unit. The relocation reports it
/// as unsatisfied when the owners after the move disagree with the policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelocationPreference {
    pub(crate) policy: PlacementPolicy,
    pub(crate) left: NodeRef,
    pub(crate) right: NodeRef,
    pub(crate) winning_rules: Vec<PlacementName>,
    pub(in crate::registry) from_domain_default: bool,
}

/// The graph-side result of planning a relocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelocationUnit {
    /// One entry per `FROM`/`TO` pair; empty for the list selection form.
    pub(crate) coverage: Vec<RelocationCoverage>,
    /// Unit members ordered by hard group, selected groups before captured ones.
    pub(crate) members: Vec<RelocationUnitMember>,
    /// Every effective `PREFER COLOCATION` and `SUGGEST SEPARATION` relationship touching the
    /// unit, in canonical order.
    pub(crate) preferences: Vec<RelocationPreference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RelocationPlanError {
    #[error("{kind} '{name}' does not exist in domain '{domain}'")]
    UnknownRuntimeNode {
        domain: String,
        kind: &'static str,
        name: String,
    },
    #[error(
        "ingestor '{name}' cannot be relocated: server-listener ingestors execute on every \
         cluster node"
    )]
    ServerListenerIngestor { name: String },
    #[error("relocation covers no runtime node: no FROM/TO pair is connected")]
    DisconnectedCorridor,
    #[error("conflicting preference strategies for hard group [{members}]")]
    ConflictingGroupStrategies { members: String },
    #[error("{kind} '{name}' is not part of the relocation")]
    OverrideOutsideUnit { kind: &'static str, name: String },
}

/// A hard group while the unit is being built.
#[derive(Debug, Clone)]
struct UnitGroup {
    members: Vec<NodeRef>,
    strategy: RelocationPreferenceStrategy,
    reason: RelocationMemberReason,
}

impl ActiveGraph {
    /// Computes the unit a relocation moves from the domain's effective placement plan and the
    /// currently active graph.
    pub(crate) fn relocation_unit(
        &self,
        domain: &nervix_models::DomainName,
        default_policy: PlacementPolicy,
        selection: &RelocationSelection,
        default_strategy: RelocationPreferenceStrategy,
        overrides: &[RelocationPreferenceOverride],
    ) -> Result<RelocationUnit, RelocationPlanError> {
        // Every named runtime node is resolved before anything is planned, so a misspelled member
        // reports that it does not exist rather than a consequence of leaving it out.
        let mut resolved_overrides = Vec::with_capacity(overrides.len());
        let mut override_by_key = HashMap::new();
        for override_clause in overrides {
            let key = self.resolve_relocation_member(domain, &override_clause.member)?;
            override_by_key
                .entry(key.clone())
                .or_insert_with(Vec::new)
                .push(override_clause.strategy);
            resolved_overrides.push((key, &override_clause.member));
        }

        let (selected, coverage) = match selection {
            RelocationSelection::List(members) => {
                let mut selected = Vec::new();
                for member in members {
                    let key = self.resolve_relocation_member(domain, member)?;
                    if !selected.contains(&key) {
                        selected.push(key);
                    }
                }
                (selected, Vec::new())
            }
            RelocationSelection::Corridor { from, to } => {
                self.relocation_corridor_selection(domain, from, to)?
            }
        };

        let placement = self.placement.effective(default_policy);

        let hard_group = |key: &NodeRef| -> Vec<NodeRef> {
            match placement.group_by_member.get(key) {
                Some(group_index) => {
                    let mut members = placement.require_groups[*group_index].clone();
                    members.sort();
                    members
                }
                None => vec![key.clone()],
            }
        };

        let mut groups: Vec<UnitGroup> = Vec::new();
        let mut group_by_member = HashMap::new();
        let selected_set = selected.iter().cloned().collect::<HashSet<_>>();
        for key in &selected {
            if group_by_member.contains_key(key) {
                continue;
            }
            let members = hard_group(key);
            let strategy = Self::group_strategy(&members, &override_by_key, default_strategy)?;
            let index = groups.len();
            for member in &members {
                group_by_member.insert(member.clone(), index);
            }
            groups.push(UnitGroup {
                members,
                strategy,
                reason: RelocationMemberReason::Selected,
            });
        }

        self.capture_preferred_groups(
            &placement,
            &hard_group,
            &override_by_key,
            default_strategy,
            &mut groups,
            &mut group_by_member,
        )?;

        if let Some((_, member)) = resolved_overrides
            .iter()
            .find(|(key, _)| !group_by_member.contains_key(key))
        {
            return Err(RelocationPlanError::OverrideOutsideUnit {
                kind: member.kind.as_str(),
                name: member.name.as_str().to_string(),
            });
        }

        let selected_set = &selected_set;
        let members = groups
            .iter()
            .enumerate()
            .flat_map(|(index, group)| {
                group
                    .members
                    .iter()
                    .map(move |member| RelocationUnitMember {
                        runtime_node: member.clone(),
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
            .collect::<Vec<_>>();

        let preferences = Self::unit_preferences(&placement.pairs, &group_by_member);

        Ok(RelocationUnit {
            coverage,
            members,
            preferences,
        })
    }

    /// Resolves one kind-qualified member against the active graph.
    fn resolve_relocation_member(
        &self,
        domain: &nervix_models::DomainName,
        member: &RelocationMember,
    ) -> Result<NodeRef, RelocationPlanError> {
        let key = NodeRef::new(member.kind, member.name.clone());
        let Some(node) = self.node(member.kind, &member.name) else {
            return Err(RelocationPlanError::UnknownRuntimeNode {
                domain: domain.as_str().to_string(),
                kind: member.kind.as_str(),
                name: member.name.as_str().to_string(),
            });
        };
        if let Model::Ingestor(_) = node.config.as_ref()
            && node.config.executes_on_every_cluster_node()
        {
            return Err(RelocationPlanError::ServerListenerIngestor {
                name: member.name.as_str().to_string(),
            });
        }
        Ok(key)
    }

    /// Covers each `FROM`/`TO` pair with the path-gated coverage placement rules use.
    fn relocation_corridor_selection(
        &self,
        domain: &nervix_models::DomainName,
        from: &[RelocationMember],
        to: &[RelocationMember],
    ) -> Result<(Vec<NodeRef>, Vec<RelocationCoverage>), RelocationPlanError> {
        let mut selected = Vec::new();
        let mut coverage = Vec::new();
        let mut connected_pairs = 0usize;
        for source in from {
            let source_key = self.resolve_relocation_member(domain, source)?;
            for destination in to {
                let destination_key = self.resolve_relocation_member(domain, destination)?;
                let endpoint = self
                    .placement
                    .topology
                    .endpoint_analysis(source_key.clone(), destination_key.clone());
                let connected = !endpoint.corridor.is_empty();
                if connected {
                    connected_pairs = connected_pairs
                        .checked_add(1)
                        .assured("the pairs counted here are graph endpoints held in memory");
                }
                for covered in &endpoint.corridor {
                    if !selected.contains(covered) {
                        selected.push(covered.clone());
                    }
                }
                coverage.push(RelocationCoverage {
                    source: source_key.clone(),
                    destination: destination_key.clone(),
                    connected,
                    covered: endpoint.corridor.len(),
                });
            }
        }
        if connected_pairs == 0 {
            return Err(RelocationPlanError::DisconnectedCorridor);
        }
        selected.sort();
        Ok((selected, coverage))
    }

    /// Repeats capture rounds until the unit stops growing.
    fn capture_preferred_groups(
        &self,
        placement: &EffectivePlacementPlan,
        hard_group: &impl Fn(&NodeRef) -> Vec<NodeRef>,
        override_by_key: &HashMap<NodeRef, Vec<RelocationPreferenceStrategy>>,
        default_strategy: RelocationPreferenceStrategy,
        groups: &mut Vec<UnitGroup>,
        group_by_member: &mut HashMap<NodeRef, usize>,
    ) -> Result<(), RelocationPlanError> {
        loop {
            let mut candidates = Vec::new();
            let mut seen = HashSet::new();
            for (pair, resolved) in &placement.pairs {
                if resolved.policy != PlacementPolicy::PreferColocation {
                    continue;
                }
                for (inside, outside) in [(&pair.left, &pair.right), (&pair.right, &pair.left)] {
                    let Some(group_index) = group_by_member.get(inside) else {
                        continue;
                    };
                    if !groups[*group_index].strategy.follows_preferences() {
                        continue;
                    }
                    if group_by_member.contains_key(outside) {
                        continue;
                    }
                    let candidate = hard_group(outside);
                    let Some(first) = candidate.first() else {
                        continue;
                    };
                    if seen.insert(first.clone()) {
                        candidates.push(candidate);
                    }
                }
            }
            if candidates.is_empty() {
                return Ok(());
            }
            candidates.sort_by(|left, right| {
                left.first()
                    .verified(
                        "the empty-candidates branch above already returned and a group is never \
                         built empty",
                    )
                    .cmp(right.first().verified(
                        "the empty-candidates branch above already returned and a group is never \
                         built empty",
                    ))
            });

            let mut joined = false;
            for candidate in candidates {
                if candidate
                    .iter()
                    .any(|member| group_by_member.contains_key(member))
                {
                    continue;
                }
                if Self::separated_from_unit(placement, &candidate, groups, group_by_member) {
                    continue;
                }
                let strategy = Self::group_strategy(&candidate, override_by_key, default_strategy)?;
                let index = groups.len();
                for member in &candidate {
                    group_by_member.insert(member.clone(), index);
                }
                groups.push(UnitGroup {
                    members: candidate,
                    strategy,
                    reason: RelocationMemberReason::Preferred,
                });
                joined = true;
            }
            if !joined {
                return Ok(());
            }
        }
    }

    /// True when any candidate member is separated from a `FOLLOW PREFERENCES` group already in
    /// the unit.
    fn separated_from_unit(
        placement: &EffectivePlacementPlan,
        candidate: &[NodeRef],
        groups: &[UnitGroup],
        group_by_member: &HashMap<NodeRef, usize>,
    ) -> bool {
        placement.pairs.iter().any(|(pair, resolved)| {
            if resolved.policy != PlacementPolicy::SuggestSeparation {
                return false;
            }
            [(&pair.left, &pair.right), (&pair.right, &pair.left)]
                .into_iter()
                .any(|(outside, inside)| {
                    candidate.contains(outside)
                        && group_by_member
                            .get(inside)
                            .is_some_and(|index| groups[*index].strategy.follows_preferences())
                })
        })
    }

    /// A hard group carries one strategy, because it cannot be moved in pieces and its
    /// preferences are one set.
    fn group_strategy(
        members: &[NodeRef],
        override_by_key: &HashMap<NodeRef, Vec<RelocationPreferenceStrategy>>,
        default_strategy: RelocationPreferenceStrategy,
    ) -> Result<RelocationPreferenceStrategy, RelocationPlanError> {
        let mut chosen = None;
        for member in members {
            for strategy in override_by_key.get(member).into_iter().flatten() {
                match chosen {
                    None => chosen = Some(*strategy),
                    Some(existing) if existing == *strategy => {}
                    Some(_) => {
                        return Err(RelocationPlanError::ConflictingGroupStrategies {
                            members: members
                                .iter()
                                .map(|member| {
                                    format!("{} {}", member.kind.as_str(), member.identifier)
                                })
                                .collect::<Vec<_>>()
                                .join(", "),
                        });
                    }
                }
            }
        }
        Ok(chosen.unwrap_or(default_strategy))
    }

    /// Every effective soft preference with at least one endpoint in the unit.
    fn unit_preferences(
        pairs: &HashMap<PlacementPair, ResolvedPlacementPair>,
        group_by_member: &HashMap<NodeRef, usize>,
    ) -> Vec<RelocationPreference> {
        let mut preferences = pairs
            .iter()
            .filter(|(pair, resolved)| {
                matches!(
                    resolved.policy,
                    PlacementPolicy::PreferColocation | PlacementPolicy::SuggestSeparation
                ) && (group_by_member.contains_key(&pair.left)
                    || group_by_member.contains_key(&pair.right))
            })
            .map(|(pair, resolved)| RelocationPreference {
                policy: resolved.policy,
                left: pair.left.clone(),
                right: pair.right.clone(),
                winning_rules: resolved.winning_rules.clone(),
                from_domain_default: resolved.from_domain_default,
            })
            .collect::<Vec<_>>();
        preferences.sort_by(|left, right| {
            left.policy
                .as_ref()
                .cmp(right.policy.as_ref())
                .then_with(|| relocation_runtime_node_cmp(&left.left, &right.left))
                .then_with(|| relocation_runtime_node_cmp(&left.right, &right.right))
        });
        preferences
    }
}

fn relocation_runtime_node_cmp(left: &NodeRef, right: &NodeRef) -> std::cmp::Ordering {
    left.kind
        .as_str()
        .cmp(right.kind.as_str())
        .then_with(|| left.identifier.as_str().cmp(right.identifier.as_str()))
}
