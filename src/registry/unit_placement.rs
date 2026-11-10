//! Correspondence and indivisible groups of exact execution units.
//!
//! Layer: decisions.
//! - **Owns.** Unit layouts, direct-hop correspondence and hard unit components.
//! - **Depends on.** The validated graph and vocabulary identities.
//! - **Must not know.** Runtime state, transport, tasks or locks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::{NonZeroU16, NonZeroU64},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ModelKind, NodeUnitRef, PlacementPolicy, Unit, UnitKind};
use petgraph::unionfind::UnionFind;

use super::{
    emitter_units::EmitterUnits,
    graph::{ActiveGraph, ActiveNode},
    ingestor_units::IngestorUnits,
    placement::EffectivePlacementPlan,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HopClass {
    Identity,
    Fiber,
    SingleMany,
    DataDependent,
}

impl HopClass {
    pub fn permits(self, policy: PlacementPolicy) -> bool {
        match policy {
            PlacementPolicy::Neutral => true,
            PlacementPolicy::RequireColocation | PlacementPolicy::PreferColocation => {
                matches!(self, Self::Identity | Self::Fiber)
            }
            PlacementPolicy::SuggestSeparation => self != Self::DataDependent,
        }
    }
}

impl fmt::Display for HopClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Identity => "identity",
            Self::Fiber => "fiber",
            Self::SingleMany => "single↔many",
            Self::DataDependent => "data-dependent",
        })
    }
}

/// A compact projection of a validated node, without allocating its units for every hop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::registry) enum UnitLayout {
    Single,
    Partitions(NonZeroU16),
    Instances(NonZeroU64),
}

impl UnitLayout {
    /// Whether a branch's partitions divide a node of `kind` into units.
    pub fn divides_by_partitions(kind: ModelKind) -> bool {
        matches!(
            kind,
            ModelKind::Relay
                | ModelKind::Junction
                | ModelKind::Deduplicator
                | ModelKind::Reorderer
                | ModelKind::WindowProcessor
                | ModelKind::Correlator
                | ModelKind::Inferencer
                | ModelKind::WasmProcessor
                | ModelKind::Generator
        )
    }

    pub fn of(node: &ActiveNode) -> Self {
        if let Some(instancing) = &node.ingestor_units {
            return instancing.unit_layout();
        }
        if let Some(instancing) = &node.emitter_units {
            return instancing.unit_layout();
        }
        if Self::divides_by_partitions(node.kind)
            && let Some(partitions) = node
                .resolved_branching
                .as_ref()
                .and_then(|branch| branch.partitions())
            && partitions.count().get() > 1
        {
            return Self::Partitions(partitions.count());
        }
        Self::Single
    }

    fn count(self) -> u64 {
        match self {
            Self::Single => 1,
            Self::Partitions(count) => u64::from(count.get()),
            Self::Instances(count) => count.get(),
        }
    }
    fn unit(self, index: u64) -> Unit {
        match self {
            Self::Single => Unit::SINGLE,
            Self::Partitions(_) => Unit::partition(
                u32::try_from(index).verified("a partition ordinal is below its u16 count"),
            ),
            Self::Instances(_) => Unit::try_instance(index)
                .verified("the instancing decision validated its allocation layout"),
        }
    }
    pub fn units(self) -> impl Iterator<Item = Unit> {
        (0..self.count()).map(move |index| self.unit(index))
    }
    pub fn contains(self, unit: Unit) -> bool {
        self.ordinal(unit).is_some()
    }
    fn ordinal(self, unit: Unit) -> Option<u64> {
        let index = match (self, unit.kind()) {
            (Self::Single, UnitKind::Single) => 0,
            (Self::Partitions(_), UnitKind::Partition(index)) => u64::from(index),
            (Self::Instances(_), UnitKind::Instance(index)) => index,
            _ => return None,
        };
        (index < self.count()).then_some(index)
    }
}

/// A declared mapping, retained once per hop. Partner iteration allocates no edge table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::registry) struct UnitCorrespondence {
    pub class: HopClass,
    left: UnitLayout,
    right: UnitLayout,
}

impl UnitCorrespondence {
    pub fn decide(left: &ActiveNode, right: &ActiveNode) -> Self {
        let lhs = UnitLayout::of(left);
        let rhs = UnitLayout::of(right);
        let singleton_pair = lhs.count() == 1 && rhs.count() == 1;
        let partition_intake = !singleton_pair
            && matches!(&left.ingestor_units, Some(IngestorUnits::PerPartition { relays, .. }) if right.kind == ModelKind::Relay && relays.iter().any(|relay| relay.as_str() == right.identifier.as_str()));
        let partition_emission = !singleton_pair
            && matches!(&right.emitter_units, Some(EmitterUnits::PerPartition { relays, .. }) if left.kind == ModelKind::Relay && relays.iter().any(|relay| relay.as_str() == left.identifier.as_str()));
        // A relay maps each partition to one emitter unit by declaration: every partition to a
        // single emitter, and partition `p` to instance `p mod n` of an emitter with `n`.
        let declared_emission = left.kind == ModelKind::Relay
            && right.kind == ModelKind::Emitter
            && (matches!(lhs, UnitLayout::Partitions(_))
                || matches!(rhs, UnitLayout::Instances(_)));
        let class = if singleton_pair || partition_intake || partition_emission {
            HopClass::Identity
        } else if (left.kind == ModelKind::Ingestor || left.kind == ModelKind::Reingestor)
            && rhs.count() > 1
        {
            HopClass::DataDependent
        } else if declared_emission {
            HopClass::Fiber
        } else if matches!((lhs, rhs), (UnitLayout::Partitions(a), UnitLayout::Partitions(b)) if a == b)
            && left
                .resolved_branching
                .as_ref()
                .map(|branch| branch.fields())
                == right
                    .resolved_branching
                    .as_ref()
                    .map(|branch| branch.fields())
        {
            HopClass::Identity
        } else if lhs.count() == 1 || rhs.count() == 1 {
            HopClass::SingleMany
        } else {
            HopClass::DataDependent
        };
        Self {
            class,
            left: lhs,
            right: rhs,
        }
    }

    /// The units on the other side that correspond to this exact unit.
    pub fn partners(self, from_left: bool, unit: Unit) -> impl Iterator<Item = Unit> {
        let (own, other) = if from_left {
            (self.left, self.right)
        } else {
            (self.right, self.left)
        };
        let (start, end, stride) = match own.ordinal(unit) {
            None => (0, 0, 1),
            Some(index) => match self.class {
                HopClass::Identity => (
                    index,
                    index
                        .checked_add(1)
                        .verified("a validated unit ordinal leaves room for its exclusive end")
                        .min(other.count()),
                    1,
                ),
                HopClass::Fiber if from_left => {
                    let partner = index % self.right.count();
                    let end = partner
                        .checked_add(1)
                        .verified("a fiber partner is below its nonzero recipient count");
                    (partner, end, 1)
                }
                HopClass::Fiber => (index, self.left.count(), self.right.count()),
                HopClass::SingleMany if own.count() == 1 => (0, other.count(), 1),
                HopClass::SingleMany => (0, 1, 1),
                HopClass::DataDependent => (0, 0, 1),
            },
        };
        std::iter::successors((start < end).then_some(start), move |index| {
            index.checked_add(stride).filter(|next| *next < end)
        })
        .map(move |index| other.unit(index))
    }
}

pub(in crate::registry) struct UnitPlacementGroups {
    pub groups: Vec<Vec<NodeUnitRef>>,
    pub by_member: BTreeMap<NodeUnitRef, usize>,
}

impl UnitPlacementGroups {
    pub fn decide(graph: &ActiveGraph, placement: &EffectivePlacementPlan) -> Self {
        // Identity components share one ordinal mapping. Expand that component once, rather
        // than repeating the same unit unions for every edge of a dense corridor.
        let mut identities = BTreeMap::new();
        let mut identity_components = UnionFind::<usize>::new(0);
        for (pair, resolved) in &placement.pairs {
            if resolved.policy != PlacementPolicy::RequireColocation {
                continue;
            }
            match resolved.correspondence.class {
                HopClass::Identity => connect(
                    &mut identities,
                    &mut identity_components,
                    pair.left.clone(),
                    pair.right.clone(),
                ),
                HopClass::Fiber => {
                    connect(
                        &mut identities,
                        &mut identity_components,
                        pair.left.clone(),
                        pair.left.clone(),
                    );
                    connect(
                        &mut identities,
                        &mut identity_components,
                        pair.right.clone(),
                        pair.right.clone(),
                    );
                }
                HopClass::SingleMany | HopClass::DataDependent => {
                    unreachable!("validation refuses a hard policy on this hop class")
                }
            }
        }
        for node in graph.graph.node_weights() {
            let relays = match (&node.ingestor_units, &node.emitter_units) {
                (Some(IngestorUnits::PerPartition { relays, .. }), _)
                | (_, Some(EmitterUnits::PerPartition { relays, .. })) => relays,
                _ => continue,
            };
            for relay in relays {
                connect(
                    &mut identities,
                    &mut identity_components,
                    node.node_ref(),
                    nervix_models::NodeRef::new(ModelKind::Relay, relay.clone()),
                );
            }
        }
        let mut identity_members =
            BTreeMap::<usize, Vec<(nervix_models::NodeRef, UnitLayout)>>::new();
        for (member, index) in &identities {
            let node = graph
                .node(member.kind, &member.identifier)
                .verified("every placement or intake member belongs to this validated graph");
            identity_members
                .entry(identity_components.find(*index))
                .or_default()
                .push((member.clone(), UnitLayout::of(node)));
        }
        let mut members = BTreeMap::new();
        let mut components = UnionFind::<usize>::new(0);
        for identity in identity_members.values() {
            let Some((first, first_layout)) = identity.first() else {
                continue;
            };
            for (other, layout) in &identity[1..] {
                for index in 0..first_layout.count() {
                    connect(
                        &mut members,
                        &mut components,
                        NodeUnitRef::new(first.clone(), first_layout.unit(index)),
                        NodeUnitRef::new(other.clone(), layout.unit(index)),
                    );
                }
            }
        }
        let mut collapsed = BTreeSet::new();
        let mut fibers = BTreeSet::new();
        for (pair, resolved) in &placement.pairs {
            let correspondence = resolved.correspondence;
            if resolved.policy != PlacementPolicy::RequireColocation
                || correspondence.class != HopClass::Fiber
            {
                continue;
            }
            let left_component = identity_components.find(identities[&pair.left]);
            let right_component = identity_components.find(identities[&pair.right]);
            if !fibers.insert((left_component, right_component)) {
                continue;
            }
            if correspondence.right.count() == 1 {
                // Every current single-emitter fiber joins all ordinals of its input identity
                // component. Collapse it once even when several emitters consume that component.
                let first = NodeUnitRef::new(pair.left.clone(), correspondence.left.unit(0));
                if collapsed.insert(left_component) {
                    for unit in correspondence.left.units().skip(1) {
                        connect(
                            &mut members,
                            &mut components,
                            first.clone(),
                            NodeUnitRef::new(pair.left.clone(), unit),
                        );
                    }
                }
                connect(
                    &mut members,
                    &mut components,
                    first,
                    NodeUnitRef::new(pair.right.clone(), correspondence.right.unit(0)),
                );
            } else {
                for left in correspondence.left.units() {
                    for right in correspondence.partners(true, left) {
                        connect(
                            &mut members,
                            &mut components,
                            NodeUnitRef::new(pair.left.clone(), left),
                            NodeUnitRef::new(pair.right.clone(), right),
                        );
                    }
                }
            }
        }
        let mut grouped = BTreeMap::<usize, Vec<NodeUnitRef>>::new();
        for (member, index) in members {
            grouped
                .entry(components.find(index))
                .or_default()
                .push(member);
        }
        let mut groups = grouped.into_values().collect::<Vec<_>>();
        groups.sort_by(|left, right| left.first().cmp(&right.first()));
        let by_member = groups
            .iter()
            .enumerate()
            .flat_map(|(index, group)| group.iter().cloned().map(move |member| (member, index)))
            .collect();
        Self { groups, by_member }
    }
}

fn connect<K: Ord>(
    members: &mut BTreeMap<K, usize>,
    components: &mut UnionFind<usize>,
    left: K,
    right: K,
) {
    let mut index = |member| {
        *members
            .entry(member)
            .or_insert_with(|| components.new_set())
    };
    let left = index(left);
    let right = index(right);
    components.union(left, right);
}

#[cfg(test)]
mod tests {
    use nonzero_ext::nonzero;

    use super::*;

    #[test]
    fn hop_classes_admit_only_policies_with_a_defined_unit_mapping() {
        let policies = [
            PlacementPolicy::Neutral,
            PlacementPolicy::RequireColocation,
            PlacementPolicy::PreferColocation,
            PlacementPolicy::SuggestSeparation,
        ];
        for (class, expected) in [
            (HopClass::Identity, [true, true, true, true]),
            (HopClass::Fiber, [true, true, true, true]),
            (HopClass::SingleMany, [true, false, false, true]),
            (HopClass::DataDependent, [true, false, false, false]),
        ] {
            assert_eq!(policies.map(|policy| class.permits(policy)), expected);
        }
    }

    #[test]
    fn identity_maps_exact_ordinals_across_instance_and_partition_kinds() {
        let correspondence = UnitCorrespondence {
            class: HopClass::Identity,
            left: UnitLayout::Instances(nonzero!(2u64)),
            right: UnitLayout::Partitions(nonzero!(2u16)),
        };
        for index in 0..2 {
            let instance = Unit::try_instance(index).assured("small instance");
            let partition = Unit::partition(u32::try_from(index).assured("small partition"));
            assert_eq!(
                correspondence.partners(true, instance).collect::<Vec<_>>(),
                [partition]
            );
            assert_eq!(
                correspondence
                    .partners(false, partition)
                    .collect::<Vec<_>>(),
                [instance]
            );
        }
        assert!(correspondence.partners(true, Unit::SINGLE).next().is_none());
        assert!(
            correspondence
                .partners(false, Unit::partition(2))
                .next()
                .is_none()
        );
    }

    #[test]
    fn a_single_emitter_fiber_has_all_partition_partners_in_reverse() {
        let correspondence = UnitCorrespondence {
            class: HopClass::Fiber,
            left: UnitLayout::Partitions(nonzero!(3u16)),
            right: UnitLayout::Single,
        };
        for index in 0..3 {
            assert_eq!(
                correspondence
                    .partners(true, Unit::partition(index))
                    .collect::<Vec<_>>(),
                [Unit::SINGLE]
            );
        }
        assert_eq!(
            correspondence
                .partners(false, Unit::SINGLE)
                .collect::<Vec<_>>(),
            [Unit::partition(0), Unit::partition(1), Unit::partition(2)]
        );
    }
}
