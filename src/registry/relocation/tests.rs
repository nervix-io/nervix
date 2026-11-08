//! Exact-unit relocation decisions from typed graph inputs.
//!
//! Outside the layer order: test harness.
//! - **Owns.** Partition closure, preference traversal and selector refusal assertions.
//! - **Depends on.** The production relocation owner and typed graph fixtures.
//! - **Must not know.** Storage, cluster membership, transport or runtime handoffs.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the production decision from typed inputs")
)]

use std::collections::BTreeSet;

use meticulous::ResultExt as _;
use nervix_models::{BranchPartitions, Model, ModelIndex, ModelKind, ModelName};
use nonzero_ext::nonzero;

use super::*;
use crate::registry::{
    domain_state::DomainState,
    test_fixtures::{full_graph_batch, named, placement},
};

fn partition_graph(policy: PlacementPolicy) -> ActiveGraph {
    let mut models = full_graph_batch();
    for model in &mut models {
        if let Model::Branch(branch) = model {
            branch.partitions = BranchPartitions::Direct(nonzero!(2u16));
        }
    }
    models.push(placement(
        "corridor",
        &["notifications"],
        &["p99"],
        policy,
        None,
    ));
    let models = models.into_iter().collect::<ModelIndex>();
    DomainState::build(&named("unit_moves"), &models)
        .assured("the typed two-partition corridor is valid")
        .graph
}

fn selected(partition: u32) -> RelocationSelection {
    RelocationSelection::List(vec![RelocationMember {
        node: NodeRef::new(ModelKind::Deduplicator, named::<ModelName>("p99_proc")),
        selection: RelocationUnitSelection::Partition(partition),
    }])
}

fn member(kind: ModelKind, name: &str, partition: u32) -> NodeUnitRef {
    NodeUnitRef::new(
        NodeRef::new(kind, named::<ModelName>(name)),
        Unit::partition(partition),
    )
}

fn partition_zero_corridor() -> BTreeSet<NodeUnitRef> {
    BTreeSet::from([
        member(ModelKind::Relay, "notifications", 0),
        member(ModelKind::Deduplicator, "p99_proc", 0),
        member(ModelKind::Relay, "p99", 0),
    ])
}

#[test]
fn selecting_one_partition_closes_only_its_hard_group() {
    let graph = partition_graph(PlacementPolicy::RequireColocation);
    let plan = graph
        .relocation_unit(
            &named("unit_moves"),
            &selected(0),
            RelocationPreferenceStrategy::Ignore,
            &[],
        )
        .assured("one partition of the hard corridor can be selected");
    let actual: BTreeSet<_> = plan
        .members
        .iter()
        .map(|member| member.runtime_unit.clone())
        .collect();
    assert_eq!(partition_zero_corridor(), actual);
    assert!(plan.members.iter().all(|member| member.group == 1));
    for member in &plan.members {
        let expected = if member.runtime_unit.node.kind == ModelKind::Deduplicator {
            RelocationMemberReason::Selected
        } else {
            RelocationMemberReason::Required
        };
        assert_eq!(member.reason, expected);
    }
}

#[test]
fn following_preferences_traverses_only_corresponding_partition_units() {
    let graph = partition_graph(PlacementPolicy::PreferColocation);
    for strategy in [
        RelocationPreferenceStrategy::Follow,
        RelocationPreferenceStrategy::Ignore,
    ] {
        let plan = graph
            .relocation_unit(&named("unit_moves"), &selected(0), strategy, &[])
            .assured("the soft corridor supports either preference strategy");
        let expected = match strategy {
            RelocationPreferenceStrategy::Follow => partition_zero_corridor(),
            RelocationPreferenceStrategy::Ignore => {
                BTreeSet::from([member(ModelKind::Deduplicator, "p99_proc", 0)])
            }
        };
        let actual: BTreeSet<_> = plan
            .members
            .iter()
            .map(|member| member.runtime_unit.clone())
            .collect();
        assert_eq!(expected, actual);
    }
}

#[test]
fn selectors_and_overrides_are_checked_against_the_exact_unit() {
    let graph = partition_graph(PlacementPolicy::RequireColocation);
    let Err(error) = graph.relocation_unit(
        &named("unit_moves"),
        &selected(2),
        RelocationPreferenceStrategy::Ignore,
        &[],
    ) else {
        panic!("a two-partition graph has no partition 2");
    };
    assert!(matches!(
        error.current_context(),
        RelocationPlanError::UnknownUnit {
            selection: RelocationUnitSelection::Partition(2),
            ..
        }
    ));

    let outside = RelocationPreferenceOverride {
        member: RelocationMember {
            node: NodeRef::new(ModelKind::Deduplicator, named::<ModelName>("p99_proc")),
            selection: RelocationUnitSelection::Partition(1),
        },
        strategy: RelocationPreferenceStrategy::Follow,
    };
    let Err(error) = graph.relocation_unit(
        &named("unit_moves"),
        &selected(0),
        RelocationPreferenceStrategy::Ignore,
        &[outside],
    ) else {
        panic!("an override for the sibling partition is outside this relocation");
    };
    let RelocationPlanError::OverrideOutsideUnit { member: outside } = error.current_context()
    else {
        panic!("the owner must report the exact out-of-selection unit: {error:?}");
    };
    assert_eq!(outside, &member(ModelKind::Deduplicator, "p99_proc", 1));
}
