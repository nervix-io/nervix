//! The schedule a START moves misplaced units with.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests for which units a START move places as the planner wants and which
//!   keep their committed assignment.
//! - **Depends on.** The START placement decisions and the shared application test fixtures.
//! - **Must not know.** Consensus, the ownership handoff or the runtime that applies the move.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeSet;

use nervix_models::{DomainName, DomainSchedule, ModelKind, NodeRef, NodeUnitRef, Unit};

use super::{
    super::test_fixtures::{
        node_named, placement_group, placement_member, scheduled_node, scheduled_node_on,
    },
    start_placement_schedule,
};

/// The node that owns `member`'s single unit in `schedule`.
fn owner<'schedule>(
    schedule: &'schedule DomainSchedule,
    member: &NodeRef,
) -> Option<&'schedule str> {
    schedule
        .nodes
        .get(member)
        .and_then(|node| node.unit(Unit::SINGLE))
        .and_then(|unit| unit.execution_node())
        .map(|node| node.as_str())
}

#[test]
fn a_start_move_carries_the_hard_colocation_group_of_a_misplaced_unit_and_nothing_else() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let scorer = placement_member("scorer", ModelKind::Junction);
    let archiver = placement_member("archiver", ModelKind::Junction);
    let auditor = placement_member("auditor", ModelKind::Junction);
    let group = vec![scorer.clone(), archiver.clone()];
    let current = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node_on("scorer", ModelKind::Junction, "node-1"),
            scheduled_node_on("archiver", ModelKind::Junction, "node-1"),
            scheduled_node_on("auditor", ModelKind::Junction, "node-1"),
        ],
        vec![placement_group(group.clone(), &node_named("node-1"))],
    );
    let desired = DomainSchedule::new(
        domain,
        vec![
            scheduled_node_on("scorer", ModelKind::Junction, "node-2"),
            scheduled_node_on("archiver", ModelKind::Junction, "node-2"),
            scheduled_node_on("auditor", ModelKind::Junction, "node-3"),
        ],
        vec![placement_group(group, &node_named("node-2"))],
    );

    let planned = start_placement_schedule(
        &current,
        &desired,
        &BTreeSet::from([NodeUnitRef::new(scorer.clone(), Unit::SINGLE)]),
    );

    assert_eq!(
        owner(&planned, &scorer),
        Some("node-2"),
        "the misplaced unit moves"
    );
    assert_eq!(
        owner(&planned, &archiver),
        Some("node-2"),
        "its hard colocation group moves with it"
    );
    assert_eq!(
        owner(&planned, &auditor),
        Some("node-1"),
        "a unit outside the move keeps its committed assignment"
    );
    assert_eq!(
        planned.placement_groups[0].primary_node,
        Some(node_named("node-2")),
        "the group's host follows its first member"
    );
}

#[test]
fn a_start_move_keeps_the_committed_assignment_of_a_unit_the_planner_does_not_place() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let scorer = placement_member("scorer", ModelKind::Junction);
    let archiver = placement_member("archiver", ModelKind::Junction);
    let current = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node_on("scorer", ModelKind::Junction, "node-1"),
            scheduled_node_on("archiver", ModelKind::Junction, "node-1"),
        ],
        Vec::new(),
    );
    let desired = DomainSchedule::new(
        domain,
        vec![scheduled_node("scorer", ModelKind::Junction).placed_on(None, Vec::new())],
        Vec::new(),
    );

    let planned = start_placement_schedule(
        &current,
        &desired,
        &BTreeSet::from([
            NodeUnitRef::new(scorer.clone(), Unit::SINGLE),
            NodeUnitRef::new(archiver.clone(), Unit::SINGLE),
        ]),
    );

    assert_eq!(
        owner(&planned, &scorer),
        Some("node-1"),
        "a unit the planner leaves unassigned keeps where it is"
    );
    assert_eq!(
        owner(&planned, &archiver),
        Some("node-1"),
        "a unit the planner does not schedule keeps where it is"
    );
}
