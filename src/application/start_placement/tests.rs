//! The schedule a START moves misplaced units with, and how long a START waits for an admitted
//! node.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests for which units a START move places as the planner wants and which
//!   keep their committed assignment, and for a waiting START that only its own task ends.
//! - **Depends on.** The START placement decisions and the shared application test fixtures,
//!   including the single-node test service a waiting START runs on.
//! - **Must not know.** The ownership handoff or the runtime that applies the move.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeSet;

use meticulous::OptionExt as _;
use nervix_models::{DomainName, DomainSchedule, ModelKind, NodeRef, NodeUnitRef, Unit};
use nervix_recovery::Discarded as _;

use super::{
    super::{
        subscription::SessionSubscriptions,
        test_fixtures::{
            TestService, build_test_service, named, node_named, placement_group, placement_member,
            scheduled_node, scheduled_node_on, test_command_request,
        },
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

/// The stopped `default` domain of a single-node test service, whose deduplicator only a node
/// tagged `zone=mars` may run. The service's one node carries no tags, so a START of this domain
/// waits for an admitted node.
const UNADMITTED_DEDUPLICATOR: [&str; 5] = [
    "CREATE SCHEMA transaction ( tenant STRING, transaction_id STRING );",
    "CREATE RELAY inbound SCHEMA transaction UNBRANCHED;",
    "CREATE RELAY deduped SCHEMA transaction UNBRANCHED;",
    "CREATE DEDUPLICATOR dedup_txns FROM inbound DEDUPLICATE ON input.transaction_id MAX TIME 10m \
     UNBRANCHED TO deduped INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
    "CREATE PLACEMENT mars_dedup FOR dedup_txns REQUIRE NODES WHERE tag('zone') = 'mars';",
];

/// A START that waits for an admitted node is a command like any other: when its node stops, it
/// ends with the node's service work and the leader that follows resumes it. Neither the node's
/// stop request nor the end of the services its save and moves rely on ends the wait with an
/// answer of its own, so no refusal is ever recorded for a START because its node stopped.
#[nervix_primitives::test(start_paused = true)]
async fn a_waiting_start_keeps_waiting_through_its_nodes_stop_signals() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let mut subscriptions = SessionSubscriptions::new();
    for command in UNADMITTED_DEDUPLICATOR {
        let result = service
            .test_command(test_command_request(command, "default"), &mut subscriptions)
            .await;
        assert!(
            result.succeeded(),
            "test graph command must succeed: {command}: {}",
            result.message
        );
    }
    let domain: DomainName = named("default");
    let looks = service
        .inner
        .operational
        .control_plane
        .scheduling_interval()
        .checked_mul(5)
        .assured("five scheduling intervals fit a Duration");
    {
        let placing = service.place_units_for_start(&domain);
        tokio::pin!(placing);

        let before_stop = nervix_primitives::time::timeout(looks, &mut placing).await;
        assert!(
            before_stop.is_err(),
            "START must wait while no node satisfies its node rule: {before_stop:?}"
        );

        service.inner.admission_shutdown.cancel();
        service.inner.drain_support_shutdown.cancel();
        let after_stop = nervix_primitives::time::timeout(looks, &mut placing).await;
        assert!(
            after_stop.is_err(),
            "a waiting START must keep waiting when its node stops, not end with: {after_stop:?}"
        );
    }

    subscriptions.stop_all().await;
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}
