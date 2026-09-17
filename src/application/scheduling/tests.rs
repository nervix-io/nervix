//! Schedule decision tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests for drain, relocation, failover, quorum and schedule-merge decisions.
//! - **Depends on.** The application scheduling decisions and the shared application test fixtures.
//! - **Must not know.** Runtime execution, consensus storage or edge protocols.

use std::collections::BTreeSet;

use nervix_models::{
    ClusterNodeName, DomainName, DomainSchedule, KafkaPartitionSchedule, ModelKind,
};
use nonzero_ext::nonzero;

use super::super::{
    ownership_handoff::{DrainMove, prefer_former_owners_as_replicas},
    session_service::SessionServiceImpl,
    test_fixtures::{
        named, node_named, placement_group, placement_member, scheduled_node, scheduled_node_on,
    },
};

#[test]
fn drop_node_quorum_error_allows_available_current_quorum() {
    let voters = BTreeSet::from([
        named::<ClusterNodeName>("node-1"),
        named::<ClusterNodeName>("node-2"),
        named::<ClusterNodeName>("node-3"),
    ]);
    let live_node_ids = BTreeSet::from([
        named::<ClusterNodeName>("node-1"),
        named::<ClusterNodeName>("node-3"),
    ]);

    assert!(
        SessionServiceImpl::drop_node_quorum_error(
            &ClusterNodeName::parse("node-2").expect("valid name"),
            &voters,
            &live_node_ids
        )
        .is_none()
    );
}

#[test]
fn drop_node_rebuild_uses_only_schedulable_nodes_for_new_assignments() {
    let live_voters = vec![
        named::<ClusterNodeName>("node-live"),
        named::<ClusterNodeName>("node-cordoned"),
    ];
    let schedulable_nodes = vec![named::<ClusterNodeName>("node-live")];

    let (new_assignment_candidates, preservable_nodes) =
        SessionServiceImpl::drop_node_schedule_node_sets(&live_voters, &schedulable_nodes);

    assert_eq!(new_assignment_candidates, schedulable_nodes);
    assert_eq!(preservable_nodes, live_voters);
}

#[test]
fn move_next_scheduled_node_for_drain_moves_only_one_node_in_canonical_order() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let mut schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node_on("ingest_notifications", ModelKind::Ingestor, "node-2"),
            scheduled_node_on("emit_notifications", ModelKind::Emitter, "node-2"),
        ],
        Vec::new(),
    );
    let desired = DomainSchedule::new(
        domain,
        vec![
            scheduled_node_on("ingest_notifications", ModelKind::Ingestor, "node-1"),
            scheduled_node_on("emit_notifications", ModelKind::Emitter, "node-3"),
        ],
        Vec::new(),
    );

    let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
        &mut schedule,
        &desired,
        &ClusterNodeName::parse("node-2").expect("valid name"),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ]),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]),
    );

    assert_eq!(
        moved,
        Some(DrainMove {
            label: "emitter emit_notifications".to_string(),
            promoted_replica: None,
            fallback_node: Some(ClusterNodeName::parse("node-3").expect("valid name")),
        })
    );
    assert_eq!(
        schedule.nodes[0].assigned_nodes,
        vec![named::<ClusterNodeName>("node-2")]
    );
    assert_eq!(
        schedule.nodes[1].assigned_nodes,
        vec![named::<ClusterNodeName>("node-3")]
    );
}

#[test]
fn planned_drain_uses_canonical_runtime_node_order() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let mut schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node_on("zeta", ModelKind::Junction, "node-2"),
            scheduled_node_on("alpha", ModelKind::Junction, "node-2"),
        ],
        Vec::new(),
    );
    let desired = DomainSchedule::new(
        domain,
        vec![
            scheduled_node_on("zeta", ModelKind::Junction, "node-1"),
            scheduled_node_on("alpha", ModelKind::Junction, "node-1"),
        ],
        Vec::new(),
    );

    let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
        &mut schedule,
        &desired,
        &ClusterNodeName::parse("node-2").expect("valid name"),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
        ]),
        &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
    );

    assert_eq!(
        moved,
        Some(DrainMove {
            label: "junction alpha".to_string(),
            promoted_replica: None,
            fallback_node: Some(named::<ClusterNodeName>("node-1")),
        })
    );
    assert_eq!(
        schedule.nodes[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-2"))
    );
    assert_eq!(
        schedule.nodes[1].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
}

#[test]
fn planned_drain_uses_the_first_member_to_order_placement_groups() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let alpha_members = vec![placement_member("alpha", ModelKind::Junction)];
    let zeta_members = vec![placement_member("zeta", ModelKind::Junction)];
    let mut schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node_on("zeta", ModelKind::Junction, "node-2"),
            scheduled_node_on("alpha", ModelKind::Junction, "node-2"),
        ],
        vec![
            placement_group(
                zeta_members.clone(),
                &ClusterNodeName::parse("node-2").expect("valid name"),
            ),
            placement_group(
                alpha_members.clone(),
                &ClusterNodeName::parse("node-2").expect("valid name"),
            ),
        ],
    );
    let desired = DomainSchedule::new(
        domain,
        vec![
            scheduled_node_on("zeta", ModelKind::Junction, "node-1"),
            scheduled_node_on("alpha", ModelKind::Junction, "node-1"),
        ],
        vec![
            placement_group(
                zeta_members,
                &ClusterNodeName::parse("node-1").expect("valid name"),
            ),
            placement_group(
                alpha_members,
                &ClusterNodeName::parse("node-1").expect("valid name"),
            ),
        ],
    );

    let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
        &mut schedule,
        &desired,
        &ClusterNodeName::parse("node-2").expect("valid name"),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
        ]),
        &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
    );

    assert_eq!(
        moved,
        Some(DrainMove {
            label: "placement group [alpha]".to_string(),
            promoted_replica: None,
            fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
        })
    );
    assert_eq!(
        schedule.nodes[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-2"))
    );
    assert_eq!(
        schedule.nodes[1].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
}

#[test]
fn planned_drain_prefers_policy_target_and_retains_former_owner_as_first_replica() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let mut schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-2")),
                vec![
                    node_named("node-2"),
                    node_named("node-3"),
                    node_named("node-4"),
                ],
            ),
        ],
        Vec::new(),
    );
    let desired = DomainSchedule::new(
        domain,
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-1")),
                vec![node_named("node-1"), node_named("node-3")],
            ),
        ],
        Vec::new(),
    );

    let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
        &mut schedule,
        &desired,
        &ClusterNodeName::parse("node-2").expect("valid name"),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ]),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]),
    );

    assert_eq!(
        moved,
        Some(DrainMove {
            label: "deduplicator dedup_notifications".to_string(),
            promoted_replica: None,
            fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
        })
    );
    assert_eq!(
        schedule.nodes[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
    assert_eq!(
        schedule.nodes[0].assigned_nodes,
        vec![
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ]
    );
}

#[test]
fn drain_require_group_prefers_policy_target_over_common_replica() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let members = vec![
        placement_member("corridor_source", ModelKind::Junction),
        placement_member("corridor_sink", ModelKind::Junction),
    ];
    let mut schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node("corridor_source", ModelKind::Junction).placed_on(
                Some(node_named("node-2")),
                vec![node_named("node-2"), node_named("node-3")],
            ),
            scheduled_node("corridor_sink", ModelKind::Junction).placed_on(
                Some(node_named("node-2")),
                vec![node_named("node-2"), node_named("node-3")],
            ),
        ],
        vec![placement_group(
            members.clone(),
            &ClusterNodeName::parse("node-2").expect("valid name"),
        )],
    );
    let desired = DomainSchedule::new(
        domain,
        vec![
            scheduled_node("corridor_source", ModelKind::Junction).placed_on(
                Some(node_named("node-1")),
                vec![node_named("node-1"), node_named("node-3")],
            ),
            scheduled_node("corridor_sink", ModelKind::Junction).placed_on(
                Some(node_named("node-1")),
                vec![node_named("node-1"), node_named("node-3")],
            ),
        ],
        vec![placement_group(
            members,
            &ClusterNodeName::parse("node-1").expect("valid name"),
        )],
    );

    let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
        &mut schedule,
        &desired,
        &ClusterNodeName::parse("node-2").expect("valid name"),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ]),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]),
    );

    assert_eq!(
        moved,
        Some(DrainMove {
            label: "placement group [corridor_source, corridor_sink]".to_string(),
            promoted_replica: None,
            fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
        })
    );
    assert!(
        schedule
            .nodes
            .values()
            .all(|node| node.primary_node.as_ref()
                == Some(&ClusterNodeName::parse("node-1").expect("valid name")))
    );
    assert_eq!(
        schedule.placement_groups[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
    assert!(schedule.nodes.values().all(|node| {
        node.assigned_nodes
            == vec![
                named::<ClusterNodeName>("node-1"),
                named::<ClusterNodeName>("node-2"),
            ]
    }));
}

#[test]
fn planned_drain_replaces_an_unavailable_replica_with_the_former_owner() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let mut schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-2")),
                vec![node_named("node-2"), node_named("node-3")],
            ),
        ],
        Vec::new(),
    );
    let desired = DomainSchedule::new(
        domain,
        vec![scheduled_node_on(
            "dedup_notifications",
            ModelKind::Deduplicator,
            "node-1",
        )],
        Vec::new(),
    );

    let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
        &mut schedule,
        &desired,
        &ClusterNodeName::parse("node-2").expect("valid name"),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
        ]),
        &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
    );

    assert_eq!(
        moved,
        Some(DrainMove {
            label: "deduplicator dedup_notifications".to_string(),
            promoted_replica: None,
            fallback_node: Some(ClusterNodeName::parse("node-1").expect("valid name")),
        })
    );
    assert_eq!(
        schedule.nodes[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
    assert_eq!(
        schedule.nodes[0].assigned_nodes,
        vec![
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2")
        ]
    );
}

#[test]
fn planned_schedule_move_prefers_the_former_owner_for_the_first_replica_slot() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let current = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-2")),
                vec![node_named("node-2"), node_named("node-3")],
            ),
        ],
        Vec::new(),
    );
    let mut planned = DomainSchedule::new(
        domain,
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-1")),
                vec![node_named("node-1"), node_named("node-3")],
            ),
        ],
        Vec::new(),
    );

    prefer_former_owners_as_replicas(
        Some(&current),
        &mut planned,
        &[
            ClusterNodeName::parse("node-1").expect("valid name"),
            ClusterNodeName::parse("node-2").expect("valid name"),
            ClusterNodeName::parse("node-3").expect("valid name"),
        ],
    );

    assert_eq!(
        planned.nodes[0].assigned_nodes,
        vec![
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2")
        ]
    );
}

#[test]
fn merge_existing_schedule_data_prefers_policy_target_when_primary_dies() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let mut next = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-1")),
                vec![node_named("node-1"), node_named("node-4")],
            ),
        ],
        Vec::new(),
    );
    let existing = DomainSchedule::new(
        domain,
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-2")),
                vec![
                    node_named("node-2"),
                    node_named("node-3"),
                    node_named("node-4"),
                ],
            ),
        ],
        Vec::new(),
    );

    SessionServiceImpl::merge_existing_schedule_data(
        &mut next,
        Some(&existing),
        &[
            ClusterNodeName::parse("node-1").expect("valid name"),
            ClusterNodeName::parse("node-3").expect("valid name"),
        ],
    );

    assert_eq!(
        next.nodes[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
    assert_eq!(
        next.nodes[0].assigned_nodes,
        vec![named::<ClusterNodeName>("node-1")]
    );
}

#[test]
fn merge_existing_schedule_data_falls_back_to_fresh_assignment_without_live_replica() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let mut next = DomainSchedule::new(
        domain.clone(),
        vec![scheduled_node_on(
            "dedup_notifications",
            ModelKind::Deduplicator,
            "node-1",
        )],
        Vec::new(),
    );
    let existing = DomainSchedule::new(
        domain,
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-2")),
                vec![node_named("node-2"), node_named("node-3")],
            ),
        ],
        Vec::new(),
    );

    SessionServiceImpl::merge_existing_schedule_data(
        &mut next,
        Some(&existing),
        &[ClusterNodeName::parse("node-1").expect("valid name")],
    );

    assert_eq!(
        next.nodes[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
    assert_eq!(
        next.nodes[0].assigned_nodes,
        vec![named::<ClusterNodeName>("node-1")]
    );
}

#[test]
fn merge_existing_schedule_data_preserves_matching_ingestor_schedule_and_assignment() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let preserved_schedule = KafkaPartitionSchedule::new(nonzero!(2u64), vec![0, 1], 7);
    let mut next = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node("ingest_notifications", ModelKind::Ingestor).placed_on(
                Some(node_named("node-1")),
                vec![node_named("node-2"), node_named("node-3")],
            ),
        ],
        Vec::new(),
    );
    let existing = DomainSchedule::new(
        domain,
        vec![
            scheduled_node_on("ingest_notifications", ModelKind::Ingestor, "node-1")
                .with_kafka_partitions(preserved_schedule.clone()),
        ],
        Vec::new(),
    );

    SessionServiceImpl::merge_existing_schedule_data(
        &mut next,
        Some(&existing),
        &[
            ClusterNodeName::parse("node-1").expect("valid name"),
            ClusterNodeName::parse("node-2").expect("valid name"),
            ClusterNodeName::parse("node-3").expect("valid name"),
        ],
    );

    assert_eq!(
        next.nodes[0].kafka_partition_schedule,
        Some(preserved_schedule)
    );
    assert_eq!(
        next.nodes[0].assigned_nodes,
        vec![named::<ClusterNodeName>("node-1")]
    );
}

#[test]
fn merge_existing_schedule_data_ignores_non_matching_nodes() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let mut next = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node("ingest_notifications", ModelKind::Ingestor),
            scheduled_node("kafka_main", ModelKind::Client),
        ],
        Vec::new(),
    );
    let existing = DomainSchedule::new(
        domain,
        vec![
            scheduled_node("other_ingestor", ModelKind::Ingestor)
                .with_kafka_partitions(KafkaPartitionSchedule::new(nonzero!(2u64), vec![0, 1], 3)),
            scheduled_node("ingest_notifications", ModelKind::Client)
                .with_kafka_partitions(KafkaPartitionSchedule::new(nonzero!(1u64), vec![0], 2)),
        ],
        Vec::new(),
    );

    SessionServiceImpl::merge_existing_schedule_data(&mut next, Some(&existing), &[]);

    assert_eq!(next.nodes[0].kafka_partition_schedule, None);
    assert_eq!(next.nodes[1].kafka_partition_schedule, None);
}

#[test]
fn merge_existing_schedule_data_rejects_a_split_require_group() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let members = vec![
        placement_member("corridor_source", ModelKind::Junction),
        placement_member("corridor_sink", ModelKind::Junction),
    ];
    let mut next = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node_on("corridor_source", ModelKind::Junction, "node-1"),
            scheduled_node_on("corridor_sink", ModelKind::Junction, "node-1"),
        ],
        vec![placement_group(
            members.clone(),
            &ClusterNodeName::parse("node-1").expect("valid name"),
        )],
    );
    let existing = DomainSchedule::new(
        domain,
        vec![
            scheduled_node_on("corridor_source", ModelKind::Junction, "node-2"),
            scheduled_node_on("corridor_sink", ModelKind::Junction, "node-3"),
        ],
        vec![placement_group(
            members,
            &ClusterNodeName::parse("node-2").expect("valid name"),
        )],
    );

    SessionServiceImpl::merge_existing_schedule_data(
        &mut next,
        Some(&existing),
        &[
            ClusterNodeName::parse("node-1").expect("valid name"),
            ClusterNodeName::parse("node-2").expect("valid name"),
            ClusterNodeName::parse("node-3").expect("valid name"),
        ],
    );

    assert!(next.nodes.values().all(|node| node.primary_node.as_ref()
        == Some(&ClusterNodeName::parse("node-1").expect("valid name"))));
    assert_eq!(
        next.placement_groups[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
}

#[test]
fn merge_existing_schedule_data_preserves_an_intact_require_group() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let members = vec![
        placement_member("corridor_source", ModelKind::Junction),
        placement_member("corridor_sink", ModelKind::Junction),
    ];
    let mut next = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node_on("corridor_source", ModelKind::Junction, "node-1"),
            scheduled_node_on("corridor_sink", ModelKind::Junction, "node-1"),
        ],
        vec![placement_group(
            members.clone(),
            &ClusterNodeName::parse("node-1").expect("valid name"),
        )],
    );
    let existing = DomainSchedule::new(
        domain,
        vec![
            scheduled_node_on("corridor_source", ModelKind::Junction, "node-2"),
            scheduled_node_on("corridor_sink", ModelKind::Junction, "node-2"),
        ],
        vec![placement_group(
            members,
            &ClusterNodeName::parse("node-2").expect("valid name"),
        )],
    );

    SessionServiceImpl::merge_existing_schedule_data(
        &mut next,
        Some(&existing),
        &[
            ClusterNodeName::parse("node-1").expect("valid name"),
            ClusterNodeName::parse("node-2").expect("valid name"),
        ],
    );

    assert!(next.nodes.values().all(|node| node.primary_node.as_ref()
        == Some(&ClusterNodeName::parse("node-2").expect("valid name"))));
    assert_eq!(
        next.placement_groups[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-2"))
    );
}

#[test]
fn drain_relocates_a_require_group_as_one_unit() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let members = vec![
        placement_member("corridor_source", ModelKind::Junction),
        placement_member("corridor_sink", ModelKind::Junction),
    ];
    let mut schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node_on("corridor_source", ModelKind::Junction, "node-2"),
            scheduled_node_on("corridor_sink", ModelKind::Junction, "node-2"),
        ],
        vec![placement_group(
            members.clone(),
            &ClusterNodeName::parse("node-2").expect("valid name"),
        )],
    );
    let desired = DomainSchedule::new(
        domain,
        vec![
            scheduled_node_on("corridor_source", ModelKind::Junction, "node-1"),
            scheduled_node_on("corridor_sink", ModelKind::Junction, "node-1"),
        ],
        vec![placement_group(
            members,
            &ClusterNodeName::parse("node-1").expect("valid name"),
        )],
    );

    let moved = SessionServiceImpl::move_next_scheduled_node_for_drain(
        &mut schedule,
        &desired,
        &ClusterNodeName::parse("node-2").expect("valid name"),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-2"),
            named::<ClusterNodeName>("node-3"),
        ]),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]),
    );

    assert!(moved.is_some());
    assert!(
        schedule
            .nodes
            .values()
            .all(|node| node.primary_node.as_ref()
                == Some(&ClusterNodeName::parse("node-1").expect("valid name")))
    );
    assert_eq!(
        schedule.placement_groups[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
}

#[test]
fn failover_relocates_a_require_group_to_one_target() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let members = vec![
        placement_member("corridor_source", ModelKind::Junction),
        placement_member("corridor_sink", ModelKind::Junction),
    ];
    let mut schedule = DomainSchedule::new(
        domain,
        vec![
            scheduled_node("corridor_source", ModelKind::Junction).placed_on(
                Some(node_named("node-2")),
                vec![node_named("node-2"), node_named("node-3")],
            ),
            scheduled_node("corridor_sink", ModelKind::Junction).placed_on(
                Some(node_named("node-2")),
                vec![node_named("node-2"), node_named("node-1")],
            ),
        ],
        vec![placement_group(
            members,
            &ClusterNodeName::parse("node-2").expect("valid name"),
        )],
    );

    let moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
        &mut schedule,
        None,
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]),
    );

    assert!(!moves.is_empty());
    let group_host = schedule.placement_groups[0]
        .primary_node
        .as_ref()
        .expect("require group must retain a host");
    assert!(
        schedule
            .nodes
            .values()
            .all(|node| node.primary_node.as_ref() == Some(group_host))
    );
}

#[test]
fn failover_does_not_promote_a_cordoned_live_replica() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let mut schedule = DomainSchedule::new(
        domain,
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-2")),
                vec![node_named("node-2"), node_named("node-3")],
            ),
        ],
        Vec::new(),
    );

    let moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
        &mut schedule,
        None,
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]),
        &BTreeSet::from([named::<ClusterNodeName>("node-1")]),
    );

    assert!(!moves.is_empty());
    assert_eq!(
        schedule.nodes[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
}

#[test]
fn failover_promotes_live_replica_before_policy_target() {
    let domain = DomainName::parse("payments").expect("valid domain");
    let mut schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-2")),
                vec![node_named("node-2"), node_named("node-3")],
            ),
        ],
        Vec::new(),
    );
    let desired = DomainSchedule::new(
        domain,
        vec![
            scheduled_node("dedup_notifications", ModelKind::Deduplicator).placed_on(
                Some(node_named("node-1")),
                vec![node_named("node-1"), node_named("node-3")],
            ),
        ],
        Vec::new(),
    );

    let moves = SessionServiceImpl::failover_unavailable_scheduled_nodes(
        &mut schedule,
        Some(&desired),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]),
        &BTreeSet::from([
            named::<ClusterNodeName>("node-1"),
            named::<ClusterNodeName>("node-3"),
        ]),
    );

    assert_eq!(
        moves,
        vec![DrainMove {
            label: "deduplicator dedup_notifications".to_string(),
            promoted_replica: Some(ClusterNodeName::parse("node-3").expect("valid name")),
            fallback_node: None,
        }]
    );
    assert_eq!(
        schedule.nodes[0].primary_node.as_ref(),
        Some(&named::<ClusterNodeName>("node-3"))
    );
    assert_eq!(
        schedule.nodes[0].assigned_nodes,
        vec![
            named::<ClusterNodeName>("node-3"),
            named::<ClusterNodeName>("node-1")
        ]
    );
}
