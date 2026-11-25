//! Layer: test harness.
//! Owns: which uncommitted ownership-handoff preparations a node keeps when the leader reconciles
//! them, while the move that needs a preparation can still be committed.
//! May depend on: runtime state replication fixtures in the parent test module.
//! Must not know: edge protocols or production scheduling decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

/// A destination that restarted after it confirmed a move, and before the move was committed,
/// reloads its preparation without the gate its earlier run held and under a new run of its own.
/// The leader that prepared the move can still commit it, so its reconciliation keeps the
/// preparation, and the committed move then activates the copy the destination kept.
#[nervix_primitives::test]
async fn a_restarted_destination_keeps_a_preparation_its_leader_may_still_commit() {
    let fixture = EmptyRelayHandoffFixture::new("in-flight-operation");
    let dir = tempdir().expect("temporary runtime state directory should open");
    let coordination = CoordinationIdentity::new(named("coordinator-a"), 11, 1);
    let source_incarnation = ClusterNodeIncarnation::new(31);
    let prepared_destination_incarnation = {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
            .expect("runtime should open persisted state");
        let destination_incarnation = attach_loopback_cluster(&runtime, &fixture.destination).await;
        fixture
            .rebuild_destination(&runtime, fixture.base_schedule.clone())
            .await;
        fixture
            .prepare(
                &runtime,
                coordination.clone(),
                source_incarnation,
                destination_incarnation,
            )
            .await;
        destination_incarnation
    };

    // The destination restarts before the move is committed: its run changes and its hold ends.
    let db = Database::builder(dir.path())
        .open()
        .expect("database should reopen after destination restart");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("restarted runtime should restore persisted state");
    let restarted_destination_incarnation =
        attach_loopback_cluster(&runtime, &fixture.destination).await;
    assert_ne!(
        restarted_destination_incarnation,
        prepared_destination_incarnation
    );
    let base_revision = PlannedClusterRevision::between(
        None,
        &ClusterSchedule::from_iter([fixture.base_schedule.clone()]),
    )
    .assured("the handoff fixture has a complete cluster revision");
    let same_leader = CoordinationIdentity::new(named("coordinator-a"), 11, 2);
    assert_eq!(
        runtime
            .reconcile_prepared_ownership_handoffs(&same_leader, &base_revision)
            .expect("the leader that prepared the move should reconcile its destination"),
        0,
        "the leader that prepared the move can still commit it, so its restarted destination \
         keeps the copy"
    );
    let key = DomainNodeUnitRef::single(fixture.entity.in_domain(&fixture.domain));
    assert!(
        runtime
            .inner
            .prepared_runtime_state_handoffs
            .contains_key(&key)
    );

    // The move commits, and ordinary schedule application activates the copy the destination kept.
    let mut domain_state = unpaced_domain_state(fixture.domain.as_str());
    domain_state.status = DomainStatus::Stopped;
    runtime.sync_domains(&BTreeMap::from([(fixture.domain.clone(), domain_state)]));
    runtime
        .rebuild_domain_from_schedule(
            &fixture.destination,
            &fixture.domain,
            Some(fixture.target_schedule.clone()),
            false,
        )
        .await
        .expect("the committed move should activate the kept preparation");
    assert!(
        !runtime
            .inner
            .prepared_runtime_state_handoffs
            .contains_key(&key)
    );
    let activated = runtime
        .inner
        .activated_runtime_state_handoffs
        .get(&key)
        .expect("the committed move should be activated from the kept preparation");
    assert_eq!(activated.coordination, coordination);
    assert_eq!(activated.operation_id, fixture.operation_id);
}

/// The end of a move's hold does not stop its leader from committing the move, so the leader's
/// reconciliation keeps the preparation while the domain's committed schedule is the move's base.
#[nervix_primitives::test]
async fn a_leader_keeps_its_preparation_after_the_hold_ends_while_the_move_can_still_commit() {
    let fixture = EmptyRelayHandoffFixture::new("unheld-operation");
    let runtime = Runtime::new();
    let source_incarnation = ClusterNodeIncarnation::new(31);
    let destination_incarnation = attach_loopback_cluster(&runtime, &fixture.destination).await;
    fixture
        .rebuild_destination(&runtime, fixture.base_schedule.clone())
        .await;
    let coordination = CoordinationIdentity::new(named("coordinator-a"), 11, 1);
    fixture
        .prepare(
            &runtime,
            coordination.clone(),
            source_incarnation,
            destination_incarnation,
        )
        .await;
    runtime
        .release_entity_gate_operation(&coordination, &fixture.domain)
        .await
        .expect("the move's hold should release");

    let base_revision = PlannedClusterRevision::between(
        None,
        &ClusterSchedule::from_iter([fixture.base_schedule.clone()]),
    )
    .assured("the handoff fixture has a complete cluster revision");
    let same_leader = CoordinationIdentity::new(named("coordinator-a"), 11, 2);
    assert_eq!(
        runtime
            .reconcile_prepared_ownership_handoffs(&same_leader, &base_revision)
            .expect("the leader that prepared the move should reconcile its destination"),
        0,
        "the end of the hold does not stop the leader from committing the move"
    );
    assert!(runtime.inner.prepared_runtime_state_handoffs.contains_key(
        &DomainNodeUnitRef::single(fixture.entity.in_domain(&fixture.domain))
    ));
}

/// Once the domain's committed schedule is no longer the move's base, the move can never commit,
/// and the leader's reconciliation discards the preparation.
#[nervix_primitives::test]
async fn a_leader_discards_its_preparation_once_the_domain_schedule_left_the_move_base() {
    let fixture = EmptyRelayHandoffFixture::new("superseded-operation");
    let runtime = Runtime::new();
    let source_incarnation = ClusterNodeIncarnation::new(31);
    let destination_incarnation = attach_loopback_cluster(&runtime, &fixture.destination).await;
    fixture
        .rebuild_destination(&runtime, fixture.base_schedule.clone())
        .await;
    let coordination = CoordinationIdentity::new(named("coordinator-a"), 11, 1);
    fixture
        .prepare(
            &runtime,
            coordination.clone(),
            source_incarnation,
            destination_incarnation,
        )
        .await;

    // Another move of the same unit committed, so this preparation's move can no longer commit.
    let committed = EmptyRelayHandoffFixture::new("committed-operation");
    let moved_on_revision = PlannedClusterRevision::between(
        None,
        &ClusterSchedule::from_iter([committed.target_schedule.clone()]),
    )
    .assured("the handoff fixture has a complete cluster revision");
    let same_leader = CoordinationIdentity::new(named("coordinator-a"), 11, 2);
    assert_eq!(
        runtime
            .reconcile_prepared_ownership_handoffs(&same_leader, &moved_on_revision)
            .expect("the leader should reconcile a preparation its schedule moved past"),
        1
    );
    assert!(!runtime.inner.prepared_runtime_state_handoffs.contains_key(
        &DomainNodeUnitRef::single(fixture.entity.in_domain(&fixture.domain))
    ));
}
