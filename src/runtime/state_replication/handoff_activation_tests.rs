//! Layer: test harness.
//! Owns: how a destination answers a planned move's activation request when it restarted before it
//! installed the committed revision.
//! May depend on: runtime state replication fixtures in the parent test module.
//! Must not know: edge protocols or production scheduling decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

/// A destination that restarted between a planned move's commit and its own installation of the
/// committed revision runs the domain under the base revision and holds the move's preparation
/// from its earlier run. An activation request for that preparation leaves the running execution
/// in place: the node's ordinary application of the committed revision activates the moved unit,
/// as it activates every planned move, and the domain's other work keeps what it holds.
#[nervix_primitives::test]
async fn an_activation_request_leaves_a_restarted_destinations_running_domain_in_place() {
    let fixture = EmptyRelayHandoffFixture::new("restarted-destination-operation");
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

    let db = Database::builder(dir.path())
        .open()
        .expect("database should reopen after destination restart");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("restarted runtime should restore persisted state");
    attach_loopback_cluster(&runtime, &fixture.destination).await;
    let domains = BTreeMap::from([(
        fixture.domain.clone(),
        unpaced_domain_state(fixture.domain.as_str()),
    )]);
    runtime
        .apply_cluster_state(
            &fixture.destination,
            1,
            &domains,
            &BTreeMap::new(),
            &ClusterSchedule::from_iter([fixture.base_schedule.clone()]),
        )
        .await
        .expect("the restarted destination runs the domain under the base revision");
    let moved = DomainNodeUnitRef::single(fixture.entity.in_domain(&fixture.domain));
    assert!(
        runtime
            .inner
            .prepared_runtime_state_handoffs
            .contains_key(&moved),
        "the base revision does not activate the move's preparation"
    );
    let mut running = runtime
        .inner
        .executions
        .get(&fixture.domain)
        .expect("the restarted destination runs the domain")
        .shutdown
        .subscribe();
    fixture.engage_gate(&runtime, &coordination).await;

    let request = ActivateOwnershipHandoffStateRequest {
        coordination: coordination.clone(),
        operation_id: fixture.operation_id.clone(),
        source: fixture.source.clone(),
        destination: fixture.destination.clone(),
        source_incarnation,
        destination_incarnation: prepared_destination_incarnation,
        domain: fixture.domain.clone(),
        unit: nervix_models::Unit::SINGLE,
        entity: fixture.entity.clone(),
        base_schedule_fingerprint: fixture.base_schedule_fingerprint,
        target_schedule_fingerprint: fixture.target_schedule_fingerprint,
        activation_budget: Duration::from_secs(30),
    };
    // The request is polled first, so it reaches the preparation while the domain still runs the
    // base revision; the committed revision's application follows it.
    let activation = runtime.wait_for_ownership_handoff_activation(&request);
    let committed = ClusterSchedule::from_iter([fixture.target_schedule.clone()]);
    let clock_authorities = BTreeMap::new();
    let application = runtime.apply_cluster_state(
        &fixture.destination,
        2,
        &domains,
        &clock_authorities,
        &committed,
    );
    let (activated, applied) = timeout(Duration::from_secs(30), async {
        tokio::join!(activation, application)
    })
    .await
    .expect("the activation and the committed revision's application should finish");
    applied.expect("the destination applies the committed revision");
    activated.expect("the destination activates the move it prepared before its restart");

    assert!(
        !*running.borrow_and_update(),
        "an activation request must not stop the domain's running execution"
    );
    assert!(
        runtime
            .inner
            .activated_runtime_state_handoffs
            .contains_key(&moved),
        "the committed revision activates the move"
    );
    assert!(
        !runtime
            .inner
            .prepared_runtime_state_handoffs
            .contains_key(&moved),
        "the activated move leaves no preparation behind"
    );
}
