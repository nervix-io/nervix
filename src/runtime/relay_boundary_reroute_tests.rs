//! Relay boundary tests for a delivery whose unit's route changes before it reaches the owner.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests that a delivery a route change retires before it reached the owner it
//!   named follows the unit to its new owner, from the ingest path and from a relay producer, that
//!   a producer whose own branch ended fails such a delivery instead, and that a delivery whose
//!   owner left before the delivery reached it waits, holding no dispatch gate, for the route that
//!   replaces that owner or for the owner's return, and fails only when its admission bound
//!   passes or its node is stopping.
//! - **Depends on.** The runtime relay boundary, a loopback cluster and the runtime test fixtures.
//! - **Must not know.** Production control-plane orchestration or external connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use nervix_models::{AckMode, ClusterNodeName, RelayName};
use nervix_primitives::{
    sync::Arc,
    time::{sleep, timeout},
};

use super::*;
use crate::runtime_ack::{AckCompletion, AckOutcome, AckSet};

/// A runtime on a loopback cluster of one node, `node-1`, with domain `default`, and the boundary
/// of its relay `orders`, whose single unit `node-2` owns. Nothing routes to `node-2`, as nothing
/// routes to a node that stopped.
async fn runtime_forwarding_to_a_departed_owner() -> (Runtime, Arc<RelayBoundaryServices>) {
    let runtime = Runtime::default();
    attach_loopback_cluster(&runtime, &named("node-1")).await;
    install_unpaced_test_domain(&runtime, &domain("default"));
    let dispatcher = runtime
        .inner
        .remote_dispatcher
        .load_full()
        .assured("attaching the loopback cluster attaches the dispatcher");
    let services = Arc::new(RelayBoundaryServices::new(
        RelayBoundaryFanout::direct_with_capacity(STUPID_CHANNEL_CAPACITY_REMOVE_ME),
        0,
        0,
        Vec::new(),
        Some(dispatcher),
        RelayBranchPresences::single(Arc::new(BranchPresence::new())),
        Arc::new(crate::runtime_ack::AckRootTracker::default()),
    ));
    services.replace_owner_routes(RelayOwnerRoutes::single(named::<ClusterNodeName>("node-2")));
    (runtime, services)
}

fn acknowledged_batch() -> (RelayRecordBatch, AckSet, AckCompletion) {
    let (acks, completion) = AckSet::root();
    let mut batch = quiesce_test_batch();
    batch.acks = vec![acks.clone()];
    (batch, acks, completion)
}

/// A producer of relay `orders` on this node, as a processor or reingestor holds one, whose own
/// branch ends when `lifetime` is cancelled.
fn relay_producer(
    runtime: &Runtime,
    services: &Arc<RelayBoundaryServices>,
    lifetime: &CancellationToken,
) -> ConcreteRelayRuntime {
    ConcreteRelayRuntime::new(ConcreteRelayRuntimeBuild {
        key: None,
        runtime: runtime.clone(),
        domain: domain("default"),
        relay: named("orders"),
        services: services.clone(),
        delivery_lifetime: lifetime.clone(),
    })
}

/// The forced recovery installation that moves the relay's unit from `node-2` to this node,
/// `node-1`, once it has begun: it holds the unit's dispatch gate and has retired the route that
/// names `node-2`, and it has not yet published the route that replaces it.
struct FailoverInstallation {
    route: RelayOwnerRoutes,
    gate: RelayDispatchGateLease,
}

impl FailoverInstallation {
    fn begin(services: &RelayBoundaryServices) -> Self {
        let route = RelayOwnerRoutes::single(named::<ClusterNodeName>("node-1"));
        let gate = RelayDispatchGateLease::engage(
            services.fanout.dispatch_gate(),
            Instant::now() + Duration::from_secs(30),
            "forced recovery installs the unit's new route",
        );
        services.fence_routing_change(&route, &[]);
        Self { route, gate }
    }

    /// Waits, as the installation does before it swaps the unit's owner, until no delivery holds a
    /// dispatch permit of the gate. `expectation` says why the deliveries release their permits
    /// within a second.
    async fn wait_quiescent(&mut self, expectation: &str) {
        let fenced = timeout(Duration::from_secs(1), self.gate.wait_quiescent())
            .await
            .expect(expectation);
        assert!(fenced, "the installation's fence completes");
    }

    /// Starts the unit's owner on this node with one attached runtime consumer, then publishes the
    /// route that names this node and releases the gate.
    fn complete(self, runtime: &Runtime, services: &Arc<RelayBoundaryServices>) -> NewOwner {
        let consumer = RelayRuntimeFanIn::new(
            services
                .fanout
                .runtime_consumer_receiver_for_mode(AckMode::Attached),
        );
        let task = runtime
            .spawn_relay_owner_task(
                &domain("default"),
                &named("orders"),
                Unit::SINGLE,
                services.clone(),
                RelayRetention::default(),
            )
            .expect("a whole relay schedules its single unit");
        services.replace_owner_routes(self.route);
        drop(self.gate);
        NewOwner { consumer, task }
    }
}

/// The unit's owner on this node once the failover installed it, and the runtime consumer attached
/// to it.
struct NewOwner {
    consumer: RelayRuntimeFanIn,
    task: RelayOwnerTask,
}

impl NewOwner {
    /// Asserts that the forwarded batch reaches the consumer exactly once and that its record, whose
    /// source share the test holds as `acks`, succeeds through this owner alone.
    async fn assert_delivers_once(&mut self, acks: AckSet, completion: AckCompletion) {
        acks.ack_success();
        let delivered = timeout(Duration::from_secs(5), self.consumer.recv())
            .await
            .expect("the new owner fans the batch out")
            .expect("the consumer stays attached");
        assert_eq!(delivered.batch.batch().num_rows(), 1);
        for ack in delivered.acks.iter() {
            ack.ack_success();
        }
        assert_eq!(
            timeout(Duration::from_secs(5), completion.wait())
                .await
                .expect("the record's acknowledgement completes"),
            AckOutcome::Ack,
            "the record succeeds through its new owner alone"
        );
        self.assert_receives_nothing_more("the batch reaches its consumer exactly once")
            .await;
    }

    async fn assert_receives_nothing_more(&mut self, expectation: &str) {
        sleep(Duration::from_secs(1)).await;
        assert!(
            matches!(self.consumer.try_recv(), RelayTryRecv::Empty),
            "{expectation}"
        );
    }

    async fn stop(self) {
        self.task
            .stop(crate::runtime::TaskStopWait::Teardown {
                grace: Duration::from_secs(1),
            })
            .await
            .expect("the relay owner stops");
    }
}

/// A forced failover moves a relay unit away from a node that stopped while this node is still
/// delivering a batch to it. Installing the new route retires the delivery's channel. Nothing of
/// the delivery reached the stopped owner, so the installation does not wait on that owner, and
/// the batch goes exactly once to the owner the new route names, here this node.
#[nervix_primitives::test(start_paused = true)]
async fn a_batch_forwarded_to_a_departed_owner_follows_the_route_that_replaces_it() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    let domain = domain("default");
    let relay = named::<RelayName>("orders");
    let (batch, acks, completion) = acknowledged_batch();
    let forward = nervix_primitives::task::spawn({
        let runtime = runtime.clone();
        let services = services.clone();
        async move {
            runtime
                .ingest_stream_boundary_message(&domain, &relay, &services, &batch)
                .await
        }
    });
    // The delivery now holds its dispatch permits and waits for a connection to `node-2`.
    sleep(Duration::from_millis(100)).await;
    assert!(
        !forward.is_finished(),
        "a delivery to an owner nothing routes to keeps trying within its no-progress bound"
    );

    let mut installation = FailoverInstallation::begin(&services);
    installation
        .wait_quiescent(
            "the retired delivery releases its dispatch permits at once instead of asking the \
             stopped owner to cancel it",
        )
        .await;
    let mut new_owner = installation.complete(&runtime, &services);

    timeout(Duration::from_secs(5), forward)
        .await
        .expect("the delivery ends once the new route is in force")
        .expect("the delivery task joins")
        .expect("the unit's new owner admits the batch");
    new_owner.assert_delivers_once(acks, completion).await;
    new_owner.stop().await;
}

/// The same failover for a producer of the relay on this node, as a processor or reingestor holds
/// one: its delivery follows the unit to the owner on the new route as well.
#[nervix_primitives::test(start_paused = true)]
async fn a_produced_batch_forwarded_to_a_departed_owner_follows_the_route_that_replaces_it() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    let lifetime = CancellationToken::new();
    let mut producer = relay_producer(&runtime, &services, &lifetime);
    let (batch, acks, completion) = acknowledged_batch();
    let forward =
        nervix_primitives::task::spawn(async move { producer.dispatch_boundary(&batch).await });
    // The delivery now holds its dispatch permits and waits for a connection to `node-2`.
    sleep(Duration::from_millis(100)).await;
    assert!(
        !forward.is_finished(),
        "a delivery to an owner nothing routes to keeps trying within its no-progress bound"
    );

    let mut installation = FailoverInstallation::begin(&services);
    installation
        .wait_quiescent(
            "the producer's retired delivery releases its dispatch permits at once instead of \
             asking the stopped owner to cancel it",
        )
        .await;
    let mut new_owner = installation.complete(&runtime, &services);

    timeout(Duration::from_secs(5), forward)
        .await
        .expect("the delivery ends once the new route is in force")
        .expect("the delivery task joins")
        .expect("the unit's new owner admits the batch");
    new_owner.assert_delivers_once(acks, completion).await;
    new_owner.stop().await;
}

/// A producer whose own branch ends while the failover holds its delivery fails the delivery's
/// rows for its source to retry, as the end of its branch fails its other work still waiting for
/// admission, rather than handing them to the unit's new owner.
#[nervix_primitives::test(start_paused = true)]
async fn a_producer_that_ends_during_the_failover_fails_its_forwarded_rows() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    let lifetime = CancellationToken::new();
    let mut producer = relay_producer(&runtime, &services, &lifetime);
    let (batch, _acks, completion) = acknowledged_batch();
    let forward =
        nervix_primitives::task::spawn(async move { producer.dispatch_boundary(&batch).await });
    // The delivery now holds its dispatch permits and waits for a connection to `node-2`.
    sleep(Duration::from_millis(100)).await;
    assert!(
        !forward.is_finished(),
        "a delivery to an owner nothing routes to keeps trying within its no-progress bound"
    );

    let mut installation = FailoverInstallation::begin(&services);
    lifetime.cancel();
    installation
        .wait_quiescent(
            "the ended producer's delivery releases its dispatch permits at once instead of \
             asking the stopped owner to cancel it",
        )
        .await;
    let mut new_owner = installation.complete(&runtime, &services);

    let unadmitted = timeout(Duration::from_secs(5), forward)
        .await
        .expect("the delivery ends with its producer")
        .expect("the delivery task joins")
        .expect_err("a producer whose branch ended fails its rows");
    assert_eq!(unadmitted.batch.batch().num_rows(), 1);
    assert!(
        matches!(completion.wait().await, AckOutcome::NoAck(_)),
        "the record fails for its source to retry"
    );
    new_owner
        .assert_receives_nothing_more("the new owner never receives rows of a producer that ended")
        .await;
    new_owner.stop().await;
}

/// Publishes, as discovery does, a membership that lists `live` alone.
fn publish_membership(runtime: &Runtime, live: &[&str]) {
    let live = live
        .iter()
        .map(|name| named::<ClusterNodeName>(name))
        .collect::<std::collections::BTreeSet<_>>();
    runtime
        .inner
        .remote_dispatcher
        .load_full()
        .assured("attaching the loopback cluster attaches the dispatcher")
        .interconnect
        .replace_live_nodes(&live);
}

/// Forwards `batch` to the owner of relay `orders` from the ingest path, in its own task.
fn spawn_forward(
    runtime: &Runtime,
    services: &Arc<RelayBoundaryServices>,
    batch: RelayRecordBatch,
) -> nervix_primitives::task::JoinHandle<RelayDispatchResult> {
    let runtime = runtime.clone();
    let services = services.clone();
    nervix_primitives::task::spawn(async move {
        runtime
            .ingest_stream_boundary_message(&domain("default"), &named("orders"), &services, &batch)
            .await
    })
}

/// Membership can remove a stopped owner before this node installs the failover that replaces it,
/// so a delivery learns that its owner left while the route naming that owner is still in force.
/// Nothing of the delivery reached the owner, so it waits for the route that replaces the owner
/// instead of failing, and gives its dispatch gates back meanwhile: another change engaging them
/// finds no delivery to wait for. The batch then goes exactly once to the owner the new route
/// names, here this node.
#[nervix_primitives::test(start_paused = true)]
async fn a_delivery_whose_owner_left_waits_for_the_route_that_replaces_it_holding_no_gate() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    publish_membership(&runtime, &["node-1"]);
    let (batch, acks, completion) = acknowledged_batch();
    let forward = spawn_forward(&runtime, &services, batch);
    sleep(Duration::from_millis(100)).await;
    assert!(
        !forward.is_finished(),
        "a delivery whose owner left before it reached that owner waits for the route that \
         replaces the owner"
    );

    let mut other_change = RelayDispatchGateLease::engage_producers(
        services.fanout.dispatch_gate(),
        Instant::now() + Duration::from_secs(30),
        "another change of the domain holds the relay's producers",
    );
    let quiescent = timeout(Duration::from_secs(1), other_change.wait_quiescent())
        .await
        .expect("the waiting delivery holds no dispatch permit for another change to wait for");
    assert!(quiescent, "the other change's gate becomes quiescent");
    drop(other_change);
    sleep(Duration::from_millis(100)).await;
    assert!(
        !forward.is_finished(),
        "a change that leaves the route in force does not end the wait"
    );

    let mut installation = FailoverInstallation::begin(&services);
    installation
        .wait_quiescent("the waiting delivery holds no dispatch permit")
        .await;
    let mut new_owner = installation.complete(&runtime, &services);

    timeout(Duration::from_secs(5), forward)
        .await
        .expect("the delivery ends once the new route is in force")
        .expect("the delivery task joins")
        .expect("the unit's new owner admits the batch");
    new_owner.assert_delivers_once(acks, completion).await;
    new_owner.stop().await;
}

/// A delivery that waits for the route that replaces an owner that left goes to that owner again
/// when membership lists it again before any route change, as a voter that returns within the
/// failover grace keeps its work. It asks for its grant there once more, and when the owner leaves
/// again it waits again, until the failover's route takes the batch to the unit's new owner exactly
/// once.
#[nervix_primitives::test(start_paused = true)]
async fn a_delivery_whose_owner_left_goes_to_that_owner_when_it_returns() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    let domain = domain("default");
    let faults = &runtime.inner.fault_injection;
    faults.pause_relay_before_grant(domain.as_str().to_string());
    let (batch, acks, completion) = acknowledged_batch();
    let forward = spawn_forward(&runtime, &services, batch);
    timeout(
        Duration::from_secs(5),
        faults.wait_for_relay_before_grant_pause(domain.as_str()),
    )
    .await
    .expect("the delivery reaches its grant boundary");
    publish_membership(&runtime, &["node-1"]);
    faults.release_relay_before_grant_pause(domain.as_str());
    sleep(Duration::from_millis(100)).await;
    assert!(
        !forward.is_finished(),
        "the delivery waits once it finds its owner gone"
    );

    faults.pause_relay_before_grant(domain.as_str().to_string());
    publish_membership(&runtime, &["node-1", "node-2"]);
    timeout(
        Duration::from_secs(5),
        faults.wait_for_relay_before_grant_pause(domain.as_str()),
    )
    .await
    .expect("the delivery asks the returned owner for its grant on the unchanged route");

    publish_membership(&runtime, &["node-1"]);
    faults.release_relay_before_grant_pause(domain.as_str());
    sleep(Duration::from_millis(100)).await;
    assert!(
        !forward.is_finished(),
        "the delivery waits again once its owner leaves again"
    );
    let mut installation = FailoverInstallation::begin(&services);
    installation
        .wait_quiescent("the waiting delivery holds no dispatch permit")
        .await;
    let mut new_owner = installation.complete(&runtime, &services);

    timeout(Duration::from_secs(5), forward)
        .await
        .expect("the delivery ends once the new route is in force")
        .expect("the delivery task joins")
        .expect("the unit's new owner admits the batch");
    new_owner.assert_delivers_once(acks, completion).await;
    new_owner.stop().await;
}

/// A delivery whose owner left and that no route change or return of that owner ever lets carry
/// on fails when its admission bound, `REMOTE_RELAY_TOTAL_TIMEOUT`, passes, and not before. It
/// never asks the owner it never reached to cancel it, and the failure names both the bound and
/// that owner.
#[nervix_primitives::test(start_paused = true)]
async fn a_delivery_whose_owner_left_fails_only_when_its_admission_bound_passes() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    let bound = runtime
        .inner
        .remote_dispatcher
        .load_full()
        .assured("attaching the loopback cluster attaches the dispatcher")
        .registry
        .timing()
        .remote_relay_total_timeout;
    publish_membership(&runtime, &["node-1"]);
    let (batch, _acks, completion) = acknowledged_batch();
    let started = Instant::now();

    let after_the_bound = bound
        .checked_add(Duration::from_secs(60))
        .assured("the five-minute admission bound and a minute fit a duration");
    let unadmitted = timeout(after_the_bound, spawn_forward(&runtime, &services, batch))
        .await
        .expect("the delivery ends by its admission bound")
        .expect("the delivery task joins")
        .expect_err("no route ever replaces the owner that left");
    let waited = started.elapsed();

    assert_eq!(unadmitted.batch.batch().num_rows(), 1);
    assert!(
        waited >= bound,
        "the delivery waited for the route that replaces its owner for its whole admission bound \
         of {bound:?}, not {waited:?}"
    );
    let AckOutcome::NoAck(reason) = completion.wait().await else {
        panic!("a delivery whose admission bound passed fails its record");
    };
    assert!(
        reason.contains("waiting for relay admission from node 'node-2'")
            && reason.contains("never reached node 'node-2'"),
        "the failure names the bound and the owner the delivery never reached: {reason}"
    );
}

/// A stopping node awaits no route that replaces an owner that left. Its save holds its
/// installation turn, so no such route reaches it before it stops, and when every node stops, none
/// is published at all. A delivery that finds its owner gone on a stopping node therefore fails at
/// once, as work that reaches a peer after that peer saved does, and the failure says why.
#[nervix_primitives::test(start_paused = true)]
async fn a_stopping_node_fails_a_delivery_whose_owner_left_at_once() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    runtime.close_local_intake();
    publish_membership(&runtime, &["node-1"]);
    let (batch, _acks, completion) = acknowledged_batch();

    let unadmitted = timeout(
        Duration::from_secs(5),
        spawn_forward(&runtime, &services, batch),
    )
    .await
    .expect("a stopping node awaits no route that replaces the owner that left")
    .expect("the delivery task joins")
    .expect_err("nothing replaces the owner that left on a stopping node");

    assert_eq!(unadmitted.batch.batch().num_rows(), 1);
    let AckOutcome::NoAck(reason) = completion.wait().await else {
        panic!("a delivery a stopping node gives up fails its record");
    };
    assert!(
        reason.contains("this node is stopping") && reason.contains("never reached node 'node-2'"),
        "the failure names the stop and the owner the delivery never reached: {reason}"
    );
}

/// A delivery that already waits for the route that replaces an owner that left ends as its node
/// begins stopping, instead of holding the node's stop until its admission bound passes.
#[nervix_primitives::test(start_paused = true)]
async fn a_delivery_waiting_for_its_departed_owner_fails_once_its_node_begins_stopping() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    publish_membership(&runtime, &["node-1"]);
    let (batch, _acks, completion) = acknowledged_batch();
    let forward = spawn_forward(&runtime, &services, batch);
    sleep(Duration::from_millis(100)).await;
    assert!(
        !forward.is_finished(),
        "a serving node's delivery waits for the route that replaces the owner that left"
    );

    runtime.close_local_intake();
    let unadmitted = timeout(Duration::from_secs(5), forward)
        .await
        .expect("the wait ends as its node begins stopping")
        .expect("the delivery task joins")
        .expect_err("nothing replaces the owner that left on a stopping node");

    assert_eq!(unadmitted.batch.batch().num_rows(), 1);
    let AckOutcome::NoAck(reason) = completion.wait().await else {
        panic!("a delivery a stopping node gives up fails its record");
    };
    assert!(
        reason.contains("this node is stopping") && reason.contains("never reached node 'node-2'"),
        "the failure names the stop and the owner the delivery never reached: {reason}"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_batch_held_before_its_grant_follows_the_route_that_replaces_its_departed_owner() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    let domain = domain("default");
    let relay = named::<RelayName>("orders");
    runtime
        .inner
        .fault_injection
        .pause_relay_before_grant(domain.as_str().to_string());
    let (batch, acks, completion) = acknowledged_batch();
    let forward = nervix_primitives::task::spawn({
        let runtime = runtime.clone();
        let services = services.clone();
        let domain = domain.clone();
        async move {
            runtime
                .ingest_stream_boundary_message(&domain, &relay, &services, &batch)
                .await
        }
    });
    timeout(
        Duration::from_secs(5),
        runtime
            .inner
            .fault_injection
            .wait_for_relay_before_grant_pause(domain.as_str()),
    )
    .await
    .expect("the delivery reaches its grant boundary holding its dispatch permits");

    let mut installation = FailoverInstallation::begin(&services);
    runtime
        .inner
        .fault_injection
        .release_relay_before_grant_pause(domain.as_str());
    installation
        .wait_quiescent(
            "the retired delivery releases its dispatch permits without contacting anyone",
        )
        .await;
    let mut new_owner = installation.complete(&runtime, &services);

    timeout(Duration::from_secs(5), forward)
        .await
        .expect("the delivery ends once the new route is in force")
        .expect("the delivery task joins")
        .expect("the unit's new owner admits the batch");
    new_owner.assert_delivers_once(acks, completion).await;
    new_owner.stop().await;
}

/// The same failover, when the route change retires the delivery's channel while the delivery
/// waits for room to store its records: it has sent nothing on that channel, so it follows the new
/// route as well, and its record succeeds through the new owner alone.
#[nervix_primitives::test(start_paused = true)]
async fn a_batch_waiting_to_store_its_records_follows_the_route_that_replaces_its_departed_owner() {
    let (runtime, services) = runtime_forwarding_to_a_departed_owner().await;
    let domain = domain("default");
    let relay = named::<RelayName>("orders");
    let local = named::<ClusterNodeName>("node-1");
    runtime
        .inner
        .fault_injection
        .fill_relay_memory_before_next_record_storage_on(local.clone());
    let (batch, acks, completion) = acknowledged_batch();
    let forward = nervix_primitives::task::spawn({
        let runtime = runtime.clone();
        let services = services.clone();
        async move {
            runtime
                .ingest_stream_boundary_message(&domain, &relay, &services, &batch)
                .await
        }
    });
    // The delivery now holds its dispatch permits and waits for room to store its records.
    sleep(Duration::from_millis(100)).await;
    assert!(
        runtime
            .inner
            .fault_injection
            .relay_memory_fill_holds_on(&local),
        "the delivery reached the storage of its records"
    );
    assert!(
        !forward.is_finished(),
        "the delivery waits for room to store its records"
    );

    let mut installation = FailoverInstallation::begin(&services);
    installation
        .wait_quiescent(
            "the delivery whose channel was retired before it sent anything releases its dispatch \
             permits at once",
        )
        .await;
    runtime
        .inner
        .fault_injection
        .release_relay_memory_fill_on(&local);
    let mut new_owner = installation.complete(&runtime, &services);

    timeout(Duration::from_secs(5), forward)
        .await
        .expect("the delivery ends once the new route is in force")
        .expect("the delivery task joins")
        .expect("the unit's new owner admits the batch");
    new_owner.assert_delivers_once(acks, completion).await;
    new_owner.stop().await;
}
