//! The order in which effectful walks of the peer target table act.
//!
//! Layer: test harness.
//! - **Owns.** The order in which shutdown and credential replacement retire peer targets.
//! - **Depends on.** The production transport state, its target table and slot cancellations.
//! - **Must not know.** Relay routing, the simulation runner or the publication backend's internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "the test harness creates and retires independent production owners"
    )
)]

use super::*;

/// Eight peers: their hash order matches node order only by a one-in-40,320 accident, so a walk in
/// hash order fails these tests in practice whatever the process's hash seed is.
const PEERS: [&str; 8] = [
    "peer-a", "peer-b", "peer-c", "peer-d", "peer-e", "peer-f", "peer-g", "peer-h",
];

/// Installs one target for every peer and returns the peers in node order.
fn install_peers(state: &TransportState) -> Vec<ClusterNodeName> {
    let mut nodes = Vec::new();
    for (index, peer) in PEERS.iter().enumerate() {
        let node = ClusterNodeName::parse(peer).expect("the peer names are valid node names");
        let port = 7500 + u16::try_from(index).expect("eight peers fit a port offset");
        state
            .install_outbound_target(
                node.clone(),
                NodeEndpoint::new("localhost", port),
                OutboundDial::Advertised,
            )
            .expect("eight peers fit the default peer limit");
        nodes.push(node);
    }
    nodes.sort();
    nodes
}

/// The cancellation of a target's first slot, which a retirement cancels before its other slots.
fn first_slot_cancellation(target: &OutboundTarget) -> CancellationToken {
    for class in PoolClass::ALL {
        if let Some(slot) = target.slots(class).first() {
            return slot.cancel.clone();
        }
    }
    panic!("every peer target has slots");
}

/// The retirement reports of every peer and the watchers that send them.
struct Retirements {
    reports: mpsc::UnboundedReceiver<ClusterNodeName>,
    watchers: Vec<nervix_primitives::task::JoinHandle<()>>,
}

impl Retirements {
    /// Watches every peer's first slot and reports each peer once its slot is cancelled. The test
    /// runtime runs on one thread and runs woken tasks in the order they were woken, so the reports
    /// arrive in the order a walk retired the targets.
    async fn watch(state: &TransportState, nodes: &[ClusterNodeName]) -> Self {
        let (sender, reports) = mpsc::unbounded_channel();
        let targets = state.targets.load();
        let mut watchers = Vec::with_capacity(nodes.len());
        for node in nodes {
            let target = targets.get(node).expect("every peer has a target");
            let cancellation = first_slot_cancellation(target);
            let sender = sender.clone();
            let node = node.clone();
            watchers.push(nervix_primitives::task::spawn(async move {
                cancellation.cancelled().await;
                sender
                    .send(node)
                    .expect("the test keeps its receiver until every peer reported");
            }));
        }
        // Every watcher registers for its cancellation before the walk under test runs, and the
        // publication guard ends before this task awaits.
        drop(targets);
        nervix_primitives::task::yield_now().await;
        Self { reports, watchers }
    }

    /// The peers in the order the walk retired them, once every watcher reported and ended.
    async fn order(mut self) -> Vec<ClusterNodeName> {
        let mut order = Vec::with_capacity(self.watchers.len());
        while order.len() < self.watchers.len() {
            let node = self
                .reports
                .recv()
                .await
                .expect("every watcher reports its peer before it ends");
            order.push(node);
        }
        for watcher in self.watchers {
            watcher.await.expect("every watcher ends after it reports");
        }
        order
    }
}

#[nervix_primitives::test]
async fn cancelling_every_slot_retires_the_peer_targets_in_node_order() {
    let fixture = crate::tests::bound_transports_with_options(TransportOptions::default()).await;
    let state = &fixture.transport_a.inner;
    let nodes = install_peers(state);
    let retirements = Retirements::watch(state, &nodes).await;

    state.cancel_all_slots();

    assert_eq!(retirements.order().await, nodes);
    fixture.transport_a.shutdown().await;
    fixture.transport_b.shutdown().await;
}

#[nervix_primitives::test]
async fn credential_replacement_retires_the_peer_targets_in_node_order() {
    let fixture = crate::tests::bound_transports_with_options(TransportOptions::default()).await;
    let state = &fixture.transport_a.inner;
    let nodes = install_peers(state);
    let retirements = Retirements::watch(state, &nodes).await;

    state
        .replace_tls(fixture.reissued_tls_a())
        .await
        .expect("the first transport's own identity is reissued");

    assert_eq!(retirements.order().await, nodes);
    let replaced = state.targets.load();
    for node in &nodes {
        let target = replaced.get(node).expect("every peer keeps a target");
        assert!(
            !first_slot_cancellation(target).is_cancelled(),
            "the replacement targets start with live slots"
        );
    }
    fixture.transport_a.shutdown().await;
    fixture.transport_b.shutdown().await;
}
