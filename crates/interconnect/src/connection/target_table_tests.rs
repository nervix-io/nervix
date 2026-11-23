//! A transport keeps a dial target for every peer it is given.
//!
//! Layer: test harness.
//! - **Owns.** Assertions that the peer target table holds every peer registered or published to
//!   it, however many there are and however their registrations race, and that a replacement or
//!   an authentication keeps the targets whose endpoint it leaves unchanged.
//! - **Depends on.** The production transport state and its target table.
//! - **Must not know.** Membership, gossip or how the application chooses the peers it publishes.

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

/// Far more peers than the clusters the other transport tests build, so a table that kept only a
/// fixed number of them would lose some.
const PEERS: u16 = 100;

fn peer(index: u16) -> ClusterNodeName {
    ClusterNodeName::parse(&format!("peer-{index:03}"))
        .expect("the peer names are valid node names")
}

fn endpoint(index: u16) -> NodeEndpoint {
    NodeEndpoint::new("localhost", 7000 + index)
}

#[nervix_primitives::test]
async fn every_registered_peer_keeps_its_target_however_many_there_are() {
    let fixture = crate::tests::bound_transports_with_options(TransportOptions::default()).await;
    let state = &fixture.transport_a.inner;

    for index in 0..PEERS {
        state.install_outbound_target(peer(index), endpoint(index), OutboundDial::Advertised);
    }

    assert_eq!(state.targets.load().len(), usize::from(PEERS));
    fixture.transport_a.shutdown().await;
    fixture.transport_b.shutdown().await;
}

#[nervix_primitives::test]
async fn every_published_peer_keeps_its_target_however_many_there_are() {
    let fixture = crate::tests::bound_transports_with_options(TransportOptions::default()).await;
    let mut endpoints = BTreeMap::new();
    for index in 0..PEERS {
        endpoints.insert(peer(index), endpoint(index));
    }

    fixture.transport_a.replace_outbound_targets(&endpoints);

    let registered = fixture
        .transport_a
        .inner
        .targets_in_node_order()
        .into_keys()
        .collect::<BTreeSet<_>>();
    let published = endpoints.into_keys().collect::<BTreeSet<_>>();
    assert_eq!(registered, published);
    fixture.transport_a.shutdown().await;
    fixture.transport_b.shutdown().await;
}

#[nervix_primitives::test]
async fn a_replacement_retires_only_the_peers_it_withdraws_or_moves() {
    let fixture = crate::tests::bound_transports_with_options(TransportOptions::default()).await;
    let state = &fixture.transport_a.inner;
    let first = BTreeMap::from([
        (peer(0), endpoint(0)),
        (peer(1), endpoint(1)),
        (peer(2), endpoint(2)),
    ]);
    fixture.transport_a.replace_outbound_targets(&first);
    let before = state.targets_in_node_order();
    assert_eq!(
        before.keys().cloned().collect::<BTreeSet<_>>(),
        first.keys().cloned().collect::<BTreeSet<_>>()
    );

    let moved = NodeEndpoint::new("localhost", 9001);
    let second = BTreeMap::from([
        (peer(0), endpoint(0)),
        (peer(1), moved.clone()),
        (peer(3), endpoint(3)),
    ]);
    fixture.transport_a.replace_outbound_targets(&second);
    let after = state.targets_in_node_order();

    assert_eq!(
        after.keys().cloned().collect::<BTreeSet<_>>(),
        second.keys().cloned().collect::<BTreeSet<_>>(),
        "the withdrawn peer is retired and the new one is dialled"
    );
    let kept_before = before
        .get(&peer(0))
        .verified("the first key set asserted above holds peer-000");
    let kept_after = after
        .get(&peer(0))
        .verified("the second key set asserted above holds peer-000");
    assert!(
        Arc::ptr_eq(&kept_before.pool, &kept_after.pool),
        "a peer published again at the same endpoint keeps its pool and connections"
    );
    let moved_before = before
        .get(&peer(1))
        .verified("the first key set asserted above holds peer-001");
    let moved_after = after
        .get(&peer(1))
        .verified("the second key set asserted above holds peer-001");
    assert_eq!(moved_after.endpoint, moved);
    assert!(
        !Arc::ptr_eq(&moved_before.pool, &moved_after.pool),
        "a peer published at a new endpoint is dialled through a new pool"
    );
    fixture.transport_a.shutdown().await;
    fixture.transport_b.shutdown().await;
}

#[nervix_primitives::test]
async fn an_authentication_at_the_registered_endpoint_keeps_its_target() {
    let fixture = crate::tests::bound_transports_with_options(TransportOptions::default()).await;
    let state = &fixture.transport_a.inner;
    let authenticated = PeerTarget::new(SocketAddr::from(([127, 0, 0, 1], 7000)), "localhost");

    let first = state.install_authenticated_target(peer(0), authenticated.clone());
    let again = state.install_authenticated_target(peer(0), authenticated);

    assert!(
        Arc::ptr_eq(&first, &again),
        "authenticating the same endpoint again keeps the registered target"
    );
    fixture.transport_a.shutdown().await;
    fixture.transport_b.shutdown().await;
}

// This real-network fixture enters the ordinary runtime from external contender threads.
// Production slot ownership has its separately registered Shuttle check.
#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn racing_registrations_each_publish_their_target() {
    let fixture = crate::tests::bound_transports_with_options(TransportOptions::default()).await;
    let barrier =
        nervix_primitives::sync::Arc::new(nervix_primitives::sync::blocking::Barrier::new(3));
    let runtime = nervix_primitives::runtime::Handle::current();
    let mut contenders = Vec::new();
    for index in 0..2 {
        let transport = fixture.transport_a.clone();
        let barrier = barrier.clone();
        let runtime = runtime.clone();
        contenders.push(nervix_primitives::thread::spawn(move || {
            let _entered = runtime.enter();
            barrier.wait();
            transport.register_outbound_target(peer(index), endpoint(index));
        }));
    }
    barrier.wait();
    for contender in contenders {
        contender.join().expect("registration contender finishes");
    }

    let registered = fixture
        .transport_a
        .inner
        .targets_in_node_order()
        .into_keys()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        registered,
        BTreeSet::from([peer(0), peer(1)]),
        "both racing registrations published their target"
    );
    fixture.transport_a.shutdown().await;
    fixture.transport_b.shutdown().await;
}
