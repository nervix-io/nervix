//! Retained pool-slot lifetime and single-worker properties.
//!
//! Layer: test harness.
//! - **Owns.** Claims, cancellation and fixed pool bounds over production slot owners.
//! - **Depends on.** OutboundTarget, SlotControl and the shared property/model harnesses.
//! - **Must not know.** Graph routing or the publication backend's internals.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "the test harness creates and retires independent production owners"
    )
)]

use super::*;

fn target() -> OutboundTarget {
    OutboundTarget::new(
        &ClusterNodeName::parse("peer").expect("valid peer"),
        NodeEndpoint::new("peer.example.com", 7443),
        OutboundDial::Advertised,
    )
}

#[nervix_primitives::test]
async fn withdrawal_ends_shared_dial_slots_and_preserves_a_recreated_pool() {
    let fixture = crate::tests::bound_transports_with_options(TransportOptions::default()).await;
    let state = &fixture.transport_a.inner;
    let endpoint = NodeEndpoint::new("localhost", 7443);
    let selected = state
        .install_outbound_target(
            fixture.node_b.clone(),
            endpoint.clone(),
            OutboundDial::Advertised,
        )
        .expect("a peer target installs");
    let updated = state
        .install_outbound_target(
            fixture.node_b.clone(),
            endpoint.clone(),
            OutboundDial::Authenticated("127.0.0.1:7443".parse().expect("literal address")),
        )
        .expect("the dial policy changes without replacing slots");
    assert!(!Arc::ptr_eq(&selected, &updated));
    for class in PoolClass::ALL {
        for (a, b) in selected.slots(class).iter().zip(updated.slots(class)) {
            assert!(Arc::ptr_eq(a, b));
        }
    }
    // Withdrawal captured the preceding wrapper before this dial-policy publication won.
    state.retire_target(&selected);
    state.withdraw_target(&fixture.node_b, &selected);
    assert!(state.targets.load().get(&fixture.node_b).is_none());

    let recreated = state
        .install_outbound_target(fixture.node_b.clone(), endpoint, OutboundDial::Advertised)
        .expect("the peer registers a new pool lifetime");
    state.withdraw_target(&fixture.node_b, &selected);
    assert!(Arc::ptr_eq(
        state
            .targets
            .load()
            .get(&fixture.node_b)
            .expect("the recreated pool remains"),
        &recreated,
    ));
    for class in PoolClass::ALL {
        assert!(
            recreated
                .slots(class)
                .iter()
                .all(|slot| !slot.cancel.is_cancelled())
        );
    }
    fixture.transport_a.shutdown().await;
    fixture.transport_b.shutdown().await;
}

#[test]
fn bolero_pool_slot_generations_have_one_worker_and_fixed_capacity() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let mut current = target();
            let mut retained = Vec::new();
            for byte in bytes {
                let class = PoolClass::ALL[usize::from(byte / 4) % PoolClass::COUNT];
                let slot = current.slots(class)
                    [usize::from(byte / 16) % class.connections_per_peer()]
                .clone();
                match byte % 4 {
                    0 => {
                        let started = slot.started.load(Ordering::Relaxed);
                        let claimed = slot.claim_worker();
                        assert_eq!(claimed, !started && !slot.cancel.is_cancelled());
                        assert!(!slot.claim_worker());
                        retained.push(slot);
                    }
                    1 => {
                        for slots in current.slots.iter() {
                            for selected in slots.iter() {
                                selected.cancel.cancel();
                            }
                        }
                        assert!(
                            retained
                                .iter()
                                .all(|selected| selected.cancel.is_cancelled())
                        );
                        current = target();
                    }
                    2 => {
                        let next = current.with_dial(OutboundDial::Authenticated(
                            "127.0.0.1:7443".parse().expect("literal address"),
                        ));
                        for pool in PoolClass::ALL {
                            for (a, b) in current.slots(pool).iter().zip(next.slots(pool)) {
                                assert!(Arc::ptr_eq(a, b));
                            }
                        }
                        current = next;
                    }
                    _ => {
                        slot.cancel.cancel();
                        assert!(!slot.claim_worker());
                    }
                }
                for pool in PoolClass::ALL {
                    assert_eq!(current.slots(pool).len(), pool.connections_per_peer());
                }
            }
        });
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_a_retained_pool_slot_claims_one_worker_and_retirement_is_final() {
    use nervix_model_harness::shuttle::check_interleavings;
    use nervix_primitives::{sync::atomic::AtomicUsize, thread};
    check_interleavings(|| {
        let target = target();
        let slot = target.slots(PoolClass::Relay)[0].clone();
        let claims = Arc::new(AtomicUsize::new(0));
        let a = slot.clone();
        let a_claims = claims.clone();
        let first = thread::spawn(move || {
            if a.claim_worker() {
                a_claims.fetch_add(1, Ordering::Relaxed);
            }
        });
        let b = slot.clone();
        let b_claims = claims.clone();
        let second = thread::spawn(move || {
            if b.claim_worker() {
                b_claims.fetch_add(1, Ordering::Relaxed);
            }
        });
        let ending = slot.clone();
        let retire = thread::spawn(move || ending.cancel.cancel());
        first.join().expect("Shuttle fails on thread panic");
        second.join().expect("Shuttle fails on thread panic");
        retire.join().expect("Shuttle fails on thread panic");
        assert!(claims.load(Ordering::Relaxed) <= 1);
        assert!(slot.cancel.is_cancelled());
        assert!(!slot.claim_worker());
    });
}
