//! Relay channel lifetime properties over the production publication owner.
//!
//! Layer: test harness.
//! - **Owns.** Retained identity, cancellation, sequence and bounded churn assertions.
//! - **Depends on.** RelayChannels and typed runtime branch identities.
//! - **Must not know.** Socket transport or publication backend internals.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "the test harness creates and retires independent production owners"
    )
)]

use super::*;

fn channel() -> RelayOutboundChannel {
    RelayOutboundChannel {
        node_id: ClusterNodeName::parse("node-2").expect("valid peer"),
        relay: RelayName::parse("events").expect("valid relay"),
        kind: RelayPayloadKind::Routed,
    }
}

#[test]
fn retained_branches_follow_routes_but_never_follow_recreated_incarnations() {
    let channels = RelayChannels::default();
    let key = string_branch_key("tenant", "acme");
    let branch = channels.bind(&key);
    let ingress = branch.ingress();
    let outbound = branch.outbound(channel());
    assert!(StdArc::ptr_eq(&branch, &channels.bind(&key)));
    assert!(StdArc::ptr_eq(&outbound, &branch.outbound(channel())));
    channels.replace_owner();
    assert!(ingress.cancellation().is_cancelled());
    assert!(outbound.cancellation().is_cancelled());
    let current = branch.ingress();
    assert!(!current.cancellation().is_cancelled());
    channels.retire(&key);
    assert!(current.cancellation().is_cancelled());
    assert!(branch.is_retired());
    assert!(branch.ingress().cancellation().is_cancelled());
    let recreated = channels.bind(&key);
    assert!(!StdArc::ptr_eq(&branch, &recreated));
    assert!(!recreated.ingress().cancellation().is_cancelled());
    channels.retire_all();
    assert!(recreated.is_retired());
    assert!(recreated.ingress().cancellation().is_cancelled());
    assert!(channels.branches.load().is_empty());
}

#[test]
fn bolero_relay_sequences_preserve_exact_lifetimes_and_order() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let channels = RelayChannels::default();
            let keys = [
                string_branch_key("tenant", "acme"),
                string_branch_key("tenant", "beta"),
            ];
            let mut retained = Vec::new();
            let mut last = HashMap::default();
            for byte in bytes {
                let key = &keys[usize::from(byte / 5) % keys.len()];
                match byte % 5 {
                    0 | 1 => {
                        let branch = channels.bind(key);
                        let slot = branch.outbound(channel());
                        let delivery = slot.next_delivery();
                        if let Some(previous) =
                            last.insert(delivery.channel_incarnation, delivery.sequence)
                        {
                            assert_eq!(delivery.sequence, previous + 1);
                        } else {
                            assert_eq!(delivery.sequence, 0);
                        }
                        retained.push((key.clone(), branch, slot));
                    }
                    2 => {
                        channels.replace_owner();
                        for (_, _, slot) in &retained {
                            assert!(slot.cancellation().is_cancelled());
                        }
                    }
                    3 => {
                        channels.retire(key);
                        for (selected, branch, slot) in &retained {
                            if selected == key {
                                assert!(branch.is_retired());
                                assert!(slot.cancellation().is_cancelled());
                            }
                        }
                    }
                    _ => channels.replace_destinations(),
                }
                assert!(channels.branches.load().len() <= keys.len());
                for branch in channels.branches.load().values() {
                    assert!(branch.routes().destinations.load().len() <= 1);
                }
            }
            channels.retire_all();
            assert!(
                retained
                    .iter()
                    .all(|(_, branch, slot)| branch.is_retired()
                        && slot.cancellation().is_cancelled())
            );
        });
}

#[test]
fn withdrawal_releases_the_branch_after_its_last_producer_ends() {
    let channels = RelayChannels::default();
    let key = string_branch_key("tenant", "acme");
    let retained = channels.bind(&key);
    let weak = StdArc::downgrade(&retained);
    channels.retire(&key);
    assert!(weak.upgrade().is_some());
    drop(retained);
    assert!(weak.upgrade().is_none());
}

#[nervix_primitives::test]
async fn retirement_ends_a_queued_channel_wait_without_waiting_for_its_holder() {
    use futures_util::FutureExt as _;
    let channels = RelayChannels::default();
    let branch = channels.bind(&None);
    let slot = branch.outbound(channel());
    let holder = slot
        .lock_for_delivery()
        .await
        .expect("active channel grants its gate");
    let waiter = slot.lock_for_delivery();
    tokio::pin!(waiter);
    assert!(waiter.as_mut().now_or_never().is_none());
    channels.retire(&None);
    assert!(waiter.await.is_none());
    assert!(slot.lock_for_delivery().await.is_none());
    drop(holder);
}

#[test]
#[ignore = "same-host retained-channel latency, allocation and churn measurement"]
fn relay_channel_cost() {
    let channels = RelayChannels::default();
    let key = string_branch_key("tenant", "acme");
    let branch = channels.bind(&key);
    let allocated = tikv_jemalloc_ctl::thread::allocatedp::read().expect("server uses jemalloc");
    let deallocated =
        tikv_jemalloc_ctl::thread::deallocatedp::read().expect("server uses jemalloc");
    let retained = branch.ingress();
    let references = StdArc::strong_count(&retained);
    let selected = branch.ingress();
    assert_eq!(StdArc::strong_count(&retained), references + 1);
    drop(selected);
    for retained_branch in [false, true] {
        for sample in 0..5 {
            let mut timings = Vec::with_capacity(10_000);
            let before = allocated.get();
            let whole = Instant::now();
            for _ in 0..10_000 {
                let start = Instant::now();
                let slot = if retained_branch {
                    branch.ingress()
                } else {
                    channels.bind(&key).ingress()
                };
                std::hint::black_box(slot);
                timings.push(start.elapsed().as_nanos());
            }
            let elapsed = whole.elapsed();
            let bytes = allocated.get() - before;
            timings.sort_unstable();
            println!(
                "relay-channel retained_branch={retained_branch} sample={sample} operations=10000 \
                 ns_per_operation={} p50_ns={} p95_ns={} p99_ns={} allocated_bytes={bytes}",
                elapsed.as_nanos() / 10_000,
                timings[5000],
                timings[9500],
                timings[9900]
            );
            assert_eq!(bytes, 0, "an established selection allocates no bytes");
        }
    }
    let allocated_before = allocated.get();
    let freed_before = deallocated.get();
    let start = Instant::now();
    for _ in 0..10_000 {
        channels.retire(&key);
        let current = channels.bind(&key);
        std::hint::black_box(current.outbound(channel()));
        assert_eq!(channels.branches.load().len(), 1);
    }
    channels.retire(&key);
    let allocated_churn = allocated.get() - allocated_before;
    let freed_churn = deallocated.get() - freed_before;
    println!(
        "relay-channel-churn cycles=10000 ns_per_cycle={} allocated_bytes={allocated_churn} \
         deallocated_bytes={freed_churn} live_branches={}",
        start.elapsed().as_nanos() / 10_000,
        channels.branches.load().len()
    );
    assert!(channels.branches.load().is_empty());
}

#[test]
fn subscription_generations_preserve_equal_interests_and_release_withdrawn_peers() {
    use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation};
    let channels = RelayChannels::default();
    let branch = channels.bind(&None);
    let domain = DomainName::parse("sales").expect("valid domain");
    let relay = RelayName::parse("events").expect("valid relay");
    let node = ClusterNodeName::parse("node-2").expect("valid node");
    let identity = ClusterNodeIdentity::new(node.clone(), ClusterNodeIncarnation::new(1));
    let mut index = SubscriptionInterestIndex::default();
    index.record("sales", "events", &identity, 1);
    let snapshot = StdArc::new(index);
    let first = branch.subscriptions(snapshot.clone(), &domain, &relay);
    assert!(StdArc::ptr_eq(
        &first,
        &branch.subscriptions(snapshot, &domain, &relay)
    ));
    let slot = first
        .slot(&node, &relay)
        .expect("live subscriber has a slot");
    let mut index = SubscriptionInterestIndex::default();
    index.record("sales", "events", &identity, 1);
    let equal = branch.subscriptions(StdArc::new(index), &domain, &relay);
    assert!(StdArc::ptr_eq(
        &slot,
        &equal.slot(&node, &relay).expect("same live subscriber")
    ));
    assert!(!slot.cancellation().is_cancelled());
    let replacement = ClusterNodeIdentity::new(node.clone(), ClusterNodeIncarnation::new(2));
    let mut index = SubscriptionInterestIndex::default();
    index.record("sales", "events", &replacement, 2);
    let renewed = branch.subscriptions(StdArc::new(index), &domain, &relay);
    assert!(slot.cancellation().is_cancelled());
    let current = renewed
        .slot(&node, &relay)
        .expect("replacement subscriber is live");
    assert!(!current.cancellation().is_cancelled());
    let empty = branch.subscriptions(
        StdArc::new(SubscriptionInterestIndex::default()),
        &domain,
        &relay,
    );
    assert!(empty.slots.is_empty());
    assert!(current.cancellation().is_cancelled());
    for generation in 1..=256 {
        let node =
            ClusterNodeName::parse(&format!("peer-{generation}")).expect("valid generated node");
        let identity =
            ClusterNodeIdentity::new(node.clone(), ClusterNodeIncarnation::new(generation));
        let mut index = SubscriptionInterestIndex::default();
        index.record("sales", "events", &identity, generation);
        let selected = branch.subscriptions(StdArc::new(index), &domain, &relay);
        assert_eq!(selected.slots.len(), 1);
        assert!(
            !selected
                .slot(&node, &relay)
                .expect("one live peer")
                .cancellation()
                .is_cancelled()
        );
    }
    channels.retire_all();
    assert!(branch.is_retired());
}
