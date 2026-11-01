//! Registration and retirement races over production relay channel owners.
//!
//! Layer: test harness.
//! - **Owns.** Single-winner binding and cancellation across route and branch lifetimes.
//! - **Depends on.** RelayChannels and the primitive/model boundaries.
//! - **Must not know.** ArcSwap internals, real sockets or graph scheduling.

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

use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::thread;

use super::*;

const JOINS: &str = "Shuttle fails an execution when a model thread panics";

#[test]
fn shuttle_forced_routing_fence_cancels_racing_bindings_until_successor_publication() {
    check_interleavings(|| {
        let channels = Arc::new(RelayChannels::default());
        let branch = channels.bind(&None, Unit::Single);
        let retained = branch.ingress();
        let producer = thread::spawn(move || {
            branch.outbound(RelayOutboundChannel {
                node_id: ClusterNodeName::parse("node-3").assured("valid peer"),
                relay: RelayName::parse("events").assured("valid relay"),
                kind: RelayPayloadKind::Routed,
            })
        });
        let fencing = channels.clone();
        let fence = thread::spawn(move || fencing.fence_routing());
        let racing = producer.join().assured(JOINS);
        fence.join().assured(JOINS);
        assert!(retained.cancellation().is_cancelled());
        assert!(racing.cancellation().is_cancelled());
        let during = channels.bind(&None, Unit::Single).ingress();
        assert!(during.cancellation().is_cancelled());
        channels.replace_destinations();
        let successor = channels.bind(&None, Unit::Single).ingress();
        assert!(!successor.cancellation().is_cancelled());
        assert!(retained.cancellation().is_cancelled());
        assert!(racing.cancellation().is_cancelled());
        assert!(during.cancellation().is_cancelled());
    });
}

#[test]
fn shuttle_first_binding_has_one_winner_and_routing_retirement_cancels_every_slot() {
    check_interleavings(|| {
        let channels = Arc::new(RelayChannels::default());
        let first = channels.clone();
        let left = thread::spawn(move || {
            let branch = first.bind(&None, Unit::Single);
            let slot = branch.ingress();
            (branch, slot)
        });
        let second = channels.clone();
        let right = thread::spawn(move || {
            let branch = second.bind(&None, Unit::Single);
            let slot = branch.ingress();
            (branch, slot)
        });
        let (a, first_slot) = left.join().assured(JOINS);
        let (b, second_slot) = right.join().assured(JOINS);
        assert!(StdArc::ptr_eq(&a, &b));
        assert!(StdArc::ptr_eq(&first_slot, &second_slot));
        let writer_channels = channels.clone();
        let writer = thread::spawn(move || writer_channels.replace_owner_of(&[Unit::Single]));
        let reader = thread::spawn(move || b.ingress());
        writer.join().assured(JOINS);
        let selected = reader.join().assured(JOINS);
        assert!(first_slot.cancellation().is_cancelled());
        channels.retire(&None);
        assert!(selected.cancellation().is_cancelled());
        assert!(a.is_retired());
        assert!(a.ingress().cancellation().is_cancelled());
        assert!(!channels.bind(&None, Unit::Single).is_retired());
    });
}

#[test]
fn shuttle_branch_retirement_racing_a_route_refresh_never_revives_a_retained_producer() {
    check_interleavings(|| {
        let channels = Arc::new(RelayChannels::default());
        let branch = channels.bind(&None, Unit::Single);
        channels.replace_owner_of(&[Unit::Single]);
        let reader_branch = branch.clone();
        let reader = thread::spawn(move || reader_branch.ingress());
        let writer_channels = channels.clone();
        let writer = thread::spawn(move || writer_channels.retire(&None));
        let slot = reader.join().assured(JOINS);
        writer.join().assured(JOINS);
        assert!(branch.is_retired());
        assert!(slot.cancellation().is_cancelled());
        assert!(branch.ingress().cancellation().is_cancelled());
        let replacement = channels.bind(&None, Unit::Single);
        assert!(!replacement.is_retired());
        channels.retire_all();
        assert!(replacement.is_retired());
    });
}

#[test]
fn shuttle_first_delivery_binding_racing_terminal_retirement_cannot_publish_a_live_channel() {
    check_interleavings(|| {
        let channels = Arc::new(RelayChannels::default());
        let producer_channels = channels.clone();
        let producer = thread::spawn(move || {
            let branch = producer_channels.bind(&None, Unit::Single);
            let outbound = branch.outbound(RelayOutboundChannel {
                node_id: ClusterNodeName::parse("node-2").assured("valid peer"),
                relay: RelayName::parse("events").assured("valid relay"),
                kind: RelayPayloadKind::Routed,
            });
            (branch, outbound)
        });
        let ending_channels = channels.clone();
        let ending = thread::spawn(move || ending_channels.retire_all());
        let (branch, slot) = producer.join().assured(JOINS);
        ending.join().assured(JOINS);
        assert!(branch.is_retired());
        assert!(slot.cancellation().is_cancelled());
        assert!(channels.branches.load().is_empty());
    });
}

/// One key of each of the two partitions a two-partition test relay owns. The partition of a key is
/// read from its fingerprint, so the keys are found rather than assumed.
fn keys_of_two_partitions() -> (Option<BranchKey>, Option<BranchKey>) {
    let count = std::num::NonZeroU16::new(2).assured("two is not zero");
    let partitioning = RelayPartitioning::Partitioned(BranchPartitions::Direct(count));
    let mut first = None;
    let mut second = None;
    for tenant in ["acme", "beta", "eta", "theta", "nu", "xi"] {
        let key = crate::runtime::test_fixtures::string_branch_key("tenant", tenant);
        match partitioning.unit_of(&key) {
            Ok(Unit::Partition(0)) if first.is_none() => first = Some(key),
            Ok(Unit::Partition(1)) if second.is_none() => second = Some(key),
            _ => {}
        }
    }
    (
        first.assured("eta falls in partition 0 of 2"),
        second.assured("acme falls in partition 1 of 2"),
    )
}

#[test]
fn shuttle_replacing_one_partition_owner_retires_only_that_partitions_channels() {
    let (moving_key, staying_key) = keys_of_two_partitions();
    check_interleavings(move || {
        let units = [Unit::Partition(0), Unit::Partition(1)];
        let channels = Arc::new(RelayChannels::for_units(&units));
        let moving = channels.bind(&moving_key, Unit::Partition(0));
        let staying = channels.bind(&staying_key, Unit::Partition(1));
        let moving_slot = moving.ingress();
        let staying_slot = staying.ingress();
        let staying_branch = staying.clone();
        let producer = thread::spawn(move || staying_branch.ingress());
        let moving_branch = moving.clone();
        let mover = thread::spawn(move || moving_branch.ingress());
        let writer_channels = channels.clone();
        let writer = thread::spawn(move || writer_channels.replace_owner_of(&[Unit::Partition(0)]));
        let staying_racer = producer.join().assured(JOINS);
        let moving_racer = mover.join().assured(JOINS);
        writer.join().assured(JOINS);

        // The other partition never sees the move: its channels stay the ones it bound.
        assert!(!staying_slot.cancellation().is_cancelled());
        assert!(!staying_racer.cancellation().is_cancelled());
        assert!(StdArc::ptr_eq(&staying_slot, &staying_racer));
        assert!(!staying.is_retired());
        // The moved partition's preceding channel is retired, and its branch rebinds to the
        // successor generation.
        assert!(moving_slot.cancellation().is_cancelled());
        assert!(!moving.is_retired());
        let successor = moving.ingress();
        assert!(!successor.cancellation().is_cancelled());
        if !moving_racer.cancellation().is_cancelled() {
            assert!(StdArc::ptr_eq(&moving_racer, &successor));
        }
    });
}

#[test]
fn shuttle_fencing_one_partition_holds_only_its_bindings_until_successor_publication() {
    let (moving_key, staying_key) = keys_of_two_partitions();
    check_interleavings(move || {
        let units = [Unit::Partition(0), Unit::Partition(1)];
        let channels = Arc::new(RelayChannels::for_units(&units));
        let moving = channels.bind(&moving_key, Unit::Partition(0));
        let retained = moving.ingress();
        let binding_channels = channels.clone();
        let staying_key_for_binding = staying_key.clone();
        let producer = thread::spawn(move || {
            binding_channels
                .bind(&staying_key_for_binding, Unit::Partition(1))
                .ingress()
        });
        let fencing = channels.clone();
        let fence = thread::spawn(move || fencing.fence_routing_of(&[Unit::Partition(0)]));
        let staying_slot = producer.join().assured(JOINS);
        fence.join().assured(JOINS);

        assert!(retained.cancellation().is_cancelled());
        assert!(moving.ingress().cancellation().is_cancelled());
        assert!(!staying_slot.cancellation().is_cancelled());
        channels.replace_owner_of(&[Unit::Partition(0)]);
        assert!(!moving.ingress().cancellation().is_cancelled());
        assert!(
            !channels
                .bind(&staying_key, Unit::Partition(1))
                .ingress()
                .cancellation()
                .is_cancelled()
        );
        assert!(retained.cancellation().is_cancelled());
    });
}
