//! Registration and retirement races over production relay channel owners.
//!
//! Layer: test harness.
//! - **Owns.** Single-winner binding and cancellation across route and branch lifetimes.
//! - **Depends on.** RelayChannels and the primitive/model boundaries.
//! - **Must not know.** ArcSwap internals, real sockets or graph scheduling.

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
fn shuttle_first_binding_has_one_winner_and_routing_retirement_cancels_every_slot() {
    check_interleavings(|| {
        let channels = Arc::new(RelayChannels::default());
        let first = channels.clone();
        let left = thread::spawn(move || {
            let branch = first.bind(&None);
            let slot = branch.ingress();
            (branch, slot)
        });
        let second = channels.clone();
        let right = thread::spawn(move || {
            let branch = second.bind(&None);
            let slot = branch.ingress();
            (branch, slot)
        });
        let (a, first_slot) = left.join().assured(JOINS);
        let (b, second_slot) = right.join().assured(JOINS);
        assert!(StdArc::ptr_eq(&a, &b));
        assert!(StdArc::ptr_eq(&first_slot, &second_slot));
        let writer_channels = channels.clone();
        let writer = thread::spawn(move || writer_channels.replace_owner());
        let reader = thread::spawn(move || b.ingress());
        writer.join().assured(JOINS);
        let selected = reader.join().assured(JOINS);
        assert!(first_slot.cancellation().is_cancelled());
        channels.retire(&None);
        assert!(selected.cancellation().is_cancelled());
        assert!(a.is_retired());
        assert!(a.ingress().cancellation().is_cancelled());
        assert!(!channels.bind(&None).is_retired());
    });
}

#[test]
fn shuttle_branch_retirement_racing_a_route_refresh_never_revives_a_retained_producer() {
    check_interleavings(|| {
        let channels = Arc::new(RelayChannels::default());
        let branch = channels.bind(&None);
        channels.replace_owner();
        let reader_branch = branch.clone();
        let reader = thread::spawn(move || reader_branch.ingress());
        let writer_channels = channels.clone();
        let writer = thread::spawn(move || writer_channels.retire(&None));
        let slot = reader.join().assured(JOINS);
        writer.join().assured(JOINS);
        assert!(branch.is_retired());
        assert!(slot.cancellation().is_cancelled());
        assert!(branch.ingress().cancellation().is_cancelled());
        let replacement = channels.bind(&None);
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
            let branch = producer_channels.bind(&None);
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
