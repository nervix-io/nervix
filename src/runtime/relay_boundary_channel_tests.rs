//! Native relay-channel incarnation and routing-fence regressions.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Exact channel retirement, idle rotation and routing-fence regression cases.
//! - **Depends on.** The production relay boundary and its shared test helpers.
//! - **Must not know.** Connector drivers, guest state or application transactions.

use super::*;

#[rstest::rstest]
#[case(false)]
#[case(true)]
fn forced_routing_fence_ends_changed_channels_until_publication(#[case] move_owner: bool) {
    let owner = named::<ClusterNodeName>("node-1");
    let consumers = vec![RemoteRuntimeConsumer {
        node_id: named("node-3"),
        relay: named("events"),
        mode: Some(AckMode::Attached),
        unit_modes: BTreeMap::new(),
    }];
    let services = RelayBoundaryServices::new(
        RelayBoundaryFanout::direct_with_capacity(NonZeroUsize::MIN),
        0,
        0,
        consumers.clone(),
        None,
        RelayBranchPresences::single(Arc::new(BranchPresence::new())),
        Arc::new(crate::runtime_ack::AckRootTracker::default()),
    );
    services.replace_owner_routes(RelayOwnerRoutes::single(owner.clone()));
    let retained = services.ingress_slot(&None, Unit::SINGLE);
    services.fence_routing_change(&RelayOwnerRoutes::single(owner.clone()), &consumers);
    assert!(!retained.cancellation().is_cancelled());

    let destination = if move_owner { named("node-2") } else { owner };
    let destinations = if move_owner { consumers } else { Vec::new() };
    services.fence_routing_change(
        &RelayOwnerRoutes::single(destination.clone()),
        &destinations,
    );
    assert!(retained.cancellation().is_cancelled());
    assert!(single_ingress_cancelled(&services, &None));
    let appearing = string_branch_key("tenant", "beta");
    assert!(single_ingress_cancelled(&services, &appearing));

    services.replace_owner_routes(RelayOwnerRoutes::single(destination));
    services.replace_remote_runtime_consumers(destinations);
    assert!(!single_ingress_cancelled(&services, &None));
    assert!(!single_ingress_cancelled(&services, &appearing));
    assert!(retained.cancellation().is_cancelled());
}

#[test]
fn idle_relay_channel_reopens_with_a_fresh_incarnation() {
    let slot = RelayOutboundSlot::new();
    let start = Instant::now();
    let first = slot.next_delivery_at(start);
    let active_at = start
        .checked_add(
            RELAY_CHANNEL_IDLE_ROTATION
                .checked_sub(Duration::from_nanos(1))
                .expect("the relay idle duration is longer than one nanosecond"),
        )
        .expect("the fixed relay idle duration should fit the monotonic clock");
    let second = slot.next_delivery_at(active_at);
    let reopened = slot.next_delivery_at(
        active_at
            .checked_add(RELAY_CHANNEL_IDLE_ROTATION)
            .expect("the fixed relay idle duration should fit the monotonic clock"),
    );

    assert_eq!(first.channel_incarnation, second.channel_incarnation);
    assert_eq!(first.sequence, 0);
    assert_eq!(second.sequence, 1);
    assert_ne!(first.channel_incarnation, reopened.channel_incarnation);
    assert_eq!(reopened.sequence, 0);
}

/// A receiver may forget a channel opened after this one and refuse every unseen channel at or
/// below it, so a channel that opened and carried nothing for the idle rotation opens a fresh
/// incarnation for its first batch.
#[test]
fn a_channel_left_unused_since_it_opened_reopens_before_its_first_delivery() {
    let slot = RelayOutboundSlot::new();
    let after_opening = Instant::now();
    let opened_later = uuid::Uuid::now_v7().into_bytes();

    let first = slot.next_delivery_at(
        after_opening
            .checked_add(RELAY_CHANNEL_IDLE_ROTATION)
            .expect("the fixed relay idle duration should fit the monotonic clock"),
    );

    assert!(
        first.channel_incarnation > opened_later,
        "the first delivery used the incarnation the slot opened {RELAY_CHANNEL_IDLE_ROTATION:?} \
         before it"
    );
    assert_eq!(first.sequence, 0);
}

#[test]
fn a_channel_left_unused_since_it_reopened_reopens_before_its_next_delivery() {
    let slot = RelayOutboundSlot::new();
    let unresolved = slot.next_delivery();
    slot.reopen_delivery_channel();
    let after_reopening = Instant::now();
    let reopened_later = uuid::Uuid::now_v7().into_bytes();

    let next = slot.next_delivery_at(
        after_reopening
            .checked_add(RELAY_CHANNEL_IDLE_ROTATION)
            .expect("the fixed relay idle duration should fit the monotonic clock"),
    );

    assert!(unresolved.channel_incarnation < reopened_later);
    assert!(
        next.channel_incarnation > reopened_later,
        "the next delivery used the incarnation reopened {RELAY_CHANNEL_IDLE_ROTATION:?} before it"
    );
    assert_eq!(next.sequence, 0);
}

#[test]
fn a_channel_used_within_the_idle_rotation_of_its_opening_keeps_its_incarnation() {
    let slot = RelayOutboundSlot::new();
    let opened_later = uuid::Uuid::now_v7().into_bytes();
    let after_opening = Instant::now();

    let first = slot.next_delivery_at(after_opening);

    assert!(first.channel_incarnation < opened_later);
    assert_eq!(first.sequence, 0);
}

/// A receiver refuses a channel at or below the greatest one it forgot, so every channel this
/// process opens, at a slot's creation and at each reopening, sorts after every channel it opened
/// before.
#[test]
fn every_channel_this_process_opens_sorts_after_the_channels_it_opened_before() {
    let slots = [RelayOutboundSlot::new(), RelayOutboundSlot::new()];
    let mut previous = slots[0].next_delivery().channel_incarnation;
    let mut latest = slots[1].next_delivery().channel_incarnation;
    assert!(latest > previous);
    for round in 0..1_000 {
        previous = latest;
        let slot = &slots[round % 2];
        slot.reopen_delivery_channel();
        latest = slot.next_delivery().channel_incarnation;
        assert!(
            latest > previous,
            "reopening {round} opened {latest:?} after {previous:?}"
        );
    }
}

#[test]
fn indeterminate_delivery_reopens_with_a_fresh_channel_incarnation() {
    let slot = RelayOutboundSlot::new();
    let unresolved = slot.next_delivery();

    slot.reopen_delivery_channel();

    let retry = slot.next_delivery();
    assert_ne!(unresolved.channel_incarnation, retry.channel_incarnation);
    assert_eq!(retry.sequence, 0);
}

#[test]
fn branch_eviction_cancels_and_reopens_relay_channel_generations() {
    let services = test_relay_boundary_services();
    let node = ClusterNodeName::parse("node-2").expect("valid node name");
    let relay = RelayName::parse("notifications").expect("valid relay name");
    let branch = string_branch_key("tenant", "acme");
    let ingress = services.ingress_slot(&branch, Unit::SINGLE);
    let outbound = services.outbound_slot(
        &node,
        &relay,
        RelayPayloadKind::Routed,
        &branch,
        Unit::SINGLE,
    );
    let first_delivery = outbound.next_delivery();

    services.remove_branch_slots(&branch);

    assert!(ingress.cancellation().is_cancelled());
    assert!(outbound.cancellation().is_cancelled());
    let reopened = services.outbound_slot(
        &node,
        &relay,
        RelayPayloadKind::Routed,
        &branch,
        Unit::SINGLE,
    );
    let reopened_delivery = reopened.next_delivery();
    assert!(!reopened.cancellation().is_cancelled());
    assert_ne!(
        first_delivery.channel_incarnation,
        reopened_delivery.channel_incarnation
    );
    assert_eq!(reopened_delivery.sequence, 0);
}
