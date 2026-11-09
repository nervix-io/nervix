//! Ownership of a channel position refused before any receiver attempt exists.
//!
//! Layer: data plane.
//! - **Owns.** Returning an unused current position while the delivery gate remains held.
//! - **Depends on.** The concrete outbound channel's sequence and incarnation.
//! - **Must not know.** Arrow, peer limits, placement or acknowledgement outcomes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns unused relay sequence positions")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "each delivery transfers or returns its unused position"
    )
)]

use super::{RelayDelivery, RelayOutboundSlot};

pub(in crate::runtime) struct UnusedRelayPosition<'a> {
    slot: &'a RelayOutboundSlot,
    delivery: Option<RelayDelivery>,
}

impl<'a> UnusedRelayPosition<'a> {
    /// The caller holds the slot's delivery gate and has proved that no receiver attempt exists.
    pub(in crate::runtime) fn new(slot: &'a RelayOutboundSlot, delivery: RelayDelivery) -> Self {
        Self {
            slot,
            delivery: Some(delivery),
        }
    }

    /// The admission future takes cancellation ownership before its first suspension.
    pub(in crate::runtime) fn disarm(&mut self) {
        self.delivery = None;
    }

    /// A sizing refusal, or a freshly allocated next piece, has no receiver attempt.
    pub(in crate::runtime) fn retain(&mut self, delivery: RelayDelivery) {
        self.delivery = Some(delivery);
    }
}

impl Drop for UnusedRelayPosition<'_> {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the held delivery gate serializes the current channel position",
            key = "outbound channel incarnation and sequence",
            bound = "one short sequence guard with no await"
        )
    )]
    fn drop(&mut self) {
        let Some(delivery) = self.delivery else {
            return;
        };
        let mut sequence = self.slot.sequence.lock();
        if sequence.channel_incarnation == delivery.channel_incarnation
            && sequence.next_sequence.checked_sub(1) == Some(delivery.sequence)
        {
            sequence.next_sequence = delivery.sequence;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[nervix_primitives::test]
    async fn an_abandoned_sizing_position_is_reused_but_an_admitted_prefix_is_not() {
        let slot = RelayOutboundSlot::new();
        let _gate = slot.lock_for_delivery().await.expect("live slot");
        let first = slot.next_delivery();
        let mut owner = UnusedRelayPosition::new(&slot, first);
        owner.disarm();
        let second = slot.next_delivery();
        owner.retain(second);
        drop(owner);
        assert_eq!(slot.next_delivery(), second);
        assert_eq!(slot.next_delivery().sequence, second.sequence + 1);
    }

    #[nervix_primitives::test]
    async fn a_reopened_channel_never_reuses_its_predecessors_position() {
        let slot = RelayOutboundSlot::new();
        let _gate = slot.lock_for_delivery().await.expect("live slot");
        let first = slot.next_delivery();
        let owner = UnusedRelayPosition::new(&slot, first);
        slot.reopen_delivery_channel();
        let replacement = slot.next_delivery();
        assert_ne!(replacement.channel_incarnation, first.channel_incarnation);
        drop(owner);
        let next = slot.next_delivery();
        assert_eq!(next.channel_incarnation, replacement.channel_incarnation);
        assert_eq!(next.sequence, replacement.sequence + 1);
    }
}
