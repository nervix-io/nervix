//! Subscription retention and overflow isolation through the client queue owner.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Default and configured retention limits and isolation between subscriptions.
//! - **Depends on.** The client test fixtures and production event queue.
//! - **Must not know.** Server scheduling, connector drivers or external transports.

use super::*;

#[test]
fn one_subscription_overflow_preserves_other_subscription_events() {
    let queue =
        EventQueue::for_subscriptions(crate::ClientCapabilities::default().subscription_limits());
    let generation = Arc::new(());
    queue.begin(&generation);
    let full = subscription("full", 1);
    let healthy = subscription("healthy", 1);
    for _ in 0..32 {
        queue.push(
            &generation,
            SubscriptionEvent::DeliveryLost(nervix_client_wire::SubscriptionDeliveryLost {
                subscription: full.clone(),
                dropped_rows: NonZeroU64::MIN,
            }),
            1,
        );
    }
    queue.push(
        &generation,
        SubscriptionEvent::DeliveryLost(nervix_client_wire::SubscriptionDeliveryLost {
            subscription: healthy.clone(),
            dropped_rows: NonZeroU64::MIN,
        }),
        1,
    );
    queue.push(
        &generation,
        SubscriptionEvent::DeliveryLost(nervix_client_wire::SubscriptionDeliveryLost {
            subscription: full.clone(),
            dropped_rows: NonZeroU64::MIN,
        }),
        1,
    );
    let Some(SubscriptionEvent::ConsumerOverflow(overflowed)) = queue.try_next() else {
        panic!("the full subscription reports terminal consumer overflow");
    };
    assert_eq!(overflowed, full);
    let Some(SubscriptionEvent::DeliveryLost(delivered)) = queue.try_next() else {
        panic!("the other subscription retains its event");
    };
    assert_eq!(delivered.subscription, healthy);
    assert!(queue.try_next().is_none());
}

#[test]
fn configured_subscription_limits_preserve_other_subscriptions_on_overflow() {
    use std::num::NonZeroUsize;
    let capabilities = crate::ClientCapabilities::from_options(&ConnectOptions {
        subscription_record_capacity: NonZeroUsize::new(2).assured("two is positive"),
        subscriptions_retained_in_full: NonZeroUsize::new(2).assured("two is positive"),
        ..ConnectOptions::default()
    })
    .assured("small retention bounds validate");
    let queue = EventQueue::for_subscriptions(capabilities.subscription_limits());
    let generation = Arc::new(());
    queue.begin(&generation);
    let full = subscription("configured-full", 1);
    let healthy = subscription("configured-healthy", 1);
    let lost = |handle: &SubscriptionHandle| {
        SubscriptionEvent::DeliveryLost(nervix_client_wire::SubscriptionDeliveryLost {
            subscription: handle.clone(),
            dropped_rows: NonZeroU64::MIN,
        })
    };
    assert!(!queue.push(&generation, lost(&full), 1));
    assert!(!queue.push(&generation, lost(&full), 1));
    assert!(!queue.push(&generation, lost(&healthy), 1));
    assert!(queue.push(&generation, lost(&full), 1));
    assert!(
        matches!(queue.try_next(), Some(SubscriptionEvent::ConsumerOverflow(handle)) if handle == full)
    );
    assert!(
        matches!(queue.try_next(), Some(SubscriptionEvent::DeliveryLost(event)) if event.subscription == healthy)
    );
    assert!(queue.try_next().is_none());
}

#[test]
fn each_subscription_retains_a_frame_of_the_frame_limit_and_overflows_alone_past_it() {
    let queue =
        EventQueue::for_subscriptions(crate::ClientCapabilities::default().subscription_limits());
    let generation = Arc::new(());
    queue.begin(&generation);
    // Rows keep the whole frame that carried them alive, so the largest event a subscription
    // receives is charged a frame of the frame limit together with the event itself.
    let largest_event = SESSION_LIMITS.frame_bytes() + std::mem::size_of::<SubscriptionEvent>();
    let first = subscription("first", 1);
    let second = subscription("second", 1);
    let lost = |handle: &SubscriptionHandle| {
        SubscriptionEvent::DeliveryLost(nervix_client_wire::SubscriptionDeliveryLost {
            subscription: handle.clone(),
            dropped_rows: NonZeroU64::MIN,
        })
    };

    assert!(
        !queue.push(&generation, lost(&first), largest_event),
        "a subscription retains a frame of the frame limit"
    );
    assert!(
        !queue.push(&generation, lost(&second), largest_event),
        "another subscription retains its own frame of the frame limit at the same time"
    );
    assert!(
        queue.push(&generation, lost(&first), 1),
        "an event past a subscription's allowance overflows it"
    );

    let Some(SubscriptionEvent::ConsumerOverflow(overflowed)) = queue.try_next() else {
        panic!("the subscription past its allowance reports terminal consumer overflow");
    };
    assert_eq!(overflowed, first);
    let Some(SubscriptionEvent::DeliveryLost(retained)) = queue.try_next() else {
        panic!("the other subscription keeps its frame of the frame limit");
    };
    assert_eq!(retained.subscription, second);
    assert!(queue.try_next().is_none());
}

#[test]
fn a_subscription_past_the_exchange_allowance_overflows_without_evicting_full_subscriptions() {
    let queue =
        EventQueue::for_subscriptions(crate::ClientCapabilities::default().subscription_limits());
    let generation = Arc::new(());
    queue.begin(&generation);
    let lost = |handle: &SubscriptionHandle| {
        SubscriptionEvent::DeliveryLost(nervix_client_wire::SubscriptionDeliveryLost {
            subscription: handle.clone(),
            dropped_rows: NonZeroU64::MIN,
        })
    };
    let mut full = Vec::new();
    for index in 0..crate::ClientCapabilities::default()
        .subscriptions_retained_in_full()
        .get()
    {
        let handle = subscription(&format!("full_{index}"), 1);
        assert!(
            !queue.push(
                &generation,
                lost(&handle),
                crate::ClientCapabilities::default()
                    .subscription_limits()
                    .bytes
            ),
            "subscription {index} retains its whole allowance"
        );
        full.push(handle);
    }
    let late = subscription("late", 1);

    assert!(
        queue.push(&generation, lost(&late), 1),
        "a subscription that finds the exchange's allowance spent overflows"
    );

    let Some(SubscriptionEvent::ConsumerOverflow(overflowed)) = queue.try_next() else {
        panic!("the late subscription reports terminal consumer overflow");
    };
    assert_eq!(overflowed, late);
    for handle in full {
        let Some(SubscriptionEvent::DeliveryLost(retained)) = queue.try_next() else {
            panic!("a full subscription keeps its events when another one overflows");
        };
        assert_eq!(retained.subscription, handle);
    }
    assert!(queue.try_next().is_none());
}
