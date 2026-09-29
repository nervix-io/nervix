//! The contract of the watch channel, which every mode's backend keeps: which value a receiver has
//! seen, when a wait completes, and what dropping an endpoint does.
//!
//! The ordinary build runs these scripts against Tokio's channel and the Shuttle build against this
//! crate's, so a difference between the two fails here.

use std::pin::pin;

use meticulous::ResultExt as _;

use super::notification::{poll_once, ready};
use crate::sync::watch;

/// Subscribing marks the current value as seen: only a later send is a change to the new receiver.
pub(super) fn subscribing_marks_the_current_value_as_seen() {
    let (sender, receiver) = watch::channel(0_u8);
    sender.send_replace(1);
    let subscribed = sender.subscribe();
    assert!(!subscribed.has_changed().assured("the sender is alive"));
    assert!(receiver.has_changed().assured("the sender is alive"));
    sender.send_replace(2);
    assert!(subscribed.has_changed().assured("the sender is alive"));
    assert_eq!(*subscribed.borrow(), 2);
}

/// Borrowing does not mark the value as seen; borrowing and updating does.
pub(super) fn borrowing_and_updating_marks_the_value_as_seen() {
    let (sender, mut receiver) = watch::channel(0_u8);
    sender.send_replace(1);
    let borrowed = receiver.borrow();
    assert!(borrowed.has_changed());
    assert_eq!(*borrowed, 1);
    drop(borrowed);
    assert!(receiver.has_changed().assured("the sender is alive"));
    let updated = receiver.borrow_and_update();
    assert!(updated.has_changed());
    drop(updated);
    assert!(!receiver.has_changed().assured("the sender is alive"));
    assert!(!sender.borrow().has_changed());
}

/// A wait completes at once for an unseen value, and otherwise waits for the next send.
pub(super) fn a_wait_completes_for_an_unseen_or_a_later_value() {
    let (sender, mut receiver) = watch::channel(0_u8);
    sender.send_replace(1);
    {
        let mut unseen = pin!(receiver.changed());
        assert!(poll_once(unseen.as_mut()).is_ready());
    }
    assert!(!receiver.has_changed().assured("the sender is alive"));
    let mut later = pin!(receiver.changed());
    assert!(poll_once(later.as_mut()).is_pending());
    sender.send_replace(2);
    assert!(poll_once(later.as_mut()).is_ready());
}

/// Once every sender is dropped, a wait fails, after an unseen value was still delivered.
pub(super) fn a_wait_fails_once_the_senders_are_dropped_and_nothing_is_unseen() {
    let (sender, mut receiver) = watch::channel(0_u8);
    sender.send_replace(1);
    drop(sender);
    {
        let mut unseen = pin!(receiver.changed());
        let delivered = ready(
            poll_once(unseen.as_mut()),
            "an unseen value ends the wait at once",
        );
        assert!(delivered.is_ok());
    }
    {
        let mut closed = pin!(receiver.changed());
        let ended = ready(
            poll_once(closed.as_mut()),
            "a closed channel ends the wait at once",
        );
        assert!(ended.is_err());
    }
    assert!(receiver.has_changed().is_err());
}

/// A pending wait completes with a failure when the last sender is dropped.
pub(super) fn dropping_the_last_sender_ends_a_pending_wait() {
    let (sender, mut receiver) = watch::channel(0_u8);
    let clone = sender.clone();
    let mut waiting = pin!(receiver.changed());
    assert!(poll_once(waiting.as_mut()).is_pending());
    drop(sender);
    assert!(poll_once(waiting.as_mut()).is_pending());
    drop(clone);
    let ended = ready(
        poll_once(waiting.as_mut()),
        "the last sender's drop ends the wait",
    );
    assert!(ended.is_err());
}

/// Sending fails without receivers, returning the value; replacing succeeds, and a later
/// subscriber sees the replaced value.
pub(super) fn sending_without_receivers_fails_and_replacing_succeeds() {
    let sender = watch::Sender::new(0_u8);
    assert!(sender.is_closed());
    assert_eq!(sender.receiver_count(), 0);
    let failed = sender.send(1);
    assert!(matches!(failed, Err(watch::error::SendError(1))));
    assert_eq!(sender.send_replace(2), 0);
    let subscribed = sender.subscribe();
    assert_eq!(*subscribed.borrow(), 2);
    assert_eq!(sender.receiver_count(), 1);
    assert!(sender.send(3).is_ok());
}

/// A conditional send notifies only when it reports a change.
pub(super) fn a_conditional_send_notifies_only_a_change() {
    let (sender, receiver) = watch::channel(0_u8);
    assert!(!sender.send_if_modified(|_| false));
    assert!(!receiver.has_changed().assured("the sender is alive"));
    assert!(sender.send_if_modified(|value| {
        *value = 1;
        true
    }));
    assert!(receiver.has_changed().assured("the sender is alive"));
}

/// Waiting for a condition returns the first value that satisfies it, marking it as seen.
pub(super) fn waiting_for_a_condition_returns_the_first_value_that_meets_it() {
    let (sender, mut receiver) = watch::channel(0_u8);
    {
        let mut waiting = pin!(receiver.wait_for(|value| *value >= 2));
        assert!(poll_once(waiting.as_mut()).is_pending());
        sender.send_replace(1);
        assert!(poll_once(waiting.as_mut()).is_pending());
        sender.send_replace(2);
        let met = ready(
            poll_once(waiting.as_mut()),
            "the value 2 meets the condition",
        );
        assert_eq!(*met.assured("the sender is alive"), 2);
    }
    assert!(!receiver.has_changed().assured("the sender is alive"));
}

/// The sender's `closed` completes once every receiver is dropped.
pub(super) fn closing_completes_when_every_receiver_is_dropped() {
    let (sender, receiver) = watch::channel(0_u8);
    let clone = receiver.clone();
    let mut closed = pin!(sender.closed());
    assert!(poll_once(closed.as_mut()).is_pending());
    drop(receiver);
    assert!(poll_once(closed.as_mut()).is_pending());
    drop(clone);
    assert!(poll_once(closed.as_mut()).is_ready());
}

/// Every script of this contract, in one run.
pub(super) fn keeps_the_channel_contract() {
    subscribing_marks_the_current_value_as_seen();
    borrowing_and_updating_marks_the_value_as_seen();
    a_wait_completes_for_an_unseen_or_a_later_value();
    a_wait_fails_once_the_senders_are_dropped_and_nothing_is_unseen();
    dropping_the_last_sender_ends_a_pending_wait();
    sending_without_receivers_fails_and_replacing_succeeds();
    a_conditional_send_notifies_only_a_change();
    waiting_for_a_condition_returns_the_first_value_that_meets_it();
    closing_completes_when_every_receiver_is_dropped();
}
