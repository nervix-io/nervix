//! Focused channel invariants.
//!
//! Layer: test harness.
//! - **Owns.** Exact-fit, shrink, and ordered reclamation assertions.
//! - **Depends on.** The production channel owners.
//! - **Must not know.** Any Nervix runtime or model type.

mod reference;

use std::{
    future::{Future, poll_fn},
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    task::{Context, Poll, Wake, Waker},
};

use meticulous::OptionExt as _;
use nervix_primitives::sync::{
    Arc, StdArc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{
    Capture, Channel, Leave, Reader, ReaderAccess, Reserve, Restore, SavedEntry, Send, Snapshot,
    budget::{Admission, Budget, State},
    log::{Arena, Entry, Log, Read, Writer},
};

fn weight(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).assured("test weights are positive")
}

#[test]
fn strict_fit_uses_the_whole_reservation() {
    let budget = Budget::new(State::new(weight(10)));
    let mut writer = Budget::writer(&budget);
    assert_eq!(writer.try_reserve(weight(8)), Admission::Granted);
    assert_eq!(writer.try_reserve(weight(4)), Admission::Waiting);
    assert_eq!(
        writer.try_reserve(weight(11)),
        Admission::Oversized { limit: weight(10) },
    );
    assert_eq!(writer.try_reserve(weight(2)), Admission::Granted);
    assert_eq!(budget.state().used, 10);
}

#[test]
fn a_live_shrink_keeps_charges_and_refuses_new_overage() {
    let budget = Budget::new(State::new(weight(10)));
    let mut producer = Budget::writer(&budget);
    let mut reader = Budget::writer(&budget);
    assert_eq!(producer.try_reserve(weight(3)), Admission::Granted);
    assert_eq!(producer.try_reserve(weight(5)), Admission::Granted);
    reader.resize(weight(6));
    assert_eq!(budget.state().used, 8);
    assert_eq!(producer.try_reserve(weight(2)), Admission::Waiting);
    assert!(!reader.release(weight(3)));
    assert_eq!(producer.try_reserve(weight(2)), Admission::Waiting);
    assert_eq!(producer.try_reserve(weight(1)), Admission::Granted);
    assert_eq!(budget.state().used, 6);
}

fn attached<T>(access: ReaderAccess<T>) -> Reader<T> {
    let ReaderAccess::Attached(reader) = access else {
        panic!("the test reader should attach");
    };
    reader
}

async fn poll_now<F: Future + ?Sized>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: StdArc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[nervix_primitives::test]
async fn a_parked_reader_is_woken_for_publication_and_closing() {
    let mut channel = Channel::new(weight(3));
    let mut reader = attached(channel.subscribe("reader"));
    let mut sender = channel.sender();
    let wake_count = StdArc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(wake_count.clone());
    let mut context = Context::from_waker(&waker);

    let mut receiving = Box::pin(reader.read());
    assert!(receiving.as_mut().poll(&mut context).is_pending());
    assert_eq!(wake_count.0.load(Ordering::Relaxed), 0);
    assert_eq!(sender.send(1, weight(3)).await, Send::Published);
    assert_eq!(wake_count.0.load(Ordering::Relaxed), 1);
    assert_eq!(receiving.as_mut().poll(&mut context), Poll::Ready(Some(1)));
    drop(receiving);
    assert_eq!(channel.charged(), 0);

    let mut ending = Box::pin(reader.read());
    assert!(ending.as_mut().poll(&mut context).is_pending());
    channel.close();
    assert_eq!(wake_count.0.load(Ordering::Relaxed), 2);
    assert_eq!(ending.as_mut().poll(&mut context), Poll::Ready(None));
}

#[nervix_primitives::test]
async fn a_parked_reader_follows_a_membership_change_and_waits_in_the_new_log() {
    let mut channel = Channel::new(weight(3));
    let mut first = attached(channel.subscribe("first"));
    let mut sender = channel.sender();
    let wake_count = StdArc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(wake_count.clone());
    let mut context = Context::from_waker(&waker);
    let mut receiving = Box::pin(first.read());
    assert!(receiving.as_mut().poll(&mut context).is_pending());
    let mut second = attached(channel.subscribe("second"));
    assert_eq!(wake_count.0.load(Ordering::Relaxed), 1);
    assert!(receiving.as_mut().poll(&mut context).is_pending());
    assert_eq!(sender.send(1, weight(3)).await, Send::Published);
    assert_eq!(wake_count.0.load(Ordering::Relaxed), 2);
    assert_eq!(receiving.as_mut().poll(&mut context), Poll::Ready(Some(1)));
    assert_eq!(channel.charged(), 3);
    assert_eq!(second.try_read(), crate::Read::Item(1));
    assert_eq!(channel.charged(), 0);
}

#[nervix_primitives::test]
async fn a_reader_identity_has_at_most_one_attached_handle() {
    let mut channel = Channel::<u16, _>::new(weight(1));
    assert!(matches!(channel.attach(&"reader"), ReaderAccess::Missing));
    assert_eq!(channel.unsubscribe(&"reader").await, Leave::Missing);
    let reader = attached(channel.subscribe("reader"));
    assert!(matches!(
        channel.subscribe("reader"),
        ReaderAccess::Duplicate
    ));
    assert!(matches!(channel.attach(&"reader"), ReaderAccess::Busy));
    assert_eq!(channel.unsubscribe(&"reader").await, Leave::Busy);
    drop(reader);
    drop(attached(channel.attach(&"reader")));
    assert_eq!(channel.unsubscribe(&"reader").await, Leave::Left);
    channel.close();
    assert!(matches!(channel.subscribe("reader"), ReaderAccess::Closed));
}

#[nervix_primitives::test]
async fn dropping_the_channel_ends_sends_and_delivers_the_items_it_already_holds() {
    let mut channel = Channel::new(weight(3));
    let mut reader = attached(channel.subscribe("reader"));
    let mut sender = channel.sender();
    let mut waiting = channel.sender();
    assert_eq!(sender.send(1, weight(3)).await, Send::Published);
    let mut pending = Box::pin(waiting.send(2, weight(2)));
    assert!(poll_now(pending.as_mut()).await.is_pending());
    drop(channel);
    assert_eq!(
        poll_now(pending.as_mut()).await,
        Poll::Ready(Send::Closed(2))
    );
    assert_eq!(sender.send(3, weight(1)).await, Send::Closed(3));
    assert_eq!(reader.try_read(), crate::Read::Item(1));
    assert_eq!(reader.try_read(), crate::Read::Closed);
}

#[nervix_primitives::test]
async fn fifo_reserves_the_request_in_full_and_rejects_impossible_weights() {
    let mut channel = Channel::new(weight(10));
    let mut reader = attached(channel.subscribe("reader"));
    let mut producer = channel.sender();
    let mut older = channel.sender();
    let mut younger = channel.sender();
    let mut oversized = channel.sender();
    assert_eq!(producer.send(1, weight(8)).await, Send::Published);
    assert_eq!(
        oversized.send(9, weight(11)).await,
        Send::Oversized {
            item: 9,
            weight: weight(11),
            limit: weight(10)
        }
    );
    let mut first_send = Box::pin(older.send(2, weight(4)));
    assert!(poll_now(first_send.as_mut()).await.is_pending());
    let mut last_send = Box::pin(younger.send(3, weight(2)));
    assert!(poll_now(last_send.as_mut()).await.is_pending());
    assert_eq!(channel.charged(), 8);

    assert_eq!(reader.try_read(), crate::Read::Item(1));
    assert_eq!(channel.charged(), 6);
    assert_eq!(
        poll_now(first_send.as_mut()).await,
        Poll::Ready(Send::Published)
    );
    assert_eq!(
        poll_now(last_send.as_mut()).await,
        Poll::Ready(Send::Published)
    );
    assert_eq!(reader.try_read(), crate::Read::Item(2));
    assert_eq!(reader.try_read(), crate::Read::Item(3));
    assert_eq!(channel.charged(), 0);
}

#[nervix_primitives::test]
async fn a_grant_survives_shrink_but_a_new_request_uses_the_lower_limit() {
    let mut channel = Channel::new(weight(10));
    let mut reader = attached(channel.subscribe("reader"));
    let mut first_sender = channel.sender();
    let mut later_sender = channel.sender();
    let Reserve::Reserved(reservation) = first_sender.reserve(weight(8)).await else {
        panic!("the initial 8 fits inside 10");
    };
    channel.resize(weight(6));
    assert_eq!(channel.charged(), 8);
    assert_eq!(
        later_sender.send("too large", weight(7)).await,
        Send::Oversized {
            item: "too large",
            weight: weight(7),
            limit: weight(6)
        }
    );
    let mut second = Box::pin(later_sender.send("second", weight(2)));
    assert!(poll_now(second.as_mut()).await.is_pending());
    assert_eq!(reservation.publish("first"), Send::Published);
    assert!(poll_now(second.as_mut()).await.is_pending());
    assert_eq!(reader.try_read(), crate::Read::Item("first"));
    assert_eq!(
        poll_now(second.as_mut()).await,
        Poll::Ready(Send::Published)
    );
    assert_eq!(reader.try_read(), crate::Read::Item("second"));
    assert_eq!(channel.charged(), 0);
}

#[nervix_primitives::test]
async fn cancellation_returns_a_grant_before_the_next_waiter_uses_its_bytes() {
    let mut channel = Channel::new(weight(10));
    let mut reader = attached(channel.subscribe("reader"));
    let mut first_sender = channel.sender();
    let mut older = channel.sender();
    let mut younger = channel.sender();
    assert_eq!(first_sender.send(0, weight(10)).await, Send::Published);
    let mut first = Box::pin(older.send(1, weight(9)));
    assert!(poll_now(first.as_mut()).await.is_pending());
    let mut next = Box::pin(younger.send(2, weight(2)));
    assert!(poll_now(next.as_mut()).await.is_pending());
    assert_eq!(reader.try_read(), crate::Read::Item(0));
    assert_eq!(channel.charged(), 9);
    assert!(poll_now(next.as_mut()).await.is_pending());
    drop(first);
    assert_eq!(channel.charged(), 2);
    assert_eq!(poll_now(next.as_mut()).await, Poll::Ready(Send::Published));
    assert_eq!(reader.try_read(), crate::Read::Item(2));
    assert_eq!(channel.charged(), 0);
}

#[nervix_primitives::test]
async fn waking_a_follower_keeps_its_batch_pending_and_cancellable() {
    let capacity = weight(10);
    let mut channel = Channel::new(capacity);
    let mut reader = attached(channel.subscribe("reader"));
    let mut initial = channel.sender();
    let mut older = channel.sender();
    let mut younger = channel.sender();
    assert_eq!(initial.send(0, capacity).await, Send::Published);
    let mut first = Box::pin(older.send(1, capacity));
    assert!(poll_now(first.as_mut()).await.is_pending());

    let wakes = StdArc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut next = Box::pin(younger.send(2, capacity));
    assert!(next.as_mut().poll(&mut context).is_pending());

    assert_eq!(reader.try_read(), crate::Read::Item(0));
    assert_eq!(channel.charged(), capacity.get());
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    assert!(next.as_mut().poll(&mut context).is_pending());
    assert!(next.as_mut().poll(&mut context).is_pending());
    assert_eq!(wakes.0.load(Ordering::Relaxed), 2);

    drop(first);
    assert_eq!(channel.charged(), capacity.get());
    assert_eq!(wakes.0.load(Ordering::Relaxed), 3);
    drop(next);
    assert_eq!(channel.charged(), 0);
    assert_eq!(reader.try_read(), crate::Read::Empty);
}

#[nervix_primitives::test]
async fn a_later_reader_starts_at_the_end_and_leaving_returns_unread_weight() {
    let mut channel = Channel::new(weight(12));
    let mut first = attached(channel.subscribe("first"));
    let mut sender = channel.sender();
    assert_eq!(sender.send(1, weight(4)).await, Send::Published);
    let mut second = attached(channel.subscribe("second"));
    assert_eq!(second.try_read(), crate::Read::Empty);
    assert_eq!(sender.send(2, weight(5)).await, Send::Published);
    assert_eq!(first.try_read(), crate::Read::Item(1));
    assert_eq!(channel.charged(), 5);
    assert_eq!(first.try_read(), crate::Read::Item(2));
    assert_eq!(channel.charged(), 5);
    drop(second);
    assert_eq!(channel.unsubscribe(&"second").await, Leave::Left);
    assert_eq!(channel.charged(), 0);
    assert_eq!(sender.send(3, weight(6)).await, Send::Published);
    assert_eq!(first.try_read(), crate::Read::Item(3));
    assert_eq!(channel.charged(), 0);
}

#[nervix_primitives::test]
async fn standstill_round_trips_each_position_weight_and_closed_end() {
    let mut channel = Channel::new(weight(12));
    let mut first = attached(channel.subscribe("first"));
    let mut second = attached(channel.subscribe("second"));
    let mut sender = channel.sender();
    assert_eq!(sender.send("one", weight(4)).await, Send::Published);
    assert_eq!(sender.send("two", weight(5)).await, Send::Published);
    assert!(matches!(channel.capture(), Capture::Busy));
    assert_eq!(first.try_read(), crate::Read::Item("one"));
    assert_eq!(second.try_read(), crate::Read::Item("one"));
    assert_eq!(second.try_read(), crate::Read::Item("two"));
    channel.resize(weight(3));
    channel.close();
    drop(first);
    drop(second);
    drop(sender);

    let Capture::Saved(saved) = channel.capture() else {
        panic!("all handles returned to the controller");
    };
    assert_eq!(saved.capacity, weight(3));
    assert!(saved.closed);
    assert_eq!(saved.positions.get("first"), Some(&0));
    assert_eq!(saved.positions.get("second"), Some(&1));
    assert_eq!(saved.entries.len(), 1);
    assert_eq!(saved.entries[0].weight, weight(5));
    assert_eq!(saved.entries[0].remaining, NonZeroUsize::MIN);
    let Restore::Restored(restored) = Channel::restore(saved) else {
        panic!("captured state restores");
    };
    let mut first = attached(restored.attach(&"first"));
    let mut second = attached(restored.attach(&"second"));
    assert_eq!(second.try_read(), crate::Read::Closed);
    assert_eq!(first.try_read(), crate::Read::Item("two"));
    assert_eq!(first.try_read(), crate::Read::Closed);
    assert_eq!(restored.charged(), 0);
}

#[nervix_primitives::test]
async fn stopping_returns_unpublished_items_and_restoring_admits_a_new_reader_at_the_end() {
    let mut channel = Channel::new(weight(6));
    let mut reader = attached(channel.subscribe("existing"));
    let mut sender = channel.sender();
    let mut waiting = channel.sender();
    assert_eq!(sender.send(1, weight(2)).await, Send::Published);
    let Reserve::Reserved(unpublished) = sender.reserve(weight(4)).await else {
        panic!("the reservation fits exactly");
    };
    let mut pending = Box::pin(waiting.send(3, weight(1)));
    assert!(poll_now(pending.as_mut()).await.is_pending());

    channel.stop_reservations();
    assert_eq!(
        poll_now(pending.as_mut()).await,
        Poll::Ready(Send::Closed(3))
    );
    drop(pending);
    assert_eq!(unpublished.publish(2), Send::Closed(2));
    assert_eq!(channel.charged(), 2);
    assert_eq!(reader.try_read(), crate::Read::Item(1));
    assert_eq!(reader.try_read(), crate::Read::Closed);
    drop(reader);
    drop(waiting);
    drop(sender);
    let Capture::Saved(saved) = channel.capture() else {
        panic!("all handles have been returned");
    };
    assert!(!saved.closed);
    assert!(saved.entries.is_empty());
    assert_eq!(saved.positions.get("existing"), Some(&0));
    let Restore::Restored(mut restored) = Channel::restore(saved) else {
        panic!("the empty content restores");
    };
    assert!(matches!(restored.attach(&"new"), ReaderAccess::Missing));
    let mut reader = attached(restored.attach(&"existing"));
    let mut sender = restored.sender();
    assert_eq!(sender.send(4, weight(2)).await, Send::Published);
    let mut later = attached(restored.subscribe("new"));
    assert_eq!(later.try_read(), crate::Read::Empty);
    assert_eq!(reader.try_read(), crate::Read::Item(4));
    assert_eq!(sender.send(5, weight(3)).await, Send::Published);
    assert_eq!(reader.try_read(), crate::Read::Item(5));
    assert_eq!(later.try_read(), crate::Read::Item(5));
    assert_eq!(restored.charged(), 0);
}

#[nervix_primitives::test]
async fn shrinking_rejects_an_ungranted_item_and_absent_readers_return_it() {
    let mut channel = Channel::new(weight(8));
    let mut sender = channel.sender();
    assert_eq!(sender.send(0, weight(1)).await, Send::NoReaders(0));
    let mut reader = attached(channel.subscribe("reader"));
    assert_eq!(sender.send(1, weight(8)).await, Send::Published);
    let mut too_large = Box::pin(sender.send(2, weight(7)));
    assert!(poll_now(too_large.as_mut()).await.is_pending());
    channel.resize(weight(4));
    assert_eq!(
        poll_now(too_large.as_mut()).await,
        Poll::Ready(Send::Oversized {
            item: 2,
            weight: weight(7),
            limit: weight(4)
        })
    );
    assert_eq!(channel.charged(), 8);
    assert_eq!(reader.try_read(), crate::Read::Item(1));
    assert_eq!(channel.charged(), 0);
}

#[nervix_primitives::test]
async fn removing_the_last_reader_returns_an_ungranted_batch() {
    let mut channel = Channel::new(weight(1));
    let reader = attached(channel.subscribe("last"));
    let mut sender = channel.sender();
    assert_eq!(sender.send(1, weight(1)).await, Send::Published);
    let mut pending = Box::pin(sender.send(2, weight(1)));
    assert!(poll_now(pending.as_mut()).await.is_pending());
    drop(reader);
    assert_eq!(channel.unsubscribe(&"last").await, Leave::Left);
    assert_eq!(
        poll_now(pending.as_mut()).await,
        Poll::Ready(Send::NoReaders(2))
    );
    assert_eq!(channel.charged(), 0);
}

#[nervix_primitives::test]
async fn shrink_rejects_a_follower_that_can_never_fit_while_the_head_still_waits() {
    let mut channel = Channel::new(weight(10));
    let mut reader = attached(channel.subscribe("reader"));
    let mut source = channel.sender();
    let mut head = channel.sender();
    let mut follower = channel.sender();
    assert_eq!(source.send(1, weight(10)).await, Send::Published);
    let mut first = Box::pin(head.send(2, weight(1)));
    let mut second = Box::pin(follower.send(3, weight(8)));
    assert!(poll_now(first.as_mut()).await.is_pending());
    assert!(poll_now(second.as_mut()).await.is_pending());
    channel.resize(weight(5));
    assert_eq!(
        poll_now(second.as_mut()).await,
        Poll::Ready(Send::Oversized {
            item: 3,
            weight: weight(8),
            limit: weight(5)
        })
    );
    assert!(poll_now(first.as_mut()).await.is_pending());
    assert_eq!(channel.charged(), 10);
    assert_eq!(reader.try_read(), crate::Read::Item(1));
    assert_eq!(first.await, Send::Published);
    assert_eq!(reader.try_read(), crate::Read::Item(2));
    assert_eq!(channel.charged(), 0);
}

#[test]
fn restoration_refuses_impossible_positions_reader_counts_and_total_weight() {
    let snapshot = |remaining, position, count| Snapshot {
        capacity: NonZeroU64::MAX,
        closed: false,
        positions: [("reader", position)].into(),
        entries: (0..count)
            .map(|item| SavedEntry {
                item,
                weight: NonZeroU64::MAX,
                remaining,
            })
            .collect(),
    };
    assert!(matches!(
        Channel::restore(snapshot(NonZeroUsize::MIN, 2, 1)),
        Restore::InvalidPosition
    ));
    assert!(matches!(
        Channel::restore(snapshot(NonZeroUsize::MIN, 1, 1)),
        Restore::InvalidRemaining
    ));
    assert!(matches!(
        Channel::restore(snapshot(NonZeroUsize::MIN, 0, 2)),
        Restore::WeightOverflow
    ));
}

#[test]
fn a_later_small_request_leaves_room_for_the_head() {
    let budget = Budget::new(State::new(weight(10)));
    let mut first = Budget::writer(&budget);
    let mut second = Budget::writer(&budget);
    assert_eq!(first.try_reserve(weight(8)), Admission::Granted);
    first.register_waiter();
    assert_eq!(
        first.try_reserve_head(weight(4), None),
        (Admission::Waiting, false)
    );
    assert_eq!(second.try_reserve(weight(2)), Admission::Waiting);
    assert!(second.release(weight(8)));
    assert_eq!(
        first.try_reserve_head(weight(4), None),
        (Admission::Granted, false)
    );
    assert_eq!(second.try_reserve(weight(2)), Admission::Granted);
    assert_eq!(budget.state().used, 6);
}

#[test]
fn charges_at_the_full_width_limit_stay_exact() {
    let budget = Budget::new(State::new(NonZeroU64::MAX));
    let mut writer = Budget::writer(&budget);
    assert_eq!(writer.try_reserve(weight(1)), Admission::Granted);
    assert_eq!(writer.try_reserve(NonZeroU64::MAX), Admission::Waiting);
    assert!(!writer.release(weight(1)));
    assert_eq!(writer.try_reserve(NonZeroU64::MAX), Admission::Granted);
    assert_eq!(budget.state().used, u64::MAX);
    assert!(!writer.release(NonZeroU64::MAX));
    assert_eq!(budget.state().used, 0);
}

#[test]
fn a_block_is_reused_only_after_both_readers_have_passed_each_slot() {
    let arena = Arc::new(Arena::new());
    let mut writer = Writer::new(arena.clone());
    let readers = NonZeroUsize::new(2).assured("this test has two readers");
    let log = Arc::new(Log::new(arena, readers));
    let mut first = Log::start(&log).assured("the log has a first reader");
    let mut second = Log::start(&log).assured("the log has a second reader");
    assert!(Log::start(&log).is_none());

    for item in 0..160 {
        let weight = weight(4);
        log.append(&mut writer, Entry { item, weight });
        let Read::Item(first_item) = first.try_read(true) else {
            panic!("the first reader should find the appended item");
        };
        assert_eq!(first_item.item, Some(item));
        assert!(first_item.finished.is_none());
        let Read::Item(second_item) = second.try_read(true) else {
            panic!("the second reader should find the same item");
        };
        assert_eq!(second_item.item, Some(item));
        let released = second_item
            .finished
            .assured("the second reader returns the weight");
        assert_eq!(released.weight, weight);
    }
    assert!(matches!(first.try_read(true), Read::Empty));
    assert!(matches!(second.try_read(true), Read::Empty));
}
