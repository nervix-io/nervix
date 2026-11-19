//! Reservations, byte release, and lifecycle handoff explored through the public owner.
//!
//! Layer: test harness.
//! - **Owns.** The overlapping reservation and read schedules.
//! - **Depends on.** The public channel and Shuttle harness.
//! - **Must not know.** Product models.

use std::{
    future::{Future, poll_fn},
    num::NonZeroU64,
    pin::Pin,
    task::Poll,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_primitives::{runtime::Builder, sync::oneshot, task::spawn};

use crate::{Capture, Channel, Leave, Read, Reader, ReaderAccess, Send};

fn check<F, Fut>(scenario: F)
where
    F: Fn() -> Fut + Clone + std::marker::Send + Sync + 'static,
    Fut: Future<Output = ()>,
{
    check_random_and_pct(move || {
        Builder::new_current_thread()
            .build()
            .assured("Shuttle can create a current-thread runtime")
            .block_on(scenario());
    });
}

fn count(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).assured("the modeled weight is positive")
}

fn reader<T>(access: ReaderAccess<T>) -> Reader<T> {
    let ReaderAccess::Attached(reader) = access else {
        panic!("the model adds a distinct reader");
    };
    reader
}

async fn once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

#[test]
fn shuttle_an_older_request_reserves_first_even_when_a_small_request_fits() {
    check(|| async {
        let mut channel = Channel::new(count(4));
        let mut reader = reader(channel.subscribe(0));
        let mut source = channel.sender();
        let mut head = channel.sender();
        let mut tail = channel.sender();
        assert_eq!(source.send(0, count(3)).await, Send::Published);
        let mut first = Box::pin(head.send(1, count(3)));
        assert!(once(first.as_mut()).await.is_pending());
        let mut second = Box::pin(tail.send(2, count(2)));
        assert!(once(second.as_mut()).await.is_pending());

        let reading = spawn(async move {
            assert_eq!(reader.read().await, Some(0));
            reader
        });
        assert_eq!(first.await, Send::Published);
        let mut reader = reading.await.assured("the first read completed");
        assert!(once(second.as_mut()).await.is_pending());
        assert_eq!(reader.read().await, Some(1));
        assert_eq!(second.await, Send::Published);
        assert_eq!(reader.read().await, Some(2));
        assert_eq!(channel.charged(), 0);
    });
}

#[test]
fn shuttle_cancelling_the_head_racing_release_keeps_the_next_request_live() {
    check(|| async {
        let mut channel = Channel::new(count(4));
        let mut reader = reader(channel.subscribe(0));
        let mut source = channel.sender();
        let mut following = channel.sender();
        assert_eq!(source.send(0, count(4)).await, Send::Published);
        let mut head = channel.sender();
        let mut first = Box::pin(head.send(1, count(4)));
        assert!(once(first.as_mut()).await.is_pending());
        let mut next = Box::pin(following.send(2, count(3)));
        assert!(once(next.as_mut()).await.is_pending());
        let reading = spawn(async move {
            assert_eq!(reader.read().await, Some(0));
            reader
        });
        drop(first);
        assert_eq!(next.await, Send::Published);
        let mut reader = reading.await.assured("the first read completed");
        assert_eq!(reader.read().await, Some(2));
        assert_eq!(channel.charged(), 0);
    });
}

#[test]
fn shuttle_both_readers_return_one_charge_while_the_producer_waits() {
    check(|| async {
        let mut channel = Channel::new(count(1));
        let mut first = reader(channel.subscribe(0));
        let mut second = reader(channel.subscribe(1));
        let mut sender = channel.sender();
        assert_eq!(sender.send(0, count(1)).await, Send::Published);
        let a = spawn(async move {
            assert_eq!(first.read().await, Some(0));
            first
        });
        let b = spawn(async move {
            assert_eq!(second.read().await, Some(0));
            second
        });
        assert_eq!(sender.send(1, count(1)).await, Send::Published);
        let mut first = a.await.assured("the first reader made progress");
        let mut second = b.await.assured("the second reader made progress");
        assert_eq!(channel.charged(), 1);
        assert_eq!(first.try_read(), Read::Item(1));
        assert_eq!(channel.charged(), 1);
        assert_eq!(second.try_read(), Read::Item(1));
        assert_eq!(channel.charged(), 0);
    });
}

#[test]
fn shuttle_a_departing_reader_releases_its_share_during_an_active_read() {
    check(|| async {
        let mut channel = Channel::new(count(6));
        let mut staying = reader(channel.subscribe(0));
        let leaving = reader(channel.subscribe(1));
        let mut producer = channel.sender();
        assert_eq!(producer.send(0, count(6)).await, Send::Published);
        drop(leaving);
        let reading = spawn(async move {
            assert_eq!(staying.read().await, Some(0));
            staying
        });
        assert_eq!(channel.unsubscribe(&1).await, Leave::Left);
        assert_eq!(producer.send(1, count(6)).await, Send::Published);
        let mut staying = reading.await.assured("the remaining reader advanced");
        assert_eq!(staying.read().await, Some(1));
        assert_eq!(channel.charged(), 0);
    });
}

#[test]
fn shuttle_growing_the_budget_releases_a_waiter_without_a_read() {
    check(|| async {
        let mut channel = Channel::new(count(5));
        let mut reader = reader(channel.subscribe(0));
        let mut producer = channel.sender();
        assert_eq!(producer.send(0, count(5)).await, Send::Published);
        let growing = spawn(async move {
            channel.resize(count(9));
            channel
        });
        assert_eq!(producer.send(1, count(4)).await, Send::Published);
        let channel = growing.await.assured("the live increase completed");
        assert_eq!(channel.charged(), 9);
        assert_eq!(reader.read().await, Some(0));
        assert_eq!(reader.read().await, Some(1));
        assert_eq!(channel.charged(), 0);
    });
}

#[test]
fn shuttle_standstill_acquires_a_reader_returning_its_position() {
    check(|| async {
        let mut channel = Channel::new(count(5));
        let mut reader = reader(channel.subscribe(0));
        let mut producer = channel.sender();
        assert_eq!(producer.send(1, count(2)).await, Send::Published);
        assert_eq!(producer.send(2, count(3)).await, Send::Published);
        drop(producer);
        let (release, held) = oneshot::channel();
        let active = spawn(async move {
            assert_eq!(reader.try_read(), Read::Item(1));
            held.await.assured("capture has checked the live handle");
            drop(reader);
        });
        assert!(matches!(channel.capture(), Capture::Busy));
        release
            .send(())
            .assured("the active reader still owns its position");
        // The exclusive check may finish before or after the actor returns the reader.
        // Capture acquires that return itself if it wins this race.
        let racing = channel.capture();
        active.await.assured("the reader returned its position");
        let saved = match racing {
            Capture::Saved(saved) => saved,
            Capture::Busy => {
                let Capture::Saved(saved) = channel.capture() else {
                    panic!("the finished reader returned its position");
                };
                saved
            }
        };
        assert_eq!(saved.positions.get(&0), Some(&0));
        assert_eq!(saved.entries.len(), 1);
        assert_eq!(saved.entries[0].item, 2);
        assert_eq!(saved.entries[0].weight, count(3));
    });
}
