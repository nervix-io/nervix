//! The contracts of the other native families, which every mode's backend keeps.
//!
//! A script that needs a second participant starts it with [`crate::thread`], so it is an
//! operating-system thread in ordinary execution and a modeled thread under Shuttle. The ordinary
//! build runs the scripts directly and the Shuttle build inside a Shuttle execution.

use std::{cell::Cell, collections::hash_map::RandomState, pin::pin, sync::Arc, task::Waker};

use meticulous::{OptionExt as _, ResultExt as _};

use super::notification::{WakeCount, poll_once, ready};
use crate::{
    collections::{ConcurrentQueue, DashMap, PopError, PushError, dash_map::Entry},
    publication::{ArcSwap, ArcSwapOption, Cache},
    sync::{
        AtomicWaker, CancellationToken,
        blocking::{Barrier, Condvar, Mutex, Once, OnceLock, RwLock, mpsc},
    },
    thread,
};

const PARTICIPANT_JOINS: &str =
    "a participant panics only when an assertion fails, which fails the test first";

/// A lock guards its value, and a condition variable wakes a waiter once the condition it waits for
/// holds; the waiter waits in a loop because a wakeup may come without a notification.
pub(super) fn a_condition_wakes_its_waiter_once_the_condition_holds() {
    let state = Arc::new((Mutex::new(false), Condvar::new()));
    let setter = {
        let state = Arc::clone(&state);
        thread::spawn(move || {
            let (ready, changed) = &*state;
            *ready.lock() = true;
            changed.notify_all();
        })
    };
    let (ready, changed) = &*state;
    let mut guard = ready.lock();
    while !*guard {
        changed.wait(&mut guard);
    }
    drop(guard);
    setter.join().assured(PARTICIPANT_JOINS);
}

/// A read-write lock admits readers together and a writer alone.
pub(super) fn a_read_write_lock_hands_out_its_value() {
    let lock = RwLock::new(1_u8);
    {
        let first = lock.read();
        let second = lock.read();
        assert_eq!(*first + *second, 2);
    }
    *lock.write() = 2;
    assert_eq!(*lock.read(), 2);
}

/// A cell is set once: a second value is returned to its sender.
pub(super) fn a_cell_is_set_once() {
    let cell = OnceLock::new();
    assert!(cell.get().is_none());
    assert!(cell.set(1_u8).is_ok());
    assert_eq!(cell.set(2), Err(2));
    assert_eq!(cell.get(), Some(&1));
}

/// A `Once` runs its initializer once, however often it is called.
pub(super) fn once_runs_its_initializer_once() {
    let once = Once::new();
    let runs = Cell::new(0_u8);
    once.call_once(|| runs.set(runs.get() + 1));
    once.call_once(|| runs.set(runs.get() + 1));
    assert_eq!(runs.get(), 1);
    assert!(once.is_completed());
}

/// A barrier releases its participants together, and exactly one of them leads.
pub(super) fn a_barrier_releases_its_participants_together() {
    let barrier = Arc::new(Barrier::new(2));
    let other = {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || barrier.wait().is_leader())
    };
    let this_leads = barrier.wait().is_leader();
    let other_leads = other.join().assured(PARTICIPANT_JOINS);
    assert_ne!(this_leads, other_leads);
}

/// A synchronous channel delivers in order and ends once every sender is dropped.
pub(super) fn a_synchronous_channel_delivers_in_order_and_ends_with_its_senders() {
    let (sender, receiver) = mpsc::channel();
    let producer = thread::spawn(move || {
        for value in 0..3_u8 {
            sender
                .send(value)
                .assured("the receiver outlives the producer");
        }
    });
    let received: Vec<u8> = receiver.iter().collect();
    assert_eq!(received, [0, 1, 2]);
    producer.join().assured(PARTICIPANT_JOINS);
}

/// A queue is first in, first out, and refuses new values once closed while its old ones drain.
pub(super) fn a_queue_drains_in_order_after_it_closes() {
    let queue = ConcurrentQueue::unbounded();
    assert!(queue.push(1_u8).is_ok());
    assert!(queue.push(2).is_ok());
    assert_eq!(queue.len(), 2);
    assert_eq!(queue.pop(), Ok(1));
    assert!(queue.close());
    assert!(queue.is_closed());
    assert_eq!(queue.push(3), Err(PushError::Closed(3)));
    assert_eq!(queue.pop(), Ok(2));
    assert_eq!(queue.pop(), Err(PopError::Closed));
    assert!(queue.is_empty());
}

/// A bounded queue refuses a value past its capacity and takes one again once a value leaves.
pub(super) fn a_bounded_queue_refuses_a_value_past_its_capacity() {
    let queue = ConcurrentQueue::bounded(1);
    assert!(queue.push(1_u8).is_ok());
    assert_eq!(queue.push(2), Err(PushError::Full(2)));
    assert_eq!(queue.pop(), Ok(1));
    assert_eq!(queue.pop(), Err(PopError::Empty));
    assert!(queue.push(3).is_ok());
}

/// A map entry inserts once and then finds what it inserted.
pub(super) fn a_map_entry_inserts_once() {
    let map: DashMap<u8, u8> = DashMap::new();
    match map.entry(1) {
        Entry::Vacant(vacant) => {
            vacant.insert(10);
        }
        Entry::Occupied(_) => panic!("the key was never inserted"),
    }
    match map.entry(1) {
        Entry::Occupied(occupied) => assert_eq!(*occupied.get(), 10),
        Entry::Vacant(_) => panic!("the key was inserted above"),
    }
    assert_eq!(map.len(), 1);
}

/// A map built with a capacity or a hasher holds entries like any other, and iterating a borrowed
/// map visits each entry once.
pub(super) fn a_map_built_with_a_capacity_or_a_hasher_holds_its_entries() {
    let presized: DashMap<u8, u8> = DashMap::with_capacity(4);
    let hashed: DashMap<u8, u8, RandomState> = DashMap::with_hasher(RandomState::new());
    let sized: DashMap<u8, u8, RandomState> =
        DashMap::with_capacity_and_hasher(4, RandomState::new());
    presized.insert(1, 10);
    hashed.insert(2, 20);
    sized.insert(3, 30);
    sized.insert(4, 40);
    assert_eq!(presized.get(&1).map(|entry| *entry), Some(10));
    assert_eq!(hashed.get(&2).map(|entry| *entry), Some(20));
    let mut visited = Vec::new();
    for entry in &sized {
        visited.push(*entry.key());
    }
    visited.sort_unstable();
    assert_eq!(visited, [3, 4]);
}

/// A publication serves the latest value to every reader, and a cache follows it.
pub(super) fn a_publication_serves_the_latest_value() {
    let published = Arc::new(ArcSwap::from_pointee(1_u8));
    let mut cache = Cache::new(Arc::clone(&published));
    assert_eq!(**cache.load(), 1);
    published.store(Arc::new(2));
    assert_eq!(**published.load(), 2);
    assert_eq!(**cache.load(), 2);
    let previous = published.load_full();
    let swapped = published.compare_and_swap(&previous, Arc::new(3));
    assert_eq!(**swapped, 2);
    published.rcu(|current| **current + 1);
    assert_eq!(*published.load_full(), 4);

    let optional = ArcSwapOption::empty();
    assert!(optional.load_full().is_none());
    optional.store(Some(Arc::new(5_u8)));
    assert_eq!(optional.load_full().map(|value| *value), Some(5));
}

/// A publication converts from the value it starts with, an optional one starts empty by default,
/// and read-copy-update replaces the present value and returns the one it replaced.
pub(super) fn a_publication_starts_from_a_value_and_updates_in_place() {
    let converted = ArcSwap::from(Arc::new(8_u8));
    assert_eq!(**converted.load(), 8);

    let defaulted: ArcSwapOption<u8> = ArcSwapOption::default();
    assert!(defaulted.load().is_none());
    let present = ArcSwapOption::from(Some(Arc::new(9_u8)));
    let replaced = present.rcu(|current| current.as_ref().map(|value| Arc::new(**value + 1)));
    assert_eq!(replaced.map(|value| *value), Some(9));
    assert_eq!(present.load_full().map(|value| *value), Some(10));
}

/// A token equals its clones and not its children; cancelling it cancels its children, and running
/// a future under a cancelled token ends it without its output.
pub(super) fn a_cancellation_token_has_clone_identity_and_cancels_its_children() {
    let defaulted = CancellationToken::default();
    assert!(!defaulted.is_cancelled());
    let token = CancellationToken::new();
    let clone = token.clone();
    let child = token.child_token();
    assert!(token == clone);
    assert!(token != child);

    let guarded = token.child_token();
    let guard = guarded.clone().drop_guard();
    drop(guard);
    assert!(guarded.is_cancelled());

    let mut completed = pin!(token.clone().run_until_cancelled_owned(async { 7_u8 }));
    let output = ready(
        poll_once(completed.as_mut()),
        "a ready future completes at once",
    );
    assert_eq!(output, Some(7));

    token.cancel();
    assert!(clone.is_cancelled());
    assert!(child.is_cancelled());
    let mut waited = pin!(clone.cancelled_owned());
    ready(
        poll_once(waited.as_mut()),
        "the wait of a cancelled token ends at once",
    );
    let mut ended = pin!(token.run_until_cancelled_owned(std::future::pending::<u8>()));
    let output = ready(
        poll_once(ended.as_mut()),
        "a cancelled token ends the future at once",
    );
    assert!(output.is_none());
}

/// A waker registration wakes the waker registered last, once: a wake clears the registration, a
/// later registration replaces an earlier one without waking it, a take hands the waker out
/// without waking it, and a wake with nothing registered leaves nothing for a later registration.
pub(super) fn a_waker_registration_wakes_the_last_registered_waker_once() {
    let registration = AtomicWaker::new();
    registration.wake();
    let earlier = Arc::new(WakeCount::default());
    let later = Arc::new(WakeCount::default());
    registration.register(&Waker::from(Arc::clone(&earlier)));
    assert_eq!(earlier.count(), 0);
    registration.register(&Waker::from(Arc::clone(&later)));
    registration.wake();
    assert_eq!(earlier.count(), 0);
    assert_eq!(later.count(), 1);
    registration.wake();
    assert_eq!(later.count(), 1);

    registration.register(&Waker::from(Arc::clone(&earlier)));
    let taken = registration.take().assured("a waker was registered above");
    assert!(registration.take().is_none());
    assert_eq!(earlier.count(), 0);
    taken.wake();
    assert_eq!(earlier.count(), 1);
}

/// A detached thread runs its body without anyone joining it.
pub(super) fn a_detached_thread_runs_its_body() {
    let (sender, receiver) = mpsc::channel();
    thread::spawn_detached("detached-probe", move || {
        sender
            .send(7_u8)
            .assured("the script waits for the detached body");
    })
    .assured("a detached thread starts");
    let received = receiver.recv().ok().assured("the detached body sends once");
    assert_eq!(received, 7);
}

crate::thread_local! {
    static SEEN_BY_THIS_THREAD: Cell<u8> = const { Cell::new(0) };
}

/// Each thread sees its own thread-local value.
pub(super) fn each_thread_sees_its_own_thread_local() {
    SEEN_BY_THIS_THREAD.with(|seen| seen.set(1));
    let other = thread::spawn(|| SEEN_BY_THIS_THREAD.with(Cell::get));
    assert_eq!(other.join().assured(PARTICIPANT_JOINS), 0);
    assert_eq!(SEEN_BY_THIS_THREAD.with(Cell::get), 1);
}

/// Every script of these contracts, in one run.
pub(super) fn keep_their_contracts() {
    a_condition_wakes_its_waiter_once_the_condition_holds();
    a_read_write_lock_hands_out_its_value();
    a_cell_is_set_once();
    once_runs_its_initializer_once();
    a_barrier_releases_its_participants_together();
    a_synchronous_channel_delivers_in_order_and_ends_with_its_senders();
    a_queue_drains_in_order_after_it_closes();
    a_bounded_queue_refuses_a_value_past_its_capacity();
    a_map_entry_inserts_once();
    a_map_built_with_a_capacity_or_a_hasher_holds_its_entries();
    a_publication_serves_the_latest_value();
    a_publication_starts_from_a_value_and_updates_in_place();
    a_cancellation_token_has_clone_identity_and_cancels_its_children();
    a_waker_registration_wakes_the_last_registered_waker_once();
    a_detached_thread_runs_its_body();
    each_thread_sees_its_own_thread_local();
}
