//! The races the Shuttle adapters let the scheduler reach, and the semantics they keep while it
//! does.
//!
//! Each broken protocol reads some state and only then registers for the notification its
//! publisher sends after changing that state. A publication that lands between the read and the
//! registration is lost, the waiter waits forever, and Shuttle reports the deadlock. The checks
//! explore every schedule and require that some schedule fails the broken order and none fails the
//! correct one: a publication between a read and a registration is reachable, and registering
//! first is what closes it. A single notification chosen for a waiter that is cancelled before it
//! observes it must pass on, so a surviving waiter still completes in every schedule.

use std::{
    any::Any,
    panic::{self, AssertUnwindSafe},
    pin::pin,
    sync::Arc,
};

use meticulous::{OptionExt as _, ResultExt as _};
use shuttle::future::block_on;

use crate::{
    sync::{
        Notify,
        atomic::{AtomicBool, Ordering},
        watch,
    },
    thread,
};

const PUBLISHER_JOINS: &str = "the publisher only stores and notifies, which cannot panic";

/// Whether exploring every schedule of `protocol` finds one that deadlocks it.
fn some_schedule_deadlocks(protocol: fn()) -> bool {
    let explored = panic::catch_unwind(AssertUnwindSafe(|| shuttle::check_dfs(protocol, None)));
    let Err(failure) = explored else {
        return false;
    };
    let message = failure_message(failure.as_ref());
    assert!(
        message.contains("deadlock"),
        "the protocol failed for another reason than a lost wakeup: {message}"
    );
    true
}

fn failure_message(failure: &(dyn Any + Send)) -> String {
    if let Some(message) = failure.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = failure.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    None::<String>.assured("Shuttle reports a failed schedule with a text panic")
}

/// The state a waiter reads and the notification its publisher sends after changing it.
#[derive(Default)]
struct Published {
    done: AtomicBool,
    changed: Notify,
}

impl Published {
    fn start_publisher(self: &Arc<Self>) -> thread::JoinHandle<()> {
        let published = Arc::clone(self);
        thread::spawn(move || {
            published.done.store(true, Ordering::SeqCst);
            published.changed.notify_waiters();
        })
    }
}

fn read_then_register_for_every_waiter() {
    let published = Arc::new(Published::default());
    let publisher = published.start_publisher();
    block_on(async {
        let done = published.done.load(Ordering::SeqCst);
        let changed = published.changed.notified();
        if !done {
            changed.await;
        }
    });
    publisher.join().assured(PUBLISHER_JOINS);
}

fn register_then_read_for_every_waiter() {
    let published = Arc::new(Published::default());
    let publisher = published.start_publisher();
    block_on(async {
        let changed = published.changed.notified();
        let done = published.done.load(Ordering::SeqCst);
        if !done {
            changed.await;
        }
    });
    publisher.join().assured(PUBLISHER_JOINS);
}

#[test]
fn shuttle_reaches_a_notify_waiters_published_between_a_read_and_the_registration() {
    assert!(
        some_schedule_deadlocks(read_then_register_for_every_waiter),
        "a waiter that reads before it registers must lose a publication in some schedule"
    );
}

#[test]
fn shuttle_registering_before_reading_never_misses_a_notify_waiters() {
    shuttle::check_dfs(register_then_read_for_every_waiter, None);
}

/// A single notification keeps a permit when no waiter is registered, so a waiter that reads
/// before it registers still completes: the permit is what makes that order correct for
/// `notify_one`.
fn read_then_register_for_one_waiter() {
    let published = Arc::new(Published::default());
    let publisher = {
        let published = Arc::clone(&published);
        thread::spawn(move || {
            published.done.store(true, Ordering::SeqCst);
            published.changed.notify_one();
        })
    };
    block_on(async {
        let done = published.done.load(Ordering::SeqCst);
        if !done {
            published.changed.notified().await;
        }
    });
    publisher.join().assured(PUBLISHER_JOINS);
}

#[test]
fn shuttle_a_notify_one_between_a_read_and_the_registration_is_kept_as_the_permit() {
    shuttle::check_dfs(read_then_register_for_one_waiter, None);
}

/// A watched value, and whether its publisher has changed it.
struct Watched {
    done: AtomicBool,
    value: watch::Sender<u8>,
}

impl Watched {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            done: AtomicBool::new(false),
            value: watch::Sender::new(0),
        })
    }

    fn start_publisher(self: &Arc<Self>) -> thread::JoinHandle<()> {
        let watched = Arc::clone(self);
        thread::spawn(move || {
            watched.done.store(true, Ordering::SeqCst);
            watched.value.send_replace(1);
        })
    }
}

fn read_then_subscribe() {
    let watched = Watched::new();
    let publisher = watched.start_publisher();
    block_on(async {
        let done = watched.done.load(Ordering::SeqCst);
        let mut receiver = watched.value.subscribe();
        if !done {
            receiver
                .changed()
                .await
                .assured("the watched state holds its sender");
        }
    });
    publisher.join().assured(PUBLISHER_JOINS);
}

fn subscribe_then_read() {
    let watched = Watched::new();
    let publisher = watched.start_publisher();
    block_on(async {
        let mut receiver = watched.value.subscribe();
        let done = watched.done.load(Ordering::SeqCst);
        if !done {
            receiver
                .changed()
                .await
                .assured("the watched state holds its sender");
        }
    });
    publisher.join().assured(PUBLISHER_JOINS);
}

#[test]
fn shuttle_reaches_a_send_between_a_read_and_the_subscription() {
    assert!(
        some_schedule_deadlocks(read_then_subscribe),
        "a receiver that subscribes after it reads must miss a send in some schedule"
    );
}

#[test]
fn shuttle_subscribing_before_reading_never_misses_a_send() {
    shuttle::check_dfs(subscribe_then_read, None);
}

/// The last receiver is dropped while the sender waits for the channel to close. In every order the
/// wait ends: it registers before it reads the receiver count, so a drop between the two still wakes
/// it.
fn the_last_receiver_is_dropped_while_the_sender_waits_to_close() {
    let (sender, receiver) = watch::channel(0_u8);
    let dropper = thread::spawn(move || drop(receiver));
    block_on(sender.closed());
    dropper
        .join()
        .assured("the dropper only drops a receiver, which cannot panic");
}

#[test]
fn shuttle_closing_never_misses_the_last_receivers_drop() {
    shuttle::check_dfs(
        the_last_receiver_is_dropped_while_the_sender_waits_to_close,
        None,
    );
}

/// Two waiters are registered and the first is cancelled while a single notification is sent. In
/// every order the other waiter completes: the notification chooses it, or chooses the cancelled
/// waiter and passes on when that one is dropped.
fn a_cancelled_waiter_passes_its_notification_on() {
    let changed = Arc::new(Notify::new());
    block_on(async {
        let mut cancelled = Box::pin(changed.notified());
        let mut surviving = pin!(changed.notified());
        assert!(!cancelled.as_mut().enable());
        assert!(!surviving.as_mut().enable());
        let notifier = {
            let changed = Arc::clone(&changed);
            thread::spawn(move || changed.notify_one())
        };
        // Let the notification run before the cancellation as well as after it.
        thread::yield_now();
        drop(cancelled);
        surviving.await;
        notifier.join().assured(PUBLISHER_JOINS);
    });
}

#[test]
fn shuttle_a_waiter_cancelled_as_it_is_notified_passes_the_notification_on() {
    shuttle::check_dfs(a_cancelled_waiter_passes_its_notification_on, None);
}
