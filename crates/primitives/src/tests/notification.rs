//! The registration contract of `Notify`, which every mode's backend keeps.
//!
//! Each script drives its futures by hand, polling them with a waker that does nothing, so it
//! observes exactly when a future registers and which notification reaches it. The ordinary build
//! runs the scripts against Tokio's `Notify` and the Shuttle build against this crate's, so a
//! difference between the two fails here.

use std::{
    future::Future,
    pin::{Pin, pin},
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use meticulous::OptionExt as _;

use crate::sync::{
    Notify,
    atomic::{AtomicUsize, Ordering},
};

/// Poll `future` once, with a waker that does nothing.
pub(super) fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

/// The output of a poll that `why` says completed.
pub(super) fn ready<T>(poll: Poll<T>, why: &str) -> T {
    let output = match poll {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    };
    output.assured(why)
}

/// `notify_waiters` completes every future created before it, polled or not, and no future created
/// after it.
pub(super) fn notify_waiters_reaches_every_future_created_before_it() {
    let notify = Notify::new();
    let mut created = pin!(notify.notified());
    let mut polled = pin!(notify.notified());
    assert!(poll_once(polled.as_mut()).is_pending());
    notify.notify_waiters();
    let mut created_after = pin!(notify.notified());
    assert!(poll_once(created.as_mut()).is_ready());
    assert!(poll_once(polled.as_mut()).is_ready());
    assert!(poll_once(created_after.as_mut()).is_pending());
}

/// `notify_waiters` stores no permit for a future created later.
pub(super) fn notify_waiters_stores_no_permit() {
    let notify = Notify::new();
    notify.notify_waiters();
    let mut later = pin!(notify.notified());
    assert!(poll_once(later.as_mut()).is_pending());
}

/// Creating a future does not register it for `notify_one`: only a polled or enabled future can be
/// chosen, and a future that was only created is passed over.
pub(super) fn notify_one_chooses_only_a_registered_future() {
    let notify = Notify::new();
    let mut created = pin!(notify.notified());
    let mut enabled = pin!(notify.notified());
    assert!(!enabled.as_mut().enable());
    notify.notify_one();
    assert!(poll_once(enabled.as_mut()).is_ready());
    assert!(poll_once(created.as_mut()).is_pending());
}

/// With no registered future, `notify_one` stores one permit, and the next future to register
/// takes it; a second notification stores nothing more.
pub(super) fn notify_one_without_a_waiter_stores_one_permit() {
    let notify = Notify::new();
    notify.notify_one();
    notify.notify_one();
    let mut first = pin!(notify.notified());
    let mut second = pin!(notify.notified());
    assert!(poll_once(first.as_mut()).is_ready());
    assert!(poll_once(second.as_mut()).is_pending());
}

/// `notify_one` chooses the future that registered first, and `notify_last` the one that
/// registered last.
pub(super) fn single_notifications_choose_by_registration_order() {
    let notify = Notify::new();
    let mut first = pin!(notify.notified());
    let mut second = pin!(notify.notified());
    let mut third = pin!(notify.notified());
    assert!(!first.as_mut().enable());
    assert!(!second.as_mut().enable());
    assert!(!third.as_mut().enable());
    notify.notify_one();
    assert!(poll_once(first.as_mut()).is_ready());
    assert!(poll_once(second.as_mut()).is_pending());
    notify.notify_last();
    assert!(poll_once(third.as_mut()).is_ready());
    assert!(poll_once(second.as_mut()).is_pending());
}

/// A future a single notification chose, dropped before it observed the notification, passes it on
/// to the next registered future.
pub(super) fn a_dropped_chosen_future_passes_its_notification_on() {
    let notify = Notify::new();
    let mut chosen = Box::pin(notify.notified());
    let mut next = pin!(notify.notified());
    assert!(!chosen.as_mut().enable());
    assert!(!next.as_mut().enable());
    notify.notify_one();
    drop(chosen);
    assert!(poll_once(next.as_mut()).is_ready());
}

/// With no other registered future, the notification a dropped chosen future never observed
/// becomes the stored permit.
pub(super) fn a_dropped_chosen_future_leaves_its_notification_as_the_permit() {
    let notify = Notify::new();
    let mut chosen = Box::pin(notify.notified());
    assert!(!chosen.as_mut().enable());
    notify.notify_one();
    drop(chosen);
    let mut later = pin!(notify.notified());
    assert!(poll_once(later.as_mut()).is_ready());
}

/// A registered future that is dropped leaves the wait list, so a later single notification is kept
/// as the permit rather than given to it.
pub(super) fn a_dropped_waiting_future_leaves_the_wait_list() {
    let notify = Notify::new();
    let mut cancelled = Box::pin(notify.notified());
    assert!(!cancelled.as_mut().enable());
    drop(cancelled);
    notify.notify_one();
    let mut later = pin!(notify.notified());
    assert!(poll_once(later.as_mut()).is_ready());
}

/// A future that observed its notification stays complete: enabling or polling it again completes
/// at once.
pub(super) fn an_observed_notification_stays_observed() {
    let notify = Notify::new();
    notify.notify_one();
    let mut observed = pin!(notify.notified());
    assert!(observed.as_mut().enable());
    assert!(poll_once(observed.as_mut()).is_ready());
    assert!(observed.as_mut().enable());
}

/// Counts the wakes of the waker it backs.
#[derive(Default)]
pub(super) struct WakeCount(AtomicUsize);

impl WakeCount {
    pub(super) fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// The notification a dropped chosen future passes on wakes the next registered future that waits
/// in a poll.
pub(super) fn a_passed_on_notification_wakes_the_next_waiter() {
    let notify = Notify::new();
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut waiting = Context::from_waker(&waker);
    let mut chosen = Box::pin(notify.notified());
    let mut next = pin!(notify.notified());
    assert!(poll_once(chosen.as_mut()).is_pending());
    assert!(next.as_mut().poll(&mut waiting).is_pending());
    notify.notify_one();
    assert_eq!(wakes.count(), 0);
    drop(chosen);
    assert_eq!(wakes.count(), 1);
    assert!(poll_once(next.as_mut()).is_ready());
}

/// Every script of this contract, in one run.
pub(super) fn keeps_the_registration_contract() {
    notify_waiters_reaches_every_future_created_before_it();
    notify_waiters_stores_no_permit();
    notify_one_chooses_only_a_registered_future();
    notify_one_without_a_waiter_stores_one_permit();
    single_notifications_choose_by_registration_order();
    a_dropped_chosen_future_passes_its_notification_on();
    a_dropped_chosen_future_leaves_its_notification_as_the_permit();
    a_dropped_waiting_future_leaves_the_wait_list();
    an_observed_notification_stays_observed();
    a_passed_on_notification_wakes_the_next_waiter();
}
