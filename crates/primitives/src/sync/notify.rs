//! Notification between async tasks in a Shuttle build: Tokio's semantics, with every waiter
//! registration a point Shuttle can schedule around.
//!
//! Tokio's `Notify` registers a waiter in two ways, and a lost wakeup lives in the gap before
//! either:
//!
//! - A [`Notified`] future observes every [`Notify::notify_waiters`] call made after it was created,
//!   whether or not it has been polled. Creating it is its registration for that notification.
//! - [`Notify::notify_one`] wakes only a waiter already in the wait list, and a future joins the list
//!   the first time it is polled or [enabled](Notified::enable). With no waiter in the list,
//!   `notify_one` stores one permit, which the next future to register consumes. Creating a future
//!   does not register it for this notification.
//!
//! An owner that reads some state and only then registers loses a notification published between
//! the two. Shuttle's own `Notify` keeps its waiters under a real lock, so no scheduling point
//! separates a caller's read from its registration, and no schedule can place a notification there.
//! This `Notify` takes a scheduling point immediately before each registration and each
//! notification, so the scheduler can run a notifier between any read and the registration that
//! follows it, as production can.
//!
//! A single notification chooses the waiter that registered first, and [`Notify::notify_last`] the
//! one that registered last, as in Tokio. Dropping a future is not a scheduling point, because a
//! drop can run while Shuttle ends an execution. A dropped future leaves the wait list, and a single
//! notification that chose it before it observed the notification passes on to the next registered
//! waiter, or becomes the stored permit when none is left, as in Tokio.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "notification backend implementation is a primitive mechanism rather than graph \
                  policy"
    )
)]

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Mutex, MutexGuard},
    task::{Context, Poll, Waker},
};

use meticulous::OptionExt as _;

use crate::scheduling;

/// Notifies one waiting task, or every task waiting, of an event.
#[derive(Debug, Default)]
pub struct Notify {
    // A real lock: no scheduling point is taken while it is held, so it is never contended, and
    // the points this type takes are the explicit ones before each registration and notification.
    state: Mutex<NotifyState>,
}

#[derive(Debug, Default)]
struct NotifyState {
    /// A single notification that found no registered waiter, kept for the next future that
    /// registers.
    permit: bool,
    /// How many times `notify_waiters` has run. A future created before a call observes it.
    notify_waiters_calls: u64,
    /// The identity the next registered waiter takes. Identities only grow, so the maps below are
    /// ordered by registration.
    next_waiter: u64,
    /// Registered waiters no notification has reached yet, with the waker of their latest poll.
    waiting: BTreeMap<u64, Option<Waker>>,
    /// Waiters a single notification chose that have not observed it yet, with how it chose them.
    chosen: BTreeMap<u64, Choice>,
}

/// Which registered waiter a single notification chooses. It stays with the choice, so a
/// notification a dropped waiter never observed passes on the same way.
#[derive(Clone, Copy, Debug)]
enum Choice {
    Earliest,
    Latest,
}

impl Notify {
    pub fn new() -> Self {
        Self::default()
    }

    /// A future that completes once this is notified.
    ///
    /// Creating it registers it for [`Self::notify_waiters`]; it registers for [`Self::notify_one`]
    /// when it is first polled or enabled.
    pub fn notified(&self) -> Notified<'_> {
        scheduling::point();
        let notify_waiters_calls = self.lock().notify_waiters_calls;
        Notified {
            notify: self,
            notify_waiters_calls,
            registration: Registration::Created,
        }
    }

    /// Wake the waiter that registered first, or store a permit for the next one to register.
    pub fn notify_one(&self) {
        self.notify_single(Choice::Earliest);
    }

    /// Wake the waiter that registered last, or store a permit for the next one to register.
    pub fn notify_last(&self) {
        self.notify_single(Choice::Latest);
    }

    /// Wake every registered waiter and every future created before this call. Stores no permit.
    pub fn notify_waiters(&self) {
        scheduling::point();
        let waiting = self.release_waiters();
        wake(waiting);
    }

    /// [`Self::notify_waiters`] without its scheduling point, for a drop, which must not take one.
    /// A drop while a panic unwinds, including Shuttle ending an execution, releases the waiters but
    /// wakes nobody: the execution is over, and a wake would reenter a scheduler that no longer
    /// runs.
    pub(crate) fn notify_waiters_from_drop(&self) {
        let waiting = self.release_waiters();
        if std::thread::panicking() {
            return;
        }
        wake(waiting);
    }

    /// Count one more `notify_waiters` and take every registered waiter out of the wait list.
    fn release_waiters(&self) -> BTreeMap<u64, Option<Waker>> {
        let mut state = self.lock();
        state.notify_waiters_calls = state
            .notify_waiters_calls
            .checked_add(1)
            .assured("a model makes far fewer than u64::MAX notifications");
        std::mem::take(&mut state.waiting)
    }

    fn notify_single(&self, choice: Choice) {
        scheduling::point();
        let waker = self.lock().choose(choice);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn lock(&self) -> MutexGuard<'_, NotifyState> {
        match self.state.lock() {
            Ok(state) => state,
            // Nothing that can panic runs while the lock is held, so the state stays whole.
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// Wake every waiter that has polled; one only enabled wakes when it is next polled.
fn wake(waiting: BTreeMap<u64, Option<Waker>>) {
    for waker in waiting.into_values().flatten() {
        waker.wake();
    }
}

impl NotifyState {
    /// Give one notification to the waiter `choice` names, or keep it as the permit when no waiter
    /// is registered. Returns the waker to wake once the lock is released.
    fn choose(&mut self, choice: Choice) -> Option<Waker> {
        let chosen = match choice {
            Choice::Earliest => self.waiting.pop_first(),
            Choice::Latest => self.waiting.pop_last(),
        };
        let Some((waiter, waker)) = chosen else {
            self.permit = true;
            return None;
        };
        self.chosen.insert(waiter, choice);
        waker
    }

    fn register(&mut self, waker: Option<Waker>) -> u64 {
        let waiter = self.next_waiter;
        self.next_waiter = waiter
            .checked_add(1)
            .assured("a model registers far fewer than u64::MAX waiters");
        self.waiting.insert(waiter, waker);
        waiter
    }
}

/// The future [`Notify::notified`] returns.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Notified<'a> {
    notify: &'a Notify,
    /// The `notify_waiters` calls made before this future was created.
    notify_waiters_calls: u64,
    registration: Registration,
}

#[derive(Clone, Copy, Debug)]
enum Registration {
    /// Observes `notify_waiters`, but no single notification can choose it yet.
    Created,
    /// In the wait list, where a single notification can choose it.
    Waiting(u64),
    Done,
}

impl Notified<'_> {
    /// Register this future for [`Notify::notify_one`] without polling it. Returns whether it is
    /// already notified, in which case polling it completes at once.
    pub fn enable(self: Pin<&mut Self>) -> bool {
        self.get_mut().register(None).is_ready()
    }

    fn register(&mut self, waker: Option<&Waker>) -> Poll<()> {
        match self.registration {
            Registration::Done => Poll::Ready(()),
            Registration::Created => {
                scheduling::point();
                let mut state = self.notify.lock();
                if state.permit {
                    state.permit = false;
                    self.registration = Registration::Done;
                    return Poll::Ready(());
                }
                if state.notify_waiters_calls != self.notify_waiters_calls {
                    self.registration = Registration::Done;
                    return Poll::Ready(());
                }
                let waiter = state.register(waker.cloned());
                self.registration = Registration::Waiting(waiter);
                Poll::Pending
            }
            Registration::Waiting(waiter) => {
                let mut state = self.notify.lock();
                let chosen = state.chosen.remove(&waiter).is_some();
                if chosen || state.notify_waiters_calls != self.notify_waiters_calls {
                    state.waiting.remove(&waiter);
                    self.registration = Registration::Done;
                    return Poll::Ready(());
                }
                if let Some(waker) = waker
                    && let Some(registered) = state.waiting.get_mut(&waiter)
                {
                    *registered = Some(waker.clone());
                }
                Poll::Pending
            }
        }
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        self.get_mut().register(Some(context.waker()))
    }
}

impl Drop for Notified<'_> {
    fn drop(&mut self) {
        let Registration::Waiting(waiter) = self.registration else {
            return;
        };
        let waker = {
            let mut state = self.notify.lock();
            state.waiting.remove(&waiter);
            let unobserved = state.chosen.remove(&waiter);
            match unobserved {
                Some(choice) => state.choose(choice),
                None => None,
            }
        };
        // A drop while a panic unwinds, including Shuttle ending an execution, wakes nobody: the
        // execution is over, and waking a task would reenter a scheduler that no longer runs.
        if let Some(waker) = waker
            && !std::thread::panicking()
        {
            waker.wake();
        }
    }
}
