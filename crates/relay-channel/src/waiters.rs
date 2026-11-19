//! Admission of producers that have to wait.
//!
//! Layer: primitives.
//! - **Owns.** Arrival order, grant handover, and cancellation of reservations.
//! - **Depends on.** The weighted budget and selected synchronization. The granter's private
//!   list uses the workspace's intrusive collections.
//! - **Must not know.** How items are linked or how readers advance.

#[cfg(test)]
#[cfg(feature = "loom")]
#[path = "waiters/loom_models.rs"]
mod loom_models;

use std::{
    cell::UnsafeCell,
    future::poll_fn,
    num::NonZeroU64,
    task::{Context, Poll},
};

use intrusive_collections::{LinkedList, LinkedListAtomicLink, intrusive_adapter};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    collections::ConcurrentQueue,
    sync::{
        Arc, AtomicWaker, StdArc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
};

use crate::budget::{Admission, Budget, State, Writer};

// The granter claims the list and its write slot only while RUNNING is set. A signal racing an
// active granter leaves WORK set so the active granter observes it before handing over.
const IDLE: u8 = 0;
const RUNNING: u8 = 1;
const WORK: u8 = 2;
const RUNNING_WITH_WORK: u8 = 0b11;

// An item using the entire byte limit serializes consecutive sends. The drain benchmark
// qualified an eight-task wake batch including the granted request. The cap keeps wake work
// independent of the length of the waiting line.
const FOLLOWER_WAKE_LIMIT: usize = 7;

struct Turns(AtomicU8);

impl Turns {
    fn new() -> Self {
        Self(AtomicU8::new(IDLE))
    }

    fn request(&self) -> bool {
        let mut observed = self.0.load(Ordering::Acquire);
        loop {
            let next = if observed == IDLE {
                RUNNING
            } else {
                RUNNING_WITH_WORK
            };
            match self
                .0
                .compare_exchange_weak(observed, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return observed == IDLE,
                Err(actual) => observed = actual,
            }
        }
    }

    fn take_work(&self) {
        self.0.fetch_and(!WORK, Ordering::AcqRel);
    }

    fn try_end(&self) -> bool {
        self.0
            .compare_exchange(RUNNING, IDLE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

#[repr(u8)]
enum Request {
    Waiting,
    Cancelled,
    Reserved,
    Oversized,
    Closed,
    NoReaders,
}

impl Request {
    fn from_bits(bits: u8) -> Self {
        match bits {
            0 => Self::Waiting,
            1 => Self::Cancelled,
            2 => Self::Reserved,
            3 => Self::Oversized,
            4 => Self::Closed,
            5 => Self::NoReaders,
            _ => unreachable!("the request atomic holds one of the declared states"),
        }
    }

    fn bits(self) -> u8 {
        // These tags are the private encoding of the request's one atomic state.
        match self {
            Self::Waiting => 0,
            Self::Cancelled => 1,
            Self::Reserved => 2,
            Self::Oversized => 3,
            Self::Closed => 4,
            Self::NoReaders => 5,
        }
    }
}

#[derive(Debug)]
struct Waiter {
    link: LinkedListAtomicLink,
    weight: NonZeroU64,
    status: AtomicU8,
    rejected_limit: AtomicU64,
    waker: AtomicWaker,
    retry_poll: AtomicBool,
}

// This external adapter uses the standard Arc through the primitive boundary. Its references
// determine lifetime only. The request status publishes each grant and refusal.
intrusive_adapter!(WaiterAdapter = StdArc<Waiter>: Waiter { link => LinkedListAtomicLink });

impl Waiter {
    fn new(weight: NonZeroU64) -> StdArc<Self> {
        StdArc::new(Self {
            link: LinkedListAtomicLink::new(),
            weight,
            status: AtomicU8::new(Request::Waiting.bits()),
            rejected_limit: AtomicU64::new(NonZeroU64::MIN.get()),
            waker: AtomicWaker::new(),
            retry_poll: AtomicBool::new(false),
        })
    }

    fn complete(&self, admission: Admission) -> bool {
        let outcome = match admission {
            Admission::Granted => Request::Reserved,
            Admission::Oversized { limit } => {
                self.rejected_limit.store(limit.get(), Ordering::Relaxed);
                Request::Oversized
            }
            Admission::Closed => Request::Closed,
            Admission::NoReaders => Request::NoReaders,
            Admission::Waiting => unreachable!("a blocked waiter has no completion"),
        };
        self.status
            .compare_exchange(
                Request::Waiting.bits(),
                outcome.bits(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

struct Granter {
    queue: LinkedList<WaiterAdapter>,
    writer: Writer,
    last_limit: NonZeroU64,
}

struct ReadyWakes {
    first: Option<StdArc<Waiter>>,
    rest: LinkedList<WaiterAdapter>,
    followers: [Option<StdArc<Waiter>>; FOLLOWER_WAKE_LIMIT],
}

impl ReadyWakes {
    fn new() -> Self {
        Self {
            first: None,
            rest: LinkedList::new(WaiterAdapter::new()),
            followers: [const { None }; FOLLOWER_WAKE_LIMIT],
        }
    }

    fn push(&mut self, waiter: StdArc<Waiter>) {
        if self.first.is_none() {
            self.first = Some(waiter);
        } else {
            self.rest.push_back(waiter);
        }
    }

    fn wake(mut self) {
        if let Some(waiter) = self.first.take() {
            waiter.waker.wake();
        }
        while let Some(waiter) = self.rest.pop_front() {
            waiter.waker.wake();
        }
        if let Some(next) = &self.followers[0] {
            // Let the next sender reschedule itself once if it polls just before its grant.
            // This hint only affects scheduling. The grant has its own status and wake.
            next.retry_poll.store(true, Ordering::Relaxed);
        }
        for waiter in self.followers.into_iter().flatten() {
            waiter.waker.wake();
        }
    }

    fn note_followers(&mut self, queue: &LinkedList<WaiterAdapter>) {
        let mut cursor = queue.front();
        for follower in &mut self.followers {
            let Some(waiter) = cursor.clone_pointer() else {
                break;
            };
            *follower = Some(waiter);
            cursor.move_next();
        }
    }
}

#[derive(Debug)]
enum Event {
    Arrived(StdArc<Waiter>),
    Cancelled(StdArc<Waiter>),
}

pub(super) struct Waiters {
    budget: Arc<Budget>,
    incoming: ConcurrentQueue<Event>,
    turns: Turns,
    granter: UnsafeCell<Granter>,
}

// SAFETY: only the actor that changes the turn state from idle to running enters the granter.
// The handover RMW acquires the previous actor's release. The shared inbox publishes additions.
unsafe impl Sync for Waiters {}

impl Waiters {
    pub(super) fn new(budget: Arc<Budget>) -> Self {
        let limit = budget.state().limit;
        let writer = Budget::writer(&budget);
        Self {
            budget,
            incoming: ConcurrentQueue::unbounded(),
            turns: Turns::new(),
            granter: UnsafeCell::new(Granter {
                queue: LinkedList::new(WaiterAdapter::new()),
                writer,
                last_limit: limit,
            }),
        }
    }

    pub(super) async fn wait(&self, writer: &mut Writer, weight: NonZeroU64) -> Admission {
        let waiter = Waiter::new(weight);
        writer.register_waiter();
        let mut pending = Pending {
            owner: self,
            waiter,
            writer,
            active: true,
        };
        self.submit(Event::Arrived(pending.waiter.clone()));
        poll_fn(|cx| pending.poll(cx)).await
    }

    fn submit(&self, event: Event) {
        self.incoming
            .push(event)
            .assured("the incoming waiter queue is open and unbounded");
        self.drive();
    }

    #[inline]
    pub(super) fn release(&self, writer: &mut Writer, weight: NonZeroU64) {
        if !writer.release_without_waiters(weight) {
            self.grant(Some((writer, weight)));
        }
    }

    pub(super) fn drive(&self) {
        self.grant(None);
    }

    fn grant(&self, returned: Option<(&mut Writer, NonZeroU64)>) {
        if !self.turns.request() {
            // Another actor already owns the waiting line. Return the charge and notify it
            // after the update, so it can observe the newly available room.
            if let Some((writer, weight)) = returned
                && writer.release(weight)
            {
                self.drive();
            }
            return;
        }

        let mut returned = returned.map(|(_, weight)| weight);
        // A release can serve the existing head directly. An arrival, cancellation, or
        // limit change requests its own turn, including when it races this release.
        let mut drive_events = returned.is_none();
        let mut ready = ReadyWakes::new();
        loop {
            // SAFETY: this actor acquired the running turn. It keeps exclusive access until
            // the compare-and-exchange returns it to idle. Wakers run after the handover.
            let granter = unsafe { &mut *self.granter.get() };
            if drive_events {
                let state = self.budget.state();
                granter.prune(state, &mut ready);
                while let Ok(event) = self.incoming.pop() {
                    match event {
                        // The request may have arrived after a concurrent resize or membership
                        // change. Its refusal must use a decision observed after that arrival.
                        Event::Arrived(waiter) => {
                            granter.enqueue(waiter, self.budget.state(), &mut ready)
                        }
                        Event::Cancelled(waiter) => granter.remove_cancelled(&waiter),
                    }
                }
            }
            granter.reserve_ready(&mut ready, returned.take());
            if self.turns.try_end() {
                break;
            }
            self.turns.take_work();
            drive_events = true;
        }
        ready.wake();
    }
}

impl Granter {
    fn refusal(state: State, waiter: &Waiter) -> Option<Admission> {
        if state.closed {
            Some(Admission::Closed)
        } else if !state.readers {
            Some(Admission::NoReaders)
        } else if waiter.weight > state.limit {
            Some(Admission::Oversized { limit: state.limit })
        } else {
            None
        }
    }

    fn enqueue(&mut self, waiter: StdArc<Waiter>, state: State, ready: &mut ReadyWakes) {
        if waiter.status.load(Ordering::Acquire) == Request::Cancelled.bits() {
            self.writer.unregister_waiter();
        } else if let Some(outcome) = Self::refusal(state, &waiter) {
            self.writer.unregister_waiter();
            Self::finish(&mut self.writer, waiter, outcome, ready);
        } else {
            self.queue.push_back(waiter);
        }
    }

    fn remove_cancelled(&mut self, waiter: &StdArc<Waiter>) {
        if waiter.link.is_linked() {
            // SAFETY: only the granter links or unlinks the pending node. This event comes
            // from a cancellation that won before completion, so the node cannot be in
            // another granter's list of wakers to notify. The Arc pointer preserves the
            // whole allocation's provenance when removal reconstructs the list's Arc.
            let mut position = unsafe { self.queue.cursor_mut_from_ptr(StdArc::as_ptr(waiter)) };
            position
                .remove()
                .verified("the cancelled waiter was linked here");
            self.writer.unregister_waiter();
        }
    }

    fn prune(&mut self, state: State, ready: &mut ReadyWakes) {
        let previous = std::mem::replace(&mut self.last_limit, state.limit);
        if state.limit.cmp(&previous).is_ge() {
            return;
        }
        let mut cursor = self.queue.front_mut();
        while let Some(waiter) = cursor.get() {
            let Some(outcome) = Self::refusal(state, waiter) else {
                cursor.move_next();
                continue;
            };
            let waiter = cursor
                .remove()
                .verified("the cursor contains the rejected request");
            self.writer.unregister_waiter();
            Self::finish(&mut self.writer, waiter, outcome, ready);
        }
    }

    fn reserve_ready(&mut self, ready: &mut ReadyWakes, mut returned: Option<NonZeroU64>) {
        loop {
            let front = self.queue.front();
            let Some(waiter) = front.get() else {
                if let Some(weight) = returned {
                    self.writer.release(weight);
                }
                return;
            };
            if waiter.status.load(Ordering::Acquire) == Request::Cancelled.bits() {
                self.writer.unregister_waiter();
                self.queue.pop_front();
                continue;
            }
            // Return the reader's charge and grant the oldest request as one budget decision.
            let returning = returned.is_some();
            let (outcome, uses_whole_budget) =
                self.writer.try_reserve_head(waiter.weight, returned.take());
            if outcome == Admission::Waiting {
                return;
            }
            let waiter = self
                .queue
                .pop_front()
                .verified("the granted request was the head");
            let completed = Self::finish(&mut self.writer, waiter, outcome, ready);
            if uses_whole_budget && completed {
                if returning {
                    ready.note_followers(&self.queue);
                }
                // A concurrent return or resize asks for another turn after updating the budget.
                return;
            }
        }
    }

    fn finish(
        writer: &mut Writer,
        waiter: StdArc<Waiter>,
        outcome: Admission,
        ready: &mut ReadyWakes,
    ) -> bool {
        if waiter.complete(outcome) {
            ready.push(waiter);
            return true;
        }
        if outcome == Admission::Granted {
            // A cancelling producer won the status race. Its reservation still belongs to
            // this granter, which returns it before taking the next request from the queue.
            writer.release(waiter.weight);
        }
        false
    }
}

struct Pending<'a> {
    owner: &'a Waiters,
    waiter: StdArc<Waiter>,
    writer: &'a mut Writer,
    active: bool,
}

impl Pending<'_> {
    fn poll(&mut self, cx: &Context<'_>) -> Poll<Admission> {
        let mut state = Request::from_bits(self.waiter.status.load(Ordering::Acquire));
        if matches!(state, Request::Waiting) {
            self.waiter.waker.register(cx.waker());
            state = Request::from_bits(self.waiter.status.load(Ordering::Acquire));
        }
        let result = match state {
            Request::Waiting => {
                if self.waiter.retry_poll.load(Ordering::Relaxed)
                    && self.waiter.retry_poll.swap(false, Ordering::Relaxed)
                {
                    cx.waker().wake_by_ref();
                }
                return Poll::Pending;
            }
            Request::Reserved => Admission::Granted,
            Request::Oversized => {
                let limit = NonZeroU64::new(self.waiter.rejected_limit.load(Ordering::Relaxed))
                    .assured("the refused request carries the positive capacity");
                Admission::Oversized { limit }
            }
            Request::Closed => Admission::Closed,
            Request::NoReaders => Admission::NoReaders,
            Request::Cancelled => unreachable!("only dropping this owner cancels this waiter"),
        };
        self.active = false;
        Poll::Ready(result)
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        drop(self.waiter.waker.take());
        let previous = self
            .waiter
            .status
            .swap(Request::Cancelled.bits(), Ordering::AcqRel);
        match Request::from_bits(previous) {
            Request::Reserved => self.owner.release(self.writer, self.waiter.weight),
            Request::Waiting => self.owner.submit(Event::Cancelled(self.waiter.clone())),
            Request::Oversized | Request::Closed | Request::NoReaders | Request::Cancelled => {}
        }
    }
}

#[cfg(test)]
#[cfg(not(any(feature = "loom", feature = "shuttle")))]
mod tests {
    use std::{
        future::Future as _,
        num::NonZeroU64,
        pin::pin,
        task::{Context, Waker},
    };

    use nervix_primitives::sync::atomic::Ordering;

    use super::{Admission, Budget, Granter, ReadyWakes, Request, State, Turns, Waiter, Waiters};

    #[test]
    fn a_signal_to_an_active_granter_prevents_an_early_handover() {
        let turns = Turns::new();
        assert!(turns.request());
        assert!(!turns.request());
        assert!(!turns.try_end());
        turns.take_work();
        assert!(turns.try_end());
        assert!(turns.request());
        assert!(turns.try_end());
    }

    #[test]
    fn cancellation_after_charging_and_before_delivery_refunds_only_a_real_grant() {
        let weight = NonZeroU64::MIN;
        let budget = Budget::new(State::new(weight));
        let mut granter = Budget::writer(&budget);
        let mut ready = ReadyWakes::new();
        let cancelled = Waiter::new(weight);
        cancelled
            .status
            .store(Request::Cancelled.bits(), Ordering::Release);
        assert_eq!(granter.try_reserve(weight), Admission::Granted);
        Granter::finish(
            &mut granter,
            cancelled.clone(),
            Admission::Granted,
            &mut ready,
        );
        assert_eq!(budget.state().used, 0);
        assert!(ready.first.is_none() && ready.rest.is_empty());
        Granter::finish(&mut granter, cancelled, Admission::Closed, &mut ready);
        assert_eq!(budget.state().used, 0);
        assert!(ready.first.is_none() && ready.rest.is_empty());
    }

    #[test]
    fn cancelling_a_follower_unlinks_it_while_the_head_is_still_waiting() {
        let capacity = NonZeroU64::MIN;
        let budget = Budget::new(State::new(capacity));
        let waiters = Waiters::new(budget.clone());
        let mut source = Budget::writer(&budget);
        assert_eq!(source.try_reserve(capacity), Admission::Granted);
        let mut first = Budget::writer(&budget);
        let mut second = Budget::writer(&budget);
        let mut cx = Context::from_waker(Waker::noop());
        let mut head = pin!(waiters.wait(&mut first, capacity));
        assert!(head.as_mut().poll(&mut cx).is_pending());
        {
            let mut follower = pin!(waiters.wait(&mut second, capacity));
            assert!(follower.as_mut().poll(&mut cx).is_pending());
            assert_eq!(budget.state().waiting, 2);
        }
        assert_eq!(budget.state().waiting, 1);
        assert!(head.as_mut().poll(&mut cx).is_pending());
        assert_eq!(budget.state().used, capacity.get());
    }
}
