//! Exact reservations against a live byte limit.
//!
//! Layer: primitives.
//! - **Owns.** The atomic decision over the limit, charged bytes, and queued requests.
//! - **Depends on.** The selected atomics and the reusable publication slots.
//! - **Must not know.** The log or the item whose weight is charged.

mod slots;

#[cfg(test)]
#[cfg(feature = "loom")]
#[path = "budget/loom_models.rs"]
mod loom_models;

use std::num::NonZeroU64;

use meticulous::OptionExt as _;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use self::slots::{Key, Slots};

// A live queue with ordinary limits and waiter counts publishes its complete decision in
// one word. The other representation retains full u64 weights, all waiter counts, and the
// ending states. Two 27-bit sizes cover limits up to just under 128 MiB while leaving nine
// bits for up to 511 parked producers. Larger values use the same admission rules.
const DIRECT: u64 = 1 << (u64::BITS - 1);
const SIZE_BITS: u32 = 27;
const SIZE_MASK: u64 = (1 << SIZE_BITS) - 1;
const WAIT_SHIFT: u32 = SIZE_BITS * 2;
const WAIT_UNIT: u64 = 1 << WAIT_SHIFT;
const WAIT_MASK: u64 = (DIRECT - 1) >> WAIT_SHIFT;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct State {
    pub(super) limit: NonZeroU64,
    pub(super) used: u64,
    pub(super) waiting: usize,
    pub(super) closed: bool,
    pub(super) readers: bool,
}

impl State {
    pub(super) fn new(limit: NonZeroU64) -> Self {
        Self {
            limit,
            used: 0,
            waiting: 0,
            closed: false,
            readers: true,
        }
    }

    fn charge(self, weight: NonZeroU64) -> Transition<Admission> {
        if self.closed {
            return Transition::Observed(Admission::Closed);
        }
        if !self.readers {
            return Transition::Observed(Admission::NoReaders);
        }
        if weight > self.limit {
            return Transition::Observed(Admission::Oversized { limit: self.limit });
        }
        let Some(room) = self.limit.get().checked_sub(self.used) else {
            // A shrink can retain more weight than its new limit.
            return Transition::Observed(Admission::Waiting);
        };
        if room < weight.get() {
            return Transition::Observed(Admission::Waiting);
        }
        let used = self
            .used
            .checked_add(weight.get())
            .verified("the weight fits in the room computed above");
        let next = Self { used, ..self };
        Transition::Changed(next, Admission::Granted)
    }

    fn without_waiter(mut self) -> Self {
        self.waiting = self
            .waiting
            .checked_sub(1)
            .assured("the queued request added its own waiting count");
        self
    }

    fn returning(mut self, weight: NonZeroU64) -> Self {
        self.used = self
            .used
            .checked_sub(weight.get())
            .assured("each release returns the full weight of an outstanding reservation");
        self
    }

    fn direct(self) -> Option<u64> {
        let limit = self.limit.get();
        let waiting = u64::try_from(self.waiting).ok()?;
        if self.closed
            || !self.readers
            || waiting > WAIT_MASK
            || limit > SIZE_MASK
            || self.used > SIZE_MASK
        {
            return None;
        }
        // Each shifted field stays within its own bits.
        DIRECT
            .checked_add(waiting << WAIT_SHIFT)?
            .checked_add(limit << SIZE_BITS)?
            .checked_add(self.used)
    }

    fn from_direct(raw: u64) -> Self {
        Self {
            limit: NonZeroU64::new((raw >> SIZE_BITS) & SIZE_MASK)
                .assured("direct decisions always encode a positive limit"),
            used: raw & SIZE_MASK,
            waiting: usize::try_from(Self::direct_waiting(raw))
                .ok()
                .assured("the packed wait count fits usize"),
            closed: false,
            readers: true,
        }
    }

    fn direct_waiting(raw: u64) -> u64 {
        (raw >> WAIT_SHIFT) & WAIT_MASK
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Admission {
    Granted,
    Waiting,
    Oversized { limit: NonZeroU64 },
    Closed,
    NoReaders,
}

enum Transition<R> {
    Observed(R),
    Changed(State, R),
}

#[derive(PartialEq, Eq)]
enum Returned {
    WithoutWaiters,
    WithWaiters,
    Deferred,
}

pub(super) struct Budget {
    current: AtomicU64,
    slots: Slots,
}

impl Budget {
    pub(super) fn new(state: State) -> Arc<Self> {
        Arc::new(Self {
            current: AtomicU64::new(state.direct().unwrap_or(Key::INITIAL.bits())),
            slots: Slots::new(state),
        })
    }

    pub(super) fn state(&self) -> State {
        self.observe().state
    }

    fn observe(&self) -> Observation {
        loop {
            let raw = self.current.load(Ordering::Acquire);
            if raw & DIRECT != 0 {
                return Observation {
                    raw,
                    state: State::from_direct(raw),
                };
            }
            if let Some(observed) = self.observe_slot(raw) {
                return observed;
            }
        }
    }

    fn observe_slot(&self, raw: u64) -> Option<Observation> {
        let key = Key::from_bits(raw);
        let state = self.slots.read(key);
        if self.current.load(Ordering::Acquire) == raw {
            // Publication of this key acquired the complete state. A recycled slot has a
            // different key, so observing the same key again rules out a concurrent rewrite
            // of the atomic fields we copied.
            return Some(Observation { raw, state });
        }
        None
    }

    pub(super) fn writer(budget: &Arc<Self>) -> Writer {
        Writer {
            budget: budget.clone(),
            spare: None,
        }
    }
}

struct Observation {
    raw: u64,
    state: State,
}

impl Observation {
    fn slot(&self) -> Option<Key> {
        if self.raw & DIRECT == 0 {
            Some(Key::from_bits(self.raw))
        } else {
            None
        }
    }
}

/// Holds one private slot for the next update. A successful publication hands the previous
/// slot to this writer, so steady-state updates reuse storage.
pub(super) struct Writer {
    budget: Arc<Budget>,
    spare: Option<Key>,
}

impl Writer {
    #[inline]
    fn update<R>(&mut self, operation: impl Fn(State) -> Transition<R>) -> R {
        loop {
            let observed = self.budget.observe();
            let (next, outcome) = match operation(observed.state) {
                Transition::Observed(outcome) => return outcome,
                Transition::Changed(next, outcome) => (next, outcome),
            };
            if self.publish(&observed, next) {
                return outcome;
            }
        }
    }

    #[inline]
    fn publish(&mut self, observed: &Observation, next: State) -> bool {
        let Some(raw) = next.direct() else {
            return self.publish_slot(observed, next);
        };
        if !self.compare(observed.raw, raw) {
            return false;
        }
        self.recycle(observed);
        true
    }

    #[cold]
    fn publish_slot(&mut self, observed: &Observation, next: State) -> bool {
        let spare = *self.spare.get_or_insert_with(|| self.budget.slots.claim());
        self.budget.slots.write(spare, next);
        if !self.compare(observed.raw, spare.bits()) {
            return false;
        }
        self.spare = None;
        self.recycle(observed);
        true
    }

    #[inline]
    fn compare(&self, observed: u64, next: u64) -> bool {
        self.budget
            .current
            .compare_exchange_weak(observed, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    #[inline]
    fn recycle(&mut self, observed: &Observation) {
        if let Some(key) = observed.slot()
            && let Some(reusable) = key.successor()
            && let Some(previous) = self.spare.replace(reusable)
        {
            self.budget.slots.return_spare(previous);
        }
    }

    #[inline]
    pub(super) fn try_reserve(&mut self, weight: NonZeroU64) -> Admission {
        let mut observed = self.budget.current.load(Ordering::Acquire);
        while observed & DIRECT != 0 {
            let limit = (observed >> SIZE_BITS) & SIZE_MASK;
            if weight.get() > limit {
                return Admission::Oversized {
                    limit: NonZeroU64::new(limit).assured("packed capacities are positive"),
                };
            }
            let used = observed & SIZE_MASK;
            if State::direct_waiting(observed) != 0 || used > limit - weight.get() {
                return Admission::Waiting;
            }
            // The full charge stays within the low field, so adding it preserves the other fields.
            let next = observed
                .checked_add(weight.get())
                .verified("the full reservation fits the packed limit");
            match self.budget.current.compare_exchange_weak(
                observed,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Admission::Granted,
                Err(current) => observed = current,
            }
        }
        self.update(|state| match state.charge(weight) {
            Transition::Changed(..) if state.waiting != 0 => {
                Transition::Observed(Admission::Waiting)
            }
            decision => decision,
        })
    }

    pub(super) fn register_waiter(&mut self) {
        self.update(|mut state| {
            state.waiting = state
                .waiting
                .checked_add(1)
                .assured("each concurrently waiting producer occupies a distinct allocation");
            Transition::Changed(state, ())
        });
    }

    /// Also reports whether the granted request took the entire byte limit by itself.
    pub(super) fn try_reserve_head(
        &mut self,
        weight: NonZeroU64,
        returned: Option<NonZeroU64>,
    ) -> (Admission, bool) {
        let mut observed = self.budget.current.load(Ordering::Acquire);
        while observed & DIRECT != 0 {
            let limit = (observed >> SIZE_BITS) & SIZE_MASK;
            let used = observed & SIZE_MASK;
            let returned_weight = returned.map_or(0, NonZeroU64::get);
            let remaining = used
                .checked_sub(returned_weight)
                .assured("the returned reservation is still charged");
            let Some(room) = limit.checked_sub(remaining) else {
                break;
            };
            if weight.get() > room {
                break;
            }
            debug_assert_ne!(State::direct_waiting(observed), 0);
            // Returning the charge and adding the new one both stay within the used field.
            // The head has one waiting count to take out of the adjacent field.
            let next = observed
                .checked_sub(returned_weight)
                .and_then(|word| word.checked_add(weight.get()))
                .and_then(|word| word.checked_sub(WAIT_UNIT))
                .verified("the head fits and owns one waiting count");
            match self.budget.current.compare_exchange_weak(
                observed,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return (Admission::Granted, weight.get() == limit),
                Err(actual) => observed = actual,
            }
        }

        self.update(|mut state| {
            if let Some(weight) = returned {
                state = state.returning(weight);
            }
            match state.charge(weight) {
                Transition::Observed(Admission::Waiting) => match returned {
                    Some(_) => Transition::Changed(state, (Admission::Waiting, false)),
                    None => Transition::Observed((Admission::Waiting, false)),
                },
                Transition::Observed(outcome) => {
                    Transition::Changed(state.without_waiter(), (outcome, false))
                }
                Transition::Changed(next, outcome) => {
                    Transition::Changed(next.without_waiter(), (outcome, weight == next.limit))
                }
            }
        })
    }

    pub(super) fn unregister_waiter(&mut self) {
        self.update(|state| Transition::Changed(state.without_waiter(), ()));
    }

    #[inline]
    pub(super) fn release(&mut self, weight: NonZeroU64) -> bool {
        self.return_charge::<false>(weight) == Returned::WithWaiters
    }

    /// Leaves the charge for the reservation granter to return when a request might take it.
    #[inline]
    pub(super) fn release_without_waiters(&mut self, weight: NonZeroU64) -> bool {
        self.return_charge::<true>(weight) == Returned::WithoutWaiters
    }

    #[inline]
    fn return_charge<const DEFER_WHILE_WAITING: bool>(&mut self, weight: NonZeroU64) -> Returned {
        let mut observed = self.budget.current.load(Ordering::Acquire);
        while observed & DIRECT != 0 {
            let waiting = State::direct_waiting(observed) != 0;
            if DEFER_WHILE_WAITING && waiting {
                return Returned::Deferred;
            }
            let used = observed & SIZE_MASK;
            assert!(
                used >= weight.get(),
                "the full reservation is still charged"
            );
            let next = observed
                .checked_sub(weight.get())
                .verified("the outstanding charge fits the used field");
            match self.budget.current.compare_exchange_weak(
                observed,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) if waiting => return Returned::WithWaiters,
                Ok(_) => return Returned::WithoutWaiters,
                Err(current) => observed = current,
            }
        }
        self.update(|state| match state.waiting {
            0 => Transition::Changed(state.returning(weight), Returned::WithoutWaiters),
            _ if DEFER_WHILE_WAITING => Transition::Observed(Returned::Deferred),
            _ => Transition::Changed(state.returning(weight), Returned::WithWaiters),
        })
    }

    pub(super) fn resize(&mut self, limit: NonZeroU64) {
        self.update(|state| Transition::Changed(State { limit, ..state }, ()));
    }

    pub(super) fn close(&mut self) {
        self.update(|state| {
            Transition::Changed(
                State {
                    closed: true,
                    ..state
                },
                (),
            )
        });
    }

    pub(super) fn readers(&mut self, readers: bool) {
        self.update(|state| Transition::Changed(State { readers, ..state }, ()));
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if let Some(spare) = self.spare {
            self.budget.slots.return_spare(spare);
        }
    }
}

#[cfg(test)]
#[cfg(not(any(feature = "loom", feature = "shuttle")))]
mod tests {
    use std::num::NonZeroU64;

    use meticulous::{OptionExt as _, ResultExt as _};

    use super::{Admission, Budget, SIZE_MASK, State, WAIT_MASK};

    #[test]
    fn the_packed_budget_round_trips_each_field_at_its_maximum() {
        let limit = NonZeroU64::new(SIZE_MASK).assured("the packed capacity is positive");
        let waiting = usize::try_from(WAIT_MASK).assured("usize holds the packed wait count");
        let state = State {
            used: SIZE_MASK,
            waiting,
            ..State::new(limit)
        };
        let packed = state.direct().assured("the field maxima fit in one word");
        assert_eq!(packed, u64::MAX);
        assert_eq!(State::from_direct(packed), state);
        for overflow in [
            State {
                used: SIZE_MASK + 1,
                ..state
            },
            State {
                limit: NonZeroU64::MAX,
                ..state
            },
            State {
                waiting: waiting + 1,
                ..state
            },
            State {
                closed: true,
                ..state
            },
            State {
                readers: false,
                ..state
            },
        ] {
            assert!(overflow.direct().is_none());
        }
    }

    #[test]
    fn combining_return_and_grant_preserves_full_width_charges_and_wait_order() {
        let limit = NonZeroU64::MAX;
        let budget = Budget::new(State::new(limit));
        let mut caller = Budget::writer(&budget);
        let mut granter = Budget::writer(&budget);
        assert_eq!(caller.try_reserve(limit), Admission::Granted);
        caller.register_waiter();
        assert!(!caller.release_without_waiters(limit));
        assert_eq!(budget.state().used, limit.get());
        assert_eq!(
            granter.try_reserve_head(limit, Some(limit)),
            (Admission::Granted, true)
        );
        assert_eq!(budget.state().used, limit.get());
        assert_eq!(budget.state().waiting, 0);
        assert!(caller.release_without_waiters(limit));
        assert_eq!(budget.state().used, 0);
    }

    #[test]
    fn a_blocked_head_applies_the_return_without_granting_the_follower() {
        let limit = NonZeroU64::MAX;
        let small = NonZeroU64::MIN;
        let budget = Budget::new(State::new(limit));
        let mut caller = Budget::writer(&budget);
        let mut granter = Budget::writer(&budget);
        assert_eq!(caller.try_reserve(limit), Admission::Granted);
        caller.register_waiter();
        assert_eq!(
            granter.try_reserve_head(limit, Some(small)),
            (Admission::Waiting, false)
        );
        assert_eq!(budget.state().used, limit.get() - small.get());
        assert_eq!(caller.try_reserve(small), Admission::Waiting);
        assert!(caller.release(small));
        assert_eq!(budget.state().used, limit.get() - 2);
    }

    #[test]
    fn an_updater_returns_its_recycled_slot_when_dropped() {
        let limit = NonZeroU64::MAX;
        let budget = Budget::new(State::new(limit));
        let first = budget
            .observe()
            .slot()
            .assured("the wide budget occupies a slot");
        let mut writer = Budget::writer(&budget);
        writer.resize(limit);
        let recycled = first
            .successor()
            .assured("the first slot has a next generation");
        assert_eq!(writer.spare.map(super::Key::bits), Some(recycled.bits()));
        drop(writer);
        assert_eq!(budget.slots.claim().bits(), recycled.bits());
    }

    #[test]
    #[should_panic(expected = "the full reservation is still charged")]
    fn the_packed_budget_cannot_release_weight_that_was_never_granted() {
        let budget = Budget::new(State::new(NonZeroU64::MIN));
        Budget::writer(&budget).release(NonZeroU64::MIN);
    }
}
