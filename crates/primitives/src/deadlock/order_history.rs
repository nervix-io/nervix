//! Run-bounded acquisition context, retained before an upstream order callback can be dispatched.
//!
//! Layer: primitives.
//! - **Owns.** Thread-local held guards, bounded historical witnesses and explicit context loss.
//! - **Depends on.** The diagnostic registry and primitive concurrent maps and counters.
//! - **Must not know.** Application values, engineer review, or process exit policy.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "diagnostic backend bookkeeping; consumer acquisitions are checked at their \
                  resolved calls"
    )
)]

use std::{
    cell::RefCell, collections::BTreeMap, num::NonZeroU64, panic::Location, time::SystemTime,
};

use meticulous::{OptionExt as _, ResultExt as _};

use super::{
    Access, BlockedAttempt, MAX_ORDER_EDGES, MAX_ORDER_WITNESSES, OrderEdge, OrderLock,
    OrderWitness, PotentialCycle, SourceSite, TrackedLockId, TrackedThreadId,
    detector::order_enabled, registry::Registry,
};
use crate::{
    collections::{DashMap, dash_map::Entry},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// History does not grow with workload duration once these capacities are reached: lost context
/// is delivered as an overload finding, preventing qualification.
const HISTORY_EDGE_CAPACITY: usize = 8_192;
const HELD_GUARD_CAPACITY: usize = MAX_ORDER_EDGES;

std::thread_local! { static HELD: RefCell<HeldGuards> = RefCell::new(HeldGuards::default()); }

#[derive(Default)]
struct HeldGuards {
    next: u64,
    guards: BTreeMap<NonZeroU64, HeldAcquisition>,
}

#[derive(Clone)]
struct HeldAcquisition {
    lock: OrderLock,
    attempt: BlockedAttempt,
}

/// One guard's actual lifetime, independent of other guards on the same shared lock.
pub(crate) struct HeldLease {
    token: NonZeroU64,
}

impl Drop for HeldLease {
    fn drop(&mut self) {
        match HELD.try_with(|held| held.borrow_mut().guards.remove(&self.token)) {
            Ok(_) => {}
            Err(_) => Registry::global().history.loss(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EdgeKey {
    before: TrackedLockId,
    after: TrackedLockId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ContextKey {
    thread: TrackedThreadId,
    held: BlockedAttempt,
    requested: BlockedAttempt,
    held_count: NonZeroU64,
}

#[derive(Clone)]
struct EdgeRecord {
    before: OrderLock,
    after: OrderLock,
    witnesses: BTreeMap<ContextKey, OrderWitness>,
}

struct EdgeContext {
    before: OrderLock,
    after: OrderLock,
    witnesses: Vec<OrderWitness>,
}

#[derive(Default)]
pub(crate) struct OrderHistory {
    edges: DashMap<EdgeKey, EdgeRecord>,
    slots: AtomicUsize,
    lost: AtomicU64,
}

impl OrderHistory {
    /// Only the caller's actual live guards can establish a self wait. Runtime order checking may
    /// be disabled in an instrumented build, whose upstream active graph still tracks held locks.
    pub(crate) fn holds_exclusively(&self, lock: usize) -> bool {
        if !cfg!(feature = "deloxide-order") {
            return false;
        }
        let lock = u64::try_from(lock).assured("supported targets address at most 64 bits");
        HELD.try_with(|held| {
            held.borrow().guards.values().any(|guard| {
                guard.lock.id.get().get() == lock && guard.attempt.access == Access::Exclusive
            })
        })
        .unwrap_or(false)
    }
    pub(crate) fn loss(&self) {
        let counted = self
            .lost
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(count.checked_add(1).unwrap_or(count))
            });
        counted
            .assured("the update always supplies a count; at the limit it remains a lower bound");
    }

    pub(crate) fn take_lost(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.lost.swap(0, Ordering::Relaxed))
    }

    /// Record source attempts before acquiring. This records a failed try honestly as an attempt;
    /// the historical cycle still comes exclusively from Deloxide's successful-acquisition graph.
    pub(crate) fn attempting(
        &self,
        registry: &Registry,
        lock: usize,
        access: Access,
        at: &'static Location<'static>,
    ) {
        if !order_enabled() {
            return;
        }
        let Some(thread) = registry.current_thread() else {
            self.loss();
            return;
        };
        let after = registry.order_lock(lock);
        let requested = BlockedAttempt {
            access,
            at: SourceSite::from_location(at),
        };
        let name = registry.thread_name(thread);
        let held = HELD.try_with(|held| {
            let held = held.borrow();
            let mut grouped: BTreeMap<HeldContext, HeldGroup> = BTreeMap::new();
            for acquired in held.guards.values() {
                let key = HeldContext {
                    lock: acquired.lock.id,
                    attempt: acquired.attempt.clone(),
                };
                let group = grouped.entry(key).or_insert_with(|| HeldGroup {
                    acquired: acquired.clone(),
                    count: 0,
                });
                group.count = group
                    .count
                    .checked_add(1)
                    .assured("at most HELD_GUARD_CAPACITY guards are recorded");
            }
            grouped
        });
        let Ok(held) = held else {
            self.loss();
            return;
        };
        for group in held.into_values() {
            if group.acquired.lock.id == after.id {
                continue;
            }
            let held_count =
                NonZeroU64::new(group.count).assured("each group contains a live guard");
            let key = EdgeKey {
                before: group.acquired.lock.id,
                after: after.id,
            };
            let context = ContextKey {
                thread,
                held: group.acquired.attempt,
                requested: requested.clone(),
                held_count,
            };
            let record = match self.edges.entry(key) {
                Entry::Occupied(record) => record.into_ref(),
                Entry::Vacant(record) => {
                    let reserved =
                        self.slots
                            .try_update(Ordering::Relaxed, Ordering::Relaxed, |slots| {
                                if slots < HISTORY_EDGE_CAPACITY {
                                    slots.checked_add(1)
                                } else {
                                    None
                                }
                            });
                    if reserved.is_err() {
                        self.loss();
                        continue;
                    }
                    record.insert(EdgeRecord {
                        before: group.acquired.lock,
                        after: after.clone(),
                        witnesses: BTreeMap::new(),
                    })
                }
            };
            let mut record = record;
            if let Some(witness) = record.witnesses.get_mut(&context) {
                if let Some(count) = witness.attempts.get().checked_add(1) {
                    witness.attempts = NonZeroU64::new(count)
                        .assured("incrementing a nonzero count keeps it nonzero");
                } else {
                    self.loss();
                }
            } else {
                if record.witnesses.len() == MAX_ORDER_WITNESSES {
                    self.loss();
                    continue;
                }
                let witness = OrderWitness {
                    thread,
                    name: name.clone(),
                    held: context.held.clone(),
                    requested: context.requested.clone(),
                    attempts: NonZeroU64::MIN,
                    held_count,
                };
                record.witnesses.insert(context, witness);
            }
        }
    }

    pub(crate) fn acquired(
        &self,
        registry: &Registry,
        lock: usize,
        access: Access,
        at: &'static Location<'static>,
    ) -> Option<HeldLease> {
        if !cfg!(feature = "deloxide-order") {
            return None;
        }
        let acquired = HeldAcquisition {
            lock: registry.order_lock(lock),
            attempt: BlockedAttempt {
                access,
                at: SourceSite::from_location(at),
            },
        };
        match HELD.try_with(|held| {
            let mut held = held.borrow_mut();
            if held.guards.len() == HELD_GUARD_CAPACITY {
                self.loss();
                return None;
            }
            let Some(next) = held.next.checked_add(1) else {
                self.loss();
                return None;
            };
            held.next = next;
            let token = NonZeroU64::new(next).assured("guard tokens start at one");
            held.guards.insert(token, acquired);
            Some(HeldLease { token })
        }) {
            Ok(lease) => lease,
            Err(_) => {
                self.loss();
                None
            }
        }
    }

    pub(crate) fn describe(
        &self,
        registry: &Registry,
        detected_at: SystemTime,
        locks: Vec<usize>,
        total_edges: u64,
    ) -> Option<PotentialCycle> {
        if locks.is_empty() {
            self.loss();
            return None;
        }
        let described = locks.len().min(MAX_ORDER_EDGES);
        let mut edges = Vec::with_capacity(described);
        for index in 0..described {
            let before = registry.order_lock(locks[index]);
            let after = registry.order_lock(locks[(index + 1) % locks.len()]);
            let key = EdgeKey {
                before: before.id,
                after: after.id,
            };
            let recorded = self.edges.get(&key).map(|record| record.clone());
            let context = match recorded {
                Some(record) => {
                    let mut recorded_before = record.before.clone();
                    let mut recorded_after = record.after.clone();
                    recorded_before.lifetime = registry.lifetime(recorded_before.id);
                    recorded_after.lifetime = registry.lifetime(recorded_after.id);
                    EdgeContext {
                        before: recorded_before,
                        after: recorded_after,
                        witnesses: record.witnesses.values().cloned().collect(),
                    }
                }
                None => EdgeContext {
                    before,
                    after,
                    witnesses: Vec::new(),
                },
            };
            let edge = OrderEdge::new(context.before, context.after, context.witnesses, 0)
                .assured("history bounds every witness set");
            edges.push(edge);
        }
        let described =
            u64::try_from(described).assured("supported targets address at most 64 bits");
        let omitted = total_edges
            .checked_sub(described)
            .assured("the bounded callback retains at most the reported edges");
        match PotentialCycle::new(detected_at, edges, omitted) {
            Ok(cycle) => Some(cycle),
            Err(_) => {
                self.loss();
                None
            }
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct HeldContext {
    lock: TrackedLockId,
    attempt: BlockedAttempt,
}
struct HeldGroup {
    acquired: HeldAcquisition,
    count: u64,
}
