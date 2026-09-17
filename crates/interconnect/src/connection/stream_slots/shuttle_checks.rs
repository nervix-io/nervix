//! Stream-slot leases racing their connection's drain, explored under Shuttle.
//!
//! Layer: test harness.
//!
//! - **Owns.** The reservation, lease, and drain invariants of every stream-slot partition.
//! - **Depends on.** The production stream-slot quotas and the interconnect Shuttle runner.
//! - **Must not know.** Connections, sockets, or what a leased stream carries.

// The standard library's atomics are not Shuttle scheduling points, so each record below changes in
// the same step as the lease or drain operation it records.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use meticulous::{OptionExt as _, ResultExt as _};
use shuttle::rand::{Rng as _, thread_rng};
use tokio::sync::Notify;
use triomphe::Arc;

use super::{
    BULK_RESOURCE_STREAMS, BULK_SHARED_STREAMS, BULK_SNAPSHOT_STREAMS,
    MANAGEMENT_ADMISSION_STREAMS, MANAGEMENT_CANCELLATION_STREAMS, MANAGEMENT_DISCOVERY_STREAMS,
    MANAGEMENT_LIVENESS_STREAMS, MANAGEMENT_PROGRESS_STREAMS, MANAGEMENT_SHARED_STREAMS,
    MANAGEMENT_TERMINAL_STREAMS, REPLICATION_APPEND_STREAMS, REPLICATION_SHARED_STREAMS,
    StreamSlotQuotas,
};
use crate::{PoolClass, RequestSubquota, shuttle_test::check_random_and_pct};

/// One subquota of a partition and the stream slots it reserves.
#[derive(Debug, Clone, Copy)]
struct Reservation {
    subquota: RequestSubquota,
    slots: usize,
}

impl Reservation {
    const fn new(subquota: RequestSubquota, slots: usize) -> Self {
        Self { subquota, slots }
    }
}

/// The reservations `class` is specified to partition its connection's stream slots into.
fn reservations(class: PoolClass) -> Vec<Reservation> {
    let reservations = match class {
        PoolClass::Management => vec![
            Reservation::new(RequestSubquota::Shared, MANAGEMENT_SHARED_STREAMS),
            Reservation::new(RequestSubquota::Discovery, MANAGEMENT_DISCOVERY_STREAMS),
            Reservation::new(RequestSubquota::Liveness, MANAGEMENT_LIVENESS_STREAMS),
            Reservation::new(RequestSubquota::Progress, MANAGEMENT_PROGRESS_STREAMS),
            Reservation::new(RequestSubquota::Admission, MANAGEMENT_ADMISSION_STREAMS),
            Reservation::new(
                RequestSubquota::Cancellation,
                MANAGEMENT_CANCELLATION_STREAMS,
            ),
            Reservation::new(RequestSubquota::Terminal, MANAGEMENT_TERMINAL_STREAMS),
        ],
        PoolClass::Replication => vec![
            Reservation::new(RequestSubquota::Shared, REPLICATION_SHARED_STREAMS),
            Reservation::new(RequestSubquota::Append, REPLICATION_APPEND_STREAMS),
        ],
        PoolClass::Bulk => vec![
            Reservation::new(RequestSubquota::Shared, BULK_SHARED_STREAMS),
            Reservation::new(RequestSubquota::Resource, BULK_RESOURCE_STREAMS),
            Reservation::new(RequestSubquota::Snapshot, BULK_SNAPSHOT_STREAMS),
        ],
        PoolClass::Commands | PoolClass::Relay => vec![Reservation::new(
            RequestSubquota::Shared,
            class.stream_slots_per_connection(),
        )],
    };
    let mut reserved = 0_usize;
    for reservation in &reservations {
        reserved = reserved
            .checked_add(reservation.slots)
            .assured("a connection holds at most a few dozen stream slots");
    }
    assert_eq!(
        reserved,
        class.stream_slots_per_connection(),
        "the {class:?} reservations must partition every stream slot of its connection"
    );
    reservations
}

/// The yields for which the holder of the last leased slot keeps it while the drain waits for that
/// slot alone. A drain that waits for every leased slot never completes within any number of them,
/// so a longer window only gives a drain that stopped waiting early more steps to be caught in.
const LAST_SLOT_HOLD_YIELDS: usize = 64;

/// When a lease task returns the last slot it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LastReturn {
    /// Once the drain has begun.
    AfterDrainBegan,
    /// Once the drain has begun and every other leased slot has returned, and only after holding
    /// it for `LAST_SLOT_HOLD_YIELDS` more yields, so the drain waits for this slot alone.
    AfterEveryOtherSlot,
}

/// What the lease tasks and the drain have observed of each other.
///
/// `try_lease` decides a lease in its last step, and `drain` raises its fence in its last step, so
/// a task that records either right after the call returns records it in the same Shuttle step as
/// the decision. A lease recorded after the drain was recorded as begun was therefore granted after
/// the drain raised its fence.
#[derive(Default)]
struct DrainObservation {
    began: AtomicBool,
    completed: AtomicBool,
    outstanding: AtomicUsize,
    changed: Notify,
}

impl DrainObservation {
    fn begin(&self) {
        self.began.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    fn has_begun(&self) -> bool {
        self.began.load(Ordering::SeqCst)
    }

    fn complete(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }

    fn has_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    fn lease_granted(&self, subquota: RequestSubquota) {
        assert!(
            !self.has_begun(),
            "the {subquota:?} subquota leased a slot after the drain began"
        );
        let granted =
            self.outstanding
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |outstanding| {
                    outstanding.checked_add(1)
                });
        granted.assured("a connection holds at most a few dozen stream slots");
    }

    fn lease_returned(&self) {
        let returned =
            self.outstanding
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |outstanding| {
                    outstanding.checked_sub(1)
                });
        returned.assured("a lease task returns only the slots it recorded as granted");
        self.changed.notify_waiters();
    }

    fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::SeqCst)
    }

    /// Whether a task that holds one leased slot may return it under `last_return`.
    fn permits(&self, last_return: LastReturn) -> bool {
        match last_return {
            LastReturn::AfterDrainBegan => self.has_begun(),
            LastReturn::AfterEveryOtherSlot => self.has_begun() && self.outstanding() == 1,
        }
    }

    async fn until_permitted(&self, last_return: LastReturn) {
        loop {
            tokio::task::consume_budget().await;
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.permits(last_return) {
                return;
            }
            changed.await;
        }
    }
}

/// Lease every slot `reservation` holds, then return all but one of them, trying to lease again
/// after each return, and return the last one as `last_return` permits. Only this task leases from
/// its subquota, so a refusal below the reservation is explained by nothing but the drain, and a
/// drain that begins after the first lease must wait for this subquota whatever its size.
async fn lease_and_return_a_reservation(
    quotas: StreamSlotQuotas,
    reservation: Reservation,
    last_return: LastReturn,
    observation: Arc<DrainObservation>,
) {
    let subquota = reservation.subquota;
    let mut held = Vec::with_capacity(reservation.slots);
    while held.len() < reservation.slots {
        tokio::task::consume_budget().await;
        let Some(slot) = quotas.try_lease(subquota) else {
            assert!(
                observation.has_begun(),
                "the {subquota:?} subquota refused a lease below its reservation of {} slots",
                reservation.slots
            );
            break;
        };
        observation.lease_granted(subquota);
        held.push(slot);
    }
    if held.len() == reservation.slots {
        let beyond_reservation = quotas.try_lease(subquota);
        assert!(
            beyond_reservation.is_none(),
            "the {subquota:?} subquota leased a slot beyond its reservation of {} slots",
            reservation.slots
        );
    }

    while held.len() > 1 {
        tokio::task::consume_budget().await;
        let slot = held
            .pop()
            .verified("the loop runs only while more than one slot is held");
        observation.lease_returned();
        drop(slot);
        let Some(slot) = quotas.try_lease(subquota) else {
            assert!(
                observation.has_begun(),
                "the {subquota:?} subquota refused a lease while one of its slots was free"
            );
            continue;
        };
        observation.lease_granted(subquota);
        observation.lease_returned();
        drop(slot);
    }

    let Some(last) = held.pop() else {
        return;
    };
    observation.until_permitted(last_return).await;
    match last_return {
        LastReturn::AfterDrainBegan => {}
        LastReturn::AfterEveryOtherSlot => {
            for _ in 0..LAST_SLOT_HOLD_YIELDS {
                tokio::task::consume_budget().await;
                assert!(
                    !observation.has_completed(),
                    "the drain completed while the {subquota:?} subquota still held a leased slot"
                );
            }
        }
    }
    observation.lease_returned();
    drop(last);
}

/// Drain the connection, recording that the drain began once `drain` has returned its wait.
async fn drain_while_leased(
    quotas: StreamSlotQuotas,
    reservations: Vec<Reservation>,
    observation: Arc<DrainObservation>,
) {
    let drained = quotas.drain();
    observation.begin();
    drained.await;
    observation.complete();
    assert_eq!(
        observation.outstanding(),
        0,
        "the drain completed while leased slots were still out"
    );
    for reservation in reservations {
        let lease = quotas.try_lease(reservation.subquota);
        assert!(
            lease.is_none(),
            "the {:?} subquota leased a slot after the drain completed",
            reservation.subquota
        );
    }
}

/// Every subquota of `class` leases its whole reservation and returns it while the connection
/// drains. The drain completes only after every leased slot returns, no lease is granted once the
/// drain began, and until then a saturated subquota never consumes another subquota's reservation.
/// The schedule chooses one subquota to return its last slot after all the others, so a drain that
/// stops waiting early is caught whichever subquota it stops waiting for.
fn drain_stops_leasing_and_waits_for_every_leased_slot(class: PoolClass) {
    shuttle::future::block_on(async move {
        let quotas = StreamSlotQuotas::new(class);
        let reservations = reservations(class);
        let observation = Arc::new(DrainObservation::default());
        let returned_last = thread_rng().gen_range(0..reservations.len());

        let mut leases = Vec::with_capacity(reservations.len());
        for (index, reservation) in reservations.iter().enumerate() {
            let last_return = if index == returned_last {
                LastReturn::AfterEveryOtherSlot
            } else {
                LastReturn::AfterDrainBegan
            };
            leases.push(tokio::spawn(lease_and_return_a_reservation(
                quotas.clone(),
                *reservation,
                last_return,
                Arc::clone(&observation),
            )));
        }
        let drain = tokio::spawn(drain_while_leased(
            quotas,
            reservations,
            Arc::clone(&observation),
        ));

        for lease in leases {
            tokio::task::consume_budget().await;
            lease
                .await
                .assured("a lease task panics only on a violated invariant, which fails the check");
        }
        drain
            .await
            .assured("the drain task panics only on a violated invariant, which fails the check");
    });
}

#[test]
fn management_drain_stops_leasing_and_waits_for_every_leased_slot() {
    check_random_and_pct(|| {
        drain_stops_leasing_and_waits_for_every_leased_slot(PoolClass::Management);
    });
}

#[test]
fn replication_drain_stops_leasing_and_waits_for_every_leased_slot() {
    check_random_and_pct(|| {
        drain_stops_leasing_and_waits_for_every_leased_slot(PoolClass::Replication);
    });
}

#[test]
fn bulk_drain_stops_leasing_and_waits_for_every_leased_slot() {
    check_random_and_pct(|| {
        drain_stops_leasing_and_waits_for_every_leased_slot(PoolClass::Bulk);
    });
}

#[test]
fn relay_drain_stops_leasing_and_waits_for_every_leased_slot() {
    check_random_and_pct(|| {
        drain_stops_leasing_and_waits_for_every_leased_slot(PoolClass::Relay);
    });
}
