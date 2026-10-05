//! The memory-ordering claims of the relay dispatch gate and fan-out, explored by Loom over the
//! production owners.
//!
//! Layer: test harness.
//!
//! - **Owns.** The ordering invariants of the dispatch gate: a fence counts every dispatch that
//!   finds the gate open, a lease follows what every counted dispatch did, the last dispatch to
//!   leave a closed gate wakes its fence, and a dispatch that finds the gate reopened follows what
//!   the protected mutation changed. And the fan-out's: a consumer that frees room a waiting
//!   publisher needs either admits it or wakes it.
//! - **Depends on.** The production gate and fan-out, and the Loom runner of
//!   `nervix-model-harness`.
//! - **Must not know.** Relays, batches, acknowledgements, or why a gate is engaged.
//!
//! In each model one thread dispatches, releases or consumes and the other engages, looks at the
//! fence or waits for room, so the only synchronization between them is the owner's own. The gate's
//! engagement state is behind a real lock, its wakeups and the fan-out's are real notifications, and
//! the fan-out's queues are real lock-free queues, all outside every model; only one thread of each
//! model takes that lock, and each model observes the decisions the owners' atomics make, never a
//! wakeup. `just test-loom` runs them, and `just test-loom-qualification` shows that weakening the
//! decisive ordering of each makes it fail.

use std::num::NonZeroUsize;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::{
    OwnedRelayDispatchPermit, RelayAdmissionFreed, RelayAdmissionWait, RelayBroadcast,
    RelayDispatchFence, RelayDispatchGate, RelayDispatchLeft, deadline_no_model_reaches,
};

const ENTRY_FENCE: InvariantId = InvariantId::new("runtime.relay-dispatch-gate.entry-fence");
const LEASE_PUBLICATION: InvariantId =
    InvariantId::new("runtime.relay-dispatch-gate.lease-publication");
const DRAIN_WAKEUP: InvariantId = InvariantId::new("runtime.relay-dispatch-gate.drain-wakeup");
const REOPEN_PUBLICATION: InvariantId =
    InvariantId::new("runtime.relay-dispatch-gate.reopen-publication");
const ADMISSION_WAKEUP: InvariantId = InvariantId::new("runtime.relay-fanout.admission-wakeup");

/// What a dispatch writes under its permit, or the protected mutation writes while the gate is
/// closed. Both sides access it with `Relaxed`, so only the gate's own orderings can publish it.
const WITNESSED: usize = 1;

#[test]
fn loom_a_fence_counts_every_dispatch_that_finds_the_gate_open() {
    explore(ENTRY_FENCE, || {
        let gate = Arc::new(RelayDispatchGate::new());
        let dispatching = Arc::clone(&gate);
        // The dispatch keeps any count it raised: a permit that entered is never dropped here.
        let dispatch = spawn(move || dispatching.try_enter());

        let generation = gate.engage(deadline_no_model_reaches(), "loom entry fence");
        let fence = gate.poll_quiescence(generation);
        // Both decisions were taken before the join below, which orders nothing either side read.
        let entered = dispatch
            .join()
            .assured("the dispatching side only enters the gate");
        assert!(
            !(entered && fence == RelayDispatchFence::Quiescent),
            "the fence took its lease while a dispatch it did not count was still running"
        );
    });
}

#[test]
fn loom_a_lease_follows_what_every_counted_dispatch_did_under_its_permit() {
    explore(LEASE_PUBLICATION, || {
        let gate = Arc::new(RelayDispatchGate::new());
        assert!(gate.try_enter(), "an unengaged gate admits a dispatch");
        let permit = OwnedRelayDispatchPermit {
            gate: Arc::clone(&gate),
        };
        let witness = Arc::new(AtomicUsize::new(0));
        let dispatch_witness = Arc::clone(&witness);
        let dispatch = spawn(move || {
            dispatch_witness.store(WITNESSED, Ordering::Relaxed);
            drop(permit);
        });

        let generation = gate.engage(deadline_no_model_reaches(), "loom lease publication");
        if gate.poll_quiescence(generation) == RelayDispatchFence::Quiescent {
            // Read the moment the fence takes its lease, before the join could order the threads.
            assert_eq!(
                witness.load(Ordering::Relaxed),
                WITNESSED,
                "the fence took its lease without what the dispatch did under its permit"
            );
        }
        dispatch
            .join()
            .assured("the dispatching side only writes the witness and drops its permit");
    });
}

#[test]
fn loom_the_last_dispatch_to_leave_a_closed_gate_wakes_its_fence() {
    explore(DRAIN_WAKEUP, || {
        let gate = Arc::new(RelayDispatchGate::new());
        assert!(gate.try_enter(), "an unengaged gate admits a dispatch");
        let leaving = Arc::clone(&gate);
        let dispatch = spawn(move || leaving.release_dispatch());

        let generation = gate.engage(deadline_no_model_reaches(), "loom drain wakeup");
        let fence = gate.poll_quiescence(generation);
        let left = dispatch
            .join()
            .assured("the dispatching side only leaves the gate");
        assert!(
            fence == RelayDispatchFence::Quiescent || left == RelayDispatchLeft::Drained,
            "the fence waits for a dispatch that left without waking it"
        );
    });
}

#[test]
fn loom_a_dispatch_that_finds_the_gate_reopened_observes_the_protected_change() {
    explore(REOPEN_PUBLICATION, || {
        let gate = Arc::new(RelayDispatchGate::new());
        let generation = gate.engage(deadline_no_model_reaches(), "loom reopen publication");
        let change = Arc::new(AtomicUsize::new(0));
        let releasing = Arc::clone(&gate);
        let releasing_change = Arc::clone(&change);
        let release = spawn(move || {
            releasing_change.store(WITNESSED, Ordering::Relaxed);
            releasing.release(generation);
        });

        if gate.try_enter() {
            // Read the moment the dispatch enters, before the join could order the threads.
            assert_eq!(
                change.load(Ordering::Relaxed),
                WITNESSED,
                "a dispatch entered the reopened gate without the change made while it was closed"
            );
        }
        release
            .join()
            .assured("the releasing side only writes the change and releases its engagement");
    });
}

#[test]
fn loom_a_consumer_freeing_room_admits_or_wakes_the_publisher_waiting_for_it() {
    explore(ADMISSION_WAKEUP, || {
        const CAPACITY: usize = 1;
        let fanout = RelayBroadcast::with_capacity(
            NonZeroUsize::new(CAPACITY).assured("the model's capacity is one"),
        );
        let receiver = fanout.new_receiver();
        // The consumer holds one delivered batch, so it is at capacity before either side runs.
        assert!(
            receiver.consumer.try_admit(CAPACITY),
            "an empty consumer admits a batch"
        );
        receiver.consumer.deliver(WITNESSED);
        let consuming_fanout = Arc::clone(&fanout.fanout);
        let consuming_queue = Arc::clone(&receiver.consumer);
        let consumer = spawn(move || {
            let (_batch, freed) = consuming_fanout
                .take_from(&consuming_queue)
                .assured("the consumer holds one delivered batch");
            freed
        });

        let wait = RelayAdmissionWait::begin(&fanout.fanout);
        let admitted = receiver.consumer.try_admit_while_waiting(&wait, CAPACITY);
        // Both decisions were taken before the join below, which orders nothing either side read.
        let freed = consumer
            .join()
            .assured("the consuming side only takes the delivered batch");
        assert!(
            admitted || freed == RelayAdmissionFreed::PublisherWaiting,
            "a consumer freed room without admitting or waking the publisher waiting for it"
        );
        drop(wait);
    });
}
