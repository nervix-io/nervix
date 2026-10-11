//! The silence sweep of forwarded record acknowledgements, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The single-resolution and reported-share invariants the production registry of
//!   forwarded acknowledgements is held to while its sweep races reports and terminal outcomes.
//! - **Depends on.** The remote dispatch registry, acknowledgement roots, and the server Shuttle
//!   runner.
//! - **Must not know.** Relays, deliveries, the interconnect, or what the acknowledged records are.

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::ClusterNodeName;
use nervix_primitives::{sync::Arc, thread};

use crate::{
    runtime::{
        remote_ack_owner::RemoteDispatchRegistry,
        remote_dispatch::{REMOTE_ACK_SILENT_SWEEPS, RelayAdmissionUpdate},
    },
    runtime_ack::{AckCompletion, AckOutcome, AckSet},
};

const JOINED: &str =
    "a modeled thread's panic fails the Shuttle execution before its joiner resumes";

fn receiver() -> ClusterNodeName {
    ClusterNodeName::parse("node-3").assured("the fixture node name is valid")
}

/// A registry holding one forwarded acknowledgement of an admitted delivery whose receiver reported
/// nothing for every sweep but the one that fails it, with the number it is registered under and
/// the completion of the root it stands for.
struct OneSweepFromFailing {
    registry: Arc<RemoteDispatchRegistry>,
    ack_id: u64,
    completion: AckCompletion,
}

impl OneSweepFromFailing {
    fn new() -> Self {
        let registry = Arc::new(RemoteDispatchRegistry::with_capacity(2));
        let (acks, completion) = AckSet::root();
        let ack_id = registry
            .register_ack(receiver(), acks)
            .assured("the fixture has free correlation capacity");
        registry.admit_ack(ack_id);
        for _ in 0..REMOTE_ACK_SILENT_SWEEPS {
            assert!(
                registry.fail_silent_acks().is_empty(),
                "a share fails only at the first sweep past the silence bound"
            );
        }
        Self {
            registry,
            ack_id,
            completion,
        }
    }

    /// The outcome the root delivered, or `None` while it is unresolved.
    fn outcome(self) -> Option<AckOutcome> {
        self.completion.wait().now_or_never()
    }
}

/// The receiver's terminal outcome arrives while the sweep that would fail the share runs. Exactly
/// one of them takes the share out of the registry, and the root delivers the winner's outcome.
#[test]
fn shuttle_a_terminal_outcome_racing_the_final_sweep_resolves_the_share_once() {
    check_interleavings(|| {
        let model = OneSweepFromFailing::new();
        let sweeping = model.registry.clone();
        let sweep = thread::spawn(move || sweeping.fail_silent_acks());
        let resolving = model.registry.clone();
        let ack_id = model.ack_id;
        let resolve = thread::spawn(move || resolving.resolve_ack(ack_id, AckOutcome::Ack));

        let failed = sweep.join().assured(JOINED);
        let resolved = resolve.join().assured(JOINED);

        let failed_by_sweep = failed.get(&receiver()) == Some(&1);
        assert_ne!(
            failed_by_sweep, resolved,
            "exactly one of the sweep and the terminal outcome must take the share"
        );
        assert!(
            !model.registry.holds_ack(ack_id),
            "whichever resolved the share must have removed it"
        );
        match model.outcome() {
            Some(AckOutcome::Ack) => assert!(
                resolved,
                "the root may succeed only through the terminal outcome"
            ),
            Some(AckOutcome::NoAck(_)) => {
                assert!(failed_by_sweep, "the root may fail only through the sweep")
            }
            None => panic!("a share taken out of the registry must resolve its root"),
        }
    });
}

/// The receiver's report arrives while the sweep that would fail the share runs. A report that
/// reaches the share keeps it pending; only a report that finds it already removed lets it fail.
#[test]
fn shuttle_a_report_racing_the_final_sweep_keeps_the_share_it_reached() {
    check_interleavings(|| {
        let model = OneSweepFromFailing::new();
        let sweeping = model.registry.clone();
        let sweep = thread::spawn(move || sweeping.fail_silent_acks());
        let reporting = model.registry.clone();
        let ack_id = model.ack_id;
        let report = thread::spawn(move || reporting.report_ack(ack_id));

        let failed = sweep.join().assured(JOINED);
        let reached = report.join().assured(JOINED);

        if reached {
            assert!(
                failed.is_empty(),
                "a share its receiver reported before the sweep's removal must not fail"
            );
            assert!(
                model.registry.holds_ack(ack_id),
                "a reported share stays pending for its receiver's outcome"
            );
            assert!(
                model.outcome().is_none(),
                "a pending share must leave its root unresolved"
            );
            return;
        }
        assert_eq!(
            failed.get(&receiver()),
            Some(&1),
            "a report that found no share came after the sweep failed it"
        );
        assert!(!model.registry.holds_ack(ack_id));
        assert!(
            matches!(model.outcome(), Some(AckOutcome::NoAck(_))),
            "the sweep must fail the root of the share it removed"
        );
    });
}

/// A delivery claims its position for the first time while a sweep runs. The sweep bounds its scan
/// by the positions claimed so far, so it counts the new share or leaves it to the next sweep, and
/// no later sweep skips it: the share fails once its receiver has stayed silent for the bound.
#[test]
fn shuttle_a_first_claim_racing_a_sweep_is_counted_by_that_sweep_or_the_next() {
    check_interleavings(|| {
        let registry = Arc::new(RemoteDispatchRegistry::with_capacity(1));
        let (acks, completion) = AckSet::root();
        let registering = registry.clone();
        let registration = thread::spawn(move || {
            let ack_id = registering
                .register_ack(receiver(), acks)
                .assured("the fixture has free correlation capacity");
            registering.admit_ack(ack_id);
            ack_id
        });
        let sweeping = registry.clone();
        let sweep = thread::spawn(move || sweeping.fail_silent_acks());

        let ack_id = registration.join().assured(JOINED);
        let racing = sweep.join().assured(JOINED);
        assert!(
            racing.is_empty(),
            "a share cannot fail in the sweep it registers beside"
        );

        let mut later_sweeps = 0_u64;
        let failed = loop {
            later_sweeps = later_sweeps
                .checked_add(1)
                .assured("the check sweeps a bounded number of times");
            let failed = registry.fail_silent_acks();
            if !failed.is_empty() {
                break failed;
            }
            assert!(
                later_sweeps <= REMOTE_ACK_SILENT_SWEEPS,
                "a registered share must fail once its silence bound has passed"
            );
        };
        assert_eq!(failed.get(&receiver()), Some(&1));
        // The racing sweep either counted the admitted share, which leaves the bound itself to the
        // later sweeps, or left the share to them, which takes one sweep more.
        assert!(
            matches!(
                later_sweeps.checked_sub(REMOTE_ACK_SILENT_SWEEPS),
                Some(0 | 1)
            ),
            "the share failed after {later_sweeps} later sweeps"
        );
        assert!(!registry.holds_ack(ack_id));
        assert!(matches!(
            completion.wait().now_or_never(),
            Some(AckOutcome::NoAck(_))
        ));
    });
}

/// A sweep takes guards only for positions a side has claimed. A position no registrar has claimed
/// never held a correlation, so while that position's guard is held elsewhere a sweep and the
/// report it races both complete, and the reported share stays pending.
#[test]
fn shuttle_a_sweep_waits_for_no_position_a_side_has_not_claimed() {
    check_interleavings(|| {
        let registry = Arc::new(RemoteDispatchRegistry::with_capacity(2));
        let (acks, completion) = AckSet::root();
        let ack_id = registry
            .register_ack(receiver(), acks)
            .assured("the fixture has free correlation capacity");
        registry.admit_ack(ack_id);
        // The registration claimed the first delivery position. The second delivery position is
        // unclaimed, and so are both admission positions after it.
        let unclaimed_delivery = 1;
        let sweeping = registry.clone();
        let reporting = registry.clone();
        // A sweep that waited for the held position would never return, which the scheduler
        // reports as a deadlock of this execution.
        let (failed, reached) = registry.while_position_is_held(unclaimed_delivery, || {
            let sweep = thread::spawn(move || sweeping.fail_silent_acks());
            let report = thread::spawn(move || reporting.report_ack(ack_id));
            (sweep.join().assured(JOINED), report.join().assured(JOINED))
        });
        assert!(
            failed.is_empty(),
            "one sweep cannot exhaust a new share's bound"
        );
        assert!(reached, "a report must reach the share that is pending");
        assert!(registry.holds_ack(ack_id));
        assert!(
            completion.wait().now_or_never().is_none(),
            "a pending share must leave its root unresolved"
        );
    });
}

#[test]
fn shuttle_delayed_events_cannot_resolve_a_reused_delivery_position() {
    check_interleavings(|| {
        let registry = Arc::new(RemoteDispatchRegistry::with_capacity(1));
        let (first, first_completion) = AckSet::root();
        let first = registry
            .register_ack(receiver(), first)
            .assured("the first delivery fits");
        assert!(registry.resolve_ack(first, AckOutcome::Ack));
        let (current, current_completion) = AckSet::root();
        let current = registry
            .register_ack(receiver(), current)
            .assured("the completed delivery returned its position");
        assert_ne!(first, current);
        let delayed = registry.clone();
        let stale = thread::spawn(move || {
            assert!(!delayed.progress_ack(first, 1, true));
            assert!(!delayed.resolve_ack(
                first,
                AckOutcome::NoAck("delayed terminal reply".to_string())
            ));
        });
        let resolving = registry.clone();
        let resolve =
            thread::spawn(move || assert!(resolving.resolve_ack(current, AckOutcome::Ack)));
        stale.join().assured(JOINED);
        resolve.join().assured(JOINED);
        assert_eq!(
            first_completion.wait().now_or_never(),
            Some(AckOutcome::Ack)
        );
        assert_eq!(
            current_completion.wait().now_or_never(),
            Some(AckOutcome::Ack)
        );
        assert!(!registry.holds_ack(current));
    });
}

#[test]
fn shuttle_registration_racing_shutdown_leaves_no_unresolved_accepted_share() {
    check_interleavings(|| {
        let registry = Arc::new(RemoteDispatchRegistry::with_capacity(1));
        let (acks, completion) = AckSet::root();
        let registering = registry.clone();
        let registration = thread::spawn(move || {
            if registering.register_ack(receiver(), acks.clone()).is_err() {
                acks.no_ack("registration was refused");
            }
        });
        let ending = registry.clone();
        let shutdown = thread::spawn(move || ending.shutdown());
        registration.join().assured(JOINED);
        shutdown.join().assured(JOINED);
        assert!(matches!(
            completion.wait().now_or_never(),
            Some(AckOutcome::NoAck(_))
        ));
        assert!(registry.register_admission().is_err());
    });
}

#[test]
fn shuttle_a_departed_admission_waiter_racing_its_reply_returns_one_position() {
    check_interleavings(|| {
        let registry = Arc::new(RemoteDispatchRegistry::with_capacity(1));
        let (id, updates) = registry
            .register_admission()
            .assured("the fixture has admission room");
        let dropping = registry.clone();
        let departure = thread::spawn(move || {
            drop(updates);
            dropping.fail_silent_acks();
        });
        let replying = registry.clone();
        let reply = thread::spawn(move || replying.resolve_ack(id, AckOutcome::Ack));
        departure.join().assured(JOINED);
        reply.join().assured(JOINED);
        let (current, updates) = registry
            .register_admission()
            .assured("the departed admission returned exactly one position");
        assert_ne!(id, current);
        assert!(!registry.resolve_ack(id, AckOutcome::NoAck("delayed reply".to_string())));
        assert!(registry.holds_ack(current));
        assert!(registry.resolve_ack(current, AckOutcome::Ack));
        assert!(matches!(*updates.borrow(), RelayAdmissionUpdate::Admitted));
    });
}
