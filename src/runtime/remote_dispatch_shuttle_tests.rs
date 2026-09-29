//! The silence sweep of forwarded record acknowledgements, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The single-resolution and reported-share invariants the production registry of
//!   forwarded acknowledgements is held to while its sweep races reports and terminal outcomes.
//! - **Depends on.** The remote dispatch registry, acknowledgement roots, and the server Shuttle
//!   runner.
//! - **Must not know.** Relays, deliveries, the interconnect, or what the acknowledged records are.

use futures_util::FutureExt as _;
use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;
use shuttle::thread;
use triomphe::Arc;

use super::{REMOTE_ACK_SILENT_SWEEPS, RemoteDispatchRegistry};
use crate::{
    runtime_ack::{AckCompletion, AckOutcome, AckSet},
    shuttle_test::check_interleavings,
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
        let registry = Arc::new(RemoteDispatchRegistry::new());
        let (acks, completion) = AckSet::root();
        let ack_id = registry.next_ack_id();
        registry.register_ack(ack_id, receiver(), acks);
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
