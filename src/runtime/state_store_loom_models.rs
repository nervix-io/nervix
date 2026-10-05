//! Layer: test harness.
//! Owns: memory ordering evidence for draining admitted state-assignment operations.
//! May depend on: the production admission owner and the shared Loom harness.
//! Must not know: async orchestration or opaque ArcSwap publication internals.

use meticulous::ResultExt as _;
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::StateAdmissions;

const DRAIN: InvariantId = InvariantId::new("server.state-assignment.drained-publication");

#[test]
fn loom_assignment_drain_observes_every_completed_materialized_publication() {
    explore(DRAIN, || {
        let admissions = Arc::new(StateAdmissions::default());
        let payload = Arc::new(AtomicUsize::new(0));
        let admitted = admissions.admit(1);
        // The admission predates the observer. Only its production completion and the drain
        // synchronize the payload: no join, notification or lock publishes the witness.
        let observer = spawn({
            let admissions = admissions.clone();
            let payload = payload.clone();
            move || {
                admissions.wait_until_finished(1);
                assert_eq!(
                    payload.load(Ordering::Relaxed),
                    7,
                    "assignment drain omitted an admitted publication's preceding writes"
                );
            }
        });
        payload.store(7, Ordering::Relaxed);
        drop(admitted);
        observer.join().assured("the qualified observer finishes");
    });
}
