//! The memory-ordering claim of the client batch admission fence, explored by Loom over the
//! production quiesce control and acknowledgement trackers.
//!
//! Layer: test harness.
//!
//! - **Owns.** The admission-fence invariant: a client batch whose admission races a quiesce is
//!   either counted by the drain that follows the quiesce, or refused before anything is
//!   dispatched under it.
//! - **Depends on.** `IngestorQuiesceControl::track_client_batch`, the ingestor's acknowledgement
//!   root trackers, and the Loom runner of `nervix-model-harness`.
//! - **Must not know.** The endpoint task, producers, the admission worker, or what a batch holds.
//!
//! One thread admits a batch and the other engages a quiesce and reads the counts a drain reads,
//! so the only synchronization between them is the fence's own. The quiesce decision is a real
//! publication outside every model: Loom runs its loads and replacements in the order it schedules
//! the threads, and it schedules a thread only at its own operations, exploring the orders of those
//! that touch the same location. The draining thread therefore reads the root count once before it
//! engages, as a drain that polls does. That read touches the count the admission raises, so Loom
//! also explores running the whole admission, including its real read of the decision, before the
//! engagement publishes. The model then observes whether the root counts' own orderings close the
//! race. A yield would not do: after a thread yields, Loom never lets its loads return a value the
//! thread saw before the yield, which hides exactly the stale read the race needs. `just test-loom`
//! runs the model, and `just test-loom-qualification` shows that a drain loading either count
//! instead of reading it by read-modify-write makes it fail.

use meticulous::ResultExt as _;
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_models::{DomainName, IngestorName};
use nervix_primitives::sync::Arc;

use super::{
    IngestQuiesceMode, IngestorAckRootTrackers, IngestorQuiesceCause, IngestorQuiesceControl,
    RuntimeMetrics,
};

const ADMISSION_FENCE: InvariantId = InvariantId::new("runtime.client-ingestor.admission-fence");

#[test]
fn loom_a_client_batch_racing_a_quiesce_is_counted_by_its_drain_or_refused() {
    explore(ADMISSION_FENCE, || {
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("tenant").assured("a literal domain name");
        let ingestor = IngestorName::parse("orders_in").assured("a literal ingestor name");
        let metric_labels = metrics.register_ingestor_quiesce(&domain, &ingestor, None);
        let control = Arc::new(IngestorQuiesceControl::new(
            IngestQuiesceMode::Suspend,
            metrics,
            metric_labels,
        ));
        let trackers = Arc::new(IngestorAckRootTrackers::detached());
        let admitting_control = Arc::clone(&control);
        let admitting_trackers = Arc::clone(&trackers);
        // An admitted batch keeps its root unresolved past the join, as one dispatched is.
        let admission = spawn(move || {
            admitting_control
                .track_client_batch(&admitting_trackers)
                .ok()
        });

        // The read a polling drain makes before the quiesce, which lets the admission run first:
        // see the module documentation. Nothing is decided on it.
        let held_before_the_quiesce = trackers.ingestor_outstanding();
        control.engage(IngestorQuiesceCause::OwnershipHandoff);
        let held = trackers.ingestor_outstanding();
        let held_for_handoff = trackers.ingestor_outstanding_for_ownership_handoff();
        // Every decision was taken before the join below, which orders nothing either side read.
        let admitted = admission
            .join()
            .assured("the admitting side only tracks and decides one batch");
        if admitted.is_some() {
            assert!(
                held != 0 && held_for_handoff != 0,
                "a client batch was admitted after a quiesce whose drain did not count it"
            );
        }
        assert!(
            held_before_the_quiesce <= 1,
            "one admission tracks one root"
        );
    });
}
