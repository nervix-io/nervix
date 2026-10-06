//! The memory-ordering claims of runtime-state assignment, explored by Loom over the production
//! authority and its admission count.
//!
//! Layer: test harness.
//!
//! - **Owns.** The admission-fence invariant of a state assignment: an operation admitted under a
//!   binding that a rebind supersedes either observes the new binding and is refused, or finishes
//!   before the rebind returns, with everything it did visible to the rebinding side. And the
//!   drain's publication invariant: a drain that waits out an admitted operation observes every
//!   write that operation completed.
//! - **Depends on.** The production `StateAssignmentAuthority` and `StateAdmissions`, and the Loom
//!   runner of `nervix-model-harness`.
//! - **Must not know.** What a runtime state holds, how it is persisted or replicated, which node
//!   owns it, or the internals of its `ArcSwap` publication.
//!
//! In the admission-fence model, one thread runs an operation admitted under the first binding and
//! the other rebinds, so the only synchronization between them is the authority's own. The rebind
//! holds the assignment barrier, a real lock outside every model that only the rebinding thread
//! takes, and publishes the roles through a real publication the operation never reads. In the
//! drain model, the operation is admitted before its observer starts, so only the operation's
//! completion and the drain's read of the count synchronize what it wrote: no join, notification
//! or lock publishes the witness. `just test-loom` runs both models, and
//! `just test-loom-qualification` shows that weakening the rebind's read of the admission count,
//! or the release an operation makes when it finishes, makes them fail.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::{StateAdmissions, StateAssignmentAuthority, StateCapability, StateReplicationRoles};

const ADMISSION_FENCE: InvariantId = InvariantId::new("runtime.state-assignment.admission-fence");
const DRAIN: InvariantId = InvariantId::new("server.state-assignment.drained-publication");

/// What the admitted operation writes. Both sides access it with `Relaxed`, so only the authority's
/// own orderings can publish it.
const WRITTEN_BY_THE_OPERATION: usize = 1;

#[test]
fn loom_an_operation_admitted_under_a_superseded_binding_never_outlives_its_rebind() {
    explore(ADMISSION_FENCE, || {
        let authority = Arc::new(StateAssignmentAuthority::default());
        let token = authority
            .rebind(StateReplicationRoles::owned_by(None), None)
            .token_for(StateCapability::Originate)
            .assured("a state no node owns is originated where it lives");
        let witness = Arc::new(AtomicUsize::new(0));
        let operating_authority = Arc::clone(&authority);
        let operating_witness = Arc::clone(&witness);
        let operation = spawn(move || {
            operating_authority
                .authorize(token, StateCapability::Originate, || {
                    operating_witness.store(WRITTEN_BY_THE_OPERATION, Ordering::Relaxed);
                })
                .is_ok()
        });

        authority.rebind(StateReplicationRoles::owned_by(None), None);
        // Read the moment the rebind returns, before the join could order the threads.
        let observed = witness.load(Ordering::Relaxed);
        let ran = operation
            .join()
            .assured("the operating side only runs one admitted operation");
        assert!(
            !ran || observed == WRITTEN_BY_THE_OPERATION,
            "an operation admitted under the superseded binding was still running after its \
             superseding rebind returned"
        );
    });
}

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
