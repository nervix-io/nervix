//! The memory-ordering claim of runtime-state assignment, explored by Loom over the production
//! authority.
//!
//! Layer: test harness.
//!
//! - **Owns.** The admission-fence invariant of a state assignment: an operation admitted under a
//!   binding that a rebind supersedes either observes the new binding and is refused, or finishes
//!   before the rebind returns, with everything it did visible to the rebinding side.
//! - **Depends on.** The production `StateAssignmentAuthority` and the Loom runner of
//!   `nervix-model-harness`.
//! - **Must not know.** What a runtime state holds, how it is persisted or replicated, or which
//!   node owns it.
//!
//! One thread runs an operation admitted under the first binding and the other rebinds, so the only
//! synchronization between them is the authority's own. The rebind holds the assignment barrier, a
//! real lock outside every model that only the rebinding thread takes, and publishes the roles
//! through a real publication the operation never reads. `just test-loom` runs the model, and
//! `just test-loom-qualification` shows that weakening the rebind's read of the admission count,
//! or the release an operation makes when it finishes, makes it fail.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::{StateAssignmentAuthority, StateCapability, StateReplicationRoles};

const ADMISSION_FENCE: InvariantId = InvariantId::new("runtime.state-assignment.admission-fence");

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
