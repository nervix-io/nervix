//! The state assignment authority, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The fence, exclusion and yield invariants the production state assignment authority
//!   is held to across admission, rebind, exclusive installation and capture.
//! - **Depends on.** The state assignment types and the server Shuttle runner.
//! - **Must not know.** Which runtime state an assignment governs, or what an operation does to it.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::ClusterNodeName;
use nervix_recovery::Discarded as _;
use shuttle::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};
use triomphe::Arc;

use super::{
    StateAssignmentAuthority, StateAssignmentToken, StateCapability, StateReplicationRoles,
};
use crate::shuttle_test::check_interleavings;

const MODEL_THREAD_JOINS: &str =
    "Shuttle fails the whole execution when a model thread panics, so no join observes one";

/// Operation threads in the model that rebinds repeatedly.
const OPERATION_THREADS: usize = 2;
/// Originating threads in the model that installs snapshots.
const ORIGINATOR_THREADS: usize = 2;
/// Operations or captures each operating or capturing thread performs.
const OPERATIONS_PER_THREAD: usize = 4;
/// Rebinds after the first assignment. The assignments run from generation one to four, so each
/// admission counter serves two generations while operations are being admitted.
const REBINDS: usize = 3;

/// What the operations and rebinds of one fence model execution observe of each other.
struct FenceModel {
    authority: StateAssignmentAuthority,
    /// The token of the first assignment. Operations keep submitting it after rebinds supersede
    /// it, so stale admissions reach the counter its generation shares with later ones.
    first: StateAssignmentToken,
    /// The generation published by the latest rebind that has returned.
    returned_generation: AtomicU64,
    /// One slot per operation thread, holding the generation of the operation it is running.
    running_generations: Vec<RunningGeneration>,
}

/// The generation one operation thread runs its admitted operation under, as the single atomic
/// a rebinding thread reads while that operation runs.
///
/// No operation runs under generation zero, which is the unassigned binding and grants none, so
/// the slot spends zero on running nothing and answers with the generation only while one runs.
#[derive(Default)]
struct RunningGeneration(AtomicU64);

impl RunningGeneration {
    fn enter(&self, generation: u64) {
        self.0.store(generation, Ordering::SeqCst);
    }

    fn leave(&self) {
        self.0.store(0, Ordering::SeqCst);
    }

    /// The generation of the operation running in this slot, while one runs.
    fn running(&self) -> Option<u64> {
        match self.0.load(Ordering::SeqCst) {
            0 => None,
            generation => Some(generation),
        }
    }
}

impl FenceModel {
    fn new(operation_threads: usize) -> Self {
        let authority = StateAssignmentAuthority::default();
        let first = authority
            .rebind(StateReplicationRoles::owned_by(None), None)
            .token_for(StateCapability::Originate)
            .assured("a state without roles is originated locally");
        let mut running_generations = Vec::with_capacity(operation_threads);
        for _ in 0..operation_threads {
            running_generations.push(RunningGeneration::default());
        }
        Self {
            authority,
            first,
            returned_generation: AtomicU64::new(first.binding.fence()),
            running_generations,
        }
    }

    /// Submit operations from `operation_thread`, alternating a token for the assignment in
    /// force with the first token.
    fn submit_alternating(&self, operation_thread: usize) {
        for operation in 0..OPERATIONS_PER_THREAD {
            if operation.is_multiple_of(2) {
                let current = self
                    .authority
                    .current_binding()
                    .token_for(StateCapability::Originate)
                    .assured("every assignment of a state without roles is originated locally");
                self.submit(operation_thread, current);
            } else {
                self.submit(operation_thread, self.first);
            }
        }
    }

    /// Submit one operation under `token` from `operation_thread`. It either observes a
    /// superseding binding and never runs, or finishes before a rebind superseding its
    /// generation returns.
    fn submit(&self, operation_thread: usize, token: StateAssignmentToken) {
        let running_generation = self
            .running_generations
            .get(operation_thread)
            .assured("the model holds a slot for every operation thread it spawns");
        let generation = token.binding.fence();
        self.authority
            .authorize(token, StateCapability::Originate, || {
                running_generation.enter(generation);
                let returned = self.returned_generation.load(Ordering::SeqCst);
                assert!(
                    returned <= generation,
                    "an operation admitted under generation {generation} was still running after \
                     the rebind publishing generation {returned} returned"
                );
                running_generation.leave();
            })
            .discarded("a refused operation observed a superseding binding and never ran");
    }

    /// Rebind once and assert that no operation admitted under a superseded generation is still
    /// running when the rebind has returned.
    fn rebind(&self) {
        let published = self
            .authority
            .rebind(StateReplicationRoles::owned_by(None), None)
            .fence();
        self.returned_generation.store(published, Ordering::SeqCst);
        for running_generation in &self.running_generations {
            let Some(running) = running_generation.running() else {
                continue;
            };
            assert!(
                running >= published,
                "the rebind publishing generation {published} returned while an operation \
                 admitted under generation {running} was still running"
            );
        }
    }
}

/// What the originations, installations and captures of one installation model execution
/// observe of each other.
///
/// Each of them marks itself running, yields in place of the work it does, and only then looks
/// for the others, so a scheduler that prefers another thread runs it inside that work.
struct InstallationModel {
    authority: StateAssignmentAuthority,
    local: ClusterNodeName,
    peer: ClusterNodeName,
    /// The token of the first assignment, which makes the local node the owner.
    first: StateAssignmentToken,
    originations: AtomicUsize,
    installing: AtomicBool,
    capturing: AtomicBool,
}

impl InstallationModel {
    /// A model whose first assignment makes the local node the owner.
    fn new() -> Self {
        let local = ClusterNodeName::parse("node-1")
            .assured("the test node name satisfies the cluster-node grammar");
        let peer = ClusterNodeName::parse("node-2")
            .assured("the test node name satisfies the cluster-node grammar");
        let authority = StateAssignmentAuthority::default();
        let first = authority
            .rebind(
                StateReplicationRoles::owned_by(Some(local.clone())),
                Some(&local),
            )
            .token_for(StateCapability::Originate)
            .assured("the local node owns a state whose primary it is");
        Self {
            authority,
            local,
            peer,
            first,
            originations: AtomicUsize::new(0),
            installing: AtomicBool::new(false),
            capturing: AtomicBool::new(false),
        }
    }

    /// Originate repeatedly, alternating admitted and exclusive originations, each under the
    /// latest assignment this thread saw make the local node the owner. That assignment may
    /// since have made the local node a replica, as it does for a task still holding an
    /// originator a rebind replaced.
    fn originate_repeatedly(&self) {
        let mut token = self.first;
        for operation in 0..OPERATIONS_PER_THREAD {
            if let Some(current) = self
                .authority
                .current_binding()
                .token_for(StateCapability::Originate)
            {
                token = current;
            }
            self.originate(token, !operation.is_multiple_of(2));
        }
    }

    /// Originate under `token`, admitted beside other operations or exclusively.
    fn originate(&self, token: StateAssignmentToken, exclusive: bool) {
        let origination = || {
            self.originations.fetch_add(1, Ordering::SeqCst);
            thread::yield_now();
            assert!(
                !self.installing.load(Ordering::SeqCst),
                "an origination ran while a snapshot installation replaced the state"
            );
            self.originations.fetch_sub(1, Ordering::SeqCst);
        };
        let outcome = if exclusive {
            self.authority
                .authorize_exclusive(token, StateCapability::Originate, origination)
        } else {
            self.authority
                .authorize(token, StateCapability::Originate, origination)
        };
        outcome.discarded("a refused origination observed a superseding binding and never ran");
    }

    /// Install a snapshot under `token`, which a rebind granted the local node as a replica.
    fn install(&self, token: StateAssignmentToken) {
        self.authority
            .authorize_exclusive(token, StateCapability::InstallSnapshot, || {
                self.installing.store(true, Ordering::SeqCst);
                thread::yield_now();
                assert_eq!(
                    self.originations.load(Ordering::SeqCst),
                    0,
                    "a snapshot installation replaced the state while an origination ran"
                );
                assert!(
                    !self.capturing.load(Ordering::SeqCst),
                    "a snapshot installation replaced the state while a capture read it"
                );
                self.installing.store(false, Ordering::SeqCst);
            })
            .discarded("a refused installation observed a superseding binding and never ran");
    }

    /// Capture under the barrier, which keeps the assignment the capture observed in force
    /// until the capture finishes.
    fn capture(&self) {
        self.authority.serialize_with(|observed| {
            self.capturing.store(true, Ordering::SeqCst);
            thread::yield_now();
            assert!(
                !self.installing.load(Ordering::SeqCst),
                "a capture read the state while a snapshot installation replaced it"
            );
            assert_eq!(
                self.authority.current_binding(),
                observed,
                "the assignment changed while a capture held the barrier"
            );
            self.capturing.store(false, Ordering::SeqCst);
        });
    }

    /// Rebind the local node to replicate the state or to own it, returning the installation
    /// token a replica assignment grants.
    fn rebind(&self, replicate: bool) -> Option<StateAssignmentToken> {
        let roles = if replicate {
            StateReplicationRoles::new(Some(self.peer.clone()), vec![self.local.clone()], 1)
        } else {
            StateReplicationRoles::owned_by(Some(self.local.clone()))
        };
        self.authority
            .rebind(roles, Some(&self.local))
            .token_for(StateCapability::InstallSnapshot)
    }
}

/// One operation admitted under the first assignment races one rebind.
fn one_operation_against_one_rebind() {
    let model = Arc::new(FenceModel::new(1));
    let operation = thread::spawn({
        let model = model.clone();
        move || model.submit(0, model.first)
    });
    // A depth-first search runs the lowest-numbered runnable thread first and ignores yields,
    // so the rebind that spins on the operation is spawned after it.
    let rebinding = thread::spawn(move || model.rebind());
    operation.join().assured(MODEL_THREAD_JOINS);
    rebinding.join().assured(MODEL_THREAD_JOINS);
}

/// Operations with fresh and stale tokens race three rebinds.
fn operations_against_three_rebinds() {
    let model = Arc::new(FenceModel::new(OPERATION_THREADS));
    let mut operations = Vec::with_capacity(OPERATION_THREADS);
    for operation_thread in 0..OPERATION_THREADS {
        let model = model.clone();
        operations.push(thread::spawn(move || {
            model.submit_alternating(operation_thread);
        }));
    }
    // Spawned after the operations it spins on, for the depth-first search.
    let rebinding = thread::spawn(move || {
        for _ in 0..REBINDS {
            model.rebind();
        }
    });
    for operation in operations {
        operation.join().assured(MODEL_THREAD_JOINS);
    }
    rebinding.join().assured(MODEL_THREAD_JOINS);
}

/// Originations and captures race rebinds that move the local node between owning and
/// replicating the state, and each replica assignment hands its token to an installing thread,
/// as binding a replicated state hands its installer to the task that installs snapshots.
fn installations_originations_and_captures_against_three_rebinds() {
    let model = Arc::new(InstallationModel::new());
    let mut originators = Vec::with_capacity(ORIGINATOR_THREADS);
    for _ in 0..ORIGINATOR_THREADS {
        let model = model.clone();
        originators.push(thread::spawn(move || model.originate_repeatedly()));
    }
    let capturer = thread::spawn({
        let model = model.clone();
        move || {
            for _ in 0..OPERATIONS_PER_THREAD {
                model.capture();
            }
        }
    });
    let (installations_tx, installations_rx) = mpsc::channel();
    let installer = thread::spawn({
        let model = model.clone();
        move || {
            while let Ok(token) = installations_rx.recv() {
                model.install(token);
            }
        }
    });
    // Spawned after the originations it spins on, for the depth-first search.
    let rebinding = thread::spawn(move || {
        for rebind in 0..REBINDS {
            let Some(installation) = model.rebind(rebind.is_multiple_of(2)) else {
                continue;
            };
            installations_tx
                .send(installation)
                .assured("the installing thread receives until this sender is dropped");
        }
    });
    for originator in originators {
        originator.join().assured(MODEL_THREAD_JOINS);
    }
    capturer.join().assured(MODEL_THREAD_JOINS);
    rebinding.join().assured(MODEL_THREAD_JOINS);
    installer.join().assured(MODEL_THREAD_JOINS);
}

/// A rebind that returned while an operation admitted under the assignment it replaced was
/// still running would let that stale operation land after the new assignment took over. The
/// rebind spins until the operation finishes and yields through the execution crate while it
/// does, so a PCT schedule that ranks the rebind above the operation still runs the operation
/// rather than spinning until the step bound fails it.
#[test]
fn shuttle_a_rebind_yields_until_the_operation_admitted_under_its_replaced_binding_finishes() {
    check_interleavings(one_operation_against_one_rebind);
}

/// Rebinds serialize on the barrier, and each waits only for the admission counter of the
/// generation it supersedes, a counter that generation shares with the generations two apart.
/// Operations keep submitting the first token too, so stale admissions arrive at the counter a
/// later rebind waits on, and still no operation admitted under a superseded generation may run
/// once the rebind superseding it has returned.
#[test]
fn shuttle_no_operation_admitted_under_a_superseded_binding_outlives_its_superseding_rebind() {
    check_interleavings(operations_against_three_rebinds);
}

/// Installing a snapshot replaces the whole state, so no origination, admitted or exclusive,
/// and no capture may run beside it, and the assignment a capture observed stays in force for
/// the whole capture.
#[test]
fn shuttle_snapshot_installation_never_overlaps_origination_or_a_capture() {
    check_interleavings(installations_originations_and_captures_against_three_rebinds);
}
