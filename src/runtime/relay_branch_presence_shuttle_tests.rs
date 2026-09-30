//! Relay branch presence publication, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants that observers of a relay's branch presence see each owner step whole,
//!   never an older step than one the owner finished, and never a replaced owner's branches once
//!   its successor has claimed the presence.
//! - **Depends on.** The production relay presence, the branch owner the relay owner task holds,
//!   and the server Shuttle runner.
//! - **Must not know.** Relay buffers, fan-out, metrics, or how batches reach the owner.

// Unmodeled atomics are not Shuttle scheduling points, so each step record below changes in the
// same scheduling step as the publication it records.
use std::time::Duration;

use nervix_models::Timestamp;
use nervix_primitives::{
    sync::{Arc, StdArc},
    thread,
    unmodeled::sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use nonzero_ext::nonzero;

use super::*;
use crate::{runtime_schema::RuntimeValue, shuttle_test::check_interleavings};

const MODEL_THREAD_JOINS: &str =
    "Shuttle fails the execution when a model thread panics, so no join observes one";
const INFALLIBLE: &str = "the check's branch constructor cannot fail";

fn tenant(name: &str) -> Option<BranchKey> {
    Some(
        BranchKey::from_fields([(
            FieldName::parse("tenant").assured("the check uses a valid field name"),
            RuntimeValue::String(name.to_string()),
        )])
        .assured("one field forms a concrete branch key"),
    )
}

fn at(millis: i64) -> Timestamp {
    Timestamp::from_unix_nanos(
        millis
            .checked_mul(1_000_000)
            .assured("the check's clock stays within a second"),
    )
}

/// Admit one batch for `key` through `owner`, the way the relay owner admits a batch it fans out.
fn admit(
    owner: &mut OwnedBranches<BranchKey, ()>,
    key: &Option<BranchKey>,
    millis: i64,
    capacity: Option<NonZeroUsize>,
) {
    owner
        .admit(key.as_ref(), at(millis), capacity, |_, _| {
            Ok::<(), std::convert::Infallible>(())
        })
        .assured(INFALLIBLE);
}

/// Which of `branches` a membership holds, in order.
fn holds(presence: &RelayBranchPresence, branches: &[&Option<BranchKey>]) -> Vec<bool> {
    let membership = presence.load();
    branches
        .iter()
        .map(|branch| membership.contains(branch.as_ref()))
        .collect()
}

/// An owner at capacity one admits `acme`, then `beta`, which evicts `acme`, then `acme` again,
/// which recreates it and evicts `beta`, and finally releases its presence. An observer that
/// registers while this runs reads the membership repeatedly.
///
/// Every membership it reads is one the owner published whole, so it never holds both branches at
/// capacity one, and it is never older than the step the owner had finished before the read.
fn an_observer_sees_every_owner_step_whole_and_never_an_older_one() {
    let presence: RelayBranchPresence = Arc::new(BranchPresence::new());
    let acme = tenant("acme");
    let beta = tenant("beta");
    // The membership after each owner step, as whether it holds acme and beta.
    let steps = [
        [false, false],
        [true, false],
        [false, true],
        [true, false],
        [false, false],
    ];
    let finished = StdArc::new(AtomicUsize::new(0));
    let mut owner = OwnedBranches::claim(presence.clone());

    let owner_finished = StdArc::clone(&finished);
    let owner_acme = acme.clone();
    let owner_beta = beta.clone();
    let writer = thread::spawn(move || {
        let capacity = Some(nonzero!(1usize));
        admit(&mut owner, &owner_acme, 1, capacity);
        owner_finished.store(1, Ordering::SeqCst);
        admit(&mut owner, &owner_beta, 2, capacity);
        owner_finished.store(2, Ordering::SeqCst);
        admit(&mut owner, &owner_acme, 3, capacity);
        owner_finished.store(3, Ordering::SeqCst);
        drop(owner);
        owner_finished.store(4, Ordering::SeqCst);
    });
    let observer_presence = presence.clone();
    let observer = thread::spawn(move || {
        let registered = observer_presence;
        for _ in 0..4 {
            let floor = finished.load(Ordering::SeqCst);
            let seen = holds(&registered, &[&acme, &beta]);
            let current = (floor..steps.len()).any(|step| steps[step].as_slice() == seen);
            assert!(
                current,
                "an observer read {seen:?} after the owner finished step {floor}; the owner never \
                 published it at or after that step"
            );
            thread::yield_now();
        }
    });

    writer.join().assured(MODEL_THREAD_JOINS);
    observer.join().assured(MODEL_THREAD_JOINS);
    assert_eq!(
        holds(&presence, &[&tenant("acme"), &tenant("beta")]),
        [false, false]
    );
}

#[test]
fn shuttle_an_observer_sees_every_owner_step_whole_and_never_an_older_one() {
    check_interleavings(an_observer_sees_every_owner_step_whole_and_never_an_older_one);
}

/// An owner at capacity two admits three branches and then expires the ones idle past the TTL,
/// while an observer reads the membership. No read exceeds the owner's capacity, and once the
/// owner has finished its expiry no read holds an expired branch.
fn capacity_and_expiry_publish_whole_memberships() {
    let presence: RelayBranchPresence = Arc::new(BranchPresence::new());
    let acme = tenant("acme");
    let beta = tenant("beta");
    let gamma = tenant("gamma");
    let expired = StdArc::new(AtomicBool::new(false));
    let mut owner = OwnedBranches::claim(presence.clone());

    let owner_expired = StdArc::clone(&expired);
    let branches = [acme.clone(), beta.clone(), gamma.clone()];
    let writer = thread::spawn(move || {
        let capacity = Some(nonzero!(2usize));
        admit(&mut owner, &branches[0], 0, capacity);
        admit(&mut owner, &branches[1], 10, capacity);
        admit(&mut owner, &branches[2], 20, capacity);
        let released = owner.expire(at(35), Duration::from_millis(20));
        assert_eq!(
            released.len(),
            1,
            "only beta has been idle for the whole TTL"
        );
        owner_expired.store(true, Ordering::SeqCst);
        owner
    });
    let observer_presence = presence.clone();
    let observer = thread::spawn(move || {
        for _ in 0..4 {
            let finished_expiry = expired.load(Ordering::SeqCst);
            let seen = holds(&observer_presence, &[&acme, &beta, &gamma]);
            let held = seen.iter().filter(|held| **held).count();
            assert!(
                held <= 2,
                "a read held {held} branches of an owner at capacity two"
            );
            if finished_expiry {
                assert_eq!(
                    seen,
                    [false, false, true],
                    "a read after the expiry must hold only gamma"
                );
            }
            thread::yield_now();
        }
    });

    let owner = writer.join().assured(MODEL_THREAD_JOINS);
    observer.join().assured(MODEL_THREAD_JOINS);
    assert_eq!(
        holds(
            &presence,
            &[&tenant("acme"), &tenant("beta"), &tenant("gamma")]
        ),
        [false, false, true]
    );
    drop(owner);
}

#[test]
fn shuttle_capacity_and_expiry_publish_whole_memberships() {
    check_interleavings(capacity_and_expiry_publish_whole_memberships);
}

/// A predecessor owner keeps admitting and then ends while its successor claims the presence and
/// admits a branch of its own, and an observer reads throughout. Once the successor's claim is
/// visible, no read holds a branch only the predecessor admitted, and the predecessor's release
/// never removes the successor's branch.
fn a_replaced_owner_never_publishes_over_its_successor() {
    let presence: RelayBranchPresence = Arc::new(BranchPresence::new());
    let acme = tenant("acme");
    let beta = tenant("beta");
    let gamma = tenant("gamma");
    let claimed = StdArc::new(AtomicBool::new(false));
    let mut predecessor = OwnedBranches::claim(presence.clone());
    admit(&mut predecessor, &acme, 1, None);

    let predecessor_beta = beta.clone();
    let predecessor_thread = thread::spawn(move || {
        admit(&mut predecessor, &predecessor_beta, 2, None);
        thread::yield_now();
        predecessor.expire(at(1_000), Duration::from_millis(1));
        thread::yield_now();
        drop(predecessor);
    });
    let successor_presence = presence.clone();
    let successor_claimed = StdArc::clone(&claimed);
    let successor_gamma = gamma.clone();
    let successor_thread = thread::spawn(move || {
        let mut successor = OwnedBranches::claim(successor_presence);
        successor_claimed.store(true, Ordering::SeqCst);
        admit(&mut successor, &successor_gamma, 3, None);
        successor
    });
    let observer_presence = presence.clone();
    let observer_acme = acme.clone();
    let observer_beta = beta.clone();
    let observer = thread::spawn(move || {
        for _ in 0..4 {
            let after_claim = claimed.load(Ordering::SeqCst);
            let seen = holds(&observer_presence, &[&observer_acme, &observer_beta]);
            if after_claim {
                assert_eq!(
                    seen,
                    [false, false],
                    "a read after the successor claimed held a branch only its predecessor \
                     admitted"
                );
            }
            thread::yield_now();
        }
    });

    predecessor_thread.join().assured(MODEL_THREAD_JOINS);
    let successor = successor_thread.join().assured(MODEL_THREAD_JOINS);
    observer.join().assured(MODEL_THREAD_JOINS);
    assert_eq!(
        holds(&presence, &[&acme, &beta, &gamma]),
        [false, false, true],
        "the successor's branch survives its predecessor's release"
    );
    drop(successor);
    assert_eq!(holds(&presence, &[&gamma]), [false]);
}

#[test]
fn shuttle_a_replaced_owner_never_publishes_over_its_successor() {
    check_interleavings(a_replaced_owner_never_publishes_over_its_successor);
}
