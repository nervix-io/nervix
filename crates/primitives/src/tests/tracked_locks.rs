//! The tracked locks of a `deloxide` build keep the contracts of the locks they replace.
//!
//! A tracked lock refuses to run before the process installs its detector, so every check here
//! starts by installing it, once for the whole test process. A check that deadlocked would reach
//! the sink, which fails the process: this suite's checks never block on one another's locks. The
//! cycles the detector reports are checked in disposable processes by nervix-deadlock, which owns
//! what a finding means.

use std::{
    sync::{Arc, Once},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};

use super::is_same_type;
use crate::{
    deadlock::{self, InstallError},
    sync::blocking::{
        Barrier, Condvar, Mutex, MutexGuard, Once as SelectedOnce, OnceLock, RwLock,
        RwLockReadGuard, RwLockWriteGuard, mpsc, tracked,
    },
    thread,
    unmodeled::time::Instant,
};

/// How long a participant may take to reach the state a check waits for. It only decides when a
/// broken check fails instead of hanging.
const PARTICIPANT_BOUND: Duration = Duration::from_secs(60);

const PARTICIPANT_JOINS: &str =
    "a participant panics only when an assertion fails, which fails the test first";

/// Install this test process's detector, once, with a sink that fails the process on any finding.
pub(super) fn detector_installed() {
    static INSTALLATION: Once = Once::new();
    INSTALLATION.call_once(|| {
        let installed = deadlock::install(|finding| {
            panic!("a conformance check of the tracked locks deadlocked: {finding:?}")
        });
        installed.assured("the test process installs its detector once, before any tracked lock");
    });
}

/// The locks and the condition variable are the tracked adapters; barriers, one-time
/// initialization and the channel stay the standard library's, which the detector does not track.
#[test]
fn deloxide_selects_the_tracked_locks_and_keeps_the_rest() {
    assert!(is_same_type::<Mutex<u8>, tracked::Mutex<u8>>());
    assert!(is_same_type::<
        MutexGuard<'static, u8>,
        tracked::MutexGuard<'static, u8>,
    >());
    assert!(is_same_type::<RwLock<u8>, tracked::RwLock<u8>>());
    assert!(is_same_type::<
        RwLockReadGuard<'static, u8>,
        tracked::RwLockReadGuard<'static, u8>,
    >());
    assert!(is_same_type::<
        RwLockWriteGuard<'static, u8>,
        tracked::RwLockWriteGuard<'static, u8>,
    >());
    assert!(is_same_type::<Condvar, tracked::Condvar>());
    assert!(is_same_type::<Barrier, std::sync::Barrier>());
    assert!(is_same_type::<SelectedOnce, std::sync::Once>());
    assert!(is_same_type::<OnceLock<u8>, std::sync::OnceLock<u8>>());
    assert!(is_same_type::<mpsc::Sender<u8>, std::sync::mpsc::Sender<u8>>());
}

/// `Debug` never waits: a held lock prints a placeholder instead of its value, as `parking_lot`'s
/// does, and readers still see a read-write lock's value while other readers hold it.
#[test]
fn debug_formatting_never_waits_for_a_held_lock() {
    detector_installed();
    let mutex = Mutex::new(7_u8);
    assert_eq!(format!("{mutex:?}"), "Mutex { data: 7 }");
    let held = mutex.lock();
    assert_eq!(format!("{mutex:?}"), "Mutex { data: <locked> }");
    assert_eq!(format!("{held:?} {held}"), "7 7");
    drop(held);

    let lock = RwLock::new(3_u8);
    let reader = lock.read();
    assert_eq!(format!("{lock:?}"), "RwLock { data: 3 }");
    drop(reader);
    let writer = lock.write();
    assert_eq!(format!("{lock:?}"), "RwLock { data: <locked> }");
    assert_eq!(format!("{writer:?} {writer}"), "3 3");
    drop(writer);

    assert_eq!(format!("{:?}", Condvar::new()), "Condvar { .. }");
}

/// The value moves in and out of a lock and is reachable through an exclusive borrow, and a lock is
/// built from its value or its value's default.
#[test]
fn a_lock_hands_its_value_in_and_out() {
    detector_installed();
    let mut mutex = Mutex::from(1_u8);
    *mutex.get_mut() = 2;
    assert_eq!(*mutex.lock(), 2);
    assert!(mutex.try_lock().is_some());
    assert_eq!(mutex.into_inner(), 2);
    assert_eq!(Mutex::<u8>::default().into_inner(), 0);

    let mut lock = RwLock::from(1_u8);
    *lock.get_mut() = 4;
    {
        let reader = lock.try_read().assured("no writer holds the lock");
        assert!(lock.try_write().is_none());
        assert_eq!(*reader, 4);
    }
    *lock.try_write().assured("no thread holds the lock") += 1;
    assert_eq!(lock.into_inner(), 5);
    assert_eq!(RwLock::<u8>::default().into_inner(), 0);
}

/// A notification counts exactly the waiters it wakes: every thread waiting when it starts a new
/// generation, and none that a later notification would find again.
#[test]
fn notify_all_counts_the_waiters_it_wakes() {
    detector_installed();
    let state = Arc::new((Mutex::new(Gate::default()), Condvar::default()));
    let mut waiters = Vec::new();
    for _ in 0..2 {
        let state = Arc::clone(&state);
        waiters.push(thread::spawn(move || {
            let (gate, changed) = &*state;
            let mut gate = gate.lock();
            gate.waiting = gate
                .waiting
                .checked_add(1)
                .assured("two waiters fit a byte");
            while !gate.open {
                changed.wait(&mut gate);
            }
        }));
    }
    let (gate, changed) = &*state;
    // A waiter counts itself under the lock and releases it only once its wait is recorded, so a
    // count of two seen under the lock means both waits are recorded.
    let started = Instant::now();
    loop {
        if gate.lock().waiting == 2 {
            break;
        }
        assert!(
            started.elapsed() < PARTICIPANT_BOUND,
            "both waiters start waiting"
        );
        thread::yield_now();
    }
    gate.lock().open = true;
    assert_eq!(changed.notify_all(), 2);
    for waiter in waiters {
        waiter.join().assured(PARTICIPANT_JOINS);
    }
    assert_eq!(changed.notify_all(), 0);
}

#[derive(Default)]
struct Gate {
    waiting: u8,
    open: bool,
}

/// The detector is installed once per process: a second installation is refused, and the first
/// stays in place.
#[test]
fn a_second_installation_is_refused() {
    detector_installed();
    let refused = deadlock::install(|_| {});
    let Err(refusal) = refused else {
        panic!("a second installation in one process succeeded");
    };
    assert_eq!(*refusal.current_context(), InstallError::AlreadyClaimed);
    assert!(deadlock::is_installed());
}

/// A thread that waits for a lock another one holds proceeds once it is released: tracking a wait
/// changes nothing about it.
#[test]
fn a_waiting_acquisition_proceeds_once_the_lock_is_released() {
    detector_installed();
    let lock = Arc::new(RwLock::new(0_u8));
    let writer = lock.write();
    let reader = {
        let lock = Arc::clone(&lock);
        thread::spawn(move || *lock.read())
    };
    let mut writer = writer;
    *writer = 9;
    drop(writer);
    assert_eq!(reader.join().assured(PARTICIPANT_JOINS), 9);
}
