//! A condition variable with `parking_lot`'s interface for a Shuttle build.
//!
//! Shuttle's `parking_lot` locks are modeled, but it has no condition variable for them, and its
//! standard-library condition variable waits on a standard-library guard. This one waits on the
//! modeled `parking_lot` guard: a waiter records the generation it waits in before it releases the
//! caller's lock, and every notification starts a new generation, so a notification between the
//! release and the wait is never lost. Every step is one of Shuttle's modeled lock and condition
//! operations, so the scheduler sees the whole protocol.

use meticulous::OptionExt as _;
use shuttle_parking_lot::MutexGuard;

/// Blocks a thread until another notifies it, releasing the lock it holds while it waits.
#[derive(Debug, Default)]
pub struct Condvar {
    state: shuttle::sync::Mutex<Waiters>,
    notified: shuttle::sync::Condvar,
}

#[derive(Debug, Default)]
struct Waiters {
    /// Advanced by every notification, so a waiter can tell its own wait from a later one.
    generation: u64,
    /// Threads waiting in the current generation.
    waiting: usize,
}

impl Condvar {
    pub fn new() -> Self {
        Self::default()
    }

    /// Release `guard`'s lock, wait for a notification, and reacquire the lock before returning.
    /// May return without a notification, so a caller waits in a loop over its own condition.
    pub fn wait<T: ?Sized>(&self, guard: &mut MutexGuard<'_, T>) {
        let mut waiters = self.lock();
        let generation = waiters.generation;
        waiters.waiting = waiters
            .waiting
            .checked_add(1)
            .assured("a model runs far fewer than usize::MAX threads");
        MutexGuard::unlocked(guard, move || {
            while waiters.generation == generation {
                waiters = match self.notified.wait(waiters) {
                    Ok(waiters) => waiters,
                    Err(poisoned) => poisoned.into_inner(),
                };
            }
        });
    }

    /// Wake every waiting thread. Returns how many were waiting.
    pub fn notify_all(&self) -> usize {
        let mut waiters = self.lock();
        let woken = std::mem::take(&mut waiters.waiting);
        waiters.generation = waiters
            .generation
            .checked_add(1)
            .assured("a model notifies far fewer than u64::MAX times");
        self.notified.notify_all();
        woken
    }

    fn lock(&self) -> shuttle::sync::MutexGuard<'_, Waiters> {
        match self.state.lock() {
            Ok(waiters) => waiters,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
