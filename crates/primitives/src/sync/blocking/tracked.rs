//! The thread-blocking locks and condition variable of a `deloxide` build: `parking_lot`'s
//! interface over Deloxide's tracked locks.
//!
//! Every acquisition goes through Deloxide's lock, which updates the process-wide wait-for graph
//! when it has to wait, and every acquisition that has to wait records where it waits in the
//! detector's registry, as every constructor records where the lock was made, so a finding can name
//! both. An acquisition first tries the lock and records nothing when it is free at once: only an
//! acquisition that waits can be part of a cycle.
//!
//! The surface is the part of `parking_lot`'s that keeps its meaning over Deloxide. A lock is never
//! poisoned, and `Debug` never waits: it tries the lock and prints `<locked>` when it is held, as
//! `parking_lot` does. Deloxide's locks hold sized values, construct at run time and have no timed,
//! upgradable, mapped or reentrant acquisition, so those operations do not exist here and code that
//! uses them fails to compile in this build rather than running untracked.
//!
//! The condition variable has the interface the Shuttle adapter keeps, `wait` and `notify_all`,
//! with the count of woken waiters `parking_lot` returns. Deloxide's condition variable returns no
//! count, so this one keeps its own: a waiter records the generation it waits in under the
//! condition variable's state lock before it releases the caller's lock, and every notification
//! starts a new generation and counts the waiters of the one it ends. Each of them returns from its
//! wait, so the count is exactly the waiters that notification woke.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the tracked lock adapters are the deloxide backend's implementation; consumer \
                  acquisitions are checked at their resolved calls"
    )
)]

use std::{
    fmt,
    ops::{Deref, DerefMut},
    panic::Location,
};

use meticulous::OptionExt as _;

use crate::deadlock::{Access, LockKind, detector::require_installed, registry::Registry};

/// Keeps a lock's construction site in the registry for as long as the lock lives.
struct Registered {
    lock: usize,
}

impl Registered {
    fn new(lock: usize, kind: LockKind, at: &'static Location<'static>) -> Self {
        Registry::global().constructed(lock, kind, at);
        Self { lock }
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        Registry::global().dropped(self.lock);
    }
}

/// A mutual-exclusion lock whose acquisitions the deadlock detector tracks.
///
/// It runs only once the process has installed its detector:
///
/// ```
/// let _detector = nervix_primitives::deadlock::install(|_finding| {});
/// let mutex = nervix_primitives::sync::blocking::Mutex::new(1_u8);
/// *mutex.lock() += 1;
/// assert_eq!(mutex.into_inner(), 2);
/// ```
///
/// An acquisition the detector cannot track, such as a timed one, does not exist in this build:
///
/// ```compile_fail,E0599
/// let mutex = nervix_primitives::sync::blocking::Mutex::new(1_u8);
/// let held = mutex.try_lock_for(std::time::Duration::from_secs(1));
/// ```
pub struct Mutex<T> {
    inner: deloxide::Mutex<T>,
    _registered: Registered,
}

impl<T> Mutex<T> {
    #[track_caller]
    pub fn new(value: T) -> Self {
        Self::with_kind(value, LockKind::Mutex, Location::caller())
    }

    fn with_kind(value: T, kind: LockKind, at: &'static Location<'static>) -> Self {
        require_installed();
        let inner = deloxide::Mutex::new(value);
        let registered = Registered::new(inner.id(), kind, at);
        Self {
            inner,
            _registered: registered,
        }
    }

    /// Acquire the lock, waiting for it if another thread holds it.
    #[track_caller]
    pub fn lock(&self) -> MutexGuard<'_, T> {
        MutexGuard {
            mutex: self,
            inner: Some(self.acquire(Location::caller())),
        }
    }

    /// Acquire the lock if no thread holds it.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        let inner = self.inner.try_lock()?;
        Some(MutexGuard {
            mutex: self,
            inner: Some(inner),
        })
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn into_inner(self) -> T {
        let Self { inner, _registered } = self;
        inner.into_inner()
    }

    /// Deloxide's guard, recording where the caller waits when the lock is held.
    fn acquire(&self, at: &'static Location<'static>) -> deloxide::MutexGuard<'_, T> {
        if let Some(guard) = self.inner.try_lock() {
            return guard;
        }
        let waiting = Registry::global().waiting(self.inner.id(), Access::Exclusive, at);
        let guard = self.inner.lock();
        drop(waiting);
        guard
    }
}

impl<T: Default> Default for Mutex<T> {
    #[track_caller]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    #[track_caller]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.try_lock() {
            Some(guard) => f.debug_struct("Mutex").field("data", &&*guard).finish(),
            None => f.debug_struct("Mutex").field("data", &Locked).finish(),
        }
    }
}

/// Held access to a [`Mutex`]'s value; dropping it releases the lock.
pub struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
    /// Absent only while a condition variable waits with the lock released, which no caller of
    /// the guard can observe.
    inner: Option<deloxide::MutexGuard<'a, T>>,
}

impl<T> MutexGuard<'_, T> {
    /// Release the lock while `released` runs, then acquire it again, recording `at` as where the
    /// caller waits if it has to.
    fn unlocked<Released>(&mut self, at: &'static Location<'static>, released: Released)
    where
        Released: FnOnce(),
    {
        drop(self.inner.take());
        released();
        self.inner = Some(self.mutex.acquire(at));
    }
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.inner
            .as_ref()
            .assured("a guard holds its lock outside a condition variable's wait")
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.inner
            .as_mut()
            .assured("a guard holds its lock outside a condition variable's wait")
    }
}

impl<T: fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

/// A reader-writer lock whose acquisitions the deadlock detector tracks.
///
/// An upgradable read, which the detector cannot track, does not exist in this build:
///
/// ```compile_fail,E0599
/// let lock = nervix_primitives::sync::blocking::RwLock::new(1_u8);
/// let reading = lock.upgradable_read();
/// ```
pub struct RwLock<T> {
    inner: deloxide::RwLock<T>,
    _registered: Registered,
}

impl<T> RwLock<T> {
    #[track_caller]
    pub fn new(value: T) -> Self {
        require_installed();
        let inner = deloxide::RwLock::new(value);
        let registered = Registered::new(inner.id(), LockKind::RwLock, Location::caller());
        Self {
            inner,
            _registered: registered,
        }
    }

    /// Acquire shared access, waiting while a writer holds the lock.
    #[track_caller]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        if let Some(inner) = self.inner.try_read() {
            return RwLockReadGuard { inner };
        }
        let waiting =
            Registry::global().waiting(self.inner.id(), Access::Shared, Location::caller());
        let inner = self.inner.read();
        drop(waiting);
        RwLockReadGuard { inner }
    }

    /// Acquire exclusive access, waiting while any thread holds the lock.
    #[track_caller]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        if let Some(inner) = self.inner.try_write() {
            return RwLockWriteGuard { inner };
        }
        let waiting =
            Registry::global().waiting(self.inner.id(), Access::Exclusive, Location::caller());
        let inner = self.inner.write();
        drop(waiting);
        RwLockWriteGuard { inner }
    }

    /// Acquire shared access if no writer holds the lock.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let inner = self.inner.try_read()?;
        Some(RwLockReadGuard { inner })
    }

    /// Acquire exclusive access if no thread holds the lock.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        let inner = self.inner.try_write()?;
        Some(RwLockWriteGuard { inner })
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn into_inner(self) -> T {
        let Self { inner, _registered } = self;
        inner.into_inner()
    }
}

impl<T: Default> Default for RwLock<T> {
    #[track_caller]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for RwLock<T> {
    #[track_caller]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.try_read() {
            Some(guard) => f.debug_struct("RwLock").field("data", &&*guard).finish(),
            None => f.debug_struct("RwLock").field("data", &Locked).finish(),
        }
    }
}

/// Held shared access to a [`RwLock`]'s value; dropping it releases the access.
pub struct RwLockReadGuard<'a, T> {
    inner: deloxide::RwLockReadGuard<'a, T>,
}

impl<T> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: fmt::Display> fmt::Display for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

/// Held exclusive access to a [`RwLock`]'s value; dropping it releases the lock.
pub struct RwLockWriteGuard<'a, T> {
    inner: deloxide::RwLockWriteGuard<'a, T>,
}

impl<T> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: fmt::Debug> fmt::Debug for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: fmt::Display> fmt::Display for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

/// Blocks a thread until another notifies it, releasing the lock it holds while it waits.
///
/// A notification wakes every waiter and returns how many there were:
///
/// ```
/// let _detector = nervix_primitives::deadlock::install(|_finding| {});
/// assert_eq!(nervix_primitives::sync::blocking::Condvar::new().notify_all(), 0);
/// ```
///
/// Waking a single waiter is not part of the surface every mode keeps:
///
/// ```compile_fail,E0599
/// nervix_primitives::sync::blocking::Condvar::new().notify_one();
/// ```
pub struct Condvar {
    state: Mutex<Waiters>,
    notified: deloxide::Condvar,
}

#[derive(Default)]
struct Waiters {
    /// Advanced by every notification, so a waiter can tell its own wait from a later one.
    generation: u64,
    /// Threads waiting in the current generation.
    waiting: usize,
}

impl Condvar {
    #[track_caller]
    pub fn new() -> Self {
        Self {
            state: Mutex::with_kind(
                Waiters::default(),
                LockKind::CondvarState,
                Location::caller(),
            ),
            notified: deloxide::Condvar::new(),
        }
    }

    /// Release `guard`'s lock, wait for a notification, and acquire the lock again before
    /// returning. May return without a notification, so a caller waits in a loop over its own
    /// condition.
    #[track_caller]
    pub fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
        let at = Location::caller();
        let mut waiters = self.state.acquire(at);
        let generation = waiters.generation;
        waiters.waiting = waiters
            .waiting
            .checked_add(1)
            .assured("a process runs far fewer than usize::MAX threads");
        guard.unlocked(at, move || {
            while waiters.generation == generation {
                self.notified.wait(&mut waiters);
            }
        });
    }

    /// Wake every waiting thread. Returns how many were waiting.
    #[track_caller]
    pub fn notify_all(&self) -> usize {
        let mut waiters = self.state.acquire(Location::caller());
        let woken = std::mem::take(&mut waiters.waiting);
        waiters.generation = waiters
            .generation
            .checked_add(1)
            .assured("a process notifies far fewer than u64::MAX times");
        self.notified.notify_all();
        woken
    }
}

impl Default for Condvar {
    #[track_caller]
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Condvar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condvar").finish_non_exhaustive()
    }
}

/// What `Debug` prints for the value of a lock another thread holds.
struct Locked;

impl fmt::Debug for Locked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<locked>")
    }
}
