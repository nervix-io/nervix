//! The context a finding correlates with the detector's identities: where each tracked lock was
//! constructed, which acquisition each blocked thread waits in, and each thread's name.
//!
//! The detector names threads and locks by numbers. The adapters of `sync::blocking` record here,
//! under those numbers, the construction site of every lock they create and the site of every
//! acquisition that has to wait, so a cycle can be described in source terms while its threads are
//! still blocked. An acquisition that succeeds at once records nothing: only an acquisition that
//! waits can be part of a cycle.
//!
//! The maps are concurrent maps, whose shard locks are held only inside one map operation and never
//! across a tracked acquisition, so recording here can never join a cycle it describes, and reading
//! here from the findings thread never waits for a blocked thread.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the diagnostic registry is a primitive mechanism of the deloxide backend; \
                  consumer acquisitions are checked at their resolved calls"
    )
)]

use std::{cell::Cell, num::NonZeroU64, panic::Location, sync::LazyLock};

use meticulous::{OptionExt as _, ResultExt as _};

use super::{
    Access, BlockedAttempt, BlockedThread, BoundedText, LockKind, LockSite, SourceSite,
    TrackedLockId, TrackedThreadId, WaitedLock,
};
use crate::collections::DashMap;

/// Every record of this process. Process-wide like the detector it describes.
static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::default);

std::thread_local! {
    /// The calling thread's number, learned the first time the thread records an acquisition.
    static CURRENT_THREAD: Cell<Option<TrackedThreadId>> = const { Cell::new(None) };
}

#[derive(Default)]
pub(crate) struct Registry {
    locks: DashMap<TrackedLockId, LockRecord>,
    attempts: DashMap<TrackedThreadId, AttemptRecord>,
    names: DashMap<TrackedThreadId, Option<String>>,
}

#[derive(Clone, Copy)]
struct LockRecord {
    kind: LockKind,
    constructed_at: &'static Location<'static>,
}

#[derive(Clone, Copy)]
struct AttemptRecord {
    lock: TrackedLockId,
    access: Access,
    at: &'static Location<'static>,
}

/// One recorded acquisition that is waiting. Dropping it, once the lock is held or the waiting
/// thread unwinds, removes the record.
pub(crate) struct WaitingAttempt {
    thread: TrackedThreadId,
}

impl Drop for WaitingAttempt {
    fn drop(&mut self) {
        REGISTRY.attempts.remove(&self.thread);
    }
}

impl Registry {
    pub(crate) fn global() -> &'static Self {
        &REGISTRY
    }

    /// Record that the lock Deloxide numbered `lock` is a `kind` constructed at `constructed_at`.
    pub(crate) fn constructed(
        &self,
        lock: usize,
        kind: LockKind,
        constructed_at: &'static Location<'static>,
    ) {
        self.locks.insert(
            lock_id(lock),
            LockRecord {
                kind,
                constructed_at,
            },
        );
    }

    /// Forget the lock Deloxide numbered `lock`, which is being dropped.
    pub(crate) fn dropped(&self, lock: usize) {
        self.locks.remove(&lock_id(lock));
    }

    /// Record that the calling thread waits for the lock Deloxide numbered `lock`, asking for it
    /// with `access` at `at`, until the returned record is dropped.
    pub(crate) fn waiting(
        &self,
        lock: usize,
        access: Access,
        at: &'static Location<'static>,
    ) -> WaitingAttempt {
        let thread = self.current_thread();
        self.attempts.insert(
            thread,
            AttemptRecord {
                lock: lock_id(lock),
                access,
                at,
            },
        );
        WaitingAttempt { thread }
    }

    /// The calling thread's number. Deloxide numbers a thread the first time it uses a tracked lock
    /// and exposes the number only as the creator of a lock, so the first call on a thread
    /// constructs and drops a probe lock to learn it, and records the thread's name with it.
    fn current_thread(&self) -> TrackedThreadId {
        if let Some(thread) = CURRENT_THREAD.get() {
            return thread;
        }
        let probe = deloxide::Mutex::new(());
        let thread = TrackedThreadId::new(nonzero(probe.creator_thread_id()));
        drop(probe);
        let name = std::thread::current().name().map(str::to_string);
        self.names.insert(thread, name);
        CURRENT_THREAD.set(Some(thread));
        thread
    }

    /// One thread of a reported cycle, with whatever context this registry holds for it. `waits_for`
    /// is the lock the detector reported the thread waiting for.
    pub(crate) fn blocked_thread(&self, thread: usize, waits_for: Option<usize>) -> BlockedThread {
        let thread = TrackedThreadId::new(nonzero(thread));
        let name = match self.names.get(&thread) {
            Some(name) => name.as_deref().map(BoundedText::new),
            None => None,
        };
        let Some(lock) = waits_for.map(lock_id) else {
            return BlockedThread {
                thread,
                name,
                waits_for: None,
                attempt: None,
            };
        };
        let site = match self.locks.get(&lock) {
            Some(record) => Some(LockSite {
                kind: record.kind,
                constructed_at: SourceSite::from_location(record.constructed_at),
            }),
            None => None,
        };
        // A record of an acquisition of another lock is not this wait's: it is not reported.
        let attempt = match self.attempts.get(&thread) {
            Some(record) if record.lock == lock => Some(BlockedAttempt {
                access: record.access,
                at: SourceSite::from_location(record.at),
            }),
            Some(_) | None => None,
        };
        BlockedThread {
            thread,
            name,
            waits_for: Some(WaitedLock { id: lock, site }),
            attempt,
        }
    }
}

fn lock_id(lock: usize) -> TrackedLockId {
    TrackedLockId::new(nonzero(lock))
}

/// A Deloxide thread or lock number, which its counters assign from one upwards.
fn nonzero(number: usize) -> NonZeroU64 {
    let number = u64::try_from(number).assured("supported targets address at most 64 bits");
    NonZeroU64::new(number).assured("Deloxide assigns thread and lock numbers from one upwards")
}
