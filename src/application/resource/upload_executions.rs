//! Node-local ownership of resource upload installations and their waiting retries.
//!
//! Layer: control plane.
//! - **Owns.** One execution mutex per live upload key and retirement after its final attempt.
//! - **Depends on.** Typed upload keys and the selected map, shared ownership and async mutex.
//! - **Must not know.** Archives, durable upload outcomes, consensus or client transports.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;

use ahash::RandomState;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::ResourceUploadKey;
use nervix_primitives::{
    collections::{DashMap, dash_map::Entry},
    sync::{Arc, Mutex as AsyncMutex, OwnedMutexGuard, StdArc},
};

/// Every attempt registers before waiting; registration and final retirement use one entry guard.
#[derive(Clone)]
pub(in crate::application) struct ResourceUploadExecutions {
    entries: Arc<DashMap<ResourceUploadKey, ExecutionEntry, RandomState>>,
}

struct ExecutionEntry {
    // The owned async mutex API requires the standard shared pointer, confined to this boundary.
    mutex: StdArc<AsyncMutex<()>>,
    attempts: NonZeroUsize,
}

impl ExecutionEntry {
    fn new() -> Self {
        Self {
            mutex: StdArc::new(AsyncMutex::new(())),
            attempts: NonZeroUsize::MIN,
        }
    }

    fn register_attempt(&mut self) {
        self.attempts = self
            .attempts
            .checked_add(1)
            .assured("the process cannot hold usize::MAX simultaneous upload attempts");
    }
}

impl Default for ResourceUploadExecutions {
    fn default() -> Self {
        Self {
            entries: Arc::new(DashMap::with_hasher(RandomState::new())),
        }
    }
}

impl ResourceUploadExecutions {
    pub(super) async fn lock(&self, key: &ResourceUploadKey) -> ResourceUploadExecution {
        let attempt = self.register(key);
        attempt.lock().await
    }

    pub(super) fn try_lock(&self, key: &ResourceUploadKey) -> Option<ResourceUploadExecution> {
        let guard = match self.entries.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let execution = entry.get_mut();
                let guard = match execution.mutex.clone().try_lock_owned() {
                    Ok(guard) => guard,
                    Err(_) => return None,
                };
                execution.register_attempt();
                guard
            }
            Entry::Vacant(entry) => {
                let execution = ExecutionEntry::new();
                let guard = execution
                    .mutex
                    .clone()
                    .try_lock_owned()
                    .assured("a fresh mutex is private until this entry guard publishes it");
                drop(entry.insert(execution));
                guard
            }
        };
        Some(ResourceUploadExecution {
            _guard: guard,
            _lease: self.lease(key),
        })
    }

    fn register(&self, key: &ResourceUploadKey) -> PendingExecution {
        let mutex = match self.entries.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let execution = entry.get_mut();
                execution.register_attempt();
                execution.mutex.clone()
            }
            Entry::Vacant(entry) => {
                let execution = ExecutionEntry::new();
                let mutex = execution.mutex.clone();
                drop(entry.insert(execution));
                mutex
            }
        };
        PendingExecution {
            mutex,
            lease: self.lease(key),
        }
    }

    fn lease(&self, key: &ResourceUploadKey) -> ExecutionLease {
        ExecutionLease {
            executions: self.clone(),
            key: key.clone(),
        }
    }

    fn release(&self, key: &ResourceUploadKey) {
        let Entry::Occupied(mut entry) = self.entries.entry(key.clone()) else {
            unreachable!("a live attempt keeps its upload execution registered");
        };
        let attempts = entry.get().attempts.get();
        if attempts == 1 {
            entry.remove();
        } else {
            let remaining = attempts
                .checked_sub(1)
                .verified("the entry has more than one live attempt");
            entry.get_mut().attempts =
                NonZeroUsize::new(remaining).assured("another upload attempt remains registered");
        }
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, key: &ResourceUploadKey) -> bool {
        self.entries.contains_key(key)
    }
}

struct ExecutionLease {
    executions: ResourceUploadExecutions,
    key: ResourceUploadKey,
}

impl Drop for ExecutionLease {
    fn drop(&mut self) {
        self.executions.release(&self.key);
    }
}

struct PendingExecution {
    mutex: StdArc<AsyncMutex<()>>,
    lease: ExecutionLease,
}

impl PendingExecution {
    async fn lock(self) -> ResourceUploadExecution {
        let Self { mutex, lease } = self;
        let guard = mutex.lock_owned().await;
        ResourceUploadExecution {
            _guard: guard,
            _lease: lease,
        }
    }
}

/// Unlock before releasing the attempt: a queued retry must retain the same registered mutex.
pub(super) struct ResourceUploadExecution {
    _guard: OwnedMutexGuard<()>,
    _lease: ExecutionLease,
}
