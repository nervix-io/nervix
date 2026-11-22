//! Telling the owner of some work whether that work waits for capacity.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The count of one owner's submissions that wait for a place in a full worker queue or
//!   for room in a memory class, and waking the owner when that count changes.
//! - **Depends on.** The primitive boundary's watch channel and shared ownership.
//! - **Must not know.** What the work computes, which classes it runs in, or what its owner does
//!   while it waits.

use std::{future::Future, pin::pin};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::{Arc, watch};

/// Tells the owner of some work whether any of it waits for capacity: a place in a worker class
/// whose wait queue is full, or room in a memory class that cannot grant its charge yet.
///
/// The owner submits the work through the executor handle [`crate::Executor::reporting_waits_to`]
/// returns, keeps this value, and reads or awaits it. A wait begins when a submission finds its
/// class full and has to wait, and ends when the submission takes its place or its room, or when
/// its future is dropped. A job that already has its place and waits for a worker is not waiting
/// for capacity: every job passes through that, however idle the node is. Cloning shares one
/// count.
#[derive(Clone, Debug)]
pub struct CapacityWaits {
    waiting: Arc<watch::Sender<usize>>,
}

impl Default for CapacityWaits {
    fn default() -> Self {
        Self::new()
    }
}

impl CapacityWaits {
    /// A count no work waits on yet.
    pub fn new() -> Self {
        Self {
            waiting: Arc::new(watch::Sender::new(0)),
        }
    }

    /// Whether some of the owner's work waits for capacity now.
    pub fn is_waiting(&self) -> bool {
        *self.waiting.borrow() > 0
    }

    /// Return once some of the owner's work waits for capacity, at once when some already does.
    pub async fn began(&self) {
        self.reached(|waiting| waiting > 0).await;
    }

    /// Return once none of the owner's work waits for capacity, at once when none does.
    pub async fn ended(&self) {
        self.reached(|waiting| waiting == 0).await;
    }

    async fn reached(&self, condition: impl Fn(usize) -> bool) {
        // Subscribing marks the current count as seen before the condition reads it, so a wait
        // that begins or ends in between is a newer count this receiver still wakes for.
        let mut changes = self.waiting.subscribe();
        let reached = changes
            .wait_for(|waiting| condition(*waiting))
            .await
            .assured("this value holds the count's only sender for as long as it waits on it");
        drop(reached);
    }

    /// Await `acquisition`, counting it as a wait for capacity from the first poll that finds it
    /// not ready until it is ready or dropped. One that is ready when first polled never counts.
    pub(crate) async fn report<F: Future>(&self, acquisition: F) -> F::Output {
        let mut acquisition = pin!(acquisition);
        let mut wait = None::<CapacityWait<'_>>;
        std::future::poll_fn(|context| {
            let polled = acquisition.as_mut().poll(context);
            if polled.is_pending() && wait.is_none() {
                wait = Some(self.begin_wait());
            }
            polled
        })
        .await
    }

    /// Count one submission's wait from now until the returned wait is dropped.
    pub(crate) fn begin_wait(&self) -> CapacityWait<'_> {
        CapacityWait::begin(self)
    }
}

/// One submission's wait for capacity, counted from when it begins until it is dropped.
pub(crate) struct CapacityWait<'a> {
    waits: &'a CapacityWaits,
}

impl<'a> CapacityWait<'a> {
    fn begin(waits: &'a CapacityWaits) -> Self {
        waits.waiting.send_modify(|waiting| {
            *waiting = waiting.checked_add(1).assured(
                "each waiting submission is a future its owner holds, and no owner holds \
                 usize::MAX of them",
            );
        });
        Self { waits }
    }
}

impl Drop for CapacityWait<'_> {
    fn drop(&mut self) {
        self.waits.waiting.send_modify(|waiting| {
            *waiting = waiting
                .checked_sub(1)
                .assured("this wait added itself to the count when it began");
        });
    }
}

// The ordinary tests build executors outside any model, so a Loom build, which runs only the
// cancellation models, and a Shuttle build, whose check lives with the executor's other checks,
// leave them out. Their held jobs block a worker thread, which Turmoil's single-threaded simulated
// CPU strategy would never return to the test, so a Turmoil build leaves them out too.
#[cfg(all(test, not(any(feature = "loom", feature = "shuttle", feature = "turmoil"))))]
#[path = "capacity_waits_tests.rs"]
mod tests;
