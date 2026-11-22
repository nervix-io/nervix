//! One attempt at work a processor task runs alone while it makes progress and beside its commands
//! while it waits for capacity.
//!
//! Layer: data plane.
//!
//! - **Owns.** Stepping one piece of work, whose executor handle tells the attempt while it waits for
//!   a place in a full worker class or for room in a full memory class: until it ends or waits, or
//!   while it waits.
//! - **Depends on.** The bounded executor's capacity waits and boxed futures.
//! - **Must not know.** What the work builds, or what its task answers while the work waits.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{future::Future, pin::Pin};

use nervix_execution::CapacityWaits;

/// One attempt at a piece of work that its task owns until the work ends.
///
/// The work runs on a future that owns everything it reads, submitted through an executor handle
/// that reports its waits to this attempt's [`CapacityWaits`], so that its task can tell work that
/// makes progress from work that waits for capacity, and answer its commands only while it waits.
pub(super) struct CapacityAttempt<T> {
    waits: CapacityWaits,
    work: AttemptWork<T>,
}

/// Where one attempt's work stands.
enum AttemptWork<T> {
    /// Still running, on a future that owns everything it reads, so that the task can answer its
    /// commands between polls of it.
    Running(Pin<Box<dyn Future<Output = T> + Send>>),
    /// Ended, with its outcome, until the task takes it.
    Ended(T),
}

/// What an attempt does after its task has run it.
pub(super) enum AttemptProgress {
    /// Its work ended, and the task takes its outcome.
    Ended,
    /// Its work waits for capacity, and the task answers its commands beside it.
    Waiting,
}

impl<T> CapacityAttempt<T> {
    /// An attempt at `work`, whose executor handle reports its waits to `waits`.
    pub(super) fn start(
        waits: CapacityWaits,
        work: impl Future<Output = T> + Send + 'static,
    ) -> Self {
        Self {
            waits,
            work: AttemptWork::Running(Box::pin(work)),
        }
    }

    /// An attempt that has nothing to run, its outcome already known.
    pub(super) fn ended(outcome: T) -> Self {
        Self {
            waits: CapacityWaits::new(),
            work: AttemptWork::Ended(outcome),
        }
    }

    /// Run the work until it has ended or waits for capacity.
    ///
    /// Its task answers no command meanwhile: work that makes progress keeps every command behind
    /// it, as work its task awaited inline did.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one attempt runs the lifecycle work its processor task started"
        )
    )]
    pub(super) async fn run_until_waiting(&mut self) -> AttemptProgress {
        let Self { waits, work } = self;
        let AttemptWork::Running(running) = work else {
            return AttemptProgress::Ended;
        };
        let ended = nervix_primitives::select! {
            biased;
            ended = running.as_mut() => Some(ended),
            () = waits.began() => None,
        };
        match ended {
            Some(ended) => {
                *work = AttemptWork::Ended(ended);
                AttemptProgress::Ended
            }
            None => AttemptProgress::Waiting,
        }
    }

    /// Run the work while it waits for capacity, and return once it no longer does: it took its
    /// place or its room and goes on, or it ended.
    ///
    /// The task answers its commands beside this future and may drop it between them: the work it
    /// drives stays in the attempt, and so does its place in line.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one attempt waits for the capacity its lifecycle work needs"
        )
    )]
    pub(super) async fn until_no_longer_waiting(&mut self) {
        let Self { waits, work } = self;
        let AttemptWork::Running(running) = work else {
            return;
        };
        let ended = nervix_primitives::select! {
            biased;
            ended = running.as_mut() => Some(ended),
            () = waits.ended() => None,
        };
        if let Some(ended) = ended {
            *work = AttemptWork::Ended(ended);
        }
    }

    /// The work's outcome, once it has ended.
    pub(super) fn into_outcome(self) -> Option<T> {
        match self.work {
            AttemptWork::Ended(outcome) => Some(outcome),
            AttemptWork::Running(_) => None,
        }
    }
}
