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
use nervix_primitives::task::AbortOnDropHandle;

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

/// An attempt whose work waited for capacity and goes on on a task of its own.
///
/// The executor hands a freed place, or freed room, to the waiter first in line, and only that
/// waiter's own progress uses it. Work on a task of its own makes that progress whatever its owner
/// awaits meanwhile, so nothing the owner awaits beside it can hold back capacity handed to it.
/// Dropping the attempt ends the work, which gives up its place in line.
pub(super) struct DetachedAttempt<T> {
    work: AbortOnDropHandle<T>,
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

    /// The work's outcome once it ends, for an attempt that its task no longer runs: the work goes
    /// on wherever this future is polled.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one attempt finishes the lifecycle work its processor task started"
        )
    )]
    pub(super) async fn outcome(self) -> T {
        match self.work {
            AttemptWork::Running(running) => running.await,
            AttemptWork::Ended(outcome) => outcome,
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

impl<T: Send + 'static> CapacityAttempt<T> {
    /// Let the work go on on a task of its own, for an attempt whose owner answers its commands
    /// beside it while it waits.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one attempt that waits for capacity moves its lifecycle work to a task of \
                      its own once"
        )
    )]
    pub(super) fn detach(self) -> DetachedAttempt<T> {
        let work = nervix_primitives::task::spawn(self.outcome());
        DetachedAttempt {
            work: AbortOnDropHandle::new(work),
        }
    }
}

impl<T> DetachedAttempt<T> {
    /// The work's outcome once it ends, or nothing when the runtime ended its task as it shut down.
    ///
    /// A panic in the work resumes here, as it would have in the owner had the owner run the work
    /// itself.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one attempt waits for the lifecycle work it moved to a task of its own"
        )
    )]
    pub(super) async fn ended(&mut self) -> Option<T> {
        match (&mut self.work).await {
            Ok(outcome) => Some(outcome),
            Err(ended) => match ended.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(_cancelled) => None,
            },
        }
    }
}
