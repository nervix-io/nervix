//! Jobs whose charge grows while they run, and the rule that none of them waits for room while it
//! holds any.
//!
//! A synchronous job cannot wait for memory: it runs on a worker, and a worker blocked on a charge
//! held by work queued behind it would never be released. Waiting while holding part of a class is
//! no better on the async side, because a queued charge takes every byte its class frees until it
//! is satisfied, so it can stall behind the very bytes it holds. A job that outgrows its charge
//! therefore gives everything back and reports what it needs; its owner waits for that charge
//! while holding nothing, and runs the job again under it.

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;

use crate::{
    Cancellation, CpuClass, ExecutionFailure, Executor, MemoryClass, Reservation, Shortfall,
};

/// How one attempt of a job run through [`Executor::run_cpu_growing`] ended.
#[derive(Debug)]
pub enum Attempt<T, I> {
    /// The job ran to its end and produced its outcome, which may itself be a failure of the job.
    Finished(T),
    /// The job's class had no room for the growth it needed. The job hands back its input, which
    /// the attempt left as it found it, and the whole charge it needs, at least, to get further.
    Short { input: I, needed: u64 },
}

impl<T, C, I> Attempt<Result<T, Report<C>>, I> {
    /// The attempt a job's outcome makes: short of room when a growth refusal beneath its failure
    /// says what the job needed (a [`Shortfall`]), and finished with the outcome otherwise.
    pub fn of(outcome: Result<T, Report<C>>, input: I) -> Self {
        let Err(failure) = &outcome else {
            return Self::Finished(outcome);
        };
        match Shortfall::of(failure) {
            Some(needed) => Self::Short { input, needed },
            None => Self::Finished(outcome),
        }
    }
}

/// What an attempt hands back across the worker boundary: its outcome, and the job itself so the
/// next attempt can run it again.
struct RanAttempt<T, I, J> {
    attempt: Attempt<T, I>,
    job: J,
}

impl Executor {
    /// Run `operation` within an allowance of `class` ([`Self::reserve_allowance`]), starting from
    /// a share of `initial` bytes.
    ///
    /// An operation that runs short of its share gives everything back, and runs again within a
    /// share of everything it needed and at least twice its last share, which it waits for while
    /// holding nothing. So the operation never waits behind its own charges, a full class only
    /// holds it back, and an operation that grows step by step runs a few times rather than once
    /// per step. It returns the operation's own outcome, or the refusal of a share larger than the
    /// whole class.
    pub async fn within_allowance<T, C, F, Fut>(
        &self,
        class: MemoryClass,
        initial: u64,
        mut operation: F,
    ) -> Result<Result<T, Report<C>>, Report<crate::AdmissionError>>
    where
        F: FnMut(Executor) -> Fut,
        Fut: std::future::Future<Output = Result<T, Report<C>>>,
    {
        let mut share = initial;
        loop {
            nervix_primitives::task::consume_budget().await;
            let allowance = self.reserve_allowance(class, share).await?;
            let watch = allowance.clone();
            let outcome = operation(allowance).await;
            // A charge the operation needed and its share had no room for means the attempt ran
            // short, however the operation reported it: a degraded answer is not the operation's
            // answer. Only a shortfall of the allowance's own class is one a larger share can end.
            let needed = match watch.allowance_shortfall(class) {
                Some(needed) => needed,
                None => {
                    let Err(failure) = &outcome else {
                        return Ok(outcome);
                    };
                    let Some(shortfall) = Shortfall::beneath(failure) else {
                        return Ok(outcome);
                    };
                    if shortfall.class != class {
                        return Ok(outcome);
                    }
                    shortfall.needed
                }
            };
            drop(outcome);
            drop(watch);
            share = next_charge(share, needed, self.budget_capacity(class));
        }
    }

    /// Run a CPU job whose charge grows while it runs, without ever waiting for room while holding
    /// any.
    ///
    /// The first attempt runs under a charge of `initial` bytes of `memory`. When the job's class
    /// has no room for the growth it needs, the job returns [`Attempt::Short`] with its input and
    /// the charge it needs: everything the attempt held goes back to the class, the executor waits
    /// for the larger charge while holding nothing, and runs the job again under it. A class that
    /// is full is backpressure for the job, never its failure.
    ///
    /// It fails only for a charge larger than the whole class, which no wait could grant, and once
    /// the executor admits no more work. The caller bounds the waits by dropping the future.
    pub async fn run_cpu_growing<I, T, J>(
        &self,
        class: CpuClass,
        memory: MemoryClass,
        initial: u64,
        input: I,
        job: J,
    ) -> Result<T, Report<ExecutionFailure>>
    where
        I: Send + 'static,
        T: Send + 'static,
        J: FnMut(I, Reservation, &Cancellation) -> Attempt<T, I> + Send + 'static,
    {
        let mut charge = initial;
        let mut input = input;
        let mut job = job;
        loop {
            nervix_primitives::task::consume_budget().await;
            let reservation = self
                .reserve(memory, charge)
                .await
                .change_context(ExecutionFailure::Admission)?;
            let ran = self
                .run_cpu(class, reservation, move |reservation, cancellation| {
                    let attempt = job(input, reservation, cancellation);
                    RanAttempt { attempt, job }
                })
                .await
                .change_context(ExecutionFailure::Execution)?;
            match ran.attempt {
                Attempt::Finished(outcome) => return Ok(outcome),
                Attempt::Short {
                    input: returned,
                    needed,
                } => {
                    charge = next_charge(charge, needed, self.budget_capacity(memory));
                    input = returned;
                    job = ran.job;
                }
            }
        }
    }
}

/// The charge an attempt that ran short under `current` bytes runs again under: everything it was
/// found to `need`, and at least twice what it had, up to the `capacity` of the budget it draws
/// from.
///
/// Doubling bounds how often an operation that grows one step at a time runs again: a charge that
/// covered only the step where the attempt ran short would end the next attempt one step later,
/// so the operation would run once per step. A job that reports a need no larger than what it
/// held would meet the same shortfall again, so each attempt asks for more than the last; a need
/// beyond the capacity is asked for as it is, and refused as larger than the budget.
fn next_charge(current: u64, needed: u64, capacity: u64) -> u64 {
    let doubled = current.saturating_mul(2).min(capacity);
    let more = current
        .checked_add(1)
        .assured("a granted charge fits the u32 class capacity, so one more byte fits u64");
    needed.max(doubled).max(more)
}
