//! Layer: engines and infrastructure.
//! - **Owns.** The single-atomic GCRA horizon and exact unit-owned charge rollback.
//! - **Depends on.** Governor's store trait and the selected atomic boundary.
//! - **Must not know.** Graphs, key retention, wall clocks or persistence.

use governor::{
    nanos::Nanos,
    state::{NotKeyed, StateStore},
};
use meticulous::OptionExt as _;
use nervix_primitives::sync::atomic::{AtomicU64, Ordering};

/// Zero is the actual horizon at the limiter's epoch, a fully replenished allowance.
#[derive(Debug)]
pub(crate) struct DirectCell(AtomicU64);

impl DirectCell {
    pub(crate) fn new(horizon: u64) -> Self {
        Self(AtomicU64::new(horizon))
    }

    pub(crate) fn horizon(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// The owning transaction excludes another writer until it commits or refunds.
    pub(crate) fn refund(&self, before: u64) {
        self.0.store(before, Ordering::Relaxed);
    }
}

impl StateStore for DirectCell {
    type Key = NotKeyed;

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "governor supplies one synchronous GCRA update; this call \
                                   neither awaits nor escapes its store guard")
    )]
    fn measure_and_replace<T, F, E>(&self, _key: &NotKeyed, f: F) -> Result<T, E>
    where
        F: Fn(Option<Nanos>) -> Result<(T, Nanos), E>,
    {
        let mut before = self.horizon();
        loop {
            let (result, after) = f(Some(before.into()))?;
            match self.0.compare_exchange_weak(
                before,
                after.as_u64(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(result),
                Err(current) => before = current,
            }
        }
    }
}

/// Governor sees a horizon relative to this admission, so its arithmetic stays within two bursts.
pub(crate) struct RelativeStore<'a> {
    pub(crate) cell: &'a DirectCell,
    pub(crate) now: u64,
}

impl StateStore for RelativeStore<'_> {
    type Key = NotKeyed;

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "governor supplies one synchronous GCRA update; this call \
                                   neither awaits nor escapes its store guard")
    )]
    fn measure_and_replace<T, F, E>(&self, key: &NotKeyed, f: F) -> Result<T, E>
    where
        F: Fn(Option<Nanos>) -> Result<(T, Nanos), E>,
    {
        self.cell.measure_and_replace(key, |stored| {
            let stored = stored
                .verified("DirectCell always supplies its current horizon")
                .as_u64();
            // A horizon before now is fully replenished, so its relative debt is clamped to zero.
            let relative = nervix_primitives::expect_lint!(
                nervix::clamped_arithmetic,
                "a horizon at or before this admission has zero remaining allowance debt",
                stored.saturating_sub(self.now)
            );
            let (result, next) = f(Some(relative.into()))?;
            let absolute = self
                .now
                .checked_add(next.as_u64())
                .verified("admission checks that its epoch offset plus a burst fits u64");
            Ok((result, absolute.into()))
        })
    }
}
