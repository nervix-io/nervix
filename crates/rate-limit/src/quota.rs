//! Layer: engines and infrastructure.
//! - **Owns.** Validated GCRA quotas and keyed retention bounds.
//! - **Depends on.** Governor, durations and typed reports.
//! - **Must not know.** Models, graph policy, stores or clocks.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use error_stack::Report;
use meticulous::OptionExt as _;
use thiserror::Error;

/// A refusal to construct an allowance or its bounded key store.
#[derive(Debug, Error, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(outcome, reason = "pure allowance configuration validation")
)]
pub enum QuotaError {
    #[error("rate count must be positive")]
    ZeroCount,
    #[error("rate period must be positive")]
    ZeroPeriod,
    #[error("rate count exceeds the number of nanoseconds in its period")]
    CountExceedsPeriod,
    #[error("burst must be between 1 and 4294967295")]
    BurstRange,
    #[error("cost must be between 1 and 4294967295")]
    CostRange,
    #[error("the allowance horizon exceeds governor's nanosecond range")]
    HorizonRange,
    #[error("key TTL must be positive and fit the nanosecond range")]
    TtlRange,
}

/// A validated quota. Rounding the cell interval up never admits faster than the requested rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quota {
    pub(crate) governor: governor::Quota,
    pub(crate) horizon: Duration,
}

impl Quota {
    pub fn new(count: u64, period: Duration, burst: u64) -> Result<Self, Report<QuotaError>> {
        if count == 0 {
            return Err(Report::new(QuotaError::ZeroCount));
        }
        if period.is_zero() {
            return Err(Report::new(QuotaError::ZeroPeriod));
        }
        if u128::from(count) > period.as_nanos() {
            return Err(Report::new(QuotaError::CountExceedsPeriod));
        }
        let burst = match u32::try_from(burst) {
            Ok(value) => NonZeroU32::new(value),
            Err(_) => None,
        }
        .ok_or_else(|| Report::new(QuotaError::BurstRange))?;
        let interval = period.as_nanos().div_ceil(u128::from(count));
        // GCRA may add one burst's weight while comparing an existing full horizon.
        let horizon = interval
            .checked_mul(u128::from(burst.get()))
            .ok_or_else(|| Report::new(QuotaError::HorizonRange))?;
        if horizon > u128::from(u64::MAX / 2) {
            return Err(Report::new(QuotaError::HorizonRange));
        }
        let interval =
            u64::try_from(interval).map_err(|_| Report::new(QuotaError::HorizonRange))?;
        let horizon = u64::try_from(horizon).map_err(|_| Report::new(QuotaError::HorizonRange))?;
        let governor = governor::Quota::with_period(Duration::from_nanos(interval))
            .verified(
                "positive period and count no greater than its nanoseconds give a positive \
                 interval",
            )
            .allow_burst(burst);
        Ok(Self {
            governor,
            horizon: Duration::from_nanos(horizon),
        })
    }

    pub fn cost(value: u64) -> Result<NonZeroU32, Report<QuotaError>> {
        let value = u32::try_from(value).map_err(|_| Report::new(QuotaError::CostRange))?;
        NonZeroU32::new(value).ok_or_else(|| Report::new(QuotaError::CostRange))
    }

    pub fn burst(self) -> NonZeroU32 {
        self.governor.burst_size()
    }

    pub fn replenish_interval(self) -> Duration {
        self.governor.replenish_interval()
    }
}

/// An explicit shard count and global capacity, including a legitimate zero-capacity key budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyedOptions {
    pub(crate) ttl: Duration,
    pub(crate) max_keys: usize,
    pub(crate) shards: NonZeroUsize,
}

impl KeyedOptions {
    pub fn new(
        ttl: Duration,
        max_keys: usize,
        shards: NonZeroUsize,
    ) -> Result<Self, Report<QuotaError>> {
        if ttl.is_zero() || ttl.as_nanos() > u128::from(u64::MAX) {
            return Err(Report::new(QuotaError::TtlRange));
        }
        Ok(Self {
            ttl,
            max_keys,
            shards,
        })
    }
}
