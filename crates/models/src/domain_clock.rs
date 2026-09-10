//! Validated domain-clock values and their pure arithmetic.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** Positive clock rates and periods, committed clock mappings and authorities,
//!   fenced progress, tick boundaries and their checked projection arithmetic.
//! - **Depends on.** Timestamp and serialization primitives.
//! - **Must not know.** Runtime tasks, consensus, notifications, sleeps or transport behavior.

use std::{fmt, num::NonZeroU64, str::FromStr, time::Duration};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use rkyv::{
    Archive, Archived, Deserialize as RkyvDeserialize, Place, Serialize as RkyvSerialize,
    rancor::{Fallible, Source},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ClusterNodeIdentity, Timestamp};

mod admission;
pub use admission::DomainAdmissionWindow;

#[derive(Debug, Error)]
pub enum DomainClockError {
    #[error("time rate '{value}' must be a positive finite number")]
    InvalidTimeRate { value: String },
    #[error("domain clock period '{value}' must be positive and fit in 64-bit nanoseconds")]
    InvalidPeriod { value: String },
    #[error("domain clock tick ids start at one")]
    InvalidTickId,
    #[error("domain clock tick advancement exceeds the supported tick-id range")]
    TickIdOverflow,
    #[error("domain clock {operation} exceeds the supported duration range")]
    DurationOverflow { operation: &'static str },
    #[error("domain clock {operation} leaves the signed Unix-nanosecond range")]
    TimestampOverflow { operation: &'static str },
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DomainClockAuthorityRevision(u64);

impl DomainClockAuthorityRevision {
    pub const INITIAL: Self = Self(0);

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum DomainClockAuthority {
    Unassigned {
        revision: DomainClockAuthorityRevision,
    },
    Assigned {
        revision: DomainClockAuthorityRevision,
        owner: ClusterNodeIdentity,
    },
}

impl DomainClockAuthority {
    pub const fn initial() -> Self {
        Self::Unassigned {
            revision: DomainClockAuthorityRevision::INITIAL,
        }
    }

    pub const fn unassigned(revision: DomainClockAuthorityRevision) -> Self {
        Self::Unassigned { revision }
    }

    pub const fn assigned(
        revision: DomainClockAuthorityRevision,
        owner: ClusterNodeIdentity,
    ) -> Self {
        Self::Assigned { revision, owner }
    }

    pub const fn revision(&self) -> DomainClockAuthorityRevision {
        match self {
            Self::Unassigned { revision } | Self::Assigned { revision, .. } => *revision,
        }
    }

    pub const fn owner(&self) -> Option<&ClusterNodeIdentity> {
        match self {
            Self::Unassigned { .. } => None,
            Self::Assigned { owner, .. } => Some(owner),
        }
    }

    pub fn checked_reassign(&self, owner: Option<ClusterNodeIdentity>) -> Option<Self> {
        let revision = self.revision().checked_next()?;
        match owner {
            Some(owner) => Some(Self::Assigned { revision, owner }),
            None => Some(Self::Unassigned { revision }),
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(try_from = "f64", into = "f64")]
pub struct DomainTimeRate(#[rkyv(with = PositiveFiniteRate)] f64);

impl Eq for DomainTimeRate {}

impl DomainTimeRate {
    pub const ONE: Self = Self(1.0);

    pub const fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for DomainTimeRate {
    type Error = DomainClockError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !value.is_finite() || value <= 0.0 {
            return Err(DomainClockError::InvalidTimeRate {
                value: value.to_string(),
            });
        }
        Ok(Self(value))
    }
}

impl From<DomainTimeRate> for f64 {
    fn from(value: DomainTimeRate) -> Self {
        value.get()
    }
}

impl FromStr for DomainTimeRate {
    type Err = DomainClockError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = value
            .parse::<f64>()
            .map_err(|_| DomainClockError::InvalidTimeRate {
                value: value.to_string(),
            })?;
        Self::try_from(parsed).map_err(|_| DomainClockError::InvalidTimeRate {
            value: value.to_string(),
        })
    }
}

impl fmt::Display for DomainTimeRate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

struct PositiveFiniteRate;

impl ArchiveWith<f64> for PositiveFiniteRate {
    type Archived = Archived<f64>;
    type Resolver = <f64 as Archive>::Resolver;

    fn resolve_with(field: &f64, resolver: Self::Resolver, out: Place<Self::Archived>) {
        field.resolve(resolver, out);
    }
}

impl<S> SerializeWith<f64, S> for PositiveFiniteRate
where
    S: Fallible + ?Sized,
    f64: RkyvSerialize<S>,
{
    fn serialize_with(field: &f64, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        RkyvSerialize::serialize(field, serializer)
    }
}

impl<D> DeserializeWith<Archived<f64>, f64, D> for PositiveFiniteRate
where
    D: Fallible + ?Sized,
    D::Error: Source,
    Archived<f64>: RkyvDeserialize<f64, D>,
{
    fn deserialize_with(field: &Archived<f64>, deserializer: &mut D) -> Result<f64, D::Error> {
        let value = field.deserialize(deserializer)?;
        DomainTimeRate::try_from(value).map_err(D::Error::new)?;
        Ok(value)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DomainClockPeriod(NonZeroU64);

impl DomainClockPeriod {
    pub const fn as_nanos(self) -> u64 {
        self.0.get()
    }

    pub const fn as_duration(self) -> Duration {
        Duration::from_nanos(self.as_nanos())
    }
}

impl TryFrom<Duration> for DomainClockPeriod {
    type Error = DomainClockError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        let nanos =
            u64::try_from(value.as_nanos()).map_err(|_| DomainClockError::InvalidPeriod {
                value: humantime::format_duration(value).to_string(),
            })?;
        let nanos = NonZeroU64::new(nanos).ok_or_else(|| DomainClockError::InvalidPeriod {
            value: humantime::format_duration(value).to_string(),
        })?;
        Ok(Self(nanos))
    }
}

impl FromStr for DomainClockPeriod {
    type Err = DomainClockError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let duration =
            humantime::parse_duration(value).map_err(|_| DomainClockError::InvalidPeriod {
                value: value.to_string(),
            })?;
        Self::try_from(duration).map_err(|_| DomainClockError::InvalidPeriod {
            value: value.to_string(),
        })
    }
}

impl fmt::Display for DomainClockPeriod {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        humantime::format_duration(self.as_duration()).fmt(formatter)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainClockState {
    wall_started_at: Timestamp,
    logical_start: Timestamp,
    time_rate: DomainTimeRate,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainClockProgress {
    pub generation: u64,
    pub authority_revision: DomainClockAuthorityRevision,
    pub authority: ClusterNodeIdentity,
    pub tick: crate::DomainTick,
}

impl DomainClockState {
    pub const fn new(
        wall_started_at: Timestamp,
        logical_start: Timestamp,
        time_rate: DomainTimeRate,
    ) -> Self {
        Self {
            wall_started_at,
            logical_start,
            time_rate,
        }
    }

    pub const fn wall_started_at(&self) -> Timestamp {
        self.wall_started_at
    }

    pub const fn logical_start(&self) -> Timestamp {
        self.logical_start
    }

    pub const fn time_rate(&self) -> DomainTimeRate {
        self.time_rate
    }

    /// Projects physical time through the committed mapping. Fractional logical nanoseconds are
    /// rounded down, so a read never claims a logical instant that has not yet been reached.
    pub fn logical_time_at(
        &self,
        wall_time: Timestamp,
    ) -> Result<Timestamp, Report<DomainClockError>> {
        let elapsed = wall_time
            .duration_since(self.wall_started_at)
            .unwrap_or(Duration::ZERO);
        let logical_nanos = if self.time_rate == DomainTimeRate::ONE {
            u64::try_from(elapsed.as_nanos()).map_err(|_| {
                Report::new(DomainClockError::DurationOverflow {
                    operation: "projection",
                })
            })?
        } else {
            let scaled = (elapsed.as_nanos().approx_into::<f64>() * self.time_rate.get()).floor();
            scaled.checked_approx_into().ok_or_else(|| {
                Report::new(DomainClockError::DurationOverflow {
                    operation: "projection",
                })
            })?
        };
        self.logical_start
            .checked_add(Duration::from_nanos(logical_nanos))
            .change_context(DomainClockError::TimestampOverflow {
                operation: "projection",
            })
    }

    /// Converts a positive logical delta to physical time. Fractional physical nanoseconds are
    /// rounded up, so a deadline never fires before its logical target.
    pub fn wall_duration_until(
        &self,
        current_logical: Timestamp,
        target_logical: Timestamp,
    ) -> Result<Duration, Report<DomainClockError>> {
        let Some(logical_delta) = target_logical.duration_since(current_logical) else {
            return Ok(Duration::ZERO);
        };
        if logical_delta.is_zero() {
            return Ok(Duration::ZERO);
        }
        if self.time_rate == DomainTimeRate::ONE {
            return Ok(logical_delta);
        }
        let wall_nanos =
            (logical_delta.as_nanos().approx_into::<f64>() / self.time_rate.get()).ceil();
        let wall_nanos: u64 = wall_nanos.checked_approx_into().ok_or_else(|| {
            Report::new(DomainClockError::DurationOverflow {
                operation: "rate conversion",
            })
        })?;
        Ok(Duration::from_nanos(wall_nanos.max(1)))
    }

    pub fn tick_boundary(
        &self,
        period: DomainClockPeriod,
        tick_id: u64,
    ) -> Result<DomainClockBoundary, Report<DomainClockError>> {
        let zero_based = tick_id
            .checked_sub(1)
            .ok_or_else(|| Report::new(DomainClockError::InvalidTickId))?;
        let offset = zero_based.checked_mul(period.as_nanos()).ok_or_else(|| {
            Report::new(DomainClockError::DurationOverflow {
                operation: "tick boundary",
            })
        })?;
        let logical_timestamp = self
            .logical_start
            .checked_add(Duration::from_nanos(offset))
            .change_context(DomainClockError::TimestampOverflow {
                operation: "tick boundary",
            })?;
        Ok(DomainClockBoundary {
            tick_id,
            logical_timestamp,
        })
    }

    /// Returns the latest due advancement at or after `next_tick_id`, coalescing all earlier
    /// missed periods with bounded direct arithmetic.
    pub fn due_advancement(
        &self,
        period: DomainClockPeriod,
        next_tick_id: u64,
        wall_time: Timestamp,
    ) -> Result<Option<DomainClockAdvancement>, Report<DomainClockError>> {
        if next_tick_id == 0 {
            return Err(Report::new(DomainClockError::InvalidTickId));
        }
        let reached = self.logical_time_at(wall_time)?;
        let elapsed = reached
            .duration_since(self.logical_start)
            .assured("projection never returns a timestamp before the logical origin");
        let zero_based = elapsed.as_nanos() / u128::from(period.as_nanos());
        let zero_based =
            u64::try_from(zero_based).map_err(|_| Report::new(DomainClockError::TickIdOverflow))?;
        let tick_id = zero_based
            .checked_add(1)
            .ok_or_else(|| Report::new(DomainClockError::TickIdOverflow))?;
        if tick_id < next_tick_id {
            return Ok(None);
        }
        let boundary = self.tick_boundary(period, tick_id)?;
        let next_tick_id = tick_id
            .checked_add(1)
            .ok_or_else(|| Report::new(DomainClockError::TickIdOverflow))?;
        Ok(Some(DomainClockAdvancement {
            boundary,
            next_tick_id,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DomainClockBoundary {
    tick_id: u64,
    logical_timestamp: Timestamp,
}

impl DomainClockBoundary {
    pub const fn tick_id(self) -> u64 {
        self.tick_id
    }

    pub const fn logical_timestamp(self) -> Timestamp {
        self.logical_timestamp
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DomainClockAdvancement {
    boundary: DomainClockBoundary,
    next_tick_id: u64,
}

impl DomainClockAdvancement {
    pub const fn boundary(self) -> DomainClockBoundary {
        self.boundary
    }

    pub const fn next_tick_id(self) -> u64 {
        self.next_tick_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping(rate: f64) -> DomainClockState {
        DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(0),
            DomainTimeRate::try_from(rate).expect("fixture rate is positive and finite"),
        )
    }

    #[test]
    fn accepts_tiny_and_large_finite_positive_rates() {
        assert!(DomainTimeRate::try_from(f64::MIN_POSITIVE).is_ok());
        assert!(DomainTimeRate::try_from(f64::MAX).is_ok());
    }

    #[test]
    fn rejects_nonpositive_and_nonfinite_rates() {
        for rate in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(DomainTimeRate::try_from(rate).is_err());
        }
        assert!(serde_json::from_str::<DomainTimeRate>("0.0").is_err());
    }

    #[test]
    fn period_must_be_positive_and_fit_in_nanoseconds() {
        assert!(DomainClockPeriod::try_from(Duration::ZERO).is_err());
        assert!(DomainClockPeriod::try_from(Duration::from_nanos(1)).is_ok());
        assert!(DomainClockPeriod::try_from(Duration::from_secs(u64::MAX)).is_err());
        assert!(serde_json::from_str::<DomainClockPeriod>("0").is_err());
    }

    #[test]
    fn projection_at_or_before_anchor_returns_logical_origin() {
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(i64::MAX),
            Timestamp::from_unix_nanos(i64::MAX),
            DomainTimeRate::ONE,
        );
        assert_eq!(
            clock
                .logical_time_at(Timestamp::from_unix_nanos(i64::MAX))
                .expect("the anchor projects to the logical origin"),
            Timestamp::from_unix_nanos(i64::MAX)
        );
        assert_eq!(
            clock
                .logical_time_at(Timestamp::from_unix_nanos(i64::MIN))
                .expect("wall time before the anchor clamps to the logical origin"),
            Timestamp::from_unix_nanos(i64::MAX)
        );
    }

    #[test]
    fn projection_rejects_progress_past_timestamp_maximum() {
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(i64::MAX),
            DomainTimeRate::ONE,
        );
        let error = clock
            .logical_time_at(Timestamp::from_unix_nanos(1))
            .expect_err("one nanosecond past the maximum origin must fail");
        assert!(matches!(
            error.current_context(),
            DomainClockError::TimestampOverflow { .. }
        ));
    }

    #[test]
    fn identity_projection_reaches_both_timestamp_endpoints_exactly() {
        let clock = DomainClockState::new(
            Timestamp::from_unix_nanos(i64::MIN),
            Timestamp::from_unix_nanos(i64::MIN),
            DomainTimeRate::ONE,
        );
        assert_eq!(
            clock
                .logical_time_at(Timestamp::from_unix_nanos(i64::MAX))
                .expect("the complete signed nanosecond span is representable"),
            Timestamp::from_unix_nanos(i64::MAX)
        );
    }

    #[test]
    fn rate_conversion_uses_ceil_and_never_fires_early() {
        let clock = mapping(4.0);
        assert_eq!(
            clock
                .wall_duration_until(
                    Timestamp::from_unix_nanos(0),
                    Timestamp::from_unix_nanos(1_000_000_000),
                )
                .expect("one second at 4x is representable"),
            Duration::from_millis(250)
        );
        assert_eq!(
            clock
                .wall_duration_until(Timestamp::from_unix_nanos(0), Timestamp::from_unix_nanos(1),)
                .expect("one logical nanosecond is representable"),
            Duration::from_nanos(1)
        );
    }

    #[test]
    fn missed_periods_coalesce_to_the_latest_due_boundary() {
        let clock = mapping(1.0);
        let period = DomainClockPeriod::try_from(Duration::from_secs(1))
            .expect("one second is a valid period");
        let advancement = clock
            .due_advancement(period, 1, Timestamp::from_unix_nanos(3_500_000_000))
            .expect("fixture arithmetic is representable")
            .expect("an advancement is due");
        assert_eq!(advancement.boundary().tick_id(), 4);
        assert_eq!(
            advancement.boundary().logical_timestamp(),
            Timestamp::from_unix_nanos(3_000_000_000)
        );
        assert_eq!(advancement.next_tick_id(), 5);
    }

    #[test]
    fn advancement_reports_tick_id_and_period_overflow() {
        let full_range_clock = DomainClockState::new(
            Timestamp::from_unix_nanos(i64::MIN),
            Timestamp::from_unix_nanos(i64::MIN),
            DomainTimeRate::ONE,
        );
        let nanosecond = DomainClockPeriod::try_from(Duration::from_nanos(1))
            .expect("one nanosecond is a valid period");
        let error = full_range_clock
            .due_advancement(nanosecond, 1, Timestamp::from_unix_nanos(i64::MAX))
            .expect_err("the full range contains one more boundary than u64 can identify");
        assert!(matches!(
            error.current_context(),
            DomainClockError::TickIdOverflow
        ));

        let period = DomainClockPeriod::try_from(Duration::from_nanos(u64::MAX))
            .expect("the maximum u64 nanosecond duration is a valid period");
        let error = mapping(1.0)
            .tick_boundary(period, 3)
            .expect_err("two maximum periods cannot be represented");
        assert!(matches!(
            error.current_context(),
            DomainClockError::DurationOverflow { .. }
        ));
    }
}
