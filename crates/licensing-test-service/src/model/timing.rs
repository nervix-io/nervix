//! Issue-relative refresh instants for the signed scenario peer's decisions.
//!
//! Layer: test harness.
//! - **Owns.** Refresh, overdue and offline instants derived from explicit fixture time.
//! - **Depends on.** The fixture's validated operating conditions and timestamp arithmetic.
//! - **Must not know.** Activation expiry, commercial terms, transitions, HTTP or cluster state.

use chrono::{DateTime, TimeDelta, Utc};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_licensing_protocol::conditions::{
    ConditionDuration, OFFLINE_ALLOWANCE, OperatingConditions, REFRESH_INTERVAL, REFRESH_OVERDUE,
};

pub(super) struct RefreshTiming {
    pub(super) refresh_after: DateTime<Utc>,
    pub(super) overdue_at: DateTime<Utc>,
    pub(super) valid_until: Option<DateTime<Utc>>,
}

impl RefreshTiming {
    pub(super) fn new(issued_at: DateTime<Utc>, operating: &OperatingConditions) -> Self {
        let refresh_after = Self::bounded_at(issued_at, operating, REFRESH_INTERVAL);
        let overdue_duration = match operating
            .duration(REFRESH_OVERDUE)
            .assured("validated overdue duration")
        {
            ConditionDuration::Bounded { seconds } => seconds,
            ConditionDuration::Unbounded => panic!("overdue duration is bounded"),
        };
        let overdue_at = refresh_after
            .checked_add_signed(TimeDelta::seconds(
                i64::try_from(overdue_duration).assured("fixture duration fits i64"),
            ))
            .assured("fixture deadline fits chrono");
        let valid_until = match operating
            .duration(OFFLINE_ALLOWANCE)
            .assured("validated offline allowance")
        {
            ConditionDuration::Unbounded => None,
            ConditionDuration::Bounded { .. } => {
                Some(Self::bounded_at(issued_at, operating, OFFLINE_ALLOWANCE))
            }
        };
        RefreshTiming {
            refresh_after,
            overdue_at,
            valid_until,
        }
    }

    pub(super) fn bounded_at(
        issued_at: DateTime<Utc>,
        operating: &OperatingConditions,
        condition: &str,
    ) -> DateTime<Utc> {
        let duration = operating
            .duration(condition)
            .assured("validated fixture plan supplies duration");
        let ConditionDuration::Bounded { seconds } = duration else {
            panic!("fixture requires a bounded duration for {condition}");
        };
        issued_at
            .checked_add_signed(TimeDelta::seconds(
                i64::try_from(seconds).assured("fixture duration fits i64"),
            ))
            .assured("fixture deadline fits chrono")
    }
}
