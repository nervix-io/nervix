//! How nodes find, judge and verify one another, declared as settings.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The definitions of the node unavailability timeout, the gossip interval and the
//!   clock-skew tolerance, their single defaults, the failure-detection intervals derived from the
//!   gossip interval, and the group that holds them.
//! - **Depends on.** The vocabulary's setting classes and timing prerequisites.
//! - **Must not know.** Command-line parsing or how membership observes a node.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

// Cluster policy definitions have one vocabulary owner. These facades hold bootstrap inputs.
use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
pub use nervix_models::NodeUnavailabilityTimeout;
use nervix_models::{Prerequisites, Setting, SettingDefinition, TimingPrerequisite};
use thiserror::Error;

/// How many gossip intervals the failure detector assumes between a peer's heartbeats before it
/// has observed any.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the failure detector's intervals derive from the gossip interval by fixed \
                  factors"
    )
)]
const FAILURE_DETECTOR_INITIAL_GOSSIP_INTERVALS: u32 = 10;

/// How many gossip intervals the failure detector accepts between two of a peer's heartbeats
/// before it drops the sample as an outlier.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the failure detector's intervals derive from the gossip interval by fixed \
                  factors"
    )
)]
const FAILURE_DETECTOR_MAX_GOSSIP_INTERVALS: u32 = 20;

const _: () = assert!(
    FAILURE_DETECTOR_INITIAL_GOSSIP_INTERVALS < FAILURE_DETECTOR_MAX_GOSSIP_INTERVALS,
    "the failure detector's initial interval must be one it can still accept as a sample"
);

/// Why a membership timing cannot run. Each variant names the setting by its option name, with
/// the value given.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MembershipSettingsError {
    #[error("invalid cluster parameter")]
    InvalidClusterParameter,
    #[error("{prerequisite} ({value:?}) must be at least 1ms")]
    BelowOneMillisecond {
        prerequisite: TimingPrerequisite,
        value: Duration,
    },
    #[error(
        "{prerequisite} ({value:?}) times {FAILURE_DETECTOR_MAX_GOSSIP_INTERVALS} must fit a \
         duration",
        prerequisite = TimingPrerequisite::GossipInterval
    )]
    GossipIntervalTooLong { value: Duration },
}

/// Refuses a timing prerequisite shorter than one millisecond.
fn at_least_one_millisecond(
    prerequisite: TimingPrerequisite,
    value: Duration,
) -> Result<(), Report<MembershipSettingsError>> {
    if value.as_millis() == 0 {
        return Err(Report::new(MembershipSettingsError::BelowOneMillisecond {
            prerequisite,
            value,
        }));
    }
    Ok(())
}

/// How often a node exchanges its gossip record with its peers.
pub struct GossipInterval;

impl SettingDefinition for GossipInterval {
    type Class = Prerequisites;
    type Value = Duration;
    type Context = ();
    type Error = MembershipSettingsError;

    const NAME: &'static str = TimingPrerequisite::GossipInterval.setting_name();
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_millis(500)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<MembershipSettingsError>> {
        at_least_one_millisecond(TimingPrerequisite::GossipInterval, *value)?;
        if value
            .checked_mul(FAILURE_DETECTOR_MAX_GOSSIP_INTERVALS)
            .is_none()
        {
            return Err(Report::new(
                MembershipSettingsError::GossipIntervalTooLong { value: *value },
            ));
        }
        Ok(())
    }
}

/// The physical clock skew this node tolerates against a peer it joins the cluster through.
pub struct ClockSkewTolerance;

impl SettingDefinition for ClockSkewTolerance {
    type Class = Prerequisites;
    type Value = Duration;
    type Context = ();
    type Error = MembershipSettingsError;

    const NAME: &'static str = TimingPrerequisite::ClockSkewTolerance.setting_name();
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_millis(500)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<MembershipSettingsError>> {
        at_least_one_millisecond(TimingPrerequisite::ClockSkewTolerance, *value)
    }
}

/// How the cluster discovers its members, judges whether they are available, and how far apart
/// their clocks may be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipSettings {
    pub node_unavailability_timeout: Setting<NodeUnavailabilityTimeout>,
    pub gossip_interval: Setting<GossipInterval>,
    pub clock_skew_tolerance: Setting<ClockSkewTolerance>,
}

/// Membership values an operator or a test gives; an absent value takes its definition's default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MembershipValues {
    pub node_unavailability_timeout: Option<Duration>,
    pub gossip_interval: Option<Duration>,
    pub clock_skew_tolerance: Option<Duration>,
}

impl MembershipSettings {
    /// Resolves each value that is not given through its definition's default, then validates it.
    pub fn resolve(values: MembershipValues) -> Result<Self, Report<MembershipSettingsError>> {
        let node_unavailability_timeout = Setting::resolve(values.node_unavailability_timeout, &())
            .change_context(MembershipSettingsError::InvalidClusterParameter)?;
        Ok(Self {
            node_unavailability_timeout,
            gossip_interval: Setting::resolve(values.gossip_interval, &())?,
            clock_skew_tolerance: Setting::resolve(values.clock_skew_tolerance, &())?,
        })
    }

    pub fn node_unavailability_timeout(&self) -> Duration {
        *self.node_unavailability_timeout.value()
    }

    pub fn gossip_interval(&self) -> Duration {
        *self.gossip_interval.value()
    }

    pub fn clock_skew_tolerance(&self) -> Duration {
        *self.clock_skew_tolerance.value()
    }

    /// The interval the failure detector assumes between a peer's heartbeats before it has
    /// observed any: ten gossip intervals.
    pub fn failure_detector_initial_interval(&self) -> Duration {
        self.gossip_intervals(FAILURE_DETECTOR_INITIAL_GOSSIP_INTERVALS)
    }

    /// The longest interval between two of a peer's heartbeats the failure detector still takes as
    /// a sample: twenty gossip intervals.
    pub fn failure_detector_max_interval(&self) -> Duration {
        self.gossip_intervals(FAILURE_DETECTOR_MAX_GOSSIP_INTERVALS)
    }

    /// The failure detector's configuration, with its intervals derived from the gossip cadence.
    pub(super) fn failure_detector_config(&self) -> chitchat::FailureDetectorConfig {
        chitchat::FailureDetectorConfig {
            initial_interval: self.failure_detector_initial_interval(),
            max_interval: self.failure_detector_max_interval(),
            ..chitchat::FailureDetectorConfig::default()
        }
    }

    fn gossip_intervals(&self, count: u32) -> Duration {
        self.gossip_interval()
            .checked_mul(count)
            .verified("the gossip interval's validation keeps twenty of them inside a duration")
    }
}

impl Default for MembershipSettings {
    fn default() -> Self {
        Self::resolve(MembershipValues::default())
            .assured("the membership defaults are at least one millisecond and short")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_keep_the_failure_detector_it_ran_with() {
        let defaults = MembershipSettings::default();
        assert_eq!(defaults.gossip_interval(), Duration::from_millis(500));
        assert_eq!(defaults.clock_skew_tolerance(), Duration::from_millis(500));
        assert_eq!(
            defaults.node_unavailability_timeout(),
            Duration::from_secs(10)
        );
        assert_eq!(
            defaults.failure_detector_initial_interval(),
            Duration::from_secs(5)
        );
        assert_eq!(
            defaults.failure_detector_max_interval(),
            Duration::from_secs(10)
        );
        assert_eq!(GossipInterval::NAME, "gossip-interval");
        assert_eq!(ClockSkewTolerance::NAME, "clock-skew-tolerance");
    }

    #[test]
    fn a_timing_below_one_millisecond_is_refused_naming_it() {
        let refused = MembershipSettings::resolve(MembershipValues {
            gossip_interval: Some(Duration::from_micros(10)),
            ..MembershipValues::default()
        })
        .err()
        .assured("ten microseconds is below one millisecond");
        assert_eq!(
            format!("{refused:#}"),
            "gossip-interval (10µs) must be at least 1ms"
        );
        let refused = MembershipSettings::resolve(MembershipValues {
            clock_skew_tolerance: Some(Duration::ZERO),
            ..MembershipValues::default()
        })
        .err()
        .assured("no tolerance is below one millisecond");
        assert_eq!(
            refused.current_context(),
            &MembershipSettingsError::BelowOneMillisecond {
                prerequisite: TimingPrerequisite::ClockSkewTolerance,
                value: Duration::ZERO,
            }
        );
    }

    #[test]
    fn a_gossip_interval_whose_failure_detector_overflows_is_refused() {
        let refused = MembershipSettings::resolve(MembershipValues {
            gossip_interval: Some(Duration::MAX),
            ..MembershipValues::default()
        })
        .err()
        .assured("twenty of the longest duration overflow");
        assert_eq!(
            refused.current_context(),
            &MembershipSettingsError::GossipIntervalTooLong {
                value: Duration::MAX,
            }
        );
    }
}
