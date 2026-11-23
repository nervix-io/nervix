//! How nodes find, judge and verify one another, declared as settings.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The definitions of the node unavailability timeout, the gossip interval, the
//!   clock-skew tolerance, the gossip receive queue capacity and the gossip tombstone grace period,
//!   their single defaults, the failure-detection intervals derived from the gossip interval, the
//!   capability claim of the gossip bounds, and the group that holds them.
//! - **Depends on.** The vocabulary's setting classes, timing prerequisites and capability record,
//!   and the native primitive channel bound.
//! - **Must not know.** Command-line parsing or how membership observes a node.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

// Cluster policy definitions have one vocabulary owner. These facades hold bootstrap inputs.
use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
pub use nervix_models::NodeUnavailabilityTimeout;
use nervix_models::{
    NodeCapabilities, Prerequisites, Setting, SettingDefinition, TimingPrerequisite,
};
use nervix_primitives::sync::Semaphore;
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

/// How many gossip intervals a stopping node keeps gossiping after it advertised that it is
/// terminating, so that the advertisement reaches its peers before it leaves.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "a gossip round carries a node's newest state to its peers, and a second round \
                  covers a round that began just before the advertisement"
    )
)]
const TERMINATING_PROPAGATION_GOSSIP_INTERVALS: u32 = 2;

const _: () = assert!(
    TERMINATING_PROPAGATION_GOSSIP_INTERVALS <= FAILURE_DETECTOR_MAX_GOSSIP_INTERVALS,
    "the gossip interval's validation keeps only the failure detector's largest count of \
     intervals inside a duration"
);

const _: () = assert!(
    FAILURE_DETECTOR_INITIAL_GOSSIP_INTERVALS < FAILURE_DETECTOR_MAX_GOSSIP_INTERVALS,
    "the failure detector's initial interval must be one it can still accept as a sample"
);

/// Why a membership timing or gossip bound cannot run. Each variant names the setting by its
/// option name, with the value given.
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
    #[error(
        "{setting} ({value}) exceeds the {maximum} items a channel holds",
        setting = GossipReceiveQueueCapacity::NAME
    )]
    GossipReceiveQueueCapacity { value: usize, maximum: usize },
    #[error(
        "{setting} ({value:?}) must be positive and at most {maximum:?}",
        setting = GossipTombstoneGracePeriod::NAME
    )]
    GossipTombstoneGracePeriod { value: Duration, maximum: Duration },
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

/// How many incoming gossip datagrams wait between the interconnect handler that receives them
/// and Chitchat, which reads them as its socket. Each holds at most the 60 KiB gossip bound, so
/// the queue retains at most this many times that bound.
pub struct GossipReceiveQueueCapacity;

impl SettingDefinition for GossipReceiveQueueCapacity {
    type Class = NodeCapabilities;
    type Value = NonZeroUsize;
    type Context = ();
    type Error = MembershipSettingsError;

    const NAME: &'static str = "gossip-receive-queue-capacity";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> NonZeroUsize {
        NonZeroUsize::new(1024).assured("the gossip receive queue default is positive")
    }

    fn validate(value: &NonZeroUsize, _: &()) -> Result<(), Report<MembershipSettingsError>> {
        if value.get() > Semaphore::MAX_PERMITS {
            return Err(Report::new(
                MembershipSettingsError::GossipReceiveQueueCapacity {
                    value: value.get(),
                    maximum: Semaphore::MAX_PERMITS,
                },
            ));
        }
        Ok(())
    }
}

/// How long Chitchat keeps the tombstone of a key a node deleted from its gossip state before it
/// collects it, so peers learn of the deletion first.
pub struct GossipTombstoneGracePeriod;

impl GossipTombstoneGracePeriod {
    /// The longest grace: `u32::MAX` whole seconds, which Chitchat can add to the instant a key
    /// was deleted on every supported monotonic clock.
    fn maximum() -> Duration {
        Duration::from_secs(u64::from(u32::MAX))
    }
}

impl SettingDefinition for GossipTombstoneGracePeriod {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = MembershipSettingsError;

    const NAME: &'static str = "gossip-tombstone-grace-period";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(60)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<MembershipSettingsError>> {
        let maximum = Self::maximum();
        if value.is_zero() || *value > maximum {
            return Err(Report::new(
                MembershipSettingsError::GossipTombstoneGracePeriod {
                    value: *value,
                    maximum,
                },
            ));
        }
        Ok(())
    }
}

/// How the cluster discovers its members, judges whether they are available, how far apart
/// their clocks may be, and the gossip bounds this node carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipSettings {
    pub node_unavailability_timeout: Setting<NodeUnavailabilityTimeout>,
    pub gossip_interval: Setting<GossipInterval>,
    pub clock_skew_tolerance: Setting<ClockSkewTolerance>,
    pub gossip_receive_queue_capacity: Setting<GossipReceiveQueueCapacity>,
    pub gossip_tombstone_grace_period: Setting<GossipTombstoneGracePeriod>,
}

/// Membership values an operator or a test gives; an absent value takes its definition's default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MembershipValues {
    pub node_unavailability_timeout: Option<Duration>,
    pub gossip_interval: Option<Duration>,
    pub clock_skew_tolerance: Option<Duration>,
    pub gossip_receive_queue_capacity: Option<NonZeroUsize>,
    pub gossip_tombstone_grace_period: Option<Duration>,
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
            gossip_receive_queue_capacity: Setting::resolve(
                values.gossip_receive_queue_capacity,
                &(),
            )?,
            gossip_tombstone_grace_period: Setting::resolve(
                values.gossip_tombstone_grace_period,
                &(),
            )?,
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

    pub fn gossip_receive_queue_capacity(&self) -> NonZeroUsize {
        *self.gossip_receive_queue_capacity.value()
    }

    pub fn gossip_tombstone_grace_period(&self) -> Duration {
        *self.gossip_tombstone_grace_period.value()
    }

    /// The gossip bounds this run claims, as its capability record carries them.
    pub fn capabilities(&self) -> nervix_models::MembershipCapabilities {
        let queue: u64 = self.gossip_receive_queue_capacity().get().arch_into();
        nervix_models::MembershipCapabilities {
            gossip_receive_queue_capacity: NonZeroU64::new(queue)
                .assured("a positive native queue capacity stays positive as a 64-bit count"),
            gossip_tombstone_grace_period: self.gossip_tombstone_grace_period(),
        }
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

    /// How long a stopping node keeps gossiping after it advertised that it is terminating, so
    /// that its peers hear it before it leaves: two gossip intervals.
    pub fn terminating_propagation(&self) -> Duration {
        self.gossip_intervals(TERMINATING_PROPAGATION_GOSSIP_INTERVALS)
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
    fn the_defaults_keep_the_gossip_queue_and_tombstone_grace_it_ran_with() {
        let defaults = MembershipSettings::default();
        assert_eq!(defaults.gossip_receive_queue_capacity().get(), 1024);
        assert_eq!(
            defaults.gossip_tombstone_grace_period(),
            Duration::from_secs(60)
        );
        assert_eq!(
            defaults.capabilities(),
            nervix_models::MembershipCapabilities {
                gossip_receive_queue_capacity: NonZeroU64::new(1024).assured("1,024 is positive"),
                gossip_tombstone_grace_period: Duration::from_secs(60),
            }
        );
        assert_eq!(
            GossipReceiveQueueCapacity::NAME,
            "gossip-receive-queue-capacity"
        );
        assert_eq!(
            GossipTombstoneGracePeriod::NAME,
            "gossip-tombstone-grace-period"
        );
    }

    #[test]
    fn configured_gossip_bounds_are_kept_and_claimed() {
        let settings = MembershipSettings::resolve(MembershipValues {
            gossip_receive_queue_capacity: NonZeroUsize::new(2),
            gossip_tombstone_grace_period: Some(Duration::from_millis(1500)),
            ..MembershipValues::default()
        })
        .assured("a positive queue and a positive grace are valid");
        assert_eq!(settings.gossip_receive_queue_capacity().get(), 2);
        assert_eq!(
            settings.gossip_tombstone_grace_period(),
            Duration::from_millis(1500)
        );
        assert_eq!(
            settings.capabilities().gossip_receive_queue_capacity.get(),
            2
        );
        assert_eq!(
            settings.capabilities().gossip_tombstone_grace_period,
            Duration::from_millis(1500)
        );
        let widest = MembershipSettings::resolve(MembershipValues {
            gossip_receive_queue_capacity: NonZeroUsize::new(Semaphore::MAX_PERMITS),
            gossip_tombstone_grace_period: Some(Duration::from_secs(u64::from(u32::MAX))),
            ..MembershipValues::default()
        })
        .assured("the channel bound and u32::MAX seconds are the widest valid gossip bounds");
        assert_eq!(
            widest.gossip_receive_queue_capacity().get(),
            Semaphore::MAX_PERMITS
        );
    }

    #[test]
    fn a_gossip_queue_beyond_the_channel_bound_is_refused_naming_it() {
        let value = Semaphore::MAX_PERMITS + 1;
        let refused = MembershipSettings::resolve(MembershipValues {
            gossip_receive_queue_capacity: NonZeroUsize::new(value),
            ..MembershipValues::default()
        })
        .err()
        .assured("one item beyond the native channel bound is refused");
        assert_eq!(
            refused.current_context(),
            &MembershipSettingsError::GossipReceiveQueueCapacity {
                value,
                maximum: Semaphore::MAX_PERMITS,
            }
        );
        assert_eq!(
            format!("{refused:#}"),
            format!(
                "gossip-receive-queue-capacity ({value}) exceeds the {} items a channel holds",
                Semaphore::MAX_PERMITS
            )
        );
    }

    #[test]
    fn a_tombstone_grace_of_nothing_or_beyond_u32_seconds_is_refused_naming_it() {
        let maximum = Duration::from_secs(u64::from(u32::MAX));
        for value in [Duration::ZERO, maximum + Duration::from_nanos(1)] {
            let refused = MembershipSettings::resolve(MembershipValues {
                gossip_tombstone_grace_period: Some(value),
                ..MembershipValues::default()
            })
            .err()
            .assured("no grace and a grace beyond u32::MAX seconds are refused");
            assert_eq!(
                refused.current_context(),
                &MembershipSettingsError::GossipTombstoneGracePeriod { value, maximum }
            );
        }
        let refused = MembershipSettings::resolve(MembershipValues {
            gossip_tombstone_grace_period: Some(Duration::ZERO),
            ..MembershipValues::default()
        })
        .err()
        .assured("no grace is refused");
        assert_eq!(
            format!("{refused:#}"),
            "gossip-tombstone-grace-period (0ns) must be positive and at most 4294967295s"
        );
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
