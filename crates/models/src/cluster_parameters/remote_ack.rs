//! One coherent liveness policy for a remote delivery and its acknowledgement watches.
//!
//! Layer: vocabulary.
//! - **Owns.** Committed ACK timing definitions, relationships and a delivery's captured policy.
//! - **Depends on.** Class-typed parameters and duration arithmetic.
//! - **Must not know.** Runtime tasks, timers, transport or acknowledgement storage.

use meticulous::OptionExt as _;

use super::*;
use crate::ClusterParameters;

pub struct RemoteAckAliveInterval;
impl RemoteAckAliveInterval {
    /// The compile-time source of this setting definition's default.
    pub const DEFAULT: Duration = Duration::from_millis(100);
}
impl SettingDefinition for RemoteAckAliveInterval {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "remote-ack-alive-interval";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        ClusterParameterKey::RemoteAckAliveInterval.validate_duration(*value)
    }
}
pub struct RemoteAckSweepInterval;
impl RemoteAckSweepInterval {
    /// The compile-time source of this setting definition's default.
    pub const DEFAULT: Duration = Duration::from_secs(1);
}
impl SettingDefinition for RemoteAckSweepInterval {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "remote-ack-sweep-interval";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        ClusterParameterKey::RemoteAckSweepInterval.validate_duration(*value)
    }
}
pub struct RemoteRelayTotalTimeout;
impl RemoteRelayTotalTimeout {
    /// The compile-time source of this setting definition's default.
    pub const DEFAULT: Duration = Duration::from_secs(300);
}
impl SettingDefinition for RemoteRelayTotalTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "remote-relay-total-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        ClusterParameterKey::RemoteRelayTotalTimeout.validate_duration(*value)
    }
}

/// The sender captures this policy once for a delivery. It travels once with the batch, so live
/// parameter application never mixes a sender's policy with another revision's reporter.
///
/// No part of it bounds a wait for a live counterpart: liveness and membership decide that a node
/// is gone. The alive interval paces keepalives and retries, the sweep interval paces how often a
/// registering node applies membership and the admission ceiling to what it waits for, and the
/// total timeout is the operator's ceiling for one delivery.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct RemoteAckTiming {
    pub remote_ack_alive_interval: Duration,
    pub remote_ack_sweep_interval: Duration,
    pub remote_relay_total_timeout: Duration,
}
impl RemoteAckTiming {
    /// The shortest retry cadence, which keeps a sub-microsecond alive interval from rounding its
    /// cadence to zero.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "a Duration counts whole nanoseconds, so one nanosecond is the shortest \
                      cadence that is not zero"
        )
    )]
    const CADENCE_FLOOR: Duration = Duration::from_nanos(1);

    /// The first wait before a sender asks again after a failed send or a capacity refusal, and
    /// the wait between two sends of a report or outcome another node needs: a quarter of the
    /// alive interval, 25 milliseconds at the default, so a retry comes four times as often as the
    /// sender reports the delivery's records alive. A one-nanosecond floor keeps a
    /// sub-microsecond policy from rounding the cadence to zero.
    pub fn retry_interval(self) -> Duration {
        (self.remote_ack_alive_interval / 4).max(Self::CADENCE_FLOOR)
    }

    /// The longest wait a held delivery's backoff reaches before it asks for its grant again:
    /// twenty-five alive intervals, 2.5 seconds at the default, so a held delivery notices that
    /// room returned at most that late. It paces asking, and never ends a wait.
    pub fn retry_ceiling(self) -> Duration {
        self.remote_ack_alive_interval
            .checked_mul(25)
            .assured(
                "a validated alive interval is at most u32::MAX seconds, so twenty-five of them \
                 fit a Duration",
            )
            .max(self.retry_interval())
    }

    /// How long a sender leaves a relay channel without a batch before its next batch opens a
    /// fresh incarnation: the delivery's total admission budget. It is shorter than the channel's
    /// retention, so a receiver still holds the watermark of a channel its sender resumes.
    pub fn relay_channel_idle_rotation(self) -> Duration {
        self.remote_relay_total_timeout
    }

    /// How long a receiver keeps what reconciling this delivery's channel needs after it last made
    /// progress or was last recorded or consulted: twice the total admission budget, so it outlasts
    /// the delivery and its channel's idle rotation.
    pub fn relay_channel_retention(self) -> Duration {
        self.remote_relay_total_timeout.checked_mul(2).assured(
            "a validated total admission budget is at most u32::MAX seconds, so twice it fits a \
             Duration",
        )
    }

    pub fn from_parameters(values: &ClusterParameterValues) -> Self {
        Self {
            remote_ack_alive_interval: values.remote_ack_alive_interval(),
            remote_ack_sweep_interval: values.remote_ack_sweep_interval(),
            remote_relay_total_timeout: values.remote_relay_total_timeout(),
        }
    }
    /// Also checked on receipt: a wire value cannot introduce zero divisors or an inconsistent
    /// policy merely because its sender claims it came from committed parameters.
    pub fn validate(self) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::RemoteAckAliveInterval
            .validate_duration(self.remote_ack_alive_interval)?;
        ClusterParameterKey::RemoteAckSweepInterval
            .validate_duration(self.remote_ack_sweep_interval)?;
        ClusterParameterKey::RemoteRelayTotalTimeout
            .validate_duration(self.remote_relay_total_timeout)?;
        let sweep = self.remote_ack_sweep_interval.as_nanos();
        if !self
            .remote_relay_total_timeout
            .as_nanos()
            .is_multiple_of(sweep)
        {
            return Err(Report::new(ClusterParameterError::AckSweepDivisibility));
        }
        Ok(())
    }
    /// How many of its own sweeps a registering node counts before it gives up a registration
    /// whose delivery never reported admission: the total timeout in sweep intervals.
    pub fn admission_sweeps(self) -> u64 {
        u64::try_from(
            self.remote_relay_total_timeout.as_nanos() / self.remote_ack_sweep_interval.as_nanos(),
        )
        .assured("validated duration ratios fit the unsigned 64-bit sweep counter")
    }
}

impl Default for RemoteAckTiming {
    fn default() -> Self {
        Self::from_parameters(&ClusterParameterValues::default())
    }
}
