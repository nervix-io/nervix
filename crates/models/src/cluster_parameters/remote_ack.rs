//! One coherent liveness policy for a remote delivery and its acknowledgement watches.
//!
//! Layer: vocabulary.
//! - **Owns.** Committed ACK timing definitions, relationships and a delivery's captured policy.
//! - **Depends on.** Class-typed parameters and duration arithmetic.
//! - **Must not know.** Runtime tasks, timers, transport or acknowledgement storage.

use meticulous::OptionExt as _;

use super::*;
use crate::{ClusterParameters, SHORTEST_POSITIVE_DURATION};

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
pub struct RemoteAckDispatchTimeout;
impl RemoteAckDispatchTimeout {
    /// The compile-time source of this setting definition's default.
    pub const DEFAULT: Duration = Duration::from_secs(5);
}
impl SettingDefinition for RemoteAckDispatchTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "remote-ack-dispatch-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        ClusterParameterKey::RemoteAckDispatchTimeout.validate_duration(*value)
    }
}
pub struct RemoteAckSilenceTimeout;
impl RemoteAckSilenceTimeout {
    /// The compile-time source of this setting definition's default.
    pub const DEFAULT: Duration = Duration::from_secs(15);
}
impl SettingDefinition for RemoteAckSilenceTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "remote-ack-silence-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        ClusterParameterKey::RemoteAckSilenceTimeout.validate_duration(*value)
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
pub struct RemoteRelayInstantiationWait;
impl RemoteRelayInstantiationWait {
    /// The compile-time source of this setting definition's default.
    pub const DEFAULT: Duration = Duration::from_secs(5);
}
impl SettingDefinition for RemoteRelayInstantiationWait {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "remote-relay-instantiation-wait";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        ClusterParameterKey::RemoteRelayInstantiationWait.validate_duration(*value)
    }
}

/// The sender captures this policy once for a delivery. It travels once with the batch, so live
/// parameter application never mixes a sender's silence bound with another revision's reporter.
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
    pub remote_ack_dispatch_timeout: Duration,
    pub remote_ack_silence_timeout: Duration,
    pub remote_ack_sweep_interval: Duration,
    pub remote_relay_total_timeout: Duration,
    pub remote_relay_instantiation_wait: Duration,
}
impl RemoteAckTiming {
    /// At most two hundred poll opportunities in one owning deadline. The shortest positive
    /// duration floors it, so a deadline too short to divide into two hundred opportunities never
    /// rounds the internal retry cadence to zero.
    pub fn retry_interval(self) -> Duration {
        (self.remote_ack_dispatch_timeout / 200).max(SHORTEST_POSITIVE_DURATION)
    }
    pub fn instantiation_poll(self) -> Duration {
        (self.remote_relay_instantiation_wait / 200).max(SHORTEST_POSITIVE_DURATION)
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
            remote_ack_dispatch_timeout: values.remote_ack_dispatch_timeout(),
            remote_ack_silence_timeout: values.remote_ack_silence_timeout(),
            remote_ack_sweep_interval: values.remote_ack_sweep_interval(),
            remote_relay_total_timeout: values.remote_relay_total_timeout(),
            remote_relay_instantiation_wait: values.remote_relay_instantiation_wait(),
        }
    }
    /// Also checked on receipt: a wire value cannot introduce zero divisors or an inconsistent
    /// policy merely because its sender claims it came from committed parameters.
    pub fn validate(self) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::RemoteAckAliveInterval
            .validate_duration(self.remote_ack_alive_interval)?;
        ClusterParameterKey::RemoteAckDispatchTimeout
            .validate_duration(self.remote_ack_dispatch_timeout)?;
        ClusterParameterKey::RemoteAckSilenceTimeout
            .validate_duration(self.remote_ack_silence_timeout)?;
        ClusterParameterKey::RemoteAckSweepInterval
            .validate_duration(self.remote_ack_sweep_interval)?;
        ClusterParameterKey::RemoteRelayTotalTimeout
            .validate_duration(self.remote_relay_total_timeout)?;
        ClusterParameterKey::RemoteRelayInstantiationWait
            .validate_duration(self.remote_relay_instantiation_wait)?;
        if 2 * (self.remote_ack_dispatch_timeout + self.remote_ack_alive_interval)
            >= self.remote_ack_silence_timeout
        {
            return Err(Report::new(ClusterParameterError::AckReportWindow));
        }
        let sweep = self.remote_ack_sweep_interval.as_nanos();
        if !self
            .remote_ack_silence_timeout
            .as_nanos()
            .is_multiple_of(sweep)
            || !self
                .remote_relay_total_timeout
                .as_nanos()
                .is_multiple_of(sweep)
        {
            return Err(Report::new(ClusterParameterError::AckSweepDivisibility));
        }
        if self.remote_ack_dispatch_timeout > self.remote_relay_total_timeout
            || self.remote_relay_instantiation_wait > self.remote_relay_total_timeout
        {
            return Err(Report::new(ClusterParameterError::AckAdmissionWindow));
        }
        Ok(())
    }
    pub fn silent_sweeps(self) -> u64 {
        u64::try_from(
            self.remote_ack_silence_timeout.as_nanos() / self.remote_ack_sweep_interval.as_nanos(),
        )
        .assured("validated duration ratios fit the unsigned 64-bit sweep counter")
    }
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
