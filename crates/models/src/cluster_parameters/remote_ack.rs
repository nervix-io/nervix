//! One coherent liveness policy for a remote delivery and its acknowledgement watches.
//!
//! Layer: vocabulary.
//! - **Owns.** Committed ACK timing definitions, relationships and a delivery's captured policy.
//! - **Depends on.** Class-typed parameters and duration arithmetic.
//! - **Must not know.** Runtime tasks, timers, transport or acknowledgement storage.

use super::*;
use crate::ClusterParameters;

pub struct RemoteAckAliveInterval;
impl RemoteAckAliveInterval {
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
    /// At most two hundred poll opportunities in one owning deadline. A one-nanosecond floor
    /// keeps sub-microsecond policies from rounding the internal retry cadence to zero.
    pub fn retry_interval(self) -> Duration {
        (self.remote_ack_dispatch_timeout / 200).max(Duration::from_nanos(1))
    }
    pub fn instantiation_poll(self) -> Duration {
        (self.remote_relay_instantiation_wait / 200).max(Duration::from_nanos(1))
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
