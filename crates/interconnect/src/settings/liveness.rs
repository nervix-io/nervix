//! Liveness budgets derived from the committed cluster policy.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** HTTP/2 ping and application-probe derivations from member unavailability.
//! - **Depends on.** Vocabulary setting classes and the committed parameter values.
//! - **Must not know.** Membership decisions, gossip publication or graph execution.
#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns transport liveness derivations")
)]

use std::{convert::Infallible, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{ClusterParameterValues, ClusterParameters, Setting, SettingDefinition};

/// Half the unavailability budget between HTTP/2 pings.
pub struct PingInterval;
impl SettingDefinition for PingInterval {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ClusterParameterValues;
    type Error = Infallible;
    const NAME: &'static str = "interconnect-ping-interval";
    const DERIVATION: Option<&'static str> =
        Some("node-unavailability-timeout / 2, at least one timer nanosecond");
    fn default_value(context: &ClusterParameterValues) -> Duration {
        (context.node_unavailability_timeout() / 2).max(Duration::from_nanos(1))
    }
    fn validate(_: &Duration, _: &ClusterParameterValues) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// One quarter of the unavailability budget for a ping answer.
pub struct PingTimeout;
impl SettingDefinition for PingTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ClusterParameterValues;
    type Error = Infallible;
    const NAME: &'static str = "interconnect-ping-timeout";
    const DERIVATION: Option<&'static str> =
        Some("node-unavailability-timeout / 4, at least one timer nanosecond");
    fn default_value(context: &ClusterParameterValues) -> Duration {
        (context.node_unavailability_timeout() / 4).max(Duration::from_nanos(1))
    }
    fn validate(_: &Duration, _: &ClusterParameterValues) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// One tenth of the unavailability budget for an application health answer.
pub struct ProbeTimeout;
impl SettingDefinition for ProbeTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ClusterParameterValues;
    type Error = Infallible;
    const NAME: &'static str = "interconnect-probe-timeout";
    const DERIVATION: Option<&'static str> =
        Some("node-unavailability-timeout / 10, at least one timer nanosecond");
    fn default_value(context: &ClusterParameterValues) -> Duration {
        (context.node_unavailability_timeout() / 10).max(Duration::from_nanos(1))
    }
    fn validate(_: &Duration, _: &ClusterParameterValues) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// One coherent live projection, published to existing and future transport connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterconnectLiveness {
    ping_interval: Setting<PingInterval>,
    ping_timeout: Setting<PingTimeout>,
    probe_timeout: Setting<ProbeTimeout>,
}
impl InterconnectLiveness {
    pub fn from_parameters(values: &ClusterParameterValues) -> Self {
        Self {
            ping_interval: Setting::resolve(None, values).assured("the derivation is infallible"),
            ping_timeout: Setting::resolve(None, values).assured("the derivation is infallible"),
            probe_timeout: Setting::resolve(None, values).assured("the derivation is infallible"),
        }
    }
    pub fn ping_interval(self) -> Duration {
        *self.ping_interval.value()
    }
    pub fn ping_timeout(self) -> Duration {
        *self.ping_timeout.value()
    }
    pub fn probe_timeout(self) -> Duration {
        *self.probe_timeout.value()
    }
}
impl Default for InterconnectLiveness {
    fn default() -> Self {
        Self::from_parameters(&ClusterParameterValues::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transport_liveness_tracks_the_committed_unavailability_budget() {
        let values = ClusterParameterValues::resolve(nervix_models::ClusterParameterSeedValues {
            node_unavailability_timeout: Some(Duration::from_secs(4)),
            ..Default::default()
        })
        .assured("four seconds is positive");
        let settings = InterconnectLiveness::from_parameters(&values);
        assert_eq!(settings.ping_interval(), Duration::from_secs(2));
        assert_eq!(settings.ping_timeout(), Duration::from_secs(1));
        assert_eq!(settings.probe_timeout(), Duration::from_millis(400));
    }
}
