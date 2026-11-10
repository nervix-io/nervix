//! Relay reconciliation retention derived from the host's one admission contract.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Retention and sweep derivations that keep admitted relay work reconcilable.
//! - **Depends on.** The host admission bound and vocabulary setting classes.
//! - **Must not know.** Runtime records, acknowledgement ownership or graph policy.
#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns derived relay reconciliation timing")
)]
use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{ClusterParameters, Setting, SettingDefinition};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "relay host admission bound ({admission:?}) must be positive and leave room for twice its \
     retention"
)]
pub struct RelayRetentionError {
    pub admission: Duration,
}

pub struct RelayChannelRetention;
impl SettingDefinition for RelayChannelRetention {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = Duration;
    type Error = RelayRetentionError;
    const NAME: &'static str = "relay-channel-retention";
    const DERIVATION: Option<&'static str> = Some("twice the host's total relay admission bound");
    fn default_value(admission: &Duration) -> Duration {
        nervix_primitives::expect_lint!(
            nervix::clamped_arithmetic,
            "an overflowing projection clamps only so validation can return its typed admission \
             error",
            admission.saturating_mul(2)
        )
    }
    fn validate(value: &Duration, admission: &Duration) -> Result<(), Report<Self::Error>> {
        if admission.is_zero() || admission.checked_mul(2) != Some(*value) {
            return Err(Report::new(RelayRetentionError {
                admission: *admission,
            }));
        }
        Ok(())
    }
}
pub struct RelayChannelSweep;
impl SettingDefinition for RelayChannelSweep {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = Duration;
    type Error = RelayRetentionError;
    const NAME: &'static str = "relay-channel-sweep";
    const DERIVATION: Option<&'static str> = Some("one tenth of relay channel retention");
    fn default_value(retention: &Duration) -> Duration {
        (*retention / 10).max(Duration::from_nanos(1))
    }
    fn validate(_: &Duration, _: &Duration) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}
/// A projection of the host policy, with no independent flag or cluster authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayRetentionSettings {
    retention: Setting<RelayChannelRetention>,
    sweep: Setting<RelayChannelSweep>,
}
impl RelayRetentionSettings {
    pub fn for_host_admission(bound: Duration) -> Result<Self, Report<RelayRetentionError>> {
        let retention = Setting::<RelayChannelRetention>::resolve(None, &bound)?;
        Ok(Self {
            sweep: Setting::resolve(None, retention.value())
                .assured("sweep derivation is infallible"),
            retention,
        })
    }
    pub fn retention(self) -> Duration {
        *self.retention.value()
    }
    pub fn sweep(self) -> Duration {
        *self.sweep.value()
    }
}
impl Default for RelayRetentionSettings {
    fn default() -> Self {
        Self::for_host_admission(nervix_models::HOST_RELAY_ADMISSION_BOUND)
            .assured("the host admission contract is positive and fits doubled retention")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_and_sweep_follow_the_validated_host_bound() {
        let settings = RelayRetentionSettings::for_host_admission(Duration::from_secs(30))
            .assured("positive bounded admission");
        assert_eq!(settings.retention(), Duration::from_secs(60));
        assert_eq!(settings.sweep(), Duration::from_secs(6));
        assert!(RelayRetentionSettings::for_host_admission(Duration::ZERO).is_err());
        assert!(RelayRetentionSettings::for_host_admission(Duration::MAX).is_err());
    }
}
