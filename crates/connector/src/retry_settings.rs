//! The host's shared bounded retry law.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Retry multiplier and jitter capabilities and the delay they select under a cap.
//! - **Depends on.** Vocabulary setting classes, durations and the cryptography boundary's entropy.
//! - **Must not know.** Graphs, connector drivers, acknowledgement ownership or retry deadlines.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the shared connector retry law")
)]

use std::{num::NonZeroU32, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};

#[derive(Debug, thiserror::Error)]
pub enum RetrySettingsError {
    #[error("connector-retry-jitter-percent must be between zero and one hundred: {value}")]
    Jitter { value: u8 },
}

pub struct ConnectorRetryMultiplier;
impl SettingDefinition for ConnectorRetryMultiplier {
    type Class = NodeCapabilities;
    type Value = NonZeroU32;
    type Context = ();
    type Error = RetrySettingsError;
    const NAME: &'static str = "connector-retry-multiplier";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> NonZeroU32 {
        NonZeroU32::new(2).assured("the default multiplier is positive")
    }
    fn validate(_: &NonZeroU32, _: &()) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}
pub struct ConnectorRetryJitterPercent;
impl SettingDefinition for ConnectorRetryJitterPercent {
    type Class = NodeCapabilities;
    type Value = u8;
    type Context = ();
    type Error = RetrySettingsError;
    const NAME: &'static str = "connector-retry-jitter-percent";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u8 {
        0
    }
    fn validate(value: &u8, _: &()) -> Result<(), Report<Self::Error>> {
        if *value > 100 {
            return Err(Report::new(RetrySettingsError::Jitter { value: *value }));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryBackoffSettings {
    multiplier: Setting<ConnectorRetryMultiplier>,
    jitter_percent: Setting<ConnectorRetryJitterPercent>,
}

impl RetryBackoffSettings {
    pub fn resolve(
        multiplier: Option<NonZeroU32>,
        jitter_percent: Option<u8>,
    ) -> Result<Self, Report<RetrySettingsError>> {
        Ok(Self {
            multiplier: Setting::resolve(multiplier, &())?,
            jitter_percent: Setting::resolve(jitter_percent, &())?,
        })
    }
    pub fn multiplier(self) -> NonZeroU32 {
        *self.multiplier.value()
    }
    pub fn jitter_percent(self) -> u8 {
        *self.jitter_percent.value()
    }
    pub fn next_delay(self, current: Duration, cap: Duration) -> Duration {
        let nominal = match current.checked_mul(self.multiplier().get()) {
            Some(delay) => delay.min(cap),
            None => cap,
        };
        if self.jitter_percent() == 0 {
            return nominal;
        }
        self.jittered(nominal, cap, nervix_crypto::random::u64())
    }
    /// Add between zero and the configured percentage, keeping the caller's hard cap intact.
    fn jittered(self, nominal: Duration, cap: Duration, sample: u64) -> Duration {
        let percent = u32::try_from(
            sample
                % u64::from(self.jitter_percent())
                    .checked_add(1)
                    .assured("the validated percentage is at most one hundred"),
        )
        .assured("a jitter percentage is at most one hundred");
        let extra_nanos =
            nominal.as_nanos().checked_mul(u128::from(percent)).assured(
                "Duration's u64 seconds times nanoseconds and at most one hundred fit u128",
            ) / 100;
        let extra = Duration::new(
            u64::try_from(extra_nanos / 1_000_000_000)
                .assured("a percentage at most one hundred cannot exceed the nominal duration"),
            u32::try_from(extra_nanos % 1_000_000_000).assured("a nanosecond remainder fits u32"),
        );
        match nominal.checked_add(extra) {
            Some(value) => value.min(cap),
            None => cap,
        }
    }
}
impl Default for RetryBackoffSettings {
    fn default() -> Self {
        Self::resolve(None, None).assured("the retry defaults validate")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multiplier_and_jitter_preserve_the_hard_cap() {
        let settings = RetryBackoffSettings::resolve(NonZeroU32::new(3), Some(20))
            .assured("the selected law validates");
        let without_jitter = RetryBackoffSettings::resolve(NonZeroU32::new(3), Some(0))
            .assured("the selected multiplier validates");
        assert_eq!(
            without_jitter.next_delay(Duration::from_millis(100), Duration::from_secs(1)),
            Duration::from_millis(300)
        );
        let half = Duration::MAX / 2;
        assert_eq!(
            settings.jittered(half, Duration::MAX, 20),
            half.checked_add(half / 5)
                .assured("six tenths of Duration MAX fit")
        );
        assert_eq!(
            settings.jittered(Duration::from_millis(300), Duration::from_secs(1), 20),
            Duration::from_millis(360)
        );
        assert_eq!(
            settings.jittered(Duration::from_millis(900), Duration::from_secs(1), 20),
            Duration::from_secs(1)
        );
        assert_eq!(
            settings.next_delay(Duration::MAX, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
        assert!(RetryBackoffSettings::resolve(None, Some(101)).is_err());
        assert_eq!(
            RetryBackoffSettings::default()
                .next_delay(Duration::from_millis(100), Duration::from_secs(1)),
            Duration::from_millis(200)
        );
    }
}
