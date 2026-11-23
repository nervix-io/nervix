//! Local Roto UDF execution capabilities.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The per-call watchdog setting of compiled UDFs, its single default and its validated
//!   construction.
//! - **Depends on.** Vocabulary setting classes and durations.
//! - **Must not know.** Graphs, command-line arguments, the committed compile budget or the node
//!   that supplies its settings.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns Roto UDF execution capabilities")
)]

use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};

#[derive(Debug, thiserror::Error)]
pub enum UdfSettingsError {
    #[error("udf-watchdog must be positive")]
    ZeroWatchdog,
}

/// How long one call of a compiled UDF may run on this node before its batch fails. The watchdog
/// is judged when native code returns, so it bounds nothing a call that never returns holds.
pub struct UdfWatchdog;

impl SettingDefinition for UdfWatchdog {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = UdfSettingsError;
    const NAME: &'static str = "udf-watchdog";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(5)
    }
    /// The watchdog is only compared with a call's elapsed time and printed in its expiry, so any
    /// positive duration is one it can hold.
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        if value.is_zero() {
            return Err(Report::new(UdfSettingsError::ZeroWatchdog));
        }
        Ok(())
    }
}

/// The UDF settings one node resolves before it compiles any domain's UDFs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdfSettings {
    watchdog: Setting<UdfWatchdog>,
}

impl UdfSettings {
    pub fn resolve(watchdog: Option<Duration>) -> Result<Self, Report<UdfSettingsError>> {
        Ok(Self {
            watchdog: Setting::resolve(watchdog, &())?,
        })
    }

    pub fn watchdog(self) -> Duration {
        *self.watchdog.value()
    }
}

impl Default for UdfSettings {
    fn default() -> Self {
        Self::resolve(None).assured("the UDF watchdog default is positive")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_watchdog_defaults_to_five_seconds_and_keeps_an_explicit_value() {
        assert_eq!(UdfSettings::default().watchdog(), Duration::from_secs(5));
        let settings = UdfSettings::resolve(Some(Duration::from_nanos(1)))
            .assured("the shortest positive watchdog validates");
        assert_eq!(settings.watchdog(), Duration::from_nanos(1));
        let settings = UdfSettings::resolve(Some(Duration::new(u64::MAX, 999_999_999)))
            .assured("the longest duration is a watchdog a call can be compared with");
        assert_eq!(settings.watchdog(), Duration::new(u64::MAX, 999_999_999));
    }

    #[test]
    fn a_zero_watchdog_is_refused_before_any_udf_compiles() {
        let refused = UdfSettings::resolve(Some(Duration::ZERO))
            .expect_err("no call returns within a zero watchdog");
        assert!(matches!(
            refused.current_context(),
            UdfSettingsError::ZeroWatchdog
        ));
        assert_eq!(format!("{refused:#}"), "udf-watchdog must be positive");
    }
}
