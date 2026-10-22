//! The watermarks and cadence of the memory-pressure supervisor, declared as settings.
//!
//! Layer: control plane.
//!
//! - **Owns.** The setting definitions of the high and low allocation watermarks, the check
//!   interval and the resume jitter, their single defaults, the pairing of the two watermarks, and
//!   the group that holds them.
//! - **Depends on.** The vocabulary's setting classes and the supervisor's configuration.
//! - **Must not know.** Command-line parsing, or what the supervisor pauses.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::{convert::Infallible, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use ubyte::ByteUnit;

use super::{
    DEFAULT_MEMORY_PRESSURE_CHECK_INTERVAL, DEFAULT_MEMORY_PRESSURE_RESUME_JITTER,
    MemoryPressureConfig, MemoryPressureConfigError,
};

/// The allocated bytes at which the node pauses every ingestor. Without it, and without the low
/// watermark, the node runs no memory-pressure supervisor.
pub struct MemoryHighWatermark;

impl SettingDefinition for MemoryHighWatermark {
    type Class = NodeCapabilities;
    type Value = Option<ByteUnit>;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "memory-high-watermark";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Option<ByteUnit> {
        None
    }

    fn validate(_: &Option<ByteUnit>, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The allocated bytes at or below which paused ingestors resume. It is set exactly when the high
/// watermark is, and below it.
pub struct MemoryLowWatermark;

impl SettingDefinition for MemoryLowWatermark {
    type Class = NodeCapabilities;
    type Value = Option<ByteUnit>;
    /// The high watermark the low one pairs with.
    type Context = Option<ByteUnit>;
    type Error = MemoryPressureConfigError;

    const NAME: &'static str = "memory-low-watermark";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &Option<ByteUnit>) -> Option<ByteUnit> {
        None
    }

    fn validate(
        low: &Option<ByteUnit>,
        high: &Option<ByteUnit>,
    ) -> Result<(), Report<MemoryPressureConfigError>> {
        match (*high, *low) {
            (None, None) => Ok(()),
            (Some(_), None) => Err(Report::new(
                MemoryPressureConfigError::HighWatermarkWithoutLow,
            )),
            (None, Some(_)) => Err(Report::new(
                MemoryPressureConfigError::LowWatermarkWithoutHigh,
            )),
            (Some(high), Some(low)) if low >= high => Err(Report::new(
                MemoryPressureConfigError::LowWatermarkNotBelowHigh { low, high },
            )),
            (Some(_), Some(_)) => Ok(()),
        }
    }
}

/// How often the supervisor samples the allocator.
pub struct MemoryPressureCheckInterval;

impl SettingDefinition for MemoryPressureCheckInterval {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "memory-pressure-check-interval";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        DEFAULT_MEMORY_PRESSURE_CHECK_INTERVAL
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The most jitter before each paused ingestor tries to resume.
pub struct MemoryPressureResumeJitter;

impl SettingDefinition for MemoryPressureResumeJitter {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "memory-pressure-resume-jitter";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        DEFAULT_MEMORY_PRESSURE_RESUME_JITTER
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The memory-pressure supervisor's watermarks and cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryPressureSettings {
    pub high_watermark: Setting<MemoryHighWatermark>,
    pub low_watermark: Setting<MemoryLowWatermark>,
    pub check_interval: Setting<MemoryPressureCheckInterval>,
    pub resume_jitter: Setting<MemoryPressureResumeJitter>,
}

impl MemoryPressureSettings {
    /// Resolves each value that is not given through its definition's default, pairs the
    /// watermarks, and validates the supervisor's configuration when they are set.
    pub fn resolve(
        high_watermark: Option<ByteUnit>,
        low_watermark: Option<ByteUnit>,
        check_interval: Option<Duration>,
        resume_jitter: Option<Duration>,
    ) -> Result<Self, Report<MemoryPressureConfigError>> {
        let high_watermark = Setting::<MemoryHighWatermark>::new(high_watermark, &())
            .assured("the high watermark declares no validation of its own");
        let low_watermark = Setting::new(low_watermark, high_watermark.value())?;
        let check_interval = Setting::resolve(check_interval, &())
            .assured("the check interval declares no validation of its own");
        let resume_jitter = Setting::resolve(resume_jitter, &())
            .assured("the resume jitter declares no validation of its own");
        let settings = Self {
            high_watermark,
            low_watermark,
            check_interval,
            resume_jitter,
        };
        if let Some(config) = settings.config() {
            config.validate()?;
        }
        Ok(settings)
    }

    /// The supervisor's configuration, absent when no watermarks are set and the node runs no
    /// supervisor.
    pub fn config(&self) -> Option<MemoryPressureConfig> {
        let (Some(high_watermark), Some(low_watermark)) =
            (*self.high_watermark.value(), *self.low_watermark.value())
        else {
            return None;
        };
        Some(MemoryPressureConfig {
            high_watermark,
            low_watermark,
            check_interval: *self.check_interval.value(),
            resume_jitter: *self.resume_jitter.value(),
        })
    }
}

impl Default for MemoryPressureSettings {
    fn default() -> Self {
        Self::resolve(None, None, None, None)
            .assured("without watermarks the memory-pressure defaults have nothing to pair")
    }
}
