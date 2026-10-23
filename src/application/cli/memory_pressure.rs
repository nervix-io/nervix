//! The memory-pressure options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the allocation watermarks, the check interval and the
//!   resume jitter, their conversion into the memory-pressure settings group, and the rendering of
//!   that group back into options.
//! - **Depends on.** The memory-pressure settings definitions and the edge's duration and byte
//!   forms.
//! - **Must not know.** What the supervisor pauses.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::Report;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::{
    application::cli_values::{HumanBytes, HumanDuration},
    memory_pressure::{
        MemoryHighWatermark, MemoryLowWatermark, MemoryPressureCheckInterval,
        MemoryPressureConfigError, MemoryPressureResumeJitter, MemoryPressureSettings,
    },
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Memory pressure")]
pub struct MemoryPressureArgs {
    #[arg(
        long = MemoryHighWatermark::NAME,
        env = "NERVIX_MEMORY_HIGH_WATERMARK",
        help = "Allocated jemalloc bytes that pause all ingestors"
    )]
    pub memory_high_watermark: Option<HumanBytes>,
    #[arg(
        long = MemoryLowWatermark::NAME,
        env = "NERVIX_MEMORY_LOW_WATERMARK",
        help = "Allocated jemalloc bytes that allow paused ingestors to resume"
    )]
    pub memory_low_watermark: Option<HumanBytes>,
    #[arg(
        long = MemoryPressureCheckInterval::NAME,
        env = "NERVIX_MEMORY_PRESSURE_CHECK_INTERVAL",
        default_value_t = HumanDuration(MemoryPressureCheckInterval::default_value(&())),
        help = "Interval between jemalloc memory pressure checks"
    )]
    pub memory_pressure_check_interval: HumanDuration,
    #[arg(
        long = MemoryPressureResumeJitter::NAME,
        env = "NERVIX_MEMORY_PRESSURE_RESUME_JITTER",
        default_value_t = HumanDuration(MemoryPressureResumeJitter::default_value(&())),
        help = "Maximum jitter before each paused ingestor resume attempt"
    )]
    pub memory_pressure_resume_jitter: HumanDuration,
}

impl MemoryPressureArgs {
    pub(super) fn settings(
        &self,
    ) -> Result<MemoryPressureSettings, Report<MemoryPressureConfigError>> {
        MemoryPressureSettings::resolve(
            self.memory_high_watermark.map(|watermark| watermark.0),
            self.memory_low_watermark.map(|watermark| watermark.0),
            Some(self.memory_pressure_check_interval.0),
            Some(self.memory_pressure_resume_jitter.0),
        )
    }

    pub(super) fn from_settings(settings: &MemoryPressureSettings) -> Self {
        Self {
            memory_high_watermark: settings.high_watermark.value().map(HumanBytes),
            memory_low_watermark: settings.low_watermark.value().map(HumanBytes),
            memory_pressure_check_interval: HumanDuration(*settings.check_interval.value()),
            memory_pressure_resume_jitter: HumanDuration(*settings.resume_jitter.value()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        if let Some(watermark) = self.memory_high_watermark {
            command_line.option(MemoryHighWatermark::NAME, watermark);
        }
        if let Some(watermark) = self.memory_low_watermark {
            command_line.option(MemoryLowWatermark::NAME, watermark);
        }
        command_line.option(
            MemoryPressureCheckInterval::NAME,
            self.memory_pressure_check_interval,
        );
        command_line.option(
            MemoryPressureResumeJitter::NAME,
            self.memory_pressure_resume_jitter,
        );
    }
}
