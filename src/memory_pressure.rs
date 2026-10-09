//! The node's memory-pressure supervisor.
//!
//! Layer: control plane.
//!
//! - **Owns.** Sampling the allocator, the high and low watermark state machine between them, and
//!   pausing and resuming ingestion across the node as that state changes.
//! - **Depends on.** The runtime handle it pauses, and jemalloc's statistics.
//! - **Must not know.** Which ingestors exist, what they are connected to, or what a message is. It
//!   watches one number and applies one decision through the runtime's own control.

use std::time::Duration;

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use nervix_primitives::{
    sync::CancellationToken,
    time::{MissedTickBehavior, interval, sleep},
};
use strum::Display;
use thiserror::Error;
use tikv_jemalloc_ctl::{epoch, stats};
use tracing::{debug, info, warn};
use typed_builder::TypedBuilder;
use ubyte::ByteUnit;

use crate::runtime::Runtime;

pub const DEFAULT_MEMORY_PRESSURE_CHECK_INTERVAL: Duration = Duration::from_millis(500);
pub const DEFAULT_MEMORY_PRESSURE_RESUME_JITTER: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, TypedBuilder)]
pub struct MemoryPressureConfig {
    #[builder(setter(into))]
    pub high_watermark: ByteUnit,
    #[builder(setter(into))]
    pub low_watermark: ByteUnit,
    #[builder(default = DEFAULT_MEMORY_PRESSURE_CHECK_INTERVAL)]
    pub check_interval: Duration,
    #[builder(default = DEFAULT_MEMORY_PRESSURE_RESUME_JITTER)]
    pub resume_jitter: Duration,
}

impl MemoryPressureConfig {
    pub fn validate(&self) -> error_stack::Result<(), MemoryPressureConfigError> {
        if self.low_watermark >= self.high_watermark {
            return Err(Report::new(
                MemoryPressureConfigError::LowWatermarkNotBelowHigh {
                    low: self.low_watermark,
                    high: self.high_watermark,
                },
            ));
        }
        if self.check_interval.is_zero() {
            return Err(Report::new(MemoryPressureConfigError::ZeroCheckInterval));
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MemoryPressureConfigError {
    #[error("memory low watermark ({low}) must be lower than high watermark ({high})")]
    LowWatermarkNotBelowHigh { low: ByteUnit, high: ByteUnit },
    #[error("memory pressure check interval must be greater than zero")]
    ZeroCheckInterval,
}

/// Why the memory-pressure supervisor cannot start or take a sample. The configuration or
/// allocator failure stays beneath it in the report.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressureError {
    #[error("invalid memory pressure configuration")]
    InvalidConfig,
    #[error("failed to read jemalloc memory usage: {step}")]
    ReadJemalloc { step: JemallocReadStep },
}

/// The jemalloc control a memory sample reads.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq)]
pub enum JemallocReadStep {
    /// Advancing the epoch that refreshes the cached statistics.
    #[strum(serialize = "advance epoch")]
    AdvanceEpoch,
    /// Reading `stats.allocated`.
    #[strum(serialize = "stats.allocated")]
    Allocated,
    /// Reading `stats.resident`.
    #[strum(serialize = "stats.resident")]
    Resident,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryUsageSnapshot {
    pub allocated: u64,
    pub resident: u64,
}

impl MemoryUsageSnapshot {
    fn is_at_or_above_high(self, config: &MemoryPressureConfig) -> bool {
        self.allocated >= config.high_watermark.as_u64()
    }

    fn is_at_or_below_low(self, config: &MemoryPressureConfig) -> bool {
        self.allocated <= config.low_watermark.as_u64()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoryPressureState {
    Healthy,
    Pressured,
}

#[derive(Debug)]
pub struct MemoryPressureController {
    config: MemoryPressureConfig,
}

impl MemoryPressureController {
    pub fn new(config: MemoryPressureConfig) -> error_stack::Result<Self, MemoryPressureError> {
        config
            .validate()
            .change_context(MemoryPressureError::InvalidConfig)?;
        advance_jemalloc_epoch()?;
        Ok(Self { config })
    }

    pub async fn run(self, runtime: Runtime, shutdown: CancellationToken) {
        let mut state = MemoryPressureState::Healthy;
        let mut ticker = interval(self.config.check_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                _ = shutdown.cancelled() => break,
                _ = ticker.tick() => {}
            }

            let snapshot = match jemalloc_memory_usage() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(error = format!("{error:#}"), "memory pressure check failed");
                    continue;
                }
            };

            match state {
                MemoryPressureState::Healthy if snapshot.is_at_or_above_high(&self.config) => {
                    let stopped = runtime.pause_ingestors_for_memory_pressure().await;
                    state = MemoryPressureState::Pressured;
                    warn!(
                        allocated_bytes = snapshot.allocated,
                        resident_bytes = snapshot.resident,
                        allocated = %ByteUnit::from(snapshot.allocated),
                        resident = %ByteUnit::from(snapshot.resident),
                        high_watermark = %self.config.high_watermark,
                        stopped_ingestors = stopped,
                        "memory pressure high watermark reached; paused ingestors"
                    );
                }
                MemoryPressureState::Pressured if snapshot.is_at_or_below_low(&self.config) => {
                    state = self.resume_with_jitter(&runtime, &shutdown).await;
                    if state == MemoryPressureState::Healthy {
                        info!(
                            allocated_bytes = snapshot.allocated,
                            resident_bytes = snapshot.resident,
                            allocated = %ByteUnit::from(snapshot.allocated),
                            resident = %ByteUnit::from(snapshot.resident),
                            low_watermark = %self.config.low_watermark,
                            "memory pressure cleared; ingestors resumed"
                        );
                    }
                }
                _ => {}
            }
        }
    }

    async fn resume_with_jitter(
        &self,
        runtime: &Runtime,
        shutdown: &CancellationToken,
    ) -> MemoryPressureState {
        loop {
            nervix_primitives::task::consume_budget().await;
            if shutdown.is_cancelled() {
                return MemoryPressureState::Pressured;
            }
            let delay = self.resume_delay();
            if !delay.is_zero() {
                nervix_primitives::select! {
                    _ = shutdown.cancelled() => return MemoryPressureState::Pressured,
                    _ = sleep(delay) => {}
                }
            }

            let snapshot = match jemalloc_memory_usage() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(
                        error = format!("{error:#}"),
                        "memory pressure resume check failed"
                    );
                    return MemoryPressureState::Pressured;
                }
            };

            if snapshot.is_at_or_above_high(&self.config) {
                let stopped = runtime.pause_ingestors_for_memory_pressure().await;
                warn!(
                    allocated_bytes = snapshot.allocated,
                    resident_bytes = snapshot.resident,
                    allocated = %ByteUnit::from(snapshot.allocated),
                    resident = %ByteUnit::from(snapshot.resident),
                    high_watermark = %self.config.high_watermark,
                    stopped_ingestors = stopped,
                    "memory pressure returned during jittered resume; paused ingestors"
                );
                return MemoryPressureState::Pressured;
            }

            if !snapshot.is_at_or_below_low(&self.config) {
                debug!(
                    allocated_bytes = snapshot.allocated,
                    resident_bytes = snapshot.resident,
                    allocated = %ByteUnit::from(snapshot.allocated),
                    resident = %ByteUnit::from(snapshot.resident),
                    low_watermark = %self.config.low_watermark,
                    "memory usage rose above low watermark during jittered resume"
                );
                return MemoryPressureState::Pressured;
            }

            if !runtime.resume_one_ingestor_after_memory_pressure().await {
                return MemoryPressureState::Healthy;
            }
        }
    }

    fn resume_delay(&self) -> Duration {
        let jitter_millis =
            u64::try_from(self.config.resume_jitter.as_millis()).unwrap_or(u64::MAX);
        if jitter_millis == 0 {
            Duration::ZERO
        } else {
            Duration::from_millis(fastrand::u64(0..=jitter_millis))
        }
    }
}

pub fn jemalloc_memory_usage() -> error_stack::Result<MemoryUsageSnapshot, MemoryPressureError> {
    advance_jemalloc_epoch()?;
    let allocated = stats::allocated::read().change_context(MemoryPressureError::ReadJemalloc {
        step: JemallocReadStep::Allocated,
    })?;
    let resident = stats::resident::read().change_context(MemoryPressureError::ReadJemalloc {
        step: JemallocReadStep::Resident,
    })?;
    Ok(MemoryUsageSnapshot {
        allocated: allocated.arch_into(),
        resident: resident.arch_into(),
    })
}

/// Refreshes the statistics jemalloc caches, so the reads that follow see current usage.
fn advance_jemalloc_epoch() -> error_stack::Result<(), MemoryPressureError> {
    epoch::advance().change_context(MemoryPressureError::ReadJemalloc {
        step: JemallocReadStep::AdvanceEpoch,
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_low_watermark_at_or_above_high_watermark() {
        let config = MemoryPressureConfig::builder()
            .high_watermark(ByteUnit::Megabyte(10))
            .low_watermark(ByteUnit::Megabyte(10))
            .build();

        let error = config
            .validate()
            .expect_err("equal watermarks must be rejected");
        assert_eq!(
            error.current_context(),
            &MemoryPressureConfigError::LowWatermarkNotBelowHigh {
                low: ByteUnit::Megabyte(10),
                high: ByteUnit::Megabyte(10),
            }
        );
    }

    #[test]
    fn config_rejects_zero_check_interval() {
        let config = MemoryPressureConfig::builder()
            .high_watermark(ByteUnit::Megabyte(100))
            .low_watermark(ByteUnit::Megabyte(40))
            .check_interval(Duration::ZERO)
            .build();

        let error = config
            .validate()
            .expect_err("a zero check interval must be rejected");
        assert_eq!(
            error.current_context(),
            &MemoryPressureConfigError::ZeroCheckInterval
        );
    }

    #[test]
    fn controller_keeps_the_configuration_failure_beneath_its_own() {
        let config = MemoryPressureConfig::builder()
            .high_watermark(ByteUnit::Megabyte(40))
            .low_watermark(ByteUnit::Megabyte(100))
            .build();

        let error = MemoryPressureController::new(config)
            .expect_err("a low watermark above the high one must be rejected");
        assert_eq!(error.current_context(), &MemoryPressureError::InvalidConfig);
        assert_eq!(
            error.downcast_ref::<MemoryPressureConfigError>(),
            Some(&MemoryPressureConfigError::LowWatermarkNotBelowHigh {
                low: ByteUnit::Megabyte(100),
                high: ByteUnit::Megabyte(40),
            })
        );
        assert_eq!(
            format!("{error:#}"),
            "invalid memory pressure configuration: memory low watermark (100MB) must be lower \
             than high watermark (40MB)"
        );
    }

    #[test]
    fn controller_starts_from_a_valid_configuration() {
        let config = MemoryPressureConfig::builder()
            .high_watermark(ByteUnit::Megabyte(100))
            .low_watermark(ByteUnit::Megabyte(40))
            .build();

        let controller =
            MemoryPressureController::new(config).expect("a valid configuration must start");
        assert_eq!(controller.config, config);
    }

    #[test]
    fn memory_usage_reads_the_allocator_statistics() {
        let retained = std::hint::black_box(vec![0_u8; 1 << 20]);
        let retained_bytes = u64::try_from(retained.len()).expect("one MiB fits in u64");

        let usage = jemalloc_memory_usage().expect("the test allocator is jemalloc");

        assert!(usage.allocated >= retained_bytes);
        assert!(usage.resident > 0);
        drop(retained);
    }

    #[test]
    fn snapshot_uses_allocated_bytes_for_watermark_decisions() {
        let config = MemoryPressureConfig::builder()
            .high_watermark(ByteUnit::Megabyte(100))
            .low_watermark(ByteUnit::Megabyte(40))
            .build();

        assert!(
            MemoryUsageSnapshot {
                allocated: ByteUnit::Megabyte(100).as_u64(),
                resident: ByteUnit::Megabyte(10).as_u64(),
            }
            .is_at_or_above_high(&config)
        );
        assert!(
            MemoryUsageSnapshot {
                allocated: ByteUnit::Megabyte(40).as_u64(),
                resident: ByteUnit::Gigabyte(1).as_u64(),
            }
            .is_at_or_below_low(&config)
        );
    }
}
