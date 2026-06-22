use std::time::Duration;

use thiserror::Error;
use tikv_jemalloc_ctl::{epoch, stats};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tokio_util::sync::CancellationToken;
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
    pub fn validate(&self) -> Result<(), MemoryPressureConfigError> {
        if self.low_watermark >= self.high_watermark {
            return Err(MemoryPressureConfigError::LowWatermarkNotBelowHigh {
                low: self.low_watermark,
                high: self.high_watermark,
            });
        }
        if self.check_interval.is_zero() {
            return Err(MemoryPressureConfigError::ZeroCheckInterval);
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

#[derive(Debug, Error)]
pub enum MemoryPressureError {
    #[error("{0}")]
    InvalidConfig(#[from] MemoryPressureConfigError),
    #[error("failed to read jemalloc memory usage: {0}")]
    ReadJemalloc(String),
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
    pub fn new(config: MemoryPressureConfig) -> Result<Self, MemoryPressureError> {
        config.validate()?;
        epoch::advance().map_err(|error| MemoryPressureError::ReadJemalloc(error.to_string()))?;
        Ok(Self { config })
    }

    pub async fn run(self, runtime: Runtime, shutdown: CancellationToken) {
        let mut state = MemoryPressureState::Healthy;
        let mut ticker = interval(self.config.check_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticker.tick() => {}
            }

            let snapshot = match jemalloc_memory_usage() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(error = %error, "memory pressure check failed");
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
            tokio::task::consume_budget().await;
            if shutdown.is_cancelled() {
                return MemoryPressureState::Pressured;
            }
            let delay = self.resume_delay();
            if !delay.is_zero() {
                tokio::select! {
                    _ = shutdown.cancelled() => return MemoryPressureState::Pressured,
                    _ = sleep(delay) => {}
                }
            }

            let snapshot = match jemalloc_memory_usage() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(error = %error, "memory pressure resume check failed");
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

            match runtime.resume_one_ingestor_after_memory_pressure().await {
                Ok(true) => {}
                Ok(false) => return MemoryPressureState::Healthy,
                Err(error) => {
                    warn!(error = %error, "failed to resume ingestor after memory pressure");
                }
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

pub fn jemalloc_memory_usage() -> Result<MemoryUsageSnapshot, MemoryPressureError> {
    epoch::advance().map_err(|error| MemoryPressureError::ReadJemalloc(error.to_string()))?;
    let allocated = stats::allocated::read()
        .map_err(|error| MemoryPressureError::ReadJemalloc(error.to_string()))?
        .try_into()
        .unwrap_or(u64::MAX);
    let resident = stats::resident::read()
        .map_err(|error| MemoryPressureError::ReadJemalloc(error.to_string()))?
        .try_into()
        .unwrap_or(u64::MAX);
    Ok(MemoryUsageSnapshot {
        allocated,
        resident,
    })
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

        assert!(matches!(
            config.validate(),
            Err(MemoryPressureConfigError::LowWatermarkNotBelowHigh { .. })
        ));
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
