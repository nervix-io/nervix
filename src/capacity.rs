//! What this node can carry, as it detects it and as its operator overrides it.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** CPU and memory overrides and detection, the CPU model and GPU detection, the node
//!   detection timeout, and filesystem-volume capacity.
//! - **Depends on.** The vocabulary's setting classes, the operating system's memory and
//!   filesystem reports, and the maintained system-information and NVML crates.
//! - **Must not know.** Command-line parsing, or what the node sizes or refuses with these values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{fmt, num::NonZeroU64, path::Path, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use thiserror::Error;
use ubyte::ByteUnit;

mod hardware;
#[cfg(target_os = "linux")]
mod linux;

pub use self::hardware::{detect_cpu_model, detect_gpus};

/// Why a memory limit an operator gives cannot be the node's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CapacitySettingsError {
    #[error("{name} must be greater than zero", name = MemoryLimit::NAME)]
    EmptyMemoryLimit,
    #[error("{name} must be greater than zero", name = CpuCount::NAME)]
    EmptyCpuCount,
    #[error("{name} must be greater than zero", name = NodeDetectionTimeout::NAME)]
    EmptyDetectionTimeout,
}

/// The parallel CPU capacity of this process, detected with its control-group quota by default.
pub struct CpuCount;

impl SettingDefinition for CpuCount {
    type Class = NodeCapabilities;
    type Value = Option<u64>;
    type Context = ();
    type Error = CapacitySettingsError;

    const NAME: &'static str = "cpu-count";
    const DERIVATION: Option<&'static str> =
        Some("the process's cgroup-aware available parallelism");

    fn default_value(_: &()) -> Self::Value {
        None
    }

    fn validate(value: &Self::Value, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == Some(0) {
            return Err(Report::new(CapacitySettingsError::EmptyCpuCount));
        }
        Ok(())
    }
}

impl CpuCount {
    /// Queries the operating system through the execution primitive boundary. Failure is unknown.
    pub fn detect() -> Option<NonZeroU64> {
        match nervix_primitives::thread::available_parallelism() {
            Ok(count) => {
                NonZeroU64::new(u64::try_from(count.get()).assured("host CPU counts fit u64"))
            }
            Err(_) => None,
        }
    }
}

/// The memory this node may use. Without a value the node detects it at startup.
pub struct MemoryLimit;

impl SettingDefinition for MemoryLimit {
    type Class = NodeCapabilities;
    type Value = Option<ByteUnit>;
    type Context = ();
    type Error = CapacitySettingsError;

    const NAME: &'static str = "memory-limit";
    const DERIVATION: Option<&'static str> = Some(
        "the smallest memory limit of the node's control group and its ancestors, or its physical \
         memory when no limit is smaller",
    );

    fn default_value(_: &()) -> Option<ByteUnit> {
        None
    }

    fn validate(value: &Option<ByteUnit>, _: &()) -> Result<(), Report<CapacitySettingsError>> {
        if *value == Some(ByteUnit::Byte(0)) {
            return Err(Report::new(CapacitySettingsError::EmptyMemoryLimit));
        }
        Ok(())
    }
}

/// How long the startup preflight waits for this node to detect its properties. A probe that has
/// not answered by then leaves its properties undetected, and the node starts without it.
pub struct NodeDetectionTimeout;

impl SettingDefinition for NodeDetectionTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = CapacitySettingsError;

    const NAME: &'static str = "node-detection-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(30)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<CapacitySettingsError>> {
        if value.is_zero() {
            return Err(Report::new(CapacitySettingsError::EmptyDetectionTimeout));
        }
        Ok(())
    }
}

/// What this node can carry, as its operator gives it; an absent value is detected at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacitySettings {
    pub memory_limit: Setting<MemoryLimit>,
    pub cpu_count: Setting<CpuCount>,
    pub detection_timeout: Setting<NodeDetectionTimeout>,
}

impl CapacitySettings {
    pub fn resolve(
        memory_limit: Option<ByteUnit>,
        cpu_count: Option<u64>,
        detection_timeout: Option<Duration>,
    ) -> Result<Self, Report<CapacitySettingsError>> {
        Ok(Self {
            memory_limit: Setting::new(memory_limit, &())?,
            cpu_count: Setting::new(cpu_count, &())?,
            detection_timeout: Setting::resolve(detection_timeout, &())?,
        })
    }

    /// How long the startup preflight waits for this node's properties.
    pub fn detection_timeout(&self) -> Duration {
        *self.detection_timeout.value()
    }

    /// The memory limit the operator gave, if any.
    pub fn memory_limit(&self) -> Option<ByteUnit> {
        *self.memory_limit.value()
    }

    pub fn effective_cpu_count(&self, detected: Option<NonZeroU64>) -> Option<NonZeroU64> {
        match *self.cpu_count.value() {
            Some(count) => NonZeroU64::new(count),
            None => detected,
        }
    }

    /// The memory this node may use: the operator's value, or what the operating system reports.
    /// A platform that reports nothing leaves the limit unknown.
    pub fn effective_memory_limit(&self, detected: DetectedMemory) -> NodeMemory {
        match self.memory_limit() {
            Some(limit) => NodeMemory::Configured(limit),
            None => match detected.limit {
                Some(limit) => NodeMemory::Detected(limit),
                None => NodeMemory::Unknown,
            },
        }
    }
}

impl Default for CapacitySettings {
    fn default() -> Self {
        Self::resolve(None, None, None)
            .assured("absent capacity overrides are detected and the timeout has its default")
    }
}

/// The memory limit the operating system reports for this process, if it reports one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DetectedMemory {
    pub limit: Option<ByteUnit>,
}

impl DetectedMemory {
    /// Reads the smallest of the control-group limits on this process's path and its physical
    /// memory. It reads files the kernel serves, so the caller runs it as filesystem work.
    pub fn detect() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self {
                limit: linux::memory_limit(),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self { limit: None }
        }
    }
}

/// The memory this node may use, and where that value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeMemory {
    /// The operator gave it.
    Configured(ByteUnit),
    /// The operating system reported it.
    Detected(ByteUnit),
    /// Neither gave one, so nothing can be checked against it.
    Unknown,
}

impl fmt::Display for NodeMemory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configured(limit) => write!(formatter, "{} ({limit})", MemoryLimit::NAME),
            Self::Detected(limit) => write!(formatter, "the detected memory limit of {limit}"),
            Self::Unknown => formatter.write_str("an unknown memory limit"),
        }
    }
}

impl NodeMemory {
    pub fn limit(self) -> Option<ByteUnit> {
        match self {
            Self::Configured(limit) | Self::Detected(limit) => Some(limit),
            Self::Unknown => None,
        }
    }
}

/// Why the capacity of a directory's volume could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum VolumeCapacityError {
    #[error("the volume's statistics could not be read")]
    Statistics,
    #[error("the volume reports more bytes than a byte count holds")]
    Unaddressable,
}

/// The total size of the filesystem `directory` lives on, whatever is free of it now.
pub fn volume_capacity(directory: &Path) -> Result<ByteUnit, Report<VolumeCapacityError>> {
    let statistics = nix::sys::statvfs::statvfs(directory)
        .map_err(|error| Report::new(error).change_context(VolumeCapacityError::Statistics))?;
    // The kernel reports both counts in platform-sized integers, so they multiply as 128 bits.
    let blocks = u128::from(statistics.blocks());
    let block_size = u128::from(statistics.fragment_size());
    let bytes = blocks
        .checked_mul(block_size)
        .assured("two counts of at most 64 bits multiply within 128 bits");
    let bytes = u64::try_from(bytes)
        .map_err(|error| Report::new(error).change_context(VolumeCapacityError::Unaddressable))?;
    Ok(ByteUnit::Byte(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_overrides_win_and_a_failed_query_remains_unknown() {
        let detected = NonZeroU64::new(2);
        assert_eq!(
            CapacitySettings::default().effective_cpu_count(detected),
            detected
        );
        assert_eq!(CapacitySettings::default().effective_cpu_count(None), None);
        let configured =
            CapacitySettings::resolve(None, Some(4), None).expect("positive CPU count");
        assert_eq!(configured.effective_cpu_count(detected), NonZeroU64::new(4));
        let refused =
            CapacitySettings::resolve(None, Some(0), None).expect_err("zero CPUs are refused");
        assert_eq!(
            refused.current_context(),
            &CapacitySettingsError::EmptyCpuCount
        );
    }

    #[test]
    fn an_operator_limit_wins_over_the_detected_one_and_absence_detects() {
        let detected = DetectedMemory {
            limit: Some(ByteUnit::Gibibyte(8)),
        };
        assert_eq!(
            CapacitySettings::default().effective_memory_limit(detected),
            NodeMemory::Detected(ByteUnit::Gibibyte(8))
        );
        let configured = CapacitySettings::resolve(Some(ByteUnit::Gibibyte(2)), None, None)
            .expect("two GiB is a limit");
        assert_eq!(
            configured.effective_memory_limit(detected),
            NodeMemory::Configured(ByteUnit::Gibibyte(2))
        );
        assert_eq!(
            CapacitySettings::default().effective_memory_limit(DetectedMemory { limit: None }),
            NodeMemory::Unknown
        );
        assert_eq!(NodeMemory::Unknown.limit(), None);
    }

    #[test]
    fn a_zero_memory_limit_is_refused_naming_the_setting() {
        let refused = CapacitySettings::resolve(Some(ByteUnit::Byte(0)), None, None)
            .expect_err("no memory is no limit");
        assert_eq!(
            format!("{refused:#}"),
            "memory-limit must be greater than zero"
        );
    }

    #[test]
    fn a_zero_detection_timeout_is_refused_naming_the_setting_and_absence_waits_the_default() {
        let refused = CapacitySettings::resolve(None, None, Some(Duration::ZERO))
            .expect_err("no time is no deadline");
        assert_eq!(
            format!("{refused:#}"),
            "node-detection-timeout must be greater than zero"
        );
        let defaulted = CapacitySettings::resolve(None, None, None).expect("defaults are valid");
        assert_eq!(defaulted.detection_timeout(), Duration::from_secs(30));
        let given = CapacitySettings::resolve(None, None, Some(Duration::from_millis(1)))
            .expect("a positive deadline is valid");
        assert_eq!(given.detection_timeout(), Duration::from_millis(1));
    }

    #[test]
    fn the_volume_of_an_existing_directory_has_a_capacity() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let capacity = volume_capacity(directory.path()).expect("its volume reports statistics");
        assert!(capacity.as_u64() > 0);
        assert!(volume_capacity(&directory.path().join("absent")).is_err());
    }
}
