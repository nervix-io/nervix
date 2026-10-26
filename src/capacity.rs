//! What this node can carry, as it detects it and as its operator overrides it.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The memory-limit setting, its detection from the node's control group and physical
//!   memory, the capacity of the volume a directory lives on, and the group that holds them.
//! - **Depends on.** The vocabulary's setting classes and the operating system's memory and
//!   filesystem reports.
//! - **Must not know.** Command-line parsing, or what the node sizes or refuses with these values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{fmt, path::Path};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use thiserror::Error;
use ubyte::ByteUnit;

#[cfg(target_os = "linux")]
mod linux;

/// Why a memory limit an operator gives cannot be the node's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CapacitySettingsError {
    #[error("{name} must be greater than zero", name = MemoryLimit::NAME)]
    EmptyMemoryLimit,
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

/// What this node can carry, as its operator gives it; an absent value is detected at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacitySettings {
    pub memory_limit: Setting<MemoryLimit>,
}

impl CapacitySettings {
    pub fn resolve(memory_limit: Option<ByteUnit>) -> Result<Self, Report<CapacitySettingsError>> {
        Ok(Self {
            memory_limit: Setting::new(memory_limit, &())?,
        })
    }

    /// The memory limit the operator gave, if any.
    pub fn memory_limit(&self) -> Option<ByteUnit> {
        *self.memory_limit.value()
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
        Self::resolve(None).assured("an absent memory limit is detected, which is always valid")
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
    fn an_operator_limit_wins_over_the_detected_one_and_absence_detects() {
        let detected = DetectedMemory {
            limit: Some(ByteUnit::Gibibyte(8)),
        };
        assert_eq!(
            CapacitySettings::default().effective_memory_limit(detected),
            NodeMemory::Detected(ByteUnit::Gibibyte(8))
        );
        let configured =
            CapacitySettings::resolve(Some(ByteUnit::Gibibyte(2))).expect("two GiB is a limit");
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
        let refused =
            CapacitySettings::resolve(Some(ByteUnit::Byte(0))).expect_err("no memory is no limit");
        assert_eq!(
            format!("{refused:#}"),
            "memory-limit must be greater than zero"
        );
    }

    #[test]
    fn the_volume_of_an_existing_directory_has_a_capacity() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let capacity = volume_capacity(directory.path()).expect("its volume reports statistics");
        assert!(capacity.as_u64() > 0);
        assert!(volume_capacity(&directory.path().join("absent")).is_err());
    }
}
