//! The CPU model and the GPUs this node detects at startup.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading the CPU model through the maintained system-information crate, and the GPU
//!   inventory and driver version through NVML loaded dynamically, each as a typed property with a
//!   reason for every value that was not detected.
//! - **Depends on.** The vocabulary's property types, `sysinfo` and `nvml-wrapper`.
//! - **Must not know.** Where a probe runs, how long its caller waits, or what the record is for.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::num::NonZeroU64;

use nervix_models::{
    Detected, GpuDevice, GpuDevices, GpuInventory, MAX_PROPERTY_LIST_ITEMS, PropertyText,
    PropertyValueError, UndetectedReason,
};
use nvml_wrapper::{Nvml, error::NvmlError};
use tracing::debug;

/// The soname every NVIDIA driver installs and the NVIDIA container toolkit mounts; the
/// unversioned name comes only with development packages.
const NVML_LIBRARY: &str = "libnvidia-ml.so.1";

/// The model of this host's first CPU, as the operating system reports it. Every CPU of one host
/// is one model in practice; the first stands for them.
pub fn detect_cpu_model() -> Detected<PropertyText> {
    let mut system = sysinfo::System::new();
    system.refresh_cpu_list(sysinfo::CpuRefreshKind::nothing());
    let Some(cpu) = system.cpus().first() else {
        return Detected::Undetected(UndetectedReason::NotReported);
    };
    detected_text(cpu.brand())
}

/// The GPUs NVML reports, in device order, and the driver serving them. Without the NVML library,
/// or with a library whose driver is not running, the node has none.
pub fn detect_gpus() -> GpuInventory {
    let nvml = match Nvml::builder().lib_path(NVML_LIBRARY.as_ref()).init() {
        Ok(nvml) => nvml,
        Err(error) => {
            let reason = undetected_by(&error);
            debug!(%error, ?reason, "NVML reports no GPU inventory");
            return GpuInventory::Undetected(reason);
        }
    };
    let driver = match nvml.sys_driver_version() {
        Ok(version) => detected_text(&version),
        Err(error) => {
            debug!(%error, "NVML reports no driver version");
            Detected::Undetected(undetected_by(&error))
        }
    };
    let count = match nvml.device_count() {
        Ok(count) => count,
        Err(error) => {
            debug!(%error, "NVML reports no device count");
            return GpuInventory::Undetected(undetected_by(&error));
        }
    };
    if count == 0 {
        return GpuInventory::NoDevice { driver };
    }
    let listed = match usize::try_from(count) {
        Ok(count) => count <= MAX_PROPERTY_LIST_ITEMS,
        Err(_) => false,
    };
    if !listed {
        return GpuInventory::Undetected(UndetectedReason::ExceedsRecordBound);
    }
    let mut devices = Vec::new();
    for index in 0..count {
        devices.push(device(&nvml, index));
    }
    match GpuDevices::try_from(devices) {
        Ok(devices) => GpuInventory::Devices { devices, driver },
        Err(_) => GpuInventory::Undetected(UndetectedReason::ExceedsRecordBound),
    }
}

/// The model and memory of the device NVML lists at `index`.
fn device(nvml: &Nvml, index: u32) -> GpuDevice {
    let device = match nvml.device_by_index(index) {
        Ok(device) => device,
        Err(error) => {
            debug!(%error, index, "NVML refused a listed device");
            let reason = undetected_by(&error);
            return GpuDevice {
                model: Detected::Undetected(reason),
                memory_bytes: Detected::Undetected(reason),
            };
        }
    };
    let model = match device.name() {
        Ok(name) => detected_text(&name),
        Err(error) => {
            debug!(%error, index, "NVML reports no device name");
            Detected::Undetected(undetected_by(&error))
        }
    };
    let memory_bytes = match device.memory_info() {
        Ok(memory) => match NonZeroU64::new(memory.total) {
            Some(total) => Detected::Value(total),
            None => Detected::Undetected(UndetectedReason::NotReported),
        },
        Err(error) => {
            debug!(%error, index, "NVML reports no device memory");
            Detected::Undetected(undetected_by(&error))
        }
    };
    GpuDevice {
        model,
        memory_bytes,
    }
}

/// Detected text, nothing when the source reported none, or the bound it breaks.
fn detected_text(text: &str) -> Detected<PropertyText> {
    match PropertyText::new(text.trim()) {
        Ok(text) => Detected::Value(text),
        Err(PropertyValueError::Empty) => Detected::Undetected(UndetectedReason::NotReported),
        Err(_) => Detected::Undetected(UndetectedReason::ExceedsRecordBound),
    }
}

/// Why NVML answered `error` instead of a value.
fn undetected_by(error: &NvmlError) -> UndetectedReason {
    match error {
        NvmlError::LibloadingError(_)
        | NvmlError::FailedToLoadSymbol(_)
        | NvmlError::LibraryNotFound
        | NvmlError::FunctionNotFound => UndetectedReason::LibraryUnavailable,
        NvmlError::DriverNotLoaded => UndetectedReason::DriverUnavailable,
        _ => UndetectedReason::QueryFailed,
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::MAX_PROPERTY_TEXT_BYTES;

    use super::*;

    #[test]
    fn each_nvml_answer_names_why_a_gpu_property_is_undetected() {
        for missing in [
            NvmlError::LibraryNotFound,
            NvmlError::FunctionNotFound,
            NvmlError::FailedToLoadSymbol("nvmlInit_v2".to_string()),
        ] {
            assert_eq!(
                undetected_by(&missing),
                UndetectedReason::LibraryUnavailable
            );
        }
        assert_eq!(
            undetected_by(&NvmlError::DriverNotLoaded),
            UndetectedReason::DriverUnavailable
        );
        for refused in [
            NvmlError::NoPermission,
            NvmlError::NotSupported,
            NvmlError::Timeout,
        ] {
            assert_eq!(undetected_by(&refused), UndetectedReason::QueryFailed);
        }
    }

    #[test]
    fn detected_text_is_trimmed_absent_when_empty_and_bounded() {
        assert_eq!(
            detected_text("  NVIDIA L4 \n"),
            Detected::Value(PropertyText::new("NVIDIA L4").expect("short printable text"))
        );
        assert_eq!(
            detected_text(" \t "),
            Detected::Undetected(UndetectedReason::NotReported)
        );
        assert_eq!(
            detected_text(&"x".repeat(MAX_PROPERTY_TEXT_BYTES + 1)),
            Detected::Undetected(UndetectedReason::ExceedsRecordBound)
        );
        assert_eq!(
            detected_text("GPU\u{2028}0"),
            Detected::Undetected(UndetectedReason::ExceedsRecordBound)
        );
    }

    #[test]
    fn the_host_probes_answer_a_value_or_why_there_is_none() {
        match detect_cpu_model() {
            Detected::Value(model) => assert!(!model.as_str().is_empty()),
            Detected::Undetected(reason) => assert_eq!(reason, UndetectedReason::NotReported),
        }
        let gpus = detect_gpus();
        assert_eq!(gpus.count(), gpus.devices().len());
    }
}
