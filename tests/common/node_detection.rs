//! The detection double a scenario node starts with in place of probing its host.
//!
//! Outside the layer order: a harness module.
//! - **Owns.** The properties a scenario node reports: a CPU-only host by default, with the GPU
//!   count, the CUDA execution provider and the CPU model a scenario names instead.
//! - **Depends on.** The vocabulary's property types and the composition root's detection input.
//! - **Must not know.** How a deployed node probes its host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "supplies scenario node properties through the composition root"
    )
)]

use std::{collections::BTreeMap, io, num::NonZeroU64};

use nervix_models::{
    CudaProviderRegistration, Detected, ExecutionProviders, GpuDevice, GpuDevices, GpuInventory,
    NodeProperties, OnnxRuntime, OnnxRuntimeDetection, PropertyText, UndetectedReason,
};
use nervix_server::application::NodeDetection;

/// The memory every reported GPU has.
const REPORTED_GPU_MEMORY_BYTES: u64 = 16 << 30;

/// The properties a node reports when a scenario named `reports` for it, each by its name as
/// `DESCRIBE CLUSTER` prints it: `gpu.count`, `onnx.cuda` and `cpu.model`.
pub(crate) fn detection_double(reports: &BTreeMap<String, String>) -> io::Result<NodeDetection> {
    let mut properties = NodeProperties {
        architecture: text(std::env::consts::ARCH)?,
        cpu_model: Detected::Value(text("Scenario CPU")?),
        gpus: GpuInventory::Undetected(UndetectedReason::LibraryUnavailable),
        onnx: OnnxRuntimeDetection::Loaded(OnnxRuntime {
            version: Detected::Value(text("scenario")?),
            providers: Detected::Value(providers(&["CPUExecutionProvider"])?),
            cuda: CudaProviderRegistration::NotInLibrary,
        }),
    };
    for (property, value) in reports {
        match property.as_str() {
            "gpu.count" => properties.gpus = gpus(value)?,
            "onnx.cuda" => properties.onnx = onnx(value)?,
            "cpu.model" => properties.cpu_model = Detected::Value(text(value)?),
            other => {
                return Err(io::Error::other(format!(
                    "a detection double reports gpu.count, onnx.cuda or cpu.model, not '{other}'"
                )));
            }
        }
    }
    Ok(NodeDetection::Supplied(properties))
}

/// `count` GPUs of one model, with a driver; none when the count is zero.
fn gpus(count: &str) -> io::Result<GpuInventory> {
    let count = count
        .parse::<usize>()
        .map_err(|error| io::Error::other(format!("gpu.count '{count}': {error}")))?;
    let driver = Detected::Value(text("scenario-driver")?);
    if count == 0 {
        return Ok(GpuInventory::NoDevice { driver });
    }
    let memory = NonZeroU64::new(REPORTED_GPU_MEMORY_BYTES)
        .ok_or_else(|| io::Error::other("a reported GPU has memory"))?;
    let mut devices = Vec::new();
    for _ in 0..count {
        devices.push(GpuDevice {
            model: Detected::Value(text("Scenario GPU")?),
            memory_bytes: Detected::Value(memory),
        });
    }
    let devices = GpuDevices::try_from(devices)
        .map_err(|error| io::Error::other(format!("gpu.count {count}: {error}")))?;
    Ok(GpuInventory::Devices { devices, driver })
}

/// A runtime whose CUDA execution provider registers, or one whose library has none.
fn onnx(cuda: &str) -> io::Result<OnnxRuntimeDetection> {
    let (names, registration): (&[&str], _) = match cuda {
        "true" => (
            &["CUDAExecutionProvider", "CPUExecutionProvider"],
            CudaProviderRegistration::Registered,
        ),
        "false" => (
            &["CPUExecutionProvider"],
            CudaProviderRegistration::NotInLibrary,
        ),
        other => {
            return Err(io::Error::other(format!(
                "onnx.cuda is true or false, not '{other}'"
            )));
        }
    };
    Ok(OnnxRuntimeDetection::Loaded(OnnxRuntime {
        version: Detected::Value(text("scenario")?),
        providers: Detected::Value(providers(names)?),
        cuda: registration,
    }))
}

fn providers(names: &[&str]) -> io::Result<ExecutionProviders> {
    let mut providers = Vec::new();
    for name in names {
        providers.push(text(name)?);
    }
    ExecutionProviders::try_from(providers).map_err(io::Error::other)
}

fn text(text: &str) -> io::Result<PropertyText> {
    PropertyText::new(text).map_err(io::Error::other)
}
