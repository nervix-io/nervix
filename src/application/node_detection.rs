//! What a node detects about itself at startup, as its composition root chooses: this host's own
//! facts, or properties the root supplies in their place.
//!
//! Layer: edges.
//!
//! - **Owns.** The detection input of the composition root, and running the host's probes on the
//!   bulk workers within the node detection timeout, recording every probe that fails or does not
//!   answer in time as undetected with its reason. Detection never refuses the start.
//! - **Depends on.** The capacity module's hardware probes, the inferencer's ONNX Runtime probe,
//!   the bounded executor and the vocabulary's property types.
//! - **Must not know.** Consensus, how the record is published, or what placement does with it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_execution::{CpuClass, Executor, MemoryClass};
use nervix_models::{
    Detected, GpuInventory, NodeProperties, OnnxRuntimeDetection, PropertyText, UndetectedReason,
};
use thiserror::Error;
use tracing::info;

use super::startup::STORAGE_JOB_RESERVATION_BYTES;

/// Where a node's properties come from.
#[derive(Debug, Clone, Default)]
pub enum NodeDetection {
    /// Detect every property from this host and this process.
    #[default]
    Host,
    /// These properties, which the composition root supplies in place of detection, as a test
    /// harness supplies a double.
    Supplied(NodeProperties),
}

/// Why a host probe answered nothing.
#[derive(Debug, Error)]
enum ProbeError {
    #[error("the node's bounded execution did not run the {probe} probe")]
    Execution { probe: &'static str },
}

impl NodeDetection {
    /// The node's properties: the supplied ones, or what this host's probes report within
    /// `timeout`.
    pub(in crate::application) async fn detect(
        &self,
        executor: &Executor,
        timeout: Duration,
    ) -> NodeProperties {
        match self {
            Self::Supplied(properties) => properties.clone(),
            Self::Host => HostDetection::run(executor, timeout).await,
        }
    }
}

/// The host's probes, run side by side so a slow one delays none of the others.
struct HostDetection;

impl HostDetection {
    async fn run(executor: &Executor, timeout: Duration) -> NodeProperties {
        let cpu_model = nervix_primitives::time::timeout(
            timeout,
            Self::probe(executor, "CPU model", crate::capacity::detect_cpu_model),
        );
        let gpus = nervix_primitives::time::timeout(
            timeout,
            Self::probe(executor, "GPU", crate::capacity::detect_gpus),
        );
        let onnx =
            nervix_primitives::time::timeout(timeout, crate::runtime::probe_onnx_runtime(executor));
        let (cpu_model, gpus, onnx) = futures_util::future::join3(cpu_model, gpus, onnx).await;
        let cpu_model = match cpu_model {
            Ok(Ok(cpu_model)) => cpu_model,
            Ok(Err(error)) => {
                info!(error = %format_args!("{error:#}"), "the CPU model was not detected");
                Detected::Undetected(UndetectedReason::DetectionFailed)
            }
            Err(_) => Detected::Undetected(UndetectedReason::DeadlineExpired),
        };
        let gpus = match gpus {
            Ok(Ok(gpus)) => gpus,
            Ok(Err(error)) => {
                info!(error = %format_args!("{error:#}"), "the GPUs were not detected");
                GpuInventory::Undetected(UndetectedReason::DetectionFailed)
            }
            Err(_) => GpuInventory::Undetected(UndetectedReason::DeadlineExpired),
        };
        let onnx = match onnx {
            Ok(Ok(onnx)) => onnx,
            Ok(Err(error)) => {
                info!(error = %format_args!("{error:#}"), "the ONNX Runtime was not detected");
                OnnxRuntimeDetection::Undetected(UndetectedReason::DetectionFailed)
            }
            Err(_) => OnnxRuntimeDetection::Undetected(UndetectedReason::DeadlineExpired),
        };
        NodeProperties {
            architecture: PropertyText::new(std::env::consts::ARCH)
                .assured("a target architecture's name is short printable text"),
            cpu_model,
            gpus,
            onnx,
        }
    }

    /// Runs one synchronous probe on a bulk worker. Its libraries allocate what they need
    /// themselves, so the charge only admits the probe, as a small startup job's does.
    async fn probe<T: Send + 'static>(
        executor: &Executor,
        probe: &'static str,
        detect: fn() -> T,
    ) -> Result<T, Report<ProbeError>> {
        let reservation = executor
            .reserve(MemoryClass::Bulk, STORAGE_JOB_RESERVATION_BYTES)
            .await
            .change_context(ProbeError::Execution { probe })?;
        executor
            .run_cpu(CpuClass::Bulk, reservation, move |_, _| detect())
            .await
            .change_context(ProbeError::Execution { probe })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use meticulous::OptionExt as _;
    use nervix_execution::{ExecutionConfig, WorkerCounts};
    use nervix_primitives::sync::blocking::mpsc;

    use super::*;
    use crate::runtime::{FilledCpuClass, single_worker_executor};

    fn supplied() -> NodeProperties {
        NodeProperties {
            architecture: PropertyText::new("aarch64").assured("short printable text"),
            cpu_model: Detected::Undetected(UndetectedReason::NotReported),
            gpus: GpuInventory::Undetected(UndetectedReason::DriverUnavailable),
            onnx: OnnxRuntimeDetection::Undetected(UndetectedReason::LibraryUnavailable),
        }
    }

    /// An executor whose one bulk worker runs a job until the returned sender drops, with room in
    /// its wait queue for every probe.
    async fn held_bulk_worker() -> (Executor, mpsc::Sender<()>) {
        let executor = Executor::new(ExecutionConfig {
            workers: WorkerCounts {
                bulk_cpu: NonZeroUsize::MIN,
                pending_jobs: NonZeroUsize::new(4).assured("four is positive"),
                ..WorkerCounts::default()
            },
            ..ExecutionConfig::default()
        })
        .expect("the default budgets hold the default operation limits");
        let (release, held) = mpsc::channel::<()>();
        let holder = executor.clone();
        nervix_primitives::task::spawn(async move {
            let reservation = holder
                .try_reserve(MemoryClass::Bulk, 0)
                .expect("a zero charge is always admitted");
            holder
                .run_cpu(
                    CpuClass::Bulk,
                    reservation,
                    move |_charge, _cancellation| {
                        // Dropping the sender disconnects the channel, which is the release.
                        match held.recv() {
                            Ok(()) | Err(mpsc::RecvError) => {}
                        }
                    },
                )
                .await
                .expect("the holding job runs on the idle worker");
        });
        while executor.snapshot().cpu_class(CpuClass::Bulk).running == 0 {
            nervix_primitives::task::yield_now().await;
        }
        (executor, release)
    }

    #[nervix_primitives::test]
    async fn supplied_properties_are_the_node_s_without_probing_the_host() {
        let executor = single_worker_executor();
        let properties = NodeDetection::Supplied(supplied())
            .detect(&executor, Duration::from_millis(1))
            .await;
        assert_eq!(properties, supplied());
        assert_eq!(executor.snapshot().cpu_class(CpuClass::Bulk).admitted, 0);
    }

    #[nervix_primitives::test]
    async fn probes_that_do_not_answer_in_time_are_undetected_and_the_start_goes_on() {
        let (executor, release) = held_bulk_worker().await;
        let properties = NodeDetection::Host
            .detect(&executor, Duration::from_millis(50))
            .await;
        assert_eq!(
            properties.cpu_model,
            Detected::Undetected(UndetectedReason::DeadlineExpired)
        );
        assert_eq!(
            properties.gpus,
            GpuInventory::Undetected(UndetectedReason::DeadlineExpired)
        );
        assert_eq!(
            properties.onnx,
            OnnxRuntimeDetection::Undetected(UndetectedReason::DeadlineExpired)
        );
        assert_eq!(properties.architecture.as_str(), std::env::consts::ARCH);
        assert!(!properties.cuda_available());
        // A probe whose caller stopped waiting leaves the queue without running.
        assert_eq!(executor.snapshot().cpu_class(CpuClass::Bulk).pending, 0);
        drop(release);
    }

    #[nervix_primitives::test]
    async fn probes_the_bulk_workers_refuse_are_undetected_and_the_start_goes_on() {
        let executor = single_worker_executor();
        let filled = FilledCpuClass::fill(&executor, CpuClass::Bulk).await;
        let properties = NodeDetection::Host
            .detect(&executor, Duration::from_secs(60))
            .await;
        assert_eq!(
            properties.cpu_model,
            Detected::Undetected(UndetectedReason::DetectionFailed)
        );
        assert_eq!(
            properties.gpus,
            GpuInventory::Undetected(UndetectedReason::DetectionFailed)
        );
        assert_eq!(
            properties.onnx,
            OnnxRuntimeDetection::Undetected(UndetectedReason::DetectionFailed)
        );
        assert_eq!(executor.snapshot().cpu_class(CpuClass::Bulk).refused, 3);
        filled.release().await;
    }
}
