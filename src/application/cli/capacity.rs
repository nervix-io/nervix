//! The capacity options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of what the node can carry, its conversion into the capacity
//!   settings group, and the rendering of that group back into options.
//! - **Depends on.** The capacity settings definitions and the edge's byte form.
//! - **Must not know.** How the node detects its capacity or what it sizes with it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::Report;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::{
    application::cli_values::{HumanBytes, HumanDuration},
    capacity::{
        CapacitySettings, CapacitySettingsError, CpuCount, MemoryLimit, NodeDetectionTimeout,
    },
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Capacity")]
pub struct CapacityArgs {
    #[arg(long = nervix_interconnect::RelayBatchBytes::NAME, env = "NERVIX_INTERCONNECT_RELAY_BATCH_BYTES", help = "Largest encoded relay body this node accepts; defaults to the executor's relay encoded limit")]
    pub relay_batch_bytes: Option<HumanBytes>,
    #[arg(long = nervix_interconnect::RelayDecodedBytes::NAME, env = "NERVIX_INTERCONNECT_RELAY_DECODED_BYTES", help = "Largest decoded relay batch this node accepts; defaults to the executor's relay decoded limit")]
    pub relay_decoded_bytes: Option<HumanBytes>,
    #[arg(long = nervix_interconnect::BulkChunkBytes::NAME, env = "NERVIX_INTERCONNECT_BULK_CHUNK_BYTES", help = "Largest bulk response chunk this node accepts; defaults to the executor's bulk chunk limit")]
    pub bulk_chunk_bytes: Option<HumanBytes>,
    #[arg(long = nervix_interconnect::BulkReceiveWindow::NAME, env = "NERVIX_INTERCONNECT_BULK_RECEIVE_WINDOW", help = "The HTTP/2 receive window reserved by each bulk stream; defaults to 1MiB")]
    pub bulk_window_bytes: Option<HumanBytes>,
    #[arg(long = CpuCount::NAME, env = "NERVIX_CPU_COUNT", help = "The CPU capacity of this node; defaults to the process's cgroup-aware available parallelism")]
    pub cpu_count: Option<u64>,
    #[arg(
        long = MemoryLimit::NAME,
        env = "NERVIX_MEMORY_LIMIT",
        help = "The memory this node may use; without it the node detects its control group's \
                memory limit, or its physical memory when no limit is smaller"
    )]
    pub memory_limit: Option<HumanBytes>,
    #[arg(
        long = NodeDetectionTimeout::NAME,
        env = "NERVIX_NODE_DETECTION_TIMEOUT",
        default_value_t = HumanDuration(NodeDetectionTimeout::default_value(&())),
        help = "How long startup waits for the node to detect its CPU model, GPUs and ONNX \
                Runtime; a probe still running then leaves its properties undetected"
    )]
    pub node_detection_timeout: HumanDuration,
}

impl CapacityArgs {
    pub(super) fn settings(&self) -> Result<CapacitySettings, Report<CapacitySettingsError>> {
        CapacitySettings::resolve(
            self.memory_limit.map(|limit| limit.0),
            self.cpu_count,
            Some(self.node_detection_timeout.0),
        )
    }

    pub(super) fn peer_receive(
        &self,
    ) -> Result<
        nervix_interconnect::PeerReceiveSettings,
        Report<nervix_interconnect::PeerReceiveSettingsError>,
    > {
        nervix_interconnect::PeerReceiveSettings::resolve(
            self.relay_batch_bytes.map(|v| v.0),
            self.relay_decoded_bytes.map(|v| v.0),
            self.bulk_chunk_bytes.map(|v| v.0),
            self.bulk_window_bytes.map(|v| v.0),
            &nervix_execution::OperationLimits::default(),
        )
    }

    pub(super) fn from_settings(
        settings: &CapacitySettings,
        receive: &nervix_interconnect::PeerReceiveSettings,
    ) -> Self {
        Self {
            relay_batch_bytes: Some(HumanBytes(*receive.relay_batch_bytes.value())),
            relay_decoded_bytes: Some(HumanBytes(*receive.relay_decoded_bytes.value())),
            bulk_chunk_bytes: Some(HumanBytes(*receive.bulk_chunk_bytes.value())),
            bulk_window_bytes: Some(HumanBytes(*receive.bulk_window_bytes.value())),
            cpu_count: *settings.cpu_count.value(),
            memory_limit: settings.memory_limit().map(HumanBytes),
            node_detection_timeout: HumanDuration(settings.detection_timeout()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(
            nervix_interconnect::RelayBatchBytes::NAME,
            self.relay_batch_bytes,
        );
        command_line.optional(
            nervix_interconnect::RelayDecodedBytes::NAME,
            self.relay_decoded_bytes,
        );
        command_line.optional(
            nervix_interconnect::BulkChunkBytes::NAME,
            self.bulk_chunk_bytes,
        );
        command_line.optional(
            nervix_interconnect::BulkReceiveWindow::NAME,
            self.bulk_window_bytes,
        );
        if let Some(count) = self.cpu_count {
            command_line.option(CpuCount::NAME, count);
        }
        if let Some(limit) = self.memory_limit {
            command_line.option(MemoryLimit::NAME, limit);
        }
        command_line.option(NodeDetectionTimeout::NAME, self.node_detection_timeout);
    }
}
