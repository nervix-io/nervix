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
    application::cli_values::HumanBytes,
    capacity::{CapacitySettings, CapacitySettingsError, CpuCount, MemoryLimit},
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
}

impl CapacityArgs {
    pub(super) fn settings(&self) -> Result<CapacitySettings, Report<CapacitySettingsError>> {
        CapacitySettings::resolve(self.memory_limit.map(|limit| limit.0), self.cpu_count)
    }

    pub(super) fn peer_receive(
        &self,
    ) -> Result<
        nervix_interconnect::PeerReceiveOverrides,
        Report<nervix_interconnect::PeerReceiveSettingsError>,
    > {
        let overrides = nervix_interconnect::PeerReceiveOverrides {
            relay_batch_bytes: self.relay_batch_bytes.map(|v| v.0),
            relay_decoded_bytes: self.relay_decoded_bytes.map(|v| v.0),
            bulk_chunk_bytes: self.bulk_chunk_bytes.map(|v| v.0),
            bulk_window_bytes: self.bulk_window_bytes.map(|v| v.0),
        };
        overrides.validate()?;
        Ok(overrides)
    }

    pub(super) fn from_settings(
        settings: &CapacitySettings,
        receive: &nervix_interconnect::PeerReceiveOverrides,
    ) -> Self {
        Self {
            relay_batch_bytes: receive.relay_batch_bytes.map(HumanBytes),
            relay_decoded_bytes: receive.relay_decoded_bytes.map(HumanBytes),
            bulk_chunk_bytes: receive.bulk_chunk_bytes.map(HumanBytes),
            bulk_window_bytes: receive.bulk_window_bytes.map(HumanBytes),
            cpu_count: *settings.cpu_count.value(),
            memory_limit: settings.memory_limit().map(HumanBytes),
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
    }
}
