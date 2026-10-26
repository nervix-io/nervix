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
    capacity::{CapacitySettings, CapacitySettingsError, MemoryLimit},
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Capacity")]
pub struct CapacityArgs {
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
        CapacitySettings::resolve(self.memory_limit.map(|limit| limit.0))
    }

    pub(super) fn from_settings(settings: &CapacitySettings) -> Self {
        Self {
            memory_limit: settings.memory_limit().map(HumanBytes),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        if let Some(limit) = self.memory_limit {
            command_line.option(MemoryLimit::NAME, limit);
        }
    }
}
