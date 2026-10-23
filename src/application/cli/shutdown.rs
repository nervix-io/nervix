//! The shutdown options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the shutdown and drain timeouts, their conversion into
//!   the shutdown settings group, and the rendering of that group back into options.
//! - **Depends on.** The shutdown settings definitions and the edge's duration form.
//! - **Must not know.** What a shutdown stops.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::Report;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::application::{
    cli_values::HumanDuration,
    shutdown::{DrainTimeout, ShutdownSettings, ShutdownSettingsError, ShutdownTimeout},
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Shutdown")]
pub struct ShutdownArgs {
    #[arg(
        long = DrainTimeout::NAME,
        env = "NERVIX_DRAIN_TIMEOUT",
        default_value_t = HumanDuration(ShutdownSettings::default().drain_timeout()),
        help = "Maximum time to wait for drain operations before continuing"
    )]
    pub drain_timeout: HumanDuration,
    #[arg(
        long = ShutdownTimeout::NAME,
        env = "NERVIX_SHUTDOWN_TIMEOUT",
        default_value_t = HumanDuration(ShutdownTimeout::default_value(&())),
        help = "Maximum time from the first stop request until the process exits"
    )]
    pub shutdown_timeout: HumanDuration,
}

impl ShutdownArgs {
    pub(super) fn settings(&self) -> Result<ShutdownSettings, Report<ShutdownSettingsError>> {
        ShutdownSettings::resolve(Some(self.shutdown_timeout.0), Some(self.drain_timeout.0))
    }

    pub(super) fn from_settings(settings: &ShutdownSettings) -> Self {
        Self {
            drain_timeout: HumanDuration(settings.drain_timeout()),
            shutdown_timeout: HumanDuration(settings.shutdown_timeout()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.option(DrainTimeout::NAME, self.drain_timeout);
        command_line.option(ShutdownTimeout::NAME, self.shutdown_timeout);
    }
}
