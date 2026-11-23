//! The shutdown options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the shutdown timeout and the cooperative task grace, their
//!   conversion into the shutdown settings group, and the rendering of that group back into
//!   options.
//! - **Depends on.** The shutdown settings definitions and the edge's duration form.
//! - **Must not know.** What a shutdown stops.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::application::{
    cli_values::HumanDuration,
    shutdown::{ShutdownSettings, ShutdownTimeout, TaskGracePeriod},
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Shutdown")]
pub struct ShutdownArgs {
    #[arg(
        long = ShutdownTimeout::NAME,
        env = "NERVIX_SHUTDOWN_TIMEOUT",
        default_value_t = HumanDuration(ShutdownTimeout::default_value(&())),
        help = "Maximum time a stopping node's work may take to move to other nodes once the node has saved it; reaching it is a cluster disruption"
    )]
    pub shutdown_timeout: HumanDuration,
    #[arg(
        long = TaskGracePeriod::NAME,
        env = "NERVIX_TASK_GRACE_PERIOD",
        default_value_t = HumanDuration(TaskGracePeriod::default_value(&())),
        help = "Time each listener or background task has to stop cooperatively"
    )]
    pub task_grace_period: HumanDuration,
}

impl ShutdownArgs {
    pub(super) fn settings(&self) -> ShutdownSettings {
        ShutdownSettings::resolve(
            Some(self.shutdown_timeout.0),
            Some(self.task_grace_period.0),
        )
    }

    pub(super) fn from_settings(settings: &ShutdownSettings) -> Self {
        Self {
            shutdown_timeout: HumanDuration(settings.shutdown_timeout()),
            task_grace_period: HumanDuration(settings.task_grace_period()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.option(ShutdownTimeout::NAME, self.shutdown_timeout);
        command_line.option(TaskGracePeriod::NAME, self.task_grace_period);
    }
}
