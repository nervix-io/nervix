//! The scheduling options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the replica count, its conversion into the scheduling
//!   settings group, and the rendering of that group back into options.
//! - **Depends on.** The scheduling settings definition.
//! - **Must not know.** How a schedule is computed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::application::scheduling::{ReplicaCount, SchedulingSettings};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Scheduling")]
pub struct SchedulingArgs {
    #[arg(
        long = ReplicaCount::NAME,
        env = "NERVIX_REPLICA_COUNT",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ReplicaCount, "How many nodes besides its owner hold a replica of each piece of runtime state")
    )]
    pub replica_count: Option<usize>,
}

impl SchedulingArgs {
    pub(super) fn settings(&self) -> SchedulingSettings {
        SchedulingSettings::resolve(self.replica_count)
            .assured("the scheduling settings have valid bounds")
    }

    pub(super) fn from_settings(settings: &SchedulingSettings) -> Self {
        Self {
            replica_count: Some(settings.replica_count()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(ReplicaCount::NAME, self.replica_count);
    }
}
