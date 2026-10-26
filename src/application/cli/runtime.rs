//! The runtime state options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the state snapshot interval and the restore staging
//!   quota, their conversion into the runtime settings group, and the rendering of that group back
//!   into options.
//! - **Depends on.** The runtime settings definitions and the edge's duration and byte forms.
//! - **Must not know.** How the runtime snapshots or stages state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::{
    application::cli_values::{HumanBytes, HumanDuration},
    runtime::{RestoreStagingMaxBytes, RuntimeSettings, StateSnapshotInterval},
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Runtime state")]
pub struct RuntimeArgs {
    #[arg(
        long = StateSnapshotInterval::NAME,
        env = "NERVIX_STATE_SNAPSHOT_INTERVAL",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::StateSnapshotInterval, "How often the owner of runtime state publishes a snapshot of it to its replicas")
    )]
    pub state_snapshot_interval: Option<HumanDuration>,
    #[arg(
        long = RestoreStagingMaxBytes::NAME,
        env = "NERVIX_RESTORE_STAGING_MAX_BYTES",
        help = "Maximum unpublished restore checkpoint key and value bytes per node; without it \
                the smaller of 128 GiB and the capacity of the database's volume"
    )]
    pub restore_staging_max_bytes: Option<HumanBytes>,
}

impl RuntimeArgs {
    pub(super) fn settings(&self) -> RuntimeSettings {
        RuntimeSettings::resolve(
            self.state_snapshot_interval.map(|value| value.0),
            self.restore_staging_max_bytes.map(|bytes| bytes.0),
        )
        .assured("the runtime settings have valid bounds")
    }

    pub(super) fn from_settings(settings: &RuntimeSettings) -> Self {
        Self {
            state_snapshot_interval: Some(HumanDuration(settings.state_snapshot_interval())),
            restore_staging_max_bytes: settings.restore_staging_max_bytes().map(HumanBytes),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(StateSnapshotInterval::NAME, self.state_snapshot_interval);
        if let Some(bytes) = self.restore_staging_max_bytes {
            command_line.option(RestoreStagingMaxBytes::NAME, bytes);
        }
    }
}
