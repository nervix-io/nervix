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
        default_value_t = HumanDuration(StateSnapshotInterval::default_value(&())),
        help = "How often the owner of runtime state publishes a snapshot of it to its replicas"
    )]
    pub state_snapshot_interval: HumanDuration,
    #[arg(
        long = RestoreStagingMaxBytes::NAME,
        env = "NERVIX_RESTORE_STAGING_MAX_BYTES",
        default_value_t = HumanBytes(RestoreStagingMaxBytes::default_value(&())),
        help = "Maximum unpublished restore checkpoint key and value bytes per node"
    )]
    pub restore_staging_max_bytes: HumanBytes,
}

impl RuntimeArgs {
    pub(super) fn settings(&self) -> RuntimeSettings {
        RuntimeSettings::resolve(
            Some(self.state_snapshot_interval.0),
            Some(self.restore_staging_max_bytes.0),
        )
        .assured("the runtime settings declare no validation")
    }

    pub(super) fn from_settings(settings: &RuntimeSettings) -> Self {
        Self {
            state_snapshot_interval: HumanDuration(settings.state_snapshot_interval()),
            restore_staging_max_bytes: HumanBytes(*settings.restore_staging_max_bytes.value()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.option(StateSnapshotInterval::NAME, self.state_snapshot_interval);
        command_line.option(RestoreStagingMaxBytes::NAME, self.restore_staging_max_bytes);
    }
}
