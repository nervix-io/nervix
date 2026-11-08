//! The runtime state options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the state snapshot interval and the restore staging
//!   quota and inspection capabilities, their conversion into typed settings, and the rendering
//!   of that group back into options.
//! - **Depends on.** The runtime settings definitions and the edge's duration and byte forms.
//! - **Must not know.** How the runtime snapshots or stages state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroUsize;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::{
    application::cli_values::{HumanBytes, HumanDuration},
    runtime::{
        RestoreStagingMaxBytes, RuntimeSettings, StateInspectionMaxRows,
        StateInspectionMaxSections, StateInspectionReadTimeout, StateInspectionSettings,
        StateInspectionSettingsError, StateSnapshotInterval,
    },
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
    #[arg(long = StateInspectionMaxRows::NAME, env = "NERVIX_INSPECTION_MAX_ROWS", help = "Maximum rows in one state inspection reply")]
    pub inspection_max_rows: Option<NonZeroUsize>,
    #[arg(long = StateInspectionMaxSections::NAME, env = "NERVIX_INSPECTION_MAX_SECTIONS", help = "Maximum named Row sections in one state inspection reply")]
    pub inspection_max_sections: Option<NonZeroUsize>,
    #[arg(long = StateInspectionReadTimeout::NAME, env = "NERVIX_INSPECTION_READ_TIMEOUT", help = "Elapsed deadline of one complete state inspection read")]
    pub inspection_read_timeout: Option<HumanDuration>,
}

impl RuntimeArgs {
    pub(super) fn settings(&self) -> Result<RuntimeSettings, Report<StateInspectionSettingsError>> {
        let mut settings = RuntimeSettings::resolve(
            self.state_snapshot_interval.map(|value| value.0),
            self.restore_staging_max_bytes.map(|bytes| bytes.0),
        )
        .assured("the runtime settings have valid bounds");
        settings.inspection = StateInspectionSettings::resolve(
            self.inspection_max_rows,
            self.inspection_max_sections,
            self.inspection_read_timeout.map(|value| value.0),
        )?;
        Ok(settings)
    }

    pub(super) fn from_settings(settings: &RuntimeSettings) -> Self {
        Self {
            inspection_max_rows: Some(*settings.inspection.max_rows.value()),
            inspection_max_sections: Some(*settings.inspection.max_sections.value()),
            inspection_read_timeout: Some(HumanDuration(*settings.inspection.read_timeout.value())),
            state_snapshot_interval: Some(HumanDuration(settings.state_snapshot_interval())),
            restore_staging_max_bytes: settings.restore_staging_max_bytes().map(HumanBytes),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(StateInspectionMaxRows::NAME, self.inspection_max_rows);
        command_line.optional(
            StateInspectionMaxSections::NAME,
            self.inspection_max_sections,
        );
        command_line.optional(
            StateInspectionReadTimeout::NAME,
            self.inspection_read_timeout,
        );
        command_line.optional(StateSnapshotInterval::NAME, self.state_snapshot_interval);
        if let Some(bytes) = self.restore_staging_max_bytes {
            command_line.option(RestoreStagingMaxBytes::NAME, bytes);
        }
    }
}
