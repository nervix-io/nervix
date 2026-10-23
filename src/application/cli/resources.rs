//! The resource options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the per-version resource bounds, their conversion into
//!   the resource settings group, and the rendering of that group back into options.
//! - **Depends on.** The resource settings definitions and the edge's byte form.
//! - **Must not know.** How a resource version is staged or extracted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::{
    application::cli_values::HumanBytes,
    resource::{
        ResourceMaxArchiveBytes, ResourceMaxExtractedBytes, ResourceMaxFileCount, ResourceSettings,
    },
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Resources")]
pub struct ResourceArgs {
    #[arg(
        long = ResourceMaxArchiveBytes::NAME,
        env = "NERVIX_RESOURCE_MAX_ARCHIVE_BYTES",
        default_value_t = HumanBytes(ResourceMaxArchiveBytes::default_value(&())),
        help = "Maximum staged archive bytes for one resource version"
    )]
    pub resource_max_archive_bytes: HumanBytes,
    #[arg(
        long = ResourceMaxExtractedBytes::NAME,
        env = "NERVIX_RESOURCE_MAX_EXTRACTED_BYTES",
        default_value_t = HumanBytes(ResourceMaxExtractedBytes::default_value(&())),
        help = "Maximum extracted bytes for one resource version"
    )]
    pub resource_max_extracted_bytes: HumanBytes,
    #[arg(
        long = ResourceMaxFileCount::NAME,
        env = "NERVIX_RESOURCE_MAX_FILE_COUNT",
        default_value_t = ResourceMaxFileCount::default_value(&()),
        help = "Maximum extracted files for one resource version"
    )]
    pub resource_max_file_count: u64,
}

impl ResourceArgs {
    pub(super) fn settings(&self) -> ResourceSettings {
        ResourceSettings::resolve(
            Some(self.resource_max_archive_bytes.0),
            Some(self.resource_max_extracted_bytes.0),
            Some(self.resource_max_file_count),
        )
        .assured("the resource settings declare no validation")
    }

    pub(super) fn from_settings(settings: &ResourceSettings) -> Self {
        Self {
            resource_max_archive_bytes: HumanBytes(*settings.max_archive_bytes.value()),
            resource_max_extracted_bytes: HumanBytes(*settings.max_extracted_bytes.value()),
            resource_max_file_count: *settings.max_file_count.value(),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.option(
            ResourceMaxArchiveBytes::NAME,
            self.resource_max_archive_bytes,
        );
        command_line.option(
            ResourceMaxExtractedBytes::NAME,
            self.resource_max_extracted_bytes,
        );
        command_line.option(ResourceMaxFileCount::NAME, self.resource_max_file_count);
    }
}
