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
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ResourceMaxArchiveBytes, "Maximum staged archive bytes for one resource version")
    )]
    pub resource_max_archive_bytes: Option<HumanBytes>,
    #[arg(
        long = ResourceMaxExtractedBytes::NAME,
        env = "NERVIX_RESOURCE_MAX_EXTRACTED_BYTES",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ResourceMaxExtractedBytes, "Maximum extracted bytes for one resource version")
    )]
    pub resource_max_extracted_bytes: Option<HumanBytes>,
    #[arg(
        long = ResourceMaxFileCount::NAME,
        env = "NERVIX_RESOURCE_MAX_FILE_COUNT",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ResourceMaxFileCount, "Maximum extracted files for one resource version")
    )]
    pub resource_max_file_count: Option<u64>,
}

impl ResourceArgs {
    pub(super) fn settings(&self) -> ResourceSettings {
        ResourceSettings::resolve(
            self.resource_max_archive_bytes.map(|value| value.0),
            self.resource_max_extracted_bytes.map(|value| value.0),
            self.resource_max_file_count,
        )
        .assured("the resource settings have valid bounds")
    }

    pub(super) fn from_settings(settings: &ResourceSettings) -> Self {
        Self {
            resource_max_archive_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                *settings.max_archive_bytes.value(),
            ))),
            resource_max_extracted_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                *settings.max_extracted_bytes.value(),
            ))),
            resource_max_file_count: Some(*settings.max_file_count.value()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(
            ResourceMaxArchiveBytes::NAME,
            self.resource_max_archive_bytes,
        );
        command_line.optional(
            ResourceMaxExtractedBytes::NAME,
            self.resource_max_extracted_bytes,
        );
        command_line.optional(ResourceMaxFileCount::NAME, self.resource_max_file_count);
    }
}
