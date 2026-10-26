//! The size bounds the cluster puts on one resource version, declared as settings.
//!
//! Layer: control plane.
//!
//! - **Owns.** The setting definitions of the archive, extracted-tree and file-count bounds, their
//!   single defaults, the group that holds them and the store limits it yields.
//! - **Depends on.** The vocabulary's setting classes and the store's limits.
//! - **Must not know.** Command-line parsing or how an archive is extracted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::Setting;
use ubyte::ByteUnit;

use super::ResourceStoreLimits;

/// The size bounds the cluster puts on one resource version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceSettings {
    pub max_archive_bytes: Setting<ResourceMaxArchiveBytes>,
    pub max_extracted_bytes: Setting<ResourceMaxExtractedBytes>,
    pub max_file_count: Setting<ResourceMaxFileCount>,
}

impl ResourceSettings {
    /// Resolves each bound that is not given through its definition's default.
    pub fn resolve(
        max_archive_bytes: Option<ByteUnit>,
        max_extracted_bytes: Option<ByteUnit>,
        max_file_count: Option<u64>,
    ) -> Result<Self, Report<nervix_models::ClusterParameterError>> {
        Ok(Self {
            max_archive_bytes: Setting::resolve(
                max_archive_bytes.map(|bytes| bytes.as_u64()),
                &(),
            )?,
            max_extracted_bytes: Setting::resolve(
                max_extracted_bytes.map(|bytes| bytes.as_u64()),
                &(),
            )?,
            max_file_count: Setting::resolve(max_file_count, &())?,
        })
    }

    /// The bounds the resource store applies to each version.
    pub fn limits(&self) -> ResourceStoreLimits {
        ResourceStoreLimits {
            max_archive_bytes: *self.max_archive_bytes.value(),
            max_extracted_bytes: *self.max_extracted_bytes.value(),
            max_file_count: *self.max_file_count.value(),
        }
    }
}

impl Default for ResourceSettings {
    fn default() -> Self {
        Self::resolve(None, None, None).assured("the resource defaults have valid bounds")
    }
}

// Cluster policy definitions have one vocabulary owner. These facades hold bootstrap inputs.
pub use nervix_models::{ResourceMaxArchiveBytes, ResourceMaxExtractedBytes, ResourceMaxFileCount};
