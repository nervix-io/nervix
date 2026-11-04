//! What a node checks about its environment before it binds a listener or opens a store.
//!
//! Layer: edges.
//!
//! - **Owns.** The startup preflight: building the executor from limits validated together, the
//!   node's memory limit against its memory budgets and its memory-pressure watermark, its database
//!   and temporary directories, the quotas their volume must hold and the restore staging quota it
//!   resolves against that volume, and loading its resolver.
//! - **Depends on.** The executor's configuration, the settings groups it checks, the capacity
//!   detection, and the resolver's loader.
//! - **Must not know.** What the node runs once it has started.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{
    fmt,
    path::{Path, PathBuf},
};

use error_stack::{Report, ResultExt as _};
use nervix_dns::{DnsConfiguration, DnsResolver};
use nervix_execution::{ExecutionConfig, Executor, MemoryClass, StorageClass};
use nervix_models::SettingDefinition as _;
use thiserror::Error;
use tracing::{Instrument as _, info};
use ubyte::ByteUnit;

use super::{NodeSettings, startup::STORAGE_JOB_RESERVATION_BYTES};
use crate::{
    capacity::{DetectedMemory, NodeMemory, VolumeCapacityError, volume_capacity},
    memory_pressure::MemoryHighWatermark,
    resource::{ResourceMaxArchiveBytes, ResourceMaxExtractedBytes},
    runtime::RestoreStagingMaxBytes,
};

/// A directory the node stages work in and must be able to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagingDirectory {
    /// The node database, which also holds restore staging and resource bundles being staged.
    Database,
    /// The local temporary directory, which holds snapshot transfers and emitter staging.
    Temporary,
}

impl fmt::Display for StagingDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database => formatter.write_str("database directory"),
            Self::Temporary => formatter.write_str("temporary directory"),
        }
    }
}

/// Why the startup preflight refuses this node. Each variant names the setting or directory it
/// judged, with the values involved.
#[derive(Debug, Error)]
pub enum PreflightError {
    #[error("the executor's memory budgets cannot hold the operations its limits admit")]
    ExecutionLimits,
    #[error("the preflight's filesystem work could not be admitted")]
    Execution,
    #[error("the node's memory budgets do not add up to a byte count")]
    UnaddressableBudgets,
    #[error("{memory} cannot hold the node's memory budgets of {budgets}")]
    MemoryBelowBudgets {
        memory: NodeMemory,
        budgets: ByteUnit,
    },
    #[error(
        "{watermark_name} ({watermark}) exceeds {memory}",
        watermark_name = MemoryHighWatermark::NAME
    )]
    WatermarkAboveMemory {
        watermark: ByteUnit,
        memory: NodeMemory,
    },
    #[error("the {directory} {} cannot be created", path.display())]
    DirectoryUnavailable {
        directory: StagingDirectory,
        path: PathBuf,
    },
    #[error("the {directory} {} is not writable", path.display())]
    DirectoryNotWritable {
        directory: StagingDirectory,
        path: PathBuf,
    },
    #[error("the capacity of the volume holding the {directory} {} cannot be read", path.display())]
    VolumeUnreadable {
        directory: StagingDirectory,
        path: PathBuf,
    },
    #[error(
        "{setting} ({quota}) exceeds the {capacity} capacity of the volume holding the {directory} \
         {}",
        path.display()
    )]
    QuotaExceedsVolume {
        setting: &'static str,
        quota: ByteUnit,
        capacity: ByteUnit,
        directory: StagingDirectory,
        path: PathBuf,
    },
    #[error("failed to load the name resolver configuration")]
    Resolver,
}

/// What a node that passed its preflight starts with.
pub(in crate::application) struct PreflightOutcome {
    pub(in crate::application) executor: Executor,
    pub(in crate::application) dns: DnsResolver,
    pub(in crate::application) memory: NodeMemory,
    /// The restore staging the node's state store may hold, resolved against the database volume.
    pub(in crate::application) restore_staging_max_bytes: ByteUnit,
    pub(in crate::application) database_volume: ByteUnit,
}

/// What the preflight checks: the node's paths and settings.
pub(in crate::application) struct Preflight<'node> {
    pub(in crate::application) settings: &'node NodeSettings,
    pub(in crate::application) db_path: &'node Path,
    pub(in crate::application) temp_dir: &'node Path,
    pub(in crate::application) dns: &'node DnsConfiguration,
}

/// One directory's facts: it exists, is writable, and lives on a volume of this capacity.
struct PreparedDirectory {
    capacity: ByteUnit,
}

impl Preflight<'_> {
    /// Runs every check, refusing the node at the first one that fails.
    pub(in crate::application) async fn run(
        self,
    ) -> Result<PreflightOutcome, Report<PreflightError>> {
        let config = ExecutionConfig::default();
        let executor = Executor::new(config).change_context(PreflightError::ExecutionLimits)?;
        let budgets = config
            .budgets
            .total()
            .ok_or_else(|| Report::new(PreflightError::UnaddressableBudgets))?;

        let detected = run_filesystem_work(&executor, DetectedMemory::detect).await?;
        let memory = self.settings.capacity.effective_memory_limit(detected);
        if let Some(limit) = memory.limit() {
            if limit < budgets {
                return Err(Report::new(PreflightError::MemoryBelowBudgets {
                    memory,
                    budgets,
                }));
            }
            if let Some(watermark) = *self.settings.memory_pressure.high_watermark.value()
                && watermark > limit
            {
                return Err(Report::new(PreflightError::WatermarkAboveMemory {
                    watermark,
                    memory,
                }));
            }
        }

        let database =
            prepare_directory(&executor, StagingDirectory::Database, self.db_path).await?;
        let temporary =
            prepare_directory(&executor, StagingDirectory::Temporary, self.temp_dir).await?;
        let restore_staging_max_bytes = self
            .settings
            .runtime
            .restore_staging_quota(database.capacity);
        let database_quotas = [(RestoreStagingMaxBytes::NAME, restore_staging_max_bytes)];
        for (setting, quota) in database_quotas {
            if quota > database.capacity {
                return Err(Report::new(PreflightError::QuotaExceedsVolume {
                    setting,
                    quota,
                    capacity: database.capacity,
                    directory: StagingDirectory::Database,
                    path: self.db_path.to_path_buf(),
                }));
            }
        }

        let dns = DnsResolver::load(self.dns.clone())
            .instrument(tracing::info_span!("load_dns_configuration"))
            .await
            .change_context(PreflightError::Resolver)?;

        info!(
            memory = %memory,
            memory_budgets = %budgets,
            database_volume = %database.capacity,
            temporary_volume = %temporary.capacity,
            restore_staging_max_bytes = %restore_staging_max_bytes,
            "the startup preflight passed"
        );
        Ok(PreflightOutcome {
            executor,
            dns,
            memory,
            restore_staging_max_bytes,
            database_volume: database.capacity,
        })
    }
}

/// Runs `work` as the node's filesystem work, charged to its management budget.
async fn run_filesystem_work<T: Send + 'static>(
    executor: &Executor,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Report<PreflightError>> {
    let reservation = executor
        .reserve(MemoryClass::Management, STORAGE_JOB_RESERVATION_BYTES)
        .await
        .change_context(PreflightError::Execution)?;
    executor
        .run_storage(StorageClass::Filesystem, reservation, move |_, _| work())
        .await
        .change_context(PreflightError::Execution)
}

/// Creates `path` when it is absent, as the store that uses it would, proves it writable with a
/// probe file that is removed again, and measures its volume.
async fn prepare_directory(
    executor: &Executor,
    directory: StagingDirectory,
    path: &Path,
) -> Result<PreparedDirectory, Report<PreflightError>> {
    let owned = path.to_path_buf();
    run_filesystem_work(executor, move || prepare_directory_now(directory, &owned)).await?
}

fn prepare_directory_now(
    directory: StagingDirectory,
    path: &Path,
) -> Result<PreparedDirectory, Report<PreflightError>> {
    std::fs::create_dir_all(path).map_err(|error| {
        Report::new(error).change_context(PreflightError::DirectoryUnavailable {
            directory,
            path: path.to_path_buf(),
        })
    })?;
    let probe = tempfile::Builder::new()
        .prefix(".nervix-preflight-")
        .tempfile_in(path)
        .map_err(|error| {
            Report::new(error).change_context(PreflightError::DirectoryNotWritable {
                directory,
                path: path.to_path_buf(),
            })
        })?;
    probe.close().map_err(|error| {
        Report::new(error).change_context(PreflightError::DirectoryNotWritable {
            directory,
            path: path.to_path_buf(),
        })
    })?;
    let capacity = volume_capacity(path).map_err(|error: Report<VolumeCapacityError>| {
        error.change_context(PreflightError::VolumeUnreadable {
            directory,
            path: path.to_path_buf(),
        })
    })?;
    Ok(PreparedDirectory { capacity })
}

#[cfg(test)]
#[path = "preflight_tests.rs"]
mod tests;

/// Validate resource promises only after choosing the actual bootstrap or live committed policy.
/// Local defaults on a returning node are never a substitute for that policy.
pub(in crate::application) fn validate_resource_volume(
    parameters: &nervix_models::ClusterParameterValues,
    capacity: ByteUnit,
    path: &Path,
) -> Result<(), Report<PreflightError>> {
    for (setting, quota) in [
        (
            ResourceMaxArchiveBytes::NAME,
            ByteUnit::Byte(parameters.resource_max_archive_bytes()),
        ),
        (
            ResourceMaxExtractedBytes::NAME,
            ByteUnit::Byte(parameters.resource_max_extracted_bytes()),
        ),
    ] {
        if quota > capacity {
            return Err(Report::new(PreflightError::QuotaExceedsVolume {
                setting,
                quota,
                capacity,
                directory: StagingDirectory::Database,
                path: path.to_path_buf(),
            }));
        }
    }
    Ok(())
}
