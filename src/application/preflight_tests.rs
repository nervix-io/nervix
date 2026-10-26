//! Tests of what the startup preflight refuses a node for.
//!
//! Layer: test harness.
//!
//! - **Owns.** Coverage of the preflight's refusals: a memory-pressure watermark above the memory
//!   limit, a directory the node cannot create or write, and a quota its volume cannot hold.
//! - **Depends on.** The preflight, the settings groups it checks and temporary directories.
//! - **Must not know.** What the node runs once it has started.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::path::Path;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_dns::DnsConfiguration;
use nervix_models::SettingDefinition as _;
use tempfile::TempDir;
use ubyte::ByteUnit;

use super::{Preflight, PreflightError, StagingDirectory};
use crate::{
    application::NodeSettings,
    capacity::{CapacitySettings, NodeMemory},
    memory_pressure::MemoryPressureSettings,
    resource::{ResourceMaxArchiveBytes, ResourceSettings},
};

/// Runs the preflight of a node with `settings` over `db_path` and `temp_dir`, which must refuse it.
async fn refusal(
    settings: &NodeSettings,
    db_path: &Path,
    temp_dir: &Path,
) -> Report<PreflightError> {
    let dns = DnsConfiguration::system();
    let preflight = Preflight {
        settings,
        db_path,
        temp_dir,
        dns: &dns,
    };
    let Err(refusal) = preflight.run().await else {
        panic!("the preflight must refuse this node");
    };
    refusal
}

fn scratch() -> TempDir {
    tempfile::tempdir().assured("the test host provides a writable temporary directory")
}

#[nervix_primitives::test]
async fn a_high_watermark_above_the_memory_limit_refuses_the_node_naming_both() {
    let scratch = scratch();
    let settings = NodeSettings {
        capacity: CapacitySettings::resolve(Some(ByteUnit::Gibibyte(4)))
            .assured("four GiB is a memory limit"),
        memory_pressure: MemoryPressureSettings::resolve(
            Some(ByteUnit::Gibibyte(8)),
            Some(ByteUnit::Gibibyte(6)),
            None,
            None,
        )
        .assured("the low watermark is below the high one"),
        ..NodeSettings::default()
    };

    let refused = refusal(&settings, &scratch.path().join("db"), scratch.path()).await;

    let PreflightError::WatermarkAboveMemory { watermark, memory } = refused.current_context()
    else {
        panic!("a watermark above the memory limit must refuse as one: {refused:?}");
    };
    assert_eq!(*watermark, ByteUnit::Gibibyte(8));
    assert_eq!(*memory, NodeMemory::Configured(ByteUnit::Gibibyte(4)));
    assert_eq!(
        refused.current_context().to_string(),
        "memory-high-watermark (8GiB) exceeds memory-limit (4GiB)"
    );
}

#[nervix_primitives::test]
async fn a_database_directory_that_cannot_be_created_refuses_the_node_naming_it() {
    let scratch = scratch();
    let file = scratch.path().join("file");
    std::fs::write(&file, b"").assured("the scratch directory is writable");
    let db_path = file.join("db");

    let refused = refusal(&NodeSettings::default(), &db_path, scratch.path()).await;

    let PreflightError::DirectoryUnavailable {
        directory: StagingDirectory::Database,
        path,
    } = refused.current_context()
    else {
        panic!("a database path below a file must refuse as unavailable: {refused:?}");
    };
    assert_eq!(path, &db_path);
    assert_eq!(
        refused.current_context().to_string(),
        format!(
            "the database directory {} cannot be created",
            db_path.display()
        )
    );
}

/// No user, the superuser included, may create a file in the root of the process filesystem.
#[cfg(target_os = "linux")]
#[nervix_primitives::test]
async fn a_temporary_directory_the_node_cannot_write_refuses_the_node_naming_it() {
    let scratch = scratch();
    let temp_dir = Path::new("/proc");

    let refused = refusal(
        &NodeSettings::default(),
        &scratch.path().join("db"),
        temp_dir,
    )
    .await;

    let PreflightError::DirectoryNotWritable {
        directory: StagingDirectory::Temporary,
        path,
    } = refused.current_context()
    else {
        panic!("a directory no file can be created in must refuse as unwritable: {refused:?}");
    };
    assert_eq!(path, temp_dir);
    assert_eq!(
        refused.current_context().to_string(),
        "the temporary directory /proc is not writable"
    );
}

#[nervix_primitives::test]
async fn a_quota_beyond_the_database_volume_refuses_the_node_naming_the_setting() {
    let scratch = scratch();
    let quota = ByteUnit::Exbibyte(1);
    let settings = NodeSettings {
        resources: ResourceSettings::resolve(Some(quota), None, None)
            .assured("the resource bounds declare no validation"),
        ..NodeSettings::default()
    };
    let db_path = scratch.path().join("db");

    let checked = Preflight {
        settings: &settings,
        db_path: &db_path,
        temp_dir: scratch.path(),
        dns: &DnsConfiguration::system(),
    }
    .run()
    .await
    .assured("local preflight measures the volume without interpreting local resource seeds");
    let parameters = settings
        .bootstrap_parameters()
        .assured("the bootstrap values have valid scalar bounds");
    let Err(refused) =
        super::validate_resource_volume(&parameters, checked.database_volume, &db_path)
    else {
        panic!("the chosen policy cannot fit this measured volume");
    };

    let PreflightError::QuotaExceedsVolume {
        setting,
        quota: refused_quota,
        capacity,
        directory: StagingDirectory::Database,
        path,
    } = refused.current_context()
    else {
        panic!("a quota above the volume must refuse as one: {refused:?}");
    };
    assert_eq!(*setting, ResourceMaxArchiveBytes::NAME);
    assert_eq!(*refused_quota, quota);
    assert!(*capacity < quota, "{capacity} holds the quota");
    assert_eq!(path, &db_path);
    assert_eq!(
        refused.current_context().to_string(),
        format!(
            "resource-max-archive-bytes (1EiB) exceeds the {capacity} capacity of the volume \
             holding the database directory {}",
            db_path.display()
        )
    );
}
