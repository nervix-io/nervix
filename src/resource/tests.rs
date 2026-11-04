//! Resource archive, installation and cleanup tests.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** Unit coverage of the production resource store and its archive lifecycle.
//! - **Depends on.** The parent resource owner and repository test primitives.
//! - **Must not know.** Registry Models, graph planning or control-plane resource operations.

use std::{io::Write as _, path::Path, time::Duration};

use nervix_execution::{ExecutionConfig, Executor};
use nervix_models::{ClusterNodeName, DomainName, ResourceId, ResourceName, Timestamp};
use tempfile::{NamedTempFile, tempdir};
use ubyte::ByteUnit;

use super::{
    ResourceEntryContent, ResourceStore, ResourceStoreError, ResourceStoreLimits, encode_hex,
};

fn resource_id(domain: &str, identifier: &str, version: u64) -> ResourceId {
    ResourceId::new(
        DomainName::parse(domain).expect("valid domain"),
        ResourceName::parse(identifier).expect("valid identifier"),
        version,
    )
}

async fn read_archive(store: &ResourceStore, id: &ResourceId) -> Vec<u8> {
    let mut archive = Vec::new();
    let mut reader = store.open_archive(id).await.expect("archive should open");
    while let Some(chunk) = reader
        .next_chunk()
        .await
        .expect("archive chunk should be readable")
    {
        archive.extend_from_slice(&chunk);
    }
    archive
}

#[nervix_primitives::test]
async fn archive_stager_writes_bounded_chunks_and_reports_their_digest() {
    let install_root = tempdir().expect("install tempdir");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");
    let mut stager = store
        .create_archive_stager()
        .await
        .expect("archive stager should open");
    for bytes in [b"first".as_slice(), b"-second".as_slice()] {
        let chunk = store
            .admit_staging_bytes(bytes)
            .await
            .expect("archive chunk should be admitted");
        stager
            .write_chunk(chunk)
            .await
            .expect("archive chunk should be staged");
    }

    let staged = stager.finish().await.expect("archive should finish");

    assert!(staged.path().starts_with(install_root.path()));
    assert_eq!(staged.archive_bytes(), 12);
    assert_eq!(
        staged.root_checksum(),
        encode_hex(blake3::hash(b"first-second").as_bytes())
    );
    assert_eq!(
        std::fs::read(staged.path()).expect("staged archive should be readable"),
        b"first-second"
    );
}

#[nervix_primitives::test]
async fn bundle_stager_builds_an_installable_archive_and_removes_its_source_tree() {
    let install_root = tempdir().expect("install tempdir");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");
    let mut stager = store
        .create_bundle_stager()
        .await
        .expect("bundle stager should open");
    let staging_root = stager
        .root
        .as_ref()
        .expect("unfinished stager should own its tree")
        .clone();
    stager
        .create_file(Path::new("nested/model.bin"))
        .await
        .expect("bundle file should be created");
    for bytes in [b"model".as_slice(), b"-bytes".as_slice()] {
        let chunk = store
            .admit_staging_bytes(bytes)
            .await
            .expect("bundle bytes should be admitted");
        stager
            .write_chunk(chunk)
            .await
            .expect("bundle bytes should be written");
    }

    let staged = stager.finish().await.expect("bundle archive should finish");

    assert!(!staging_root.exists());
    assert!(staged.path().starts_with(install_root.path()));
    assert_eq!(
        std::fs::metadata(staged.path())
            .expect("staged archive should exist")
            .len(),
        staged.archive_bytes()
    );
    let id = resource_id("tenant", "model", 1);
    let manifest = store
        .install_from_archive_path(
            id.clone(),
            staged.path(),
            staged.root_checksum().to_string(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("bundle archive should install");
    assert_eq!(manifest.resource.file_count, 1);
    assert_eq!(
        std::fs::read(store.content_root(&id).join("nested/model.bin"))
            .expect("installed bundle file should be readable"),
        b"model-bytes"
    );
}

#[nervix_primitives::test]
async fn bundle_stager_checks_extracted_quota_before_writing_a_chunk() {
    let install_root = tempdir().expect("install tempdir");
    let store = ResourceStore::open_with_limits(
        install_root.path(),
        Executor::default(),
        ResourceStoreLimits {
            max_archive_bytes: 1024 * 1024,
            max_extracted_bytes: 3,
            max_file_count: 1,
        },
    )
    .expect("store should open");
    let mut stager = store
        .create_bundle_stager()
        .await
        .expect("bundle stager should open");
    let staging_root = stager
        .root
        .as_ref()
        .expect("unfinished stager should own its tree")
        .clone();
    stager
        .create_file(Path::new("model.bin"))
        .await
        .expect("bundle file should be created");
    let chunk = store
        .admit_staging_bytes(b"four")
        .await
        .expect("bundle bytes should be admitted");

    let error = stager
        .write_chunk(chunk)
        .await
        .expect_err("the extracted quota must reject the chunk");

    assert!(matches!(
        error.current_context(),
        ResourceStoreError::ExtractedQuotaExceeded {
            actual: 4,
            limit: 3
        }
    ));
    assert_eq!(
        std::fs::metadata(staging_root.join("model.bin"))
            .expect("rejected destination should remain present")
            .len(),
        0,
        "a rejected chunk must not be partially written"
    );
    stager.abort().await.expect("staging should be removed");
    assert!(!staging_root.exists());
}

#[nervix_primitives::test]
async fn dropping_bundle_stager_removes_its_source_tree() {
    let install_root = tempdir().expect("install tempdir");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");
    let stager = store
        .create_bundle_stager()
        .await
        .expect("bundle stager should open");
    let staging_root = stager
        .root
        .as_ref()
        .expect("unfinished stager should own its tree")
        .clone();

    drop(stager);

    nervix_primitives::time::timeout(Duration::from_secs(5), async {
        while staging_root.exists() {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("dropping a bundle stager should schedule source-tree cleanup");
}

#[nervix_primitives::test]
async fn install_from_directory_writes_manifest_and_preserves_tree() {
    let source = tempdir().expect("source tempdir");
    std::fs::create_dir_all(source.path().join("proto"))
        .expect("source directory should be created");
    std::fs::write(source.path().join("model.onnx"), b"onnx")
        .expect("model file should be written");
    std::fs::write(
        source.path().join("proto/schema.proto"),
        b"syntax = \"proto3\";",
    )
    .expect("proto file should be written");

    let install_root = tempdir().expect("install tempdir");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");
    let manifest = store
        .install_from_directory(
            resource_id("tenant", "fraud_model", 1),
            source.path(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("resource should install");

    assert_eq!(manifest.resource.id.version, 1);
    assert_eq!(manifest.resource.file_count, 2);
    assert!(store.manifest_path(&manifest.resource.id).exists());
    assert!(
        store
            .content_root(&manifest.resource.id)
            .join("proto/schema.proto")
            .exists()
    );
    assert!(
        !store
            .content_root(&resource_id("other", "fraud_model", 1))
            .exists(),
        "the same name in another domain must have its own content root"
    );
    assert!(manifest.entries.iter().any(|entry| {
        entry.path == "proto" && entry.content == ResourceEntryContent::Directory
    }));
    assert!(
        manifest
            .entries
            .iter()
            .any(|entry| { entry.path == "model.onnx" && entry.content.is_file() })
    );
}

#[nervix_primitives::test]
async fn install_from_archive_path_rehydrates_same_resource_content() {
    let source = tempdir().expect("source tempdir");
    std::fs::create_dir_all(source.path().join("proto/nested"))
        .expect("nested source directory should be created");
    std::fs::write(source.path().join("model.onnx"), b"onnx")
        .expect("model file should be written");
    std::fs::write(
        source.path().join("proto/nested/schema.proto"),
        b"syntax = \"proto3\";",
    )
    .expect("proto file should be written");

    let install_root = tempdir().expect("install tempdir");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");
    let source_id = resource_id("tenant", "fraud_model", 1);
    let source_manifest = store
        .install_from_directory(
            source_id.clone(),
            source.path(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("resource should install");
    let archive_bytes = read_archive(&store, &source_id).await;

    let temp_archive = NamedTempFile::new().expect("temp archive should be created");
    std::fs::write(temp_archive.path(), &archive_bytes).expect("temp archive should be written");

    let replica_id = resource_id("other", "fraud_model_replica", 7);
    let replica_manifest = store
        .install_from_archive_path(
            replica_id.clone(),
            temp_archive.path(),
            source_manifest.resource.root_checksum.clone(),
            ClusterNodeName::parse("node-2").expect("valid name"),
            Timestamp::from_unix_nanos(84),
        )
        .await
        .expect("resource should install from archive");

    assert_eq!(
        source_manifest.resource.root_checksum,
        replica_manifest.resource.root_checksum
    );
    assert_eq!(
        source_manifest.resource.manifest_checksum,
        replica_manifest.resource.manifest_checksum
    );
    assert!(
        store
            .content_root(&replica_id)
            .join("proto/nested/schema.proto")
            .exists()
    );
    assert!(
        store.archive_path(&replica_id).exists(),
        "replica archive should be written"
    );
}

#[nervix_primitives::test]
async fn install_from_archive_path_preserves_streamed_checksum() {
    let source = tempdir().expect("source tempdir");
    std::fs::create_dir_all(source.path().join("proto"))
        .expect("source directory should be created");
    std::fs::write(source.path().join("model.onnx"), b"onnx")
        .expect("model file should be written");
    std::fs::write(
        source.path().join("proto/schema.proto"),
        b"syntax = \"proto3\";",
    )
    .expect("proto file should be written");

    let install_root = tempdir().expect("install tempdir");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");
    let source_id = resource_id("tenant", "fraud_model", 1);
    let source_manifest = store
        .install_from_directory(
            source_id.clone(),
            source.path(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("resource should install");
    let archive_bytes = read_archive(&store, &source_id).await;

    let temp_archive = NamedTempFile::new().expect("temp archive should be created");
    std::fs::write(temp_archive.path(), &archive_bytes).expect("temp archive should be written");

    let replica_manifest = store
        .install_from_archive_path(
            resource_id("tenant", "fraud_model_streamed", 8),
            temp_archive.path(),
            source_manifest.resource.root_checksum.clone(),
            ClusterNodeName::parse("node-2").expect("valid name"),
            Timestamp::from_unix_nanos(84),
        )
        .await
        .expect("resource should install from archive path");

    assert_eq!(
        replica_manifest.resource.root_checksum,
        source_manifest.resource.root_checksum
    );
    assert_eq!(
        replica_manifest.resource.manifest_checksum,
        source_manifest.resource.manifest_checksum
    );
}

#[nervix_primitives::test]
async fn replica_metadata_is_verified_before_atomic_install() {
    let source = tempdir().expect("source tempdir");
    std::fs::write(source.path().join("model.bin"), b"model")
        .expect("source file should be written");
    let source_root = tempdir().expect("source install tempdir");
    let source_store = ResourceStore::open(source_root.path(), Executor::default())
        .expect("source store should open");
    let source_id = resource_id("tenant", "model", 1);
    let source_manifest = source_store
        .install_from_directory(
            source_id.clone(),
            source.path(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("source resource should install");
    let replica_root = tempdir().expect("replica install tempdir");
    let replica_store = ResourceStore::open(replica_root.path(), Executor::default())
        .expect("replica store should open");
    let replica_id = resource_id("tenant", "model", 2);
    let mut expected = source_manifest.resource;
    expected.id = replica_id.clone();
    expected.created_by_node = ClusterNodeName::parse("node-2").expect("valid name");
    expected.created_at = Timestamp::from_unix_nanos(84);
    expected.manifest_checksum = "different-manifest".to_string();

    let error = replica_store
        .install_replica_from_archive_path(expected, source_store.archive_path(&source_id))
        .await
        .expect_err("mismatched published metadata must fail");

    assert!(
        matches!(
            error.current_context(),
            ResourceStoreError::ResourceMetadataMismatch
        ),
        "unexpected replica install error: {error:?}"
    );
    assert!(!replica_store.staging_root(&replica_id).exists());
    assert!(!replica_store.version_root(&replica_id).exists());
}

#[nervix_primitives::test]
async fn failed_archive_install_removes_staging() {
    let source = tempdir().expect("source tempdir");
    std::fs::write(source.path().join("model.bin"), b"model")
        .expect("source file should be written");
    let install_root = tempdir().expect("install tempdir");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");
    let source_id = resource_id("tenant", "model", 1);
    let source_manifest = store
        .install_from_directory(
            source_id.clone(),
            source.path(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("source resource should install");
    let archive = NamedTempFile::new().expect("temporary archive should open");
    std::fs::copy(store.archive_path(&source_id), archive.path()).expect("archive should copy");
    std::fs::OpenOptions::new()
        .append(true)
        .open(archive.path())
        .expect("archive should open for corruption")
        .write_all(b"corrupt")
        .expect("archive should be corrupted");
    let replica_id = resource_id("tenant", "model", 2);

    let error = store
        .install_from_archive_path(
            replica_id.clone(),
            archive.path(),
            source_manifest.resource.root_checksum,
            ClusterNodeName::parse("node-2").expect("valid name"),
            Timestamp::from_unix_nanos(84),
        )
        .await
        .expect_err("a mismatched archive must fail");
    assert!(matches!(
        error.current_context(),
        ResourceStoreError::ArchiveChecksumMismatch
    ));

    assert!(
        !store.staging_root(&replica_id).exists(),
        "a failed install must remove its partial staging tree"
    );
    assert!(
        !store.version_root(&replica_id).exists(),
        "a failed install must not publish a version directory"
    );
}

#[nervix_primitives::test]
async fn archive_quota_is_checked_before_staging_is_created() {
    let archive = NamedTempFile::new().expect("temporary archive should open");
    std::fs::write(archive.path(), b"too large").expect("archive should be written");
    let install_root = tempdir().expect("install tempdir");
    let store = ResourceStore::open_with_limits(
        install_root.path(),
        Executor::default(),
        ResourceStoreLimits {
            max_archive_bytes: 4,
            ..ResourceStoreLimits::default()
        },
    )
    .expect("store should open");
    let id = resource_id("tenant", "model", 1);

    let error = store
        .install_from_archive_path(
            id.clone(),
            archive.path(),
            "checksum".to_string(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect_err("an archive beyond the staging quota must fail");

    assert!(matches!(
        error.current_context(),
        ResourceStoreError::ArchiveQuotaExceeded {
            actual: 9,
            limit: 4
        }
    ));
    assert!(!store.staging_root(&id).exists());
}

#[nervix_primitives::test]
async fn extraction_failure_removes_staging() {
    let archive = NamedTempFile::new().expect("temporary archive should open");
    let bytes = b"this is not a tar archive";
    std::fs::write(archive.path(), bytes).expect("archive should be written");
    let root_checksum = encode_hex(blake3::hash(bytes).as_bytes());
    let install_root = tempdir().expect("install tempdir");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");
    let id = resource_id("tenant", "model", 1);

    let error = store
        .install_from_archive_path(
            id.clone(),
            archive.path(),
            root_checksum,
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect_err("an invalid tar archive must fail extraction");

    assert!(matches!(
        error.current_context(),
        ResourceStoreError::InvalidArchive
    ));
    assert!(!store.staging_root(&id).exists());
    assert!(!store.version_root(&id).exists());
}

#[nervix_primitives::test]
async fn extracted_quota_failure_removes_staging() {
    let source = tempdir().expect("source tempdir");
    std::fs::write(source.path().join("model.bin"), b"model")
        .expect("source file should be written");
    let source_root = tempdir().expect("source install tempdir");
    let source_store = ResourceStore::open(source_root.path(), Executor::default())
        .expect("source store should open");
    let source_id = resource_id("tenant", "model", 1);
    let source_manifest = source_store
        .install_from_directory(
            source_id.clone(),
            source.path(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("source resource should install");
    let replica_root = tempdir().expect("replica install tempdir");
    let replica_store = ResourceStore::open_with_limits(
        replica_root.path(),
        Executor::default(),
        ResourceStoreLimits {
            max_extracted_bytes: 4,
            ..ResourceStoreLimits::default()
        },
    )
    .expect("replica store should open");
    let replica_id = resource_id("tenant", "model", 2);

    let error = replica_store
        .install_from_archive_path(
            replica_id.clone(),
            source_store.archive_path(&source_id),
            source_manifest.resource.root_checksum,
            ClusterNodeName::parse("node-2").expect("valid name"),
            Timestamp::from_unix_nanos(84),
        )
        .await
        .expect_err("expanded content beyond its quota must fail");

    assert!(matches!(
        error.current_context(),
        ResourceStoreError::ExtractedQuotaExceeded {
            actual: 5,
            limit: 4
        }
    ));
    assert!(!replica_store.staging_root(&replica_id).exists());
    assert!(!replica_store.version_root(&replica_id).exists());
}

#[nervix_primitives::test]
async fn file_count_quota_failure_removes_staging() {
    let source = tempdir().expect("source tempdir");
    std::fs::write(source.path().join("model.bin"), b"model")
        .expect("source file should be written");
    let source_root = tempdir().expect("source install tempdir");
    let source_store = ResourceStore::open(source_root.path(), Executor::default())
        .expect("source store should open");
    let source_id = resource_id("tenant", "model", 1);
    let source_manifest = source_store
        .install_from_directory(
            source_id.clone(),
            source.path(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("source resource should install");
    let replica_root = tempdir().expect("replica install tempdir");
    let replica_store = ResourceStore::open_with_limits(
        replica_root.path(),
        Executor::default(),
        ResourceStoreLimits {
            max_file_count: 0,
            ..ResourceStoreLimits::default()
        },
    )
    .expect("replica store should open");
    let replica_id = resource_id("tenant", "model", 2);

    let error = replica_store
        .install_from_archive_path(
            replica_id.clone(),
            source_store.archive_path(&source_id),
            source_manifest.resource.root_checksum,
            ClusterNodeName::parse("node-2").expect("valid name"),
            Timestamp::from_unix_nanos(84),
        )
        .await
        .expect_err("a file count beyond its quota must fail");

    assert!(matches!(
        error.current_context(),
        ResourceStoreError::FileCountQuotaExceeded {
            actual: 1,
            limit: 0
        }
    ));
    assert!(!replica_store.staging_root(&replica_id).exists());
    assert!(!replica_store.version_root(&replica_id).exists());
}

#[nervix_primitives::test]
async fn startup_cleanup_removes_abandoned_staging_trees() {
    let install_root = tempdir().expect("install tempdir");
    let staging = install_root.path().join("tenant/model/.7.staging/content");
    let staged_archive = install_root.path().join(".archive-abandoned.staging");
    let installed_content = install_root
        .path()
        .join("tenant/model/7/content/.user.staging");
    std::fs::create_dir_all(&staging).expect("staging tree should be created");
    std::fs::create_dir_all(
        installed_content
            .parent()
            .expect("installed content fixture should have a parent"),
    )
    .expect("installed content directory should be created");
    std::fs::write(staging.join("partial.bin"), b"partial")
        .expect("staging file should be written");
    std::fs::write(&staged_archive, b"partial archive").expect("staged archive should be written");
    std::fs::write(&installed_content, b"user content")
        .expect("installed content should be written");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");

    store
        .cleanup_abandoned_staging()
        .await
        .expect("startup cleanup should succeed");

    assert!(!install_root.path().join("tenant/model/.7.staging").exists());
    assert!(!staged_archive.exists());
    assert_eq!(
        std::fs::read(installed_content).expect("installed content must survive cleanup"),
        b"user content"
    );
}

#[nervix_primitives::test]
async fn cancelled_archive_install_removes_staging() {
    let source = tempdir().expect("source tempdir");
    std::fs::write(source.path().join("model.bin"), vec![0_u8; 64 * 1024])
        .expect("source file should be written");
    let source_root = tempdir().expect("source install tempdir");
    let source_store = ResourceStore::open(source_root.path(), Executor::default())
        .expect("source store should open");
    let source_id = resource_id("tenant", "model", 1);
    let source_manifest = source_store
        .install_from_directory(
            source_id.clone(),
            source.path(),
            ClusterNodeName::parse("node-1").expect("valid name"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect("source resource should install");
    let mut execution_config = ExecutionConfig::default();
    execution_config.limits.bulk_chunk_bytes = ByteUnit::Byte(1);
    let executor = Executor::new(execution_config).expect("execution config should be valid");
    let replica_root = tempdir().expect("replica install tempdir");
    let replica_store =
        ResourceStore::open(replica_root.path(), executor).expect("replica store should open");
    let replica_id = resource_id("tenant", "model", 2);
    let staging_root = replica_store.staging_root(&replica_id);
    let installing_store = replica_store.clone();
    let installing_id = replica_id.clone();
    let archive_path = source_store.archive_path(&source_id);
    let root_checksum = source_manifest.resource.root_checksum;
    let install = nervix_primitives::task::spawn(async move {
        installing_store
            .install_from_archive_path(
                installing_id,
                archive_path,
                root_checksum,
                ClusterNodeName::parse("node-2").expect("valid name"),
                Timestamp::from_unix_nanos(84),
            )
            .await
    });
    nervix_primitives::time::timeout(Duration::from_secs(2), async {
        while !staging_root.exists() {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("the install should create its staging tree");

    install.abort();
    match install.await {
        Err(error) if error.is_cancelled() => {}
        result => panic!("the install task should be cancelled, got {result:?}"),
    }
    nervix_primitives::time::timeout(Duration::from_secs(5), async {
        while staging_root.exists() {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled install cleanup should finish");
    assert!(!replica_store.version_root(&replica_id).exists());
}

#[test]
fn resolve_content_path_rejects_parent_segments() {
    let install_root = tempdir().expect("install tempdir");
    let store =
        ResourceStore::open(install_root.path(), Executor::default()).expect("store should open");

    let err = store
        .resolve_content_path(&resource_id("tenant", "fraud_model", 7), "../escape")
        .expect_err("parent segments must be rejected");
    assert!(matches!(
        err.current_context(),
        ResourceStoreError::InvalidResourcePath
    ));
}
