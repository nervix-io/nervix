//! Harness checks for resource-store failure causes.
//! Layer: harness.
//! Owns: typed and rendered diagnostics from production manifest reads.
//! Depends on: the resource store, its executor and current manifest decoding.
//! Must not know: session transports, graph execution or archive installation policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "resource diagnostic regressions")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::Executor;
use nervix_models::{DomainName, ResourceId, ResourceName};
use tempfile::tempdir;

use super::ResourceStore;

#[nervix_primitives::test]
async fn resource_manifest_absence_is_distinct_from_io_and_decode_failures() {
    let root = tempdir().assured("the fixture has a temporary resource root");
    let store = ResourceStore::open(root.path(), Executor::default())
        .assured("the fixture resource root is writable");
    let id = ResourceId::new(
        DomainName::parse("tenant").assured("the domain literal is a valid name"),
        ResourceName::parse("resource").assured("the resource literal is a valid name"),
        1,
    );
    let missing = store
        .read_manifest(&id)
        .await
        .assured("the absent local copy is an ordinary outcome");
    assert_eq!(missing, None);

    let version_root = store.version_root(&id);
    let parent = version_root
        .parent()
        .assured("a version has a resource parent directory");
    std::fs::create_dir_all(parent).assured("the fixture creates its resource directory");
    std::fs::write(&version_root, b"not a directory")
        .assured("the fixture obstructs the version directory");
    let obstructed = store
        .read_manifest(&id)
        .await
        .expect_err("the version path is a file rather than a directory");
    assert_eq!(
        obstructed
            .downcast_ref::<std::io::Error>()
            .assured("opening the obstructed path reports its typed filesystem cause")
            .kind(),
        std::io::ErrorKind::NotADirectory
    );
    std::fs::remove_file(&version_root).assured("the fixture clears its directory obstruction");
    let path = store.manifest_path(&id);
    std::fs::create_dir_all(
        path.parent()
            .assured("a manifest has a version parent directory"),
    )
    .assured("the fixture creates its version directory");
    std::fs::write(&path, b"{").assured("the fixture writes its truncated JSON manifest");
    let malformed = store
        .read_manifest(&id)
        .await
        .expect_err("manifest is truncated");
    assert!(
        malformed
            .downcast_ref::<serde_json::Error>()
            .assured("the truncated current manifest reports its typed JSON cause")
            .is_eof()
    );
    assert!(format!("{malformed:#}").contains("EOF while parsing an object"));
}
