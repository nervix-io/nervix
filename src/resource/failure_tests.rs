//! Harness checks for resource-store failure causes.
//! Layer: harness.
//! Owns: typed and rendered diagnostics from production manifest reads.
//! Depends on: the resource store, its executor and current manifest decoding.
//! Must not know: session transports, graph execution or archive installation policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "resource diagnostic regressions")
)]

use nervix_execution::Executor;
use nervix_models::{DomainName, ResourceId, ResourceName};
use tempfile::tempdir;

use super::ResourceStore;

#[nervix_primitives::test]
async fn resource_read_failures_retain_typed_io_and_json_causes() {
    let root = tempdir().expect("resource root");
    let store =
        ResourceStore::open(root.path(), Executor::default()).expect("resource store opens");
    let id = ResourceId::new(
        DomainName::parse("tenant").expect("valid domain"),
        ResourceName::parse("resource").expect("valid resource"),
        1,
    );
    let missing = store
        .read_manifest(&id)
        .await
        .expect_err("manifest is missing");
    assert_eq!(
        missing
            .downcast_ref::<std::io::Error>()
            .expect("typed I/O cause")
            .kind(),
        std::io::ErrorKind::NotFound
    );
    let path = store.manifest_path(&id);
    std::fs::create_dir_all(path.parent().expect("manifest parent")).expect("version directory");
    std::fs::write(&path, b"{").expect("truncated JSON manifest");
    let malformed = store
        .read_manifest(&id)
        .await
        .expect_err("manifest is truncated");
    assert!(
        malformed
            .downcast_ref::<serde_json::Error>()
            .expect("typed JSON cause")
            .is_eof()
    );
    assert!(format!("{malformed:#}").contains("EOF while parsing an object"));
}
