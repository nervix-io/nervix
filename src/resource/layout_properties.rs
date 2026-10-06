//! Resource versions installed into a node's resource store under every name the vocabulary
//! admits, and read back from the directories the store gave them.
//!
//! Layer: test harness.
//! - **Owns.** Generated resource identities whose names meet the store's path segments and the
//!   names of its staging directories, archives that name the version they hold, and the checks
//!   that every version keeps its own directory through installation, staging cleanup and the
//!   removal of another version.
//! - **Depends on.** The resource store, its bundle stager, a temporary directory, and the
//!   vocabulary generators.
//! - **Must not know.** How a version was uploaded, replicated or bound.

use std::{collections::BTreeSet, path::Path};

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_execution::Executor;
use nervix_models::{ClusterNodeName, DomainName, ResourceId, ResourceName, Timestamp};

use super::{ResourceManifest, ResourceStore, ResourceStoreError};

/// The one file every generated archive holds. Its bytes name the version it was built for.
const IDENTITY_FILE: &str = "identity";

/// Resource names spelled like the directories the store itself writes or a filesystem reserves:
/// the directory itself, its parent, staging directories, and version numbers.
const LAYOUT_NAMES: [&str; 6] = [".", "..", ".staging", ".1.staging", "1", "2"];

/// The text an archive built for `id` holds. No name holds a `/`, so no two versions share it.
fn identity_text(id: &ResourceId) -> String {
    format!(
        "{}/{}/{}",
        id.domain.as_str(),
        id.identifier.as_str(),
        id.version
    )
}

fn resource_id(domain: &str, resource: &str, version: u64) -> ResourceId {
    ResourceId::new(
        DomainName::parse(domain).assured("the test domain satisfies the name rule"),
        ResourceName::parse(resource).assured("the test resource satisfies the name rule"),
        version,
    )
}

/// Installs `id` from an archive that holds only its identity file, built by the stager an upload
/// builds its archive with, and returns the manifest the installation reported.
async fn install(store: &ResourceStore, id: &ResourceId) -> ResourceManifest {
    let mut stager = store
        .create_bundle_stager()
        .await
        .assured("a bundle stager opens in a temporary store");
    stager
        .create_file(Path::new(IDENTITY_FILE))
        .await
        .assured("a fixed one-segment path is a valid bundle file");
    let identity = identity_text(id);
    let chunk = store
        .admit_staging_bytes(identity.as_bytes())
        .await
        .assured("a short identity is admitted");
    stager
        .write_chunk(chunk)
        .await
        .assured("a short identity is written");
    let staged = stager
        .finish()
        .await
        .assured("a one-file bundle builds an archive");
    store
        .install_from_archive_path(
            id.clone(),
            staged.path(),
            staged.root_checksum().to_string(),
            ClusterNodeName::parse("node-1").assured("the fixed node name satisfies the name rule"),
            Timestamp::from_unix_nanos(42),
        )
        .await
        .assured("a version installs from the archive its bundle built")
}

/// What the store holds for `id`: its manifest as read back from disk, or the failure, and the
/// bytes of its identity file.
async fn read_back(
    store: &ResourceStore,
    id: &ResourceId,
) -> (Result<ResourceManifest, String>, Option<Vec<u8>>) {
    let manifest = store
        .read_manifest(id)
        .await
        .map_err(|report| format!("{report:?}"));
    let identity = std::fs::read(store.content_root(id).join(IDENTITY_FILE)).ok();
    (manifest, identity)
}

/// Asserts that the store still holds exactly the version `installed` describes, with the
/// identity file the archive built for it held.
async fn assert_holds(store: &ResourceStore, installed: &ResourceManifest) {
    let id = &installed.resource.id;
    let (manifest, identity) = read_back(store, id).await;
    assert_eq!(
        manifest,
        Ok(installed.clone()),
        "version {} of resource `{}` in domain `{}` reads back as it was installed",
        id.version,
        id.identifier.as_str(),
        id.domain.as_str()
    );
    assert_eq!(identity, Some(identity_text(id).into_bytes()));
}

fn open_store(root: &Path) -> ResourceStore {
    ResourceStore::open(root, Executor::default()).assured("a store opens in a temporary directory")
}

#[nervix_primitives::test]
async fn a_resource_named_dot_keeps_its_versions_apart_from_a_resource_named_like_a_version() {
    let directory = tempfile::tempdir().assured("the test host provides a temporary directory");
    let store = open_store(directory.path());
    let numbered = install(&store, &resource_id("tenant", "1", 1)).await;
    let dot = install(&store, &resource_id("tenant", ".", 1)).await;
    assert_holds(&store, &numbered).await;
    assert_holds(&store, &dot).await;
}

#[nervix_primitives::test]
async fn resources_named_dot_dot_in_two_domains_keep_their_own_versions() {
    let directory = tempfile::tempdir().assured("the test host provides a temporary directory");
    let store = open_store(directory.path());
    let first = install(&store, &resource_id("first", "..", 1)).await;
    let second = install(&store, &resource_id("second", "..", 1)).await;
    assert_holds(&store, &first).await;
    assert_holds(&store, &second).await;
}

#[nervix_primitives::test]
async fn a_resource_named_like_a_staging_directory_survives_staging_cleanup() {
    let directory = tempfile::tempdir().assured("the test host provides a temporary directory");
    let store = open_store(directory.path());
    let staging = install(&store, &resource_id("tenant", ".staging", 1)).await;
    let numbered_staging = install(&store, &resource_id("tenant", ".1.staging", 1)).await;
    store
        .cleanup_abandoned_staging()
        .await
        .assured("staging cleanup walks a readable temporary store");
    assert_holds(&store, &staging).await;
    assert_holds(&store, &numbered_staging).await;
}

/// Up to four distinct resource versions in at most two domains. Half the names are spelled like
/// the directories the store writes, and the rest are drawn from the whole name rule.
fn generated_versions(arbitrary: &mut Arbitrary<'_>) -> Vec<ResourceId> {
    let domains: [DomainName; 2] = [arbitrary.rule_name(), arbitrary.rule_name()];
    let count = arbitrary.entropy().between(1..=4);
    let mut seen = BTreeSet::new();
    let mut ids = Vec::new();
    for _ in 0..count {
        let domain = arbitrary.entropy().pick(domains.clone());
        let resource = if arbitrary.entropy().flag() {
            ResourceName::parse(arbitrary.entropy().pick(LAYOUT_NAMES))
                .assured("every layout name satisfies the resource name rule")
        } else {
            arbitrary.rule_name()
        };
        let version = arbitrary.entropy().pick([1, 2, u64::MAX]);
        let id = ResourceId::new(domain, resource, version);
        // A second installation of one version replaces it, so each version is installed once.
        if seen.insert(id.clone()) {
            ids.push(id);
        }
    }
    ids
}

/// Every installed version reads back its complete manifest and the archive built for it, after
/// every other version installed, after staging cleanup runs as it does at startup, and after
/// another version is removed; the removed version alone is gone, with the store's typed failure.
#[test]
fn bolero_resource_versions_keep_their_own_directories() {
    bolero::check!()
        .with_iterations(64)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let ids = generated_versions(&mut arbitrary);
            let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .assured("a property runtime opens");
            runtime.block_on(async {
                let directory =
                    tempfile::tempdir().assured("the test host provides a temporary directory");
                let store = open_store(directory.path());
                let mut installed = Vec::new();
                for id in &ids {
                    installed.push(install(&store, id).await);
                }
                for manifest in &installed {
                    assert_holds(&store, manifest).await;
                }

                store
                    .cleanup_abandoned_staging()
                    .await
                    .assured("staging cleanup walks a readable temporary store");
                for manifest in &installed {
                    assert_holds(&store, manifest).await;
                }

                let Some((removed, kept)) = installed.split_first() else {
                    return;
                };
                store
                    .remove_version(&removed.resource.id)
                    .await
                    .assured("an installed version is removed");
                for manifest in kept {
                    assert_holds(&store, manifest).await;
                }
                let failure = store
                    .read_manifest(&removed.resource.id)
                    .await
                    .expect_err("a removed version has no manifest");
                assert!(
                    matches!(failure.current_context(), ResourceStoreError::ReadFile),
                    "{failure:?}"
                );
            });
        });
}
