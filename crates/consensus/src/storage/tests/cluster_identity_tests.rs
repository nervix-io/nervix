//! Identity installation, durable recovery and snapshot validation through the production store.
//!
//! Layer: test harness.
//! - **Owns.** Verifying complete identity preservation and refusal of corrupt singleton storage.
//! - **Depends on.** The real Raft state-machine, snapshot and database owners and current fixtures.
//! - **Must not know.** Bootstrap scheduling, session formatting or private-key interpretation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns persisted identity verification")
)]

use meticulous::OptionExt as _;
use openraft::storage::{RaftSnapshotBuilder as _, RaftStateMachine as _};

use super::{Harness, TestResult};
use crate::{
    ConsensusCommand,
    cluster_identity::tests::{candidate, inconsistent_candidate},
    durable_batch::DurableBatch,
    storage::{KEY_CLUSTER_IDENTITY, KEY_METADATA, StateMetadata, StateMetadataRecord},
};

#[nervix_primitives::test]
async fn first_installed_identity_survives_reopen_snapshot_installation_and_reopen() -> TestResult {
    let mut source = Harness::new().await?;
    let first = candidate(0x11);
    source
        .apply(
            1,
            ConsensusCommand::InstallClusterIdentity {
                identity: Box::new(first.clone()),
            },
        )
        .await?;
    let revision = source.store.inner.state().runtime_revision;
    source
        .apply(
            2,
            ConsensusCommand::InstallClusterIdentity {
                identity: Box::new(candidate(0x22)),
            },
        )
        .await?;
    assert_eq!(source.store.inner.state().runtime_revision, revision);
    assert_eq!(
        source.store.inner.state().cluster_identity.as_deref(),
        Some(&first)
    );
    let mut source = source.reopen().await?;
    assert_eq!(
        source.store.inner.state().cluster_identity.as_deref(),
        Some(&first)
    );
    let snapshot = source.store.build_snapshot().await?;
    let mut target = Harness::new().await?;
    let staged = target.receive(&snapshot.snapshot).await?;
    target
        .store
        .install_snapshot(&snapshot.meta, staged)
        .await?;
    assert_eq!(
        target.store.inner.state().cluster_identity.as_deref(),
        Some(&first)
    );
    let target = target.reopen().await?;
    assert_eq!(
        target.store.inner.state().cluster_identity.as_deref(),
        Some(&first)
    );
    Ok(())
}

enum StorageDamage {
    MissingIdentity,
    MetadataDisagrees,
    KeyDisagrees,
}

#[nervix_primitives::test]
async fn incomplete_or_disagreeing_identity_storage_cannot_open_as_a_new_cluster() -> TestResult {
    for damage in [
        StorageDamage::MissingIdentity,
        StorageDamage::MetadataDisagrees,
        StorageDamage::KeyDisagrees,
    ] {
        nervix_primitives::task::consume_budget().await;
        let mut harness = Harness::new().await?;
        harness
            .apply(
                1,
                ConsensusCommand::InstallClusterIdentity {
                    identity: Box::new(candidate(0x11)),
                },
            )
            .await?;
        match damage {
            StorageDamage::MissingIdentity => {
                harness.store.inner.sm.remove(KEY_CLUSTER_IDENTITY)?;
            }
            StorageDamage::MetadataDisagrees => {
                let mut metadata = StateMetadata::read(&harness.store.inner.sm)?
                    .verified("applying the identity wrote metadata");
                metadata.cluster_identity = Some(candidate(0x22).cluster_uid());
                let encoded =
                    DurableBatch::encode(&StateMetadataRecord::from(&metadata), 16 * 1024)?;
                harness.store.inner.sm.insert(KEY_METADATA, encoded)?;
            }
            StorageDamage::KeyDisagrees => {
                let encoded = DurableBatch::encode(&inconsistent_candidate(), 16 * 1024)?;
                harness
                    .store
                    .inner
                    .sm
                    .insert(KEY_CLUSTER_IDENTITY, encoded)?;
            }
        }
        harness
            .store
            .inner
            .db
            .persist(fjall::PersistMode::SyncAll)?;
        let Err(error) = harness.reopen().await else {
            panic!("damaged identity storage cannot reopen or regenerate an identity");
        };
        assert!(
            error
                .to_string()
                .contains("invalid consensus record storage"),
            "{error}"
        );
    }
    Ok(())
}

#[nervix_primitives::test]
async fn an_inconsistent_identity_in_a_snapshot_is_never_published() -> TestResult {
    let mut source = Harness::new().await?;
    source
        .apply(
            1,
            ConsensusCommand::InstallClusterIdentity {
                identity: Box::new(candidate(0x11)),
            },
        )
        .await?;
    let encoded = DurableBatch::encode(&inconsistent_candidate(), 16 * 1024)?;
    source
        .store
        .inner
        .sm
        .insert(KEY_CLUSTER_IDENTITY, encoded)?;
    source.store.inner.db.persist(fjall::PersistMode::SyncAll)?;
    let snapshot = source.store.build_snapshot().await?;
    let mut target = Harness::new().await?;
    let preceding = target.store.inner.state();
    let staged = target.receive(&snapshot.snapshot).await?;
    let Err(error) = target.store.install_snapshot(&snapshot.meta, staged).await else {
        panic!("a snapshot cannot publish a mismatched private/public identity");
    };
    assert!(
        error
            .to_string()
            .contains("invalid consensus record storage"),
        "{error}"
    );
    assert_eq!(target.store.inner.state(), preceding);
    Ok(())
}
