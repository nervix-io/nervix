//! Activated licensing recovery and snapshot verification through the actual state store.
//!
//! Layer: test harness.
//! - **Owns.** Exact signed record preservation and semantic degradation across storage boundaries.
//! - **Depends on.** The production storage traits, protocol fixtures and current command owner.
//! - **Must not know.** HTTP, runtime policy publication or clock scheduling.

use meticulous::OptionExt as _;
use openraft::storage::{RaftSnapshotBuilder as _, RaftStateMachine as _};

use super::{Harness, TestResult};
use crate::{
    ConsensusCommand, LicensingMode, durable_batch::DurableBatch,
    licensing::verification_tests::SignedFixture, storage::KEY_LICENSING,
};

#[nervix_primitives::test]
async fn signed_activation_survives_reopen_snapshot_installation_and_semantic_damage() -> TestResult
{
    let fixture = SignedFixture::new();
    let mut source = Harness::with_licensing(
        nervix_execution::Executor::default(),
        fixture.environment.clone(),
    )
    .await?;
    source
        .apply(
            1,
            ConsensusCommand::InstallClusterIdentity {
                identity: Box::new(fixture.identity.clone()),
            },
        )
        .await?;
    let received = fixture.received(&fixture.decision, &[]);
    let binding = fixture.binding(&received);
    source
        .apply(
            2,
            ConsensusCommand::ActivateLicense {
                binding: Box::new(binding.clone()),
                decision: Box::new(received),
            },
        )
        .await?;
    let committed = source
        .store
        .inner
        .state()
        .licensing
        .clone()
        .verified("activation committed");
    source = source.reopen().await?;
    assert_eq!(
        source.store.inner.state().licensing.as_ref(),
        Some(&committed)
    );
    assert!(
        source
            .store
            .inner
            .state()
            .licensing_verification
            .as_ref()
            .verified("verification rebuilt")
            .is_ok()
    );
    let snapshot = source.store.build_snapshot().await?;
    let mut target = Harness::with_licensing(
        nervix_execution::Executor::default(),
        fixture.environment.clone(),
    )
    .await?;
    let staged = target.receive(&snapshot.snapshot).await?;
    target
        .store
        .install_snapshot(&snapshot.meta, staged)
        .await?;
    target = target.reopen().await?;
    assert_eq!(
        target.store.inner.state().licensing.as_ref(),
        Some(&committed)
    );
    assert!(
        target
            .store
            .inner
            .state()
            .licensing_verification
            .as_ref()
            .verified("snapshot verification rebuilt")
            .is_ok()
    );
    let mut damaged = committed.as_ref().clone();
    let LicensingMode::Activated(activation) = &mut damaged.mode else {
        unreachable!()
    };
    activation.decision.bytes.push('!');
    let encoded = DurableBatch::encode(&damaged, 64 * 1024)?;
    target.store.inner.sm.insert(KEY_LICENSING, encoded)?;
    target.store.inner.db.persist(fjall::PersistMode::SyncAll)?;
    target = target.reopen().await?;
    assert!(
        target
            .store
            .inner
            .state()
            .licensing_verification
            .as_ref()
            .verified("damaged signed bytes produce a derived verdict")
            .is_err()
    );
    let recovered = target
        .store
        .inner
        .state()
        .licensing
        .clone()
        .verified("node loads its current record");
    let LicensingMode::Activated(activation) = &recovered.mode else {
        unreachable!()
    };
    assert_eq!(activation.binding, binding);
    let damaged_snapshot = target.store.build_snapshot().await?;
    let mut receiver = Harness::with_licensing(
        nervix_execution::Executor::default(),
        fixture.environment.clone(),
    )
    .await?;
    let staged = receiver.receive(&damaged_snapshot.snapshot).await?;
    receiver
        .store
        .install_snapshot(&damaged_snapshot.meta, staged)
        .await?;
    assert!(
        receiver
            .store
            .inner
            .state()
            .licensing_verification
            .as_ref()
            .verified("snapshot installs with an unverifiable derived view")
            .is_err()
    );
    Ok(())
}
