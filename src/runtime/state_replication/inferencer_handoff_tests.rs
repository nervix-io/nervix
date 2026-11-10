//! Layer: test harness.
//! Owns: focused verification of how ownership handoffs and forced recovery carry a stateful
//! inferencer branch's tensors.
//! May depend on: runtime internals and test-only storage fixtures.
//! Must not know: production control-plane orchestration or edge protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use fjall::Database;
use nervix_models::{ModelKind, OwnershipStateResetCause};
use nervix_primitives::sync::Arc;
use tempfile::tempdir;

use super::{tests::unchanged_schema_fingerprint, *};

fn inferencer_placement(tenant: &str) -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::Inferencer {
            schema: unchanged_schema_fingerprint(),
            model: nervix_models::InferencerModelFingerprint::from_digest([9; 32]),
        },
        kind: ModelKind::Inferencer,
        identifier: named("running"),
        unit: nervix_models::Unit::SINGLE,
        branch_key: string_branch_key("tenant", tenant),
    }
}

/// The freeze makes a stateful inferencer branch publish its tensors, so its handoff carries that
/// publication even while the source's storage still holds an older periodic snapshot.
#[nervix_primitives::test]
async fn a_handoff_carries_an_inferencer_publication_newer_than_its_stored_snapshot() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("database should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("runtime should open persisted state");
    let placement = inferencer_placement("acme");
    let store = runtime
        .inner
        .state_store
        .as_ref()
        .expect("the runtime persists runtime state");
    store
        .persist_latest_snapshot(&placement, 1, b"an older periodic snapshot")
        .expect("the periodic snapshot should persist");
    let state = Arc::new(ReplicatedInferencerState::from_generations(
        placement.clone(),
        crate::runtime::published_generation::PublishedGenerations::restored(1, None),
    ));
    state.publish(None);
    state.publish(None);
    runtime.publish_state_replication_route(&placement, ReplicatedState::Inferencer(state.clone()));

    let carried = runtime
        .uncaptured_handoff_checkpoint(&placement)
        .await
        .expect("the frozen branch's publication should encode")
        .expect("the frozen branch's publication should be carried");
    let published = state
        .snapshot_after(None, &runtime.inner.executor)
        .await
        .expect("the publication should encode")
        .expect("the branch has a publication");

    assert_eq!(carried.lsm, 3);
    assert_eq!(carried, published);
}

/// A branch without a routed inference state, as in a stopped domain, carries the newest
/// checkpoint on the source's storage, like every other placement the frozen maps did not hold.
#[nervix_primitives::test]
async fn a_handoff_without_a_routed_inferencer_state_carries_the_stored_snapshot() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("database should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("runtime should open persisted state");
    let placement = inferencer_placement("beta");
    let store = runtime
        .inner
        .state_store
        .as_ref()
        .expect("the runtime persists runtime state");
    store
        .persist_latest_snapshot(&placement, 4, b"the stopped branch's final snapshot")
        .expect("the final snapshot should persist");

    let carried = runtime
        .uncaptured_handoff_checkpoint(&placement)
        .await
        .expect("the stored snapshot should read")
        .expect("the stored snapshot should be carried");

    assert_eq!(carried.lsm, 4);
    assert_eq!(carried.payload, b"the stopped branch's final snapshot");
}

/// A forced recovery that finds no surviving tensors for an inferencer branch its recovered
/// lifecycle names resets that branch with the cause `missing_checkpoint`, and stages an empty
/// generation, so the branch restores its declared `INITIAL` tensors.
#[nervix_primitives::test]
async fn forced_recovery_resets_an_inferencer_branch_without_surviving_tensors_to_initial() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("database should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("runtime should open persisted state");
    let placement = inferencer_placement("acme");
    let deadline = nervix_primitives::time::Instant::now() + Duration::from_secs(5);

    let recovered = runtime
        .forced_recovery_checkpoint(&placement, &[], deadline)
        .await;
    assert!(recovered.snapshot.is_none());
    assert_eq!(
        recovered.reset_cause,
        OwnershipStateResetCause::MissingCheckpoint
    );

    let staged = runtime
        .empty_forced_branch_state_snapshot(&placement)
        .await
        .expect("the empty generation should encode");
    let executor = &runtime.inner.executor;
    let limit = executor.limits().snapshot_section_bytes.as_u64();
    let charge = executor
        .reserve(
            nervix_execution::MemoryClass::Bulk,
            crate::inferencer_state_snapshot::footprint([])
                .expect("an empty generation has a footprint"),
        )
        .await
        .expect("decoding the empty generation is admitted");
    let decoded = executor
        .run_cpu(
            nervix_execution::CpuClass::Bulk,
            charge,
            move |_charge, cancellation| {
                crate::inferencer_state_snapshot::Snapshot::decode(
                    staged.payload,
                    limit,
                    cancellation,
                )
            },
        )
        .await
        .expect("the decode job should run")
        .expect("the staged generation should decode");
    assert!(
        decoded.branch.is_none(),
        "the staged generation must carry no tensors, so the branch restores INITIAL"
    );
}
