//! Layer: test harness.
//! Owns: recovery generation and restored lifecycle state regressions.
//! May depend on: runtime state replication fixtures in the parent test module.
//! Must not know: edge protocols or production scheduling decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

/// Forced recovery selects checkpoints only from the generation it recovers. A snapshot of a replaced
/// generation stays on disk with a higher revision than anything current, and is still never
/// selected.
#[nervix_primitives::test]
async fn forced_recovery_never_selects_a_checkpoint_of_a_replaced_generation() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("database should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(60))
        .expect("runtime with persistence should open");
    let domain = domain("default");
    let acme = string_branch_key("tenant", "acme");
    let mut node = wasm_processor_node();
    runtime.install_schedule_state_identities(&DomainSchedule::new(
        domain.clone(),
        vec![node.clone()],
        Vec::new(),
    ));
    let replaced = guest_state_placement(&runtime, &domain, acme.clone());
    runtime
        .inner
        .state_store
        .as_ref()
        .expect("the runtime has a state store")
        .persist_latest_snapshot(&replaced, 9, &[7, 7])
        .expect("the replaced generation's guest state should persist");

    node.begin_wasm_state_generation();
    runtime.install_schedule_state_identities(&DomainSchedule::new(
        domain.clone(),
        vec![node],
        Vec::new(),
    ));
    let current = guest_state_placement(&runtime, &domain, acme);
    let deadline = nervix_primitives::time::Instant::now() + Duration::from_secs(5);

    let recovered = runtime
        .forced_recovery_checkpoint(&current, &[], deadline)
        .await;
    assert!(recovered.snapshot.is_none());
    assert_eq!(
        recovered.reset_cause,
        OwnershipStateResetCause::MissingCheckpoint
    );
    let stale = runtime
        .forced_recovery_checkpoint(&replaced, &[], deadline)
        .await;
    assert_eq!(stale.snapshot.map(|snapshot| snapshot.lsm), Some(9));
}

/// A restore reads its entity's lifecycle without releasing it, so a failed attempt reads the same
/// checkpoint again. Releasing consumes a checkpoint a transfer left only while it is still the one
/// the restore read, and never the lifecycle the node holds.
#[nervix_primitives::test]
async fn a_restore_releases_only_the_transferred_lifecycle_it_read() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let processor = named::<ModelName>("open_windows");
    publish_state_identity(
        &runtime,
        &domain,
        ModelKind::WindowProcessor,
        processor.clone(),
    );
    let placement = runtime
        .state_placement(
            &domain,
            RuntimeStateKind::BranchLru,
            ModelKind::WindowProcessor,
            processor,
            None,
        )
        .expect("the processor's lifecycle is placed");
    let alpha = string_branch_key("tenant", "alpha");
    let transfer = |lsm| PreparedRuntimeStateSnapshot {
        preparation: RuntimeStatePreparationIdentity::ForcedRecovery,
        snapshot: branch_lifecycle_snapshot(lsm, std::slice::from_ref(&alpha)),
    };
    let restorable = |runtime: &Runtime| {
        runtime
            .restorable_branch_lru_snapshot(&placement)
            .expect("the lifecycle reads")
            .expect("the entity has a lifecycle")
    };
    runtime
        .inner
        .prepared_runtime_state_snapshots
        .insert(placement.clone(), transfer(5));

    let failed = restorable(&runtime);
    drop(failed);
    let read = restorable(&runtime);
    assert_eq!(
        read.lsm(),
        5,
        "a failed restore leaves the transfer to read again"
    );
    assert_eq!(
        read.branches()
            .expect("the transferred lifecycle decodes")
            .into_iter()
            .map(|branch| branch.key)
            .collect::<Vec<_>>(),
        vec![alpha.clone()]
    );

    runtime
        .inner
        .prepared_runtime_state_snapshots
        .insert(placement.clone(), transfer(7));
    runtime.release_restored_branch_lru_snapshot(read);
    let replacement = restorable(&runtime);
    assert_eq!(
        replacement.lsm(),
        7,
        "releasing an earlier read keeps the transfer that replaced it"
    );
    runtime.release_restored_branch_lru_snapshot(replacement);
    assert!(
        runtime
            .inner
            .prepared_runtime_state_snapshots
            .get(&placement)
            .is_none(),
        "releasing the transfer a restore installed consumes it"
    );

    runtime
        .persist_branch_lru_snapshot(
            placement.clone(),
            branch_lifecycle_snapshot(8, std::slice::from_ref(&alpha)),
        )
        .expect("the node holds its lifecycle");
    let held = restorable(&runtime);
    assert_eq!(held.lsm(), 8);
    runtime.release_restored_branch_lru_snapshot(held);
    assert_eq!(
        restorable(&runtime).lsm(),
        8,
        "the lifecycle the node holds outlives the restore that read it"
    );
}
