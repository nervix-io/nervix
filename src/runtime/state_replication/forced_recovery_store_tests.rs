//! Layer: test harness.
//! - **Owns.** Forced recovery staging, discard and activation checks for the runtime state store.
//! - **Depends on.** Runtime state persistence and checkpoint test fixtures.
//! - **Must not know.** Cluster scheduling, transport or graph orchestration.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns forced recovery persistence tests")
)]

use fjall::Database;
use nervix_models::{DomainNodeRef, ModelKind, SchemaFingerprint, SettingDefinition as _};
use tempfile::tempdir;

use super::*;
use crate::runtime::RestoreStagingMaxBytes;

#[test]
fn forced_recovery_refuses_missing_or_stale_preparation_without_changing_state() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("database should open");
    let store = RuntimeStateStore::from_database(
        db,
        Executor::default(),
        RestoreStagingMaxBytes::default_value(&()).as_u64(),
    )
    .expect("state store should open");
    let domain = domain("default");
    let source = named::<ClusterNodeName>("node-1");
    let destination = named::<ClusterNodeName>("node-2");
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("deduplicate_orders"),
        unit: nervix_models::Unit::SINGLE,
        branch_key: string_branch_key("tenant", "acme"),
    };
    let sibling_branch = RuntimeStatePlacement {
        unit: nervix_models::Unit::SINGLE,
        branch_key: string_branch_key("tenant", "globex"),
        ..placement.clone()
    };
    let other_entity = RuntimeStatePlacement {
        identifier: named("other_deduplicator"),
        ..placement.clone()
    };
    let payload = ReplicatedDeduplicatorState::new(placement.clone(), None)
        .expect("deduplicator state should initialize")
        .latest_snapshot()
        .expect("deduplicator state should snapshot")
        .payload;
    store
        .persist_latest_snapshot(&placement, 6, &payload)
        .expect("current branch checkpoint should persist");
    store
        .persist_latest_snapshot(&sibling_branch, 7, &payload)
        .expect("sibling branch checkpoint should persist");
    store
        .persist_latest_snapshot(&other_entity, 8, &payload)
        .expect("other entity checkpoint should persist");
    let entity = DomainNodeRef::node_in(
        domain,
        ModelKind::Deduplicator,
        placement.identifier.clone(),
    );
    let prepared_recovery = ForcedRuntimeStateRecoveryTransition {
        operation_id: "recover-deduplicator",
        source: &source,
        destination: &destination,
        destination_incarnation: ClusterNodeIncarnation::new(42),
        entity: &entity,
        unit: nervix_models::Unit::SINGLE,
        target_schedule_fingerprint: [9; 32],
    };

    let missing = store
        .activate_forced_recovery(
            &prepared_recovery,
            ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
            None,
        )
        .expect_err("activation without a preparation should fail");
    assert!(matches!(
        missing.current_context(),
        RuntimePersistenceError::MissingForcedRecoveryPreparation
    ));
    let stale_checkpoint = PersistedRuntimeStateEntry {
        lsm: 5,
        payload: payload.clone(),
    };
    store
        .persist_forced_recovery_preparation(
            &prepared_recovery,
            &[(placement.clone(), stale_checkpoint)],
        )
        .expect("preparation should persist");
    let changed_incarnation = ForcedRuntimeStateRecoveryTransition {
        destination_incarnation: ClusterNodeIncarnation::new(43),
        ..prepared_recovery
    };
    store
        .discard_forced_recovery_preparation(&changed_incarnation)
        .expect("discarding a different attempt is an idempotent success");
    let stale_incarnation = store
        .activate_forced_recovery(
            &changed_incarnation,
            ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
            None,
        )
        .expect_err("a preparation for another process incarnation should fail");
    assert!(matches!(
        stale_incarnation.current_context(),
        RuntimePersistenceError::ForcedRecoveryPreparationMismatch
    ));
    let changed_fingerprint = ForcedRuntimeStateRecoveryTransition {
        target_schedule_fingerprint: [10; 32],
        ..prepared_recovery
    };
    store
        .discard_forced_recovery_preparation(&changed_fingerprint)
        .expect("discarding a different target schedule preserves the staged record");
    let stale_fingerprint = store
        .activate_forced_recovery(
            &changed_fingerprint,
            ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
            None,
        )
        .expect_err("a preparation for another schedule should fail");
    assert!(matches!(
        stale_fingerprint.current_context(),
        RuntimePersistenceError::ForcedRecoveryPreparationMismatch
    ));
    for _ in 0..2 {
        store
            .discard_forced_recovery_preparation(&prepared_recovery)
            .expect("discarding the exact preparation is durable and retryable");
    }
    let discarded = store
        .activate_forced_recovery(
            &prepared_recovery,
            ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
            None,
        )
        .expect_err("the discarded attempt must not leave staged checkpoints");
    assert!(matches!(
        discarded.current_context(),
        RuntimePersistenceError::MissingForcedRecoveryPreparation
    ));
    assert_eq!(
        store
            .latest_snapshot(&placement)
            .expect("current branch checkpoint should load")
            .expect("current branch checkpoint should remain")
            .lsm,
        6
    );
    assert_eq!(
        store
            .latest_snapshot(&sibling_branch)
            .expect("sibling branch checkpoint should load")
            .expect("sibling branch checkpoint should remain")
            .lsm,
        7
    );
    assert_eq!(
        store
            .latest_snapshot(&other_entity)
            .expect("other entity checkpoint should load")
            .expect("other entity checkpoint should remain")
            .lsm,
        8
    );
}
