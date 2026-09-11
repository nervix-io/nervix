//! Layer: test harness.
//! Owns: focused verification of runtime state persistence and replication.
//! May depend on: runtime internals and test-only storage fixtures.
//! Must not know: production control-plane orchestration or edge protocols.

use std::sync::atomic::{AtomicBool, Ordering};

use ahash::HashMap;
use fjall::Database;
use nervix_models::{
    ClusterNodeName, DomainSchedule, ModelKind, ModelName, NodeRef, ParseAsType, ScheduledNode,
    Timestamp,
};
use nonzero_ext::nonzero;
use tempfile::tempdir;
use tokio::{
    sync::{mpsc, watch},
    time::{Duration, timeout},
};
use triomphe::Arc;

use super::*;
use crate::{
    metrics::RuntimeMetrics,
    runtime_schema::{RuntimeValue, test_runtime_row},
};

#[test]
fn recovered_handoff_retries_schedule_rebuild_until_activation() {
    let mut recovered = OwnershipHandoffActivationAuthorization::RecoveredAwaitingRequest;

    assert!(recovered.authorize());
    assert!(recovered.is_authorized());
    assert!(
        recovered.authorize(),
        "a failed or cancelled recovered schedule rebuild must remain retriable"
    );

    let mut ordinary = OwnershipHandoffActivationAuthorization::AuthorizedByPreparation;
    assert!(!ordinary.authorize());
    assert!(ordinary.is_authorized());
}

#[test]
fn runtime_state_store_persists_latest_snapshot_with_monotonic_lsm() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };

    let first_lsm = 1;
    store
        .persist_latest_snapshot(&placement, first_lsm, b"first")
        .expect("first snapshot should persist");
    let second_lsm = 2;
    store
        .persist_latest_snapshot(&placement, second_lsm, b"second")
        .expect("second snapshot should persist");

    assert_eq!(first_lsm, 1);
    assert_eq!(second_lsm, 2);
    assert_eq!(
        store
            .latest_snapshot(&placement)
            .expect("latest snapshot should load")
            .expect("latest snapshot should exist")
            .payload,
        b"second".to_vec()
    );
}

#[tokio::test]
async fn deduplicator_snapshot_task_persists_dirty_state_on_interval() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_millis(10))
        .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_deduplicator_state(placement.clone())
        .expect("deduplicator state should initialize");
    let (shutdown_tx, _) = watch::channel(false);
    let task = runtime
        .spawn_deduplicator_snapshot_task(&shutdown_tx, state.clone())
        .expect("persisted runtime should spawn a snapshot task");

    assert!(state.reserve_new_key(
        DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]),
        Timestamp::from_unix_nanos(1),
        Duration::from_secs(600),
    ));
    let expected_lsm = state.current_lsm.current();

    wait_for_persisted_runtime_state_lsm(&runtime, &placement, expected_lsm).await;
    assert_eq!(
        state.last_persisted_lsm.load(Ordering::SeqCst),
        expected_lsm
    );
    assert!(!state.dirty.load(Ordering::SeqCst));

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");
}

#[tokio::test]
async fn materialized_relay_snapshot_task_owns_persistence() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::MaterializedRelay,
        kind: ModelKind::Relay,
        identifier: named("latest_orders"),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    let schema = test_schema(&[("status", ParseAsType::String)]);
    let mut assignment = runtime
        .replicated_materialized_stream_state(
            placement.clone(),
            schema.arrow_schema(),
            None,
            Vec::new(),
            None,
        )
        .expect("materialized relay state should initialize");
    let state = assignment
        .originator
        .take()
        .expect("branch-local state should grant authoritative access");
    let persistence = assignment.persistence;
    let (shutdown_tx, _) = watch::channel(false);
    let task = runtime
        .spawn_materialized_stream_snapshot_task(&shutdown_tx, persistence.clone())
        .expect("persisted runtime should spawn a snapshot task");
    let record = test_runtime_row([(
        "status".to_string(),
        RuntimeValue::String("ready".to_string()),
    )]);

    runtime
        .update_materialized_stream_last_by_timestamp(&state, &None, &record)
        .expect("the materialized state assignment should remain authoritative");

    assert_eq!(state.read().current_lsm(), 1);
    assert!(persistence.is_dirty());
    assert_eq!(persistence.last_persisted_lsm(), 0);
    assert!(
        runtime
            .inner
            .state_store
            .as_ref()
            .expect("test runtime should have a state store")
            .latest_snapshot(&placement)
            .expect("snapshot lookup should succeed")
            .is_none(),
        "the relay-state hot path must not persist a snapshot"
    );

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");
    assert_eq!(
        runtime
            .inner
            .state_store
            .as_ref()
            .expect("test runtime should have a state store")
            .latest_snapshot(&placement)
            .expect("snapshot lookup should succeed")
            .expect("shutdown should flush the dirty snapshot")
            .lsm,
        1
    );
}

#[tokio::test]
async fn window_processor_snapshot_task_persists_dirty_state_on_interval() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_millis(10))
        .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::WindowProcessor,
        kind: ModelKind::WindowProcessor,
        identifier: named("latency_window"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_window_processor_state(placement.clone())
        .expect("window processor state should initialize");
    let (shutdown_tx, _) = watch::channel(false);
    let (snapshot_request_tx, mut snapshot_requests) = mpsc::channel(1);
    let task = runtime
        .spawn_window_processor_snapshot_task(&shutdown_tx, state.clone(), snapshot_request_tx)
        .expect("persisted runtime should spawn a snapshot task");
    let live_state =
        WindowProcessorState::new(&window_aggregate("SET count = COUNT(input.latency)"));
    let snapshot_state = state.clone();
    let snapshot_owner = tokio::spawn(async move {
        let response = timeout(Duration::from_secs(1), snapshot_requests.recv())
            .await
            .expect("snapshot task should request live state")
            .expect("snapshot request channel should remain open");
        let result = snapshot_state
            .replace_state(&live_state)
            .map(|_| ())
            .map_err(|error| error.to_string());
        let _ = response.send(result);
    });

    state.mark_live_dirty();
    let expected_lsm = 1;

    wait_for_persisted_runtime_state_lsm(&runtime, &placement, expected_lsm).await;
    snapshot_owner
        .await
        .expect("snapshot owner should stop cleanly");
    assert_eq!(
        state.last_persisted_lsm.load(Ordering::SeqCst),
        expected_lsm
    );
    assert!(!state.dirty.load(Ordering::SeqCst));

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");
}

#[test]
fn runtime_state_store_purges_only_stale_schema_fingerprints() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let base = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: None,
    };
    let current = RuntimeStatePlacement {
        schema_fingerprint: [2; 32],
        ..base.clone()
    };
    store
        .persist_latest_snapshot(&base, 1, b"old")
        .expect("old snapshot should persist");
    store
        .persist_latest_snapshot(&current, 2, b"current")
        .expect("current snapshot should persist");

    store
        .purge_stale_schema_fingerprints(
            &base.domain,
            &HashMap::from_iter([(
                NodeRef {
                    kind: base.kind,
                    identifier: base.identifier.clone(),
                },
                current.schema_fingerprint,
            )]),
        )
        .expect("stale snapshots should purge");

    assert!(
        store
            .latest_snapshot(&base)
            .expect("old snapshot lookup should succeed")
            .is_none()
    );
    assert_eq!(
        store
            .latest_snapshot(&current)
            .expect("current snapshot lookup should succeed")
            .expect("current snapshot should remain")
            .payload,
        b"current".to_vec()
    );
}

#[test]
fn runtime_state_store_purges_only_the_requested_domain() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let stopped = RuntimeStatePlacement {
        domain: domain("stopped"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: None,
    };
    let running = RuntimeStatePlacement {
        domain: domain("running"),
        ..stopped.clone()
    };
    store
        .persist_latest_snapshot(&stopped, 1, b"stopped")
        .expect("stopped-domain snapshot should persist");
    store
        .persist_latest_snapshot(&running, 2, b"running")
        .expect("running-domain snapshot should persist");

    store
        .purge_domain(&stopped.domain)
        .expect("stopped-domain snapshots should purge");

    assert!(
        store
            .latest_snapshot(&stopped)
            .expect("stopped-domain snapshot lookup should succeed")
            .is_none()
    );
    assert_eq!(
        store
            .latest_snapshot(&running)
            .expect("running-domain snapshot lookup should succeed")
            .expect("running-domain snapshot should remain")
            .payload,
        b"running".to_vec()
    );
}

#[test]
fn runtime_state_store_purges_only_the_requested_entity() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let removed = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::MaterializedRelay,
        kind: ModelKind::Relay,
        identifier: named("events"),
        schema_fingerprint: [1; 32],
        branch_key: None,
    };
    let retained = RuntimeStatePlacement {
        identifier: named("audit"),
        ..removed.clone()
    };
    store
        .persist_latest_snapshot(&removed, 1, b"removed")
        .expect("removed snapshot should persist");
    store
        .persist_latest_snapshot(&retained, 2, b"retained")
        .expect("retained snapshot should persist");

    store
        .purge_entity(
            &removed.domain,
            removed.state,
            removed.kind,
            &removed.identifier,
        )
        .expect("entity snapshots should purge");

    assert!(
        store
            .latest_snapshot(&removed)
            .expect("removed snapshot lookup should succeed")
            .is_none()
    );
    assert_eq!(
        store
            .latest_snapshot(&retained)
            .expect("retained snapshot lookup should succeed")
            .expect("unrelated entity snapshot should remain")
            .payload,
        b"retained".to_vec()
    );
}

#[test]
fn kafka_offset_state_roundtrips_partition_schedule_through_fjall() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::KafkaOffset,
        kind: ModelKind::Ingestor,
        identifier: named("kafka_notifications"),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    let state = Arc::new(
        ReplicatedKafkaOffsetState::new(placement.clone(), None)
            .expect("kafka state should initialize"),
    );
    let mut assignment =
        ReplicatedKafkaOffsetState::bind(&state, StateReplicationRoles::owned_by(None), None);
    let originator = assignment
        .originator
        .take()
        .expect("local Kafka state should grant authoritative access");
    let (offset_lsm, offset_payload) = originator
        .replace_offsets(HashMap::from_iter([
            (
                KafkaTopicPartition {
                    topic: "notifications".to_string(),
                    partition: 0,
                },
                12,
            ),
            (
                KafkaTopicPartition {
                    topic: "notifications".to_string(),
                    partition: 1,
                },
                18,
            ),
        ]))
        .expect("offsets should update");
    store
        .persist_latest_snapshot(&placement, offset_lsm, &offset_payload)
        .expect("offset snapshot should persist");
    let (schedule_lsm, schedule_payload) = originator
        .update_partition_schedule("notifications", nonzero!(2u64), vec![0, 1])
        .expect("schedule should update")
        .expect("schedule snapshot should be produced");
    store
        .persist_latest_snapshot(&placement, schedule_lsm, &schedule_payload)
        .expect("schedule snapshot should persist");

    let restored = Arc::new(
        ReplicatedKafkaOffsetState::new(
            placement.clone(),
            store
                .latest_snapshot(&placement)
                .expect("snapshot should load"),
        )
        .expect("restored kafka state should initialize"),
    );
    let read = ReplicatedKafkaOffsetState::read(&restored);
    assert_eq!(read.next_offset("notifications", 0), Some(12));
    assert_eq!(read.next_offset("notifications", 1), Some(18));
    assert_eq!(
        read.describe_topic("notifications"),
        Some(KafkaDomainOffsetDescribe {
            topic: "notifications".to_string(),
            instances: 2,
            observed_partitions: vec![0, 1],
            rebalance_epoch: 0,
            instance_assignments: vec![vec![0], vec![1]],
        })
    );
}

#[test]
fn branch_aggregated_state_snapshot_roundtrips_metrics() {
    let metrics = RuntimeMetrics::default();
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: named("redis_notifications"),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    let relay = named("notifications");
    let state = ReplicatedBranchAggregatedState::new(
        placement.clone(),
        Some(ClusterNodeName::parse("node-1").expect("valid name")),
        ClusterNodeName::parse("node-1").expect("valid name"),
        Vec::new(),
        0,
        &metrics,
        None,
    )
    .expect("branch-aggregated state should initialize");
    metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
        domain: &placement.domain,
        kind: placement.kind,
        node: &placement.identifier,
        relay: &relay,
        physical_node_id: Some(&ClusterNodeName::parse("node-1").expect("valid name")),
        messages: 2,
        bytes: 64,
        domain_timestamp: None,
    });
    let lsm = state.mark_metrics_updated();
    let snapshot = state
        .latest_snapshot(&metrics)
        .expect("metrics snapshot should encode");
    assert_eq!(snapshot.lsm, lsm);

    let restored_metrics = RuntimeMetrics::default();
    let _restored = ReplicatedBranchAggregatedState::new(
        placement.clone(),
        Some(ClusterNodeName::parse("node-1").expect("valid name")),
        ClusterNodeName::parse("node-1").expect("valid name"),
        Vec::new(),
        0,
        &restored_metrics,
        Some(snapshot),
    )
    .expect("branch-aggregated state should restore");

    let rendered = restored_metrics.describe_global_target(
        &placement.domain,
        "INGESTOR",
        &placement.identifier,
    );
    assert!(
        rendered.iter().any(
            |line| line.contains("messages_total sent relay=notifications")
                && line.contains("total=2")
        ),
        "expected restored metrics total in {rendered:?}"
    );
}

#[tokio::test]
async fn state_sync_request_returns_latest_snapshot_only_when_lsm_advances() {
    let runtime = Runtime::default();
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_deduplicator_state(placement.clone())
        .expect("deduplicator state should initialize");
    let initial = runtime
        .handle_state_sync_request(&placement, None)
        .await
        .expect("initial state sync request should succeed")
        .expect("an explicit empty checkpoint should be returned");
    assert_eq!(initial.lsm, 0);
    let unchanged_initial = runtime
        .handle_state_sync_request(&placement, Some(0))
        .await
        .expect("state sync request at the initial LSM should succeed");
    assert!(unchanged_initial.is_none());
    assert!(state.reserve_new_key(
        DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]),
        Timestamp::from_unix_nanos(1),
        Duration::from_secs(600),
    ));
    let lsm = state.current_lsm.current();

    let first = runtime
        .handle_state_sync_request(&placement, Some(0))
        .await
        .expect("state sync request should succeed")
        .expect("snapshot should be returned");
    assert_eq!(first.lsm, lsm);

    let none = runtime
        .handle_state_sync_request(&placement, Some(lsm))
        .await
        .expect("state sync request should succeed");
    assert!(none.is_none());
}

#[test]
fn deduplicator_key_reservation_reports_new_and_duplicate_keys() {
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = ReplicatedDeduplicatorState::new(placement, None)
        .expect("deduplicator state should initialize");
    let seen_at = Timestamp::from_unix_nanos(1);
    let max_time = Duration::from_secs(600);

    let key = DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]);
    assert!(state.reserve_new_key(key.clone(), seen_at, max_time));
    assert!(!state.reserve_new_key(key, seen_at, max_time));
    assert_eq!(state.current_lsm.current(), 1);
}

#[test]
fn runtime_state_placement_storage_key_includes_branch_key() {
    let tenant_beta = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: string_branch_key("tenant", "beta"),
    };
    let tenant = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: string_branch_key("tenant", "acme"),
    };

    assert_ne!(tenant_beta.as_storage_key(), tenant.as_storage_key());
    let branch_aggregated = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [0; 32],
        branch_key: None,
    };
    assert_ne!(
        tenant_beta.as_storage_key(),
        branch_aggregated.as_storage_key()
    );
    let deduplicator_global = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        schema_fingerprint: [1; 32],
        branch_key: None,
    };
    assert_ne!(
        deduplicator_global.as_storage_key(),
        branch_aggregated.as_storage_key()
    );
}

/// Reinstalling a schedule must never leave a scheduled node without its fingerprint, even
/// for an instant. `state_placement` keys every runtime state by that fingerprint, so a reader
/// that resolves a placement while the map is being rebuilt would address a different state
/// and find it empty. Relocation rebuilds the fingerprints while the relay's own state task is
/// still running, which is exactly when that read happens.
#[test]
fn reinstalling_schema_fingerprints_never_exposes_a_node_without_one() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let identifier = named::<ModelName>("moving_state");
    let schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            ScheduledNode::new(nervix_models::Model::Relay(nervix_models::CreateRelay {
                name: nervix_models::RelayName::from(&identifier.clone()),
                schema: nervix_models::SchemaName::from(&identifier.clone()),
                buffer: nonzero!(4usize),
                branching: nervix_models::RelayBranching::unbranched(),
                materialized_state: Some(nervix_models::MaterializedRelayState::LastByTimestamp),
            }))
            .with_schema_fingerprint([1; 32])
            .placed_on(
                Some(ClusterNodeName::parse("node-1").expect("valid name")),
                vec![ClusterNodeName::parse("node-1").expect("valid name")],
            ),
        ],
        Vec::new(),
    );

    let resolve = || {
        runtime.state_placement(
            &domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            &identifier,
            None,
        )
    };
    runtime.install_state_schema_fingerprints(&schedule);
    let installed = resolve();

    let reads_stopped = AtomicBool::new(false);
    let missed = AtomicBool::new(false);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            while !reads_stopped.load(Ordering::Relaxed) {
                if resolve() != installed {
                    missed.store(true, Ordering::Relaxed);
                }
            }
        });
        for _ in 0..2_000 {
            runtime.install_state_schema_fingerprints(&schedule);
        }
        reads_stopped.store(true, Ordering::Release);
    });

    assert!(
        !missed.load(Ordering::Relaxed),
        "a placement resolved during a reinstall addressed a different runtime state"
    );
}

#[test]
fn schema_fingerprints_reuse_unaffected_state_and_isolate_changed_state() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let identifier = named::<ModelName>("dedup_orders");
    let schedule = |fingerprint| {
        DomainSchedule::new(
            domain.clone(),
            vec![
                ScheduledNode::new(nervix_models::Model::Deduplicator(
                    nervix_models::CreateDeduplicator {
                        name: nervix_models::DeduplicatorName::from(&identifier.clone()),
                        from: nervix_models::ProcessorInputs::new(Vec::new(), Vec::new()),
                        output_routes: nervix_models::ProcessorOutputs::new(Vec::new()),
                        branched_by: nervix_models::BranchSelection::unbranched(),
                        deduplicate_on: Vec::new(),
                        max_time: "1m".to_string(),
                        mode: nervix_models::AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    },
                ))
                .with_schema_fingerprint(fingerprint)
                .placed_on(
                    Some(ClusterNodeName::parse("node-1").expect("valid name")),
                    vec![ClusterNodeName::parse("node-1").expect("valid name")],
                ),
            ],
            Vec::new(),
        )
    };

    runtime.install_state_schema_fingerprints(&schedule([1; 32]));
    let original_placement = runtime.state_placement(
        &domain,
        RuntimeStateKind::Deduplicator,
        ModelKind::Deduplicator,
        &identifier,
        None,
    );
    let original = runtime
        .replicated_deduplicator_state(original_placement.clone())
        .expect("state should initialize");

    runtime.install_state_schema_fingerprints(&schedule([1; 32]));
    let unchanged = runtime
        .replicated_deduplicator_state(runtime.state_placement(
            &domain,
            RuntimeStateKind::Deduplicator,
            ModelKind::Deduplicator,
            &identifier,
            None,
        ))
        .expect("unchanged state should initialize");
    assert!(Arc::ptr_eq(&original, &unchanged));

    runtime.install_state_schema_fingerprints(&schedule([2; 32]));
    let changed = runtime
        .replicated_deduplicator_state(runtime.state_placement(
            &domain,
            RuntimeStateKind::Deduplicator,
            ModelKind::Deduplicator,
            &identifier,
            None,
        ))
        .expect("changed state should initialize");
    assert!(!Arc::ptr_eq(&original, &changed));
    runtime
        .purge_stale_runtime_state(&domain)
        .expect("stale state should purge");
    assert!(
        !runtime
            .inner
            .replicated_deduplicator_states
            .contains_key(&original_placement)
    );
}
