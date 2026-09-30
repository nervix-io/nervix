//! Layer: test harness.
//! Owns: the routing of replica acknowledgements and owner announcements to the replication of the
//! state they name, the handover of an announcement, the Kafka replica quorum wait through the
//! runtime, and the branch lifecycle a replica holds.
//! May depend on: runtime internals and test-only storage fixtures.
//! Must not know: production control-plane orchestration or edge protocols.

use std::{sync::Arc as StdArc, time::Duration};

use fjall::Database;
use futures_util::FutureExt as _;
use nervix_models::{ClusterNodeName, ModelKind, ModelName, SchemaFingerprint, Timestamp};
use nervix_primitives::time::Instant;
use tempfile::tempdir;

use super::*;

const REVISION: u64 = 3;

fn placement(
    state: RuntimeState,
    kind: ModelKind,
    identifier: &str,
    branch_key: Option<BranchKey>,
) -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: domain("default"),
        state,
        kind,
        identifier: named(identifier),
        branch_key,
    }
}

fn schema() -> SchemaFingerprint {
    SchemaFingerprint::from_digest([7; 32])
}

fn held_by(replication: &CheckpointReplication, replica: &ClusterNodeName) -> Option<u64> {
    replication.with_progress(|progress| progress.held(replica))
}

fn acknowledge(runtime: &Runtime, replica: &ClusterNodeName, placement: &RuntimeStatePlacement) {
    runtime.handle_state_replication_ack(
        replica,
        StateSyncAck {
            placement: placement.clone(),
            lsm: REVISION,
        },
    );
}

/// A branch lifecycle checkpoint at `lsm` naming `branches`.
fn lifecycle_snapshot(lsm: u64, branches: &[Option<BranchKey>]) -> PersistedRuntimeStateEntry {
    let mut entries = Vec::new();
    for key in branches {
        entries.push(BranchInstanceSnapshotEntry {
            key: key.clone(),
            last_ingestion: Timestamp::from_unix_nanos(1),
            incarnation: 1,
        });
    }
    PersistedRuntimeStateEntry {
        lsm,
        payload: encode_branch_lru_snapshot(&entries)
            .expect("a current branch lifecycle checkpoint encodes"),
    }
}

#[test]
fn every_acknowledgement_reaches_the_replication_of_the_state_it_names() {
    let runtime = Runtime::default();
    let replica = named::<ClusterNodeName>("node-2");
    let branch = string_branch_key("tenant", "acme");

    let kafka = placement(
        RuntimeState::KafkaOffset,
        ModelKind::Ingestor,
        "orders",
        None,
    );
    let kafka_state = runtime
        .replicated_kafka_offset_state(kafka.clone(), None, Vec::new(), 0, None)
        .expect("Kafka offset state initializes");
    acknowledge(&runtime, &replica, &kafka);
    assert_eq!(
        held_by(kafka_state.persistence.read().replication(), &replica),
        Some(REVISION)
    );

    let deduplicator = placement(
        RuntimeState::Deduplicator { schema: schema() },
        ModelKind::Deduplicator,
        "dedup_orders",
        branch.clone(),
    );
    let deduplicator_state = runtime
        .replicated_deduplicator_state(deduplicator.clone())
        .expect("deduplicator state initializes");
    acknowledge(&runtime, &replica, &deduplicator);
    assert_eq!(
        held_by(deduplicator_state.replication(), &replica),
        Some(REVISION)
    );

    let window = placement(
        RuntimeState::WindowProcessor { schema: schema() },
        ModelKind::WindowProcessor,
        "window_orders",
        branch.clone(),
    );
    let window_state = runtime
        .replicated_window_processor_state(window.clone())
        .expect("window state initializes");
    acknowledge(&runtime, &replica, &window);
    assert_eq!(
        held_by(window_state.replication(), &replica),
        Some(REVISION)
    );

    let wasm = placement(
        RuntimeState::WasmProcessor {
            schema: schema(),
            generation: nervix_models::WasmStateGeneration::FIRST,
        },
        ModelKind::WasmProcessor,
        "guest",
        branch.clone(),
    );
    let wasm_state = runtime
        .replicated_wasm_processor_state(wasm.clone())
        .expect("WASM state initializes");
    acknowledge(&runtime, &replica, &wasm);
    assert_eq!(held_by(wasm_state.replication(), &replica), Some(REVISION));

    let materialized = placement(
        RuntimeState::MaterializedRelay { schema: schema() },
        ModelKind::Relay,
        "latest_orders",
        None,
    );
    let materialized_state = runtime
        .replicated_materialized_stream_state(
            materialized.clone(),
            StdArc::new(arrow_schema::Schema::empty()),
            None,
            Vec::new(),
            None,
        )
        .expect("materialized state initializes");
    acknowledge(&runtime, &replica, &materialized);
    assert_eq!(
        held_by(
            materialized_state.persistence.read().replication(),
            &replica
        ),
        Some(REVISION)
    );

    let aggregated = placement(
        RuntimeState::BranchAggregated,
        ModelKind::Deduplicator,
        "dedup_orders",
        None,
    );
    let aggregated_state = runtime
        .replicated_branch_aggregated_state(aggregated.clone(), None, named("node-1"))
        .expect("branch-aggregated state initializes");
    acknowledge(&runtime, &replica, &aggregated);
    assert_eq!(
        held_by(aggregated_state.replication(), &replica),
        Some(REVISION)
    );

    let lifecycle = placement(
        RuntimeState::BranchLru { schema: schema() },
        ModelKind::Deduplicator,
        "dedup_orders",
        None,
    );
    let lifecycle_state = runtime.replicated_branch_lifecycle(&lifecycle);
    acknowledge(&runtime, &replica, &lifecycle);
    assert_eq!(
        held_by(lifecycle_state.replication(), &replica),
        Some(REVISION)
    );
}

#[test]
fn an_acknowledgement_or_announcement_of_a_placement_without_state_creates_nothing() {
    let runtime = Runtime::default();
    let replica = named::<ClusterNodeName>("node-2");
    let placements = [
        placement(
            RuntimeState::Correlator { schema: schema() },
            ModelKind::Correlator,
            "joined",
            None,
        ),
        placement(
            RuntimeState::Deduplicator { schema: schema() },
            ModelKind::Deduplicator,
            "dedup_orders",
            string_branch_key("tenant", "unknown"),
        ),
        placement(
            RuntimeState::BranchLru { schema: schema() },
            ModelKind::Deduplicator,
            "dedup_orders",
            None,
        ),
    ];
    for placement in &placements {
        acknowledge(&runtime, &replica, placement);
        runtime.with_placement_replication(placement, |_| {
            panic!("a placement this node holds no state for has no replication");
        });
    }
    assert!(runtime.inner.replicated_deduplicator_states.is_empty());
    assert!(runtime.inner.replicated_branch_lifecycles.is_empty());
}

#[test]
fn an_owners_announcement_wakes_the_replica_task_of_the_state_it_names() {
    let runtime = Runtime::default();
    let kafka = placement(
        RuntimeState::KafkaOffset,
        ModelKind::Ingestor,
        "orders",
        None,
    );
    let kafka_state = runtime
        .replicated_kafka_offset_state(kafka.clone(), None, Vec::new(), 0, None)
        .expect("Kafka offset state initializes");
    let replication = kafka_state.persistence.read().replication();
    assert!(
        replication.next_announcement().now_or_never().is_none(),
        "nothing was announced yet"
    );
    runtime.with_placement_replication(&kafka, CheckpointReplication::announced);
    assert!(
        replication.next_announcement().now_or_never().is_some(),
        "an announcement that arrived while nothing waited wakes the next wait"
    );
}

#[nervix_primitives::test]
async fn an_announcer_without_replicas_hands_its_announcement_back() {
    let runtime = Runtime::default();
    let kafka = placement(
        RuntimeState::KafkaOffset,
        ModelKind::Ingestor,
        "orders",
        None,
    );
    let replication = CheckpointReplication::new();
    runtime.announce_checkpoint(&kafka, &replication, 1);
    // A runtime that has not joined a cluster has no replicas, so its announcer ends at its first
    // step and hands the announcement back.
    nervix_primitives::task::yield_now().await;
    runtime.inner.state_replication_tasks.close();
    runtime.inner.state_replication_tasks.wait().await;
    assert!(
        replication.offer(2).is_some(),
        "the next offer starts another announcer"
    );
}

#[nervix_primitives::test]
async fn an_announcer_of_a_stopping_runtime_ends() {
    let runtime = Runtime::default();
    let kafka = placement(
        RuntimeState::KafkaOffset,
        ModelKind::Ingestor,
        "orders",
        None,
    );
    let replication = CheckpointReplication::new();
    runtime.inner.state_replication_tasks.close();
    runtime.announce_checkpoint(&kafka, &replication, 1);
    runtime.inner.state_replication_tasks.wait().await;
    assert!(
        replication.offer(2).is_some(),
        "an announcer that ended with its runtime hands its announcement back"
    );
}

/// The quorum wait registers before it reads, so the acknowledgement that satisfies it wakes it at
/// once: on a paused clock, no time passes between the commit and its completion. A lost wake-up
/// would leave the commit to its deadline, which the paused clock reaches as soon as nothing else
/// can run.
#[nervix_primitives::test(start_paused = true)]
async fn a_committed_offset_completes_when_its_replica_acknowledges_it() {
    let runtime = Runtime::default();
    let owner = named::<ClusterNodeName>("node-1");
    let replica = named::<ClusterNodeName>("node-2");
    let kafka = placement(
        RuntimeState::KafkaOffset,
        ModelKind::Ingestor,
        "orders",
        None,
    );
    let mut assignment = runtime
        .replicated_kafka_offset_state(
            kafka.clone(),
            Some(owner.clone()),
            vec![replica.clone()],
            1,
            Some(&owner),
        )
        .expect("Kafka offset state initializes");
    let originator = assignment
        .originator
        .take()
        .expect("the primary originates the offsets");
    let started = Instant::now();
    let committing = nervix_primitives::task::spawn({
        let runtime = runtime.clone();
        async move {
            runtime
                .commit_domain_kafka_offset(
                    &originator,
                    KafkaOffsetPosition {
                        topic: "orders".to_string(),
                        partition: 0,
                        offset: 43,
                    },
                )
                .await
        }
    });
    nervix_primitives::task::yield_now().await;
    runtime.handle_state_replication_ack(
        &replica,
        StateSyncAck {
            placement: kafka.clone(),
            lsm: 0,
        },
    );
    nervix_primitives::task::yield_now().await;
    assert!(
        !committing.is_finished(),
        "an acknowledgement of an older revision does not complete the commit"
    );
    runtime.handle_state_replication_ack(
        &replica,
        StateSyncAck {
            placement: kafka,
            lsm: 1,
        },
    );
    committing
        .await
        .expect("the commit task does not panic")
        .expect("the replica's acknowledgement completes the commit");
    assert_eq!(
        Instant::now(),
        started,
        "the acknowledgement, not the deadline, completed the commit"
    );
}

#[test]
fn an_older_acknowledgement_never_lowers_what_a_replica_holds() {
    let runtime = Runtime::default();
    let owner = named::<ClusterNodeName>("node-1");
    let replica = named::<ClusterNodeName>("node-2");
    let kafka = placement(
        RuntimeState::KafkaOffset,
        ModelKind::Ingestor,
        "orders",
        None,
    );
    let assignment = runtime
        .replicated_kafka_offset_state(
            kafka.clone(),
            Some(owner.clone()),
            vec![replica.clone()],
            1,
            Some(&owner),
        )
        .expect("Kafka offset state initializes");
    for lsm in [5, 2] {
        runtime.handle_state_replication_ack(
            &replica,
            StateSyncAck {
                placement: kafka.clone(),
                lsm,
            },
        );
    }
    let offsets = assignment.persistence.read();
    assert!(offsets.replica_quorum_holds(5));
    assert!(!offsets.replica_quorum_holds(6));
}

#[test]
fn holding_a_replica_copy_keeps_the_newer_revision_and_moves_its_payload() {
    let runtime = Runtime::default();
    let deduplicator = placement(
        RuntimeState::Deduplicator { schema: schema() },
        ModelKind::Deduplicator,
        "dedup_orders",
        string_branch_key("tenant", "acme"),
    );
    let payload = vec![7_u8; 64];
    let payload_address = payload.as_ptr();
    runtime.hold_passive_state_replica_snapshot(
        &deduplicator,
        PersistedRuntimeStateEntry { lsm: 2, payload },
    );
    runtime.hold_passive_state_replica_snapshot(
        &deduplicator,
        PersistedRuntimeStateEntry {
            lsm: 1,
            payload: vec![1],
        },
    );
    let held = runtime
        .inner
        .passive_runtime_state_snapshots
        .get(&deduplicator)
        .expect("the replica holds a copy");
    assert_eq!(held.lsm, 2, "an older copy never replaces a newer one");
    assert_eq!(
        held.payload.as_ptr(),
        payload_address,
        "holding a copy moves its payload in instead of copying it"
    );
}

#[test]
fn a_replica_decodes_each_branch_lifecycle_once_and_prunes_the_branches_it_drops() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let deduplicator = named::<ModelName>("dedup_orders");
    publish_state_identity(
        &runtime,
        &domain,
        ModelKind::Deduplicator,
        deduplicator.clone(),
    );
    let branch_lru = runtime
        .state_placement(
            &domain,
            RuntimeStateKind::BranchLru,
            ModelKind::Deduplicator,
            deduplicator.clone(),
            None,
        )
        .expect("the published identity places the branch lifecycle");
    let acme = string_branch_key("tenant", "acme");
    let beta = string_branch_key("tenant", "beta");
    let branch_placement = |branch: &Option<BranchKey>| {
        runtime
            .state_placement(
                &domain,
                RuntimeStateKind::Deduplicator,
                ModelKind::Deduplicator,
                deduplicator.clone(),
                branch.clone(),
            )
            .expect("the published identity places the branch state")
    };
    let acme_placement = branch_placement(&acme);
    let beta_placement = branch_placement(&beta);
    for placement in [&acme_placement, &beta_placement] {
        runtime.hold_passive_state_replica_snapshot(
            placement,
            PersistedRuntimeStateEntry {
                lsm: 1,
                payload: vec![1],
            },
        );
    }

    runtime
        .install_replica_branch_lru_snapshot(
            &branch_lru,
            lifecycle_snapshot(1, &[acme.clone(), beta.clone()]),
        )
        .expect("a lifecycle naming both branches installs");
    assert!(
        runtime
            .replica_branch_is_current(&acme_placement)
            .expect("the lifecycle decodes")
    );
    assert!(
        runtime
            .replica_branch_is_current(&beta_placement)
            .expect("the lifecycle decodes")
    );
    let held = runtime
        .held_branch_lifecycle(&branch_lru)
        .expect("the replica holds the lifecycle it installed");
    let first = held.branches().expect("the lifecycle decodes");
    let again = held.branches().expect("the lifecycle decodes");
    assert!(
        std::ptr::eq(first, again),
        "a lifecycle checkpoint decodes its branches once"
    );

    runtime
        .install_replica_branch_lru_snapshot(
            &branch_lru,
            lifecycle_snapshot(2, std::slice::from_ref(&acme)),
        )
        .expect("a newer lifecycle without beta installs");
    assert!(
        !runtime
            .replica_branch_is_current(&beta_placement)
            .expect("the lifecycle decodes")
    );
    assert!(
        runtime
            .inner
            .passive_runtime_state_snapshots
            .contains_key(&acme_placement)
    );
    assert!(
        !runtime
            .inner
            .passive_runtime_state_snapshots
            .contains_key(&beta_placement),
        "the checkpoint of a branch the lifecycle dropped is dropped with it"
    );

    runtime
        .install_replica_branch_lru_snapshot(&branch_lru, lifecycle_snapshot(1, &[acme, beta]))
        .expect("an older lifecycle arriving late decodes");
    let held = runtime
        .held_branch_lifecycle(&branch_lru)
        .expect("the replica holds a lifecycle");
    assert_eq!(
        held.lsm(),
        2,
        "a late older lifecycle never replaces a newer one"
    );
    assert!(
        !runtime
            .replica_branch_is_current(&beta_placement)
            .expect("the lifecycle decodes")
    );
}

#[nervix_primitives::test]
async fn a_replica_reads_a_stored_branch_lifecycle_once_and_holds_it() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("runtime should open persisted state");
    let domain = domain("default");
    let deduplicator = named::<ModelName>("dedup_orders");
    publish_state_identity(
        &runtime,
        &domain,
        ModelKind::Deduplicator,
        deduplicator.clone(),
    );
    let branch_lru = runtime
        .state_placement(
            &domain,
            RuntimeStateKind::BranchLru,
            ModelKind::Deduplicator,
            deduplicator.clone(),
            None,
        )
        .expect("the published identity places the branch lifecycle");
    let acme = string_branch_key("tenant", "acme");
    let stored = lifecycle_snapshot(4, std::slice::from_ref(&acme));
    runtime
        .inner
        .state_store
        .as_ref()
        .expect("the runtime has a state store")
        .persist_latest_snapshot(&branch_lru, stored.lsm, &stored.payload)
        .expect("the lifecycle persists");
    assert!(runtime.held_branch_lifecycle(&branch_lru).is_none());

    let acme_placement = runtime
        .state_placement(
            &domain,
            RuntimeStateKind::Deduplicator,
            ModelKind::Deduplicator,
            deduplicator,
            acme,
        )
        .expect("the published identity places the branch state");
    assert!(
        runtime
            .replica_branch_is_current(&acme_placement)
            .expect("the stored lifecycle decodes")
    );
    assert_eq!(
        runtime
            .held_branch_lifecycle(&branch_lru)
            .map(|held| held.lsm()),
        Some(4),
        "the lifecycle read from storage is held, so its branches decode once"
    );
    assert_eq!(
        runtime
            .passive_state_replica_lsm(&branch_lru)
            .expect("the held lifecycle has a revision"),
        Some(4)
    );
}
