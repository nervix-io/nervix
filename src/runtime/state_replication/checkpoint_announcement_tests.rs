//! Layer: test harness.
//! Owns: the routing of replica acknowledgements and owner announcements to the replication of the
//! state they name, the handover of an announcement, the Kafka replica quorum wait through the
//! runtime, and the branch lifecycle a replica holds.
//! May depend on: runtime internals and test-only storage fixtures.
//! Must not know: production control-plane orchestration or edge protocols.

use std::time::Duration;

use fjall::Database;
use futures_util::FutureExt as _;
use nervix_models::{ClusterNodeName, ModelKind, ModelName, SchemaFingerprint};
use nervix_primitives::{sync::StdArc, time::Instant};
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

fn publish_current_assignment(runtime: &Runtime, placement: &RuntimeStatePlacement) {
    runtime.publish_state_assignment(
        placement.entity(),
        ScheduledStateAssignment {
            identity: ScheduledStateIdentity {
                schema_fingerprint: schema(),
                wasm_state_generations: match placement.state {
                    RuntimeState::WasmProcessor { .. } => {
                        Some(nervix_models::WasmStateGenerations::first())
                    }
                    _ => None,
                },
            },
            checkpoint_owners: None,
        },
    );
}

#[nervix_primitives::test]
async fn branch_lifecycle_confirmation_keeps_its_replica_minimum_and_generation() {
    let runtime = Runtime::new();
    let local = named::<ClusterNodeName>("node-1");
    let first_replica = named::<ClusterNodeName>("node-2");
    let second_replica = named::<ClusterNodeName>("node-3");
    attach_loopback_cluster(&runtime, &local).await;
    let placement = placement(
        RuntimeState::BranchLru { schema: schema() },
        ModelKind::Deduplicator,
        "dedup_orders",
        None,
    );
    let publish = |fingerprint, replicas| {
        runtime.publish_state_assignment(
            placement.entity(),
            ScheduledStateAssignment {
                identity: ScheduledStateIdentity {
                    schema_fingerprint: fingerprint,
                    wasm_state_generations: None,
                },
                checkpoint_owners: Some(CheckpointOwners {
                    primary: Some(local.clone()),
                    executors: BTreeSet::from([local.clone()]),
                    replicas,
                }),
            },
        );
    };
    publish(
        schema(),
        BTreeSet::from([first_replica.clone(), second_replica.clone()]),
    );
    let lifecycle = runtime.replicated_branch_lifecycle(&placement);
    let deadline = || Instant::now() + Duration::from_secs(10);
    let expired = runtime
        .confirm_branch_lru_checkpoint(&placement, REVISION, Instant::now())
        .await
        .expect_err("an unconfirmed lifecycle reaches its physical deadline");
    assert!(matches!(
        expired.current_context(),
        StateReplicationError::ReplicaConfirmation { awaiting, .. }
            if awaiting.0 == BTreeSet::from([first_replica.clone(), second_replica.clone()])
    ));

    let mut confirmation =
        Box::pin(runtime.confirm_branch_lru_checkpoint(&placement, REVISION, deadline()));
    assert!(confirmation.as_mut().now_or_never().is_none());
    acknowledge(&runtime, &first_replica, &placement);
    acknowledge(&runtime, &second_replica, &placement);
    confirmation
        .await
        .expect("the selected lifecycle confirms after both replicas acknowledge it");

    let mut confirmation =
        Box::pin(runtime.confirm_branch_lru_checkpoint(&placement, REVISION + 1, deadline()));
    assert!(confirmation.as_mut().now_or_never().is_none());
    publish(schema(), BTreeSet::from([first_replica.clone()]));
    lifecycle.replication().record(&first_replica, REVISION + 1);
    let shrunk = confirmation
        .await
        .expect_err("reassignment cannot reduce a pending checkpoint's replica minimum");
    assert!(matches!(
        shrunk.current_context(),
        StateReplicationError::ReplicaPlanShrunk {
            required: 2,
            assigned: 1,
            ..
        }
    ));

    let mut confirmation =
        Box::pin(runtime.confirm_branch_lru_checkpoint(&placement, REVISION + 2, deadline()));
    assert!(confirmation.as_mut().now_or_never().is_none());
    publish(
        SchemaFingerprint::from_digest([8; 32]),
        BTreeSet::from([first_replica.clone()]),
    );
    lifecycle.replication().record(&first_replica, REVISION + 2);
    let replaced = confirmation
        .await
        .expect_err("replaced assignments fence a retained lifecycle's confirmation");
    assert!(matches!(
        replaced.current_context(),
        StateReplicationError::Superseded { .. }
    ));
}

#[test]
fn a_replaced_assignment_fences_acknowledgements_for_the_retained_state() {
    let runtime = Runtime::new();
    let replica = named::<ClusterNodeName>("node-2");
    let entity = DomainNodeRef::node_in(
        domain("default"),
        ModelKind::Deduplicator,
        named::<ModelName>("dedup_orders"),
    );
    runtime.publish_state_assignment(
        entity.clone(),
        ScheduledStateAssignment {
            identity: ScheduledStateIdentity {
                schema_fingerprint: schema(),
                wasm_state_generations: None,
            },
            checkpoint_owners: None,
        },
    );
    let placed = placement(
        RuntimeState::Deduplicator { schema: schema() },
        ModelKind::Deduplicator,
        "dedup_orders",
        string_branch_key("tenant", "acme"),
    );
    let state = runtime
        .replicated_deduplicator_state(placed.clone())
        .assured("a current deduplicator state initializes");
    runtime.publish_state_assignment(
        entity,
        ScheduledStateAssignment {
            identity: ScheduledStateIdentity {
                schema_fingerprint: SchemaFingerprint::from_digest([8; 32]),
                wasm_state_generations: None,
            },
            checkpoint_owners: None,
        },
    );

    acknowledge(&runtime, &replica, &placed);
    assert_eq!(held_by(state.replication(), &replica), None);
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
    publish_current_assignment(&runtime, &kafka);
    let kafka_state = runtime
        .replicated_kafka_offset_state(kafka.clone(), None, Vec::new(), 0, None)
        .expect("Kafka offset state initializes");
    acknowledge(&runtime, &replica, &kafka);
    assert_eq!(
        held_by(kafka_state.persistence.read().replication(), &replica),
        Some(REVISION)
    );
    publish_current_assignment(&runtime, &kafka);

    let deduplicator = placement(
        RuntimeState::Deduplicator { schema: schema() },
        ModelKind::Deduplicator,
        "dedup_orders",
        branch.clone(),
    );
    publish_current_assignment(&runtime, &deduplicator);
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
    publish_current_assignment(&runtime, &window);
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
    publish_current_assignment(&runtime, &wasm);
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
    publish_current_assignment(&runtime, &materialized);
    let materialized_state = runtime.replicated_materialized_stream_state(
        materialized.clone(),
        StdArc::new(arrow_schema::Schema::empty()),
        None,
        Vec::new(),
        None,
    );
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
    publish_current_assignment(&runtime, &aggregated);
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
    publish_current_assignment(&runtime, &lifecycle);
    let lifecycle_state = runtime.replicated_branch_lifecycle(&lifecycle);
    acknowledge(&runtime, &replica, &lifecycle);
    assert_eq!(
        held_by(lifecycle_state.replication(), &replica),
        Some(REVISION)
    );

    for placement in [
        &kafka,
        &deduplicator,
        &window,
        &wasm,
        &materialized,
        &aggregated,
        &lifecycle,
    ] {
        let route = runtime
            .inner
            .state_replication_routing
            .resolve(placement)
            .assured("every installed replicated state has a resolved frame route");
        let admitted = route
            .state()
            .assured("a frame can borrow the current state");
        runtime.inner.state_replication_routing.retire(placement);
        runtime.handle_state_replication_ack(
            &replica,
            StateSyncAck {
                placement: placement.clone(),
                lsm: REVISION + 1,
            },
        );
        runtime.with_placement_replication(placement, CheckpointReplication::announced);
        assert!(route.state().is_none());
        assert_eq!(held_by(admitted.replication(), &replica), Some(REVISION));
        assert!(
            admitted
                .replication()
                .next_announcement()
                .now_or_never()
                .is_none()
        );
    }
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
    publish_current_assignment(&runtime, &kafka);
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
    publish_current_assignment(&runtime, &kafka);
    let assignment = runtime
        .replicated_kafka_offset_state(kafka.clone(), None, Vec::new(), 0, None)
        .assured("an installed offset state owns the announcer");
    let replication = assignment.persistence.read().replication();
    runtime.announce_checkpoint(&kafka, replication, 1);
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
    publish_current_assignment(&runtime, &kafka);
    let assignment = runtime
        .replicated_kafka_offset_state(kafka.clone(), None, Vec::new(), 0, None)
        .assured("an installed offset state owns the announcer");
    let replication = assignment.persistence.read().replication();
    runtime.inner.state_replication_tasks.close();
    runtime.announce_checkpoint(&kafka, replication, 1);
    runtime.inner.state_replication_tasks.wait().await;
    assert!(
        replication.offer(2).is_some(),
        "an announcer that ended with its runtime hands its announcement back"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn runtime_shutdown_cancels_an_in_flight_checkpoint_announcement() {
    let runtime = Runtime::new();
    let local = named::<ClusterNodeName>("node-1");
    let replica = named::<ClusterNodeName>("node-2");
    attach_loopback_cluster(&runtime, &local).await;
    let kafka = placement(
        RuntimeState::KafkaOffset,
        ModelKind::Ingestor,
        "orders",
        None,
    );
    runtime.publish_state_assignment(
        kafka.entity(),
        ScheduledStateAssignment {
            identity: ScheduledStateIdentity {
                schema_fingerprint: schema(),
                wasm_state_generations: None,
            },
            checkpoint_owners: Some(CheckpointOwners {
                primary: Some(local.clone()),
                executors: BTreeSet::from([local.clone()]),
                replicas: BTreeSet::from([replica.clone()]),
            }),
        },
    );
    let state = runtime
        .replicated_kafka_offset_state(kafka.clone(), Some(local), vec![replica], 0, None)
        .assured("the current offset state installs");
    let replication = state.persistence.read().replication();
    let route = runtime
        .inner
        .state_replication_routing
        .resolve(&kafka)
        .assured("the installed state publishes its announcement route");
    let announcer = route
        .offer(replication, 1)
        .assured("the first checkpoint starts an announcer");
    let announcing_runtime = runtime.clone();
    let mut announcing = Box::pin(async move {
        announcing_runtime
            .offer_to_lagging_replicas(route, announcer)
            .await;
    });
    assert!(
        announcing.as_mut().now_or_never().is_none(),
        "dispatch to a replica outside the loopback cluster is pending"
    );
    let announcing = runtime.inner.state_replication_tasks.spawn(announcing);
    let started = Instant::now();
    let (_, joined) = futures_util::join!(runtime.shutdown(), announcing);
    joined.assured("the cancelled announcement exits without a task failure");
    assert_eq!(
        Instant::now(),
        started,
        "terminal teardown cancels the pending offer without waiting for its dispatch deadline"
    );
    assert!(
        replication.offer(2).is_some(),
        "cancelling the task returns its announcement to the retained state"
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
    publish_current_assignment(&runtime, &kafka);
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
    publish_current_assignment(&runtime, &kafka);
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
    let lifecycle = runtime.replicated_branch_lifecycle(
        &deduplicator
            .branch_lifecycle()
            .assured("a branch state has a lifecycle"),
    );
    lifecycle.hold_passive_checkpoint(
        &deduplicator,
        PersistedRuntimeStateEntry { lsm: 2, payload },
    );
    lifecycle.hold_passive_checkpoint(
        &deduplicator,
        PersistedRuntimeStateEntry {
            lsm: 1,
            payload: vec![1],
        },
    );
    let held = lifecycle
        .passive_checkpoint(&deduplicator)
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
    let lifecycle = runtime.replicated_branch_lifecycle(&branch_lru);
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
        lifecycle.hold_passive_checkpoint(
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
            &lifecycle,
            branch_lifecycle_snapshot(1, &[acme.clone(), beta.clone()]),
        )
        .expect("a lifecycle naming both branches installs");
    assert!(
        lifecycle
            .names(acme.as_ref())
            .expect("the lifecycle decodes")
    );
    assert!(
        lifecycle
            .names(beta.as_ref())
            .expect("the lifecycle decodes")
    );
    let held = lifecycle
        .latest()
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
            &lifecycle,
            branch_lifecycle_snapshot(2, std::slice::from_ref(&acme)),
        )
        .expect("a newer lifecycle without beta installs");
    assert!(
        !lifecycle
            .names(beta.as_ref())
            .expect("the lifecycle decodes")
    );
    assert!(lifecycle.passive_checkpoint(&acme_placement).is_some());
    assert!(
        !lifecycle.passive_checkpoint(&beta_placement).is_some(),
        "the checkpoint of a branch the lifecycle dropped is dropped with it"
    );

    runtime
        .install_replica_branch_lru_snapshot(
            &branch_lru,
            &lifecycle,
            branch_lifecycle_snapshot(1, &[acme, beta.clone()]),
        )
        .expect("an older lifecycle arriving late decodes");
    let held = lifecycle.latest().expect("the replica holds a lifecycle");
    assert_eq!(
        held.lsm(),
        2,
        "a late older lifecycle never replaces a newer one"
    );
    assert!(
        !lifecycle
            .names(beta.as_ref())
            .expect("the lifecycle decodes")
    );
}

#[test]
fn assignment_replacement_purges_superseded_passive_guest_generations() {
    use nervix_models::{WasmStateGeneration, WasmStateGenerations};

    let runtime = Runtime::new();
    let branch = string_branch_key("tenant", "acme");
    let guest = placement(
        RuntimeState::WasmProcessor {
            schema: schema(),
            generation: WasmStateGeneration::FIRST,
        },
        ModelKind::WasmProcessor,
        "guest",
        branch.clone(),
    );
    let publish = |generations| {
        runtime.publish_state_assignment(
            guest.entity(),
            ScheduledStateAssignment {
                identity: ScheduledStateIdentity {
                    schema_fingerprint: schema(),
                    wasm_state_generations: Some(generations),
                },
                checkpoint_owners: None,
            },
        );
    };
    let mut generations = WasmStateGenerations::first();
    publish(generations.clone());
    let lifecycle = runtime.replicated_branch_lifecycle(
        &guest
            .branch_lifecycle()
            .assured("a guest branch has an entity lifecycle"),
    );
    lifecycle.hold_passive_checkpoint(
        &guest,
        PersistedRuntimeStateEntry {
            lsm: 9,
            payload: vec![7],
        },
    );
    let retained = lifecycle
        .passive_checkpoint(&guest)
        .assured("the current generation is held");
    let mut current = guest.clone();
    current.state = RuntimeState::WasmProcessor {
        schema: schema(),
        generation: generations.begin_branch(
            branch
                .as_ref()
                .assured("the test names a branch")
                .fingerprint(),
        ),
    };
    publish(generations);
    lifecycle.hold_passive_checkpoint(
        &current,
        PersistedRuntimeStateEntry {
            lsm: 1,
            payload: vec![8],
        },
    );
    runtime
        .purge_stale_runtime_state(&guest.domain)
        .assured("assignment cleanup succeeds");
    assert!(lifecycle.passive_checkpoint(&guest).is_none());
    let held = lifecycle
        .passive_checkpoint(&current)
        .assured("the current generation remains held");
    assert_eq!(held.payload, vec![8]);
    assert_eq!(
        retained.payload,
        vec![7],
        "an already borrowed checkpoint can finish after cleanup"
    );
}

#[nervix_primitives::test]
async fn a_replica_task_restores_the_stored_branch_lifecycle_only_while_it_holds_none() {
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
            deduplicator,
            None,
        )
        .expect("the published identity places the branch lifecycle");
    let acme = string_branch_key("tenant", "acme");
    let stored = branch_lifecycle_snapshot(4, std::slice::from_ref(&acme));
    runtime
        .inner
        .state_store
        .as_ref()
        .expect("the runtime has a state store")
        .persist_latest_snapshot(&branch_lru, stored.lsm, &stored.payload)
        .expect("the lifecycle persists");
    let lifecycle = runtime.replicated_branch_lifecycle(&branch_lru);
    assert!(lifecycle.latest().is_none());

    runtime
        .restore_replica_branch_lifecycle(&branch_lru, &lifecycle)
        .expect("the stored lifecycle reads");

    assert_eq!(lifecycle.latest().map(|held| held.lsm()), Some(4));
    assert!(
        lifecycle
            .names(acme.as_ref())
            .expect("the lifecycle decodes")
    );

    lifecycle.install(StdArc::new(BranchLifecycleCheckpoint::new(
        branch_lifecycle_snapshot(6, &[]),
    )));
    runtime
        .restore_replica_branch_lifecycle(&branch_lru, &lifecycle)
        .expect("a held lifecycle needs no read");
    assert_eq!(
        lifecycle.latest().map(|held| held.lsm()),
        Some(6),
        "the stored lifecycle never replaces one the replica already holds"
    );
}
