//! Layer: test harness.
//! Owns: focused verification of runtime state persistence and replication.
//! May depend on: runtime internals and test-only storage fixtures.
//! Must not know: production control-plane orchestration or edge protocols.

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};

use ahash::HashMap;
use fjall::Database;
use nervix_interconnect::{
    ActivateOwnershipHandoffStateRequest, EntityGatePurpose, PrepareOwnershipHandoffStateRequest,
};
use nervix_models::{
    ClusterNodeIncarnation, ClusterNodeName, ClusterSchedule, CoordinationIdentity, CreateRelay,
    CreateSchema, DomainNodeRef, DomainSchedule, DomainStatus, MaterializedRelayState, ModelKind,
    ModelName, NodeRef, OwnershipStateRecoveryOutcome, OwnershipStateReset,
    OwnershipStateResetCause, OwnershipTransition, ParseAsType, RelayBranching, RelayName,
    ResolvedBranching, ScheduledNode, SchemaField, SchemaFingerprint, SchemaName, Timestamp,
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

struct EmptyRelayHandoffFixture {
    domain: DomainName,
    source: ClusterNodeName,
    destination: ClusterNodeName,
    entity: NodeRef,
    base_schedule: DomainSchedule,
    target_schedule: DomainSchedule,
    base_schedule_fingerprint: [u8; 32],
    target_schedule_fingerprint: [u8; 32],
    operation_id: String,
}

impl EmptyRelayHandoffFixture {
    fn new(operation_id: &str) -> Self {
        let domain = domain("default");
        let source = named::<ClusterNodeName>("node-1");
        let destination = named::<ClusterNodeName>("node-2");
        let identifier = named::<ModelName>("moving_relay");
        let entity = NodeRef::new(ModelKind::Relay, identifier.clone());
        let schema = SchemaName::from(&identifier);
        let schema_node = ScheduledNode::new(
            nervix_models::Model::Schema(CreateSchema {
                name: schema.clone(),
                fields: Vec::new(),
            }),
            SchemaFingerprint::from_digest([1; 32]),
        );
        let relay = ScheduledNode::new(
            nervix_models::Model::Relay(CreateRelay {
                name: RelayName::from(&identifier),
                schema,
                buffer: nonzero!(4usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            }),
            SchemaFingerprint::from_digest([1; 32]),
        )
        .with_resolved_branching(Some(ResolvedBranching::unbranched()))
        .placed_on(Some(source.clone()), vec![source.clone()]);
        let base_schedule = DomainSchedule::new(
            domain.clone(),
            vec![schema_node.clone(), relay.clone()],
            Vec::new(),
        );
        let mut moved = relay.placed_on(Some(destination.clone()), vec![destination.clone()]);
        moved.ownership_transition = Some(OwnershipTransition {
            id: operation_id.to_string(),
            source: source.clone(),
            destination: destination.clone(),
            state_recovery: OwnershipStateRecoveryOutcome::Complete,
            resets: Vec::new(),
        });
        let target_schedule =
            DomainSchedule::new(domain.clone(), vec![schema_node, moved], Vec::new());
        let base_schedule_fingerprint =
            Runtime::ownership_handoff_schedule_fingerprint(&base_schedule)
                .expect("base schedule should have a fingerprint");
        let target_schedule_fingerprint =
            Runtime::ownership_handoff_schedule_fingerprint(&target_schedule)
                .expect("target schedule should have a fingerprint");
        Self {
            domain,
            source,
            destination,
            entity,
            base_schedule,
            target_schedule,
            base_schedule_fingerprint,
            target_schedule_fingerprint,
            operation_id: operation_id.to_string(),
        }
    }

    async fn rebuild_destination(&self, runtime: &Runtime, schedule: DomainSchedule) {
        let mut domain_state = unpaced_domain_state(self.domain.as_str());
        domain_state.status = DomainStatus::Stopped;
        runtime.sync_domains(&BTreeMap::from([(self.domain.clone(), domain_state)]));
        runtime
            .rebuild_domain_from_schedule(&self.destination, &self.domain, Some(schedule), false)
            .await
            .expect("handoff test schedule should build on the destination");
    }

    async fn prepare(
        &self,
        runtime: &Runtime,
        coordination: CoordinationIdentity,
        source_incarnation: ClusterNodeIncarnation,
        destination_incarnation: ClusterNodeIncarnation,
    ) {
        runtime
            .engage_entity_gate_operation(
                &coordination,
                &self.domain,
                &[],
                std::slice::from_ref(&self.entity),
                EntityGatePurpose::OwnershipHandoff,
                EntityGateLease {
                    deadline: tokio::time::Instant::now() + Duration::from_secs(30),
                    reason: "prepare ownership handoff test fixture",
                },
            )
            .await
            .expect("ownership handoff gate should engage");
        runtime
            .prepare_ownership_handoff_state(PrepareOwnershipHandoffStateRequest {
                coordination,
                operation_id: self.operation_id.clone(),
                source: self.source.clone(),
                destination: self.destination.clone(),
                source_incarnation,
                destination_incarnation,
                domain: self.domain.clone(),
                entity: self.entity.clone(),
                base_schedule_fingerprint: self.base_schedule_fingerprint,
                target_schedule_fingerprint: self.target_schedule_fingerprint,
                checkpoints: Vec::new(),
            })
            .await
            .expect("ownership handoff preparation should persist");
    }
}

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

#[tokio::test]
async fn restarted_destination_reclaims_an_uncommitted_handoff_preparation() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let abandoned = EmptyRelayHandoffFixture::new("abandoned-operation");
    let source_incarnation = ClusterNodeIncarnation::new(31);
    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
            .expect("runtime should open persisted state");
        let destination_incarnation =
            attach_loopback_cluster(&runtime, &abandoned.destination).await;
        abandoned
            .rebuild_destination(&runtime, abandoned.base_schedule.clone())
            .await;
        abandoned
            .prepare(
                &runtime,
                CoordinationIdentity::new(named("coordinator-a"), 11, 1),
                source_incarnation,
                destination_incarnation,
            )
            .await;
    }

    let db = Database::builder(dir.path())
        .open()
        .expect("database should reopen after destination restart");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("restarted runtime should restore persisted state");
    let replacement = EmptyRelayHandoffFixture::new("replacement-operation");
    let destination_incarnation = attach_loopback_cluster(&runtime, &replacement.destination).await;
    replacement
        .rebuild_destination(&runtime, replacement.base_schedule.clone())
        .await;
    let coordination = CoordinationIdentity::new(named("coordinator-b"), 12, 1);
    replacement
        .prepare(
            &runtime,
            coordination.clone(),
            source_incarnation,
            destination_incarnation,
        )
        .await;

    let prepared = runtime
        .inner
        .prepared_runtime_state_handoffs
        .get(&replacement.entity.in_domain(&replacement.domain))
        .expect("replacement preparation should be cached");
    assert_eq!(prepared.coordination, coordination);
    assert_eq!(prepared.operation_id, replacement.operation_id);
    let persisted = runtime
        .inner
        .state_store
        .as_ref()
        .expect("runtime should own a state store")
        .handoff_preparations()
        .expect("preparations should remain readable");
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].operation_id, replacement.operation_id);
}

#[tokio::test]
async fn surviving_authority_reconciles_a_dead_coordinators_preparation() {
    let fixture = EmptyRelayHandoffFixture::new("abandoned-operation");
    let runtime = Runtime::new();
    let source_incarnation = ClusterNodeIncarnation::new(31);
    let destination_incarnation = attach_loopback_cluster(&runtime, &fixture.destination).await;
    fixture
        .rebuild_destination(&runtime, fixture.base_schedule.clone())
        .await;
    let abandoned_coordination = CoordinationIdentity::new(named("coordinator-a"), 11, 1);
    fixture
        .prepare(
            &runtime,
            abandoned_coordination.clone(),
            source_incarnation,
            destination_incarnation,
        )
        .await;
    runtime
        .release_entity_gate_operation(&abandoned_coordination, &fixture.domain)
        .await
        .expect("the abandoned gate should release");
    assert!(
        runtime
            .inner
            .prepared_runtime_state_handoffs
            .contains_key(&fixture.entity.in_domain(&fixture.domain)),
        "gate release alone is not durable preparation cleanup"
    );

    let surviving_authority = CoordinationIdentity::new(named("coordinator-b"), 12, 1);
    let schedule = ClusterSchedule::from_iter([fixture.base_schedule.clone()]);
    let incarnations = BTreeMap::from([
        (fixture.source.clone(), source_incarnation),
        (fixture.destination.clone(), destination_incarnation),
    ]);
    assert_eq!(
        runtime
            .reconcile_prepared_ownership_handoffs(&surviving_authority, &schedule, &incarnations,)
            .expect("the surviving authority should reconcile the abandoned operation"),
        1
    );
    assert_eq!(
        runtime
            .reconcile_prepared_ownership_handoffs(&surviving_authority, &schedule, &incarnations,)
            .expect("duplicate reconciliation should be idempotent"),
        0
    );

    let replacement = EmptyRelayHandoffFixture::new("replacement-operation");
    let replacement_coordination = CoordinationIdentity::new(named("coordinator-b"), 12, 2);
    replacement
        .prepare(
            &runtime,
            replacement_coordination.clone(),
            source_incarnation,
            destination_incarnation,
        )
        .await;
    runtime
        .discard_prepared_ownership_handoff_state(
            &abandoned_coordination,
            &fixture.operation_id,
            &fixture.domain,
            &fixture.entity,
        )
        .expect("a reordered stale discard should remain idempotent");
    let prepared = runtime
        .inner
        .prepared_runtime_state_handoffs
        .get(&fixture.entity.in_domain(&fixture.domain))
        .expect("the replacement preparation should remain cached");
    assert_eq!(prepared.coordination, replacement_coordination);
    assert_eq!(prepared.operation_id, replacement.operation_id);
}

#[tokio::test]
async fn committed_preparation_survives_coordinator_failure_and_destination_restart() {
    let fixture = EmptyRelayHandoffFixture::new("committed-operation");
    let dir = tempdir().expect("temporary runtime state directory should open");
    let coordination = CoordinationIdentity::new(named("coordinator-a"), 11, 1);
    let source_incarnation = ClusterNodeIncarnation::new(31);
    let prepared_destination_incarnation = {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
            .expect("runtime should open persisted state");
        let destination_incarnation = attach_loopback_cluster(&runtime, &fixture.destination).await;
        fixture
            .rebuild_destination(&runtime, fixture.base_schedule.clone())
            .await;
        fixture
            .prepare(
                &runtime,
                coordination.clone(),
                source_incarnation,
                destination_incarnation,
            )
            .await;
        destination_incarnation
    };

    let db = Database::builder(dir.path())
        .open()
        .expect("database should reopen after destination restart");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("restarted runtime should restore persisted state");
    let restarted_destination_incarnation =
        attach_loopback_cluster(&runtime, &fixture.destination).await;
    let committed_schedule = ClusterSchedule::from_iter([fixture.target_schedule.clone()]);
    let incarnations = BTreeMap::from([
        (fixture.source.clone(), source_incarnation),
        (
            fixture.destination.clone(),
            restarted_destination_incarnation,
        ),
    ]);
    let surviving_authority = CoordinationIdentity::new(named("coordinator-b"), 12, 1);
    assert_eq!(
        runtime
            .reconcile_prepared_ownership_handoffs(
                &surviving_authority,
                &committed_schedule,
                &incarnations,
            )
            .expect("committed preparation should survive reconciliation"),
        0
    );
    assert!(
        runtime
            .inner
            .prepared_runtime_state_handoffs
            .contains_key(&fixture.entity.in_domain(&fixture.domain))
    );

    let mut domain_state = unpaced_domain_state(fixture.domain.as_str());
    domain_state.status = DomainStatus::Stopped;
    runtime.sync_domains(&BTreeMap::from([(fixture.domain.clone(), domain_state)]));
    runtime
        .rebuild_domain_from_schedule(
            &fixture.destination,
            &fixture.domain,
            Some(fixture.target_schedule.clone()),
            false,
        )
        .await
        .expect("the committed restored preparation should activate");
    assert!(
        !runtime
            .inner
            .prepared_runtime_state_handoffs
            .contains_key(&fixture.entity.in_domain(&fixture.domain))
    );
    runtime
        .activate_persisted_ownership_handoff(
            &fixture.destination,
            &ActivateOwnershipHandoffStateRequest {
                coordination: coordination.clone(),
                operation_id: fixture.operation_id.clone(),
                source: fixture.source.clone(),
                destination: fixture.destination.clone(),
                source_incarnation,
                destination_incarnation: prepared_destination_incarnation,
                domain: fixture.domain.clone(),
                entity: fixture.entity.clone(),
                base_schedule_fingerprint: fixture.base_schedule_fingerprint,
                target_schedule_fingerprint: fixture.target_schedule_fingerprint,
                activation_budget: Duration::from_secs(1),
            },
            fixture.target_schedule.clone(),
        )
        .await
        .expect("duplicate activation should be idempotent");
    assert_eq!(
        runtime
            .reconcile_prepared_ownership_handoffs(
                &surviving_authority,
                &committed_schedule,
                &incarnations,
            )
            .expect("reconciliation reordered after activation should be idempotent"),
        0
    );
    let activated = runtime
        .inner
        .activated_runtime_state_handoffs
        .get(&fixture.entity.in_domain(&fixture.domain))
        .expect("committed handoff should be activated after restart");
    assert_eq!(activated.coordination, coordination);
    assert_eq!(activated.operation_id, fixture.operation_id);
    let persisted = runtime
        .inner
        .state_store
        .as_ref()
        .expect("runtime should own a state store")
        .handoff_activation(
            &coordination,
            &fixture.operation_id,
            &fixture.domain,
            fixture.entity.kind,
            &fixture.entity.identifier,
        )
        .expect("activation should remain readable")
        .expect("activation should be durable");
    assert_eq!(persisted.destination, fixture.destination);
}

#[tokio::test]
async fn forced_recovery_completion_survives_runtime_restart_and_schedule_rebuild() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let domain = domain("default");
    let identifier = named::<ModelName>("latest_orders");
    let source = named::<ClusterNodeName>("node-1");
    let destination = named::<ClusterNodeName>("node-2");
    let operation_id = "forced-recovery";
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Relay,
        identifier: identifier.clone(),
        branch_key: None,
    };
    let payload = empty_sealed_container().expect("empty materialized state should seal");
    let prepared = PersistedRuntimeStateEntry {
        lsm: 5,
        payload: payload.clone(),
    };
    let schema = SchemaName::from(&identifier);
    let schema_node = ScheduledNode::new(
        nervix_models::Model::Schema(CreateSchema {
            name: schema.clone(),
            fields: vec![SchemaField {
                name: named("order_id"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }),
        SchemaFingerprint::from_digest([1; 32]),
    );
    let mut scheduled = ScheduledNode::new(
        nervix_models::Model::Relay(CreateRelay {
            name: RelayName::from(&identifier),
            schema,
            buffer: nonzero!(4usize),
            branching: RelayBranching::unbranched(),
            materialized_state: Some(MaterializedRelayState::LastByTimestamp),
        }),
        SchemaFingerprint::from_digest([7; 32]),
    )
    .with_resolved_branching(Some(ResolvedBranching::unbranched()))
    .placed_on(Some(destination.clone()), vec![destination.clone()]);
    scheduled.ownership_transition = Some(OwnershipTransition {
        id: operation_id.to_string(),
        source: source.clone(),
        destination: destination.clone(),
        state_recovery: OwnershipStateRecoveryOutcome::Unverified,
        resets: Vec::new(),
    });
    let initial_schedule = DomainSchedule::new(
        domain.clone(),
        vec![schema_node.clone(), scheduled.clone()],
        Vec::new(),
    );
    let initial_fingerprint = Runtime::ownership_handoff_schedule_fingerprint(&initial_schedule)
        .expect("initial schedule should have a recovery fingerprint");
    let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, identifier.clone());

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
            .expect("runtime should open persisted state");
        let destination_incarnation = attach_loopback_cluster(&runtime, &destination).await;
        let mut domain_state = unpaced_domain_state(domain.as_str());
        domain_state.status = DomainStatus::Stopped;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), domain_state)]));
        let recovery = ForcedRuntimeStateRecoveryTransition {
            operation_id,
            source: &source,
            destination: &destination,
            destination_incarnation,
            entity: &entity,
            target_schedule_fingerprint: initial_fingerprint,
        };
        let store = runtime
            .inner
            .state_store
            .as_ref()
            .expect("runtime should own a state store");
        store
            .persist_forced_recovery_preparation(&recovery, &[(placement.clone(), prepared)])
            .expect("forced recovery preparation should persist");
        runtime
            .rebuild_domain_from_schedule(
                &destination,
                &domain,
                Some(initial_schedule.clone()),
                false,
            )
            .await
            .expect("the initial forced-recovery schedule should build");
        store
            .persist_latest_snapshot(&placement, 6, &payload)
            .expect("new owner checkpoint should persist");
    }

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen after the owner restart");
        let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
            .expect("restarted runtime should open persisted state");
        attach_loopback_cluster(&runtime, &destination).await;
        let mut domain_state = unpaced_domain_state(domain.as_str());
        domain_state.status = DomainStatus::Stopped;
        runtime.sync_domains(&BTreeMap::from([(domain.clone(), domain_state)]));
        let unrelated_schema = ScheduledNode::new(
            nervix_models::Model::Schema(CreateSchema {
                name: named("unrelated_event"),
                fields: vec![SchemaField {
                    name: named("event_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            }),
            SchemaFingerprint::from_digest([1; 32]),
        );
        let rebuilt_schedule = DomainSchedule::new(
            domain.clone(),
            vec![schema_node, scheduled, unrelated_schema],
            Vec::new(),
        );
        let rebuilt_fingerprint =
            Runtime::ownership_handoff_schedule_fingerprint(&rebuilt_schedule)
                .expect("rebuilt schedule should have a recovery fingerprint");
        assert_ne!(initial_fingerprint, rebuilt_fingerprint);
        runtime
            .rebuild_domain_from_schedule(
                &destination,
                &domain,
                Some(rebuilt_schedule.clone()),
                false,
            )
            .await
            .expect("retained recovery should survive the owner restart and domain rebuild");
        runtime
            .rebuild_domain_from_schedule(&destination, &domain, Some(rebuilt_schedule), false)
            .await
            .expect("retained recovery should survive another schedule rebuild");

        let current = runtime
            .inner
            .state_store
            .as_ref()
            .expect("runtime should own a state store")
            .latest_snapshot(&placement)
            .expect("checkpoint should load")
            .expect("new owner checkpoint should remain");
        assert_eq!(current.lsm, 6);
    }
}

#[tokio::test]
async fn forced_recovery_recreates_state_only_for_a_complete_reset_decision() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("database should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("runtime should open persisted state");
    let domain = domain("default");
    let identifier = named::<ModelName>("latest_orders");
    let source = named::<ClusterNodeName>("node-1");
    let destination = named::<ClusterNodeName>("node-2");
    attach_loopback_cluster(&runtime, &destination).await;
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Relay,
        identifier: identifier.clone(),
        branch_key: None,
    };
    let payload = empty_sealed_container().expect("empty materialized state should seal");
    let store = runtime
        .inner
        .state_store
        .as_ref()
        .expect("runtime should own a state store");
    store
        .persist_latest_snapshot(&placement, 6, &payload)
        .expect("current checkpoint should persist");
    let mut scheduled = ScheduledNode::new(
        nervix_models::Model::Relay(CreateRelay {
            name: RelayName::from(&identifier),
            schema: SchemaName::from(&identifier),
            buffer: nonzero!(4usize),
            branching: RelayBranching::unbranched(),
            materialized_state: Some(MaterializedRelayState::LastByTimestamp),
        }),
        SchemaFingerprint::from_digest([7; 32]),
    )
    .with_resolved_branching(Some(ResolvedBranching::unbranched()))
    .placed_on(Some(destination.clone()), vec![destination.clone()]);
    scheduled.ownership_transition = Some(OwnershipTransition {
        id: "forced-reset".to_string(),
        source,
        destination: destination.clone(),
        state_recovery: OwnershipStateRecoveryOutcome::Reset,
        resets: Vec::new(),
    });

    let incomplete = runtime
        .activate_prepared_forced_ownership_recovery_state(
            &domain,
            &scheduled,
            &destination,
            [9; 32],
            false,
        )
        .expect_err("a reset without its state-component outcome should fail");
    assert!(matches!(
        incomplete.current_context(),
        RuntimePersistenceError::InvalidForcedRecoveryDecision
    ));
    assert_eq!(
        store
            .latest_snapshot(&placement)
            .expect("current checkpoint should load")
            .expect("an invalid reset must preserve the checkpoint")
            .lsm,
        6
    );

    scheduled
        .ownership_transition
        .as_mut()
        .expect("scheduled recovery should exist")
        .resets
        .push(OwnershipStateReset {
            component: OwnershipStateComponent::MaterializedRelay,
            cause: OwnershipStateResetCause::MissingCheckpoint,
        });
    runtime
        .activate_prepared_forced_ownership_recovery_state(
            &domain,
            &scheduled,
            &destination,
            [9; 32],
            false,
        )
        .expect("a complete reset decision should recreate state");
    assert!(
        store
            .latest_snapshot(&placement)
            .expect("checkpoint lookup should succeed")
            .is_none(),
        "the complete reset decision should remove the prior checkpoint"
    );
}

#[test]
fn forced_recovery_replay_preserves_source_offsets_and_branch_processor_state() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let domain = domain("default");
    let source = named::<ClusterNodeName>("node-1");
    let destination = named::<ClusterNodeName>("node-2");
    let kafka_placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::KafkaOffset,
        kind: ModelKind::Ingestor,
        identifier: named("orders_source"),
        branch_key: None,
    };
    let kafka_state = Arc::new(
        ReplicatedKafkaOffsetState::new(kafka_placement.clone(), None)
            .expect("Kafka offset state should initialize"),
    );
    let mut kafka_assignment =
        ReplicatedKafkaOffsetState::bind(&kafka_state, StateReplicationRoles::owned_by(None), None);
    let kafka_originator = kafka_assignment
        .originator
        .take()
        .expect("local Kafka state should grant authoritative access");
    let (_, kafka_payload) = kafka_originator
        .replace_offsets(HashMap::from_iter([(
            KafkaTopicPartition {
                topic: "orders".to_string(),
                partition: 3,
            },
            42,
        )]))
        .expect("Kafka offset should update");
    let deduplicator_placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("deduplicate_orders"),
        branch_key: string_branch_key("tenant", "acme"),
    };
    let sibling_branch_placement = RuntimeStatePlacement {
        branch_key: string_branch_key("tenant", "globex"),
        ..deduplicator_placement.clone()
    };
    let deduplicator_state = Arc::new(
        ReplicatedDeduplicatorState::new(deduplicator_placement.clone(), None)
            .expect("deduplicator state should initialize"),
    );
    let deduplicator_key =
        DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("order-123".to_string())]);
    let mut deduplicator_keyspace = ReplicatedDeduplicatorState::keyspace(&deduplicator_state);
    assert!(deduplicator_keyspace.reserve_new_key(
        deduplicator_key.clone(),
        Timestamp::from_unix_nanos(1),
        Duration::from_secs(600),
    ));
    deduplicator_keyspace.publish();
    let deduplicator_payload = deduplicator_state
        .latest_snapshot()
        .expect("deduplicator state should snapshot")
        .payload;
    let other_entity_placement = RuntimeStatePlacement {
        identifier: named("other_deduplicator"),
        branch_key: string_branch_key("tenant", "acme"),
        ..deduplicator_placement.clone()
    };
    let kafka_entity = DomainNodeRef::node_in(
        domain.clone(),
        ModelKind::Ingestor,
        kafka_placement.identifier.clone(),
    );
    let deduplicator_entity = DomainNodeRef::node_in(
        domain.clone(),
        ModelKind::Deduplicator,
        deduplicator_placement.identifier.clone(),
    );
    let kafka_recovery = ForcedRuntimeStateRecoveryTransition {
        operation_id: "recover-kafka-source",
        source: &source,
        destination: &destination,
        destination_incarnation: ClusterNodeIncarnation::new(42),
        entity: &kafka_entity,
        target_schedule_fingerprint: [9; 32],
    };
    let deduplicator_recovery = ForcedRuntimeStateRecoveryTransition {
        operation_id: "recover-deduplicator",
        source: &source,
        destination: &destination,
        destination_incarnation: ClusterNodeIncarnation::new(42),
        entity: &deduplicator_entity,
        target_schedule_fingerprint: [9; 32],
    };
    let kafka_prepared = PersistedRuntimeStateEntry {
        lsm: 5,
        payload: kafka_payload.clone(),
    };
    let deduplicator_prepared = PersistedRuntimeStateEntry {
        lsm: 5,
        payload: deduplicator_payload.clone(),
    };

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should open");
        store
            .persist_forced_recovery_preparation(
                &kafka_recovery,
                &[(kafka_placement.clone(), kafka_prepared.clone())],
            )
            .expect("Kafka recovery preparation should persist");
        store
            .persist_forced_recovery_preparation(
                &deduplicator_recovery,
                &[
                    (
                        deduplicator_placement.clone(),
                        deduplicator_prepared.clone(),
                    ),
                    (
                        sibling_branch_placement.clone(),
                        deduplicator_prepared.clone(),
                    ),
                ],
            )
            .expect("branch recovery preparation should persist");
        assert!(
            store
                .activate_forced_recovery(
                    &kafka_recovery,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
                )
                .expect("Kafka recovery should activate")
                .is_some()
        );
        assert!(
            store
                .activate_forced_recovery(
                    &deduplicator_recovery,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
                )
                .expect("branch recovery should activate")
                .is_some()
        );
        store
            .persist_latest_snapshot(&kafka_placement, 6, &kafka_payload)
            .expect("new Kafka offset checkpoint should persist");
        store
            .persist_latest_snapshot(&deduplicator_placement, 6, &deduplicator_payload)
            .expect("new branch checkpoint should persist");
        store
            .persist_latest_snapshot(&sibling_branch_placement, 7, &deduplicator_payload)
            .expect("new sibling branch checkpoint should persist");
        store
            .persist_latest_snapshot(&other_entity_placement, 8, &deduplicator_payload)
            .expect("other entity checkpoint should persist");
    }

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should reopen");
        let replayed_kafka_recovery = ForcedRuntimeStateRecoveryTransition {
            destination_incarnation: ClusterNodeIncarnation::new(43),
            target_schedule_fingerprint: [10; 32],
            ..kafka_recovery
        };
        let replayed_deduplicator_recovery = ForcedRuntimeStateRecoveryTransition {
            destination_incarnation: ClusterNodeIncarnation::new(43),
            target_schedule_fingerprint: [10; 32],
            ..deduplicator_recovery
        };
        store
            .persist_forced_recovery_preparation(
                &replayed_kafka_recovery,
                &[(kafka_placement.clone(), kafka_prepared)],
            )
            .expect("completed Kafka recovery should accept a repeated preparation request");
        store
            .persist_forced_recovery_preparation(
                &replayed_deduplicator_recovery,
                &[(deduplicator_placement.clone(), deduplicator_prepared)],
            )
            .expect("completed branch recovery should accept a repeated preparation request");
        assert!(
            store
                .activate_forced_recovery(
                    &replayed_kafka_recovery,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
                )
                .expect("completed Kafka recovery should replay")
                .is_none()
        );
        assert!(
            store
                .activate_forced_recovery(
                    &replayed_deduplicator_recovery,
                    ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                    None,
                )
                .expect("completed branch recovery should replay")
                .is_none()
        );

        let kafka_snapshot = store
            .latest_snapshot(&kafka_placement)
            .expect("Kafka checkpoint should load")
            .expect("Kafka checkpoint should remain");
        assert_eq!(kafka_snapshot.lsm, 6);
        let restored_kafka = Arc::new(
            ReplicatedKafkaOffsetState::new(kafka_placement, Some(kafka_snapshot))
                .expect("preserved Kafka checkpoint should decode"),
        );
        assert_eq!(
            ReplicatedKafkaOffsetState::read(&restored_kafka).next_offset("orders", 3),
            Some(42)
        );
        let deduplicator_snapshot = store
            .latest_snapshot(&deduplicator_placement)
            .expect("branch checkpoint should load")
            .expect("branch checkpoint should remain");
        assert_eq!(deduplicator_snapshot.lsm, 6);
        let restored_deduplicator = Arc::new(
            ReplicatedDeduplicatorState::new(deduplicator_placement, Some(deduplicator_snapshot))
                .expect("preserved branch checkpoint should decode"),
        );
        let mut restored_keyspace = ReplicatedDeduplicatorState::keyspace(&restored_deduplicator);
        assert!(!restored_keyspace.reserve_new_key(
            deduplicator_key,
            Timestamp::from_unix_nanos(2),
            Duration::from_secs(600),
        ));
        assert_eq!(
            store
                .latest_snapshot(&sibling_branch_placement)
                .expect("sibling branch checkpoint should load")
                .expect("sibling branch checkpoint should remain")
                .lsm,
            7
        );
        assert_eq!(
            store
                .latest_snapshot(&other_entity_placement)
                .expect("other entity checkpoint should load")
                .expect("other entity checkpoint should remain")
                .lsm,
            8
        );
    }
}

#[test]
fn forced_recovery_refuses_missing_or_stale_preparation_without_changing_state() {
    let dir = tempdir().expect("temporary runtime state directory should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("database should open");
    let store =
        RuntimeStateStore::from_database(db, Executor::default()).expect("state store should open");
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
        branch_key: string_branch_key("tenant", "acme"),
    };
    let sibling_branch = RuntimeStatePlacement {
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

#[test]
fn runtime_state_store_persists_latest_snapshot_with_monotonic_lsm() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store =
        RuntimeStateStore::from_database(db, Executor::default()).expect("state store should open");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::Deduplicator {
            schema: unchanged_schema_fingerprint(),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
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
async fn deduplicator_snapshot_task_persists_published_keys_on_interval() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_millis(10))
        .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::Deduplicator {
            schema: unchanged_schema_fingerprint(),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_deduplicator_state(placement.clone())
        .expect("deduplicator state should initialize");
    let (shutdown_tx, _) = watch::channel(false);
    let (snapshot_request_tx, mut snapshot_requests) = mpsc::channel(1);
    let task = runtime
        .spawn_published_branch_state_snapshot_task(
            &shutdown_tx,
            PublishedBranchState::Deduplicator(state.clone()),
            snapshot_request_tx,
        )
        .expect("persisted runtime should spawn a snapshot task");
    let mut keyspace = ReplicatedDeduplicatorState::keyspace(&state);
    assert!(keyspace.reserve_new_key(
        DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]),
        Timestamp::from_unix_nanos(1),
        Duration::from_secs(600),
    ));
    let snapshot_owner = tokio::spawn(async move {
        let response = timeout(Duration::from_secs(1), snapshot_requests.recv())
            .await
            .expect("snapshot task should ask the branch task to publish")
            .expect("snapshot request channel should remain open");
        keyspace.publish();
        response
            .send(Ok(()))
            .expect("the snapshot task waits for the publication it asked for");
    });
    let expected_lsm = 1;

    wait_for_persisted_runtime_state_lsm(&runtime, &placement, expected_lsm).await;
    snapshot_owner
        .await
        .expect("snapshot owner should stop cleanly");
    assert_eq!(state.generations.last_persisted_lsm(), expected_lsm);
    assert!(!state.generations.is_live_dirty());

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");
}

/// A branch task that is gone, such as one aborted past its shutdown grace, can no longer publish.
/// What it published before is then the newest state anyone can restore, so the snapshot task still
/// persists it.
#[tokio::test]
async fn deduplicator_snapshot_task_persists_published_keys_after_the_branch_task_is_gone() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::Deduplicator {
            schema: unchanged_schema_fingerprint(),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_deduplicator_state(placement.clone())
        .expect("deduplicator state should initialize");
    let (shutdown_tx, _) = watch::channel(false);
    let (snapshot_request_tx, snapshot_requests) = mpsc::channel(1);
    let task = runtime
        .spawn_published_branch_state_snapshot_task(
            &shutdown_tx,
            PublishedBranchState::Deduplicator(state.clone()),
            snapshot_request_tx,
        )
        .expect("persisted runtime should spawn a snapshot task");
    let mut keyspace = ReplicatedDeduplicatorState::keyspace(&state);
    assert!(keyspace.reserve_new_key(
        DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]),
        Timestamp::from_unix_nanos(1),
        Duration::from_secs(600),
    ));
    keyspace.publish();
    assert!(keyspace.reserve_new_key(
        DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-2".to_string())]),
        Timestamp::from_unix_nanos(2),
        Duration::from_secs(600),
    ));
    drop(keyspace);
    drop(snapshot_requests);

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");

    let persisted = runtime
        .inner
        .state_store
        .as_ref()
        .expect("test runtime should have a state store")
        .latest_snapshot(&placement)
        .expect("snapshot lookup should succeed")
        .expect("the keys the branch task published should be persisted after it is gone");
    assert_eq!(persisted.lsm, 1);
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
        state: RuntimeState::MaterializedRelay {
            schema: unchanged_schema_fingerprint(),
        },
        kind: ModelKind::Relay,
        identifier: named("latest_orders"),
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
        .apply_materialized_stream_records(&state, &None, [record])
        .await
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
async fn kafka_offset_snapshot_task_owns_persistence() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_secs(3_600))
        .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::KafkaOffset,
        kind: ModelKind::Ingestor,
        identifier: named("orders_source"),
        branch_key: None,
    };
    let mut assignment = runtime
        .replicated_kafka_offset_state(placement.clone(), None, Vec::new(), 0, None)
        .expect("Kafka offset state should initialize");
    let originator = assignment
        .originator
        .take()
        .expect("local Kafka state should grant authoritative access");
    let persistence = assignment.persistence;
    let (shutdown_tx, _) = watch::channel(false);
    let task = runtime
        .spawn_kafka_offset_snapshot_task(&shutdown_tx, persistence.clone())
        .expect("persisted runtime should spawn a snapshot task");
    let store = runtime
        .inner
        .state_store
        .as_ref()
        .expect("test runtime should have a state store")
        .clone();

    runtime
        .commit_domain_kafka_offset(&originator, "orders", 3, 43)
        .await
        .expect("the Kafka offset assignment should remain authoritative");

    assert!(
        store
            .latest_snapshot(&placement)
            .expect("snapshot lookup should succeed")
            .is_none(),
        "the offset commit path must not encode or persist a snapshot"
    );
    assert_eq!(originator.read().current_lsm(), 1);
    assert_eq!(persistence.last_persisted_lsm(), 0);

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");
    let flushed = store
        .latest_snapshot(&placement)
        .expect("snapshot lookup should succeed")
        .expect("shutdown should flush the committed offset");
    assert_eq!(flushed.lsm, 1);
    let restored = Arc::new(
        ReplicatedKafkaOffsetState::new(placement, Some(flushed))
            .expect("the flushed offset snapshot should decode"),
    );
    assert_eq!(
        ReplicatedKafkaOffsetState::read(&restored).next_offset("orders", 3),
        Some(43)
    );
}

#[tokio::test]
async fn window_processor_snapshot_task_persists_published_state_on_interval() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let runtime = Runtime::with_persistence(Some(db), Duration::from_millis(10))
        .expect("runtime should open persisted state");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::WindowProcessor {
            schema: unchanged_schema_fingerprint(),
        },
        kind: ModelKind::WindowProcessor,
        identifier: named("latency_window"),
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_window_processor_state(placement.clone())
        .expect("window processor state should initialize");
    let (shutdown_tx, _) = watch::channel(false);
    let (snapshot_request_tx, mut snapshot_requests) = mpsc::channel(1);
    let task = runtime
        .spawn_published_branch_state_snapshot_task(
            &shutdown_tx,
            PublishedBranchState::WindowProcessor(state.clone()),
            snapshot_request_tx,
        )
        .expect("persisted runtime should spawn a snapshot task");
    let live_state = WindowProcessorState::new(&window_plan(
        "SET count = COUNT(input.latency)",
        ParseAsType::I64,
        &[("count", ParseAsType::I64)],
    ));
    let snapshot_state = state.clone();
    let snapshot_branch = placement.branch_key.clone();
    let snapshot_owner = tokio::spawn(async move {
        let response = timeout(Duration::from_secs(1), snapshot_requests.recv())
            .await
            .expect("snapshot task should ask the branch task to publish")
            .expect("snapshot request channel should remain open");
        let published = snapshot_state.replace_state(&live_state).map_err(|error| {
            Report::new(error).change_context(ProcessorLiveStateError {
                branch: snapshot_branch,
            })
        });
        response
            .send(published)
            .expect("the snapshot task waits for the publication it asked for");
    });

    state.generations.mark_live_dirty();
    let expected_lsm = 1;

    wait_for_persisted_runtime_state_lsm(&runtime, &placement, expected_lsm).await;
    snapshot_owner
        .await
        .expect("snapshot owner should stop cleanly");
    assert_eq!(state.generations.last_persisted_lsm(), expected_lsm);
    assert!(!state.generations.is_live_dirty());

    shutdown_tx.send_replace(true);
    task.await.expect("snapshot task should stop cleanly");
}

/// The snapshot task, replicas and ownership handoff read the window a branch published for as
/// long as encoding it takes. The branch keeps publishing meanwhile, and the window they read stays
/// the generation they loaded.
#[test]
fn a_window_state_publication_proceeds_while_a_snapshot_reads_the_previous_one() {
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::WindowProcessor {
            schema: unchanged_schema_fingerprint(),
        },
        kind: ModelKind::WindowProcessor,
        identifier: named("latency_window"),
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = ReplicatedWindowProcessorState::new(placement, None)
        .expect("window processor state should initialize");
    let live_state = WindowProcessorState::new(&window_plan(
        "SET count = COUNT(input.latency)",
        ParseAsType::I64,
        &[("count", ParseAsType::I64)],
    ));
    state
        .replace_state(&live_state)
        .expect("the first window state should publish");
    let snapshot_read = state.generations.load();

    state
        .replace_state(&live_state)
        .expect("a later window state should publish while the first one is read");

    let latest = state.generations.load();
    assert_eq!(snapshot_read.revision, 1);
    assert!(snapshot_read.value.is_some());
    assert_eq!(latest.revision, 2);
    assert!(
        !std::sync::Arc::ptr_eq(&snapshot_read, &latest),
        "publishing replaced the window a snapshot was reading in place"
    );
}

#[test]
fn runtime_state_store_purges_only_stale_schema_fingerprints() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store =
        RuntimeStateStore::from_database(db, Executor::default()).expect("state store should open");
    let base = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([1; 32]),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        branch_key: None,
    };
    let current_schema = SchemaFingerprint::from_digest([2; 32]);
    let current = RuntimeStatePlacement {
        state: RuntimeState::Deduplicator {
            schema: current_schema,
        },
        ..base.clone()
    };
    store
        .persist_latest_snapshot(&base, 1, b"old")
        .expect("old snapshot should persist");
    store
        .persist_latest_snapshot(&current, 2, b"current")
        .expect("current snapshot should persist");

    store
        .purge_stale_state_identities(
            &base.domain,
            &HashMap::from_iter([(
                NodeRef {
                    kind: base.kind,
                    identifier: base.identifier.clone(),
                },
                ScheduledStateIdentity {
                    schema_fingerprint: current_schema,
                    wasm_state_generations: None,
                },
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
    let store =
        RuntimeStateStore::from_database(db, Executor::default()).expect("state store should open");
    let stopped = RuntimeStatePlacement {
        domain: domain("stopped"),
        state: RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([1; 32]),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
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
    let store =
        RuntimeStateStore::from_database(db, Executor::default()).expect("state store should open");
    let removed = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([1; 32]),
        },
        kind: ModelKind::Relay,
        identifier: named("events"),
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
            removed.state.kind(),
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
    let store =
        RuntimeStateStore::from_database(db, Executor::default()).expect("state store should open");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::KafkaOffset,
        kind: ModelKind::Ingestor,
        identifier: named("kafka_notifications"),
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
        state: RuntimeState::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: named("redis_notifications"),
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
    metrics
        .resolve_node_batch_metrics(NodeBatchMetricsSpec {
            domain: &placement.domain,
            kind: placement.kind,
            node: &placement.identifier,
            relay: &relay,
            physical_node_id: Some(&ClusterNodeName::parse("node-1").expect("valid name")),
            direction: "sent",
            branch_key: None,
        })
        .observe(2, 64, None);
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
        state: RuntimeState::Deduplicator {
            schema: unchanged_schema_fingerprint(),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
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
    let mut keyspace = ReplicatedDeduplicatorState::keyspace(&state);
    assert!(keyspace.reserve_new_key(
        DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]),
        Timestamp::from_unix_nanos(1),
        Duration::from_secs(600),
    ));
    let unpublished = runtime
        .handle_state_sync_request(&placement, Some(0))
        .await
        .expect("state sync request before the branch publishes should succeed");
    assert!(
        unpublished.is_none(),
        "a state sync request serves only what the branch task published"
    );
    keyspace.publish();
    let lsm = state.generations.load().revision;

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
        state: RuntimeState::Deduplicator {
            schema: unchanged_schema_fingerprint(),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = Arc::new(
        ReplicatedDeduplicatorState::new(placement, None)
            .expect("deduplicator state should initialize"),
    );
    let mut keyspace = ReplicatedDeduplicatorState::keyspace(&state);
    let seen_at = Timestamp::from_unix_nanos(1);
    let max_time = Duration::from_secs(600);

    let key = DeduplicatorKey::new(vec![ReorderKeyPart::Utf8("txn-1".to_string())]);
    assert!(keyspace.reserve_new_key(key.clone(), seen_at, max_time));
    keyspace.publish();
    assert!(!keyspace.reserve_new_key(key, seen_at, max_time));
    assert!(
        !state.generations.is_live_dirty(),
        "a duplicate key changed the keyspace"
    );
    assert_eq!(state.generations.load().revision, 1);
}

#[test]
fn runtime_state_placement_storage_key_includes_branch_key() {
    let tenant_beta = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([1; 32]),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        branch_key: string_branch_key("tenant", "beta"),
    };
    let tenant = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([1; 32]),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        branch_key: string_branch_key("tenant", "acme"),
    };

    assert_ne!(tenant_beta.as_storage_key(), tenant.as_storage_key());
    let branch_aggregated = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::BranchAggregated,
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        branch_key: None,
    };
    assert_ne!(
        tenant_beta.as_storage_key(),
        branch_aggregated.as_storage_key()
    );
    let deduplicator_global = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([1; 32]),
        },
        kind: ModelKind::Deduplicator,
        identifier: named("dedup_orders"),
        branch_key: None,
    };
    assert_ne!(
        deduplicator_global.as_storage_key(),
        branch_aggregated.as_storage_key()
    );
}

/// Reinstalling a schedule must never leave a scheduled node without its fingerprint, even
/// for an instant. `state_placement` keys every schema-bound runtime state by that fingerprint, so
/// a reader that resolves a placement while the map is being rebuilt would find no identity for the
/// node and could not address the state it owns. Relocation rebuilds the fingerprints while the
/// relay's own state task is still running, which is exactly when that read happens.
#[test]
fn reinstalling_schema_fingerprints_never_exposes_a_node_without_one() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let identifier = named::<ModelName>("moving_state");
    let schedule = DomainSchedule::new(
        domain.clone(),
        vec![
            ScheduledNode::new(
                nervix_models::Model::Relay(nervix_models::CreateRelay {
                    name: nervix_models::RelayName::from(&identifier.clone()),
                    schema: nervix_models::SchemaName::from(&identifier.clone()),
                    buffer: nonzero!(4usize),
                    branching: nervix_models::RelayBranching::unbranched(),
                    materialized_state: Some(
                        nervix_models::MaterializedRelayState::LastByTimestamp,
                    ),
                }),
                SchemaFingerprint::from_digest([1; 32]),
            )
            .with_resolved_branching(Some(ResolvedBranching::unbranched()))
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
    runtime.install_state_identities(&schedule);
    let installed = resolve().expect("the installed schedule publishes the relay's fingerprint");

    let reads_stopped = AtomicBool::new(false);
    let missed = AtomicBool::new(false);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            while !reads_stopped.load(Ordering::Relaxed) {
                match resolve() {
                    Ok(placement) if placement == installed => {}
                    Ok(_) | Err(_) => missed.store(true, Ordering::Relaxed),
                }
            }
        });
        for _ in 0..2_000 {
            runtime.install_state_identities(&schedule);
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
            vec![deduplicator_node(&identifier, fingerprint)],
            Vec::new(),
        )
    };

    runtime.install_state_identities(&schedule(SchemaFingerprint::from_digest([1; 32])));
    let original_placement = runtime
        .state_placement(
            &domain,
            RuntimeStateKind::Deduplicator,
            ModelKind::Deduplicator,
            &identifier,
            None,
        )
        .expect("the installed schedule publishes the deduplicator's fingerprint");
    let original = runtime
        .replicated_deduplicator_state(original_placement.clone())
        .expect("state should initialize");

    runtime.install_state_identities(&schedule(SchemaFingerprint::from_digest([1; 32])));
    let unchanged = runtime
        .replicated_deduplicator_state(
            runtime
                .state_placement(
                    &domain,
                    RuntimeStateKind::Deduplicator,
                    ModelKind::Deduplicator,
                    &identifier,
                    None,
                )
                .expect("the installed schedule publishes the deduplicator's fingerprint"),
        )
        .expect("unchanged state should initialize");
    assert!(Arc::ptr_eq(&original, &unchanged));

    runtime.install_state_identities(&schedule(SchemaFingerprint::from_digest([2; 32])));
    let changed = runtime
        .replicated_deduplicator_state(
            runtime
                .state_placement(
                    &domain,
                    RuntimeStateKind::Deduplicator,
                    ModelKind::Deduplicator,
                    &identifier,
                    None,
                )
                .expect("the installed schedule publishes the deduplicator's fingerprint"),
        )
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

/// Materialized relay state belongs to the domain start that began it, so a START resets it. An
/// execution built from a domain's graph keys that state exactly as the schedule of the same graph
/// does; otherwise the state one of them writes is purged as stale by the other.
#[test]
fn graph_and_schedule_key_materialized_relay_state_alike() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let mut restarted = unpaced_domain_state(domain.as_str());
    restarted.start_version = 2;
    runtime.sync_domains(&BTreeMap::from([(domain.clone(), restarted)]));
    let relay = named::<RelayName>("events");
    let schema = named::<SchemaName>("event");
    let models = vec![
        nervix_models::Model::Schema(CreateSchema {
            name: schema.clone(),
            fields: vec![SchemaField {
                name: named("seq"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }),
        nervix_models::Model::Relay(CreateRelay {
            name: relay.clone(),
            schema,
            buffer: nonzero!(4usize),
            branching: RelayBranching::unbranched(),
            materialized_state: Some(MaterializedRelayState::LastByTimestamp),
        }),
    ];
    let unscheduled = models
        .into_iter()
        .map(|model| ScheduledNode::new(model, SchemaFingerprint::from_digest([1; 32])));
    let graph = ActiveGraph::from_scheduled_models(&DomainSchedule::new(
        domain.clone(),
        unscheduled,
        Vec::new(),
    ))
    .expect("a schema and the relay it lays out form a valid graph");
    let nodes = graph.unplaced_schedule_nodes();
    let schedule = DomainSchedule::new(domain.clone(), nodes.clone(), Vec::new());
    let placement = || {
        runtime
            .state_placement(
                &domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                &relay,
                None,
            )
            .expect("both installs publish the relay's schema fingerprint")
    };

    runtime.install_state_identities_from_graph(&domain, &nodes);
    let from_graph = placement();
    runtime.install_state_identities(&schedule);
    let from_schedule = placement();

    assert_eq!(from_graph, from_schedule);
}

/// Branch-aggregated metrics and Kafka offsets depend on no schema, so they are placed without any
/// published identity. Every other kind of state is placed only under the schema fingerprint the
/// committed schedule publishes for its node, and WASM guest state also in the generation it names,
/// so a node without them reports which one is missing instead of addressing some other state.
#[test]
fn schema_bound_state_is_placed_only_under_a_published_identity() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let identifier = named::<ModelName>("counting_guest");
    let place = |state| {
        runtime.state_placement(&domain, state, ModelKind::WasmProcessor, &identifier, None)
    };
    for (state, placed) in [
        (
            RuntimeStateKind::BranchAggregated,
            RuntimeState::BranchAggregated,
        ),
        (RuntimeStateKind::KafkaOffset, RuntimeState::KafkaOffset),
    ] {
        let placement = place(state).expect("state that depends on no schema needs no identity");
        assert_eq!(placement.state, placed);
    }
    for state in [
        RuntimeStateKind::Correlator,
        RuntimeStateKind::Deduplicator,
        RuntimeStateKind::MaterializedRelay,
        RuntimeStateKind::WasmProcessor,
        RuntimeStateKind::WindowProcessor,
        RuntimeStateKind::BranchLru,
    ] {
        let unpublished = place(state).expect_err("schema-bound state needs a published identity");
        assert!(matches!(
            unpublished.current_context(),
            StateIdentityError::SchemaFingerprintUnpublished { .. }
        ));
    }

    let node = wasm_processor_node();
    runtime.install_state_identities_from_graph(&domain, std::slice::from_ref(&node));
    let lifecycle = place(RuntimeStateKind::BranchLru).expect("a graph publishes the fingerprint");
    assert_eq!(
        lifecycle.state,
        RuntimeState::BranchLru {
            schema: node.schema_fingerprint
        }
    );
    let ungenerated = place(RuntimeStateKind::WasmProcessor)
        .expect_err("a graph publishes no guest-state generation");
    assert!(matches!(
        ungenerated.current_context(),
        StateIdentityError::GenerationUnpublished { .. }
    ));

    runtime.install_state_identities(&DomainSchedule::new(
        domain.clone(),
        vec![node.clone()],
        Vec::new(),
    ));
    let guest = place(RuntimeStateKind::WasmProcessor)
        .expect("a committed schedule publishes the guest-state generation");
    assert_eq!(
        guest.state,
        RuntimeState::WasmProcessor {
            schema: node.schema_fingerprint,
            generation: nervix_models::WasmStateGeneration::FIRST,
        }
    );
}

/// A schema change publishes a new fingerprint for the node, so a checkpoint written under the
/// replaced one no longer names current state: no replica installs or serves it and no ownership
/// handoff carries it. State that depends on no schema keeps its placement and stays current.
#[test]
fn a_schema_change_leaves_only_schema_bound_checkpoints_stale() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let identifier = named::<ModelName>("dedup_orders");
    let schedule = |fingerprint| {
        DomainSchedule::new(
            domain.clone(),
            vec![deduplicator_node(&identifier, fingerprint)],
            Vec::new(),
        )
    };
    let place = |state, branch_key| {
        runtime
            .state_placement(
                &domain,
                state,
                ModelKind::Deduplicator,
                &identifier,
                branch_key,
            )
            .expect("the installed schedule publishes the deduplicator's identity")
    };
    let acme = string_branch_key("tenant", "acme");

    runtime.install_state_identities(&schedule(SchemaFingerprint::from_digest([1; 32])));
    let replaced = place(RuntimeStateKind::Deduplicator, acme.clone());
    let metrics = place(RuntimeStateKind::BranchAggregated, None);
    assert!(runtime.runtime_state_placement_is_current(&replaced));

    runtime.install_state_identities(&schedule(SchemaFingerprint::from_digest([2; 32])));
    let current = place(RuntimeStateKind::Deduplicator, acme);

    assert_ne!(current, replaced);
    assert!(!runtime.runtime_state_placement_is_current(&replaced));
    assert!(runtime.runtime_state_placement_is_current(&current));
    assert_eq!(place(RuntimeStateKind::BranchAggregated, None), metrics);
    assert!(runtime.runtime_state_placement_is_current(&metrics));
}

fn deduplicator_node(identifier: &ModelName, fingerprint: SchemaFingerprint) -> ScheduledNode {
    ScheduledNode::new(
        nervix_models::Model::Deduplicator(nervix_models::CreateDeduplicator {
            name: nervix_models::DeduplicatorName::from(identifier),
            from: nervix_models::ProcessorInputs::new(Vec::new(), Vec::new()),
            output_routes: nervix_models::ProcessorOutputs::new(Vec::new()),
            branched_by: nervix_models::BranchSelection::unbranched(),
            deduplicate_on: Vec::new(),
            max_time: "1m".to_string(),
            mode: nervix_models::AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }),
        fingerprint,
    )
    .placed_on(
        Some(ClusterNodeName::parse("node-1").expect("valid name")),
        vec![ClusterNodeName::parse("node-1").expect("valid name")],
    )
}

/// The fingerprint of a schema a test never changes.
fn unchanged_schema_fingerprint() -> SchemaFingerprint {
    SchemaFingerprint::from_digest([7; 32])
}

fn wasm_processor_node() -> ScheduledNode {
    ScheduledNode::new(
        nervix_models::Model::WasmProcessor(nervix_models::CreateWasmProcessor {
            name: named("counting_guest"),
            from: nervix_models::ProcessorInputs::single(named("counted_input")),
            output_routes: nervix_models::ProcessorOutputs::single(named("counted_output")),
            branched_by: nervix_models::BranchSelection::unbranched(),
            resource: named("counting_bundle"),
            resource_version: 1,
            file: "processors/counting.wasm".to_string(),
            limits: nervix_models::WasmProcessorLimits {
                max_fuel: nonzero!(1_000_000u64),
                max_memory_bytes: nonzero!(67_108_864u64),
            },
            global_error_policy: nervix_models::GeneralErrorPolicy::Log,
            mode: nervix_models::AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }),
        SchemaFingerprint::from_digest([5; 32]),
    )
}

fn guest_state_placement(
    runtime: &Runtime,
    domain: &DomainName,
    branch: Option<BranchKey>,
) -> RuntimeStatePlacement {
    runtime
        .state_placement(
            domain,
            RuntimeStateKind::WasmProcessor,
            ModelKind::WasmProcessor,
            named::<ModelName>("counting_guest"),
            branch,
        )
        .expect("the installed schedule publishes the processor's generations")
}

/// Only the generation the committed schedule names for a branch is current. A concrete-branch
/// transition leaves every other branch in its lifetime, a transition of every branch replaces all
/// of them at once, and installing the same committed schedule again changes nothing.
#[test]
fn only_the_committed_generation_of_each_branch_is_current() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let acme = string_branch_key("tenant", "acme");
    let beta = string_branch_key("tenant", "beta");
    let mut node = wasm_processor_node();
    let install = |node: &ScheduledNode| {
        runtime.install_state_identities(&DomainSchedule::new(
            domain.clone(),
            vec![node.clone()],
            Vec::new(),
        ));
    };

    install(&node);
    let first_acme = guest_state_placement(&runtime, &domain, acme.clone());
    let first_beta = guest_state_placement(&runtime, &domain, beta.clone());
    assert!(runtime.runtime_state_placement_is_current(&first_acme));
    assert!(runtime.runtime_state_placement_is_current(&first_beta));

    let acme_fingerprint = acme
        .as_ref()
        .expect("the acme branch is concrete")
        .fingerprint();
    node.begin_wasm_branch_state_generation(acme_fingerprint);
    install(&node);
    let second_acme = guest_state_placement(&runtime, &domain, acme.clone());
    assert_ne!(second_acme, first_acme);
    assert!(!runtime.runtime_state_placement_is_current(&first_acme));
    assert!(runtime.runtime_state_placement_is_current(&second_acme));
    assert_eq!(
        guest_state_placement(&runtime, &domain, beta.clone()),
        first_beta
    );
    assert!(runtime.runtime_state_placement_is_current(&first_beta));

    node.begin_wasm_state_generation();
    install(&node);
    install(&node);
    let third_acme = guest_state_placement(&runtime, &domain, acme);
    let third_beta = guest_state_placement(&runtime, &domain, beta);
    for replaced in [&first_acme, &second_acme, &first_beta] {
        assert!(!runtime.runtime_state_placement_is_current(replaced));
    }
    assert_eq!(third_acme.state, third_beta.state);
    assert!(runtime.runtime_state_placement_is_current(&third_acme));
    assert!(runtime.runtime_state_placement_is_current(&third_beta));
}

/// A branch task that outlives a generation transition must not publish or persist its next
/// checkpoint: nothing restores the lifetime that checkpoint describes.
#[test]
fn a_checkpoint_of_a_replaced_generation_is_refused_before_it_is_published() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let mut node = wasm_processor_node();
    runtime.install_state_identities(&DomainSchedule::new(
        domain.clone(),
        vec![node.clone()],
        Vec::new(),
    ));
    let placement = guest_state_placement(&runtime, &domain, string_branch_key("tenant", "acme"));
    let state = runtime
        .replicated_wasm_processor_state(placement)
        .expect("guest state should initialize");
    assert_eq!(
        runtime
            .wasm_checkpoint_boundary(&state)
            .expect("a checkpoint in the current generation is authorized"),
        WasmCheckpointBoundary::LocalStorage
    );

    node.begin_wasm_state_generation();
    runtime.install_state_identities(&DomainSchedule::new(domain, vec![node], Vec::new()));

    let refused = runtime
        .wasm_checkpoint_boundary(&state)
        .expect_err("a checkpoint of a replaced generation must be refused");
    assert!(matches!(
        refused.current_context(),
        StateReplicationError::Superseded { .. }
    ));
    assert!(refused.current_context().is_authority_rejection());
    assert_eq!(state.committed_revision(), 0);
}

/// Forced recovery selects checkpoints only from the generation it recovers. A snapshot of a replaced
/// generation stays on disk with a higher revision than anything current, and is still never
/// selected.
#[tokio::test]
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
    runtime.install_state_identities(&DomainSchedule::new(
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
    runtime.install_state_identities(&DomainSchedule::new(domain.clone(), vec![node], Vec::new()));
    let current = guest_state_placement(&runtime, &domain, acme);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

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
