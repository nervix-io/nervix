//! Test harness, outside the product layer order.
//! Owns: regressions for the enclosing runtime state owner.
//! Depends on: that owner, typed vocabulary and the primitive boundary.
//! Must not know: product decisions beyond the behavior exercised by these tests.

use super::*;

#[test]
fn ownership_handoff_preparation_retains_exact_coordination_identity_across_reopen() {
    let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
    let domain = DomainName::parse("testing").expect("valid domain name");
    let identifier = ModelName::parse("moving_state").expect("valid model name");
    let coordinator = ClusterNodeName::parse("leader-a").expect("valid coordinator name");
    let source = ClusterNodeName::parse("node-1").expect("valid cluster node name");
    let destination = ClusterNodeName::parse("node-2").expect("valid cluster node name");
    let coordination = CoordinationIdentity::new(coordinator.clone(), 22, 1);
    let prior_incarnation = CoordinationIdentity::new(coordinator, 21, 1);
    let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, identifier.clone());
    let operation_id = "handoff-operation";
    let transition = RuntimeStateHandoffTransition {
        coordination: &coordination,
        operation_id,
        source: &source,
        destination: &destination,
        source_incarnation: ClusterNodeIncarnation::new(31),
        destination_incarnation: ClusterNodeIncarnation::new(32),
        entity: &entity,
        base_schedule_fingerprint: [4; 32],
        target_schedule_fingerprint: [5; 32],
    };

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should open");
        store
            .persist_handoff_preparation(&transition, &[])
            .expect("ownership handoff preparation should persist");
    }

    let db = Database::builder(dir.path())
        .open()
        .expect("database should reopen");
    let store = RuntimeStateStore::from_database(db, Executor::default())
        .expect("state store should reopen");
    let preparations = store
        .handoff_preparations()
        .expect("persisted handoff preparation should load");
    assert_eq!(preparations.len(), 1);
    assert_eq!(preparations[0].coordination, coordination);

    store
        .activate_handoff_preparation(&transition, &[])
        .expect("the exact preparation should activate");
    assert!(
        store
            .handoff_activation(
                &prior_incarnation,
                operation_id,
                &domain,
                ModelKind::Relay,
                &identifier,
            )
            .expect("a stale activation lookup should be readable")
            .is_none()
    );
    store
        .discard_handoff_preparation(
            &prior_incarnation,
            operation_id,
            &domain,
            ModelKind::Relay,
            &identifier,
        )
        .expect("stale cleanup should remain idempotent");
    assert!(
        store
            .handoff_activation(
                &coordination,
                operation_id,
                &domain,
                ModelKind::Relay,
                &identifier,
            )
            .expect("the exact activation should remain readable")
            .is_some()
    );
    store
        .discard_handoff_preparation(
            &coordination,
            operation_id,
            &domain,
            ModelKind::Relay,
            &identifier,
        )
        .expect("the exact coordination identity should discard its activation");
    assert!(
        store
            .handoff_activation(
                &coordination,
                operation_id,
                &domain,
                ModelKind::Relay,
                &identifier,
            )
            .expect("discarded activation lookup should be readable")
            .is_none()
    );
}

#[test]
fn forced_recovery_preparation_survives_reopen_and_activation_is_idempotent() {
    let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
    let domain = DomainName::parse("testing").expect("valid domain name");
    let identifier = ModelName::parse("moving_state").expect("valid model name");
    let source = ClusterNodeName::parse("node-1").expect("valid cluster node name");
    let destination = ClusterNodeName::parse("node-2").expect("valid cluster node name");
    let destination_incarnation = ClusterNodeIncarnation::new(42);
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Relay,
        identifier: identifier.clone(),
        branch_key: None,
    };
    let payload = crate::runtime::empty_sealed_container()
        .expect("an empty materialized generation should seal");
    let prepared = PersistedRuntimeStateEntry {
        lsm: 5,
        payload: payload.clone(),
    };
    let operation_id = "handoff-operation";
    let target_schedule_fingerprint = [9; 32];
    let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, identifier.clone());
    let transition = ForcedRuntimeStateRecoveryTransition {
        operation_id,
        source: &source,
        destination: &destination,
        destination_incarnation,
        entity: &entity,
        target_schedule_fingerprint,
    };

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should open");
        store
            .persist_latest_snapshot(&placement, 4, &payload)
            .expect("earlier state should persist");
        store
            .persist_forced_recovery_preparation(
                &transition,
                &[(placement.clone(), prepared.clone())],
            )
            .expect("forced recovery preparation should persist");
    }

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should reopen");
        let activated = store
            .activate_forced_recovery(
                &transition,
                ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                None,
            )
            .expect("persisted forced recovery should activate")
            .expect("the first activation should apply its checkpoint");
        assert_eq!(activated, vec![(placement.clone(), prepared)]);
        store
            .persist_latest_snapshot(&placement, 6, &payload)
            .expect("post-activation state should persist");
    }

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen again");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should reopen");
        let activated = store
            .activate_forced_recovery(
                &transition,
                ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                None,
            )
            .expect("repeated forced recovery activation should be readable");
        assert!(activated.is_none());
        let current = store
            .latest_snapshot(&placement)
            .expect("current state should load")
            .expect("post-activation state should remain");
        assert_eq!(current.lsm, 6);
    }
}

#[test]
fn forced_recovery_preserves_checkpoint_after_incarnation_change() {
    let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
    let domain = DomainName::parse("testing").expect("valid domain name");
    let identifier = ModelName::parse("moving_state").expect("valid model name");
    let source = ClusterNodeName::parse("node-1").expect("valid cluster node name");
    let destination = ClusterNodeName::parse("node-2").expect("valid cluster node name");
    let destination_incarnation = ClusterNodeIncarnation::new(42);
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Relay,
        identifier: identifier.clone(),
        branch_key: None,
    };
    let payload = crate::runtime::empty_sealed_container()
        .expect("an empty materialized generation should seal");
    let prepared = PersistedRuntimeStateEntry {
        lsm: 5,
        payload: payload.clone(),
    };
    let operation_id = "handoff-operation";
    let target_schedule_fingerprint = [9; 32];
    let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, identifier.clone());
    let transition = ForcedRuntimeStateRecoveryTransition {
        operation_id,
        source: &source,
        destination: &destination,
        destination_incarnation,
        entity: &entity,
        target_schedule_fingerprint,
    };

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should open");
        store
            .persist_latest_snapshot(&placement, 4, &payload)
            .expect("earlier state should persist");
        store
            .persist_forced_recovery_preparation(
                &transition,
                &[(placement.clone(), prepared.clone())],
            )
            .expect("forced recovery preparation should persist");
    }

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should reopen");
        let activated = store
            .activate_forced_recovery(
                &transition,
                ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                None,
            )
            .expect("persisted forced recovery should activate")
            .expect("the first activation should apply its checkpoint");
        assert_eq!(activated, vec![(placement.clone(), prepared)]);
        store
            .persist_latest_snapshot(&placement, 6, &payload)
            .expect("post-activation state should persist");
    }

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen again");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should reopen");
        let transition = ForcedRuntimeStateRecoveryTransition {
            destination_incarnation: ClusterNodeIncarnation::new(43),
            ..transition
        };
        store
            .activate_forced_recovery(
                &transition,
                ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                None,
            )
            .expect("repeated forced recovery activation should be readable");
        let current = store
            .latest_snapshot(&placement)
            .expect("current state should load")
            .expect("post-activation state should remain");
        assert_eq!(current.lsm, 6);
    }
}

#[test]
fn forced_recovery_preserves_checkpoint_after_fingerprint_change() {
    let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
    let domain = DomainName::parse("testing").expect("valid domain name");
    let identifier = ModelName::parse("moving_state").expect("valid model name");
    let source = ClusterNodeName::parse("node-1").expect("valid cluster node name");
    let destination = ClusterNodeName::parse("node-2").expect("valid cluster node name");
    let destination_incarnation = ClusterNodeIncarnation::new(42);
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Relay,
        identifier: identifier.clone(),
        branch_key: None,
    };
    let payload = crate::runtime::empty_sealed_container()
        .expect("an empty materialized generation should seal");
    let prepared = PersistedRuntimeStateEntry {
        lsm: 5,
        payload: payload.clone(),
    };
    let operation_id = "handoff-operation";
    let target_schedule_fingerprint = [9; 32];
    let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, identifier.clone());
    let transition = ForcedRuntimeStateRecoveryTransition {
        operation_id,
        source: &source,
        destination: &destination,
        destination_incarnation,
        entity: &entity,
        target_schedule_fingerprint,
    };

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should open");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should open");
        store
            .persist_latest_snapshot(&placement, 4, &payload)
            .expect("earlier state should persist");
        store
            .persist_forced_recovery_preparation(
                &transition,
                &[(placement.clone(), prepared.clone())],
            )
            .expect("forced recovery preparation should persist");
    }

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should reopen");
        let activated = store
            .activate_forced_recovery(
                &transition,
                ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                None,
            )
            .expect("persisted forced recovery should activate")
            .expect("the first activation should apply its checkpoint");
        assert_eq!(activated, vec![(placement.clone(), prepared)]);
        store
            .persist_latest_snapshot(&placement, 6, &payload)
            .expect("post-activation state should persist");
    }

    {
        let db = Database::builder(dir.path())
            .open()
            .expect("database should reopen again");
        let store = RuntimeStateStore::from_database(db, Executor::default())
            .expect("state store should reopen");
        let transition = ForcedRuntimeStateRecoveryTransition {
            target_schedule_fingerprint: [10; 32],
            ..transition
        };
        store
            .activate_forced_recovery(
                &transition,
                ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                None,
            )
            .expect("repeated forced recovery activation should be readable");
        let current = store
            .latest_snapshot(&placement)
            .expect("current state should load")
            .expect("post-activation state should remain");
        assert_eq!(current.lsm, 6);
    }
}

fn tenant_branch(tenant: &str) -> BranchKey {
    BranchKey::from_fields([(
        nervix_models::FieldName::parse("tenant").expect("valid field name"),
        crate::runtime_schema::RuntimeValue::String(tenant.to_string()),
    )])
    .expect("a tenant branch key is not empty")
}

fn generation(value: u64) -> WasmStateGeneration {
    WasmStateGeneration::try_from(value).expect("test generations are non-zero")
}

fn guest_schema() -> SchemaFingerprint {
    SchemaFingerprint::from_digest([4; 32])
}

fn wasm_guest_placement(tenant: &str, value: u64) -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: DomainName::parse("testing").expect("valid domain name"),
        state: RuntimeState::WasmProcessor {
            schema: guest_schema(),
            generation: generation(value),
        },
        kind: ModelKind::WasmProcessor,
        identifier: ModelName::parse("counting_guest").expect("valid model name"),
        branch_key: Some(tenant_branch(tenant)),
    }
}

pub(super) fn open_store(dir: &tempfile::TempDir) -> RuntimeStateStore {
    let db = Database::builder(dir.path())
        .open()
        .expect("database should open");
    RuntimeStateStore::from_database(db, Executor::default()).expect("state store should open")
}

fn current_identity(generations: WasmStateGenerations) -> HashMap<NodeRef, ScheduledStateIdentity> {
    HashMap::from_iter([(
        NodeRef::new(
            ModelKind::WasmProcessor,
            ModelName::parse("counting_guest").expect("valid model name"),
        ),
        ScheduledStateIdentity {
            schema_fingerprint: guest_schema(),
            wasm_state_generations: Some(generations),
        },
    )])
}

/// A snapshot saved in an earlier generation keeps its own key, so no revision it carries, however
/// high, can replace or stand in for the guest state of the generation that succeeded it.
#[test]
fn an_earlier_generation_never_addresses_the_current_guest_state() {
    let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
    let store = open_store(&dir);
    let earlier = wasm_guest_placement("acme", 1);
    let current = wasm_guest_placement("acme", 2);

    store
        .persist_latest_snapshot(&current, 1, b"current")
        .expect("current guest state should persist");
    store
        .persist_latest_snapshot(&earlier, 9, b"earlier")
        .expect("a late save of the earlier generation writes only its own key");

    let restored = store
        .latest_snapshot(&current)
        .expect("current guest state should load")
        .expect("current guest state should remain");
    assert_eq!(
        (restored.lsm, restored.payload.as_slice()),
        (1, b"current".as_slice())
    );
    let decoded =
        stored_placement(&current.as_storage_key()).expect("a current-shape WASM key decodes");
    assert_eq!(decoded.state, current.state);
    assert_eq!(decoded.branch, Some(tenant_branch("acme").fingerprint()));
}

/// Purging a domain against its committed identities removes exactly the guest state of the
/// generations a transition replaced: a concrete-branch transition leaves every other branch in
/// place, and a transition of every branch fences all of them at once, including branches that
/// exist only as persisted state. The purge survives a restart of the store.
#[test]
fn purging_removes_only_guest_state_of_superseded_generations() {
    let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
    let mut generations = WasmStateGenerations::first();
    {
        let store = open_store(&dir);
        for (tenant, value) in [("acme", 1), ("beta", 1)] {
            store
                .persist_latest_snapshot(&wasm_guest_placement(tenant, value), 3, b"state")
                .expect("first-generation guest state should persist");
        }
        generations.begin_branch(tenant_branch("acme").fingerprint());
        store
            .persist_latest_snapshot(&wasm_guest_placement("acme", 2), 1, b"reset")
            .expect("the transitioned branch saves in its new generation");
        store
            .purge_stale_state_identities(
                &DomainName::parse("testing").expect("valid domain name"),
                &current_identity(generations.clone()),
            )
            .expect("stale guest state should purge");
    }

    let store = open_store(&dir);
    let loaded = |tenant: &str, value: u64| {
        store
            .latest_snapshot(&wasm_guest_placement(tenant, value))
            .expect("guest state should load")
            .map(|snapshot| snapshot.lsm)
    };
    assert_eq!(loaded("acme", 1), None);
    assert_eq!(loaded("acme", 2), Some(1));
    assert_eq!(loaded("beta", 1), Some(3));

    let every_branch = generations.begin_every_branch();
    store
        .purge_stale_state_identities(
            &DomainName::parse("testing").expect("valid domain name"),
            &current_identity(generations),
        )
        .expect("stale guest state should purge");
    assert_eq!(every_branch, generation(3));
    assert_eq!(loaded("acme", 2), None);
    assert_eq!(loaded("beta", 1), None);
}

#[test]
fn persisted_runtime_state_decodes_from_unaligned_storage() {
    let expected = PersistedRuntimeStateEntry {
        lsm: 7,
        payload: vec![1, 2, 3],
    };
    let encoded =
        rkyv::to_bytes::<rkyv::rancor::Error>(&expected).expect("runtime state should encode");
    let mut unaligned = vec![0];
    unaligned.extend_from_slice(&encoded);

    let decoded = PersistedRuntimeStateEntry::decode(&unaligned[1..])
        .expect("runtime state should decode from an unaligned database buffer");

    assert_eq!(decoded, expected);
}

/// Guest checkpoints and replica installations go through the store's storage workers and
/// return only once a synchronization covered them. A replica installation hands back the
/// checkpoint it wrote, and refuses one that is not newer than the checkpoint already stored for
/// that generation.
#[nervix_primitives::test]
async fn guest_checkpoints_and_replica_installs_return_once_synchronized() {
    let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
    let store = open_store(&dir);
    let placement = wasm_guest_placement("acme", 1);
    let guest = super::super::ReplicatedWasmProcessorState::new(
        placement.clone(),
        None,
        Arc::new(nervix_primitives::publication::ArcSwapOption::empty()),
    );
    let captured = guest.capture(
        vec![1, 2, 3],
        super::super::WasmCheckpointBoundary::LocalStorage,
    );

    store
        .persist_wasm_checkpoint(&placement, captured.saved())
        .await
        .expect("the guest checkpoint should reach stable storage");
    assert_eq!(store.durability.rounds(), 1);
    let stored = store
        .latest_snapshot(&placement)
        .expect("guest state should load")
        .expect("the checkpointed guest state is stored");
    assert_eq!((stored.lsm, stored.payload), (1, vec![1, 2, 3]));

    let older = PersistedRuntimeStateEntry {
        lsm: 1,
        payload: vec![9],
    };
    assert_eq!(
        store
            .persist_replica_snapshot_if_newer(&placement, older)
            .await
            .expect("the replica installation should run"),
        None
    );
    let newer = PersistedRuntimeStateEntry {
        lsm: 5,
        payload: vec![7],
    };
    let installed = store
        .persist_replica_snapshot_if_newer(&placement, newer.clone())
        .await
        .expect("the replica installation should run");
    assert_eq!(installed, Some(newer));
    assert_eq!(
        store.durability.rounds(),
        2,
        "only the installation that wrote a checkpoint synchronizes"
    );
}

/// Activating a forced recovery publishes the checkpoints it staged in the generation the
/// committed schedule names, and replaying the same recovery publishes nothing again.
#[test]
fn forced_recovery_publishes_staged_guest_state_in_the_committed_generation() {
    let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
    let store = open_store(&dir);
    let staged = wasm_guest_placement("acme", 1);
    let snapshot = PersistedRuntimeStateEntry {
        lsm: 6,
        payload: vec![2],
    };
    let entity = DomainNodeRef::node_in(
        staged.domain.clone(),
        ModelKind::WasmProcessor,
        staged.identifier.clone(),
    );
    let source = ClusterNodeName::parse("node-1").expect("valid cluster node name");
    let destination = ClusterNodeName::parse("node-2").expect("valid cluster node name");
    let transition = ForcedRuntimeStateRecoveryTransition {
        operation_id: "owner-replacement",
        source: &source,
        destination: &destination,
        destination_incarnation: ClusterNodeIncarnation::new(7),
        entity: &entity,
        target_schedule_fingerprint: [8; 32],
    };
    store
        .persist_forced_recovery_preparation(&transition, &[(staged.clone(), snapshot.clone())])
        .expect("the recovery preparation should persist");
    let mut committed = WasmStateGenerations::first();
    committed.begin_every_branch();

    let activated = store
        .activate_forced_recovery(
            &transition,
            ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
            Some(&committed),
        )
        .expect("the recovery should activate")
        .expect("the first activation publishes its checkpoints");

    let published = wasm_guest_placement("acme", 2);
    assert_eq!(activated, vec![(published.clone(), snapshot)]);
    assert_eq!(
        store
            .latest_snapshot(&published)
            .expect("guest state should load")
            .map(|stored| stored.lsm),
        Some(6)
    );
    assert_eq!(
        store
            .latest_snapshot(&staged)
            .expect("guest state should load"),
        None
    );
    assert_eq!(
        store
            .activate_forced_recovery(
                &transition,
                ForcedRuntimeStateRecoveryAuthorization::PreparedCheckpoints,
                Some(&committed),
            )
            .expect("a replayed activation should be readable"),
        None
    );
}

/// A stored key is bytes read back from the database, so decoding one that ends inside the
/// state-kind prefix must report a decode error rather than index past the key.
#[test]
fn truncated_state_key_reports_a_decode_error() {
    let mut key = b"acme".to_vec();
    key.push(0);
    key.push(u8::from(RuntimeStateKind::Deduplicator));

    let error = stored_placement(&key)
        .err()
        .expect("a key that ends inside the state-kind prefix must not decode");

    assert!(
        matches!(error.current_context(), RuntimePersistenceError::DecodeState(message)
            if message.contains("model-kind separator")),
        "unexpected error for a truncated state key: {error:?}"
    );
}

fn orders_placement(state: RuntimeState, branch_key: Option<BranchKey>) -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: DomainName::parse("testing").expect("valid domain name"),
        state,
        kind: ModelKind::Deduplicator,
        identifier: ModelName::parse("orders").expect("valid model name"),
        branch_key,
    }
}

/// Every runtime state keeps its whole identity in its storage key: state that depends on no
/// schema is keyed by its kind alone, schema-bound state also by its schema fingerprint, and
/// WASM guest state also by its generation, for unbranched execution and a concrete branch.
#[test]
fn every_runtime_state_round_trips_through_its_storage_key() {
    let schema = SchemaFingerprint::from_digest([3; 32]);
    let states = [
        RuntimeState::BranchAggregated,
        RuntimeState::KafkaOffset,
        RuntimeState::Correlator { schema },
        RuntimeState::Deduplicator { schema },
        RuntimeState::MaterializedRelay { schema },
        RuntimeState::WasmProcessor {
            schema,
            generation: generation(2),
        },
        RuntimeState::WindowProcessor { schema },
        RuntimeState::BranchLru { schema },
    ];
    for state in states {
        for branch_key in [None, Some(tenant_branch("acme"))] {
            let placement = orders_placement(state, branch_key.clone());

            let decoded = stored_placement(&placement.as_storage_key())
                .expect("a current-shape runtime state key decodes");

            assert_eq!(decoded.state, state);
            assert_eq!(decoded.kind, placement.kind);
            assert_eq!(decoded.identifier, placement.identifier);
            assert_eq!(
                decoded.branch,
                branch_key.as_ref().map(BranchKey::fingerprint)
            );
        }
    }
}

/// A schema change starts a new lifetime for schema-bound state and leaves state that depends on
/// no schema where it is: purging against the new identity removes only the checkpoints written
/// under the replaced fingerprint, and everything that remains loads again after a restart.
#[test]
fn a_schema_change_replaces_only_schema_bound_state() {
    let dir = tempfile::tempdir().expect("temporary runtime state directory should open");
    let replaced = SchemaFingerprint::from_digest([1; 32]);
    let current = SchemaFingerprint::from_digest([2; 32]);
    let independent = [RuntimeState::BranchAggregated, RuntimeState::KafkaOffset];
    let replaced_branch = orders_placement(
        RuntimeState::Deduplicator { schema: replaced },
        Some(tenant_branch("acme")),
    );
    let current_branch = orders_placement(
        RuntimeState::Deduplicator { schema: current },
        Some(tenant_branch("beta")),
    );
    {
        let store = open_store(&dir);
        for state in independent {
            store
                .persist_latest_snapshot(&orders_placement(state, None), 3, b"kept")
                .expect("schema-independent state should persist");
        }
        store
            .persist_latest_snapshot(&replaced_branch, 4, b"replaced")
            .expect("state of the replaced schema should persist");
        store
            .persist_latest_snapshot(&current_branch, 5, b"current")
            .expect("state of the current schema should persist");
        store
            .purge_stale_state_identities(
                &current_branch.domain,
                &HashMap::from_iter([(
                    NodeRef::new(ModelKind::Deduplicator, current_branch.identifier.clone()),
                    ScheduledStateIdentity {
                        schema_fingerprint: current,
                        wasm_state_generations: None,
                    },
                )]),
            )
            .expect("state of the replaced schema should purge");
    }

    let store = open_store(&dir);
    let loaded = |placement: &RuntimeStatePlacement| {
        store
            .latest_snapshot(placement)
            .expect("runtime state should load")
    };
    for state in independent {
        assert_eq!(
            loaded(&orders_placement(state, None)),
            Some(PersistedRuntimeStateEntry {
                lsm: 3,
                payload: b"kept".to_vec(),
            })
        );
    }
    assert_eq!(loaded(&replaced_branch), None);
    assert_eq!(
        loaded(&current_branch),
        Some(PersistedRuntimeStateEntry {
            lsm: 5,
            payload: b"current".to_vec(),
        })
    );
}

/// Schema-bound state is addressed only through its whole fingerprint, so a key that ends
/// inside the fingerprint reports a decode error instead of naming some other state.
#[test]
fn a_key_that_ends_inside_its_schema_fingerprint_does_not_decode() {
    let placement = orders_placement(
        RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([3; 32]),
        },
        None,
    );
    let mut key = placement.as_storage_key();
    let inside_fingerprint = key
        .len()
        .checked_sub(20)
        .expect("the key is longer than the tail of its fingerprint");
    key.truncate(inside_fingerprint);

    let error = stored_placement(&key)
        .err()
        .expect("a key that ends inside its schema fingerprint must not decode");

    assert!(
        matches!(error.current_context(), RuntimePersistenceError::DecodeState(message)
            if message.contains("truncated schema fingerprint")),
        "unexpected error for a truncated schema fingerprint: {error:?}"
    );
}

/// An unbranched key ends with its scope, so bytes after it are refused rather than ignored.
#[test]
fn an_unbranched_key_that_continues_after_its_scope_does_not_decode() {
    let mut key = orders_placement(RuntimeState::KafkaOffset, None).as_storage_key();
    key.push(7);

    let error = stored_placement(&key)
        .err()
        .expect("an unbranched key with trailing bytes must not decode");

    assert!(
        matches!(error.current_context(), RuntimePersistenceError::DecodeState(message)
            if message.contains("continues after its unbranched scope")),
        "unexpected error for trailing key bytes: {error:?}"
    );
}

#[test]
fn an_operation_under_a_replaced_assignment_is_refused() {
    let authority = StateAssignmentAuthority::default();
    let replaced = authority
        .rebind(StateReplicationRoles::owned_by(None), None)
        .token_for(StateCapability::Originate)
        .assured("a state without roles is originated locally");
    let current = authority
        .rebind(StateReplicationRoles::owned_by(None), None)
        .token_for(StateCapability::Originate)
        .assured("a state without roles is originated locally");

    assert!(
        authority
            .authorize(replaced, StateCapability::Originate, || ())
            .is_err()
    );
    assert!(
        authority
            .authorize_exclusive(replaced, StateCapability::Originate, || ())
            .is_err()
    );
    assert!(
        authority
            .authorize(current, StateCapability::Originate, || ())
            .is_ok()
    );
    assert!(
        authority
            .authorize_exclusive(current, StateCapability::Originate, || ())
            .is_ok()
    );
    assert!(
        authority
            .authorize(current, StateCapability::InstallSnapshot, || ())
            .is_err()
    );
}
