//! Layer: test harness.
//! Owns: resolved frame routing, assignment fencing and exact route retirement regressions.
//! May depend on: the production routing owner and replicated state carriers.
//! Must not know: transport implementation or control-plane transactions.

use super::*;

#[test]
fn materialized_relay_publication_retains_exact_routes_and_reclaims_ended_branches() {
    use crate::{
        runtime::materialized_state::ReplicatedMaterializedRelayState,
        runtime_schema::{RuntimeValue, test_runtime_row},
    };

    let routes = StateReplicationRouting::default();
    let slot = Arc::new(ArcSwapOption::from(Some(assigned(1))));
    let placements = [
        None,
        string_branch_key("tenant", "acme"),
        string_branch_key("tenant", "beta"),
    ]
    .map(|branch_key| RuntimeStatePlacement {
        kind: ModelKind::Relay,
        identifier: named("profiles"),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([1; 32]),
        },
        branch_key,
        ..placed(0, 0, 1)
    });
    let entity = placements[0].entity();
    routes.register_assignment(&entity, &slot);
    let published = routes
        .materialized(&entity)
        .assured("assignment publishes a retained relay index");
    for (index, placement) in placements.iter().enumerate() {
        let row = test_runtime_row([(
            "value".to_string(),
            RuntimeValue::I64(i64::try_from(index).assured("three placements")),
        )]);
        let state = Arc::new(ReplicatedMaterializedRelayState::new(
            placement.clone(),
            row.arrow_schema(),
        ));
        let mut originator = ReplicatedMaterializedRelayState::bind(
            &state,
            StateReplicationRoles::owned_by(None),
            None,
        )
        .originator
        .assured("local origination");
        originator
            .update_last_by_timestamp(&placement.branch_key, row)
            .assured("current assignment");
        routes.install(
            placement.clone(),
            slot.clone(),
            ReplicatedState::MaterializedRelay(state),
        );
    }
    assert_eq!(published.states().len(), 3);
    for (index, placement) in placements.iter().enumerate() {
        assert!(published.has_state_for(&placement.branch_key));
        assert_eq!(
            published
                .record(&placement.branch_key)
                .assured("exact branch record")
                .row
                .value_at(0)
                .assured("valid column"),
            Some(RuntimeValue::I64(
                i64::try_from(index).assured("three placements")
            ))
        );
    }
    let retiring = routes
        .resolve(&placements[1])
        .assured("installed exact route");
    let row = test_runtime_row([("value".to_string(), RuntimeValue::I64(7))]);
    let state = Arc::new(ReplicatedMaterializedRelayState::new(
        placements[1].clone(),
        row.arrow_schema(),
    ));
    let mut originator =
        ReplicatedMaterializedRelayState::bind(&state, StateReplicationRoles::owned_by(None), None)
            .originator
            .assured("local origination");
    originator
        .update_last_by_timestamp(&placements[1].branch_key, row)
        .assured("current assignment");
    routes.install(
        placements[1].clone(),
        slot.clone(),
        ReplicatedState::MaterializedRelay(state),
    );
    published.retire(&retiring);
    assert!(!retiring.is_current());
    assert_eq!(
        published
            .record(&placements[1].branch_key)
            .assured("replacement route remains installed")
            .row
            .value_at(0)
            .assured("valid column"),
        Some(RuntimeValue::I64(7))
    );
    routes.retire(&placements[1]);
    assert!(published.record(&placements[1].branch_key).is_none());
    assert_eq!(published.states().len(), 2);
    slot.store(Some(assigned(2)));
    assert!(published.record(&placements[2].branch_key).is_none());
    routes.purge_stale(&placements[0].domain);
    assert!(published.states().is_empty());
    assert!(!published.has_state_for(&None));
    assert!(published.current.load().is_empty());
    routes.retire_entity(&entity);
    routes.retire_domain(&placements[0].domain);
    routes.withdraw_entity(&entity);
    assert!(routes.materialized(&entity).is_none());
    routes.clear();
}

#[test]
fn a_materialized_branch_publication_owns_absence_beside_the_relay_snapshot() {
    let routes = StateReplicationRouting::default();
    let slot = Arc::new(ArcSwapOption::from(Some(assigned(1))));
    let acme = string_branch_key("tenant", "acme");
    let beta = string_branch_key("tenant", "beta");
    let root_placement = RuntimeStatePlacement {
        kind: ModelKind::Relay,
        identifier: named("profiles"),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([1; 32]),
        },
        branch_key: None,
        ..placed(0, 0, 1)
    };
    let row = |value| test_runtime_row([("value".to_string(), RuntimeValue::I64(value))]);
    let root = Arc::new(ReplicatedMaterializedRelayState::new(
        root_placement.clone(),
        row(7).arrow_schema(),
    ));
    let mut relay_owner =
        ReplicatedMaterializedRelayState::bind(&root, StateReplicationRoles::owned_by(None), None)
            .originator
            .assured("the relay originates");
    relay_owner
        .update_last_by_timestamp(&acme, row(7))
        .assured("current relay assignment");
    relay_owner
        .update_last_by_timestamp(&beta, row(8))
        .assured("current relay assignment");
    routes.install(
        root_placement.clone(),
        slot.clone(),
        ReplicatedState::MaterializedRelay(root),
    );
    let branch_placement = RuntimeStatePlacement {
        branch_key: acme.clone(),
        ..root_placement.clone()
    };
    let branch = Arc::new(ReplicatedMaterializedRelayState::new(
        branch_placement.clone(),
        row(9).arrow_schema(),
    ));
    let mut branch_owner = ReplicatedMaterializedRelayState::bind(
        &branch,
        StateReplicationRoles::owned_by(None),
        None,
    )
    .originator
    .assured("the concrete branch originates");
    routes.install(
        branch_placement,
        slot,
        ReplicatedState::MaterializedRelay(branch),
    );
    let published = routes
        .materialized(&root_placement.entity())
        .assured("the installed relay publishes its states");
    assert!(published.has_state_for(&acme));
    assert!(
        published.record(&acme).is_none(),
        "the concrete owner owns absence"
    );
    assert!(published.record(&beta).is_some());
    branch_owner
        .update_last_by_timestamp(&acme, row(9))
        .assured("current branch assignment");
    assert_eq!(
        published
            .record(&acme)
            .assured("current branch row")
            .row
            .value_at(0)
            .assured("valid field"),
        Some(RuntimeValue::I64(9))
    );
    branch_owner
        .remove_key(&acme)
        .assured("current branch assignment");
    assert!(published.record(&acme).is_none());
    assert!(relay_owner.read().record(&acme).is_some());
    assert!(published.record(&beta).is_some());
}

pub(super) fn placed(entity: usize, branch: usize, generation: u8) -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: domain("routes"),
        kind: ModelKind::Deduplicator,
        identifier: named::<ModelName>(&format!("dedup_{entity}")),
        branch_key: string_branch_key("tenant", &format!("tenant-{branch}")),
        state: RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([generation; 32]),
        },
    }
}

pub(super) fn assigned(generation: u8) -> StdArc<ScheduledStateAssignment> {
    StdArc::new(ScheduledStateAssignment {
        identity: ScheduledStateIdentity {
            schema_fingerprint: SchemaFingerprint::from_digest([generation; 32]),
            wasm_state_generations: None,
        },
        checkpoint_owners: Some(CheckpointOwners {
            primary: Some(named("node-1")),
            executors: BTreeSet::from([named("node-1")]),
            replicas: BTreeSet::from([named("node-2")]),
        }),
    })
}

pub(super) fn install(
    routes: &StateReplicationRouting,
    slot: &SharedStateAssignment,
    placement: &RuntimeStatePlacement,
) -> Arc<ReplicatedDeduplicatorState> {
    let state = Arc::new(
        ReplicatedDeduplicatorState::new(placement.clone(), None)
            .assured("an empty current deduplicator checkpoint initializes"),
    );
    routes.install(
        placement.clone(),
        slot.clone(),
        ReplicatedState::Deduplicator(state.clone()),
    );
    state
}

#[test]
fn retained_routes_end_before_replacement_and_never_attach_to_the_successor() {
    let routes = StateReplicationRouting::default();
    let placement = placed(0, 0, 1);
    let slot = Arc::new(ArcSwapOption::from(Some(assigned(1))));
    routes.register_assignment(&placement.entity(), &slot);
    let first = install(&routes, &slot, &placement);
    let retained = routes
        .resolve(&placement)
        .assured("installation publishes a route");
    let request = routes
        .assigned_request(&placement, &named("node-1"))
        .assured("the current owner admits a request through its selected handle");
    assert!(
        routes
            .assigned_request(&placement, &named("node-3"))
            .is_none()
    );
    let admitted = request
        .state
        .assured("an admitted request borrows the installed state");
    let successor = install(&routes, &slot, &placement);
    assert!(retained.state().is_none());
    assert!(!retained.is_current());
    let peer = named::<ClusterNodeName>("node-2");
    admitted.replication().record(&peer, 1);
    routes.acknowledge(&placement, &peer, 2);
    assert_eq!(
        first.replication().with_progress(|p| p.held(&peer)),
        Some(1)
    );
    assert_eq!(
        successor.replication().with_progress(|p| p.held(&peer)),
        Some(2)
    );
    let current = routes
        .resolve(&placement)
        .assured("the successor has its own route");
    assert!(current.offer(first.replication(), 3).is_none());
    assert!(current.offer(successor.replication(), 3).is_some());
    assert_eq!(
        current.owned_replicas(&named("node-1")),
        Some(BTreeSet::from([peer]))
    );
    assert_eq!(current.owned_replicas(&named("node-3")), None);
    routes.retire_entity(&placement.entity());
    assert!(current.state().is_none());
    assert!(current.offer(successor.replication(), 4).is_none());
    assert!(routes.assignment(&placement).is_some());
    let unheld = routes
        .assigned_request(&placement, &named("node-1"))
        .assured("the assigned placement can be served from storage");
    assert!(unheld.state.is_none());
    let third = install(&routes, &slot, &placement);
    routes.retire_domain(&placement.domain);
    routes.acknowledge(&placement, &named("node-2"), 3);
    assert_eq!(
        third
            .replication()
            .with_progress(|p| p.held(&named("node-2"))),
        None
    );
    assert!(routes.assignment(&placement).is_some());
    routes.withdraw_entity(&placement.entity());
    assert!(routes.assignment(&placement).is_none());
    assert!(
        routes
            .assigned_request(&placement, &named("node-1"))
            .is_none()
    );
}

#[test]
fn bolero_frame_sequences_preserve_assignments_and_exact_state_lifetimes() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            struct Retained {
                placement: RuntimeStatePlacement,
                route: Arc<StateReplicationRoute>,
                state: Arc<ReplicatedDeduplicatorState>,
                live: bool,
                held: Option<u64>,
            }
            let routes = StateReplicationRouting::default();
            let slots = [
                Arc::new(ArcSwapOption::empty()),
                Arc::new(ArcSwapOption::empty()),
            ];
            let mut generations: [Option<u8>; 2] = [None, None];
            let mut retained: Vec<Retained> = Vec::new();
            let peer = named::<ClusterNodeName>("node-2");
            for (step, byte) in bytes.iter().copied().enumerate() {
                let entity = usize::from((byte / 6) % 2);
                let branch = usize::from((byte / 12) % 2);
                let generation = (byte / 24) % 3 + 1;
                let placement = placed(entity, branch, generation);
                match byte % 6 {
                    0 => {
                        slots[entity].store(Some(assigned(generation)));
                        routes.register_assignment(&placement.entity(), &slots[entity]);
                        generations[entity] = Some(generation);
                        routes.purge_stale(&placement.domain);
                        for entry in &mut retained {
                            if entry.placement.entity() == placement.entity()
                                && entry.placement.state != placement.state
                            {
                                entry.live = false;
                            }
                        }
                    }
                    1 => {
                        if generations[entity] == Some(generation) {
                            for entry in &mut retained {
                                if entry.placement == placement {
                                    entry.live = false;
                                }
                            }
                            let state = install(&routes, &slots[entity], &placement);
                            let route = routes
                                .resolve(&placement)
                                .assured("installation publishes the route");
                            retained.push(Retained {
                                placement: placement.clone(),
                                route,
                                state,
                                live: true,
                                held: None,
                            });
                        }
                    }
                    2 => {
                        routes.retire(&placement);
                        for entry in &mut retained {
                            if entry.placement == placement {
                                entry.live = false;
                            }
                        }
                    }
                    3 => {
                        let lsm =
                            u64::try_from(step).assured("a bounded 64-step trace fits u64") + 1;
                        routes.acknowledge(&placement, &peer, lsm);
                        for entry in &mut retained {
                            if entry.live
                                && entry.placement == placement
                                && generations[entity] == Some(generation)
                            {
                                entry.held = Some(lsm);
                            }
                        }
                    }
                    4 => {
                        slots[entity].store(None);
                        routes.withdraw_entity(&placement.entity());
                        generations[entity] = None;
                        for entry in &mut retained {
                            if entry.placement.entity() == placement.entity() {
                                entry.live = false;
                            }
                        }
                    }
                    _ => {
                        routes.clear();
                        for entry in &mut retained {
                            entry.live = false;
                        }
                        for slot in &slots {
                            slot.store(None);
                        }
                        generations = [None, None];
                    }
                }
                for entry in &retained {
                    assert_eq!(entry.route.state().is_some(), entry.live);
                    assert_eq!(entry.route.is_current(), entry.live);
                    assert_eq!(
                        entry.state.replication().with_progress(|p| p.held(&peer)),
                        entry.held
                    );
                    let resolved = routes.resolve(&entry.placement);
                    if entry.live {
                        assert!(Arc::ptr_eq(
                            &resolved.assured("a live route remains published"),
                            &entry.route
                        ));
                    }
                }
            }
        });
}
