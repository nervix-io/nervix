//! Native regression tests for runtime entity gates and quiescence.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Entity-gate ownership, drain accounting and lifecycle regression cases.
//! - **Depends on.** The production runtime gate owners and focused test fixtures.
//! - **Must not know.** Connector implementations or application transaction policy.

use std::time::Duration;

use nervix_interconnect::EntityGatePurpose;
use nervix_models::{
    AckMode, BranchSelection, ClusterNodeName, CoordinationIdentity, CreateEmitter, CreateJunction,
    CreateRelay, DomainSchedule, EmitterEntity, EmitterName, ErrorPolicies, IngestQuiesceMode,
    IngestorName, ModelKind, ModelName, NodeRef, ProcessorInputWhere, ProcessorInputs,
    ProcessorOutputs, RelayBranching, RelayName, RetryPolicy, ZeroMqEmitter, ZeroMqPushEmitter,
};
use nervix_primitives::{
    sync::{
        Arc, StdArc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        watch,
    },
    time::Instant,
};
use nonzero_ext::nonzero;

use super::*;
use crate::emitter_execution_plan::EmitterExecutionPlans;

fn coordination(coordinator: &str, process_epoch: u64, sequence: u64) -> CoordinationIdentity {
    CoordinationIdentity::new(named(coordinator), process_epoch, sequence)
}

#[nervix_primitives::test]
async fn releasing_an_ended_unit_leaves_its_prepared_replacement_and_sibling_installed() {
    let runtime = Runtime::default();
    let domain = domain("instances");
    let ingestor = named::<IngestorName>("source");
    let control = |unit| {
        Arc::new(IngestorQuiesceControl::new(
            IngestQuiesceMode::Suspend,
            runtime.inner.metrics.clone(),
            runtime.inner.metrics.register_ingestor_quiesce(
                &domain,
                &ingestor,
                unit,
                None,
                nervix_models::IngestorEntityKind::Http(
                    nervix_models::HttpIngestorEntity::Endpoint,
                ),
            ),
        ))
    };
    let ending = control(Unit::FIRST_INSTANCE);
    ending.engage(IngestorQuiesceCause::OwnershipHandoff);
    let replacement = control(Unit::FIRST_INSTANCE);
    let sibling =
        control(Unit::try_instance(2).assured("the fixture index fits the execution-unit range"));
    replacement.engage(IngestorQuiesceCause::EntityHold);
    let key = |unit| {
        DomainNodeUnitRef::new(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone()),
            unit,
        )
    };
    runtime
        .inner
        .ingestor_quiescence
        .insert(key(Unit::FIRST_INSTANCE), replacement.clone());
    runtime.inner.ingestor_quiescence.insert(
        key(Unit::try_instance(2).assured("the fixture index fits the execution-unit range")),
        sibling.clone(),
    );
    Runtime::release_entity_alter_hold(
        &runtime.inner.ingestors,
        &runtime.inner.ingestor_quiescence,
        &runtime.inner.frozen_ownership_handoff_entities,
        &domain,
        EntityAlterHold {
            coordination: coordination("coordinator", 1, 1),
            gates: EntityGateHold {
                gates: Vec::new(),
                branch_gates: Vec::new(),
            },
            targets: EntityGateTargets::Units(vec![NodeUnitRef::new(
                NodeRef::new(ModelKind::Ingestor, &ingestor),
                Unit::FIRST_INSTANCE,
            )]),
            purpose: EntityGatePurpose::OwnershipHandoff,
            quiesced_ingestors: vec![QuiescedIngestorHold {
                unit: Unit::FIRST_INSTANCE,
                ingestor: ingestor.clone(),
                cause: IngestorQuiesceCause::OwnershipHandoff,
                control: ending.clone(),
            }],
        },
    )
    .await;
    assert!(!ending.is_quiesced());
    assert!(replacement.is_quiesced());
    for (unit, expected) in [
        (Unit::FIRST_INSTANCE, &replacement),
        (
            Unit::try_instance(2).assured("the fixture index fits the execution-unit range"),
            &sibling,
        ),
    ] {
        let current = runtime
            .ingestor_quiesce_control(&domain, &ingestor, unit)
            .expect("each current unit keeps its own control");
        assert!(Arc::ptr_eq(&current, expected));
    }
}

#[test]
fn bolero_freeze_sequences_publish_exact_coordination_owners() {
    bolero::check!().with_type::<[u8; 64]>().for_each(|steps| {
        let state = Arc::new(OwnershipHandoffFreezeState::default());
        let watch = OwnershipHandoffFreezeWatch::over(state.clone());
        let mut expected = BTreeSet::new();
        for step in steps {
            let owner = coordination("coordinator", 1, u64::from((step / 2) % 8) + 1);
            if step % 2 == 0 {
                state.insert(owner.clone());
                expected.insert(owner);
            } else {
                state.remove(&owner);
                expected.remove(&owner);
            }
            assert_eq!(&**state.published.load(), &expected);
            assert_eq!(watch.observe().is_frozen(), !expected.is_empty());
            for sequence in 1..=8 {
                let owner = coordination("coordinator", 1, sequence);
                assert_eq!(state.contains(&owner), expected.contains(&owner));
            }
        }
    });
}

#[test]
fn domain_drain_status_reports_structured_emitter_publishing_state() {
    let runtime = Runtime::new();
    let domain = domain("default");
    let confirming = named::<EmitterName>("confirming");
    let retrying = named::<EmitterName>("retrying");
    let committing = named::<EmitterName>("held_for_commit");

    for (emitter, pending_messages) in [
        (&confirming, 3_usize),
        (&retrying, 2_usize),
        (&committing, 5_usize),
    ] {
        runtime.inner.emitter_buffers.insert(
            DomainNodeUnitRef::single(DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Emitter,
                emitter.clone(),
            )),
            Arc::new(AtomicUsize::new(pending_messages)),
        );
    }

    let confirmation = runtime.begin_emitter_confirmation_wait(&domain, &confirming);
    runtime.record_emitter_transient_error_with_backoff(
        &domain,
        &retrying,
        "sensitive infrastructure detail that drain status must not expose",
        Duration::from_secs(2),
    );
    runtime.record_commit_failure_with_backoff(
        &domain,
        &committing,
        "sensitive catalog detail that drain status must not expose",
        Duration::from_secs(3),
    );

    let status = runtime.domain_drain_status(&domain);

    assert_eq!(status.emitter_publishing.len(), 3);
    assert_eq!(
        status.emitter_publishing[0],
        EmitterPublishingDrainStatus {
            emitter: confirming,
            unit: Unit::SINGLE,
            state: EmitterPublishingDrainState::AwaitingConfirmation,
            pending_messages: 3,
            retry_backoff: None,
            retry_wait: None,
        }
    );
    assert_eq!(status.emitter_publishing[1].emitter, committing);
    assert_eq!(
        status.emitter_publishing[1].state,
        EmitterPublishingDrainState::RetryingCommit
    );
    assert_eq!(
        status.emitter_publishing[1].retry_backoff,
        Some(Duration::from_secs(3))
    );
    assert!(
        status.emitter_publishing[1]
            .retry_wait
            .is_some_and(|wait| wait <= Duration::from_secs(3))
    );
    assert_eq!(status.emitter_publishing[2].emitter, retrying);
    assert_eq!(
        status.emitter_publishing[2].state,
        EmitterPublishingDrainState::RetryingInfrastructure
    );
    assert_eq!(
        status.emitter_publishing[2].retry_backoff,
        Some(Duration::from_secs(2))
    );
    let affected_emitters = [
        status.emitter_publishing[0].emitter.clone(),
        status.emitter_publishing[1].emitter.clone(),
        status.emitter_publishing[2].emitter.clone(),
    ]
    .into_iter()
    .map(|identifier| NodeRef {
        kind: ModelKind::Emitter,
        identifier: ModelName::from(&identifier),
    })
    .collect::<Vec<_>>();
    let entity_status = runtime
        .entity_drain_status(
            &domain,
            &[],
            &nervix_models::EntityGateTargets::Entities(affected_emitters.to_vec()),
            EntityGatePurpose::ModelAlteration,
        )
        .emitter_publishing;
    assert_eq!(entity_status.len(), status.emitter_publishing.len());
    for (entity, domain) in entity_status.iter().zip(&status.emitter_publishing) {
        assert_eq!(entity.emitter, domain.emitter);
        assert_eq!(entity.state, domain.state);
        assert_eq!(entity.pending_messages, domain.pending_messages);
        assert_eq!(entity.retry_backoff, domain.retry_backoff);
        assert_eq!(entity.retry_wait.is_some(), domain.retry_wait.is_some());
    }

    drop(confirmation);
    assert!(
        runtime
            .domain_drain_status(&domain)
            .emitter_publishing
            .iter()
            .all(|status| status.emitter != named("confirming")),
        "a completed confirmation must disappear from drain status"
    );
}

#[nervix_primitives::test]
async fn entity_gate_hold_quiesces_an_ingestor_without_stopping_it() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("events");
    let ingestor = named::<IngestorName>("events_source");
    let coordination = coordination("coordinator-a", 7, 41);

    let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let gate = fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
        fanout,
    );

    let key = DomainNodeUnitRef::new(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone()),
        Unit::SINGLE,
    );
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let stopped = StdArc::new(AtomicBool::new(false));
    let task_stopped = stopped.clone();
    let task = nervix_primitives::task::spawn(async move {
        let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
        task_stopped.store(true, Ordering::SeqCst);
    });
    runtime.inner.ingestors.insert(
        key.clone(),
        IngestorRuntime {
            shutdown: shutdown_tx,
            branched: Vec::new(),
            tasks: vec![task],
            domain_offsets: None,
        },
    );
    runtime.inner.ingestor_quiescence.insert(
        key.clone(),
        test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Suspend),
    );

    let affected = NodeRef {
        kind: ModelKind::Ingestor,
        identifier: ModelName::from(&ingestor.clone()),
    };
    runtime
        .engage_entity_gate_operation(
            &coordination,
            &domain,
            std::slice::from_ref(&relay),
            &nervix_models::EntityGateTargets::Entities((std::slice::from_ref(&affected)).to_vec()),
            EntityGatePurpose::ModelAlteration,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(5),
                reason: "quiesce regression",
            },
        )
        .await
        .expect("entity hold should engage");

    assert!(runtime.inner.ingestors.get(&key).is_some());
    assert!(!stopped.load(Ordering::SeqCst));
    assert!(gate.is_closed());
    assert!(runtime.entity_gate_operation_is_held(&coordination));
    assert_eq!(
        runtime
            .inner
            .ingestor_quiescence
            .get(&key)
            .and_then(|control| control.cause()),
        Some(IngestorQuiesceCause::EntityHold)
    );

    runtime
        .release_entity_gate_operation(&coordination, &domain)
        .await
        .expect("entity hold should release");
    assert!(!runtime.entity_gate_operation_is_held(&coordination));
    assert!(!gate.is_closed());
    assert!(runtime.inner.ingestors.get(&key).is_some());
    assert!(!stopped.load(Ordering::SeqCst));
    assert_eq!(
        runtime
            .inner
            .ingestor_quiescence
            .get(&key)
            .and_then(|control| control.cause()),
        None
    );

    runtime
        .stop_ingestor(&domain, &ingestor)
        .await
        .expect("test ingestor should stop");
}

#[nervix_primitives::test]
async fn entity_gate_operation_releases_when_its_lease_deadline_expires() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("events");
    let coordination = coordination("coordinator-a", 7, 42);
    let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let gate = fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
        fanout,
    );

    runtime
        .engage_entity_gate_operation(
            &coordination,
            &domain,
            std::slice::from_ref(&relay),
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::OwnershipHandoff,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_millis(25),
                reason: "deadline regression",
            },
        )
        .await
        .expect("entity hold should engage");
    assert!(gate.is_closed());

    nervix_primitives::time::timeout(Duration::from_secs(1), async {
        while runtime.entity_gate_operation_is_held(&coordination) {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("entity hold should release at its deadline");
    assert!(!gate.is_closed());
}

/// A lease that a handoff left held, with a deadline past the node's stop, ends with the
/// runtime: teardown releases its hold, so nothing of it is left to retain the ingestor control
/// it quiesced, or what that control holds, after the runtime stopped.
#[nervix_primitives::test]
async fn runtime_shutdown_ends_an_entity_gate_lease_held_past_its_stop() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("events");
    let ingestor = named::<IngestorName>("events_source");
    let coordination = coordination("coordinator-a", 7, 43);

    let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let gate = fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
        fanout,
    );
    let key = DomainNodeUnitRef::new(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone()),
        Unit::SINGLE,
    );
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let task = nervix_primitives::task::spawn(async move {
        let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
    });
    runtime.inner.ingestors.insert(
        key.clone(),
        IngestorRuntime {
            shutdown: shutdown_tx,
            branched: Vec::new(),
            tasks: vec![task],
            domain_offsets: None,
        },
    );
    runtime.inner.ingestor_quiescence.insert(
        key.clone(),
        test_ingestor_quiesce_control(&runtime, &domain, &ingestor, IngestQuiesceMode::Suspend),
    );
    let affected = NodeRef {
        kind: ModelKind::Ingestor,
        identifier: ModelName::from(&ingestor.clone()),
    };
    runtime
        .engage_entity_gate_operation(
            &coordination,
            &domain,
            std::slice::from_ref(&relay),
            &nervix_models::EntityGateTargets::Entities((std::slice::from_ref(&affected)).to_vec()),
            EntityGatePurpose::OwnershipHandoff,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(3600),
                reason: "a lease past the node's stop",
            },
        )
        .await
        .expect("the handoff gate should engage");
    let quiesced = runtime
        .inner
        .ingestor_quiescence
        .get(&key)
        .map(|control| control.value().clone())
        .expect("the engaged ingestor keeps its quiesce control");
    assert!(runtime.entity_gate_operation_is_held(&coordination));
    assert!(gate.is_closed());
    assert_eq!(
        quiesced.cause(),
        Some(IngestorQuiesceCause::OwnershipHandoff)
    );

    runtime.shutdown(runtime.domain_drain_timeout()).await;

    assert!(
        !runtime.entity_gate_operation_is_held(&coordination),
        "runtime teardown must end the lease it holds"
    );
    assert!(
        !gate.is_closed(),
        "the ended lease reopens the relay it fenced"
    );
    assert_eq!(
        quiesced.cause(),
        None,
        "the ended lease releases the ingestor it quiesced"
    );

    runtime
        .stop_ingestor(&domain, &ingestor)
        .await
        .expect("test ingestor should stop");
}

/// An engagement still waiting for its relays when the runtime stops ends with the stop: its
/// waiter learns why, the relay it fenced reopens, and nothing is left registered.
#[nervix_primitives::test]
async fn runtime_shutdown_ends_an_engagement_still_waiting_for_its_relays() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("events");
    let coordination = coordination("leader-a", 12, 1);
    let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let gate = fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
        fanout,
    );
    let dispatch = gate.acquire_dispatch().await;
    let request = nervix_primitives::task::spawn({
        let runtime = runtime.clone();
        let domain = domain.clone();
        let relay = relay.clone();
        let coordination = coordination.clone();
        async move {
            runtime
                .engage_entity_gate_operation(
                    &coordination,
                    &domain,
                    &[relay],
                    &nervix_models::EntityGateTargets::Entities(Vec::new()),
                    EntityGatePurpose::ModelAlteration,
                    EntityGateLease {
                        deadline: Instant::now() + Duration::from_secs(3600),
                        reason: "an engagement the stop interrupts",
                    },
                )
                .await
        }
    });
    while !gate.is_closed() {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::task::yield_now().await;
    }

    runtime.shutdown(runtime.domain_drain_timeout()).await;

    let outcome = request
        .await
        .expect("the engagement request must not panic");
    let Err(failure) = outcome else {
        panic!("an engagement the stop interrupted is never held");
    };
    assert!(matches!(
        failure.current_context(),
        EntityGateOperationError::RuntimeStopped { domain: stopped } if stopped == &domain
    ));
    assert!(
        !gate.is_closed(),
        "the interrupted engagement reopens the relay it fenced"
    );
    assert!(runtime.inner.entity_gate_holds.get(&coordination).is_none());
    drop(dispatch);
}

/// A runtime that has stopped engages no new gate, and a peer that asked learns that the
/// operation failed rather than that it addressed a hold this node never had.
#[nervix_primitives::test]
async fn a_stopped_runtime_refuses_a_new_entity_gate_engagement() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let coordination = coordination("leader-a", 13, 1);
    runtime.shutdown(runtime.domain_drain_timeout()).await;

    let Err(refusal) = runtime
        .engage_entity_gate_operation(
            &coordination,
            &domain,
            &[],
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::OwnershipHandoff,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(5),
                reason: "an engagement after the stop",
            },
        )
        .await
    else {
        panic!("a stopped runtime must refuse the engagement");
    };
    assert!(matches!(
        refusal.current_context(),
        EntityGateOperationError::RuntimeStopped { domain: stopped } if stopped == &domain
    ));
    assert!(matches!(
        refusal.current_context().as_remote_failure(
            nervix_interconnect::RemoteOperationSubject::Domain {
                domain: domain.clone(),
            }
        ),
        nervix_interconnect::RemoteOperationFailure::Failed { .. }
    ));
    assert!(runtime.inner.entity_gate_holds.get(&coordination).is_none());
}

#[nervix_primitives::test]
async fn equal_operation_ids_from_different_coordinators_fence_each_requested_relay() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let first_relay = named::<RelayName>("first_events");
    let second_relay = named::<RelayName>("second_events");
    let first_coordination = coordination("leader-a", 10, 1);
    let second_coordination = coordination("leader-b", 20, 1);
    let first_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let first_gate = first_fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, first_relay.clone()),
        first_fanout,
    );
    let second_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let second_gate = second_fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, second_relay.clone()),
        second_fanout,
    );

    runtime
        .engage_entity_gate_operation(
            &first_coordination,
            &domain,
            std::slice::from_ref(&first_relay),
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::ModelAlteration,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(5),
                reason: "first coordinator",
            },
        )
        .await
        .expect("the first coordinator hold should engage");
    runtime
        .engage_entity_gate_operation(
            &second_coordination,
            &domain,
            std::slice::from_ref(&second_relay),
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::ModelAlteration,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(5),
                reason: "replacement coordinator",
            },
        )
        .await
        .expect("a successful second coordinator hold should engage its complete scope");

    assert!(first_gate.is_closed());
    assert!(second_gate.is_closed());
}

#[nervix_primitives::test]
async fn concurrent_coordinators_with_equal_sequences_hold_independent_scopes() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let first_relay = named::<RelayName>("first_events");
    let second_relay = named::<RelayName>("second_events");
    let first_coordination = coordination("leader-a", 10, 1);
    let second_coordination = coordination("leader-b", 20, 1);
    let first_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let first_gate = first_fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, first_relay.clone()),
        first_fanout,
    );
    let second_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let second_gate = second_fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, second_relay.clone()),
        second_fanout,
    );

    let targets = EntityGateTargets::default();
    let first = runtime.engage_entity_gate_operation(
        &first_coordination,
        &domain,
        std::slice::from_ref(&first_relay),
        &targets,
        EntityGatePurpose::ModelAlteration,
        EntityGateLease {
            deadline: Instant::now() + Duration::from_secs(5),
            reason: "first concurrent coordinator",
        },
    );
    let second = runtime.engage_entity_gate_operation(
        &second_coordination,
        &domain,
        std::slice::from_ref(&second_relay),
        &targets,
        EntityGatePurpose::ModelAlteration,
        EntityGateLease {
            deadline: Instant::now() + Duration::from_secs(5),
            reason: "second concurrent coordinator",
        },
    );
    let (first, second) = tokio::join!(first, second);

    first.expect("the first concurrent hold should engage");
    second.expect("the second concurrent hold should engage");
    assert!(first_gate.is_closed());
    assert!(second_gate.is_closed());
}

#[nervix_primitives::test]
async fn receiver_finishes_engagement_after_the_coordinator_request_is_cancelled() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("events");
    let coordination = coordination("leader-a", 10, 1);
    let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let gate = fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
        fanout,
    );
    let dispatch = gate.acquire_dispatch().await;
    let request = nervix_primitives::task::spawn({
        let runtime = runtime.clone();
        let domain = domain.clone();
        let relay = relay.clone();
        let coordination = coordination.clone();
        async move {
            runtime
                .engage_entity_gate_operation(
                    &coordination,
                    &domain,
                    &[relay],
                    &nervix_models::EntityGateTargets::Entities(Vec::new()),
                    EntityGatePurpose::ModelAlteration,
                    EntityGateLease {
                        deadline: Instant::now() + Duration::from_secs(5),
                        reason: "coordinator request cancellation regression",
                    },
                )
                .await
        }
    });
    while !gate.is_closed() {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::task::yield_now().await;
    }

    request.abort();
    assert!(
        request
            .await
            .expect_err("the simulated coordinator request should be cancelled")
            .is_cancelled()
    );
    drop(dispatch);

    nervix_primitives::time::timeout(
        Duration::from_secs(1),
        runtime.engage_entity_gate_operation(
            &coordination,
            &domain,
            std::slice::from_ref(&relay),
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::ModelAlteration,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(5),
                reason: "same operation retry after coordinator loss",
            },
        ),
    )
    .await
    .expect("the receiver-owned engagement should finish after request cancellation")
    .expect("the same operation retry should observe the completed hold");
    assert!(runtime.entity_gate_operation_is_held(&coordination));
    assert!(gate.is_closed());
}

#[nervix_primitives::test]
async fn retries_require_the_same_scope_and_stale_operations_cannot_observe_or_release_it() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let held_relay = named::<RelayName>("held_events");
    let conflicting_relay = named::<RelayName>("conflicting_events");
    let current = coordination("leader-a", 11, 1);
    let prior_incarnation = coordination("leader-a", 10, 1);
    let held_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let held_gate = held_fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, held_relay.clone()),
        held_fanout,
    );
    let conflicting_fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let conflicting_gate = conflicting_fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, conflicting_relay.clone()),
        conflicting_fanout,
    );
    let lease = || EntityGateLease {
        deadline: Instant::now() + Duration::from_secs(5),
        reason: "scope identity regression",
    };

    runtime
        .engage_entity_gate_operation(
            &current,
            &domain,
            std::slice::from_ref(&held_relay),
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::ModelAlteration,
            lease(),
        )
        .await
        .expect("the current operation should engage");
    runtime
        .engage_entity_gate_operation(
            &current,
            &domain,
            &[held_relay.clone(), held_relay.clone()],
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::ModelAlteration,
            lease(),
        )
        .await
        .expect("a retry with the same canonical scope should succeed");

    let conflict = runtime
        .engage_entity_gate_operation(
            &current,
            &domain,
            std::slice::from_ref(&conflicting_relay),
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::ModelAlteration,
            lease(),
        )
        .await;
    assert!(conflict.is_err());
    assert!(!conflicting_gate.is_closed());
    runtime
        .entity_gate_operation_drain_status(
            &current,
            &domain,
            std::slice::from_ref(&held_relay),
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::ModelAlteration,
        )
        .expect("the exact operation and scope should report status");
    assert!(
        runtime
            .entity_gate_operation_drain_status(
                &prior_incarnation,
                &domain,
                std::slice::from_ref(&held_relay),
                &nervix_models::EntityGateTargets::Entities(Vec::new()),
                EntityGatePurpose::ModelAlteration,
            )
            .is_err()
    );

    runtime
        .release_entity_gate_operation(&prior_incarnation, &domain)
        .await
        .expect("a stale release should be idempotent");
    assert!(runtime.entity_gate_operation_is_held(&current));
    assert!(held_gate.is_closed());
    runtime
        .release_entity_gate_operation(&current, &domain)
        .await
        .expect("the exact operation should release");
    assert!(!held_gate.is_closed());
}

#[nervix_primitives::test]
async fn releasing_one_coordinator_preserves_an_overlapping_ownership_freeze() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let affected = NodeRef {
        kind: ModelKind::Deduplicator,
        identifier: named("deduplicate_events"),
    };
    let entity = affected.clone().in_domain(&domain);
    let targets = EntityGateTargets::Units(vec![NodeUnitRef::new(affected, Unit::SINGLE)]);
    let first = coordination("leader-a", 10, 1);
    let second = coordination("leader-b", 20, 1);
    let lease = || EntityGateLease {
        deadline: Instant::now() + Duration::from_secs(5),
        reason: "overlapping ownership freeze regression",
    };

    runtime
        .engage_entity_gate_operation(
            &first,
            &domain,
            &[],
            &targets,
            EntityGatePurpose::OwnershipHandoff,
            lease(),
        )
        .await
        .expect("the first ownership hold should engage");
    runtime
        .engage_entity_gate_operation(
            &second,
            &domain,
            &[],
            &targets,
            EntityGatePurpose::OwnershipHandoff,
            lease(),
        )
        .await
        .expect("the replacement coordinator ownership hold should engage");

    for coordination in [&first, &second] {
        assert!(
            runtime
                .entity_gate_operation_drain_status(
                    coordination,
                    &domain,
                    &[],
                    &targets,
                    EntityGatePurpose::OwnershipHandoff,
                )
                .expect("each exact hold reports its drain and installs its freeze")
                .is_drained()
        );
    }

    runtime
        .release_entity_gate_operation(&first, &domain)
        .await
        .expect("the first ownership hold should release");
    assert!(!runtime.ownership_handoff_entity_is_frozen_by(&entity, &first));
    assert!(runtime.ownership_handoff_entity_is_frozen_by(&entity, &second));
    assert!(runtime.ownership_handoff_entity_is_frozen(&entity));

    runtime
        .release_entity_gate_operation(&second, &domain)
        .await
        .expect("the replacement ownership hold should release");
    assert!(!runtime.ownership_handoff_entity_is_frozen(&entity));
}

#[nervix_primitives::test(start_paused = true)]
async fn preceding_lease_expiry_does_not_release_a_reengaged_hold() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("events");
    let coordination = coordination("leader-a", 10, 1);
    let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let gate = fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
        fanout,
    );

    runtime
        .engage_entity_gate_operation(
            &coordination,
            &domain,
            std::slice::from_ref(&relay),
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::ModelAlteration,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_millis(10),
                reason: "preceding lease",
            },
        )
        .await
        .expect("the preceding hold should engage");
    runtime
        .release_entity_gate_operation(&coordination, &domain)
        .await
        .expect("the preceding hold should release");
    runtime
        .engage_entity_gate_operation(
            &coordination,
            &domain,
            std::slice::from_ref(&relay),
            &nervix_models::EntityGateTargets::Entities(Vec::new()),
            EntityGatePurpose::ModelAlteration,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(1),
                reason: "replacement lease",
            },
        )
        .await
        .expect("the replacement hold should engage");

    nervix_primitives::time::advance(Duration::from_millis(11)).await;
    nervix_primitives::task::yield_now().await;

    assert!(runtime.entity_gate_operation_is_held(&coordination));
    assert!(gate.is_closed());
}

#[test]
fn emitter_entity_pause_gates_every_input_relay() {
    let emitter = CreateEmitter {
        instances: nervix_models::EmitterInstances::ONE,
        name: named("combined_emitter"),
        from: ProcessorInputs::new(
            vec![named("source_b"), named("source_a")],
            vec![ProcessorInputWhere {
                relay: named("source_b"),
                where_clause: nervix_plexus::parse_expression("input.seq > 0")
                    .expect("the source predicate is valid"),
            }],
        ),
        body: nervix_models::EmitterBody::Codec {
            codec: named("event_codec"),
        },
        envelope: Box::new(nervix_models::EmitterEnvelope::default()),
        connector: Box::new(EmitterEntity::ZeroMq(ZeroMqEmitter::Push(
            ZeroMqPushEmitter {
                mode: nervix_models::ZeroMqPushPublishingMode::NoAck {
                    retry_policy: RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: named("emitter"),
            },
        ))),
        batch: None,
        flush_policy: FlushPolicy::Immediate,
        error_policies: ErrorPolicies::handled_by_log(),
        mode: AckMode::Attached,
        construction: nervix_models::RouteConstruction::default(),
        materialized_state: Vec::new(),
    };
    let input_relay = |name: &str| {
        scheduled_model(nervix_models::Model::Relay(CreateRelay {
            name: named(name),
            schema: named("event"),
            buffer: nonzero!(2usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        }))
    };
    let mut schedule = DomainSchedule::new(
        domain("testing"),
        vec![
            scheduled_model(nervix_models::Model::Schema(nervix_models::CreateSchema {
                name: named("event"),
                fields: vec![nervix_models::SchemaField {
                    name: named("seq"),
                    ty: nervix_models::ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            scheduled_model(nervix_models::Model::Codec(nervix_models::CreateCodec {
                name: named("event_codec"),
                wire_format: nervix_models::CodecWireFormat::Syslog,
                schema: named("event"),
                encoding_rules: Vec::new(),
            })),
            scheduled_model(nervix_models::Model::ClientZeroMq(
                nervix_models::CreateClientZeroMq {
                    name: named("emitter"),
                    mount: None,
                    config: Vec::new(),
                },
            )),
            input_relay("source_a"),
            input_relay("source_b"),
            scheduled_model(nervix_models::Model::Emitter(emitter.clone())),
        ],
        Vec::new(),
    );
    let emitter_node = schedule
        .nodes
        .values_mut()
        .find(|node| node.kind() == ModelKind::Emitter)
        .expect("test schedule must contain its emitter");
    emitter_node.single_unit_mut().primary_node =
        Some(ClusterNodeName::parse("node-2").expect("valid name"));
    emitter_node.single_unit_mut().assigned_nodes =
        vec![ClusterNodeName::parse("node-2").expect("valid name")];
    let entity = NodeRef {
        kind: ModelKind::Emitter,
        identifier: ModelName::from(&emitter.name),
    };

    assert_eq!(
        crate::registry::entity_pause_relays_for_schedule(&schedule, &[entity]),
        vec![named("source_a"), named("source_b")]
    );
    let activation =
        DomainActivationPlan::from_scheduled_nodes(&domain("testing"), &schedule.nodes)
            .expect("the test schedule has planned schemas and relays");
    let emitter_plans = EmitterExecutionPlans::from_scheduled_nodes(&schedule.nodes, &activation)
        .expect("the test emitter has a matching client and inputs");
    let planned = emitter_plans
        .emitter(&emitter.name)
        .expect("the scheduled emitter has one plan");
    assert_eq!(
        planned
            .inputs
            .iter()
            .map(|input| input.relay.as_str())
            .collect::<Vec<_>>(),
        ["source_b", "source_a"]
    );
    assert!(planned.inputs[0].from_where.is_some());
    assert!(planned.inputs[1].from_where.is_none());
    let revision = ExecutionRevision::from_schedule(&schedule)
        .assured("the emitter fixture produces a complete revision");
    let remote_consumers = Runtime::remote_runtime_consumers_for_revision(
        &revision,
        &ClusterNodeName::parse("node-1").expect("valid name"),
    );
    assert_eq!(remote_consumers.len(), 2);
    for relay in [named("source_a"), named("source_b")] {
        let consumers = remote_consumers
            .get(&relay)
            .expect("every emitter input needs a remote consumer");
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].relay, relay);
        assert_eq!(
            consumers[0].node_id,
            ClusterNodeName::parse("node-2").expect("valid name")
        );
    }
}

#[test]
fn ownership_handoff_keeps_internal_moved_group_relays_open() {
    let junction = |name: &str, input: &str, output: &str| {
        scheduled_model(nervix_models::Model::Junction(CreateJunction {
            name: named(name),
            from: ProcessorInputs::single(named(input)),
            output_routes: (ProcessorOutputs::single(named(output)))
                .with_flush_policy(FlushPolicy::Immediate),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        }))
    };
    let schedule = DomainSchedule::new(
        domain("testing"),
        vec![
            junction("corridor_source", "inbound", "corridor_stage"),
            junction("corridor_emitter", "corridor_stage", "outbound"),
        ],
        Vec::new(),
    );
    let affected = ["corridor_source", "corridor_emitter"].map(|name| NodeRef {
        kind: ModelKind::Junction,
        identifier: named(name),
    });

    assert_eq!(
        crate::registry::entity_pause_relays_for_schedule(&schedule, &affected),
        vec![named("corridor_stage"), named("inbound")]
    );
    assert_eq!(
        crate::registry::ownership_handoff_relays_for_schedule(&schedule, &affected),
        vec![named("inbound")]
    );
}

#[test]
fn quiesce_counters_belong_to_one_node_not_to_a_shared_identifier() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let shared = named::<ModelName>("orders");
    let relay = runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Relay, &shared));
    let emitter = runtime.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Emitter, &shared));

    let _work = NodeQuiesceWorkGuard::begin(relay.clone());

    assert_eq!(relay.outstanding_work(), 1);
    assert_eq!(emitter.outstanding_work(), 0);
}
#[nervix_primitives::test(start_paused = true)]
async fn ownership_handoff_expiry_reopens_gates_after_an_unfinished_local_flush() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("events");
    let entity = NodeRef::new(ModelKind::Ingestor, named::<IngestorName>("source"));
    let coordination = coordination("coordinator-a", 7, 42);
    let counters = runtime.node_quiesce_counters(&domain, entity.clone());
    let participant = runtime.force_flush_participant(&domain, counters.clone());
    let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(2));
    let dispatch_gate = fanout.dispatch_gate();
    runtime.inner.relay_boundary_fanouts.insert(
        DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay.clone()),
        fanout,
    );
    runtime
        .engage_entity_gate_operation(
            &coordination,
            &domain,
            std::slice::from_ref(&relay),
            &nervix_models::EntityGateTargets::Entities((std::slice::from_ref(&entity)).to_vec()),
            EntityGatePurpose::OwnershipHandoff,
            EntityGateLease {
                deadline: Instant::now() + Duration::from_secs(5),
                reason: "unfinished local flush",
            },
        )
        .await
        .expect("closed admission engages while its admitted work remains observable");
    assert!(dispatch_gate.is_closed());
    assert!(runtime.entity_gate_operation_is_held(&coordination));
    let status = runtime
        .entity_gate_operation_drain_status(
            &coordination,
            &domain,
            std::slice::from_ref(&relay),
            &nervix_models::EntityGateTargets::Entities((std::slice::from_ref(&entity)).to_vec()),
            EntityGatePurpose::OwnershipHandoff,
        )
        .expect("the engaged operation reports its pending flush");
    assert_eq!(status.node_work_items, 1);
    assert!(!runtime.entity_gate_operation_owns_unit(
        &coordination,
        &domain,
        &NodeUnitRef::new(entity.clone(), Unit::SINGLE),
        EntityGatePurpose::OwnershipHandoff,
    ));
    let freeze = OwnershipHandoffFreezeWatch::new(&runtime, entity.in_domain(&domain));
    assert!(!freeze.observe().is_frozen());
    nervix_primitives::time::advance(Duration::from_secs(6)).await;
    nervix_primitives::time::timeout(Duration::from_secs(1), async {
        while runtime.entity_gate_operation_is_held(&coordination) {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("the original lease releases its unfinished operation");
    assert!(!dispatch_gate.is_closed());
    assert!(!freeze.observe().is_frozen());
    assert_eq!(counters.force_flush_obligations(), 1);
    drop(participant);
    assert_eq!(counters.force_flush_obligations(), 0);
}
