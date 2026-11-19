//! Model checks for relay-owner fan-out while a consumer schedule changes.
//!
//! Layer: test harness.
//! - **Owns.** Owner-side ACK and delivery across registration, backpressure, and dispatch fences.
//! - **Depends on.** The production relay boundary, dispatch gate, ACK tree, and Shuttle runner.
//! - **Must not know.** Plexus, placement policy, or connector internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use nervix_model_harness::shuttle::{check_dfs, check_random};
use nervix_primitives::{sync::oneshot, time::Instant};
use nonzero_ext::nonzero;

use super::*;
use crate::runtime_ack::AckOutcome;

#[test]
fn shuttle_a_routed_batch_is_counted_before_its_admission_is_reported() {
    check_dfs(
        || {
            shuttle::future::block_on(async {
                let services = test_relay_boundary_services();
                let mut consumer = services.add_local_runtime_consumer(AckMode::Attached);
                let (root, completion) = AckSet::root();
                let mut batch = quiesce_test_batch();
                batch.acks = vec![root];
                let (admitted_tx, admitted_rx) = oneshot::channel();
                let (release_tx, release_rx) = oneshot::channel();
                let receiving = services.clone();
                let routing = nervix_primitives::task::spawn(async move {
                    let dispatch = receiving.inject_remote_message(Unit::SINGLE, &batch);
                    admitted_tx.send(()).expect("the drain observes admission");
                    release_rx
                        .await
                        .expect("the admission response is released");
                    dispatch
                        .await
                        .expect("the attached consumer accepts the batch");
                    batch.ack_success();
                });

                admitted_rx.await.expect("the receiver reports admission");
                assert_eq!(
                    services.fanout.transit().held(),
                    1,
                    "a drain sees the admitted batch before its dispatch future is polled"
                );
                release_tx
                    .send(())
                    .expect("the receiver awaits the response");
                routing.await.expect("routing finishes");
                assert_eq!(services.fanout.transit().held(), 0);
                let mut completion = std::pin::pin!(completion.wait());
                assert!(futures_util::poll!(&mut completion).is_pending());
                consumer
                    .recv()
                    .await
                    .expect("the consumer receives the batch")
                    .ack_success();
                assert_eq!(completion.await, AckOutcome::Ack);
                services.remove_local_runtime_consumer(AckMode::Attached);

                let batch = quiesce_test_batch();
                let cancelled = services.inject_remote_message(Unit::SINGLE, &batch);
                assert_eq!(services.fanout.transit().held(), 1);
                drop(cancelled);
                assert_eq!(services.fanout.transit().held(), 0);
            });
        },
        Some(128),
    );
}

#[test]
fn shuttle_a_detached_consumer_joins_during_attached_backpressure() {
    check_dfs(
        || {
            shuttle::future::block_on(async {
                let partitioned = RelayPartitioning::of_partitions(
                    nervix_models::ResolvedBranchPartitions::Direct(nonzero!(2u16)),
                );
                for partitioning in [RelayPartitioning::Whole, partitioned] {
                    nervix_primitives::task::consume_budget().await;
                    for common in [false, true] {
                        nervix_primitives::task::consume_budget().await;
                        let fanout = match partitioning {
                            RelayPartitioning::Whole => {
                                RelayBoundaryFanout::direct_with_capacity(nonzero!(1usize))
                            }
                            RelayPartitioning::Partitioned(_) => {
                                RelayBoundaryFanout::branch_collapse_with_capacity(nonzero!(1usize))
                            }
                            RelayPartitioning::AwaitingTopology => {
                                panic!("the model selects whole or resolved direct partitions")
                            }
                        };
                        let services = Arc::new(RelayBoundaryServices::new(
                            fanout,
                            0,
                            0,
                            Vec::new(),
                            None,
                            RelayBranchPresences::fresh(partitioning),
                            Arc::new(crate::runtime_ack::AckRootTracker::default()),
                        ));
                        let mut batch = quiesce_test_batch();
                        batch.key = string_branch_key("tenant", "acme");
                        let unit = partitioning
                            .unit_of(&batch.key)
                            .assured("the concrete key names its relay unit");
                        let mut attached =
                            services.add_local_unit_runtime_consumer(AckMode::Attached, unit);
                        services
                            .dispatch_local_runtime_consumers(
                                RuntimeConsumerDispatch::Owner,
                                unit,
                                &batch,
                            )
                            .await
                            .assured("the initial delivery fills the attached queue");
                        let publishing = services.clone();
                        let owner = nervix_primitives::task::spawn(async move {
                            publishing
                                .dispatch_local_runtime_consumers(
                                    RuntimeConsumerDispatch::Owner,
                                    unit,
                                    &batch,
                                )
                                .await
                        });
                        let registering = services.clone();
                        let joining = nervix_primitives::task::spawn(async move {
                            if common {
                                registering.add_local_runtime_consumer(AckMode::Detached)
                            } else {
                                registering.add_local_unit_runtime_consumer(AckMode::Detached, unit)
                            }
                        });
                        // Registration finishes before the attached queue is freed. The owner may
                        // have started its attached publication before or after that registration.
                        let mut detached = joining.await.assured("the detached consumer registers");
                        assert!(matches!(attached.try_recv(), RelayTryRecv::Batch(_)));
                        owner
                            .await
                            .assured("the publishing task finishes")
                            .assured("the attached queue returned its admission");
                        assert!(matches!(attached.try_recv(), RelayTryRecv::Batch(_)));
                        assert!(
                            matches!(detached.try_recv(), RelayTryRecv::Batch(_)),
                            "a detached consumer registered before attached admission completes \
                             receives that publication exactly once"
                        );
                        assert!(matches!(detached.try_recv(), RelayTryRecv::Empty));
                    }
                }
            });
        },
        Some(128),
    );
}

#[test]
fn shuttle_a_partition_handoff_fences_its_delivery_and_materialized_reads_while_its_sibling_flows()
{
    check_dfs(
        || {
            shuttle::future::block_on(async {
                let partitioning = RelayPartitioning::of_partitions(
                    nervix_models::ResolvedBranchPartitions::Direct(nonzero!(2u16)),
                );
                let services = Arc::new(RelayBoundaryServices::new(
                    RelayBoundaryFanout::branch_collapse_with_capacity(nonzero!(1usize)),
                    0,
                    0,
                    Vec::new(),
                    None,
                    RelayBranchPresences::fresh(partitioning),
                    Arc::new(crate::runtime_ack::AckRootTracker::default()),
                ));
                let first = Unit::partition(0);
                let second = Unit::partition(1);
                let mut moving = services.add_local_unit_runtime_consumer(AckMode::Attached, first);
                let mut sibling =
                    services.add_local_unit_runtime_consumer(AckMode::Attached, second);
                let mut before = quiesce_test_batch();
                before.key = string_branch_key("tenant", "eta");
                assert_eq!(
                    services
                        .unit_of(&before.key)
                        .assured("the key names a declared partition"),
                    first
                );
                let (root, first_completion) = AckSet::root();
                before.acks = vec![root];
                let first_services = services.clone();
                let owner = nervix_primitives::task::spawn(async move {
                    first_services
                        .fanout_owner_batch(
                            &domain("default"),
                            &named("orders"),
                            first,
                            &before,
                            &ConfiguredFaultInjection::default(),
                        )
                        .await
                });
                let (held, is_held) = oneshot::channel();
                let (release, may_release) = oneshot::channel();
                let moving_services = services.clone();
                let handoff = nervix_primitives::task::spawn(async move {
                    let gate = moving_services
                        .fanout
                        .unit_dispatch_gate(first)
                        .assured("a partition has a dispatch gate");
                    let mut lease = RelayDispatchGateLease::engage(
                        gate,
                        Instant::now() + Duration::from_secs(86_400),
                        "move partition zero",
                    );
                    assert!(lease.wait_quiescent().await);
                    assert!(moving_services.dispatch_is_fenced(first));
                    assert!(!moving_services.dispatch_is_fenced(second));
                    held.send(())
                        .assured("the sibling waits for the selected partition fence");
                    may_release
                        .await
                        .assured("the sibling releases the hold after delivery");
                    drop(lease);
                });
                is_held
                    .await
                    .assured("the handoff publishes its selected unit fence");
                let mut after = quiesce_test_batch();
                after.key = string_branch_key("tenant", "acme");
                assert_eq!(
                    services
                        .unit_of(&after.key)
                        .assured("the sibling key names its partition"),
                    second
                );
                let (root, completion) = AckSet::root();
                after.acks = vec![root];
                services
                    .fanout_owner_batch(
                        &domain("default"),
                        &named("orders"),
                        second,
                        &after,
                        &ConfiguredFaultInjection::default(),
                    )
                    .await
                    .assured("the sibling is never fenced by the moving partition");
                let RelayTryRecv::Batch(delivered) = sibling.try_recv() else {
                    panic!("the sibling receives its batch while the other unit is held");
                };
                assert_eq!(delivered.key, after.key);
                delivered.ack_success();
                assert_eq!(completion.wait().await, AckOutcome::Ack);
                release
                    .send(())
                    .assured("the selected partition is held until sibling completion");
                handoff.await.assured("the handoff releases its own gate");
                let result = owner
                    .await
                    .assured("the first owner finishes its admitted attempt");
                let received = match moving.try_recv() {
                    RelayTryRecv::Batch(delivered) => {
                        assert_eq!(delivered.key, string_branch_key("tenant", "eta"));
                        delivered.ack_success();
                        true
                    }
                    RelayTryRecv::Empty | RelayTryRecv::Closed => false,
                };
                let outcome = first_completion.wait().await;
                if outcome == AckOutcome::Ack {
                    assert!(received && result.is_ok());
                } else {
                    assert!(matches!(outcome, AckOutcome::NoAck(_)));
                }
            });
        },
        Some(128),
    );
}

/// One key of each partition of the relay `services` serve, found by the relay's own routing.
fn keys_by_partition(
    services: &RelayBoundaryServices,
    partitions: u32,
) -> std::collections::BTreeMap<Unit, Option<BranchKey>> {
    let mut keys = std::collections::BTreeMap::new();
    for candidate in 0..1024 {
        let key = string_branch_key("tenant", &format!("tenant-{candidate}"));
        let unit = services
            .unit_of(&key)
            .assured("a concrete key names a declared partition");
        keys.entry(unit).or_insert(key);
        if keys.len() == usize::try_from(partitions).assured("the fixture count fits usize") {
            return keys;
        }
    }
    panic!("1024 keys name every one of {partitions} partitions")
}

/// The batch a consumer received, acknowledged, or `None` when it received nothing.
fn received_key(consumer: &mut RelayRuntimeFanIn) -> Option<Option<BranchKey>> {
    match consumer.try_recv() {
        RelayTryRecv::Batch(delivered) => {
            let key = delivered.key.clone();
            delivered.ack_success();
            Some(key)
        }
        RelayTryRecv::Empty | RelayTryRecv::Closed => None,
    }
}

/// The two instances of an emitter with `INSTANCES 2` read a relay of four partitions through one
/// consumer of each partition they serve: instance 0 partitions 0 and 2, instance 1 partitions 1
/// and 3. While instance 1 moves (its partitions fenced, its consumers removed and their
/// successors registered), instance 0 receives exactly its own partitions' batches, and each batch
/// of instance 1 reaches the consumer of its own partition at most once or ends unacknowledged.
#[test]
fn shuttle_emitter_instances_receive_only_their_partitions_while_a_sibling_moves() {
    check_dfs(
        || {
            shuttle::future::block_on(async {
                let partitions = nervix_models::ResolvedBranchPartitions::Direct(nonzero!(4u16));
                let services = Arc::new(RelayBoundaryServices::new(
                    RelayBoundaryFanout::branch_collapse_with_capacity(nonzero!(1usize)),
                    0,
                    0,
                    Vec::new(),
                    None,
                    RelayBranchPresences::fresh(RelayPartitioning::of_partitions(partitions)),
                    Arc::new(crate::runtime_ack::AckRootTracker::default()),
                ));
                let instances = nonzero!(2u64);
                let staying = partitions.units_served_by(0, instances);
                let moving = partitions.units_served_by(1, instances);
                assert_eq!(staying, [Unit::partition(0), Unit::partition(2)]);
                assert_eq!(moving, [Unit::partition(1), Unit::partition(3)]);
                let keys = keys_by_partition(&services, 4);
                let mut staying_consumers = Vec::new();
                for unit in &staying {
                    staying_consumers
                        .push(services.add_local_unit_runtime_consumer(AckMode::Attached, *unit));
                }
                let mut moving_consumers = Vec::new();
                for unit in &moving {
                    moving_consumers
                        .push(services.add_local_unit_runtime_consumer(AckMode::Attached, *unit));
                }

                // The relay owner publishes one batch to every partition, in partition order.
                let mut completions = Vec::new();
                let mut batches = Vec::new();
                for (unit, key) in &keys {
                    let mut batch = quiesce_test_batch();
                    batch.key = key.clone();
                    let (root, completion) = AckSet::root();
                    batch.acks = vec![root];
                    completions.push((*unit, completion));
                    batches.push((*unit, batch));
                }
                let owner_services = services.clone();
                let owner = nervix_primitives::task::spawn(async move {
                    let mut results = Vec::new();
                    for (unit, batch) in batches {
                        let result = owner_services
                            .fanout_owner_batch(
                                &domain("default"),
                                &named("orders"),
                                unit,
                                &batch,
                                &ConfiguredFaultInjection::default(),
                            )
                            .await;
                        results.push((unit, result.is_ok()));
                    }
                    results
                });

                // Instance 1 moves: each of its partitions is fenced until its consumer is
                // replaced, and what the old consumer already took is handed over first.
                let moving_services = services.clone();
                let moving_units = moving.clone();
                let mover = nervix_primitives::task::spawn(async move {
                    let mut leases = Vec::new();
                    for unit in &moving_units {
                        let gate = moving_services
                            .fanout
                            .unit_dispatch_gate(*unit)
                            .assured("a partition has a dispatch gate");
                        let mut lease = RelayDispatchGateLease::engage(
                            gate,
                            Instant::now() + Duration::from_secs(86_400),
                            "move an emitter instance",
                        );
                        assert!(lease.wait_quiescent().await);
                        leases.push(lease);
                    }
                    let mut handed_over = Vec::new();
                    for (unit, mut consumer) in moving_units.iter().zip(moving_consumers) {
                        if let Some(key) = received_key(&mut consumer) {
                            handed_over.push((*unit, key));
                        }
                        drop(consumer);
                        moving_services
                            .remove_local_unit_runtime_consumer(AckMode::Attached, *unit);
                    }
                    let mut successors = Vec::new();
                    for unit in &moving_units {
                        successors.push(
                            moving_services
                                .add_local_unit_runtime_consumer(AckMode::Attached, *unit),
                        );
                    }
                    drop(leases);
                    (handed_over, successors)
                });

                let (handed_over, mut successors) =
                    mover.await.assured("the moving instance finishes its move");
                let published = owner.await.assured("the relay owner finishes publishing");
                for (unit, mut consumer) in staying.iter().zip(staying_consumers) {
                    let key = received_key(&mut consumer);
                    assert_eq!(
                        key.as_ref(),
                        Some(&keys[unit]),
                        "instance 0 receives the batch of its partition {unit:?}"
                    );
                    assert!(
                        received_key(&mut consumer).is_none(),
                        "instance 0 receives nothing but its own partitions' batches"
                    );
                }
                let mut delivered_moving = handed_over;
                for (unit, consumer) in moving.iter().zip(successors.iter_mut()) {
                    if let Some(key) = received_key(consumer) {
                        delivered_moving.push((*unit, key));
                    }
                }
                for (unit, key) in &delivered_moving {
                    assert!(moving.contains(unit));
                    assert_eq!(
                        key, &keys[unit],
                        "a moved partition receives only its own batch"
                    );
                }
                for (unit, completion) in completions {
                    let outcome = completion.wait().await;
                    let deliveries = delivered_moving
                        .iter()
                        .filter(|(delivered, _)| *delivered == unit)
                        .count();
                    if staying.contains(&unit) {
                        assert_eq!(outcome, AckOutcome::Ack);
                        continue;
                    }
                    assert!(
                        deliveries <= 1,
                        "a moved partition's batch is delivered at most once"
                    );
                    let admitted = published
                        .iter()
                        .any(|(published, ok)| *published == unit && *ok);
                    if outcome == AckOutcome::Ack {
                        assert!(deliveries == 1 && admitted);
                    } else {
                        assert!(matches!(outcome, AckOutcome::NoAck(_)));
                    }
                }
            });
        },
        Some(128),
    );
}

#[test]
fn shuttle_owner_fanout_fails_its_ack_while_an_attached_consumer_moves() {
    check_random(
        || {
            shuttle::future::block_on(async {
                let owner_domain = domain("default");
                let relay = named("orders");
                let services = test_relay_boundary_services();
                let mut sibling = services.add_local_runtime_consumer(AckMode::Attached);
                let mut moving = services.add_local_runtime_consumer(AckMode::Attached);
                let gate = services.fanout.dispatch_gate();
                let (root, completion) = AckSet::root();
                let mut batch = quiesce_test_batch();
                batch.acks = vec![root];
                let (owner_done, owner_is_done) = oneshot::channel();
                let owner_services = services.clone();
                let owner = nervix_primitives::task::spawn(async move {
                    let result = owner_services
                        .fanout_owner_batch(
                            &owner_domain,
                            &relay,
                            Unit::SINGLE,
                            &batch,
                            &ConfiguredFaultInjection::default(),
                        )
                        .await;
                    owner_done
                        .send(())
                        .assured("the moving consumer waits until fan-out returns");
                    result
                });

                let moving_services = services.clone();
                let move_consumer = nervix_primitives::task::spawn(async move {
                    let mut lease = RelayDispatchGateLease::engage(
                        gate,
                        Instant::now() + Duration::from_secs(86_400),
                        "move an attached consumer",
                    );
                    assert!(lease.wait_quiescent().await);
                    let received = match moving.try_recv() {
                        RelayTryRecv::Batch(delivered) => {
                            delivered.ack_success();
                            true
                        }
                        RelayTryRecv::Empty | RelayTryRecv::Closed => false,
                    };
                    drop(moving);
                    moving_services.remove_local_runtime_consumer(AckMode::Attached);
                    owner_is_done
                        .await
                        .assured("the owner finishes before the schedule gate reopens");
                    drop(lease);
                    received
                });

                let owner_result = owner.await.assured("owner task completes");
                let moving_received = move_consumer.await.assured("consumer move completes");
                let sibling_received = match sibling.try_recv() {
                    RelayTryRecv::Batch(delivered) => {
                        delivered.ack_success();
                        true
                    }
                    RelayTryRecv::Empty | RelayTryRecv::Closed => false,
                };
                let outcome = completion.wait().await;
                if outcome == AckOutcome::Ack {
                    assert!(
                        moving_received && sibling_received,
                        "a successful source ACK requires delivery to both attached consumers"
                    );
                    assert!(owner_result.is_ok());
                } else {
                    assert!(matches!(outcome, AckOutcome::NoAck(_)));
                }

                let (retry_root, retry_completion) = AckSet::root();
                let mut retry = quiesce_test_batch();
                retry.acks = vec![retry_root];
                services
                    .fanout_owner_batch(
                        &domain("default"),
                        &named("orders"),
                        Unit::SINGLE,
                        &retry,
                        &ConfiguredFaultInjection::default(),
                    )
                    .await
                    .assured("a released gate admits a retry to its live sibling");
                sibling
                    .recv()
                    .await
                    .expect("the sibling receives the retry")
                    .ack_success();
                assert_eq!(retry_completion.wait().await, AckOutcome::Ack);
            });
        },
        1_000,
    );
}

/// A batch another node routed here reaches this node's attached consumers while one more attached
/// consumer of the relay registers here. Every consumer the batch is delivered to must hold a share
/// of its acknowledgement, so the source is acknowledged only once each of them acknowledged it: a
/// consumer that joins in time and fails the batch fails the ingestor acknowledgement.
#[test]
fn shuttle_a_routed_batch_racing_a_joining_consumer_reserves_a_share_for_every_delivery() {
    check_random(
        || {
            shuttle::future::block_on(async {
                let services = test_relay_boundary_services();
                let mut sibling = services.add_local_runtime_consumer(AckMode::Attached);
                let (root, completion) = AckSet::root();
                let mut batch = quiesce_test_batch();
                batch.acks = vec![root];
                let routing_services = services.clone();
                let routing = nervix_primitives::task::spawn(async move {
                    let routed = routing_services
                        .inject_remote_message(Unit::SINGLE, &batch)
                        .await;
                    // The dispatcher resolves the share it routed the batch with once the batch
                    // reached this node's consumers, as remote dispatch does.
                    if routed.is_ok() {
                        batch.ack_success();
                    }
                });
                let joining_services = services.clone();
                let joining = nervix_primitives::task::spawn(async move {
                    joining_services.add_local_runtime_consumer(AckMode::Attached)
                });
                routing
                    .await
                    .assured("the routing side only routes one batch");
                let mut joined = joining
                    .await
                    .assured("the joining side only registers its consumer");

                if let RelayTryRecv::Batch(delivered) = sibling.try_recv() {
                    delivered.ack_success();
                }
                let joined_received = match joined.try_recv() {
                    RelayTryRecv::Batch(delivered) => {
                        for ack in delivered.acks.iter() {
                            ack.no_ack("the joining consumer fails the batch it received");
                        }
                        true
                    }
                    RelayTryRecv::Empty | RelayTryRecv::Closed => false,
                };
                let outcome = completion.wait().await;
                if joined_received {
                    assert_ne!(
                        outcome,
                        AckOutcome::Ack,
                        "the source was acknowledged although a consumer that received the batch \
                         failed it"
                    );
                }
            });
        },
        1_000,
    );
}

/// The batches a consumer's receiver holds, read and counted.
fn received_batches(consumer: &mut RelayRuntimeFanIn) -> usize {
    let mut received = 0usize;
    while let RelayTryRecv::Batch(_) = consumer.try_recv() {
        received = received
            .checked_add(1)
            .assured("a check publishes one batch");
    }
    received
}

/// A batch the owner publishes while a failed entity swap holds the relay reaches the consumer the
/// restore starts again exactly once, and its publication completes. The swap stops the unit's
/// previous task after the fence and, when it got that far, moves the registration to a
/// replacement; the restore puts the registration back and starts the previous consumer before
/// the hold ends, so no publication meets a counted consumer without a receiver.
#[test]
fn shuttle_a_batch_held_through_a_failed_swap_reaches_the_restored_consumer_once() {
    check_dfs(
        || {
            shuttle::future::block_on(async {
                let partitioned = RelayPartitioning::of_partitions(
                    nervix_models::ResolvedBranchPartitions::Direct(nonzero!(2u16)),
                );
                for partitioning in [RelayPartitioning::Whole, partitioned] {
                    nervix_primitives::task::consume_budget().await;
                    for replacement_attached in [false, true] {
                        nervix_primitives::task::consume_budget().await;
                        let fanout = match partitioning {
                            RelayPartitioning::Whole => {
                                RelayBoundaryFanout::direct_with_capacity(nonzero!(1usize))
                            }
                            RelayPartitioning::Partitioned(_) => {
                                RelayBoundaryFanout::branch_collapse_with_capacity(nonzero!(1usize))
                            }
                            RelayPartitioning::AwaitingTopology => {
                                panic!("the model selects whole or resolved direct partitions")
                            }
                        };
                        let services = Arc::new(RelayBoundaryServices::new(
                            fanout,
                            0,
                            0,
                            Vec::new(),
                            None,
                            RelayBranchPresences::fresh(partitioning),
                            Arc::new(crate::runtime_ack::AckRootTracker::default()),
                        ));
                        let mut batch = quiesce_test_batch();
                        batch.key = string_branch_key("tenant", "acme");
                        let unit = partitioning
                            .unit_of(&batch.key)
                            .assured("the concrete key names its relay unit");
                        let gate = match partitioning {
                            RelayPartitioning::Whole => services.fanout.dispatch_gate(),
                            RelayPartitioning::Partitioned(_)
                            | RelayPartitioning::AwaitingTopology => services
                                .fanout
                                .unit_dispatch_gate(unit)
                                .assured("a partition has a dispatch gate"),
                        };
                        let mut previous =
                            services.add_local_unit_runtime_consumer(AckMode::Attached, unit);
                        let publishing = services.clone();
                        let owner = nervix_primitives::task::spawn(async move {
                            publishing
                                .fanout_owner_batch(
                                    &domain("default"),
                                    &named("orders"),
                                    unit,
                                    &batch,
                                    &ConfiguredFaultInjection::default(),
                                )
                                .await
                        });

                        let mut lease = RelayDispatchGateLease::engage(
                            gate,
                            Instant::now() + Duration::from_secs(86_400),
                            "failed entity swap",
                        );
                        assert!(lease.wait_quiescent().await);
                        // The previous task handles what reached it before it hands its branches
                        // over, and its receiver ends with it.
                        let mut deliveries = received_batches(&mut previous);
                        drop(previous);
                        let mut restored = if replacement_attached {
                            services.remove_local_unit_runtime_consumer(AckMode::Attached, unit);
                            let replacement =
                                services.add_local_unit_runtime_consumer(AckMode::Attached, unit);
                            drop(replacement);
                            services.remove_local_unit_runtime_consumer(AckMode::Attached, unit);
                            services.add_local_unit_runtime_consumer(AckMode::Attached, unit)
                        } else {
                            services.registered_unit_runtime_fan_in(AckMode::Attached, unit)
                        };
                        drop(lease);

                        owner
                            .await
                            .assured("the owner task finishes")
                            .assured("the restored consumer admits the held batch");
                        deliveries = deliveries
                            .checked_add(received_batches(&mut restored))
                            .assured("a check publishes one batch");
                        assert_eq!(
                            deliveries, 1,
                            "a batch published across a failed swap reaches the unit's consumer \
                             exactly once"
                        );
                    }
                }
            });
        },
        Some(512),
    );
}
