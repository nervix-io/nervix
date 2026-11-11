//! Remote dispatch and acknowledgement lifecycle regressions.
//!
//! Layer: test harness.
//! - **Owns.** Production remote dispatch, admission and acknowledgement lifetime assertions.
//! - **Depends on.** Runtime fixtures and the production relay and acknowledgement owners.
//! - **Must not know.** Connector implementations or parser internals.

use std::time::Duration;

use futures_util::FutureExt as _;
use nervix_models::{AckMode, ClusterNodeName, RemoteAckOutcome};
use nervix_primitives::{
    sync::{oneshot, watch},
    time::{Instant, sleep, timeout},
};

use super::*;
use crate::runtime_ack::{AckCompletion, AckOutcome, AckSet, AckWaitOutcome};

/// Scheduler-independent hang guard for event-driven unit-test observations.
const ASYNC_EVENT_FAILSAFE: Duration = Duration::from_secs(30);

#[nervix_primitives::test(start_paused = true)]
async fn runtime_report_chain_remote_admission_response_log() {
    use crate::runtime::report_observer::ReportLogObserver;

    let runtime = Runtime::default();
    let local = named::<ClusterNodeName>("node-1");
    attach_loopback_cluster(&runtime, &local).await;
    runtime
        .inner
        .remote_dispatcher
        .load_full()
        .assured("the loopback cluster installs its dispatcher")
        .interconnect
        .shutdown()
        .await;
    let admission = RemoteAckRegistration {
        ack_id: 7,
        registrar: ClusterNodeIdentity::new(
            named("node-2"),
            nervix_models::ClusterNodeIncarnation::new(1),
        ),
    };
    let mut logs = ReportLogObserver::new();
    logs.observe(runtime.send_remote_relay_admission_outcome(&admission, RemoteAckOutcome::Ack))
        .await;
    let record = logs.next("failed to return relay admission response");
    assert_eq!(record.fields["admission_id"], "7");
    assert_eq!(
        record.fields["error"],
        "failed to send a remote payload to node 'node-2': transport is shutting down"
    );
}

struct DropNotice(Option<oneshot::Sender<()>>);

impl Drop for DropNotice {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            sender
                .send(())
                .means_peer_left("remote ACK watcher cancellation test");
        }
    }
}

#[test]
fn a_reingestor_on_another_node_consumes_the_relays_this_node_owns() {
    let domain = domain("default");
    let fixture = EntrypointTestDomain {
        relays: &["incoming", "outgoing"],
        fields: &[("value", nervix_models::ParseAsType::I64)],
        branch_fields: &[],
    };
    let reingestor = nervix_models::CreateReingestor {
        labels: Default::default(),
        name: named("repartition"),
        from: nervix_models::ProcessorInputs::single(named("incoming")),
        output_routes: with_inherit_all(nervix_models::ProcessorOutputs::single(named("outgoing")))
            .with_flush_policy(FlushPolicy::Immediate)
            .with_branch(nervix_models::OutputBranch::Unbranched),
        mode: AckMode::Detached,
        materialized_state: Vec::new(),
        filter_where: None,
    };
    let plans = fixture.plans(
        &domain,
        vec![nervix_models::Model::Reingestor(reingestor.clone())],
    );
    let owner = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let executor = ClusterNodeName::parse("node-2").assured("the fixture node name is valid");
    let mut nodes = plans.nodes.into_values().collect::<Vec<_>>();
    for node in &mut nodes {
        let placement = if node.kind() == ModelKind::Reingestor {
            &executor
        } else {
            &owner
        };
        node.single_unit_mut().primary_node = Some(placement.clone());
        node.single_unit_mut().assigned_nodes = vec![placement.clone()];
    }
    let schedule = DomainSchedule::new(domain.clone(), nodes, Vec::new());
    let revision = ExecutionRevision::from_schedule(&schedule)
        .assured("the fixture schedule resolves its complete execution plans");

    let owned = Runtime::remote_runtime_consumers_for_revision(&revision, &owner);
    let consumers = owned
        .get(&named::<RelayName>("incoming"))
        .assured("the remote reingestor reads the relay this node owns");
    assert_eq!(consumers.len(), 1);
    assert_eq!(consumers[0].node_id, executor);
    assert_eq!(consumers[0].mode, Some(AckMode::Detached));
    assert!(!owned.contains_key(&named::<RelayName>("outgoing")));

    let executing = Runtime::remote_runtime_consumers_for_revision(&revision, &executor);
    assert!(executing.is_empty());
}

#[nervix_primitives::test]
async fn runtime_shutdown_cancels_remote_ack_watcher_tasks() {
    let runtime = Runtime::default();
    let (started_tx, started_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    runtime.spawn_remote_ack_watcher_task(async move {
        let _notice = DropNotice(Some(dropped_tx));
        started_tx
            .send(())
            .means_peer_left("remote ACK watcher cancellation test starter");
        std::future::pending::<()>().await;
    });
    started_rx
        .await
        .assured("the tracked watcher sends before entering its pending state");

    runtime.shutdown(runtime.domain_drain_timeout()).await;

    let dropped = timeout(ASYNC_EVENT_FAILSAFE, dropped_rx).await;
    assert!(
        dropped.is_ok(),
        "runtime shutdown should cancel a pending remote ACK watcher"
    );
    dropped
        .verified("the watcher cancellation deadline was checked by the assertion above")
        .assured("dropping the watcher always sends its drop notice");
    assert!(runtime.inner.remote_ack_watcher_tasks.is_empty());
}

#[nervix_primitives::test(start_paused = true)]
async fn ack_alive_resets_ingestor_ack_timeout() {
    let (acks, mut completion) = AckSet::root();
    let ack_task = acks.clone();

    nervix_primitives::task::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        ack_task.ack_alive();
        sleep(Duration::from_millis(150)).await;
        ack_task.ack_success();
    });

    assert_eq!(
        completion
            .wait_while_alive(Duration::from_millis(200))
            .await,
        AckWaitOutcome::Resolved(AckOutcome::Ack)
    );
}

/// A runtime attached to a loopback cluster of one node, and the dispatcher that hands out its
/// registrations.
async fn joined_runtime() -> (Runtime, StdArc<RemoteDispatcher>) {
    let runtime = Runtime::default();
    let node = ClusterNodeName::parse("node-1").expect("the fixture node name is valid");
    attach_loopback_cluster(&runtime, &node).await;
    let dispatcher = runtime
        .inner
        .remote_dispatcher
        .load_full()
        .expect("attaching the loopback cluster attaches the dispatcher");
    (runtime, dispatcher)
}

#[nervix_primitives::test]
async fn a_cancelled_remote_delivery_resolves_its_records_and_returns_its_owner() {
    let (runtime, dispatcher) = joined_runtime().await;
    let (acks, completion) = AckSet::root();
    let storage = dispatcher
        .store_pending_acks(
            &receiving_node(),
            vec![acks],
            RemoteDispatcher::delivery_deadline(),
            std::future::pending(),
        )
        .await
        .assured("the fixture delivery fits");
    let RemoteAckStorage::Stored(delivery) = storage else {
        panic!("an uncancelled delivery with room stores its records");
    };
    let registration = delivery.registrations()[0]
        .clone()
        .assured("the fixture row has an acknowledgement");
    assert!(dispatcher.registry.holds_ack(registration.ack_id));
    drop(delivery);
    assert_eq!(
        completion.wait().await,
        AckOutcome::NoAck("remote delivery ended before admission".to_string())
    );
    assert!(!dispatcher.registry.holds_ack(registration.ack_id));
    assert_eq!(
        dispatcher.executor.snapshot().relay_memory.reserved_bytes,
        0
    );
    runtime.shutdown(runtime.domain_drain_timeout()).await;
}

#[nervix_primitives::test]
async fn remote_ack_watcher_memory_is_released_when_the_registrar_run_differs() {
    let (runtime, dispatcher) = joined_runtime().await;
    let (acks, completion) = AckSet::root();
    let registration = from_an_earlier_run(&dispatcher.registration(1));
    runtime
        .spawn_remote_ack_watcher(
            named::<DomainName>("watcher"),
            completion,
            Some(registration),
        )
        .await
        .assured("the fixture watcher fits");
    timeout(ASYNC_EVENT_FAILSAFE, async {
        while runtime.inner.remote_ack_watcher_tasks.len() > 1 {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .assured("the published registrar incarnation ends the stale watcher");
    assert_eq!(
        dispatcher.executor.snapshot().relay_memory.reserved_bytes,
        0
    );
    acks.ack_success();
    runtime.shutdown(runtime.domain_drain_timeout()).await;
}

#[nervix_primitives::test]
async fn wide_remote_ack_batch_owns_one_charged_task_and_releases_it_on_shutdown() {
    let (runtime, dispatcher) = joined_runtime().await;
    let count = 8192;
    let mut roots = Vec::with_capacity(count);
    let mut watches = Vec::with_capacity(count);
    for _ in 0..count {
        let (root, completion) = AckSet::root();
        roots.push(root);
        watches.push(RemoteAckWatch::new(completion, dispatcher.registration(1)));
    }
    let RemoteAckWatchAdmission::Charged { dispatcher, charge } = runtime
        .hold_remote_ack_watches(count, std::future::pending())
        .await
        .assured("the wide batch fits its relay budget")
    else {
        panic!("a joined runtime charges the watches of a wide batch at once");
    };
    let executor = dispatcher.executor.clone();
    let expected_bytes =
        u64::try_from(count * 1024 + 4096).assured("the fixture's watcher allocation fits in u64");
    assert_eq!(charge.bytes_held(), expected_bytes);
    runtime.spawn_remote_ack_watchers(named::<DomainName>("watcher"), dispatcher, watches, charge);
    assert_eq!(runtime.inner.remote_ack_watcher_tasks.len(), 2);
    assert_eq!(
        executor.snapshot().relay_memory.reserved_bytes,
        expected_bytes,
    );
    runtime.shutdown(runtime.domain_drain_timeout()).await;
    assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0,);
    drop(roots);
}

/// A watcher that finds the relay budget full waits for room instead of being refused, and
/// starts with its charge once the memory that filled the budget returns.
#[nervix_primitives::test]
async fn a_watcher_held_by_a_full_relay_budget_starts_once_room_returns() {
    let (runtime, dispatcher) = joined_runtime().await;
    let capacity = dispatcher.executor.snapshot().relay_memory.capacity_bytes;
    let occupied = dispatcher
        .executor
        .try_reserve(nervix_execution::MemoryClass::Relay, capacity)
        .assured("the fixture reserves exactly its relay budget");
    let (acks, completion) = AckSet::root();
    let watcher = runtime.spawn_remote_ack_watcher(
        named::<DomainName>("watcher"),
        completion,
        Some(dispatcher.registration(1)),
    );
    let mut watcher = std::pin::pin!(watcher);
    assert!(watcher.as_mut().now_or_never().is_none());
    assert_eq!(runtime.inner.remote_ack_watcher_tasks.len(), 1);
    assert_eq!(dispatcher.executor.snapshot().relay_memory.refused, 0);

    drop(occupied);
    timeout(ASYNC_EVENT_FAILSAFE, watcher)
        .await
        .assured("the watcher takes its charge at its first recheck after room returns")
        .assured("one watch fits the emptied relay budget");
    assert_eq!(runtime.inner.remote_ack_watcher_tasks.len(), 2);
    assert_eq!(
        dispatcher.executor.snapshot().relay_memory.reserved_bytes,
        1024 + 4096
    );
    acks.ack_success();
    runtime.shutdown(runtime.domain_drain_timeout()).await;
    assert_eq!(
        dispatcher.executor.snapshot().relay_memory.reserved_bytes,
        0
    );
}

/// A registration the fixture's registering node handed out in its run `incarnation`.
fn registered_by(incarnation: ClusterNodeIncarnation) -> RemoteAckRegistration {
    RemoteAckRegistration {
        ack_id: 1,
        registrar: ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-1").expect("the fixture node name is valid"),
            incarnation,
        ),
    }
}

fn after(start: Instant, elapsed: Duration) -> Instant {
    start
        .checked_add(elapsed)
        .expect("a fixture instant a few seconds ahead fits the monotonic clock")
}

/// A failure detector that briefly drops the registrar's run from the live view does not end
/// that run, so the outcome the root reaches meanwhile waits for the run and is returned once
/// it is back, instead of being dropped with the watch.
#[nervix_primitives::test]
async fn an_outcome_reached_while_the_registrar_is_missing_is_returned_once_it_is_back() {
    let run = ClusterNodeIncarnation::new(7);
    let (acks, completion) = AckSet::root();
    let mut watch = RemoteAckWatch::new(completion, registered_by(run));
    let start = Instant::now();
    let RemoteAckWatchReport::Send(first) = watch.next_report(None, Some(run), start) else {
        panic!("a watch reports to a registrar its live view shows");
    };
    assert!(first.is_progress());

    acks.ack_success();
    let progress = watch.completion.wait_for_progress().await;
    assert_eq!(progress, AckProgress::Complete(AckOutcome::Ack));
    let missing = after(start, Duration::from_millis(100));
    assert!(matches!(
        watch.next_report(Some(progress), None, missing),
        RemoteAckWatchReport::Hold
    ));
    // The completion delivered its outcome once; waiting again only passes time.
    let (mut watch, progress) = wait_remote_ack_progress(watch).await;
    assert_eq!(progress, None);
    let still_missing = after(start, Duration::from_millis(200));
    assert!(matches!(
        watch.next_report(progress, None, still_missing),
        RemoteAckWatchReport::Hold
    ));

    let back = after(start, Duration::from_millis(300));
    assert!(matches!(
        watch.next_report(None, Some(run), back),
        RemoteAckWatchReport::Send(RemoteAckOutcome::Ack)
    ));
}

/// A negative outcome reaches a registrar the live view shows as the outcome the root decided.
#[nervix_primitives::test]
async fn a_negative_outcome_is_returned_as_the_root_decided_it() {
    let run = ClusterNodeIncarnation::new(7);
    let (acks, completion) = AckSet::root();
    let mut watch = RemoteAckWatch::new(completion, registered_by(run));
    acks.no_ack("the attached emitter refused the record");
    let progress = watch.completion.wait_for_progress().await;
    let RemoteAckWatchReport::Send(outcome) =
        watch.next_report(Some(progress), Some(run), Instant::now())
    else {
        panic!("a watch reports to a registrar its live view shows");
    };
    assert_eq!(
        outcome,
        RemoteAckOutcome::NoAck("the attached emitter refused the record".to_string())
    );
}

/// Once the registrar's run has stayed missing past its own silence bound and one sweep, a
/// registering node whose run goes on has failed the acknowledgement, so the watch gives the
/// outcome up.
#[nervix_primitives::test]
async fn an_outcome_whose_registrar_stays_missing_past_its_grace_is_given_up() {
    let run = ClusterNodeIncarnation::new(7);
    let (acks, completion) = AckSet::root();
    let mut watch = RemoteAckWatch::new(completion, registered_by(run));
    let start = Instant::now();
    assert!(matches!(
        watch.next_report(None, Some(run), start),
        RemoteAckWatchReport::Send(_)
    ));
    acks.ack_success();
    let progress = watch.completion.wait_for_progress().await;
    assert!(matches!(
        watch.next_report(Some(progress), None, start),
        RemoteAckWatchReport::Hold
    ));
    let past_grace = after(start, RegistrarPresence::missing_grace());
    assert!(matches!(
        watch.next_report(None, None, past_grace),
        RemoteAckWatchReport::Release
    ));
}

/// The node the tests' deliveries go to, which resolves the acknowledgements they forward.
fn receiving_node() -> ClusterNodeName {
    ClusterNodeName::parse("node-2").expect("the fixture node name is valid")
}

/// The registration an earlier run of the same node handed out under the same number.
fn from_an_earlier_run(registration: &RemoteAckRegistration) -> RemoteAckRegistration {
    let incarnation = registration
        .registrar
        .incarnation()
        .get()
        .checked_sub(1)
        .expect("a loopback cluster's incarnation is its start time in nanoseconds");
    RemoteAckRegistration {
        ack_id: registration.ack_id,
        registrar: ClusterNodeIdentity::new(
            registration.registrar.node_id().clone(),
            ClusterNodeIncarnation::new(incarnation),
        ),
    }
}

#[nervix_primitives::test]
async fn remote_ack_alive_packet_resets_ingestor_ack_timeout() {
    let (runtime, dispatcher) = joined_runtime().await;
    nervix_primitives::time::pause();
    let (acks, mut completion) = AckSet::root();
    let registration = dispatcher
        .register_pending_ack(&receiving_node(), acks)
        .assured("the fixture has free correlation capacity");
    let runtime_task = runtime.clone();
    let resolved = registration.clone();

    nervix_primitives::task::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        runtime_task.handle_remote_ack_resolution(resolved.resolution(RemoteAckOutcome::Alive));
        sleep(Duration::from_millis(150)).await;
        runtime_task.handle_remote_ack_resolution(resolved.resolution(RemoteAckOutcome::Ack));
    });

    assert_eq!(
        completion
            .wait_while_alive(Duration::from_millis(200))
            .await,
        AckWaitOutcome::Resolved(AckOutcome::Ack)
    );
    assert!(
        !runtime.inner.remote_dispatch.holds_ack(registration.ack_id),
        "terminal ack must clear the pending remote ack"
    );
}

#[nervix_primitives::test]
async fn remote_parked_progress_releases_and_restores_upstream_handoff_ownership() {
    let (runtime, dispatcher) = joined_runtime().await;
    let tracker = Arc::new(AckRootTracker::default());
    let (acks, completion) = AckSet::tracked_root(tracker.clone());
    let registration = dispatcher
        .register_pending_ack(&receiving_node(), acks)
        .assured("the fixture has free correlation capacity");
    dispatcher.registry.admit_ack(registration.ack_id);
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 1);

    runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Progress {
        sequence: 1,
        parked: true,
    }));
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    assert!(completion.remote_progress().1);

    // A delayed status cannot undo the newer park. An actual resume can.
    runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Progress {
        sequence: 0,
        parked: false,
    }));
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Progress {
        sequence: 2,
        parked: false,
    }));
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 1);
    assert!(!completion.remote_progress().1);

    runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Ack));
    assert_eq!(completion.wait().await, AckOutcome::Ack);
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    assert!(!runtime.inner.remote_dispatch.holds_ack(registration.ack_id));
}

#[nervix_primitives::test]
async fn remote_relay_admission_alive_resets_dispatch_timeout() {
    let (runtime, dispatcher) = joined_runtime().await;
    nervix_primitives::time::pause();
    let (registration, admission_rx) = dispatcher
        .register_pending_relay_admission()
        .assured("the fixture has free correlation capacity");
    let runtime_task = runtime.clone();
    let resolved = registration.clone();

    nervix_primitives::task::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        runtime_task.handle_remote_ack_resolution(resolved.resolution(RemoteAckOutcome::Alive));
        sleep(Duration::from_millis(150)).await;
        runtime_task.handle_remote_ack_resolution(resolved.resolution(RemoteAckOutcome::Ack));
    });

    RemoteDispatcher::await_relay_admission(
        &ClusterNodeName::parse("relay-owner").expect("valid name"),
        admission_rx,
        Duration::from_millis(200),
        RemoteDispatcher::delivery_deadline(),
    )
    .await
    .expect("alive progress should preserve the relay admission wait");
    assert!(
        !runtime.inner.remote_dispatch.holds_ack(registration.ack_id),
        "terminal admission ack must clear pending admission state"
    );
}

#[nervix_primitives::test]
async fn a_resolution_addressed_to_an_earlier_run_leaves_the_pending_ack_unresolved() {
    let (runtime, dispatcher) = joined_runtime().await;
    let (acks, completion) = AckSet::root();
    let registration = dispatcher
        .register_pending_ack(&receiving_node(), acks)
        .assured("the fixture has free correlation capacity");
    let completion = completion.wait();
    tokio::pin!(completion);

    let earlier = from_an_earlier_run(&registration);
    runtime.handle_remote_ack_resolution(earlier.resolution(RemoteAckOutcome::Alive));
    runtime.handle_remote_ack_resolution(earlier.resolution(RemoteAckOutcome::Ack));

    assert!(
        (&mut completion).now_or_never().is_none(),
        "an acknowledgement addressed to an earlier run must not complete this run's record"
    );
    assert!(
        runtime.inner.remote_dispatch.holds_ack(registration.ack_id),
        "this run's registration stays pending for its own resolution"
    );

    runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Ack));
    assert_eq!(
        timeout(ASYNC_EVENT_FAILSAFE, completion)
            .await
            .expect("this run's own resolution completes the record"),
        AckOutcome::Ack
    );
}

#[nervix_primitives::test]
async fn a_resolution_addressed_to_an_earlier_run_leaves_the_relay_admission_pending() {
    let (runtime, dispatcher) = joined_runtime().await;
    let (registration, mut admission_rx) = dispatcher
        .register_pending_relay_admission()
        .assured("the fixture has free correlation capacity");

    let earlier = from_an_earlier_run(&registration);
    runtime.handle_remote_ack_resolution(earlier.resolution(RemoteAckOutcome::Alive));
    runtime.handle_remote_ack_resolution(earlier.resolution(RemoteAckOutcome::Ack));

    assert!(
        !admission_rx
            .has_changed()
            .expect("the pending admission keeps its sender"),
        "an admission outcome addressed to an earlier run must not reach this run's admission"
    );

    runtime.handle_remote_ack_resolution(
        registration.resolution(RemoteAckOutcome::NoAck("branch is draining".to_string())),
    );
    admission_rx
        .changed()
        .await
        .expect("this run's own admission outcome remains observable");
    assert!(matches!(
        &*admission_rx.borrow_and_update(),
        RelayAdmissionUpdate::Rejected(reason) if reason == "branch is draining"
    ));
}

#[nervix_primitives::test]
async fn a_cleared_registration_ignores_a_late_resolution() {
    let (runtime, dispatcher) = joined_runtime().await;
    let (acks, completion) = AckSet::root();
    let acknowledgement = dispatcher
        .register_pending_ack(&receiving_node(), acks.clone())
        .assured("the fixture has free correlation capacity");
    let (admission, admission_rx) = dispatcher
        .register_pending_relay_admission()
        .assured("the fixture has free correlation capacity");

    dispatcher.clear_pending_ack(&acknowledgement);
    dispatcher.clear_pending_relay_admission(&admission);
    runtime.handle_remote_ack_resolution(acknowledgement.resolution(RemoteAckOutcome::Alive));
    runtime.handle_remote_ack_resolution(acknowledgement.resolution(RemoteAckOutcome::Ack));
    runtime.handle_remote_ack_resolution(admission.resolution(RemoteAckOutcome::Ack));

    let completion = completion.wait();
    tokio::pin!(completion);
    assert!(
        (&mut completion).now_or_never().is_none(),
        "the dispatch that cleared the registration resolves the record, not a late outcome"
    );
    assert!(
        admission_rx.has_changed().is_err(),
        "clearing the admission ends its updates"
    );
    drop(acks);
}

#[nervix_primitives::test]
async fn progress_for_an_abandoned_admission_retires_its_registration() {
    let (runtime, dispatcher) = joined_runtime().await;
    let (admission, admission_rx) = dispatcher
        .register_pending_relay_admission()
        .assured("the fixture has free correlation capacity");
    drop(admission_rx);

    runtime.handle_remote_ack_resolution(admission.resolution(RemoteAckOutcome::Alive));

    assert!(
        !runtime.inner.remote_dispatch.holds_ack(admission.ack_id),
        "an admission nobody waits for is retired by its next progress report"
    );
}

#[nervix_primitives::test]
async fn a_node_outside_a_cluster_resolves_nothing() {
    let runtime = Runtime::default();
    let (acks, completion) = AckSet::root();
    let ack_id = runtime
        .inner
        .remote_dispatch
        .register_ack(receiving_node(), acks)
        .assured("the fixture has free correlation capacity");
    let completion = completion.wait();
    tokio::pin!(completion);
    let registration = RemoteAckRegistration {
        ack_id,
        registrar: ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-1").expect("the fixture node name is valid"),
            ClusterNodeIncarnation::new(1),
        ),
    };

    runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Ack));

    assert!(
        (&mut completion).now_or_never().is_none(),
        "a node that has not joined a cluster has handed out no registration to resolve"
    );
}

#[nervix_primitives::test]
async fn remote_relay_admission_rejection_preserves_the_typed_reason() {
    let target = ClusterNodeName::parse("relay-owner").expect("valid name");
    let (admission_tx, admission_rx) = watch::channel(RelayAdmissionUpdate::Pending);
    admission_tx.send_replace(RelayAdmissionUpdate::Rejected(
        "branch is draining".to_string(),
    ));

    let error = RemoteDispatcher::await_relay_admission(
        &target,
        admission_rx,
        Duration::from_secs(1),
        RemoteDispatcher::delivery_deadline(),
    )
    .await
    .expect_err("a rejected relay admission must fail dispatch");

    assert!(matches!(
        error.current_context(),
        RemoteDispatchError::AdmissionRejected {
            target: error_target,
        } if error_target == &target
    ));
    assert!(matches!(
        error.downcast_ref::<TransportError>(),
        Some(TransportError::RelayRejected(reason)) if reason == "branch is draining"
    ));
    assert_eq!(
        format!("{error:#}"),
        "node 'relay-owner' rejected relay admission: relay delivery was rejected before runtime \
         admission: branch is draining"
    );
}

#[nervix_primitives::test]
async fn remote_relay_admission_reports_a_closed_response_channel() {
    let target = ClusterNodeName::parse("relay-owner").expect("valid name");
    let (admission_tx, admission_rx) = watch::channel(RelayAdmissionUpdate::Pending);
    drop(admission_tx);

    let error = RemoteDispatcher::await_relay_admission(
        &target,
        admission_rx,
        Duration::from_secs(1),
        RemoteDispatcher::delivery_deadline(),
    )
    .await
    .expect_err("a closed relay admission response must fail dispatch");

    assert!(matches!(
        error.current_context(),
        RemoteDispatchError::AdmissionResponseClosed {
            target: error_target,
        } if error_target == &target
    ));
}

#[nervix_primitives::test(start_paused = true)]
async fn remote_relay_admission_bounds_the_total_wait() {
    let target = ClusterNodeName::parse("relay-owner").expect("valid name");
    let (_admission_tx, admission_rx) = watch::channel(RelayAdmissionUpdate::Pending);

    let error = RemoteDispatcher::await_relay_admission(
        &target,
        admission_rx,
        REMOTE_RELAY_TOTAL_TIMEOUT + Duration::from_secs(1),
        RemoteDispatcher::delivery_deadline(),
    )
    .await
    .expect_err("relay admission must stop at its total deadline");

    assert!(matches!(
        error.current_context(),
        RemoteDispatchError::AdmissionTotalTimeout {
            target: error_target,
            timeout: REMOTE_RELAY_TOTAL_TIMEOUT,
        } if error_target == &target
    ));
}

#[nervix_primitives::test]
async fn remote_relay_admission_progress_is_coalesced() {
    let (runtime, dispatcher) = joined_runtime().await;
    let (registration, mut admission_rx) = dispatcher
        .register_pending_relay_admission()
        .assured("the fixture has free correlation capacity");

    for _ in 0..100 {
        runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Alive));
    }
    assert!(
        admission_rx
            .has_changed()
            .expect("sender should remain open")
    );
    assert!(matches!(
        &*admission_rx.borrow_and_update(),
        RelayAdmissionUpdate::Alive
    ));
    assert!(
        !admission_rx
            .has_changed()
            .expect("sender should remain open"),
        "replaceable progress must occupy one pending update"
    );

    runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Ack));
    admission_rx
        .changed()
        .await
        .expect("the terminal admission outcome should remain observable");
    assert!(matches!(
        &*admission_rx.borrow_and_update(),
        RelayAdmissionUpdate::Admitted
    ));
}

#[nervix_primitives::test]
async fn forwarding_to_a_remote_relay_owner_extends_the_ack_chain() {
    let (acks, completion) = AckSet::root();
    let forwarded = RemoteDispatcher::forwarded_ack(&acks);
    let completion = completion.wait();
    tokio::pin!(completion);

    acks.ack_success();
    assert!(
        timeout(Duration::from_millis(50), &mut completion)
            .await
            .is_err(),
        "local producer completion must wait for the remote relay path"
    );
    forwarded.ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), &mut completion)
            .await
            .expect("remote relay completion should resolve the producer ACK"),
        AckOutcome::Ack
    );
}

#[nervix_primitives::test]
async fn relay_gate_holds_nonowner_dispatch_before_the_remote_slot() {
    let services = test_relay_boundary_services();
    services.replace_owner_routes(RelayOwnerRoutes::single(
        ClusterNodeName::parse("node-2").expect("valid name"),
    ));
    let gate = services.fanout.dispatch_gate();
    let lease = RelayDispatchGateLease::engage(
        gate,
        Instant::now() + Duration::from_secs(1),
        "relay owner is moving",
    );
    let task_services = services.clone();
    let dispatch = nervix_primitives::task::spawn(async move {
        task_services
            .dispatch_to_owner(
                &domain("default"),
                &named("orders"),
                &quiesce_test_batch(),
                Unit::SINGLE,
            )
            .await
    });

    nervix_primitives::task::yield_now().await;
    assert!(
        !dispatch.is_finished(),
        "a gated nonowner must not enter its remote dispatch slot"
    );

    drop(lease);
    timeout(Duration::from_secs(1), dispatch)
        .await
        .expect("releasing the gate should wake the nonowner dispatch")
        .expect("dispatch task should join")
        .expect_err("the isolated test has no remote dispatcher");
}

#[nervix_primitives::test]
async fn relay_gate_fails_buffered_owner_batch_when_its_attached_consumer_moves() {
    let domain = domain("default");
    let relay = named("orders");
    let services = test_relay_boundary_services();
    let consumer = services.add_local_runtime_consumer(AckMode::Attached);
    let (acks, completion) = AckSet::root();
    let mut batch = quiesce_test_batch();
    batch.acks = vec![acks.clone()];
    let gate = services.fanout.dispatch_gate();
    let mut lease = RelayDispatchGateLease::engage(
        gate,
        Instant::now() + Duration::from_secs(1),
        "attached consumer is moving",
    );
    assert!(lease.wait_quiescent().await);
    drop(consumer);
    services.remove_local_runtime_consumer(AckMode::Attached);
    let result = services
        .fanout_owner_batch(
            &domain,
            &relay,
            Unit::SINGLE,
            &batch,
            &ConfiguredFaultInjection::default(),
        )
        .await;
    assert!(
        result.is_err(),
        "a closed routing gate must fail the attached batch before fan-out"
    );
    let outcome = timeout(Duration::from_secs(1), completion.wait())
        .await
        .expect("the failed owner fan-out must resolve its root ACK");
    assert!(matches!(outcome, AckOutcome::NoAck(_)));
}

/// One acknowledgement forwarded to `receiving_node()`, held by a registry of its own.
struct ForwardedAck {
    registry: RemoteDispatchRegistry,
    ack_id: u64,
    completion: AckCompletion,
}

impl ForwardedAck {
    fn registered() -> Self {
        let registry = RemoteDispatchRegistry::with_capacity(8);
        let (acks, completion) = AckSet::root();
        let ack_id = registry
            .register_ack(receiving_node(), acks)
            .assured("the fixture has free correlation capacity");
        Self {
            registry,
            ack_id,
            completion,
        }
    }

    /// Runs `sweeps` sweeps and requires that none of them fails anything.
    fn sweep_without_failing(&self, sweeps: u64) {
        for _ in 0..sweeps {
            assert!(
                self.registry.fail_silent_acks().is_empty(),
                "no sweep before the silence bound passes may fail the acknowledgement"
            );
        }
    }

    /// Requires the next sweep to fail the acknowledgement.
    fn sweep_failing_it(&self) {
        assert_eq!(
            self.registry.fail_silent_acks().get(&receiving_node()),
            Some(&1),
            "the first sweep past the silence bound fails the acknowledgement"
        );
        assert!(!self.registry.holds_ack(self.ack_id));
    }
}

#[nervix_primitives::test]
async fn an_admitted_acknowledgement_its_receiver_stops_reporting_fails_at_the_silence_bound() {
    let forwarded = ForwardedAck::registered();
    forwarded.registry.admit_ack(forwarded.ack_id);

    forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);
    forwarded.sweep_failing_it();

    let outcome = timeout(ASYNC_EVENT_FAILSAFE, forwarded.completion.wait())
        .await
        .expect("the failed acknowledgement resolves its root");
    assert_eq!(
        outcome,
        AckOutcome::NoAck(
            "node 'node-2' reported nothing about the forwarded record for 15s".to_string()
        )
    );
    assert!(
        !forwarded.registry.report_ack(forwarded.ack_id),
        "a report after the failure finds nothing to keep alive"
    );
    assert!(
        !forwarded
            .registry
            .resolve_ack(forwarded.ack_id, AckOutcome::Ack),
        "an outcome after the failure finds nothing to resolve"
    );
}

#[nervix_primitives::test]
async fn one_sweep_counts_every_acknowledgement_it_fails_against_its_receiver() {
    let registry = RemoteDispatchRegistry::with_capacity(8);
    let mut completions = Vec::new();
    for _ in 0..2 {
        let (acks, completion) = AckSet::root();
        let ack_id = registry
            .register_ack(receiving_node(), acks)
            .assured("the fixture has free correlation capacity");
        registry.admit_ack(ack_id);
        completions.push(completion);
    }
    for _ in 0..REMOTE_ACK_SILENT_SWEEPS {
        assert!(registry.fail_silent_acks().is_empty());
    }

    let failed = registry.fail_silent_acks();

    assert_eq!(failed.get(&receiving_node()), Some(&2));
    for completion in completions {
        let outcome = timeout(ASYNC_EVENT_FAILSAFE, completion.wait())
            .await
            .expect("every failed acknowledgement resolves its root");
        assert!(matches!(outcome, AckOutcome::NoAck(_)));
    }
}

#[nervix_primitives::test]
async fn a_report_restarts_the_silence_of_an_admitted_acknowledgement() {
    let forwarded = ForwardedAck::registered();
    forwarded.registry.admit_ack(forwarded.ack_id);
    forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);

    assert!(forwarded.registry.report_ack(forwarded.ack_id));

    forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);
    forwarded.sweep_failing_it();
}

#[nervix_primitives::test]
async fn an_acknowledgement_is_not_swept_before_its_delivery_is_admitted() {
    let forwarded = ForwardedAck::registered();
    forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);
    forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);

    let registration = RemoteAckRegistration {
        ack_id: forwarded.ack_id,
        registrar: ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-1").expect("the fixture node name is valid"),
            ClusterNodeIncarnation::new(1),
        ),
    };
    forwarded.registry.admit_acks(&[None, Some(registration)]);

    forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);
    forwarded.sweep_failing_it();
}

#[nervix_primitives::test]
async fn a_terminal_outcome_leaves_the_sweep_nothing_to_fail() {
    let forwarded = ForwardedAck::registered();
    forwarded.registry.admit_ack(forwarded.ack_id);
    forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);

    assert!(
        forwarded
            .registry
            .resolve_ack(forwarded.ack_id, AckOutcome::Ack)
    );
    forwarded.sweep_without_failing(1);

    assert_eq!(
        timeout(ASYNC_EVENT_FAILSAFE, forwarded.completion.wait())
            .await
            .expect("the receiver's outcome resolves the root"),
        AckOutcome::Ack
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn the_running_sweep_fails_an_acknowledgement_at_the_silence_bound() {
    let registry = Arc::new(RemoteDispatchRegistry::with_capacity(8));
    let (acks, completion) = AckSet::root();
    let ack_id = registry
        .register_ack(receiving_node(), acks)
        .assured("the fixture has free correlation capacity");
    registry.admit_ack(ack_id);
    let admitted_at = Instant::now();
    let sweeping = registry.clone();
    let sweeps = nervix_primitives::task::spawn(async move { sweeping.sweep_silent_acks().await });

    let outcome = completion.wait().await;
    let silent_for = admitted_at.elapsed();

    assert!(matches!(outcome, AckOutcome::NoAck(_)));
    assert!(
        silent_for >= REMOTE_ACK_SILENCE_TIMEOUT,
        "the acknowledgement failed after {silent_for:?} of silence"
    );
    assert!(
        silent_for <= REMOTE_ACK_SILENCE_TIMEOUT + REMOTE_ACK_SILENCE_SWEEP_INTERVAL,
        "the acknowledgement failed after {silent_for:?} of silence"
    );
    sweeps.abort();
}

#[nervix_primitives::test(start_paused = true)]
async fn the_running_sweep_keeps_an_acknowledgement_its_receiver_keeps_reporting() {
    let registry = Arc::new(RemoteDispatchRegistry::with_capacity(8));
    let (acks, completion) = AckSet::root();
    let ack_id = registry
        .register_ack(receiving_node(), acks)
        .assured("the fixture has free correlation capacity");
    registry.admit_ack(ack_id);
    let sweeping = registry.clone();
    let sweeps = nervix_primitives::task::spawn(async move { sweeping.sweep_silent_acks().await });

    for _ in 0..10 {
        sleep(Duration::from_secs(7)).await;
        assert!(
            registry.report_ack(ack_id),
            "an acknowledgement reported within the bound stays pending"
        );
    }
    assert!(registry.resolve_ack(ack_id, AckOutcome::Ack));
    assert_eq!(completion.wait().await, AckOutcome::Ack);
    sweeps.abort();
}

#[nervix_primitives::test(start_paused = true)]
async fn a_stalled_sweep_counts_once_for_the_time_it_missed() {
    let registry = Arc::new(RemoteDispatchRegistry::with_capacity(8));
    let (acks, _completion) = AckSet::root();
    let ack_id = registry
        .register_ack(receiving_node(), acks)
        .assured("the fixture has free correlation capacity");
    registry.admit_ack(ack_id);
    let sweeping = registry.clone();
    let sweeps = nervix_primitives::task::spawn(async move { sweeping.sweep_silent_acks().await });
    nervix_primitives::task::yield_now().await;

    // The node's execution stalls for four silence bounds, as a paused container's does, so no
    // report could have reached it.
    nervix_primitives::time::advance(REMOTE_ACK_SILENCE_TIMEOUT * 4).await;
    nervix_primitives::task::yield_now().await;

    assert!(
        registry.holds_ack(ack_id),
        "a sweep that ran late counts once rather than for every interval it missed"
    );
    sweeps.abort();
}

/// Runtime teardown cancels the silence sweeps while a sweep that overran its interval runs, so
/// the next tick is already due when that sweep returns. The sweeps still end there: teardown's
/// join waits for the sweep in progress and for no other, and the acknowledgement that sweep
/// left pending ends with the registry rather than by a later sweep.
///
/// A paused clock does not advance while a synchronous sweep runs, and a current-thread runtime
/// fires a timer only between polls, so no sweep here can make the next tick due itself. The
/// sweeps start a thousand intervals late instead and keep every tick they missed, so each tick
/// they reach is already due: the state an overrunning sweep leaves the node's own cadence in
/// when the node's other workers keep its timers current.
#[nervix_primitives::test(start_paused = true)]
async fn teardown_ends_the_silence_sweeps_after_the_overrunning_sweep_it_interrupts() {
    use nervix_primitives::{
        sync::atomic::{AtomicUsize, Ordering},
        time::{MissedTickBehavior, advance, interval},
    };

    use crate::runtime::remote_ack_owner::SilenceSweeps;

    let runtime = Runtime::default();
    let registry = runtime.inner.remote_dispatch.clone();
    let (acks, completion) = AckSet::root();
    let ack_id = registry
        .register_ack(receiving_node(), acks)
        .assured("the fixture has free correlation capacity");
    registry.admit_ack(ack_id);
    let mut ticks = interval(REMOTE_ACK_SILENCE_SWEEP_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Burst);
    advance(REMOTE_ACK_SILENCE_SWEEP_INTERVAL * 1000).await;
    let sweeps = Arc::new(AtomicUsize::new(0));
    let teardown = runtime.inner.remote_ack_watcher_shutdown.clone();
    let swept = sweeps.clone();
    let cancelling = teardown.clone();
    runtime.spawn_remote_ack_watcher_task(async move {
        SilenceSweeps::ticking(ticks)
            .run(|| {
                registry.fail_silent_acks();
                swept.fetch_add(1, Ordering::Relaxed);
                // Teardown cancels the sweeps while this sweep runs.
                cancelling.cancel();
            })
            .await;
    });
    teardown.cancelled().await;

    runtime.shutdown(runtime.domain_drain_timeout()).await;

    assert_eq!(
        sweeps.load(Ordering::Relaxed),
        1,
        "teardown cancelled the sweeps during their first sweep"
    );
    assert_eq!(
        completion.wait().await,
        AckOutcome::NoAck("remote correlation owner ended".to_string()),
        "the registry, not a later sweep, ends the acknowledgement the interrupted sweep kept"
    );
}

/// One fresh diagnostic process installs its detector before constructing the registry's
/// tracked position guards, then ends overrunning silence sweeps through runtime teardown.
#[cfg(feature = "deloxide")]
#[test]
fn deloxide_remote_ack_silence_sweeps() {
    let directory =
        std::env::var_os("NERVIX_DEADLOCK_EVIDENCE").map(nervix_deadlock::EvidenceDirectory::new);
    nervix_deadlock::DiagnosticRun::start(directory)
        .assured("the silence sweep diagnostic process starts its detector once");
    teardown_ends_the_silence_sweeps_after_the_overrunning_sweep_it_interrupts();
}
