//! Entity gate holds and one node's quiesce accounting, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The fencing, admission, counter and waiter invariants an entity gate hold and the
//!   node quiesce counters are held to while work is admitted, parked, resumed and released.
//! - **Depends on.** The entity gate types, the relay dispatch gate, and the server Shuttle runner.
//! - **Must not know.** What an entity is, what a relay carries, or what a work item does.

// The standard library's atomics are not Shuttle scheduling points, so each record below changes in
// the same scheduling step as the operation it records. The counters under test are Shuttle's
// atomics, so a check reads them while another task is between two of its own adjustments.
use std::{
    collections::BTreeSet,
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use ahash::RandomState;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::sync::DashMap;
use nervix_interconnect::EntityGatePurpose;
use nervix_models::{
    ClusterNodeName, CoordinationIdentity, DomainName, DomainNodeRef, ModelKind, ModelName,
    NodeRef, RelayName,
};
use tokio::{
    sync::{Notify, oneshot},
    time::{Duration, Instant},
};
use triomphe::Arc;

use super::{
    BranchQuiesceDepths, BranchQuiesceGauges, EntityAlterHold, EntityGateHold, EntityGateOperation,
    EntityGateOperationError, EntityGateScope, NodeQuiesceCounters, NodeQuiesceWorkGuard,
    OutputBufferQuiesceGauge, OwnershipHandoffFreezeWatch, RelayDispatchGate,
    RelayDispatchGateLease, Runtime,
};
use crate::shuttle_test::{check_pct, check_random};

const RANDOM_ITERATIONS: usize = 1_000;
const PCT_ITERATIONS: usize = 1_000;
const PCT_DEPTH: usize = 3;

const CHECK_TASK_JOINS: &str =
    "a check task that panics fails the execution before its join returns";

/// Relays one entity gate hold fences together.
const FENCED_RELAYS: usize = 2;
/// Batches one intake task publishes into its relay.
const PUBLISHED_BATCHES: usize = 2;
/// Reads the drain observer takes of the node's counters while one work item is live.
const DRAIN_OBSERVATIONS: usize = 3;
/// Reads the gauge observer takes of the node's counters while the gauges are still adjusting them.
const GAUGE_OBSERVATIONS: usize = 3;
/// Rounds the parking work item spends waiting for and resuming from materialized state.
const MATERIALIZED_ROUNDS: usize = 2;
/// Batches the output-buffer gauge holds at once at the deepest point of its sequence.
const DEEPEST_BUFFERED_BATCHES: usize = 2;
/// The deepest total the branch gauge check publishes across its three depths at once.
const DEEPEST_PUBLISHED_DEPTHS: usize = 3;
/// One work item counts once in the total its node holds, parked or admitted.
const DEEPEST_WORK_ITEM_COUNTS: usize = 1;
/// Everything the gauge check holds at once. A larger reading means a withdrawal took one count
/// below zero and wrapped.
const MOST_WORK_THE_GAUGE_CHECK_HOLDS: usize =
    DEEPEST_BUFFERED_BATCHES + DEEPEST_PUBLISHED_DEPTHS + DEEPEST_WORK_ITEM_COUNTS;
/// Waiters that ask one entity gate operation for its outcome.
const ENGAGEMENT_WAITERS: usize = 2;
/// Tasks that try to take one entity gate operation's hold away for release.
const RELEASING_TASKS: usize = 2;
/// Tasks waiting for an ownership handoff to lift the freeze it raised on their entity.
const FREEZE_WAITERS: usize = 2;

/// Explores `invariant` under Shuttle's random scheduler and then under its PCT scheduler.
fn explore(invariant: fn()) {
    check_random(invariant, RANDOM_ITERATIONS);
    check_pct(invariant, PCT_ITERATIONS, PCT_DEPTH);
}

/// A fence deadline no check outlives, so only a release ends an engagement.
fn far_future_deadline() -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(86_400))
        .assured("the monotonic clock represents one day past its current reading")
}

fn relay_gates(count: usize) -> Vec<Arc<RelayDispatchGate>> {
    (0..count)
        .map(|_| Arc::new(RelayDispatchGate::new()))
        .collect()
}

/// Engages one hold over every relay gate, with no branch-scoped gate of its own.
fn engage(gates: &[Arc<RelayDispatchGate>], reason: &str) -> EntityGateHold {
    EntityGateHold {
        gates: gates
            .iter()
            .map(|gate| RelayDispatchGateLease::engage(gate.clone(), far_future_deadline(), reason))
            .collect(),
        branch_gates: Vec::new(),
    }
}

/// What the intake tasks and the hold report to each other about one execution.
#[derive(Debug, Default)]
struct HoldRecords {
    /// Raised once a hold's fence reports quiescence and lowered before that hold is released.
    quiescent: AtomicBool,
    /// Fences that reported quiescence across the whole execution.
    completed_fences: AtomicUsize,
}

impl HoldRecords {
    fn fence_completed(&self) {
        self.completed_fences.fetch_add(1, Ordering::SeqCst);
        self.quiescent.store(true, Ordering::SeqCst);
    }

    fn releasing(&self) {
        self.quiescent.store(false, Ordering::SeqCst);
    }

    /// Records that a relay the hold fences granted a dispatch and admitted one work item.
    fn work_admitted(&self) {
        assert!(
            !self.quiescent.load(Ordering::SeqCst),
            "a hold whose fence reported quiescence admitted new work into its relay"
        );
    }
}

/// Publishes `batches` into `gate`, admitting each as node work the way a relay boundary does.
async fn publish_batches(
    gate: Arc<RelayDispatchGate>,
    counters: Arc<NodeQuiesceCounters>,
    records: StdArc<HoldRecords>,
    batches: usize,
) {
    for _ in 0..batches {
        tokio::task::consume_budget().await;
        let permit = gate.acquire_dispatch().await;
        records.work_admitted();
        let mut work = NodeQuiesceWorkGuard::begin(counters.clone());
        records.work_admitted();
        drop(permit);
        tokio::task::yield_now().await;
        work.park_for_required_materialized_state();
        tokio::task::yield_now().await;
        work.resume_from_required_materialized_state();
        drop(work);
    }
}

/// Holds every relay gate until `release` arrives, then engages a second hold and releases it.
async fn hold_twice(
    gates: Vec<Arc<RelayDispatchGate>>,
    records: StdArc<HoldRecords>,
    quiescent: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
) {
    let mut first = engage(&gates, "shuttle entity hold");
    assert!(
        first.wait_quiescent().await,
        "a far-future fence completes once every earlier dispatch drops its permit"
    );
    records.fence_completed();
    quiescent
        .send(())
        .assured("the observer waits for the first hold to report quiescence");
    release
        .await
        .assured("the observer releases the first hold before joining");
    records.releasing();
    first.release();

    let mut second = engage(&gates, "shuttle entity re-engagement");
    assert!(
        second.wait_quiescent().await,
        "a hold re-engaged after a release fences the same relays again"
    );
    records.fence_completed();
    records.releasing();
    drop(second);
}

fn a_hold_fences_every_relay_it_names() {
    shuttle::future::block_on(async {
        let gates = relay_gates(FENCED_RELAYS);
        let counters = Arc::new(NodeQuiesceCounters::default());
        let records = StdArc::new(HoldRecords::default());
        let (quiescent, first_hold_is_quiescent) = oneshot::channel();
        let (release, first_hold_is_released) = oneshot::channel();

        let intakes = gates
            .iter()
            .map(|gate| {
                tokio::spawn(publish_batches(
                    gate.clone(),
                    counters.clone(),
                    records.clone(),
                    PUBLISHED_BATCHES,
                ))
            })
            .collect::<Vec<_>>();
        let holder = tokio::spawn(hold_twice(
            gates.clone(),
            records.clone(),
            quiescent,
            first_hold_is_released,
        ));

        first_hold_is_quiescent
            .await
            .assured("the hold reports quiescence before it waits for its release");
        for gate in &gates {
            assert!(
                gate.is_closed(),
                "every relay the quiescent hold names stays closed until it is released"
            );
        }
        // Parking and resuming move a work item between the counts without admitting one, so the
        // total this node holds is what a quiescent hold keeps from rising.
        let mut outstanding = counters.outstanding_work();
        for _ in 0..DRAIN_OBSERVATIONS {
            tokio::task::consume_budget().await;
            tokio::task::yield_now().await;
            let still_outstanding = counters.outstanding_work();
            assert!(
                still_outstanding <= outstanding,
                "a quiescent hold let its node take in work: {outstanding} rose to \
                 {still_outstanding}"
            );
            outstanding = still_outstanding;
        }
        release
            .send(())
            .assured("the hold waits for this release before re-engaging");

        for intake in intakes {
            tokio::task::consume_budget().await;
            intake.await.assured(CHECK_TASK_JOINS);
        }
        holder.await.assured(CHECK_TASK_JOINS);

        assert_eq!(
            records.completed_fences.load(Ordering::SeqCst),
            2,
            "the hold fences its relays once before its release and once after it re-engages"
        );
        for gate in &gates {
            assert!(
                !gate.is_closed(),
                "the released and dropped holds reopen every relay they fenced"
            );
            assert_eq!(
                gate.in_flight_dispatches(),
                0,
                "every permit and rolled-back acquisition returns its dispatch count"
            );
        }
        assert_eq!(
            counters.outstanding_work(),
            0,
            "every admitted work item withdrew its count"
        );
        assert_eq!(
            counters.admitted_work(),
            0,
            "every admitted work item withdrew its count"
        );
    });
}

#[test]
fn shuttle_an_entity_gate_hold_fences_every_relay_and_admits_no_work_until_it_is_released() {
    explore(a_hold_fences_every_relay_it_names);
}

/// Holds one work item across a park and resume for as long as the drain observer is reading.
async fn park_and_resume(
    counters: Arc<NodeQuiesceCounters>,
    admitted: oneshot::Sender<()>,
    observed: oneshot::Receiver<()>,
) {
    let mut work = NodeQuiesceWorkGuard::begin(counters);
    admitted
        .send(())
        .assured("the drain observer waits for this work item to be admitted");
    for _ in 0..MATERIALIZED_ROUNDS {
        tokio::task::consume_budget().await;
        work.park_for_required_materialized_state();
        tokio::task::yield_now().await;
        work.resume_from_required_materialized_state();
    }
    observed
        .await
        .assured("the drain observer finishes its reads before this work item is released");
    drop(work);
}

/// Reads the node's counters the way a drain does while one work item is known to be live.
async fn observe_drain(
    counters: Arc<NodeQuiesceCounters>,
    admitted: oneshot::Receiver<()>,
    observed: oneshot::Sender<()>,
) {
    admitted
        .await
        .assured("the parking work item reports its admission before this observer reads");
    for _ in 0..DRAIN_OBSERVATIONS {
        tokio::task::consume_budget().await;
        assert_ne!(
            counters.outstanding_work(),
            0,
            "a node still holding one admitted work item reported that it holds none"
        );
        tokio::task::yield_now().await;
    }
    observed
        .send(())
        .assured("the parking work item waits for this observer before it is released");
}

fn a_parked_work_item_is_never_missing_from_the_counters() {
    shuttle::future::block_on(async {
        let counters = Arc::new(NodeQuiesceCounters::default());
        let (admitted, work_is_admitted) = oneshot::channel();
        let (observed, drain_is_observed) = oneshot::channel();

        let parking = tokio::spawn(park_and_resume(
            counters.clone(),
            admitted,
            drain_is_observed,
        ));
        let observer = tokio::spawn(observe_drain(counters.clone(), work_is_admitted, observed));

        parking.await.assured(CHECK_TASK_JOINS);
        observer.await.assured(CHECK_TASK_JOINS);

        assert_eq!(
            counters.outstanding_work(),
            0,
            "the released work item withdrew its count"
        );
        assert_eq!(
            counters.admitted_work(),
            0,
            "the released work item withdrew its count"
        );
    });
}

#[test]
fn shuttle_a_work_item_parked_for_materialized_state_is_never_missing_from_a_drain() {
    explore(a_parked_work_item_is_never_missing_from_the_counters);
}

/// Fills and empties one task's output buffers, then withdraws what it still holds by dropping.
async fn buffer_output_batches(counters: Arc<NodeQuiesceCounters>) {
    let mut gauge = OutputBufferQuiesceGauge::new(counters);
    gauge.add_batch();
    tokio::task::yield_now().await;
    gauge.add_batch();
    tokio::task::yield_now().await;
    gauge.remove_batches(1);
    tokio::task::yield_now().await;
    gauge.add_batch();
    tokio::task::yield_now().await;
    gauge.remove_batches(2);
    tokio::task::yield_now().await;
    gauge.add_batch();
    drop(gauge);
}

/// Publishes one processor's depths the way a branch task republishes them after every step.
async fn republish_branch_depths(counters: Arc<NodeQuiesceCounters>) {
    let mut gauges = BranchQuiesceGauges::new(counters);
    for depths in [
        BranchQuiesceDepths {
            collected_inputs: 2,
            pending_materialized: 0,
            output_buffers: 0,
        },
        BranchQuiesceDepths {
            collected_inputs: 1,
            pending_materialized: 1,
            output_buffers: 1,
        },
        BranchQuiesceDepths {
            collected_inputs: 0,
            pending_materialized: 0,
            output_buffers: 2,
        },
    ] {
        tokio::task::consume_budget().await;
        gauges.publish(depths);
        tokio::task::yield_now().await;
    }
    drop(gauges);
}

/// Runs one work item through a park, a resume and its release beside the two gauges.
async fn admit_and_park_once(counters: Arc<NodeQuiesceCounters>) {
    let mut work = NodeQuiesceWorkGuard::begin(counters);
    tokio::task::yield_now().await;
    work.park_for_required_materialized_state();
    tokio::task::yield_now().await;
    work.resume_from_required_materialized_state();
    tokio::task::yield_now().await;
    drop(work);
}

/// Reads the node's counters while the gauges and the work item are still adjusting them.
async fn observe_gauge_bounds(counters: Arc<NodeQuiesceCounters>) {
    for _ in 0..GAUGE_OBSERVATIONS {
        tokio::task::consume_budget().await;
        assert!(
            counters.outstanding_work() <= MOST_WORK_THE_GAUGE_CHECK_HOLDS,
            "a withdrawn count fell below zero and wrapped"
        );
        tokio::task::yield_now().await;
    }
}

fn every_gauge_withdraws_exactly_what_it_contributed() {
    shuttle::future::block_on(async {
        let counters = Arc::new(NodeQuiesceCounters::default());

        let buffering = tokio::spawn(buffer_output_batches(counters.clone()));
        let republishing = tokio::spawn(republish_branch_depths(counters.clone()));
        let admitting = tokio::spawn(admit_and_park_once(counters.clone()));
        let observer = tokio::spawn(observe_gauge_bounds(counters.clone()));

        buffering.await.assured(CHECK_TASK_JOINS);
        republishing.await.assured(CHECK_TASK_JOINS);
        admitting.await.assured(CHECK_TASK_JOINS);
        observer.await.assured(CHECK_TASK_JOINS);

        assert_eq!(
            counters.outstanding_work(),
            0,
            "every buffered batch, published depth and work item withdrew its count"
        );
        assert_eq!(
            counters.admitted_work(),
            0,
            "every buffered batch, published depth and work item withdrew its count"
        );
        assert_eq!(
            counters.outstanding_work_for(EntityGatePurpose::OwnershipHandoff),
            0,
            "an ownership handoff sees no work left once every count has been withdrawn"
        );
    });
}

#[test]
fn shuttle_every_node_quiesce_gauge_withdraws_exactly_what_it_contributed() {
    explore(every_gauge_withdraws_exactly_what_it_contributed);
}

fn coordination() -> CoordinationIdentity {
    CoordinationIdentity::new(
        ClusterNodeName::parse("coordinator-a").assured("the check names a valid cluster node"),
        7,
        11,
    )
}

fn scope(domain: &DomainName, relay: &RelayName) -> EntityGateScope {
    EntityGateScope::new(
        domain,
        std::slice::from_ref(relay),
        &[NodeRef {
            kind: ModelKind::Relay,
            identifier: ModelName::from(relay),
        }],
        EntityGatePurpose::ModelAlteration,
    )
}

/// What the waiters and takers of one entity gate operation report about its outcome.
#[derive(Debug, Default)]
struct OperationRecords {
    /// Waiters that observed the completed engagement.
    held: AtomicUsize,
    /// Waiters that woke without a hold: the engagement failed, or its hold had already been
    /// taken for release.
    released: AtomicUsize,
    /// Tasks that took the hold away and released it.
    takers: AtomicUsize,
}

async fn wait_for_engagement(
    operation: Arc<EntityGateOperation>,
    records: StdArc<OperationRecords>,
) {
    match operation.wait_until_held().await {
        Ok(()) => {
            records.held.fetch_add(1, Ordering::SeqCst);
        }
        Err(report) => {
            assert!(
                matches!(report.current_context(), EntityGateOperationError::Released),
                "a completed engagement only ever becomes released: {report:?}"
            );
            records.released.fetch_add(1, Ordering::SeqCst);
        }
    }
}

async fn take_and_release(operation: Arc<EntityGateOperation>, records: StdArc<OperationRecords>) {
    let Some(hold) = operation.take_hold().await else {
        return;
    };
    records.takers.fetch_add(1, Ordering::SeqCst);
    hold.gates.release();
}

fn one_engagement_wakes_every_waiter_and_is_taken_once() {
    shuttle::future::block_on(async {
        let domain = DomainName::parse("default").assured("the check names a valid domain");
        let relay = RelayName::parse("events").assured("the check names a valid relay");
        let gate = Arc::new(RelayDispatchGate::new());
        let operation = Arc::new(EntityGateOperation::new(scope(&domain, &relay)));
        let records = StdArc::new(OperationRecords::default());

        let waiters = (0..ENGAGEMENT_WAITERS)
            .map(|_| tokio::spawn(wait_for_engagement(operation.clone(), records.clone())))
            .collect::<Vec<_>>();
        let releasers = (0..RELEASING_TASKS)
            .map(|_| tokio::spawn(take_and_release(operation.clone(), records.clone())))
            .collect::<Vec<_>>();

        let engaging = operation.clone();
        let engaged_gate = gate.clone();
        let engagement = tokio::spawn(async move {
            tokio::task::yield_now().await;
            engaging.complete(EntityAlterHold {
                coordination: coordination(),
                gates: engage(std::slice::from_ref(&engaged_gate), "shuttle engagement"),
                affected_entities: Vec::new(),
                purpose: EntityGatePurpose::ModelAlteration,
                quiesced_ingestors: Vec::new(),
            });
        });

        engagement.await.assured(CHECK_TASK_JOINS);
        for waiter in waiters {
            tokio::task::consume_budget().await;
            waiter.await.assured(CHECK_TASK_JOINS);
        }
        for releaser in releasers {
            tokio::task::consume_budget().await;
            releaser.await.assured(CHECK_TASK_JOINS);
        }

        assert_eq!(
            records.takers.load(Ordering::SeqCst),
            1,
            "exactly one release takes a completed engagement's hold"
        );
        let held = records.held.load(Ordering::SeqCst);
        let released = records.released.load(Ordering::SeqCst);
        assert_eq!(
            held.checked_add(released)
                .assured("both counts total the waiters this check spawned"),
            ENGAGEMENT_WAITERS,
            "every waiter wakes with an outcome once the engagement completes and is taken"
        );
        assert!(
            !operation.is_held(),
            "an operation whose hold was taken no longer reports it"
        );
        assert!(
            !gate.is_closed(),
            "releasing the taken hold reopens the relay its engagement fenced"
        );
    });
}

#[test]
fn shuttle_every_engagement_waiter_wakes_and_exactly_one_release_takes_the_hold() {
    explore(one_engagement_wakes_every_waiter_and_is_taken_once);
}

/// Acquires and drops one dispatch permit, which the engaged hold parks until it is dropped.
async fn dispatch_once(gate: Arc<RelayDispatchGate>) {
    tokio::task::consume_budget().await;
    let permit = gate.acquire_dispatch().await;
    tokio::task::yield_now().await;
    drop(permit);
}

fn a_hold_dropped_before_its_fence_completes_reopens_every_relay() {
    shuttle::future::block_on(async {
        let gates = relay_gates(FENCED_RELAYS);
        let dispatchers = gates
            .iter()
            .map(|gate| tokio::spawn(dispatch_once(gate.clone())))
            .collect::<Vec<_>>();

        let dropping = gates.clone();
        let holder = tokio::spawn(async move {
            let hold = engage(&dropping, "shuttle abandoned hold");
            tokio::task::yield_now().await;
            drop(hold);
        });

        holder.await.assured(CHECK_TASK_JOINS);
        for dispatcher in dispatchers {
            tokio::task::consume_budget().await;
            dispatcher.await.assured(CHECK_TASK_JOINS);
        }

        for gate in &gates {
            assert!(
                !gate.is_closed(),
                "a hold dropped before its fence completed still reopens every relay it engaged"
            );
            assert_eq!(
                gate.in_flight_dispatches(),
                0,
                "every permit and rolled-back acquisition returns its dispatch count"
            );
        }
    });
}

#[test]
fn shuttle_a_hold_dropped_before_its_fence_completes_reopens_every_relay_it_engaged() {
    explore(a_hold_dropped_before_its_fence_completes_reopens_every_relay);
}

fn one_failed_engagement_wakes_every_waiter_with_its_failure() {
    shuttle::future::block_on(async {
        let domain = DomainName::parse("default").assured("the check names a valid domain");
        let relay = RelayName::parse("events").assured("the check names a valid relay");
        let operation = Arc::new(EntityGateOperation::new(scope(&domain, &relay)));
        let records = StdArc::new(OperationRecords::default());

        let waiters = (0..ENGAGEMENT_WAITERS)
            .map(|_| {
                let operation = operation.clone();
                let records = records.clone();
                tokio::spawn(async move {
                    let Err(report) = operation.wait_until_held().await else {
                        panic!("a failed engagement never reports a hold");
                    };
                    assert!(
                        matches!(
                            report.current_context(),
                            EntityGateOperationError::RelayFenceDeadline { .. }
                        ),
                        "a waiter reports the failure the engagement recorded: {report:?}"
                    );
                    records.released.fetch_add(1, Ordering::SeqCst);
                })
            })
            .collect::<Vec<_>>();
        let releasers = (0..RELEASING_TASKS)
            .map(|_| tokio::spawn(take_and_release(operation.clone(), records.clone())))
            .collect::<Vec<_>>();

        let failing = operation.clone();
        let failing_domain = domain.clone();
        let engagement = tokio::spawn(async move {
            tokio::task::yield_now().await;
            failing.fail(EntityGateOperationError::RelayFenceDeadline {
                domain: failing_domain,
            });
        });

        engagement.await.assured(CHECK_TASK_JOINS);
        for waiter in waiters {
            tokio::task::consume_budget().await;
            waiter.await.assured(CHECK_TASK_JOINS);
        }
        for releaser in releasers {
            tokio::task::consume_budget().await;
            releaser.await.assured(CHECK_TASK_JOINS);
        }

        assert_eq!(
            records.released.load(Ordering::SeqCst),
            ENGAGEMENT_WAITERS,
            "every waiter wakes with the failure once the engagement records it"
        );
        assert_eq!(
            records.takers.load(Ordering::SeqCst),
            0,
            "a failed engagement has no hold to take"
        );
        assert!(
            !operation.is_held(),
            "a failed engagement never reports a hold"
        );
    });
}

#[test]
fn shuttle_a_failed_engagement_wakes_every_waiter_with_its_failure() {
    explore(one_failed_engagement_wakes_every_waiter_with_its_failure);
}

/// Waits the way a branch task does: register for the next change, then read the freeze.
///
/// A release that notified before it lifted the freeze would wake this loop into a reread that
/// still sees the entity frozen, and the wait it registers next has nothing left to wake it.
async fn wait_until_thawed(watch: Arc<OwnershipHandoffFreezeWatch>) {
    loop {
        tokio::task::consume_budget().await;
        let freeze = watch.observe();
        if !freeze.is_frozen() {
            return;
        }
        freeze.changed().await;
    }
}

fn releasing_an_ownership_handoff_wakes_every_frozen_waiter() {
    shuttle::future::block_on(async {
        let domain = DomainName::parse("default").assured("the check names a valid domain");
        let relay = RelayName::parse("events").assured("the check names a valid relay");
        let entity = NodeRef {
            kind: ModelKind::Relay,
            identifier: ModelName::from(&relay),
        };
        let key = DomainNodeRef::node_in(domain.clone(), entity.kind, entity.identifier.clone());
        let frozen_entities: Arc<
            DashMap<DomainNodeRef, BTreeSet<CoordinationIdentity>, RandomState>,
        > = Arc::new(DashMap::default());
        let changed = Arc::new(Notify::new());
        frozen_entities
            .entry(key.clone())
            .or_default()
            .insert(coordination());
        let gate = Arc::new(RelayDispatchGate::new());

        let watch = Arc::new(OwnershipHandoffFreezeWatch::over(
            frozen_entities.clone(),
            changed.clone(),
            key.clone(),
        ));
        let waiters = (0..FREEZE_WAITERS)
            .map(|_| tokio::spawn(wait_until_thawed(watch.clone())))
            .collect::<Vec<_>>();

        let releasing_entities = frozen_entities.clone();
        let releasing_changed = changed.clone();
        let releasing_domain = domain.clone();
        let released_gate = gate.clone();
        let release = tokio::spawn(async move {
            let ingestors = DashMap::default();
            let ingestor_quiescence = DashMap::default();
            Runtime::release_entity_alter_hold(
                &ingestors,
                &ingestor_quiescence,
                &releasing_entities,
                &releasing_changed,
                &releasing_domain,
                EntityAlterHold {
                    coordination: coordination(),
                    gates: engage(
                        std::slice::from_ref(&released_gate),
                        "shuttle ownership handoff",
                    ),
                    affected_entities: vec![entity],
                    purpose: EntityGatePurpose::OwnershipHandoff,
                    quiesced_ingestors: Vec::new(),
                },
            )
            .await;
        });

        release.await.assured(CHECK_TASK_JOINS);
        for waiter in waiters {
            tokio::task::consume_budget().await;
            waiter.await.assured(CHECK_TASK_JOINS);
        }

        assert!(
            frozen_entities.is_empty(),
            "releasing the only hold lifts the freeze it raised"
        );
        assert!(
            !gate.is_closed(),
            "releasing the hold reopens the relay it fenced"
        );
    });
}

#[test]
fn shuttle_releasing_an_ownership_handoff_wakes_every_waiter_frozen_by_it() {
    explore(releasing_an_ownership_handoff_wakes_every_frozen_waiter);
}
