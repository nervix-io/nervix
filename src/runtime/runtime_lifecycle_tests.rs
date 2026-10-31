//! Runtime teardown tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests for how long stopping one domain's execution may take when its tasks
//!   outlast their stop grace, and for the tasks a stopping runtime waits for after it.
//! - **Depends on.** The runtime lifecycle and its test fixtures.
//! - **Must not know.** Production control-plane orchestration or external connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use ahash::HashMap;
use arrow_array::{ArrayRef, Int64Array};
use fjall::Database;
use nervix_execution::{CpuClass, Executor, MemoryClass};
use nervix_models::{
    DomainName, EmitterName, GeneratorName, ModelKind, NodeRef, ParseAsType, ReingestorName,
    RelayName, SchemaFingerprint, Timestamp,
};
use nervix_primitives::{
    sync::{StdArc, blocking, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

use super::*;
use crate::runtime_schema::{RuntimeValue, test_runtime_row};

/// How long a check waits for a task or a worker to reach the state it names.
const REACHED_WITHIN: Duration = Duration::from_secs(60);

/// How many tasks of one kind each check leaves running past the stop. More than one is what
/// tells a grace the tasks share from a grace each of them is given in turn.
const UNRESPONSIVE_TASKS: u32 = 5;

/// A task that never observes its domain's stop, as a generator blocked on a peer that is itself
/// stopping never does.
fn unresponsive_task() -> JoinHandle<()> {
    nervix_primitives::task::spawn(std::future::pending::<()>())
}

/// A domain execution that holds no task, for a check to add the tasks it stops.
fn idle_execution(domain: &DomainName) -> DomainExecution {
    let (shutdown, _) = watch::channel(false);
    DomainExecution {
        revision: test_execution_revision(domain, Vec::new()),
        start_version: 0,
        domain_clock: test_domain_clock(domain),
        shutdown,
        routing: DomainRouting::new(DomainRoutingSnapshot::default()),

        branched_entrypoints: HashMap::default(),
        endpoint_routes: HashMap::default(),
        node_tasks: HashMap::default(),
        emitter_tasks: HashMap::default(),
        generator_tasks: HashMap::default(),
        reingestor_tasks: HashMap::default(),
        placement_tasks: HashMap::default(),
        relay_state_tasks: HashMap::default(),
        relay_owner_tasks: HashMap::default(),
        tasks: Vec::new(),
    }
}

/// How long stopping `execution` takes on the paused clock, which advances only while every task
/// of the test waits.
async fn stop_duration(
    runtime: &Runtime,
    domain: &DomainName,
    execution: DomainExecution,
) -> Duration {
    let started = Instant::now();
    runtime
        .stop_domain_execution(domain, execution, runtime.emitter_finish_deadline())
        .await;
    started.elapsed()
}

/// An emitter task that reports the deadline its stop signal holds once its domain signals the stop,
/// which is what an emitter task reads when teardown ends its loop.
fn deadline_reporting_emitter(
    execution: &DomainExecution,
    report: oneshot::Sender<Option<Instant>>,
) -> ScheduledEmitterTask {
    let (commands, _) = mpsc::channel(1);
    let (stop_signal, stop_rx) = watch::channel(None);
    let mut stopped = execution.shutdown.subscribe();
    let task = nervix_primitives::task::spawn(async move {
        stopped
            .wait_for(|stopped| *stopped)
            .await
            .expect("the domain signals its stop before it drops the sender");
        let deadline = *stop_rx.borrow();
        report
            .send(deadline)
            .expect("the check waits for the deadline the emitter read");
    });
    ScheduledEmitterTask {
        commands,
        stop_signal,
        task,
    }
}

/// A domain execution with one emitter task that reports the finish deadline it read when its
/// domain stopped.
fn execution_with_deadline_reporting_emitter(
    domain: &DomainName,
) -> (DomainExecution, oneshot::Receiver<Option<Instant>>) {
    let mut execution = idle_execution(domain);
    let (report, reported) = oneshot::channel();
    let emitter = named::<EmitterName>("published");
    let task = deadline_reporting_emitter(&execution, report);
    execution
        .emitter_tasks
        .insert(NodeRef::new(ModelKind::Emitter, &emitter), task);
    (execution, reported)
}

#[nervix_primitives::test(start_paused = true)]
async fn a_stopping_execution_gives_each_emitter_its_finish_deadline_before_its_stop() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let (execution, reported) = execution_with_deadline_reporting_emitter(&domain);
    let finish_by = Instant::now() + Duration::from_secs(7);

    runtime
        .stop_domain_execution(&domain, execution, finish_by)
        .await;

    assert_eq!(
        reported
            .await
            .expect("the emitter task reports what it read"),
        Some(finish_by),
        "an emitter that the stop ends must already hold the deadline it finishes its emitter by"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_stopping_runtime_gives_its_emitters_only_the_time_its_shutdown_leaves() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let (execution, reported) = execution_with_deadline_reporting_emitter(&domain);
    runtime.inner.executions.insert(domain.clone(), execution);
    let left = Duration::from_secs(3);
    let started = Instant::now();

    runtime.shutdown(left).await;

    assert_eq!(
        reported
            .await
            .expect("the emitter task reports what it read"),
        Some(started + left),
        "a shutdown with less time left than the domain drain timeout bounds the finish by it"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn a_stopping_runtime_gives_its_emitters_no_more_than_the_domain_drain_timeout() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let (execution, reported) = execution_with_deadline_reporting_emitter(&domain);
    runtime.inner.executions.insert(domain.clone(), execution);
    let started = Instant::now();

    runtime.shutdown(Duration::from_secs(3_600)).await;

    assert_eq!(
        reported
            .await
            .expect("the emitter task reports what it read"),
        Some(started + runtime.domain_drain_timeout()),
        "a long shutdown deadline still gives an emitter's finish only the domain drain timeout"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn generators_that_outlast_their_stop_grace_share_one_grace() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let mut execution = idle_execution(&domain);
    for index in 0..UNRESPONSIVE_TASKS {
        let generator = named::<GeneratorName>(&format!("produce_{index}"));
        execution.generator_tasks.insert(
            NodeRef::new(ModelKind::Generator, &generator),
            unresponsive_task(),
        );
    }

    let elapsed = stop_duration(&runtime, &domain, execution).await;

    assert_eq!(
        elapsed, TASK_STOP_GRACE,
        "{UNRESPONSIVE_TASKS} generators that never stop must cost the domain one stop grace, not \
         one each"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn reingestor_tasks_that_outlast_their_stop_grace_share_one_grace() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let mut execution = idle_execution(&domain);
    for index in 0..UNRESPONSIVE_TASKS {
        let reingestor = named::<ReingestorName>(&format!("reroute_{index}"));
        execution.reingestor_tasks.insert(
            NodeRef::new(ModelKind::Reingestor, &reingestor),
            vec![unresponsive_task(), unresponsive_task()],
        );
    }

    let elapsed = stop_duration(&runtime, &domain, execution).await;

    assert_eq!(
        elapsed, TASK_STOP_GRACE,
        "every task of {UNRESPONSIVE_TASKS} reingestors that never stop must share one stop grace"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn task_kinds_stop_in_turn_so_each_kind_keeps_its_own_grace() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let mut execution = idle_execution(&domain);
    let generator = named::<GeneratorName>("produce");
    execution.generator_tasks.insert(
        NodeRef::new(ModelKind::Generator, &generator),
        unresponsive_task(),
    );
    let reingestor = named::<ReingestorName>("reroute");
    execution.reingestor_tasks.insert(
        NodeRef::new(ModelKind::Reingestor, &reingestor),
        vec![unresponsive_task()],
    );

    let elapsed = stop_duration(&runtime, &domain, execution).await;

    assert_eq!(
        elapsed,
        TASK_STOP_GRACE * 2,
        "a generator and a reingestor that never stop belong to two kinds, each with its grace"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn tasks_that_stop_at_once_cost_no_grace() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let mut execution = idle_execution(&domain);
    let mut stopped = execution.shutdown.subscribe();
    for index in 0..UNRESPONSIVE_TASKS {
        let generator = named::<GeneratorName>(&format!("produce_{index}"));
        let mut stopped = stopped.clone();
        execution.generator_tasks.insert(
            NodeRef::new(ModelKind::Generator, &generator),
            nervix_primitives::task::spawn(async move {
                stopped
                    .wait_for(|stopped| *stopped)
                    .await
                    .expect("the domain signals its stop before it drops the sender");
            }),
        );
    }

    let elapsed = stop_duration(&runtime, &domain, execution).await;

    assert_eq!(
        elapsed,
        Duration::ZERO,
        "generators that end on the domain's stop signal must not wait out a grace"
    );
    assert!(
        *stopped.borrow_and_update(),
        "stopping the execution publishes its stop to every task"
    );
}

/// A runtime that persists its state, with one window a branch has published and not yet written.
struct PublishedWindow {
    runtime: Runtime,
    placement: RuntimeStatePlacement,
    state: Arc<ReplicatedWindowProcessorState>,
    _directory: tempfile::TempDir,
}

impl PublishedWindow {
    async fn new() -> Self {
        let directory = tempfile::tempdir().expect("the test host provides a temporary directory");
        let database = Database::builder(directory.path())
            .open()
            .expect("a new directory opens as a database");
        let runtime = Runtime::with_persistence(Some(database), Duration::from_secs(3_600))
            .expect("the runtime opens its state store");
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeState::WindowProcessor {
                schema: SchemaFingerprint::from_digest([8; 32]),
            },
            kind: ModelKind::WindowProcessor,
            identifier: named("latency_window"),
            unit: nervix_models::Unit::Single,
            branch_key: None,
        };
        let state = runtime
            .replicated_window_processor_state(placement.clone())
            .expect("the window state initializes");
        let mut window = TestWindow::new(
            "SET total = SUM(input.latency)",
            &[OptionalTestField {
                name: "latency",
                ty: ParseAsType::I64,
                optional: false,
            }],
            &[OptionalTestField {
                name: "total",
                ty: ParseAsType::I64,
                optional: true,
            }],
        );
        let latencies: ArrayRef = StdArc::new(Int64Array::from(vec![10, 20]));
        let refused = window.admit(vec![latencies], &[1, 2]).await;
        assert!(refused.is_empty(), "the window admits both rows");
        state
            .replace_state(&window.state)
            .expect("the branch publishes its window");
        Self {
            runtime,
            placement,
            state,
            _directory: directory,
        }
    }

    /// The revision of this window the runtime's state store holds.
    fn persisted_revision(&self) -> Option<u64> {
        let persisted = self
            .runtime
            .inner
            .state_store
            .as_ref()
            .expect("the test runtime has a state store")
            .latest_snapshot(&self.placement)
            .expect("the snapshot lookup succeeds");
        persisted.map(|snapshot| snapshot.lsm)
    }
}

/// Holds every bulk worker of `executor` until the returned holders are dropped, so a job that
/// needs one, such as encoding a published window, waits for the test to give one back.
async fn hold_every_bulk_worker(executor: &Executor) -> Vec<blocking::mpsc::Sender<()>> {
    let workers = executor.snapshot().bulk_cpu.workers;
    let mut holders = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (holder, held) = blocking::mpsc::channel::<()>();
        holders.push(holder);
        let reservation = executor
            .try_reserve(MemoryClass::Bulk, 0)
            .expect("bulk admission accepts a zero charge");
        let occupying = executor.clone();
        nervix_primitives::task::spawn(async move {
            occupying
                .run_cpu(CpuClass::Bulk, reservation, move |_, _| {
                    held.recv()
                        .discarded("dropping the holder is how the test releases this worker");
                })
                .await
                .discarded("the job only holds a worker until the test releases it");
        });
    }
    timeout(REACHED_WITHIN, async {
        while executor.snapshot().bulk_cpu.running < workers {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("every bulk worker is held");
    holders
}

/// Starts the snapshot task of `window`'s branch and ends that branch the way a stop grace does:
/// its task is dropped with everything it held, without telling its snapshot task anything. With
/// every bulk worker held, the snapshot task's last write then waits for one.
async fn end_branch_at_its_grace(window: &PublishedWindow) {
    let (branch_stop, _) = watch::channel(false);
    let (snapshot_requests, branch_requests) = mpsc::channel(1);
    let snapshot_task = window
        .runtime
        .spawn_published_branch_state_snapshot_task(
            &branch_stop,
            PublishedBranchState::WindowProcessor(window.state.clone()),
            snapshot_requests,
        )
        .expect("a persisting runtime starts a snapshot task");
    drop(branch_requests);
    drop(branch_stop);
    drop(snapshot_task);
    let executor = window.runtime.executor().clone();
    timeout(REACHED_WITHIN, async {
        while executor.snapshot().bulk_cpu.pending == 0 {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("the snapshot task's write waits for a bulk worker");
}

/// A window branch whose task was ended at its stop grace leaves its snapshot task writing what
/// the branch published last. That task holds the runtime and its storage, so a runtime that stops
/// waits for it: here the write waits for a bulk worker, and the runtime finishes stopping only
/// once the worker is given back and the window is on disk.
#[nervix_primitives::test]
async fn a_stopping_runtime_waits_for_the_snapshot_of_a_branch_whose_task_is_gone() {
    let window = PublishedWindow::new().await;
    let runtime = window.runtime.clone();
    let holders = hold_every_bulk_worker(runtime.executor()).await;
    end_branch_at_its_grace(&window).await;

    let stopping = nervix_primitives::task::spawn({
        let runtime = runtime.clone();
        async move {
            let left = runtime.domain_drain_timeout();
            runtime.shutdown(left).await
        }
    });
    let told_to_stop = runtime.inner.state_snapshot_tasks.stopping();
    timeout(REACHED_WITHIN, told_to_stop.cancelled())
        .await
        .expect("the stopping runtime reaches its snapshot tasks");
    assert!(
        !stopping.is_finished(),
        "the runtime stopped while a snapshot task still held its storage"
    );
    assert_eq!(window.persisted_revision(), None);

    drop(holders);
    timeout(REACHED_WITHIN, stopping)
        .await
        .expect("the runtime stops once the snapshot is written")
        .expect("the stopping task does not panic");
    assert_eq!(
        window.persisted_revision(),
        Some(1),
        "the window the branch published is on disk when the runtime has stopped"
    );
}

/// A snapshot task whose branch never tells it to stop still ends with its runtime: the stopping
/// runtime tells it to write what the branch published last, and waits for that write.
#[nervix_primitives::test]
async fn a_stopping_runtime_has_the_snapshot_task_of_a_silent_branch_write_and_end() {
    let window = PublishedWindow::new().await;
    let runtime = window.runtime.clone();
    let (branch_stop, _) = watch::channel(false);
    let (snapshot_requests, _branch_requests) = mpsc::channel(1);
    let snapshot_task = runtime
        .spawn_published_branch_state_snapshot_task(
            &branch_stop,
            PublishedBranchState::WindowProcessor(window.state.clone()),
            snapshot_requests,
        )
        .expect("a persisting runtime starts a snapshot task");
    assert_eq!(window.persisted_revision(), None);

    timeout(
        REACHED_WITHIN,
        runtime.shutdown(runtime.domain_drain_timeout()),
    )
    .await
    .expect("the runtime stops without its branch's stop");

    timeout(REACHED_WITHIN, snapshot_task)
        .await
        .expect("the snapshot task has ended with its runtime")
        .expect("the snapshot task does not panic");
    assert_eq!(
        window.persisted_revision(),
        Some(1),
        "the window the branch published is on disk when the runtime has stopped"
    );
    drop(branch_stop);
}

/// How long a check gives a domain's stop to finish while the write it must wait for is held. The
/// stop must never finish while the write is held, so a longer window only strengthens the check.
const HELD_WRITE_WINDOW: Duration = Duration::from_secs(2);

/// A window branch whose task was ended at its stop grace leaves its snapshot task writing what
/// the branch published last, and the stop of its domain waits for that write: a stopped domain's
/// checkpoints are what a stopped backup cut reads as soon as the stop returns. Here the write
/// waits for a bulk worker, so the domain stays stopping while every worker is held, and the window
/// is on disk the moment its stop returns.
#[nervix_primitives::test]
async fn a_stopping_domain_waits_for_the_snapshot_of_a_branch_whose_task_is_gone() {
    let window = StdArc::new(PublishedWindow::new().await);
    let holders = hold_every_bulk_worker(window.runtime.executor()).await;
    end_branch_at_its_grace(&window).await;

    let mut stopping = nervix_primitives::task::spawn({
        let window = window.clone();
        async move {
            let domain = window.placement.domain.clone();
            let runtime = window.runtime.clone();
            runtime
                .stop_domain_execution(
                    &domain,
                    idle_execution(&domain),
                    runtime.emitter_finish_deadline(),
                )
                .await;
            window.persisted_revision()
        }
    });
    assert!(
        timeout(HELD_WRITE_WINDOW, &mut stopping).await.is_err(),
        "the domain finished stopping while its branch's last write had no bulk worker"
    );

    drop(holders);
    let persisted_when_stopped = timeout(REACHED_WITHIN, stopping)
        .await
        .expect("the domain stops once its branch's last write is done")
        .expect("the stopping task does not panic");
    assert_eq!(
        persisted_when_stopped,
        Some(1),
        "the window the branch published is on disk when its domain's stop returns"
    );
}

/// A domain's stop waits only for the snapshot tasks of its own branches. Another domain's branch
/// that is still running keeps its snapshot task waiting, and the stopping domain does not wait
/// for it.
#[nervix_primitives::test]
async fn a_stopping_domain_does_not_wait_for_another_domains_snapshot_task() {
    let window = PublishedWindow::new().await;
    let runtime = window.runtime.clone();
    let (branch_stop, _) = watch::channel(false);
    let (snapshot_requests, _branch_requests) = mpsc::channel(1);
    let snapshot_task = runtime
        .spawn_published_branch_state_snapshot_task(
            &branch_stop,
            PublishedBranchState::WindowProcessor(window.state.clone()),
            snapshot_requests,
        )
        .expect("a persisting runtime starts a snapshot task");
    let other = domain("other");

    timeout(
        REACHED_WITHIN,
        runtime.stop_domain_execution(
            &other,
            idle_execution(&other),
            runtime.emitter_finish_deadline(),
        ),
    )
    .await
    .expect("a domain stops while another domain's branch keeps running");
    assert!(
        !snapshot_task.is_finished(),
        "the running branch's snapshot task still waits for its branch"
    );
    assert_eq!(window.persisted_revision(), None);

    drop(branch_stop);
    timeout(REACHED_WITHIN, snapshot_task)
        .await
        .expect("the snapshot task writes once its branch stops")
        .expect("the snapshot task does not panic");
    assert_eq!(window.persisted_revision(), Some(1));
}

/// Where a check reads the revision of a materialized relay that the runtime's state store holds.
#[derive(Clone)]
struct RelayCheckpoints {
    runtime: Runtime,
    placement: RuntimeStatePlacement,
}

impl RelayCheckpoints {
    /// The revision of the relay's records the runtime's state store holds.
    fn persisted_revision(&self) -> Option<u64> {
        let persisted = self
            .runtime
            .inner
            .state_store
            .as_ref()
            .expect("the test runtime has a state store")
            .latest_snapshot(&self.placement)
            .expect("the snapshot lookup succeeds");
        persisted.map(|snapshot| snapshot.lsm)
    }
}

/// A runtime that persists its state, with one materialized relay that has applied a record no
/// checkpoint holds yet.
struct AppliedRelay {
    checkpoints: RelayCheckpoints,
    /// The authoritative assignment of the relay, which its state task holds while it runs.
    state: MaterializedRelayStateOriginator,
    persistence: MaterializedRelayStatePersistence,
    _directory: tempfile::TempDir,
}

impl AppliedRelay {
    async fn new() -> Self {
        let directory = tempfile::tempdir().expect("the test host provides a temporary directory");
        let database = Database::builder(directory.path())
            .open()
            .expect("a new directory opens as a database");
        let runtime = Runtime::with_persistence(Some(database), Duration::from_secs(3_600))
            .expect("the runtime opens its state store");
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeState::MaterializedRelay {
                schema: SchemaFingerprint::from_digest([9; 32]),
            },
            kind: ModelKind::Relay,
            identifier: named("latest_orders"),
            unit: nervix_models::Unit::Single,
            branch_key: None,
        };
        let schema = test_schema(&[("status", ParseAsType::String)]);
        let mut assignment = runtime.replicated_materialized_stream_state(
            placement.clone(),
            schema.arrow_schema(),
            None,
            Vec::new(),
            None,
        );
        let mut state = assignment
            .originator
            .take()
            .expect("the node that executes the relay holds its authoritative assignment");
        runtime
            .apply_materialized_stream_records(&mut state, &None, [status_record("ready", 1)])
            .await
            .expect("the relay's assignment stays authoritative");
        Self {
            checkpoints: RelayCheckpoints { runtime, placement },
            state,
            persistence: assignment.persistence,
            _directory: directory,
        }
    }
}

/// A record of the relay ingested at `watermark`, so a record with a later one replaces it.
fn status_record(status: &str, watermark: i64) -> RuntimeRow {
    test_runtime_row([(
        "status".to_string(),
        RuntimeValue::String(status.to_string()),
    )])
    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(watermark))
}

/// How long a check holds a materialized relay's last write: past the stop grace of every task of
/// its domain, so a stop that ends that write at a grace has finished inside the window.
const HELD_PAST_THE_STOP_GRACE: Duration = Duration::from_secs(TASK_STOP_GRACE.as_secs() * 2);

/// A materialized relay's checkpoint writer makes its last write once its domain stops, and the
/// stop waits for that write with no grace, however long it takes: a stopped backup cut reads the
/// relay's checkpoint as soon as the stop returns. Here the write waits for a bulk worker past the
/// stop grace of every task, so the domain stays stopping while every worker is held, and the
/// record the relay applied is on disk the moment its stop returns.
#[nervix_primitives::test]
async fn a_stopping_domain_waits_past_the_stop_grace_for_a_materialized_relays_last_checkpoint() {
    let relay = AppliedRelay::new().await;
    let checkpoints = relay.checkpoints.clone();
    let domain = checkpoints.placement.domain.clone();
    let mut execution = idle_execution(&domain);
    let writer = checkpoints
        .runtime
        .spawn_materialized_stream_snapshot_task(relay.persistence.clone())
        .expect("a persisting runtime starts the relay's checkpoint writer");
    execution.placement_tasks.insert(
        NodeRef::new(ModelKind::Relay, &checkpoints.placement.identifier),
        PlacementTasks {
            replication: Vec::new(),
            checkpoints: vec![writer],
        },
    );
    let holders = hold_every_bulk_worker(checkpoints.runtime.executor()).await;

    let mut stopping = nervix_primitives::task::spawn({
        let checkpoints = checkpoints.clone();
        async move {
            let runtime = checkpoints.runtime.clone();
            runtime
                .stop_domain_execution(&domain, execution, runtime.emitter_finish_deadline())
                .await;
            checkpoints.persisted_revision()
        }
    });
    assert!(
        timeout(HELD_PAST_THE_STOP_GRACE, &mut stopping)
            .await
            .is_err(),
        "the domain finished stopping while its relay's last checkpoint had no bulk worker"
    );

    drop(holders);
    let persisted_when_stopped = timeout(REACHED_WITHIN, stopping)
        .await
        .expect("the domain stops once its relay's last checkpoint is written")
        .expect("the stopping task does not panic");
    assert_eq!(
        persisted_when_stopped,
        Some(1),
        "the record the relay applied is on disk when its domain's stop returns"
    );
    drop(relay);
}

/// How long a check holds a materialized relay's state task as it stops: well inside the grace that
/// task is given, so it applies what it still holds instead of being ended.
const HELD_STATE_TASK_WINDOW: Duration = Duration::from_millis(500);

/// How often a check looks for a checkpoint while it holds a relay's state task.
const CHECKPOINT_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A relay state task that applies `record`, what the relay still delivers to it, only once its
/// domain's stop reaches it and the check opens `applying`; it reports through `told_to_stop` that
/// the stop reached it.
fn relay_state_task_applying_as_it_stops(
    runtime: &Runtime,
    mut state: MaterializedRelayStateOriginator,
    record: RuntimeRow,
    told_to_stop: oneshot::Sender<()>,
    applying: oneshot::Receiver<()>,
) -> RelayStateTask {
    let (shutdown, mut stopped) = watch::channel(false);
    let runtime = runtime.clone();
    let task = nervix_primitives::task::spawn(async move {
        stopped
            .wait_for(|stopped| *stopped)
            .await
            .expect("the domain's stop tells the state task to stop before it drops the sender");
        told_to_stop
            .send(())
            .expect("the check waits until the stop reaches the state task");
        applying
            .await
            .expect("the check lets the state task apply what it holds");
        runtime
            .apply_materialized_stream_records(&mut state, &None, [record])
            .await
            .expect("the relay's assignment stays authoritative");
    });
    RelayStateTask { shutdown, task }
}

/// A materialized relay's state task goes on applying what the relay delivers while its domain
/// stops, so the relay's checkpoint writer makes its last write only once that task has ended: the
/// stopped domain's checkpoint then holds the record the state task applied last, not the state the
/// relay held when the stop was published. Here the state task holds that record until the check
/// releases it, and no checkpoint is written while it does.
#[nervix_primitives::test]
async fn a_stopping_domain_writes_a_materialized_relays_last_checkpoint_once_its_state_task_has_ended()
 {
    let AppliedRelay {
        checkpoints,
        state,
        persistence,
        _directory,
    } = AppliedRelay::new().await;
    let domain = checkpoints.placement.domain.clone();
    let mut execution = idle_execution(&domain);
    let (told_to_stop, stop_reached) = oneshot::channel();
    let (apply, applying) = oneshot::channel();
    execution.relay_state_tasks.insert(
        RelayName::from(&checkpoints.placement.identifier),
        relay_state_task_applying_as_it_stops(
            &checkpoints.runtime,
            state,
            status_record("shipped", 2),
            told_to_stop,
            applying,
        ),
    );
    let writer = checkpoints
        .runtime
        .spawn_materialized_stream_snapshot_task(persistence)
        .expect("a persisting runtime starts the relay's checkpoint writer");
    execution.placement_tasks.insert(
        NodeRef::new(ModelKind::Relay, &checkpoints.placement.identifier),
        PlacementTasks {
            replication: Vec::new(),
            checkpoints: vec![writer],
        },
    );

    let stopping = nervix_primitives::task::spawn({
        let checkpoints = checkpoints.clone();
        async move {
            let runtime = checkpoints.runtime.clone();
            runtime
                .stop_domain_execution(&domain, execution, runtime.emitter_finish_deadline())
                .await;
            checkpoints.persisted_revision()
        }
    });
    timeout(REACHED_WITHIN, stop_reached)
        .await
        .expect("the domain's stop reaches the relay's state task")
        .expect("the state task reports that the stop reached it");
    let written_while_applying = timeout(HELD_STATE_TASK_WINDOW, async {
        while checkpoints.persisted_revision().is_none() {
            sleep(CHECKPOINT_POLL_INTERVAL).await;
        }
    })
    .await;
    assert!(
        written_while_applying.is_err(),
        "the relay's checkpoint was written while its state task was still applying what it held"
    );

    apply
        .send(())
        .expect("the state task waits for the check to release it");
    let persisted_when_stopped = timeout(REACHED_WITHIN, stopping)
        .await
        .expect("the domain stops once its relay's last checkpoint is written")
        .expect("the stopping task does not panic");
    assert_eq!(
        persisted_when_stopped,
        Some(2),
        "the relay's last checkpoint holds the record its state task applied as it stopped"
    );
}
