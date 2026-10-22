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
use nervix_execution::{CpuClass, MemoryClass};
use nervix_models::{
    DomainName, EmitterName, GeneratorName, ModelKind, NodeRef, ParseAsType, ReingestorName,
    SchemaFingerprint,
};
use nervix_primitives::{
    sync::{StdArc, blocking, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout},
};

use super::*;

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
        "an emitter that the stop ends must already hold the deadline it finishes its sink by"
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

/// A window branch whose task was ended at its stop grace leaves its snapshot task writing what
/// the branch published last. That task holds the runtime and its storage, so a runtime that stops
/// waits for it: here the write waits for a bulk worker, and the runtime finishes stopping only
/// once the worker is given back and the window is on disk.
#[nervix_primitives::test]
async fn a_stopping_runtime_waits_for_the_snapshot_of_a_branch_whose_task_is_gone() {
    let window = PublishedWindow::new().await;
    let runtime = window.runtime.clone();

    // Every bulk worker is held, so encoding the published window waits for one.
    let executor = runtime.executor().clone();
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

    let (branch_stop, _) = watch::channel(false);
    let (snapshot_requests, branch_requests) = mpsc::channel(1);
    let snapshot_task = runtime
        .spawn_published_branch_state_snapshot_task(
            &branch_stop,
            PublishedBranchState::WindowProcessor(window.state.clone()),
            snapshot_requests,
        )
        .expect("a persisting runtime starts a snapshot task");
    // A branch task ended at its stop grace is dropped with everything it held, without telling
    // its snapshot task anything.
    drop(branch_requests);
    drop(branch_stop);
    drop(snapshot_task);
    timeout(REACHED_WITHIN, async {
        while executor.snapshot().bulk_cpu.pending == 0 {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("the snapshot task's write waits for a bulk worker");

    let stopping = nervix_primitives::task::spawn({
        let runtime = runtime.clone();
        async move {
            let left = runtime.domain_drain_timeout();
            runtime.shutdown(left).await
        }
    });
    let told_to_stop = runtime.inner.branch_snapshot_tasks.stopping();
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
