//! Runtime teardown tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests for how long stopping one domain's execution may take when its tasks
//!   outlast their stop grace.
//! - **Depends on.** The runtime lifecycle and its test fixtures.
//! - **Must not know.** Production control-plane orchestration or external connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use ahash::HashMap;
use nervix_models::{DomainName, GeneratorName, ModelKind, NodeRef, ReingestorName};
use nervix_primitives::{sync::watch, task::JoinHandle, time::Instant};

use super::*;

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
    runtime.stop_domain_execution(domain, execution).await;
    started.elapsed()
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
