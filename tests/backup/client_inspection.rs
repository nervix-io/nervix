//! Public restore progress while its real filesystem admission queue is full.
//!
//! Layer: test harness.
//! - **Owns.** Held filesystem workers and queue places and the public restore outcome probe.
//! - **Depends on.** Registered executors, first-poll observations and the scenario world.
//! - **Must not know.** Client validation or restore application implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_execution::{Executor, MemoryClass, QueueAdmission, StorageClass};
use nervix_primitives::sync::{blocking::mpsc, oneshot};

use super::*;
use crate::common::cluster;

pub(crate) struct FilesystemHold {
    releases: Vec<mpsc::Sender<()>>,
    tasks: Vec<AbortOnDropHandle<()>>,
}

impl Drop for FilesystemHold {
    fn drop(&mut self) {
        self.releases.clear();
        self.tasks.clear();
    }
}

impl FilesystemHold {
    async fn fill(executor: &Executor) -> Self {
        let shape = executor.snapshot().filesystem_storage;
        let mut held = Self {
            releases: Vec::new(),
            tasks: Vec::new(),
        };
        for _ in 0..shape.workers {
            nervix_primitives::task::consume_budget().await;
            let (release, parked) = mpsc::channel();
            let (started, running) = oneshot::channel();
            let worker = executor.clone();
            held.releases.push(release);
            held.tasks
                .push(AbortOnDropHandle::new(nervix_primitives::task::spawn(
                    async move {
                        let charge = worker
                            .try_reserve(MemoryClass::Management, 0)
                            .assured("zero charge");
                        worker
                            .run_storage_with(
                                StorageClass::Filesystem,
                                QueueAdmission::WaitForPlace,
                                charge,
                                move |_charge, _cancel| {
                                    started.send(()).discarded(
                                        "a cancelled fixture no longer waits for its worker",
                                    );
                                    match parked.recv() {
                                        Ok(()) | Err(mpsc::RecvError) => {}
                                    }
                                },
                            )
                            .await
                            .assured("the held filesystem job runs");
                    },
                )));
            running.await.assured("the holding worker starts");
        }
        for _ in 0..shape.queue_capacity {
            nervix_primitives::task::consume_budget().await;
            let waiter = executor.clone();
            held.tasks
                .push(AbortOnDropHandle::new(nervix_primitives::task::spawn(
                    async move {
                        let charge = waiter
                            .try_reserve(MemoryClass::Management, 0)
                            .assured("zero charge");
                        waiter
                            .run_storage_with(
                                StorageClass::Filesystem,
                                QueueAdmission::WaitForPlace,
                                charge,
                                |_charge, _cancel| (),
                            )
                            .await
                            .assured("a released queue holder runs");
                    },
                )));
        }
        while executor.snapshot().filesystem_storage.pending < shape.queue_capacity {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
        held
    }
}

#[given(expr = "client inspection storage admission for domain {string} is observed")]
fn given_inspection_is_observed(world: &mut ScenarioWorld, domain: String) {
    let domain = DomainName::parse(&expand_placeholders(world, &domain)).assured("valid domain");
    world.fault_injection.observe_client_inspection_on(domain);
}

#[then(expr = "the restoring model batch pauses on node {string}")]
async fn then_restoring_model_batch_pauses(world: &mut ScenarioWorld, node: String) {
    let node = cluster::node_name(&expand_placeholders(world, &node));
    let fault = world.fault_injection.clone();
    let background = world
        .background_restore
        .as_mut()
        .assured("the public restore stream was started");
    nervix_primitives::time::timeout(Duration::from_secs(30), async {
        nervix_primitives::select! {
            () = fault.wait_for_command_admission_pause(&node) => {},
            ended = &mut background.task => panic!("restore ended before its model batch paused: {ended:?}"),
        }
    })
    .await
    .assured("the restoring batch reaches its admission pause");
}

#[when(expr = "filesystem workers and their queue on node {string} are held full")]
async fn when_filesystem_capacity_is_held(world: &mut ScenarioWorld, node: String) {
    assert!(world.restore_inspection_capacity.is_none());
    let executor = world
        .fault_injection
        .executor_on(&cluster::node_name(&expand_placeholders(world, &node)));
    let held =
        nervix_primitives::time::timeout(Duration::from_secs(30), FilesystemHold::fill(&executor))
            .await
            .assured("filesystem workers and their queue are full");
    world.restore_inspection_capacity = Some(held);
}

#[then(expr = "client inspection of domain {string} has polled its storage admission")]
async fn then_inspection_admission_is_polled(world: &mut ScenarioWorld, domain: String) {
    let domain = DomainName::parse(&expand_placeholders(world, &domain)).assured("valid domain");
    let fault = world.fault_injection.clone();
    let background = world
        .background_restore
        .as_mut()
        .assured("the public restore stream is active");
    nervix_primitives::time::timeout(Duration::from_secs(30), async {
        nervix_primitives::select! {
            biased;
            () = fault.wait_for_client_inspection_admission(&domain) => {},
            ended = &mut background.task => panic!("restore ended before inspecting its client: {ended:?}"),
        }
    })
    .await
    .assured("the actual inspector polls its storage admission while the queue is full");
}

#[when("the scenario releases its client inspection capacity")]
fn when_inspection_capacity_is_released(world: &mut ScenarioWorld) {
    assert!(world.restore_inspection_capacity.take().is_some());
}
