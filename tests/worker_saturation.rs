//! Steps that hold one worker class full on the cluster's nodes, and the public witness that the
//! work it could not take waited for a place instead of being refused.
//!
//! Outside the layer order: a harness module.
//! - **Owns.** Saturating and releasing one CPU or storage worker class on every node or on one
//!   node, and the refusal count each node reported for a class once the class was full.
//! - **Depends on.** The fault injector's execution controls and each node's metrics endpoint.
//! - **Must not know.** Which product work runs on a class, or how a node sizes its classes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "holds worker classes full and reads their public counts"
    )
)]

use std::{collections::BTreeMap, time::Duration};

use cucumber::{then, when};
use nervix_execution::{CpuClass, StorageClass, WorkerClassName};
use nervix_primitives::time::Instant;

use crate::{ScenarioWorld, expand_placeholders};

/// The public count of jobs a class refused because its wait queue was full.
const REFUSALS: &str = "nervix_execution_job_rejections_total";

/// The public count of jobs waiting for a place in a class's full wait queue.
const WAITING: &str = "nervix_execution_jobs_waiting";

/// How often a wait for a waiting job reads the nodes' counts again.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How long filling one node's class may take: it waits only for the jobs already running on the
/// class to leave its workers.
const SATURATION_BUDGET: Duration = Duration::from_secs(60);

/// Each class a scenario holds full on a node, with the refusals the node reported for that
/// class once it was full and the waiting jobs a step last saw there.
#[derive(Debug, Default)]
pub(crate) struct SaturatedClasses {
    refusals: BTreeMap<SaturatedClass, f64>,
    waiting: BTreeMap<SaturatedClass, f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SaturatedClass {
    node_id: String,
    class: &'static str,
}

/// The worker class a step spells. The step's own pattern admits only these names.
fn worker_class(name: &str) -> WorkerClassName {
    match name {
        "control" => CpuClass::Control.into(),
        "credentials" => CpuClass::Credentials.into(),
        "data" => CpuClass::Data.into(),
        "extension" => CpuClass::Extension.into(),
        "bulk" => CpuClass::Bulk.into(),
        "consensus storage" => StorageClass::Consensus.into(),
        "filesystem storage" => StorageClass::Filesystem.into(),
        "connector storage" => StorageClass::Connector.into(),
        other => panic!("no worker class is spelled '{other}'"),
    }
}

/// Fill every worker and every wait-queue place of the class on every node, so the next job a
/// node is handed for that class is refused, or waits for a place when its caller keeps it.
#[when(
    regex = r"^(control|credentials|data|extension|bulk|consensus storage|filesystem storage|connector storage) execution is saturated on every node$"
)]
async fn when_execution_is_saturated_on_every_node(world: &mut ScenarioWorld, class: String) {
    let class = worker_class(&class);
    for node_id in world.cluster().node_ids() {
        nervix_primitives::task::consume_budget().await;
        saturate(world, node_id, class).await;
    }
}

/// Fill every worker and every wait-queue place of the class on one node.
#[when(
    regex = r#"^(control|credentials|data|extension|bulk|consensus storage|filesystem storage|connector storage) execution on node "([^"]+)" is saturated$"#
)]
async fn when_execution_on_node_is_saturated(
    world: &mut ScenarioWorld,
    class: String,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    saturate(world, node_id, worker_class(&class)).await;
}

/// Give back the class's workers on every node. The jobs that held its wait-queue places then run
/// empty, and the work that waited for a place takes the places they free, in the order it asked.
#[when(
    regex = r"^(control|credentials|data|extension|bulk|consensus storage|filesystem storage|connector storage) execution is released on every node$"
)]
async fn when_execution_is_released_on_every_node(world: &mut ScenarioWorld, class: String) {
    let class = worker_class(&class);
    for node_id in world.cluster().node_ids() {
        world
            .fault_injection
            .release_execution_class(&crate::common::cluster::node_name(&node_id), class);
    }
}

/// A job of the class waits for a place on the node: its caller found the class's wait queue full
/// and keeps the work instead of having it refused.
#[then(
    regex = r#"^within "([^"]+)" a (control|credentials|data|extension|bulk|consensus storage|filesystem storage|connector storage) job waits for a place on node "([^"]+)"$"#
)]
async fn then_a_job_waits_for_a_place_on_node(
    world: &mut ScenarioWorld,
    duration: String,
    class: String,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let class = worker_class(&class);
    let seen = wait_for_waiting_jobs(world, &duration, class, vec![node_id.clone()], 1.0).await;
    world.worker_saturation.waiting.insert(
        SaturatedClass {
            node_id,
            class: class.as_str(),
        },
        seen,
    );
}

/// One more job of the class waits for a place on the node than the last waiting step saw there.
/// Work the node runs on its own cadence may already wait in the class, so only the growth tells
/// that the work the scenario just let go of waits as well.
#[then(
    regex = r#"^within "([^"]+)" another (control|credentials|data|extension|bulk|consensus storage|filesystem storage|connector storage) job waits for a place on node "([^"]+)"$"#
)]
async fn then_another_job_waits_for_a_place_on_node(
    world: &mut ScenarioWorld,
    duration: String,
    class: String,
    node_id: String,
) {
    let node_id = expand_placeholders(world, &node_id);
    let class = worker_class(&class);
    let key = SaturatedClass {
        node_id: node_id.clone(),
        class: class.as_str(),
    };
    let Some(before) = world.worker_saturation.waiting.get(&key).copied() else {
        panic!(
            "no earlier step saw a {} job waiting on node '{node_id}'",
            class.as_str()
        );
    };
    let seen = wait_for_waiting_jobs(world, &duration, class, vec![node_id], before + 1.0).await;
    world.worker_saturation.waiting.insert(key, seen);
}

/// A job of the class waits for a place on one of the cluster's nodes, where the scenario does not
/// fix which node runs the work.
#[then(
    regex = r#"^within "([^"]+)" a (control|credentials|data|extension|bulk|consensus storage|filesystem storage|connector storage) job waits for a place on some node$"#
)]
async fn then_a_job_waits_for_a_place_on_some_node(
    world: &mut ScenarioWorld,
    duration: String,
    class: String,
) {
    let nodes = world.cluster().node_ids();
    wait_for_waiting_jobs(world, &duration, worker_class(&class), nodes, 1.0).await;
}

/// Every node the scenario saturated the class on still reports the refusals it reported once the
/// class was full: the work the class could not take waited for a place instead.
#[then(
    regex = r"^no node refused a (control|credentials|data|extension|bulk|consensus storage|filesystem storage|connector storage) job while it was saturated$"
)]
async fn then_no_node_refused_a_job_while_saturated(world: &mut ScenarioWorld, class: String) {
    let class = worker_class(&class);
    let mut saturated = Vec::new();
    for (key, refusals) in &world.worker_saturation.refusals {
        if key.class == class.as_str() {
            saturated.push((key.node_id.clone(), *refusals));
        }
    }
    assert!(
        !saturated.is_empty(),
        "no earlier step saturated {} execution on any node",
        class.as_str()
    );
    for (node_id, before) in saturated {
        nervix_primitives::task::consume_budget().await;
        let now = refusals(world, &node_id, class).await;
        assert!(
            now == before,
            "node '{node_id}' refused {} {} jobs after the class was saturated, {before} before \
             and {now} now",
            now - before,
            class.as_str()
        );
    }
}

async fn saturate(world: &mut ScenarioWorld, node_id: String, class: WorkerClassName) {
    let fault_injection = world.fault_injection.clone();
    let node_name = crate::common::cluster::node_name(&node_id);
    nervix_primitives::time::timeout(
        SATURATION_BUDGET,
        fault_injection.saturate_execution(&node_name, class),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "{} execution on '{node_id}' never filled its workers and wait queue: {error}",
            class.as_str()
        )
    });
    // Filling the class may itself be refused a place another job took first, so the count it
    // compares against is the one the node reports once the class is full.
    let refused = refusals(world, &node_id, class).await;
    world.worker_saturation.refusals.insert(
        SaturatedClass {
            node_id,
            class: class.as_str(),
        },
        refused,
    );
}

/// Waits until one of `nodes` reports at least `at_least` jobs of the class waiting for a place,
/// and returns the count it saw there.
async fn wait_for_waiting_jobs(
    world: &ScenarioWorld,
    duration: &str,
    class: WorkerClassName,
    nodes: Vec<String>,
    at_least: f64,
) -> f64 {
    let budget =
        crate::parse_duration_text(duration).expect("step duration must be a valid duration");
    let deadline = Instant::now() + budget;
    loop {
        nervix_primitives::task::consume_budget().await;
        for node_id in &nodes {
            nervix_primitives::task::consume_budget().await;
            let waiting = class_count(world, node_id, WAITING, class).await;
            if waiting >= at_least {
                return waiting;
            }
        }
        assert!(
            Instant::now() < deadline,
            "fewer than {at_least} {} jobs waited for a place on {nodes:?} within {budget:?}",
            class.as_str()
        );
        nervix_primitives::time::sleep(POLL_INTERVAL).await;
    }
}

async fn refusals(world: &ScenarioWorld, node_id: &str, class: WorkerClassName) -> f64 {
    class_count(world, node_id, REFUSALS, class).await
}

/// One of the class's public counts on a node.
async fn class_count(
    world: &ScenarioWorld,
    node_id: &str,
    metric: &str,
    class: WorkerClassName,
) -> f64 {
    let label = format!("class=\"{}\"", class.as_str());
    match world
        .cluster()
        .read_observability_metric(node_id, metric, &[label])
        .await
    {
        Ok(count) => count,
        Err(error) => panic!(
            "node '{node_id}' did not expose '{metric}' for its {} class: {error}",
            class.as_str()
        ),
    }
}
