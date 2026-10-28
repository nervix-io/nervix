//! Public Nervix follower catch-up through an opaque rate-limited interconnect.
//!
//! Outside the layer order: a Cucumber harness.
//! - **Owns.** The paced follower's applied-domain assertion and catch-up observations.
//! - **Depends on.** The scenario world, public cluster operations and existing consensus observers.
//! - **Must not know.** Frame encoding or implementation of the writer's progress deadline.

use std::{collections::BTreeMap, time::Duration};

use cucumber::{given, then, when};
use nervix_primitives::time::{Instant, sleep};

use crate::{
    ScenarioWorld, applied_burst_domains, common::cluster::node_name, expand_placeholders,
};

#[derive(Clone, Debug)]
pub(crate) struct MovingCatchUp {
    follower: String,
    opens: BTreeMap<String, u64>,
    attempts: BTreeMap<String, u64>,
}

#[given(
    expr = "inbound interconnect traffic to stopped node {string} is paced at {int} bytes per \
            second"
)]
async fn given_inbound_interconnect_is_paced(world: &mut ScenarioWorld, node: String, rate: u64) {
    let node = expand_placeholders(world, &node);
    world
        .cluster_mut()
        .pace_stopped_node_interconnect(&node, rate)
        .await
        .unwrap_or_else(|error| panic!("could not pace follower '{node}': {error}"));
}

#[when(expr = "node {string} starts catching up through its paced interconnect")]
async fn when_paced_catch_up_starts(world: &mut ScenarioWorld, node: String) {
    let follower = expand_placeholders(world, &node);
    let mut opens = BTreeMap::new();
    let mut attempts = BTreeMap::new();
    for source in world.cluster().node_ids() {
        if source != follower {
            let source_name = node_name(&source);
            let follower_name = node_name(&follower);
            opens.insert(
                source.clone(),
                world
                    .fault_injection
                    .consensus_append_stream_open_count(&source_name, &follower_name),
            );
            attempts.insert(
                source.clone(),
                world
                    .fault_injection
                    .consensus_append_stream_attempt_count(&source_name, &follower_name),
            );
        }
    }
    world.moving_catch_up = Some(MovingCatchUp {
        follower: follower.clone(),
        opens,
        attempts,
    });
    world
        .cluster_mut()
        .start_node_without_catching_up(&follower)
        .await
        .unwrap_or_else(|error| panic!("could not start paced follower '{follower}': {error}"));
}

#[then(expr = "within {string} the paced follower applies {int} domains named {string}")]
async fn then_paced_follower_catches_up(
    world: &mut ScenarioWorld,
    bound: String,
    count: usize,
    prefix: String,
) {
    let observation = world
        .moving_catch_up
        .as_ref()
        .expect("the scenario started paced catch-up");
    let started_at = Instant::now();
    let deadline = started_at + crate::parse_duration_text(&bound).expect("valid catch-up bound");
    let mut next_sample = started_at;
    let mut samples = Vec::new();
    loop {
        nervix_primitives::task::consume_budget().await;
        let applied = applied_burst_domains(world, &observation.follower, &prefix).await;
        let now = Instant::now();
        if now >= next_sample || applied == count || now >= deadline {
            let sample = progress_sample(world, observation, started_at, applied).await;
            tracing::info!(sample = %sample, "paced follower catch-up observation");
            samples.push(sample);
            next_sample = now + Duration::from_secs(5);
        }
        if applied == count {
            let paced = world
                .cluster()
                .paced_interconnect_observation(&observation.follower);
            assert!(
                paced.maximum_forwarded > 192 * 1024,
                "the paced connection must carry more than five seconds of bytes at 32768 \
                 bytes/s; observation: {paced:?}"
            );
            return;
        }
        if now >= deadline {
            let streams = stream_totals(world, observation).0;
            let statuses = brief_statuses(world).await;
            panic!(
                "paced follower applied {applied}/{count} domains using {streams} streams within \
                 {bound}\nprogress samples:\n{}\nnode status:\n{statuses}",
                samples.join("\n")
            );
        }
        sleep(Duration::from_millis(200)).await;
    }
}

fn stream_totals(world: &ScenarioWorld, observation: &MovingCatchUp) -> (u64, u64, Vec<String>) {
    let follower = node_name(&observation.follower);
    let mut opens = 0_u64;
    let mut attempts = 0_u64;
    let mut sources = Vec::new();
    for (source, baseline) in &observation.opens {
        let source_name = node_name(source);
        let source_opens = world
            .fault_injection
            .consensus_append_stream_open_count(&source_name, &follower)
            .checked_sub(*baseline)
            .expect("open counters increase");
        let source_attempts = world
            .fault_injection
            .consensus_append_stream_attempt_count(&source_name, &follower)
            .checked_sub(observation.attempts[source])
            .expect("attempt counters increase");
        opens = opens.saturating_add(source_opens);
        attempts = attempts.saturating_add(source_attempts);
        sources.push(format!(
            "{source}:opens={source_opens},attempts={source_attempts}"
        ));
    }
    (opens, attempts, sources)
}

async fn progress_sample(
    world: &ScenarioWorld,
    observation: &MovingCatchUp,
    started_at: Instant,
    applied: usize,
) -> String {
    let (opens, attempts, sources) = stream_totals(world, observation);
    let paced = world
        .cluster()
        .paced_interconnect_observation(&observation.follower);
    let mut nodes = Vec::new();
    for node in world.cluster().node_ids() {
        let observer = world.fault_injection.consensus_observer(&node_name(&node));
        let log = observer.raft_log_retention();
        let revision = observer.current_revision().await;
        nodes.push(format!(
            "{node}:revision={revision},last_log={:?},snapshot={:?},purged={:?}",
            log.last_log_index, log.snapshot_index, log.purged_index
        ));
    }
    format!(
        "elapsed={:?} applied={applied} stream_opens={opens} stream_attempts={attempts} \
         pacer=accepted:{},connected:{},connect_failures:{},transfer_failures:{},cancelled:{},\
         completed:{},total_bytes:{},max_connection_bytes:{} sources=[{}] nodes=[{}]",
        started_at.elapsed(),
        paced.accepted,
        paced.connected,
        paced.connect_failures,
        paced.transfer_failures,
        paced.cancelled,
        paced.completed,
        paced.total_forwarded,
        paced.maximum_forwarded,
        sources.join("; "),
        nodes.join("; ")
    )
}

async fn brief_statuses(world: &ScenarioWorld) -> String {
    let statuses = world.cluster().collect_status_snapshots().await;
    statuses
        .into_iter()
        .map(|(node, status)| match status {
            Ok(text) => {
                let selected = text
                    .lines()
                    .filter(|line| {
                        line.starts_with("raft.")
                            || line.starts_with("node.initialization=")
                            || line.contains(" [voter]")
                            || line.contains(" [learner]")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{node}: {selected}")
            }
            Err(error) => format!("{node}: {error:#}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}
