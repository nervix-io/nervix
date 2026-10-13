//! Public NSPL catch-up through an opaque rate-limited interconnect.
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
    for source in world.cluster().node_ids() {
        if source != follower {
            opens.insert(
                source.clone(),
                world
                    .fault_injection
                    .consensus_append_stream_open_count(&node_name(&source), &node_name(&follower)),
            );
        }
    }
    world.moving_catch_up = Some(MovingCatchUp {
        follower: follower.clone(),
        opens,
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
    let deadline =
        Instant::now() + crate::parse_duration_text(&bound).expect("valid catch-up bound");
    loop {
        nervix_primitives::task::consume_budget().await;
        let applied = applied_burst_domains(world, &observation.follower, &prefix).await;
        let mut streams = 0_u64;
        for (source, baseline) in &observation.opens {
            let opened = world.fault_injection.consensus_append_stream_open_count(
                &node_name(source),
                &node_name(&observation.follower),
            );
            streams = streams.saturating_add(
                opened
                    .checked_sub(*baseline)
                    .expect("open counters increase"),
            );
        }
        if applied == count {
            assert!(
                world
                    .cluster()
                    .maximum_paced_interconnect_bytes(&observation.follower)
                    > 192 * 1024,
                "the paced connection must carry more than five seconds of bytes at 32768 bytes/s"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "paced follower applied {applied}/{count} domains using {streams} streams within \
             {bound}"
        );
        sleep(Duration::from_millis(200)).await;
    }
}
