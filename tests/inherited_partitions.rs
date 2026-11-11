//! Native-partition publishing and relay observations for inherited-partition scenarios.
//!
//! Outside the layer order: a scenario harness.
//! - **Owns.** Explicit native-partition events and aggregate public relay-hop observations.
//! - **Depends on.** The scenario world, broker publishing fixture and public metrics.
//! - **Must not know.** Runtime routing, registry plans or connector internals.

use std::time::Duration;

use cucumber::{then, when};
use meticulous::ResultExt as _;

use super::{ScenarioWorld, expand_placeholders};

#[when(expr = "each of the {int} partitions of {word} topic {string} receives one distinct event")]
async fn publish_every_partition(
    world: &mut ScenarioWorld,
    count: u32,
    broker: String,
    topic: String,
) {
    let topic = expand_placeholders(world, &topic);
    for partition in 0..count {
        let payload = format!("{{\"tenant\":\"tenant-{partition}\",\"seq\":{partition}}}");
        match broker.as_str() {
            "Kafka" => {
                world
                    .cluster()
                    .publish_kafka_partition(
                        &topic,
                        i32::try_from(partition).assured("the fixture partition fits i32"),
                        &payload,
                    )
                    .await
            }
            "Pulsar" => {
                world
                    .cluster()
                    .publish_pulsar(&format!("{topic}-partition-{partition}"), &payload)
                    .await
            }
            _ => panic!("the inherited-partition fixture declares Kafka or Pulsar"),
        }
        .assured("the broker confirms this partition's event");
    }
}

#[then(expr = "the cluster reports at least {int} admitted relay hops")]
async fn admitted_relay_hops(world: &mut ScenarioWorld, minimum: u32) {
    let mut observed = Vec::new();
    let outcome = nervix_primitives::time::timeout(Duration::from_secs(30), async {
        loop {
            observed.clear();
            for node in world.cluster().node_ids() {
                observed.push(
                    world
                        .cluster()
                        .sum_observability_metric(
                            &node,
                            "nervix_interconnect_relay_admissions_total",
                            &["outcome=\"admitted\"".to_string()],
                        )
                        .await
                        .assured("the node exposes its relay admission counter"),
                );
            }
            if observed.iter().sum::<f64>() >= f64::from(minimum) {
                break;
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "relay admissions did not reach {minimum}: {observed:?}"
    );
}
