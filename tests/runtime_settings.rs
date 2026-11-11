//! Public runtime-settings fixtures.
//!
//! Outside the layer order: test harness.
//! - **Owns.** Bounded acknowledged population of a materialized relay through HTTP.
//! - **Depends on.** The public endpoint publisher and scenario-owned cluster.
//! - **Must not know.** Runtime state handles, snapshot encoding or private materialization APIs.

use std::time::Duration;

use cucumber::{then, when};
use meticulous::{OptionExt as _, ResultExt as _};

use crate::{ScenarioWorld, expand_placeholders};

#[when(
    expr = "{int} distinct materialized rows in batches of {int} are acknowledged through host \
            {string} path {string}"
)]
async fn distinct_materialized_rows(
    world: &mut ScenarioWorld,
    count: u32,
    batch_rows: u32,
    host: String,
    path: String,
) {
    assert!(batch_rows > 0, "a fixture batch contains rows");
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let publisher = world.cluster().acknowledged_http_publisher(
        "node-1",
        &host,
        &path,
        Duration::from_secs(180),
    );
    let mut next = 0_u32;
    while next < count {
        nervix_primitives::task::consume_budget().await;
        let end = next
            .checked_add(batch_rows)
            .assured("the bounded fixture count fits u32")
            .min(count);
        let mut rows = Vec::new();
        for id in next..end {
            let value = i64::from(id)
                .checked_mul(17)
                .assured("u32 times seventeen fits i64");
            rows.push(serde_json::json!({"id": id, "value": value}));
        }
        let payload = serde_json::to_string(&rows).assured("the fixture has JSON scalar fields");
        publisher
            .publish_after_admission_refusals(&payload)
            .await
            .unwrap_or_else(|error| {
                panic!("materialized fixture batch {next}..{end} was not acknowledged: {error}")
            });
        next = end;
    }
}

#[when("the forwarded acknowledgement measurement starts")]
fn acknowledgement_measurement_starts(world: &mut ScenarioWorld) {
    world.runtime_ack_started_at = Some(nervix_primitives::time::Instant::now());
}

#[then(
    expr = "the lost acknowledgement recovers consumer group {string} topic {string} after at \
            least {string} and within {string} of the measurement start"
)]
async fn lost_acknowledgement_recovers(
    world: &mut ScenarioWorld,
    group: String,
    topic: String,
    minimum: String,
    maximum: String,
) {
    let started = world
        .runtime_ack_started_at
        .expect("the measurement starts before publication");
    let minimum =
        crate::parse_duration_text(&minimum).expect("the lower measurement bound is a duration");
    let maximum =
        crate::parse_duration_text(&maximum).expect("the measurement bound is a duration");
    let group = expand_placeholders(world, &group);
    let topic = expand_placeholders(world, &topic);
    world
        .cluster()
        .assert_kafka_consumer_group_next_offset(&group, &topic, 0, 2, true, maximum)
        .await
        .expect("the source resumes after the lost forwarded acknowledgement expires");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= minimum && elapsed < maximum,
        "forwarded ACK recovery took {elapsed:?}, beyond {maximum:?}"
    );
}
