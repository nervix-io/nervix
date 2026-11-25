//! Public runtime-settings fixtures.
//!
//! Outside the layer order: test harness.
//! - **Owns.** Bounded acknowledged population of a materialized relay through HTTP.
//! - **Depends on.** The public endpoint publisher and scenario-owned cluster.
//! - **Must not know.** Runtime state handles, snapshot encoding or private materialization APIs.

use std::time::Duration;

use cucumber::when;
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
            .populate_idempotent_materialized_batch(&payload)
            .await
            .unwrap_or_else(|error| {
                panic!("materialized fixture batch {next}..{end} was not acknowledged: {error}")
            });
        next = end;
    }
}
