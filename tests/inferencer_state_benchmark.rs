//! Timed requests for the opt-in inference cluster benchmark.
//!
//! Layer: benchmark harness, outside the product layer order.
//! - **Owns.** The acknowledged request sample and its latency and throughput report.
//! - **Depends on.** The scenario cluster's single-attempt HTTP publisher and monotonic time.
//! - **Must not know.** The model's implementation, state storage or the cluster's schedule.

use std::{io, time::Duration};

use cucumber::when;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;

use crate::{ScenarioWorld, common::cluster::AcknowledgedHttpPublisher, expand_placeholders};

const INPUT: &str = r#"{"vector":[1.0,0.0]}"#;
const REQUEST_DEADLINE: Duration = Duration::from_secs(15);

#[when(
    expr = "inference case {string} is timed at {int} concurrent requests after {int} warmup \
            requests for {int} measured requests to host {string} path {string}"
)]
async fn measure_inference_requests(
    world: &mut ScenarioWorld,
    case: String,
    concurrency: usize,
    warmup: u32,
    count: u32,
    host: String,
    path: String,
) {
    assert!((1..=64).contains(&concurrency));
    assert!((1..=100_000).contains(&warmup));
    assert!((1..=100_000).contains(&count));
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let publisher =
        world
            .cluster()
            .acknowledged_http_publisher("node-1", &host, &path, REQUEST_DEADLINE);
    sample_requests(&publisher, concurrency, warmup)
        .await
        .verified("each warmup request received a completed graph acknowledgement");
    let start = Instant::now();
    let mut latencies = sample_requests(&publisher, concurrency, count)
        .await
        .verified("each measured request received a completed graph acknowledgement");
    let elapsed = start.elapsed();

    // Publish once after all concurrent work finishes. Its later timestamp lets the materialized
    // relay expose the final score even if the measured requests arrived in a different order.
    publisher
        .publish_once(INPUT)
        .await
        .verified("the final score request received a completed graph acknowledgement");

    latencies.sort_unstable();
    let report = serde_json::json!({
        "case": case,
        "concurrency": concurrency,
        "warmup_requests": warmup,
        "measured_requests": count,
        "elapsed_seconds": elapsed.as_secs_f64(),
        "rows_per_second": f64::from(count) / elapsed.as_secs_f64(),
        "p50_ms": percentile_ms(&latencies, 50),
        "p95_ms": percentile_ms(&latencies, 95),
        "p99_ms": percentile_ms(&latencies, 99),
    });
    println!("INFERENCE_BENCHMARK {report}");
}

async fn sample_requests(
    publisher: &AcknowledgedHttpPublisher,
    concurrency: usize,
    count: u32,
) -> io::Result<Vec<Duration>> {
    let requests = stream::iter(0..count).map(|_| time_acknowledgement(publisher));
    requests.buffer_unordered(concurrency).try_collect().await
}

async fn time_acknowledgement(publisher: &AcknowledgedHttpPublisher) -> io::Result<Duration> {
    let start = Instant::now();
    publisher.publish_once(INPUT).await?;
    Ok(start.elapsed())
}

fn percentile_ms(sorted: &[Duration], percentile: usize) -> f64 {
    let weighted = sorted
        .len()
        .checked_mul(percentile)
        .assured("the benchmark caps the sample count and asks for percentiles up to 99");
    let rank = weighted
        .div_ceil(100)
        .checked_sub(1)
        .assured("the sample count and requested percentile are positive");
    sorted[rank].as_secs_f64() * 1_000.0
}
