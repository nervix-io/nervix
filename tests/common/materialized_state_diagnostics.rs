//! Small observations of a materialized fixture's public state after its row deadline expires.
//!
//! Layer: test harness.
//! - **Owns.** A bounded sequence of failure observations retaining placement, branch, round,
//!   value length and watermarks without copying large fixture values into failure output.
//! - **Depends on.** Typed node names, primitive timers, current public materialized-state output
//!   and JSON.
//! - **Must not know.** Runtime publications, checkpoint storage or restore installation internals.

use std::{future::Future, io, time::Duration};

use nervix_models::ClusterNodeName;
use nervix_primitives::time::{Instant, timeout};

/// Query a bounded subset after failure, retaining errors and timeouts as observations.
pub(crate) async fn observe<F, Q>(
    nodes: &[ClusterNodeName],
    relays: impl Iterator<Item = usize>,
    mut query: F,
) -> Vec<String>
where
    F: FnMut(&ClusterNodeName, usize) -> Q,
    Q: Future<Output = io::Result<String>>,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut observations = vec![
        "materialized state diagnostics: first four missing relays on the first three nodes; \
         ten-second total budget"
            .to_string(),
    ];
    for relay in relays.take(4) {
        for node in nodes.iter().take(3) {
            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(remaining) if !remaining.is_zero() => remaining,
                _ => {
                    observations
                        .push("materialized state diagnostics: total budget expired".to_string());
                    return observations;
                }
            };
            let observation =
                timeout(remaining.min(Duration::from_secs(2)), query(node, relay)).await;
            observations.push(format!(
                "materialized state observation: node={node}, relay=state_{relay}"
            ));
            match observation {
                Ok(Ok(output)) => observations.push(summarize(&output)),
                Ok(Err(error)) => {
                    observations.push(format!("public materialized state query failed: {error}"));
                }
                Err(_) => observations.push(
                    "public materialized state query exceeded its observation budget".to_string(),
                ),
            }
        }
    }
    observations
}

/// Keep every observed row and placement, but replace the fixture value with its byte length.
pub(crate) fn summarize(output: &str) -> String {
    let mut lines = Vec::new();
    for line in output.lines() {
        if line.starts_with("key=") {
            lines.push(summarize_row(line));
        } else {
            lines.push(line.to_string());
        }
    }
    lines.join("\n")
}

fn summarize_row(line: &str) -> String {
    let Some((key, body)) = line.split_once(" payload=") else {
        return "unrecognized public materialized row: no payload boundary".to_string();
    };
    let Some((payload, watermarks)) = body.rsplit_once(" low=") else {
        return "unrecognized public materialized row: no watermark boundary".to_string();
    };
    let row = match serde_json::from_str::<serde_json::Value>(payload) {
        Ok(row) => row,
        Err(error) => return format!("unreadable public materialized row: {error}"),
    };
    let round = match row.get("round") {
        Some(serde_json::Value::Number(round)) => round,
        _ => return format!("{key}: fixture round is not a number"),
    };
    let value_bytes = match row.get("value") {
        Some(serde_json::Value::String(value)) => value.len(),
        _ => return format!("{key}: fixture value is not a string"),
    };
    format!("{key} round={round} value_bytes={value_bytes} low={watermarks}")
}
