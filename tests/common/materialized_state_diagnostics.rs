//! Small observations of a materialized fixture's public state after its row deadline expires.
//!
//! Layer: test harness.
//! - **Owns.** A bounded sequence of failure observations retaining placement, branch, round,
//!   value length and watermarks without copying large fixture values into failure output.
//! - **Depends on.** Typed node names, primitive timers, verified public materialized-state envelopes.
//! - **Must not know.** Runtime publications, checkpoint storage or restore installation internals.

use std::{future::Future, io, time::Duration};

use nervix_client_wire::{
    CellView, CellsView, StateInspection, StateInspectionResult, StateInspectionSection,
};
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
    Q: Future<Output = io::Result<StateInspection>>,
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

/// Keep placement, exact branch identity and fixture measurements without copying payload values.
pub(crate) fn summarize(state: &StateInspection) -> String {
    let entity = match state.entity.domain_node() {
        Some(entity) => format!("{}/{}", entity.domain, entity.identifier()),
        None => "cluster license".to_string(),
    };
    let mut lines = vec![format!("entity: {entity}")];
    let StateInspectionResult::Page(page) = &state.result else {
        if let StateInspectionResult::Rejected(reason) = &state.result {
            lines.push(format!("rejection: {}", reason.as_ref()));
        }
        return lines.join("\n");
    };
    lines.push(format!("rows: {}", page.counts.rows));
    for unit in &page.units {
        lines.push(format!(
            "unit: {:?} owner: {} replicas: {:?} origin: {:?}",
            unit.unit, unit.owner, unit.replicas, unit.origin
        ));
        for section in &unit.sections {
            for frame in &section.frames {
                let batch = frame.batch();
                let key = serde_json::to_string(&batch.branch_key())
                    .unwrap_or_else(|error| format!("unreadable branch: {error}"));
                for row in batch.rows() {
                    lines.push(summarize_row(&key, section, row));
                }
            }
        }
    }
    lines.join("\n")
}

fn summarize_row(key: &str, section: &StateInspectionSection, row: CellsView<'_>) -> String {
    let cell = |name: &str| {
        section
            .schema
            .fields
            .iter()
            .position(|field| field.name.as_str() == name)
            .and_then(|index| row.get(index))
    };
    let Some(CellView::I64(round)) = cell("round") else {
        return format!("key={key}: fixture round is not I64");
    };
    let Some(CellView::String(value)) = cell("value") else {
        return format!("key={key}: fixture value is not a string");
    };
    let Some(watermarks) = section.watermarks else {
        return format!("key={key}: watermark identities are absent");
    };
    let timestamp = |index| {
        usize::try_from(index)
            .ok()
            .and_then(|index| row.get(index))
            .and_then(|cell| match cell {
                CellView::Datetime(value) => Some(value.unix_nanos()),
                _ => None,
            })
    };
    format!(
        "key={key} round={round} value_bytes={} low={:?} high={:?}",
        value.len(),
        timestamp(watermarks.low),
        timestamp(watermarks.high)
    )
}
