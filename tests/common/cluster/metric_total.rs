//! Public metric aggregation across a node's independently labeled series.
//!
//! Outside the layer order: a scenario harness.
//! - **Owns.** Summing every matching public sample and distinguishing absent metric families.
//! - **Depends on.** The cluster's HTTP metric reader and Prometheus sample parsing.
//! - **Must not know.** Product metric storage, runtime instances or placement implementation.

use super::{Cluster, io, line_starts_with_metric, parse_prometheus_sample_value};

impl Cluster {
    /// A known counter with no matching series contributes zero, including on a node without a
    /// matching scheduled instance. An unknown metric family remains an observation error.
    pub(crate) async fn sum_observability_metric(
        &self,
        node_id: &str,
        metric_name: &str,
        label_fragments: &[String],
    ) -> io::Result<f64> {
        let response = self.observability_metric_text(node_id).await?;
        let declaration = format!("# TYPE {metric_name} ");
        let counter_declaration = metric_name
            .strip_suffix("_total")
            .map(|family| format!("# TYPE {family} counter"));
        let mut present = false;
        let mut total = 0.0;
        for line in response.lines() {
            if line.starts_with(&declaration)
                || counter_declaration
                    .as_ref()
                    .is_some_and(|prefix| line == prefix)
            {
                present = true;
            }
            if line.starts_with('#') || !line_starts_with_metric(line, metric_name) {
                continue;
            }
            present = true;
            if label_fragments
                .iter()
                .all(|fragment| line.contains(fragment))
                && let Some(value) = parse_prometheus_sample_value(line)
            {
                total += value;
            }
        }
        if present {
            Ok(total)
        } else {
            Err(io::Error::other(format!(
                "node '{node_id}' exposed no metric family '{metric_name}'"
            )))
        }
    }
}
