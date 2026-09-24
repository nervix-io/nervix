//! Prometheus paced-source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Prometheus connector plan with the domain cadence the host polls it
//!   on.
//! - **Depends on.** The connector source contract, typed Prometheus plans and pre-resolved runtime
//!   handles.
//! - **Must not know.** Prometheus query or sample details, NSPL parsing, registry validation, or
//!   placement computation.

use nervix_connector_prometheus::{PrometheusSource, PrometheusSourcePlan};

use super::{
    super::*,
    source::{PacedSourceStart, SourceStart},
};

impl PrometheusIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> Result<SourceStart, RuntimeError> {
        let PrometheusIngestorStartPlan {
            client,
            query,
            every,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        PacedSourceStart {
            connector: PrometheusSourcePlan {
                config: resolved.entries,
                query,
            },
            every,
            cadence_start: DomainCadenceStart::AfterInterval,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "prometheus",
        }
        .open::<PrometheusSource>(runtime, ingestor)
        .await
    }
}
