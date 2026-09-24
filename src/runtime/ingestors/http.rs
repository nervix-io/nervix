//! HTTP paced-source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the HTTP connector plan with the domain cadence the host polls it on.
//! - **Depends on.** The connector source contract, typed HTTP plans and pre-resolved runtime
//!   handles.
//! - **Must not know.** HTTP request or response details, NSPL parsing, registry validation, or
//!   placement computation.

use nervix_connector_http::{HttpSource, HttpSourcePlan};

use super::{
    super::*,
    source::{PacedSourceStart, SourceStart},
};

impl HttpIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> Result<SourceStart, RuntimeError> {
        let HttpIngestorStartPlan { client, every } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        PacedSourceStart {
            connector: HttpSourcePlan {
                config: resolved.entries,
            },
            every,
            cadence_start: DomainCadenceStart::Immediate,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "http",
        }
        .open::<HttpSource>(runtime, ingestor)
        .await
    }
}
