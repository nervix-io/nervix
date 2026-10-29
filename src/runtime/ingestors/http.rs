//! HTTP paced-ingestor runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the HTTP connector plan with the domain cadence the host polls it on.
//! - **Depends on.** The connector ingestor contract, typed HTTP plans and pre-resolved runtime
//!   handles.
//! - **Must not know.** HTTP request or response details, Plexus parsing, registry validation, or
//!   placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::IngestorConnector as _;
use nervix_connector_http::{HttpRequestIngestor, HttpRequestIngestorPlan};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{IngestorStart, PacedIngestorStart},
};

const _: () = assert!(matches!(
    HttpRequestIngestor::KIND,
    nervix_models::IngestorEntityKind::Http(nervix_models::HttpIngestorEntity::Request)
));

impl HttpRequestIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let HttpRequestIngestorStartPlan {
            client,
            method,
            headers,
            max_body_size,
            every,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let Some(dns) = runtime.dns() else {
            return Err(
                ingestor.connector_start_failure(IngestorConnectorStartError::NodeDnsUnavailable)
            );
        };
        PacedIngestorStart {
            connector: HttpRequestIngestorPlan {
                config: resolved.entries,
                dns: dns.clone(),
                method,
                headers,
                max_body_size,
            },
            every,
            cadence_start: DomainCadenceStart::Immediate,
            // A poll answers at most one message, its response body.
            intake_batch: NonZeroUsize::MIN,
            client_mounts: resolved.mounts.into_iter().collect(),
        }
        .open::<HttpRequestIngestor>(runtime, ingestor)
        .await
    }
}
