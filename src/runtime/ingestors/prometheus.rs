//! Prometheus paced-ingestor runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Prometheus connector plan with the domain cadence the host polls it
//!   on, and taking a poll's samples in intake batches of one ingest group each, the bound the
//!   entity declares for its samples.
//! - **Depends on.** The connector ingestor contract, typed Prometheus plans and pre-resolved runtime
//!   handles.
//! - **Must not know.** Prometheus query or sample details, Plexus parsing, registry validation, or
//!   placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::IngestorConnector as _;
use nervix_connector_prometheus::{PrometheusQueryIngestor, PrometheusQueryIngestorPlan};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{IngestorStart, PacedIngestorStart},
};

const _: () = assert!(matches!(
    PrometheusQueryIngestor::KIND,
    nervix_models::IngestorEntityKind::Prometheus(nervix_models::PrometheusIngestorEntity::Query)
));

impl PrometheusQueryIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let PrometheusQueryIngestorStartPlan {
            client,
            query,
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
        // Each intake batch fills at most one ingest group, so a vector of any size enters the
        // graph group by group.
        let intake_batch = NonZeroUsize::new(INGEST_GROUP_MAX_ROWS)
            .assured("an ingest group holds at least one message");
        PacedIngestorStart {
            connector: PrometheusQueryIngestorPlan {
                config: resolved.entries,
                query,
                max_body_size,
                dns: dns.clone(),
            },
            every,
            cadence_start: DomainCadenceStart::AfterInterval,
            intake_batch,
            client_mounts: resolved.mounts.into_iter().collect(),
        }
        .open::<PrometheusQueryIngestor>(runtime, ingestor)
        .await
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::{
        BufferBound, BufferCapacity, BufferOverflow, IngestorDeclaration as _,
        PrometheusQueryIngestor,
    };

    use super::*;

    /// The samples per intake batch the entity declares are the ingest group bound the host takes
    /// each batch in with.
    #[test]
    fn the_declared_intake_batch_is_one_ingest_group() {
        let [_, intake_batch] = PrometheusQueryIngestor::CAPABILITIES.bounds.buffers else {
            panic!("a query source declares its response and its intake batch");
        };
        let group = u64::try_from(INGEST_GROUP_MAX_ROWS)
            .assured("the ingest group bound fits u64 on every supported target");
        assert_eq!(
            intake_batch.holds,
            "samples the host takes in as one intake batch"
        );
        assert_eq!(
            intake_batch.bound,
            BufferBound::Bounded {
                capacity: BufferCapacity::Items {
                    count: group,
                    unit: "samples",
                },
                overflow: BufferOverflow::Backpressure,
            }
        );
    }
}
