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
        let intake_batch = intake_batch_bound(runtime.inner.data_plane.ingest_group_max_rows());
        PacedIngestorStart {
            connector: PrometheusQueryIngestorPlan {
                tls_policy: runtime.outward_tls().clone(),
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

/// Respect both the local grouping choice and the entity's declared intake memory bound.
fn intake_batch_bound(group: u64) -> NonZeroUsize {
    use nervix_models::{BufferBound, BufferCapacity, IngestorDeclaration as _};
    let [_, intake] = nervix_models::PrometheusQueryIngestor::CAPABILITIES
        .bounds
        .buffers
    else {
        unreachable!("the query declaration names its response and sample intake")
    };
    let BufferBound::Bounded {
        capacity: BufferCapacity::Items { count, .. },
        ..
    } = intake.bound
    else {
        unreachable!("the sample intake declares an item bound")
    };
    NonZeroUsize::new(group.min(count).arch_into()).assured("both declared bounds are positive")
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
            panic!("a query ingestor declares its response and its intake batch");
        };
        let group = nervix_models::DataPlaneSettings::default().ingest_group_max_rows();
        assert_eq!(intake_batch_bound(3).get(), 3);
        assert_eq!(
            intake_batch_bound(group * 2).get(),
            intake_batch_bound(group).get()
        );
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
