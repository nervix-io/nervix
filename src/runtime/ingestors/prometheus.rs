//! Prometheus paced-source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Prometheus connector plan with the domain cadence the host polls it
//!   on, and taking a poll's samples in intake batches of one ingest group each, the bound the
//!   entity declares for its samples.
//! - **Depends on.** The connector source contract, typed Prometheus plans and pre-resolved runtime
//!   handles.
//! - **Must not know.** Prometheus query or sample details, NSPL parsing, registry validation, or
//!   placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector_prometheus::{PrometheusSource, PrometheusSourcePlan};

use super::{
    super::*,
    IngestorStartError, SourceStartError,
    source::{PacedSourceStart, SourceStart},
};

impl PrometheusIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let PrometheusIngestorStartPlan {
            client,
            query,
            max_body_size,
            every,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let Some(dns) = runtime.dns() else {
            return Err(ingestor.source_start_failure(SourceStartError::NodeDnsUnavailable));
        };
        // Each intake batch fills at most one ingest group, so a vector of any size enters the
        // graph group by group.
        let intake_batch = NonZeroUsize::new(INGEST_GROUP_MAX_ROWS)
            .assured("an ingest group holds at least one message");
        PacedSourceStart {
            connector: PrometheusSourcePlan {
                config: resolved.entries,
                query,
                max_body_size,
                dns: dns.clone(),
            },
            every,
            cadence_start: DomainCadenceStart::AfterInterval,
            intake_batch,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "prometheus",
        }
        .open::<PrometheusSource>(runtime, ingestor)
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
        BufferBound, BufferCapacity, BufferOverflow, PrometheusQuerySource, SourceEntity as _,
    };

    use super::*;

    /// The samples per intake batch the entity declares are the ingest group bound the host takes
    /// each batch in with.
    #[test]
    fn the_declared_intake_batch_is_one_ingest_group() {
        let [_, intake_batch] = PrometheusQuerySource::CAPABILITIES.bounds.buffers else {
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
