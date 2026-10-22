//! SQS source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the SQS connector plan with the declared acknowledgement policy and
//!   host-owned intake.
//! - **Depends on.** The connector source contract, the SQS connector, the node resolver, and
//!   pre-resolved runtime execution handles.
//! - **Must not know.** The SQS SDK, queue polling, NSPL parsing, registry validation, or
//!   placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector_sqs::{SqsIngestor, SqsIngestorPlan};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{BrokerIngestorStart, IngestorStart},
};

impl SqsIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let SqsIngestorStartPlan {
            client,
            queue,
            instances,
            mode,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let Some(dns) = runtime.dns() else {
            return Err(
                ingestor.connector_start_failure(IngestorConnectorStartError::NodeDnsUnavailable)
            );
        };
        // The plan builds its client without a request; each instance looks the queue up when it
        // resumes, so an unreachable service is a transient source failure, not a failed start.
        let connector = SqsIngestorPlan::new(&resolved.entries, queue, dns.clone())
            .await
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerIngestorStart {
            connector,
            instances,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: false,
            flush_each_intake: false,
            // Every delivery mode of this source is acknowledged: a batch the extension workers
            // refuse is rejected and delivered again, and no payload is taken in unacknowledged.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "sqs",
        }
        .open::<SqsIngestor>(ingestor)
        .await
    }
}
