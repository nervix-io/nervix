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

use error_stack::ResultExt as _;
use nervix_connector_sqs::{SqsSource, SqsSourcePlan};

use super::{
    super::*,
    IngestorStartError, SourceStartError,
    source::{BrokerSourceStart, SourceStart},
};

impl SqsIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
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
            return Err(ingestor.source_start_failure(SourceStartError::NodeDnsUnavailable));
        };
        let connector = SqsSourcePlan::connect(&resolved.entries, &queue, dns.clone())
            .await
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerSourceStart {
            connector,
            instances,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: false,
            flush_each_intake: false,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "sqs",
        }
        .open::<SqsSource>(ingestor)
        .await
    }
}
