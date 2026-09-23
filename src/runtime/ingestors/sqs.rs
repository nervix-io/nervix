//! SQS source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the SQS connector plan with the declared acknowledgement policy and
//!   host-owned intake.
//! - **Depends on.** The connector source contract, the SQS connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The SQS SDK, queue polling, NSPL parsing, registry validation, or
//!   placement computation.

use nervix_connector_sqs::{SqsSource, SqsSourcePlan};

use super::{
    super::*,
    source::{BrokerSourceStart, DeclaredSourceAcknowledgement},
};

pub(in crate::runtime) struct SqsIngestor;

impl SqsIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: SqsIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let SqsIngestorStartPlan {
            ingestor,
            client,
            queue,
            instances,
            mode,
        } = plan;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        let connector = SqsSourcePlan::connect(&resolved.entries, &queue)
            .await
            .map_err(|error| ingestor.start_failure(format!("{error:#}")))?;
        runtime
            .start_broker_source::<SqsSource>(BrokerSourceStart {
                ingestor: &ingestor,
                connector,
                instances,
                acknowledgement: DeclaredSourceAcknowledgement::from(&mode),
                buffered_intake: false,
                flush_each_intake: false,
                client_mounts: resolved.mounts.into_iter().collect(),
                connector_label: "sqs",
            })
            .await
    }
}
