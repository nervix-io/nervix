//! NATS source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the NATS connector plan with host-owned intake, whose quiesce buffers or
//!   drops what the queue subscription keeps receiving.
//! - **Depends on.** The connector source contract, the NATS connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The NATS driver, subscription lifecycle, NSPL parsing, registry
//!   validation, or placement computation.

mod source;

use source::{NatsSource, NatsSourcePlan};

use super::{
    super::*,
    source::{BrokerSourceStart, DeclaredSourceAcknowledgement},
};

pub(in crate::runtime) struct NatsIngestor;

impl NatsIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: NatsIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let NatsIngestorStartPlan {
            ingestor,
            client,
            subject,
            queue_group,
            instances,
            mode,
        } = plan;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        runtime
            .start_broker_source::<NatsSource>(BrokerSourceStart {
                ingestor: &ingestor,
                connector: NatsSourcePlan::new(resolved.entries, subject, queue_group),
                instances,
                acknowledgement: DeclaredSourceAcknowledgement::from(&mode),
                buffered_intake: true,
                flush_each_intake: false,
                client_mounts: resolved.mounts.into_iter().collect(),
                connector_label: "nats",
            })
            .await
    }
}
