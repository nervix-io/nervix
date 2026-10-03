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

use error_stack::ResultExt as _;
use nervix_connector_nats::{NatsSource, NatsSourcePlan};

use super::{
    super::*,
    IngestorStartError,
    source::{BrokerSourceStart, SourceStart},
};

impl NatsIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let NatsIngestorStartPlan {
            client,
            subject,
            queue_group,
            instances,
            mode,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerSourceStart {
            connector: NatsSourcePlan::new(resolved.entries, subject, queue_group),
            instances,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: false,
            // The client reads the subscription and answers the server from its own task while the
            // loop waits, holding at most its subscription capacity of 65,536 messages before it
            // drops the newest as a slow consumer.
            unacknowledged_admission: QueueAdmission::WaitForPlace,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "nats",
        }
        .open::<NatsSource>(ingestor)
        .await
    }
}
