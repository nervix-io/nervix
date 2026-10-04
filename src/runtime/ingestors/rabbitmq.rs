//! RabbitMQ source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the RabbitMQ connector plan with the declared acknowledgement policy and
//!   host-owned intake.
//! - **Depends on.** The connector source contract, the RabbitMQ connector, the node resolver,
//!   and pre-resolved runtime execution handles.
//! - **Must not know.** The AMQP driver, channel lifecycle, NSPL parsing, registry validation, or
//!   placement computation.

use error_stack::ResultExt as _;
use nervix_connector_rabbitmq::{RabbitMqSource, RabbitMqSourcePlan};

use super::{
    super::*,
    IngestorStartError, SourceStartError,
    source::{BrokerSourceStart, SourceStart},
};

impl RabbitMqIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let RabbitMqIngestorStartPlan {
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
        BrokerSourceStart {
            connector: RabbitMqSourcePlan::new(
                resolved.entries,
                dns.clone(),
                queue,
                ingestor.name.as_str().to_string(),
            ),
            instances,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: false,
            flush_each_intake: false,
            // Every delivery mode of this source is acknowledged: a batch the extension workers
            // refuse is rejected and delivered again, and no payload is taken in unacknowledged.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "rabbitmq",
        }
        .open::<RabbitMqSource>(ingestor)
        .await
    }
}
