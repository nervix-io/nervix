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

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector_rabbitmq::{RabbitMqConsumerMode, RabbitMqSource, RabbitMqSourcePlan};
use nervix_models::RabbitMqIngestMode;

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
        let consumer_mode = match &mode {
            RabbitMqIngestMode::NoAck => RabbitMqConsumerMode::NoAck,
            RabbitMqIngestMode::AckSequential { .. } => RabbitMqConsumerMode::Ack {
                prefetch: std::num::NonZeroU16::MIN,
            },
            RabbitMqIngestMode::AckParallel { max, .. } => {
                RabbitMqConsumerMode::Ack { prefetch: *max }
            }
        };
        BrokerSourceStart {
            connector: RabbitMqSourcePlan::new(
                resolved.entries,
                dns.clone(),
                queue,
                ingestor.name.as_str().to_string(),
                consumer_mode,
            ),
            instances,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: false,
            flush_each_intake: false,
            // Auto-ack deliveries cannot be presented again. Refuse and count a payload when
            // the extension workers are full rather than holding the transport's read loop.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "rabbitmq",
        }
        .open::<RabbitMqSource>(ingestor)
        .await
    }
}
