//! RabbitMQ source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the RabbitMQ connector plan with the declared acknowledgement policy and
//!   host-owned intake.
//! - **Depends on.** The connector source contract, the RabbitMQ connector, and pre-resolved
//!   runtime execution handles.
//! - **Must not know.** The AMQP driver, channel lifecycle, NSPL parsing, registry validation, or
//!   placement computation.

use nervix_connector_rabbitmq::{RabbitMqSource, RabbitMqSourcePlan};

use super::{
    super::*,
    source::{BrokerSourceStart, DeclaredSourceAcknowledgement},
};

pub(in crate::runtime) struct RabbitMqIngestor;

impl RabbitMqIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: RabbitMqIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let RabbitMqIngestorStartPlan {
            ingestor,
            client,
            queue,
            instances,
            mode,
        } = plan;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        runtime
            .start_broker_source::<RabbitMqSource>(BrokerSourceStart {
                ingestor: &ingestor,
                connector: RabbitMqSourcePlan::new(
                    resolved.entries,
                    queue,
                    ingestor.name.as_str().to_string(),
                ),
                instances,
                acknowledgement: DeclaredSourceAcknowledgement::from(&mode),
                buffered_intake: false,
                flush_each_intake: false,
                client_mounts: resolved.mounts.into_iter().collect(),
                connector_label: "rabbitmq",
            })
            .await
    }
}
