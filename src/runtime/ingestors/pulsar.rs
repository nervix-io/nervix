//! Pulsar source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Pulsar connector plan with the declared acknowledgement policy and
//!   host-owned intake.
//! - **Depends on.** The connector source contract, the Pulsar connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The Pulsar driver, consumer lifecycle, NSPL parsing, registry validation,
//!   or placement computation.

mod source;

use source::{PulsarSource, PulsarSourcePlan, PulsarSourceSettings};

use super::{
    super::*,
    source::{BrokerSourceStart, DeclaredSourceAcknowledgement},
};

pub(in crate::runtime) struct PulsarIngestor;

impl PulsarIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: PulsarIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let PulsarIngestorStartPlan {
            ingestor,
            client,
            topic,
            subscription,
            instances,
            mode,
        } = plan;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        let connector = PulsarSourcePlan::connect(PulsarSourceSettings {
            config: &resolved.entries,
            topic: &topic,
            subscription,
            consumer_name: ingestor.name.as_str().to_string(),
        })
        .await
        .map_err(|error| ingestor.start_failure(format!("{error:#}")))?;
        runtime
            .start_broker_source::<PulsarSource>(BrokerSourceStart {
                ingestor: &ingestor,
                connector,
                instances,
                acknowledgement: DeclaredSourceAcknowledgement::from(&mode),
                buffered_intake: false,
                flush_each_intake: false,
                client_mounts: resolved.mounts.into_iter().collect(),
                connector_label: "pulsar",
            })
            .await
    }
}
