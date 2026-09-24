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

use nervix_connector_pulsar::{PulsarSource, PulsarSourcePlan, PulsarSourceSettings};

use super::{
    super::*,
    source::{BrokerSourceStart, SourceStart},
};

impl PulsarIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> Result<SourceStart, RuntimeError> {
        let PulsarIngestorStartPlan {
            client,
            topic,
            subscription,
            instances,
            mode,
        } = self;
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
        BrokerSourceStart {
            connector,
            instances,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: false,
            flush_each_intake: false,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "pulsar",
        }
        .open::<PulsarSource>(ingestor)
        .await
    }
}
