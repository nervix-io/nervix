//! Redis Pub/Sub source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Redis Pub/Sub connector plan with host-owned intake, whose quiesce
//!   buffers or drops what the subscription keeps receiving.
//! - **Depends on.** The connector source contract, the Redis connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The Redis driver, subscription lifecycle, NSPL parsing, registry
//!   validation, or placement computation.

mod source;

use source::{RedisPubSubSource, RedisPubSubSourcePlan};

use super::{
    super::*,
    source::{BrokerSourceStart, DeclaredSourceAcknowledgement},
};

pub(in crate::runtime) struct RedisPubSubIngestor;

impl RedisPubSubIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: RedisPubSubIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let RedisPubSubIngestorStartPlan {
            ingestor,
            client,
            channel,
            mode,
        } = plan;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        let connector = RedisPubSubSourcePlan::new(resolved.entries, channel)
            .map_err(|error| ingestor.start_failure(format!("{error:#}")))?;
        runtime
            .start_broker_source::<RedisPubSubSource>(BrokerSourceStart {
                ingestor: &ingestor,
                connector,
                instances: NonZeroU64::MIN,
                acknowledgement: DeclaredSourceAcknowledgement::from(&mode),
                buffered_intake: true,
                flush_each_intake: false,
                client_mounts: resolved.mounts.into_iter().collect(),
                connector_label: "redis",
            })
            .await
    }
}
