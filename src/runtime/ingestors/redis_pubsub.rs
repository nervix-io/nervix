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

use nervix_connector_redis::{RedisPubSubSource, RedisPubSubSourcePlan};

use super::{
    super::*,
    source::{BrokerSourceStart, SourceStart},
};

impl RedisPubSubIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> Result<SourceStart, RuntimeError> {
        let RedisPubSubIngestorStartPlan {
            client,
            channel,
            mode,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
        let connector = RedisPubSubSourcePlan::new(resolved.entries, channel)
            .map_err(|error| ingestor.start_failure(format!("{error:#}")))?;
        BrokerSourceStart {
            connector,
            instances: NonZeroU64::MIN,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: false,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "redis",
        }
        .open::<RedisPubSubSource>(ingestor)
        .await
    }
}
