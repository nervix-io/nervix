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

use error_stack::ResultExt as _;
use nervix_connector_redis::{RedisPubSubSource, RedisPubSubSourcePlan};

use super::{
    super::*,
    IngestorStartError, SourceStartError,
    source::{BrokerSourceStart, SourceStart},
};

impl RedisPubSubIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let RedisPubSubIngestorStartPlan {
            client,
            channel,
            mode,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let Some(dns) = runtime.dns() else {
            return Err(ingestor.source_start_failure(SourceStartError::NodeDnsUnavailable));
        };
        let connector = RedisPubSubSourcePlan::new(resolved.entries, channel, dns.clone())
            .change_context_lazy(|| ingestor.initialize_failure())?;
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
