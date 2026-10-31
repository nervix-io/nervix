//! Redis Pub/Sub ingestor runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Redis Pub/Sub connector plan with host-owned intake, whose quiesce
//!   buffers or drops what the subscription keeps receiving.
//! - **Depends on.** The connector ingestor contract, the Redis connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The Redis driver, subscription lifecycle, Plexus parsing, registry
//!   validation, or placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::IngestorConnector as _;
use nervix_connector_redis::{RedisPubSubIngestor, RedisPubSubIngestorPlan};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{BrokerIngestorStart, IngestorStart},
};

const _: () = assert!(matches!(
    RedisPubSubIngestor::KIND,
    nervix_models::IngestorEntityKind::Redis(nervix_models::RedisIngestorEntity::PubSub)
));

impl RedisPubSubIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let RedisPubSubIngestorStartPlan {
            client,
            subscription,
            mode,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let Some(dns) = runtime.dns() else {
            return Err(
                ingestor.connector_start_failure(IngestorConnectorStartError::NodeDnsUnavailable)
            );
        };
        let connector = RedisPubSubIngestorPlan::new(resolved.entries, subscription, dns.clone())
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerIngestorStart {
            connector,
            instances: NonZeroU64::MIN,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: false,
            // The bounded reader applies TCP backpressure when full. Keep polling so idle
            // probes remain active instead of holding this loop at executor admission.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved.mounts.into_iter().collect(),
        }
        .open::<RedisPubSubIngestor>(ingestor)
        .await
    }
}
