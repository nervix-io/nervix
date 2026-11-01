//! NATS ingestor runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the NATS connector plans with host-owned intake: a subject's, whose
//!   quiesce buffers or drops what the queue subscription keeps receiving, and a stream's, whose
//!   quiesce suspends its pull or buffers or drops what it keeps pulling.
//! - **Depends on.** The connector ingestor contract, the NATS connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The NATS driver, subscription lifecycle, Plexus parsing, registry
//!   validation, or placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::IngestorConnector as _;
use nervix_connector_nats::{
    NatsStreamIngestor, NatsStreamIngestorPlan, NatsSubjectIngestor, NatsSubjectIngestorPlan,
};

use super::{
    super::*,
    IngestorStartError,
    connector::{BrokerIngestorStart, IngestorStart},
};

const _: () = assert!(matches!(
    NatsSubjectIngestor::KIND,
    nervix_models::IngestorEntityKind::Nats(nervix_models::NatsIngestorEntity::Subject)
));

const _: () = assert!(matches!(
    NatsStreamIngestor::KIND,
    nervix_models::IngestorEntityKind::Nats(nervix_models::NatsIngestorEntity::Stream)
));

impl NatsSubjectIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let NatsSubjectIngestorStartPlan {
            client,
            subject,
            queue_group,
            instances,
            mode,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerIngestorStart {
            connector: NatsSubjectIngestorPlan::new(resolved.entries, subject, queue_group),
            instances,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: false,
            // The client reads the subscription and answers the server from its own task while the
            // loop waits, holding at most its subscription capacity of 65,536 messages before it
            // drops the newest as a slow consumer.
            unacknowledged_admission: QueueAdmission::WaitForPlace,
            client_mounts: resolved.mounts.into_iter().collect(),
        }
        .open::<NatsSubjectIngestor>(ingestor)
        .await
    }
}

impl NatsStreamIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let NatsStreamIngestorStartPlan {
            client,
            stream,
            consumer,
            mode,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerIngestorStart {
            connector: NatsStreamIngestorPlan::new(resolved.entries, stream, consumer),
            instances: NonZeroU64::MIN,
            acknowledgement: mode.acknowledgement(),
            // Under BUFFER and DROP the pull keeps running while the ingestor is quiesced, and the
            // host decides on what it brings.
            buffered_intake: true,
            flush_each_intake: false,
            // Every delivery mode of this ingestor is acknowledged: a batch the extension workers
            // refuse is rejected and delivered again, and no payload is taken in unacknowledged.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved.mounts.into_iter().collect(),
        }
        .open::<NatsStreamIngestor>(ingestor)
        .await
    }
}
