//! Pulsar source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Pulsar connector plan with the declared acknowledgement policy and
//!   host-owned intake.
//! - **Depends on.** The connector source contract, the Pulsar connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The Pulsar driver, consumer lifecycle, Plexus parsing, registry validation,
//!   or placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector_pulsar::{
    PulsarAckReceipts, PulsarSource, PulsarSourcePlan, PulsarSourceSettings,
};
use nervix_models::IngestAcknowledgement;

use super::{
    super::*,
    IngestorStartError,
    source::{BrokerSourceStart, SourceStart},
};

impl PulsarIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let PulsarIngestorStartPlan {
            client,
            topic,
            subscription,
            instances,
            mode,
        } = self;
        let resolved = runtime
            .resolve_client_config(&ingestor.domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let acknowledgement = mode.acknowledgement();
        // A mode without an acknowledgement tree reads one message at a time, so only an
        // acknowledged mode waits for the broker to answer what it acknowledges.
        let ack_receipts = match acknowledgement {
            IngestAcknowledgement::Unacknowledged => PulsarAckReceipts::Unawaited,
            IngestAcknowledgement::Sequential { .. } | IngestAcknowledgement::Parallel { .. } => {
                PulsarAckReceipts::Awaited
            }
        };
        let connector = PulsarSourcePlan::connect(PulsarSourceSettings {
            config: &resolved.entries,
            topic: &topic,
            subscription,
            consumer_name: ingestor.name.as_str().to_string(),
            ack_receipts,
        })
        .await
        .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerSourceStart {
            connector,
            instances,
            acknowledgement,
            buffered_intake: false,
            flush_each_intake: false,
            // A consumer whose receive queue fills while the loop is held cannot complete its close,
            // so a later suspension or stop would wait on it. A refused message stays
            // unacknowledged until the consumer reconnects and the broker delivers it again.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved.mounts.into_iter().collect(),
            connector_label: "pulsar",
        }
        .open::<PulsarSource>(ingestor)
        .await
    }
}
