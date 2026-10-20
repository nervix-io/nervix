//! Pulsar source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Pulsar connector plan with the declared acknowledgement policy,
//!   host-owned intake and the domain-qualified name its consumers register under, and retiring
//!   an input by removing its owned subscription when the replacement no longer reads it.
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
use nervix_models::{ClientConfigEntry, IngestAcknowledgement};

use super::{
    super::*,
    IngestorStartError, SourceStartError,
    source::{BrokerSourceStart, SourceStart},
};

impl PulsarIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let resolved = runtime
            .resolve_client_config(
                &ingestor.domain,
                self.client.mount.as_ref(),
                &self.client.config,
            )
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let connector = PulsarSourcePlan::connect(self.settings(&resolved.entries, ingestor))
            .await
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerSourceStart {
            connector,
            instances: self.instances,
            acknowledgement: self.mode.acknowledgement(),
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

    /// Removes an owned subscription the replacement does not read, after this input's exact
    /// source tasks stop. The caller reports failures best-effort and still starts the replacement.
    pub(super) async fn retire_before(
        &self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
        replacement: Option<&Self>,
    ) -> error_stack::Result<(), RuntimeError> {
        let context = || RuntimeError::RetireIngestorSource {
            domain: ingestor.domain.clone(),
            ingestor: ingestor.name.clone(),
        };
        // The resolved client keeps the lease of its pinned resource mounts until the removal
        // has finished with them.
        let resolved = runtime
            .resolve_client_config(
                &ingestor.domain,
                self.client.mount.as_ref(),
                &self.client.config,
            )
            .change_context(SourceStartError::ClientConfiguration)
            .change_context_lazy(context)?;
        let settings = self.settings(&resolved.entries, ingestor);
        if let Some(replacement) = replacement {
            let replacement_config = runtime
                .resolve_client_config(
                    &ingestor.domain,
                    replacement.client.mount.as_ref(),
                    &replacement.client.config,
                )
                .change_context(SourceStartError::ClientConfiguration)
                .change_context_lazy(context)?;
            let replacement_settings = replacement.settings(&replacement_config.entries, ingestor);
            let retained = settings
                .reads_same_subscription(&replacement_settings)
                .change_context_lazy(context)?;
            if retained {
                return Ok(());
            }
        }
        PulsarSourcePlan::remove_subscription(settings)
            .await
            .change_context_lazy(context)
    }

    /// What this input's connector subscribes with. Its consumers register under the ingestor's
    /// name qualified by its domain, so ingestors of one name in different domains are told apart
    /// on the broker.
    fn settings<'a>(
        &'a self,
        config: &'a [ClientConfigEntry],
        ingestor: &IngestorSpec,
    ) -> PulsarSourceSettings<'a> {
        // A mode without an acknowledgement tree reads one message at a time, so only an
        // acknowledged mode waits for the broker to answer what it acknowledges.
        let ack_receipts = match self.mode.acknowledgement() {
            IngestAcknowledgement::Unacknowledged => PulsarAckReceipts::Unawaited,
            IngestAcknowledgement::Sequential { .. } | IngestAcknowledgement::Parallel { .. } => {
                PulsarAckReceipts::Awaited
            }
        };
        PulsarSourceSettings {
            config,
            topic: &self.topic,
            subscription: self.subscription.clone(),
            consumer_name: format!("{}~{}", ingestor.domain.as_str(), ingestor.name.as_str()),
            ack_receipts,
        }
    }
}
