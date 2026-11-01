//! MQTT ingestor runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the MQTT connector plan, with one rendered client configuration per
//!   instance, the node resolver its connections resolve the broker through, the declared
//!   acknowledgement policy, and host-owned intake whose quiesce either disconnects the session or
//!   buffers and drops what stays subscribed, and retiring inputs through their connector plans.
//! - **Depends on.** The connector ingestor contract, the MQTT connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The MQTT driver, session protocol, Plexus parsing, registry validation, or
//!   placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::{ClientResourceMounts, IngestorConnector as _};
use nervix_connector_mqtt::{
    MqttTopicIngestor, MqttTopicIngestorError, MqttTopicIngestorPlan, MqttTopicIngestorSettings,
};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{BrokerIngestorStart, IngestorStart},
};

/// The resolved connector plan and the resource mounts kept alive while it is used.
struct ResolvedMqttIngestor {
    connector: MqttTopicIngestorPlan,
    client_mounts: Vec<Arc<ClientResourceMounts>>,
}

const _: () = assert!(matches!(
    MqttTopicIngestor::KIND,
    nervix_models::IngestorEntityKind::Mqtt(nervix_models::MqttIngestorEntity::Topic)
));

impl ResolvedMqttIngestor {
    /// Runs at most the declared instance count of cleanup connections concurrently. The resource
    /// mounts remain borrowed until every attempt finishes; all typed failures are returned.
    async fn retire_instances(&self, instances: Vec<u64>) -> Vec<Report<MqttTopicIngestorError>> {
        let mut cleanups = FuturesUnordered::new();
        for instance in instances {
            cleanups.push(self.connector.retire(instance));
        }
        let mut failures = Vec::new();
        while let Some(result) = cleanups.next().await {
            nervix_primitives::task::consume_budget().await;
            if let Err(error) = result {
                failures.push(error);
            }
        }
        failures
    }
}

impl MqttTopicIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let ResolvedMqttIngestor {
            connector,
            client_mounts,
        } = self
            .resolve(runtime, ingestor)
            .await
            .change_context_lazy(|| ingestor.initialize_failure())?;
        BrokerIngestorStart {
            connector,
            instances: self.instances,
            acknowledgement: self.mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: self.mode.is_ack(),
            // The connection reads a bounded number of publishes ahead of the loop and then stops
            // reading, which stops its keep-alive too, so a held loop would let the broker close
            // the connection once one and a half keep-alive intervals pass without a packet from
            // it, and lose what was published until the ingestor reconnects.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts,
        }
        .open::<MqttTopicIngestor>(ingestor)
        .await
    }

    async fn resolve(
        &self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<ResolvedMqttIngestor, IngestorConnectorStartError> {
        let mut instance_configs = Vec::with_capacity(self.instances.get().arch_into());
        let mut client_mounts = Vec::new();
        for instance_index in 0..self.instances.get() {
            nervix_primitives::task::consume_budget().await;
            let resolved = runtime
                .resolve_client_config_with_instance(
                    &ingestor.domain,
                    self.client.mount.as_ref(),
                    &self.client.config,
                    instance_index,
                )
                .change_context(IngestorConnectorStartError::ClientConfiguration)?;
            instance_configs.push(resolved.entries);
            if let Some(mounts) = resolved.mounts {
                client_mounts.push(mounts);
            }
        }
        let Some(dns) = runtime.dns() else {
            return Err(Report::new(IngestorConnectorStartError::NodeDnsUnavailable));
        };
        let default_client_id = format!("{}~{}", ingestor.domain.as_str(), ingestor.name.as_str());
        let dispatcher = runtime.inner.remote_dispatcher.load();
        let read_ahead_dropped = runtime.inner.metrics.ingestor_read_ahead_dropped_payloads(
            &ingestor.domain,
            &ingestor.name,
            dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
        );
        let connector = MqttTopicIngestorPlan::new(MqttTopicIngestorSettings {
            instances: instance_configs,
            client_id_conflict: MqttTopicIngestorPlan::client_id_conflict(
                &self.client.config,
                self.instances,
            ),
            share_group: default_client_id.clone(),
            default_client_id,
            topic: self.topic.as_str().to_string(),
            session: self.session,
            qos: self.qos,
            acknowledgement_window: self.mode.acknowledgement_window(),
            dns: dns.clone(),
            read_ahead_dropped,
        });
        Ok(ResolvedMqttIngestor {
            connector,
            client_mounts,
        })
    }

    pub(super) async fn retire_before(
        &self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
        replacement: Option<&Self>,
    ) -> error_stack::Result<(), RuntimeError> {
        let context = || RuntimeError::RetireIngestorEntity {
            domain: ingestor.domain.clone(),
            ingestor: ingestor.name.clone(),
        };
        let replacement = match replacement {
            Some(replacement) => Some(
                replacement
                    .resolve(runtime, ingestor)
                    .await
                    .change_context_lazy(context)?,
            ),
            None => None,
        };
        let replacement_sessions = match replacement.as_ref() {
            Some(replacement) => Some(
                replacement
                    .connector
                    .sessions()
                    .change_context_lazy(context)?,
            ),
            None => None,
        };
        // An invalid or unavailable previous configuration remains a reported cleanup failure.
        // The explicitly supplied replacement is prepared independently at its own endpoint.
        let previous = match self.resolve(runtime, ingestor).await {
            Ok(previous) => Some(previous),
            Err(error) => {
                let error = error.change_context(context());
                runtime.events().report_error(format!("{error:#}"));
                None
            }
        };
        let previous_sessions = match previous.as_ref() {
            Some(previous) => match previous.connector.sessions() {
                Ok(sessions) => Some(sessions),
                Err(error) => {
                    let error = error.change_context(context());
                    runtime.events().report_error(format!("{error:#}"));
                    None
                }
            },
            None => None,
        };
        if let Some(previous) = previous.as_ref()
            && let Some(sessions) = previous_sessions.as_ref()
        {
            let instances = sessions.retiring_instances(replacement_sessions.as_ref());
            for error in previous.retire_instances(instances).await {
                nervix_primitives::task::consume_budget().await;
                let error = error.change_context(context());
                runtime.events().report_error(format!("{error:#}"));
            }
        }
        let Some(replacement) = replacement.as_ref() else {
            return Ok(());
        };
        let sessions = replacement_sessions.as_ref().verified(
            "the Some(replacement) arm above validated and retained this replacement's sessions",
        );
        // A changed subscription starts from a clean session at the new endpoint even when
        // cleanup at the preceding endpoint failed. Sessions unchanged by the transition stay.
        let instances = sessions.retiring_instances(previous_sessions.as_ref());
        let mut first_failure = None;
        for error in replacement.retire_instances(instances).await {
            nervix_primitives::task::consume_budget().await;
            let error = error.change_context(context());
            if first_failure.is_none() {
                first_failure = Some(error);
            } else {
                runtime.events().report_error(format!("{error:#}"));
            }
        }
        match first_failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
