//! MQTT source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the MQTT connector plan, with one rendered client configuration per
//!   instance, the node resolver its connections resolve the broker through, the declared
//!   acknowledgement policy, and host-owned intake whose quiesce either disconnects the session or
//!   buffers and drops what stays subscribed.
//! - **Depends on.** The connector source contract, the MQTT connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The MQTT driver, session lifecycle, NSPL parsing, registry validation, or
//!   placement computation.

use error_stack::ResultExt as _;
use nervix_connector_mqtt::{MqttSource, MqttSourcePlan, MqttSourceSettings};

use super::{
    super::*,
    IngestorStartError, SourceStartError,
    source::{BrokerSourceStart, SourceStart},
};

impl MqttIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let MqttIngestorStartPlan {
            client,
            topic,
            instances,
            mode,
        } = self;
        let mut instance_configs = Vec::with_capacity(instances.get().arch_into());
        let mut client_mounts = Vec::new();
        for instance_index in 0..instances.get() {
            let resolved = runtime
                .resolve_client_config_with_instance(
                    &ingestor.domain,
                    client.mount.as_ref(),
                    &client.config,
                    instance_index,
                )
                .change_context_lazy(|| ingestor.initialize_failure())?;
            instance_configs.push(resolved.entries);
            if let Some(mounts) = resolved.mounts {
                client_mounts.push(mounts);
            }
        }
        let Some(dns) = runtime.dns() else {
            return Err(ingestor.source_start_failure(SourceStartError::NodeDnsUnavailable));
        };
        let connector = MqttSourcePlan::new(MqttSourceSettings {
            instances: instance_configs,
            client_id_conflict: MqttSourcePlan::client_id_conflict(&client.config, instances),
            default_client_id: ingestor.name.as_str().to_string(),
            share_group: format!("{}~{}", ingestor.domain.as_str(), ingestor.name.as_str()),
            topic,
            session: mode.session(),
            qos: mode.qos(),
            manual_acks: mode.is_ack(),
            dns: dns.clone(),
        });
        BrokerSourceStart {
            connector,
            instances,
            acknowledgement: mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: mode.is_ack(),
            client_mounts,
            connector_label: "mqtt",
        }
        .open::<MqttSource>(ingestor)
        .await
    }
}
