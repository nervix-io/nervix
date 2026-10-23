//! Mqtt emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Mqtt connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

const _: () = assert!(matches!(
    MqttTopicEmitter::KIND,
    nervix_models::EmitterEntityKind::Mqtt(nervix_models::MqttEmitterEntity::Topic)
));

impl EmitterConnectorStarter {
    pub(super) fn mqtt(
        connector: &MqttTopicEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
        label: &str,
        batch: Option<EmitterBatchPolicy>,
        plan: &EmitterStartPlan,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Self::record(
            label,
            codec,
            batch,
            MqttTopicEmitter::new(
                MqttTopicEmitterConfig {
                    config: connector.client.config.entries.clone(),
                    topic: connector.topic.clone(),
                    mode: connector.mode,
                    retry_policy: plan.retry_policy,
                    default_client_id: format!(
                        "{}~{}",
                        context.domain.as_str(),
                        context.emitter.as_str()
                    ),
                    dns: context.dns()?,
                },
                context.emitter_host(),
            ),
        )?;
        Ok(opened)
    }
}
