//! Kafka emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Kafka connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

const _: () = assert!(matches!(
    KafkaTopicEmitter::KIND,
    nervix_models::EmitterEntityKind::Kafka(nervix_models::KafkaEmitterEntity::Topic)
));

impl EmitterConnectorStarter {
    pub(super) fn kafka(
        connector: &KafkaTopicEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
        label: &str,
        batch: Option<EmitterBatchPolicy>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Self::record(
            label,
            codec,
            batch,
            KafkaTopicEmitter::new(
                KafkaTopicEmitterConfig {
                    config: connector.client.config.entries.clone(),
                    topic: connector.topic.clone(),
                    mode: connector.mode,
                },
                context.emitter_host(),
            ),
        )?;
        Ok(opened)
    }
}
