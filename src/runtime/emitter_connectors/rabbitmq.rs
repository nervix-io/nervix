//! RabbitMq emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the RabbitMq connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

impl EmitterConnectorStarter {
    pub(super) async fn rabbitmq(
        connector: &RabbitMqEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
        label: &str,
        batch: Option<EmitterBatchPolicy>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Self::record(
            label,
            codec,
            batch,
            RabbitMqEmitter::new(
                RabbitMqEmitterConfig {
                    config: connector.client.config.entries.clone(),
                    dns: context.dns()?,
                    queue: connector.queue.clone(),
                    mode: connector.mode,
                },
                context.emitter_host(),
            )
            .await,
        )?;
        Ok(opened)
    }
}
