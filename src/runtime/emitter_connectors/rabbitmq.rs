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

const _: () = assert!(matches!(
    RabbitMqQueueEmitter::KIND,
    nervix_models::EmitterEntityKind::RabbitMq(nervix_models::RabbitMqEmitterEntity::Queue)
));
const _: () = assert!(matches!(
    RabbitMqExchangeEmitter::KIND,
    nervix_models::EmitterEntityKind::RabbitMq(nervix_models::RabbitMqEmitterEntity::Exchange)
));

impl EmitterConnectorStarter {
    pub(super) async fn rabbitmq(
        connector: &RabbitMqQueueEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
        label: &str,
        batch: Option<EmitterBatchPolicy>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Self::record(
            label,
            codec,
            batch,
            RabbitMqQueueEmitter::new(
                RabbitMqQueueEmitterConfig {
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

    pub(super) async fn rabbitmq_exchange(
        connector: &RabbitMqExchangeEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
        label: &str,
        batch: Option<EmitterBatchPolicy>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Self::record(
            label,
            codec,
            batch,
            RabbitMqExchangeEmitter::new(
                RabbitMqExchangeEmitterConfig {
                    config: connector.client.config.entries.clone(),
                    dns: context.dns()?,
                    exchange: connector.exchange.clone(),
                    mode: connector.mode,
                },
                context.emitter_host(),
            )
            .await,
        )?;
        Ok(opened)
    }
}
