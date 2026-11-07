//! Sqs emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Sqs connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

const _: () = assert!(matches!(
    SqsQueueEmitter::KIND,
    nervix_models::EmitterEntityKind::Sqs(nervix_models::SqsEmitterEntity::Queue)
));

impl EmitterConnectorStarter {
    pub(super) async fn sqs(
        connector: &SqsQueueEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
        label: &str,
        batch: Option<EmitterBatchPolicy>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Self::record(
            label,
            codec,
            batch,
            SqsQueueEmitter::new(
                SqsQueueEmitterConfig {
                    config: connector.client.config.entries.clone(),
                    queue: connector.queue.clone(),
                    send: connector.send,
                    dns: context.dns()?,
                },
                context.emitter_host(),
            )
            .await,
        )?;
        Ok(opened)
    }
}
