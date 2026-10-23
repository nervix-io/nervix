//! ZeroMq emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the ZeroMq connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

const _: () = assert!(matches!(
    ZeroMqPushEmitter::KIND,
    nervix_models::EmitterEntityKind::ZeroMq(nervix_models::ZeroMqEmitterEntity::Push)
));

impl EmitterConnectorStarter {
    pub(super) async fn zeromq(
        connector: &ZeroMqPushEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
        label: &str,
        batch: Option<EmitterBatchPolicy>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Self::record(
            label,
            codec,
            batch,
            ZeroMqPushEmitter::new(
                ZeroMqPushEmitterConfig {
                    config: connector.client.config.entries.clone(),
                },
                context.emitter_host(),
            )
            .await,
        )?;
        Ok(opened)
    }
}
