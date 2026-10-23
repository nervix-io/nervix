//! Redis emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Redis connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

const _: () = assert!(matches!(
    RedisPubSubEmitter::KIND,
    nervix_models::EmitterEntityKind::Redis(nervix_models::RedisEmitterEntity::PubSub)
));

impl EmitterConnectorStarter {
    pub(super) async fn redis(
        connector: &RedisPubSubEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
        label: &str,
        batch: Option<EmitterBatchPolicy>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = {
            let pool = context.lease_redis_pool(connector).await?;
            Self::record(
                label,
                codec,
                batch,
                RedisPubSubEmitter::new(
                    RedisPubSubEmitterConfig {
                        pool,
                        channel: connector.channel.clone(),
                    },
                    context.emitter_host(),
                ),
            )?
        };
        Ok(opened)
    }
}
