//! Http emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Http connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

impl EmitterConnectorStarter {
    pub(super) fn http(
        connector: &HttpEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Self::http_request(
            codec,
            HttpEmitter::new(
                HttpEmitterConfig {
                    config: connector.client.config.entries.clone(),
                    dns: context.dns()?,
                },
                context.emitter_host(),
            ),
        )?;
        Ok(opened)
    }
}
