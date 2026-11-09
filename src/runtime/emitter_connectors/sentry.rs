//! Sentry emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Sentry connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use nervix_connector::EmitterConnector as _;

use super::*;

const _: () = assert!(matches!(
    SentryEventEmitter::KIND,
    nervix_models::EmitterEntityKind::Sentry(nervix_models::SentryEmitterEntity::Event)
));

impl EmitterConnectorStarter {
    pub(super) async fn sentry(
        connector: &SentryEventEmitterPlan,
        context: &EmitterConnectorContext,
        codec: Option<&Arc<CompiledCodec>>,
        label: &str,
        batch: Option<EmitterBatchPolicy>,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Self::record(
            label,
            codec,
            batch,
            SentryEventEmitter::open(
                SentryEventEmitterConfig {
                    tls_policy: context.runtime.outward_tls().clone(),
                    config: connector.client.config.entries.clone(),
                    dns: context.dns()?,
                    platform: connector.platform.clone(),
                },
                context.emitter_host(),
            )
            .await,
        )?;
        Ok(opened)
    }
}
