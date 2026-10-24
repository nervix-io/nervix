//! Nats emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Nats connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

const _: () = assert!(matches!(
    NatsSubjectEmitter::KIND,
    nervix_models::EmitterEntityKind::Nats(nervix_models::NatsEmitterEntity::Subject)
));

impl EmitterConnectorStarter {
    pub(super) async fn nats(
        connector: &NatsSubjectEmitterPlan,
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
            NatsSubjectEmitter::new(
                NatsSubjectEmitterConfig {
                    config: connector.client.config.entries.clone(),
                    subject: connector.subject.clone(),
                    mode: connector.mode,
                    retry_policy: plan.retry_policy,
                },
                context.emitter_host(),
            )
            .await,
        )?;
        Ok(opened)
    }
}
