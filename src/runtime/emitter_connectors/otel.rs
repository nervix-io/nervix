//! Otel emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Otel connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

impl EmitterConnectorStarter {
    pub(super) fn otel(
        connector: &OtelEmitterPlan,
        context: &EmitterConnectorContext,
        input_schema: &CompiledSchema,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = {
            // The signal's own values and its attributes are mapped as one program, so the
            // attribute columns follow the signal's own in the batch the host projects.
            let projection = Self::projection(MappedValuesProjectionInit {
                label: "OTEL",
                namespace: "otel",
                emitter: &context.emitter,
                mapping: &connector.mapping,
                input_schema: input_schema.arrow_schema(),
                udfs: context.udfs.as_ref(),
            })?;
            let config = OtelEmitterConfig {
                config: connector.client.config.entries.clone(),
                dns: context.dns()?,
                signal: connector.signal.clone(),
                batch: connector.batch,
                values: connector.values.clone(),
                attributes: connector.attributes.clone(),
                resource: connector.resource.clone(),
                scope: connector.scope.clone(),
                mapped_schema: projection.mapped_schema().clone(),
            };
            Self::row_request(projection, OtelEmitter::new(config, context.emitter_host()))?
        };
        Ok(opened)
    }
}
