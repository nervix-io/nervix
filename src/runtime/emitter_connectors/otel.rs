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

const _: () = assert!(matches!(
    OtelMetricsEmitter::KIND,
    nervix_models::EmitterEntityKind::Otel(nervix_models::OtelEmitterEntity::Metrics)
));

const _: () = assert!(matches!(
    OtelTracesEmitter::KIND,
    nervix_models::EmitterEntityKind::Otel(nervix_models::OtelEmitterEntity::Traces)
));

const _: () = assert!(matches!(
    OtelLogsEmitter::KIND,
    nervix_models::EmitterEntityKind::Otel(nervix_models::OtelEmitterEntity::Logs)
));

impl EmitterConnectorStarter {
    pub(super) fn otel(
        connector: &OtelEmitterPlan,
        context: &EmitterConnectorContext,
        input_schema: &CompiledSchema,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        macro_rules! open {
            ($entity:ident, $config:ident, $plan:expr $(, $metric:expr)?) => {{
                let connector = &$plan.export;
                let projection = Self::projection(MappedValuesProjectionInit {
                    label: $entity::KIND.connector().keyword(),
                    namespace: "otel",
                    emitter: &context.emitter,
                    mapping: &connector.mapping,
                    input_schema: input_schema.arrow_schema(),
                    udfs: context.udfs.as_ref(),
                })?;
                let export = OtelExportConfig {
                    config: connector.client.config.entries.clone(), dns: context.dns()?,
                    batch: connector.batch, values: connector.values.clone(), attributes: connector.attributes.clone(),
                    resource: connector.resource.clone(), scope: connector.scope.clone(), mapped_schema: projection.mapped_schema().clone(),
                };
                Self::row_request(projection, $entity::new($config { export $(, metric: $metric.clone())? }, context.emitter_host()))?
            }};
        }
        let opened = match connector {
            OtelEmitterPlan::Logs(plan) => open!(OtelLogsEmitter, OtelLogsEmitterConfig, plan),
            OtelEmitterPlan::Traces(plan) => {
                open!(OtelTracesEmitter, OtelTracesEmitterConfig, plan)
            }
            OtelEmitterPlan::Metrics(plan) => open!(
                OtelMetricsEmitter,
                OtelMetricsEmitterConfig,
                plan,
                plan.metric
            ),
        };
        Ok(opened)
    }
}
