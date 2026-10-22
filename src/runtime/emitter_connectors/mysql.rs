//! MySql emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the MySql connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

impl EmitterConnectorStarter {
    pub(super) async fn mysql(
        connector: &MySqlEmitterPlan,
        context: &EmitterConnectorContext,
        input_schema: &CompiledSchema,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = {
            let projection = Self::projection(MappedValuesProjectionInit {
                label: "MySQL",
                namespace: "mysql",
                emitter: &context.emitter,
                mapping: &connector.mapping,
                input_schema: input_schema.arrow_schema(),
                udfs: context.udfs.as_ref(),
            })?;
            let connections =
                PooledEmitterClient::lease(context, &connector.client, connector.pooled_client())
                    .await
                    .map_err(emitter_init_error)?;
            Self::row(
                projection,
                MySqlEmitter::new(
                    MySqlEmitterConfig {
                        table: connector.table.clone(),
                        conflict_action: connector.conflict_action,
                        batch: connector.batch,
                    },
                    Box::new(connections),
                    context.emitter_host(),
                ),
            )?
        };
        Ok(opened)
    }
}
