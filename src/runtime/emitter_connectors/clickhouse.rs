//! ClickHouse emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the ClickHouse connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

const _: () = assert!(matches!(
    ClickHouseTableEmitter::KIND,
    nervix_models::EmitterEntityKind::ClickHouse(nervix_models::ClickHouseEmitterEntity::Table)
));

impl EmitterConnectorStarter {
    pub(super) fn clickhouse(
        connector: &ClickHouseTableEmitterPlan,
        context: &EmitterConnectorContext,
        input_schema: &CompiledSchema,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = {
            let projection = Self::projection(MappedValuesProjectionInit {
                label: "ClickHouse",
                namespace: "clickhouse",
                emitter: &context.emitter,
                mapping: &connector.mapping,
                input_schema: input_schema.arrow_schema(),
                udfs: context.udfs.as_ref(),
            })?;
            Self::row(
                projection,
                ClickHouseTableEmitter::new(
                    ClickHouseTableEmitterConfig {
                        config: connector.client.config.entries.clone(),
                        table: connector.table.clone(),
                        dns: context.dns()?,
                        batch: connector.batch,
                    },
                    context.emitter_host(),
                ),
            )?
        };
        Ok(opened)
    }
}
